//! Transport-wide quota admission before a request can reach the channel.
use super::limiter::{
    Scope, TbankRpcFailure, diagnostic_failure_with_context, method_name, rpc_failure,
    rpc_failure_message, shared_scope,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tonic::{Status, body::Body, codegen::Service, transport::Channel};

/// Evidence shared with a mutating command to distinguish local expiry from an unknown outcome.
#[derive(Clone, Default)]
pub struct TbankDispatchMarker {
    dispatched: Arc<AtomicBool>,
    client_order_id: Option<String>,
}
impl TbankDispatchMarker {
    /// Associates all transport stages with a Nautilus command.
    pub fn for_order(client_order_id: String) -> Self {
        Self {
            dispatched: Arc::default(),
            client_order_id: Some(client_order_id),
        }
    }
    /// True only after quota admission and channel readiness, immediately before dispatch.
    pub fn started(&self) -> bool {
        self.dispatched.load(Ordering::Acquire)
    }
}
/// Channel wrapper used by all generated broker clients, including reconnect-created clients.
#[derive(Clone)]
pub struct TbankLimitedChannel {
    channel: Channel,
    explicit_scope: Option<Arc<super::limiter::TbankRateLimiter>>,
}
impl TbankLimitedChannel {
    /// Wraps a channel. Scope is derived privately from authorization metadata at dispatch.
    pub fn new(channel: Channel) -> Self {
        Self::with_quota_scope(channel, None)
    }
    /// Uses an explicit limit domain shared by clients with different credentials.
    pub fn with_quota_scope(
        channel: Channel,
        explicit_scope: Option<Arc<super::limiter::TbankRateLimiter>>,
    ) -> Self {
        Self {
            channel,
            explicit_scope,
        }
    }
}
impl Service<http::Request<Body>> for TbankLimitedChannel {
    type Response = http::Response<Body>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let mut channel = self.channel.clone();
        let explicit_scope = self.explicit_scope.clone();
        Box::pin(async move {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let method = method_name(request.uri().path()).to_string();
            let explicit_deadline = request.headers().contains_key("grpc-timeout");
            let timeout = request
                .headers()
                .get("grpc-timeout")
                .and_then(|h| h.to_str().ok())
                .and_then(parse_timeout)
                .unwrap_or(Duration::from_secs(30));
            let deadline = tokio::time::Instant::now() + timeout;
            let client_order_id = request
                .extensions()
                .get::<TbankDispatchMarker>()
                .and_then(|m| m.client_order_id.clone());
            let record = |stage| {
                super::limiter::diagnostic_with_context(
                    id,
                    &method,
                    stage,
                    client_order_id.as_deref(),
                    None,
                )
            };
            let record_failure = |failure| {
                diagnostic_failure_with_context(id, &method, client_order_id.as_deref(), failure)
            };
            record("received");
            let mut failure = None::<TbankRpcFailure>;
            let mut failure_phase = "quota_admission";
            let result = tokio::time::timeout_at(deadline, async {
                let auth = request
                    .headers()
                    .get("authorization")
                    .cloned()
                    .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
                let scope = shared_scope(super::limiter::scope_key(auth.as_bytes()));
                let limiter = match explicit_scope {
                    Some(limiter) => limiter,
                    None => quota_scope(&scope, channel.clone(), auth.clone(), deadline).await?,
                };
                record("quota_wait");
                let streaming = method.ends_with("Stream") || method.ends_with("ServerSideStream");
                let stream_permits = if streaming {
                    limiter
                        .reserve_stream(&method, deadline)
                        .await
                        .map_err(|e| Status::resource_exhausted(e.to_string()))?
                } else {
                    Vec::new()
                };
                let reservation = if !streaming {
                    let urgent = method.starts_with("Cancel") || method.ends_with("OrderState");
                    Some(
                        limiter
                            .reserve_guard(&method, urgent, deadline)
                            .await
                            .map_err(|e| Status::resource_exhausted(e.to_string()))?,
                    )
                } else {
                    None
                };
                // Hold admission from the fresh snapshot through opening headers.
                // A full external quota releases the gate so other groups can progress.
                let stream_admission = if streaming {
                    Some(loop {
                        let admission = limiter.stream_admission.lock().await;
                        let reservation = limiter
                            .reserve_guard("GetUserTariff", false, deadline)
                            .await
                            .map_err(|e| Status::resource_exhausted(e.to_string()))?;
                        reservation.commit();
                        let tariff = fetch_tariff(channel.clone(), auth.clone(), deadline).await?;
                        let stream_available =
                            super::limiter::TbankRateLimiter::stream_available(&tariff, &method)
                                .map_err(|e| Status::failed_precondition(e.to_string()))?;
                        super::limiter::TbankRateLimiter::from_tariff(&tariff)
                            .map_err(|e| Status::failed_precondition(e.to_string()))?;
                        scope.cache_tariff(Arc::new(tariff));
                        if stream_available {
                            break admission;
                        }
                        drop(admission);
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + Duration::from_secs(1)),
                        )
                        .await;
                    })
                } else {
                    None
                };
                record("quota_reserved");
                std::future::poll_fn(|cx| channel.poll_ready(cx))
                    .await
                    .map_err(|error| {
                        failure = Some(rpc_failure_message(
                            "channel_ready",
                            "Transport",
                            &format!("{error:?}"),
                        ));
                        Status::unavailable("channel is not ready")
                    })?;
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    let status = Status::deadline_exceeded("RPC deadline expired before dispatch");
                    failure = Some(rpc_failure("before_dispatch", &status));
                    return Err(status);
                }
                // Admission/opening is bounded, but a stream without a consumer deadline must
                // not acquire an invented lifetime through grpc-timeout metadata.
                if !streaming || explicit_deadline {
                    let timeout_request = super::with_timeout((), remaining);
                    let value = timeout_request
                        .metadata()
                        .get("grpc-timeout")
                        .expect("timeout metadata");
                    request.headers_mut().insert(
                        "grpc-timeout",
                        http::HeaderValue::from_bytes(value.as_bytes())
                            .map_err(|_| Status::internal("invalid remaining timeout"))?,
                    );
                }
                // Charge the sliding windows at the actual dispatch boundary. A local expiry
                // or cancelled readiness wait drops the reservation without consuming quota.
                if let Some(reservation) = reservation {
                    reservation.commit();
                }
                if let Some(marker) = request.extensions().get::<TbankDispatchMarker>() {
                    marker.dispatched.store(true, Ordering::Release);
                }
                failure_phase = "await_headers";
                record("rpc_started");
                let response = tokio::time::timeout_at(deadline, channel.call(request))
                    .await
                    .map_err(|_| {
                        let status =
                            Status::deadline_exceeded("RPC deadline expired after dispatch");
                        failure = Some(rpc_failure("await_headers", &status));
                        status
                    })?
                    .map_err(|error| {
                        failure = Some(rpc_failure_message(
                            "await_headers",
                            "Transport",
                            &format!("{error:?}"),
                        ));
                        Status::unavailable("broker transport failed after dispatch")
                    })?;
                drop(stream_admission);
                record("rpc_headers");
                failure_phase = "response_body";
                let finished = http_body::Body::is_end_stream(response.body());
                if finished {
                    match Status::from_header_map(response.headers()) {
                        Some(status) if status.code() != tonic::Code::Ok => {
                            record_failure(rpc_failure("response_headers", &status));
                        }
                        _ => record("rpc_finished"),
                    }
                }
                Ok(response.map(|inner| {
                    Body::new(RpcBody {
                        inner,
                        _permits: stream_permits,
                        id,
                        method: method.clone(),
                        client_order_id: client_order_id.clone(),
                        finished,
                        deadline: (!streaming || explicit_deadline)
                            .then(|| Box::pin(tokio::time::sleep_until(deadline))),
                    })
                }))
            })
            .await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(Status::deadline_exceeded("RPC deadline expired")),
            };
            if let Err(status) = &result {
                record_failure(failure.unwrap_or_else(|| rpc_failure(failure_phase, status)));
            }
            result
        })
    }
}
async fn quota_scope(
    scope: &Scope,
    channel: Channel,
    auth: http::HeaderValue,
    deadline: tokio::time::Instant,
) -> Result<Arc<super::limiter::TbankRateLimiter>, Status> {
    let tariff = scope
        .tariff
        .get_or_try_init(|| async move {
            let tariff = fetch_tariff(channel, auth, deadline).await?;
            super::limiter::TbankRateLimiter::from_tariff(&tariff)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            let tariff = Arc::new(tariff);
            scope.cache_tariff(Arc::clone(&tariff));
            Ok::<_, Status>(tariff)
        })
        .await?
        .clone();
    scope
        .limiter
        .get_or_try_init(|| async move {
            super::limiter::TbankRateLimiter::from_tariff(&tariff)
                .map(|limiter| {
                    limiter.record_discovery();
                    Arc::new(limiter)
                })
                .map_err(|e| Status::failed_precondition(e.to_string()))
        })
        .await
        .cloned()
}
async fn fetch_tariff(
    channel: Channel,
    auth: http::HeaderValue,
    deadline: tokio::time::Instant,
) -> Result<super::generated::GetUserTariffResponse, Status> {
    let mut client = super::generated::users_service_client::UsersServiceClient::new(channel);
    let mut request = super::with_timeout(
        super::generated::GetUserTariffRequest {},
        deadline.saturating_duration_since(tokio::time::Instant::now()),
    );
    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", auth);
    let metadata = tonic::metadata::MetadataMap::from_headers(headers);
    request.metadata_mut().insert(
        "authorization",
        metadata
            .get("authorization")
            .cloned()
            .ok_or_else(|| Status::unauthenticated("invalid authorization"))?,
    );
    request.metadata_mut().insert(
        "x-app-name",
        "tbank_nt_community".parse().expect("static app name"),
    );
    let tariff = tokio::time::timeout_at(deadline, client.get_user_tariff(request))
        .await
        .map_err(|_| {
            Status::deadline_exceeded("quota discovery deadline expired before dispatch")
        })??
        .into_inner();
    Ok(tariff)
}
fn parse_timeout(value: &str) -> Option<Duration> {
    let (n, unit) = value.split_at(value.len().checked_sub(1)?);
    let n: u64 = n.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

struct RpcBody {
    inner: Body,
    _permits: Vec<tokio::sync::OwnedSemaphorePermit>,
    id: u64,
    method: String,
    client_order_id: Option<String>,
    finished: bool,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl http_body::Body for RpcBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if self
            .deadline
            .as_mut()
            .is_some_and(|timer| timer.as_mut().poll(cx).is_ready())
        {
            self.finished = true;
            let status = Status::deadline_exceeded("RPC body exceeded the consumer deadline");
            diagnostic_failure_with_context(
                self.id,
                &self.method,
                self.client_order_id.as_deref(),
                rpc_failure("response_body", &status),
            );
            return Poll::Ready(Some(Err(status)));
        }
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        let terminal = match &frame {
            Poll::Ready(None) => Some(false),
            Poll::Ready(Some(Err(_))) => Some(true),
            Poll::Ready(Some(Ok(frame))) => frame.trailers_ref().map(|trailers| {
                Status::from_header_map(trailers)
                    .is_none_or(|status| status.code() != tonic::Code::Ok)
            }),
            Poll::Pending => None,
        };
        if let Some(failed) = terminal
            && !self.finished
        {
            self.finished = true;
            if failed {
                let status = match &frame {
                    Poll::Ready(Some(Err(status))) => Some(status.clone()),
                    Poll::Ready(Some(Ok(frame))) => {
                        frame.trailers_ref().and_then(Status::from_header_map)
                    }
                    _ => None,
                }
                .unwrap_or_else(|| {
                    Status::unknown("gRPC response trailers are missing grpc-status")
                });
                diagnostic_failure_with_context(
                    self.id,
                    &self.method,
                    self.client_order_id.as_deref(),
                    rpc_failure("response_body", &status),
                );
            } else {
                super::limiter::diagnostic_with_context(
                    self.id,
                    &self.method,
                    "rpc_finished",
                    self.client_order_id.as_deref(),
                    None,
                );
            }
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
impl Drop for RpcBody {
    fn drop(&mut self) {
        if !self.finished {
            super::limiter::diagnostic_with_context(
                self.id,
                &self.method,
                "rpc_body_dropped",
                self.client_order_id.as_deref(),
                None,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Trailers {
        status: &'static str,
        sent: bool,
    }
    impl http_body::Body for Trailers {
        type Data = bytes::Bytes;
        type Error = Status;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            if self.sent {
                return Poll::Ready(None);
            }
            self.sent = true;
            let mut headers = http::HeaderMap::new();
            headers.insert("grpc-status", http::HeaderValue::from_static(self.status));
            headers.insert(
                "grpc-message",
                http::HeaderValue::from_static("permission%20denied"),
            );
            Poll::Ready(Some(Ok(http_body::Frame::trailers(headers))))
        }
    }
    #[tokio::test]
    async fn grpc_trailers_finish_diagnostics_before_tonic_drops_the_body() {
        let mut diagnostics = super::super::limiter::subscribe_tbank_rpc_diagnostics();
        for status in ["0", "7"] {
            let rpc_id = if status == "7" {
                u64::MAX - 1
            } else {
                u64::MAX - 2
            };
            let mut body = RpcBody {
                inner: Body::new(Trailers {
                    status,
                    sent: false,
                }),
                _permits: vec![],
                id: rpc_id,
                method: "diagnostic-test".into(),
                client_order_id: None,
                finished: false,
                deadline: None,
            };
            let frame =
                std::future::poll_fn(|cx| http_body::Body::poll_frame(Pin::new(&mut body), cx))
                    .await;
            assert!(matches!(frame, Some(Ok(ref f)) if f.is_trailers()));
            // Tonic can stop at trailers without polling the inner body for EOF again.
            assert!(
                body.finished,
                "completed RPC must not be recorded as abandoned"
            );
            if status == "7" {
                let event = loop {
                    match diagnostics.try_recv() {
                        Ok(event) if event.rpc_id == rpc_id => break event,
                        Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                            panic!("failed RPC diagnostic was not emitted")
                        }
                        Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                            panic!("RPC diagnostic channel closed")
                        }
                    }
                };
                assert_eq!(event.stage, "rpc_failed");
                let failure = event
                    .failure
                    .expect("failed RPC should include status details");
                assert_eq!(failure.phase, "response_body");
                assert_eq!(failure.code, "PermissionDenied");
                assert_eq!(failure.message, "permission denied");
            }
        }
    }
    struct PendingBody;
    impl http_body::Body for PendingBody {
        type Data = bytes::Bytes;
        type Error = Status;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }
    #[tokio::test]
    async fn consumer_deadline_covers_body_after_response_headers() {
        let mut body = RpcBody {
            inner: Body::new(PendingBody),
            _permits: vec![],
            id: u64::MAX,
            method: "deadline-test".into(),
            client_order_id: None,
            finished: false,
            deadline: Some(Box::pin(tokio::time::sleep(Duration::from_millis(1)))),
        };
        let frame =
            std::future::poll_fn(|cx| http_body::Body::poll_frame(Pin::new(&mut body), cx)).await;
        assert!(
            matches!(frame, Some(Err(status)) if status.code() == tonic::Code::DeadlineExceeded)
        );
        assert!(http_body::Body::is_end_stream(&body));
    }
}
