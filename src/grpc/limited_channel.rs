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

/// Optional consumer-owned admission registry for submissions. It contains only command IDs:
/// the consumer retains all scheduling and stop-policy semantics.
#[derive(Clone, Default)]
pub struct TbankSubmitAdmission(
    Arc<std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<bool>>>>,
);
impl PartialEq for TbankSubmitAdmission {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for TbankSubmitAdmission {}
impl TbankSubmitAdmission {
    /// Registers a revocation signal before handing the command to ExecutionEngine.
    /// `true` or a closed sender forbids starting a new RPC; admitted RPCs are never aborted.
    /// A duplicate identity is rejected without replacing the original owner's signal.
    pub fn register(
        &self,
        id: String,
        revoked: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        use std::collections::hash_map::Entry;
        anyhow::ensure!(!id.is_empty(), "empty submit admission identity");
        match self.0.lock().expect("submit admission poisoned").entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(revoked);
                Ok(())
            }
            Entry::Occupied(_) => anyhow::bail!("submit admission identity already registered"),
        }
    }
    /// Releases the registry entry. Existing transport markers retain their revocation signal.
    pub fn remove(&self, id: &str) {
        self.0.lock().expect("submit admission poisoned").remove(id);
    }
    pub(crate) fn marker(&self, id: String) -> TbankDispatchMarker {
        let revoked = self
            .0
            .lock()
            .expect("submit admission poisoned")
            .get(&id)
            .cloned();
        let mut marker = TbankDispatchMarker::for_order(id);
        marker.admission = revoked.map_or(SubmitAdmission::Missing, SubmitAdmission::Revocable);
        marker
    }
}

#[derive(Clone, Default)]
enum SubmitAdmission {
    #[default]
    Unrestricted,
    Missing,
    Revocable(tokio::sync::watch::Receiver<bool>),
}

/// Evidence shared with a mutating command to distinguish local expiry from an unknown outcome.
#[derive(Clone, Default)]
pub struct TbankDispatchMarker {
    dispatched: Arc<AtomicBool>,
    client_order_id: Option<String>,
    admission: SubmitAdmission,
}
impl TbankDispatchMarker {
    /// Associates all transport stages with a Nautilus command.
    pub fn for_order(client_order_id: String) -> Self {
        Self {
            dispatched: Arc::default(),
            client_order_id: Some(client_order_id),
            admission: SubmitAdmission::Unrestricted,
        }
    }
    fn revoked_status() -> Status {
        Status::cancelled("submission admission revoked before dispatch")
    }

    async fn revoked(&self) {
        match &self.admission {
            SubmitAdmission::Unrestricted => std::future::pending().await,
            SubmitAdmission::Missing => {}
            SubmitAdmission::Revocable(signal) => {
                let mut signal = signal.clone();
                loop {
                    if *signal.borrow_and_update() {
                        return;
                    }
                    if signal.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Only pre-dispatch work is cancelable. Dropping it releases quota tickets and tentative
    /// reservations; a started broker mutation must keep its independent recovery lifetime.
    async fn while_waiting<T>(
        &self,
        work: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        if matches!(self.admission, SubmitAdmission::Unrestricted) {
            return work.await;
        }
        tokio::select! {
            biased;
            _=self.revoked()=>Err(Self::revoked_status()),
            result=work=>result,
        }
    }

    /// Linearizes revocation with the synchronous transport call. Hold the signal's read borrow
    /// only across quota commit and Channel::call, never while awaiting a broker response.
    fn dispatch<T>(&self, start: impl FnOnce() -> T) -> Result<T, Status> {
        match &self.admission {
            SubmitAdmission::Unrestricted => Ok(start()),
            SubmitAdmission::Missing => Err(Self::revoked_status()),
            SubmitAdmission::Revocable(signal) => {
                let revoked = signal.borrow();
                if *revoked || signal.has_changed().is_err() {
                    return Err(Self::revoked_status());
                }
                let result = start();
                drop(revoked);
                Ok(result)
            }
        }
    }

    #[cfg(test)]
    fn check_admission(&self) -> Result<(), Status> {
        self.dispatch(|| ())
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
            let marker = request
                .extensions()
                .get::<TbankDispatchMarker>()
                .cloned()
                .unwrap_or_default();
            let authorization = request.headers().get("authorization").cloned();
            let result = tokio::time::timeout_at(deadline, async {
                let (auth, scope, limiter) = marker
                    .while_waiting(async {
                        let auth = authorization
                            .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
                        let scope = shared_scope(super::limiter::scope_key(auth.as_bytes()));
                        let limiter = match explicit_scope {
                            Some(limiter) => limiter,
                            None => {
                                quota_scope(&scope, channel.clone(), auth.clone(), deadline).await?
                            }
                        };
                        Ok((auth, scope, limiter))
                    })
                    .await?;
                let streaming = method.ends_with("Stream") || method.ends_with("ServerSideStream");
                let (stream_permits, reservation, stream_admission) = marker
                    .while_waiting(async {
                        record("quota_wait");
                        let stream_permits = if streaming {
                            limiter
                                .reserve_stream(&method, deadline)
                                .await
                                .map_err(|e| Status::resource_exhausted(e.to_string()))?
                        } else {
                            Vec::new()
                        };
                        let reservation = if !streaming {
                            let urgent =
                                method.starts_with("Cancel") || method.ends_with("OrderState");
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
                                let tariff =
                                    fetch_tariff(channel.clone(), auth.clone(), deadline).await?;
                                let stream_available =
                                    super::limiter::TbankRateLimiter::stream_available(
                                        &tariff, &method,
                                    )
                                    .map_err(|e| Status::failed_precondition(e.to_string()))?;
                                super::limiter::TbankRateLimiter::from_tariff(&tariff)
                                    .map_err(|e| Status::failed_precondition(e.to_string()))?;
                                scope.cache_tariff(Arc::new(tariff));
                                if stream_available {
                                    break admission;
                                }
                                drop(admission);
                                tokio::time::sleep_until(
                                    deadline
                                        .min(tokio::time::Instant::now() + Duration::from_secs(1)),
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
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        if remaining.is_zero() {
                            let status =
                                Status::deadline_exceeded("RPC deadline expired before dispatch");
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
                        Ok((stream_permits, reservation, stream_admission))
                    })
                    .await
                    .inspect_err(|status| {
                        if status.code() == tonic::Code::Cancelled {
                            failure = Some(rpc_failure("before_dispatch", status));
                        }
                    })?;
                // The revocation borrow closes the check/call race without holding any guard
                // over network I/O. A failed dispatch drops the uncommitted quota reservation.
                let response_future = marker
                    .dispatch(|| {
                        if let Some(reservation) = reservation {
                            reservation.commit();
                        }
                        marker.dispatched.store(true, Ordering::Release);
                        record("rpc_started");
                        channel.call(request)
                    })
                    .inspect_err(|status| {
                        failure = Some(rpc_failure("before_dispatch", status));
                    })?;
                failure_phase = "await_headers";
                let response = tokio::time::timeout_at(deadline, response_future)
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
    #[test]
    fn submit_revocation_is_rechecked_after_registration_and_is_command_scoped() {
        let admission = TbankSubmitAdmission::default();
        let (one, rx) = tokio::sync::watch::channel(false);
        let (two, other) = tokio::sync::watch::channel(false);
        admission.register("one".into(), rx).unwrap();
        admission.register("two".into(), other).unwrap();
        let (_replacement, replacement) = tokio::sync::watch::channel(false);
        assert!(admission.register("one".into(), replacement).is_err());
        let marker = admission.marker("one".into());
        let other = admission.marker("two".into());
        assert!(marker.check_admission().is_ok());
        one.send_replace(true);
        assert_eq!(
            marker.check_admission().unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert!(other.check_admission().is_ok());
        assert!(!marker.started());
        admission.remove("two");
        assert!(admission.marker("two".into()).check_admission().is_err());
        drop(two);
        assert!(other.check_admission().is_err());
    }
    #[tokio::test]
    async fn revoked_transport_waiter_releases_its_quota_ticket_without_dispatch() {
        use super::super::limiter::{TbankRateLimiter, TbankRpcQuota};
        let limiter = Arc::new(
            TbankRateLimiter::new(
                vec![TbankRpcQuota {
                    methods: vec!["PostOrder".into()],
                    capacity: 1,
                    window: Duration::from_secs(3600),
                }],
                1,
            )
            .unwrap(),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let occupied = limiter
            .reserve_guard("PostOrder", false, deadline)
            .await
            .unwrap();
        let registry = TbankSubmitAdmission::default();
        let (stop, receiver) = tokio::sync::watch::channel(false);
        registry.register("queued-order".into(), receiver).unwrap();
        let marker = registry.marker("queued-order".into());
        let channel = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        let mut transport = TbankLimitedChannel::with_quota_scope(channel, Some(limiter.clone()));
        let mut request = http::Request::builder()
            .uri("/orders/PostOrder")
            .header("authorization", "Bearer transport-admission-test")
            .header("grpc-timeout", "60S")
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(marker.clone());
        let pending = transport.call(request);
        tokio::pin!(pending);
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        stop.send_replace(true);
        let error = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .expect_err("revoked call must fail locally");
        assert_eq!(error.code(), tonic::Code::Cancelled);
        assert!(!marker.started());
        // The queue has just one slot. A stale cancelled ticket would prevent this new waiter.
        let other = limiter.reserve_guard("PostOrder", false, deadline);
        tokio::pin!(other);
        assert!(futures_util::poll!(other.as_mut()).is_pending());
        drop(occupied);
        let reservation = tokio::time::timeout(Duration::from_secs(1), other)
            .await
            .unwrap()
            .unwrap();
        drop(reservation);
        // Neither the cancelled request nor the tentative reservations charged the sliding window.
        let _available = tokio::time::timeout(
            Duration::from_secs(1),
            limiter.reserve_guard("PostOrder", false, deadline),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn revoked_channel_readiness_wait_refunds_reserved_quota() {
        use super::super::limiter::{TbankRateLimiter, TbankRpcQuota};
        let limiter = Arc::new(
            TbankRateLimiter::new(
                vec![TbankRpcQuota {
                    methods: vec![],
                    capacity: 1,
                    window: Duration::from_secs(3600),
                }],
                1,
            )
            .unwrap(),
        );
        let registry = TbankSubmitAdmission::default();
        let (stop, receiver) = tokio::sync::watch::channel(false);
        registry.register("ready-order".into(), receiver).unwrap();
        let marker = registry.marker("ready-order".into());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let preparing = marker.while_waiting(async {
            let reservation = limiter
                .reserve_guard("PostOrder", false, deadline)
                .await
                .unwrap();
            // Channel readiness has not completed, although quota was tentatively reserved.
            std::future::pending::<()>().await;
            Ok(reservation)
        });
        tokio::pin!(preparing);
        assert!(futures_util::poll!(preparing.as_mut()).is_pending());
        stop.send_replace(true);
        assert_eq!(
            preparing.await.err().unwrap().code(),
            tonic::Code::Cancelled
        );
        assert!(!marker.started());
        let _available = tokio::time::timeout(
            Duration::from_secs(1),
            limiter.reserve_guard("PostOrder", false, deadline),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[test]
    fn revocation_and_the_transport_call_have_an_ordered_boundary() {
        let registry = TbankSubmitAdmission::default();
        let (stop, receiver) = tokio::sync::watch::channel(false);
        registry.register("racing-order".into(), receiver).unwrap();
        let marker = registry.marker("racing-order".into());
        let (enter_tx, enter_rx) = std::sync::mpsc::channel();
        let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let revoker = std::thread::spawn(move || {
            enter_rx.recv().unwrap();
            attempt_tx.send(()).unwrap();
            stop.send_replace(true);
            done_tx.send(()).unwrap();
        });
        marker
            .dispatch(|| {
                enter_tx.send(()).unwrap();
                attempt_rx.recv().unwrap();
                assert!(matches!(
                    done_rx.recv_timeout(Duration::from_millis(10)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ));
                marker.dispatched.store(true, Ordering::Release);
            })
            .unwrap();
        revoker.join().unwrap();
        done_rx.recv().unwrap();
        assert!(
            marker.started(),
            "revocation does not erase an already started RPC"
        );
        assert!(
            marker.check_admission().is_err(),
            "subsequent dispatch is revoked"
        );
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
