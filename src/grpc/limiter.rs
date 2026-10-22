//! Shared atomic RPC quotas and bounded, cancellation-safe admission.
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};
use tonic::Status;

/// One quota shared by all matching RPC methods (empty methods means all RPCs).
#[derive(Clone, Debug)]
pub struct TbankRpcQuota {
    /// RPC method names, e.g. PostOrder; shared groups consume one common window.
    pub methods: Vec<String>,
    /// Maximum calls in the sliding window.
    pub capacity: usize,
    /// Sliding window duration.
    pub window: Duration,
}
#[derive(Debug)]
struct Bucket {
    quota: TbankRpcQuota,
    used: VecDeque<Usage>,
}
#[derive(Debug)]
struct Usage {
    id: u64,
    dispatched_at: Option<Instant>,
}
#[derive(Debug)]
struct State {
    buckets: Vec<Bucket>,
    next: u64,
    waiters: VecDeque<(u64, bool, String)>,
    urgent_streak: usize,
}
/// Share one instance across every client in a broker quota scope.
#[derive(Debug)]
pub struct TbankRateLimiter {
    streams: Vec<(Vec<String>, Arc<tokio::sync::Semaphore>)>,
    stream_queue: Arc<tokio::sync::Semaphore>,
    pub(crate) stream_admission: tokio::sync::Mutex<()>,
    state: Mutex<State>,
    changed: Notify,
    max_queue: usize,
}
// Config equality compares quota-domain identity, not mutable token counts.
impl PartialEq for TbankRateLimiter {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for TbankRateLimiter {}
impl TbankRateLimiter {
    /// Creates an explicitly configured scope; overlapping quotas are reserved atomically.
    pub fn new(quotas: Vec<TbankRpcQuota>, max_queue: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(max_queue > 0, "quota queue must be bounded and nonzero");
        anyhow::ensure!(
            quotas.iter().all(|q| q.capacity > 0 && !q.window.is_zero()),
            "invalid RPC quota"
        );
        Ok(Self {
            streams: Vec::new(),
            stream_admission: tokio::sync::Mutex::new(()),
            stream_queue: Arc::new(tokio::sync::Semaphore::new(max_queue)),
            state: Mutex::new(State {
                buckets: quotas
                    .into_iter()
                    .map(|quota| Bucket {
                        quota,
                        used: VecDeque::new(),
                    })
                    .collect(),
                next: 0,
                waiters: VecDeque::new(),
                urgent_streak: 0,
            }),
            changed: Notify::new(),
            max_queue,
        })
    }
    /// Builds all minute/second shared groups from the broker's authoritative tariff.
    pub fn from_tariff(tariff: &super::generated::GetUserTariffResponse) -> anyhow::Result<Self> {
        let mut quotas = Vec::new();
        for limit in &tariff.unary_limits {
            anyhow::ensure!(
                limit.limit_per_minute > 0,
                "broker returned invalid minute quota"
            );
            let methods = limit
                .methods
                .iter()
                .map(|m| method_name(m).to_string())
                .collect::<Vec<_>>();
            anyhow::ensure!(
                !methods.is_empty() && methods.iter().all(|m| !m.is_empty()),
                "broker returned quota without valid methods"
            );
            quotas.push(TbankRpcQuota {
                methods: methods.clone(),
                capacity: usize::try_from(limit.limit_per_minute)?,
                window: Duration::from_secs(60),
            });
            if let Some(per_second) = limit.limit_per_second {
                anyhow::ensure!(per_second > 0, "broker returned invalid second quota");
                quotas.push(TbankRpcQuota {
                    methods,
                    capacity: usize::try_from(per_second)?,
                    window: Duration::from_secs(1),
                });
            }
        }
        anyhow::ensure!(!quotas.is_empty(), "broker returned no unary quotas");
        let mut limiter = Self::new(quotas, 1024)?;
        for limit in &tariff.stream_limits {
            anyhow::ensure!(
                limit.limit > 0
                    && limit.open >= 0
                    && !limit.streams.is_empty()
                    && limit.streams.iter().all(|m| !method_name(m).is_empty()),
                "invalid broker stream quota"
            );
            // Local lifetime permits track our streams only. External occupancy is
            // checked afresh at the transport opening boundary, never cached here.
            let available = usize::try_from(limit.limit)?;
            limiter.streams.push((
                limit
                    .streams
                    .iter()
                    .map(|s| method_name(s).to_string())
                    .collect(),
                Arc::new(tokio::sync::Semaphore::new(available)),
            ));
        }
        Ok(limiter)
    }

    /// A fresh broker snapshot includes both our open streams and external clients.
    pub(crate) fn stream_available(
        tariff: &super::generated::GetUserTariffResponse,
        method: &str,
    ) -> anyhow::Result<bool> {
        let method = canonical_method(method_name(method));
        let matching = tariff
            .stream_limits
            .iter()
            .filter(|limit| {
                limit
                    .streams
                    .iter()
                    .any(|m| canonical_method(method_name(m)) == method)
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !matching.is_empty(),
            "broker stream quota is unknown for RPC {method}"
        );
        anyhow::ensure!(
            matching.iter().all(|l| l.limit > 0 && l.open >= 0),
            "invalid broker stream quota"
        );
        Ok(matching.iter().all(|l| l.open < l.limit))
    }

    /// Reserves shared stream slots until the response body is dropped.
    pub async fn reserve_stream(
        &self,
        method: &str,
        deadline: Instant,
    ) -> anyhow::Result<Vec<tokio::sync::OwnedSemaphorePermit>> {
        let _queue = Arc::clone(&self.stream_queue)
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("stream quota queue is full"))?;
        let method = canonical_method(method_name(method));
        let matching = self
            .streams
            .iter()
            .filter(|(methods, _)| methods.iter().any(|m| canonical_method(m) == method))
            .map(|(_, s)| Arc::clone(s))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !matching.is_empty(),
            "broker stream quota is unknown for RPC {method}"
        );
        loop {
            let mut permits = Vec::new();
            let mut blocked = None;
            for semaphore in &matching {
                match Arc::clone(semaphore).try_acquire_owned() {
                    Ok(permit) => permits.push(permit),
                    Err(_) => {
                        blocked = Some(Arc::clone(semaphore));
                        break;
                    }
                }
            }
            if let Some(semaphore) = blocked {
                // Roll back every tentative slot before waiting on any shared group.
                drop(permits);
                drop(tokio::time::timeout_at(deadline, semaphore.acquire_owned()).await??);
            } else {
                return Ok(permits);
            }
        }
    }

    pub(crate) fn record_discovery(&self) {
        let mut s = self.state.lock().expect("RPC quota lock poisoned");
        for b in &mut s.buckets {
            if matches_method(&b.quota, "GetUserTariff") {
                b.used.push_back(Usage {
                    id: 0,
                    dispatched_at: Some(Instant::now()),
                });
            }
        }
    }

    /// Atomically admits and immediately charges a call; useful for explicit RPC wrappers.
    pub async fn reserve(
        self: &Arc<Self>,
        method: &str,
        urgent: bool,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        self.reserve_guard(method, urgent, deadline).await?.commit();
        Ok(())
    }

    /// Tentative admission remains held until dispatch; cancellation refunds it.
    pub(crate) async fn reserve_guard(
        self: &Arc<Self>,
        method: &str,
        urgent: bool,
        deadline: Instant,
    ) -> anyhow::Result<QuotaReservation> {
        let method = canonical_method(method_name(method));
        let id = {
            let mut s = self.state.lock().expect("RPC quota lock poisoned");
            anyhow::ensure!(
                s.waiters.iter().filter(|(_, u, _)| *u == urgent).count() < self.max_queue,
                "RPC quota queue is full"
            );
            anyhow::ensure!(
                s.buckets.iter().any(|b| matches_method(&b.quota, &method)),
                "broker quota is unknown for RPC {method}"
            );
            s.next = s.next.checked_add(1).expect("quota ticket overflow");
            let id = s.next;
            s.waiters.push_back((id, urgent, method.clone()));
            id
        };
        let _ticket = Ticket {
            limiter: Arc::clone(self),
            id,
        };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let now = Instant::now();
            anyhow::ensure!(now < deadline, "RPC deadline expired before dispatch");
            let wake = {
                let mut s = self.state.lock().expect("RPC quota lock poisoned");
                for b in &mut s.buckets {
                    b.used.retain(|used| {
                        used.dispatched_at
                            .is_none_or(|t| now.duration_since(t) < b.quota.window)
                    });
                }
                let eligible = |m: &str| {
                    s.buckets
                        .iter()
                        .all(|b| !matches_method(&b.quota, m) || b.used.len() < b.quota.capacity)
                };
                let prefer_urgent = s.urgent_streak < 8;
                let selected = s
                    .waiters
                    .iter()
                    .find(|(_, u, m)| *u == prefer_urgent && eligible(m))
                    .or_else(|| s.waiters.iter().find(|(_, _, m)| eligible(m)))
                    .map(|(i, _, _)| *i);
                let mut wake = deadline;
                let mut available = true;
                for b in &s.buckets {
                    if matches_method(&b.quota, &method) && b.used.len() >= b.quota.capacity {
                        available = false;
                        if let Some(expires) = b
                            .used
                            .iter()
                            .filter_map(|used| used.dispatched_at.map(|t| t + b.quota.window))
                            .min()
                        {
                            wake = wake.min(expires);
                        }
                    }
                }
                if selected == Some(id) && available {
                    for b in &mut s.buckets {
                        if matches_method(&b.quota, &method) {
                            b.used.push_back(Usage {
                                id,
                                dispatched_at: None,
                            });
                        }
                    }
                    s.waiters.retain(|(i, _, _)| *i != id);
                    s.urgent_streak = if urgent { s.urgent_streak + 1 } else { 0 };
                    self.changed.notify_waiters();
                    return Ok(QuotaReservation {
                        limiter: Arc::clone(self),
                        id,
                        committed: false,
                    });
                }
                wake
            };
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep_until(wake) => {} }
        }
    }
}

pub(crate) struct QuotaReservation {
    limiter: Arc<TbankRateLimiter>,
    id: u64,
    committed: bool,
}
impl QuotaReservation {
    pub(crate) fn commit(mut self) {
        {
            let mut s = self.limiter.state.lock().expect("RPC quota lock poisoned");
            let now = Instant::now();
            for b in &mut s.buckets {
                for used in &mut b.used {
                    if used.id == self.id {
                        used.dispatched_at = Some(now);
                    }
                }
            }
        }
        self.committed = true;
        self.limiter.changed.notify_waiters();
    }
}
impl Drop for QuotaReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut s = self.limiter.state.lock().expect("RPC quota lock poisoned");
            for b in &mut s.buckets {
                b.used.retain(|used| used.id != self.id);
            }
            drop(s);
            self.limiter.changed.notify_waiters();
        }
    }
}

struct Ticket {
    limiter: Arc<TbankRateLimiter>,
    id: u64,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.limiter
            .state
            .lock()
            .expect("RPC quota lock poisoned")
            .waiters
            .retain(|(i, _, _)| *i != self.id);
        self.limiter.changed.notify_waiters();
    }
}
fn matches_method(q: &TbankRpcQuota, method: &str) -> bool {
    q.methods.is_empty()
        || q.methods
            .iter()
            .any(|m| canonical_method(method_name(m)) == method)
}
pub(crate) fn method_name(path: &str) -> &str {
    path.rsplit(['/', '.']).next().unwrap_or(path)
}
fn canonical_method(method: &str) -> String {
    method.replace("Sandbox", "")
}

/// Sequenced transport diagnostics; a lagged broadcast receiver reports the exact lost count.
#[derive(Clone, Debug)]
pub struct TbankRpcDiagnostic {
    /// Process-local sequence number shared by all scopes.
    pub sequence: u64,
    /// RPC correlation ID, constant across its stages.
    pub rpc_id: u64,
    /// RPC method (never request payload or account metadata).
    pub method: String,
    /// Stable stage name.
    pub stage: &'static str,
    /// UTC event timestamp in nanoseconds.
    pub utc_ns: u64,
    /// Duration from the process diagnostic origin, measured monotonically.
    pub monotonic_ns: u128,
    /// Nautilus command identity, when this RPC belongs to an execution command.
    pub client_order_id: Option<String>,
    /// Broker timestamp, when supplied; never used to measure local durations.
    pub broker_time_ns: Option<u64>,
    /// Bounded failure details. Never includes request payloads or metadata.
    pub failure: Option<TbankRpcFailure>,
}
/// Error context attached to a failed transport diagnostic.
#[derive(Clone, Debug)]
pub struct TbankRpcFailure {
    /// Transport phase where the RPC failed.
    pub phase: &'static str,
    /// gRPC status variant or `Transport` for an underlying channel error.
    pub code: String,
    /// Sanitized, bounded status message.
    pub message: String,
}
const MAX_RPC_FAILURE_MESSAGE_CHARS: usize = 512;
pub(crate) fn rpc_failure(phase: &'static str, status: &Status) -> TbankRpcFailure {
    rpc_failure_message(phase, format!("{:?}", status.code()), status.message())
}
pub(crate) fn rpc_failure_message(
    phase: &'static str,
    code: impl Into<String>,
    raw_message: &str,
) -> TbankRpcFailure {
    let mut chars = raw_message.chars().filter(|ch| !ch.is_control());
    let mut message = chars
        .by_ref()
        .take(MAX_RPC_FAILURE_MESSAGE_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        message.push('…');
    }
    TbankRpcFailure {
        phase,
        code: code.into(),
        message,
    }
}
fn diagnostics() -> &'static tokio::sync::broadcast::Sender<TbankRpcDiagnostic> {
    static CHANNEL: OnceLock<tokio::sync::broadcast::Sender<TbankRpcDiagnostic>> = OnceLock::new();
    CHANNEL.get_or_init(|| tokio::sync::broadcast::channel(4096).0)
}
/// Subscribe before submitting commands. Handle `RecvError::Lagged` as diagnostic loss.
pub fn subscribe_tbank_rpc_diagnostics() -> tokio::sync::broadcast::Receiver<TbankRpcDiagnostic> {
    diagnostics().subscribe()
}
#[cfg(test)]
pub(crate) fn diagnostic(rpc_id: u64, method: &str, stage: &'static str) {
    diagnostic_with_context(rpc_id, method, stage, None, None);
}
pub(crate) fn diagnostic_with_context(
    rpc_id: u64,
    method: &str,
    stage: &'static str,
    client_order_id: Option<&str>,
    broker_time_ns: Option<u64>,
) {
    diagnostic_with_failure(rpc_id, method, stage, client_order_id, broker_time_ns, None);
}
pub(crate) fn diagnostic_failure_with_context(
    rpc_id: u64,
    method: &str,
    client_order_id: Option<&str>,
    failure: TbankRpcFailure,
) {
    diagnostic_with_failure(
        rpc_id,
        method,
        "rpc_failed",
        client_order_id,
        None,
        Some(failure),
    );
}
fn diagnostic_with_failure(
    rpc_id: u64,
    method: &str,
    stage: &'static str,
    client_order_id: Option<&str>,
    broker_time_ns: Option<u64>,
    failure: Option<TbankRpcFailure>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
    let _ = diagnostics().send(TbankRpcDiagnostic {
        sequence: SEQUENCE.fetch_add(1, Ordering::Relaxed),
        rpc_id,
        method: method.to_string(),
        stage,
        utc_ns: nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64(),
        client_order_id: client_order_id.map(str::to_owned),
        broker_time_ns,
        monotonic_ns: ORIGIN
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_nanos(),
        failure,
    });
}

#[derive(Default)]
pub(crate) struct Scope {
    pub(crate) limiter: tokio::sync::OnceCell<Arc<TbankRateLimiter>>,
}
pub(crate) fn shared_scope(key: u64) -> Arc<Scope> {
    static SCOPES: OnceLock<Mutex<HashMap<u64, Arc<Scope>>>> = OnceLock::new();
    Arc::clone(
        SCOPES
            .get_or_init(Mutex::default)
            .lock()
            .expect("quota scope lock")
            .entry(key)
            .or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rpc_failure_details_keep_code_and_bound_the_message() {
        let status = Status::unavailable(format!("{}\nextra", "x".repeat(520)));
        let failure = rpc_failure("await_headers", &status);

        assert_eq!(failure.phase, "await_headers");
        assert_eq!(failure.code, "Unavailable");
        assert_eq!(
            failure.message.chars().count(),
            MAX_RPC_FAILURE_MESSAGE_CHARS + 1
        );
        assert!(failure.message.ends_with('…'));
        assert!(!failure.message.chars().any(char::is_control));
    }

    fn quota(methods: &[&str], capacity: usize) -> TbankRpcQuota {
        TbankRpcQuota {
            methods: methods.iter().map(|s| s.to_string()).collect(),
            capacity,
            window: Duration::from_millis(40),
        }
    }
    #[tokio::test]
    async fn external_occupancy_is_refreshed_without_replacing_local_lifetime_permits() {
        use super::super::generated::{GetUserTariffResponse, StreamLimit, UnaryLimit};
        let mut tariff = GetUserTariffResponse {
            unary_limits: vec![UnaryLimit {
                limit_per_minute: 100,
                methods: vec!["GetUserTariff".into()],
                limit_per_second: None,
            }],
            stream_limits: vec![StreamLimit {
                limit: 2,
                open: 2,
                streams: vec!["MarketDataStream".into()],
            }],
        };
        let limiter = TbankRateLimiter::from_tariff(&tariff).unwrap();
        assert!(!TbankRateLimiter::stream_available(&tariff, "MarketDataStream").unwrap());
        tariff.stream_limits[0].open = 0;
        assert!(TbankRateLimiter::stream_available(&tariff, "MarketDataStream").unwrap());
        let first = limiter
            .reserve_stream("MarketDataStream", Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        // The snapshot now includes our first open stream; it must not be subtracted
        // from local capacity a second time.
        tariff.stream_limits[0].open = 1;
        assert!(TbankRateLimiter::stream_available(&tariff, "MarketDataStream").unwrap());
        let second = limiter
            .reserve_stream("MarketDataStream", Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            limiter
                .reserve_stream(
                    "MarketDataStream",
                    Instant::now() + Duration::from_millis(1)
                )
                .await
                .is_err()
        );
        drop(first);
        limiter
            .reserve_stream("MarketDataStream", Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        drop(second);
        tariff.stream_limits.push(StreamLimit {
            limit: 1,
            open: 1,
            streams: vec!["MarketDataStream".into()],
        });
        assert!(!TbankRateLimiter::stream_available(&tariff, "MarketDataStream").unwrap());
        assert!(TbankRateLimiter::stream_available(&tariff, "UnknownStream").is_err());
    }
    #[tokio::test]
    async fn overlapping_quotas_are_reserved_atomically_across_clones() {
        let limiter = Arc::new(
            TbankRateLimiter::new(
                vec![quota(&[], 2), quota(&["PostOrder", "PostOrderAsync"], 1)],
                8,
            )
            .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        let start = Instant::now();
        let (a, b) = tokio::join!(
            limiter.reserve("PostOrder", false, deadline),
            limiter.reserve("PostOrderAsync", false, deadline)
        );
        a.unwrap();
        b.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(40));
        let s = limiter.state.lock().unwrap();
        assert_eq!(s.buckets[1].used.len(), 1);
    }
    #[tokio::test]
    async fn expired_and_cancelled_waiters_do_not_consume_quota_or_block_recovery() {
        let limiter = Arc::new(TbankRateLimiter::new(vec![quota(&[], 1)], 2).unwrap());
        limiter
            .reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            limiter
                .reserve(
                    "PostOrder",
                    false,
                    Instant::now() + Duration::from_millis(1)
                )
                .await
                .is_err()
        );
        let pending = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move {
                limiter
                    .reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
                    .await
            })
        };
        tokio::task::yield_now().await;
        pending.abort();
        let _ = pending.await;
        limiter
            .reserve(
                "GetOrderState",
                true,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(limiter.state.lock().unwrap().waiters.is_empty());
    }
    #[tokio::test]
    async fn bounded_queue_and_urgent_admission() {
        let limiter = Arc::new(TbankRateLimiter::new(vec![quota(&[], 1)], 1).unwrap());
        limiter
            .reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let task = {
            let l = Arc::clone(&limiter);
            tokio::spawn(async move {
                l.reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(
            limiter
                .reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err()
                .to_string()
                .contains("full")
        );
        limiter
            .reserve("CancelOrder", true, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn diagnostics_expose_lost_events() {
        let mut receiver = subscribe_tbank_rpc_diagnostics();
        for _ in 0..5000 {
            diagnostic(1, "PostOrder", "test");
        }
        assert!(matches!(
            receiver.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
    }
    #[tokio::test]
    async fn admission_is_refundable_and_rate_window_starts_at_dispatch() {
        let limiter = Arc::new(TbankRateLimiter::new(vec![quota(&[], 1)], 4).unwrap());
        let admitted = limiter
            .reserve_guard("PostOrder", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            limiter
                .reserve(
                    "PostOrder",
                    false,
                    Instant::now() + Duration::from_millis(1)
                )
                .await
                .is_err(),
            "tentative capacity must not expire while channel readiness waits"
        );
        admitted.commit();
        assert!(
            limiter
                .reserve(
                    "PostOrder",
                    false,
                    Instant::now() + Duration::from_millis(1)
                )
                .await
                .is_err(),
            "rate window begins at dispatch, not initial admission"
        );
        limiter
            .reserve("PostOrder", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let other = Arc::new(TbankRateLimiter::new(vec![quota(&[], 1)], 4).unwrap());
        let admitted = other
            .reserve_guard("PostOrder", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        drop(admitted);
        other
            .reserve("CancelOrder", true, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
}
