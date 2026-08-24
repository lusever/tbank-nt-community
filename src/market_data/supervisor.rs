use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use nautilus_model::data::Bar;
use tokio::sync::{Mutex, watch};

use crate::{
    common::time::unix_nanos_to_timestamp,
    config::TbankCandleTimestampMode,
    grpc::{
        generated::{
            CandleInterval, GetCandlesRequest, get_candles_request,
            market_data_service_client::MarketDataServiceClient,
        },
        with_timeout,
    },
    market_data::{
        MarketDataInstrumentMetadata,
        candles::{ONE_MINUTE_NANOS, one_minute_candle_query_chunks},
        client::{nautilus_bar_from_candle, now_unix_nanos},
    },
};

pub(crate) type MarketDataClient = MarketDataServiceClient<
    tonic::codegen::InterceptedService<
        tonic::transport::Channel,
        crate::grpc::TbankAuthInterceptor,
    >,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryPublication {
    Published,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryRangeResult {
    Published(Vec<i128>),
    Superseded,
}

/// A client-wide pacing gate shared by every historical candle producer.
///
/// T-Bank applies request limits per token, not per stream. Keeping the next request timestamp
/// behind one mutex prevents concurrent lifecycle recovery groups and periodic polling from
/// multiplying the configured request rate.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalRequestLimiter {
    interval: Duration,
    last_request_at: Arc<Mutex<Option<Instant>>>,
    recovery_state: Arc<HistoricalRecoveryState>,
}

#[derive(Debug)]
struct HistoricalRecoveryState {
    flights: HistoricalRecoveryFlights,
    circuit: Arc<StdMutex<HistoricalCircuitState>>,
}

#[derive(Clone, Debug, Eq)]
struct HistoricalRecoveryKey {
    instrument_uid: String,
    bar_type: nautilus_model::data::BarType,
    from_ts_event: i128,
    to_ts_event: i128,
}

impl PartialEq for HistoricalRecoveryKey {
    fn eq(&self, other: &Self) -> bool {
        self.instrument_uid == other.instrument_uid
            && self.bar_type == other.bar_type
            && self.from_ts_event == other.from_ts_event
            && self.to_ts_event == other.to_ts_event
    }
}

impl Hash for HistoricalRecoveryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.instrument_uid.hash(state);
        self.bar_type.hash(state);
        self.from_ts_event.hash(state);
        self.to_ts_event.hash(state);
    }
}

#[derive(Debug)]
struct HistoricalRecoveryFlight {
    state: watch::Sender<HistoricalRecoveryFlightState>,
    // Keep one receiver alive for the lifetime of the flight. Without it, a sender-only flight
    // would be considered closed before followers can subscribe.
    _receiver: watch::Receiver<HistoricalRecoveryFlightState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalRecoveryFlightState {
    Pending,
    Completed(HistoricalRecoveryCompletion),
}

type HistoricalRecoveryFlights =
    Arc<StdMutex<HashMap<HistoricalRecoveryKey, Arc<HistoricalRecoveryFlight>>>>;

struct HistoricalRecoveryGuard {
    flight: Arc<HistoricalRecoveryFlight>,
    flights: HistoricalRecoveryFlights,
    key: HistoricalRecoveryKey,
    circuit: Arc<StdMutex<HistoricalCircuitState>>,
    half_open: bool,
}

impl HistoricalRecoveryGuard {
    fn new(
        flights: HistoricalRecoveryFlights,
        key: HistoricalRecoveryKey,
        flight: Arc<HistoricalRecoveryFlight>,
        circuit: Arc<StdMutex<HistoricalCircuitState>>,
        half_open: bool,
    ) -> Self {
        Self {
            flight,
            flights,
            key,
            circuit,
            half_open,
        }
    }
}

impl Drop for HistoricalRecoveryGuard {
    fn drop(&mut self) {
        // Cancellation can race with finish_recovery after it publishes a terminal result but
        // before the owner future returns. Never turn a completed flight back into a cancellation.
        let cancelled = matches!(
            &*self.flight.state.borrow(),
            HistoricalRecoveryFlightState::Pending
        );
        if cancelled {
            let _ = self
                .flight
                .state
                .send(HistoricalRecoveryFlightState::Completed(
                    HistoricalRecoveryCompletion::Cancelled,
                ));
        }

        if self.half_open && cancelled {
            // A cancelled half-open probe is not a failed probe. Release the circuit immediately
            // so the next recovery owner can retry instead of inheriting another full cooldown.
            let mut circuit = self
                .circuit
                .lock()
                .expect("historical recovery circuit lock");
            circuit.opened_at = None;
            circuit.consecutive_failures = 0;
        }

        let mut flights = self
            .flights
            .lock()
            .expect("historical recovery flights lock");
        if flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            flights.remove(&self.key);
        }
    }
}

#[derive(Debug)]
struct HistoricalCircuitState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

const HISTORICAL_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const HISTORICAL_CIRCUIT_COOLDOWN: Duration = Duration::from_secs(30);

enum HistoricalRecoveryPermit {
    Owner {
        key: HistoricalRecoveryKey,
        flight: Arc<HistoricalRecoveryFlight>,
        half_open: bool,
    },
    Follower {
        flight: Arc<HistoricalRecoveryFlight>,
    },
}

impl HistoricalRequestLimiter {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_request_at: Arc::new(Mutex::new(None)),
            recovery_state: Arc::new(HistoricalRecoveryState {
                flights: Arc::new(StdMutex::new(HashMap::new())),
                circuit: Arc::new(StdMutex::new(HistoricalCircuitState {
                    consecutive_failures: 0,
                    opened_at: None,
                })),
            }),
        }
    }

    pub(crate) async fn acquire(&self) {
        if self.interval.is_zero() {
            return;
        }
        let mut last_request_at = self.last_request_at.lock().await;
        if let Some(last_request_at) = *last_request_at {
            tokio::time::sleep(self.interval.saturating_sub(last_request_at.elapsed())).await;
        }
        *last_request_at = Some(Instant::now());
    }

    fn begin_recovery(
        &self,
        key: HistoricalRecoveryKey,
    ) -> anyhow::Result<HistoricalRecoveryPermit> {
        {
            let mut flights = self
                .recovery_state
                .flights
                .lock()
                .expect("historical recovery flights lock");
            if let Some(flight) = flights.get(&key).cloned() {
                if matches!(
                    &*flight.state.borrow(),
                    HistoricalRecoveryFlightState::Pending
                ) {
                    return Ok(HistoricalRecoveryPermit::Follower { flight });
                }
                flights.remove(&key);
            }
        }

        let mut circuit = self
            .recovery_state
            .circuit
            .lock()
            .expect("historical recovery circuit lock");
        let half_open = if let Some(opened_at) = circuit.opened_at {
            if opened_at.elapsed() < HISTORICAL_CIRCUIT_COOLDOWN {
                anyhow::bail!("historical candle recovery circuit is open")
            }
            // One caller is allowed to be the half-open probe. Keep the circuit open until it
            // reports success or failure below.
            circuit.opened_at = Some(Instant::now());
            true
        } else {
            false
        };
        drop(circuit);

        let mut flights = self
            .recovery_state
            .flights
            .lock()
            .expect("historical recovery flights lock");
        if let Some(flight) = flights.get(&key).cloned() {
            if matches!(
                &*flight.state.borrow(),
                HistoricalRecoveryFlightState::Pending
            ) {
                return Ok(HistoricalRecoveryPermit::Follower { flight });
            }
            flights.remove(&key);
        }
        let (state, receiver) = watch::channel(HistoricalRecoveryFlightState::Pending);
        let flight = Arc::new(HistoricalRecoveryFlight {
            state,
            _receiver: receiver,
        });
        flights.insert(key.clone(), flight.clone());
        Ok(HistoricalRecoveryPermit::Owner {
            key,
            flight,
            half_open,
        })
    }

    fn finish_recovery(
        &self,
        key: HistoricalRecoveryKey,
        flight: &Arc<HistoricalRecoveryFlight>,
        completion: HistoricalRecoveryCompletion,
        half_open: bool,
    ) {
        let _ = flight
            .state
            .send(HistoricalRecoveryFlightState::Completed(completion));

        let mut circuit = self
            .recovery_state
            .circuit
            .lock()
            .expect("historical recovery circuit lock");
        match completion {
            HistoricalRecoveryCompletion::Succeeded => {
                circuit.consecutive_failures = 0;
                circuit.opened_at = None;
            }
            HistoricalRecoveryCompletion::Failed => {
                circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
                if circuit.consecutive_failures >= HISTORICAL_CIRCUIT_FAILURE_THRESHOLD {
                    circuit.opened_at = Some(Instant::now());
                }
            }
            HistoricalRecoveryCompletion::Cancelled => {
                if half_open {
                    // Subscription replacement is not a broker failure. Release a half-open
                    // probe so a current generation can retry immediately.
                    circuit.opened_at = None;
                    circuit.consecutive_failures = 0;
                }
            }
        }
        drop(circuit);

        let mut flights = self
            .recovery_state
            .flights
            .lock()
            .expect("historical recovery flights lock");
        if flights
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, flight))
        {
            flights.remove(&key);
        }
    }

    async fn await_recovery(
        flight: Arc<HistoricalRecoveryFlight>,
    ) -> anyhow::Result<HistoricalRecoveryCompletion> {
        let mut state = flight.state.subscribe();
        loop {
            let current_state = *state.borrow();
            match current_state {
                HistoricalRecoveryFlightState::Completed(completion) => return Ok(completion),
                HistoricalRecoveryFlightState::Pending => {
                    if state.changed().await.is_err() {
                        anyhow::bail!("historical recovery flight was dropped before completion");
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoricalRecoveryCompletion {
    Succeeded,
    Failed,
    Cancelled,
}

pub(crate) struct BackfillCoordinator {
    pub market_data_client: MarketDataClient,
    pub timestamp_mode: TbankCandleTimestampMode,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_base_delay: Duration,
    pub require_complete_candles: bool,
    pub instrument_metadata: HashMap<String, MarketDataInstrumentMetadata>,
    pub request_limiter: HistoricalRequestLimiter,
}

impl BackfillCoordinator {
    /// Recovers broker candles and invokes `publish` after conversion succeeds.
    ///
    /// The callback owns the lifecycle-current check and must commit any continuity cursor only
    /// after all prepared bars were accepted by the Nautilus data channel, under that same guard.
    pub async fn recover_range<F>(
        &mut self,
        instrument_uid: &str,
        bar_type: nautilus_model::data::BarType,
        from_ts_event: i128,
        to_ts_event: i128,
        publish: F,
    ) -> anyhow::Result<RecoveryRangeResult>
    where
        F: FnMut(&[Bar]) -> anyhow::Result<RecoveryPublication> + Send,
    {
        let recovery_key = HistoricalRecoveryKey {
            instrument_uid: instrument_uid.to_string(),
            bar_type,
            from_ts_event,
            to_ts_event,
        };
        let mut publish = publish;
        loop {
            let permit = self.request_limiter.begin_recovery(recovery_key.clone())?;
            if let HistoricalRecoveryPermit::Follower { flight } = permit {
                match HistoricalRequestLimiter::await_recovery(flight).await? {
                    HistoricalRecoveryCompletion::Succeeded => {
                        // The owner already published the range. Invoke the callback with an
                        // empty batch so each follower can commit its own local continuity
                        // cursor through the same lifecycle guard, without publishing twice.
                        let publication = publish(&[])?;
                        return match publication {
                            RecoveryPublication::Published => {
                                Ok(RecoveryRangeResult::Published(Vec::new()))
                            }
                            RecoveryPublication::Superseded => Ok(RecoveryRangeResult::Superseded),
                        };
                    }
                    HistoricalRecoveryCompletion::Cancelled => {
                        // The owner was superseded by a newer subscription snapshot. This is
                        // not a broker failure: the current follower must become the next owner
                        // and retry the same range under the current lifecycle generation.
                        continue;
                    }
                    HistoricalRecoveryCompletion::Failed => {
                        anyhow::bail!("coalesced historical candle recovery failed");
                    }
                }
            }
            let HistoricalRecoveryPermit::Owner {
                key,
                flight,
                half_open,
            } = permit
            else {
                unreachable!("historical recovery permit was handled above")
            };
            let _flight_guard = HistoricalRecoveryGuard::new(
                self.request_limiter.recovery_state.flights.clone(),
                key.clone(),
                flight.clone(),
                self.request_limiter.recovery_state.circuit.clone(),
                half_open,
            );
            let result = match self
                .recover_range_uncoordinated(instrument_uid, bar_type, from_ts_event, to_ts_event)
                .await
            {
                Ok(backfilled) => {
                    let backfilled = backfilled
                        .into_iter()
                        .filter(|bar| {
                            let ts_event = i128::from(bar.ts_event.as_u64());
                            ts_event >= from_ts_event && ts_event <= to_ts_event
                        })
                        .collect::<Vec<_>>();
                    let published_ts = backfilled
                        .iter()
                        .map(|bar| i128::from(bar.ts_event.as_u64()))
                        .collect::<Vec<_>>();
                    publish(&backfilled).map(|publication| match publication {
                        RecoveryPublication::Published => {
                            RecoveryRangeResult::Published(published_ts)
                        }
                        RecoveryPublication::Superseded => RecoveryRangeResult::Superseded,
                    })
                }
                Err(error) => Err(error),
            };
            let completion = match &result {
                Ok(RecoveryRangeResult::Published(_)) => HistoricalRecoveryCompletion::Succeeded,
                Ok(RecoveryRangeResult::Superseded) => HistoricalRecoveryCompletion::Cancelled,
                Err(_) => HistoricalRecoveryCompletion::Failed,
            };
            self.request_limiter
                .finish_recovery(key, &flight, completion, half_open);
            return result;
        }
    }

    async fn recover_range_uncoordinated(
        &mut self,
        instrument_uid: &str,
        bar_type: nautilus_model::data::BarType,
        from_ts_event: i128,
        to_ts_event: i128,
    ) -> anyhow::Result<Vec<Bar>> {
        let (request_from, request_to) =
            candle_query_bounds(self.timestamp_mode, from_ts_event, to_ts_event);
        let mut backfilled = Vec::new();
        for (chunk_from, chunk_to) in one_minute_candle_query_chunks(request_from, request_to) {
            let request = GetCandlesRequest {
                #[allow(deprecated)]
                figi: None,
                from: Some(unix_nanos_to_timestamp(chunk_from)?),
                to: Some(unix_nanos_to_timestamp(chunk_to)?),
                interval: CandleInterval::CandleInterval1Min as i32,
                instrument_id: Some(instrument_uid.to_string()),
                candle_source_type: Some(get_candles_request::CandleSource::Exchange as i32),
                limit: None,
            };
            let response = {
                let mut attempt = 0;
                loop {
                    self.request_limiter.acquire().await;
                    match tokio::time::timeout(
                        self.request_timeout,
                        self.market_data_client
                            .get_candles(with_timeout(request.clone(), self.request_timeout)),
                    )
                    .await
                    {
                        Ok(Ok(response)) => break response.into_inner(),
                        Ok(Err(status))
                            if attempt < self.max_retries && retryable_stream_status(&status) =>
                        {
                            let delay = historical_retry_delay(
                                Some(&status),
                                attempt,
                                self.retry_base_delay,
                            );
                            tracing::warn!(
                                instrument_uid,
                                attempt = attempt + 1,
                                delay_ms = delay.as_millis(),
                                code = ?status.code(),
                                "retrying lifecycle GetCandles request"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        Err(_) if attempt < self.max_retries => {
                            let delay =
                                historical_retry_delay(None, attempt, self.retry_base_delay);
                            tracing::warn!(
                                instrument_uid,
                                attempt = attempt + 1,
                                delay_ms = delay.as_millis(),
                                "retrying lifecycle GetCandles request after timeout"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        Ok(Err(status)) => return Err(status.into()),
                        Err(_) => {
                            anyhow::bail!(
                                "T-Bank GetCandles timed out after {} ms",
                                self.request_timeout.as_millis()
                            );
                        }
                    }
                }
            };
            for candle in response.candles.into_iter().filter(|candle| {
                historic_candle_is_eligible(candle.is_complete, self.require_complete_candles)
            }) {
                let candle = crate::grpc::generated::Candle {
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: candle.volume,
                    time: candle.time,
                    instrument_uid: instrument_uid.to_string(),
                    ..crate::grpc::generated::Candle::default()
                };
                let metadata = self
                    .instrument_metadata
                    .get(instrument_uid)
                    .copied()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "missing market-data metadata for T-Bank instrument {instrument_uid}"
                        )
                    })?;
                backfilled.push(nautilus_bar_from_candle(
                    &candle,
                    bar_type,
                    metadata,
                    self.timestamp_mode,
                    now_unix_nanos(),
                )?);
            }
        }
        backfilled.sort_by_key(|bar| bar.ts_event);
        backfilled.dedup_by_key(|bar| bar.ts_event);
        Ok(backfilled)
    }
}

fn historical_retry_delay(
    status: Option<&tonic::Status>,
    attempt: u32,
    base_delay: Duration,
) -> Duration {
    if let Some(status) = status
        && status.code() == tonic::Code::ResourceExhausted
        && let Some(reset_seconds) = status
            .metadata()
            .get("x-ratelimit-reset")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(reset_seconds.saturating_add(1).min(300));
    }
    base_delay.saturating_mul(1_u32 << attempt.min(6))
}

fn historic_candle_is_eligible(is_complete: bool, require_complete: bool) -> bool {
    !require_complete || is_complete
}

fn candle_query_bounds(
    timestamp_mode: TbankCandleTimestampMode,
    from_ts_event: i128,
    to_ts_event: i128,
) -> (i128, i128) {
    match timestamp_mode {
        TbankCandleTimestampMode::StartAsBarEnd => {
            (from_ts_event.saturating_sub(ONE_MINUTE_NANOS), to_ts_event)
        }
        TbankCandleTimestampMode::StartAsBarStart => {
            (from_ts_event, to_ts_event.saturating_add(ONE_MINUTE_NANOS))
        }
    }
}

pub(crate) fn retryable_stream_status(status: &tonic::Status) -> bool {
    crate::grpc::retry::is_transient_status(status.code())
        || status.to_string().contains("h2 protocol error")
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use nautilus_model::{
        data::{BarSpecification, BarType},
        enums::{AggregationSource, BarAggregation, PriceType},
    };
    use tonic::{Code, Status};

    use super::*;

    #[tokio::test]
    async fn historical_request_limiter_is_shared_by_clones() {
        let limiter = HistoricalRequestLimiter::new(Duration::from_millis(20));
        limiter.acquire().await;
        let clone = limiter.clone();
        let started = Instant::now();

        clone.acquire().await;

        assert!(started.elapsed() >= Duration::from_millis(15));
    }

    fn recovery_key(from_ts_event: i128) -> HistoricalRecoveryKey {
        HistoricalRecoveryKey {
            instrument_uid: "uid-1".to_string(),
            bar_type: BarType::new(
                "SBER_TQBR.MOEX".parse().unwrap(),
                BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
                AggregationSource::External,
            ),
            from_ts_event,
            to_ts_event: from_ts_event + ONE_MINUTE_NANOS,
        }
    }

    #[tokio::test]
    async fn historical_recovery_requests_are_single_flighted() {
        let limiter = HistoricalRequestLimiter::new(Duration::ZERO);
        let owner = limiter.begin_recovery(recovery_key(0)).unwrap();
        let follower = limiter.begin_recovery(recovery_key(0)).unwrap();
        let HistoricalRecoveryPermit::Owner { key, flight, .. } = owner else {
            panic!("first recovery request must own the flight");
        };
        let HistoricalRecoveryPermit::Follower {
            flight: follower_flight,
        } = follower
        else {
            panic!("second recovery request must join the flight");
        };

        limiter.finish_recovery(key, &flight, HistoricalRecoveryCompletion::Succeeded, false);
        assert_eq!(
            HistoricalRequestLimiter::await_recovery(follower_flight)
                .await
                .unwrap(),
            HistoricalRecoveryCompletion::Succeeded
        );
    }

    #[tokio::test]
    async fn dropped_recovery_owner_unblocks_followers_and_releases_key() {
        let limiter = HistoricalRequestLimiter::new(Duration::ZERO);
        let key = recovery_key(0);
        let HistoricalRecoveryPermit::Owner {
            key: owner_key,
            flight,
            half_open,
        } = limiter.begin_recovery(key.clone()).unwrap()
        else {
            panic!("first recovery request must own the flight");
        };
        let HistoricalRecoveryPermit::Follower { flight: follower } =
            limiter.begin_recovery(key.clone()).unwrap()
        else {
            panic!("second recovery request must join the flight");
        };

        {
            let _guard = HistoricalRecoveryGuard::new(
                limiter.recovery_state.flights.clone(),
                owner_key,
                flight,
                limiter.recovery_state.circuit.clone(),
                half_open,
            );
        }

        assert_eq!(
            HistoricalRequestLimiter::await_recovery(follower)
                .await
                .unwrap(),
            HistoricalRecoveryCompletion::Cancelled
        );
        assert!(matches!(
            limiter.begin_recovery(key).unwrap(),
            HistoricalRecoveryPermit::Owner { .. }
        ));
    }

    #[tokio::test]
    async fn dropped_recovery_owner_does_not_overwrite_completed_flight() {
        let limiter = HistoricalRequestLimiter::new(Duration::ZERO);
        let key = recovery_key(0);
        let HistoricalRecoveryPermit::Owner {
            key: owner_key,
            flight,
            half_open,
        } = limiter.begin_recovery(key.clone()).unwrap()
        else {
            panic!("first recovery request must own the flight");
        };
        let HistoricalRecoveryPermit::Follower { flight: follower } =
            limiter.begin_recovery(key).unwrap()
        else {
            panic!("second recovery request must join the flight");
        };
        let guard = HistoricalRecoveryGuard::new(
            limiter.recovery_state.flights.clone(),
            owner_key,
            flight.clone(),
            limiter.recovery_state.circuit.clone(),
            half_open,
        );

        limiter.finish_recovery(
            recovery_key(0),
            &flight,
            HistoricalRecoveryCompletion::Succeeded,
            half_open,
        );
        drop(guard);

        assert_eq!(
            *flight.state.borrow(),
            HistoricalRecoveryFlightState::Completed(HistoricalRecoveryCompletion::Succeeded)
        );
        assert_eq!(
            HistoricalRequestLimiter::await_recovery(follower)
                .await
                .unwrap(),
            HistoricalRecoveryCompletion::Succeeded
        );
    }

    #[tokio::test]
    async fn historical_recovery_circuit_breaker_opens_after_failures() {
        let limiter = HistoricalRequestLimiter::new(Duration::ZERO);
        for index in 0..HISTORICAL_CIRCUIT_FAILURE_THRESHOLD {
            let key = recovery_key(i128::from(index) * ONE_MINUTE_NANOS);
            let HistoricalRecoveryPermit::Owner { key, flight, .. } =
                limiter.begin_recovery(key).unwrap()
            else {
                panic!("failure probe must own its recovery flight");
            };
            limiter.finish_recovery(key, &flight, HistoricalRecoveryCompletion::Failed, false);
        }

        assert!(limiter.begin_recovery(recovery_key(99)).is_err());
    }

    #[tokio::test]
    async fn dropped_half_open_probe_releases_circuit_without_cooldown() {
        let limiter = HistoricalRequestLimiter::new(Duration::ZERO);
        {
            let mut circuit = limiter
                .recovery_state
                .circuit
                .lock()
                .expect("historical recovery circuit lock");
            circuit.consecutive_failures = HISTORICAL_CIRCUIT_FAILURE_THRESHOLD;
            circuit.opened_at = Some(Instant::now() - HISTORICAL_CIRCUIT_COOLDOWN);
        }

        let HistoricalRecoveryPermit::Owner {
            key,
            flight,
            half_open,
        } = limiter.begin_recovery(recovery_key(0)).unwrap()
        else {
            panic!("cooldown expiry must grant the half-open probe");
        };
        assert!(half_open);

        {
            let _guard = HistoricalRecoveryGuard::new(
                limiter.recovery_state.flights.clone(),
                key,
                flight,
                limiter.recovery_state.circuit.clone(),
                half_open,
            );
        }

        let circuit = limiter
            .recovery_state
            .circuit
            .lock()
            .expect("historical recovery circuit lock");
        assert!(circuit.opened_at.is_none());
        assert_eq!(circuit.consecutive_failures, 0);
        drop(circuit);
        assert!(matches!(
            limiter.begin_recovery(recovery_key(1)).unwrap(),
            HistoricalRecoveryPermit::Owner { .. }
        ));
    }

    #[test]
    fn historical_retry_honors_broker_rate_limit_reset() {
        let mut status = Status::resource_exhausted("rate limited");
        status
            .metadata_mut()
            .insert("x-ratelimit-reset", "7".parse().unwrap());

        assert_eq!(
            historical_retry_delay(Some(&status), 0, Duration::from_secs(1)),
            Duration::from_secs(8)
        );
        assert_eq!(
            historical_retry_delay(None, 3, Duration::from_millis(250)),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn completed_only_backfill_rejects_open_candle() {
        assert!(historic_candle_is_eligible(true, true));
        assert!(!historic_candle_is_eligible(false, true));
        assert!(historic_candle_is_eligible(false, false));
    }

    #[test]
    fn retryable_stream_status_covers_tbank_disconnect_codes() {
        assert!(retryable_stream_status(&Status::new(
            Code::Unknown,
            "h2 protocol error"
        )));
        assert!(retryable_stream_status(&Status::unavailable(
            "tcp connect error"
        )));
        assert!(retryable_stream_status(&Status::resource_exhausted(
            "rate limit"
        )));
        assert!(!retryable_stream_status(&Status::permission_denied(
            "bad token"
        )));
    }

    #[test]
    fn candle_query_bounds_convert_bar_end_timestamps_to_candle_starts() {
        let minute_2 = ONE_MINUTE_NANOS * 2;
        let minute_3 = ONE_MINUTE_NANOS * 3;

        assert_eq!(
            candle_query_bounds(TbankCandleTimestampMode::StartAsBarEnd, minute_2, minute_3),
            (ONE_MINUTE_NANOS, minute_3)
        );
        assert_eq!(
            candle_query_bounds(
                TbankCandleTimestampMode::StartAsBarStart,
                minute_2,
                minute_3
            ),
            (minute_2, ONE_MINUTE_NANOS * 4)
        );
    }
}
