use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use nautilus_common::messages::DataEvent;
use nautilus_model::data::Data;
use tokio::sync::Mutex;

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

/// A client-wide pacing gate shared by every historical candle producer.
///
/// T-Bank applies request limits per token, not per stream. Keeping the next request timestamp
/// behind one mutex prevents concurrent lifecycle recovery groups and periodic polling from
/// multiplying the configured request rate.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalRequestLimiter {
    interval: Duration,
    last_request_at: Arc<Mutex<Option<Instant>>>,
}

impl HistoricalRequestLimiter {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_request_at: Arc::new(Mutex::new(None)),
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
    /// Recovers broker candles for an explicit lifecycle or polling interval.
    pub async fn recover_range(
        &mut self,
        instrument_uid: &str,
        bar_type: nautilus_model::data::BarType,
        from_ts_event: i128,
        to_ts_event: i128,
        sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ) -> anyhow::Result<Vec<i128>> {
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
        let published = backfilled
            .iter()
            .filter(|bar| {
                let ts_event = i128::from(bar.ts_event.as_u64());
                ts_event >= from_ts_event && ts_event <= to_ts_event
            })
            .map(|bar| i128::from(bar.ts_event.as_u64()))
            .collect::<Vec<_>>();
        for bar in backfilled.iter().filter(|bar| {
            let ts_event = i128::from(bar.ts_event.as_u64());
            ts_event >= from_ts_event && ts_event <= to_ts_event
        }) {
            sender
                .send(DataEvent::Data(Data::from(*bar)))
                .map_err(|error| anyhow::anyhow!("data event receiver dropped: {error}"))?;
        }
        Ok(published)
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

pub(crate) fn retryable_stream_error_text(error: &str) -> bool {
    error.contains("h2 protocol error")
        || error.contains("ResourceExhausted")
        || error.contains("Unavailable")
        || error.contains("Unknown")
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

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
    fn retryable_stream_error_text_covers_transport_strings() {
        assert!(retryable_stream_error_text(
            "h2 protocol error: stream closed"
        ));
        assert!(retryable_stream_error_text("status: ResourceExhausted"));
        assert!(retryable_stream_error_text("status: Unavailable"));
        assert!(retryable_stream_error_text("status: Unknown"));
        assert!(!retryable_stream_error_text("PermissionDenied"));
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
