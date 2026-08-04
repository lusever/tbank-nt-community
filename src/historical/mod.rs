//! Historical request support used by the main T-Bank data client.

use std::{collections::BTreeSet, str::FromStr, time::Duration};

use anyhow::{Context, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, TradeTick},
    enums::{AggressorSide, BarAggregation},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    common::{
        decimal::quotation_to_decimal,
        ids::{TbankInstrumentIdParts, instrument_id_from_ticker_class},
        time::{timestamp_to_unix_nanos, unix_nanos_to_timestamp},
    },
    config::TbankCandleTimestampMode,
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients,
        generated::{
            CandleInterval, FindInstrumentRequest, GetCandlesRequest, GetLastTradesRequest,
            HistoricCandle, InstrumentIdType, InstrumentRequest, InstrumentShort, InstrumentType,
            Trade, TradeDirection, TradeSourceType, get_candles_request,
        },
        with_timeout,
    },
};

pub(crate) const DEFAULT_TRADE_SOURCE: TbankTradeSource = TbankTradeSource::All;
const MARKET_CONTEXT_ID: &str = "IMOEX2.MOEX";
const DEFAULT_GRPC_PRICE_PRECISION: u8 = 9;
const DEFAULT_GRPC_SIZE_PRECISION: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TbankBarPrecision {
    pub(crate) price: u8,
    pub(crate) size: u8,
}

impl Default for TbankBarPrecision {
    fn default() -> Self {
        Self {
            price: DEFAULT_GRPC_PRICE_PRECISION,
            size: DEFAULT_GRPC_SIZE_PRECISION,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum TbankTradeSource {
    All,
    Exchange,
    Dealer,
}

impl TbankTradeSource {
    const fn as_proto(self) -> TradeSourceType {
        match self {
            Self::All => TradeSourceType::TradeSourceAll,
            Self::Exchange => TradeSourceType::TradeSourceExchange,
            Self::Dealer => TradeSourceType::TradeSourceDealer,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum TbankBarInterval {
    Sec5,
    Sec10,
    Sec30,
    Min1,
    Min2,
    Min3,
    Min5,
    Min10,
    Min15,
    Min30,
    Hour1,
    Hour2,
    Hour4,
    Day1,
}

impl TbankBarInterval {
    const fn as_proto(self) -> CandleInterval {
        match self {
            Self::Sec5 => CandleInterval::CandleInterval5Sec,
            Self::Sec10 => CandleInterval::CandleInterval10Sec,
            Self::Sec30 => CandleInterval::CandleInterval30Sec,
            Self::Min1 => CandleInterval::CandleInterval1Min,
            Self::Min2 => CandleInterval::CandleInterval2Min,
            Self::Min3 => CandleInterval::CandleInterval3Min,
            Self::Min5 => CandleInterval::CandleInterval5Min,
            Self::Min10 => CandleInterval::CandleInterval10Min,
            Self::Min15 => CandleInterval::CandleInterval15Min,
            Self::Min30 => CandleInterval::CandleInterval30Min,
            Self::Hour1 => CandleInterval::Hour,
            Self::Hour2 => CandleInterval::CandleInterval2Hour,
            Self::Hour4 => CandleInterval::CandleInterval4Hour,
            Self::Day1 => CandleInterval::Day,
        }
    }

    const fn duration_nanos(self) -> u64 {
        match self {
            Self::Sec5 => 5_000_000_000,
            Self::Sec10 => 10_000_000_000,
            Self::Sec30 => 30_000_000_000,
            Self::Min1 => 60_000_000_000,
            Self::Min2 => 2 * 60_000_000_000,
            Self::Min3 => 3 * 60_000_000_000,
            Self::Min5 => 5 * 60_000_000_000,
            Self::Min10 => 10 * 60_000_000_000,
            Self::Min15 => 15 * 60_000_000_000,
            Self::Min30 => 30 * 60_000_000_000,
            Self::Hour1 => 60 * 60_000_000_000,
            Self::Hour2 => 2 * 60 * 60_000_000_000,
            Self::Hour4 => 4 * 60 * 60_000_000_000,
            Self::Day1 => 24 * 60 * 60_000_000_000,
        }
    }

    fn grpc_chunk(self) -> ChronoDuration {
        match self {
            Self::Sec5 | Self::Sec10 | Self::Sec30 | Self::Min1 => ChronoDuration::hours(24),
            Self::Min2 | Self::Min3 | Self::Min5 | Self::Min10 | Self::Min15 | Self::Min30 => {
                ChronoDuration::days(7)
            }
            Self::Hour1 | Self::Hour2 | Self::Hour4 => ChronoDuration::days(31),
            Self::Day1 => ChronoDuration::days(365),
        }
    }

    pub(crate) fn try_from_bar_type(bar_type: BarType) -> anyhow::Result<Self> {
        let spec = bar_type.spec();
        match (spec.aggregation, spec.step.get()) {
            (BarAggregation::Second, 5) => Ok(Self::Sec5),
            (BarAggregation::Second, 10) => Ok(Self::Sec10),
            (BarAggregation::Second, 30) => Ok(Self::Sec30),
            (BarAggregation::Minute, 1) => Ok(Self::Min1),
            (BarAggregation::Minute, 2) => Ok(Self::Min2),
            (BarAggregation::Minute, 3) => Ok(Self::Min3),
            (BarAggregation::Minute, 5) => Ok(Self::Min5),
            (BarAggregation::Minute, 10) => Ok(Self::Min10),
            (BarAggregation::Minute, 15) => Ok(Self::Min15),
            (BarAggregation::Minute, 30) => Ok(Self::Min30),
            (BarAggregation::Hour, 1) => Ok(Self::Hour1),
            (BarAggregation::Hour, 2) => Ok(Self::Hour2),
            (BarAggregation::Hour, 4) => Ok(Self::Hour4),
            (BarAggregation::Day, 1) => Ok(Self::Day1),
            _ => bail!("unsupported T-Bank bar type {bar_type}"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedTbankInstrument {
    pub(crate) instrument_uid: String,
    pub(crate) ticker: String,
    pub(crate) class_code: String,
    pub(crate) instrument_kind: String,
}

impl ResolvedTbankInstrument {
    fn stream_id(&self) -> String {
        if self.instrument_uid.is_empty() {
            format!("{}_{}", self.ticker, self.class_code)
        } else {
            self.instrument_uid.clone()
        }
    }
}

#[derive(Clone)]
pub(crate) struct TbankHistoricalClient {
    clients: TbankGrpcClients<TbankAuthInterceptor>,
    timestamp_mode: TbankCandleTimestampMode,
    request_timeout: Duration,
}

impl TbankHistoricalClient {
    pub(crate) fn new(
        clients: TbankGrpcClients<TbankAuthInterceptor>,
        timestamp_mode: TbankCandleTimestampMode,
        request_timeout: Duration,
    ) -> Self {
        Self {
            clients,
            timestamp_mode,
            request_timeout,
        }
    }

    pub(crate) async fn resolve_instrument(
        &mut self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<ResolvedTbankInstrument> {
        let id = instrument_id.to_string();
        if id == MARKET_CONTEXT_ID {
            return self.resolve_index("IMOEX2").await;
        }
        let parts = TbankInstrumentIdParts::from_str(&id)?;
        let response = self
            .clients
            .instruments
            .share_by(with_timeout(
                InstrumentRequest {
                    id_type: InstrumentIdType::Ticker as i32,
                    class_code: Some(parts.class_code.clone()),
                    id: parts.ticker,
                },
                self.request_timeout,
            ))
            .await?
            .into_inner();
        let share = response
            .instrument
            .context("ShareBy returned no instrument")?;
        Ok(ResolvedTbankInstrument {
            instrument_uid: share.uid,
            ticker: share.ticker,
            class_code: share.class_code,
            instrument_kind: "share".to_string(),
        })
    }

    async fn resolve_index(&mut self, ticker: &str) -> anyhow::Result<ResolvedTbankInstrument> {
        let response = self
            .clients
            .instruments
            .find_instrument(with_timeout(
                FindInstrumentRequest {
                    query: ticker.to_string(),
                    instrument_kind: Some(InstrumentType::Index as i32),
                    api_trade_available_flag: None,
                },
                self.request_timeout,
            ))
            .await?
            .into_inner();
        let instrument = find_exact_or_named(response.instruments, ticker)
            .with_context(|| format!("FindInstrument returned no {ticker} index"))?;
        Ok(ResolvedTbankInstrument {
            instrument_uid: instrument.uid,
            ticker: instrument.ticker,
            class_code: instrument.class_code,
            instrument_kind: "index".to_string(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_bars(
        &mut self,
        resolved: &ResolvedTbankInstrument,
        bar_type: BarType,
        interval: TbankBarInterval,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: Option<usize>,
        precision: Option<TbankBarPrecision>,
        retries: usize,
    ) -> anyhow::Result<Vec<Bar>> {
        let chunks = candle_chunks(from, to, interval)?;
        let mut bars = Vec::new();
        let precision = precision.unwrap_or_default();
        for (chunk_from, chunk_to) in chunks {
            let remaining = limit.map(|limit| limit.saturating_sub(bars.len()));
            if remaining == Some(0) {
                break;
            }
            let request = GetCandlesRequest {
                #[allow(deprecated)]
                figi: None,
                from: Some(unix_nanos_to_timestamp(datetime_to_nanos(chunk_from)?)?),
                to: Some(unix_nanos_to_timestamp(datetime_to_nanos(chunk_to)?)?),
                interval: interval.as_proto() as i32,
                instrument_id: Some(resolved.stream_id()),
                candle_source_type: Some(get_candles_request::CandleSource::Exchange as i32),
                limit: remaining.and_then(|value| i32::try_from(value).ok()),
            };
            let mut attempt = 0usize;
            let response = loop {
                match self
                    .clients
                    .market_data
                    .get_candles(with_timeout(request.clone(), self.request_timeout))
                    .await
                {
                    Ok(response) => break response.into_inner(),
                    Err(status)
                        if crate::grpc::retry::is_transient_status(status.code())
                            && attempt < retries =>
                    {
                        let delay = candle_retry_delay(&status, attempt);
                        attempt += 1;
                        tracing::warn!(
                            from = %chunk_from,
                            to = %chunk_to,
                            error = %status,
                            attempt,
                            retries,
                            delay_secs = delay.as_secs(),
                            "retrying T-Bank GetCandles request",
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Err(status) => return Err(status.into()),
                }
            };
            let force_zero_volume = resolved.instrument_kind == "index";
            let mut chunk_bars = response
                .candles
                .iter()
                .map(|candle| {
                    historic_candle_to_bar(
                        candle,
                        bar_type,
                        interval,
                        self.timestamp_mode,
                        force_zero_volume,
                        precision,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            bars.append(&mut chunk_bars);
        }
        dedup_bars(&mut bars);
        if let Some(limit) = limit {
            bars.truncate(limit);
        }
        Ok(bars)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_trades(
        &mut self,
        resolved: &ResolvedTbankInstrument,
        instrument_id: InstrumentId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        source: TbankTradeSource,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<TradeTick>> {
        let now = Utc::now();
        if from < now - ChronoDuration::hours(1) {
            bail!("GetLastTrades supports only the last guaranteed hour");
        }
        let response = self
            .clients
            .market_data
            .get_last_trades(with_timeout(
                GetLastTradesRequest {
                    #[allow(deprecated)]
                    figi: None,
                    from: Some(unix_nanos_to_timestamp(datetime_to_nanos(from)?)?),
                    to: Some(unix_nanos_to_timestamp(datetime_to_nanos(to)?)?),
                    instrument_id: Some(resolved.stream_id()),
                    trade_source: source.as_proto() as i32,
                },
                self.request_timeout,
            ))
            .await?
            .into_inner();
        let mut trades = response
            .trades
            .iter()
            .map(|trade| tbank_trade_to_nautilus(trade, instrument_id))
            .collect::<anyhow::Result<Vec<_>>>()?;
        trades.sort_by_key(|trade| trade.ts_event.as_u64());
        if let Some(limit) = limit {
            trades.truncate(limit);
        }
        Ok(trades)
    }
}

fn candle_chunks(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    interval: TbankBarInterval,
) -> anyhow::Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    if from >= to {
        bail!("invalid historical range: from must be before to");
    }
    let chunk = interval.grpc_chunk();
    let mut result = Vec::new();
    let mut cursor = from;
    while cursor < to {
        let end = std::cmp::min(cursor + chunk, to);
        result.push((cursor, end));
        cursor = end;
    }
    Ok(result)
}

fn candle_retry_delay(status: &tonic::Status, attempt: usize) -> Duration {
    if status.code() == tonic::Code::ResourceExhausted
        && let Some(reset_seconds) = status
            .metadata()
            .get("x-ratelimit-reset")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    {
        return Duration::from_secs(reset_seconds.saturating_add(1).min(300));
    }
    Duration::from_secs(2_u64.saturating_pow(attempt.min(5) as u32))
}

fn historic_candle_to_bar(
    candle: &HistoricCandle,
    bar_type: BarType,
    interval: TbankBarInterval,
    mode: TbankCandleTimestampMode,
    force_zero_volume: bool,
    precision: TbankBarPrecision,
) -> anyhow::Result<Bar> {
    let start = timestamp_to_unix_nanos(candle.time.as_ref().context("missing candle time")?)?;
    let ts_event = match mode {
        TbankCandleTimestampMode::StartAsBarEnd => {
            u64::try_from(start)? + interval.duration_nanos()
        }
        TbankCandleTimestampMode::StartAsBarStart => u64::try_from(start)?,
    };
    Bar::new_checked(
        bar_type,
        price_from_decimal(
            quotation_to_decimal(candle.open.as_ref().context("missing candle open")?),
            precision.price,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.high.as_ref().context("missing candle high")?),
            precision.price,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.low.as_ref().context("missing candle low")?),
            precision.price,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.close.as_ref().context("missing candle close")?),
            precision.price,
        )?,
        quantity_from_i64(
            if force_zero_volume { 0 } else { candle.volume },
            precision.size,
        )?,
        UnixNanos::from(ts_event),
        UnixNanos::from(ts_event),
    )
}

fn tbank_trade_to_nautilus(trade: &Trade, fallback: InstrumentId) -> anyhow::Result<TradeTick> {
    let ts = UnixNanos::from(u64::try_from(timestamp_to_unix_nanos(
        trade.time.as_ref().context("missing trade time")?,
    )?)?);
    let instrument_id = if !trade.ticker.is_empty() && !trade.class_code.is_empty() {
        instrument_id_from_ticker_class(&trade.ticker, &trade.class_code).parse()?
    } else {
        fallback
    };
    Ok(TradeTick::new(
        instrument_id,
        price_from_decimal(
            quotation_to_decimal(trade.price.as_ref().context("missing trade price")?),
            9,
        )?,
        quantity_from_i64(trade.quantity, 0)?,
        match TradeDirection::try_from(trade.direction).unwrap_or(TradeDirection::Unspecified) {
            TradeDirection::Buy => AggressorSide::Buyer,
            TradeDirection::Sell => AggressorSide::Seller,
            TradeDirection::Unspecified => AggressorSide::NoAggressor,
        },
        nautilus_model::identifiers::TradeId::new(format!("{}", ts.as_u64())),
        ts,
        ts,
    ))
}

fn price_from_decimal(value: Decimal, precision: u8) -> anyhow::Result<Price> {
    let rounded = value.round_dp(u32::from(precision));
    Ok(Price::from(
        format!("{rounded:.precision$}", precision = usize::from(precision)).as_str(),
    ))
}

fn quantity_from_i64(value: i64, precision: u8) -> anyhow::Result<Quantity> {
    Quantity::new_checked(value as f64, precision).map_err(Into::into)
}

fn datetime_to_nanos(value: DateTime<Utc>) -> anyhow::Result<i128> {
    value
        .timestamp_nanos_opt()
        .map(i128::from)
        .context("timestamp out of range")
}

fn dedup_bars(bars: &mut Vec<Bar>) {
    bars.sort_by_key(|bar| bar.ts_event.as_u64());
    let mut seen = BTreeSet::new();
    bars.retain(|bar| seen.insert(bar.ts_event.as_u64()));
}

fn find_exact_or_named(
    mut instruments: Vec<InstrumentShort>,
    ticker: &str,
) -> Option<InstrumentShort> {
    instruments
        .iter()
        .position(|instrument| instrument.ticker == ticker)
        .map(|index| instruments.remove(index))
        .or_else(|| {
            instruments
                .into_iter()
                .find(|instrument| instrument.name.contains(ticker))
        })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn chunks_minute_candles_by_day() {
        let from = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2025, 1, 3, 12, 0, 0).unwrap();
        let chunks = candle_chunks(from, to, TbankBarInterval::Min1).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks[0],
            (from, Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap())
        );
        assert_eq!(chunks[2].1, to);
    }

    #[test]
    fn candle_retry_delay_respects_rate_limit_reset() {
        let mut status = tonic::Status::resource_exhausted("rate limited");
        status
            .metadata_mut()
            .insert("x-ratelimit-reset", "16".parse().unwrap());
        assert_eq!(candle_retry_delay(&status, 0), Duration::from_secs(17));
    }

    #[test]
    fn candle_retry_delay_uses_bounded_backoff() {
        let status = tonic::Status::unavailable("temporary");
        assert_eq!(candle_retry_delay(&status, 0), Duration::from_secs(1));
        assert_eq!(candle_retry_delay(&status, 10), Duration::from_secs(32));
    }
}
