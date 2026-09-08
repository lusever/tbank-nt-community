//! Historical request support used by the main T-Bank data client.

use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr,
    time::Duration,
};

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
        ids::{TbankInstrumentIdParts, instrument_id_from_ticker_class_for_venue},
        time::{timestamp_to_unix_nanos, unix_nanos_to_timestamp},
    },
    config::{TbankCandleTimestampMode, TbankIndicativeInstrumentConfig},
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients,
        generated::{
            CandleInterval, FindInstrumentRequest, GetCandlesRequest, GetFuturesMarginRequest,
            GetFuturesMarginResponse, GetLastTradesRequest, HistoricCandle, InstrumentIdType,
            InstrumentRequest, InstrumentShort, InstrumentType, Trade, TradeDirection,
            TradeSourceType, get_candles_request,
        },
        with_timeout,
    },
    instruments::TbankInstrumentMetadata,
};

pub(crate) const DEFAULT_TRADE_SOURCE: TbankTradeSource = TbankTradeSource::All;
const DEFAULT_QUANTITY_PRECISION: u8 = 0;

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
    /// Number of Nautilus units represented by one T-Bank trade lot.
    pub(crate) lot_size: u32,
    /// Price precision from the resolved Nautilus instrument definition.
    pub(crate) price_precision: u8,
    /// Quantity precision from the resolved Nautilus instrument definition.
    pub(crate) quantity_precision: u8,
    /// Keep the configured ID when T-Bank returns descriptive ticker/class fields for an
    /// indicative instrument instead of a tradable canonical identity.
    pub(crate) preserve_requested_instrument_id: bool,
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
    indicative_instruments: HashMap<String, TbankIndicativeInstrumentConfig>,
}

impl TbankHistoricalClient {
    pub(crate) fn new(
        clients: TbankGrpcClients<TbankAuthInterceptor>,
        timestamp_mode: TbankCandleTimestampMode,
        request_timeout: Duration,
        indicative_instruments: HashMap<String, TbankIndicativeInstrumentConfig>,
    ) -> Self {
        Self {
            clients,
            timestamp_mode,
            request_timeout,
            indicative_instruments,
        }
    }

    pub(crate) async fn resolve_instrument(
        &mut self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<ResolvedTbankInstrument> {
        let id = instrument_id.to_string();
        let indicative_price_precision = self
            .indicative_instruments
            .get(&id)
            .map(|definition| definition.price_increment.normalize().scale());
        if let Some(price_precision) = indicative_price_precision {
            let price_precision = u8::try_from(price_precision)
                .context("indicative instrument price precision does not fit Nautilus")?;
            let ticker = id
                .rsplit_once('.')
                .map_or(id.as_str(), |(ticker, _)| ticker);
            return self.resolve_index(ticker, price_precision).await;
        }
        let parts = TbankInstrumentIdParts::from_str(&id)?;
        if !parts.has_supported_venue() {
            bail!("unsupported T-Bank instrument venue in historical request: {id}");
        }
        let is_share = parts.is_spbe_share() || parts.is_moex_tqbr_equity();
        let is_futures = parts.is_moex_futures();
        if !is_share && !is_futures {
            bail!("unsupported T-Bank instrument family in historical request: {id}");
        }
        let request = InstrumentRequest {
            id_type: InstrumentIdType::Ticker as i32,
            class_code: Some(parts.class_code.clone()),
            id: parts.ticker.clone(),
        };
        if is_share {
            let response = self
                .clients
                .instruments
                .share_by(with_timeout(request, self.request_timeout))
                .await?
                .into_inner();
            let share = response
                .instrument
                .context("ShareBy returned no instrument")?;
            return resolved_from_metadata(
                &parts,
                TbankInstrumentMetadata::from_share(&share)?,
                "share",
            );
        }

        let response = self
            .clients
            .instruments
            .future_by(with_timeout(request, self.request_timeout))
            .await?
            .into_inner();
        let future = response
            .instrument
            .context("FutureBy returned no instrument")?;
        let metadata = TbankInstrumentMetadata::from_future(&future)?;
        let margin_request = GetFuturesMarginRequest {
            #[allow(deprecated)]
            figi: String::new(),
            instrument_id: metadata.futures_margin_instrument_id()?,
        };
        let margin = self
            .clients
            .instruments
            .get_futures_margin(with_timeout(margin_request, self.request_timeout))
            .await?
            .into_inner();
        resolved_from_futures_metadata(&parts, metadata, &margin)
    }

    async fn resolve_index(
        &mut self,
        ticker: &str,
        price_precision: u8,
    ) -> anyhow::Result<ResolvedTbankInstrument> {
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
        let instrument = Self::find_exact_index(response.instruments, ticker)
            .with_context(|| format!("FindInstrument returned no {ticker} index"))?;
        Ok(ResolvedTbankInstrument {
            instrument_uid: instrument.uid,
            ticker: instrument.ticker,
            class_code: instrument.class_code,
            instrument_kind: "index".to_string(),
            lot_size: 1,
            price_precision,
            quantity_precision: DEFAULT_QUANTITY_PRECISION,
            preserve_requested_instrument_id: true,
        })
    }

    fn find_exact_index(
        instruments: impl IntoIterator<Item = InstrumentShort>,
        ticker: &str,
    ) -> Option<InstrumentShort> {
        instruments
            .into_iter()
            .find(|instrument| instrument.ticker.eq_ignore_ascii_case(ticker))
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
        retries: usize,
    ) -> anyhow::Result<Vec<Bar>> {
        let chunks = candle_chunks(from, to, interval)?;
        let mut bars = Vec::new();
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
                        resolved.lot_size,
                        resolved.price_precision,
                        resolved.quantity_precision,
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
        let price_precision = resolved.price_precision;
        let quantity_precision = resolved.quantity_precision;
        let mut trades = response
            .trades
            .iter()
            .map(|trade| {
                tbank_trade_to_nautilus(
                    trade,
                    instrument_id,
                    resolved.preserve_requested_instrument_id,
                    resolved.lot_size,
                    price_precision,
                    quantity_precision,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        trades.sort_by_key(|trade| trade.ts_event.as_u64());
        if let Some(limit) = limit {
            trades.truncate(limit);
        }
        Ok(trades)
    }
}

fn resolved_from_futures_metadata(
    requested: &TbankInstrumentIdParts,
    mut metadata: TbankInstrumentMetadata,
    margin: &GetFuturesMarginResponse,
) -> anyhow::Result<ResolvedTbankInstrument> {
    metadata.update_futures_margin_contract(margin)?;
    resolved_from_metadata(requested, metadata, "futures")
}

fn resolved_from_metadata(
    requested: &TbankInstrumentIdParts,
    metadata: TbankInstrumentMetadata,
    instrument_kind: &str,
) -> anyhow::Result<ResolvedTbankInstrument> {
    ensure_canonical_resolution(requested, &metadata.instrument_id)?;
    Ok(ResolvedTbankInstrument {
        instrument_uid: metadata.instrument_uid,
        ticker: metadata.ticker,
        class_code: metadata.class_code,
        instrument_kind: instrument_kind.to_string(),
        lot_size: metadata.lot,
        price_precision: u8::try_from(metadata.price_precision)
            .context("historical instrument price precision does not fit Nautilus")?,
        quantity_precision: u8::try_from(metadata.quantity_precision)
            .context("historical instrument quantity precision does not fit Nautilus")?,
        preserve_requested_instrument_id: false,
    })
}

fn ensure_canonical_resolution(
    requested: &TbankInstrumentIdParts,
    resolved_instrument_id: &str,
) -> anyhow::Result<()> {
    if resolved_instrument_id != requested.instrument_id() {
        bail!(
            "T-Bank resolved historical instrument as {resolved_instrument_id}, requested {}",
            requested.instrument_id()
        );
    }
    Ok(())
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

#[allow(clippy::too_many_arguments)]
fn historic_candle_to_bar(
    candle: &HistoricCandle,
    bar_type: BarType,
    interval: TbankBarInterval,
    mode: TbankCandleTimestampMode,
    force_zero_volume: bool,
    lot_size: u32,
    price_precision: u8,
    quantity_precision: u8,
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
            price_precision,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.high.as_ref().context("missing candle high")?),
            price_precision,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.low.as_ref().context("missing candle low")?),
            price_precision,
        )?,
        price_from_decimal(
            quotation_to_decimal(candle.close.as_ref().context("missing candle close")?),
            price_precision,
        )?,
        quantity_from_lots(
            if force_zero_volume { 0 } else { candle.volume },
            lot_size,
            quantity_precision,
        )?,
        UnixNanos::from(ts_event),
        UnixNanos::from(ts_event),
    )
}

fn tbank_trade_to_nautilus(
    trade: &Trade,
    requested_instrument_id: InstrumentId,
    preserve_requested_instrument_id: bool,
    lot_size: u32,
    price_precision: u8,
    quantity_precision: u8,
) -> anyhow::Result<TradeTick> {
    let ts = UnixNanos::from(u64::try_from(timestamp_to_unix_nanos(
        trade.time.as_ref().context("missing trade time")?,
    )?)?);
    let instrument_id = if preserve_requested_instrument_id {
        requested_instrument_id
    } else if !trade.ticker.is_empty() && !trade.class_code.is_empty() {
        let resolved = instrument_id_from_ticker_class_for_venue(
            &trade.ticker,
            &trade.class_code,
            requested_instrument_id.venue.as_str(),
        )
        .parse::<InstrumentId>()?;
        if resolved != requested_instrument_id {
            bail!(
                "T-Bank returned historical trade for {resolved}, requested {requested_instrument_id}"
            );
        }
        resolved
    } else {
        requested_instrument_id
    };
    Ok(TradeTick::new(
        instrument_id,
        price_from_decimal(
            quotation_to_decimal(trade.price.as_ref().context("missing trade price")?),
            price_precision,
        )?,
        quantity_from_lots(trade.quantity, lot_size, quantity_precision)?,
        match TradeDirection::try_from(trade.direction).unwrap_or(TradeDirection::Unspecified) {
            TradeDirection::Buy => AggressorSide::Buy,
            TradeDirection::Sell => AggressorSide::Sell,
            TradeDirection::Unspecified => AggressorSide::NoAggressor,
        },
        nautilus_model::identifiers::TradeId::new(format!("{}", ts.as_u64())),
        ts,
        ts,
    ))
}

fn quantity_from_lots(value_lots: i64, lot_size: u32, precision: u8) -> anyhow::Result<Quantity> {
    if lot_size == 0 {
        bail!("historical quantity lot size must be positive");
    }
    let value_units = value_lots
        .checked_mul(i64::from(lot_size))
        .context("historical trade quantity overflow")?;
    quantity_from_i64(value_units, precision)
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use nautilus_model::{
        data::BarSpecification,
        enums::{AggregationSource, PriceType},
    };

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

    #[test]
    fn canonical_historical_resolution_rejects_wrong_or_unsupported_venue() {
        let requested = TbankInstrumentIdParts::from_str("Si-9.26_SPBFUT.MOEX").unwrap();
        assert!(ensure_canonical_resolution(&requested, "Si-9.26_SPBFUT.MOEX").is_ok());
        assert!(ensure_canonical_resolution(&requested, "Si-9.26_SPBFUT.SPBE").is_err());

        let unsupported = TbankInstrumentIdParts::from_str("Si-9.26_SPBFUT.TEST").unwrap();
        assert!(!unsupported.has_supported_venue());
        assert!(ensure_canonical_resolution(&unsupported, "Si-9.26_SPBFUT.MOEX").is_err());
    }

    #[test]
    fn resolved_historical_metadata_carries_broker_tick_precision() {
        let future = crate::grpc::generated::Future {
            ticker: "Si-9.26".to_string(),
            class_code: "SPBFUT".to_string(),
            lot: 1,
            currency: "RUB".to_string(),
            min_price_increment: Some(crate::grpc::generated::Quotation { units: 1, nano: 0 }),
            min_price_increment_amount: Some(crate::grpc::generated::Quotation {
                units: 12,
                nano: 500_000_000,
            }),
            uid: "si-uid".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..crate::grpc::generated::Future::default()
        };
        let future_parts = TbankInstrumentIdParts::from_str("Si-9.26_SPBFUT.MOEX").unwrap();
        let resolved_future = resolved_from_futures_metadata(
            &future_parts,
            TbankInstrumentMetadata::from_future(&future).unwrap(),
            &crate::grpc::generated::GetFuturesMarginResponse {
                initial_margin_on_buy: Some(crate::grpc::generated::MoneyValue {
                    currency: "RUB".to_string(),
                    units: 15_000,
                    nano: 0,
                }),
                initial_margin_on_sell: Some(crate::grpc::generated::MoneyValue {
                    currency: "RUB".to_string(),
                    units: 16_000,
                    nano: 0,
                }),
                min_price_increment: Some(crate::grpc::generated::Quotation {
                    units: 0,
                    nano: 500_000_000,
                }),
                min_price_increment_amount: Some(crate::grpc::generated::Quotation {
                    units: 75,
                    nano: 0,
                }),
            },
        )
        .unwrap();
        assert_eq!(resolved_future.price_precision, 1);

        let share = crate::grpc::generated::Share {
            ticker: "AAPL".to_string(),
            class_code: "SPBXM".to_string(),
            lot: 1,
            currency: "USD".to_string(),
            min_price_increment: Some(crate::grpc::generated::Quotation {
                units: 0,
                nano: 10_000_000,
            }),
            uid: "aapl-uid".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Rts as i32,
            ..crate::grpc::generated::Share::default()
        };
        let share_parts = TbankInstrumentIdParts::from_str("AAPL_SPBXM.SPBE").unwrap();
        let resolved_share = resolved_from_metadata(
            &share_parts,
            TbankInstrumentMetadata::from_share(&share).unwrap(),
            "share",
        )
        .unwrap();
        assert_eq!(resolved_share.price_precision, 2);
    }

    #[test]
    fn historical_trade_quantity_is_converted_from_lots_to_units() {
        let instrument_id: InstrumentId = "Si-9.26_SPBFUT.MOEX".parse().unwrap();
        let trade = Trade {
            ticker: "Si-9.26".to_string(),
            class_code: "SPBFUT".to_string(),
            quantity: 3,
            price: Some(crate::grpc::generated::Quotation {
                units: 70_000,
                nano: 0,
            }),
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Trade::default()
        };

        let tick = tbank_trade_to_nautilus(&trade, instrument_id, false, 10, 0, 0).unwrap();

        assert_eq!(tick.size.as_f64(), 30.0);
        assert_eq!(tick.price.as_f64(), 70_000.0);
        assert_eq!(tick.price.precision, 0);
    }

    #[test]
    fn configured_indicative_trade_keeps_registered_instrument_id() {
        let instrument_id: InstrumentId = "IMOEX2.MOEX".parse().unwrap();
        let trade = Trade {
            ticker: "IMOEX2".to_string(),
            class_code: "INDEX".to_string(),
            quantity: 1,
            price: Some(crate::grpc::generated::Quotation {
                units: 3_000,
                nano: 0,
            }),
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Trade::default()
        };

        let tick = tbank_trade_to_nautilus(&trade, instrument_id, true, 1, 0, 0).unwrap();

        assert_eq!(tick.instrument_id, instrument_id);
    }

    #[test]
    fn historical_index_resolution_requires_exact_ticker() {
        let named_match = InstrumentShort {
            ticker: "IMOEX2_FUTURE".to_string(),
            name: "IMOEX2 related index".to_string(),
            ..InstrumentShort::default()
        };
        assert!(TbankHistoricalClient::find_exact_index(vec![named_match], "IMOEX2").is_none());

        let exact_match = InstrumentShort {
            ticker: "imoex2".to_string(),
            uid: "index-uid".to_string(),
            ..InstrumentShort::default()
        };
        let resolved = TbankHistoricalClient::find_exact_index(vec![exact_match], "IMOEX2")
            .expect("case-insensitive exact ticker should resolve");
        assert_eq!(resolved.uid, "index-uid");
    }

    #[test]
    fn historical_candle_volume_is_converted_from_lots_to_units() {
        let instrument_id: InstrumentId = "Si-9.26_SPBFUT.MOEX".parse().unwrap();
        let bar_type = BarType::new(
            instrument_id,
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        );
        let candle = HistoricCandle {
            open: Some(crate::grpc::generated::Quotation {
                units: 70_000,
                nano: 0,
            }),
            high: Some(crate::grpc::generated::Quotation {
                units: 70_001,
                nano: 0,
            }),
            low: Some(crate::grpc::generated::Quotation {
                units: 69_999,
                nano: 0,
            }),
            close: Some(crate::grpc::generated::Quotation {
                units: 70_000,
                nano: 0,
            }),
            volume: 3,
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..HistoricCandle::default()
        };

        let bar = historic_candle_to_bar(
            &candle,
            bar_type,
            TbankBarInterval::Min1,
            TbankCandleTimestampMode::StartAsBarStart,
            false,
            10,
            0,
            0,
        )
        .unwrap();

        assert_eq!(bar.volume.as_f64(), 30.0);
        assert_eq!(bar.open.as_f64(), 70_000.0);
        assert_eq!(bar.open.precision, 0);
    }

    #[test]
    fn historical_spbe_prices_use_share_precision() {
        let instrument_id: InstrumentId = "AAPL_SPBXM.SPBE".parse().unwrap();
        let trade = Trade {
            ticker: "AAPL".to_string(),
            class_code: "SPBXM".to_string(),
            quantity: 1,
            price: Some(crate::grpc::generated::Quotation {
                units: 123,
                nano: 450_000_000,
            }),
            time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Trade::default()
        };
        let tick = tbank_trade_to_nautilus(&trade, instrument_id, false, 1, 2, 0).unwrap();

        assert_eq!(tick.price.as_f64(), 123.45);
        assert_eq!(tick.price.precision, 2);
    }
}
