use crate::{
    common::{
        decimal::quotation_to_decimal,
        error::{Result, TbankAdapterError},
        time::timestamp_to_unix_nanos,
    },
    config::TbankCandleTimestampMode,
    grpc::generated::{Candle, LastPrice, OrderBook, Trade, TradeDirection},
    market_data::{
        TbankBar, TbankOrderBookLevel, TbankOrderBookSnapshot, TbankQuoteTick, TbankTradeTick,
        trades::TbankTradeSide,
    },
};

const ONE_MINUTE_NANOS: i128 = 60_000_000_000;

/// Converts a T-Bank candle to an adapter bar using the supplied receipt timestamp.
pub fn candle_to_bar(
    candle: &Candle,
    mode: TbankCandleTimestampMode,
    ts_init: i128,
) -> Result<TbankBar> {
    let start = timestamp_to_unix_nanos(required_timestamp(candle.time.as_ref(), "candle.time")?)?;
    let ts_event = match mode {
        TbankCandleTimestampMode::StartAsBarEnd => start + ONE_MINUTE_NANOS,
        TbankCandleTimestampMode::StartAsBarStart => start,
    };

    Ok(TbankBar {
        instrument_uid: candle.instrument_uid.clone(),
        open: quotation_to_decimal(required_quotation(candle.open.as_ref(), "candle.open")?),
        high: quotation_to_decimal(required_quotation(candle.high.as_ref(), "candle.high")?),
        low: quotation_to_decimal(required_quotation(candle.low.as_ref(), "candle.low")?),
        close: quotation_to_decimal(required_quotation(candle.close.as_ref(), "candle.close")?),
        volume_lots: candle.volume,
        ts_event,
        ts_init,
    })
}

/// Converts a T-Bank trade to an adapter trade tick using the supplied receipt timestamp.
pub fn trade_to_tick(trade: &Trade, ts_init: i128) -> Result<TbankTradeTick> {
    let ts_event = timestamp_to_unix_nanos(required_timestamp(trade.time.as_ref(), "trade.time")?)?;
    Ok(TbankTradeTick {
        instrument_uid: trade.instrument_uid.clone(),
        price: quotation_to_decimal(required_quotation(trade.price.as_ref(), "trade.price")?),
        quantity_lots: trade.quantity,
        side: match TradeDirection::try_from(trade.direction).unwrap_or(TradeDirection::Unspecified)
        {
            TradeDirection::Buy => TbankTradeSide::Buy,
            TradeDirection::Sell => TbankTradeSide::Sell,
            TradeDirection::Unspecified => TbankTradeSide::Unknown,
        },
        ts_event,
        ts_init,
    })
}

/// Converts a T-Bank last price to an adapter quote tick using the supplied receipt timestamp.
pub fn last_price_to_quote(last_price: &LastPrice, ts_init: i128) -> Result<TbankQuoteTick> {
    let ts_event = timestamp_to_unix_nanos(required_timestamp(
        last_price.time.as_ref(),
        "last_price.time",
    )?)?;
    Ok(TbankQuoteTick {
        instrument_uid: last_price.instrument_uid.clone(),
        bid: None,
        ask: None,
        last: Some(quotation_to_decimal(required_quotation(
            last_price.price.as_ref(),
            "last_price.price",
        )?)),
        ts_event,
        ts_init,
    })
}

/// Converts a T-Bank order book to an adapter snapshot using the supplied receipt timestamp.
pub fn orderbook_to_snapshot(
    orderbook: &OrderBook,
    ts_init: i128,
) -> Result<TbankOrderBookSnapshot> {
    let ts_event = timestamp_to_unix_nanos(required_timestamp(
        orderbook.time.as_ref(),
        "orderbook.time",
    )?)?;
    Ok(TbankOrderBookSnapshot {
        instrument_uid: orderbook.instrument_uid.clone(),
        depth: orderbook.depth,
        is_consistent: orderbook.is_consistent,
        bids: orderbook
            .bids
            .iter()
            .map(order_to_level)
            .collect::<Result<Vec<_>>>()?,
        asks: orderbook
            .asks
            .iter()
            .map(order_to_level)
            .collect::<Result<Vec<_>>>()?,
        ts_event,
        ts_init,
    })
}

fn order_to_level(order: &crate::grpc::generated::Order) -> Result<TbankOrderBookLevel> {
    Ok(TbankOrderBookLevel {
        price: quotation_to_decimal(required_quotation(order.price.as_ref(), "order.price")?),
        quantity_lots: order.quantity,
    })
}

fn required_timestamp<'a>(
    timestamp: Option<&'a prost_types::Timestamp>,
    field: &str,
) -> Result<&'a prost_types::Timestamp> {
    timestamp.ok_or_else(|| TbankAdapterError::ConversionError(format!("missing {field}")))
}

fn required_quotation<'a>(
    quotation: Option<&'a crate::grpc::generated::Quotation>,
    field: &str,
) -> Result<&'a crate::grpc::generated::Quotation> {
    quotation.ok_or_else(|| TbankAdapterError::ConversionError(format!("missing {field}")))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::grpc::generated::{Quotation, SubscriptionInterval};

    use super::*;

    fn ts(seconds: i64) -> prost_types::Timestamp {
        prost_types::Timestamp { seconds, nanos: 0 }
    }

    fn q(value: i64) -> Option<Quotation> {
        Some(Quotation {
            units: value,
            nano: 0,
        })
    }

    #[test]
    fn candle_start_timestamp_defaults_to_bar_end() {
        let candle = Candle {
            interval: SubscriptionInterval::OneMinute as i32,
            open: q(1),
            high: q(2),
            low: q(1),
            close: q(2),
            volume: 100,
            time: Some(ts(36_000)),
            instrument_uid: "uid".to_string(),
            ..Candle::default()
        };
        let bar = candle_to_bar(&candle, TbankCandleTimestampMode::StartAsBarEnd, 42).unwrap();
        assert_eq!(
            bar.ts_event,
            (36_000_i128 * 1_000_000_000) + ONE_MINUTE_NANOS
        );
        assert_eq!(bar.ts_init, 42);
    }

    #[test]
    fn trade_to_tick_maps_side() {
        let trade = Trade {
            direction: TradeDirection::Buy as i32,
            price: q(250),
            quantity: 3,
            time: Some(ts(1)),
            instrument_uid: "uid".to_string(),
            ..Trade::default()
        };
        let tick = trade_to_tick(&trade, 42).unwrap();
        assert_eq!(tick.side, TbankTradeSide::Buy);
        assert_eq!(tick.price, Decimal::from(250));
        assert_eq!(tick.ts_init, 42);
    }

    #[test]
    fn last_price_maps_to_quote_last() {
        let last_price = LastPrice {
            price: q(250),
            time: Some(ts(1)),
            instrument_uid: "uid".to_string(),
            ..LastPrice::default()
        };
        assert_eq!(
            last_price_to_quote(&last_price, 42).unwrap().last,
            Some(Decimal::from(250))
        );
        assert_eq!(last_price_to_quote(&last_price, 42).unwrap().ts_init, 42);
    }
}
