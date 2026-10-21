//! Source-specific snapshots carried by Nautilus custom data.
use super::TbankOrderBookLevel;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::InstrumentId,
};
use std::sync::Arc;

/// Source selected explicitly for a streaming book subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankBookSource {
    /// Exchange book.
    Exchange,
    /// Dealer book.
    Dealer,
}
impl TbankBookSource {
    /// Stable routing and catalog name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exchange => "exchange",
            Self::Dealer => "dealer",
        }
    }
    pub(crate) fn broker_type(self) -> i32 {
        use crate::grpc::generated::OrderBookType;
        match self {
            Self::Exchange => OrderBookType::OrderbookTypeExchange as i32,
            Self::Dealer => OrderBookType::OrderbookTypeDealer as i32,
        }
    }
}

/// Complete identity of one custom book subscription, preserved across reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TbankBookSubscription {
    /// Real instrument identity, without source suffixes.
    pub instrument_id: InstrumentId,
    /// Explicit broker source.
    pub source: TbankBookSource,
    /// Requested maximum number of levels.
    pub depth: i32,
}
impl TbankBookSubscription {
    /// Stable lifecycle stream ID shared by all active custom book subscriptions.
    /// `Connected` confirms the complete group; reconnect or membership changes invalidate it.
    pub const STREAM_ID: &'static str = "custom_book:all:group";

    /// Validates a subscription before transport admission.
    pub fn new(
        instrument_id: InstrumentId,
        source: TbankBookSource,
        depth: i32,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            matches!(depth, 1 | 10 | 20 | 30 | 40 | 50),
            "unsupported T-Bank book depth"
        );
        Ok(Self {
            instrument_id,
            source,
            depth,
        })
    }
    /// Full routing metadata and collision-free catalog identifier.
    pub fn data_type(self) -> DataType {
        let mut metadata = Params::new();
        metadata.insert(
            "instrument_id".into(),
            serde_json::json!(self.instrument_id.to_string()),
        );
        metadata.insert("source".into(), serde_json::json!(self.source));
        metadata.insert("depth".into(), serde_json::json!(self.depth));
        DataType::new(
            "TbankOrderBookSnapshot",
            Some(metadata),
            Some(format!(
                "{}--{}--{}",
                self.instrument_id,
                self.source.as_str(),
                self.depth
            )),
        )
    }
    /// Parses and validates full custom-data routing identity.
    pub fn from_data_type(dt: &DataType) -> anyhow::Result<Self> {
        anyhow::ensure!(
            dt.type_name() == "TbankOrderBookSnapshot",
            "unsupported custom data type"
        );
        let m = dt
            .metadata()
            .ok_or_else(|| anyhow::anyhow!("book metadata required"))?;
        let instrument_id = m
            .get_str("instrument_id")
            .ok_or_else(|| anyhow::anyhow!("instrument_id required"))?
            .parse()?;
        let source = serde_json::from_value(
            m.get("source")
                .ok_or_else(|| anyhow::anyhow!("source required"))?
                .clone(),
        )?;
        let depth = i32::try_from(
            m.get("depth")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| anyhow::anyhow!("depth required"))?,
        )?;
        let key = Self::new(instrument_id, source, depth)?;
        anyhow::ensure!(
            key.data_type() == *dt,
            "book routing metadata/identifier mismatch"
        );
        Ok(key)
    }
}

/// Full broker book, with exact decimal prices and quantities in broker lots.
#[nautilus_serialization::arrow_custom_data]
#[nautilus_model::custom_data]
pub struct TbankOrderBookSnapshot {
    /// Real Nautilus instrument identity.
    pub instrument_id: InstrumentId,
    /// Broker-confirmed source.
    #[custom_data_field(serde)]
    pub source: TbankBookSource,
    /// Requested depth.
    pub depth: i32,
    /// Broker consistency flag, preserved without filtering.
    pub is_consistent: bool,
    /// Aggregate bid levels; price remains decimal, quantity is lots.
    #[custom_data_field(serde)]
    pub bids: Vec<TbankOrderBookLevel>,
    /// Aggregate ask levels.
    #[custom_data_field(serde)]
    pub asks: Vec<TbankOrderBookLevel>,
    /// Broker formation time, preserved independently of local receipt time.
    pub broker_timestamp: UnixNanos,
    /// Broker formation time; equal to broker_timestamp.
    pub ts_event: UnixNanos,
    /// Local UTC receipt time before conversion/publication.
    pub ts_init: UnixNanos,
}
impl TbankOrderBookSnapshot {
    /// Wraps a snapshot with its full subscription routing identity.
    pub fn into_custom_data(self) -> CustomData {
        let dt = TbankBookSubscription {
            instrument_id: self.instrument_id,
            source: self.source,
            depth: self.depth,
        }
        .data_type();
        CustomData::new(Arc::new(self), dt)
    }
    pub(crate) fn from_broker(
        book: &crate::grpc::generated::OrderBook,
        key: TbankBookSubscription,
        received_at: UnixNanos,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            book.order_book_type == key.source.broker_type(),
            "broker book source differs from subscription"
        );
        anyhow::ensure!(
            book.depth == key.depth,
            "broker book depth differs from subscription"
        );
        let raw = super::orderbook_to_snapshot(book, i128::from(received_at.as_u64()))?;
        let ts_event = UnixNanos::from(u64::try_from(raw.ts_event)?);
        Ok(Self {
            instrument_id: key.instrument_id,
            source: key.source,
            depth: key.depth,
            is_consistent: raw.is_consistent,
            bids: raw.bids,
            asks: raw.asks,
            broker_timestamp: ts_event,
            ts_event,
            ts_init: received_at,
        })
    }
}

/// Registers the JSON and Arrow codecs in each writer and reader process.
pub fn register_tbank_book_custom_data() {
    nautilus_serialization::ensure_custom_data_registered::<TbankOrderBookSnapshot>();
}
