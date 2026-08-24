//! Live T-Bank market-data models and client integration.

/// Candle-derived bar model.
pub mod bars;
/// Order-book models and synthetic deltas.
pub mod book;
pub(crate) mod candles;
/// Nautilus market-data client.
pub mod client;
pub(crate) mod continuity;
/// Protobuf-to-domain conversions.
pub mod converters;
mod events;
/// Subscription registry.
pub mod subscriptions;
pub(crate) mod supervisor;
/// Trade and quote models.
pub mod trades;

pub use bars::TbankBar;
pub use book::{SyntheticBookDelta, TbankBookSide, TbankOrderBookLevel, TbankOrderBookSnapshot};
pub use client::TbankDataClient;
pub use converters::{candle_to_bar, last_price_to_quote, orderbook_to_snapshot, trade_to_tick};
pub use events::{
    TbankCandleReadinessState, TbankMarketDataEvent, TbankMarketDataStreamState,
    subscribe_market_data_events,
};
pub use subscriptions::TbankSubscriptionRegistry;
pub use trades::{TbankQuoteTick, TbankTradeTick};

/// Canonical instrument fields required to translate venue market data into Nautilus types.
///
/// Price precision is instrument metadata, not a property of an individual wire value. For
/// example, `100` and `100.01` for the same instrument must both become Nautilus prices with the
/// instrument's configured precision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MarketDataInstrumentMetadata {
    pub lot_size: u32,
    pub price_precision: u8,
    /// Whether T-Bank's ticker/class fields are descriptive for a configured indicative and
    /// the registered Nautilus ID must remain the event identity.
    pub preserve_instrument_id: bool,
}
