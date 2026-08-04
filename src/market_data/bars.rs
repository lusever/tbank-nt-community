use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized one-minute T-Bank candle.
pub struct TbankBar {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Open price.
    pub open: Decimal,
    /// High price.
    pub high: Decimal,
    /// Low price.
    pub low: Decimal,
    /// Close price.
    pub close: Decimal,
    /// Traded volume in lots.
    pub volume_lots: i64,
    /// Event timestamp in Unix nanoseconds.
    pub ts_event: i128,
    /// Initialization timestamp in Unix nanoseconds.
    pub ts_init: i128,
}
