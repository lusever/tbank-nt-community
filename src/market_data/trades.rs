use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Aggressor side reported for a T-Bank trade.
pub enum TbankTradeSide {
    /// Buyer aggressed.
    Buy,
    /// Seller aggressed.
    Sell,
    /// Broker did not specify the side.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized T-Bank trade tick.
pub struct TbankTradeTick {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Trade price.
    pub price: Decimal,
    /// Trade quantity in lots.
    pub quantity_lots: i64,
    /// Trade aggressor side.
    pub side: TbankTradeSide,
    /// Event timestamp in Unix nanoseconds.
    pub ts_event: i128,
    /// Initialization timestamp in Unix nanoseconds.
    pub ts_init: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized top-of-book and last-price update.
pub struct TbankQuoteTick {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Best bid, when supplied.
    pub bid: Option<Decimal>,
    /// Best ask, when supplied.
    pub ask: Option<Decimal>,
    /// Last traded price, when supplied.
    pub last: Option<Decimal>,
    /// Event timestamp in Unix nanoseconds.
    pub ts_event: i128,
    /// Initialization timestamp in Unix nanoseconds.
    pub ts_init: i128,
}
