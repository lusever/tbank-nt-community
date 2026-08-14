//! Shared adapter primitives.

/// Adapter constants.
pub mod consts;
/// T-Bank currency registration and resolution.
pub mod currency;
/// Decimal and protobuf quotation conversions.
pub mod decimal;
/// Broker-neutral order enums.
pub mod enums;
/// Adapter error types.
pub mod error;
/// Instrument identifier parsing and formatting.
pub mod ids;
/// Protobuf timestamp conversions.
pub mod time;
/// Public broker and instrument venue types.
pub mod venue;

pub use currency::register_tbank_currencies;
pub use decimal::{
    decimal_to_money_value, decimal_to_quotation, money_value_to_decimal, price_to_quotation,
    quantity_units_to_lots, quotation_to_decimal,
};
pub use enums::{TbankOrderSide, TbankOrderType};
pub use error::{Result, TbankAdapterError};
pub use ids::TbankInstrumentIdParts;
pub use venue::{TbankInstrumentType, TbankVenue};
