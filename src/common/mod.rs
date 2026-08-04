//! Shared adapter primitives.

/// Adapter constants.
pub mod consts;
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

pub use decimal::{
    decimal_to_money_value, decimal_to_quotation, money_value_to_decimal, price_to_quotation,
    quantity_shares_to_lots, quotation_to_decimal,
};
pub use enums::{TbankOrderSide, TbankOrderType};
pub use error::{Result, TbankAdapterError};
pub use ids::TbankInstrumentIdParts;
