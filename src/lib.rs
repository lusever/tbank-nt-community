//! Rust-only NautilusTrader adapter foundation for T-Bank Invest API.
//!
//! This crate intentionally contains no Python bindings and no executable capable of
//! submitting live orders.

#![warn(missing_docs)]

/// Shared constants, conversions, identifiers, and errors.
pub mod common;
/// Adapter configuration types.
pub mod config;
/// Order submission and execution-client integration.
pub mod execution;
/// Nautilus client factories.
pub mod factory;
/// T-Bank gRPC transport and generated contracts.
pub mod grpc;
mod historical;
/// Instrument mapping and provider support.
pub mod instruments;
/// Live market-data clients and conversions.
pub mod market_data;
#[cfg(test)]
pub(crate) mod testing;

pub use common::consts::{SPBE, SPBE_VENUE, TBANK_VENUE};
pub use common::{TbankInstrumentType, TbankVenue, register_tbank_currencies};
pub use config::{
    TbankDataClientConfig, TbankEnvironment, TbankExecutionClientConfig,
    TbankIndicativeInstrumentConfig,
};
pub use factory::{TbankDataClientFactory, TbankExecutionClientFactory};
pub use instruments::{
    TbankInstrumentMapper, TbankInstrumentMetadata, TbankInstrumentProvider,
    TbankMarketDataInstrumentMetadata,
};
pub use market_data::{
    TbankCandleReadinessState, TbankMarketDataEvent, TbankMarketDataStreamState,
    register_tbank_market_data_custom_data,
};

/// Registers every T-Bank custom data type for Nautilus JSON deserialization.
///
/// Call this during process initialization before replaying persisted custom data. Both the
/// market-data and execution registrations are idempotent and process-local.
pub fn register_tbank_custom_data() {
    register_tbank_market_data_custom_data();
    execution::register_tbank_execution_custom_data();
}
