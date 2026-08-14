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
