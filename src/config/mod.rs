//! Configuration shared by data and execution clients.

mod data;
mod environment;
mod execution;

pub use data::{TbankCandleTimestampMode, TbankDataClientConfig, TbankIndicativeInstrumentConfig};
pub use environment::TbankEnvironment;
pub use execution::TbankExecutionClientConfig;
use serde::{Deserialize, Serialize};

#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Exponential reconnect backoff parameters.
pub struct TbankReconnectPolicy {
    /// Initial delay in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum delay in milliseconds.
    pub max_backoff_ms: u64,
    /// Enables bounded random jitter on reconnect delays.
    ///
    /// When enabled, each delay receives up to 100 ms of additive jitter while
    /// remaining within the configured `[initial_backoff_ms, max_backoff_ms]` range.
    pub jitter: bool,
}

impl Default for TbankReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            jitter: true,
        }
    }
}
