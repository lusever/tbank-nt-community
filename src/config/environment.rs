use crate::common::consts::{LIVE_ENDPOINT, LIVE_TOKEN_ENV, SANDBOX_ENDPOINT, SANDBOX_TOKEN_ENV};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// T-Bank API environment selected by a client.
pub enum TbankEnvironment {
    /// Broker sandbox environment.
    Sandbox,
    /// Live broker environment.
    Live,
}

impl TbankEnvironment {
    /// Returns the default endpoint for this environment.
    pub const fn default_endpoint(self) -> &'static str {
        match self {
            Self::Sandbox => SANDBOX_ENDPOINT,
            Self::Live => LIVE_ENDPOINT,
        }
    }

    /// Returns the token environment-variable name for this environment.
    pub const fn token_env(self) -> &'static str {
        match self {
            Self::Sandbox => SANDBOX_TOKEN_ENV,
            Self::Live => LIVE_TOKEN_ENV,
        }
    }

    /// Returns whether this is the production environment.
    pub const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}
