use std::{env, fmt, time::Duration};

use nautilus_common::factories::ClientConfig;
use nautilus_live::config::LiveExecutionEngineConfig;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    common::{
        consts::{ACCOUNT_ID_ENV, DEFAULT_REQUEST_TIMEOUT, SANDBOX_ACCOUNT_ID_ENV},
        error::{Result, TbankAdapterError},
    },
    config::{TbankEnvironment, TbankReconnectPolicy, data::validate_endpoint},
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
/// Configuration for the T-Bank execution client.
pub struct TbankExecutionClientConfig {
    #[builder(default = TbankEnvironment::Sandbox)]
    /// Broker environment.
    pub environment: TbankEnvironment,
    #[serde(skip_serializing)]
    /// Optional API token; environment lookup is used when absent.
    pub token: Option<String>,
    #[serde(skip_serializing)]
    /// Optional broker account ID; environment lookup is used when absent.
    pub account_id: Option<String>,
    /// Optional gRPC endpoint override.
    pub endpoint: Option<String>,
    #[builder(default = DEFAULT_REQUEST_TIMEOUT)]
    /// Timeout for broker requests.
    pub request_timeout: Duration,
    #[builder(default = Duration::from_secs(30))]
    /// Timeout for initial account registration with Nautilus.
    pub account_registration_timeout: Duration,
    #[builder(default = 2_000)]
    /// Mirrors `LiveExecutionEngineConfig::inflight_check_interval_ms`.
    ///
    /// Keep this synchronized with the config passed to `LiveNode` so ambiguous-submit recovery
    /// ends before Nautilus closes an in-flight order.
    pub live_node_inflight_check_interval_ms: u32,
    #[builder(default = 5_000)]
    /// Mirrors `LiveExecutionEngineConfig::inflight_check_threshold_ms`.
    pub live_node_inflight_check_threshold_ms: u32,
    #[builder(default = 5)]
    /// Mirrors `LiveExecutionEngineConfig::inflight_check_retries`.
    pub live_node_inflight_check_retries: u32,
    #[builder(default)]
    /// Stream reconnect backoff policy.
    pub reconnect_policy: TbankReconnectPolicy,
    #[builder(default = false)]
    /// Enables order submission.
    pub enable_trading: bool,
    #[builder(default = false)]
    /// Explicitly permits submission to the live environment.
    pub allow_live_trading: bool,
    #[builder(default = false)]
    /// Default value for T-Bank's margin-trade confirmation flag.
    pub confirm_margin_trade_default: bool,
}

impl fmt::Debug for TbankExecutionClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TbankExecutionClientConfig")
            .field("environment", &self.environment)
            .field("token_present", &self.token.is_some())
            .field("account_id_present", &self.account_id.is_some())
            .field("endpoint_present", &self.endpoint.is_some())
            .field("request_timeout", &self.request_timeout)
            .field(
                "account_registration_timeout",
                &self.account_registration_timeout,
            )
            .field(
                "live_node_inflight_check_interval_ms",
                &self.live_node_inflight_check_interval_ms,
            )
            .field(
                "live_node_inflight_check_threshold_ms",
                &self.live_node_inflight_check_threshold_ms,
            )
            .field(
                "live_node_inflight_check_retries",
                &self.live_node_inflight_check_retries,
            )
            .field("reconnect_policy", &self.reconnect_policy)
            .field("enable_trading", &self.enable_trading)
            .field("allow_live_trading", &self.allow_live_trading)
            .field(
                "confirm_margin_trade_default",
                &self.confirm_margin_trade_default,
            )
            .finish()
    }
}

impl ClientConfig for TbankExecutionClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for TbankExecutionClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl TbankExecutionClientConfig {
    /// Copies the in-flight reconciliation settings from the config used to build `LiveNode`.
    ///
    /// Use the same `LiveExecutionEngineConfig` value for this method and
    /// `LiveNodeBuilder::with_exec_engine_config` to keep submit recovery within Nautilus' actual
    /// in-flight window.
    #[must_use]
    pub fn with_live_node_execution_engine_config(
        mut self,
        config: &LiveExecutionEngineConfig,
    ) -> Self {
        self.live_node_inflight_check_interval_ms = config.inflight_check_interval_ms;
        self.live_node_inflight_check_threshold_ms = config.inflight_check_threshold_ms;
        self.live_node_inflight_check_retries = config.inflight_check_retries;
        self
    }

    /// Returns the validated gRPC endpoint URI.
    pub fn endpoint_uri(&self) -> Result<String> {
        validate_endpoint(
            self.endpoint
                .as_deref()
                .unwrap_or_else(|| self.environment.default_endpoint()),
        )
    }

    /// Resolves the API token from configuration or the environment.
    ///
    /// The returned allocation is zeroized when dropped.
    pub fn resolve_token_secret(&self) -> Result<Zeroizing<String>> {
        self.token
            .clone()
            .or_else(|| env::var(self.environment.token_env()).ok())
            .filter(|token| !token.trim().is_empty())
            .map(Zeroizing::new)
            .ok_or(TbankAdapterError::MissingToken)
    }

    /// Resolves the broker account ID from configuration or the environment.
    pub fn resolve_account_id(&self) -> Result<String> {
        self.account_id
            .clone()
            .or_else(|| {
                if self.environment == TbankEnvironment::Sandbox {
                    env::var(SANDBOX_ACCOUNT_ID_ENV).ok()
                } else {
                    env::var(ACCOUNT_ID_ENV).ok()
                }
            })
            .filter(|account_id| !account_id.trim().is_empty())
            .ok_or(TbankAdapterError::MissingAccountId)
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<()> {
        self.endpoint_uri()?;
        self.resolve_token_secret()?;
        self.resolve_account_id()?;
        if self.account_registration_timeout.is_zero() {
            return Err(TbankAdapterError::ConfigError(
                "account_registration_timeout must be positive".to_string(),
            ));
        }
        Ok(())
    }

    /// Verifies that order submission is allowed by the configuration.
    pub fn ensure_submit_allowed(&self) -> Result<()> {
        if !self.enable_trading {
            return Err(TbankAdapterError::PermissionDenied(
                "trading is disabled in config".to_string(),
            ));
        }
        if self.environment.is_live() && !self.allow_live_trading {
            return Err(TbankAdapterError::PermissionDenied(
                "live submit_order requires allow_live_trading=true".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::consts::{LIVE_TOKEN_ENV, SANDBOX_TOKEN_ENV};

    fn with_env_vars(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = crate::config::ENV_LOCK.lock().unwrap();
        let previous = vars
            .iter()
            .map(|(key, _)| (*key, env::var(key).ok()))
            .collect::<Vec<_>>();

        for (key, value) in vars {
            match value {
                Some(value) => unsafe { env::set_var(key, value) },
                None => unsafe { env::remove_var(key) },
            }
        }

        f();

        for (key, value) in previous {
            match value {
                Some(value) => unsafe { env::set_var(key, value) },
                None => unsafe { env::remove_var(key) },
            }
        }
    }

    #[test]
    fn missing_account_id_is_error() {
        let config = TbankExecutionClientConfig {
            token: Some("secret".to_string()),
            ..TbankExecutionClientConfig::default()
        };
        with_env_vars(&[(SANDBOX_ACCOUNT_ID_ENV, None)], || {
            assert!(matches!(
                config.validate(),
                Err(TbankAdapterError::MissingAccountId)
            ));
        });
    }

    #[test]
    fn live_submit_requires_explicit_allow_live_trading() {
        let config = TbankExecutionClientConfig {
            environment: TbankEnvironment::Live,
            token: Some("secret".to_string()),
            account_id: Some("account".to_string()),
            enable_trading: true,
            allow_live_trading: false,
            ..TbankExecutionClientConfig::default()
        };
        assert!(matches!(
            config.ensure_submit_allowed(),
            Err(TbankAdapterError::PermissionDenied(_))
        ));
    }

    #[test]
    fn sandbox_submit_requires_enable_trading() {
        let config = TbankExecutionClientConfig {
            token: Some("secret".to_string()),
            account_id: Some("account".to_string()),
            enable_trading: false,
            ..TbankExecutionClientConfig::default()
        };
        assert!(matches!(
            config.ensure_submit_allowed(),
            Err(TbankAdapterError::PermissionDenied(_))
        ));
    }

    #[test]
    fn serialization_and_debug_omit_credentials() {
        let config = TbankExecutionClientConfig {
            token: Some("secret-token".to_string()),
            account_id: Some("secret-account".to_string()),
            endpoint: Some("https://secret-endpoint@invest-public-api.tbank.ru".to_string()),
            ..TbankExecutionClientConfig::default()
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let debug = format!("{config:?}");

        assert!(!serialized.contains("secret-token"));
        assert!(!serialized.contains("secret-account"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("secret-account"));
        assert!(!debug.contains("secret-endpoint"));
    }

    #[test]
    fn live_node_execution_engine_config_is_copied_for_submit_recovery() {
        let live_config = LiveExecutionEngineConfig {
            inflight_check_interval_ms: 2_000,
            inflight_check_threshold_ms: 1_000,
            inflight_check_retries: 2,
            ..LiveExecutionEngineConfig::default()
        };

        let config = TbankExecutionClientConfig::default()
            .with_live_node_execution_engine_config(&live_config);

        assert_eq!(config.live_node_inflight_check_interval_ms, 2_000);
        assert_eq!(config.live_node_inflight_check_threshold_ms, 1_000);
        assert_eq!(config.live_node_inflight_check_retries, 2);
    }

    #[test]
    fn execution_token_uses_environment_specific_env() {
        with_env_vars(
            &[
                (SANDBOX_TOKEN_ENV, Some("sandbox-env-secret")),
                (LIVE_TOKEN_ENV, Some("live-env-secret")),
            ],
            || {
                let sandbox = TbankExecutionClientConfig::default();
                assert_eq!(
                    sandbox.resolve_token_secret().unwrap().as_str(),
                    "sandbox-env-secret"
                );

                let live = TbankExecutionClientConfig {
                    environment: TbankEnvironment::Live,
                    ..TbankExecutionClientConfig::default()
                };
                assert_eq!(
                    live.resolve_token_secret().unwrap().as_str(),
                    "live-env-secret"
                );
            },
        );
    }
}
