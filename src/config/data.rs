use std::{
    collections::{HashMap, HashSet},
    env, fmt,
    time::Duration,
};

use nautilus_common::factories::ClientConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    common::{
        consts::DEFAULT_REQUEST_TIMEOUT,
        error::{Result, TbankAdapterError},
    },
    config::{TbankEnvironment, TbankReconnectPolicy},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Interpretation of T-Bank candle timestamps.
pub enum TbankCandleTimestampMode {
    /// Treat the broker candle start timestamp as the Nautilus bar end.
    StartAsBarEnd,
    /// Treat the broker candle start timestamp as the Nautilus bar start.
    StartAsBarStart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Explicit Nautilus metadata for a non-tradable T-Bank indicative instrument.
pub struct TbankIndicativeInstrumentConfig {
    /// ISO 4217 currency used to denominate the index value.
    pub currency: String,
    /// Canonical minimum price increment and source of price precision.
    pub price_increment: Decimal,
}

#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
/// Configuration for the T-Bank market-data client.
pub struct TbankDataClientConfig {
    #[builder(default = TbankEnvironment::Sandbox)]
    /// Broker environment.
    pub environment: TbankEnvironment,
    #[serde(skip_serializing)]
    /// Optional API token; environment lookup is used when absent.
    pub token: Option<String>,
    /// Optional gRPC endpoint override.
    pub endpoint: Option<String>,
    #[builder(default = DEFAULT_REQUEST_TIMEOUT)]
    /// Timeout for unary broker requests.
    pub request_timeout: Duration,
    #[builder(default)]
    /// Stream reconnect backoff policy.
    pub reconnect_policy: TbankReconnectPolicy,
    #[builder(default = true)]
    /// Whether active subscriptions are restored after reconnecting.
    pub subscriptions_on_reconnect: bool,
    #[builder(default = TbankCandleTimestampMode::StartAsBarEnd)]
    /// Mapping used for broker candle timestamps.
    pub candle_timestamp_mode: TbankCandleTimestampMode,
    #[builder(default = Duration::ZERO)]
    /// Delay inserted between historical candle requests.
    pub historical_candle_request_delay: Duration,
    #[builder(default = DEFAULT_REQUEST_TIMEOUT)]
    /// Timeout for historical candle requests.
    pub historical_candle_request_timeout: Duration,
    #[builder(default = 0)]
    /// Maximum number of historical candle retries.
    pub historical_candle_max_retries: u32,
    #[builder(default = Duration::from_millis(1_000))]
    /// Base delay for historical candle retry backoff.
    pub historical_candle_retry_base_delay: Duration,
    #[builder(default)]
    /// Explicit Nautilus instrument IDs mapped to broker stream IDs.
    ///
    /// Futures must be listed here to be loaded: their current tick-value contract requires a
    /// separate broker request per instrument and is therefore not auto-discovered.
    pub instrument_stream_ids: HashMap<String, String>,
    #[builder(default)]
    /// Explicit definitions for non-tradable indicative instruments requested from T-Bank.
    pub indicative_instruments: HashMap<String, TbankIndicativeInstrumentConfig>,
    #[builder(default)]
    /// Instruments whose candles are periodically polled.
    pub periodic_candle_poll_instrument_ids: HashSet<String>,
    #[builder(default = Duration::from_secs(5))]
    /// Interval between periodic candle polls.
    pub periodic_candle_poll_interval: Duration,
    #[builder(default = 50)]
    /// Maximum candle subscriptions placed on one stream.
    pub max_candle_instruments_per_stream: usize,
    #[builder(default = 25)]
    /// Maximum market-data stream reconnect attempts.
    pub max_market_data_reconnect_attempts: u32,
    #[builder(default = Duration::from_secs(180))]
    /// Maximum allowed period without market-data stream traffic.
    pub market_data_stream_idle_timeout: Duration,
    #[builder(default = Duration::from_secs(60 * 60))]
    /// Interval between broker instrument-catalog refreshes; zero disables refresh.
    pub instrument_refresh_interval: Duration,
}

impl fmt::Debug for TbankDataClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TbankDataClientConfig")
            .field("environment", &self.environment)
            .field("token_present", &self.token.is_some())
            .field("endpoint_present", &self.endpoint.is_some())
            .field("request_timeout", &self.request_timeout)
            .field("reconnect_policy", &self.reconnect_policy)
            .field(
                "subscriptions_on_reconnect",
                &self.subscriptions_on_reconnect,
            )
            .field("candle_timestamp_mode", &self.candle_timestamp_mode)
            .field(
                "historical_candle_request_delay",
                &self.historical_candle_request_delay,
            )
            .field(
                "historical_candle_request_timeout",
                &self.historical_candle_request_timeout,
            )
            .field(
                "historical_candle_max_retries",
                &self.historical_candle_max_retries,
            )
            .field(
                "historical_candle_retry_base_delay",
                &self.historical_candle_retry_base_delay,
            )
            .field("instrument_stream_ids", &self.instrument_stream_ids.len())
            .field("indicative_instruments", &self.indicative_instruments.len())
            .field(
                "periodic_candle_poll_instrument_ids",
                &self.periodic_candle_poll_instrument_ids.len(),
            )
            .field(
                "periodic_candle_poll_interval",
                &self.periodic_candle_poll_interval,
            )
            .field(
                "max_candle_instruments_per_stream",
                &self.max_candle_instruments_per_stream,
            )
            .field(
                "max_market_data_reconnect_attempts",
                &self.max_market_data_reconnect_attempts,
            )
            .field(
                "market_data_stream_idle_timeout",
                &self.market_data_stream_idle_timeout,
            )
            .field(
                "instrument_refresh_interval",
                &self.instrument_refresh_interval,
            )
            .finish()
    }
}

impl Default for TbankDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl TbankDataClientConfig {
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

    /// Validates the configuration.
    pub fn validate(&self) -> Result<()> {
        self.endpoint_uri()?;
        self.resolve_token_secret()?;
        if self.max_candle_instruments_per_stream == 0 {
            return Err(TbankAdapterError::ConfigError(
                "max_candle_instruments_per_stream must be positive".to_string(),
            ));
        }
        for (instrument_id, definition) in &self.indicative_instruments {
            if !instrument_id.ends_with(".MOEX")
                || definition.price_increment <= Decimal::ZERO
                || crate::common::currency::currency_from_code(&definition.currency).is_err()
            {
                return Err(TbankAdapterError::ConfigError(format!(
                    "invalid indicative definition for {instrument_id}: currency={} price_increment={}",
                    definition.currency, definition.price_increment
                )));
            }
        }
        if !self.periodic_candle_poll_instrument_ids.is_empty()
            && self.periodic_candle_poll_interval.is_zero()
        {
            return Err(TbankAdapterError::ConfigError(
                "periodic_candle_poll_interval must be positive when polling instruments"
                    .to_string(),
            ));
        }
        if self.max_market_data_reconnect_attempts == 0 {
            return Err(TbankAdapterError::ConfigError(
                "max_market_data_reconnect_attempts must be positive".to_string(),
            ));
        }
        if self.market_data_stream_idle_timeout.is_zero() {
            return Err(TbankAdapterError::ConfigError(
                "market_data_stream_idle_timeout must be positive".to_string(),
            ));
        }
        if self.historical_candle_request_timeout.is_zero() {
            return Err(TbankAdapterError::ConfigError(
                "historical_candle_request_timeout must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

impl ClientConfig for TbankDataClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<String> {
    let parsed = endpoint
        .parse::<http::Uri>()
        .map_err(|_| TbankAdapterError::InvalidEndpoint)?;
    let secure = parsed.scheme_str() == Some("https");
    let loopback = parsed.host().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    let clean_origin = parsed
        .authority()
        .is_some_and(|authority| !authority.as_str().contains('@'))
        && parsed.path() == "/"
        && parsed.query().is_none()
        && !endpoint.contains('#')
        && parsed.scheme().is_some();
    if !(secure || parsed.scheme_str() == Some("http") && loopback) || !clean_origin {
        return Err(TbankAdapterError::InvalidEndpoint);
    }

    tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .map_err(|_| TbankAdapterError::InvalidEndpoint)?;
    Ok(endpoint.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::consts::{
        LIVE_ENDPOINT, LIVE_TOKEN_ENV, SANDBOX_ENDPOINT, SANDBOX_TOKEN_ENV,
    };

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
    fn default_sandbox_endpoint() {
        let config = TbankDataClientConfig::default();
        assert_eq!(config.endpoint_uri().unwrap(), SANDBOX_ENDPOINT);
    }

    #[test]
    fn default_live_endpoint() {
        let config = TbankDataClientConfig {
            environment: TbankEnvironment::Live,
            ..TbankDataClientConfig::default()
        };
        assert_eq!(config.endpoint_uri().unwrap(), LIVE_ENDPOINT);
    }

    #[test]
    fn endpoint_override_requires_scheme() {
        let config = TbankDataClientConfig {
            endpoint: Some("invest-public-api.tbank.ru:443".to_string()),
            ..TbankDataClientConfig::default()
        };
        assert!(matches!(
            config.endpoint_uri(),
            Err(TbankAdapterError::InvalidEndpoint)
        ));
    }

    #[test]
    fn remote_plaintext_endpoint_is_rejected() {
        assert!(matches!(
            validate_endpoint("http://invest-public-api.tbank.ru"),
            Err(TbankAdapterError::InvalidEndpoint)
        ));
    }

    #[test]
    fn endpoint_rejects_loggable_credentials_and_non_origin_components() {
        for endpoint in [
            "https://secret@invest-public-api.tbank.ru",
            "https://invest-public-api.tbank.ru/path",
            "https://invest-public-api.tbank.ru/?token=secret",
            "https://invest-public-api.tbank.ru/#secret",
        ] {
            let error = validate_endpoint(endpoint).unwrap_err();
            assert_eq!(error, TbankAdapterError::InvalidEndpoint);
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn debug_does_not_expose_unvalidated_endpoint() {
        let config = TbankDataClientConfig {
            endpoint: Some("https://secret@invest-public-api.tbank.ru".to_string()),
            ..TbankDataClientConfig::default()
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("endpoint_present: true"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn loopback_plaintext_endpoint_is_allowed_for_tests() {
        assert_eq!(
            validate_endpoint("http://127.0.0.1:8080").unwrap(),
            "http://127.0.0.1:8080"
        );
        assert_eq!(
            validate_endpoint("http://[::1]:8080").unwrap(),
            "http://[::1]:8080"
        );
    }

    #[test]
    fn explicit_token_validates() {
        with_env_vars(
            &[
                (SANDBOX_TOKEN_ENV, Some("sandbox-env-secret")),
                (LIVE_TOKEN_ENV, Some("live-env-secret")),
            ],
            || {
                let config = TbankDataClientConfig {
                    token: Some("secret".to_string()),
                    ..TbankDataClientConfig::default()
                };
                assert_eq!(config.resolve_token_secret().unwrap().as_str(), "secret");
                assert!(!format!("{config:?}").contains("secret"));
            },
        );
    }

    #[test]
    fn serialization_omits_token() {
        let config = TbankDataClientConfig {
            token: Some("secret".to_string()),
            ..TbankDataClientConfig::default()
        };

        let serialized = serde_json::to_string(&config).unwrap();

        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("token"));
    }

    #[test]
    fn stream_idle_timeout_must_be_positive() {
        let config = TbankDataClientConfig {
            token: Some("secret".to_string()),
            market_data_stream_idle_timeout: Duration::ZERO,
            ..TbankDataClientConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(TbankAdapterError::ConfigError(message))
                if message == "market_data_stream_idle_timeout must be positive"
        ));
    }

    #[test]
    fn historical_candle_request_timeout_must_be_positive() {
        let config = TbankDataClientConfig {
            token: Some("secret".to_string()),
            historical_candle_request_timeout: Duration::ZERO,
            ..TbankDataClientConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(TbankAdapterError::ConfigError(message))
                if message == "historical_candle_request_timeout must be positive"
        ));
    }

    #[test]
    fn periodic_candle_poll_interval_must_be_positive_when_enabled() {
        let config = TbankDataClientConfig {
            token: Some("secret".to_string()),
            periodic_candle_poll_instrument_ids: HashSet::from(["index-uid".to_string()]),
            periodic_candle_poll_interval: Duration::ZERO,
            ..TbankDataClientConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(TbankAdapterError::ConfigError(message))
                if message == "periodic_candle_poll_interval must be positive when polling instruments"
        ));
    }

    #[test]
    fn instrument_catalogue_refresh_is_enabled_by_default() {
        assert_eq!(
            TbankDataClientConfig::default().instrument_refresh_interval,
            Duration::from_secs(60 * 60)
        );
    }

    #[test]
    fn indicative_definition_requires_explicit_known_currency() {
        let config = TbankDataClientConfig {
            token: Some("secret".to_string()),
            indicative_instruments: HashMap::from([(
                "IMOEX2.MOEX".to_string(),
                TbankIndicativeInstrumentConfig {
                    currency: String::new(),
                    price_increment: Decimal::new(1, 8),
                },
            )]),
            ..TbankDataClientConfig::default()
        };

        assert!(matches!(
            config.validate(),
            Err(TbankAdapterError::ConfigError(message))
                if message.contains("invalid indicative definition for IMOEX2.MOEX")
        ));
    }

    #[test]
    fn sandbox_uses_sandbox_token_env() {
        with_env_vars(
            &[
                (SANDBOX_TOKEN_ENV, Some("sandbox-env-secret")),
                (LIVE_TOKEN_ENV, Some("live-env-secret")),
            ],
            || {
                let config = TbankDataClientConfig::default();
                assert_eq!(
                    config.resolve_token_secret().unwrap().as_str(),
                    "sandbox-env-secret"
                );
            },
        );
    }

    #[test]
    fn live_uses_live_token_env() {
        with_env_vars(
            &[
                (SANDBOX_TOKEN_ENV, Some("sandbox-env-secret")),
                (LIVE_TOKEN_ENV, Some("live-env-secret")),
            ],
            || {
                let config = TbankDataClientConfig {
                    environment: TbankEnvironment::Live,
                    ..TbankDataClientConfig::default()
                };
                assert_eq!(
                    config.resolve_token_secret().unwrap().as_str(),
                    "live-env-secret"
                );
            },
        );
    }
}
