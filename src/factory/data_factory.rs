use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory as NautilusDataClientFactory},
};

use crate::{common::consts::TBANK, config::TbankDataClientConfig, market_data::TbankDataClient};

#[derive(Debug, Clone, Copy, Default)]
/// Factory for creating T-Bank market-data clients.
pub struct TbankDataClientFactory;

impl TbankDataClientFactory {
    /// Creates a new instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl NautilusDataClientFactory for TbankDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<TbankDataClientConfig>()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid config type for TbankDataClientFactory: expected TbankDataClientConfig"
                )
            })?;
        config.validate()?;
        Ok(Box::new(
            TbankDataClient::new(config)
                .with_client_id(name.into())
                .with_cache(cache),
        ))
    }

    fn name(&self) -> &str {
        TBANK
    }

    fn config_type(&self) -> &str {
        "TbankDataClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nautilus_common::{cache::Cache, clock::TestClock};
    use nautilus_model::identifiers::ClientId;

    struct WrongConfig;

    impl std::fmt::Debug for WrongConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("WrongConfig { token: secret-token }")
        }
    }

    impl ClientConfig for WrongConfig {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn dependencies() -> (CacheView, Rc<RefCell<dyn Clock>>) {
        let cache = Rc::new(RefCell::new(Cache::default())).into();
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        (cache, clock)
    }

    #[test]
    fn creates_client_with_requested_identity() {
        let (cache, clock) = dependencies();
        let config = TbankDataClientConfig {
            token: Some("test-token".to_string()),
            ..TbankDataClientConfig::default()
        };
        let client = TbankDataClientFactory::new()
            .create("TBANK-CUSTOM", &config, cache, clock)
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("TBANK-CUSTOM"));
    }

    #[test]
    fn rejects_wrong_config_type() {
        let (cache, clock) = dependencies();
        let error = TbankDataClientFactory::new()
            .create("TBANK", &WrongConfig, cache, clock)
            .err()
            .expect("wrong config type should fail");

        assert!(error.to_string().contains("TbankDataClientConfig"));
        assert!(!error.to_string().contains("secret-token"));
    }

    #[test]
    fn rejects_invalid_config_before_creating_client() {
        let (cache, clock) = dependencies();
        let config = TbankDataClientConfig {
            token: Some("test-token".to_string()),
            endpoint: Some("http://example.com".to_string()),
            ..TbankDataClientConfig::default()
        };

        let error = TbankDataClientFactory::new()
            .create("TBANK", &config, cache, clock)
            .err()
            .expect("invalid config should fail at the factory boundary");

        assert!(
            error
                .to_string()
                .contains("invalid or insecure T-Bank endpoint")
        );
    }
}
