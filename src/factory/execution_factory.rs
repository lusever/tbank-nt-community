use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory as NautilusExecutionClientFactory},
};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_model::enums::{AccountType, OmsType};

use crate::{
    common::consts::{TBANK, TBANK_VENUE},
    common::venue::TbankVenue,
    config::TbankExecutionClientConfig,
    execution::{TbankExecutionClient, tbank_account_id},
};

/// Factory for creating T-Bank execution clients.
#[derive(Debug, Clone, Copy, Default)]
pub struct TbankExecutionClientFactory;

impl TbankExecutionClientFactory {
    /// Creates a new instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl NautilusExecutionClientFactory for TbankExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<TbankExecutionClientConfig>()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid config type for TbankExecutionClientFactory: expected TbankExecutionClientConfig"
                )
            })?;
        config.validate()?;
        let account_id = tbank_account_id(&config.resolve_account_id()?);
        let core = ExecutionClientCore::new(
            config.trader_id,
            name.into(),
            *TBANK_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let mut client = TbankExecutionClient::new(core, config);
        for venue in TbankVenue::all() {
            for instrument in cache.borrow().instruments(&venue.venue(), None) {
                client.on_instrument(instrument.clone());
            }
        }
        Ok(Box::new(client))
    }

    fn name(&self) -> &str {
        TBANK
    }

    fn config_type(&self) -> &str {
        "TbankExecutionClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nautilus_common::cache::Cache;
    use nautilus_model::identifiers::ClientId;
    use std::{cell::RefCell, rc::Rc};

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

    fn cache() -> CacheView {
        Rc::new(RefCell::new(Cache::default())).into()
    }

    #[test]
    fn creates_client_with_requested_identity() {
        let config = TbankExecutionClientConfig {
            token: Some("test-token".to_string()),
            account_id: Some("account".to_string()),
            ..TbankExecutionClientConfig::default()
        };
        let client = TbankExecutionClientFactory::new()
            .create("TBANK-CUSTOM", &config, cache())
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("TBANK-CUSTOM"));
        assert_eq!(
            TbankExecutionClientFactory.config_type(),
            "TbankExecutionClientConfig"
        );
    }

    #[test]
    fn rejects_wrong_config_type() {
        let error = TbankExecutionClientFactory::new()
            .create("TBANK", &WrongConfig, cache())
            .err()
            .expect("wrong config type should fail");

        assert!(error.to_string().contains("TbankExecutionClientConfig"));
        assert!(!error.to_string().contains("secret-token"));
    }

    #[test]
    fn rejects_invalid_config_before_creating_client() {
        let config = TbankExecutionClientConfig {
            token: Some("test-token".to_string()),
            account_id: Some("account".to_string()),
            endpoint: Some("http://example.com".to_string()),
            ..TbankExecutionClientConfig::default()
        };

        let error = TbankExecutionClientFactory::new()
            .create("TBANK", &config, cache())
            .err()
            .expect("invalid config should fail at the factory boundary");

        assert!(
            error
                .to_string()
                .contains("invalid or insecure T-Bank endpoint")
        );
    }
}
