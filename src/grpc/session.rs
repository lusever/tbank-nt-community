//! Application-owned broker transport, independent of client settings and instrument state.
use super::{TbankAuthInterceptor, TbankGrpcClients, connect_channel};
use crate::{
    common::{Result, TbankAdapterError},
    config::TbankEnvironment,
};
use nautilus_core::string::secret::SecretString;
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;

struct SessionInner {
    channel: Channel,
    credentials: Arc<SecretString>,
    environment: TbankEnvironment,
    endpoint: String,
}
/// Cloneable broker session. The owner decides its lifetime; clients only borrow cloned handles.
#[derive(Clone)]
pub struct TbankSession(Arc<SessionInner>);
impl TbankSession {
    /// Opens an authenticated transport using adapter endpoint and TLS rules.
    pub async fn connect(
        environment: TbankEnvironment,
        endpoint: Option<&str>,
        credentials: Arc<SecretString>,
        timeout: Duration,
    ) -> Result<Self> {
        let endpoint =
            crate::config::validate_endpoint(endpoint.unwrap_or(environment.default_endpoint()))?;
        TbankAuthInterceptor::validate_secret(&credentials)?;
        let channel = connect_channel(&endpoint, timeout).await?;
        Ok(Self(Arc::new(SessionInner {
            channel,
            credentials,
            environment,
            endpoint,
        })))
    }
    /// Returns service clients sharing this session's transport and credentials.
    pub fn clients(&self) -> TbankGrpcClients<TbankAuthInterceptor> {
        TbankGrpcClients::new(
            self.channel(),
            TbankAuthInterceptor::new(self.credentials()).expect("validated session credentials"),
        )
    }
    /// Creates an authenticated view on the same transport, for example for account discovery.
    pub fn with_credentials(&self, credentials: Arc<SecretString>) -> Result<Self> {
        TbankAuthInterceptor::validate_secret(&credentials)?;
        Ok(Self(Arc::new(SessionInner {
            channel: self.channel(),
            credentials,
            environment: self.0.environment,
            endpoint: self.0.endpoint.clone(),
        })))
    }
    pub(crate) fn channel(&self) -> Channel {
        self.0.channel.clone()
    }
    pub(crate) fn credentials(&self) -> Arc<SecretString> {
        Arc::clone(&self.0.credentials)
    }
    pub(crate) fn validate_settings(
        &self,
        environment: TbankEnvironment,
        endpoint: &str,
    ) -> Result<()> {
        if self.0.environment != environment || self.0.endpoint != endpoint {
            return Err(TbankAdapterError::ConfigError(
                "session environment or endpoint differs from client settings".into(),
            ));
        }
        Ok(())
    }
    /// Venue environment of this transport.
    pub(crate) fn environment(&self) -> TbankEnvironment {
        self.0.environment
    }

    /// Tests whether a credential belongs to this authenticated session.
    pub fn uses_credentials(&self, credentials: &SecretString) -> bool {
        self.0.credentials.expose_secret() == credentials.expose_secret()
    }
}
impl std::fmt::Debug for TbankSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TbankSession")
            .field("environment", &self.0.environment)
            .finish_non_exhaustive()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn session() -> TbankSession {
        TbankSession(Arc::new(SessionInner {
            channel: Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            credentials: Arc::new(SecretString::from("session-token")),
            environment: TbankEnvironment::Sandbox,
            endpoint: "http://127.0.0.1:1".into(),
        }))
    }
    #[tokio::test]
    async fn session_binding_rejects_different_environment_endpoint_and_credentials() {
        let session = session();
        for config in [
            crate::config::TbankDataClientConfig {
                environment: TbankEnvironment::Live,
                endpoint: Some("http://127.0.0.1:1".into()),
                ..Default::default()
            },
            crate::config::TbankDataClientConfig {
                endpoint: Some("http://127.0.0.1:2".into()),
                ..Default::default()
            },
            crate::config::TbankDataClientConfig {
                endpoint: Some("http://127.0.0.1:1".into()),
                token: Some(SecretString::from("other-token")),
                ..Default::default()
            },
        ] {
            assert!(
                crate::market_data::TbankDataClient::new(config.clone())
                    .with_session(session.clone())
                    .is_err()
            );
            assert!(
                crate::instruments::TbankInstrumentProvider::new(config)
                    .with_session(session.clone())
                    .is_err()
            );
        }
        assert!(!format!("{session:?}").contains("session-token"));
        let mut source = crate::config::TbankCredentialSource::default();
        source.bind_session(&session).unwrap();
        assert!(Arc::ptr_eq(
            &source.resolve(TbankEnvironment::Sandbox).unwrap(),
            &session.credentials()
        ));
    }
    #[tokio::test]
    async fn live_node_factories_take_runtime_sessions_without_environment_credentials() {
        use nautilus_common::{
            cache::Cache,
            clock::VirtualClock,
            factories::{DataClientFactory, ExecutionClientFactory},
        };
        use std::{cell::RefCell, rc::Rc};
        let session = session();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let data_config = crate::config::TbankDataClientConfig {
            endpoint: Some("http://127.0.0.1:1".into()),
            ..Default::default()
        };
        let data = crate::factory::TbankDataClientFactory::new()
            .with_session(session.clone())
            .create(
                "TBANK",
                &data_config,
                cache.clone().into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap();
        let execution_config = crate::config::TbankExecutionClientConfig {
            endpoint: Some("http://127.0.0.1:1".into()),
            account_id: Some("test-account".into()),
            ..Default::default()
        };
        let execution = crate::factory::TbankExecutionClientFactory::new()
            .with_session(session)
            .create(
                nautilus_model::identifiers::TraderId::from("TEST-001"),
                "TBANK",
                &execution_config,
                cache.into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap();
        assert_eq!(data.client_id().as_str(), "TBANK");
        assert_eq!(execution.client_id().as_str(), "TBANK");
    }

    #[tokio::test]
    async fn credential_views_preserve_original_credentials() {
        let original = session();
        let same_credentials = original.with_credentials(original.credentials()).unwrap();
        assert!(same_credentials.uses_credentials(&SecretString::from("session-token")));
        let view = original
            .with_credentials(Arc::new(SecretString::from("other-token")))
            .unwrap();
        assert!(original.uses_credentials(&SecretString::from("session-token")));
        assert!(view.uses_credentials(&SecretString::from("other-token")));
        let settings =
            serde_json::to_value(crate::config::TbankExecutionClientConfig::default()).unwrap();
        assert!(settings.get("shared_channel").is_none());
    }
}
