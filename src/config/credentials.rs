use std::{env, fmt, sync::Arc};

use nautilus_core::string::secret::SecretString;

use crate::{
    common::{Result, TbankAdapterError},
    config::TbankEnvironment,
    grpc::TbankAuthInterceptor,
};

/// Owns an optional explicitly configured credential separately from runtime settings.
#[derive(Clone, Default)]
pub(crate) struct TbankCredentialSource {
    configured: Option<Arc<SecretString>>,
}

impl TbankCredentialSource {
    pub(crate) fn take_from_config(token: &mut Option<SecretString>) -> Self {
        Self {
            configured: token.take().map(Arc::new),
        }
    }

    pub(crate) fn resolve(&self, environment: TbankEnvironment) -> Result<Arc<SecretString>> {
        let credential = match &self.configured {
            Some(credential) => Arc::clone(credential),
            None => Arc::new(read_environment_token(environment)?),
        };
        TbankAuthInterceptor::validate_secret(&credential)?;
        Ok(credential)
    }

    pub(crate) fn from_resolved(credential: Arc<SecretString>) -> Self {
        Self {
            configured: Some(credential),
        }
    }
}

impl fmt::Debug for TbankCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TbankCredentialSource")
            .field("token_present", &self.configured.is_some())
            .finish()
    }
}

pub(crate) fn resolve_token_secret(
    configured: Option<&SecretString>,
    environment: TbankEnvironment,
) -> Result<SecretString> {
    let token = match configured {
        Some(token) => token.clone(),
        None => read_environment_token(environment)?,
    };
    TbankAuthInterceptor::validate_secret(&token)?;
    Ok(token)
}

pub(crate) fn validate_token_source(
    configured: Option<&SecretString>,
    environment: TbankEnvironment,
) -> Result<()> {
    if let Some(token) = configured {
        return TbankAuthInterceptor::validate_secret(token);
    }

    let token = read_environment_token(environment)?;
    TbankAuthInterceptor::validate_secret(&token)
}

fn read_environment_token(environment: TbankEnvironment) -> Result<SecretString> {
    let token = env::var(environment.token_env()).map_err(|_| TbankAdapterError::MissingToken)?;
    // Transfer std::env's owned allocation directly into Nautilus' zeroizing owner.
    Ok(SecretString::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_token_moves_out_of_settings_and_is_shared_without_copying() {
        let value = "shared-configured-token";
        let mut token = Some(SecretString::from(value));
        let credentials = TbankCredentialSource::take_from_config(&mut token);

        assert!(token.is_none());
        let first = credentials.resolve(TbankEnvironment::Live).unwrap();
        let second = credentials.resolve(TbankEnvironment::Sandbox).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.expose_secret(), value);
        assert!(!format!("{credentials:?}").contains(value));
        assert!(!format!("{first:?}").contains(value));
    }
}
