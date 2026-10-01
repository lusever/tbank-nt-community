use std::{fmt, sync::Arc};

use nautilus_core::string::secret::SecretString;
use tonic::{
    Request, Status,
    metadata::{AsciiMetadataValue, MetadataValue},
    service::Interceptor,
};
use zeroize::Zeroizing;

use crate::common::error::{Result, TbankAdapterError};

#[derive(Clone)]
/// Interceptor which adds redacted T-Bank authentication metadata.
pub struct TbankAuthInterceptor {
    credentials: Arc<SecretString>,
}

impl TbankAuthInterceptor {
    /// Creates a new instance.
    pub fn new(credentials: Arc<SecretString>) -> Result<Self> {
        Self::validate_secret(&credentials)?;
        Ok(Self { credentials })
    }

    pub(crate) fn validate_secret(credentials: &SecretString) -> Result<()> {
        let token = credentials.expose_secret();
        if token.trim().is_empty() {
            return Err(TbankAdapterError::MissingToken);
        }
        // Match Tonic's HeaderValue visible-ASCII check without allocating a throwaway metadata
        // copy during configuration validation or connection setup.
        if !token
            .bytes()
            .all(|byte| (32..127).contains(&byte) || byte == b'\t')
        {
            return Err(TbankAdapterError::ConfigError(
                "invalid auth metadata".to_string(),
            ));
        }
        Ok(())
    }

    /// Applies authentication and application metadata to a gRPC request.
    pub fn apply<T>(&self, request: &mut Request<T>) {
        // Construction validates this immutable credential; request metadata is rebuilt for each
        // request so the interceptor never retains a long-lived Bearer value.
        let authorization = authorization_metadata(&self.credentials)
            .expect("T-Bank auth credential was validated when the interceptor was created");
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.metadata_mut().insert(
            "x-app-name",
            AsciiMetadataValue::from_static("tbank_nt_community"),
        );
    }
}

fn authorization_metadata(credentials: &SecretString) -> Result<AsciiMetadataValue> {
    TbankAuthInterceptor::validate_secret(credentials)?;
    let token = credentials.expose_secret();

    // Tonic copies this borrowed value into its HeaderValue-owned bytes. The temporary source
    // buffer is zeroized on drop; request/transport-owned copies have their own lifecycle.
    let bearer = Zeroizing::new(format!("Bearer {token}"));
    let mut authorization = MetadataValue::try_from(bearer.as_str())
        .map_err(|_| TbankAdapterError::ConfigError("invalid auth metadata".to_string()))?;
    authorization.set_sensitive(true);
    Ok(authorization)
}

impl fmt::Debug for TbankAuthInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TbankAuthInterceptor")
            .field("token_present", &true)
            .field("app_name", &"tbank_nt_community")
            .finish()
    }
}

impl Interceptor for TbankAuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        self.apply(&mut request);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nautilus_core::string::secret::SecretString;
    use tonic::Request;
    use zeroize::ZeroizeOnDrop;

    use super::*;

    #[test]
    fn metadata_includes_authorization_and_app_name() {
        let interceptor =
            TbankAuthInterceptor::new(Arc::new(SecretString::from("secret-token"))).unwrap();
        let mut request = Request::new(());
        interceptor.apply(&mut request);

        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer secret-token"
        );
        assert_eq!(
            request.metadata().get("x-app-name").unwrap(),
            "tbank_nt_community"
        );
        assert!(
            request
                .metadata()
                .get("authorization")
                .unwrap()
                .is_sensitive()
        );
        assert!(!format!("{interceptor:?}").contains("secret-token"));
    }

    #[test]
    fn cloned_interceptor_keeps_credential_alive_for_admitted_request() {
        let interceptor =
            TbankAuthInterceptor::new(Arc::new(SecretString::from("recovery-token"))).unwrap();
        let admitted = interceptor.clone();
        drop(interceptor);

        let mut request = Request::new(());
        admitted.apply(&mut request);
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer recovery-token"
        );
        assert!(
            request
                .metadata()
                .get("authorization")
                .unwrap()
                .is_sensitive()
        );
    }

    #[test]
    fn invalid_token_errors_are_redacted_and_secret_is_zeroizing_owner() {
        fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}

        assert_zeroize_on_drop::<SecretString>();
        let token = "bad-é-secret";
        let error = TbankAuthInterceptor::new(Arc::new(SecretString::from(token))).unwrap_err();
        assert!(!error.to_string().contains(token));
        assert!(!format!("{error:?}").contains(token));
    }
}
