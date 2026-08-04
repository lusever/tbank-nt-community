use std::fmt;

use tonic::{
    Request, Status,
    metadata::{AsciiMetadataValue, MetadataValue},
    service::Interceptor,
};

use crate::common::error::{Result, TbankAdapterError};

#[derive(Clone)]
/// Interceptor which adds redacted T-Bank authentication metadata.
pub struct TbankAuthInterceptor {
    authorization: AsciiMetadataValue,
}

impl TbankAuthInterceptor {
    /// Creates a new instance.
    pub fn new(token: &str) -> Result<Self> {
        let authorization = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|e| TbankAdapterError::ConfigError(format!("invalid auth metadata: {e}")))?;

        Ok(Self { authorization })
    }

    /// Applies authentication and application metadata to a gRPC request.
    pub fn apply<T>(&self, request: &mut Request<T>) {
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request.metadata_mut().insert(
            "x-app-name",
            AsciiMetadataValue::from_static("tbank_nt_community"),
        );
    }
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
    use tonic::Request;

    use super::*;

    #[test]
    fn metadata_includes_authorization_and_app_name() {
        let interceptor = TbankAuthInterceptor::new("secret-token").unwrap();
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
        assert!(!format!("{interceptor:?}").contains("secret-token"));
    }
}
