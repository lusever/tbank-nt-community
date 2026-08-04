use std::{error::Error, io, time::Duration};

use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use crate::common::error::{Result, TbankAdapterError};

const RUSSIAN_TRUSTED_ROOT_CA: &[u8] = include_bytes!("../../certs/russian_trusted_root_ca.pem");
const RUSSIAN_TRUSTED_SUB_CA: &[u8] = include_bytes!("../../certs/russian_trusted_sub_ca.pem");

/// Builds the T-Bank TLS trust configuration with public roots and both
/// certificates required by the Russian Trusted CA migration.
#[must_use]
pub fn tbank_tls_config() -> ClientTlsConfig {
    ClientTlsConfig::new()
        .with_enabled_roots()
        .ca_certificate(Certificate::from_pem(RUSSIAN_TRUSTED_ROOT_CA))
        .ca_certificate(Certificate::from_pem(RUSSIAN_TRUSTED_SUB_CA))
}

/// Connects a TLS gRPC channel to a validated endpoint.
pub async fn connect_channel(endpoint_uri: &str, timeout: Duration) -> Result<Channel> {
    let mut endpoint = Channel::from_shared(endpoint_uri.to_string())
        .map_err(|_| TbankAdapterError::InvalidEndpoint)?
        .connect_timeout(timeout);

    if endpoint_uri.starts_with("https://") {
        endpoint = endpoint
            .tls_config(tbank_tls_config())
            .map_err(|e| TbankAdapterError::ConfigError(format!("TLS config failed: {e}")))?;
    }

    endpoint
        .connect()
        .await
        .map_err(|e| TbankAdapterError::GrpcStatus {
            code: tonic::Code::Unavailable,
            message: classify_transport_error(&e),
        })
}

fn classify_transport_error(error: &tonic::transport::Error) -> String {
    let mut cause: &(dyn Error + 'static) = error;
    loop {
        if let Some(io_error) = cause.downcast_ref::<io::Error>() {
            return match io_error.kind() {
                io::ErrorKind::ConnectionRefused => {
                    "TCP connection refused by the remote endpoint".to_string()
                }
                io::ErrorKind::TimedOut => {
                    "TCP connection to the remote endpoint timed out".to_string()
                }
                io::ErrorKind::NotFound => "remote endpoint DNS resolution failed".to_string(),
                kind => format!("transport I/O failure ({kind:?})"),
            };
        }

        let message = cause.to_string().to_ascii_lowercase();
        if message.contains("certificate")
            || message.contains("unknown issuer")
            || message.contains("invalid peer certificate")
        {
            return "TLS certificate validation failed".to_string();
        }

        let Some(source) = cause.source() else {
            return "transport connection failed".to_string();
        };
        cause = source;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendored_russian_ca_chain_is_accepted_by_tls_config() {
        Channel::from_static("https://example.com")
            .tls_config(tbank_tls_config())
            .expect("vendored CA PEM files must be valid");
    }
}
