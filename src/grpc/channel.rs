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

/// Connects a gRPC channel on the Nautilus runtime, independent of the caller's executor.
pub async fn connect_channel(endpoint_uri: &str, timeout: Duration) -> Result<Channel> {
    let mut endpoint = Channel::from_shared(endpoint_uri.to_string())
        .map_err(|_| TbankAdapterError::InvalidEndpoint)?
        .connect_timeout(timeout);

    if endpoint_uri.starts_with("https://") {
        endpoint = endpoint
            .tls_config(tbank_tls_config())
            .map_err(|e| TbankAdapterError::ConfigError(format!("TLS config failed: {e}")))?;
    }

    // Execution operation tasks already run on the global Nautilus runtime. The
    // socket, channel buffer and HTTP/2 driver must live there too: a consumer's
    // LocalSet/current-thread runtime may be blocked by synchronous persistence.
    // Entering only a Handle or moving an already connected Channel would leave
    // I/O resources and spawned connection tasks bound to that consumer runtime.
    let mut connecting = tokio::task::JoinSet::new();
    connecting.spawn_on(
        async move {
            endpoint
                .connect()
                .await
                .map_err(|e| TbankAdapterError::GrpcStatus {
                    code: tonic::Code::Unavailable,
                    message: classify_transport_error(&e),
                })
        },
        nautilus_common::live::get_runtime().handle(),
    );
    // JoinSet owns and aborts the connection attempt if this future is cancelled.
    connecting
        .join_next()
        .await
        .expect("one owned connect task")
        .map_err(|_| TbankAdapterError::GrpcStatus {
            code: tonic::Code::Unavailable,
            message: "transport connection task failed".into(),
        })?
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

    #[tokio::test(flavor = "current_thread")]
    async fn channel_progresses_when_caller_runtime_is_blocked() {
        use crate::grpc::generated::{
            GetUserTariffRequest, users_service_client::UsersServiceClient,
            users_service_server::UsersServiceServer,
        };
        use tokio_stream::wrappers::TcpListenerStream;

        // Both the mock broker and the command worker are independent of the host
        // current-thread runtime, just like Nautilus execution operation tasks.
        let runtime = nautilus_common::live::get_runtime();
        let listener = runtime
            .spawn(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() })
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn_on(
            async move {
                tonic::transport::Server::builder()
                    .add_service(UsersServiceServer::new(crate::testing::QuotaUsersService))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            },
            runtime.handle(),
        );
        let channel = connect_channel(&format!("http://{address}"), Duration::from_secs(5))
            .await
            .unwrap();
        let mut client = UsersServiceClient::new(channel);
        client
            .get_user_tariff(GetUserTariffRequest {})
            .await
            .unwrap();
        let (completed, result) = std::sync::mpsc::channel();
        tasks.spawn_on(
            async move {
                let response = client.get_user_tariff(GetUserTariffRequest {}).await;
                completed.send(response.is_ok()).unwrap();
            },
            runtime.handle(),
        );

        // Deliberately block the host executor as a synchronous disk write would.
        // A channel owned by Nautilus must still complete its read-only RPC.
        assert_eq!(result.recv_timeout(Duration::from_secs(2)), Ok(true));
    }

    #[test]
    fn vendored_russian_ca_chain_is_accepted_by_tls_config() {
        Channel::from_static("https://example.com")
            .tls_config(tbank_tls_config())
            .expect("vendored CA PEM files must be valid");
    }
}
