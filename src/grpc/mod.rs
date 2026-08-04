//! T-Bank gRPC transport primitives and generated API contracts.

/// TLS channel construction.
pub mod channel;
/// Typed gRPC service clients.
pub mod clients;
#[allow(missing_docs)]
pub mod generated;
/// Authentication metadata interceptor.
pub mod metadata;
/// Request timeout helpers.
pub mod request;
/// Reconnect retry helpers.
pub mod retry;

pub use channel::{connect_channel, tbank_tls_config};
pub use clients::TbankGrpcClients;
pub use metadata::TbankAuthInterceptor;
pub use request::with_timeout;
