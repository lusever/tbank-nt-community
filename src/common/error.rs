use thiserror::Error;

/// Result type returned by adapter operations.
pub type Result<T> = std::result::Result<T, TbankAdapterError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
/// Errors produced by configuration, conversion, transport, and execution operations.
pub enum TbankAdapterError {
    /// Configuration is invalid.
    #[error("config error: {0}")]
    ConfigError(String),
    /// No API token was configured.
    #[error("missing T-Bank Invest API token")]
    MissingToken,
    /// No broker account ID was configured.
    #[error("missing T-Bank account id")]
    MissingAccountId,
    /// The configured endpoint is invalid or insecure.
    #[error("invalid or insecure T-Bank endpoint")]
    InvalidEndpoint,
    /// The T-Bank gRPC API returned an error status.
    #[error("gRPC status ({code:?}): {message}")]
    GrpcStatus {
        /// gRPC status code.
        code: tonic::Code,
        /// Redacted status message.
        message: String,
    },
    /// The requested operation is not permitted.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// The broker rate limit was reached.
    #[error("rate limited: {0}")]
    RateLimited(String),
    /// The requested instrument was not found.
    #[error("instrument not found: {0}")]
    InstrumentNotFound(String),
    /// The requested instrument is unsupported.
    #[error("unsupported instrument: {0}")]
    UnsupportedInstrument(String),
    /// The requested order type is unsupported.
    #[error("unsupported order type: {0}")]
    UnsupportedOrderType(String),
    /// The requested time-in-force is unsupported.
    #[error("unsupported time in force: {0}")]
    UnsupportedTimeInForce(String),
    /// An order quantity is invalid.
    #[error("invalid quantity: {0}")]
    InvalidQuantity(String),
    /// An order price is invalid.
    #[error("invalid price: {0}")]
    InvalidPrice(String),
    /// A share quantity is not an exact lot multiple.
    #[error("quantity {quantity} is not divisible by lot size {lot}")]
    QuantityNotMultipleOfLot {
        /// Requested share quantity.
        quantity: String,
        /// Instrument lot size.
        lot: u32,
    },
    /// A price is not aligned to the instrument tick size.
    #[error("price {price} is not divisible by tick size {tick}")]
    PriceNotMultipleOfTick {
        /// Requested price.
        price: String,
        /// Required tick size.
        tick: String,
    },
    /// A value could not be converted between broker and Nautilus representations.
    #[error("conversion error: {0}")]
    ConversionError(String),
    /// Reconnection attempts were exhausted.
    #[error("reconnect failed: {0}")]
    ReconnectFailed(String),
}

impl From<tonic::Status> for TbankAdapterError {
    fn from(status: tonic::Status) -> Self {
        let message = grpc_status_message(&status);
        match status.code() {
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                Self::PermissionDenied(message)
            }
            tonic::Code::ResourceExhausted => Self::RateLimited(message),
            _ => Self::GrpcStatus {
                code: status.code(),
                message,
            },
        }
    }
}

fn grpc_status_message(status: &tonic::Status) -> String {
    status.message().to_string()
}

#[cfg(test)]
mod tests {
    use tonic::{Code, metadata::MetadataValue};

    use super::*;

    #[test]
    fn grpc_error_does_not_expose_response_metadata() {
        let mut status = tonic::Status::new(Code::ResourceExhausted, "too many requests");
        status
            .metadata_mut()
            .insert("x-tracking-id", MetadataValue::from_static("tracking-123"));
        status
            .metadata_mut()
            .insert("x-ratelimit-reset", MetadataValue::from_static("7"));

        let error = TbankAdapterError::from(status);

        assert!(matches!(
            error,
            TbankAdapterError::RateLimited(message) if message == "too many requests"
        ));
    }
}
