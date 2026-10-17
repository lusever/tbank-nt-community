use std::time::Duration;

use tonic::Request;

/// Creates a gRPC request with the supplied timeout.
pub fn with_timeout<T>(message: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_is_sent_as_grpc_metadata() {
        let request = with_timeout((), Duration::from_millis(1_500));

        assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "1500000u");
    }
}

/// Creates a request with evidence of whether quota admission reached actual dispatch.
pub fn with_dispatch_marker<T>(
    message: T,
    timeout: Duration,
    marker: super::limited_channel::TbankDispatchMarker,
) -> Request<T> {
    let mut request = with_timeout(message, timeout);
    request.extensions_mut().insert(marker);
    request
}
