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
