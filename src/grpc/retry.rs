use std::time::Duration;

use rand::RngExt;

use crate::config::TbankReconnectPolicy;

const RECONNECT_JITTER_MS: u64 = 100;

/// Calculates bounded exponential reconnect backoff for an attempt.
///
/// When jitter is enabled, this follows Nautilus' transport policy: a random
/// additive delay of up to 100 ms is applied without exceeding the configured cap.
pub fn backoff_duration(policy: &TbankReconnectPolicy, attempt: u32) -> Duration {
    let multiplier = 2_u64.saturating_pow(attempt.min(31));
    let base_ms = policy
        .initial_backoff_ms
        .saturating_mul(multiplier)
        .min(policy.max_backoff_ms);
    if !policy.jitter {
        return Duration::from_millis(base_ms);
    }
    let jitter_ms = policy.max_backoff_ms.min(RECONNECT_JITTER_MS);
    let jitter_ms = rand::rng().random_range(0..=jitter_ms);

    bounded_backoff_duration(policy, base_ms, jitter_ms)
}

fn bounded_backoff_duration(
    policy: &TbankReconnectPolicy,
    base_ms: u64,
    jitter_ms: u64,
) -> Duration {
    let jitter_ceiling = policy.max_backoff_ms.min(RECONNECT_JITTER_MS);
    let jitter_ms = jitter_ms.min(jitter_ceiling);
    let jittered_base_ms = base_ms.min(policy.max_backoff_ms.saturating_sub(jitter_ceiling));
    let floor_ms = policy.initial_backoff_ms.min(policy.max_backoff_ms);
    let millis = jittered_base_ms
        .saturating_add(jitter_ms)
        .clamp(floor_ms, policy.max_backoff_ms);

    Duration::from_millis(millis)
}

/// Returns whether a gRPC status is transient and retryable.
pub fn is_transient_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Internal
            | tonic::Code::Unknown
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(jitter: bool) -> TbankReconnectPolicy {
        TbankReconnectPolicy {
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            jitter,
        }
    }

    #[test]
    fn disabled_jitter_preserves_deterministic_exponential_backoff() {
        let policy = policy(false);

        assert_eq!(backoff_duration(&policy, 0), Duration::from_millis(500));
        assert_eq!(backoff_duration(&policy, 1), Duration::from_millis(1_000));
        assert_eq!(backoff_duration(&policy, 10), Duration::from_millis(30_000));
    }

    #[test]
    fn bounded_jitter_preserves_the_configured_floor_and_cap() {
        let policy = policy(true);

        assert_eq!(
            bounded_backoff_duration(&policy, 500, 0),
            Duration::from_millis(500)
        );
        assert_eq!(
            bounded_backoff_duration(&policy, 500, RECONNECT_JITTER_MS),
            Duration::from_millis(600)
        );
        assert_eq!(
            bounded_backoff_duration(&policy, 30_000, 0),
            Duration::from_millis(29_900)
        );
        assert_eq!(
            bounded_backoff_duration(&policy, 30_000, RECONNECT_JITTER_MS),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn enabled_jitter_stays_bounded_for_initial_and_capped_attempts() {
        let policy = policy(true);

        for _ in 0..10 {
            assert!(
                (Duration::from_millis(500)..=Duration::from_millis(600))
                    .contains(&backoff_duration(&policy, 0))
            );
            assert!(
                (Duration::from_millis(29_900)..=Duration::from_millis(30_000))
                    .contains(&backoff_duration(&policy, 10))
            );
        }
    }

    #[test]
    fn bounded_jitter_handles_a_cap_smaller_than_the_jitter_window() {
        let policy = TbankReconnectPolicy {
            initial_backoff_ms: 5,
            max_backoff_ms: 20,
            jitter: true,
        };

        assert_eq!(
            bounded_backoff_duration(&policy, 20, 0),
            Duration::from_millis(5)
        );
        assert_eq!(
            bounded_backoff_duration(&policy, 20, RECONNECT_JITTER_MS),
            Duration::from_millis(20)
        );
    }
}
