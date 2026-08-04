#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarContinuityDecision {
    Accepted,
    Duplicate,
}

/// Tracks only the latest authoritative candle timestamp.
///
/// T-Bank streams are sparse: the absence of a one-minute candle does not imply
/// lost data. Missing data is therefore recovered only at lifecycle boundaries
/// (startup and reconnect) with GetCandles, never inferred from wall-clock gaps
/// between live candles.
#[derive(Debug, Default, Clone)]
pub struct BarContinuityTracker {
    latest_seen: Option<i128>,
}

impl BarContinuityTracker {
    pub const fn from_seeded_bar(ts_event_nanos: i128) -> Self {
        Self {
            latest_seen: Some(ts_event_nanos),
        }
    }

    pub fn observe_live_bar(&mut self, ts_event_nanos: i128) -> BarContinuityDecision {
        if self
            .latest_seen
            .is_some_and(|latest| ts_event_nanos <= latest)
        {
            return BarContinuityDecision::Duplicate;
        }
        self.latest_seen = Some(ts_event_nanos);
        BarContinuityDecision::Accepted
    }

    pub const fn latest_seen(&self) -> Option<i128> {
        self.latest_seen
    }

    pub fn record_backfilled_bar(&mut self, ts_event_nanos: i128) {
        if self
            .latest_seen
            .is_none_or(|latest| ts_event_nanos > latest)
        {
            self.latest_seen = Some(ts_event_nanos);
        }
    }

    /// Advances the authoritative recovery cursor after a successful
    /// GetCandles request, including intervals where the broker returned no
    /// candles because no trades occurred.
    pub fn record_recovered_through(&mut self, ts_event_nanos: i128) {
        if self
            .latest_seen
            .is_none_or(|latest| ts_event_nanos > latest)
        {
            self.latest_seen = Some(ts_event_nanos);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: i128 = 60_000_000_000;

    #[test]
    fn sparse_live_bars_are_accepted_without_inferring_missing_minutes() {
        let mut tracker = BarContinuityTracker::default();

        assert_eq!(tracker.observe_live_bar(0), BarContinuityDecision::Accepted);
        assert_eq!(
            tracker.observe_live_bar(3 * MINUTE),
            BarContinuityDecision::Accepted
        );
        assert_eq!(tracker.latest_seen(), Some(3 * MINUTE));
    }

    #[test]
    fn duplicate_and_out_of_order_bars_are_ignored() {
        let mut tracker = BarContinuityTracker::from_seeded_bar(2 * MINUTE);

        assert_eq!(
            tracker.observe_live_bar(2 * MINUTE),
            BarContinuityDecision::Duplicate
        );
        assert_eq!(
            tracker.observe_live_bar(MINUTE),
            BarContinuityDecision::Duplicate
        );
        assert_eq!(tracker.latest_seen(), Some(2 * MINUTE));
    }

    #[test]
    fn successful_empty_recovery_advances_cursor() {
        let mut tracker = BarContinuityTracker::from_seeded_bar(MINUTE);
        tracker.record_recovered_through(10 * MINUTE);

        assert_eq!(tracker.latest_seen(), Some(10 * MINUTE));
        assert_eq!(
            tracker.observe_live_bar(10 * MINUTE),
            BarContinuityDecision::Duplicate
        );
        assert_eq!(
            tracker.observe_live_bar(11 * MINUTE),
            BarContinuityDecision::Accepted
        );
    }
}
