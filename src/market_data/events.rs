use std::sync::OnceLock;

use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

const MARKET_DATA_EVENT_CAPACITY: usize = 10_240;

/// Operational lifecycle state of one T-Bank market-data stream group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankMarketDataStreamState {
    /// The stream controller has started and is opening the subscription.
    Connecting,
    /// The broker acknowledged the subscription and required continuity recovery completed.
    Connected,
    /// The controller is recovering from a stream interruption or validating continuity after subscription.
    Reconnecting,
    /// The controller reached a terminal failure and will not recover.
    Dead,
}

impl TbankMarketDataStreamState {
    pub(crate) fn from_stage(stage: &str) -> Option<Self> {
        match stage {
            "stream_supervisor_started" => Some(Self::Connecting),
            "stream_subscription_acked" => Some(Self::Reconnecting),
            "stream_ready" => Some(Self::Connected),
            "stream_task_panicked"
            | "stream_closed_by_server"
            | "stream_closed_error"
            | "stream_idle_timeout"
            | "open_failed"
            | "stream_recovery_failed"
            | "stream_worker_normal_exit"
            | "pre_ack_buffer_overflow"
            | "reconnect_scheduled"
            | "stream_supervisor_reconnect_failed"
            | "subscription_rejection_partitioned"
            | "stream_subscriptions_stopped"
            | "stream_reconnect_exhausted"
            | "stream_supervisor_reconnect_exhausted" => Some(Self::Reconnecting),
            "stream_supervisor_exhausted"
            | "subscription_permanently_rejected"
            | "stream_subscriptions_disabled" => Some(Self::Dead),
            _ => None,
        }
    }
}

/// A typed market-data lifecycle event emitted by the T-Bank adapter.
///
/// Generation ownership, task cancellation, and stale-worker fencing stay inside the adapter.
/// Consumers receive stable logical IDs and never need to parse adapter task keys or lifecycle
/// stage strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TbankMarketDataEvent {
    /// The current logical stream changed state.
    StreamState {
        /// Stable logical stream identity, independent of reconnect generation.
        stream_id: String,
        /// Current operational state of the logical stream.
        state: TbankMarketDataStreamState,
        /// Stable readiness identities currently owned by this stream.
        readiness_ids: Vec<String>,
        /// Human-readable explanation for the state transition.
        reason: String,
    },
    /// The logical stream is no longer part of the desired subscription snapshot.
    StreamRetired {
        /// Stable logical stream identity, independent of reconnect generation.
        stream_id: String,
        /// Stable readiness identities retired together with this stream.
        readiness_ids: Vec<String>,
        /// Human-readable explanation for the retirement.
        reason: String,
    },
    /// The current logical candle source changed readiness state.
    CandleReadiness {
        /// Stable logical readiness identity, independent of reconnect generation.
        readiness_id: String,
        /// Broker instrument UID associated with the readiness source.
        instrument_uid: String,
        /// Current readiness state of the candle source.
        state: TbankCandleReadinessState,
        /// Latest closed-minute boundary authoritatively accepted for this source, when ready.
        ready_through: Option<UnixNanos>,
        /// Human-readable explanation for the readiness transition.
        reason: String,
    },
}

/// Candle-source readiness established by lifecycle recovery, an acknowledged initial live
/// candle, or an explicit poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankCandleReadinessState {
    /// Previously established readiness is no longer authoritative while the current source recovers.
    Recovering,
    /// The broker was checked successfully through the supplied closed minute.
    Ready,
    /// The broker check failed and entries requiring this source must remain blocked.
    Failed,
}

static MARKET_DATA_EVENTS: OnceLock<broadcast::Sender<TbankMarketDataEvent>> = OnceLock::new();

fn market_data_event_sender() -> &'static broadcast::Sender<TbankMarketDataEvent> {
    MARKET_DATA_EVENTS.get_or_init(|| broadcast::channel(MARKET_DATA_EVENT_CAPACITY).0)
}

/// Subscribes to the ordered typed T-Bank market-data lifecycle stream for this process.
#[must_use]
pub fn subscribe_market_data_events() -> broadcast::Receiver<TbankMarketDataEvent> {
    market_data_event_sender().subscribe()
}

pub(crate) fn publish_market_data_event(event: TbankMarketDataEvent) {
    let _ = market_data_event_sender().send(event);
}
