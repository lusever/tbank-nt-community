use std::sync::OnceLock;

use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

const STREAM_EVENT_CAPACITY: usize = 2_048;
const READINESS_EVENT_CAPACITY: usize = 8_192;

/// Operational lifecycle state of one T-Bank market-data stream group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankMarketDataStreamState {
    /// The stream controller has started and is opening the subscription.
    Connecting,
    /// The broker acknowledged the subscription and the stream is usable.
    Connected,
    /// The controller is recovering from a retryable stream failure.
    Reconnecting,
    /// The controller exhausted retries or encountered a permanent rejection.
    Dead,
}

impl TbankMarketDataStreamState {
    pub(crate) fn from_stage(stage: &str) -> Option<Self> {
        match stage {
            "stream_supervisor_started" => Some(Self::Connecting),
            "stream_ready" => Some(Self::Connected),
            "stream_task_panicked"
            | "stream_closed_by_server"
            | "stream_closed_error"
            | "stream_idle_timeout"
            | "open_failed"
            | "pre_ack_buffer_overflow"
            | "reconnect_scheduled"
            | "stream_supervisor_reconnect_failed"
            | "subscription_rejection_partitioned" => Some(Self::Reconnecting),
            "stream_supervisor_exhausted"
            | "stream_reconnect_exhausted"
            | "subscription_permanently_rejected" => Some(Self::Dead),
            _ => None,
        }
    }
}

/// A typed market-data transport transition emitted by the T-Bank adapter.
///
/// Consumers can project these events into operational health without parsing tracing text. The
/// tracing record remains the durable operator-facing representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TbankMarketDataStreamEvent {
    /// Typed lifecycle state for this transition.
    pub state: TbankMarketDataStreamState,
    /// Stable lifecycle stage identifier.
    pub stage: String,
    /// Stable adapter task key for the stream group.
    pub task_key: String,
    /// Human-readable transition reason.
    pub reason: String,
}

/// Candle-source readiness established by lifecycle recovery or an explicit poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankCandleReadinessState {
    /// Previously established readiness is no longer authoritative.
    Recovering,
    /// The broker was checked successfully through the supplied closed minute.
    Ready,
    /// The broker check failed and entries requiring this source must remain blocked.
    Failed,
}

/// Per-instrument candle readiness event emitted independently of bar arrival.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TbankCandleReadinessEvent {
    /// Readiness transition.
    pub state: TbankCandleReadinessState,
    /// Stable instrument-scoped continuity key.
    pub task_key: String,
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Latest closed minute authoritatively checked with the broker.
    pub ready_through: Option<UnixNanos>,
    /// Human-readable transition reason.
    pub reason: String,
}

static STREAM_EVENTS: OnceLock<broadcast::Sender<TbankMarketDataStreamEvent>> = OnceLock::new();
static READINESS_EVENTS: OnceLock<broadcast::Sender<TbankCandleReadinessEvent>> = OnceLock::new();

fn stream_event_sender() -> &'static broadcast::Sender<TbankMarketDataStreamEvent> {
    STREAM_EVENTS.get_or_init(|| {
        let (sender, _) = broadcast::channel(STREAM_EVENT_CAPACITY);
        sender
    })
}

/// Subscribes to typed T-Bank market-data stream lifecycle events for this process.
#[must_use]
pub fn subscribe_market_data_stream_events() -> broadcast::Receiver<TbankMarketDataStreamEvent> {
    stream_event_sender().subscribe()
}

pub(crate) fn publish_market_data_stream_event(event: TbankMarketDataStreamEvent) {
    let _ = stream_event_sender().send(event);
}

fn readiness_event_sender() -> &'static broadcast::Sender<TbankCandleReadinessEvent> {
    READINESS_EVENTS.get_or_init(|| {
        let (sender, _) = broadcast::channel(READINESS_EVENT_CAPACITY);
        sender
    })
}

/// Subscribes to per-instrument candle readiness events for this process.
#[must_use]
pub fn subscribe_candle_readiness_events() -> broadcast::Receiver<TbankCandleReadinessEvent> {
    readiness_event_sender().subscribe()
}

pub(crate) fn publish_candle_readiness_event(event: TbankCandleReadinessEvent) {
    let _ = readiness_event_sender().send(event);
}
