use std::{any::Any, sync::Arc};

use nautilus_common::messages::DataEvent;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::data::{CustomData, CustomDataTrait, Data, DataType, HasTsInit};
use serde::{Deserialize, Serialize};

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

/// A typed market-data lifecycle event emitted through the Nautilus custom-data pipeline.
///
/// Generation ownership, task cancellation, and stale-worker fencing stay inside the adapter.
/// Consumers receive stable logical IDs and never need to parse adapter task keys or lifecycle
/// stage strings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
        /// UNIX timestamp (nanoseconds) when the transition occurred.
        ts_event: UnixNanos,
        /// UNIX timestamp (nanoseconds) when the event instance was initialized.
        ts_init: UnixNanos,
    },
    /// The logical stream is no longer part of the desired subscription snapshot.
    StreamRetired {
        /// Stable logical stream identity, independent of reconnect generation.
        stream_id: String,
        /// Stable readiness identities retired together with this stream.
        readiness_ids: Vec<String>,
        /// Human-readable explanation for the retirement.
        reason: String,
        /// UNIX timestamp (nanoseconds) when the transition occurred.
        ts_event: UnixNanos,
        /// UNIX timestamp (nanoseconds) when the event instance was initialized.
        ts_init: UnixNanos,
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
        /// UNIX timestamp (nanoseconds) when the transition occurred.
        ts_event: UnixNanos,
        /// UNIX timestamp (nanoseconds) when the event instance was initialized.
        ts_init: UnixNanos,
    },
}

impl TbankMarketDataEvent {
    const TYPE_NAME: &'static str = "TbankMarketDataEvent";

    fn timestamps() -> (UnixNanos, UnixNanos) {
        let now = get_atomic_clock_realtime().get_time_ns();
        (now, now)
    }

    /// Creates a stream-state transition with Nautilus event timestamps.
    #[must_use]
    pub fn stream_state(
        stream_id: String,
        state: TbankMarketDataStreamState,
        readiness_ids: Vec<String>,
        reason: String,
    ) -> Self {
        let (ts_event, ts_init) = Self::timestamps();
        Self::StreamState {
            stream_id,
            state,
            readiness_ids,
            reason,
            ts_event,
            ts_init,
        }
    }

    /// Creates a stream-retirement transition with Nautilus event timestamps.
    #[must_use]
    pub fn stream_retired(stream_id: String, readiness_ids: Vec<String>, reason: String) -> Self {
        let (ts_event, ts_init) = Self::timestamps();
        Self::StreamRetired {
            stream_id,
            readiness_ids,
            reason,
            ts_event,
            ts_init,
        }
    }

    /// Creates a candle-readiness transition with Nautilus event timestamps.
    #[must_use]
    pub fn candle_readiness(
        readiness_id: String,
        instrument_uid: String,
        state: TbankCandleReadinessState,
        ready_through: Option<UnixNanos>,
        reason: String,
    ) -> Self {
        let (ts_event, ts_init) = Self::timestamps();
        Self::CandleReadiness {
            readiness_id,
            instrument_uid,
            state,
            ready_through,
            reason,
            ts_event,
            ts_init,
        }
    }

    /// Returns the Nautilus data type used for MessageBus routing.
    #[must_use]
    pub fn data_type() -> DataType {
        DataType::new(Self::TYPE_NAME, None, None)
    }

    pub(crate) fn into_data_event(self) -> DataEvent {
        DataEvent::Data(Data::Custom(CustomData::from_arc(Arc::new(self))))
    }

    fn event_timestamp(&self) -> UnixNanos {
        match self {
            Self::StreamState { ts_event, .. }
            | Self::StreamRetired { ts_event, .. }
            | Self::CandleReadiness { ts_event, .. } => *ts_event,
        }
    }
}

impl HasTsInit for TbankMarketDataEvent {
    fn ts_init(&self) -> UnixNanos {
        match self {
            Self::StreamState { ts_init, .. }
            | Self::StreamRetired { ts_init, .. }
            | Self::CandleReadiness { ts_init, .. } => *ts_init,
        }
    }
}

impl CustomDataTrait for TbankMarketDataEvent {
    fn type_name(&self) -> &'static str {
        Self::TYPE_NAME
    }

    fn type_name_static() -> &'static str {
        Self::TYPE_NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.event_timestamp()
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        Ok(Arc::new(serde_json::from_value::<Self>(value)?))
    }
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

/// Registers T-Bank custom data types for Nautilus JSON deserialization.
///
/// Call this during process initialization before replaying persisted [`CustomData`]. The
/// registration is idempotent and remains process-local, matching Nautilus custom-data contracts.
pub fn register_tbank_custom_data() {
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<TbankMarketDataEvent>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_event_uses_the_nautilus_custom_data_contract() {
        register_tbank_custom_data();
        let event = TbankMarketDataEvent::stream_state(
            "bars:group:0:1m".to_string(),
            TbankMarketDataStreamState::Connected,
            vec!["bars:group:0:1m:instrument:uid".to_string()],
            "ready".to_string(),
        );
        let json = event.to_json().unwrap();
        let decoded =
            TbankMarketDataEvent::from_json(serde_json::from_str(&json).unwrap()).unwrap();
        assert_eq!(
            decoded
                .as_any()
                .downcast_ref::<TbankMarketDataEvent>()
                .unwrap(),
            &event
        );

        let DataEvent::Data(Data::Custom(custom)) = event.clone().into_data_event() else {
            panic!("lifecycle event must use DataEvent::Data(Data::Custom)");
        };
        assert_eq!(custom.data_type, TbankMarketDataEvent::data_type());

        let json = serde_json::to_vec(&Data::Custom(custom)).unwrap();
        let decoded = CustomData::from_json_bytes(&json).unwrap();
        assert_eq!(decoded.data_type, TbankMarketDataEvent::data_type());
        assert_eq!(
            decoded
                .data
                .as_any()
                .downcast_ref::<TbankMarketDataEvent>()
                .unwrap(),
            &event
        );
    }
}
