//! Typed execution events emitted through the Nautilus custom-data pipeline.
//!
//! Commission provenance is the reason this module exists. The T-Invest order-state stream
//! (`OrderStateStreamResponse.OrderState`) and the trades stream (`OrderTrade`) carry no
//! commission field at all, so for those fills the venue value is unknown. Nautilus
//! `FillReport.commission` is not optional, so an unknown commission must still cross the adapter
//! boundary as a zero amount. That zero is a placeholder for an unknown value, never a
//! measurement, and this module keeps the distinction visible to consumers.

use std::{any::Any, ops::Deref, sync::Arc};

use nautilus_common::messages::DataEvent;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{CustomData, CustomDataTrait, Data, DataType, HasTsInit},
    reports::FillReport,
    types::{Currency, Money},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) type TbankDataEventSender = tokio::sync::mpsc::UnboundedSender<DataEvent>;

/// The adapter path that produced a fill report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankFillCommissionSource {
    /// Operation history from `GetOperationsByCursor`.
    OperationsCursor,
    /// Order trades delivered by the `TradesStream` subscription.
    TradesStream,
    /// Embedded order-state trades delivered by the `OrderStateStream` subscription.
    OrderStateStream,
    /// A single-order `GetOrderState` query used for reconciliation.
    OrderStateQuery,
    /// The synchronous `PostOrder` submit response.
    SubmitResponse,
}

impl TbankFillCommissionSource {
    /// Returns the stable wire name of this source.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OperationsCursor => "operations_cursor",
            Self::TradesStream => "trades_stream",
            Self::OrderStateStream => "order_state_stream",
            Self::OrderStateQuery => "order_state_query",
            Self::SubmitResponse => "submit_response",
        }
    }
}

/// Provenance of the commission value associated with a reported fill.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankFillCommissionStatus {
    /// The venue message carried a commission value for this fill.
    Reported,
    /// The venue supplied an aggregate commission which the adapter attributed to this fill.
    Allocated,
    /// The venue message carries no commission field, so the value is unknown rather than zero.
    Unknown,
}

/// A fill commission as far as the adapter could establish it from the venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TbankFillCommission {
    /// The venue supplied a commission value.
    Reported(Money),
    /// The venue supplied an aggregate commission which the adapter attributed to this fill.
    Allocated(Money),
    /// The venue message carries no commission field for this fill.
    Unknown,
}

impl TbankFillCommission {
    /// Returns whether the venue supplied a commission value.
    #[must_use]
    pub fn status(&self) -> TbankFillCommissionStatus {
        match self {
            Self::Reported(_) => TbankFillCommissionStatus::Reported,
            Self::Allocated(_) => TbankFillCommissionStatus::Allocated,
            Self::Unknown => TbankFillCommissionStatus::Unknown,
        }
    }

    /// Returns the per-fill commission only when the venue supplied it for this fill.
    #[must_use]
    pub fn reported(&self) -> Option<Money> {
        match self {
            Self::Reported(money) => Some(*money),
            Self::Allocated(_) | Self::Unknown => None,
        }
    }

    /// Returns the amount carried by this commission provenance, when known.
    #[must_use]
    pub fn amount(&self) -> Option<Money> {
        match self {
            Self::Reported(money) | Self::Allocated(money) => Some(*money),
            Self::Unknown => None,
        }
    }
}

impl From<Option<Money>> for TbankFillCommission {
    fn from(value: Option<Money>) -> Self {
        value.map_or(Self::Unknown, Self::Reported)
    }
}

/// Identity of the fill a commission belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TbankFillIdentity<'a> {
    /// Nautilus client order ID, when the adapter could resolve it for this fill.
    pub(crate) client_order_id: Option<&'a str>,
    /// Broker order ID the fill belongs to.
    pub(crate) venue_order_id: &'a str,
    /// Broker trade ID the fill was matched by.
    pub(crate) trade_id: &'a str,
    /// Canonical Nautilus instrument ID of the filled instrument.
    pub(crate) instrument_id: &'a str,
    /// Adapter path that produced the fill.
    pub(crate) source: TbankFillCommissionSource,
    /// UNIX timestamp (nanoseconds) when the fill occurred.
    pub(crate) ts_event: UnixNanos,
}

/// A fill report with the commission provenance which must remain attached until projection accepts
/// the fill. `FillReport` itself cannot carry the distinction between an unknown commission and a
/// measured zero, so keeping these values together avoids a side table keyed by unstable aliases.
#[derive(Clone, Debug)]
pub(crate) struct TbankFillReport {
    pub(crate) report: FillReport,
    pub(crate) commission: TbankFillCommission,
    pub(crate) source: TbankFillCommissionSource,
    /// `true` when this report only corrects commission provenance for an already emitted fill.
    /// Such a report must publish the custom event but must not enter Nautilus' fill stream again.
    pub(crate) provenance_only: bool,
}

impl Deref for TbankFillReport {
    type Target = FillReport;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

impl TbankFillReport {
    /// Creates a fill report and retains the venue commission status for deferred publication.
    pub(crate) fn new(
        report: FillReport,
        commission: TbankFillCommission,
        source: TbankFillCommissionSource,
    ) -> Self {
        if commission.status() == TbankFillCommissionStatus::Unknown {
            tracing::warn!(
                venue_order_id = %report.venue_order_id,
                trade_id = %report.trade_id,
                instrument_id = %report.instrument_id,
                source = source.as_str(),
                "T-Bank fill commission is unknown; Nautilus FillReport requires an amount, so a zero placeholder is reported"
            );
        }
        Self {
            report,
            commission,
            source,
            provenance_only: false,
        }
    }

    /// Creates a provenance-only correction for a fill already accepted by Nautilus.
    pub(crate) fn commission_correction(
        report: FillReport,
        commission: TbankFillCommission,
        source: TbankFillCommissionSource,
    ) -> Self {
        Self {
            report,
            commission,
            source,
            provenance_only: true,
        }
    }

    /// Returns the commission status and amount that the provenance event will carry.
    #[must_use]
    pub(crate) fn provenance_commission(&self) -> TbankFillCommission {
        match self.commission {
            TbankFillCommission::Reported(_) => {
                TbankFillCommission::Reported(self.report.commission)
            }
            TbankFillCommission::Allocated(_) => {
                TbankFillCommission::Allocated(self.report.commission)
            }
            TbankFillCommission::Unknown => TbankFillCommission::Unknown,
        }
    }

    /// Publishes provenance after the supplied report has passed canonicalization and
    /// deduplication. Provenance is supplementary to Nautilus' standard FillReport path: a
    /// missing or closed custom-data channel must not suppress an executed fill. The sender is
    /// passed explicitly because Nautilus stores it in thread-local state which is not available
    /// on runtime worker threads.
    pub(crate) fn publish_provenance_best_effort(&self, sender: Option<&TbankDataEventSender>) {
        let client_order_id = self.report.client_order_id.map(|value| value.to_string());
        let venue_order_id = self.report.venue_order_id.to_string();
        let trade_id = self.report.trade_id.to_string();
        let instrument_id = self.report.instrument_id.to_string();
        let commission = self.provenance_commission();
        let event = TbankExecutionEvent::fill_commission(
            commission,
            TbankFillIdentity {
                client_order_id: client_order_id.as_deref(),
                venue_order_id: venue_order_id.as_str(),
                trade_id: trade_id.as_str(),
                instrument_id: instrument_id.as_str(),
                source: self.source,
                ts_event: self.report.ts_event,
            },
        );
        if let Err(error) = event.publish(sender) {
            tracing::warn!(
                %error,
                venue_order_id = %self.report.venue_order_id,
                trade_id = %self.report.trade_id,
                instrument_id = %self.report.instrument_id,
                source = self.source.as_str(),
                "failed to publish T-Bank fill commission provenance"
            );
        }
    }
}

/// A typed execution event emitted through the Nautilus custom-data pipeline.
///
/// Consumers receive stable identities and typed status values; they never need to parse adapter
/// tracing or infer commission provenance from a numeric amount.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TbankExecutionEvent {
    /// Commission provenance for one fill the adapter reported to Nautilus.
    FillCommission {
        /// Nautilus client order ID, when the adapter could resolve it for this fill.
        client_order_id: Option<String>,
        /// Broker order ID the fill belongs to.
        venue_order_id: String,
        /// Broker trade ID the fill was matched by.
        trade_id: String,
        /// Canonical Nautilus instrument ID of the filled instrument.
        instrument_id: String,
        /// Whether the commission was reported for this fill, allocated from an operation total,
        /// or unknown.
        status: TbankFillCommissionStatus,
        /// Known commission as an exact decimal string, absent only when `status` is `unknown`.
        amount: Option<String>,
        /// Commission currency, absent only when `status` is `unknown`.
        currency: Option<String>,
        /// Adapter path that produced the fill.
        source: TbankFillCommissionSource,
        /// UNIX timestamp (nanoseconds) when the fill occurred.
        ts_event: UnixNanos,
        /// UNIX timestamp (nanoseconds) when the event instance was initialized.
        ts_init: UnixNanos,
    },
}

impl TbankExecutionEvent {
    const TYPE_NAME: &'static str = "TbankExecutionEvent";

    /// Creates a fill-commission event for the supplied fill identity.
    #[must_use]
    pub(crate) fn fill_commission(
        commission: TbankFillCommission,
        identity: TbankFillIdentity<'_>,
    ) -> Self {
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        let amount = commission.amount();
        Self::FillCommission {
            client_order_id: identity.client_order_id.map(str::to_string),
            venue_order_id: identity.venue_order_id.to_string(),
            trade_id: identity.trade_id.to_string(),
            instrument_id: identity.instrument_id.to_string(),
            status: commission.status(),
            amount: amount.map(|money| money.as_decimal().to_string()),
            currency: amount.map(|money| money.currency.code.as_str().to_string()),
            source: identity.source,
            ts_event: identity.ts_event,
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
            Self::FillCommission { ts_event, .. } => *ts_event,
        }
    }

    fn publish(self, sender: Option<&TbankDataEventSender>) -> anyhow::Result<()> {
        let sender =
            sender.ok_or_else(|| anyhow::anyhow!("T-Bank execution event sender is not bound"))?;
        sender
            .send(self.into_data_event())
            .map_err(|error| anyhow::anyhow!("failed to publish T-Bank execution event: {error}"))
    }
}

impl HasTsInit for TbankExecutionEvent {
    fn ts_init(&self) -> UnixNanos {
        match self {
            Self::FillCommission { ts_init, .. } => *ts_init,
        }
    }
}

impl CustomDataTrait for TbankExecutionEvent {
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

/// Registers T-Bank execution custom data types for Nautilus JSON deserialization.
///
/// Idempotent and process-local, matching the market-data custom-data registration contract.
pub fn register_tbank_execution_custom_data() {
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<TbankExecutionEvent>();
}

/// Flattens the commission provenance into the commission value Nautilus `FillReport` requires.
///
/// Nautilus `FillReport.commission` is not optional, so an unknown venue commission must be
/// flattened to a zero amount. The zero is a placeholder for an unknown value and must not be read
/// as a measured zero; [`TbankExecutionEvent::FillCommission`] carries the distinction to
/// consumers. The placeholder is a hardcoded zero RUB, matching the value the adapter reported
/// before commission provenance was tracked; it is not derived from the instrument or the account,
/// so consumers must not infer a currency from it.
#[must_use]
pub(crate) fn fill_report_commission(commission: TbankFillCommission) -> Money {
    commission
        .amount()
        .unwrap_or_else(|| unknown_commission_placeholder())
}

fn unknown_commission_placeholder() -> Money {
    Money::from_decimal(Decimal::ZERO, Currency::from("RUB"))
        .expect("zero RUB is a representable money amount")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_commission_event_uses_the_nautilus_custom_data_contract() {
        register_tbank_execution_custom_data();
        let event = TbankExecutionEvent::fill_commission(
            TbankFillCommission::Reported(Money::from("35.96 RUB")),
            TbankFillIdentity {
                client_order_id: Some("client-order"),
                venue_order_id: "venue-order",
                trade_id: "trade",
                instrument_id: "CHMF_TQBR.MOEX",
                source: TbankFillCommissionSource::OperationsCursor,
                ts_event: UnixNanos::from(7),
            },
        );
        let json = event.to_json().unwrap();
        let decoded = TbankExecutionEvent::from_json(serde_json::from_str(&json).unwrap()).unwrap();
        assert_eq!(
            decoded
                .as_any()
                .downcast_ref::<TbankExecutionEvent>()
                .unwrap(),
            &event
        );

        let DataEvent::Data(Data::Custom(custom)) = event.clone().into_data_event() else {
            panic!("execution event must use DataEvent::Data(Data::Custom)");
        };
        assert_eq!(custom.data_type, TbankExecutionEvent::data_type());

        let json = serde_json::to_vec(&Data::Custom(custom)).unwrap();
        let decoded = CustomData::from_json_bytes(&json).unwrap();
        assert_eq!(decoded.data_type, TbankExecutionEvent::data_type());
        assert_eq!(
            decoded
                .data
                .as_any()
                .downcast_ref::<TbankExecutionEvent>()
                .unwrap(),
            &event
        );
    }

    #[test]
    fn reported_commission_keeps_the_venue_amount_and_currency() {
        let event = TbankExecutionEvent::fill_commission(
            TbankFillCommission::Reported(Money::from("12.5 RUB")),
            identity(TbankFillCommissionSource::SubmitResponse),
        );
        let TbankExecutionEvent::FillCommission {
            status,
            amount,
            currency,
            ..
        } = event;
        assert_eq!(status, TbankFillCommissionStatus::Reported);
        // The amount is the exact decimal carried by `Money`, scale included: the adapter does not
        // re-quantize a venue value, so `12.5 RUB` is reported as `12.50`.
        assert_eq!(amount.as_deref(), Some("12.50"));
        assert_eq!(currency.as_deref(), Some("RUB"));
    }

    #[test]
    fn allocated_commission_marks_operation_level_distribution() {
        let event = TbankExecutionEvent::fill_commission(
            TbankFillCommission::Allocated(Money::from("7.50 RUB")),
            identity(TbankFillCommissionSource::OperationsCursor),
        );
        let TbankExecutionEvent::FillCommission {
            status,
            amount,
            currency,
            ..
        } = event;
        assert_eq!(status, TbankFillCommissionStatus::Allocated);
        assert_eq!(amount.as_deref(), Some("7.50"));
        assert_eq!(currency.as_deref(), Some("RUB"));
    }

    #[test]
    fn unknown_commission_reports_no_amount() {
        let event = TbankExecutionEvent::fill_commission(
            TbankFillCommission::Unknown,
            identity(TbankFillCommissionSource::OrderStateStream),
        );
        let TbankExecutionEvent::FillCommission {
            status,
            amount,
            currency,
            source,
            ..
        } = event;
        assert_eq!(status, TbankFillCommissionStatus::Unknown);
        assert_eq!(amount, None);
        assert_eq!(currency, None);
        assert_eq!(source, TbankFillCommissionSource::OrderStateStream);
    }

    #[test]
    fn unknown_commission_flattens_to_a_zero_placeholder() {
        let money = fill_report_commission(TbankFillCommission::Unknown);
        assert_eq!(money.as_decimal(), Decimal::ZERO);
        assert_eq!(money.currency.code.as_str(), "RUB");
    }

    #[test]
    fn reported_commission_flattens_to_the_venue_amount() {
        let money = fill_report_commission(TbankFillCommission::Reported(Money::from("4.20 RUB")));
        assert_eq!(money, Money::from("4.20 RUB"));
    }

    #[test]
    fn absent_venue_value_maps_to_unknown() {
        assert_eq!(
            TbankFillCommission::from(None),
            TbankFillCommission::Unknown
        );
        assert_eq!(
            TbankFillCommission::from(Some(Money::from("1.00 RUB"))),
            TbankFillCommission::Reported(Money::from("1.00 RUB"))
        );
    }

    #[test]
    fn event_identity_is_carried_verbatim() {
        let event = TbankExecutionEvent::fill_commission(
            TbankFillCommission::Unknown,
            TbankFillIdentity {
                client_order_id: None,
                venue_order_id: "84203543334",
                trade_id: "98153ced-8ac8-5e6f-baab-1b71dc24ac48",
                instrument_id: "CHMF_TQBR.MOEX",
                source: TbankFillCommissionSource::OrderStateQuery,
                ts_event: UnixNanos::from(11),
            },
        );
        let TbankExecutionEvent::FillCommission {
            client_order_id,
            venue_order_id,
            instrument_id,
            ts_event,
            ..
        } = event;
        assert_eq!(client_order_id, None);
        assert_eq!(venue_order_id, "84203543334");
        assert_eq!(instrument_id, "CHMF_TQBR.MOEX");
        assert_eq!(ts_event, UnixNanos::from(11));
    }

    fn identity(source: TbankFillCommissionSource) -> TbankFillIdentity<'static> {
        TbankFillIdentity {
            client_order_id: Some("client-order"),
            venue_order_id: "venue-order",
            trade_id: "trade",
            instrument_id: "CHMF_TQBR.MOEX",
            source,
            ts_event: UnixNanos::from(1),
        }
    }
}
