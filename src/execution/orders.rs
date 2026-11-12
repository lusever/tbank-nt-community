use nautilus_model::enums::{TimeInForce, TrailingOffsetType, TriggerType};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    common::{
        TbankOrderSide, TbankOrderType,
        decimal::{price_to_quotation, quantity_units_to_lots},
        error::{Result, TbankAdapterError},
    },
    config::TbankEnvironment,
    grpc::generated::{OrderType, PostOrderRequest, PriceType, TimeInForceType},
    instruments::TbankInstrumentMetadata,
};

/// Per-command regular-order submission method. Never automatically substituted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankSubmitMethod {
    /// Synchronous broker response (the default).
    #[default]
    PostOrder,
    /// Asynchronous broker acknowledgement; exchange acceptance arrives separately.
    PostOrderAsync,
}

/// SubmitOrder.params key; prefer [`TbankSubmitMethod::apply`].
pub const TBANK_SUBMIT_METHOD_PARAM: &str = "tbank_submit_method";

impl TbankSubmitMethod {
    /// Sets this method without removing unrelated command parameters.
    pub fn apply(self, params: &mut nautilus_core::Params) {
        TbankSubmitOptions {
            method: Some(self),
            ..TbankSubmitOptions::default()
        }
        .apply(params);
    }

    /// Decodes the command parameter, rejecting malformed or unknown values.
    pub fn from_params(params: Option<&nautilus_core::Params>) -> anyhow::Result<Self> {
        match params.and_then(|p| p.get(TBANK_SUBMIT_METHOD_PARAM)) {
            None => Ok(Self::default()),
            Some(value) => Ok(serde_json::from_value(value.clone())?),
        }
    }
}

/// Typed SubmitOrder parameters; apply to an existing Params to preserve consumer metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TbankSubmitOptions {
    /// Explicit broker method. `None` leaves method selection to the order type.
    pub method: Option<TbankSubmitMethod>,
    /// Consumer's UTC command expiry. Converted once to a monotonic deadline on admission.
    pub deadline: Option<nautilus_core::UnixNanos>,
    /// Optional override of margin confirmation for this command.
    pub confirm_margin_trade: Option<bool>,
}
/// SubmitOrder.params command expiry key; prefer TbankSubmitOptions.
pub const TBANK_SUBMIT_DEADLINE_PARAM: &str = "tbank_submit_deadline_ns";
impl TbankSubmitOptions {
    /// Encodes typed options without removing unrelated Params.
    pub fn apply(self, params: &mut nautilus_core::Params) {
        if self == Self::default() {
            return;
        }
        let mut values = params.to_shared_map();
        if let Some(method) = self.method {
            values.insert(
                TBANK_SUBMIT_METHOD_PARAM.into(),
                serde_json::to_value(method)
                    .expect("enum serialization")
                    .into(),
            );
        }
        if let Some(deadline) = self.deadline {
            values.insert(
                TBANK_SUBMIT_DEADLINE_PARAM.into(),
                serde_json::json!(deadline.as_u64()).into(),
            );
        }
        if let Some(confirm) = self.confirm_margin_trade {
            values.insert(
                crate::execution::TBANK_CONFIRM_MARGIN_TRADE_PARAM.into(),
                serde_json::json!(confirm).into(),
            );
        }
        *params = nautilus_core::Params::from_shared(values);
    }
    /// Decodes options strictly. Unsupported and malformed values fail before RPC.
    pub fn from_params(params: Option<&nautilus_core::Params>) -> anyhow::Result<Self> {
        let value = |key: &str| params.and_then(|p| p.get(key));
        let deadline = value(TBANK_SUBMIT_DEADLINE_PARAM)
            .map(|v| {
                v.as_u64()
                    .map(nautilus_core::UnixNanos::from)
                    .ok_or_else(|| anyhow::anyhow!("submit deadline must be UTC nanoseconds"))
            })
            .transpose()?;
        let confirm_margin_trade = value(crate::execution::TBANK_CONFIRM_MARGIN_TRADE_PARAM)
            .map(|v| {
                v.as_bool()
                    .ok_or_else(|| anyhow::anyhow!("confirm_margin_trade must be a boolean"))
            })
            .transpose()?;
        Ok(Self {
            method: params
                .and_then(|p| p.get(TBANK_SUBMIT_METHOD_PARAM))
                .map(|_| TbankSubmitMethod::from_params(params))
                .transpose()?,
            deadline,
            confirm_margin_trade,
        })
    }
    pub(crate) fn monotonic_deadline(self) -> anyhow::Result<Option<tokio::time::Instant>> {
        self.deadline
            .map(|deadline| {
                let now = nautilus_core::time::get_atomic_clock_realtime()
                    .get_time_ns()
                    .as_u64();
                let remaining = deadline
                    .as_u64()
                    .checked_sub(now)
                    .filter(|r| *r > 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!("consumer submit deadline expired before RPC")
                    })?;
                tokio::time::Instant::now()
                    .checked_add(std::time::Duration::from_nanos(remaining))
                    .ok_or_else(|| anyhow::anyhow!("submit deadline out of range"))
            })
            .transpose()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// T-Bank service responsible for submitting an order.
pub enum TbankExecutionService {
    /// Live regular-orders service.
    LiveOrders,
    /// Live stop-orders service.
    LiveStopOrders,
    /// Sandbox service.
    Sandbox,
}

/// Broker-native trailing-stop parameters preserved across submit and stream reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TbankTrailingStopParams {
    /// Optional activation price.
    pub activation_price: Option<Decimal>,
    /// Trailing distance.
    pub trailing_offset: Decimal,
    /// Unit used for the trailing distance.
    pub trailing_offset_type: TrailingOffsetType,
    /// Optional limit offset for trailing stop-limit orders.
    pub limit_offset: Option<Decimal>,
    /// Optional trigger-price source.
    pub trigger_type: Option<TriggerType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized order submitted by the adapter to T-Bank.
pub struct TbankSubmitOrder {
    /// Explicit broker method for this logical order.
    pub submit_method: TbankSubmitMethod,
    /// Canonical Nautilus instrument ID.
    pub instrument_id: String,
    /// Nautilus client order ID.
    pub client_order_id: String,
    /// Canonical UUID used as the broker request ID.
    pub broker_request_id: String,
    /// Order side.
    pub side: TbankOrderSide,
    /// Adapter order type.
    pub order_type: TbankOrderType,
    /// Nautilus time-in-force.
    pub time_in_force: TimeInForce,
    /// Requested quantity in Nautilus units (shares or futures contracts).
    pub quantity_units: Decimal,
    /// Optional limit price.
    pub limit_price: Option<Decimal>,
    /// Optional stop trigger price.
    pub trigger_price: Option<Decimal>,
    /// Optional trailing-stop parameters.
    pub trailing: Option<TbankTrailingStopParams>,
    /// T-Bank margin-trade confirmation flag.
    pub confirm_margin_trade: bool,
}

pub(crate) fn validate_tbank_request_id(request_id: &str) -> Result<()> {
    if request_id.len() != 36 || Uuid::parse_str(request_id).is_err() {
        return Err(TbankAdapterError::ConfigError(format!(
            "T-Bank order request id must be a canonical 36-character UUID, got {request_id}"
        )));
    }
    Ok(())
}

fn tbank_time_in_force(
    order: &TbankSubmitOrder,
    instrument: &TbankInstrumentMetadata,
) -> Result<TimeInForceType> {
    if order.order_type == TbankOrderType::Market {
        if order.time_in_force != TimeInForce::Ioc {
            return Err(TbankAdapterError::UnsupportedTimeInForce(format!(
                "T-Bank market orders require IOC semantics, got {:?}",
                order.time_in_force
            )));
        }
        return Ok(TimeInForceType::TimeInForceUnspecified);
    }
    match order.time_in_force {
        TimeInForce::Day => Ok(TimeInForceType::TimeInForceDay),
        TimeInForce::Ioc => Ok(TimeInForceType::TimeInForceFillAndKill),
        TimeInForce::Fok
            if instrument.instrument_type == crate::common::TbankInstrumentType::Futures =>
        {
            Err(TbankAdapterError::UnsupportedTimeInForce(
                "T-Bank futures orders do not support FOK".to_string(),
            ))
        }
        TimeInForce::Fok => Ok(TimeInForceType::TimeInForceFillOrKill),
        unsupported => Err(TbankAdapterError::UnsupportedTimeInForce(format!(
            "T-Bank regular limit orders support DAY, IOC, or FOK, got {unsupported:?}"
        ))),
    }
}

impl TbankSubmitOrder {
    /// Returns the execution service used for the selected environment.
    pub fn service(&self, environment: TbankEnvironment) -> TbankExecutionService {
        match (environment, self.order_type) {
            (TbankEnvironment::Live, TbankOrderType::Market | TbankOrderType::Limit) => {
                TbankExecutionService::LiveOrders
            }
            (
                TbankEnvironment::Live,
                TbankOrderType::StopMarket
                | TbankOrderType::MarketIfTouched
                | TbankOrderType::TrailingStopMarket
                | TbankOrderType::TrailingStopLimit,
            ) => TbankExecutionService::LiveStopOrders,
            (TbankEnvironment::Sandbox, _) => TbankExecutionService::Sandbox,
        }
    }
}

/// Builds a T-Bank regular-order submission request.
pub fn build_post_order_request(
    order: &TbankSubmitOrder,
    account_id: &str,
    instrument: &TbankInstrumentMetadata,
) -> Result<PostOrderRequest> {
    validate_tbank_request_id(order.broker_request_id.as_str())?;
    let order_type = order.order_type.to_order_type().ok_or_else(|| {
        TbankAdapterError::UnsupportedOrderType(format!("{:?}", order.order_type))
    })?;
    let price = match order_type {
        OrderType::Market => None,
        OrderType::Limit => Some(price_to_quotation(
            order.limit_price.ok_or_else(|| {
                TbankAdapterError::InvalidPrice("limit price required".to_string())
            })?,
            instrument.min_price_increment,
            instrument.price_precision,
        )?),
        _ => {
            return Err(TbankAdapterError::UnsupportedOrderType(format!(
                "{order_type:?}"
            )));
        }
    };

    Ok(PostOrderRequest {
        #[allow(deprecated)]
        figi: None,
        quantity: quantity_units_to_lots(order.quantity_units, instrument.lot)?,
        price,
        direction: order.side.to_order_direction() as i32,
        account_id: account_id.to_string(),
        order_type: order_type as i32,
        order_id: order.broker_request_id.clone(),
        instrument_id: instrument.instrument_uid.clone(),
        time_in_force: tbank_time_in_force(order, instrument)? as i32,
        price_type: if instrument.price_in_points {
            PriceType::Point as i32
        } else {
            PriceType::Currency as i32
        },
        confirm_margin_trade: order.confirm_margin_trade,
    })
}

/// Builds an async request using the same quantity, price and TIF validation as PostOrder.
pub fn build_post_order_async_request(
    order: &TbankSubmitOrder,
    account_id: &str,
    instrument: &TbankInstrumentMetadata,
) -> Result<crate::grpc::generated::PostOrderAsyncRequest> {
    let r = build_post_order_request(order, account_id, instrument)?;
    Ok(crate::grpc::generated::PostOrderAsyncRequest {
        instrument_id: r.instrument_id,
        quantity: r.quantity,
        price: r.price,
        direction: r.direction,
        account_id: r.account_id,
        order_type: r.order_type,
        order_id: r.order_id,
        time_in_force: Some(r.time_in_force),
        price_type: Some(r.price_type),
        confirm_margin_trade: r.confirm_margin_trade,
    })
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    const REQUEST_ID: &str = "524b1a03-efdd-4cd0-bd56-7cc6570c7156";

    fn instrument() -> TbankInstrumentMetadata {
        TbankInstrumentMetadata {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            figi: "figi".to_string(),
            instrument_uid: "uid".to_string(),
            position_uid: "pos".to_string(),
            lot: 10,
            min_price_increment: Decimal::new(1, 2),
            currency: "RUB".to_string(),
            exchange: "MOEX".to_string(),
            price_precision: 2,
            quantity_precision: 0,
            ..Default::default()
        }
    }

    #[test]
    fn market_buy_maps_to_post_order() {
        let order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Ioc,
            quantity_units: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };
        let request = build_post_order_request(&order, "account", &instrument()).unwrap();
        assert_eq!(request.quantity, 2);
        assert_eq!(request.price, None);
        assert_eq!(request.instrument_id, "uid");
        assert_eq!(request.order_type, OrderType::Market as i32);
        assert_eq!(
            request.time_in_force,
            TimeInForceType::TimeInForceUnspecified as i32
        );
    }

    #[test]
    fn market_order_forwards_margin_confirmation() {
        let order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Ioc,
            quantity_units: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: true,
        };
        let request = build_post_order_request(&order, "account", &instrument()).unwrap();
        assert!(request.confirm_margin_trade);
    }

    #[test]
    fn market_order_rejects_non_ioc_time_in_force() {
        let order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: "client-order-1".to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Day,
            quantity_units: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };

        assert!(matches!(
            build_post_order_request(&order, "account", &instrument()),
            Err(TbankAdapterError::UnsupportedTimeInForce(_))
        ));
    }

    #[test]
    fn limit_without_price_errors() {
        let order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            order_type: TbankOrderType::Limit,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Buy,
            quantity_units: Decimal::from(20),
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };
        assert!(matches!(
            build_post_order_request(&order, "account", &instrument()),
            Err(TbankAdapterError::InvalidPrice(_))
        ));
    }

    #[test]
    fn limit_time_in_force_maps_to_tbank_contract() {
        for (time_in_force, expected) in [
            (TimeInForce::Day, TimeInForceType::TimeInForceDay),
            (TimeInForce::Ioc, TimeInForceType::TimeInForceFillAndKill),
            (TimeInForce::Fok, TimeInForceType::TimeInForceFillOrKill),
        ] {
            let order = TbankSubmitOrder {
                submit_method: crate::execution::TbankSubmitMethod::PostOrder,
                instrument_id: "SBER_TQBR.MOEX".to_string(),
                client_order_id: REQUEST_ID.to_string(),
                broker_request_id: REQUEST_ID.to_string(),
                side: TbankOrderSide::Buy,
                order_type: TbankOrderType::Limit,
                time_in_force,
                quantity_units: Decimal::from(20),
                limit_price: Some(Decimal::from(250)),
                trigger_price: None,
                trailing: None,
                confirm_margin_trade: false,
            };

            let request = build_post_order_request(&order, "account", &instrument()).unwrap();
            assert_eq!(request.time_in_force, expected as i32);
        }
    }

    #[test]
    fn regular_limit_rejects_unsupported_gtc() {
        let order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            quantity_units: Decimal::from(20),
            limit_price: Some(Decimal::from(250)),
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };

        assert!(matches!(
            build_post_order_request(&order, "account", &instrument()),
            Err(TbankAdapterError::UnsupportedTimeInForce(_))
        ));
    }

    #[test]
    fn request_id_must_be_uuid() {
        let mut order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: "client-order-1".to_string(),
            broker_request_id: "not-a-uuid".to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Ioc,
            quantity_units: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };

        assert!(build_post_order_request(&order, "account", &instrument()).is_err());
        order.broker_request_id = REQUEST_ID.to_string();
        assert!(build_post_order_request(&order, "account", &instrument()).is_ok());
    }

    #[test]
    fn service_selection_matches_environment() {
        let mut order = TbankSubmitOrder {
            submit_method: crate::execution::TbankSubmitMethod::PostOrder,
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Ioc,
            quantity_units: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        };
        assert_eq!(
            order.service(TbankEnvironment::Live),
            TbankExecutionService::LiveOrders
        );
        assert_eq!(
            order.service(TbankEnvironment::Sandbox),
            TbankExecutionService::Sandbox
        );
        order.order_type = TbankOrderType::StopMarket;
        assert_eq!(
            order.service(TbankEnvironment::Live),
            TbankExecutionService::LiveStopOrders
        );
    }
    #[test]
    fn both_methods_share_the_regular_order_capability_and_conversion_matrix() {
        use crate::common::TbankInstrumentType;
        for instrument_type in [TbankInstrumentType::Share, TbankInstrumentType::Futures] {
            let mut metadata = instrument();
            metadata.instrument_type = instrument_type;
            for order_type in [
                TbankOrderType::Market,
                TbankOrderType::Limit,
                TbankOrderType::StopMarket,
            ] {
                for tif in [
                    TimeInForce::Day,
                    TimeInForce::Ioc,
                    TimeInForce::Fok,
                    TimeInForce::Gtc,
                ] {
                    let order = TbankSubmitOrder {
                        submit_method: TbankSubmitMethod::PostOrderAsync,
                        instrument_id: metadata.instrument_id.clone(),
                        client_order_id: "matrix".into(),
                        broker_request_id: REQUEST_ID.into(),
                        side: TbankOrderSide::Sell,
                        order_type,
                        time_in_force: tif,
                        quantity_units: Decimal::from(20),
                        limit_price: Some(Decimal::new(12345, 2)),
                        trigger_price: None,
                        trailing: None,
                        confirm_margin_trade: true,
                    };
                    let sync = build_post_order_request(&order, "account", &metadata);
                    let asynchronous = build_post_order_async_request(&order, "account", &metadata);
                    let supported = match order_type {
                        TbankOrderType::Market => tif == TimeInForce::Ioc,
                        TbankOrderType::Limit => {
                            matches!(tif, TimeInForce::Day | TimeInForce::Ioc)
                                || (tif == TimeInForce::Fok
                                    && instrument_type == TbankInstrumentType::Share)
                        }
                        _ => false,
                    };
                    assert_eq!(sync.is_ok(), supported);
                    assert_eq!(asynchronous.is_ok(), supported);
                    if let (Ok(a), Ok(b)) = (sync, asynchronous) {
                        assert_eq!(a.instrument_id, b.instrument_id);
                        assert_eq!(a.quantity, b.quantity);
                        assert_eq!(a.price, b.price);
                        assert_eq!(Some(a.price_type), b.price_type);
                        assert_eq!(a.order_id, b.order_id);
                        assert_eq!(a.direction, b.direction);
                        assert_eq!(Some(a.time_in_force), b.time_in_force);
                        assert_eq!(a.order_type, b.order_type);
                        assert!(a.confirm_margin_trade && b.confirm_margin_trade);
                    }
                }
            }
        }
    }
}
