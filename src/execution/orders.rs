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
}
