use crate::{
    common::{
        TbankOrderType,
        decimal::{decimal_to_quotation, price_to_quotation, quantity_shares_to_lots},
        error::{Result, TbankAdapterError},
    },
    execution::orders::{TbankSubmitOrder, validate_tbank_request_id},
    grpc::generated::{
        ExchangeOrderType, PostStopOrderRequest, PriceType, StopOrderExpirationType,
        TakeProfitType, TrailingValueType, post_stop_order_request,
    },
    instruments::TbankInstrumentMetadata,
};
use nautilus_model::enums::{TimeInForce, TrailingOffsetType};
use rust_decimal::Decimal;

fn trailing_value(
    value: Decimal,
    value_type: TrailingOffsetType,
    field: &str,
) -> Result<(crate::grpc::generated::Quotation, TrailingValueType)> {
    if value <= Decimal::ZERO {
        return Err(TbankAdapterError::InvalidPrice(format!(
            "trailing {field} must be positive, got {value}"
        )));
    }
    match value_type {
        TrailingOffsetType::Price => Ok((
            decimal_to_quotation(value)?,
            TrailingValueType::TrailingValueAbsolute,
        )),
        TrailingOffsetType::BasisPoints => Ok((
            decimal_to_quotation(value / Decimal::from(100))?,
            TrailingValueType::TrailingValueRelative,
        )),
        unsupported => Err(TbankAdapterError::UnsupportedOrderType(format!(
            "T-Bank trailing stops support PRICE or BASIS_POINTS offsets, got {unsupported:?}"
        ))),
    }
}

/// Builds a T-Bank stop-order submission request.
pub fn build_post_stop_order_request(
    order: &TbankSubmitOrder,
    account_id: &str,
    instrument: &TbankInstrumentMetadata,
) -> Result<PostStopOrderRequest> {
    validate_tbank_request_id(order.broker_request_id.as_str())?;
    if order.time_in_force != TimeInForce::Gtc {
        return Err(TbankAdapterError::UnsupportedTimeInForce(format!(
            "T-Bank stop orders currently require GTC, got {:?}",
            order.time_in_force
        )));
    }
    let stop_order_type = order.order_type.to_stop_order_type().ok_or_else(|| {
        TbankAdapterError::UnsupportedOrderType(format!("{:?}", order.order_type))
    })?;
    let trailing = match order.order_type {
        TbankOrderType::TrailingStopMarket | TbankOrderType::TrailingStopLimit => {
            let params = order.trailing.ok_or_else(|| {
                TbankAdapterError::InvalidPrice("trailing parameters required".to_string())
            })?;
            if order.trigger_price.is_some() {
                return Err(TbankAdapterError::InvalidPrice(
                    "T-Bank native trailing stops use activation_price, not trigger_price"
                        .to_string(),
                ));
            }
            Some(params)
        }
        _ => {
            if order.trailing.is_some() {
                return Err(TbankAdapterError::UnsupportedOrderType(format!(
                    "trailing parameters are invalid for {:?}",
                    order.order_type
                )));
            }
            None
        }
    };
    let stop_price = match trailing {
        Some(params) => params
            .activation_price
            .map(|price| {
                price_to_quotation(
                    price,
                    instrument.min_price_increment,
                    instrument.price_precision,
                )
            })
            .transpose()?,
        None => Some(price_to_quotation(
            order.trigger_price.ok_or_else(|| {
                TbankAdapterError::InvalidPrice("trigger price required".to_string())
            })?,
            instrument.min_price_increment,
            instrument.price_precision,
        )?),
    };
    let price = match order.order_type {
        TbankOrderType::StopMarket | TbankOrderType::TakeProfitMarket => None,
        TbankOrderType::TrailingStopMarket | TbankOrderType::TrailingStopLimit => None,
        TbankOrderType::Market | TbankOrderType::Limit => {
            return Err(TbankAdapterError::UnsupportedOrderType(format!(
                "{:?}",
                order.order_type
            )));
        }
    };

    Ok(PostStopOrderRequest {
        #[allow(deprecated)]
        figi: None,
        quantity: quantity_shares_to_lots(order.quantity_shares, instrument.lot)?,
        price,
        stop_price,
        direction: order.side.to_stop_order_direction() as i32,
        account_id: account_id.to_string(),
        expiration_type: StopOrderExpirationType::GoodTillCancel as i32,
        stop_order_type: stop_order_type as i32,
        expire_date: None,
        instrument_id: instrument.instrument_uid.clone(),
        exchange_order_type: match order.order_type {
            TbankOrderType::StopMarket
            | TbankOrderType::TakeProfitMarket
            | TbankOrderType::TrailingStopMarket => ExchangeOrderType::Market,
            TbankOrderType::TrailingStopLimit => ExchangeOrderType::Limit,
            TbankOrderType::Market | TbankOrderType::Limit => ExchangeOrderType::Unspecified,
        } as i32,
        take_profit_type: match order.order_type {
            TbankOrderType::TakeProfitMarket => TakeProfitType::Regular,
            TbankOrderType::TrailingStopMarket | TbankOrderType::TrailingStopLimit => {
                TakeProfitType::Trailing
            }
            TbankOrderType::Market | TbankOrderType::Limit | TbankOrderType::StopMarket => {
                TakeProfitType::Unspecified
            }
        } as i32,
        trailing_data: trailing
            .map(|params| {
                let (indent, indent_type) = trailing_value(
                    params.trailing_offset,
                    params.trailing_offset_type,
                    "offset",
                )?;
                let (spread, spread_type) = match params.limit_offset {
                    Some(value) => {
                        let (value, value_type) =
                            trailing_value(value, params.trailing_offset_type, "limit offset")?;
                        (Some(value), value_type)
                    }
                    None if order.order_type == TbankOrderType::TrailingStopMarket => {
                        (None, TrailingValueType::TrailingValueUnspecified)
                    }
                    None => {
                        return Err(TbankAdapterError::InvalidPrice(
                            "TrailingStopLimit requires limit_offset".to_string(),
                        ));
                    }
                };
                Ok(post_stop_order_request::TrailingData {
                    indent: Some(indent),
                    indent_type: indent_type as i32,
                    spread,
                    spread_type: spread_type as i32,
                })
            })
            .transpose()?,
        price_type: PriceType::Currency as i32,
        order_id: order.broker_request_id.clone(),
        confirm_margin_trade: order.confirm_margin_trade,
        instant_execution: trailing.map(|params| params.activation_price.is_none()),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::enums::TriggerType;
    use rust_decimal::Decimal;

    use crate::{
        common::{TbankOrderSide, TbankOrderType},
        execution::{TbankTrailingStopParams, orders::TbankSubmitOrder},
        grpc::generated::{ExchangeOrderType, StopOrderType, TakeProfitType, TrailingValueType},
        instruments::TbankInstrumentMetadata,
    };

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
        }
    }

    #[test]
    fn take_profit_market_maps_to_regular_take_profit() {
        let order = TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::TakeProfitMarket,
            time_in_force: TimeInForce::Gtc,
            quantity_shares: Decimal::from(20),
            limit_price: None,
            trigger_price: Some(Decimal::new(26_000, 2)),
            trailing: None,
            confirm_margin_trade: false,
        };
        let request = build_post_stop_order_request(&order, "account", &instrument()).unwrap();
        assert_eq!(request.quantity, 2);
        assert_eq!(request.stop_order_type, StopOrderType::TakeProfit as i32);
        assert_eq!(
            request.exchange_order_type,
            ExchangeOrderType::Market as i32
        );
        assert_eq!(request.take_profit_type, TakeProfitType::Regular as i32);
        assert!(request.price.is_none());
        assert!(request.stop_price.is_some());
    }

    #[test]
    fn stop_order_forwards_margin_confirmation() {
        let order = TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::StopMarket,
            time_in_force: TimeInForce::Gtc,
            quantity_shares: Decimal::from(20),
            limit_price: None,
            trigger_price: Some(Decimal::new(25_000, 2)),
            trailing: None,
            confirm_margin_trade: true,
        };
        let request = build_post_stop_order_request(&order, "account", &instrument()).unwrap();
        assert!(request.confirm_margin_trade);
    }

    #[test]
    fn stop_order_rejects_request_id_longer_than_tbank_limit() {
        let order = TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156-P33-SL".to_string(),
            broker_request_id: "not-a-uuid".to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::StopMarket,
            time_in_force: TimeInForce::Gtc,
            quantity_shares: Decimal::from(20),
            limit_price: None,
            trigger_price: Some(Decimal::new(25_000, 2)),
            trailing: None,
            confirm_margin_trade: true,
        };
        let error = build_post_stop_order_request(&order, "account", &instrument())
            .expect_err("overlong broker request id must be rejected locally");

        assert!(error.to_string().contains("canonical 36-character UUID"));
    }

    #[test]
    fn trailing_stop_market_maps_basis_points_to_tbank_percent() {
        let order = TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::TrailingStopMarket,
            time_in_force: TimeInForce::Gtc,
            quantity_shares: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: Some(TbankTrailingStopParams {
                activation_price: None,
                trailing_offset: Decimal::from(125),
                trailing_offset_type: TrailingOffsetType::BasisPoints,
                limit_offset: None,
                trigger_type: None,
            }),
            confirm_margin_trade: false,
        };

        let request = build_post_stop_order_request(&order, "account", &instrument()).unwrap();
        let trailing = request.trailing_data.unwrap();
        assert_eq!(request.stop_order_type, StopOrderType::TakeProfit as i32);
        assert_eq!(request.take_profit_type, TakeProfitType::Trailing as i32);
        assert_eq!(
            request.exchange_order_type,
            ExchangeOrderType::Market as i32
        );
        assert_eq!(request.instant_execution, Some(true));
        assert!(request.stop_price.is_none());
        assert_eq!(
            trailing.indent_type,
            TrailingValueType::TrailingValueRelative as i32
        );
        assert_eq!(
            crate::common::decimal::quotation_to_decimal(trailing.indent.as_ref().unwrap()),
            Decimal::new(125, 2)
        );
        assert!(trailing.spread.is_none());
    }

    #[test]
    fn trailing_stop_limit_maps_activation_indent_and_spread() {
        let order = TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: REQUEST_ID.to_string(),
            broker_request_id: REQUEST_ID.to_string(),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::TrailingStopLimit,
            time_in_force: TimeInForce::Gtc,
            quantity_shares: Decimal::from(20),
            limit_price: None,
            trigger_price: None,
            trailing: Some(TbankTrailingStopParams {
                activation_price: Some(Decimal::from(260)),
                trailing_offset: Decimal::from(5),
                trailing_offset_type: TrailingOffsetType::Price,
                limit_offset: Some(Decimal::from(2)),
                trigger_type: Some(TriggerType::LastPrice),
            }),
            confirm_margin_trade: false,
        };

        let request = build_post_stop_order_request(&order, "account", &instrument()).unwrap();
        let trailing = request.trailing_data.unwrap();
        assert_eq!(request.exchange_order_type, ExchangeOrderType::Limit as i32);
        assert_eq!(request.instant_execution, Some(false));
        assert_eq!(
            crate::common::decimal::quotation_to_decimal(request.stop_price.as_ref().unwrap()),
            Decimal::from(260)
        );
        assert_eq!(
            trailing.indent_type,
            TrailingValueType::TrailingValueAbsolute as i32
        );
        assert_eq!(
            crate::common::decimal::quotation_to_decimal(trailing.spread.as_ref().unwrap()),
            Decimal::from(2)
        );
    }
}
