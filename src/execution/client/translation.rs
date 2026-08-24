//! Translation between T-Bank protobuf models and Nautilus reports.

use super::*;
use crate::grpc::generated::{StopOrderDirection, StopOrderType};
use anyhow::Context;

pub(super) fn tbank_side(side: OrderSide) -> anyhow::Result<crate::common::TbankOrderSide> {
    match side {
        OrderSide::Buy => Ok(crate::common::TbankOrderSide::Buy),
        OrderSide::Sell => Ok(crate::common::TbankOrderSide::Sell),
        OrderSide::NoOrderSide => anyhow::bail!("unsupported order side {side:?}"),
    }
}

pub(super) fn tbank_order_type(
    order_type: OrderType,
) -> anyhow::Result<crate::common::TbankOrderType> {
    match order_type {
        OrderType::Market => Ok(crate::common::TbankOrderType::Market),
        OrderType::Limit => Ok(crate::common::TbankOrderType::Limit),
        OrderType::StopMarket => Ok(crate::common::TbankOrderType::StopMarket),
        OrderType::MarketIfTouched => Ok(crate::common::TbankOrderType::MarketIfTouched),
        OrderType::TrailingStopMarket => Ok(crate::common::TbankOrderType::TrailingStopMarket),
        OrderType::TrailingStopLimit => Ok(crate::common::TbankOrderType::TrailingStopLimit),
        other => anyhow::bail!("unsupported T-Bank order type {other:?}"),
    }
}

pub(super) fn nautilus_order_type(order_type: i32) -> OrderType {
    match crate::grpc::generated::OrderType::try_from(order_type).ok() {
        Some(crate::grpc::generated::OrderType::Limit) => OrderType::Limit,
        Some(crate::grpc::generated::OrderType::Market) => OrderType::Market,
        _ => OrderType::Market,
    }
}

pub(super) fn nautilus_stop_order_type(stop: &StopOrder) -> OrderType {
    if matches!(
        crate::grpc::generated::TakeProfitType::try_from(stop.take_profit_type).ok(),
        Some(crate::grpc::generated::TakeProfitType::Trailing)
    ) {
        return match crate::grpc::generated::ExchangeOrderType::try_from(stop.exchange_order_type)
            .ok()
        {
            Some(crate::grpc::generated::ExchangeOrderType::Limit) => OrderType::TrailingStopLimit,
            _ => OrderType::TrailingStopMarket,
        };
    }
    match StopOrderType::try_from(stop.order_type).ok() {
        Some(StopOrderType::StopLoss) => OrderType::StopMarket,
        Some(StopOrderType::StopLimit) => OrderType::StopLimit,
        Some(StopOrderType::TakeProfit) => {
            match crate::grpc::generated::ExchangeOrderType::try_from(stop.exchange_order_type).ok()
            {
                Some(crate::grpc::generated::ExchangeOrderType::Limit) => OrderType::LimitIfTouched,
                _ => OrderType::MarketIfTouched,
            }
        }
        _ => OrderType::StopMarket,
    }
}

fn nautilus_stop_order_type_with_context(
    stop: &StopOrder,
    managed_order_type: Option<crate::common::TbankOrderType>,
) -> OrderType {
    managed_order_type
        .map(|order_type| nautilus_stream_stop_order_type(Some(order_type)))
        .unwrap_or_else(|| nautilus_stop_order_type(stop))
}

fn currency_price_to_nautilus_decimal(
    value: Decimal,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Decimal> {
    // Keep this for fields whose endpoint contract is explicitly currency;
    // point-valued response fields must use a direct parser instead.
    match metadata.filter(|metadata| metadata.price_in_points) {
        Some(metadata) => crate::common::decimal::futures_currency_to_points(
            value,
            metadata.min_price_increment,
            metadata.min_price_increment_amount.ok_or_else(|| {
                anyhow::anyhow!("futures price conversion requires min price increment amount")
            })?,
        )
        .map_err(anyhow::Error::from),
        None => Ok(value),
    }
}

fn average_currency_price_to_nautilus_decimal(
    value: Decimal,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Decimal> {
    // Execution averages are currency-valued and may legitimately fall
    // between futures ticks, so they use the non-validating conversion.
    let points = match metadata.filter(|metadata| metadata.price_in_points) {
        Some(metadata) => {
            crate::common::decimal::futures_currency_to_points_without_tick_validation(
                value,
                metadata.min_price_increment,
                metadata.min_price_increment_amount.ok_or_else(|| {
                    anyhow::anyhow!("futures price conversion requires min price increment amount")
                })?,
            )
            .map_err(anyhow::Error::from)
        }
        None => Ok(value),
    }?;
    bounded_nautilus_price_decimal(points)
}

fn bounded_nautilus_price_decimal(value: Decimal) -> anyhow::Result<Decimal> {
    Price::from_decimal_dp(value, nautilus_model::types::fixed::FIXED_PRECISION)
        .map(|price| price.as_decimal())
        .map_err(anyhow::Error::from)
}

/// Converts a currency-valued execution price into Nautilus price points.
///
/// T-Bank may report a cumulative execution average between futures ticks, so this conversion
/// intentionally does not validate the resulting point price against the tick size.
pub(super) fn execution_price_from_money_value_for_instrument(
    value: &MoneyValue,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Price> {
    let points = average_currency_price_to_nautilus_decimal(
        crate::common::decimal::money_value_to_decimal(value),
        metadata,
    )?;
    Price::from_decimal_dp(points, nautilus_model::types::fixed::FIXED_PRECISION)
        .map_err(anyhow::Error::from)
}

pub(super) fn average_price_from_money_value_for_instrument(
    value: &MoneyValue,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Price> {
    execution_price_from_money_value_for_instrument(value, metadata)
}

pub(super) fn cached_instrument_metadata(
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
    instrument_uid: &str,
    figi: &str,
    instrument_id: &str,
) -> Option<TbankInstrumentMetadata> {
    instruments.and_then(|instruments| {
        instruments
            .lock()
            .expect("instruments lock")
            .values()
            .find(|metadata| {
                (!instrument_uid.is_empty() && metadata.instrument_uid == instrument_uid)
                    || (!figi.is_empty() && metadata.figi == figi)
                    || (!instrument_id.is_empty() && metadata.instrument_id == instrument_id)
            })
            .cloned()
    })
}

pub(super) fn trailing_params_from_stop(
    stop: &StopOrder,
) -> anyhow::Result<Option<crate::execution::TbankTrailingStopParams>> {
    // GetStopOrders returns futures stop prices in Nautilus points. Absolute
    // trailing values use the same point unit as the explicit point-valued
    // PostStopOrder request; equities retain their native currency number.
    if !matches!(
        crate::grpc::generated::TakeProfitType::try_from(stop.take_profit_type).ok(),
        Some(crate::grpc::generated::TakeProfitType::Trailing)
    ) {
        return Ok(None);
    }
    let data = stop
        .trailing_data
        .as_ref()
        .context("T-Bank trailing stop is missing trailing_data")?;
    let value_type = crate::grpc::generated::TrailingValueType::try_from(data.indent_type)
        .context("T-Bank trailing stop has unknown indent_type")?;
    let indent = data
        .indent
        .as_ref()
        .context("T-Bank trailing stop is missing indent")?;
    let (trailing_offset, trailing_offset_type) = match value_type {
        crate::grpc::generated::TrailingValueType::TrailingValueAbsolute => (
            crate::common::decimal::quotation_to_decimal(indent),
            nautilus_model::enums::TrailingOffsetType::Price,
        ),
        crate::grpc::generated::TrailingValueType::TrailingValueRelative => (
            crate::common::decimal::quotation_to_decimal(indent) * Decimal::from(100),
            nautilus_model::enums::TrailingOffsetType::BasisPoints,
        ),
        crate::grpc::generated::TrailingValueType::TrailingValueUnspecified => {
            anyhow::bail!("T-Bank trailing stop has unspecified indent_type")
        }
    };
    let limit_offset = match data.spread.as_ref() {
        Some(spread) => {
            let spread_type = crate::grpc::generated::TrailingValueType::try_from(data.spread_type)
                .context("T-Bank trailing stop has unknown spread_type")?;
            if spread_type != value_type {
                anyhow::bail!(
                    "T-Bank trailing indent_type and spread_type cannot be represented by one Nautilus TrailingOffsetType"
                );
            }
            Some(match spread_type {
                crate::grpc::generated::TrailingValueType::TrailingValueAbsolute => {
                    crate::common::decimal::quotation_to_decimal(spread)
                }
                crate::grpc::generated::TrailingValueType::TrailingValueRelative => {
                    crate::common::decimal::quotation_to_decimal(spread) * Decimal::from(100)
                }
                crate::grpc::generated::TrailingValueType::TrailingValueUnspecified => {
                    anyhow::bail!("T-Bank trailing stop has unspecified spread_type")
                }
            })
        }
        None if matches!(
            crate::grpc::generated::ExchangeOrderType::try_from(stop.exchange_order_type).ok(),
            Some(crate::grpc::generated::ExchangeOrderType::Limit)
        ) =>
        {
            anyhow::bail!("T-Bank trailing limit stop is missing protective spread")
        }
        None => None,
    };
    Ok(Some(crate::execution::TbankTrailingStopParams {
        activation_price: stop
            .stop_price
            .as_ref()
            .map(|value| price_from_point_valued_money_value(value).map(|price| price.as_decimal()))
            .transpose()?,
        trailing_offset,
        trailing_offset_type,
        limit_offset,
        trigger_type: Some(TriggerType::LastPrice),
    }))
}

pub(super) fn apply_trailing_params(
    mut report: OrderStatusReport,
    params: Option<crate::execution::TbankTrailingStopParams>,
) -> anyhow::Result<OrderStatusReport> {
    let Some(params) = params else {
        return Ok(report);
    };
    if let Some(activation_price) = params.activation_price {
        report = report.with_activation_price(Price::from_decimal(activation_price)?);
    }
    report = report
        .with_trailing_offset(params.trailing_offset)
        .with_trailing_offset_type(params.trailing_offset_type);
    if let Some(limit_offset) = params.limit_offset {
        report = report.with_limit_offset(limit_offset);
    }
    if let Some(trigger_type) = params.trigger_type {
        report = report.with_trigger_type(trigger_type);
    }
    Ok(report)
}

pub(super) fn nautilus_order_side(direction: i32) -> OrderSide {
    match OrderDirection::try_from(direction).ok() {
        Some(OrderDirection::Sell) => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

pub(super) fn nautilus_stop_order_side(direction: i32) -> OrderSide {
    match StopOrderDirection::try_from(direction).ok() {
        Some(StopOrderDirection::Sell) => OrderSide::Sell,
        Some(StopOrderDirection::Buy) => OrderSide::Buy,
        _ => OrderSide::NoOrderSide,
    }
}

pub(super) fn nautilus_order_status(status: i32, requested: i64, executed: i64) -> OrderStatus {
    match TbankOrderExecutionReportStatus::try_from(status).ok() {
        Some(TbankOrderExecutionReportStatus::ExecutionReportStatusFill) => OrderStatus::Filled,
        Some(TbankOrderExecutionReportStatus::ExecutionReportStatusRejected) => {
            OrderStatus::Rejected
        }
        Some(TbankOrderExecutionReportStatus::ExecutionReportStatusCancelled) => {
            OrderStatus::Canceled
        }
        Some(TbankOrderExecutionReportStatus::ExecutionReportStatusPartiallyfill) => {
            OrderStatus::PartiallyFilled
        }
        Some(TbankOrderExecutionReportStatus::ExecutionReportStatusNew) if executed > 0 => {
            if executed >= requested {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            }
        }
        _ => OrderStatus::Accepted,
    }
}

pub(super) fn nautilus_stream_order_status(
    state: &order_state_stream_response::OrderState,
) -> OrderStatus {
    let status = nautilus_order_status(
        state.execution_report_status,
        state.lots_requested,
        state.lots_executed,
    );
    if status == OrderStatus::PartiallyFilled
        && state.completion_time.is_some()
        && state.lots_executed < state.lots_requested
        && (state.lots_cancelled > 0 || state.lots_left == 0)
    {
        OrderStatus::Canceled
    } else {
        status
    }
}

pub(super) fn nautilus_time_in_force(time_in_force: i32) -> TimeInForce {
    match TbankTimeInForceType::try_from(time_in_force).ok() {
        Some(TbankTimeInForceType::TimeInForceFillAndKill) => TimeInForce::Ioc,
        Some(TbankTimeInForceType::TimeInForceFillOrKill) => TimeInForce::Fok,
        Some(TbankTimeInForceType::TimeInForceDay)
        | Some(TbankTimeInForceType::TimeInForceUnspecified)
        | None => TimeInForce::Day,
    }
}

pub(super) fn nautilus_stream_time_in_force(
    order_type: i32,
    time_in_force: i32,
    managed_time_in_force: Option<TimeInForce>,
) -> TimeInForce {
    managed_time_in_force.unwrap_or_else(|| {
        if nautilus_order_type(order_type) == OrderType::Market {
            TimeInForce::Ioc
        } else {
            nautilus_time_in_force(time_in_force)
        }
    })
}

pub(super) fn nautilus_stop_order_status(status: i32) -> OrderStatus {
    match StopOrderStatusOption::try_from(status).ok() {
        Some(StopOrderStatusOption::StopOrderStatusExecuted) => OrderStatus::Triggered,
        Some(StopOrderStatusOption::StopOrderStatusCanceled) => OrderStatus::Canceled,
        Some(StopOrderStatusOption::StopOrderStatusExpired) => OrderStatus::Expired,
        Some(StopOrderStatusOption::StopOrderStatusActive) => OrderStatus::Accepted,
        _ => OrderStatus::Accepted,
    }
}

pub(super) fn average_price_from_executed_order_price(
    value: &MoneyValue,
    lots_executed: i64,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Option<Decimal>> {
    if lots_executed <= 0 {
        return Ok(None);
    }
    let average_price =
        crate::common::decimal::money_value_to_decimal(value) / Decimal::from(lots_executed);
    average_currency_price_to_nautilus_decimal(average_price, metadata).map(Some)
}

fn average_price_from_order_stages(
    stages: &[crate::grpc::generated::OrderStage],
    lots_executed: i64,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Option<Decimal>> {
    if lots_executed <= 0 {
        return Ok(None);
    }
    let mut total_quantity = Decimal::ZERO;
    let mut total_value = Decimal::ZERO;
    let mut missing_price = false;
    let mut invalid_quantity = false;
    for stage in stages {
        if stage.quantity == 0 {
            continue;
        }
        if stage.quantity < 0 {
            invalid_quantity = true;
            continue;
        }
        let Some(price) = stage.price.as_ref() else {
            missing_price = true;
            continue;
        };
        let quantity = Decimal::from(stage.quantity);
        total_quantity += quantity;
        total_value += crate::common::decimal::money_value_to_decimal(price) * quantity;
    }
    if missing_price || invalid_quantity || total_quantity != Decimal::from(lots_executed) {
        return Ok(None);
    }
    average_currency_price_to_nautilus_decimal(total_value / total_quantity, metadata).map(Some)
}

pub(super) fn order_status_report_from_state_with_metadata(
    account_id: AccountId,
    state: OrderState,
    ts_init: UnixNanos,
    instrument_id: nautilus_model::identifiers::InstrumentId,
    lot: u32,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<OrderStatusReport> {
    let ts_event = state
        .order_date
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    let order_type = nautilus_order_type(state.order_type);
    let time_in_force = if order_type == OrderType::Market {
        TimeInForce::Ioc
    } else {
        TimeInForce::Day
    };
    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        nonempty_client_order_id(&state.order_request_id),
        state.order_id.as_str().into(),
        nautilus_order_side(state.direction),
        order_type,
        time_in_force,
        nautilus_order_status(
            state.execution_report_status,
            state.lots_requested,
            state.lots_executed,
        ),
        lots_to_quantity(state.lots_requested, lot)?,
        lots_to_quantity(state.lots_executed, lot)?,
        ts_event,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = state
        .initial_security_price
        .as_ref()
        .map(|value| price_from_money_value_for_instrument(value, metadata))
        .transpose()?
    {
        report = report.with_price(price);
    }
    if let Some(avg_px) =
        average_price_from_order_stages(&state.stages, state.lots_executed, metadata)?
    {
        report = report.with_avg_px(avg_px);
    } else if let Some(value) = state.executed_order_price.as_ref()
        && let Some(avg_px) =
            average_price_from_executed_order_price(value, state.lots_executed, metadata)?
    {
        report = report.with_avg_px(avg_px);
    }
    Ok(report)
}

#[cfg(test)]
pub(super) fn stream_order_status_report_from_state(
    state: order_state_stream_response::OrderState,
    venue_order_id: &str,
    ts_init: UnixNanos,
    client_order_id_override: Option<&str>,
    managed_time_in_force: Option<TimeInForce>,
) -> anyhow::Result<OrderStatusReport> {
    stream_order_status_report_from_state_with_instruments(
        state,
        venue_order_id,
        ts_init,
        client_order_id_override,
        managed_time_in_force,
        None,
    )
}

pub(super) fn stream_order_status_report_from_state_with_instruments(
    state: order_state_stream_response::OrderState,
    venue_order_id: &str,
    ts_init: UnixNanos,
    client_order_id_override: Option<&str>,
    managed_time_in_force: Option<TimeInForce>,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> anyhow::Result<OrderStatusReport> {
    let instrument_id = match instruments {
        Some(instruments) => instrument_id_from_ticker_class_or_cached_identity(
            &state.ticker,
            &state.class_code,
            &state.instrument_uid,
            "",
            Some(instruments),
        )?,
        None => {
            #[cfg(test)]
            {
                instrument_id_from_ticker_class_or_identity(
                    &state.ticker,
                    &state.class_code,
                    &state.instrument_uid,
                    "",
                )?
            }
            #[cfg(not(test))]
            {
                anyhow::bail!(
                    "instrument metadata cache is required for T-Bank order-state translation"
                );
            }
        }
    };
    let metadata = instruments.and_then(|instruments| {
        instruments
            .lock()
            .expect("instruments lock")
            .values()
            .find(|metadata| {
                metadata.instrument_uid == state.instrument_uid
                    || metadata.instrument_id == instrument_id.to_string()
            })
            .cloned()
    });
    let lot = u32::try_from(state.lot_size.max(1)).unwrap_or(1);
    let ts_accepted = state
        .created_at
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    let ts_last = state
        .completion_time
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_accepted);
    let mut report = OrderStatusReport::new(
        nautilus_account_id(&state.account_id),
        instrument_id,
        client_order_id_override
            .and_then(nonempty_client_order_id)
            .or_else(|| {
                state
                    .order_request_id
                    .as_ref()
                    .and_then(|value| nonempty_client_order_id(value))
            }),
        venue_order_id.into(),
        nautilus_order_side(state.direction),
        nautilus_order_type(state.order_type),
        nautilus_stream_time_in_force(state.order_type, state.time_in_force, managed_time_in_force),
        nautilus_stream_order_status(&state),
        lots_to_quantity(state.lots_requested, lot)?,
        lots_to_quantity(state.lots_executed, lot)?,
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = state
        .order_price
        .as_ref()
        .map(price_from_point_valued_money_value)
        .transpose()?
    {
        report = report.with_price(price);
    }
    if let Some(avg_px) = average_price_from_stream_trades(&state)? {
        report = report.with_avg_px(avg_px);
    } else if let Some(value) = state.executed_order_price.as_ref() {
        let avg_px = average_price_from_money_value_for_instrument(value, metadata.as_ref())?;
        report = report.with_avg_px(avg_px.as_decimal());
    }
    Ok(report)
}

pub(super) fn stream_order_state_client_order_id(
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    order_request_id: Option<&str>,
    order_id: &str,
    trade_order_id: &str,
) -> Option<String> {
    let order_request_id = order_request_id.filter(|value| !value.is_empty());
    let index = broker_order_index.lock().expect("broker_order_index lock");
    if !order_id.is_empty()
        && let Some(client_order_id) = index.client_order_id_for_venue_order_id(order_id)
    {
        return Some(client_order_id);
    }
    if !trade_order_id.is_empty()
        && let Some(client_order_id) = index.client_order_id_for_venue_order_id(trade_order_id)
    {
        return Some(client_order_id);
    }
    order_request_id.and_then(|request_id| index.client_order_id_for_request_id(request_id))
}

pub(super) fn resolve_stream_order_venue_id(
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    fill_projection: &Arc<Mutex<TbankFillProjection>>,
    order_request_id: Option<&str>,
    order_id: &str,
    trade_order_id: &str,
    execution_report_status: i32,
) -> Option<TbankResolvedStreamOrderIdentity> {
    let order_request_id = order_request_id.filter(|value| !value.is_empty());
    let mut index = broker_order_index.lock().expect("broker_order_index lock");
    let client_order_id =
        order_request_id.and_then(|request_id| index.client_order_id_for_request_id(request_id));
    let is_initial_ack = TbankOrderExecutionReportStatus::try_from(execution_report_status).ok()
        == Some(TbankOrderExecutionReportStatus::ExecutionReportStatusNew);
    if let Some(identity) = index.identity_for(None, Some(trade_order_id))
        && identity.route == TbankBrokerOrderRoute::StopOrder
    {
        let stop_order_id = identity.broker_order_id;
        let child_order_id = if order_id.is_empty() {
            stop_order_id.clone()
        } else {
            order_id.to_string()
        };
        if is_initial_ack && child_order_id != stop_order_id {
            return None;
        }
        let mut pending_cancel = None;
        if child_order_id != stop_order_id {
            let client_order_id = index
                .client_order_id_for_venue_order_id(stop_order_id.as_str())
                .unwrap_or_default();
            let should_cancel = index.record_activated_stop_child_mapping(
                client_order_id.as_str(),
                stop_order_id.as_str(),
                child_order_id.as_str(),
            );
            let mut projection = fill_projection.lock().expect("fill_projection lock");
            merge_fill_projection_alias(
                &mut projection,
                child_order_id.as_str(),
                stop_order_id.as_str(),
            );
            if should_cancel {
                pending_cancel = Some(TbankBrokerOrderIdentity {
                    route: TbankBrokerOrderRoute::RegularOrder,
                    broker_order_id: child_order_id,
                });
            }
        }
        return Some(TbankResolvedStreamOrderIdentity {
            venue_order_id: stop_order_id,
            pending_cancel,
        });
    }

    if let Some(identity) = index.identity_for(None, Some(trade_order_id))
        && identity.route == TbankBrokerOrderRoute::RegularOrder
    {
        let canonical_order_id = index.canonical_venue_order_id_or_self(trade_order_id);
        let mut pending_cancel = None;
        if !is_initial_ack
            && !order_id.is_empty()
            && order_id != trade_order_id
            && let Some(client_order_id) = index.client_order_id_for_venue_order_id(trade_order_id)
        {
            let should_cancel = index.record_regular_order_alias(
                client_order_id.as_str(),
                canonical_order_id.as_str(),
                order_id,
            );
            let mut projection = fill_projection.lock().expect("fill_projection lock");
            merge_fill_projection_alias(&mut projection, order_id, canonical_order_id.as_str());
            if should_cancel {
                pending_cancel = Some(TbankBrokerOrderIdentity {
                    route: TbankBrokerOrderRoute::RegularOrder,
                    broker_order_id: order_id.to_string(),
                });
            }
        }
        return Some(TbankResolvedStreamOrderIdentity {
            venue_order_id: canonical_order_id,
            pending_cancel,
        });
    }

    if let Some(client_order_id) = client_order_id.as_deref()
        && let Some(identity) = index.identity_for(Some(client_order_id), None)
    {
        let known_exchange_id = identity.broker_order_id;
        let canonical_order_id = index.canonical_venue_order_id_or_self(&known_exchange_id);
        let mut pending_cancel = None;
        if !is_initial_ack && !order_id.is_empty() && order_id != known_exchange_id {
            let should_cancel = index.record_regular_order_alias(
                client_order_id,
                canonical_order_id.as_str(),
                order_id,
            );
            let mut projection = fill_projection.lock().expect("fill_projection lock");
            merge_fill_projection_alias(&mut projection, order_id, canonical_order_id.as_str());
            if should_cancel {
                pending_cancel = Some(TbankBrokerOrderIdentity {
                    route: TbankBrokerOrderRoute::RegularOrder,
                    broker_order_id: order_id.to_string(),
                });
            }
        }
        return Some(TbankResolvedStreamOrderIdentity {
            venue_order_id: canonical_order_id,
            pending_cancel,
        });
    }
    if is_initial_ack {
        // T-Bank documents that the first NEW event carries an internal broker-receipt ID in
        // `order_id`, before an exchange ID exists. Never persist or cancel by that value.
        // Regular unknown submits are reconciled by their UUID request key; activated stops
        // schedule the equivalent child reconciliation in `publish_order_state_stream`.
        return None;
    }

    let venue_order_id = if !order_id.is_empty() {
        order_id
    } else {
        trade_order_id
    };
    if venue_order_id.is_empty() {
        return None;
    }
    let pending_cancel = if let Some(client_order_id) = client_order_id.as_deref() {
        let route = index
            .route_for_client_order_id(client_order_id)
            .unwrap_or(TbankBrokerOrderRoute::RegularOrder);
        index
            .record_mapping(route, client_order_id, venue_order_id)
            .then(|| TbankBrokerOrderIdentity {
                route,
                broker_order_id: venue_order_id.to_string(),
            })
    } else {
        index.record_venue_order_id(TbankBrokerOrderRoute::RegularOrder, venue_order_id);
        None
    };
    Some(TbankResolvedStreamOrderIdentity {
        venue_order_id: venue_order_id.to_string(),
        pending_cancel,
    })
}

#[cfg(test)]
pub(super) fn stream_stop_order_status_report_from_state(
    state: order_state_stream_response::StopOrderState,
    ts_init: UnixNanos,
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
) -> anyhow::Result<Option<OrderStatusReport>> {
    stream_stop_order_status_report_from_state_with_instruments(
        state,
        ts_init,
        pending_submits,
        broker_order_index,
        None,
    )
}

pub(super) fn stream_stop_order_status_report_from_state_with_instruments(
    state: order_state_stream_response::StopOrderState,
    ts_init: UnixNanos,
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> anyhow::Result<Option<OrderStatusReport>> {
    let client_order_id = {
        let index = broker_order_index.lock().expect("broker_order_index lock");
        index.client_order_id_for_venue_order_id(state.stop_order_id.as_str())
    };
    let Some(client_order_id) = client_order_id else {
        tracing::debug!("ignoring unmanaged T-Bank stop-order stream event");
        return Ok(None);
    };
    let pending = {
        let pending_submits = pending_submits.lock().expect("pending_submits lock");
        pending_submits.get(client_order_id.as_str()).cloned()
    };
    let managed_context = match pending.as_ref() {
        Some(pending) => TbankManagedOrderContext {
            side: Some(pending.side),
            order_type: Some(pending.order_type),
            report_order_type: None,
            time_in_force: Some(pending.time_in_force),
            quantity_units: Some(pending.quantity_units),
            trailing: pending.trailing,
        },
        None => {
            let index = broker_order_index.lock().expect("broker_order_index lock");
            index
                .managed_context_for_client_order_id(client_order_id.as_str())
                .unwrap_or(TbankManagedOrderContext {
                    side: None,
                    order_type: Some(crate::common::TbankOrderType::StopMarket),
                    report_order_type: None,
                    time_in_force: Some(TimeInForce::Gtc),
                    quantity_units: None,
                    trailing: None,
                })
        }
    };
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        &state.ticker,
        &state.class_code,
        &state.instrument_uid,
        "",
        instruments,
    )?;
    let ts_event = state
        .created_at
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    let mut report = OrderStatusReport::new(
        nautilus_account_id(&state.account_id),
        instrument_id,
        Some(client_order_id.as_str().into()),
        state.stop_order_id.as_str().into(),
        nautilus_order_side(state.direction),
        nautilus_stream_stop_order_type_from_context(&managed_context),
        TimeInForce::Gtc,
        nautilus_stop_order_status(state.status),
        Quantity::from_decimal(managed_context.quantity_units.unwrap_or(Decimal::ZERO))?,
        Quantity::from(0),
        ts_event,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = state
        .price
        .as_ref()
        .map(price_from_point_valued_money_value)
        .transpose()?
    {
        report = report.with_price(price);
    }
    if let Some(trigger_price) = state
        .stop_price
        .as_ref()
        .map(price_from_point_valued_money_value)
        .transpose()?
    {
        if managed_context.trailing.is_some() {
            report = report.with_activation_price(trigger_price);
        } else {
            report = report.with_trigger_price(trigger_price);
        }
    }
    report = apply_trailing_params(report, managed_context.trailing)?;
    Ok(Some(with_default_stop_trigger_type(report)))
}

pub(super) fn nautilus_stream_stop_order_type(
    order_type: Option<crate::common::TbankOrderType>,
) -> OrderType {
    match order_type {
        Some(crate::common::TbankOrderType::StopMarket) | None => OrderType::StopMarket,
        Some(crate::common::TbankOrderType::MarketIfTouched) => OrderType::MarketIfTouched,
        Some(crate::common::TbankOrderType::Market) => OrderType::Market,
        Some(crate::common::TbankOrderType::Limit) => OrderType::Limit,
        Some(crate::common::TbankOrderType::TrailingStopMarket) => OrderType::TrailingStopMarket,
        Some(crate::common::TbankOrderType::TrailingStopLimit) => OrderType::TrailingStopLimit,
    }
}

pub(super) fn nautilus_stream_stop_order_type_from_context(
    context: &TbankManagedOrderContext,
) -> OrderType {
    context
        .report_order_type
        .or_else(|| {
            context
                .order_type
                .map(|order_type| nautilus_stream_stop_order_type(Some(order_type)))
        })
        .unwrap_or(OrderType::StopMarket)
}

pub(super) fn with_default_stop_trigger_type(report: OrderStatusReport) -> OrderStatusReport {
    if matches!(
        report.order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
    ) && report.trigger_type.is_none()
    {
        report.with_trigger_type(TriggerType::Default)
    } else {
        report
    }
}

pub(super) fn stop_order_status_report_with_context(
    account_id: AccountId,
    stop: StopOrder,
    ts_init: UnixNanos,
    lot_size: u32,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
    managed_order_type: Option<crate::common::TbankOrderType>,
) -> anyhow::Result<OrderStatusReport> {
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        &stop.ticker,
        &stop.class_code,
        &stop.instrument_uid,
        &stop.figi,
        instruments,
    )?;
    let ts_event = stop
        .create_date
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        None,
        stop.stop_order_id.as_str().into(),
        nautilus_stop_order_side(stop.direction),
        nautilus_stop_order_type_with_context(&stop, managed_order_type),
        TimeInForce::Gtc,
        nautilus_stop_order_status(stop.status),
        lots_to_quantity(stop.lots_requested, lot_size)?,
        Quantity::from(0),
        ts_event,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = stop
        .price
        .as_ref()
        .map(price_from_point_valued_money_value)
        .transpose()?
    {
        report = report.with_price(price);
    }
    if let Some(trigger_price) = stop
        .stop_price
        .as_ref()
        .map(price_from_point_valued_money_value)
        .transpose()?
    {
        if matches!(
            report.order_type,
            OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
        ) {
            report = report.with_activation_price(trigger_price);
        } else {
            report = report.with_trigger_price(trigger_price);
        }
    }
    report = apply_trailing_params(report, trailing_params_from_stop(&stop)?)?;
    Ok(with_default_stop_trigger_type(report))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn activated_stop_child_status_report_with_context(
    account_id: AccountId,
    stop: &StopOrder,
    state: &OrderState,
    ts_init: UnixNanos,
    lot_size: u32,
    client_order_id: Option<&str>,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
    managed_order_type: Option<crate::common::TbankOrderType>,
) -> anyhow::Result<OrderStatusReport> {
    let mut report = stop_order_status_report_with_context(
        account_id,
        stop.clone(),
        ts_init,
        lot_size,
        instruments,
        managed_order_type,
    )?;
    report.client_order_id = client_order_id.map(Into::into);
    report.order_status = activated_stop_child_status(nautilus_order_status(
        state.execution_report_status,
        state.lots_requested,
        state.lots_executed,
    ));
    report.filled_qty = lots_to_quantity(state.lots_executed, lot_size)?;
    if let Some(ts_last) = state
        .order_date
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
    {
        report.ts_last = ts_last;
    }
    let instrument_id_string = report.instrument_id.to_string();
    let metadata = cached_instrument_metadata(
        instruments,
        &stop.instrument_uid,
        &stop.figi,
        &instrument_id_string,
    );
    if let Some(avg_px) =
        average_price_from_order_stages(&state.stages, state.lots_executed, metadata.as_ref())?
    {
        report = report.with_avg_px(avg_px);
    } else if let Some(value) = state.executed_order_price.as_ref()
        && let Some(avg_px) =
            average_price_from_executed_order_price(value, state.lots_executed, metadata.as_ref())?
    {
        report = report.with_avg_px(avg_px);
    }
    Ok(with_default_stop_trigger_type(report))
}

pub(super) fn activated_stop_child_status(status: OrderStatus) -> OrderStatus {
    if status == OrderStatus::Accepted {
        OrderStatus::Triggered
    } else {
        status
    }
}

pub(super) fn fill_reports_from_cursor_operation_with_instruments(
    account_id: AccountId,
    item: &OperationItem,
    ts_init: UnixNanos,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> Vec<anyhow::Result<FillReport>> {
    let Some(side) = fill_side_from_operation_type(item.r#type) else {
        return Vec::new();
    };
    let instrument_id = match instrument_id_from_ticker_class_or_cached_identity(
        &item.ticker,
        &item.class_code,
        &item.instrument_uid,
        &item.figi,
        instruments,
    ) {
        Ok(value) => value,
        Err(error) => return vec![Err(error)],
    };
    let commission = match item.commission.as_ref().map(money_from_value).transpose() {
        Ok(Some(value)) => value,
        Ok(None) => Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        Err(error) => return vec![Err(error)],
    };
    let trades = item
        .trades_info
        .as_ref()
        .map(|info| info.trades.as_slice())
        .unwrap_or_default();
    trades
        .iter()
        .map(|trade| {
            let ts_event = trade
                .date
                .as_ref()
                .map(timestamp_to_unix_nanos)
                .transpose()?
                .unwrap_or(ts_init);
            Ok(FillReport::new(
                account_id,
                instrument_id,
                item.id.as_str().into(),
                trade.num.as_str().into(),
                side,
                Quantity::from_decimal(Decimal::from(trade.quantity))?,
                price_from_point_valued_money_value(
                    trade
                        .price
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("missing trade price"))?,
                )?,
                commission,
                LiquiditySide::NoLiquiditySide,
                None,
                // T-Bank exposes a stable position_uid for the instrument, but this
                // client uses Nautilus NETTING semantics. Supplying that broker
                // identity as venue_position_id makes Nautilus take its HEDGING
                // reconciliation path and look for a cache position with the broker
                // UUID instead of the netting position ID derived from the fills.
                None,
                ts_event,
                ts_init,
                Some(UUID4::new()),
            ))
        })
        .collect()
}

pub(super) fn fill_side_from_operation_type(operation_type: i32) -> Option<OrderSide> {
    match TbankOperationType::try_from(operation_type).ok() {
        Some(
            TbankOperationType::Buy | TbankOperationType::BuyCard | TbankOperationType::BuyMargin,
        ) => Some(OrderSide::Buy),
        Some(
            TbankOperationType::Sell
            | TbankOperationType::SellCard
            | TbankOperationType::SellMargin,
        ) => Some(OrderSide::Sell),
        Some(_) | None => None,
    }
}

pub(super) fn fill_report_from_order_trade(
    order: &crate::grpc::generated::OrderTrades,
    trade: &crate::grpc::generated::OrderTrade,
    ts_init: UnixNanos,
    instruments: &Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
) -> anyhow::Result<FillReport> {
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        "",
        "",
        &order.instrument_uid,
        &order.figi,
        Some(instruments),
    )?;
    let ts_event = trade
        .date_time
        .as_ref()
        .map(timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    Ok(FillReport::new(
        nautilus_account_id(&order.account_id),
        instrument_id,
        order.order_id.as_str().into(),
        trade.trade_id.as_str().into(),
        nautilus_order_side(order.direction),
        Quantity::from_decimal(Decimal::from(trade.quantity))?,
        quotation_price_from_points_required(trade.price.as_ref())?,
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB"))?,
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    ))
}

fn average_price_from_stream_trades(
    state: &order_state_stream_response::OrderState,
) -> anyhow::Result<Option<Decimal>> {
    if state.lots_executed <= 0 {
        return Ok(None);
    }
    let mut total_quantity = Decimal::ZERO;
    let mut total_value = Decimal::ZERO;
    let mut missing_price = false;
    let mut invalid_quantity = false;
    for trade in &state.trades {
        if trade.quantity == 0 {
            continue;
        }
        if trade.quantity < 0 {
            invalid_quantity = true;
            continue;
        }
        let Some(price) = trade.price.as_ref() else {
            missing_price = true;
            continue;
        };
        let quantity = Decimal::from(trade.quantity);
        total_quantity += quantity;
        total_value += crate::common::decimal::quotation_to_decimal(price) * quantity;
    }
    let expected_quantity =
        Decimal::from(state.lots_executed) * Decimal::from(i64::from(state.lot_size.max(1)));
    if missing_price || invalid_quantity || total_quantity != expected_quantity {
        return Ok(None);
    }
    bounded_nautilus_price_decimal(total_value / total_quantity).map(Some)
}

pub(super) fn fill_reports_from_order_state_stream(
    state: &order_state_stream_response::OrderState,
    venue_order_id: &str,
    client_order_id: Option<&str>,
    ts_init: UnixNanos,
    instruments: &Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
) -> Vec<anyhow::Result<FillReport>> {
    let instrument_id = match instrument_id_from_ticker_class_or_cached_identity(
        &state.ticker,
        &state.class_code,
        &state.instrument_uid,
        "",
        Some(instruments),
    ) {
        Ok(value) => value,
        Err(error) => return vec![Err(error)],
    };
    let account_id = nautilus_account_id(&state.account_id);
    let client_order_id = client_order_id.and_then(nonempty_client_order_id);
    state
        .trades
        .iter()
        .map(|trade| {
            let ts_event = trade
                .date_time
                .as_ref()
                .map(timestamp_to_unix_nanos)
                .transpose()?
                .unwrap_or(ts_init);
            Ok(FillReport::new(
                account_id,
                instrument_id,
                venue_order_id.into(),
                trade.trade_id.as_str().into(),
                nautilus_order_side(state.direction),
                Quantity::from_decimal(Decimal::from(trade.quantity))?,
                quotation_price_from_points_required(trade.price.as_ref())?,
                Money::from_decimal(Decimal::ZERO, Currency::from("RUB"))?,
                LiquiditySide::NoLiquiditySide,
                client_order_id,
                None,
                ts_event,
                ts_init,
                Some(UUID4::new()),
            ))
        })
        .collect()
}

pub(super) fn project_managed_trade_fill_report(
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    fill_projection: &Arc<Mutex<TbankFillProjection>>,
    report: FillReport,
) -> anyhow::Result<Option<FillReport>> {
    let report = canonicalize_managed_trade_fill_report(broker_order_index, report);
    let mut projection = fill_projection.lock().expect("fill_projection lock");
    project_trade_fill_report_locked(&mut projection, report)
}

pub(super) fn canonicalize_managed_trade_fill_report(
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    mut report: FillReport,
) -> FillReport {
    let venue_order_id = report.venue_order_id.to_string();
    let index = broker_order_index.lock().expect("broker_order_index lock");
    if let Some((canonical_venue_order_id, client_order_id)) =
        index.canonical_venue_order_identity(venue_order_id.as_str())
    {
        report.venue_order_id = canonical_venue_order_id.as_str().into();
        report.client_order_id = client_order_id.map(Into::into);
    }
    report
}

pub(super) fn position_status_report_from_portfolio_with_instruments(
    account_id: AccountId,
    position: &PortfolioPosition,
    ts_init: UnixNanos,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> Option<PositionStatusReport> {
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        &position.ticker,
        &position.class_code,
        &position.instrument_uid,
        &position.figi,
        instruments,
    )
    .ok()?;
    let quantity = position
        .quantity
        .as_ref()
        .map(crate::common::decimal::quotation_to_decimal)
        .unwrap_or(Decimal::ZERO);
    let side = position_side(quantity);
    let abs_qty = Quantity::from_decimal(quantity.abs()).ok()?;
    let instrument_id_string = instrument_id.to_string();
    let metadata = cached_instrument_metadata(
        instruments,
        &position.instrument_uid,
        &position.figi,
        &instrument_id_string,
    );
    let avg_px = if metadata
        .as_ref()
        .is_some_and(|metadata| metadata.price_in_points)
    {
        #[allow(deprecated)]
        position
            .average_position_price_pt
            .as_ref()
            .map(crate::common::decimal::quotation_to_decimal)
            .or_else(|| {
                position.average_position_price.as_ref().and_then(|value| {
                    average_currency_price_to_nautilus_decimal(
                        crate::common::decimal::money_value_to_decimal(value),
                        metadata.as_ref(),
                    )
                    .ok()
                })
            })
    } else {
        position
            .average_position_price
            .as_ref()
            .map(crate::common::decimal::money_value_to_decimal)
    };
    Some(PositionStatusReport::new(
        account_id,
        instrument_id,
        side,
        abs_qty,
        ts_init,
        ts_init,
        Some(UUID4::new()),
        None,
        avg_px,
    ))
}

pub(super) fn position_status_report_from_security_with_instruments(
    account_id: AccountId,
    position: &PositionsSecurities,
    ts_init: UnixNanos,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> Option<PositionStatusReport> {
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        &position.ticker,
        &position.class_code,
        &position.instrument_uid,
        &position.figi,
        instruments,
    )
    .ok()?;
    let quantity = Decimal::from(position.balance);
    let side = position_side(quantity);
    let abs_qty = Quantity::from_decimal(quantity.abs()).ok()?;
    Some(PositionStatusReport::new(
        account_id,
        instrument_id,
        side,
        abs_qty,
        ts_init,
        ts_init,
        Some(UUID4::new()),
        None,
        None,
    ))
}

pub(super) fn position_status_report_from_future_with_instruments(
    account_id: AccountId,
    position: &PositionsFutures,
    ts_init: UnixNanos,
    instruments: &Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
) -> Option<PositionStatusReport> {
    let instrument_id = instrument_id_from_ticker_class_or_cached_identity(
        &position.ticker,
        &position.class_code,
        &position.instrument_uid,
        &position.figi,
        Some(instruments),
    )
    .ok()?;
    let quantity = Decimal::from(position.balance);
    let abs_qty = Quantity::from_decimal(quantity.abs()).ok()?;
    Some(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side(quantity),
        abs_qty,
        ts_init,
        ts_init,
        Some(UUID4::new()),
        None,
        None,
    ))
}

pub(super) fn position_side(quantity: Decimal) -> PositionSideSpecified {
    if quantity > Decimal::ZERO {
        PositionSideSpecified::Long
    } else if quantity < Decimal::ZERO {
        PositionSideSpecified::Short
    } else {
        PositionSideSpecified::Flat
    }
}

pub(super) fn account_state_from_portfolio(
    portfolio: &PortfolioResponse,
) -> anyhow::Result<Option<nautilus_model::events::AccountState>> {
    let Some(total) = portfolio.total_amount_portfolio.as_ref() else {
        return Ok(None);
    };
    let currency = crate::common::currency::currency_from_code(&total.currency)?;
    let total_amount = crate::common::decimal::money_value_to_decimal(total);
    let free_amount = portfolio
        .total_amount_currencies
        .as_ref()
        .filter(|cash| cash.currency.eq_ignore_ascii_case(total.currency.as_str()))
        .map(crate::common::decimal::money_value_to_decimal)
        .unwrap_or(total_amount);
    Ok(Some(nautilus_model::events::AccountState::new(
        nautilus_account_id(&portfolio.account_id),
        AccountType::Margin,
        vec![AccountBalance::from_total_and_free(
            total_amount,
            free_amount,
            currency,
        )?],
        Vec::new(),
        true,
        UUID4::new(),
        current_unix_nanos(),
        current_unix_nanos(),
        Some(currency),
    )))
}

pub(super) fn metadata_from_instrument(
    instrument: &InstrumentAny,
) -> Option<TbankInstrumentMetadata> {
    TbankInstrumentMetadata::from_instrument(instrument)
}

#[cfg(test)]
pub(super) fn instrument_id_from_ticker_class_or_identity(
    ticker: &str,
    class_code: &str,
    instrument_uid: &str,
    figi: &str,
) -> anyhow::Result<nautilus_model::identifiers::InstrumentId> {
    let value = if !ticker.is_empty() && !class_code.is_empty() {
        crate::common::ids::instrument_id_from_ticker_class(ticker, class_code)
    } else if !instrument_uid.is_empty() {
        instrument_uid.to_string()
    } else if !figi.is_empty() {
        figi.to_string()
    } else {
        anyhow::bail!("instrument event has no complete identity");
    };
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid instrument id {value}: {error}"))
}

pub(super) fn instrument_id_from_ticker_class_or_cached_identity(
    ticker: &str,
    class_code: &str,
    instrument_uid: &str,
    figi: &str,
    instruments: Option<&Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>>,
) -> anyhow::Result<nautilus_model::identifiers::InstrumentId> {
    let has_authoritative_identity = !instrument_uid.is_empty() || !figi.is_empty();
    if let Some(metadata) = instruments.and_then(|instruments| {
        let instruments = instruments.lock().expect("instruments lock");
        if has_authoritative_identity {
            instruments
                .values()
                .find(|metadata| {
                    metadata_matches_event_identity(
                        metadata,
                        instrument_uid,
                        figi,
                        ticker,
                        class_code,
                    )
                })
                .cloned()
        } else if !ticker.is_empty() && !class_code.is_empty() {
            let mut matches = instruments.values().filter(|metadata| {
                metadata.ticker.eq_ignore_ascii_case(ticker)
                    && metadata.class_code.eq_ignore_ascii_case(class_code)
            });
            let first = matches.next().cloned();
            first.filter(|_| matches.next().is_none())
        } else {
            None
        }
    }) {
        return metadata
            .instrument_id
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid cached instrument id: {error}"));
    }
    #[cfg(test)]
    if instruments.is_none() {
        return instrument_id_from_ticker_class_or_identity(
            ticker,
            class_code,
            instrument_uid,
            figi,
        );
    }
    let identity = super::instrument_metadata_identity(instrument_uid, figi, ticker, class_code);
    Err(anyhow::anyhow!(
        "instrument metadata is unavailable for {identity}; refusing to infer venue"
    ))
}

pub(super) fn nonempty_client_order_id(
    value: &str,
) -> Option<nautilus_model::identifiers::ClientOrderId> {
    (!value.is_empty()).then(|| value.into())
}

pub(crate) fn nautilus_account_id(account_id: &str) -> AccountId {
    let account_id = account_id.trim();
    if account_id.contains('-') {
        AccountId::from(account_id)
    } else if account_id.is_empty() {
        AccountId::from("TBANK-UNKNOWN")
    } else {
        AccountId::from(format!("TBANK-{account_id}").as_str())
    }
}

pub(super) fn lots_to_quantity(lots: i64, lot_size: u32) -> anyhow::Result<Quantity> {
    Quantity::from_decimal(Decimal::from(lots) * Decimal::from(lot_size))
        .map_err(anyhow::Error::from)
}

pub(super) fn timestamp_to_unix_nanos(
    timestamp: &prost_types::Timestamp,
) -> anyhow::Result<UnixNanos> {
    let seconds = i128::from(timestamp.seconds);
    let nanos = i128::from(timestamp.nanos);
    let value = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos))
        .ok_or_else(|| anyhow::anyhow!("timestamp out of range"))?;
    let value = u64::try_from(value).map_err(|_| anyhow::anyhow!("negative timestamp"))?;
    Ok(UnixNanos::from(value))
}

pub(super) fn current_unix_nanos() -> UnixNanos {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    UnixNanos::from(now.as_secs().saturating_mul(1_000_000_000) + u64::from(now.subsec_nanos()))
}

/// Parses a T-Bank response price whose unit is already the Nautilus price
/// point. Futures use this representation in the point-valued stream,
/// stop-order, order-trade, and cursor-operation fields even though the
/// protobuf type is still `MoneyValue`.
pub(super) fn price_from_point_valued_money_value(value: &MoneyValue) -> anyhow::Result<Price> {
    Price::from_decimal(crate::common::decimal::money_value_to_decimal(value))
        .map_err(anyhow::Error::from)
}

pub(super) fn price_from_money_value_for_instrument(
    value: &MoneyValue,
    metadata: Option<&TbankInstrumentMetadata>,
) -> anyhow::Result<Price> {
    let value = crate::common::decimal::money_value_to_decimal(value);
    let points = currency_price_to_nautilus_decimal(value, metadata)?;
    Price::from_decimal(points).map_err(anyhow::Error::from)
}

pub(super) fn quotation_price_from_points_required(
    value: Option<&Quotation>,
) -> anyhow::Result<Price> {
    let value = value.ok_or_else(|| anyhow::anyhow!("missing price"))?;
    Price::from_decimal(crate::common::decimal::quotation_to_decimal(value))
        .map_err(anyhow::Error::from)
}

pub(super) fn money_from_value(value: &MoneyValue) -> anyhow::Result<Money> {
    let currency = crate::common::currency::currency_from_code(&value.currency)?;
    Money::from_decimal(
        crate::common::decimal::money_value_to_decimal(value),
        currency,
    )
    .map_err(anyhow::Error::from)
}

pub(super) fn commission_from_money_value(
    value: Option<&MoneyValue>,
) -> anyhow::Result<Option<Money>> {
    value.map(money_from_value).transpose()
}
