//! Order submission pipeline and failure classification.

use super::*;
use crate::grpc::generated::{
    StopOrderDirection, StopOrderType, post_stop_order_request, stop_order,
};
use uuid::Uuid;

const TBANK_SUBMIT_TRADE_NAMESPACE: Uuid =
    Uuid::from_u128(0xb8bb_8370_8e0d_5798_99f4_9815_594a_9f46);

pub(super) fn trailing_stop_params(
    init: &nautilus_model::events::OrderInitialized,
) -> anyhow::Result<Option<crate::execution::TbankTrailingStopParams>> {
    if !matches!(
        init.order_type,
        OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
    ) {
        return Ok(None);
    }
    if init.trigger_price.is_some() {
        anyhow::bail!(
            "T-Bank native trailing stops use activation_price, not an explicit trigger_price"
        );
    }
    let requested_trigger_type = init.trigger_type;
    if !matches!(
        requested_trigger_type,
        None | Some(TriggerType::Default | TriggerType::LastPrice)
    ) {
        anyhow::bail!(
            "T-Bank trailing stops do not support trigger source {requested_trigger_type:?}; use DEFAULT or LAST_PRICE"
        );
    }
    let trailing_offset = init
        .trailing_offset
        .ok_or_else(|| anyhow::anyhow!("trailing_offset is required"))?;
    let trailing_offset_type = init
        .trailing_offset_type
        .ok_or_else(|| anyhow::anyhow!("trailing_offset_type is required"))?;
    match init.order_type {
        OrderType::TrailingStopLimit if init.limit_offset.is_none() => {
            anyhow::bail!("TrailingStopLimit requires limit_offset")
        }
        OrderType::TrailingStopMarket if init.limit_offset.is_some() => {
            anyhow::bail!("TrailingStopMarket does not accept limit_offset")
        }
        _ => {}
    }
    Ok(Some(crate::execution::TbankTrailingStopParams {
        activation_price: init.activation_price.map(|price| price.as_decimal()),
        trailing_offset,
        trailing_offset_type,
        limit_offset: init.limit_offset,
        trigger_type: Some(TriggerType::LastPrice),
    }))
}

pub(super) fn synthetic_fill_trade_id(
    source: &str,
    order_id: &str,
    cumulative_quantity: Decimal,
) -> String {
    Uuid::new_v5(
        &TBANK_SUBMIT_TRADE_NAMESPACE,
        format!("{source}:{order_id}:{cumulative_quantity}").as_bytes(),
    )
    .to_string()
}

pub(super) enum SubmitPipelineOutcome {
    Reports(Vec<ExecutionReport>),
    Rejected(String),
}

pub(super) struct PreparedNautilusOrder {
    pub(super) cmd: nautilus_common::messages::execution::SubmitOrder,
    pub(super) order: TbankSubmitOrder,
    pub(super) metadata: TbankInstrumentMetadata,
}

pub(super) async fn prepare_nautilus_order(
    client: &mut TbankExecutionRuntime,
    cmd: nautilus_common::messages::execution::SubmitOrder,
) -> anyhow::Result<PreparedNautilusOrder> {
    client.config.ensure_submit_allowed()?;
    let account_id = client.config.resolve_account_id()?;
    let instrument_id = order_initialized_instrument_id(&cmd);
    let mut cmd = cmd;
    cmd.instrument_id = instrument_id;
    cmd.order_init.instrument_id = instrument_id;
    let order = TbankSubmitOrder {
        instrument_id: instrument_id.to_string(),
        client_order_id: cmd.client_order_id.to_string(),
        broker_request_id: client
            .get_or_allocate_broker_request_id(cmd.client_order_id.as_str())?,
        side: tbank_side(cmd.order_init.order_side)?,
        order_type: tbank_order_type(cmd.order_init.order_type)?,
        time_in_force: cmd.order_init.time_in_force,
        quantity_units: cmd.order_init.quantity.as_decimal(),
        limit_price: cmd.order_init.price.map(|price| price.as_decimal()),
        trigger_price: cmd.order_init.trigger_price.map(|price| price.as_decimal()),
        trailing: trailing_stop_params(&cmd.order_init)?,
        confirm_margin_trade: confirm_margin_trade_for_submit(
            client.config.confirm_margin_trade_default,
            cmd.params.as_ref(),
        ),
    };
    let metadata = client
        .load_instrument_metadata(&order.instrument_id)
        .await?;
    if !metadata.api_trade_available {
        anyhow::bail!(
            "T-Bank API trading is unavailable for {}",
            metadata.instrument_id
        );
    }
    if !metadata.required_tests.is_empty() {
        anyhow::bail!(
            "T-Bank instrument tests are required for {}: {}",
            metadata.instrument_id,
            metadata.required_tests.join(", ")
        );
    }
    match order.side {
        crate::common::TbankOrderSide::Buy if !metadata.buy_available => {
            anyhow::bail!(
                "T-Bank buying is unavailable for {}",
                metadata.instrument_id
            );
        }
        crate::common::TbankOrderSide::Sell if !metadata.sell_available => {
            anyhow::bail!(
                "T-Bank selling is unavailable for {}",
                metadata.instrument_id
            );
        }
        _ => {}
    }
    match order.service(client.config.environment) {
        TbankExecutionService::LiveOrders => {
            build_post_order_request(&order, &account_id, &metadata)?;
        }
        TbankExecutionService::LiveStopOrders => {
            build_post_stop_order_request(&order, &account_id, &metadata)?;
        }
        TbankExecutionService::Sandbox => match order.order_type {
            crate::common::TbankOrderType::Market | crate::common::TbankOrderType::Limit => {
                build_post_order_request(&order, &account_id, &metadata)?;
            }
            crate::common::TbankOrderType::StopMarket
            | crate::common::TbankOrderType::MarketIfTouched
            | crate::common::TbankOrderType::TrailingStopMarket
            | crate::common::TbankOrderType::TrailingStopLimit => {
                build_post_stop_order_request(&order, &account_id, &metadata)?;
            }
        },
    }
    Ok(PreparedNautilusOrder {
        cmd,
        order,
        metadata,
    })
}

pub(super) async fn submit_prepared_nautilus_order(
    client: &mut TbankExecutionRuntime,
    prepared: PreparedNautilusOrder,
    emitter: ExecutionEventEmitter,
) -> anyhow::Result<()> {
    let ts_init = current_unix_nanos();
    let order: nautilus_model::orders::OrderAny = prepared.cmd.order_init.clone().try_into()?;
    let reports = match submit_prepared_nautilus_order_reports_with_recovery(
        client,
        prepared,
        ts_init,
        Some(emitter.clone()),
    )
    .await?
    {
        SubmitPipelineOutcome::Reports(reports) => reports,
        SubmitPipelineOutcome::Rejected(reason) => {
            emitter.emit_order_rejected(&order, &reason, ts_init, false);
            return Ok(());
        }
    };
    emit_submit_reports(client, &emitter, &order, reports);
    Ok(())
}

fn emit_submit_reports(
    client: &mut TbankExecutionRuntime,
    emitter: &ExecutionEventEmitter,
    order: &nautilus_model::orders::OrderAny,
    reports: Vec<ExecutionReport>,
) {
    for report in reports {
        match report {
            ExecutionReport::Order(order_report)
                if order_report.order_status == OrderStatus::Rejected =>
            {
                let reason = order_report
                    .cancel_reason
                    .as_deref()
                    .unwrap_or("T-Bank rejected order");
                emitter.emit_order_rejected(order, reason, order_report.ts_last, false);
            }
            ExecutionReport::Order(order) => {
                if let Some(order) =
                    project_order_status_report(&client.order_status_projection, *order)
                {
                    emitter.send_order_status_report(order);
                }
            }
            ExecutionReport::OrderWithFills(order, fills) => {
                if let Some(order) =
                    project_order_status_report(&client.order_status_projection, *order)
                {
                    emitter.send_order_with_fills(order, fills);
                } else {
                    for fill in fills {
                        emitter.send_fill_report(fill);
                    }
                }
            }
            report => emitter.send_execution_report(report),
        }
    }
}

#[cfg(test)]
pub(super) async fn submit_nautilus_order_reports(
    client: &mut TbankExecutionRuntime,
    cmd: &nautilus_common::messages::execution::SubmitOrder,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<ExecutionReport>> {
    reports_or_rejection_error(
        submit_nautilus_order_reports_with_recovery(client, cmd, ts_init, None).await?,
    )
}

#[cfg(test)]
fn reports_or_rejection_error(
    outcome: SubmitPipelineOutcome,
) -> anyhow::Result<Vec<ExecutionReport>> {
    match outcome {
        SubmitPipelineOutcome::Reports(reports) => Ok(reports),
        SubmitPipelineOutcome::Rejected(reason) => Err(anyhow::anyhow!(reason)),
    }
}

#[cfg(test)]
pub(super) async fn submit_nautilus_order_reports_with_recovery(
    client: &mut TbankExecutionRuntime,
    cmd: &nautilus_common::messages::execution::SubmitOrder,
    ts_init: UnixNanos,
    recovery_emitter: Option<ExecutionEventEmitter>,
) -> anyhow::Result<SubmitPipelineOutcome> {
    match prepare_nautilus_order(client, cmd.clone()).await {
        Ok(prepared) => {
            submit_prepared_nautilus_order_reports_with_recovery(
                client,
                prepared,
                ts_init,
                recovery_emitter,
            )
            .await
        }
        Err(error) => {
            tracing::error!(%error, client_order_id = %cmd.client_order_id, "rejecting Nautilus order before broker submit");
            Ok(SubmitPipelineOutcome::Rejected(error.to_string()))
        }
    }
}

async fn submit_prepared_nautilus_order_reports_with_recovery(
    client: &mut TbankExecutionRuntime,
    prepared: PreparedNautilusOrder,
    ts_init: UnixNanos,
    recovery_emitter: Option<ExecutionEventEmitter>,
) -> anyhow::Result<SubmitPipelineOutcome> {
    let PreparedNautilusOrder {
        cmd,
        order,
        metadata,
    } = prepared;

    tracing::info!(
        instrument_id = %order.instrument_id,
        client_order_id = %order.client_order_id,
        side = ?order.side,
        order_type = ?order.order_type,
        quantity_units = %order.quantity_units,
        confirm_margin_trade = order.confirm_margin_trade,
        "submitting Nautilus order to T-Bank"
    );
    let response = match client.submit_order(&order, &metadata).await {
        Ok(response) => response,
        Err(error) => {
            match classify_submit_failure(&error) {
                SubmitFailureKind::LocalRejected | SubmitFailureKind::BrokerRejected => {
                    tracing::error!(%error, client_order_id = %cmd.client_order_id, "T-Bank rejected Nautilus order");
                    return Ok(SubmitPipelineOutcome::Rejected(error.to_string()));
                }
                SubmitFailureKind::OutcomeUnknown => {}
            }
            tracing::warn!(
                %error,
                client_order_id = %cmd.client_order_id,
                "T-Bank submit response failed; running broker reconciliation"
            );
            let unresolved_reason = match client
                .reconcile_submit_outcome(&order, &metadata, ts_init)
                .await
            {
                Ok(Some(reconciled)) => {
                    client.mark_pending_submit_report(&reconciled.order_report);
                    return Ok(SubmitPipelineOutcome::Reports(
                        order_status_execution_reports(
                            reconciled.order_report,
                            reconciled.fill_reports,
                        ),
                    ));
                }
                Ok(None) => format!(
                    "T-Bank submit response failed and reconciliation found no broker state: {error}"
                ),
                Err(reconciliation_error) => format!(
                    "T-Bank submit response failed and immediate reconciliation also failed: {reconciliation_error}"
                ),
            };
            tracing::warn!(
                client_order_id = %cmd.client_order_id,
                reason = %unresolved_reason,
                "T-Bank submit outcome remains unresolved after immediate reconciliation"
            );
            client.mark_pending_submit_stage(
                cmd.client_order_id.as_str(),
                TbankPendingSubmitStage::Unknown,
                Some(ts_init),
            );
            if let Some(emitter) = recovery_emitter {
                client.spawn_submit_outcome_recovery(order, metadata, ts_init, emitter);
            }
            return Ok(SubmitPipelineOutcome::Reports(Vec::new()));
        }
    };
    match &response {
        TbankSubmitResponse::Order(response) => tracing::info!(
            instrument_id = %order.instrument_id,
            client_order_id = %order.client_order_id,
            execution_report_status = response.execution_report_status,
            lots_requested = response.lots_requested,
            lots_executed = response.lots_executed,
            "received T-Bank order response"
        ),
        TbankSubmitResponse::StopOrder(_) => tracing::info!(
            instrument_id = %order.instrument_id,
            client_order_id = %order.client_order_id,
            "received T-Bank stop-order response"
        ),
    }
    submit_response_execution_reports(client, &cmd, &metadata, &response, ts_init)
        .map(SubmitPipelineOutcome::Reports)
}

pub(super) fn order_status_execution_reports(
    order_report: OrderStatusReport,
    fill_reports: Vec<FillReport>,
) -> Vec<ExecutionReport> {
    if fill_reports.is_empty() {
        vec![ExecutionReport::Order(Box::new(order_report))]
    } else {
        vec![ExecutionReport::OrderWithFills(
            Box::new(order_report),
            fill_reports,
        )]
    }
}

pub(super) fn submit_response_execution_reports(
    client: &mut TbankExecutionRuntime,
    cmd: &nautilus_common::messages::execution::SubmitOrder,
    metadata: &TbankInstrumentMetadata,
    response: &TbankSubmitResponse,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<ExecutionReport>> {
    let (order_report, fill_reports) = match response {
        TbankSubmitResponse::Order(response) => {
            let order_report = order_status_report_from_post_order_response(
                client.account_id(),
                cmd,
                response,
                metadata,
                ts_init,
            )?;
            let trade_id = synthetic_fill_trade_id(
                "submit",
                response.order_id.as_str(),
                order_report.filled_qty.as_decimal(),
            );
            let fill_reports = client
                .project_order_status_fill_report(
                    &order_report,
                    response.order_id.as_str(),
                    trade_id.as_str(),
                    ts_init,
                    Some(cmd.client_order_id.as_str()),
                    commission_from_money_value(response.executed_commission.as_ref())?,
                )?
                .into_iter()
                .collect();
            (order_report, fill_reports)
        }
        TbankSubmitResponse::StopOrder(response) => (
            order_status_report_from_post_stop_order_response(
                client.account_id(),
                cmd,
                response,
                ts_init,
            ),
            Vec::new(),
        ),
    };
    client.mark_pending_submit_report(&order_report);
    Ok(order_status_execution_reports(order_report, fill_reports))
}

pub(super) fn confirm_margin_trade_for_submit(default: bool, params: Option<&Params>) -> bool {
    params
        .and_then(|params| params.get_bool(TBANK_CONFIRM_MARGIN_TRADE_PARAM))
        .unwrap_or(default)
}

pub(super) fn broker_order_route_for_submit(
    order: &TbankSubmitOrder,
    service: TbankExecutionService,
) -> TbankBrokerOrderRoute {
    match service {
        TbankExecutionService::LiveStopOrders => TbankBrokerOrderRoute::StopOrder,
        TbankExecutionService::Sandbox
            if matches!(
                order.order_type,
                crate::common::TbankOrderType::StopMarket
                    | crate::common::TbankOrderType::MarketIfTouched
                    | crate::common::TbankOrderType::TrailingStopMarket
                    | crate::common::TbankOrderType::TrailingStopLimit
            ) =>
        {
            TbankBrokerOrderRoute::StopOrder
        }
        TbankExecutionService::LiveOrders | TbankExecutionService::Sandbox => {
            TbankBrokerOrderRoute::RegularOrder
        }
    }
}

pub(super) fn stop_order_matches_submit(
    order: &TbankSubmitOrder,
    metadata: &TbankInstrumentMetadata,
    stop: &StopOrder,
) -> bool {
    let Ok(expected) = build_post_stop_order_request(order, "", metadata) else {
        return false;
    };
    if stop.stop_order_id.is_empty()
        || stop.instrument_uid != expected.instrument_id
        || stop.ticker != metadata.ticker
        || stop.class_code != metadata.class_code
        || stop.lots_requested != expected.quantity
        || stop.direction != expected.direction
        || stop.order_type != expected.stop_order_type
        || stop.exchange_order_type != expected.exchange_order_type
        || stop.take_profit_type != expected.take_profit_type
        || !money_value_matches(expected.price.as_ref(), stop.price.as_ref())
        || !money_value_matches(expected.stop_price.as_ref(), stop.stop_price.as_ref())
    {
        return false;
    }
    trailing_data_matches(expected.trailing_data.as_ref(), stop.trailing_data.as_ref())
}

fn money_value_matches(expected: Option<&Quotation>, actual: Option<&MoneyValue>) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => {
            crate::common::decimal::quotation_to_decimal(expected)
                == crate::common::decimal::money_value_to_decimal(actual)
        }
        (None, None) => true,
        _ => false,
    }
}

fn trailing_data_matches(
    expected: Option<&post_stop_order_request::TrailingData>,
    actual: Option<&stop_order::TrailingData>,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            expected.indent_type == actual.indent_type
                && expected.spread_type == actual.spread_type
                && quotation_matches(expected.indent.as_ref(), actual.indent.as_ref())
                && quotation_matches(expected.spread.as_ref(), actual.spread.as_ref())
        }
        _ => false,
    }
}

fn quotation_matches(expected: Option<&Quotation>, actual: Option<&Quotation>) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => {
            crate::common::decimal::quotation_to_decimal(expected)
                == crate::common::decimal::quotation_to_decimal(actual)
        }
        (None, None) => true,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SubmitFailureKind {
    LocalRejected,
    BrokerRejected,
    OutcomeUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CancelFailureKind {
    LocalFailure,
    BrokerRejected,
    OutcomeUnknown,
}

pub(super) fn classify_cancel_failure(error: &TbankAdapterError) -> CancelFailureKind {
    match error {
        TbankAdapterError::PermissionDenied(_) => CancelFailureKind::BrokerRejected,
        TbankAdapterError::GrpcStatus { code, .. } => match classify_submit_grpc_status(*code) {
            SubmitFailureKind::BrokerRejected => CancelFailureKind::BrokerRejected,
            SubmitFailureKind::OutcomeUnknown => CancelFailureKind::OutcomeUnknown,
            SubmitFailureKind::LocalRejected => unreachable!("gRPC status is never local"),
        },
        TbankAdapterError::RateLimited(_)
        | TbankAdapterError::InstrumentNotFound(_)
        | TbankAdapterError::InstrumentMetadataUnresolved(_)
        | TbankAdapterError::FuturesMarginUnresolved(_)
        | TbankAdapterError::SubmitOutcomeUnknown(_)
        | TbankAdapterError::ReconnectFailed(_) => CancelFailureKind::OutcomeUnknown,
        TbankAdapterError::ConfigError(_)
        | TbankAdapterError::MissingToken
        | TbankAdapterError::MissingAccountId
        | TbankAdapterError::InvalidEndpoint
        | TbankAdapterError::UnsupportedInstrument(_)
        | TbankAdapterError::InstrumentOutOfScope(_)
        | TbankAdapterError::UnsupportedOrderType(_)
        | TbankAdapterError::UnsupportedTimeInForce(_)
        | TbankAdapterError::InvalidQuantity(_)
        | TbankAdapterError::InvalidPrice(_)
        | TbankAdapterError::QuantityNotMultipleOfLot { .. }
        | TbankAdapterError::PriceNotMultipleOfTick { .. }
        | TbankAdapterError::ConversionError(_)
        | TbankAdapterError::InvalidInstrumentIdentity(_)
        | TbankAdapterError::BrokerOrderIdentityUnresolved(_) => CancelFailureKind::LocalFailure,
    }
}

pub(super) fn classify_submit_failure(error: &TbankAdapterError) -> SubmitFailureKind {
    match error {
        TbankAdapterError::ConfigError(_)
        | TbankAdapterError::MissingToken
        | TbankAdapterError::MissingAccountId
        | TbankAdapterError::InvalidEndpoint
        | TbankAdapterError::UnsupportedInstrument(_)
        | TbankAdapterError::InstrumentOutOfScope(_)
        | TbankAdapterError::UnsupportedOrderType(_)
        | TbankAdapterError::UnsupportedTimeInForce(_)
        | TbankAdapterError::InvalidQuantity(_)
        | TbankAdapterError::InvalidPrice(_)
        | TbankAdapterError::QuantityNotMultipleOfLot { .. }
        | TbankAdapterError::PriceNotMultipleOfTick { .. }
        | TbankAdapterError::ConversionError(_)
        | TbankAdapterError::InvalidInstrumentIdentity(_)
        | TbankAdapterError::BrokerOrderIdentityUnresolved(_) => SubmitFailureKind::LocalRejected,
        TbankAdapterError::PermissionDenied(_) | TbankAdapterError::RateLimited(_) => {
            SubmitFailureKind::BrokerRejected
        }
        TbankAdapterError::InstrumentNotFound(_)
        | TbankAdapterError::InstrumentMetadataUnresolved(_)
        | TbankAdapterError::FuturesMarginUnresolved(_)
        | TbankAdapterError::SubmitOutcomeUnknown(_)
        | TbankAdapterError::ReconnectFailed(_) => SubmitFailureKind::OutcomeUnknown,
        TbankAdapterError::GrpcStatus { code, .. } => classify_submit_grpc_status(*code),
    }
}

pub(super) fn classify_submit_grpc_status(code: tonic::Code) -> SubmitFailureKind {
    match code {
        tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::AlreadyExists
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange
        | tonic::Code::Unimplemented => SubmitFailureKind::BrokerRejected,
        tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Aborted
        | tonic::Code::Internal
        | tonic::Code::DataLoss
        | tonic::Code::Unavailable => SubmitFailureKind::OutcomeUnknown,
        tonic::Code::Ok
        | tonic::Code::PermissionDenied
        | tonic::Code::ResourceExhausted
        | tonic::Code::Unauthenticated => SubmitFailureKind::BrokerRejected,
    }
}

pub(super) fn pending_stage_after_submit_response(
    response: &TbankSubmitResponse,
) -> TbankPendingSubmitStage {
    match response {
        TbankSubmitResponse::StopOrder(_) => TbankPendingSubmitStage::Submitted,
        TbankSubmitResponse::Order(response) => {
            match TbankOrderExecutionReportStatus::try_from(response.execution_report_status).ok() {
                Some(TbankOrderExecutionReportStatus::ExecutionReportStatusRejected) => {
                    TbankPendingSubmitStage::Rejected
                }
                _ => TbankPendingSubmitStage::Submitted,
            }
        }
    }
}

pub(super) fn pending_stage_from_order_status(
    status: OrderStatus,
) -> Option<TbankPendingSubmitStage> {
    match status {
        OrderStatus::Filled | OrderStatus::PartiallyFilled => Some(TbankPendingSubmitStage::Filled),
        OrderStatus::Accepted => Some(TbankPendingSubmitStage::Accepted),
        OrderStatus::Rejected => Some(TbankPendingSubmitStage::Rejected),
        OrderStatus::Canceled | OrderStatus::Expired => Some(TbankPendingSubmitStage::Cancelled),
        _ => None,
    }
}

pub(super) fn update_pending_submit(
    pending: &mut TbankPendingSubmit,
    stage: TbankPendingSubmitStage,
    venue_order_id: Option<String>,
    reconciliation_ts: UnixNanos,
) {
    pending.stage = stage;
    if let Some(venue_order_id) = venue_order_id {
        pending.venue_order_id = Some(venue_order_id);
    }
    pending.last_reconciliation_ts = Some(reconciliation_ts);
    tracing::debug!(
        instrument_id = %pending.instrument_id,
        submitted_ts = %pending.submitted_ts,
        quantity_units = %pending.quantity_units,
        side = ?pending.side,
        stage = ?pending.stage,
        "updated T-Bank pending submit state"
    );
}

pub(super) fn mark_pending_submit_order_report(
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    report: &OrderStatusReport,
) {
    let Some(stage) = pending_stage_from_order_status(report.order_status) else {
        return;
    };
    let Some(client_order_id) = report.client_order_id.map(|id| id.to_string()) else {
        return;
    };
    if let Some(pending) = pending_submits
        .lock()
        .expect("pending_submits lock")
        .get_mut(client_order_id.as_str())
    {
        update_pending_submit(
            pending,
            stage,
            Some(report.venue_order_id.to_string()),
            report.ts_last,
        );
    }
}

pub(super) fn settle_order_report_mutation_state(
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    unresolved_cancellations: &Arc<Mutex<HashSet<TbankBrokerOrderIdentity>>>,
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    report: &OrderStatusReport,
) {
    mark_pending_submit_order_report(pending_submits, report);
    if !report.order_status.is_closed() {
        return;
    }

    let venue_order_id = report.venue_order_id.to_string();
    let related_order_ids = {
        let index = broker_order_index.lock().expect("broker_order_index lock");
        let canonical_order_id = index.canonical_venue_order_id_or_self(&venue_order_id);
        let mut order_ids = index.aliases_for_canonical_venue_order_id(&canonical_order_id);
        order_ids.push(venue_order_id);
        order_ids.push(canonical_order_id);
        order_ids
    };
    unresolved_cancellations
        .lock()
        .expect("unresolved_cancellations lock")
        .retain(|identity| !related_order_ids.contains(&identity.broker_order_id));
}

pub(super) fn mark_pending_submit_fill_report(
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    report: &FillReport,
) {
    let venue_order_id = report.venue_order_id.to_string();
    let mut pending_submits = pending_submits.lock().expect("pending_submits lock");
    if let Some(client_order_id) = report.client_order_id.map(|id| id.to_string())
        && let Some(pending) = pending_submits.get_mut(client_order_id.as_str())
    {
        update_pending_submit(
            pending,
            TbankPendingSubmitStage::Filled,
            Some(venue_order_id),
            report.ts_event,
        );
        return;
    }
    if let Some(pending) = pending_submits
        .values_mut()
        .find(|pending| pending.venue_order_id.as_deref() == Some(venue_order_id.as_str()))
    {
        update_pending_submit(
            pending,
            TbankPendingSubmitStage::Filled,
            Some(venue_order_id),
            report.ts_event,
        );
    }
}

pub(super) fn tbank_side_from_stop_direction(
    direction: i32,
) -> Option<crate::common::TbankOrderSide> {
    match StopOrderDirection::try_from(direction).ok() {
        Some(StopOrderDirection::Buy) => Some(crate::common::TbankOrderSide::Buy),
        Some(StopOrderDirection::Sell) => Some(crate::common::TbankOrderSide::Sell),
        Some(StopOrderDirection::Unspecified) | None => None,
    }
}

pub(super) fn tbank_order_type_from_stop_order(
    stop: &StopOrder,
) -> Option<crate::common::TbankOrderType> {
    if matches!(
        crate::grpc::generated::TakeProfitType::try_from(stop.take_profit_type).ok(),
        Some(crate::grpc::generated::TakeProfitType::Trailing)
    ) {
        return Some(
            match crate::grpc::generated::ExchangeOrderType::try_from(stop.exchange_order_type).ok()
            {
                Some(crate::grpc::generated::ExchangeOrderType::Limit) => {
                    crate::common::TbankOrderType::TrailingStopLimit
                }
                _ => crate::common::TbankOrderType::TrailingStopMarket,
            },
        );
    }
    match StopOrderType::try_from(stop.order_type).ok() {
        Some(StopOrderType::StopLoss) => Some(crate::common::TbankOrderType::StopMarket),
        Some(StopOrderType::TakeProfit) => {
            match crate::grpc::generated::ExchangeOrderType::try_from(stop.exchange_order_type).ok()
            {
                Some(crate::grpc::generated::ExchangeOrderType::Limit) => None,
                _ => Some(crate::common::TbankOrderType::MarketIfTouched),
            }
        }
        Some(StopOrderType::Unspecified | StopOrderType::StopLimit) | None => None,
    }
}

pub(super) fn order_status_report_from_post_order_response(
    account_id: AccountId,
    cmd: &nautilus_common::messages::execution::SubmitOrder,
    response: &PostOrderResponse,
    metadata: &TbankInstrumentMetadata,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let mut report = OrderStatusReport::new(
        account_id,
        order_initialized_instrument_id(cmd),
        Some(cmd.client_order_id),
        response.order_id.as_str().into(),
        Some(cmd.order_init.order_side),
        cmd.order_init.order_type,
        cmd.order_init.time_in_force,
        nautilus_order_status(
            response.execution_report_status,
            response.lots_requested,
            response.lots_executed,
        ),
        lots_to_quantity(response.lots_requested, metadata.lot)?,
        lots_to_quantity(response.lots_executed, metadata.lot)?,
        ts_init,
        ts_init,
        cmd.ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = cmd.order_init.price {
        report = report.with_price(price);
    }
    if let Some(trigger_price) = cmd.order_init.trigger_price {
        report = report.with_trigger_price(trigger_price);
    }
    if let Some(trigger_type) = cmd.order_init.trigger_type {
        report = report.with_trigger_type(trigger_type);
    }
    // PostOrderResponse.executed_order_price is already the average price per instrument;
    // only GetOrderState.executed_order_price is a cumulative value divided by lots.
    if let Some(avg_px) = response
        .executed_order_price
        .as_ref()
        .map(|value| average_price_from_money_value_for_instrument(value, Some(metadata)))
        .transpose()?
    {
        report = report.with_avg_px(avg_px.as_decimal());
    }
    Ok(with_default_stop_trigger_type(report))
}

pub(super) fn order_status_report_from_post_stop_order_response(
    account_id: AccountId,
    cmd: &nautilus_common::messages::execution::SubmitOrder,
    response: &PostStopOrderResponse,
    ts_init: UnixNanos,
) -> OrderStatusReport {
    let mut report = OrderStatusReport::new(
        account_id,
        order_initialized_instrument_id(cmd),
        Some(cmd.client_order_id),
        response.stop_order_id.as_str().into(),
        Some(cmd.order_init.order_side),
        cmd.order_init.order_type,
        cmd.order_init.time_in_force,
        OrderStatus::Accepted,
        cmd.order_init.quantity,
        Quantity::from(0),
        ts_init,
        ts_init,
        cmd.ts_init,
        Some(UUID4::new()),
    );
    if let Some(price) = cmd.order_init.price {
        report = report.with_price(price);
    }
    if let Some(trigger_price) = cmd.order_init.trigger_price {
        report = report.with_trigger_price(trigger_price);
    }
    if let Some(trigger_type) = cmd.order_init.trigger_type {
        report = report.with_trigger_type(trigger_type);
    }
    with_default_stop_trigger_type(report)
}

pub(super) fn order_initialized_instrument_id(
    cmd: &nautilus_common::messages::execution::SubmitOrder,
) -> nautilus_model::identifiers::InstrumentId {
    let initialized_instrument_id = cmd.order_init.instrument_id;
    if cmd.instrument_id != initialized_instrument_id {
        let command_is_supported = is_supported_tbank_submit_instrument(&cmd.instrument_id);
        let initialized_is_supported =
            is_supported_tbank_submit_instrument(&initialized_instrument_id);
        if command_is_supported {
            tracing::warn!(
                command_instrument_id = %cmd.instrument_id,
                order_initialized_instrument_id = %initialized_instrument_id,
                client_order_id = %cmd.client_order_id,
                "Nautilus SubmitOrder instrument_id differed from OrderInitialized; using the canonical SubmitOrder instrument_id"
            );
            return cmd.instrument_id;
        }
        if initialized_is_supported {
            tracing::warn!(
                command_instrument_id = %cmd.instrument_id,
                order_initialized_instrument_id = %initialized_instrument_id,
                client_order_id = %cmd.client_order_id,
                "Nautilus SubmitOrder instrument_id was not a supported T-Bank instrument; using OrderInitialized instrument_id"
            );
        } else {
            tracing::warn!(
                command_instrument_id = %cmd.instrument_id,
                order_initialized_instrument_id = %initialized_instrument_id,
                client_order_id = %cmd.client_order_id,
                "Nautilus SubmitOrder and OrderInitialized instrument_ids were not recognized as supported T-Bank instruments; preserving OrderInitialized instrument_id for validation"
            );
        }
    }
    initialized_instrument_id
}

pub(super) fn is_supported_tbank_submit_instrument(instrument_id: &InstrumentId) -> bool {
    crate::common::ids::TbankInstrumentIdParts::from_str(&instrument_id.to_string())
        .is_ok_and(|parts| parts.is_supported_family())
}
