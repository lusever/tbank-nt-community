//! NautilusTrader [`ExecutionClient`] boundary for the T-Bank client.

use std::sync::{Arc, Mutex};

use super::*;
use crate::common::venue::TbankVenue;
use anyhow::Context;

#[derive(Clone, Copy)]
struct ExecutionMassStatusWindow {
    order_start: Option<UnixNanos>,
    report_start: UnixNanos,
    complete: bool,
}

struct PreparedFillReports {
    reports: Vec<FillReport>,
    provenance: Vec<TbankFillReport>,
    complete: bool,
}

struct PreparedPositionReports {
    account_id: AccountId,
    reports: Vec<PositionStatusReport>,
    complete: bool,
}

impl PreparedFillReports {
    fn publish_provenance(&self, client: &TbankExecutionRuntime) {
        client.publish_snapshot_fill_provenance(&self.provenance);
    }
}

fn execution_mass_status_window(
    requested_start: Option<UnixNanos>,
    current_day_start: UnixNanos,
) -> ExecutionMassStatusWindow {
    let order_start = requested_start.map(|start| start.max(current_day_start));
    let report_start = order_start.unwrap_or(current_day_start);
    // An absent lookback is unbounded at the Nautilus API boundary, but T-Bank history still
    // starts today. Never present that clamped response as a complete account snapshot.
    let complete = requested_start.is_some_and(|start| start >= current_day_start);

    ExecutionMassStatusWindow {
        order_start,
        report_start,
        complete,
    }
}

fn utc_day_start(timestamp: UnixNanos) -> UnixNanos {
    const NANOS_PER_DAY: u64 = 86_400 * 1_000_000_000;
    UnixNanos::from(timestamp.as_u64() / NANOS_PER_DAY * NANOS_PER_DAY)
}

pub(super) fn order_report_matches_command(
    report: &OrderStatusReport,
    cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
) -> bool {
    let active = report.order_status.is_open() || report.order_status.is_inflight();
    cmd.instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id)
        && (active
            || (!cmd.open_only
                && cmd.start.is_none_or(|start| report.ts_last >= start)
                && cmd.end.is_none_or(|end| report.ts_last <= end)))
}

pub(super) fn fill_report_matches_command(
    report: &FillReport,
    cmd: &nautilus_common::messages::execution::GenerateFillReports,
) -> bool {
    cmd.instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id)
        && cmd
            .venue_order_id
            .is_none_or(|venue_order_id| report.venue_order_id == venue_order_id)
        && cmd.start.is_none_or(|start| report.ts_event >= start)
        && cmd.end.is_none_or(|end| report.ts_event <= end)
}

pub(super) fn report_instrument_matches_identity(
    instrument_id: InstrumentId,
    ticker: &str,
    class_code: &str,
) -> bool {
    if ticker.is_empty() || class_code.is_empty() {
        return false;
    }
    let Ok(parts) = instrument_id
        .to_string()
        .parse::<crate::common::ids::TbankInstrumentIdParts>()
    else {
        return false;
    };
    parts.ticker.eq_ignore_ascii_case(ticker) && parts.class_code.eq_ignore_ascii_case(class_code)
}

fn report_time_matches_command(
    ts_last: UnixNanos,
    cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
) -> bool {
    cmd.start.is_none_or(|start| ts_last >= start) && cmd.end.is_none_or(|end| ts_last <= end)
}

fn order_state_is_open_or_inflight(state: &OrderState) -> bool {
    let status = nautilus_order_status(
        state.execution_report_status,
        state.lots_requested,
        state.lots_executed,
    );
    status.is_open() || status.is_inflight()
}

fn merge_open_order_states(order_states: &mut Vec<OrderState>, open_states: Vec<OrderState>) {
    let mut indices = order_states
        .iter()
        .enumerate()
        .filter(|(_, state)| !state.order_id.is_empty())
        .map(|(index, state)| (state.order_id.clone(), index))
        .collect::<HashMap<_, _>>();
    for state in open_states {
        if state.order_id.is_empty() {
            order_states.push(state);
        } else if let Some(index) = indices.get(state.order_id.as_str()).copied() {
            order_states[index] = state;
        } else {
            indices.insert(state.order_id.clone(), order_states.len());
            order_states.push(state);
        }
    }
}

pub(super) fn order_state_matches_report_command(
    state: &OrderState,
    cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
) -> anyhow::Result<bool> {
    if cmd.instrument_id.is_some_and(|id| {
        !state.ticker.is_empty()
            && !state.class_code.is_empty()
            && !report_instrument_matches_identity(id, &state.ticker, &state.class_code)
    }) {
        return Ok(false);
    }
    let active = order_state_is_open_or_inflight(state);
    if cmd.open_only && !active {
        return Ok(false);
    }
    if active {
        return Ok(true);
    }
    let ts_last = state
        .order_date
        .as_ref()
        .map(super::timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(cmd.ts_init);
    Ok(report_time_matches_command(ts_last, cmd))
}

pub(super) fn stop_order_links_to_state(stop: &StopOrder, state: &OrderState) -> bool {
    (!state.order_request_id.is_empty() && stop.stop_order_id == state.order_request_id)
        || stop.exchange_order_id.as_deref() == Some(state.order_id.as_str())
}

pub(super) fn stop_order_matches_report_command(
    stop: &StopOrder,
    cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
    linked_state: Option<&OrderState>,
) -> anyhow::Result<bool> {
    if let Some(instrument_id) = cmd.instrument_id {
        let identity_matches = if !stop.ticker.is_empty() && !stop.class_code.is_empty() {
            report_instrument_matches_identity(instrument_id, &stop.ticker, &stop.class_code)
        } else {
            linked_state.is_none_or(|state| {
                state.ticker.is_empty()
                    || state.class_code.is_empty()
                    || report_instrument_matches_identity(
                        instrument_id,
                        &state.ticker,
                        &state.class_code,
                    )
            })
        };
        if !identity_matches {
            return Ok(false);
        }
    }

    let stop_active = StopOrderStatusOption::try_from(stop.status).ok()
        == Some(StopOrderStatusOption::StopOrderStatusActive);
    let child_active = linked_state.is_some_and(order_state_is_open_or_inflight);
    let active = stop_active || child_active;
    if cmd.open_only && !active {
        return Ok(false);
    }
    if active {
        return Ok(true);
    }

    // Activated stops report the child's latest order timestamp while retaining the parent
    // stop's venue identity. Preserve that parent when the linked child falls inside the query.
    let ts_last = linked_state
        .and_then(|state| state.order_date.as_ref())
        .or(stop.create_date.as_ref())
        .map(super::timestamp_to_unix_nanos)
        .transpose()?
        .unwrap_or(cmd.ts_init);
    Ok(report_time_matches_command(ts_last, cmd))
}

pub(super) fn position_report_matches_command(
    report: &PositionStatusReport,
    cmd: &nautilus_common::messages::execution::GeneratePositionStatusReports,
) -> bool {
    cmd.instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id)
}

pub(super) async fn submit_prepared_nautilus_order_list(
    client: &mut TbankExecutionRuntime,
    prepared: Vec<(PreparedNautilusOrder, ClientOrderId)>,
    orders: Vec<nautilus_model::orders::OrderAny>,
    emitter: ExecutionEventEmitter,
    recovery_deadline: tokio::time::Instant,
) {
    let mut remaining = prepared.into_iter().zip(orders);
    while let Some(((prepared, client_order_id), order)) = remaining.next() {
        if submit_outcome_submit_budget(
            client.config.request_timeout,
            recovery_deadline,
            tokio::time::Instant::now(),
        )
        .is_none()
        {
            let reason = "order-list submit deadline leaves no dispatch window";
            client.remove_unresolved_broker_order_route(client_order_id.as_str());
            emitter.emit_order_denied(&order, reason);
            for ((_, client_order_id), order) in remaining {
                client.remove_unresolved_broker_order_route(client_order_id.as_str());
                emitter.emit_order_denied(&order, reason);
            }
            return;
        }

        emitter.emit_order_submitted(&order);
        if let Err(error) =
            submit_prepared_nautilus_order(client, prepared, emitter.clone(), recovery_deadline)
                .await
        {
            tracing::error!(%error, %client_order_id, "failed to submit order-list leg to T-Bank");
        }
    }
}

pub(super) fn submit_commands_from_list(
    cmd: nautilus_common::messages::execution::SubmitOrderList,
) -> Vec<nautilus_common::messages::execution::SubmitOrder> {
    cmd.order_inits
        .into_iter()
        .map(|order_init| {
            let mut order_cmd = nautilus_common::messages::execution::SubmitOrder::new(
                cmd.trader_id,
                cmd.client_id,
                cmd.strategy_id,
                order_init.instrument_id,
                order_init.client_order_id,
                order_init,
                cmd.exec_algorithm_id,
                cmd.position_id,
                cmd.params.clone(),
                UUID4::new(),
                cmd.ts_init,
                cmd.correlation_id,
            );
            order_cmd.causation_id = cmd.causation_id;
            order_cmd
        })
        .collect()
}

async fn prepare_fill_reports(
    execution_client: &TbankExecutionClient,
    cmd: nautilus_common::messages::execution::GenerateFillReports,
) -> anyhow::Result<PreparedFillReports> {
    let mut client = execution_client.runtime.clone();
    // T-Bank exposes terminal order states only through the current-day GetOrders filters, whose
    // from/to fields are explicitly limited to orders created today. Operation history contains
    // trade IDs but no broker order IDs, so it cannot produce canonical fill identities outside
    // that order window. An omitted start means unbounded history in the Nautilus report
    // contract; fail closed instead of silently returning only today's fills.
    let (today, _) = current_utc_day_bounds();
    let today_start = i128::from(today.seconds) * 1_000_000_000;
    let operations_from = cmd
        .start
        .map(|value| i128::from(value.as_u64()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "T-Bank fill report generation requires an explicit start; unbounded fill history cannot be mapped to broker order identities"
            )
        })?;
    if cmd
        .end
        .is_some_and(|value| i128::from(value.as_u64()) < operations_from)
    {
        return Ok(PreparedFillReports {
            reports: Vec::new(),
            provenance: Vec::new(),
            complete: true,
        });
    }
    // The order endpoint is the identity authority for operation trades and only exposes
    // terminal orders from the current UTC day. Warm it from day start even when the caller asks
    // for a later operation window; otherwise a fill whose order was created earlier today is
    // returned without a venue_order_id and is lost at the identity boundary.
    let order_warmup_from = if operations_from < today_start {
        // Preserve the explicit fail-closed error for unsupported historical ranges.
        operations_from
    } else {
        today_start
    };
    let mut order_states = client.query_orders_since(order_warmup_from).await?.orders;
    // A direct fill snapshot can be the first request after restart. Rebuild the same
    // activated-stop aliases that order-status generation/reconciliation normally creates;
    // otherwise OperationsService trades are left under the exchange child ID and cannot be
    // canonicalized to the stop parent.
    let stops = client
        .query_stop_orders_for_reconciliation(None)
        .await?
        .stop_orders;
    client
        .append_missing_activated_stop_children(&mut order_states, &stops)
        .await?;
    let stop_id_by_exchange_order_id = stops
        .iter()
        .filter_map(|stop| {
            stop.exchange_order_id
                .as_ref()
                .filter(|order_id| !order_id.is_empty())
                .map(|order_id| (order_id.clone(), stop.stop_order_id.clone()))
        })
        .collect::<HashMap<_, _>>();
    let stop_ids = stops
        .iter()
        .map(|stop| (stop.stop_order_id.clone(), stop.stop_order_id.clone()))
        .collect::<HashMap<_, _>>();
    for stop in &stops {
        client.record_broker_order_id(
            TbankBrokerOrderRoute::StopOrder,
            stop.stop_order_id.as_str(),
        );
    }
    for order in &order_states {
        client.record_trade_order_mappings_from_order_state(order, order.order_id.as_str());
        if order.order_id.is_empty() {
            continue;
        }
        let activated_stop_id = stop_ids
            .get(order.order_request_id.as_str())
            .map(String::as_str)
            .or_else(|| {
                stop_id_by_exchange_order_id
                    .get(order.order_id.as_str())
                    .map(String::as_str)
            });
        if let Some(stop_order_id) = activated_stop_id {
            client.record_activated_stop_child_alias(stop_order_id, order.order_id.as_str());
        }
    }
    // A generated report is a repeatable venue snapshot, not a live execution event. Its
    // projection must therefore not consume seen_trade_ids from streams or an earlier snapshot.
    let snapshot_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let instrument_uid = match cmd.instrument_id {
        Some(id) => {
            let instrument_id = id.to_string();
            Some(
                client
                    .load_instrument_metadata(&instrument_id)
                    .await?
                    .instrument_uid,
            )
        }
        None => None,
    };
    let response = client
        .query_fills(
            instrument_uid,
            Some(operations_from),
            cmd.end.map(|value| i128::from(value.as_u64())),
        )
        .await?;
    let mut reports = Vec::new();
    let mut provenance = Vec::new();
    let mut complete = true;
    for item in &response.items {
        match classify_fill_operation_type(item.r#type) {
            TbankFillOperationKind::Trade(_) => {}
            TbankFillOperationKind::NonTrade => continue,
            TbankFillOperationKind::Unknown => {
                complete = false;
                tracing::warn!(
                    operation_type = item.r#type,
                    "skipping T-Bank operation with an unknown type from the fill snapshot"
                );
                continue;
            }
        }
        match client
            .load_supported_metadata_for_identity(
                &item.instrument_uid,
                &item.figi,
                &item.ticker,
                &item.class_code,
            )
            .await
        {
            Ok(_) => {}
            Err(TbankAdapterError::InstrumentOutOfScope(_)) => {
                tracing::debug!(
                    "ignoring T-Bank fill operation outside the supported adapter scope"
                );
                continue;
            }
            Err(error) if TbankExecutionRuntime::metadata_error_is_event_rejection(&error) => {
                complete = false;
                tracing::warn!(
                    %error,
                    "skipping malformed T-Bank fill operation with invalid instrument identity"
                );
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        if item
            .trades_info
            .as_ref()
            .is_none_or(|trades| trades.trades.is_empty())
        {
            complete = false;
            tracing::warn!(
                operation_id = %item.id,
                "T-Bank trade operation has no trade rows in the fill snapshot"
            );
            continue;
        }
        for report in fill_reports_from_cursor_operation_with_instruments(
            client.account_id(),
            item,
            cmd.ts_init,
            Some(&client.instruments),
            Some(&client.broker_order_index),
        ) {
            let mut report = match report {
                Ok(report) => report,
                Err(error)
                    if error
                        .downcast_ref::<super::TbankFillQuantityInvalid>()
                        .is_some() =>
                {
                    complete = false;
                    tracing::warn!(%error, "skipping T-Bank operation trade with invalid quantity");
                    continue;
                }
                Err(error) => return Err(error),
            };
            // Nautilus requires a Money value on FillReport. Preserve the fill for snapshot
            // reconciliation even when that value is only the zero placeholder; its typed
            // provenance event distinguishes unknown from zero after the full snapshot succeeds.
            report.report =
                canonicalize_managed_trade_fill_report(&client.broker_order_index, report.report);
            if !fill_report_matches_command(&report.report, &cmd) {
                continue;
            }
            let (projected, pending_provenance) = project_managed_trade_fill_report_deferred(
                &client.broker_order_index,
                &snapshot_projection,
                report,
            )?;
            provenance.extend(pending_provenance);
            if let Some(report) = projected {
                reports.push(report);
            }
        }
    }
    reports.retain(|report| fill_report_matches_command(report, &cmd));
    Ok(PreparedFillReports {
        reports,
        provenance,
        complete,
    })
}

async fn prepare_position_status_reports(
    client: &mut TbankExecutionRuntime,
    cmd: &nautilus_common::messages::execution::GeneratePositionStatusReports,
) -> anyhow::Result<PreparedPositionReports> {
    let positions = client.query_positions().await?;
    let account_id = if positions.account_id.is_empty() {
        client.account_id()
    } else {
        nautilus_account_id(&positions.account_id)
    };
    let mut complete = !positions.limits_loading_in_progress;
    let mut reports = Vec::new();
    for position in &positions.securities {
        match client
            .metadata_resolution_for_identity(
                &position.instrument_uid,
                &position.figi,
                &position.ticker,
                &position.class_code,
            )
            .await?
        {
            TbankInstrumentMetadataResolution::Enabled => {}
            TbankInstrumentMetadataResolution::OutOfScope => {
                tracing::debug!(
                    "ignoring T-Bank security position outside the supported adapter scope"
                );
                continue;
            }
            TbankInstrumentMetadataResolution::Rejected => {
                complete = false;
                tracing::warn!(
                    "skipping malformed T-Bank security position without a trustworthy instrument identity"
                );
                continue;
            }
        }
        match position_status_report_from_security_with_instruments(
            account_id,
            position,
            cmd.ts_init,
            Some(&client.instruments),
        ) {
            Some(report) => reports.push(report),
            None => complete = false,
        }
    }
    for position in &positions.futures {
        match client
            .metadata_resolution_for_identity(
                &position.instrument_uid,
                &position.figi,
                &position.ticker,
                &position.class_code,
            )
            .await?
        {
            TbankInstrumentMetadataResolution::Enabled => {}
            TbankInstrumentMetadataResolution::OutOfScope => {
                tracing::debug!(
                    "ignoring T-Bank futures position outside the supported adapter scope"
                );
                continue;
            }
            TbankInstrumentMetadataResolution::Rejected => {
                complete = false;
                tracing::warn!(
                    "skipping malformed T-Bank futures position without a trustworthy instrument identity"
                );
                continue;
            }
        }
        match position_status_report_from_future_with_instruments(
            account_id,
            position,
            cmd.ts_init,
            &client.instruments,
        ) {
            Some(report) => reports.push(report),
            None => complete = false,
        }
    }
    Ok(PreparedPositionReports {
        account_id,
        reports,
        complete,
    })
}

#[async_trait(?Send)]
impl ExecutionClient for TbankExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn handles_order_venue(&self, venue: Venue) -> bool {
        crate::common::venue::TbankVenue::from_str(venue.as_str())
            .ok()
            .is_some_and(|venue| TbankVenue::all().contains(&venue))
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.runtime
            .emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        if !self.runtime.emitter.is_initialized() {
            self.runtime.emitter.set_sender(get_exec_event_sender());
        }
        self.subscribe_instrument_updates();
        self.core.set_started();
        Ok(())
    }

    // Sync lifecycle hooks request cancellation. Reset/dispose fail until async drain and broker
    // mutation resolution are complete.
    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.core.set_stopped();
        self.disconnect();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        if !self.task_owner.can_reset() || self.runtime.has_unresolved_mutation_outcomes() {
            anyhow::bail!(
                "cannot reset T-Bank execution client before task drain and broker mutation resolution"
            );
        }
        self.unsubscribe_instrument_updates();
        self.runtime.reset_state();
        self.core.set_stopped();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        if !self.task_owner.can_reset() || self.runtime.has_unresolved_mutation_outcomes() {
            anyhow::bail!(
                "cannot dispose T-Bank execution client before task drain and broker mutation resolution"
            );
        }
        self.unsubscribe_instrument_updates();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        TbankExecutionClient::connect(self)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.disconnect_async().await.map_err(anyhow::Error::from)
    }

    fn submit_order(
        &self,
        cmd: nautilus_common::messages::execution::SubmitOrder,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        if !self.runtime.emitter.is_initialized() {
            anyhow::bail!("Nautilus execution event emitter is not initialized");
        }
        let mut client = self.runtime.clone();
        if !self.runtime.emitter.is_initialized() {
            anyhow::bail!("Nautilus execution event emitter is not initialized");
        }
        let order = self
            .core
            .get_order(&cmd.client_order_id)
            .or_else(|_| cmd.order_init.clone().try_into())?;
        if nautilus_model::orders::Order::is_closed(&order) {
            tracing::warn!(
                client_order_id = %cmd.client_order_id,
                "ignoring submit for closed Nautilus order"
            );
            return Ok(());
        }
        let client_order_id = cmd.client_order_id;
        let order_type = cmd.order_init.order_type;
        let emitter = self.runtime.emitter.clone();
        let route_runtime = self.runtime.clone();
        let panic_runtime = self.runtime.clone();
        let route_client_order_id = client_order_id;
        let panic_client_order_id = client_order_id;
        let recovery_deadline =
            tokio::time::Instant::now() + submit_outcome_recovery_budget(&self.runtime.config);
        self.task_owner.spawn_mutating_command_task_with(
            &self.runtime,
            "submit_order",
            async move {
                let prepared = match prepare_nautilus_order_before_deadline(
                    &mut client,
                    cmd,
                    recovery_deadline,
                )
                .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        tracing::warn!(%error, %client_order_id, "denying Nautilus order during local preflight");
                        client.remove_unresolved_broker_order_route(client_order_id.as_str());
                        emitter.emit_order_denied(&order, &error.to_string());
                        return;
                    }
                };
                emitter.emit_order_submitted(&order);
                if let Err(error) = submit_prepared_nautilus_order(
                    &mut client,
                    prepared,
                    emitter,
                    recovery_deadline,
                )
                .await
                {
                    tracing::error!(%error, "failed to submit Nautilus order to T-Bank");
                }
            },
            move || {
                // Register the route before opening the task's polling gate.
                route_runtime.prepare_submit_route(&route_client_order_id, order_type);
            },
            move || {
                panic_runtime.mark_pending_submit_stage(
                    panic_client_order_id.as_str(),
                    TbankPendingSubmitStage::Unknown,
                    None,
                );
            },
        )?;
        Ok(())
    }

    fn submit_order_list(
        &self,
        cmd: nautilus_common::messages::execution::SubmitOrderList,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        if !self.runtime.emitter.is_initialized() {
            anyhow::bail!("Nautilus execution event emitter is not initialized");
        }
        // LiveNode tracks every leg from this command as in-flight. Keep preparation, dispatch,
        // and recovery for the whole list inside the same window.
        let recovery_deadline =
            tokio::time::Instant::now() + submit_outcome_recovery_budget(&self.runtime.config);
        let commands = submit_commands_from_list(cmd);
        let mut orders = Vec::with_capacity(commands.len());
        for command in &commands {
            let order = self
                .core
                .get_order(&command.client_order_id)
                .or_else(|_| command.order_init.clone().try_into())?;
            if nautilus_model::orders::Order::is_closed(&order) {
                anyhow::bail!(
                    "cannot submit order list containing closed order {}",
                    command.client_order_id
                );
            }
            orders.push(order);
        }
        let mut client = self.runtime.clone();
        let emitter = self.runtime.emitter.clone();
        let submit_routes = if commands
            .iter()
            .any(|command| command.order_init.contingency_type.is_some())
        {
            Vec::new()
        } else {
            commands
                .iter()
                .map(|command| (command.client_order_id, command.order_init.order_type))
                .collect::<Vec<_>>()
        };
        let submit_routes_for_cleanup = submit_routes.clone();
        let route_runtime = self.runtime.clone();
        let panic_runtime = self.runtime.clone();
        let panic_client_order_ids = submit_routes
            .iter()
            .map(|(client_order_id, _)| client_order_id.to_string())
            .collect::<Vec<_>>();
        self.task_owner.spawn_mutating_command_task_with(
            &self.runtime,
            "submit_order_list",
            async move {
                if commands
                    .iter()
                    .any(|command| command.order_init.contingency_type.is_some())
                {
                    let reason = "T-Bank adapter does not support contingent order lists";
                    for order in &orders {
                        emitter.emit_order_denied(order, reason);
                    }
                    return;
                }

                let mut prepared = Vec::with_capacity(commands.len());
                for command in commands {
                    let client_order_id = command.client_order_id;
                    match prepare_nautilus_order_before_deadline(
                        &mut client,
                        command,
                        recovery_deadline,
                    )
                    .await
                    {
                        Ok(order) => prepared.push((order, client_order_id)),
                        Err(error) => {
                            let reason = format!("order list preflight failed: {error}");
                            for (client_order_id, _) in &submit_routes_for_cleanup {
                                client
                                    .remove_unresolved_broker_order_route(client_order_id.as_str());
                            }
                            for order in &orders {
                                emitter.emit_order_denied(order, &reason);
                            }
                            return;
                        }
                    }
                }

                submit_prepared_nautilus_order_list(
                    &mut client,
                    prepared,
                    orders,
                    emitter,
                    recovery_deadline,
                )
                .await;
            },
            move || {
                // Register every list-leg route while the mutating task is already
                // visible, before its first metadata preflight await.
                for (client_order_id, order_type) in submit_routes {
                    route_runtime.prepare_submit_route(&client_order_id, order_type);
                }
            },
            move || {
                for client_order_id in panic_client_order_ids {
                    panic_runtime.mark_pending_submit_stage(
                        client_order_id.as_str(),
                        TbankPendingSubmitStage::Unknown,
                        None,
                    );
                }
            },
        )?;
        Ok(())
    }

    fn modify_order(
        &self,
        cmd: nautilus_common::messages::execution::ModifyOrder,
    ) -> anyhow::Result<()> {
        self.runtime.emitter.emit_order_modify_rejected_event(
            cmd.strategy_id,
            cmd.instrument_id,
            cmd.client_order_id,
            cmd.venue_order_id,
            "T-Bank adapter does not support modify_order; cancel and submit a replacement",
            current_unix_nanos(),
        );
        Ok(())
    }

    fn cancel_order(
        &self,
        cmd: nautilus_common::messages::execution::CancelOrder,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        let mut client = self.runtime.clone();
        let emitter = self.runtime.emitter.clone();
        let account_id = self.runtime.account_id();
        self.task_owner
            .spawn_mutating_command_task(&self.runtime, "cancel_order", async move {
                let client_order_id = cmd.client_order_id.to_string();
                let venue_order_id = cmd.venue_order_id.map(|id| id.to_string());
                let pending_cancel_receiver =
                    client.register_pending_cancel_waiter(&client_order_id);
                let target = match client
                    .resolve_cancel_target(&client_order_id, venue_order_id.as_deref())
                    .await
                {
                    Ok(target) => target,
                    Err(error) => {
                        drop(pending_cancel_receiver);
                        client.remove_closed_pending_cancel_waiters(&client_order_id);
                        let command_error = TbankCommandError::before_rpc(error);
                        tracing::error!(
                            %command_error,
                            %client_order_id,
                            "failed to resolve T-Bank cancel target"
                        );
                        if matches!(
                            classify_command_failure(&command_error),
                            CommandFailure::NotSent(_) | CommandFailure::VenueRejected(_)
                        ) {
                            emitter.emit_order_cancel_rejected_event(
                                cmd.strategy_id,
                                cmd.instrument_id,
                                cmd.client_order_id,
                                cmd.venue_order_id,
                                &command_error.to_string(),
                                current_unix_nanos(),
                            );
                        }
                        return;
                    }
                };
                let identity = match target {
                    TbankCancelTarget::Ready(identity) => {
                        drop(pending_cancel_receiver);
                        client.remove_closed_pending_cancel_waiters(&client_order_id);
                        identity
                    }
                    TbankCancelTarget::Pending {
                        route,
                        client_order_id: pending_client_order_id,
                        owner,
                    } => {
                        if !owner {
                            drop(pending_cancel_receiver);
                            client.remove_closed_pending_cancel_waiters(&pending_client_order_id);
                            tracing::debug!(
                                client_order_id = %pending_client_order_id,
                                route = ?route,
                                "T-Bank cancel is already awaiting order identity"
                            );
                            return;
                        }
                        tracing::info!(
                            client_order_id = %pending_client_order_id,
                            route = ?route,
                            "retaining admitted T-Bank cancel until broker order id is known"
                        );
                        match pending_cancel_receiver.await {
                            Ok(Ok(identity)) => identity,
                            Ok(Err(reason)) => {
                                emitter.emit_order_cancel_rejected_event(
                                    cmd.strategy_id,
                                    cmd.instrument_id,
                                    cmd.client_order_id,
                                    cmd.venue_order_id,
                                    &reason,
                                    current_unix_nanos(),
                                );
                                return;
                            }
                            Err(_) => {
                                tracing::error!(
                                    %client_order_id,
                                    "pending T-Bank cancel lost its identity continuation"
                                );
                                return;
                            }
                        }
                    }
                };
                if let Err(error) = client
                    .cancel_resolved_broker_order_admitted(identity.clone())
                    .await
                {
                    tracing::error!(
                        %error,
                        %client_order_id,
                        "failed to cancel T-Bank order"
                    );
                    if matches!(
                        classify_command_failure(&error),
                        CommandFailure::VenueRejected(_)
                    ) {
                        emitter.emit_order_cancel_rejected_event(
                            cmd.strategy_id,
                            cmd.instrument_id,
                            cmd.client_order_id,
                            cmd.venue_order_id,
                            &error.to_string(),
                            current_unix_nanos(),
                        );
                    } else {
                        match client.recover_ambiguous_cancel(identity).await {
                            Ok(TbankCancelRecoveryOutcome::Canceled) => {
                                let ts_event = current_unix_nanos();
                                client.lifecycle_active.run_if_active(|| {
                                    emitter.send_order_event(OrderEventAny::Canceled(
                                        OrderCanceled::new(
                                            cmd.trader_id,
                                            cmd.strategy_id,
                                            cmd.instrument_id,
                                            cmd.client_order_id,
                                            UUID4::new(),
                                            ts_event,
                                            ts_event,
                                            true,
                                            cmd.venue_order_id,
                                            Some(account_id),
                                            None,
                                        ),
                                    ));
                                });
                            }
                            Ok(TbankCancelRecoveryOutcome::Active) => {
                                client.lifecycle_active.run_if_active(|| {
                                    emitter.emit_order_cancel_rejected_event(
                                        cmd.strategy_id,
                                        cmd.instrument_id,
                                        cmd.client_order_id,
                                        cmd.venue_order_id,
                                        "broker reconciliation confirmed the order remains active",
                                        current_unix_nanos(),
                                    );
                                });
                            }
                            Err(recovery_error) => {
                                tracing::warn!(
                                    %recovery_error,
                                    %client_order_id,
                                    "T-Bank cancel outcome recovery remained unresolved"
                                );
                            }
                        }
                    }
                }
            })?;
        Ok(())
    }

    fn query_order(
        &self,
        cmd: nautilus_common::messages::execution::QueryOrder,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        let mut client = self.runtime.clone();
        let emitter = self.runtime.emitter.clone();
        self.task_owner
            .spawn_read_only_command_task(&self.runtime, "query_order", async move {
                let result = client
                    .query_order_status_report_by_ids(
                        Some(cmd.client_order_id),
                        cmd.venue_order_id,
                        cmd.ts_init,
                    )
                    .await;
                // Abort is asynchronous: the current poll may finish after reset. The generation
                // gate prevents stale events; any earlier mapping touched only old reset-isolated Arcs.
                match result {
                    Ok(Some(report)) => {
                        client.lifecycle_active.run_if_active(|| {
                            emitter.send_order_status_report(report);
                        });
                    }
                    Ok(None) => {
                        tracing::warn!("T-Bank query order returned no order status report")
                    }
                    Err(error) => tracing::warn!(%error, "failed to query T-Bank order status"),
                }
            })?;
        Ok(())
    }

    fn query_account(
        &self,
        _cmd: nautilus_common::messages::execution::QueryAccount,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        let mut client = self.runtime.clone();
        self.task_owner.spawn_read_only_command_task(
            &self.runtime,
            "query_account",
            async move {
                let result = client.query_portfolio().await;
                // See query_order: a stale generation may finish I/O after abort was requested.
                match result {
                    Ok(portfolio) => match account_state_from_portfolio(&portfolio) {
                        Ok(Some(state)) => {
                            if let Some(Err(error)) = client
                                .lifecycle_active
                                .run_if_active(|| client.publish_account_state(state))
                            {
                                tracing::warn!(%error, "failed to publish T-Bank account state");
                            }
                        }
                        Ok(None) => tracing::warn!("T-Bank portfolio has no total account value"),
                        Err(error) => tracing::warn!(%error, "failed to map T-Bank account state"),
                    },
                    Err(error) => tracing::warn!(%error, "failed to query T-Bank account state"),
                }
            },
        )?;
        Ok(())
    }

    fn cancel_all_orders(
        &self,
        _cmd: nautilus_common::messages::execution::CancelAllOrders,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        let mut client = self.runtime.clone();
        self.task_owner.spawn_mutating_command_task(
            &self.runtime,
            "cancel_all_orders",
            async move {
                if let Err(error) =
                    TbankExecutionRuntime::cancel_all_orders_admitted(&mut client).await
                {
                    tracing::error!(%error, "failed to cancel all T-Bank orders");
                }
            },
        )?;
        Ok(())
    }

    fn batch_cancel_orders(
        &self,
        cmd: nautilus_common::messages::execution::BatchCancelOrders,
    ) -> anyhow::Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &nautilus_common::messages::execution::GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let mut client = self.runtime.clone();
        client
            .query_order_status_report_by_ids(cmd.client_order_id, cmd.venue_order_id, cmd.ts_init)
            .await
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut client = self.runtime.clone();
        let mut queried_order_states = if cmd.open_only {
            client.query_open_orders().await?.orders
        } else if let Some(start) = cmd.start {
            client
                .query_orders_since(i128::from(start.as_u64()))
                .await?
                .orders
        } else {
            client.query_orders(true).await?.orders
        };
        if !cmd.open_only {
            let open_order_states = client.query_open_orders().await?.orders;
            merge_open_order_states(&mut queried_order_states, open_order_states);
        }
        let queried_stops = client
            .query_stop_orders_for_reconciliation(None)
            .await?
            .stop_orders;

        // Activated children can fall inside the requested time range even when the parent
        // stop was created earlier and the broker's order-history query omitted the child.
        // Recover those children before applying time filters. A complete, mismatching stop
        // instrument identity is enough to avoid an unnecessary child query; incomplete parent
        // identities must be resolved from the child state.
        if !cmd.open_only {
            let stops_for_child_recovery = queried_stops
                .iter()
                .filter(|stop| {
                    cmd.instrument_id.is_none_or(|instrument_id| {
                        stop.ticker.is_empty()
                            || stop.class_code.is_empty()
                            || report_instrument_matches_identity(
                                instrument_id,
                                &stop.ticker,
                                &stop.class_code,
                            )
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            client
                .append_missing_activated_stop_children(
                    &mut queried_order_states,
                    &stops_for_child_recovery,
                )
                .await?;
        }

        let mut order_states = Vec::with_capacity(queried_order_states.len());
        for state in &queried_order_states {
            if order_state_matches_report_command(state, cmd)? {
                order_states.push(state.clone());
            }
        }

        // Keep stop parents available for identity correlation, then apply report scopes using
        // the recovered child timestamp when one exists.
        let mut stops = Vec::with_capacity(queried_stops.len());
        for stop in queried_stops {
            let linked_state = queried_order_states
                .iter()
                .find(|state| stop_order_links_to_state(&stop, state));
            if stop_order_matches_report_command(&stop, cmd, linked_state)? {
                stops.push(stop);
            }
        }
        let stop_by_id = stops
            .iter()
            .map(|stop| (stop.stop_order_id.clone(), stop))
            .collect::<HashMap<_, _>>();
        let stop_id_by_exchange_order_id = stops
            .iter()
            .filter_map(|stop| {
                stop.exchange_order_id
                    .as_ref()
                    .filter(|order_id| !order_id.is_empty())
                    .map(|order_id| (order_id.clone(), stop.stop_order_id.clone()))
            })
            .collect::<HashMap<_, _>>();
        let stop_client_order_ids = stops
            .iter()
            .filter_map(|stop| {
                client
                    .broker_order_index
                    .lock()
                    .expect("broker_order_index lock")
                    .client_order_id_for_venue_order_id(stop.stop_order_id.as_str())
                    .map(|client_order_id| (stop.stop_order_id.clone(), client_order_id))
            })
            .collect::<HashMap<_, _>>();

        let mut activated_stop_ids = HashSet::new();
        let mut reports = Vec::with_capacity(order_states.len() + stops.len());
        for state in order_states {
            let activated_stop_id = stop_by_id
                .contains_key(state.order_request_id.as_str())
                .then(|| state.order_request_id.clone())
                .or_else(|| {
                    stop_id_by_exchange_order_id
                        .get(state.order_id.as_str())
                        .cloned()
                });
            if let Some(stop_id) = activated_stop_id
                && let Some(stop) = stop_by_id.get(stop_id.as_str())
            {
                activated_stop_ids.insert(stop_id.clone());
                let metadata = match client.metadata_for_stop_order(stop).await {
                    Ok(metadata) => metadata,
                    Err(error) if reconciliation_adapter_error_is_safe_to_skip(&error) => {
                        tracing::warn!(
                            %error,
                            "skipping T-Bank activated stop order with unsupported or invalid event identity"
                        );
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                client.record_broker_order_id(
                    TbankBrokerOrderRoute::StopOrder,
                    stop.stop_order_id.as_str(),
                );
                if let Some(client_order_id) = stop_client_order_ids.get(stop_id.as_str()) {
                    client.record_stop_order_context(client_order_id, stop, &metadata);
                }
                if !state.order_id.is_empty() {
                    client.record_activated_stop_child_mapping(
                        stop_client_order_ids
                            .get(stop_id.as_str())
                            .map(String::as_str)
                            .unwrap_or(""),
                        stop.stop_order_id.as_str(),
                        state.order_id.as_str(),
                    );
                }
                let managed_order_type = client.managed_order_type_for_client_order_id(
                    stop_client_order_ids
                        .get(stop_id.as_str())
                        .map(String::as_str),
                );
                match activated_stop_child_status_report_with_context(
                    client.account_id(),
                    stop,
                    &state,
                    cmd.ts_init,
                    metadata.lot,
                    stop_client_order_ids
                        .get(stop_id.as_str())
                        .map(String::as_str),
                    Some(&client.instruments),
                    managed_order_type,
                ) {
                    Ok(report) => reports.push(report),
                    Err(error) if reconnect_reconciliation_error_is_safe_to_skip(&error) => {
                        tracing::warn!(%error, "skipping T-Bank activated stop order event");
                    }
                    Err(error) => return Err(error),
                }
            } else {
                match client
                    .order_status_report_from_state_with_lots(
                        client.account_id(),
                        state,
                        cmd.ts_init,
                    )
                    .await
                {
                    Ok(report) => reports.push(report),
                    Err(error) if reconnect_reconciliation_error_is_safe_to_skip(&error) => {
                        tracing::warn!(%error, "skipping T-Bank order event");
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        for stop in stops {
            if activated_stop_ids.contains(stop.stop_order_id.as_str()) {
                continue;
            }
            if cmd.open_only
                && StopOrderStatusOption::try_from(stop.status).ok()
                    != Some(StopOrderStatusOption::StopOrderStatusActive)
            {
                continue;
            }
            let client_order_id = stop_client_order_ids.get(stop.stop_order_id.as_str());
            let metadata = match client.metadata_for_stop_order(&stop).await {
                Ok(metadata) => metadata,
                Err(error) if reconciliation_adapter_error_is_safe_to_skip(&error) => {
                    tracing::warn!(
                        %error,
                        "skipping T-Bank stop order with unsupported or invalid event identity"
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            client.record_broker_order_id(
                TbankBrokerOrderRoute::StopOrder,
                stop.stop_order_id.as_str(),
            );
            if let Some(client_order_id) = client_order_id {
                client.record_stop_order_context(client_order_id, &stop, &metadata);
            }
            let managed_order_type = client.managed_order_type_for_client_order_id(
                client_order_id.map(|value| value.as_str()),
            );
            let mut report = match stop_order_status_report_with_context(
                client.account_id(),
                stop,
                cmd.ts_init,
                metadata.lot,
                Some(&client.instruments),
                managed_order_type,
            ) {
                Ok(report) => report,
                Err(error) if reconnect_reconciliation_error_is_safe_to_skip(&error) => {
                    tracing::warn!(%error, "skipping T-Bank stop order event");
                    continue;
                }
                Err(error) => return Err(error),
            };
            report.client_order_id = client_order_id.map(|value| value.as_str().into());
            reports.push(report);
        }
        reports.retain(|report| order_report_matches_command(report, cmd));
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: nautilus_common::messages::execution::GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let prepared = prepare_fill_reports(self, cmd).await?;
        anyhow::ensure!(
            prepared.complete,
            "T-Bank fill report query returned an incomplete snapshot; refusing partial authoritative fill reports"
        );
        prepared.publish_provenance(&self.runtime);
        Ok(prepared.reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &nautilus_common::messages::execution::GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut client = self.runtime.clone();
        let mut prepared = prepare_position_status_reports(&mut client, cmd).await?;
        anyhow::ensure!(
            prepared.complete,
            "T-Bank position status report query returned an incomplete snapshot; refusing partial authoritative position reports"
        );
        apply_position_snapshot(
            &client.position_projection,
            prepared.account_id,
            &mut prepared.reports,
            cmd.ts_init,
            TbankPositionProjectionSource::SecuritiesSnapshot,
            true,
        );
        prepared
            .reports
            .retain(|report| position_report_matches_command(report, cmd));
        Ok(prepared.reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = current_unix_nanos();
        let mut status = ExecutionMassStatus::new(
            self.client_id(),
            self.account_id(),
            self.venue(),
            ts_init,
            Some(UUID4::new()),
        );
        let requested_start = lookback_mins
            .map(|mins| {
                let lookback = nautilus_core::DurationNanos::try_from_mins(mins)
                    .context("execution mass-status lookback exceeds nanosecond range")?;
                Ok::<_, anyhow::Error>(ts_init.saturating_sub(lookback))
            })
            .transpose()?;
        // GetOrders only exposes terminal orders created during the current UTC day, and fill
        // identities depend on that order history. Clamp both historical sources to the same
        // boundary and report incomplete coverage when the caller requested an earlier start.
        let window = execution_mass_status_window(requested_start, utc_day_start(ts_init));
        let order_cmd = nautilus_common::messages::execution::GenerateOrderStatusReports::new(
            UUID4::new(),
            ts_init,
            false,
            None,
            window.order_start,
            None,
            None,
            None,
        );
        let fill_cmd = nautilus_common::messages::execution::GenerateFillReports::new(
            UUID4::new(),
            ts_init,
            None,
            None,
            Some(window.report_start),
            None,
            None,
            None,
        );
        let position_cmd = nautilus_common::messages::execution::GeneratePositionStatusReports::new(
            UUID4::new(),
            ts_init,
            None,
            None,
            None,
            None,
            None,
        );

        status.add_order_reports(self.generate_order_status_reports(&order_cmd).await?);
        let mut reports_complete = window.complete;
        let mut pending_fill_provenance = Vec::new();
        let fill_reports = match prepare_fill_reports(self, fill_cmd).await {
            Ok(prepared) => {
                reports_complete &= prepared.complete;
                pending_fill_provenance = prepared.provenance;
                prepared.reports
            }
            Err(error)
                if error
                    .downcast_ref::<super::TbankFillIdentityUnresolved>()
                    .is_some() =>
            {
                // Mass status is an explicitly completeness-aware snapshot. Preserve its
                // independently recoverable order/position reports, but never publish a partial
                // fill set when a broker operation cannot be linked to an order.
                tracing::warn!(
                    %error,
                    "T-Bank mass status omits fills because broker order identity is unresolved"
                );
                reports_complete = false;
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        status.add_fill_reports(fill_reports);
        let mut position_client = self.runtime.clone();
        let mut position_reports =
            prepare_position_status_reports(&mut position_client, &position_cmd).await?;
        reports_complete &= position_reports.complete;
        apply_position_snapshot(
            &position_client.position_projection,
            position_reports.account_id,
            &mut position_reports.reports,
            ts_init,
            TbankPositionProjectionSource::SecuritiesSnapshot,
            position_reports.complete,
        );
        status.add_position_reports(position_reports.reports);
        status.set_report_window(Some(window.report_start), reports_complete);
        // Commission events are part of the mass-status snapshot. Publish them only after all
        // fallible order, fill, and position report generation has completed successfully. The
        // runtime resolves the current sender and serializes provenance with live publication.
        self.runtime
            .publish_snapshot_fill_provenance(&pending_fill_provenance);
        Ok(Some(status))
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        if let Some(metadata) =
            metadata_from_instrument(&instrument).filter(TbankInstrumentMetadata::is_supported)
        {
            self.runtime
                .instruments
                .lock()
                .expect("instruments lock")
                .insert(metadata.instrument_id.clone(), metadata);
        }
    }
}

#[cfg(test)]
mod mass_status_window_tests {
    use super::*;

    #[test]
    fn unbounded_mass_status_marks_day_limited_coverage_incomplete() {
        let current_day_start = UnixNanos::from(86_400_000_000_000_u64);

        let window = execution_mass_status_window(None, current_day_start);

        assert_eq!(window.order_start, None);
        assert_eq!(window.report_start, current_day_start);
        assert!(!window.complete);
    }
}
