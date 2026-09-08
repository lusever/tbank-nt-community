//! NautilusTrader [`ExecutionClient`] boundary for the T-Bank client.

use super::*;
use crate::common::venue::TbankVenue;
use anyhow::Context;

pub(super) fn order_report_matches_command(
    report: &OrderStatusReport,
    cmd: &nautilus_common::messages::execution::GenerateOrderStatusReports,
) -> bool {
    cmd.instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id)
        && cmd.start.is_none_or(|start| report.ts_last >= start)
        && cmd.end.is_none_or(|end| report.ts_last <= end)
        && (!cmd.open_only || report.order_status.is_open())
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

pub(super) fn position_report_matches_command(
    report: &PositionStatusReport,
    cmd: &nautilus_common::messages::execution::GeneratePositionStatusReports,
) -> bool {
    cmd.instrument_id
        .is_none_or(|instrument_id| report.instrument_id == instrument_id)
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
        if self.runtime.has_unfinished_mutating_tasks()
            || self.runtime.has_unresolved_mutation_outcomes()
        {
            anyhow::bail!(
                "cannot reset T-Bank execution client while broker mutation outcomes are unresolved"
            );
        }
        self.unsubscribe_instrument_updates();
        self.runtime.reset_state();
        self.core.set_stopped();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        if self.runtime.has_unfinished_mutating_tasks()
            || self.runtime.has_unresolved_mutation_outcomes()
        {
            anyhow::bail!(
                "cannot dispose T-Bank execution client while broker mutation outcomes are unresolved"
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
        self.runtime.disconnect_async().await;
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(
        &self,
        cmd: nautilus_common::messages::execution::SubmitOrder,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
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
        let route_client_order_id = client_order_id;
        self.runtime.spawn_mutating_command_task_with(async move {
            let prepared = match prepare_nautilus_order(&mut client, cmd).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    tracing::warn!(%error, %client_order_id, "denying Nautilus order during local preflight");
                    client.remove_unresolved_broker_order_route(client_order_id.as_str());
                    emitter.emit_order_denied(&order, &error.to_string());
                    return;
                }
            };
            emitter.emit_order_submitted(&order);
            if let Err(error) = submit_prepared_nautilus_order(&mut client, prepared, emitter).await
            {
                tracing::error!(%error, "failed to submit Nautilus order to T-Bank");
            }
        }, move || {
            // The task is registered but has not started its async preflight.
            // Publish the route at this acceptance boundary so a concurrent
            // CancelOrder cannot fall back to OrdersService.
            route_runtime.prepare_submit_route(&route_client_order_id, order_type);
        })?;
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
        self.runtime.spawn_mutating_command_task_with(async move {
            if commands
                .iter()
                .any(|command| {
                    command
                        .order_init
                        .contingency_type
                        .is_some()
                })
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
                match prepare_nautilus_order(&mut client, command).await {
                    Ok(order) => prepared.push((order, client_order_id)),
                    Err(error) => {
                        let reason = format!("order list preflight failed: {error}");
                        for (client_order_id, _) in &submit_routes_for_cleanup {
                            client.remove_unresolved_broker_order_route(client_order_id.as_str());
                        }
                        for order in &orders {
                            emitter.emit_order_denied(order, &reason);
                        }
                        return;
                    }
                }
            }

            for ((prepared, client_order_id), order) in
                prepared.into_iter().zip(orders)
            {
                emitter.emit_order_submitted(&order);
                if let Err(error) =
                    submit_prepared_nautilus_order(&mut client, prepared, emitter.clone()).await
                {
                    tracing::error!(%error, %client_order_id, "failed to submit order-list leg to T-Bank");
                }
            }
        }, move || {
            // Register every list-leg route while the mutating task is already
            // visible, before its first metadata preflight await.
            for (client_order_id, order_type) in submit_routes {
                route_runtime.prepare_submit_route(&client_order_id, order_type);
            }
        })?;
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
        self.runtime.spawn_mutating_command_task(async move {
            let client_order_id = cmd.client_order_id.to_string();
            let venue_order_id = cmd.venue_order_id.map(|id| id.to_string());
            let target = match client
                .resolve_cancel_target(&client_order_id, venue_order_id.as_deref())
                .await
            {
                Ok(target) => target,
                Err(error) => {
                    tracing::error!(
                        %error,
                        %client_order_id,
                        "failed to resolve T-Bank cancel target"
                    );
                    if matches!(
                        classify_cancel_failure(&error),
                        CancelFailureKind::BrokerRejected | CancelFailureKind::LocalFailure
                    ) {
                        emitter.emit_order_cancel_rejected_event(
                            cmd.strategy_id,
                            cmd.instrument_id,
                            cmd.client_order_id,
                            cmd.venue_order_id,
                            &error.to_string(),
                            current_unix_nanos(),
                        );
                    }
                    return;
                }
            };
            match target {
                TbankCancelTarget::Ready(identity) => {
                    if let Err(error) = client.cancel_resolved_broker_order(identity.clone()).await
                    {
                        tracing::error!(
                            %error,
                            %client_order_id,
                            "failed to cancel T-Bank order"
                        );
                        if classify_cancel_failure(&error) == CancelFailureKind::BrokerRejected {
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
                }
                TbankCancelTarget::Pending {
                    route,
                    client_order_id,
                } => {
                    tracing::info!(
                        %client_order_id,
                        route = ?route,
                        "deferred T-Bank cancel until broker order id is known"
                    );
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
        self.runtime.spawn_read_only_command_task(async move {
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
                Ok(None) => tracing::warn!("T-Bank query order returned no order status report"),
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
        self.runtime.spawn_read_only_command_task(async move {
            let result = client.query_portfolio().await;
            // See query_order: a stale generation may finish I/O after abort was requested.
            match result {
                Ok(portfolio) => match account_state_from_portfolio(&portfolio) {
                    Ok(Some(state)) => {
                        client.lifecycle_active.run_if_active(|| {
                            client.publish_account_state(state);
                        });
                    }
                    Ok(None) => tracing::warn!("T-Bank portfolio has no total account value"),
                    Err(error) => tracing::warn!(%error, "failed to map T-Bank account state"),
                },
                Err(error) => tracing::warn!(%error, "failed to query T-Bank account state"),
            }
        })?;
        Ok(())
    }

    fn cancel_all_orders(
        &self,
        _cmd: nautilus_common::messages::execution::CancelAllOrders,
    ) -> anyhow::Result<()> {
        self.runtime.ensure_lifecycle_active()?;
        let mut client = self.runtime.clone();
        self.runtime.spawn_mutating_command_task(async move {
            if let Err(error) = TbankExecutionRuntime::cancel_all_orders(&mut client).await {
                tracing::error!(%error, "failed to cancel all T-Bank orders");
            }
        })?;
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
        let mut order_states = if cmd.open_only {
            client.query_orders(false).await?.orders
        } else if let Some(start) = cmd.start {
            client
                .query_orders_since(i128::from(start.as_u64()))
                .await?
                .orders
        } else {
            client.query_orders(true).await?.orders
        };
        // Active stop orders alone are insufficient here: after activation T-Bank
        // exposes the child as a regular order and the parent only in stop-order
        // history. Keep that parent metadata available for identity correlation.
        let stops = client
            .query_stop_orders_for_reconciliation(None)
            .await?
            .stop_orders;
        if !cmd.open_only {
            client
                .append_missing_activated_stop_children(&mut order_states, &stops)
                .await?;
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
        let mut client = self.runtime.clone();
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
                cmd.start.map(|value| i128::from(value.as_u64())),
                cmd.end.map(|value| i128::from(value.as_u64())),
            )
            .await?;
        let mut reports = Vec::new();
        for item in &response.items {
            if fill_side_from_operation_type(item.r#type).is_none() {
                continue;
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
                    tracing::warn!(
                        %error,
                        "skipping malformed T-Bank fill operation with invalid instrument identity"
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            for report in fill_reports_from_cursor_operation_with_instruments(
                client.account_id(),
                item,
                cmd.ts_init,
                Some(&client.instruments),
            ) {
                reports.push(report.map(|report| {
                    canonicalize_managed_trade_fill_report(&self.runtime.broker_order_index, report)
                })?);
            }
        }
        reports.retain(|report| fill_report_matches_command(report, &cmd));
        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &nautilus_common::messages::execution::GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut client = self.runtime.clone();
        let positions = client.query_positions().await?;
        let account_id = if positions.account_id.is_empty() {
            client.account_id()
        } else {
            nautilus_account_id(&positions.account_id)
        };
        let mut snapshot_complete = !positions.limits_loading_in_progress;
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
                    snapshot_complete = false;
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
                None => snapshot_complete = false,
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
                    snapshot_complete = false;
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
                None => snapshot_complete = false,
            }
        }
        apply_position_snapshot(
            &client.position_projection,
            account_id,
            &mut reports,
            cmd.ts_init,
            TbankPositionProjectionSource::SecuritiesSnapshot,
            snapshot_complete,
        );
        reports.retain(|report| position_report_matches_command(report, cmd));
        Ok(reports)
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
        let start = lookback_mins
            .map(|mins| {
                let lookback_nanos = nautilus_core::datetime::checked_mins_to_nanos(mins)
                    .context("execution mass-status lookback exceeds nanosecond range")?;
                Ok::<_, anyhow::Error>(UnixNanos::from(
                    ts_init.as_u64().saturating_sub(lookback_nanos),
                ))
            })
            .transpose()?;
        let order_cmd = nautilus_common::messages::execution::GenerateOrderStatusReports::new(
            UUID4::new(),
            ts_init,
            false,
            None,
            start,
            None,
            None,
            None,
        );
        let fill_cmd = nautilus_common::messages::execution::GenerateFillReports::new(
            UUID4::new(),
            ts_init,
            None,
            None,
            start,
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
        status.add_fill_reports(self.generate_fill_reports(fill_cmd).await?);
        status.add_position_reports(self.generate_position_status_reports(&position_cmd).await?);
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
