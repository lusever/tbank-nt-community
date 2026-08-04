//! Reconnect, order-identity, and unresolved-fill reconciliation workflows.

use super::*;

#[derive(Clone)]
pub(super) struct TbankReconnectReconciler {
    client: TbankExecutionRuntime,
    emitter: ExecutionEventEmitter,
    gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Default)]
pub(super) struct TbankReconnectReconciliationCounts {
    orders: usize,
    fills: usize,
}

impl TbankReconnectReconciler {
    pub(super) fn new(client: TbankExecutionRuntime, emitter: ExecutionEventEmitter) -> Self {
        Self {
            client,
            emitter,
            gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn reconcile(&self, reason: &str, from_unix_nanos: i128) -> anyhow::Result<()> {
        let _guard = self.gate.lock().await;
        let counts =
            publish_reconnect_reconciliation(&self.client, &self.emitter, from_unix_nanos).await?;
        tracing::info!(
            reason,
            orders = counts.orders,
            fills = counts.fills,
            "completed T-Bank reconnect reconciliation"
        );
        Ok(())
    }
}

pub(super) fn merge_stop_order_snapshots(
    mut primary: GetStopOrdersResponse,
    secondary: GetStopOrdersResponse,
) -> GetStopOrdersResponse {
    let mut indexes = primary
        .stop_orders
        .iter()
        .enumerate()
        .map(|(index, stop)| (stop.stop_order_id.clone(), index))
        .collect::<HashMap<_, _>>();
    for stop in secondary.stop_orders {
        if let Some(index) = indexes.get(stop.stop_order_id.as_str()).copied() {
            primary.stop_orders[index] = stop;
        } else {
            indexes.insert(stop.stop_order_id.clone(), primary.stop_orders.len());
            primary.stop_orders.push(stop);
        }
    }
    primary
}

pub(super) fn reconnect_reconciliation_error_is_transient(error: &anyhow::Error) -> bool {
    if let Some(error) = error.downcast_ref::<TbankAdapterError>() {
        return tbank_adapter_error_is_transient(error);
    }
    error
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| crate::grpc::retry::is_transient_status(status.code()))
}

pub(super) fn tbank_adapter_error_is_transient(error: &TbankAdapterError) -> bool {
    match error {
        TbankAdapterError::RateLimited(_) => true,
        TbankAdapterError::GrpcStatus { code, .. } => {
            crate::grpc::retry::is_transient_status(*code)
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TbankReconnectReconciliationOutcome {
    Completed,
    Degraded,
    Permanent,
}

pub(super) fn apply_reconnect_reconciliation_outcome(
    recovery_from: &mut Option<i128>,
    attempted_from: i128,
    outcome: TbankReconnectReconciliationOutcome,
) -> bool {
    if outcome == TbankReconnectReconciliationOutcome::Completed {
        *recovery_from = None;
        true
    } else {
        *recovery_from = Some(attempted_from);
        false
    }
}

pub(super) async fn reconcile_after_stream_reopen(
    reconciler: &TbankReconnectReconciler,
    reason: &str,
    from_unix_nanos: i128,
    reconnect_policy: &crate::config::TbankReconnectPolicy,
) -> TbankReconnectReconciliationOutcome {
    let mut attempt = 0_u32;
    loop {
        match reconciler.reconcile(reason, from_unix_nanos).await {
            Ok(()) => {
                return TbankReconnectReconciliationOutcome::Completed;
            }
            Err(error) => {
                if !reconnect_reconciliation_error_is_transient(&error) {
                    tracing::error!(
                        %error,
                        reason,
                        "T-Bank reconnect reconciliation failed permanently; consuming reopened stream in degraded state"
                    );
                    return TbankReconnectReconciliationOutcome::Permanent;
                }
                attempt = attempt.saturating_add(1);
                if attempt >= RECONNECT_RECONCILIATION_MAX_ATTEMPTS {
                    tracing::error!(
                        %error,
                        reason,
                        attempts = attempt,
                        "T-Bank reconnect reconciliation retry budget exhausted; consuming reopened stream in degraded state"
                    );
                    return TbankReconnectReconciliationOutcome::Degraded;
                }
                let delay = crate::grpc::retry::backoff_duration(
                    reconnect_policy,
                    attempt.saturating_sub(1),
                );
                tracing::warn!(
                    %error,
                    reason,
                    delay_ms = delay.as_millis(),
                    "T-Bank reconnect reconciliation failed; retrying before consuming reopened stream"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

pub(super) async fn reconcile_degraded_stream_until_complete(
    reconciler: &TbankReconnectReconciler,
    reason: &str,
    from_unix_nanos: i128,
    reconnect_policy: &crate::config::TbankReconnectPolicy,
) {
    loop {
        match reconcile_after_stream_reopen(reconciler, reason, from_unix_nanos, reconnect_policy)
            .await
        {
            TbankReconnectReconciliationOutcome::Completed
            | TbankReconnectReconciliationOutcome::Permanent => return,
            TbankReconnectReconciliationOutcome::Degraded => {}
        }
        let delay = crate::grpc::retry::backoff_duration(
            reconnect_policy,
            RECONNECT_RECONCILIATION_MAX_ATTEMPTS.saturating_sub(1),
        );
        tracing::warn!(
            reason,
            delay_ms = delay.as_millis(),
            "T-Bank reconnect reconciliation remains degraded; retrying while stream stays open"
        );
        tokio::time::sleep(delay).await;
    }
}

pub(super) async fn publish_reconnect_reconciliation(
    client: &TbankExecutionRuntime,
    emitter: &ExecutionEventEmitter,
    from_unix_nanos: i128,
) -> anyhow::Result<TbankReconnectReconciliationCounts> {
    let ts_init = current_unix_nanos();
    let mut query_client = client.detached_query_clone();

    let mut order_states = query_client
        .query_orders_since(from_unix_nanos)
        .await?
        .orders;
    let mut known_order_ids = order_states
        .iter()
        .map(|state| state.order_id.clone())
        .collect::<HashSet<_>>();
    for order_id in query_client.known_regular_broker_order_ids() {
        if known_order_ids.contains(order_id.as_str()) {
            continue;
        }
        match TbankExecutionRuntime::query_order(&mut query_client, order_id.as_str()).await {
            Ok(state) => {
                known_order_ids.insert(state.order_id.clone());
                order_states.push(state);
            }
            Err(TbankAdapterError::GrpcStatus {
                code: tonic::Code::NotFound,
                message,
            }) => tracing::debug!(
                %message,
                %order_id,
                "known T-Bank order was absent during reconnect reconciliation"
            ),
            Err(error) => return Err(error.into()),
        }
    }
    let mut known_request_ids = order_states
        .iter()
        .map(|state| state.order_request_id.clone())
        .collect::<HashSet<_>>();
    for (client_order_id, broker_request_id) in query_client.unresolved_regular_request_mappings() {
        if known_request_ids.contains(broker_request_id.as_str()) {
            continue;
        }
        match query_client
            .query_order_by_request_id(broker_request_id.as_str())
            .await
        {
            Ok(state) => {
                known_request_ids.insert(state.order_request_id.clone());
                query_client
                    .record_broker_order_mapping_and_drain_cancel(
                        TbankBrokerOrderRoute::RegularOrder,
                        client_order_id.as_str(),
                        state.order_id.as_str(),
                    )
                    .await;
                order_states.push(state);
            }
            Err(TbankAdapterError::GrpcStatus {
                code: tonic::Code::NotFound,
                message,
            }) => tracing::debug!(
                %message,
                %client_order_id,
                %broker_request_id,
                "persisted T-Bank request id was absent during reconnect reconciliation"
            ),
            Err(error) => return Err(error.into()),
        }
    }
    let mut stops = query_client
        .query_stop_orders_for_reconciliation(Some(from_unix_nanos))
        .await?
        .stop_orders;
    let known_stop_ids = query_client
        .known_stop_broker_order_ids()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut returned_stop_ids = stops
        .iter()
        .map(|stop| stop.stop_order_id.clone())
        .collect::<HashSet<_>>();
    if known_stop_ids
        .iter()
        .any(|stop_id| !returned_stop_ids.contains(stop_id))
    {
        let historical = query_client
            .query_stop_orders_for_reconciliation(None)
            .await?
            .stop_orders;
        for stop in historical {
            if known_stop_ids.contains(stop.stop_order_id.as_str())
                && returned_stop_ids.insert(stop.stop_order_id.clone())
            {
                stops.push(stop);
            }
        }
    }
    query_client
        .append_missing_activated_stop_children(&mut order_states, &stops)
        .await?;

    let stop_by_id = stops
        .iter()
        .cloned()
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
            query_client
                .broker_order_index
                .lock()
                .expect("broker_order_index lock")
                .client_order_id_for_venue_order_id(stop.stop_order_id.as_str())
                .map(|client_order_id| (stop.stop_order_id.clone(), client_order_id))
        })
        .collect::<HashMap<_, _>>();

    let mut activated_stop_ids = HashSet::new();
    let mut order_reports = Vec::with_capacity(order_states.len() + stops.len());
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
            let lot_size = query_client.lot_size_for_stop_order(stop).await?;
            query_client.record_broker_order_id(
                TbankBrokerOrderRoute::StopOrder,
                stop.stop_order_id.as_str(),
            );
            if let Some(client_order_id) = stop_client_order_ids.get(stop_id.as_str()) {
                query_client.record_stop_order_context(client_order_id, stop, lot_size);
            }
            if !state.order_id.is_empty() {
                query_client.record_activated_stop_child_mapping(
                    stop_client_order_ids
                        .get(stop_id.as_str())
                        .map(String::as_str)
                        .unwrap_or(""),
                    stop.stop_order_id.as_str(),
                    state.order_id.as_str(),
                );
            }
            match activated_stop_child_status_report(
                query_client.account_id(),
                stop,
                &state,
                ts_init,
                lot_size,
                stop_client_order_ids
                    .get(stop_id.as_str())
                    .map(String::as_str),
            ) {
                Ok(report) => order_reports.push(report),
                Err(error) => tracing::warn!(
                    %error,
                    %stop_id,
                    "skipping malformed activated T-Bank stop child during reconnect reconciliation"
                ),
            }
            continue;
        }
        match query_client
            .order_status_report_from_state_with_lots(query_client.account_id(), state, ts_init)
            .await
        {
            Ok(report) => order_reports.push(report),
            Err(error) if reconnect_reconciliation_error_is_transient(&error) => {
                return Err(error);
            }
            Err(error) => {
                tracing::warn!(%error, "skipping malformed T-Bank order during reconnect reconciliation")
            }
        }
    }

    for stop in stops {
        if activated_stop_ids.contains(stop.stop_order_id.as_str()) {
            continue;
        }
        let client_order_id = query_client
            .broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .client_order_id_for_venue_order_id(stop.stop_order_id.as_str());
        query_client.record_broker_order_id(
            TbankBrokerOrderRoute::StopOrder,
            stop.stop_order_id.as_str(),
        );
        let lot_size = query_client.lot_size_for_stop_order(&stop).await?;
        if let Some(client_order_id) = client_order_id.as_deref() {
            query_client.record_stop_order_context(client_order_id, &stop, lot_size);
        }
        match stop_order_status_report(query_client.account_id(), stop, ts_init, lot_size) {
            Ok(mut report) => {
                report.client_order_id = client_order_id.map(Into::into);
                order_reports.push(report);
            }
            Err(error) => {
                tracing::warn!(%error, "skipping malformed T-Bank stop order during reconnect reconciliation")
            }
        }
    }

    let operations = query_client
        .query_fills(None, Some(from_unix_nanos), None)
        .await?;
    query_client.ensure_lifecycle_active()?;
    let mut fill_reports = Vec::new();
    for item in &operations.items {
        for report in fill_reports_from_operation(query_client.account_id(), item, ts_init) {
            let report = report.map(|report| {
                let source_identity = (
                    report.venue_order_id.to_string(),
                    report.trade_id.to_string(),
                );
                let report = canonicalize_reconciled_stop_fill(
                    &query_client,
                    report,
                    &stop_id_by_exchange_order_id,
                    &stop_client_order_ids,
                );
                (source_identity, report)
            });
            match report.and_then(|(source_identity, report)| {
                project_and_settle_reconciled_trade_fill(
                    &query_client,
                    report,
                    &source_identity.0,
                    &source_identity.1,
                )
            }) {
                Ok(report) => {
                    if let Some(report) = report {
                        fill_reports.push(report);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed T-Bank fill during reconnect reconciliation")
                }
            }
        }
    }

    let mut counts = TbankReconnectReconciliationCounts::default();
    for report in order_reports {
        if query_client
            .lifecycle_active
            .run_if_active(|| {
                settle_order_report_mutation_state(
                    &query_client.pending_submits,
                    &query_client.unresolved_cancellations,
                    &query_client.broker_order_index,
                    &report,
                );
                if let Some(report) =
                    project_order_status_report(&query_client.order_status_projection, report)
                {
                    emitter.send_order_status_report(report);
                    counts.orders += 1;
                }
            })
            .is_none()
        {
            return Ok(counts);
        }
    }
    for report in fill_reports {
        if query_client
            .lifecycle_active
            .run_if_active(|| {
                emitter.send_fill_report(report);
                counts.fills += 1;
            })
            .is_none()
        {
            return Ok(counts);
        }
    }
    Ok(counts)
}

pub(super) fn project_and_settle_reconciled_trade_fill(
    query_client: &TbankExecutionRuntime,
    report: FillReport,
    source_venue_order_id: &str,
    source_trade_id: &str,
) -> anyhow::Result<Option<FillReport>> {
    query_client
        .lifecycle_active
        .run_if_active(|| {
            let report = query_client.project_trade_fill_report(report)?;
            settle_reconciled_buffered_trade_fill(
                &query_client.unresolved_trade_fills,
                source_venue_order_id,
                source_trade_id,
            );
            Ok(report)
        })
        .ok_or_else(|| anyhow::anyhow!("T-Bank execution lifecycle is no longer active"))?
}

pub(super) fn settle_reconciled_buffered_trade_fill(
    unresolved_trade_fills: &Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    venue_order_id: &str,
    trade_id: &str,
) {
    let mut buffered = unresolved_trade_fills
        .lock()
        .expect("unresolved_trade_fills lock");
    let should_remove = if let Some(reports) = buffered.get_mut(venue_order_id) {
        reports.retain(|report| report.trade_id.to_string() != trade_id);
        reports.is_empty()
    } else {
        false
    };
    if should_remove {
        buffered.remove(venue_order_id);
    }
}

pub(super) fn canonicalize_reconciled_stop_fill(
    client: &TbankExecutionRuntime,
    mut report: FillReport,
    stop_id_by_exchange_order_id: &HashMap<String, String>,
    stop_client_order_ids: &HashMap<String, String>,
) -> FillReport {
    let child_order_id = report.venue_order_id.to_string();
    let Some(stop_order_id) = stop_id_by_exchange_order_id.get(child_order_id.as_str()) else {
        return report;
    };
    let client_order_id = stop_client_order_ids.get(stop_order_id.as_str());
    client.record_activated_stop_child_mapping(
        client_order_id.map(String::as_str).unwrap_or(""),
        stop_order_id,
        child_order_id.as_str(),
    );
    if let Some(client_order_id) = client_order_id {
        report.client_order_id = Some(client_order_id.as_str().into());
    }
    report.venue_order_id = stop_order_id.as_str().into();
    report
}

pub(super) fn schedule_regular_order_reconciliation(
    context: TbankOrderStreamContext,
    client_order_id: Option<String>,
    broker_request_id: String,
) {
    if broker_request_id.is_empty() {
        return;
    }
    {
        let mut pending = context
            .regular_order_reconciliations
            .lock()
            .expect("regular_order_reconciliations lock");
        if !pending.insert(broker_request_id.clone()) {
            return;
        }
    }
    let reconciliation_tasks = context.reconciliation_tasks.clone();
    let mut tasks = reconciliation_tasks
        .lock()
        .expect("reconciliation_tasks lock");
    tasks.retain(|task| !task.is_finished());
    let task = get_runtime().spawn(async move {
        let mut query_client = context.query_client.detached_query_clone();
        let mut attempt = 0_u32;
        loop {
            match query_client
                .reconcile_order_by_request_id(broker_request_id.as_str(), current_unix_nanos())
                .await
            {
                Ok(Some(reports)) => {
                    if !context.is_active() {
                        break;
                    }
                    let reconciled_venue_order_id = reports.order_report.venue_order_id.to_string();
                    context.run_if_active(|| {
                        if let Some(order_report) = project_order_status_report(
                            &context.order_status_projection,
                            reports.order_report,
                        ) {
                            context.emitter.send_order_status_report(order_report);
                        }
                    });
                    for fill_report in reports.fill_reports {
                        context.run_if_active(|| {
                            context.emitter.send_fill_report(fill_report);
                        });
                    }
                    let current_order_id = context
                        .broker_order_index
                        .lock()
                        .expect("broker_order_index lock")
                        .identity_for(client_order_id.as_deref(), None)
                        .and_then(|identity| identity.broker_order_id)
                        .unwrap_or(reconciled_venue_order_id);
                    if let Err(error) = publish_buffered_trade_fills_for_venue(
                        current_order_id.as_str(),
                        &context.emitter,
                        &context.broker_order_index,
                        &context.fill_projection,
                        &context.pending_submits,
                        &context.unresolved_trade_fills,
                        &context.lifecycle_active,
                    ) {
                        tracing::warn!(
                            %error,
                            client_order_id = client_order_id.as_deref().unwrap_or(""),
                            %current_order_id,
                            "failed to publish buffered fill after regular-order reconciliation"
                        );
                    }
                    break;
                }
                Ok(None) => {}
                Err(error) if !reconnect_reconciliation_error_is_transient(&error) => {
                    tracing::error!(
                        %error,
                        client_order_id = client_order_id.as_deref().unwrap_or(""),
                        %broker_request_id,
                        "regular T-Bank order reconciliation failed permanently"
                    );
                    break;
                }
                Err(error) => tracing::warn!(
                    %error,
                    client_order_id = client_order_id.as_deref().unwrap_or(""),
                    %broker_request_id,
                    attempt,
                    "regular T-Bank order reconciliation failed transiently"
                ),
            }
            attempt = attempt.saturating_add(1);
            if attempt >= SUBMIT_OUTCOME_RECOVERY_ATTEMPTS {
                tracing::error!(
                    client_order_id = client_order_id.as_deref().unwrap_or(""),
                    %broker_request_id,
                    attempts = attempt,
                    "regular T-Bank order reconciliation retry budget exhausted"
                );
                break;
            }
            tokio::time::sleep(crate::grpc::retry::backoff_duration(
                &context.reconnect_policy,
                attempt.saturating_sub(1),
            ))
            .await;
        }
        context
            .regular_order_reconciliations
            .lock()
            .expect("regular_order_reconciliations lock")
            .remove(broker_request_id.as_str());
    });
    tasks.push(task);
}

pub(super) fn schedule_unresolved_trade_reconciliation(
    context: TbankOrderStreamContext,
    venue_order_id: String,
) {
    if venue_order_id.is_empty() {
        return;
    }
    let reconciliation_key = format!("exchange:{venue_order_id}");
    {
        let mut pending = context
            .regular_order_reconciliations
            .lock()
            .expect("regular_order_reconciliations lock");
        if !pending.insert(reconciliation_key.clone()) {
            return;
        }
    }
    let reconciliation_tasks = context.reconciliation_tasks.clone();
    let mut tasks = reconciliation_tasks
        .lock()
        .expect("reconciliation_tasks lock");
    tasks.retain(|task| !task.is_finished());
    let task = get_runtime().spawn(async move {
        let mut query_client = context.query_client.detached_query_clone();
        let mut attempt = 0_u32;
        let mut latest_order_state: Option<OrderState> = None;
        loop {
            if finish_unresolved_trade_reconciliation_if_idle(
                &context.unresolved_trade_fills,
                &context.regular_order_reconciliations,
                reconciliation_key.as_str(),
                venue_order_id.as_str(),
            ) {
                return;
            }
            let mut permanently_unresolvable = false;
            match TbankExecutionRuntime::query_order(&mut query_client, venue_order_id.as_str())
                .await
            {
                Ok(state) => {
                    latest_order_state = Some(state.clone());
                    let ts_init = current_unix_nanos();
                    let report = match query_client
                        .resolve_activated_stop_mapping(
                            Some(state.order_request_id.as_str()),
                            venue_order_id.as_str(),
                        )
                        .await
                    {
                        Ok(Some((stop, client_order_id))) => {
                            let report = match query_client.lot_size_for_stop_order(&stop).await {
                                Ok(lot_size) => {
                                    if let Some(client_order_id) = client_order_id.as_deref() {
                                        query_client.record_stop_order_context(
                                            client_order_id,
                                            &stop,
                                            lot_size,
                                        );
                                    }
                                    activated_stop_child_status_report(
                                        query_client.account_id(),
                                        &stop,
                                        &state,
                                        ts_init,
                                        lot_size,
                                        client_order_id.as_deref(),
                                    )
                                    .map(|report| (report, true))
                                }
                                Err(error) => Err(anyhow::Error::from(error)),
                            };
                            Some(report)
                        }
                        Ok(None) => {
                            let known_regular_route = {
                                let index = context
                                    .broker_order_index
                                    .lock()
                                    .expect("broker_order_index lock");
                                index.is_known_regular_order_request_id(
                                    state.order_request_id.as_str(),
                                )
                            };
                            if known_regular_route {
                                Some(
                                    query_client
                                        .order_status_report_from_state_with_lots(
                                            query_client.account_id(),
                                            state,
                                            ts_init,
                                        )
                                        .await
                                        .map(|report| (report, false)),
                                )
                            } else {
                                None
                            }
                        }
                        Err(error) => Some(Err(error)),
                    };
                    match report {
                        Some(Ok((report, activated_stop))) => {
                            if !activated_stop
                                && let Some(client_order_id) = report.client_order_id
                            {
                                query_client
                                    .record_broker_order_mapping_and_drain_cancel(
                                        TbankBrokerOrderRoute::RegularOrder,
                                        client_order_id.as_str(),
                                        venue_order_id.as_str(),
                                    )
                                    .await;
                            } else if !activated_stop {
                                let mut broker_order_index = context
                                    .broker_order_index
                                    .lock()
                                    .expect("broker_order_index lock");
                                if broker_order_index
                                    .identity_for(None, Some(venue_order_id.as_str()))
                                    .is_none()
                                {
                                    broker_order_index.record_venue_order_id(
                                        TbankBrokerOrderRoute::RegularOrder,
                                        venue_order_id.as_str(),
                                    );
                                }
                            }
                            context.run_if_active(|| {
                                if let Some(report) = project_order_status_report(
                                    &context.order_status_projection,
                                    report,
                                ) {
                                    context.emitter.send_order_status_report(report);
                                }
                            });
                            match publish_buffered_trade_fills_for_venue(
                                venue_order_id.as_str(),
                                &context.emitter,
                                &context.broker_order_index,
                                &context.fill_projection,
                                &context.pending_submits,
                                &context.unresolved_trade_fills,
                                &context.lifecycle_active,
                            ) {
                                Ok(_) => {
                                    if finish_unresolved_trade_reconciliation_if_idle(
                                        &context.unresolved_trade_fills,
                                        &context.regular_order_reconciliations,
                                        reconciliation_key.as_str(),
                                        venue_order_id.as_str(),
                                    ) {
                                        return;
                                    }
                                    continue;
                                }
                                Err(error) => tracing::warn!(
                                    %error,
                                    "failed to publish buffered fill after exchange-order reconciliation"
                                ),
                            }
                        }
                        None => tracing::debug!(
                            attempt,
                            "T-Bank order is not yet visible as an activated stop child"
                        ),
                        Some(Err(error)) if reconnect_reconciliation_error_is_transient(&error) => {
                            tracing::warn!(%error, attempt, "transient T-Bank exchange-order mapping failure");
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, "T-Bank exchange-order mapping failed; retaining buffered fills for durable retry");
                            permanently_unresolvable = true;
                        }
                    }
                }
                Err(error) if tbank_adapter_error_is_transient(&error) => {
                    tracing::warn!(%error, attempt, "transient unresolved T-Bank trade lookup failure");
                }
                Err(TbankAdapterError::GrpcStatus {
                    code: tonic::Code::NotFound,
                    ..
                }) => {}
                Err(error) => {
                    tracing::error!(%error, "unresolved T-Bank trade lookup failed; retaining buffered fills for durable retry");
                    permanently_unresolvable = true;
                }
            }
            attempt = attempt.saturating_add(1);
            if permanently_unresolvable || attempt >= SUBMIT_OUTCOME_RECOVERY_ATTEMPTS {
                tracing::error!(
                    attempts = attempt,
                    "publishing unresolved T-Bank fill with external regular-order identity after lookup exhaustion"
                );
                let activated_stop = match query_client
                    .resolve_activated_stop_mapping(None, venue_order_id.as_str())
                    .await
                {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(error) if reconnect_reconciliation_error_is_transient(&error) => {
                        tracing::warn!(
                            %error,
                            "could not exclude activated-stop route before unresolved fill fallback"
                        );
                        tokio::time::sleep(crate::grpc::retry::backoff_duration(
                            &context.reconnect_policy,
                            attempt.saturating_sub(1),
                        ))
                        .await;
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            "permanent StopOrders lookup failure; publishing buffered fill with external regular-order identity"
                        );
                        false
                    }
                };
                if !activated_stop {
                    let mut mapped_client_order_id = None;
                    if let Some(state) = latest_order_state.clone() {
                        match query_client
                            .order_status_report_from_state_with_lots(
                                query_client.account_id(),
                                state,
                                current_unix_nanos(),
                            )
                            .await
                        {
                            Ok(report) => {
                                mapped_client_order_id = report.client_order_id;
                                context.run_if_active(|| {
                                    if let Some(report) = project_order_status_report(
                                        &context.order_status_projection,
                                        report,
                                    ) {
                                        context.emitter.send_order_status_report(report);
                                    }
                                });
                            }
                            Err(error) => tracing::warn!(
                                %error,
                                "could not build regular-order status report during unresolved fill fallback"
                            ),
                        }
                    }
                    if let Some(client_order_id) = mapped_client_order_id {
                        query_client
                            .record_broker_order_mapping_and_drain_cancel(
                                TbankBrokerOrderRoute::RegularOrder,
                                client_order_id.as_str(),
                                venue_order_id.as_str(),
                            )
                            .await;
                    }
                }
                let mut broker_order_index = context
                    .broker_order_index
                    .lock()
                    .expect("broker_order_index lock");
                if broker_order_index
                    .identity_for(None, Some(venue_order_id.as_str()))
                    .is_none()
                {
                    broker_order_index.record_venue_order_id(
                        TbankBrokerOrderRoute::RegularOrder,
                        venue_order_id.as_str(),
                    );
                }
                drop(broker_order_index);
                match publish_buffered_trade_fills_for_venue(
                    venue_order_id.as_str(),
                    &context.emitter,
                    &context.broker_order_index,
                    &context.fill_projection,
                    &context.pending_submits,
                    &context.unresolved_trade_fills,
                    &context.lifecycle_active,
                ) {
                    Ok(_) => {
                        if finish_unresolved_trade_reconciliation_if_idle(
                            &context.unresolved_trade_fills,
                            &context.regular_order_reconciliations,
                            reconciliation_key.as_str(),
                            venue_order_id.as_str(),
                        ) {
                            return;
                        }
                        continue;
                    }
                    Err(error) => tracing::error!(
                        %error,
                        "failed to publish unresolved T-Bank fill fallback"
                    ),
                }
            }
            tokio::time::sleep(crate::grpc::retry::backoff_duration(
                &context.reconnect_policy,
                attempt.saturating_sub(1),
            ))
            .await;
        }
    });
    tasks.push(task);
}

pub(super) fn finish_unresolved_trade_reconciliation_if_idle(
    unresolved_trade_fills: &Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    regular_order_reconciliations: &Arc<Mutex<HashSet<String>>>,
    reconciliation_key: &str,
    venue_order_id: &str,
) -> bool {
    let buffered = unresolved_trade_fills
        .lock()
        .expect("unresolved_trade_fills lock");
    if buffered.contains_key(venue_order_id) {
        return false;
    }
    regular_order_reconciliations
        .lock()
        .expect("regular_order_reconciliations lock")
        .remove(reconciliation_key);
    true
}

pub(super) fn schedule_activated_stop_child_reconciliation(
    context: TbankOrderStreamContext,
    client_order_id: Option<String>,
    stop_order_id: String,
) {
    if stop_order_id.is_empty() {
        return;
    }
    {
        let mut pending = context
            .activated_stop_reconciliations
            .lock()
            .expect("activated_stop_reconciliations lock");
        if !pending.insert(stop_order_id.clone()) {
            return;
        }
    }
    let reconciliation_tasks = context.reconciliation_tasks.clone();
    let mut tasks = reconciliation_tasks
        .lock()
        .expect("reconciliation_tasks lock");
    tasks.retain(|task| !task.is_finished());
    let task = get_runtime().spawn(async move {
        let mut query_client = context.query_client.detached_query_clone();
        let mut attempt = 0_u32;
        loop {
            let result = query_client
                .stop_order_status_report_for_known_id(
                    client_order_id.as_deref().map(Into::into),
                    stop_order_id.clone(),
                    current_unix_nanos(),
                )
                .await;
            let child_resolved = context
                .broker_order_index
                .lock()
                .expect("broker_order_index lock")
                .has_activated_stop_child_mapping(stop_order_id.as_str());
            match result {
                Ok(Some(report)) if child_resolved => {
                    if !context.is_active() {
                        break;
                    }
                    context.run_if_active(|| {
                        if let Some(report) =
                            project_order_status_report(&context.order_status_projection, report)
                        {
                            context.emitter.send_order_status_report(report);
                        }
                    });
                    let child_aliases = context
                        .broker_order_index
                        .lock()
                        .expect("broker_order_index lock")
                        .aliases_for_canonical_venue_order_id(stop_order_id.as_str());
                    for child_order_id in child_aliases {
                        if let Err(error) = publish_buffered_trade_fills_for_venue(
                            child_order_id.as_str(),
                            &context.emitter,
                            &context.broker_order_index,
                            &context.fill_projection,
                            &context.pending_submits,
                            &context.unresolved_trade_fills,
                            &context.lifecycle_active,
                        ) {
                            tracing::warn!(
                                %error,
                                "failed to publish buffered activated-stop fill"
                            );
                        }
                    }
                    break;
                }
                Ok(_) => {}
                Err(error) if !reconnect_reconciliation_error_is_transient(&error) => {
                    tracing::error!(
                        %error,
                        "activated T-Bank stop child reconciliation failed permanently"
                    );
                    break;
                }
                Err(error) => tracing::warn!(
                    %error,
                    attempt,
                    "activated T-Bank stop child reconciliation failed transiently"
                ),
            }
            attempt = attempt.saturating_add(1);
            if attempt >= SUBMIT_OUTCOME_RECOVERY_ATTEMPTS {
                tracing::error!(
                    attempts = attempt,
                    "activated T-Bank stop child reconciliation retry budget exhausted"
                );
                break;
            }
            tokio::time::sleep(crate::grpc::retry::backoff_duration(
                &context.reconnect_policy,
                attempt.saturating_sub(1),
            ))
            .await;
        }
        context
            .activated_stop_reconciliations
            .lock()
            .expect("activated_stop_reconciliations lock")
            .remove(stop_order_id.as_str());
    });
    tasks.push(task);
}

pub(super) fn publish_buffered_trade_fills_for_venue(
    venue_order_id: &str,
    emitter: &ExecutionEventEmitter,
    broker_order_index: &Arc<Mutex<TbankBrokerOrderIndex>>,
    fill_projection: &Arc<Mutex<TbankFillProjection>>,
    pending_submits: &Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    unresolved_trade_fills: &Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    lifecycle_active: &Arc<TbankLifecycleToken>,
) -> anyhow::Result<usize> {
    lifecycle_active
        .run_if_active(|| {
            let reports = unresolved_trade_fills
                .lock()
                .expect("unresolved_trade_fills lock")
                .remove(venue_order_id)
                .unwrap_or_default();
            let mut reports = reports.into_iter();
            let mut published = 0_usize;
            while let Some(raw_report) = reports.next() {
                let projected = match project_managed_trade_fill_report(
                    broker_order_index,
                    fill_projection,
                    raw_report.clone(),
                ) {
                    Ok(projected) => projected,
                    Err(error) => {
                        let mut unprocessed = vec![raw_report];
                        unprocessed.extend(reports);
                        restore_unprocessed_trade_fills(
                            unresolved_trade_fills,
                            venue_order_id,
                            unprocessed,
                        );
                        return Err(error);
                    }
                };
                if let Some(report) = projected {
                    emitter.send_fill_report(report.clone());
                    mark_pending_submit_fill_report(pending_submits, &report);
                    published = published.saturating_add(1);
                }
            }
            Ok(published)
        })
        .unwrap_or(Ok(0))
}

pub(super) fn restore_unprocessed_trade_fills(
    unresolved_trade_fills: &Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    venue_order_id: &str,
    mut unprocessed: Vec<FillReport>,
) {
    let mut pending = unresolved_trade_fills
        .lock()
        .expect("unresolved_trade_fills lock");
    if let Some(mut appended) = pending.remove(venue_order_id) {
        unprocessed.append(&mut appended);
    }
    pending.insert(venue_order_id.to_string(), unprocessed);
}

pub(super) fn buffer_unresolved_trade_fill(
    unresolved_trade_fills: &Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    venue_order_id: String,
    report: FillReport,
) -> bool {
    let mut pending = unresolved_trade_fills
        .lock()
        .expect("unresolved_trade_fills lock");
    let trade_id = report.trade_id.to_string();
    let reports = pending.entry(venue_order_id.clone()).or_default();
    if reports
        .iter()
        .any(|buffered| buffered.trade_id.to_string() == trade_id)
    {
        return false;
    }
    let per_order_pressure = reports.len() >= MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER;
    reports.push(report);
    let buffered_for_order = reports.len();
    let total = pending.values().map(Vec::len).sum::<usize>();
    let global_pressure = total > MAX_UNRESOLVED_TRADE_FILLS;
    if per_order_pressure || global_pressure {
        tracing::error!(
            total_unresolved_fills = total,
            max_unresolved_fills = MAX_UNRESOLVED_TRADE_FILLS,
            %venue_order_id,
            buffered_for_order,
            max_per_order = MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER,
            "pausing T-Bank trades stream after unresolved-fill buffer pressure"
        );
    }
    per_order_pressure || global_pressure
}
