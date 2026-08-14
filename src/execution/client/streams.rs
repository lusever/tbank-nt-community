//! T-Bank execution stream consumers.

use super::*;

pub(super) async fn publish_order_state_stream(
    mut stream: tonic::Streaming<OrderStateStreamResponse>,
    context: TbankOrderStreamContext,
    _stream_generation: u64,
    _reconnect_recovery_pending: Arc<AtomicBool>,
    last_observed_unix_nanos: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                if !context.is_active() {
                    return Ok(());
                }
                match response.payload {
                    Some(order_state_stream_response::Payload::OrderState(state)) => {
                        last_observed_unix_nanos
                            .store(current_unix_nanos().as_u64(), Ordering::Release);
                        let mut metadata_client = context.query_client.detached_query_clone();
                        match warm_instrument_metadata(
                            &mut metadata_client,
                            &state.instrument_uid,
                            "",
                            &state.ticker,
                            &state.class_code,
                            "order_state_stream",
                        )
                        .await?
                        {
                            TbankInstrumentMetadataResolution::Enabled => {}
                            TbankInstrumentMetadataResolution::OutOfScope => continue,
                            TbankInstrumentMetadataResolution::Rejected => {
                                tracing::warn!(
                                    "skipping malformed T-Bank order-state event without a trustworthy instrument identity"
                                );
                                continue;
                            }
                        }
                        let client_order_id = stream_order_state_client_order_id(
                            &context.broker_order_index,
                            state.order_request_id.as_deref(),
                            state.order_id.as_str(),
                            state.trade_order_id.as_str(),
                        );
                        let managed_context = client_order_id.as_deref().and_then(|client_id| {
                            context
                                .broker_order_index
                                .lock()
                                .expect("broker_order_index lock")
                                .managed_context_for_client_order_id(client_id)
                        });
                        let managed_time_in_force = managed_context
                            .as_ref()
                            .and_then(|managed| managed.time_in_force);
                        let activated_from_stop = context
                            .broker_order_index
                            .lock()
                            .expect("broker_order_index lock")
                            .identity_for(None, Some(state.trade_order_id.as_str()))
                            .is_some_and(|identity| {
                                identity.route == TbankBrokerOrderRoute::StopOrder
                            });
                        let resolved_identity = resolve_stream_order_venue_id(
                            &context.broker_order_index,
                            &context.fill_projection,
                            state.order_request_id.as_deref(),
                            state.order_id.as_str(),
                            state.trade_order_id.as_str(),
                            state.execution_report_status,
                        );
                        let Some(resolved_identity) = resolved_identity else {
                            if activated_from_stop {
                                schedule_activated_stop_child_reconciliation(
                                    context.clone(),
                                    client_order_id.clone(),
                                    state.trade_order_id.clone(),
                                );
                            } else if let Some(broker_request_id) = state
                                .order_request_id
                                .as_deref()
                                .filter(|value| !value.is_empty())
                            {
                                schedule_regular_order_reconciliation(
                                    context.clone(),
                                    client_order_id.clone(),
                                    broker_request_id.to_string(),
                                );
                            }
                            tracing::debug!(
                                "deferred T-Bank initial order-state ack until exchange order id is known"
                            );
                            continue;
                        };
                        if let Some(pending_cancel) = resolved_identity.pending_cancel {
                            let mut cancel_client = context.query_client.detached_query_clone();
                            if !context.query_client.spawn_mutating_followup_task_if_active(
                                async move {
                                    match cancel_client
                                        .cancel_resolved_broker_order(pending_cancel.clone())
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error)
                                            if classify_cancel_failure(&error)
                                                == CancelFailureKind::OutcomeUnknown =>
                                        {
                                            match cancel_client
                                                .recover_ambiguous_cancel(pending_cancel)
                                                .await
                                            {
                                                Ok(TbankCancelRecoveryOutcome::Canceled) => {}
                                                Ok(TbankCancelRecoveryOutcome::Active) => {
                                                    tracing::warn!(
                                                        "deferred T-Bank cancel reconciliation confirmed the order remains active"
                                                    );
                                                }
                                                Err(recovery_error) => tracing::warn!(
                                                    %recovery_error,
                                                    "deferred T-Bank cancel outcome remained unresolved"
                                                ),
                                            }
                                        }
                                        Err(error) => tracing::error!(
                                            %error,
                                            "failed to drain pending T-Bank cancel after stream identity resolution"
                                        ),
                                    }
                                },
                            ) {
                                return Ok(());
                            }
                        }
                        let venue_order_id = resolved_identity.venue_order_id;
                        let current_broker_order_id = state.order_id.clone();
                        match stream_order_status_report_from_state_with_instruments(
                            state,
                            venue_order_id.as_str(),
                            current_unix_nanos(),
                            client_order_id.as_deref(),
                            managed_time_in_force,
                            Some(&context.instruments),
                        ) {
                            Ok(mut report) => {
                                if activated_from_stop {
                                    report.order_type = managed_context
                                        .as_ref()
                                        .map(nautilus_stream_stop_order_type_from_context)
                                        .unwrap_or(OrderType::StopMarket);
                                    report.time_in_force = TimeInForce::Gtc;
                                    report.order_status =
                                        activated_stop_child_status(report.order_status);
                                    match apply_trailing_params(
                                        report,
                                        managed_context.as_ref().and_then(|value| value.trailing),
                                    ) {
                                        Ok(updated) => report = updated,
                                        Err(error) => {
                                            tracing::error!(%error, "failed to preserve trailing-stop context on activated child report");
                                            continue;
                                        }
                                    }
                                    report = with_default_stop_trigger_type(report);
                                }
                                context.run_if_active(|| {
                                    settle_order_report_mutation_state(
                                        &context.pending_submits,
                                        &context.unresolved_cancellations,
                                        &context.broker_order_index,
                                        &report,
                                    );
                                    if let Some(report) = project_order_status_report(
                                        &context.order_status_projection,
                                        report,
                                    ) {
                                        context.emitter.send_order_status_report(report);
                                    }
                                });
                                if context.is_active()
                                    && let Err(error) = publish_buffered_trade_fills_for_venue(
                                        current_broker_order_id.as_str(),
                                        &context.emitter,
                                        &context.broker_order_index,
                                        &context.fill_projection,
                                        &context.pending_submits,
                                        &context.unresolved_trade_fills,
                                        &context.lifecycle_active,
                                    )
                                {
                                    tracing::warn!(
                                        %error,
                                        "failed to publish buffered T-Bank trade fills after order identity resolution"
                                    );
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "failed to map T-Bank order-state stream event")
                            }
                        }
                    }
                    Some(order_state_stream_response::Payload::StopOrderState(state)) => {
                        last_observed_unix_nanos
                            .store(current_unix_nanos().as_u64(), Ordering::Release);
                        let mut metadata_client = context.query_client.detached_query_clone();
                        match warm_instrument_metadata(
                            &mut metadata_client,
                            &state.instrument_uid,
                            "",
                            &state.ticker,
                            &state.class_code,
                            "stop_order_state_stream",
                        )
                        .await?
                        {
                            TbankInstrumentMetadataResolution::Enabled => {}
                            TbankInstrumentMetadataResolution::OutOfScope => continue,
                            TbankInstrumentMetadataResolution::Rejected => {
                                tracing::warn!(
                                    "skipping malformed T-Bank stop-order event without a trustworthy instrument identity"
                                );
                                continue;
                            }
                        }
                        match stream_stop_order_status_report_from_state_with_instruments(
                            state,
                            current_unix_nanos(),
                            &context.pending_submits,
                            &context.broker_order_index,
                            Some(&context.instruments),
                        ) {
                            Ok(Some(report)) => {
                                context.run_if_active(|| {
                                    settle_order_report_mutation_state(
                                        &context.pending_submits,
                                        &context.unresolved_cancellations,
                                        &context.broker_order_index,
                                        &report,
                                    );
                                    if let Some(report) = project_order_status_report(
                                        &context.order_status_projection,
                                        report,
                                    ) {
                                        context.emitter.send_order_status_report(report);
                                    }
                                });
                            }
                            Ok(None) => {}
                            Err(error) => {
                                tracing::warn!(%error, "failed to map T-Bank stop-order stream event")
                            }
                        }
                    }
                    Some(
                        order_state_stream_response::Payload::Ping(_)
                        | order_state_stream_response::Payload::Subscription(_),
                    )
                    | None => {}
                }
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_trades_stream(
    mut stream: tonic::Streaming<TradesStreamResponse>,
    emitter: ExecutionEventEmitter,
    instruments: Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    fill_projection: Arc<Mutex<TbankFillProjection>>,
    broker_order_index: Arc<Mutex<TbankBrokerOrderIndex>>,
    pending_submits: Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    unresolved_trade_fills: Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    last_observed_unix_nanos: Arc<AtomicU64>,
    order_context: TbankOrderStreamContext,
) -> anyhow::Result<()> {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                if !order_context.is_active() {
                    return Ok(());
                }
                let Some(trades_stream_response::Payload::OrderTrades(order)) = response.payload
                else {
                    continue;
                };
                last_observed_unix_nanos.store(current_unix_nanos().as_u64(), Ordering::Release);
                let mut metadata_client = order_context.query_client.detached_query_clone();
                match warm_instrument_metadata(
                    &mut metadata_client,
                    &order.instrument_uid,
                    &order.figi,
                    "",
                    "",
                    "trades_stream",
                )
                .await?
                {
                    TbankInstrumentMetadataResolution::Enabled => {}
                    TbankInstrumentMetadataResolution::OutOfScope => continue,
                    TbankInstrumentMetadataResolution::Rejected => {
                        tracing::warn!(
                            "skipping malformed T-Bank trade event without a trustworthy instrument identity"
                        );
                        continue;
                    }
                }
                for trade in &order.trades {
                    match fill_report_from_order_trade(
                        &order,
                        trade,
                        current_unix_nanos(),
                        &instruments,
                    ) {
                        Ok(report) => {
                            if !order_context.is_active() {
                                return Ok(());
                            }
                            let venue_order_id = report.venue_order_id.to_string();
                            let known_order = broker_order_index
                                .lock()
                                .expect("broker_order_index lock")
                                .identity_for(None, Some(venue_order_id.as_str()))
                                .is_some();
                            if !known_order {
                                let buffer_pressure = buffer_unresolved_trade_fill(
                                    &unresolved_trade_fills,
                                    venue_order_id.clone(),
                                    report,
                                );
                                schedule_unresolved_trade_reconciliation(
                                    order_context.clone(),
                                    venue_order_id,
                                );
                                if buffer_pressure {
                                    return Err(anyhow::anyhow!(
                                        "T-Bank unresolved-fill buffer requires authoritative reconciliation"
                                    ));
                                }
                                continue;
                            }
                            let publication = order_context.run_if_active(|| {
                                if let Some(report) = project_managed_trade_fill_report(
                                    &broker_order_index,
                                    &fill_projection,
                                    report,
                                )? {
                                    mark_pending_submit_fill_report(&pending_submits, &report);
                                    emitter.send_fill_report(report);
                                }
                                anyhow::Ok(())
                            });
                            if let Some(publication) = publication {
                                publication?;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "failed to map T-Bank trades stream event")
                        }
                    }
                }
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn publish_portfolio_response(
    response: PortfolioStreamResponse,
    emitter: &ExecutionEventEmitter,
    position_projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    instruments: &Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    lifecycle_active: &Arc<TbankLifecycleToken>,
    mut snapshot_complete: bool,
    out_of_scope_positions: &HashSet<String>,
) {
    lifecycle_active.run_if_active(|| {
        let Some(portfolio_stream_response::Payload::Portfolio(portfolio)) = response.payload
        else {
            return;
        };
        match account_state_from_portfolio(&portfolio) {
            Ok(Some(state)) => {
                emitter.send_account_state(state);
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "failed to map T-Bank portfolio stream event"),
        }
        let ts_init = current_unix_nanos();
        let account_id = nautilus_account_id(&portfolio.account_id);
        let mut reports = Vec::with_capacity(portfolio.positions.len());
        for position in &portfolio.positions {
            let position_identity = snapshot_position_identity(
                &position.instrument_uid,
                &position.figi,
                &position.ticker,
                &position.class_code,
            );
            if is_out_of_scope_position(position_identity.as_ref(), out_of_scope_positions) {
                tracing::debug!(
                    "ignoring T-Bank portfolio position outside the supported adapter scope"
                );
                continue;
            }
            match position_status_report_from_portfolio_with_instruments(
                account_id,
                position,
                ts_init,
                Some(instruments),
            ) {
                Some(report) => reports.push(report),
                None => {
                    snapshot_complete = false;
                    tracing::warn!(
                        "T-Bank portfolio snapshot position could not be translated; preserving it as unresolved"
                    );
                }
            }
        }
        apply_position_snapshot(
            position_projection,
            account_id,
            &mut reports,
            ts_init,
            TbankPositionProjectionSource::PortfolioStream,
            snapshot_complete,
        );
        for report in reports {
            emitter.send_position_report(report);
        }
    });
}

async fn warm_instrument_metadata(
    query_client: &mut TbankExecutionRuntime,
    instrument_uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
    source: &str,
) -> anyhow::Result<TbankInstrumentMetadataResolution> {
    // Transport failures remain errors for the reconnect loop. A malformed broker event is a
    // per-event rejection and must not tear down an otherwise healthy stream.
    let resolution = query_client
        .metadata_resolution_for_identity(instrument_uid, figi, ticker, class_code)
        .await?;
    match resolution {
        TbankInstrumentMetadataResolution::Enabled => {}
        TbankInstrumentMetadataResolution::OutOfScope => {
            tracing::debug!(
                source,
                "ignoring T-Bank event outside the supported adapter scope"
            );
        }
        TbankInstrumentMetadataResolution::Rejected => {
            tracing::warn!(
                source,
                "rejecting malformed T-Bank execution event with inconsistent instrument identity"
            );
        }
    }
    Ok(resolution)
}

fn snapshot_position_identity(
    instrument_uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
) -> Option<String> {
    if !instrument_uid.is_empty() {
        Some(format!("uid:{instrument_uid}"))
    } else if !figi.is_empty() {
        Some(format!("figi:{figi}"))
    } else if !ticker.is_empty() && !class_code.is_empty() {
        Some(format!("ticker:{ticker}:{class_code}"))
    } else {
        None
    }
}

fn is_out_of_scope_position(
    position_identity: Option<&String>,
    out_of_scope_positions: &HashSet<String>,
) -> bool {
    position_identity.is_some_and(|identity| out_of_scope_positions.contains(identity))
}

pub(super) async fn publish_portfolio_stream(
    mut stream: tonic::Streaming<PortfolioStreamResponse>,
    emitter: ExecutionEventEmitter,
    position_projection: Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    instruments: Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    lifecycle_active: Arc<TbankLifecycleToken>,
    mut query_client: TbankExecutionRuntime,
) -> anyhow::Result<()> {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                let mut snapshot_complete = true;
                let mut out_of_scope_positions = HashSet::new();
                if let Some(portfolio_stream_response::Payload::Portfolio(portfolio)) =
                    response.payload.as_ref()
                {
                    for position in &portfolio.positions {
                        match warm_instrument_metadata(
                            &mut query_client,
                            &position.instrument_uid,
                            &position.figi,
                            &position.ticker,
                            &position.class_code,
                            "portfolio_stream",
                        )
                        .await?
                        {
                            TbankInstrumentMetadataResolution::Enabled => {}
                            TbankInstrumentMetadataResolution::OutOfScope => {
                                if let Some(identity) = snapshot_position_identity(
                                    &position.instrument_uid,
                                    &position.figi,
                                    &position.ticker,
                                    &position.class_code,
                                ) {
                                    out_of_scope_positions.insert(identity);
                                }
                            }
                            TbankInstrumentMetadataResolution::Rejected => {
                                snapshot_complete = false;
                                if let Some(identity) = snapshot_position_identity(
                                    &position.instrument_uid,
                                    &position.figi,
                                    &position.ticker,
                                    &position.class_code,
                                ) {
                                    out_of_scope_positions.insert(identity);
                                }
                            }
                        }
                    }
                }
                publish_portfolio_response(
                    response,
                    &emitter,
                    &position_projection,
                    &instruments,
                    &lifecycle_active,
                    snapshot_complete,
                    &out_of_scope_positions,
                )
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn publish_positions_response(
    response: PositionsStreamResponse,
    emitter: &ExecutionEventEmitter,
    position_projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    instruments: &Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    lifecycle_active: &Arc<TbankLifecycleToken>,
    snapshot_complete: bool,
    out_of_scope_positions: &HashSet<String>,
) {
    lifecycle_active.run_if_active(|| match response.payload {
        Some(positions_stream_response::Payload::Position(position)) => {
            for security in &position.securities {
                let position_identity = snapshot_position_identity(
                    &security.instrument_uid,
                    &security.figi,
                    &security.ticker,
                    &security.class_code,
                );
                if is_out_of_scope_position(position_identity.as_ref(), out_of_scope_positions) {
                    tracing::debug!(
                        "ignoring T-Bank position outside the supported adapter scope"
                    );
                    continue;
                }
                if let Some(report) = position_status_report_from_security_with_instruments(
                    nautilus_account_id(&position.account_id),
                    security,
                    current_unix_nanos(),
                    Some(instruments),
                ) && record_position_projection(position_projection, &report)
                {
                    emitter.send_position_report(report);
                }
            }
            for future in &position.futures {
                let position_identity = snapshot_position_identity(
                    &future.instrument_uid,
                    &future.figi,
                    &future.ticker,
                    &future.class_code,
                );
                if is_out_of_scope_position(position_identity.as_ref(), out_of_scope_positions) {
                    tracing::debug!(
                        "ignoring T-Bank futures position outside the supported adapter scope"
                    );
                    continue;
                }
                if let Some(report) = position_status_report_from_future_with_instruments(
                    nautilus_account_id(&position.account_id),
                    future,
                    current_unix_nanos(),
                    instruments,
                ) && record_position_projection(position_projection, &report)
                {
                    emitter.send_position_report(report);
                }
            }
        }
        Some(positions_stream_response::Payload::InitialPositions(positions)) => {
            let ts_init = current_unix_nanos();
            let account_id = nautilus_account_id(&positions.account_id);
            let mut snapshot_complete = snapshot_complete && !positions.limits_loading_in_progress;
            let mut reports = Vec::with_capacity(
                positions.securities.len().saturating_add(positions.futures.len()),
            );
            for security in &positions.securities {
                let position_identity = snapshot_position_identity(
                    &security.instrument_uid,
                    &security.figi,
                    &security.ticker,
                    &security.class_code,
                );
                if is_out_of_scope_position(position_identity.as_ref(), out_of_scope_positions) {
                    tracing::debug!(
                        "ignoring T-Bank initial securities position outside the supported adapter scope"
                    );
                    continue;
                }
                match position_status_report_from_security_with_instruments(
                    account_id,
                    security,
                    ts_init,
                    Some(instruments),
                ) {
                    Some(report) => reports.push(report),
                    None => {
                        snapshot_complete = false;
                        tracing::warn!(
                            "T-Bank initial securities position could not be translated; preserving it as unresolved"
                        );
                    }
                }
            }
            for future in &positions.futures {
                let position_identity = snapshot_position_identity(
                    &future.instrument_uid,
                    &future.figi,
                    &future.ticker,
                    &future.class_code,
                );
                if is_out_of_scope_position(position_identity.as_ref(), out_of_scope_positions) {
                    tracing::debug!(
                        "ignoring T-Bank initial futures position outside the supported adapter scope"
                    );
                    continue;
                }
                match position_status_report_from_future_with_instruments(
                    account_id,
                    future,
                    ts_init,
                    instruments,
                ) {
                    Some(report) => reports.push(report),
                    None => {
                        snapshot_complete = false;
                        tracing::warn!(
                            "T-Bank initial futures position could not be translated; preserving it as unresolved"
                        );
                    }
                }
            }
            apply_position_snapshot(
                position_projection,
                account_id,
                &mut reports,
                ts_init,
                TbankPositionProjectionSource::SecuritiesSnapshot,
                snapshot_complete,
            );
            for report in reports {
                emitter.send_position_report(report);
            }
        }
        Some(
            positions_stream_response::Payload::Subscriptions(_)
            | positions_stream_response::Payload::Ping(_),
        )
        | None => {}
    });
}

pub(super) async fn publish_positions_stream(
    mut stream: tonic::Streaming<PositionsStreamResponse>,
    emitter: ExecutionEventEmitter,
    position_projection: Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    instruments: Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    lifecycle_active: Arc<TbankLifecycleToken>,
    mut query_client: TbankExecutionRuntime,
) -> anyhow::Result<()> {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                let mut snapshot_complete = true;
                let mut out_of_scope_positions = HashSet::new();
                match response.payload.as_ref() {
                    Some(positions_stream_response::Payload::Position(position)) => {
                        for security in &position.securities {
                            match warm_instrument_metadata(
                                &mut query_client,
                                &security.instrument_uid,
                                &security.figi,
                                &security.ticker,
                                &security.class_code,
                                "positions_stream",
                            )
                            .await?
                            {
                                TbankInstrumentMetadataResolution::Enabled => {}
                                TbankInstrumentMetadataResolution::OutOfScope => {
                                    if let Some(identity) = snapshot_position_identity(
                                        &security.instrument_uid,
                                        &security.figi,
                                        &security.ticker,
                                        &security.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                                TbankInstrumentMetadataResolution::Rejected => {
                                    snapshot_complete = false;
                                    if let Some(identity) = snapshot_position_identity(
                                        &security.instrument_uid,
                                        &security.figi,
                                        &security.ticker,
                                        &security.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                            }
                        }
                        for future in &position.futures {
                            match warm_instrument_metadata(
                                &mut query_client,
                                &future.instrument_uid,
                                &future.figi,
                                &future.ticker,
                                &future.class_code,
                                "positions_stream",
                            )
                            .await?
                            {
                                TbankInstrumentMetadataResolution::Enabled => {}
                                TbankInstrumentMetadataResolution::OutOfScope => {
                                    if let Some(identity) = snapshot_position_identity(
                                        &future.instrument_uid,
                                        &future.figi,
                                        &future.ticker,
                                        &future.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                                TbankInstrumentMetadataResolution::Rejected => {
                                    snapshot_complete = false;
                                    if let Some(identity) = snapshot_position_identity(
                                        &future.instrument_uid,
                                        &future.figi,
                                        &future.ticker,
                                        &future.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                            }
                        }
                    }
                    Some(positions_stream_response::Payload::InitialPositions(positions)) => {
                        for security in &positions.securities {
                            match warm_instrument_metadata(
                                &mut query_client,
                                &security.instrument_uid,
                                &security.figi,
                                &security.ticker,
                                &security.class_code,
                                "positions_stream",
                            )
                            .await?
                            {
                                TbankInstrumentMetadataResolution::Enabled => {}
                                TbankInstrumentMetadataResolution::OutOfScope => {
                                    if let Some(identity) = snapshot_position_identity(
                                        &security.instrument_uid,
                                        &security.figi,
                                        &security.ticker,
                                        &security.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                                TbankInstrumentMetadataResolution::Rejected => {
                                    snapshot_complete = false;
                                    if let Some(identity) = snapshot_position_identity(
                                        &security.instrument_uid,
                                        &security.figi,
                                        &security.ticker,
                                        &security.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                            }
                        }
                        for future in &positions.futures {
                            match warm_instrument_metadata(
                                &mut query_client,
                                &future.instrument_uid,
                                &future.figi,
                                &future.ticker,
                                &future.class_code,
                                "positions_stream",
                            )
                            .await?
                            {
                                TbankInstrumentMetadataResolution::Enabled => {}
                                TbankInstrumentMetadataResolution::OutOfScope => {
                                    if let Some(identity) = snapshot_position_identity(
                                        &future.instrument_uid,
                                        &future.figi,
                                        &future.ticker,
                                        &future.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                                TbankInstrumentMetadataResolution::Rejected => {
                                    snapshot_complete = false;
                                    if let Some(identity) = snapshot_position_identity(
                                        &future.instrument_uid,
                                        &future.figi,
                                        &future.ticker,
                                        &future.class_code,
                                    ) {
                                        out_of_scope_positions.insert(identity);
                                    }
                                }
                            }
                        }
                    }
                    Some(
                        positions_stream_response::Payload::Subscriptions(_)
                        | positions_stream_response::Payload::Ping(_),
                    )
                    | None => {}
                }
                publish_positions_response(
                    response,
                    &emitter,
                    &position_projection,
                    &instruments,
                    &lifecycle_active,
                    snapshot_complete,
                    &out_of_scope_positions,
                )
            }
            Ok(None) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}
