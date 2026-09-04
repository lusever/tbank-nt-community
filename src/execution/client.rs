//! Core T-Bank execution client state, transport access, and lifecycle management.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    common::{
        Result, TbankAdapterError,
        error::REDACTED_BROKER_IDENTITY,
        time::unix_nanos_to_timestamp,
        venue::{TbankInstrumentType, TbankVenue},
    },
    config::TbankExecutionClientConfig,
    execution::{
        TbankExecutionService, TbankSubmitOrder, build_post_order_request,
        build_post_stop_order_request,
    },
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients, connect_channel,
        generated::{
            CancelOrderRequest, CancelOrderResponse, CancelStopOrderRequest,
            CancelStopOrderResponse, GetFuturesMarginRequest, GetOperationsByCursorRequest,
            GetOperationsByCursorResponse, GetOrderStateRequest, GetOrdersRequest,
            GetOrdersResponse, GetStopOrdersRequest, GetStopOrdersResponse, InstrumentIdType,
            InstrumentRequest, InstrumentType, MoneyValue, OperationItem, OperationState,
            OperationType as TbankOperationType, OrderDirection,
            OrderExecutionReportStatus as TbankOrderExecutionReportStatus, OrderIdType, OrderState,
            OrderStateStreamRequest, OrderStateStreamResponse, PortfolioPosition, PortfolioRequest,
            PortfolioResponse, PortfolioStreamRequest, PortfolioStreamResponse, PositionsFutures,
            PositionsRequest, PositionsResponse, PositionsSecurities, PositionsStreamRequest,
            PositionsStreamResponse, PostOrderResponse, PostStopOrderResponse, PriceType,
            Quotation, StopOrder, StopOrderStatusOption, TimeInForceType as TbankTimeInForceType,
            TradesStreamRequest, TradesStreamResponse, get_orders_request,
            order_state_stream_response, portfolio_stream_response, positions_stream_response,
            trades_stream_response,
        },
        with_timeout,
    },
    instruments::TbankInstrumentMetadata,
};

use async_trait::async_trait;
use chrono::Utc;
use nautilus_common::{
    clients::ExecutionClient,
    live::{
        runner::{get_exec_event_sender, try_get_data_event_sender},
        runtime::get_runtime,
    },
    messages::{DataEvent, execution::ExecutionReport},
    msgbus::{self, TypedHandler, switchboard},
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
        TimeInForce, TriggerType,
    },
    events::{OrderCanceled, OrderEventAny},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use tokio::{sync::watch, task::JoinHandle};

mod nautilus;
mod reconciliation;
mod streams;
mod submit;
mod translation;

use reconciliation::*;
use streams::*;
use submit::*;
use translation::*;

/// Nautilus execution parameter controlling T-Bank margin-trade confirmation.
pub const TBANK_CONFIRM_MARGIN_TRADE_PARAM: &str = "tbank_confirm_margin_trade";

fn log_tbank_rpc_failure(rpc: &'static str, error: &TbankAdapterError) {
    match error {
        TbankAdapterError::GrpcStatus { code, message } => {
            let broker_code = message.trim();
            let broker_code = if !broker_code.is_empty()
                && broker_code.len() <= 16
                && broker_code.bytes().all(|byte| byte.is_ascii_digit())
            {
                broker_code
            } else {
                "<redacted>"
            };
            tracing::event!(
                target: "tbank.rpc",
                tracing::Level::WARN,
                rpc,
                grpc_code = ?code,
                broker_code,
                "T-Bank RPC failed"
            );
        }
        TbankAdapterError::PermissionDenied(_) => {
            tracing::event!(
                target: "tbank.rpc",
                tracing::Level::WARN,
                rpc,
                error_kind = "permission_denied",
                "T-Bank RPC failed"
            );
        }
        TbankAdapterError::RateLimited(_) => {
            tracing::event!(
                target: "tbank.rpc",
                tracing::Level::WARN,
                rpc,
                error_kind = "rate_limited",
                "T-Bank RPC failed"
            );
        }
        _ => {
            tracing::event!(
                target: "tbank.rpc",
                tracing::Level::WARN,
                rpc,
                error_kind = "adapter_error",
                "T-Bank RPC failed"
            );
        }
    }
}

#[derive(Clone)]
struct TbankExecutionRuntime {
    client_id: ClientId,
    account_id: AccountId,
    pub config: TbankExecutionClientConfig,
    clients: Option<TbankGrpcClients<TbankAuthInterceptor>>,
    instruments: Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    futures_margin_refreshed_at: Arc<Mutex<HashMap<String, Instant>>>,
    futures_margin_inflight: TbankFuturesMarginFlights,
    futures_margin_generation: Arc<Mutex<u64>>,
    futures_margin_generation_id: u64,
    broker_order_index: Arc<Mutex<TbankBrokerOrderIndex>>,
    fill_projection: Arc<Mutex<TbankFillProjection>>,
    order_status_projection: Arc<Mutex<HashMap<String, TbankProjectedOrderStatus>>>,
    position_projection: Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    pending_submits: Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    unresolved_trade_fills: Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    unresolved_cancellations: Arc<Mutex<HashSet<TbankBrokerOrderIdentity>>>,
    stream_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    reconciliation_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    command_tasks: Arc<Mutex<Vec<TbankCommandTask>>>,
    lifecycle_active: Arc<TbankLifecycleToken>,
    emitter: ExecutionEventEmitter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TbankInstrumentMetadataResolution {
    Enabled,
    OutOfScope,
    Rejected,
}

fn instrument_metadata_identity(
    instrument_uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
) -> String {
    if !ticker.is_empty() && !class_code.is_empty() {
        format!("ticker:{ticker}:{class_code}")
    } else if !ticker.is_empty() {
        format!("ticker:{ticker}")
    } else if !class_code.is_empty() {
        format!("class_code:{class_code}")
    } else if !instrument_uid.is_empty() {
        format!("instrument_uid:{instrument_uid}")
    } else if !figi.is_empty() {
        format!("figi:{figi}")
    } else {
        REDACTED_BROKER_IDENTITY.to_string()
    }
}

#[cfg(test)]
fn unresolved_instrument_metadata_error(ticker: &str, class_code: &str) -> TbankAdapterError {
    TbankAdapterError::InstrumentMetadataUnresolved(instrument_metadata_identity(
        "", "", ticker, class_code,
    ))
}

fn invalid_instrument_identity_error(
    instrument_uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
    reason: &str,
) -> TbankAdapterError {
    TbankAdapterError::InvalidInstrumentIdentity(format!(
        "{}: {reason}",
        instrument_metadata_identity(instrument_uid, figi, ticker, class_code)
    ))
}

fn metadata_matches_event_identity(
    metadata: &TbankInstrumentMetadata,
    instrument_uid: &str,
    figi: &str,
    ticker: &str,
    class_code: &str,
) -> bool {
    let uid_matches = instrument_uid.is_empty() || metadata.instrument_uid == instrument_uid;
    let figi_matches = figi.is_empty() || metadata.figi == figi;
    let ticker_matches = ticker.is_empty() || metadata.ticker.eq_ignore_ascii_case(ticker);
    let class_code_matches =
        class_code.is_empty() || metadata.class_code.eq_ignore_ascii_case(class_code);
    uid_matches && figi_matches && ticker_matches && class_code_matches
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TbankCommandTaskKind {
    ReadOnly,
    Mutating,
}

#[derive(Debug)]
struct TbankCommandTask {
    kind: TbankCommandTaskKind,
    handle: JoinHandle<()>,
}

/// Nautilus execution client backed by T-Bank order and operation services.
pub struct TbankExecutionClient {
    core: ExecutionClientCore,
    runtime: TbankExecutionRuntime,
    instrument_subscriptions: Vec<(Venue, TypedHandler<InstrumentAny>)>,
}

impl TbankExecutionClient {
    /// Creates a new instance.
    #[must_use]
    pub fn new(core: ExecutionClientCore, config: TbankExecutionClientConfig) -> Self {
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let runtime = TbankExecutionRuntime::new(config, core.client_id, core.account_id, emitter);
        Self {
            core,
            runtime,
            instrument_subscriptions: Vec::new(),
        }
    }

    /// Subscribes the execution client to every public venue handled by the broker.
    ///
    /// Nautilus' live-node builder subscribes the execution engine only to the client's
    /// primary `venue()`. T-Bank is a multi-venue broker, so the adapter owns these additional
    /// subscriptions and feeds the same runtime metadata cache used by order translation.
    pub(crate) fn subscribe_instrument_updates(&mut self) {
        if !self.instrument_subscriptions.is_empty() {
            return;
        }

        for configured_venue in TbankVenue::all() {
            let venue = configured_venue.venue();
            let instruments = Arc::downgrade(&self.runtime.instruments);
            let handler = TypedHandler::from(move |instrument: &InstrumentAny| {
                let Some(instruments) = instruments.upgrade() else {
                    return;
                };
                let Some(metadata) = TbankInstrumentMetadata::from_instrument(instrument) else {
                    tracing::warn!(
                        instrument_id = %instrument.id(),
                        "ignoring T-Bank instrument definition without adapter metadata"
                    );
                    return;
                };
                if metadata.venue != configured_venue || !metadata.is_supported() {
                    return;
                }
                instruments
                    .lock()
                    .expect("instruments lock")
                    .insert(metadata.instrument_id.clone(), metadata);
            });
            msgbus::subscribe_instruments(
                switchboard::get_instruments_pattern(venue),
                handler.clone(),
                None,
            );
            self.instrument_subscriptions.push((venue, handler));
            tracing::debug!(%venue, "subscribed T-Bank execution client to instrument definitions");
        }
    }

    /// Removes the adapter-owned instrument subscriptions before replacing runtime state.
    pub(crate) fn unsubscribe_instrument_updates(&mut self) {
        for (venue, handler) in self.instrument_subscriptions.drain(..) {
            msgbus::unsubscribe_instruments(switchboard::get_instruments_pattern(venue), &handler);
        }
    }

    /// Connects the client to the configured T-Bank endpoint.
    pub async fn connect(&mut self) -> Result<()> {
        self.runtime.connect().await?;
        if let Err(error) = self.await_account_registered().await {
            self.runtime.disconnect();
            return Err(error);
        }
        self.core.set_connected();
        Ok(())
    }

    async fn await_account_registered(&self) -> Result<()> {
        let timeout = self.runtime.config.account_registration_timeout;
        let started = Instant::now();
        let poll_interval = Duration::from_millis(10);
        loop {
            if self.core.cache().account(&self.core.account_id).is_some() {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err(TbankAdapterError::ConfigError(format!(
                    "timed out waiting for Nautilus account registration after {} ms",
                    timeout.as_millis()
                )));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Connects only the query services required for reconciliation.
    pub async fn connect_for_queries(&mut self) -> Result<()> {
        self.runtime.connect_for_queries().await?;
        self.core.set_connected();
        Ok(())
    }

    /// Disconnects the client and stops its background tasks.
    pub fn disconnect(&mut self) {
        self.runtime.disconnect();
        self.core.set_disconnected();
    }

    /// Returns whether the client is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.core.is_connected()
    }
}

#[derive(Clone)]
struct TbankOrderStreamContext {
    emitter: ExecutionEventEmitter,
    query_client: TbankExecutionRuntime,
    lifecycle_active: Arc<TbankLifecycleToken>,
    pending_submits: Arc<Mutex<HashMap<String, TbankPendingSubmit>>>,
    unresolved_trade_fills: Arc<Mutex<HashMap<String, Vec<FillReport>>>>,
    unresolved_cancellations: Arc<Mutex<HashSet<TbankBrokerOrderIdentity>>>,
    broker_order_index: Arc<Mutex<TbankBrokerOrderIndex>>,
    fill_projection: Arc<Mutex<TbankFillProjection>>,
    order_status_projection: Arc<Mutex<HashMap<String, TbankProjectedOrderStatus>>>,
    instruments: Arc<Mutex<HashMap<String, TbankInstrumentMetadata>>>,
    reconnect_policy: crate::config::TbankReconnectPolicy,
    activated_stop_reconciliations: Arc<Mutex<HashSet<String>>>,
    regular_order_reconciliations: Arc<Mutex<HashSet<String>>>,
    reconciliation_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TbankOrderStreamContext {
    fn is_active(&self) -> bool {
        self.lifecycle_active.load(Ordering::Acquire)
    }

    fn run_if_active<R>(&self, action: impl FnOnce() -> R) -> Option<R> {
        self.lifecycle_active.run_if_active(action)
    }
}

#[derive(Debug)]
struct TbankLifecycleToken {
    active: AtomicBool,
    publication_gate: Mutex<()>,
}

impl TbankLifecycleToken {
    fn new(active: bool) -> Self {
        Self {
            active: AtomicBool::new(active),
            publication_gate: Mutex::new(()),
        }
    }

    fn load(&self, ordering: Ordering) -> bool {
        self.active.load(ordering)
    }

    fn store(&self, active: bool, ordering: Ordering) {
        let _guard = self
            .publication_gate
            .lock()
            .expect("lifecycle publication gate");
        self.active.store(active, ordering);
    }

    fn run_if_active<R>(&self, action: impl FnOnce() -> R) -> Option<R> {
        let _guard = self
            .publication_gate
            .lock()
            .expect("lifecycle publication gate");
        self.active.load(Ordering::Acquire).then(action)
    }
}

pub use super::broker_order_index::tbank_broker_request_id_for_client_order_id;
use super::broker_order_index::{
    TbankBrokerOrderIdentity, TbankBrokerOrderIndex, TbankBrokerOrderRoute, TbankCancelTarget,
    TbankManagedOrderContext, TbankResolvedStreamOrderIdentity,
};

#[cfg(test)]
use super::projections::project_trade_fill_report;
#[cfg(test)]
use super::projections::record_position_projection_from_source;
use super::projections::{
    TbankFillProjection, TbankPositionProjectionSource, TbankProjectedOrderStatus,
    TbankProjectedPosition, apply_position_snapshot, merge_fill_projection_alias,
    project_cumulative_order_fill, project_order_status_report, project_trade_fill_report_locked,
    record_position_projection,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TbankPendingSubmitStage {
    Submitted,
    Unknown,
    Accepted,
    Rejected,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TbankCancelRecoveryOutcome {
    Canceled,
    Active,
}

const SUBMIT_OUTCOME_RECOVERY_ATTEMPTS: u32 = 8;
const STOP_ORDER_SUBMIT_RECONCILIATION_WINDOW: Duration = Duration::from_secs(5 * 60);
const RECONNECT_RECONCILIATION_MAX_ATTEMPTS: u32 = 5;
const CANCEL_OUTCOME_RECOVERY_ATTEMPTS: u32 = 5;
const RECONNECT_RECONCILIATION_OVERLAP_NANOS: u64 = 5 * 60 * 1_000_000_000;
const FUTURES_MARGIN_CACHE_TTL: Duration = Duration::from_secs(60);
const MAX_UNRESOLVED_TRADE_FILLS: usize = 1_024;
const MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER: usize = 64;

fn current_utc_day_bounds() -> (prost_types::Timestamp, prost_types::Timestamp) {
    let now = Utc::now();
    let start = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .and_utc();
    (
        prost_types::Timestamp {
            seconds: start.timestamp(),
            nanos: 0,
        },
        prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: i32::try_from(now.timestamp_subsec_nanos()).unwrap_or(0),
        },
    )
}

fn order_execution_status_filters() -> Vec<i32> {
    [
        TbankOrderExecutionReportStatus::ExecutionReportStatusNew,
        TbankOrderExecutionReportStatus::ExecutionReportStatusPartiallyfill,
        TbankOrderExecutionReportStatus::ExecutionReportStatusFill,
        TbankOrderExecutionReportStatus::ExecutionReportStatusCancelled,
        TbankOrderExecutionReportStatus::ExecutionReportStatusRejected,
    ]
    .into_iter()
    .map(|status| status as i32)
    .collect()
}

fn order_filter_windows(
    from_unix_nanos: i128,
    to_unix_nanos: i128,
) -> Result<Vec<get_orders_request::GetOrdersRequestFilters>> {
    const MAX_WINDOW_NANOS: i128 = 60 * 60 * 1_000_000_000;

    let (today, _) = current_utc_day_bounds();
    let today_unix_nanos = i128::from(today.seconds) * 1_000_000_000;
    let mut cursor = from_unix_nanos.max(today_unix_nanos);
    let to_unix_nanos = to_unix_nanos.max(cursor);
    let mut windows = Vec::new();
    while cursor < to_unix_nanos {
        let end = cursor.saturating_add(MAX_WINDOW_NANOS).min(to_unix_nanos);
        windows.push(get_orders_request::GetOrdersRequestFilters {
            from: Some(unix_nanos_to_timestamp(cursor)?),
            to: Some(unix_nanos_to_timestamp(end)?),
            execution_status: order_execution_status_filters(),
        });
        cursor = end;
    }
    Ok(windows)
}

fn reconnect_reconciliation_from(last_observed_unix_nanos: &AtomicU64) -> i128 {
    i128::from(
        last_observed_unix_nanos
            .load(Ordering::Acquire)
            .saturating_sub(RECONNECT_RECONCILIATION_OVERLAP_NANOS),
    )
}

const STOP_ORDER_SUBMIT_TIMESTAMP_TOLERANCE: Duration = Duration::from_secs(2);

fn stop_order_submit_earliest_timestamp(submitted_ts: UnixNanos) -> u64 {
    let tolerance_nanos =
        u64::try_from(STOP_ORDER_SUBMIT_TIMESTAMP_TOLERANCE.as_nanos()).unwrap_or(u64::MAX);
    submitted_ts.as_u64().saturating_sub(tolerance_nanos)
}

fn stop_order_submit_reconciliation_from(submitted_ts: UnixNanos) -> i128 {
    let now = current_unix_nanos().as_u64();
    let window_nanos =
        u64::try_from(STOP_ORDER_SUBMIT_RECONCILIATION_WINDOW.as_nanos()).unwrap_or(u64::MAX);
    i128::from(
        stop_order_submit_earliest_timestamp(submitted_ts).max(now.saturating_sub(window_nanos)),
    )
}

fn stop_order_is_after_submit(stop: &StopOrder, submitted_ts: UnixNanos, query_from: i128) -> bool {
    match stop.create_date.as_ref() {
        Some(create_date) => timestamp_to_unix_nanos(create_date).is_ok_and(|created_ts| {
            created_ts.as_u64() >= stop_order_submit_earliest_timestamp(submitted_ts)
        }),
        // The broker contract includes create_date. Test/reduced deployments may omit it; in
        // that case trust only an untrimmed request whose lower bound includes the full skew
        // tolerance. A reconciliation clamped by the bounded history window remains unresolved.
        None => query_from == i128::from(stop_order_submit_earliest_timestamp(submitted_ts)),
    }
}

#[derive(Debug, Clone)]
struct TbankPendingSubmit {
    instrument_id: String,
    submitted_ts: UnixNanos,
    quantity_units: Decimal,
    side: crate::common::TbankOrderSide,
    order_type: crate::common::TbankOrderType,
    time_in_force: TimeInForce,
    trailing: Option<crate::execution::TbankTrailingStopParams>,
    venue_order_id: Option<String>,
    last_reconciliation_ts: Option<UnixNanos>,
    stage: TbankPendingSubmitStage,
}

#[derive(Clone)]
enum TbankFuturesMarginFlightState {
    Pending,
    Completed(Box<TbankFuturesMarginResult>),
    Cancelled,
}

type TbankFuturesMarginResult = std::result::Result<TbankInstrumentMetadata, TbankAdapterError>;

struct TbankFuturesMarginFlight {
    state: watch::Sender<TbankFuturesMarginFlightState>,
    _receiver: watch::Receiver<TbankFuturesMarginFlightState>,
}

type TbankFuturesMarginFlights = Arc<Mutex<HashMap<String, Arc<TbankFuturesMarginFlight>>>>;

struct TbankFuturesMarginFlightGuard {
    flights: TbankFuturesMarginFlights,
    cache_key: String,
    flight: Arc<TbankFuturesMarginFlight>,
}

impl Drop for TbankFuturesMarginFlightGuard {
    fn drop(&mut self) {
        let mut flights = self.flights.lock().expect("futures_margin_inflight lock");
        if flights
            .get(&self.cache_key)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            flights.remove(&self.cache_key);
        }
        if matches!(
            &*self.flight.state.borrow(),
            TbankFuturesMarginFlightState::Pending
        ) {
            let _ = self
                .flight
                .state
                .send(TbankFuturesMarginFlightState::Cancelled);
        }
    }
}

impl TbankExecutionRuntime {
    fn new(
        config: TbankExecutionClientConfig,
        client_id: ClientId,
        account_id: AccountId,
        emitter: ExecutionEventEmitter,
    ) -> Self {
        Self {
            client_id,
            account_id,
            config,
            clients: None,
            instruments: Arc::new(Mutex::new(HashMap::new())),
            futures_margin_refreshed_at: Arc::new(Mutex::new(HashMap::new())),
            futures_margin_inflight: Arc::new(Mutex::new(HashMap::new())),
            futures_margin_generation: Arc::new(Mutex::new(0)),
            futures_margin_generation_id: 0,
            broker_order_index: Arc::new(Mutex::new(TbankBrokerOrderIndex::default())),
            fill_projection: Arc::new(Mutex::new(TbankFillProjection::default())),
            order_status_projection: Arc::new(Mutex::new(HashMap::new())),
            position_projection: Arc::new(Mutex::new(HashMap::new())),
            pending_submits: Arc::new(Mutex::new(HashMap::new())),
            unresolved_trade_fills: Arc::new(Mutex::new(HashMap::new())),
            unresolved_cancellations: Arc::new(Mutex::new(HashSet::new())),
            stream_tasks: Arc::new(Mutex::new(Vec::new())),
            reconciliation_tasks: Arc::new(Mutex::new(Vec::new())),
            command_tasks: Arc::new(Mutex::new(Vec::new())),
            lifecycle_active: Arc::new(TbankLifecycleToken::new(false)),
            emitter,
        }
    }

    fn ensure_lifecycle_active(&self) -> Result<()> {
        if self.lifecycle_active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(TbankAdapterError::ConfigError(
                "T-Bank execution client is disconnected".to_string(),
            ))
        }
    }

    fn spawn_read_only_command_task<F>(&self, future: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
        self.ensure_lifecycle_active()?;
        tasks.retain(|task| !task.handle.is_finished());
        tasks.push(TbankCommandTask {
            kind: TbankCommandTaskKind::ReadOnly,
            handle: get_runtime().spawn(future),
        });
        Ok(())
    }

    fn spawn_mutating_command_task<F>(&self, future: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_mutating_command_task_with(future, || {})
    }

    fn spawn_mutating_followup_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
        tasks.retain(|task| !task.handle.is_finished());
        tasks.push(TbankCommandTask {
            kind: TbankCommandTaskKind::Mutating,
            handle: get_runtime().spawn(future),
        });
    }

    fn spawn_mutating_followup_task_if_active<F>(&self, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
        self.lifecycle_active
            .run_if_active(|| {
                tasks.retain(|task| !task.handle.is_finished());
                tasks.push(TbankCommandTask {
                    kind: TbankCommandTaskKind::Mutating,
                    handle: get_runtime().spawn(future),
                });
            })
            .is_some()
    }

    fn spawn_mutating_command_task_with<F, C>(&self, future: F, on_registered: C) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
        C: FnOnce(),
    {
        let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
        self.ensure_lifecycle_active()?;
        tasks.retain(|task| !task.handle.is_finished());
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let handle = get_runtime().spawn(async move {
            if start_rx.await.is_ok() {
                future.await;
            }
        });
        tasks.push(TbankCommandTask {
            kind: TbankCommandTaskKind::Mutating,
            handle,
        });
        // Keep the registry lock held until externally visible acceptance is published. A
        // concurrent disconnect can invalidate the lifecycle, but cannot observe an accepted
        // submit without also observing its registered mutating task.
        on_registered();
        let _ = start_tx.send(());
        Ok(())
    }

    fn has_unfinished_mutating_tasks(&self) -> bool {
        let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
        tasks.retain(|task| !task.handle.is_finished());
        tasks
            .iter()
            .any(|task| task.kind == TbankCommandTaskKind::Mutating)
    }

    fn has_unresolved_mutation_outcomes(&self) -> bool {
        let unresolved_submit = self
            .pending_submits
            .lock()
            .expect("pending_submits lock")
            .values()
            .any(|pending| {
                matches!(
                    pending.stage,
                    TbankPendingSubmitStage::Submitted | TbankPendingSubmitStage::Unknown
                )
            });
        unresolved_submit
            || !self
                .unresolved_cancellations
                .lock()
                .expect("unresolved_cancellations lock")
                .is_empty()
            || !self
                .unresolved_trade_fills
                .lock()
                .expect("unresolved_trade_fills lock")
                .values()
                .all(Vec::is_empty)
    }

    fn reset_state(&mut self) {
        // Install fresh state containers so an already-polled task racing with abort cannot
        // repopulate the state used by the next lifecycle run.
        let generation = {
            let mut generation = self
                .futures_margin_generation
                .lock()
                .expect("futures_margin_generation lock");
            *generation = generation.saturating_add(1);
            *generation
        };
        self.instruments = Arc::new(Mutex::new(HashMap::new()));
        self.futures_margin_refreshed_at = Arc::new(Mutex::new(HashMap::new()));
        self.futures_margin_inflight = Arc::new(Mutex::new(HashMap::new()));
        self.futures_margin_generation_id = generation;
        self.broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
        self.fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
        self.order_status_projection = Arc::new(Mutex::new(HashMap::new()));
        self.position_projection = Arc::new(Mutex::new(HashMap::new()));
        self.pending_submits = Arc::new(Mutex::new(HashMap::new()));
        self.unresolved_trade_fills = Arc::new(Mutex::new(HashMap::new()));
        self.unresolved_cancellations = Arc::new(Mutex::new(HashSet::new()));
        self.stream_tasks = Arc::new(Mutex::new(Vec::new()));
        self.reconciliation_tasks = Arc::new(Mutex::new(Vec::new()));
        self.command_tasks = Arc::new(Mutex::new(Vec::new()));
        self.lifecycle_active = Arc::new(TbankLifecycleToken::new(false));
    }

    fn begin_connection_generation(&mut self) {
        // Freshness belongs to a broker connection generation. A reconnect must not let a
        // previous session's futures contract suppress GetFuturesMargin for the new session.
        // Replacing the Arc also prevents an in-flight task from the old generation from
        // repopulating the cache used by the new one.
        let generation = *self
            .futures_margin_generation
            .lock()
            .expect("futures_margin_generation lock")
            + 1;
        *self
            .futures_margin_generation
            .lock()
            .expect("futures_margin_generation lock") = generation;
        self.futures_margin_generation_id = generation;
        self.futures_margin_refreshed_at = Arc::new(Mutex::new(HashMap::new()));
        self.futures_margin_inflight = Arc::new(Mutex::new(HashMap::new()));
    }

    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn publish_account_state(&self, state: nautilus_model::events::AccountState) {
        self.emitter.send_account_state(state);
    }

    /// Connects the client to the configured T-Bank endpoint.
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.config.validate()?;
        let token = self.config.resolve_token_secret()?;
        let endpoint = self.config.endpoint_uri()?;
        let channel = connect_channel(&endpoint, self.config.request_timeout).await?;
        let interceptor = TbankAuthInterceptor::new(&token)?;
        self.clients = Some(TbankGrpcClients::new(channel, interceptor));
        self.begin_connection_generation();
        // A new token is a lifecycle generation. Clones from a previous connection retain the
        // invalidated token and cannot become active again after reconnect.
        self.lifecycle_active = Arc::new(TbankLifecycleToken::new(true));
        if let Err(error) = self
            .spawn_execution_streams()
            .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))
        {
            self.disconnect();
            return Err(error);
        }
        if let Err(error) = self.publish_startup_account_state().await {
            self.disconnect();
            return Err(error);
        }
        tracing::info!(
            environment = ?self.config.environment,
            endpoint = endpoint.as_str(),
            token_present = true,
            account_id_present = true,
            "connected T-Bank execution client"
        );
        Ok(())
    }

    /// Connects only the query services required for reconciliation.
    pub async fn connect_for_queries(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.config.validate()?;
        let token = self.config.resolve_token_secret()?;
        let endpoint = self.config.endpoint_uri()?;
        let channel = connect_channel(&endpoint, self.config.request_timeout).await?;
        let interceptor = TbankAuthInterceptor::new(&token)?;
        self.clients = Some(TbankGrpcClients::new(channel, interceptor));
        self.begin_connection_generation();
        self.lifecycle_active = Arc::new(TbankLifecycleToken::new(true));
        tracing::info!(
            environment = ?self.config.environment,
            endpoint = endpoint.as_str(),
            token_present = true,
            account_id_present = true,
            "connected T-Bank execution client for query-only snapshot"
        );
        Ok(())
    }

    fn abort_background_tasks(&self) -> Vec<JoinHandle<()>> {
        let mut aborted = Vec::new();
        self.lifecycle_active.store(false, Ordering::Release);
        {
            let mut tasks = self.command_tasks.lock().expect("command_tasks lock");
            let mut retained = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                if task.handle.is_finished() {
                } else if task.kind == TbankCommandTaskKind::ReadOnly {
                    task.handle.abort();
                    aborted.push(task.handle);
                } else {
                    retained.push(task);
                }
            }
            *tasks = retained;
        }
        aborted.extend(
            self.stream_tasks
                .lock()
                .expect("stream_tasks lock")
                .drain(..)
                .inspect(|task| task.abort()),
        );
        aborted.extend(
            self.reconciliation_tasks
                .lock()
                .expect("reconciliation_tasks lock")
                .drain(..)
                .inspect(|task| task.abort()),
        );
        aborted
    }

    /// Disconnects the client and stops its background tasks.
    pub fn disconnect(&mut self) {
        drop(self.abort_background_tasks());
        self.clients = None;
        tracing::info!("disconnected T-Bank execution client");
    }

    async fn disconnect_async(&mut self) {
        let tasks = self.abort_background_tasks();
        self.clients = None;
        for task in tasks {
            let _ = task.await;
        }
        tracing::info!("disconnected T-Bank execution client");
    }

    /// Returns whether the client is connected.
    pub fn is_connected(&self) -> bool {
        self.clients.is_some()
    }

    fn record_broker_order_mapping(
        &self,
        route: TbankBrokerOrderRoute,
        client_order_id: &str,
        venue_order_id: &str,
    ) -> bool {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_mapping(route, client_order_id, venue_order_id)
    }

    fn record_activated_stop_child_mapping(
        &self,
        client_order_id: &str,
        stop_order_id: &str,
        child_order_id: &str,
    ) -> bool {
        let mut index = self
            .broker_order_index
            .lock()
            .expect("broker_order_index lock");
        let should_cancel = index.record_activated_stop_child_mapping(
            client_order_id,
            stop_order_id,
            child_order_id,
        );
        let mut projection = self.fill_projection.lock().expect("fill_projection lock");
        merge_fill_projection_alias(&mut projection, child_order_id, stop_order_id);
        drop(projection);
        drop(index);
        if should_cancel {
            let mut cancel_client = self.detached_query_clone();
            let child_order_id = child_order_id.to_string();
            let mut tasks = self
                .reconciliation_tasks
                .lock()
                .expect("reconciliation_tasks lock");
            tasks.retain(|task| !task.is_finished());
            let task = get_runtime().spawn(async move {
                let identity = TbankBrokerOrderIdentity {
                    route: TbankBrokerOrderRoute::RegularOrder,
                    broker_order_id: child_order_id.clone(),
                };
                if let Err(error) = cancel_client.cancel_resolved_broker_order(identity).await {
                    tracing::error!(
                        %error,
                        %child_order_id,
                        "failed to drain pending T-Bank cancel after activated-stop child resolution"
                    );
                }
            });
            tasks.push(task);
        }
        should_cancel
    }

    fn record_activated_stop_child_alias(&self, stop_order_id: &str, child_order_id: &str) {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_activated_stop_child_alias(stop_order_id, child_order_id);
        merge_fill_projection_alias(
            &mut self.fill_projection.lock().expect("fill_projection lock"),
            child_order_id,
            stop_order_id,
        );
    }

    async fn record_activated_stop_child_mapping_and_drain_cancel(
        &mut self,
        client_order_id: &str,
        stop_order_id: &str,
        child_order_id: &str,
    ) {
        let should_cancel = {
            let mut index = self
                .broker_order_index
                .lock()
                .expect("broker_order_index lock");
            let should_cancel = index.record_activated_stop_child_mapping(
                client_order_id,
                stop_order_id,
                child_order_id,
            );
            merge_fill_projection_alias(
                &mut self.fill_projection.lock().expect("fill_projection lock"),
                child_order_id,
                stop_order_id,
            );
            should_cancel
        };
        self.record_pending_submit_venue_order_id(client_order_id, child_order_id);
        if should_cancel {
            let identity = TbankBrokerOrderIdentity {
                route: TbankBrokerOrderRoute::RegularOrder,
                broker_order_id: child_order_id.to_string(),
            };
            if let Err(error) = self.cancel_resolved_broker_order(identity).await {
                tracing::error!(
                    %error,
                    %client_order_id,
                    %child_order_id,
                    "failed to drain pending T-Bank cancel after activated-stop child resolution"
                );
            }
        }
    }

    async fn record_broker_order_mapping_and_drain_cancel(
        &mut self,
        route: TbankBrokerOrderRoute,
        client_order_id: &str,
        venue_order_id: &str,
    ) {
        let should_cancel =
            self.record_broker_order_mapping(route, client_order_id, venue_order_id);
        self.record_pending_submit_venue_order_id(client_order_id, venue_order_id);
        if should_cancel {
            let identity = TbankBrokerOrderIdentity {
                route,
                broker_order_id: venue_order_id.to_string(),
            };
            if let Err(error) = self.cancel_resolved_broker_order(identity).await {
                tracing::error!(
                    %error,
                    %client_order_id,
                    route = ?route,
                    "failed to drain pending T-Bank cancel after broker order mapping"
                );
            }
        }
    }

    async fn record_regular_order_alias_and_drain_cancel(
        &mut self,
        client_order_id: &str,
        canonical_order_id: &str,
        current_order_id: &str,
    ) {
        let should_cancel = {
            let mut index = self
                .broker_order_index
                .lock()
                .expect("broker_order_index lock");
            let should_cancel = index.record_regular_order_alias(
                client_order_id,
                canonical_order_id,
                current_order_id,
            );
            let mut projection = self.fill_projection.lock().expect("fill_projection lock");
            merge_fill_projection_alias(&mut projection, current_order_id, canonical_order_id);
            should_cancel
        };
        self.record_pending_submit_venue_order_id(client_order_id, current_order_id);
        if should_cancel {
            let identity = TbankBrokerOrderIdentity {
                route: TbankBrokerOrderRoute::RegularOrder,
                broker_order_id: current_order_id.to_string(),
            };
            if let Err(error) = self.cancel_resolved_broker_order(identity).await {
                tracing::error!(
                    %error,
                    %client_order_id,
                    %current_order_id,
                    "failed to drain pending T-Bank cancel after regular order alias resolution"
                );
            }
        }
    }

    fn record_pending_submit_venue_order_id(&self, client_order_id: &str, venue_order_id: &str) {
        if client_order_id.is_empty() || venue_order_id.is_empty() {
            return;
        }
        if let Some(pending) = self
            .pending_submits
            .lock()
            .expect("pending_submits lock")
            .get_mut(client_order_id)
        {
            pending.venue_order_id = Some(venue_order_id.to_string());
        }
    }

    fn record_broker_order_route(&self, route: TbankBrokerOrderRoute, client_order_id: &str) {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_client_order_route(route, client_order_id);
    }

    fn remove_unresolved_broker_order_route(&self, client_order_id: &str) {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .remove_unresolved_client_order_route(client_order_id);
    }

    fn record_broker_order_id(&self, route: TbankBrokerOrderRoute, venue_order_id: &str) {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_venue_order_id(route, venue_order_id);
    }

    fn record_managed_order_context(
        &self,
        client_order_id: &str,
        context: TbankManagedOrderContext,
    ) {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_managed_context(client_order_id, context);
    }

    fn record_submit_order_context(&self, order: &TbankSubmitOrder) {
        let mut index = self
            .broker_order_index
            .lock()
            .expect("broker_order_index lock");
        index.record_managed_context(
            order.client_order_id.as_str(),
            TbankManagedOrderContext {
                side: Some(order.side),
                order_type: Some(order.order_type),
                report_order_type: None,
                time_in_force: Some(order.time_in_force),
                quantity_units: Some(order.quantity_units),
                trailing: order.trailing,
            },
        );
    }

    fn get_or_allocate_broker_request_id(&self, client_order_id: &str) -> Result<String> {
        let deterministic = tbank_broker_request_id_for_client_order_id(client_order_id);
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .get_or_allocate_request_mapping(client_order_id, Some(deterministic.as_str()))
            .map_err(TbankAdapterError::ConfigError)
    }

    fn ensure_broker_request_mapping(&self, order: &TbankSubmitOrder) -> Result<()> {
        let deterministic =
            tbank_broker_request_id_for_client_order_id(order.client_order_id.as_str());
        if order.broker_request_id != deterministic {
            return Err(TbankAdapterError::ConfigError(format!(
                "custom broker request id {} is unsupported for client order {}; use deterministic id {deterministic}",
                order.broker_request_id, order.client_order_id
            )));
        }
        let mapped = self.get_or_allocate_broker_request_id(order.client_order_id.as_str())?;
        if mapped != order.broker_request_id {
            return Err(TbankAdapterError::ConfigError(format!(
                "client order {} is already mapped to broker request id {mapped}, refusing conflicting id {}",
                order.client_order_id, order.broker_request_id
            )));
        }
        Ok(())
    }

    fn record_stop_order_context(
        &self,
        client_order_id: &str,
        stop: &StopOrder,
        metadata: &TbankInstrumentMetadata,
    ) {
        let existing = self
            .broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .managed_context_for_client_order_id(client_order_id);
        let trailing = match trailing_params_from_stop(stop) {
            Ok(Some(params)) => Some(params),
            Ok(None) => existing.as_ref().and_then(|context| context.trailing),
            Err(error) => {
                tracing::warn!(%error, "could not preserve external T-Bank trailing-stop context");
                existing.as_ref().and_then(|context| context.trailing)
            }
        };
        self.record_managed_order_context(
            client_order_id,
            TbankManagedOrderContext {
                side: existing
                    .as_ref()
                    .and_then(|context| context.side)
                    .or_else(|| tbank_side_from_stop_direction(stop.direction)),
                order_type: existing
                    .as_ref()
                    .and_then(|context| context.order_type)
                    .or_else(|| tbank_order_type_from_stop_order(stop)),
                report_order_type: existing
                    .as_ref()
                    .and_then(|context| context.report_order_type)
                    .or_else(|| Some(nautilus_stop_order_type(stop))),
                time_in_force: existing
                    .as_ref()
                    .and_then(|context| context.time_in_force)
                    .or(Some(TimeInForce::Gtc)),
                quantity_units: existing
                    .as_ref()
                    .and_then(|context| context.quantity_units)
                    .or_else(|| {
                        Some(Decimal::from(stop.lots_requested) * Decimal::from(metadata.lot))
                    }),
                trailing,
            },
        );
    }

    fn managed_order_type_for_client_order_id(
        &self,
        client_order_id: Option<&str>,
    ) -> Option<crate::common::TbankOrderType> {
        client_order_id.and_then(|client_order_id| {
            self.broker_order_index
                .lock()
                .expect("broker_order_index lock")
                .managed_context_for_client_order_id(client_order_id)
                .and_then(|context| context.order_type)
        })
    }

    fn known_broker_order_identity(
        &self,
        client_order_id: Option<&ClientOrderId>,
        venue_order_id: Option<&VenueOrderId>,
    ) -> Option<TbankBrokerOrderIdentity> {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .identity_for(
                client_order_id.as_ref().map(|id| id.as_str()),
                venue_order_id.as_ref().map(|id| id.as_str()),
            )
    }

    fn known_regular_broker_order_ids(&self) -> Vec<String> {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .known_regular_broker_order_ids()
    }

    fn unresolved_regular_request_mappings(&self) -> Vec<(String, String)> {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .unresolved_regular_request_mappings()
    }

    fn known_stop_broker_order_ids(&self) -> Vec<String> {
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .known_stop_broker_order_ids()
    }

    async fn resolve_cancel_target(
        &mut self,
        client_order_id: &str,
        venue_order_id: Option<&str>,
    ) -> Result<TbankCancelTarget> {
        let venue_order_id = venue_order_id.filter(|venue_order_id| !venue_order_id.is_empty());
        let known_identity = {
            self.broker_order_index
                .lock()
                .expect("broker_order_index lock")
                .identity_for(Some(client_order_id), venue_order_id)
        };
        if let Some(identity) = known_identity {
            return Ok(TbankCancelTarget::Ready(identity));
        }
        let pending_route = {
            let index = self
                .broker_order_index
                .lock()
                .expect("broker_order_index lock");
            index.route_for_client_order_id(client_order_id)
        };
        if let Some(route) = pending_route {
            if let Some(venue_order_id) = venue_order_id {
                return Ok(TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
                    route,
                    broker_order_id: venue_order_id.to_string(),
                }));
            }
            self.broker_order_index
                .lock()
                .expect("broker_order_index lock")
                .record_pending_cancel(client_order_id);
            return Ok(TbankCancelTarget::Pending {
                route,
                client_order_id: client_order_id.to_string(),
            });
        }
        if venue_order_id.is_some()
            && let Some(identity) = self.stop_order_identity_from_broker(venue_order_id).await?
        {
            return Ok(TbankCancelTarget::Ready(identity));
        }
        if let Some(venue_order_id) = venue_order_id {
            return Ok(TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
                route: TbankBrokerOrderRoute::RegularOrder,
                broker_order_id: venue_order_id.to_string(),
            }));
        }
        Err(TbankAdapterError::BrokerOrderIdentityUnresolved(format!(
            "client order {client_order_id} has no resolved broker order ID"
        )))
    }

    async fn recover_ambiguous_cancel(
        &mut self,
        identity: TbankBrokerOrderIdentity,
    ) -> Result<TbankCancelRecoveryOutcome> {
        self.unresolved_cancellations
            .lock()
            .expect("unresolved_cancellations lock")
            .insert(identity.clone());
        let mut last_error = None;
        for attempt in 0..CANCEL_OUTCOME_RECOVERY_ATTEMPTS {
            tokio::time::sleep(crate::grpc::retry::backoff_duration(
                &self.config.reconnect_policy,
                attempt,
            ))
            .await;
            match self
                .cancel_resolved_broker_order_unchecked(identity.clone())
                .await
            {
                Ok(()) => {
                    self.unresolved_cancellations
                        .lock()
                        .expect("unresolved_cancellations lock")
                        .remove(&identity);
                    tracing::info!(
                        attempt = attempt + 1,
                        "recovered ambiguous T-Bank cancel outcome"
                    );
                    return Ok(TbankCancelRecoveryOutcome::Canceled);
                }
                Err(error)
                    if classify_cancel_failure(&error) == CancelFailureKind::OutcomeUnknown =>
                {
                    tracing::warn!(
                        %error,
                        attempt = attempt + 1,
                        "T-Bank cancel outcome is still ambiguous"
                    );
                    last_error = Some(error);
                }
                Err(error) => {
                    tracing::info!(
                        %error,
                        attempt = attempt + 1,
                        "T-Bank cancel retry reached a terminal broker response"
                    );
                    match self
                        .reconcile_cancel_after_terminal_response(&identity)
                        .await
                    {
                        Ok(true) => {
                            tracing::info!(
                                attempt = attempt + 1,
                                "reconciled ambiguous T-Bank cancel as completed"
                            );
                            return Ok(TbankCancelRecoveryOutcome::Canceled);
                        }
                        Ok(false) => return Ok(TbankCancelRecoveryOutcome::Active),
                        Err(reconciliation_error) => {
                            tracing::warn!(
                                %reconciliation_error,
                                attempt = attempt + 1,
                                "could not reconcile terminal T-Bank cancel response"
                            );
                            return Err(reconciliation_error);
                        }
                    }
                }
            }
        }
        tracing::warn!(
            attempts = CANCEL_OUTCOME_RECOVERY_ATTEMPTS,
            "T-Bank cancel outcome remained ambiguous after bounded retries"
        );
        Err(last_error.unwrap_or_else(|| {
            TbankAdapterError::ConfigError("T-Bank cancel outcome remained unresolved".to_string())
        }))
    }

    async fn reconcile_cancel_after_terminal_response(
        &mut self,
        identity: &TbankBrokerOrderIdentity,
    ) -> Result<bool> {
        let broker_order_id = identity.broker_order_id.as_str();
        let cancelled = match identity.route {
            TbankBrokerOrderRoute::RegularOrder => {
                let state = self.query_order(broker_order_id).await?;
                match TbankOrderExecutionReportStatus::try_from(state.execution_report_status).ok()
                {
                    Some(TbankOrderExecutionReportStatus::ExecutionReportStatusCancelled) => true,
                    Some(
                        TbankOrderExecutionReportStatus::ExecutionReportStatusNew
                        | TbankOrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
                        | TbankOrderExecutionReportStatus::ExecutionReportStatusFill
                        | TbankOrderExecutionReportStatus::ExecutionReportStatusRejected,
                    ) => false,
                    _ => {
                        return Err(TbankAdapterError::ConfigError(format!(
                            "T-Bank order {broker_order_id} has unknown status during cancel reconciliation"
                        )));
                    }
                }
            }
            TbankBrokerOrderRoute::StopOrder => {
                let stop = self
                    .query_stop_orders_for_reconciliation(None)
                    .await?
                    .stop_orders
                    .into_iter()
                    .find(|order| order.stop_order_id == broker_order_id)
                    .ok_or_else(|| {
                        TbankAdapterError::ConfigError(format!(
                            "T-Bank stop order {broker_order_id} was absent during cancel reconciliation"
                        ))
                    })?;
                match StopOrderStatusOption::try_from(stop.status).ok() {
                    Some(StopOrderStatusOption::StopOrderStatusCanceled) => true,
                    Some(
                        StopOrderStatusOption::StopOrderStatusActive
                        | StopOrderStatusOption::StopOrderStatusExecuted
                        | StopOrderStatusOption::StopOrderStatusExpired,
                    ) => false,
                    _ => {
                        return Err(TbankAdapterError::ConfigError(format!(
                            "T-Bank stop order {broker_order_id} has unknown status during cancel reconciliation"
                        )));
                    }
                }
            }
        };
        self.unresolved_cancellations
            .lock()
            .expect("unresolved_cancellations lock")
            .remove(identity);
        Ok(cancelled)
    }

    #[cfg(test)]
    fn cancellation_is_unresolved(&self, identity: &TbankBrokerOrderIdentity) -> bool {
        self.unresolved_cancellations
            .lock()
            .expect("unresolved_cancellations lock")
            .contains(identity)
    }

    async fn stop_order_identity_from_broker(
        &mut self,
        venue_order_id: Option<&str>,
    ) -> Result<Option<TbankBrokerOrderIdentity>> {
        let stop = self.stop_order_from_broker(venue_order_id).await?;
        let Some(stop) = stop else {
            return Ok(None);
        };
        self.record_broker_order_id(
            TbankBrokerOrderRoute::StopOrder,
            stop.stop_order_id.as_str(),
        );
        Ok(Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: stop.stop_order_id,
        }))
    }

    async fn stop_order_from_broker(
        &mut self,
        venue_order_id: Option<&str>,
    ) -> Result<Option<StopOrder>> {
        let stops = match self.query_stop_orders_for_reconciliation(None).await {
            Ok(response) => response.stop_orders,
            // A venue may expose only the regular-order API in a test or reduced
            // deployment. Preserve the regular-order fallback in that case.
            Err(TbankAdapterError::GrpcStatus {
                code: tonic::Code::Unimplemented,
                ..
            }) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(stops.into_iter().find(|stop| {
            venue_order_id.is_some_and(|venue_order_id| {
                stop.stop_order_id == venue_order_id
                    || stop.exchange_order_id.as_deref() == Some(venue_order_id)
            })
        }))
    }

    fn record_pending_submit(&self, order: &TbankSubmitOrder, submitted_ts: UnixNanos) {
        self.pending_submits
            .lock()
            .expect("pending_submits lock")
            .insert(
                order.client_order_id.clone(),
                TbankPendingSubmit {
                    instrument_id: order.instrument_id.clone(),
                    submitted_ts,
                    quantity_units: order.quantity_units,
                    side: order.side,
                    order_type: order.order_type,
                    time_in_force: order.time_in_force,
                    trailing: order.trailing,
                    venue_order_id: None,
                    last_reconciliation_ts: None,
                    stage: TbankPendingSubmitStage::Submitted,
                },
            );
    }

    fn prepare_submit_route(&self, client_order_id: &ClientOrderId, order_type: OrderType) {
        let route = if matches!(
            order_type,
            OrderType::StopMarket
                | OrderType::MarketIfTouched
                | OrderType::TrailingStopMarket
                | OrderType::TrailingStopLimit
        ) {
            TbankBrokerOrderRoute::StopOrder
        } else {
            TbankBrokerOrderRoute::RegularOrder
        };
        self.broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .record_client_order_route(route, client_order_id.as_str());
    }

    fn mark_pending_submit_stage(
        &self,
        client_order_id: &str,
        stage: TbankPendingSubmitStage,
        reconciliation_ts: Option<UnixNanos>,
    ) {
        if let Some(pending) = self
            .pending_submits
            .lock()
            .expect("pending_submits lock")
            .get_mut(client_order_id)
        {
            pending.stage = stage;
            if let Some(ts) = reconciliation_ts {
                pending.last_reconciliation_ts = Some(ts);
            }
            tracing::debug!(
                instrument_id = %pending.instrument_id,
                submitted_ts = %pending.submitted_ts,
                quantity_units = %pending.quantity_units,
                side = ?pending.side,
                order_type = ?pending.order_type,
                stage = ?pending.stage,
                "updated T-Bank pending submit state"
            );
        }
    }

    fn mark_pending_submit_report(&self, report: &OrderStatusReport) {
        mark_pending_submit_order_report(&self.pending_submits, report);
    }

    fn pending_submit_timestamp(&self, client_order_id: &str) -> Option<UnixNanos> {
        self.pending_submits
            .lock()
            .expect("pending_submits lock")
            .get(client_order_id)
            .map(|pending| pending.submitted_ts)
    }

    fn mark_pending_submit_fill_report(&self, report: &FillReport) {
        mark_pending_submit_fill_report(&self.pending_submits, report);
    }

    fn spawn_submit_outcome_recovery(
        &self,
        order: TbankSubmitOrder,
        metadata: TbankInstrumentMetadata,
        ts_init: UnixNanos,
        emitter: ExecutionEventEmitter,
    ) {
        let mut client = self.clone();
        let policy = self.config.reconnect_policy.clone();
        self.spawn_mutating_followup_task(async move {
            for attempt in 0..SUBMIT_OUTCOME_RECOVERY_ATTEMPTS {
                let delay = crate::grpc::retry::backoff_duration(&policy, attempt);
                tokio::time::sleep(delay).await;
                let reconciliation_ts = current_unix_nanos();
                match client
                    .reconcile_submit_outcome(&order, &metadata, ts_init)
                    .await
                {
                    Ok(Some(reconciled)) => {
                        client.mark_pending_submit_report(&reconciled.order_report);
                        for report in order_status_execution_reports(
                            reconciled.order_report,
                            reconciled.fill_reports,
                        ) {
                            emitter.send_execution_report(report);
                        }
                        tracing::info!(
                            client_order_id = %order.client_order_id,
                            attempt = attempt + 1,
                            "recovered unresolved T-Bank submit outcome"
                        );
                        return;
                    }
                    Ok(None) => {
                        client.mark_pending_submit_stage(
                            order.client_order_id.as_str(),
                            TbankPendingSubmitStage::Unknown,
                            Some(reconciliation_ts),
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            client_order_id = %order.client_order_id,
                            attempt = attempt + 1,
                            "T-Bank submit outcome recovery attempt failed"
                        );
                    }
                }
            }
            tracing::warn!(
                client_order_id = %order.client_order_id,
                attempts = SUBMIT_OUTCOME_RECOVERY_ATTEMPTS,
                "T-Bank submit outcome remained unresolved after background recovery"
            );
        });
    }

    /// Submits an order to T-Bank.
    pub async fn submit_order(
        &mut self,
        order: &TbankSubmitOrder,
        instrument: &TbankInstrumentMetadata,
    ) -> Result<TbankSubmitResponse> {
        let request_timeout = self.config.request_timeout;
        if let Err(error) = self.ensure_broker_request_mapping(order) {
            self.remove_unresolved_broker_order_route(order.client_order_id.as_str());
            return Err(error);
        }
        self.record_pending_submit(order, current_unix_nanos());
        self.submit_order_request(order, instrument, request_timeout)
            .await
    }

    async fn submit_order_request(
        &mut self,
        order: &TbankSubmitOrder,
        instrument: &TbankInstrumentMetadata,
        request_timeout: Duration,
    ) -> Result<TbankSubmitResponse> {
        if let Err(error) = self.config.ensure_submit_allowed() {
            self.remove_unresolved_broker_order_route(order.client_order_id.as_str());
            self.mark_pending_submit_stage(
                order.client_order_id.as_str(),
                TbankPendingSubmitStage::Rejected,
                None,
            );
            return Err(error);
        }
        let account_id = match self.config.resolve_account_id() {
            Ok(account_id) => account_id,
            Err(error) => {
                self.remove_unresolved_broker_order_route(order.client_order_id.as_str());
                self.mark_pending_submit_stage(
                    order.client_order_id.as_str(),
                    TbankPendingSubmitStage::Rejected,
                    None,
                );
                return Err(error);
            }
        };

        let service = order.service(self.config.environment);
        let route = broker_order_route_for_submit(order, service);
        self.record_broker_order_route(route, order.client_order_id.as_str());
        self.record_submit_order_context(order);

        // Re-check at the mutation boundary in case disconnect invalidated this runtime clone
        // while the submit pipeline was resolving instrument metadata.
        if let Err(error) = self.ensure_lifecycle_active() {
            self.remove_unresolved_broker_order_route(order.client_order_id.as_str());
            self.mark_pending_submit_stage(
                order.client_order_id.as_str(),
                TbankPendingSubmitStage::Rejected,
                None,
            );
            return Err(error);
        }

        let result = match service {
            TbankExecutionService::LiveOrders => {
                match build_post_order_request(order, &account_id, instrument) {
                    Ok(request) => match self.clients_mut() {
                        Ok(clients) => clients
                            .orders
                            .post_order(with_timeout(request, request_timeout))
                            .await
                            .map_err(TbankAdapterError::from)
                            .map(|response| TbankSubmitResponse::Order(response.into_inner())),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            TbankExecutionService::LiveStopOrders => {
                match build_post_stop_order_request(order, &account_id, instrument) {
                    Ok(request) => match self.clients_mut() {
                        Ok(clients) => clients
                            .stop_orders
                            .post_stop_order(with_timeout(request, request_timeout))
                            .await
                            .map_err(TbankAdapterError::from)
                            .map(|response| TbankSubmitResponse::StopOrder(response.into_inner())),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            TbankExecutionService::Sandbox => match order.order_type {
                crate::common::TbankOrderType::Market | crate::common::TbankOrderType::Limit => {
                    match build_post_order_request(order, &account_id, instrument) {
                        Ok(request) => match self.clients_mut() {
                            Ok(clients) => clients
                                .sandbox
                                .post_sandbox_order(with_timeout(request, request_timeout))
                                .await
                                .map_err(TbankAdapterError::from)
                                .map(|response| TbankSubmitResponse::Order(response.into_inner())),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    }
                }
                crate::common::TbankOrderType::StopMarket
                | crate::common::TbankOrderType::MarketIfTouched
                | crate::common::TbankOrderType::TrailingStopMarket
                | crate::common::TbankOrderType::TrailingStopLimit => {
                    const RPC: &str = "SandboxService.PostSandboxStopOrder";
                    tracing::debug!(
                        rpc = RPC,
                        instrument_id = %order.instrument_id,
                        order_type = ?order.order_type,
                        "calling T-Bank RPC"
                    );
                    match build_post_stop_order_request(order, &account_id, instrument) {
                        Ok(request) => match self.clients_mut() {
                            Ok(clients) => clients
                                .sandbox
                                .post_sandbox_stop_order(with_timeout(request, request_timeout))
                                .await
                                .map_err(TbankAdapterError::from)
                                .map(|response| {
                                    TbankSubmitResponse::StopOrder(response.into_inner())
                                }),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    }
                }
            },
        };

        if let Err(error) = &result {
            let rpc = match service {
                TbankExecutionService::Sandbox
                    if matches!(
                        order.order_type,
                        crate::common::TbankOrderType::StopMarket
                            | crate::common::TbankOrderType::MarketIfTouched
                            | crate::common::TbankOrderType::TrailingStopMarket
                            | crate::common::TbankOrderType::TrailingStopLimit
                    ) =>
                {
                    "SandboxService.PostSandboxStopOrder"
                }
                TbankExecutionService::Sandbox => "SandboxService.PostSandboxOrder",
                TbankExecutionService::LiveOrders => "OrdersService.PostOrder",
                TbankExecutionService::LiveStopOrders => "StopOrdersService.PostStopOrder",
            };
            log_tbank_rpc_failure(rpc, error);
        }

        match result {
            Ok(response) => {
                let (route, broker_order_id, identity_name) = match &response {
                    TbankSubmitResponse::Order(response) => (
                        TbankBrokerOrderRoute::RegularOrder,
                        response.order_id.as_str(),
                        "order_id",
                    ),
                    TbankSubmitResponse::StopOrder(response) => (
                        TbankBrokerOrderRoute::StopOrder,
                        response.stop_order_id.as_str(),
                        "stop_order_id",
                    ),
                };
                if broker_order_id.is_empty() {
                    let error = TbankAdapterError::SubmitOutcomeUnknown(format!(
                        "submit response is missing {identity_name}"
                    ));
                    self.mark_pending_submit_stage(
                        order.client_order_id.as_str(),
                        TbankPendingSubmitStage::Unknown,
                        Some(current_unix_nanos()),
                    );
                    return Err(error);
                }
                self.mark_pending_submit_stage(
                    order.client_order_id.as_str(),
                    pending_stage_after_submit_response(&response),
                    None,
                );
                self.record_broker_order_mapping_and_drain_cancel(
                    route,
                    order.client_order_id.as_str(),
                    broker_order_id,
                )
                .await;
                Ok(response)
            }
            Err(error) => {
                if matches!(
                    classify_submit_failure(&error),
                    SubmitFailureKind::LocalRejected | SubmitFailureKind::BrokerRejected
                ) {
                    self.remove_unresolved_broker_order_route(order.client_order_id.as_str());
                    self.mark_pending_submit_stage(
                        order.client_order_id.as_str(),
                        TbankPendingSubmitStage::Rejected,
                        None,
                    );
                }
                Err(error)
            }
        }
    }

    /// Cancels a regular T-Bank order.
    pub async fn cancel_order(&mut self, order_id: &str) -> Result<CancelOrderResponse> {
        let account_id = self.config.resolve_account_id()?;
        let request = CancelOrderRequest {
            account_id,
            order_id: order_id.to_string(),
            order_id_type: Some(OrderIdType::Exchange as i32),
        };
        let request = with_timeout(request, self.config.request_timeout);

        if self.config.environment.is_live() {
            Ok(self
                .clients_mut()?
                .orders
                .cancel_order(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        } else {
            Ok(self
                .clients_mut()?
                .sandbox
                .cancel_sandbox_order(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        }
    }

    /// Cancels a T-Bank stop order.
    pub async fn cancel_stop_order(
        &mut self,
        stop_order_id: &str,
    ) -> Result<CancelStopOrderResponse> {
        let request = CancelStopOrderRequest {
            account_id: self.config.resolve_account_id()?,
            stop_order_id: stop_order_id.to_string(),
        };
        let request = with_timeout(request, self.config.request_timeout);

        if self.config.environment.is_live() {
            Ok(self
                .clients_mut()?
                .stop_orders
                .cancel_stop_order(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        } else {
            Ok(self
                .clients_mut()?
                .sandbox
                .cancel_sandbox_stop_order(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        }
    }

    async fn cancel_resolved_broker_order(
        &mut self,
        identity: TbankBrokerOrderIdentity,
    ) -> Result<()> {
        self.ensure_lifecycle_active()?;
        let was_unresolved = self
            .unresolved_cancellations
            .lock()
            .expect("unresolved_cancellations lock")
            .contains(&identity);
        let result = self
            .cancel_resolved_broker_order_unchecked(identity.clone())
            .await;
        match &result {
            Ok(()) => {
                self.unresolved_cancellations
                    .lock()
                    .expect("unresolved_cancellations lock")
                    .remove(&identity);
            }
            Err(error) if classify_cancel_failure(error) == CancelFailureKind::OutcomeUnknown => {
                self.unresolved_cancellations
                    .lock()
                    .expect("unresolved_cancellations lock")
                    .insert(identity.clone());
            }
            Err(_) => {}
        }
        if was_unresolved
            && result.as_ref().is_err_and(|error| {
                classify_cancel_failure(error) != CancelFailureKind::OutcomeUnknown
            })
        {
            return match self
                .reconcile_cancel_after_terminal_response(&identity)
                .await
            {
                Ok(true) => Ok(()),
                Ok(false) => result,
                Err(error) => Err(error),
            };
        }
        result
    }

    async fn cancel_resolved_broker_order_unchecked(
        &mut self,
        identity: TbankBrokerOrderIdentity,
    ) -> Result<()> {
        let broker_order_id = identity.broker_order_id;
        match identity.route {
            TbankBrokerOrderRoute::RegularOrder => {
                TbankExecutionRuntime::cancel_order(self, broker_order_id.as_str()).await?;
            }
            TbankBrokerOrderRoute::StopOrder => {
                TbankExecutionRuntime::cancel_stop_order(self, broker_order_id.as_str()).await?;
            }
        }
        Ok(())
    }

    /// Cancels all open regular and stop orders for the configured account.
    pub async fn cancel_all_orders(&mut self) -> Result<usize> {
        self.ensure_lifecycle_active()?;
        let orders = self.query_open_orders().await?;
        let stops = self.query_stop_orders().await?;
        let identities = orders
            .orders
            .into_iter()
            .map(|order| TbankBrokerOrderIdentity {
                route: TbankBrokerOrderRoute::RegularOrder,
                broker_order_id: order.order_id,
            })
            .chain(
                stops
                    .stop_orders
                    .into_iter()
                    .map(|stop| TbankBrokerOrderIdentity {
                        route: TbankBrokerOrderRoute::StopOrder,
                        broker_order_id: stop.stop_order_id,
                    }),
            );
        let mut cancelled = 0;
        let mut first_error = None;
        for identity in identities {
            match self.cancel_resolved_broker_order(identity.clone()).await {
                Ok(()) => cancelled += 1,
                Err(error)
                    if classify_cancel_failure(&error) == CancelFailureKind::OutcomeUnknown =>
                {
                    match self.recover_ambiguous_cancel(identity.clone()).await {
                        Ok(TbankCancelRecoveryOutcome::Canceled) => cancelled += 1,
                        Ok(TbankCancelRecoveryOutcome::Active) => {
                            if first_error.is_none() {
                                first_error = Some(TbankAdapterError::ConfigError(
                                    "T-Bank cancel reconciliation confirmed the order remains active"
                                        .to_string(),
                                ));
                            }
                        }
                        Err(recovery_error) if first_error.is_none() => {
                            first_error = Some(recovery_error);
                        }
                        Err(_) => {}
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(cancelled), Err)
    }

    async fn query_order_with_id_type(
        &mut self,
        order_id: &str,
        order_id_type: OrderIdType,
    ) -> Result<OrderState> {
        let request = GetOrderStateRequest {
            account_id: self.config.resolve_account_id()?,
            order_id: order_id.to_string(),
            price_type: PriceType::Currency as i32,
            order_id_type: Some(order_id_type as i32),
        };
        let request = with_timeout(request, self.config.request_timeout);
        let rpc = if self.config.environment.is_live() {
            "OrdersService.GetOrderState"
        } else {
            "SandboxService.GetSandboxOrderState"
        };

        let response = if self.config.environment.is_live() {
            self.clients_mut()?
                .orders
                .get_order_state(request)
                .await
                .map(|response| response.into_inner())
        } else {
            self.clients_mut()?
                .sandbox
                .get_sandbox_order_state(request)
                .await
                .map(|response| response.into_inner())
        };
        match response {
            Ok(response) => Ok(response),
            Err(error) => {
                let error = TbankAdapterError::from(error);
                log_tbank_rpc_failure(rpc, &error);
                Err(error)
            }
        }
    }

    /// Queries a regular order by its broker order ID.
    pub async fn query_order(&mut self, order_id: &str) -> Result<OrderState> {
        self.query_order_with_id_type(order_id, OrderIdType::Exchange)
            .await
    }

    /// Queries a regular order by its broker request ID.
    pub async fn query_order_by_request_id(
        &mut self,
        order_request_id: &str,
    ) -> Result<OrderState> {
        self.query_order_with_id_type(order_request_id, OrderIdType::Request)
            .await
    }

    /// Queries all open regular orders for the configured account.
    pub async fn query_open_orders(&mut self) -> Result<GetOrdersResponse> {
        self.query_orders(false).await
    }

    /// Queries regular orders for the configured account.
    pub async fn query_orders(&mut self, include_terminal: bool) -> Result<GetOrdersResponse> {
        if include_terminal {
            let (from, _) = current_utc_day_bounds();
            self.query_orders_since(i128::from(from.seconds) * 1_000_000_000)
                .await
        } else {
            self.query_orders_with_filters(None).await
        }
    }

    async fn query_orders_since(&mut self, from_unix_nanos: i128) -> Result<GetOrdersResponse> {
        let to_unix_nanos = i128::from(current_unix_nanos().as_u64());
        let mut windows = VecDeque::from(order_filter_windows(from_unix_nanos, to_unix_nanos)?);
        let mut orders = Vec::new();
        let mut order_index = HashMap::<String, usize>::new();
        while let Some(filters) = windows.pop_front() {
            let response = self
                .query_orders_with_filters(Some(filters.clone()))
                .await?;
            if response.orders.len() >= 100 {
                let from = filters.from.as_ref().ok_or_else(|| {
                    TbankAdapterError::ConfigError(
                        "T-Bank saturated order-history window has no lower bound".to_string(),
                    )
                })?;
                let to = filters.to.as_ref().ok_or_else(|| {
                    TbankAdapterError::ConfigError(
                        "T-Bank saturated order-history window has no upper bound".to_string(),
                    )
                })?;
                let from_unix_nanos = i128::from(
                    timestamp_to_unix_nanos(from)
                        .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))?
                        .as_u64(),
                );
                let to_unix_nanos = i128::from(
                    timestamp_to_unix_nanos(to)
                        .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))?
                        .as_u64(),
                );
                if to_unix_nanos.saturating_sub(from_unix_nanos) <= 1 {
                    return Err(TbankAdapterError::ConfigError(
                        "T-Bank returned 100 orders in the minimum temporal window; complete order history cannot be recovered"
                            .to_string(),
                    ));
                }
                let midpoint = from_unix_nanos + (to_unix_nanos - from_unix_nanos) / 2;
                let left = get_orders_request::GetOrdersRequestFilters {
                    from: Some(unix_nanos_to_timestamp(from_unix_nanos)?),
                    to: Some(unix_nanos_to_timestamp(midpoint)?),
                    execution_status: filters.execution_status.clone(),
                };
                let right = get_orders_request::GetOrdersRequestFilters {
                    from: Some(unix_nanos_to_timestamp(midpoint)?),
                    to: Some(unix_nanos_to_timestamp(to_unix_nanos)?),
                    execution_status: filters.execution_status,
                };
                windows.push_front(right);
                windows.push_front(left);
                continue;
            }
            for order in response.orders {
                if let Some(index) = order_index.get(order.order_id.as_str()).copied() {
                    orders[index] = order;
                } else {
                    order_index.insert(order.order_id.clone(), orders.len());
                    orders.push(order);
                }
            }
        }
        Ok(GetOrdersResponse { orders })
    }

    async fn query_orders_with_filters(
        &mut self,
        advanced_filters: Option<get_orders_request::GetOrdersRequestFilters>,
    ) -> Result<GetOrdersResponse> {
        let request = GetOrdersRequest {
            account_id: self.config.resolve_account_id()?,
            advanced_filters,
        };
        let request = with_timeout(request, self.config.request_timeout);

        if self.config.environment.is_live() {
            Ok(self
                .clients_mut()?
                .orders
                .get_orders(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        } else {
            Ok(self
                .clients_mut()?
                .sandbox
                .get_sandbox_orders(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        }
    }

    /// Queries active stop orders for the configured account.
    pub async fn query_stop_orders(&mut self) -> Result<GetStopOrdersResponse> {
        self.query_stop_orders_by_status(StopOrderStatusOption::StopOrderStatusActive, None)
            .await
    }

    /// Queries stop orders needed for lifecycle reconciliation.
    pub async fn query_stop_orders_for_reconciliation(
        &mut self,
        from_unix_nanos: Option<i128>,
    ) -> Result<GetStopOrdersResponse> {
        let Some(from_unix_nanos) = from_unix_nanos else {
            return self
                .query_stop_orders_by_status(StopOrderStatusOption::StopOrderStatusAll, None)
                .await;
        };
        let active = self.query_stop_orders().await?;
        let terminal = self
            .query_stop_orders_by_status(
                StopOrderStatusOption::StopOrderStatusAll,
                Some((
                    unix_nanos_to_timestamp(from_unix_nanos)?,
                    unix_nanos_to_timestamp(i128::from(current_unix_nanos().as_u64()))?,
                )),
            )
            .await?;
        Ok(merge_stop_order_snapshots(active, terminal))
    }

    async fn append_missing_activated_stop_children(
        &mut self,
        order_states: &mut Vec<OrderState>,
        stops: &[StopOrder],
    ) -> Result<()> {
        let mut known_order_ids = order_states
            .iter()
            .map(|state| state.order_id.clone())
            .collect::<HashSet<_>>();
        let mut children = Vec::new();
        for stop in stops {
            if StopOrderStatusOption::try_from(stop.status).ok()
                != Some(StopOrderStatusOption::StopOrderStatusExecuted)
            {
                continue;
            }
            let Some(exchange_order_id) = stop
                .exchange_order_id
                .as_deref()
                .filter(|order_id| !order_id.is_empty())
            else {
                continue;
            };
            if known_order_ids.contains(exchange_order_id) {
                continue;
            }
            match self.query_order(exchange_order_id).await {
                Ok(state) => {
                    known_order_ids.insert(state.order_id.clone());
                    children.push(state);
                }
                Err(TbankAdapterError::GrpcStatus {
                    code: tonic::Code::NotFound,
                    message,
                }) => tracing::debug!(
                    %message,
                    "activated T-Bank stop child was absent from GetOrderState"
                ),
                Err(error) => return Err(error),
            }
        }
        order_states.extend(children);
        Ok(())
    }

    async fn query_stop_orders_by_status(
        &mut self,
        status: StopOrderStatusOption,
        range: Option<(prost_types::Timestamp, prost_types::Timestamp)>,
    ) -> Result<GetStopOrdersResponse> {
        let (from, to) = range
            .map(|(from, to)| (Some(from), Some(to)))
            .unwrap_or((None, None));
        let request = GetStopOrdersRequest {
            account_id: self.config.resolve_account_id()?,
            status: status as i32,
            from,
            to,
        };
        let request = with_timeout(request, self.config.request_timeout);
        let rpc = if self.config.environment.is_live() {
            "StopOrdersService.GetStopOrders"
        } else {
            "SandboxService.GetSandboxStopOrders"
        };

        let response = if self.config.environment.is_live() {
            self.clients_mut()?
                .stop_orders
                .get_stop_orders(request)
                .await
                .map(|response| response.into_inner())
        } else {
            self.clients_mut()?
                .sandbox
                .get_sandbox_stop_orders(request)
                .await
                .map(|response| response.into_inner())
        };
        match response {
            Ok(response) => Ok(response),
            Err(error) => {
                let error = TbankAdapterError::from(error);
                log_tbank_rpc_failure(rpc, &error);
                Err(error)
            }
        }
    }

    /// Queries the portfolio for the configured account.
    pub async fn query_portfolio(&mut self) -> Result<PortfolioResponse> {
        let request = PortfolioRequest {
            account_id: self.config.resolve_account_id()?,
            currency: None,
        };
        let request = with_timeout(request, self.config.request_timeout);
        let rpc = if self.config.environment.is_live() {
            "OperationsService.GetPortfolio"
        } else {
            "SandboxService.GetSandboxPortfolio"
        };

        let response = if self.config.environment.is_live() {
            self.clients_mut()?
                .operations
                .get_portfolio(request)
                .await
                .map(|response| response.into_inner())
        } else {
            self.clients_mut()?
                .sandbox
                .get_sandbox_portfolio(request)
                .await
                .map(|response| response.into_inner())
        };
        match response {
            Ok(response) => Ok(response),
            Err(error) => {
                let error = TbankAdapterError::from(error);
                log_tbank_rpc_failure(rpc, &error);
                Err(error)
            }
        }
    }

    /// Queries positions for the configured account.
    pub async fn query_positions(&mut self) -> Result<PositionsResponse> {
        let request = PositionsRequest {
            account_id: self.config.resolve_account_id()?,
        };
        let request = with_timeout(request, self.config.request_timeout);

        if self.config.environment.is_live() {
            Ok(self
                .clients_mut()?
                .operations
                .get_positions(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        } else {
            Ok(self
                .clients_mut()?
                .sandbox
                .get_sandbox_positions(request)
                .await
                .map_err(TbankAdapterError::from)?
                .into_inner())
        }
    }

    /// Queries executed operations and fills for the configured account.
    pub async fn query_fills(
        &mut self,
        instrument_uid: Option<String>,
        from_unix_nanos: Option<i128>,
        to_unix_nanos: Option<i128>,
    ) -> Result<GetOperationsByCursorResponse> {
        let mut request = GetOperationsByCursorRequest {
            account_id: self.config.resolve_account_id()?,
            instrument_id: instrument_uid,
            from: from_unix_nanos.map(unix_nanos_to_timestamp).transpose()?,
            to: to_unix_nanos.map(unix_nanos_to_timestamp).transpose()?,
            cursor: None,
            limit: Some(1000),
            operation_types: Vec::new(),
            state: Some(OperationState::Executed as i32),
            without_commissions: Some(false),
            without_trades: Some(false),
            without_overnights: Some(false),
        };

        let request_timeout = self.config.request_timeout;
        let mut response = GetOperationsByCursorResponse::default();
        loop {
            let mut page = if self.config.environment.is_live() {
                self.clients_mut()?
                    .operations
                    .get_operations_by_cursor(with_timeout(request.clone(), request_timeout))
                    .await
                    .map_err(TbankAdapterError::from)?
                    .into_inner()
            } else {
                self.clients_mut()?
                    .sandbox
                    .get_sandbox_operations_by_cursor(with_timeout(
                        request.clone(),
                        request_timeout,
                    ))
                    .await
                    .map_err(TbankAdapterError::from)?
                    .into_inner()
            };

            let has_next = page.has_next && !page.next_cursor.is_empty();
            let next_cursor = page.next_cursor.clone();
            response.items.append(&mut page.items);

            if !has_next {
                response.has_next = false;
                response.next_cursor.clear();
                break;
            }

            response.has_next = true;
            response.next_cursor = next_cursor.clone();
            request.cursor = Some(next_cursor);
        }

        Ok(response)
    }

    /// Reconciles an order submission by its broker request ID.
    pub async fn reconcile_order_by_request_id(
        &mut self,
        order_request_id: &str,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<TbankOrderReconciliationReports>> {
        let state = match self.query_order_by_request_id(order_request_id).await {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    %order_request_id,
                    %error,
                    "T-Bank order reconciliation found no broker order state"
                );
                return Ok(None);
            }
        };
        let account_id = self.account_id();
        let report = self
            .order_status_report_from_state_with_lots(account_id, state.clone(), ts_init)
            .await?;
        let fills = self.reconciled_fill_reports_from_state(&state, &report, ts_init)?;
        self.mark_pending_submit_report(&report);
        Ok(Some(TbankOrderReconciliationReports {
            order_report: report,
            fill_reports: fills,
        }))
    }

    /// Reconciles an ambiguous order-submission outcome.
    pub async fn reconcile_submit_outcome(
        &mut self,
        order: &TbankSubmitOrder,
        metadata: &TbankInstrumentMetadata,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<TbankOrderReconciliationReports>> {
        match order.service(self.config.environment) {
            TbankExecutionService::LiveOrders => {
                self.reconcile_order_by_request_id(order.broker_request_id.as_str(), ts_init)
                    .await
            }
            TbankExecutionService::LiveStopOrders => {
                self.reconcile_stop_order_submit_outcome(order, metadata, ts_init)
                    .await
            }
            TbankExecutionService::Sandbox
                if matches!(
                    order.order_type,
                    crate::common::TbankOrderType::StopMarket
                        | crate::common::TbankOrderType::MarketIfTouched
                        | crate::common::TbankOrderType::TrailingStopMarket
                        | crate::common::TbankOrderType::TrailingStopLimit
                ) =>
            {
                self.reconcile_stop_order_submit_outcome(order, metadata, ts_init)
                    .await
            }
            TbankExecutionService::Sandbox => {
                self.reconcile_order_by_request_id(order.broker_request_id.as_str(), ts_init)
                    .await
            }
        }
    }

    async fn reconcile_stop_order_submit_outcome(
        &mut self,
        order: &TbankSubmitOrder,
        metadata: &TbankInstrumentMetadata,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<TbankOrderReconciliationReports>> {
        let Some(submitted_ts) = self.pending_submit_timestamp(order.client_order_id.as_str())
        else {
            return Err(anyhow::anyhow!(
                "T-Bank stop-order submit has no pending timestamp for bounded reconciliation"
            ));
        };
        let query_from = stop_order_submit_reconciliation_from(submitted_ts);
        // GetStopOrders omits order_request_id. Query only active orders and terminal orders from
        // the bounded submit window, then accept exactly one full wire-shape match. Ambiguity must
        // remain outcome-unknown.
        let stop_orders = self
            .query_stop_orders_for_reconciliation(Some(query_from))
            .await?
            .stop_orders;
        let candidates = {
            let broker_order_index = self
                .broker_order_index
                .lock()
                .expect("broker_order_index lock");
            stop_orders
                .into_iter()
                .filter(|stop| {
                    broker_order_index
                        .identity_for(None, Some(stop.stop_order_id.as_str()))
                        .is_none()
                })
                .filter(|stop| stop_order_is_after_submit(stop, submitted_ts, query_from))
                .filter(|stop| stop_order_matches_submit(order, metadata, stop))
                .collect::<Vec<_>>()
        };
        let [stop] = candidates.as_slice() else {
            if candidates.len() > 1 {
                tracing::warn!(
                    client_order_id = %order.client_order_id,
                    candidates = candidates.len(),
                    "T-Bank stop-order submit reconciliation found ambiguous broker candidates"
                );
            }
            return Ok(None);
        };
        let report = self
            .stop_order_status_report_for_stop(
                Some(ClientOrderId::from(order.client_order_id.as_str())),
                stop.clone(),
                ts_init,
            )
            .await?;
        Ok(report.map(|order_report| TbankOrderReconciliationReports {
            order_report,
            fill_reports: Vec::new(),
        }))
    }

    fn reconciled_fill_reports_from_state(
        &self,
        state: &OrderState,
        report: &OrderStatusReport,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<FillReport>> {
        if state.lots_executed <= 0 {
            return Ok(Vec::new());
        }
        let order_id = if !state.order_id.is_empty() {
            state.order_id.as_str()
        } else {
            report.venue_order_id.as_str()
        };
        let trade_id =
            synthetic_fill_trade_id("reconciled", order_id, report.filled_qty.as_decimal());
        self.project_order_status_fill_report(
            report,
            order_id,
            trade_id.as_str(),
            ts_init,
            Some(state.order_request_id.as_str()),
            commission_from_money_value(state.executed_commission.as_ref())?,
        )
        .map(|report| report.into_iter().collect())
    }

    fn project_order_status_fill_report(
        &self,
        report: &OrderStatusReport,
        order_id: &str,
        trade_id: &str,
        ts_init: UnixNanos,
        order_request_id: Option<&str>,
        cumulative_commission: Option<Money>,
    ) -> anyhow::Result<Option<FillReport>> {
        let cumulative_quantity = report.filled_qty.as_decimal();
        if cumulative_quantity <= Decimal::ZERO {
            return Ok(None);
        }
        let Some(cumulative_avg_px) = report.avg_px else {
            tracing::warn!(
                order_id,
                order_request_id = order_request_id.unwrap_or(""),
                "skipping order-status fill report because T-Bank order state has no execution average price"
            );
            return Ok(None);
        };
        let Some(order_side) = report.order_side else {
            tracing::warn!(
                order_id,
                order_request_id = order_request_id.unwrap_or(""),
                "skipping order-status fill report because T-Bank order state has unknown direction"
            );
            return Ok(None);
        };
        let cumulative_notional = cumulative_avg_px * cumulative_quantity;
        let Some(projected) = project_cumulative_order_fill(
            &self.fill_projection,
            order_id,
            cumulative_quantity,
            cumulative_notional,
            cumulative_commission,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(FillReport::new(
            report.account_id,
            report.instrument_id,
            report.venue_order_id,
            trade_id.into(),
            order_side,
            projected.quantity,
            projected.price,
            projected.commission,
            LiquiditySide::NoLiquiditySide,
            report.client_order_id,
            None,
            report.ts_last,
            ts_init,
            Some(UUID4::new()),
        )))
    }

    fn project_trade_fill_report(&self, report: FillReport) -> anyhow::Result<Option<FillReport>> {
        let report = project_managed_trade_fill_report(
            &self.broker_order_index,
            &self.fill_projection,
            report,
        )?;
        if let Some(report) = report.as_ref() {
            self.mark_pending_submit_fill_report(report);
        }
        Ok(report)
    }

    fn clients_mut(&mut self) -> Result<&mut TbankGrpcClients<TbankAuthInterceptor>> {
        self.clients.as_mut().ok_or_else(|| {
            TbankAdapterError::ConfigError("execution client is not connected".to_string())
        })
    }

    fn detached_query_clone(&self) -> Self {
        Self {
            client_id: self.client_id,
            account_id: self.account_id,
            config: self.config.clone(),
            clients: self.clients.clone(),
            instruments: self.instruments.clone(),
            futures_margin_refreshed_at: self.futures_margin_refreshed_at.clone(),
            futures_margin_inflight: self.futures_margin_inflight.clone(),
            futures_margin_generation: self.futures_margin_generation.clone(),
            futures_margin_generation_id: self.futures_margin_generation_id,
            broker_order_index: self.broker_order_index.clone(),
            fill_projection: self.fill_projection.clone(),
            order_status_projection: self.order_status_projection.clone(),
            position_projection: self.position_projection.clone(),
            pending_submits: self.pending_submits.clone(),
            unresolved_trade_fills: self.unresolved_trade_fills.clone(),
            unresolved_cancellations: self.unresolved_cancellations.clone(),
            stream_tasks: Arc::new(Mutex::new(Vec::new())),
            reconciliation_tasks: self.reconciliation_tasks.clone(),
            command_tasks: self.command_tasks.clone(),
            lifecycle_active: self.lifecycle_active.clone(),
            emitter: self.emitter.clone(),
        }
    }

    fn spawn_execution_streams(&mut self) -> anyhow::Result<()> {
        if !self.emitter.is_initialized() {
            tracing::debug!("Nautilus execution event sender not initialized; skipping streams");
            return Ok(());
        }
        let Some(clients) = self.clients.as_ref() else {
            return Ok(());
        };
        let account_id = self.config.resolve_account_id()?;
        let reconnect_reconciler =
            TbankReconnectReconciler::new(self.detached_query_clone(), self.emitter.clone());

        let mut order_stream = clients.orders_stream.clone();
        let order_account = account_id.clone();
        let order_reconnect_policy = self.config.reconnect_policy.clone();
        let order_context = TbankOrderStreamContext {
            emitter: self.emitter.clone(),
            query_client: self.detached_query_clone(),
            lifecycle_active: self.lifecycle_active.clone(),
            pending_submits: self.pending_submits.clone(),
            unresolved_trade_fills: self.unresolved_trade_fills.clone(),
            unresolved_cancellations: self.unresolved_cancellations.clone(),
            broker_order_index: self.broker_order_index.clone(),
            fill_projection: self.fill_projection.clone(),
            order_status_projection: self.order_status_projection.clone(),
            instruments: self.instruments.clone(),
            reconnect_policy: self.config.reconnect_policy.clone(),
            activated_stop_reconciliations: Arc::new(Mutex::new(HashSet::new())),
            regular_order_reconciliations: Arc::new(Mutex::new(HashSet::new())),
            reconciliation_tasks: self.reconciliation_tasks.clone(),
        };
        let trades_order_context = order_context.clone();
        let order_reconciler = reconnect_reconciler.clone();
        let order_last_observed_unix_nanos =
            Arc::new(AtomicU64::new(current_unix_nanos().as_u64()));
        self.stream_tasks
            .lock()
            .expect("stream_tasks lock")
            .push(get_runtime().spawn(async move {
                let request = OrderStateStreamRequest {
                    accounts: vec![order_account],
                    ping_delay_millis: None,
                };
                let mut attempt = 0;
                let mut stream_generation = 0_u64;
                let mut recovery_from = None;
                loop {
                    match order_stream.order_state_stream(request.clone()).await {
                        Ok(response) => {
                            stream_generation = stream_generation.saturating_add(1);
                            if recovery_from.is_none() {
                                attempt = 0;
                            }
                            let stream = response.into_inner();
                            let recovery_pending = Arc::new(AtomicBool::new(false));
                            if let Some(from_unix_nanos) = recovery_from {
                                let outcome = reconcile_after_stream_reopen(
                                    &order_reconciler,
                                    "order_state_stream",
                                    from_unix_nanos,
                                    &order_reconnect_policy,
                                )
                                .await;
                                if apply_reconnect_reconciliation_outcome(
                                    &mut recovery_from,
                                    from_unix_nanos,
                                    outcome,
                                ) {
                                    attempt = 0;
                                    order_last_observed_unix_nanos
                                        .store(current_unix_nanos().as_u64(), Ordering::Release);
                                } else {
                                    recovery_pending.store(true, Ordering::Release);
                                }
                            }
                            let publish = publish_order_state_stream(
                                stream,
                                order_context.clone(),
                                stream_generation,
                                recovery_pending.clone(),
                                order_last_observed_unix_nanos.clone(),
                            );
                            tokio::pin!(publish);
                            let stream_result = if let Some(from_unix_nanos) = recovery_from {
                                let reconciliation = reconcile_degraded_stream_until_complete(
                                    &order_reconciler,
                                    "order_state_stream_background",
                                    from_unix_nanos,
                                    &order_reconnect_policy,
                                );
                                tokio::pin!(reconciliation);
                                tokio::select! {
                                    result = &mut publish => result,
                                    () = &mut reconciliation => {
                                        recovery_from = None;
                                        attempt = 0;
                                        order_last_observed_unix_nanos
                                            .store(current_unix_nanos().as_u64(), Ordering::Release);
                                        recovery_pending.store(false, Ordering::Release);
                                        publish.await
                                    }
                                }
                            } else {
                                publish.await
                            };
                            match stream_result {
                                Ok(()) => {
                                    tracing::warn!("T-Bank order-state stream closed by server");
                                }
                                Err(error) => {
                                    tracing::warn!(%error, "T-Bank order-state stream closed with error");
                                }
                            }
                            recovery_from.get_or_insert_with(|| {
                                reconnect_reconciliation_from(&order_last_observed_unix_nanos)
                            });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "failed to open T-Bank order-state stream");
                        }
                    }
                    recovery_from.get_or_insert_with(|| {
                        reconnect_reconciliation_from(&order_last_observed_unix_nanos)
                    });
                    let delay = crate::grpc::retry::backoff_duration(&order_reconnect_policy, attempt);
                    attempt = attempt.saturating_add(1);
                    tracing::warn!(delay_ms = delay.as_millis(), "reopening T-Bank order-state stream after disconnect");
                    tokio::time::sleep(delay).await;
                }
            }));

        let mut trades_stream = clients.orders_stream.clone();
        let trades_account = account_id.clone();
        let trades_emitter = self.emitter.clone();
        let trades_instruments = self.instruments.clone();
        let trades_fill_projection = self.fill_projection.clone();
        let trades_broker_order_index = self.broker_order_index.clone();
        let trades_pending_submits = self.pending_submits.clone();
        let trades_unresolved_trade_fills = self.unresolved_trade_fills.clone();
        let trades_reconnect_policy = self.config.reconnect_policy.clone();
        let trades_reconciler = reconnect_reconciler.clone();
        let trades_last_observed_unix_nanos =
            Arc::new(AtomicU64::new(current_unix_nanos().as_u64()));
        let trades_task = get_runtime().spawn(async move {
            let request = TradesStreamRequest {
                accounts: vec![trades_account],
                ping_delay_ms: None,
            };
            let mut attempt = 0;
            let mut recovery_from = None;
            loop {
                match trades_stream.trades_stream(request.clone()).await {
                    Ok(response) => {
                        if recovery_from.is_none() {
                            attempt = 0;
                        }
                        let stream = response.into_inner();
                        if let Some(from_unix_nanos) = recovery_from {
                            let outcome = reconcile_after_stream_reopen(
                                &trades_reconciler,
                                "trades_stream",
                                from_unix_nanos,
                                &trades_reconnect_policy,
                            )
                            .await;
                            let completed = apply_reconnect_reconciliation_outcome(
                                &mut recovery_from,
                                from_unix_nanos,
                                outcome,
                            );
                            if completed {
                                attempt = 0;
                                trades_last_observed_unix_nanos
                                    .store(current_unix_nanos().as_u64(), Ordering::Release);
                            }
                        }
                        let publish = publish_trades_stream(
                            stream,
                            trades_emitter.clone(),
                            trades_instruments.clone(),
                            trades_fill_projection.clone(),
                            trades_broker_order_index.clone(),
                            trades_pending_submits.clone(),
                            trades_unresolved_trade_fills.clone(),
                            trades_last_observed_unix_nanos.clone(),
                            trades_order_context.clone(),
                        );
                        tokio::pin!(publish);
                        let stream_result = if let Some(from_unix_nanos) = recovery_from {
                            let reconciliation = reconcile_degraded_stream_until_complete(
                                &trades_reconciler,
                                "trades_stream_background",
                                from_unix_nanos,
                                &trades_reconnect_policy,
                            );
                            tokio::pin!(reconciliation);
                            tokio::select! {
                                result = &mut publish => result,
                                () = &mut reconciliation => {
                                    recovery_from = None;
                                    attempt = 0;
                                    trades_last_observed_unix_nanos
                                        .store(current_unix_nanos().as_u64(), Ordering::Release);
                                    publish.await
                                }
                            }
                        } else {
                            publish.await
                        };
                        match stream_result {
                            Ok(()) => tracing::warn!("T-Bank trades stream closed by server"),
                            Err(error) => {
                                tracing::warn!(%error, "T-Bank trades stream closed with error")
                            }
                        }
                        recovery_from.get_or_insert_with(|| {
                            reconnect_reconciliation_from(&trades_last_observed_unix_nanos)
                        });
                    }
                    Err(error) => tracing::warn!(%error, "failed to open T-Bank trades stream"),
                }
                recovery_from.get_or_insert_with(|| {
                    reconnect_reconciliation_from(&trades_last_observed_unix_nanos)
                });
                let delay = crate::grpc::retry::backoff_duration(&trades_reconnect_policy, attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    delay_ms = delay.as_millis(),
                    "reopening T-Bank trades stream after disconnect"
                );
                tokio::time::sleep(delay).await;
            }
        });
        self.stream_tasks
            .lock()
            .expect("stream_tasks lock")
            .push(trades_task);

        let mut portfolio_stream = clients.operations_stream.clone();
        let portfolio_account = account_id.clone();
        let portfolio_emitter = self.emitter.clone();
        let portfolio_position_projection = self.position_projection.clone();
        let portfolio_instruments = self.instruments.clone();
        let portfolio_lifecycle_active = self.lifecycle_active.clone();
        let portfolio_query_client = self.detached_query_clone();
        let portfolio_reconnect_policy = self.config.reconnect_policy.clone();
        let portfolio_task = get_runtime().spawn(async move {
            let request = PortfolioStreamRequest {
                accounts: vec![portfolio_account],
                ping_settings: None,
            };
            let mut attempt = 0;
            loop {
                match portfolio_stream.portfolio_stream(request.clone()).await {
                    Ok(response) => {
                        attempt = 0;
                        let stream = response.into_inner();
                        let stream_result = publish_portfolio_stream(
                            stream,
                            portfolio_emitter.clone(),
                            portfolio_position_projection.clone(),
                            portfolio_instruments.clone(),
                            portfolio_lifecycle_active.clone(),
                            portfolio_query_client.clone(),
                        )
                        .await;
                        match stream_result {
                            Ok(()) => {
                                tracing::warn!("T-Bank portfolio stream closed by server")
                            }
                            Err(error) => {
                                tracing::warn!(%error, "T-Bank portfolio stream closed with error")
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%error, "failed to open T-Bank portfolio stream"),
                }
                let delay =
                    crate::grpc::retry::backoff_duration(&portfolio_reconnect_policy, attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    delay_ms = delay.as_millis(),
                    "reopening T-Bank portfolio stream after disconnect"
                );
                tokio::time::sleep(delay).await;
            }
        });
        self.stream_tasks
            .lock()
            .expect("stream_tasks lock")
            .push(portfolio_task);

        let mut positions_stream = clients.operations_stream.clone();
        let positions_account = account_id;
        let positions_emitter = self.emitter.clone();
        let positions_projection = self.position_projection.clone();
        let positions_instruments = self.instruments.clone();
        let positions_lifecycle_active = self.lifecycle_active.clone();
        let positions_query_client = self.detached_query_clone();
        let positions_reconnect_policy = self.config.reconnect_policy.clone();
        let positions_task = get_runtime().spawn(async move {
            let request = PositionsStreamRequest {
                accounts: vec![positions_account],
                with_initial_positions: true,
                ping_settings: None,
            };
            let mut attempt = 0;
            loop {
                match positions_stream.positions_stream(request.clone()).await {
                    Ok(response) => {
                        attempt = 0;
                        let stream = response.into_inner();
                        let stream_result = publish_positions_stream(
                            stream,
                            positions_emitter.clone(),
                            positions_projection.clone(),
                            positions_instruments.clone(),
                            positions_lifecycle_active.clone(),
                            positions_query_client.clone(),
                        )
                        .await;
                        match stream_result {
                            Ok(()) => {
                                tracing::warn!("T-Bank positions stream closed by server")
                            }
                            Err(error) => {
                                tracing::warn!(%error, "T-Bank positions stream closed with error")
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%error, "failed to open T-Bank positions stream"),
                }
                let delay =
                    crate::grpc::retry::backoff_duration(&positions_reconnect_policy, attempt);
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    delay_ms = delay.as_millis(),
                    "reopening T-Bank positions stream after disconnect"
                );
                tokio::time::sleep(delay).await;
            }
        });
        self.stream_tasks
            .lock()
            .expect("stream_tasks lock")
            .push(positions_task);

        Ok(())
    }

    async fn publish_startup_account_state(&mut self) -> Result<()> {
        if !self.emitter.is_initialized() {
            return Ok(());
        }
        let portfolio = self.query_portfolio().await?;
        if let Some(state) = account_state_from_portfolio(&portfolio)
            .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))?
        {
            self.publish_account_state(state);
        }
        Ok(())
    }

    async fn load_instrument_metadata(
        &mut self,
        instrument_id: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let cached_metadata = self
            .instruments
            .lock()
            .expect("instruments lock")
            .get(instrument_id)
            .cloned();
        if let Some(metadata) = cached_metadata {
            self.ensure_metadata_supported(&metadata)?;
            if metadata.instrument_type != TbankInstrumentType::Futures
                || metadata.conservative_initial_margin_rate().is_some()
            {
                return self.refresh_futures_margin(metadata).await;
            }

            // A persisted FuturesContract is only a cache seed. The current v0.2 futures
            // contract requires both T-Bank risk rates from FutureBy; an older or otherwise
            // incomplete definition must not be passed to GetFuturesMargin and rebuilt as if it
            // were current. Drop it and resolve the authoritative definition below.
            self.instruments
                .lock()
                .expect("instruments lock")
                .remove(instrument_id);
            self.futures_margin_refreshed_at
                .lock()
                .expect("futures_margin_refreshed_at lock")
                .remove(instrument_id);
        }

        let parts = crate::common::ids::TbankInstrumentIdParts::from_str(instrument_id)?;
        let is_share = parts.is_spbe_share() || parts.is_moex_tqbr_equity();
        let is_moex_futures = parts.is_moex_futures();
        let request = InstrumentRequest {
            id_type: InstrumentIdType::Ticker as i32,
            class_code: Some(parts.class_code),
            id: parts.ticker,
        };
        let metadata = if is_share {
            self.fetch_share_metadata(request, instrument_id).await?
        } else if is_moex_futures {
            self.fetch_future_metadata(request, instrument_id).await?
        } else {
            return Err(TbankAdapterError::UnsupportedInstrument(
                instrument_id.to_string(),
            ));
        };
        if metadata.instrument_id != instrument_id {
            return Err(TbankAdapterError::InstrumentNotFound(
                instrument_id.to_string(),
            ));
        }
        self.ensure_metadata_supported(&metadata)?;
        self.cache_instrument_metadata(metadata.clone());
        self.refresh_futures_margin(metadata).await
    }

    async fn refresh_futures_margin(
        &mut self,
        metadata: TbankInstrumentMetadata,
    ) -> Result<TbankInstrumentMetadata> {
        if !metadata.price_in_points {
            return Ok(metadata);
        }
        let cache_key = metadata.instrument_id.clone();
        let expected_generation = self.futures_margin_generation_id;
        loop {
            // Hold the generation lock across the cache/flight transition. If reconnect has
            // already advanced the generation, reject this detached runtime before it can
            // observe any state belonging to the previous connection.
            let (flight, is_leader) = {
                let generation_guard = self
                    .futures_margin_generation
                    .lock()
                    .expect("futures_margin_generation lock");
                if *generation_guard != expected_generation {
                    return Err(TbankAdapterError::FuturesMarginUnresolved(format!(
                        "discarding stale futures margin request for {}",
                        metadata.instrument_id
                    )));
                }

                let margin_is_fresh = self
                    .futures_margin_refreshed_at
                    .lock()
                    .expect("futures_margin_refreshed_at lock")
                    .get(&cache_key)
                    .is_some_and(|refreshed_at| refreshed_at.elapsed() < FUTURES_MARGIN_CACHE_TTL);
                if margin_is_fresh {
                    return Ok(self
                        .instruments
                        .lock()
                        .expect("instruments lock")
                        .get(&cache_key)
                        .cloned()
                        .unwrap_or_else(|| metadata.clone()));
                }

                let mut flights = self
                    .futures_margin_inflight
                    .lock()
                    .expect("futures_margin_inflight lock");
                if let Some(flight) = flights.get(&cache_key) {
                    (Arc::clone(flight), false)
                } else {
                    let (state, receiver) = watch::channel(TbankFuturesMarginFlightState::Pending);
                    let flight = Arc::new(TbankFuturesMarginFlight {
                        state,
                        _receiver: receiver,
                    });
                    flights.insert(cache_key.clone(), Arc::clone(&flight));
                    (flight, true)
                }
            };

            if !is_leader {
                let mut state = flight.state.subscribe();
                loop {
                    let current_state = state.borrow().clone();
                    match current_state {
                        TbankFuturesMarginFlightState::Completed(result) => {
                            let generation_guard = self
                                .futures_margin_generation
                                .lock()
                                .expect("futures_margin_generation lock");
                            if *generation_guard != expected_generation {
                                return Err(TbankAdapterError::FuturesMarginUnresolved(format!(
                                    "discarding stale futures margin response for {}",
                                    metadata.instrument_id
                                )));
                            }
                            return *result;
                        }
                        TbankFuturesMarginFlightState::Cancelled => break,
                        TbankFuturesMarginFlightState::Pending => {
                            if state.changed().await.is_err() {
                                break;
                            }
                        }
                    }
                }
                continue;
            }

            let _flight_guard = TbankFuturesMarginFlightGuard {
                flights: Arc::clone(&self.futures_margin_inflight),
                cache_key: cache_key.clone(),
                flight: Arc::clone(&flight),
            };
            let result = self
                .refresh_futures_margin_uncached(
                    metadata.clone(),
                    cache_key.clone(),
                    expected_generation,
                )
                .await;

            let generation_guard = self
                .futures_margin_generation
                .lock()
                .expect("futures_margin_generation lock");
            if *generation_guard != expected_generation {
                let result = Err(TbankAdapterError::FuturesMarginUnresolved(format!(
                    "discarding stale futures margin response for {}",
                    metadata.instrument_id
                )));
                let _ = flight
                    .state
                    .send(TbankFuturesMarginFlightState::Completed(Box::new(
                        result.clone(),
                    )));
                return result;
            }
            let _ = flight
                .state
                .send(TbankFuturesMarginFlightState::Completed(Box::new(
                    result.clone(),
                )));
            return result;
        }
    }

    async fn refresh_futures_margin_uncached(
        &mut self,
        mut metadata: TbankInstrumentMetadata,
        cache_key: String,
        generation: u64,
    ) -> Result<TbankInstrumentMetadata> {
        let instrument_id = metadata.futures_margin_instrument_id()?;
        let request = GetFuturesMarginRequest {
            #[allow(deprecated)]
            figi: String::new(),
            instrument_id,
        };
        let request_timeout = self.config.request_timeout;
        let response =
            self.clients_mut()?
                .instruments
                .get_futures_margin(with_timeout(request, request_timeout))
                .await
                .map_err(|status| {
                    let error = TbankAdapterError::from(status);
                    log_tbank_rpc_failure("InstrumentsService.GetFuturesMargin", &error);
                    match error {
                        TbankAdapterError::PermissionDenied(_)
                        | TbankAdapterError::RateLimited(_) => error,
                        error => TbankAdapterError::FuturesMarginUnresolved(format!(
                            "{}: {error}",
                            metadata.instrument_id
                        )),
                    }
                })?
                .into_inner();
        metadata.update_futures_margin_contract(&response)?;

        let generation_guard = self
            .futures_margin_generation
            .lock()
            .expect("futures_margin_generation lock");
        if *generation_guard != generation {
            return Err(TbankAdapterError::FuturesMarginUnresolved(format!(
                "discarding stale futures margin response for {}",
                metadata.instrument_id
            )));
        }

        // The Nautilus cache is the shared instrument owner. Publish the rebuilt definition
        // before committing the execution metadata, so market-data and risk consumers cannot
        // observe a partially applied tick/multiplier/GO update.
        let instrument =
            crate::instruments::build_futures_instrument(&metadata).map_err(|error| {
                TbankAdapterError::FuturesMarginUnresolved(format!(
                    "cannot publish current futures risk definition for {}: {error}",
                    metadata.instrument_id
                ))
            })?;
        if let Some(sender) = try_get_data_event_sender() {
            sender
                .send(DataEvent::Instrument(instrument))
                .map_err(|error| {
                    TbankAdapterError::ConversionError(format!(
                        "failed to publish current futures instrument definition: {error}"
                    ))
                })?;
        }
        self.cache_instrument_metadata(metadata.clone());
        self.futures_margin_refreshed_at
            .lock()
            .expect("futures_margin_refreshed_at lock")
            .insert(cache_key, Instant::now());
        drop(generation_guard);
        Ok(metadata)
    }

    fn ensure_metadata_supported(&self, metadata: &TbankInstrumentMetadata) -> Result<()> {
        if !metadata.is_supported() {
            return Err(TbankAdapterError::InstrumentOutOfScope(
                metadata.instrument_id.clone(),
            ));
        }
        Ok(())
    }

    fn cache_instrument_metadata(&self, metadata: TbankInstrumentMetadata) {
        self.instruments
            .lock()
            .expect("instruments lock")
            .insert(metadata.instrument_id.clone(), metadata);
    }

    async fn fetch_share_metadata(
        &mut self,
        request: InstrumentRequest,
        requested_id: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let request_timeout = self.config.request_timeout;
        let response = self
            .clients_mut()?
            .instruments
            .share_by(with_timeout(request, request_timeout))
            .await
            .map_err(|status| {
                let error = TbankAdapterError::from(status);
                log_tbank_rpc_failure("InstrumentsService.ShareBy", &error);
                error
            })?
            .into_inner();
        let share = response
            .instrument
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(requested_id.to_string()))?;
        let metadata = TbankInstrumentMetadata::from_share(&share)?;
        Ok(metadata)
    }

    async fn fetch_future_metadata(
        &mut self,
        request: InstrumentRequest,
        requested_id: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let request_timeout = self.config.request_timeout;
        let response = self
            .clients_mut()?
            .instruments
            .future_by(with_timeout(request, request_timeout))
            .await
            .map_err(|status| {
                let error = TbankAdapterError::from(status);
                log_tbank_rpc_failure("InstrumentsService.FutureBy", &error);
                error
            })?
            .into_inner();
        let future = response
            .instrument
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(requested_id.to_string()))?;
        let metadata = TbankInstrumentMetadata::from_future(&future)?;
        Ok(metadata)
    }

    fn metadata_lookup_error_is_miss(error: &TbankAdapterError) -> bool {
        match error {
            TbankAdapterError::InstrumentNotFound(_) => true,
            TbankAdapterError::GrpcStatus { code, .. } => matches!(
                *code,
                tonic::Code::InvalidArgument | tonic::Code::NotFound | tonic::Code::Unimplemented
            ),
            _ => false,
        }
    }

    fn metadata_identity_error_allows_alternate(error: &TbankAdapterError) -> bool {
        Self::metadata_lookup_error_is_miss(error)
            || matches!(
                error,
                TbankAdapterError::UnsupportedInstrument(_)
                    | TbankAdapterError::InstrumentOutOfScope(_)
            )
    }

    fn metadata_error_is_event_rejection(error: &TbankAdapterError) -> bool {
        Self::metadata_lookup_error_is_miss(error)
            || matches!(error, TbankAdapterError::InvalidInstrumentIdentity(_))
    }

    fn metadata_lookup_allows_fallback(error: &TbankAdapterError) -> bool {
        Self::metadata_lookup_error_is_miss(error) || tbank_adapter_error_is_transient(error)
    }

    fn select_metadata_lookup_error(
        first: TbankAdapterError,
        second: TbankAdapterError,
    ) -> TbankAdapterError {
        if tbank_adapter_error_is_transient(&second) {
            second
        } else if tbank_adapter_error_is_transient(&first)
            && Self::metadata_lookup_error_is_miss(&second)
        {
            first
        } else if !Self::metadata_lookup_error_is_miss(&second) {
            second
        } else {
            first
        }
    }

    async fn fetch_metadata_by_identifier(
        &mut self,
        request: InstrumentRequest,
        identifier: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let identifier_for_error = identifier;
        let request_for_details = request.clone();
        let request_timeout = self.config.request_timeout;
        let kind = match self
            .clients_mut()?
            .instruments
            .get_instrument_by(with_timeout(request_for_details, request_timeout))
            .await
        {
            Ok(response) => response
                .into_inner()
                .instrument
                .and_then(|instrument| InstrumentType::try_from(instrument.instrument_kind).ok()),
            Err(status) => {
                let error = TbankAdapterError::from(status);
                if Self::metadata_lookup_error_is_miss(&error) {
                    None
                } else {
                    return Err(error);
                }
            }
        };

        match kind {
            Some(InstrumentType::Share) => {
                self.fetch_share_metadata(request, identifier_for_error)
                    .await
            }
            Some(InstrumentType::Futures) => {
                self.fetch_future_metadata(request, identifier_for_error)
                    .await
            }
            Some(other) => Err(TbankAdapterError::InstrumentOutOfScope(format!(
                "unsupported instrument type {other:?} for {identifier_for_error}"
            ))),
            None => {
                let request_for_future = request.clone();
                match self
                    .fetch_share_metadata(request, identifier_for_error)
                    .await
                {
                    Ok(metadata) => Ok(metadata),
                    Err(share_error) if Self::metadata_lookup_allows_fallback(&share_error) => {
                        match self
                            .fetch_future_metadata(request_for_future, identifier_for_error)
                            .await
                        {
                            Ok(metadata) => Ok(metadata),
                            Err(future_error) => Err(Self::select_metadata_lookup_error(
                                share_error,
                                future_error,
                            )),
                        }
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    async fn metadata_for_stop_order(
        &mut self,
        stop: &StopOrder,
    ) -> Result<TbankInstrumentMetadata> {
        self.load_supported_metadata_for_identity(
            &stop.instrument_uid,
            &stop.figi,
            &stop.ticker,
            &stop.class_code,
        )
        .await
    }

    async fn load_instrument_metadata_by_uid(
        &mut self,
        instrument_uid: &str,
    ) -> Result<TbankInstrumentMetadata> {
        if let Some(metadata) = self
            .instruments
            .lock()
            .expect("instruments lock")
            .values()
            .find(|metadata| metadata.instrument_uid == instrument_uid)
            .cloned()
        {
            self.ensure_metadata_supported(&metadata)?;
            return Ok(metadata);
        }

        let request = InstrumentRequest {
            id_type: InstrumentIdType::Uid as i32,
            class_code: None,
            id: instrument_uid.to_string(),
        };
        let metadata = self
            .fetch_metadata_by_identifier(request, instrument_uid)
            .await?;
        if metadata.instrument_uid != instrument_uid {
            return Err(TbankAdapterError::InstrumentNotFound(
                instrument_uid.to_string(),
            ));
        }
        self.ensure_metadata_supported(&metadata)?;
        self.cache_instrument_metadata(metadata.clone());
        Ok(metadata)
    }

    async fn load_instrument_metadata_by_figi(
        &mut self,
        figi: &str,
    ) -> Result<TbankInstrumentMetadata> {
        if let Some(metadata) = self
            .instruments
            .lock()
            .expect("instruments lock")
            .values()
            .find(|metadata| metadata.figi == figi)
            .cloned()
        {
            self.ensure_metadata_supported(&metadata)?;
            return Ok(metadata);
        }

        let request = InstrumentRequest {
            id_type: InstrumentIdType::Figi as i32,
            class_code: None,
            id: figi.to_string(),
        };
        let metadata = self.fetch_metadata_by_identifier(request, figi).await?;
        if metadata.figi != figi {
            return Err(TbankAdapterError::InstrumentNotFound(figi.to_string()));
        }
        self.ensure_metadata_supported(&metadata)?;
        self.cache_instrument_metadata(metadata.clone());
        Ok(metadata)
    }

    async fn load_instrument_metadata_by_ticker_class(
        &mut self,
        ticker: &str,
        class_code: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let metadata = {
            let instruments = self.instruments.lock().expect("instruments lock");
            let mut matches = instruments.values().filter(|metadata| {
                metadata.ticker.eq_ignore_ascii_case(ticker)
                    && metadata.class_code.eq_ignore_ascii_case(class_code)
            });
            let first = matches.next().cloned();
            first.filter(|_| matches.next().is_none())
        };
        if let Some(metadata) = metadata {
            self.ensure_metadata_supported(&metadata)?;
            return Ok(metadata);
        }

        let request = InstrumentRequest {
            id_type: InstrumentIdType::Ticker as i32,
            class_code: Some(class_code.to_string()),
            id: ticker.to_string(),
        };
        let requested_identity = format!("{ticker}_{class_code}");
        let metadata = self
            .fetch_metadata_by_identifier(request, &requested_identity)
            .await?;
        if !metadata.ticker.eq_ignore_ascii_case(ticker)
            || !metadata.class_code.eq_ignore_ascii_case(class_code)
        {
            return Err(TbankAdapterError::InstrumentNotFound(requested_identity));
        }
        self.ensure_metadata_supported(&metadata)?;
        self.cache_instrument_metadata(metadata.clone());
        Ok(metadata)
    }

    async fn metadata_for_identity(
        &mut self,
        instrument_uid: &str,
        figi: &str,
        ticker: &str,
        class_code: &str,
    ) -> Result<TbankInstrumentMetadata> {
        let mut metadata = None;
        let mut identity_error = None;

        if !instrument_uid.is_empty() {
            match self.load_instrument_metadata_by_uid(instrument_uid).await {
                Ok(value) => metadata = Some(value),
                Err(error) if tbank_adapter_error_is_transient(&error) => return Err(error),
                Err(error) if Self::metadata_identity_error_allows_alternate(&error) => {
                    identity_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        if metadata.is_none() && !figi.is_empty() && figi != instrument_uid {
            match self.load_instrument_metadata_by_figi(figi).await {
                Ok(value) => metadata = Some(value),
                Err(error) if tbank_adapter_error_is_transient(&error) => return Err(error),
                Err(error) if Self::metadata_identity_error_allows_alternate(&error) => {
                    identity_error = Some(match identity_error {
                        Some(first) => Self::select_metadata_lookup_error(first, error),
                        None => error,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        let metadata = if let Some(metadata) = metadata {
            metadata
        } else if instrument_uid.is_empty() && figi.is_empty() {
            if ticker.is_empty() || class_code.is_empty() {
                return Err(invalid_instrument_identity_error(
                    "",
                    "",
                    ticker,
                    class_code,
                    "event has no broker identity or complete ticker/class_code pair",
                ));
            }
            self.load_instrument_metadata_by_ticker_class(ticker, class_code)
                .await?
        } else {
            return Err(identity_error.unwrap_or_else(|| {
                TbankAdapterError::InstrumentNotFound(instrument_metadata_identity(
                    instrument_uid,
                    figi,
                    ticker,
                    class_code,
                ))
            }));
        };

        if !metadata_matches_event_identity(&metadata, instrument_uid, figi, ticker, class_code) {
            return Err(invalid_instrument_identity_error(
                instrument_uid,
                figi,
                ticker,
                class_code,
                "resolved identity contradicts the event identity",
            ));
        }
        self.refresh_futures_margin(metadata).await
    }

    async fn metadata_resolution_for_identity(
        &mut self,
        instrument_uid: &str,
        figi: &str,
        ticker: &str,
        class_code: &str,
    ) -> Result<TbankInstrumentMetadataResolution> {
        match self
            .metadata_for_identity(instrument_uid, figi, ticker, class_code)
            .await
        {
            Ok(_) => Ok(TbankInstrumentMetadataResolution::Enabled),
            Err(error) if tbank_adapter_error_is_transient(&error) => Err(error),
            Err(TbankAdapterError::InstrumentOutOfScope(_))
            | Err(TbankAdapterError::UnsupportedInstrument(_)) => {
                Ok(TbankInstrumentMetadataResolution::OutOfScope)
            }
            Err(error) if Self::metadata_error_is_event_rejection(&error) => {
                Ok(TbankInstrumentMetadataResolution::Rejected)
            }
            Err(error) => Err(error),
        }
    }

    async fn load_supported_metadata_for_identity(
        &mut self,
        instrument_uid: &str,
        figi: &str,
        ticker: &str,
        class_code: &str,
    ) -> Result<TbankInstrumentMetadata> {
        match self
            .metadata_for_identity(instrument_uid, figi, ticker, class_code)
            .await
        {
            Err(TbankAdapterError::UnsupportedInstrument(_)) => {
                Err(TbankAdapterError::InstrumentOutOfScope(
                    instrument_metadata_identity(instrument_uid, figi, ticker, class_code),
                ))
            }
            result => result,
        }
    }

    async fn metadata_for_order_state(
        &mut self,
        state: &OrderState,
    ) -> Result<TbankInstrumentMetadata> {
        self.load_supported_metadata_for_identity(
            &state.instrument_uid,
            &state.figi,
            &state.ticker,
            &state.class_code,
        )
        .await
    }

    async fn order_status_report_from_state_with_lots(
        &mut self,
        account_id: AccountId,
        state: OrderState,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let (
            client_order_id,
            managed_time_in_force,
            known_current_order_id,
            canonical_order_id,
            activated_stop_order_id,
        ) = {
            let index = self
                .broker_order_index
                .lock()
                .expect("broker_order_index lock");
            let client_order_id = index
                .client_order_id_for_request_id(state.order_request_id.as_str())
                .or_else(|| index.client_order_id_for_venue_order_id(state.order_id.as_str()));
            let managed_time_in_force = client_order_id
                .as_deref()
                .and_then(|client_order_id| {
                    index.managed_context_for_client_order_id(client_order_id)
                })
                .and_then(|context| context.time_in_force);
            let known_current_order_id = client_order_id
                .as_deref()
                .and_then(|client_order_id| index.identity_for(Some(client_order_id), None))
                .map(|identity| identity.broker_order_id);
            let canonical_order_id = known_current_order_id
                .as_deref()
                .map(|order_id| index.canonical_venue_order_id_or_self(order_id))
                .or_else(|| (!state.order_id.is_empty()).then(|| state.order_id.clone()));
            let activated_stop_order_id = canonical_order_id
                .as_deref()
                .filter(|order_id| index.is_known_stop_broker_order_id(order_id))
                .map(str::to_string);
            (
                client_order_id,
                managed_time_in_force,
                known_current_order_id,
                canonical_order_id,
                activated_stop_order_id,
            )
        };
        if let Some(client_order_id) = client_order_id.as_deref() {
            if known_current_order_id.is_none() {
                self.record_broker_order_mapping_and_drain_cancel(
                    TbankBrokerOrderRoute::RegularOrder,
                    client_order_id,
                    state.order_id.as_str(),
                )
                .await;
            } else if let Some(canonical_order_id) = canonical_order_id.as_deref() {
                self.record_regular_order_alias_and_drain_cancel(
                    client_order_id,
                    canonical_order_id,
                    state.order_id.as_str(),
                )
                .await;
            }
        } else if activated_stop_order_id.is_none() {
            self.record_broker_order_id(
                TbankBrokerOrderRoute::RegularOrder,
                state.order_id.as_str(),
            );
        }
        if let Some(stop_order_id) = activated_stop_order_id.as_deref() {
            let stop = self
                .query_stop_orders_for_reconciliation(None)
                .await?
                .stop_orders
                .into_iter()
                .find(|stop| stop.stop_order_id == stop_order_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "activated T-Bank stop parent {stop_order_id} was absent during child reconciliation"
                    )
                })?;
            let metadata = self.metadata_for_stop_order(&stop).await?;
            let managed_order_type =
                self.managed_order_type_for_client_order_id(client_order_id.as_deref());
            return activated_stop_child_status_report_with_context(
                account_id,
                &stop,
                &state,
                ts_init,
                metadata.lot,
                client_order_id.as_deref(),
                Some(&self.instruments),
                managed_order_type,
            );
        }
        let metadata = self.metadata_for_order_state(&state).await?;
        let instrument_id = metadata
            .instrument_id
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid cached instrument id: {error}"))?;
        let lot_size = metadata.lot;
        let mut report = order_status_report_from_state_with_metadata(
            account_id,
            state,
            ts_init,
            instrument_id,
            lot_size,
            Some(&metadata),
        )?;
        report.client_order_id = client_order_id
            .as_deref()
            .and_then(nonempty_client_order_id);
        if client_order_id.is_some()
            && let Some(canonical_order_id) = canonical_order_id.as_deref()
        {
            report.venue_order_id = canonical_order_id.into();
        }
        if let Some(time_in_force) = managed_time_in_force {
            report.time_in_force = time_in_force;
        }
        Ok(report)
    }

    async fn resolve_activated_stop_mapping(
        &mut self,
        exchange_order_id: &str,
        order_request_id: Option<&str>,
    ) -> anyhow::Result<Option<(StopOrder, Option<String>)>> {
        let stops = self
            .query_stop_orders_for_reconciliation(None)
            .await?
            .stop_orders;
        // During activation, OrderState can expose the child before the stop-order
        // projection has populated optional exchange_order_id. The parent request ID
        // is the authoritative correlation key for that transition.
        let stop = stops
            .iter()
            .find(|stop| stop.exchange_order_id.as_deref() == Some(exchange_order_id))
            .cloned()
            .or_else(|| {
                order_request_id
                    .filter(|order_request_id| !order_request_id.is_empty())
                    .and_then(|order_request_id| {
                        stops
                            .into_iter()
                            .find(|stop| stop.stop_order_id == order_request_id)
                    })
            });
        let Some(stop) = stop else {
            return Ok(None);
        };
        let client_order_id = self
            .broker_order_index
            .lock()
            .expect("broker_order_index lock")
            .client_order_id_for_venue_order_id(stop.stop_order_id.as_str());
        self.record_broker_order_id(
            TbankBrokerOrderRoute::StopOrder,
            stop.stop_order_id.as_str(),
        );
        if let Some(client_order_id) = client_order_id.as_deref() {
            self.record_activated_stop_child_mapping_and_drain_cancel(
                client_order_id,
                stop.stop_order_id.as_str(),
                exchange_order_id,
            )
            .await;
        } else {
            self.record_activated_stop_child_alias(stop.stop_order_id.as_str(), exchange_order_id);
        }
        Ok(Some((stop, client_order_id)))
    }

    async fn stop_order_status_report_for_known_id(
        &mut self,
        client_order_id: Option<ClientOrderId>,
        stop_order_id: String,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let stops = self
            .query_stop_orders_for_reconciliation(None)
            .await?
            .stop_orders;
        let Some(stop) = stops
            .into_iter()
            .find(|stop| stop.stop_order_id == stop_order_id)
        else {
            tracing::warn!(
                client_order_id = client_order_id.as_ref().map(|id| id.as_str()).unwrap_or(""),
                "known T-Bank stop order was not returned by StopOrders query"
            );
            return Ok(None);
        };

        self.stop_order_status_report_for_stop(client_order_id, stop, ts_init)
            .await
    }

    async fn stop_order_status_report_for_stop(
        &mut self,
        client_order_id: Option<ClientOrderId>,
        stop: StopOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if let Some(client_order_id) = client_order_id.as_ref() {
            self.record_broker_order_mapping_and_drain_cancel(
                TbankBrokerOrderRoute::StopOrder,
                client_order_id.as_str(),
                stop.stop_order_id.as_str(),
            )
            .await;
        } else {
            self.record_broker_order_id(
                TbankBrokerOrderRoute::StopOrder,
                stop.stop_order_id.as_str(),
            );
        }
        let metadata = self.metadata_for_stop_order(&stop).await?;
        if let Some(client_order_id) = client_order_id.as_ref() {
            self.record_stop_order_context(client_order_id.as_str(), &stop, &metadata);
        }
        if StopOrderStatusOption::try_from(stop.status).ok()
            == Some(StopOrderStatusOption::StopOrderStatusExecuted)
            && let Some(exchange_order_id) = stop
                .exchange_order_id
                .as_deref()
                .filter(|order_id| !order_id.is_empty())
        {
            match self.query_order(exchange_order_id).await {
                Ok(state) => {
                    self.record_activated_stop_child_mapping(
                        client_order_id.as_ref().map(|id| id.as_str()).unwrap_or(""),
                        stop.stop_order_id.as_str(),
                        state.order_id.as_str(),
                    );
                    let managed_order_type = self.managed_order_type_for_client_order_id(
                        client_order_id.as_ref().map(|id| id.as_str()),
                    );
                    let report = activated_stop_child_status_report_with_context(
                        self.account_id(),
                        &stop,
                        &state,
                        ts_init,
                        metadata.lot,
                        client_order_id.as_ref().map(|id| id.as_str()),
                        Some(&self.instruments),
                        managed_order_type,
                    )?;
                    self.mark_pending_submit_report(&report);
                    return Ok(Some(report));
                }
                Err(TbankAdapterError::GrpcStatus {
                    code: tonic::Code::NotFound,
                    message,
                }) => tracing::debug!(
                    %message,
                    "activated T-Bank stop child was absent during single-order lookup"
                ),
                Err(error) => return Err(error.into()),
            }
        }
        let managed_order_type = self
            .managed_order_type_for_client_order_id(client_order_id.as_ref().map(|id| id.as_str()));
        let mut report = stop_order_status_report_with_context(
            self.account_id(),
            stop,
            ts_init,
            metadata.lot,
            Some(&self.instruments),
            managed_order_type,
        )?;
        report.client_order_id = client_order_id;
        self.mark_pending_submit_report(&report);
        Ok(Some(report))
    }

    async fn query_order_status_report_by_ids(
        &mut self,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let requested_client_order_id = client_order_id;
        if let Some(identity) =
            self.known_broker_order_identity(client_order_id.as_ref(), venue_order_id.as_ref())
            && identity.route == TbankBrokerOrderRoute::StopOrder
        {
            return self
                .stop_order_status_report_for_known_id(
                    client_order_id,
                    identity.broker_order_id,
                    ts_init,
                )
                .await;
        }

        // The in-memory broker-order index is empty after a fresh client is created on
        // reconnect. Resolve a supplied venue ID against T-Bank's stop-order history before
        // falling back to OrdersService.GetOrderState: a stop-order ID is not a regular order ID.
        if venue_order_id.is_some()
            && let Some(stop) = self
                .stop_order_from_broker(venue_order_id.as_ref().map(|id| id.as_str()))
                .await?
        {
            return self
                .stop_order_status_report_for_stop(client_order_id, stop, ts_init)
                .await;
        }

        let state = match (venue_order_id, client_order_id) {
            (Some(venue_order_id), _) => {
                TbankExecutionRuntime::query_order(self, venue_order_id.as_str()).await?
            }
            (None, Some(client_order_id)) => {
                let broker_request_id =
                    self.get_or_allocate_broker_request_id(client_order_id.as_str())?;
                TbankExecutionRuntime::query_order_by_request_id(self, broker_request_id.as_str())
                    .await?
            }
            (None, None) => {
                return Err(anyhow::anyhow!(
                    "venue_order_id or client_order_id is required"
                ));
            }
        };

        let mut report = self
            .order_status_report_from_state_with_lots(self.account_id(), state, ts_init)
            .await?;
        if requested_client_order_id.is_some() {
            report.client_order_id = requested_client_order_id;
        }
        Ok(Some(report))
    }
}

/// Converts a raw T-Bank account identifier into the canonical Nautilus account identity.
///
/// Numeric broker identifiers receive the `TBANK-` prefix. Already namespaced identifiers
/// containing a hyphen are preserved.
/// Maps a broker account ID to the canonical Nautilus account ID.
#[must_use]
pub fn tbank_account_id(account_id: &str) -> AccountId {
    nautilus_account_id(account_id)
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
/// Broker response returned after submitting a regular or stop order.
pub enum TbankSubmitResponse {
    /// Regular-order response.
    Order(PostOrderResponse),
    /// Stop-order response.
    StopOrder(PostStopOrderResponse),
}

#[derive(Debug, Clone, PartialEq)]
/// Nautilus reports reconstructed while reconciling one broker order.
pub struct TbankOrderReconciliationReports {
    /// Current order-status report.
    pub order_report: OrderStatusReport,
    /// Fill reports associated with the order.
    pub fill_reports: Vec<FillReport>,
}

#[cfg(test)]
mod tests;
