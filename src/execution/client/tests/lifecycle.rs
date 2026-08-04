use super::submit::synthetic_fill_trade_id;
use super::{
    CANCEL_OUTCOME_RECOVERY_ATTEMPTS, CancelFailureKind, MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER,
    TBANK_CONFIRM_MARGIN_TRADE_PARAM, TbankFillProjection, activated_stop_child_status_report,
    buffer_unresolved_trade_fill, canonicalize_reconciled_stop_fill, classify_cancel_failure,
    current_utc_day_bounds, order_filter_windows, project_and_settle_reconciled_trade_fill,
    project_managed_trade_fill_report, project_trade_fill_report, tbank_account_id,
};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use futures_util::{Stream, StreamExt, stream};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            CancelOrder, ExecutionReport, GenerateFillReports, GenerateOrderStatusReports,
            GeneratePositionStatusReports, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
            SubmitOrderList,
        },
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::ExecutionEventEmitter;
use rust_decimal::Decimal;
use serde_json::json;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, transport::Server};

use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    enums::{
        AccountType, ContingencyType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce, TrailingOffsetType, TriggerType,
    },
    events::{AccountState, OrderEventAny, OrderInitialized},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, Venue, VenueOrderId,
    },
    orders::OrderList,
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};

use crate::{
    common::{TbankAdapterError, TbankOrderSide, TbankOrderType},
    config::{TbankEnvironment, TbankExecutionClientConfig, TbankReconnectPolicy},
    execution::{TbankSubmitOrder, projections::TbankProjectedOrderStatus},
    grpc::generated::{
        BrokerReportRequest, BrokerReportResponse, CancelOrderRequest, CancelOrderResponse,
        CancelStopOrderRequest, CancelStopOrderResponse, ExchangeOrderType,
        GetDividendsForeignIssuerRequest, GetDividendsForeignIssuerResponse, GetMaxLotsRequest,
        GetMaxLotsResponse, GetOperationsByCursorRequest, GetOperationsByCursorResponse,
        GetOrderPriceRequest, GetOrderPriceResponse, GetOrderStateRequest, GetOrdersRequest,
        GetOrdersResponse, GetStopOrdersRequest, GetStopOrdersResponse, MoneyValue, OperationItem,
        OperationItemTrade, OperationItemTrades, OperationState,
        OperationType as TbankOperationType, OperationsRequest, OperationsResponse, OrderDirection,
        OrderExecutionReportStatus, OrderIdType, OrderState, OrderTrade, OrderTrades,
        PortfolioRequest, PortfolioResponse, PortfolioStreamResponse, PositionsRequest,
        PositionsResponse, PostOrderAsyncRequest, PostOrderAsyncResponse, PostOrderRequest,
        PostOrderResponse, PostStopOrderRequest, PostStopOrderResponse, Quotation,
        ReplaceOrderRequest, StopOrder, StopOrderDirection, StopOrderStatusOption, StopOrderType,
        TakeProfitType, TradesStreamRequest, TradesStreamResponse, TrailingValueType,
        WithdrawLimitsRequest, WithdrawLimitsResponse,
        operations_service_server::{OperationsService, OperationsServiceServer},
        order_state_stream_response,
        orders_service_server::{OrdersService, OrdersServiceServer},
        orders_stream_service_server::{OrdersStreamService, OrdersStreamServiceServer},
        portfolio_stream_response, stop_order,
        stop_orders_service_server::{StopOrdersService, StopOrdersServiceServer},
    },
    testing::fixtures::sber_metadata,
};

fn test_client(config: TbankExecutionClientConfig) -> TbankExecutionClient {
    let account_id = tbank_account_id(
        &config
            .resolve_account_id()
            .unwrap_or_else(|_| "UNKNOWN".to_string()),
    );
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("TBANK"),
        Venue::from("MOEX"),
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache,
    );
    TbankExecutionClient::new(core, config)
}

fn test_emitter(
    sender: tokio::sync::mpsc::UnboundedSender<ExecutionEvent>,
) -> ExecutionEventEmitter {
    let mut emitter = ExecutionEventEmitter::new(
        get_atomic_clock_realtime(),
        TraderId::from("TRADER-001"),
        AccountId::from("TBANK-account-1"),
        AccountType::Margin,
        None,
    );
    emitter.set_sender(sender);
    emitter
}

fn activate_test_lifecycle(client: &TbankExecutionClient) {
    client
        .runtime
        .lifecycle_active
        .store(true, std::sync::atomic::Ordering::Release);
}

struct TaskDropSignal(Option<std::sync::mpsc::Sender<()>>);

impl Drop for TaskDropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn disconnect_aborts_tracked_command_tasks_and_invalidates_clones() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .lifecycle_active
        .store(true, std::sync::atomic::Ordering::Release);
    let stale_clone = client.runtime.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();

    client
        .runtime
        .spawn_read_only_command_task(async move {
            let _drop_signal = TaskDropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        })
        .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("command task did not start");

    client.disconnect();

    dropped_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("command task was not aborted");
    assert!(client.runtime.command_tasks.lock().unwrap().is_empty());
    assert!(stale_clone.ensure_lifecycle_active().is_err());
    assert!(
        client
            .runtime
            .spawn_read_only_command_task(async {})
            .is_err()
    );
    assert!(client.runtime.command_tasks.lock().unwrap().is_empty());
}

#[test]
fn disconnected_submit_is_rejected_before_registration_or_submitted_event() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);

    let cmd = submit_order_cmd(None);
    let client_order_id = cmd.client_order_id;
    let result = ExecutionClient::submit_order(&client, cmd);

    assert!(result.is_err());
    assert!(receiver.try_recv().is_err());
    assert!(client.runtime.command_tasks.lock().unwrap().is_empty());
    assert!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .identity_for(Some(client_order_id.as_str()), None)
            .is_none()
    );
}

#[test]
fn stale_read_generation_cannot_publish_after_reset_and_new_generation() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    activate_test_lifecycle(&client);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(event_tx);
    let stale_runtime = client.runtime.clone();
    let stale_account_id = stale_runtime.account_id();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    nautilus_common::live::runtime::get_runtime().spawn(async move {
        let _ = started_tx.send(());
        let _ = release_rx.await;
        if stale_runtime.ensure_lifecycle_active().is_ok() {
            stale_runtime.publish_account_state(AccountState::new(
                stale_account_id,
                AccountType::Margin,
                Vec::new(),
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::from(1_u64),
                UnixNanos::from(1_u64),
                None,
            ));
        }
        let _ = completed_tx.send(());
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("stale read task did not start");

    ExecutionClient::reset(&mut client).unwrap();
    activate_test_lifecycle(&client);
    release_tx.send(()).unwrap();
    completed_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("stale read task did not finish");

    assert!(event_rx.try_recv().is_err());
}

#[test]
fn stale_stream_generation_cannot_publish_after_new_generation_activates() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let emitter = test_emitter(event_tx);
    let stale_generation = Arc::new(super::TbankLifecycleToken::new(false));
    let new_generation = Arc::new(AtomicBool::new(true));
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let response = PortfolioStreamResponse {
        payload: Some(portfolio_stream_response::Payload::Portfolio(
            PortfolioResponse {
                account_id: "account-1".to_string(),
                total_amount_portfolio: Some(MoneyValue {
                    currency: "rub".to_string(),
                    units: 100,
                    nano: 0,
                }),
                ..PortfolioResponse::default()
            },
        )),
    };

    super::publish_portfolio_response(response, &emitter, &projection, &stale_generation);

    assert!(new_generation.load(Ordering::Acquire));
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn reset_and_dispose_refuse_while_mutating_command_is_in_flight() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    activate_test_lifecycle(&client);
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    client
        .runtime
        .spawn_mutating_command_task(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            let _ = completed_tx.send(());
        })
        .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("mutating command did not start");

    client.disconnect();
    assert!(ExecutionClient::reset(&mut client).is_err());
    assert!(ExecutionClient::dispose(&mut client).is_err());
    assert!(!client.runtime.instruments.lock().unwrap().is_empty());

    release_tx.send(()).unwrap();
    completed_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("mutating command was aborted by disconnect");
    for _ in 0..100 {
        if !client.runtime.has_unfinished_mutating_tasks() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(!client.runtime.has_unfinished_mutating_tasks());
    let pending = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: "uncertain-order".to_string(),
        broker_request_id: "uncertain-request".to_string(),
        side: TbankOrderSide::Buy,
        order_type: TbankOrderType::Market,
        time_in_force: TimeInForce::Day,
        quantity_shares: Decimal::ONE,
        limit_price: None,
        trigger_price: None,
        trailing: None,
        confirm_margin_trade: false,
    };
    client
        .runtime
        .record_pending_submit(&pending, UnixNanos::from(1_u64));
    client.runtime.mark_pending_submit_stage(
        "uncertain-order",
        TbankPendingSubmitStage::Unknown,
        None,
    );
    assert!(ExecutionClient::reset(&mut client).is_err());
    assert!(!client.runtime.instruments.lock().unwrap().is_empty());
    client.runtime.mark_pending_submit_stage(
        "uncertain-order",
        TbankPendingSubmitStage::Accepted,
        None,
    );
    ExecutionClient::reset(&mut client).unwrap();
    assert!(client.runtime.instruments.lock().unwrap().is_empty());
}

#[test]
fn reset_refuses_to_discard_buffered_broker_fill() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    let ts = UnixNanos::from(1_u64);
    let fill = FillReport::new(
        client.runtime.account_id(),
        InstrumentId::from("SBER_TQBR.MOEX"),
        VenueOrderId::from("venue-1"),
        TradeId::from("trade-1"),
        OrderSide::Buy,
        Quantity::from(1),
        Price::from("100.00"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        Some(ClientOrderId::from("order-1")),
        None,
        ts,
        ts,
        Some(UUID4::new()),
    );
    client
        .runtime
        .unresolved_trade_fills
        .lock()
        .unwrap()
        .insert("venue-1".to_string(), vec![fill]);

    assert!(ExecutionClient::reset(&mut client).is_err());
    assert_eq!(
        client
            .runtime
            .unresolved_trade_fills
            .lock()
            .unwrap()
            .get("venue-1")
            .map(Vec::len),
        Some(1)
    );

    client
        .runtime
        .unresolved_trade_fills
        .lock()
        .unwrap()
        .clear();
    ExecutionClient::reset(&mut client).unwrap();
}

#[test]
fn terminal_order_report_settles_submit_and_ambiguous_cancel_state() {
    let pending_submits = Arc::new(Mutex::new(HashMap::from([(
        "client-order-1".to_string(),
        TbankPendingSubmit {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            submitted_ts: UnixNanos::from(1_u64),
            quantity_shares: Decimal::ONE,
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Limit,
            time_in_force: TimeInForce::Day,
            trailing: None,
            venue_order_id: Some("venue-order-1".to_string()),
            last_reconciliation_ts: None,
            stage: TbankPendingSubmitStage::Unknown,
        },
    )])));
    let unresolved_cancellations =
        Arc::new(Mutex::new(HashSet::from([TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("venue-order-1".to_string()),
        }])));
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::RegularOrder,
        "client-order-1",
        "venue-order-1",
    );
    let ts = UnixNanos::from(2_u64);
    let report = OrderStatusReport::new(
        AccountId::from("TBANK-account-1"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        Some(ClientOrderId::from("client-order-1")),
        VenueOrderId::from("venue-order-1"),
        OrderSide::Buy,
        OrderType::Limit,
        TimeInForce::Day,
        OrderStatus::Canceled,
        Quantity::from(1),
        Quantity::from(0),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    );

    settle_order_report_mutation_state(
        &pending_submits,
        &unresolved_cancellations,
        &broker_order_index,
        &report,
    );

    assert_eq!(
        pending_submits
            .lock()
            .unwrap()
            .get("client-order-1")
            .unwrap()
            .stage,
        TbankPendingSubmitStage::Cancelled
    );
    assert!(unresolved_cancellations.lock().unwrap().is_empty());
}

#[test]
fn reconnect_fill_removes_only_the_authoritative_buffered_trade() {
    let ts = UnixNanos::from(1_u64);
    let fill = |trade_id: &str| {
        FillReport::new(
            AccountId::from("TBANK-account-1"),
            InstrumentId::from("SBER_TQBR.MOEX"),
            VenueOrderId::from("venue-order-1"),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from(1),
            Price::from("100.00"),
            Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
            LiquiditySide::NoLiquiditySide,
            Some(ClientOrderId::from("client-order-1")),
            None,
            ts,
            ts,
            Some(UUID4::new()),
        )
    };
    let buffered = Arc::new(Mutex::new(HashMap::from([(
        "venue-order-1".to_string(),
        vec![fill("trade-1"), fill("trade-2")],
    )])));

    settle_reconciled_buffered_trade_fill(&buffered, "venue-order-1", "trade-1");

    let buffered = buffered.lock().unwrap();
    let remaining = buffered.get("venue-order-1").unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].trade_id.to_string(), "trade-2");
}

#[test]
fn unresolved_fill_pressure_preserves_every_unique_trade() {
    let ts = UnixNanos::from(1_u64);
    let fill = |trade_id: String| {
        FillReport::new(
            AccountId::from("TBANK-account-1"),
            InstrumentId::from("SBER_TQBR.MOEX"),
            VenueOrderId::from("venue-order-1"),
            TradeId::from(trade_id),
            OrderSide::Buy,
            Quantity::from(1),
            Price::from("100.00"),
            Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
            LiquiditySide::NoLiquiditySide,
            Some(ClientOrderId::from("client-order-1")),
            None,
            ts,
            ts,
            Some(UUID4::new()),
        )
    };
    let buffered = Arc::new(Mutex::new(HashMap::new()));

    for index in 0..MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER {
        assert!(!buffer_unresolved_trade_fill(
            &buffered,
            "venue-order-1".to_string(),
            fill(format!("trade-{index}")),
        ));
    }
    assert!(buffer_unresolved_trade_fill(
        &buffered,
        "venue-order-1".to_string(),
        fill(format!("trade-{MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER}")),
    ));
    assert!(!buffer_unresolved_trade_fill(
        &buffered,
        "venue-order-1".to_string(),
        fill(format!("trade-{MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER}")),
    ));

    let buffered = buffered.lock().unwrap();
    let reports = buffered.get("venue-order-1").unwrap();
    assert_eq!(reports.len(), MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER + 1);
    assert_eq!(reports[0].trade_id.to_string(), "trade-0");
}

#[test]
fn stale_reconnect_fill_cannot_settle_the_active_buffer() {
    let client = test_client(TbankExecutionClientConfig::default());
    let report = FillReport::new(
        AccountId::from("TBANK-account-1"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        VenueOrderId::from("venue-order-1"),
        TradeId::from("trade-1"),
        OrderSide::Buy,
        Quantity::from(1),
        Price::from("100.00"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        Some(ClientOrderId::from("client-order-1")),
        None,
        UnixNanos::from(1_u64),
        UnixNanos::from(1_u64),
        None,
    );
    client
        .runtime
        .unresolved_trade_fills
        .lock()
        .unwrap()
        .insert("venue-order-1".to_string(), vec![report.clone()]);

    let error = project_and_settle_reconciled_trade_fill(
        &client.runtime,
        report,
        "venue-order-1",
        "trade-1",
    )
    .unwrap_err();

    assert!(error.to_string().contains("lifecycle"));
    assert_eq!(
        client
            .runtime
            .unresolved_trade_fills
            .lock()
            .unwrap()
            .get("venue-order-1")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn reset_clears_execution_lifecycle_state() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    let metadata = sber_metadata();
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .record_mapping(
            TbankBrokerOrderRoute::RegularOrder,
            "client-order-1",
            "venue-order-1",
        );
    client
        .runtime
        .fill_projection
        .lock()
        .unwrap()
        .orders
        .insert("venue-order-1".to_string(), Default::default());
    client
        .runtime
        .order_status_projection
        .lock()
        .unwrap()
        .insert(
            "venue-order-1".to_string(),
            TbankProjectedOrderStatus {
                status: OrderStatus::Accepted,
                ts_last: UnixNanos::from(1_u64),
                filled_quantity: Decimal::ZERO,
            },
        );
    client.runtime.record_pending_submit(
        &TbankSubmitOrder {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            client_order_id: "client-order-1".to_string(),
            broker_request_id: "request-1".to_string(),
            side: TbankOrderSide::Buy,
            order_type: TbankOrderType::Market,
            time_in_force: TimeInForce::Day,
            quantity_shares: Decimal::ONE,
            limit_price: None,
            trigger_price: None,
            trailing: None,
            confirm_margin_trade: false,
        },
        UnixNanos::from(1_u64),
    );
    client.runtime.mark_pending_submit_stage(
        "client-order-1",
        TbankPendingSubmitStage::Accepted,
        None,
    );
    client
        .runtime
        .unresolved_trade_fills
        .lock()
        .unwrap()
        .insert("venue-order-1".to_string(), Vec::new());
    let old_position_projection = client.runtime.position_projection.clone();

    ExecutionClient::reset(&mut client).unwrap();

    assert!(client.runtime.instruments.lock().unwrap().is_empty());
    assert!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .identity_for(Some("client-order-1"), Some("venue-order-1"))
            .is_none()
    );
    assert!(
        client
            .runtime
            .fill_projection
            .lock()
            .unwrap()
            .orders
            .is_empty()
    );
    assert!(
        client
            .runtime
            .order_status_projection
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .runtime
            .position_projection
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(!Arc::ptr_eq(
        &old_position_projection,
        &client.runtime.position_projection
    ));
    assert!(client.runtime.pending_submits.lock().unwrap().is_empty());
    assert!(
        client
            .runtime
            .unresolved_trade_fills
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(client.runtime.ensure_lifecycle_active().is_err());
    assert!(!client.is_connected());
}

#[test]
fn terminal_order_query_filters_use_hourly_current_day_windows() {
    let (midnight, _) = current_utc_day_bounds();
    let from = i128::from(midnight.seconds - 60) * 1_000_000_000;
    let to = i128::from(midnight.seconds + 2 * 60 * 60 + 30 * 60) * 1_000_000_000;
    let filters = order_filter_windows(from, to).unwrap();

    assert_eq!(filters.len(), 3);
    assert_eq!(filters[0].from.as_ref().unwrap().seconds, midnight.seconds);
    for window in &filters {
        let from = window.from.as_ref().unwrap();
        let to = window.to.as_ref().unwrap();
        assert!(to.seconds - from.seconds <= 60 * 60);
    }
    assert_eq!(
        filters[0].execution_status,
        vec![
            OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
            OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            OrderExecutionReportStatus::ExecutionReportStatusCancelled as i32,
            OrderExecutionReportStatus::ExecutionReportStatusRejected as i32,
        ]
    );
}

#[test]
fn submit_fill_trade_id_is_deterministic_and_fits_nautilus() {
    let first = synthetic_fill_trade_id("submit", "broker-order-id", Decimal::from(10));
    let second = synthetic_fill_trade_id("submit", "broker-order-id", Decimal::from(10));

    assert_eq!(first, second);
    assert_eq!(first.len(), 36);
    assert!(uuid::Uuid::parse_str(&first).is_ok());
}
#[test]
fn reconnect_reconciliation_retries_only_transient_failures() {
    let unavailable = anyhow::Error::new(TbankAdapterError::GrpcStatus {
        code: Code::Unavailable,
        message: "temporary".to_string(),
    });
    let rate_limited = anyhow::Error::new(TbankAdapterError::RateLimited("try later".to_string()));
    let permission_denied =
        anyhow::Error::new(TbankAdapterError::PermissionDenied("forbidden".to_string()));
    let malformed = anyhow::anyhow!("malformed broker data");

    assert!(reconnect_reconciliation_error_is_transient(&unavailable));
    assert!(reconnect_reconciliation_error_is_transient(&rate_limited));
    assert!(!reconnect_reconciliation_error_is_transient(
        &permission_denied
    ));
    assert!(!reconnect_reconciliation_error_is_transient(&malformed));
}

#[test]
fn cancel_failure_classification_matches_upstream_terminal_semantics() {
    assert_eq!(
        super::classify_cancel_failure(&TbankAdapterError::ConfigError("missing id".to_string())),
        super::CancelFailureKind::LocalFailure
    );
    assert_eq!(
        super::classify_cancel_failure(&TbankAdapterError::GrpcStatus {
            code: Code::NotFound,
            message: "order not found".to_string(),
        }),
        super::CancelFailureKind::BrokerRejected
    );
    assert_eq!(
        super::classify_cancel_failure(&TbankAdapterError::GrpcStatus {
            code: Code::Unavailable,
            message: "transport lost".to_string(),
        }),
        super::CancelFailureKind::OutcomeUnknown
    );
    assert_eq!(
        super::classify_submit_grpc_status(Code::DataLoss),
        super::SubmitFailureKind::OutcomeUnknown
    );
    assert_eq!(
        super::classify_cancel_failure(&TbankAdapterError::GrpcStatus {
            code: Code::DataLoss,
            message: "response corrupted".to_string(),
        }),
        super::CancelFailureKind::OutcomeUnknown
    );
}

#[test]
fn reconnect_reconciliation_starts_before_last_observed_stream_event() {
    let last_observed = 1_700_000_000_000_000_000_u64;
    let watermark = AtomicU64::new(last_observed);

    assert_eq!(
        super::reconnect_reconciliation_from(&watermark),
        i128::from(last_observed - super::RECONNECT_RECONCILIATION_OVERLAP_NANOS)
    );
}

#[test]
fn reconnect_reconciliation_outcomes_update_recovery_gap() {
    let original_from = 1_700_000_000_000_000_000_i128;
    for (outcome, expected_completed, expected_recovery_from) in [
        (
            super::TbankReconnectReconciliationOutcome::Completed,
            true,
            None,
        ),
        (
            super::TbankReconnectReconciliationOutcome::Degraded,
            false,
            Some(original_from),
        ),
        (
            super::TbankReconnectReconciliationOutcome::Permanent,
            false,
            Some(original_from),
        ),
    ] {
        let mut recovery_from = Some(original_from);
        let reconciliation_completed = super::apply_reconnect_reconciliation_outcome(
            &mut recovery_from,
            original_from,
            outcome,
        );

        assert_eq!(reconciliation_completed, expected_completed, "{outcome:?}");
        assert_eq!(recovery_from, expected_recovery_from, "{outcome:?}");
    }
}

#[test]
fn unresolved_fill_reconciliation_key_is_released_only_for_an_empty_buffer() {
    let unresolved = Arc::new(Mutex::new(HashMap::from([(
        "venue-order-1".to_string(),
        Vec::new(),
    )])));
    let reconciliations = Arc::new(Mutex::new(HashSet::from([
        "exchange:venue-order-1".to_string()
    ])));

    assert!(!super::finish_unresolved_trade_reconciliation_if_idle(
        &unresolved,
        &reconciliations,
        "exchange:venue-order-1",
        "venue-order-1",
    ));
    assert!(
        reconciliations
            .lock()
            .unwrap()
            .contains("exchange:venue-order-1")
    );

    unresolved.lock().unwrap().remove("venue-order-1");
    assert!(super::finish_unresolved_trade_reconciliation_if_idle(
        &unresolved,
        &reconciliations,
        "exchange:venue-order-1",
        "venue-order-1",
    ));
    assert!(reconciliations.lock().unwrap().is_empty());
}

#[test]
fn executed_stop_is_triggered_until_activated_child_finishes() {
    assert_eq!(
        super::nautilus_stop_order_status(StopOrderStatusOption::StopOrderStatusExecuted as i32),
        OrderStatus::Triggered
    );
    assert_eq!(
        super::activated_stop_child_status(OrderStatus::Accepted),
        OrderStatus::Triggered
    );
    assert_eq!(
        super::activated_stop_child_status(OrderStatus::PartiallyFilled),
        OrderStatus::PartiallyFilled
    );
}

#[derive(Clone, Default)]
struct MockOrdersService {
    calls: Arc<Mutex<Vec<PostOrderRequest>>>,
    post_error: Arc<Mutex<Option<(Code, String)>>>,
    post_response: Arc<Mutex<Option<PostOrderResponse>>>,
    cancel_calls: Arc<Mutex<Vec<CancelOrderRequest>>>,
    cancel_error: Arc<Mutex<Option<(Code, String)>>>,
    state_calls: Arc<Mutex<Vec<GetOrderStateRequest>>>,
    state_error: Arc<Mutex<Option<(Code, String)>>>,
    state_response: Arc<Mutex<Option<OrderState>>>,
    get_orders_calls: Arc<AtomicU64>,
    get_orders_response: Arc<Mutex<Option<GetOrdersResponse>>>,
}

#[derive(Clone)]
struct ReconnectingOrdersStreamService {
    order_stream_calls: Arc<AtomicU64>,
    reopened_state: order_state_stream_response::OrderState,
}

#[derive(Clone, Default)]
struct MockStopOrdersService {
    post_calls: Arc<Mutex<Vec<PostStopOrderRequest>>>,
    post_error: Arc<Mutex<Option<(Code, String)>>>,
    get_calls: Arc<Mutex<Vec<GetStopOrdersRequest>>>,
    get_error: Arc<Mutex<Option<(Code, String)>>>,
    get_response: Arc<Mutex<Option<GetStopOrdersResponse>>>,
    cancel_calls: Arc<Mutex<Vec<CancelStopOrderRequest>>>,
}

#[derive(Clone, Default)]
struct MockOperationsService {
    calls: Arc<Mutex<Vec<GetOperationsByCursorRequest>>>,
    pages: Arc<Mutex<VecDeque<GetOperationsByCursorResponse>>>,
    portfolio_calls: Arc<AtomicU64>,
    portfolio_response: Arc<Mutex<Option<PortfolioResponse>>>,
}

#[tonic::async_trait]
impl OrdersStreamService for ReconnectingOrdersStreamService {
    type TradesStreamStream =
        Pin<Box<dyn Stream<Item = std::result::Result<TradesStreamResponse, Status>> + Send>>;
    type OrderStateStreamStream = Pin<
        Box<
            dyn Stream<
                    Item = std::result::Result<
                        crate::grpc::generated::OrderStateStreamResponse,
                        Status,
                    >,
                > + Send,
        >,
    >;

    async fn trades_stream(
        &self,
        _request: Request<TradesStreamRequest>,
    ) -> std::result::Result<Response<Self::TradesStreamStream>, Status> {
        Ok(Response::new(Box::pin(stream::pending())))
    }

    async fn order_state_stream(
        &self,
        _request: Request<crate::grpc::generated::OrderStateStreamRequest>,
    ) -> std::result::Result<Response<Self::OrderStateStreamStream>, Status> {
        let call = self.order_stream_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Ok(Response::new(Box::pin(stream::empty())));
        }
        let response = crate::grpc::generated::OrderStateStreamResponse {
            payload: Some(order_state_stream_response::Payload::OrderState(
                self.reopened_state.clone(),
            )),
        };
        Ok(Response::new(Box::pin(
            stream::once(async move { Ok(response) }).chain(stream::pending()),
        )))
    }
}

#[tokio::test]
async fn reconnect_stop_query_keeps_active_orders_and_bounds_terminal_history() {
    let service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&service.get_calls);
    *service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StopOrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect().await.unwrap();
    let from_seconds = chrono::Utc::now().timestamp() - 60;

    let response = client
        .runtime
        .query_stop_orders_for_reconciliation(Some(i128::from(from_seconds) * 1_000_000_000))
        .await
        .unwrap();

    assert_eq!(response.stop_orders.len(), 1);
    let calls = get_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].status,
        StopOrderStatusOption::StopOrderStatusActive as i32
    );
    assert!(calls[0].from.is_none());
    assert!(calls[0].to.is_none());
    assert_eq!(
        calls[1].status,
        StopOrderStatusOption::StopOrderStatusAll as i32
    );
    assert_eq!(calls[1].from.as_ref().unwrap().seconds, from_seconds);
    assert!(calls[1].to.as_ref().unwrap().seconds >= from_seconds);
}

#[tokio::test]
async fn executed_stop_reconciliation_queries_missing_exchange_child() {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-child-1".to_string(),
        order_request_id: "stop-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 2,
        lots_executed: 2,
        direction: OrderDirection::Sell as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.connect_for_queries().await.unwrap();
    let stops = vec![StopOrder {
        stop_order_id: "stop-order-1".to_string(),
        exchange_order_id: Some("exchange-child-1".to_string()),
        status: StopOrderStatusOption::StopOrderStatusExecuted as i32,
        ..StopOrder::default()
    }];
    let mut order_states = Vec::new();

    client
        .runtime
        .append_missing_activated_stop_children(&mut order_states, &stops)
        .await
        .unwrap();

    assert_eq!(order_states.len(), 1);
    assert_eq!(order_states[0].order_id, "exchange-child-1");
    let state_calls = state_calls.lock().unwrap();
    assert_eq!(state_calls.len(), 1);
    assert_eq!(state_calls[0].order_id, "exchange-child-1");
    assert_eq!(
        state_calls[0].order_id_type,
        Some(OrderIdType::Exchange as i32)
    );
}

#[test]
fn activated_stop_child_report_keeps_stop_identity_and_uses_child_state() {
    let client_order_id = "RS-SHOCK-LIVE-42";
    let mut stop = active_sber_stop_order("stop-order-1");
    stop.exchange_order_id = Some("exchange-order-1".to_string());
    let state = OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "stop-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 2,
        lots_executed: 2,
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 271,
            nano: 500_000_000,
        }),
        ..OrderState::default()
    };

    let report = activated_stop_child_status_report(
        "TBANK-001".into(),
        &stop,
        &state,
        current_unix_nanos(),
        10,
        Some(client_order_id),
    )
    .unwrap();

    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some(client_order_id.to_string())
    );
    assert_eq!(report.order_status, OrderStatus::Filled);
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(20));
    assert_eq!(report.avg_px, Some(Decimal::new(2715, 1)));
}

fn order_status_report_cmd(
    client_order_id: Option<&str>,
    venue_order_id: Option<&str>,
) -> nautilus_common::messages::execution::GenerateOrderStatusReport {
    nautilus_common::messages::execution::GenerateOrderStatusReport::new(
        nautilus_core::UUID4::new(),
        current_unix_nanos(),
        Some(InstrumentId::from("SBER_TQBR.MOEX")),
        client_order_id.map(ClientOrderId::from),
        venue_order_id.map(VenueOrderId::from),
        None,
        None,
    )
}

fn active_sber_stop_order(stop_order_id: &str) -> StopOrder {
    StopOrder {
        stop_order_id: stop_order_id.to_string(),
        lots_requested: 2,
        direction: StopOrderDirection::Sell as i32,
        order_type: StopOrderType::StopLoss as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        stop_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 270,
            nano: 0,
        }),
        status: StopOrderStatusOption::StopOrderStatusActive as i32,
        ..StopOrder::default()
    }
}

fn assert_reconciliation_stop_queries(calls: &[GetStopOrdersRequest]) {
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].status,
        StopOrderStatusOption::StopOrderStatusAll as i32
    );
    assert!(calls[0].from.is_none());
    assert!(calls[0].to.is_none());
}

fn submit_order_cmd(params: Option<Params>) -> SubmitOrder {
    submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, params)
}

fn submit_stop_order_cmd() -> SubmitOrder {
    let ts_init = current_unix_nanos();
    let trader_id = TraderId::from("TRADER-001");
    let strategy_id = StrategyId::from("RS-SHOCK-LIVE");
    let instrument_id = InstrumentId::from("SBER_TQBR.MOEX");
    let client_order_id = ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156");
    let trigger_price = Price::from("270.00");
    let order_init = OrderInitialized::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        OrderType::StopMarket,
        Quantity::from_decimal(Decimal::from(20)).unwrap(),
        TimeInForce::Gtc,
        false,
        true,
        false,
        false,
        UUID4::new(),
        ts_init,
        ts_init,
        None,
        None,
        Some(trigger_price),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    SubmitOrder::new(
        trader_id,
        None,
        strategy_id,
        instrument_id,
        client_order_id,
        order_init,
        None,
        None,
        None,
        UUID4::new(),
        ts_init,
        None,
    )
}

fn submit_order_cmd_for(
    instrument_id: &str,
    order_type: OrderType,
    params: Option<Params>,
) -> SubmitOrder {
    let ts_init = current_unix_nanos();
    let trader_id = TraderId::from("TRADER-001");
    let strategy_id = StrategyId::from("RS-SHOCK-LIVE");
    let instrument_id = InstrumentId::from(instrument_id);
    let client_order_id = ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156");
    let order_init = OrderInitialized::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        order_type,
        Quantity::from_decimal(Decimal::from(20)).unwrap(),
        match order_type {
            OrderType::Market => TimeInForce::Ioc,
            OrderType::Limit => TimeInForce::Day,
            _ => TimeInForce::Gtc,
        },
        false,
        false,
        false,
        false,
        UUID4::new(),
        ts_init,
        ts_init,
        matches!(order_type, OrderType::Limit | OrderType::StopLimit)
            .then(|| Price::from("275.00")),
        None,
        matches!(order_type, OrderType::StopLimit).then(|| Price::from("270.00")),
        matches!(order_type, OrderType::StopLimit).then_some(TriggerType::LastPrice),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    SubmitOrder::new(
        trader_id,
        None,
        strategy_id,
        instrument_id,
        client_order_id,
        order_init,
        None,
        None,
        params,
        UUID4::new(),
        ts_init,
        None,
    )
}

fn single_order_report(reports: &[ExecutionReport]) -> &nautilus_model::reports::OrderStatusReport {
    assert_eq!(reports.len(), 1);
    let ExecutionReport::Order(report) = &reports[0] else {
        panic!("expected order status report");
    };
    report
}

fn single_order_with_fills_report(
    reports: &[ExecutionReport],
) -> (
    &nautilus_model::reports::OrderStatusReport,
    &[nautilus_model::reports::FillReport],
) {
    assert_eq!(reports.len(), 1);
    let ExecutionReport::OrderWithFills(report, fills) = &reports[0] else {
        panic!("expected bundled order status and fill reports");
    };
    (report, fills)
}

async fn run_order_status_report_query_test(
    cmd: nautilus_common::messages::execution::GenerateOrderStatusReport,
    expected_order_id_type: OrderIdType,
    expected_order_id: &str,
) {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        lots_executed: 0,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.connect_for_queries().await.unwrap();

    let report =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_order_status_report(
            &client, &cmd,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(report.venue_order_id.to_string(), "exchange-order-1");
    let state_calls = state_calls.lock().unwrap();
    assert_eq!(state_calls.len(), 1);
    assert_eq!(state_calls[0].order_id, expected_order_id);
    assert_eq!(
        state_calls[0].order_id_type,
        Some(expected_order_id_type as i32)
    );
}

use super::{
    TbankBrokerOrderIdentity, TbankBrokerOrderIndex, TbankBrokerOrderRoute, TbankCancelTarget,
    TbankExecutionClient, TbankManagedOrderContext, TbankPendingSubmit, TbankPendingSubmitStage,
    TbankSubmitResponse, TbankTimeInForceType, confirm_margin_trade_for_submit, current_unix_nanos,
    fill_report_from_order_trade, fill_side_from_operation_type, project_order_status_report,
    reconnect_reconciliation_error_is_transient, resolve_stream_order_venue_id,
    settle_order_report_mutation_state, settle_reconciled_buffered_trade_fill,
    stream_order_state_client_order_id, stream_order_status_report_from_state,
    stream_stop_order_status_report_from_state, submit_nautilus_order_reports,
    submit_nautilus_order_reports_with_recovery, tbank_broker_request_id_for_client_order_id,
    trailing_stop_params,
};
