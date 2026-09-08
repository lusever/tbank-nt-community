use super::submit::synthetic_fill_trade_id;
use super::{
    CANCEL_OUTCOME_RECOVERY_ATTEMPTS, CancelFailureKind, MAX_UNRESOLVED_TRADE_FILLS_PER_ORDER,
    TBANK_CONFIRM_MARGIN_TRADE_PARAM, TbankFillProjection,
    activated_stop_child_status_report_with_context, buffer_unresolved_trade_fill,
    canonicalize_reconciled_stop_fill, classify_cancel_failure,
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
    msgbus::{self, switchboard},
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
        PositionSide, TimeInForce, TrailingOffsetType, TriggerType,
    },
    events::{AccountState, OrderEventAny, OrderInitialized},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, Venue, VenueOrderId,
    },
    orders::OrderList,
    instruments::Instrument,
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};

use crate::{
    common::{TbankAdapterError, TbankOrderSide, TbankOrderType, venue::TbankVenue},
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
        PortfolioPosition, PortfolioRequest, PortfolioResponse, PortfolioStreamResponse,
        PositionData, PositionsRequest, PositionsResponse, PositionsSecurities,
        PositionsStreamResponse,
        PostOrderAsyncRequest, PostOrderAsyncResponse, PostOrderRequest,
        PostOrderResponse, PostStopOrderRequest, PostStopOrderResponse, Quotation,
        ReplaceOrderRequest, StopOrder, StopOrderDirection, StopOrderStatusOption, StopOrderType,
        TakeProfitType, TradesStreamRequest, TradesStreamResponse, TrailingValueType,
        WithdrawLimitsRequest, WithdrawLimitsResponse,
        operations_service_server::{OperationsService, OperationsServiceServer},
        order_state_stream_response,
        orders_service_server::{OrdersService, OrdersServiceServer},
        orders_stream_service_server::{OrdersStreamService, OrdersStreamServiceServer},
        portfolio_stream_response, positions_stream_response, stop_order,
        stop_orders_service_server::{StopOrdersService, StopOrdersServiceServer},
    },
    testing::fixtures::sber_metadata,
};

#[test]
fn execution_client_subscribes_to_each_supported_public_venue() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client.subscribe_instrument_updates();

    let mut metadata = sber_metadata();
    metadata.instrument_id = "AAPL_SPBXM.SPBE".to_string();
    metadata.ticker = "AAPL".to_string();
    metadata.class_code = "SPBXM".to_string();
    metadata.figi = "BBG-AAPL-SPBE".to_string();
    metadata.instrument_uid = "aapl-spbe-uid".to_string();
    metadata.position_uid = "aapl-spbe-position".to_string();
    metadata.exchange = "SPBE".to_string();
    metadata.venue = TbankVenue::Spbe;
    let instrument = crate::instruments::build_equity_instrument(&metadata).unwrap();

    msgbus::publish_instrument(
        switchboard::get_instrument_topic(instrument.id()),
        &instrument,
    );

    assert!(client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .contains_key("AAPL_SPBXM.SPBE"));
    client.unsubscribe_instrument_updates();
}

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

fn futures_margin_response() -> crate::grpc::generated::GetFuturesMarginResponse {
    crate::grpc::generated::GetFuturesMarginResponse {
        initial_margin_on_buy: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 15_000,
            nano: 0,
        }),
        initial_margin_on_sell: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 16_000,
            nano: 0,
        }),
        min_price_increment: Some(Quotation {
            units: 0,
            nano: 500_000_000,
        }),
        min_price_increment_amount: Some(Quotation { units: 75, nano: 0 }),
    }
}

fn current_si_future() -> crate::grpc::generated::Future {
    crate::grpc::generated::Future {
        ticker: "Si-9.26".to_string(),
        class_code: "SPBFUT".to_string(),
        lot: 1,
        currency: "rub".to_string(),
        exchange: "moex_mrng_evng_e_wknd_dlr".to_string(),
        min_price_increment: Some(Quotation { units: 1, nano: 0 }),
        min_price_increment_amount: Some(Quotation {
            units: 12,
            nano: 500_000_000,
        }),
        dlong_client: Some(Quotation {
            units: 0,
            nano: 100_000_000,
        }),
        dshort_client: Some(Quotation {
            units: 0,
            nano: 150_000_000,
        }),
        uid: "current-si-future-uid".to_string(),
        position_uid: "current-si-position-uid".to_string(),
        basic_asset: "USD".to_string(),
        asset_type: "currency".to_string(),
        real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
        ..crate::grpc::generated::Future::default()
    }
}

fn live_query_client(endpoint: String) -> TbankExecutionClient {
    test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(endpoint),
        ..TbankExecutionClientConfig::default()
    })
}

struct FuturesInstrumentsTestServer {
    endpoint: String,
    margin_calls: Arc<AtomicU64>,
    margin_started: Arc<tokio::sync::Notify>,
    margin_release: Arc<tokio::sync::Notify>,
    future_calls: Arc<AtomicU64>,
}

async fn start_futures_instruments_server() -> FuturesInstrumentsTestServer {
    let margin_calls = Arc::new(AtomicU64::new(0));
    let margin_started = Arc::new(tokio::sync::Notify::new());
    let margin_release = Arc::new(tokio::sync::Notify::new());
    let future_calls = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());

    let service = FuturesInstrumentsService {
        calls: Arc::clone(&margin_calls),
        started: Arc::clone(&margin_started),
        release: Arc::clone(&margin_release),
        response: futures_margin_response(),
        future: current_si_future(),
        future_calls: Arc::clone(&future_calls),
    };
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    FuturesInstrumentsTestServer {
        endpoint,
        margin_calls,
        margin_started,
        margin_release,
        future_calls,
    }
}

fn seed_sber_metadata(client: &mut TbankExecutionClient) {
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
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

fn order_stream_context(
    client: &TbankExecutionClient,
    event_tx: tokio::sync::mpsc::UnboundedSender<ExecutionEvent>,
    lifecycle_active: Arc<super::TbankLifecycleToken>,
) -> super::TbankOrderStreamContext {
    super::TbankOrderStreamContext {
        emitter: test_emitter(event_tx),
        query_client: client.runtime.detached_query_clone(),
        lifecycle_active,
        pending_submits: client.runtime.pending_submits.clone(),
        unresolved_trade_fills: client.runtime.unresolved_trade_fills.clone(),
        unresolved_cancellations: client.runtime.unresolved_cancellations.clone(),
        broker_order_index: client.runtime.broker_order_index.clone(),
        fill_projection: client.runtime.fill_projection.clone(),
        order_status_projection: client.runtime.order_status_projection.clone(),
        instruments: client.runtime.instruments.clone(),
        reconnect_policy: client.runtime.config.reconnect_policy.clone(),
        activated_stop_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        regular_order_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        reconciliation_tasks: client.runtime.reconciliation_tasks.clone(),
    }
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
                account_id: "001".to_string(),
                total_amount_portfolio: Some(MoneyValue {
                    currency: "rub".to_string(),
                    units: 100,
                    nano: 0,
                }),
                ..PortfolioResponse::default()
            },
        )),
    };

    let instruments = Arc::new(Mutex::new(HashMap::new()));
    super::publish_portfolio_response(
        response,
        &emitter,
        &projection,
        &instruments,
        &stale_generation,
        true,
        &HashSet::new(),
    );

    assert!(new_generation.load(Ordering::Acquire));
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn inactive_order_state_does_not_commit_fill_projection() {
    let client = test_client(TbankExecutionClientConfig::default());
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let lifecycle_active = Arc::new(super::TbankLifecycleToken::new(false));
    let context = order_stream_context(&client, event_tx, lifecycle_active);
    let ts = current_unix_nanos();
    let report = OrderStatusReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        Some("client-order-1".into()),
        "venue-order-1".into(),
        Some(OrderSide::Buy),
        OrderType::Market,
        TimeInForce::Ioc,
        OrderStatus::PartiallyFilled,
        Quantity::from(10),
        Quantity::from(10),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    )
    .with_avg_px(Decimal::from(100));
    let fill = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "venue-order-1".into(),
        "trade-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("100"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        Some("client-order-1".into()),
        None,
        ts,
        ts,
        Some(UUID4::new()),
    );

    assert!(super::publish_order_state_report_with_fills(&context, report, vec![fill]).is_none());
    assert!(context.fill_projection.lock().unwrap().orders.is_empty());
    assert!(context
        .order_status_projection
        .lock()
        .unwrap()
        .is_empty());
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn partial_order_state_trades_use_cumulative_fill_without_mixing_prices() {
    let client = test_client(TbankExecutionClientConfig::default());
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let context = order_stream_context(
        &client,
        event_tx,
        Arc::new(super::TbankLifecycleToken::new(true)),
    );
    let ts = current_unix_nanos();
    let report = OrderStatusReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        Some("client-order-1".into()),
        "venue-order-1".into(),
        Some(OrderSide::Buy),
        OrderType::Market,
        TimeInForce::Ioc,
        OrderStatus::PartiallyFilled,
        Quantity::from(10),
        Quantity::from(10),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    )
    .with_avg_px(Decimal::from(105));
    let raw_fill = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "venue-order-1".into(),
        "trade-1".into(),
        OrderSide::Buy,
        Quantity::from(5),
        Price::from("100"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        Some("client-order-1".into()),
        None,
        ts,
        ts,
        Some(UUID4::new()),
    );

    assert!(matches!(
        super::publish_order_state_report_with_fills(&context, report, vec![raw_fill]),
        Some(Ok(()))
    ));
    let ExecutionEvent::Report(ExecutionReport::OrderWithFills(order, fills)) =
        event_rx.try_recv().unwrap()
    else {
        panic!("expected bundled order status and cumulative fill");
    };
    assert_eq!(order.filled_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].last_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fills[0].last_px.as_decimal(), Decimal::from(105));
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn valid_order_state_trade_survives_status_mapping_error() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    seed_sber_metadata(&mut client);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let context = order_stream_context(
        &client,
        event_tx,
        Arc::new(super::TbankLifecycleToken::new(true)),
    );
    let state = order_state_stream_response::OrderState {
        order_request_id: Some("client-order-1".to_string()),
        order_id: "venue-order-1".to_string(),
        trade_order_id: "trade-order-1".to_string(),
        account_id: "account-1".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        instrument_uid: "sber-uid".to_string(),
        lot_size: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 10,
        lots_executed: 1,
        completion_time: Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 0,
        }),
        trades: vec![OrderTrade {
            price: Some(Quotation { units: 100, nano: 0 }),
            quantity: 5,
            trade_id: "trade-1".to_string(),
            ..OrderTrade::default()
        }],
        ..order_state_stream_response::OrderState::default()
    };
    let ts = current_unix_nanos();
    let raw_fill = super::fill_reports_from_order_state_stream(
        &state,
        "venue-order-1",
        None,
        ts,
        &context.instruments,
    )
    .into_iter()
    .next()
    .unwrap()
    .unwrap();
    let status_error = super::stream_order_status_report_from_state_with_instruments(
        state,
        "venue-order-1",
        ts,
        Some("client-order-1"),
        None,
        Some(&context.instruments),
    )
    .unwrap_err();
    assert!(status_error.to_string().contains("timestamp"));

    assert!(matches!(
        super::publish_order_state_report_or_fills(&context, None, vec![raw_fill]),
        Some(Ok(()))
    ));
    let ExecutionEvent::Report(ExecutionReport::Fill(fill)) = event_rx.try_recv().unwrap() else {
        panic!("expected the valid trade fill to be published");
    };
    assert_eq!(fill.last_qty.as_decimal(), Decimal::from(5));
    assert_eq!(fill.last_px.as_decimal(), Decimal::from(100));
    assert!(event_rx.try_recv().is_err());
}

#[test]
fn unresolved_portfolio_position_does_not_flatten_projection() {
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let emitter = test_emitter(event_tx);
    let lifecycle_active = Arc::new(super::TbankLifecycleToken::new(true));
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let account_id: AccountId = "TBANK-001".into();
    let instrument_id: InstrumentId = "SBER_TQBR.MOEX".parse().unwrap();
    let ts_init = current_unix_nanos();
    let active = PositionStatusReport::new(
        account_id,
        instrument_id,
        PositionSide::Long,
        Quantity::from(10),
        ts_init,
        ts_init,
        Some(UUID4::new()),
        Some("SBER-POSITION".into()),
        None,
    );
    super::record_position_projection_from_source(
        &projection,
        &active,
        super::TbankPositionProjectionSource::PortfolioStream,
    );

    let response = PortfolioStreamResponse {
        payload: Some(portfolio_stream_response::Payload::Portfolio(
            PortfolioResponse {
                account_id: "001".to_string(),
                positions: vec![PortfolioPosition {
                    instrument_uid: "unknown-uid".to_string(),
                    quantity: Some(Quotation {
                        units: 10,
                        nano: 0,
                    }),
                    ..PortfolioPosition::default()
                }],
                ..PortfolioResponse::default()
            },
        )),
    };
    let instruments = Arc::new(Mutex::new(HashMap::new()));

    super::publish_portfolio_response(
        response,
        &emitter,
        &projection,
        &instruments,
        &lifecycle_active,
        false,
        &HashSet::new(),
    );

    assert!(!projection.lock().unwrap().values().next().unwrap().is_flat);
}

#[test]
fn out_of_scope_portfolio_position_does_not_block_scoped_reconciliation() {
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let emitter = test_emitter(event_tx);
    let lifecycle_active = Arc::new(super::TbankLifecycleToken::new(true));
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let account_id: AccountId = "TBANK-001".into();
    let instrument_id: InstrumentId = "SBER_TQBR.MOEX".parse().unwrap();
    let ts_init = current_unix_nanos();
    let active = PositionStatusReport::new(
        account_id,
        instrument_id,
        PositionSide::Long,
        Quantity::from(10),
        ts_init,
        ts_init,
        Some(UUID4::new()),
        Some("SBER-POSITION".into()),
        None,
    );
    super::record_position_projection_from_source(
        &projection,
        &active,
        super::TbankPositionProjectionSource::PortfolioStream,
    );

    let response = PortfolioStreamResponse {
        payload: Some(portfolio_stream_response::Payload::Portfolio(
            PortfolioResponse {
                account_id: "001".to_string(),
                positions: vec![
                    PortfolioPosition {
                        instrument_uid: sber_metadata().instrument_uid,
                        ticker: "SBER".to_string(),
                        class_code: "TQBR".to_string(),
                        quantity: Some(Quotation {
                            units: 10,
                            nano: 0,
                        }),
                        ..PortfolioPosition::default()
                    },
                    PortfolioPosition {
                        instrument_uid: "outside-uid".to_string(),
                        quantity: Some(Quotation {
                            units: 10,
                            nano: 0,
                        }),
                        ..PortfolioPosition::default()
                    },
                ],
                ..PortfolioResponse::default()
            },
        )),
    };
    let metadata = sber_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));
    let out_of_scope_positions = HashSet::from(["uid:outside-uid".to_string()]);

    super::publish_portfolio_response(
        response,
        &emitter,
        &projection,
        &instruments,
        &lifecycle_active,
        true,
        &out_of_scope_positions,
    );

    assert!(!projection.lock().unwrap().values().next().unwrap().is_flat);
}

#[test]
fn cached_out_of_scope_positions_are_not_published_by_any_position_stream_variant() {
    let mut metadata = sber_metadata();
    metadata.instrument_id = "BOND_TQOB.MOEX".to_string();
    metadata.class_code = "TQOB".to_string();
    metadata.instrument_uid = "outside-uid".to_string();
    metadata.position_uid = "outside-position".to_string();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));
    let out_of_scope_positions = HashSet::from(["uid:outside-uid".to_string()]);

    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let emitter = test_emitter(event_tx);
    let lifecycle_active = Arc::new(super::TbankLifecycleToken::new(true));
    let portfolio_projection = Arc::new(Mutex::new(HashMap::new()));
    super::publish_portfolio_response(
        PortfolioStreamResponse {
            payload: Some(portfolio_stream_response::Payload::Portfolio(
                PortfolioResponse {
                    account_id: "001".to_string(),
                    positions: vec![PortfolioPosition {
                        instrument_uid: "outside-uid".to_string(),
                        quantity: Some(Quotation { units: 10, nano: 0 }),
                        ..PortfolioPosition::default()
                    }],
                    ..PortfolioResponse::default()
                },
            )),
        },
        &emitter,
        &portfolio_projection,
        &instruments,
        &lifecycle_active,
        true,
        &out_of_scope_positions,
    );
    assert!(portfolio_projection.lock().unwrap().is_empty());

    let position_projection = Arc::new(Mutex::new(HashMap::new()));
    super::publish_positions_response(
        PositionsStreamResponse {
            payload: Some(positions_stream_response::Payload::Position(PositionData {
                    account_id: "001".to_string(),
                    securities: vec![PositionsSecurities {
                        instrument_uid: "outside-uid".to_string(),
                        balance: 10,
                        ..PositionsSecurities::default()
                    }],
                    ..PositionData::default()
                })),
        },
        &emitter,
        &position_projection,
        &instruments,
        &lifecycle_active,
        true,
        &out_of_scope_positions,
    );
    assert!(position_projection.lock().unwrap().is_empty());

    let initial_projection = Arc::new(Mutex::new(HashMap::new()));
    super::publish_positions_response(
        PositionsStreamResponse {
            payload: Some(positions_stream_response::Payload::InitialPositions(
                PositionsResponse {
                    account_id: "001".to_string(),
                    securities: vec![PositionsSecurities {
                        instrument_uid: "outside-uid".to_string(),
                        balance: 10,
                        ..PositionsSecurities::default()
                    }],
                    ..PositionsResponse::default()
                },
            )),
        },
        &emitter,
        &initial_projection,
        &instruments,
        &lifecycle_active,
        true,
        &out_of_scope_positions,
    );
    assert!(initial_projection.lock().unwrap().is_empty());
}

#[test]
fn unresolved_initial_position_does_not_flatten_projection() {
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let emitter = test_emitter(event_tx);
    let lifecycle_active = Arc::new(super::TbankLifecycleToken::new(true));
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let account_id: AccountId = "TBANK-001".into();
    let instrument_id: InstrumentId = "SBER_TQBR.MOEX".parse().unwrap();
    let ts_init = current_unix_nanos();
    let active = PositionStatusReport::new(
        account_id,
        instrument_id,
        PositionSide::Long,
        Quantity::from(10),
        ts_init,
        ts_init,
        Some(UUID4::new()),
        Some("SBER-POSITION".into()),
        None,
    );
    super::record_position_projection_from_source(
        &projection,
        &active,
        super::TbankPositionProjectionSource::SecuritiesSnapshot,
    );

    let response = PositionsStreamResponse {
        payload: Some(positions_stream_response::Payload::InitialPositions(
            PositionsResponse {
                account_id: "001".to_string(),
                securities: vec![PositionsSecurities {
                    instrument_uid: "unknown-uid".to_string(),
                    balance: 10,
                    ..PositionsSecurities::default()
                }],
                ..PositionsResponse::default()
            },
        )),
    };
    let instruments = Arc::new(Mutex::new(HashMap::new()));

    super::publish_positions_response(
        response,
        &emitter,
        &projection,
        &instruments,
        &lifecycle_active,
        true,
        &HashSet::new(),
    );

    assert!(!projection.lock().unwrap().values().next().unwrap().is_flat);
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
        quantity_units: Decimal::ONE,
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
            quantity_units: Decimal::ONE,
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
            broker_order_id: "venue-order-1".to_string(),
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
        Some(OrderSide::Buy),
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
            quantity_units: Decimal::ONE,
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
fn reconnect_starts_a_new_futures_margin_freshness_generation() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .futures_margin_refreshed_at
        .lock()
        .unwrap()
        .insert("Si-9.26_SPBFUT.MOEX".to_string(), std::time::Instant::now());
    let previous_cache = Arc::clone(&client.runtime.futures_margin_refreshed_at);

    client.runtime.disconnect();
    client.runtime.begin_connection_generation();

    assert!(!Arc::ptr_eq(
        &previous_cache,
        &client.runtime.futures_margin_refreshed_at
    ));
    assert!(client
        .runtime
        .futures_margin_refreshed_at
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn stale_futures_margin_clone_cannot_use_old_generation_cache() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    let mut stale_runtime = client.runtime.clone();
    stale_runtime
        .futures_margin_refreshed_at
        .lock()
        .unwrap()
        .insert("Si-9.26_SPBFUT.MOEX".to_string(), std::time::Instant::now());

    client.runtime.begin_connection_generation();

    let error = stale_runtime
        .refresh_futures_margin(si_futures_metadata())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TbankAdapterError::FuturesMarginUnresolved(message)
            if message.contains("discarding stale futures margin request")
    ));
}

#[tokio::test]
async fn reset_invalidates_stale_futures_margin_clone() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    let mut stale_runtime = client.runtime.clone();

    ExecutionClient::reset(&mut client).unwrap();

    let error = stale_runtime
        .refresh_futures_margin(si_futures_metadata())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TbankAdapterError::FuturesMarginUnresolved(message)
            if message.contains("discarding stale futures margin request")
    ));
}

#[tokio::test]
async fn concurrent_futures_margin_refreshes_share_one_request() {
    let server = start_futures_instruments_server().await;
    let mut client = live_query_client(server.endpoint.clone());
    client.runtime.connect_for_queries().await.unwrap();

    let mut metadata = si_futures_metadata();
    metadata.instrument_type = crate::common::venue::TbankInstrumentType::Futures;
    metadata.initial_margin_rate_on_buy = Some(Decimal::ONE);
    metadata.initial_margin_rate_on_sell = Some(Decimal::ONE);
    let mut first_runtime = client.runtime.clone();
    let first_future = first_runtime.refresh_futures_margin(metadata.clone());
    tokio::pin!(first_future);
    tokio::select! {
        result = &mut first_future => panic!("first refresh completed unexpectedly: {result:?}"),
        _ = server.margin_started.notified() => {},
    }

    let mut second_runtime = client.runtime.clone();
    let second_future = second_runtime.refresh_futures_margin(metadata);
    tokio::pin!(second_future);
    tokio::select! {
        result = &mut second_future => panic!("second refresh completed unexpectedly: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
    }
    assert_eq!(server.margin_calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.future_calls.load(Ordering::SeqCst), 0);

    server.margin_release.notify_waiters();
    let first_result = first_future.await;
    let second_result = second_future.await;
    assert!(first_result.is_ok(), "first refresh failed: {first_result:?}");
    assert!(
        second_result.is_ok(),
        "second refresh failed: {second_result:?}"
    );
}

#[tokio::test]
async fn incomplete_cached_futures_metadata_is_replaced_by_future_by() {
    let server = start_futures_instruments_server().await;
    let mut client = live_query_client(server.endpoint.clone());
    client.runtime.connect_for_queries().await.unwrap();

    let mut cached = si_futures_metadata();
    cached.instrument_type = crate::common::venue::TbankInstrumentType::Futures;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(cached.instrument_id.clone(), cached);
    client
        .runtime
        .futures_margin_refreshed_at
        .lock()
        .unwrap()
        .insert(
            "Si-9.26_SPBFUT.MOEX".to_string(),
            std::time::Instant::now(),
        );

    let mut load = Box::pin(
        client
            .runtime
            .load_instrument_metadata("Si-9.26_SPBFUT.MOEX"),
    );
    tokio::select! {
        result = &mut load => panic!("futures metadata load completed before margin response: {result:?}"),
        _ = server.margin_started.notified() => {},
    }
    assert_eq!(server.future_calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.margin_calls.load(Ordering::SeqCst), 1);
    server.margin_release.notify_waiters();

    let resolved = load.await.unwrap();
    assert_eq!(
        resolved.instrument_uid,
        "current-si-future-uid"
    );
    assert_eq!(
        resolved.initial_margin_rate_on_buy,
        Some(Decimal::new(10, 2))
    );
    assert_eq!(
        resolved.initial_margin_rate_on_sell,
        Some(Decimal::new(15, 2))
    );
    assert_eq!(resolved.min_price_increment, Decimal::new(5, 1));
    assert_eq!(resolved.multiplier, Decimal::from(150));
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
    let unresolved_metadata = anyhow::Error::new(
        TbankAdapterError::InstrumentMetadataUnresolved("uid:pending".to_string()),
    );
    let unresolved_futures_margin = anyhow::Error::new(
        TbankAdapterError::FuturesMarginUnresolved("Si-9.26_SPBFUT.MOEX".to_string()),
    );
    let out_of_scope =
        anyhow::Error::new(TbankAdapterError::InstrumentOutOfScope("uid:outside".to_string()));
    let invalid_event = anyhow::Error::new(TbankAdapterError::InvalidInstrumentIdentity(
        "ticker:AAPL:TQBR".to_string(),
    ));
    let malformed = anyhow::anyhow!("malformed broker data");

    assert!(reconnect_reconciliation_error_is_transient(&unavailable));
    assert!(reconnect_reconciliation_error_is_transient(&rate_limited));
    assert!(reconnect_reconciliation_error_is_transient(
        &unresolved_metadata
    ));
    assert!(reconnect_reconciliation_error_is_transient(
        &unresolved_futures_margin
    ));
    assert!(!super::TbankExecutionRuntime::metadata_error_is_event_rejection(
        &TbankAdapterError::FuturesMarginUnresolved("Si-9.26_SPBFUT.MOEX".to_string())
    ));
    assert!(!reconnect_reconciliation_error_is_transient(
        &permission_denied
    ));
    assert!(!reconnect_reconciliation_error_is_transient(&out_of_scope));
    assert!(!reconnect_reconciliation_error_is_transient(&malformed));
    assert!(super::reconnect_reconciliation_error_is_safe_to_skip(
        &out_of_scope
    ));
    assert!(super::reconnect_reconciliation_error_is_safe_to_skip(
        &invalid_event
    ));
    assert!(!super::reconnect_reconciliation_error_is_safe_to_skip(
        &permission_denied
    ));
    assert!(!super::reconnect_reconciliation_error_is_safe_to_skip(
        &malformed
    ));
}

#[test]
fn event_identity_rejects_each_contradictory_partial_component() {
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.figi = "sber-figi".to_string();

    assert!(super::metadata_matches_event_identity(
        &metadata,
        "sber-uid",
        "",
        "SBER",
        "",
    ));
    assert!(!super::metadata_matches_event_identity(
        &metadata,
        "sber-uid",
        "",
        "AAPL",
        "",
    ));
    assert!(!super::metadata_matches_event_identity(
        &metadata,
        "sber-uid",
        "",
        "",
        "SPBXM",
    ));
    assert!(!super::metadata_matches_event_identity(
        &metadata,
        "sber-figi",
        "",
        "",
        "",
    ));
    assert!(super::metadata_matches_event_identity(
        &metadata,
        "",
        "sber-figi",
        "",
        "",
    ));
    assert!(!super::metadata_matches_event_identity(
        &metadata,
        "sber-uid",
        "other-figi",
        "",
        "",
    ));
}

#[tokio::test]
async fn missing_instrument_identity_is_rejected_without_retry() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    let error = client
        .runtime
        .load_supported_metadata_for_identity("", "", "", "")
        .await
        .unwrap_err();

    assert!(matches!(
        &error,
        TbankAdapterError::InvalidInstrumentIdentity(identity)
            if identity.starts_with("broker instrument identity:")
    ));
    assert!(!reconnect_reconciliation_error_is_transient(&anyhow::Error::new(error)));
}

#[test]
fn metadata_error_identity_uses_public_or_safe_label() {
    assert_eq!(
        super::instrument_metadata_identity("public-uid", "", "", ""),
        "instrument_uid:public-uid"
    );
    assert_eq!(
        super::instrument_metadata_identity("", "BBG000000000", "", ""),
        "figi:BBG000000000"
    );
    let public = super::unresolved_instrument_metadata_error("SBER", "TQBR");
    assert_eq!(
        public.to_string(),
        "instrument metadata unresolved: ticker:SBER:TQBR"
    );

    let redacted = super::unresolved_instrument_metadata_error("", "");
    assert_eq!(
        redacted.to_string(),
        "instrument metadata unresolved: broker instrument identity"
    );
}

#[tokio::test]
async fn uncached_share_by_resolution_classifies_out_of_scope() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let share = crate::grpc::generated::Share {
        ticker: "BOND".to_string(),
        class_code: "TQTF".to_string(),
        currency: "rub".to_string(),
        lot: 1,
        min_price_increment: Some(Quotation {
            units: 0,
            nano: 10_000_000,
        }),
        uid: "bond-uid".to_string(),
        real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
        ..crate::grpc::generated::Share::default()
    };

    tokio::spawn(async move {
        Server::builder()
            .add_service(ShareByOnlyInstrumentsService {
                share: Some(share),
                lookup_started: None,
            })
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

    let error = client
        .runtime
        .load_supported_metadata_for_identity("bond-uid", "", "BOND", "TQTF")
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            TbankAdapterError::InstrumentOutOfScope(identity)
                if identity == "BOND_TQTF.MOEX"
        ),
        "unexpected metadata resolution error: {error:?}"
    );
    assert!(!reconnect_reconciliation_error_is_transient(&anyhow::Error::new(
        error
    )));
}

#[tokio::test]
async fn malformed_share_metadata_classifies_out_of_scope_without_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let share = crate::grpc::generated::Share {
        ticker: "BROKEN".to_string(),
        class_code: "TQBR".to_string(),
        currency: "ZZZ".to_string(),
        lot: 1,
        min_price_increment: Some(Quotation { units: 1, nano: 0 }),
        uid: "broken-share-uid".to_string(),
        real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
        ..crate::grpc::generated::Share::default()
    };

    tokio::spawn(async move {
        Server::builder()
            .add_service(ShareByOnlyInstrumentsService {
                share: Some(share),
                lookup_started: None,
            })
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

    let error = client
        .runtime
        .load_supported_metadata_for_identity("broken-share-uid", "", "BROKEN", "TQBR")
        .await
        .unwrap_err();

    assert!(matches!(
        &error,
        TbankAdapterError::InstrumentOutOfScope(identity)
            if identity == "ticker:BROKEN:TQBR"
    ));
    assert!(!reconnect_reconciliation_error_is_transient(&anyhow::Error::new(
        error
    )));
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
    initial_state: Option<order_state_stream_response::OrderState>,
    reopened_state: order_state_stream_response::OrderState,
}

#[derive(Clone, Default)]
struct MockStopOrdersService {
    post_calls: Arc<Mutex<Vec<PostStopOrderRequest>>>,
    post_error: Arc<Mutex<Option<(Code, String)>>>,
    post_response: Arc<Mutex<Option<PostStopOrderResponse>>>,
    get_calls: Arc<Mutex<Vec<GetStopOrdersRequest>>>,
    get_responses: Arc<Mutex<VecDeque<GetStopOrdersResponse>>>,
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

#[derive(Clone)]
struct ShareByOnlyInstrumentsService {
    share: Option<crate::grpc::generated::Share>,
    lookup_started: Option<Arc<AtomicBool>>,
}

impl tonic::server::NamedService for ShareByOnlyInstrumentsService {
    const NAME: &'static str = "tinkoff.public.invest.api.contract.v1.InstrumentsService";
}

impl<B> tonic::codegen::Service<http::Request<B>> for ShareByOnlyInstrumentsService
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let share = self.share.clone();
        match request.uri().path() {
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy" => {
                struct ShareByService {
                    share: Option<crate::grpc::generated::Share>,
                }

                impl tonic::server::UnaryService<crate::grpc::generated::InstrumentRequest>
                    for ShareByService
                {
                    type Response = crate::grpc::generated::ShareResponse;
                    type Future = tonic::codegen::BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;

                    fn call(
                        &mut self,
                        _request: tonic::Request<crate::grpc::generated::InstrumentRequest>,
                    ) -> Self::Future {
                        let share = self.share.clone();
                        Box::pin(async move {
                            let Some(share) = share else {
                                return Err(tonic::Status::unimplemented(
                                    "share lookup is intentionally unavailable",
                                ));
                            };
                            Ok(tonic::Response::new(
                                crate::grpc::generated::ShareResponse {
                                    instrument: Some(share),
                                },
                            ))
                        })
                    }
                }

                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc
                        .unary(ShareByService { share }, request)
                        .await)
                })
            }
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy" => {
                struct GetInstrumentByService;

                impl tonic::server::UnaryService<crate::grpc::generated::InstrumentRequest>
                    for GetInstrumentByService
                {
                    type Response = crate::grpc::generated::InstrumentResponse;
                    type Future = tonic::codegen::BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;

                    fn call(
                        &mut self,
                        _request: tonic::Request<crate::grpc::generated::InstrumentRequest>,
                    ) -> Self::Future {
                        Box::pin(async {
                            Err(tonic::Status::unimplemented(
                                "instrument kind lookup is intentionally unavailable",
                            ))
                        })
                    }
                }

                let lookup_started = self.lookup_started.clone();
                Box::pin(async move {
                    if let Some(lookup_started) = lookup_started {
                        lookup_started.store(true, Ordering::SeqCst);
                    }
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc
                        .unary(GetInstrumentByService, request)
                        .await)
                })
            }
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/FutureBy" => {
                struct FutureByService;

                impl tonic::server::UnaryService<crate::grpc::generated::InstrumentRequest>
                    for FutureByService
                {
                    type Response = crate::grpc::generated::FutureResponse;
                    type Future = tonic::codegen::BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;

                    fn call(
                        &mut self,
                        _request: tonic::Request<crate::grpc::generated::InstrumentRequest>,
                    ) -> Self::Future {
                        Box::pin(async {
                            Err(tonic::Status::unimplemented(
                                "future lookup is intentionally unavailable",
                            ))
                        })
                    }
                }

                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.unary(FutureByService, request).await)
                })
            }
            _ => Box::pin(async {
                Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(tonic::body::Body::empty())
                    .expect("valid mock gRPC response"))
            }),
        }
    }
}

#[derive(Clone)]
struct FuturesInstrumentsService {
    calls: Arc<AtomicU64>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    response: crate::grpc::generated::GetFuturesMarginResponse,
    future: crate::grpc::generated::Future,
    future_calls: Arc<AtomicU64>,
}

impl tonic::server::NamedService for FuturesInstrumentsService {
    const NAME: &'static str = "tinkoff.public.invest.api.contract.v1.InstrumentsService";
}

impl<B> tonic::codegen::Service<http::Request<B>> for FuturesInstrumentsService
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path()
            == "/tinkoff.public.invest.api.contract.v1.InstrumentsService/FutureBy"
        {
            struct FutureByService {
                future: crate::grpc::generated::Future,
                calls: Arc<AtomicU64>,
            }

            impl tonic::server::UnaryService<crate::grpc::generated::InstrumentRequest>
                for FutureByService
            {
                type Response = crate::grpc::generated::FutureResponse;
                type Future = tonic::codegen::BoxFuture<
                    tonic::Response<Self::Response>,
                    tonic::Status,
                >;

                fn call(
                    &mut self,
                    _request: tonic::Request<crate::grpc::generated::InstrumentRequest>,
                ) -> Self::Future {
                    let future = self.future.clone();
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        Ok(tonic::Response::new(crate::grpc::generated::FutureResponse {
                            instrument: Some(future),
                        }))
                    })
                }
            }

            let service = FutureByService {
                future: self.future.clone(),
                calls: Arc::clone(&self.future_calls),
            };
            return Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(service, request).await)
            });
        }

        if request.uri().path()
            != "/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetFuturesMargin"
        {
            return Box::pin(async {
                Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(tonic::body::Body::empty())
                    .expect("valid mock gRPC response"))
            });
        }

        struct GetFuturesMarginService {
            calls: Arc<AtomicU64>,
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            response: crate::grpc::generated::GetFuturesMarginResponse,
        }

        impl tonic::server::UnaryService<crate::grpc::generated::GetFuturesMarginRequest>
            for GetFuturesMarginService
        {
            type Response = crate::grpc::generated::GetFuturesMarginResponse;
            type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

            fn call(
                &mut self,
                _request: tonic::Request<crate::grpc::generated::GetFuturesMarginRequest>,
            ) -> Self::Future {
                let calls = Arc::clone(&self.calls);
                let started = Arc::clone(&self.started);
                let release = Arc::clone(&self.release);
                let response = self.response.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                    Ok(tonic::Response::new(response))
                })
            }
        }

        let service = GetFuturesMarginService {
            calls: Arc::clone(&self.calls),
            started: Arc::clone(&self.started),
            release: Arc::clone(&self.release),
            response: self.response.clone(),
        };
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(service, request).await)
        })
    }
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
            let Some(initial_state) = self.initial_state.clone() else {
                return Ok(Response::new(Box::pin(stream::empty())));
            };
            let response = crate::grpc::generated::OrderStateStreamResponse {
                payload: Some(order_state_stream_response::Payload::OrderState(
                    initial_state,
                )),
            };
            return Ok(Response::new(Box::pin(
                stream::once(async move { Ok(response) }).chain(stream::pending()),
            )));
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
    seed_sber_metadata(&mut client);
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

    let report = activated_stop_child_status_report_with_context(
        "TBANK-001".into(),
        &stop,
        &state,
        current_unix_nanos(),
        10,
        Some(client_order_id),
        None,
        None,
    )
    .unwrap();

    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some(client_order_id.to_string())
    );
    assert_eq!(report.order_status, OrderStatus::Filled);
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(20));
    assert_eq!(report.avg_px, Some(Decimal::new(13575, 2)));
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
    seed_sber_metadata(&mut client);
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

#[test]
fn metadata_lookup_preserves_transient_fallback_error() {
    let share_error = TbankAdapterError::GrpcStatus {
        code: Code::InvalidArgument,
        message: "share lookup rejected".to_string(),
    };
    let future_error = TbankAdapterError::GrpcStatus {
        code: Code::Unavailable,
        message: "future lookup unavailable".to_string(),
    };

    let selected = super::TbankExecutionRuntime::select_metadata_lookup_error(
        share_error,
        future_error,
    );

    assert!(matches!(
        selected,
        TbankAdapterError::GrpcStatus {
            code: Code::Unavailable,
            ..
        }
    ));
}
