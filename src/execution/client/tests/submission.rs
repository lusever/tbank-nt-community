#[test]
fn confirm_margin_trade_param_overrides_global_default() {
    let mut params = Params::new();
    params.insert(TBANK_CONFIRM_MARGIN_TRADE_PARAM.to_string(), json!(false));
    assert!(!confirm_margin_trade_for_submit(true, Some(&params)));

    params.insert(TBANK_CONFIRM_MARGIN_TRADE_PARAM.to_string(), json!(true));
    assert!(confirm_margin_trade_for_submit(false, Some(&params)));

    assert!(confirm_margin_trade_for_submit(true, None));
    assert!(!confirm_margin_trade_for_submit(false, None));
}
#[test]
fn nautilus_trailing_order_maps_to_native_submit_params() {
    let mut cmd = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::TrailingStopLimit, None);
    cmd.order_init.activation_price = Some(Price::from("275.00"));
    cmd.order_init.trailing_offset = Some(Decimal::from(125));
    cmd.order_init.trailing_offset_type = Some(TrailingOffsetType::BasisPoints);
    cmd.order_init.limit_offset = Some(Decimal::from(50));
    cmd.order_init.trigger_type = Some(TriggerType::LastPrice);

    let params = trailing_stop_params(&cmd.order_init).unwrap().unwrap();
    assert_eq!(params.activation_price, Some(Decimal::from(275)));
    assert_eq!(params.trailing_offset, Decimal::from(125));
    assert_eq!(params.trailing_offset_type, TrailingOffsetType::BasisPoints);
    assert_eq!(params.limit_offset, Some(Decimal::from(50)));
    assert_eq!(params.trigger_type, Some(TriggerType::LastPrice));
}

#[test]
fn reconciliation_report_filters_match_upstream_command_contracts() {
    let account_id = AccountId::from("TBANK-account-1");
    let instrument_id = InstrumentId::from("SBER_TQBR.MOEX");
    let other_instrument_id = InstrumentId::from("GAZP_TQBR.MOEX");
    let ts = UnixNanos::from(200);
    let order = OrderStatusReport::new(
        account_id,
        instrument_id,
        Some(ClientOrderId::from("order-1")),
        VenueOrderId::from("venue-1"),
        OrderSide::Buy,
        OrderType::Limit,
        TimeInForce::Gtc,
        OrderStatus::Accepted,
        Quantity::from(1),
        Quantity::from(0),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    );
    let order_cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        ts,
        true,
        Some(instrument_id),
        Some(UnixNanos::from(100)),
        Some(UnixNanos::from(300)),
        None,
        None,
    );
    assert!(super::nautilus::order_report_matches_command(
        &order, &order_cmd
    ));
    let wrong_order_cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        ts,
        false,
        Some(other_instrument_id),
        None,
        None,
        None,
        None,
    );
    assert!(!super::nautilus::order_report_matches_command(
        &order,
        &wrong_order_cmd
    ));
    let mut terminal_order = order.clone();
    terminal_order.order_status = OrderStatus::Filled;
    assert!(!super::nautilus::order_report_matches_command(
        &terminal_order,
        &order_cmd
    ));
    let mut early_order = order.clone();
    early_order.ts_last = UnixNanos::from(50);
    assert!(!super::nautilus::order_report_matches_command(
        &early_order,
        &order_cmd
    ));

    let fill = FillReport::new(
        account_id,
        instrument_id,
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
    let fill_cmd = GenerateFillReports::new(
        UUID4::new(),
        ts,
        Some(instrument_id),
        Some(VenueOrderId::from("venue-1")),
        Some(UnixNanos::from(100)),
        Some(UnixNanos::from(300)),
        None,
        None,
    );
    assert!(super::nautilus::fill_report_matches_command(
        &fill, &fill_cmd
    ));
    let wrong_fill_cmd = GenerateFillReports::new(
        UUID4::new(),
        ts,
        None,
        Some(VenueOrderId::from("venue-2")),
        None,
        None,
        None,
        None,
    );
    assert!(!super::nautilus::fill_report_matches_command(
        &fill,
        &wrong_fill_cmd
    ));
    let mut early_fill = fill.clone();
    early_fill.ts_event = UnixNanos::from(50);
    assert!(!super::nautilus::fill_report_matches_command(
        &early_fill,
        &fill_cmd
    ));

    let position = PositionStatusReport::new(
        account_id,
        instrument_id,
        PositionSideSpecified::Long,
        Quantity::from(1),
        ts,
        ts,
        Some(UUID4::new()),
        None,
        None,
    );
    let position_cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        ts,
        Some(instrument_id),
        Some(UnixNanos::from(100)),
        Some(UnixNanos::from(300)),
        None,
        None,
    );
    assert!(super::nautilus::position_report_matches_command(
        &position,
        &position_cmd
    ));
    let wrong_position_cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        ts,
        Some(other_instrument_id),
        None,
        None,
        None,
        None,
    );
    assert!(!super::nautilus::position_report_matches_command(
        &position,
        &wrong_position_cmd
    ));
}

#[test]
fn execution_client_lifecycle_is_idempotent() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);

    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::start(&mut client)
            .is_ok()
    );
    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::start(&mut client)
            .is_ok()
    );
    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::stop(&mut client)
            .is_ok()
    );
    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::stop(&mut client)
            .is_ok()
    );
    assert!(!client.is_connected());
}

#[tokio::test]
async fn execution_client_connect_and_disconnect_are_idempotent() {
    let service = MockOrdersService::default();
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

    client.runtime.connect().await.unwrap();
    client.runtime.connect().await.unwrap();
    assert!(client.runtime.is_connected());
    client.disconnect();
    client.disconnect();
    assert!(!client.runtime.is_connected());
}

#[test]
fn generate_account_state_emits_without_duplicating_core_cache() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let balance = AccountBalance::from_total_and_free(
        Decimal::from(100),
        Decimal::from(75),
        Currency::from("RUB"),
    )
    .unwrap();

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_account_state(
        &client,
        vec![balance],
        Vec::new(),
        true,
        UnixNanos::from(100),
    )
    .unwrap();

    let event = receiver.try_recv().unwrap();
    let ExecutionEvent::Account(state) = event else {
        panic!("expected AccountState");
    };
    assert!(state.is_reported);
    assert_eq!(state.ts_event, UnixNanos::from(100));
    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::get_account(&client)
            .is_none()
    );
}

#[tokio::test]
async fn query_account_returns_immediately_and_emits_account_state() {
    let service = MockOperationsService::default();
    let portfolio_calls = Arc::clone(&service.portfolio_calls);
    *service.portfolio_response.lock().unwrap() = Some(PortfolioResponse {
        account_id: "account-1".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 75,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(OperationsServiceServer::new(service))
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
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let cmd = QueryAccount::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("TBANK")),
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::account_id(&client),
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    );

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::query_account(
        &client, cmd,
    )
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, ExecutionEvent::Account(_)));
    assert_eq!(
        portfolio_calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn connect_rolls_back_after_account_registration_timeout() {
    let service = MockOperationsService::default();
    *service.portfolio_response.lock().unwrap() = Some(PortfolioResponse {
        account_id: "account-1".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 75,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(OperationsServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        account_registration_timeout: std::time::Duration::from_millis(20),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);

    let error = client.connect().await.unwrap_err();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, ExecutionEvent::Account(_)));
    assert!(
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::get_account(&client)
            .is_none()
    );
    assert!(error.to_string().contains("account registration"));
    assert!(!error.to_string().contains("account-1"));
    assert!(!client.runtime.is_connected());
}

#[tokio::test]
async fn account_registration_gate_observes_nautilus_cache() {
    let account_id = AccountId::from("TBANK-account-1");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("TBANK"),
        Venue::from("MOEX"),
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let client = TbankExecutionClient::new(
        core,
        TbankExecutionClientConfig {
            account_id: Some("account-1".to_string()),
            account_registration_timeout: std::time::Duration::from_secs(1),
            ..TbankExecutionClientConfig::default()
        },
    );
    cache
        .borrow_mut()
        .add_account(AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                Vec::new(),
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::from(1),
                UnixNanos::from(1),
                None,
            ),
            true,
        )))
        .unwrap();

    client.await_account_registered().await.unwrap();
}

#[tokio::test]
async fn order_stream_reconnect_reconciles_before_consuming_reopened_events() {
    let client_order_id = ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156");
    let venue_order_id = VenueOrderId::from("exchange-order-1");
    let orders = MockOrdersService::default();
    let get_orders_calls = Arc::clone(&orders.get_orders_calls);
    *orders.get_orders_response.lock().unwrap() = Some(GetOrdersResponse {
        orders: vec![OrderState {
            order_id: venue_order_id.to_string(),
            order_request_id: client_order_id.to_string(),
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
                as i32,
            lots_requested: 2,
            lots_executed: 1,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..OrderState::default()
        }],
    });

    let order_stream_calls = Arc::new(AtomicU64::new(0));
    let streams = ReconnectingOrdersStreamService {
        order_stream_calls: Arc::clone(&order_stream_calls),
        initial_state: None,
        reopened_state: order_state_stream_response::OrderState {
            order_request_id: Some(client_order_id.to_string()),
            order_id: venue_order_id.to_string(),
            trade_order_id: venue_order_id.to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            lots_requested: 2,
            lots_executed: 2,
            ..order_state_stream_response::OrderState::default()
        },
    };
    let stops = MockStopOrdersService::default();
    let operations = MockOperationsService::default();
    *operations.portfolio_response.lock().unwrap() = Some(PortfolioResponse {
        account_id: "account-1".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    });
    operations
        .pages
        .lock()
        .unwrap()
        .push_back(GetOperationsByCursorResponse::default());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders))
            .add_service(OrdersStreamServiceServer::new(streams))
            .add_service(StopOrdersServiceServer::new(stops))
            .add_service(OperationsServiceServer::new(operations))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(sender);
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 10,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::RegularOrder,
        client_order_id.as_str(),
        venue_order_id.as_str(),
    );
    nautilus_common::clients::ExecutionClient::start(&mut client).unwrap();
    client.runtime.connect().await.unwrap();

    let reports = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut reports = Vec::new();
        while reports.len() < 2 {
            if let Some(ExecutionEvent::Report(ExecutionReport::Order(report))) =
                receiver.recv().await
                && report.client_order_id == Some(client_order_id)
            {
                reports.push(*report);
            }
        }
        reports
    })
    .await
    .expect("timed out waiting for reconciled and reopened-stream reports");

    assert_eq!(reports[0].order_status, OrderStatus::PartiallyFilled);
    assert_eq!(reports[1].order_status, OrderStatus::Filled);
    assert!(get_orders_calls.load(Ordering::SeqCst) >= 1);
    assert!(order_stream_calls.load(Ordering::SeqCst) >= 2);
    nautilus_common::clients::ExecutionClient::disconnect(&mut client)
        .await
        .unwrap();
}

#[tokio::test]
async fn malformed_order_stream_event_is_rejected_without_reconnect() {
    let venue_order_id = VenueOrderId::from("late-exchange-order-1");
    let late_instrument_uid = "late-instrument-uid";
    let orders = MockOrdersService::default();
    let get_orders_calls = Arc::clone(&orders.get_orders_calls);
    let state_calls = Arc::clone(&orders.state_calls);
    *orders.get_orders_response.lock().unwrap() = Some(GetOrdersResponse::default());
    *orders.state_response.lock().unwrap() = Some(OrderState {
        order_id: venue_order_id.to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 1,
        lots_executed: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: late_instrument_uid.to_string(),
        ..OrderState::default()
    });

    let lookup_started = Arc::new(AtomicBool::new(false));
    let order_stream_calls = Arc::new(AtomicU64::new(0));
    let unresolved_state = order_state_stream_response::OrderState {
        order_id: venue_order_id.to_string(),
        trade_order_id: venue_order_id.to_string(),
        account_id: "account-1".to_string(),
        instrument_uid: late_instrument_uid.to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lot_size: 10,
        lots_requested: 1,
        lots_executed: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        ..order_state_stream_response::OrderState::default()
    };
    let streams = ReconnectingOrdersStreamService {
        order_stream_calls: Arc::clone(&order_stream_calls),
        initial_state: Some(unresolved_state.clone()),
        reopened_state: unresolved_state,
    };
    let stops = MockStopOrdersService::default();
    let operations = MockOperationsService::default();
    *operations.portfolio_response.lock().unwrap() = Some(PortfolioResponse {
        account_id: "account-1".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    });
    operations
        .pages
        .lock()
        .unwrap()
        .push_back(GetOperationsByCursorResponse::default());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let lookup_started_for_server = Arc::clone(&lookup_started);
    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders))
            .add_service(OrdersStreamServiceServer::new(streams))
            .add_service(StopOrdersServiceServer::new(stops))
            .add_service(OperationsServiceServer::new(operations))
            .add_service(ShareByOnlyInstrumentsService {
                share: None,
                lookup_started: Some(lookup_started_for_server),
            })
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(sender);
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 10,
            max_backoff_ms: 10,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    nautilus_common::clients::ExecutionClient::start(&mut client).unwrap();
    client.runtime.connect().await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !lookup_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for unresolved metadata lookup");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(get_orders_calls.load(Ordering::SeqCst), 0);
    assert!(state_calls.lock().unwrap().is_empty());
    assert_eq!(order_stream_calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let mut malformed_report = false;
    while let Ok(event) = receiver.try_recv() {
        if matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::Order(report))
                if report.venue_order_id == venue_order_id
        ) {
            malformed_report = true;
            break;
        }
    }
    assert!(!malformed_report);

    nautilus_common::clients::ExecutionClient::disconnect(&mut client)
        .await
        .unwrap();
}

#[tokio::test]
async fn execution_client_emits_upstream_submitted_event_before_broker_work() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);
    let cmd = submit_order_cmd(None);
    let client_order_id = cmd.client_order_id;

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order(&client, cmd)
        .unwrap();

    let event = receiver.recv().await.unwrap();
    let ExecutionEvent::Order(OrderEventAny::Submitted(event)) = event else {
        panic!("expected OrderSubmitted");
    };
    assert_eq!(event.client_order_id, client_order_id);
    assert_eq!(event.account_id.to_string(), "account-1");
}

#[tokio::test]
async fn pre_submit_validation_failures_emit_upstream_denied_events() {
    for (cmd, expected_reason) in [
        (
            submit_order_cmd_for("BAD.MOEX", OrderType::Market, None),
            "unsupported instrument",
        ),
        (
            submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::StopLimit, None),
            "unsupported T-Bank order type",
        ),
    ] {
        let mut client = test_client(TbankExecutionClientConfig {
            account_id: Some("account-1".to_string()),
            enable_trading: true,
            allow_live_trading: true,
            ..TbankExecutionClientConfig::default()
        });
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.runtime.emitter.set_sender(sender);
        activate_test_lifecycle(&client);

        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order(
            &client, cmd,
        )
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = event else {
            panic!("expected OrderDenied");
        };
        assert!(event.reason.as_str().contains(expected_reason));
        assert!(receiver.try_recv().is_err());
    }
}

#[tokio::test]
async fn explicit_broker_submit_rejection_emits_upstream_rejected_event() {
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() = Some((
        Code::FailedPrecondition,
        "not enough buying power".to_string(),
    ));
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
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    client.connect_for_queries().await.unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order(
        &client,
        submit_order_cmd(None),
    )
    .unwrap();

    assert!(matches!(
        receiver.recv().await,
        Some(ExecutionEvent::Order(OrderEventAny::Submitted(_)))
    ));
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = event else {
        panic!("expected OrderRejected");
    };
    assert!(event.reason.as_str().contains("not enough buying power"));
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .route_for_client_order_id(event.client_order_id.as_str()),
        None
    );
    assert!(matches!(
        client
            .runtime
            .resolve_cancel_target(event.client_order_id.as_str(), None)
            .await,
        Err(TbankAdapterError::BrokerOrderIdentityUnresolved(_))
    ));
}

#[tokio::test]
async fn successful_submit_emits_submitted_then_accepted_report() {
    let service = MockOrdersService::default();
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
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    client.connect_for_queries().await.unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order(
        &client,
        submit_order_cmd(None),
    )
    .unwrap();

    assert!(matches!(
        receiver.recv().await,
        Some(ExecutionEvent::Order(OrderEventAny::Submitted(_)))
    ));
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected accepted OrderStatusReport");
    };
    assert_eq!(report.order_status, OrderStatus::Accepted);
}

#[test]
fn unsupported_modify_emits_upstream_modify_rejected_event() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("TBANK")),
        StrategyId::from("STRATEGY-001"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        ClientOrderId::from("order-1"),
        Some(VenueOrderId::from("venue-1")),
        Some(Quantity::from(2)),
        None,
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    );

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::modify_order(&client, cmd)
        .unwrap();

    let event = receiver.try_recv().unwrap();
    let ExecutionEvent::Order(OrderEventAny::ModifyRejected(event)) = event else {
        panic!("expected OrderModifyRejected");
    };
    assert!(
        event
            .reason
            .as_str()
            .contains("does not support modify_order")
    );
}

async fn cancel_event_for_broker_error(code: Code) -> Option<ExecutionEvent> {
    let service = MockOrdersService::default();
    *service.cancel_error.lock().unwrap() = Some((code, "cancel failed".to_string()));
    let cancel_calls = Arc::clone(&service.cancel_calls);
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
    client.runtime.record_broker_order_mapping(
        super::TbankBrokerOrderRoute::RegularOrder,
        "order-1",
        "venue-1",
    );
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let cmd = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("TBANK")),
        StrategyId::from("STRATEGY-001"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        ClientOrderId::from("order-1"),
        Some(VenueOrderId::from("venue-1")),
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    );
    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::cancel_order(&client, cmd)
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while cancel_calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn explicit_broker_cancel_rejection_emits_cancel_rejected() {
    let event = cancel_event_for_broker_error(Code::NotFound)
        .await
        .expect("expected cancel rejection event");
    assert!(matches!(
        event,
        ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
    ));
}

#[tokio::test]
async fn unresolved_broker_identity_emits_cancel_rejected() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    activate_test_lifecycle(&client);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let cmd = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("TBANK")),
        StrategyId::from("STRATEGY-001"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        ClientOrderId::from("client-only-order"),
        None,
        UUID4::new(),
        UnixNanos::from(1),
        None,
        None,
    );

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::cancel_order(
        &client, cmd,
    )
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .expect("expected cancel rejection event");
    assert!(matches!(
        event,
        ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
    ));
}

#[tokio::test]
async fn ambiguous_cancel_failure_does_not_emit_cancel_rejected() {
    assert!(
        cancel_event_for_broker_error(Code::Unavailable)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn exhausted_single_cancel_recovery_blocks_reset_until_known_outcome() {
    let service = MockOrdersService::default();
    *service.cancel_error.lock().unwrap() = Some((Code::Unavailable, "ambiguous".to_string()));
    let cancel_error = Arc::clone(&service.cancel_error);
    let cancel_calls = Arc::clone(&service.cancel_calls);
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
        reconnect_policy: TbankReconnectPolicy {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    client.connect_for_queries().await.unwrap();
    let identity = TbankBrokerOrderIdentity {
        route: TbankBrokerOrderRoute::RegularOrder,
        broker_order_id: "venue-1".to_string(),
    };

    let initial = client
        .runtime
        .cancel_resolved_broker_order(identity.clone())
        .await
        .unwrap_err();
    assert_eq!(
        classify_cancel_failure(&initial),
        CancelFailureKind::OutcomeUnknown
    );
    assert!(
        client
            .runtime
            .recover_ambiguous_cancel(identity.clone())
            .await
            .is_err()
    );
    assert_eq!(
        cancel_calls.lock().unwrap().len(),
        1 + CANCEL_OUTCOME_RECOVERY_ATTEMPTS as usize
    );
    assert!(client.runtime.cancellation_is_unresolved(&identity));
    assert!(ExecutionClient::reset(&mut client).is_err());

    *cancel_error.lock().unwrap() = Some((Code::Unauthenticated, "auth failed".to_string()));
    client.connect_for_queries().await.unwrap();
    assert!(
        client
            .runtime
            .cancel_resolved_broker_order(identity.clone())
            .await
            .is_err()
    );
    assert!(!client.runtime.cancellation_is_unresolved(&identity));
    ExecutionClient::reset(&mut client).unwrap();
}

#[tokio::test]
async fn terminal_cancel_retry_reconciles_cancelled_state_and_clears_unknown_outcome() {
    let service = MockOrdersService::default();
    *service.cancel_error.lock().unwrap() = Some((Code::Unavailable, "ambiguous".to_string()));
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "venue-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusCancelled as i32,
        ..OrderState::default()
    });
    let cancel_error = Arc::clone(&service.cancel_error);
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
        reconnect_policy: TbankReconnectPolicy {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    client.connect_for_queries().await.unwrap();
    let identity = TbankBrokerOrderIdentity {
        route: TbankBrokerOrderRoute::RegularOrder,
        broker_order_id: "venue-1".to_string(),
    };

    let initial = client
        .runtime
        .cancel_resolved_broker_order(identity.clone())
        .await
        .unwrap_err();
    assert_eq!(
        classify_cancel_failure(&initial),
        CancelFailureKind::OutcomeUnknown
    );
    *cancel_error.lock().unwrap() = Some((Code::NotFound, "already gone".to_string()));

    client
        .runtime
        .recover_ambiguous_cancel(identity.clone())
        .await
        .unwrap();

    assert!(!client.runtime.cancellation_is_unresolved(&identity));
    ExecutionClient::reset(&mut client).unwrap();
}

fn submit_order_list_cmd(order_inits: Vec<OrderInitialized>) -> SubmitOrderList {
    let strategy_id = StrategyId::from("STRATEGY-001");
    let instrument_id = order_inits[0].instrument_id;
    let order_list = OrderList::new(
        OrderListId::from("ORDER-LIST-001"),
        instrument_id,
        strategy_id,
        order_inits
            .iter()
            .map(|order| order.client_order_id)
            .collect(),
        UnixNanos::from(1),
    );
    let mut cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("TBANK")),
        strategy_id,
        order_list,
        order_inits,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    cmd.causation_id = Some(UUID4::new());
    cmd
}

#[test]
fn submit_order_list_preserves_trace_context_for_every_leg() {
    let first = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, None).order_init;
    let mut second = first.clone();
    second.client_order_id = ClientOrderId::from("order-2");
    let list = submit_order_list_cmd(vec![first, second]);
    let correlation_id = list.correlation_id;
    let causation_id = list.causation_id;

    let commands = super::nautilus::submit_commands_from_list(list);

    assert_eq!(commands.len(), 2);
    assert!(
        commands
            .iter()
            .all(|command| command.correlation_id == correlation_id)
    );
    assert!(
        commands
            .iter()
            .all(|command| command.causation_id == causation_id)
    );
    assert_ne!(commands[0].command_id, commands[1].command_id);
}

#[test]
fn submit_order_list_rejects_invalid_leg_without_partial_dispatch() {
    let mut invalid = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, None).order_init;
    invalid.order_type = OrderType::Limit;
    invalid.price = None;
    let mut valid = invalid.clone();
    valid.client_order_id = ClientOrderId::from("order-2");
    valid.order_type = OrderType::Market;
    let list = submit_order_list_cmd(vec![invalid, valid]);
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);

    let error =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order_list(
            &client, list,
        )
        .unwrap_err();

    assert!(error.to_string().contains("price"));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn submit_order_list_denies_every_leg_when_preflight_fails() {
    let valid = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, None).order_init;
    let mut invalid = valid.clone();
    invalid.client_order_id = ClientOrderId::from("order-2");
    invalid.instrument_id = InstrumentId::from("BAD.MOEX");
    let list = submit_order_list_cmd(vec![valid, invalid]);
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order_list(
        &client, list,
    )
    .unwrap();

    for _ in 0..2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            ExecutionEvent::Order(OrderEventAny::Denied(_))
        ));
    }
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn submit_order_list_denies_contingent_orders_as_a_whole() {
    let mut first = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, None).order_init;
    let mut second = first.clone();
    second.client_order_id = ClientOrderId::from("order-2");
    first.contingency_type = Some(ContingencyType::Oco);
    first.linked_order_ids = Some(vec![second.client_order_id]);
    second.contingency_type = Some(ContingencyType::Oco);
    second.linked_order_ids = Some(vec![first.client_order_id]);
    let list = submit_order_list_cmd(vec![first, second]);
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order_list(
        &client, list,
    )
    .unwrap();

    for _ in 0..2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = event else {
            panic!("expected OrderDenied");
        };
        assert!(event.reason.as_str().contains("contingent order lists"));
    }
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn submit_order_list_accepts_explicit_no_contingency_orders() {
    let mut first = submit_order_cmd_for("SBER_TQBR.MOEX", OrderType::Market, None).order_init;
    let mut second = first.clone();
    second.client_order_id = ClientOrderId::from("order-2");
    first.contingency_type = Some(ContingencyType::NoContingency);
    second.contingency_type = Some(ContingencyType::NoContingency);
    let list = submit_order_list_cmd(vec![first, second]);
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order_list(
        &client, list,
    )
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        event,
        ExecutionEvent::Order(OrderEventAny::Submitted(_))
    ));
}

#[tokio::test]
async fn submit_uses_order_initialized_instrument_when_command_instrument_diverges() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());

    let mut cmd = submit_order_cmd(None);
    cmd.instrument_id = InstrumentId::from("SBER_TQBR.moex_mrng_evng_e_wknd_dlr");

    let error = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap_err();

    let reason = error.to_string();
    assert!(reason.contains("trading is disabled"));
    assert!(!reason.contains("unsupported instrument"));
}

#[test]
fn submit_instrument_scope_covers_spbe_shares_and_moex_futures() {
    assert!(super::is_supported_tbank_submit_instrument(
        &"AAPL_SPBXM.SPBE".parse().unwrap()
    ));
    assert!(super::is_supported_tbank_submit_instrument(
        &"Si-9.26_SPBFUT.MOEX".parse().unwrap()
    ));
}

#[test]
fn submit_prefers_command_instrument_for_supported_multi_venue_ids() {
    let mut cmd = submit_order_cmd_for("AAPL_SPBXM.SPBE", OrderType::Market, None);
    cmd.order_init.instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
    assert_eq!(
        super::order_initialized_instrument_id(&cmd).to_string(),
        "AAPL_SPBXM.SPBE"
    );

    let mut cmd = submit_order_cmd_for("Si-9.26_SPBFUT.MOEX", OrderType::Market, None);
    cmd.order_init.instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
    assert_eq!(
        super::order_initialized_instrument_id(&cmd).to_string(),
        "Si-9.26_SPBFUT.MOEX"
    );
}

#[tokio::test]
async fn submit_uses_command_instrument_when_order_initialized_is_malformed_position_id() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.instruments.lock().unwrap().insert(
        "MAGN_TQBR.MOEX".to_string(),
        crate::instruments::TbankInstrumentMetadata {
            instrument_id: "MAGN_TQBR.MOEX".to_string(),
            ticker: "MAGN".to_string(),
            figi: "BBG004S685M3".to_string(),
            instrument_uid: "magn-uid".to_string(),
            position_uid: "magn-position-uid".to_string(),
            ..sber_metadata()
        },
    );

    let mut cmd = submit_order_cmd_for("MAGN_TQBR.MOEX", OrderType::Market, None);
    cmd.order_init.instrument_id = InstrumentId::from("MAGN_TQBR.moex_mrng_evng_e_wknd_dlr");

    let error = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap_err();

    let reason = error.to_string();
    assert!(reason.contains("trading is disabled"));
    assert!(!reason.contains("unsupported instrument"));
}

#[tokio::test]
async fn trading_disabled_submit_emits_upstream_denied_event() {
    let mut client = test_client(TbankExecutionClientConfig {
        account_id: Some("account-1".to_string()),
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    activate_test_lifecycle(&client);

    let cmd = submit_order_cmd(None);
    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::submit_order(&client, cmd)
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Order(OrderEventAny::Denied(event)) = event else {
        panic!("expected OrderDenied");
    };
    assert!(event.reason.as_str().contains("trading is disabled"));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn submit_broker_permission_denied_returns_rejection_without_reconciliation() {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.post_error.lock().unwrap() =
        Some((Code::PermissionDenied, "not enough permissions".to_string()));
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
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    client.connect_for_queries().await.unwrap();

    let cmd = submit_order_cmd(None);
    let error = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("permission denied"));
    assert!(state_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn submit_response_without_order_id_is_outcome_unknown() {
    let service = MockOrdersService::default();
    *service.post_response.lock().unwrap() = Some(PostOrderResponse::default());
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
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.runtime.connect_for_queries().await.unwrap();

    let prepared = super::prepare_nautilus_order(&mut client.runtime, submit_order_cmd(None))
        .await
        .unwrap();
    let client_order_id = prepared.order.client_order_id.clone();
    let error = client
        .runtime
        .submit_order(&prepared.order, &prepared.metadata)
        .await
        .expect_err("missing order_id must fail closed");
    assert!(error.to_string().contains("missing order_id"));
    assert_eq!(
        client
            .runtime
            .pending_submits
            .lock()
            .unwrap()
            .get(client_order_id.as_str())
            .map(|pending| pending.stage),
        Some(TbankPendingSubmitStage::Unknown)
    );
}

#[tokio::test]
async fn submit_response_partial_fill_returns_fallback_reports_without_polling() {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.post_response.lock().unwrap() = Some(PostOrderResponse {
        order_id: "exchange-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 2,
        lots_executed: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 275,
            nano: 0,
        }),
        executed_commission: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 3,
            nano: 500_000_000,
        }),
        ..PostOrderResponse::default()
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
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.connect_for_queries().await.unwrap();

    let cmd = submit_order_cmd(None);
    let reports = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();

    let (order_report, fill_reports) = single_order_with_fills_report(&reports);
    assert_eq!(order_report.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(order_report.venue_order_id.to_string(), "exchange-order-1");
    assert_eq!(order_report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(order_report.filled_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fill_reports.len(), 1);
    assert_eq!(fill_reports[0].last_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fill_reports[0].commission.as_decimal(), Decimal::new(35, 1));
    assert!(state_calls.lock().unwrap().is_empty());
    let pending = client.runtime.pending_submits.lock().unwrap();
    let pending = pending.get("524b1a03-efdd-4cd0-bd56-7cc6570c7156").unwrap();
    assert_eq!(pending.stage, TbankPendingSubmitStage::Filled);
    assert_eq!(pending.venue_order_id.as_deref(), Some("exchange-order-1"));
}

// ---------------------------------------------------------------------------
// Failure classification and pending-submit lifecycle unit tests.
// ---------------------------------------------------------------------------

#[test]
fn submit_failure_classification_covers_local_broker_and_unknown_outcomes() {
    use super::{SubmitFailureKind, classify_submit_failure};

    // Local validation and configuration failures reject without reconciliation.
    for error in [
        TbankAdapterError::ConfigError("bad endpoint".to_string()),
        TbankAdapterError::MissingToken,
        TbankAdapterError::MissingAccountId,
        TbankAdapterError::InvalidEndpoint,
        TbankAdapterError::UnsupportedInstrument("bond".to_string()),
        TbankAdapterError::InstrumentOutOfScope("SBER".to_string()),
        TbankAdapterError::UnsupportedOrderType("StopLimit".to_string()),
        TbankAdapterError::UnsupportedTimeInForce("Fok".to_string()),
        TbankAdapterError::InvalidQuantity("negative".to_string()),
        TbankAdapterError::InvalidPrice("nan".to_string()),
        TbankAdapterError::QuantityNotMultipleOfLot {
            quantity: "15".to_string(),
            lot: 10,
        },
        TbankAdapterError::PriceNotMultipleOfTick {
            price: "0.15".to_string(),
            tick: "0.01".to_string(),
        },
        TbankAdapterError::ConversionError("rounding".to_string()),
        TbankAdapterError::InvalidInstrumentIdentity("uid".to_string()),
        TbankAdapterError::BrokerOrderIdentityUnresolved("id".to_string()),
    ] {
        assert_eq!(
            classify_submit_failure(&error),
            SubmitFailureKind::LocalRejected,
            "local error {error:?}"
        );
    }

    // Broker rejections are terminal and must not schedule reconciliation.
    for error in [
        TbankAdapterError::PermissionDenied("no access".to_string()),
        TbankAdapterError::RateLimited("slow down".to_string()),
    ] {
        assert_eq!(
            classify_submit_failure(&error),
            SubmitFailureKind::BrokerRejected,
            "broker rejection {error:?}"
        );
    }

    // Outcome-unknown failures must trigger reconciliation, never a rejection.
    for error in [
        TbankAdapterError::InstrumentNotFound("SBER".to_string()),
        TbankAdapterError::InstrumentMetadataUnresolved("SBER".to_string()),
        TbankAdapterError::FuturesMarginUnresolved("Si".to_string()),
        TbankAdapterError::SubmitOutcomeUnknown("timeout".to_string()),
        TbankAdapterError::ReconnectFailed("transport".to_string()),
    ] {
        assert_eq!(
            classify_submit_failure(&error),
            SubmitFailureKind::OutcomeUnknown,
            "outcome-unknown {error:?}"
        );
    }
}

#[test]
fn submit_grpc_status_classification_matches_broker_semantics() {
    use super::{SubmitFailureKind, classify_submit_grpc_status};

    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::FailedPrecondition,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::Unauthenticated,
    ] {
        assert_eq!(
            classify_submit_grpc_status(code),
            SubmitFailureKind::BrokerRejected,
            "code {code:?}"
        );
    }

    for code in [
        Code::Cancelled,
        Code::Unknown,
        Code::DeadlineExceeded,
        Code::Aborted,
        Code::Internal,
        Code::DataLoss,
        Code::Unavailable,
    ] {
        assert_eq!(
            classify_submit_grpc_status(code),
            SubmitFailureKind::OutcomeUnknown,
            "code {code:?}"
        );
    }

    assert_eq!(classify_submit_grpc_status(Code::Ok), SubmitFailureKind::BrokerRejected);
}

#[test]
fn classify_cancel_failure_distinguishes_rejected_from_unknown() {
    use super::{CancelFailureKind, classify_cancel_failure};

    assert_eq!(
        classify_cancel_failure(&TbankAdapterError::PermissionDenied("no".to_string())),
        CancelFailureKind::BrokerRejected
    );
    assert_eq!(
        classify_cancel_failure(&TbankAdapterError::RateLimited("slow".to_string())),
        CancelFailureKind::OutcomeUnknown
    );
    assert_eq!(
        classify_cancel_failure(&TbankAdapterError::InstrumentMetadataUnresolved("x".to_string())),
        CancelFailureKind::OutcomeUnknown
    );
    assert_eq!(
        classify_cancel_failure(&TbankAdapterError::UnsupportedOrderType("x".to_string())),
        CancelFailureKind::LocalFailure
    );
}

#[test]
fn pending_stage_after_submit_response_marks_rejected_orders_terminal() {
    use super::{TbankPendingSubmitStage, pending_stage_after_submit_response};

    let rejected = pending_stage_after_submit_response(&TbankSubmitResponse::Order(PostOrderResponse {
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusRejected as i32,
        ..PostOrderResponse::default()
    }));
    assert_eq!(rejected, TbankPendingSubmitStage::Rejected);

    let accepted = pending_stage_after_submit_response(&TbankSubmitResponse::Order(PostOrderResponse {
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        ..PostOrderResponse::default()
    }));
    assert_eq!(accepted, TbankPendingSubmitStage::Submitted);

    let stop = pending_stage_after_submit_response(&TbankSubmitResponse::StopOrder(
        PostStopOrderResponse {
            stop_order_id: "stop-1".to_string(),
            ..PostStopOrderResponse::default()
        },
    ));
    assert_eq!(stop, TbankPendingSubmitStage::Submitted);
}

#[test]
fn pending_stage_from_order_status_maps_lifecycle_terminal_states() {
    use super::{TbankPendingSubmitStage, pending_stage_from_order_status};

    assert_eq!(pending_stage_from_order_status(OrderStatus::Accepted), Some(TbankPendingSubmitStage::Accepted));
    assert_eq!(pending_stage_from_order_status(OrderStatus::PartiallyFilled), Some(TbankPendingSubmitStage::Filled));
    assert_eq!(pending_stage_from_order_status(OrderStatus::Filled), Some(TbankPendingSubmitStage::Filled));
    assert_eq!(pending_stage_from_order_status(OrderStatus::Rejected), Some(TbankPendingSubmitStage::Rejected));
    assert_eq!(pending_stage_from_order_status(OrderStatus::Canceled), Some(TbankPendingSubmitStage::Cancelled));
    assert_eq!(pending_stage_from_order_status(OrderStatus::Expired), Some(TbankPendingSubmitStage::Cancelled));
    assert_eq!(pending_stage_from_order_status(OrderStatus::Initialized), None);
    assert_eq!(pending_stage_from_order_status(OrderStatus::Triggered), None);
}

#[test]
fn update_pending_submit_binds_venue_order_id_and_timestamp() {
    use super::{TbankPendingSubmitStage, update_pending_submit};

    let ts = UnixNanos::from(200_u64);
    let mut pending = TbankPendingSubmit {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        submitted_ts: UnixNanos::from(100_u64),
        quantity_units: Decimal::from(20),
        side: TbankOrderSide::Buy,
        order_type: TbankOrderType::Limit,
        time_in_force: TimeInForce::Day,
        trailing: None,
        venue_order_id: None,
        last_reconciliation_ts: None,
        stage: TbankPendingSubmitStage::Submitted,
    };

    update_pending_submit(&mut pending, TbankPendingSubmitStage::Accepted, Some("venue-1".to_string()), ts);
    assert_eq!(pending.stage, TbankPendingSubmitStage::Accepted);
    assert_eq!(pending.venue_order_id.as_deref(), Some("venue-1"));
    assert_eq!(pending.last_reconciliation_ts, Some(ts));

    // A subsequent update without a venue order id keeps the bound identity.
    update_pending_submit(&mut pending, TbankPendingSubmitStage::Filled, None, ts);
    assert_eq!(pending.stage, TbankPendingSubmitStage::Filled);
    assert_eq!(pending.venue_order_id.as_deref(), Some("venue-1"));
}

#[test]
fn mark_pending_submit_order_report_only_touches_matching_client_order() {
    use super::{TbankPendingSubmitStage, mark_pending_submit_order_report};

    let ts = UnixNanos::from(200_u64);
    let pending_submits: Arc<Mutex<HashMap<String, TbankPendingSubmit>>> =
        Arc::new(Mutex::new(HashMap::from([(
            "client-1".to_string(),
            TbankPendingSubmit {
                instrument_id: "SBER_TQBR.MOEX".to_string(),
                submitted_ts: UnixNanos::from(100_u64),
                quantity_units: Decimal::from(20),
                side: TbankOrderSide::Buy,
                order_type: TbankOrderType::Limit,
                time_in_force: TimeInForce::Day,
                trailing: None,
                venue_order_id: None,
                last_reconciliation_ts: None,
                stage: TbankPendingSubmitStage::Submitted,
            },
        )])));

    let report = OrderStatusReport::new(
        "TBANK-001".into(),
        InstrumentId::from("SBER_TQBR.MOEX"),
        Some(ClientOrderId::from("client-1")),
        VenueOrderId::from("venue-1"),
        OrderSide::Buy,
        OrderType::Limit,
        TimeInForce::Day,
        OrderStatus::Accepted,
        Quantity::from(20),
        Quantity::from(0),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    );
    mark_pending_submit_order_report(&pending_submits, &report);
    let pending = pending_submits.lock().unwrap();
    assert_eq!(pending.get("client-1").unwrap().stage, TbankPendingSubmitStage::Accepted);
    assert_eq!(
        pending.get("client-1").unwrap().venue_order_id.as_deref(),
        Some("venue-1")
    );

    // An unlinked report leaves the pending submit untouched.
    let unlinked = OrderStatusReport::new(
        "TBANK-001".into(),
        InstrumentId::from("SBER_TQBR.MOEX"),
        None,
        VenueOrderId::from("venue-2"),
        OrderSide::Buy,
        OrderType::Limit,
        TimeInForce::Day,
        OrderStatus::Filled,
        Quantity::from(20),
        Quantity::from(20),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    );
    mark_pending_submit_order_report(&pending_submits, &unlinked);
    assert_eq!(pending.get("client-1").unwrap().stage, TbankPendingSubmitStage::Accepted);
}

#[test]
fn mark_pending_submit_fill_report_matches_by_client_then_venue_order_id() {
    use super::{TbankPendingSubmitStage, mark_pending_submit_fill_report};

    let ts = UnixNanos::from(200_u64);
    let pending_submits: Arc<Mutex<HashMap<String, TbankPendingSubmit>>> =
        Arc::new(Mutex::new(HashMap::from([(
            "client-1".to_string(),
            TbankPendingSubmit {
                instrument_id: "SBER_TQBR.MOEX".to_string(),
                submitted_ts: UnixNanos::from(100_u64),
                quantity_units: Decimal::from(20),
                side: TbankOrderSide::Buy,
                order_type: TbankOrderType::Limit,
                time_in_force: TimeInForce::Day,
                trailing: None,
                venue_order_id: Some("venue-1".to_string()),
                last_reconciliation_ts: None,
                stage: TbankPendingSubmitStage::Accepted,
            },
        )])));

    let fill = FillReport::new(
        "TBANK-001".into(),
        InstrumentId::from("SBER_TQBR.MOEX"),
        VenueOrderId::from("venue-1"),
        TradeId::from("trade-1"),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("100.00"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        Some(ClientOrderId::from("client-1")),
        None,
        ts,
        ts,
        Some(UUID4::new()),
    );
    mark_pending_submit_fill_report(&pending_submits, &fill);
    assert_eq!(
        pending_submits.lock().unwrap().get("client-1").unwrap().stage,
        TbankPendingSubmitStage::Filled
    );

    // A fill without a client order id still settles via the venue order id.
    let anonymous = FillReport::new(
        "TBANK-001".into(),
        InstrumentId::from("SBER_TQBR.MOEX"),
        VenueOrderId::from("venue-1"),
        TradeId::from("trade-2"),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("100.00"),
        Money::from_decimal(Decimal::ZERO, Currency::from("RUB")).unwrap(),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        ts,
        ts,
        Some(UUID4::new()),
    );
    mark_pending_submit_fill_report(&pending_submits, &anonymous);
    assert_eq!(
        pending_submits.lock().unwrap().get("client-1").unwrap().stage,
        TbankPendingSubmitStage::Filled
    );
}

#[test]
fn stop_direction_and_type_mapping_covers_submit_side_contracts() {
    use super::{tbank_order_type_from_stop_order, tbank_side_from_stop_direction};

    assert_eq!(tbank_side_from_stop_direction(StopOrderDirection::Buy as i32), Some(TbankOrderSide::Buy));
    assert_eq!(tbank_side_from_stop_direction(StopOrderDirection::Sell as i32), Some(TbankOrderSide::Sell));
    assert_eq!(tbank_side_from_stop_direction(9999), None);

    // Trailing take-profit stops keep their trailing identity.
    let trailing_limit = StopOrder {
        take_profit_type: TakeProfitType::Trailing as i32,
        exchange_order_type: ExchangeOrderType::Limit as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        tbank_order_type_from_stop_order(&trailing_limit),
        Some(TbankOrderType::TrailingStopLimit)
    );

    let trailing_market = StopOrder {
        take_profit_type: TakeProfitType::Trailing as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        tbank_order_type_from_stop_order(&trailing_market),
        Some(TbankOrderType::TrailingStopMarket)
    );

    let stop_loss = StopOrder {
        order_type: StopOrderType::StopLoss as i32,
        ..StopOrder::default()
    };
    assert_eq!(tbank_order_type_from_stop_order(&stop_loss), Some(TbankOrderType::StopMarket));

    let take_profit_limit = StopOrder {
        order_type: StopOrderType::TakeProfit as i32,
        exchange_order_type: ExchangeOrderType::Limit as i32,
        ..StopOrder::default()
    };
    assert_eq!(tbank_order_type_from_stop_order(&take_profit_limit), None);

    let take_profit_market = StopOrder {
        order_type: StopOrderType::TakeProfit as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        tbank_order_type_from_stop_order(&take_profit_market),
        Some(TbankOrderType::MarketIfTouched)
    );

    let stop_limit = StopOrder {
        order_type: StopOrderType::StopLimit as i32,
        ..StopOrder::default()
    };
    assert_eq!(tbank_order_type_from_stop_order(&stop_limit), None);
}

#[test]
fn post_stop_order_response_builds_accepted_report_with_stop_identity() {
    use super::order_status_report_from_post_stop_order_response;

    let cmd = submit_stop_order_cmd();
    let ts_init = current_unix_nanos();
    let report = order_status_report_from_post_stop_order_response(
        "TBANK-001".into(),
        &cmd,
        &PostStopOrderResponse {
            stop_order_id: "stop-1".to_string(),
            order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
            ..PostStopOrderResponse::default()
        },
        ts_init,
    );

    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.venue_order_id.to_string(), "stop-1");
    assert_eq!(report.client_order_id.map(|id| id.to_string()).as_deref(), Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"));
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.trigger_price, Some(Price::from("270.00")));
    assert_eq!(report.trigger_type, Some(TriggerType::Default));
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(report.filled_qty.as_decimal(), Decimal::ZERO);
}

#[test]
fn broker_order_route_for_submit_selects_stop_service_by_environment_and_type() {
    use super::broker_order_route_for_submit;
    use crate::execution::TbankExecutionService;

    let base = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: "client-1".to_string(),
        broker_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        side: TbankOrderSide::Buy,
        order_type: TbankOrderType::Market,
        time_in_force: TimeInForce::Ioc,
        quantity_units: Decimal::from(20),
        limit_price: None,
        trigger_price: None,
        trailing: None,
        confirm_margin_trade: false,
    };
    assert_eq!(
        broker_order_route_for_submit(&base, TbankExecutionService::LiveOrders),
        TbankBrokerOrderRoute::RegularOrder
    );
    assert_eq!(
        broker_order_route_for_submit(&base, TbankExecutionService::Sandbox),
        TbankBrokerOrderRoute::RegularOrder
    );

    let mut stop = base.clone();
    stop.order_type = TbankOrderType::StopMarket;
    assert_eq!(
        broker_order_route_for_submit(&stop, TbankExecutionService::LiveStopOrders),
        TbankBrokerOrderRoute::StopOrder
    );
    assert_eq!(
        broker_order_route_for_submit(&stop, TbankExecutionService::Sandbox),
        TbankBrokerOrderRoute::StopOrder
    );

    let mut trailing = base.clone();
    trailing.order_type = TbankOrderType::TrailingStopLimit;
    assert_eq!(
        broker_order_route_for_submit(&trailing, TbankExecutionService::Sandbox),
        TbankBrokerOrderRoute::StopOrder
    );
}

#[test]
fn stop_order_matches_submit_requires_exact_identity_and_prices() {
    use super::stop_order_matches_submit;

    let mut metadata = sber_metadata();
    metadata.lot = 10;

    let order = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: "client-1".to_string(),
        broker_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        side: TbankOrderSide::Sell,
        order_type: TbankOrderType::StopMarket,
        time_in_force: TimeInForce::Gtc,
        quantity_units: Decimal::from(20),
        limit_price: None,
        trigger_price: Some(Decimal::from(270)),
        trailing: None,
        confirm_margin_trade: false,
    };

    let matching_stop = StopOrder {
        stop_order_id: "stop-1".to_string(),
        lots_requested: 2,
        direction: StopOrderDirection::Sell as i32,
        order_type: StopOrderType::StopLoss as i32,
        instrument_uid: metadata.instrument_uid.clone(),
        ticker: metadata.ticker.clone(),
        class_code: metadata.class_code.clone(),
        stop_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 270,
            nano: 0,
        }),
        exchange_order_type: ExchangeOrderType::Market as i32,
        ..StopOrder::default()
    };
    assert!(stop_order_matches_submit(&order, &metadata, &matching_stop));

    // A mismatched trigger price must not be attributed to this submit.
    let wrong_price = StopOrder {
        stop_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 271,
            nano: 0,
        }),
        ..matching_stop.clone()
    };
    assert!(!stop_order_matches_submit(&order, &metadata, &wrong_price));

    // A mismatched instrument identity must not be attributed to this submit.
    let wrong_instrument = StopOrder {
        instrument_uid: "other-uid".to_string(),
        ..matching_stop.clone()
    };
    assert!(!stop_order_matches_submit(&order, &metadata, &wrong_instrument));

    // A mismatched direction must not be attributed to this submit.
    let wrong_direction = StopOrder {
        direction: StopOrderDirection::Buy as i32,
        ..matching_stop
    };
    assert!(!stop_order_matches_submit(&order, &metadata, &wrong_direction));
}
