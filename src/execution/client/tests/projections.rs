#[test]
fn order_state_stream_regular_status_maps_without_polling() {
    let report = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_request_id: Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string()),
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
                as i32,
            lots_requested: 2,
            lots_executed: 1,
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();

    assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "exchange-order-1");
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(10));
    assert_eq!(report.time_in_force, TimeInForce::Ioc);
}
#[test]
fn order_status_projection_blocks_buffered_reconnect_regression() {
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let accepted = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            lots_requested: 2,
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();
    let filled = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            lots_requested: 2,
            lots_executed: 2,
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            completion_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();

    assert!(project_order_status_report(&projection, accepted.clone()).is_some());
    assert!(project_order_status_report(&projection, filled).is_some());
    assert!(project_order_status_report(&projection, accepted).is_none());
}

#[test]
fn order_status_projection_blocks_triggered_to_accepted_regression() {
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let ts_init = current_unix_nanos();
    let mut accepted_stop = active_sber_stop_order("stop-order-1");
    accepted_stop.status = StopOrderStatusOption::StopOrderStatusActive as i32;
    let accepted =
        super::stop_order_status_report("TBANK-001".into(), accepted_stop, ts_init, 10).unwrap();
    let mut triggered_stop = active_sber_stop_order("stop-order-1");
    triggered_stop.status = StopOrderStatusOption::StopOrderStatusExecuted as i32;
    let triggered =
        super::stop_order_status_report("TBANK-001".into(), triggered_stop, ts_init, 10).unwrap();

    assert!(project_order_status_report(&projection, accepted.clone()).is_some());
    assert!(project_order_status_report(&projection, triggered).is_some());
    assert!(project_order_status_report(&projection, accepted).is_none());
}

#[test]
fn order_status_projection_accepts_later_terminal_fill_increase() {
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let first = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusCancelled
                as i32,
            lots_requested: 3,
            lots_executed: 1,
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            completion_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();
    let mut later = first.clone();
    later.filled_qty = Quantity::from(20);
    later.ts_last = UnixNanos::from(1_700_000_002_000_000_000_u64);

    assert_eq!(first.order_status, OrderStatus::Canceled);
    assert!(project_order_status_report(&projection, first).is_some());
    assert!(project_order_status_report(&projection, later.clone()).is_some());
    assert!(project_order_status_report(&projection, later).is_none());
}

#[test]
fn order_status_projection_accepts_progress_with_older_source_timestamp() {
    let projection = Arc::new(Mutex::new(HashMap::new()));
    let mut accepted = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            lots_requested: 2,
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();
    accepted.ts_last = UnixNanos::from(1_700_000_002_000_000_000_u64);
    let mut partial = accepted.clone();
    partial.order_status = OrderStatus::PartiallyFilled;
    partial.filled_qty = Quantity::from(10);
    partial.ts_last = UnixNanos::from(1_700_000_000_000_000_000_u64);

    assert!(project_order_status_report(&projection, accepted.clone()).is_some());
    let projected = project_order_status_report(&projection, partial).unwrap();
    assert_eq!(projected.ts_last, accepted.ts_last);
    assert_eq!(
        projection
            .lock()
            .unwrap()
            .get("exchange-order-1")
            .unwrap()
            .ts_last,
        accepted.ts_last
    );
}

#[test]
fn order_state_stream_partial_fill_with_cancelled_remainder_is_terminal() {
    let report = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_request_id: Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string()),
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Limit as i32,
            time_in_force: TbankTimeInForceType::TimeInForceDay as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
                as i32,
            lots_requested: 2,
            lots_executed: 1,
            lots_left: 0,
            lots_cancelled: 1,
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            completion_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        current_unix_nanos(),
        None,
        None,
    )
    .unwrap();

    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert_eq!(report.time_in_force, TimeInForce::Day);
    assert_eq!(report.ts_last.as_u64(), 1_700_000_001_000_000_000);
}

#[test]
fn initial_order_state_ack_cannot_promote_internal_order_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index
        .lock()
        .unwrap()
        .record_client_order_route(
            TbankBrokerOrderRoute::RegularOrder,
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        );

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        "internal-uuid-like-id",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
    );

    assert!(venue_order_id.is_none());
    let index = broker_order_index.lock().unwrap();
    assert_eq!(
        index.identity_for(Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: None,
        })
    );
    assert!(
        index
            .identity_for(None, Some("internal-uuid-like-id"))
            .is_none()
    );
}

#[test]
fn delayed_initial_ack_keeps_confirmed_exchange_order_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    {
        let mut index = broker_order_index.lock().unwrap();
        index
            .get_or_allocate_request_mapping(
                "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
                Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
            )
            .unwrap();
        index.record_mapping(
            TbankBrokerOrderRoute::RegularOrder,
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            "exchange-order-1",
        );
    }

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        "internal-uuid-like-id",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
    );

    assert_eq!(venue_order_id.as_deref(), Some("exchange-order-1"));
    assert!(
        broker_order_index
            .lock()
            .unwrap()
            .identity_for(None, Some("internal-uuid-like-id"))
            .is_none()
    );
}

#[test]
fn unindexed_new_order_does_not_promote_internal_broker_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        Some("external-request-id"),
        "exchange-order-1",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
    );

    assert!(venue_order_id.is_none());
}

#[test]
fn order_state_stream_mapping_uses_exchange_order_id_not_trade_order_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index
        .lock()
        .unwrap()
        .get_or_allocate_request_mapping(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        )
        .unwrap();

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        "exchange-order-1",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
    );

    assert_eq!(venue_order_id.as_deref(), Some("exchange-order-1"));

    let index = broker_order_index.lock().unwrap();
    assert_eq!(
        index.identity_for(Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("exchange-order-1".to_string()),
        })
    );
    assert!(index.identity_for(None, Some("trade-order-1")).is_none());
}

#[test]
fn reissued_regular_order_keeps_canonical_identity_and_current_cancel_route() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::RegularOrder,
        "client-order-1",
        "trade-order-1",
    );

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &fill_projection,
        None,
        "reissued-order-2",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
    );

    assert_eq!(venue_order_id.as_deref(), Some("trade-order-1"));
    let index = broker_order_index.lock().unwrap();
    assert_eq!(
        index.identity_for(Some("client-order-1"), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("reissued-order-2".to_string()),
        })
    );
    assert_eq!(
        index.canonical_venue_order_identity("reissued-order-2"),
        Some((
            "trade-order-1".to_string(),
            Some("client-order-1".to_string())
        ))
    );
}

#[test]
fn activated_stop_child_keeps_stop_identity_and_uses_regular_cancel_route() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "client-stop-1",
        "stop-order-1",
    );

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        None,
        "exchange-child-1",
        "stop-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
    );

    assert_eq!(venue_order_id.as_deref(), Some("stop-order-1"));
    assert_eq!(
        broker_order_index
            .lock()
            .unwrap()
            .identity_for(Some("client-stop-1"), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("exchange-child-1".to_string()),
        })
    );
    assert_eq!(
        broker_order_index
            .lock()
            .unwrap()
            .known_stop_broker_order_ids(),
        vec!["stop-order-1".to_string()]
    );
    assert_eq!(
        broker_order_index
            .lock()
            .unwrap()
            .canonical_venue_order_identity("exchange-child-1"),
        Some((
            "stop-order-1".to_string(),
            Some("client-stop-1".to_string())
        ))
    );

    let instruments = Arc::new(Mutex::new(HashMap::new()));
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    let trade = OrderTrade {
        price: Some(Quotation {
            units: 275,
            nano: 0,
        }),
        quantity: 10,
        trade_id: "trade-stop-1".to_string(),
        ..OrderTrade::default()
    };
    let fill = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-child-1".to_string(),
            direction: OrderDirection::Sell as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: vec![trade.clone()],
            ..OrderTrades::default()
        },
        &trade,
        current_unix_nanos(),
        &instruments,
    )
    .unwrap();
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let fill = project_managed_trade_fill_report(&broker_order_index, &fill_projection, fill)
        .unwrap()
        .unwrap();
    assert_eq!(fill.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        fill.client_order_id.map(|id| id.to_string()),
        Some("client-stop-1".to_string())
    );

    let race_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    race_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "client-stop-race",
        "stop-order-race",
    );
    let race_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let unresolved_fill = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-child-race".to_string(),
            direction: OrderDirection::Sell as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: vec![trade.clone()],
            ..OrderTrades::default()
        },
        &trade,
        current_unix_nanos(),
        &instruments,
    )
    .unwrap();
    assert!(
        project_trade_fill_report(&race_projection, unresolved_fill)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        resolve_stream_order_venue_id(
            &race_index,
            &race_projection,
            None,
            "exchange-child-race",
            "stop-order-race",
            OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        )
        .as_deref(),
        Some("stop-order-race")
    );
    let duplicate_fill = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-child-race".to_string(),
            direction: OrderDirection::Sell as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: vec![trade.clone()],
            ..OrderTrades::default()
        },
        &trade,
        current_unix_nanos(),
        &instruments,
    )
    .unwrap();
    assert!(
        project_managed_trade_fill_report(&race_index, &race_projection, duplicate_fill)
            .unwrap()
            .is_none()
    );
    let projection = race_projection.lock().unwrap();
    assert!(!projection.orders.contains_key("exchange-child-race"));
    assert!(projection.orders.contains_key("stop-order-race"));
    drop(projection);

    let recovery_client = test_client(TbankExecutionClientConfig::default());
    recovery_client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "client-stop-1",
        "stop-order-1",
    );
    let recovery_fill = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-child-1".to_string(),
            direction: OrderDirection::Sell as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: vec![trade.clone()],
            ..OrderTrades::default()
        },
        &trade,
        current_unix_nanos(),
        &instruments,
    )
    .unwrap();
    let recovery_fill = canonicalize_reconciled_stop_fill(
        &recovery_client.runtime,
        recovery_fill,
        &HashMap::from([("exchange-child-1".to_string(), "stop-order-1".to_string())]),
        &HashMap::from([("stop-order-1".to_string(), "client-stop-1".to_string())]),
    );
    assert_eq!(recovery_fill.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        recovery_client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .canonical_venue_order_identity("exchange-child-1"),
        Some((
            "stop-order-1".to_string(),
            Some("client-stop-1".to_string())
        ))
    );
}
#[test]
fn external_activated_stop_child_keeps_parent_identity_without_client_owner() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index
        .lock()
        .unwrap()
        .record_venue_order_id(TbankBrokerOrderRoute::StopOrder, "stop-order-1");

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        None,
        "exchange-child-1",
        "stop-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
    );

    assert_eq!(venue_order_id.as_deref(), Some("stop-order-1"));
    let index = broker_order_index.lock().unwrap();
    assert_eq!(
        index.identity_for(None, Some("exchange-child-1")),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("exchange-child-1".to_string()),
        })
    );
    assert_eq!(
        index.canonical_venue_order_identity("exchange-child-1"),
        Some(("stop-order-1".to_string(), None))
    );
}
#[test]
fn activated_stop_initial_child_ack_does_not_promote_internal_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "client-stop-1",
        "stop-order-1",
    );

    let venue_order_id = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        None,
        "internal-child-1",
        "stop-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
    );

    assert!(venue_order_id.is_none());
    let index = broker_order_index.lock().unwrap();
    assert_eq!(
        index.identity_for(Some("client-stop-1"), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: Some("stop-order-1".to_string()),
        })
    );
    assert!(index.identity_for(None, Some("internal-child-1")).is_none());
}

#[tokio::test]
async fn external_activated_stop_initial_ack_recovers_confirmed_child_in_background() {
    let orders_service = MockOrdersService::default();
    *orders_service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-child-1".to_string(),
        order_request_id: "stop-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        direction: OrderDirection::Sell as i32,
        order_type: crate::grpc::generated::OrderType::Limit as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let stop_orders_service = MockStopOrdersService::default();
    let mut stop = active_sber_stop_order("stop-order-1");
    stop.status = StopOrderStatusOption::StopOrderStatusExecuted as i32;
    stop.exchange_order_id = Some("exchange-child-1".to_string());
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![stop],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .record_broker_order_id(TbankBrokerOrderRoute::StopOrder, "stop-order-1");
    client.connect_for_queries().await.unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let context = super::TbankOrderStreamContext {
        lifecycle_active: Arc::new(super::TbankLifecycleToken::new(true)),
        emitter: test_emitter(sender),
        query_client: client.runtime.detached_query_clone(),
        pending_submits: client.runtime.pending_submits.clone(),
        unresolved_trade_fills: client.runtime.unresolved_trade_fills.clone(),
        unresolved_cancellations: client.runtime.unresolved_cancellations.clone(),
        broker_order_index: client.runtime.broker_order_index.clone(),
        fill_projection: client.runtime.fill_projection.clone(),
        order_status_projection: client.runtime.order_status_projection.clone(),
        reconnect_policy: client.runtime.config.reconnect_policy.clone(),
        activated_stop_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        regular_order_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        reconciliation_tasks: client.runtime.reconciliation_tasks.clone(),
    };

    super::schedule_activated_stop_child_reconciliation(context, None, "stop-order-1".to_string());

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected activated stop child order report");
    };
    assert_eq!(report.order_status, OrderStatus::Triggered);
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert!(report.client_order_id.is_none());
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .identity_for(None, Some("exchange-child-1")),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("exchange-child-1".to_string()),
        })
    );
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .canonical_venue_order_identity("exchange-child-1"),
        Some(("stop-order-1".to_string(), None))
    );
}

#[tokio::test]
async fn external_regular_initial_ack_schedules_request_id_reconciliation() {
    let orders_service = MockOrdersService::default();
    *orders_service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Limit as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            jitter: false,
        },
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.connect_for_queries().await.unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let context = super::TbankOrderStreamContext {
        lifecycle_active: Arc::new(super::TbankLifecycleToken::new(true)),
        emitter: test_emitter(sender),
        query_client: client.runtime.detached_query_clone(),
        pending_submits: client.runtime.pending_submits.clone(),
        unresolved_trade_fills: client.runtime.unresolved_trade_fills.clone(),
        unresolved_cancellations: client.runtime.unresolved_cancellations.clone(),
        broker_order_index: client.runtime.broker_order_index.clone(),
        fill_projection: client.runtime.fill_projection.clone(),
        order_status_projection: client.runtime.order_status_projection.clone(),
        reconnect_policy: client.runtime.config.reconnect_policy.clone(),
        activated_stop_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        regular_order_reconciliations: Arc::new(Mutex::new(HashSet::new())),
        reconciliation_tasks: client.runtime.reconciliation_tasks.clone(),
    };

    super::schedule_regular_order_reconciliation(
        context,
        None,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
    );

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected regular order recovery report");
    };
    assert!(report.client_order_id.is_none());
    assert_eq!(report.venue_order_id.to_string(), "exchange-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .identity_for(None, Some("exchange-order-1")),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: Some("exchange-order-1".to_string()),
        })
    );
}

#[test]
fn order_state_stream_restores_client_id_from_exchange_order_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::RegularOrder,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        "exchange-order-1",
    );

    let client_order_id = stream_order_state_client_order_id(
        &broker_order_index,
        None,
        "exchange-order-1",
        "trade-order-1",
    );
    let report = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
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
        "exchange-order-1",
        current_unix_nanos(),
        client_order_id.as_deref(),
        None,
    )
    .unwrap();

    assert_eq!(
        client_order_id.as_deref(),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
    );
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "exchange-order-1");
}

#[test]
fn stop_order_state_stream_updates_known_stop_lifecycle() {
    let pending_submits = Arc::new(Mutex::new(HashMap::from([(
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        TbankPendingSubmit {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            submitted_ts: current_unix_nanos(),
            quantity_shares: Decimal::from(20),
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::StopMarket,
            time_in_force: TimeInForce::Gtc,
            trailing: None,
            venue_order_id: Some("stop-order-1".to_string()),
            last_reconciliation_ts: None,
            stage: TbankPendingSubmitStage::Submitted,
        },
    )])));
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        "stop-order-1",
    );

    let report = stream_stop_order_status_report_from_state(
        order_state_stream_response::StopOrderState {
            stop_order_id: "stop-order-1".to_string(),
            account_id: "account-1".to_string(),
            direction: OrderDirection::Sell as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 270,
                nano: 0,
            }),
            ..order_state_stream_response::StopOrderState::default()
        },
        current_unix_nanos(),
        &pending_submits,
        &broker_order_index,
    )
    .unwrap()
    .unwrap();

    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.trigger_type, Some(TriggerType::Default));
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(
        report.trigger_price.map(|price| price.as_decimal()),
        Some(Decimal::from(270))
    );
}

#[test]
fn stop_order_state_stream_updates_restored_stop_without_pending_submit() {
    let pending_submits = Arc::new(Mutex::new(HashMap::new()));
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    {
        let mut index = broker_order_index.lock().unwrap();
        index.record_mapping(
            TbankBrokerOrderRoute::StopOrder,
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            "stop-order-1",
        );
        index.record_managed_context(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            TbankManagedOrderContext {
                side: Some(TbankOrderSide::Sell),
                order_type: Some(TbankOrderType::StopMarket),
                time_in_force: Some(TimeInForce::Gtc),
                quantity_shares: Some(Decimal::from(20)),
                trailing: None,
            },
        );
    }

    let report = stream_stop_order_status_report_from_state(
        order_state_stream_response::StopOrderState {
            stop_order_id: "stop-order-1".to_string(),
            account_id: "account-1".to_string(),
            direction: OrderDirection::Sell as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            status: StopOrderStatusOption::StopOrderStatusCanceled as i32,
            ..order_state_stream_response::StopOrderState::default()
        },
        current_unix_nanos(),
        &pending_submits,
        &broker_order_index,
    )
    .unwrap()
    .unwrap();

    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.trigger_type, Some(TriggerType::Default));
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
}

#[test]
fn activated_stop_order_state_links_by_trade_order_id() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        "stop-order-1",
    );
    let client_order_id =
        stream_order_state_client_order_id(&broker_order_index, None, "", "stop-order-1");

    let report = stream_order_status_report_from_state(
        order_state_stream_response::OrderState {
            trade_order_id: "stop-order-1".to_string(),
            account_id: "account-1".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot_size: 10,
            direction: OrderDirection::Sell as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            lots_requested: 2,
            lots_executed: 2,
            ..order_state_stream_response::OrderState::default()
        },
        "stop-order-1",
        current_unix_nanos(),
        client_order_id.as_deref(),
        None,
    )
    .unwrap();

    assert_eq!(report.order_status, OrderStatus::Filled);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(20));
}

#[test]
fn operation_fill_allowlist_excludes_funding_and_unknown_operations() {
    assert_eq!(
        fill_side_from_operation_type(TbankOperationType::Buy as i32),
        Some(OrderSide::Buy)
    );
    assert_eq!(
        fill_side_from_operation_type(TbankOperationType::Sell as i32),
        Some(OrderSide::Sell)
    );
    assert_eq!(
        fill_side_from_operation_type(TbankOperationType::Funding as i32),
        None
    );
    assert_eq!(fill_side_from_operation_type(99_999), None);
}

#[tokio::test]
async fn submit_reconciliation_partial_fill_bundles_status_and_fill_reports() {
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 2,
        lots_executed: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 275,
            nano: 0,
        }),
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
    assert_eq!(order_report.filled_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fill_reports.len(), 1);
    assert_eq!(fill_reports[0].last_qty.as_decimal(), Decimal::from(10));

    let late_stream_report = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: Vec::new(),
            ..OrderTrades::default()
        },
        &OrderTrade {
            price: Some(Quotation {
                units: 275,
                nano: 0,
            }),
            quantity: 10,
            trade_id: "late-trade-1".to_string(),
            ..OrderTrade::default()
        },
        current_unix_nanos(),
        &client.runtime.instruments,
    )
    .unwrap();
    assert!(
        client
            .runtime
            .project_trade_fill_report(late_stream_report)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn submit_stop_order_timeout_reconciles_against_stop_orders() {
    let service = MockStopOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    *service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![StopOrder {
            stop_order_id: "stop-order-1".to_string(),
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
        }],
    });
    let post_calls = service.post_calls.clone();
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

    let cmd = submit_stop_order_cmd();
    let reports = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();

    assert_eq!(post_calls.lock().unwrap().len(), 1);
    let report = single_order_report(&reports);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(
        client.runtime.known_broker_order_identity(
            Some(&ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156")),
            None
        ),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: Some("stop-order-1".to_string()),
        })
    );
}

#[tokio::test]
async fn submit_stop_order_reconciliation_error_starts_background_recovery() {
    let service = MockStopOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let get_error = Arc::clone(&service.get_error);
    let get_response = Arc::clone(&service.get_response);
    let get_calls = Arc::clone(&service.get_calls);
    *get_error.lock().unwrap() = Some((Code::Unavailable, "query unavailable".to_string()));
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
        enable_trading: true,
        allow_live_trading: true,
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
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
    client.connect_for_queries().await.unwrap();

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let cmd = submit_stop_order_cmd();
    let outcome = submit_nautilus_order_reports_with_recovery(
        &mut client.runtime,
        &cmd,
        current_unix_nanos(),
        Some(test_emitter(sender)),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        super::submit::SubmitPipelineOutcome::Reports(reports) if reports.is_empty()
    ));
    assert_eq!(
        client
            .runtime
            .pending_submits
            .lock()
            .unwrap()
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap()
            .stage,
        TbankPendingSubmitStage::Unknown
    );

    *get_error.lock().unwrap() = None;
    *get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![StopOrder {
            stop_order_id: "stop-order-1".to_string(),
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
        }],
    });

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected recovered stop-order status report");
    };
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert!(get_calls.lock().unwrap().len() >= 2);
}

#[tokio::test]
async fn submit_stop_order_response_returns_accepted_fallback_without_reconciliation() {
    let service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&service.get_calls);
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

    let cmd = submit_stop_order_cmd();
    let reports = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();

    let report = single_order_report(&reports);
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(
        report.trigger_price.map(|price| price.as_decimal()),
        Some(Decimal::from(270))
    );
    assert!(get_calls.lock().unwrap().is_empty());
}

#[test]
fn stop_order_submit_match_allows_broker_timestamp_precision_tolerance() {
    let client = test_client(TbankExecutionClientConfig::default());
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    let order = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        broker_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        side: TbankOrderSide::Sell,
        order_type: TbankOrderType::StopMarket,
        time_in_force: TimeInForce::Gtc,
        quantity_shares: Decimal::from(20),
        limit_price: None,
        trigger_price: Some(Decimal::from(270)),
        trailing: None,
        confirm_margin_trade: false,
    };
    let submitted_ts = UnixNanos::from(1_700_000_000_999_000_000);
    let stop = StopOrder {
        stop_order_id: "stop-order-1".to_string(),
        lots_requested: 2,
        direction: StopOrderDirection::Sell as i32,
        order_type: StopOrderType::StopLoss as i32,
        instrument_uid: "sber-uid".to_string(),
        stop_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 270,
            nano: 0,
        }),
        create_date: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        ..StopOrder::default()
    };

    assert!(
        client
            .runtime
            .stop_order_matches_submit(&stop, &order, &metadata, submitted_ts)
    );
}

#[test]
fn stop_order_submit_match_rejects_stale_broker_timestamp() {
    let client = test_client(TbankExecutionClientConfig::default());
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    let order = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        broker_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        side: TbankOrderSide::Sell,
        order_type: TbankOrderType::StopMarket,
        time_in_force: TimeInForce::Gtc,
        quantity_shares: Decimal::from(20),
        limit_price: None,
        trigger_price: Some(Decimal::from(270)),
        trailing: None,
        confirm_margin_trade: false,
    };
    let submitted_ts = UnixNanos::from(1_700_000_003_000_000_000);
    let stop = StopOrder {
        stop_order_id: "stop-order-1".to_string(),
        lots_requested: 2,
        direction: StopOrderDirection::Sell as i32,
        order_type: StopOrderType::StopLoss as i32,
        instrument_uid: "sber-uid".to_string(),
        stop_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 270,
            nano: 0,
        }),
        create_date: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        ..StopOrder::default()
    };

    assert!(
        !client
            .runtime
            .stop_order_matches_submit(&stop, &order, &metadata, submitted_ts)
    );
}

async fn submit_with_post_error(
    code: Code,
    message: &str,
) -> (String, Arc<Mutex<Vec<GetOrderStateRequest>>>) {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.post_error.lock().unwrap() = Some((code, message.to_string()));
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
    (error.to_string(), state_calls)
}

#[tokio::test]
async fn submit_broker_validation_statuses_return_rejection_without_reconciliation() {
    for (code, message) in [
        (Code::InvalidArgument, "invalid order quantity"),
        (Code::FailedPrecondition, "not enough buying power"),
    ] {
        let (reason, state_calls) = submit_with_post_error(code, message).await;
        assert!(reason.contains(message));
        assert!(state_calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn submit_unresolved_reconciliation_keeps_unknown_pending() {
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let state_error = Arc::clone(&service.state_error);
    let state_response = Arc::clone(&service.state_response);
    *state_error.lock().unwrap() = Some((Code::NotFound, "order state not found".to_string()));
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
    let reports = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();

    assert!(reports.is_empty());
    {
        let pending_submits = client.runtime.pending_submits.lock().unwrap();
        let pending = pending_submits
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap();
        assert_eq!(pending.stage, TbankPendingSubmitStage::Unknown);
        assert_eq!(pending.instrument_id, "SBER_TQBR.MOEX");
        assert!(pending.last_reconciliation_ts.is_some());
    }

    *state_error.lock().unwrap() = None;
    *state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: tbank_broker_request_id_for_client_order_id(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        ),
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
    let reconciled = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconciled.order_report.order_status, OrderStatus::Accepted);
    {
        let pending_submits = client.runtime.pending_submits.lock().unwrap();
        let pending = pending_submits
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap();
        assert_eq!(pending.stage, TbankPendingSubmitStage::Accepted);
        assert_eq!(pending.venue_order_id.as_deref(), Some("exchange-order-1"));
    }
    let stream_report = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: sber_metadata().instrument_uid,
            trades: Vec::new(),
            ..OrderTrades::default()
        },
        &OrderTrade {
            price: Some(Quotation {
                units: 275,
                nano: 0,
            }),
            quantity: 10,
            trade_id: "late-trade-1".to_string(),
            ..OrderTrade::default()
        },
        current_unix_nanos(),
        &client.runtime.instruments,
    )
    .unwrap();
    assert!(
        client
            .runtime
            .project_trade_fill_report(stream_report)
            .unwrap()
            .is_some()
    );
    {
        let pending_submits = client.runtime.pending_submits.lock().unwrap();
        let pending = pending_submits
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap();
        assert_eq!(pending.stage, TbankPendingSubmitStage::Filled);
    }
}

#[tokio::test]
async fn submit_unknown_outcome_recovers_in_background_after_initial_miss() {
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let state_error = Arc::clone(&service.state_error);
    let state_response = Arc::clone(&service.state_response);
    let state_calls = Arc::clone(&service.state_calls);
    *state_error.lock().unwrap() = Some((Code::NotFound, "order state not found".to_string()));
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
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            jitter: false,
        },
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
    let cmd = submit_order_cmd(None);
    let reports = submit_nautilus_order_reports_with_recovery(
        &mut client.runtime,
        &cmd,
        current_unix_nanos(),
        Some(test_emitter(sender)),
    )
    .await
    .unwrap();

    assert!(matches!(
        reports,
        super::submit::SubmitPipelineOutcome::Reports(reports) if reports.is_empty()
    ));
    {
        let pending_submits = client.runtime.pending_submits.lock().unwrap();
        let pending = pending_submits
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap();
        assert_eq!(pending.stage, TbankPendingSubmitStage::Unknown);
    }

    *state_error.lock().unwrap() = None;
    *state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: tbank_broker_request_id_for_client_order_id(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        ),
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

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected recovered order status report");
    };
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.venue_order_id.to_string(), "exchange-order-1");
    assert!(state_calls.lock().unwrap().len() >= 2);
    {
        let pending_submits = client.runtime.pending_submits.lock().unwrap();
        let pending = pending_submits
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap();
        assert_eq!(pending.stage, TbankPendingSubmitStage::Accepted);
        assert_eq!(pending.venue_order_id.as_deref(), Some("exchange-order-1"));
    }
}
