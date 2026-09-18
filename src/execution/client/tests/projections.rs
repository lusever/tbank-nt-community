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
    let accepted = super::stop_order_status_report_with_context(
        "TBANK-001".into(),
        accepted_stop,
        ts_init,
        10,
        None,
        None,
    )
    .unwrap();
    let mut triggered_stop = active_sber_stop_order("stop-order-1");
    triggered_stop.status = StopOrderStatusOption::StopOrderStatusExecuted as i32;
    let triggered = super::stop_order_status_report_with_context(
        "TBANK-001".into(),
        triggered_stop,
        ts_init,
        10,
        None,
        None,
    )
    .unwrap();

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
        None
    );
    assert_eq!(
        index.route_for_client_order_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        Some(TbankBrokerOrderRoute::RegularOrder)
    );
    assert!(
        index
            .identity_for(None, Some("internal-uuid-like-id"))
            .is_none()
    );
}

#[test]
fn unresolved_stop_stream_event_preserves_stop_route_when_identity_arrives_later() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let client_order_id = "stop-request-with-delayed-identity";
    let broker_request_id = {
        let mut index = broker_order_index.lock().unwrap();
        index.record_client_order_route(TbankBrokerOrderRoute::StopOrder, client_order_id);
        index
            .get_or_allocate_request_mapping(client_order_id, None)
            .unwrap()
    };

    let resolved = resolve_stream_order_venue_id(
        &broker_order_index,
        &Arc::new(Mutex::new(TbankFillProjection::default())),
        Some(broker_request_id.as_str()),
        "stop-order-1",
        "trade-order-1",
        OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
    )
    .expect("stop event should resolve once it carries a broker order id");

    assert_eq!(resolved.venue_order_id, "stop-order-1");
    assert_eq!(
        broker_order_index
            .lock()
            .unwrap()
            .identity_for(Some(client_order_id), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
    assert!(
        broker_order_index
            .lock()
            .unwrap()
            .is_known_stop_broker_order_id("stop-order-1")
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
            broker_order_id: "exchange-order-1".to_string(),
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
            broker_order_id: "reissued-order-2".to_string(),
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
            broker_order_id: "exchange-child-1".to_string(),
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
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let fill = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        fill,
        Some(&sender),
    )
    .unwrap()
    .unwrap();
    assert!(receiver.try_recv().is_ok());
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
        project_trade_fill_report(&race_projection, unresolved_fill.report)
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
        project_managed_trade_fill_report(
            &race_index,
            &race_projection,
            duplicate_fill,
            Some(&sender),
        )
            .unwrap()
            .is_none()
    );
    assert!(receiver.try_recv().is_err());
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
        recovery_fill.report,
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
            broker_order_id: "exchange-child-1".to_string(),
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
            broker_order_id: "stop-order-1".to_string(),
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
        stages: vec![
            crate::grpc::generated::OrderStage {
                trade_id: "stop-child-trade-1".to_string(),
                quantity: 1,
                ..crate::grpc::generated::OrderStage::default()
            },
            crate::grpc::generated::OrderStage {
                trade_id: "stop-child-trade-2".to_string(),
                quantity: 1,
                ..crate::grpc::generated::OrderStage::default()
            },
        ],
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
    seed_sber_metadata(&mut client);
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
        instruments: client.runtime.instruments.clone(),
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
            broker_order_id: "exchange-child-1".to_string(),
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
    let index = client.runtime.broker_order_index.lock().unwrap();
    assert_eq!(
        index.venue_order_id_for_trade_id("stop-child-trade-1"),
        Some("stop-order-1".to_string())
    );
    assert_eq!(
        index.venue_order_id_for_trade_id("stop-child-trade-2"),
        Some("stop-order-1".to_string())
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
        instruments: client.runtime.instruments.clone(),
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
            broker_order_id: "exchange-order-1".to_string(),
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
            quantity_units: Decimal::from(20),
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
                report_order_type: Some(OrderType::StopMarket),
                time_in_force: Some(TimeInForce::Gtc),
                quantity_units: Some(Decimal::from(20)),
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
fn stop_order_state_stream_preserves_restored_limit_take_profit_type() {
    let pending_submits = Arc::new(Mutex::new(HashMap::new()));
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    {
        let mut index = broker_order_index.lock().unwrap();
        index.record_mapping(
            TbankBrokerOrderRoute::StopOrder,
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            "limit-tp-stop-1",
        );
        index.record_managed_context(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            TbankManagedOrderContext {
                side: Some(TbankOrderSide::Buy),
                order_type: None,
                report_order_type: Some(OrderType::LimitIfTouched),
                time_in_force: Some(TimeInForce::Gtc),
                quantity_units: Some(Decimal::from(20)),
                trailing: None,
            },
        );
    }

    let report = stream_stop_order_status_report_from_state(
        order_state_stream_response::StopOrderState {
            stop_order_id: "limit-tp-stop-1".to_string(),
            account_id: "account-1".to_string(),
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Limit as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..order_state_stream_response::StopOrderState::default()
        },
        current_unix_nanos(),
        &pending_submits,
        &broker_order_index,
    )
    .unwrap()
    .unwrap();

    assert_eq!(report.order_type, OrderType::LimitIfTouched);
}

#[test]
fn stop_order_context_records_limit_take_profit_reporting_type() {
    let client = test_client(TbankExecutionClientConfig::default());
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    client
        .runtime
        .record_stop_order_context(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            &StopOrder {
                stop_order_id: "limit-tp-stop-1".to_string(),
                direction: StopOrderDirection::Buy as i32,
                order_type: StopOrderType::TakeProfit as i32,
                exchange_order_type: ExchangeOrderType::Limit as i32,
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..StopOrder::default()
            },
            &metadata,
        );

    let context = client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .managed_context_for_client_order_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
        .unwrap();
    assert_eq!(context.report_order_type, Some(OrderType::LimitIfTouched));
}

#[test]
fn stop_order_submit_reconciliation_accepts_small_broker_clock_skew() {
    let submitted_ts = UnixNanos::from(1_700_000_000_000_000_000_u64);
    let earliest_ts = 1_699_999_998_000_000_000_u64;
    assert_eq!(
        super::stop_order_submit_earliest_timestamp(submitted_ts),
        earliest_ts
    );
    let query_from = i128::from(earliest_ts);
    let mut stop = active_sber_stop_order("skewed-stop-1");
    stop.create_date = Some(prost_types::Timestamp {
        seconds: 1_699_999_998,
        nanos: 500_000_000,
    });
    assert!(super::stop_order_is_after_submit(
        &stop,
        submitted_ts,
        query_from,
    ));

    stop.create_date = Some(prost_types::Timestamp {
        seconds: 1_699_999_997,
        nanos: 999_999_999,
    });
    assert!(!super::stop_order_is_after_submit(
        &stop,
        submitted_ts,
        query_from,
    ));
}

#[tokio::test]
async fn stop_submit_reconciliation_does_not_rebind_known_broker_order() {
    let service = MockStopOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let mut known_candidate = active_sber_stop_order("known-stop-order");
    known_candidate.exchange_order_type = ExchangeOrderType::Market as i32;
    *service.get_responses.lock().unwrap() = VecDeque::from([
        GetStopOrdersResponse::default(),
        GetStopOrdersResponse {
            stop_orders: vec![known_candidate],
        },
    ]);
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
    client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .record_mapping(
            TbankBrokerOrderRoute::StopOrder,
            "other-client-order",
            "known-stop-order",
        );
    client.connect_for_queries().await.unwrap();

    let cmd = submit_stop_order_cmd();
    let reports = submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .expect("known broker order must not be rebound during reconciliation");

    assert!(reports.is_empty());
    let index = client.runtime.broker_order_index.lock().unwrap();
    assert_eq!(
        index.client_order_id_for_venue_order_id("known-stop-order"),
        Some("other-client-order".to_string())
    );
    assert_eq!(
        index.identity_for(
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
            None
        ),
        None
    );
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
async fn duplicate_reported_commission_updates_provenance_without_replaying_fill() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::RegularOrder,
        "client-order-1",
        "broker-order-1",
    );
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    let make_fill = |commission: TbankFillCommission, source: TbankFillCommissionSource| {
        let report = FillReport::new(
            "TBANK-001".into(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            "broker-order-1".into(),
            "trade-1".into(),
            OrderSide::Buy,
            Quantity::from(10),
            Price::from("275"),
            commission
                .amount()
                .unwrap_or_else(|| Money::from("0 RUB")),
            LiquiditySide::NoLiquiditySide,
            Some("client-order-1".into()),
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            Some(UUID4::new()),
        );
        TbankFillReport::new(report, commission, source)
    };

    let first = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        make_fill(
            TbankFillCommission::Unknown,
            TbankFillCommissionSource::OrderStateStream,
        ),
        Some(&sender),
    )
    .unwrap();
    assert!(first.is_some());
    let first_event = receiver.try_recv().unwrap();
    let first_event = match first_event {
        nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) => {
            data.data
                .as_any()
                .downcast_ref::<crate::execution::events::TbankExecutionEvent>()
                .unwrap()
                .clone()
        }
        other => panic!("unexpected event: {other:?}"),
    };
    assert!(matches!(
        first_event,
        crate::execution::events::TbankExecutionEvent::FillCommission {
            status: crate::execution::events::TbankFillCommissionStatus::Unknown,
            ..
        }
    ));

    let second = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        make_fill(
            TbankFillCommission::Reported(Money::from("1.25 RUB")),
            TbankFillCommissionSource::OperationsCursor,
        ),
        Some(&sender),
    )
    .unwrap();
    assert!(second.is_none());
    let second_event = receiver.try_recv().unwrap();
    let second_event = match second_event {
        nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) => {
            data.data
                .as_any()
                .downcast_ref::<crate::execution::events::TbankExecutionEvent>()
                .unwrap()
                .clone()
        }
        other => panic!("unexpected event: {other:?}"),
    };
    assert!(matches!(
        second_event,
        crate::execution::events::TbankExecutionEvent::FillCommission {
            status: crate::execution::events::TbankFillCommissionStatus::Reported,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        } if amount == "1.25"
    ));
    assert!(receiver.try_recv().is_err());
}

#[test]
fn unavailable_trade_provenance_channel_does_not_block_fill_projection() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let fill = TbankFillReport::new(
        FillReport::new(
            "TBANK-001".into(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            "broker-order-1".into(),
            "trade-1".into(),
            OrderSide::Buy,
            Quantity::from(10),
            Price::from("275"),
            Money::from("0 RUB"),
            LiquiditySide::NoLiquiditySide,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            Some(UUID4::new()),
        ),
        TbankFillCommission::Unknown,
        TbankFillCommissionSource::TradesStream,
    );

    let projected = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        fill.clone(),
        None,
    )
    .unwrap();
    assert!(
        projected.is_some(),
        "missing custom-data sender must not drop FillReport"
    );
    assert_eq!(fill_projection.lock().unwrap().orders.len(), 1);

    let (closed_sender, closed_receiver) = tokio::sync::mpsc::unbounded_channel();
    drop(closed_receiver);
    let separate_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let projected = project_managed_trade_fill_report(
        &broker_order_index,
        &separate_projection,
        fill,
        Some(&closed_sender),
    )
    .unwrap();
    assert!(
        projected.is_some(),
        "closed custom-data sender must not drop FillReport"
    );
    assert_eq!(separate_projection.lock().unwrap().orders.len(), 1);
}

#[test]
fn detached_clone_captures_runner_sender_after_client_creation() {
    let (initial_sender, mut initial_receiver) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(initial_sender);
    let client = test_client(TbankExecutionClientConfig::default());

    let (runner_sender, mut runner_receiver) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(runner_sender);
    let detached = client.runtime.detached_query_clone();
    let projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "trade-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("0 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );

    assert!(
        project_managed_trade_fill_report(
            &detached.broker_order_index,
            &projection,
            unknown_fill_report(report),
            detached.current_data_event_sender().as_ref(),
        )
        .unwrap()
        .is_some()
    );
    assert!(initial_receiver.try_recv().is_err());
    assert!(runner_receiver.try_recv().is_ok());
}

#[test]
fn existing_detached_clone_observes_a_refreshed_sender_on_worker_thread() {
    let (initial_sender, mut initial_receiver) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(initial_sender);
    let client = test_client(TbankExecutionClientConfig::default());
    let detached = client.runtime.detached_query_clone();

    let (runner_sender, mut runner_receiver) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(runner_sender);
    client.runtime.refresh_data_event_sender();

    let projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "trade-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("0 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    let result = std::thread::spawn(move || {
        let sender = detached.current_data_event_sender();
        project_managed_trade_fill_report(
            &detached.broker_order_index,
            &projection,
            unknown_fill_report(report),
            sender.as_ref(),
        )
    })
    .join()
    .expect("worker publication must not panic")
    .unwrap();

    assert!(result.is_some());
    assert!(initial_receiver.try_recv().is_err());
    assert!(runner_receiver.try_recv().is_ok());
}

#[test]
fn cumulative_commission_stays_unknown_when_an_earlier_fill_has_unknown_provenance() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let stream_fill = TbankFillReport::new(
        FillReport::new(
            "TBANK-001".into(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            "broker-order-1".into(),
            "trade-1".into(),
            OrderSide::Buy,
            Quantity::from(10),
            Price::from("275"),
            Money::from("0 RUB"),
            LiquiditySide::NoLiquiditySide,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            Some(UUID4::new()),
        ),
        TbankFillCommission::Unknown,
        TbankFillCommissionSource::OrderStateStream,
    );
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
            stream_fill,
            Some(&sender),
        )
        .unwrap()
        .is_some()
    );

    let projected = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-20",
        Decimal::from(20),
        Decimal::from(5_500),
        Some(Money::from("10 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(projected.quantity.as_decimal(), Decimal::from(10));
    assert_eq!(projected.commission, TbankFillCommission::Unknown);
}

#[test]
fn repeated_cumulative_snapshot_does_not_charge_ambiguous_total_to_last_delta() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    let first = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-5",
        Decimal::from(5),
        Decimal::from(1_375),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(first.commission, TbankFillCommission::Unknown);

    let second = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("10 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(second.commission, TbankFillCommission::Unknown);

    assert!(
        project_cumulative_order_fill(
            &fill_projection,
            "broker-order-1",
            "synthetic-order-state-10",
            Decimal::from(10),
            Decimal::from(2_750),
            Some(Money::from("10 RUB")),
        )
        .unwrap()
        .is_none(),
        "an ambiguous cumulative total must not become a provenance correction"
    );
}

#[test]
fn stale_cumulative_snapshot_cannot_correct_a_newer_fill_commission() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    let initial = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(initial.commission, TbankFillCommission::Unknown);

    assert!(project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "stale-order-state-5",
        Decimal::from(5),
        Decimal::from(1_375),
        Some(Money::from("0.5 RUB")),
    )
    .unwrap()
    .is_none());

    let correction = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "fresh-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1 RUB")),
    )
    .unwrap()
    .expect("fresh cumulative commission corrects the unknown fill");
    assert!(correction.provenance_only);
    assert_eq!(correction.trade_id.as_deref(), Some("synthetic-order-state-10"));
    assert_eq!(
        correction.commission,
        TbankFillCommission::Allocated(Money::from("1 RUB"))
    );
}

#[test]
fn repeated_cumulative_snapshot_reports_only_incremental_commission() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    let first = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        first.commission,
        TbankFillCommission::Allocated(Money::from("1 RUB"))
    );

    let second = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-20",
        Decimal::from(20),
        Decimal::from(5_500),
        Some(Money::from("2 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(second.quantity.as_decimal(), Decimal::from(10));
    assert_eq!(
        second.commission,
        TbankFillCommission::Allocated(Money::from("1 RUB"))
    );
}

#[test]
fn stale_cumulative_commission_does_not_resolve_a_larger_stream_fill() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let stream_fill = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "stream-trade-10".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("0 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    crate::execution::projections::project_trade_fill_report_locked(
        &mut fill_projection.lock().unwrap(),
        stream_fill,
        TbankFillCommission::Unknown,
    )
    .unwrap()
    .expect("the stream trade emits the ten-lot fill");

    assert!(project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "stale-order-state-5",
        Decimal::from(5),
        Decimal::from(1_375),
        Some(Money::from("0.5 RUB")),
    )
    .unwrap()
    .is_none());

    let correction = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "fresh-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1 RUB")),
    )
    .unwrap()
    .expect("the fresh cumulative commission resolves the stream fill");

    assert!(correction.provenance_only);
    assert_eq!(correction.trade_id.as_deref(), Some("stream-trade-10"));
    assert_eq!(correction.quantity.as_decimal(), Decimal::from(10));
    assert_eq!(
        correction.commission,
        TbankFillCommission::Allocated(Money::from("1 RUB"))
    );
}

#[test]
fn alias_projection_deduplicates_cumulative_queue_before_real_trade() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    project_cumulative_order_fill(
        &fill_projection,
        "canonical-order-1",
        "canonical-synthetic-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();
    project_cumulative_order_fill(
        &fill_projection,
        "alias-order-1",
        "alias-synthetic-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();

    super::merge_fill_projection_alias(
        &mut fill_projection.lock().unwrap(),
        "alias-order-1",
        "canonical-order-1",
    );

    let real_trade = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "canonical-order-1".into(),
        "real-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(20),
        Price::from("275"),
        Money::from("0 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    let projected = project_trade_fill_report(&fill_projection, real_trade)
        .unwrap()
        .expect("the real trade must retain its non-cumulative remainder");

    assert_eq!(projected.last_qty.as_decimal(), Decimal::from(10));
}

#[test]
fn alias_projection_transfers_equal_sized_fill_commissions_one_to_one() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    for (order_id, trade_prefix, cumulative_commission) in [
        ("canonical-order-1", "canonical", None),
        ("alias-order-1", "alias", Some(Money::from("0.01 RUB"))),
    ] {
        project_cumulative_order_fill(
            &fill_projection,
            order_id,
            &format!("{trade_prefix}-synthetic-1"),
            Decimal::ONE,
            Decimal::from(275),
            cumulative_commission,
        )
        .unwrap()
        .unwrap();
        project_cumulative_order_fill(
            &fill_projection,
            order_id,
            &format!("{trade_prefix}-synthetic-2"),
            Decimal::from(2),
            Decimal::from(550),
            cumulative_commission.map(|_| Money::from("0.02 RUB")),
        )
        .unwrap()
        .unwrap();
    }

    super::merge_fill_projection_alias(
        &mut fill_projection.lock().unwrap(),
        "alias-order-1",
        "canonical-order-1",
    );

    let next_fill = project_cumulative_order_fill(
        &fill_projection,
        "canonical-order-1",
        "canonical-synthetic-3",
        Decimal::from(3),
        Decimal::from(825),
        Some(Money::from("0.03 RUB")),
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        next_fill.commission,
        TbankFillCommission::Allocated(Money::from("0.01 RUB"))
    );
}

#[test]
fn partial_alias_merge_resizes_commission_before_trade_reconciliation() {
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    project_cumulative_order_fill(
        &fill_projection,
        "canonical-order-1",
        "canonical-synthetic-5",
        Decimal::from(5),
        Decimal::from(1_375),
        None,
    )
    .unwrap()
    .unwrap();
    project_cumulative_order_fill(
        &fill_projection,
        "alias-order-1",
        "alias-synthetic-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1 RUB")),
    )
    .unwrap()
    .unwrap();

    super::merge_fill_projection_alias(
        &mut fill_projection.lock().unwrap(),
        "alias-order-1",
        "canonical-order-1",
    );

    let real_trade = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "canonical-order-1".into(),
        "real-trade-10".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("1 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    {
        let mut projection = fill_projection.lock().unwrap();
        assert!(crate::execution::projections::project_trade_fill_report_locked(
            &mut projection,
            real_trade.clone(),
            TbankFillCommission::Reported(Money::from("1 RUB")),
        )
        .unwrap()
        .is_none());
        let (corrections, residual) =
            crate::execution::projections::update_duplicate_fill_provenance(
                &mut projection,
                &real_trade,
                TbankFillCommission::Reported(Money::from("1 RUB")),
            )
            .unwrap();
        assert_eq!(
            corrections,
            vec![(
                "canonical-synthetic-5".to_string(),
                TbankFillCommission::Allocated(Money::from("0.50 RUB")),
            )]
        );
        assert_eq!(residual, None);
    }

    let next_fill = project_cumulative_order_fill(
        &fill_projection,
        "canonical-order-1",
        "canonical-synthetic-15",
        Decimal::from(15),
        Decimal::from(4_125),
        Some(Money::from("1.50 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(next_fill.quantity.as_decimal(), Decimal::from(5));
    assert_eq!(
        next_fill.commission,
        TbankFillCommission::Allocated(Money::from("0.50 RUB"))
    );
}

#[test]
fn reported_operation_commission_corrects_an_emitted_synthetic_fill() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("1.25 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    let projected = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        TbankFillReport::new(
            report,
            TbankFillCommission::Reported(Money::from("1.25 RUB")),
            TbankFillCommissionSource::OperationsCursor,
        ),
        Some(&sender),
    )
    .unwrap();

    assert!(projected.is_none());
    let event = receiver.try_recv().unwrap();
    let nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) = event
    else {
        panic!("expected custom execution event");
    };
    assert!(matches!(
        data.data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>(),
        Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            trade_id,
            status: crate::execution::events::TbankFillCommissionStatus::Reported,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        }) if trade_id == "synthetic-order-state-10" && amount == "1.25"
    ));
    assert!(receiver.try_recv().is_err());
}

#[test]
fn partially_matched_operation_commission_allocates_the_published_residual() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-1",
        Decimal::from(1),
        Decimal::from(275),
        None,
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(2),
        Price::from("275"),
        Money::from("0.03 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    let projected = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        TbankFillReport::new(
            report,
            TbankFillCommission::Reported(Money::from("0.03 RUB")),
            TbankFillCommissionSource::OperationsCursor,
        ),
        Some(&sender),
    )
    .unwrap()
    .expect("the unmatched operation quantity must remain a real fill");

    assert_eq!(projected.last_qty.as_decimal(), Decimal::ONE);
    assert_eq!(projected.commission, Money::from("0.01 RUB"));

    let mut provenance = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        let nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) =
            event
        else {
            panic!("expected custom execution event");
        };
        let Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            trade_id,
            status,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        }) = data
            .data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>()
        else {
            panic!("expected commission provenance");
        };
        provenance.push((
            trade_id.to_string(),
            *status,
            amount.to_string().parse::<Decimal>().unwrap(),
        ));
    }
    assert!(provenance.contains(&(
        "synthetic-order-state-1".to_string(),
        crate::execution::events::TbankFillCommissionStatus::Allocated,
        Decimal::new(2, 2),
    )));
    assert!(provenance.contains(&(
        "operations-trade-1".to_string(),
        crate::execution::events::TbankFillCommissionStatus::Allocated,
        Decimal::new(1, 2),
    )));
    assert_eq!(
        provenance
            .iter()
            .map(|(_, _, amount)| *amount)
            .sum::<Decimal>(),
        Decimal::new(3, 2)
    );
}

#[test]
fn partially_matched_operation_commission_reserves_known_synthetic_fee() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let known_synthetic_commission = Money::from("0.01 RUB");

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-1",
        Decimal::ONE,
        Decimal::from(275),
        Some(known_synthetic_commission),
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(2),
        Price::from("275"),
        Money::from("0.03 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    let projected = project_managed_trade_fill_report(
        &broker_order_index,
        &fill_projection,
        TbankFillReport::new(
            report,
            TbankFillCommission::Reported(Money::from("0.03 RUB")),
            TbankFillCommissionSource::OperationsCursor,
        ),
        Some(&sender),
    )
    .unwrap()
    .expect("the unmatched operation quantity must remain a real fill");

    assert_eq!(projected.last_qty.as_decimal(), Decimal::ONE);
    assert_eq!(projected.commission, Money::from("0.02 RUB"));
    assert_eq!(
        known_synthetic_commission.as_decimal() + projected.commission.as_decimal(),
        Decimal::new(3, 2)
    );

    let event = receiver.try_recv().unwrap();
    let nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) = event
    else {
        panic!("expected custom execution event");
    };
    assert!(matches!(
        data.data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>(),
        Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            trade_id,
            status: crate::execution::events::TbankFillCommissionStatus::Allocated,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        }) if trade_id == "operations-trade-1" && amount == "0.02"
    ));
    assert!(receiver.try_recv().is_err());
}

#[test]
fn synthetic_commission_correction_allocates_minor_units_without_negative_residual() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    for quantity in 1..=5 {
        project_cumulative_order_fill(
            &fill_projection,
            "broker-order-1",
            &format!("synthetic-order-state-{quantity}"),
            Decimal::from(quantity),
            Decimal::from(quantity * 275),
            None,
        )
        .unwrap()
        .unwrap();
    }

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(5),
        Price::from("275"),
        Money::from("0.03 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
            TbankFillReport::new(
                report,
                TbankFillCommission::Reported(Money::from("0.03 RUB")),
                TbankFillCommissionSource::OperationsCursor,
            ),
            Some(&sender),
        )
        .unwrap()
        .is_none()
    );

    let mut amounts = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        let nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) =
            event
        else {
            panic!("expected custom execution event");
        };
        let Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            status: crate::execution::events::TbankFillCommissionStatus::Allocated,
            amount: Some(amount),
            ..
        }) = data
            .data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>()
        else {
            panic!("expected allocated commission provenance");
        };
        amounts.push(amount.to_string().parse::<Decimal>().unwrap());
    }

    assert_eq!(amounts.len(), 5);
    assert!(amounts.iter().all(|amount| *amount >= Decimal::ZERO));
    assert_eq!(amounts.into_iter().sum::<Decimal>(), Decimal::new(3, 2));
}

#[test]
fn fully_matched_unknown_trade_does_not_block_later_cumulative_commission() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1.25 RUB")),
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "trades-stream-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("0 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
            TbankFillReport::new(
                report,
                TbankFillCommission::Unknown,
                TbankFillCommissionSource::TradesStream,
            ),
            None,
        )
        .unwrap()
        .is_none()
    );

    let next = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-20",
        Decimal::from(20),
        Decimal::from(5_500),
        Some(Money::from("2.50 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(next.commission, TbankFillCommission::Allocated(Money::from("1.25 RUB")));
}

#[test]
fn allocated_operation_commissions_resolve_later_cumulative_fill() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
    let allocated_commissions = super::allocate_operation_commission(
        TbankFillCommission::Reported(Money::from("1 RUB")),
        &[Decimal::ONE, Decimal::ONE],
    )
    .unwrap();

    for (index, commission) in allocated_commissions.into_iter().enumerate() {
        assert!(matches!(commission, TbankFillCommission::Allocated(_)));
        let report = FillReport::new(
            "TBANK-001".into(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            "broker-order-1".into(),
            format!("operation-trade-{index}").into(),
            OrderSide::Buy,
            Quantity::from(1),
            Price::from("100"),
            commission.amount().expect("allocated commission is known"),
            LiquiditySide::NoLiquiditySide,
            None,
            None,
            UnixNanos::from(index as u64 + 1),
            UnixNanos::from(index as u64 + 1),
            Some(UUID4::new()),
        );
        assert!(
            project_managed_trade_fill_report(
                &broker_order_index,
                &fill_projection,
                TbankFillReport::new(
                    report,
                    commission,
                    TbankFillCommissionSource::OperationsCursor,
                ),
                Some(&sender),
            )
            .unwrap()
            .is_some()
        );
    }

    let next = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-3",
        Decimal::from(3),
        Decimal::from(300),
        Some(Money::from("1.50 RUB")),
    )
    .unwrap()
    .expect("the later cumulative fill adds one lot");

    assert_eq!(next.quantity, Quantity::from(1));
    assert_eq!(
        next.commission,
        TbankFillCommission::Allocated(Money::from("0.50 RUB"))
    );
}

#[test]
fn reported_operation_commission_upgrades_exact_allocated_synthetic_fill() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    let synthetic = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("1 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        synthetic.commission,
        TbankFillCommission::Allocated(Money::from("1 RUB"))
    );

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(10),
        Price::from("275"),
        Money::from("1.25 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
            TbankFillReport::new(
                report,
                TbankFillCommission::Reported(Money::from("1.25 RUB")),
                TbankFillCommissionSource::OperationsCursor,
            ),
            Some(&sender),
        )
        .unwrap()
        .is_none()
    );

    let event = receiver.try_recv().unwrap();
    let nautilus_common::messages::DataEvent::Data(nautilus_model::data::Data::Custom(data)) = event
    else {
        panic!("expected custom execution event");
    };
    assert!(matches!(
        data.data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>(),
        Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            trade_id,
            status: crate::execution::events::TbankFillCommissionStatus::Reported,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        }) if trade_id == "synthetic-order-state-10" && amount == "1.25"
    ));
    assert!(receiver.try_recv().is_err());

    let next = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-20",
        Decimal::from(20),
        Decimal::from(5_500),
        Some(Money::from("2 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        next.commission,
        TbankFillCommission::Allocated(Money::from("0.75 RUB"))
    );
}

#[test]
fn split_operation_commissions_accumulate_on_one_synthetic_fill() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();

    let make_report = |trade_id: &str, quantity: u64, commission: &str| {
        FillReport::new(
            "TBANK-001".into(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            "broker-order-1".into(),
            trade_id.into(),
            OrderSide::Buy,
            Quantity::from(quantity),
            Price::from("275"),
            Money::from(commission),
            LiquiditySide::NoLiquiditySide,
            None,
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            Some(UUID4::new()),
        )
    };

    for (index, (trade_id, quantity, commission)) in [
        ("operations-trade-1", 6, "0.75 RUB"),
        ("operations-trade-2", 4, "0.50 RUB"),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(
            project_managed_trade_fill_report(
                &broker_order_index,
                &fill_projection,
                TbankFillReport::new(
                    make_report(trade_id, quantity, commission),
                    TbankFillCommission::Reported(Money::from(commission)),
                    TbankFillCommissionSource::OperationsCursor,
                ),
                Some(&sender),
            )
            .unwrap()
            .is_none()
        );
        if index == 0 {
            assert!(
                receiver.try_recv().is_err(),
                "partial synthetic-fill commission must stay internal"
            );
        }
    }

    let event = receiver.try_recv().unwrap();
    let event_amount = |event| {
        let nautilus_common::messages::DataEvent::Data(
            nautilus_model::data::Data::Custom(data),
        ) = event
        else {
            panic!("expected custom execution event");
        };
        let Some(crate::execution::events::TbankExecutionEvent::FillCommission {
            trade_id,
            status: crate::execution::events::TbankFillCommissionStatus::Reported,
            amount: Some(amount),
            source: TbankFillCommissionSource::OperationsCursor,
            ..
        }) = data
            .data
            .as_any()
            .downcast_ref::<crate::execution::events::TbankExecutionEvent>()
        else {
            panic!("expected reported commission provenance");
        };
        (trade_id.to_string(), amount.to_string())
    };

    assert_eq!(
        event_amount(event),
        ("synthetic-order-state-10".to_string(), "1.25".to_string())
    );
    assert!(receiver.try_recv().is_err());
}
#[test]
fn partially_matched_operation_commission_keeps_residual_unknown_for_later_snapshot() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(2),
        Price::from("275"),
        Money::from("0.02 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
            TbankFillReport::new(
                report,
                TbankFillCommission::Reported(Money::from("0.02 RUB")),
                TbankFillCommissionSource::OperationsCursor,
            ),
            Some(&sender),
        )
        .unwrap()
        .is_none()
    );
    assert!(receiver.try_recv().is_err());

    // The partial 2-lot match stays internal because the event has no coverage field.
    // The next cumulative snapshot must not charge the unresolved remainder of the first fill
    // to the new delta: the fee split between the eight unresolved lots and the new fill is
    // still unknown, so the new fill stays fail-closed.
    let next = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-12",
        Decimal::from(12),
        Decimal::from(3_300),
        Some(Money::from("0.04 RUB")),
    )
    .unwrap()
    .unwrap();
    assert_eq!(next.quantity, Quantity::from(2));
    assert_eq!(next.commission, TbankFillCommission::Unknown);
}

#[test]
fn repeated_cumulative_snapshot_resolves_a_partially_matched_operation_commission() {
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    let fill_projection = Arc::new(Mutex::new(TbankFillProjection::default()));
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();

    project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        None,
    )
    .unwrap()
    .unwrap();

    let report = FillReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        "broker-order-1".into(),
        "operations-trade-1".into(),
        OrderSide::Buy,
        Quantity::from(2),
        Price::from("275"),
        Money::from("0.02 RUB"),
        LiquiditySide::NoLiquiditySide,
        None,
        None,
        UnixNanos::from(1),
        UnixNanos::from(2),
        Some(UUID4::new()),
    );
    assert!(
        project_managed_trade_fill_report(
            &broker_order_index,
            &fill_projection,
                TbankFillReport::new(
                report,
                TbankFillCommission::Reported(Money::from("0.02 RUB")),
                    TbankFillCommissionSource::OperationsCursor,
                ),
                Some(&sender),
        )
        .unwrap()
        .is_none()
    );

    // A repeated snapshot at the same quantity carries the full cumulative commission. The
    // partially resolved synthetic fill is the sole unresolved entry, so the residual for its
    // eight unresolved lots completes the record instead of being dropped.
    let correction = project_cumulative_order_fill(
        &fill_projection,
        "broker-order-1",
        "synthetic-order-state-10",
        Decimal::from(10),
        Decimal::from(2_750),
        Some(Money::from("0.04 RUB")),
    )
    .unwrap()
    .expect("the cumulative snapshot corrects the partially resolved fill");
    assert!(correction.provenance_only);
    assert_eq!(
        correction.trade_id.as_deref(),
        Some("synthetic-order-state-10")
    );
    assert_eq!(correction.quantity, Quantity::from(10));
    assert_eq!(
        correction.commission,
        TbankFillCommission::Allocated(Money::from("0.04 RUB"))
    );
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
    let _data_event_receiver = bind_test_data_event_sender(&client.runtime);
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
async fn submit_stop_order_timeout_stays_unknown_when_snapshot_attribution_is_ambiguous() {
    let service = MockStopOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let mut first_candidate = active_sber_stop_order("stop-order-1");
    first_candidate.exchange_order_type = ExchangeOrderType::Market as i32;
    let mut second_candidate = active_sber_stop_order("stop-order-2");
    second_candidate.exchange_order_type = ExchangeOrderType::Market as i32;
    *service.get_responses.lock().unwrap() = VecDeque::from([
        GetStopOrdersResponse::default(),
        GetStopOrdersResponse {
            stop_orders: vec![first_candidate, second_candidate],
        },
    ]);
    let post_calls = service.post_calls.clone();
    let get_calls = service.get_calls.clone();
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
        .expect("ambiguous stop snapshot must remain an unresolved outcome");
    assert!(reports.is_empty());

    let post_calls = post_calls.lock().unwrap();
    assert_eq!(post_calls.len(), 1);
    let get_calls = get_calls.lock().unwrap();
    assert_eq!(get_calls.len(), 2);
    assert_eq!(
        get_calls[0].status,
        StopOrderStatusOption::StopOrderStatusActive as i32
    );
    assert!(get_calls[0].from.is_none());
    assert_eq!(
        get_calls[1].status,
        StopOrderStatusOption::StopOrderStatusAll as i32
    );
    assert!(get_calls[1].from.is_some());
    assert!(get_calls[1].to.is_some());
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
    assert_eq!(
        client.runtime.known_broker_order_identity(
            Some(&ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156")),
            None
        ),
        None
    );
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .route_for_client_order_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        Some(TbankBrokerOrderRoute::StopOrder)
    );
}

#[tokio::test]
async fn stop_order_submit_missing_broker_identity_remains_unknown() {
    let service = MockStopOrdersService::default();
    *service.post_response.lock().unwrap() = Some(PostStopOrderResponse {
        order_request_id: "missing-stop-order-id".to_string(),
        ..PostStopOrderResponse::default()
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
        .expect("missing stop_order_id must remain recoverable");
    assert!(reports.is_empty());
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
}

#[tokio::test]
async fn submit_stop_order_unknown_outcome_recovers_in_background() {
    let service = MockStopOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::Unavailable, "submit response lost".to_string()));
    let mut recovered_stop = active_sber_stop_order("stop-order-1");
    recovered_stop.exchange_order_type = ExchangeOrderType::Market as i32;
    *service.get_responses.lock().unwrap() = VecDeque::from([
        GetStopOrdersResponse::default(),
        GetStopOrdersResponse::default(),
        GetStopOrdersResponse {
            stop_orders: vec![recovered_stop],
        },
    ]);
    let post_calls = Arc::clone(&service.post_calls);
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

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let cmd = submit_stop_order_cmd();
    let reports = submit_nautilus_order_reports_with_recovery(
        &mut client.runtime,
        &cmd,
        current_unix_nanos(),
        Some(test_emitter(sender)),
    )
    .await
    .expect("unknown stop submit should enter bounded recovery");
    assert!(matches!(
        reports,
        super::submit::SubmitPipelineOutcome::Reports(reports) if reports.is_empty()
    ));
    assert_eq!(post_calls.lock().unwrap().len(), 1);
    {
        let get_calls = get_calls.lock().unwrap();
        assert!(get_calls.len() >= 2);
        assert_eq!(
            get_calls[0].status,
            StopOrderStatusOption::StopOrderStatusActive as i32
        );
        assert!(get_calls[0].from.is_none());
        assert_eq!(
            get_calls[1].status,
            StopOrderStatusOption::StopOrderStatusAll as i32
        );
        assert!(get_calls[1].from.is_some());
        assert!(get_calls[1].to.is_some());
    }
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected recovered stop-order status report");
    };
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        client
            .runtime
            .pending_submits
            .lock()
            .unwrap()
            .get("524b1a03-efdd-4cd0-bd56-7cc6570c7156")
            .unwrap()
            .stage,
        TbankPendingSubmitStage::Accepted
    );
}

#[tokio::test]
async fn submit_stop_order_response_returns_accepted_without_preflight_query() {
    let service = MockStopOrdersService::default();
    let post_calls = Arc::clone(&service.post_calls);
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
    assert_eq!(post_calls.lock().unwrap().len(), 1);
    assert_eq!(get_calls.lock().unwrap().len(), 0);
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
    seed_sber_metadata(&mut client);
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
    let _data_event_receiver = bind_test_data_event_sender(&client.runtime);
    seed_sber_metadata(&mut client);
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
    seed_sber_metadata(&mut client);
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
