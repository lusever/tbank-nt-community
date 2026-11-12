async fn async_submit_client(service: MockOrdersService) -> super::TbankExecutionClient {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                crate::grpc::generated::users_service_server::UsersServiceServer::new(
                    crate::testing::QuotaUsersService,
                ),
            )
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".into()),
        account_id: Some("account-1".into()),
        endpoint: Some(format!("http://{addr}")),
        enable_trading: true,
        allow_live_trading: true,
        ..Default::default()
    });
    client
        .runtime
        .connect(&mut client.task_owner)
        .await
        .unwrap();
    client
}
#[tokio::test]
async fn closed_execution_channel_prevents_single_and_list_broker_submits() {
    let service = MockOrdersService::default();
    let sync_calls = service.calls.clone();
    let async_calls = service.async_calls.clone();
    let mut client = async_submit_client(service).await;
    client.runtime.instruments.lock().unwrap()
        .insert("SBER_TQBR.MOEX".to_string(), sber_metadata());
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    drop(receiver);
    client.runtime.emitter.set_sender(sender);

    let single = submit_order_cmd(None);
    let first = single.order_init.clone();
    let mut second = first.clone();
    second.client_order_id = ClientOrderId::from("order-list-second");
    let ids = [first.client_order_id, second.client_order_id];
    nautilus_common::clients::ExecutionClient::submit_order(&client, single).unwrap();
    wait_for_closed_channel_submit_tasks(&client).await;

    nautilus_common::clients::ExecutionClient::submit_order_list(
        &client,
        submit_order_list_cmd(vec![first, second]),
    ).unwrap();
    wait_for_closed_channel_submit_tasks(&client).await;

    assert!(sync_calls.lock().unwrap().is_empty());
    assert!(async_calls.lock().unwrap().is_empty());
    assert!(client.unresolved_submits().is_empty());
    let index = client.runtime.broker_order_index.lock().unwrap();
    for id in ids {
        assert!(index.route_for_client_order_id(id.as_str()).is_none());
    }
}

async fn wait_for_closed_channel_submit_tasks(client: &TbankExecutionClient) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !client.task_owner.operation_tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("submission task did not finish");
}

fn async_order(id: &str) -> TbankSubmitOrder {
    let mut order = pending_submit_order(id);
    order.submit_method = crate::execution::TbankSubmitMethod::PostOrderAsync;
    order.broker_request_id = tbank_broker_request_id_for_client_order_id(id);
    order.time_in_force = TimeInForce::Ioc;
    order.quantity_units = Decimal::from(20);
    order
}
#[tokio::test]
async fn independent_async_submits_can_wait_together_without_get_order_state() {
    let service = MockOrdersService::default();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *service.post_gate.lock().unwrap() = Some(MockPostOrderGate {
        started,
        release: release.clone(),
    });
    let calls = service.async_calls.clone();
    let states = service.state_calls.clone();
    let sync_calls = service.calls.clone();
    let client = async_submit_client(service).await;
    let mut first = client.runtime.clone();
    let mut second = client.runtime.clone();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let unblock = tokio::spawn(async move {
        tokio::time::timeout_at(deadline, async {
            while calls.lock().unwrap().len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release.notify_waiters();
    });
    let a = async_order("async-a");
    let b = async_order("async-b");
    let metadata = sber_metadata();
    let (a, b) = tokio::join!(
        first.submit_order_with_recovery_deadline(&a, &metadata, deadline),
        second.submit_order_with_recovery_deadline(&b, &metadata, deadline)
    );
    assert!(matches!(a.unwrap(), TbankSubmitResponse::Async(_)));
    assert!(matches!(b.unwrap(), TbankSubmitResponse::Async(_)));
    unblock.await.unwrap();
    assert!(states.lock().unwrap().is_empty());
    assert!(sync_calls.lock().unwrap().is_empty());
    let unresolved = client.unresolved_submits();
    assert_eq!(unresolved.len(), 2);
    assert!(unresolved.iter().all(|s| s.submit_method
        == crate::execution::TbankSubmitMethod::PostOrderAsync
        && s.trade_intent_id.as_deref() == Some("broker-intent-1")));
}
#[tokio::test]
async fn async_ack_after_exchange_mapping_never_replaces_venue_id_with_intent_id() {
    let service = MockOrdersService::default();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *service.post_gate.lock().unwrap() = Some(MockPostOrderGate {
        started: started.clone(),
        release: release.clone(),
    });
    let client = async_submit_client(service).await;
    let mut rpc = client.runtime.clone();
    let mut observer = client.runtime.clone();
    let order = async_order("async-race");
    let task = tokio::spawn(async move {
        rpc.submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
        .await
        .expect("async RPC never started");
    observer.record_broker_order_mapping_and_notify_cancel(
        TbankBrokerOrderRoute::RegularOrder,
        "async-race",
        "exchange-123",
    );
    release.notify_one();
    assert!(matches!(
        task.await.unwrap().unwrap(),
        TbankSubmitResponse::Async(_)
    ));
    let index = observer.broker_order_index.lock().unwrap();
    assert_eq!(
        index
            .identity_for(Some("async-race"), None)
            .unwrap()
            .broker_order_id,
        "exchange-123"
    );
}
#[tokio::test]
async fn pending_async_cancel_and_recovery_use_request_uuid() {
    let service = MockOrdersService::default();
    let cancels = service.cancel_calls.clone();
    let states = service.state_calls.clone();
    let mut client = async_submit_client(service).await;
    let order = async_order("async-cancel");
    client
        .runtime
        .submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();
    let target = client
        .runtime
        .resolve_cancel_target(&order.client_order_id, None)
        .await
        .unwrap();
    let TbankCancelTarget::Ready(identity) = target else {
        panic!("request UUID must be immediately cancellable")
    };
    assert_eq!(identity.broker_order_id, order.broker_request_id);
    client
        .runtime
        .cancel_resolved_broker_order_admitted(identity)
        .await
        .unwrap();
    let _ = client
        .runtime
        .query_order_by_request_id(&order.broker_request_id)
        .await;
    assert_eq!(
        cancels.lock().unwrap()[0].order_id_type,
        Some(OrderIdType::Request as i32)
    );
    assert_eq!(
        states.lock().unwrap()[0].order_id_type,
        Some(OrderIdType::Request as i32)
    );
}
#[tokio::test]
async fn async_transport_timeout_is_ambiguous_and_retry_keeps_uuid() {
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() =
        Some((Code::DeadlineExceeded, "lost async acknowledgement".into()));
    let calls = service.async_calls.clone();
    let mut client = async_submit_client(service).await;
    let order = async_order("async-timeout");
    for _ in 0..2 {
        let error = client
            .runtime
            .submit_order_with_recovery_deadline(
                &order,
                &sber_metadata(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            super::classify_command_failure(&error),
            nautilus_live::execution::failure::CommandFailure::Ambiguous(_)
        ));
    }
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].order_id, calls[1].order_id);
}

#[tokio::test]
async fn quota_expiry_is_not_sent_and_does_not_leave_an_unknown_order() {
    use crate::grpc::limiter::{TbankRateLimiter, TbankRpcQuota};
    let service = MockOrdersService::default();
    let calls = service.async_calls.clone();
    let mut client = async_submit_client(service).await;
    let limiter = Arc::new(
        TbankRateLimiter::new(
            vec![TbankRpcQuota {
                methods: vec![],
                capacity: 1,
                window: std::time::Duration::from_secs(1),
            }],
            4,
        )
        .unwrap(),
    );
    limiter
        .reserve(
            "PostOrderAsync",
            false,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    client.runtime.config.quota_scope = Some(limiter.clone());
    let creds = client.runtime.active_credentials.clone().unwrap();
    let channel = crate::grpc::connect_channel(
        &client.runtime.config.endpoint_uri().unwrap(),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    client.runtime.clients = Some(crate::grpc::TbankGrpcClients::new_with_quota_scope(
        channel,
        crate::grpc::TbankAuthInterceptor::new(creds).unwrap(),
        Some(limiter),
    ));
    let order = async_order("quota-expired");
    let error = client
        .runtime
        .submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_millis(15),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        super::classify_command_failure(&error),
        nautilus_live::execution::failure::CommandFailure::NotSent(_)
    ));
    assert!(calls.lock().unwrap().is_empty());
    assert!(client.unresolved_submits().is_empty());
}

#[test]
fn typed_submit_options_reject_malformed_values_and_expired_deadlines() {
    use crate::execution::{TBANK_SUBMIT_METHOD_PARAM, TbankSubmitMethod, TbankSubmitOptions};
    for value in [
        serde_json::json!(false),
        serde_json::json!("automatic"),
        serde_json::json!(null),
    ] {
        let params = Params::new([(TBANK_SUBMIT_METHOD_PARAM.into(), value)].into());
        assert!(TbankSubmitOptions::from_params(Some(&params)).is_err());
    }
    let mut params = Params::new([("consumer".into(), serde_json::json!("preserved"))].into());
    TbankSubmitOptions {
        method: Some(TbankSubmitMethod::PostOrderAsync),
        deadline: Some(UnixNanos::from(1)),
        confirm_margin_trade: Some(true),
    }
    .apply(&mut params);
    let decoded = TbankSubmitOptions::from_params(Some(&params)).unwrap();
    assert_eq!(decoded.method, Some(TbankSubmitMethod::PostOrderAsync));
    assert_eq!(params.get_str("consumer"), Some("preserved"));
    assert!(decoded.monotonic_deadline().is_err());
}

#[test]
fn typed_submit_options_preserve_implicit_method_selection() {
    use crate::execution::{TBANK_SUBMIT_METHOD_PARAM, TbankSubmitOptions};
    let mut params = Params::empty();
    TbankSubmitOptions {
        method: None,
        deadline: Some(UnixNanos::from(10)),
        confirm_margin_trade: Some(true),
    }
    .apply(&mut params);
    assert!(params.get(TBANK_SUBMIT_METHOD_PARAM).is_none());
    let decoded = TbankSubmitOptions::from_params(Some(&params)).unwrap();
    assert_eq!(decoded.method, None);
    assert_eq!(decoded.confirm_margin_trade, Some(true));
}

#[test]
fn fresh_runtime_restores_uuid_and_pending_cancel_from_nautilus_cache() {
    use nautilus_model::{
        events::{OrderPendingCancel, OrderSubmitted},
        orders::{Order, OrderAny},
    };
    let command = submit_order_cmd(None);
    let mut order: OrderAny = command.order_init.try_into().unwrap();
    let account = tbank_account_id("account-1");
    order
        .apply(OrderEventAny::Submitted(OrderSubmitted::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            account,
            UUID4::new(),
            UnixNanos::from(2),
            UnixNanos::from(2),
        )))
        .unwrap();
    order
        .apply(OrderEventAny::PendingCancel(OrderPendingCancel::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            Some(account),
            UUID4::new(),
            UnixNanos::from(3),
            UnixNanos::from(3),
            false,
            None,
        )))
        .unwrap();
    let restored: OrderAny = serde_json::from_value(serde_json::to_value(&order).unwrap()).unwrap();
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_order(restored, None, Some(ClientId::from("TBANK")), false)
        .unwrap();
    let core = ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("TBANK"),
        Venue::from("MOEX"),
        OmsType::Netting,
        account,
        AccountType::Margin,
        None,
        cache,
    );
    let mut client = TbankExecutionClient::new(
        core,
        TbankExecutionClientConfig {
            account_id: Some("account-1".into()),
            ..Default::default()
        },
    );
    let cancels = client.restore_cached_execution_intents().unwrap();
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].client_order_id, order.client_order_id());
    assert!(cancels[0].venue_order_id.is_none());
    assert_eq!(
        client.runtime.unresolved_regular_request_mappings(),
        vec![(
            order.client_order_id().to_string(),
            tbank_broker_request_id_for_client_order_id(order.client_order_id().as_str())
        )]
    );
}

#[tokio::test]
async fn cancel_reconciliation_preserves_partial_fill_before_terminal_state() {
    let service = MockOrdersService::default();
    let metadata = sber_metadata();
    let order = async_order("cancel-with-fill");
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-with-fill".into(),
        order_request_id: order.broker_request_id.clone(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusCancelled as i32,
        lots_requested: 2,
        lots_executed: 1,
        order_type: crate::grpc::generated::OrderType::Limit as i32,
        direction: OrderDirection::Buy as i32,
        instrument_uid: metadata.instrument_uid.clone(),
        executed_order_price: Some(MoneyValue {
            currency: "rub".into(),
            units: 1230,
            nano: 0,
        }),
        initial_security_price: Some(MoneyValue {
            currency: "rub".into(),
            units: 123,
            nano: 0,
        }),
        ..Default::default()
    });
    let mut client = async_submit_client(service).await;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client
        .runtime
        .get_or_allocate_broker_request_id(&order.client_order_id)
        .unwrap();
    client
        .runtime
        .record_broker_order_mapping_and_notify_cancel(
            TbankBrokerOrderRoute::RegularOrder,
            &order.client_order_id,
            "exchange-with-fill",
        );
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    let _provenance = bind_test_data_event_sender(&client.runtime);
    assert!(
        client
            .runtime
            .reconcile_cancel_after_terminal_response(&TbankBrokerOrderIdentity {
                route: TbankBrokerOrderRoute::RegularOrder,
                broker_order_id: "exchange-with-fill".into()
            })
            .await
            .unwrap()
    );
    let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) =
        receiver.try_recv().unwrap()
    else {
        panic!("cancel reconciliation must publish state and fills together")
    };
    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(10));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].last_qty.as_decimal(), Decimal::from(10));
}

#[tokio::test]
async fn not_yet_indexed_request_cancel_retains_intent_and_retry_prefers_exchange_id() {
    let service = MockOrdersService::default();
    let error_control = service.cancel_error.clone();
    let calls = service.cancel_calls.clone();
    *error_control.lock().unwrap() = Some((Code::NotFound, "50005".into()));
    let mut client = async_submit_client(service).await;
    let order = async_order("cancel-before-indexed");
    client.runtime.prepare_submit_route(
        &ClientOrderId::from(order.client_order_id.as_str()),
        OrderType::Market,
    );
    assert!(matches!(
        client
            .runtime
            .resolve_cancel_target(&order.client_order_id, None)
            .await
            .unwrap(),
        TbankCancelTarget::Ready(_)
    ));
    let identity = TbankBrokerOrderIdentity {
        route: TbankBrokerOrderRoute::RegularOrder,
        broker_order_id: order.broker_request_id.clone(),
    };
    let error = client
        .runtime
        .cancel_resolved_broker_order_admitted(identity.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        super::classify_command_failure(&error),
        nautilus_live::execution::failure::CommandFailure::Ambiguous(_)
    ));
    assert!(client.runtime.cancellation_is_unresolved(&identity));
    client
        .runtime
        .submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();
    client
        .runtime
        .record_broker_order_mapping_and_notify_cancel(
            TbankBrokerOrderRoute::RegularOrder,
            &order.client_order_id,
            "exchange-indexed",
        );
    *error_control.lock().unwrap() = None;
    client
        .runtime
        .cancel_resolved_broker_order_admitted(identity)
        .await
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].order_id, order.broker_request_id);
    assert_eq!(calls[0].order_id_type, Some(OrderIdType::Request as i32));
    assert_eq!(calls[1].order_id, "exchange-indexed");
    assert_eq!(calls[1].order_id_type, Some(OrderIdType::Exchange as i32));
}

#[test]
fn terminal_report_settles_request_uuid_cancel_alias() {
    let client = test_client(TbankExecutionClientConfig::default());
    let id = "cancel-alias";
    let request = tbank_broker_request_id_for_client_order_id(id);
    let identity = TbankBrokerOrderIdentity {
        route: TbankBrokerOrderRoute::RegularOrder,
        broker_order_id: request,
    };
    client
        .runtime
        .unresolved_cancellations
        .lock()
        .unwrap()
        .insert(identity.clone());
    let report = OrderStatusReport::new(
        AccountId::from("TBANK-account-1"),
        InstrumentId::from("SBER_TQBR.MOEX"),
        Some(ClientOrderId::from(id)),
        VenueOrderId::from("exchange-closed"),
        Some(OrderSide::Buy),
        OrderType::Limit,
        TimeInForce::Day,
        OrderStatus::Canceled,
        Quantity::from(20),
        Quantity::from(10),
        UnixNanos::from(1),
        UnixNanos::from(2),
        UnixNanos::from(3),
        None,
    );
    settle_order_report_mutation_state(
        &client.runtime.pending_submits,
        &client.runtime.unresolved_cancellations,
        &client.runtime.broker_order_index,
        &report,
    );
    assert!(!client.runtime.cancellation_is_unresolved(&identity));
}

#[tokio::test]
async fn a_not_sent_retry_does_not_reject_an_earlier_unknown_attempt() {
    use crate::grpc::limiter::{TbankRateLimiter, TbankRpcQuota};
    let service = MockOrdersService::default();
    *service.post_error.lock().unwrap() = Some((Code::DeadlineExceeded, "lost response".into()));
    let calls = service.async_calls.clone();
    let mut client = async_submit_client(service).await;
    let order = async_order("unknown-before-retry");
    let first = client
        .runtime
        .submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        super::classify_command_failure(&first),
        nautilus_live::execution::failure::CommandFailure::Ambiguous(_)
    ));
    let limiter = Arc::new(
        TbankRateLimiter::new(
            vec![TbankRpcQuota {
                methods: vec![],
                capacity: 1,
                window: std::time::Duration::from_secs(1),
            }],
            4,
        )
        .unwrap(),
    );
    limiter
        .reserve(
            "PostOrderAsync",
            false,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    let channel = crate::grpc::connect_channel(
        &client.runtime.config.endpoint_uri().unwrap(),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    client.runtime.clients = Some(crate::grpc::TbankGrpcClients::new_with_quota_scope(
        channel,
        crate::grpc::TbankAuthInterceptor::new(client.runtime.active_credentials.clone().unwrap())
            .unwrap(),
        Some(limiter),
    ));
    let retry = client
        .runtime
        .submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_millis(15),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        super::classify_command_failure(&retry),
        nautilus_live::execution::failure::CommandFailure::NotSent(_)
    ));
    assert_eq!(calls.lock().unwrap().len(), 1);
    let unknown = client.unresolved_submits();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].broker_request_id, order.broker_request_id);
    assert!(unknown[0].stage.blocks_lifecycle_reset());
}

#[tokio::test]
async fn lost_async_reply_restores_from_cached_submitted_order_without_resubmission() {
    use nautilus_model::{
        events::OrderSubmitted,
        orders::{Order, OrderAny},
    };
    let mut command = submit_order_cmd(None);
    command.order_init.quantity = Quantity::from(20);
    command.order_init.time_in_force = TimeInForce::Ioc;
    let id = command.client_order_id;
    let order = async_order(id.as_str());
    let service = MockOrdersService::default();
    let calls = service.async_calls.clone();
    let state_calls = service.state_calls.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *service.post_gate.lock().unwrap() = Some(MockPostOrderGate {
        started: started.clone(),
        release: release.clone(),
    });
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-after-process-loss".into(),
        order_request_id: order.broker_request_id.clone(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        lots_executed: 0,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: sber_metadata().instrument_uid,
        ..Default::default()
    });
    let original = async_submit_client(service).await;
    let config = original.runtime.config.clone();
    // The runtime removes credentials from config; preserve this synthetic fixture credential.
    let config = TbankExecutionClientConfig {
        token: Some("test-token".into()),
        ..config
    };
    let mut rpc = original.runtime.clone();
    let task = tokio::spawn(async move {
        rpc.submit_order_with_recovery_deadline(
            &order,
            &sber_metadata(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let account = tbank_account_id("account-1");
    let mut cached: OrderAny = command.order_init.try_into().unwrap();
    cached
        .apply(OrderEventAny::Submitted(OrderSubmitted::new(
            cached.trader_id(),
            cached.strategy_id(),
            cached.instrument_id(),
            id,
            account,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(1),
        )))
        .unwrap();
    let persisted = serde_json::to_value(&cached).unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(original);
    let restored: OrderAny = serde_json::from_value(persisted).unwrap();
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_order(restored, None, Some(ClientId::from("TBANK")), false)
        .unwrap();
    let core = ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("TBANK"),
        Venue::from("MOEX"),
        OmsType::Netting,
        account,
        AccountType::Margin,
        None,
        cache,
    );
    let mut fresh = TbankExecutionClient::new(core, config);
    fresh
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert("SBER_TQBR.MOEX".into(), sber_metadata());
    assert!(fresh.restore_cached_execution_intents().unwrap().is_empty());
    fresh.connect_for_queries().await.unwrap();
    let report = ExecutionClient::generate_order_status_report(
        &fresh,
        &nautilus_common::messages::execution::GenerateOrderStatusReport::new(
            UUID4::new(),
            UnixNanos::from(2),
            Some(InstrumentId::from("SBER_TQBR.MOEX")),
            Some(id),
            None,
            None,
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        report.venue_order_id.as_str(),
        "exchange-after-process-loss"
    );
    assert_eq!(report.client_order_id, Some(id));
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "recovery must not re-post an order"
    );
    {
        let queries = state_calls.lock().unwrap();
        assert_eq!(
            queries.last().unwrap().order_id,
            tbank_broker_request_id_for_client_order_id(id.as_str())
        );
        assert_eq!(
            queries.last().unwrap().order_id_type,
            Some(OrderIdType::Request as i32)
        );
    }
    release.notify_waiters();
    fresh.disconnect_async().await.unwrap();
}
