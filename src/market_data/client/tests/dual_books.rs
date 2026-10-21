type BookStreamSender =
    tokio::sync::mpsc::UnboundedSender<std::result::Result<MarketDataResponse, tonic::Status>>;
type RecordedBookStream = (Vec<OrderBookInstrument>, BookStreamSender);

#[derive(Clone, Default)]
struct MockBookStreams {
    calls: Arc<Mutex<Vec<Vec<OrderBookInstrument>>>>,
    senders: Arc<Mutex<Vec<RecordedBookStream>>>,
}
#[tonic::async_trait]
impl crate::grpc::generated::market_data_stream_service_server::MarketDataStreamService
    for MockBookStreams
{
    type MarketDataStreamStream = tokio_stream::wrappers::UnboundedReceiverStream<
        std::result::Result<MarketDataResponse, tonic::Status>,
    >;
    type MarketDataServerSideStreamStream = Self::MarketDataStreamStream;
    async fn market_data_stream(
        &self,
        _: tonic::Request<tonic::Streaming<MarketDataRequest>>,
    ) -> std::result::Result<tonic::Response<Self::MarketDataStreamStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn market_data_server_side_stream(
        &self,
        request: tonic::Request<MarketDataServerSideStreamRequest>,
    ) -> std::result::Result<tonic::Response<Self::MarketDataServerSideStreamStream>, tonic::Status>
    {
        use crate::grpc::generated::{OrderBookSubscription, SubscribeOrderBookResponse};
        let instrument_list = request
            .into_inner()
            .subscribe_order_book_request
            .unwrap()
            .instruments;
        self.calls.lock().unwrap().push(instrument_list.clone());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(Ok(MarketDataResponse {
                payload: Some(market_data_response::Payload::SubscribeOrderBookResponse(
                    SubscribeOrderBookResponse {
                        order_book_subscriptions: instrument_list
                            .iter()
                            .map(|instrument| OrderBookSubscription {
                                instrument_uid: instrument.instrument_id.clone(),
                                depth: instrument.depth,
                                order_book_type: instrument.order_book_type,
                                subscription_status: SubscriptionStatus::Success as i32,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }))
            .unwrap();
        for instrument in &instrument_list {
            sender
                .send(Ok(MarketDataResponse {
                    payload: Some(market_data_response::Payload::Orderbook(OrderBook {
                        instrument_uid: instrument.instrument_id.clone(),
                        depth: instrument.depth,
                        order_book_type: instrument.order_book_type,
                        time: Some(ts(100)),
                        is_consistent: true,
                        bids: vec![Order {
                            price: q(100, 1),
                            quantity: 5,
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }))
                .unwrap();
        }
        self.senders.lock().unwrap().push((instrument_list, sender));
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver),
        ))
    }
}
async fn next_book_group_connected(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut reconnecting = false;
        loop {
            if let DataEvent::Data(Data::Custom(custom)) = receiver.recv().await.unwrap()
                && let Some(TbankMarketDataEvent::StreamState { stream_id, state, .. }) =
                    custom.data.as_any().downcast_ref::<TbankMarketDataEvent>()
            {
                assert_eq!(stream_id, TbankBookSubscription::STREAM_ID);
                reconnecting |= *state == TbankMarketDataStreamState::Reconnecting;
                assert_ne!(*state, TbankMarketDataStreamState::Dead);
                if *state == TbankMarketDataStreamState::Connected {
                    assert!(reconnecting, "group must remain unready before Connected");
                    break;
                }
            }
        }
    })
    .await
    .expect("book group did not become connected");
}

async fn next_custom_book(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> TbankOrderBookSnapshot {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let DataEvent::Data(Data::Custom(c)) =
                receiver.recv().await.expect("data channel closed")
                && let Some(book) = c.data.as_any().downcast_ref::<TbankOrderBookSnapshot>()
            {
                return book.clone();
            }
        }
    })
    .await
    .expect("custom book not delivered")
}

#[test]
fn custom_book_group_rejects_subscriptions_over_the_stream_limit() {
    use crate::market_data::TbankBookSource;
    use nautilus_common::messages::data::SubscribeCustomData;

    let mut client = TbankDataClient::new(TbankDataClientConfig::default());
    client.custom_book_subscriptions = (0..MAX_SUBSCRIPTIONS_PER_STREAM)
        .map(|index| {
            TbankBookSubscription::new(
                InstrumentId::from(format!("TEST{index:03}.MOEX")),
                TbankBookSource::Exchange,
                10,
            )
            .unwrap()
        })
        .collect();
    let previous_subscriptions = client.custom_book_subscriptions.clone();
    let rejected = TbankBookSubscription::new(
        InstrumentId::from("OVER_LIMIT.MOEX"),
        TbankBookSource::Exchange,
        10,
    )
    .unwrap();
    let command = SubscribeCustomData::new(
        Some(client.client_id),
        None,
        rejected.data_type(),
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    let error = DataClient::subscribe(&mut client, command).unwrap_err();

    assert!(error.to_string().contains("300"));
    assert_eq!(client.custom_book_subscriptions, previous_subscriptions);
}

#[tokio::test]
async fn grpc_dual_books_share_one_stream_and_reconnect_as_a_group() {
    use crate::{
        grpc::generated::market_data_stream_service_server::MarketDataStreamServiceServer,
        market_data::TbankBookSource,
    };
    use nautilus_common::messages::data::{SubscribeCustomData, UnsubscribeCustomData};
    let service = MockBookStreams::default();
    let observer = service.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                crate::grpc::generated::users_service_server::UsersServiceServer::new(
                    crate::testing::QuotaUsersService,
                ),
            )
            .add_service(MarketDataStreamServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::set_data_event_sender(sender);
    let credentials = Arc::new(SecretString::from("book-test-token"));
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = TbankDataClient::new(TbankDataClientConfig {
        endpoint: Some(endpoint),
        token: Some(SecretString::from("book-test-token")),
        reconnect_policy: crate::config::TbankReconnectPolicy {
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            jitter: false,
            ..Default::default()
        },
        ..Default::default()
    });
    client.connection = Some(TbankDataConnection::Live(TbankGrpcClients::new(
        channel,
        TbankAuthInterceptor::new(credentials.clone()).unwrap(),
    )));
    client.active_credentials = Some(credentials.clone());
    let exchange = TbankBookSubscription::new(sber_id(), TbankBookSource::Exchange, 10).unwrap();
    let dealer = TbankBookSubscription::new(sber_id(), TbankBookSource::Dealer, 10).unwrap();
    for key in [exchange, dealer] {
        client
            .subscribe(SubscribeCustomData::new(
                Some(client.client_id),
                None,
                key.data_type(),
                nautilus_core::UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
    }
    next_book_group_connected(&mut receiver).await;
    let initial_sources = [
        next_custom_book(&mut receiver).await.source,
        next_custom_book(&mut receiver).await.source,
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    assert_eq!(
        initial_sources,
        HashSet::from([TbankBookSource::Exchange, TbankBookSource::Dealer])
    );
    assert_eq!(observer.calls.lock().unwrap().len(), 1);
    let group_sender = observer.senders.lock().unwrap().last().unwrap().1.clone();
    let active_task_key = client
        .active_stream_task_keys
        .get(&TbankDataClient::custom_book_task_key())
        .unwrap()
        .clone();
    let old_generation_instruments = observer.calls.lock().unwrap()[0].clone();
    client.active_credentials = None;
    let rejected_replacement =
        TbankBookSubscription::new(sber_id(), TbankBookSource::Exchange, 20).unwrap();
    let client_id = client.client_id;
    let error = DataClient::subscribe(
        &mut client,
        SubscribeCustomData::new(
            Some(client_id),
            None,
            rejected_replacement.data_type(),
            nautilus_core::UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )
    .unwrap_err();
    assert!(error.to_string().contains("credentials are not connected"));
    assert_eq!(client.custom_book_subscriptions, HashSet::from([exchange, dealer]));
    let unsubscribe = UnsubscribeCustomData::new(
        Some(client_id),
        None,
        exchange.data_type(),
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let error = DataClient::unsubscribe(&mut client, &unsubscribe).unwrap_err();
    assert!(error.to_string().contains("credentials are not connected"));
    assert_eq!(client.custom_book_subscriptions, HashSet::from([exchange, dealer]));
    assert_eq!(
        client
            .active_stream_task_keys
            .get(&TbankDataClient::custom_book_task_key()),
        Some(&active_task_key)
    );
    assert!(!client.stream_tasks[&active_task_key]
        .cancellation
        .is_cancelled());
    assert_eq!(observer.calls.lock().unwrap().len(), 1);
    client.active_credentials = Some(credentials);
    let first_book_instrument = old_generation_instruments[0].clone();
    group_sender
        .send(Ok(MarketDataResponse {
            payload: Some(market_data_response::Payload::Orderbook(OrderBook {
                instrument_uid: first_book_instrument.instrument_id,
                depth: first_book_instrument.depth,
                order_book_type: first_book_instrument.order_book_type,
                time: Some(ts(101)),
                is_consistent: true,
                bids: vec![Order {
                    price: q(101, 1),
                    quantity: 6,
                }],
                ..Default::default()
            })),
            ..Default::default()
        }))
        .unwrap();
    let still_active_book = next_custom_book(&mut receiver).await;
    assert_eq!(still_active_book.depth, old_generation_instruments[0].depth);
    group_sender
        .send(Err(tonic::Status::unavailable(
            "controlled grouped-book disconnect",
        )))
        .unwrap();
    next_book_group_connected(&mut receiver).await;
    let reconnect_sources = [
        next_custom_book(&mut receiver).await.source,
        next_custom_book(&mut receiver).await.source,
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    assert_eq!(reconnect_sources, initial_sources);
    client
        .unsubscribe(&UnsubscribeCustomData::new(
            Some(client.client_id),
            None,
            exchange.data_type(),
            nautilus_core::UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    assert_eq!(client.custom_book_subscriptions, HashSet::from([dealer]));
    next_book_group_connected(&mut receiver).await;
    assert_eq!(
        next_custom_book(&mut receiver).await.source,
        TbankBookSource::Dealer
    );
    let remaining_instruments = observer.calls.lock().unwrap().last().unwrap().clone();
    assert_eq!(remaining_instruments.len(), 1);
    assert_eq!(
        remaining_instruments[0].order_book_type,
        dealer.source.broker_type()
    );
    assert!(
        client
            .active_stream_task_keys
            .contains_key(&TbankDataClient::custom_book_task_key())
    );
    // Rebuilding the desired snapshot after reconnect preserves exactly the remaining source.
    client.restore_market_data_streams().unwrap();
    next_book_group_connected(&mut receiver).await;
    assert_eq!(
        next_custom_book(&mut receiver).await.source,
        TbankBookSource::Dealer
    );
    assert!(
        observer
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.iter().all(|instrument| instrument.depth == 10))
    );
    client.disconnect_async().await.unwrap();
    server.abort();
    let _ = server.await;
}
