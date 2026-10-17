#[derive(Clone, Default)]
struct MockBookStreams {
    calls: Arc<Mutex<Vec<OrderBookInstrument>>>,
    senders: Arc<
        Mutex<
            Vec<(
                i32,
                tokio::sync::mpsc::UnboundedSender<
                    std::result::Result<MarketDataResponse, tonic::Status>,
                >,
            )>,
        >,
    >,
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
        let instrument = request
            .into_inner()
            .subscribe_order_book_request
            .unwrap()
            .instruments[0]
            .clone();
        self.calls.lock().unwrap().push(instrument.clone());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(Ok(MarketDataResponse {
                payload: Some(market_data_response::Payload::SubscribeOrderBookResponse(
                    SubscribeOrderBookResponse {
                        order_book_subscriptions: vec![OrderBookSubscription {
                            instrument_uid: instrument.instrument_id.clone(),
                            depth: instrument.depth,
                            order_book_type: instrument.order_book_type,
                            subscription_status: SubscriptionStatus::Success as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }))
            .unwrap();
        sender
            .send(Ok(MarketDataResponse {
                payload: Some(market_data_response::Payload::Orderbook(OrderBook {
                    instrument_uid: instrument.instrument_id,
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
        self.senders
            .lock()
            .unwrap()
            .push((instrument.order_book_type, sender));
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver),
        ))
    }
}
async fn next_custom_book(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> TbankOrderBookSnapshot {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let DataEvent::Data(Data::Custom(c)) =
                receiver.recv().await.expect("data channel closed")
            {
                if let Some(book) = c.data.as_any().downcast_ref::<TbankOrderBookSnapshot>() {
                    return book.clone();
                }
            }
        }
    })
    .await
    .expect("custom book not delivered")
}
#[tokio::test]
async fn grpc_dual_books_reconnect_and_unsubscribe_are_independent() {
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
    client.active_credentials = Some(credentials);
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
    let a = next_custom_book(&mut receiver).await;
    let b = next_custom_book(&mut receiver).await;
    assert_ne!(a.source, b.source);
    let dealer_sender = observer
        .senders
        .lock()
        .unwrap()
        .iter()
        .find(|(s, _)| *s == dealer.source.broker_type())
        .unwrap()
        .1
        .clone();
    dealer_sender
        .send(Err(tonic::Status::unavailable(
            "controlled dealer disconnect",
        )))
        .unwrap();
    assert_eq!(
        next_custom_book(&mut receiver).await.source,
        TbankBookSource::Dealer
    );
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
    assert!(
        client
            .active_stream_task_keys
            .contains_key(&TbankDataClient::custom_book_task_key(dealer))
    );
    assert!(
        !client
            .active_stream_task_keys
            .contains_key(&TbankDataClient::custom_book_task_key(exchange))
    );
    // Rebuilding the desired snapshot after reconnect preserves exactly the remaining source.
    client.restore_market_data_streams().unwrap();
    assert_eq!(
        next_custom_book(&mut receiver).await.source,
        TbankBookSource::Dealer
    );
    assert!(observer.calls.lock().unwrap().iter().all(|r| r.depth == 10));
    client.disconnect_async().await.unwrap();
    server.abort();
    let _ = server.await;
}
