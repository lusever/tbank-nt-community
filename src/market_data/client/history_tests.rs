use super::*;
use crate::grpc::generated::{
    GetCandlesRequest, GetCandlesResponse, GetLastTradesRequest, GetLastTradesResponse,
    HistoricCandle, InstrumentRequest, Quotation, RealExchange, Share, ShareResponse, Trade,
};
use crate::{
    config::TbankCandleTimestampMode,
    grpc::limiter::{TbankRateLimiter, TbankRpcQuota},
};
use nautilus_common::{
    cache::Cache,
    clock::VirtualClock,
    messages::data::{DataResponseEnvelope, RequestCommand},
    msgbus::{MessageBus, stubs::get_typed_message_saving_handler},
};
use nautilus_core::{Params, UUID4};
use nautilus_data::{client::DataClientAdapter, engine::DataEngine};
use nautilus_model::data::{BarSpecification, BarType};
use nautilus_model::{
    enums::{AggregationSource, PriceType},
    identifiers::TraderId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use std::{
    cell::RefCell,
    future::{Ready, ready},
    num::NonZeroUsize,
    rc::Rc,
    result::Result,
};
use tonic::{
    codegen::{Body, BoxFuture, Service, StdError},
    server::{Grpc, NamedService, UnaryService},
};

#[derive(Clone)]
struct HistoricalRpc<const MARKET: bool>(Arc<MockHistory>);

#[derive(Default)]
struct MockHistory {
    share_calls: AtomicU32,
    candle_calls: Mutex<Vec<GetCandlesRequest>>,
    trade_calls: Mutex<Vec<GetLastTradesRequest>>,
    candles: CandleReply,
    trades: TradeReply,
}

#[derive(Default, Clone, Copy)]
enum CandleReply {
    #[default]
    Empty,
    At(prost_types::Timestamp),
    Open,
    AtCap,
    FailSecond,
}

#[derive(Default)]
enum TradeReply {
    #[default]
    Empty,
    Batch,
    EmptySecond,
    FailSecond,
    WrongInstrumentSecond,
}

fn quotation() -> Quotation {
    Quotation {
        units: 100,
        nano: 0,
    }
}

impl MockHistory {
    fn share(&self, request: InstrumentRequest) -> ShareResponse {
        self.share_calls.fetch_add(1, Ordering::Relaxed);
        ShareResponse {
            instrument: Some(Share {
                uid: format!("{}-uid", request.id),
                ticker: request.id,
                class_code: "TQBR".into(),
                currency: "rub".into(),
                lot: 10,
                min_price_increment: Some(Quotation {
                    units: 0,
                    nano: 10_000_000,
                }),
                real_exchange: RealExchange::Moex as i32,
                ..Default::default()
            }),
        }
    }

    fn get_candles(&self, request: GetCandlesRequest) -> Result<GetCandlesResponse, tonic::Status> {
        let mut calls = self.candle_calls.lock().unwrap();
        let response = match self.candles {
            CandleReply::Empty => vec![],
            CandleReply::At(time)
                if timestamp_key(request.from.unwrap()) < timestamp_key(time)
                    && timestamp_key(time) < timestamp_key(request.to.unwrap()) =>
            {
                vec![candle(time, true)]
            }
            CandleReply::At(_) => vec![],
            CandleReply::Open => vec![candle(shift_timestamp(request.from.unwrap(), 1), false)],
            CandleReply::AtCap => vec![
                candle(shift_timestamp(request.from.unwrap(), 1), true);
                request.limit.unwrap() as usize
            ],
            CandleReply::FailSecond if calls.len().is_multiple_of(2) => {
                vec![candle(shift_timestamp(request.from.unwrap(), 1), true)]
            }
            CandleReply::FailSecond => {
                calls.push(request);
                return Err(tonic::Status::invalid_argument(
                    "second candle window failed",
                ));
            }
        };
        calls.push(request);
        Ok(GetCandlesResponse {
            candles: response,
            ..Default::default()
        })
    }

    fn get_trades(
        &self,
        request: GetLastTradesRequest,
    ) -> Result<GetLastTradesResponse, tonic::Status> {
        let mut calls = self.trade_calls.lock().unwrap();
        let uid = request.instrument_id.clone().unwrap();
        let time = request.from.unwrap();
        let end = request.to.unwrap();
        calls.push(request);
        let trade = |time, quantity| Trade {
            instrument_uid: uid.clone(),
            price: Some(quotation()),
            quantity,
            time: Some(time),
            ..Default::default()
        };
        let trades = match (&self.trades, uid.as_str()) {
            (TradeReply::Empty, _) | (TradeReply::EmptySecond, "GAZP-uid") => vec![],
            (TradeReply::FailSecond, "GAZP-uid") => {
                return Err(tonic::Status::unavailable("GAZP trade history failed"));
            }
            (TradeReply::WrongInstrumentSecond, "GAZP-uid") => vec![Trade {
                instrument_uid: "SBER-uid".into(),
                ..trade(time, 1)
            }],
            (_, "GAZP-uid") => vec![trade(time, 4)],
            _ => vec![
                trade(
                    prost_types::Timestamp {
                        seconds: end.seconds + 1,
                        ..end
                    },
                    5,
                ),
                trade(end, 3),
                trade(time, 1),
                trade(time, 2),
            ],
        };
        Ok(GetLastTradesResponse {
            trades,
            ..Default::default()
        })
    }
}

fn timestamp_key(time: prost_types::Timestamp) -> (i64, i32) {
    (time.seconds, time.nanos)
}

fn shift_timestamp(time: prost_types::Timestamp, nanos: i128) -> prost_types::Timestamp {
    unix_nanos_to_timestamp(crate::common::time::timestamp_to_unix_nanos(&time).unwrap() + nanos)
        .unwrap()
}

fn candle(time: prost_types::Timestamp, is_complete: bool) -> HistoricCandle {
    HistoricCandle {
        open: Some(quotation()),
        high: Some(quotation()),
        low: Some(quotation()),
        close: Some(quotation()),
        time: Some(time),
        is_complete,
        ..Default::default()
    }
}

impl<const MARKET: bool> NamedService for HistoricalRpc<MARKET> {
    const NAME: &'static str = if MARKET {
        "tinkoff.public.invest.api.contract.v1.MarketDataService"
    } else {
        "tinkoff.public.invest.api.contract.v1.InstrumentsService"
    };
}

struct Reply<F>(F);

impl<I, O, F> UnaryService<I> for Reply<F>
where
    F: FnMut(I) -> Result<O, tonic::Status>,
{
    type Response = O;
    type Future = Ready<Result<tonic::Response<O>, tonic::Status>>;

    fn call(&mut self, request: tonic::Request<I>) -> Self::Future {
        ready((self.0)(request.into_inner()).map(tonic::Response::new))
    }
}

impl<B, const MARKET: bool> Service<http::Request<B>> for HistoricalRpc<MARKET>
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let mock = Arc::clone(&self.0);
        Box::pin(async move {
            let response = match (MARKET, request.uri().path().rsplit('/').next()) {
                (false, Some("ShareBy")) => {
                    Grpc::new(
                        tonic_prost::ProstCodec::<ShareResponse, InstrumentRequest>::default(),
                    )
                    .unary(Reply(|req| Ok(mock.share(req))), request)
                    .await
                }
                (true, Some("GetCandles")) => {
                    Grpc::new(tonic_prost::ProstCodec::<
                        GetCandlesResponse,
                        GetCandlesRequest,
                    >::default())
                    .unary(Reply(|req| mock.get_candles(req)), request)
                    .await
                }
                (true, Some("GetLastTrades")) => {
                    Grpc::new(tonic_prost::ProstCodec::<
                        GetLastTradesResponse,
                        GetLastTradesRequest,
                    >::default())
                    .unary(Reply(|req| mock.get_trades(req)), request)
                    .await
                }
                _ => tonic::Status::unimplemented("unexpected historical RPC").into_http(),
            };
            Ok(response)
        })
    }
}

async fn connect_mock(
    mock: Arc<MockHistory>,
    candle_timestamp_mode: TbankCandleTimestampMode,
) -> (TbankDataClient, mpsc::UnboundedReceiver<DataEvent>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HistoricalRpc::<true>(Arc::clone(&mock)))
            .add_service(HistoricalRpc::<false>(mock))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let (sender, events) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(sender);
    let mut client = TbankDataClient::new(TbankDataClientConfig {
        token: Some("test-token".into()),
        endpoint: Some(endpoint),
        quota_scope: Some(Arc::new(
            TbankRateLimiter::new(
                vec![TbankRpcQuota {
                    methods: vec![],
                    capacity: 100,
                    window: Duration::from_secs(1),
                }],
                10,
            )
            .unwrap(),
        )),
        candle_timestamp_mode,
        historical_candle_max_retries: 0,
        ..Default::default()
    });
    client.connect_historical_download().await.unwrap();
    (client, events)
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<DataEvent>) -> DataEvent {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("historical response timed out")
        .expect("historical sender dropped")
}

#[tokio::test]
async fn single_history_requests_return_correlated_responses() {
    let (mut client, mut events) = connect_mock(
        Arc::new(MockHistory::default()),
        TbankCandleTimestampMode::StartAsBarEnd,
    )
    .await;

    let instrument_id = InstrumentId::from("SBER_TQBR.MOEX");
    let bar_type = BarType::new(
        instrument_id,
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
        AggregationSource::External,
    );
    let end = jiff::Timestamp::now();
    let start = end
        .checked_sub(jiff::SignedDuration::from_secs(120))
        .unwrap();
    let ts_start = Some(nautilus_core::datetime::try_datetime_to_unix_nanos(start).unwrap());
    let ts_end = Some(nautilus_core::datetime::try_datetime_to_unix_nanos(end).unwrap());
    let params = Some(Params::new(
        [("consumer".to_owned(), serde_json::json!(42))].into(),
    ));
    let bars_id = UUID4::new();
    let trades_id = UUID4::new();
    client
        .request_bars(RequestBars::new(
            bar_type,
            Some(start),
            Some(end),
            None,
            None,
            bars_id,
            now_unix_nanos(),
            params.clone(),
        ))
        .unwrap();
    client
        .request_trades(RequestTrades::new(
            instrument_id,
            Some(start),
            Some(end),
            None,
            None,
            trades_id,
            now_unix_nanos(),
            params.clone(),
        ))
        .unwrap();

    let mut received = HashSet::new();
    for _ in 0..2 {
        match next_event(&mut events).await {
            DataEvent::Response(DataResponse::Bars(response)) => {
                assert_eq!(response.bar_type, bar_type);
                assert_eq!(response.correlation_id, bars_id);
                assert_eq!(response.client_id, client.client_id());
                assert_eq!((response.start, response.end), (ts_start, ts_end));
                assert_eq!(response.params, params);
                received.insert(bars_id);
            }
            DataEvent::Response(DataResponse::Trades(response)) => {
                assert_eq!(response.single_instrument_id().unwrap(), instrument_id);
                assert_eq!(response.correlation_id, trades_id);
                assert_eq!(response.client_id, client.client_id());
                assert_eq!((response.start, response.end), (ts_start, ts_end));
                assert_eq!(response.params, params);
                received.insert(trades_id);
            }
            event => panic!("unexpected history response {event:?}"),
        }
    }
    assert_eq!(received, HashSet::from([bars_id, trades_id]));
    client.disconnect_async().await.unwrap();
}

fn trade_request(ids: Vec<InstrumentId>, update_catalog: bool) -> RequestTrades {
    let end = jiff::Timestamp::now();
    let start = end
        .checked_sub(jiff::SignedDuration::from_secs(120))
        .unwrap();
    RequestTrades::new(
        ids,
        Some(start),
        Some(end),
        None,
        None,
        UUID4::new(),
        now_unix_nanos(),
        update_catalog.then(catalog_params),
    )
}

#[tokio::test]
async fn batch_trades_reports_one_sorted_response_with_partial_failures() {
    let sber = InstrumentId::from("SBER_TQBR.MOEX");
    let gazp = InstrumentId::from("GAZP_TQBR.MOEX");
    for (reply, expect_failure, expect_gazp) in [
        (TradeReply::Batch, false, true),
        (TradeReply::EmptySecond, false, false),
        (TradeReply::FailSecond, true, false),
        (TradeReply::WrongInstrumentSecond, true, false),
    ] {
        let mock = Arc::new(MockHistory {
            trades: reply,
            ..Default::default()
        });
        let (mut client, mut events) =
            connect_mock(Arc::clone(&mock), TbankCandleTimestampMode::StartAsBarEnd).await;
        let mut request = trade_request(vec![sber, gazp], false);
        request.params = Some(Params::new(
            [("consumer".to_owned(), serde_json::json!(42))].into(),
        ));
        request.limit = NonZeroUsize::new(1);
        assert!(
            client
                .request_trades(request.clone())
                .unwrap_err()
                .to_string()
                .contains("record limit")
        );
        assert_eq!(mock.share_calls.load(Ordering::Relaxed), 0);
        assert!(mock.trade_calls.lock().unwrap().is_empty());

        request.limit = None;
        client.request_trades(request.clone()).unwrap();
        let response = match next_event(&mut events).await {
            DataEvent::Response(DataResponse::Trades(response)) if !expect_failure => response,
            DataEvent::RequestFailed(failure) if expect_failure => {
                assert_eq!(failure.request_id, request.request_id);
                assert!(failure.reason.contains("GAZP_TQBR.MOEX"));
                assert!(!failure.reason.contains("SBER_TQBR.MOEX"));
                let Some(DataResponse::Trades(response)) = failure.response else {
                    panic!("partial trades were lost");
                };
                response
            }
            event => panic!("unexpected history event {event:?}"),
        };
        let start =
            nautilus_core::datetime::try_datetime_to_unix_nanos(request.start.unwrap()).unwrap();
        let end =
            nautilus_core::datetime::try_datetime_to_unix_nanos(request.end.unwrap()).unwrap();
        assert_eq!(response.correlation_id, request.request_id);
        assert_eq!(response.instrument_ids, [sber, gazp]);
        assert_eq!((response.start, response.end), (Some(start), Some(end)));
        assert_eq!(response.params, request.params);
        let rows = response
            .data
            .iter()
            .map(|trade| (trade.instrument_id, trade.ts_event, trade.size.as_f64()))
            .collect::<Vec<_>>();
        let mut expected = vec![(sber, start, 10.0), (sber, start, 20.0)];
        if expect_gazp {
            expected.push((gazp, start, 40.0));
        }
        expected.push((sber, end, 30.0));
        assert_eq!(rows, expected);
        let uids = mock
            .trade_calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.instrument_id.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(uids, ["SBER-uid", "GAZP-uid"]);

        client.disconnect_async().await.unwrap();
        assert!(events.try_recv().is_err());
    }
}

#[tokio::test]
async fn single_trade_limit_applies_after_filtering_and_sorting_and_rpc_errors_complete() {
    let (mut client, mut events) = connect_mock(
        Arc::new(MockHistory {
            trades: TradeReply::FailSecond,
            ..Default::default()
        }),
        TbankCandleTimestampMode::StartAsBarEnd,
    )
    .await;
    let mut request = trade_request(vec![InstrumentId::from("SBER_TQBR.MOEX")], false);
    request.limit = NonZeroUsize::new(1);
    client.request_trades(request).unwrap();
    let DataEvent::Response(DataResponse::Trades(response)) = next_event(&mut events).await else {
        panic!("expected ordinary trade response");
    };
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].size.as_f64(), 10.0);
    let request = trade_request(vec![InstrumentId::from("GAZP_TQBR.MOEX")], false);
    let request_id = request.request_id;
    client.request_trades(request).unwrap();
    let DataEvent::RequestFailed(failure) = next_event(&mut events).await else {
        panic!("expected failed trade response");
    };
    assert_eq!(failure.request_id, request_id);
    assert!(failure.reason.contains("GAZP_TQBR.MOEX"));
    let mut request = trade_request(vec![InstrumentId::from("SBER_TQBR.MOEX")], false);
    request.start = Some(
        request
            .end
            .unwrap()
            .checked_sub(jiff::SignedDuration::from_hours(2))
            .unwrap(),
    );
    let request_id = request.request_id;
    client.request_trades(request).unwrap();
    let DataEvent::RequestFailed(failure) = next_event(&mut events).await else {
        panic!("expected failure for a trade window outside the last hour");
    };
    assert_eq!(failure.request_id, request_id);
    assert!(
        failure
            .reason
            .contains("GetLastTrades supports only the last hour")
    );
    client.disconnect_async().await.unwrap();
}

fn catalog_params() -> Params {
    Params::new(
        [
            ("update_catalog".to_owned(), serde_json::json!(true)),
            ("catalog_name".to_owned(), serde_json::json!("history")),
        ]
        .into(),
    )
}

fn catalog_bar_request(start: jiff::Timestamp, end: jiff::Timestamp) -> RequestBars {
    RequestBars::new(
        BarType::new(
            InstrumentId::from("SBER_TQBR.MOEX"),
            BarSpecification::new(5, BarAggregation::Second, PriceType::Last),
            AggregationSource::External,
        ),
        Some(start),
        Some(end),
        None,
        None,
        UUID4::new(),
        now_unix_nanos(),
        Some(catalog_params()),
    )
}

fn catalog_engine(client: TbankDataClient, path: Option<&std::path::Path>) -> DataEngine {
    MessageBus::new(TraderId::from("HISTORY-001"), UUID4::new(), None, None).register_message_bus();
    let mut clock = VirtualClock::new();
    clock.advance_time(now_unix_nanos(), true);
    let mut engine = DataEngine::new(
        Rc::new(RefCell::new(clock)),
        Rc::new(RefCell::new(Cache::default())),
        None,
    );
    if let Some(path) = path {
        engine.register_catalog(
            Box::new(ParquetDataCatalog::new(path, None, None, None, None)),
            Some("history"),
        );
    }
    engine.register_client(
        DataClientAdapter::new(client.client_id(), None, false, false, Box::new(client)),
        None,
    );
    engine
}

fn watch_response(request: &RequestCommand) -> impl FnOnce() -> DataResponseEnvelope + use<> {
    let (handler, sink) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    let id = *request.request_id();
    msgbus::register_response_handler(&id, handler);
    move || {
        let messages = sink.get_messages();
        assert_eq!(messages.len(), 1);
        let response = messages.into_iter().next().unwrap();
        assert!(response.completed());
        assert_eq!(*response.correlation_id(), id);
        response
    }
}

fn drive_history_event(engine: &mut DataEngine, event: DataEvent) {
    match event {
        DataEvent::Response(response) => engine.response(response),
        DataEvent::ResponseEnvelope(envelope) => engine.response_envelope(envelope),
        DataEvent::RequestFailed(failure) => engine.request_failed(failure),
        other => panic!("unexpected history event {other:?}"),
    }
}

fn bar_response(envelope: &DataResponseEnvelope) -> &BarsResponse {
    let Some(DataResponse::Bars(response)) = envelope.response() else {
        panic!("expected bars, got {envelope:?}");
    };
    response
}

#[tokio::test]
async fn catalog_reads_full_candle_windows_and_adjacent_empty_coverage_without_rpc() {
    let start = jiff::Timestamp::from_second(1_700_000_000).unwrap();
    let end = start
        .checked_add(jiff::SignedDuration::from_hours(4))
        .unwrap();
    for (mode, offset) in [
        (TbankCandleTimestampMode::StartAsBarEnd, 5),
        (TbankCandleTimestampMode::StartAsBarStart, 0),
    ] {
        let mock = Arc::new(MockHistory {
            candles: CandleReply::At(prost_types::Timestamp {
                seconds: start.as_second() - offset,
                nanos: 0,
            }),
            ..Default::default()
        });
        let dir = tempfile::tempdir().unwrap();
        let (client, mut events) = connect_mock(Arc::clone(&mock), mode).await;
        let mut engine = catalog_engine(client, Some(dir.path()));
        let request = RequestCommand::Bars(catalog_bar_request(start, end));
        let result = watch_response(&request);
        engine.execute_request(request).unwrap();
        let event = next_event(&mut events).await;
        assert!(matches!(event, DataEvent::ResponseEnvelope(_)));
        drive_history_event(&mut engine, event);
        let result = result();
        assert_eq!(result.failure_reason(), None);
        let rows = &bar_response(&result).data;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].ts_event,
            UnixNanos::from(start.as_nanosecond() as u64)
        );

        {
            let calls = mock.candle_calls.lock().unwrap();
            assert_eq!(calls.len(), 2);
            for call in calls.iter() {
                let from =
                    crate::common::time::timestamp_to_unix_nanos(&call.from.unwrap()).unwrap();
                let to = crate::common::time::timestamp_to_unix_nanos(&call.to.unwrap()).unwrap();
                assert!(to - from <= 200 * 60 * 1_000_000_000);
                assert_eq!(call.limit, Some(2500));
            }
        }

        // The end of this request is past the last returned row. A cache hit
        // verifies that the catalog recorded the requested bounds.
        let request = RequestCommand::Bars(catalog_bar_request(start, end));
        let readback = watch_response(&request);
        engine.execute_request(request).unwrap();
        let readback = readback();
        assert_eq!(readback.failure_reason(), None);
        assert_eq!(bar_response(&readback).data.as_ref(), rows.as_ref());

        let empty_start = end
            .checked_add(jiff::SignedDuration::from_nanos(1))
            .unwrap();
        let empty_end = empty_start
            .checked_add(jiff::SignedDuration::from_secs(60))
            .unwrap();
        let request = RequestCommand::Bars(catalog_bar_request(empty_start, empty_end));
        let empty = watch_response(&request);
        engine.execute_request(request).unwrap();
        drive_history_event(&mut engine, next_event(&mut events).await);
        let empty = empty();
        assert_eq!(empty.failure_reason(), None);
        assert!(bar_response(&empty).data.is_empty());

        let request = RequestCommand::Bars(catalog_bar_request(empty_start, empty_end));
        let readback = watch_response(&request);
        engine.execute_request(request).unwrap();
        let readback = readback();
        assert_eq!(readback.failure_reason(), None);
        assert!(bar_response(&readback).data.is_empty());
        assert_eq!(mock.candle_calls.lock().unwrap().len(), 3);
        assert!(events.try_recv().is_err());
    }
}

#[tokio::test]
async fn incomplete_candle_windows_keep_partial_rows_and_leave_no_catalog_coverage() {
    let start = jiff::Timestamp::from_second(1_700_000_000).unwrap();
    let end = start
        .checked_add(jiff::SignedDuration::from_hours(4))
        .unwrap();
    for (candles, expected, calls) in [
        (CandleReply::FailSecond, "second candle window failed", 2),
        (CandleReply::Open, "open candle", 1),
        (CandleReply::AtCap, "row cap", 1),
    ] {
        let mock = Arc::new(MockHistory {
            candles,
            ..Default::default()
        });
        let dir = tempfile::tempdir().unwrap();
        let (client, mut events) =
            connect_mock(Arc::clone(&mock), TbankCandleTimestampMode::StartAsBarEnd).await;
        let mut engine = catalog_engine(client, Some(dir.path()));
        for expected_calls in [calls, calls * 2] {
            let request = RequestCommand::Bars(catalog_bar_request(start, end));
            let result = watch_response(&request);
            engine.execute_request(request).unwrap();
            let event = next_event(&mut events).await;
            assert!(matches!(event, DataEvent::RequestFailed(_)));
            drive_history_event(&mut engine, event);
            let result = result();
            assert!(
                result.failure_reason().unwrap().contains(expected),
                "{result:?}"
            );
            assert_eq!(bar_response(&result).data.len(), 1);
            assert_eq!(mock.candle_calls.lock().unwrap().len(), expected_calls);
        }
    }
}

#[tokio::test]
async fn catalog_trade_history_fails_with_rows_and_does_not_record_coverage() {
    let mock = Arc::new(MockHistory {
        trades: TradeReply::Batch,
        ..Default::default()
    });
    let dir = tempfile::tempdir().unwrap();
    let (client, mut events) =
        connect_mock(Arc::clone(&mock), TbankCandleTimestampMode::StartAsBarEnd).await;
    let mut engine = catalog_engine(client, Some(dir.path()));
    let mut trades = trade_request(
        vec![
            InstrumentId::from("SBER_TQBR.MOEX"),
            InstrumentId::from("GAZP_TQBR.MOEX"),
        ],
        true,
    );
    for expected_calls in [2, 4] {
        trades.request_id = UUID4::new();
        let request = RequestCommand::Trades(trades.clone());
        let result = watch_response(&request);
        engine.execute_request(request).unwrap();
        let event = next_event(&mut events).await;
        assert!(matches!(event, DataEvent::RequestFailed(_)));
        drive_history_event(&mut engine, event);
        let result = result();
        assert!(
            result
                .failure_reason()
                .unwrap()
                .contains("does not certify complete coverage")
        );
        let Some(DataResponse::Trades(response)) = result.response() else {
            panic!("expected partial trades, got {result:?}");
        };
        assert_eq!(response.data.len(), 4);
        assert_eq!(mock.trade_calls.lock().unwrap().len(), expected_calls);
    }
}

#[tokio::test]
async fn parquet_write_error_reaches_the_requester() {
    let start = jiff::Timestamp::from_second(1_700_000_000).unwrap();
    let end = start
        .checked_add(jiff::SignedDuration::from_secs(60))
        .unwrap();
    let mock = Arc::new(MockHistory {
        candles: CandleReply::At(prost_types::Timestamp {
            seconds: start.as_second() - 5,
            nanos: 0,
        }),
        ..Default::default()
    });
    let dir = tempfile::tempdir().unwrap();
    let (client, mut events) = connect_mock(mock, TbankCandleTimestampMode::StartAsBarEnd).await;
    let mut engine = catalog_engine(client, Some(dir.path()));
    let request = RequestCommand::Bars(catalog_bar_request(start, end));
    let result = watch_response(&request);
    engine.execute_request(request).unwrap();
    let event = next_event(&mut events).await;
    assert!(matches!(event, DataEvent::ResponseEnvelope(_)));
    std::fs::write(dir.path().join("data"), b"obstruct the Parquet directory").unwrap();
    drive_history_event(&mut engine, event);
    let result = result();
    assert!(
        result
            .failure_reason()
            .unwrap()
            .contains("Catalog write-back failed"),
        "{result:?}"
    );
    assert_eq!(bar_response(&result).data.len(), 1);
}

#[tokio::test]
async fn parquet_rc9_cannot_record_a_standalone_empty_window() {
    let start = jiff::Timestamp::from_second(1_700_000_000).unwrap();
    let end = start
        .checked_add(jiff::SignedDuration::from_secs(60))
        .unwrap();
    let mock = Arc::new(MockHistory::default());
    let dir = tempfile::tempdir().unwrap();
    let (client, mut events) =
        connect_mock(Arc::clone(&mock), TbankCandleTimestampMode::StartAsBarEnd).await;
    let mut engine = catalog_engine(client, Some(dir.path()));

    for expected_calls in [1, 2] {
        let request = RequestCommand::Bars(catalog_bar_request(start, end));
        let result = watch_response(&request);
        engine.execute_request(request).unwrap();
        let event = next_event(&mut events).await;
        assert!(matches!(event, DataEvent::ResponseEnvelope(_)));
        drive_history_event(&mut engine, event);
        let result = result();
        assert_eq!(result.failure_reason(), None);
        assert!(bar_response(&result).data.is_empty());
        assert_eq!(mock.candle_calls.lock().unwrap().len(), expected_calls);
    }
}

#[tokio::test]
async fn data_engine_validates_catalog_options_before_rpc() {
    let start = jiff::Timestamp::from_second(1_700_000_000).unwrap();
    let end = start
        .checked_add(jiff::SignedDuration::from_secs(60))
        .unwrap();
    let mock = Arc::new(MockHistory::default());
    let dir = tempfile::tempdir().unwrap();
    let (client, mut events) =
        connect_mock(Arc::clone(&mock), TbankCandleTimestampMode::StartAsBarEnd).await;
    let mut engine = catalog_engine(client, None);
    let bars = catalog_bar_request(start, end);
    let request = RequestCommand::Bars(bars.clone());
    let result = watch_response(&request);
    assert!(engine.execute_request(request).is_err());
    assert!(
        result()
            .failure_reason()
            .unwrap()
            .contains("registered catalog")
    );

    engine.register_catalog(
        Box::new(ParquetDataCatalog::new(dir.path(), None, None, None, None)),
        Some("history"),
    );
    let mut bars = bars;
    bars.limit = NonZeroUsize::new(1);
    let request = RequestCommand::Bars(bars);
    let result = watch_response(&request);
    assert!(engine.execute_request(request).is_err());
    assert!(result().failure_reason().unwrap().contains("record limit"));

    let mut trades = trade_request(vec![InstrumentId::from("SBER_TQBR.MOEX")], true);
    trades.params = Some(Params::new(
        [
            ("update_catalog".to_owned(), serde_json::json!(true)),
            ("schema".to_owned(), serde_json::json!("trades")),
        ]
        .into(),
    ));
    let request = RequestCommand::Trades(trades);
    let result = watch_response(&request);
    assert!(engine.execute_request(request).is_err());
    assert!(result().failure_reason().unwrap().contains("source schema"));
    assert_eq!(mock.share_calls.load(Ordering::Relaxed), 0);
    assert!(events.try_recv().is_err());
}
