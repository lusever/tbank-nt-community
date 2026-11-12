use super::*;
use crate::grpc::generated::{
    GetCandlesRequest, GetCandlesResponse, GetLastTradesRequest, GetLastTradesResponse,
    InstrumentRequest, Quotation, RealExchange, Share, ShareResponse,
};
use nautilus_core::{Params, UUID4};
use nautilus_model::data::{BarSpecification, BarType};
use nautilus_model::enums::{AggregationSource, PriceType};
use std::{
    future::{Ready, ready},
    result::Result,
};
use tonic::{
    codegen::{Body, BoxFuture, Service, StdError},
    server::{Grpc, NamedService, UnaryService},
};

#[derive(Clone)]
struct HistoricalRpc<const MARKET: bool>;

impl<const MARKET: bool> NamedService for HistoricalRpc<MARKET> {
    const NAME: &'static str = if MARKET {
        "tinkoff.public.invest.api.contract.v1.MarketDataService"
    } else {
        "tinkoff.public.invest.api.contract.v1.InstrumentsService"
    };
}

struct Reply<T>(Option<T>);

impl<I, O> UnaryService<I> for Reply<O> {
    type Response = O;
    type Future = Ready<Result<tonic::Response<O>, tonic::Status>>;

    fn call(&mut self, _: tonic::Request<I>) -> Self::Future {
        ready(Ok(tonic::Response::new(self.0.take().unwrap())))
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
        Box::pin(async move {
            let response = match (MARKET, request.uri().path().rsplit('/').next()) {
                (false, Some("ShareBy")) => {
                    let share = Share {
                        ticker: "SBER".into(),
                        class_code: "TQBR".into(),
                        currency: "rub".into(),
                        lot: 10,
                        min_price_increment: Some(Quotation {
                            units: 0,
                            nano: 10_000_000,
                        }),
                        uid: "sber-uid".into(),
                        real_exchange: RealExchange::Moex as i32,
                        ..Default::default()
                    };
                    Grpc::new(
                        tonic_prost::ProstCodec::<ShareResponse, InstrumentRequest>::default(),
                    )
                    .unary(
                        Reply(Some(ShareResponse {
                            instrument: Some(share),
                        })),
                        request,
                    )
                    .await
                }
                (true, Some("GetCandles")) => {
                    Grpc::new(tonic_prost::ProstCodec::<
                        GetCandlesResponse,
                        GetCandlesRequest,
                    >::default())
                    .unary(Reply(Some(GetCandlesResponse::default())), request)
                    .await
                }
                (true, Some("GetLastTrades")) => {
                    Grpc::new(tonic_prost::ProstCodec::<
                        GetLastTradesResponse,
                        GetLastTradesRequest,
                    >::default())
                    .unary(Reply(Some(GetLastTradesResponse::default())), request)
                    .await
                }
                _ => tonic::Status::unimplemented("unexpected historical RPC").into_http(),
            };
            Ok(response)
        })
    }
}

#[tokio::test]
async fn single_history_requests_return_correlated_responses() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HistoricalRpc::<true>)
            .add_service(HistoricalRpc::<false>)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(sender);
    let mut client = TbankDataClient::new(TbankDataClientConfig {
        token: Some("test-token".into()),
        endpoint: Some(endpoint),
        ..Default::default()
    });
    client.connect_historical_download().await.unwrap();

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
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .expect("historical response timed out");
        match event {
            Some(DataEvent::Response(DataResponse::Bars(response))) => {
                assert_eq!(response.bar_type, bar_type);
                assert_eq!(response.correlation_id, bars_id);
                assert_eq!(response.client_id, client.client_id());
                assert_eq!((response.start, response.end), (ts_start, ts_end));
                assert_eq!(response.params, params);
                received.insert(bars_id);
            }
            Some(DataEvent::Response(DataResponse::Trades(response))) => {
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
