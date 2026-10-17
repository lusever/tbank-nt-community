use super::limited_channel::TbankLimitedChannel;
use tonic::{codegen::InterceptedService, service::Interceptor, transport::Channel};

use crate::grpc::generated::{
    instruments_service_client::InstrumentsServiceClient,
    market_data_service_client::MarketDataServiceClient,
    market_data_stream_service_client::MarketDataStreamServiceClient,
    operations_service_client::OperationsServiceClient,
    operations_stream_service_client::OperationsStreamServiceClient,
    orders_service_client::OrdersServiceClient,
    orders_stream_service_client::OrdersStreamServiceClient,
    sandbox_service_client::SandboxServiceClient,
    stop_orders_service_client::StopOrdersServiceClient, users_service_client::UsersServiceClient,
};

#[derive(Clone)]
/// Collection of typed T-Bank gRPC service clients sharing one channel.
pub struct TbankGrpcClients<I>
where
    I: Interceptor + Clone,
{
    /// Instruments service client.
    pub instruments: InstrumentsServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Unary market-data service client.
    pub market_data: MarketDataServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Streaming market-data service client.
    pub market_data_stream:
        MarketDataStreamServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Regular orders service client.
    pub orders: OrdersServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Orders stream service client.
    pub orders_stream: OrdersStreamServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Stop-orders service client.
    pub stop_orders: StopOrdersServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Portfolio operations service client.
    pub operations: OperationsServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Portfolio operations stream service client.
    pub operations_stream:
        OperationsStreamServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// Sandbox service client.
    pub sandbox: SandboxServiceClient<InterceptedService<TbankLimitedChannel, I>>,
    /// User accounts service client.
    pub users: UsersServiceClient<InterceptedService<TbankLimitedChannel, I>>,
}

impl<I> TbankGrpcClients<I>
where
    I: Interceptor + Clone,
{
    /// Creates a new instance.
    pub fn new(channel: Channel, interceptor: I) -> Self {
        Self::new_with_quota_scope(channel, interceptor, None)
    }

    /// Creates clients using a consumer-provided shared quota scope, or automatic tariff discovery.
    pub fn new_with_quota_scope(
        channel: Channel,
        interceptor: I,
        scope: Option<std::sync::Arc<super::limiter::TbankRateLimiter>>,
    ) -> Self {
        let channel = TbankLimitedChannel::with_quota_scope(channel, scope);
        Self {
            instruments: InstrumentsServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            market_data: MarketDataServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            market_data_stream: MarketDataStreamServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            orders: OrdersServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            orders_stream: OrdersStreamServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            stop_orders: StopOrdersServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            operations: OperationsServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            operations_stream: OperationsStreamServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            sandbox: SandboxServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            users: UsersServiceClient::with_interceptor(channel, interceptor),
        }
    }
}
