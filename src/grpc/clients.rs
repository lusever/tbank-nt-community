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
    pub instruments: InstrumentsServiceClient<InterceptedService<Channel, I>>,
    /// Unary market-data service client.
    pub market_data: MarketDataServiceClient<InterceptedService<Channel, I>>,
    /// Streaming market-data service client.
    pub market_data_stream: MarketDataStreamServiceClient<InterceptedService<Channel, I>>,
    /// Regular orders service client.
    pub orders: OrdersServiceClient<InterceptedService<Channel, I>>,
    /// Orders stream service client.
    pub orders_stream: OrdersStreamServiceClient<InterceptedService<Channel, I>>,
    /// Stop-orders service client.
    pub stop_orders: StopOrdersServiceClient<InterceptedService<Channel, I>>,
    /// Portfolio operations service client.
    pub operations: OperationsServiceClient<InterceptedService<Channel, I>>,
    /// Portfolio operations stream service client.
    pub operations_stream: OperationsStreamServiceClient<InterceptedService<Channel, I>>,
    /// Sandbox service client.
    pub sandbox: SandboxServiceClient<InterceptedService<Channel, I>>,
    /// User accounts service client.
    pub users: UsersServiceClient<InterceptedService<Channel, I>>,
}

impl<I> TbankGrpcClients<I>
where
    I: Interceptor + Clone,
{
    /// Creates a new instance.
    pub fn new(channel: Channel, interceptor: I) -> Self {
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
