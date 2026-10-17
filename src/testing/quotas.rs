use crate::grpc::generated::*;
#[derive(Clone)]
pub struct QuotaUsersService;
#[tonic::async_trait]
impl users_service_server::UsersService for QuotaUsersService {
    async fn get_accounts(
        &self,
        _: tonic::Request<GetAccountsRequest>,
    ) -> Result<tonic::Response<GetAccountsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn get_margin_attributes(
        &self,
        _: tonic::Request<GetMarginAttributesRequest>,
    ) -> Result<tonic::Response<GetMarginAttributesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn get_user_tariff(
        &self,
        _: tonic::Request<GetUserTariffRequest>,
    ) -> Result<tonic::Response<GetUserTariffResponse>, tonic::Status> {
        Ok(tonic::Response::new(GetUserTariffResponse {
            unary_limits: vec![UnaryLimit {
                limit_per_minute: 1000000,
                methods: mock_rpc_methods(),
                limit_per_second: None,
            }],
            stream_limits: vec![StreamLimit {
                limit: 10000,
                open: 0,
                streams: mock_rpc_methods(),
            }],
        }))
    }
    async fn get_info(
        &self,
        _: tonic::Request<GetInfoRequest>,
    ) -> Result<tonic::Response<GetInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn get_bank_accounts(
        &self,
        _: tonic::Request<GetBankAccountsRequest>,
    ) -> Result<tonic::Response<GetBankAccountsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn currency_transfer(
        &self,
        _: tonic::Request<CurrencyTransferRequest>,
    ) -> Result<tonic::Response<CurrencyTransferResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn pay_in(
        &self,
        _: tonic::Request<PayInRequest>,
    ) -> Result<tonic::Response<PayInResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn get_account_values(
        &self,
        _: tonic::Request<GetAccountValuesRequest>,
    ) -> Result<tonic::Response<GetAccountValuesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
}

fn mock_rpc_methods() -> Vec<String> {
    // Use the pinned method names rather than a production-only wildcard/bypass for tests.
    let contracts = [
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/instruments.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/marketdata.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/orders.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/stoporders.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/operations.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/users.proto"),
        include_str!("../../proto/tinkoff/public/invest/api/contract/v1/sandbox.proto"),
    ];
    contracts
        .into_iter()
        .flat_map(str::lines)
        .filter_map(|line| line.trim().strip_prefix("rpc "))
        .filter_map(|line| line.split([' ', '(']).next())
        .filter(|method| !method.is_empty())
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}
