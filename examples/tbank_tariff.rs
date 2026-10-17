//! Read-only quota discovery; prints no credentials or account identifiers.
use nautilus_core::string::secret::SecretString;
use std::{sync::Arc, time::Duration};
use tbank_nt_community::{
    config::TbankEnvironment,
    grpc::{
        TbankAuthInterceptor, connect_channel,
        generated::{GetUserTariffRequest, users_service_client::UsersServiceClient},
        with_timeout,
    },
};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let environment = TbankEnvironment::Sandbox;
    let secret = SecretString::from(
        std::env::var("TBANK_SANDBOX_INVEST_TOKEN")
            .map_err(|_| anyhow::anyhow!("sandbox token missing"))?,
    );
    let timeout = Duration::from_secs(15);
    let channel = connect_channel(environment.default_endpoint(), timeout).await?;
    let mut client =
        UsersServiceClient::with_interceptor(channel, TbankAuthInterceptor::new(Arc::new(secret))?);
    let tariff = client
        .get_user_tariff(with_timeout(GetUserTariffRequest {}, timeout))
        .await
        .map_err(|status| anyhow::anyhow!("GetUserTariff failed: {:?}", status.code()))?
        .into_inner();
    for quota in tariff.unary_limits {
        println!(
            "unary minute={} second={:?} methods={:?}",
            quota.limit_per_minute, quota.limit_per_second, quota.methods
        );
    }
    for quota in tariff.stream_limits {
        println!(
            "stream limit={} open={} methods={:?}",
            quota.limit, quota.open, quota.streams
        );
    }
    Ok(())
}
