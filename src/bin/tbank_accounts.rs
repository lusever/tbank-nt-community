use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use tbank_nt_community::{
    common::consts::{
        ACCOUNT_ID_ENV, DEFAULT_REQUEST_TIMEOUT, LIVE_ENDPOINT, LIVE_TOKEN_ENV,
        SANDBOX_ACCOUNT_ID_ENV, SANDBOX_ENDPOINT, SANDBOX_TOKEN_ENV,
    },
    grpc::{
        clients::TbankGrpcClients,
        connect_channel,
        generated::{AccessLevel, AccountStatus, AccountType, GetAccountsRequest},
        metadata::TbankAuthInterceptor,
        with_timeout,
    },
};
use zeroize::Zeroizing;

#[derive(Debug, Parser)]
#[command(about = "List T-Bank accounts available to the configured token")]
struct Cli {
    #[arg(long, value_enum, default_value_t = Environment::Live)]
    environment: Environment,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Environment {
    Live,
    Sandbox,
}

impl Environment {
    const fn endpoint(self) -> &'static str {
        match self {
            Self::Live => LIVE_ENDPOINT,
            Self::Sandbox => SANDBOX_ENDPOINT,
        }
    }

    const fn token_env(self) -> &'static str {
        match self {
            Self::Live => LIVE_TOKEN_ENV,
            Self::Sandbox => SANDBOX_TOKEN_ENV,
        }
    }

    const fn configured_account_env(self) -> &'static str {
        match self {
            Self::Live => ACCOUNT_ID_ENV,
            Self::Sandbox => SANDBOX_ACCOUNT_ID_ENV,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let token = Zeroizing::new(
        std::env::var(cli.environment.token_env())
            .with_context(|| format!("missing {}", cli.environment.token_env()))?,
    );
    if token.trim().is_empty() {
        bail!("{} is empty", cli.environment.token_env());
    }
    let configured_account = std::env::var(cli.environment.configured_account_env()).ok();
    let mut clients = clients(cli.environment.endpoint(), &token).await?;
    let request = GetAccountsRequest {
        status: Some(AccountStatus::Open as i32),
    };
    let response = match cli.environment {
        Environment::Live => clients
            .users
            .get_accounts(with_timeout(request, DEFAULT_REQUEST_TIMEOUT))
            .await?
            .into_inner(),
        Environment::Sandbox => clients
            .sandbox
            .get_sandbox_accounts(with_timeout(request, DEFAULT_REQUEST_TIMEOUT))
            .await?
            .into_inner(),
    };

    println!("environment={:?}", cli.environment);
    println!(
        "configured_account_env={} configured_account_present={}",
        cli.environment.configured_account_env(),
        configured_account.is_some()
    );
    println!("accounts={}", response.accounts.len());
    for account in response.accounts {
        let configured = configured_account.as_deref() == Some(account.id.as_str());
        println!(
            "account configured={} type={} status={} access_level={}",
            configured,
            account_type_name(account.r#type),
            account_status_name(account.status),
            access_level_name(account.access_level)
        );
    }
    Ok(())
}

async fn clients(
    endpoint: &str,
    token: &str,
) -> anyhow::Result<TbankGrpcClients<TbankAuthInterceptor>> {
    let channel = connect_channel(endpoint, DEFAULT_REQUEST_TIMEOUT)
        .await
        .with_context(|| format!("connect to T-Bank endpoint {endpoint}"))?;
    let interceptor = TbankAuthInterceptor::new(token)?;
    Ok(TbankGrpcClients::new(channel, interceptor))
}

fn account_type_name(value: i32) -> &'static str {
    AccountType::try_from(value)
        .map(|value| value.as_str_name())
        .unwrap_or("ACCOUNT_TYPE_UNKNOWN")
}

fn account_status_name(value: i32) -> &'static str {
    AccountStatus::try_from(value)
        .map(|value| value.as_str_name())
        .unwrap_or("ACCOUNT_STATUS_UNKNOWN")
}

fn access_level_name(value: i32) -> &'static str {
    AccessLevel::try_from(value)
        .map(|value| value.as_str_name())
        .unwrap_or("ACCESS_LEVEL_UNKNOWN")
}
