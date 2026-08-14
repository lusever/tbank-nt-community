#![allow(deprecated)]
#![cfg_attr(feature = "sandbox-futures-tests", allow(dead_code, unused_imports))]

use std::{cell::RefCell, env, future::Future, panic::AssertUnwindSafe, rc::Rc, time::Duration};

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures_util::{FutureExt, StreamExt};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            CancelOrder, ExecutionReport, GenerateFillReports, GenerateOrderStatusReport,
            GenerateOrderStatusReports, GeneratePositionStatusReports, SubmitOrder,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce, TriggerType},
    events::{AccountState, OrderEventAny},
    identifiers::{ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue},
    orders::OrderTestBuilder,
    reports::{FillReport, OrderStatusReport},
    types::{Price, Quantity},
};
use prost_types::Timestamp;
use rust_decimal::Decimal;
use tbank_nt_community::{
    common::{
        consts::{
            DEFAULT_REQUEST_TIMEOUT, MOEX, RUB_CURRENCY, SANDBOX_ENDPOINT, SANDBOX_TOKEN_ENV,
            SPBFUT_CLASS_CODE, TQBR_CLASS_CODE,
        },
        decimal::{decimal_to_money_value, money_value_to_decimal, quotation_to_decimal},
    },
    config::{TbankEnvironment, TbankExecutionClientConfig},
    execution::{TbankExecutionClient, tbank_account_id},
    grpc::{
        clients::TbankGrpcClients,
        connect_channel,
        generated::{
            self, CandleInterval, GetAccountsRequest, GetCandlesRequest, GetFuturesMarginRequest,
            GetLastPricesRequest, GetOrderBookRequest, GetOrdersRequest, GetStopOrdersRequest,
            GetTradingStatusRequest, InstrumentIdType, InstrumentRequest, LastPriceType,
            OrderDirection, PortfolioRequest, PositionsRequest, SandboxPayInRequest,
            SecurityTradingStatus, StopOrderStatusOption, SubscribeLastPriceRequest,
            SubscriptionAction, SubscriptionStatus,
            market_data_response::Payload as MarketDataPayload,
        },
        metadata::TbankAuthInterceptor,
        with_timeout,
    },
    instruments::TbankInstrumentMetadata,
};
use tokio::sync::OnceCell;
use tokio::time::timeout;
use tonic::transport::Channel;
use tonic::{Response, Status};
use uuid::Uuid;

#[cfg(feature = "sandbox-futures-tests")]
use tbank_nt_community::common::decimal::futures_currency_to_points_without_tick_validation;

const SANDBOX_ACCOUNT_ID_ENV: &str = "TBANK_SANDBOX_ACCOUNT_ID";
const SANDBOX_PAY_IN_RUB_ENV: &str = "TBANK_SANDBOX_PAY_IN_RUB";
const SANDBOX_TEST_INSTRUMENT_ENV: &str = "TBANK_SANDBOX_TEST_INSTRUMENT";
#[cfg(feature = "sandbox-futures-tests")]
const SANDBOX_FUTURES_INSTRUMENT_ENV: &str = "TBANK_SANDBOX_FUTURES_INSTRUMENT";
const DEFAULT_TEST_INSTRUMENT: &str = "SBER_TQBR.MOEX";
const EXPECTED_SANDBOX_ENDPOINT: &str = "https://sandbox-invest-public-api.tbank.ru:443";
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

type Clients = TbankGrpcClients<TbankAuthInterceptor>;

static SANDBOX_PREFLIGHT: OnceCell<std::result::Result<(), String>> = OnceCell::const_new();

fn execution_core(account_id: &str, cache: Rc<RefCell<Cache>>) -> ExecutionClientCore {
    ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("TBANK"),
        Venue::from("TBANK"),
        OmsType::Netting,
        tbank_account_id(account_id),
        AccountType::Margin,
        None,
        cache,
    )
}

#[derive(Clone)]
struct SandboxEnv {
    token: String,
    account_id: Option<String>,
    pay_in_rub: Option<Decimal>,
    instrument: InstrumentSpec,
}

#[derive(Debug, Clone)]
struct InstrumentSpec {
    env_value: String,
    ticker: String,
    class_code: String,
}

#[derive(Debug, Clone)]
struct InstrumentMeta {
    ticker: String,
    class_code: String,
    instrument_uid: String,
    figi: String,
    position_uid: String,
    lot: i32,
    min_price_increment: Decimal,
    price_in_points: bool,
    min_price_increment_amount: Option<Decimal>,
    #[cfg(feature = "sandbox-futures-tests")]
    initial_margin_on_buy: Option<Decimal>,
    #[cfg(feature = "sandbox-futures-tests")]
    initial_margin_on_sell: Option<Decimal>,
    is_futures: bool,
}

impl SandboxEnv {
    fn from_env() -> Result<Self> {
        Self::from_instrument(InstrumentSpec::from_env()?)
    }

    #[cfg(feature = "sandbox-futures-tests")]
    fn from_futures_env() -> Result<Self> {
        Self::from_instrument(InstrumentSpec::from_futures_env()?)
    }

    fn from_instrument(instrument: InstrumentSpec) -> Result<Self> {
        assert_sandbox_endpoint()?;

        let token = env::var(SANDBOX_TOKEN_ENV).map_err(|_| {
            anyhow!(
                "missing required sandbox token env: token_present=false account_id_present={} endpoint_host=sandbox-invest-public-api.tbank.ru",
                env::var_os(SANDBOX_ACCOUNT_ID_ENV).is_some()
            )
        })?;
        ensure!(
            !token.trim().is_empty(),
            "sandbox token env is empty: token_present=false"
        );

        let account_id = env::var(SANDBOX_ACCOUNT_ID_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        Ok(Self {
            token,
            account_id,
            pay_in_rub: env_decimal(SANDBOX_PAY_IN_RUB_ENV)?,
            instrument,
        })
    }
}

impl InstrumentSpec {
    fn from_env() -> Result<Self> {
        let env_value = env::var(SANDBOX_TEST_INSTRUMENT_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_TEST_INSTRUMENT.to_string());
        let (ticker, class_code) = parse_ticker_class(&env_value)?;
        Ok(Self {
            env_value,
            ticker,
            class_code,
        })
    }

    #[cfg(feature = "sandbox-futures-tests")]
    fn from_futures_env() -> Result<Self> {
        let env_value = env::var(SANDBOX_FUTURES_INSTRUMENT_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .with_context(|| {
                format!(
                    "{SANDBOX_FUTURES_INSTRUMENT_ENV} is required for MOEX futures sandbox acceptance"
                )
            })?;
        let (ticker, class_code) = parse_futures_ticker_class(&env_value)?;
        Ok(Self {
            env_value,
            ticker,
            class_code,
        })
    }
}

fn assert_sandbox_endpoint() -> Result<()> {
    ensure!(
        SANDBOX_ENDPOINT == EXPECTED_SANDBOX_ENDPOINT,
        "resolved endpoint differs from sandbox endpoint: endpoint_host={}",
        endpoint_host(SANDBOX_ENDPOINT)
    );
    Ok(())
}

fn endpoint_host(endpoint: &str) -> &str {
    endpoint
        .strip_prefix("https://")
        .unwrap_or(endpoint)
        .trim_end_matches(":443")
}

fn env_decimal(name: &str) -> Result<Option<Decimal>> {
    let Some(raw) = env::var(name).ok().filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    Decimal::from_str_exact(&raw)
        .with_context(|| format!("invalid decimal env {name}={raw}"))
        .map(Some)
}

fn parse_ticker_class(value: &str) -> Result<(String, String)> {
    let (ticker, class_code) = split_ticker_class(value)?;
    ensure!(
        class_code.eq_ignore_ascii_case(TQBR_CLASS_CODE),
        "sandbox acceptance currently supports only {MOEX}/{TQBR_CLASS_CODE} RUB shares; got {value}"
    );
    Ok((ticker, class_code))
}

#[cfg(feature = "sandbox-futures-tests")]
fn parse_futures_ticker_class(value: &str) -> Result<(String, String)> {
    let (ticker, class_code) = split_ticker_class(value)?;
    ensure!(
        class_code.eq_ignore_ascii_case(SPBFUT_CLASS_CODE),
        "MOEX futures sandbox acceptance requires class {SPBFUT_CLASS_CODE}; got {value}"
    );
    Ok((ticker, class_code))
}

fn split_ticker_class(value: &str) -> Result<(String, String)> {
    let without_suffix = value.strip_suffix(".MOEX").unwrap_or(value);
    let mut parts = without_suffix.split('_');
    let ticker = parts.next().unwrap_or_default();
    let class_code = parts.next().unwrap_or_default();
    ensure!(
        !ticker.is_empty() && !class_code.is_empty() && parts.next().is_none(),
        "instrument must be TICKER_CLASS or TICKER_CLASS.MOEX, got {value}"
    );
    Ok((ticker.to_string(), class_code.to_string()))
}

async fn sandbox_clients(env: &SandboxEnv) -> Result<Clients> {
    let interceptor = TbankAuthInterceptor::new(&env.token)?;
    let channel = connect_sandbox_channel().await?;
    Ok(TbankGrpcClients::new(channel, interceptor))
}

async fn connect_sandbox_channel() -> Result<Channel> {
    connect_channel(SANDBOX_ENDPOINT, DEFAULT_REQUEST_TIMEOUT)
        .await
        .context("connect to T-Bank sandbox gRPC endpoint")
}

async fn sandbox_context() -> Result<(SandboxEnv, Clients)> {
    let env = SandboxEnv::from_env()?;
    let clients = sandbox_clients(&env).await?;
    Ok((env, clients))
}

#[cfg(feature = "sandbox-futures-tests")]
async fn sandbox_futures_context() -> Result<(SandboxEnv, Clients)> {
    init_sandbox_tracing();
    let env = SandboxEnv::from_futures_env()?;
    let clients = sandbox_clients(&env).await?;
    Ok((env, clients))
}

#[cfg(feature = "sandbox-futures-tests")]
fn init_sandbox_tracing() {
    static INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INIT.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::new("tbank.rpc=warn"))
            .try_init();
    });
}

fn sanitize_status(status: &Status, request_type: &str, env: &SandboxEnv) -> String {
    format!(
        "sandbox request failed: request_type={request_type} code={} token_present=true account_id_present={} endpoint_host=sandbox-invest-public-api.tbank.ru",
        status.code(),
        env.account_id.is_some()
    )
}

async fn call<T, F>(request_type: &str, env: &SandboxEnv, future: F) -> Result<T>
where
    F: Future<Output = std::result::Result<Response<T>, Status>>,
{
    future
        .await
        .map(Response::into_inner)
        .map_err(|status| anyhow!(sanitize_status(&status, request_type, env)))
}

async fn load_instrument(env: &SandboxEnv, clients: &mut Clients) -> Result<InstrumentMeta> {
    if env
        .instrument
        .class_code
        .eq_ignore_ascii_case(SPBFUT_CLASS_CODE)
    {
        let response = call(
            "InstrumentsService.FutureBy",
            env,
            clients.instruments.future_by(InstrumentRequest {
                id_type: InstrumentIdType::Ticker as i32,
                class_code: Some(env.instrument.class_code.clone()),
                id: env.instrument.ticker.clone(),
            }),
        )
        .await?;
        let future = response
            .instrument
            .context("FutureBy returned empty instrument")?;
        ensure!(
            !future.uid.is_empty(),
            "futures instrument_uid is empty for {}",
            env.instrument.env_value
        );
        ensure!(
            future.lot > 0,
            "futures lot must be positive for {}",
            env.instrument.env_value
        );
        ensure!(
            future.currency.eq_ignore_ascii_case(RUB_CURRENCY),
            "expected RUB futures, got currency={}",
            future.currency
        );
        let mut metadata = TbankInstrumentMetadata::from_future(&future)?;
        let margin = call(
            "InstrumentsService.GetFuturesMargin",
            env,
            clients.instruments.get_futures_margin(with_timeout(
                GetFuturesMarginRequest {
                    #[allow(deprecated)]
                    figi: String::new(),
                    instrument_id: future.uid.clone(),
                },
                DEFAULT_REQUEST_TIMEOUT,
            )),
        )
        .await?;
        metadata.update_futures_margin_contract(&margin)?;
        return sandbox_instrument_from_metadata(metadata);
    }

    let response = call(
        "InstrumentsService.ShareBy",
        env,
        clients.instruments.share_by(InstrumentRequest {
            id_type: InstrumentIdType::Ticker as i32,
            class_code: Some(env.instrument.class_code.clone()),
            id: env.instrument.ticker.clone(),
        }),
    )
    .await?;
    let share = response
        .instrument
        .context("ShareBy returned empty instrument")?;

    ensure!(
        !share.uid.is_empty(),
        "instrument_uid is empty for {}",
        env.instrument.env_value
    );
    ensure!(
        !share.figi.is_empty(),
        "figi is empty for {}",
        env.instrument.env_value
    );
    ensure!(
        share.lot > 0,
        "lot must be positive for {}",
        env.instrument.env_value
    );
    ensure!(
        share.currency.eq_ignore_ascii_case(RUB_CURRENCY),
        "expected RUB instrument, got currency={}",
        share.currency
    );
    let min_price_increment = quotation_to_decimal(
        share
            .min_price_increment
            .as_ref()
            .context("min_price_increment is missing")?,
    );
    ensure!(
        min_price_increment > Decimal::ZERO,
        "min_price_increment must be positive for {}",
        env.instrument.env_value
    );

    sandbox_instrument_from_metadata(TbankInstrumentMetadata::from_share(&share)?)
}

fn sandbox_instrument_from_metadata(metadata: TbankInstrumentMetadata) -> Result<InstrumentMeta> {
    ensure!(
        !metadata.instrument_uid.is_empty(),
        "instrument_uid is empty"
    );
    ensure!(metadata.lot > 0, "instrument lot must be positive");
    ensure!(
        metadata.min_price_increment > Decimal::ZERO,
        "instrument min_price_increment must be positive"
    );
    Ok(InstrumentMeta {
        ticker: metadata.ticker.clone(),
        class_code: metadata.class_code.clone(),
        instrument_uid: metadata.instrument_uid.clone(),
        figi: metadata.figi.clone(),
        position_uid: metadata.position_uid.clone(),
        lot: i32::try_from(metadata.lot)
            .with_context(|| format!("invalid instrument lot {}", metadata.lot))?,
        min_price_increment: metadata.min_price_increment,
        price_in_points: metadata.price_in_points,
        min_price_increment_amount: metadata.min_price_increment_amount,
        #[cfg(feature = "sandbox-futures-tests")]
        initial_margin_on_buy: metadata.initial_margin_on_buy,
        #[cfg(feature = "sandbox-futures-tests")]
        initial_margin_on_sell: metadata.initial_margin_on_sell,
        is_futures: metadata.price_in_points,
    })
}

fn adapter_instrument(instrument: &InstrumentMeta) -> Result<TbankInstrumentMetadata> {
    Ok(TbankInstrumentMetadata {
        instrument_id: format!("{}_{}.MOEX", instrument.ticker, instrument.class_code),
        ticker: instrument.ticker.clone(),
        class_code: instrument.class_code.clone(),
        figi: instrument.figi.clone(),
        instrument_uid: instrument.instrument_uid.clone(),
        position_uid: instrument.position_uid.clone(),
        lot: u32::try_from(instrument.lot)
            .with_context(|| format!("invalid lot {}", instrument.lot))?,
        min_price_increment: instrument.min_price_increment,
        currency: RUB_CURRENCY.to_string(),
        exchange: "MOEX".to_string(),
        price_precision: instrument.min_price_increment.normalize().scale(),
        quantity_precision: 0,
        price_in_points: instrument.price_in_points,
        min_price_increment_amount: instrument.min_price_increment_amount,
        ..Default::default()
    })
}

async fn sandbox_execution_client(
    env: &SandboxEnv,
    account_id: &str,
) -> Result<SandboxExecutionHarness> {
    sandbox_execution_client_with_trading(env, account_id, true).await
}

async fn sandbox_execution_client_with_trading(
    env: &SandboxEnv,
    account_id: &str,
    enable_trading: bool,
) -> Result<SandboxExecutionHarness> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(sender);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut client = TbankExecutionClient::new(
        execution_core(account_id, cache.clone()),
        TbankExecutionClientConfig {
            environment: TbankEnvironment::Sandbox,
            token: Some(env.token.clone()),
            account_id: Some(account_id.to_string()),
            endpoint: Some(SANDBOX_ENDPOINT.to_string()),
            enable_trading,
            ..TbankExecutionClientConfig::default()
        },
    );
    ExecutionClient::start(&mut client)?;
    let mut receiver = receiver;
    let initial_account_state = {
        // The production ExecutionEngine applies this event to the shared cache while connect
        // waits for readiness. This direct-client harness must perform the same boundary step.
        let connect = ExecutionClient::connect(&mut client);
        tokio::pin!(connect);
        loop {
            tokio::select! {
                result = &mut connect => {
                    result?;
                    bail!("execution client connected before publishing initial AccountState");
                }
                event = receiver.recv() => {
                    let event = event.context("execution event channel closed during connect")?;
                    if let ExecutionEvent::Account(state) = event {
                        cache.borrow_mut().update_account_state(&state)?;
                        connect.await?;
                        break state;
                    }
                }
            }
        }
    };
    Ok(SandboxExecutionHarness {
        client,
        events: receiver,
        initial_account_state,
    })
}

struct SandboxExecutionHarness {
    client: TbankExecutionClient,
    events: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    initial_account_state: AccountState,
}

impl SandboxExecutionHarness {
    async fn submit(&mut self, command: SubmitOrder) -> Result<OrderStatusReport> {
        submit_through_nautilus(&self.client, &mut self.events, command).await
    }

    async fn disconnect(&mut self) -> Result<()> {
        ExecutionClient::disconnect(&mut self.client).await
    }
}

fn submit_command(
    instrument: &TbankInstrumentMetadata,
    side: OrderSide,
    order_type: OrderType,
    time_in_force: TimeInForce,
    price: Option<Decimal>,
    trigger_price: Option<Decimal>,
) -> Result<SubmitOrder> {
    let ts_init = UnixNanos::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos() as u64,
    );
    let trader_id = TraderId::from("SANDBOX-TESTER");
    let strategy_id = StrategyId::from("SANDBOX-ACCEPTANCE");
    let instrument_id = InstrumentId::from(instrument.instrument_id.as_str());
    let client_order_id = ClientOrderId::from(order_request_id().as_str());
    let mut builder = OrderTestBuilder::new(order_type);
    builder
        .trader_id(trader_id)
        .strategy_id(strategy_id)
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(Quantity::from_decimal(Decimal::from(instrument.lot))?)
        .time_in_force(time_in_force)
        .ts_init(ts_init);
    if let Some(price) = price {
        builder.price(Price::from_decimal(price)?);
    }
    if let Some(trigger_price) = trigger_price {
        builder
            .trigger_price(Price::from_decimal(trigger_price)?)
            .trigger_type(TriggerType::LastPrice);
    }
    let order = builder.build();
    Ok(SubmitOrder::from_order(
        &order,
        trader_id,
        None,
        None,
        UUID4::new(),
        ts_init,
    ))
}

fn execution_event_kind(event: &ExecutionEvent) -> &'static str {
    match event {
        ExecutionEvent::Order(_) => "order",
        ExecutionEvent::OrderSubmittedBatch(_) => "order_submitted_batch",
        ExecutionEvent::OrderAcceptedBatch(_) => "order_accepted_batch",
        ExecutionEvent::OrderCanceledBatch(_) => "order_canceled_batch",
        ExecutionEvent::Report(ExecutionReport::Order(_)) => "order_report",
        ExecutionEvent::Report(ExecutionReport::Fill(_)) => "fill_report",
        ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, _)) => "order_with_fills_report",
        ExecutionEvent::Report(ExecutionReport::Position(_)) => "position_report",
        ExecutionEvent::Report(ExecutionReport::MassStatus(_)) => "mass_status_report",
        ExecutionEvent::Account(_) => "account",
    }
}

async fn recv_execution_event_matching<F>(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    description: &str,
    predicate: F,
) -> Result<ExecutionEvent>
where
    F: Fn(&ExecutionEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        tokio::select! {
            event = receiver.recv() => {
                let event = event.context("Nautilus execution event channel closed")?;
                if predicate(&event) {
                    return Ok(event);
                }
                seen.push(execution_event_kind(&event));
            }
            () = tokio::time::sleep_until(deadline) => {
                bail!("timed out waiting for {description}; seen_event_kinds={seen:?}");
            }
        }
    }
}

async fn submit_through_nautilus(
    client: &TbankExecutionClient,
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    command: SubmitOrder,
) -> Result<OrderStatusReport> {
    let client_order_id = command.client_order_id;
    ExecutionClient::submit_order(client, command)?;
    let submitted = recv_execution_event_matching(receiver, "Nautilus submit outcome", |event| {
        matches!(
            event,
            ExecutionEvent::Order(OrderEventAny::Submitted(submitted))
                if submitted.client_order_id == client_order_id
        ) || matches!(
            event,
            ExecutionEvent::Order(OrderEventAny::Rejected(rejected))
                if rejected.client_order_id == client_order_id
        )
    })
    .await?;
    if let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = submitted {
        bail!("Nautilus submit was rejected: {}", event.reason);
    }

    let report =
        recv_execution_event_matching(receiver, "matching Nautilus order report", |event| {
            match event {
                ExecutionEvent::Report(ExecutionReport::Order(report))
                | ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, _)) => {
                    report.client_order_id == Some(client_order_id)
                }
                _ => false,
            }
        })
        .await?;
    match report {
        ExecutionEvent::Report(ExecutionReport::Order(report))
        | ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, _)) => Ok(*report),
        _ => unreachable!("predicate only accepts matching order reports"),
    }
}

async fn cancel_through_nautilus(
    client: &TbankExecutionClient,
    report: &OrderStatusReport,
) -> Result<OrderStatusReport> {
    let client_order_id = report
        .client_order_id
        .context("order report has no client_order_id")?;
    ExecutionClient::cancel_order(
        client,
        CancelOrder::new(
            TraderId::from("SANDBOX-TESTER"),
            None,
            StrategyId::from("SANDBOX-ACCEPTANCE"),
            report.instrument_id,
            client_order_id,
            Some(report.venue_order_id),
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        ),
    )?;

    timeout(Duration::from_secs(30), async {
        loop {
            let current = ExecutionClient::generate_order_status_report(
                client,
                &GenerateOrderStatusReport::new(
                    UUID4::new(),
                    UnixNanos::from(1),
                    Some(report.instrument_id),
                    Some(client_order_id),
                    Some(report.venue_order_id),
                    None,
                    None,
                ),
            )
            .await?
            .context("cancelled order disappeared from Nautilus status report")?;
            if current.order_status == OrderStatus::Canceled {
                return Ok(current);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("timed out waiting for Nautilus canceled status")?
}

async fn last_price(
    env: &SandboxEnv,
    clients: &mut Clients,
    instrument_id: &str,
) -> Result<Decimal> {
    let response = call(
        "MarketDataService.GetLastPrices",
        env,
        clients.market_data.get_last_prices(GetLastPricesRequest {
            figi: Vec::new(),
            instrument_id: vec![instrument_id.to_string()],
            last_price_type: LastPriceType::LastPriceUnspecified as i32,
            instrument_status: None,
        }),
    )
    .await?;
    let price = response
        .last_prices
        .first()
        .and_then(|price| price.price.as_ref())
        .context("GetLastPrices returned no price")?;
    Ok(quotation_to_decimal(price))
}

async fn trading_status(
    env: &SandboxEnv,
    clients: &mut Clients,
    instrument_uid: &str,
) -> Result<generated::GetTradingStatusResponse> {
    call(
        "MarketDataService.GetTradingStatus",
        env,
        clients
            .market_data
            .get_trading_status(GetTradingStatusRequest {
                figi: None,
                instrument_id: Some(instrument_uid.to_string()),
            }),
    )
    .await
}

async fn require_sandbox_order_availability(
    env: &SandboxEnv,
    clients: &mut Clients,
    instrument: &InstrumentMeta,
    require_market: bool,
    test_name: &str,
) -> Result<bool> {
    let status = trading_status(env, clients, &instrument.instrument_uid).await?;
    let trading_status = SecurityTradingStatus::try_from(status.trading_status)
        .unwrap_or(SecurityTradingStatus::Unspecified);
    let order_flag = if require_market {
        status.market_order_available_flag
    } else {
        status.limit_order_available_flag
    };
    let trading_session_open = matches!(
        trading_status,
        SecurityTradingStatus::NormalTrading | SecurityTradingStatus::DealerNormalTrading
    );
    if status.api_trade_available_flag && order_flag && trading_session_open {
        return Ok(true);
    }
    eprintln!(
        "skipping {test_name}: sandbox instrument {} is not currently orderable; trading_status={trading_status:?} api_trade_available={} limit_available={} market_available={}",
        env.instrument.env_value,
        status.api_trade_available_flag,
        status.limit_order_available_flag,
        status.market_order_available_flag
    );
    Ok(false)
}

async fn rub_balance(env: &SandboxEnv, clients: &mut Clients, account_id: &str) -> Result<Decimal> {
    let response = call(
        "SandboxService.GetSandboxPositions",
        env,
        clients.sandbox.get_sandbox_positions(PositionsRequest {
            account_id: account_id.to_string(),
        }),
    )
    .await?;
    Ok(response
        .money
        .iter()
        .filter(|money| money.currency.eq_ignore_ascii_case(RUB_CURRENCY))
        .map(money_value_to_decimal)
        .sum())
}

async fn ensure_rub_balance(
    env: &SandboxEnv,
    clients: &mut Clients,
    account_id: &str,
    required: Decimal,
) -> Result<()> {
    let mut balance = rub_balance(env, clients, account_id).await?;
    if balance >= required {
        return Ok(());
    }

    let Some(pay_in) = env.pay_in_rub else {
        bail!(
            "insufficient RUB balance for sandbox order: required={} balance={} token_present=true account_id_present=true endpoint_host=sandbox-invest-public-api.tbank.ru; set {} to top up",
            required,
            balance,
            SANDBOX_PAY_IN_RUB_ENV
        );
    };

    ensure!(
        pay_in > Decimal::ZERO,
        "{} must be positive when set",
        SANDBOX_PAY_IN_RUB_ENV
    );
    call(
        "SandboxService.SandboxPayIn",
        env,
        clients.sandbox.sandbox_pay_in(SandboxPayInRequest {
            account_id: account_id.to_string(),
            amount: Some(decimal_to_money_value(RUB_CURRENCY, pay_in)?),
        }),
    )
    .await?;
    balance = rub_balance(env, clients, account_id).await?;
    ensure!(
        balance >= required,
        "RUB balance is still insufficient after pay-in: required={} balance={}",
        required,
        balance
    );
    Ok(())
}

#[cfg(feature = "sandbox-futures-tests")]
fn futures_initial_margin(instrument: &InstrumentMeta, side: OrderSide) -> Result<Decimal> {
    ensure!(instrument.is_futures, "expected a futures instrument");
    let margin = match side {
        OrderSide::Buy => instrument.initial_margin_on_buy,
        OrderSide::Sell => instrument.initial_margin_on_sell,
        _ => None,
    }
    .with_context(|| {
        format!(
            "FutureBy returned no initial margin for {} {:?}",
            instrument.ticker, side
        )
    })?;
    ensure!(
        margin > Decimal::ZERO,
        "initial margin must be positive for {} {:?}",
        instrument.ticker,
        side
    );
    Ok(margin)
}

#[cfg(feature = "sandbox-futures-tests")]
fn sandbox_operations_futures_trade_price_to_points(
    value: &generated::MoneyValue,
    instrument: &InstrumentMeta,
) -> Result<Decimal> {
    let tick_amount = instrument
        .min_price_increment_amount
        .context("futures metadata has no min price increment amount")?;
    futures_currency_to_points_without_tick_validation(
        money_value_to_decimal(value),
        instrument.min_price_increment,
        tick_amount,
    )
    .map_err(Into::into)
}

#[cfg(feature = "sandbox-futures-tests")]
fn ensure_futures_stop_wire_price(stop: &generated::StopOrder, expected: Decimal) -> Result<()> {
    let stop_price = stop
        .stop_price
        .as_ref()
        .context("sandbox futures stop order has no broker stop price")?;
    // GetSandboxStopOrders returns futures stop prices in points. Keep this
    // separate from GetSandboxOperations, whose legacy trade prices are
    // currency-valued and are normalized by the helper below.
    let actual = money_value_to_decimal(stop_price);
    ensure!(
        actual == expected,
        "sandbox futures stop wire price did not normalize to points: expected={expected} actual={actual}"
    );
    Ok(())
}

#[cfg(feature = "sandbox-futures-tests")]
fn sandbox_operations_trade_prices_in_points(
    operations: &generated::OperationsResponse,
    instrument: &InstrumentMeta,
) -> Result<Vec<Decimal>> {
    let mut prices = Vec::new();
    for operation in &operations.operations {
        for trade in &operation.trades {
            let price = trade
                .price
                .as_ref()
                .context("sandbox futures operation trade has no price")?;
            prices.push(sandbox_operations_futures_trade_price_to_points(
                price, instrument,
            )?);
        }
    }
    Ok(prices)
}

fn floor_to_tick(value: Decimal, tick: Decimal) -> Result<Decimal> {
    ensure!(tick > Decimal::ZERO, "tick must be positive");
    let units = (value / tick).floor();
    Ok((units * tick).max(tick).normalize())
}

fn ceil_to_tick(value: Decimal, tick: Decimal) -> Result<Decimal> {
    ensure!(tick > Decimal::ZERO, "tick must be positive");
    let units = (value / tick).ceil();
    Ok((units * tick).max(tick).normalize())
}

fn order_request_id() -> String {
    Uuid::new_v4().to_string()
}

fn current_test_unix_nanos() -> UnixNanos {
    UnixNanos::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos() as u64,
    )
}

fn recent_history_start() -> UnixNanos {
    UnixNanos::from(
        current_test_unix_nanos()
            .as_u64()
            .saturating_sub(60 * 60 * 1_000_000_000),
    )
}

async fn wait_for_order_reports(
    client: &TbankExecutionClient,
    instrument_id: InstrumentId,
    start: UnixNanos,
    predicate: impl Fn(&[OrderStatusReport]) -> bool,
) -> Result<Vec<OrderStatusReport>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        let reports = ExecutionClient::generate_order_status_reports(
            client,
            &GenerateOrderStatusReports::new(
                UUID4::new(),
                current_test_unix_nanos(),
                false,
                Some(instrument_id),
                Some(start),
                None,
                None,
                None,
            ),
        )
        .await?;
        for report in reports {
            if !seen
                .iter()
                .any(|seen: &OrderStatusReport| seen.venue_order_id == report.venue_order_id)
            {
                seen.push(report);
            }
        }
        if predicate(&seen) {
            return Ok(seen);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for expected Nautilus order reports; seen_semantics={:?}",
            seen.iter()
                .map(|report| (report.order_type, report.order_side, report.order_status))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_position_report(
    client: &TbankExecutionClient,
    instrument_id: InstrumentId,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let reports = ExecutionClient::generate_position_status_reports(
            client,
            &GeneratePositionStatusReports::new(
                UUID4::new(),
                current_test_unix_nanos(),
                Some(instrument_id),
                None,
                None,
                None,
                None,
            ),
        )
        .await?;
        if reports
            .iter()
            .any(|report| report.instrument_id == instrument_id)
        {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Nautilus position report"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_fill_reports(
    client: &TbankExecutionClient,
    instrument_id: InstrumentId,
    start: UnixNanos,
    predicate: impl Fn(&[FillReport]) -> bool,
) -> Result<Vec<FillReport>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        let reports = ExecutionClient::generate_fill_reports(
            client,
            GenerateFillReports::new(
                UUID4::new(),
                current_test_unix_nanos(),
                Some(instrument_id),
                None,
                Some(start),
                None,
                None,
                None,
            ),
        )
        .await?;
        for report in reports {
            if !seen.iter().any(|seen: &FillReport| {
                seen.venue_order_id == report.venue_order_id && seen.trade_id == report.trade_id
            }) {
                seen.push(report);
            }
        }
        if predicate(&seen) {
            return Ok(seen);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for expected Nautilus fill reports; seen_sides={:?}",
            seen.iter()
                .map(|report| report.order_side)
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_mass_order_report(
    client: &TbankExecutionClient,
    predicate: impl Fn(&[OrderStatusReport]) -> bool,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        let status = ExecutionClient::generate_mass_status(client, Some(60))
            .await?
            .context("Nautilus mass status was not generated")?;
        for report in status.order_reports().into_values() {
            if !seen
                .iter()
                .any(|seen: &OrderStatusReport| seen.venue_order_id == report.venue_order_id)
            {
                seen.push(report);
            }
        }
        if predicate(&seen) {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for expected Nautilus mass-status order report"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn timestamp_now_minus(seconds: i64) -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch");
    Timestamp {
        seconds: now.as_secs() as i64 - seconds,
        nanos: now.subsec_nanos() as i32,
    }
}

fn timestamp_now() -> Timestamp {
    timestamp_now_minus(0)
}

fn timestamp_plus_seconds(timestamp: &Timestamp, seconds: i64) -> Timestamp {
    Timestamp {
        seconds: timestamp.seconds + seconds,
        nanos: timestamp.nanos,
    }
}

struct CleanupGuard {
    env: SandboxEnv,
    clients: Clients,
    account_id: String,
    order_ids: Vec<String>,
    stop_order_ids: Vec<String>,
}

impl CleanupGuard {
    fn new(env: SandboxEnv, clients: Clients, account_id: String) -> Self {
        Self {
            env,
            clients,
            account_id,
            order_ids: Vec::new(),
            stop_order_ids: Vec::new(),
        }
    }

    fn track_order(&mut self, order_id: impl Into<String>) {
        self.order_ids.push(order_id.into());
    }

    fn track_stop_order(&mut self, stop_order_id: impl Into<String>) {
        self.stop_order_ids.push(stop_order_id.into());
    }

    async fn cleanup(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        for order_id in self.order_ids.clone() {
            if let Err(error) = self.cancel_order(&order_id).await {
                eprintln!("best-effort regular-order cancel failed: {error}");
            }
        }
        for stop_order_id in self.stop_order_ids.clone() {
            if let Err(error) = self.cancel_stop_order(&stop_order_id).await {
                eprintln!("best-effort stop-order cancel failed: {error}");
            }
        }

        if !self.order_ids.is_empty()
            && let Err(error) = self.assert_no_active_orders().await
        {
            errors.push(error.to_string());
        }
        if !self.stop_order_ids.is_empty()
            && let Err(error) = self.assert_no_active_stop_orders().await
        {
            errors.push(error.to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            bail!("sandbox cleanup failed: {}", errors.join("; "))
        }
    }

    async fn cancel_order(&mut self, order_id: &str) -> Result<()> {
        let result = self
            .clients
            .sandbox
            .cancel_sandbox_order(generated::CancelOrderRequest {
                account_id: self.account_id.clone(),
                order_id: order_id.to_string(),
                order_id_type: None,
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(anyhow!(sanitize_status(
                &status,
                "SandboxService.CancelSandboxOrder",
                &self.env
            ))),
        }
    }

    async fn cancel_stop_order(&mut self, stop_order_id: &str) -> Result<()> {
        let result = self
            .clients
            .sandbox
            .cancel_sandbox_stop_order(generated::CancelStopOrderRequest {
                account_id: self.account_id.clone(),
                stop_order_id: stop_order_id.to_string(),
            })
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(anyhow!(sanitize_status(
                &status,
                "SandboxService.CancelSandboxStopOrder",
                &self.env
            ))),
        }
    }

    async fn assert_no_active_orders(&mut self) -> Result<()> {
        let active = call(
            "SandboxService.GetSandboxOrders",
            &self.env,
            self.clients.sandbox.get_sandbox_orders(GetOrdersRequest {
                account_id: self.account_id.clone(),
                advanced_filters: None,
            }),
        )
        .await?;
        let active_ids: Vec<_> = active
            .orders
            .iter()
            .map(|order| order.order_id.as_str())
            .collect();
        let residual_count = self
            .order_ids
            .iter()
            .filter(|order_id| active_ids.contains(&order_id.as_str()))
            .count();
        ensure!(
            residual_count == 0,
            "cleanup left {residual_count} active sandbox order(s)"
        );
        Ok(())
    }

    async fn assert_no_active_stop_orders(&mut self) -> Result<()> {
        let active = call(
            "SandboxService.GetSandboxStopOrders",
            &self.env,
            self.clients
                .sandbox
                .get_sandbox_stop_orders(GetStopOrdersRequest {
                    account_id: self.account_id.clone(),
                    status: StopOrderStatusOption::StopOrderStatusActive as i32,
                    from: None,
                    to: None,
                }),
        )
        .await?;
        let active_ids: Vec<_> = active
            .stop_orders
            .iter()
            .map(|order| order.stop_order_id.as_str())
            .collect();
        let residual_count = self
            .stop_order_ids
            .iter()
            .filter(|stop_order_id| active_ids.contains(&stop_order_id.as_str()))
            .count();
        ensure!(
            residual_count == 0,
            "cleanup left {residual_count} active sandbox stop order(s)"
        );
        Ok(())
    }
}

struct MarketFillCleanupGuard {
    env: SandboxEnv,
    clients: Clients,
    account_id: String,
    position: Option<MarketFillPositionCleanup>,
}

struct MarketFillPositionCleanup {
    instrument: InstrumentMeta,
    baseline_quantity: i64,
    armed: bool,
}

impl MarketFillCleanupGuard {
    fn new(env: SandboxEnv, clients: Clients, account_id: String) -> Self {
        Self {
            env,
            clients,
            account_id,
            position: None,
        }
    }

    fn track_position(&mut self, instrument: InstrumentMeta, baseline_quantity: i64) {
        self.position = Some(MarketFillPositionCleanup {
            instrument,
            baseline_quantity,
            armed: false,
        });
    }

    fn arm(&mut self) {
        if let Some(position) = &mut self.position {
            position.armed = true;
        }
    }

    fn disarm(&mut self) {
        if let Some(position) = &mut self.position {
            position.armed = false;
        }
    }

    async fn cleanup(&mut self) -> Result<()> {
        let Some(position) = &self.position else {
            return Ok(());
        };
        if !position.armed {
            return Ok(());
        }
        let instrument = position.instrument.clone();
        let instrument_uid = instrument.instrument_uid.clone();
        let lot = instrument.lot;
        let baseline_quantity = position.baseline_quantity;
        let current =
            sandbox_position_quantity(&self.env, &mut self.clients, &self.account_id, &instrument)
                .await?;
        if current <= baseline_quantity {
            return Ok(());
        }

        let sell = post_market_order(
            &self.env,
            &self.account_id,
            &instrument,
            OrderDirection::Sell,
        )
        .await?;
        ensure!(
            sell.order_status == OrderStatus::Filled,
            "market-fill cleanup sell did not fill: status={} residual_position={} lot={}",
            sell.order_status,
            current,
            lot
        );

        let after =
            sandbox_position_quantity(&self.env, &mut self.clients, &self.account_id, &instrument)
                .await?;
        ensure!(
            after <= baseline_quantity,
            "market-fill cleanup left residual position: baseline={} after={} instrument_uid={}",
            baseline_quantity,
            after,
            instrument_uid
        );
        Ok(())
    }
}

async fn finish_with_cleanup<T>(
    body: std::result::Result<Result<T>, Box<dyn std::any::Any + Send>>,
    cleanup: Result<()>,
) -> Result<T> {
    match (body, cleanup) {
        (Ok(Ok(value)), Ok(())) => Ok(value),
        (Ok(Err(body_error)), Ok(())) => Err(body_error),
        (Ok(Ok(_)), Err(cleanup_error)) => Err(cleanup_error),
        (Ok(Err(body_error)), Err(cleanup_error)) => {
            Err(body_error.context(format!("cleanup also failed: {cleanup_error}")))
        }
        (Err(panic), Ok(())) => std::panic::resume_unwind(panic),
        (Err(panic), Err(cleanup_error)) => {
            eprintln!("cleanup after panic failed: {cleanup_error}");
            std::panic::resume_unwind(panic)
        }
    }
}

fn subscription_status_name(status: i32) -> &'static str {
    match SubscriptionStatus::try_from(status) {
        Ok(value) => value.as_str_name(),
        Err(_) => "SUBSCRIPTION_STATUS_UNKNOWN",
    }
}

fn stock_quantity_from_positions(
    response: &generated::PositionsResponse,
    instrument_uid: &str,
) -> i64 {
    response
        .securities
        .iter()
        .find(|position| position.instrument_uid == instrument_uid)
        .map(|position| position.balance)
        .unwrap_or_default()
}

fn futures_quantity_from_positions(
    response: &generated::PositionsResponse,
    instrument_uid: &str,
) -> i64 {
    response
        .futures
        .iter()
        .find(|position| position.instrument_uid == instrument_uid)
        .map(|position| position.balance)
        .unwrap_or_default()
}

fn instrument_quantity_from_positions(
    response: &generated::PositionsResponse,
    instrument: &InstrumentMeta,
) -> i64 {
    if instrument.is_futures {
        futures_quantity_from_positions(response, &instrument.instrument_uid)
    } else {
        stock_quantity_from_positions(response, &instrument.instrument_uid)
    }
}

fn one_order_position_delta(instrument: &InstrumentMeta) -> i64 {
    if instrument.is_futures {
        1
    } else {
        i64::from(instrument.lot)
    }
}

async fn sandbox_position_quantity(
    env: &SandboxEnv,
    clients: &mut Clients,
    account_id: &str,
    instrument: &InstrumentMeta,
) -> Result<i64> {
    for attempt in 0..3_u64 {
        match clients
            .sandbox
            .get_sandbox_positions(with_timeout(
                PositionsRequest {
                    account_id: account_id.to_string(),
                },
                DEFAULT_REQUEST_TIMEOUT,
            ))
            .await
        {
            Ok(response) => {
                return Ok(instrument_quantity_from_positions(
                    &response.into_inner(),
                    instrument,
                ));
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unknown | tonic::Code::Unavailable | tonic::Code::Internal
                ) && attempt < 2 =>
            {
                tokio::time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
            }
            Err(status) => {
                return Err(anyhow!(sanitize_status(
                    &status,
                    "SandboxService.GetSandboxPositions",
                    env,
                )));
            }
        }
    }
    unreachable!("sandbox position retry loop always returns")
}

async fn require_existing_sandbox_account(
    env: &SandboxEnv,
    clients: &mut Clients,
    account_id: &str,
) -> Result<()> {
    let accounts = call(
        "SandboxService.GetSandboxAccounts",
        env,
        clients
            .sandbox
            .get_sandbox_accounts(GetAccountsRequest { status: None }),
    )
    .await?;
    ensure!(
        accounts
            .accounts
            .iter()
            .any(|account| account.id == account_id),
        "configured sandbox account id was not returned by GetSandboxAccounts: token_present=true account_id_present=true endpoint_host=sandbox-invest-public-api.tbank.ru"
    );
    Ok(())
}

async fn run_sandbox_preflight() -> Result<()> {
    let (env, mut clients) = sandbox_context().await?;
    let account_id = env
        .account_id
        .as_deref()
        .context("account id required for sandbox preflight")?;

    require_existing_sandbox_account(&env, &mut clients, account_id).await?;
    let portfolio = call(
        "SandboxService.GetSandboxPortfolio",
        &env,
        clients.sandbox.get_sandbox_portfolio(PortfolioRequest {
            account_id: account_id.to_string(),
            currency: None,
        }),
    )
    .await?;
    ensure!(
        portfolio.account_id == account_id,
        "sandbox preflight portfolio account id mismatch"
    );
    Ok(())
}

async fn require_sandbox_preflight() -> Result<()> {
    SANDBOX_PREFLIGHT
        .get_or_init(|| async {
            run_sandbox_preflight().await.map_err(|error| {
                format!(
                    "sandbox preflight failed before mutation; no order was submitted: {error:#}"
                )
            })
        })
        .await
        .clone()
        .map_err(anyhow::Error::msg)
}

#[cfg(feature = "sandbox-futures-tests")]
async fn require_futures_sandbox_preflight(
    env: &SandboxEnv,
    clients: &mut Clients,
    account_id: &str,
) -> Result<()> {
    require_existing_sandbox_account(env, clients, account_id).await?;
    let portfolio = call(
        "SandboxService.GetSandboxPortfolio",
        env,
        clients.sandbox.get_sandbox_portfolio(PortfolioRequest {
            account_id: account_id.to_string(),
            currency: None,
        }),
    )
    .await?;
    ensure!(
        portfolio.account_id == account_id,
        "futures sandbox preflight portfolio account id mismatch"
    );
    Ok(())
}

async fn post_market_order(
    env: &SandboxEnv,
    account_id: &str,
    instrument: &InstrumentMeta,
    direction: OrderDirection,
) -> Result<OrderStatusReport> {
    let metadata = adapter_instrument(instrument)?;
    let mut execution = sandbox_execution_client(env, account_id)
        .await
        .context("connect execution client for sandbox market order")?;
    submit_market_order_with_client(&mut execution, &metadata, direction).await
}

async fn submit_market_order_with_client(
    execution: &mut SandboxExecutionHarness,
    instrument: &TbankInstrumentMetadata,
    direction: OrderDirection,
) -> Result<OrderStatusReport> {
    let side = match direction {
        OrderDirection::Buy => OrderSide::Buy,
        OrderDirection::Sell => OrderSide::Sell,
        _ => bail!("unsupported market test direction {direction:?}"),
    };
    execution
        .submit(submit_command(
            instrument,
            side,
            OrderType::Market,
            TimeInForce::Ioc,
            None,
            None,
        )?)
        .await
}

#[cfg(all(feature = "sandbox-tests", not(feature = "sandbox-futures-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_00_preflight() -> Result<()> {
    require_sandbox_preflight().await
}

#[cfg(all(feature = "sandbox-tests", not(feature = "sandbox-futures-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_readonly() -> Result<()> {
    let (env, mut clients) = sandbox_context().await?;

    let accounts = call(
        "SandboxService.GetSandboxAccounts",
        &env,
        clients
            .sandbox
            .get_sandbox_accounts(GetAccountsRequest { status: None }),
    )
    .await?;
    let account_ids: Vec<_> = accounts
        .accounts
        .iter()
        .map(|account| account.id.as_str())
        .collect();
    eprintln!("sandbox accounts returned: {}", account_ids.len());
    let readonly_account_id = env
        .account_id
        .as_ref()
        .filter(|account_id| account_ids.contains(&account_id.as_str()));
    if env.account_id.is_some() && readonly_account_id.is_none() {
        eprintln!(
            "skipping sandbox portfolio/positions: configured account_id was not returned by GetSandboxAccounts"
        );
    }

    let instrument = load_instrument(&env, &mut clients).await?;
    ensure!(instrument.lot > 0, "instrument lot must be positive");
    ensure!(
        !instrument.figi.is_empty(),
        "instrument figi must be present"
    );
    ensure!(
        !instrument.instrument_uid.is_empty(),
        "instrument uid must be present"
    );

    let last_prices = call(
        "MarketDataService.GetLastPrices",
        &env,
        clients.market_data.get_last_prices(GetLastPricesRequest {
            figi: Vec::new(),
            instrument_id: vec![instrument.instrument_uid.clone()],
            last_price_type: LastPriceType::LastPriceUnspecified as i32,
            instrument_status: None,
        }),
    )
    .await?;
    ensure!(
        !last_prices.last_prices.is_empty(),
        "GetLastPrices returned no data"
    );

    let daily_candles = call(
        "MarketDataService.GetCandles",
        &env,
        clients.market_data.get_candles(GetCandlesRequest {
            figi: Some(instrument.figi.clone()),
            from: Some(timestamp_now_minus(21 * SECONDS_PER_DAY)),
            to: Some(timestamp_now()),
            interval: CandleInterval::Day as i32,
            instrument_id: None,
            candle_source_type: None,
            limit: None,
        }),
    )
    .await?;
    let latest_daily_time = daily_candles
        .candles
        .iter()
        .filter_map(|candle| candle.time.as_ref())
        .max_by_key(|timestamp| (timestamp.seconds, timestamp.nanos))
        .cloned()
        .context("GetCandles returned no daily candles in the last 21 days")?;

    let candles = call(
        "MarketDataService.GetCandles",
        &env,
        clients.market_data.get_candles(GetCandlesRequest {
            figi: Some(instrument.figi.clone()),
            from: Some(latest_daily_time),
            to: Some(timestamp_plus_seconds(
                &latest_daily_time,
                SECONDS_PER_DAY - 1,
            )),
            interval: CandleInterval::CandleInterval1Min as i32,
            instrument_id: None,
            candle_source_type: None,
            limit: None,
        }),
    )
    .await?;
    ensure!(
        !candles.candles.is_empty(),
        "GetCandles returned no 1-minute candles for the latest daily candle window"
    );

    let order_book = call(
        "MarketDataService.GetOrderBook",
        &env,
        clients.market_data.get_order_book(GetOrderBookRequest {
            figi: None,
            depth: 10,
            instrument_id: Some(instrument.instrument_uid.clone()),
        }),
    )
    .await?;
    ensure!(
        order_book.depth == 10,
        "GetOrderBook returned unexpected depth"
    );

    let status = call(
        "MarketDataService.GetTradingStatus",
        &env,
        clients
            .market_data
            .get_trading_status(GetTradingStatusRequest {
                figi: None,
                instrument_id: Some(instrument.instrument_uid.clone()),
            }),
    )
    .await?;
    ensure!(
        status.instrument_uid == instrument.instrument_uid || !status.figi.is_empty(),
        "GetTradingStatus returned no instrument identity"
    );

    let stream = call(
        "MarketDataStreamService.MarketDataServerSideStream",
        &env,
        clients.market_data_stream.market_data_server_side_stream(
            generated::MarketDataServerSideStreamRequest {
                subscribe_candles_request: None,
                subscribe_order_book_request: None,
                subscribe_trades_request: None,
                subscribe_info_request: None,
                subscribe_last_price_request: Some(SubscribeLastPriceRequest {
                    subscription_action: SubscriptionAction::Subscribe as i32,
                    instruments: vec![generated::LastPriceInstrument {
                        figi: String::new(),
                        instrument_id: instrument.instrument_uid.clone(),
                    }],
                }),
                ping_settings: Some(generated::PingDelaySettings {
                    ping_delay_ms: Some(5_000),
                }),
            },
        ),
    )
    .await?;
    let mut stream = stream;
    let event = timeout(Duration::from_secs(20), stream.next())
        .await
        .context("market data stream timeout")?
        .context("market data stream ended without events")?
        .map_err(|status| {
            anyhow!(sanitize_status(
                &status,
                "MarketDataStreamService.MarketDataServerSideStream",
                &env
            ))
        })?;
    match event
        .payload
        .context("market data stream event had empty payload")?
    {
        MarketDataPayload::SubscribeLastPriceResponse(response) => {
            let subscription = response
                .last_price_subscriptions
                .first()
                .context("last price subscription response is empty")?;
            ensure!(
                subscription.subscription_status == SubscriptionStatus::Success as i32,
                "last price subscription failed: {}",
                subscription_status_name(subscription.subscription_status)
            );
        }
        MarketDataPayload::LastPrice(_)
        | MarketDataPayload::TradingStatus(_)
        | MarketDataPayload::Ping(_) => {}
        other => bail!("unexpected stream payload: {other:?}"),
    }

    if let Some(account_id) = readonly_account_id {
        let portfolio = call(
            "SandboxService.GetSandboxPortfolio",
            &env,
            clients.sandbox.get_sandbox_portfolio(PortfolioRequest {
                account_id: account_id.clone(),
                currency: None,
            }),
        )
        .await?;
        ensure!(
            portfolio.account_id == *account_id,
            "portfolio account id mismatch"
        );

        let positions = call(
            "SandboxService.GetSandboxPositions",
            &env,
            clients.sandbox.get_sandbox_positions(PositionsRequest {
                account_id: account_id.clone(),
            }),
        )
        .await?;
        let shares_quantity = stock_quantity_from_positions(&positions, &instrument.instrument_uid);
        let orders = call(
            "SandboxService.GetSandboxOrders",
            &env,
            clients.sandbox.get_sandbox_orders(GetOrdersRequest {
                account_id: account_id.clone(),
                advanced_filters: None,
            }),
        )
        .await?;
        let stop_orders = call(
            "SandboxService.GetSandboxStopOrders",
            &env,
            clients
                .sandbox
                .get_sandbox_stop_orders(GetStopOrdersRequest {
                    account_id: account_id.clone(),
                    status: StopOrderStatusOption::StopOrderStatusActive as i32,
                    from: None,
                    to: None,
                }),
        )
        .await?;
        eprintln!(
            "sandbox residual state: instrument_shares={} active_orders={} active_stop_orders={}",
            shares_quantity,
            orders.orders.len(),
            stop_orders.stop_orders.len()
        );

        let execution = sandbox_execution_client_with_trading(&env, account_id, false)
            .await
            .context("connect read-only execution client")?;
        ensure!(
            execution.initial_account_state.account_id
                == ExecutionClient::account_id(&execution.client)
        );
    }

    Ok(())
}

#[cfg(all(feature = "sandbox-tests", not(feature = "sandbox-futures-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_order_lifecycle() -> Result<()> {
    require_sandbox_preflight().await?;
    let (env, mut clients) = sandbox_context().await?;
    let account_id = env
        .account_id
        .clone()
        .context("account id required for sandbox order-lifecycle test")?;
    require_existing_sandbox_account(&env, &mut clients, &account_id).await?;
    let mut cleanup = CleanupGuard::new(env.clone(), clients.clone(), account_id.clone());

    let body = AssertUnwindSafe(async {
        let instrument = load_instrument(&env, &mut clients).await?;
        if !require_sandbox_order_availability(
            &env,
            &mut clients,
            &instrument,
            false,
            "sandbox order-lifecycle test",
        )
        .await?
        {
            return Ok(());
        }
        let adapter_instrument = adapter_instrument(&instrument)?;
        let mut execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect initial execution client for limit order")?;
        let last_price = last_price(&env, &mut clients, &instrument.instrument_uid).await?;
        let buy_limit_price = floor_to_tick(
            last_price * Decimal::new(50, 2),
            instrument.min_price_increment,
        )?;
        let required_cash = buy_limit_price * Decimal::from(instrument.lot);
        ensure_rub_balance(&env, &mut clients, &account_id, required_cash).await?;
        let history_start = recent_history_start();

        let order = execution
            .submit(submit_command(
                &adapter_instrument,
                OrderSide::Buy,
                OrderType::Limit,
                TimeInForce::Day,
                Some(buy_limit_price),
                None,
            )?)
            .await?;
        cleanup.track_order(order.venue_order_id.to_string());
        ensure!(
            matches!(
                order.order_status,
                OrderStatus::Accepted | OrderStatus::PartiallyFilled
            ),
            "unexpected limit order status before cancel: {}",
            order.order_status
        );

        execution.disconnect().await?;
        let execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("reconnect execution client for limit-order recovery")?;
        let recovered = ExecutionClient::generate_order_status_report(
            &execution.client,
            &GenerateOrderStatusReport::new(
                UUID4::new(),
                UnixNanos::from(1),
                Some(order.instrument_id),
                order.client_order_id,
                Some(order.venue_order_id),
                None,
                None,
            ),
        )
        .await?
        .context(
            "restarted client did not recover the resting order through Nautilus report API",
        )?;
        ensure!(recovered.client_order_id == order.client_order_id);
        ensure!(recovered.venue_order_id == order.venue_order_id);

        let state = cancel_through_nautilus(&execution.client, &recovered).await?;
        ensure!(
            state.order_status == OrderStatus::Canceled,
            "cancelled limit order has unexpected state: {}",
            state.order_status
        );

        let mut execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for stop order")?;

        let stop_price = ceil_to_tick(
            last_price * Decimal::from(2),
            instrument.min_price_increment,
        )?;
        let stop_order = execution
            .submit(submit_command(
                &adapter_instrument,
                OrderSide::Buy,
                OrderType::StopMarket,
                TimeInForce::Gtc,
                None,
                Some(stop_price),
            )?)
            .await?;
        cleanup.track_stop_order(stop_order.venue_order_id.to_string());
        let stopped = cancel_through_nautilus(&execution.client, &stop_order).await?;
        ensure!(stopped.order_status == OrderStatus::Canceled);
        wait_for_order_reports(
            &execution.client,
            order.instrument_id,
            history_start,
            |reports| {
                reports.iter().any(|report| {
                    report.order_type == OrderType::StopMarket
                        && report.order_side == OrderSide::Buy
                        && report.order_status == OrderStatus::Canceled
                })
            },
        )
        .await?;
        let mass_execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for mass-status recovery")?;
        wait_for_mass_order_report(&mass_execution.client, |reports| {
            reports.iter().any(|report| {
                report.order_type == OrderType::StopMarket
                    && report.order_side == OrderSide::Buy
                    && report.order_status == OrderStatus::Canceled
            })
        })
        .await?;

        Ok(())
    })
    .catch_unwind()
    .await;
    let cleanup_result = cleanup.cleanup().await;
    finish_with_cleanup(body, cleanup_result).await
}

#[cfg(all(feature = "sandbox-tests", not(feature = "sandbox-futures-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_market_fill() -> Result<()> {
    require_sandbox_preflight().await?;
    let (env, mut clients) = sandbox_context().await?;
    let account_id = env
        .account_id
        .clone()
        .context("account id required for market-fill sandbox test")?;
    require_existing_sandbox_account(&env, &mut clients, &account_id).await?;

    let instrument = load_instrument(&env, &mut clients).await?;
    if !require_sandbox_order_availability(
        &env,
        &mut clients,
        &instrument,
        true,
        "market-fill sandbox test",
    )
    .await?
    {
        return Ok(());
    }
    let price = last_price(&env, &mut clients, &instrument.instrument_uid).await?;
    let required_cash = price * Decimal::from(instrument.lot);
    ensure_rub_balance(&env, &mut clients, &account_id, required_cash).await?;

    let before = sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
    let mut cleanup = MarketFillCleanupGuard::new(env.clone(), clients.clone(), account_id.clone());
    cleanup.track_position(instrument.clone(), before);

    let body = AssertUnwindSafe(async {
        let adapter_instrument = adapter_instrument(&instrument)?;
        let mut execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for market-fill buy")?;
        let history_start = recent_history_start();
        cleanup.arm();
        let buy = submit_market_order_with_client(
            &mut execution,
            &adapter_instrument,
            OrderDirection::Buy,
        )
        .await?;
        ensure!(
            buy.order_status == OrderStatus::Filled,
            "market buy did not fill: status={}",
            buy.order_status
        );

        wait_for_position_report(&execution.client, buy.instrument_id).await?;

    let after_buy = sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
    ensure!(
        after_buy >= before + one_order_position_delta(&instrument),
        "position did not increase by one lot after buy: before={before} after_buy={after_buy} lot={}",
        instrument.lot
    );

    let mut sell_execution = sandbox_execution_client(&env, &account_id)
        .await
        .context("connect execution client for market-fill cleanup sell")?;
    let sell = submit_market_order_with_client(
        &mut sell_execution,
        &adapter_instrument,
        OrderDirection::Sell,
    )
    .await?;
    ensure!(
        sell.order_status == OrderStatus::Filled,
        "market sell did not fill: status={} residual_position={}",
        sell.order_status,
        after_buy
    );

    let after_sell = sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
    ensure!(
        after_sell == before,
        "market-fill test left residual position: before={before} after_sell={after_sell} instrument_uid={}",
        instrument.instrument_uid
    );
    cleanup.disarm();

    let reports_execution = sandbox_execution_client(&env, &account_id)
        .await
        .context("connect execution client for market-fill report recovery")?;
    wait_for_fill_reports(
        &reports_execution.client,
        buy.instrument_id,
        history_start,
        |reports| {
            reports
                .iter()
                .any(|report| report.order_side == OrderSide::Buy)
                && reports
                    .iter()
                    .any(|report| report.order_side == OrderSide::Sell)
        },
    )
    .await?;

    let operations = call(
        "SandboxService.GetSandboxOperations",
        &env,
        clients
            .sandbox
            .get_sandbox_operations(generated::OperationsRequest {
                account_id: account_id.clone(),
                from: Some(timestamp_now_minus(60 * 60)),
                to: Some(timestamp_now()),
                state: None,
                figi: Some(instrument.figi.clone()),
            }),
    )
    .await?;
    ensure!(
        operations
            .operations
            .iter()
            .any(|operation| !operation.trades.is_empty()),
        "sandbox operations did not expose fills for recent market round trip"
    );

    Ok(())
    })
    .catch_unwind()
    .await;
    let cleanup_result = cleanup.cleanup().await;
    finish_with_cleanup(body, cleanup_result).await
}

#[cfg(all(feature = "sandbox-futures-tests", not(feature = "sandbox-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_futures_readonly() -> Result<()> {
    let (env, mut clients) = sandbox_futures_context().await?;
    let account_id = env
        .account_id
        .clone()
        .context("account id required for futures read-only sandbox test")?;
    require_futures_sandbox_preflight(&env, &mut clients, &account_id).await?;

    let instrument = load_instrument(&env, &mut clients).await?;
    ensure!(
        instrument.is_futures,
        "futures sandbox resolved a non-futures instrument"
    );
    let position_quantity =
        sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
    let active_stops = call(
        "SandboxService.GetSandboxStopOrders",
        &env,
        clients
            .sandbox
            .get_sandbox_stop_orders(GetStopOrdersRequest {
                account_id,
                status: StopOrderStatusOption::StopOrderStatusActive as i32,
                from: None,
                to: None,
            }),
    )
    .await?;
    let instrument_stop_count = active_stops
        .stop_orders
        .iter()
        .filter(|stop| stop.instrument_uid == instrument.instrument_uid)
        .count();

    eprintln!(
        "sandbox futures residual state: instrument_position={position_quantity} active_stop_orders={instrument_stop_count}"
    );
    ensure!(
        position_quantity == 0,
        "futures read-only probe found residual instrument position: quantity={position_quantity}"
    );
    ensure!(
        instrument_stop_count == 0,
        "futures read-only probe found active instrument stop orders: count={instrument_stop_count}"
    );
    Ok(())
}

#[cfg(all(feature = "sandbox-futures-tests", not(feature = "sandbox-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_futures_market_fill() -> Result<()> {
    let (env, mut clients) = sandbox_futures_context().await?;
    let account_id = env
        .account_id
        .clone()
        .context("account id required for futures market-fill sandbox test")?;
    require_futures_sandbox_preflight(&env, &mut clients, &account_id).await?;

    let instrument = load_instrument(&env, &mut clients).await?;
    ensure!(
        instrument.is_futures,
        "futures sandbox resolved a non-futures instrument"
    );
    ensure!(
        !instrument.figi.is_empty(),
        "FutureBy returned no FIGI required for operations acceptance"
    );
    if !require_sandbox_order_availability(
        &env,
        &mut clients,
        &instrument,
        true,
        "futures market-fill sandbox test",
    )
    .await?
    {
        return Ok(());
    }

    let price = last_price(&env, &mut clients, &instrument.instrument_uid).await?;
    ensure!(price > Decimal::ZERO, "futures last price must be positive");
    ensure_rub_balance(
        &env,
        &mut clients,
        &account_id,
        futures_initial_margin(&instrument, OrderSide::Buy)?,
    )
    .await?;

    let before = sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
    let mut cleanup = MarketFillCleanupGuard::new(env.clone(), clients.clone(), account_id.clone());
    cleanup.track_position(instrument.clone(), before);

    let body = AssertUnwindSafe(async {
        let adapter_instrument = adapter_instrument(&instrument)?;
        let mut execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for futures market-fill buy")?;
        let history_start = recent_history_start();
        cleanup.arm();
        let buy = submit_market_order_with_client(
            &mut execution,
            &adapter_instrument,
            OrderDirection::Buy,
        )
        .await?;
        ensure!(
            buy.order_status == OrderStatus::Filled,
            "futures market buy did not fill: status={}",
            buy.order_status
        );
        wait_for_position_report(&execution.client, buy.instrument_id).await?;

        let after_buy =
            sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
        ensure!(
            after_buy >= before + one_order_position_delta(&instrument),
            "futures position did not increase by one lot after buy: before={before} after_buy={after_buy} lot={}",
            instrument.lot
        );

        let mut sell_execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for futures market-fill cleanup sell")?;
        let sell = submit_market_order_with_client(
            &mut sell_execution,
            &adapter_instrument,
            OrderDirection::Sell,
        )
        .await?;
        ensure!(
            sell.order_status == OrderStatus::Filled,
            "futures market sell did not fill: status={} residual_position={}",
            sell.order_status,
            after_buy
        );

        let after_sell =
            sandbox_position_quantity(&env, &mut clients, &account_id, &instrument).await?;
        ensure!(
            after_sell == before,
            "futures market-fill left residual position: before={before} after_sell={after_sell}"
        );
        cleanup.disarm();

        let reports_execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for futures fill-report recovery")?;
        let reports = wait_for_fill_reports(
            &reports_execution.client,
            buy.instrument_id,
            history_start,
            |reports| {
                reports
                    .iter()
                    .any(|report| report.order_side == OrderSide::Buy)
                    && reports
                        .iter()
                        .any(|report| report.order_side == OrderSide::Sell)
            },
        )
        .await?;

        let operations = call(
            "SandboxService.GetSandboxOperations",
            &env,
            clients
                .sandbox
                .get_sandbox_operations(generated::OperationsRequest {
                    account_id: account_id.clone(),
                    from: Some(timestamp_now_minus(60 * 60)),
                    to: Some(timestamp_now()),
                    state: None,
                    figi: Some(instrument.figi.clone()),
                }),
        )
        .await?;
        let expected_prices = sandbox_operations_trade_prices_in_points(&operations, &instrument)?;
        ensure!(
            !expected_prices.is_empty(),
            "sandbox futures operations did not expose fill prices"
        );

        for report in reports.iter() {
            ensure!(
                expected_prices
                    .iter()
                    .any(|expected| *expected == report.last_px.as_decimal()),
                "futures fill report price is not in Nautilus points: last_px={}",
                report.last_px
            );
        }
        for (label, report) in [("buy", &buy), ("sell", &sell)] {
            let avg_px = report
                .avg_px
                .with_context(|| format!("futures {label} report has no average price"))?;
            ensure!(
                expected_prices.contains(&avg_px),
                "futures {label} order report average price is not in Nautilus points: avg_px={avg_px}"
            );
        }

        Ok(())
    })
    .catch_unwind()
    .await;
    let cleanup_result = cleanup.cleanup().await;
    finish_with_cleanup(body, cleanup_result).await
}

#[cfg(all(feature = "sandbox-futures-tests", not(feature = "sandbox-tests")))]
#[tokio::test]
#[ignore]
async fn sandbox_futures_stop_lifecycle() -> Result<()> {
    let (env, mut clients) = sandbox_futures_context().await?;
    let account_id = env
        .account_id
        .clone()
        .context("account id required for futures stop-order sandbox test")?;
    require_futures_sandbox_preflight(&env, &mut clients, &account_id)
        .await
        .context("futures stop-order sandbox preflight")?;

    let instrument = load_instrument(&env, &mut clients)
        .await
        .context("load futures instrument for stop-order sandbox test")?;
    ensure!(
        instrument.is_futures,
        "futures sandbox resolved a non-futures instrument"
    );
    if !require_sandbox_order_availability(
        &env,
        &mut clients,
        &instrument,
        true,
        "futures stop-order sandbox test",
    )
    .await
    .context("check futures stop-order sandbox availability")?
    {
        return Ok(());
    }
    ensure_rub_balance(
        &env,
        &mut clients,
        &account_id,
        futures_initial_margin(&instrument, OrderSide::Buy)?,
    )
    .await
    .context("ensure futures stop-order sandbox margin balance")?;
    let last_price = last_price(&env, &mut clients, &instrument.instrument_uid)
        .await
        .context("read futures stop-order sandbox last price")?;
    ensure!(
        last_price > Decimal::ZERO,
        "futures last price must be positive"
    );
    let stop_price = ceil_to_tick(
        last_price * Decimal::from(2),
        instrument.min_price_increment,
    )?;
    let history_start = recent_history_start();
    let mut cleanup = CleanupGuard::new(env.clone(), clients.clone(), account_id.clone());

    let body = AssertUnwindSafe(async {
        let adapter_instrument = adapter_instrument(&instrument)?;
        let mut execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for futures stop order")?;
        let submitted = execution
            .submit(submit_command(
                &adapter_instrument,
                OrderSide::Buy,
                OrderType::StopMarket,
                TimeInForce::Gtc,
                None,
                Some(stop_price),
            )?)
            .await
            .context("submit futures stop order through Nautilus")?;
        cleanup.track_stop_order(submitted.venue_order_id.to_string());
        ensure!(
            submitted.order_status == OrderStatus::Accepted,
            "futures stop order was not accepted: {}",
            submitted.order_status
        );
        ensure!(submitted.order_type == OrderType::StopMarket);
        ensure_eq_decimal_price(
            submitted.trigger_price.as_ref(),
            stop_price,
            "futures stop submit trigger price",
        )?;
        let broker_stops = call(
            "SandboxService.GetSandboxStopOrders",
            &env,
            clients
                .sandbox
                .get_sandbox_stop_orders(GetStopOrdersRequest {
                    account_id: account_id.clone(),
                    status: StopOrderStatusOption::StopOrderStatusActive as i32,
                    from: None,
                    to: None,
                }),
        )
        .await?;
        let broker_stop = broker_stops
            .stop_orders
            .iter()
            .find(|stop| stop.stop_order_id == submitted.venue_order_id.to_string())
            .context("sandbox futures stop order was not returned by the broker")?;
        ensure_futures_stop_wire_price(broker_stop, stop_price)?;

        let client_order_id = submitted
            .client_order_id
            .context("futures stop submit report has no client_order_id")?;
        let venue_order_id = submitted.venue_order_id;
        execution.disconnect().await?;

        let execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("reconnect execution client for futures stop recovery")?;
        let recovered = ExecutionClient::generate_order_status_report(
            &execution.client,
            &GenerateOrderStatusReport::new(
                UUID4::new(),
                UnixNanos::from(1),
                Some(submitted.instrument_id),
                Some(client_order_id),
                Some(venue_order_id),
                None,
                None,
            ),
        )
        .await?
        .context("restarted client did not recover the futures stop order")?;
        ensure!(recovered.order_type == OrderType::StopMarket);
        ensure!(recovered.order_status == OrderStatus::Accepted);
        ensure_eq_decimal_price(
            recovered.trigger_price.as_ref(),
            stop_price,
            "futures stop recovery trigger price",
        )?;

        let canceled = cancel_through_nautilus(&execution.client, &recovered).await?;
        ensure!(canceled.order_status == OrderStatus::Canceled);
        let reports = wait_for_order_reports(
            &execution.client,
            submitted.instrument_id,
            history_start,
            |reports| {
                reports.iter().any(|report| {
                    report.venue_order_id == venue_order_id
                        && report.order_type == OrderType::StopMarket
                        && report.order_status == OrderStatus::Canceled
                        && report
                            .trigger_price
                            .is_some_and(|price| price.as_decimal() == stop_price)
                })
            },
        )
        .await?;
        ensure!(!reports.is_empty());

        let mass_execution = sandbox_execution_client(&env, &account_id)
            .await
            .context("connect execution client for futures stop mass-status recovery")?;
        wait_for_mass_order_report(&mass_execution.client, |reports| {
            reports.iter().any(|report| {
                report.venue_order_id == venue_order_id
                    && report.order_type == OrderType::StopMarket
                    && report.order_status == OrderStatus::Canceled
                    && report
                        .trigger_price
                        .is_some_and(|price| price.as_decimal() == stop_price)
            })
        })
        .await?;

        Ok(())
    })
    .catch_unwind()
    .await;
    let cleanup_result = cleanup.cleanup().await;
    finish_with_cleanup(body, cleanup_result).await
}

#[cfg(feature = "sandbox-futures-tests")]
fn ensure_eq_decimal_price(
    actual: Option<&Price>,
    expected: Decimal,
    description: &str,
) -> Result<()> {
    ensure!(
        actual.is_some_and(|price| price.as_decimal() == expected),
        "{description} mismatch: expected={expected}"
    );
    Ok(())
}

#[cfg(feature = "sandbox-tests")]
#[test]
fn sandbox_redaction_diagnostics_do_not_leak_token_or_metadata() -> Result<()> {
    let token = "secret-sandbox-token";
    let interceptor = TbankAuthInterceptor::new(token)?;
    let debug = format!("{interceptor:?}");
    ensure!(!debug.contains(token), "interceptor Debug leaked token");
    ensure!(
        !debug.contains("Bearer"),
        "interceptor Debug leaked bearer prefix"
    );
    ensure!(
        !debug.contains("Authorization"),
        "interceptor Debug leaked authorization metadata"
    );

    let env = SandboxEnv {
        token: token.to_string(),
        account_id: Some("sandbox-account".to_string()),
        pay_in_rub: None,
        instrument: InstrumentSpec::from_env()?,
    };
    let status = Status::unauthenticated("Bearer secret-sandbox-token Authorization metadata");
    let message = sanitize_status(&status, "UnitTest.Request", &env);
    ensure!(!message.contains(token), "sanitized status leaked token");
    ensure!(
        !message.contains("Bearer"),
        "sanitized status leaked bearer prefix"
    );
    ensure!(
        !message.contains("Authorization"),
        "sanitized status leaked authorization metadata"
    );
    ensure!(message.contains("token_present=true"));
    ensure!(message.contains("account_id_present=true"));
    ensure!(message.contains("endpoint_host=sandbox-invest-public-api.tbank.ru"));
    Ok(())
}
