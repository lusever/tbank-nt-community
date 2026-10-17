//! Read-only acceptance reconciliation. Credentials/account stay in the process environment.
use nautilus_core::string::secret::SecretString;
use std::{sync::Arc, time::Duration};
use tbank_nt_community::{
    config::TbankEnvironment,
    grpc::{TbankAuthInterceptor, TbankGrpcClients, connect_channel, generated::*, with_timeout},
};
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let environment = TbankEnvironment::Sandbox;
    let secret = SecretString::from(
        std::env::var(environment.token_env())
            .map_err(|_| anyhow::anyhow!("sandbox token missing"))?,
    );
    let account = std::env::var("TBANK_SANDBOX_ACCOUNT_ID")
        .map_err(|_| anyhow::anyhow!("sandbox account configuration missing"))?;
    let timeout = Duration::from_secs(15);
    let channel = connect_channel(environment.default_endpoint(), timeout).await?;
    let mut clients = TbankGrpcClients::new(channel, TbankAuthInterceptor::new(Arc::new(secret))?);
    let positions = clients
        .sandbox
        .get_sandbox_positions(with_timeout(
            PositionsRequest {
                account_id: account.clone(),
            },
            timeout,
        ))
        .await
        .map_err(|s| anyhow::anyhow!("GetSandboxPositions: {:?}", s.code()))?
        .into_inner();
    let nonzero = positions
        .securities
        .iter()
        .filter(|p| p.balance != 0 || p.blocked != 0)
        .collect::<Vec<_>>();
    println!(
        "positions nonzero_securities={} nonzero_futures={}",
        nonzero.len(),
        positions
            .futures
            .iter()
            .filter(|p| p.balance != 0 || p.blocked != 0)
            .count()
    );
    for p in nonzero {
        println!(
            "security instrument_uid={} balance={} blocked={}",
            p.instrument_uid, p.balance, p.blocked
        );
    }
    let from = chrono::Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();
    let orders = clients
        .sandbox
        .get_sandbox_orders(with_timeout(
            GetOrdersRequest {
                account_id: account.clone(),
                advanced_filters: Some(get_orders_request::GetOrdersRequestFilters {
                    from: Some(prost_types::Timestamp {
                        seconds: from,
                        nanos: 0,
                    }),
                    to: None,
                    execution_status: vec![
                        OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
                        OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
                        OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
                        OrderExecutionReportStatus::ExecutionReportStatusCancelled as i32,
                        OrderExecutionReportStatus::ExecutionReportStatusRejected as i32,
                    ],
                }),
            },
            timeout,
        ))
        .await
        .map_err(|s| anyhow::anyhow!("GetSandboxOrders: {:?}", s.code()))?
        .into_inner();
    let active = orders
        .orders
        .iter()
        .filter(|o| {
            matches!(
                OrderExecutionReportStatus::try_from(o.execution_report_status),
                Ok(OrderExecutionReportStatus::ExecutionReportStatusNew
                    | OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill)
            )
        })
        .count();
    println!(
        "orders active={active} terminal_in_today_snapshot={}",
        orders.orders.len() - active
    );
    let args = std::env::args().collect::<Vec<_>>();
    if let Some(path) = args.get(1) {
        for id in std::fs::read_to_string(path)?
            .lines()
            .filter(|s| !s.is_empty())
        {
            let state = clients
                .sandbox
                .get_sandbox_order_state(with_timeout(
                    GetOrderStateRequest {
                        account_id: account.clone(),
                        order_id: id.to_string(),
                        price_type: PriceType::Currency as i32,
                        order_id_type: Some(OrderIdType::Exchange as i32),
                    },
                    timeout,
                ))
                .await;
            match state {
                Ok(r) => {
                    let s = r.into_inner();
                    println!(
                        "order id={id} status={:?} requested={} executed={}",
                        OrderExecutionReportStatus::try_from(s.execution_report_status),
                        s.lots_requested,
                        s.lots_executed
                    );
                }
                Err(s) => println!("order id={id} query_error={:?}", s.code()),
            }
        }
    }
    if let Some(operation_id) = args.get(2) {
        for kind in [OrderIdType::Exchange, OrderIdType::Request] {
            let result = clients
                .sandbox
                .get_sandbox_order_state(with_timeout(
                    GetOrderStateRequest {
                        account_id: account.clone(),
                        order_id: operation_id.clone(),
                        price_type: PriceType::Currency as i32,
                        order_id_type: Some(kind as i32),
                    },
                    timeout,
                ))
                .await;
            match result {
                Ok(r) => println!(
                    "operation_lookup type={kind:?} order_id={}",
                    r.into_inner().order_id
                ),
                Err(s) => println!("operation_lookup type={kind:?} error={:?}", s.code()),
            }
        }
    }
    Ok(())
}
