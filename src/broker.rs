//! Venue operations used by broker tools in addition to Nautilus' generic client traits.
//! Scheduling, persistence and application ownership deliberately stay outside the adapter.
use crate::{
    TbankBookSource,
    config::TbankEnvironment,
    grpc::{TbankAuthInterceptor, TbankGrpcClients, TbankSession, generated as g, with_timeout},
};
use nautilus_core::string::secret::SecretString;
use std::{sync::Arc, time::Duration};

/// A sanitized broker failure, preserving the authoritative broker status code.
#[derive(Debug, thiserror::Error)]
#[error("{operation}: {code:?}: {message}")]
pub struct TbankBrokerError {
    operation: &'static str,
    code: tonic::Code,
    message: String,
}
impl TbankBrokerError {
    /// Venue operation that failed.
    pub fn operation(&self) -> &str {
        self.operation
    }

    /// Broker/transport status code.
    pub fn code(&self) -> tonic::Code {
        self.code
    }
    /// Failure detail with the account and credentials removed.
    pub fn message(&self) -> &str {
        &self.message
    }
}
/// Result of a venue operation.
pub type BrokerResult<T> = std::result::Result<T, TbankBrokerError>;

/// Cloneable service handle over an already connected adapter session.
/// Construction has no network side effects; the embedding application owns the session lifetime.
#[derive(Clone)]
pub struct TbankBrokerClient {
    clients: TbankGrpcClients<TbankAuthInterceptor>,
    environment: TbankEnvironment,
    credentials: Arc<SecretString>,
    timeout: Duration,
}
impl TbankBrokerClient {
    /// Builds services without connecting or issuing a request.
    pub fn new(session: &TbankSession, timeout: Duration) -> Self {
        Self {
            clients: session.clients(),
            environment: session.environment(),
            credentials: session.credentials(),
            timeout,
        }
    }
    fn error(
        &self,
        operation: &'static str,
        account: &str,
        status: tonic::Status,
    ) -> TbankBrokerError {
        let mut message = status
            .message()
            .replace(self.credentials.expose_secret(), "[token]");
        if !account.is_empty() {
            message = message.replace(account, "[account]");
        }
        TbankBrokerError {
            operation,
            code: status.code(),
            message,
        }
    }
    /// Returns accounts accessible to the current credentials.
    pub async fn accounts(&self) -> BrokerResult<Vec<g::Account>> {
        let mut c = self.clients.clone();
        let request = with_timeout(g::GetAccountsRequest::default(), self.timeout);
        let result = if self.environment.is_live() {
            c.users.get_accounts(request).await
        } else {
            c.sandbox.get_sandbox_accounts(request).await
        };
        Ok(result
            .map_err(|s| self.error("GetAccounts", "", s))?
            .into_inner()
            .accounts)
    }
    /// Checks configured account access while establishing the application's clients.
    pub async fn validate_account(&self, account: &str) -> BrokerResult<()> {
        if self.accounts().await?.iter().any(|a| a.id == account) {
            return Ok(());
        }
        Err(self.error(
            "GetAccounts",
            account,
            tonic::Status::permission_denied(
                "configured account is not accessible with selected token",
            ),
        ))
    }
    /// Measures broker reachability with an actual read-only RPC.
    pub async fn ping(&self) -> BrokerResult<()> {
        self.clients
            .users
            .clone()
            .get_info(with_timeout(g::GetInfoRequest {}, self.timeout))
            .await
            .map_err(|s| self.error("GetInfo", "", s))?;
        Ok(())
    }
    /// Resolves one share using its exact ticker and class code.
    pub async fn share(&self, ticker: &str, class: &str) -> BrokerResult<g::Share> {
        self.clients
            .instruments
            .clone()
            .share_by(with_timeout(
                g::InstrumentRequest {
                    id_type: g::InstrumentIdType::Ticker as i32,
                    class_code: Some(class.into()),
                    id: ticker.into(),
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("ShareBy", "", s))?
            .into_inner()
            .instrument
            .ok_or_else(|| {
                self.error(
                    "ShareBy",
                    "",
                    tonic::Status::not_found("instrument not found"),
                )
            })
    }
    /// Returns current trading capabilities for an instrument.
    pub async fn trading_status(&self, uid: &str) -> BrokerResult<g::GetTradingStatusResponse> {
        Ok(self
            .clients
            .market_data
            .clone()
            .get_trading_status(with_timeout(
                g::GetTradingStatusRequest {
                    instrument_id: Some(uid.into()),
                    ..Default::default()
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("GetTradingStatus", "", s))?
            .into_inner())
    }
    /// Returns exchange schedules over the requested time range.
    pub async fn schedules(
        &self,
        from: Option<prost_types::Timestamp>,
        to: Option<prost_types::Timestamp>,
    ) -> BrokerResult<g::TradingSchedulesResponse> {
        Ok(self
            .clients
            .instruments
            .clone()
            .trading_schedules(with_timeout(
                g::TradingSchedulesRequest {
                    exchange: None,
                    from,
                    to,
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("TradingSchedules", "", s))?
            .into_inner())
    }
    /// Searches API-tradable shares.
    pub async fn find_shares(&self, query: &str) -> BrokerResult<Vec<g::InstrumentShort>> {
        Ok(self
            .clients
            .instruments
            .clone()
            .find_instrument(with_timeout(
                g::FindInstrumentRequest {
                    query: query.into(),
                    instrument_kind: Some(g::InstrumentType::Share as i32),
                    api_trade_available_flag: Some(true),
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("FindInstrument", "", s))?
            .into_inner()
            .instruments)
    }
    /// Queries an order by broker order ID or the original idempotency/request ID.
    pub async fn order(
        &self,
        account: &str,
        id: &str,
        id_type: g::OrderIdType,
    ) -> BrokerResult<g::OrderState> {
        let request = with_timeout(
            g::GetOrderStateRequest {
                account_id: account.into(),
                order_id: id.into(),
                order_id_type: Some(id_type as i32),
                price_type: g::PriceType::Currency as i32,
            },
            self.timeout,
        );
        let mut c = self.clients.clone();
        let result = if self.environment.is_live() {
            c.orders.get_order_state(request).await
        } else {
            c.sandbox.get_sandbox_order_state(request).await
        };
        Ok(result
            .map_err(|s| self.error("GetOrderState", account, s))?
            .into_inner())
    }
    /// Lists active orders, or all today's statuses for reconciliation/observation.
    pub async fn orders(
        &self,
        account: &str,
        include_terminal: bool,
    ) -> BrokerResult<Vec<g::OrderState>> {
        let request = with_timeout(
            g::GetOrdersRequest {
                account_id: account.into(),
                advanced_filters: include_terminal.then(|| {
                    g::get_orders_request::GetOrdersRequestFilters {
                        execution_status: (1..=5).collect(),
                        ..Default::default()
                    }
                }),
            },
            self.timeout,
        );
        let mut c = self.clients.clone();
        let result = if self.environment.is_live() {
            c.orders.get_orders(request).await
        } else {
            c.sandbox.get_sandbox_orders(request).await
        };
        Ok(result
            .map_err(|s| self.error("GetOrders", account, s))?
            .into_inner()
            .orders)
    }
    /// Sends one cancellation without retries or changes to the supplied order identity.
    pub async fn cancel_order(
        &self,
        account: &str,
        id: &str,
        id_type: g::OrderIdType,
    ) -> BrokerResult<g::CancelOrderResponse> {
        let request = with_timeout(
            g::CancelOrderRequest {
                account_id: account.into(),
                order_id: id.into(),
                order_id_type: Some(id_type as i32),
            },
            self.timeout,
        );
        let mut c = self.clients.clone();
        let result = if self.environment.is_live() {
            c.orders.cancel_order(request).await
        } else {
            c.sandbox.cancel_sandbox_order(request).await
        };
        Ok(result
            .map_err(|s| self.error("CancelOrder", account, s))?
            .into_inner())
    }
    /// Queries purchasing/selling limits at a given price.
    pub async fn max_lots(
        &self,
        account: &str,
        instrument: &str,
        price: g::Quotation,
    ) -> BrokerResult<g::GetMaxLotsResponse> {
        let request = with_timeout(
            g::GetMaxLotsRequest {
                account_id: account.into(),
                instrument_id: instrument.into(),
                price: Some(price),
            },
            self.timeout,
        );
        let mut c = self.clients.clone();
        let result = if self.environment.is_live() {
            c.orders.get_max_lots(request).await
        } else {
            c.sandbox.get_sandbox_max_lots(request).await
        };
        Ok(result
            .map_err(|s| self.error("GetMaxLots", account, s))?
            .into_inner())
    }
    /// Returns a book snapshot.
    pub async fn order_book(
        &self,
        instrument: &str,
        depth: i32,
    ) -> BrokerResult<g::GetOrderBookResponse> {
        Ok(self
            .clients
            .market_data
            .clone()
            .get_order_book(with_timeout(
                g::GetOrderBookRequest {
                    instrument_id: Some(instrument.into()),
                    depth,
                    ..Default::default()
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("GetOrderBook", "", s))?
            .into_inner())
    }
    /// Returns last dealer prices for an instrument.
    pub async fn dealer_prices(&self, instrument: &str) -> BrokerResult<Vec<g::LastPrice>> {
        Ok(self
            .clients
            .market_data
            .clone()
            .get_last_prices(with_timeout(
                g::GetLastPricesRequest {
                    instrument_id: vec![instrument.into()],
                    last_price_type: g::LastPriceType::LastPriceDealer as i32,
                    ..Default::default()
                },
                self.timeout,
            ))
            .await
            .map_err(|s| self.error("GetLastPrices", "", s))?
            .into_inner()
            .last_prices)
    }
    /// Returns current tariff occupancy for stream admission.
    async fn tariff(&self) -> BrokerResult<g::GetUserTariffResponse> {
        Ok(self
            .clients
            .users
            .clone()
            .get_user_tariff(with_timeout(g::GetUserTariffRequest {}, self.timeout))
            .await
            .map_err(|s| self.error("GetUserTariff", "", s))?
            .into_inner())
    }
    /// Checks broker-reported market-data stream occupancy before opening a preview.
    pub async fn check_market_data_capacity(&self) -> BrokerResult<()> {
        let tariff = self.tariff().await?;
        let limits = tariff
            .stream_limits
            .iter()
            .filter(|limit| {
                limit.streams.iter().any(|method| {
                    matches!(
                        method.rsplit(['.', '/']).next(),
                        Some("MarketDataStream" | "MarketDataServerSideStream")
                    )
                })
            })
            .collect::<Vec<_>>();
        if limits.is_empty()
            || !limits
                .iter()
                .all(|limit| limit.limit > 0 && limit.open >= 0 && limit.open < limit.limit)
        {
            return Err(self.error(
                "GetUserTariff",
                "",
                tonic::Status::resource_exhausted("no broker market-data stream capacity"),
            ));
        }
        Ok(())
    }

    async fn stream(
        &self,
        request: g::MarketDataServerSideStreamRequest,
    ) -> BrokerResult<TbankMarketDataStream> {
        let stream = tokio::time::timeout(
            self.timeout,
            self.clients
                .market_data_stream
                .clone()
                .market_data_server_side_stream(request),
        )
        .await
        .map_err(|_| {
            self.error(
                "MarketDataServerSideStream",
                "",
                tonic::Status::deadline_exceeded("stream opening timeout"),
            )
        })?
        .map_err(|s| self.error("MarketDataServerSideStream", "", s))?
        .into_inner();
        Ok(TbankMarketDataStream {
            stream,
            client: self.clone(),
        })
    }
    /// Opens trades and trading-status observations for one instrument.
    pub async fn subscribe_auxiliary(&self, uid: &str) -> BrokerResult<TbankMarketDataStream> {
        self.stream(g::MarketDataServerSideStreamRequest {
            subscribe_trades_request: Some(g::SubscribeTradesRequest {
                subscription_action: g::SubscriptionAction::Subscribe as i32,
                instruments: vec![g::TradeInstrument {
                    instrument_id: uid.into(),
                    ..Default::default()
                }],
                trade_source: g::TradeSourceType::TradeSourceAll as i32,
                with_open_interest: false,
            }),
            subscribe_info_request: Some(g::SubscribeInfoRequest {
                subscription_action: g::SubscriptionAction::Subscribe as i32,
                instruments: vec![g::InfoInstrument {
                    instrument_id: uid.into(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })
        .await
    }
    /// Opens a venue order book observation stream.
    pub async fn subscribe_book(
        &self,
        instrument: &str,
        depth: i32,
        source: TbankBookSource,
    ) -> BrokerResult<TbankMarketDataStream> {
        let kind = match source {
            TbankBookSource::Exchange => g::OrderBookType::OrderbookTypeExchange,
            TbankBookSource::Dealer => g::OrderBookType::OrderbookTypeDealer,
        };
        self.stream(g::MarketDataServerSideStreamRequest {
            subscribe_order_book_request: Some(g::SubscribeOrderBookRequest {
                subscription_action: g::SubscriptionAction::Subscribe as i32,
                instruments: vec![g::OrderBookInstrument {
                    instrument_id: instrument.into(),
                    depth,
                    order_book_type: kind as i32,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })
        .await
    }
}
/// A broker observation stream with transport errors sanitized by the adapter.
pub struct TbankMarketDataStream {
    stream: tonic::Streaming<g::MarketDataResponse>,
    client: TbankBrokerClient,
}
impl TbankMarketDataStream {
    /// Receives the next broker observation; stream lifetime is owned by the caller.
    pub async fn message(&mut self) -> BrokerResult<Option<g::MarketDataResponse>> {
        self.stream
            .message()
            .await
            .map_err(|s| self.client.error("MarketDataServerSideStream", "", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn broker_failures_keep_status_without_disclosing_credentials_or_account() {
        let credentials = Arc::new(SecretString::from("synthetic-secret"));
        let client = TbankBrokerClient {
            clients: TbankGrpcClients::new(
                tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy(),
                TbankAuthInterceptor::new(Arc::clone(&credentials)).unwrap(),
            ),
            environment: TbankEnvironment::Sandbox,
            credentials,
            timeout: Duration::from_secs(1),
        };
        let error = client.error(
            "CancelOrder",
            "synthetic-account",
            tonic::Status::unavailable("synthetic-secret synthetic-account transport failed"),
        );
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(error.message(), "[token] [account] transport failed");
        assert!(!format!("{error:?}").contains("synthetic-secret"));
        assert!(!format!("{error:?}").contains("synthetic-account"));
    }
}
