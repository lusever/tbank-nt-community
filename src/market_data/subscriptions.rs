use std::collections::HashSet;

use crate::grpc::generated::{
    CandleInstrument, LastPriceInstrument, MarketDataRequest, OrderBookInstrument, OrderBookType,
    SubscribeCandlesRequest, SubscribeLastPriceRequest, SubscribeOrderBookRequest,
    SubscribeTradesRequest, SubscriptionAction, SubscriptionInterval, TradeInstrument,
    TradeSourceType, get_candles_request, market_data_request,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Identity of an order-book subscription.
pub struct BookSubscription {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Requested order-book depth.
    pub depth: i32,
}

#[derive(Debug, Clone, Default)]
/// Registry of desired market-data subscriptions used across reconnects.
pub struct TbankSubscriptionRegistry {
    bars_1m: HashSet<String>,
    trades: HashSet<String>,
    quotes: HashSet<String>,
    books: HashSet<BookSubscription>,
}

impl TbankSubscriptionRegistry {
    /// Registers a one-minute bar subscription.
    pub fn subscribe_bars_1m(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        let instrument_uid = instrument_uid.into();
        self.bars_1m.insert(instrument_uid.clone());
        bars_request(SubscriptionAction::Subscribe, instrument_uid)
    }

    /// Registers one-minute bar subscriptions for multiple instruments.
    pub fn subscribe_bars_1m_many(
        &mut self,
        instrument_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> MarketDataRequest {
        let instrument_uids = instrument_uids
            .into_iter()
            .map(Into::into)
            .inspect(|instrument_uid| {
                self.bars_1m.insert(instrument_uid.clone());
            })
            .collect::<Vec<_>>();
        bars_request_many(SubscriptionAction::Subscribe, instrument_uids)
    }

    /// Removes a one-minute bar subscription.
    pub fn unsubscribe_bars_1m(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.bars_1m.remove(instrument_uid);
        bars_request(SubscriptionAction::Unsubscribe, instrument_uid.to_string())
    }

    /// Registers a trade subscription.
    pub fn subscribe_trades(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        let instrument_uid = instrument_uid.into();
        self.trades.insert(instrument_uid.clone());
        trades_request(SubscriptionAction::Subscribe, instrument_uid)
    }

    /// Removes a trade subscription.
    pub fn unsubscribe_trades(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.trades.remove(instrument_uid);
        trades_request(SubscriptionAction::Unsubscribe, instrument_uid.to_string())
    }

    /// Registers a quote subscription.
    pub fn subscribe_quotes(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        let instrument_uid = instrument_uid.into();
        self.quotes.insert(instrument_uid.clone());
        quotes_request(SubscriptionAction::Subscribe, instrument_uid)
    }

    /// Removes a quote subscription.
    pub fn unsubscribe_quotes(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.quotes.remove(instrument_uid);
        quotes_request(SubscriptionAction::Unsubscribe, instrument_uid.to_string())
    }

    /// Registers an order-book subscription.
    pub fn subscribe_order_book(
        &mut self,
        instrument_uid: impl Into<String>,
        depth: i32,
    ) -> MarketDataRequest {
        let instrument_uid = instrument_uid.into();
        self.books.insert(BookSubscription {
            instrument_uid: instrument_uid.clone(),
            depth,
        });
        book_request(SubscriptionAction::Subscribe, instrument_uid, depth)
    }

    /// Removes an order-book subscription.
    pub fn unsubscribe_order_book(
        &mut self,
        instrument_uid: &str,
        depth: i32,
    ) -> MarketDataRequest {
        self.books.remove(&BookSubscription {
            instrument_uid: instrument_uid.to_string(),
            depth,
        });
        book_request(
            SubscriptionAction::Unsubscribe,
            instrument_uid.to_string(),
            depth,
        )
    }

    /// Removes all depth-book subscriptions for an instrument.
    pub fn unsubscribe_depth_books(&mut self, instrument_uid: &str) -> MarketDataRequest {
        let depth = self
            .books
            .iter()
            .filter(|subscription| {
                subscription.instrument_uid == instrument_uid && subscription.depth != 1
            })
            .map(|subscription| subscription.depth)
            .max()
            .unwrap_or(10);
        self.books.retain(|subscription| {
            subscription.instrument_uid != instrument_uid || subscription.depth == 1
        });
        book_request(
            SubscriptionAction::Unsubscribe,
            instrument_uid.to_string(),
            depth,
        )
    }

    /// Builds requests that restore all registered subscriptions.
    pub fn restore_requests(&self) -> Vec<MarketDataRequest> {
        let mut requests = Vec::new();
        requests.extend(
            self.bars_1m
                .iter()
                .cloned()
                .map(|uid| bars_request(SubscriptionAction::Subscribe, uid)),
        );
        requests.extend(
            self.trades
                .iter()
                .cloned()
                .map(|uid| trades_request(SubscriptionAction::Subscribe, uid)),
        );
        requests.extend(
            self.quotes
                .iter()
                .cloned()
                .map(|uid| quotes_request(SubscriptionAction::Subscribe, uid)),
        );
        requests.extend(self.books.iter().map(|subscription| {
            book_request(
                SubscriptionAction::Subscribe,
                subscription.instrument_uid.clone(),
                subscription.depth,
            )
        }));
        requests
    }
}

fn bars_request(action: SubscriptionAction, instrument_uid: String) -> MarketDataRequest {
    bars_request_many(action, vec![instrument_uid])
}

pub(crate) fn bars_request_many(
    action: SubscriptionAction,
    instrument_uids: Vec<String>,
) -> MarketDataRequest {
    MarketDataRequest {
        payload: Some(market_data_request::Payload::SubscribeCandlesRequest(
            SubscribeCandlesRequest {
                subscription_action: action as i32,
                instruments: instrument_uids
                    .into_iter()
                    .map(|instrument_uid| CandleInstrument {
                        instrument_id: instrument_uid,
                        interval: SubscriptionInterval::OneMinute as i32,
                        ..CandleInstrument::default()
                    })
                    .collect(),
                waiting_close: true,
                candle_source_type: Some(get_candles_request::CandleSource::Exchange as i32),
            },
        )),
    }
}

fn trades_request(action: SubscriptionAction, instrument_uid: String) -> MarketDataRequest {
    MarketDataRequest {
        payload: Some(market_data_request::Payload::SubscribeTradesRequest(
            SubscribeTradesRequest {
                subscription_action: action as i32,
                instruments: vec![TradeInstrument {
                    instrument_id: instrument_uid,
                    ..TradeInstrument::default()
                }],
                trade_source: TradeSourceType::TradeSourceExchange as i32,
                with_open_interest: false,
            },
        )),
    }
}

fn quotes_request(action: SubscriptionAction, instrument_uid: String) -> MarketDataRequest {
    MarketDataRequest {
        payload: Some(market_data_request::Payload::SubscribeLastPriceRequest(
            SubscribeLastPriceRequest {
                subscription_action: action as i32,
                instruments: vec![LastPriceInstrument {
                    instrument_id: instrument_uid,
                    ..LastPriceInstrument::default()
                }],
            },
        )),
    }
}

fn book_request(
    action: SubscriptionAction,
    instrument_uid: String,
    depth: i32,
) -> MarketDataRequest {
    MarketDataRequest {
        payload: Some(market_data_request::Payload::SubscribeOrderBookRequest(
            SubscribeOrderBookRequest {
                subscription_action: action as i32,
                instruments: vec![OrderBookInstrument {
                    instrument_id: instrument_uid,
                    depth,
                    order_book_type: OrderBookType::OrderbookTypeExchange as i32,
                    ..OrderBookInstrument::default()
                }],
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_restores_active_subscriptions() {
        let mut registry = TbankSubscriptionRegistry::default();
        registry.subscribe_bars_1m("sber");
        registry.subscribe_trades("sber");
        registry.subscribe_order_book("sber", 10);

        assert_eq!(registry.restore_requests().len(), 3);
    }

    #[test]
    fn order_book_subscriptions_keep_depths_separate() {
        let mut registry = TbankSubscriptionRegistry::default();
        registry.subscribe_order_book("sber", 1);
        registry.subscribe_order_book("sber", 10);

        assert_eq!(registry.restore_requests().len(), 2);

        registry.unsubscribe_order_book("sber", 1);

        assert_eq!(registry.restore_requests().len(), 1);
    }

    #[test]
    fn candle_subscription_can_batch_multiple_instruments() {
        let mut registry = TbankSubscriptionRegistry::default();
        let request = registry.subscribe_bars_1m_many(["sber", "lkoh"]);

        let Some(market_data_request::Payload::SubscribeCandlesRequest(request)) = request.payload
        else {
            panic!("expected candle subscription request");
        };

        assert_eq!(request.instruments.len(), 2);
        assert_eq!(request.instruments[0].instrument_id, "sber");
        assert_eq!(request.instruments[1].instrument_id, "lkoh");
        assert_eq!(
            request.candle_source_type,
            Some(get_candles_request::CandleSource::Exchange as i32)
        );
        assert_eq!(registry.restore_requests().len(), 2);
    }

    #[test]
    fn trade_and_order_book_subscriptions_request_exchange_data() {
        let mut registry = TbankSubscriptionRegistry::default();

        let trade_request = registry.subscribe_trades("sber");
        let Some(market_data_request::Payload::SubscribeTradesRequest(trades)) =
            trade_request.payload
        else {
            panic!("expected trade subscription request");
        };
        assert_eq!(
            trades.trade_source,
            TradeSourceType::TradeSourceExchange as i32
        );

        let book_request = registry.subscribe_order_book("sber", 10);
        let Some(market_data_request::Payload::SubscribeOrderBookRequest(book)) =
            book_request.payload
        else {
            panic!("expected order-book subscription request");
        };
        assert_eq!(
            book.instruments[0].order_book_type,
            OrderBookType::OrderbookTypeExchange as i32
        );
    }
}
