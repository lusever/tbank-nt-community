use std::collections::HashSet;

use crate::grpc::generated::{
    CandleInstrument, LastPriceInstrument, MarketDataRequest, OrderBookInstrument, OrderBookType,
    SubscribeCandlesRequest, SubscribeLastPriceRequest, SubscribeOrderBookRequest,
    SubscribeTradesRequest, SubscriptionAction, SubscriptionInterval, TradeInstrument,
    TradeSourceType, get_candles_request, market_data_request,
};
use nautilus_model::identifiers::InstrumentId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Identity of an order-book subscription.
pub struct BookSubscription {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Requested order-book depth.
    pub depth: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StableBookSubscription {
    instrument_id: InstrumentId,
    depth: i32,
}

#[derive(Debug, Clone, Default)]
/// Registry of desired market-data subscriptions used across reconnects.
pub struct TbankSubscriptionRegistry {
    // UID-only entries are retained for the public low-level helpers below. Nautilus DataClient
    // commands use the stable InstrumentId-backed entries, whose broker route is resolved only
    // when a concrete restore request is built.
    bars_1m: HashSet<String>,
    trades: HashSet<String>,
    quotes: HashSet<String>,
    books: HashSet<BookSubscription>,
    stable_bars_1m: HashSet<InstrumentId>,
    stable_trades: HashSet<InstrumentId>,
    stable_books: HashSet<StableBookSubscription>,
}

impl TbankSubscriptionRegistry {
    /// Registers a one-minute bar subscription owned by a stable Nautilus instrument identity.
    pub fn subscribe_bars_1m_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        stream_id: impl Into<String>,
    ) -> MarketDataRequest {
        self.stable_bars_1m.insert(instrument_id);
        bars_request(SubscriptionAction::Subscribe, stream_id.into())
    }

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

    /// Removes a one-minute bar subscription by stable Nautilus instrument identity.
    pub fn unsubscribe_bars_1m_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        stream_id: impl Into<String>,
    ) -> MarketDataRequest {
        self.stable_bars_1m.remove(&instrument_id);
        bars_request(SubscriptionAction::Unsubscribe, stream_id.into())
    }

    /// Registers a trade subscription owned by a stable Nautilus instrument identity.
    pub fn subscribe_trades_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        stream_id: impl Into<String>,
    ) -> MarketDataRequest {
        self.stable_trades.insert(instrument_id);
        trades_request(SubscriptionAction::Subscribe, stream_id.into())
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

    /// Removes a trade subscription by stable Nautilus instrument identity.
    pub fn unsubscribe_trades_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        stream_id: impl Into<String>,
    ) -> MarketDataRequest {
        self.stable_trades.remove(&instrument_id);
        trades_request(SubscriptionAction::Unsubscribe, stream_id.into())
    }

    /// Replaces order-book depths for a stable Nautilus instrument identity.
    pub fn replace_order_books_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        depths: impl IntoIterator<Item = i32>,
    ) {
        self.stable_books
            .retain(|subscription| subscription.instrument_id != instrument_id);
        self.stable_books
            .extend(depths.into_iter().map(|depth| StableBookSubscription {
                instrument_id,
                depth,
            }));
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

    /// Replaces all active order-book depths for an instrument.
    pub fn replace_order_books(
        &mut self,
        instrument_uid: impl Into<String>,
        depths: impl IntoIterator<Item = i32>,
    ) {
        let instrument_uid = instrument_uid.into();
        self.books
            .retain(|subscription| subscription.instrument_uid != instrument_uid);
        self.books
            .extend(depths.into_iter().map(|depth| BookSubscription {
                instrument_uid: instrument_uid.clone(),
                depth,
            }));
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

    /// Removes all non-quote order-book depths by stable Nautilus instrument identity.
    pub fn unsubscribe_depth_books_for_instrument(
        &mut self,
        instrument_id: InstrumentId,
        stream_id: impl Into<String>,
    ) -> MarketDataRequest {
        let depth = self
            .stable_books
            .iter()
            .filter(|subscription| {
                subscription.instrument_id == instrument_id && subscription.depth != 1
            })
            .map(|subscription| subscription.depth)
            .max()
            .unwrap_or(10);
        self.stable_books.retain(|subscription| {
            subscription.instrument_id != instrument_id || subscription.depth == 1
        });
        book_request(SubscriptionAction::Unsubscribe, stream_id.into(), depth)
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

    /// Builds restore requests for stable Nautilus subscriptions using the current broker route.
    pub fn restore_requests_with_stream_ids(
        &self,
        resolve_stream_id: impl Fn(InstrumentId) -> String,
    ) -> Vec<MarketDataRequest> {
        let mut requests = self.restore_requests();
        requests.extend(self.stable_bars_1m.iter().map(|instrument_id| {
            bars_request(
                SubscriptionAction::Subscribe,
                resolve_stream_id(*instrument_id),
            )
        }));
        requests.extend(self.stable_trades.iter().map(|instrument_id| {
            trades_request(
                SubscriptionAction::Subscribe,
                resolve_stream_id(*instrument_id),
            )
        }));
        requests.extend(self.stable_books.iter().map(|subscription| {
            book_request(
                SubscriptionAction::Subscribe,
                resolve_stream_id(subscription.instrument_id),
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
    fn replacing_order_books_removes_stale_depths_and_keeps_desired_union() {
        let mut registry = TbankSubscriptionRegistry::default();
        registry.subscribe_order_book("sber", 5);
        registry.replace_order_books("sber", [1, 10]);

        let mut depths = registry
            .restore_requests()
            .into_iter()
            .filter_map(|request| match request.payload {
                Some(market_data_request::Payload::SubscribeOrderBookRequest(request)) => {
                    Some(request.instruments[0].depth)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        depths.sort_unstable();

        assert_eq!(depths, vec![1, 10]);

        registry.replace_order_books("sber", [1]);

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
