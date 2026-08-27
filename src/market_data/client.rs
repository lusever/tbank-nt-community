use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    common::venue::TbankVenue,
    common::{
        Result, TbankAdapterError,
        consts::TBANK_CLIENT_ID,
        ids::{TbankInstrumentIdParts, instrument_id_from_ticker_class_for_venue},
        time::unix_nanos_to_timestamp,
    },
    config::{TbankDataClientConfig, TbankIndicativeInstrumentConfig},
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients, connect_channel,
        generated::{
            Candle, CandleInstrument, CandleInterval, GetCandlesRequest, GetLastPricesRequest,
            GetLastPricesResponse, GetOrderBookRequest, GetOrderBookResponse, LastPriceType,
            MarketDataRequest, MarketDataResponse, MarketDataServerSideStreamRequest, OrderBook,
            OrderBookInstrument, OrderBookType, SubscribeCandlesRequest, SubscribeOrderBookRequest,
            SubscribeTradesRequest, SubscriptionAction, SubscriptionInterval, SubscriptionStatus,
            Trade, TradeInstrument, TradeSourceType, get_candles_request, market_data_request,
            market_data_response,
        },
        with_timeout,
    },
    instruments::{TbankInstrumentMetadata, TbankInstrumentProvider},
    market_data::{
        MarketDataInstrumentMetadata, TbankBar, TbankCandleReadinessState, TbankMarketDataEvent,
        TbankMarketDataStreamState, TbankOrderBookSnapshot, TbankQuoteTick,
        TbankSubscriptionRegistry,
        candles::{ONE_MINUTE_NANOS, one_minute_candle_query_chunks},
        continuity::{BarContinuityDecision, BarContinuityTracker},
        converters::{candle_to_bar, last_price_to_quote, orderbook_to_snapshot, trade_to_tick},
        events::publish_market_data_event,
        supervisor::{
            BackfillCoordinator, HistoricalRequestLimiter, MarketDataClient, RecoveryPublication,
            RecoveryRangeResult, retryable_stream_status,
        },
    },
};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::future::join_all;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    live::{
        runner::{get_data_event_sender, try_get_data_event_sender},
        runtime::get_runtime,
    },
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, RequestBars, RequestTrades, SubscribeBars,
            SubscribeBookDepth10, SubscribeQuotes, SubscribeTrades, TradesResponse,
            UnsubscribeBars, UnsubscribeBookDepth10, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
    msgbus::{self, TypedHandler, switchboard},
    providers::InstrumentProvider,
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Bar, BarType, BookOrder, DEPTH10_LEN, Data, OrderBookDepth10, QuoteTick, TradeTick},
    enums::{AggregationSource, AggressorSide, BarAggregation, BookType, OrderSide},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::InstrumentAny,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use tokio::task::{JoinHandle, JoinSet};

type MarketDataStreamClient =
    crate::grpc::generated::market_data_stream_service_client::MarketDataStreamServiceClient<
        tonic::codegen::InterceptedService<tonic::transport::Channel, TbankAuthInterceptor>,
    >;

const MAX_QUOTES_PER_STREAM: usize = 300;
const MAX_PRE_ACK_MESSAGES: usize = 2_048;
type SharedInstrumentMetadata = Arc<RwLock<HashMap<String, MarketDataInstrumentMetadata>>>;
type SharedInstrumentStreamIds = Arc<RwLock<HashMap<String, String>>>;
/// The continuity cursor belongs to the stable Nautilus subscription, not to a broker route.
/// Broker instrument UIDs can change after catalogue refresh while `BarType` retains the
/// InstrumentId that owns the subscription.
type SharedBarWatermarks = Arc<std::sync::Mutex<HashMap<BarType, i128>>>;

fn snapshot_bar_watermarks(watermarks: &SharedBarWatermarks) -> HashMap<BarType, i128> {
    watermarks
        .lock()
        .expect("market-data watermark lock")
        .clone()
}

fn record_bar_watermark(
    watermarks: &SharedBarWatermarks,
    bar_type: BarType,
    ts_event: i128,
) -> bool {
    let mut watermarks = watermarks.lock().expect("market-data watermark lock");
    let had_baseline = watermarks.contains_key(&bar_type);
    if watermarks
        .get(&bar_type)
        .is_none_or(|latest| ts_event > *latest)
    {
        watermarks.insert(bar_type, ts_event);
    }
    !had_baseline
}

fn has_bar_watermark(watermarks: &SharedBarWatermarks, bar_type: BarType) -> bool {
    watermarks
        .lock()
        .expect("market-data watermark lock")
        .contains_key(&bar_type)
}

fn commit_live_bar(
    bar_watermarks: &SharedBarWatermarks,
    continuity: &mut HashMap<String, BarContinuityTracker>,
    instrument_uid: &str,
    bar_type: BarType,
    ts_event: i128,
) {
    continuity
        .entry(instrument_uid.to_string())
        .or_default()
        .record_live_bar(ts_event);
    record_bar_watermark(bar_watermarks, bar_type, ts_event);
}

#[derive(Debug, Default)]
struct MarketDataStreamHealthState {
    /// Every task/readiness key owned by the current subscription snapshot.
    current_task_keys: HashSet<String>,
    /// Keys whose lifecycle contributes to client operational health.
    expected_groups: HashSet<String>,
    non_operational_groups: HashSet<String>,
}

#[derive(Debug, Default)]
struct MarketDataStreamHealth {
    state: std::sync::Mutex<MarketDataStreamHealthState>,
}

impl MarketDataStreamHealth {
    fn register(&self, task_key: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        Self::register_task_key(&mut state, task_key);
    }

    fn register_task_key(state: &mut MarketDataStreamHealthState, task_key: &str) {
        state.current_task_keys.insert(task_key.to_string());
        state.expected_groups.insert(task_key.to_string());
        state.non_operational_groups.insert(task_key.to_string());
    }

    fn register_current(&self, task_key: &str) {
        self.state
            .lock()
            .expect("market-data lifecycle lock")
            .current_task_keys
            .insert(task_key.to_string());
    }

    fn mark_reconnecting(&self, task_key: &str) {
        self.mark_non_operational(task_key);
    }

    fn mark_terminal(&self, task_key: &str) {
        self.mark_non_operational(task_key);
    }

    fn mark_non_operational(&self, task_key: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        if !state.expected_groups.contains(task_key) {
            return;
        }
        state.non_operational_groups.insert(task_key.to_string());
    }

    fn mark_operational(&self, task_key: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        if !state.expected_groups.contains(task_key) {
            return;
        }
        state.non_operational_groups.remove(task_key);
    }

    fn retire_task_key(&self, task_key: &str, reason: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        Self::retire_task_key_locked(&mut state, task_key, None, reason);
    }

    fn retire_task_key_locked(
        state: &mut MarketDataStreamHealthState,
        task_key: &str,
        stage: Option<&str>,
        reason: &str,
    ) -> bool {
        if !state.expected_groups.contains(task_key) {
            return false;
        }

        let readiness_ids = Self::readiness_ids_for_task_key_locked(state, task_key);
        if let Some(stage) = stage {
            trace_market_data_stream_event(
                MarketDataStreamEventInput {
                    stage,
                    task_key,
                    stream_kind: stream_kind_from_task_key(task_key),
                    instrument_count: 0,
                    status: None,
                    reason: reason.to_string(),
                    delay_ms: None,
                    attempt: 0,
                },
                readiness_ids.clone(),
            );
        }
        trace_market_data_stream_event(
            MarketDataStreamEventInput {
                stage: "stream_snapshot_replaced",
                task_key,
                stream_kind: stream_kind_from_task_key(task_key),
                instrument_count: 0,
                status: None,
                reason: reason.to_string(),
                delay_ms: None,
                attempt: 0,
            },
            readiness_ids,
        );
        state.current_task_keys.remove(task_key);
        state.expected_groups.remove(task_key);
        state.non_operational_groups.remove(task_key);
        true
    }

    fn retire_prefix(&self, prefix: &str, stage: Option<&str>, reason: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        let task_keys = state
            .expected_groups
            .iter()
            .filter(|task_key| task_key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for task_key in task_keys {
            Self::retire_task_key_locked(&mut state, &task_key, stage, reason);
        }
        state
            .current_task_keys
            .retain(|task_key| !task_key.starts_with(prefix));
    }

    fn replace_expected<'a>(&self, prefix: &str, task_keys: impl IntoIterator<Item = &'a str>) {
        let task_keys = task_keys
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        let old_task_keys = state
            .expected_groups
            .iter()
            .filter(|task_key| task_key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for old_task_key in old_task_keys {
            Self::retire_task_key_locked(
                &mut state,
                &old_task_key,
                None,
                "stream subscription snapshot was replaced",
            );
        }
        state
            .current_task_keys
            .retain(|task_key| !task_key.starts_with(prefix));
        for task_key in task_keys {
            Self::register_task_key(&mut state, &task_key);
        }
    }

    #[cfg(test)]
    fn advance_bar_generation(&self, generation: u64) {
        self.advance_bar_generation_with_stage(
            generation,
            None,
            "bar stream snapshot was replaced",
        );
    }

    fn advance_bar_generation_with_stage(
        &self,
        generation: u64,
        stage: Option<&str>,
        reason: &str,
    ) {
        let reason = format!("{reason} (bar generation {generation})");
        self.retire_prefix("bars:", stage, &reason);
    }

    fn retire_all(&self, stage: &str, reason: &str) {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        let task_keys = state.expected_groups.iter().cloned().collect::<Vec<_>>();
        for task_key in task_keys {
            Self::retire_task_key_locked(&mut state, &task_key, Some(stage), reason);
        }
        state.current_task_keys.clear();
        state.expected_groups.clear();
        state.non_operational_groups.clear();
    }

    /// Registers and starts an isolated child while holding the same lifecycle lock that checks
    /// the parent lease. Snapshot replacement therefore cannot remove the parent between the
    /// ownership check, child registration, and `spawn`.
    fn spawn_child_if_current<F>(
        &self,
        parent_task_key: &str,
        child_task_key: &str,
        spawn: F,
    ) -> Option<JoinHandle<()>>
    where
        F: FnOnce() -> JoinHandle<()>,
    {
        let mut state = self.state.lock().expect("market-data lifecycle lock");
        if !task_key_is_current(&state, parent_task_key) {
            return None;
        }
        Self::register_task_key(&mut state, child_task_key);
        if !task_key_is_current(&state, parent_task_key) {
            state.current_task_keys.remove(child_task_key);
            state.expected_groups.remove(child_task_key);
            state.non_operational_groups.remove(child_task_key);
            return None;
        }
        Some(spawn())
    }

    #[cfg(test)]
    fn expected_task_keys(&self) -> Vec<String> {
        let state = self.state.lock().expect("market-data lifecycle lock");
        let mut task_keys = state.expected_groups.iter().cloned().collect::<Vec<_>>();
        task_keys.sort();
        task_keys
    }

    fn is_current_task_key(&self, task_key: &str) -> bool {
        let state = self.state.lock().expect("market-data lifecycle lock");
        task_key_is_current(&state, task_key)
    }

    fn with_current_task_key(&self, task_key: &str, operation: impl FnOnce()) -> bool {
        let state = self.state.lock().expect("market-data lifecycle lock");
        if task_key_is_current(&state, task_key) {
            operation();
            true
        } else {
            false
        }
    }

    fn with_current_task_key_and_readiness(
        &self,
        task_key: &str,
        operation: impl FnOnce(Vec<String>),
    ) -> bool {
        let state = self.state.lock().expect("market-data lifecycle lock");
        if !task_key_is_current(&state, task_key) {
            return false;
        }
        operation(Self::readiness_ids_for_task_key_locked(&state, task_key));
        true
    }

    fn readiness_ids_for_task_key_locked(
        state: &MarketDataStreamHealthState,
        task_key: &str,
    ) -> Vec<String> {
        let prefix = format!("{task_key}:instrument:");
        let mut readiness_ids = state
            .current_task_keys
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .map(|key| logical_readiness_id(key))
            .collect::<Vec<_>>();
        if task_key.contains(":poll:") {
            readiness_ids.push(logical_readiness_id(task_key));
        }
        readiness_ids.sort();
        readiness_ids.dedup();
        readiness_ids
    }

    /// Runs publication and its cursor commit while holding the lifecycle ownership lock.
    ///
    /// A snapshot replacement must not be able to observe a published event before its
    /// continuity cursor is committed. The commit is therefore invoked only after publication
    /// succeeds and before the current task key can be replaced.
    fn with_current_task_key_after_publish<P, C>(
        &self,
        task_key: &str,
        publish: P,
        commit: C,
    ) -> anyhow::Result<bool>
    where
        P: FnOnce() -> anyhow::Result<()>,
        C: FnOnce(),
    {
        let mut publication_error = None;
        let published = self.with_current_task_key(task_key, || match publish() {
            Ok(()) => commit(),
            Err(error) => publication_error = Some(error),
        });
        if let Some(error) = publication_error {
            Err(error)
        } else {
            Ok(published)
        }
    }

    fn is_operational(&self) -> bool {
        self.state
            .lock()
            .expect("market-data lifecycle lock")
            .non_operational_groups
            .is_empty()
    }
}

fn task_key_is_current(state: &MarketDataStreamHealthState, task_key: &str) -> bool {
    state.current_task_keys.contains(task_key)
}

fn instrument_metadata_snapshot(
    metadata: &SharedInstrumentMetadata,
) -> HashMap<String, MarketDataInstrumentMetadata> {
    metadata.read().expect("market-data metadata lock").clone()
}

fn merge_resolved_instrument_stream_ids(
    stream_ids: &SharedInstrumentStreamIds,
    refreshed_stream_ids: HashMap<String, String>,
) {
    stream_ids
        .write()
        .expect("market-data stream IDs lock")
        .extend(refreshed_stream_ids);
}

#[cfg(test)]
fn bar_watermarks_for_subscriptions(
    subscriptions: &[(String, BarType)],
    cached: &HashMap<BarType, i128>,
) -> HashMap<BarType, i128> {
    subscriptions
        .iter()
        .filter_map(|(_, bar_type)| {
            cached
                .get(bar_type)
                .copied()
                .map(|watermark| (*bar_type, watermark))
        })
        .collect()
}

fn bar_watermarks_for_streams(
    subscriptions: &[(String, BarType)],
    watermarks: &HashMap<BarType, i128>,
) -> HashMap<BarType, i128> {
    subscriptions
        .iter()
        .filter_map(|(_, bar_type)| {
            watermarks
                .get(bar_type)
                .copied()
                .map(|watermark| (*bar_type, watermark))
        })
        .collect()
}

type BarStreamSubscriptions = Vec<(String, BarType)>;

fn partition_bar_stream_subscriptions(
    subscriptions: Vec<(String, BarType)>,
    periodic_instrument_ids: &HashSet<String>,
) -> (BarStreamSubscriptions, BarStreamSubscriptions) {
    subscriptions
        .into_iter()
        .partition(|(stream_id, _)| periodic_instrument_ids.contains(stream_id))
}

fn should_restore_market_data_streams(
    replacing_clients: bool,
    subscriptions_on_reconnect: bool,
) -> bool {
    !replacing_clients || subscriptions_on_reconnect
}

fn quote_subscription_groups(
    subscriptions: &[(String, InstrumentId)],
) -> Vec<(Vec<String>, HashMap<String, InstrumentId>)> {
    let mut entries = subscriptions.to_vec();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
        .chunks(MAX_QUOTES_PER_STREAM)
        .map(|chunk| {
            let stream_ids = chunk
                .iter()
                .map(|(stream_id, _)| stream_id.clone())
                .collect::<Vec<_>>();
            let instrument_ids = chunk
                .iter()
                .map(|(stream_id, instrument_id)| (stream_id.clone(), *instrument_id))
                .collect();
            (stream_ids, instrument_ids)
        })
        .collect()
}

fn publish_instrument_definitions(instrument_provider: &TbankInstrumentProvider) {
    let Some(sender) = try_get_data_event_sender() else {
        return;
    };
    for instrument in instrument_provider.store().list_all() {
        if let Err(error) = sender.send(DataEvent::Instrument(instrument.clone())) {
            tracing::warn!(%error, "failed to publish T-Bank instrument definition");
        }
    }
}

async fn refresh_published_instrument_catalogue(
    config: &TbankDataClientConfig,
    configured_stream_ids: &HashMap<String, String>,
    resolved_stream_ids: &SharedInstrumentStreamIds,
    instrument_metadata: &SharedInstrumentMetadata,
) -> Result<()> {
    let (provider, refreshed_stream_ids, resolved_metadata) =
        load_resolved_instrument_catalogue(config, configured_stream_ids).await?;

    // Merge successful refreshes so a transiently unresolved auto-discovered contract cannot
    // remove either decoder metadata or its canonical stream route from an already active
    // subscription. Full connect remains the owner of routing-map replacement.
    merge_resolved_instrument_stream_ids(resolved_stream_ids, refreshed_stream_ids);
    instrument_metadata
        .write()
        .expect("market-data metadata lock")
        .extend(resolved_metadata);
    publish_instrument_definitions(&provider);
    Ok(())
}

async fn load_resolved_instrument_catalogue(
    config: &TbankDataClientConfig,
    configured_stream_ids: &HashMap<String, String>,
) -> Result<(
    TbankInstrumentProvider,
    HashMap<String, String>,
    HashMap<String, MarketDataInstrumentMetadata>,
)> {
    // Keep provider selection tied to the immutable user configuration. Runtime stream routing
    // may contain auto-discovered shares after connect, but must never widen futures enrichment.
    let mut provider_config = config.clone();
    provider_config.instrument_stream_ids = configured_stream_ids.clone();
    let mut provider = TbankInstrumentProvider::new(provider_config);
    provider
        .load_all_current(None)
        .await
        .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))?;
    provider.ensure_configured_futures_resolved(configured_stream_ids)?;
    let (resolved_stream_ids, resolved_metadata) = resolve_instrument_metadata(
        configured_stream_ids,
        &config.indicative_instruments,
        provider.market_data_metadata(),
    )?;
    Ok((provider, resolved_stream_ids, resolved_metadata))
}

fn resolve_instrument_metadata<'a>(
    configured_stream_ids: &HashMap<String, String>,
    indicative_instruments: &HashMap<String, TbankIndicativeInstrumentConfig>,
    provider_metadata: impl IntoIterator<
        Item = &'a crate::instruments::TbankMarketDataInstrumentMetadata,
    >,
) -> Result<(
    HashMap<String, String>,
    HashMap<String, MarketDataInstrumentMetadata>,
)> {
    let mut resolved_stream_ids = configured_stream_ids.clone();
    let mut instrument_metadata = HashMap::new();
    for metadata in provider_metadata {
        if let Some(configured_uid) = configured_stream_ids.get(&metadata.instrument_id)
            && configured_uid != &metadata.instrument_uid
        {
            return Err(TbankAdapterError::ConfigError(format!(
                "configured stream ID {configured_uid} does not match provider UID {} for {}",
                metadata.instrument_uid, metadata.instrument_id
            )));
        }
        resolved_stream_ids.insert(
            metadata.instrument_id.clone(),
            metadata.instrument_uid.clone(),
        );
        let resolved_metadata = MarketDataInstrumentMetadata {
            lot_size: metadata.lot_size,
            price_precision: metadata.price_precision,
            preserve_instrument_id: indicative_instruments.contains_key(&metadata.instrument_id),
        };
        instrument_metadata.insert(metadata.instrument_id.clone(), resolved_metadata);
        instrument_metadata.insert(metadata.instrument_uid.clone(), resolved_metadata);
    }
    Ok((resolved_stream_ids, instrument_metadata))
}

/// Nautilus market-data client backed by T-Bank gRPC streams.
pub struct TbankDataClient {
    client_id: ClientId,
    config: TbankDataClientConfig,
    /// The runtime map includes canonical broker UIDs discovered by the catalogue refresh.
    resolved_instrument_stream_ids: SharedInstrumentStreamIds,
    subscriptions: TbankSubscriptionRegistry,
    cache: Option<CacheView>,
    instrument_metadata: SharedInstrumentMetadata,
    instrument_subscriptions: Vec<(Venue, TypedHandler<InstrumentAny>)>,
    clients: Option<TbankGrpcClients<TbankAuthInterceptor>>,
    stream_tasks: HashMap<String, JoinHandle<()>>,
    bar_stream_task: Option<JoinHandle<()>>,
    quote_stream_task: Option<JoinHandle<()>>,
    instrument_refresh_task: Option<JoinHandle<()>>,
    message_sequence: Arc<AtomicU64>,
    /// Desired Nautilus subscriptions. Broker stream IDs are derived only while building a
    /// concrete stream request, because catalogue discovery can change them between sessions.
    bar_subscriptions: HashMap<InstrumentId, nautilus_model::data::BarType>,
    /// Instrument-scoped readiness keys for the currently scheduled bar snapshot.
    scheduled_bar_continuity_keys: HashMap<String, String>,
    quote_subscriptions: HashSet<InstrumentId>,
    trade_subscriptions: HashSet<InstrumentId>,
    depth10_subscriptions: HashMap<InstrumentId, i32>,
    historical_request_limiter: HistoricalRequestLimiter,
    stream_health: Arc<MarketDataStreamHealth>,
    /// Watermarks belong to the client lifecycle, not to one replaceable stream supervisor.
    bar_watermarks: SharedBarWatermarks,
    /// Monotonic ownership generations for logical non-bar stream tasks.
    stream_task_generations: HashMap<String, u64>,
    /// Maps a logical non-bar stream to the currently owned generation-qualified task key.
    active_stream_task_keys: HashMap<String, String>,
    bar_stream_generation: u64,
}

impl TbankDataClient {
    /// Creates a new instance.
    pub fn new(config: TbankDataClientConfig) -> Self {
        let resolved_instrument_stream_ids =
            Arc::new(RwLock::new(config.instrument_stream_ids.clone()));
        let historical_request_limiter =
            HistoricalRequestLimiter::new(config.historical_candle_request_delay);
        Self {
            client_id: *TBANK_CLIENT_ID,
            config,
            resolved_instrument_stream_ids,
            subscriptions: TbankSubscriptionRegistry::default(),
            cache: None,
            instrument_metadata: Arc::new(RwLock::new(HashMap::new())),
            instrument_subscriptions: Vec::new(),
            clients: None,
            stream_tasks: HashMap::new(),
            bar_stream_task: None,
            quote_stream_task: None,
            instrument_refresh_task: None,
            message_sequence: Arc::new(AtomicU64::new(0)),
            bar_subscriptions: HashMap::new(),
            scheduled_bar_continuity_keys: HashMap::new(),
            quote_subscriptions: HashSet::new(),
            trade_subscriptions: HashSet::new(),
            depth10_subscriptions: HashMap::new(),
            historical_request_limiter,
            stream_health: Arc::new(MarketDataStreamHealth::default()),
            bar_watermarks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            stream_task_generations: HashMap::new(),
            active_stream_task_keys: HashMap::new(),
            bar_stream_generation: 0,
        }
    }

    /// Sets the Nautilus client ID and returns the updated client.
    #[must_use]
    pub fn with_client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = client_id;
        self
    }

    pub(crate) fn with_cache(mut self, cache: CacheView) -> Self {
        self.cache = Some(cache);
        self
    }

    fn subscribe_instrument_updates(&mut self) {
        if !self.instrument_subscriptions.is_empty() {
            return;
        }

        let configured_indicatives = Arc::new(
            self.config
                .indicative_instruments
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
        );
        for configured_venue in TbankVenue::all() {
            let venue = configured_venue.venue();
            let metadata = Arc::downgrade(&self.instrument_metadata);
            let configured_indicatives = configured_indicatives.clone();
            let handler = TypedHandler::from(move |instrument: &InstrumentAny| {
                let Some(metadata_store) = metadata.upgrade() else {
                    return;
                };
                let Some(instrument_metadata) =
                    TbankInstrumentMetadata::from_instrument(instrument)
                else {
                    return;
                };
                if instrument_metadata.venue != configured_venue
                    || !instrument_metadata.is_supported()
                {
                    return;
                }
                let Ok(price_precision) = u8::try_from(instrument_metadata.price_precision) else {
                    tracing::warn!(
                        instrument_id = %instrument_metadata.instrument_id,
                        price_precision = instrument_metadata.price_precision,
                        "ignoring T-Bank instrument update with unsupported price precision"
                    );
                    return;
                };
                let mut store = metadata_store.write().expect("market-data metadata lock");
                let preserve_instrument_id = configured_indicatives
                    .contains(&instrument_metadata.instrument_id)
                    || store
                        .get(&instrument_metadata.instrument_id)
                        .is_some_and(|value| value.preserve_instrument_id);
                let resolved = MarketDataInstrumentMetadata {
                    lot_size: instrument_metadata.lot,
                    price_precision,
                    preserve_instrument_id,
                };
                store.insert(instrument_metadata.instrument_id.clone(), resolved);
                store.insert(instrument_metadata.instrument_uid.clone(), resolved);
            });
            msgbus::subscribe_instruments(
                switchboard::get_instruments_pattern(venue),
                handler.clone(),
                None,
            );
            self.instrument_subscriptions.push((venue, handler));
        }
    }

    fn unsubscribe_instrument_updates(&mut self) {
        for (venue, handler) in self.instrument_subscriptions.drain(..) {
            msgbus::unsubscribe_instruments(switchboard::get_instruments_pattern(venue), &handler);
        }
    }

    /// Connects the client to the configured T-Bank endpoint.
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        let replacing_clients = self.clients.is_some();
        self.config.validate()?;
        let (instrument_provider, instrument_stream_ids, instrument_metadata) =
            load_resolved_instrument_catalogue(&self.config, &self.config.instrument_stream_ids)
                .await?;
        let token = self.config.resolve_token_secret()?;
        let endpoint = self.config.endpoint_uri()?;
        let channel = connect_channel(&endpoint, self.config.request_timeout).await?;
        let interceptor = TbankAuthInterceptor::new(&token)?;
        self.stop_market_data_streams(
            "market data clients are being replaced",
            replacing_clients && !self.config.subscriptions_on_reconnect,
        );
        *self
            .resolved_instrument_stream_ids
            .write()
            .expect("market-data stream IDs lock") = instrument_stream_ids;
        *self
            .instrument_metadata
            .write()
            .expect("market-data metadata lock") = instrument_metadata;
        self.clients = Some(TbankGrpcClients::new(channel, interceptor));
        self.subscribe_instrument_updates();
        publish_instrument_definitions(&instrument_provider);
        self.schedule_instrument_refresh();
        if should_restore_market_data_streams(
            replacing_clients,
            self.config.subscriptions_on_reconnect,
        ) {
            self.restore_market_data_streams()
                .map_err(|error| TbankAdapterError::ConfigError(error.to_string()))?;
        } else if replacing_clients {
            // `subscriptions_on_reconnect = false` is an explicit opt-out, not merely a
            // request to delay the old streams. Drop the desired state as well, otherwise the
            // next unrelated subscribe command would reschedule every pre-reconnect stream.
            self.clear_market_data_subscription_state();
        }
        tracing::info!(
            environment = ?self.config.environment,
            endpoint = endpoint.as_str(),
            token_present = true,
            "connected T-Bank data client"
        );
        Ok(())
    }

    /// Disconnects the client and stops its background tasks.
    pub fn disconnect(&mut self) {
        self.stop_market_data_streams("market data client disconnected", false);
        if let Some(task) = self.instrument_refresh_task.take() {
            task.abort();
        }
        self.unsubscribe_instrument_updates();
        self.clients = None;
        *self
            .resolved_instrument_stream_ids
            .write()
            .expect("market-data stream IDs lock") = self.config.instrument_stream_ids.clone();
        tracing::info!("disconnected T-Bank data client");
    }

    fn stop_market_data_streams(&mut self, reason: &str, terminal: bool) {
        let stage = if terminal {
            "stream_subscriptions_disabled"
        } else {
            "stream_subscriptions_stopped"
        };
        self.advance_bar_stream_generation_with_stage(Some(stage), reason);
        self.stream_health.retire_all(stage, reason);
        for (_, task) in self.stream_tasks.drain() {
            task.abort();
        }
        self.active_stream_task_keys.clear();
        if let Some(task) = self.bar_stream_task.take() {
            task.abort();
        }
        if let Some(task) = self.quote_stream_task.take() {
            task.abort();
        }
    }

    fn advance_bar_stream_generation_with_stage(
        &mut self,
        stage: Option<&str>,
        reason: &str,
    ) -> u64 {
        self.bar_stream_generation = self.bar_stream_generation.saturating_add(1);
        self.stream_health.advance_bar_generation_with_stage(
            self.bar_stream_generation,
            stage,
            reason,
        );
        self.bar_stream_generation
    }

    fn invalidate_bar_snapshot(&mut self, stream_reason: &str) -> u64 {
        self.advance_bar_stream_generation_with_stage(None, stream_reason)
    }

    fn clear_market_data_subscription_state(&mut self) {
        self.subscriptions = TbankSubscriptionRegistry::default();
        self.bar_subscriptions.clear();
        self.scheduled_bar_continuity_keys.clear();
        self.quote_subscriptions.clear();
        self.trade_subscriptions.clear();
        self.depth10_subscriptions.clear();
        self.active_stream_task_keys.clear();
        self.bar_watermarks
            .lock()
            .expect("market-data watermark lock")
            .clear();
    }

    fn restore_market_data_streams(&mut self) -> anyhow::Result<()> {
        self.schedule_bar_streams()?;
        self.schedule_quote_stream()?;

        let trade_subscriptions = self.trade_subscriptions.iter().copied().collect::<Vec<_>>();
        for instrument_id in trade_subscriptions {
            self.schedule_trade_stream(instrument_id)?;
        }

        let depth10_subscriptions = self
            .depth10_subscriptions
            .iter()
            .map(|(instrument_id, depth)| (*instrument_id, *depth))
            .collect::<Vec<_>>();
        for (instrument_id, depth) in depth10_subscriptions {
            self.schedule_depth10_stream(instrument_id, depth)?;
        }

        Ok(())
    }

    fn schedule_instrument_refresh(&mut self) {
        if let Some(task) = self.instrument_refresh_task.take() {
            task.abort();
        }
        let refresh_interval = self.config.instrument_refresh_interval;
        if refresh_interval.is_zero() {
            return;
        }
        let config = self.config.clone();
        let configured_stream_ids = config.instrument_stream_ids.clone();
        let resolved_stream_ids = Arc::clone(&self.resolved_instrument_stream_ids);
        let instrument_metadata = Arc::clone(&self.instrument_metadata);
        self.instrument_refresh_task = Some(get_runtime().spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The connect path has just loaded and published the same authoritative catalogue.
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = refresh_published_instrument_catalogue(
                    &config,
                    &configured_stream_ids,
                    &resolved_stream_ids,
                    &instrument_metadata,
                )
                .await
                {
                    tracing::warn!(
                        %error,
                        interval_ms = refresh_interval.as_millis(),
                        "failed to refresh T-Bank instrument catalogue"
                    );
                }
            }
        }));
    }

    /// Returns whether the client is connected.
    pub fn is_connected(&self) -> bool {
        self.clients.is_some() && self.stream_health.is_operational()
    }

    /// Loads one-minute bars for an instrument and time range.
    pub async fn get_1m_bars(
        &mut self,
        instrument_uid: &str,
        from_unix_nanos: i128,
        to_unix_nanos: i128,
    ) -> Result<Vec<TbankBar>> {
        let requests =
            Self::build_1m_candle_requests(instrument_uid, from_unix_nanos, to_unix_nanos)?;
        let mut bars = Vec::new();
        let request_count = requests.len();
        let request_timeout = self.config.historical_candle_request_timeout;
        for (request_index, request) in requests.into_iter().enumerate() {
            let response = {
                let mut attempt = 0_u32;
                loop {
                    self.throttle_historical_candle_request().await;
                    let request_result = {
                        let clients = self.clients_mut()?;
                        tokio::time::timeout(
                            request_timeout,
                            clients
                                .market_data
                                .get_candles(with_timeout(request.clone(), request_timeout)),
                        )
                        .await
                    };
                    match request_result {
                        Ok(Ok(response)) => break response.into_inner(),
                        Err(_) if attempt < self.config.historical_candle_max_retries => {
                            let multiplier = 1_u32 << attempt.min(6);
                            let delay = self
                                .config
                                .historical_candle_retry_base_delay
                                .saturating_mul(multiplier);
                            tracing::warn!(
                                instrument_uid = %instrument_uid,
                                chunk_index = request_index + 1,
                                chunk_count = request_count,
                                attempt = attempt + 1,
                                delay_ms = delay.as_millis(),
                                timeout_ms = request_timeout.as_millis(),
                                "retrying T-Bank GetCandles chunk after timeout"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        Err(_) => return Err(historical_candle_timeout_error(request_timeout)),
                        Ok(Err(status))
                            if attempt < self.config.historical_candle_max_retries
                                && retryable_candle_request_status(&status) =>
                        {
                            let multiplier = 1_u32 << attempt.min(6);
                            let delay = self
                                .config
                                .historical_candle_retry_base_delay
                                .saturating_mul(multiplier);
                            tracing::warn!(
                                instrument_uid = %instrument_uid,
                                chunk_index = request_index + 1,
                                chunk_count = request_count,
                                attempt = attempt + 1,
                                delay_ms = delay.as_millis(),
                                code = %status.code(),
                                reason = %status.message(),
                                "retrying T-Bank GetCandles chunk"
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                        }
                        Ok(Err(status)) => return Err(TbankAdapterError::from(status)),
                    }
                }
            };

            let received_at = i128::from(now_unix_nanos().as_u64());
            let mut chunk_bars = response
                .candles
                .into_iter()
                .map(|candle| {
                    let instrument_uid = instrument_uid.to_string();
                    candle_to_bar(
                        &crate::grpc::generated::Candle {
                            open: candle.open,
                            high: candle.high,
                            low: candle.low,
                            close: candle.close,
                            volume: candle.volume,
                            time: candle.time,
                            instrument_uid,
                            ..crate::grpc::generated::Candle::default()
                        },
                        self.config.candle_timestamp_mode,
                        received_at,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            bars.append(&mut chunk_bars);
        }
        bars.sort_by_key(|bar| bar.ts_event);
        bars.dedup_by_key(|bar| bar.ts_event);
        Ok(bars)
    }

    async fn throttle_historical_candle_request(&self) {
        self.historical_request_limiter.acquire().await;
    }

    fn build_1m_candle_requests(
        instrument_uid: &str,
        from_unix_nanos: i128,
        to_unix_nanos: i128,
    ) -> Result<Vec<GetCandlesRequest>> {
        one_minute_candle_query_chunks(from_unix_nanos, to_unix_nanos)
            .into_iter()
            .map(|(chunk_from, chunk_to)| {
                Ok(GetCandlesRequest {
                    #[allow(deprecated)]
                    figi: None,
                    from: Some(unix_nanos_to_timestamp(chunk_from)?),
                    to: Some(unix_nanos_to_timestamp(chunk_to)?),
                    interval: CandleInterval::CandleInterval1Min as i32,
                    instrument_id: Some(instrument_uid.to_string()),
                    candle_source_type: Some(get_candles_request::CandleSource::Exchange as i32),
                    limit: None,
                })
            })
            .collect()
    }

    /// Loads the latest prices for the requested instruments.
    pub async fn get_last_prices(
        &mut self,
        instrument_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<GetLastPricesResponse> {
        let request = GetLastPricesRequest {
            #[allow(deprecated)]
            figi: Vec::new(),
            instrument_id: instrument_uids.into_iter().map(Into::into).collect(),
            last_price_type: LastPriceType::LastPriceExchange as i32,
            instrument_status: None,
        };
        let request = with_timeout(request, self.config.request_timeout);
        Ok(self
            .clients_mut()?
            .market_data
            .get_last_prices(request)
            .await
            .map_err(TbankAdapterError::from)?
            .into_inner())
    }

    /// Loads the latest price as an adapter quote tick.
    pub async fn get_last_price_quote(&mut self, instrument_uid: &str) -> Result<TbankQuoteTick> {
        let response = self.get_last_prices([instrument_uid.to_string()]).await?;
        let price = response
            .last_prices
            .first()
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_uid.to_string()))?;
        last_price_to_quote(price, i128::from(now_unix_nanos().as_u64()))
    }

    /// Loads a raw T-Bank order-book snapshot.
    pub async fn get_order_book(
        &mut self,
        instrument_uid: &str,
        depth: i32,
    ) -> Result<GetOrderBookResponse> {
        let request = GetOrderBookRequest {
            #[allow(deprecated)]
            figi: None,
            depth,
            instrument_id: Some(instrument_uid.to_string()),
        };
        let request = with_timeout(request, self.config.request_timeout);
        Ok(self
            .clients_mut()?
            .market_data
            .get_order_book(request)
            .await
            .map_err(TbankAdapterError::from)?
            .into_inner())
    }

    /// Loads and converts a T-Bank order-book snapshot.
    pub async fn get_order_book_snapshot(
        &mut self,
        instrument_uid: &str,
        depth: i32,
    ) -> Result<TbankOrderBookSnapshot> {
        let response = self.get_order_book(instrument_uid, depth).await?;
        orderbook_to_snapshot(
            &crate::grpc::generated::OrderBook {
                depth: response.depth,
                is_consistent: true,
                bids: response.bids,
                asks: response.asks,
                time: response.orderbook_ts,
                instrument_uid: response.instrument_uid,
                ..crate::grpc::generated::OrderBook::default()
            },
            i128::from(now_unix_nanos().as_u64()),
        )
    }

    /// Opens a T-Bank server-side market-data stream.
    pub async fn open_server_side_stream(
        &mut self,
        request: MarketDataServerSideStreamRequest,
    ) -> Result<tonic::Streaming<MarketDataResponse>> {
        Ok(self
            .clients_mut()?
            .market_data_stream
            .market_data_server_side_stream(request)
            .await
            .map_err(TbankAdapterError::from)?
            .into_inner())
    }

    /// Registers a one-minute bar subscription.
    pub fn subscribe_bars_1m(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        self.subscriptions.subscribe_bars_1m(instrument_uid)
    }

    /// Removes a one-minute bar subscription.
    pub fn unsubscribe_bars_1m(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.subscriptions.unsubscribe_bars_1m(instrument_uid)
    }

    /// Registers a trade subscription.
    pub fn subscribe_trades(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        self.subscriptions.subscribe_trades(instrument_uid)
    }

    /// Removes a trade subscription.
    pub fn unsubscribe_trades(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.subscriptions.unsubscribe_trades(instrument_uid)
    }

    /// Registers a quote subscription.
    pub fn subscribe_quotes(&mut self, instrument_uid: impl Into<String>) -> MarketDataRequest {
        self.subscriptions.subscribe_quotes(instrument_uid)
    }

    /// Removes a quote subscription.
    pub fn unsubscribe_quotes(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.subscriptions.unsubscribe_quotes(instrument_uid)
    }

    /// Registers an order-book subscription.
    pub fn subscribe_order_book(
        &mut self,
        instrument_uid: impl Into<String>,
        depth: i32,
    ) -> MarketDataRequest {
        self.subscriptions
            .subscribe_order_book(instrument_uid, depth)
    }

    /// Removes an order-book subscription.
    pub fn unsubscribe_order_book(
        &mut self,
        instrument_uid: &str,
        depth: i32,
    ) -> MarketDataRequest {
        self.subscriptions
            .unsubscribe_order_book(instrument_uid, depth)
    }

    /// Removes all depth-book subscriptions for an instrument.
    pub fn unsubscribe_depth_books(&mut self, instrument_uid: &str) -> MarketDataRequest {
        self.subscriptions.unsubscribe_depth_books(instrument_uid)
    }

    /// Builds requests that restore current subscriptions after reconnect.
    pub fn restore_subscription_requests(&self) -> Vec<crate::grpc::generated::MarketDataRequest> {
        self.subscriptions
            .restore_requests_with_stream_ids(|instrument_id| self.stream_id(instrument_id))
    }

    fn stream_id(&self, instrument_id: InstrumentId) -> String {
        let instrument_id_string = instrument_id.to_string();
        self.resolved_instrument_stream_ids
            .read()
            .expect("market-data stream IDs lock")
            .get(&instrument_id_string)
            .cloned()
            .unwrap_or_else(|| instrument_stream_id(instrument_id))
    }

    /// Wraps a subscription request for the server-side streaming API.
    pub fn server_side_request_from_subscription(
        request: MarketDataRequest,
    ) -> Result<MarketDataServerSideStreamRequest> {
        match request.payload {
            Some(market_data_request::Payload::SubscribeCandlesRequest(request)) => {
                Ok(MarketDataServerSideStreamRequest {
                    subscribe_candles_request: Some(request),
                    ..MarketDataServerSideStreamRequest::default()
                })
            }
            Some(market_data_request::Payload::SubscribeOrderBookRequest(request)) => {
                Ok(MarketDataServerSideStreamRequest {
                    subscribe_order_book_request: Some(request),
                    ..MarketDataServerSideStreamRequest::default()
                })
            }
            Some(market_data_request::Payload::SubscribeTradesRequest(request)) => {
                Ok(MarketDataServerSideStreamRequest {
                    subscribe_trades_request: Some(request),
                    ..MarketDataServerSideStreamRequest::default()
                })
            }
            Some(market_data_request::Payload::SubscribeLastPriceRequest(request)) => {
                Ok(MarketDataServerSideStreamRequest {
                    subscribe_last_price_request: Some(request),
                    ..MarketDataServerSideStreamRequest::default()
                })
            }
            _ => Err(TbankAdapterError::ConfigError(
                "unsupported server-side stream subscription request".to_string(),
            )),
        }
    }

    fn clients_mut(&mut self) -> Result<&mut TbankGrpcClients<TbankAuthInterceptor>> {
        self.clients.as_mut().ok_or_else(|| {
            TbankAdapterError::ConfigError("data client is not connected".to_string())
        })
    }

    fn cached_bar_watermarks(&self) -> HashMap<BarType, i128> {
        let Some(cache) = &self.cache else {
            return HashMap::new();
        };
        let cache = cache.borrow();
        cache
            .bar_types(None, None, AggregationSource::External)
            .into_iter()
            .filter_map(|bar_type| {
                let latest = cache.bars(bar_type)?.last().copied()?;
                Some((*bar_type, i128::from(latest.ts_event.as_u64())))
            })
            .collect()
    }

    fn seed_bar_watermarks_from_cache(&self, subscriptions: &[(String, BarType)]) {
        let cached = self.cached_bar_watermarks();
        for (_, bar_type) in subscriptions {
            if let Some(watermark) = cached.get(bar_type).copied() {
                // Cache is only a fallback baseline. A live or previously recovered watermark
                // from this client lifecycle is authoritative and must never move backwards.
                record_bar_watermark(&self.bar_watermarks, *bar_type, watermark);
            }
        }
    }

    fn next_stream_generation(&mut self, logical_task_key: &str) -> u64 {
        let generation = self
            .stream_task_generations
            .entry(logical_task_key.to_string())
            .or_default();
        *generation = generation.saturating_add(1);
        *generation
    }

    fn generation_task_key(logical_task_key: &str, generation: u64) -> String {
        format!("{logical_task_key}:generation:{generation}")
    }

    fn schedule_bar_streams(&mut self) -> anyhow::Result<()> {
        let generation = self.invalidate_bar_snapshot("bar stream snapshot was replaced");
        if self.bar_subscriptions.is_empty() {
            self.scheduled_bar_continuity_keys.clear();
            self.stream_health
                .replace_expected("bars:", std::iter::empty());
            if let Some(existing) = self.bar_stream_task.take() {
                existing.abort();
            }
            return Ok(());
        }

        let market_data_stream = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data_stream
            .clone();
        let market_data_client = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data
            .clone();
        let sender = get_data_event_sender();
        let timestamp_mode = self.config.candle_timestamp_mode;
        let config = self.config.clone();
        let instrument_metadata = self.instrument_metadata.clone();
        let historical_request_limiter = self.historical_request_limiter.clone();
        let batch_size = self.config.max_candle_instruments_per_stream.max(1);

        let mut subscriptions = self
            .bar_subscriptions
            .iter()
            .map(|(instrument_id, bar_type)| (self.stream_id(*instrument_id), *bar_type))
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| {
            left.0
                .to_string()
                .cmp(&right.0.to_string())
                .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
        });
        self.seed_bar_watermarks_from_cache(&subscriptions);
        let initial_bar_watermarks = snapshot_bar_watermarks(&self.bar_watermarks);

        let (poll_subscriptions, stream_subscriptions) = partition_bar_stream_subscriptions(
            subscriptions,
            &config.periodic_candle_poll_instrument_ids,
        );

        let groups = stream_subscriptions
            .chunks(batch_size)
            .enumerate()
            .map(|(group_index, chunk)| {
                let stream_ids = chunk
                    .iter()
                    .map(|(stream_id, _)| stream_id.clone())
                    .collect::<Vec<_>>();
                let bar_types = chunk
                    .iter()
                    .map(|(stream_id, bar_type)| (stream_id.clone(), *bar_type))
                    .collect::<HashMap<_, _>>();
                let request = MarketDataServerSideStreamRequest {
                    subscribe_candles_request: Some(SubscribeCandlesRequest {
                        subscription_action: SubscriptionAction::Subscribe as i32,
                        instruments: stream_ids
                            .iter()
                            .cloned()
                            .map(|instrument_uid| CandleInstrument {
                                instrument_id: instrument_uid,
                                interval: SubscriptionInterval::OneMinute as i32,
                                ..CandleInstrument::default()
                            })
                            .collect(),
                        waiting_close: true,
                        candle_source_type: Some(
                            get_candles_request::CandleSource::Exchange as i32,
                        ),
                    }),
                    ..MarketDataServerSideStreamRequest::default()
                };
                (
                    format!("bars:generation:{generation}:group:{group_index}:1m"),
                    request,
                    TbankStreamKind::Bars { bar_types },
                )
            })
            .collect::<Vec<_>>();

        let mut next_continuity_keys = HashMap::new();
        for (task_key, _, kind) in &groups {
            let TbankStreamKind::Bars { bar_types } = kind else {
                unreachable!("bar scheduler only creates bar stream groups");
            };
            next_continuity_keys.extend(bar_types.keys().map(|instrument_uid| {
                (
                    instrument_uid.clone(),
                    bar_continuity_key(task_key, instrument_uid),
                )
            }));
        }
        next_continuity_keys.extend(poll_subscriptions.iter().map(|(instrument_uid, _)| {
            (
                instrument_uid.clone(),
                periodic_candle_stream_key(generation, instrument_uid),
            )
        }));

        // Replace the desired lifecycle snapshot synchronously. `invalidate_bar_snapshot` has
        // already fenced the old generation and invalidated its readiness before this new set is
        // registered or the old task is aborted.
        let mut health_task_keys = groups
            .iter()
            .map(|(task_key, _, _)| task_key.clone())
            .collect::<Vec<_>>();
        health_task_keys.extend(
            poll_subscriptions
                .iter()
                .map(|(instrument_uid, _)| periodic_candle_stream_key(generation, instrument_uid)),
        );
        self.stream_health
            .replace_expected("bars:", health_task_keys.iter().map(String::as_str));
        for (instrument_uid, continuity_key) in &next_continuity_keys {
            if !poll_subscriptions
                .iter()
                .any(|(poll_uid, _)| poll_uid == instrument_uid)
            {
                // Live-bar readiness is fenced by the current snapshot but is not itself a
                // separate health group; the parent stream group carries the operational gate.
                self.stream_health.register_current(continuity_key);
            }
        }
        self.scheduled_bar_continuity_keys = next_continuity_keys;
        if let Some(existing) = self.bar_stream_task.take() {
            existing.abort();
        }

        if !groups.is_empty() || !poll_subscriptions.is_empty() {
            let stream_market_data_client = market_data_client.clone();
            let stream_config = config.clone();
            let stream_sender = sender.clone();
            let stream_instrument_metadata = instrument_metadata.clone();
            let stream_historical_request_limiter = historical_request_limiter.clone();
            let stream_message_sequence = self.message_sequence.clone();
            let stream_health = self.stream_health.clone();
            let stream_bar_watermarks = self.bar_watermarks.clone();
            let poll_watermarks =
                bar_watermarks_for_streams(&poll_subscriptions, &initial_bar_watermarks);
            let task = get_runtime().spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                tracing::info!(
                    stream_subscriptions = stream_subscriptions.len(),
                    poll_subscriptions = poll_subscriptions.len(),
                    groups = groups.len(),
                    max_candle_instruments_per_stream = batch_size,
                    "starting batched T-Bank candle subscription snapshot"
                );
                let stream_future = async {
                    let futures = groups
                        .into_iter()
                        .map(|(task_key, request, kind)| {
                            run_market_data_stream(MarketDataStreamContext {
                                market_data_stream: market_data_stream.clone(),
                                market_data_client: stream_market_data_client.clone(),
                                sender: stream_sender.clone(),
                                request,
                                kind,
                                config: stream_config.clone(),
                                historical_request_limiter: stream_historical_request_limiter
                                    .clone(),
                                instrument_metadata: stream_instrument_metadata.clone(),
                                task_key,
                                bar_watermarks: stream_bar_watermarks.clone(),
                                bar_continuity_key_overrides: HashMap::new(),
                                reconnect_attempt: Arc::new(AtomicU32::new(0)),
                                message_sequence: stream_message_sequence.clone(),
                                stream_health: stream_health.clone(),
                            })
                        })
                        .collect::<Vec<_>>();
                    join_all(futures).await;
                };
                if poll_subscriptions.is_empty() {
                    stream_future.await;
                } else {
                    let poll_stream_health = stream_health.clone();
                    let poll_bar_watermarks = stream_bar_watermarks.clone();
                    tokio::join!(
                        stream_future,
                        run_periodic_candle_poll(
                            market_data_client,
                            sender,
                            timestamp_mode,
                            config,
                            historical_request_limiter,
                            instrument_metadata,
                            poll_subscriptions,
                            poll_watermarks,
                            poll_bar_watermarks,
                            HashMap::new(),
                            generation,
                            poll_stream_health,
                        )
                    );
                }
            });
            self.bar_stream_task = Some(task);
        }
        Ok(())
    }

    fn schedule_quote_stream(&mut self) -> anyhow::Result<()> {
        self.stream_health.retire_prefix(
            "quotes:",
            None,
            "quote stream subscription snapshot was replaced",
        );
        if let Some(existing) = self.quote_stream_task.take() {
            existing.abort();
        }
        let generation = self.next_stream_generation("quotes:snapshot");
        if self.quote_subscriptions.is_empty() {
            return Ok(());
        }

        let market_data_stream = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data_stream
            .clone();
        let market_data_client = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data
            .clone();
        let subscriptions = self
            .quote_subscriptions
            .iter()
            .map(|instrument_id| (self.stream_id(*instrument_id), *instrument_id))
            .collect::<Vec<_>>();
        let groups = quote_subscription_groups(&subscriptions);
        let expected_task_keys = (0..groups.len())
            .map(|group_index| format!("quotes:generation:{generation}:group:{group_index}:depth1"))
            .collect::<Vec<_>>();
        // Register the complete desired snapshot synchronously. `run_market_data_stream` must
        // not own this transition because the parent task intentionally waits before opening the
        // broker stream.
        self.stream_health
            .replace_expected("quotes:", expected_task_keys.iter().map(String::as_str));
        let sender = get_data_event_sender();
        let config = self.config.clone();
        let instrument_metadata = self.instrument_metadata.clone();
        let historical_request_limiter = self.historical_request_limiter.clone();
        let message_sequence = self.message_sequence.clone();
        let stream_health = self.stream_health.clone();
        let bar_watermarks = self.bar_watermarks.clone();
        self.quote_stream_task = Some(get_runtime().spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let futures = groups.into_iter().enumerate().map(
                |(group_index, (stream_ids, instrument_ids))| {
                    let request = MarketDataServerSideStreamRequest {
                        subscribe_order_book_request: Some(SubscribeOrderBookRequest {
                            subscription_action: SubscriptionAction::Subscribe as i32,
                            instruments: stream_ids
                                .into_iter()
                                .map(|instrument_id| OrderBookInstrument {
                                    instrument_id,
                                    depth: 1,
                                    order_book_type: OrderBookType::OrderbookTypeExchange as i32,
                                    ..OrderBookInstrument::default()
                                })
                                .collect(),
                        }),
                        ..MarketDataServerSideStreamRequest::default()
                    };
                    run_market_data_stream(MarketDataStreamContext {
                        market_data_stream: market_data_stream.clone(),
                        market_data_client: market_data_client.clone(),
                        sender: sender.clone(),
                        request,
                        kind: TbankStreamKind::Quotes { instrument_ids },
                        config: config.clone(),
                        historical_request_limiter: historical_request_limiter.clone(),
                        instrument_metadata: instrument_metadata.clone(),
                        task_key: format!(
                            "quotes:generation:{generation}:group:{group_index}:depth1"
                        ),
                        bar_watermarks: bar_watermarks.clone(),
                        bar_continuity_key_overrides: HashMap::new(),
                        reconnect_attempt: Arc::new(AtomicU32::new(0)),
                        message_sequence: message_sequence.clone(),
                        stream_health: stream_health.clone(),
                    })
                },
            );
            join_all(futures).await;
        }));
        Ok(())
    }

    fn schedule_trade_stream(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let instrument_uid = self.stream_id(instrument_id);
        let request = MarketDataServerSideStreamRequest {
            subscribe_trades_request: Some(SubscribeTradesRequest {
                subscription_action: SubscriptionAction::Subscribe as i32,
                instruments: vec![TradeInstrument {
                    instrument_id: instrument_uid.clone(),
                    ..TradeInstrument::default()
                }],
                trade_source: TradeSourceType::TradeSourceExchange as i32,
                with_open_interest: false,
            }),
            ..MarketDataServerSideStreamRequest::default()
        };
        self.spawn_stream(
            stream_task_key("trades", &instrument_id.to_string(), "all"),
            request,
            TbankStreamKind::Trades {
                instrument_id,
                instrument_uid,
            },
        )
    }

    fn schedule_depth10_stream(
        &mut self,
        instrument_id: InstrumentId,
        depth: i32,
    ) -> anyhow::Result<()> {
        let instrument_uid = self.stream_id(instrument_id);
        let request = MarketDataServerSideStreamRequest {
            subscribe_order_book_request: Some(SubscribeOrderBookRequest {
                subscription_action: SubscriptionAction::Subscribe as i32,
                instruments: vec![OrderBookInstrument {
                    instrument_id: instrument_uid.clone(),
                    depth,
                    order_book_type: OrderBookType::OrderbookTypeExchange as i32,
                    ..OrderBookInstrument::default()
                }],
            }),
            ..MarketDataServerSideStreamRequest::default()
        };
        self.spawn_stream(
            stream_task_key("depth10", &instrument_id.to_string(), "book"),
            request,
            TbankStreamKind::Depth10 {
                instrument_id,
                instrument_uid,
            },
        )
    }

    fn spawn_stream(
        &mut self,
        logical_task_key: String,
        request: MarketDataServerSideStreamRequest,
        kind: TbankStreamKind,
    ) -> anyhow::Result<()> {
        if let Some(previous_task_key) = self.active_stream_task_keys.remove(&logical_task_key) {
            self.stream_health.retire_task_key(
                &previous_task_key,
                "logical market-data stream generation was replaced",
            );
            if let Some(existing) = self.stream_tasks.remove(&previous_task_key) {
                existing.abort();
            }
        }
        let market_data_stream = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data_stream
            .clone();
        let market_data_client = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .market_data
            .clone();
        let generation = self.next_stream_generation(&logical_task_key);
        let task_key = Self::generation_task_key(&logical_task_key, generation);
        // Ownership is established before the task is spawned. A stale supervisor can therefore
        // only observe an old key after a replacement and cannot change the new task's health.
        self.stream_health.register(&task_key);
        let sender = get_data_event_sender();
        let config = self.config.clone();
        let instrument_metadata = self.instrument_metadata.clone();
        let historical_request_limiter = self.historical_request_limiter.clone();
        let message_sequence = self.message_sequence.clone();
        let task = get_runtime().spawn(run_market_data_stream(MarketDataStreamContext {
            market_data_stream,
            market_data_client,
            sender,
            request,
            kind,
            config,
            historical_request_limiter,
            instrument_metadata,
            task_key: task_key.clone(),
            bar_watermarks: self.bar_watermarks.clone(),
            bar_continuity_key_overrides: HashMap::new(),
            reconnect_attempt: Arc::new(AtomicU32::new(0)),
            message_sequence,
            stream_health: self.stream_health.clone(),
        }));
        self.stream_tasks.insert(task_key.clone(), task);
        self.active_stream_task_keys
            .insert(logical_task_key, task_key);
        Ok(())
    }

    fn abort_stream(&mut self, logical_task_key: &str) {
        let Some(task_key) = self.active_stream_task_keys.remove(logical_task_key) else {
            return;
        };
        self.stream_health
            .retire_task_key(&task_key, "market-data stream subscription was removed");
        if let Some(task) = self.stream_tasks.remove(&task_key) {
            task.abort();
        }
    }
}

fn retryable_candle_request_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::ResourceExhausted
            | tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unknown
            | tonic::Code::Internal
    )
}

fn historical_candle_timeout_error(timeout: Duration) -> TbankAdapterError {
    TbankAdapterError::GrpcStatus {
        code: tonic::Code::DeadlineExceeded,
        message: format!(
            "T-Bank GetCandles timed out after {} ms",
            timeout.as_millis()
        ),
    }
}

enum StreamMessageOutcome<T> {
    Message(T),
    Closed,
    Error(tonic::Status),
    IdleTimeout,
}

async fn next_stream_message_with_idle_timeout<T, F>(
    future: F,
    timeout: Duration,
) -> StreamMessageOutcome<T>
where
    F: Future<Output = std::result::Result<Option<T>, tonic::Status>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(Some(message))) => StreamMessageOutcome::Message(message),
        Ok(Ok(None)) => StreamMessageOutcome::Closed,
        Ok(Err(status)) => StreamMessageOutcome::Error(status),
        Err(_) => StreamMessageOutcome::IdleTimeout,
    }
}

fn market_data_stream_idle_timeout_reason(timeout: Duration) -> String {
    format!(
        "no T-Bank market data stream message within {} ms",
        timeout.as_millis()
    )
}

struct AbortableTask {
    task: JoinHandle<()>,
    health_cleanup: Option<(Arc<MarketDataStreamHealth>, String)>,
}

impl AbortableTask {
    fn retire_health_key(&mut self, reason: &str) {
        if let Some((health, task_key)) = self.health_cleanup.take() {
            health.retire_task_key(&task_key, reason);
        }
    }
}

#[derive(Default)]
struct AbortTasksOnDrop(Vec<AbortableTask>);

impl Drop for AbortTasksOnDrop {
    fn drop(&mut self) {
        for mut task in self.0.drain(..) {
            task.retire_health_key("isolated stream task owner exited");
            task.task.abort();
        }
    }
}

impl AbortTasksOnDrop {
    #[cfg(test)]
    fn push(&mut self, task: JoinHandle<()>) {
        self.0.push(AbortableTask {
            task,
            health_cleanup: None,
        });
    }

    fn push_with_health_cleanup(
        &mut self,
        task: JoinHandle<()>,
        health: Arc<MarketDataStreamHealth>,
        task_key: String,
    ) {
        self.0.push(AbortableTask {
            task,
            health_cleanup: Some((health, task_key)),
        });
    }

    async fn wait_for_completion(&mut self) {
        // Keep handles owned by the guard while awaiting so aborting the parent subscription
        // still aborts every isolated child instead of detaching the currently awaited task.
        for task in &mut self.0 {
            let _ = (&mut task.task).await;
        }
        for task in &mut self.0 {
            task.retire_health_key("isolated stream task completed");
        }
        self.0.clear();
    }
}

fn periodic_candle_stream_key(generation: u64, instrument_uid: &str) -> String {
    format!("bars:generation:{generation}:poll:indicative:1m:instrument:{instrument_uid}")
}

fn periodic_candle_poll_from(
    next_from: &mut HashMap<BarType, i128>,
    bar_type: BarType,
    to_ts_event: i128,
) -> i128 {
    *next_from.entry(bar_type).or_insert(to_ts_event)
}

fn publish_recovery_batch_if_current<C>(
    stream_health: &MarketDataStreamHealth,
    task_key: &str,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    bars: &[Bar],
    commit: C,
) -> anyhow::Result<RecoveryPublication>
where
    C: FnOnce(&[Bar]),
{
    let published = stream_health.with_current_task_key_after_publish(
        task_key,
        || {
            for bar in bars {
                sender
                    .send(DataEvent::Data(Data::from(*bar)))
                    .map_err(|error| anyhow::anyhow!("data event receiver dropped: {error}"))?;
            }
            Ok(())
        },
        || commit(bars),
    )?;
    Ok(if published {
        RecoveryPublication::Published
    } else {
        RecoveryPublication::Superseded
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_periodic_candle_poll(
    mut market_data_client: MarketDataClient,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    config: TbankDataClientConfig,
    historical_request_limiter: HistoricalRequestLimiter,
    instrument_metadata: SharedInstrumentMetadata,
    subscriptions: Vec<(String, BarType)>,
    initial_bar_watermarks: HashMap<BarType, i128>,
    bar_watermarks: SharedBarWatermarks,
    continuity_key_overrides: HashMap<String, String>,
    generation: u64,
    stream_health: Arc<MarketDataStreamHealth>,
) {
    let mut next_from = initial_bar_watermarks
        .into_iter()
        .map(|(bar_type, latest)| (bar_type, latest.saturating_add(ONE_MINUTE_NANOS)))
        .collect::<HashMap<_, _>>();

    loop {
        let to_ts_event = latest_closed_minute_bar_ts_event(Utc::now());
        let mut backfill = BackfillCoordinator {
            market_data_client: market_data_client.clone(),
            timestamp_mode,
            request_timeout: config.historical_candle_request_timeout,
            max_retries: config.historical_candle_max_retries,
            retry_base_delay: config.historical_candle_retry_base_delay,
            require_complete_candles: true,
            instrument_metadata: instrument_metadata_snapshot(&instrument_metadata),
            request_limiter: historical_request_limiter.clone(),
        };

        for (instrument_uid, bar_type) in &subscriptions {
            let from_ts_event = periodic_candle_poll_from(&mut next_from, *bar_type, to_ts_event);
            let continuity_key = continuity_key_overrides
                .get(instrument_uid)
                .cloned()
                .unwrap_or_else(|| periodic_candle_stream_key(generation, instrument_uid));
            if !stream_health.is_current_task_key(&continuity_key) {
                return;
            }
            if from_ts_event > to_ts_event {
                stream_health.mark_operational(&continuity_key);
                publish_candle_readiness_if_current(
                    &stream_health,
                    TbankCandleReadinessState::Ready,
                    &continuity_key,
                    instrument_uid,
                    Some(to_ts_event),
                    "periodic candle watermark already covers the latest closed minute".to_string(),
                );
                continue;
            }
            let recovery = backfill
                .recover_range(
                    instrument_uid,
                    *bar_type,
                    from_ts_event,
                    to_ts_event,
                    |bars| {
                        publish_recovery_batch_if_current(
                            &stream_health,
                            &continuity_key,
                            &sender,
                            bars,
                            |bars| {
                                for bar in bars {
                                    record_bar_watermark(
                                        &bar_watermarks,
                                        *bar_type,
                                        i128::from(bar.ts_event.as_u64()),
                                    );
                                }
                                record_bar_watermark(&bar_watermarks, *bar_type, to_ts_event);
                            },
                        )
                    },
                )
                .await;
            if !stream_health.is_current_task_key(&continuity_key) {
                return;
            }
            match recovery {
                Ok(RecoveryRangeResult::Published(published_ts)) => {
                    next_from.insert(*bar_type, to_ts_event.saturating_add(ONE_MINUTE_NANOS));
                    stream_health.mark_operational(&continuity_key);
                    publish_candle_readiness_if_current(
                        &stream_health,
                        TbankCandleReadinessState::Ready,
                        &continuity_key,
                        instrument_uid,
                        Some(to_ts_event),
                        format!(
                            "periodic GetCandles checked through the latest closed minute and published {} bars",
                            published_ts.len()
                        ),
                    );
                    if !published_ts.is_empty() {
                        tracing::debug!(
                            instrument_uid,
                            bars = published_ts.len(),
                            "T-Bank periodic GetCandles published indicative bars"
                        );
                    }
                }
                Ok(RecoveryRangeResult::Superseded) => return,
                Err(error) => {
                    stream_health.mark_non_operational(&continuity_key);
                    let reason = format!("periodic GetCandles failed: {error}");
                    publish_candle_readiness_if_current(
                        &stream_health,
                        TbankCandleReadinessState::Failed,
                        &continuity_key,
                        instrument_uid,
                        None,
                        reason.clone(),
                    );
                    if stream_health.is_current_task_key(&continuity_key) {
                        trace_market_data_stream_event(
                            MarketDataStreamEventInput {
                                stage: "periodic_get_candles_failed",
                                task_key: &continuity_key,
                                stream_kind: "bars_poll",
                                instrument_count: 1,
                                status: None,
                                reason,
                                delay_ms: Some(config.periodic_candle_poll_interval.as_millis()),
                                attempt: 0,
                            },
                            Vec::new(),
                        );
                    }
                }
            }
        }
        market_data_client = backfill.market_data_client;
        tokio::time::sleep(config.periodic_candle_poll_interval).await;
    }
}

#[derive(Clone)]
struct MarketDataStreamContext {
    market_data_stream: MarketDataStreamClient,
    market_data_client: MarketDataClient,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    request: MarketDataServerSideStreamRequest,
    kind: TbankStreamKind,
    config: TbankDataClientConfig,
    historical_request_limiter: HistoricalRequestLimiter,
    instrument_metadata: SharedInstrumentMetadata,
    task_key: String,
    bar_watermarks: SharedBarWatermarks,
    bar_continuity_key_overrides: HashMap<String, String>,
    reconnect_attempt: Arc<AtomicU32>,
    message_sequence: Arc<AtomicU64>,
    stream_health: Arc<MarketDataStreamHealth>,
}

fn run_market_data_stream(
    context: MarketDataStreamContext,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let instrument_count = context.kind.instrument_count();
        trace_market_data_stream_event_if_current(
            &context.stream_health,
            MarketDataStreamEventInput {
                stage: "stream_supervisor_started",
                task_key: &context.task_key,
                stream_kind: context.kind.name(),
                instrument_count,
                status: None,
                reason: "market data stream supervisor started".to_string(),
                delay_ms: None,
                attempt: 0,
            },
        );
        let mut restart_after_worker_exit = false;
        loop {
            let mut workers = JoinSet::new();
            workers.spawn(run_market_data_stream_worker(
                context.clone(),
                restart_after_worker_exit,
            ));
            match workers
                .join_next()
                .await
                .expect("stream worker is registered")
            {
                Ok(Ok(())) => {
                    context.stream_health.mark_reconnecting(&context.task_key);
                    tracing::error!(
                        task_key = context.task_key.as_str(),
                        "T-Bank market data stream worker exited normally"
                    );
                    trace_market_data_stream_event_if_current(
                        &context.stream_health,
                        MarketDataStreamEventInput {
                            stage: "stream_worker_normal_exit",
                            task_key: &context.task_key,
                            stream_kind: context.kind.name(),
                            instrument_count,
                            status: None,
                            reason: "unexpected normal worker completion".to_string(),
                            delay_ms: None,
                            attempt: context.reconnect_attempt.load(Ordering::Relaxed),
                        },
                    );
                    restart_after_worker_exit = true;
                }
                Ok(Err(StreamWorkerExit::RetryBudgetExhausted)) => {
                    context.stream_health.mark_reconnecting(&context.task_key);
                    tokio::time::sleep(crate::grpc::retry::backoff_duration(
                        &context.config.reconnect_policy,
                        context.config.max_market_data_reconnect_attempts,
                    ))
                    .await;
                    context.reconnect_attempt.store(0, Ordering::Release);
                    restart_after_worker_exit = false;
                }
                Ok(Err(StreamWorkerExit::Permanent(reason))) => {
                    context.stream_health.mark_terminal(&context.task_key);
                    trace_market_data_stream_event_if_current(
                        &context.stream_health,
                        MarketDataStreamEventInput {
                            stage: "stream_supervisor_exhausted",
                            task_key: &context.task_key,
                            stream_kind: context.kind.name(),
                            instrument_count,
                            status: None,
                            reason,
                            delay_ms: None,
                            attempt: context.reconnect_attempt.load(Ordering::Relaxed),
                        },
                    );
                    return;
                }
                Err(error) if error.is_panic() => {
                    context.stream_health.mark_reconnecting(&context.task_key);
                    let reason = error.to_string();
                    tracing::error!(
                        task_key = context.task_key.as_str(),
                        %reason,
                        "T-Bank market data stream task panicked"
                    );
                    trace_market_data_stream_event_if_current(
                        &context.stream_health,
                        MarketDataStreamEventInput {
                            stage: "stream_task_panicked",
                            task_key: &context.task_key,
                            stream_kind: context.kind.name(),
                            instrument_count,
                            status: Some("panic".to_string()),
                            reason,
                            delay_ms: None,
                            attempt: context.reconnect_attempt.load(Ordering::Relaxed),
                        },
                    );
                    restart_after_worker_exit = true;
                }
                Err(error) => {
                    context.stream_health.mark_terminal(&context.task_key);
                    tracing::error!(
                        task_key = context.task_key.as_str(),
                        error = %error,
                        "T-Bank market data stream worker failed"
                    );
                    return;
                }
            }
        }
    })
}

#[derive(Debug)]
enum StreamWorkerExit {
    RetryBudgetExhausted,
    Permanent(String),
}

fn permanent_stream_status(status: &tonic::Status) -> Option<StreamWorkerExit> {
    (!retryable_stream_status(status))
        .then(|| StreamWorkerExit::Permanent(format!("non-retryable stream status: {status}")))
}

fn run_market_data_stream_worker(
    context: MarketDataStreamContext,
    restart_after_worker_exit: bool,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), StreamWorkerExit>> + Send>> {
    Box::pin(async move {
        let MarketDataStreamContext {
            mut market_data_stream,
            mut market_data_client,
            sender,
            mut request,
            mut kind,
            config,
            historical_request_limiter,
            instrument_metadata,
            task_key,
            bar_continuity_key_overrides,
            reconnect_attempt,
            message_sequence: message_sequence_counter,
            stream_health,
            bar_watermarks,
        } = context;
        let timestamp_mode = config.candle_timestamp_mode;
        let mut instrument_count = kind.instrument_count();

        if restart_after_worker_exit {
            if !wait_for_market_data_reconnect(MarketDataReconnectContext {
                task_key: &task_key,
                kind: &kind,
                instrument_count,
                config: &config,
                attempt: &reconnect_attempt,
                stream_health: &stream_health,
                reason: "restarting market data stream after worker exit",
                exhausted_stage: "stream_supervisor_reconnect_exhausted",
            })
            .await
            {
                return Err(StreamWorkerExit::RetryBudgetExhausted);
            }
            match reconnect_market_data_clients(&config).await {
                Ok((next_stream, next_data)) => {
                    market_data_stream = next_stream;
                    market_data_client = next_data;
                }
                Err(error) => {
                    trace_market_data_stream_event_if_current(
                        &stream_health,
                        MarketDataStreamEventInput {
                            stage: "stream_supervisor_reconnect_failed",
                            task_key: &task_key,
                            stream_kind: kind.name(),
                            instrument_count,
                            status: None,
                            reason: error.to_string(),
                            delay_ms: None,
                            attempt: reconnect_attempt.load(Ordering::Relaxed),
                        },
                    );
                }
            }
        }
        let mut attempt = reconnect_attempt.load(Ordering::Relaxed);
        let mut isolated_subscription_tasks = AbortTasksOnDrop::default();
        let initial_bar_watermarks = snapshot_bar_watermarks(&bar_watermarks);
        let mut continuity = continuity_from_bar_watermarks(&kind, &initial_bar_watermarks);
        // Never let historical recovery prevent the real-time stream from opening. The broker
        // subscription acknowledgement establishes transport readiness first; recovery then runs
        // under the shared request limiter while the open HTTP/2 stream buffers live messages.
        let mut pending_recovery = Some(RecoveryCause::Startup);
        let mut has_permanent_bar_rejection = false;
        loop {
            let _reconnect_trigger = match market_data_stream
                .market_data_server_side_stream(request.clone())
                .await
            {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut subscription_ready = false;
                    let mut pre_ack_messages = VecDeque::new();
                    loop {
                        match next_stream_message_with_idle_timeout(
                            stream.message(),
                            config.market_data_stream_idle_timeout,
                        )
                        .await
                        {
                            StreamMessageOutcome::Message(response) => {
                                if !stream_health.is_current_task_key(&task_key) {
                                    // The supervisor may have completed a receive concurrently
                                    // with unsubscribe/replacement. Its ownership lease is gone,
                                    // so it must not process the response or spawn retry children.
                                    continue;
                                }
                                let received_at = now_unix_nanos();
                                let message_sequence =
                                    message_sequence_counter.fetch_add(1, Ordering::Relaxed);
                                let subscription_ack =
                                    is_market_data_subscription_ack(&response, &kind);
                                if let Err(rejection) =
                                    validate_market_data_subscription_ack(&response, &kind)
                                {
                                    if !rejection.is_partial()
                                        && rejection.has_mixed_retryability()
                                        && matches!(kind, TbankStreamKind::Bars { .. })
                                    {
                                        has_permanent_bar_rejection |=
                                            mark_permanent_bar_rejections(
                                                &task_key,
                                                &kind,
                                                &rejection,
                                                &bar_continuity_key_overrides,
                                                &stream_health,
                                            );
                                        if let Some(candles_request) =
                                            request.subscribe_candles_request.as_mut()
                                        {
                                            candles_request.instruments.retain(|instrument| {
                                                !rejection.failures.iter().any(|failure| {
                                                    !failure.retryable
                                                        && failure.instrument_uid
                                                            == instrument.instrument_id
                                                })
                                            });
                                        }
                                        if let TbankStreamKind::Bars { bar_types } = &mut kind {
                                            for failure in rejection
                                                .failures
                                                .iter()
                                                .filter(|failure| !failure.retryable)
                                            {
                                                bar_types.remove(&failure.instrument_uid);
                                            }
                                        }
                                        instrument_count = kind.instrument_count();
                                        trace_market_data_stream_event_if_current(
                                            &stream_health,
                                            MarketDataStreamEventInput {
                                                stage: "subscription_rejection_partitioned",
                                                task_key: &task_key,
                                                stream_kind: kind.name(),
                                                instrument_count,
                                                status: None,
                                                reason: rejection.reason.clone(),
                                                delay_ms: None,
                                                attempt,
                                            },
                                        );
                                        break ReconnectTrigger::SubscriptionRejected;
                                    }
                                    if rejection.is_partial()
                                        && matches!(kind, TbankStreamKind::Bars { .. })
                                    {
                                        has_permanent_bar_rejection |=
                                            mark_permanent_bar_rejections(
                                                &task_key,
                                                &kind,
                                                &rejection,
                                                &bar_continuity_key_overrides,
                                                &stream_health,
                                            );
                                        let reason = rejection.reason.clone();
                                        tracing::warn!(
                                            task_key = task_key.as_str(),
                                            %reason,
                                            "T-Bank partially rejected a batched candle subscription; preserving accepted instruments"
                                        );
                                        trace_market_data_stream_event_if_current(
                                            &stream_health,
                                            MarketDataStreamEventInput {
                                                stage: "subscription_partially_rejected",
                                                task_key: &task_key,
                                                stream_kind: kind.name(),
                                                instrument_count,
                                                status: None,
                                                reason: reason.clone(),
                                                delay_ms: None,
                                                attempt,
                                            },
                                        );

                                        let failed_bar_types = match &kind {
                                            TbankStreamKind::Bars { bar_types } => rejection
                                                .failures
                                                .iter()
                                                .filter_map(|failure| {
                                                    bar_types
                                                        .get(&failure.instrument_uid)
                                                        .copied()
                                                        .map(|bar_type| (failure.clone(), bar_type))
                                                })
                                                .collect::<Vec<_>>(),
                                            _ => Vec::new(),
                                        };
                                        if let Some(candles_request) =
                                            request.subscribe_candles_request.as_mut()
                                        {
                                            candles_request.instruments.retain(|instrument| {
                                                !rejection.failures.iter().any(|failure| {
                                                    failure.instrument_uid
                                                        == instrument.instrument_id
                                                })
                                            });
                                        }
                                        if let TbankStreamKind::Bars { bar_types } = &mut kind {
                                            for failure in &rejection.failures {
                                                bar_types.remove(&failure.instrument_uid);
                                            }
                                        }
                                        instrument_count = kind.instrument_count();

                                        for (failure, bar_type) in failed_bar_types {
                                            if !failure.retryable {
                                                continue;
                                            }

                                            let (
                                                isolated_task_key,
                                                isolated_request,
                                                isolated_kind,
                                                isolated_continuity_keys,
                                            ) = isolated_retryable_bar_stream(
                                                &task_key,
                                                &failure.instrument_uid,
                                                bar_type,
                                            );
                                            let isolated_stream_kind = isolated_kind.name();
                                            let Some(isolated_task) = stream_health
                                                .spawn_child_if_current(
                                                    &task_key,
                                                    &isolated_task_key,
                                                    || {
                                                        trace_market_data_stream_event(
                                                            MarketDataStreamEventInput {
                                                                stage: "subscription_retry_isolated",
                                                                task_key: &isolated_task_key,
                                                                stream_kind: isolated_stream_kind,
                                                                instrument_count: 1,
                                                                status: None,
                                                                reason: failure.reason,
                                                                delay_ms: None,
                                                                attempt,
                                                            },
                                                            Vec::new(),
                                                        );
                                                        get_runtime().spawn(run_market_data_stream(
                                                            MarketDataStreamContext {
                                                                market_data_stream:
                                                                    market_data_stream.clone(),
                                                                market_data_client:
                                                                    market_data_client.clone(),
                                                                sender: sender.clone(),
                                                                request: isolated_request,
                                                                kind: isolated_kind,
                                                                config: config.clone(),
                                                                historical_request_limiter:
                                                                    historical_request_limiter.clone(),
                                                                instrument_metadata:
                                                                    instrument_metadata.clone(),
                                                                task_key: isolated_task_key.clone(),
                                                                bar_watermarks: bar_watermarks.clone(),
                                                                bar_continuity_key_overrides:
                                                                    isolated_continuity_keys,
                                                                reconnect_attempt:
                                                                    Arc::new(AtomicU32::new(0)),
                                                                message_sequence:
                                                                    message_sequence_counter.clone(),
                                                                stream_health: stream_health.clone(),
                                                            },
                                                        ))
                                                    },
                                                )
                                            else {
                                                continue;
                                            };
                                            isolated_subscription_tasks.push_with_health_cleanup(
                                                isolated_task,
                                                stream_health.clone(),
                                                isolated_task_key,
                                            );
                                        }
                                        if instrument_count == 0 {
                                            stream_health.mark_terminal(&task_key);
                                            isolated_subscription_tasks.wait_for_completion().await;
                                            return Err(StreamWorkerExit::Permanent(
                                                "all instruments in the stream group were rejected"
                                                    .to_string(),
                                            ));
                                        }
                                        let recovery_ready = recover_pending_bars_after_stream_ack(
                                            &mut pending_recovery,
                                            &task_key,
                                            &kind,
                                            &mut market_data_client,
                                            &bar_watermarks,
                                            &bar_continuity_key_overrides,
                                            timestamp_mode,
                                            &sender,
                                            &mut continuity,
                                            instrument_count,
                                            &config,
                                            &historical_request_limiter,
                                            &instrument_metadata,
                                            &stream_health,
                                            attempt,
                                            !has_permanent_bar_rejection,
                                        )
                                        .await;
                                        if !recovery_ready {
                                            stream_health.mark_reconnecting(&task_key);
                                            break ReconnectTrigger::RecoveryFailed;
                                        }
                                        if !has_permanent_bar_rejection {
                                            stream_health.mark_operational(&task_key);
                                        }
                                        subscription_ready = true;
                                        drain_pre_ack_messages(
                                            &mut pre_ack_messages,
                                            &sender,
                                            &kind,
                                            &instrument_metadata,
                                            &bar_watermarks,
                                            timestamp_mode,
                                            &task_key,
                                            &stream_health,
                                            &bar_continuity_key_overrides,
                                            &mut continuity,
                                            &mut attempt,
                                        );
                                        reconnect_attempt.store(attempt, Ordering::Relaxed);
                                        continue;
                                    }
                                    let reason = rejection.reason.clone();
                                    tracing::error!(
                                        task_key = task_key.as_str(),
                                        %reason,
                                        "T-Bank rejected market data subscription"
                                    );
                                    trace_market_data_stream_event_if_current(
                                        &stream_health,
                                        MarketDataStreamEventInput {
                                            stage: "subscription_rejected",
                                            task_key: &task_key,
                                            stream_kind: kind.name(),
                                            instrument_count,
                                            status: None,
                                            reason: reason.clone(),
                                            delay_ms: None,
                                            attempt,
                                        },
                                    );
                                    if !rejection.retryable {
                                        mark_permanent_bar_rejections(
                                            &task_key,
                                            &kind,
                                            &rejection,
                                            &bar_continuity_key_overrides,
                                            &stream_health,
                                        );
                                        tracing::error!(
                                            task_key = task_key.as_str(),
                                            %reason,
                                            "T-Bank market data subscription was permanently rejected"
                                        );
                                        trace_market_data_stream_event_if_current(
                                            &stream_health,
                                            MarketDataStreamEventInput {
                                                stage: "subscription_permanently_rejected",
                                                task_key: &task_key,
                                                stream_kind: kind.name(),
                                                instrument_count,
                                                status: None,
                                                reason: reason.clone(),
                                                delay_ms: None,
                                                attempt,
                                            },
                                        );
                                        isolated_subscription_tasks.wait_for_completion().await;
                                        stream_health.mark_terminal(&task_key);
                                        return Err(StreamWorkerExit::Permanent(reason));
                                    }
                                    break ReconnectTrigger::SubscriptionRejected;
                                }
                                if subscription_ack {
                                    let recovery_ready = recover_pending_bars_after_stream_ack(
                                        &mut pending_recovery,
                                        &task_key,
                                        &kind,
                                        &mut market_data_client,
                                        &bar_watermarks,
                                        &bar_continuity_key_overrides,
                                        timestamp_mode,
                                        &sender,
                                        &mut continuity,
                                        instrument_count,
                                        &config,
                                        &historical_request_limiter,
                                        &instrument_metadata,
                                        &stream_health,
                                        attempt,
                                        !has_permanent_bar_rejection,
                                    )
                                    .await;
                                    if !recovery_ready {
                                        stream_health.mark_reconnecting(&task_key);
                                        break ReconnectTrigger::RecoveryFailed;
                                    }
                                    if !has_permanent_bar_rejection {
                                        stream_health.mark_operational(&task_key);
                                    }
                                    subscription_ready = true;
                                    drain_pre_ack_messages(
                                        &mut pre_ack_messages,
                                        &sender,
                                        &kind,
                                        &instrument_metadata,
                                        &bar_watermarks,
                                        timestamp_mode,
                                        &task_key,
                                        &stream_health,
                                        &bar_continuity_key_overrides,
                                        &mut continuity,
                                        &mut attempt,
                                    );
                                    reconnect_attempt.store(attempt, Ordering::Relaxed);
                                }
                                if !subscription_ready {
                                    if is_usable_market_data_response(&response, &kind) {
                                        if pre_ack_messages.len() >= MAX_PRE_ACK_MESSAGES {
                                            let reason = format!(
                                                "pre-ack market data buffer reached {MAX_PRE_ACK_MESSAGES} messages"
                                            );
                                            tracing::warn!(
                                                task_key = task_key.as_str(),
                                                buffered = pre_ack_messages.len(),
                                                "T-Bank pre-ack market data buffer is full"
                                            );
                                            trace_market_data_stream_event_if_current(
                                                &stream_health,
                                                MarketDataStreamEventInput {
                                                    stage: "pre_ack_buffer_overflow",
                                                    task_key: &task_key,
                                                    stream_kind: kind.name(),
                                                    instrument_count,
                                                    status: None,
                                                    reason,
                                                    delay_ms: None,
                                                    attempt,
                                                },
                                            );
                                            break ReconnectTrigger::PreAckBufferOverflow;
                                        }
                                        if pre_ack_messages.is_empty() {
                                            trace_market_data_stream_event_if_current(
                                                &stream_health,
                                                MarketDataStreamEventInput {
                                                    stage: "message_buffered_before_subscription_ack",
                                                    task_key: &task_key,
                                                    stream_kind: kind.name(),
                                                    instrument_count,
                                                    status: None,
                                                    reason: "buffering market data until broker subscription acknowledgement"
                                                        .to_string(),
                                                    delay_ms: None,
                                                    attempt,
                                                },
                                            );
                                        }
                                        pre_ack_messages.push_back((
                                            response,
                                            received_at,
                                            message_sequence,
                                        ));
                                    }
                                    continue;
                                }
                                publish_ready_market_data_response(
                                    &sender,
                                    response,
                                    &kind,
                                    &instrument_metadata,
                                    &bar_watermarks,
                                    timestamp_mode,
                                    received_at,
                                    &task_key,
                                    &stream_health,
                                    &bar_continuity_key_overrides,
                                    &mut continuity,
                                    &mut attempt,
                                    message_sequence,
                                );
                                reconnect_attempt.store(attempt, Ordering::Relaxed);
                            }
                            StreamMessageOutcome::Closed => {
                                stream_health.mark_reconnecting(&task_key);
                                tracing::warn!(
                                    task_key = task_key.as_str(),
                                    "T-Bank market data stream closed by server"
                                );
                                trace_market_data_stream_event_if_current(
                                    &stream_health,
                                    MarketDataStreamEventInput {
                                        stage: "stream_closed_by_server",
                                        task_key: &task_key,
                                        stream_kind: kind.name(),
                                        instrument_count,
                                        status: None,
                                        reason: "server closed stream".to_string(),
                                        delay_ms: None,
                                        attempt,
                                    },
                                );
                                break ReconnectTrigger::StreamClosed;
                            }
                            StreamMessageOutcome::Error(error) => {
                                stream_health.mark_reconnecting(&task_key);
                                tracing::warn!(
                                    %error,
                                    task_key = task_key.as_str(),
                                    "T-Bank market data stream closed with error"
                                );
                                trace_market_data_stream_event_if_current(
                                    &stream_health,
                                    MarketDataStreamEventInput {
                                        stage: "stream_closed_error",
                                        task_key: &task_key,
                                        stream_kind: kind.name(),
                                        instrument_count,
                                        status: Some(format!("{:?}", error.code())),
                                        reason: error.to_string(),
                                        delay_ms: None,
                                        attempt,
                                    },
                                );
                                if let Some(exit) = permanent_stream_status(&error) {
                                    return Err(exit);
                                }
                                match reconnect_market_data_clients(&config).await {
                                    Ok((next_stream, next_data)) => {
                                        market_data_stream = next_stream;
                                        market_data_client = next_data;
                                    }
                                    Err(reconnect_error) => tracing::warn!(
                                        %reconnect_error,
                                        task_key = task_key.as_str(),
                                        "failed to recreate T-Bank market data clients"
                                    ),
                                }
                                break ReconnectTrigger::StreamError;
                            }
                            StreamMessageOutcome::IdleTimeout => {
                                stream_health.mark_reconnecting(&task_key);
                                let reason = market_data_stream_idle_timeout_reason(
                                    config.market_data_stream_idle_timeout,
                                );
                                tracing::warn!(
                                    task_key = task_key.as_str(),
                                    timeout_ms = config.market_data_stream_idle_timeout.as_millis(),
                                    "T-Bank market data stream idle timeout"
                                );
                                trace_market_data_stream_event_if_current(
                                    &stream_health,
                                    MarketDataStreamEventInput {
                                        stage: "stream_idle_timeout",
                                        task_key: &task_key,
                                        stream_kind: kind.name(),
                                        instrument_count,
                                        status: None,
                                        reason: reason.clone(),
                                        delay_ms: None,
                                        attempt,
                                    },
                                );
                                let trigger = match reconnect_market_data_clients(&config).await {
                                    Ok((next_stream, next_data)) => {
                                        market_data_stream = next_stream;
                                        market_data_client = next_data;
                                        ReconnectTrigger::IdleTimeout
                                    }
                                    Err(reconnect_error) => {
                                        tracing::warn!(
                                            %reconnect_error,
                                            task_key = task_key.as_str(),
                                            "failed to recreate T-Bank market data clients after idle timeout"
                                        );
                                        ReconnectTrigger::IdleTimeoutReconnectFailed
                                    }
                                };
                                break trigger;
                            }
                        }
                    }
                }
                Err(error) => {
                    stream_health.mark_reconnecting(&task_key);
                    tracing::warn!(
                        %error,
                        task_key = task_key.as_str(),
                        "failed to open T-Bank market data stream"
                    );
                    trace_market_data_stream_event_if_current(
                        &stream_health,
                        MarketDataStreamEventInput {
                            stage: "open_failed",
                            task_key: &task_key,
                            stream_kind: kind.name(),
                            instrument_count,
                            status: Some(format!("{:?}", error.code())),
                            reason: error.to_string(),
                            delay_ms: None,
                            attempt,
                        },
                    );
                    if let Some(exit) = permanent_stream_status(&error) {
                        return Err(exit);
                    }
                    match reconnect_market_data_clients(&config).await {
                        Ok((next_stream, next_data)) => {
                            market_data_stream = next_stream;
                            market_data_client = next_data;
                        }
                        Err(reconnect_error) => tracing::warn!(
                            %reconnect_error,
                            task_key = task_key.as_str(),
                            "failed to recreate T-Bank market data clients after open failure"
                        ),
                    }
                    ReconnectTrigger::OpenFailed
                }
            };
            // Every new broker acknowledgement must cross the continuity barrier. The previous
            // interval gate allowed a fast disconnect/reconnect to publish live bars without
            // checking the gap; the shared recovery coordinator already bounds duplicate work.
            stream_health.mark_reconnecting(&task_key);
            pending_recovery = Some(RecoveryCause::Reconnect);
            reconnect_attempt.store(attempt, Ordering::Relaxed);
            if !wait_for_market_data_reconnect(MarketDataReconnectContext {
                task_key: &task_key,
                kind: &kind,
                instrument_count,
                config: &config,
                attempt: &reconnect_attempt,
                stream_health: &stream_health,
                reason: "reopening T-Bank market data stream after disconnect",
                exhausted_stage: "stream_reconnect_exhausted",
            })
            .await
            {
                // Retryable instruments removed from `request` and `kind` are owned by the
                // isolated supervisors. Reconnect-budget exhaustion is recoverable for the
                // parent, so keep those supervisors alive while this owner re-arms its probe.
                // `AbortTasksOnDrop` still cancels them when the owner is actually torn down.
                let catch_up = reconnect_catch_up_bars(
                    RecoveryCause::Reconnect,
                    &task_key,
                    &kind,
                    &mut market_data_client,
                    &bar_watermarks,
                    &bar_continuity_key_overrides,
                    timestamp_mode,
                    &sender,
                    &mut continuity,
                    instrument_count,
                    &config,
                    &historical_request_limiter,
                    &instrument_metadata,
                    &stream_health,
                    false,
                )
                .await;
                tracing::warn!(
                    task_key = task_key.as_str(),
                    checked = catch_up.checked,
                    backfilled = catch_up.backfilled,
                    failed = catch_up.failed,
                    "T-Bank reconnect budget exhausted; bounded historical recovery completed before probe"
                );
                tokio::time::sleep(crate::grpc::retry::backoff_duration(
                    &config.reconnect_policy,
                    config.max_market_data_reconnect_attempts,
                ))
                .await;
                if let Ok((next_stream, next_data)) = reconnect_market_data_clients(&config).await {
                    market_data_stream = next_stream;
                    market_data_client = next_data;
                }
                reconnect_attempt.store(0, Ordering::Release);
                attempt = 0;
                pending_recovery = Some(RecoveryCause::Reconnect);
                continue;
            }
            attempt = reconnect_attempt.load(Ordering::Relaxed);
        }
    })
}

struct MarketDataReconnectContext<'a> {
    task_key: &'a str,
    kind: &'a TbankStreamKind,
    instrument_count: usize,
    config: &'a TbankDataClientConfig,
    attempt: &'a AtomicU32,
    stream_health: &'a MarketDataStreamHealth,
    reason: &'a str,
    exhausted_stage: &'a str,
}

async fn wait_for_market_data_reconnect(context: MarketDataReconnectContext<'_>) -> bool {
    let MarketDataReconnectContext {
        task_key,
        kind,
        instrument_count,
        config,
        attempt,
        stream_health,
        reason,
        exhausted_stage,
    } = context;
    let mut next_attempt = attempt.load(Ordering::Relaxed);
    let Some(schedule) = plan_next_market_data_reconnect(
        &config.reconnect_policy,
        config.max_market_data_reconnect_attempts,
        &mut next_attempt,
    ) else {
        attempt.store(next_attempt, Ordering::Relaxed);
        tracing::error!(
            task_key,
            attempt = next_attempt,
            "T-Bank market data reconnect retry budget exhausted"
        );
        trace_market_data_stream_event_if_current(
            stream_health,
            MarketDataStreamEventInput {
                stage: exhausted_stage,
                task_key,
                stream_kind: kind.name(),
                instrument_count,
                status: None,
                reason: "market data reconnect retry budget exhausted".to_string(),
                delay_ms: None,
                attempt: next_attempt,
            },
        );
        return false;
    };
    attempt.store(next_attempt, Ordering::Relaxed);
    trace_market_data_stream_event_if_current(
        stream_health,
        MarketDataStreamEventInput {
            stage: "reconnect_scheduled",
            task_key,
            stream_kind: kind.name(),
            instrument_count,
            status: None,
            reason: reason.to_string(),
            delay_ms: Some(schedule.delay.as_millis()),
            attempt: schedule.attempt,
        },
    );
    tracing::warn!(
        task_key,
        delay_ms = schedule.delay.as_millis(),
        attempt = schedule.attempt,
        %reason,
        "reopening T-Bank market data stream after disconnect"
    );
    tokio::time::sleep(schedule.delay).await;
    true
}

fn reset_reconnect_attempt_if_usable(usable: bool, attempt: &mut u32) -> bool {
    if !usable {
        return false;
    }
    *attempt = 0;
    true
}

fn is_usable_market_data_response(response: &MarketDataResponse, kind: &TbankStreamKind) -> bool {
    match (response.payload.as_ref(), kind) {
        (
            Some(market_data_response::Payload::Candle(candle)),
            TbankStreamKind::Bars { bar_types },
        ) => bar_types.contains_key(&candle.instrument_uid),
        (
            Some(market_data_response::Payload::Trade(trade)),
            TbankStreamKind::Trades { instrument_uid, .. },
        ) => trade.instrument_uid == *instrument_uid,
        (
            Some(market_data_response::Payload::Orderbook(orderbook)),
            TbankStreamKind::Quotes { instrument_ids },
        ) => instrument_ids.contains_key(&orderbook.instrument_uid),
        (
            Some(market_data_response::Payload::Orderbook(orderbook)),
            TbankStreamKind::Depth10 { instrument_uid, .. },
        ) => orderbook.instrument_uid == *instrument_uid,
        _ => false,
    }
}

fn is_market_data_subscription_ack(response: &MarketDataResponse, kind: &TbankStreamKind) -> bool {
    matches!(
        (response.payload.as_ref(), kind),
        (
            Some(market_data_response::Payload::SubscribeCandlesResponse(_)),
            TbankStreamKind::Bars { .. }
        ) | (
            Some(market_data_response::Payload::SubscribeTradesResponse(_)),
            TbankStreamKind::Trades { .. }
        ) | (
            Some(market_data_response::Payload::SubscribeOrderBookResponse(_)),
            TbankStreamKind::Quotes { .. } | TbankStreamKind::Depth10 { .. }
        )
    )
}

#[allow(clippy::too_many_arguments)]
async fn recover_pending_bars_after_stream_ack(
    pending_recovery: &mut Option<RecoveryCause>,
    task_key: &str,
    kind: &TbankStreamKind,
    market_data_client: &mut MarketDataClient,
    bar_watermarks: &SharedBarWatermarks,
    bar_continuity_key_overrides: &HashMap<String, String>,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    continuity: &mut HashMap<String, BarContinuityTracker>,
    instrument_count: usize,
    config: &TbankDataClientConfig,
    historical_request_limiter: &HistoricalRequestLimiter,
    instrument_metadata: &SharedInstrumentMetadata,
    stream_health: &MarketDataStreamHealth,
    attempt: u32,
    publish_stream_ready: bool,
) -> bool {
    if !stream_health.is_current_task_key(task_key) {
        return true;
    }
    trace_market_data_stream_event_if_current(
        stream_health,
        MarketDataStreamEventInput {
            stage: "stream_subscription_acked",
            task_key,
            stream_kind: kind.name(),
            instrument_count,
            status: None,
            reason: "broker acknowledged the active market data subscription".to_string(),
            delay_ms: None,
            attempt,
        },
    );

    let Some(cause) = pending_recovery.take() else {
        if publish_stream_ready {
            trace_market_data_stream_event_if_current(
                stream_health,
                MarketDataStreamEventInput {
                    stage: "stream_ready",
                    task_key,
                    stream_kind: kind.name(),
                    instrument_count,
                    status: None,
                    reason: "broker acknowledgement did not require historical recovery"
                        .to_string(),
                    delay_ms: None,
                    attempt,
                },
            );
        }
        return true;
    };
    let catch_up = reconnect_catch_up_bars(
        cause,
        task_key,
        kind,
        market_data_client,
        bar_watermarks,
        bar_continuity_key_overrides,
        timestamp_mode,
        sender,
        continuity,
        instrument_count,
        config,
        historical_request_limiter,
        instrument_metadata,
        stream_health,
        true,
    )
    .await;
    if !stream_health.is_current_task_key(task_key) {
        return true;
    }
    if catch_up.backfilled > 0 {
        tracing::info!(
            task_key,
            recovery_cause = cause.as_str(),
            backfilled = catch_up.backfilled,
            "T-Bank GetCandles caught up bars after stream acknowledgement"
        );
    }
    if catch_up.failed > 0 {
        trace_market_data_stream_event_if_current(
            stream_health,
            MarketDataStreamEventInput {
                stage: "stream_recovery_failed",
                task_key,
                stream_kind: kind.name(),
                instrument_count,
                status: None,
                reason: format!("{} instruments failed historical recovery", catch_up.failed),
                delay_ms: None,
                attempt,
            },
        );
        tracing::warn!(
            task_key,
            recovery_cause = cause.as_str(),
            failed = catch_up.failed,
            "T-Bank stream remains active while incomplete candle recovery waits for a future reconnect"
        );
        return false;
    }
    if publish_stream_ready {
        trace_market_data_stream_event_if_current(
            stream_health,
            MarketDataStreamEventInput {
                stage: "stream_ready",
                task_key,
                stream_kind: kind.name(),
                instrument_count,
                status: None,
                reason: "subscription gap recovery completed after broker acknowledgement"
                    .to_string(),
                delay_ms: None,
                attempt,
            },
        );
    }
    true
}

fn bar_continuity_key(group: &str, instrument_uid: &str) -> String {
    format!("{group}:instrument:{instrument_uid}")
}

fn bar_continuity_key_with_overrides(
    group: &str,
    instrument_uid: &str,
    overrides: &HashMap<String, String>,
) -> String {
    overrides
        .get(instrument_uid)
        .cloned()
        .unwrap_or_else(|| bar_continuity_key(group, instrument_uid))
}

fn mark_permanent_bar_rejections(
    group: &str,
    kind: &TbankStreamKind,
    rejection: &SubscriptionAckRejection,
    bar_continuity_key_overrides: &HashMap<String, String>,
    stream_health: &MarketDataStreamHealth,
) -> bool {
    let TbankStreamKind::Bars { bar_types } = kind else {
        return false;
    };

    let mut found = false;
    for failure in rejection
        .failures
        .iter()
        .filter(|failure| !failure.retryable)
    {
        if !bar_types.contains_key(&failure.instrument_uid) {
            continue;
        }
        found = true;
        stream_health.mark_non_operational(group);
        let continuity_key = bar_continuity_key_with_overrides(
            group,
            &failure.instrument_uid,
            bar_continuity_key_overrides,
        );
        publish_candle_readiness_if_current(
            stream_health,
            TbankCandleReadinessState::Failed,
            &continuity_key,
            &failure.instrument_uid,
            None,
            format!(
                "T-Bank candle subscription permanently rejected: {}",
                failure.reason
            ),
        );
    }
    found
}

fn latest_closed_minute_bar_ts_event(now: chrono::DateTime<Utc>) -> i128 {
    let nanos =
        i128::from(now.timestamp()) * 1_000_000_000 + i128::from(now.timestamp_subsec_nanos());
    nanos - nanos.rem_euclid(ONE_MINUTE_NANOS)
}

#[derive(Debug, Default, Clone, Copy)]
struct ReconnectCatchUpOutcome {
    checked: usize,
    backfilled: usize,
    failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconnectTrigger {
    OpenFailed,
    SubscriptionRejected,
    StreamClosed,
    StreamError,
    IdleTimeout,
    IdleTimeoutReconnectFailed,
    PreAckBufferOverflow,
    RecoveryFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReconnectSchedule {
    attempt: u32,
    delay: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionAckRejection {
    reason: String,
    retryable: bool,
    acknowledged_count: usize,
    failures: Vec<SubscriptionFailure>,
}

impl SubscriptionAckRejection {
    fn is_partial(&self) -> bool {
        self.acknowledged_count > self.failures.len()
    }

    fn has_mixed_retryability(&self) -> bool {
        self.failures.iter().any(|failure| failure.retryable)
            && self.failures.iter().any(|failure| !failure.retryable)
    }
}

fn validate_market_data_subscription_ack(
    response: &MarketDataResponse,
    kind: &TbankStreamKind,
) -> std::result::Result<(), SubscriptionAckRejection> {
    let (failures, acknowledged_count) = match (response.payload.as_ref(), kind) {
        (
            Some(market_data_response::Payload::SubscribeCandlesResponse(response)),
            TbankStreamKind::Bars { bar_types },
        ) => (
            response
                .candles_subscriptions
                .iter()
                .filter(|subscription| bar_types.contains_key(&subscription.instrument_uid))
                .filter_map(|subscription| {
                    subscription_failure(
                        subscription.instrument_uid.as_str(),
                        subscription.subscription_status,
                    )
                })
                .collect::<Vec<_>>(),
            response
                .candles_subscriptions
                .iter()
                .filter(|subscription| bar_types.contains_key(&subscription.instrument_uid))
                .count(),
        ),
        (
            Some(market_data_response::Payload::SubscribeTradesResponse(response)),
            TbankStreamKind::Trades { instrument_uid, .. },
        ) => (
            response
                .trade_subscriptions
                .iter()
                .filter(|subscription| subscription.instrument_uid == *instrument_uid)
                .filter_map(|subscription| {
                    subscription_failure(
                        subscription.instrument_uid.as_str(),
                        subscription.subscription_status,
                    )
                })
                .collect::<Vec<_>>(),
            response
                .trade_subscriptions
                .iter()
                .filter(|subscription| subscription.instrument_uid == *instrument_uid)
                .count(),
        ),
        (
            Some(market_data_response::Payload::SubscribeOrderBookResponse(response)),
            TbankStreamKind::Quotes { instrument_ids },
        ) => (
            response
                .order_book_subscriptions
                .iter()
                .filter(|subscription| instrument_ids.contains_key(&subscription.instrument_uid))
                .filter_map(|subscription| {
                    subscription_failure(
                        subscription.instrument_uid.as_str(),
                        subscription.subscription_status,
                    )
                })
                .collect::<Vec<_>>(),
            response
                .order_book_subscriptions
                .iter()
                .filter(|subscription| instrument_ids.contains_key(&subscription.instrument_uid))
                .count(),
        ),
        (
            Some(market_data_response::Payload::SubscribeOrderBookResponse(response)),
            TbankStreamKind::Depth10 { instrument_uid, .. },
        ) => (
            response
                .order_book_subscriptions
                .iter()
                .filter(|subscription| subscription.instrument_uid == *instrument_uid)
                .filter_map(|subscription| {
                    subscription_failure(
                        subscription.instrument_uid.as_str(),
                        subscription.subscription_status,
                    )
                })
                .collect::<Vec<_>>(),
            response
                .order_book_subscriptions
                .iter()
                .filter(|subscription| subscription.instrument_uid == *instrument_uid)
                .count(),
        ),
        _ => return Ok(()),
    };

    if failures.is_empty() {
        Ok(())
    } else {
        Err(SubscriptionAckRejection {
            reason: failures
                .iter()
                .map(|failure| failure.reason.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            retryable: failures.iter().all(|failure| failure.retryable),
            acknowledged_count,
            failures,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionFailure {
    instrument_uid: String,
    reason: String,
    retryable: bool,
}

fn isolated_retryable_bar_stream(
    parent_task_key: &str,
    instrument_uid: &str,
    bar_type: nautilus_model::data::BarType,
) -> (
    String,
    MarketDataServerSideStreamRequest,
    TbankStreamKind,
    HashMap<String, String>,
) {
    let task_key = format!("{parent_task_key}:retry:{instrument_uid}");
    let request = MarketDataServerSideStreamRequest {
        subscribe_candles_request: Some(SubscribeCandlesRequest {
            subscription_action: SubscriptionAction::Subscribe as i32,
            instruments: vec![CandleInstrument {
                instrument_id: instrument_uid.to_string(),
                interval: SubscriptionInterval::OneMinute as i32,
                ..CandleInstrument::default()
            }],
            waiting_close: true,
            candle_source_type: Some(get_candles_request::CandleSource::Exchange as i32),
        }),
        ..MarketDataServerSideStreamRequest::default()
    };
    let kind = TbankStreamKind::Bars {
        bar_types: HashMap::from([(instrument_uid.to_string(), bar_type)]),
    };
    let continuity_keys = HashMap::from([(
        instrument_uid.to_string(),
        bar_continuity_key(parent_task_key, instrument_uid),
    )]);
    (task_key, request, kind, continuity_keys)
}

fn subscription_failure(instrument_uid: &str, status: i32) -> Option<SubscriptionFailure> {
    let status = SubscriptionStatus::try_from(status).ok();
    if status == Some(SubscriptionStatus::Success) {
        return None;
    }
    let retryable = matches!(
        status,
        None | Some(SubscriptionStatus::Unspecified)
            | Some(SubscriptionStatus::InternalError)
            | Some(SubscriptionStatus::LimitIsExceeded)
            | Some(SubscriptionStatus::TooManyRequests)
    );
    Some(SubscriptionFailure {
        instrument_uid: instrument_uid.to_string(),
        reason: format!(
            "instrument_uid={instrument_uid} status={}",
            status
                .map(|status| status.as_str_name())
                .unwrap_or("SUBSCRIPTION_STATUS_UNKNOWN")
        ),
        retryable,
    })
}

fn plan_next_market_data_reconnect(
    reconnect_policy: &crate::config::TbankReconnectPolicy,
    max_attempts: u32,
    attempt: &mut u32,
) -> Option<ReconnectSchedule> {
    let scheduled_attempt = *attempt;
    *attempt = attempt.saturating_add(1);
    if *attempt >= max_attempts {
        return None;
    }
    Some(ReconnectSchedule {
        attempt: scheduled_attempt,
        delay: crate::grpc::retry::backoff_duration(reconnect_policy, scheduled_attempt),
    })
}

#[allow(clippy::too_many_arguments)]
async fn reconnect_catch_up_bars(
    cause: RecoveryCause,
    group: &str,
    kind: &TbankStreamKind,
    market_data_client: &mut MarketDataClient,
    bar_watermarks: &SharedBarWatermarks,
    bar_continuity_key_overrides: &HashMap<String, String>,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    continuity: &mut HashMap<String, BarContinuityTracker>,
    instrument_count: usize,
    config: &TbankDataClientConfig,
    request_limiter: &HistoricalRequestLimiter,
    instrument_metadata: &SharedInstrumentMetadata,
    stream_health: &MarketDataStreamHealth,
    publish_ready: bool,
) -> ReconnectCatchUpOutcome {
    if !stream_health.is_current_task_key(group) {
        return ReconnectCatchUpOutcome::default();
    }

    let TbankStreamKind::Bars { bar_types } = kind else {
        return ReconnectCatchUpOutcome::default();
    };

    let to_ts_event = latest_closed_minute_bar_ts_event(Utc::now());
    let mut already_current = Vec::new();
    let mut jobs = Vec::new();
    for (instrument_uid, bar_type) in bar_types {
        let Some(latest_seen) = continuity
            .get(instrument_uid)
            .and_then(BarContinuityTracker::latest_seen)
        else {
            continue;
        };
        let continuity_key =
            bar_continuity_key_with_overrides(group, instrument_uid, bar_continuity_key_overrides);
        let from_ts_event = latest_seen.saturating_add(ONE_MINUTE_NANOS);
        if from_ts_event <= to_ts_event {
            jobs.push((
                instrument_uid.clone(),
                continuity_key,
                *bar_type,
                from_ts_event,
                to_ts_event,
            ));
        } else {
            already_current.push((instrument_uid.clone(), continuity_key));
        }
    }
    jobs.sort_by(|left, right| left.0.cmp(&right.0));
    already_current.sort_by(|left, right| left.0.cmp(&right.0));
    if jobs.is_empty() && already_current.is_empty() {
        // A missing baseline means there is no bounded gap to recover. Keep the transport
        // lifecycle independent from the first observation; the initial live candle or the
        // periodic candle poll establishes the baseline for a later reconnect recovery.
        return ReconnectCatchUpOutcome::default();
    }

    trace_market_data_stream_event_if_current(
        stream_health,
        MarketDataStreamEventInput {
            stage: cause.started_stage(),
            task_key: group,
            stream_kind: kind.name(),
            instrument_count,
            status: None,
            reason: format!(
                "recovering {} instruments via GetCandles after {}",
                jobs.len(),
                cause.as_str()
            ),
            delay_ms: None,
            attempt: 0,
        },
    );

    let mut outcome = ReconnectCatchUpOutcome {
        ..ReconnectCatchUpOutcome::default()
    };
    for (instrument_uid, continuity_key) in already_current {
        outcome.checked += 1;
        if publish_ready {
            publish_candle_readiness_if_current(
                stream_health,
                TbankCandleReadinessState::Ready,
                &continuity_key,
                &instrument_uid,
                Some(to_ts_event),
                format!(
                    "{} candle continuity already covers the latest closed minute",
                    cause.as_str()
                ),
            );
        }
    }
    let mut backfill = BackfillCoordinator {
        market_data_client: market_data_client.clone(),
        timestamp_mode,
        request_timeout: config.historical_candle_request_timeout,
        max_retries: config.historical_candle_max_retries,
        retry_base_delay: config.historical_candle_retry_base_delay,
        require_complete_candles: true,
        instrument_metadata: instrument_metadata_snapshot(instrument_metadata),
        request_limiter: request_limiter.clone(),
    };

    for (instrument_uid, continuity_key, bar_type, from_ts_event, to_ts_event) in jobs {
        publish_candle_readiness_if_current(
            stream_health,
            TbankCandleReadinessState::Recovering,
            &continuity_key,
            &instrument_uid,
            None,
            format!("{} GetCandles recovery started", cause.as_str()),
        );
        let recovery = backfill
            .recover_range(
                &instrument_uid,
                bar_type,
                from_ts_event,
                to_ts_event,
                |bars| {
                    publish_recovery_batch_if_current(stream_health, group, sender, bars, |bars| {
                        let tracker = continuity.entry(instrument_uid.clone()).or_default();
                        for bar in bars {
                            let ts_event = i128::from(bar.ts_event.as_u64());
                            tracker.record_backfilled_bar(ts_event);
                            record_bar_watermark(bar_watermarks, bar_type, ts_event);
                        }
                        tracker.record_recovered_through(to_ts_event);
                        record_bar_watermark(bar_watermarks, bar_type, to_ts_event);
                    })
                },
            )
            .await;
        if !stream_health.is_current_task_key(group) {
            return outcome;
        }
        match recovery {
            Ok(RecoveryRangeResult::Published(backfilled_ts)) => {
                outcome.checked += 1;
                outcome.backfilled += backfilled_ts.len();
                if publish_ready {
                    publish_candle_readiness_if_current(
                        stream_health,
                        TbankCandleReadinessState::Ready,
                        &continuity_key,
                        &instrument_uid,
                        Some(to_ts_event),
                        format!(
                            "{} GetCandles recovery checked through the latest closed minute and published {} bars",
                            cause.as_str(),
                            backfilled_ts.len()
                        ),
                    );
                }
            }
            Ok(RecoveryRangeResult::Superseded) => return outcome,
            Err(error) => {
                outcome.failed += 1;
                publish_candle_readiness_if_current(
                    stream_health,
                    TbankCandleReadinessState::Failed,
                    &continuity_key,
                    &instrument_uid,
                    None,
                    format!("{} GetCandles recovery failed: {error}", cause.as_str()),
                );
                tracing::warn!(
                    %error,
                    group,
                    continuity_key,
                    instrument_uid,
                    recovery_cause = cause.as_str(),
                    "T-Bank lifecycle GetCandles recovery failed"
                );
            }
        }
    }
    *market_data_client = backfill.market_data_client;

    trace_market_data_stream_event_if_current(
        stream_health,
        MarketDataStreamEventInput {
            stage: cause.finished_stage(),
            task_key: group,
            stream_kind: kind.name(),
            instrument_count,
            status: None,
            reason: format!(
                "checked {} instruments and backfilled {} bars via GetCandles",
                outcome.checked, outcome.backfilled
            ),
            delay_ms: None,
            attempt: 0,
        },
    );

    outcome
}

#[derive(Debug, Clone, Copy)]
enum RecoveryCause {
    Startup,
    Reconnect,
}

impl RecoveryCause {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Reconnect => "reconnect",
        }
    }

    const fn started_stage(self) -> &'static str {
        match self {
            Self::Startup => "startup_recovery_started",
            Self::Reconnect => "reconnect_recovery_started",
        }
    }

    const fn finished_stage(self) -> &'static str {
        match self {
            Self::Startup => "startup_recovery_finished",
            Self::Reconnect => "reconnect_recovery_finished",
        }
    }
}

struct MarketDataStreamEventInput<'a> {
    stage: &'a str,
    task_key: &'a str,
    stream_kind: &'a str,
    instrument_count: usize,
    status: Option<String>,
    reason: String,
    delay_ms: Option<u128>,
    attempt: u32,
}

fn trace_market_data_stream_event(
    input: MarketDataStreamEventInput<'_>,
    readiness_ids: Vec<String>,
) {
    tracing::info!(
        stage = input.stage,
        task_key = input.task_key,
        stream_kind = input.stream_kind,
        instrument_count = input.instrument_count,
        status = input.status.as_deref(),
        reason = input.reason,
        delay_ms = input.delay_ms,
        attempt = input.attempt,
        "T-Bank market data stream transition"
    );
    let stream_id = logical_stream_id(input.task_key);
    if input.stage == "stream_snapshot_replaced" {
        publish_market_data_event(TbankMarketDataEvent::StreamRetired {
            stream_id,
            readiness_ids,
            reason: input.reason,
        });
        return;
    }
    if let Some(state) = TbankMarketDataStreamState::from_stage(input.stage) {
        publish_market_data_event(TbankMarketDataEvent::StreamState {
            stream_id,
            state,
            readiness_ids,
            reason: input.reason,
        });
    }
}

fn trace_market_data_stream_event_if_current(
    stream_health: &MarketDataStreamHealth,
    input: MarketDataStreamEventInput<'_>,
) {
    let task_key = input.task_key.to_string();
    stream_health.with_current_task_key_and_readiness(&task_key, |readiness_ids| {
        trace_market_data_stream_event(input, readiness_ids);
    });
}

fn publish_candle_readiness(
    state: TbankCandleReadinessState,
    task_key: &str,
    instrument_uid: &str,
    ready_through: Option<i128>,
    reason: String,
) {
    publish_market_data_event(TbankMarketDataEvent::CandleReadiness {
        readiness_id: logical_readiness_id(task_key),
        instrument_uid: instrument_uid.to_string(),
        state,
        ready_through: ready_through
            .and_then(|value| u64::try_from(value).ok())
            .map(UnixNanos::from),
        reason,
    });
}

fn publish_candle_readiness_if_current(
    stream_health: &MarketDataStreamHealth,
    state: TbankCandleReadinessState,
    task_key: &str,
    instrument_uid: &str,
    ready_through: Option<i128>,
    reason: String,
) {
    let task_key_copy = task_key.to_string();
    stream_health.with_current_task_key(&task_key_copy, || {
        publish_candle_readiness(state, task_key, instrument_uid, ready_through, reason);
    });
}

fn logical_readiness_id(task_key: &str) -> String {
    logical_stream_id(task_key)
}

fn logical_stream_id(task_key: &str) -> String {
    for prefix in ["bars:generation:", "quotes:generation:"] {
        if let Some(rest) = task_key.strip_prefix(prefix) {
            let Some((_, logical_suffix)) = rest.split_once(':') else {
                break;
            };
            let logical_prefix = prefix
                .strip_suffix("generation:")
                .expect("generation-qualified stream prefix");
            return format!("{logical_prefix}{logical_suffix}");
        }
    }
    if let Some((logical_key, generation)) = task_key.rsplit_once(":generation:")
        && !logical_key.is_empty()
        && !generation.is_empty()
        && generation
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return logical_key.to_string();
    }
    task_key.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingLiveBar {
    instrument_uid: String,
    bar_type: BarType,
    ts_event: i128,
    establishes_initial_baseline: bool,
}

fn filter_market_data_response_for_continuity(
    response: MarketDataResponse,
    kind: &TbankStreamKind,
    group: &str,
    bar_continuity_key_overrides: &HashMap<String, String>,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    bar_watermarks: &SharedBarWatermarks,
    continuity: &HashMap<String, BarContinuityTracker>,
) -> Option<(MarketDataResponse, Option<PendingLiveBar>)> {
    let Some(market_data_response::Payload::Candle(candle)) = response.payload.as_ref() else {
        return Some((response, None));
    };
    let TbankStreamKind::Bars { bar_types } = kind else {
        return Some((response, None));
    };
    let instrument_uid = candle.instrument_uid.clone();
    let bar_type = bar_types.get(&instrument_uid).copied()?;
    let continuity_key =
        bar_continuity_key_with_overrides(group, &instrument_uid, bar_continuity_key_overrides);
    let bar = match candle_to_bar(
        candle,
        timestamp_mode,
        i128::from(now_unix_nanos().as_u64()),
    ) {
        Ok(bar) => bar,
        Err(error) => {
            tracing::warn!(%error, continuity_key, "discarding invalid T-Bank candle");
            return None;
        }
    };
    let decision = continuity
        .get(&instrument_uid)
        .map_or(BarContinuityDecision::Accepted, |tracker| {
            tracker.classify_live_bar(bar.ts_event)
        });
    match decision {
        BarContinuityDecision::Accepted => Some((
            response,
            Some(PendingLiveBar {
                establishes_initial_baseline: !has_bar_watermark(bar_watermarks, bar_type),
                instrument_uid,
                bar_type,
                ts_event: bar.ts_event,
            }),
        )),
        BarContinuityDecision::Duplicate => None,
    }
}

async fn reconnect_market_data_clients(
    config: &TbankDataClientConfig,
) -> Result<(MarketDataStreamClient, MarketDataClient)> {
    let token = config.resolve_token_secret()?;
    let endpoint = config.endpoint_uri()?;
    let channel = connect_channel(&endpoint, config.request_timeout).await?;
    let interceptor = TbankAuthInterceptor::new(&token)?;
    let clients = TbankGrpcClients::new(channel, interceptor);
    Ok((clients.market_data_stream, clients.market_data))
}

#[derive(Debug, Clone)]
enum TbankStreamKind {
    Bars {
        bar_types: HashMap<String, nautilus_model::data::BarType>,
    },
    Trades {
        instrument_id: InstrumentId,
        instrument_uid: String,
    },
    Quotes {
        instrument_ids: HashMap<String, InstrumentId>,
    },
    Depth10 {
        instrument_id: InstrumentId,
        instrument_uid: String,
    },
}

impl TbankStreamKind {
    fn name(&self) -> &'static str {
        match self {
            Self::Bars { .. } => "bars",
            Self::Trades { .. } => "trades",
            Self::Quotes { .. } => "quotes",
            Self::Depth10 { .. } => "depth10",
        }
    }

    fn instrument_count(&self) -> usize {
        match self {
            Self::Bars { bar_types } => bar_types.len(),
            Self::Quotes { instrument_ids } => instrument_ids.len(),
            Self::Trades { .. } | Self::Depth10 { .. } => 1,
        }
    }
}

fn continuity_from_bar_watermarks(
    kind: &TbankStreamKind,
    watermarks: &HashMap<BarType, i128>,
) -> HashMap<String, BarContinuityTracker> {
    let TbankStreamKind::Bars { bar_types } = kind else {
        return HashMap::new();
    };
    bar_types
        .iter()
        .filter_map(|(instrument_uid, bar_type)| {
            watermarks.get(bar_type).copied().map(|watermark| {
                (
                    instrument_uid.clone(),
                    BarContinuityTracker::from_seeded_bar(watermark),
                )
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn drain_pre_ack_messages(
    messages: &mut VecDeque<(MarketDataResponse, UnixNanos, u64)>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    kind: &TbankStreamKind,
    instrument_metadata: &SharedInstrumentMetadata,
    bar_watermarks: &SharedBarWatermarks,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    task_key: &str,
    stream_health: &MarketDataStreamHealth,
    bar_continuity_key_overrides: &HashMap<String, String>,
    continuity: &mut HashMap<String, BarContinuityTracker>,
    attempt: &mut u32,
) {
    if !stream_health.is_current_task_key(task_key) {
        messages.clear();
        return;
    }
    let buffered = messages.len();
    while let Some((response, received_at, message_sequence)) = messages.pop_front() {
        publish_ready_market_data_response(
            sender,
            response,
            kind,
            instrument_metadata,
            bar_watermarks,
            timestamp_mode,
            received_at,
            task_key,
            stream_health,
            bar_continuity_key_overrides,
            continuity,
            attempt,
            message_sequence,
        );
    }
    if buffered > 0 {
        trace_market_data_stream_event_if_current(
            stream_health,
            MarketDataStreamEventInput {
                stage: "pre_ack_buffer_drained",
                task_key,
                stream_kind: kind.name(),
                instrument_count: kind.instrument_count(),
                status: None,
                reason: format!(
                    "published {buffered} buffered market data messages after acknowledgement"
                ),
                delay_ms: None,
                attempt: *attempt,
            },
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_ready_market_data_response(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    response: MarketDataResponse,
    kind: &TbankStreamKind,
    instrument_metadata: &SharedInstrumentMetadata,
    bar_watermarks: &SharedBarWatermarks,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    received_at: UnixNanos,
    task_key: &str,
    stream_health: &MarketDataStreamHealth,
    bar_continuity_key_overrides: &HashMap<String, String>,
    continuity: &mut HashMap<String, BarContinuityTracker>,
    attempt: &mut u32,
    message_sequence: u64,
) {
    if !stream_health.is_current_task_key(task_key) {
        return;
    }
    let Some((response, pending_live_bar)) = filter_market_data_response_for_continuity(
        response,
        kind,
        task_key,
        bar_continuity_key_overrides,
        timestamp_mode,
        bar_watermarks,
        continuity,
    ) else {
        return;
    };
    let usable_stream_message = is_usable_market_data_response(&response, kind);
    let published = stream_health.with_current_task_key_after_publish(
        task_key,
        || {
            publish_market_data_response(
                sender,
                response,
                kind,
                instrument_metadata,
                timestamp_mode,
                received_at,
                message_sequence,
            )
        },
        || {
            reset_reconnect_attempt_if_usable(usable_stream_message, attempt);
            if let Some(pending_live_bar) = pending_live_bar.as_ref() {
                commit_live_bar(
                    bar_watermarks,
                    continuity,
                    &pending_live_bar.instrument_uid,
                    pending_live_bar.bar_type,
                    pending_live_bar.ts_event,
                );
            }
        },
    );
    let published = match published {
        Ok(published) => published,
        Err(error) => {
            tracing::warn!(%error, task_key, "failed to publish T-Bank market data event");
            return;
        }
    };
    if !published {
        return;
    }
    let Some(pending_live_bar) = pending_live_bar else {
        return;
    };
    if !pending_live_bar.establishes_initial_baseline {
        return;
    }
    let continuity_key = bar_continuity_key_with_overrides(
        task_key,
        &pending_live_bar.instrument_uid,
        bar_continuity_key_overrides,
    );
    publish_candle_readiness_if_current(
        stream_health,
        TbankCandleReadinessState::Ready,
        &continuity_key,
        &pending_live_bar.instrument_uid,
        Some(pending_live_bar.ts_event),
        "first acknowledged live candle established the initial continuity baseline".to_string(),
    );
}

fn publish_market_data_response(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    response: MarketDataResponse,
    kind: &TbankStreamKind,
    instrument_metadata: &SharedInstrumentMetadata,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    received_at: UnixNanos,
    message_sequence: u64,
) -> anyhow::Result<()> {
    let Some(payload) = response.payload else {
        return Ok(());
    };
    let data = match (payload, kind) {
        (market_data_response::Payload::Candle(candle), TbankStreamKind::Bars { bar_types }) => {
            let bar_type = bar_types
                .get(&candle.instrument_uid)
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "received candle for unregistered T-Bank stream instrument {}",
                        candle.instrument_uid
                    )
                })?;
            let metadata = market_data_metadata_for(
                instrument_metadata,
                &candle.instrument_uid,
                Some(bar_type.instrument_id()),
            )?;
            Some(Data::from(nautilus_bar_from_candle(
                &candle,
                bar_type,
                metadata,
                timestamp_mode,
                received_at,
            )?))
        }
        (
            market_data_response::Payload::Trade(trade),
            TbankStreamKind::Trades {
                instrument_id,
                instrument_uid,
            },
        ) => {
            if trade.instrument_uid != *instrument_uid {
                anyhow::bail!(
                    "received trade for unexpected T-Bank stream instrument {}; expected {}",
                    trade.instrument_uid,
                    instrument_uid
                );
            }
            let metadata = market_data_metadata_for(
                instrument_metadata,
                &trade.instrument_uid,
                Some(*instrument_id),
            )?;
            Some(Data::from(nautilus_trade_from_tbank(
                &trade,
                *instrument_id,
                metadata,
                received_at,
                message_sequence,
            )?))
        }
        (
            market_data_response::Payload::Orderbook(orderbook),
            TbankStreamKind::Quotes { instrument_ids },
        ) => {
            let instrument_id = instrument_ids
                .get(&orderbook.instrument_uid)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "received quote for unregistered T-Bank stream instrument {}",
                        orderbook.instrument_uid
                    )
                })?;
            let metadata = market_data_metadata_for(
                instrument_metadata,
                &orderbook.instrument_uid,
                Some(*instrument_id),
            )?;
            nautilus_quote_from_orderbook(&orderbook, *instrument_id, metadata, received_at)?
                .map(Data::from)
        }
        (
            market_data_response::Payload::Orderbook(orderbook),
            TbankStreamKind::Depth10 {
                instrument_id,
                instrument_uid,
            },
        ) => {
            if orderbook.instrument_uid != *instrument_uid {
                anyhow::bail!(
                    "received order book for unexpected T-Bank stream instrument {}; expected {}",
                    orderbook.instrument_uid,
                    instrument_uid
                );
            }
            let metadata = market_data_metadata_for(
                instrument_metadata,
                &orderbook.instrument_uid,
                Some(*instrument_id),
            )?;
            Some(Data::from(nautilus_depth10_from_orderbook(
                &orderbook,
                *instrument_id,
                metadata,
                received_at,
            )?))
        }
        _ => None,
    };

    if let Some(data) = data {
        sender
            .send(DataEvent::Data(data))
            .map_err(|error| anyhow::anyhow!("data event receiver dropped: {error}"))?;
    }
    Ok(())
}

fn market_data_metadata_for(
    metadata: &SharedInstrumentMetadata,
    instrument_uid: &str,
    instrument_id: Option<InstrumentId>,
) -> anyhow::Result<MarketDataInstrumentMetadata> {
    let metadata = metadata.read().expect("market-data metadata lock");
    metadata
        .get(instrument_uid)
        .or_else(|| instrument_id.and_then(|id| metadata.get(&id.to_string())))
        .copied()
        .ok_or_else(|| {
            anyhow::anyhow!("missing market-data metadata for T-Bank instrument {instrument_uid}")
        })
}

fn instrument_stream_id(instrument_id: InstrumentId) -> String {
    let instrument_id_string = instrument_id.to_string();
    TbankInstrumentIdParts::from_str(&instrument_id_string)
        .map(|parts| parts.ticker_class_code())
        .unwrap_or_else(|_| instrument_id.symbol.as_str().to_string())
}

fn stream_task_key(kind: &str, stream_id: &str, detail: impl std::fmt::Display) -> String {
    format!("{kind}:{stream_id}:{detail}")
}

fn stream_kind_from_task_key(task_key: &str) -> &'static str {
    if task_key.starts_with("bars:") {
        "bars"
    } else if task_key.starts_with("quotes:") {
        "quotes"
    } else if task_key.starts_with("trades:") {
        "trades"
    } else if task_key.starts_with("depth10:") {
        "depth10"
    } else {
        "unknown"
    }
}

fn instrument_id_from_stream_parts(
    ticker: &str,
    class_code: &str,
    fallback: InstrumentId,
    preserve_fallback: bool,
) -> anyhow::Result<InstrumentId> {
    if preserve_fallback || ticker.is_empty() || class_code.is_empty() {
        return Ok(fallback);
    }
    instrument_id_from_ticker_class_for_venue(ticker, class_code, fallback.venue.as_str())
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid stream instrument id: {error}"))
}

fn decimal_price(value: Decimal, precision: u8) -> anyhow::Result<Price> {
    Price::from_decimal_dp(value, precision)
        .map_err(|error| anyhow::anyhow!("invalid price {value} at precision {precision}: {error}"))
}

fn lot_quantity(value_lots: i64, lot_size: u32) -> anyhow::Result<Quantity> {
    let value_shares = Decimal::from(value_lots) * Decimal::from(lot_size);
    Quantity::from_decimal(value_shares)
        .map_err(|error| anyhow::anyhow!("invalid quantity {value_shares}: {error}"))
}

fn unix_nanos(value: i128) -> anyhow::Result<UnixNanos> {
    let value = u64::try_from(value).map_err(|_| anyhow::anyhow!("negative timestamp {value}"))?;
    Ok(UnixNanos::from(value))
}

fn datetime_to_unix_nanos(value: Option<chrono::DateTime<chrono::Utc>>) -> Option<UnixNanos> {
    value.and_then(|datetime| {
        datetime
            .timestamp_nanos_opt()
            .and_then(|nanos| u64::try_from(nanos).ok())
            .map(UnixNanos::from)
    })
}

pub(crate) fn now_unix_nanos() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}

pub(crate) fn nautilus_bar_from_candle(
    candle: &Candle,
    bar_type: nautilus_model::data::BarType,
    metadata: MarketDataInstrumentMetadata,
    timestamp_mode: crate::config::TbankCandleTimestampMode,
    received_at: UnixNanos,
) -> anyhow::Result<Bar> {
    let bar = candle_to_bar(candle, timestamp_mode, i128::from(received_at.as_u64()))?;
    Bar::new_checked(
        bar_type,
        decimal_price(bar.open, metadata.price_precision)?,
        decimal_price(bar.high, metadata.price_precision)?,
        decimal_price(bar.low, metadata.price_precision)?,
        decimal_price(bar.close, metadata.price_precision)?,
        lot_quantity(bar.volume_lots, metadata.lot_size)?,
        unix_nanos(bar.ts_event)?,
        received_at,
    )
}

fn nautilus_trade_from_tbank(
    trade: &Trade,
    fallback_instrument_id: InstrumentId,
    metadata: MarketDataInstrumentMetadata,
    received_at: UnixNanos,
    message_sequence: u64,
) -> anyhow::Result<TradeTick> {
    let tick = trade_to_tick(trade, i128::from(received_at.as_u64()))?;
    let instrument_id = instrument_id_from_stream_parts(
        &trade.ticker,
        &trade.class_code,
        fallback_instrument_id,
        metadata.preserve_instrument_id,
    )?;
    let aggressor_side = match tick.side {
        crate::market_data::trades::TbankTradeSide::Buy => AggressorSide::Buyer,
        crate::market_data::trades::TbankTradeSide::Sell => AggressorSide::Seller,
        crate::market_data::trades::TbankTradeSide::Unknown => AggressorSide::NoAggressor,
    };
    let trade_id = tbank_trade_id(trade, instrument_id, message_sequence);
    TradeTick::new_checked(
        instrument_id,
        decimal_price(tick.price, metadata.price_precision)?,
        lot_quantity(tick.quantity_lots, metadata.lot_size)?,
        aggressor_side,
        trade_id,
        unix_nanos(tick.ts_event)?,
        unix_nanos(tick.ts_init)?,
    )
    .map_err(|error| anyhow::anyhow!("invalid T-Bank trade tick: {error}"))
}

/// Builds a bounded synthetic trade ID because T-Bank's public `Trade` message has no
/// venue-provided match identifier. The message sequence is the collision-free part within
/// this client instance; the fingerprint keeps the ID tied to the immutable source fields and
/// makes it deterministic for a given source message and sequence.
fn tbank_trade_id(trade: &Trade, instrument_id: InstrumentId, message_sequence: u64) -> TradeId {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut fingerprint = FNV_OFFSET;
    let mut update = |bytes: &[u8]| {
        for byte in bytes {
            fingerprint ^= u64::from(*byte);
            fingerprint = fingerprint.wrapping_mul(FNV_PRIME);
        }
        fingerprint ^= 0xff;
        fingerprint = fingerprint.wrapping_mul(FNV_PRIME);
    };

    update(instrument_id.to_string().as_bytes());
    update(trade.figi.as_bytes());
    update(trade.instrument_uid.as_bytes());
    update(trade.ticker.as_bytes());
    update(trade.class_code.as_bytes());
    update(&trade.direction.to_le_bytes());
    update(&trade.trade_source.to_le_bytes());
    update(&trade.quantity.to_le_bytes());
    match trade.price.as_ref() {
        Some(price) => {
            update(&price.units.to_le_bytes());
            update(&price.nano.to_le_bytes());
        }
        None => update(&[]),
    }
    match trade.time.as_ref() {
        Some(time) => {
            update(&time.seconds.to_le_bytes());
            update(&time.nanos.to_le_bytes());
        }
        None => update(&[]),
    }

    // `TradeId` is capped at 36 ASCII characters: "tb-" + 16 hex chars + "-" + 16 hex chars.
    TradeId::new(format!("tb-{fingerprint:016x}-{message_sequence:016x}"))
}

fn nautilus_quote_from_orderbook(
    orderbook: &OrderBook,
    fallback_instrument_id: InstrumentId,
    metadata: MarketDataInstrumentMetadata,
    received_at: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let snapshot = orderbook_to_snapshot(orderbook, i128::from(received_at.as_u64()))?;
    let (Some(bid), Some(ask)) = (snapshot.bids.first(), snapshot.asks.first()) else {
        return Ok(None);
    };
    let instrument_id = instrument_id_from_stream_parts(
        &orderbook.ticker,
        &orderbook.class_code,
        fallback_instrument_id,
        metadata.preserve_instrument_id,
    )?;
    QuoteTick::new_checked(
        instrument_id,
        decimal_price(bid.price, metadata.price_precision)?,
        decimal_price(ask.price, metadata.price_precision)?,
        lot_quantity(bid.quantity_lots, metadata.lot_size)?,
        lot_quantity(ask.quantity_lots, metadata.lot_size)?,
        unix_nanos(snapshot.ts_event)?,
        unix_nanos(snapshot.ts_init)?,
    )
    .map(Some)
    .map_err(|error| anyhow::anyhow!("invalid T-Bank quote tick: {error}"))
}

fn nautilus_depth10_from_orderbook(
    orderbook: &OrderBook,
    fallback_instrument_id: InstrumentId,
    metadata: MarketDataInstrumentMetadata,
    received_at: UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    let snapshot = orderbook_to_snapshot(orderbook, i128::from(received_at.as_u64()))?;
    let instrument_id = instrument_id_from_stream_parts(
        &orderbook.ticker,
        &orderbook.class_code,
        fallback_instrument_id,
        metadata.preserve_instrument_id,
    )?;
    let mut bids = [BookOrder::default(); DEPTH10_LEN];
    let mut asks = [BookOrder::default(); DEPTH10_LEN];
    let mut bid_counts = [0_u32; DEPTH10_LEN];
    let mut ask_counts = [0_u32; DEPTH10_LEN];

    for (idx, level) in snapshot.bids.iter().take(DEPTH10_LEN).enumerate() {
        bids[idx] = BookOrder::new(
            OrderSide::Buy,
            decimal_price(level.price, metadata.price_precision)?,
            lot_quantity(level.quantity_lots, metadata.lot_size)?,
            idx as u64 + 1,
        );
        bid_counts[idx] = 1;
    }
    for (idx, level) in snapshot.asks.iter().take(DEPTH10_LEN).enumerate() {
        asks[idx] = BookOrder::new(
            OrderSide::Sell,
            decimal_price(level.price, metadata.price_precision)?,
            lot_quantity(level.quantity_lots, metadata.lot_size)?,
            idx as u64 + 1,
        );
        ask_counts[idx] = 1;
    }

    Ok(OrderBookDepth10::new(
        instrument_id,
        bids,
        asks,
        bid_counts,
        ask_counts,
        0,
        snapshot.ts_event as u64,
        unix_nanos(snapshot.ts_event)?,
        unix_nanos(snapshot.ts_init)?,
    ))
}

mod nautilus;

#[cfg(test)]
mod tests {
    // Protobuf responses use struct update syntax so tests remain source-compatible
    // when upstream adds response fields.
    #![allow(clippy::needless_update)]

    use chrono::TimeZone;
    use nautilus_model::{
        data::{BarSpecification, BarType},
        enums::{AggregationSource, BarAggregation, PriceType},
    };

    use crate::{
        config::TbankCandleTimestampMode,
        grpc::generated::{
            Candle, CandleSubscription, MarketDataResponse, Order, OrderBook, Quotation,
            SubscribeCandlesResponse, SubscribeTradesResponse, SubscriptionInterval, Trade,
            TradeDirection, TradeSubscription, market_data_response,
        },
    };

    use super::*;

    fn q(units: i64, nano: i32) -> Option<Quotation> {
        Some(Quotation { units, nano })
    }

    fn ts(seconds: i64) -> prost_types::Timestamp {
        prost_types::Timestamp { seconds, nanos: 0 }
    }

    fn sber_id() -> InstrumentId {
        "SBER_TQBR.MOEX".parse().unwrap()
    }

    fn sber_market_data_metadata() -> MarketDataInstrumentMetadata {
        MarketDataInstrumentMetadata {
            lot_size: 10,
            price_precision: 2,
            preserve_instrument_id: false,
        }
    }

    fn sber_bar_type() -> BarType {
        BarType::new(
            sber_id(),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
            AggregationSource::External,
        )
    }

    fn sber_hour_bar_type() -> BarType {
        BarType::new(
            sber_id(),
            BarSpecification::new(1, BarAggregation::Hour, PriceType::Last),
            AggregationSource::External,
        )
    }

    #[tokio::test]
    async fn reset_disconnects_client_and_clears_subscription_state() {
        use nautilus_common::clients::DataClient;

        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = TbankAuthInterceptor::new("test-token").unwrap();
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.clients = Some(TbankGrpcClients::new(channel, interceptor));
        client.bar_subscriptions.insert(sber_id(), sber_bar_type());
        client.quote_subscriptions.insert(sber_id());
        client.trade_subscriptions.insert(sber_id());
        client.depth10_subscriptions.insert(sber_id(), 10);

        DataClient::reset(&mut client).unwrap();

        assert!(DataClient::is_disconnected(&client));
        assert!(client.bar_subscriptions.is_empty());
        assert!(client.quote_subscriptions.is_empty());
        assert!(client.trade_subscriptions.is_empty());
        assert!(client.depth10_subscriptions.is_empty());
    }

    #[test]
    fn disconnect_resets_routes_but_keeps_stable_subscription_state() {
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client
            .resolved_instrument_stream_ids
            .write()
            .unwrap()
            .insert(
                "SBER_TQBR.MOEX".to_string(),
                "sber-canonical-uid".to_string(),
            );
        client.bar_subscriptions.insert(sber_id(), sber_bar_type());
        client.quote_subscriptions.insert(sber_id());

        client.disconnect();

        assert_eq!(client.stream_id(sber_id()), "SBER_TQBR");
        assert!(client.bar_subscriptions.contains_key(&sber_id()));
        assert!(client.quote_subscriptions.contains(&sber_id()));
    }

    #[tokio::test]
    async fn terminal_stream_health_is_not_reported_as_connected() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = TbankAuthInterceptor::new("test-token").unwrap();
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.clients = Some(TbankGrpcClients::new(channel, interceptor));

        assert!(client.is_connected());
        client.stream_health.register("bars:group:0:1m");
        assert!(!client.is_connected());
        client.stream_health.mark_operational("bars:group:0:1m");
        assert!(client.is_connected());
        client.stream_health.register("test:terminal");
        client.stream_health.mark_terminal("test:terminal");
        assert!(!client.is_connected());
    }

    #[test]
    fn reconnecting_stream_health_is_visible_before_retry_budget_exhaustion() {
        let health = MarketDataStreamHealth::default();
        let task_key = "bars:generation:1:group:0:1m";

        health.register(task_key);
        health.mark_operational(task_key);
        assert!(health.is_operational());

        health.mark_reconnecting(task_key);

        assert!(!health.is_operational());
    }

    #[test]
    fn stale_bar_generation_cannot_restore_current_stream_health() {
        let health = MarketDataStreamHealth::default();
        let old_key = "bars:generation:1:group:0:1m";
        let current_key = "bars:generation:2:group:0:1m";

        health.advance_bar_generation(1);
        health.register(old_key);
        health.mark_operational(old_key);
        assert!(health.is_operational());

        health.advance_bar_generation(2);
        health.register(current_key);
        health.mark_operational(old_key);

        assert!(!health.is_operational());
    }

    #[test]
    fn retry_child_registration_and_spawn_share_parent_lifecycle_lock() {
        let health = Arc::new(MarketDataStreamHealth::default());
        let parent_key = "bars:generation:1:group:0:1m";
        let child_key = "bars:generation:1:group:0:1m:retry:uid";
        health.register(parent_key);

        let (spawn_started_tx, spawn_started_rx) = std::sync::mpsc::sync_channel(0);
        let (spawn_release_tx, spawn_release_rx) = std::sync::mpsc::sync_channel(0);
        let spawning_health = health.clone();
        let spawn_thread = std::thread::spawn(move || {
            spawning_health.spawn_child_if_current(parent_key, child_key, || {
                spawn_started_tx.send(()).unwrap();
                spawn_release_rx.recv().unwrap();
                get_runtime().spawn(async {})
            })
        });
        spawn_started_rx.recv().unwrap();

        let (replacement_started_tx, replacement_started_rx) = std::sync::mpsc::sync_channel(0);
        let (replacement_finished_tx, replacement_finished_rx) = std::sync::mpsc::channel();
        let replacement_health = health.clone();
        let replacement_thread = std::thread::spawn(move || {
            replacement_started_tx.send(()).unwrap();
            replacement_health.advance_bar_generation(2);
            replacement_finished_tx.send(()).unwrap();
        });
        replacement_started_rx.recv().unwrap();
        assert!(
            replacement_finished_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );

        spawn_release_tx.send(()).unwrap();
        let child = spawn_thread
            .join()
            .unwrap()
            .expect("child should be spawned");
        replacement_thread.join().unwrap();
        child.abort();

        assert!(!health.is_current_task_key(parent_key));
        assert!(!health.is_current_task_key(child_key));
    }

    #[test]
    fn stale_non_bar_generation_cannot_change_current_stream_health() {
        let health = MarketDataStreamHealth::default();
        let logical_key = "trades:SBER_TQBR.MOEX:all";
        let old_key = TbankDataClient::generation_task_key(logical_key, 1);
        let current_key = TbankDataClient::generation_task_key(logical_key, 2);

        health.register(&old_key);
        health.mark_operational(&old_key);
        health.retire_task_key(&old_key, "test generation replacement");
        health.register(&current_key);

        health.mark_operational(&old_key);
        health.mark_non_operational(&old_key);
        assert!(!health.is_operational());

        health.mark_operational(&current_key);
        assert!(health.is_operational());
    }

    #[test]
    fn lifecycle_replacement_cannot_split_publication_and_cursor_commit() {
        let task_key = "bars:generation:1:group:0:1m";
        let health = Arc::new(MarketDataStreamHealth::default());
        health.register(task_key);
        let (replacement_started_tx, replacement_started_rx) = std::sync::mpsc::sync_channel(0);
        let (replacement_finished_tx, replacement_finished_rx) = std::sync::mpsc::channel();
        let replacement_health = health.clone();
        let replacement = std::thread::spawn(move || {
            replacement_started_tx.send(()).unwrap();
            replacement_health.replace_expected("bars:", std::iter::empty::<&str>());
            replacement_finished_tx.send(()).unwrap();
        });
        let mut committed = false;

        let published = health
            .with_current_task_key_after_publish(
                task_key,
                || {
                    replacement_started_rx
                        .recv_timeout(Duration::from_secs(1))
                        .unwrap();
                    Ok(())
                },
                || {
                    assert!(
                        replacement_finished_rx
                            .recv_timeout(Duration::from_millis(50))
                            .is_err()
                    );
                    committed = true;
                },
            )
            .unwrap();

        replacement.join().unwrap();
        assert!(published);
        assert!(committed);
        assert!(!health.is_current_task_key(task_key));
    }

    #[tokio::test]
    async fn non_bar_snapshots_register_expected_groups_before_spawn() {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        nautilus_common::live::runner::replace_data_event_sender(sender);
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = TbankAuthInterceptor::new("test-token").unwrap();
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.clients = Some(TbankGrpcClients::new(channel, interceptor));

        client.quote_subscriptions.insert(sber_id());
        client.schedule_quote_stream().unwrap();
        client.schedule_trade_stream(sber_id()).unwrap();
        client.schedule_depth10_stream(sber_id(), 10).unwrap();

        let expected = client.stream_health.expected_task_keys();
        assert_eq!(expected.len(), 3);
        assert!(
            expected
                .iter()
                .any(|key| key.starts_with("quotes:generation:"))
        );
        assert!(
            expected
                .iter()
                .any(|key| key.starts_with("trades:SBER_TQBR.MOEX:all:generation:"))
        );
        assert!(
            expected
                .iter()
                .any(|key| key.starts_with("depth10:SBER_TQBR.MOEX:book:generation:"))
        );
        assert!(!client.is_connected());

        client.stop_market_data_streams("test cleanup", false);
    }

    #[test]
    fn bar_watermark_survives_broker_route_change_without_cache() {
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.bar_subscriptions.insert(sber_id(), sber_bar_type());
        client
            .resolved_instrument_stream_ids
            .write()
            .unwrap()
            .insert("SBER_TQBR.MOEX".to_string(), "old-route".to_string());
        record_bar_watermark(
            &client.bar_watermarks,
            sber_bar_type(),
            1_700_000_000_000_000_000,
        );

        assert_eq!(client.stream_id(sber_id()), "old-route");
        client
            .resolved_instrument_stream_ids
            .write()
            .unwrap()
            .insert("SBER_TQBR.MOEX".to_string(), "new-route".to_string());
        assert_eq!(client.stream_id(sber_id()), "new-route");

        let watermarks = snapshot_bar_watermarks(&client.bar_watermarks);
        assert_eq!(watermarks[&sber_bar_type()], 1_700_000_000_000_000_000);
        let new_route_kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("new-route".to_string(), sber_bar_type())]),
        };
        assert_eq!(
            continuity_from_bar_watermarks(&new_route_kind, &watermarks)
                .get("new-route")
                .and_then(BarContinuityTracker::latest_seen),
            Some(1_700_000_000_000_000_000)
        );
    }

    #[tokio::test]
    async fn disabling_reconnect_invalidates_readiness_and_publishes_terminal_group_state() {
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        let group_key = "bars:generation:0:group:opt_out:1m";
        let readiness_key = format!("{group_key}:instrument:SBER_TQBR");
        client.stream_health.register(group_key);
        client.stream_health.register_current(&readiness_key);
        let mut events = crate::market_data::subscribe_market_data_events();

        client.stop_market_data_streams("subscriptions disabled for reconnect", true);

        let stream = loop {
            match events.try_recv() {
                Ok(TbankMarketDataEvent::StreamState {
                    stream_id, state, ..
                }) if stream_id == "bars:group:opt_out:1m" => break state,
                Ok(_) => continue,
                Err(error) => panic!("terminal reconnect stream event missing: {error:?}"),
            }
        };
        assert_eq!(stream, TbankMarketDataStreamState::Dead);

        let retirement = loop {
            match events.try_recv() {
                Ok(TbankMarketDataEvent::StreamRetired {
                    stream_id,
                    readiness_ids,
                    ..
                }) if stream_id == "bars:group:opt_out:1m" => {
                    break readiness_ids;
                }
                Ok(_) => continue,
                Err(error) => panic!("stream retirement event missing: {error:?}"),
            }
        };
        assert_eq!(
            retirement,
            vec!["bars:group:opt_out:1m:instrument:SBER_TQBR".to_string()]
        );
    }

    #[tokio::test]
    async fn rebuilding_bar_snapshot_invalidates_readiness_and_registers_expected_groups() {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        nautilus_common::live::runner::replace_data_event_sender(sender);
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = TbankAuthInterceptor::new("test-token").unwrap();
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.clients = Some(TbankGrpcClients::new(channel, interceptor));
        client.bar_subscriptions.insert(sber_id(), sber_bar_type());

        client.schedule_bar_streams().unwrap();

        assert!(!client.is_connected());
        assert_eq!(
            client.scheduled_bar_continuity_keys.get("SBER_TQBR"),
            Some(&"bars:generation:1:group:0:1m:instrument:SBER_TQBR".to_string())
        );

        client
            .stream_health
            .mark_operational("bars:generation:1:group:0:1m");
        assert!(client.is_connected());
        let mut events = crate::market_data::subscribe_market_data_events();

        client.bar_subscriptions.clear();
        client.schedule_bar_streams().unwrap();

        assert!(client.is_connected());
        let (stream_id, readiness_ids) = loop {
            match events.try_recv() {
                Ok(TbankMarketDataEvent::StreamRetired {
                    stream_id,
                    readiness_ids,
                    ..
                }) => break (stream_id, readiness_ids),
                Ok(_) => continue,
                Err(error) => panic!("stream retirement event missing: {error:?}"),
            }
        };
        assert_eq!(stream_id, "bars:group:0:1m");
        assert_eq!(
            readiness_ids,
            vec!["bars:group:0:1m:instrument:SBER_TQBR".to_string()]
        );
        assert!(client.scheduled_bar_continuity_keys.is_empty());
    }

    #[test]
    fn recoverable_stream_failures_are_typed_reconnect_states() {
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_worker_normal_exit"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_recovery_failed"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_subscription_acked"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_reconnect_exhausted"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_supervisor_reconnect_exhausted"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_supervisor_exhausted"),
            Some(TbankMarketDataStreamState::Dead)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("subscription_permanently_rejected"),
            Some(TbankMarketDataStreamState::Dead)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_subscriptions_stopped"),
            Some(TbankMarketDataStreamState::Reconnecting)
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_snapshot_replaced"),
            None
        );
        assert_eq!(
            TbankMarketDataStreamState::from_stage("stream_subscriptions_disabled"),
            Some(TbankMarketDataStreamState::Dead)
        );
    }

    #[test]
    fn public_market_data_ids_hide_stream_generations() {
        assert_eq!(
            logical_stream_id("bars:generation:3:group:0:1m"),
            "bars:group:0:1m"
        );
        assert_eq!(
            logical_readiness_id("bars:generation:3:group:0:1m:instrument:uid"),
            "bars:group:0:1m:instrument:uid"
        );
        assert_eq!(
            logical_stream_id("bars:generation:3:poll:indicative:1m:instrument:uid"),
            "bars:poll:indicative:1m:instrument:uid"
        );
        assert_eq!(
            logical_stream_id("quotes:generation:2:group:0:depth1"),
            "quotes:group:0:depth1"
        );
        assert_eq!(
            logical_stream_id("trades:SBER_TQBR.MOEX:all:generation:4"),
            "trades:SBER_TQBR.MOEX:all"
        );
    }

    #[test]
    fn non_retryable_stream_status_terminates_the_worker() {
        let exit = permanent_stream_status(&tonic::Status::unauthenticated("expired token"));

        assert!(matches!(
            exit,
            Some(StreamWorkerExit::Permanent(reason))
                if reason.contains("non-retryable stream status")
        ));
        assert!(permanent_stream_status(&tonic::Status::unavailable("temporary outage")).is_none());
    }

    #[test]
    fn stream_restart_reuses_the_latest_shared_bar_watermark() {
        let bar_type = sber_bar_type();
        let watermarks = Arc::new(std::sync::Mutex::new(HashMap::from([(bar_type, 100_i128)])));

        record_bar_watermark(&watermarks, bar_type, 90);
        record_bar_watermark(&watermarks, bar_type, 120);

        assert_eq!(snapshot_bar_watermarks(&watermarks)[&bar_type], 120);
    }

    #[test]
    fn cached_bar_watermarks_remain_distinct_per_bar_interval() {
        let minute = sber_bar_type();
        let hour = sber_hour_bar_type();
        let cached = HashMap::from([(minute, 60), (hour, 3_600)]);

        assert_eq!(
            bar_watermarks_for_subscriptions(&[("sber-uid".to_string(), minute)], &cached),
            HashMap::from([(minute, 60)])
        );
        assert_eq!(
            bar_watermarks_for_subscriptions(&[("sber-uid".to_string(), hour)], &cached),
            HashMap::from([(hour, 3_600)])
        );
    }

    #[test]
    fn periodic_poll_partition_matches_materialized_broker_stream_id() {
        let client = TbankDataClient::new(TbankDataClientConfig {
            instrument_stream_ids: HashMap::from([(
                "SBER_TQBR.MOEX".to_string(),
                "index-uid".to_string(),
            )]),
            periodic_candle_poll_instrument_ids: HashSet::from(["index-uid".to_string()]),
            ..TbankDataClientConfig::default()
        });
        let subscriptions = vec![(client.stream_id(sber_id()), sber_bar_type())];

        let (poll, live) = partition_bar_stream_subscriptions(
            subscriptions,
            &client.config.periodic_candle_poll_instrument_ids,
        );

        assert_eq!(poll.len(), 1);
        assert!(live.is_empty());
        assert_eq!(poll[0].0, "index-uid");
    }

    #[test]
    fn reconnect_subscription_restore_honors_opt_out() {
        assert!(should_restore_market_data_streams(false, false));
        assert!(should_restore_market_data_streams(false, true));
        assert!(!should_restore_market_data_streams(true, false));
        assert!(should_restore_market_data_streams(true, true));
    }

    #[test]
    fn reconnect_opt_out_clears_desired_and_legacy_subscription_state() {
        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        client.bar_subscriptions.insert(sber_id(), sber_bar_type());
        client.quote_subscriptions.insert(sber_id());
        client.trade_subscriptions.insert(sber_id());
        client.depth10_subscriptions.insert(sber_id(), 10);
        client.subscriptions.subscribe_trades("sber-uid");

        client.clear_market_data_subscription_state();

        assert!(client.bar_subscriptions.is_empty());
        assert!(client.quote_subscriptions.is_empty());
        assert!(client.trade_subscriptions.is_empty());
        assert!(client.depth10_subscriptions.is_empty());
        assert!(client.restore_subscription_requests().is_empty());
    }

    #[test]
    fn nautilus_trade_command_updates_restore_subscription_registry() {
        use nautilus_common::{clients::DataClient, messages::data::SubscribeTrades};
        use nautilus_core::{UUID4, UnixNanos};

        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        let command = SubscribeTrades::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        assert!(DataClient::subscribe_trades(&mut client, command).is_err());
        assert_eq!(client.restore_subscription_requests().len(), 1);
    }

    #[test]
    fn nautilus_restore_and_unsubscribe_use_the_current_route_for_stable_identity() {
        use nautilus_common::{clients::DataClient, messages::data::UnsubscribeTrades};
        use nautilus_core::{UUID4, UnixNanos};

        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        let subscribe = SubscribeTrades::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        assert!(DataClient::subscribe_trades(&mut client, subscribe).is_err());
        let fallback_requests = client.restore_subscription_requests();
        let Some(market_data_request::Payload::SubscribeTradesRequest(request)) =
            &fallback_requests[0].payload
        else {
            panic!("expected trade restore request");
        };
        assert_eq!(request.instruments[0].instrument_id, "SBER_TQBR");

        merge_resolved_instrument_stream_ids(
            &client.resolved_instrument_stream_ids,
            HashMap::from([(
                "SBER_TQBR.MOEX".to_string(),
                "sber-canonical-uid".to_string(),
            )]),
        );
        let canonical_requests = client.restore_subscription_requests();
        let Some(market_data_request::Payload::SubscribeTradesRequest(request)) =
            &canonical_requests[0].payload
        else {
            panic!("expected trade restore request");
        };
        assert_eq!(request.instruments[0].instrument_id, "sber-canonical-uid");

        let unsubscribe = UnsubscribeTrades::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        DataClient::unsubscribe_trades(&mut client, &unsubscribe).unwrap();
        assert!(client.restore_subscription_requests().is_empty());
    }

    #[test]
    fn nautilus_quote_restore_uses_the_depth_one_order_book_stream() {
        use nautilus_core::UUID4;

        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        let command = SubscribeQuotes::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        assert!(DataClient::subscribe_quotes(&mut client, command).is_err());
        let requests = client.restore_subscription_requests();
        let Some(market_data_request::Payload::SubscribeOrderBookRequest(request)) =
            &requests[0].payload
        else {
            panic!("expected order-book restore request");
        };
        assert_eq!(request.instruments[0].depth, 1);
    }

    #[test]
    fn nautilus_depth_restore_replaces_and_removes_all_previous_depths() {
        use std::num::NonZeroUsize;

        use nautilus_core::UUID4;

        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        for depth in [5, 10] {
            let command = SubscribeBookDepth10::new(
                sber_id(),
                BookType::L2_MBP,
                Some(*TBANK_CLIENT_ID),
                None,
                UUID4::new(),
                UnixNanos::default(),
                NonZeroUsize::new(depth),
                false,
                None,
                None,
            );
            assert!(DataClient::subscribe_book_depth10(&mut client, command).is_err());
        }

        let requests = client.restore_subscription_requests();
        assert_eq!(requests.len(), 1);
        let Some(market_data_request::Payload::SubscribeOrderBookRequest(request)) =
            &requests[0].payload
        else {
            panic!("expected order-book restore request");
        };
        assert_eq!(request.instruments[0].depth, 10);

        let command = UnsubscribeBookDepth10::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        DataClient::unsubscribe_book_depth10(&mut client, &command).unwrap();

        assert!(client.restore_subscription_requests().is_empty());
    }

    #[test]
    fn nautilus_quote_and_depth_one_share_restore_stream_until_both_unsubscribe() {
        use std::num::NonZeroUsize;

        use nautilus_core::UUID4;

        let mut client = TbankDataClient::new(TbankDataClientConfig::default());
        let quote = SubscribeQuotes::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        assert!(DataClient::subscribe_quotes(&mut client, quote).is_err());
        let depth = SubscribeBookDepth10::new(
            sber_id(),
            BookType::L2_MBP,
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            NonZeroUsize::new(1),
            false,
            None,
            None,
        );
        assert!(DataClient::subscribe_book_depth10(&mut client, depth).is_err());
        assert_eq!(client.restore_subscription_requests().len(), 1);

        let quote = UnsubscribeQuotes::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        DataClient::unsubscribe_quotes(&mut client, &quote).unwrap();
        assert_eq!(client.restore_subscription_requests().len(), 1);

        let depth = UnsubscribeBookDepth10::new(
            sber_id(),
            Some(*TBANK_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        DataClient::unsubscribe_book_depth10(&mut client, &depth).unwrap();
        assert!(client.restore_subscription_requests().is_empty());
    }

    #[tokio::test]
    async fn reconnect_without_bar_watermark_does_not_fail_recovery() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = TbankAuthInterceptor::new("test-token").unwrap();
        let mut market_data_client = TbankGrpcClients::new(channel, interceptor).market_data;
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let stream_health = MarketDataStreamHealth::default();
        let outcome = reconnect_catch_up_bars(
            RecoveryCause::Reconnect,
            "bars:group:0:1m",
            &kind,
            &mut market_data_client,
            &Arc::new(std::sync::Mutex::new(HashMap::new())),
            &HashMap::new(),
            TbankCandleTimestampMode::StartAsBarEnd,
            &sender,
            &mut HashMap::new(),
            1,
            &TbankDataClientConfig::default(),
            &HistoricalRequestLimiter::new(Duration::ZERO),
            &Arc::new(RwLock::new(HashMap::new())),
            &stream_health,
            true,
        )
        .await;

        assert_eq!(outcome.checked, 0);
        assert_eq!(outcome.backfilled, 0);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn superseded_recovery_does_not_publish_prepared_bars() {
        let bar = nautilus_bar_from_candle(
            &Candle {
                interval: SubscriptionInterval::OneMinute as i32,
                open: q(250, 0),
                high: q(252, 0),
                low: q(249, 0),
                close: q(251, 0),
                volume: 42,
                time: Some(ts(1_000)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..Candle::default()
            },
            sber_bar_type(),
            sber_market_data_metadata(),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
        )
        .unwrap();
        let stream_health = MarketDataStreamHealth::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        assert_eq!(
            publish_recovery_batch_if_current(
                &stream_health,
                "bars:generation:old:group:0",
                &sender,
                &[bar],
                |_| {},
            )
            .unwrap(),
            RecoveryPublication::Superseded
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn quote_subscriptions_are_chunked_at_broker_stream_limit() {
        let subscriptions = (0..301)
            .map(|index| {
                (
                    format!("uid-{index:03}"),
                    InstrumentId::from(format!("S{index:03}.MOEX")),
                )
            })
            .collect::<Vec<_>>();

        let groups = quote_subscription_groups(&subscriptions);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0.len(), MAX_QUOTES_PER_STREAM);
        assert_eq!(groups[0].1.len(), MAX_QUOTES_PER_STREAM);
        assert_eq!(groups[1].0.len(), 1);
        assert_eq!(groups[1].1.len(), 1);
    }

    #[tokio::test]
    async fn managed_stream_task_reports_panic_to_supervisor() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            panic!("synthetic stream panic");
        });
        let error = tasks.join_next().await.unwrap().unwrap_err();

        assert!(error.is_panic());
        assert!(error.to_string().contains("synthetic stream panic"));
    }

    #[test]
    fn pre_ack_quote_buffer_drains_after_readiness_with_canonical_precision() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let kind = TbankStreamKind::Quotes {
            instrument_ids: HashMap::from([("sber-uid".to_string(), sber_id())]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Orderbook(OrderBook {
                bids: vec![Order {
                    price: q(250, 0),
                    quantity: 2,
                }],
                asks: vec![Order {
                    price: q(251, 10_000_000),
                    quantity: 3,
                }],
                time: Some(ts(1_000)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..OrderBook::default()
            })),
            ..MarketDataResponse::default()
        };
        let mut buffered = VecDeque::from([(response, UnixNanos::from(1_000_000_000_000_u64), 0)]);
        let metadata = Arc::new(RwLock::new(HashMap::from([(
            "sber-uid".to_string(),
            sber_market_data_metadata(),
        )])));
        let watermarks = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let stream_health = MarketDataStreamHealth::default();
        stream_health.register("quotes:group:0:depth1");
        let mut attempt = 2;

        drain_pre_ack_messages(
            &mut buffered,
            &sender,
            &kind,
            &metadata,
            &watermarks,
            TbankCandleTimestampMode::StartAsBarEnd,
            "quotes:group:0:depth1",
            &stream_health,
            &HashMap::new(),
            &mut HashMap::new(),
            &mut attempt,
        );

        assert!(buffered.is_empty());
        assert_eq!(attempt, 0);
        let DataEvent::Data(Data::Quote(quote)) = receiver.try_recv().unwrap() else {
            panic!("expected buffered quote");
        };
        assert_eq!(quote.bid_price.precision, 2);
        assert_eq!(quote.ask_price.precision, 2);
    }

    #[test]
    fn stream_id_uses_ticker_class_fallback_for_moex_instrument_ids() {
        assert_eq!(instrument_stream_id(sber_id()), "SBER_TQBR");
    }

    #[test]
    fn configured_stream_id_uses_tbank_uid_for_nautilus_instrument() {
        let client = TbankDataClient::new(TbankDataClientConfig {
            instrument_stream_ids: HashMap::from([(
                "SBER_TQBR.MOEX".to_string(),
                "sber-real-uid".to_string(),
            )]),
            ..TbankDataClientConfig::default()
        });

        assert_eq!(client.stream_id(sber_id()), "sber-real-uid");
    }

    #[test]
    fn instrument_refresh_replaces_changed_and_adds_stream_routes() {
        let client = TbankDataClient::new(TbankDataClientConfig::default());
        client
            .resolved_instrument_stream_ids
            .write()
            .unwrap()
            .insert(
                "SBER_TQBR.MOEX".to_string(),
                "sber-existing-uid".to_string(),
            );

        merge_resolved_instrument_stream_ids(
            &client.resolved_instrument_stream_ids,
            HashMap::from([
                (
                    "SBER_TQBR.MOEX".to_string(),
                    "sber-refreshed-uid".to_string(),
                ),
                ("GAZP_TQBR.MOEX".to_string(), "gazp-new-uid".to_string()),
            ]),
        );

        assert_eq!(client.stream_id(sber_id()), "sber-refreshed-uid");
        assert_eq!(
            client.stream_id("GAZP_TQBR.MOEX".parse().unwrap()),
            "gazp-new-uid"
        );
    }

    #[tokio::test]
    async fn instrument_refresh_task_follows_client_lifecycle() {
        let mut disabled = TbankDataClient::new(TbankDataClientConfig {
            instrument_refresh_interval: Duration::ZERO,
            ..TbankDataClientConfig::default()
        });
        disabled.schedule_instrument_refresh();
        assert!(disabled.instrument_refresh_task.is_none());

        let mut enabled = TbankDataClient::new(TbankDataClientConfig {
            instrument_refresh_interval: Duration::from_secs(60 * 60),
            ..TbankDataClientConfig::default()
        });
        enabled.schedule_instrument_refresh();
        assert!(enabled.instrument_refresh_task.is_some());
        enabled.disconnect();
        assert!(enabled.instrument_refresh_task.is_none());
    }

    #[test]
    fn reconnect_resolution_rebuilds_runtime_streams_from_explicit_config() {
        let client = TbankDataClient::new(TbankDataClientConfig {
            instrument_stream_ids: HashMap::from([(
                "SBER_TQBR.MOEX".to_string(),
                "sber-current-uid".to_string(),
            )]),
            ..TbankDataClientConfig::default()
        });
        let future = crate::instruments::TbankMarketDataInstrumentMetadata {
            instrument_id: "Si-9.26_SPBFUT.MOEX".to_string(),
            instrument_uid: "si-current-uid".to_string(),
            lot_size: 1,
            price_precision: 0,
        };
        let share = crate::instruments::TbankMarketDataInstrumentMetadata {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            instrument_uid: "sber-current-uid".to_string(),
            lot_size: 10,
            price_precision: 2,
        };

        let (first_streams, _) = resolve_instrument_metadata(
            &client.config.instrument_stream_ids,
            &HashMap::new(),
            [&share, &future],
        )
        .unwrap();
        assert!(first_streams.contains_key("Si-9.26_SPBFUT.MOEX"));

        let (reconnected_streams, _) = resolve_instrument_metadata(
            &client.config.instrument_stream_ids,
            &HashMap::new(),
            [&share],
        )
        .unwrap();
        assert!(!reconnected_streams.contains_key("Si-9.26_SPBFUT.MOEX"));
        assert_eq!(
            reconnected_streams.get("SBER_TQBR.MOEX"),
            Some(&"sber-current-uid".to_string())
        );
    }

    #[test]
    fn instrument_metadata_resolution_is_atomic_on_configured_uid_mismatch() {
        let configured = HashMap::from([
            (
                "GAZP_TQBR.MOEX".to_string(),
                "gazp-configured-uid".to_string(),
            ),
            ("SBER_TQBR.MOEX".to_string(), "stale-sber-uid".to_string()),
        ]);
        let provider_metadata = [
            crate::instruments::TbankMarketDataInstrumentMetadata {
                instrument_id: "GAZP_TQBR.MOEX".to_string(),
                instrument_uid: "gazp-configured-uid".to_string(),
                lot_size: 10,
                price_precision: 2,
            },
            crate::instruments::TbankMarketDataInstrumentMetadata {
                instrument_id: "SBER_TQBR.MOEX".to_string(),
                instrument_uid: "sber-provider-uid".to_string(),
                lot_size: 10,
                price_precision: 2,
            },
        ];

        let error = resolve_instrument_metadata(&configured, &HashMap::new(), &provider_metadata)
            .unwrap_err();

        assert!(error.to_string().contains("stale-sber-uid"));
        assert_eq!(configured["SBER_TQBR.MOEX"], "stale-sber-uid");
    }

    #[test]
    fn configured_indicative_metadata_marks_registered_id_as_canonical() {
        let indicative_instruments = HashMap::from([(
            "IMOEX2.MOEX".to_string(),
            TbankIndicativeInstrumentConfig {
                currency: "RUB".to_string(),
                price_increment: Decimal::ONE,
            },
        )]);
        let provider_metadata = [crate::instruments::TbankMarketDataInstrumentMetadata {
            instrument_id: "IMOEX2.MOEX".to_string(),
            instrument_uid: "imoex2-uid".to_string(),
            lot_size: 1,
            price_precision: 0,
        }];

        let (_, metadata) = resolve_instrument_metadata(
            &HashMap::new(),
            &indicative_instruments,
            &provider_metadata,
        )
        .unwrap();

        assert!(metadata["IMOEX2.MOEX"].preserve_instrument_id);
        assert!(metadata["imoex2-uid"].preserve_instrument_id);
    }

    #[test]
    fn publishing_instruments_without_live_runner_is_a_noop() {
        assert!(try_get_data_event_sender().is_none());
        let provider = TbankInstrumentProvider::new(TbankDataClientConfig::default());

        publish_instrument_definitions(&provider);
    }

    #[tokio::test]
    async fn stream_message_idle_timeout_returns_timeout_outcome() {
        let result = next_stream_message_with_idle_timeout::<MarketDataResponse, _>(
            std::future::pending(),
            Duration::from_millis(1),
        )
        .await;

        assert!(matches!(result, StreamMessageOutcome::IdleTimeout));
    }

    #[tokio::test]
    async fn stream_message_success_beats_idle_timeout() {
        let response = MarketDataResponse::default();
        let result = next_stream_message_with_idle_timeout(
            async { Ok::<_, tonic::Status>(Some(response)) },
            Duration::from_secs(60),
        )
        .await;

        assert!(matches!(result, StreamMessageOutcome::Message(_)));
    }

    #[test]
    fn one_minute_history_requests_are_chunked_without_aggregate_limit() {
        let one_day = crate::market_data::candles::ONE_DAY_NANOS;
        let requests = TbankDataClient::build_1m_candle_requests(
            "sber-real-uid",
            0,
            one_day * 2 + ONE_MINUTE_NANOS,
        )
        .unwrap();

        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request.limit.is_none()));
        assert!(requests.iter().all(|request| {
            request.interval == CandleInterval::CandleInterval1Min as i32
                && request.instrument_id.as_deref() == Some("sber-real-uid")
        }));
        assert_eq!(request_from_nanos(&requests[0]), 0);
        assert_eq!(request_to_nanos(&requests[0]), one_day);
        assert_eq!(request_from_nanos(&requests[1]), one_day);
        assert_eq!(request_to_nanos(&requests[1]), one_day * 2);
        assert_eq!(request_from_nanos(&requests[2]), one_day * 2);
        assert_eq!(
            request_to_nanos(&requests[2]),
            one_day * 2 + ONE_MINUTE_NANOS
        );
    }

    fn request_from_nanos(request: &GetCandlesRequest) -> i128 {
        crate::common::time::timestamp_to_unix_nanos(request.from.as_ref().unwrap()).unwrap()
    }

    fn request_to_nanos(request: &GetCandlesRequest) -> i128 {
        crate::common::time::timestamp_to_unix_nanos(request.to.as_ref().unwrap()).unwrap()
    }

    #[test]
    fn reconnect_schedule_advances_until_usable_stream_data_resets_attempt() {
        let reconnect_policy = crate::config::TbankReconnectPolicy {
            jitter: false,
            ..crate::config::TbankReconnectPolicy::default()
        };
        let mut attempt = 12;
        let schedule = plan_next_market_data_reconnect(&reconnect_policy, 100, &mut attempt)
            .expect("reconnect should be scheduled");

        assert_eq!(attempt, 13);
        assert_eq!(schedule.attempt, 12);
        assert_eq!(schedule.delay, Duration::from_millis(30_000));
    }

    #[test]
    fn exhausted_reconnect_budget_does_not_schedule_sleep() {
        let mut attempt = 2;

        let schedule = plan_next_market_data_reconnect(
            &crate::config::TbankReconnectPolicy::default(),
            3,
            &mut attempt,
        );

        assert!(schedule.is_none());
        assert_eq!(attempt, 3);
    }

    #[test]
    fn candle_stream_message_resets_reconnect_attempt() {
        let mut attempt = 12;
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("uid0".to_string(), sber_bar_type())]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Candle(Candle {
                instrument_uid: "uid0".to_string(),
                time: Some(ts(1_700_000_000)),
                ..Default::default()
            })),
            ..MarketDataResponse::default()
        };

        let reset = reset_reconnect_attempt_if_usable(
            is_usable_market_data_response(&response, &kind),
            &mut attempt,
        );

        assert!(reset);
        assert_eq!(attempt, 0);
    }

    #[test]
    fn subscription_ack_does_not_reset_reconnect_attempt() {
        let mut attempt = 12;
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("uid0".to_string(), sber_bar_type())]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::SubscribeCandlesResponse(
                SubscribeCandlesResponse::default(),
            )),
            ..MarketDataResponse::default()
        };

        let reset = reset_reconnect_attempt_if_usable(
            is_usable_market_data_response(&response, &kind),
            &mut attempt,
        );

        assert!(is_market_data_subscription_ack(&response, &kind));
        assert!(!reset);
        assert_eq!(attempt, 12);
    }

    #[test]
    fn rejected_subscription_ack_keeps_status_without_tracking_id() {
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("uid0".to_string(), sber_bar_type())]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::SubscribeCandlesResponse(
                SubscribeCandlesResponse {
                    tracking_id: "tracking-123".to_string(),
                    candles_subscriptions: vec![CandleSubscription {
                        instrument_uid: "uid0".to_string(),
                        subscription_status: SubscriptionStatus::SourceIsInvalid as i32,
                        ..CandleSubscription::default()
                    }],
                },
            )),
            ..MarketDataResponse::default()
        };

        let error = validate_market_data_subscription_ack(&response, &kind).unwrap_err();

        assert!(!error.reason.contains("tracking-123"));
        assert!(error.reason.contains("instrument_uid=uid0"));
        assert!(
            error
                .reason
                .contains("SUBSCRIPTION_STATUS_SOURCE_IS_INVALID")
        );
        assert!(!error.retryable);
    }

    #[test]
    fn partial_subscription_ack_is_scoped_to_rejected_instruments() {
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([
                ("uid-ok".to_string(), sber_bar_type()),
                ("uid-retry".to_string(), sber_bar_type()),
                ("uid-dead".to_string(), sber_bar_type()),
            ]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::SubscribeCandlesResponse(
                SubscribeCandlesResponse {
                    tracking_id: "tracking-partial".to_string(),
                    candles_subscriptions: vec![
                        CandleSubscription {
                            instrument_uid: "uid-ok".to_string(),
                            subscription_status: SubscriptionStatus::Success as i32,
                            ..CandleSubscription::default()
                        },
                        CandleSubscription {
                            instrument_uid: "uid-retry".to_string(),
                            subscription_status: SubscriptionStatus::LimitIsExceeded as i32,
                            ..CandleSubscription::default()
                        },
                        CandleSubscription {
                            instrument_uid: "uid-dead".to_string(),
                            subscription_status: SubscriptionStatus::SourceIsInvalid as i32,
                            ..CandleSubscription::default()
                        },
                    ],
                },
            )),
            ..MarketDataResponse::default()
        };

        let rejection = validate_market_data_subscription_ack(&response, &kind).unwrap_err();

        assert!(rejection.is_partial());
        assert_eq!(rejection.acknowledged_count, 3);
        assert_eq!(rejection.failures.len(), 2);
        assert_eq!(rejection.failures[0].instrument_uid, "uid-retry");
        assert!(rejection.failures[0].retryable);
        assert_eq!(rejection.failures[1].instrument_uid, "uid-dead");
        assert!(!rejection.failures[1].retryable);
        assert!(!rejection.retryable);
    }

    #[test]
    fn permanent_partial_bar_rejection_keeps_group_degraded_and_fails_readiness() {
        let task_key = "bars:generation:1:group:0:1m";
        let readiness_key = format!("{task_key}:instrument:uid-dead");
        let health = MarketDataStreamHealth::default();
        health.register(task_key);
        health.register_current(&readiness_key);
        health.mark_operational(task_key);
        let mut events = crate::market_data::subscribe_market_data_events();
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("uid-dead".to_string(), sber_bar_type())]),
        };
        let rejection = SubscriptionAckRejection {
            reason: "permanent rejection".to_string(),
            retryable: false,
            acknowledged_count: 2,
            failures: vec![SubscriptionFailure {
                instrument_uid: "uid-dead".to_string(),
                reason: "source is invalid".to_string(),
                retryable: false,
            }],
        };

        assert!(mark_permanent_bar_rejections(
            task_key,
            &kind,
            &rejection,
            &HashMap::new(),
            &health,
        ));
        assert!(!health.is_operational());

        let event = loop {
            match events.try_recv() {
                Ok(TbankMarketDataEvent::CandleReadiness {
                    readiness_id,
                    state,
                    instrument_uid,
                    ..
                }) if readiness_id == "bars:group:0:1m:instrument:uid-dead" => {
                    break (state, instrument_uid);
                }
                Ok(_) => continue,
                Err(error) => panic!("permanent rejection readiness event missing: {error:?}"),
            }
        };
        assert_eq!(event.0, TbankCandleReadinessState::Failed);
        assert_eq!(event.1, "uid-dead");
    }

    #[test]
    fn retryable_partial_candle_failure_gets_an_isolated_stream_subscription() {
        let bar_type = sber_bar_type();

        let (task_key, request, kind, continuity_keys) =
            isolated_retryable_bar_stream("bars:group:0:1m", "uid-retry", bar_type);

        assert_eq!(task_key, "bars:group:0:1m:retry:uid-retry");
        let candles = request.subscribe_candles_request.unwrap();
        assert_eq!(candles.instruments.len(), 1);
        assert_eq!(candles.instruments[0].instrument_id, "uid-retry");
        assert_eq!(
            candles.candle_source_type,
            Some(get_candles_request::CandleSource::Exchange as i32)
        );
        let TbankStreamKind::Bars { bar_types } = kind else {
            panic!("expected isolated bars stream");
        };
        assert_eq!(bar_types.get("uid-retry"), Some(&bar_type));
        assert_eq!(
            continuity_keys.get("uid-retry").map(String::as_str),
            Some("bars:group:0:1m:instrument:uid-retry")
        );
    }

    #[tokio::test]
    async fn isolated_subscription_owner_waits_for_children_before_normal_exit() {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_by_task = completed.clone();
        let mut tasks = AbortTasksOnDrop::default();
        tasks.push(tokio::spawn(async move {
            tokio::task::yield_now().await;
            completed_by_task.store(true, std::sync::atomic::Ordering::Release);
        }));

        tasks.wait_for_completion().await;

        assert!(completed.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn isolated_subscription_owner_wait_clears_health_key() {
        let health = Arc::new(MarketDataStreamHealth::default());
        let task_key = "bars:group:0:1m:retry:uid-completed".to_string();
        health.register(&task_key);

        let mut tasks = AbortTasksOnDrop::default();
        tasks.push_with_health_cleanup(tokio::spawn(async {}), health.clone(), task_key);
        tasks.wait_for_completion().await;

        assert!(health.is_operational());
    }

    #[tokio::test]
    async fn isolated_subscription_child_survives_parent_reconnect_budget_exhaustion() {
        let health = Arc::new(MarketDataStreamHealth::default());
        let parent_key = "bars:group:0:1m";
        let child_key = "bars:group:0:1m:retry:uid-retry";
        health.register(parent_key);
        health.register(child_key);

        let child_reconnected = Arc::new(tokio::sync::Notify::new());
        let child_reconnected_task = child_reconnected.clone();
        let child_health = health.clone();
        let child = tokio::spawn(async move {
            child_reconnected_task.notified().await;
            child_health.mark_operational(child_key);
        });
        let mut isolated_tasks = AbortTasksOnDrop::default();
        isolated_tasks.push(child);

        // Parent budget exhaustion re-arms only the parent probe. The child supervisor must
        // remain alive long enough to acknowledge its own subscription and clear its health key.
        health.mark_reconnecting(parent_key);
        child_reconnected.notify_one();
        health.mark_operational(parent_key);
        tokio::task::yield_now().await;

        assert!(health.is_operational());
    }

    #[tokio::test]
    async fn aborting_isolated_subscription_owner_aborts_awaited_child() {
        struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
        let child = tokio::spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped_sender));
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        let owner = tokio::spawn(async move {
            let mut tasks = AbortTasksOnDrop::default();
            tasks.push(child);
            tasks.wait_for_completion().await;
        });
        tokio::task::yield_now().await;

        owner.abort();
        let _ = owner.await;

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("isolated child was not aborted with its owner")
            .expect("isolated child drop notification was lost");
    }

    #[tokio::test]
    async fn aborting_isolated_subscription_child_clears_health_key() {
        let health = Arc::new(MarketDataStreamHealth::default());
        let task_key = "bars:group:0:1m:retry:uid-retry".to_string();
        health.register(&task_key);

        let child = tokio::spawn(std::future::pending::<()>());
        let mut tasks = AbortTasksOnDrop::default();
        tasks.push_with_health_cleanup(child, health.clone(), task_key);

        drop(tasks);

        assert!(health.is_operational());
    }

    #[test]
    fn trade_subscription_ack_ignores_other_instruments_failures() {
        let kind = TbankStreamKind::Trades {
            instrument_id: "SBER_TQBR.MOEX".parse().unwrap(),
            instrument_uid: "uid-ok".to_string(),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::SubscribeTradesResponse(
                SubscribeTradesResponse {
                    tracking_id: "tracking-trades".to_string(),
                    trade_subscriptions: vec![
                        TradeSubscription {
                            instrument_uid: "uid-ok".to_string(),
                            subscription_status: SubscriptionStatus::Success as i32,
                            ..TradeSubscription::default()
                        },
                        TradeSubscription {
                            instrument_uid: "uid-dead".to_string(),
                            subscription_status: SubscriptionStatus::SourceIsInvalid as i32,
                            ..TradeSubscription::default()
                        },
                    ],
                    ..SubscribeTradesResponse::default()
                },
            )),
            ..MarketDataResponse::default()
        };

        assert!(validate_market_data_subscription_ack(&response, &kind).is_ok());
    }

    #[test]
    fn all_failed_mixed_ack_requires_retryability_partition() {
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([
                ("uid-retry".to_string(), sber_bar_type()),
                ("uid-dead".to_string(), sber_bar_type()),
            ]),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::SubscribeCandlesResponse(
                SubscribeCandlesResponse {
                    tracking_id: "tracking-mixed".to_string(),
                    candles_subscriptions: vec![
                        CandleSubscription {
                            instrument_uid: "uid-retry".to_string(),
                            subscription_status: SubscriptionStatus::TooManyRequests as i32,
                            ..CandleSubscription::default()
                        },
                        CandleSubscription {
                            instrument_uid: "uid-dead".to_string(),
                            subscription_status: SubscriptionStatus::SourceIsInvalid as i32,
                            ..CandleSubscription::default()
                        },
                    ],
                },
            )),
            ..MarketDataResponse::default()
        };

        let rejection = validate_market_data_subscription_ack(&response, &kind).unwrap_err();

        assert!(!rejection.is_partial());
        assert!(rejection.has_mixed_retryability());
    }

    #[test]
    fn transient_subscription_ack_remains_retryable() {
        let failure =
            subscription_failure("uid0", SubscriptionStatus::TooManyRequests as i32).unwrap();

        assert!(failure.retryable);
        assert!(
            failure
                .reason
                .contains("SUBSCRIPTION_STATUS_TOO_MANY_REQUESTS")
        );
    }

    #[test]
    fn latest_closed_minute_bar_ts_event_rounds_down_to_minute() {
        let now = Utc
            .with_ymd_and_hms(2026, 5, 21, 14, 48, 37)
            .single()
            .unwrap()
            + chrono::Duration::milliseconds(123);

        assert_eq!(
            latest_closed_minute_bar_ts_event(now),
            1_779_374_880_000_000_000
        );
    }

    #[test]
    fn indicative_poll_continuity_key_is_instrument_scoped() {
        assert_eq!(
            periodic_candle_stream_key(7, "imoex2-uid"),
            "bars:generation:7:poll:indicative:1m:instrument:imoex2-uid"
        );
    }

    #[test]
    fn indicative_poll_initializes_cursor_once() {
        let mut next_from = HashMap::new();
        let bar_type = sber_bar_type();

        assert_eq!(periodic_candle_poll_from(&mut next_from, bar_type, 60), 60);
        assert_eq!(periodic_candle_poll_from(&mut next_from, bar_type, 120), 60);
    }

    #[test]
    fn stream_task_key_includes_stream_id_and_stream_shape() {
        assert_eq!(
            stream_task_key("bars", "SBER_TQBR", "1m"),
            "bars:SBER_TQBR:1m"
        );
        assert_eq!(
            stream_task_key("depth10", "SBER_TQBR", "book"),
            "depth10:SBER_TQBR:book"
        );
    }

    #[test]
    fn stream_candle_maps_to_nautilus_bar() {
        let candle = Candle {
            interval: SubscriptionInterval::OneMinute as i32,
            open: q(250, 0),
            high: q(252, 0),
            low: q(249, 0),
            close: q(251, 0),
            volume: 42,
            time: Some(ts(1_000)),
            instrument_uid: "uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..Candle::default()
        };

        let bar = nautilus_bar_from_candle(
            &candle,
            sber_bar_type(),
            sber_market_data_metadata(),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
        )
        .unwrap();

        assert_eq!(bar.bar_type.instrument_id(), sber_id());
        assert_eq!(bar.close.as_f64(), 251.0);
        assert_eq!(bar.volume.as_f64(), 420.0);
        assert_eq!(bar.ts_event.as_u64(), 1_060_000_000_000);
        assert_eq!(bar.ts_init.as_u64(), 1_070_000_000_000);
    }

    #[test]
    fn batched_stream_candle_routes_by_instrument_uid() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let candle = Candle {
            interval: SubscriptionInterval::OneMinute as i32,
            open: q(250, 0),
            high: q(252, 0),
            low: q(249, 0),
            close: q(251, 0),
            volume: 42,
            time: Some(ts(1_000)),
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..Candle::default()
        };
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };

        publish_market_data_response(
            &sender,
            MarketDataResponse {
                payload: Some(market_data_response::Payload::Candle(candle)),
                ..MarketDataResponse::default()
            },
            &kind,
            &Arc::new(RwLock::new(HashMap::from([(
                "sber-uid".to_string(),
                sber_market_data_metadata(),
            )]))),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
            0,
        )
        .unwrap();

        assert!(matches!(
            receiver.try_recv().unwrap(),
            DataEvent::Data(Data::Bar(_))
        ));
    }

    #[test]
    fn batched_stream_rejects_unknown_candle_uid() {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let candle = Candle {
            interval: SubscriptionInterval::OneMinute as i32,
            close: q(251, 0),
            volume: 42,
            time: Some(ts(1_000)),
            instrument_uid: "unknown-uid".to_string(),
            ..Candle::default()
        };
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };

        let error = publish_market_data_response(
            &sender,
            MarketDataResponse {
                payload: Some(market_data_response::Payload::Candle(candle)),
                ..MarketDataResponse::default()
            },
            &kind,
            &Arc::new(RwLock::new(HashMap::new())),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
            0,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown-uid"));
    }

    #[test]
    fn sparse_live_bars_are_published_without_gap_recovery() {
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };
        let mut continuity = HashMap::new();
        let watermarks = Arc::new(std::sync::Mutex::new(HashMap::new()));

        let first = MarketDataResponse {
            payload: Some(market_data_response::Payload::Candle(Candle {
                interval: SubscriptionInterval::OneMinute as i32,
                open: q(250, 0),
                high: q(252, 0),
                low: q(249, 0),
                close: q(251, 0),
                volume: 42,
                time: Some(ts(1_000)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..Candle::default()
            })),
            ..MarketDataResponse::default()
        };
        let after_gap = MarketDataResponse {
            payload: Some(market_data_response::Payload::Candle(Candle {
                interval: SubscriptionInterval::OneMinute as i32,
                open: q(253, 0),
                high: q(255, 0),
                low: q(252, 0),
                close: q(254, 0),
                volume: 43,
                time: Some(ts(1_120)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..Candle::default()
            })),
            ..MarketDataResponse::default()
        };

        let (_, first_pending) = filter_market_data_response_for_continuity(
            first,
            &kind,
            "bars:group:0:1m",
            &HashMap::new(),
            TbankCandleTimestampMode::StartAsBarEnd,
            &watermarks,
            &continuity,
        )
        .expect("first live candle should be accepted");
        let first_pending = first_pending.expect("first live candle should have a commit");
        assert!(first_pending.establishes_initial_baseline);
        commit_live_bar(
            &watermarks,
            &mut continuity,
            &first_pending.instrument_uid,
            first_pending.bar_type,
            first_pending.ts_event,
        );

        let (_, after_gap_pending) = filter_market_data_response_for_continuity(
            after_gap,
            &kind,
            "bars:group:0:1m",
            &HashMap::new(),
            TbankCandleTimestampMode::StartAsBarEnd,
            &watermarks,
            &continuity,
        )
        .expect("sparse live candle should be accepted");
        let after_gap_pending = after_gap_pending.expect("sparse live candle should have a commit");
        assert!(!after_gap_pending.establishes_initial_baseline);
        commit_live_bar(
            &watermarks,
            &mut continuity,
            &after_gap_pending.instrument_uid,
            after_gap_pending.bar_type,
            after_gap_pending.ts_event,
        );
        let tracker = continuity.get("sber-uid").unwrap();
        assert_eq!(tracker.latest_seen(), Some(1_180_000_000_000));
    }

    #[test]
    fn failed_live_bar_publication_does_not_advance_cursors() {
        let task_key = "bars:test:failed-live:1m";
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        drop(receiver);
        let metadata = Arc::new(RwLock::new(HashMap::from([(
            "sber-uid".to_string(),
            sber_market_data_metadata(),
        )])));
        let stream_health = MarketDataStreamHealth::default();
        stream_health.register(task_key);
        let watermarks = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut continuity = HashMap::new();
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Candle(Candle {
                interval: SubscriptionInterval::OneMinute as i32,
                open: q(250, 0),
                high: q(252, 0),
                low: q(249, 0),
                close: q(251, 0),
                volume: 42,
                time: Some(ts(1_000)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..Candle::default()
            })),
            ..MarketDataResponse::default()
        };

        publish_ready_market_data_response(
            &sender,
            response,
            &kind,
            &metadata,
            &watermarks,
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
            task_key,
            &stream_health,
            &HashMap::new(),
            &mut continuity,
            &mut 0,
            0,
        );

        assert!(continuity.is_empty());
        assert!(snapshot_bar_watermarks(&watermarks).is_empty());
    }

    #[test]
    fn first_acknowledged_live_candle_establishes_readiness_without_watermark() {
        let task_key = "bars:test:first-live:1m";
        let readiness_key = format!("{task_key}:instrument:sber-uid");
        let kind = TbankStreamKind::Bars {
            bar_types: HashMap::from([("sber-uid".to_string(), sber_bar_type())]),
        };
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let metadata = Arc::new(RwLock::new(HashMap::from([(
            "sber-uid".to_string(),
            sber_market_data_metadata(),
        )])));
        let stream_health = MarketDataStreamHealth::default();
        stream_health.register(task_key);
        stream_health.mark_operational(task_key);
        stream_health.register_current(&readiness_key);
        let mut events = crate::market_data::subscribe_market_data_events();
        let mut continuity = HashMap::new();
        let watermarks = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Candle(Candle {
                interval: SubscriptionInterval::OneMinute as i32,
                open: q(250, 0),
                high: q(252, 0),
                low: q(249, 0),
                close: q(251, 0),
                volume: 42,
                time: Some(ts(1_000)),
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..Candle::default()
            })),
            ..MarketDataResponse::default()
        };
        publish_ready_market_data_response(
            &sender,
            response,
            &kind,
            &metadata,
            &watermarks,
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(1_070_000_000_000_u64),
            task_key,
            &stream_health,
            &HashMap::new(),
            &mut continuity,
            &mut 0,
            0,
        );

        assert_eq!(
            snapshot_bar_watermarks(&watermarks)[&sber_bar_type()],
            1_060_000_000_000
        );

        let event = loop {
            match events.try_recv() {
                Ok(TbankMarketDataEvent::CandleReadiness {
                    readiness_id,
                    state,
                    ready_through,
                    ..
                }) if readiness_id == "bars:test:first-live:1m:instrument:sber-uid" => {
                    break (state, ready_through);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    panic!("initial live candle readiness event missing")
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    panic!("candle readiness event channel closed")
                }
            }
        };

        assert_eq!(event.0, TbankCandleReadinessState::Ready);
        assert!(event.1.is_some());
        assert_eq!(
            snapshot_bar_watermarks(&watermarks)[&sber_bar_type()],
            1_060_000_000_000
        );
    }

    #[test]
    fn stream_trade_maps_side_and_instrument() {
        let trade = Trade {
            direction: TradeDirection::Sell as i32,
            price: q(251, 0),
            quantity: 3,
            time: Some(ts(1_000)),
            instrument_uid: "uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..Trade::default()
        };

        let received_at = UnixNanos::from(2_000_000_000_u64);
        let tick = nautilus_trade_from_tbank(
            &trade,
            sber_id(),
            sber_market_data_metadata(),
            received_at,
            0,
        )
        .unwrap();

        assert_eq!(tick.instrument_id, sber_id());
        assert_eq!(tick.aggressor_side, AggressorSide::Seller);
        assert_eq!(tick.price.as_f64(), 251.0);
        assert_eq!(tick.size.as_f64(), 30.0);
        assert_eq!(tick.ts_init, received_at);

        let replay = nautilus_trade_from_tbank(
            &trade,
            sber_id(),
            sber_market_data_metadata(),
            received_at,
            0,
        )
        .unwrap();
        let same_timestamp = nautilus_trade_from_tbank(
            &trade,
            sber_id(),
            sber_market_data_metadata(),
            received_at,
            1,
        )
        .unwrap();
        assert_eq!(tick.trade_id, replay.trade_id);
        assert_ne!(tick.trade_id, same_timestamp.trade_id);
        assert_eq!(tick.trade_id.as_str().len(), 36);
    }

    #[test]
    fn single_instrument_trade_stream_rejects_mismatched_uid() {
        let kind = TbankStreamKind::Trades {
            instrument_id: sber_id(),
            instrument_uid: "expected-uid".to_string(),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Trade(Trade {
                instrument_uid: "other-uid".to_string(),
                ..Trade::default()
            })),
            ..MarketDataResponse::default()
        };
        assert!(!is_usable_market_data_response(&response, &kind));

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let error = publish_market_data_response(
            &sender,
            response,
            &kind,
            &Arc::new(RwLock::new(HashMap::new())),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(2_000_000_000_u64),
            0,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unexpected T-Bank stream instrument")
        );
    }

    #[test]
    fn stream_orderbook_maps_quote_and_depth10() {
        let orderbook = OrderBook {
            depth: 2,
            is_consistent: true,
            bids: vec![
                Order {
                    price: q(250, 0),
                    quantity: 10,
                },
                Order {
                    price: q(249, 0),
                    quantity: 8,
                },
            ],
            asks: vec![
                Order {
                    price: q(251, 10_000_000),
                    quantity: 7,
                },
                Order {
                    price: q(252, 0),
                    quantity: 9,
                },
            ],
            time: Some(ts(1_000)),
            instrument_uid: "uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..OrderBook::default()
        };

        let received_at = UnixNanos::from(2_000_000_000_u64);
        let quote = nautilus_quote_from_orderbook(
            &orderbook,
            sber_id(),
            sber_market_data_metadata(),
            received_at,
        )
        .unwrap()
        .unwrap();
        assert_eq!(quote.bid_price.as_f64(), 250.0);
        assert_eq!(quote.ask_price.as_f64(), 251.01);
        assert_eq!(quote.bid_price.precision, 2);
        assert_eq!(quote.ask_price.precision, 2);
        assert_eq!(quote.ts_init, received_at);

        let depth = nautilus_depth10_from_orderbook(
            &orderbook,
            sber_id(),
            sber_market_data_metadata(),
            received_at,
        )
        .unwrap();
        assert_eq!(depth.instrument_id, sber_id());
        assert_eq!(depth.bids[0].price.as_f64(), 250.0);
        assert_eq!(depth.asks[1].size.as_f64(), 90.0);
        assert_eq!(depth.bid_counts[0], 1);
        assert_eq!(depth.ts_init, received_at);
        assert_eq!(depth.ask_counts[1], 1);
    }

    #[test]
    fn single_instrument_depth10_stream_rejects_mismatched_uid() {
        let kind = TbankStreamKind::Depth10 {
            instrument_id: sber_id(),
            instrument_uid: "expected-uid".to_string(),
        };
        let response = MarketDataResponse {
            payload: Some(market_data_response::Payload::Orderbook(OrderBook {
                instrument_uid: "other-uid".to_string(),
                ..OrderBook::default()
            })),
            ..MarketDataResponse::default()
        };
        assert!(!is_usable_market_data_response(&response, &kind));

        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let error = publish_market_data_response(
            &sender,
            response,
            &kind,
            &Arc::new(RwLock::new(HashMap::new())),
            TbankCandleTimestampMode::StartAsBarEnd,
            UnixNanos::from(2_000_000_000_u64),
            0,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unexpected T-Bank stream instrument")
        );
    }

    #[test]
    fn configured_indicative_events_keep_registered_id() {
        let instrument_id: InstrumentId = "IMOEX2.MOEX".parse().unwrap();
        let metadata = MarketDataInstrumentMetadata {
            lot_size: 1,
            price_precision: 0,
            preserve_instrument_id: true,
        };
        let trade = Trade {
            direction: TradeDirection::Buy as i32,
            price: q(3_000, 0),
            quantity: 1,
            time: Some(ts(1_000)),
            instrument_uid: "imoex2-uid".to_string(),
            ticker: "IMOEX2".to_string(),
            class_code: "INDEX".to_string(),
            ..Trade::default()
        };
        let orderbook = OrderBook {
            bids: vec![Order {
                price: q(2_999, 0),
                quantity: 1,
            }],
            asks: vec![Order {
                price: q(3_001, 0),
                quantity: 1,
            }],
            time: Some(ts(1_000)),
            instrument_uid: "imoex2-uid".to_string(),
            ticker: "IMOEX2".to_string(),
            class_code: "INDEX".to_string(),
            ..OrderBook::default()
        };
        let received_at = UnixNanos::from(2_000_000_000_u64);

        let tick =
            nautilus_trade_from_tbank(&trade, instrument_id, metadata, received_at, 0).unwrap();
        let quote = nautilus_quote_from_orderbook(&orderbook, instrument_id, metadata, received_at)
            .unwrap()
            .unwrap();
        let depth =
            nautilus_depth10_from_orderbook(&orderbook, instrument_id, metadata, received_at)
                .unwrap();

        assert_eq!(tick.instrument_id, instrument_id);
        assert_eq!(quote.instrument_id, instrument_id);
        assert_eq!(depth.instrument_id, instrument_id);
    }

    #[test]
    fn one_sided_orderbook_is_skipped_without_error() {
        let mut orderbook = OrderBook {
            depth: 1,
            is_consistent: true,
            bids: vec![Order {
                price: q(250, 0),
                quantity: 10,
            }],
            asks: Vec::new(),
            time: Some(ts(1_000)),
            instrument_uid: "uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..OrderBook::default()
        };

        assert!(
            nautilus_quote_from_orderbook(
                &orderbook,
                sber_id(),
                sber_market_data_metadata(),
                UnixNanos::from(2_000_000_000_u64),
            )
            .unwrap()
            .is_none()
        );

        orderbook.asks = orderbook.bids.clone();
        orderbook.bids.clear();
        assert!(
            nautilus_quote_from_orderbook(
                &orderbook,
                sber_id(),
                sber_market_data_metadata(),
                UnixNanos::from(2_000_000_000_u64),
            )
            .unwrap()
            .is_none()
        );
    }
}
