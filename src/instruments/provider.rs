use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};

use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_model::identifiers::InstrumentId;

use crate::{
    common::{
        TbankAdapterError, TbankInstrumentIdParts,
        consts::{SPBFUT_CLASS_CODE, TQBR_CLASS_CODE},
        venue::TbankVenue,
    },
    config::{TbankDataClientConfig, TbankReconnectPolicy},
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients, connect_channel,
        generated::{
            GetFuturesMarginRequest, IndicativeResponse, IndicativesRequest, InstrumentIdType,
            InstrumentRequest, InstrumentStatus, InstrumentsRequest,
        },
        with_timeout,
    },
    instruments::{
        TbankInstrumentMapper, TbankInstrumentMetadata, TbankMarketDataInstrumentMetadata,
        build_equity_instrument, build_futures_instrument, build_index_instrument,
    },
};

const FUTURES_MARGIN_MAX_RETRIES: u32 = 2;

/// Loads selected T-Bank definitions into NautilusTrader's canonical instrument store.
#[derive(Debug)]
pub struct TbankInstrumentProvider {
    config: TbankDataClientConfig,
    store: InstrumentStore,
    mapper: TbankInstrumentMapper,
    market_data_metadata: HashMap<String, TbankMarketDataInstrumentMetadata>,
    unresolved_futures: HashSet<String>,
}

impl TbankInstrumentProvider {
    /// Creates a new instance.
    #[must_use]
    pub fn new(config: TbankDataClientConfig) -> Self {
        Self {
            config,
            store: InstrumentStore::new(),
            mapper: TbankInstrumentMapper::new(),
            market_data_metadata: HashMap::new(),
            unresolved_futures: HashSet::new(),
        }
    }

    /// Returns the instrument mapper maintained by the provider.
    #[must_use]
    pub const fn mapper(&self) -> &TbankInstrumentMapper {
        &self.mapper
    }

    /// Iterates over market-data metadata for tradable and indicative instruments.
    pub fn market_data_metadata(&self) -> impl Iterator<Item = &TbankMarketDataInstrumentMetadata> {
        self.market_data_metadata.values()
    }

    /// Returns futures omitted from the last bulk load because current margin metadata was unavailable.
    pub fn unresolved_futures(&self) -> impl Iterator<Item = &str> {
        self.unresolved_futures.iter().map(String::as_str)
    }

    /// Rejects configured futures whose current margin contract was not resolved during loading.
    ///
    /// Bulk discovery may omit an individual future when its session-dependent contract cannot be
    /// loaded, but a configured market-data stream must never be accepted without the metadata
    /// needed to decode its events.
    pub fn ensure_configured_futures_resolved(
        &self,
        configured_stream_ids: &HashMap<String, String>,
    ) -> crate::common::Result<()> {
        for instrument_id in configured_stream_ids.keys() {
            let is_futures = TbankInstrumentIdParts::from_str(instrument_id)
                .is_ok_and(|parts| parts.is_moex_futures());
            if !is_futures {
                continue;
            }
            if self.unresolved_futures.contains(instrument_id)
                || !self.market_data_metadata.contains_key(instrument_id)
            {
                return Err(TbankAdapterError::FuturesMarginUnresolved(
                    instrument_id.clone(),
                ));
            }
        }
        Ok(())
    }

    async fn clients(&self) -> anyhow::Result<TbankGrpcClients<TbankAuthInterceptor>> {
        self.config.validate()?;
        let endpoint = self.config.endpoint_uri()?;
        let token = self.config.resolve_token_secret()?;
        let channel = connect_channel(&endpoint, self.config.request_timeout).await?;
        let interceptor = TbankAuthInterceptor::new(&token)?;
        Ok(TbankGrpcClients::new(channel, interceptor))
    }

    fn add_share(&mut self, share: &crate::grpc::generated::Share) -> anyhow::Result<()> {
        let metadata = TbankInstrumentMetadata::from_share(share)?;
        if !metadata.is_supported() {
            return Ok(());
        }
        self.add_share_metadata(metadata)
    }

    fn add_share_metadata(&mut self, metadata: TbankInstrumentMetadata) -> anyhow::Result<()> {
        let instrument = build_equity_instrument(&metadata)?;
        let market_data_metadata = TbankMarketDataInstrumentMetadata {
            instrument_id: metadata.instrument_id.clone(),
            instrument_uid: metadata.instrument_uid.clone(),
            lot_size: metadata.lot,
            price_precision: u8::try_from(metadata.price_precision)?,
        };
        self.mapper.insert(metadata)?;
        self.market_data_metadata.insert(
            market_data_metadata.instrument_id.clone(),
            market_data_metadata,
        );
        self.store.add(instrument);
        Ok(())
    }

    fn add_future_metadata(&mut self, metadata: TbankInstrumentMetadata) -> anyhow::Result<()> {
        let instrument_id = metadata.instrument_id.clone();
        let instrument = build_futures_instrument(&metadata)?;
        let market_data_metadata = TbankMarketDataInstrumentMetadata {
            instrument_id: metadata.instrument_id.clone(),
            instrument_uid: metadata.instrument_uid.clone(),
            lot_size: metadata.lot,
            price_precision: u8::try_from(metadata.price_precision)?,
        };
        self.mapper.insert(metadata)?;
        self.market_data_metadata.insert(
            market_data_metadata.instrument_id.clone(),
            market_data_metadata,
        );
        self.store.add(instrument);
        self.unresolved_futures.remove(&instrument_id);
        Ok(())
    }

    /// Resolves the session-dependent futures contract before the instrument is built.
    async fn resolve_current_futures_metadata(
        clients: &mut TbankGrpcClients<TbankAuthInterceptor>,
        mut metadata: TbankInstrumentMetadata,
        request_timeout: Duration,
        retry_policy: &TbankReconnectPolicy,
    ) -> anyhow::Result<TbankInstrumentMetadata> {
        let instrument_id = metadata.futures_margin_instrument_id()?;
        let request = GetFuturesMarginRequest {
            #[allow(deprecated)]
            figi: String::new(),
            instrument_id: instrument_id.clone(),
        };
        let mut attempt = 0;
        let response = loop {
            match clients
                .instruments
                .get_futures_margin(with_timeout(request.clone(), request_timeout))
                .await
            {
                Ok(response) => break response.into_inner(),
                Err(status)
                    if crate::grpc::retry::is_transient_status(status.code())
                        && attempt < FUTURES_MARGIN_MAX_RETRIES =>
                {
                    let delay = crate::grpc::retry::backoff_duration(retry_policy, attempt);
                    attempt += 1;
                    tracing::warn!(
                        %instrument_id,
                        error = %status,
                        attempt,
                        retries = FUTURES_MARGIN_MAX_RETRIES,
                        delay_ms = delay.as_millis(),
                        "retrying T-Bank GetFuturesMargin request"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(status) => return Err(TbankAdapterError::from(status).into()),
            }
        };
        metadata.update_futures_margin_contract(&response)?;
        Ok(metadata)
    }

    fn add_indicative(
        &mut self,
        instrument_id: &str,
        indicative: &IndicativeResponse,
    ) -> anyhow::Result<()> {
        let definition = self
            .config
            .indicative_instruments
            .get(instrument_id)
            .ok_or_else(|| anyhow::anyhow!("missing indicative definition for {instrument_id}"))?;
        let currency = crate::common::currency::currency_from_code(&definition.currency)?;
        let price_increment = definition.price_increment;
        let instrument =
            build_index_instrument(instrument_id, indicative, currency, price_increment)?;
        let metadata = TbankMarketDataInstrumentMetadata {
            instrument_id: instrument_id.to_string(),
            instrument_uid: indicative.uid.clone(),
            lot_size: 1,
            price_precision: u8::try_from(price_increment.normalize().scale())?,
        };
        self.market_data_metadata
            .insert(metadata.instrument_id.clone(), metadata);
        self.store.add(instrument);
        Ok(())
    }

    /// Replaces the published catalog after a complete replacement has loaded successfully.
    fn commit_catalog(&mut self, staged: Self) {
        self.store = staged.store;
        self.mapper = staged.mapper;
        self.market_data_metadata = staged.market_data_metadata;
        self.unresolved_futures = staged.unresolved_futures;
    }

    fn indicative_ticker(instrument_id: &str) -> &str {
        instrument_id.strip_suffix(".MOEX").unwrap_or(instrument_id)
    }

    fn is_supported_share_family(share: &crate::grpc::generated::Share) -> bool {
        let Ok(venue) = crate::instruments::mapper::venue_from_real_exchange(
            share.real_exchange,
            &share.ticker,
        ) else {
            return false;
        };
        match venue {
            TbankVenue::Moex => share.class_code.eq_ignore_ascii_case(TQBR_CLASS_CODE),
            TbankVenue::Spbe => !share.class_code.eq_ignore_ascii_case(SPBFUT_CLASS_CODE),
        }
    }

    fn is_supported_future_family(future: &crate::grpc::generated::Future) -> bool {
        crate::instruments::mapper::venue_from_real_exchange(future.real_exchange, &future.ticker)
            .is_ok_and(|venue| {
                venue == TbankVenue::Moex
                    && future.class_code.eq_ignore_ascii_case(SPBFUT_CLASS_CODE)
            })
    }

    fn catalog_share_metadata(
        share: &crate::grpc::generated::Share,
    ) -> anyhow::Result<Option<TbankInstrumentMetadata>> {
        if !Self::is_supported_share_family(share) {
            return Ok(None);
        }
        match TbankInstrumentMetadata::from_share(share) {
            Ok(metadata) if metadata.is_supported() => Ok(Some(metadata)),
            Ok(_) => Ok(None),
            Err(
                TbankAdapterError::UnsupportedInstrument(reason)
                | TbankAdapterError::InstrumentOutOfScope(reason),
            ) => {
                tracing::warn!(
                    ticker = %share.ticker,
                    class_code = %share.class_code,
                    %reason,
                    "skipping unsupported T-Bank share definition"
                );
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn catalog_future_metadata(
        future: &crate::grpc::generated::Future,
    ) -> anyhow::Result<Option<TbankInstrumentMetadata>> {
        if !Self::is_supported_future_family(future) {
            return Ok(None);
        }
        match TbankInstrumentMetadata::from_future(future) {
            Ok(metadata) if metadata.is_supported() => {
                if metadata.conservative_initial_margin_rate().is_none() {
                    tracing::warn!(
                        ticker = %future.ticker,
                        "skipping supported T-Bank futures definition without both positive initial-margin risk rates"
                    );
                    return Ok(None);
                }
                Ok(Some(metadata))
            }
            Ok(_) => Ok(None),
            Err(
                TbankAdapterError::UnsupportedInstrument(reason)
                | TbankAdapterError::InstrumentOutOfScope(reason),
            ) => {
                tracing::warn!(
                    ticker = %future.ticker,
                    class_code = %future.class_code,
                    %reason,
                    "skipping unsupported T-Bank futures definition"
                );
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn matches_indicative(indicative: &IndicativeResponse, instrument_id: &str) -> bool {
        indicative
            .ticker
            .eq_ignore_ascii_case(Self::indicative_ticker(instrument_id))
    }

    fn matches_indicative_filters(
        indicative: Option<&IndicativeResponse>,
        instrument_id: &str,
        definition: &crate::config::TbankIndicativeInstrumentConfig,
        filters: Option<&HashMap<String, String>>,
    ) -> bool {
        let Some(filters) = filters else {
            return true;
        };
        let venue = instrument_id
            .rsplit_once('.')
            .map_or(instrument_id, |(_, venue)| venue);
        filters.iter().all(|(key, value)| match key.as_str() {
            // The broker response is required only for class-code filtering. Keeping this
            // predicate true before the RPC lets filters such as `instrument_type=futures`
            // exclude configured indicatives without requiring their endpoint to be available.
            "class_code" => {
                indicative.is_none_or(|value_| value_.class_code.eq_ignore_ascii_case(value))
            }
            "currency" => definition.currency.eq_ignore_ascii_case(value),
            "venue" => venue.eq_ignore_ascii_case(value),
            "instrument_type" => value.eq_ignore_ascii_case("index"),
            _ => false,
        })
    }

    fn configured_indicatives_for_filters(
        configured: &HashMap<String, crate::config::TbankIndicativeInstrumentConfig>,
        filters: Option<&HashMap<String, String>>,
    ) -> Vec<String> {
        configured
            .iter()
            .filter(|(instrument_id, definition)| {
                Self::matches_indicative_filters(None, instrument_id, definition, filters)
            })
            .map(|(instrument_id, _)| instrument_id.clone())
            .collect()
    }

    fn configured_futures_for_catalogue(
        configured_stream_ids: &HashMap<String, String>,
    ) -> HashSet<String> {
        configured_stream_ids
            .keys()
            .filter(|instrument_id| {
                TbankInstrumentIdParts::from_str(instrument_id)
                    .is_ok_and(|parts| parts.is_moex_futures())
            })
            .cloned()
            .collect()
    }

    fn indicative_presence_is_required(filters: Option<&HashMap<String, String>>) -> bool {
        // A missing broker definition cannot be classified by class code. In that case it is an
        // unselected candidate, not a missing instrument which was proven to match the filter.
        !filters.is_some_and(|filters| filters.contains_key("class_code"))
    }

    fn validate_filters(filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        let Some(filters) = filters else {
            return Ok(());
        };
        for key in filters.keys() {
            if key != "class_code"
                && key != "currency"
                && key != "venue"
                && key != "instrument_type"
            {
                anyhow::bail!("unsupported T-Bank instrument filter: {key}");
            }
        }
        Ok(())
    }

    fn matches_filters(
        metadata: &TbankInstrumentMetadata,
        filters: Option<&HashMap<String, String>>,
    ) -> bool {
        let Some(filters) = filters else {
            return true;
        };
        filters.iter().all(|(key, value)| match key.as_str() {
            "class_code" => metadata.class_code.eq_ignore_ascii_case(value),
            "currency" => metadata.currency.eq_ignore_ascii_case(value),
            "venue" => metadata.venue.to_string().eq_ignore_ascii_case(value),
            "instrument_type" => metadata
                .instrument_type
                .to_string()
                .eq_ignore_ascii_case(value),
            _ => false,
        })
    }

    /// Loads and atomically replaces the current broker catalogue.
    ///
    /// This inherent future remains `Send` so live clients can use the same provider owner from
    /// background refresh tasks. Nautilus' `InstrumentProvider` trait is `?Send`; its method below
    /// delegates here for foreground framework calls.
    pub(crate) async fn load_all_current(
        &mut self,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        Self::validate_filters(filters)?;
        let mut clients = self.clients().await?;
        let configured_futures =
            Self::configured_futures_for_catalogue(&self.config.instrument_stream_ids);
        // Build the replacement off to the side. The currently published catalog remains
        // usable if any later RPC, conversion, or instrument insertion fails.
        let mut staged = Self::new(self.config.clone());
        {
            let response = clients
                .instruments
                .shares(with_timeout(
                    InstrumentsRequest {
                        instrument_status: Some(InstrumentStatus::Base as i32),
                        ..InstrumentsRequest::default()
                    },
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            for share in &response.instruments {
                let Some(metadata) = Self::catalog_share_metadata(share)? else {
                    continue;
                };
                if !Self::matches_filters(&metadata, filters) {
                    continue;
                }
                staged.add_share_metadata(metadata)?;
            }
        }
        if !configured_futures.is_empty() {
            let response = clients
                .instruments
                .futures(with_timeout(
                    InstrumentsRequest {
                        instrument_status: Some(InstrumentStatus::Base as i32),
                        ..InstrumentsRequest::default()
                    },
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            for future in &response.instruments {
                let Some(metadata) = Self::catalog_future_metadata(future)? else {
                    continue;
                };
                if !Self::matches_filters(&metadata, filters) {
                    continue;
                }
                let instrument_id = metadata.instrument_id.clone();
                if !configured_futures.contains(&instrument_id) {
                    continue;
                }
                let metadata = match Self::resolve_current_futures_metadata(
                    &mut clients,
                    metadata,
                    self.config.request_timeout,
                    &self.config.reconnect_policy,
                )
                .await
                {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        // Bulk providers follow Nautilus' skip-on-error pattern for individual
                        // definitions. FutureBy metadata is unsafe to publish as a fallback
                        // because the session contract controls its tick and multiplier.
                        staged.unresolved_futures.insert(instrument_id);
                        tracing::warn!(
                            ticker = %future.ticker,
                            class_code = %future.class_code,
                            %error,
                            "skipping futures instrument without current margin metadata"
                        );
                        continue;
                    }
                };
                staged.add_future_metadata(metadata)?;
            }
        }
        // Apply filters against the configured definitions before touching the broker RPC. This
        // keeps a futures-only (or otherwise non-index) catalog independent of Indicatives
        // availability and avoids an unnecessary unary request.
        let configured_indicatives =
            Self::configured_indicatives_for_filters(&self.config.indicative_instruments, filters);
        if !configured_indicatives.is_empty() {
            let response = clients
                .instruments
                .indicatives(with_timeout(
                    IndicativesRequest {},
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            for instrument_id in configured_indicatives {
                let definition = self
                    .config
                    .indicative_instruments
                    .get(&instrument_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing indicative definition for {instrument_id}")
                    })?;
                let indicative = response
                    .instruments
                    .iter()
                    .find(|instrument| Self::matches_indicative(instrument, &instrument_id));
                let Some(indicative) = indicative else {
                    if Self::indicative_presence_is_required(filters) {
                        return Err(
                            TbankAdapterError::InstrumentNotFound(instrument_id.clone()).into()
                        );
                    }
                    continue;
                };
                if !Self::matches_indicative_filters(
                    Some(indicative),
                    &instrument_id,
                    definition,
                    filters,
                ) {
                    continue;
                }
                staged.add_indicative(&instrument_id, indicative)?;
            }
        }
        staged.store.set_initialized();
        self.commit_catalog(staged);
        Ok(())
    }
}

#[async_trait(?Send)]
impl InstrumentProvider for TbankInstrumentProvider {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }

    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }

    async fn load_all(&mut self, filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        self.load_all_current(filters).await
    }

    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        Self::validate_filters(filters)?;
        let instrument_id_string = instrument_id.to_string();
        if self
            .config
            .indicative_instruments
            .contains_key(&instrument_id_string)
        {
            let definition = self
                .config
                .indicative_instruments
                .get(&instrument_id_string)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing indicative definition for {instrument_id_string}")
                })?;
            if !Self::matches_indicative_filters(None, &instrument_id_string, definition, filters) {
                anyhow::bail!("instrument {instrument_id} does not match adapter filters");
            }
            let mut clients = self.clients().await?;
            let response = clients
                .instruments
                .indicatives(with_timeout(
                    IndicativesRequest {},
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            let indicative = response
                .instruments
                .iter()
                .find(|instrument| Self::matches_indicative(instrument, &instrument_id_string))
                .ok_or_else(|| {
                    TbankAdapterError::InstrumentNotFound(instrument_id_string.clone())
                })?;
            if !Self::matches_indicative_filters(
                Some(indicative),
                &instrument_id_string,
                definition,
                filters,
            ) {
                anyhow::bail!("instrument {instrument_id} does not match adapter filters");
            }
            return self.add_indicative(&instrument_id_string, indicative);
        }
        let parts = TbankInstrumentIdParts::from_str(&instrument_id.to_string())?;
        if !parts.has_supported_venue() {
            anyhow::bail!("unsupported T-Bank instrument venue: {instrument_id}");
        }
        let is_share = parts.is_spbe_share() || parts.is_moex_tqbr_equity();
        let is_futures = parts.is_moex_futures();
        if !parts.is_supported_family() {
            return Err(TbankAdapterError::UnsupportedInstrument(instrument_id.to_string()).into());
        }
        let mut clients = self.clients().await?;
        if is_share {
            let response = clients
                .instruments
                .share_by(with_timeout(
                    InstrumentRequest {
                        id_type: InstrumentIdType::Ticker as i32,
                        class_code: Some(parts.class_code.clone()),
                        id: parts.ticker.clone(),
                    },
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            let share = response
                .instrument
                .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_id.to_string()))?;
            let metadata = TbankInstrumentMetadata::from_share(&share)?;
            if metadata.instrument_id != instrument_id_string
                || !metadata.is_supported()
                || !Self::matches_filters(&metadata, filters)
            {
                anyhow::bail!("instrument {instrument_id} does not match adapter support");
            }
            return self.add_share(&share);
        }
        if is_futures {
            let response = clients
                .instruments
                .future_by(with_timeout(
                    InstrumentRequest {
                        id_type: InstrumentIdType::Ticker as i32,
                        class_code: Some(parts.class_code),
                        id: parts.ticker,
                    },
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            let future = response
                .instrument
                .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_id.to_string()))?;
            let metadata = TbankInstrumentMetadata::from_future(&future)?;
            if metadata.instrument_id != instrument_id_string
                || !metadata.is_supported()
                || !Self::matches_filters(&metadata, filters)
            {
                anyhow::bail!("instrument {instrument_id} does not match adapter support");
            }
            let metadata = Self::resolve_current_futures_metadata(
                &mut clients,
                metadata,
                self.config.request_timeout,
                &self.config.reconnect_policy,
            )
            .await?;
            return self.add_future_metadata(metadata);
        }
        Err(TbankAdapterError::UnsupportedInstrument(instrument_id.to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::grpc::generated::{Future, IndicativeResponse, Share};

    #[test]
    fn accepts_supported_filters() {
        let filters = HashMap::from([
            ("class_code".to_string(), "tqbr".to_string()),
            ("currency".to_string(), "rub".to_string()),
        ]);

        assert!(TbankInstrumentProvider::validate_filters(Some(&filters)).is_ok());
    }

    #[test]
    fn rejects_unknown_filters() {
        let filters = HashMap::from([("exchange".to_string(), "MOEX".to_string())]);

        let error = TbankInstrumentProvider::validate_filters(Some(&filters)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported T-Bank instrument filter")
        );
    }

    #[test]
    fn catalog_skips_unsupported_families_before_metadata_conversion() {
        let moex_bond = Share {
            class_code: "TQTF".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };
        assert!(!TbankInstrumentProvider::is_supported_share_family(
            &moex_bond
        ));

        let unknown_exchange = Share {
            class_code: "TQBR".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Unspecified as i32,
            ..Share::default()
        };
        assert!(!TbankInstrumentProvider::is_supported_share_family(
            &unknown_exchange
        ));

        let moex_future = Future {
            class_code: SPBFUT_CLASS_CODE.to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Future::default()
        };
        assert!(TbankInstrumentProvider::is_supported_future_family(
            &moex_future
        ));

        let spbe_future = Future {
            class_code: SPBFUT_CLASS_CODE.to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Rts as i32,
            ..Future::default()
        };
        assert!(!TbankInstrumentProvider::is_supported_future_family(
            &spbe_future
        ));
    }

    #[test]
    fn bulk_catalog_skips_unsupported_currency_without_hiding_supported_shares() {
        let quotation = crate::grpc::generated::Quotation { units: 1, nano: 0 };
        let unsupported = Share {
            ticker: "UNKNOWN-CURRENCY-SHARE".to_string(),
            class_code: TQBR_CLASS_CODE.to_string(),
            currency: "ZZZ".to_string(),
            lot: 1,
            min_price_increment: Some(quotation),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };
        let supported = Share {
            ticker: "SBER".to_string(),
            class_code: TQBR_CLASS_CODE.to_string(),
            currency: "RUB".to_string(),
            lot: 10,
            min_price_increment: Some(quotation),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };

        assert!(
            TbankInstrumentProvider::catalog_share_metadata(&unsupported)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            TbankInstrumentProvider::catalog_share_metadata(&supported)
                .unwrap()
                .unwrap()
                .instrument_id,
            "SBER_TQBR.MOEX"
        );
    }

    #[test]
    fn bulk_catalog_supports_spbkz_share_in_tenge() {
        let share = Share {
            ticker: "KMG@KT".to_string(),
            class_code: "SPBKZ".to_string(),
            currency: "KZT".to_string(),
            lot: 1,
            min_price_increment: Some(crate::grpc::generated::Quotation { units: 1, nano: 0 }),
            real_exchange: crate::grpc::generated::RealExchange::Rts as i32,
            ..Share::default()
        };

        let metadata = TbankInstrumentProvider::catalog_share_metadata(&share)
            .unwrap()
            .expect("SPBKZ share should be supported");
        assert_eq!(metadata.instrument_id, "KMG@KT_SPBKZ.SPBE");
        assert_eq!(metadata.currency, "KZT");
        assert_eq!(metadata.venue, TbankVenue::Spbe);
        assert!(build_equity_instrument(&metadata).is_ok());
    }

    #[test]
    fn bulk_catalog_skips_spbe_share_with_nonpositive_tick() {
        let share = Share {
            ticker: "BROKEN@KT".to_string(),
            class_code: "SPBKZ".to_string(),
            currency: "KZT".to_string(),
            lot: 1,
            min_price_increment: Some(crate::grpc::generated::Quotation { units: 0, nano: 0 }),
            real_exchange: crate::grpc::generated::RealExchange::Rts as i32,
            ..Share::default()
        };

        assert!(
            TbankInstrumentProvider::catalog_share_metadata(&share)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn bulk_catalog_skips_malformed_supported_futures_record() {
        let future = Future {
            ticker: "BROKEN".to_string(),
            class_code: SPBFUT_CLASS_CODE.to_string(),
            currency: "RUB".to_string(),
            lot: 1,
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Future::default()
        };

        assert!(
            TbankInstrumentProvider::catalog_future_metadata(&future)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn filter_matching_is_case_insensitive() {
        let share = Share {
            class_code: "TQBR".to_string(),
            currency: "rub".to_string(),
            lot: 1,
            min_price_increment: Some(crate::grpc::generated::Quotation { units: 1, nano: 0 }),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };
        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        let filters = HashMap::from([
            ("class_code".to_string(), "tqbr".to_string()),
            ("currency".to_string(), "RUB".to_string()),
        ]);

        assert!(TbankInstrumentProvider::matches_filters(
            &metadata,
            Some(&filters)
        ));
    }

    #[test]
    fn indicative_definition_populates_data_metadata_without_execution_metadata() {
        let mut provider = TbankInstrumentProvider::new(TbankDataClientConfig {
            indicative_instruments: HashMap::from([(
                "IMOEX2.MOEX".to_string(),
                crate::config::TbankIndicativeInstrumentConfig {
                    currency: "RUB".to_string(),
                    price_increment: rust_decimal::Decimal::new(1, 8),
                },
            )]),
            ..TbankDataClientConfig::default()
        });
        let indicative = IndicativeResponse {
            ticker: "IMOEX2".to_string(),
            currency: "RUB".to_string(),
            uid: "imoex2-uid".to_string(),
            ..IndicativeResponse::default()
        };

        provider.add_indicative("IMOEX2.MOEX", &indicative).unwrap();

        assert!(provider.mapper().all_metadata().next().is_none());
        let metadata = provider.market_data_metadata().next().unwrap();
        assert_eq!(metadata.instrument_uid, "imoex2-uid");
        assert_eq!(metadata.lot_size, 1);
        assert_eq!(metadata.price_precision, 8);
        assert!(provider.store().contains(&"IMOEX2.MOEX".parse().unwrap()));
    }

    #[test]
    fn configured_futures_require_resolved_market_data_metadata() {
        let instrument_id = "Si-9.26_SPBFUT.MOEX".to_string();
        let configured = HashMap::from([(instrument_id.clone(), "stale-uid".to_string())]);
        let mut provider = TbankInstrumentProvider::new(TbankDataClientConfig::default());
        provider.unresolved_futures.insert(instrument_id.clone());

        assert_eq!(
            provider
                .ensure_configured_futures_resolved(&configured)
                .unwrap_err(),
            TbankAdapterError::FuturesMarginUnresolved(instrument_id)
        );
    }

    #[test]
    fn successful_point_load_clears_unresolved_futures_marker() {
        let future = Future {
            ticker: "Si-9.26".to_string(),
            class_code: SPBFUT_CLASS_CODE.to_string(),
            currency: "RUB".to_string(),
            lot: 1,
            uid: "si-current-uid".to_string(),
            min_price_increment: Some(crate::grpc::generated::Quotation { units: 1, nano: 0 }),
            min_price_increment_amount: Some(crate::grpc::generated::Quotation {
                units: 12,
                nano: 500_000_000,
            }),
            dlong: Some(crate::grpc::generated::Quotation {
                units: 0,
                nano: 100_000_000,
            }),
            dshort: Some(crate::grpc::generated::Quotation {
                units: 0,
                nano: 100_000_000,
            }),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Future::default()
        };
        let metadata = TbankInstrumentMetadata::from_future(&future).unwrap();
        let instrument_id = metadata.instrument_id.clone();
        let mut provider = TbankInstrumentProvider::new(TbankDataClientConfig::default());
        provider.unresolved_futures.insert(instrument_id.clone());

        provider.add_future_metadata(metadata).unwrap();

        assert!(!provider.unresolved_futures.contains(&instrument_id));
        provider
            .ensure_configured_futures_resolved(&HashMap::from([(
                instrument_id,
                "si-current-uid".to_string(),
            )]))
            .unwrap();
    }

    #[test]
    fn indicative_filters_are_applied_to_configured_indexes() {
        let definition = crate::config::TbankIndicativeInstrumentConfig {
            currency: "RUB".to_string(),
            price_increment: rust_decimal::Decimal::new(1, 8),
        };
        let indicative = IndicativeResponse {
            ticker: "IMOEX2".to_string(),
            class_code: "INDEX".to_string(),
            ..IndicativeResponse::default()
        };
        let futures = HashMap::from([(String::from("instrument_type"), String::from("futures"))]);
        let index = HashMap::from([(String::from("instrument_type"), String::from("index"))]);
        let venue = HashMap::from([(String::from("venue"), String::from("moex"))]);
        let configured = HashMap::from([(String::from("IMOEX2.MOEX"), definition.clone())]);

        assert!(
            TbankInstrumentProvider::configured_indicatives_for_filters(
                &configured,
                Some(&futures),
            )
            .is_empty()
        );
        assert_eq!(
            TbankInstrumentProvider::configured_indicatives_for_filters(&configured, Some(&index),),
            vec!["IMOEX2.MOEX"]
        );

        assert!(!TbankInstrumentProvider::matches_indicative_filters(
            None,
            "IMOEX2.MOEX",
            &definition,
            Some(&futures),
        ));
        assert!(TbankInstrumentProvider::matches_indicative_filters(
            None,
            "IMOEX2.MOEX",
            &definition,
            Some(&index),
        ));
        assert!(!TbankInstrumentProvider::matches_indicative_filters(
            Some(&indicative),
            "IMOEX2.MOEX",
            &definition,
            Some(&futures),
        ));
        assert!(TbankInstrumentProvider::matches_indicative_filters(
            Some(&indicative),
            "IMOEX2.MOEX",
            &definition,
            Some(&index),
        ));
        assert!(TbankInstrumentProvider::matches_indicative_filters(
            Some(&indicative),
            "IMOEX2.MOEX",
            &definition,
            Some(&venue),
        ));

        let class_code = HashMap::from([(String::from("class_code"), String::from("INDEX"))]);
        assert!(!TbankInstrumentProvider::indicative_presence_is_required(
            Some(&class_code)
        ));
        assert!(TbankInstrumentProvider::indicative_presence_is_required(
            Some(&index)
        ));
    }

    #[test]
    fn bulk_catalogue_enriches_only_explicit_futures() {
        let configured = HashMap::from([
            (
                String::from("Si-9.26_SPBFUT.MOEX"),
                String::from("future-uid"),
            ),
            (String::from("SBER_TQBR.MOEX"), String::from("share-uid")),
        ]);

        assert_eq!(
            TbankInstrumentProvider::configured_futures_for_catalogue(&configured),
            HashSet::from([String::from("Si-9.26_SPBFUT.MOEX")])
        );
    }
}
