use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_model::identifiers::InstrumentId;

use crate::{
    common::{TbankAdapterError, TbankInstrumentIdParts, consts::TQBR_CLASS_CODE},
    config::TbankDataClientConfig,
    grpc::{
        TbankAuthInterceptor, TbankGrpcClients, connect_channel,
        generated::{
            IndicativeResponse, IndicativesRequest, InstrumentIdType, InstrumentRequest,
            InstrumentStatus, InstrumentsRequest,
        },
        with_timeout,
    },
    instruments::{
        TbankInstrumentMapper, TbankInstrumentMetadata, TbankMarketDataInstrumentMetadata,
        build_equity_instrument, build_index_instrument,
    },
};

/// Loads T-Bank share definitions into NautilusTrader's canonical instrument store.
#[derive(Debug)]
pub struct TbankInstrumentProvider {
    config: TbankDataClientConfig,
    store: InstrumentStore,
    mapper: TbankInstrumentMapper,
    market_data_metadata: HashMap<String, TbankMarketDataInstrumentMetadata>,
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
        let currency =
            nautilus_model::types::Currency::from_str(&definition.currency.to_uppercase())?;
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

    fn indicative_ticker(instrument_id: &str) -> &str {
        instrument_id.strip_suffix(".MOEX").unwrap_or(instrument_id)
    }

    fn matches_indicative(indicative: &IndicativeResponse, instrument_id: &str) -> bool {
        indicative
            .ticker
            .eq_ignore_ascii_case(Self::indicative_ticker(instrument_id))
    }

    fn validate_filters(filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        let Some(filters) = filters else {
            return Ok(());
        };
        for key in filters.keys() {
            if key != "class_code" && key != "currency" {
                anyhow::bail!("unsupported T-Bank instrument filter: {key}");
            }
        }
        Ok(())
    }

    fn matches_filters(
        share: &crate::grpc::generated::Share,
        filters: Option<&HashMap<String, String>>,
    ) -> bool {
        let Some(filters) = filters else {
            return true;
        };
        filters.iter().all(|(key, value)| match key.as_str() {
            "class_code" => share.class_code.eq_ignore_ascii_case(value),
            "currency" => share.currency.eq_ignore_ascii_case(value),
            _ => false,
        })
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
        Self::validate_filters(filters)?;
        let mut clients = self.clients().await?;
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

        self.store.clear();
        self.mapper = TbankInstrumentMapper::new();
        self.market_data_metadata.clear();
        for share in &response.instruments {
            if share.class_code != TQBR_CLASS_CODE
                || !share.currency.eq_ignore_ascii_case("RUB")
                || !Self::matches_filters(share, filters)
            {
                continue;
            }
            self.add_share(share)?;
        }
        if !self.config.indicative_instruments.is_empty() {
            let response = clients
                .instruments
                .indicatives(with_timeout(
                    IndicativesRequest {},
                    self.config.request_timeout,
                ))
                .await?
                .into_inner();
            let configured = self
                .config
                .indicative_instruments
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for instrument_id in configured {
                let indicative = response
                    .instruments
                    .iter()
                    .find(|instrument| Self::matches_indicative(instrument, &instrument_id))
                    .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_id.clone()))?;
                self.add_indicative(&instrument_id, indicative)?;
            }
        }
        self.store.set_initialized();
        Ok(())
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
            return self.add_indicative(&instrument_id_string, indicative);
        }
        let parts = TbankInstrumentIdParts::from_str(&instrument_id.to_string())?;
        if !parts.is_moex_tqbr_equity() {
            return Err(TbankAdapterError::UnsupportedInstrument(instrument_id.to_string()).into());
        }

        let mut clients = self.clients().await?;
        let response = clients
            .instruments
            .share_by(with_timeout(
                InstrumentRequest {
                    id_type: InstrumentIdType::Ticker as i32,
                    class_code: Some(parts.class_code),
                    id: parts.ticker,
                },
                self.config.request_timeout,
            ))
            .await?
            .into_inner();
        let share = response
            .instrument
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_id.to_string()))?;
        if !Self::matches_filters(&share, filters) {
            anyhow::bail!("instrument {instrument_id} does not match requested filters");
        }
        self.add_share(&share)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::grpc::generated::{IndicativeResponse, Share};

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
    fn filter_matching_is_case_insensitive() {
        let share = Share {
            class_code: "TQBR".to_string(),
            currency: "rub".to_string(),
            ..Share::default()
        };
        let filters = HashMap::from([
            ("class_code".to_string(), "tqbr".to_string()),
            ("currency".to_string(), "RUB".to_string()),
        ]);

        assert!(TbankInstrumentProvider::matches_filters(
            &share,
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
}
