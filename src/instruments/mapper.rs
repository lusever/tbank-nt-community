use std::{collections::HashMap, str::FromStr};

use nautilus_core::{Params, time::get_atomic_clock_realtime};
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny, equity::Equity},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::{RUB_CURRENCY, TQBR_CLASS_CODE},
        decimal::quotation_to_decimal,
        error::{Result, TbankAdapterError},
        ids::{TbankInstrumentIdParts, instrument_id_from_ticker_class},
    },
    grpc::generated::{IndicativeResponse, Share},
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Canonical fields required to translate T-Bank market data for any instrument.
pub struct TbankMarketDataInstrumentMetadata {
    /// Canonical Nautilus instrument ID.
    pub instrument_id: String,
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Quantity units represented by one broker lot.
    pub lot_size: u32,
    /// Decimal precision for prices.
    pub price_precision: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Broker metadata needed to map and submit orders for an instrument.
pub struct TbankInstrumentMetadata {
    /// Canonical Nautilus instrument ID.
    pub instrument_id: String,
    /// Exchange ticker.
    pub ticker: String,
    /// T-Bank class code.
    pub class_code: String,
    /// Legacy FIGI identifier.
    pub figi: String,
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Broker position UID.
    pub position_uid: String,
    /// Shares per exchange lot.
    pub lot: u32,
    /// Minimum price increment.
    pub min_price_increment: Decimal,
    /// Trading currency code.
    pub currency: String,
    /// Exchange code reported by T-Bank.
    pub exchange: String,
    /// Decimal precision for prices.
    pub price_precision: u32,
    /// Decimal precision for quantities.
    pub quantity_precision: u32,
}

impl TbankInstrumentMetadata {
    /// Builds adapter instrument metadata from a T-Bank share.
    pub fn from_share(share: &Share) -> Result<Self> {
        if share.class_code != TQBR_CLASS_CODE {
            return Err(TbankAdapterError::UnsupportedInstrument(format!(
                "{}_{}.{}",
                share.ticker, share.class_code, share.exchange
            )));
        }
        if share.currency.to_uppercase() != RUB_CURRENCY {
            return Err(TbankAdapterError::UnsupportedInstrument(format!(
                "unsupported currency {} for {}",
                share.currency, share.ticker
            )));
        }
        if share.lot <= 0 {
            return Err(TbankAdapterError::UnsupportedInstrument(format!(
                "invalid lot {} for {}",
                share.lot, share.ticker
            )));
        }

        let tick = share
            .min_price_increment
            .as_ref()
            .map(quotation_to_decimal)
            .ok_or_else(|| {
                TbankAdapterError::UnsupportedInstrument(format!(
                    "missing min_price_increment for {}",
                    share.ticker
                ))
            })?;

        Ok(Self {
            instrument_id: instrument_id_from_ticker_class(&share.ticker, &share.class_code),
            ticker: share.ticker.clone(),
            class_code: share.class_code.clone(),
            figi: share.figi.clone(),
            instrument_uid: share.uid.clone(),
            position_uid: share.position_uid.clone(),
            lot: u32::try_from(share.lot).map_err(|_| {
                TbankAdapterError::UnsupportedInstrument(format!(
                    "invalid lot {} for {}",
                    share.lot, share.ticker
                ))
            })?,
            min_price_increment: tick,
            currency: share.currency.to_uppercase(),
            exchange: share.exchange.clone(),
            price_precision: tick.normalize().scale(),
            quantity_precision: 0,
        })
    }

    /// Restores broker metadata embedded in a Nautilus equity instrument.
    #[must_use]
    pub fn from_instrument(instrument: &InstrumentAny) -> Option<Self> {
        let InstrumentAny::Equity(equity) = instrument else {
            return None;
        };
        let instrument_id = equity.id.to_string();
        let parts = TbankInstrumentIdParts::from_str(&instrument_id).ok()?;
        let info = equity.info.as_ref()?;
        let instrument_uid = info
            .get_str("instrument_uid")
            .filter(|value| !value.trim().is_empty())?
            .to_string();
        let position_uid = info.get_str("position_uid").unwrap_or("").to_string();
        let figi = info.get_str("figi").unwrap_or("").to_string();
        Some(Self {
            instrument_id,
            ticker: parts.ticker,
            class_code: parts.class_code,
            figi,
            instrument_uid,
            position_uid,
            lot: equity
                .lot_size
                .and_then(|qty| qty.as_decimal().to_string().parse::<u32>().ok())
                .filter(|lot| *lot > 0)?,
            min_price_increment: equity.price_increment.as_decimal(),
            currency: equity.currency.to_string(),
            exchange: parts.venue,
            price_precision: u32::from(equity.price_precision),
            quantity_precision: 0,
        })
    }
}

/// Builds a Nautilus equity instrument from resolved T-Bank metadata.
pub fn build_equity_instrument(
    metadata: &TbankInstrumentMetadata,
) -> anyhow::Result<InstrumentAny> {
    let timestamp = get_atomic_clock_realtime().get_time_ns();
    let instrument_id = metadata.instrument_id.parse::<InstrumentId>()?;
    let price_increment = metadata.min_price_increment.normalize().to_string();
    let mut info = Params::new();
    info.insert(
        "instrument_uid".to_string(),
        metadata.instrument_uid.clone().into(),
    );
    info.insert("figi".to_string(), metadata.figi.clone().into());
    info.insert(
        "position_uid".to_string(),
        metadata.position_uid.clone().into(),
    );
    info.insert("class_code".to_string(), metadata.class_code.clone().into());
    info.insert("exchange".to_string(), metadata.exchange.clone().into());
    Ok(InstrumentAny::Equity(Equity::new(
        instrument_id,
        Symbol::new(metadata.instrument_id.trim_end_matches(".MOEX")),
        None,
        Currency::from("RUB"),
        metadata.price_precision as u8,
        Price::from(price_increment.as_str()),
        Some(Quantity::from(metadata.lot.to_string().as_str())),
        None,
        Some(Quantity::from("1")),
        None,
        Some(Price::from(price_increment.as_str())),
        Some(Decimal::ZERO),
        Some(Decimal::ZERO),
        Some(Decimal::ZERO),
        Some(Decimal::ZERO),
        None,
        Some(info),
        timestamp,
        timestamp,
    )))
}

/// Builds a non-tradable Nautilus index definition from a T-Bank indicative instrument.
pub fn build_index_instrument(
    instrument_id: &str,
    indicative: &IndicativeResponse,
    currency: Currency,
    price_increment: Decimal,
) -> anyhow::Result<InstrumentAny> {
    if price_increment <= Decimal::ZERO {
        anyhow::bail!("index price increment must be positive for {instrument_id}");
    }
    let price_increment = price_increment.normalize();
    let price_precision = u8::try_from(price_increment.scale())?;
    let timestamp = get_atomic_clock_realtime().get_time_ns();
    let instrument_id = instrument_id.parse::<InstrumentId>()?;
    let mut info = Params::new();
    info.insert("instrument_uid".to_string(), indicative.uid.clone().into());
    info.insert("figi".to_string(), indicative.figi.clone().into());
    info.insert(
        "class_code".to_string(),
        indicative.class_code.clone().into(),
    );
    info.insert("exchange".to_string(), indicative.exchange.clone().into());
    info.insert("tbank_market_data_only".to_string(), true.into());

    Ok(InstrumentAny::IndexInstrument(
        IndexInstrument::new_checked(
            instrument_id,
            Symbol::new(&indicative.ticker),
            currency,
            price_precision,
            0,
            Price::from_decimal_dp(price_increment, price_precision)?,
            Quantity::from("1"),
            None,
            Some(info),
            timestamp,
            timestamp,
        )?,
    ))
}

#[derive(Debug, Clone, Default)]
/// Bidirectional lookup indexes for T-Bank instrument identities.
pub struct TbankInstrumentMapper {
    by_instrument_id: HashMap<String, TbankInstrumentMetadata>,
    by_instrument_uid: HashMap<String, String>,
    by_figi: HashMap<String, String>,
    by_ticker_class: HashMap<String, String>,
}

impl TbankInstrumentMapper {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts instrument metadata into every lookup index.
    pub fn insert(&mut self, metadata: TbankInstrumentMetadata) -> Result<()> {
        let parts = TbankInstrumentIdParts::from_str(&metadata.instrument_id)?;
        if !parts.is_moex_tqbr_equity() {
            return Err(TbankAdapterError::UnsupportedInstrument(
                metadata.instrument_id.clone(),
            ));
        }

        self.by_instrument_uid.insert(
            metadata.instrument_uid.clone(),
            metadata.instrument_id.clone(),
        );
        self.by_figi
            .insert(metadata.figi.clone(), metadata.instrument_id.clone());
        self.by_ticker_class.insert(
            format!("{}_{}", metadata.ticker, metadata.class_code),
            metadata.instrument_id.clone(),
        );
        self.by_instrument_id
            .insert(metadata.instrument_id.clone(), metadata);
        Ok(())
    }

    /// Iterates over all indexed instrument metadata.
    pub fn all_metadata(&self) -> impl Iterator<Item = &TbankInstrumentMetadata> {
        self.by_instrument_id.values()
    }

    /// Returns instrument metadata by canonical Nautilus instrument ID.
    pub fn get_by_instrument_id(&self, instrument_id: &str) -> Result<&TbankInstrumentMetadata> {
        self.by_instrument_id
            .get(instrument_id)
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_id.to_string()))
    }

    /// Returns the T-Bank instrument UID for a Nautilus instrument ID.
    pub fn instrument_uid_for(&self, instrument_id: &str) -> Result<&str> {
        Ok(self
            .get_by_instrument_id(instrument_id)?
            .instrument_uid
            .as_str())
    }

    /// Returns the Nautilus instrument ID for a T-Bank instrument UID.
    pub fn instrument_id_for_uid(&self, instrument_uid: &str) -> Result<&str> {
        self.by_instrument_uid
            .get(instrument_uid)
            .map(String::as_str)
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(instrument_uid.to_string()))
    }

    /// Returns the Nautilus instrument ID for a FIGI.
    pub fn instrument_id_for_figi(&self, figi: &str) -> Result<&str> {
        self.by_figi
            .get(figi)
            .map(String::as_str)
            .ok_or_else(|| TbankAdapterError::InstrumentNotFound(figi.to_string()))
    }

    /// Returns the Nautilus instrument ID for a ticker and class code.
    pub fn instrument_id_for_ticker_class(&self, ticker: &str, class_code: &str) -> Result<&str> {
        let key = format!("{ticker}_{class_code}");
        self.by_ticker_class
            .get(&key)
            .map(String::as_str)
            .ok_or(TbankAdapterError::InstrumentNotFound(key))
    }
}

#[cfg(test)]
mod tests {
    use crate::grpc::generated::{IndicativeResponse, Quotation};

    use super::*;

    fn sber() -> TbankInstrumentMetadata {
        TbankInstrumentMetadata {
            instrument_id: "SBER_TQBR.MOEX".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            figi: "BBG004730N88".to_string(),
            instrument_uid: "sber-uid".to_string(),
            position_uid: "sber-pos".to_string(),
            lot: 10,
            min_price_increment: Decimal::new(1, 2),
            currency: "RUB".to_string(),
            exchange: "MOEX".to_string(),
            price_precision: 2,
            quantity_precision: 0,
        }
    }

    #[test]
    fn maps_sber_in_both_directions() {
        let mut mapper = TbankInstrumentMapper::new();
        mapper.insert(sber()).unwrap();

        assert_eq!(
            mapper.instrument_uid_for("SBER_TQBR.MOEX").unwrap(),
            "sber-uid"
        );
        assert_eq!(
            mapper.instrument_id_for_uid("sber-uid").unwrap(),
            "SBER_TQBR.MOEX"
        );
        assert_eq!(
            mapper.instrument_id_for_figi("BBG004730N88").unwrap(),
            "SBER_TQBR.MOEX"
        );
        assert_eq!(
            mapper
                .instrument_id_for_ticker_class("SBER", "TQBR")
                .unwrap(),
            "SBER_TQBR.MOEX"
        );
    }

    #[test]
    fn rejects_unknown_instrument() {
        let mapper = TbankInstrumentMapper::new();
        assert!(matches!(
            mapper.instrument_uid_for("SBER_TQBR.MOEX"),
            Err(TbankAdapterError::InstrumentNotFound(_))
        ));
    }

    #[test]
    fn builds_non_tradable_index_instrument() {
        let indicative = IndicativeResponse {
            figi: "IMOEX2".to_string(),
            ticker: "IMOEX2".to_string(),
            class_code: "INDEX".to_string(),
            currency: String::new(),
            exchange: "MOEX".to_string(),
            uid: "imoex2-uid".to_string(),
            ..IndicativeResponse::default()
        };

        let instrument = build_index_instrument(
            "IMOEX2.MOEX",
            &indicative,
            Currency::from("RUB"),
            Decimal::new(1, 8),
        )
        .unwrap();

        let InstrumentAny::IndexInstrument(index) = instrument else {
            panic!("expected IndexInstrument");
        };
        assert_eq!(index.id.to_string(), "IMOEX2.MOEX");
        assert_eq!(index.currency.to_string(), "RUB");
        assert_eq!(index.price_precision, 8);
        assert_eq!(index.price_increment, Price::from("0.00000001"));
        assert_eq!(index.ts_event, index.ts_init);
        assert!(index.ts_init.as_u64() > 0);
        assert_eq!(
            index
                .info
                .as_ref()
                .and_then(|info| info.get_str("instrument_uid")),
            Some("imoex2-uid")
        );
    }

    #[test]
    fn builds_metadata_from_tqbr_share() {
        let share = Share {
            figi: "BBG004730N88".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot: 10,
            currency: "rub".to_string(),
            exchange: "MOEX".to_string(),
            min_price_increment: Some(Quotation {
                units: 0,
                nano: 10_000_000,
            }),
            uid: "sber-uid".to_string(),
            position_uid: "sber-pos".to_string(),
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "SBER_TQBR.MOEX");
        assert_eq!(metadata.lot, 10);
        assert_eq!(metadata.min_price_increment, Decimal::new(1, 2));
        assert_eq!(metadata.currency, "RUB");
    }

    #[test]
    fn equity_instrument_uses_nautilus_clock_for_initial_timestamps() {
        let instrument = build_equity_instrument(&sber()).unwrap();
        let InstrumentAny::Equity(equity) = instrument else {
            panic!("expected Equity");
        };

        assert_eq!(equity.ts_event, equity.ts_init);
        assert!(equity.ts_init.as_u64() > 0);
    }

    #[test]
    fn cached_equity_requires_authoritative_broker_identity_and_lot_size() {
        let instrument = build_equity_instrument(&sber()).unwrap();
        assert!(TbankInstrumentMetadata::from_instrument(&instrument).is_some());

        let InstrumentAny::Equity(mut missing_identity) = instrument.clone() else {
            panic!("expected Equity");
        };
        missing_identity.info = None;
        assert!(
            TbankInstrumentMetadata::from_instrument(&InstrumentAny::Equity(missing_identity))
                .is_none()
        );

        let InstrumentAny::Equity(mut missing_lot_size) = instrument else {
            panic!("expected Equity");
        };
        missing_lot_size.lot_size = None;
        assert!(
            TbankInstrumentMetadata::from_instrument(&InstrumentAny::Equity(missing_lot_size))
                .is_none()
        );
    }

    #[test]
    fn builds_metadata_from_tqbr_share_with_tbank_exchange_variant() {
        let share = Share {
            figi: "BBG004730N88".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            lot: 10,
            currency: "rub".to_string(),
            exchange: "moex_mrng_evng_e_wknd_dlr".to_string(),
            min_price_increment: Some(Quotation {
                units: 0,
                nano: 10_000_000,
            }),
            uid: "sber-uid".to_string(),
            position_uid: "sber-pos".to_string(),
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "SBER_TQBR.MOEX");
        assert_eq!(metadata.exchange, "moex_mrng_evng_e_wknd_dlr");
    }
}
