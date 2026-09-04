use std::{collections::HashMap, str::FromStr};

use nautilus_core::{Params, time::get_atomic_clock_realtime};
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{FuturesContract, IndexInstrument, InstrumentAny, equity::Equity},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{
        consts::{RTSX_MIC, SPBFUT_CLASS_CODE, TQBR_CLASS_CODE},
        decimal::quotation_to_decimal,
        error::{REDACTED_BROKER_IDENTITY, Result, TbankAdapterError},
        ids::{TbankInstrumentIdParts, instrument_id_from_ticker_class_for_venue},
        venue::{TbankInstrumentType, TbankVenue},
    },
    grpc::generated::{Future, GetFuturesMarginResponse, IndicativeResponse, Quotation, Share},
};

fn canonical_futures_underlying(future: &Future) -> Option<String> {
    let basic_asset = future.basic_asset.trim();
    if basic_asset.is_empty() {
        return None;
    }
    if basic_asset.is_ascii() {
        return Some(basic_asset.to_string());
    }

    // T-Bank uses this field as a localized display name for some contracts
    // (for example, `Пшеница`). Nautilus requires an ASCII identifier here,
    // so use the broker's stable underlying-position identity when available.
    let basic_asset_position_uid = future.basic_asset_position_uid.trim();
    Some(if basic_asset_position_uid.is_empty() {
        future.ticker.clone()
    } else {
        basic_asset_position_uid.to_string()
    })
}

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
    /// Public instrument family.
    pub instrument_type: TbankInstrumentType,
    /// Public Nautilus venue.
    pub venue: TbankVenue,
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
    /// Whether the broker permits API trading for this instrument.
    pub api_trade_available: bool,
    /// Whether the broker currently permits buying this instrument.
    pub buy_available: bool,
    /// Whether the broker currently permits selling this instrument.
    pub sell_available: bool,
    /// Broker-required client tests which have not been satisfied.
    pub required_tests: Vec<String>,
    /// Whether prices are represented in T-Bank price points.
    pub price_in_points: bool,
    /// Futures contract multiplier used by Nautilus.
    pub multiplier: Decimal,
    /// Currency value of one minimum price increment, when supplied by T-Bank.
    pub min_price_increment_amount: Option<Decimal>,
    /// Contract activation timestamp in nanoseconds.
    pub activation_ns: Option<u64>,
    /// Contract expiration timestamp in nanoseconds.
    pub expiration_ns: Option<u64>,
    /// Futures underlying symbol.
    pub underlying: Option<String>,
    /// Broker-reported size of the underlying asset for one contract.
    pub basic_asset_size: Option<Decimal>,
    /// Nautilus futures asset class.
    pub asset_class: AssetClass,
    /// Absolute initial margin on buy, when supplied by T-Bank.
    pub initial_margin_on_buy: Option<Decimal>,
    /// Absolute initial margin on sell, when supplied by T-Bank.
    pub initial_margin_on_sell: Option<Decimal>,
    /// Current T-Bank initial-margin risk rate for a buy position.
    pub initial_margin_rate_on_buy: Option<Decimal>,
    /// Current T-Bank initial-margin risk rate for a sell position.
    pub initial_margin_rate_on_sell: Option<Decimal>,
}

impl Default for TbankInstrumentMetadata {
    fn default() -> Self {
        Self {
            instrument_id: String::new(),
            instrument_type: TbankInstrumentType::Share,
            venue: TbankVenue::Moex,
            ticker: String::new(),
            class_code: String::new(),
            figi: String::new(),
            instrument_uid: String::new(),
            position_uid: String::new(),
            lot: 1,
            min_price_increment: Decimal::ONE,
            currency: "RUB".to_string(),
            exchange: "MOEX".to_string(),
            price_precision: 0,
            quantity_precision: 0,
            api_trade_available: true,
            buy_available: true,
            sell_available: true,
            required_tests: Vec::new(),
            price_in_points: false,
            multiplier: Decimal::ONE,
            min_price_increment_amount: None,
            activation_ns: None,
            expiration_ns: None,
            underlying: None,
            basic_asset_size: None,
            asset_class: AssetClass::Equity,
            initial_margin_on_buy: None,
            initial_margin_on_sell: None,
            initial_margin_rate_on_buy: None,
            initial_margin_rate_on_sell: None,
        }
    }
}

impl TbankInstrumentMetadata {
    fn positive_quotation(value: Option<&Quotation>) -> Option<Decimal> {
        value
            .map(quotation_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
    }

    /// Returns the conservative side-independent rate accepted by Nautilus' static futures
    /// margin model. T-Bank exposes side-specific rates; the larger one prevents the local
    /// buying-power check from underestimating either side until a side-aware model is available
    /// in the pinned Nautilus version.
    pub(crate) fn conservative_initial_margin_rate(&self) -> Option<Decimal> {
        let buy = self
            .initial_margin_rate_on_buy
            .filter(|rate| *rate > Decimal::ZERO);
        let sell = self
            .initial_margin_rate_on_sell
            .filter(|rate| *rate > Decimal::ZERO);
        match (buy, sell) {
            (Some(buy), Some(sell)) => Some(buy.max(sell)),
            _ => None,
        }
    }

    /// Returns the ISO 10383 MIC for the market segment where the instrument trades.
    pub(crate) const fn exchange_mic(&self) -> &'static str {
        match self.instrument_type {
            TbankInstrumentType::Futures => RTSX_MIC,
            TbankInstrumentType::Share => self.venue.mic(),
        }
    }

    /// Returns whether this metadata belongs to a tradable instrument supported by the adapter.
    pub(crate) fn is_supported(&self) -> bool {
        match (self.venue, self.instrument_type) {
            (TbankVenue::Moex, TbankInstrumentType::Share) => {
                self.class_code.eq_ignore_ascii_case(TQBR_CLASS_CODE)
            }
            (TbankVenue::Moex, TbankInstrumentType::Futures) => {
                self.class_code.eq_ignore_ascii_case(SPBFUT_CLASS_CODE)
            }
            (TbankVenue::Spbe, TbankInstrumentType::Share) => true,
            (TbankVenue::Spbe, TbankInstrumentType::Futures) => false,
        }
    }

    /// Builds adapter instrument metadata from a T-Bank share.
    pub fn from_share(share: &Share) -> Result<Self> {
        let venue = venue_from_real_exchange(share.real_exchange, &share.ticker)?;
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
        if tick <= Decimal::ZERO {
            return Err(TbankAdapterError::UnsupportedInstrument(format!(
                "invalid share min_price_increment {} for {}",
                tick, share.ticker
            )));
        }
        let currency = share.currency.to_uppercase();
        crate::common::currency::currency_from_code(&currency).map_err(|error| {
            TbankAdapterError::UnsupportedInstrument(format!(
                "unsupported share currency {currency}: {error}"
            ))
        })?;

        Ok(Self {
            instrument_id: instrument_id_from_ticker_class_for_venue(
                &share.ticker,
                &share.class_code,
                venue.as_str(),
            ),
            instrument_type: TbankInstrumentType::Share,
            venue,
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
            currency,
            exchange: share.exchange.clone(),
            price_precision: tick.normalize().scale(),
            quantity_precision: 0,
            api_trade_available: share.api_trade_available_flag,
            buy_available: share.buy_available_flag,
            sell_available: share.sell_available_flag,
            required_tests: share.required_tests.clone(),
            price_in_points: false,
            multiplier: Decimal::ONE,
            min_price_increment_amount: None,
            activation_ns: None,
            expiration_ns: None,
            underlying: None,
            basic_asset_size: None,
            asset_class: AssetClass::Equity,
            initial_margin_on_buy: None,
            initial_margin_on_sell: None,
            initial_margin_rate_on_buy: None,
            initial_margin_rate_on_sell: None,
        })
    }

    fn insert_trading_flags(info: &mut Params, metadata: &Self) {
        info.insert(
            "api_trade_available".to_string(),
            metadata.api_trade_available.into(),
        );
        info.insert("buy_available".to_string(), metadata.buy_available.into());
        info.insert("sell_available".to_string(), metadata.sell_available.into());
        info.insert(
            "required_tests".to_string(),
            metadata.required_tests.join("\n").into(),
        );
    }

    /// Reads the current adapter metadata contract from a Nautilus instrument.
    ///
    /// These fields are deliberately presence-sensitive. An instrument without the current
    /// trading metadata is not a valid T-Bank instrument definition and must be refreshed from
    /// the broker instead of being interpreted as non-tradable.
    fn trading_flags_from_info(info: &Params) -> Option<(bool, bool, bool, Vec<String>)> {
        let required_tests = info
            .get_str("required_tests")?
            .split('\n')
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect();
        Some((
            info.get_bool("api_trade_available")?,
            info.get_bool("buy_available")?,
            info.get_bool("sell_available")?,
            required_tests,
        ))
    }

    /// Builds metadata for a T-Bank futures contract.
    pub fn from_future(future: &crate::grpc::generated::Future) -> Result<Self> {
        let venue = venue_from_real_exchange(future.real_exchange, &future.ticker)?;
        let tick = future
            .min_price_increment
            .as_ref()
            .map(quotation_to_decimal)
            .ok_or_else(|| {
                TbankAdapterError::UnsupportedInstrument(format!(
                    "missing min_price_increment for {}",
                    future.ticker
                ))
            })?;
        if tick <= Decimal::ZERO || future.lot <= 0 {
            return Err(TbankAdapterError::UnsupportedInstrument(format!(
                "invalid futures tick or lot for {}",
                future.ticker
            )));
        }
        let tick_amount = future
            .min_price_increment_amount
            .as_ref()
            .map(quotation_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                TbankAdapterError::UnsupportedInstrument(format!(
                    "missing min_price_increment_amount for {}",
                    future.ticker
                ))
            })?;
        let multiplier = tick_amount / tick;
        let currency = future.currency.to_uppercase();
        crate::common::currency::currency_from_code(&currency).map_err(|error| {
            TbankAdapterError::UnsupportedInstrument(format!(
                "unsupported futures currency {currency}: {error}"
            ))
        })?;

        Ok(Self {
            instrument_id: instrument_id_from_ticker_class_for_venue(
                &future.ticker,
                &future.class_code,
                venue.as_str(),
            ),
            instrument_type: TbankInstrumentType::Futures,
            venue,
            ticker: future.ticker.clone(),
            class_code: future.class_code.clone(),
            figi: future.figi.clone(),
            instrument_uid: future.uid.clone(),
            position_uid: future.position_uid.clone(),
            lot: u32::try_from(future.lot).map_err(|_| {
                TbankAdapterError::UnsupportedInstrument(format!(
                    "invalid futures lot {} for {}",
                    future.lot, future.ticker
                ))
            })?,
            min_price_increment: tick,
            currency,
            exchange: future.exchange.clone(),
            price_precision: tick.normalize().scale(),
            quantity_precision: 0,
            api_trade_available: future.api_trade_available_flag,
            buy_available: future.buy_available_flag,
            sell_available: future.sell_available_flag,
            required_tests: future.required_tests.clone(),
            price_in_points: true,
            multiplier,
            min_price_increment_amount: Some(tick_amount),
            activation_ns: future
                .first_trade_date
                .as_ref()
                .and_then(|value| crate::common::time::timestamp_to_unix_nanos(value).ok())
                .and_then(|value| u64::try_from(value).ok()),
            expiration_ns: future
                .expiration_date
                .as_ref()
                .and_then(|value| crate::common::time::timestamp_to_unix_nanos(value).ok())
                .and_then(|value| u64::try_from(value).ok()),
            underlying: canonical_futures_underlying(future),
            basic_asset_size: future.basic_asset_size.as_ref().map(quotation_to_decimal),
            asset_class: match future.asset_type.to_ascii_lowercase().as_str() {
                "currency" => AssetClass::FX,
                "security" => AssetClass::Equity,
                "index" => AssetClass::Index,
                _ => AssetClass::Commodity,
            },
            initial_margin_on_buy: future
                .initial_margin_on_buy
                .as_ref()
                .map(crate::common::decimal::money_value_to_decimal),
            initial_margin_on_sell: future
                .initial_margin_on_sell
                .as_ref()
                .map(crate::common::decimal::money_value_to_decimal),
            initial_margin_rate_on_buy: Self::positive_quotation(future.dlong_client.as_ref())
                .or_else(|| Self::positive_quotation(future.dlong.as_ref())),
            initial_margin_rate_on_sell: Self::positive_quotation(future.dshort_client.as_ref())
                .or_else(|| Self::positive_quotation(future.dshort.as_ref())),
        })
    }

    /// Applies the current futures tick and tick-value contract returned by `GetFuturesMargin`.
    ///
    /// `FutureBy` data is cached by the execution client, but the currency value of a futures
    /// tick is session-dependent. Keep this update at the metadata owner so every currency/point
    /// conversion uses the same current multiplier.
    pub(crate) fn update_futures_margin(
        &mut self,
        min_price_increment: Decimal,
        min_price_increment_amount: Decimal,
    ) -> Result<()> {
        if !self.price_in_points {
            return Err(TbankAdapterError::UnsupportedInstrument(
                "futures margin supplied for a non-futures instrument".to_string(),
            ));
        }
        if min_price_increment <= Decimal::ZERO || min_price_increment_amount <= Decimal::ZERO {
            return Err(TbankAdapterError::FuturesMarginUnresolved(format!(
                "invalid futures margin for {}",
                self.instrument_id
            )));
        }
        self.min_price_increment = min_price_increment;
        self.price_precision = min_price_increment.normalize().scale();
        self.min_price_increment_amount = Some(min_price_increment_amount);
        self.multiplier = min_price_increment_amount / min_price_increment;
        Ok(())
    }

    /// Returns the broker identifier accepted by `GetFuturesMargin`.
    pub(crate) fn futures_margin_instrument_id(&self) -> Result<String> {
        if !self.instrument_uid.trim().is_empty() {
            return Ok(self.instrument_uid.clone());
        }
        if !self.figi.trim().is_empty() {
            return Ok(self.figi.clone());
        }
        if !self.ticker.trim().is_empty() && !self.class_code.trim().is_empty() {
            return Ok(format!("{}_{}", self.ticker, self.class_code));
        }
        Err(TbankAdapterError::InvalidInstrumentIdentity(format!(
            "{REDACTED_BROKER_IDENTITY}: futures margin has no instrument identifier"
        )))
    }

    /// Applies the complete current futures contract returned by `GetFuturesMargin`.
    pub fn update_futures_margin_contract(
        &mut self,
        response: &GetFuturesMarginResponse,
    ) -> Result<()> {
        let tick = response
            .min_price_increment
            .as_ref()
            .map(quotation_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                TbankAdapterError::FuturesMarginUnresolved(format!(
                    "missing current futures tick for {}",
                    self.instrument_id
                ))
            })?;
        let tick_amount = response
            .min_price_increment_amount
            .as_ref()
            .map(quotation_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                TbankAdapterError::FuturesMarginUnresolved(format!(
                    "missing current futures tick amount for {}",
                    self.instrument_id
                ))
            })?;
        let initial_margin_on_buy = response
            .initial_margin_on_buy
            .as_ref()
            .filter(|value| {
                value.currency.is_empty() || value.currency.eq_ignore_ascii_case(&self.currency)
            })
            .map(crate::common::decimal::money_value_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                TbankAdapterError::FuturesMarginUnresolved(format!(
                    "missing or invalid current futures initial margin on buy for {}",
                    self.instrument_id
                ))
            })?;
        let initial_margin_on_sell = response
            .initial_margin_on_sell
            .as_ref()
            .filter(|value| {
                value.currency.is_empty() || value.currency.eq_ignore_ascii_case(&self.currency)
            })
            .map(crate::common::decimal::money_value_to_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                TbankAdapterError::FuturesMarginUnresolved(format!(
                    "missing or invalid current futures initial margin on sell for {}",
                    self.instrument_id
                ))
            })?;

        self.update_futures_margin(tick, tick_amount)?;
        self.initial_margin_on_buy = Some(initial_margin_on_buy);
        self.initial_margin_on_sell = Some(initial_margin_on_sell);
        Ok(())
    }

    /// Restores broker metadata embedded in a Nautilus equity instrument.
    #[must_use]
    pub fn from_instrument(instrument: &InstrumentAny) -> Option<Self> {
        let (
            instrument_id,
            instrument_type,
            venue,
            ticker,
            class_code,
            figi,
            instrument_uid,
            position_uid,
            lot,
            min_price_increment,
            currency,
            exchange,
            price_precision,
            price_in_points,
            multiplier,
            min_price_increment_amount,
            activation_ns,
            expiration_ns,
            underlying,
            basic_asset_size,
            asset_class,
            api_trade_available,
            buy_available,
            sell_available,
            required_tests,
            initial_margin_on_buy,
            initial_margin_on_sell,
            initial_margin_rate_on_buy,
            initial_margin_rate_on_sell,
        ) = match instrument {
            InstrumentAny::Equity(equity) => {
                let info = equity.info.as_ref()?;
                let (api_trade_available, buy_available, sell_available, required_tests) =
                    Self::trading_flags_from_info(info)?;
                (
                    equity.id.to_string(),
                    TbankInstrumentType::Share,
                    TbankVenue::from_str(equity.id.venue.as_str()).ok()?,
                    equity.raw_symbol.to_string(),
                    info.get_str("class_code").unwrap_or("").to_string(),
                    info.get_str("figi").unwrap_or("").to_string(),
                    info.get_str("instrument_uid")?.to_string(),
                    info.get_str("position_uid").unwrap_or("").to_string(),
                    equity
                        .lot_size
                        .and_then(|qty| qty.as_decimal().to_string().parse::<u32>().ok())
                        .filter(|lot| *lot > 0)?,
                    equity.price_increment.as_decimal(),
                    equity.currency.to_string(),
                    info.get_str("exchange")
                        .unwrap_or(equity.id.venue.as_str())
                        .to_string(),
                    u32::from(equity.price_precision),
                    false,
                    Decimal::ONE,
                    None,
                    None,
                    None,
                    None,
                    None,
                    AssetClass::Equity,
                    api_trade_available,
                    buy_available,
                    sell_available,
                    required_tests,
                    None,
                    None,
                    None,
                    None,
                )
            }
            InstrumentAny::FuturesContract(future) => {
                let info = future.info.as_ref()?;
                let (api_trade_available, buy_available, sell_available, required_tests) =
                    Self::trading_flags_from_info(info)?;
                (
                    future.id.to_string(),
                    TbankInstrumentType::Futures,
                    TbankVenue::from_str(future.id.venue.as_str()).ok()?,
                    future.raw_symbol.to_string(),
                    info.get_str("class_code").unwrap_or("").to_string(),
                    info.get_str("figi").unwrap_or("").to_string(),
                    info.get_str("instrument_uid")?.to_string(),
                    info.get_str("position_uid").unwrap_or("").to_string(),
                    future
                        .lot_size
                        .as_decimal()
                        .to_string()
                        .parse::<u32>()
                        .ok()
                        .filter(|lot| *lot > 0)?,
                    future.price_increment.as_decimal(),
                    future.currency.to_string(),
                    info.get_str("exchange")
                        .unwrap_or(future.id.venue.as_str())
                        .to_string(),
                    u32::from(future.price_precision),
                    true,
                    future.multiplier.as_decimal(),
                    info.get_str("min_price_increment_amount")
                        .and_then(|value| Decimal::from_str(value).ok()),
                    Some(future.activation_ns.as_u64()),
                    Some(future.expiration_ns.as_u64()),
                    Some(future.underlying.to_string()),
                    info.get_str("basic_asset_size")
                        .and_then(|value| Decimal::from_str(value).ok()),
                    future.asset_class,
                    api_trade_available,
                    buy_available,
                    sell_available,
                    required_tests,
                    info.get_str("initial_margin_on_buy")
                        .and_then(|value| Decimal::from_str(value).ok()),
                    info.get_str("initial_margin_on_sell")
                        .and_then(|value| Decimal::from_str(value).ok()),
                    info.get_str("initial_margin_rate_on_buy")
                        .and_then(|value| Decimal::from_str(value).ok()),
                    info.get_str("initial_margin_rate_on_sell")
                        .and_then(|value| Decimal::from_str(value).ok()),
                )
            }
            _ => return None,
        };
        let parts = TbankInstrumentIdParts::from_str(&instrument_id).ok()?;
        if instrument_uid.trim().is_empty() {
            return None;
        }
        Some(Self {
            instrument_id,
            instrument_type,
            venue,
            ticker,
            class_code: if class_code.is_empty() {
                parts.class_code
            } else {
                class_code
            },
            figi,
            instrument_uid,
            position_uid,
            lot,
            min_price_increment,
            currency,
            exchange,
            price_precision,
            quantity_precision: 0,
            api_trade_available,
            buy_available,
            sell_available,
            required_tests,
            price_in_points,
            multiplier,
            min_price_increment_amount,
            activation_ns,
            expiration_ns,
            underlying,
            basic_asset_size,
            asset_class,
            initial_margin_on_buy,
            initial_margin_on_sell,
            initial_margin_rate_on_buy,
            initial_margin_rate_on_sell,
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
    info.insert("venue".to_string(), metadata.venue.to_string().into());
    TbankInstrumentMetadata::insert_trading_flags(&mut info, metadata);
    Ok(InstrumentAny::Equity(Equity::build_checked(
        instrument_id,
        Symbol::new(metadata.ticker.as_str()),
        None,
        crate::common::currency::currency_from_code(metadata.currency.as_str())?,
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
    )?))
}

/// Builds a Nautilus futures contract from resolved T-Bank metadata.
pub fn build_futures_instrument(
    metadata: &TbankInstrumentMetadata,
) -> anyhow::Result<InstrumentAny> {
    let timestamp = get_atomic_clock_realtime().get_time_ns();
    let instrument_id = metadata.instrument_id.parse::<InstrumentId>()?;
    let currency = crate::common::currency::currency_from_code(metadata.currency.as_str())?;
    let activation_ns = metadata
        .activation_ns
        .map(nautilus_core::UnixNanos::from)
        .unwrap_or_default();
    let expiration_ns = metadata
        .expiration_ns
        .map(nautilus_core::UnixNanos::from)
        .unwrap_or_default();
    let margin_rate = metadata.conservative_initial_margin_rate().ok_or_else(|| {
        anyhow::anyhow!(
            "missing positive T-Bank futures initial-margin risk rate for {}",
            metadata.instrument_id
        )
    })?;
    let mut info = Params::new();
    info.insert(
        "instrument_uid".to_string(),
        metadata.instrument_uid.clone().into(),
    );
    info.insert(
        "position_uid".to_string(),
        metadata.position_uid.clone().into(),
    );
    info.insert("figi".to_string(), metadata.figi.clone().into());
    info.insert("class_code".to_string(), metadata.class_code.clone().into());
    info.insert("exchange".to_string(), metadata.exchange.clone().into());
    info.insert("venue".to_string(), metadata.venue.to_string().into());
    TbankInstrumentMetadata::insert_trading_flags(&mut info, metadata);
    info.insert("price_in_points".to_string(), true.into());
    info.insert(
        "multiplier".to_string(),
        metadata.multiplier.to_string().into(),
    );
    if let Some(value) = metadata.min_price_increment_amount {
        info.insert(
            "min_price_increment_amount".to_string(),
            value.to_string().into(),
        );
    }
    if let Some(value) = metadata.basic_asset_size {
        info.insert("basic_asset_size".to_string(), value.to_string().into());
    }
    if let Some(value) = metadata.initial_margin_on_buy {
        info.insert(
            "initial_margin_on_buy".to_string(),
            value.to_string().into(),
        );
    }
    if let Some(value) = metadata.initial_margin_on_sell {
        info.insert(
            "initial_margin_on_sell".to_string(),
            value.to_string().into(),
        );
    }
    if let Some(value) = metadata.initial_margin_rate_on_buy {
        info.insert(
            "initial_margin_rate_on_buy".to_string(),
            value.to_string().into(),
        );
    }
    if let Some(value) = metadata.initial_margin_rate_on_sell {
        info.insert(
            "initial_margin_rate_on_sell".to_string(),
            value.to_string().into(),
        );
    }
    Ok(InstrumentAny::FuturesContract(
        FuturesContract::build_checked(
            instrument_id,
            Symbol::new(&metadata.ticker),
            metadata.asset_class,
            Some(Ustr::from(metadata.exchange_mic())),
            Ustr::from(
                metadata
                    .underlying
                    .as_deref()
                    .unwrap_or(metadata.ticker.as_str()),
            ),
            activation_ns,
            expiration_ns,
            currency,
            metadata.price_precision as u8,
            Price::from_decimal_dp(metadata.min_price_increment, metadata.price_precision as u8)?,
            Quantity::from_decimal(metadata.multiplier)?,
            Quantity::from(metadata.lot.to_string().as_str()),
            None,
            Some(Quantity::from("1")),
            None,
            None,
            Some(margin_rate),
            Some(margin_rate),
            None,
            None,
            None,
            Some(info),
            timestamp,
            timestamp,
        )?,
    ))
}

pub(crate) fn venue_from_real_exchange(value: i32, ticker: &str) -> Result<TbankVenue> {
    match crate::grpc::generated::RealExchange::try_from(value).ok() {
        Some(crate::grpc::generated::RealExchange::Moex) => Ok(TbankVenue::Moex),
        // This is the sole transport-layer conversion of T-Bank's enum name.
        Some(crate::grpc::generated::RealExchange::Rts) => Ok(TbankVenue::Spbe),
        _ => Err(TbankAdapterError::UnsupportedInstrument(format!(
            "unsupported exchange for {ticker}"
        ))),
    }
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
        IndexInstrument::build_checked(
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
        if !parts.has_supported_venue() || !metadata.is_supported() {
            return Err(TbankAdapterError::InstrumentOutOfScope(
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
            format!(
                "{}_{}.{}",
                metadata.ticker, metadata.class_code, metadata.venue
            ),
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
        self.instrument_id_for_ticker_class_in_venue(ticker, class_code, TbankVenue::Moex)
    }

    /// Returns the Nautilus instrument ID for a ticker, class code, and venue.
    pub fn instrument_id_for_ticker_class_in_venue(
        &self,
        ticker: &str,
        class_code: &str,
        venue: TbankVenue,
    ) -> Result<&str> {
        let key = format!("{ticker}_{class_code}.{venue}");
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
            ..Default::default()
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
        assert_eq!(
            mapper
                .instrument_id_for_uid("private-uid")
                .unwrap_err()
                .to_string(),
            "instrument not found: private-uid"
        );
        assert_eq!(
            mapper
                .instrument_id_for_figi("private-figi")
                .unwrap_err()
                .to_string(),
            "instrument not found: private-figi"
        );
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
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "SBER_TQBR.MOEX");
        assert_eq!(metadata.lot, 10);
        assert_eq!(metadata.min_price_increment, Decimal::new(1, 2));
        assert_eq!(metadata.currency, "RUB");
    }

    #[test]
    fn builds_metadata_for_out_of_scope_moex_share() {
        let share = Share {
            ticker: "BOND".to_string(),
            class_code: "TQTF".to_string(),
            lot: 1,
            currency: "rub".to_string(),
            min_price_increment: Some(Quotation {
                units: 0,
                nano: 10_000_000,
            }),
            uid: "bond-uid".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "BOND_TQTF.MOEX");
        assert!(!metadata.is_supported());
    }

    #[test]
    fn maps_transport_rts_to_public_spbe_share() {
        let share = Share {
            ticker: "AAPL".to_string(),
            class_code: "SPBXM".to_string(),
            lot: 1,
            currency: "usd".to_string(),
            exchange: "SPB".to_string(),
            min_price_increment: Some(Quotation {
                units: 0,
                nano: 1_000_000,
            }),
            uid: "aapl-spbe-uid".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Rts as i32,
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "AAPL_SPBXM.SPBE");
        assert_eq!(metadata.venue, TbankVenue::Spbe);
        assert_eq!(metadata.currency, "USD");
        assert!(!metadata.instrument_id.contains("RTS"));
        let InstrumentAny::Equity(equity) = build_equity_instrument(&metadata).unwrap() else {
            panic!("expected SPBE share equity");
        };
        assert_eq!(equity.currency.to_string(), "USD");
    }

    #[test]
    fn maps_futures_multiplier_expiry_and_points() {
        let future = crate::grpc::generated::Future {
            ticker: "Si-9.26".to_string(),
            class_code: "SPBFUT".to_string(),
            lot: 1,
            currency: "rub".to_string(),
            exchange: "moex_mrng_evng_e_wknd_dlr".to_string(),
            min_price_increment: Some(Quotation { units: 1, nano: 0 }),
            min_price_increment_amount: Some(Quotation {
                units: 12,
                nano: 500_000_000,
            }),
            dlong_client: Some(Quotation {
                units: 0,
                nano: 100_000_000,
            }),
            dshort_client: Some(Quotation {
                units: 0,
                nano: 150_000_000,
            }),
            initial_margin_on_buy: Some(crate::grpc::generated::MoneyValue {
                currency: "rub".to_string(),
                units: 12_000,
                nano: 0,
            }),
            initial_margin_on_sell: Some(crate::grpc::generated::MoneyValue {
                currency: "rub".to_string(),
                units: 14_000,
                nano: 0,
            }),
            uid: "si-future-uid".to_string(),
            position_uid: "si-future-position".to_string(),
            basic_asset: "USD".to_string(),
            asset_type: "currency".to_string(),
            first_trade_date: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            expiration_date: Some(prost_types::Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            }),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..crate::grpc::generated::Future::default()
        };

        let metadata = TbankInstrumentMetadata::from_future(&future).unwrap();
        assert_eq!(metadata.instrument_id, "Si-9.26_SPBFUT.MOEX");
        assert_eq!(metadata.instrument_type, TbankInstrumentType::Futures);
        assert!(metadata.price_in_points);
        assert_eq!(metadata.multiplier, Decimal::new(125, 1));
        assert_eq!(metadata.activation_ns, Some(1_700_000_000_000_000_000));
        assert_eq!(metadata.expiration_ns, Some(1_800_000_000_000_000_000));
        assert_eq!(metadata.underlying.as_deref(), Some("USD"));
        assert_eq!(metadata.asset_class, AssetClass::FX);
        assert_eq!(
            metadata.conservative_initial_margin_rate(),
            Some(Decimal::new(15, 2))
        );

        let InstrumentAny::FuturesContract(instrument) =
            build_futures_instrument(&metadata).unwrap()
        else {
            panic!("expected FuturesContract");
        };
        assert_eq!(instrument.exchange, Some(Ustr::from("RTSX")));
        assert_eq!(instrument.margin_init, Decimal::new(15, 2));
        assert_eq!(instrument.margin_maint, Decimal::new(15, 2));
        assert_eq!(metadata.initial_margin_on_buy, Some(Decimal::from(12_000)));
        assert_eq!(metadata.initial_margin_on_sell, Some(Decimal::from(14_000)));
        assert_eq!(
            instrument
                .info
                .as_ref()
                .and_then(|info| info.get_str("exchange")),
            Some("moex_mrng_evng_e_wknd_dlr")
        );

        let restored =
            TbankInstrumentMetadata::from_instrument(&InstrumentAny::FuturesContract(instrument))
                .expect("published futures definition should retain T-Bank metadata");
        assert_eq!(restored.initial_margin_on_buy, Some(Decimal::from(12_000)));
        assert_eq!(restored.initial_margin_on_sell, Some(Decimal::from(14_000)));
        assert_eq!(
            restored.initial_margin_rate_on_buy,
            Some(Decimal::new(10, 2))
        );
        assert_eq!(
            restored.initial_margin_rate_on_sell,
            Some(Decimal::new(15, 2))
        );
    }

    #[test]
    fn refreshes_futures_multiplier_from_current_margin_contract() {
        let mut metadata = TbankInstrumentMetadata {
            instrument_id: "Si-9.26_SPBFUT.MOEX".to_string(),
            price_in_points: true,
            min_price_increment: Decimal::ONE,
            min_price_increment_amount: Some(Decimal::new(125, 1)),
            multiplier: Decimal::new(125, 1),
            ..TbankInstrumentMetadata::default()
        };

        metadata
            .update_futures_margin(Decimal::new(5, 1), Decimal::new(75, 1))
            .unwrap();

        assert_eq!(metadata.min_price_increment, Decimal::new(5, 1));
        assert_eq!(
            metadata.min_price_increment_amount,
            Some(Decimal::new(75, 1))
        );
        assert_eq!(metadata.multiplier, Decimal::from(15));
        assert_eq!(metadata.price_precision, 1);
    }

    #[test]
    fn applies_complete_current_futures_margin_contract() {
        let mut metadata = TbankInstrumentMetadata {
            instrument_id: "Si-9.26_SPBFUT.MOEX".to_string(),
            currency: "RUB".to_string(),
            price_in_points: true,
            min_price_increment: Decimal::ONE,
            min_price_increment_amount: Some(Decimal::new(125, 1)),
            multiplier: Decimal::new(125, 1),
            ..TbankInstrumentMetadata::default()
        };
        let response = GetFuturesMarginResponse {
            initial_margin_on_buy: Some(crate::grpc::generated::MoneyValue {
                currency: "rub".to_string(),
                units: 15_000,
                nano: 0,
            }),
            initial_margin_on_sell: Some(crate::grpc::generated::MoneyValue {
                currency: "rub".to_string(),
                units: 16_000,
                nano: 0,
            }),
            min_price_increment: Some(Quotation {
                units: 0,
                nano: 500_000_000,
            }),
            min_price_increment_amount: Some(Quotation { units: 75, nano: 0 }),
        };

        metadata.update_futures_margin_contract(&response).unwrap();

        assert_eq!(metadata.min_price_increment, Decimal::new(5, 1));
        assert_eq!(metadata.min_price_increment_amount, Some(Decimal::from(75)));
        assert_eq!(metadata.multiplier, Decimal::from(150));
        assert_eq!(metadata.price_precision, 1);
        assert_eq!(metadata.initial_margin_on_buy, Some(Decimal::from(15_000)));
        assert_eq!(metadata.initial_margin_on_sell, Some(Decimal::from(16_000)));
    }

    #[test]
    fn invalid_futures_margin_is_unresolved_not_out_of_scope() {
        let mut metadata = TbankInstrumentMetadata {
            instrument_id: "Si-9.26_SPBFUT.MOEX".to_string(),
            price_in_points: true,
            ..TbankInstrumentMetadata::default()
        };

        let error = metadata
            .update_futures_margin(Decimal::ZERO, Decimal::ONE)
            .unwrap_err();

        assert!(matches!(
            error,
            TbankAdapterError::FuturesMarginUnresolved(_)
        ));
    }

    #[test]
    fn maps_localized_futures_underlying_to_ascii_broker_identity() {
        let mut future = crate::grpc::generated::Future {
            ticker: "W4U6".to_string(),
            dlong: Some(Quotation {
                units: 0,
                nano: 100_000_000,
            }),
            dshort: Some(Quotation {
                units: 0,
                nano: 100_000_000,
            }),
            class_code: "SPBFUT".to_string(),
            lot: 1,
            currency: "rub".to_string(),
            min_price_increment: Some(Quotation { units: 10, nano: 0 }),
            min_price_increment_amount: Some(Quotation { units: 10, nano: 0 }),
            uid: "wheat-future-uid".to_string(),
            position_uid: "wheat-future-position".to_string(),
            basic_asset: "Пшеница".to_string(),
            basic_asset_position_uid: "wheat-underlying-position".to_string(),
            asset_type: "commodity".to_string(),
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..crate::grpc::generated::Future::default()
        };

        let metadata = TbankInstrumentMetadata::from_future(&future).unwrap();
        assert_eq!(
            metadata.underlying.as_deref(),
            Some("wheat-underlying-position")
        );
        let InstrumentAny::FuturesContract(instrument) =
            build_futures_instrument(&metadata).unwrap()
        else {
            panic!("expected FuturesContract");
        };
        assert_eq!(instrument.underlying.as_str(), "wheat-underlying-position");

        future.basic_asset_position_uid.clear();
        let metadata = TbankInstrumentMetadata::from_future(&future).unwrap();
        assert_eq!(metadata.underlying.as_deref(), Some("W4U6"));
        assert!(build_futures_instrument(&metadata).is_ok());
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
    fn cached_instrument_preserves_broker_trading_flags() {
        let mut metadata = sber();
        metadata.api_trade_available = false;
        metadata.buy_available = false;
        metadata.sell_available = true;
        metadata.required_tests = vec!["qualified_investor".to_string(), "margin".to_string()];

        let instrument = build_equity_instrument(&metadata).unwrap();
        let restored = TbankInstrumentMetadata::from_instrument(&instrument).unwrap();

        assert!(!restored.api_trade_available);
        assert!(!restored.buy_available);
        assert!(restored.sell_available);
        assert_eq!(
            restored.required_tests,
            vec!["qualified_investor".to_string(), "margin".to_string()]
        );
    }

    #[test]
    fn cached_equity_restores_ticker_from_raw_symbol() {
        let instrument = build_equity_instrument(&sber()).unwrap();
        let InstrumentAny::Equity(mut equity) = instrument else {
            panic!("expected Equity");
        };
        equity.raw_symbol = Symbol::new("SBER");

        let restored =
            TbankInstrumentMetadata::from_instrument(&InstrumentAny::Equity(equity)).unwrap();

        assert_eq!(restored.instrument_id, "SBER_TQBR.MOEX");
        assert_eq!(restored.ticker, "SBER");
    }

    #[test]
    fn cached_instrument_requires_current_trading_metadata() {
        let mut info = Params::new();
        info.insert("required_tests".to_string(), "".into());
        info.insert("api_trade_available".to_string(), true.into());
        info.insert("sell_available".to_string(), true.into());

        assert!(TbankInstrumentMetadata::trading_flags_from_info(&info).is_none());
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
            real_exchange: crate::grpc::generated::RealExchange::Moex as i32,
            ..Share::default()
        };

        let metadata = TbankInstrumentMetadata::from_share(&share).unwrap();
        assert_eq!(metadata.instrument_id, "SBER_TQBR.MOEX");
        assert_eq!(metadata.exchange, "moex_mrng_evng_e_wknd_dlr");
    }
}
