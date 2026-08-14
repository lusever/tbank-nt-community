use std::str::FromStr;

use crate::common::{
    consts::{MOEX, SPBE, SPBFUT_CLASS_CODE, TQBR_CLASS_CODE},
    error::{Result, TbankAdapterError},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Parsed components of a canonical T-Bank instrument identifier.
pub struct TbankInstrumentIdParts {
    /// Exchange ticker.
    pub ticker: String,
    /// T-Bank class code.
    pub class_code: String,
    /// Venue suffix.
    pub venue: String,
}

impl TbankInstrumentIdParts {
    /// Returns the canonical Nautilus instrument ID.
    pub fn instrument_id(&self) -> String {
        format!("{}_{}.{}", self.ticker, self.class_code, self.venue)
    }

    /// Returns the T-Bank ticker and class-code pair.
    pub fn ticker_class_code(&self) -> String {
        format!("{}_{}", self.ticker, self.class_code)
    }

    /// Returns whether the identifier denotes a MOEX TQBR equity.
    pub fn is_moex_tqbr_equity(&self) -> bool {
        self.venue == MOEX && self.class_code == TQBR_CLASS_CODE
    }

    /// Returns whether the identifier denotes a supported SPBE share.
    pub fn is_spbe_share(&self) -> bool {
        self.venue == SPBE && self.class_code != SPBFUT_CLASS_CODE
    }

    /// Returns whether the identifier denotes a supported MOEX futures contract.
    pub fn is_moex_futures(&self) -> bool {
        self.venue == MOEX && self.class_code == SPBFUT_CLASS_CODE
    }

    /// Returns whether the identifier belongs to a family supported by this adapter.
    pub fn is_supported_family(&self) -> bool {
        self.is_spbe_share() || self.is_moex_tqbr_equity() || self.is_moex_futures()
    }

    /// Returns whether the identifier has a supported public venue suffix.
    pub fn has_supported_venue(&self) -> bool {
        self.venue == MOEX || self.venue == SPBE
    }
}

impl FromStr for TbankInstrumentIdParts {
    type Err = TbankAdapterError;

    fn from_str(value: &str) -> Result<Self> {
        let (symbol, venue) = value
            .rsplit_once('.')
            .ok_or_else(|| TbankAdapterError::UnsupportedInstrument(value.to_string()))?;
        let (ticker, class_code) = symbol
            .rsplit_once('_')
            .ok_or_else(|| TbankAdapterError::UnsupportedInstrument(value.to_string()))?;

        if ticker.is_empty() || class_code.is_empty() || venue.is_empty() {
            return Err(TbankAdapterError::UnsupportedInstrument(value.to_string()));
        }

        Ok(Self {
            ticker: ticker.to_string(),
            class_code: class_code.to_string(),
            venue: venue.to_string(),
        })
    }
}

/// Builds a canonical Nautilus instrument ID from a T-Bank ticker and class code.
pub fn instrument_id_from_ticker_class(ticker: &str, class_code: &str) -> String {
    instrument_id_from_ticker_class_for_venue(ticker, class_code, MOEX)
}

/// Builds a canonical Nautilus instrument ID for an explicit public venue.
pub fn instrument_id_from_ticker_class_for_venue(
    ticker: &str,
    class_code: &str,
    venue: &str,
) -> String {
    format!("{ticker}_{class_code}.{venue}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sber_tqbr_moex() {
        let parts = TbankInstrumentIdParts::from_str("SBER_TQBR.MOEX").unwrap();
        assert_eq!(parts.ticker, "SBER");
        assert_eq!(parts.class_code, "TQBR");
        assert_eq!(parts.venue, "MOEX");
        assert!(parts.is_moex_tqbr_equity());
    }

    #[test]
    fn parses_spbe_without_transport_rts_name() {
        let parts = TbankInstrumentIdParts::from_str("AAPL_SPBXM.SPBE").unwrap();
        assert!(parts.is_spbe_share());
        assert!(parts.has_supported_venue());
        assert!(!parts.instrument_id().contains("RTS"));
    }

    #[test]
    fn parses_futures_ticker_containing_dot() {
        let parts = TbankInstrumentIdParts::from_str("Si-9.26_SPBFUT.MOEX").unwrap();
        assert_eq!(parts.ticker, "Si-9.26");
        assert_eq!(parts.class_code, "SPBFUT");
        assert_eq!(parts.venue, MOEX);
        assert!(parts.is_moex_futures());
    }

    #[test]
    fn family_checks_reject_wrong_venue_or_class_code() {
        let spbe_futures = TbankInstrumentIdParts::from_str("Si-9.26_SPBFUT.SPBE").unwrap();
        assert!(!spbe_futures.is_spbe_share());
        assert!(!spbe_futures.is_supported_family());

        let moex_bond = TbankInstrumentIdParts::from_str("BOND_TQTF.MOEX").unwrap();
        assert!(!moex_bond.is_moex_tqbr_equity());
        assert!(!moex_bond.is_moex_futures());
        assert!(!moex_bond.is_supported_family());
    }
}
