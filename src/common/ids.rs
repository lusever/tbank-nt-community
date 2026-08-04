use std::str::FromStr;

use crate::common::{
    consts::{MOEX, TQBR_CLASS_CODE},
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
}

impl FromStr for TbankInstrumentIdParts {
    type Err = TbankAdapterError;

    fn from_str(value: &str) -> Result<Self> {
        let (symbol, venue) = value
            .split_once('.')
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
    format!("{ticker}_{class_code}.{MOEX}")
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
}
