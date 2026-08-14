use std::{fmt, str::FromStr};

use nautilus_model::identifiers::Venue;
use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TbankAdapterError};

/// Canonical public venues supported by the T-Bank adapter.
///
/// `REAL_EXCHANGE_RTS` is a T-Bank transport enum and is deliberately not part
/// of this public domain type. The transport mapper translates it to [`Self::Spbe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TbankVenue {
    /// Moscow Exchange.
    Moex,
    /// Saint Petersburg Exchange.
    Spbe,
}

impl TbankVenue {
    /// Returns every public exchange venue supported by the adapter.
    pub(crate) const fn all() -> [Self; 2] {
        [Self::Moex, Self::Spbe]
    }

    /// Returns the canonical Nautilus venue string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Moex => crate::common::consts::MOEX,
            Self::Spbe => crate::common::consts::SPBE,
        }
    }

    /// Returns the ISO 10383 MIC for the venue's real execution venue.
    #[must_use]
    pub const fn mic(self) -> &'static str {
        match self {
            Self::Moex => crate::common::consts::MOEX,
            Self::Spbe => crate::common::consts::SPBX_MIC,
        }
    }

    /// Returns the corresponding Nautilus venue identifier.
    #[must_use]
    pub fn venue(self) -> Venue {
        Venue::new(self.as_str())
    }
}

impl fmt::Display for TbankVenue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TbankVenue {
    type Err = TbankAdapterError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_uppercase().as_str() {
            crate::common::consts::MOEX => Ok(Self::Moex),
            crate::common::consts::SPBE => Ok(Self::Spbe),
            _ => Err(TbankAdapterError::ConfigError(format!(
                "unsupported T-Bank venue {value}"
            ))),
        }
    }
}

/// Instrument families supported by the T-Bank provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TbankInstrumentType {
    /// Exchange share.
    Share,
    /// Deliverable futures contract.
    Futures,
}

impl fmt::Display for TbankInstrumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Share => "share",
            Self::Futures => "futures",
        })
    }
}
