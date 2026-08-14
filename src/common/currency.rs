use std::str::FromStr;

use nautilus_model::{enums::CurrencyType, types::Currency};

use crate::common::{Result, TbankAdapterError};

const KZT_CODE: &str = "KZT";
const KZT_ISO4217: u16 = 398;
const KZT_NAME: &str = "Kazakhstani tenge";
const KZT_PRECISION: u8 = 2;

/// Registers fiat currencies required by supported T-Bank instrument families but absent from
/// NautilusTrader's built-in currency registry.
///
/// Call this before deserializing persistent Nautilus state which can contain T-Bank instruments.
pub fn register_tbank_currencies() -> anyhow::Result<()> {
    Currency::register(
        Currency::new(
            KZT_CODE,
            KZT_PRECISION,
            KZT_ISO4217,
            KZT_NAME,
            CurrencyType::Fiat,
        ),
        false,
    )?;
    Ok(())
}

pub(crate) fn currency_from_code(code: &str) -> Result<Currency> {
    register_tbank_currencies().map_err(|error| {
        TbankAdapterError::ConfigError(format!("failed to register T-Bank currencies: {error}"))
    })?;
    let normalized = code.to_uppercase();
    Currency::from_str(&normalized).map_err(|error| {
        TbankAdapterError::UnsupportedInstrument(format!(
            "unsupported currency {normalized}: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_kazakhstani_tenge_as_iso_fiat_currency() {
        register_tbank_currencies().unwrap();
        register_tbank_currencies().unwrap();

        let currency = Currency::from_str(KZT_CODE).unwrap();
        assert_eq!(currency.code.as_str(), KZT_CODE);
        assert_eq!(currency.precision, KZT_PRECISION);
        assert_eq!(currency.iso4217, KZT_ISO4217);
        assert_eq!(currency.name.as_str(), KZT_NAME);
        assert_eq!(currency.currency_type, CurrencyType::Fiat);
    }
}
