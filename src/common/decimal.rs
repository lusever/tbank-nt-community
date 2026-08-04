use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    common::{
        consts::NANOS_PER_UNIT,
        error::{Result, TbankAdapterError},
    },
    grpc::generated::{MoneyValue, Quotation},
};

/// Converts a T-Bank quotation to a decimal value.
pub fn quotation_to_decimal(value: &Quotation) -> Decimal {
    Decimal::from(value.units) + Decimal::from(value.nano) / Decimal::from(NANOS_PER_UNIT)
}

/// Converts a decimal value to a T-Bank quotation.
pub fn decimal_to_quotation(value: Decimal) -> Result<Quotation> {
    let units_decimal = value.trunc();
    let units = units_decimal.to_i64().ok_or_else(|| {
        TbankAdapterError::ConversionError(format!("units out of range: {value}"))
    })?;
    let nano_decimal = (value - Decimal::from(units)) * Decimal::from(NANOS_PER_UNIT);

    if !nano_decimal.fract().is_zero() {
        return Err(TbankAdapterError::ConversionError(format!(
            "decimal has more than 9 fractional digits: {value}"
        )));
    }

    let nano = nano_decimal
        .to_i32()
        .ok_or_else(|| TbankAdapterError::ConversionError(format!("nano out of range: {value}")))?;

    Ok(Quotation { units, nano })
}

/// Converts a T-Bank money value to a decimal amount.
pub fn money_value_to_decimal(value: &MoneyValue) -> Decimal {
    Decimal::from(value.units) + Decimal::from(value.nano) / Decimal::from(NANOS_PER_UNIT)
}

/// Builds a T-Bank money value from a currency and decimal amount.
pub fn decimal_to_money_value(currency: impl Into<String>, value: Decimal) -> Result<MoneyValue> {
    let quotation = decimal_to_quotation(value)?;
    Ok(MoneyValue {
        currency: currency.into(),
        units: quotation.units,
        nano: quotation.nano,
    })
}

/// Validates that a price is aligned to the instrument tick size.
pub fn ensure_price_on_tick(price: Decimal, tick_size: Decimal) -> Result<()> {
    if tick_size <= Decimal::ZERO {
        return Err(TbankAdapterError::InvalidPrice(format!(
            "tick size must be positive, got {tick_size}"
        )));
    }
    let units = price / tick_size;
    if !units.fract().is_zero() {
        return Err(TbankAdapterError::PriceNotMultipleOfTick {
            price: price.to_string(),
            tick: tick_size.to_string(),
        });
    }
    Ok(())
}

/// Validates and converts a price to a T-Bank quotation.
pub fn price_to_quotation(
    price: Decimal,
    tick_size: Decimal,
    price_precision: u32,
) -> Result<Quotation> {
    let effective_scale = price.normalize().scale();
    if effective_scale > price_precision {
        return Err(TbankAdapterError::InvalidPrice(format!(
            "price precision {} exceeds instrument precision {price_precision}",
            effective_scale
        )));
    }
    ensure_price_on_tick(price, tick_size)?;
    decimal_to_quotation(price)
}

/// Converts a Nautilus share quantity to whole T-Bank lots.
pub fn quantity_shares_to_lots(quantity_shares: Decimal, lot_size: u32) -> Result<i64> {
    if lot_size == 0 {
        return Err(TbankAdapterError::InvalidQuantity(
            "lot size must be positive".to_string(),
        ));
    }
    if quantity_shares <= Decimal::ZERO || !quantity_shares.fract().is_zero() {
        return Err(TbankAdapterError::InvalidQuantity(format!(
            "share quantity must be a positive whole number, got {quantity_shares}"
        )));
    }

    let lot = Decimal::from(lot_size);
    let lots = quantity_shares / lot;
    if !lots.fract().is_zero() {
        return Err(TbankAdapterError::QuantityNotMultipleOfLot {
            quantity: quantity_shares.to_string(),
            lot: lot_size,
        });
    }

    lots.to_i64()
        .ok_or_else(|| TbankAdapterError::InvalidQuantity(format!("lots out of i64 range: {lots}")))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn quotation_positive_to_decimal() {
        assert_eq!(
            quotation_to_decimal(&Quotation {
                units: 123,
                nano: 450_000_000
            }),
            Decimal::new(12_345, 2)
        );
    }

    #[test]
    fn quotation_negative_to_decimal() {
        assert_eq!(
            quotation_to_decimal(&Quotation {
                units: -123,
                nano: -450_000_000
            }),
            Decimal::new(-12_345, 2)
        );
    }

    #[test]
    fn decimal_to_quotation_positive() {
        assert_eq!(
            decimal_to_quotation(Decimal::new(12_345, 2)).unwrap(),
            Quotation {
                units: 123,
                nano: 450_000_000
            }
        );
    }

    #[test]
    fn money_value_round_trip() {
        let money = decimal_to_money_value("RUB", Decimal::new(10_001, 2)).unwrap();
        assert_eq!(money.currency, "RUB");
        assert_eq!(money_value_to_decimal(&money), Decimal::new(10_001, 2));
    }

    #[test]
    fn quantity_shares_to_lots_validates_lot_multiple() {
        assert_eq!(quantity_shares_to_lots(Decimal::from(20), 10).unwrap(), 2);
        assert!(matches!(
            quantity_shares_to_lots(Decimal::from(15), 10),
            Err(TbankAdapterError::QuantityNotMultipleOfLot { .. })
        ));
    }

    #[test]
    fn price_tick_validation() {
        assert!(price_to_quotation(Decimal::new(25_010, 2), Decimal::new(1, 2), 2).is_ok());
        assert!(matches!(
            price_to_quotation(Decimal::new(250_105, 3), Decimal::new(1, 2), 3),
            Err(TbankAdapterError::PriceNotMultipleOfTick { .. })
        ));
    }

    #[test]
    fn price_precision_ignores_trailing_zeros() {
        assert!(price_to_quotation(Decimal::new(25_000_000_000, 8), Decimal::new(1, 2), 2).is_ok());
    }
}
