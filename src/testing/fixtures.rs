use rust_decimal::Decimal;

use crate::instruments::TbankInstrumentMetadata;

/// Builds deterministic SBER instrument metadata for tests.
pub fn sber_metadata() -> TbankInstrumentMetadata {
    TbankInstrumentMetadata {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        figi: "BBG004730N88".to_string(),
        instrument_uid: "e6123145-9665-43e0-8413-cd61b8aa9b13".to_string(),
        position_uid: "position-sber".to_string(),
        lot: 10,
        min_price_increment: Decimal::new(1, 2),
        currency: "RUB".to_string(),
        exchange: "MOEX".to_string(),
        price_precision: 2,
        quantity_precision: 0,
        ..Default::default()
    }
}
