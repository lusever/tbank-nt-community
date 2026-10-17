//! Reproduce source-separated recording and fresh-process reading without a broker connection.
use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, identifiers::InstrumentId};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, common::custom::group_custom_data_by_type,
};
use std::path::Path;
use tbank_nt_community::{
    TbankBookSource, TbankBookSubscription, TbankOrderBookSnapshot,
    market_data::TbankOrderBookLevel, register_tbank_custom_data,
};
fn key(source: TbankBookSource) -> TbankBookSubscription {
    TbankBookSubscription::new(InstrumentId::from("SBER_TQBR.MOEX"), source, 10).unwrap()
}
fn main() -> anyhow::Result<()> {
    register_tbank_custom_data();
    let args = std::env::args().collect::<Vec<_>>();
    anyhow::ensure!(
        args.len() == 3,
        "usage: book_catalog write|read <catalog-directory>"
    );
    if args[1] == "write" {
        std::fs::create_dir_all(&args[2])?;
    }
    let catalog = ParquetDataCatalog::new(Path::new(&args[2]), None, None, None, None);
    if args[1] == "write" {
        let mut batch = Vec::new();
        for source in [TbankBookSource::Exchange, TbankBookSource::Dealer] {
            for n in 1..=3_u64 {
                batch.push(
                    TbankOrderBookSnapshot {
                        instrument_id: key(source).instrument_id,
                        source,
                        depth: 10,
                        is_consistent: true,
                        bids: vec![TbankOrderBookLevel {
                            price: rust_decimal::Decimal::new(123456789123, 9),
                            quantity_lots: n as i64,
                        }],
                        asks: vec![],
                        broker_timestamp: UnixNanos::from(n * 100),
                        ts_event: UnixNanos::from(n * 100),
                        ts_init: UnixNanos::from(n * 100 + 10),
                    }
                    .into_custom_data(),
                );
            }
        }
        for group in group_custom_data_by_type(batch.iter()) {
            let rows = group.into_iter().cloned().collect::<Vec<_>>();
            catalog.write_custom_data_batch(&rows, None, None, Some(false))?;
        }
        println!("wrote 3 exchange + 3 dealer snapshots");
    } else {
        anyhow::ensure!(args[1] == "read", "expected write or read");
        let mut catalog = catalog;
        for source in [TbankBookSource::Exchange, TbankBookSource::Dealer] {
            let dt = key(source).data_type();
            let ids = vec![dt.identifier().unwrap().to_string()];
            let rows = catalog.query_custom_data_dynamic(
                "TbankOrderBookSnapshot",
                Some(&ids),
                None,
                None,
                None,
                None,
                true,
            )?;
            anyhow::ensure!(rows.len() == 3, "lost records for {source:?}");
            for row in &rows {
                let Data::Custom(c) = row else {
                    anyhow::bail!("non-custom record")
                };
                anyhow::ensure!(c.data_type == dt, "routing changed");
                let book = c
                    .data
                    .as_any()
                    .downcast_ref::<TbankOrderBookSnapshot>()
                    .unwrap();
                anyhow::ensure!(
                    book.source == source
                        && book.bids[0].price == rust_decimal::Decimal::new(123456789123, 9),
                    "payload changed"
                );
            }
            println!("{} records={} source={source:?}", ids[0], rows.len());
        }
    }
    Ok(())
}
