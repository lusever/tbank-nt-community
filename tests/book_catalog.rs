use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomData, Data},
    identifiers::InstrumentId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use std::{path::Path, process::Command};
use tbank_nt_community::{
    market_data::{
        TbankBookSource, TbankBookSubscription, TbankOrderBookLevel, TbankOrderBookSnapshot,
    },
    register_tbank_custom_data,
};

fn key(source: TbankBookSource) -> TbankBookSubscription {
    TbankBookSubscription::new(InstrumentId::from("SBER_TQBR.MOEX"), source, 10).unwrap()
}
fn snapshot(source: TbankBookSource, seq: u64) -> CustomData {
    TbankOrderBookSnapshot {
        instrument_id: key(source).instrument_id,
        source,
        depth: 10,
        is_consistent: true,
        bids: vec![TbankOrderBookLevel {
            price: rust_decimal::Decimal::new(123456789012345, 9),
            quantity_lots: seq as i64,
        }],
        asks: vec![TbankOrderBookLevel {
            price: rust_decimal::Decimal::new(123456789012346, 9),
            quantity_lots: seq as i64 + 1,
        }],
        broker_timestamp: UnixNanos::from(seq * 100),
        ts_event: UnixNanos::from(seq * 100),
        ts_init: UnixNanos::from(seq * 100 + 10),
    }
    .into_custom_data()
}
fn verify(path: &Path) -> anyhow::Result<()> {
    register_tbank_custom_data();
    let mut catalog = ParquetDataCatalog::new(path, None, None, None, None);
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
        anyhow::ensure!(rows.len() == 3, "lost events for {source:?}");
        for (seq, row) in rows.iter().enumerate() {
            let Data::Custom(custom) = row else {
                anyhow::bail!("unexpected record")
            };
            anyhow::ensure!(
                *custom == snapshot(source, seq as u64 + 1),
                "payload, precision, timestamps or routing changed"
            );
            anyhow::ensure!(
                TbankBookSubscription::from_data_type(&custom.data_type)? == key(source),
                "routing changed"
            );
        }
    }
    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        msgbus::{self, MessageBus, stubs::get_any_saving_handler, switchboard},
    };
    use nautilus_model::identifiers::TraderId;
    use std::{cell::RefCell, rc::Rc};
    let _bus = MessageBus::new(
        TraderId::from("BOOK-REPLAY"),
        nautilus_core::UUID4::new(),
        None,
        None,
    )
    .register_message_bus();
    let mut engine = nautilus_data::engine::DataEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::new(RefCell::new(Cache::default())),
        None,
    );
    let mut sinks = Vec::new();
    for source in [TbankBookSource::Exchange, TbankBookSource::Dealer] {
        let topic = switchboard::get_custom_topic(&key(source).data_type());
        let (handler, sink) = get_any_saving_handler::<CustomData>(None);
        msgbus::subscribe_any(topic.into(), handler, None);
        sinks.push((source, sink));
    }
    for row in catalog.query_custom_data_dynamic(
        "TbankOrderBookSnapshot",
        None,
        None,
        None,
        None,
        None,
        true,
    )? {
        engine.process_data(row);
    }
    anyhow::ensure!(engine.data_count() == 6, "replay lost records");
    for (source, sink) in sinks {
        let rows = sink.get_messages();
        anyhow::ensure!(
            rows.len() == 3 && rows.iter().all(|c| c.data_type == key(source).data_type()),
            "cross-source replay delivery"
        );
    }
    Ok(())
}
#[test]
fn catalog_fresh_process_reader() -> anyhow::Result<()> {
    if let Some(path) = std::env::var_os("TBANK_BOOK_TEST_CATALOG") {
        verify(Path::new(&path))?;
    }
    Ok(())
}
#[test]
fn both_sources_with_identical_timestamps_survive_a_fresh_process() -> anyhow::Result<()> {
    register_tbank_custom_data();
    let path = std::env::temp_dir().join(format!("tbank-books-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path)?;
    let catalog = ParquetDataCatalog::new(&path, None, None, None, None);
    let rows = (1..=3)
        .flat_map(|seq| {
            [
                snapshot(TbankBookSource::Exchange, seq),
                snapshot(TbankBookSource::Dealer, seq),
            ]
        })
        .map(Data::Custom)
        .collect::<Vec<_>>();
    catalog.write_data_enum(&rows, None, None, Some(false))?;
    verify(&path)?;
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "catalog_fresh_process_reader", "--nocapture"])
        .env("TBANK_BOOK_TEST_CATALOG", &path)
        .status()?;
    anyhow::ensure!(status.success(), "separate-process replay failed");
    std::fs::remove_dir_all(path)?;
    Ok(())
}
