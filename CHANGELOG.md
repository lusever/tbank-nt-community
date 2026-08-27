# Changelog

## 0.2.1 - 2026-08-27

- Adds venue-aware MOEX/SPB share and MOEX futures routing, historical and market-data support,
  execution lifecycle reconciliation, broker order routing, metadata recovery, and atomic catalog
  reloads.
- Hardens market-data stream recovery with stable logical stream identities, explicit retirement,
  supervised worker completion, bounded historical-gap recovery, single-flight coordination, shared
  request limiting, circuit breaking, and watermark-gated candle readiness.
- Expands offline and sandbox coverage for execution lifecycle, projections, futures, and market-data
  recovery.

## 0.1.0 - 2026-08-10

- Initial release of the Rust-native T-Bank Invest API adapter for NautilusTrader, including
  instrument, market-data, historical-data, and execution clients.
