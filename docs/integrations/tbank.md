# T-Bank adapter capability matrix

This matrix describes the public behavior of `tbank-nt-community` `0.1.x` with NautilusTrader
`v1.231.0`.

## Instruments and data

| Capability | Status | Notes |
| --- | --- | --- |
| MOEX TQBR equities | Supported | Canonical ID: `{ticker}_{class_code}.MOEX` |
| Instrument discovery | Supported | TQBR/RUB shares are loaded during `DataClient::connect` |
| Live quotes and trades | Supported | T-Bank market-data streams; quote subscriptions are chunked to 300 instruments per stream and prices use canonical instrument precision |
| Order book snapshots | Supported | Snapshot semantics; native venue deltas are unavailable |
| External bars | Supported | Sparse streamed candles; pre-ACK data is buffered, an acknowledged stream opens before startup/reconnect `GetCandles` recovery, and all recovery shares one request limiter |
| Historical bars | Supported | `RequestBars` on the main data client; requests are chunked |
| Historical trades | Limited | `RequestTrades` on the main data client; T-Bank guarantees only the recent `GetLastTrades` window |
| Archive/catalog downloader | Not provided | Archive ingestion and catalog policy belong in consumers |
| ETFs, bonds, futures, currencies | Not supported | Identity and execution semantics are not complete |

## Execution

| Nautilus order type | Status | Notes |
| --- | --- | --- |
| Market | Supported | Broker-native submission and reconciliation |
| Limit | Supported | Broker-native submission and cancellation |
| StopMarket | Supported | Mapped to T-Bank stop-loss market behavior |
| MarketIfTouched | Supported | Mapped to T-Bank take-profit market behavior |
| TrailingStopMarket | Supported | Price and basis-point trailing offsets |
| TrailingStopLimit | Supported | Requires `limit_offset` |
| Native order book deltas | Not supported | T-Bank does not provide authoritative delta semantics |
| `OrderFillVoided` | Not supported | Broker correction data is insufficient to construct the event safely |

Trailing offsets in ticks or price tiers and trigger sources other than `Default`/`LastPrice` are
rejected locally. Live order submission requires both `enable_trading = true` and
`allow_live_trading = true`; sandbox and live credentials are never interchangeable.

### Execution command surface

| Nautilus command | Status | Behavior |
| --- | --- | --- |
| `SubmitOrder` | Supported | Local validation emits `OrderDenied`; `OrderSubmitted` is emitted only after preflight succeeds |
| `SubmitOrderList` | Limited | Independent orders only; every leg is preflighted before any leg is submitted |
| Contingent OCO/OTO/OUO lists | Not supported | The complete list is denied locally; no leg is sent to T-Bank |
| `ModifyOrder` | Not supported | Emits modify-rejected; cancel and submit a replacement |
| `CancelOrder` | Supported | Resolves the canonical broker route before cancellation |
| `CancelAllOrders` / `BatchCancelOrders` | Supported | Cancels the resolved open-order set |
| Order/fill/position reports | Supported | Query-backed reports and reconnect reconciliation |

## Distribution boundary

The adapter is distributed only from immutable signed Git tags. It is not published to crates.io.
There are no Python bindings, downloader crate, catalog CLI, strategy, portfolio, or live-runner
components in this repository.
