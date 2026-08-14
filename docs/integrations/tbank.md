# T-Bank adapter capability matrix

This matrix describes the public behavior of `tbank-nt-community` `0.2.x` with NautilusTrader
`v1.231.0`.

## Instruments and data

| Capability | Status | Notes |
| --- | --- | --- |
| MOEX TQBR equities | Supported | Canonical ID: `{ticker}_{class_code}.MOEX` |
| SPB equities | Supported | Canonical ID: `{ticker}_{class_code}.SPBE`; includes `SPBKZ` shares settled in `KZT` |
| MOEX futures | Supported | Canonical ID: `{ticker}_{class_code}.MOEX`; Nautilus `FuturesContract`, points pricing, expiry and contract multiplier |
| Instrument discovery | Supported | Supported shares and futures are loaded during `DataClient::connect`; standard instrument filters can narrow requests |
| Live quotes and trades | Supported | T-Bank market-data streams; quote subscriptions are chunked to 300 instruments per stream and prices use canonical instrument precision |
| Order book snapshots | Supported | Snapshot semantics; native venue deltas are unavailable |
| External bars | Supported | Sparse streamed candles; pre-ACK data is buffered, an acknowledged stream opens before startup/reconnect `GetCandles` recovery, and all recovery shares one request limiter |
| Historical bars | Supported | `RequestBars` on the main data client; requests are chunked |
| Historical trades | Limited | `RequestTrades` on the main data client; T-Bank guarantees only the recent `GetLastTrades` window |
| Archive/catalog downloader | Not provided | Archive ingestion and catalog policy belong in consumers |
| OTC, dealer, ETF, bonds, currencies | Not supported | These instrument families remain outside the adapter scope |

Non-tradable indicative instruments are opt-in: a consumer supplies each definition through
`TbankDataClientConfig::indicative_instruments` when it needs the instrument for market data or
historical requests. The adapter has no built-in market-context symbol.

T-Bank live `Trade` messages do not include a venue match ID. The adapter therefore emits a
bounded synthetic `TradeId` derived from the immutable trade fields and the adapter-wide stream
message sequence. The sequence disambiguates distinct ticks with identical timestamps and
payloads; it is not a broker identifier.

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

Futures quantities are contracts in Nautilus and whole broker lots at the transport boundary.
Regular and stop futures orders use point prices and send `PriceType::Point`; stop-order reports
and order-trade fields are also consumed as points. `OperationItemTrade` prices from cursor
operations are already point-valued and are passed through without tick conversion. Explicitly
currency-valued execution averages and the legacy `GetOperations`/`GetSandboxOperations` paths
are converted through the current tick amount.
FOK is rejected locally for futures until the broker provides equivalent semantics.

### 0.2.x routing migration

Register one T-Bank data client and one T-Bank execution client with the same routing configuration:

```rust
let routing = RoutingConfig::builder()
    .default(true)
    .venues(vec!["MOEX".to_string(), "SPBE".to_string()])
    .build();
node = node
    .add_data_client_with_routing(None, Box::new(TbankDataClientFactory::new()),
        Box::new(data_config), routing.clone())?
    .add_exec_client_with_routing(None, Box::new(TbankExecutionClientFactory::new()),
        Box::new(execution_config), routing)?;
```

The broker-only transport enum value `REAL_EXCHANGE_RTS` is mapped once at the transport boundary
to public `SPBE`; it is never part of a Nautilus instrument ID, route, event, cache key, or error.
`KZT` is registered by the adapter as ISO 4217 code 398 before SPBKZ instruments or persisted
Nautilus cache entries are decoded. Consumers which deserialize T-Bank state before constructing
an adapter client must call `register_tbank_currencies()` first.

## Distribution boundary

The adapter is distributed only from immutable signed Git tags. It is not published to crates.io.
There are no Python bindings, downloader crate, catalog CLI, strategy, portfolio, or live-runner
components in this repository.
