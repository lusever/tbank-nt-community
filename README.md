# tbank-nt-community

> This is an independent community project. It is not affiliated with, endorsed by, or supported
> by Nautech Systems Pty Ltd or the official NautilusTrader project.

Rust-native T-Bank Invest API adapter for NautilusTrader.

The repository contains venue integration code only. Trading strategies, portfolio policy,
position sizing, research, and live-runner orchestration belong in consumer repositories.

## Status

Rust-only adapter with NautilusTrader pinned to a Git source revision.

Compatibility:

| tbank-nt-community | NautilusTrader | Rust | T-Bank contracts |
| --- | --- | --- | --- |
| `0.1.x` | `v1.231.0` | `1.97.1` | Release 1.49, `ef3337c71b7d6dffe61dfdef814fc4e603004f8b` |

## Scope

Supported:

- MOEX TQBR equities.
- Sandbox and live environments.
- Instruments and symbol mapping.
- Market data bars, trades, quotes, and order book snapshots.
- Historical bars and trades through the main Nautilus data client.
- Market, limit, stop-market, take-profit-market, trailing-stop-market, and
  trailing-stop-limit execution mapping.
- Broker order routing, deterministic request idempotency, and Nautilus execution reconciliation.

Not currently supported:

- Python bindings.
- ETFs, bonds, futures, and currencies as complete trading products.
- Native venue order book deltas.
- `OrderFillVoided`: T-Bank does not publish an authoritative trade-void/correction reference,
  voided quantity, or reopened-order signal, so the adapter does not infer this event from order or
  operation cancellation states.

## Distribution and use

Releases are **Git-only**. This crate is intentionally not published to crates.io
(`publish = false`); immutable signed Git tags are the distribution artifacts.

Use a local path during development:

```toml
tbank-nt-community = { path = "../tbank-nt-community" }
```

For a reproducible Git dependency, use an immutable tag:

```toml
tbank-nt-community = { git = "https://github.com/lusever/tbank-nt-community.git", tag = "v0.1.0" }
```

All direct Nautilus dependencies in the consumer must use the same `v1.231.0` source as this
adapter. Mixing registry, branch, and Git-tag sources can create incompatible duplicate Nautilus
domain types.

Native trailing stops accept Nautilus `TrailingOffsetType::Price` and
`TrailingOffsetType::BasisPoints`. Basis points are converted to the percentage representation
required by T-Bank (`100 bps = 1%`). `Ticks`, `PriceTier`, and trigger sources other than
`Default`/`LastPrice` are rejected locally because T-Bank cannot preserve those semantics. `TrailingStopLimit` requires a
`limit_offset`; omitting `activation_price` requests T-Bank's immediate trailing activation.

Order submission follows Nautilus lifecycle semantics: local validation failures emit
`OrderDenied`, while `OrderSubmitted` is emitted only after local preflight succeeds. Order lists
are supported for independent orders with all-leg preflight; contingent OCO/OTO/OUO lists are
denied as a whole because T-Bank cannot preserve their semantics.

The adapter follows Nautilus venue-adapter naming conventions: `TBANK` is the canonical client
identifier and `MOEX` is the canonical venue identifier. Typed values are exported as
`TBANK_CLIENT_ID` and `MOEX_VENUE`; instrument IDs use the
`{ticker}_{class_code}.MOEX` form and broker accounts use `TBANK-{broker_account_id}`.

Nautilus supplies the concrete client config and client name to each `create` call. Both client
factories are stateless and reject a wrong config type instead of silently substituting defaults.
The execution config owns the trader identity, matching Nautilus' adapter config contract.

Minimal `LiveNode` wiring:

```rust
use nautilus_common::enums::Environment as NautilusEnvironment;
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::TraderId;
use tbank_nt_community::{
    TbankDataClientConfig, TbankDataClientFactory, TbankEnvironment,
    TbankExecutionClientConfig, TbankExecutionClientFactory,
};

let trader_id = TraderId::from("TRADER-001");
let data_config = TbankDataClientConfig {
    environment: TbankEnvironment::Sandbox,
    ..TbankDataClientConfig::default()
};
let execution_config = TbankExecutionClientConfig {
    trader_id,
    environment: TbankEnvironment::Sandbox,
    ..TbankExecutionClientConfig::default()
};

let node = LiveNode::builder(trader_id, NautilusEnvironment::Sandbox)?
    .add_data_client(
        Some("tbank".to_string()),
        Box::new(TbankDataClientFactory::new()),
        Box::new(data_config),
    )?
    .add_exec_client(
        Some("tbank".to_string()),
        Box::new(TbankExecutionClientFactory::new()),
        Box::new(execution_config),
    )?
    .build()?;
# Ok::<(), anyhow::Error>(())
```

The data client loads the supported TQBR/RUB instrument universe during `connect`, publishes
Nautilus `Instrument` events before market data starts, and uses the same metadata to map T-Bank
lot quantities to Nautilus share quantities. Custom remote endpoints must use HTTPS; plaintext HTTP
is accepted only for loopback test servers.

Historical `RequestBars` and `RequestTrades` messages are handled by that same data client, as in
the Nautilus OKX, Bybit, and Deribit adapters. There is no downloader crate or catalog CLI in this
repository. Candle requests are chunked to T-Bank limits; `GetLastTrades` is limited to T-Bank's
guaranteed recent window. Catalog materialization and long-range archive ingestion belong in the
consumer that owns the data policy.

See the [capability matrix](docs/integrations/tbank.md) for the exact supported request and order
surface.

The crate enables Nautilus `high-precision` mode by default, matching official venue adapters. A
consumer that disables default features must make the same precision choice for all direct Nautilus
dependencies.

Tokens and account IDs can be supplied in config, but environment variables are preferred:

```dotenv
# Live
TBANK_INVEST_TOKEN=...
TBANK_ACCOUNT_ID=...

# Sandbox
TBANK_SANDBOX_INVEST_TOKEN=...
TBANK_SANDBOX_ACCOUNT_ID=...
```

`TBANK_INVEST_TOKEN` is the live-token environment variable.

Secret values are redacted from config serialization and debug output.

## Offline validation

```bash
bash scripts/check-proto-contracts.sh
cargo fmt --check
cargo check --locked --all-targets --no-default-features
cargo test --locked
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
bash scripts/check-public-method-docs.sh
```

Default tests do not connect to T-Bank and do not submit orders.

## T-Bank sandbox validation

Required environment variables:

- `TBANK_SANDBOX_INVEST_TOKEN`
- `TBANK_SANDBOX_ACCOUNT_ID` for account and order tests
- `TBANK_SANDBOX_TEST_INSTRUMENT` optionally overrides `SBER_TQBR.MOEX`
- `TBANK_SANDBOX_PAY_IN_RUB` optionally funds the configured sandbox account

Store these values in the repository-local `.env`, which is excluded from Git, and restrict the
file to the current user. The tests do not load `.env` themselves, so export it into the current
shell before running any sandbox command:

```bash
chmod 600 .env
set -a
source .env
set +a
```

Do not print the file or put its values directly into commands, logs, chat, or tracked files.

Read-only checks:

```bash
cargo test --locked \
  --features sandbox-tests \
  --test sandbox_integration \
  sandbox_readonly \
  -- --ignored --exact --nocapture
```

Limit/stop placement and cancellation:

```bash
cargo test --locked \
  --features sandbox-tests \
  --test sandbox_integration \
  sandbox_order_lifecycle \
  -- --ignored --exact --nocapture
```

Market-fill round trip:

```bash
cargo test --locked \
  --features sandbox-tests \
  --test sandbox_integration \
  sandbox_market_fill \
  -- --ignored --exact --nocapture
```

Full sandbox acceptance:

```bash
cargo test --locked \
  --features sandbox-tests \
  --test sandbox_integration \
  -- --ignored --test-threads=1 --nocapture
```

For adapter, protobuf, transport, factory, execution, market-data, config, or sandbox-test changes,
agents run this full acceptance suite by default immediately after `cargo test --locked`. The
repository instructions provide standing authorization for sandbox mutations only; live trading is
never implied. A failed or skipped acceptance run must be reported and is not a successful
validation result.

The feature flag, ignored-test boundary, and explicit test name are the opt-in for sandbox order
submission. A full acceptance run intentionally submits both resting and market orders. Tests
serialize access to the account and clean up regular orders, stop orders, and market-fill position
changes. Missing sandbox credentials or account configuration fails the affected test instead of
self-skipping it.

Sandbox execution acceptance drives the public Nautilus `ExecutionClient` boundary and asserts
`OrderSubmitted`, execution reports, account state, cancellation, and report-based recovery after
creating a fresh client. It does not call adapter-internal submit helpers or inspect adapter-owned
JSON/JSONL state.

## Safety

The execution client is dry by default. Order submission requires `enable_trading = true`.
Live submission additionally requires `allow_live_trading = true`.
Execution `connect` publishes the initial account state and waits up to
`account_registration_timeout` (30 seconds by default) for Nautilus to register that account before
reporting the client as connected.

Never place access tokens in tracked repository files, command output, fixtures, or CI artifacts.
Broker tracking metadata, account identifiers, and venue order identifiers are not included in
human-readable adapter errors or logs.

The adapter does not persist parallel lifecycle, event-journal, or market-data-health files.
Nautilus execution events and reconciliation reports are the execution contract. The adapter
keeps one debounced desired subscription snapshot, opens each broker stream and validates its
subscription acknowledgement before publishing runtime data. Venue data that races ahead of the
acknowledgement is held in a bounded per-stream buffer and drained in order after the ACK.
`GetCandles` catch-up then runs
at startup and after reconnects behind one client-wide request limiter shared by every stream group
and periodic poller. Recovery never runs in front of a closed stream, never resets reconnect
backoff, and does not infer missing data from sparse wall-clock minutes. Stream and recovery
transitions are emitted through structured tracing and typed `TbankMarketDataStreamEvent`s for
consumer health projection. A panicked stream session is converted into a supervised failure and
reconnected instead of silently detaching its task. Every Nautilus market-data `Price` uses the
instrument precision derived from `min_price_increment`; wire-value scale is never treated as
instrument metadata. Consumers own readiness policy and any durable operational storage.

## Tools

The package includes one read-only operational tool behind the opt-in `tools` feature:

- `tbank-accounts`

Use `cargo run --features tools --bin <name> -- --help` for arguments.
Universe selection, margin snapshots, strategy configuration, and other research tooling belong
in adapter consumers.

## Proto contracts

T-Bank protobuf contracts are vendored under `proto/`. Their source revision is recorded in
`proto/contracts.lock`; `proto/contracts.sha256` pins the exact vendored file set and contents.
Contract updates must be isolated, reviewed, regenerated, and validated against offline and sandbox
suites before release. T-Bank confirmed that the public contracts and generated bindings may be
redistributed under the Apache License 2.0, including as part of an independent open-source project
and crates.io package. The permission record and attribution are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## License

The adapter's original source code is licensed under the Apache License 2.0. See
[LICENSE](LICENSE). Vendored T-Bank protobuf contracts and generated bindings are also distributed
under the Apache License 2.0 under their respective ownership; third-party files retain their
respective terms. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and release expectations and
[SECURITY.md](SECURITY.md) for responsible vulnerability reporting.
