# Explicit execution methods and independent book sources

The contracts are pinned to Invest API Release_1.51 (`798f880d8010ef4fffe74d0f63d7e5a0d57f23f7`)
and NautilusTrader rc9 (`da6b57aefdd62b44f675e434ffcfefca7f4025dd`).
No protobuf update or separate execution engine is required.

## Submitting a command

Use the standard Nautilus `ExecutionClient::submit_order` with typed parameters:

```rust,ignore
use tbank_nt_community::{TbankSubmitMethod, TbankSubmitOptions};
let options = TbankSubmitOptions {
    method: TbankSubmitMethod::PostOrderAsync,
    deadline: Some(consumer_deadline_utc_ns),
    confirm_margin_trade: Some(true),
};
options.apply(command.params.get_or_insert_with(Default::default));
execution.submit_order(command)?;
```

Without the selector, regular orders retain `PostOrder`. There is no configuration-wide
method switch, fallback to another method, or adapter scheduling policy. Unknown/malformed
selectors and unsupported order/TIF combinations fail before mutation RPC dispatch.
The consumer deadline is UTC nanoseconds at the API boundary and becomes a monotonic deadline
for preparation, quota admission and dispatch. Existing request/recovery budgets remain upper
bounds. An expired local deadline is `NotSent`; failure after dispatch remains ambiguous.
Recovery can resolve the outcome without resubmitting the order.

| Explicit method | Order | TIF | Live and sandbox protocol support |
| --- | --- | --- | --- |
| PostOrder | Market | IOC | Yes; sandbox uses PostSandboxOrder |
| PostOrderAsync | Market | IOC | Yes; sandbox uses PostSandboxOrderAsync |
| Either | Limit, shares | DAY, IOC, FOK | Yes |
| Either | Limit, futures | DAY, IOC | Yes |
| Either | Futures FOK, market DAY/GTC, other regular TIF | — | Rejected locally |
| Either | Stop or trailing order | — | Explicit regular-method selector rejected locally |

Legacy stop submission remains available **without** a regular-method selector and retains its
existing stop-service/TIF rules. Capability and instrument flags can still cause broker rejection;
protocol support in this table is not a claim of live or sandbox economic acceptance.

Both regular methods share normalization, quantity-to-lots conversion, exact price conversion,
margin confirmation, deterministic ClientOrderId-to-request-UUID mapping and reconciliation.
Async acknowledgements contain their own response type and optional `trade_intent_id`.
Neither identifier becomes a VenueOrderId or an OrderAccepted event. Concurrent independent
commands and independent list legs can await results together; no GetOrderState barrier is
inserted between dispatches. Stream confirmation and later unary replies share existing lifecycle
projections and fill deduplication.

Transport retries retain the same UUID and selected method. Initial NEW stream receipt IDs are
not promoted to exchange IDs. Cached Nautilus orders seed request identities after restart;
GetOrderState with OrderIdType::Request resolves submissions interrupted before a unary reply.
Nautilus pending-cancel state restores already-received cancellation intent. The consumer must
configure Nautilus cache persistence for recovery across process loss; the adapter adds no database.

CancelOrder uses a confirmed exchange identity when available, otherwise a registered request
UUID with OrderIdType::Request. Regular submit admission registers the deterministic request UUID before metadata preparation.
Only imported routes without a registered request key retain the bounded identity-wait path. A cancel acknowledgement does not establish the absence of fills;
execution streams and reconciliation determine filled quantity and the remaining state. The adapter
never selects orders to cancel or cancels because a different order was accepted.

## Book subscriptions and snapshots

```rust,ignore
use tbank_nt_community::{TbankBookSource, TbankBookSubscription};
let exchange = TbankBookSubscription::new(instrument_id, TbankBookSource::Exchange, 10)?;
let dealer = TbankBookSubscription::new(instrument_id, TbankBookSource::Dealer, 10)?;
// Pass each key.data_type() to Nautilus SubscribeCustomData; retain that same full DataType
// for UnsubscribeCustomData. Both subscriptions can be active simultaneously.
```

Each subscription's complete key is `(InstrumentId, source, depth)`. It is checked against broker
acknowledgements and payloads, retained across reconnect, and used for unsubscribe. Duplicate
subscription admission is idempotent. All active custom book keys are sent together in one
server-side stream request; the shared stream becomes ready only after the broker confirms every
requested `(InstrumentId, source, depth)` key. Adding or removing a key rebuilds that grouped
subscription, while reconnect restores the complete active set. Unsubscribing one source does not
remove another source or the existing standard exchange depth/quote subscriptions. Dealer payloads cannot update
standard exchange book/quote projections. Broker subscription responses determine availability;
pending or failed acknowledgements do not establish availability.

The public lifecycle ID is `TbankBookSubscription::STREAM_ID` (`custom_book:all:group`),
including for a single subscription. `Connected` confirms all active keys; reconnect and
membership changes invalidate the whole group's readiness. Internal task generations do not
appear in this public ID. Consumers must use this grouped lifecycle contract when deploying
this adapter revision.

`TbankOrderBookSnapshot` is the registered custom type. Its real InstrumentId stays
`SBER_TQBR.MOEX`. Source and depth occur in the payload, DataType metadata and identifier:

- `SBER_TQBR.MOEX--exchange--10`
- `SBER_TQBR.MOEX--dealer--10`

Prices remain exact Decimal values, quantities are integer broker **lots**, and the broker
consistency flag is preserved even when false. `broker_timestamp` and `ts_event` are the broker's
book formation time; `ts_init` is the local UTC receipt time captured before conversion/publication.
A missing or invalid broker timestamp is an error; it is not replaced with local receipt time.

The ordinary `get_order_book` RPC and `get_order_book_snapshot` helper have **no source selector**,
as in the pinned API. They do not claim to return a specifically exchange or dealer snapshot and
never wait for a future stream update. No additional freshness threshold is imposed. The older
normalized transport model is now named `TbankWireOrderBookSnapshot` to distinguish it from
source-specific Nautilus CustomData.

## Recording and replay

Every writer and fresh reader process must call `register_tbank_custom_data()`. JSON and Arrow
codecs are registered process-locally. Preserve the full DataType produced by
`TbankBookSubscription::data_type()` when wrapping or persisting a snapshot.

On rc9, pass the mixed batch to the catalog. Keep each snapshot's full DataType and
`instrument--source--depth` identifier so sources with overlapping timestamps write separate files.

```rust,ignore
use nautilus_model::data::Data;

let batch: Vec<Data> = custom_records.into_iter().map(Data::Custom).collect();
catalog.write_data_enum(&batch, None, None, Some(false))?;
```

The same identifiers and payload schema are used for rc7 recordings. The consumer
(`nt-otc`) groups and sorts indexed recording parts to preserve row references.

Reproduce without credentials, using a new empty catalog directory:

```bash
cargo run --locked --example book_catalog -- write /tmp/tbank-book-example
cargo run --locked --example book_catalog -- read /tmp/tbank-book-example
cargo test --locked --test book_catalog
```

The regression starts a separate reader process, verifies all six exact payloads and their full
DataTypes, then replays through a fresh DataEngine/MessageBus and verifies 3+3 deliveries with no
cross-source messages. Both sources intentionally share their timestamp ranges.

## Quotas, diagnostics and session data

All generated clients use the same quota-aware transport, including provider refresh and stream
reconnect. Identical credentials share broker GetUserTariff quota groups across clients. Minute
and second windows are reserved together, with bounded admission and reserved urgent capacity
for cancel/state recovery. Waiting does not hold an execution mutex. Stream capacity is held until
the response body is released; quota discovery accounts for already-open streams conservatively.
A stream without a consumer deadline is not assigned an artificial lifetime.

For different credentials in the same broker limit domain, set the same
`Arc<TbankRateLimiter>` in both clients' `quota_scope` configuration. Construct it from authoritative
GetUserTariff data (required for streaming scopes), or explicit applicable `TbankRpcQuota` policies
for unary-only clients. This process-local limiter does
not coordinate with other processes or applications. Unknown quotas fail before dispatch;
discovery does not silently disable limits. Tariff policy is a startup snapshot; recreate the scope
when broker limits change. The read-only `tbank_tariff` example prints limits without account IDs.

Subscribe to `grpc::limiter::subscribe_tbank_rpc_diagnostics()` before sending commands. Events
have a shared process sequence, per-RPC correlation ID, UTC timestamp and monotonic elapsed
nanoseconds. Execution markers also identify command receipt and broker state receipt. Broker
created_at is stored separately from local observation times. Handle `RecvError::Lagged(count)`
as diagnostic loss; do not silently continue latency analysis across a gap. RPC headers, complete
body consumption and body abandonment are distinct stages. No tokens or account identifiers
are included.

`TbankDataClient::trading_schedules` and `get_trading_status` expose the pinned typed broker
responses, including optional session fields and trading flags. Instrument capability flags remain
available through instrument metadata. The adapter does not choose sessions, wait for opening,
switch book source by time, delay submission until a status, or cancel on a session boundary.
