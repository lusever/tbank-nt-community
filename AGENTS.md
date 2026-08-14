# Repository instructions

- Keep this repository limited to the T-Bank venue adapter. Trading strategies, research,
  portfolio selection, position sizing, and live-runner orchestration belong in consumers.
- Keep every direct Nautilus dependency on the same upstream tag or revision.
- Before changing the adapter, inspect the pinned Nautilus source and comparable official adapters,
  then follow their established contracts and patterns.
- After any adapter, protobuf, transport, factory, execution, market-data, config, or sandbox-test
  change, run the complete validation sequence before declaring the work finished. Start with the
  offline/default suite:

```bash
cargo test --locked
cargo test --locked --all-features
```

- Sandbox credentials belong only in the repository-local, git-ignored `.env`. Never print,
  inspect, or copy its contents into commands, logs, or chat. Sandbox tests do not load `.env`
  themselves, so export it into the test process before running them.
- Then run the full mutating sandbox acceptance suite. This is required by default for the adapter
  change scopes above, not an optional follow-up. The command below is standing authorization to
  submit, fill, and cancel sandbox orders without additional confirmation. It applies only to
  sandbox, never live, and the suite must run serially:

```bash
set -a
source .env
set +a
cargo test --locked --features sandbox-tests --test sandbox_integration -- \
  --ignored --test-threads=1 --nocapture
```

- If sandbox acceptance fails before an authoritative order result, do not blindly rerun mutating
  cases: report the exact failed request/stage first and assess duplicate-order or residual-position
  risk. A failed or skipped sandbox run does not count as completed validation.

- Missing sandbox credentials or account configuration must fail affected tests. Capability-based
  skips are allowed only for unsupported or unavailable sandbox behavior and must be reported.
- Do not expose tokens, authorization metadata, account identifiers, venue order or position
  identifiers, broker request/idempotency IDs, or other private broker tracking metadata. Public
  instrument identifiers (`ticker`, `class_code`, `FIGI`, and `instrument_uid`) are not secrets or
  private tracking metadata and may be included in human-readable diagnostics when useful.
- Do not change order lifecycle, broker request-id, or persisted projection contracts without
  updating every producer, consumer, fixture, test, and documented version in the same change.
- On execution mutex poisoning, fail the runtime; never recover corrupted projections. Commit
  projection updates only after all fallible work succeeds.
- Vendored protobuf contracts must keep their exact upstream revision in `proto/contracts.lock`.
