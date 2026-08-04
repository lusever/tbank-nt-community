# Contributing

This repository contains only the T-Bank venue adapter. Trading strategies, research, portfolio
selection, position sizing, and live-runner orchestration belong in consumer repositories.

## Development

- Use the Rust toolchain pinned in `rust-toolchain.toml`.
- Keep every direct NautilusTrader dependency on the same upstream tag or revision.
- Keep vendored protobufs at the exact revision recorded in `proto/contracts.lock`.
- Do not expose tokens, account identifiers, broker request IDs, venue order IDs, or tracking
  metadata in logs, errors, fixtures, or CI output.
- Update all producers, consumers, fixtures, tests, and documented versions together when changing
  persisted order lifecycle or projection contracts.

Before opening a pull request, run:

```bash
bash scripts/check-proto-contracts.sh
cargo fmt --check
cargo check --locked --all-targets --no-default-features
cargo test --locked
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo deny check
bash scripts/check-public-method-docs.sh
```

Sandbox tests are ignored and feature-gated because they can submit orders. Run the complete suite
only against a dedicated sandbox account and always serially:

```bash
cargo test --locked --features sandbox-tests --test sandbox_integration -- --ignored --test-threads=1
```

Missing credentials or account configuration is a failure. Capability-based skips are allowed only
for sandbox behavior that T-Bank reports as unsupported or unavailable, and must be reported.

## Release checklist

1. Update `CHANGELOG.md` and the compatibility table in `README.md`.
2. Verify the NautilusTrader tag, `proto/contracts.lock` revision, and contract checksums.
3. Run offline validation and the explicit sandbox acceptance suite.
4. Create an immutable signed Git tag for the release. Releases are Git-only; do not publish the
   crate to crates.io.
