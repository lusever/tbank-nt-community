#!/usr/bin/env bash
set -euo pipefail

RUSTDOCFLAGS="${RUSTDOCFLAGS:-} -D warnings" cargo doc --locked --all-features --no-deps
