#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

manifest=proto/contracts.sha256
listed=$(mktemp)
vendored=$(mktemp)
trap 'rm -f "$listed" "$vendored"' EXIT

awk '{ print $2 }' "$manifest" | sort >"$listed"
find proto -type f -name '*.proto' | sort >"$vendored"
if ! diff -u "$listed" "$vendored"; then
    echo "proto/contracts.sha256 must list every vendored .proto file" >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check "$manifest"
elif command -v shasum >/dev/null 2>&1; then
    shasum --algorithm 256 --check "$manifest"
else
    echo "sha256sum or shasum is required to verify vendored contracts" >&2
    exit 1
fi
