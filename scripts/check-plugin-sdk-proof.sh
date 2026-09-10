#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$repo_root/crates/radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml"
target="$repo_root/target/plugin-sdk-proof"
library="$target/release/libradixdb_sdk_proof_plugin.so"

CARGO_TARGET_DIR="$target" cargo build --locked --release --manifest-path "$fixture"

mapfile -t exports < <(nm -D --defined-only "$library" | awk '{print $3}' | sort -u)
if [[ "${#exports[@]}" -ne 1 || "${exports[0]}" != "radixdb_plugin_entry_v1" ]]; then
    printf 'unexpected exported symbols in %s:\n' "$library" >&2
    printf '  %s\n' "${exports[@]}" >&2
    exit 1
fi

if rg -n 'unsafe|extern[[:space:]]+"C"|radixdb-plugin-abi' \
    "$repo_root/crates/radixdb-plugin/tests/fixtures/proof-plugin/src" \
    "$repo_root/crates/radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml"; then
    printf 'proof plugin bypasses the safe single-crate authoring surface\n' >&2
    exit 1
fi

printf 'plugin SDK proof passed: one dependency, safe source, one exported entrypoint\n'
