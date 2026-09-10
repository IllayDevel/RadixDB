#!/usr/bin/env bash
set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "$example_dir/../../.." && pwd)"
manifest="$example_dir/Cargo.toml"
descriptor="$example_dir/schema/radixtrade.schema.json"
generated="$example_dir/generated_schema.rs"

cargo run --locked --offline --manifest-path "$manifest" --bin orm_quickstart -- --transaction-smoke
cargo run --locked --offline --manifest-path "$manifest" --bin bootstrap
cargo run --locked --offline --manifest-path "$manifest" --bin export_schema
cargo run --locked --offline --manifest-path "$repository_root/Cargo.toml" \
  -p radixdb-orm --bin radixdb-orm-codegen -- \
  "$descriptor" "$generated"
cargo run --locked --offline --manifest-path "$manifest" \
  --features generated-app --bin trading_company
