#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

cargo test --locked -p radixdb-plugin-abi --test malformed_subprocess
cargo test --locked -p radixdb-plugin-abi --test storage_isolation
cargo test --locked -p radixdb-plugin --test proof_plugin
cargo test --locked -p radixdb-plugin --test ui
cargo test --locked -p radixdb-plugin-host --test loader
cargo test --locked -p radixdb-executor --test extension_binding
cargo test --locked --manifest-path crates/radixdb-spatial/Cargo.toml \
    --test database_lifecycle

"$root/scripts/check-plugin-sdk-proof.sh"
"$root/scripts/check-plugin-package-tooling.sh"
"$root/scripts/check-plugin-toolchain-matrix.sh"

printf 'RadixDB v1.2 plugin security, failure and compatibility gate passed\n'
