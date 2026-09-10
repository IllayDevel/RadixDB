#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/crates/radixdb-spatial/Cargo.toml"
tool="$root/target/debug/cargo-radixdb-plugin"
evidence="$root/target/v1.2-evidence/toolchain-matrix"
toolchains=(1.97.0 1.97.1)

mkdir -p "$evidence"
cargo build --locked -p cargo-radixdb-plugin

for toolchain in "${toolchains[@]}"; do
    if ! rustup toolchain list | awk '{print $1}' | grep -Fxq "$toolchain-x86_64-unknown-linux-gnu"; then
        printf 'required supported toolchain is not installed: %s\n' "$toolchain" >&2
        exit 1
    fi
    target="$evidence/target-$toolchain"
    CARGO_TARGET_DIR="$target" cargo "+$toolchain" build --locked --release \
        --manifest-path "$manifest"
    library="$target/release/libradixdb_spatial.so"
    sha256sum "$library" >"$evidence/library-$toolchain.sha256"
    "$tool" inspect --library "$library" >"$evidence/descriptor-$toolchain.json"
done

diff -u "$evidence/descriptor-1.97.0.json" "$evidence/descriptor-1.97.1.json"
printf 'Rust toolchain matrix passed\n'
cat "$evidence"/library-*.sha256
