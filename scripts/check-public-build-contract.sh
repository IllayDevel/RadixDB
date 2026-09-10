#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

case "$(rustc --version)" in
  "rustc 1.97.0 "*) ;;
  *)
    echo "public build requires the pinned Rust 1.97.0 toolchain" >&2
    exit 1
    ;;
esac

if grep -Eq '^wasm[[:space:]]*=|^ffi[[:space:]]*=' Cargo.toml; then
  echo "unsupported FFI/WASM features must not be advertised" >&2
  exit 1
fi

cargo test --locked --test server_identity_test -- --test-threads=1
cargo check --locked --bin radixdb-server --bin radixdb-password \
  --bin radixdb-cli --features cli
cargo check --locked --manifest-path examples/public/rust-client/Cargo.toml --bins
