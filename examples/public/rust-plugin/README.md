# RadixDB Rust plugin example

This standalone `cdylib` is the smallest public example of a RadixDB 1.2
native plugin. It defines one fixed external type and one native scalar
function through the safe `radixdb-plugin` SDK.

Run the local authoring checks from the repository root:

```sh
cargo run --locked -p cargo-radixdb-plugin -- check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
cargo run --locked -p cargo-radixdb-plugin -- test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

Creating a distributable package additionally requires the official
`rust:1.97.0-bookworm` build environment. See the native extension and plugin
tooling chapters in the RadixDB manual before installing trusted code in a
server process.
