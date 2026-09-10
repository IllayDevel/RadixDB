# RadixDB Rust client examples

These examples use the public `radixdb-client` crate and connect to a running
RadixDB TCP server.

Build from the repository root:

```bash
cargo check --manifest-path examples/public/rust-client/Cargo.toml
```

Run against an existing local server:

```bash
cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin basic -- \
  127.0.0.1:15441 radixtrade_client_demo

cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin parameters -- \
  127.0.0.1:15441 radixtrade_client_demo

cargo run --manifest-path examples/public/rust-client/Cargo.toml --bin import_radixtrade -- \
  127.0.0.1:15441 radixtrade_tcp_import_demo examples/public/radixtrade
```

`import_radixtrade` is the TCP client/server version of the tutorial import
flow. It imports `examples/public/radixtrade/schema.sql` and `seed-small.sql`
through the binary protocol, then verifies table counts and index metadata.

Or start a temporary local server and run all examples:

```bash
examples/public/rust-client/scripts/smoke.sh
```

The examples read optional credentials from:

- `RADIXDB_LOGIN` — default `root`;
- `RADIXDB_PASSWORD` — omitted when empty or unset.

Current authentication is a protocol placeholder: the server requires an
authenticate message but does not yet validate credentials. Keep test servers on
loopback or inside a trusted local boundary.
