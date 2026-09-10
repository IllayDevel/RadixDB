# RadixTrade public SQL examples

RadixTrade Group is the shared tutorial domain for the public RadixDB
documentation. It is a fictional trading company with branches, warehouses,
suppliers, customers, products, orders, payments, shipments and audit events.

The first step is always to import the schema and seed data. Query examples are
written against that imported database.

## Run

From the repository root:

```bash
examples/public/radixtrade/scripts/01-import-schema.sh
examples/public/radixtrade/scripts/02-run-query-tour.sh
examples/public/radixtrade/scripts/03-partial-index-reject-probe.sh
```

The scripts prefer `/opt/radixdb/bin/radixdb-cli` when it exists. If the
installed CLI is not available, they fall back to:

```bash
cargo run -q --bin radixdb-cli --features cli --
```

The default database is a local file database under
`examples/public/radixtrade/runtime/`, which is ignored by Git.

Override it when needed:

```bash
RADIXTRADE_DB_DSN='file:///tmp/radixtrade-demo?sync_mode=none' \
  examples/public/radixtrade/scripts/01-import-schema.sh
```

## Files

- `schema.sql` — DDL for the tutorial database.
- `seed-small.sql` — deterministic small dataset.
- `queries/` — the SQL tour: select, filters, joins, aggregates, DML,
  indexes, UUIDs, transactions and optimistic updates.
- `server/server.toml` — minimal documented server configuration sample.
- `maintenance/` — runnable maintenance smoke; maintenance goes after
  schema/query basics in the learning path.

Rust TCP client examples live next to this SQL tutorial:

- `examples/public/rust-client/basic.rs`;
- `examples/public/rust-client/parameters.rs`.
