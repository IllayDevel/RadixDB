# RadixDB Rust ORM: complete RadixTrade application

[Русская версия](README.ru.md)

This standalone crate demonstrates the complete generated-ORM lifecycle on the
fictional RadixTrade trading-company database:

1. create and seed a real database through the ordinary TCP connection;
2. export the versioned `DESCRIBE DATABASE FORMAT JSON` descriptor;
3. run deterministic offline Rust codegen;
4. compile and execute generated records and references;
5. mix typed CRUD, navigation, aggregation and raw SQL in one transaction and
   on one connection.

The example reuses the public RadixTrade schema in
[`../radixtrade`](../radixtrade/README.md). It therefore covers customers,
branches, product dictionaries, sales orders and order lines instead of a
synthetic one-table fixture.

## Prerequisite

Start a server built from the same RadixDB checkout as the client. The example
contains an isolated config and does not touch `/opt/radixdb`:

```bash
cargo build --locked --bin radixdb-server
target/debug/radixdb-server --config examples/public/rust-orm/server.toml
```

Defaults used by the example:

| Setting | Default | Override |
|---|---|---|
| Server | `127.0.0.1:16441` | `RADIXDB_ADDRESS` |
| Database | `radixtrade_orm_demo` | `RADIXDB_DATABASE` |
| Login | `root` | `RADIXDB_LOGIN` |
| Password | absent | `RADIXDB_PASSWORD` |

`select_database()` creates the isolated tutorial database when it does not
exist. `bootstrap` recreates only the tables inside that selected database.

## One-command run

From any directory:

```bash
examples/public/rust-orm/run-complete-demo.sh
```

With an authenticated server:

```bash
RADIXDB_ADDRESS=127.0.0.1:16441 \
RADIXDB_DATABASE=radixtrade_orm_demo \
RADIXDB_LOGIN=root \
RADIXDB_PASSWORD=secret \
  examples/public/rust-orm/run-complete-demo.sh
```

## The four explicit stages

The script intentionally performs four visible commands. They can also be run
manually from the repository root.

### 1. Bootstrap the trading schema and seed

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin bootstrap
```

This executes [`../radixtrade/schema.sql`](../radixtrade/schema.sql) followed by
[`../radixtrade/seed-small.sql`](../radixtrade/seed-small.sql).

### 2. Export the live schema descriptor

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin export_schema
```

Output:

```text
examples/public/rust-orm/schema/radixtrade.schema.json
```

The export is explicit. Codegen never connects to the server.

### 3. Generate the typed Rust facade offline

```bash
cargo run --locked -p radixdb-orm --bin radixdb-orm-codegen -- \
  examples/public/rust-orm/schema/radixtrade.schema.json \
  examples/public/rust-orm/generated_schema.rs
```

The generated source contains records such as `RtCustomersRecord`, entities
such as `RtCustomers`, typed columns, `Reference<T>` constructors and the
schema fingerprint. Both the descriptor and generated source are ignored in
this tutorial because catalog identities belong to the local demo database.
In a real application they should be reviewed and committed to VCS.

### 4. Run the generated application

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --features generated-app \
  --bin trading_company
```

[`trading_company.rs`](trading_company.rs) demonstrates:

- generated `new()`, `insert()`, `get()` and `save()`;
- `Reference<RtCustomers>`, `Reference<RtBranches>` and product references;
- transitive navigation
  `order_line -> sales_order -> customer -> name`;
- navigation mixed with aggregation;
- ORM IR/JSON and rendered SQL inspection;
- a direct SQL statement inside the same transaction and connection.

The application uses deterministic IDs and removes its prior demo rows first,
so the fourth stage can be repeated without recreating the schema.

## Schema changes

Generated CRUD is fingerprint-bound. After an intentional `ALTER TABLE`, rerun
stages 2 and 3 and review the descriptor/generated-source diff. Old generated
code fails closed with `SchemaChanged`; it is never silently rebound or
regenerated during the build.

## Small builder-only example

The original no-server IR/SQL demonstration remains available:

```bash
cargo run --locked \
  --manifest-path examples/public/rust-orm/Cargo.toml \
  --bin orm_quickstart
```

See also the [English ORM guide](../../../doc/src/content/docs/en/clients/orm.md) and the
[Russian ORM guide](../../../doc/src/content/docs/ru/clients/orm.md).
