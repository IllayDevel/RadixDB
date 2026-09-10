# radixdb-client

`radixdb-client` is the standalone blocking TCP client for the RadixDB binary
protocol.

It intentionally depends only on the wire protocol implementation. It does not
link the database engine, storage layer, SQL parser, executor or server crate
internals into your application.

## Dependency

Until the crate is published separately, use a workspace/path dependency or a
Git dependency pinned to the RadixDB revision you deploy:

```toml
[dependencies]
radixdb-client = { path = "path/to/RadixDB/crates/radixdb-client" }
```

When the crate is published, replace this with the published version.

## Connection lifecycle

The normal lifecycle is:

1. `Connection::connect("host:port")`;
2. `authenticate(login, password)`;
3. `select_database(database)`;
4. `execute(...)` or `execute_with_parameters(...)`;
5. `fetch(...)` until cursor `eof`, or `close_cursor(...)` / `cancel(...)`;
6. optional `shutdown()`.

Minimal example:

```rust
use radixdb_client::{Connection, ExecuteResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Connection::connect("127.0.0.1:15441")?;
    db.authenticate("root", None)?;
    db.select_database("radixtrade_client_demo")?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            name TEXT NOT NULL
        )",
    )?;

    let inserted = db.execute("INSERT INTO users (name) VALUES ('Alice')")?;
    if let ExecuteResult::CommandComplete {
        affected_rows,
        last_insert_id,
    } = inserted
    {
        println!("affected rows: {affected_rows}");
        println!("last insert id: {last_insert_id}");
    }

    if let ExecuteResult::Cursor(cursor) = db.execute("SELECT id, name FROM users")? {
        let batch = db.fetch(&cursor)?;
        println!("{:?}", batch.rows);
    }

    db.shutdown()?;
    Ok(())
}
```

More complete runnable examples live in:

- `examples/public/rust-client/basic.rs`;
- `examples/public/rust-client/parameters.rs`.

## Authentication

The server requires an authentication protocol message.

Current public contract:

- `authenticate("root", None)` is accepted only when the server `bind_ip` is a
  loopback address;
- non-root users are rejected;
- password authentication is rejected because no password/token backend is
  configured yet;
- do not expose the server on a public interface until a real auth backend is
  added.

Recommended application convention:

```rust
connection.authenticate("root", None)?;
```

## Database selection

The current external protocol does not expose a separate `CREATE DATABASE`
command. `select_database(name)` opens or creates:

```text
<server data_dir>/databases/<name>
```

and selects it for the current connection.

You cannot select another database while a cursor, BLOB transfer, bulk insert or
explicit transaction is active.

## Server and database readiness

`Connection::server_status()` returns the server lifecycle after connect and
authentication, without selecting a database. `Connection::database_status(name)`
returns the same envelope focused on one database.

```rust
let mut db = Connection::connect("127.0.0.1:15441")?;
db.authenticate("root", None)?;

let status = db.database_status("radixtrade_client_demo")?;
if !status.ready {
    eprintln!("database is not ready yet: {}", status.message);
}
```

The status payload exposes:

- `lifecycle`: `Starting`, `Recovering`, `Opening`, `Warming`, `Ready`,
  `Degraded` or `Emergency`;
- `ready`: whether the requested scope is ready for normal work;
- `databases`: per-database lifecycle, readiness message and artifact counters
  for table directories, WAL files, volume files, snapshots, checkpoints and
  manifests.

This endpoint is protocol-level readiness, not an HTTP health endpoint. It is
safe to use before `select_database(name)` and is intended for supervisors,
IDEs, smoke tests and application startup gates.

## Query results and cursors

`execute(...)` returns:

```rust
ExecuteResult::CommandComplete {
    affected_rows,
    last_insert_id,
}
```

for statements such as `CREATE`, `INSERT`, `UPDATE` and `DELETE`, or:

```rust
ExecuteResult::Cursor(cursor)
```

for result-producing queries.

Fetch every cursor until `eof == true`, or close/cancel it before sending the
next command. Otherwise the client returns `ClientError::CommandsOutOfSync`.

## ColumnBatchV1

`Connection::fetch_column_batch(&cursor)` is an optional cursor fetch mode for
eligible immutable artifact scans. It requires negotiated
`ProtocolCapability::ColumnBatchV1`; otherwise the client returns
`ClientError::CapabilityUnavailable`.

The method returns either:

- `ColumnCursorBatch::Columnar { columns, row_count, eof }` — typed
  column-major data;
- `ColumnCursorBatch::Rows(batch)` — semantic row fallback for query shapes or
  value types that do not have a typed wire layout yet.

`Rows` is not an error. It preserves the same SQL/MVCC semantics as ordinary
`fetch(...)`, so callers must handle both variants.

After a cursor is fetched through `fetch_column_batch(...)`, keep using
`fetch_column_batch(...)` until `eof == true`, or explicitly call
`close_cursor(...)` / `cancel(...)`. Mixing ordinary `fetch(...)` with an
already-started typed cursor returns `ClientError::CommandsOutOfSync`.

Large typed DATA row groups are bounded by the server's `cursor_batch_max_bytes`
and negotiated `max_frame_bytes`. If a group does not fit into one response,
the server splits it into several ordered `Columnar` batches and finishes with
an empty `eof == true` batch. `cursor_batch_max_rows` limits legacy row batches;
it does not force row-count slicing of immutable DATA column groups.

If a connection times out between `FetchColumnBatch` requests, the server closes
that TCP session and drops any retained split-tail. Cursors are not resumable
after reconnect.

Current typed columns include integers, floats, booleans, timestamps,
dictionary text, bytes and JSON text. `VECTOR(N)` projections intentionally use
row fallback until a dedicated vector wire layout exists.

## Generated IDs

`last_insert_id` is the value generated by the last successful insert into a
single-column `INTEGER PRIMARY KEY AUTO_INCREMENT`.

If a statement inserts multiple rows, the value is the last generated id in
statement order. For tables without an auto-increment integer primary key the
value is `0`.

When the application needs generated values as regular result data, use SQL
`RETURNING`, for example:

```sql
INSERT INTO users (name) VALUES ('Alice') RETURNING id;
```

UUID primary keys are represented through `WireValue::Uuid([u8; 16])`.
Exact decimal, date and binary values are represented through their dedicated
wire variants; do not downcast them to strings or floats in metadata-driven
clients.

## Bound parameters

Use `execute_with_parameters(sql, BTreeMap<String, WireValue>)`. SQL references
parameters by name:

```rust
use std::collections::BTreeMap;
use radixdb_client::{Connection, WireValue};

let mut params = BTreeMap::new();
params.insert("email".to_string(), WireValue::String("owner@example.test".to_string()));
params.insert("active".to_string(), WireValue::Bool(true));

db.execute_with_parameters(
    "SELECT id, email FROM users WHERE email = :email AND active = :active",
    params,
)?;
```

Important public wire values:

- `WireValue::Null`;
- `WireValue::Bool(bool)`;
- `WireValue::Int(i64)` and smaller integer variants;
- `WireValue::Float64(f64)`;
- `WireValue::Decimal { unscaled, precision, scale }`;
- `WireValue::String(String)`;
- `WireValue::Date { days_since_unix_epoch }`;
- `WireValue::DateTime { millis_since_unix_epoch_utc }`;
- `WireValue::Uuid([u8; 16])`;
- `WireValue::Bytes(Vec<u8>)`.

`WireValue::Decimal`, `WireValue::Date` and `WireValue::Bytes` are accepted by
`execute_with_parameters()` and returned by cursors without lossy string,
floating-point or base64 fallback. The integration contract is covered by the
public TCP scalar round-trip test in the main RadixDB repository.

`JSON` row values use `WireValue::Json`, preserving the JSON payload without
confusing it with SQL `TEXT`. In `ColumnBatchV1`, binary and JSON columns have typed layouts:
`WireColumn::Bytes` and `WireColumn::JsonText` store one contiguous byte buffer,
per-row `(offset, length)` pairs and a null bitmap.

`VECTOR(N)` row values use `WireValue::Vector`, preserving the declared
dimension and raw `f32` bytes. They are intentionally not exposed as a
`WireColumn` typed batch yet: a `ColumnBatchV1` fetch falls back to
`ColumnCursorBatch::Rows` when a projection includes `VECTOR`.

## Transactions

The client exposes explicit transaction helpers:

```rust
db.begin()?;
db.execute("INSERT INTO events (message) VALUES ('inside tx')")?;
db.savepoint("before_optional_work")?;
db.rollback_to_savepoint("before_optional_work")?;
db.commit()?;
```

Use `begin_with_isolation(TransactionIsolation::Snapshot)` when one statement
sequence must retain a snapshot view. Transaction-control SQL is intentionally
rejected by generic `execute`: the dedicated methods keep client and server
state synchronized.

After a statement-time `UNIQUE`, primary-key, foreign-key or `CHECK` error, and
after a constraint conflict found by `commit()`, inspect
`db.in_transaction()`. Protocol v14 reports the server-side state explicitly:
`true` means the transaction is still active and `rollback()` discards every
write since `begin()`; `false` means a lower-level commit failure already
forced an atomic abort. A failed multi-table commit never publishes only the
tables processed before the error.

## Error handling

Client methods return `Result<_, ClientError>`.

Common variants:

- `ClientError::Io` — TCP/socket failure;
- `ClientError::Protocol` — malformed frame or protocol-level failure;
- `ClientError::Server` — SQL/auth/runtime error reported by RadixDB server;
- `ClientError::CommandsOutOfSync` — another cursor/stream is still active;
- `ClientError::TransactionState` — invalid operation for the current
  transaction state;
- `ClientError::CapabilityUnavailable` — negotiated server capabilities do not
  include the requested optional feature.

For SQL errors, match `ClientError::Server(error)` and inspect
`error.code`/`error.message`.

## Protocol compatibility

The client negotiates the binary protocol version and optional capabilities
during handshake. Use a client crate built from the same RadixDB release or an
explicitly documented compatible release.

RadixDB `0.5.2` uses protocol `14`. It is not wire-compatible with older
releases: upgrade the server and all clients together, and roll back the same
way. Mixed-version peers fail the handshake before authentication or SQL.

Protocol 14 also owns prepared/positional execution, savepoints, explicit
cursor discard, out-of-band request cancellation and database close/status
operations, plus explicit retryable compaction-backpressure errors. A prepared
handle is connection-owned and cannot be reused by a different client
connection.

The current crate exposes optional `ColumnBatchV1` support.

Applications that only use `execute` and row `fetch` do not need to handle
column batches.
