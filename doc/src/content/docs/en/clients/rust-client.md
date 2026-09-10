---
title: Rust TCP Client
description: Connect to RadixDB protocol 17, execute prepared statements and manage typed values safely.
---

`radixdb-client` is the synchronous Rust client for the RadixDB binary protocol.
It does not contain the database engine and it is not a PostgreSQL client. The
1.2 documentation baseline negotiates protocol 17.

## Connect and select a database

Create a connection and authenticate against a database before executing SQL.
The Principal needs database `CONNECT` and the object privileges used by the
application.

```rust
use std::time::Duration;
use radixdb_client::Connection;

let timeout = Duration::from_secs(5);
let mut connection = Connection::connect_with_timeouts(
    "127.0.0.1:15441",
    timeout,
    timeout,
    timeout,
)?;
connection.authenticate_database(
    "application",
    "application_reader",
    "secret",
)?;
```

`connect()` uses operating-system socket defaults. `connect_with_timeouts()`
sets separate connect, read and write limits. The protocol handshake is part of
connection creation and rejects an incompatible server.

Use `TlsConnection::connect_tls()` with a `TlsClientConfig` when the server
endpoint is configured for direct TLS. A configured administrative `root`
account uses `authenticate("root", Some(password))`; if no verifier is
configured, passwordless `root` remains only as a plaintext loopback recovery
login. See [Authentication](../../administration/authentication/).

## Commands and prepared statements

`execute()` returns `ExecuteResult::CommandComplete` for commands and
`ExecuteResult::Cursor` for row-producing statements. Bind parameters rather
than interpolating them into SQL:

```rust
use radixdb_client::{ExecuteResult, WireValue};

let insert = connection.prepare(
    "INSERT INTO notes (id, title) VALUES ($1, $2)",
)?;
let result = connection.execute_prepared(
    &insert,
    vec![WireValue::Int(1), WireValue::String("first".to_string())],
)?;
assert!(matches!(result, ExecuteResult::CommandComplete { .. }));
connection.close_prepared(insert)?;
```

`execute_with_positional_parameters()` accepts a `Vec<WireValue>`.
`execute_with_parameters()` accepts a `BTreeMap<String, WireValue>` for named
parameters, and `execute_with_bindings()` accepts both sets. A prepared handle
belongs to the connection that created it; another connection returns
`PreparedStatementOwnerMismatch`.

## Native extension values

Protocol 17 represents an external value by stable type object ID, nonzero
codec version and bounded canonical bytes. `radixdb-client` re-exports this as
`WireValue::External`:

```rust
let mut payload = Vec::with_capacity(16);
payload.extend_from_slice(&1_i64.to_le_bytes());
payload.extend_from_slice(&2_i64.to_le_bytes());
let pair = WireValue::External {
    type_object_id: [
        0xda, 0xa5, 0xe8, 0x3d, 0x0e, 0xa3, 0x3c, 0x36,
        0xca, 0xde, 0x62, 0x86, 0x2b, 0xae, 0xef, 0x54,
    ],
    codec_version: 1,
    payload,
};
```

Obtain identity, codec revision and payload bounds from `DESCRIBE DATABASE` or
generated plugin metadata. Do not infer them from the SQL type name and do not
substitute `WireValue::Bytes`. The high-level ORM intentionally rejects an
external value unless a plugin-aware adapter handles its canonical codec.

## Cursors

A cursor is a connection-level stream. Fetch it to EOF, close it, or cancel it
before issuing an unrelated command:

```rust
let ExecuteResult::Cursor(cursor) = connection.execute(
    "SELECT id, title FROM notes ORDER BY id",
)? else {
    return Err("SELECT did not open a cursor".into());
};
loop {
    let batch = connection.fetch(&cursor)?;
    for row in batch.rows {
        println!("{:?}", row.values);
    }
    if batch.eof {
        break;
    }
}
```

Sending a new command while a cursor is active returns
`ClientError::CommandsOutOfSync`. `close_cursor(cursor)` and `cancel(cursor)`
consume the cursor handle. `fetch_batch()` can request columnar transport;
protocol 17 may return its documented row fallback when the query is not
eligible. Do not mix fetch modes on one cursor.

## Transactions

Transaction control uses dedicated protocol operations:

```rust
connection.begin()?;
connection.execute(
    "UPDATE accounts SET balance = balance - 100 WHERE id = 7",
)?;
connection.rollback()?;
```

Use `begin_with_isolation()` for an explicit `ReadCommitted` or `Snapshot`
choice. `commit()`, `rollback()`, `savepoint()`, `rollback_to_savepoint()` and
`release_savepoint()` update the client's tracked state. Generic
`execute("BEGIN")`, `execute("COMMIT")` and `execute("ROLLBACK")` are rejected
by the server boundary.

## Errors and uncertain outcomes

`ClientError::Server` is an explicit server reply. `is_retryable()` is true
only when that reply classifies an operation as known not to have published.
It is false for transport failures.

If the connection is lost or a timeout occurs after a write or commit was sent
but before its reply arrived, the outcome is unknown. The server may have
published the operation. Discard the connection, reconnect, and reconcile an
application idempotency key or durable operation record before deciding whether
to retry. Never automatically retry such a write from the transport error alone.

An incomplete transport round trip poisons the connection and changes the
transaction state to unknown. Check `is_poisoned()` for diagnostics and
`is_reusable()` before returning a connection to a pool. A reusable connection
is open, unpoisoned, has no active cursor and has no active or unknown
transaction.

The Tokio `AsyncConnection` follows the same ownership rules. It processes one
command at a time rather than multiplexing hidden work. Dropping a future after
I/O has started poisons the connection because its outcome can no longer be
matched safely.

## Shutdown

Finish or close the active cursor, resolve the transaction and close prepared
statements before pooling or shutdown. Then close the socket explicitly:

```rust
assert!(connection.is_reusable());
connection.shutdown()?;
```

The complete `doc/examples/clients/tcp.rs` example verifies prepared
execution, UNIQUE rejection, `CommandsOutOfSync`, cursor close and rollback
against a matching server build.

Continue with the [ORM chapter](../orm/) for descriptor-driven and generated
models on the same connection, or
[Developing native extensions](../../programming/native-extensions/) for the
external codec contract.
