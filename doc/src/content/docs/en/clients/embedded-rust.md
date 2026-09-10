---
title: Embedded Rust
description: Open RadixDB in a Rust process, bind parameters, read rows and control transactions.
---

The embedded interface runs the database engine in the application process.
It does not require `radixdb-server`, but the process must own the database
directory and its lifecycle. This chapter describes the 1.2 public API exposed
by the top-level `radixdb` crate and its `radixdb::api` module.

## Adding the crate

When building from a source checkout, depend on the workspace root. Replace the
example path with the checked-out 1.2 source used by the application:

```toml
[dependencies]
radixdb = { path = "/path/to/RadixDB" }
```

The verification project for this manual pins that path to commit
`23bf35df011aae6816d77578be96074b02bc363c` and builds offline with a lockfile.

## Opening a database

`Database::open_in_memory()` creates a distinct temporary engine for a process.
`Database::open("memory://name")` uses the canonical registry and shares an
engine with another open of the same DSN. A persistent database uses a file DSN:

```rust
use radixdb::api::Database;

let db = Database::open(
    "file:///srv/radixdb/app?sync_mode=full&checkpoint_on_close=off",
)?;
```

Opening the same canonical file DSN again in one process returns another handle
to the same engine. An incompatible configuration for an already-open DSN is
rejected. Do not let separate processes open the same database directory.

## Commands and parameters

`execute()` returns the number of affected rows. Positional placeholders are
`$1`, `$2`, and so on. A tuple or `params!` supplies positional values;
`execute_named()` and `named_params!` supply `:name` values:

```rust
use radixdb::{named_params, params};

db.execute(
    "INSERT INTO notes (id, title) VALUES ($1, $2)",
    params![1_i64, "first"],
)?;
db.execute_named(
    "INSERT INTO notes (id, title, done) VALUES (:id, :title, :done)",
    named_params! { id: 2_i64, title: "second", done: true },
)?;
```

Supported bindings include integer and floating-point values, Boolean, strings,
bytes, date/time values, UUID, JSON, decimal values, vectors and `Option<T>`.
Binding is separate from SQL text. Do not construct values through string
interpolation.

`prepare()` parses a reusable statement. A `Statement` can execute or query
with a new parameter set each time. It retains an owner reference, so release
statements and rows before an explicit database close.

## Reading results

`query()` returns `Rows`. Its iterator yields `Result<ResultRow>` because an
error can occur after the query has started:

```rust
let rows = db.query(
    "SELECT id, title FROM notes WHERE id >= $1 ORDER BY id",
    (1_i64,),
)?;
for row in rows {
    let row = row?;
    let id: i64 = row.get(0)?;
    let title: String = row.get_by_name("title")?;
    println!("{id}: {title}");
}
```

Column positions are zero-based. `get_by_name()` is case-insensitive and
rejects an ambiguous duplicate name; qualify or alias duplicate projections.
Use `query_one::<T, _>()` when exactly one scalar value is required,
`query_opt::<T, _>()` for zero or one, and `query_as::<T, _>()` with a
`FromRow` implementation for application records.

The lower-allocation `Rows::advance()` interface returns `false` both at EOF
and after a deferred execution or close error. Call `Rows::error()` after
`false`. Iterator users receive that error as an item. `Rows::close()` closes
early; dropping the cursor also closes it.

## Transactions

Use the transaction handle rather than SQL text for transaction control:

```rust
let mut transaction = db.begin()?;
transaction.execute(
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
    (100_i64, 7_i64),
)?;
transaction.rollback()?;
```

`begin()` uses read committed isolation. `begin_with_isolation()` also exposes
snapshot isolation. The handle provides `commit()`, `rollback()`, `savepoint()`,
`rollback_to_savepoint()` and `release_savepoint()`. Dropping an uncommitted
transaction rolls it back.

## Errors and closing

All calls return `radixdb::Result`. Constraint, type, parse, storage and
transaction failures must be handled by the application; an error is not an
empty result. Drop every `Rows`, `Statement` and transaction owner before
calling `Database::close()`:

```rust
db.close()?;
```

Explicit close performs the engine close and releases the file lock. It fails
while another database handle, transaction or retained owner still exists.
Normal `Drop` closes the engine after the last handle disappears, but explicit
close is useful when the process must prove the directory is released.

The complete executable example is
`doc/examples/clients/embedded.rs`. The documentation gate creates a
persistent temporary database, checks a UNIQUE error, verifies rollback and
then performs an explicit close.

Continue with the [Rust TCP client](../rust-client/) when the engine must run
in a separate server process.
