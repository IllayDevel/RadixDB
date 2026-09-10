---
title: Rust ORM
description: Use dynamic or generated records without hiding SQL, transactions or references.
---

`radixdb-orm` defines transport-neutral schema descriptors, typed values,
records, query builders and a versioned intermediate representation. Execution
is supplied by either the embedded API or `radixdb-client`; choosing ORM does
not choose a deployment mode or create another connection.

## Builders and execution

Every builder produces an `IrDocument`. `OrmBuilder::to_json()` emits the
versioned `radixdb.orm.v1` representation, and `to_sql()` returns SQL plus typed
parameters. Values are parameters rather than interpolated text, and there is
no raw-SQL expression node inside the IR.

```rust
use radixdb_orm::{table, Expr, OrmBuilder, QueryBuilder};

let query = QueryBuilder::from_relation(table("tasks"))
    .select([Expr::column("id"), Expr::column("title")])
    .filter(Expr::column("done").eq(false));
let compiled = query.to_sql()?;
println!("{} {:?}", compiled.sql, compiled.parameters);
```

`query.fetch(&mut connection)` executes on the borrowed TCP connection.
Embedded applications pass `&db` or `&mut transaction`. Builders do not own a
session and cannot silently escape the caller's transaction.

## Dynamic entities and records

`connection.entity("tasks")` reads the live `radixdb.schema.v1` table
descriptor and constructs a `DynamicEntity`. Its columns are checked against
that descriptor while a builder is created.

```rust
use radixdb_orm::{DynamicRecord, TypedValue};

let tasks = connection.entity("tasks")?;
let mut task = DynamicRecord::new(tasks.descriptor().clone());
task.set("id", TypedValue::Integer(10))?;
task.set("title", TypedValue::Text("write documentation".to_string()))?;
task.insert(&mut connection)?;

task.set("done", TypedValue::Boolean(true))?;
task.update(&mut connection)?;
assert!(!task.is_dirty());
```

`insert()`, `save()` and `update()` use `RETURNING *` and hydrate the record only
after a successful complete result. A failed mutation retains dirty fields.
`save()` is a primary-key upsert; it is not a graph save. `delete()` reports a
missing record instead of silently succeeding. `set_null()` writes a typed
NULL, while `unset()` omits that field from the next mutation.

## References

`Reference<T>` in generated code and `DynamicReference` at runtime contain only
a validated target key. They do not hold or lazily load a target row. Dynamic
references require a one-column primary key or `UNIQUE NOT NULL` key and check
that the source column has the matching foreign key:

```rust
let owners = connection.entity("owners")?;
let tasks = connection.entity("tasks")?;
let owner = owners.reference("id", TypedValue::Integer(1))?;

let mut task = DynamicRecord::new(tasks.descriptor().clone());
task.set("id", TypedValue::Integer(10))?;
task.set_reference("owner_id", &owner)?;
```

Navigable SQL paths are read-only query expressions. A reference assignment
still writes the source foreign-key field explicitly.

## Generated models

Generated code starts from an explicit live descriptor export:

```sql
DESCRIBE DATABASE FORMAT JSON
```

Run deterministic code generation offline and commit both the reviewed schema
descriptor and generated source with the application:

```bash
cargo run --locked -p radixdb-orm --bin radixdb-orm-codegen -- \
  schema.json src/generated_schema.rs
```

Generation is not performed by a procedural macro or `build.rs`, and it never
connects to a database. Generated entities expose typed columns, typed records,
key/reference constructors and CRUD methods. They include a schema fingerprint;
a mismatching live descriptor fails closed with `SchemaChanged`. Export,
regenerate and review the diff after an accepted schema migration.

## One SQL and ORM transaction

Raw SQL and ORM use the same caller-owned session. Start and finish a TCP
transaction through dedicated methods:

```rust
connection.begin()?;
connection.execute(
    "INSERT INTO tasks (id, title) VALUES (11, 'raw row')",
)?;
let rows = tasks
    .query()
    .select([tasks.column("id")?.expr()])
    .fetch(&mut connection)?;
connection.rollback()?;
```

The ORM query observes the raw uncommitted write because both borrow the same
connection. Use `db.begin()` and pass `&mut transaction` for the embedded form.
After an outer rollback, discard or reload in-memory records that were hydrated
inside that transaction: record objects do not rewind their local fields.

## Automation boundaries

The 1.2 ORM deliberately does not provide an identity map, lazy loading,
automatic relationship fetch, cascade save/delete, reverse collections,
automatic schema diff or migration. References are one-column keys. The
application owns transaction boundaries, batching, retries and reconciliation
after an uncertain TCP outcome.

The complete `doc/examples/clients/orm.rs` program verifies live descriptor
binding, a validated reference, dynamic CRUD and a raw-SQL/ORM rollback on one
connection. The larger `examples/public/rust-orm` application demonstrates the
offline generated-model workflow. Its `orm_quickstart.rs --transaction-smoke`
path executes raw write -> ORM read -> rollback, commit and error-state recovery
on one connection through the dedicated transaction methods.

Return to the [client interface overview](../overview/) to compare deployment
modes.
