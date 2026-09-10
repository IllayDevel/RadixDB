---
title: Architecture overview
description: Crate ownership, request path and state lifetimes in RadixDB 1.2.
---

This chapter is a map of the implemented RadixDB 1.2 architecture. It explains
where a request and its state live; it does not define a second Rust, storage or
wire contract.

## Contract authority

| Subject | Canonical authority |
| --- | --- |
| Supported application API | Top-level exports of the `radixdb` and `radixdb-client` packages |
| Crate ownership | Workspace manifests, crate public modules and executable boundary tests |
| Physical format and recovery | `radixdb-storage` codecs, format constants and recovery tests |
| Wire format | `crates/radixdb-protocol/src/lib.rs` |
| Native extension ABI and package admission | `radixdb-plugin-abi`, `radixdb-plugin-host` and public `radixdb-plugin` SDK |
| SQL behavior | Parser, executor and the versioned SQL coverage matrix |

Internal modules may change without source compatibility. Applications should
use the interfaces described in [Client interfaces](../../clients/overview/).

## Crate graph

Implementation dependencies point from composition layers to narrower owners:

```text
radixdb-core ---> radixdb-sql ---------+
      |                                |
      +--------> radixdb-functions ----+--> radixdb-executor --+
      |                                |                       |
      +--------> radixdb-storage ------+                       v
                                                    radixdb-api --> radixdb
radixdb-orm ---------------------------------------------^
radixdb-protocol ---> radixdb-client
        |
        +---------------------------------------> server runtime
```

`radixdb-core` owns neutral values, schemas and errors. `radixdb-sql` owns the
lexer, AST and parser. `radixdb-storage` owns MVCC, WAL, indexes and physical
generations without depending on SQL. `radixdb-executor` binds and plans SQL,
authorizes statements and runs relational operators. `radixdb-api` composes
those owners behind embedded handles. The root `radixdb` package is the public
facade and also contains the server process runtime.

`radixdb-protocol` is deliberately neutral: both the server and
`radixdb-client` use the same message and codec types. The client does not link
the engine. Historical root module aliases preserve source compatibility, but
they are not independent implementation owners or extension APIs.

## Crate ownership

| Crate | Responsibility |
| --- | --- |
| `radixdb` | Public embedded facade, CLI and server process composition |
| `radixdb-core` | Neutral values, schemas and errors |
| `radixdb-sql` | SQL lexer, AST and parser |
| `radixdb-functions` | Built-in scalar, aggregate and semantic functions |
| `radixdb-storage` | MVCC, WAL, indexes, physical generations and recovery |
| `radixdb-executor` | Authorization, binding, planning and relational execution |
| `radixdb-api` | Embedded database and connection handles |
| `radixdb-protocol` | Versioned wire messages and codecs |
| `radixdb-client` | Synchronous and asynchronous TCP client |
| `radixdb-orm` | Language-neutral ORM IR, schema descriptors and SQL rendering |
| `radixdb-procedural` | Bounded procedural bytecode and runtime contracts |
| `radixdb-plugin-abi` | Stable native extension ABI types and constants |
| `radixdb-plugin-host` | Native package admission, loading and invocation |
| `radixdb-plugin` | Safe extension SDK |
| `radixdb-plugin-macros` | Extension descriptor and callback generation |
| `cargo-radixdb-plugin` | Extension project, inspection and packaging commands |

Only the top-level `radixdb`, `radixdb-client`, `radixdb-orm` and documented
extension SDK interfaces are application surfaces. Implementation crates are
workspace owners, not separate compatibility promises.

## Native extension path

Native packages enter through a separate, fail-closed composition path:

```text
plugin cdylib -> C ABI 1.0 descriptor -> startup package admission
                                         |
                                         v
immutable process registry -> exact catalog binding -> executor callback
             |                        |                 |
             |                        |                 +-> typed scalar/batch
             |                        +-> type/function/operator identities
             +-> no storage, WAL, catalog or ACL handles
```

`radixdb-plugin-abi` owns numeric values and C layouts. `radixdb-plugin-host`
owns manifest, platform, checksum, ownership, descriptor and callback
admission. The safe `radixdb-plugin` SDK and `radixdb-plugin-macros` generate
that ABI from bounded Rust declarations; `cargo-radixdb-plugin` owns the
repeatable authoring and packaging workflow.

The server constructs one immutable registry before listener bind and pins
loaded libraries until process exit. Catalog 6.2 stores exact package,
fingerprint, object and codec identities. The executor resolves native
functions, operators, operator classes and planner support from that registry,
but authorization and storage access remain core-owned. There is no plugin
storage engine, catalog mutation callback, hot unload or hidden network install.
See [Developing native extensions](../../programming/native-extensions/).

## Request path

Embedded and TCP requests converge before SQL execution:

```text
embedded Database API ------------------------------+
                                                    v
TCP frame -> session state -> selected Database -> Executor
                                                    |
                  parse/cache -> authorize -> bind/plan -> operators
                                                    |
                         MVCC transaction -> WAL -> visibility
                                                    |
                  QueryResult -> cursor/batches -> caller or TCP frame
```

For ordinary SQL, the executor first tries an eligible cached or
borrowed-parameter fast path. Otherwise it parses a program, admits one or more
statements, checks authorization, binds navigable references, establishes the
statement visibility boundary and routes DDL, DML, SELECT or utility work to
its concrete owner. SELECT planning chooses storage access and join operators;
the result pipeline applies projection, filtering, aggregation, windows,
ordering, set operations and paging as required by the query.

Mutations use the same executor boundary but enter an MVCC transaction. The
storage engine owns WAL durability and the commit visibility point. A
statement-level savepoint prevents a failed statement inside an explicit
transaction from leaking a partial effect.

## State ownership

| Lifetime | State owner | Shared with |
| --- | --- | --- |
| Process | Server config, immutable plugin registry, listener, connection admission, global frame budget and cancellation registry | All server sessions |
| Database root | `DatabaseOwner`: one `MVCCEngine`, writer lock, semantic cache and feedback cache per canonical DSN | Connections to the same DSN |
| Connection | One `DatabaseInner`, `Executor`, parsed-plan cache and hidden SQL transaction | Clones receive a new connection-local owner |
| Statement | `ExecutionContext`, parameters, cancellation, statement scope, fences and savepoint | Nested work in that statement only |
| Cursor | Result iterator and pending row/column batch | One active cursor in a TCP session |

The global embedded registry serializes the first open of a canonical DSN and
shares the resulting durable engine. It does not share an executor or SQL
transaction. This distinction is why `Database::clone()` can observe the same
committed data without inheriting another handle's active transaction.

The server has a second name-based registry below its configured data root. It
tracks opening, ready and retryable failed databases and leases a connection
handle from the embedded owner. Recovery of different database names may run
independently; one name has a single opening owner.

## Concurrency boundaries

The storage engine publishes lifecycle state before admitting transactions.
Catalog DDL, statement visibility, sealing, snapshots and maintenance use
separate fences with an explicit acquisition order. Readers hold immutable
catalog and physical-generation snapshots; publishers construct a successor
before swapping the shared generation.

These fences are implementation coordination, not a public locking API.
Application guarantees are described under [Transactions](../../sql/transactions/),
while physical publication and recovery are described in
[Storage internals](../storage/).
