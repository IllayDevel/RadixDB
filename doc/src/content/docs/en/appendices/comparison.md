---
title: Choosing RadixDB
description: An honest capability comparison with SQLite, DuckDB and PostgreSQL for selecting a database architecture.
---

RadixDB, SQLite, DuckDB and PostgreSQL solve overlapping but different
problems. This chapter is a selection guide, not a universal ranking. A `Yes`
in one row does not imply identical semantics, operational maturity or
performance.

The RadixDB column describes version 1.2.25. The other columns summarize the
official product documentation reviewed on 15 September 2026; follow the
source links at the end because those projects continue to evolve.

## Capability comparison

| Capability | RadixDB 1.2.25 | SQLite | DuckDB | PostgreSQL |
| --- | --- | --- | --- | --- |
| Embedded operation | Yes. The Rust API opens memory or file databases in the application process. | Yes. In-process, self-contained and serverless is the primary model. | Yes. In-process operation is the primary model, with APIs for several languages. | No core in-process mode. Applications connect to a server. |
| Separate network server | Yes. The same engine runs behind a native TCP protocol. | No built-in server. Applications read and write the database file directly. | Not the traditional primary model. The official Quack remote protocol is beta in the reviewed documentation. | Yes. Client/server operation is the primary architecture. |
| Transaction and concurrency model | MVCC row updates with `READ COMMITTED` and `SNAPSHOT`; `SERIALIZABLE` is not supported. | ACID and snapshot behavior, with concurrent readers but serialized writes to a database file. | ACID with bulk-optimized MVCC and optimistic concurrent writes inside one writer process. | Multi-session MVCC with standard isolation levels, including serializable isolation. |
| Physical data organization | Hybrid: mutable MVCC rows plus immutable compressed column blocks in row groups. | Row records in a compact B-tree-oriented database file. | Columnar storage and vectorized execution for analytical work. | Row-oriented heap storage in the core server; extensions can add other approaches. |
| Primary workload emphasis | Mixed transactional updates and analytical scans over long-lived application data. | Local application state, device storage and portable application files. | In-process OLAP, bulk operations and analysis of files, data frames and lakehouse data. | General multi-user server workloads with broad OLTP, analytical and operational facilities. |
| Functions, procedures and triggers | RadixDB PL provides stored functions, procedures, triggers and jobs within bounded runtime contracts. | Triggers and host-defined SQL functions are available; there is no stored-procedure subsystem. | Macros, SQL functions and extension functions are available; it is not a traditional server stored-procedure model. | Stored functions, procedures and triggers through PL/pgSQL and other procedural languages. |
| Native extensions | Yes. Operator-trusted in-process packages use a stable C ABI, Rust SDK and exact allowlist. | Yes. Loadable extensions can add functions, collations, virtual tables and other features. | Yes. Core and community extensions add types, functions, formats and protocols. | Yes. The extension framework can add types, functions, operators, access methods and native code. |
| PostgreSQL wire protocol | No. RadixDB uses its own versioned protocol and client. | No. | No. | Yes. |
| Built-in replication, failover and PITR | No. Version 1.2.25 provides snapshots and restore, but does not claim HA, replication, automatic failover or arbitrary-WAL-position PITR. | No built-in server-level HA subsystem; replication requires an application or third-party layer. | Not a primary built-in database-server HA model. | Yes. Physical and logical replication, standbys, failover building blocks and PITR are established server features. |
| Public ecosystem stage | First packaged public release; Rust-first clients and tooling, with a deliberately narrow ecosystem. | Extremely mature and widely deployed, with bindings and tools across platforms. | Mature analytical ecosystem with broad language and data-tool integration. | Extremely mature server ecosystem with broad drivers, administration tools, hosting and extensions. |

These entries compare product architecture, not SQL syntax line by line. See the
[RadixDB SQL matrix](../compatibility/) for exact accepted and rejected syntax.

## Where RadixDB is especially useful

RadixDB is primarily intended for application systems whose operational data
lives for years, changes continuously and also serves reports, complex queries
and analytics. Its most natural application areas include:

- ERP, CRM, inventory, accounting and manufacturing systems where many related
  entities coexist with daily updates, reports and aggregates;
- geospatial databases, catalogs and reference systems with complex schemas,
  many repeated values and frequent traversal between related records;
- monitoring, telemetry and event history where recent data is processed
  actively while the accumulated body must remain compact and quick to scan;
- local, on-premises and edge systems with constrained resources, where
  compressed storage, low memory use and autonomous operation matter;
- Rust applications that need one engine for local in-process operation and a
  later move to a multi-user server;
- specialized domain systems where functions, triggers and trusted extensions
  keep validated domain logic close to the data.

RadixDB's main advantage appears in mixed workloads: small transactional
changes coexist with regular reads across large data sets. Hybrid storage lets
one database serve both patterns without introducing a separate analytical
store at an early stage, while compression helps keep more of the working set
in memory.

The [benchmarks](../benchmarks/) show strong and slower cases on specific
hardware. They help determine whether a workload fits, but do not replace
testing the real application schema.

## Where another engine is a better fit

RadixDB is not intended to replace every database. The following workloads
have more suitable and mature options:

- for a small local application that stores settings and simple records in one
  portable file without a server or analytics, **SQLite** is simpler and
  smaller;
- for predominantly analytical processing of Parquet, data frames and object
  storage data, with bulk changes and no continuously running multi-user
  application, **DuckDB** is a better fit;
- for an existing PostgreSQL application that depends on its drivers, ORMs,
  extensions and administration tools, moving to RadixDB is not compatible
  without application changes;
- for a critical 24/7 system that already requires built-in replication,
  automatic failover, a high-availability cluster and point-in-time recovery,
  choose **PostgreSQL** or another mature server database;
- for globally distributed writes, horizontal scaling and automatic sharding,
  use a purpose-built distributed engine;
- for projects that require a long public track record, certified support and
  a broad market of ready-made integrations and specialists, the RadixDB
  ecosystem is still too young.

In short, RadixDB fits best as the compact core of an application with related
data and concurrent transactional and analytical work, rather than as a
universal replacement for a mature database server.

## Sources and scope

RadixDB behavior is documented in [What is RadixDB?](../../preface/what-is-radixdb/),
[transactions](../../sql/transactions/), [client interfaces](../../clients/overview/),
[native extensions](../../programming/native-extensions/) and
[recovery troubleshooting](../../administration/troubleshooting/).

External product descriptions use primary documentation:

- SQLite: [About SQLite](https://www.sqlite.org/about.html),
  [serverless architecture](https://www.sqlite.org/serverless.html) and
  [transaction isolation](https://www.sqlite.org/isolation.html).
- DuckDB: [Why DuckDB](https://duckdb.org/why_duckdb) and
  [concurrency](https://duckdb.org/docs/current/connect/concurrency).
- PostgreSQL: [client/server architecture](https://www.postgresql.org/docs/current/tutorial-arch.html),
  [MVCC](https://www.postgresql.org/docs/current/mvcc-intro.html),
  [PL/pgSQL](https://www.postgresql.org/docs/current/plpgsql-overview.html) and
  [extensibility](https://www.postgresql.org/docs/current/extend-how.html).

The maturity descriptions are qualitative. Benchmark results are intentionally
kept out of this capability table because cross-engine performance depends on
the workload and configuration.
