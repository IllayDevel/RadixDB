<p align="center">
  <img src="logo-light.png" alt="RadixDB" width="720">
</p>

# RadixDB

[Русский](README.ru.md)

[Official website](https://radixdb.org) · [Support](mailto:dev@radixdb.org)

RadixDB is an open source SQL database written in Rust. It combines transactional
row updates with compressed column-oriented storage, bringing a compact data
footprint and analytical query capabilities to application databases.

RadixDB runs as a standalone TCP server or as an embedded Rust library. Its
design focuses on reliable recovery, controlled memory use and efficient access
to large, connected datasets.

Development of RadixDB is sponsored by [Light Soft](http://light-soft.info/).

[Documentation](doc/src/content/docs/en/index.md) · [Getting started](#getting-started) ·
[Release notes](CHANGELOG.md) · [Test results](doc/src/content/docs/en/appendices/benchmarks.md) ·
[Evidence archive](doc/public/evidence/README.md)

## Performance and accessibility

### Query performance

A comparative test on a Ryzen 9 7950X and NVMe storage measured these query
latencies on a 100-million-row relational dataset:

| Query | RadixDB | PostgreSQL 18.3 |
| --- | ---: | ---: |
| Full scan | 100.299 ms | 246.457 ms |
| Projected scan | 23.189 ms | 84.456 ms |
| Grouped aggregate with HAVING | 9.907 ms | 24.948 ms |

RadixDB was approximately 2.5 to 3.6 times faster in these cases. PostgreSQL was
faster in the same comparison for point/range lookup, the parent JOIN, bulk
loading and index creation. Results are workload-specific, use recorded
revisions and do not represent measurements of every subsequent commit.
The [full results and methodology](doc/src/content/docs/en/appendices/benchmarks.md) include
both strengths and slower cases.

### Reliability under demanding conditions

Transactions, write-ahead logging, checksummed storage and verified physical
snapshots provide the foundation for recovery. A six-hour endurance test used
100 million rows and up to 256 clients on a machine with 1.76 GiB RAM and a
5400 rpm hard drive.

During the run, a real SATA transport failure interrupted a disk flush. After
the kernel reset the link and retried the operation, the engine resumed work.
The test completed 2,351,035 operations and 2,100 invariant checks with no
invariant failures; the restored snapshot matched the source logical digest.
This demonstrates recovery from the observed transient failure, rather than
permanent loss of the drive. The report also records temporary stalls and their
recovery.

### Low memory requirements

Large databases do not have to be loaded completely into process memory.
In the same 100-million-row endurance test, peak server RSS was approximately
1,011 MiB and final RSS returned to 197 MiB.

For smaller deployments, the 20,000-row profile with 120 tables measured
23.4 MiB at clean reopen using the system allocator, or 54.2 MiB with the default
mimalloc allocator. These are separate, measured workload profiles, with
explicit worker and cache settings.

### Rust foundation

A shared Rust implementation serves embedded applications and the TCP server,
without requiring a managed runtime. Rust provides a foundation for porting the
engine across processor architectures and operating systems. Linux x86-64 is
the platform demonstrated by the results above; other targets require their
own build, filesystem and recovery validation.

Applications can use the embedded `radixdb` library, the standalone
`radixdb-client` TCP client and the `radixdb-orm` data-access layer.

## Engine design and functionality

### Hybrid storage for connected data

RadixDB combines mutable MVCC rows with immutable, compressed column blocks
organized into row groups. This hybrid row/column design supports transactional
changes while allowing scans to read the columns a query needs. Block statistics
can eliminate irrelevant row groups before reading their payloads.

The design is useful for wide, related datasets with a mixture of updates,
filtering and aggregation: asset inventories, telemetry and geographic
attribute databases, for example. Geographic attributes can benefit from these
access patterns; dedicated geometry types, spatial indexes and GIS functions
are supplied separately by a native extension rather than implied by the
storage layout.

### Compact storage and configurable caching

Immutable data artifacts store column blocks with compression. In the
September 2026 comparative test, the 100-million-row database occupied
1.96 GB of logical storage, compared with 16.13 GB for PostgreSQL: approximately
8.2 times smaller for that dataset.

Compression also makes more data fit into memory. RadixDB provides optional
operating-system page-cache warmup. When the compressed database fits the
available memory budget, most or all of its file data can remain cached,
reducing repeated disk reads. Cache residency depends on available memory and
workload; the operating system can evict pages.

The server exposes `page_cache_level`, `page_cache_max_bytes` and
`page_cache_memory_reserve`. See
[configuration and memory boundaries](doc/src/content/docs/en/administration/memory.md).

### Navigable references

RadixDB lets SQL follow declared foreign keys using a field path. Applications
can express relationships directly, without repeating JOIN clauses for every
related attribute:

```sql
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER REFERENCES departments(id)
);

SELECT e.name, e.department_id.name AS department
FROM employees AS e;
```

For valid foreign-key data, this returns the same values as a LEFT JOIN from
employees to departments. A NULL department key produces a NULL department
name. The key remains an ordinary scalar value in the employee row.

Paths can span several relationships, for example
`p.employee_id.department_id.name`. Fields sharing a path reuse its canonical
edges. The engine binds the relationships from schema metadata and executes
one query plan, avoiding an application-side query for every row. Paths can
participate in filters, grouping, sorting and aggregate expressions;
`EXPLAIN ANALYZE` exposes their execution strategy.

Navigation is read-only. It follows single-column foreign keys to a primary
key or UNIQUE NOT NULL target in the same database, with up to eight steps.
Writes name their target table explicitly; reverse one-to-many traversal uses
ordinary JOINs. See [navigable references](doc/src/content/docs/en/sql/navigable-references.md) for
integrity checks and supported query contexts.

### Integrated ORM

RadixDB includes an ORM for building queries and working with records in Rust.
It is integrated with both the embedded database API and the TCP client.
ORM operations compile to ordinary SQL with typed parameters, so raw SQL and
ORM queries can share the same connection, transaction and snapshot.

```rust
use radixdb_orm::{table, Expr, OrmBuilder, QueryBuilder};

let query = QueryBuilder::from_relation(table("employees"))
    .select([
        Expr::column("name"),
        Expr::navigation("employees", ["department_id", "name"]),
    ])
    .filter(Expr::column("department_id").eq(10))
    .limit(100);

let compiled = query.to_sql()?;
```

The example builds a query without opening a connection. Its filter value
becomes a parameter rather than interpolated SQL. A connected application can
execute the builder through the client, or use the embedded ORM execution API.

The ORM supports two complementary ways of working with data. Generated Rust
models provide typed columns, key descriptors, references and change tracking.
Dynamic entities use database schema descriptors at runtime, which is useful
for administration tools and applications whose schemas vary. Schema
descriptions can also be exported for offline model generation.

Queries cover joins, navigation, grouping, windows, CTEs and subqueries.
DDL builders describe tables and constraints; record operations distinguish
omitted values, NULL and assigned values. A versioned JSON representation and
form descriptors support tools that inspect or construct queries and forms.

A `Reference<T>` stores a target key and supports explicit read navigation.
Saving a record does not implicitly fetch or save an entire related object
graph. Automatic schema migration is a separate concern. See
[the ORM guide](doc/src/content/docs/en/clients/orm.md) and
[the runnable Rust example](examples/public/rust-orm/).

### Trusted native extensions

RadixDB 1.2 provides a stable C ABI, a safe Rust SDK and deterministic package
tooling for operator-trusted native extensions. An extension can add bounded
scalar types, native scalar and batch functions, binary operators,
B-tree/hash/bitmap operator classes and bounded planner support. The
`radixdb-spatial` proving extension implements geometry values, predicates and
Morton-key B-tree access entirely through this public boundary.

The database retains ownership of storage, WAL, MVCC, catalog publication,
index pages, ACL and recovery. Native packages run in-process and are therefore
trusted like the server binary; the startup loader accepts only complete,
checksummed packages from exact absolute allowlist entries. Database bindings
pin package version, fingerprint and codec identity.

This extension surface, together with functions, procedures and triggers,
forms a practical base feature set for domain-specific database behavior.
Applications can compose more specialized behavior from these mechanisms even
when the corresponding syntax or operational workflow differs from PostgreSQL.
See
[extension operation](doc/src/content/docs/en/administration/extensions.md),
[the developer guide](doc/src/content/docs/en/programming/native-extensions.md)
and [the public Rust example](examples/public/rust-plugin/).

### SQL capabilities

- MVCC transactions, SQL DDL/DML, joins, subqueries and window functions.
- B-tree, hash, bitmap and HNSW indexes, including unique and partial indexes.
- UUID values, UUIDv7 generation and relational constraints.
- `ROLLUP`, `CUBE`, `GROUPING SETS` and `GROUPING()`.
- Navigable references such as `employee.department_id.name`.
- Trusted native extensions with external scalar types, functions, operators
  and planner-aware index classes.

The [SQL feature matrix](doc/src/content/docs/en/appendices/compatibility.md) describes RadixDB's
own supported syntax and restrictions. Its wire protocol and SQL dialect are
native to RadixDB; drop-in PostgreSQL compatibility is not a design goal.

### PL/SQL: application logic in the database

The RadixDB 1.2 development line includes its own procedural SQL language. Functions, procedures
and triggers keep validation, multi-step updates and business rules close to
their data, within the same transaction as the changes they govern.

The accepted implementation includes typed variables and arguments,
conditions, loops, cursors, exception handling, parameterized dynamic SQL,
stored functions and atomic procedure calls through `CALL`. BEFORE/AFTER row
and statement triggers use typed `OLD` and `NEW` records. Execution shares the
SQL engine's transaction and bounded resource model; errors roll back the
enclosing statement's changes, including trigger effects. Durable Job
definitions, attempt records and the stock server's durable background
scheduler are included. Job delivery is at-least-once; procedures can use the
stable attempt idempotency key when duplicate effects matter.

This is RadixDB's own PL/SQL language, not an Oracle PL/SQL or PostgreSQL
PL/pgSQL compatibility layer.

### ACL: roles and access control

The RadixDB 1.2 development line includes database-enforced principals, roles and membership,
object ownership, table and column access, and privileges such as `SELECT`,
`INSERT`, `UPDATE`, `DELETE` and `EXECUTE`. Routines can execute with invoker
or definer authority, with checks performed inside the engine. Grants and
revocations are transactional catalog changes, and later execution observes
the new authorization state.

Row-level security is not implemented. The stock TCP server authenticates
catalog Principals with passwords and supports either ordinary TCP on a trusted
network or direct TLS with certificate and server-name validation. Authorization
then enforces database, schema, object and routine privileges for that Principal.

### Getting started

Use the toolchain in [rust-toolchain.toml](rust-toolchain.toml):

```bash
cargo build --locked --release --bin radixdb-server --bin radixdb-password --bin radixdb-cli --bin radixdb-smoke-client --features cli
target/release/radixdb-cli -e "SELECT 1"
```

The CLI opens local databases. For a persistent database, use
`-d file:///path/to/database`; for TCP connections, use the Rust client.
See [server installation](doc/src/content/docs/en/administration/installation.md),
[configuration](doc/src/content/docs/en/administration/configuration.md) and
[the RadixTrade tutorial](examples/public/radixtrade/).

Create a catalog Principal with a password for ordinary application access.
For administrative access, configure an Argon2id verifier generated by
`radixdb-password`; this requires the original password on every endpoint and
disables passwordless `root`. If no verifier is configured, passwordless
`root` remains available only as a recovery login on a plaintext loopback
endpoint. See the
[authentication guide](doc/src/content/docs/en/administration/authentication.md)
and [security policy](SECURITY.md) before exposing a connection.

### Documentation and releases

- [User documentation](doc/src/content/docs/en/index.md) and [Russian documentation](doc/src/content/docs/ru/index.md).
- [Rust client](doc/src/content/docs/en/clients/rust-client.md) and [ORM](doc/src/content/docs/en/clients/orm.md).
- [Architecture](doc/src/content/docs/en/internals/overview.md) and [storage compatibility](doc/src/content/docs/en/administration/upgrading.md).
- [Backup and maintenance](doc/src/content/docs/en/administration/backup-restore.md).
- [Release notes](CHANGELOG.md), [contributing](CONTRIBUTING.md) and [community conduct](CODE_OF_CONDUCT.md).

RadixDB 1.1.0 was released on September 8, 2026 under the annotated tag
`v1.1.0`. Use the release notes and storage compatibility contract when
selecting a revision or planning an upgrade; later branch commits are not part
of that release unless another tag says so.

### License

RadixDB uses component-based licensing. The engine, server, and embedded engine
are licensed under [PolyForm Perimeter 1.0.1](LICENSE), while the client, ORM,
wire protocol, and extension SDK components remain under
[Apache License 2.0](LICENSES/Apache-2.0.txt).

See [LICENSING.md](LICENSING.md) for the authoritative component map,
[COMMERCIAL-LICENSING.md](COMMERCIAL-LICENSING.md) for competing-use
permissions, [CLA.md](CLA.md) for contributions, and
[TRADEMARKS.md](TRADEMARKS.md) for branding rules. A Russian overview is
available in [LICENSING.ru.md](LICENSING.ru.md).
