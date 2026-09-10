---
title: SQL Coverage Matrix
description: Verified SQL 1.2 constructs, limitations, rejections and executable evidence.
---

This matrix is the conservative SQL contract documented for RadixDB 1.2. A
row is marked supported only when its published example or focused engine test
passes on the pinned source revision. A syntactically similar construct that is
not listed here is not automatically part of the public contract.

The statuses mean:

- **Supported**: the listed 1.2 form is implemented and has passing evidence.
- **Limited**: a defined subset works; read the linked chapter before relying on it.
- **Rejected**: 1.2 deliberately returns an error, and the failure is tested.

Every test column names a script under `doc/scripts/`. Those scripts read
the examples from both language versions, compare them and check successful as
well as rejected operations against revision
`40b1b3d13e050afa2666a0414b7215d5ac1452c0`.

## Representative outcomes

This small successful expression is checked along with the complete matrix:

```sql
SELECT COALESCE(NULL, 7) AS value;
```

The following unsupported clause is expected to fail; rejection is a verified
outcome rather than an omitted example:

```sql
SELECT 1 AS value QUALIFY value = 1;
```

## Syntax, types and expressions

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| SYN-01 | Identifiers, literals, comments and statement separators | Supported | 1.2 | [Syntax](../../sql/syntax/) | `test-syntax.mjs` |
| SYN-02 | Positional and named parameters | Limited | 1.2 | [Syntax](../../sql/syntax/) | `test-syntax.mjs` |
| TYPE-01 | INTEGER and integer aliases | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-02 | FLOAT, DOUBLE and REAL | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-03 | DECIMAL/NUMERIC with precision and scale | Limited | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-04 | TEXT and text aliases | Limited | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-05 | BOOLEAN/BOOL | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-06 | TIMESTAMP, DATETIME and TIME aliases | Limited | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-07 | DATE | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-08 | UUID | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-09 | BYTES and binary aliases | Supported | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-10 | JSON/JSONB storage type | Limited | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| TYPE-11 | VECTOR dimension declaration | Limited | 1.2 | [Data Types](../../sql/types/) | `test-values.mjs` |
| EXPR-01 | Arithmetic and precedence | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |
| EXPR-02 | Comparisons and three-valued NULL logic | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |
| EXPR-03 | IN, NOT IN and BETWEEN | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |
| EXPR-04 | CASE, COALESCE and NULLIF | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |
| EXPR-05 | String concatenation, LIKE and scalar calls | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |
| EXPR-06 | CAST | Supported | 1.2 | [Expressions](../../sql/expressions/) | `test-values.mjs` |

Parameters are bound through client APIs; the CLI examples do not invent a
literal substitution convention. Text length modifiers are rejected, TIME is
an alias of timestamp storage rather than a time-only type, and JSON or VECTOR
type recognition does not imply PostgreSQL's operator surface.

## Schema and indexes

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| DDL-01 | CREATE TABLE and IF NOT EXISTS | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-02 | PRIMARY KEY, NOT NULL and DEFAULT | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-03 | CHECK, UNIQUE and single-column REFERENCES | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-04 | INTEGER primary-key AUTO_INCREMENT | Limited | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-05 | DESCRIBE and SHOW TABLES | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-06 | ALTER ADD/DROP/MODIFY/RENAME COLUMN | Limited | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-07 | ALTER TABLE RENAME TO | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-08 | Transactional DDL rollback | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| DDL-09 | DROP TABLE IF EXISTS | Supported | 1.2 | [Defining a Schema](../../sql/ddl/) | `test-schema.mjs` |
| IDX-01 | CREATE INDEX with BTREE, HASH or BITMAP | Supported | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-02 | Composite index | Supported | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-03 | UNIQUE and SQL NULL uniqueness | Supported | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-04 | Partial index with row-local predicate | Supported | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-05 | Partial-index planner implication | Limited | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-06 | Partial HNSW index | Rejected | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-07 | CREATE INDEX IF NOT EXISTS identity check | Limited | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-08 | SHOW INDEXES and ALTER INDEX RENAME | Supported | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |
| IDX-09 | DROP INDEX name ON table | Limited | 1.2 | [Indexes](../../sql/indexes/) | `test-schema.mjs` |

`ALTER ... MODIFY` is limited to the forms proved in the chapter; it is not a
promise for every key transformation. A partial index is used only when its
predicate is proven for the query. `DROP INDEX` currently requires `ON table`.

## Native extensions

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| EXT-01 | Exact package binding with CREATE/DROP EXTENSION ... RESTRICT | Supported | 1.2 | [Native extension commands](../../reference/sql/extensions/) | `test-extensions.mjs` |
| EXT-02 | Catalog-bound external scalar types and protocol values | Limited | 1.2 | [Data Types](../../sql/types/) | `test-extensions.mjs` |
| EXT-03 | Native scalar and explicit batch functions | Supported | 1.2 | [Developing native extensions](../../programming/native-extensions/) | `test-extensions.mjs` |
| EXT-04 | Plugin operators, B-tree/hash/bitmap classes and bounded planner support | Limited | 1.2 | [Indexes](../../sql/indexes/) | `test-extensions.mjs` |
| EXT-05 | Path/URL install, version ranges, CASCADE, hot reload and ALTER EXTENSION UPDATE | Rejected | 1.2 | [Extensions](../../administration/extensions/) | `test-extensions.mjs` |

External types require exact package identity and codec revision. The generic
ORM and generic SQL literals do not decode or construct their canonical bytes.
The Rust authoring SDK supports binary operators and rejects external HNSW,
aggregate, window and table-valued descriptors in 1.2. Packages are trusted
in-process code admitted only from an explicit startup allowlist.

## Data modification

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| DML-01 | INSERT with column list and multiple rows | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-02 | INSERT/UPDATE/DELETE RETURNING | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-03 | UPDATE and DELETE predicates | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-04 | Zero affected rows as a command result | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-05 | ON CONFLICT DO NOTHING | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-06 | ON CONFLICT DO UPDATE and excluded values | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-07 | Statement atomicity after a row conflict | Supported | 1.2 | [Changing Data](../../sql/dml/) | `test-dml.mjs` |
| DML-08 | Navigable paths in write expressions | Rejected | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |

## Queries

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| QUERY-01 | SELECT projection and WHERE | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-02 | ORDER BY with NULLS FIRST/LAST | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-03 | LIMIT and OFFSET | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-04 | INNER, LEFT, RIGHT, FULL and CROSS JOIN | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-05 | JOIN ON, USING and NATURAL JOIN | Limited | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-06 | GROUP BY, HAVING, COUNT, SUM and AVG | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-07 | Scalar, IN, EXISTS and FROM subqueries | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-08 | Non-recursive CTE | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-09 | Recursive CTE with UNION ALL | Limited | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-10 | Recursive CTE with duplicate-eliminating UNION | Rejected | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-11 | Window ranking, navigation and aggregates | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-12 | Nullable indexed window partition after reopen | Supported | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-13 | LATERAL derived table | Rejected | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-14 | QUALIFY | Rejected | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |
| QUERY-15 | ORDER BY hidden qualified input after aggregation | Rejected | 1.2 | [Querying Data](../../sql/queries/) | `test-queries.mjs` |

NATURAL JOIN is supported but intentionally discouraged for durable schemas
because later same-name columns change its condition. Recursive CTEs require
`UNION ALL` and an explicit termination condition. Nullable indexed partitions
retain their `NULL` group across cold storage and reopen.

## Transactions and concurrency

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| TX-01 | Autocommit and BEGIN/COMMIT/ROLLBACK | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-02 | SAVEPOINT, ROLLBACK TO and RELEASE | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-03 | READ COMMITTED isolation | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-04 | SNAPSHOT isolation | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-05 | SERIALIZABLE, REPEATABLE READ and READ UNCOMMITTED | Rejected | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-06 | CLI isolation clause and savepoint routing | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-07 | SET isolation as a connection-local default | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-08 | Same-row writer wait, deadlock and retry | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |
| TX-09 | Rollback-capable constraint error and cross-table atomicity | Supported | 1.2 | [Transactions](../../sql/transactions/) | `test-transactions.mjs` |

TX-06 and TX-07 use the same connection-local transaction state in embedded,
TCP and CLI paths. Unsupported isolation levels fail before a transaction is
opened.

## Navigable references

| ID | Construct | Status | Version | Chapter | Test |
| --- | --- | --- | --- | --- | --- |
| NAV-01 | Single source-FK to target-field path | Supported | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-02 | Shared prefix and transitive path | Supported | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-03 | LEFT/NULL path semantics | Supported | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-04 | Paths in read-only SELECT contexts | Supported | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-05 | Deterministic root and schema diagnostics | Supported | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-06 | Navigation in DML | Rejected | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-07 | Navigation in persisted VIEW | Rejected | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-08 | Depth/path/edge limits 8/256/512 | Limited | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-09 | Composite or reverse path | Rejected | 1.2 | [Navigable References](../../sql/navigable-references/) | `test-navigation.mjs` |

## Maintaining the matrix

A new SQL claim must add or update a row, the linked explanation and executable
evidence in the same change. A feature moving from rejected or limited to
supported must first pass against the target release revision. Do not remove a
limitation merely because the parser accepts its tokens.

This matrix describes SQL behavior, not every function overload, configuration
parameter, client method or physical-plan strategy. Those belong in the
reference and administration parts of the manual.
