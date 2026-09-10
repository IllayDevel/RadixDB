---
title: Indexes
description: Create, inspect and remove regular, unique, composite and partial indexes.
---

An index is an additional access structure maintained with a table. It can
reduce the work needed by a matching query, and a unique index can enforce
a key rule. Every index also consumes storage and adds maintenance work to
writes. Create one for a measured access pattern or constraint, not for every
column by default.

Run the blocks below in order in one separate test database. The examples use
their own `contacts` table.

## Create and Inspect Indexes

RadixDB supports BTREE, HASH, BITMAP and vector-specific HNSW methods.
The engine can choose a default method when USING is omitted. State the method
when the schema depends on a particular contract.

```sql
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    email TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    deleted_at TIMESTAMP,
    embedding VECTOR(3)
);
INSERT INTO contacts (id, tenant_id, email) VALUES
    (1, 10, 'owner@example.test'),
    (2, 10, NULL),
    (3, 10, NULL),
    (4, 20, 'other@example.test');
CREATE INDEX contacts_tenant_email_idx
    ON contacts (tenant_id, email) USING BTREE;
CREATE INDEX contacts_active_idx
    ON contacts (active) USING BITMAP;
CREATE UNIQUE INDEX contacts_email_active_uidx
    ON contacts (email) USING HASH
    WHERE deleted_at IS NULL;
SHOW INDEXES FROM contacts;
```

SHOW INDEXES reports the primary-key index and the three explicit indexes,
including names, indexed columns, method, uniqueness and options. A composite
index stores an ordered list of columns. Its usefulness depends on predicates
and ordering that match a supported leading prefix; merely mentioning one
of its later columns does not guarantee selection.

HASH is suited to supported equality paths, BITMAP to supported low-cardinality
paths, and BTREE to supported equality, ordering and range paths. These are
capabilities, not a promise that every syntactically related query chooses
the index. Use EXPLAIN on the actual query and representative schema.

## Extension operator classes

A trusted native extension can bind comparison operators, an operator class and
bounded planner support to a core-owned index method. The column definition then
names the operator class explicitly:

```sql
CREATE INDEX asset_point_idx
ON assets (position geo.point_btree) USING BTREE;
```

The extension encodes canonical index keys and can propose bounded candidate
ranges. RadixDB still owns MVCC visibility, index pages, scan execution,
publication and recovery. When the planner-support descriptor requires recheck,
the original predicate is always evaluated after the candidate scan.

The 1.2 SDK supports external B-tree, hash and bitmap classes. External HNSW
authoring is reserved but rejected. An unavailable exact plugin package puts a
database using its objects into restricted diagnostic mode rather than reading
index keys under a different codec. See
[Native extension commands](../../reference/sql/extensions/).

## Unique and Partial Indexes

UNIQUE rejects duplicate non-NULL key values. In the setup, both rows with
a NULL email are allowed: ordinary SQL uniqueness does not make NULL equal
to another NULL.

The partial unique index includes only rows for which `deleted_at IS NULL`
is true. It expresses uniqueness among current contacts while allowing an
email to be reused after the old row is marked deleted. This active duplicate
is rejected:

```sql
INSERT INTO contacts (id, tenant_id, email)
VALUES (5, 30, 'owner@example.test');
```

After the old row leaves the partial index, the same business value can be
inserted again:

```sql
UPDATE contacts SET deleted_at = '2026-09-08T00:00:00Z' WHERE id = 1;
INSERT INTO contacts (id, tenant_id, email)
VALUES (5, 30, 'owner@example.test');
SELECT id, email, deleted_at IS NULL AS current
FROM contacts
WHERE email = 'owner@example.test'
ORDER BY id;
```

The query returns ID 1 with current=false and ID 5 with current=true.
Restoring ID 1 to NULL while ID 5 remains current would violate uniqueness.

A partial predicate must be deterministic and evaluable from the indexed row.
Supported introductory forms include literals, unqualified columns,
comparisons, IS NULL/IS NOT NULL, AND/OR/NOT, BETWEEN, literal IN lists and
LIKE with a literal pattern. Do not put subqueries, aggregates, runtime
parameters or unrelated-table references into an index predicate.

Partial HNSW indexes are not supported:

```sql
CREATE INDEX contacts_embedding_active_idx
    ON contacts (embedding) USING HNSW
    WHERE active = true;
```

This statement is expected to fail explicitly. HNSW configuration and vector
query semantics belong to the dedicated vector/index reference; a regular
index example is not evidence for those options.

## Planner Safety

The planner may use a partial index only when it can prove that the query's
rows satisfy the index predicate. This query contains the predicate explicitly:

```sql
EXPLAIN SELECT id FROM contacts
WHERE email = 'owner@example.test' AND deleted_at IS NULL;
```

On the checked hot-row dataset, the plan selects
`contacts_email_active_uidx`. By contrast, this query must also see deleted rows:

```sql
EXPLAIN SELECT id FROM contacts
WHERE email = 'owner@example.test';
```

The checked plan uses a sequential scan and reports
`Partial Index Eligibility: no_proven_partial_index`. A fallback can be slower,
but using the partial index without proving its predicate would be incorrect.
Plan text and physical access source can change after checkpointing; assert
query results first and use EXPLAIN as diagnostic evidence for the tested state.

## Repeat, Rename and Drop

IF NOT EXISTS suppresses only a repeated compatible definition:

```sql
CREATE INDEX IF NOT EXISTS contacts_tenant_email_idx
    ON contacts (tenant_id, email) USING BTREE;
```

Reusing that name for different columns or a different method is an error,
not a migration:

```sql
CREATE INDEX IF NOT EXISTS contacts_tenant_email_idx
    ON contacts (email) USING HASH;
```

Rename preserves the index definition. DROP INDEX currently requires both
the index and table names:

```sql
ALTER INDEX contacts_tenant_email_idx RENAME TO contacts_scope_idx;
SHOW INDEXES FROM contacts;
DROP INDEX contacts_scope_idx ON contacts;
SHOW INDEXES FROM contacts;
```

The first SHOW includes `contacts_scope_idx`; the second does not. The primary
key, bitmap index and partial unique index remain. `DROP INDEX contacts_scope_idx`
without `ON contacts` is rejected in this version.

Index creation and deletion are schema changes. Test them in a transaction
when atomic publication with related DDL/DML matters, and verify the result
with SHOW INDEXES after commit or rollback. Do not edit rebuildable index files
or table metadata directly.
