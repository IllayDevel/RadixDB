---
title: CREATE INDEX
description: Create regular, unique, composite and partial indexes.
---

`CREATE INDEX` adds a maintained access structure to a table.

## Synopsis

```text
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name
ON table_name (column_name [operator_class] [, ...])
[USING { BTREE | HASH | BITMAP | HNSW }]
[WITH (option = expression [, ...])]
[WHERE condition]
```

## Description

BTREE, HASH and BITMAP are verified regular methods. A multi-column list creates
a composite index. UNIQUE enforces uniqueness among indexed non-NULL keys, and
WHERE limits membership to rows for which its row-local predicate is true.

## Parameters

`index_name` identifies the catalog object. `column_name` lists stored keys in
order. `operator_class` selects a visible built-in or extension-bound key and
strategy contract for that column. USING selects the method; omitted USING lets
the engine choose. WITH is primarily relevant to method-specific options.

## Result

Success returns a command result with no rows. SHOW INDEXES reports the stored
definition; EXPLAIN is needed to inspect planner selection.

## Transaction behavior

Index creation is transactional and is published with the table catalog
generation at commit.

## Errors and limitations

A duplicate active UNIQUE key fails. Partial predicates must be deterministic
and row-local. Partial HNSW indexes are rejected. IF NOT EXISTS suppresses only
an identical definition; a different definition under the same name fails. An
operator class must match the column type and selected access method.

## Privileges

The effective Principal must own the target table; the session also needs
CONNECT. The index records compatible ownership in the catalog.

## Example

```sql
CREATE TABLE ref_create_index (id INTEGER PRIMARY KEY, email TEXT, active BOOLEAN);
CREATE UNIQUE INDEX ref_email_active_idx
ON ref_create_index (email) USING HASH WHERE active = true;
SHOW INDEXES FROM ref_create_index;
```

## See also

See [Indexes](../../../sql/indexes/),
[Native extension commands](../extensions/), [ALTER INDEX](../alter-index/) and
[SHOW](../show/).
