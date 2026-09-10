---
title: DELETE
description: Remove rows selected by a predicate.
---

`DELETE` removes matching rows from one table.

## Synopsis

```text
DELETE FROM table_name
[WHERE condition]
[RETURNING expression [, ...]]
```

## Description

WHERE selects rows to remove. Omitting it targets every row. Foreign-key actions
and restrictions are applied as part of the same statement.

## Parameters

`table_name` is the target table. `condition` is evaluated for each candidate
row. RETURNING reads values from rows selected for deletion.

## Result

Without RETURNING, the result is an affected-row count. RETURNING produces the
deleted rows; no row order is guaranteed.

## Transaction behavior

The deletion and all supported referential actions are atomic. A rollback
restores transaction-private deletions; other sessions see them only after
commit.

## Errors and limitations

Restrictive foreign keys can reject a delete. Writer waits, deadlocks and
timeouts follow the transaction retry contract. Navigable paths are rejected in
DML predicates and RETURNING expressions in 1.2.

## Privileges

A non-bootstrap session needs CONNECT, schema USAGE and table-level DELETE.
Columns read by WHERE or RETURNING additionally require SELECT.

## Example

```sql
CREATE TABLE ref_delete (id INTEGER PRIMARY KEY, state TEXT NOT NULL);
INSERT INTO ref_delete VALUES (1, 'done'), (2, 'open');
DELETE FROM ref_delete WHERE state = 'done' RETURNING id, state;
```

## See also

See [Changing Data](../../../sql/dml/), [DROP TABLE](../drop-table/) and
[Transactions](../../../sql/transactions/).
