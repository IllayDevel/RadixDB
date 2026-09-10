---
title: UPDATE
description: Change columns of rows selected by a predicate.
---

`UPDATE` changes matching rows in one table.

## Synopsis

```text
UPDATE table_name
SET column_name = expression [, ...]
[WHERE condition]
[RETURNING expression [, ...]]
```

## Description

Each assignment is evaluated for a target row using that row's original values.
Without WHERE, every row is a target. A predicate that matches nothing is a
successful zero-row update.

## Parameters

`column_name` names a writable target column. `expression` computes its new
value, and `condition` selects rows. RETURNING evaluates the requested
expressions against changed rows.

## Result

Without RETURNING, the result is an affected-row count. RETURNING produces the
changed rows with no implied order.

## Transaction behavior

All matching rows and maintained indexes change atomically. A constraint or
write conflict cannot leave an earlier row from the same statement published.

## Errors and limitations

Constraints are checked against new values. Competing writers can report a
retryable serialization conflict or row-lock timeout. Navigable paths are not
accepted in DML expressions in 1.2.

## Privileges

A non-bootstrap session needs CONNECT, schema USAGE and UPDATE on changed
columns. Reading columns in assignments, WHERE or RETURNING independently
requires SELECT.

## Example

```sql
CREATE TABLE ref_update (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL);
INSERT INTO ref_update VALUES (1, 1), (2, 1);
UPDATE ref_update SET revision = revision + 1 WHERE id = 1 RETURNING id, revision;
```

## See also

See [Changing Data](../../../sql/dml/), [Transactions](../../../sql/transactions/)
and [ROLLBACK](../rollback/).
