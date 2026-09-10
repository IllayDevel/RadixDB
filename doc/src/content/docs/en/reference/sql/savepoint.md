---
title: SAVEPOINT
description: Mark a recoverable point inside the current transaction.
---

`SAVEPOINT` records a rollback point in an active transaction.

## Synopsis

```text
SAVEPOINT savepoint_name
```

## Description

Later work can be undone with ROLLBACK TO without discarding earlier changes.
The savepoint remains live after ROLLBACK TO until released or the transaction
ends.

## Parameters

`savepoint_name` is an identifier. Unquoted names are case-insensitive and
quoted names preserve case.

## Result

Success returns an empty command result and adds the savepoint to the current
transaction.

## Transaction behavior

SAVEPOINT is valid only after BEGIN. COMMIT or plain ROLLBACK removes all
savepoints owned by that transaction.

## Errors and limitations

Using the command outside a transaction fails. Embedded, TCP and the 1.2 CLI
all route `SAVEPOINT` and `ROLLBACK TO SAVEPOINT` through the active
connection-local transaction.

## Privileges

SAVEPOINT requires no object privilege of its own. Statements before and after
it retain their normal authorization requirements.

## Example

```sql
CREATE TABLE ref_savepoint (id INTEGER PRIMARY KEY, value INTEGER);
BEGIN;
INSERT INTO ref_savepoint VALUES (1, 10);
SAVEPOINT before_change;
UPDATE ref_savepoint SET value = 20 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_change;
COMMIT;
SELECT id, value FROM ref_savepoint;
```

## See also

See [Transactions and Concurrency](../../../sql/transactions/),
[ROLLBACK](../rollback/) and [RELEASE SAVEPOINT](../release-savepoint/).
