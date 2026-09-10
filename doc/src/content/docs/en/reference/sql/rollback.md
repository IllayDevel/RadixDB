---
title: ROLLBACK
description: Discard a transaction or return it to a savepoint.
---

`ROLLBACK` undoes transaction-private work.

## Synopsis

```text
ROLLBACK [TRANSACTION]
ROLLBACK [TRANSACTION] TO [SAVEPOINT] savepoint_name
```

## Description

The plain form ends the transaction and discards all its work. ROLLBACK TO
undoes only changes after the named savepoint and keeps the transaction and
target savepoint active.

## Parameters

TRANSACTION and SAVEPOINT are optional words. Unquoted savepoint names are
case-insensitive; quoted names preserve case.

## Result

Success returns an empty command result. The TO form leaves a transaction
active; the plain form does not.

## Transaction behavior

Use plain ROLLBACK before retrying an entire transaction after a serialization
conflict, deadlock or row-lock timeout. Constraint errors remain explicitly
rollback-capable in the verified TCP contract.

## Errors and limitations

A plain rollback without a transaction, or a TO form without the named live
savepoint, fails. Embedded, TCP and the 1.2 CLI route `ROLLBACK TO` through the
active connection-local transaction.

## Privileges

ROLLBACK needs no object privilege of its own and cannot undo another
connection's transaction.

## Example

```sql
CREATE TABLE ref_rollback (id INTEGER PRIMARY KEY);
BEGIN;
INSERT INTO ref_rollback VALUES (1);
ROLLBACK;
SELECT id FROM ref_rollback;
```

## See also

See [Transactions and Concurrency](../../../sql/transactions/),
[SAVEPOINT](../savepoint/) and [RELEASE SAVEPOINT](../release-savepoint/).
