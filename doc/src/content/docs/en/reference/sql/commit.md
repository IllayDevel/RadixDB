---
title: COMMIT
description: Atomically publish the current transaction.
---

`COMMIT` finishes the current transaction successfully.

## Synopsis

```text
COMMIT [TRANSACTION]
```

## Description

COMMIT publishes staged row, catalog and index changes as one visibility
boundary. TRANSACTION is optional noise syntax.

## Parameters

The command has no data parameters and applies only to the active transaction
on the current connection.

## Result

Success returns an empty command result and leaves no transaction active.

## Transaction behavior

Atomic visibility does not itself select storage durability. The configured
synchronization mode determines when committed storage is forced to media.

## Errors and limitations

COMMIT without an active transaction fails. A commit error can leave a
transaction rollback-capable; inspect the client state instead of assuming the
write either succeeded or vanished.

## Privileges

COMMIT requires no object privilege of its own. Every staged statement has
already passed its normal authorization checks.

## Example

```sql
CREATE TABLE ref_commit (id INTEGER PRIMARY KEY, title TEXT);
BEGIN;
INSERT INTO ref_commit VALUES (1, 'published');
COMMIT;
SELECT id, title FROM ref_commit;
```

## See also

See [Transactions and Concurrency](../../../sql/transactions/),
[BEGIN](../begin/) and [Storage](../../../administration/storage/).
