---
title: BEGIN
description: Start an explicit transaction at a supported isolation level.
---

`BEGIN` starts a transaction on the current connection.

## Synopsis

```text
BEGIN [TRANSACTION]
      [ISOLATION LEVEL { READ COMMITTED | SNAPSHOT }]
```

## Description

READ COMMITTED takes a new committed view for each statement. SNAPSHOT fixes
the committed view when the transaction begins. Without a clause, the engine
uses its configured default.

## Parameters

TRANSACTION is optional noise syntax. The isolation level accepts READ
COMMITTED and SNAPSHOT at execution time.

## Result

Success returns an empty command result and leaves a transaction active on the
connection.

## Transaction behavior

A second BEGIN on the same active transaction fails. Finish the transaction
with COMMIT or ROLLBACK; closing an embedded transaction handle rolls it back.

## Errors and limitations

SERIALIZABLE, REPEATABLE READ and READ UNCOMMITTED parse but are explicitly
rejected by the executor. The 1.2 CLI drops an isolation clause and starts its
default transaction; use embedded or TCP `begin_with_isolation` for SNAPSHOT.

## Privileges

BEGIN needs an authenticated session with CONNECT. Statements inside the
transaction are authorized independently when they execute.

## Example

```sql
CREATE TABLE ref_begin (id INTEGER PRIMARY KEY);
BEGIN;
INSERT INTO ref_begin VALUES (1);
ROLLBACK;
SELECT id FROM ref_begin;
```

## See also

See [Transactions and Concurrency](../../../sql/transactions/),
[COMMIT](../commit/) and [ROLLBACK](../rollback/).
