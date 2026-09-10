---
title: SET
description: Change the supported default isolation setting.
---

`SET` changes the default isolation level for the current connection.

## Synopsis

```text
SET { ISOLATION_LEVEL | ISOLATIONLEVEL | TRANSACTION_ISOLATION }
    { = | TO } { 'READ COMMITTED' | 'SNAPSHOT' }
```

## Description

All three variable spellings target the same connection-local isolation
setting. Other connections, including handles for the same embedded engine,
retain their own defaults.

## Parameters

The value must be a string literal naming READ COMMITTED or SNAPSHOT. Use BEGIN
with an explicit isolation level when a per-transaction choice is required.

## Result

Success returns an empty command result. New transactions without an explicit
level use the updated default.

## Transaction behavior

SET changes the default isolation of the current connection only. It does not
modify an already active transaction or the defaults of sibling connections.

## Errors and limitations

Unknown variable names and isolation values fail. Changing the default while a
transaction is active also fails, so the next transaction has an unambiguous
connection-local default.

## Privileges

SET requires an authenticated session with CONNECT but no object privilege.

## Example

```sql
SET ISOLATION_LEVEL = 'READ COMMITTED';
BEGIN;
ROLLBACK;
```

## See also

See [BEGIN](../begin/), [Transactions and Concurrency](../../../sql/transactions/)
and [Configuration](../../configuration/).
