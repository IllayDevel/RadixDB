---
title: RELEASE SAVEPOINT
description: Remove a savepoint while keeping its transaction active.
---

`RELEASE SAVEPOINT` removes a named rollback point.

## Synopsis

```text
RELEASE [SAVEPOINT] savepoint_name
```

## Description

The command keeps all changes made before and after the savepoint. It removes
only the ability to roll back to that marker.

## Parameters

SAVEPOINT is optional. `savepoint_name` follows the same quoted and unquoted
identifier rules as SAVEPOINT.

## Result

Success returns an empty command result and leaves the transaction active.

## Transaction behavior

RELEASE is valid only inside the transaction that owns the live savepoint. It
does not commit any change.

## Errors and limitations

A missing transaction or savepoint fails. Embedded, TCP and the 1.2 CLI route
`RELEASE SAVEPOINT` through the active connection-local transaction.

## Privileges

RELEASE needs no object privilege of its own.

## Example

```sql
CREATE TABLE ref_release_savepoint (id INTEGER PRIMARY KEY);
BEGIN;
SAVEPOINT completed_step;
INSERT INTO ref_release_savepoint VALUES (1);
RELEASE SAVEPOINT completed_step;
COMMIT;
SELECT id FROM ref_release_savepoint;
```

## See also

See [SAVEPOINT](../savepoint/), [ROLLBACK](../rollback/) and
[Client Interfaces](../../../clients/overview/).
