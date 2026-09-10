---
title: DROP TABLE
description: Remove a table and its table-local index metadata.
---

`DROP TABLE` removes an existing table.

## Synopsis

```text
DROP TABLE [IF EXISTS] table_name
```

## Description

The command removes the relation, its rows and table-local index metadata. IF
EXISTS suppresses the missing-object error only.

## Parameters

`table_name` is an unqualified table name in the current catalog namespace.

## Result

Success returns a command result with no rows. IF EXISTS also succeeds when no
matching table exists.

## Transaction behavior

DROP TABLE is transactional. Until commit, other sessions retain their valid
committed view; ROLLBACK preserves the table.

## Errors and limitations

Without IF EXISTS, a missing table is an error. Dependencies and referential
constraints can prevent removal. The command does not delete another database
or unrelated files.

## Privileges

The effective Principal must own the table; the session also needs CONNECT.

## Example

```sql
CREATE TABLE ref_drop_table (id INTEGER PRIMARY KEY);
DROP TABLE ref_drop_table;
SHOW TABLES;
```

## See also

See [Defining a Schema](../../../sql/ddl/), [CREATE TABLE](../create-table/) and
[ROLLBACK](../rollback/).
