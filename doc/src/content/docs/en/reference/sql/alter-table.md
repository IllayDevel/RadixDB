---
title: ALTER TABLE
description: Add, remove, rename or modify a table column, or rename a table.
---

`ALTER TABLE` changes an existing table definition.

## Synopsis

```text
ALTER TABLE table_name ADD [COLUMN] column_definition
ALTER TABLE table_name DROP [COLUMN] [IF EXISTS] column_name
ALTER TABLE table_name MODIFY [COLUMN] column_definition
ALTER TABLE table_name RENAME [COLUMN] column_name TO new_column_name
ALTER TABLE table_name RENAME TO new_table_name
```

## Description

ADD introduces a column, DROP removes one, MODIFY replaces the supported column
definition, and RENAME changes a column or table name. Treat every form as a
data migration.

## Parameters

`column_definition` contains a name, type and supported constraints. A required
column added to existing rows needs a usable default. New names must not collide
with existing catalog names.

## Result

Success returns a command result with no rows. DESCRIBE shows the resulting
column definition.

## Transaction behavior

ALTER TABLE is transactional and keeps catalog, rows and indexes within the
same commit boundary. ROLLBACK restores the prior definition.

## Errors and limitations

Existing data must satisfy the new definition. Dropping referenced or indexed
columns and unsupported key transformations can fail. The matrix does not
promise every constraint or type conversion through MODIFY.

## Privileges

The effective Principal must own the table; the session also needs CONNECT.
The bootstrap owner bypasses ordinary object checks.

## Example

```sql
CREATE TABLE ref_alter_table (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO ref_alter_table VALUES (1, 'draft');
ALTER TABLE ref_alter_table ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE ref_alter_table RENAME COLUMN label TO title;
SELECT id, title, revision FROM ref_alter_table;
```

## See also

See [Defining a Schema](../../../sql/ddl/), [CREATE TABLE](../create-table/) and
[Access Control](../../../administration/access-control/).
