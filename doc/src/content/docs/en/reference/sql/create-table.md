---
title: CREATE TABLE
description: Define a table, its columns and verified constraints.
---

`CREATE TABLE` adds a relation to the catalog.

## Synopsis

```text
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name data_type [column_constraint ...] [, ...]
    [, table_constraint ...]
)

column_constraint := [PRIMARY KEY] [AUTO_INCREMENT] [NOT NULL]
                     [UNIQUE] [DEFAULT expression] [CHECK (condition)]
                     [REFERENCES table_name (column_name)]
table_constraint  := PRIMARY KEY (column_name [, ...])
                   | UNIQUE (column_name [, ...])
                   | CHECK (condition)
```

## Description

The command creates columns and supported integrity rules as one catalog
change. IF NOT EXISTS suppresses creation only when the relation name already
exists; it does not reconcile definitions.

## Parameters

`data_type` uses the RadixDB type names. DEFAULT supplies omitted values. CHECK
rejects false expressions. REFERENCES declares a verified single-column
foreign-key path. AUTO_INCREMENT is limited to supported INTEGER and UUID
primary-key paths.

## Result

Success returns a command result with no rows. The new table becomes visible at
commit and can be inspected with DESCRIBE.

## Transaction behavior

CREATE TABLE is transactional. It may share an explicit transaction with DML;
ROLLBACK removes the uncommitted relation and its rows.

## Errors and limitations

Duplicate names, invalid types or constraints and incompatible generated-key
forms fail. IF NOT EXISTS is not a migration and does not compare schemas.

## Privileges

A non-bootstrap session needs CONNECT and USAGE on the target schema. RadixDB
1.2 has no separate schema CREATE privilege, so USAGE also permits creation.
The creator becomes owner.

## Example

```sql
CREATE TABLE ref_create_table (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    code TEXT NOT NULL UNIQUE,
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0)
);
DESCRIBE ref_create_table;
```

## See also

See [Defining a Schema](../../../sql/ddl/), [ALTER TABLE](../alter-table/) and
[Data Types](../../../sql/types/).
