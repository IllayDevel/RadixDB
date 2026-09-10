---
title: Defining a Schema
description: Create, inspect, alter and remove tables and constraints.
---

Data definition statements create and change database objects. A table combines
named columns, their types and constraints. Schema changes affect subsequent
reads and writes, so apply them as reviewed migrations rather than as startup
side effects in every application process.

The examples below use their own tables. Run the blocks in order in one test
database. Do not reuse these names in a database containing important data.

## Create Tables

Each column definition contains a name and a [data type](../types/), followed
by optional constraints. This example uses a generated primary key, a foreign
key, required values, defaults, uniqueness and a check expression:

```sql
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
CREATE TABLE assets (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    department_id INTEGER REFERENCES departments(id),
    serial TEXT NOT NULL UNIQUE,
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    note TEXT
);
INSERT INTO departments VALUES (1, 'ops');
INSERT INTO assets (department_id, serial) VALUES (1, 'A-1');
DESCRIBE assets;
SELECT id, department_id, serial, quantity, note FROM assets;
```

DESCRIBE reports the column name, type, nullability, key marker, default and
extra properties. The inserted asset has generated ID 1, quantity 0 and a NULL
note. `PRIMARY KEY` implies NOT NULL. AUTO_INCREMENT is supported for INTEGER
and UUID primary-key paths; it is not a general sequence declaration for every
type.

`CREATE TABLE IF NOT EXISTS` can make a repeated creation request a no-op.
It does not migrate an existing table to a new definition. Inspect the existing
schema and use explicit ALTER statements when a migration is intended.

## Constraint Failures

NOT NULL rejects a missing required value:

```sql
INSERT INTO assets (department_id, serial) VALUES (1, NULL);
```

CHECK accepts only rows for which its expression is not false. This insert
violates `quantity >= 0`:

```sql
INSERT INTO assets (department_id, serial, quantity) VALUES (1, 'A-2', -1);
```

A foreign key requires the referenced row:

```sql
INSERT INTO assets (department_id, serial) VALUES (99, 'A-3');
```

UNIQUE rejects a second non-NULL serial:

```sql
INSERT INTO assets (department_id, serial) VALUES (1, 'A-1');
```

Each statement above is expected to fail. An error is not a successful
zero-row write. In an explicit transaction, inspect the reported transaction
state and roll back when required before continuing. UNIQUE normally permits
multiple NULL values; use NOT NULL as well when a business key must always
be present.

Foreign-key actions default to restrictive behavior. `ON DELETE` and
`ON UPDATE` actions such as CASCADE and SET NULL require deliberate schema
design; verify their effect on the complete relationship before deploying
a migration. SET NULL is incompatible with a child column that cannot be NULL.

## Alter a Table

ALTER TABLE supports adding and dropping columns, renaming a column or table,
and modifying a column definition in the supported forms:

```sql
CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO items VALUES (1, 'one');
ALTER TABLE items ADD COLUMN location TEXT NOT NULL DEFAULT 'warehouse';
SELECT id, label, location FROM items;
ALTER TABLE items RENAME COLUMN label TO title;
ALTER TABLE items MODIFY COLUMN title TEXT NOT NULL DEFAULT 'untitled';
ALTER TABLE items DROP COLUMN location;
ALTER TABLE items RENAME TO inventory;
DESCRIBE inventory;
SELECT id, title FROM inventory;
```

The existing row receives `warehouse` when the required column is added.
After the remaining changes, `inventory` contains row `1, one`; DESCRIBE shows
`title` as required with default `'untitled'`.

Treat ALTER as a data migration. A new NOT NULL rule must be valid for existing
rows, type changes can require conversion, and dropping a column discards its
values. Back up important data and test the migration on a copy. Do not infer
that every ALTER form is an inexpensive metadata-only operation.

Constraints added or changed by ALTER must still be satisfied by existing and
future rows. The introductory contract does not promise every combination of
PRIMARY KEY, UNIQUE and AUTO_INCREMENT in MODIFY COLUMN; use explicit indexes
and a tested migration when changing key structure.

## Transactional DDL and Cleanup

Schema and data changes can share an explicit transaction. A rollback removes
the uncommitted table and its row:

```sql
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS inventory;
BEGIN;
CREATE TABLE staged (id INTEGER PRIMARY KEY);
INSERT INTO staged VALUES (1);
ROLLBACK;
SHOW TABLES;
```

SHOW TABLES returns no rows. `DROP TABLE IF EXISTS` suppresses the missing-table
error during repeatable cleanup; DROP TABLE without IF EXISTS reports a missing
object. Dropping a table also removes its table-local index metadata. It does
not remove unrelated files or databases.

Keep BEGIN, the schema statements and COMMIT or ROLLBACK in the same session.
Before applying a production migration, verify the resulting schema with
DESCRIBE/SHOW INDEXES and exercise both the success and rollback paths.

Continue with [indexes](../indexes/) for access paths and additional uniqueness.
