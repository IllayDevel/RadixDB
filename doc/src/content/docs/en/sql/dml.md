---
title: Changing Data
description: Insert, update and delete rows, inspect results and handle conflicts.
---

INSERT adds rows, UPDATE changes existing rows and DELETE removes rows.
Each operation is checked against the table's types, constraints and applicable
permissions. Use an explicit transaction when several statements must form
one application operation; a sequence of separate CLI processes is not one
transaction.

Run the following blocks in order in one test database. They use their own
`tasks` table and do not depend on the tutorial's employees table.

```sql
CREATE TABLE tasks (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL DEFAULT false,
    revision INTEGER NOT NULL DEFAULT 1
);
INSERT INTO tasks (id, title) VALUES (1, 'inspect'), (2, 'publish');
```

## Insert Rows

Use an explicit column list so the statement does not depend on the physical
order of every column. Omitted columns use their defaults where declared.
An explicit NULL is not the same request as omitting a column with a default.

```sql
INSERT INTO tasks (id, title) VALUES (3, 'archive')
RETURNING id, title, done;
```

The result is `3, archive, false`. RETURNING produces a result set; consume it
through the client result/cursor interface. Do not assume every write returns
only an affected-row count. Without RETURNING, successful writes report a
command result; the initial multi-row INSERT affects two rows.

## Update Selected Rows

SET defines the new values, and WHERE selects the rows to update. Without a
WHERE clause, UPDATE applies to all rows in the table. Inspect the intended
selection before running a broad update.

```sql
UPDATE tasks SET done = true, revision = revision + 1
WHERE id = 1 AND revision = 1
RETURNING id, done, revision;
```

The result is `1, true, 2`. The revision predicate is an application-level
optimistic check: the update is permitted only while the stored revision has
the value the application previously read. The application, not the database,
chooses the revision convention.

```sql
UPDATE tasks SET title = 'stale update'
WHERE id = 1 AND revision = 1;
```

This affects zero rows because the preceding update changed the revision to 2.
Zero affected rows is not a SQL error. The application must decide whether it
means a stale revision, a missing row or simply no matching work. This example
does not establish the isolation semantics of two concurrent transactions.

## Handle an Insert Conflict

Use ON CONFLICT for the conflict policy supported by the selected key.
DO NOTHING leaves an existing row unchanged:

```sql
INSERT INTO tasks (id, title) VALUES (1, 'duplicate')
ON CONFLICT (id) DO NOTHING;
```

This affects zero rows. DO UPDATE can replace selected values with those
proposed for insertion; `excluded.title` refers to the proposed title:

```sql
INSERT INTO tasks (id, title) VALUES (1, 'renamed')
ON CONFLICT (id) DO UPDATE SET title = excluded.title
RETURNING id, title;
```

The result is `1, renamed`. The existing `done` and `revision` values remain
unchanged because SET does not assign them. A key conflict policy is not a
general exception handler for invalid types, permissions or unrelated constraints.

## Delete Rows

DELETE selects rows with WHERE. Without WHERE it removes all rows; it does
not remove the table definition. RETURNING reports values from the deleted row.

```sql
DELETE FROM tasks WHERE id = 2 RETURNING id, title;
SELECT id, title, done, revision FROM tasks ORDER BY id;
```

The deleted row is `2, publish`. The final SELECT returns:

| id | title | done | revision |
| --- | --- | --- | --- |
| 1 | renamed | true | 2 |
| 3 | archive | false | 1 |

The SELECT uses ORDER BY deliberately. RETURNING does not specify a stable
ordering for a write affecting several rows.

## Transactions and Errors

To undo a temporary change, keep BEGIN and ROLLBACK in the same session:

```sql
BEGIN;
UPDATE tasks SET title = 'temporary' WHERE id = 1;
ROLLBACK;
SELECT title FROM tasks WHERE id = 1;
```

The title remains `renamed`. Use the [transaction tutorial](../../tutorial/transactions/)
for the distinction between commit and rollback. Network clients may provide
dedicated transaction methods instead of accepting transaction control through
their generic SQL execution method.

Handle errors explicitly. A lost connection or missing reply does not prove
that a write failed; do not retry blindly. For a constraint failure, inspect
the client transaction state and roll back an explicit transaction when needed.
The example suite also checks that a rejected multi-row INSERT leaves no first
row behind after a later row conflicts with an existing primary key.
