---
title: INSERT
description: Add rows and resolve supported uniqueness conflicts.
---

`INSERT` adds one or more rows to a table.

## Synopsis

```text
INSERT INTO table_name [(column_name [, ...])]
{ VALUES (expression [, ...]) [, ...] | select_query }
[ON CONFLICT [(column_name [, ...])]
    DO { NOTHING | UPDATE SET column_name = expression [, ...] }]
[RETURNING expression [, ...]]
```

## Description

The column list maps source values to target columns. Omitted columns use their
default, generated value or NULL when allowed. INSERT SELECT and a CTE followed
by SELECT are accepted source forms.

## Parameters

Every VALUES row must have the same width as the target list. `ON CONFLICT DO
NOTHING` skips a conflicting row. `DO UPDATE` may use the proposed row through
`excluded.column_name`. RETURNING evaluates against rows actually inserted or
updated.

## Result

Without RETURNING, clients receive the affected-row count. RETURNING produces a
row set; its order is unspecified unless a later query orders persisted rows.

## Transaction behavior

The whole statement is atomic, including multi-row input and conflict actions.
It joins the active transaction or runs as one autocommit transaction.

## Errors and limitations

Type, NOT NULL, CHECK, foreign-key and uniqueness failures reject the statement.
Navigable paths are read-only and cannot be target columns or write expressions.
Conflict handling is limited to the forms verified by the coverage matrix.

## Privileges

A non-bootstrap session needs CONNECT, schema USAGE and INSERT on every target
column. Expressions, SELECT sources and DO UPDATE reads additionally require
SELECT; conflict updates require UPDATE on changed columns.

## Example

```sql
CREATE TABLE ref_insert (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
INSERT INTO ref_insert (id, title) VALUES (1, 'draft'), (2, 'review')
RETURNING id, title;
```

## See also

See [Changing Data](../../../sql/dml/), [UPDATE](../update/) and
[Access Control](../../../administration/access-control/).
