---
title: DESCRIBE
description: Inspect a table or emit the JSON database descriptor.
---

`DESCRIBE` returns catalog metadata.

## Synopsis

```text
{ DESCRIBE | DESC } [TABLE] table_name [FORMAT JSON]
DESCRIBE DATABASE FORMAT JSON
```

## Description

The tabular table form reports columns. FORMAT JSON returns the versioned table
descriptor envelope; DATABASE is available only with FORMAT JSON.

## Parameters

`table_name` names a relation. TABLE is optional. FORMAT JSON selects one JSON
value instead of the legacy tabular shape.

## Result

The tabular result contains `Field`, `Type`, `Null`, `Key`, `Default` and
`Extra`. JSON mode returns one descriptor column and one row.

## Transaction behavior

DESCRIBE reads transaction-visible catalog metadata and makes no changes.

## Errors and limitations

A missing object fails. `DESCRIBE DATABASE` without FORMAT JSON is rejected.
Descriptor JSON is versioned and should be parsed by field, not by formatting.

## Privileges

Table metadata requires CONNECT, schema USAGE and SELECT on the relation.
Database-wide description is bootstrap-only.

## Example

```sql
CREATE TABLE ref_describe (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL DEFAULT 'untitled'
);
DESCRIBE ref_describe;
```

## See also

See [Defining a Schema](../../../sql/ddl/), [SHOW](../show/) and
[Protocol Internals](../../../internals/protocol/).
