---
title: SHOW
description: List relations and inspect stored table, view or index definitions.
---

`SHOW` returns selected catalog metadata.

## Synopsis

```text
SHOW TABLES
SHOW VIEWS
SHOW CREATE { TABLE table_name | VIEW view_name }
SHOW { INDEX | INDEXES } FROM table_name
```

## Description

SHOW TABLES and SHOW VIEWS list catalog names. SHOW CREATE reconstructs the
stored relation definition. SHOW INDEXES reports index name, columns, method,
uniqueness and options for one table.

## Parameters

`table_name` or `view_name` selects the metadata target. INDEX and INDEXES are
equivalent spellings in the FROM form.

## Result

Every form returns a row set. Listing order is an implementation detail unless
the returned rows are consumed by a later explicitly ordered query.

## Transaction behavior

SHOW reads transaction-visible catalog metadata and makes no changes.

## Errors and limitations

A named missing relation fails. SHOW is not a general information-schema query
and does not list effective grants.

## Privileges

SHOW TABLES and SHOW VIEWS are bootstrap-only. Named metadata forms require
CONNECT, schema USAGE and SELECT on the target relation.

## Example

```sql
CREATE TABLE ref_show (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_show_code_idx ON ref_show (code) USING BTREE;
SHOW TABLES;
SHOW CREATE TABLE ref_show;
SHOW INDEXES FROM ref_show;
```

## See also

See [DESCRIBE](../describe/), [Indexes](../../../sql/indexes/) and
[Access Control](../../../administration/access-control/).
