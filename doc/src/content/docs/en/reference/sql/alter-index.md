---
title: ALTER INDEX
description: Rename an index without changing its definition.
---

`ALTER INDEX` renames an existing index.

## Synopsis

```text
ALTER INDEX index_name RENAME TO new_index_name
```

## Description

Version 1.2 supports only RENAME TO. The index method, columns, predicate and
uniqueness remain unchanged.

## Parameters

`index_name` is the current unqualified name. `new_index_name` must be available
in the catalog namespace.

## Result

Success returns a command result with no rows. SHOW INDEXES exposes the new
name.

## Transaction behavior

The rename is transactional. ROLLBACK restores the old catalog name.

## Errors and limitations

A missing source index or conflicting destination name fails. Rebuilding,
changing method and changing indexed columns are not ALTER INDEX forms in 1.2.

## Privileges

The effective Principal must own the index; the session also needs CONNECT.

## Example

```sql
CREATE TABLE ref_alter_index (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_code_idx ON ref_alter_index (code) USING BTREE;
ALTER INDEX ref_code_idx RENAME TO ref_code_lookup_idx;
SHOW INDEXES FROM ref_alter_index;
```

## See also

See [Indexes](../../../sql/indexes/), [CREATE INDEX](../create-index/) and
[DROP INDEX](../drop-index/).
