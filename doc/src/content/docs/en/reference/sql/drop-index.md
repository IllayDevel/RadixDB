---
title: DROP INDEX
description: Remove a named index from a named table.
---

`DROP INDEX` removes an index definition and its rebuildable storage.

## Synopsis

```text
DROP INDEX [IF EXISTS] index_name ON table_name
```

## Description

The 1.2 execution contract requires the ON table clause even though the parser
can represent an omitted table name. IF EXISTS suppresses a missing-index error.

## Parameters

`index_name` identifies the index and `table_name` fixes its owning table.

## Result

Success returns a command result with no rows. SHOW INDEXES confirms the
remaining definitions.

## Transaction behavior

Index removal is transactional. ROLLBACK keeps the prior index visible.

## Errors and limitations

Omitting `ON table_name` is rejected in 1.2. A mismatched table, missing index
without IF EXISTS, or attempt to remove an implicit primary-key structure fails.

## Privileges

The effective Principal must own the index; the session also needs CONNECT.

## Example

```sql
CREATE TABLE ref_drop_index (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_drop_code_idx ON ref_drop_index (code) USING BTREE;
DROP INDEX ref_drop_code_idx ON ref_drop_index;
SHOW INDEXES FROM ref_drop_index;
```

## See also

See [Indexes](../../../sql/indexes/), [CREATE INDEX](../create-index/) and
[ALTER INDEX](../alter-index/).
