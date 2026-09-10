---
title: SQL Command Reference
description: Syntax and execution contracts for SQL commands verified in RadixDB 1.2.
---

This reference describes the SQL command surface verified for RadixDB 1.2 at
revision `23bf35df011aae6816d77578be96074b02bc363c`. Each command page gives the
accepted syntax, result, transaction behavior, errors, privileges and an
executable example. The [coverage matrix](../../appendices/compatibility/)
remains the authoritative list of supported, limited and rejected contracts.

## Queries and data changes

- [SELECT](./select/) reads and combines rows.
- [INSERT](./insert/) adds rows and handles supported conflicts.
- [UPDATE](./update/) changes matching rows.
- [DELETE](./delete/) removes matching rows.

## Schema and metadata

- [CREATE TABLE](./create-table/), [ALTER TABLE](./alter-table/) and
  [DROP TABLE](./drop-table/) manage tables.
- [CREATE INDEX](./create-index/), [ALTER INDEX](./alter-index/) and
  [DROP INDEX](./drop-index/) manage secondary access structures.
- [DESCRIBE](./describe/) and [SHOW](./show/) inspect catalog metadata.

## Transaction control

- [BEGIN](./begin/), [COMMIT](./commit/) and [ROLLBACK](./rollback/) delimit
  explicit transactions.
- [SAVEPOINT](./savepoint/) and [RELEASE SAVEPOINT](./release-savepoint/)
  provide partial rollback points.
- [SET](./set/) changes the supported engine default.

Unquoted identifiers are case-insensitive. Brackets in a synopsis mark an
optional clause and braces mark alternatives; they are not literal SQL. Unless
an `ORDER BY` clause is present, row order is unspecified. The stock TCP server
uses a bootstrap root session, while non-bootstrap embedded hosts must enforce
the privileges stated on each page.
