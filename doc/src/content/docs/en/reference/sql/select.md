---
title: SELECT
description: Read, join, group and order rows in RadixDB 1.2.
---

`SELECT` produces a row set from expressions, relations and supported derived
queries.

## Synopsis

```text
[WITH cte AS (query) [, ...]]
SELECT [DISTINCT] expression [[AS] alias] [, ...]
[FROM source [join_clause ...]]
[WHERE condition]
[GROUP BY expression [, ...]]
[HAVING condition]
[UNION ALL query]
[ORDER BY expression [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
[LIMIT count] [OFFSET count]
```

## Description

Sources may be tables, non-recursive CTEs, supported recursive CTEs, subqueries
or joins. INNER, LEFT, RIGHT, FULL and CROSS joins are available. Window
functions run after grouping and before final ordering and limiting.

## Parameters

`expression` determines a result column. `condition` keeps rows for which it is
true. GROUP BY forms groups; HAVING filters them. LIMIT and OFFSET must be
non-negative integer expressions. Navigable paths follow declared single-column
foreign keys in read-only SELECT contexts.

## Result

The command returns named columns and zero or more rows. Row order is stable
only when ORDER BY fully determines it. NULL ordering can be stated explicitly.

## Transaction behavior

SELECT reads the current statement snapshot under READ COMMITTED or the fixed
transaction snapshot under SNAPSHOT, plus the transaction's own writes.

## Errors and limitations

Recursive CTEs require `UNION ALL`; `LATERAL`, `QUALIFY` and hidden qualified
ORDER BY inputs after aggregation are rejected. NATURAL JOIN is supported but
schema changes can alter its condition. Navigable references have depth 8,
path 256 and edge 512 limits; reverse and composite paths are rejected.

## Privileges

A non-bootstrap session needs database CONNECT, schema USAGE and SELECT on each
source table. Column-only SELECT applies only to an unambiguous single-table
projection; joins and subqueries require table-level SELECT.

## Example

```sql
CREATE TABLE ref_select (id INTEGER PRIMARY KEY, score INTEGER NOT NULL);
INSERT INTO ref_select VALUES (1, 40), (2, 90), (3, 70);
SELECT id, score FROM ref_select WHERE score >= 70 ORDER BY score DESC;
```

## See also

See [Querying Data](../../../sql/queries/),
[Navigable References](../../../sql/navigable-references/) and the
[SQL coverage matrix](../../../appendices/compatibility/).
