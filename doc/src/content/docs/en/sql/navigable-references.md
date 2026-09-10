---
title: Navigable References
description: Follow declared foreign keys in read-only SQL expressions without repeating JOIN conditions.
---

Navigable references are a RadixDB query notation for moving from a row that
stores a foreign key to one referenced row. The engine derives every step from
the schema and plans the lookups as part of one statement. It does not store an
object in the source column and does not issue one client query per row.

## Example schema

The examples use two successive relationships: an employee refers to a
department, and a department may refer to a profile.

```sql
CREATE TABLE profiles (
    id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL
);
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    cost_center TEXT NOT NULL,
    profile_id INTEGER REFERENCES profiles(id)
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    salary INTEGER NOT NULL,
    department_id INTEGER REFERENCES departments(id)
);

INSERT INTO profiles VALUES
    (1, 'Finance profile'),
    (2, 'Engineering profile');
INSERT INTO departments VALUES
    (10, 'Finance', 'FIN', 1),
    (20, 'Engineering', 'ENG', 2),
    (30, 'Unclassified', 'UNC', NULL);
INSERT INTO employees VALUES
    (100, 'Alice', 120, 10),
    (101, 'Bob', 90, 10),
    (102, 'Carol', 110, 20),
    (103, 'Dave', 70, NULL),
    (104, 'Eve', 80, 30);
```

`department_id` physically stores an ordinary integer. Selecting that column
still returns the key value 10, 20, 30 or NULL.

## One reference step

A complete path starts with a visible table alias, continues through an FK
column, and ends with a regular column of the referenced table.

```sql
SELECT e.id, e.name,
       e.department_id.label AS department,
       e.department_id.cost_center AS code
FROM employees AS e
ORDER BY e.id;
```

The first two rows return Finance and FIN, Carol returns Engineering and ENG,
Dave returns two NULL values, and Eve returns Unclassified and UNC. The result
is observably equivalent to this explicit LEFT JOIN:

```sql
SELECT e.id, e.name,
       d.label AS department,
       d.cost_center AS code
FROM employees AS e
LEFT JOIN departments AS d ON e.department_id = d.id
ORDER BY e.id;
```

Navigation is a shorthand for a declared relationship, not a replacement for
general joins. Use `JOIN` when the condition is not an FK equality or when one
target must produce a collection of source rows.

## Roots, aliases and shared prefixes

The alias-qualified spelling is the safest form in a query with several inputs.
When only one visible relation has a matching navigable FK, the table alias may
be omitted. Several terminals on the same reference prefix share the planned
edge and target lookup.

```sql
SELECT id,
       department_id.label AS department,
       department_id.cost_center AS code
FROM employees
ORDER BY id;
```

If several relation instances offer `department_id`, the shorthand is
ambiguous. RadixDB does not choose the first match; qualify the path with the
intended alias.

## Transitive paths and NULL

Every intermediate component must itself be a foreign-key column. The final
component is the value to return. Shared prefixes are planned once even when
several paths end in different fields.

```sql
SELECT e.id, e.name,
       e.department_id.label AS department,
       e.department_id.profile_id.display_name AS profile
FROM employees AS e
ORDER BY e.id;
```

Every step has LEFT semantics. A NULL `department_id` makes the remaining path
NULL for Dave. Eve reaches a department, but its NULL `profile_id` makes only
the profile NULL. A nullable terminal column also remains an ordinary typed
NULL. Navigation never removes the source row by itself.

A path may also be used in a predicate. Normal three-valued SQL logic still
applies; use `IS NULL` when missing relationships must be selected.

```sql
SELECT e.id, e.name
FROM employees AS e
WHERE e.department_id.profile_id.display_name = 'Finance profile'
   OR e.department_id.profile_id.display_name IS NULL
ORDER BY e.id;
```

This returns Alice, Bob, Dave and Eve. Carol's profile exists but does not match.

## Grouping and other SELECT contexts

Navigation is available in read-only SELECT expressions: projection, `WHERE`,
`JOIN ... ON`, `GROUP BY`, `HAVING`, `ORDER BY`, aggregate and window arguments,
`CASE`, scalar functions, CTEs, derived tables and read-only subqueries. All
occurrences are bound against the lexical query scope.

```sql
SELECT e.department_id.label AS department,
       COUNT(*) AS headcount,
       SUM(e.salary) AS payroll
FROM employees AS e
GROUP BY e.department_id.label
HAVING SUM(e.salary) >= 80
ORDER BY department NULLS LAST;
```

The result contains Engineering with 1 and 110, Finance with 2 and 210, and
Unclassified with 1 and 80. Repeating the path in projection and grouping does
not create independent client lookups. `SELECT *` does not expand referenced
columns; every terminal must be named explicitly.

## EXPLAIN

Use `EXPLAIN ANALYZE` to see the bound reference graph and the physical strategy
chosen for the actual data shape.

```sql
EXPLAIN ANALYZE
SELECT e.department_id.label AS department, COUNT(*)
FROM employees AS e
GROUP BY e.department_id.label;
```

The plan includes a `Reference Navigation` section, `Semantics: LEFT`, planned
and executed path counters, lookup batches, an integrity check and the actual
strategy. The strategy may differ with cardinality, storage state and query
context. Explain output reports aggregate counters but does not print lookup key
values or SQL parameter values.

## Name and schema errors

Navigation binding is deterministic and fails with stable diagnostic categories.
The following shorthand has two possible roots:

```sql
SELECT department_id.label
FROM employees AS e1
JOIN employees AS e2 ON e1.id = e2.id;
```

It returns `NAVIGATION_AMBIGUOUS_ROOT`. A path through a regular column is not
interpreted as an object property:

```sql
SELECT e.name.value FROM employees AS e;
```

It returns `NAVIGATION_NOT_A_REFERENCE`. A missing terminal is also rejected at
binding time:

```sql
SELECT e.department_id.missing FROM employees AS e;
```

It returns `NAVIGATION_TARGET_COLUMN_NOT_FOUND`. If a prepared statement's
bound schema objects are changed, execution rebinds or fails closed; it does not
follow a stale descriptor.

## Read-only boundary

A navigable path is never an assignment target and is rejected from every write
expression context, including filters, `RETURNING`, `ON CONFLICT`, write
subqueries and `CREATE TABLE AS SELECT`.

```sql
UPDATE employees
SET name = 'blocked'
WHERE department_id.label = 'Finance';
```

The error is `NAVIGATION_READ_ONLY`, and no row is modified. Name the target
table and write scope explicitly instead:

```sql
UPDATE departments
SET label = 'Finance and Legal'
WHERE id IN (
    SELECT department_id FROM employees WHERE id = 100
);
SELECT e.department_id.label
FROM employees AS e
WHERE e.id = 100;
```

The SELECT returns Finance and Legal. There is no reverse collection navigation,
implicit graph save, inferred ownership or implicit cascade.

Persisted view definitions containing navigation are not supported in 1.2:

```sql
CREATE VIEW employee_departments AS
SELECT e.id, e.department_id.label
FROM employees AS e;
```

This fails with `NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE` and does not create the
view. Ordinary views without navigable paths remain available.

## Schema requirements and limits

Each step requires one source FK column and one target column of a compatible
physical type, in the same database. The target must be a primary key or a
`UNIQUE NOT NULL` column. A nullable source FK is allowed. A nullable unique
target may still participate in an ordinary FK, but navigation through it is
rejected because uniqueness of NULL cannot prove one target row.

RadixDB 1.2 does not accept composite foreign-key DDL. Cross-database paths and
reverse one-to-many traversal are also unavailable. One path is limited to 8
reference steps; one statement is limited to 256 bound paths and a compiled
graph is limited to 512 distinct edges. Exceeding a limit fails with
`NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE`.

A non-NULL reference with no target, or more than one target, indicates a broken
integrity invariant. Execution fails closed with `REFERENCE_TARGET_MISSING` or
`REFERENCE_TARGET_NOT_UNIQUE` instead of returning an arbitrary row.
