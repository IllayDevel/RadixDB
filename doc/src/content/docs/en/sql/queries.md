---
title: Querying Data
description: Read, combine, group and rank rows with SELECT.
---

`SELECT` builds a result from table rows. A query can restrict rows, combine
relations, form groups, calculate window values and impose a final order.
This chapter presents those operations as one path from a small schema to
composed analytical queries.

Run the blocks in order in a separate test database. The examples use their
own three tables.

```sql
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    department_id INTEGER REFERENCES departments(id),
    name TEXT NOT NULL,
    salary INTEGER NOT NULL
);
CREATE TABLE bonuses (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id),
    amount INTEGER NOT NULL
);
INSERT INTO departments (id, name) VALUES
    (1, 'Engineering'), (2, 'Support'), (3, 'Sales');
INSERT INTO employees (id, department_id, name, salary) VALUES
    (1, 1, 'Alice', 120), (2, 1, 'Boris', 90),
    (3, 2, 'Clara', 80), (4, NULL, 'Dan', 70);
INSERT INTO bonuses (id, employee_id, amount) VALUES
    (1, 1, 10), (2, 1, 15), (3, 3, 5);
```

## Projection, Filtering and Order

The select list defines the result columns. `WHERE` removes rows before the
select list is returned. `ORDER BY` sorts the result, while `LIMIT` and
`OFFSET` select a window from that order.

```sql
SELECT id, name, salary
FROM employees
WHERE salary >= 80
ORDER BY salary DESC, id
LIMIT 2 OFFSET 1;
```

The result is Boris and Clara. The `id` tie-breaker makes the order stable
when salaries are equal. Without `ORDER BY`, row order is unspecified and
pagination with `LIMIT` or `OFFSET` is not repeatable. `ASC` is the default;
`DESC` reverses the direction. Use `NULLS FIRST` or `NULLS LAST` when the
placement of NULL values matters.

Column aliases belong to the output and can be used by the final `ORDER BY`.
Use qualified names such as `e.id` whenever more than one input exposes the
same column name.

## Joining Relations

An inner join keeps matching pairs. An outer join also preserves rows from
one or both sides and fills the missing side with NULL. RadixDB supports
`INNER`, `LEFT`, `RIGHT`, `FULL` and `CROSS JOIN`.

```sql
SELECT e.name AS employee, d.name AS department
FROM employees AS e
INNER JOIN departments AS d ON d.id = e.department_id
ORDER BY e.id;

SELECT e.name AS employee, d.name AS department
FROM employees AS e
LEFT JOIN departments AS d ON d.id = e.department_id
ORDER BY e.id;

SELECT d.id AS department_id, d.name AS department, e.name AS employee
FROM employees AS e
RIGHT JOIN departments AS d ON d.id = e.department_id
ORDER BY department_id, employee;

SELECT e.id AS employee_id, e.name AS employee,
       d.id AS department_id, d.name AS department
FROM employees AS e
FULL JOIN departments AS d ON d.id = e.department_id
ORDER BY employee_id NULLS LAST, department_id NULLS LAST;

SELECT e.name, d.name AS department
FROM employees AS e
CROSS JOIN departments AS d
WHERE e.id = 1
ORDER BY department;
```

The left join includes Dan with a NULL department. The right join includes
Sales with a NULL employee, and the full join includes both unmatched rows.
A cross join first forms every pair; its `WHERE` clause then retains Alice's
three pairs. Estimate that multiplication before using a cross join on large
inputs.

`JOIN ... USING (column)` is available when both inputs deliberately expose
the same key name. `NATURAL JOIN` compares every same-named column, including
columns added later, so explicit `ON` or `USING` is safer for durable schemas.

## Groups and Aggregates

Aggregate functions reduce rows in each group. `WHERE` filters input rows;
`HAVING` filters completed groups.

```sql
SELECT d.name AS department,
       COUNT(e.id) AS headcount,
       SUM(e.salary) AS payroll,
       AVG(e.salary) AS average_salary
FROM departments AS d
LEFT JOIN employees AS e ON e.department_id = d.id
GROUP BY d.id, d.name
HAVING COUNT(e.id) > 0
ORDER BY payroll DESC, department;
```

This returns Engineering with headcount 2 and payroll 210, then Support with
headcount 1 and payroll 80. `COUNT(e.id)` ignores the NULL introduced for an
unmatched department. In a grouped query, every selected expression must be
an aggregate or be covered by the grouping keys.

## Subqueries

A scalar subquery supplies one value, `IN` compares with a subquery result,
and `EXISTS` tests whether at least one correlated row is present.

```sql
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC;

SELECT name
FROM employees
WHERE id IN (SELECT employee_id FROM bonuses)
ORDER BY name;

SELECT e.name
FROM employees AS e
WHERE EXISTS (
    SELECT 1 FROM bonuses AS b
    WHERE b.employee_id = e.id AND b.amount >= 10
)
ORDER BY name;
```

The three results are Alice; Alice and Clara; then Alice. A scalar subquery
must return no more than one row. `EXISTS` is normally preferable when only
presence matters.

A subquery in `FROM` acts as a derived relation and must have an alias. This
example also gives both inputs the same key name so `USING` can coalesce it.

```sql
SELECT e.name AS employee, d.name AS department
FROM employees AS e
JOIN (
    SELECT id AS department_id, name FROM departments
) AS d USING (department_id)
ORDER BY e.id;
```

## Common Table Expressions

A common table expression names a query for the statement that follows.
Use it to expose an intermediate result when that improves the structure of
the query; it is not a persistent table.

```sql
WITH department_totals AS (
    SELECT department_id, SUM(salary) AS payroll
    FROM employees
    WHERE department_id IS NOT NULL
    GROUP BY department_id
)
SELECT d.name, t.payroll
FROM department_totals AS t
JOIN departments AS d ON d.id = t.department_id
ORDER BY t.payroll DESC;
```

Recursive CTEs require an anchor, `UNION ALL`, a recursive member and a
termination condition. Declare output columns when the recursive member must
refer to their names.

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 4
)
SELECT n FROM numbers ORDER BY n;
```

The result contains 1 through 4. A recursive member that never becomes empty
is an error-prone query even though the engine has a defensive iteration
limit; write and test the termination predicate explicitly.

## Window Functions

A window function calculates across related rows without collapsing them into
one group. `PARTITION BY` forms independent windows and the window's
`ORDER BY` defines the order inside each partition.

```sql
WITH ranked_employees AS (
    SELECT name, department_id, salary,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(department_id, -1)
               ORDER BY salary DESC, id
           ) AS position,
           SUM(salary) OVER (
               PARTITION BY COALESCE(department_id, -1)
           ) AS department_payroll
    FROM employees
)
SELECT name, department_id, salary, position, department_payroll
FROM ranked_employees
WHERE position <= 2
ORDER BY department_id NULLS LAST, position;
```

`COALESCE` gives employees without a department their own partition.
`ROW_NUMBER` assigns a position inside each partition; windowed `SUM` repeats
its payroll on each employee row. RadixDB also registers ranking and navigation
functions such as `RANK`, `DENSE_RANK`, `NTILE`, `LEAD`, `LAG`, `FIRST_VALUE`
and `LAST_VALUE`. Their result depends on the complete window definition,
especially its ordering and frame.

## Current Boundaries

The following limits are part of the checked 1.2 query surface.

The indexed partition fast path represents NULL as a complete partition. Its
result is identical on hot memory, cold artifacts and crash/reopen paths,
including explicit and automatically created foreign-key indexes. An expression
such as `PARTITION BY COALESCE(department_id, -1)` remains useful when the
application deliberately wants a non-NULL sentinel rather than SQL NULL
partition semantics.

Recursive CTEs accept `UNION ALL`, not duplicate-eliminating `UNION`:

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION
    SELECT n + 1 FROM numbers WHERE n < 4
)
SELECT n FROM numbers;
```

`LATERAL` derived tables are not accepted. Use a correlated scalar subquery or
`EXISTS`, or rewrite the relation as an ordinary join:

```sql
SELECT e.name, x.amount
FROM employees AS e
CROSS JOIN LATERAL (
    SELECT amount FROM bonuses WHERE employee_id = e.id
) AS x;
```

`QUALIFY` is not accepted. Put the window query in a CTE or derived table and
filter its published window column in the outer `WHERE`, as shown above:

```sql
SELECT name,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS position
FROM employees
QUALIFY position <= 2;
```

After grouping, the final sorter can use grouped expressions that are present
in the result or their output aliases. A qualified input-only expression such
as `d.id` below is not available after the aggregate result is formed:

```sql
SELECT d.name AS department, COUNT(e.id) AS headcount
FROM departments AS d
LEFT JOIN employees AS e ON e.department_id = d.id
GROUP BY d.id, d.name
ORDER BY d.id;
```

Project that key, or sort by `department` or another published expression.
These rejected examples are intentional compatibility checks, not commands to
put into a migration.
