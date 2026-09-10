---
title: Expressions
description: Arithmetic, comparisons, conditional values and NULL behavior.
---

An expression computes a value from literals, columns, parameters and function
calls. It can appear in a SELECT list, a predicate or a write expression.
The [syntax chapter](../syntax/) describes how expressions are written;
[data types](../types/) describes their representations.

## Arithmetic and Grouping

Multiplication, division and remainder bind more tightly than addition and
subtraction. Parentheses make grouping explicit.

```sql
SELECT 2 + 3 * 4 AS a, (2 + 3) * 4 AS b, 7 / 2 AS c, 7 % 2 AS d;
```

The results are 14, 20, 3 and 1. Division of these integer operands produces
an integer quotient; it does not automatically produce a fraction.

```sql
SELECT 7.0 / 2 AS fraction, 1 / 0 AS zero_result;
```

The results are 3.5 and NULL. The shown division by zero returns NULL on the
checked engine, not an exception. Do not infer identical error behavior for
every numeric function from this operator example.

Comparisons bind more tightly than AND, and AND more tightly than OR.
For clarity, parenthesize mixed logical conditions. Do not rely on a particular
left-to-right evaluation order for conditions to guard an unsafe operation;
an optimizer may choose another evaluation plan.

## Comparisons and NULL

Ordinary comparisons with NULL yield an unknown result. `IS NULL` tests for
NULL directly. Logical expressions use three-valued logic:

```sql
SELECT NULL = NULL AS a, NULL IS NULL AS b,
       TRUE AND NULL AS c, FALSE AND NULL AS d,
       TRUE OR NULL AS e, NOT NULL AS f;
```

The results are NULL, true, NULL, false, true and NULL. WHERE keeps rows for
which its predicate is true; an unknown predicate does not select a row.

```sql
SELECT 2 NOT IN (1, NULL) AS a, 2 BETWEEN 1 AND 3 AS b;
```

The first result is NULL, not true: the NULL member prevents proving that the
value differs from every member. BETWEEN includes its endpoints, so the second
result is true. Consider NULL explicitly when using NOT IN against nullable data.

## Conditional Values

COALESCE selects the first non-NULL value. NULLIF returns NULL when its two
arguments compare equal. A searched CASE selects a result according to WHEN
conditions; ELSE supplies the alternative.

```sql
SELECT COALESCE(NULL, 7) AS a, NULLIF(4, 4) AS b,
       CASE WHEN 2 > 1 THEN 10 ELSE 20 END AS c;
```

The results are 7, NULL and 10. Choose compatible result types deliberately;
these examples do not promise arbitrary coercion between unrelated types.

## Strings and Functions

Use `||` to concatenate strings. LIKE uses `%` for a sequence of characters
and `_` for a single character. A function call supplies arguments in parentheses.

```sql
SELECT 'ab' || 'cd' AS joined, 'Alpha' LIKE 'A%' AS matched,
       LOWER('HELLO') AS lowered;
```

The results are `abcd`, true and `hello`. A complete function reference must
also specify arity, types, NULL handling and errors; the examples here introduce
expression composition, not every available function.

## Explicit Conversion

CAST requests conversion to a named type. It differs from changing the display
format in a client.

```sql
SELECT CAST(42 AS TEXT) AS a, CAST(2.9 AS INTEGER) AS b,
       CAST(NULL AS INTEGER) AS c;
```

The results are text `42`, integer 2 and NULL. This FLOAT-to-INTEGER example
discards the fractional part; it is not rounding to the nearest integer.
Use explicit conversions when operand types would otherwise make the result
ambiguous, and handle invalid conversion errors in the calling application.
