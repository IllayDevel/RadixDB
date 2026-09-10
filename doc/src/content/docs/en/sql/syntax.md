---
title: SQL Syntax
description: Statements, identifiers, literals, comments and parameters in RadixDB SQL.
---

SQL describes operations on database objects and values. A statement consists
of tokens: keywords, names, literals, operators and punctuation. Whitespace
separates tokens and may include line breaks. The examples below can be run
in a local CLI memory database; they require no tables from the tutorial.

## Statements and Keywords

Terminate ordinary statements with a semicolon. SQL keywords are
case-insensitive; this manual writes them in uppercase to distinguish them
from application names. The following two queries return the same value:

```sql
SELECT 1 AS value;
select 1 as value;
```

A comma separates expressions in a select list. Parentheses group expressions
and contain argument lists. A dot qualifies a column with a table name or alias,
as in `employees.id`. Use explicit aliases when a query includes several tables
with similarly named columns.

Do not split arbitrary SQL text at every semicolon in application code.
Semicolons can occur inside strings and procedural bodies. The statement
grammar, not the character alone, determines the boundary.

## Names

An unquoted identifier begins with a letter or underscore. Subsequent
characters can include letters, digits, underscores and dollar signs.
For ordinary application names, use simple names such as `employees`,
`department_id` and `created_at` consistently.

Double quotes delimit an identifier that includes spaces or otherwise needs
quoting. A doubled double quote represents a quote inside that identifier.
Backticks are also accepted as identifier delimiters; this manual uses double
quotes. Do not assume that quoted identifiers have PostgreSQL's case semantics
or that quoting alone allows two objects differing only in letter case.

```sql
SELECT 7 AS "item count", 8 AS "a""b";
```

The result has columns named `item count` and `a"b`. Single quotes are for
text values, not for naming columns or tables. Keep these uses distinct even
where an expression context permits compatibility behavior.

## Literal Values

A string literal is enclosed in single quotes. Double a single quote to put
it inside the value. Keywords `TRUE`, `FALSE` and `NULL` are not strings.

```sql
SELECT 'O''Brien' AS name, '' AS empty_text, TRUE AS enabled, NULL AS missing;
```

An empty string is a value; it is not NULL. NULL represents a missing or unknown
value. Use `IS NULL` or `IS NOT NULL` to test it, rather than equality to NULL.

Numbers may contain a fractional part or an exponent. A leading sign is a
unary operator. Use a digit before and after the decimal point in examples.
The exponent must include digits: `1e` is not a valid number.

```sql
SELECT 42 AS whole, -7 AS negative, 1.25 AS fraction, 2e3 AS exponent;
```

The spelling of a literal does not by itself establish exact decimal arithmetic.
Type conversion and the destination column also matter. Bind typed values
through the client API when their exact representation is significant.

## Comments

`--` starts a comment that ends at the line break. No space after the two
hyphens is required. RadixDB also accepts `#` line comments. A block comment
starts with `/*` and ends with `*/`; block comments may be nested.

```sql
-- A line comment
SELECT /* outer /* inner */ outer */ 3 AS value;
```

Write `- -5` to express two separate minus operators. `--5` starts a comment,
not double negation. An unterminated string, quoted identifier or block comment
is an error. A NUL byte is not accepted as part of SQL input.

## Parameters

Use parameters for application values instead of concatenating user input
into SQL. RadixDB parses positional placeholders `?` and `$1`, `$2`, and named
placeholders such as `:employee_id`. Numbered positions start at `$1`; `$0`
is invalid. Do not mix `?` and `$n` positional styles in one parsed request.

Parameter tokens are not automatically bound values. The chosen client method
must supply the parameter collection in its supported format. A parameter
stands for a value, not an arbitrary table name, keyword or SQL fragment.
For example, the name in `FROM employees` cannot be supplied as a value parameter.
Choose dynamic object names from an application-controlled allowlist.

The [client interfaces overview](../../clients/overview/) explains the
difference between embedded and TCP access. The detailed interface contracts
define binding and error handling; recognizing a placeholder in the parser
does not imply that every client exposes the same binding methods.
