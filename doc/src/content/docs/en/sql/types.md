---
title: Data Types
description: Scalar types, aliases and type modifiers supported by RadixDB.
---

A column's type defines how its values are represented and which conversions
are required when writing data. SQL spellings are not always distinct types:
several names below map to the same internal representation.

## Supported Types

| Type | Accepted aliases | Representation |
| --- | --- | --- |
| INTEGER | INT, BIGINT, SMALLINT, TINYINT | Signed 64-bit integer |
| FLOAT | DOUBLE, REAL | 64-bit floating point |
| DECIMAL | NUMERIC | Exact decimal with precision and scale |
| TEXT | VARCHAR, CHAR, STRING, CLOB | UTF-8 text |
| BOOLEAN | BOOL | True or false |
| TIMESTAMP | DATETIME, TIME | Timestamp value, not a separate time-of-day type |
| DATE | | Calendar date |
| UUID | | 16-byte identifier |
| BYTES | BLOB, BINARY, VARBINARY | Raw bytes |
| JSON | JSONB | JSON value |
| VECTOR | | Vector value |

NULL denotes the absence of a value. A nullable column can contain NULL
regardless of its declared type. NULL is not a substitute for a column's
application type.

## External types

A trusted native extension can add a catalog-bound scalar type with
`CREATE TYPE ... FROM EXTENSION`. The package descriptor defines a stable
object ID, fixed or variable storage shape, canonical codec revision, maximum
payload and optional equality, hash and ordering semantics. The SQL name is a
schema alias for that identity and can be renamed without changing stored
bytes.

External values travel through protocol 17 with their type object ID and codec
revision. They do not fall back to `BYTES`, and the generic ORM does not decode
them without a plugin-aware adapter. The Rust SDK 1.2 also has no generic SQL
literal input/output callback, so value construction normally uses a native
function or typed client adapter. See
[Native extension commands](../../reference/sql/extensions/) and
[Developing native extensions](../../programming/native-extensions/).

## Integers, Text and Booleans

`SMALLINT` and `TINYINT` do not impose 16-bit or 8-bit bounds. They are aliases
of INTEGER. Use a constraint when an application requires a narrower range.

```sql
CREATE TABLE typed_values (id SMALLINT, label TEXT, active BOOL);
INSERT INTO typed_values VALUES (100000, 'ready', TRUE);
SELECT id, label, active FROM typed_values;
```

The row contains 100000, `ready` and true. INTEGER ranges from
-9223372036854775808 to 9223372036854775807. FLOAT is approximate and should
not be confused with exact decimal representation.

Use `TEXT` or a bare text alias such as `VARCHAR`. Length modifiers such as
`VARCHAR(255)` and `CHAR(10)` are rejected in this version. They are not accepted
as silently ignored length constraints. The same rule rejects type modifiers
for types other than DECIMAL/NUMERIC and VECTOR.

## Decimals

`DECIMAL(p,s)` specifies total precision and scale. Precision is between 1 and
38, and scale must not exceed precision. `DECIMAL(p)` uses scale zero.
Bare DECIMAL is accepted without a declared precision/scale constraint.

```sql
CREATE TABLE prices (amount DECIMAL(6,2));
INSERT INTO prices VALUES (12.34);
SELECT CAST(amount AS TEXT) AS amount FROM prices;
```

The result is `12.34`. Exact storage is not a promise that every mixed-type
arithmetic expression follows another database's rounding rules. Use typed
client parameters for exact values, and check the conversion and arithmetic
contract of the operation you need.

## Dates, Identifiers and Bytes

DATE represents a calendar day; TIMESTAMP, DATETIME and TIME share timestamp
storage. Do not treat the TIME alias as a separate SQL time-only type.
UUID has a typed representation rather than being an arbitrary text column.
BYTES stores binary values without requiring valid UTF-8.

```sql
SELECT CAST('2026-09-08' AS DATE) AS day,
       CAST('01940000-0020-7000-8000-000000000001' AS UUID) AS id,
       FROM_HEX('00ff7f') AS bytes;
```

The returned values are a date, the specified UUID and three bytes `00 ff 7f`.
Their display depends on the client. In CLI JSON output, DATE includes
`days_since_unix_epoch`, and BYTES includes `raw_hex`; not every typed cell
uses a scalar `value` field. Malformed hexadecimal input is rejected.

## JSON and Vectors

JSONB is an alias of JSON; the spelling does not imply PostgreSQL JSONB
operators or its on-disk format. Likewise, declaring VECTOR does not establish
the semantics of every vector function or index.

`VECTOR(n)` records a dimension between 1 and 65535. Dimensions outside this
range are rejected. Index and function contracts are separate from type-name
recognition and must be checked for the operations used by an application.

## Conversion and Constraints

Use `CAST(expression AS type)` for an explicit conversion. Assigning a value
to a typed column may also require conversion. An invalid conversion should
not be treated as equivalent to a missing value; handle the error returned
by the operation. `NOT NULL`, keys and other constraints are separate from
type selection.

Continue with [expressions](../expressions/) for arithmetic, comparisons and NULL.
