---
title: Data Types
description: Scalar types, aliases and type modifiers supported by RadixDB.
---

A column's type defines how its values are represented and which conversions
are required when writing data. Some SQL spellings are aliases, while other
types share a physical codec but retain distinct SQL and catalog identities.

## Supported Types

| Type | Accepted aliases | Representation |
| --- | --- | --- |
| INTEGER | INT, BIGINT, SMALLINT, TINYINT | Signed 64-bit integer |
| FLOAT | REAL | 64-bit floating point |
| DOUBLE PRECISION | DOUBLE | 64-bit floating point with a distinct SQL and catalog identity |
| DECIMAL | NUMERIC | Exact decimal with precision and scale |
| TEXT | VARCHAR, CHAR, STRING, CLOB | Variable-length UTF-8 text, optionally bounded by characters |
| BOOLEAN | BOOL | True or false |
| TIMESTAMPTZ | TIMESTAMP WITH TIME ZONE | Absolute instant normalized to UTC nanoseconds |
| TIMESTAMP | TIMESTAMP WITHOUT TIME ZONE, DATETIME | Civil date and time without a time zone |
| TIME | TIME WITHOUT TIME ZONE | Civil time of day without a date or time zone |
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

External values travel through protocol 18 with their type object ID and codec
revision. They do not fall back to `BYTES`, and the generic ORM does not decode
them without a plugin-aware adapter. The Rust SDK 1.2.19 also has no generic SQL
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
-9223372036854775808 to 9223372036854775807. FLOAT and DOUBLE PRECISION are
approximate and should not be confused with exact decimal representation.
DOUBLE and DOUBLE PRECISION preserve the DOUBLE PRECISION SQL and catalog
identity; FLOAT and REAL preserve FLOAT identity. Both use an f64 physical
representation.

Bare `TEXT`, `VARCHAR`, `CHAR`, `STRING` and `CLOB` are unbounded text aliases.
`TEXT(n)`, `VARCHAR(n)` and `CHAR(n)` enforce a positive maximum of `n` Unicode
scalar values. The limit counts characters rather than UTF-8 bytes, does not
change variable-length storage and does not add blank padding for `CHAR(n)`.
Schema output canonicalizes every bounded spelling as `TEXT(n)`.

```sql
CREATE TABLE labels (
    id INTEGER PRIMARY KEY,
    code TEXT(2) NOT NULL,
    title VARCHAR(5) NOT NULL,
    marker CHAR(1)
);
INSERT INTO labels VALUES (1, 'AB', 'ready', 'x');
SELECT code, title, marker FROM labels;
```

Zero, malformed or overflowing limits are rejected. A default, INSERT, UPDATE
or other write whose value exceeds the declared character limit also fails.
Type modifiers remain unsupported for scalar types other than TEXT aliases,
DECIMAL/NUMERIC and VECTOR.

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

## Temporal Types

Use `TIMESTAMPTZ` for an event that happened at one absolute instant. An input
offset is applied and the value is normalized to UTC. Use `TIMESTAMP` for a
civil date and time whose meaning does not include a zone, such as the local
opening time printed on a timetable. `DATETIME` is an alias of this civil type.
Use `TIME` for a time of day without a date. A civil TIMESTAMP rejects an input
that contains a UTC offset instead of silently discarding it.

```sql
SELECT CAST(TIMESTAMPTZ '2026-09-11T10:20:30.123456789+07:00' AS TEXT) AS instant,
       TYPEOF(TIMESTAMPTZ '2026-09-11T10:20:30.123456789+07:00') AS instant_type,
       CAST(TIMESTAMP '2026-09-11 10:20:30.123456789' AS TEXT) AS civil,
       TYPEOF(TIMESTAMP '2026-09-11 10:20:30.123456789') AS civil_type,
       CAST(TIME '23:59:59.999999999' AS TEXT) AS clock,
       TYPEOF(TIME '23:59:59.999999999') AS clock_type,
       TYPEOF(CURRENT_TIMESTAMP) AS current_type;
```

This returns the normalized instant
`2026-09-11T03:20:30.123456789+00:00`, the unchanged civil value and the time
of day, with types `TIMESTAMPTZ`, `TIMESTAMP` and `TIME` respectively. The final
column confirms that CURRENT_TIMESTAMP is TIMESTAMPTZ.

`CIVIL_TO_TIMESTAMPTZ(value, zone)` converts a civil value using an explicit
IANA zone or fixed offset. `TIMESTAMPTZ_TO_CIVIL(value, zone)` performs the
reverse conversion. Ambiguous or nonexistent local times at daylight-saving
transitions are rejected. TCP sessions start in UTC; `SET TIME ZONE` and
`SHOW TIME ZONE` manage connection-local parsing and rendering when a
TIMESTAMPTZ value has no explicit offset. `CURRENT_TIMESTAMP` returns
TIMESTAMPTZ.

## Dates, Identifiers and Bytes

DATE represents a calendar day. UUID has a typed representation rather than
being an arbitrary text column. BYTES stores binary values without requiring
valid UTF-8.

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
