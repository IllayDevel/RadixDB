---
title: Native extension commands
description: Bind trusted packages and create their SQL types, functions, operators and planner support.
---

These commands bind a database catalog to a package already admitted by the
server. SQL never loads a shared library from a path or downloads code.

## Synopsis

```text
CREATE EXTENSION [IF NOT EXISTS] extension_name VERSION 'exact_version';
DROP EXTENSION [IF EXISTS] extension_name RESTRICT;

CREATE TYPE schema.type_name
FROM EXTENSION extension_name AS 'local_id';
DROP TYPE [IF EXISTS] schema.type_name RESTRICT;

CREATE FUNCTION schema.function_name (
    [argument_name data_type [NULL | NOT NULL] [, ...]]
)
RETURNS data_type [NULL | NOT NULL]
LANGUAGE NATIVE
FROM EXTENSION extension_name AS 'local_id';
DROP FUNCTION schema.function_name (data_type [, ...]) RESTRICT;

CREATE OPERATOR schema.operator_symbol (
    [LEFTARG = data_type,]
    RIGHTARG = data_type,
    FUNCTION = schema.function_name(data_type [, ...])
)
FROM EXTENSION extension_name AS 'local_id';
DROP OPERATOR [IF EXISTS] schema.operator_symbol (
    [data_type], [data_type]
) RESTRICT;

CREATE OPERATOR CLASS schema.class_name
FOR TYPE data_type USING { BTREE | HASH | BITMAP | HNSW }
FROM EXTENSION extension_name AS 'local_id';
DROP OPERATOR CLASS [IF EXISTS] schema.class_name
USING { BTREE | HASH | BITMAP | HNSW } RESTRICT;

CREATE PLANNER SUPPORT schema.support_name
FOR FUNCTION schema.function_name(data_type [, ...])
FROM EXTENSION extension_name AS 'local_id';
DROP PLANNER SUPPORT [IF EXISTS] schema.support_name RESTRICT;
```

## Description

`CREATE EXTENSION` records an exact package identity in the database. The
package name and version must match one active startup-registry entry. Its UUID,
ABI range and descriptor fingerprint are stored with the binding.

The remaining `CREATE` forms publish selected package descriptors as ordinary
schema objects. `local_id` is the stable export identifier declared by the
extension author; it is case-sensitive, contains 1 to 255 UTF-8 bytes and must
not contain NUL. A package export can be bound only once. SQL names can differ
from descriptor names, but renaming a SQL object does not change its stable
object identity.

All declarations are checked against the package descriptor. SQL cannot
override a type codec, function volatility or strictness, operator signature,
operator-class strategies, key codec, or planner-support behavior.

## Extension binding

Only an exact canonical SemVer without build metadata is accepted. A version
range, package path, URL, checksum override, `CASCADE`, `FORCE` and
`IGNORE MISSING` are not part of the 1.2 grammar. `IF NOT EXISTS` succeeds only
when the existing binding has the same package UUID, version and fingerprint.

The registry selects one active version for each package UUID at startup. A
database remains pinned to its recorded version and fingerprint. There is no
`ALTER EXTENSION UPDATE` in 1.2.

## External types

The named descriptor must be an external type. Its stable object ID, codec
revision, semantic revision, storage shape, payload bound and comparison
callbacks come from the package. Values keep this identity in catalog 6.2 and
protocol 17; they are not interchangeable with `BYTES`.

The 1.2 Rust SDK does not publish generic SQL text input/output callbacks.
Construct external values through native functions or a plugin-aware protocol
adapter instead of an untyped SQL literal.

## Native functions

Argument and result types, nullability, strictness, volatility,
parallel-safety, cost, cancellation and batch capability must match the
descriptor exactly. Native aggregate, window and table-valued functions are
not accepted in 1.2. `LANGUAGE RADIX` routines are a separate facility.

## Operators and operator classes

An operator refers to an already bound native function. Prefix unary and
binary SQL forms are supported; postfix-only operators are not. The closed
operator alphabet is:

```text
= <> != < <= > >= + - * / % || & | ^ ~ << >> <=> && @> <@
```

An operator class connects declared operators and a canonical key encoder to a
core-owned `BTREE`, `HASH`, `BITMAP` or `HNSW` access method. The descriptor
defines its strategy slots. B-tree classes require `<`, `<=`, `=`, `>=` and
`>`; hash and bitmap classes require `=`. The Rust 1.2 authoring SDK rejects
external HNSW classes even though the SQL grammar reserves the method.

Use an external operator class in an index definition as follows:

```sql
CREATE INDEX asset_point_idx
ON assets (position geo.point_btree) USING BTREE;
```

## Planner support

Planner support is attached to an already bound native function. It can emit
bounded candidate ranges for a declared operator class. The core planner owns
the scan and always applies residual filtering when the support descriptor
requires recheck. An extension cannot provide an executor node, access storage
directly or replace cost-based planning.

## Ordering and transactions

Referenced objects must exist in the current transaction view. Create objects
in dependency order and remove them in reverse order. A typical installation is
atomic:

```sql
BEGIN;
CREATE EXTENSION radix_spatial VERSION '1.0.0';
CREATE TYPE geo.point FROM EXTENSION radix_spatial AS 'point';
CREATE FUNCTION geo.st_distance(left_point geo.point NOT NULL,
                                right_point geo.point NOT NULL)
RETURNS FLOAT NOT NULL
LANGUAGE NATIVE
FROM EXTENSION radix_spatial AS 'distance';
COMMIT;
```

Forward references fail, and any failed statement rolls back the catalog
generation. `DROP EXTENSION ... RESTRICT` succeeds only after every dependent
type, function, operator, operator class and planner-support object has been
removed explicitly.

## Privileges

The database owner or `root` can create and drop an extension binding.
Dependent objects require ownership of that binding plus `CREATE` on the target
schema; `root` can perform the operation administratively. Runtime function and
operator calls require `EXECUTE` on the backing function. Index creation also
uses the ordinary table and schema visibility checks.

Authorization completes before native code runs. A plugin receives no
principal, ACL bypass or catalog-mutation handle.

## Failure after restart

If the exact package or codec admission is unavailable, the database opens in
restricted diagnostic mode. Restore the matching package and restart the
server, or let `root` remove the binding with `DROP EXTENSION ... RESTRICT`
after removing its dependents. DDL cannot bypass this check.

## See also

See [Installing and operating extensions](../../../administration/extensions/),
[Developing native extensions](../../../programming/native-extensions/),
[CREATE INDEX](../create-index/) and
[`cargo radixdb-plugin`](../../programs/cargo-radixdb-plugin/).
