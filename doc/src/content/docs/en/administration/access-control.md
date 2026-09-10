---
title: Access Control
description: Manage principals, roles, ownership and object or column privileges in RadixDB 1.2.
---

RadixDB authorization is catalog-based and deny-by-default. Each non-bootstrap
statement requires a valid session Principal, database `CONNECT`, schema
`USAGE` and the privilege or ownership required by the target operation.

The stock TCP server binds login/password authentication to the same catalog
Principal IDs. Read [authentication](../authentication/) for ordinary TCP,
optional TLS and the loopback-only bootstrap recovery path.

## Principals and roles

A Principal is an execution identity. A Role groups privileges and can be
granted to a Principal or another Role. Both names share one normalized catalog
namespace.

```sql
CREATE PRINCIPAL alice;
CREATE ROLE report_reader;

GRANT CONNECT ON DATABASE application TO alice;
GRANT USAGE ON SCHEMA public TO alice;
GRANT SELECT (id, title) ON TABLE reports TO report_reader;
GRANT report_reader TO alice;
```

Membership is recursive and always active; there is no `SET ROLE`. Cycles are
rejected and authorization expands at most 65,536 subjects. `WITH ADMIN OPTION`
allows the member to grant that Role onward:

```sql
GRANT report_reader TO alice WITH ADMIN OPTION;
REVOKE ADMIN OPTION FOR report_reader FROM alice RESTRICT;
REVOKE report_reader FROM alice CASCADE;
```

Role grants retain grantor provenance. `RESTRICT` is the default and refuses to
remove an authority that still supports downstream memberships. Explicit
`CASCADE` removes only the dependent paths supported by that grant; independent
paths survive. `REVOKE ADMIN OPTION FOR` removes delegation authority without
removing the membership itself. Cycles are rejected.

## Object privileges

| Target | Privileges |
| --- | --- |
| `DATABASE name` | `CONNECT` |
| `SCHEMA name` | `USAGE`, `CREATE` |
| `TABLE name` | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| `FUNCTION name(types)` | `EXECUTE` |
| `PROCEDURE name(types)` | `EXECUTE` |

The `TABLE` target also names a view for `SELECT`; there is no separate `ON
VIEW` form. Function and procedure targets use input argument types to identify
an overload.

`SELECT`, `INSERT` and `UPDATE` accept column lists. A table-level privilege
covers every column. Column-only `SELECT` is intended for an unambiguous
single-table projection; joins and subqueries over that relation require
table-level `SELECT`. Predicates and expressions that read target columns also
need `SELECT`, independently of write privileges.

```sql
GRANT INSERT (id, title), UPDATE (title) ON TABLE reports TO report_editor;
GRANT SELECT (id, title) ON TABLE reports TO report_reader WITH GRANT OPTION;
REVOKE GRANT OPTION FOR SELECT (id, title) ON TABLE reports
  FROM report_reader CASCADE;
```

`WITH GRANT OPTION` permits delegation of only the granted privilege and column
subset. Grants retain grantor provenance; `RESTRICT` protects dependent grants,
while explicit `CASCADE` removes dependent paths without deleting independent
ones.

## Ownership and DDL

The bootstrap owner bypasses ordinary object checks. An object's owner also has
implicit rights over it. The creator becomes owner of a table, view, function
or procedure. Ownership transfer is available only for tables, functions and
procedures, and the target must be an existing Principal:

```sql
ALTER TABLE reports OWNER TO alice;
ALTER FUNCTION calculate_total(INTEGER) OWNER TO alice;
ALTER PROCEDURE refresh_reports() OWNER TO alice;
```

Only bootstrap can create schemas, Principals, Roles and Jobs. Schema `USAGE`
permits name resolution, while the independent schema `CREATE` privilege
permits object creation. Either privilege alone does not imply the other.
Creating an index or trigger also requires ownership of the target table.

Principals and Roles support stable-ID lifecycle operations:

```sql
ALTER PRINCIPAL alice DISABLE;
ALTER PRINCIPAL alice ENABLE;
ALTER PRINCIPAL alice RENAME TO alice_archive;
ALTER ROLE report_reader RENAME TO report_viewer;
DROP PRINCIPAL alice_archive RESTRICT;
DROP ROLE report_viewer CASCADE;
```

`DROP` defaults to `RESTRICT`; explicit `CASCADE` resolves catalog-owned grants,
memberships, ownership and Jobs `RUN AS` without leaving dangling edges. Rename
does not change ObjectId and dropped IDs are never reused.

## Revocation and statement execution

Authorization reads the transaction-visible catalog at every statement entry.
It applies to prepared statements, cached plans, public ORM reads and procedural
SQL. A committed `REVOKE` therefore affects the next execution without waiting
for a plan-cache refresh. Grant and revoke catalog changes are atomic.

Object grants are stored per grantor, grantee and target. Owners and bootstrap
have implicit authority; another subject may delegate only an effective grant
that includes grant option. A grantor can revoke only its own provenance path.

## Missing policy features

RadixDB 1.2 does not implement row-level security or `CREATE POLICY`. It also
has no `PUBLIC` grants, default privileges, explicit deny, `SET ROLE` or public
SQL command for listing effective grants. Enforce row predicates in reviewed
views or application queries, but do not describe that as RLS.

Routine privilege elevation adds another boundary. Continue with
[routine security](../../programming/routine-security/) before using
`SECURITY DEFINER`, triggers or Jobs.
