---
title: Functions and Procedures
description: Define typed routines, call them atomically and understand their resource limits.
---

Functions and procedures are separate durable catalog objects. Both use
RadixDB PL, stable object identities and one caller-owned transaction boundary,
but their call sites and permitted effects differ.

| Object | Invocation | Effects | Result |
| --- | --- | --- | --- |
| Function | SQL expression or `PERFORM` | Governed by volatility | Scalar or bounded table |
| Procedure | `CALL` | Query, DML and nested calls | Void, `OUT`/`INOUT`, or bounded table |

## Functions

A function has only input arguments, a result contract, required volatility
and required security mode:

```sql
CREATE FUNCTION docs_add(
    left_value INTEGER NOT NULL,
    right_value INTEGER NOT NULL DEFAULT 1
) RETURNS INTEGER NOT NULL
LANGUAGE RADIX
IMMUTABLE
SECURITY INVOKER
AS
BEGIN
    RETURN left_value + right_value;
END;

SELECT docs_add(41);
```

`IMMUTABLE` functions can use only arguments, constants and deterministic
immutable functions. `STABLE` adds reads from the current snapshot and stable
context such as `CURRENT_TIMESTAMP`. Neither can write. `VOLATILE` functions
may read and write in the caller transaction; the planner cannot remove,
duplicate or reorder an observable volatile call.

The complete static call graph is checked at definition admission. Dynamic SQL
is checked again at runtime, so it cannot bypass the volatility contract.

## Procedures and CALL

Procedure arguments are `IN` by default and may be `OUT` or `INOUT`. Defaults
are evaluated left-to-right and may refer only to earlier arguments:

```sql
CREATE PROCEDURE add_default(
    IN first_value INTEGER NOT NULL,
    IN second_value INTEGER NOT NULL DEFAULT first_value + 1,
    OUT output_value INTEGER NOT NULL
) LANGUAGE RADIX
SECURITY INVOKER
AS
BEGIN
    output_value := first_value + second_value;
END;

CALL add_default(first_value => 10);
```

The call returns one `output_value` column containing `21`. Named argument
syntax is `name => expression`. Required inputs cannot follow defaulted inputs,
and an `OUT` argument cannot have a default.

Overload identity uses object kind, namespace, name and ordered `IN`/`INOUT`
types. Exact matches are preferred over checked lossless conversions. An
untyped NULL can be ambiguous; cast it to the intended type. Return type does
not select an overload.

## Result models

A scalar function must return a value on every reachable path. A void procedure
uses `RETURN` or reaches the end. A procedure chooses either `OUT`/`INOUT`
arguments or an explicit `RETURNS` contract; it cannot mix them.

`RETURNS TABLE (name type, ...)` uses `RETURN NEXT` and `RETURN QUERY`. Rows are
staged and bounded. A partial result is not released if the call later fails,
the client closes its cursor early or a resource budget is exceeded.

## Transaction behavior

An outer `CALL` with no client transaction uses one automatic statement
transaction. Inside an explicit client transaction, it uses a statement
savepoint owned by that transaction. Nested functions, procedures, SQL and
triggers see the same MVCC state.

```rust
connection.begin()?;
connection.execute("CALL docs_flow(30, 1)")?;
connection.rollback()?;
```

Any unhandled error rolls back the entire call boundary, including side effects
from volatile argument expressions and nested triggers. It does not commit or
roll back the caller's explicit transaction. Transaction-control statements
inside a routine body are forbidden.

## Replacement and dependencies

`CREATE OR REPLACE` preserves object identity only when argument names, modes,
types, nullability and complete result contract remain compatible. Body,
defaults, volatility, security, search path and resource policy may change and
increase the definition revision. `ALTER FUNCTION ... OWNER TO` and
`ALTER PROCEDURE ... OWNER TO` can change ownership.

Functions and procedures are dropped by exact input signature:

```sql
DROP FUNCTION IF EXISTS calculate_total(INTEGER, TEXT) RESTRICT;
DROP PROCEDURE refresh_reports(UUID) CASCADE;
```

The signature is mandatory when an overload could exist. `RESTRICT` is the
default and refuses to remove a routine referenced by a trigger, Job or static
routine dependency. Explicit `CASCADE` removes those dependent catalog objects
in the same atomic catalog publication.

Definitions bind static object references to stable catalog IDs. Compiled IR is
a rebuildable cache keyed by definition and dependency revisions. The accepted
1.2 tests rebuild routines from source after a persistent reopen.

## Resource budgets

Every top-level call and all nested frames, SQL leaves, cursors, dynamic SQL and
triggers share one budget owner:

| Dimension | Default call | Hard ceiling |
| --- | ---: | ---: |
| Executed instructions | 10,000,000 | 1,000,000,000 |
| Procedural heap | 64 MiB | 256 MiB |
| Live frames | 64 | 256 |
| SQL statements | 100,000 | 10,000,000 |
| Rows read, changed or emitted | 1,000,000 | 10,000,000 |
| Result and retained bytes | 256 MiB | 1 GiB |
| Deadline | 60 s | 24 h |

The optional `RESOURCE POLICY` clause accepts only the built-in `default` or
`default_call` name in 1.2; there is no DDL for custom policy objects. Resource
errors roll back rather than truncate the result.

Embedded diagnostics expose stable `PL_*` kinds and bounded details. TCP
protocol 17 carries only a coarse `SqlError` or `AuthorizationDenied` plus a
message retaining the `PL_*:` prefix; it does not transport the structured
procedural envelope. Do not derive retry policy by parsing prose.

The executable documentation gate verifies function admission, named/default
CALL results, caller rollback and procedure persistence tests. Continue with
[Triggers](../triggers/).
