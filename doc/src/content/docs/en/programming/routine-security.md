---
title: Routine Security
description: Choose invoker or definer execution and preserve privilege boundaries across routines, triggers and Jobs.
---

Every routine call has a session Principal and an effective Principal. The
session identity owns database `CONNECT`; the effective identity supplies
schema, object and column privileges while the body executes.

## Entry checks

A direct caller needs all of the following:

1. `CONNECT` on the selected database for the session Principal.
2. `USAGE` on the routine schema for the current effective Principal.
3. `EXECUTE` on the exact function or procedure overload.

Owner and bootstrap rights still apply. Checks use stable catalog object IDs and
run again for each invocation, including cached calls. Revoking `EXECUTE`
invalidates access to the next call without recompiling the routine.

## Invoker and definer

`SECURITY INVOKER` executes body SQL with the caller's effective Principal.
It is the default choice for routines that should not add authority.

`SECURITY DEFINER` switches only the body's effective Principal to the routine
owner. It does not change the session Principal or bypass database `CONNECT`.
The caller still needs schema `USAGE` and entry `EXECUTE`.

```sql
CREATE PROCEDURE publish_report(IN report_id INTEGER NOT NULL)
LANGUAGE RADIX
SECURITY DEFINER
SEARCH PATH (public)
AS
BEGIN
    UPDATE reports SET published = TRUE WHERE id = :report_id;
END;

GRANT EXECUTE ON PROCEDURE publish_report(INTEGER) TO report_editor;
```

A definer routine requires an explicit stable `SEARCH PATH`. Names in that path
are bound to namespace object IDs when the definition is admitted. Static
dependencies are also bound by ID, while dynamic SQL is parsed, authorized and
resource-checked at runtime. Elevation is removed when the frame returns or
fails.

Keep a definer body small, use schema-qualified or bound names, grant entry
`EXECUTE` narrowly and avoid caller-derived dynamic SQL. Changing routine owner
changes the identity used by subsequent definer calls.

## Triggers

A trigger function keeps its declared invoker or definer mode. Trigger effects
share the firing statement's transaction and resource owner, so an error rolls
back both the row change and trigger effects.

`CREATE TRIGGER` requires table ownership, schema `USAGE` and `EXECUTE` on the
exact trigger function. Every firing rechecks `EXECUTE` against the stable
trigger owner, including after replacement and cached descriptor reuse. A
committed revoke therefore blocks the next firing and rolls back the outer DML;
table ownership cannot be used to attach and invoke an unrelated
bootstrap-owned definer function.

`OLD` and `NEW` are typed procedural records. Static SQL binds their fields as
`:OLD.column` and `:NEW.column`; event availability, type and nullability are
specialized when the trigger is attached.

## Jobs

A Job stores a `RUN AS` Principal and a bound procedure ID. Its trusted host
starts a fresh execution context with that Principal as session, invoker and
effective identity. Entry authorization requires the Job Principal to have
database `CONNECT`, schema `USAGE` and `EXECUTE` on the procedure. A failed
entry check leaves body effects uncommitted.

The procedure then applies its declared mode: invoker remains the Job Principal;
definer switches body privileges to the procedure owner. Only bootstrap can
create Jobs. The stock server owns the durable scheduler, attempt lease and
bounded history; it invokes the same accepted Job executor boundary.

## Context values

Routine expressions and static or dynamic SQL can read the immutable typed
values `CURRENT_PRINCIPAL`, `CURRENT_EFFECTIVE_PRINCIPAL`,
`CURRENT_TRANSACTION_ID`, `CURRENT_STATEMENT_TIMESTAMP` and
`CURRENT_REQUEST_ID`. Job frames additionally expose `CURRENT_JOB_ID`,
`CURRENT_JOB_ATTEMPT`, `CURRENT_JOB_SCHEDULED_AT` and
`CURRENT_IDEMPOTENCY_KEY`; Job-only and request-only values are NULL outside
their contexts. Caller named parameters cannot spoof these reserved names, and
nested definer frames change only the effective Principal.

## Review checklist

- Start with `SECURITY INVOKER` and add definer authority only for a reviewed operation.
- Grant the exact overload and revoke it when the entry point is retired.
- Keep database `CONNECT` on the real session or Job Principal.
- Verify trigger attachment and firing after every relevant `EXECUTE` change.
- Test both allowed and denied paths after ownership or role changes.
- Record session and effective identities separately in audit logic.

See [access control](../../administration/access-control/) for object privileges
and [functions and procedures](../routines/) for call and transaction behavior.
