---
title: Triggers
description: Attach deterministic row and statement triggers to table mutations.
---

A trigger is a catalog object that attaches a zero-argument `VOLATILE` function
with `RETURNS TRIGGER` to an `INSERT`, `UPDATE` or `DELETE` event. It runs in the
transaction of the statement that fired it and cannot commit independently.

## Definition

Define the trigger function first, then attach it to a table:

```sql
CREATE FUNCTION docs_touch_trigger() RETURNS TRIGGER
LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;

CREATE TRIGGER docs_touch
BEFORE UPDATE OF value ON docs_trigger_rows
FOR EACH ROW PRIORITY 100
WHEN (OLD.value <> NEW.value)
EXECUTE FUNCTION docs_touch_trigger();
```

The event list can contain `INSERT`, `UPDATE` and `DELETE`; `UPDATE OF` narrows
an update trigger to named columns. Timing is `BEFORE` or `AFTER`, and level is
`FOR EACH ROW` or `FOR EACH STATEMENT`. A `WHEN` expression is bound to the
target table when the trigger is attached.

## OLD and NEW

Row trigger functions receive typed records specialized from the target table:

| Event | `OLD` | `NEW` | Writable `NEW` |
| --- | --- | --- | --- |
| INSERT | unavailable | available | BEFORE ROW only |
| UPDATE | available | available | BEFORE ROW only |
| DELETE | available | unavailable | never |

Use `NEW.column` and `OLD.column` in procedural expressions. Static SQL binds
record fields directly with `:NEW.column` and `:OLD.column`, including quoted
column names. The binder specializes type, nullability and event availability
when the trigger is attached. Whole-record SQL parameters remain unsupported.
`OLD` is always read-only. Statement triggers cannot reference either record.

For a BEFORE INSERT/UPDATE row trigger, `RETURN NEW` continues with the
possibly changed row and `RETURN NULL` suppresses it. BEFORE DELETE returns
`OLD` or suppresses with NULL. AFTER ROW and all statement triggers must
`RETURN NULL`. A suppressed row does not count as affected and does not enter
later row triggers, while statement triggers still run once.

## Ordering and visibility

Matching triggers run by `(PRIORITY ASC, stable trigger object ID ASC)`. The
default priority is `1000`, and the value uses the signed 32-bit range. Equal
priorities remain deterministic through object identity.

Each BEFORE ROW trigger sees changes made by the previous one. `WHEN` is
evaluated immediately before that trigger. AFTER ROW sees the final stored row.
Statement triggers run once even when the statement changes zero rows.

## Errors and rollback

A trigger and its SQL leaves use the firing statement's MVCC owner. If a trigger
raises an error, both the outer mutation and earlier trigger effects roll back:

```sql
CREATE FUNCTION docs_fail_trigger() RETURNS TRIGGER
LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
BEGIN
    INSERT INTO docs_trigger_log VALUES (1);
    RAISE invalid_state('trigger failed');
END;
```

Definition admission rejects an invalid OLD/NEW use, wrong return contract or
incompatible table descriptor before attaching the trigger. Runtime protects
dynamic cycles with an active-chain guard and a maximum trigger depth of 32.
A cycle reports `PL_TRIGGER_CYCLE`; depth exhaustion reports
`PL_TRIGGER_DEPTH`, and the statement rolls back.

## Scope limits

Transition tables and deferred triggers are not implemented in 1.2. Trigger
functions cannot start transactions, schedule jobs or access network,
filesystem or process APIs. Static dependency cycles are rejected; dynamic SQL
retains the runtime guards and cannot bypass capabilities or privileges.

`CREATE OR REPLACE TRIGGER` updates a compatible definition. Remove an
attachment with its table-qualified identity:

```sql
DROP TRIGGER IF EXISTS docs_touch ON docs_trigger_rows RESTRICT;
```

`RESTRICT` is the default and protects static dependencies; explicit `CASCADE`
removes dependent catalog objects atomically.

The executable documentation example verifies NEW mutation and proves that a
raised trigger error leaves neither the inserted row nor its trigger-log row.
The accepted executor suite additionally checks row suppression, all four hook
positions, deterministic order, cycles and rebuild after reopen.

Continue with [Scheduled Jobs](../jobs/).
