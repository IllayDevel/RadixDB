---
title: Scheduled Jobs
description: Define durable Jobs and run them with the stock server scheduler.
---

A RadixDB Job is durable scheduler metadata over one existing Procedure. The
stock `radixdb-server` discovers enabled Jobs, claims attempts durably and calls
the bound Procedure in its own transaction.

## Definition and lifecycle

```sql
CREATE JOB maintenance.expire_sessions
SCHEDULE EVERY INTERVAL '5 minute'
RUN AS maintenance_worker
CALL auth.expire_sessions(batch_size => 1000)
ENABLE;

ALTER JOB maintenance.expire_sessions DISABLE;
ALTER JOB maintenance.expire_sessions ENABLE;
DROP JOB maintenance.expire_sessions RESTRICT;
```

A one-time Job uses `SCHEDULE AT TIMESTAMP '2026-12-31T23:59:00Z'`. Interval
units are seconds, minutes, hours, days or weeks; calendar months and years are
rejected. Timestamps are normalized to UTC nanoseconds.

The Principal and Procedure overload must exist at admission. Named/default
arguments and checked conversions are frozen into the definition, and Job
arguments must be immutable catalog-serializable constants. Altering enabled
state advances the definition version and resets scheduler state. `DROP`
defaults to `RESTRICT`; explicit `CASCADE` resolves catalog dependencies.
`CREATE OR REPLACE JOB` and manual `RUN JOB` are not implemented.

## Scheduler contract

The stock server polls ready databases and keeps two durable relations:

- `radix_system_job_state` owns the current planned time, attempt, lease and
  idempotency key;
- `radix_system_job_history` records running and terminal attempts and retains
  at most 256 rows per Job.

A conditional state update and a five-minute attempt lease prevent overlapping
live claims. Restart resumes an expired claim. Missed interval ticks are
coalesced to the latest due tick rather than replayed without bound. One-time
Jobs become complete after success; interval Jobs advance to the next planned
time.

Delivery is at-least-once. A crash after the Procedure commits but before the
ledger finalization may repeat the attempt. The stable idempotency key is
`<job-id>/<planned-nanoseconds>` and is reused for retries; application
procedures should store or enforce it with their effects when duplicates matter.

The server exports bounded scheduler counters and the latest redacted error in
its status response. Clean shutdown cancels the scheduler and waits for its
worker before closing database owners.

## Execution context and security

Each attempt runs with the Job `RUN AS` Principal as session identity. Entry
requires database `CONNECT`, schema `USAGE` and `EXECUTE` on the exact Procedure
overload. `SECURITY DEFINER` changes only the body effective Principal.

The Procedure can read these immutable values:

- `CURRENT_JOB_ID` (`UUID`);
- `CURRENT_JOB_ATTEMPT` (`INTEGER`);
- `CURRENT_JOB_SCHEDULED_AT` (`TIMESTAMP`);
- `CURRENT_IDEMPOTENCY_KEY` (`TEXT`).

They are NULL outside a Job frame. Principal, effective Principal, transaction
and statement timestamp values remain available as described under
[routine security](../routine-security/).

## Failure and retry

Every failed attempt is normalized as `PL_JOB_ATTEMPT_FAILED` with a stable
cause kind, category, attempt metadata and `scheduler_retryable` detail.
Security failures are redacted before entering history. Conflict, deadline and
cancellation causes are retryable; other causes are terminal for that planned
tick.

Retryable failures use bounded exponential backoff (100 ms through a 10 s cap),
at most five attempts and the same idempotency key. Procedure effects are
rolled back on failure. For interval Jobs, a terminal failure advances the
schedule; for one-time Jobs it leaves terminal failed state.

Continue with [RadixDB PL](../pl-sql/) for procedural syntax.
