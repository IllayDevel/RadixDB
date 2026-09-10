---
title: Задания по расписанию
description: Durable Jobs и штатный scheduler RadixDB server.
---

RadixDB Job является durable scheduler metadata над существующей Procedure.
Штатный `radixdb-server` находит enabled Jobs, durable-операцией захватывает
attempt и вызывает bound Procedure в отдельной транзакции.

## Definition и lifecycle

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

One-time Job использует `SCHEDULE AT TIMESTAMP
'2026-12-31T23:59:00Z'`. Для interval допустимы seconds, minutes, hours, days и
weeks; календарные months/years отклоняются. Timestamp нормализуется в UTC
nanoseconds.

Principal и Procedure overload должны существовать при admission. Named/default
arguments и checked conversions фиксируются в definition, а Job arguments
должны быть immutable catalog-serializable constants. Изменение enabled state
повышает definition version и сбрасывает scheduler state. `DROP` по умолчанию
использует `RESTRICT`; явный `CASCADE` разрешает catalog dependencies.
`CREATE OR REPLACE JOB` и ручной `RUN JOB` не реализованы.

## Контракт scheduler

Штатный server опрашивает ready databases и ведёт две durable relations:

- `radix_system_job_state` хранит текущие planned time, attempt, lease и
  idempotency key;
- `radix_system_job_history` записывает running/terminal attempts и сохраняет
  не более 256 строк на Job.

Conditional state update и пятиминутный attempt lease исключают overlap живых
claims. После restart просроченный claim возобновляется. Пропущенные interval
ticks coalesce-ятся до последнего due tick, а не проигрываются без ограничений.
One-time Job после success переходит в complete, interval Job — к следующему
planned time.

Delivery имеет at-least-once семантику. Crash после commit Procedure, но до
финализации ledger может повторить attempt. Stable idempotency key имеет вид
`<job-id>/<planned-nanoseconds>` и не меняется при retry; если duplicate effects
важны, application procedure должна сохранять или enforce-ить этот key вместе
с эффектом.

Server публикует bounded scheduler counters и последнюю redacted error в status
response. Clean shutdown отменяет scheduler и дожидается worker до закрытия
database owners.

## Execution context и security

Attempt выполняется с Job `RUN AS` Principal как session identity. Entry
требует database `CONNECT`, schema `USAGE` и `EXECUTE` на точный Procedure
overload. `SECURITY DEFINER` меняет только body effective Principal.

Procedure читает immutable values:

- `CURRENT_JOB_ID` (`UUID`);
- `CURRENT_JOB_ATTEMPT` (`INTEGER`);
- `CURRENT_JOB_SCHEDULED_AT` (`TIMESTAMP`);
- `CURRENT_IDEMPOTENCY_KEY` (`TEXT`).

Вне Job frame они равны NULL. Principal, effective Principal, transaction и
statement timestamp values описаны в [«Безопасности routines»](../routine-security/).

## Failure и retry

Каждый неуспешный attempt нормализуется как `PL_JOB_ATTEMPT_FAILED` со stable
cause kind/category, attempt metadata и detail `scheduler_retryable`. Security
failures редактируются перед записью в history. Conflict, deadline и
cancellation считаются retryable; остальные causes терминальны для данного
planned tick.

Retryable failures используют bounded exponential backoff от 100 ms до 10 s,
не более пяти attempts и тот же idempotency key. Effects Procedure при ошибке
откатываются. Для interval Job terminal failure двигает schedule дальше, для
one-time Job оставляет terminal failed state.

Синтаксис процедурного языка описан в [«RadixDB PL»](../pl-sql/).
