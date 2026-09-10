---
title: Функции и процедуры
description: Typed routines, атомарный CALL и ограничения ресурсов.
---

Functions и procedures являются разными durable catalog objects. Оба вида
используют RadixDB PL, stable object identities и одну transaction boundary
caller, но отличаются местом вызова и разрешёнными effects.

| Object | Invocation | Effects | Result |
| --- | --- | --- | --- |
| Function | SQL expression или `PERFORM` | Ограничены volatility | Scalar или bounded table |
| Procedure | `CALL` | Query, DML и nested calls | Void, `OUT`/`INOUT` или bounded table |

## Функции

Function имеет только input arguments, result contract, обязательную volatility
и обязательный security mode:

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

`IMMUTABLE` functions используют только arguments, constants и deterministic
immutable functions. `STABLE` добавляет чтение текущего snapshot и stable
context, например `CURRENT_TIMESTAMP`. Оба вида не могут писать.
`VOLATILE` functions могут читать и писать в transaction caller; planner не
вправе удалять, дублировать или переставлять наблюдаемый volatile call.

Полный static call graph проверяется при admission definition. Dynamic SQL
проверяется повторно во время выполнения и не обходит volatility contract.

## Procedures и CALL

Procedure arguments по умолчанию имеют режим `IN`, также доступны `OUT` и
`INOUT`. Defaults вычисляются слева направо и видят только более ранние
arguments:

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

Call возвращает один столбец `output_value` со значением `21`. Синтаксис named
argument: `name => expression`. Обязательные inputs не могут следовать после
defaulted inputs, а `OUT` argument не принимает default.

Overload identity использует object kind, namespace, name и ordered types
`IN`/`INOUT`. Exact match предпочтительнее checked lossless conversions.
Untyped NULL может быть неоднозначным; явно приведите его к нужному типу.
Return type не выбирает overload.

## Модели результата

Scalar function должна вернуть значение на каждом reachable path. Void
procedure использует `RETURN` либо достигает конца. Procedure выбирает
`OUT`/`INOUT` arguments или отдельный `RETURNS` contract и не смешивает модели.

`RETURNS TABLE (name type, ...)` использует `RETURN NEXT` и `RETURN QUERY`.
Rows staged и bounded. Partial result не выдаётся, если call позднее завершился
ошибкой, client рано закрыл cursor или исчерпан resource budget.

## Поведение транзакции

Внешний `CALL` без client transaction использует одну автоматическую statement
transaction. В explicit client transaction он использует statement savepoint,
принадлежащий этой транзакции. Nested functions, procedures, SQL и triggers
видят один MVCC state.

```rust
connection.begin()?;
connection.execute("CALL docs_flow(30, 1)")?;
connection.rollback()?;
```

Любая необработанная ошибка откатывает всю call boundary, включая effects
volatile argument expressions и nested triggers. Она не выполняет commit или
rollback explicit transaction caller. Transaction-control statements внутри
routine body запрещены.

## Замена и зависимости

`CREATE OR REPLACE` сохраняет object identity, только если argument names,
modes, types, nullability и полный result contract совместимы. Body, defaults,
volatility, security, search path и resource policy могут измениться с
увеличением definition revision. Владелец меняется через
`ALTER FUNCTION ... OWNER TO` или `ALTER PROCEDURE ... OWNER TO`.

Functions и procedures удаляются по точной input signature:

```sql
DROP FUNCTION IF EXISTS calculate_total(INTEGER, TEXT) RESTRICT;
DROP PROCEDURE refresh_reports(UUID) CASCADE;
```

Signature обязательна, когда возможен overload. `RESTRICT` является default и
не даёт удалить routine, на которую ссылаются trigger, Job или static routine
dependency. Явный `CASCADE` удаляет dependent catalog objects в той же
атомарной catalog publication.

Definitions связывают static object references со stable catalog IDs. Compiled
IR является rebuildable cache с key по revisions definition и dependencies.
Принятые tests 1.2 пересобирают routines из source после persistent reopen.

## Resource budgets

Каждый top-level call и все nested frames, SQL leaves, cursors, dynamic SQL и
triggers разделяют одного budget owner:

| Dimension | Default call | Hard ceiling |
| --- | ---: | ---: |
| Executed instructions | 10,000,000 | 1,000,000,000 |
| Procedural heap | 64 MiB | 256 MiB |
| Live frames | 64 | 256 |
| SQL statements | 100,000 | 10,000,000 |
| Rows read, changed or emitted | 1,000,000 | 10,000,000 |
| Result and retained bytes | 256 MiB | 1 GiB |
| Deadline | 60 s | 24 h |

Необязательный clause `RESOURCE POLICY` в 1.2 принимает только встроенное имя
`default` или `default_call`; DDL для custom policy objects отсутствует.
Resource errors откатывают call, а не обрезают result.

Embedded diagnostics предоставляют stable `PL_*` kinds и bounded details. TCP
protocol 17 передаёт только coarse `SqlError` либо `AuthorizationDenied` и
message с prefix `PL_*:`; structured procedural envelope по wire не передаётся.
Не определяйте retry policy разбором prose.

Исполняемый documentation gate проверяет function admission,
named/default CALL result, rollback caller и tests persistence procedure.
Далее: [«Триггеры»](../triggers/).
