---
title: Безопасность routines
description: Invoker/definer execution и границы прав в routines, triggers и Jobs.
---

Каждый routine call имеет session Principal и effective Principal. Session
identity владеет database `CONNECT`; effective identity предоставляет schema,
object и column privileges во время выполнения body.

## Проверки входа

Прямому caller нужны все следующие права:

1. `CONNECT` на выбранную database для session Principal.
2. `USAGE` на schema routine для текущего effective Principal.
3. `EXECUTE` на точный overload function или procedure.

Права владельца и начального субъекта продолжают действовать. Проверки используют
стабильные идентификаторы объектов каталога и повторяются при каждом вызове,
включая кэшированные вызовы.
Отзыв `EXECUTE` закрывает следующий call без перекомпиляции routine.

## Invoker и definer

`SECURITY INVOKER` выполняет SQL body с effective Principal caller. Это default
для routines, которые не должны добавлять полномочия.

`SECURITY DEFINER` переключает только effective Principal body на owner routine.
Session Principal не меняется, database `CONNECT` не обходится. Caller
по-прежнему нужны schema `USAGE` и entry `EXECUTE`.

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

Definer routine требует explicit stable `SEARCH PATH`. Names этого path
связываются с namespace object IDs при admission definition. Static dependencies
также связываются по ID, а dynamic SQL повторно проходит parse, authorization и
resource checks в runtime. Elevation снимается при return frame или ошибке.

Делайте definer body небольшим, используйте schema-qualified или bound names,
выдавайте entry `EXECUTE` узко и не стройте dynamic SQL из caller data. Смена
owner routine меняет identity последующих definer calls.

## Triggers

Trigger function сохраняет объявленный invoker или definer mode. Trigger effects
разделяют transaction и resource owner вызывающего statement, поэтому ошибка
откатывает и row change, и effects trigger.

`CREATE TRIGGER` требует table ownership, schema `USAGE` и `EXECUTE` на точную
trigger function. Каждый firing заново проверяет `EXECUTE` для stable trigger
owner, включая replacement и cached descriptor. Поэтому committed revoke
блокирует следующий firing и откатывает outer DML; table ownership больше не
позволяет подключить чужую bootstrap-owned definer function.

`OLD` и `NEW` являются typed procedural records. Static SQL связывает fields как
`:OLD.column` и `:NEW.column`; event availability, type и nullability
специализируются при attachment.

## Jobs

Job хранит `RUN AS` Principal и bound procedure ID. Trusted host создаёт fresh
execution context, где этот Principal является session, invoker и effective
identity. Entry authorization требует для Job Principal database `CONNECT`,
schema `USAGE` и `EXECUTE` на procedure. При отказе body effects не фиксируются.

Затем procedure применяет объявленный mode: invoker оставляет Job Principal,
definer переключает privileges body на owner procedure. Только bootstrap может
создавать Jobs. Штатный server владеет durable scheduler, attempt lease и
bounded history и вызывает тот же accepted Job executor boundary.

## Context values

Routine expressions, static и dynamic SQL читают immutable typed values
`CURRENT_PRINCIPAL`, `CURRENT_EFFECTIVE_PRINCIPAL`, `CURRENT_TRANSACTION_ID`,
`CURRENT_STATEMENT_TIMESTAMP` и `CURRENT_REQUEST_ID`. Job frames дополнительно
дают `CURRENT_JOB_ID`, `CURRENT_JOB_ATTEMPT`, `CURRENT_JOB_SCHEDULED_AT` и
`CURRENT_IDEMPOTENCY_KEY`; Job-only и request-only values равны NULL вне
соответствующего context. Caller named parameters не могут spoof-ить reserved
names, а nested definer frame меняет только effective Principal.

## Контрольный список

- Начинайте с `SECURITY INVOKER`, добавляйте definer authority только для проверенной операции.
- Выдавайте точный overload и отзывайте его после вывода entry point из работы.
- Сохраняйте database `CONNECT` у реального session или Job Principal.
- Проверяйте trigger attachment и firing после каждого изменения `EXECUTE`.
- Проверяйте разрешённые и запрещённые пути после смены владельца или ролей.
- Записывайте session и effective identities в audit раздельно.

Object privileges описаны в [«Управлении доступом»](../../administration/access-control/),
call и transaction behavior: в [«Функциях и процедурах»](../routines/).
