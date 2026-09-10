---
title: RELEASE SAVEPOINT
description: Удаление точки сохранения без завершения транзакции.
---

`RELEASE SAVEPOINT` удаляет именованную точку отката.

## Синтаксис

```text
RELEASE [SAVEPOINT] savepoint_name
```

## Описание

Команда сохраняет все изменения до и после точки. Удаляется лишь возможность
вернуться к этому маркеру.

## Параметры

SAVEPOINT необязателен. `savepoint_name` подчиняется тем же правилам кавычек и
регистра, что и в SAVEPOINT.

## Результат

Успех возвращает пустой командный результат и оставляет транзакцию активной.

## Поведение в транзакции

RELEASE допустим только внутри транзакции, которой принадлежит живая точка. Он
не фиксирует изменения.

## Ошибки и ограничения

Отсутствие транзакции или точки приводит к ошибке. Embedded, TCP и CLI версии
1.2 направляют `RELEASE SAVEPOINT` в активную транзакцию текущего подключения.

## Права

RELEASE не требует отдельного объектного права.

## Пример

```sql
CREATE TABLE ref_release_savepoint (id INTEGER PRIMARY KEY);
BEGIN;
SAVEPOINT completed_step;
INSERT INTO ref_release_savepoint VALUES (1);
RELEASE SAVEPOINT completed_step;
COMMIT;
SELECT id FROM ref_release_savepoint;
```

## См. также

См. [SAVEPOINT](../savepoint/), [ROLLBACK](../rollback/) и
[Клиентские интерфейсы](../../../clients/overview/).
