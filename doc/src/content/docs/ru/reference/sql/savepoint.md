---
title: SAVEPOINT
description: Создание точки восстановления внутри текущей транзакции.
---

`SAVEPOINT` записывает точку отката активной транзакции.

## Синтаксис

```text
SAVEPOINT savepoint_name
```

## Описание

Последующую работу можно отменить через ROLLBACK TO, не отбрасывая предыдущие
изменения. После ROLLBACK TO точка остаётся активной до RELEASE или завершения
транзакции.

## Параметры

`savepoint_name` является идентификатором. Имена без кавычек не зависят от
регистра, а имена в кавычках сохраняют регистр.

## Результат

Успех возвращает пустой командный результат и добавляет точку в текущую
транзакцию.

## Поведение в транзакции

SAVEPOINT допустим только после BEGIN. COMMIT и обычный ROLLBACK удаляют все
точки этой транзакции.

## Ошибки и ограничения

Команда вне транзакции завершается ошибкой. Embedded, TCP и CLI версии 1.2
направляют `SAVEPOINT` и `ROLLBACK TO SAVEPOINT` в активную транзакцию текущего
подключения.

## Права

SAVEPOINT не требует отдельного объектного права. Операторы до и после него
сохраняют обычные требования доступа.

## Пример

```sql
CREATE TABLE ref_savepoint (id INTEGER PRIMARY KEY, value INTEGER);
BEGIN;
INSERT INTO ref_savepoint VALUES (1, 10);
SAVEPOINT before_change;
UPDATE ref_savepoint SET value = 20 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_change;
COMMIT;
SELECT id, value FROM ref_savepoint;
```

## См. также

См. [Транзакции и конкурентность](../../../sql/transactions/),
[ROLLBACK](../rollback/) и [RELEASE SAVEPOINT](../release-savepoint/).
