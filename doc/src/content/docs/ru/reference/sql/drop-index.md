---
title: DROP INDEX
description: Удаление именованного индекса указанной таблицы.
---

`DROP INDEX` удаляет определение индекса и его перестраиваемое хранилище.

## Синтаксис

```text
DROP INDEX [IF EXISTS] index_name ON table_name
```

## Описание

Контракт выполнения 1.2 требует секцию ON table, хотя парсер способен представить
пропущенное имя таблицы. IF EXISTS подавляет ошибку отсутствующего индекса.

## Параметры

`index_name` идентифицирует индекс, а `table_name` фиксирует его таблицу-владельца.

## Результат

Успех возвращает командный результат без строк. SHOW INDEXES подтверждает
оставшиеся определения.

## Поведение в транзакции

Удаление индекса транзакционно. ROLLBACK сохраняет прежний индекс видимым.

## Ошибки и ограничения

Отсутствие `ON table_name` отклоняется в версии 1.2. Ошибка возникает также при
несовпадении таблицы, отсутствии индекса без IF EXISTS и попытке удалить
неявную структуру первичного ключа.

## Права

Эффективный Principal должен владеть индексом; сессии также нужен CONNECT.

## Пример

```sql
CREATE TABLE ref_drop_index (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_drop_code_idx ON ref_drop_index (code) USING BTREE;
DROP INDEX ref_drop_code_idx ON ref_drop_index;
SHOW INDEXES FROM ref_drop_index;
```

## См. также

См. [Индексы](../../../sql/indexes/), [CREATE INDEX](../create-index/) и
[ALTER INDEX](../alter-index/).
