---
title: ALTER TABLE
description: Добавление, удаление, переименование или изменение столбца и переименование таблицы.
---

`ALTER TABLE` изменяет определение существующей таблицы.

## Синтаксис

```text
ALTER TABLE table_name ADD [COLUMN] column_definition
ALTER TABLE table_name DROP [COLUMN] [IF EXISTS] column_name
ALTER TABLE table_name MODIFY [COLUMN] column_definition
ALTER TABLE table_name RENAME [COLUMN] column_name TO new_column_name
ALTER TABLE table_name RENAME TO new_table_name
```

## Описание

ADD вводит столбец, DROP удаляет его, MODIFY заменяет поддерживаемое определение,
а RENAME меняет имя столбца или таблицы. Каждую форму следует считать миграцией
данных.

## Параметры

`column_definition` содержит имя, тип и поддерживаемые ограничения. Обязательному
столбцу, добавляемому к существующим строкам, нужен применимый DEFAULT. Новое имя
не должно конфликтовать с каталогом.

## Результат

Успех возвращает командный результат без строк. DESCRIBE показывает итоговое
определение столбцов.

## Поведение в транзакции

ALTER TABLE транзакционен и удерживает каталог, строки и индексы в одной границе
COMMIT. ROLLBACK восстанавливает прежнее определение.

## Ошибки и ограничения

Существующие данные должны удовлетворять новому определению. Удаление столбцов
со ссылками или индексами и неподдержанные преобразования ключей могут завершиться
ошибкой. Матрица не обещает все сочетания ограничений и типов для MODIFY.

## Права

Эффективный Principal должен владеть таблицей; сессии также нужен CONNECT.
Bootstrap-владелец обходит обычные проверки объектов.

## Пример

```sql
CREATE TABLE ref_alter_table (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO ref_alter_table VALUES (1, 'draft');
ALTER TABLE ref_alter_table ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE ref_alter_table RENAME COLUMN label TO title;
SELECT id, title, revision FROM ref_alter_table;
```

## См. также

См. [Определение схемы](../../../sql/ddl/), [CREATE TABLE](../create-table/) и
[Управление доступом](../../../administration/access-control/).
