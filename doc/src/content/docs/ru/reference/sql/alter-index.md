---
title: ALTER INDEX
description: Переименование индекса без изменения его определения.
---

`ALTER INDEX` переименовывает существующий индекс.

## Синтаксис

```text
ALTER INDEX index_name RENAME TO new_index_name
```

## Описание

В версии 1.2 поддержан только RENAME TO. Метод индекса, столбцы, условие и
уникальность не изменяются.

## Параметры

`index_name` является текущим неквалифицированным именем. `new_index_name` должно
быть свободно в пространстве каталога.

## Результат

Успех возвращает командный результат без строк. SHOW INDEXES показывает новое
имя.

## Поведение в транзакции

Переименование транзакционно. ROLLBACK восстанавливает старое имя каталога.

## Ошибки и ограничения

Отсутствующий исходный индекс и занятое целевое имя приводят к ошибке. Перестройка,
смена метода и столбцов не являются формами ALTER INDEX в версии 1.2.

## Права

Эффективный Principal должен владеть индексом; сессии также нужен CONNECT.

## Пример

```sql
CREATE TABLE ref_alter_index (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_code_idx ON ref_alter_index (code) USING BTREE;
ALTER INDEX ref_code_idx RENAME TO ref_code_lookup_idx;
SHOW INDEXES FROM ref_alter_index;
```

## См. также

См. [Индексы](../../../sql/indexes/), [CREATE INDEX](../create-index/) и
[DROP INDEX](../drop-index/).
