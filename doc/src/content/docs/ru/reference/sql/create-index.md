---
title: CREATE INDEX
description: Создание обычных, уникальных, составных и частичных индексов.
---

`CREATE INDEX` добавляет обслуживаемую структуру доступа к таблице.

## Синтаксис

```text
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name
ON table_name (column_name [operator_class] [, ...])
[USING { BTREE | HASH | BITMAP | HNSW }]
[WITH (option = expression [, ...])]
[WHERE condition]
```

## Описание

BTREE, HASH и BITMAP являются проверенными обычными методами. Список из нескольких
столбцов создаёт составной индекс. UNIQUE обеспечивает уникальность индексируемых
не-NULL ключей, а WHERE включает лишь строки с истинным локальным условием.

## Параметры

`index_name` идентифицирует объект каталога. `column_name` перечисляет хранимые
ключи по порядку. `operator_class` выбирает видимый built-in или
extension-bound contract key и strategy для этого столбца. USING выбирает
метод; без него выбор делает движок. WITH в основном предназначен для
параметров конкретного метода.

## Результат

Успех возвращает командный результат без строк. SHOW INDEXES показывает
сохранённое определение; выбор планировщика проверяется через EXPLAIN.

## Поведение в транзакции

Создание индекса транзакционно и публикуется вместе с поколением каталога
таблицы при COMMIT.

## Ошибки и ограничения

Повтор активного UNIQUE-ключа отклоняется. Частичное условие должно быть
детерминированным и локальным для строки. Частичные HNSW-индексы отклоняются.
IF NOT EXISTS подавляет только идентичное определение; иное под тем же именем
завершается ошибкой. Operator class должен соответствовать типу столбца и
выбранному access method.

## Права

Эффективный Principal должен владеть целевой таблицей; сессии также нужен
CONNECT. Индекс получает совместимое владение в каталоге.

## Пример

```sql
CREATE TABLE ref_create_index (id INTEGER PRIMARY KEY, email TEXT, active BOOLEAN);
CREATE UNIQUE INDEX ref_email_active_idx
ON ref_create_index (email) USING HASH WHERE active = true;
SHOW INDEXES FROM ref_create_index;
```

## См. также

См. [Индексы](../../../sql/indexes/),
[Команды native extensions](../extensions/), [ALTER INDEX](../alter-index/) и
[SHOW](../show/).
