---
title: CREATE TABLE
description: Создание таблицы, её столбцов и проверенных ограничений.
---

`CREATE TABLE` добавляет отношение в каталог.

## Синтаксис

```text
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name data_type [column_constraint ...] [, ...]
    [, table_constraint ...]
)

column_constraint := [PRIMARY KEY] [AUTO_INCREMENT] [NOT NULL]
                     [UNIQUE] [DEFAULT expression] [CHECK (condition)]
                     [REFERENCES table_name (column_name)]
table_constraint  := PRIMARY KEY (column_name [, ...])
                   | UNIQUE (column_name [, ...])
                   | CHECK (condition)
```

## Описание

Команда создаёт столбцы и поддерживаемые правила целостности одним изменением
каталога. IF NOT EXISTS подавляет создание лишь при уже занятом имени отношения
и не согласует определения.

## Параметры

`data_type` использует имена типов RadixDB. DEFAULT задаёт пропущенное значение.
CHECK отклоняет ложное выражение. REFERENCES объявляет проверенный путь внешнего
ключа из одного столбца. AUTO_INCREMENT ограничен поддержанными путями первичного
ключа INTEGER и UUID.

## Результат

Успех возвращает командный результат без строк. Новая таблица становится видна
после COMMIT и доступна для проверки через DESCRIBE.

## Поведение в транзакции

CREATE TABLE транзакционен. Он может находиться в одной явной транзакции с DML;
ROLLBACK удаляет незавершённое отношение и его строки.

## Ошибки и ограничения

Повтор имени, неверный тип или ограничение и несовместимая форма генерируемого
ключа завершаются ошибкой. IF NOT EXISTS не является миграцией и не сравнивает
схемы.

## Права

Обычной сессии нужны CONNECT и USAGE целевой схемы. В RadixDB 1.2 нет отдельного
права CREATE схемы, поэтому USAGE также разрешает создание. Создатель становится
владельцем.

## Пример

```sql
CREATE TABLE ref_create_table (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    code TEXT NOT NULL UNIQUE,
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0)
);
DESCRIBE ref_create_table;
```

## См. также

См. [Определение схемы](../../../sql/ddl/), [ALTER TABLE](../alter-table/) и
[Типы данных](../../../sql/types/).
