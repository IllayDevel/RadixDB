---
title: SHOW
description: Список отношений и просмотр определений таблиц, представлений и индексов.
---

`SHOW` возвращает выбранные метаданные каталога.

## Синтаксис

```text
SHOW TABLES
SHOW VIEWS
SHOW CREATE { TABLE table_name | VIEW view_name }
SHOW { INDEX | INDEXES } FROM table_name
```

## Описание

SHOW TABLES и SHOW VIEWS перечисляют имена каталога. SHOW CREATE восстанавливает
сохранённое определение отношения. SHOW INDEXES показывает имя индекса, столбцы,
метод, уникальность и параметры одной таблицы.

## Параметры

`table_name` или `view_name` выбирает объект метаданных. INDEX и INDEXES являются
равнозначными написаниями формы с FROM.

## Результат

Все формы возвращают набор строк. Порядок списка является деталью реализации,
если строки не обрабатываются последующим запросом с явным ORDER BY.

## Поведение в транзакции

SHOW читает видимые транзакции метаданные каталога и ничего не изменяет.

## Ошибки и ограничения

Отсутствующее именованное отношение приводит к ошибке. SHOW не является общим
запросом к information schema и не перечисляет эффективные права.

## Права

SHOW TABLES и SHOW VIEWS доступны только bootstrap-субъекту. Для именованных
форм нужны CONNECT, USAGE схемы и SELECT целевого отношения.

## Пример

```sql
CREATE TABLE ref_show (id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX ref_show_code_idx ON ref_show (code) USING BTREE;
SHOW TABLES;
SHOW CREATE TABLE ref_show;
SHOW INDEXES FROM ref_show;
```

## См. также

См. [DESCRIBE](../describe/), [Индексы](../../../sql/indexes/) и
[Управление доступом](../../../administration/access-control/).
