---
title: DESCRIBE
description: Просмотр таблицы или получение JSON-дескриптора базы.
---

`DESCRIBE` возвращает метаданные каталога.

## Синтаксис

```text
{ DESCRIBE | DESC } [TABLE] table_name [FORMAT JSON]
DESCRIBE DATABASE FORMAT JSON
```

## Описание

Табличная форма показывает столбцы. FORMAT JSON возвращает версионированную
оболочку дескриптора таблицы; DATABASE доступен только с FORMAT JSON.

## Параметры

`table_name` задаёт отношение. TABLE необязателен. FORMAT JSON выбирает одно
JSON-значение вместо прежней табличной формы.

## Результат

Табличный результат содержит `Field`, `Type`, `Null`, `Key`, `Default` и
`Extra`. JSON-режим возвращает один столбец дескриптора и одну строку.

## Поведение в транзакции

DESCRIBE читает видимые транзакции метаданные каталога и ничего не изменяет.

## Ошибки и ограничения

Отсутствующий объект приводит к ошибке. `DESCRIBE DATABASE` без FORMAT JSON
отклоняется. JSON-дескриптор версионирован: разбирать следует поля, а не формат.

## Права

Для метаданных таблицы нужны CONNECT, USAGE схемы и SELECT отношения. Описание
всей базы доступно только bootstrap-субъекту.

## Пример

```sql
CREATE TABLE ref_describe (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL DEFAULT 'untitled'
);
DESCRIBE ref_describe;
```

## См. также

См. [Определение схемы](../../../sql/ddl/), [SHOW](../show/) и
[Внутреннее устройство протокола](../../../internals/protocol/).
