---
title: Матрица покрытия SQL
description: Проверенные конструкции SQL 1.2, ограничения, отказы и исполняемые доказательства.
---

Эта матрица задаёт консервативный SQL-контракт RadixDB 1.2. Строка получает
статус «Поддерживается» только после прохождения опубликованного примера или
профильного теста движка на закреплённой ревизии. Синтаксически похожая
конструкция, отсутствующая в таблице, не становится публичным контрактом
автоматически.

Значения статуса:

- **Поддерживается**: указанная форма 1.2 реализована и имеет зелёное доказательство.
- **С ограничениями**: работает определённое подмножество; сначала прочитайте главу.
- **Отклоняется**: версия 1.2 намеренно возвращает проверенную ошибку.

В столбце теста указан скрипт из `doc/scripts/`. Скрипты читают примеры из
обеих языковых версий, сравнивают их и проверяют успешные и отклоняемые операции
на ревизии `40b1b3d13e050afa2666a0414b7215d5ac1452c0`.

## Представительные результаты

Это небольшое успешное выражение проверяется вместе со всей матрицей:

```sql
SELECT COALESCE(NULL, 7) AS value;
```

Следующее неподдерживаемое предложение должно завершиться ошибкой; отказ
является проверенным результатом, а не пропущенным примером:

```sql
SELECT 1 AS value QUALIFY value = 1;
```

## Синтаксис, типы и выражения

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| SYN-01 | Идентификаторы, литералы, комментарии и разделители операторов | Поддерживается | 1.2 | [Синтаксис](../../sql/syntax/) | `test-syntax.mjs` |
| SYN-02 | Позиционные и именованные параметры | С ограничениями | 1.2 | [Синтаксис](../../sql/syntax/) | `test-syntax.mjs` |
| TYPE-01 | INTEGER и целочисленные псевдонимы | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-02 | FLOAT, DOUBLE и REAL | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-03 | DECIMAL/NUMERIC с точностью и масштабом | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-04 | TEXT и текстовые псевдонимы | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-05 | BOOLEAN/BOOL | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-06 | Псевдонимы TIMESTAMP, DATETIME и TIME | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-07 | DATE | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-08 | UUID | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-09 | BYTES и бинарные псевдонимы | Поддерживается | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-10 | Тип хранения JSON/JSONB | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| TYPE-11 | Объявление размерности VECTOR | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-values.mjs` |
| EXPR-01 | Арифметика и приоритеты | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |
| EXPR-02 | Сравнения и трёхзначная логика NULL | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |
| EXPR-03 | IN, NOT IN и BETWEEN | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |
| EXPR-04 | CASE, COALESCE и NULLIF | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |
| EXPR-05 | Конкатенация строк, LIKE и скалярные вызовы | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |
| EXPR-06 | CAST | Поддерживается | 1.2 | [Выражения](../../sql/expressions/) | `test-values.mjs` |

Параметры связываются клиентскими API; CLI-примеры не вводят соглашение о
подстановке литералов. Модификаторы длины текста отклоняются, TIME является
псевдонимом timestamp-хранения, а распознавание JSON или VECTOR не означает
поддержку набора операторов PostgreSQL.

## Схема и индексы

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| DDL-01 | CREATE TABLE и IF NOT EXISTS | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-02 | PRIMARY KEY, NOT NULL и DEFAULT | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-03 | CHECK, UNIQUE и одноколоночный REFERENCES | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-04 | AUTO_INCREMENT первичного ключа INTEGER | С ограничениями | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-05 | DESCRIBE и SHOW TABLES | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-06 | ALTER ADD/DROP/MODIFY/RENAME COLUMN | С ограничениями | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-07 | ALTER TABLE RENAME TO | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-08 | Откат транзакционного DDL | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| DDL-09 | DROP TABLE IF EXISTS | Поддерживается | 1.2 | [Определение схемы](../../sql/ddl/) | `test-schema.mjs` |
| IDX-01 | CREATE INDEX с BTREE, HASH или BITMAP | Поддерживается | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-02 | Составной индекс | Поддерживается | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-03 | UNIQUE и SQL-семантика NULL | Поддерживается | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-04 | Частичный индекс с row-local предикатом | Поддерживается | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-05 | Доказательство предиката частичного индекса | С ограничениями | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-06 | Частичный индекс HNSW | Отклоняется | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-07 | Проверка идентичности CREATE INDEX IF NOT EXISTS | С ограничениями | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-08 | SHOW INDEXES и ALTER INDEX RENAME | Поддерживается | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |
| IDX-09 | DROP INDEX name ON table | С ограничениями | 1.2 | [Индексы](../../sql/indexes/) | `test-schema.mjs` |

`ALTER ... MODIFY` ограничен доказанными в главе формами и не обещает любую
трансформацию ключа. Частичный индекс применяется только при доказанном
предикате запроса. Для `DROP INDEX` сейчас обязательно `ON table`.

## Native extensions

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| EXT-01 | Exact package binding через CREATE/DROP EXTENSION ... RESTRICT | Поддерживается | 1.2 | [Команды native extensions](../../reference/sql/extensions/) | `test-extensions.mjs` |
| EXT-02 | Catalog-bound external scalar types и protocol values | С ограничениями | 1.2 | [Типы данных](../../sql/types/) | `test-extensions.mjs` |
| EXT-03 | Native scalar и explicit batch functions | Поддерживается | 1.2 | [Разработка native extensions](../../programming/native-extensions/) | `test-extensions.mjs` |
| EXT-04 | Plugin operators, B-tree/hash/bitmap classes и bounded planner support | С ограничениями | 1.2 | [Индексы](../../sql/indexes/) | `test-extensions.mjs` |
| EXT-05 | Path/URL install, version ranges, CASCADE, hot reload и ALTER EXTENSION UPDATE | Отклоняется | 1.2 | [Extensions](../../administration/extensions/) | `test-extensions.mjs` |

External types требуют exact package identity и codec revision. Generic ORM и
generic SQL literals не декодируют и не создают их canonical bytes. Rust
authoring SDK поддерживает binary operators и отклоняет external HNSW,
aggregate, window и table-valued descriptors в 1.2. Packages являются trusted
in-process code и допускаются только из explicit startup allowlist.

## Изменение данных

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| DML-01 | INSERT со списком столбцов и несколькими строками | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-02 | INSERT/UPDATE/DELETE RETURNING | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-03 | Предикаты UPDATE и DELETE | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-04 | Ноль затронутых строк как результат команды | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-05 | ON CONFLICT DO NOTHING | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-06 | ON CONFLICT DO UPDATE и значения excluded | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-07 | Атомарность оператора при конфликте строки | Поддерживается | 1.2 | [Изменение данных](../../sql/dml/) | `test-dml.mjs` |
| DML-08 | Навигационные пути в выражениях записи | Отклоняется | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |

## Запросы

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| QUERY-01 | Проекция SELECT и WHERE | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-02 | ORDER BY с NULLS FIRST/LAST | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-03 | LIMIT и OFFSET | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-04 | INNER, LEFT, RIGHT, FULL и CROSS JOIN | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-05 | JOIN ON, USING и NATURAL JOIN | С ограничениями | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-06 | GROUP BY, HAVING, COUNT, SUM и AVG | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-07 | Scalar-, IN-, EXISTS- и FROM-подзапросы | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-08 | Нерекурсивный CTE | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-09 | Рекурсивный CTE с UNION ALL | С ограничениями | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-10 | Рекурсивный CTE с устраняющим повторы UNION | Отклоняется | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-11 | Оконные ранжирование, навигация и агрегаты | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-12 | Nullable indexed window partition после reopen | Поддерживается | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-13 | Производная таблица LATERAL | Отклоняется | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-14 | QUALIFY | Отклоняется | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |
| QUERY-15 | ORDER BY по скрытому qualified input после агрегации | Отклоняется | 1.2 | [Запросы к данным](../../sql/queries/) | `test-queries.mjs` |

NATURAL JOIN поддерживается, но не рекомендуется в долговечных схемах: новые
одноимённые столбцы меняют его условие. Рекурсивный CTE требует `UNION ALL` и
явного условия завершения. Nullable indexed partitions сохраняют группу `NULL`
после перехода в cold storage и повторного открытия базы.

## Транзакции и конкурентный доступ

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| TX-01 | Autocommit и BEGIN/COMMIT/ROLLBACK | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-02 | SAVEPOINT, ROLLBACK TO и RELEASE | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-03 | Изоляция READ COMMITTED | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-04 | Изоляция SNAPSHOT | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-05 | SERIALIZABLE, REPEATABLE READ и READ UNCOMMITTED | Отклоняется | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-06 | Маршрутизация isolation clause и savepoint в CLI | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-07 | SET isolation как connection-local default | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-08 | Ожидание писателя строки, deadlock и повтор | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |
| TX-09 | Rollback-capable constraint error и межтабличная атомарность | Поддерживается | 1.2 | [Транзакции](../../sql/transactions/) | `test-transactions.mjs` |

TX-06 и TX-07 используют одно connection-local состояние транзакции в
embedded, TCP и CLI. Неподдерживаемый уровень изоляции отклоняется до открытия
транзакции.

## Навигация по ссылкам

| ID | Конструкция | Статус | Версия | Глава | Тест |
| --- | --- | --- | --- | --- | --- |
| NAV-01 | Путь source-FK к полю целевой строки | Поддерживается | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-02 | Общий префикс и транзитивный путь | Поддерживается | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-03 | LEFT/NULL-семантика пути | Поддерживается | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-04 | Пути в read-only контекстах SELECT | Поддерживается | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-05 | Детерминированные ошибки root и схемы | Поддерживается | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-06 | Навигация в DML | Отклоняется | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-07 | Навигация в сохранённом VIEW | Отклоняется | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-08 | Лимиты depth/path/edge 8/256/512 | С ограничениями | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |
| NAV-09 | Составной или обратный путь | Отклоняется | 1.2 | [Навигация по ссылкам](../../sql/navigable-references/) | `test-navigation.mjs` |

## Сопровождение матрицы

Новое утверждение о SQL должно в одном изменении добавить или обновить строку,
связанное объяснение и исполняемое доказательство. Перевод возможности из
«Отклоняется» или «С ограничениями» в «Поддерживается» сначала должен пройти на
ревизии целевого выпуска. Принятие токенов парсером не снимает ограничение.

Матрица описывает поведение SQL, но не все перегрузки функций, параметры
конфигурации, методы клиентов и стратегии физических планов. Они относятся к
справочной и административной частям руководства.
