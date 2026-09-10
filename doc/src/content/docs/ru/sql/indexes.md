---
title: Индексы
description: Создание, проверка и удаление обычных, уникальных, составных и частичных индексов.
---

Индекс является дополнительной структурой доступа, поддерживаемой вместе
с таблицей. Он может уменьшить работу подходящего запроса, а уникальный индекс
способен контролировать правило ключа. Каждый индекс занимает место и добавляет
работу при записи. Создавайте его для измеренного пути доступа или ограничения,
а не для каждого столбца по умолчанию.

Выполняйте блоки ниже по порядку в отдельной тестовой базе. Примеры используют
собственную таблицу `contacts`.

## Создание и проверка индексов

RadixDB поддерживает методы BTREE, HASH, BITMAP и предназначенный для векторов
HNSW. Если USING отсутствует, движок может выбрать метод по умолчанию.
Указывайте метод, когда схема зависит от определённого контракта.

```sql
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    email TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    deleted_at TIMESTAMP,
    embedding VECTOR(3)
);
INSERT INTO contacts (id, tenant_id, email) VALUES
    (1, 10, 'owner@example.test'),
    (2, 10, NULL),
    (3, 10, NULL),
    (4, 20, 'other@example.test');
CREATE INDEX contacts_tenant_email_idx
    ON contacts (tenant_id, email) USING BTREE;
CREATE INDEX contacts_active_idx
    ON contacts (active) USING BITMAP;
CREATE UNIQUE INDEX contacts_email_active_uidx
    ON contacts (email) USING HASH
    WHERE deleted_at IS NULL;
SHOW INDEXES FROM contacts;
```

SHOW INDEXES сообщает индекс первичного ключа и три явных индекса: имена,
столбцы, метод, уникальность и параметры. Составной индекс хранит упорядоченный
список столбцов. Его применимость зависит от условий и сортировки, соответствующих
поддерживаемому начальному префиксу; одного упоминания последующего столбца
недостаточно, чтобы гарантировать выбор индекса.

HASH предназначен для поддерживаемых путей равенства, BITMAP — для поддерживаемых
значений с низкой кардинальностью, BTREE — для поддерживаемых условий равенства,
порядка и диапазонов. Это возможности, а не обещание выбора индекса для любого
синтаксически похожего запроса. Проверяйте настоящий запрос через EXPLAIN
на репрезентативной схеме.

## Extension operator classes

Trusted native extension может связать comparison operators, operator class и
bounded planner support с core-owned index method. Тогда column definition явно
указывает operator class:

```sql
CREATE INDEX asset_point_idx
ON assets (position geo.point_btree) USING BTREE;
```

Extension кодирует canonical index keys и может предложить bounded candidate
ranges. RadixDB по-прежнему владеет MVCC visibility, index pages, scan
execution, publication и recovery. Если planner-support descriptor требует
recheck, исходный predicate всегда вычисляется после candidate scan.

SDK 1.2 поддерживает external B-tree, hash и bitmap classes. Authoring external
HNSW зарезервирован, но отклоняется. Недоступность exact plugin package переводит
database с его objects в restricted diagnostic mode вместо чтения index keys
другим codec. См. [«Команды native extensions»](../../reference/sql/extensions/).

## Уникальные и частичные индексы

UNIQUE отклоняет повторные значения ключа, отличные от NULL. В начальных данных
обе строки с email=NULL допустимы: обычная уникальность SQL не считает один NULL
равным другому.

Частичный уникальный индекс включает только строки, для которых
`deleted_at IS NULL` истинно. Он задаёт уникальность среди действующих контактов,
позволяя снова использовать email после отметки старой строки как удалённой.
Эта повторная активная строка отклоняется:

```sql
INSERT INTO contacts (id, tenant_id, email)
VALUES (5, 30, 'owner@example.test');
```

После выхода старой строки из частичного индекса то же бизнес-значение
можно вставить снова:

```sql
UPDATE contacts SET deleted_at = '2026-09-08T00:00:00Z' WHERE id = 1;
INSERT INTO contacts (id, tenant_id, email)
VALUES (5, 30, 'owner@example.test');
SELECT id, email, deleted_at IS NULL AS current
FROM contacts
WHERE email = 'owner@example.test'
ORDER BY id;
```

Запрос возвращает ID 1 с current=false и ID 5 с current=true.
Возврат deleted_at строки 1 в NULL при действующей строке 5 нарушит уникальность.

Предикат частичного индекса должен быть детерминированным и вычисляться
по индексируемой строке. Вводные поддерживаемые формы включают литералы,
неквалифицированные столбцы, сравнения, IS NULL/IS NOT NULL, AND/OR/NOT,
BETWEEN, IN со списком литералов и LIKE с литеральным шаблоном.
Не помещайте в предикат подзапросы, агрегаты, параметры времени выполнения
или ссылки на другие таблицы.

Частичные HNSW-индексы не поддерживаются:

```sql
CREATE INDEX contacts_embedding_active_idx
    ON contacts (embedding) USING HNSW
    WHERE active = true;
```

Оператор должен завершиться явной ошибкой. Настройка HNSW и семантика
векторных запросов относятся к отдельному справочнику; обычный индекс
не является доказательством работоспособности этих параметров.

## Безопасность планировщика

Планировщик может применить частичный индекс, только если способен доказать,
что строки запроса удовлетворяют предикату индекса. Следующий запрос содержит
предикат явно:

```sql
EXPLAIN SELECT id FROM contacts
WHERE email = 'owner@example.test' AND deleted_at IS NULL;
```

На проверенном наборе горячих строк план выбирает
`contacts_email_active_uidx`. Следующий запрос, напротив, должен видеть
также удалённые строки:

```sql
EXPLAIN SELECT id FROM contacts
WHERE email = 'owner@example.test';
```

Проверенный план использует последовательное сканирование и сообщает
`Partial Index Eligibility: no_proven_partial_index`. Запасной путь может быть
медленнее, но применение частичного индекса без доказанного предиката было бы
ошибочным. Текст плана и физический источник могут измениться после checkpoint;
сначала проверяйте результат запроса, а EXPLAIN используйте как диагностику
конкретного состояния.

## Повторное создание, переименование и удаление

IF NOT EXISTS подавляет только повтор совместимого определения:

```sql
CREATE INDEX IF NOT EXISTS contacts_tenant_email_idx
    ON contacts (tenant_id, email) USING BTREE;
```

Повторное использование имени с другими столбцами или методом является ошибкой,
а не миграцией:

```sql
CREATE INDEX IF NOT EXISTS contacts_tenant_email_idx
    ON contacts (email) USING HASH;
```

Переименование сохраняет определение индекса. DROP INDEX в этой версии требует
как имя индекса, так и имя таблицы:

```sql
ALTER INDEX contacts_tenant_email_idx RENAME TO contacts_scope_idx;
SHOW INDEXES FROM contacts;
DROP INDEX contacts_scope_idx ON contacts;
SHOW INDEXES FROM contacts;
```

Первый SHOW содержит `contacts_scope_idx`, второй уже нет. Первичный ключ,
bitmap-индекс и частичный уникальный индекс сохраняются.
`DROP INDEX contacts_scope_idx` без `ON contacts` отклоняется.

Создание и удаление индексов изменяет схему. Проверяйте их в транзакции,
если важна атомарная публикация вместе с DDL/DML, и подтверждайте результат
через SHOW INDEXES после commit или rollback. Не изменяйте перестраиваемые
файлы индексов или метаданные таблиц напрямую.
