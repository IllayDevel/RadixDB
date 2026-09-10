---
title: Определение схемы
description: Создание, проверка, изменение и удаление таблиц и ограничений.
---

Операторы определения данных создают и изменяют объекты базы. Таблица объединяет
именованные столбцы, их типы и ограничения. Изменения схемы влияют на последующие
чтения и записи, поэтому применяйте их как проверенные миграции, а не как
побочный эффект запуска каждого процесса приложения.

Примеры ниже используют собственные таблицы. Выполняйте блоки по порядку
в одной тестовой базе. Не используйте эти имена в базе с важными данными.

## Создание таблиц

Определение столбца содержит имя и [тип данных](../types/), а затем необязательные
ограничения. В примере используются генерируемый первичный ключ, внешний ключ,
обязательные значения, значения по умолчанию, уникальность и проверочное выражение:

```sql
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
CREATE TABLE assets (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    department_id INTEGER REFERENCES departments(id),
    serial TEXT NOT NULL UNIQUE,
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    note TEXT
);
INSERT INTO departments VALUES (1, 'ops');
INSERT INTO assets (department_id, serial) VALUES (1, 'A-1');
DESCRIBE assets;
SELECT id, department_id, serial, quantity, note FROM assets;
```

DESCRIBE сообщает имя и тип столбца, допустимость NULL, признак ключа,
значение по умолчанию и дополнительные свойства. Вставленный объект получает
сгенерированный ID 1, quantity 0 и note, равный NULL. `PRIMARY KEY` подразумевает
NOT NULL. AUTO_INCREMENT поддерживается для путей первичного ключа INTEGER
и UUID, но не является общей последовательностью для любого типа.

`CREATE TABLE IF NOT EXISTS` может превратить повторное создание в пустую
операцию. Он не приводит существующую таблицу к новому определению.
Проверяйте текущую схему и используйте явные ALTER, если нужна миграция.

## Нарушения ограничений

NOT NULL отклоняет отсутствующее обязательное значение:

```sql
INSERT INTO assets (department_id, serial) VALUES (1, NULL);
```

CHECK принимает только строки, для которых выражение не является ложным.
Эта вставка нарушает `quantity >= 0`:

```sql
INSERT INTO assets (department_id, serial, quantity) VALUES (1, 'A-2', -1);
```

Внешний ключ требует существования связанной строки:

```sql
INSERT INTO assets (department_id, serial) VALUES (99, 'A-3');
```

UNIQUE отклоняет повторное значение serial, отличное от NULL:

```sql
INSERT INTO assets (department_id, serial) VALUES (1, 'A-1');
```

Каждый из четырёх операторов должен завершиться ошибкой. Ошибка не равнозначна
успешной записи нуля строк. В явной транзакции проверьте сообщённое состояние
и при необходимости выполните откат перед продолжением. UNIQUE обычно допускает
несколько NULL; добавляйте NOT NULL, если бизнес-ключ должен присутствовать всегда.

По умолчанию внешние ключи используют ограничительное поведение.
Действия `ON DELETE` и `ON UPDATE`, например CASCADE и SET NULL, требуют
осознанного проектирования; перед миграцией проверяйте их на полной связи.
SET NULL несовместим с дочерним столбцом, запрещающим NULL.

## Изменение таблицы

ALTER TABLE позволяет добавлять и удалять столбцы, переименовывать столбец
или таблицу и изменять определение столбца в поддерживаемых формах:

```sql
CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO items VALUES (1, 'one');
ALTER TABLE items ADD COLUMN location TEXT NOT NULL DEFAULT 'warehouse';
SELECT id, label, location FROM items;
ALTER TABLE items RENAME COLUMN label TO title;
ALTER TABLE items MODIFY COLUMN title TEXT NOT NULL DEFAULT 'untitled';
ALTER TABLE items DROP COLUMN location;
ALTER TABLE items RENAME TO inventory;
DESCRIBE inventory;
SELECT id, title FROM inventory;
```

При добавлении обязательного столбца существующая строка получает `warehouse`.
После остальных изменений таблица `inventory` содержит строку `1, one`,
а DESCRIBE показывает обязательный title со значением по умолчанию `'untitled'`.

Рассматривайте ALTER как миграцию данных. Новое правило NOT NULL должно
выполняться для существующих строк, изменение типа может потребовать
преобразования, а удаление столбца уничтожает его значения. Создайте резервную
копию важных данных и проверьте миграцию на копии. Не предполагайте, что каждая
форма ALTER является дешёвой операцией только над метаданными.

Ограничения, добавленные или изменённые через ALTER, должны выполняться
для существующих и будущих строк. Вводный контракт не обещает все сочетания
PRIMARY KEY, UNIQUE и AUTO_INCREMENT в MODIFY COLUMN; при изменении структуры
ключей используйте явные индексы и проверенную миграцию.

## Транзакционный DDL и очистка

Изменения схемы и данных могут находиться в одной явной транзакции. Откат
удаляет незакоммиченную таблицу вместе со строкой:

```sql
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS inventory;
BEGIN;
CREATE TABLE staged (id INTEGER PRIMARY KEY);
INSERT INTO staged VALUES (1);
ROLLBACK;
SHOW TABLES;
```

SHOW TABLES не возвращает строк. `DROP TABLE IF EXISTS` подавляет ошибку
отсутствующей таблицы при повторяемой очистке; DROP TABLE без IF EXISTS
сообщает об отсутствующем объекте. Удаление таблицы удаляет также её локальные
метаданные индексов, но не затрагивает посторонние файлы или базы.

Выполняйте BEGIN, операторы схемы и COMMIT или ROLLBACK в одном сеансе.
Перед промышленной миграцией проверьте итоговую схему через DESCRIBE/SHOW INDEXES
и воспроизведите как успешный путь, так и откат.

Далее читайте [об индексах](../indexes/): путях доступа и дополнительной уникальности.
