---
title: Навигация по ссылкам
description: Переход по объявленным внешним ключам в read-only SQL без повторения условий JOIN.
---

Навигация по ссылкам — это нотация RadixDB для перехода от строки с внешним
ключом к одной связанной строке. Движок выводит каждый переход из схемы и
планирует обращения внутри одного оператора. В исходном столбце не хранится
объект, а клиент не выполняет отдельный запрос для каждой строки.

## Схема примеров

В примерах используются две последовательные связи: сотрудник ссылается на
отдел, а отдел может ссылаться на профиль.

```sql
CREATE TABLE profiles (
    id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL
);
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    cost_center TEXT NOT NULL,
    profile_id INTEGER REFERENCES profiles(id)
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    salary INTEGER NOT NULL,
    department_id INTEGER REFERENCES departments(id)
);

INSERT INTO profiles VALUES
    (1, 'Finance profile'),
    (2, 'Engineering profile');
INSERT INTO departments VALUES
    (10, 'Finance', 'FIN', 1),
    (20, 'Engineering', 'ENG', 2),
    (30, 'Unclassified', 'UNC', NULL);
INSERT INTO employees VALUES
    (100, 'Alice', 120, 10),
    (101, 'Bob', 90, 10),
    (102, 'Carol', 110, 20),
    (103, 'Dave', 70, NULL),
    (104, 'Eve', 80, 30);
```

Физически `department_id` хранит обычное целое число. Выбор этого столбца
по-прежнему возвращает ключ 10, 20, 30 или NULL.

## Один переход

Полный путь начинается с видимого псевдонима таблицы, продолжается столбцом FK
и заканчивается обычным столбцом связанной таблицы.

```sql
SELECT e.id, e.name,
       e.department_id.label AS department,
       e.department_id.cost_center AS code
FROM employees AS e
ORDER BY e.id;
```

Первые две строки возвращают Finance и FIN, Carol — Engineering и ENG, Dave —
два NULL, а Eve — Unclassified и UNC. Наблюдаемый результат эквивалентен явному
LEFT JOIN:

```sql
SELECT e.id, e.name,
       d.label AS department,
       d.cost_center AS code
FROM employees AS e
LEFT JOIN departments AS d ON e.department_id = d.id
ORDER BY e.id;
```

Навигация сокращает запись объявленной связи, но не заменяет соединения общего
вида. Используйте `JOIN`, если условие не является равенством FK или одна
целевая строка должна вернуть коллекцию исходных строк.

## Корни, псевдонимы и общие префиксы

Форма с псевдонимом безопаснее в запросе с несколькими входами. Псевдоним можно
опустить, когда только одно видимое отношение содержит подходящий навигационный
FK. Несколько конечных полей с общим префиксом используют одно запланированное
ребро и одно обращение к целевой строке.

```sql
SELECT id,
       department_id.label AS department,
       department_id.cost_center AS code
FROM employees
ORDER BY id;
```

Если `department_id` подходит нескольким экземплярам отношений, сокращение
неоднозначно. RadixDB не выбирает первое совпадение; укажите нужный псевдоним.

## Цепочки и NULL

Каждый промежуточный компонент должен быть столбцом внешнего ключа. Последний
компонент обозначает возвращаемое значение. Общие префиксы планируются один раз,
даже если пути заканчиваются разными полями.

```sql
SELECT e.id, e.name,
       e.department_id.label AS department,
       e.department_id.profile_id.display_name AS profile
FROM employees AS e
ORDER BY e.id;
```

Каждый переход имеет LEFT-семантику. NULL в `department_id` превращает остаток
пути Dave в NULL. Eve находит отдел, но NULL в его `profile_id` превращает в
NULL только профиль. Nullable-конечный столбец также возвращает обычный
типизированный NULL. Сама навигация не удаляет исходную строку.

Путь можно использовать в предикате. Продолжает действовать обычная трёхзначная
логика SQL; для выбора отсутствующих связей применяйте `IS NULL`.

```sql
SELECT e.id, e.name
FROM employees AS e
WHERE e.department_id.profile_id.display_name = 'Finance profile'
   OR e.department_id.profile_id.display_name IS NULL
ORDER BY e.id;
```

Запрос возвращает Alice, Bob, Dave и Eve. Профиль Carol существует, но не
соответствует условию.

## Группировка и другие контексты SELECT

Навигация доступна в read-only выражениях SELECT: проекции, `WHERE`,
`JOIN ... ON`, `GROUP BY`, `HAVING`, `ORDER BY`, аргументах агрегатных и оконных
функций, `CASE`, скалярных функциях, CTE, производных таблицах и read-only
подзапросах. Все вхождения связываются в лексической области запроса.

```sql
SELECT e.department_id.label AS department,
       COUNT(*) AS headcount,
       SUM(e.salary) AS payroll
FROM employees AS e
GROUP BY e.department_id.label
HAVING SUM(e.salary) >= 80
ORDER BY department NULLS LAST;
```

Результат содержит Engineering с 1 и 110, Finance с 2 и 210, а также
Unclassified с 1 и 80. Повторение пути в проекции и группировке не создаёт
отдельных клиентских запросов. `SELECT *` не раскрывает связанные столбцы;
каждое конечное поле нужно назвать явно.

## EXPLAIN

Используйте `EXPLAIN ANALYZE`, чтобы увидеть связанный граф ссылок и физическую
стратегию, выбранную для фактической формы данных.

```sql
EXPLAIN ANALYZE
SELECT e.department_id.label AS department, COUNT(*)
FROM employees AS e
GROUP BY e.department_id.label;
```

План содержит раздел `Reference Navigation`, `Semantics: LEFT`, счётчики
запланированных и исполненных путей, пакеты поиска, проверку целостности и
фактическую стратегию. Стратегия может меняться с кардинальностью, состоянием
хранилища и контекстом запроса. EXPLAIN сообщает агрегированные счётчики, но
не выводит значения ключей поиска и SQL-параметров.

## Ошибки имён и схемы

Связывание навигации детерминировано и возвращает стабильные категории ошибок.
В следующем сокращении есть два возможных корня:

```sql
SELECT department_id.label
FROM employees AS e1
JOIN employees AS e2 ON e1.id = e2.id;
```

Результат — `NAVIGATION_AMBIGUOUS_ROOT`. Путь через обычный столбец не
интерпретируется как свойство объекта:

```sql
SELECT e.name.value FROM employees AS e;
```

Результат — `NAVIGATION_NOT_A_REFERENCE`. Отсутствующее конечное поле также
отклоняется при связывании:

```sql
SELECT e.department_id.missing FROM employees AS e;
```

Результат — `NAVIGATION_TARGET_COLUMN_NOT_FOUND`. Если связанные объекты схемы
подготовленного оператора изменились, запрос повторно связывается или закрывается
ошибкой; устаревший descriptor не исполняется.

## Граница только для чтения

Навигационный путь никогда не является целью присваивания и отклоняется во всех
контекстах записи, включая фильтры, `RETURNING`, `ON CONFLICT`, write-подзапросы
и `CREATE TABLE AS SELECT`.

```sql
UPDATE employees
SET name = 'blocked'
WHERE department_id.label = 'Finance';
```

Возвращается `NAVIGATION_READ_ONLY`, строки не изменяются. Явно укажите целевую
таблицу и область записи:

```sql
UPDATE departments
SET label = 'Finance and Legal'
WHERE id IN (
    SELECT department_id FROM employees WHERE id = 100
);
SELECT e.department_id.label
FROM employees AS e
WHERE e.id = 100;
```

SELECT возвращает Finance and Legal. Обратной навигации по коллекции, неявного
сохранения графа, угадывания владельца и неявного каскада нет.

Сохранённые определения VIEW с навигацией не поддерживаются в 1.2:

```sql
CREATE VIEW employee_departments AS
SELECT e.id, e.department_id.label
FROM employees AS e;
```

Оператор завершается `NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE` и не создаёт
представление. Обычные VIEW без навигационных путей доступны.

## Требования схемы и лимиты

Каждый переход требует один source-столбец FK и один target-столбец совместимого
физического типа в той же базе. Целью должен быть первичный ключ или столбец
`UNIQUE NOT NULL`. Nullable source FK разрешён. Nullable unique target может
участвовать в обычном FK, но навигация по нему отклоняется: уникальность NULL
не доказывает наличие не более одной целевой строки.

RadixDB 1.2 не принимает DDL составного внешнего ключа. Также отсутствуют пути
между базами и обратный переход one-to-many. Один путь ограничен 8 переходами,
один оператор — 256 связанными путями, а скомпилированный граф — 512 различными
рёбрами. Превышение лимита возвращает
`NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE`.

Ненулевая ссылка без цели или несколько целевых строк означают нарушение
инварианта целостности. Исполнение закрывается ошибкой
`REFERENCE_TARGET_MISSING` или `REFERENCE_TARGET_NOT_UNIQUE`, а не возвращает
произвольную строку.
