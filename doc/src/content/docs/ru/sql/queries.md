---
title: Запросы к данным
description: Чтение, соединение, группировка и ранжирование строк через SELECT.
---

`SELECT` строит результат из строк таблиц. Запрос может отбирать строки,
соединять отношения, формировать группы, вычислять оконные значения и задавать
итоговый порядок. В этой главе операции последовательно собираются от небольшой
схемы до составных аналитических запросов.

Выполняйте блоки по порядку в отдельной тестовой базе. Примеры используют
собственные три таблицы.

```sql
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    department_id INTEGER REFERENCES departments(id),
    name TEXT NOT NULL,
    salary INTEGER NOT NULL
);
CREATE TABLE bonuses (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id),
    amount INTEGER NOT NULL
);
INSERT INTO departments (id, name) VALUES
    (1, 'Engineering'), (2, 'Support'), (3, 'Sales');
INSERT INTO employees (id, department_id, name, salary) VALUES
    (1, 1, 'Alice', 120), (2, 1, 'Boris', 90),
    (3, 2, 'Clara', 80), (4, NULL, 'Dan', 70);
INSERT INTO bonuses (id, employee_id, amount) VALUES
    (1, 1, 10), (2, 1, 15), (3, 3, 5);
```

## Проекция, фильтрация и порядок

Список SELECT определяет столбцы результата. `WHERE` исключает строки до
возврата списка SELECT. `ORDER BY` сортирует результат, а `LIMIT` и `OFFSET`
выбирают окно из этого порядка.

```sql
SELECT id, name, salary
FROM employees
WHERE salary >= 80
ORDER BY salary DESC, id
LIMIT 2 OFFSET 1;
```

Результат содержит Boris и Clara. Дополнительный ключ `id` делает порядок
устойчивым при одинаковой зарплате. Без `ORDER BY` порядок строк не определён,
поэтому разбиение через `LIMIT` или `OFFSET` не является повторяемым. `ASC`
применяется по умолчанию; `DESC` меняет направление. Если важно положение NULL,
указывайте `NULLS FIRST` или `NULLS LAST`.

Псевдонимы столбцов принадлежат выходу и могут использоваться итоговым
`ORDER BY`. Когда несколько входов содержат одно имя столбца, используйте
квалифицированные имена наподобие `e.id`.

## Соединение отношений

Внутреннее соединение сохраняет совпавшие пары. Внешнее соединение также
сохраняет строки одной или обеих сторон и заполняет отсутствующую сторону NULL.
RadixDB поддерживает `INNER`, `LEFT`, `RIGHT`, `FULL` и `CROSS JOIN`.

```sql
SELECT e.name AS employee, d.name AS department
FROM employees AS e
INNER JOIN departments AS d ON d.id = e.department_id
ORDER BY e.id;

SELECT e.name AS employee, d.name AS department
FROM employees AS e
LEFT JOIN departments AS d ON d.id = e.department_id
ORDER BY e.id;

SELECT d.id AS department_id, d.name AS department, e.name AS employee
FROM employees AS e
RIGHT JOIN departments AS d ON d.id = e.department_id
ORDER BY department_id, employee;

SELECT e.id AS employee_id, e.name AS employee,
       d.id AS department_id, d.name AS department
FROM employees AS e
FULL JOIN departments AS d ON d.id = e.department_id
ORDER BY employee_id NULLS LAST, department_id NULLS LAST;

SELECT e.name, d.name AS department
FROM employees AS e
CROSS JOIN departments AS d
WHERE e.id = 1
ORDER BY department;
```

Левое соединение включает Dan с отделом NULL. Правое соединение включает Sales
с сотрудником NULL, а полное соединение включает обе несовпавшие строки.
Декартово соединение сначала формирует все пары; его `WHERE` затем сохраняет
три пары для Alice. До применения CROSS JOIN к большим входам оцените это
умножение.

`JOIN ... USING (column)` доступен, когда оба входа намеренно публикуют ключ
с одинаковым именем. `NATURAL JOIN` сравнивает все одноимённые столбцы, включая
добавленные позднее, поэтому для долговечной схемы безопаснее явный `ON`
или `USING`.

## Группы и агрегаты

Агрегатные функции сворачивают строки каждой группы. `WHERE` фильтрует входные
строки, а `HAVING` — уже сформированные группы.

```sql
SELECT d.name AS department,
       COUNT(e.id) AS headcount,
       SUM(e.salary) AS payroll,
       AVG(e.salary) AS average_salary
FROM departments AS d
LEFT JOIN employees AS e ON e.department_id = d.id
GROUP BY d.id, d.name
HAVING COUNT(e.id) > 0
ORDER BY payroll DESC, department;
```

Запрос возвращает Engineering с headcount 2 и payroll 210, затем Support
с headcount 1 и payroll 80. `COUNT(e.id)` игнорирует NULL, добавленный для
отдела без сотрудника. В сгруппированном запросе каждое выбранное выражение
должно быть агрегатом или входить в ключи группировки.

## Подзапросы

Скалярный подзапрос предоставляет одно значение, `IN` сравнивает со множеством
результата подзапроса, а `EXISTS` проверяет наличие хотя бы одной коррелированной
строки.

```sql
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC;

SELECT name
FROM employees
WHERE id IN (SELECT employee_id FROM bonuses)
ORDER BY name;

SELECT e.name
FROM employees AS e
WHERE EXISTS (
    SELECT 1 FROM bonuses AS b
    WHERE b.employee_id = e.id AND b.amount >= 10
)
ORDER BY name;
```

Три результата: Alice; Alice и Clara; затем Alice. Скалярный подзапрос должен
возвращать не более одной строки. `EXISTS` обычно предпочтительнее, когда важно
только наличие строки.

Подзапрос в `FROM` действует как производное отношение и должен иметь
псевдоним. В примере ключ обоих входов намеренно получает одно имя, чтобы
`USING` мог объединить его.

```sql
SELECT e.name AS employee, d.name AS department
FROM employees AS e
JOIN (
    SELECT id AS department_id, name FROM departments
) AS d USING (department_id)
ORDER BY e.id;
```

## Общие табличные выражения

Общее табличное выражение даёт имя запросу на время следующего оператора.
Используйте его, когда промежуточный результат делает структуру запроса
понятнее; постоянной таблицей оно не является.

```sql
WITH department_totals AS (
    SELECT department_id, SUM(salary) AS payroll
    FROM employees
    WHERE department_id IS NOT NULL
    GROUP BY department_id
)
SELECT d.name, t.payroll
FROM department_totals AS t
JOIN departments AS d ON d.id = t.department_id
ORDER BY t.payroll DESC;
```

Рекурсивному CTE нужны начальная часть, `UNION ALL`, рекурсивная часть и условие
завершения. Объявляйте выходные столбцы, если рекурсивная часть должна ссылаться
на их имена.

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 4
)
SELECT n FROM numbers ORDER BY n;
```

Результат содержит числа от 1 до 4. Рекурсивная часть, которая никогда
не становится пустой, остаётся ошибочным запросом, хотя движок имеет защитный
предел итераций; всегда явно задавайте и проверяйте условие завершения.

## Оконные функции

Оконная функция вычисляет значение по связанным строкам, не сворачивая их
в одну группу. `PARTITION BY` формирует независимые окна, а оконный `ORDER BY`
задаёт порядок внутри каждой секции.

```sql
WITH ranked_employees AS (
    SELECT name, department_id, salary,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(department_id, -1)
               ORDER BY salary DESC, id
           ) AS position,
           SUM(salary) OVER (
               PARTITION BY COALESCE(department_id, -1)
           ) AS department_payroll
    FROM employees
)
SELECT name, department_id, salary, position, department_payroll
FROM ranked_employees
WHERE position <= 2
ORDER BY department_id NULLS LAST, position;
```

`COALESCE` выделяет сотрудникам без отдела собственную секцию.
`ROW_NUMBER` назначает позицию внутри каждой секции; оконный `SUM` повторяет
её фонд зарплаты в каждой строке сотрудника. RadixDB также регистрирует функции
ранжирования и навигации `RANK`, `DENSE_RANK`, `NTILE`, `LEAD`, `LAG`,
`FIRST_VALUE` и `LAST_VALUE`. Результат зависит от полного определения окна,
особенно порядка и рамки.

## Текущие границы

Следующие ограничения входят в проверенную поверхность запросов версии 1.2.

Ускоренный путь partition по индексу представляет NULL полноценной секцией. Его
результат одинаков для hot memory, cold artifacts и crash/reopen, включая явные
и автоматически созданные foreign-key indexes. Выражение
`PARTITION BY COALESCE(department_id, -1)` остаётся полезным, когда приложению
намеренно нужен non-NULL sentinel вместо SQL-семантики NULL partition.

Рекурсивные CTE принимают `UNION ALL`, но не устраняющий повторы `UNION`:

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION
    SELECT n + 1 FROM numbers WHERE n < 4
)
SELECT n FROM numbers;
```

Производные таблицы `LATERAL` не принимаются. Используйте коррелированный
скалярный подзапрос или `EXISTS` либо перепишите отношение как обычный JOIN:

```sql
SELECT e.name, x.amount
FROM employees AS e
CROSS JOIN LATERAL (
    SELECT amount FROM bonuses WHERE employee_id = e.id
) AS x;
```

`QUALIFY` не принимается. Поместите оконный запрос в CTE или производную таблицу
и фильтруйте опубликованный оконный столбец во внешнем `WHERE`, как показано
выше:

```sql
SELECT name,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS position
FROM employees
QUALIFY position <= 2;
```

После группировки итоговая сортировка может использовать выражения группировки,
присутствующие в результате, или их выходные псевдонимы. Квалифицированное
выражение только из входа наподобие `d.id` ниже недоступно после формирования
агрегатного результата:

```sql
SELECT d.name AS department, COUNT(e.id) AS headcount
FROM departments AS d
LEFT JOIN employees AS e ON e.department_id = d.id
GROUP BY d.id, d.name
ORDER BY d.id;
```

Опубликуйте этот ключ либо сортируйте по `department` или другому выходному
выражению. Отклоняемые примеры являются намеренными проверками совместимости,
а не командами для миграции.
