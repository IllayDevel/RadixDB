---
title: RadixDB PL
description: Ограниченные серверные блоки с переменными, SQL, курсорами и исключениями.
---

**RadixDB PL** является встроенным процедурным языком RadixDB 1.2. В catalog
definitions он записывается как `LANGUAGE RADIX`. Язык использует общий lexer,
выражения и statements RadixDB SQL, но имеет собственный procedural binder и
bounded runtime. Это не режим совместимости с Oracle PL/SQL или PostgreSQL
PL/pgSQL.

До публикации в catalog definition полностью разбирается, связывается,
понижается в typed IR и проверяется. Body является parsed block, а не строкой с
SQL-программой. Неподдерживаемый синтаксис отклоняет definition целиком.

## Блоки и объявления

Block содержит необязательные declarations, обязательную секцию
`BEGIN ... END` и необязательные handlers. Каждое declaration и procedural
statement завершается `;`; последний `END;` также завершает routine definition.

```sql
DECLARE
    maximum_rows CONSTANT INTEGER := 1000;
    total INTEGER NOT NULL DEFAULT 0;
    document_ids ARRAY<INTEGER, 32>;
BEGIN
    document_ids.APPEND(7);
    total := document_ids[1];
END;
```

Scalar types: `INTEGER`, `FLOAT`, `TEXT`, `BOOLEAN`, `TIMESTAMP`, `JSON`,
`VECTOR`, `UUID`, `DECIMAL`, `DATE` и `BYTES`. Variables nullable по умолчанию;
для запрета NULL нужен `NOT NULL`. `CONSTANT` требует initializer.
`table_name%ROWTYPE` связывает local record с ordered descriptor таблицы.

`ARRAY<type, capacity>` является локальной однородной one-based collection.
Capacity задаётся при компиляции в диапазоне `1..=65536`. В 1.2 доступны
`APPEND`, `CLEAR`, indexing и read-only `COUNT`; arrays нельзя использовать как
столбцы таблицы, arguments или results.

Nested blocks создают lexical scopes. Имена arguments, locals, cursors, loops
и handlers нельзя объявлять повторно или shadow из внешнего scope. Это
сохраняет binding при refactoring.

## Procedural names внутри SQL

В procedural expression используйте обычный identifier. В embedded SQL
добавляйте `:` к local, argument или context record:

```sql
IF new_status IS NULL THEN
    RAISE invalid_argument('status is required');
END IF;

UPDATE documents AS d
SET status = :new_status
WHERE d.id = :document_id;
```

Имя без prefix внутри SQL разрешается только как SQL name. Static stored SQL
отклоняет `$1`, `?` и внешние host bindings; dynamic SQL получает собственные
позиционные значения через `USING`.

## Управление потоком

Язык предоставляет `IF`/`ELSIF`/`ELSE`, простой и searched `CASE`, обычный
`LOOP`, `WHILE`, numeric `FOR`, query `FOR`, `EXIT` и `CONTINUE`. Ветка
выбирается только для условия `TRUE`; SQL `FALSE` и `NULL` означают not taken.

```sql
WHILE counter < limit_value LOOP
    counter := counter + 1;
    CONTINUE WHEN counter = 2;
    total := total + counter;
END LOOP;

FOR item IN REVERSE 10 TO 1 LOOP
    EXIT WHEN item < 5;
END LOOP;
```

Numeric `FOR` также принимает ненулевой шаг `BY`. Query `FOR` выполняет
streaming, не materialize всех rows. На каждом loop backedge проверяются
instruction, deadline и cancellation budgets.

## SQL statements и cardinality

Procedures и volatile functions могут выполнять разрешённый SQL в MVCC
transaction вызывающего кода. `SELECT ... INTO` и DML `RETURNING ... INTO`
принимают одну строку или отсутствие строки; больше одной приводит к
`too_many_rows`. С `STRICT` ноль строк приводит к `no_data_found`. Без него
присваивается typed NULL, что всё равно ошибочно для `NOT NULL` target.

`SQL%ROWCOUNT`, `SQL%FOUND` и `SQL%NOTFOUND` описывают последний завершённый SQL
leaf текущего routine frame. Nested call не перезаписывает state caller.

## Курсоры

Explicit cursor имеет typed parameters и фиксированный query:

```sql
DECLARE
    CURSOR values_cursor(max_id INTEGER) FOR
        SELECT value FROM events WHERE id <= :max_id ORDER BY id;
    current_value INTEGER NOT NULL := 0;
BEGIN
    OPEN values_cursor(20);
    LOOP
        FETCH values_cursor INTO current_value;
        EXIT WHEN values_cursor%NOTFOUND;
    END LOOP;
    CLOSE values_cursor;
END;
```

Доступны attributes `%ISOPEN`, `%FOUND`, `%NOTFOUND` и `%ROWCOUNT`. Targets
`FETCH` должны совпадать с ordered result descriptor, включая nullability.
Cursor принадлежит frame и закрывается при normal exit, error или cancellation;
holdable cursors через transaction boundary отсутствуют.

Routine возвращает scalar через `RETURN expression`, выходит из void procedure
через `RETURN` либо формирует bounded table result через `RETURN NEXT` и
`RETURN QUERY`. Итерация client `Rows` всё ещё может сообщить deferred error;
пользователь low-level `advance()` обязан проверить `Rows::error()` после
`false`.

## Dynamic SQL

`EXECUTE` вычисляет одну SQL-строку, разбирает обычным parser RadixDB и запускает
через тот же executor, transaction, principal и budget:

```sql
EXECUTE
    'INSERT INTO events (id, value) VALUES ($1, $2)'
USING input_id, total;
```

Allowlist 1.2 содержит один query, `INSERT`, `UPDATE`, `DELETE` или `CALL`,
включая разрешённые `WITH` forms. `INTO [STRICT]` принимает одну result row.
Данные caller передавайте только позиционными параметрами через `USING`. Если
object name намеренно динамический, преобразуйте его в отдельное typed
identifier value:

```sql
EXECUTE 'DELETE FROM ' || SQL_IDENTIFIER(table_name);
```

`SQL_IDENTIFIER` проверяет identifier и применяет canonical quoting, поэтому
quotes, comments и statement terminators остаются данными. Результат можно
конкатенировать в dynamic SQL, но нельзя использовать как обычное SQL value без
явного преобразования.

Несколько statements, DDL и transaction control отклоняются до planning;
dynamic DDL возвращает `PL_VERIFY_DYNAMIC_DDL_NOT_SUPPORTED`. `EXECUTE` не
может спрятать DML в immutable или stable function либо обойти ACL checks.

## Исключения и атомарность

`RAISE kind(arguments)` использует закрытый набор diagnostic kinds. Bare
`RAISE` повторно выбрасывает ошибку только из handler. Handlers проверяются в
source order, а `OTHERS` должен быть последним:

```sql
BEGIN
    INSERT INTO events VALUES (:input_id, 100);
    INSERT INTO events VALUES (:input_id, 200);
EXCEPTION
    WHEN unique_violation THEN
        INSERT INTO events VALUES (:input_id, 300);
    WHEN OTHERS THEN
        RAISE;
END;
```

Каждый block с `EXCEPTION` владеет внутренним savepoint. До запуска handler
откатываются SQL и trigger effects внутри блока, закрываются его cursors и
восстанавливается SQL status. Пользователь не может адресовать этот savepoint.
Явные `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` и `RELEASE` внутри
stored body запрещены; lexical `BEGIN` не открывает transaction.

Исполняемый `doc/examples/programming/server_programming.rs` проверяет
control flow, dynamic SQL, обработанную UNIQUE error и explicit cursor на
зафиксированной базе 1.2. Далее: [«Функции и процедуры»](../routines/).
