---
title: RadixDB PL
description: Write bounded server-side blocks with variables, SQL, cursors and exceptions.
---

**RadixDB PL** is the procedural language built into RadixDB 1.2. Catalog
definitions spell it `LANGUAGE RADIX`. It shares the RadixDB SQL lexer,
expressions and statements, but has its own procedural binder and bounded
runtime. It is not a compatibility mode for Oracle PL/SQL or PostgreSQL
PL/pgSQL.

A definition is parsed, bound, lowered to typed IR and verified before its
catalog publication. A body is a parsed block, not a quoted SQL program.
Unsupported syntax rejects the complete definition.

## Blocks and declarations

A block has optional declarations, a required `BEGIN ... END` section and
optional exception handlers. Every declaration and procedural statement ends
with `;`; the final `END;` also terminates the routine definition.

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

Scalar types are `INTEGER`, `FLOAT`, `TEXT`, `BOOLEAN`, `TIMESTAMP`, `JSON`,
`VECTOR`, `UUID`, `DECIMAL`, `DATE` and `BYTES`. Variables are nullable unless
declared `NOT NULL`; a `CONSTANT` requires an initializer.
`table_name%ROWTYPE` binds a local record to the table's ordered descriptor.

`ARRAY<type, capacity>` is a local, homogeneous, one-based collection. Capacity
is a compile-time value in `1..=65536`. The 1.2 operations are `APPEND`,
`CLEAR`, indexing and read-only `COUNT`; arrays cannot be table columns,
arguments or results.

Nested blocks create lexical scopes. Argument, local, cursor, loop and handler
names cannot be redeclared or shadow an outer name. This makes binding stable
under refactoring.

## Procedural names inside SQL

Use a plain identifier in a procedural expression. Prefix a local, argument or
context record with `:` inside an embedded SQL statement:

```sql
IF new_status IS NULL THEN
    RAISE invalid_argument('status is required');
END IF;

UPDATE documents AS d
SET status = :new_status
WHERE d.id = :document_id;
```

An unprefixed name in SQL is resolved only as SQL. Static stored SQL rejects
`$1`, `?` and external host bindings; dynamic SQL receives its own positional
values through `USING`.

## Control flow

The language provides `IF`/`ELSIF`/`ELSE`, simple and searched `CASE`,
unconditional `LOOP`, `WHILE`, numeric `FOR`, query `FOR`, `EXIT` and
`CONTINUE`. A condition takes its branch only when it is `TRUE`; SQL `FALSE`
and `NULL` both mean not taken.

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

Numeric `FOR` also accepts a nonzero `BY` step. Query `FOR` streams its query
rather than materializing all rows. Every loop backedge checks instruction,
deadline and cancellation budgets.

## SQL statements and cardinality

Procedures and volatile functions can execute admitted SQL in their caller's
MVCC transaction. `SELECT ... INTO` and DML `RETURNING ... INTO` accept either
one row or no row; more than one row raises `too_many_rows`. With `STRICT`, zero
rows raises `no_data_found`. Without it, zero rows assigns typed NULL, which
still fails for a `NOT NULL` target.

`SQL%ROWCOUNT`, `SQL%FOUND` and `SQL%NOTFOUND` describe the last completed SQL
leaf in the current routine frame. A nested call does not overwrite the
caller's state.

## Cursors

An explicit cursor has typed parameters and a fixed query:

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

Available attributes are `%ISOPEN`, `%FOUND`, `%NOTFOUND` and `%ROWCOUNT`.
`FETCH` targets must match the ordered result descriptor, including nullability.
A cursor belongs to its frame and closes on normal exit, error or cancellation;
there are no holdable cursors across transaction boundaries.

A routine can return a scalar with `RETURN expression`, return from a void
procedure with `RETURN`, or produce a bounded table result with `RETURN NEXT`
and `RETURN QUERY`. Client `Rows` iteration can still report a deferred error;
low-level `advance()` users must inspect `Rows::error()` after `false`.

## Dynamic SQL

`EXECUTE` evaluates one SQL string, parses it with the ordinary RadixDB parser
and runs it through the same executor, transaction, principal and budget:

```sql
EXECUTE
    'INSERT INTO events (id, value) VALUES ($1, $2)'
USING input_id, total;
```

The 1.2 allowlist is one query, `INSERT`, `UPDATE`, `DELETE` or `CALL`, including
their admitted `WITH` forms. `INTO [STRICT]` receives one result row. Put caller
data only in positional parameters supplied by `USING`. For a deliberately
dynamic object name, convert it to the separate typed identifier value:

```sql
EXECUTE 'DELETE FROM ' || SQL_IDENTIFIER(table_name);
```

`SQL_IDENTIFIER` validates the identifier and applies canonical quoting, so
quotes, comments and statement terminators remain data. Its result may be
concatenated into dynamic SQL but cannot be used as an ordinary SQL value
without an explicit conversion.

Multiple statements, DDL and transaction control are rejected before planning;
dynamic DDL reports `PL_VERIFY_DYNAMIC_DDL_NOT_SUPPORTED`. `EXECUTE` cannot
hide DML inside an immutable or stable function or bypass ACL checks.

## Exceptions and atomicity

`RAISE kind(arguments)` uses a closed set of diagnostic kinds. Bare `RAISE`
rethrows only from a handler. Handlers are matched in source order, and
`OTHERS` must be last:

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

Every block with `EXCEPTION` owns an internal savepoint. Before a handler runs,
SQL and trigger effects created inside that block are rolled back, its cursors
are closed and SQL status is restored. The user cannot address this savepoint.
Explicit `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and `RELEASE`
are forbidden inside a stored body; lexical `BEGIN` does not open a transaction.

The executable `doc/examples/programming/server_programming.rs` verifies
control flow, dynamic SQL, a handled UNIQUE error and an explicit cursor against
the frozen 1.2 baseline. Continue with [Functions and Procedures](../routines/).
