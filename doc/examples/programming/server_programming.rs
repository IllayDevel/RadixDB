use std::thread;
use std::time::Duration;

use radixdb_client::{ClientError, Connection, Cursor, ExecuteResult, Row, WireValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .expect("usage: server_programming <address> <database>");
    let database = std::env::args()
        .nth(2)
        .expect("usage: server_programming <address> <database>");
    let mut connection = Connection::connect(address)?;
    connection.authenticate("root", None)?;
    connection.select_database(database)?;

    command(connection.execute(
        "CREATE TABLE docs_pl_events (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
        )",
    )?)?;
    command(connection.execute(
        "CREATE FUNCTION docs_add(
            left_value INTEGER NOT NULL,
            right_value INTEGER NOT NULL DEFAULT 1
        ) RETURNS INTEGER NOT NULL
        LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS
        BEGIN
            RETURN left_value + right_value;
        END;",
    )?)?;
    assert_eq!(scalar_i64(&mut connection, "SELECT docs_add(41)")?, 42);

    let rejected = connection
        .execute(
            "CREATE FUNCTION docs_bad_write() RETURNS INTEGER NOT NULL
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS
             BEGIN
                 INSERT INTO docs_pl_events VALUES (99, 99);
                 RETURN 99;
             END;",
        )
        .expect_err("IMMUTABLE DML must fail admission");
    assert!(matches!(rejected, ClientError::Server(_)));
    assert!(rejected.to_string().contains("PL_VERIFY_CAPABILITY_DENIED"));

    command(connection.execute(
        "CREATE PROCEDURE docs_flow(
            input_id INTEGER NOT NULL,
            limit_value INTEGER NOT NULL
        ) LANGUAGE RADIX SECURITY INVOKER AS
        DECLARE
            total INTEGER NOT NULL := 0;
            counter INTEGER NOT NULL := 0;
        BEGIN
            WHILE counter < limit_value LOOP
                counter := counter + 1;
                CONTINUE WHEN counter = 2;
                total := total + counter;
            END LOOP;
            EXECUTE
                'INSERT INTO docs_pl_events (id, value) VALUES ($1, $2)'
            USING input_id, total;
        END;",
    )?)?;
    command(connection.execute("CALL docs_flow(1, 3)")?)?;
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT value FROM docs_pl_events WHERE id = 1",
        )?,
        4
    );

    command(connection.execute(
        "CREATE PROCEDURE docs_exception(input_id INTEGER NOT NULL)
         LANGUAGE RADIX SECURITY INVOKER AS
         BEGIN
             BEGIN
                 INSERT INTO docs_pl_events VALUES (:input_id, 100);
                 INSERT INTO docs_pl_events VALUES (:input_id, 200);
             EXCEPTION
                 WHEN unique_violation THEN
                     INSERT INTO docs_pl_events VALUES (:input_id, 300);
             END;
         END;",
    )?)?;
    command(connection.execute("CALL docs_exception(20)")?)?;
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT value FROM docs_pl_events WHERE id = 20",
        )?,
        300
    );

    command(connection.execute(
        "CREATE PROCEDURE docs_cursor_total(OUT output_value INTEGER NOT NULL)
         LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE
             CURSOR values_cursor() FOR
                 SELECT value FROM docs_pl_events WHERE id <= 20 ORDER BY id;
             current_value INTEGER NOT NULL := 0;
         BEGIN
             output_value := 0;
             OPEN values_cursor();
             LOOP
                 FETCH values_cursor INTO current_value;
                 EXIT WHEN values_cursor%NOTFOUND;
                 output_value := output_value + current_value;
             END LOOP;
             CLOSE values_cursor;
         END;",
    )?)?;
    assert_eq!(
        scalar_i64(&mut connection, "CALL docs_cursor_total()")?,
        304
    );

    connection.begin()?;
    command(connection.execute("CALL docs_flow(30, 1)")?)?;
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*) FROM docs_pl_events WHERE id = 30",
        )?,
        1
    );
    connection.rollback()?;
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*) FROM docs_pl_events WHERE id = 30",
        )?,
        0
    );

    command(connection.execute(
        "CREATE TABLE docs_trigger_rows (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL,
            revision INTEGER NOT NULL
        )",
    )?)?;
    command(connection.execute(
        "CREATE FUNCTION docs_touch_trigger() RETURNS TRIGGER
         LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
         BEGIN
             NEW.revision := OLD.revision + 1;
             RETURN NEW;
         END;",
    )?)?;
    command(connection.execute(
        "CREATE TRIGGER docs_touch
         BEFORE UPDATE OF value ON docs_trigger_rows
         FOR EACH ROW PRIORITY 100
         WHEN (OLD.value <> NEW.value)
         EXECUTE FUNCTION docs_touch_trigger();",
    )?)?;
    command(connection.execute("INSERT INTO docs_trigger_rows VALUES (1, 10, 1)")?)?;
    command(connection.execute("UPDATE docs_trigger_rows SET value = 11 WHERE id = 1")?)?;
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT revision FROM docs_trigger_rows WHERE id = 1",
        )?,
        2
    );

    command(connection.execute("CREATE TABLE docs_trigger_log (id INTEGER PRIMARY KEY)")?)?;
    command(connection.execute(
        "CREATE FUNCTION docs_fail_trigger() RETURNS TRIGGER
         LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
         BEGIN
             INSERT INTO docs_trigger_log VALUES (1);
             RAISE invalid_state('trigger failed');
         END;",
    )?)?;
    command(connection.execute(
        "CREATE TRIGGER docs_fail_insert
         BEFORE INSERT ON docs_trigger_rows
         FOR EACH ROW
         EXECUTE FUNCTION docs_fail_trigger();",
    )?)?;
    let trigger_error = connection
        .execute("INSERT INTO docs_trigger_rows VALUES (2, 20, 1)")
        .expect_err("trigger error must fail the INSERT");
    assert!(trigger_error
        .to_string()
        .contains("PL_RUNTIME_INVALID_STATE"));
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*) FROM docs_trigger_rows WHERE id = 2",
        )?,
        0
    );
    assert_eq!(
        scalar_i64(&mut connection, "SELECT COUNT(*) FROM docs_trigger_log")?,
        0
    );

    command(connection.execute("CREATE TABLE docs_job_effects (id INTEGER PRIMARY KEY)")?)?;
    command(connection.execute(
        "CREATE PROCEDURE docs_job_work(input_id INTEGER NOT NULL)
         LANGUAGE RADIX SECURITY INVOKER AS
         BEGIN
             INSERT INTO docs_job_effects VALUES (:input_id);
         END;",
    )?)?;
    command(connection.execute(
        "CREATE JOB docs_enabled_job
         SCHEDULE EVERY INTERVAL '1 second'
         RUN AS radix_system
         CALL docs_job_work(1)
         ENABLE;",
    )?)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if scalar_i64(&mut connection, "SELECT COUNT(*) FROM docs_job_effects")? >= 1 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            return Err("stock scheduler did not execute the enabled job".into());
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*) FROM radix_system_job_history WHERE outcome = 'succeeded'",
        )? >= 1,
        "stock scheduler did not publish durable success history",
    );

    connection.shutdown()?;
    println!(
        "programming-ok function=42 flow=4 exception=300 cursor=304 \
         rollback=verified trigger=2 trigger-error=atomic job-scheduler=verified"
    );
    Ok(())
}

fn command(result: ExecuteResult) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}

fn scalar_i64(connection: &mut Connection, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let rows = query_all(connection, sql)?;
    let [row] = rows.as_slice() else {
        return Err(format!("expected one row from: {sql}").into());
    };
    let [WireValue::Int(value)] = row.values.as_slice() else {
        return Err(format!("expected one INTEGER from: {sql}").into());
    };
    Ok(*value)
}

fn query_all(
    connection: &mut Connection,
    sql: &str,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql)? else {
        return Err(format!("query did not open a cursor: {sql}").into());
    };
    fetch_all(connection, cursor)
}

fn fetch_all(
    connection: &mut Connection,
    cursor: Cursor,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    loop {
        let batch = connection.fetch(&cursor)?;
        rows.extend(batch.rows);
        if batch.eof {
            return Ok(rows);
        }
    }
}
