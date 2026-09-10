use std::time::Duration;

use radixdb_client::{ClientError, Connection, Cursor, ExecuteResult, Row, WireValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .expect("usage: tcp <address> <database>");
    let database = std::env::args()
        .nth(2)
        .expect("usage: tcp <address> <database>");
    let timeout = Duration::from_secs(5);
    let mut connection = Connection::connect_with_timeouts(address, timeout, timeout, timeout)?;
    connection.authenticate("root", None)?;
    connection.select_database(database)?;

    expect_command(connection.execute("DROP TABLE IF EXISTS docs_tcp_notes")?)?;
    expect_command(connection.execute(
        "CREATE TABLE docs_tcp_notes (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL UNIQUE
        )",
    )?)?;

    let insert = connection.prepare("INSERT INTO docs_tcp_notes (id, title) VALUES ($1, $2)")?;
    for (id, title) in [(1_i64, "first"), (2_i64, "second")] {
        expect_command(connection.execute_prepared(
            &insert,
            vec![WireValue::Int(id), WireValue::String(title.to_string())],
        )?)?;
    }
    let duplicate = connection
        .execute_prepared(
            &insert,
            vec![WireValue::Int(3), WireValue::String("first".to_string())],
        )
        .expect_err("UNIQUE violation must be returned");
    assert!(!duplicate.is_retryable());

    let ExecuteResult::Cursor(open_cursor) =
        connection.execute("SELECT id, title FROM docs_tcp_notes ORDER BY id")?
    else {
        return Err("SELECT did not open a cursor".into());
    };
    assert!(matches!(
        connection.execute("SELECT 1"),
        Err(ClientError::CommandsOutOfSync)
    ));
    connection.close_cursor(open_cursor)?;

    connection.begin()?;
    expect_command(connection.execute_prepared(
        &insert,
        vec![
            WireValue::Int(3),
            WireValue::String("rolled back".to_string()),
        ],
    )?)?;
    connection.rollback()?;

    let rows = query_all(
        &mut connection,
        "SELECT id, title FROM docs_tcp_notes ORDER BY id",
    )?;
    assert_eq!(rows.len(), 2);
    connection.close_prepared(insert)?;
    assert!(connection.is_reusable());
    connection.shutdown()?;
    println!("tcp-ok rows=2 duplicate=rejected cursor=closed rollback=verified");
    Ok(())
}

fn expect_command(result: ExecuteResult) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}

fn query_all(
    connection: &mut Connection,
    sql: &str,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql)? else {
        return Err("query did not open a cursor".into());
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
