use radixdb_client::{Connection, Cursor, ExecuteResult, Row, WireValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15441".to_string());
    let database = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "radixtrade_client_demo".to_string());

    let mut connection = connect(&address, &database)?;

    expect_command(connection.execute("DROP TABLE IF EXISTS rt_client_notes")?)?;
    expect_command(connection.execute(
        "CREATE TABLE rt_client_notes (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            title TEXT NOT NULL,
            body TEXT,
            done BOOLEAN NOT NULL DEFAULT false
        )",
    )?)?;

    let inserted = connection.execute(
        "INSERT INTO rt_client_notes (title, body)
         VALUES ('first note', 'created through radixdb-client')",
    )?;

    let ExecuteResult::CommandComplete {
        affected_rows,
        last_insert_id,
    } = inserted
    else {
        return Err("INSERT unexpectedly returned a cursor".into());
    };
    println!("inserted rows: {affected_rows}");
    println!("last insert id: {last_insert_id}");

    expect_command(connection.execute(
        "INSERT INTO rt_client_notes (title, body, done)
         VALUES ('second note', 'already done', true)",
    )?)?;

    let rows = query_all(
        &mut connection,
        "SELECT id, title, done
         FROM rt_client_notes
         ORDER BY id",
    )?;
    print_rows("notes", &rows);

    connection.shutdown()?;
    Ok(())
}

fn connect(address: &str, database: &str) -> Result<Connection, Box<dyn std::error::Error>> {
    let mut connection = Connection::connect(address)?;
    let login = std::env::var("RADIXDB_LOGIN").unwrap_or_else(|_| "root".to_string());
    let password = std::env::var("RADIXDB_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty());
    connection.authenticate(login, password)?;
    connection.select_database(database)?;
    Ok(connection)
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
            break;
        }
    }
    Ok(rows)
}

fn print_rows(label: &str, rows: &[Row]) {
    println!("{label}:");
    for row in rows {
        let values = row
            .values
            .iter()
            .map(value_to_text)
            .collect::<Vec<_>>()
            .join(" | ");
        println!("  {values}");
    }
}

fn value_to_text(value: &WireValue) -> String {
    match value {
        WireValue::Null => "NULL".to_string(),
        WireValue::Bool(value) => value.to_string(),
        WireValue::Int(value) => value.to_string(),
        WireValue::Int8(value) => value.to_string(),
        WireValue::Int16(value) => value.to_string(),
        WireValue::Int32(value) => value.to_string(),
        WireValue::UInt(value) => value.to_string(),
        WireValue::UInt8(value) => value.to_string(),
        WireValue::UInt16(value) => value.to_string(),
        WireValue::UInt32(value) => value.to_string(),
        WireValue::Float64(value) => value.to_string(),
        WireValue::String(value) => value.clone(),
        WireValue::DateTime {
            millis_since_unix_epoch_utc,
        } => format!("{millis_since_unix_epoch_utc}ms"),
        WireValue::Uuid(bytes) => format_uuid(bytes),
        other => format!("{other:?}"),
    }
}

fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}
