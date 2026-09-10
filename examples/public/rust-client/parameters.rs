use std::collections::BTreeMap;

use radixdb_client::{ClientError, Connection, Cursor, ExecuteResult, Row, WireValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15441".to_string());
    let database = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "radixtrade_client_demo".to_string());

    let mut connection = connect(&address, &database)?;

    expect_command(connection.execute("DROP TABLE IF EXISTS rt_client_accounts")?)?;
    expect_command(connection.execute(
        "CREATE TABLE rt_client_accounts (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            email TEXT NOT NULL,
            balance_cents INTEGER NOT NULL DEFAULT 0,
            active BOOLEAN NOT NULL DEFAULT true,
            revision INTEGER NOT NULL DEFAULT 1
        )",
    )?)?;
    expect_command(connection.execute(
        "CREATE UNIQUE INDEX rt_client_accounts_email_active_uidx
         ON rt_client_accounts (email)
         WHERE active = true",
    )?)?;

    let inserted = connection.execute_with_parameters(
        "INSERT INTO rt_client_accounts (email, balance_cents, active)
         VALUES (:email, :balance_cents, :active)",
        named([
            (
                "email",
                WireValue::String("client@example.test".to_string()),
            ),
            ("balance_cents", WireValue::Int(12_500)),
            ("active", WireValue::Bool(true)),
        ]),
    )?;
    let account_id = last_insert_id(inserted)?;
    println!("created account id: {account_id}");

    let rows = query_all_with_parameters(
        &mut connection,
        "SELECT id, email, balance_cents, active, revision
         FROM rt_client_accounts
         WHERE email = :email AND active = :active",
        named([
            (
                "email",
                WireValue::String("client@example.test".to_string()),
            ),
            ("active", WireValue::Bool(true)),
        ]),
    )?;
    print_rows("active account", &rows);

    expect_command(connection.execute_with_parameters(
        "UPDATE rt_client_accounts
         SET balance_cents = balance_cents + :delta,
             revision = revision + 1
         WHERE id = :id AND revision = :expected_revision",
        named([
            ("delta", WireValue::Int(2_500)),
            ("id", WireValue::Int(account_id)),
            ("expected_revision", WireValue::Int(1)),
        ]),
    )?)?;

    let rows = query_all_with_parameters(
        &mut connection,
        "SELECT id, balance_cents, revision
         FROM rt_client_accounts
         WHERE id = :id",
        named([("id", WireValue::Int(account_id))]),
    )?;
    print_rows("after optimistic update", &rows);

    match connection.execute_with_parameters(
        "INSERT INTO rt_client_accounts (email, balance_cents, active)
         VALUES (:email, :balance_cents, :active)",
        named([
            (
                "email",
                WireValue::String("client@example.test".to_string()),
            ),
            ("balance_cents", WireValue::Int(100)),
            ("active", WireValue::Bool(true)),
        ]),
    ) {
        Err(ClientError::Server(error)) if error.message.contains("unique constraint") => {
            println!("duplicate active email rejected: {}", error.message);
        }
        Err(error) => return Err(format!("unexpected insert error: {error}").into()),
        Ok(_) => return Err("duplicate active email was accepted".into()),
    }

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

fn named<const N: usize>(entries: [(&str, WireValue); N]) -> BTreeMap<String, WireValue> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn expect_command(result: ExecuteResult) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}

fn last_insert_id(result: ExecuteResult) -> Result<i64, Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { last_insert_id, .. } => {
            i64::try_from(last_insert_id).map_err(|_| "last_insert_id does not fit i64".into())
        }
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}

fn query_all_with_parameters(
    connection: &mut Connection,
    sql: &str,
    parameters: BTreeMap<String, WireValue>,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute_with_parameters(sql, parameters)? else {
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
        other => format!("{other:?}"),
    }
}
