use std::collections::BTreeMap;

use radixdb_client::{ClientError, Connection, ExecuteResult, Row, WireValue};

const TABLE: &str = "radixdb_partial_unique_probe_users";
const INDEX: &str = "radixdb_partial_unique_probe_email_active_idx";
const EMAIL: &str = "owner@example.test";
const FIRST_ID: [u8; 16] = [
    0x01, 0x94, 0x00, 0x00, 0x00, 0x01, 0x70, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
];
const SECOND_ID: [u8; 16] = [
    0x01, 0x94, 0x00, 0x00, 0x00, 0x02, 0x70, 0x00, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
];
const THIRD_ID: [u8; 16] = [
    0x01, 0x94, 0x00, 0x00, 0x00, 0x03, 0x70, 0x00, 0x80, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15441".to_string());
    let database = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "partial_unique_probe".to_string());
    let mode = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "setup".to_string());

    let mut connection = Connection::connect(address)?;
    let login = std::env::var("RADIXDB_LOGIN").unwrap_or_else(|_| "root".to_string());
    let password = std::env::var("RADIXDB_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty());
    connection.authenticate(login, password)?;
    connection.select_database(database)?;

    match mode.as_str() {
        "setup" => setup(&mut connection)?,
        "verify" => verify(&mut connection)?,
        "cleanup" => cleanup(&mut connection)?,
        "inspect" => inspect(&mut connection)?,
        other => {
            return Err(format!(
                "unknown mode `{other}`, expected setup, verify, cleanup, or inspect"
            )
            .into())
        }
    }

    println!("partial_unique_probe {mode}: ok");
    Ok(())
}

fn setup(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    cleanup(connection)?;
    expect_command(connection.execute(format!(
        "CREATE TABLE {TABLE} (
            id UUID PRIMARY KEY,
            email TEXT NOT NULL,
            __raf_deleted_at TIMESTAMP
        )"
    ))?)?;
    expect_command(connection.execute(format!(
        "CREATE UNIQUE INDEX {INDEX}
         ON {TABLE} (email)
         WHERE __raf_deleted_at IS NULL"
    ))?)?;

    expect_command(insert_user(connection, FIRST_ID, WireValue::Null)?)?;
    expect_unique_error(insert_user(connection, SECOND_ID, WireValue::Null))?;
    expect_command(soft_delete_user(connection, FIRST_ID)?)?;
    expect_command(insert_user(connection, SECOND_ID, WireValue::Null)?)?;
    expect_unique_error(restore_user_to_active(connection, FIRST_ID))?;
    verify(connection)?;
    Ok(())
}

fn verify(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    let options = show_index_options(connection)?;
    if !options.contains("where=") || !options.contains("__raf_deleted_at IS NULL") {
        return Err(format!("SHOW INDEXES does not expose partial predicate: {options}").into());
    }
    expect_unique_error(insert_user(connection, THIRD_ID, WireValue::Null))?;
    let active = fetch_count(
        connection,
        &format!(
            "SELECT COUNT(*) FROM {TABLE}
         WHERE email = 'owner@example.test' AND __raf_deleted_at IS NULL"
        ),
    )?;
    if active != 1 {
        return Err(format!("expected exactly one active row, got {active}").into());
    }
    let total = fetch_count(
        connection,
        &format!("SELECT COUNT(*) FROM {TABLE} WHERE email = 'owner@example.test'"),
    )?;
    if total != 2 {
        return Err(
            format!("expected deleted + active rows to remain visible, got {total}").into(),
        );
    }
    Ok(())
}

fn cleanup(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    expect_command(connection.execute(format!("DROP TABLE IF EXISTS {TABLE}"))?)?;
    Ok(())
}

fn inspect(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    println!("SHOW INDEXES FROM {TABLE}");
    let ExecuteResult::Cursor(cursor) = connection.execute(format!("SHOW INDEXES FROM {TABLE}"))?
    else {
        return Err("SHOW INDEXES did not open a cursor".into());
    };
    let batch = connection.fetch(&cursor)?;
    for row in &batch.rows {
        println!("{:?}", row.values);
    }

    println!("rows for {EMAIL}");
    let ExecuteResult::Cursor(cursor) = connection.execute(format!(
        "SELECT id, email, __raf_deleted_at FROM {TABLE} WHERE email = '{EMAIL}'"
    ))?
    else {
        return Err("SELECT did not open a cursor".into());
    };
    let batch = connection.fetch(&cursor)?;
    for row in &batch.rows {
        println!("{:?}", row.values);
    }

    Ok(())
}

fn insert_user(
    connection: &mut Connection,
    id: [u8; 16],
    deleted_at: WireValue,
) -> Result<ExecuteResult, ClientError> {
    connection.execute_with_parameters(
        format!(
            "INSERT INTO {TABLE} (id, email, __raf_deleted_at)
             VALUES (:id, :email, :deleted_at)"
        ),
        named([
            ("id", WireValue::Uuid(id)),
            ("email", WireValue::String(EMAIL.to_string())),
            ("deleted_at", deleted_at),
        ]),
    )
}

fn soft_delete_user(
    connection: &mut Connection,
    id: [u8; 16],
) -> Result<ExecuteResult, ClientError> {
    connection.execute_with_parameters(
        format!(
            "UPDATE {TABLE}
             SET __raf_deleted_at = :deleted_at
             WHERE id = :id"
        ),
        named([
            (
                "deleted_at",
                WireValue::DateTime {
                    millis_since_unix_epoch_utc: 1_735_689_600_000,
                },
            ),
            ("id", WireValue::Uuid(id)),
        ]),
    )
}

fn restore_user_to_active(
    connection: &mut Connection,
    id: [u8; 16],
) -> Result<ExecuteResult, ClientError> {
    connection.execute_with_parameters(
        format!(
            "UPDATE {TABLE}
             SET __raf_deleted_at = NULL
             WHERE id = :id"
        ),
        named([("id", WireValue::Uuid(id))]),
    )
}

fn show_index_options(connection: &mut Connection) -> Result<String, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute(format!("SHOW INDEXES FROM {TABLE}"))?
    else {
        return Err("SHOW INDEXES did not open a cursor".into());
    };
    let batch = connection.fetch(&cursor)?;
    for Row { values } in batch.rows {
        if values.get(1) == Some(&WireValue::String(INDEX.to_string())) {
            if values.get(4) != Some(&WireValue::Bool(true)) {
                return Err(format!("SHOW INDEXES is_unique column is invalid: {values:?}").into());
            }
            let Some(WireValue::String(options)) = values.get(5) else {
                return Err(format!("SHOW INDEXES options column is invalid: {values:?}").into());
            };
            return Ok(options.clone());
        }
    }
    Err(format!("index `{INDEX}` not found in SHOW INDEXES").into())
}

fn fetch_count(connection: &mut Connection, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql)? else {
        return Err(format!("query `{sql}` did not open a cursor").into());
    };
    let batch = connection.fetch(&cursor)?;
    if !batch.eof || batch.rows.len() != 1 || batch.rows[0].values.len() != 1 {
        return Err(format!("query `{sql}` returned unexpected batch: {:?}", batch.rows).into());
    }
    match batch.rows[0].values[0] {
        WireValue::Int(value) => Ok(value),
        ref value => Err(format!("query `{sql}` returned non-integer count: {value:?}").into()),
    }
}

fn expect_command(result: ExecuteResult) -> Result<(), Box<dyn std::error::Error>> {
    match result {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => Err(format!("unexpected cursor {}", cursor.id()).into()),
    }
}

fn expect_unique_error(
    result: Result<ExecuteResult, ClientError>,
) -> Result<(), Box<dyn std::error::Error>> {
    let error = result.expect_err("operation should fail with unique constraint");
    let message = error.to_string();
    if message.contains("unique constraint") {
        Ok(())
    } else {
        Err(format!("expected unique constraint error, got: {message}").into())
    }
}

fn named<const N: usize>(entries: [(&str, WireValue); N]) -> BTreeMap<String, WireValue> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
