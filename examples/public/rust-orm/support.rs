#![allow(dead_code)]

use std::env;

use radixdb_client::{Connection, Cursor, ExecuteResult, Row};

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:16441";
pub const DEFAULT_DATABASE: &str = "radixtrade_orm_demo";

pub fn connect() -> Result<Connection, Box<dyn std::error::Error>> {
    let address = env::var("RADIXDB_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.to_string());
    let database = env::var("RADIXDB_DATABASE").unwrap_or_else(|_| DEFAULT_DATABASE.to_string());
    let login = env::var("RADIXDB_LOGIN").unwrap_or_else(|_| "root".to_string());
    let password = env::var("RADIXDB_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty());

    let mut connection = Connection::connect(&address)?;
    connection.authenticate(login, password)?;
    connection.select_database(&database)?;
    println!("connected to {address}; database={database}");
    Ok(connection)
}

pub fn execute_script(
    connection: &mut Connection,
    label: &str,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let statements = split_sql_statements(sql);
    println!("{label}: executing {} statements", statements.len());
    for statement in statements {
        if let ExecuteResult::Cursor(cursor) = connection.execute(&statement)? {
            let _ = fetch_all(connection, &cursor)?;
        }
    }
    Ok(())
}

pub fn fetch_all(
    connection: &mut Connection,
    cursor: &Cursor,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    loop {
        let batch = connection.fetch(cursor)?;
        rows.extend(batch.rows);
        if batch.eof {
            return Ok(rows);
        }
    }
}

pub fn print_cursor(
    connection: &mut Connection,
    cursor: &Cursor,
) -> Result<(), Box<dyn std::error::Error>> {
    for row in fetch_all(connection, cursor)? {
        println!("  {:?}", row.values);
    }
    Ok(())
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_line_comment = false;

    while let Some(character) = chars.next() {
        if in_line_comment {
            if character == '\n' {
                in_line_comment = false;
                current.push(character);
            }
            continue;
        }

        if !in_single_quote && character == '-' && chars.peek() == Some(&'-') {
            let _ = chars.next();
            in_line_comment = true;
            continue;
        }

        if character == '\'' {
            current.push(character);
            if in_single_quote && chars.peek() == Some(&'\'') {
                current.push(chars.next().expect("peeked escaped quote"));
                continue;
            }
            in_single_quote = !in_single_quote;
            continue;
        }

        if character == ';' && !in_single_quote {
            let statement = current.trim();
            if !statement.is_empty() {
                statements.push(statement.to_string());
            }
            current.clear();
            continue;
        }

        current.push(character);
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    statements
}
