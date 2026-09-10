use radixdb_client::{Connection, Cursor, ExecuteResult, Row, WireValue};
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15441".to_string());
    let database = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "radixtrade_tcp_import_demo".to_string());
    let radixtrade_root = std::env::args()
        .nth(3)
        .map(PathBuf::from)
        .unwrap_or_else(default_radixtrade_root);

    let schema = radixtrade_root.join("schema.sql");
    let seed = radixtrade_root.join("seed-small.sql");

    let mut connection = connect(&address, &database)?;

    println!("connected to {address}; database={database}");
    run_sql_file(&mut connection, &schema)?;
    run_sql_file(&mut connection, &seed)?;

    expect_count(&mut connection, "rt_branches", 3)?;
    expect_count(&mut connection, "rt_customers", 3)?;
    expect_count(&mut connection, "rt_products", 4)?;
    expect_count(&mut connection, "rt_sales_orders", 3)?;

    let tables = query_all(&mut connection, "SHOW TABLES")?;
    println!("SHOW TABLES returned {} rows", tables.len());
    if tables.len() < 16 {
        return Err(format!("expected at least 16 RadixTrade tables, got {}", tables.len()).into());
    }

    let indexes = query_all(&mut connection, "SHOW INDEXES FROM rt_customers")?;
    print_rows("rt_customers indexes", &indexes);
    if !rows_contain_text(&indexes, "rt_customers_email_active_uidx") {
        return Err("partial unique customer index was not found".into());
    }

    let orders = query_all(
        &mut connection,
        "SELECT order_no, status, total_cents
         FROM rt_sales_orders
         ORDER BY id",
    )?;
    print_rows("orders", &orders);

    connection.shutdown()?;
    println!("RadixTrade TCP import flow passed.");
    Ok(())
}

fn default_radixtrade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../radixtrade")
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

fn run_sql_file(
    connection: &mut Connection,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let sql = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let statements = split_sql_statements(&sql);
    println!("executing {} statements from {}", statements.len(), path.display());

    for (index, statement) in statements.iter().enumerate() {
        match connection.execute(statement)? {
            ExecuteResult::CommandComplete { .. } => {}
            ExecuteResult::Cursor(cursor) => {
                let _ = fetch_all(connection, cursor)?;
            }
        }
        println!("  [{}/{}] ok", index + 1, statements.len());
    }

    Ok(())
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_line_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current.push(ch);
            }
            continue;
        }

        if !in_single_quote && ch == '-' && chars.peek() == Some(&'-') {
            let _ = chars.next();
            in_line_comment = true;
            continue;
        }

        if ch == '\'' {
            current.push(ch);
            if in_single_quote && chars.peek() == Some(&'\'') {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
                continue;
            }
            in_single_quote = !in_single_quote;
            continue;
        }

        if ch == ';' && !in_single_quote {
            let statement = current.trim();
            if !statement.is_empty() {
                statements.push(statement.to_string());
            }
            current.clear();
            continue;
        }

        current.push(ch);
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }

    statements
}

fn expect_count(
    connection: &mut Connection,
    table: &str,
    expected: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = query_all(connection, &format!("SELECT COUNT(*) FROM {table}"))?;
    let actual = rows
        .first()
        .and_then(|row| row.values.first())
        .and_then(wire_value_as_i64)
        .ok_or_else(|| format!("COUNT(*) for {table} did not return an integer"))?;

    if actual != expected {
        return Err(format!("expected {expected} rows in {table}, got {actual}").into());
    }

    println!("{table}: {actual} rows");
    Ok(())
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

fn rows_contain_text(rows: &[Row], needle: &str) -> bool {
    rows.iter().any(|row| {
        row.values
            .iter()
            .any(|value| value_to_text(value).contains(needle))
    })
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

fn wire_value_as_i64(value: &WireValue) -> Option<i64> {
    match value {
        WireValue::Int(value) => Some(*value),
        WireValue::Int8(value) => Some(i64::from(*value)),
        WireValue::Int16(value) => Some(i64::from(*value)),
        WireValue::Int32(value) => Some(i64::from(*value)),
        WireValue::UInt(value) => i64::try_from(*value).ok(),
        WireValue::UInt8(value) => Some(i64::from(*value)),
        WireValue::UInt16(value) => Some(i64::from(*value)),
        WireValue::UInt32(value) => Some(i64::from(*value)),
        _ => None,
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
