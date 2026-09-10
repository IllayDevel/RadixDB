// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use radixdb::Database;
use sqllogictest::{DBOutput, DefaultColumnType, Runner};
use std::path::Path;

/// Wrapper around radixdb's Database for sqllogictest
struct RadixDB {
    db: Database,
    checkpoint_after_statement: bool,
}

fn slt_type(data_type: radixdb::DataType) -> DefaultColumnType {
    match data_type {
        radixdb::DataType::Integer => DefaultColumnType::Integer,
        radixdb::DataType::Float => DefaultColumnType::FloatingPoint,
        radixdb::DataType::Null => DefaultColumnType::Any,
        _ => DefaultColumnType::Text,
    }
}

fn merge_slt_type(current: DefaultColumnType, next: DefaultColumnType) -> DefaultColumnType {
    use DefaultColumnType::{Any, FloatingPoint, Integer, Text};

    match (current, next) {
        (Any, next) => next,
        (current, Any) => current,
        (current, next) if current == next => current,
        (Integer, FloatingPoint) | (FloatingPoint, Integer) => FloatingPoint,
        _ => Text,
    }
}

fn render_slt_value(value: &radixdb::Value) -> String {
    match value {
        radixdb::Value::Null(_) => "NULL".to_string(),
        radixdb::Value::Integer(value) => value.to_string(),
        // Display for f64 is the shortest lossless round-trip representation.
        radixdb::Value::Float(value) => value.to_string(),
        radixdb::Value::Boolean(value) => value.to_string(),
        radixdb::Value::Text(value) if value.is_empty() => "(empty)".to_string(),
        radixdb::Value::Text(value) => value.to_string(),
        radixdb::Value::Timestamp(value) => value.to_rfc3339(),
        radixdb::Value::Extension(_) => value.to_string(),
    }
}

/// Error wrapper that satisfies sqllogictest's requirements
#[derive(Debug, Clone, PartialEq, Eq)]
struct RadixDBError(String);

impl std::fmt::Display for RadixDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RadixDBError {}

impl sqllogictest::DB for RadixDB {
    type Error = RadixDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // Use query() for everything - it works for DDL/DML and SELECT
        let mut rows_result = self
            .db
            .query(sql, ())
            .map_err(|e| RadixDBError(e.to_string()))?;

        let columns = rows_result.columns().to_vec();

        // If no columns, this is a DDL/DML statement
        if columns.is_empty() {
            let affected = rows_result.rows_affected();
            drop(rows_result);
            if self.checkpoint_after_statement {
                self.db
                    .execute("PRAGMA CHECKPOINT", ())
                    .map_err(|error| RadixDBError(error.to_string()))?;
            }
            return Ok(DBOutput::StatementComplete(affected as u64));
        }

        // Collect rows and determine types from the data
        let num_cols = columns.len();
        let mut result_rows: Vec<Vec<String>> = Vec::new();
        let mut col_types: Vec<DefaultColumnType> = vec![DefaultColumnType::Any; num_cols];

        for row in &mut rows_result {
            let row = row.map_err(|e| RadixDBError(e.to_string()))?;
            let mut string_row = Vec::with_capacity(num_cols);

            for (i, col_type) in col_types.iter_mut().enumerate() {
                let value = row.get_value(i);
                let Some(value) = value else {
                    string_row.push("NULL".to_string());
                    continue;
                };
                let next_type = match value {
                    radixdb::Value::Null(data_type) => slt_type(*data_type),
                    radixdb::Value::Integer(_) => DefaultColumnType::Integer,
                    radixdb::Value::Float(_) => DefaultColumnType::FloatingPoint,
                    _ => DefaultColumnType::Text,
                };
                *col_type = merge_slt_type(col_type.clone(), next_type);
                string_row.push(render_slt_value(value));
            }
            result_rows.push(string_row);
        }

        Ok(DBOutput::Rows {
            types: col_types,
            rows: result_rows,
        })
    }

    fn engine_name(&self) -> &str {
        "radixdb"
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn query_fingerprint(db: &Database, sql: &str) -> Vec<String> {
    let mut rows = db
        .query(sql, ())
        .unwrap_or_else(|error| panic!("fingerprint query failed for {sql:?}: {error}"));
    let column_count = rows.columns().len();
    let mut result = Vec::new();
    for row in &mut rows {
        let row = row.unwrap_or_else(|error| panic!("fingerprint row failed: {error}"));
        let values = (0..column_count)
            .map(|index| format!("{:?}", row.get_value(index)))
            .collect::<Vec<_>>()
            .join("\u{1f}");
        result.push(values);
    }
    result.sort();
    result
}

fn database_fingerprint(db: &Database) -> Vec<String> {
    let mut fingerprint = Vec::new();
    let table_rows = query_fingerprint(db, "SHOW TABLES");
    let mut tables = db
        .query("SHOW TABLES", ())
        .expect("enumerate tables for reopen fingerprint")
        .map(|row| {
            row.expect("read table name")
                .get::<String>(0)
                .expect("table name")
        })
        .collect::<Vec<_>>();
    tables.sort();
    fingerprint.push(format!("SHOW TABLES={table_rows:?}"));
    for table in tables {
        let quoted = quote_identifier(&table);
        fingerprint.push(format!(
            "CREATE {table}={:?}",
            query_fingerprint(db, &format!("SHOW CREATE TABLE {quoted}"))
        ));
        fingerprint.push(format!(
            "INDEXES {table}={:?}",
            query_fingerprint(db, &format!("SHOW INDEXES FROM {quoted}"))
        ));
        fingerprint.push(format!(
            "ROWS {table}={:?}",
            query_fingerprint(db, &format!("SELECT * FROM {quoted}"))
        ));
    }

    let view_rows = query_fingerprint(db, "SHOW VIEWS");
    let mut views = db
        .query("SHOW VIEWS", ())
        .expect("enumerate views for reopen fingerprint")
        .map(|row| {
            row.expect("read view name")
                .get::<String>(0)
                .expect("view name")
        })
        .collect::<Vec<_>>();
    views.sort();
    fingerprint.push(format!("SHOW VIEWS={view_rows:?}"));
    for view in views {
        let quoted = quote_identifier(&view);
        fingerprint.push(format!(
            "CREATE VIEW {view}={:?}",
            query_fingerprint(db, &format!("SHOW CREATE VIEW {quoted}"))
        ));
        fingerprint.push(format!(
            "VIEW ROWS {view}={:?}",
            query_fingerprint(db, &format!("SELECT * FROM {quoted}"))
        ));
    }
    fingerprint
}

fn run_slt_file(path: &Path) {
    let mut memory_runner = Runner::new(|| async {
        let db = Database::open_in_memory().expect("Failed to create in-memory database");
        Ok(RadixDB {
            db,
            checkpoint_after_statement: false,
        })
    });
    memory_runner
        .run_file(path)
        .unwrap_or_else(|e| panic!("Failed to run SLT file: {}: {}", path.display(), e));

    let directory = tempfile::tempdir().expect("create file-backed SLT directory");
    let dsn = format!("file://{}", directory.path().join("database").display());
    let reopen_dsn = dsn.clone();
    let checkpoint_after_statement = !path.ends_with("transaction/basic_txn.slt");
    let mut file_runner = Runner::new(move || {
        let dsn = dsn.clone();
        async move {
            let db = Database::open(&dsn).expect("open file-backed SLT database");
            Ok(RadixDB {
                db,
                checkpoint_after_statement,
            })
        }
    });
    file_runner
        .run_file(path)
        .unwrap_or_else(|e| panic!("File-backed SLT failed: {}: {}", path.display(), e));
    drop(file_runner);
    let reopened = Database::open(&reopen_dsn).expect("reopen file-backed SLT database");
    let first_fingerprint = database_fingerprint(&reopened);
    reopened.close().expect("close first reopened SLT database");
    let reopened_again = Database::open(&reopen_dsn).expect("reopen SLT database a second time");
    let second_fingerprint = database_fingerprint(&reopened_again);
    assert_eq!(
        first_fingerprint, second_fingerprint,
        "catalog, schema, indexes, views, or data changed across reopen"
    );
}

fn run_slt_dir(dir: &str) {
    let dir_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/slt")
        .join(dir);
    if !dir_path.exists() {
        panic!("SLT directory not found: {}", dir_path.display());
    }

    let mut files = Vec::new();
    let mut pending = vec![dir_path.clone()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "slt") {
                files.push(path);
            }
        }
    }
    files.sort();

    assert!(
        !files.is_empty(),
        "No .slt files found in {}",
        dir_path.display()
    );

    for file in files {
        run_slt_file(&file);
    }
}

// Test functions for each category

#[test]
fn r8_l01_batch_b_sqllogictest_basic_memory_file_and_reopen() {
    run_slt_dir("basic");
}

#[test]
fn sqllogictest_aggregate() {
    run_slt_dir("aggregate");
}

#[test]
fn sqllogictest_join() {
    run_slt_dir("join");
}

#[test]
fn sqllogictest_subquery() {
    run_slt_dir("subquery");
}

#[test]
fn sqllogictest_advanced() {
    run_slt_dir("advanced");
}

#[test]
fn sqllogictest_functions() {
    run_slt_dir("functions");
}

#[test]
fn sqllogictest_index() {
    run_slt_dir("index");
}

#[test]
fn sqllogictest_transaction() {
    run_slt_dir("transaction");
}
