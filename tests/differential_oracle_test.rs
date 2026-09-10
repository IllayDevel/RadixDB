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

#![cfg(feature = "sqlite")]

//! Differential Oracle Tests
//!
//! Compares RadixDB query results against SQLite without string/numeric
//! normalization that could hide a type or scalar-identity mismatch.

use radixdb::Database;
use radixdb::Value;
use rusqlite::Connection;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum OracleCell {
    Null,
    Integer(i64),
    Float(u64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Timestamp(String),
    Extension(Vec<u8>),
}

/// Create both an in-memory RadixDB database and an in-memory SQLite database.
fn setup_both() -> (Database, Connection) {
    let radixdb = Database::open_in_memory().expect("Failed to create RadixDB database");
    let sqlite = Connection::open_in_memory().expect("Failed to create SQLite database");
    (radixdb, sqlite)
}

/// Execute a DDL or DML statement on both databases.
fn exec_both(radixdb: &Database, sqlite: &Connection, sql: &str) {
    radixdb
        .execute(sql, ())
        .unwrap_or_else(|e| panic!("RadixDB failed on '{}': {}", sql, e));
    sqlite
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("SQLite failed on '{}': {}", sql, e));
}

fn query_radixdb(db: &Database, sql: &str) -> Vec<Vec<OracleCell>> {
    let mut rows = db
        .query(sql, ())
        .unwrap_or_else(|e| panic!("RadixDB query failed on '{}': {}", sql, e));
    let num_cols = rows.columns().len();
    let mut result: Vec<Vec<OracleCell>> = Vec::new();

    for row in &mut rows {
        let row = row.unwrap_or_else(|e| panic!("RadixDB row error on '{}': {}", sql, e));
        let mut typed_row = Vec::with_capacity(num_cols);
        for i in 0..num_cols {
            let value = row.get_value(i);
            let cell = match value {
                Some(Value::Integer(v)) => OracleCell::Integer(*v),
                Some(Value::Float(v)) => OracleCell::Float(v.to_bits()),
                Some(Value::Boolean(b)) => OracleCell::Boolean(*b),
                Some(Value::Text(t)) => OracleCell::Text(t.to_string()),
                Some(Value::Null(_)) | None => OracleCell::Null,
                Some(Value::Timestamp(ts)) => OracleCell::Timestamp(ts.to_rfc3339()),
                Some(Value::Extension(data)) => OracleCell::Extension(data.as_ref().to_vec()),
            };
            typed_row.push(cell);
        }
        result.push(typed_row);
    }
    result
}

fn query_sqlite(conn: &Connection, sql: &str) -> Vec<Vec<OracleCell>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQLite prepare failed on '{}': {}", sql, e));
    let col_count = stmt.column_count();

    let rows = stmt
        .query_map([], |row| {
            let mut typed_row = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let val: rusqlite::types::Value = row.get(i).unwrap();
                let cell = match val {
                    rusqlite::types::Value::Null => OracleCell::Null,
                    rusqlite::types::Value::Integer(v) => OracleCell::Integer(v),
                    rusqlite::types::Value::Real(v) => OracleCell::Float(v.to_bits()),
                    rusqlite::types::Value::Text(t) => OracleCell::Text(t),
                    rusqlite::types::Value::Blob(bytes) => OracleCell::Blob(bytes),
                };
                typed_row.push(cell);
            }
            Ok(typed_row)
        })
        .unwrap_or_else(|e| panic!("SQLite query_map failed on '{}': {}", sql, e));

    rows.map(|r| r.expect("SQLite row error")).collect()
}

/// Sort rows and compare the exact typed result sets.
/// If they differ, panic with a descriptive error showing the SQL and both result sets.
fn normalize_and_compare(
    sql: &str,
    radixdb_rows: Vec<Vec<OracleCell>>,
    sqlite_rows: Vec<Vec<OracleCell>>,
) {
    let mut radixdb_sorted = radixdb_rows;
    radixdb_sorted.sort();
    let mut sqlite_sorted = sqlite_rows;
    sqlite_sorted.sort();

    // Check row count
    if radixdb_sorted.len() != sqlite_sorted.len() {
        panic!(
            "Row count mismatch for SQL: {}\n  RadixDB rows: {}\n  SQLite rows: {}\n  RadixDB: {:?}\n  SQLite:  {:?}",
            sql,
            radixdb_sorted.len(),
            sqlite_sorted.len(),
            radixdb_sorted,
            sqlite_sorted
        );
    }

    // Compare each row cell by cell
    for (row_idx, (s_row, q_row)) in radixdb_sorted.iter().zip(sqlite_sorted.iter()).enumerate() {
        if s_row.len() != q_row.len() {
            panic!(
                "Column count mismatch at row {} for SQL: {}\n  RadixDB: {:?}\n  SQLite:  {:?}",
                row_idx, sql, s_row, q_row
            );
        }
        for (col_idx, (s_cell, q_cell)) in s_row.iter().zip(q_row.iter()).enumerate() {
            if s_cell != q_cell {
                panic!(
                    "Value mismatch at row {}, col {} for SQL: {}\n  RadixDB cell: {:?}\n  SQLite cell:  {:?}\n  RadixDB full row: {:?}\n  SQLite full row:  {:?}\n  All RadixDB rows: {:?}\n  All SQLite rows:  {:?}",
                    row_idx, col_idx, sql, s_cell, q_cell, s_row, q_row, radixdb_sorted, sqlite_sorted
                );
            }
        }
    }
}

/// Compare two ordered result sets row-by-row without re-sorting.
/// Use this for ORDER BY queries where row order matters.
fn assert_ordered_equal(
    sql: &str,
    radixdb_rows: &[Vec<OracleCell>],
    sqlite_rows: &[Vec<OracleCell>],
) {
    assert_eq!(
        radixdb_rows.len(),
        sqlite_rows.len(),
        "Row count mismatch for: {}\n  RadixDB: {:?}\n  SQLite:  {:?}",
        sql,
        radixdb_rows,
        sqlite_rows
    );
    for (i, (s, q)) in radixdb_rows.iter().zip(sqlite_rows.iter()).enumerate() {
        assert_eq!(
            s.len(),
            q.len(),
            "Column count mismatch at row {} for: {}",
            i,
            sql
        );
        for (j, (sc, qc)) in s.iter().zip(q.iter()).enumerate() {
            assert_eq!(
                sc, qc,
                "Mismatch at row {}, col {} for SQL: {}\n  RadixDB: {:?}\n  SQLite:  {:?}",
                i, j, sql, radixdb_rows, sqlite_rows
            );
        }
    }
}

#[test]
fn r8_l01_batch_b_differential_cells_preserve_type_and_full_precision() {
    assert_ne!(OracleCell::Integer(42), OracleCell::Text("42".into()));
    assert_ne!(
        OracleCell::Float(1.000_1_f64.to_bits()),
        OracleCell::Float(1.000_2_f64.to_bits())
    );
    assert_ne!(
        OracleCell::Integer(9_007_199_254_740_992),
        OracleCell::Integer(9_007_199_254_740_993)
    );
}

#[test]
fn r8_l01_batch_b_differential_reopens_file_backed_storage() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}", directory.path().join("radix").display());
    let sqlite = Connection::open_in_memory().unwrap();
    {
        let radixdb = Database::open(&dsn).unwrap();
        exec_both(
            &radixdb,
            &sqlite,
            "CREATE TABLE identity (id INTEGER PRIMARY KEY, txt TEXT, real FLOAT)",
        );
        exec_both(
            &radixdb,
            &sqlite,
            "INSERT INTO identity VALUES (9007199254740993, '42', 0.123456789012345)",
        );
        radixdb.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    let reopened = Database::open(&dsn).unwrap();
    let sql = "SELECT id, txt, real FROM identity";
    normalize_and_compare(
        sql,
        query_radixdb(&reopened, sql),
        query_sqlite(&sqlite, sql),
    );
}

#[test]
fn b7_transaction_stream_and_nested_views_match_sqlite_exactly() {
    let (radixdb, sqlite) = setup_both();
    for sql in [
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE entries (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, amount INTEGER NOT NULL)",
        "INSERT INTO accounts VALUES (1, 'cash'), (2, 'bank'), (3, 'reserve')",
        "INSERT INTO entries VALUES (1, 1, 100), (2, 1, -25), (3, 2, 60)",
        "CREATE VIEW account_totals AS SELECT a.id, a.name, SUM(e.amount) AS total, COUNT(e.id) AS entry_count FROM accounts a LEFT JOIN entries e ON e.account_id = a.id GROUP BY a.id, a.name",
        "CREATE VIEW positive_accounts AS SELECT id, name, total, entry_count FROM account_totals WHERE COALESCE(total, 0) > 0",
    ] {
        exec_both(&radixdb, &sqlite, sql);
    }

    let account_columns = radixdb
        .query("SELECT * FROM account_totals", ())
        .unwrap()
        .columns()
        .to_vec();
    assert_eq!(
        account_columns,
        vec!["id", "name", "total", "entry_count"],
        "aggregate VIEW published unstable output names"
    );
    let nested_columns = radixdb
        .query("SELECT * FROM positive_accounts", ())
        .unwrap()
        .columns()
        .to_vec();
    assert_eq!(
        nested_columns,
        vec!["id", "name", "total", "entry_count"],
        "nested VIEW did not preserve source output names"
    );

    let nested = "SELECT id, name, total, entry_count FROM positive_accounts ORDER BY id";
    assert_ordered_equal(
        nested,
        &query_radixdb(&radixdb, nested),
        &query_sqlite(&sqlite, nested),
    );

    exec_both(&radixdb, &sqlite, "BEGIN");
    exec_both(
        &radixdb,
        &sqlite,
        "INSERT INTO entries VALUES (4, 2, 15), (5, 3, 40)",
    );
    exec_both(
        &radixdb,
        &sqlite,
        "UPDATE entries SET amount = amount + 5 WHERE account_id = 1",
    );
    exec_both(&radixdb, &sqlite, "DELETE FROM entries WHERE id = 3");
    assert_ordered_equal(
        nested,
        &query_radixdb(&radixdb, nested),
        &query_sqlite(&sqlite, nested),
    );
    exec_both(&radixdb, &sqlite, "ROLLBACK");
    assert_ordered_equal(
        nested,
        &query_radixdb(&radixdb, nested),
        &query_sqlite(&sqlite, nested),
    );

    exec_both(&radixdb, &sqlite, "BEGIN");
    exec_both(&radixdb, &sqlite, "INSERT INTO entries VALUES (6, 3, 90)");
    exec_both(&radixdb, &sqlite, "COMMIT");
    assert_ordered_equal(
        nested,
        &query_radixdb(&radixdb, nested),
        &query_sqlite(&sqlite, nested),
    );

    let inline = "SELECT id, name, total, entry_count FROM (SELECT a.id, a.name, SUM(e.amount) AS total, COUNT(e.id) AS entry_count FROM accounts a LEFT JOIN entries e ON e.account_id = a.id GROUP BY a.id, a.name) totals WHERE COALESCE(total, 0) > 0 ORDER BY id";
    assert_ordered_equal(
        "nested VIEW / inline",
        &query_radixdb(&radixdb, nested),
        &query_radixdb(&radixdb, inline),
    );
}

// ---------------------------------------------------------------------------
// Test functions
// ---------------------------------------------------------------------------

#[test]
fn test_oracle_dml() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
    );
    exec_both(
        &radixdb,
        &sqlite,
        "INSERT INTO t1 VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 300)",
    );

    // SELECT * after initial insert
    let sql = "SELECT * FROM t1 ORDER BY id";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // UPDATE
    exec_both(
        &radixdb,
        &sqlite,
        "UPDATE t1 SET value = value + 10 WHERE id = 2",
    );

    let sql = "SELECT * FROM t1 ORDER BY id";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // DELETE
    exec_both(&radixdb, &sqlite, "DELETE FROM t1 WHERE id = 3");

    let sql = "SELECT COUNT(*) FROM t1";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_select_where() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, category TEXT, value INTEGER, flag INTEGER)",
    );

    // Insert 10 rows with various data
    let inserts = [
        "INSERT INTO t VALUES (1, 'Alice', 'A', 10, 1)",
        "INSERT INTO t VALUES (2, 'Bob', 'B', 20, 0)",
        "INSERT INTO t VALUES (3, 'Carol', 'A', 30, 1)",
        "INSERT INTO t VALUES (4, 'Dave', 'C', 40, 0)",
        "INSERT INTO t VALUES (5, 'Eve', 'B', 50, 1)",
        "INSERT INTO t VALUES (6, 'Frank', 'A', 60, 0)",
        "INSERT INTO t VALUES (7, 'Grace', 'C', 70, 1)",
        "INSERT INTO t VALUES (8, 'Hank', 'B', 80, 0)",
        "INSERT INTO t VALUES (9, 'Ivy', 'A', 90, 1)",
        "INSERT INTO t VALUES (10, 'Jack', 'C', NULL, 0)",
    ];
    for insert in &inserts {
        exec_both(&radixdb, &sqlite, insert);
    }

    let queries = [
        "SELECT * FROM t WHERE value > 50",
        "SELECT * FROM t WHERE category = 'A'",
        "SELECT * FROM t WHERE value BETWEEN 20 AND 80",
        "SELECT * FROM t WHERE category IN ('A', 'C')",
        "SELECT * FROM t WHERE name LIKE 'A%'",
        "SELECT * FROM t WHERE value IS NULL",
        "SELECT * FROM t WHERE value IS NOT NULL",
        "SELECT * FROM t WHERE value > 30 AND category = 'B'",
        "SELECT * FROM t WHERE value > 90 OR category = 'A'",
        "SELECT id, name FROM t WHERE value IS NOT NULL ORDER BY value DESC LIMIT 3",
    ];

    for sql in &queries {
        let sr = query_radixdb(&radixdb, sql);
        let qr = query_sqlite(&sqlite, sql);
        normalize_and_compare(sql, sr, qr);
    }

    // LIMIT with OFFSET: RadixDB and SQLite both support this syntax
    let sql = "SELECT id, name FROM t WHERE value IS NOT NULL ORDER BY value ASC LIMIT 3 OFFSET 2";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_aggregates() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)",
    );

    let inserts = [
        "INSERT INTO t VALUES (1, 'A', 10)",
        "INSERT INTO t VALUES (2, 'A', 20)",
        "INSERT INTO t VALUES (3, 'B', 30)",
        "INSERT INTO t VALUES (4, 'B', 40)",
        "INSERT INTO t VALUES (5, 'B', 50)",
        "INSERT INTO t VALUES (6, 'C', 60)",
    ];
    for insert in &inserts {
        exec_both(&radixdb, &sqlite, insert);
    }

    // Basic aggregates without GROUP BY
    let sql = "SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM t";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // GROUP BY count
    let sql = "SELECT category, COUNT(*) FROM t GROUP BY category";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // GROUP BY avg
    let sql = "SELECT category, AVG(value) FROM t GROUP BY category";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // HAVING
    let sql = "SELECT category, COUNT(*) FROM t GROUP BY category HAVING COUNT(*) > 1";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // COUNT DISTINCT
    let sql = "SELECT COUNT(DISTINCT category) FROM t";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // Empty result set
    let sql = "SELECT COUNT(*) FROM t WHERE id = 999";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // NULL result from aggregate on empty set
    let sql = "SELECT SUM(value) FROM t WHERE id = 999";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_joins() {
    let (radixdb, sqlite) = setup_both();

    // Create employees table
    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
    );
    // Create departments table
    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, dept_name TEXT)",
    );
    // Create projects table for 3-way join
    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE proj (id INTEGER PRIMARY KEY, proj_name TEXT, dept_id INTEGER)",
    );

    // Insert departments
    let dept_inserts = [
        "INSERT INTO dept VALUES (1, 'Engineering')",
        "INSERT INTO dept VALUES (2, 'Marketing')",
        "INSERT INTO dept VALUES (3, 'Sales')",
    ];
    for ins in &dept_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // Insert employees (some with dept_id that exists, one with NULL dept)
    let emp_inserts = [
        "INSERT INTO emp VALUES (1, 'Alice', 1)",
        "INSERT INTO emp VALUES (2, 'Bob', 2)",
        "INSERT INTO emp VALUES (3, 'Carol', 1)",
        "INSERT INTO emp VALUES (4, 'Dave', NULL)",
        "INSERT INTO emp VALUES (5, 'Eve', 3)",
    ];
    for ins in &emp_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // Insert projects
    let proj_inserts = [
        "INSERT INTO proj VALUES (1, 'ProjectX', 1)",
        "INSERT INTO proj VALUES (2, 'ProjectY', 2)",
    ];
    for ins in &proj_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // INNER JOIN
    let sql = "SELECT e.name, d.dept_name FROM emp e INNER JOIN dept d ON e.dept_id = d.id";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // LEFT JOIN (Dave should have NULL dept_name)
    let sql = "SELECT e.name, d.dept_name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // CROSS JOIN with ORDER BY for deterministic output
    let sql =
        "SELECT e.name, d.dept_name FROM emp e CROSS JOIN dept d ORDER BY e.name, d.dept_name";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // Multi-table join (3 tables)
    let sql = "SELECT e.name, d.dept_name, p.proj_name FROM emp e INNER JOIN dept d ON e.dept_id = d.id INNER JOIN proj p ON d.id = p.dept_id";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_subqueries() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)",
    );
    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER)",
    );

    let t_inserts = [
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO t VALUES (2, 50)",
        "INSERT INTO t VALUES (3, 80)",
        "INSERT INTO t VALUES (4, 30)",
    ];
    for ins in &t_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    let t2_inserts = [
        "INSERT INTO t2 VALUES (1, 1)",
        "INSERT INTO t2 VALUES (2, 1)",
        "INSERT INTO t2 VALUES (3, 3)",
    ];
    for ins in &t2_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // IN subquery
    let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE value > 50)";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // EXISTS subquery
    let sql = "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref_id = t.id)";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // Scalar subquery
    let sql = "SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.ref_id = t.id) AS cnt FROM t";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_expressions() {
    let (radixdb, sqlite) = setup_both();

    // Arithmetic expressions
    let sql = "SELECT 2 + 3, 10 - 4, 3 * 7, 15 / 4, 17 % 5";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // CASE expression
    let sql = "SELECT CASE WHEN 5 > 3 THEN 'yes' ELSE 'no' END";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // COALESCE
    let sql = "SELECT COALESCE(NULL, NULL, 'default')";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // NULLIF
    let sql = "SELECT NULLIF(10, 10), NULLIF(10, 20)";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // CAST
    let sql = "SELECT CAST(42 AS TEXT), CAST('123' AS INTEGER)";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_string_functions() {
    let (radixdb, sqlite) = setup_both();

    // UPPER and LOWER
    let sql = "SELECT UPPER('hello'), LOWER('HELLO')";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // LENGTH
    let sql = "SELECT LENGTH('hello'), LENGTH('')";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // SUBSTRING vs SUBSTR: run different SQL on each engine, compare results
    let radixdb_sql = "SELECT SUBSTRING('hello world', 1, 5)";
    let sqlite_sql = "SELECT SUBSTR('hello world', 1, 5)";
    let sr = query_radixdb(&radixdb, radixdb_sql);
    let qr = query_sqlite(&sqlite, sqlite_sql);
    normalize_and_compare("SUBSTRING/SUBSTR('hello world', 1, 5)", sr, qr);

    // REPLACE
    let sql = "SELECT REPLACE('hello world', 'world', 'there')";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // TRIM
    let sql = "SELECT TRIM('  hello  ')";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_set_operations() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY)",
    );
    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY)",
    );

    // t1: {1, 2, 3, 4}
    let t1_inserts = [
        "INSERT INTO t1 VALUES (1)",
        "INSERT INTO t1 VALUES (2)",
        "INSERT INTO t1 VALUES (3)",
        "INSERT INTO t1 VALUES (4)",
    ];
    for ins in &t1_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // t2: {3, 4, 5, 6}
    let t2_inserts = [
        "INSERT INTO t2 VALUES (3)",
        "INSERT INTO t2 VALUES (4)",
        "INSERT INTO t2 VALUES (5)",
        "INSERT INTO t2 VALUES (6)",
    ];
    for ins in &t2_inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // UNION (deduplicates)
    let sql = "SELECT id FROM t1 UNION SELECT id FROM t2";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // UNION ALL (preserves duplicates)
    let sql = "SELECT id FROM t1 UNION ALL SELECT id FROM t2";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // INTERSECT
    let sql = "SELECT id FROM t1 INTERSECT SELECT id FROM t2";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);

    // EXCEPT
    let sql = "SELECT id FROM t1 EXCEPT SELECT id FROM t2";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    normalize_and_compare(sql, sr, qr);
}

#[test]
fn test_oracle_order_by() {
    let (radixdb, sqlite) = setup_both();

    exec_both(
        &radixdb,
        &sqlite,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, grade TEXT)",
    );

    let inserts = [
        "INSERT INTO t VALUES (1, 'Alice', 85, 'B')",
        "INSERT INTO t VALUES (2, 'Bob', 92, 'A')",
        "INSERT INTO t VALUES (3, 'Carol', 78, 'C')",
        "INSERT INTO t VALUES (4, 'Dave', 92, 'A')",
        "INSERT INTO t VALUES (5, 'Eve', NULL, 'B')",
        "INSERT INTO t VALUES (6, 'Frank', 65, 'D')",
    ];
    for ins in &inserts {
        exec_both(&radixdb, &sqlite, ins);
    }

    // ORDER BY single column ASC - add , id tiebreaker for deterministic order on score ties (92, 92)
    let sql = "SELECT id, name, score FROM t WHERE score IS NOT NULL ORDER BY score ASC, id ASC";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    assert_ordered_equal(sql, &sr, &qr);

    // ORDER BY single column DESC - add , id tiebreaker
    let sql = "SELECT id, name, score FROM t WHERE score IS NOT NULL ORDER BY score DESC, id ASC";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    assert_ordered_equal(sql, &sr, &qr);

    // ORDER BY multiple columns - add , id tiebreaker within same (grade, score)
    let sql = "SELECT id, name, score, grade FROM t WHERE score IS NOT NULL ORDER BY grade ASC, score DESC, id ASC";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    assert_ordered_equal(sql, &sr, &qr);

    // ORDER BY with LIMIT - add , id tiebreaker
    let sql = "SELECT id, name FROM t WHERE score IS NOT NULL ORDER BY score DESC, id ASC LIMIT 3";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    assert_ordered_equal(sql, &sr, &qr);

    // RadixDB follows the PostgreSQL default (ASC NULLS LAST), while SQLite's
    // implicit ASC order places NULL first. A differential oracle must request
    // one explicit policy from both engines instead of comparing their defaults.
    let sql = "SELECT id, name, score FROM t ORDER BY score ASC NULLS LAST, id ASC";
    let sr = query_radixdb(&radixdb, sql);
    let qr = query_sqlite(&sqlite, sql);
    assert_ordered_equal(sql, &sr, &qr);
}
