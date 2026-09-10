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

//! Tests for parser edge cases: NULL bytes, numeric boundaries, etc.

use radixdb::api::Database;

/// Test that NULL bytes in string literals produce a clear error message
#[test]
fn test_null_byte_in_string_literal() {
    let db = Database::open_in_memory().expect("Failed to create database");

    // Create a string with an embedded NULL byte
    let sql_with_null = "SELECT 'hello\0world'";

    let result = db.query(sql_with_null, ());
    match result {
        Ok(_) => panic!("NULL byte should cause an error"),
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("NULL byte") || err_msg.contains("0x00"),
                "Error message should mention NULL byte, got: {}",
                err_msg
            );
        }
    }
}

/// Test that unterminated strings produce a clear error message
#[test]
fn test_unterminated_string_literal() {
    let db = Database::open_in_memory().expect("Failed to create database");

    let result = db.query("SELECT 'unterminated", ());
    match result {
        Ok(_) => panic!("Unterminated string should cause an error"),
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("unterminated"),
                "Error message should mention unterminated, got: {}",
                err_msg
            );
        }
    }
}

/// Test that an integer just above i64::MAX is rejected without mutation.
#[test]
fn test_integer_overflow_is_rejected() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE overflow_test (id INTEGER PRIMARY KEY, val FLOAT)",
        (),
    )
    .expect("Failed to create table");

    let error = db
        .execute(
            "INSERT INTO overflow_test VALUES (1, 9223372036854775808)",
            (),
        )
        .expect_err("out-of-range integer literal must not be rounded to float");
    assert!(
        error.to_string().contains("out of range for i64"),
        "unexpected error: {error}"
    );

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM overflow_test", ())
        .expect("failed to verify rejected insert");
    assert_eq!(count, 0);
}

/// Test f64::MAX can be inserted and queried
#[test]
fn test_f64_max() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE boundary_test (id INTEGER PRIMARY KEY, intval BIGINT, floatval FLOAT)",
        (),
    )
    .expect("Failed to create table");

    // This is the exact query from the stress test
    db.execute(
        "INSERT INTO boundary_test VALUES (0, 9223372036854775807, 1.7976931348623157e+308)",
        (),
    )
    .expect("Should handle i64::MAX and f64::MAX");

    let intval: i64 = db
        .query_one("SELECT intval FROM boundary_test WHERE id = 0", ())
        .expect("Failed to query intval");

    let floatval: f64 = db
        .query_one("SELECT floatval FROM boundary_test WHERE id = 0", ())
        .expect("Failed to query floatval");

    assert_eq!(intval, i64::MAX, "intval should be i64::MAX");
    assert!(floatval > 1e307, "floatval should be very large");
}

/// Test very small floats (near f64::MIN_POSITIVE)
#[test]
fn test_very_small_floats() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE small_float_test (id INTEGER PRIMARY KEY, val FLOAT)",
        (),
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO small_float_test VALUES (1, 1e-308)", ())
        .expect("Should handle very small float");

    let val: f64 = db
        .query_one("SELECT val FROM small_float_test WHERE id = 1", ())
        .expect("Failed to query");

    // Value should be positive and very small, but not zero
    assert!(val > 0.0, "Value should be positive");
    assert!(val < 1e-300, "Value should be very small");
}

/// Test scientific notation parsing
#[test]
fn test_scientific_notation() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE sci_test (id INTEGER PRIMARY KEY, val FLOAT)",
        (),
    )
    .expect("Failed to create table");

    // Various scientific notation formats
    db.execute("INSERT INTO sci_test VALUES (1, 1e10)", ())
        .expect("1e10");
    db.execute("INSERT INTO sci_test VALUES (2, 1E10)", ())
        .expect("1E10");
    db.execute("INSERT INTO sci_test VALUES (3, 1.5e10)", ())
        .expect("1.5e10");
    db.execute("INSERT INTO sci_test VALUES (4, 1.5E-10)", ())
        .expect("1.5E-10");
    db.execute("INSERT INTO sci_test VALUES (5, -1e10)", ())
        .expect("-1e10");

    let val1: f64 = db
        .query_one("SELECT val FROM sci_test WHERE id = 1", ())
        .expect("query 1");
    let val2: f64 = db
        .query_one("SELECT val FROM sci_test WHERE id = 2", ())
        .expect("query 2");
    let val3: f64 = db
        .query_one("SELECT val FROM sci_test WHERE id = 3", ())
        .expect("query 3");
    let val4: f64 = db
        .query_one("SELECT val FROM sci_test WHERE id = 4", ())
        .expect("query 4");
    let val5: f64 = db
        .query_one("SELECT val FROM sci_test WHERE id = 5", ())
        .expect("query 5");

    assert!((val1 - 1e10).abs() < 1e5);
    assert!((val2 - 1e10).abs() < 1e5);
    assert!((val3 - 1.5e10).abs() < 1e5);
    assert!((val4 - 1.5e-10).abs() < 1e-15);
    assert!((val5 - (-1e10)).abs() < 1e5);
}

/// Float literals that overflow the finite SQL domain are rejected.
#[test]
fn test_float_overflow_is_rejected() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE special_float_test (id INTEGER PRIMARY KEY, val FLOAT)",
        (),
    )
    .expect("Failed to create table");

    let error = db
        .execute("INSERT INTO special_float_test VALUES (1, 1e309)", ())
        .expect_err("overflowing finite literal must be rejected");
    assert!(error.to_string().contains("out of finite f64 range"));

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM special_float_test", ())
        .expect("Failed to query");
    assert_eq!(count, 0, "rejected literal must not write a row");
}

/// Test that normal strings work correctly
#[test]
fn test_normal_strings() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE string_test (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO string_test VALUES (1, 'hello world')", ())
        .expect("Simple string");
    db.execute("INSERT INTO string_test VALUES (2, 'with ''quotes''')", ())
        .expect("Escaped quotes");
    db.execute("INSERT INTO string_test VALUES (3, '')", ())
        .expect("Empty string");
    db.execute(
        "INSERT INTO string_test VALUES (4, 'unicode: café ñ 日本語')",
        (),
    )
    .expect("Unicode string");

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM string_test", ())
        .expect("Failed to count");

    assert_eq!(count, 4);
}

/// Test quoted identifiers
#[test]
fn test_quoted_identifiers() {
    let db = Database::open_in_memory().expect("Failed to create database");

    // Create table with reserved word as column name
    db.execute(
        "CREATE TABLE quoted_id_test (id INTEGER PRIMARY KEY, \"select\" TEXT, `from` TEXT)",
        (),
    )
    .expect("Failed to create table with quoted identifiers");

    db.execute(
        "INSERT INTO quoted_id_test VALUES (1, 'sel_val', 'from_val')",
        (),
    )
    .expect("Failed to insert");

    let sel: String = db
        .query_one("SELECT \"select\" FROM quoted_id_test WHERE id = 1", ())
        .expect("Failed to query select");

    let frm: String = db
        .query_one("SELECT `from` FROM quoted_id_test WHERE id = 1", ())
        .expect("Failed to query from");

    assert_eq!(sel, "sel_val");
    assert_eq!(frm, "from_val");
}

/// Test that a much larger integer literal is also rejected rather than rounded.
#[test]
fn test_large_integer_without_decimal_is_rejected() {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute(
        "CREATE TABLE large_int_test (id INTEGER PRIMARY KEY, val FLOAT)",
        (),
    )
    .expect("Failed to create table");

    let error = db
        .execute(
            "INSERT INTO large_int_test VALUES (1, 999999999999999999999999999999)",
            (),
        )
        .expect_err("integer syntax outside i64 must fail closed");
    assert!(
        error.to_string().contains("out of range for i64"),
        "unexpected error: {error}"
    );

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM large_int_test", ())
        .expect("failed to verify rejected insert");
    assert_eq!(count, 0);
}
