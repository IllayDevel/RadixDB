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

//! NR-11 permanent read-only boundary for navigable references.

use radixdb::{Database, Error};

const READ_ONLY: &str = "NAVIGATION_READ_ONLY";
const UNSUPPORTED: &str = "NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE";
const NOT_A_REFERENCE: &str = "NAVIGATION_NOT_A_REFERENCE";

fn setup(name: &str) -> Database {
    let db = Database::open(&format!("memory://navigation_nr11_{name}")).unwrap();
    db.execute(
        "CREATE TABLE profiles (
            id INTEGER PRIMARY KEY,
            display_name TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            profile_id INTEGER REFERENCES profiles(id)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department_id INTEGER REFERENCES departments(id)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO profiles VALUES (1, 'Finance profile')", ())
        .unwrap();
    db.execute("INSERT INTO departments VALUES (10, 'Finance', 1)", ())
        .unwrap();
    db.execute("INSERT INTO employees VALUES (100, 'Alice', 10)", ())
        .unwrap();
    db
}

fn execute_error(db: &Database, sql: &str) -> Error {
    match db.execute(sql, ()) {
        Ok(_) => panic!("statement unexpectedly succeeded: {sql}"),
        Err(error) => error,
    }
}

fn assert_error_code(db: &Database, sql: &str, code: &str) {
    let error = execute_error(db, sql).to_string();
    assert!(
        error.contains(code),
        "expected {code} for `{sql}`, got: {error}"
    );
}

fn employee_name(db: &Database, id: i64) -> String {
    db.query("SELECT name FROM employees WHERE id = ?", (id,))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

#[test]
fn navigation_is_rejected_across_every_write_expression_context() {
    let db = setup("matrix");
    for sql in [
        "UPDATE employees SET name = department_id.label WHERE id = 100",
        "UPDATE employees SET name = 'blocked' \
         WHERE department_id.label = 'Finance'",
        "UPDATE employees SET name = 'blocked' \
         WHERE id = 100 RETURNING department_id.label",
        "DELETE FROM employees WHERE department_id.label = 'Finance'",
        "DELETE FROM employees AS e WHERE e.department_id.label = 'Finance'",
        "DELETE FROM employees WHERE id = 100 RETURNING department_id.label",
        "INSERT INTO employees (id, name, department_id) \
         SELECT id + 1000, department_id.label, department_id FROM employees",
        "INSERT INTO employees (id, name, department_id) \
         WITH enriched AS (
             SELECT e.id + 1000 AS id,
                    e.department_id.label AS name,
                    e.department_id
             FROM employees e
         )
         SELECT id, name, department_id FROM enriched",
        "INSERT INTO employees (id, name, department_id) \
         SELECT 101, 'safe branch', 10
         UNION ALL
         SELECT e.id + 1000, e.department_id.label, e.department_id FROM employees e",
        "INSERT INTO employees VALUES (101, 'Bob', 10) \
         RETURNING department_id.label",
        "INSERT INTO employees VALUES (100, 'blocked', 10) \
         ON DUPLICATE KEY UPDATE name = department_id.label",
        "UPDATE employees SET name = (SELECT d.profile_id.display_name \
         FROM departments d WHERE d.id = employees.department_id) WHERE id = 100",
        "UPDATE employees SET name = (SELECT department_id.label) WHERE id = 100",
        "UPDATE employees SET name = 'blocked' WHERE EXISTS \
         (SELECT 1 WHERE employees.department_id.label = 'Finance')",
        "EXPLAIN UPDATE employees SET name = 'blocked' \
         WHERE department_id.label = 'Finance'",
        "CREATE TABLE employee_copy AS \
         SELECT e.id, e.department_id.label FROM employees e",
    ] {
        assert_error_code(&db, sql, READ_ONLY);
        assert_eq!(employee_name(&db, 100), "Alice", "side effect from `{sql}`");
    }

    let cached = "UPDATE employees SET name = 'cached-blocked' \
                  WHERE department_id.label = 'Finance'";
    assert_error_code(&db, cached, READ_ONLY);
    assert_error_code(&db, cached, READ_ONLY);
    assert_eq!(employee_name(&db, 100), "Alice");

    assert!(
        db.query("SELECT * FROM employee_copy", ()).is_err(),
        "rejected CTAS published its target table"
    );
}

#[test]
fn navigation_lvalues_and_persisted_view_definitions_are_rejected() {
    let db = setup("targets");
    for sql in [
        "UPDATE employees SET department_id.label = 'blocked' WHERE id = 100",
        "INSERT INTO employees (id, department_id.label) VALUES (101, 'blocked')",
        "INSERT INTO employees (id, name, department_id) VALUES (100, 'x', 10) \
         ON DUPLICATE KEY UPDATE department_id.label = 'blocked'",
        "INSERT INTO employees (id, name, department_id) VALUES (101, 'x', 10) \
         ON CONFLICT (department_id.label) DO NOTHING",
    ] {
        assert_error_code(&db, sql, READ_ONLY);
    }
    assert_error_code(
        &db,
        "CREATE VIEW forbidden_navigation AS \
         SELECT e.department_id.label FROM employees e",
        UNSUPPORTED,
    );
    assert!(
        db.query("SELECT * FROM forbidden_navigation", ()).is_err(),
        "rejected CREATE VIEW published a catalog object"
    );
    assert_eq!(employee_name(&db, 100), "Alice");
}

#[test]
fn reverse_collection_guess_is_rejected_without_touching_rows() {
    let db = setup("reverse");
    assert_error_code(
        &db,
        "UPDATE departments SET label = 'blocked' \
         WHERE departments.employees.name = 'Alice'",
        NOT_A_REFERENCE,
    );
    let label: String = db
        .query("SELECT label FROM departments WHERE id = 10", ())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(label, "Finance");
}

#[test]
fn ordinary_explicit_dml_subqueries_remain_available() {
    let db = setup("explicit");
    db.execute(
        "UPDATE employees
         SET name = (SELECT d.label FROM departments d WHERE d.id = 10)
         WHERE id = 100",
        (),
    )
    .unwrap();
    assert_eq!(employee_name(&db, 100), "Finance");

    db.execute(
        "INSERT INTO employees (id, name, department_id)
         SELECT e.id + 1, d.label, e.department_id
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         WHERE e.id = 100",
        (),
    )
    .unwrap();
    assert_eq!(employee_name(&db, 101), "Finance");

    db.execute(
        "DELETE FROM employees
         WHERE EXISTS (
             SELECT 1 FROM departments d
             WHERE d.id = employees.department_id AND d.label = 'Finance'
         )",
        (),
    )
    .unwrap();
    let count: i64 = db
        .query("SELECT COUNT(*) FROM employees", ())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn rejection_does_not_poison_an_explicit_transaction() {
    let db = setup("transaction");
    db.execute("BEGIN", ()).unwrap();
    assert_error_code(
        &db,
        "UPDATE employees SET name = 'blocked' \
         WHERE department_id.label = 'Finance'",
        READ_ONLY,
    );
    db.execute("UPDATE employees SET name = 'kept' WHERE id = 100", ())
        .unwrap();
    db.execute("COMMIT", ()).unwrap();
    assert_eq!(employee_name(&db, 100), "kept");
}
