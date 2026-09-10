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

//! NR-08 truth-table and rewrite contract for navigation in WHERE.

use radixdb::{named_params, Database};

fn fixture(name: &str) -> Database {
    let db = Database::open(&format!("memory://navigable_references_where_{name}"))
        .expect("open navigation predicate fixture");
    db.execute(
        "CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            label TEXT,
            code TEXT NOT NULL
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
    db.execute(
        "INSERT INTO departments VALUES
            (10, 'Finance', 'FIN'),
            (20, 'Engineering', 'ENG'),
            (30, NULL, 'UNC')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (100, 'Alice', 10),
            (101, 'Bob', 10),
            (102, 'Carol', 20),
            (103, 'Dave', NULL),
            (104, 'Eve', 30)",
        (),
    )
    .unwrap();
    db
}

fn rows(db: &Database, sql: &str) -> Vec<(i64, Option<String>)> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect()
}

fn explain(db: &Database, sql: &str) -> String {
    db.query(&format!("EXPLAIN ANALYZE {sql}"), ())
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn navigable_where_truth_table_matches_explicit_left_join() {
    let db = fixture("truth_table");
    let cases = [
        ("department_id.label = 'Finance'", "d.label = 'Finance'"),
        ("department_id.label <> 'Finance'", "d.label <> 'Finance'"),
        (
            "department_id.label IN ('Finance', 'Engineering')",
            "d.label IN ('Finance', 'Engineering')",
        ),
        (
            "department_id.label BETWEEN 'Engineering' AND 'Finance'",
            "d.label BETWEEN 'Engineering' AND 'Finance'",
        ),
        ("department_id.label LIKE 'Fin%'", "d.label LIKE 'Fin%'"),
        ("department_id.label IS NULL", "d.label IS NULL"),
        (
            "CASE WHEN department_id.label IS NULL THEN FALSE \
             ELSE department_id.label = 'Finance' END",
            "CASE WHEN d.label IS NULL THEN FALSE ELSE d.label = 'Finance' END",
        ),
        (
            "LOWER(department_id.label) = 'finance'",
            "LOWER(d.label) = 'finance'",
        ),
        (
            "department_id.label = 'Finance' OR id = 103",
            "d.label = 'Finance' OR e.id = 103",
        ),
        (
            "NOT (department_id.label = 'Finance')",
            "NOT (d.label = 'Finance')",
        ),
        (
            "id >= 101 AND department_id.label IS NOT NULL",
            "e.id >= 101 AND d.label IS NOT NULL",
        ),
    ];

    for (index, (navigation_predicate, join_predicate)) in cases.iter().enumerate() {
        let navigated = rows(
            &db,
            &format!(
                "SELECT id, department_id.label
                 FROM employees
                 WHERE {navigation_predicate}
                 ORDER BY id"
            ),
        );
        let explicit = rows(
            &db,
            &format!(
                "SELECT e.id, d.label
                 FROM employees e
                 LEFT JOIN departments d ON e.department_id = d.id
                 WHERE {join_predicate}
                 ORDER BY e.id"
            ),
        );
        assert_eq!(navigated, explicit, "truth-table case {index}");
    }
}

#[test]
fn only_proven_null_rejecting_conjuncts_push_to_target() {
    let db = fixture("pushdown_proof");

    let equality = explain(
        &db,
        "SELECT id, department_id.label
         FROM employees
         WHERE department_id.label = 'Finance'
         ORDER BY id",
    );
    assert!(
        equality.contains("Target Predicate Pushdown: enabled"),
        "{equality}"
    );
    assert!(
        equality.contains("LEFT-to-INNER: proven_null_rejecting"),
        "{equality}"
    );
    assert!(
        equality.contains("Rejected Distinct Keys: 2"),
        "predicate must run once for three distinct targets, not once per source row:\n{equality}"
    );

    for predicate in [
        "department_id.label IS NULL",
        "department_id.label = 'Finance' OR id = 103",
        "NOT (department_id.label = 'Finance')",
        "LOWER(department_id.label) = 'finance'",
    ] {
        let plan = explain(
            &db,
            &format!(
                "SELECT id, department_id.label
                 FROM employees
                 WHERE {predicate}
                 ORDER BY id"
            ),
        );
        assert!(
            plan.contains("Target Predicate Pushdown: disabled"),
            "unsafe predicate was pushed: {predicate}\n{plan}"
        );
        assert!(
            plan.contains("LEFT-to-INNER: disabled"),
            "unsafe LEFT-to-INNER rewrite: {predicate}\n{plan}"
        );
    }
}

#[test]
fn navigable_where_supports_star_alias_parameters_and_post_filter_limit() {
    let db = fixture("surface");

    let all: Vec<(i64, String, Option<i64>)> = db
        .query_named(
            "SELECT *
             FROM employees e
             WHERE LOWER(e.department_id.label) = LOWER(:label)
             ORDER BY e.id
             LIMIT 1 OFFSET 1",
            named_params! { label: "FINANCE" },
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            )
        })
        .collect();
    assert_eq!(all, vec![(101, "Bob".to_string(), Some(10))]);

    let projected: Vec<(i64, Option<String>)> = db
        .query_named(
            "SELECT e.id, e.department_id.label AS department
             FROM employees e
             WHERE e.department_id.label IN (:first, :second)
             ORDER BY e.id",
            named_params! { first: "Finance", second: "Engineering" },
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();
    assert_eq!(
        projected,
        vec![
            (100, Some("Finance".to_string())),
            (101, Some("Finance".to_string())),
            (102, Some("Engineering".to_string())),
        ]
    );

    let rebound: Vec<(i64, Option<String>)> = db
        .query_named(
            "SELECT e.id, e.department_id.label AS department
             FROM employees e
             WHERE e.department_id.label IN (:first, :second)
             ORDER BY e.id",
            named_params! { first: "Engineering", second: "missing" },
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();
    assert_eq!(rebound, vec![(102, Some("Engineering".to_string()))]);

    let positional_order = db.query(
        "SELECT name, department_id.label
         FROM employees
         WHERE department_id.label IS NOT NULL
         ORDER BY 1",
        (),
    );
    let error = match positional_order {
        Ok(_) => panic!("positional ORDER BY must remain behind the NR-10 boundary"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("requires NR-10"), "{error}");
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_explicit_left_join_is_a_differential_oracle_for_navigation_where() {
    use rusqlite::Connection;

    let db = fixture("sqlite_differential");
    let sqlite = Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE departments (
                id INTEGER PRIMARY KEY,
                label TEXT,
                code TEXT NOT NULL
            );
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department_id INTEGER REFERENCES departments(id)
            );
            INSERT INTO departments VALUES
                (10, 'Finance', 'FIN'),
                (20, 'Engineering', 'ENG'),
                (30, NULL, 'UNC');
            INSERT INTO employees VALUES
                (100, 'Alice', 10),
                (101, 'Bob', 10),
                (102, 'Carol', 20),
                (103, 'Dave', NULL),
                (104, 'Eve', 30);",
        )
        .unwrap();

    let predicates = [
        ("department_id.label = 'Finance'", "d.label = 'Finance'"),
        (
            "department_id.label IN ('Finance', 'Engineering')",
            "d.label IN ('Finance', 'Engineering')",
        ),
        (
            "department_id.label BETWEEN 'Engineering' AND 'Finance'",
            "d.label BETWEEN 'Engineering' AND 'Finance'",
        ),
        ("department_id.label LIKE 'Fin%'", "d.label LIKE 'Fin%'"),
        ("department_id.label IS NULL", "d.label IS NULL"),
        (
            "department_id.label = 'Finance' OR id = 103",
            "d.label = 'Finance' OR e.id = 103",
        ),
    ];
    for (navigation, explicit) in predicates {
        let actual = rows(
            &db,
            &format!(
                "SELECT id, department_id.label FROM employees
                 WHERE {navigation} ORDER BY id"
            ),
        );
        let mut statement = sqlite
            .prepare(&format!(
                "SELECT e.id, d.label FROM employees e
                 LEFT JOIN departments d ON e.department_id = d.id
                 WHERE {explicit} ORDER BY e.id"
            ))
            .unwrap();
        let expected = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<(i64, Option<String>)>, _>>()
            .unwrap();
        assert_eq!(actual, expected, "SQLite differential: {navigation}");
    }
}
