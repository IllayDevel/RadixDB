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

//! NR-10 parity for navigable references in relational expression contexts.

use radixdb::{Database, Value};

fn setup(name: &str) -> Database {
    let db = Database::open(&format!("memory://navigation_nr10_{name}")).unwrap();
    db.execute(
        "CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            rank INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department_id INTEGER REFERENCES departments(id),
            salary INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE bands (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            lo INTEGER NOT NULL,
            hi INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO departments VALUES
            (10, 'Finance', 1),
            (20, 'Engineering', 2),
            (30, 'Support', 3)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1, 'Alice', 10, 100),
            (2, 'Bob', 10, 120),
            (3, 'Carol', 20, 200),
            (4, 'Dave', 20, 210),
            (5, 'Eve', 30, 90),
            (6, 'Nobody', NULL, 50)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO bands VALUES
            (1, 'low', 1, 1),
            (2, 'middle', 2, 2),
            (3, 'high', 3, 3)",
        (),
    )
    .unwrap();
    db
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
        .map(|row| row.unwrap().into_inner().into_values())
        .collect()
}

fn explain(db: &Database, sql: &str) -> String {
    db.query(&format!("EXPLAIN ANALYZE {sql}"), ())
        .unwrap_or_else(|error| panic!("explain failed: {sql}\n{error}"))
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn join_on_navigation_matches_an_explicit_left_join_edge() {
    let db = setup("join_on");
    let navigation = rows(
        &db,
        "SELECT e.id, b.name
         FROM employees e
         JOIN bands b
           ON e.department_id.rank BETWEEN b.lo AND b.hi
         ORDER BY e.id",
    );
    let explicit = rows(
        &db,
        "SELECT e.id, b.name
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         JOIN bands b ON d.rank BETWEEN b.lo AND b.hi
         ORDER BY e.id",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn grouping_having_aggregate_distinct_and_order_use_one_reference_edge() {
    let db = setup("aggregate");
    let navigation = rows(
        &db,
        "SELECT e.department_id.label AS department,
                COUNT(DISTINCT e.name) AS people,
                SUM(e.salary) AS payroll
         FROM employees e
         GROUP BY e.department_id.label
         HAVING COUNT(*) >= 1
         ORDER BY e.department_id.label ASC NULLS LAST",
    );
    let explicit = rows(
        &db,
        "SELECT d.label AS department,
                COUNT(DISTINCT e.name) AS people,
                SUM(e.salary) AS payroll
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         GROUP BY d.label
         HAVING COUNT(*) >= 1
         ORDER BY d.label ASC NULLS LAST",
    );
    assert_eq!(navigation, explicit);

    let navigation = rows(
        &db,
        "SELECT COUNT(DISTINCT e.department_id.label),
                MAX(e.department_id.rank)
         FROM employees e
         HAVING MAX(e.department_id.rank) >= 2",
    );
    let explicit = rows(
        &db,
        "SELECT COUNT(DISTINCT d.label), MAX(d.rank)
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         HAVING MAX(d.rank) >= 2",
    );
    assert_eq!(navigation, explicit);

    let navigation = rows(
        &db,
        "SELECT DISTINCT e.department_id.label
         FROM employees e
         ORDER BY e.department_id.label DESC NULLS FIRST",
    );
    let explicit = rows(
        &db,
        "SELECT DISTINCT d.label
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         ORDER BY d.label DESC NULLS FIRST",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn navigation_in_window_partition_order_and_argument_matches_explicit_join() {
    let db = setup("window");
    let navigation = rows(
        &db,
        "SELECT e.id,
                ROW_NUMBER() OVER (
                    PARTITION BY e.department_id.label
                    ORDER BY e.department_id.rank, e.salary DESC
                ) AS rn,
                MAX(e.department_id.rank) OVER (
                    PARTITION BY e.department_id.label
                ) AS department_rank
         FROM employees e
         ORDER BY e.id",
    );
    let explicit = rows(
        &db,
        "SELECT e.id,
                ROW_NUMBER() OVER (
                    PARTITION BY d.label
                    ORDER BY d.rank, e.salary DESC
                ) AS rn,
                MAX(d.rank) OVER (PARTITION BY d.label) AS department_rank
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         ORDER BY e.id",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn direct_navigation_result_name_is_canonical_and_alias_is_preserved() {
    let db = setup("metadata");
    let rows = db
        .query(
            "SELECT e.department_id.label,
                    e.department_id.rank AS department_rank
             FROM employees e
             ORDER BY e.id",
            (),
        )
        .unwrap();
    assert_eq!(
        rows.columns(),
        &["e.department_id.label", "department_rank"]
    );
}

#[test]
fn navigation_executes_inside_cte_derived_and_scalar_subquery_scopes() {
    let db = setup("nested_scopes");

    let navigation = rows(
        &db,
        "WITH enriched AS (
             SELECT e.id, e.department_id.label AS department
             FROM employees e
         )
         SELECT id, department FROM enriched ORDER BY id",
    );
    let explicit = rows(
        &db,
        "WITH enriched AS (
             SELECT e.id, d.label AS department
             FROM employees e
             LEFT JOIN departments d ON e.department_id = d.id
         )
         SELECT id, department FROM enriched ORDER BY id",
    );
    assert_eq!(navigation, explicit);

    let navigation = rows(
        &db,
        "SELECT q.id, q.department
         FROM (
             SELECT e.id, e.department_id.label AS department
             FROM employees e
         ) q
         ORDER BY q.id",
    );
    let explicit = rows(
        &db,
        "SELECT q.id, q.department
         FROM (
             SELECT e.id, d.label AS department
             FROM employees e
             LEFT JOIN departments d ON e.department_id = d.id
         ) q
         ORDER BY q.id",
    );
    assert_eq!(navigation, explicit);

    let explicit = rows(
        &db,
        "SELECT e.id, (SELECT d.label) AS department
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         ORDER BY e.id",
    );
    let navigation = rows(
        &db,
        "SELECT e.id, (SELECT e.department_id.label) AS department
         FROM employees e
         ORDER BY e.id",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn correlated_exists_and_set_operation_scopes_match_explicit_joins() {
    let db = setup("correlated_set");

    let navigation = rows(
        &db,
        "SELECT e.id
         FROM employees e
         WHERE EXISTS (
             SELECT 1 FROM bands b
             WHERE b.lo = e.department_id.rank
         )
         ORDER BY e.id",
    );
    let explicit = rows(
        &db,
        "SELECT e.id
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         WHERE EXISTS (SELECT 1 FROM bands b WHERE b.lo = d.rank)
         ORDER BY e.id",
    );
    assert_eq!(navigation, explicit);

    let navigation = rows(
        &db,
        "SELECT e.department_id.label AS department
         FROM employees e WHERE e.id <= 2
         UNION ALL
         SELECT e.department_id.label AS department
         FROM employees e WHERE e.id >= 5
         ORDER BY department NULLS LAST",
    );
    let explicit = rows(
        &db,
        "SELECT d.label AS department
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         WHERE e.id <= 2
         UNION ALL
         SELECT d.label AS department
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         WHERE e.id >= 5
         ORDER BY department NULLS LAST",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn nested_alias_shadowing_keeps_navigation_in_its_lexical_scope() {
    let db = setup("shadowing");
    let navigation = rows(
        &db,
        "SELECT e.department_id.label AS outer_department,
                (SELECT e.department_id.label
                 FROM employees e
                 WHERE e.id = 1) AS inner_department
         FROM employees e
         WHERE e.id = 3",
    );
    let explicit = rows(
        &db,
        "SELECT outer_d.label AS outer_department,
                (SELECT inner_d.label
                 FROM employees inner_e
                 LEFT JOIN departments inner_d
                   ON inner_e.department_id = inner_d.id
                 WHERE inner_e.id = 1) AS inner_department
         FROM employees outer_e
         LEFT JOIN departments outer_d
           ON outer_e.department_id = outer_d.id
         WHERE outer_e.id = 3",
    );
    assert_eq!(navigation, explicit);
}

#[test]
fn explain_reports_batch_graph_for_navigation_aggregation() {
    let db = setup("explain");
    let plan = explain(
        &db,
        "SELECT e.department_id.label, COUNT(*)
         FROM employees e
         GROUP BY e.department_id.label",
    );
    assert!(plan.contains("Reference Navigation"), "{plan}");
    assert!(plan.contains("Semantics: LEFT"), "{plan}");
    assert!(plan.contains("lookup_batches=1"), "{plan}");
    assert!(
        plan.contains("Actual Strategy: index_nested_loop"),
        "{plan}"
    );
    assert!(!plan.contains("planner_left_join"), "{plan}");
    assert!(plan.contains("Integrity Check: enabled"), "{plan}");
}

#[test]
fn explain_reports_navigation_delegation_to_planner_left_join() {
    let db = setup("explain-delegation");
    let plan = explain(
        &db,
        "SELECT DISTINCT e.department_id.label FROM employees e",
    );
    assert!(plan.contains("Reference Navigation"), "{plan}");
    assert!(plan.contains("delegated_reference_edges=1"), "{plan}");
    assert!(
        plan.contains("Actual Strategy: planner_left_join"),
        "{plan}"
    );
    assert!(plan.contains("delegated_to_join_executor"), "{plan}");
}
