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

//! Executable contract for navigable references.
//!
//! Read-only navigation and its permanent DML rejection boundary are both
//! executable contracts. Only the manual release benchmark remains ignored.

use radixdb::Database;
use std::hint::black_box;
use std::time::{Duration, Instant};

const NAVIGATION_UNKNOWN_ROOT: &str = "NAVIGATION_UNKNOWN_ROOT";
const NAVIGATION_AMBIGUOUS_ROOT: &str = "NAVIGATION_AMBIGUOUS_ROOT";
const NAVIGATION_NOT_A_REFERENCE: &str = "NAVIGATION_NOT_A_REFERENCE";
const NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE: &str = "NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE";
const NAVIGATION_TARGET_COLUMN_NOT_FOUND: &str = "NAVIGATION_TARGET_COLUMN_NOT_FOUND";
const NAVIGATION_READ_ONLY: &str = "NAVIGATION_READ_ONLY";
const NAVIGATION_SCHEMA_CHANGED: &str = "NAVIGATION_SCHEMA_CHANGED";
const REFERENCE_TARGET_MISSING: &str = "REFERENCE_TARGET_MISSING";
const REFERENCE_TARGET_NOT_UNIQUE: &str = "REFERENCE_TARGET_NOT_UNIQUE";

type ProjectionRow = (i64, String, Option<String>, Option<String>);

fn open(name: &str) -> Database {
    Database::open(&format!("memory://navigable_references_{name}"))
        .expect("open in-memory contract database")
}

fn setup_reference_graph(name: &str) -> Database {
    let db = open(name);
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
            cost_center TEXT NOT NULL,
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

    db.execute(
        "INSERT INTO profiles VALUES
            (1, 'Finance profile'),
            (2, 'Engineering profile')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO departments VALUES
            (10, 'Finance', 'FIN', 1),
            (20, 'Engineering', 'ENG', 2),
            (30, 'Unclassified', 'UNC', NULL)",
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

fn collect_projection(db: &Database, sql: &str) -> Vec<ProjectionRow> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
            )
        })
        .collect()
}

fn query_error(db: &Database, sql: &str) -> String {
    match db.query(sql, ()) {
        Ok(mut rows) => match rows.next() {
            Some(Err(error)) => error.to_string(),
            _ => panic!("query unexpectedly succeeded: {sql}"),
        },
        Err(error) => error.to_string(),
    }
}

fn execute_error(db: &Database, sql: &str) -> String {
    match db.execute(sql, ()) {
        Ok(_) => panic!("statement unexpectedly succeeded: {sql}"),
        Err(error) => error.to_string(),
    }
}

fn assert_query_error_code(db: &Database, sql: &str, expected: &str) {
    let error = query_error(db, sql);
    assert!(
        error.contains(expected),
        "expected {expected} for `{sql}`, got: {error}"
    );
}

fn assert_execute_error_code(db: &Database, sql: &str, expected: &str) {
    let error = execute_error(db, sql);
    assert!(
        error.contains(expected),
        "expected {expected} for `{sql}`, got: {error}"
    );
}

#[test]
fn explicit_left_join_is_the_semantic_baseline() {
    let db = setup_reference_graph("explicit_semantic_baseline");
    let rows = collect_projection(
        &db,
        "SELECT e.id, e.name, d.label, p.display_name
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         LEFT JOIN profiles p ON d.profile_id = p.id
         ORDER BY e.id",
    );

    assert_eq!(
        rows,
        vec![
            (
                100,
                "Alice".to_string(),
                Some("Finance".to_string()),
                Some("Finance profile".to_string()),
            ),
            (
                101,
                "Bob".to_string(),
                Some("Finance".to_string()),
                Some("Finance profile".to_string()),
            ),
            (
                102,
                "Carol".to_string(),
                Some("Engineering".to_string()),
                Some("Engineering profile".to_string()),
            ),
            (103, "Dave".to_string(), None, None),
            (
                104,
                "Eve".to_string(),
                Some("Unclassified".to_string()),
                None,
            ),
        ]
    );
}

#[test]
fn single_step_projection_executes_after_binding() {
    let db = setup_reference_graph("nr06_single_step_projection");
    let explicit: Vec<Option<String>> = db
        .query(
            "SELECT d.label
             FROM employees e
             LEFT JOIN departments d ON e.department_id = d.id
             ORDER BY e.id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    let navigated: Vec<Option<String>> = db
        .query(
            "SELECT e.department_id.label FROM employees e ORDER BY e.id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    assert_eq!(navigated, explicit);

    let shorthand: Option<String> = db
        .query(
            "SELECT department_id.label FROM employees WHERE id = 100",
            (),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(shorthand.as_deref(), Some("Finance"));
}

#[test]
fn explain_reports_one_logical_edge_for_shared_reference_prefix() {
    let db = setup_reference_graph("nr05_reference_expand_explain");
    let lines: Vec<String> = db
        .query(
            "EXPLAIN SELECT e.department_id.label,
                            e.department_id.cost_center,
                            department_id.profile_id
             FROM employees e",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    let plan = lines.join("\n");

    assert!(plan.contains("Reference Navigation"), "{plan}");
    assert!(plan.contains("Semantics: LEFT"), "{plan}");
    assert!(plan.contains("Snapshot: statement"), "{plan}");
    assert!(
        plan.contains("Authorization: same_as_explicit_left_join"),
        "{plan}"
    );
    assert!(
        plan.contains("Counters: available_with_explain_analyze"),
        "{plan}"
    );
    assert!(
        plan.contains("Physical Strategy: adaptive_unique_lookup"),
        "{plan}"
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.trim_start().starts_with("Edge "))
            .count(),
        1,
        "{plan}"
    );
    assert!(
        plan.contains("Required Columns: label, cost_center, profile_id"),
        "{plan}"
    );
    assert!(plan.contains("Path 1:"), "{plan}");
    assert!(plan.contains("Steps: 1"), "{plan}");

    let analyzed: Vec<String> = db
        .query(
            "EXPLAIN ANALYZE
             SELECT department_id.label FROM employees ORDER BY id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    let analyzed = analyzed.join("\n");
    assert!(analyzed.contains("actual time="), "{analyzed}");
    assert!(analyzed.contains("rows=5"), "{analyzed}");
    assert!(analyzed.contains("Reference Navigation"), "{analyzed}");
    assert!(analyzed.contains("paths_planned=1"), "{analyzed}");
    assert!(analyzed.contains("paths_executed=1"), "{analyzed}");
    assert!(analyzed.contains("repeated_keys_eliminated="), "{analyzed}");
    assert!(analyzed.contains("target_lookup_hits="), "{analyzed}");
    assert!(analyzed.contains("target_lookup_misses=0"), "{analyzed}");
    assert!(
        analyzed.contains("Actual Strategy: index_nested_loop"),
        "{analyzed}"
    );
    assert!(analyzed.contains("Storage Mode: hot_mvcc"), "{analyzed}");
    assert!(
        analyzed.contains("Target Projection Columns: 2"),
        "{analyzed}"
    );

    let secret = "nr14-private-lookup-value";
    let parameterized: Vec<String> = db
        .query(
            "EXPLAIN ANALYZE
             SELECT department_id.label FROM employees
             WHERE department_id.label = $1",
            (secret,),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    let parameterized = parameterized.join("\n");
    assert!(!parameterized.contains(secret), "{parameterized}");
    assert!(!parameterized.contains("Finance"), "{parameterized}");
}

#[test]
fn future_navigation_matches_explicit_left_join() {
    let db = setup_reference_graph("future_parity");
    let explicit = collect_projection(
        &db,
        "SELECT e.id, e.name, d.label, p.display_name
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         LEFT JOIN profiles p ON d.profile_id = p.id
         ORDER BY e.id",
    );
    let navigated = collect_projection(
        &db,
        "SELECT e.id, e.name,
                e.department_id.label,
                e.department_id.profile_id.display_name
         FROM employees e
         ORDER BY e.id",
    );

    assert_eq!(navigated, explicit);
}

#[test]
fn navigation_supports_shorthand_and_shared_prefixes() {
    let db = setup_reference_graph("future_shorthand");
    let rows: Vec<(i64, Option<String>, Option<String>)> = db
        .query(
            "SELECT id, department_id.label, department_id.cost_center
             FROM employees
             ORDER BY id",
            (),
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

    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].1.as_deref(), Some("Finance"));
    assert_eq!(rows[0].2.as_deref(), Some("FIN"));
    assert_eq!(rows[3], (103, None, None));
}

#[test]
fn navigation_executes_through_unique_not_null_target() {
    let db = open("nr06_unique_not_null_target");
    db.execute(
        "CREATE TABLE lookup_values (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            lookup_code TEXT REFERENCES lookup_values(code)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO lookup_values VALUES
            (1, 'FIN', 'Finance'),
            (2, 'ENG', 'Engineering')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO documents VALUES (10, 'FIN'), (11, 'FIN'), (12, NULL)",
        (),
    )
    .unwrap();

    let rows: Vec<(i64, Option<String>)> = db
        .query(
            "SELECT id, lookup_code.label FROM documents ORDER BY id",
            (),
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (10, Some("Finance".to_string())),
            (11, Some("Finance".to_string())),
            (12, None),
        ]
    );
}

#[test]
fn future_navigation_reports_stable_binding_errors() {
    let db = setup_reference_graph("future_binding_errors");

    let cases = [
        (
            "SELECT ghost.department_id.label FROM employees e",
            NAVIGATION_UNKNOWN_ROOT,
        ),
        (
            "SELECT department_id.label
             FROM employees e1
             JOIN employees e2 ON e1.id = e2.id",
            NAVIGATION_AMBIGUOUS_ROOT,
        ),
        (
            "SELECT e.name.value FROM employees e",
            NAVIGATION_NOT_A_REFERENCE,
        ),
        (
            "SELECT e.department_id.missing FROM employees e",
            NAVIGATION_TARGET_COLUMN_NOT_FOUND,
        ),
    ];

    for (sql, code) in cases {
        assert_query_error_code(&db, sql, code);
    }
}

#[test]
fn navigation_reports_stable_descriptor_errors() {
    // Valid DDL prevents both execution states. The storage-independent NR-06
    // candidate verifier exercises them with synthetic rows; this public
    // contract keeps their wire-visible categories stable.
    assert_eq!(
        [
            NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE,
            REFERENCE_TARGET_NOT_UNIQUE,
            NAVIGATION_SCHEMA_CHANGED,
            REFERENCE_TARGET_MISSING,
        ],
        [
            "NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE",
            "REFERENCE_TARGET_NOT_UNIQUE",
            "NAVIGATION_SCHEMA_CHANGED",
            "REFERENCE_TARGET_MISSING",
        ]
    );
}

#[test]
fn current_ddl_rejects_non_unique_and_composite_fk_shapes() {
    let db = open("ddl_shape_prerequisites");
    db.execute(
        "CREATE TABLE non_unique_target (id INTEGER PRIMARY KEY, lookup INTEGER)",
        (),
    )
    .unwrap();

    let non_unique = execute_error(
        &db,
        "CREATE TABLE invalid_reference (
            id INTEGER PRIMARY KEY,
            target_lookup INTEGER REFERENCES non_unique_target(lookup)
        )",
    );
    assert!(
        non_unique.contains("neither PRIMARY KEY nor UNIQUE"),
        "unexpected non-unique target error: {non_unique}"
    );

    let composite = execute_error(
        &db,
        "CREATE TABLE invalid_composite (
            id INTEGER PRIMARY KEY,
            left_id INTEGER,
            right_id INTEGER,
            FOREIGN KEY(left_id, right_id)
                REFERENCES non_unique_target(id, lookup)
        )",
    );
    assert!(
        composite.contains("expected") || composite.contains("Parse"),
        "unexpected composite FK error: {composite}"
    );
}

macro_rules! dml_navigation_rejection_test {
    ($name:ident, $sql:expr) => {
        #[test]
        fn $name() {
            let db = setup_reference_graph(stringify!($name));
            assert_execute_error_code(&db, $sql, NAVIGATION_READ_ONLY);
        }
    };
}

dml_navigation_rejection_test!(
    navigation_is_rejected_in_insert_select,
    "INSERT INTO employees (id, name, department_id)
     SELECT id + 1000, department_id.label, department_id
     FROM employees"
);

dml_navigation_rejection_test!(
    navigation_is_rejected_in_update_assignment,
    "UPDATE employees
     SET name = department_id.label
     WHERE id = 100"
);

dml_navigation_rejection_test!(
    navigation_is_rejected_in_update_predicate,
    "UPDATE employees
     SET name = 'blocked'
     WHERE department_id.label = 'Finance'"
);

dml_navigation_rejection_test!(
    navigation_is_rejected_in_delete_predicate,
    "DELETE FROM employees
     WHERE department_id.label = 'Finance'"
);

dml_navigation_rejection_test!(
    navigation_is_rejected_in_upsert_assignment,
    "INSERT INTO employees (id, name, department_id)
     VALUES (100, 'blocked', 10)
     ON DUPLICATE KEY UPDATE name = department_id.label"
);

fn insert_benchmark_rows(db: &Database, row_count: usize) {
    const BATCH_SIZE: usize = 1_000;

    for start in (1..=row_count).step_by(BATCH_SIZE) {
        let end = (start + BATCH_SIZE - 1).min(row_count);
        let mut targets = String::from("INSERT INTO nr_target VALUES ");
        for id in start..=end {
            if id > start {
                targets.push(',');
            }
            targets.push_str(&format!("({id},'target-{id}')"));
        }
        db.execute(&targets, ()).unwrap();

        let mut sources = String::from("INSERT INTO nr_source VALUES ");
        for id in start..=end {
            if id > start {
                sources.push(',');
            }
            if id % 10 == 0 {
                sources.push_str(&format!("({id},NULL,'source-{id}')"));
            } else {
                // Repeated lookup keys make the future batched-navigation
                // comparison representative without changing LEFT semantics.
                let target_id = 1 + ((id - 1) / 2) * 2;
                sources.push_str(&format!("({id},{target_id},'source-{id}')"));
            }
        }
        db.execute(&sources, ()).unwrap();
    }
}

fn run_left_join_once(db: &Database) -> (Duration, usize, u64) {
    let started = Instant::now();
    let mut count = 0usize;
    let mut checksum = 0u64;
    let rows = db
        .query(
            "SELECT s.id, s.payload, t.label
             FROM nr_source s
             LEFT JOIN nr_target t ON s.target_id = t.id",
            (),
        )
        .unwrap();
    for row in rows {
        let row = row.unwrap();
        let id: i64 = row.get(0).unwrap();
        let label: Option<String> = row.get(2).unwrap();
        count += 1;
        checksum = checksum
            .wrapping_add(id as u64)
            .wrapping_add(label.as_ref().map_or(0, |value| value.len() as u64));
    }
    black_box(checksum);
    (started.elapsed(), count, checksum)
}

fn median_duration(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[test]
#[ignore = "NR-01 manual release baseline; run explicitly with --ignored --nocapture"]
fn explicit_pk_left_join_performance_baseline() {
    const CARDINALITIES: [usize; 3] = [1_000, 10_000, 100_000];
    const WARMUPS: usize = 2;
    const MEASURED: usize = 7;

    for row_count in CARDINALITIES {
        let db = open(&format!("explicit_left_join_baseline_{row_count}"));
        db.execute(
            "CREATE TABLE nr_target (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE nr_source (
                id INTEGER PRIMARY KEY,
                target_id INTEGER REFERENCES nr_target(id),
                payload TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
        insert_benchmark_rows(&db, row_count);

        for _ in 0..WARMUPS {
            let (_, count, _) = run_left_join_once(&db);
            assert_eq!(count, row_count);
        }

        let mut samples = Vec::with_capacity(MEASURED);
        let mut checksum = 0u64;
        for _ in 0..MEASURED {
            let (elapsed, count, measured_checksum) = run_left_join_once(&db);
            assert_eq!(count, row_count);
            samples.push(elapsed);
            checksum = measured_checksum;
        }
        let median = median_duration(samples.clone());
        let min = samples.iter().copied().min().unwrap();
        let max = samples.iter().copied().max().unwrap();
        println!(
            "NR01_LEFT_JOIN_BASELINE rows={row_count} warmups={WARMUPS} measured={MEASURED} \
             median_ms={:.3} min_ms={:.3} max_ms={:.3} checksum={checksum}",
            median.as_secs_f64() * 1_000.0,
            min.as_secs_f64() * 1_000.0,
            max.as_secs_f64() * 1_000.0,
        );
    }
}
