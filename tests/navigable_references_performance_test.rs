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

//! NR-15 executable no-N+1 and manual release performance contract.

use radixdb::Database;
use std::{hint::black_box, time::Instant};

fn execute_values(db: &Database, table: &str, values: Vec<String>) {
    for chunk in values.chunks(256) {
        db.execute(
            &format!("INSERT INTO {table} VALUES {}", chunk.join(",")),
            (),
        )
        .unwrap_or_else(|error| panic!("insert into {table} failed: {error}"));
    }
}

fn setup_fact_dictionary(name: &str, payments: usize) -> Database {
    let db = Database::open(&format!("memory://navigation_nr15_{name}"))
        .expect("open performance fixture");
    db.execute(
        "CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            cost_center TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department_id INTEGER NOT NULL REFERENCES departments(id)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE payments (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER NOT NULL REFERENCES employees(id),
            amount INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();

    execute_values(
        &db,
        "departments",
        (1..=64)
            .map(|id| format!("({id}, 'department-{id}', 'cc-{id}')"))
            .collect(),
    );
    execute_values(
        &db,
        "employees",
        (1..=256)
            .map(|id| format!("({id}, 'employee-{id}', {})", ((id - 1) % 64) + 1))
            .collect(),
    );
    execute_values(
        &db,
        "payments",
        (1..=payments)
            .map(|id| format!("({id}, {}, {})", ((id - 1) % 256) + 1, (id % 10_000) + 1))
            .collect(),
    );
    db
}

fn explain(db: &Database, sql: &str) -> String {
    db.query(&format!("EXPLAIN ANALYZE {sql}"), ())
        .unwrap_or_else(|error| panic!("EXPLAIN ANALYZE failed: {error}"))
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn grouped_totals(db: &Database, sql: &str) -> Vec<(String, i64)> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("grouped query failed: {error}"))
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect()
}

const NAVIGATION_GROUPED: &str = "SELECT p.employee_id.department_id.label, SUM(p.amount)
     FROM payments p
     GROUP BY p.employee_id.department_id.label
     ORDER BY p.employee_id.department_id.label";

const EXPLICIT_GROUPED: &str = "SELECT d.label, SUM(p.amount)
     FROM payments p
     LEFT JOIN employees e ON p.employee_id = e.id
     LEFT JOIN departments d ON e.department_id = d.id
     GROUP BY d.label
     ORDER BY d.label";

#[test]
fn navigation_batches_repeated_unique_and_transitive_keys_without_n_plus_one() {
    let db = setup_fact_dictionary("bounded_batches", 4096);

    let control = explain(&db, "SELECT SUM(amount) FROM payments");
    assert!(!control.contains("Reference Navigation"), "{control}");
    assert!(!control.contains("navigation."), "{control}");

    let direct = explain(
        &db,
        "SELECT p.employee_id.name FROM payments p WHERE p.id = 1",
    );
    assert!(
        direct.contains("Strategy: direct_unique_lookup"),
        "{direct}"
    );
    assert!(direct.contains("lookup_batches=1"), "{direct}");

    let repeated = explain(
        &db,
        "SELECT p.employee_id.name FROM payments p ORDER BY p.id",
    );
    assert!(repeated.contains("source_rows=4096"), "{repeated}");
    assert!(repeated.contains("distinct_source_keys=256"), "{repeated}");
    assert!(
        repeated.contains("repeated_keys_eliminated=3840"),
        "{repeated}"
    );
    assert!(repeated.contains("lookup_batches=1"), "{repeated}");
    assert!(
        repeated.contains("Strategy: target_hash_scan"),
        "{repeated}"
    );

    let transitive = explain(&db, NAVIGATION_GROUPED);
    assert!(transitive.contains("reference_edges=2"), "{transitive}");
    assert!(
        transitive.contains("Actual Work: delegated_to_join_executor"),
        "{transitive}"
    );
    assert_eq!(
        grouped_totals(&db, NAVIGATION_GROUPED),
        grouped_totals(&db, EXPLICIT_GROUPED)
    );

    db.execute(
        "CREATE TABLE wide_targets (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE unique_roots (
            id INTEGER PRIMARY KEY,
            target_id INTEGER NOT NULL REFERENCES wide_targets(id)
        )",
        (),
    )
    .unwrap();
    execute_values(
        &db,
        "wide_targets",
        (1..=1024)
            .map(|id| format!("({id}, 'target-{id}')"))
            .collect(),
    );
    execute_values(
        &db,
        "unique_roots",
        (1..=64).map(|id| format!("({id}, {id})")).collect(),
    );

    let unique = explain(
        &db,
        "SELECT r.target_id.label FROM unique_roots r ORDER BY r.id",
    );
    assert!(unique.contains("source_rows=64"), "{unique}");
    assert!(unique.contains("distinct_source_keys=64"), "{unique}");
    assert!(unique.contains("repeated_keys_eliminated=0"), "{unique}");
    assert!(unique.contains("lookup_batches=1"), "{unique}");
    assert!(unique.contains("Strategy: unique_batch_lookup"), "{unique}");
}

fn elapsed_ms(db: &Database, sql: &str, repeats: usize) -> f64 {
    let started = Instant::now();
    for _ in 0..repeats {
        black_box(grouped_totals(db, sql));
    }
    started.elapsed().as_secs_f64() * 1000.0 / repeats as f64
}

#[test]
#[ignore = "NR-15 manual release microbenchmark; run with --release --ignored --nocapture"]
fn release_navigation_matches_explicit_fact_dictionary_plan() {
    let db = setup_fact_dictionary("release_micro", 100_000);
    assert_eq!(
        grouped_totals(&db, NAVIGATION_GROUPED),
        grouped_totals(&db, EXPLICIT_GROUPED)
    );

    let navigation_ms = elapsed_ms(&db, NAVIGATION_GROUPED, 7);
    let explicit_ms = elapsed_ms(&db, EXPLICIT_GROUPED, 7);
    println!(
        "NR15 navigation={navigation_ms:.3} ms explicit={explicit_ms:.3} ms ratio={:.3}",
        navigation_ms / explicit_ms
    );
}
