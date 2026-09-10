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

//! NR-09 executable contract for explicit transitive reference paths.

use radixdb::Database;

type GraphRow = (
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn setup_graph(db: &Database) {
    db.execute(
        "CREATE TABLE countries (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            iso TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country_id INTEGER REFERENCES countries(id)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            region_id INTEGER REFERENCES regions(id)
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
        "INSERT INTO countries VALUES
            (1, 'Netherlands', 'NL'),
            (2, 'Germany', 'DE')",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO regions VALUES
            (10, 'West', 1),
            (20, 'Central', 2),
            (30, 'Unassigned', NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO departments VALUES
            (100, 'Sales', 10),
            (200, 'Engineering', 20),
            (300, 'Holding', 30),
            (400, 'No region', NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1000, 'Alice', 100),
            (1001, 'Bob', 200),
            (1002, 'Carol', 300),
            (1003, 'Dave', 400),
            (1004, 'Eve', NULL)",
        (),
    )
    .unwrap();
}

fn collect_graph_rows(db: &Database, sql: &str) -> Vec<GraphRow> {
    db.query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
                row.get(4).unwrap(),
            )
        })
        .collect()
}

fn navigation_graph_rows(db: &Database) -> Vec<GraphRow> {
    collect_graph_rows(
        db,
        "SELECT e.id,
                e.department_id.label,
                e.department_id.region_id.name,
                e.department_id.region_id.country_id.name,
                e.department_id.region_id.country_id.iso
         FROM employees e
         ORDER BY e.id",
    )
}

fn explicit_graph_rows(db: &Database) -> Vec<GraphRow> {
    collect_graph_rows(
        db,
        "SELECT e.id, d.label, r.name, c.name, c.iso
         FROM employees e
         LEFT JOIN departments d ON e.department_id = d.id
         LEFT JOIN regions r ON d.region_id = r.id
         LEFT JOIN countries c ON r.country_id = c.id
         ORDER BY e.id",
    )
}

#[test]
fn transitive_depth_two_three_shared_prefix_and_nulls_match_left_joins() {
    let db = Database::open("memory://navigation_nr09_graph").unwrap();
    setup_graph(&db);

    let navigation = navigation_graph_rows(&db);
    assert_eq!(navigation, explicit_graph_rows(&db));
    assert_eq!(
        navigation,
        vec![
            (
                1000,
                Some("Sales".to_string()),
                Some("West".to_string()),
                Some("Netherlands".to_string()),
                Some("NL".to_string()),
            ),
            (
                1001,
                Some("Engineering".to_string()),
                Some("Central".to_string()),
                Some("Germany".to_string()),
                Some("DE".to_string()),
            ),
            (
                1002,
                Some("Holding".to_string()),
                Some("Unassigned".to_string()),
                None,
                None,
            ),
            (1003, Some("No region".to_string()), None, None, None,),
            (1004, None, None, None, None),
        ]
    );

    let navigation_ids: Vec<i64> = db
        .query(
            "SELECT e.id
             FROM employees e
             WHERE e.department_id.region_id.country_id.iso = 'NL'
                OR e.department_id.region_id.country_id.iso IS NULL
             ORDER BY e.id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    let explicit_ids: Vec<i64> = db
        .query(
            "SELECT e.id
             FROM employees e
             LEFT JOIN departments d ON e.department_id = d.id
             LEFT JOIN regions r ON d.region_id = r.id
             LEFT JOIN countries c ON r.country_id = c.id
             WHERE c.iso = 'NL' OR c.iso IS NULL
             ORDER BY e.id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    assert_eq!(navigation_ids, explicit_ids);

    let explain = db
        .query(
            "EXPLAIN ANALYZE
             SELECT e.department_id.region_id.country_id.name,
                    e.department_id.region_id.country_id.iso
             FROM employees e",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>();
    let plan = explain.join("\n");
    assert_eq!(
        explain
            .iter()
            .filter(|line| line.trim_start().starts_with("Edge "))
            .count(),
        3,
        "shared prefixes must compile to one edge each:\n{plan}"
    );
    assert!(plan.contains("lookup_batches=3"), "{plan}");
}

#[test]
fn explicit_depth_eight_and_finite_self_cycle_match_left_join_chain() {
    let db = Database::open("memory://navigation_nr09_depth_eight").unwrap();
    db.execute(
        "CREATE TABLE nodes (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            next_id INTEGER REFERENCES nodes(id)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO nodes VALUES (9, 'terminal', NULL)", ())
        .unwrap();
    for id in (1..=8).rev() {
        db.execute(
            &format!("INSERT INTO nodes VALUES ({id}, 'node-{id}', {})", id + 1),
            (),
        )
        .unwrap();
    }
    db.execute("INSERT INTO nodes VALUES (10, 'null-root', NULL)", ())
        .unwrap();

    let navigated: Vec<(i64, Option<String>)> = db
        .query(
            "SELECT n.id,
                    n.next_id.next_id.next_id.next_id.next_id.next_id.next_id.next_id.label
             FROM nodes n
             WHERE n.id IN (1, 10)
             ORDER BY n.id",
            (),
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();
    let explicit: Vec<(i64, Option<String>)> = db
        .query(
            "SELECT n.id, j8.label
             FROM nodes n
             LEFT JOIN nodes j1 ON n.next_id = j1.id
             LEFT JOIN nodes j2 ON j1.next_id = j2.id
             LEFT JOIN nodes j3 ON j2.next_id = j3.id
             LEFT JOIN nodes j4 ON j3.next_id = j4.id
             LEFT JOIN nodes j5 ON j4.next_id = j5.id
             LEFT JOIN nodes j6 ON j5.next_id = j6.id
             LEFT JOIN nodes j7 ON j6.next_id = j7.id
             LEFT JOIN nodes j8 ON j7.next_id = j8.id
             WHERE n.id IN (1, 10)
             ORDER BY n.id",
            (),
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        })
        .collect();
    assert_eq!(navigated, explicit);
    assert_eq!(
        navigated,
        vec![(1, Some("terminal".to_string())), (10, None)]
    );

    let error = match db.query(
        "SELECT n.next_id.next_id.next_id.next_id.next_id.next_id.next_id.next_id.next_id.label
             FROM nodes n",
        (),
    ) {
        Err(error) => error,
        Ok(_) => panic!("nine explicit steps unexpectedly passed the documented limit"),
    };
    let message = error.to_string();
    assert!(message.contains("NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE"));
    assert!(message.contains("maximum is 8"), "{message}");
}

#[test]
fn transitive_paths_survive_checkpoint_reopen_and_wal_overlay() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/navigation_nr09_reopen?sync_mode=normal&checkpoint_interval=3600&checkpoint_on_close=off",
        directory.path().display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        setup_graph(&db);
        assert_eq!(navigation_graph_rows(&db), explicit_graph_rows(&db));
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(navigation_graph_rows(&db), explicit_graph_rows(&db));
        db.execute("INSERT INTO employees VALUES (1005, 'Frank', 100)", ())
            .unwrap();
        assert_eq!(navigation_graph_rows(&db), explicit_graph_rows(&db));
        db.close().unwrap();
    }

    let reopened = Database::open(&dsn).unwrap();
    assert_eq!(
        navigation_graph_rows(&reopened),
        explicit_graph_rows(&reopened)
    );
    assert_eq!(navigation_graph_rows(&reopened).len(), 6);
    reopened.close().unwrap();
}
