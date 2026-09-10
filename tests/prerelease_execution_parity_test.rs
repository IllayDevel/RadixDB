// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0

#![cfg(feature = "test-failpoints")]

use std::process::Command;

use radixdb::storage::instrumentation;
use radixdb::test_failpoints::{
    execution_path_counters, ExecutionPathControlGuard, ExecutionPathMode,
};
use radixdb::{DataType, Database, Value};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ExactCell {
    Null(u8),
    Integer(i64),
    Float(u64),
    Boolean(bool),
    Text(String),
    Timestamp(String),
    Extension(Vec<u8>),
}

fn exact_rows(db: &Database, sql: &str) -> Vec<Vec<ExactCell>> {
    let mut result = db.query(sql, ()).unwrap_or_else(|error| {
        panic!("query failed: {sql}\n{error}");
    });
    let width = result.columns().len();
    let mut rows = Vec::new();
    for row in &mut result {
        let row = row.expect("query row");
        let mut cells = Vec::with_capacity(width);
        for index in 0..width {
            cells.push(match row.get_value(index) {
                Some(Value::Null(data_type)) => ExactCell::Null(*data_type as u8),
                None => ExactCell::Null(DataType::Null as u8),
                Some(Value::Integer(value)) => ExactCell::Integer(*value),
                Some(Value::Float(value)) => ExactCell::Float(value.to_bits()),
                Some(Value::Boolean(value)) => ExactCell::Boolean(*value),
                Some(Value::Text(value)) => ExactCell::Text(value.to_string()),
                Some(Value::Timestamp(value)) => ExactCell::Timestamp(value.to_rfc3339()),
                Some(Value::Extension(value)) => ExactCell::Extension(value.as_ref().to_vec()),
            });
        }
        rows.push(cells);
    }
    rows
}

fn sorted_exact_rows(db: &Database, sql: &str) -> Vec<Vec<ExactCell>> {
    let mut rows = exact_rows(db, sql);
    rows.sort();
    rows
}

#[test]
fn hot_cold_mixed_index_and_full_scan_paths_are_exactly_equivalent() {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}/b7_path_parity?checkpoint_on_close=off",
        directory.path().display()
    );
    let db = Database::open(&dsn).expect("open file database");
    db.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            category INTEGER NOT NULL,
            score INTEGER NOT NULL,
            label TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_items_category ON items(category)", ())
        .unwrap();
    db.execute("BEGIN", ()).unwrap();
    for id in 1..=512 {
        db.execute(
            &format!(
                "INSERT INTO items VALUES ({id}, {}, {}, 'item-{id}')",
                id % 7,
                (id * 17) % 101
            ),
            (),
        )
        .unwrap();
    }
    db.execute("COMMIT", ()).unwrap();

    let query = "SELECT id, score, label FROM items WHERE category = 3 ORDER BY id";
    let hot_indexed = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::Automatic);
        let rows = exact_rows(&db, query);
        let counters = execution_path_counters();
        assert!(
            counters.hot_index_scans + counters.hot_primary_key_scans > 0,
            "index path was not observed: {counters:?}"
        );
        rows
    };
    let hot_scanned = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::DeclineIndexes);
        let rows = exact_rows(&db, query);
        let counters = execution_path_counters();
        assert!(counters.hot_full_scans > 0, "{counters:?}");
        rows
    };
    assert_eq!(hot_indexed, hot_scanned, "hot index/full-scan parity");

    db.execute(
        "CREATE TABLE exact_sums (
            id INTEGER PRIMARY KEY,
            bucket INTEGER NOT NULL,
            amount INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO exact_sums VALUES
            (1, 1, 9007199254740993),
            (2, 1, 2),
            (3, 2, -9007199254740993)",
        (),
    )
    .unwrap();
    let grouped_sum_sql =
        "SELECT bucket, SUM(amount) FROM exact_sums GROUP BY bucket ORDER BY bucket";
    let hot_sums = exact_rows(&db, grouped_sum_sql);
    assert_eq!(
        hot_sums,
        vec![
            vec![
                ExactCell::Integer(1),
                ExactCell::Integer(9_007_199_254_740_995)
            ],
            vec![
                ExactCell::Integer(2),
                ExactCell::Integer(-9_007_199_254_740_993)
            ],
        ]
    );

    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    let cold = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::Automatic);
        let rows = exact_rows(&db, query);
        let counters = execution_path_counters();
        assert!(counters.cold_table_scans > 0, "{counters:?}");
        rows
    };
    assert_eq!(hot_indexed, cold, "hot/cold parity");
    assert_eq!(
        hot_sums,
        exact_rows(&db, grouped_sum_sql),
        "hot/cold SUM parity"
    );

    db.execute("INSERT INTO items VALUES (1003, 3, 77, 'hot-tail')", ())
        .unwrap();
    db.execute("INSERT INTO exact_sums VALUES (1004, 1, 5)", ())
        .unwrap();
    let mixed = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::Automatic);
        let rows = exact_rows(&db, query);
        let counters = execution_path_counters();
        assert!(counters.mixed_table_scans > 0, "{counters:?}");
        rows
    };
    assert_eq!(mixed.len(), hot_indexed.len() + 1);
    assert!(mixed.starts_with(&hot_indexed));
    assert_eq!(
        exact_rows(&db, grouped_sum_sql),
        vec![
            vec![
                ExactCell::Integer(1),
                ExactCell::Integer(9_007_199_254_741_000)
            ],
            vec![
                ExactCell::Integer(2),
                ExactCell::Integer(-9_007_199_254_740_993)
            ],
        ],
        "mixed cold/hot SUM parity"
    );
}

#[test]
fn forced_serial_parallel_join_and_filter_paths_are_exactly_equivalent() {
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE left_items (id INTEGER PRIMARY KEY, key_id INTEGER, value INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE right_items (id INTEGER PRIMARY KEY, key_id INTEGER, label TEXT)",
        (),
    )
    .unwrap();
    db.execute("BEGIN", ()).unwrap();
    for id in 0..512 {
        db.execute(
            &format!(
                "INSERT INTO left_items VALUES ({id}, {}, {})",
                id % 31,
                id - 256
            ),
            (),
        )
        .unwrap();
        if id < 93 {
            db.execute(
                &format!(
                    "INSERT INTO right_items VALUES ({id}, {}, 'r-{id}')",
                    id % 31
                ),
                (),
            )
            .unwrap();
        }
    }
    db.execute("COMMIT", ()).unwrap();

    let filter_sql = "SELECT id, value FROM left_items WHERE CASE WHEN value >= -256 THEN TRUE ELSE FALSE END ORDER BY id";
    let serial_filter = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::ForceSerial);
        let rows = exact_rows(&db, filter_sql);
        let counters = execution_path_counters();
        assert!(counters.serial_filters > 0, "{counters:?}");
        rows
    };
    let parallel_filter = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::ForceParallel);
        let rows = exact_rows(&db, filter_sql);
        let counters = execution_path_counters();
        assert!(counters.parallel_filters > 0, "{counters:?}");
        rows
    };
    assert_eq!(
        serial_filter, parallel_filter,
        "serial/parallel filter parity"
    );

    let join_sql = "SELECT * FROM left_items l INNER JOIN right_items r ON l.key_id = r.key_id";
    let serial_join = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::ForceSerial);
        let rows = sorted_exact_rows(&db, join_sql);
        let counters = execution_path_counters();
        assert!(counters.serial_joins > 0, "{counters:?}");
        rows
    };
    let parallel_join = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::ForceParallel);
        let rows = sorted_exact_rows(&db, join_sql);
        let counters = execution_path_counters();
        assert!(counters.parallel_joins > 0, "{counters:?}");
        rows
    };
    assert_eq!(serial_join, parallel_join, "serial/parallel join parity");
}

#[test]
fn navigation_and_explicit_left_join_share_results_and_record_execution() {
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE dictionaries (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            dictionary_id INTEGER REFERENCES dictionaries(id)
        )",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO dictionaries VALUES (1, 'one'), (2, 'two')", ())
        .unwrap();
    db.execute(
        "INSERT INTO documents VALUES (10, 1), (11, 1), (12, 2), (13, NULL)",
        (),
    )
    .unwrap();

    let before = instrumentation::snapshot();
    let navigation = exact_rows(
        &db,
        "SELECT d.id, d.dictionary_id.label FROM documents d ORDER BY d.id",
    );
    let after = instrumentation::snapshot();
    assert!(
        after.navigation_paths_executed > before.navigation_paths_executed,
        "navigation path was not physically observed"
    );
    let explicit = exact_rows(
        &db,
        "SELECT d.id, CAST(x.label AS TEXT) FROM documents d LEFT JOIN dictionaries x ON d.dictionary_id = x.id ORDER BY d.id",
    );
    assert_eq!(navigation, explicit, "navigation/classic JOIN parity");
}

#[test]
fn execution_thread_child() {
    if std::env::var_os("RADIXDB_B7_THREAD_CHILD").is_none() {
        return;
    }
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE thread_items (id INTEGER PRIMARY KEY, category INTEGER, value INTEGER)",
        (),
    )
    .unwrap();
    db.execute("BEGIN", ()).unwrap();
    for id in 0..4096 {
        db.execute(
            &format!(
                "INSERT INTO thread_items VALUES ({id}, {}, {})",
                id % 7,
                id % 101
            ),
            (),
        )
        .unwrap();
    }
    db.execute("COMMIT", ()).unwrap();

    let rows = {
        let _control = ExecutionPathControlGuard::install(ExecutionPathMode::ForceParallel);
        let rows = exact_rows(
            &db,
            "SELECT category, COUNT(*), SUM(value) FROM thread_items WHERE CASE WHEN value >= 0 THEN TRUE ELSE FALSE END GROUP BY category ORDER BY category",
        );
        let counters = execution_path_counters();
        assert!(counters.parallel_filters > 0, "{counters:?}");
        rows
    };
    println!("B7_THREAD_DIGEST={rows:?}");
}

#[test]
fn executor_thread_counts_replay_the_same_state_model() {
    let executable = std::env::current_exe().expect("current test executable");
    let mut expected_digest = None;
    for threads in [1, 2, 4, 8, 16] {
        let output = Command::new(&executable)
            .args([
                "--exact",
                "execution_thread_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RADIXDB_B7_THREAD_CHILD", "1")
            .env("RAYON_NUM_THREADS", threads.to_string())
            .output()
            .unwrap_or_else(|error| panic!("spawn {threads}-thread replay: {error}"));
        assert!(
            output.status.success(),
            "{threads}-thread replay failed:\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 child output");
        let digest = stdout
            .lines()
            .find_map(|line| {
                line.find("B7_THREAD_DIGEST=")
                    .map(|index| &line[index + "B7_THREAD_DIGEST=".len()..])
            })
            .unwrap_or_else(|| panic!("missing {threads}-thread digest:\n{stdout}"))
            .to_string();
        if let Some(expected) = &expected_digest {
            assert_eq!(
                &digest, expected,
                "thread-count semantic drift at {threads}"
            );
        } else {
            expected_digest = Some(digest);
        }
    }
}
