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

//! Tests for EXPLAIN and EXPLAIN ANALYZE functionality

use radixdb::Database;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn setup_test_db() -> Database {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let db = Database::open(&format!("memory://explain_test_{}", id))
        .expect("Failed to create database");

    // Create test table
    db.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            status TEXT,
            amount DECIMAL
        )",
        (),
    )
    .expect("Failed to create table");

    // Create indexes
    db.execute("CREATE INDEX idx_user_id ON orders(user_id)", ())
        .expect("Failed to create user_id index");
    db.execute("CREATE INDEX idx_status ON orders(status)", ())
        .expect("Failed to create status index");

    // Insert test data
    for i in 1..=10 {
        let user_id = (i % 3) + 1; // user_id 1, 2, or 3
        let status = match i % 3 {
            0 => "active",
            1 => "shipped",
            _ => "pending",
        };
        let amount = i as f64 * 10.0;
        db.execute(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, '{}', {})",
                i, user_id, status, amount
            ),
            (),
        )
        .expect("Failed to insert row");
    }

    db
}

fn get_plan_output(db: &Database, query: &str) -> Vec<String> {
    let result = db.query(query, ()).expect("Failed to execute EXPLAIN");
    let mut lines = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let plan_line: String = row.get(0).unwrap_or_default();
        lines.push(plan_line);
    }
    lines
}

fn assert_plan_contains(db: &Database, query: &str, expected: &str) {
    let lines = get_plan_output(db, query);
    let plan = lines.join("\n");
    assert!(
        plan.contains(expected),
        "Expected plan to contain '{}', got:\n{}",
        expected,
        plan
    );
}

#[test]
fn test_explain_seq_scan() {
    let db = setup_test_db();

    // Query on non-indexed column should show Seq Scan
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders WHERE amount > 50");

    let plan = lines.join("\n");
    assert!(
        plan.contains("Seq Scan on orders"),
        "Expected Seq Scan, got:\n{}",
        plan
    );
    assert!(plan.contains("Filter:"), "Expected Filter, got:\n{}", plan);
}

#[test]
fn test_explain_stable_access_path_full_scan() {
    let db = setup_test_db();

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE amount > 50",
        "Access Path: scan.seq",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE amount > 50",
        "Access Source: hot_mvcc",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE amount > 50",
        "RAM Accelerator: none",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE amount > 50",
        "Hot Access Path: version_store.seq_scan",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE amount > 50",
        "Projection Boundary: projection.full_row",
    );
}

#[test]
fn test_explain_index_scan() {
    let db = setup_test_db();

    // Query on indexed column should show Index Scan
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders WHERE user_id = 1");

    let plan = lines.join("\n");
    assert!(
        plan.contains("Index Scan"),
        "Expected Index Scan, got:\n{}",
        plan
    );
    assert!(
        plan.contains("idx_user_id"),
        "Expected idx_user_id index, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Index Cond:"),
        "Expected Index Cond, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_stable_access_path_range_index() {
    let db = setup_test_db();

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE user_id > 1",
        "Access Path: scan.index",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE user_id > 1",
        "Hot Access Path: version_store.secondary_index",
    );
}

#[test]
fn test_explain_pk_lookup() {
    let db = setup_test_db();

    // Query on primary key should show PK Lookup
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders WHERE id = 5");

    let plan = lines.join("\n");
    assert!(
        plan.contains("PK Lookup"),
        "Expected PK Lookup, got:\n{}",
        plan
    );
    assert!(plan.contains("id = 5"), "Expected id = 5, got:\n{}", plan);
}

#[test]
fn test_explain_stable_access_path_pk_lookup() {
    let db = setup_test_db();

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE id = 5",
        "Access Path: scan.pk",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE id = 5",
        "Hot Access Path: version_store.primary_key",
    );
}

#[test]
fn test_explain_stable_projection_boundaries() {
    let db = setup_test_db();

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT user_id, status FROM orders",
        "Projection Boundary: projection.table_scan",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT user_id + 1 FROM orders",
        "Projection Boundary: projection.expression_dependency_scan",
    );
}

#[test]
fn test_explain_stable_aggregation_path_and_fallback() {
    let db = setup_test_db();

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT status, SUM(amount) FROM orders GROUP BY status HAVING SUM(amount) > 10",
        "Aggregation Path: aggregation.storage_group_by",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT status, SUM(amount) FROM orders GROUP BY ROLLUP(status)",
        "Aggregation Fallback: grouping-modifier",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT user_id + 1, SUM(amount) FROM orders GROUP BY user_id + 1",
        "Aggregation Fallback: non-column-group-by",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT SUM(amount), status FROM orders GROUP BY status",
        "Aggregation Fallback: unsupported-select-aggregate-shape",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT status, SUM(amount) FROM orders GROUP BY status HAVING SUM(amount) > (SELECT MIN(amount) FROM orders)",
        "Aggregation Fallback: unsupported-having",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT status, SUM(amount) FROM orders WHERE amount IN (SELECT amount FROM orders) GROUP BY status",
        "Aggregation Fallback: where-not-fully-pushable",
    );
}

#[test]
fn test_explain_stable_join_access_path() {
    let db = setup_test_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .expect("create users");
    db.execute(
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')",
        (),
    )
    .expect("insert users");

    assert_plan_contains(
        &db,
        "EXPLAIN SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id",
        "Join Access Path: join.index_nested_loop.pk",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id",
        "Projection Boundary: projection.join_operator_candidate",
    );
    assert_plan_contains(
        &db,
        "EXPLAIN SELECT orders.id, users.name FROM orders JOIN users ON orders.user_id = users.id AND orders.amount > 50",
        "Join Projection Boundary: join.projection.fallback.residual_on",
    );
}

#[test]
fn aggregate_join_uses_complete_input_hash_path_instead_of_cold_row_probes() {
    let db = setup_test_db();
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, department_id INTEGER)",
        (),
    )
    .expect("create users");
    db.execute(
        "INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Cara', 2)",
        (),
    )
    .expect("insert users");
    db.execute(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .expect("create departments");
    db.execute(
        "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Operations')",
        (),
    )
    .expect("insert departments");

    let sql = "SELECT departments.name, SUM(orders.amount) \
               FROM orders JOIN users ON orders.user_id = users.id \
               JOIN departments ON users.department_id = departments.id \
               GROUP BY departments.name";
    let plan = get_plan_output(&db, &format!("EXPLAIN {sql}")).join("\n");
    assert!(
        plan.contains("Join Access Path: join.hash")
            && !plan.contains("Join Access Path: join.index_nested_loop"),
        "complete-input aggregation must not probe the inner table row by row:\n{plan}"
    );

    let rows = db.query(sql, ()).expect("execute aggregate join").count();
    assert_eq!(rows, 2);
}

#[test]
fn test_explain_stable_access_path_cold_artifact_after_checkpoint() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .expect("open file db");

    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)",
        (),
    )
    .expect("create events");
    for i in 1..=5 {
        db.execute(
            &format!("INSERT INTO events VALUES ({}, 'cold', {})", i, i * 10),
            (),
        )
        .expect("insert cold event");
    }
    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint");

    let plan = get_plan_output(&db, "EXPLAIN SELECT id, amount FROM events").join("\n");
    assert!(
        plan.contains("Segmented Scan on events"),
        "expected segmented scan, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Access Path: scan.cold_artifact"),
        "expected cold artifact-backed access path, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Access Source: cold(artifact_blocks)+hot"),
        "expected artifact-backed source marker, got:\n{}",
        plan
    );
    assert!(
        plan.contains("RAM Accelerator: none"),
        "expected explicit no-RAM-accelerator marker, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Scan Runtime: artifact_prefetch.nvme_cpu_saturation"),
        "expected artifact-backed prefetch runtime marker, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Scan Projection Path: scan.cold_artifact.projected_prefetch"),
        "expected projected cold artifact-backed scan path, got:\n{}",
        plan
    );
    assert!(
        !plan.contains("Cold Legacy Fallback: true"),
        "checkpoint-created artifact-backed segment must not explain as legacy fallback:\n{}",
        plan
    );

    let full_plan = get_plan_output(&db, "EXPLAIN SELECT * FROM events").join("\n");
    assert!(
        full_plan.contains("Scan Projection Path: scan.cold_artifact.full_prefetch"),
        "expected full cold artifact-backed scan path for SELECT *, got:\n{}",
        full_plan
    );
}

#[test]
fn test_explain_stable_access_path_mixed_cold_artifact_hot_after_checkpoint() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .expect("open file db");

    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)",
        (),
    )
    .expect("create events");
    db.execute("INSERT INTO events VALUES (1, 'cold', 10)", ())
        .expect("insert cold event");
    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint");
    db.execute("INSERT INTO events VALUES (2, 'hot', 20)", ())
        .expect("insert hot event");

    let plan = get_plan_output(&db, "EXPLAIN SELECT id FROM events WHERE amount > 0").join("\n");
    assert!(
        plan.contains("Access Path: scan.mixed_cold_artifact_hot"),
        "expected mixed cold/hot access path, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Hot Rows Hint: 1"),
        "expected hot row hint after post-checkpoint insert, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Hot Access Path: version_store.snapshot_scan"),
        "expected hot snapshot path marker for mixed segmented scan, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Scan Projection Path: scan.cold_artifact.projected_prefetch"),
        "expected projected cold artifact-backed scan path, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Access Filter: cold_scan_predicate+hot_snapshot_residual"),
        "expected segmented filter marker, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_stable_cold_artifact_metadata_pruning_summary() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .expect("open file db");

    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)",
        (),
    )
    .expect("create events");
    db.execute(
        "INSERT INTO events VALUES (1, 'old', 10), (2, 'old', 20)",
        (),
    )
    .expect("insert first cold segment");
    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint 1");
    db.execute(
        "INSERT INTO events VALUES (1001, 'new', 30), (1002, 'new', 40)",
        (),
    )
    .expect("insert second cold segment");
    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint 2");

    // Keep the predicate off the primary-key accelerator: this gate owns
    // artifact metadata pruning, while PK ranges correctly use ordered INDEX.
    let plan = get_plan_output(&db, "EXPLAIN SELECT id FROM events WHERE amount > 20").join("\n");
    assert!(
        plan.contains("Access Path: scan.cold_artifact"),
        "expected cold artifact-backed access path, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Cold Metadata Path: zone_map+bloom+row_group_metadata"),
        "expected metadata path marker, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Cold Segments: 2"),
        "expected two cold segments, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Cold Metadata Selected Segments: 1"),
        "expected one selected segment after metadata pruning, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Cold Metadata Pruned Segments: 1"),
        "expected one pruned segment after metadata pruning, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_same_query_across_hot_cold_and_mixed_states() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .expect("open file db");

    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)",
        (),
    )
    .expect("create events");
    db.execute("CREATE INDEX idx_events_amount ON events(amount)", ())
        .expect("create amount index");
    db.execute(
        "INSERT INTO events VALUES (1, 'hot', 10), (2, 'hot', 20)",
        (),
    )
    .expect("insert hot rows");

    let query = "EXPLAIN SELECT id FROM events WHERE amount = 10";

    let hot_plan = get_plan_output(&db, query).join("\n");
    assert!(
        hot_plan.contains("Access Path: scan.index"),
        "same query should start on hot secondary index, got:\n{}",
        hot_plan
    );
    assert!(
        hot_plan.contains("Hot Access Path: version_store.secondary_index"),
        "same query should expose hot index path, got:\n{}",
        hot_plan
    );

    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint");

    let cold_plan = get_plan_output(&db, query).join("\n");
    assert!(
        cold_plan.contains("Access Path: scan.index")
            && cold_plan.contains("Cold Access Path: volume.exact_index"),
        "same query after checkpoint should use persisted exact postings, got:\n{}",
        cold_plan
    );
    assert!(
        cold_plan.contains("Hot Access Path: version_store.secondary_index"),
        "same query after checkpoint should expose the matching hot index half, got:\n{}",
        cold_plan
    );
    assert!(
        cold_plan.contains("RAM Accelerator: none"),
        "same query after checkpoint should not imply hidden RAM accelerator, got:\n{}",
        cold_plan
    );

    // Warm/cache state must not rewrite the physical postings contract.
    let _rows: Vec<_> = db
        .query("SELECT id FROM events WHERE amount = 10", ())
        .expect("run query once")
        .map(|row| row.expect("row"))
        .collect();
    let warmed_cold_plan = get_plan_output(&db, query).join("\n");
    assert!(
        warmed_cold_plan.contains("Access Path: scan.index")
            && warmed_cold_plan.contains("Cold Access Path: volume.exact_index"),
        "same query after warm read should retain exact postings, got:\n{}",
        warmed_cold_plan
    );

    db.execute("INSERT INTO events VALUES (3, 'new-hot', 10)", ())
        .expect("insert post-checkpoint hot row");

    let mixed_plan = get_plan_output(&db, query).join("\n");
    assert!(
        mixed_plan.contains("Access Path: scan.index")
            && mixed_plan.contains("Cold Access Path: volume.exact_index"),
        "same query with cold+hot rows should retain the bounded mixed index path, got:\n{}",
        mixed_plan
    );
    assert!(
        mixed_plan.contains("Hot Access Path: version_store.secondary_index"),
        "mixed path must expose its hot secondary-index side, got:\n{}",
        mixed_plan
    );
}

#[test]
fn test_explain_multi_index_or() {
    let db = setup_test_db();

    // OR with multiple indexed columns should show Multi-Index Scan
    let lines = get_plan_output(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE user_id = 1 OR status = 'active'",
    );

    let plan = lines.join("\n");
    assert!(
        plan.contains("Multi-Index Scan"),
        "Expected Multi-Index Scan, got:\n{}",
        plan
    );
    assert!(plan.contains("OR"), "Expected OR operation, got:\n{}", plan);
}

#[test]
fn test_explain_multi_index_and() {
    let db = setup_test_db();

    // AND with multiple indexed columns should show Multi-Index Scan
    let lines = get_plan_output(
        &db,
        "EXPLAIN SELECT * FROM orders WHERE user_id = 1 AND status = 'active'",
    );

    let plan = lines.join("\n");
    // Could be either Multi-Index Scan or single Index Scan depending on optimizer
    assert!(
        plan.contains("Index Scan") || plan.contains("Multi-Index Scan"),
        "Expected Index Scan or Multi-Index Scan, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_range_query() {
    let db = setup_test_db();

    // Range query on indexed column should show Index Scan
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders WHERE user_id > 1");

    let plan = lines.join("\n");
    assert!(
        plan.contains("Index Scan"),
        "Expected Index Scan, got:\n{}",
        plan
    );
    assert!(
        plan.contains("> 1"),
        "Expected > 1 condition, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_analyze_seq_scan() {
    let db = setup_test_db();

    // EXPLAIN ANALYZE on non-indexed column
    let lines = get_plan_output(
        &db,
        "EXPLAIN ANALYZE SELECT * FROM orders WHERE amount > 50",
    );

    let plan = lines.join("\n");
    assert!(
        plan.contains("actual time="),
        "Expected actual time, got:\n{}",
        plan
    );
    assert!(
        plan.contains("SELECT (actual time=") && plan.contains("rows=5)"),
        "Expected actual rows, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Seq Scan"),
        "Expected Seq Scan, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_analyze_index_scan() {
    let db = setup_test_db();

    // EXPLAIN ANALYZE on indexed column
    let lines = get_plan_output(
        &db,
        "EXPLAIN ANALYZE SELECT * FROM orders WHERE user_id = 2",
    );

    let plan = lines.join("\n");
    assert!(
        plan.contains("actual time="),
        "Expected actual time, got:\n{}",
        plan
    );
    assert!(
        plan.contains("SELECT (actual time=") && plan.contains("rows=4)"),
        "Expected actual rows, got:\n{}",
        plan
    );
    assert!(
        plan.contains("Index Scan"),
        "Expected Index Scan, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_analyze_pk_lookup() {
    let db = setup_test_db();

    // EXPLAIN ANALYZE on primary key
    let lines = get_plan_output(&db, "EXPLAIN ANALYZE SELECT * FROM orders WHERE id = 5");

    let plan = lines.join("\n");
    assert!(
        plan.contains("actual time="),
        "Expected actual time, got:\n{}",
        plan
    );
    assert!(plan.contains("rows=1"), "Expected rows=1, got:\n{}", plan);
    assert!(
        plan.contains("PK Lookup"),
        "Expected PK Lookup, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_analyze_reports_artifact_io_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("artifact_cache_explain");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).expect("open file database");
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", ())
            .expect("create table");
        for id in 1..=20 {
            db.execute(&format!("INSERT INTO items VALUES ({id}, 'item_{id}')"), ())
                .expect("insert item");
        }
        db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint");
        db.close().expect("close before reopen");
    }

    let db = Database::open(&dsn).expect("reopen file database");
    let lines = get_plan_output(&db, "EXPLAIN ANALYZE SELECT name FROM items WHERE id >= 1");
    let plan = lines.join("\n");

    assert!(
        plan.contains("artifact-backed Physical I/O:"),
        "EXPLAIN ANALYZE must expose artifact-backed physical I/O diagnostics after cold artifact-backed read:\n{}",
        plan
    );
    assert!(
        plan.contains("artifact-backed Physical I/O: request-local counters unavailable"),
        "EXPLAIN must not attribute process-global counters to one request:\n{}",
        plan
    );
}

#[test]
fn test_explain_analyze_empty_result() {
    let db = setup_test_db();

    // EXPLAIN ANALYZE with no matching rows
    let lines = get_plan_output(&db, "EXPLAIN ANALYZE SELECT * FROM orders WHERE id = 999");

    let plan = lines.join("\n");
    assert!(plan.contains("rows=0"), "Expected rows=0, got:\n{}", plan);
}

#[test]
fn test_explain_order_by() {
    let db = setup_test_db();

    // Query with ORDER BY should show Order clause
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders ORDER BY user_id DESC");

    let plan = lines.join("\n");
    assert!(
        plan.contains("Order By:"),
        "Expected Order By clause, got:\n{}",
        plan
    );
    assert!(
        plan.contains("DESC"),
        "Expected DESC in order, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_limit() {
    let db = setup_test_db();

    // Query with LIMIT should show Limit clause
    let lines = get_plan_output(&db, "EXPLAIN SELECT * FROM orders LIMIT 5");

    let plan = lines.join("\n");
    assert!(
        plan.contains("Limit:"),
        "Expected Limit clause, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_group_by() {
    let db = setup_test_db();

    // Query with GROUP BY should show Group clause
    let lines = get_plan_output(
        &db,
        "EXPLAIN SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
    );

    let plan = lines.join("\n");
    assert!(
        plan.contains("Group By:"),
        "Expected Group By clause, got:\n{}",
        plan
    );
}

#[test]
fn test_explain_join() {
    let db = setup_test_db();

    // Create another table for join
    db.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        (),
    )
    .expect("Failed to create users table");

    db.execute(
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        (),
    )
    .expect("Failed to insert users");

    // Query with JOIN
    let lines = get_plan_output(
        &db,
        "EXPLAIN SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
    );

    let plan = lines.join("\n");
    assert!(plan.contains("Join"), "Expected Join, got:\n{}", plan);
}

#[test]
fn test_explain_delete_reports_two_phase_batch_access_path() {
    let db = setup_test_db();
    let plan =
        get_plan_output(&db, "EXPLAIN DELETE FROM orders WHERE status = 'active'").join("\n");

    assert!(
        plan.contains("DML Candidate Source: dml.row_id_candidates.storage_exact_projection"),
        "expected explicit candidate discovery path:\n{plan}"
    );
    assert!(
        plan.contains("DML Read Boundary: predicate_columns + row_identity"),
        "expected predicate-only read boundary:\n{plan}"
    );
    assert!(
        plan.contains("DML Predicate Columns: status"),
        "expected exact predicate-column diagnostics:\n{plan}"
    );
    assert!(
        plan.contains("DML Mutation: dml.batch_delete.hot_mvcc+cold_tombstone"),
        "expected one batch mutation boundary:\n{plan}"
    );
}

#[test]
fn test_explain_delete_reports_full_row_fallback_reasons() {
    let db = setup_test_db();

    let returning = get_plan_output(
        &db,
        "EXPLAIN DELETE FROM orders WHERE status = 'active' RETURNING id",
    )
    .join("\n");
    assert!(
        returning.contains("DML Candidate Source: dml.executor.full_row_scan"),
        "RETURNING must report its full-row candidate path:\n{returning}"
    );
    assert!(
        returning.contains("reason=returning_payload"),
        "RETURNING fallback reason is part of the stable plan:\n{returning}"
    );
    assert!(
        returning.contains("DML Mutation: dml.per_row.primary_key_fallback"),
        "RETURNING must not be labelled as batch mutation:\n{returning}"
    );

    let executor_filter =
        get_plan_output(&db, "EXPLAIN DELETE FROM orders WHERE id + 1 > 5").join("\n");
    assert!(
        executor_filter.contains("reason=executor_predicate"),
        "non-pushdown predicate must expose its fallback:\n{executor_filter}"
    );
}

#[test]
fn test_explain_analyze_delete_keeps_batch_access_path_visible() {
    let db = setup_test_db();
    let plan = get_plan_output(
        &db,
        "EXPLAIN ANALYZE DELETE FROM orders WHERE status = 'active'",
    )
    .join("\n");

    assert!(
        plan.contains("DML Candidate Source: dml.row_id_candidates.storage_exact_projection"),
        "EXPLAIN ANALYZE must report the executed DML boundary:\n{plan}"
    );
    assert!(
        plan.contains("DML Mutation: dml.batch_delete.hot_mvcc+cold_tombstone"),
        "EXPLAIN ANALYZE must expose batch mutation:\n{plan}"
    );
}
