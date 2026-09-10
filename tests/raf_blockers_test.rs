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

//! Regression tests for RAF production blockers.

use radixdb::{named_params, DataType, Database, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn index_metadata(db: &Database, table_name: &str) -> Vec<(String, String, bool)> {
    let rows = db
        .query(&format!("SHOW INDEXES FROM {table_name}"), ())
        .expect("show indexes");

    rows.map(|row| {
        let row = row.expect("index row");
        let index_name: String = row.get(1).expect("index_name");
        let column_name: String = row.get(2).expect("column_name");
        let is_unique: bool = row.get(4).expect("is_unique");
        (index_name, column_name, is_unique)
    })
    .collect()
}

fn has_index(db: &Database, table_name: &str, index_name: &str) -> bool {
    index_metadata(db, table_name)
        .iter()
        .any(|(name, _, _)| name == index_name)
}

fn index_options(db: &Database, table_name: &str, index_name: &str) -> Option<String> {
    let rows = db
        .query(&format!("SHOW INDEXES FROM {table_name}"), ())
        .expect("show indexes");

    rows.map(|row| {
        let row = row.expect("index row");
        let name: String = row.get(1).expect("index_name");
        let options: String = row.get(5).expect("options");
        (name, options)
    })
    .find_map(|(name, options)| (name == index_name).then_some(options))
}

#[test]
fn raf_partial_unique_soft_delete_contract() {
    let db =
        Database::open("memory://raf_partial_unique_soft_delete").expect("open partial unique db");

    db.execute(
        "CREATE TABLE raf_contract_unique_records (
            id UUID PRIMARY KEY,
            email TEXT NOT NULL,
            __raf_deleted_at TIMESTAMP
        )",
        (),
    )
    .expect("create contract table");

    db.execute(
        "CREATE UNIQUE INDEX raf_contract_unique_email_live_idx
         ON raf_contract_unique_records (email)
         WHERE __raf_deleted_at IS NULL",
        (),
    )
    .expect("create partial unique index");

    db.execute_named(
        "INSERT INTO raf_contract_unique_records (id, email, __raf_deleted_at)
         VALUES (:id, :email, :deleted_at)",
        named_params! {
            id: "550e8400-e29b-41d4-a716-446655440000",
            email: "owner@example.test",
            deleted_at: Value::Null(DataType::Timestamp),
        },
    )
    .expect("insert first active record");

    let duplicate = db.execute_named(
        "INSERT INTO raf_contract_unique_records (id, email, __raf_deleted_at)
         VALUES (:id, :email, :deleted_at)",
        named_params! {
            id: "550e8400-e29b-41d4-a716-446655440001",
            email: "owner@example.test",
            deleted_at: Value::Null(DataType::Timestamp),
        },
    );
    assert!(
        duplicate.is_err(),
        "duplicate active soft-delete key must be rejected"
    );

    db.execute_named(
        "UPDATE raf_contract_unique_records
         SET __raf_deleted_at = :deleted_at
         WHERE id = :id",
        named_params! {
            id: "550e8400-e29b-41d4-a716-446655440000",
            deleted_at: "2026-08-07T00:00:00Z",
        },
    )
    .expect("soft delete first record");

    db.execute_named(
        "INSERT INTO raf_contract_unique_records (id, email, __raf_deleted_at)
         VALUES (:id, :email, :deleted_at)",
        named_params! {
            id: "550e8400-e29b-41d4-a716-446655440002",
            email: "owner@example.test",
            deleted_at: Value::Null(DataType::Timestamp),
        },
    )
    .expect("insert active replacement after soft delete");

    let active_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM raf_contract_unique_records
             WHERE email = 'owner@example.test' AND __raf_deleted_at IS NULL",
            (),
        )
        .expect("count active records");
    assert_eq!(active_count, 1);

    let options = index_options(
        &db,
        "raf_contract_unique_records",
        "raf_contract_unique_email_live_idx",
    )
    .expect("partial index must be visible in SHOW INDEXES");
    let options_lower = options.to_lowercase();
    assert!(
        options_lower.contains("where=") && options_lower.contains("__raf_deleted_at is null"),
        "SHOW INDEXES options should expose partial predicate, got: {options}"
    );
}

#[test]
fn raf_partial_index_rejects_unsupported_predicates() {
    let db = Database::open("memory://raf_partial_index_rejects_predicates")
        .expect("open partial predicate validation db");
    db.execute(
        "CREATE TABLE raf_partial_predicate_users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            __raf_deleted_at TIMESTAMP
        )",
        (),
    )
    .expect("create users");

    let function_err = db
        .execute(
            "CREATE INDEX raf_partial_func_idx
             ON raf_partial_predicate_users (email)
             WHERE LOWER(email) = 'owner@example.test'",
            (),
        )
        .expect_err("function predicate must be rejected");
    assert!(
        function_err
            .to_string()
            .contains("unsupported partial index predicate"),
        "unexpected function predicate error: {function_err}"
    );

    let unknown_column_err = db
        .execute(
            "CREATE INDEX raf_partial_missing_idx
             ON raf_partial_predicate_users (email)
             WHERE missing_column IS NULL",
            (),
        )
        .expect_err("unknown predicate column must be rejected");
    assert!(
        unknown_column_err.to_string().contains("missing_column"),
        "unexpected unknown column error: {unknown_column_err}"
    );

    let qualified_err = db
        .execute(
            "CREATE INDEX raf_partial_qualified_idx
             ON raf_partial_predicate_users (email)
             WHERE raf_partial_predicate_users.__raf_deleted_at IS NULL",
            (),
        )
        .expect_err("qualified predicate column must be rejected for now");
    assert!(
        qualified_err
            .to_string()
            .contains("qualified column reference"),
        "unexpected qualified column error: {qualified_err}"
    );
}

#[test]
fn raf_partial_index_public_syntax_variants_execute() {
    let db =
        Database::open("memory://raf_partial_index_public_syntax").expect("open partial syntax db");
    db.execute(
        "CREATE TABLE raf_partial_syntax_users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            active BOOLEAN NOT NULL,
            deleted_at TIMESTAMP
        )",
        (),
    )
    .expect("create users");

    db.execute(
        "CREATE INDEX IF NOT EXISTS raf_partial_if_not_exists_idx
         ON raf_partial_syntax_users (email)
         WHERE deleted_at IS NULL",
        (),
    )
    .expect("create partial IF NOT EXISTS index");
    db.execute(
        "CREATE INDEX IF NOT EXISTS raf_partial_if_not_exists_idx
         ON raf_partial_syntax_users (email)
         WHERE deleted_at IS NULL",
        (),
    )
    .expect("repeat partial IF NOT EXISTS index");
    let incompatible = db
        .execute(
            "CREATE INDEX IF NOT EXISTS raf_partial_if_not_exists_idx
             ON raf_partial_syntax_users (email)
             WHERE deleted_at IS NOT NULL",
            (),
        )
        .expect_err("IF NOT EXISTS must not hide incompatible partial predicate");
    assert!(
        incompatible.to_string().contains("different definition"),
        "unexpected incompatible IF NOT EXISTS error: {incompatible}"
    );
    db.execute(
        "CREATE INDEX raf_partial_btree_idx
         ON raf_partial_syntax_users (email)
         USING BTREE
         WHERE deleted_at IS NULL",
        (),
    )
    .expect("create partial BTREE index");
    db.execute(
        "CREATE INDEX raf_partial_hash_idx
         ON raf_partial_syntax_users (email)
         USING HASH
         WHERE deleted_at IS NULL",
        (),
    )
    .expect("create partial HASH index");
    db.execute(
        "CREATE INDEX raf_partial_bitmap_idx
         ON raf_partial_syntax_users (active)
         USING BITMAP
         WHERE active = true",
        (),
    )
    .expect("create partial BITMAP index");
    let unsupported_options = db.execute(
        "CREATE INDEX raf_partial_with_idx
         ON raf_partial_syntax_users (email)
         WITH (fillfactor = 90)
         WHERE deleted_at IS NULL",
        (),
    );
    assert!(
        unsupported_options.is_err(),
        "non-HNSW WITH options must be rejected instead of silently ignored"
    );

    let options = index_options(&db, "raf_partial_syntax_users", "raf_partial_btree_idx")
        .expect("partial BTREE index options");
    assert!(
        options.to_lowercase().contains("where="),
        "SHOW INDEXES must expose partial predicate for syntax variants, got: {options}"
    );
}

#[test]
fn raf_partial_index_planner_requires_predicate_implication() {
    let db = Database::open("memory://raf_partial_index_planner_safety")
        .expect("open planner safety db");
    db.execute(
        "CREATE TABLE raf_partial_planner_users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            __raf_deleted_at INTEGER
        )",
        (),
    )
    .expect("create users");
    db.execute(
        "CREATE INDEX raf_partial_planner_email_live_idx
         ON raf_partial_planner_users (email)
         WHERE __raf_deleted_at IS NULL",
        (),
    )
    .expect("create partial index");

    db.execute(
        "INSERT INTO raf_partial_planner_users (id, email, __raf_deleted_at)
         VALUES (1, 'owner@example.test', NULL)",
        (),
    )
    .expect("insert active");
    db.execute(
        "INSERT INTO raf_partial_planner_users (id, email, __raf_deleted_at)
         VALUES (2, 'owner@example.test', 10)",
        (),
    )
    .expect("insert deleted");

    let all_rows: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM raf_partial_planner_users
             WHERE email = 'owner@example.test'",
            (),
        )
        .expect("count without partial predicate");
    assert_eq!(
        all_rows, 2,
        "query without partial predicate must not use partial index and lose deleted rows"
    );

    let active_rows: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM raf_partial_planner_users
             WHERE email = 'owner@example.test' AND __raf_deleted_at IS NULL",
            (),
        )
        .expect("count with partial predicate");
    assert_eq!(active_rows, 1);

    let unsafe_explain = db
        .query(
            "EXPLAIN SELECT * FROM raf_partial_planner_users
             WHERE email = 'owner@example.test'",
            (),
        )
        .expect("explain unsafe query");
    let unsafe_plan = unsafe_explain
        .map(|row| row.expect("plan row").get::<String>(0).expect("plan line"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !unsafe_plan.contains("raf_partial_planner_email_live_idx"),
        "EXPLAIN must not choose partial index without implication:\n{unsafe_plan}"
    );
    assert!(
        unsafe_plan.contains("Partial Index Eligibility: no_proven_partial_index"),
        "EXPLAIN should expose why no partial-index path was selected:\n{unsafe_plan}"
    );

    let safe_explain = db
        .query(
            "EXPLAIN SELECT * FROM raf_partial_planner_users
             WHERE email = 'owner@example.test' AND __raf_deleted_at IS NULL",
            (),
        )
        .expect("explain safe query");
    let safe_plan = safe_explain
        .map(|row| row.expect("plan row").get::<String>(0).expect("plan line"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        safe_plan.contains("raf_partial_planner_email_live_idx"),
        "EXPLAIN may choose partial index when implication is proven:\n{safe_plan}"
    );
}

#[test]
fn raf_partial_hnsw_index_is_rejected() {
    let db = Database::open("memory://raf_partial_hnsw_rejected").expect("open hnsw db");
    db.execute(
        "CREATE TABLE raf_partial_vectors (
            id INTEGER PRIMARY KEY,
            active BOOLEAN NOT NULL,
            embedding VECTOR(3)
        )",
        (),
    )
    .expect("create vectors");

    let err = db
        .execute(
            "CREATE INDEX raf_partial_vec_idx
             ON raf_partial_vectors (embedding)
             USING HNSW
             WHERE active = TRUE",
            (),
        )
        .expect_err("partial HNSW must be rejected");
    assert!(
        err.to_string().contains("partial HNSW indexes"),
        "unexpected HNSW partial error: {err}"
    );
}

fn scoped_customers_db(name: &str) -> Database {
    let dsn = format!("memory://{name}");
    let db = Database::open(&dsn).expect("open database");
    db.execute(
        "CREATE TABLE raf080b_scoped_customers (
            id INTEGER PRIMARY KEY,
            group_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            revision INTEGER NOT NULL
        )",
        (),
    )
    .expect("create table");
    db.execute(
        "INSERT INTO raf080b_scoped_customers (id, group_id, name, revision)
         VALUES (101, 1, 'created', 0)",
        (),
    )
    .expect("insert fixture");
    db
}

fn current_name(db: &Database) -> String {
    db.query_one(
        "SELECT name FROM raf080b_scoped_customers WHERE id = 101",
        (),
    )
    .expect("read current name")
}

#[test]
fn raf_update_named_params_pk_and_revision_updates_matching_row() {
    let db = scoped_customers_db("raf_update_pk_revision");

    let visible: i64 = db
        .query_one_named(
            "SELECT COUNT(*) FROM raf080b_scoped_customers
             WHERE id = :id AND revision = :expected",
            named_params! { id: 101, expected: 0 },
        )
        .expect("select visibility");
    assert_eq!(visible, 1);

    let affected = db
        .execute_named(
            "UPDATE raf080b_scoped_customers
             SET name = :name
             WHERE id = :id AND revision = :expected",
            named_params! { id: 101, expected: 0, name: "updated" },
        )
        .expect("update by pk and revision");
    assert_eq!(affected, 1);
    assert_eq!(current_name(&db), "updated");
}

#[test]
fn raf_update_named_params_pk_and_group_updates_matching_row() {
    let db = scoped_customers_db("raf_update_pk_group_eq");

    let visible: i64 = db
        .query_one_named(
            "SELECT COUNT(*) FROM raf080b_scoped_customers
             WHERE id = :id AND group_id = :group",
            named_params! { id: 101, group: 1 },
        )
        .expect("select visibility");
    assert_eq!(visible, 1);

    let affected = db
        .execute_named(
            "UPDATE raf080b_scoped_customers
             SET name = :name
             WHERE id = :id AND group_id = :group",
            named_params! { id: 101, group: 1, name: "updated" },
        )
        .expect("update by pk and group");
    assert_eq!(affected, 1);
    assert_eq!(current_name(&db), "updated");
}

#[test]
fn raf_update_named_params_pk_and_group_in_updates_matching_row() {
    let db = scoped_customers_db("raf_update_pk_group_in");

    let visible: i64 = db
        .query_one_named(
            "SELECT COUNT(*) FROM raf080b_scoped_customers
             WHERE id = :id AND group_id IN (:group)",
            named_params! { id: 101, group: 1 },
        )
        .expect("select visibility");
    assert_eq!(visible, 1);

    let affected = db
        .execute_named(
            "UPDATE raf080b_scoped_customers
             SET name = :name
             WHERE id = :id AND group_id IN (:group)",
            named_params! { id: 101, group: 1, name: "updated" },
        )
        .expect("update by pk and group IN");
    assert_eq!(affected, 1);
    assert_eq!(current_name(&db), "updated");
}

#[test]
fn raf_update_literals_conjunctive_where_updates_matching_row() {
    let db = scoped_customers_db("raf_update_literals");

    let affected = db
        .execute(
            "UPDATE raf080b_scoped_customers
             SET name = 'updated'
             WHERE id = 101 AND revision = 0",
            (),
        )
        .expect("literal conjunctive update");
    assert_eq!(affected, 1);
    assert_eq!(current_name(&db), "updated");
}

#[test]
fn raf_update_conjunctive_where_mismatch_affects_zero_rows() {
    let db = scoped_customers_db("raf_update_mismatch");

    let affected = db
        .execute_named(
            "UPDATE raf080b_scoped_customers
             SET name = :name
             WHERE id = :id AND revision = :expected",
            named_params! { id: 101, expected: 99, name: "wrong" },
        )
        .expect("version mismatch update");
    assert_eq!(affected, 0);
    assert_eq!(current_name(&db), "created");
}

#[test]
fn raf_update_conjunctive_where_missing_row_affects_zero_rows() {
    let db = scoped_customers_db("raf_update_missing");

    let affected = db
        .execute_named(
            "UPDATE raf080b_scoped_customers
             SET name = :name
             WHERE id = :id AND group_id = :group",
            named_params! { id: 999, group: 1, name: "missing" },
        )
        .expect("missing row update");
    assert_eq!(affected, 0);
    assert_eq!(current_name(&db), "created");
}

fn scoped_orders_db(name: &str) -> Database {
    let dsn = format!("memory://{name}");
    let db = Database::open(&dsn).expect("open database");
    create_orders_fixture(&db);
    db
}

fn create_orders_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE raf080b_orders (
            id INTEGER PRIMARY KEY,
            revision INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            payload TEXT NOT NULL
        )",
        (),
    )
    .expect("create orders table");
    db.execute(
        "INSERT INTO raf080b_orders (id, revision, group_id, payload)
         VALUES
            (1, 0, 10, 'created'),
            (2, 1, 10, 'changed'),
            (3, 0, 20, 'other')",
        (),
    )
    .expect("insert orders fixture");
}

#[test]
fn raf_alter_index_rename_single_unique_keeps_metadata_and_reads() {
    let db = scoped_orders_db("raf_alter_index_single_unique");
    db.execute(
        "CREATE UNIQUE INDEX orders_payload_uq ON raf080b_orders (payload)",
        (),
    )
    .expect("create unique index");

    let before: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM raf080b_orders WHERE revision = 0 AND group_id = 10",
            (),
        )
        .expect("read before rename");
    assert_eq!(before, 1);

    db.execute(
        "ALTER INDEX orders_payload_uq RENAME TO ___orders_payload_uq",
        (),
    )
    .expect("rename index");

    let metadata = index_metadata(&db, "raf080b_orders");
    assert!(!metadata
        .iter()
        .any(|(name, _, _)| name == "orders_payload_uq"));
    assert!(metadata.iter().any(|(name, cols, unique)| {
        name == "___orders_payload_uq" && cols == "payload" && *unique
    }));

    let after: String = db
        .query_one(
            "SELECT payload FROM raf080b_orders WHERE revision = 0 AND group_id = 10",
            (),
        )
        .expect("read after rename");
    assert_eq!(after, "created");
}

#[test]
fn raf_alter_index_rename_single_nonunique_keeps_metadata_and_reads() {
    let db = scoped_orders_db("raf_alter_index_single_nonunique");
    db.execute(
        "CREATE INDEX orders_revision_idx ON raf080b_orders (revision)",
        (),
    )
    .expect("create nonunique index");

    db.execute(
        "ALTER INDEX orders_revision_idx RENAME TO ___orders_revision_idx",
        (),
    )
    .expect("rename index");

    let metadata = index_metadata(&db, "raf080b_orders");
    assert!(!metadata
        .iter()
        .any(|(name, _, _)| name == "orders_revision_idx"));
    assert!(metadata.iter().any(|(name, cols, unique)| {
        name == "___orders_revision_idx" && cols == "revision" && !*unique
    }));

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM raf080b_orders WHERE revision = 0", ())
        .expect("read after rename");
    assert_eq!(count, 2);
}

#[test]
fn raf_alter_index_rename_multicolumn_unique_and_nonunique_show_indexes() {
    let db = scoped_orders_db("raf_alter_index_multi");
    db.execute(
        "CREATE UNIQUE INDEX orders_revision_group_uq ON raf080b_orders (revision, group_id)",
        (),
    )
    .expect("create unique multi-column index");
    db.execute(
        "CREATE INDEX orders_group_revision_idx ON raf080b_orders (group_id, revision)",
        (),
    )
    .expect("create nonunique multi-column index");

    db.execute(
        "ALTER INDEX orders_revision_group_uq RENAME TO ___orders_revision_group_uq",
        (),
    )
    .expect("rename unique index");
    db.execute(
        "ALTER INDEX orders_group_revision_idx RENAME TO ___orders_group_revision_idx",
        (),
    )
    .expect("rename nonunique index");

    let metadata = index_metadata(&db, "raf080b_orders");
    assert!(metadata.iter().any(|(name, cols, unique)| {
        name == "___orders_revision_group_uq" && cols == "(revision, group_id)" && *unique
    }));
    assert!(metadata.iter().any(|(name, cols, unique)| {
        name == "___orders_group_revision_idx" && cols == "(group_id, revision)" && !*unique
    }));
    assert!(!metadata
        .iter()
        .any(|(name, _, _)| name == "orders_revision_group_uq"));
    assert!(!metadata
        .iter()
        .any(|(name, _, _)| name == "orders_group_revision_idx"));
}

#[test]
fn raf_alter_index_rename_rejects_name_collision() {
    let db = scoped_orders_db("raf_alter_index_collision");
    db.execute(
        "CREATE INDEX orders_revision_idx ON raf080b_orders (revision)",
        (),
    )
    .expect("create revision index");
    db.execute(
        "CREATE INDEX orders_group_idx ON raf080b_orders (group_id)",
        (),
    )
    .expect("create group index");

    let err = db
        .execute(
            "ALTER INDEX orders_revision_idx RENAME TO orders_group_idx",
            (),
        )
        .expect_err("collision must be rejected");
    assert!(
        err.to_string().contains("index already exists"),
        "unexpected error: {err}"
    );

    assert!(has_index(&db, "raf080b_orders", "orders_revision_idx"));
    assert!(has_index(&db, "raf080b_orders", "orders_group_idx"));
}

#[test]
fn raf_alter_index_rename_rejects_unknown_index() {
    let db = scoped_orders_db("raf_alter_index_unknown");

    let err = db
        .execute("ALTER INDEX missing_idx RENAME TO ___missing_idx", ())
        .expect_err("unknown index must be rejected");
    assert!(
        err.to_string().contains("Index not found") || err.to_string().contains("missing_idx"),
        "unexpected error: {err}"
    );
}

#[test]
fn raf_alter_index_rename_survives_wal_recovery() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}/raf_alter_index_recovery?checkpoint_interval=3600&checkpoint_on_close=off&sync_mode=normal",
        dir.path().display()
    );

    {
        let db = Database::open(&dsn).expect("open file db");
        create_orders_fixture(&db);
        db.execute(
            "CREATE UNIQUE INDEX orders_revision_idx ON raf080b_orders (revision, group_id)",
            (),
        )
        .expect("create unique index");
        db.execute(
            "ALTER INDEX orders_revision_idx RENAME TO ___orders_revision_idx",
            (),
        )
        .expect("rename index");
        db.close().expect("close without checkpoint");
    }

    {
        let db = Database::open(&dsn).expect("reopen file db");
        assert!(!has_index(&db, "raf080b_orders", "orders_revision_idx"));
        assert!(has_index(&db, "raf080b_orders", "___orders_revision_idx"));
        let count: i64 = db
            .query_one(
                "SELECT COUNT(*) FROM raf080b_orders WHERE revision = 0 AND group_id = 10",
                (),
            )
            .expect("read after recovery");
        assert_eq!(count, 1);
    }
}

#[test]
fn raf_alter_index_rename_does_not_break_concurrent_readers() {
    let db = scoped_orders_db("raf_alter_index_readers");
    db.execute(
        "CREATE INDEX orders_revision_idx ON raf080b_orders (revision)",
        (),
    )
    .expect("create index");

    let running = Arc::new(AtomicBool::new(true));
    let reader_db = db.clone();
    let reader_running = Arc::clone(&running);
    let reader = std::thread::spawn(move || {
        while reader_running.load(Ordering::Acquire) {
            let count: i64 = reader_db
                .query_one(
                    "SELECT COUNT(*) FROM raf080b_orders WHERE revision IN (0, 1)",
                    (),
                )
                .expect("concurrent read");
            assert_eq!(count, 3);
        }
    });

    db.execute(
        "ALTER INDEX orders_revision_idx RENAME TO ___orders_revision_idx",
        (),
    )
    .expect("rename index with reader");
    running.store(false, Ordering::Release);
    reader.join().expect("reader thread");

    assert!(has_index(&db, "raf080b_orders", "___orders_revision_idx"));
}
