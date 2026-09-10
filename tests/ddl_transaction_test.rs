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

//! DDL Transaction Rollback Tests
//!
//! Tests for DDL statements (CREATE TABLE, DROP TABLE) being properly
//! rolled back within transactions (Bug #86)

use radixdb::Database;

#[test]
fn test_create_table_rollback() {
    let db = Database::open("memory://ddl_create_rollback").expect("Failed to create database");

    // Begin transaction
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");

    // Create table within transaction
    db.execute("CREATE TABLE rollback_test (id INTEGER, name TEXT)", ())
        .expect("Failed to create table");

    // Insert data
    db.execute("INSERT INTO rollback_test VALUES (1, 'test')", ())
        .expect("Failed to insert");

    // Verify table exists within transaction
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM rollback_test", ())
        .expect("Table should exist within transaction");
    assert_eq!(count, 1, "Should have 1 row within transaction");

    // Rollback transaction
    db.execute("ROLLBACK", ())
        .expect("Failed to rollback transaction");

    // Verify table no longer exists after rollback
    let result = db.query("SELECT * FROM rollback_test", ());
    assert!(result.is_err(), "Table should not exist after rollback");
}

#[test]
fn test_create_table_commit() {
    let db = Database::open("memory://ddl_create_commit").expect("Failed to create database");

    // Begin transaction
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");

    // Create table within transaction
    db.execute("CREATE TABLE commit_test (id INTEGER, name TEXT)", ())
        .expect("Failed to create table");

    // Insert data
    db.execute("INSERT INTO commit_test VALUES (1, 'test')", ())
        .expect("Failed to insert");

    // Commit transaction
    db.execute("COMMIT", ())
        .expect("Failed to commit transaction");

    // Verify table exists after commit
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM commit_test", ())
        .expect("Table should exist after commit");
    assert_eq!(count, 1, "Should have 1 row after commit");
}

#[test]
fn test_drop_table_rollback() {
    let temp = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        temp.path().join("ddl_drop_rollback").display()
    );
    let db = Database::open(&dsn).expect("Failed to create database");

    // First create a table outside transaction
    db.execute("CREATE TABLE persist_test (id INTEGER, name TEXT)", ())
        .expect("Failed to create table");

    // Insert some data
    db.execute("INSERT INTO persist_test VALUES (1, 'test')", ())
        .expect("Failed to insert");
    db.execute("PRAGMA CHECKPOINT", ())
        .expect("Failed to checkpoint table before DROP");

    // Verify table exists
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM persist_test", ())
        .expect("Table should exist");
    assert_eq!(count, 1, "Should have 1 row");

    // Begin transaction and drop table
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");
    db.execute("DROP TABLE persist_test", ())
        .expect("Failed to drop table");

    // Verify table is gone within transaction
    let result = db.query("SELECT * FROM persist_test", ());
    assert!(
        result.is_err(),
        "Table should not exist after DROP within transaction"
    );

    // Rollback transaction
    db.execute("ROLLBACK", ())
        .expect("Failed to rollback transaction");

    // Rollback restores the complete table identity, including its rows.
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM persist_test", ())
        .expect("Table and data should be restored after rollback");
    assert_eq!(count, 1, "DROP rollback must not discard table contents");

    drop(db);
    let reopened = Database::open(&dsn).expect("reopen database after DROP rollback");
    let count: i64 = reopened
        .query_one("SELECT COUNT(*) FROM persist_test", ())
        .expect("restored table should survive reopen");
    assert_eq!(count, 1, "DROP rollback must be durable across reopen");
}

#[test]
fn test_drop_table_commit() {
    let db = Database::open("memory://ddl_drop_commit").expect("Failed to create database");

    // First create a table
    db.execute("CREATE TABLE drop_commit_test (id INTEGER, name TEXT)", ())
        .expect("Failed to create table");

    // Begin transaction and drop table
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");
    db.execute("DROP TABLE drop_commit_test", ())
        .expect("Failed to drop table");

    // Commit transaction
    db.execute("COMMIT", ())
        .expect("Failed to commit transaction");

    // Verify table is gone after commit
    let result = db.query("SELECT * FROM drop_commit_test", ());
    assert!(
        result.is_err(),
        "Table should not exist after committed DROP"
    );
}

#[test]
fn test_multiple_ddl_in_transaction() {
    let db = Database::open("memory://ddl_multiple").expect("Failed to create database");

    // Begin transaction
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");

    // Create multiple tables
    db.execute("CREATE TABLE table1 (id INTEGER)", ())
        .expect("Failed to create table1");
    db.execute("CREATE TABLE table2 (id INTEGER)", ())
        .expect("Failed to create table2");
    db.execute("CREATE TABLE table3 (id INTEGER)", ())
        .expect("Failed to create table3");

    // Verify all tables exist
    db.execute("INSERT INTO table1 VALUES (1)", ())
        .expect("table1 should exist");
    db.execute("INSERT INTO table2 VALUES (2)", ())
        .expect("table2 should exist");
    db.execute("INSERT INTO table3 VALUES (3)", ())
        .expect("table3 should exist");

    // Rollback
    db.execute("ROLLBACK", ())
        .expect("Failed to rollback transaction");

    // Verify all tables are gone
    assert!(
        db.query("SELECT * FROM table1", ()).is_err(),
        "table1 should not exist after rollback"
    );
    assert!(
        db.query("SELECT * FROM table2", ()).is_err(),
        "table2 should not exist after rollback"
    );
    assert!(
        db.query("SELECT * FROM table3", ()).is_err(),
        "table3 should not exist after rollback"
    );
}

#[test]
fn test_ddl_outside_transaction_auto_commits() {
    let db = Database::open("memory://ddl_auto_commit").expect("Failed to create database");

    // Create table outside explicit transaction (should auto-commit)
    db.execute("CREATE TABLE auto_commit_test (id INTEGER)", ())
        .expect("Failed to create table");

    // Table should exist
    let result = db.query("SELECT * FROM auto_commit_test", ());
    assert!(result.is_ok(), "Table should exist after auto-commit");
}

#[test]
fn test_mixed_ddl_and_dml_rollback() {
    let db = Database::open("memory://ddl_dml_mixed").expect("Failed to create database");

    // Create a table first
    db.execute("CREATE TABLE existing_table (id INTEGER, value TEXT)", ())
        .expect("Failed to create table");
    db.execute("INSERT INTO existing_table VALUES (1, 'original')", ())
        .expect("Failed to insert");

    // Begin transaction
    db.execute("BEGIN", ())
        .expect("Failed to begin transaction");

    // Create new table
    db.execute("CREATE TABLE new_table (id INTEGER)", ())
        .expect("Failed to create new table");

    // Modify existing table
    db.execute(
        "UPDATE existing_table SET value = 'modified' WHERE id = 1",
        (),
    )
    .expect("Failed to update");

    // Rollback
    db.execute("ROLLBACK", ())
        .expect("Failed to rollback transaction");

    // New table should not exist
    assert!(
        db.query("SELECT * FROM new_table", ()).is_err(),
        "New table should not exist after rollback"
    );

    // Existing table should have original value (DML rollback)
    let value: String = db
        .query_one("SELECT value FROM existing_table WHERE id = 1", ())
        .expect("Should be able to query existing table");
    assert_eq!(value, "original", "Value should be rolled back to original");
}

#[test]
fn test_create_view_is_private_until_commit_and_survives_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        temp.path().join("ddl_create_view_commit").display()
    );
    let db = Database::open(&dsn).expect("create database");
    db.execute(
        "CREATE TABLE source_rows (id INTEGER PRIMARY KEY, value TEXT)",
        (),
    )
    .expect("create source table");
    db.execute("INSERT INTO source_rows VALUES (1, 'one'), (2, 'two')", ())
        .expect("insert source rows");

    db.execute("BEGIN", ()).expect("begin transaction");
    db.execute(
        "CREATE VIEW visible_rows AS SELECT id, value FROM source_rows WHERE id >= 2",
        (),
    )
    .expect("stage view");
    let value: String = db
        .query_one("SELECT value FROM visible_rows WHERE id = 2", ())
        .expect("transaction must see its private view");
    assert_eq!(value, "two");
    let show = db
        .query("SHOW CREATE VIEW visible_rows", ())
        .expect("SHOW must use the private catalog generation");
    let rows = show.collect_vec().expect("collect SHOW CREATE VIEW");
    let create_sql = rows[0]
        .get_by_name::<String>("Create View")
        .expect("CREATE VIEW text");
    assert!(create_sql.contains("CREATE VIEW visible_rows"));
    db.execute("COMMIT", ()).expect("commit view");

    drop(db);
    let reopened = Database::open(&dsn).expect("reopen database");
    let count: i64 = reopened
        .query_one("SELECT COUNT(*) FROM visible_rows", ())
        .expect("committed view must survive reopen");
    assert_eq!(count, 1);
}

#[test]
fn test_create_and_drop_view_rollback_restore_private_catalog() {
    let db = Database::open("memory://ddl_view_rollback").expect("create database");
    db.execute("CREATE TABLE source_rows (id INTEGER PRIMARY KEY)", ())
        .expect("create source table");
    db.execute("INSERT INTO source_rows VALUES (1)", ())
        .expect("insert source row");

    db.execute("BEGIN", ()).expect("begin create rollback");
    db.execute(
        "CREATE VIEW discarded_view AS SELECT id FROM source_rows",
        (),
    )
    .expect("stage discarded view");
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM discarded_view", ())
            .expect("private created view must be queryable"),
        1
    );
    db.execute("ROLLBACK", ()).expect("rollback created view");
    assert!(db.query("SELECT * FROM discarded_view", ()).is_err());

    db.execute(
        "CREATE VIEW retained_view AS SELECT id FROM source_rows",
        (),
    )
    .expect("create retained view");
    db.execute("BEGIN", ()).expect("begin drop rollback");
    db.execute("DROP VIEW retained_view", ())
        .expect("stage dropped view");
    assert!(
        db.query("SELECT * FROM retained_view", ()).is_err(),
        "private dropped view must disappear immediately"
    );
    db.execute("ROLLBACK", ()).expect("rollback dropped view");
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM retained_view", ())
            .expect("rollback must restore the view"),
        1
    );
}

/// Test: MODIFY COLUMN to NOT NULL fails when existing rows have NULLs
#[test]
fn test_modify_column_not_null_rejects_existing_nulls() {
    let db = Database::open("memory://modify_not_null_reject").expect("Failed to create database");

    db.execute(
        "CREATE TABLE t_nullable (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO t_nullable VALUES (1, NULL)", ())
        .expect("Failed to insert");
    db.execute("INSERT INTO t_nullable VALUES (2, 'hello')", ())
        .expect("Failed to insert");

    // Should fail because row 1 has NULL in val
    let result = db.execute("ALTER TABLE t_nullable MODIFY COLUMN val TEXT NOT NULL", ());
    assert!(result.is_err(), "Should reject NOT NULL when NULLs exist");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not null constraint"),
        "Error should mention not null constraint, got: {}",
        err
    );
}

/// Test: MODIFY COLUMN to NOT NULL succeeds when no NULLs exist
#[test]
fn test_modify_column_not_null_succeeds_without_nulls() {
    let db = Database::open("memory://modify_not_null_success").expect("Failed to create database");

    db.execute(
        "CREATE TABLE t_non_null (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("Failed to create table");

    db.execute("INSERT INTO t_non_null VALUES (1, 'a')", ())
        .expect("Failed to insert");
    db.execute("INSERT INTO t_non_null VALUES (2, 'b')", ())
        .expect("Failed to insert");

    // Should succeed because no NULLs
    db.execute("ALTER TABLE t_non_null MODIFY COLUMN val TEXT NOT NULL", ())
        .expect("MODIFY COLUMN should succeed when no NULLs exist");

    // Verify the constraint is now enforced on new inserts
    let result = db.execute("INSERT INTO t_non_null VALUES (3, NULL)", ());
    assert!(
        result.is_err(),
        "INSERT with NULL should fail after NOT NULL constraint"
    );
}

/// Test: MODIFY COLUMN to NOT NULL on empty table succeeds
#[test]
fn test_modify_column_not_null_empty_table() {
    let db = Database::open("memory://modify_not_null_empty").expect("Failed to create database");

    db.execute(
        "CREATE TABLE t_empty (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("Failed to create table");

    // Empty table, no NULLs to violate
    db.execute("ALTER TABLE t_empty MODIFY COLUMN val TEXT NOT NULL", ())
        .expect("Should succeed on empty table");
}

/// Test: MODIFY COLUMN applies DEFAULT metadata instead of silently ignoring it
#[test]
fn test_modify_column_default_updates_metadata_and_insert_default() {
    let db = Database::open("memory://modify_default_metadata").expect("Failed to create database");

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .expect("Failed to create table");

    db.execute(
        "ALTER TABLE users MODIFY COLUMN name TEXT NOT NULL DEFAULT 'anonymous'",
        (),
    )
    .expect("MODIFY COLUMN DEFAULT should succeed");

    let describe = db
        .query("DESCRIBE users", ())
        .expect("Failed to describe table");
    let mut name_default = String::new();
    for row in describe {
        let row = row.expect("Failed to read DESCRIBE row");
        let column_name: String = row.get(0).unwrap();
        if column_name == "name" {
            name_default = row.get(4).unwrap();
        }
    }
    assert!(
        name_default.contains("anonymous"),
        "DESCRIBE should expose modified default, got: {name_default}"
    );

    db.execute("INSERT INTO users (id) VALUES (1)", ())
        .expect("INSERT should use modified default");
    let name: String = db
        .query_one("SELECT name FROM users WHERE id = 1", ())
        .expect("Failed to query inserted default");
    assert_eq!(name, "anonymous");
}

/// Test: MODIFY COLUMN publishes key/index constraints instead of ignoring them.
#[test]
fn test_modify_column_key_constraints_are_published() {
    let db =
        Database::open("memory://modify_key_constraints_rejected").expect("Failed to create db");

    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
        (),
    )
    .expect("Failed to create table");

    db.execute("ALTER TABLE users MODIFY COLUMN email TEXT UNIQUE", ())
        .expect("UNIQUE should be published");
    db.execute("INSERT INTO users VALUES (1, 'a@example.test')", ())
        .unwrap();
    assert!(db
        .execute("INSERT INTO users VALUES (2, 'a@example.test')", ())
        .is_err());

    let primary_key = db.execute("ALTER TABLE users MODIFY COLUMN email TEXT PRIMARY KEY", ());
    assert!(
        primary_key.is_err(),
        "a second PRIMARY KEY must be rejected"
    );
}

#[test]
fn r6_create_rejects_duplicate_default_clauses() {
    let db = Database::open("memory://duplicate_default").expect("database");
    let error = db
        .execute(
            "CREATE TABLE invalid_defaults (id INTEGER, value INTEGER DEFAULT 1 DEFAULT 2)",
            (),
        )
        .expect_err("duplicate DEFAULT must not silently choose the first value");
    assert!(error.to_string().contains("DEFAULT"), "{error}");
    assert!(db.query("SELECT * FROM invalid_defaults", ()).is_err());
}

#[test]
fn r6_modify_column_is_a_coherent_complete_definition() {
    let db = Database::open("memory://modify_complete_definition").expect("database");
    db.execute(
        "CREATE TABLE settings (\
             id INTEGER PRIMARY KEY, \
             value INTEGER DEFAULT 7 CHECK (value > 0), \
             embedding VECTOR(3))",
        (),
    )
    .unwrap();

    // Omitting PRIMARY KEY while changing the same column must not make the
    // existing key nullable or erase its identity.
    db.execute("ALTER TABLE settings MODIFY COLUMN id INTEGER", ())
        .unwrap();
    // MODIFY is a full replacement for DEFAULT/CHECK metadata.
    db.execute("ALTER TABLE settings MODIFY COLUMN value INTEGER", ())
        .unwrap();
    // A former VECTOR column must not retain hidden dimensions.
    db.execute("ALTER TABLE settings MODIFY COLUMN embedding TEXT", ())
        .unwrap();

    let rows = db
        .query("DESCRIBE settings", ())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let id = rows
        .iter()
        .find(|row| row.get::<String>(0).unwrap() == "id")
        .unwrap();
    assert_eq!(id.get::<String>(2).unwrap(), "NO");
    assert_eq!(id.get::<String>(3).unwrap(), "PRI");
    let value = rows
        .iter()
        .find(|row| row.get::<String>(0).unwrap() == "value")
        .unwrap();
    assert!(value.get::<String>(4).unwrap().is_empty());
    let embedding = rows
        .iter()
        .find(|row| row.get::<String>(0).unwrap() == "embedding")
        .unwrap();
    assert_eq!(embedding.get::<String>(1).unwrap(), "TEXT");

    db.execute("INSERT INTO settings VALUES (1, -1, 'plain text')", ())
        .expect("omitted CHECK was replaced, not retained");
    db.execute(
        "INSERT INTO settings(id, embedding) VALUES (2, 'empty')",
        (),
    )
    .expect("omitted DEFAULT was replaced, not retained");
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM settings WHERE value IS NULL", ())
            .unwrap(),
        1
    );
}

#[test]
fn r6_parent_table_rename_rebinds_foreign_key_by_stable_identity_after_reopen() {
    let directory = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_on_close=false&checkpoint_interval=0",
        directory.path().join("fk-parent-rename").display()
    );
    let db = Database::open(&dsn).expect("database");
    db.execute(
        "CREATE TABLE parents (id INTEGER PRIMARY KEY); \
         CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))",
        (),
    )
    .expect("create FK graph");
    db.execute("INSERT INTO parents VALUES (1)", ())
        .expect("insert parent");
    db.execute("ALTER TABLE parents RENAME TO renamed_parents", ())
        .expect("rename referenced table");
    db.execute("INSERT INTO children VALUES (10, 1)", ())
        .expect("live FK resolves renamed parent");
    assert!(db
        .execute("INSERT INTO children VALUES (11, 999)", ())
        .is_err());
    db.close().expect("close renamed FK graph");

    let reopened = Database::open(&dsn).expect("reopen renamed FK graph");
    reopened
        .execute("INSERT INTO children VALUES (12, 1)", ())
        .expect("recovered FK resolves renamed parent");
    assert!(reopened
        .execute("INSERT INTO children VALUES (13, 999)", ())
        .is_err());
    assert_eq!(
        reopened
            .query_one::<i64, _>(
                "SELECT COUNT(*) FROM children c \
                 INNER JOIN renamed_parents p ON p.id = c.parent_id",
                (),
            )
            .expect("query renamed FK graph"),
        2
    );
    reopened.close().expect("close recovered FK graph");
}
