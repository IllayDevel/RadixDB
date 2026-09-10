// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use radixdb::storage::mvcc::get_fast_timestamp;
use radixdb::Database;

fn count(db: &Database, sql: &str) -> i64 {
    db.query_one(sql, ()).expect("count query")
}

#[test]
fn rust_savepoint_restores_cold_only_uuid_delete_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/rust_cold_savepoint", dir.path().display());
    let key = "018f8f30-7b5c-7000-8000-000000000049";

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(&format!("INSERT INTO items VALUES ('{key}', 'kept')"), ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        let mut transaction = db.begin().unwrap();
        transaction.savepoint("before_cold_delete").unwrap();
        assert_eq!(
            transaction
                .execute(&format!("DELETE FROM items WHERE id = '{key}'"), ())
                .unwrap(),
            1
        );
        transaction
            .rollback_to_savepoint("before_cold_delete")
            .unwrap();
        transaction.commit().unwrap();
        db.close().unwrap();
    }

    let reopened = Database::open(&dsn).unwrap();
    assert_eq!(count(&reopened, "SELECT COUNT(*) FROM items"), 1);
    let value: String = reopened
        .query_one(&format!("SELECT value FROM items WHERE id = '{key}'"), ())
        .unwrap();
    assert_eq!(value, "kept");
}

#[test]
fn auto_increment_rejects_the_row_after_i64_max_without_mutation() {
    let db = Database::open("memory://v2_r2_auto_increment_exhaustion").unwrap();
    db.execute(
        "CREATE TABLE ids (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO ids VALUES (9223372036854775807, 'maximum')",
        (),
    )
    .unwrap();

    assert!(db
        .execute("INSERT INTO ids (id, value) VALUES (NULL, 'wrapped')", ())
        .is_err());
    assert_eq!(count(&db, "SELECT COUNT(*) FROM ids"), 1);
    let id: i64 = db.query_one("SELECT id FROM ids", ()).unwrap();
    assert_eq!(id, i64::MAX);
}

#[test]
fn wal_replay_preserves_insert_update_and_delete_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/wal_temporal?checkpoint_interval=3600&checkpoint_on_close=off",
        dir.path().display()
    );

    let (after_insert, after_update, after_delete) = {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE temporal (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO temporal VALUES (1, 'old')", ())
            .unwrap();
        let after_insert = get_fast_timestamp();
        db.execute("UPDATE temporal SET value = 'new' WHERE id = 1", ())
            .unwrap();
        let after_update = get_fast_timestamp();
        db.execute("DELETE FROM temporal WHERE id = 1", ()).unwrap();
        let after_delete = get_fast_timestamp();

        assert_temporal_history(&db, after_insert, after_update, after_delete);
        db.close().unwrap();
        (after_insert, after_update, after_delete)
    };

    let reopened = Database::open(&dsn).unwrap();
    assert_temporal_history(&reopened, after_insert, after_update, after_delete);
}

fn assert_temporal_history(db: &Database, after_insert: i64, after_update: i64, after_delete: i64) {
    let old: String = db
        .query_one(
            &format!("SELECT value FROM temporal AS OF TIMESTAMP {after_insert} WHERE id = 1"),
            (),
        )
        .unwrap();
    assert_eq!(old, "old");

    let new: String = db
        .query_one(
            &format!("SELECT value FROM temporal AS OF TIMESTAMP {after_update} WHERE id = 1"),
            (),
        )
        .unwrap();
    assert_eq!(new, "new");

    let deleted = db
        .query(
            &format!("SELECT value FROM temporal AS OF TIMESTAMP {after_delete} WHERE id = 1"),
            (),
        )
        .unwrap()
        .count();
    assert_eq!(deleted, 0);
}
