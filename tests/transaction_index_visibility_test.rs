// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read-your-writes regressions for secondary-index access paths.

use radixdb::Database;

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql, ())
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

#[test]
fn indexed_scan_merges_transaction_local_inserts_updates_and_deletes() {
    let db = Database::open("memory://transaction_index_visibility").unwrap();
    db.execute(
        "CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            state TEXT,
            payload TEXT
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX members_scope_state_idx ON members (scope, state)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO members VALUES
            (1, 10, 'active', 'committed'),
            (2, 20, 'inactive', 'committed')",
        (),
    )
    .unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute(
        "INSERT INTO members VALUES (3, 10, 'active', 'local insert')",
        (),
    )
    .unwrap();
    db.execute(
        "UPDATE members SET scope = 10, state = 'active' WHERE id = 2",
        (),
    )
    .unwrap();
    db.execute("DELETE FROM members WHERE id = 1", ()).unwrap();

    assert_eq!(
        ids(
            &db,
            "SELECT id FROM members
             WHERE scope = 10 AND state = 'active'
             ORDER BY id",
        ),
        vec![2, 3],
        "secondary-index candidates must include local rows and honor local deletes"
    );
    db.execute("ROLLBACK", ()).unwrap();

    assert_eq!(
        ids(
            &db,
            "SELECT id FROM members
             WHERE scope = 10 AND state = 'active'
             ORDER BY id",
        ),
        vec![1],
        "rollback must restore the committed secondary-index view"
    );
}

#[test]
fn indexed_ordered_scan_sees_insert_made_earlier_in_same_transaction() {
    let db = Database::open("memory://transaction_index_ordered_visibility").unwrap();
    db.execute(
        "CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            left_at TIMESTAMP
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE UNIQUE INDEX members_scope_user_uidx ON members (scope, user_id)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO members VALUES (1, 42, 100, NULL)", ())
        .unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute("INSERT INTO members VALUES (2, 42, 200, NULL)", ())
        .unwrap();
    assert_eq!(
        ids(
            &db,
            "SELECT user_id FROM members
             WHERE scope = 42 AND left_at IS NULL
             ORDER BY user_id",
        ),
        vec![100, 200],
        "application validation queries must observe their own inserted member"
    );
    db.execute("ROLLBACK", ()).unwrap();
}

#[test]
fn indexed_dml_merges_transaction_local_candidates() {
    let db = Database::open("memory://transaction_index_dml_visibility").unwrap();
    db.execute(
        "CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            state TEXT NOT NULL,
            payload TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX members_scope_state_idx ON members (scope, state)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO members VALUES
            (1, 10, 'active', 'committed match'),
            (2, 20, 'inactive', 'committed non-match')",
        (),
    )
    .unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute(
        "INSERT INTO members VALUES (3, 10, 'active', 'local insert')",
        (),
    )
    .unwrap();
    assert_eq!(
        db.execute(
            "UPDATE members SET payload = 'updated'
             WHERE scope = 10 AND state = 'active'",
            (),
        )
        .unwrap(),
        2,
        "indexed UPDATE must include the matching local insert"
    );
    db.execute(
        "UPDATE members SET scope = 10, state = 'active' WHERE id = 2",
        (),
    )
    .unwrap();
    assert_eq!(
        db.execute(
            "DELETE FROM members WHERE scope = 10 AND state = 'active'",
            (),
        )
        .unwrap(),
        3,
        "indexed DELETE must include rows whose local key entered the predicate"
    );
    assert!(ids(&db, "SELECT id FROM members ORDER BY id").is_empty());
    db.execute("ROLLBACK", ()).unwrap();

    assert_eq!(
        ids(&db, "SELECT id FROM members ORDER BY id"),
        vec![1, 2],
        "rollback must remove every local DML effect"
    );
}

#[test]
fn in_list_index_optimization_falls_back_for_transaction_local_rows() {
    let db = Database::open("memory://transaction_in_list_index_visibility").unwrap();
    db.execute(
        "CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            payload TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX members_scope_idx ON members (scope)", ())
        .unwrap();
    db.execute("INSERT INTO members VALUES (1, 10, 'committed')", ())
        .unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute("INSERT INTO members VALUES (2, 20, 'local insert')", ())
        .unwrap();
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM members WHERE scope IN (10, 20) ORDER BY id",
        ),
        vec![1, 2]
    );
    db.execute("ROLLBACK", ()).unwrap();
}

#[test]
fn cold_in_list_index_optimization_falls_back_for_transaction_local_rows() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        directory.path().display()
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE members (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            payload TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX members_scope_idx ON members (scope)", ())
        .unwrap();
    db.execute("INSERT INTO members VALUES (1, 10, 'cold committed')", ())
        .unwrap();
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute("INSERT INTO members VALUES (2, 20, 'local insert')", ())
        .unwrap();
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM members WHERE scope IN (10, 20) ORDER BY id",
        ),
        vec![1, 2]
    );
    db.execute("ROLLBACK", ()).unwrap();
}

fn count(db: &Database, table: &str) -> i64 {
    db.query_one::<i64, _>(&format!("SELECT COUNT(*) FROM {table}"), ())
        .unwrap()
}

fn assert_coherent_membership_winner(db: &Database) {
    assert_eq!(
        ids(
            db,
            "SELECT user_id FROM conversation_members
             WHERE conversation_id = 42 AND left_at IS NULL
             ORDER BY user_id",
        ),
        vec![100, 200]
    );
    assert_eq!(count(db, "command_results"), 1);
    assert_eq!(count(db, "messages"), 1);
    assert_eq!(count(db, "sync_events"), 2);
    assert_eq!(count(db, "outbox_jobs"), 1);
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT COUNT(*) FROM command_results
             WHERE command_key = 'same-key' AND status = 'committed'",
            (),
        )
        .unwrap(),
        1
    );
}

#[test]
fn coherent_indexed_membership_winner_survives_wal_checkpoint_and_snapshot_restore() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        directory.path().join("rdb0018-lifecycle").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE conversation_members (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                left_at TIMESTAMP
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE UNIQUE INDEX conversation_members_active_uidx
             ON conversation_members (conversation_id, user_id)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE command_results (
                id INTEGER PRIMARY KEY,
                command_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                UNIQUE (conversation_id, sequence)
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE sync_events (
                id INTEGER PRIMARY KEY,
                command_key TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                UNIQUE (command_key, user_id)
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE outbox_jobs (
                id INTEGER PRIMARY KEY,
                command_key TEXT NOT NULL UNIQUE
            )",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO conversation_members VALUES (1, 42, 100, NULL)",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        db.execute("BEGIN", ()).unwrap();
        db.execute(
            "INSERT INTO conversation_members VALUES (2, 42, 200, NULL)",
            (),
        )
        .unwrap();
        assert_eq!(
            ids(
                &db,
                "SELECT user_id FROM conversation_members
                 WHERE conversation_id = 42 AND left_at IS NULL
                 ORDER BY user_id",
            ),
            vec![100, 200],
            "the application validation query must see its own indexed insert"
        );
        db.execute(
            "INSERT INTO command_results VALUES (1, 'same-key', 'committed')",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO messages VALUES (1, 42, 1)", ())
            .unwrap();
        db.execute(
            "INSERT INTO sync_events VALUES
                (1, 'same-key', 100),
                (2, 'same-key', 200)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO outbox_jobs VALUES (1, 'same-key')", ())
            .unwrap();
        db.execute("COMMIT", ()).unwrap();
        assert_coherent_membership_winner(&db);
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_coherent_membership_winner(&db);

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("PRAGMA SNAPSHOT", ()).unwrap();

        db.execute("DELETE FROM sync_events", ()).unwrap();
        db.execute("DELETE FROM outbox_jobs", ()).unwrap();
        db.execute("DELETE FROM messages", ()).unwrap();
        db.execute("DELETE FROM command_results", ()).unwrap();
        db.execute(
            "UPDATE conversation_members SET left_at = CURRENT_TIMESTAMP
             WHERE user_id = 200",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(count(&db, "command_results"), 0);

        db.execute("PRAGMA RESTORE", ()).unwrap();
        assert_coherent_membership_winner(&db);
        db.close().unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_coherent_membership_winner(&db);
    db.close().unwrap();
}
