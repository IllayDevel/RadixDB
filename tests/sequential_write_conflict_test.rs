// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! RDB-0008 regressions for pre-claimed but unchanged DML candidates.

use radixdb::Database;
use tempfile::tempdir;

#[test]
fn sequential_index_candidate_that_fails_residual_does_not_become_an_insert_conflict() {
    let db = Database::open("memory://rdb0008_residual_candidate").unwrap();
    db.execute(
        "CREATE TABLE jobs (
            id INTEGER PRIMARY KEY,
            state TEXT NOT NULL,
            revision INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX jobs_state_idx ON jobs (state)", ())
        .unwrap();
    db.execute(
        "INSERT INTO jobs VALUES
            (1, 'active', 1),
            (2, 'active', 2)",
        (),
    )
    .unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute(
        "UPDATE jobs SET revision = 3
         WHERE state = 'active' AND revision = 2",
        (),
    )
    .unwrap();
    db.execute("COMMIT", ())
        .expect("an unchanged residual candidate is not a concurrent insert");

    assert_eq!(
        db.query_one::<i64, _>("SELECT revision FROM jobs WHERE id = 1", ())
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_one::<i64, _>("SELECT revision FROM jobs WHERE id = 2", ())
            .unwrap(),
        3
    );
}

#[test]
fn sequential_old_and_new_rows_survive_restart_snapshot_update_and_delete() {
    let dir = tempdir().unwrap();
    let dsn = format!("file://{}", dir.path().join("rdb0008.db").display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                revision INTEGER NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX sessions_user_idx ON sessions (user_id)", ())
            .unwrap();
        db.execute("INSERT INTO sessions VALUES (1, 7, 'active', 1)", ())
            .unwrap();
        db.execute(
            "UPDATE sessions SET state = 'revoked', revision = 2 WHERE id = 1",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO sessions VALUES (2, 7, 'active', 1)", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("PRAGMA SNAPSHOT", ()).unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("BEGIN", ()).unwrap();
        db.execute(
            "UPDATE sessions SET state = 'revoked', revision = revision + 1
             WHERE user_id = 7 AND state = 'active'",
            (),
        )
        .unwrap();
        db.execute(
            "DELETE FROM sessions WHERE user_id = 7 AND revision = 2 AND id = 1",
            (),
        )
        .unwrap();
        db.execute("COMMIT", ())
            .expect("previously committed old/new rows must not look concurrent");
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM sessions WHERE id = 1", ())
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT COUNT(*) FROM sessions
                 WHERE id = 2 AND state = 'revoked' AND revision = 2",
                (),
            )
            .unwrap(),
            1
        );
        db.close().unwrap();
    }
}

#[test]
fn sequential_writes_after_snapshot_restore_do_not_report_insert_conflict() {
    let dir = tempdir().unwrap();
    let dsn = format!("file://{}", dir.path().join("rdb0008_restore.db").display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                revision INTEGER NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX sessions_user_idx ON sessions (user_id)", ())
            .unwrap();
        db.execute(
            "INSERT INTO sessions VALUES
                (1, 7, 'revoked', 2),
                (2, 7, 'active', 1)",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("PRAGMA SNAPSHOT", ()).unwrap();

        // Move the live database away from the snapshot so RESTORE must
        // actually replace table state rather than reopen an identical copy.
        db.execute("DELETE FROM sessions", ()).unwrap();
        db.execute("INSERT INTO sessions VALUES (3, 8, 'active', 1)", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("PRAGMA RESTORE", ()).unwrap();

        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM sessions", ())
                .unwrap(),
            2
        );
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM sessions WHERE id = 3", ())
                .unwrap(),
            0
        );

        db.execute("BEGIN", ()).unwrap();
        db.execute(
            "UPDATE sessions SET state = 'revoked', revision = revision + 1
             WHERE user_id = 7 AND state = 'active'",
            (),
        )
        .unwrap();
        db.execute(
            "DELETE FROM sessions WHERE user_id = 7 AND revision = 2 AND id = 1",
            (),
        )
        .unwrap();
        db.execute("COMMIT", ())
            .expect("restored committed rows must retain existing-row provenance");
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM sessions WHERE id = 1", ())
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT COUNT(*) FROM sessions
                 WHERE id = 2 AND state = 'revoked' AND revision = 2",
                (),
            )
            .unwrap(),
            1
        );
        db.close().unwrap();
    }
}
