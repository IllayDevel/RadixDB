// Copyright 2026 RadixDB Contributors
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "test-failpoints")]

use radixdb::{test_failpoints, Database};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use tempfile::tempdir;

#[test]
fn r2_l06_batch_b_terminal_outcomes_are_never_silent_or_ambiguous() {
    let mut failures = Vec::new();

    #[cfg(not(feature = "test-filedb"))]
    {
        let memory = Database::open("memory://r2_l06_batch_b").unwrap();
        if memory.execute("PRAGMA SNAPSHOT", ()).is_ok() {
            failures.push("memory-only SNAPSHOT returned durable success".to_string());
        }
        if memory.execute("PRAGMA CHECKPOINT", ()).is_ok() {
            failures.push("memory-only CHECKPOINT returned durable success".to_string());
        }
        memory.close().unwrap();
    }

    for query in ["sync=typo", "compression=maybe", "unknown_durability=full"] {
        let dir = tempdir().unwrap();
        let dsn = format!("file://{}?{query}", dir.path().join("db").display());
        if let Ok(db) = Database::open(&dsn) {
            failures.push(format!("invalid persistence option was accepted: {query}"));
            db.close().unwrap();
        }
    }

    {
        let dir = tempdir().unwrap();
        let dsn = format!("file://{}", dir.path().join("empty_restore").display());
        let db = Database::open(&dsn).unwrap();
        db.create_snapshot().unwrap();
        db.execute("CREATE TABLE later (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        match db.restore_snapshot(None) {
            Ok(_) => {
                if db.table_exists("later").unwrap() {
                    failures
                        .push("empty committed snapshot did not restore an empty catalog".into());
                }
            }
            Err(error) => failures.push(format!(
                "empty committed snapshot generation was rejected: {error}"
            )),
        }
        db.close().unwrap();
    }

    {
        let _guard = test_failpoints::FailpointGuard::new();
        let dir = tempdir().unwrap();
        let dsn = format!("file://{}", dir.path().join("drop_close").display());
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute("INSERT INTO t VALUES (1)", ()).unwrap();
        test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);
        let drop_outcome = catch_unwind(AssertUnwindSafe(|| drop(db)));
        let hits = test_failpoints::WAL_SYNC_FAIL.hit_count();
        test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);
        if hits == 0 {
            failures.push("automatic close did not reach the injected WAL sync failure".into());
        }
        if drop_outcome.is_err() {
            failures.push("last Database owner panicked on automatic close failure".into());
        }
    }

    assert!(
        failures.is_empty(),
        "R2-L06 batch B terminal-outcome violations:\n{}",
        failures.join("\n")
    );
}
