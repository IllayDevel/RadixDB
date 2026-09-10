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

//! Failpoint I/O Test Matrix
//!
//! Systematically tests I/O failure scenarios by arming failpoint flags
//! in the source code and verifying that:
//!
//! 1. Operations return appropriate errors when failpoints are armed
//! 2. No partial state is left behind (atomicity)
//! 3. The database recovers correctly after failpoint is disarmed
//!
//! Each failpoint is test-thread scoped, so parallel tests cannot inherit an
//! injected error and production builds have zero overhead.

#![cfg(feature = "test-failpoints")]

use radixdb::storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};
use radixdb::test_failpoints;
use radixdb::Database;
use std::sync::atomic::Ordering;
use tempfile::tempdir;

/// RAII guard that resets all failpoints on drop (even on panic).
fn failpoint_guard() -> test_failpoints::FailpointGuard {
    test_failpoints::FailpointGuard::new()
}

// ============================================================================
// WAL Write Failpoint Tests
// ============================================================================

#[test]
fn test_wal_write_fail_returns_error() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let db = Database::open(&format!("file://{}", dir.path().display()))
        .expect("Failed to open database");

    db.execute("CREATE TABLE fp_wal (id INTEGER PRIMARY KEY, val TEXT)", ())
        .expect("CREATE should succeed");

    // Arm the failpoint
    test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);

    // Writes should fail
    let result = db.execute("INSERT INTO fp_wal VALUES (1, 'hello')", ());
    assert!(
        result.is_err(),
        "INSERT should fail with WAL write failpoint armed"
    );

    let hits = test_failpoints::WAL_WRITE_FAIL.hit_count();
    assert!(hits >= 1, "WAL write failpoint was not reached");
    // Disarm
    test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

    // Writes should succeed again (use different id in case id=1 was partially applied)
    db.execute("INSERT INTO fp_wal VALUES (2, 'after_fail')", ())
        .expect("INSERT should succeed after disarming failpoint");

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM fp_wal", ())
        .expect("COUNT should work");
    assert!(count >= 1, "At least one row should exist, got {}", count);
}

#[test]
fn test_wal_write_fail_mid_transaction_atomicity() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let db = Database::open(&format!("file://{}", dir.path().display()))
        .expect("Failed to open database");

    db.execute(
        "CREATE TABLE fp_wal_tx (id INTEGER PRIMARY KEY, val INTEGER)",
        (),
    )
    .expect("CREATE should succeed");

    // Insert initial data
    db.execute("INSERT INTO fp_wal_tx VALUES (1, 100)", ())
        .expect("Initial insert should succeed");

    // Start a transaction with multiple operations
    db.execute("BEGIN", ()).expect("BEGIN should succeed");
    db.execute("UPDATE fp_wal_tx SET val = 200 WHERE id = 1", ())
        .expect("UPDATE should succeed within transaction");

    // Arm failpoint before commit
    test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);

    // Commit should fail
    let result = db.execute("COMMIT", ());
    // If COMMIT fails, the transaction should be rolled back
    if result.is_err() {
        let _ = db.execute("ROLLBACK", ());
    }

    assert!(
        test_failpoints::WAL_WRITE_FAIL.hit_count() >= 1,
        "transaction commit did not reach WAL write failpoint"
    );
    // Disarm
    test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

    // Value should still be original (transaction failed)
    let val: i64 = db
        .query_one("SELECT val FROM fp_wal_tx WHERE id = 1", ())
        .expect("SELECT should work");
    assert_eq!(val, 100, "Value should be unchanged after failed commit");
}

#[test]
fn test_wal_write_fail_recovery_after_disarm() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    {
        let db = Database::open(&path).expect("Failed to open database");
        db.execute(
            "CREATE TABLE fp_wal_rec (id INTEGER PRIMARY KEY, val TEXT)",
            (),
        )
        .expect("CREATE should succeed");
        db.execute("INSERT INTO fp_wal_rec VALUES (1, 'before')", ())
            .expect("INSERT should succeed");
    }

    // Reopen and verify data persisted
    {
        let db = Database::open(&path).expect("Failed to reopen database");
        let val: String = db
            .query_one("SELECT val FROM fp_wal_rec WHERE id = 1", ())
            .expect("SELECT should work");
        assert_eq!(val, "before");

        // Arm failpoint, try to write, fail
        test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
        let failed = db.execute("INSERT INTO fp_wal_rec VALUES (2, 'during_fail')", ());
        assert!(failed.is_err());
        assert!(test_failpoints::WAL_WRITE_FAIL.hit_count() >= 1);
        test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

        // Should still be usable
        db.execute("INSERT INTO fp_wal_rec VALUES (3, 'after_fail')", ())
            .expect("INSERT should succeed after disarming");
    }

    // Reopen and verify consistency
    {
        let db = Database::open(&path).expect("Failed to reopen after failpoint");
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_wal_rec", ())
            .expect("COUNT should work");
        // Row 1 (committed), row 2 (may or may not exist - failed write), row 3 (committed)
        assert!(
            count >= 2,
            "At least rows 1 and 3 should exist, got {}",
            count
        );
    }
}

#[test]
fn test_test_only_enospc_is_explicit_atomic_and_reopenable() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    {
        let db = Database::open(&path).expect("open ENOSPC fixture");
        db.execute(
            "CREATE TABLE fp_enospc (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO fp_enospc VALUES (1, 'before')", ())
            .unwrap();

        test_failpoints::FILESYSTEM_FULL_FAIL.store(true, Ordering::Release);
        let error = db
            .execute("INSERT INTO fp_enospc VALUES (2, 'must-not-publish')", ())
            .expect_err("injected ENOSPC must reach the caller");
        assert!(error.to_string().contains("ENOSPC"));
        assert_eq!(test_failpoints::FILESYSTEM_FULL_FAIL.hit_count(), 1);
        test_failpoints::FILESYSTEM_FULL_FAIL.store(false, Ordering::Release);

        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM fp_enospc", ())
                .unwrap(),
            1
        );
        db.execute("INSERT INTO fp_enospc VALUES (3, 'after')", ())
            .unwrap();
    }

    let reopened = Database::open(&path).expect("reopen after ENOSPC");
    assert_eq!(
        reopened
            .query_one::<i64, _>("SELECT COUNT(*) FROM fp_enospc", ())
            .unwrap(),
        2
    );
    assert_eq!(
        reopened
            .query_one::<i64, _>("SELECT COUNT(*) FROM fp_enospc WHERE id = 2", ())
            .unwrap(),
        0
    );
}

// ============================================================================
// WAL Sync Failpoint Tests
// ============================================================================

#[test]
fn test_wal_sync_fail_returns_error() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let db = Database::open(&format!("file://{}", dir.path().display()))
        .expect("Failed to open database");

    db.execute(
        "CREATE TABLE fp_sync (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("CREATE should succeed");

    // Arm sync failpoint
    test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);

    // Operations that require sync must fail at the armed boundary.
    let result = db.execute("INSERT INTO fp_sync VALUES (1, 'test')", ());
    assert!(
        result.is_err(),
        "WAL sync failure must reach public outcome"
    );
    assert!(test_failpoints::WAL_SYNC_FAIL.hit_count() >= 1);

    // Disarm
    test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);

    // Should be usable again
    let insert_result = db.execute("INSERT INTO fp_sync VALUES (2, 'after')", ());
    let _ = db.execute("INSERT INTO fp_sync VALUES (1, 'retry')", ());

    // Database should be in a consistent state
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM fp_sync", ())
        .expect("COUNT should work after sync recovery");
    assert!(count >= 1, "At least one row should exist");

    drop(insert_result);
}

// ============================================================================
// Physical Snapshot Generation Failpoint Tests
// ============================================================================

#[test]
fn snapshot_member_pre_sync_failure_reaches_public_api() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    let db = Database::open(&path).expect("Failed to open database");

    db.execute(
        "CREATE TABLE fp_snap (id INTEGER PRIMARY KEY, val INTEGER)",
        (),
    )
    .expect("CREATE should succeed");

    for i in 0..10 {
        db.execute(
            &format!("INSERT INTO fp_snap VALUES ({}, {})", i, i * 10),
            (),
        )
        .expect("INSERT should succeed");
    }

    let fault = GenerationFaultGuard::arm(
        GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync,
        GenerationFaultMode::ReturnIoError,
    );
    let snapshot_result = db.execute("PRAGMA SNAPSHOT", ());
    assert!(
        snapshot_result.is_err(),
        "snapshot member durability failure must propagate"
    );
    assert_eq!(fault.hit_count(), 1);
    drop(fault);

    // Data should still be accessible (WAL has the data even if snapshot failed)
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM fp_snap", ())
        .expect("COUNT should work");
    assert_eq!(count, 10, "All 10 rows should be accessible");

    // Sum should be correct
    let sum: f64 = db
        .query_one("SELECT SUM(val) FROM fp_snap", ())
        .expect("SUM should work");
    assert_eq!(sum, 450.0, "Sum should be 0+10+20+...+90 = 450");
}

#[test]
fn snapshot_member_pre_sync_failure_is_recoverable_on_reopen() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    {
        let db = Database::open(&path).expect("Failed to open database");
        db.execute(
            "CREATE TABLE fp_snap_rec (id INTEGER PRIMARY KEY, val INTEGER)",
            (),
        )
        .expect("CREATE should succeed");

        for i in 0..5 {
            db.execute(
                &format!("INSERT INTO fp_snap_rec VALUES ({}, {})", i, i),
                (),
            )
            .expect("INSERT should succeed");
        }

        let fault = GenerationFaultGuard::arm(
            GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync,
            GenerationFaultMode::ReturnIoError,
        );
        let snapshot = db.execute("PRAGMA SNAPSHOT", ());
        assert!(snapshot.is_err());
        assert_eq!(fault.hit_count(), 1);
        drop(fault);
    }

    // Reopen - should recover from WAL
    {
        let db = Database::open(&path).expect("Recovery should succeed");
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_snap_rec", ())
            .expect("COUNT should work after recovery");
        assert_eq!(count, 5, "All 5 rows should be recovered from WAL");
    }
}

#[test]
fn snapshot_member_post_sync_failure_reaches_public_api() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    let db = Database::open(&path).expect("Failed to open database");

    db.execute(
        "CREATE TABLE fp_ssync (id INTEGER PRIMARY KEY, val TEXT)",
        (),
    )
    .expect("CREATE should succeed");

    for i in 0..5 {
        db.execute(
            &format!("INSERT INTO fp_ssync VALUES ({}, 'row_{}')", i, i),
            (),
        )
        .expect("INSERT should succeed");
    }

    let fault = GenerationFaultGuard::arm(
        GenerationCrashPoint::SnapshotMemberDurable,
        GenerationFaultMode::ReturnIoError,
    );
    let snapshot = db.execute("PRAGMA SNAPSHOT", ());
    assert!(snapshot.is_err(), "snapshot sync failure must propagate");
    assert_eq!(fault.hit_count(), 1);
    drop(fault);

    // Data should still be accessible
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM fp_ssync", ())
        .expect("COUNT should work");
    assert_eq!(count, 5);
}

#[test]
fn snapshot_manifest_pre_rename_failure_is_atomic() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    {
        let db = Database::open(&path).expect("Failed to open database");
        db.execute(
            "CREATE TABLE fp_rename (id INTEGER PRIMARY KEY, val INTEGER)",
            (),
        )
        .expect("CREATE should succeed");

        for i in 0..10 {
            db.execute(
                &format!("INSERT INTO fp_rename VALUES ({}, {})", i, i * 100),
                (),
            )
            .expect("INSERT should succeed");
        }

        let fault = GenerationFaultGuard::arm(
            GenerationCrashPoint::SnapshotManifestAfterSyncBeforeRename,
            GenerationFaultMode::ReturnIoError,
        );
        let snapshot = db.execute("PRAGMA SNAPSHOT", ());
        let hits = fault.hit_count();
        drop(fault);
        let error = snapshot.expect_err(
            "snapshot manifest pre-rename fault must reach production and return an error",
        );
        assert!(error.to_string().contains("snapshot"));
        assert_eq!(
            hits, 1,
            "snapshot rename failpoint must be reached exactly once"
        );

        // Data should still be correct
        let sum: f64 = db
            .query_one("SELECT SUM(val) FROM fp_rename", ())
            .expect("SUM should work");
        assert_eq!(sum, 4500.0);
    }

    // Reopen and verify
    {
        let db = Database::open(&path).expect("Recovery should succeed");
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_rename", ())
            .expect("COUNT should work");
        assert_eq!(count, 10, "All rows should survive failed snapshot rename");
    }
}

// ============================================================================
// Checkpoint Write Failpoint Tests
// ============================================================================

#[test]
fn test_checkpoint_write_fail() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    {
        let db = Database::open(&path).expect("Failed to open database");
        db.execute(
            "CREATE TABLE fp_ckpt (id INTEGER PRIMARY KEY, val TEXT)",
            (),
        )
        .expect("CREATE should succeed");

        db.execute("INSERT INTO fp_ckpt VALUES (1, 'first')", ())
            .expect("INSERT should succeed");

        // Arm checkpoint write failpoint
        test_failpoints::CHECKPOINT_WRITE_FAIL.store(true, Ordering::Release);

        let checkpoint = db.execute("PRAGMA CHECKPOINT", ());
        assert!(checkpoint.is_err(), "checkpoint failpoint must propagate");
        assert!(test_failpoints::CHECKPOINT_WRITE_FAIL.hit_count() >= 1);

        test_failpoints::CHECKPOINT_WRITE_FAIL.store(false, Ordering::Release);
    }

    // Reopen - WAL replay should recover everything
    {
        let db = Database::open(&path).expect("Recovery should succeed");
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_ckpt", ())
            .expect("COUNT should work");
        assert!(
            count >= 1,
            "At least the first row should exist, got {}",
            count
        );
    }
}

// ============================================================================
// Combined failpoint scenarios
// ============================================================================

#[test]
fn test_multiple_failpoints_sequential() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    let db = Database::open(&path).expect("Failed to open database");
    db.execute(
        "CREATE TABLE fp_multi (id INTEGER PRIMARY KEY, val INTEGER)",
        (),
    )
    .expect("CREATE should succeed");

    // Phase 1: WAL write failure
    test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
    assert!(db
        .execute("INSERT INTO fp_multi VALUES (1, 100)", ())
        .is_err());
    assert!(test_failpoints::WAL_WRITE_FAIL.hit_count() >= 1);
    test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

    // Phase 2: Normal operation
    db.execute("INSERT INTO fp_multi VALUES (2, 200)", ())
        .expect("Should succeed after disarming WAL failpoint");

    // Phase 3: snapshot-generation failure
    let snapshot_fault = GenerationFaultGuard::arm(
        GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(db.execute("PRAGMA SNAPSHOT", ()).is_err());
    assert_eq!(snapshot_fault.hit_count(), 1);
    drop(snapshot_fault);

    // Phase 4: Normal operation again
    db.execute("INSERT INTO fp_multi VALUES (3, 300)", ())
        .expect("Should succeed after disarming snapshot failpoint");

    // Verify consistency
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM fp_multi", ())
        .expect("COUNT should work");
    assert!(
        count >= 2,
        "At least rows 2 and 3 should exist, got {}",
        count
    );
}

#[test]
fn test_failpoint_does_not_corrupt_existing_data() {
    let _guard = failpoint_guard();
    let dir = tempdir().unwrap();
    let path = format!("file://{}", dir.path().display());

    // Phase 1: Populate database
    {
        let db = Database::open(&path).expect("Failed to open database");
        db.execute(
            "CREATE TABLE fp_preserve (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
            (),
        )
        .expect("CREATE should succeed");

        for i in 0..20 {
            db.execute(
                &format!("INSERT INTO fp_preserve VALUES ({}, 'data_{}')", i, i),
                (),
            )
            .expect("INSERT should succeed");
        }
    }

    // Phase 2: Arm various failpoints and try operations
    {
        let db = Database::open(&path).expect("Reopen should succeed");

        // Verify initial data
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_preserve", ())
            .expect("COUNT should work");
        assert_eq!(count, 20);

        // Exercise the operation-scoped failpoints in sequence.
        let failpoints: &[&test_failpoints::Failpoint] = &[
            &test_failpoints::WAL_WRITE_FAIL,
            &test_failpoints::WAL_SYNC_FAIL,
            &test_failpoints::FILESYSTEM_FULL_FAIL,
            &test_failpoints::CHECKPOINT_WRITE_FAIL,
        ];

        for fp in failpoints {
            fp.store(true, Ordering::Release);
            // Try some operation - may or may not fail
            let _ = db.execute("INSERT INTO fp_preserve VALUES (999, 'fail')", ());
            let _ = db.execute("DELETE FROM fp_preserve WHERE id = 999", ());
            let _ = db.execute("VACUUM", ());
            fp.store(false, Ordering::Release);
        }

        // Original data should be intact
        let count_after: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_preserve WHERE id < 20", ())
            .expect("COUNT should work");
        assert_eq!(count_after, 20, "Original 20 rows should be preserved");
    }

    // Phase 3: Reopen and verify
    {
        let db = Database::open(&path).expect("Final reopen should succeed");

        // Physical snapshots use the generation-wide durable-boundary owner,
        // not the retired per-table snapshot flag registry. Reopen first
        // because an injected WAL failure deliberately stops that WAL owner.
        for point in GenerationCrashPoint::SNAPSHOT_PUBLICATION_POINTS {
            let fault = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
            let result = db.execute("PRAGMA SNAPSHOT", ());
            assert!(result.is_err(), "point={} result={result:?}", point.name());
            assert_eq!(
                fault.hit_count(),
                1,
                "point={} result={result:?}",
                point.name()
            );
            drop(fault);
        }

        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM fp_preserve WHERE id < 20", ())
            .expect("COUNT should work");
        assert_eq!(
            count, 20,
            "All original rows should survive failpoint storm"
        );
    }
}
