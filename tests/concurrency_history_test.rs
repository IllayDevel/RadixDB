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

#![cfg(feature = "stress-tests")]

//! Concurrency History Checking Tests
//!
//! Records a history of operations from concurrent threads, then validates
//! that the history is consistent with the claimed isolation level.
//!
//! Validation rules:
//! - Read Committed: no dirty reads (a txn never sees uncommitted writes)
//! - Snapshot Isolation: consistent snapshot (all reads within a txn see
//!   the same snapshot) and write-write conflict detection

use radixdb::{Database, IsolationLevel};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

// ============================================================================
// History recording types
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum Op {
    BeginFailed {
        thread: usize,
        txn: u64,
        start: u64,
        end: u64,
        error: String,
    },
    Begin {
        thread: usize,
        txn: u64,
        start: u64,
        end: u64,
    },
    Read {
        thread: usize,
        txn: u64,
        key: i64,
        value: Option<i64>,
        start: u64,
        end: u64,
    },
    Write {
        thread: usize,
        txn: u64,
        key: i64,
        value: i64,
        start: u64,
        end: u64,
    },
    Commit {
        thread: usize,
        txn: u64,
        start: u64,
        end: u64,
    },
    Rollback {
        thread: usize,
        txn: u64,
        start: u64,
        end: u64,
    },
    CommitFailed {
        thread: usize,
        txn: u64,
        start: u64,
        end: u64,
        error: String,
    },
}

static CLOCK: AtomicU64 = AtomicU64::new(0);

fn tick() -> u64 {
    CLOCK.fetch_add(1, Ordering::SeqCst)
}

type History = Arc<Mutex<Vec<Op>>>;

fn new_history() -> History {
    Arc::new(Mutex::new(Vec::new()))
}

fn record(history: &History, op: Op) {
    history.lock().unwrap().push(op);
}

// ============================================================================
// History validation
// ============================================================================

/// Validate Read Committed: no transaction ever reads a value written by
/// an uncommitted transaction.
fn validate_read_committed(history: &[Op]) -> Result<(), String> {
    use std::collections::{HashMap, HashSet};

    let mut writes: HashMap<u64, Vec<(i64, i64, u64)>> = HashMap::new();
    let mut commits: HashMap<u64, (u64, u64)> = HashMap::new();
    let mut aborted = HashSet::new();
    for op in history {
        match op {
            Op::Write {
                txn,
                key,
                value,
                end,
                ..
            } => writes.entry(*txn).or_default().push((*key, *value, *end)),
            Op::Commit {
                txn, start, end, ..
            } => {
                commits.insert(*txn, (*start, *end));
            }
            Op::Rollback { txn, .. } | Op::CommitFailed { txn, .. } => {
                aborted.insert(*txn);
            }
            _ => {}
        }
    }

    for op in history {
        let Op::Read {
            thread,
            txn,
            key,
            value: Some(read_value),
            end: read_end,
            ..
        } = op
        else {
            continue;
        };

        let initial = *read_value == *key;
        let own_write = writes.get(txn).is_some_and(|txn_writes| {
            txn_writes
                .iter()
                .any(|(write_key, write_value, write_end)| {
                    write_key == key && write_value == read_value && write_end <= read_end
                })
        });
        let committed_before_or_during_read = commits.iter().any(|(writer, (commit_start, _))| {
            !aborted.contains(writer)
                && commit_start <= read_end
                && writes.get(writer).is_some_and(|txn_writes| {
                    txn_writes.iter().any(|(write_key, write_value, _)| {
                        write_key == key && write_value == read_value
                    })
                })
        });

        if !initial && !own_write && !committed_before_or_during_read {
            return Err(format!(
                "dirty or causally impossible read: thread={thread} txn={txn} key={key} \
                 value={read_value} read_end={read_end}"
            ));
        }
    }
    Ok(())
}

/// Validate Snapshot Isolation: within a single transaction, all reads
/// should see a consistent snapshot. If a txn reads key K twice and gets
/// different values, the snapshot is inconsistent.
fn validate_snapshot_consistency(history: &[Op]) -> Result<(), String> {
    use std::collections::HashMap;

    let mut writes: HashMap<u64, Vec<(i64, u64)>> = HashMap::new();
    for op in history {
        if let Op::Write { txn, key, end, .. } = op {
            writes.entry(*txn).or_default().push((*key, *end));
        }
    }

    let mut previous_reads: HashMap<(u64, i64), (Option<i64>, u64)> = HashMap::new();
    for op in history {
        let Op::Read {
            thread,
            txn,
            key,
            value,
            end,
            ..
        } = op
        else {
            continue;
        };
        if let Some((previous_value, previous_end)) = previous_reads.get(&(*txn, *key)) {
            let intervening_own_write = writes.get(txn).is_some_and(|txn_writes| {
                txn_writes.iter().any(|(write_key, write_end)| {
                    write_key == key && previous_end < write_end && write_end <= end
                })
            });
            if previous_value != value && !intervening_own_write {
                return Err(format!(
                    "snapshot changed without an intervening own write: thread={thread} \
                     txn={txn} key={key} first={previous_value:?} later={value:?}"
                ));
            }
        }
        previous_reads.insert((*txn, *key), (*value, *end));
    }
    Ok(())
}

// ============================================================================
// Workload runner
// ============================================================================

fn run_concurrent_workload(
    seed: u64,
    num_threads: usize,
    num_keys: usize,
    ops_per_thread: usize,
    isolation: Option<IsolationLevel>,
) -> Vec<Op> {
    let use_explicit_txn = isolation.is_some();
    let db = Database::open_in_memory().expect("Failed to create database");

    // Setup: create table with initial values
    db.execute(
        "CREATE TABLE hist_test (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("Failed to create table");
    for i in 0..num_keys {
        db.execute(
            &format!(
                "INSERT INTO hist_test VALUES ({}, {})",
                i,
                i // Initial value = key id
            ),
            (),
        )
        .expect("Failed to insert initial data");
    }

    let history = new_history();
    let barrier = Arc::new(Barrier::new(num_threads));
    let txn_sequence = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let history = Arc::clone(&history);
            let barrier = Arc::clone(&barrier);
            let txn_sequence = Arc::clone(&txn_sequence);

            thread::spawn(move || {
                barrier.wait(); // Synchronize start

                let mut local_seed = seed.wrapping_mul(thread_id as u64 + 1).wrapping_add(7);

                for _ in 0..ops_per_thread {
                    local_seed = local_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let txn = txn_sequence.fetch_add(1, Ordering::Relaxed);

                    if use_explicit_txn {
                        // BEGIN transaction
                        let start = tick();
                        let begin = match isolation.expect("explicit isolation") {
                            IsolationLevel::ReadCommitted => {
                                db.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", ())
                            }
                            IsolationLevel::SnapshotIsolation => {
                                db.execute("BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT", ())
                            }
                        };
                        let end = tick();
                        if let Err(error) = begin {
                            record(
                                &history,
                                Op::BeginFailed {
                                    thread: thread_id,
                                    txn,
                                    start,
                                    end,
                                    error: error.to_string(),
                                },
                            );
                            continue;
                        }
                        record(
                            &history,
                            Op::Begin {
                                thread: thread_id,
                                txn,
                                start,
                                end,
                            },
                        );
                    }

                    let key = (local_seed % num_keys as u64) as i64;

                    // Decide: read or write
                    let do_write = (local_seed >> 16).is_multiple_of(3); // ~33% writes

                    if do_write {
                        // Globally unique provenance: a read value identifies
                        // the exact transaction that could have produced it.
                        let new_value = 1_000_000 + txn as i64;
                        let start = tick();
                        let result = db.execute(
                            &format!(
                                "UPDATE hist_test SET value = {} WHERE id = {}",
                                new_value, key
                            ),
                            (),
                        );
                        let end = tick();
                        if result.is_ok() {
                            record(
                                &history,
                                Op::Write {
                                    thread: thread_id,
                                    txn,
                                    key,
                                    value: new_value,
                                    start,
                                    end,
                                },
                            );
                        }
                    } else {
                        // Two reads make snapshot consistency observable rather
                        // than vacuously true for one-statement transactions.
                        let read_count = if use_explicit_txn { 2 } else { 1 };
                        for _ in 0..read_count {
                            let start = tick();
                            let result: Result<Option<i64>, _> = db.query_one(
                                &format!("SELECT value FROM hist_test WHERE id = {}", key),
                                (),
                            );
                            let end = tick();
                            if let Ok(value) = result {
                                record(
                                    &history,
                                    Op::Read {
                                        thread: thread_id,
                                        txn,
                                        key,
                                        value,
                                        start,
                                        end,
                                    },
                                );
                            }
                            thread::yield_now();
                        }
                    }

                    if use_explicit_txn {
                        // Decide: commit or rollback
                        let do_rollback = (local_seed >> 24).is_multiple_of(5); // 20% rollbacks

                        if do_rollback {
                            let start = tick();
                            let _ = db.execute("ROLLBACK", ());
                            let end = tick();
                            record(
                                &history,
                                Op::Rollback {
                                    thread: thread_id,
                                    txn,
                                    start,
                                    end,
                                },
                            );
                        } else {
                            let start = tick();
                            let commit = db.execute("COMMIT", ());
                            let end = tick();
                            match commit {
                                Ok(_) => {
                                    record(
                                        &history,
                                        Op::Commit {
                                            thread: thread_id,
                                            txn,
                                            start,
                                            end,
                                        },
                                    );
                                }
                                Err(e) => {
                                    let _ = db.execute("ROLLBACK", ());
                                    record(
                                        &history,
                                        Op::CommitFailed {
                                            thread: thread_id,
                                            txn,
                                            start,
                                            end,
                                            error: e.to_string(),
                                        },
                                    );
                                }
                            }
                        }
                    }

                    // Small random delay to increase interleaving
                    if local_seed.is_multiple_of(7) {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let history = Arc::try_unwrap(history).unwrap().into_inner().unwrap();
    let begin_attempts = history
        .iter()
        .filter(|op| matches!(op, Op::Begin { .. } | Op::BeginFailed { .. }))
        .count();
    let completed_operations = history
        .iter()
        .filter(|op| matches!(op, Op::Read { .. } | Op::Write { .. }))
        .count();
    if use_explicit_txn {
        assert_eq!(
            begin_attempts,
            num_threads * ops_per_thread,
            "every declared transaction attempt must be represented in history"
        );
    }
    assert!(
        completed_operations >= num_threads * ops_per_thread / 2,
        "history executed only {completed_operations} operations out of {} attempts",
        num_threads * ops_per_thread
    );
    history
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn history_validator_rejects_a_value_committed_only_after_the_read() {
    let history = vec![
        Op::Begin {
            thread: 0,
            txn: 1,
            start: 1,
            end: 2,
        },
        Op::Write {
            thread: 0,
            txn: 1,
            key: 1,
            value: 1_000_001,
            start: 3,
            end: 4,
        },
        Op::Begin {
            thread: 1,
            txn: 2,
            start: 5,
            end: 6,
        },
        Op::Read {
            thread: 1,
            txn: 2,
            key: 1,
            value: Some(1_000_001),
            start: 7,
            end: 8,
        },
        Op::Commit {
            thread: 0,
            txn: 1,
            start: 9,
            end: 10,
        },
    ];

    assert!(validate_read_committed(&history).is_err());
}

#[test]
fn history_validator_accepts_committed_provenance_and_own_snapshot_writes() {
    let committed = vec![
        Op::Write {
            thread: 0,
            txn: 1,
            key: 1,
            value: 1_000_001,
            start: 1,
            end: 2,
        },
        Op::Commit {
            thread: 0,
            txn: 1,
            start: 3,
            end: 4,
        },
        Op::Read {
            thread: 1,
            txn: 2,
            key: 1,
            value: Some(1_000_001),
            start: 5,
            end: 6,
        },
    ];
    validate_read_committed(&committed).expect("committed writer is valid provenance");

    let own_write = vec![
        Op::Read {
            thread: 0,
            txn: 7,
            key: 1,
            value: Some(1),
            start: 1,
            end: 2,
        },
        Op::Write {
            thread: 0,
            txn: 7,
            key: 1,
            value: 1_000_007,
            start: 3,
            end: 4,
        },
        Op::Read {
            thread: 0,
            txn: 7,
            key: 1,
            value: Some(1_000_007),
            start: 5,
            end: 6,
        },
    ];
    validate_snapshot_consistency(&own_write)
        .expect("an intervening own write may change a snapshot read");

    let impossible = vec![own_write[0].clone(), own_write[2].clone()];
    assert!(validate_snapshot_consistency(&impossible).is_err());
}

#[test]
fn r8_l01_batch_a_read_committed_history_has_declared_work() {
    // Run 10 iterations with different seeds
    for seed in 0..10 {
        let history = run_concurrent_workload(
            seed * 12345 + 67890,
            4,  // 4 threads
            10, // 10 keys
            50, // 50 ops per thread
            Some(IsolationLevel::ReadCommitted),
        );

        // Validate: no dirty reads
        validate_read_committed(&history).expect("read-committed history must be causal");
    }
}

#[test]
fn r8_l01_batch_a_contended_history_has_declared_work() {
    // Higher contention with 8 threads
    for seed in 0..5 {
        let history = run_concurrent_workload(
            seed * 99991 + 42,
            8,  // 8 threads
            5,  // 5 keys (high contention)
            30, // 30 ops per thread
            Some(IsolationLevel::ReadCommitted),
        );

        validate_read_committed(&history).expect("contended read-committed history must be causal");
    }
}

#[test]
fn r8_l01_batch_a_snapshot_history_has_declared_work() {
    // Use snapshot isolation (the default for explicit transactions in radixdb)
    for seed in 0..10 {
        let history = run_concurrent_workload(
            seed * 54321 + 11111,
            4,  // 4 threads
            10, // 10 keys
            40, // 40 ops per thread
            Some(IsolationLevel::SnapshotIsolation),
        );

        // Validate: consistent snapshot reads
        validate_snapshot_consistency(&history).expect("snapshot history must remain stable");
    }
}

#[test]
fn r3_l01_batch_c_history_matches_declared_isolation_boundaries() {
    let db = Database::open("memory://r3_l01_batch_c_history").expect("open history database");
    db.execute(
        "CREATE TABLE hist_boundary (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("create history table");
    db.execute("INSERT INTO hist_boundary VALUES (1, 10), (2, 20)", ())
        .expect("seed history rows");

    let mut snapshot = db
        .begin_with_isolation(IsolationLevel::SnapshotIsolation)
        .expect("begin snapshot reader");
    let mut read_committed = db
        .begin_with_isolation(IsolationLevel::ReadCommitted)
        .expect("begin read-committed reader");

    let snapshot_before: i64 = snapshot
        .query_one("SELECT SUM(value) FROM hist_boundary", ())
        .expect("snapshot pre-commit read");
    let rc_before: i64 = read_committed
        .query_one("SELECT SUM(value) FROM hist_boundary", ())
        .expect("read-committed pre-commit read");
    assert_eq!((snapshot_before, rc_before), (30, 30));

    let mut writer = db.begin().expect("begin overlapping writer");
    writer
        .execute("UPDATE hist_boundary SET value = value + 100", ())
        .expect("stage one atomic epoch");
    writer.commit().expect("publish one atomic epoch");

    let snapshot_after: i64 = snapshot
        .query_one("SELECT SUM(value) FROM hist_boundary", ())
        .expect("snapshot post-commit read");
    let rc_after: i64 = read_committed
        .query_one("SELECT SUM(value) FROM hist_boundary", ())
        .expect("read-committed post-commit read");

    assert_eq!(
        snapshot_after, 30,
        "snapshot reader crossed the writer commit boundary"
    );
    assert_eq!(
        rc_after, 230,
        "read-committed reader did not advance to the committed epoch"
    );

    snapshot.rollback().expect("end snapshot reader");
    read_committed
        .rollback()
        .expect("end read-committed reader");
}

#[test]
fn test_autocommit_no_dirty_reads() {
    // Without explicit transactions, each statement auto-commits
    for seed in 0..10 {
        let history = run_concurrent_workload(
            seed * 77777 + 33333,
            4,    // 4 threads
            10,   // 10 keys
            100,  // 100 ops per thread
            None, // autocommit mode
        );

        // In autocommit mode, every read should see committed state
        // Check: no reads see values that were never committed
        let mut all_committed_values: std::collections::HashSet<(i64, i64)> =
            std::collections::HashSet::new();

        // Initial values are committed
        for i in 0..10i64 {
            all_committed_values.insert((i, i));
        }

        // All writes in autocommit are committed
        for op in &history {
            if let Op::Write { key, value, .. } = op {
                all_committed_values.insert((*key, *value));
            }
        }

        // Check reads
        for op in &history {
            if let Op::Read {
                thread,
                key,
                value: Some(value),
                end,
                ..
            } = op
            {
                assert!(
                    all_committed_values.contains(&(*key, *value)),
                    "Thread {} at time {} read key={} value={} which was never committed",
                    thread,
                    end,
                    key,
                    value
                );
            }
        }
    }
}

#[test]
fn test_concurrent_insert_delete_consistency() {
    // Test that concurrent inserts and deletes maintain consistency
    let db = Database::open_in_memory().expect("Failed to create database");
    db.execute(
        "CREATE TABLE cd_test (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("Failed to create table");

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                // Each thread operates on its own key range to avoid conflicts
                let base = thread_id * 100;
                for i in 0..50 {
                    let id = base + i;
                    // Insert
                    let _ = db.execute(&format!("INSERT INTO cd_test VALUES ({}, {})", id, i), ());
                    // Delete even ids
                    if i % 2 == 0 {
                        let _ = db.execute(&format!("DELETE FROM cd_test WHERE id = {}", id), ());
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify: each thread should have 25 remaining rows (odd ids)
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM cd_test", ())
        .expect("COUNT should work");
    assert_eq!(count, 100, "4 threads x 25 remaining rows = 100");

    // Verify no duplicate IDs
    let dup_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM (SELECT id FROM cd_test GROUP BY id HAVING COUNT(*) > 1) AS d",
            (),
        )
        .unwrap_or(0);
    assert_eq!(dup_count, 0, "No duplicate primary keys");
}

#[test]
fn r8_l01_batch_a_write_conflict_requires_progress() {
    // Two transactions both try to update the same row.
    // RadixDB detects write-write conflicts eagerly at UPDATE time
    // (if another txn already has uncommitted changes on the row).
    // At least one transaction's full cycle (BEGIN+UPDATE+COMMIT) should succeed.
    let db = Database::open_in_memory().expect("Failed to create database");
    db.execute(
        "CREATE TABLE ww_test (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("Failed to create table");
    db.execute("INSERT INTO ww_test VALUES (1, 0)", ())
        .expect("Failed to insert");

    let mut success_count = 0;
    let mut conflict_count = 0;

    for _ in 0..20 {
        let db1 = db.clone();
        let db2 = db.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let h1 = thread::spawn(move || -> bool {
            if db1.execute("BEGIN", ()).is_err() {
                return false;
            }
            b1.wait(); // Synchronize: both transactions active
            if db1
                .execute("UPDATE ww_test SET value = 1 WHERE id = 1", ())
                .is_err()
            {
                let _ = db1.execute("ROLLBACK", ());
                return false;
            }
            if db1.execute("COMMIT", ()).is_err() {
                let _ = db1.execute("ROLLBACK", ());
                return false;
            }
            true
        });

        let h2 = thread::spawn(move || -> bool {
            if db2.execute("BEGIN", ()).is_err() {
                return false;
            }
            b2.wait(); // Synchronize: both transactions active
            if db2
                .execute("UPDATE ww_test SET value = 2 WHERE id = 1", ())
                .is_err()
            {
                let _ = db2.execute("ROLLBACK", ());
                return false;
            }
            if db2.execute("COMMIT", ()).is_err() {
                let _ = db2.execute("ROLLBACK", ());
                return false;
            }
            true
        });

        let r1 = h1.join().expect("Thread 1 panicked");
        let r2 = h2.join().expect("Thread 2 panicked");

        let successful_transactions = usize::from(r1) + usize::from(r2);
        success_count += successful_transactions;
        conflict_count += 2 - successful_transactions;

        // Reset for next iteration
        db.execute("UPDATE ww_test SET value = 0 WHERE id = 1", ())
            .expect("reset contested row for the next iteration");
    }

    // At least some iterations should succeed
    assert!(
        success_count >= 20,
        "at least one transaction per iteration must commit; successes={success_count}, conflicts={conflict_count}"
    );
    eprintln!(
        "Write-write conflicts: {} detected conflicts, {} both-succeed out of 20",
        conflict_count, success_count
    );
}
