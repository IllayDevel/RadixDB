// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0.

#![cfg(all(feature = "stress-tests", feature = "test-failpoints"))]

mod common;

use std::{
    fs,
    net::SocketAddr,
    process::Command,
    sync::{atomic::AtomicBool, mpsc, Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use common::prerelease::{
    tcp_command, tcp_connect, tcp_rows, tcp_scalar_i64, tcp_server_config, with_tcp_server,
    DeterministicScheduler, OwnedChildProcess, OwnedFixture, RawProtocolClient,
};
use radixdb::{
    server::Server,
    test_failpoints::{InterleaveGuard, InterleavePoint},
};
use radixdb_client::{
    Connection, ExecuteResult, PreparedStatement, Row, TransactionIsolation, WireValue,
};

const DATABASE: &str = "prerelease_b5";
const CHILD_DATA_DIR: &str = "RADIXDB_PRERELEASE_B5_CHILD_DATA_DIR";
const CHILD_ADDRESS_FILE: &str = "RADIXDB_PRERELEASE_B5_CHILD_ADDRESS_FILE";

fn execute_prepared_rows(
    connection: &mut Connection,
    statement: &PreparedStatement,
) -> Result<Vec<Row>, String> {
    let ExecuteResult::Cursor(cursor) = connection
        .execute_prepared(statement, Vec::new())
        .map_err(|error| error.to_string())?
    else {
        return Err("prepared SELECT returned command completion".to_string());
    };
    let mut rows = Vec::new();
    loop {
        let batch = connection
            .fetch(&cursor)
            .map_err(|error| error.to_string())?;
        rows.extend(batch.rows);
        if batch.eof {
            return Ok(rows);
        }
    }
}

fn int(value: &WireValue) -> i64 {
    match value {
        WireValue::Int(value) => *value,
        other => panic!("expected INTEGER, got {other:?}"),
    }
}

#[test]
fn b5_named_storage_boundaries_and_catalog_generation_are_deterministic() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-boundaries-").expect("create B5 boundary fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 16, |address| {
        let mut owner = tcp_connect(address, DATABASE).unwrap();

        let guard = InterleaveGuard::install([
            InterleavePoint::IndexPrepared,
            InterleavePoint::ConstraintsValidated,
            InterleavePoint::IndexPublished,
            InterleavePoint::WalBeforeCommitMarker,
            InterleavePoint::WalCommitMarkerDurable,
            InterleavePoint::VisibilityBeforePublish,
            InterleavePoint::VisibilityPublished,
        ]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let (commit_tx, commit_rx) = mpsc::channel();
        let commit_worker = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                connection.begin().map_err(|error| error.to_string())?;
                tcp_command(
                    &mut connection,
                    "CREATE TABLE generation_rows (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE)",
                )?;
                tcp_command(
                    &mut connection,
                    "INSERT INTO generation_rows VALUES (1, 'whole-generation')",
                )?;
                connection.commit().map_err(|error| error.to_string())
            })();
            commit_tx.send(result).unwrap();
        });

        scheduler.step(InterleavePoint::IndexPrepared, None);
        scheduler.step(InterleavePoint::ConstraintsValidated, None);
        scheduler.step(InterleavePoint::IndexPublished, None);
        scheduler.step(InterleavePoint::WalBeforeCommitMarker, None);
        scheduler.step(InterleavePoint::WalCommitMarkerDurable, None);
        let before_publish = scheduler.wait(InterleavePoint::VisibilityBeforePublish, None);

        let (observer_tx, observer_rx) = mpsc::channel();
        let observer = thread::spawn(move || {
            let result = (|| -> Result<(i64, usize), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                let count =
                    tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM generation_rows")?;
                let columns = tcp_rows(&mut connection, "DESCRIBE generation_rows")?.len();
                Ok((count, columns))
            })();
            observer_tx.send(result).unwrap();
        });
        assert!(
            observer_rx
                .recv_timeout(Duration::from_millis(150))
                .is_err(),
            "catalog reader crossed a half-published DDL generation"
        );
        scheduler.release(before_publish);
        scheduler.step(InterleavePoint::VisibilityPublished, None);
        drop(guard);

        assert_eq!(
            commit_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        commit_worker.join().unwrap();
        let (count, columns) = observer_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        observer.join().unwrap();
        assert_eq!(count, 1);
        assert!(columns >= 2);
        assert_eq!(
            tcp_scalar_i64(
                &mut owner,
                "SELECT COUNT(*) FROM generation_rows WHERE code = 'whole-generation'",
            )
            .unwrap(),
            1
        );
        assert_eq!(scheduler.trace().len(), 7);

        let checkpoint_guard = InterleaveGuard::install([
            InterleavePoint::SealBeforePublish,
            InterleavePoint::CheckpointBeforePublish,
            InterleavePoint::ManifestBeforePublish,
        ]);
        let mut checkpoint_scheduler = DeterministicScheduler::new(checkpoint_guard.controller());
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel();
        let checkpoint_worker = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                tcp_command(&mut connection, "PRAGMA CHECKPOINT")
            })();
            checkpoint_tx.send(result).unwrap();
        });
        checkpoint_scheduler.step(InterleavePoint::SealBeforePublish, None);
        checkpoint_scheduler.step(InterleavePoint::CheckpointBeforePublish, None);
        checkpoint_scheduler.step(InterleavePoint::ManifestBeforePublish, None);
        drop(checkpoint_guard);
        assert_eq!(
            checkpoint_rx.recv_timeout(Duration::from_secs(15)).unwrap(),
            Ok(())
        );
        checkpoint_worker.join().unwrap();
    });
}

#[test]
fn b5_commit_preflight_keeps_committed_reads_available() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-preflight-").expect("create B5 preflight fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 8, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE preflight_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(&mut setup, "INSERT INTO preflight_rows VALUES (1, 10)").unwrap();

        let guard = InterleaveGuard::install([InterleavePoint::ConstraintsValidated]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let (commit_tx, commit_rx) = mpsc::channel();
        let commit_worker = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut writer = tcp_connect(address, DATABASE)?;
                writer.begin().map_err(|error| error.to_string())?;
                tcp_command(
                    &mut writer,
                    "UPDATE preflight_rows SET value = 20 WHERE id = 1",
                )?;
                writer.commit().map_err(|error| error.to_string())
            })();
            commit_tx.send(result).unwrap();
        });

        let paused = scheduler.wait(InterleavePoint::ConstraintsValidated, None);
        let mut observer = tcp_connect(address, DATABASE).unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut observer,
                "SELECT value FROM preflight_rows WHERE id = 1",
            )
            .expect("committed read must not wait for writer WAL preflight"),
            10
        );
        scheduler.release(paused);
        drop(guard);

        assert_eq!(
            commit_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        commit_worker.join().unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut observer,
                "SELECT value FROM preflight_rows WHERE id = 1",
            )
            .unwrap(),
            20
        );
    });
}

#[test]
fn b5_snapshot_keeps_precommit_cold_row_across_seal_skip() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-seal-skip-").expect("create B5 seal-skip fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 8, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE seal_skip_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(&mut setup, "INSERT INTO seal_skip_rows VALUES (1, 10)").unwrap();
        tcp_command(&mut setup, "PRAGMA CHECKPOINT").unwrap();
        tcp_command(
            &mut setup,
            "UPDATE seal_skip_rows SET value = 15 WHERE id = 1",
        )
        .unwrap();

        let guard = InterleaveGuard::install([
            InterleavePoint::SealBeforePublish,
            InterleavePoint::VisibilityDmlPublished,
        ]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());

        let (checkpoint_tx, checkpoint_rx) = mpsc::channel();
        let checkpoint = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                tcp_command(&mut connection, "PRAGMA CHECKPOINT")
            })();
            checkpoint_tx.send(result).unwrap();
        });
        let seal_paused = scheduler.wait(InterleavePoint::SealBeforePublish, None);

        let mut writer = tcp_connect(address, DATABASE).unwrap();
        writer.begin().unwrap();
        tcp_command(
            &mut writer,
            "UPDATE seal_skip_rows SET value = 20 WHERE id = 1",
        )
        .unwrap();
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            writer_tx
                .send(writer.commit().map_err(|error| error.to_string()))
                .unwrap();
        });
        let writer_paused = scheduler.wait(InterleavePoint::VisibilityDmlPublished, None);

        let mut reader = tcp_connect(address, DATABASE).unwrap();
        reader
            .begin_with_isolation(TransactionIsolation::Snapshot)
            .unwrap();

        scheduler.release(seal_paused);
        scheduler.release(writer_paused);
        assert_eq!(
            writer_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        writer.join().unwrap();
        let checkpoint_error = checkpoint_rx
            .recv_timeout(Duration::from_secs(15))
            .unwrap()
            .expect_err("racing hot update must defer the forced checkpoint");
        assert!(
            checkpoint_error.contains("committed hot rows unsealed"),
            "unexpected checkpoint result: {checkpoint_error}"
        );
        checkpoint.join().unwrap();

        assert_eq!(
            tcp_scalar_i64(&mut reader, "SELECT value FROM seal_skip_rows WHERE id = 1",).unwrap(),
            15
        );
        reader.rollback().unwrap();
        drop(guard);

        // A deferred force checkpoint has already published its extracted hot
        // rows as cold volumes. Repeating the same race must compact those
        // partial seals instead of accumulating one new volume per retry.
        for next_value in 21..=24 {
            let guard = InterleaveGuard::install([
                InterleavePoint::SealBeforePublish,
                InterleavePoint::VisibilityDmlPublished,
            ]);
            let mut scheduler = DeterministicScheduler::new(guard.controller());
            let (checkpoint_tx, checkpoint_rx) = mpsc::channel();
            let checkpoint = thread::spawn(move || {
                let result = (|| -> Result<(), String> {
                    let mut connection = tcp_connect(address, DATABASE)?;
                    tcp_command(&mut connection, "PRAGMA CHECKPOINT")
                })();
                checkpoint_tx.send(result).unwrap();
            });
            let seal_paused = scheduler.wait(InterleavePoint::SealBeforePublish, None);

            let mut writer = tcp_connect(address, DATABASE).unwrap();
            writer.begin().unwrap();
            tcp_command(
                &mut writer,
                format!("UPDATE seal_skip_rows SET value = {next_value} WHERE id = 1"),
            )
            .unwrap();
            let (writer_tx, writer_rx) = mpsc::channel();
            let writer = thread::spawn(move || {
                writer_tx
                    .send(writer.commit().map_err(|error| error.to_string()))
                    .unwrap();
            });
            let writer_paused = scheduler.wait(InterleavePoint::VisibilityDmlPublished, None);

            scheduler.release(seal_paused);
            scheduler.release(writer_paused);
            assert_eq!(
                writer_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
                Ok(())
            );
            writer.join().unwrap();
            let checkpoint_error = checkpoint_rx
                .recv_timeout(Duration::from_secs(15))
                .unwrap()
                .expect_err("racing hot update must defer the forced checkpoint");
            assert!(checkpoint_error.contains("committed hot rows unsealed"));
            checkpoint.join().unwrap();
            drop(guard);
        }

        let volume_count = tcp_rows(&mut setup, "PRAGMA VOLUME_STATS")
            .unwrap()
            .into_iter()
            .filter(|row| {
                matches!(
                    row.values.first(),
                    Some(WireValue::String(table)) if table == "seal_skip_rows"
                )
            })
            .count();
        assert!(
            volume_count <= 2,
            "deferred checkpoint retries fragmented one logical row across {volume_count} volumes"
        );
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT value FROM seal_skip_rows WHERE id = 1").unwrap(),
            24
        );
    });
}

#[test]
fn b5_snapshot_keeps_cold_delete_and_hot_update_on_one_commit_epoch() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b5-cold-delete-")
        .expect("create B5 cold-delete fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 8, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE cold_delete_accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE cold_delete_postings (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, amount INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "INSERT INTO cold_delete_accounts VALUES (1, 12)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "INSERT INTO cold_delete_postings VALUES (1, 1, 12)",
        )
        .unwrap();
        tcp_command(&mut setup, "PRAGMA CHECKPOINT").unwrap();

        let guard = InterleaveGuard::install([InterleavePoint::VisibilityDmlPublished]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let mut writer = tcp_connect(address, DATABASE).unwrap();
        writer.begin().unwrap();
        tcp_command(&mut writer, "DELETE FROM cold_delete_postings WHERE id = 1").unwrap();
        tcp_command(
            &mut writer,
            "UPDATE cold_delete_accounts SET balance = 0 WHERE id = 1",
        )
        .unwrap();
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            writer_tx
                .send(writer.commit().map_err(|error| error.to_string()))
                .unwrap();
        });
        let writer_paused = scheduler.wait(InterleavePoint::VisibilityDmlPublished, None);

        let mut reader = tcp_connect(address, DATABASE).unwrap();
        reader
            .begin_with_isolation(TransactionIsolation::Snapshot)
            .unwrap();
        scheduler.release(writer_paused);
        assert_eq!(
            writer_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        writer.join().unwrap();

        assert_eq!(
            tcp_scalar_i64(
                &mut reader,
                "SELECT COALESCE(SUM(balance), 0) FROM cold_delete_accounts",
            )
            .unwrap(),
            12
        );
        assert_eq!(
            tcp_scalar_i64(
                &mut reader,
                "SELECT COALESCE(SUM(amount), 0) FROM cold_delete_postings",
            )
            .unwrap(),
            12
        );
        reader.rollback().unwrap();
        drop(guard);
    });
}

#[test]
fn b5_statement_unique_claim_rejects_concurrent_contender() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b5-unique-certification-")
        .expect("create B5 unique certification fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 8, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE unique_rows (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE)",
        )
        .unwrap();

        let start = Arc::new(Barrier::new(3));
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for id in [1_i64, 2] {
            let start = Arc::clone(&start);
            let attempt_tx = attempt_tx.clone();
            let (release_tx, release_rx) = mpsc::channel();
            let worker = thread::spawn(move || -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                connection.begin().map_err(|error| error.to_string())?;
                start.wait();
                let inserted = tcp_command(
                    &mut connection,
                    format!("INSERT INTO unique_rows VALUES ({id}, 'same-key')"),
                );
                attempt_tx.send((id, inserted.clone())).unwrap();
                if let Err(error) = inserted {
                    if connection.in_transaction() {
                        connection.rollback().map_err(|error| error.to_string())?;
                    }
                    return Err(error);
                }
                release_rx
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|error| error.to_string())?;
                let committed = connection.commit().map_err(|error| error.to_string());
                if committed.is_err() && connection.in_transaction() {
                    connection.rollback().map_err(|error| error.to_string())?;
                }
                committed
            });
            workers.push((worker, release_tx));
        }
        start.wait();

        let attempts = (0..2)
            .map(|_| attempt_rx.recv_timeout(Duration::from_secs(10)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            attempts.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        assert_eq!(
            attempts
                .iter()
                .filter(|(_, result)| result
                    .as_ref()
                    .is_err_and(|error| error.to_ascii_lowercase().contains("unique")))
                .count(),
            1
        );
        for (_, release) in &workers {
            let _ = release.send(());
        }

        let results: Vec<_> = workers
            .into_iter()
            .map(|(worker, _)| worker.join().expect("unique contender joins"))
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| result
                    .as_ref()
                    .is_err_and(|error| error.to_ascii_lowercase().contains("unique")))
                .count(),
            1
        );
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM unique_rows").unwrap(),
            1
        );
    });
}

#[test]
fn b5_ddl_preflight_and_select_share_catalog_then_visibility_lock_order() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-ddl-select-order-").expect("create B5 fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 8, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE stable_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(&mut setup, "INSERT INTO stable_rows VALUES (1, 10)").unwrap();

        let guard = InterleaveGuard::install([InterleavePoint::ConstraintsValidated]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let (commit_tx, commit_rx) = mpsc::channel();
        let commit_worker = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut writer = tcp_connect(address, DATABASE)?;
                writer.begin().map_err(|error| error.to_string())?;
                tcp_command(
                    &mut writer,
                    "CREATE TABLE published_rows (id INTEGER PRIMARY KEY)",
                )?;
                writer.commit().map_err(|error| error.to_string())
            })();
            commit_tx.send(result).unwrap();
        });

        // The DDL transaction owns catalog-exclusive here, but has not yet
        // acquired visibility-exclusive. A SELECT must wait on catalog before
        // it acquires visibility; the reverse order deadlocks after release.
        let paused = scheduler.wait(InterleavePoint::ConstraintsValidated, None);
        let (reader_tx, reader_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let result = (|| -> Result<i64, String> {
                let mut observer = tcp_connect(address, DATABASE)?;
                tcp_scalar_i64(&mut observer, "SELECT value FROM stable_rows WHERE id = 1")
            })();
            reader_tx.send(result).unwrap();
        });
        assert!(
            reader_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "SELECT crossed a DDL generation still in preflight"
        );

        scheduler.release(paused);
        drop(guard);
        assert_eq!(
            commit_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        assert_eq!(
            reader_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(10)
        );
        commit_worker.join().unwrap();
        reader.join().unwrap();
    });
}

#[test]
fn b5_interleaving_corpus_preserves_update_unique_fk_and_message_outbox_contracts() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-corpus-").expect("create B5 corpus fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 24, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        for sql in [
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, balance INTEGER NOT NULL)",
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id))",
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            "CREATE TABLE outbox_jobs (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id), state TEXT NOT NULL)",
            "INSERT INTO accounts VALUES (1, 'owner@example.test', 0)",
            "INSERT INTO parents VALUES (1, 'existing')",
        ] {
            tcp_command(&mut setup, sql).unwrap();
        }

        // Same-row contenders cannot both publish an update.
        let mut first = tcp_connect(address, DATABASE).unwrap();
        first.begin().unwrap();
        tcp_command(&mut first, "UPDATE accounts SET balance = 10 WHERE id = 1").unwrap();
        let mut contenders = Vec::new();
        for balance in [20i64, 30] {
            let (result_tx, result_rx) = mpsc::channel();
            let worker = thread::spawn(move || {
                let result = (|| -> Result<(), String> {
                    let mut contender = tcp_connect(address, DATABASE)?;
                    contender.begin().map_err(|error| error.to_string())?;
                    tcp_command(
                        &mut contender,
                        format!("UPDATE accounts SET balance = {balance} WHERE id = 1"),
                    )?;
                    contender.commit().map_err(|error| error.to_string())
                })();
                result_tx.send(result).unwrap();
            });
            contenders.push((result_rx, worker));
        }
        for (result_rx, _) in &contenders {
            assert!(
                result_rx.recv_timeout(Duration::from_millis(150)).is_err(),
                "same-row contender bypassed the owning transaction"
            );
        }
        first.commit().unwrap();
        for (result_rx, worker) in contenders {
            let result = result_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            worker.join().unwrap();
            assert!(result.is_ok() || result.unwrap_err().contains("conflict"));
        }
        let final_balance =
            tcp_scalar_i64(&mut setup, "SELECT balance FROM accounts WHERE id = 1").unwrap();
        assert!(matches!(final_balance, 10 | 20 | 30));

        // UNIQUE reuse has one owner, and an FK child cannot anticipate an
        // uncommitted parent from another transaction.
        let mut reuse = tcp_connect(address, DATABASE).unwrap();
        reuse.begin().unwrap();
        tcp_command(&mut reuse, "DELETE FROM accounts WHERE id = 1").unwrap();
        tcp_command(
            &mut reuse,
            "INSERT INTO accounts VALUES (2, 'owner@example.test', 30)",
        )
        .unwrap();
        reuse.commit().unwrap();
        assert!(tcp_command(
            &mut setup,
            "INSERT INTO accounts VALUES (3, 'owner@example.test', 40)",
        )
        .is_err());

        let mut parent_writer = tcp_connect(address, DATABASE).unwrap();
        parent_writer.begin().unwrap();
        tcp_command(
            &mut parent_writer,
            "INSERT INTO parents VALUES (2, 'pending')",
        )
        .unwrap();
        assert!(tcp_command(&mut setup, "INSERT INTO children VALUES (2, 2)").is_err());
        parent_writer.commit().unwrap();
        tcp_command(&mut setup, "INSERT INTO children VALUES (2, 2)").unwrap();

        // A reader starting inside the commit publication interval blocks and
        // then sees both related rows, never a message without its outbox row.
        let guard = InterleaveGuard::install([InterleavePoint::VisibilityBeforePublish]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                connection.begin().map_err(|error| error.to_string())?;
                tcp_command(
                    &mut connection,
                    "INSERT INTO messages VALUES (10, 'atomic')",
                )?;
                tcp_command(
                    &mut connection,
                    "INSERT INTO outbox_jobs VALUES (10, 10, 'pending')",
                )?;
                connection.commit().map_err(|error| error.to_string())
            })();
            writer_tx.send(result).unwrap();
        });
        let paused = scheduler.wait(InterleavePoint::VisibilityBeforePublish, None);
        let (reader_tx, reader_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let result = (|| -> Result<(i64, i64), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                Ok((
                    tcp_scalar_i64(
                        &mut connection,
                        "SELECT COUNT(*) FROM messages WHERE id = 10",
                    )?,
                    tcp_scalar_i64(
                        &mut connection,
                        "SELECT COUNT(*) FROM outbox_jobs WHERE message_id = 10",
                    )?,
                ))
            })();
            reader_tx.send(result).unwrap();
        });
        assert!(reader_rx.recv_timeout(Duration::from_millis(150)).is_err());
        scheduler.release(paused);
        drop(guard);
        assert_eq!(
            writer_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        assert_eq!(
            reader_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok((1, 1))
        );
        writer.join().unwrap();
        reader.join().unwrap();
    });
}

fn run_lost_ack_case(address: SocketAddr, id: i64, point: InterleavePoint) {
    let guard = InterleaveGuard::install([point]);
    let mut scheduler = DeterministicScheduler::new(guard.controller());
    let mut raw = RawProtocolClient::connect(address, DATABASE).unwrap();
    raw.begin().unwrap();
    raw.execute(format!(
        "INSERT INTO idempotent_commands VALUES ({id}, 'command-{id}')"
    ))
    .unwrap();
    raw.send_commit().unwrap();
    let paused = scheduler.wait(point, None);
    raw.cut_transport().unwrap();
    scheduler.release(paused);
    drop(raw);
    drop(guard);

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut verifier = tcp_connect(address, DATABASE).unwrap();
        if tcp_scalar_i64(
            &mut verifier,
            &format!("SELECT COUNT(*) FROM idempotent_commands WHERE id = {id}"),
        )
        .unwrap()
            == 1
        {
            assert!(tcp_command(
                &mut verifier,
                format!("INSERT INTO idempotent_commands VALUES ({id}, 'command-{id}')"),
            )
            .is_err());
            assert_eq!(
                tcp_scalar_i64(
                    &mut verifier,
                    &format!("SELECT COUNT(*) FROM idempotent_commands WHERE id = {id}"),
                )
                .unwrap(),
                1
            );
            break;
        }
        assert!(Instant::now() < deadline, "lost-ACK commit did not resolve");
        thread::yield_now();
    }
}

#[test]
fn b5_lost_commit_ack_before_and_after_durable_boundary_has_one_effect() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-lost-ack-").expect("create lost ACK fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 16, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE idempotent_commands (id INTEGER PRIMARY KEY, command_key TEXT NOT NULL UNIQUE)",
        )
        .unwrap();
        run_lost_ack_case(address, 1, InterleavePoint::WalBeforeCommitMarker);
        run_lost_ack_case(address, 2, InterleavePoint::CommitAckReady);
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM idempotent_commands").unwrap(),
            2
        );
    });
}

#[test]
fn b5_view_churn_invalidates_concurrent_prepared_statement_as_one_generation() {
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-view-").expect("create view churn fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 16, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE view_source (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "INSERT INTO view_source VALUES (1, 10), (2, 20)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "CREATE VIEW churn_v AS SELECT id, value FROM view_source WHERE value >= 10",
        )
        .unwrap();
        let mut prepared_connection = tcp_connect(address, DATABASE).unwrap();
        let prepared = prepared_connection
            .prepare("SELECT id, value FROM churn_v ORDER BY id")
            .unwrap();
        assert_eq!(
            execute_prepared_rows(&mut prepared_connection, &prepared)
                .unwrap()
                .len(),
            2
        );

        let drop_guard = InterleaveGuard::install([
            InterleavePoint::CatalogBeforePublish,
            InterleavePoint::CatalogPublished,
        ]);
        let mut scheduler = DeterministicScheduler::new(drop_guard.controller());
        let (ddl_tx, ddl_rx) = mpsc::channel();
        let ddl = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                tcp_command(&mut connection, "DROP VIEW churn_v")
            })();
            ddl_tx.send(result).unwrap();
        });
        let before = scheduler.wait(InterleavePoint::CatalogBeforePublish, None);
        let (query_tx, query_rx) = mpsc::channel();
        let query = thread::spawn(move || {
            let result = execute_prepared_rows(&mut prepared_connection, &prepared);
            query_tx
                .send((prepared_connection, prepared, result))
                .unwrap();
        });
        assert!(query_rx.recv_timeout(Duration::from_millis(150)).is_err());
        scheduler.release(before);
        scheduler.step(InterleavePoint::CatalogPublished, None);
        drop(drop_guard);
        assert_eq!(
            ddl_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        ddl.join().unwrap();
        let (mut prepared_connection, prepared, missing_result) =
            query_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        query.join().unwrap();
        assert!(missing_result.is_err());

        let create_guard = InterleaveGuard::install([
            InterleavePoint::CatalogBeforePublish,
            InterleavePoint::CatalogPublished,
        ]);
        let mut scheduler = DeterministicScheduler::new(create_guard.controller());
        let (create_tx, create_rx) = mpsc::channel();
        let create = thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                let mut connection = tcp_connect(address, DATABASE)?;
                tcp_command(
                    &mut connection,
                    "CREATE VIEW churn_v AS SELECT id, value FROM view_source WHERE value >= 20",
                )
            })();
            create_tx.send(result).unwrap();
        });
        scheduler.step(InterleavePoint::CatalogBeforePublish, None);
        scheduler.step(InterleavePoint::CatalogPublished, None);
        drop(create_guard);
        assert_eq!(
            create_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Ok(())
        );
        create.join().unwrap();
        let rows = execute_prepared_rows(&mut prepared_connection, &prepared).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0].values[0]), 2);
    });
}

#[test]
fn b5_giant_transaction_and_thousands_of_short_transactions_never_publish_partial_state() {
    const GIANT_ROWS: i64 = 2_048;
    const SHORT_ROWS: i64 = 2_000;
    let fixture = OwnedFixture::new("radixdb-prerelease-b5-giant-").expect("create giant fixture");
    let data_dir = fixture.child("server-data").unwrap();
    with_tcp_server(data_dir, 24, |address| {
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE giant_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        tcp_command(
            &mut setup,
            "CREATE TABLE short_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();

        let mut giant = tcp_connect(address, DATABASE).unwrap();
        giant.begin().unwrap();
        for chunk in 0..8i64 {
            let start = chunk * (GIANT_ROWS / 8) + 1;
            let end = (chunk + 1) * (GIANT_ROWS / 8);
            let values = (start..=end)
                .map(|id| format!("({id}, {id})"))
                .collect::<Vec<_>>()
                .join(",");
            tcp_command(
                &mut giant,
                format!("INSERT INTO giant_rows VALUES {values}"),
            )
            .unwrap();
        }
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM giant_rows").unwrap(),
            0
        );

        let mut short_workers = Vec::new();
        for worker in 0..4i64 {
            short_workers.push(thread::spawn(move || {
                let mut connection = tcp_connect(address, DATABASE).unwrap();
                for ordinal in 0..(SHORT_ROWS / 4) {
                    let id = worker * (SHORT_ROWS / 4) + ordinal + 1;
                    tcp_command(
                        &mut connection,
                        format!("INSERT INTO short_rows VALUES ({id}, {id})"),
                    )
                    .unwrap();
                }
            }));
        }
        for worker in short_workers {
            worker.join().unwrap();
        }
        giant.rollback().unwrap();
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM giant_rows").unwrap(),
            0
        );
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM short_rows").unwrap(),
            SHORT_ROWS
        );

        let mut giant = tcp_connect(address, DATABASE).unwrap();
        giant.begin().unwrap();
        for chunk in 0..8i64 {
            let start = chunk * (GIANT_ROWS / 8) + 1;
            let end = (chunk + 1) * (GIANT_ROWS / 8);
            let values = (start..=end)
                .map(|id| format!("({id}, {id})"))
                .collect::<Vec<_>>()
                .join(",");
            tcp_command(
                &mut giant,
                format!("INSERT INTO giant_rows VALUES {values}"),
            )
            .unwrap();
        }
        let guard = InterleaveGuard::install([InterleavePoint::VisibilityBeforePublish]);
        let mut scheduler = DeterministicScheduler::new(guard.controller());
        let (commit_tx, commit_rx) = mpsc::channel();
        let commit = thread::spawn(move || {
            let result = giant.commit().map_err(|error| error.to_string());
            commit_tx.send(result).unwrap();
        });
        let paused = scheduler.wait(InterleavePoint::VisibilityBeforePublish, None);
        scheduler.release(paused);
        drop(guard);
        assert_eq!(
            commit_rx.recv_timeout(Duration::from_secs(15)).unwrap(),
            Ok(())
        );
        commit.join().unwrap();
        assert_eq!(
            tcp_scalar_i64(&mut setup, "SELECT COUNT(*) FROM giant_rows").unwrap(),
            GIANT_ROWS
        );
    });
}

#[test]
fn b5_giant_child_server_entrypoint() {
    let Some(data_dir) = std::env::var_os(CHILD_DATA_DIR) else {
        return;
    };
    let address_file =
        std::env::var_os(CHILD_ADDRESS_FILE).expect("B5 child receives address file");
    let config = tcp_server_config(data_dir.into(), 16);
    let server = Server::bind_ephemeral(&config).expect("bind B5 child server");
    let address = server.local_addr().expect("resolve B5 child address");
    fs::write(&address_file, address.to_string()).expect("publish B5 child address");
    let shutdown = AtomicBool::new(false);
    server
        .run_until(&shutdown)
        .expect("B5 child only exits when killed");
}

#[test]
fn b5_kill_before_durable_commit_removes_complete_giant_transaction() {
    if std::env::var_os(CHILD_DATA_DIR).is_some() {
        return;
    }
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b5-giant-kill-").expect("create kill fixture");
    let data_dir = fixture.child("server-data").unwrap();
    let address_file = fixture.child("child-address").unwrap();
    let executable = std::env::current_exe().expect("resolve B5 test executable");
    let mut command = Command::new(executable);
    command
        .arg("--exact")
        .arg("b5_giant_child_server_entrypoint")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_DATA_DIR, &data_dir)
        .env(CHILD_ADDRESS_FILE, &address_file);
    let child = OwnedChildProcess::spawn(&fixture, &mut command).expect("spawn B5 child");
    let deadline = Instant::now() + Duration::from_secs(10);
    let address: SocketAddr = loop {
        if let Ok(text) = fs::read_to_string(&address_file) {
            break text.trim().parse().expect("parse B5 child address");
        }
        assert!(Instant::now() < deadline, "B5 child did not start");
        thread::sleep(Duration::from_millis(25));
    };
    let mut connection = tcp_connect(address, DATABASE).unwrap();
    tcp_command(
        &mut connection,
        "CREATE TABLE killed_giant (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    )
    .unwrap();
    tcp_command(&mut connection, "PRAGMA CHECKPOINT").unwrap();
    connection.begin().unwrap();
    for chunk in 0..8i64 {
        let start = chunk * 256 + 1;
        let end = (chunk + 1) * 256;
        let values = (start..=end)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>()
            .join(",");
        tcp_command(
            &mut connection,
            format!("INSERT INTO killed_giant VALUES {values}"),
        )
        .unwrap();
    }
    assert_eq!(
        tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM killed_giant").unwrap(),
        2_048
    );
    let status = child.terminate().expect("kill B5 child");
    assert!(!status.success());
    drop(connection);
    with_tcp_server(data_dir, 16, |restart| {
        let mut verifier = tcp_connect(restart, DATABASE).unwrap();
        assert_eq!(
            tcp_scalar_i64(&mut verifier, "SELECT COUNT(*) FROM killed_giant").unwrap(),
            0
        );
    });
}
