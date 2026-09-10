// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{Connection, ExecuteResult, Row, WireValue};

const DATABASE: &str = "tcp_concurrent_row_update";

struct ShutdownOnDrop<'a>(&'a AtomicBool);

impl Drop for ShutdownOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 20,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: radixdb::server::default_max_databases(),
        max_database_name_bytes: radixdb::server::default_max_database_name_bytes(),
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 128,
        cursor_batch_max_bytes: 1024 * 1024,
        max_frame_bytes: 4 * 1024 * 1024,
        copy_max_transaction_bytes: radixdb::server::default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: default_target_volume_rows(),
        seal_hot_bytes_threshold: default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold: default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

fn connect(address: SocketAddr) -> Connection {
    let mut connection = Connection::connect(address).expect("connect");
    connection.authenticate("root", None).expect("authenticate");
    connection
        .select_database(DATABASE)
        .expect("select database");
    connection
}

fn command(connection: &mut Connection, sql: &str) {
    assert!(matches!(
        connection.execute(sql).expect("command succeeds"),
        ExecuteResult::CommandComplete { .. }
    ));
}

fn scalar_i64(connection: &mut Connection, sql: &str) -> i64 {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql).expect("open scalar cursor") else {
        panic!("scalar query did not return a cursor");
    };
    let batch = connection.fetch(&cursor).expect("fetch scalar");
    assert!(batch.eof);
    let Row { values } = batch.rows.into_iter().next().expect("scalar row");
    let WireValue::Int(value) = values.into_iter().next().expect("scalar value") else {
        panic!("scalar is not INTEGER");
    };
    value
}

fn optional_scalar_i64(connection: &mut Connection, sql: &str) -> Option<i64> {
    let ExecuteResult::Cursor(cursor) = connection.execute(sql).expect("open optional cursor")
    else {
        panic!("optional scalar query did not return a cursor");
    };
    let batch = connection.fetch(&cursor).expect("fetch optional scalar");
    assert!(batch.eof);
    let row = batch.rows.into_iter().next()?;
    let WireValue::Int(value) = row.values.into_iter().next()? else {
        panic!("optional scalar is not INTEGER");
    };
    Some(value)
}

fn increment_returning(connection: &mut Connection, row_id: i64) -> i64 {
    scalar_i64(
        connection,
        &format!(
            "UPDATE counters SET next_seq = next_seq + 1 WHERE id = {row_id} RETURNING next_seq"
        ),
    )
}

fn run_eight_writers(address: SocketAddr, expected: std::ops::RangeInclusive<i64>) {
    let barrier = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut connection = connect(address);
                connection.begin().expect("begin increment");
                barrier.wait();
                let value = increment_returning(&mut connection, 1);
                connection.commit().expect("commit increment");
                value
            })
        })
        .collect();

    let mut values: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("increment writer joins"))
        .collect();
    values.sort_unstable();
    assert_eq!(values, expected.collect::<Vec<_>>());
}

#[test]
fn r8_l01_batch_b_tcp_waiters_use_state_barriers() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let first_config = config(data_dir.clone());
    let server = Server::bind_ephemeral(&first_config).expect("bind server");
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let _shutdown_on_unwind = ShutdownOnDrop(&shutdown);
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut setup = connect(address);
        command(
            &mut setup,
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, next_seq INTEGER NOT NULL)",
        );
        command(&mut setup, "INSERT INTO counters VALUES (1, 0), (2, 0)");

        run_eight_writers(address, 1..=8);
        assert_eq!(
            scalar_i64(&mut setup, "SELECT next_seq FROM counters WHERE id = 1"),
            8
        );

        // A waiting writer must wake after rollback and compute from the
        // committed value, not from the owner's discarded local version.
        let mut owner = connect(address);
        owner.begin().expect("begin owner");
        assert_eq!(increment_returning(&mut owner, 2), 1);
        let (waiter_ready_tx, waiter_ready_rx) = std::sync::mpsc::channel();
        let waiter = thread::spawn(move || {
            let mut connection = connect(address);
            connection.begin().expect("begin waiter");
            waiter_ready_tx.send(()).unwrap();
            let value = increment_returning(&mut connection, 2);
            connection.commit().expect("commit waiter");
            value
        });
        waiter_ready_rx.recv().unwrap();
        owner.rollback().expect("rollback owner");
        assert_eq!(waiter.join().expect("waiter joins"), 1);
        assert_eq!(
            scalar_i64(&mut setup, "SELECT next_seq FROM counters WHERE id = 2"),
            1
        );

        // Two transactions take rows in opposite order. The wait-for graph
        // rejects exactly the edge that would close the cycle; the rejected
        // transaction remains rollback-capable and releases its first claim.
        command(&mut setup, "INSERT INTO counters VALUES (3, 0), (4, 0)");
        let deadlock_barrier = Arc::new(Barrier::new(2));
        let deadlock_workers: Vec<_> = [(3, 4), (4, 3)]
            .into_iter()
            .map(|(first, second)| {
                let barrier = Arc::clone(&deadlock_barrier);
                thread::spawn(move || {
                    let mut connection = connect(address);
                    connection.begin().expect("begin deadlock participant");
                    assert_eq!(increment_returning(&mut connection, first), 1);
                    barrier.wait();
                    match connection.execute(format!(
                        "UPDATE counters SET next_seq = next_seq + 1 WHERE id = {second}"
                    )) {
                        Ok(ExecuteResult::CommandComplete { .. }) => {
                            connection.commit().expect("commit deadlock winner");
                            true
                        }
                        Ok(ExecuteResult::Cursor(_)) => {
                            panic!("UPDATE without RETURNING opened a cursor")
                        }
                        Err(error) => {
                            assert!(
                                error.to_string().contains("serialization conflict"),
                                "unexpected deadlock error: {error}"
                            );
                            assert!(connection.in_transaction());
                            connection.rollback().expect("rollback deadlock victim");
                            false
                        }
                    }
                })
            })
            .collect();
        let outcomes: Vec<_> = deadlock_workers
            .into_iter()
            .map(|worker| worker.join().expect("deadlock participant joins"))
            .collect();
        assert_eq!(outcomes.iter().filter(|committed| **committed).count(), 1);
        assert_eq!(
            scalar_i64(&mut setup, "SELECT next_seq FROM counters WHERE id = 3"),
            1
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT next_seq FROM counters WHERE id = 4"),
            1
        );

        // The wait-for graph is engine-wide rather than table-local. Prove
        // cycle detection when the two claims belong to different tables.
        command(
            &mut setup,
            "CREATE TABLE left_locks (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        );
        command(
            &mut setup,
            "CREATE TABLE right_locks (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        );
        command(&mut setup, "INSERT INTO left_locks VALUES (1, 0)");
        command(&mut setup, "INSERT INTO right_locks VALUES (1, 0)");
        let cross_table_barrier = Arc::new(Barrier::new(2));
        let cross_table_workers: Vec<_> =
            [("left_locks", "right_locks"), ("right_locks", "left_locks")]
                .into_iter()
                .map(|(first, second)| {
                    let barrier = Arc::clone(&cross_table_barrier);
                    thread::spawn(move || {
                        let mut connection = connect(address);
                        connection.begin().expect("begin cross-table participant");
                        assert_eq!(
                            scalar_i64(
                                &mut connection,
                                &format!(
                            "UPDATE {first} SET value = value + 1 WHERE id = 1 RETURNING value"
                        ),
                            ),
                            1
                        );
                        barrier.wait();
                        match connection.execute(format!(
                            "UPDATE {second} SET value = value + 1 WHERE id = 1"
                        )) {
                            Ok(ExecuteResult::CommandComplete { .. }) => {
                                connection.commit().expect("commit cross-table winner");
                                true
                            }
                            Ok(ExecuteResult::Cursor(_)) => {
                                panic!("cross-table UPDATE unexpectedly opened a cursor")
                            }
                            Err(error) => {
                                assert!(
                                    error.to_string().contains("serialization conflict"),
                                    "unexpected cross-table deadlock error: {error}"
                                );
                                connection.rollback().expect("rollback cross-table victim");
                                false
                            }
                        }
                    })
                })
                .collect();
        let cross_table_outcomes: Vec<_> = cross_table_workers
            .into_iter()
            .map(|worker| worker.join().expect("cross-table participant joins"))
            .collect();
        assert_eq!(
            cross_table_outcomes
                .iter()
                .filter(|committed| **committed)
                .count(),
            1
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT value FROM left_locks WHERE id = 1"),
            1
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT value FROM right_locks WHERE id = 1"),
            1
        );

        // A request consumes two independent rate-limit dimensions in one
        // transaction. The tighter source limit wins and rollback prevents the
        // looser account bucket from being over-counted.
        command(
            &mut setup,
            "CREATE TABLE auth_limits (
                id INTEGER PRIMARY KEY,
                used INTEGER NOT NULL,
                max_uses INTEGER NOT NULL
            )",
        );
        command(
            &mut setup,
            "INSERT INTO auth_limits VALUES (1, 0, 3), (2, 0, 5)",
        );
        let limits_barrier = Arc::new(Barrier::new(8));
        let limit_consumers: Vec<_> = (0..8)
            .map(|_| {
                let barrier = Arc::clone(&limits_barrier);
                thread::spawn(move || {
                    let mut connection = connect(address);
                    connection.begin().expect("begin rate-limit consumer");
                    barrier.wait();
                    let source = optional_scalar_i64(
                        &mut connection,
                        "UPDATE auth_limits SET used = used + 1
                         WHERE id = 1 AND used < max_uses RETURNING used",
                    );
                    let account = source.and_then(|_| {
                        optional_scalar_i64(
                            &mut connection,
                            "UPDATE auth_limits SET used = used + 1
                             WHERE id = 2 AND used < max_uses RETURNING used",
                        )
                    });
                    if account.is_some() {
                        connection.commit().expect("commit rate-limit consume");
                        true
                    } else {
                        connection
                            .rollback()
                            .expect("rollback rejected rate-limit consume");
                        false
                    }
                })
            })
            .collect();
        let limit_outcomes: Vec<_> = limit_consumers
            .into_iter()
            .map(|worker| worker.join().expect("rate-limit consumer joins"))
            .collect();
        assert_eq!(
            limit_outcomes.iter().filter(|accepted| **accepted).count(),
            3
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT used FROM auth_limits WHERE id = 1"),
            3
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT used FROM auth_limits WHERE id = 2"),
            3
        );

        // One-time token consumption exercises predicate re-evaluation after
        // waiting. Every contender writes another table first; seven losing
        // transactions must roll that write back with the failed consume. The
        // winner's password/session side effects share the same commit.
        command(
            &mut setup,
            "CREATE TABLE tokens (id INTEGER PRIMARY KEY, consumed INTEGER NOT NULL)",
        );
        command(
            &mut setup,
            "CREATE TABLE consume_attempts (id INTEGER PRIMARY KEY)",
        );
        command(
            &mut setup,
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, password_version INTEGER NOT NULL)",
        );
        command(
            &mut setup,
            "CREATE TABLE device_sessions (id INTEGER PRIMARY KEY, active INTEGER NOT NULL)",
        );
        command(&mut setup, "INSERT INTO tokens VALUES (1, 0)");
        command(&mut setup, "INSERT INTO accounts VALUES (1, 1)");
        command(&mut setup, "INSERT INTO device_sessions VALUES (1, 1)");
        let consume_barrier = Arc::new(Barrier::new(8));
        let consumers: Vec<_> = (1..=8)
            .map(|attempt_id| {
                let barrier = Arc::clone(&consume_barrier);
                thread::spawn(move || {
                    let mut connection = connect(address);
                    connection.begin().expect("begin token consumer");
                    command(
                        &mut connection,
                        &format!("INSERT INTO consume_attempts VALUES ({attempt_id})"),
                    );
                    barrier.wait();
                    let consumed = optional_scalar_i64(
                        &mut connection,
                        "UPDATE tokens SET consumed = 1
                         WHERE id = 1 AND consumed = 0 RETURNING consumed",
                    );
                    if consumed.is_some() {
                        command(
                            &mut connection,
                            "UPDATE accounts SET password_version = 2 WHERE id = 1",
                        );
                        command(
                            &mut connection,
                            "UPDATE device_sessions SET active = 0 WHERE id = 1",
                        );
                        connection.commit().expect("commit token winner");
                        true
                    } else {
                        connection.rollback().expect("rollback token loser");
                        false
                    }
                })
            })
            .collect();
        let consume_outcomes: Vec<_> = consumers
            .into_iter()
            .map(|worker| worker.join().expect("token consumer joins"))
            .collect();
        assert_eq!(
            consume_outcomes
                .iter()
                .filter(|consumed| **consumed)
                .count(),
            1
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT consumed FROM tokens WHERE id = 1"),
            1
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT COUNT(*) FROM consume_attempts"),
            1
        );
        assert_eq!(
            scalar_i64(
                &mut setup,
                "SELECT password_version FROM accounts WHERE id = 1"
            ),
            2
        );
        assert_eq!(
            scalar_i64(
                &mut setup,
                "SELECT active FROM device_sessions WHERE id = 1"
            ),
            0
        );

        // A would-be token winner may fail before commit. Its token, password
        // and session changes disappear together; the waiter then consumes the
        // still-current token and publishes exactly one coherent result.
        command(&mut setup, "INSERT INTO tokens VALUES (2, 0)");
        command(
            &mut setup,
            "UPDATE device_sessions SET active = 1 WHERE id = 1",
        );
        let mut failed_winner = connect(address);
        failed_winner.begin().expect("begin failed token winner");
        assert_eq!(
            optional_scalar_i64(
                &mut failed_winner,
                "UPDATE tokens SET consumed = 1
                 WHERE id = 2 AND consumed = 0 RETURNING consumed",
            ),
            Some(1)
        );
        command(
            &mut failed_winner,
            "UPDATE accounts SET password_version = 3 WHERE id = 1",
        );
        command(
            &mut failed_winner,
            "UPDATE device_sessions SET active = 0 WHERE id = 1",
        );
        let (token_ready_tx, token_ready_rx) = std::sync::mpsc::channel();
        let token_waiter = thread::spawn(move || {
            let mut connection = connect(address);
            connection.begin().expect("begin token waiter");
            token_ready_tx.send(()).unwrap();
            let consumed = optional_scalar_i64(
                &mut connection,
                "UPDATE tokens SET consumed = 1
                 WHERE id = 2 AND consumed = 0 RETURNING consumed",
            );
            assert_eq!(consumed, Some(1));
            command(
                &mut connection,
                "UPDATE accounts SET password_version = 4 WHERE id = 1",
            );
            command(
                &mut connection,
                "UPDATE device_sessions SET active = 0 WHERE id = 1",
            );
            connection.commit().expect("commit token waiter");
        });
        token_ready_rx.recv().unwrap();
        failed_winner
            .rollback()
            .expect("rollback failed token winner");
        token_waiter.join().expect("token waiter joins");
        assert_eq!(
            scalar_i64(&mut setup, "SELECT consumed FROM tokens WHERE id = 2"),
            1
        );
        assert_eq!(
            scalar_i64(
                &mut setup,
                "SELECT password_version FROM accounts WHERE id = 1"
            ),
            4
        );
        assert_eq!(
            scalar_i64(
                &mut setup,
                "SELECT active FROM device_sessions WHERE id = 1"
            ),
            0
        );

        // Cursor allocation, event and outbox record form one transaction.
        // Eight writers get contiguous values; an owner rollback must release
        // cursor 9 for the waiter and publish neither orphan record nor gap.
        command(
            &mut setup,
            "CREATE TABLE sync_state (id INTEGER PRIMARY KEY, next_cursor INTEGER NOT NULL)",
        );
        command(
            &mut setup,
            "CREATE TABLE sync_events (cursor INTEGER PRIMARY KEY, payload INTEGER NOT NULL)",
        );
        command(
            &mut setup,
            "CREATE TABLE sync_outbox (cursor INTEGER PRIMARY KEY, payload INTEGER NOT NULL)",
        );
        command(&mut setup, "INSERT INTO sync_state VALUES (1, 0)");
        let sync_barrier = Arc::new(Barrier::new(8));
        let sync_workers: Vec<_> = (0..8)
            .map(|_| {
                let barrier = Arc::clone(&sync_barrier);
                thread::spawn(move || {
                    let mut connection = connect(address);
                    connection.begin().expect("begin sync allocation");
                    barrier.wait();
                    let cursor = scalar_i64(
                        &mut connection,
                        "UPDATE sync_state SET next_cursor = next_cursor + 1
                         WHERE id = 1 RETURNING next_cursor",
                    );
                    command(
                        &mut connection,
                        &format!("INSERT INTO sync_events VALUES ({cursor}, {cursor})"),
                    );
                    command(
                        &mut connection,
                        &format!("INSERT INTO sync_outbox VALUES ({cursor}, {cursor})"),
                    );
                    connection.commit().expect("commit sync allocation");
                    cursor
                })
            })
            .collect();
        let mut cursors: Vec<_> = sync_workers
            .into_iter()
            .map(|worker| worker.join().expect("sync allocation joins"))
            .collect();
        cursors.sort_unstable();
        assert_eq!(cursors, (1..=8).collect::<Vec<_>>());

        let mut cursor_owner = connect(address);
        cursor_owner.begin().expect("begin cursor rollback owner");
        assert_eq!(
            scalar_i64(
                &mut cursor_owner,
                "UPDATE sync_state SET next_cursor = next_cursor + 1
                 WHERE id = 1 RETURNING next_cursor"
            ),
            9
        );
        command(&mut cursor_owner, "INSERT INTO sync_events VALUES (9, 900)");
        command(&mut cursor_owner, "INSERT INTO sync_outbox VALUES (9, 900)");
        let (cursor_ready_tx, cursor_ready_rx) = std::sync::mpsc::channel();
        let cursor_waiter = thread::spawn(move || {
            let mut connection = connect(address);
            connection.begin().expect("begin cursor waiter");
            cursor_ready_tx.send(()).unwrap();
            let cursor = scalar_i64(
                &mut connection,
                "UPDATE sync_state SET next_cursor = next_cursor + 1
                 WHERE id = 1 RETURNING next_cursor",
            );
            command(
                &mut connection,
                &format!("INSERT INTO sync_events VALUES ({cursor}, {cursor})"),
            );
            command(
                &mut connection,
                &format!("INSERT INTO sync_outbox VALUES ({cursor}, {cursor})"),
            );
            connection.commit().expect("commit cursor waiter");
            cursor
        });
        cursor_ready_rx.recv().unwrap();
        cursor_owner
            .rollback()
            .expect("rollback cursor allocation owner");
        assert_eq!(cursor_waiter.join().expect("cursor waiter joins"), 9);
        assert_eq!(
            scalar_i64(
                &mut setup,
                "SELECT next_cursor FROM sync_state WHERE id = 1"
            ),
            9
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT COUNT(*) FROM sync_events"),
            9
        );
        assert_eq!(
            scalar_i64(&mut setup, "SELECT COUNT(*) FROM sync_outbox"),
            9
        );

        // Leave conditional and delete-vs-update cases as cold rows for the
        // restart half of the test.
        command(&mut setup, "INSERT INTO tokens VALUES (3, 0)");
        command(&mut setup, "INSERT INTO counters VALUES (5, 0)");

        drop(owner);
        drop(setup);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server stops cleanly");
    });

    // Clean shutdown seals the first hot generation. The same row-level
    // contract must hold after restart when the logical table has cold data.
    let restart_config = config(data_dir);
    let restart_server = Server::bind_ephemeral(&restart_config).expect("bind restart server");
    let restart_address = restart_server.local_addr().unwrap();
    let restart_shutdown = AtomicBool::new(false);
    thread::scope(|scope| {
        let _shutdown_on_unwind = ShutdownOnDrop(&restart_shutdown);
        let worker = scope.spawn(|| restart_server.run_until(&restart_shutdown));
        // Open/recover once before releasing concurrent clients. Database-open
        // readiness is a separate server contract from row-writer waiting.
        drop(connect(restart_address));
        run_eight_writers(restart_address, 9..=16);
        let mut verify = connect(restart_address);
        assert_eq!(
            scalar_i64(&mut verify, "SELECT next_seq FROM counters WHERE id = 1"),
            16
        );

        let cold_token_barrier = Arc::new(Barrier::new(8));
        let cold_token_workers: Vec<_> = (0..8)
            .map(|_| {
                let barrier = Arc::clone(&cold_token_barrier);
                thread::spawn(move || {
                    let mut connection = connect(restart_address);
                    connection.begin().expect("begin cold token consumer");
                    barrier.wait();
                    let consumed = optional_scalar_i64(
                        &mut connection,
                        "UPDATE tokens SET consumed = 1
                         WHERE id = 3 AND consumed = 0 RETURNING consumed",
                    );
                    if consumed.is_some() {
                        connection.commit().expect("commit cold token winner");
                        true
                    } else {
                        connection.rollback().expect("rollback cold token loser");
                        false
                    }
                })
            })
            .collect();
        let cold_token_outcomes: Vec<_> = cold_token_workers
            .into_iter()
            .map(|worker| worker.join().expect("cold token consumer joins"))
            .collect();
        assert_eq!(
            cold_token_outcomes
                .iter()
                .filter(|consumed| **consumed)
                .count(),
            1
        );

        let mut delete_owner = connect(restart_address);
        delete_owner.begin().expect("begin cold delete owner");
        command(&mut delete_owner, "DELETE FROM counters WHERE id = 5");
        let (delete_ready_tx, delete_ready_rx) = std::sync::mpsc::channel();
        let deleted_row_waiter = thread::spawn(move || {
            let mut connection = connect(restart_address);
            connection.begin().expect("begin deleted-row waiter");
            delete_ready_tx.send(()).unwrap();
            let updated = optional_scalar_i64(
                &mut connection,
                "UPDATE counters SET next_seq = next_seq + 1
                 WHERE id = 5 RETURNING next_seq",
            );
            connection.commit().expect("commit deleted-row waiter");
            updated
        });
        delete_ready_rx.recv().unwrap();
        delete_owner.commit().expect("commit cold delete owner");
        assert_eq!(
            deleted_row_waiter.join().expect("deleted-row waiter joins"),
            None
        );
        assert_eq!(
            scalar_i64(&mut verify, "SELECT COUNT(*) FROM counters WHERE id = 5"),
            0
        );
        drop(verify);
        restart_shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("restart thread joins")
            .expect("restart server stops");
    });
}
