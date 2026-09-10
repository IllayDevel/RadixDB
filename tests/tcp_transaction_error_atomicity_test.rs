// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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

const DATABASE: &str = "tcp_transaction_error_atomicity";

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 16,
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
    let mut client = Connection::connect(address).expect("connect");
    client.authenticate("root", None).expect("authenticate");
    client.select_database(DATABASE).expect("select database");
    client
}

fn command(client: &mut Connection, sql: impl AsRef<str>) {
    assert!(matches!(
        client.execute(sql.as_ref()).expect("execute command"),
        ExecuteResult::CommandComplete { .. }
    ));
}

fn count(client: &mut Connection, sql: &str) -> i64 {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("open count cursor") else {
        panic!("count query did not return a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch count");
    assert!(batch.eof);
    let Row { values } = batch.rows.into_iter().next().expect("count row");
    let WireValue::Int(value) = values.into_iter().next().expect("count value") else {
        panic!("COUNT did not return an integer");
    };
    value
}

fn verify_one_coherent_aggregate(client: &mut Connection) {
    assert_eq!(count(client, "SELECT COUNT(*) FROM parents"), 1);
    assert_eq!(count(client, "SELECT COUNT(*) FROM claims"), 1);
    assert_eq!(count(client, "SELECT COUNT(*) FROM members"), 1);
    assert_eq!(
        count(
            client,
            "SELECT COUNT(*) FROM parents p JOIN claims c ON c.parent_id = p.id"
        ),
        1
    );
    assert_eq!(
        count(
            client,
            "SELECT COUNT(*) FROM parents p JOIN members m ON m.parent_id = p.id"
        ),
        1
    );
}

#[derive(Debug)]
enum WriterOutcome {
    Committed,
    Rejected { error: String },
}

#[test]
fn transaction_errors_are_explicit_rollback_capable_and_cross_table_atomic() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let config = test_config(data_dir.clone());
    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut setup = connect(address);
        command(
            &mut setup,
            "CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                score INTEGER NOT NULL CHECK (score > 0)
            )",
        );
        command(
            &mut setup,
            "CREATE TABLE claims (
                parent_id INTEGER PRIMARY KEY REFERENCES parents(id),
                scope_id INTEGER NOT NULL UNIQUE
            )",
        );
        command(
            &mut setup,
            "CREATE TABLE members (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL REFERENCES parents(id)
            )",
        );

        let barrier = Arc::new(Barrier::new(8));
        let writers: Vec<_> = (1..=8)
            .map(|id| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let mut client = connect(address);
                    client.begin().expect("begin writer transaction");
                    command(
                        &mut client,
                        format!("INSERT INTO parents VALUES ({id}, 'candidate-{id}', 1)"),
                    );
                    barrier.wait();

                    let claim = client.execute(format!("INSERT INTO claims VALUES ({id}, 777)"));

                    match claim {
                        Ok(ExecuteResult::CommandComplete { .. }) => {
                            command(
                                &mut client,
                                format!("INSERT INTO members VALUES ({id}, {id})"),
                            );
                            client.commit().expect("commit winning aggregate");
                            assert!(!client.in_transaction());
                            WriterOutcome::Committed
                        }
                        Ok(ExecuteResult::Cursor(cursor)) => {
                            let _ = client.close_cursor(cursor);
                            panic!("INSERT unexpectedly returned a cursor")
                        }
                        Err(error) => {
                            assert!(
                                client.in_transaction(),
                                "constraint statement failure must leave rollback available: {error}"
                            );
                            client.rollback().expect("rollback rejected aggregate");
                            assert!(!client.in_transaction());
                            WriterOutcome::Rejected {
                                error: error.to_string(),
                            }
                        }
                    }
                })
            })
            .collect();

        let outcomes: Vec<_> = writers
            .into_iter()
            .map(|worker| worker.join().expect("writer joins"))
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WriterOutcome::Committed))
                .count(),
            1,
            "exactly one writer must win: {outcomes:?}"
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WriterOutcome::Rejected { .. }))
                .count(),
            7
        );
        for outcome in &outcomes {
            if let WriterOutcome::Rejected { error } = outcome {
                assert!(error.contains("unique constraint"), "{error}");
            }
        }
        verify_one_coherent_aggregate(&mut setup);

        // Statement-time constraint errors have the same explicit contract:
        // the transaction stays active and every earlier write can be rolled back.
        for (id, bad_sql, expected) in [
            (
                101,
                "INSERT INTO parents VALUES (101, 'duplicate', 1)",
                "primary key",
            ),
            (
                102,
                "INSERT INTO members VALUES (102, 999999)",
                "foreign key",
            ),
            (103, "UPDATE parents SET score = -1 WHERE id = 103", "check"),
        ] {
            setup.begin().expect("begin statement-error transaction");
            command(
                &mut setup,
                format!("INSERT INTO parents VALUES ({id}, 'must-rollback', 1)"),
            );
            let error = setup
                .execute(bad_sql)
                .expect_err("constraint statement must fail");
            assert!(
                error.to_string().to_lowercase().contains(expected),
                "{error}"
            );
            assert!(setup.in_transaction());
            setup.rollback().expect("rollback after statement error");
            assert!(!setup.in_transaction());
            assert_eq!(
                count(
                    &mut setup,
                    &format!("SELECT COUNT(*) FROM parents WHERE id = {id}")
                ),
                0
            );
        }

        drop(setup);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server stops cleanly");
    });

    // The single winner remains coherent after WAL replay/restart; no losing
    // parent from a failed multi-table commit may reappear.
    let restart_config = test_config(data_dir);
    let restart_server = Server::bind_ephemeral(&restart_config).expect("bind restart server");
    let restart_address = restart_server.local_addr().expect("restart address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| restart_server.serve_one());
        let mut client = connect(restart_address);
        verify_one_coherent_aggregate(&mut client);
        drop(client);
        worker
            .join()
            .expect("restart thread joins")
            .expect("restart server serves client");
    });
}
