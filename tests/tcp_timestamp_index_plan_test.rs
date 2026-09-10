// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
};

use chrono::{TimeZone, Utc};
use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{Connection, ExecuteResult, WireValue};

const DATABASE: &str = "tcp_timestamp_index_plan";

fn config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 8,
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

fn expect_command(result: ExecuteResult) {
    assert!(matches!(result, ExecuteResult::CommandComplete { .. }));
}

fn fetch_text(connection: &mut Connection, result: ExecuteResult) -> String {
    let ExecuteResult::Cursor(cursor) = result else {
        panic!("expected cursor")
    };
    let mut lines = Vec::new();
    loop {
        let batch = connection.fetch(&cursor).unwrap();
        for row in batch.rows {
            let WireValue::String(line) = &row.values[0] else {
                panic!("expected text plan line")
            };
            lines.push(line.clone());
        }
        if batch.eof {
            return lines.join("\n");
        }
    }
}

#[test]
fn typed_tcp_timestamp_parameter_selects_executable_composite_index_plan() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::bind_ephemeral(&config(temp.path().join("data"))).unwrap();
    let address = server.local_addr().unwrap();

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut connection = Connection::connect(address).unwrap();
        connection.authenticate("root", None).unwrap();
        connection.select_database(DATABASE).unwrap();
        expect_command(
            connection
                .execute(
                    "CREATE TABLE jobs (
                        id INTEGER PRIMARY KEY,
                        state TEXT NOT NULL,
                        due_at TIMESTAMP NOT NULL
                    )",
                )
                .unwrap(),
        );
        expect_command(
            connection
                .execute("CREATE INDEX jobs_due_idx ON jobs (due_at, id)")
                .unwrap(),
        );
        expect_command(
            connection
                .execute(
                    "INSERT INTO jobs VALUES
                     (1, 'pending', TIMESTAMP '2026-08-08 10:00:00'),
                     (2, 'pending', TIMESTAMP '2026-08-10 10:00:00')",
                )
                .unwrap(),
        );

        let mut parameters = BTreeMap::new();
        parameters.insert(
            "now".to_string(),
            WireValue::DateTime {
                millis_since_unix_epoch_utc: Utc
                    .with_ymd_and_hms(2026, 8, 9, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            },
        );
        let plan_result = connection
            .execute_with_parameters(
                "EXPLAIN SELECT id FROM jobs
                 WHERE state = 'pending' AND due_at <= :now
                 ORDER BY due_at, id LIMIT 100",
                parameters.clone(),
            )
            .unwrap();
        let plan = fetch_text(&mut connection, plan_result);
        assert!(
            plan.contains("jobs_due_idx")
                && plan.contains("Access Path: scan.composite_index")
                && !plan.contains(":now"),
            "typed TCP parameter must be bound before EXPLAIN planning:\n{plan}"
        );

        let result = connection
            .execute_with_parameters(
                "SELECT id FROM jobs WHERE due_at <= :now ORDER BY due_at, id",
                parameters,
            )
            .unwrap();
        let ExecuteResult::Cursor(cursor) = result else {
            panic!("query must return cursor")
        };
        let batch = connection.fetch(&cursor).unwrap();
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].values, [WireValue::Int(1)]);

        connection.shutdown().unwrap();
        worker.join().unwrap().unwrap();
    });
}
