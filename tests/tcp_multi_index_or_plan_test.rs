// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! TCP contract for RDB-0007 composite OR branch planning.

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

const DATABASE: &str = "tcp_multi_index_or_plan";
const QUERY: &str = "SELECT id, revision
FROM outbox_jobs
WHERE (state = 'pending' AND available_at <= :now)
   OR (state = 'leased' AND lease_until IS NOT NULL AND lease_until <= :now)
ORDER BY available_at, created_at, id
LIMIT :limit";

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

fn command(connection: &mut Connection, sql: &str) {
    assert!(matches!(
        connection.execute(sql).unwrap(),
        ExecuteResult::CommandComplete { .. }
    ));
}

fn parameters(limit: i64) -> BTreeMap<String, WireValue> {
    let mut values = BTreeMap::new();
    values.insert(
        "now".to_string(),
        WireValue::DateTime {
            millis_since_unix_epoch_utc: Utc
                .with_ymd_and_hms(2026, 8, 9, 0, 0, 0)
                .unwrap()
                .timestamp_millis(),
        },
    );
    values.insert("limit".to_string(), WireValue::Int(limit));
    values
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
                panic!("expected plan text")
            };
            lines.push(line.clone());
        }
        if batch.eof {
            return lines.join("\n");
        }
    }
}

fn fetch_ids(connection: &mut Connection, result: ExecuteResult) -> Vec<i64> {
    let ExecuteResult::Cursor(cursor) = result else {
        panic!("expected cursor")
    };
    let mut ids = Vec::new();
    loop {
        let batch = connection.fetch(&cursor).unwrap();
        for row in batch.rows {
            let WireValue::Int(id) = row.values[0] else {
                panic!("expected integer id")
            };
            ids.push(id);
        }
        if batch.eof {
            return ids;
        }
    }
}

#[test]
fn tcp_typed_parameters_expose_and_execute_composite_index_union() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::bind_ephemeral(&config(temp.path().join("data"))).unwrap();
    let address = server.local_addr().unwrap();

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut connection = Connection::connect(address).unwrap();
        connection.authenticate("root", None).unwrap();
        connection.select_database(DATABASE).unwrap();
        command(
            &mut connection,
            "CREATE TABLE outbox_jobs (
                id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                available_at TIMESTAMP NOT NULL,
                lease_until TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                revision INTEGER NOT NULL
            )",
        );
        command(
            &mut connection,
            "CREATE INDEX outbox_jobs_available_idx
             ON outbox_jobs (state, available_at)",
        );
        command(
            &mut connection,
            "CREATE INDEX outbox_jobs_lease_idx
             ON outbox_jobs (state, lease_until)",
        );
        command(
            &mut connection,
            "INSERT INTO outbox_jobs VALUES
             (1, 'pending', TIMESTAMP '2026-08-08 10:00:00', NULL,
                 TIMESTAMP '2026-08-08 09:00:00', 1),
             (2, 'leased', TIMESTAMP '2026-08-08 08:00:00',
                 TIMESTAMP '2026-08-08 11:00:00', TIMESTAMP '2026-08-08 08:00:00', 1),
             (3, 'leased', TIMESTAMP '2026-08-08 07:00:00', NULL,
                 TIMESTAMP '2026-08-08 07:00:00', 1)",
        );

        let plan_result = connection
            .execute_with_parameters(format!("EXPLAIN {QUERY}"), parameters(10))
            .unwrap();
        let plan = fetch_text(&mut connection, plan_result);
        assert!(
            plan.contains("Access Path: scan.multi_index")
                && plan.contains("outbox_jobs_available_idx")
                && plan.contains("outbox_jobs_lease_idx")
                && !plan.contains("Access Path: scan.seq")
                && !plan.contains(":now"),
            "typed TCP bindings must produce one executable index union:\n{plan}"
        );

        connection.begin().unwrap();
        let result = connection
            .execute_with_parameters(QUERY, parameters(10))
            .unwrap();
        assert_eq!(fetch_ids(&mut connection, result), vec![2, 1]);
        connection.commit().unwrap();

        let checkpoint = connection.execute("PRAGMA CHECKPOINT").unwrap();
        if let ExecuteResult::Cursor(cursor) = checkpoint {
            while !connection.fetch(&cursor).unwrap().eof {}
        }
        let cold_plan_result = connection
            .execute_with_parameters(format!("EXPLAIN {QUERY}"), parameters(10))
            .unwrap();
        let cold_plan = fetch_text(&mut connection, cold_plan_result);
        assert!(
            cold_plan.contains("Access Path: scan.multi_index")
                && cold_plan.contains("Cold Access Path: volume.multi_index_union")
                && cold_plan.contains("outbox_jobs_available_idx")
                && cold_plan.contains("outbox_jobs_lease_idx")
                && !cold_plan.contains("Access Path: scan.cold_artifact"),
            "typed TCP OR must retain its persisted union after checkpoint:\n{cold_plan}"
        );
        let cold_result = connection
            .execute_with_parameters(QUERY, parameters(10))
            .unwrap();
        assert_eq!(fetch_ids(&mut connection, cold_result), vec![2, 1]);

        connection.shutdown().unwrap();
        worker.join().unwrap().unwrap();
    });
}
