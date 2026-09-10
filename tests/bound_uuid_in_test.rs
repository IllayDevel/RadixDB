// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! RDB-0010 regressions for bound UUID values in `IN` predicates.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::{Database, Value};
use radixdb_client::{Connection, ExecuteResult, Row, WireValue};

const DATABASE: &str = "bound_uuid_in";
const UUID_A: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const UUID_B: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];
const UUID_C: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x03, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa8,
];
const UUID_MISS: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x7f, 0xff, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xff,
];

const UUID_B_TEXT: &str = "019fe3bf-6880-7302-b5d0-58ce355457a7";

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
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

fn connect(address: SocketAddr) -> Connection {
    let mut client = Connection::connect(address).expect("connect TCP client");
    client.authenticate("root", None).expect("authenticate");
    client
        .select_database(DATABASE)
        .expect("select test database");
    client
}

fn with_server(config: ServerConfig, work: impl FnOnce(&mut Connection)) {
    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address);
        work(&mut client);
        client.shutdown().expect("shutdown client");
        worker
            .join()
            .expect("join server thread")
            .expect("serve client");
    });
}

fn command(client: &mut Connection, sql: &str) {
    let result = client
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    match result {
        ExecuteResult::CommandComplete { .. } => {}
        ExecuteResult::Cursor(cursor) => loop {
            if client.fetch(&cursor).expect("fetch command cursor").eof {
                break;
            }
        },
    }
}

fn parameters(values: &[(&str, [u8; 16])]) -> BTreeMap<String, WireValue> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_string(), WireValue::Uuid(*value)))
        .collect()
}

fn fetch_uuids(
    client: &mut Connection,
    sql: &str,
    params: BTreeMap<String, WireValue>,
) -> Vec<[u8; 16]> {
    let ExecuteResult::Cursor(cursor) = client
        .execute_with_parameters(sql, params)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("query must return a cursor: {sql}")
    };

    let mut values = Vec::new();
    loop {
        let batch = client.fetch(&cursor).expect("fetch UUID query");
        for Row { values: row } in batch.rows {
            let [WireValue::Uuid(value)] = row.as_slice() else {
                panic!("expected one UUID value, got {row:?}")
            };
            values.push(*value);
        }
        if batch.eof {
            values.sort_unstable();
            return values;
        }
    }
}

fn explain_bound_in(client: &mut Connection, table: &str) -> String {
    let ExecuteResult::Cursor(cursor) = client
        .execute_with_parameters(
            format!("EXPLAIN SELECT id FROM {table} WHERE id IN (:id)"),
            parameters(&[("id", UUID_B)]),
        )
        .expect("explain bound UUID IN")
    else {
        panic!("EXPLAIN must return a cursor")
    };
    let mut lines = Vec::new();
    loop {
        let batch = client.fetch(&cursor).expect("fetch EXPLAIN");
        for Row { values } in batch.rows {
            let Some(WireValue::String(line)) = values.first() else {
                panic!("EXPLAIN must return text lines, got {values:?}")
            };
            lines.push(line.clone());
        }
        if batch.eof {
            return lines.join("\n");
        }
    }
}

fn insert(client: &mut Connection, table: &str, row_id: i64, id: [u8; 16]) {
    let mut params = parameters(&[("id", id)]);
    params.insert("row_id".to_string(), WireValue::Int(row_id));
    let result = client
        .execute_with_parameters(
            format!("INSERT INTO {table} (row_id, id) VALUES (:row_id, :id)"),
            params,
        )
        .expect("insert UUID row");
    assert!(matches!(result, ExecuteResult::CommandComplete { .. }));
}

fn assert_uuid_in_contract(client: &mut Connection, table: &str) {
    assert_eq!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id = :id"),
            parameters(&[("id", UUID_B)]),
        ),
        vec![UUID_B],
        "equality oracle failed for {table}"
    );
    assert_eq!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id IN ('{UUID_B_TEXT}')"),
            BTreeMap::new(),
        ),
        vec![UUID_B],
        "literal IN failed for {table}"
    );
    assert_eq!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id IN (:id)"),
            parameters(&[("id", UUID_B)]),
        ),
        vec![UUID_B],
        "one bound UUID failed for {table}"
    );
    assert_eq!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id IN (:a, :b, :miss)"),
            parameters(&[("a", UUID_A), ("b", UUID_B), ("miss", UUID_MISS)]),
        ),
        vec![UUID_A, UUID_B],
        "multi-value bound UUID IN failed for {table}"
    );
    assert_eq!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id IN (:a, :again)"),
            parameters(&[("a", UUID_B), ("again", UUID_B)]),
        ),
        vec![UUID_B],
        "duplicate IN candidates must not duplicate rows for {table}"
    );
    assert!(
        fetch_uuids(
            client,
            &format!("SELECT id FROM {table} WHERE id IN (:miss)"),
            parameters(&[("miss", UUID_MISS)]),
        )
        .is_empty(),
        "missing bound UUID must return no rows for {table}"
    );
}

fn create_tables(client: &mut Connection) {
    command(
        client,
        "CREATE TABLE uuid_pk (row_id INTEGER UNIQUE, id UUID PRIMARY KEY)",
    );
    command(
        client,
        "CREATE TABLE uuid_unique (row_id INTEGER PRIMARY KEY, id UUID UNIQUE)",
    );
    command(
        client,
        "CREATE TABLE uuid_indexed (row_id INTEGER PRIMARY KEY, id UUID NOT NULL)",
    );
    command(
        client,
        "CREATE INDEX uuid_indexed_id_idx ON uuid_indexed (id)",
    );
    command(
        client,
        "CREATE TABLE uuid_plain (row_id INTEGER PRIMARY KEY, id UUID NOT NULL)",
    );
}

const TABLES: [&str; 4] = ["uuid_pk", "uuid_unique", "uuid_indexed", "uuid_plain"];

#[test]
fn bound_uuid_in_matches_equality_over_tcp_in_hot_cold_mixed_and_reopen_states() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("server-data");

    with_server(test_config(data_dir.clone()), |client| {
        create_tables(client);
        for table in TABLES {
            insert(client, table, 1, UUID_A);
            insert(client, table, 2, UUID_B);
            assert_uuid_in_contract(client, table);
            let plan = explain_bound_in(client, table);
            if table == "uuid_plain" {
                assert!(
                    plan.contains("Access Path: scan.seq"),
                    "non-indexed UUID predicate must advertise a scan:\n{plan}"
                );
            } else {
                assert!(
                    plan.contains("Access Path: scan.index")
                        && !plan.contains("Access Path: scan.seq")
                        && !plan.contains(":id"),
                    "hot UUID index predicate must advertise its executable path:\n{plan}"
                );
            }
        }

        command(client, "PRAGMA CHECKPOINT");
        for table in TABLES {
            assert_uuid_in_contract(client, table);
            let plan = explain_bound_in(client, table);
            if table == "uuid_plain" {
                assert!(
                    plan.contains("Access Path: scan.cold_artifact")
                        && !plan.contains("Access Path: scan.index"),
                    "non-indexed cold UUID predicate must remain a scan:\n{plan}"
                );
            } else {
                assert!(
                    plan.contains("Access Path: scan.index")
                        && plan.contains("Cold Access Path: volume.exact_index")
                        && !plan.contains("Access Path: scan.cold_artifact"),
                    "indexed cold UUID IN must use persisted exact postings:\n{plan}"
                );
            }
            insert(client, table, 3, UUID_C);
            assert_eq!(
                fetch_uuids(
                    client,
                    &format!("SELECT id FROM {table} WHERE id IN (:cold, :hot)"),
                    parameters(&[("cold", UUID_A), ("hot", UUID_C)]),
                ),
                vec![UUID_A, UUID_C],
                "mixed cold/hot UUID IN failed for {table}"
            );
        }
    });

    with_server(test_config(data_dir), |client| {
        for table in TABLES {
            assert_uuid_in_contract(client, table);
            assert_eq!(
                fetch_uuids(
                    client,
                    &format!("SELECT id FROM {table} WHERE id IN (:a, :b, :c)"),
                    parameters(&[("a", UUID_A), ("b", UUID_B), ("c", UUID_C)]),
                ),
                vec![UUID_A, UUID_B, UUID_C],
                "reopened UUID IN failed for {table}"
            );
            let plan = explain_bound_in(client, table);
            if table == "uuid_plain" {
                assert!(plan.contains("Access Path: scan.cold_artifact"), "{plan}");
            } else {
                assert!(
                    plan.contains("Access Path: scan.index")
                        && plan.contains("Cold Access Path: volume.exact_index"),
                    "reopened UUID IN must retain persisted exact postings:\n{plan}"
                );
            }
        }
    });
}

#[test]
fn bound_uuid_in_survives_wal_replay_without_close_checkpoint() {
    let temp = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        temp.path().join("wal-replay").display()
    );

    {
        let db = Database::open(&dsn).expect("open database");
        db.execute(
            "CREATE TABLE members (row_id INTEGER PRIMARY KEY, id UUID UNIQUE)",
            (),
        )
        .expect("create members");
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("checkpoint schema");
        db.execute(
            "INSERT INTO members (row_id, id) VALUES (?, ?)",
            vec![Value::integer(1), Value::uuid(UUID_B)],
        )
        .expect("insert WAL-only UUID row");
        db.close().expect("close without checkpoint");
    }

    let db = Database::open(&dsn).expect("reopen through WAL replay");
    let ids = db
        .query(
            "SELECT id FROM members WHERE id IN (?)",
            vec![Value::uuid(UUID_B)],
        )
        .expect("query replayed UUID")
        .map(|row| {
            row.expect("UUID row")
                .get::<Value>(0)
                .expect("UUID value")
                .as_uuid_bytes()
                .expect("UUID bytes")
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![UUID_B]);
    db.close().expect("close replayed database");
}
