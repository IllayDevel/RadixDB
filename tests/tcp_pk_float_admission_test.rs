// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! TCP regression oracle for AUD-CONTRACT-205.
//!
//! Repeating the same parameterized SQL exercises the server-side compiled
//! statement cache. Every execution must admit the current wire value again;
//! a fractional/out-of-range Float must never become a cached integer key.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{Connection, ExecuteResult, WireValue};

const DATABASE: &str = "tcp_pk_float_admission";
const EXACT: i64 = 1_i64 << 53;
const NEIGHBOR: i64 = EXACT + 1;

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
    assert!(
        matches!(
            connection
                .execute(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}")),
            ExecuteResult::CommandComplete { .. }
        ),
        "expected command completion: {sql}"
    );
}

fn id_parameter(value: f64) -> BTreeMap<String, WireValue> {
    BTreeMap::from([("id".to_string(), WireValue::Float64(value))])
}

fn float_parameters(values: &[(&str, f64)]) -> BTreeMap<String, WireValue> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_string(), WireValue::Float64(*value)))
        .collect()
}

fn update_parameters(id: f64, payload: &str) -> BTreeMap<String, WireValue> {
    BTreeMap::from([
        ("id".to_string(), WireValue::Float64(id)),
        (
            "payload".to_string(),
            WireValue::String(payload.to_string()),
        ),
    ])
}

fn affected_rows(
    connection: &mut Connection,
    sql: &str,
    parameters: BTreeMap<String, WireValue>,
) -> u64 {
    match connection
        .execute_with_parameters(sql, parameters)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    {
        ExecuteResult::CommandComplete { affected_rows, .. } => affected_rows,
        ExecuteResult::Cursor(_) => panic!("DML unexpectedly returned a cursor: {sql}"),
    }
}

fn query_ids(
    connection: &mut Connection,
    sql: &str,
    parameters: BTreeMap<String, WireValue>,
) -> Vec<i64> {
    let ExecuteResult::Cursor(cursor) = connection
        .execute_with_parameters(sql, parameters)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("query must return a cursor: {sql}")
    };

    let mut ids = Vec::new();
    loop {
        let batch = connection.fetch(&cursor).expect("fetch PK query");
        for row in batch.rows {
            let Some(WireValue::Int(id)) = row.values.first() else {
                panic!("expected one INTEGER id, got {:?}", row.values)
            };
            ids.push(*id);
        }
        if batch.eof {
            return ids;
        }
    }
}

#[test]
fn tcp_repeated_float_pk_parameters_are_re_admitted_for_select_update_delete() {
    let temp = tempfile::tempdir().expect("temporary server directory");
    let server =
        Server::bind_ephemeral(&config(temp.path().join("data"))).expect("bind test server");
    let address = server.local_addr().expect("test server address");

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut connection = Connection::connect(address).expect("connect TCP client");
        connection.authenticate("root", None).expect("authenticate");
        connection
            .select_database(DATABASE)
            .expect("select test database");

        command(
            &mut connection,
            "CREATE TABLE pk_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        );
        command(
            &mut connection,
            "INSERT INTO pk_rows VALUES
             (0, 'zero'),
             (9007199254740992, 'exact'),
             (9007199254740993, 'neighbor'),
             (9223372036854775807, 'maximum')",
        );

        let select = "SELECT * FROM pk_rows WHERE id = :id";
        assert_eq!(
            query_ids(&mut connection, select, id_parameter(EXACT as f64)),
            vec![EXACT]
        );
        assert!(query_ids(&mut connection, select, id_parameter(0.5)).is_empty());
        assert!(query_ids(&mut connection, select, id_parameter(i64::MAX as f64),).is_empty());

        // NEIGHBOR cannot be represented by f64 and rounds to EXACT. It must
        // resolve to the exact Float value's INTEGER identity, never NEIGHBOR.
        assert_eq!((NEIGHBOR as f64), EXACT as f64);
        assert_eq!(
            query_ids(&mut connection, select, id_parameter(NEIGHBOR as f64)),
            vec![EXACT]
        );
        assert_eq!(
            query_ids(&mut connection, select, id_parameter(EXACT as f64)),
            vec![EXACT]
        );

        // Projecting only the PK uses the ordinary storage pushdown path rather
        // than the SELECT * PK shortcut. It must preserve the Float parameter
        // identity instead of truncating it to INTEGER during pushdown.
        let projected_select = "SELECT id FROM pk_rows WHERE id = :id";
        assert_eq!(
            query_ids(
                &mut connection,
                projected_select,
                id_parameter(EXACT as f64),
            ),
            vec![EXACT]
        );
        assert!(query_ids(&mut connection, projected_select, id_parameter(0.5)).is_empty());
        assert!(query_ids(
            &mut connection,
            projected_select,
            id_parameter(i64::MAX as f64),
        )
        .is_empty());

        let between = "SELECT id FROM pk_rows WHERE id BETWEEN :low AND :high";
        assert_eq!(
            query_ids(
                &mut connection,
                between,
                float_parameters(&[("low", EXACT as f64), ("high", EXACT as f64)]),
            ),
            vec![EXACT]
        );
        assert!(query_ids(
            &mut connection,
            between,
            float_parameters(&[("low", 0.5), ("high", 0.5)]),
        )
        .is_empty());
        assert!(query_ids(
            &mut connection,
            between,
            float_parameters(&[("low", i64::MAX as f64), ("high", i64::MAX as f64),]),
        )
        .is_empty());

        let reversed_between = "SELECT id FROM pk_rows WHERE :low <= id AND :high >= id";
        assert_eq!(
            query_ids(
                &mut connection,
                reversed_between,
                float_parameters(&[("low", EXACT as f64), ("high", EXACT as f64)]),
            ),
            vec![EXACT]
        );
        assert!(query_ids(
            &mut connection,
            reversed_between,
            float_parameters(&[("low", 0.5), ("high", 0.5)]),
        )
        .is_empty());

        let in_list = "SELECT id FROM pk_rows WHERE id IN (:first, :second)";
        assert_eq!(
            query_ids(
                &mut connection,
                in_list,
                float_parameters(&[("first", EXACT as f64), ("second", 0.5)]),
            ),
            vec![EXACT]
        );
        assert!(query_ids(
            &mut connection,
            in_list,
            float_parameters(&[("first", 0.5), ("second", i64::MAX as f64)]),
        )
        .is_empty());

        let update = "UPDATE pk_rows SET payload = :payload WHERE id = :id";
        assert_eq!(
            affected_rows(
                &mut connection,
                update,
                update_parameters(EXACT as f64, "updated"),
            ),
            1
        );
        assert_eq!(
            affected_rows(
                &mut connection,
                update,
                update_parameters(0.5, "must-not-touch-zero"),
            ),
            0
        );
        assert_eq!(
            affected_rows(
                &mut connection,
                update,
                update_parameters(i64::MAX as f64, "must-not-touch-maximum"),
            ),
            0
        );
        assert_eq!(
            affected_rows(
                &mut connection,
                update,
                update_parameters(NEIGHBOR as f64, "rounded-to-exact"),
            ),
            1
        );

        let delete = "DELETE FROM pk_rows WHERE id = :id";
        assert_eq!(affected_rows(&mut connection, delete, id_parameter(0.5)), 0);
        assert_eq!(
            affected_rows(&mut connection, delete, id_parameter(i64::MAX as f64)),
            0
        );
        assert_eq!(
            affected_rows(&mut connection, delete, id_parameter(EXACT as f64)),
            1
        );

        assert_eq!(
            query_ids(
                &mut connection,
                "SELECT id FROM pk_rows ORDER BY id",
                BTreeMap::new(),
            ),
            vec![0, NEIGHBOR, i64::MAX]
        );

        connection.shutdown().expect("shutdown TCP client");
        worker
            .join()
            .expect("join test server")
            .expect("serve TCP client");
    });
}
