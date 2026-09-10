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

//! RDB-0032: outer joins must publish result nullability, not base-table
//! nullability, through the official TCP client.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread;

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{Column, Connection, ExecuteResult, Row, WireValue};

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 4,
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

fn with_server(config: ServerConfig, work: impl FnOnce(&mut Connection)) {
    let server = Server::bind_ephemeral(&config).expect("bind RDB-0032 server");
    let address = server.local_addr().expect("RDB-0032 server address");
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address);
        work(&mut client);
        drop(client);
        server_worker
            .join()
            .expect("RDB-0032 server thread")
            .expect("RDB-0032 server shutdown");
    });
}

fn connect(address: SocketAddr) -> Connection {
    let mut client = Connection::connect(address).expect("connect RDB-0032 client");
    client.authenticate("root", None).expect("authenticate");
    client
        .select_database("rdb_0032")
        .expect("select RDB-0032 database");
    client
}

fn command(client: &mut Connection, sql: &str) {
    let result = client.execute(sql).expect("RDB-0032 fixture statement");
    assert!(
        matches!(result, ExecuteResult::CommandComplete { .. }),
        "RDB-0032 fixture statement returned a cursor: {sql}: {result:?}"
    );
}

fn query(client: &mut Connection, result: ExecuteResult) -> (Vec<Column>, Vec<Vec<WireValue>>) {
    let ExecuteResult::Cursor(cursor) = result else {
        panic!("RDB-0032 SELECT must open a cursor")
    };
    let columns = cursor.columns().to_vec();
    let mut rows = Vec::new();
    loop {
        // Before the fix, the official client fails here with
        // InvalidBatchShape because the row carries NULL while the cursor
        // metadata claims the outer-side column is non-nullable.
        let batch = client.fetch(&cursor).expect("fetch RDB-0032 cursor");
        rows.extend(batch.rows.into_iter().map(|Row { values }| values));
        if batch.eof {
            return (columns, rows);
        }
    }
}

fn nullability(columns: &[Column]) -> Vec<bool> {
    columns.iter().map(|column| column.nullable).collect()
}

fn setup(client: &mut Connection) {
    for sql in [
        "CREATE TABLE rdb32_left (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )",
        "CREATE TABLE rdb32_right (
            id INTEGER PRIMARY KEY,
            left_id INTEGER NOT NULL REFERENCES rdb32_left(id),
            value TEXT NOT NULL,
            detail TEXT NOT NULL
        )",
        "CREATE TABLE rdb32_leaf (
            id INTEGER PRIMARY KEY,
            right_id INTEGER NOT NULL REFERENCES rdb32_right(id),
            label TEXT NOT NULL
        )",
        "INSERT INTO rdb32_left VALUES (1, 'unmatched'), (2, 'matched')",
        "INSERT INTO rdb32_right VALUES (20, 2, 'right-value', 'right-value')",
        "INSERT INTO rdb32_leaf VALUES (200, 20, 'leaf-value')",
        "CREATE VIEW rdb32_outer_view AS
         SELECT l.id AS left_id, r.value AS optional_value
         FROM rdb32_left l
         LEFT JOIN rdb32_right r ON r.left_id = l.id",
    ] {
        command(client, sql);
    }
}

fn assert_left_join_prepared(client: &mut Connection, left_id: i64, expected: WireValue) {
    let statement = client
        .prepare(
            "SELECT l.id AS left_id, r.value AS nullable_value, r.detail AS nullable_detail
             FROM rdb32_left l
             LEFT JOIN rdb32_right r ON r.left_id = l.id
             WHERE l.id = $1",
        )
        .expect("prepare LEFT JOIN");
    let result = client
        .execute_prepared(&statement, vec![WireValue::Int(left_id)])
        .expect("execute prepared LEFT JOIN");
    let (columns, rows) = query(client, result);
    assert_eq!(nullability(&columns), [false, true, true]);
    assert_eq!(
        rows,
        [vec![WireValue::Int(left_id), expected.clone(), expected]]
    );
}

fn assert_reopened_view(client: &mut Connection) {
    let result = client
        .execute(
            "SELECT v.left_id, v.optional_value
             FROM rdb32_outer_view v
             WHERE v.left_id = 1",
        )
        .expect("query outer-join view");
    let (columns, rows) = query(client, result);
    assert_eq!(nullability(&columns), [false, true]);
    assert_eq!(rows, [vec![WireValue::Int(1), WireValue::Null]]);
}

#[test]
fn outer_join_wire_schema_survives_parameters_views_checkpoint_and_reopen() {
    let temp = tempfile::tempdir().expect("RDB-0032 temp dir");
    let data_dir = temp.path().join("data");

    with_server(test_config(data_dir.clone()), |client| {
        setup(client);

        assert_left_join_prepared(client, 1, WireValue::Null);
        assert_left_join_prepared(client, 2, WireValue::String("right-value".to_string()));

        let result = client
            .execute(
                "SELECT l.id AS nullable_left, r.id AS right_id, r.value AS right_value
                 FROM rdb32_left l
                 RIGHT JOIN rdb32_right r ON r.left_id = l.id AND l.id = -1
                 WHERE r.id = 20",
            )
            .expect("execute RIGHT JOIN");
        let (columns, rows) = query(client, result);
        assert_eq!(nullability(&columns), [true, false, false]);
        assert_eq!(
            rows,
            [vec![
                WireValue::Null,
                WireValue::Int(20),
                WireValue::String("right-value".to_string()),
            ]]
        );

        let result = client
            .execute(
                "SELECT l.id AS left_id, r.id AS right_id
                 FROM rdb32_left l
                 FULL JOIN rdb32_right r ON r.left_id = l.id AND l.id = -1",
            )
            .expect("execute FULL JOIN");
        let (columns, rows) = query(client, result);
        assert_eq!(nullability(&columns), [true, true]);
        assert!(rows.iter().any(|row| row[0] == WireValue::Null));
        assert!(rows.iter().any(|row| row[1] == WireValue::Null));

        let result = client
            .execute(
                "SELECT l.label, r.value, leaf.label
                 FROM rdb32_left l
                 LEFT JOIN rdb32_right r ON r.left_id = l.id
                 LEFT JOIN rdb32_leaf leaf ON leaf.right_id = r.id
                 WHERE l.id = 1",
            )
            .expect("execute nested LEFT JOIN");
        let (columns, rows) = query(client, result);
        assert_eq!(nullability(&columns), [false, true, true]);
        assert_eq!(
            rows,
            [vec![
                WireValue::String("unmatched".to_string()),
                WireValue::Null,
                WireValue::Null,
            ]]
        );

        assert_reopened_view(client);

        assert!(
            client
                .execute("INSERT INTO rdb32_right VALUES (21, 2, NULL, 'detail')")
                .is_err(),
            "result metadata widening must not weaken base-table NOT NULL"
        );
        let checkpoint = client
            .execute("PRAGMA CHECKPOINT")
            .expect("checkpoint RDB-0032 fixture");
        let _ = query(client, checkpoint);
    });

    with_server(test_config(data_dir), |client| {
        assert_left_join_prepared(client, 1, WireValue::Null);
        assert_reopened_view(client);
    });
}
