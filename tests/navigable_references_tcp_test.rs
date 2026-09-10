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

//! NR-13 embedded, TCP and prepared-query parity for navigable references.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread;

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::{Database, NavigationErrorCode, Result, Value};
use radixdb_client::{
    ClientError, Column, Connection, ExecuteResult, ProtocolErrorCode, Row, WireValue,
};

const SIMPLE_QUERY: &str = "SELECT r.target_id.label, r.target_id.label AS target_label
     FROM nr13_roots r
     WHERE r.id >= 1
     ORDER BY r.id";
const PREPARED_QUERY: &str = "SELECT r.target_id.label, r.target_id.label AS target_label
     FROM nr13_roots r
     WHERE r.id >= $1
     ORDER BY r.id";

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

fn connect(address: SocketAddr, database: &str) -> Connection {
    let mut client = Connection::connect(address).expect("connect navigation client");
    client.authenticate("root", None).expect("authenticate");
    client.select_database(database).expect("select database");
    client
}

fn setup_embedded(db: &Database) -> Result<()> {
    for sql in [
        "CREATE TABLE nr13_targets (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        "CREATE TABLE nr13_roots (
            id INTEGER PRIMARY KEY,
            target_id INTEGER REFERENCES nr13_targets(id)
        )",
        "INSERT INTO nr13_targets VALUES (1, 'one'), (2, 'two')",
        "INSERT INTO nr13_roots VALUES (1, 1), (2, NULL), (3, 2)",
    ] {
        db.execute(sql, ())?;
    }
    Ok(())
}

fn embedded_result(db: &Database, prepared: bool) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let rows = if prepared {
        db.prepare(PREPARED_QUERY)?.query((1,))?
    } else {
        db.query(SIMPLE_QUERY, ())?
    };
    let columns = rows.columns().to_vec();
    let values = rows
        .map(|row| row.map(|row| row.into_inner().into_values()))
        .collect::<Result<Vec<_>>>()?;
    Ok((columns, values))
}

fn assert_command(client: &mut Connection, sql: &str) {
    assert!(matches!(
        client.execute(sql).expect("fixture statement"),
        ExecuteResult::CommandComplete { .. }
    ));
}

fn collect_tcp(
    client: &mut Connection,
    result: ExecuteResult,
) -> (Vec<Column>, Vec<Vec<WireValue>>) {
    let ExecuteResult::Cursor(cursor) = result else {
        panic!("navigation SELECT did not return a cursor")
    };
    let columns = cursor.columns().to_vec();
    let mut rows = Vec::new();
    loop {
        let batch = client.fetch(&cursor).expect("fetch navigation cursor");
        rows.extend(batch.rows.into_iter().map(|Row { values }| values));
        if batch.eof {
            return (columns, rows);
        }
    }
}

fn navigation_failure(error: ClientError, expected: NavigationErrorCode) {
    let ClientError::Server(failure) = error else {
        panic!("expected SQL error, got {error}")
    };
    assert_eq!(failure.code, ProtocolErrorCode::SqlError);
    assert!(
        failure.message.starts_with(expected.as_str()),
        "unexpected navigation error: {}",
        failure.message
    );
}

#[test]
fn embedded_tcp_and_prepared_navigation_have_identical_values_and_metadata() {
    let embedded = Database::open("memory://navigation_nr13_embedded").unwrap();
    setup_embedded(&embedded).unwrap();
    let simple_embedded = embedded_result(&embedded, false).unwrap();
    let prepared_embedded = embedded_result(&embedded, true).unwrap();
    assert_eq!(simple_embedded, prepared_embedded);
    assert_eq!(simple_embedded.0, ["r.target_id.label", "target_label"]);
    assert_eq!(
        simple_embedded.1,
        [
            vec![Value::from("one"), Value::from("one")],
            vec![Value::Null(radixdb::DataType::Text); 2],
            vec![Value::from("two"), Value::from("two")],
        ]
    );

    let temp = tempfile::tempdir().unwrap();
    let config = test_config(temp.path().join("data"));
    let server = Server::bind_ephemeral(&config).unwrap();
    let address = server.local_addr().unwrap();
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address, "navigation_nr13_tcp");
        for sql in [
            "CREATE TABLE nr13_targets (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            "CREATE TABLE nr13_roots (
                id INTEGER PRIMARY KEY,
                target_id INTEGER REFERENCES nr13_targets(id)
            )",
            "INSERT INTO nr13_targets VALUES (1, 'one'), (2, 'two')",
            "INSERT INTO nr13_roots VALUES (1, 1), (2, NULL), (3, 2)",
        ] {
            assert_command(&mut client, sql);
        }

        let simple = client.execute(SIMPLE_QUERY).expect("simple navigation");
        let simple_tcp = collect_tcp(&mut client, simple);
        let prepared = client.prepare(PREPARED_QUERY).expect("prepare navigation");
        let prepared_result = client
            .execute_prepared(&prepared, vec![WireValue::Int(1)])
            .expect("execute prepared navigation");
        let prepared_tcp = collect_tcp(&mut client, prepared_result);
        assert_eq!(simple_tcp, prepared_tcp);
        assert_eq!(
            simple_tcp.0,
            [
                Column {
                    name: "r.target_id.label".to_string(),
                    type_name: "TEXT".to_string(),
                    nullable: true,
                    external_type: None,
                },
                Column {
                    name: "target_label".to_string(),
                    type_name: "TEXT".to_string(),
                    nullable: true,
                    external_type: None,
                },
            ]
        );
        assert_eq!(
            simple_tcp.1,
            [
                vec![
                    WireValue::String("one".to_string()),
                    WireValue::String("one".to_string()),
                ],
                vec![WireValue::Null, WireValue::Null],
                vec![
                    WireValue::String("two".to_string()),
                    WireValue::String("two".to_string()),
                ],
            ]
        );

        assert_command(
            &mut client,
            "ALTER TABLE nr13_targets RENAME COLUMN label TO title",
        );
        navigation_failure(
            client.execute(SIMPLE_QUERY).expect_err("stale simple path"),
            NavigationErrorCode::TargetColumnNotFound,
        );
        navigation_failure(
            client
                .execute_prepared(&prepared, vec![WireValue::Int(1)])
                .expect_err("stale prepared path"),
            NavigationErrorCode::TargetColumnNotFound,
        );

        let rebound_sql = "SELECT r.target_id.title FROM nr13_roots r WHERE r.id = 1";
        let rebound = client.execute(rebound_sql).expect("rebound simple path");
        assert_eq!(
            collect_tcp(&mut client, rebound).1,
            [vec![WireValue::String("one".to_string())]]
        );

        drop(client);
        server_worker
            .join()
            .expect("navigation server thread")
            .expect("navigation server shutdown");
    });
}

#[test]
fn ambiguous_join_projection_is_explicit_and_keeps_tcp_session_usable() {
    let temp = tempfile::tempdir().unwrap();
    let config = test_config(temp.path().join("ambiguous-join"));
    let server = Server::bind_ephemeral(&config).unwrap();
    let address = server.local_addr().unwrap();
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address, "ambiguous_join_projection_tcp");
        for sql in [
            "CREATE TABLE publications (id INTEGER PRIMARY KEY, upstream_id INTEGER)",
            "CREATE TABLE streams (id INTEGER PRIMARY KEY, upstream_id INTEGER)",
            "INSERT INTO publications VALUES (1, 7)",
            "INSERT INTO streams VALUES (2, 7)",
        ] {
            assert_command(&mut client, sql);
        }

        let error = client
            .execute(
                "SELECT id, upstream_id
                 FROM publications p
                 INNER JOIN streams s ON s.upstream_id = p.upstream_id",
            )
            .expect_err("unqualified duplicate projection must be rejected");
        let ClientError::Server(failure) = error else {
            panic!("expected server SQL error, got {error}")
        };
        assert_eq!(failure.code, ProtocolErrorCode::SqlError);
        assert_eq!(failure.message, "column 'id' is ambiguous");

        let qualified = client
            .execute(
                "SELECT p.id, p.upstream_id
                 FROM publications p
                 INNER JOIN streams s ON s.upstream_id = p.upstream_id",
            )
            .expect("qualified retry on the same TCP session");
        assert_eq!(
            collect_tcp(&mut client, qualified).1,
            [vec![WireValue::Int(1), WireValue::Int(7)]]
        );

        drop(client);
        server_worker
            .join()
            .expect("ambiguous projection server thread")
            .expect("ambiguous projection server shutdown");
    });
}
