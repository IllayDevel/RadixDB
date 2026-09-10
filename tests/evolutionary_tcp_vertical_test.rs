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

//! Permanent TCP boundary oracle for the evolutionary crate migration.
//!
//! Embedded semantics live in `evolutionary_embedded_vertical_test`. This
//! target covers only additional wire/session owners: transaction state,
//! typed wire values, SQL error mapping, session reuse and server-owned reopen.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread;

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{
    ClientError, Connection, ExecuteResult, ProtocolErrorCode, Row, TransactionState, WireValue,
};

const DATABASE_NAME: &str = "evolutionary_tcp_vertical";
const JOIN_QUERY: &str = "
    SELECT a.id, a.name, o.id, o.state
    FROM tcp_accounts a
    INNER JOIN tcp_orders o ON o.account_id = a.id
    ORDER BY a.id, o.id
";

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 2,
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
    let mut client = Connection::connect(address).expect("connect TCP vertical client");
    client.authenticate("root", None).expect("authenticate");
    client
        .select_database(DATABASE_NAME)
        .expect("select vertical database");
    client
}

fn command(client: &mut Connection, sql: &str) {
    assert!(
        matches!(
            client.execute(sql).expect("execute vertical command"),
            ExecuteResult::CommandComplete { .. }
        ),
        "statement must complete without opening a cursor: {sql}"
    );
}

fn complete(client: &mut Connection, sql: &str) {
    match client.execute(sql).expect("execute vertical operation") {
        ExecuteResult::CommandComplete { .. } => {}
        ExecuteResult::Cursor(cursor) => loop {
            if client
                .fetch(&cursor)
                .expect("fetch vertical operation cursor")
                .eof
            {
                break;
            }
        },
    }
}

fn rows(client: &mut Connection, sql: &str) -> Vec<Vec<WireValue>> {
    let result = client.execute(sql).expect("execute vertical query");
    let ExecuteResult::Cursor(cursor) = result else {
        panic!("query did not return a cursor: {sql}");
    };
    let mut result = Vec::new();
    loop {
        let batch = client.fetch(&cursor).expect("fetch vertical cursor");
        result.extend(batch.rows.into_iter().map(|Row { values }| values));
        if batch.eof {
            return result;
        }
    }
}

fn expected_rows() -> Vec<Vec<WireValue>> {
    vec![
        vec![
            WireValue::Int(1),
            WireValue::String("alpha".to_string()),
            WireValue::Int(10),
            WireValue::String("paid".to_string()),
        ],
        vec![
            WireValue::Int(1),
            WireValue::String("alpha".to_string()),
            WireValue::Int(11),
            WireValue::String("open".to_string()),
        ],
    ]
}

#[test]
fn evo_00_tcp_session_contract_survives_server_reopen() {
    let directory = tempfile::tempdir().expect("create TCP vertical directory");
    let config = test_config(directory.path().join("server-data"));

    {
        let server = Server::bind_ephemeral(&config).expect("bind first server");
        let address = server.local_addr().expect("first server address");
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.serve_one());
            let mut client = connect(address);

            command(
                &mut client,
                "CREATE TABLE tcp_accounts (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )",
            );
            command(
                &mut client,
                "CREATE TABLE tcp_orders (
                    id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL REFERENCES tcp_accounts(id),
                    state TEXT NOT NULL
                )",
            );

            client.begin().expect("begin committed transaction");
            assert_eq!(client.transaction_state(), TransactionState::Active);
            command(&mut client, "INSERT INTO tcp_accounts VALUES (1, 'alpha')");
            command(
                &mut client,
                "INSERT INTO tcp_orders VALUES (10, 1, 'open'), (11, 1, 'open')",
            );
            command(
                &mut client,
                "UPDATE tcp_orders SET state = 'paid' WHERE id = 10",
            );
            assert_eq!(rows(&mut client, JOIN_QUERY), expected_rows());
            client.commit().expect("commit TCP transaction");
            assert_eq!(client.transaction_state(), TransactionState::Inactive);

            client.begin().expect("begin rolled-back transaction");
            command(
                &mut client,
                "INSERT INTO tcp_accounts VALUES (2, 'transient')",
            );
            client.rollback().expect("roll back TCP transaction");
            assert_eq!(client.transaction_state(), TransactionState::Inactive);

            let error = client
                .execute("SELECT missing_column FROM tcp_accounts")
                .expect_err("invalid column must return a SQL error");
            let ClientError::Server(failure) = error else {
                panic!("SQL failure lost its server error identity: {error}");
            };
            assert_eq!(failure.code, ProtocolErrorCode::SqlError);

            assert_eq!(rows(&mut client, JOIN_QUERY), expected_rows());
            assert_eq!(
                rows(
                    &mut client,
                    "SELECT COUNT(*) FROM tcp_accounts WHERE id = 2"
                ),
                [vec![WireValue::Int(0)]]
            );
            complete(&mut client, "PRAGMA CHECKPOINT");

            drop(client);
            worker
                .join()
                .expect("first server thread")
                .expect("first server shutdown");
        });
    }

    {
        let server = Server::bind_ephemeral(&config).expect("bind reopened server");
        let address = server.local_addr().expect("reopened server address");
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.serve_one());
            let mut client = connect(address);

            assert_eq!(client.transaction_state(), TransactionState::Inactive);
            assert_eq!(rows(&mut client, JOIN_QUERY), expected_rows());
            assert_eq!(
                rows(&mut client, "SELECT COUNT(*) FROM tcp_accounts"),
                [vec![WireValue::Int(1)]]
            );

            drop(client);
            worker
                .join()
                .expect("reopened server thread")
                .expect("reopened server shutdown");
        });
    }
}
