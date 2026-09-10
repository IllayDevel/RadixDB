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

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{ClientError, Connection, ExecuteResult, Row, WireValue};

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

fn with_one_connection_server(
    config: ServerConfig,
    database: &str,
    work: impl FnOnce(&mut Connection),
) {
    let server = Server::bind_ephemeral(&config).expect("server binds");
    let address = server.local_addr().expect("local addr");
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect_and_select(address, database);
        work(&mut client);
        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}

fn connect_and_select(address: SocketAddr, database: &str) -> Connection {
    let mut client = Connection::connect(address).expect("client connects");
    client.authenticate("root", None).expect("auth");
    client.select_database(database).expect("select database");
    client
}

fn assert_command(result: ExecuteResult) {
    assert!(
        matches!(result, ExecuteResult::CommandComplete { .. }),
        "expected command completion, got {result:?}"
    );
}

fn uuid(byte: u8) -> [u8; 16] {
    [
        0x01, 0x93, 0x00, 0x00, 0x00, byte, 0x70, 0x00, 0x80, byte, 0x00, 0x00, 0x00, 0x00, 0x00,
        byte,
    ]
}

fn insert_user(
    client: &mut Connection,
    id: [u8; 16],
    email: &str,
    deleted_at: WireValue,
) -> Result<ExecuteResult, ClientError> {
    let mut params = BTreeMap::new();
    params.insert("id".to_string(), WireValue::Uuid(id));
    params.insert("email".to_string(), WireValue::String(email.to_string()));
    params.insert("deleted_at".to_string(), deleted_at);
    client.execute_with_parameters(
        "INSERT INTO users (id, email, __raf_deleted_at)
         VALUES (:id, :email, :deleted_at)",
        params,
    )
}

fn soft_delete_user(
    client: &mut Connection,
    id: [u8; 16],
    deleted_at_millis: i64,
) -> Result<ExecuteResult, ClientError> {
    let mut params = BTreeMap::new();
    params.insert("id".to_string(), WireValue::Uuid(id));
    params.insert(
        "deleted_at".to_string(),
        WireValue::DateTime {
            millis_since_unix_epoch_utc: deleted_at_millis,
        },
    );
    client.execute_with_parameters(
        "UPDATE users
         SET __raf_deleted_at = :deleted_at
         WHERE id = :id",
        params,
    )
}

fn restore_user_to_active(
    client: &mut Connection,
    id: [u8; 16],
) -> Result<ExecuteResult, ClientError> {
    let mut params = BTreeMap::new();
    params.insert("id".to_string(), WireValue::Uuid(id));
    client.execute_with_parameters(
        "UPDATE users
         SET __raf_deleted_at = NULL
         WHERE id = :id",
        params,
    )
}

fn fetch_single_value(client: &mut Connection, sql: &str) -> WireValue {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("select") else {
        panic!("select should open cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch");
    assert!(batch.eof);
    assert_eq!(batch.rows.len(), 1);
    let Row { values } = batch.rows.into_iter().next().expect("single row");
    assert_eq!(values.len(), 1);
    values.into_iter().next().expect("single value")
}

fn fetch_count(client: &mut Connection, sql: &str) -> i64 {
    match fetch_single_value(client, sql) {
        WireValue::Int(value) => value,
        value => panic!("expected integer count, got {value:?}"),
    }
}

fn assert_unique_error(result: Result<ExecuteResult, ClientError>) {
    let error = result.expect_err("duplicate active row must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("unique constraint"),
        "expected unique constraint error, got: {message}"
    );
}

fn show_index_options(client: &mut Connection, index_name: &str) -> String {
    let ExecuteResult::Cursor(cursor) = client
        .execute("SHOW INDEXES FROM users")
        .expect("show indexes")
    else {
        panic!("SHOW INDEXES should open cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch indexes");
    assert!(batch.eof);
    for Row { values } in batch.rows {
        if values.get(1) == Some(&WireValue::String(index_name.to_string())) {
            let Some(WireValue::String(options)) = values.get(5) else {
                panic!("SHOW INDEXES options column should be a string, got row {values:?}");
            };
            return options.clone();
        }
    }
    panic!("{index_name} not found in SHOW INDEXES");
}

#[test]
fn partial_unique_soft_delete_contract_survives_public_tcp_client_restart() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let email = "same@example.test";

    with_one_connection_server(test_config(data_dir.clone()), "raf_public", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE users (
                        id UUID PRIMARY KEY,
                        email TEXT NOT NULL,
                        __raf_deleted_at TIMESTAMP
                    )",
                )
                .expect("create users table"),
        );
        assert_command(
            client
                .execute(
                    "CREATE UNIQUE INDEX users_email_active_idx
                     ON users (email)
                     WHERE __raf_deleted_at IS NULL",
                )
                .expect("create partial unique index"),
        );

        assert_command(
            insert_user(client, uuid(1), email, WireValue::Null).expect("insert active user"),
        );
        assert_unique_error(insert_user(client, uuid(2), email, WireValue::Null));

        assert_eq!(
            soft_delete_user(client, uuid(1), 1_735_689_600_000).expect("soft delete user"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 0,
            }
        );
        assert_command(
            insert_user(client, uuid(2), email, WireValue::Null)
                .expect("insert replacement active user"),
        );
        assert_unique_error(restore_user_to_active(client, uuid(1)));

        assert_eq!(
            fetch_count(
                client,
                "SELECT COUNT(*) FROM users WHERE email = 'same@example.test'"
            ),
            2,
            "query without partial predicate must still see deleted and active rows"
        );
        assert_eq!(
            fetch_count(
                client,
                "SELECT COUNT(*) FROM users
                 WHERE email = 'same@example.test' AND __raf_deleted_at IS NULL"
            ),
            1,
            "query with matching partial predicate sees only active row"
        );

        let options = show_index_options(client, "users_email_active_idx");
        assert!(
            options.contains("where=") && options.contains("__raf_deleted_at IS NULL"),
            "SHOW INDEXES should expose partial predicate, got: {options}"
        );
    });

    with_one_connection_server(test_config(data_dir), "raf_public", |client| {
        let options = show_index_options(client, "users_email_active_idx");
        assert!(
            options.contains("where=") && options.contains("__raf_deleted_at IS NULL"),
            "partial predicate must survive restart/reopen, got: {options}"
        );
        assert_unique_error(insert_user(client, uuid(3), email, WireValue::Null));
        assert_eq!(
            fetch_count(
                client,
                "SELECT COUNT(*) FROM users
                 WHERE email = 'same@example.test' AND __raf_deleted_at IS NULL"
            ),
            1,
            "partial unique membership must survive restart/reopen"
        );
    });
}
