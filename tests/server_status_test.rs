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
    fs,
    net::{IpAddr, Ipv4Addr},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::Database;
use radixdb_client::{
    ClientError, Connection, ExecuteResult, ProtocolErrorCode, ServerLifecycleState,
    PROTOCOL_VERSION,
};

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

#[test]
fn r2_l01_batch_b_public_tcp_status_owns_database_startup_outcome() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("server-data");
    let disk_database = data_dir.join("databases").join("disk_only");
    let disk_dsn = format!("file://{}", disk_database.display());
    {
        let database = Database::open(&disk_dsn).expect("create persistent fixture");
        database
            .execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)",
                (),
            )
            .expect("create fixture table");
        database
            .execute("INSERT INTO orders VALUES (1, 'persisted')", ())
            .expect("insert fixture row");
        database
            .execute("PRAGMA CHECKPOINT", ())
            .expect("publish fixture volume");
        database.close().expect("close persistent fixture");
    }

    let broken_database = data_dir.join("databases").join("broken");
    fs::create_dir_all(&broken_database).expect("create broken database fixture");
    fs::write(broken_database.join("wal"), b"not a directory")
        .expect("create deterministic startup failure");

    let config = test_config(data_dir);
    let server = Server::bind_ephemeral(&config).expect("server binds");
    let address = server.local_addr().expect("local addr");

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());

        let mut client = Connection::connect(address).expect("client connects");
        client.authenticate("root", None).expect("auth");

        let server_status = client.server_status().expect("server status");
        let identity = server_status
            .build
            .as_ref()
            .expect("BuildIdentityV1 must be negotiated by the reusable client");
        assert_eq!(identity.semantic_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(identity.protocol_version, PROTOCOL_VERSION);
        let revision = identity
            .git_revision
            .strip_suffix("-dirty")
            .unwrap_or(&identity.git_revision);
        assert!(
            (revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()))
                || revision.starts_with("source-")
        );
        assert_eq!(server_status.lifecycle, ServerLifecycleState::Ready);
        assert!(server_status.ready);
        assert!(
            server_status
                .databases
                .iter()
                .any(|database| database.name == "disk_only"
                    && database.lifecycle == ServerLifecycleState::Starting
                    && !database.ready),
            "server status should expose unopened disk databases: {server_status:?}"
        );

        let disk_status = client
            .database_status("disk_only")
            .expect("disk database status");
        assert_eq!(disk_status.lifecycle, ServerLifecycleState::Starting);
        assert!(!disk_status.ready);
        assert!(!disk_status.databases[0].artifacts.complete);

        client
            .select_database("disk_only")
            .expect("explicit selection starts database open/recovery");
        let ready_status = client
            .database_status("disk_only")
            .expect("ready database status");
        assert_eq!(ready_status.lifecycle, ServerLifecycleState::Ready);
        assert!(ready_status.ready);
        let disk_database = &ready_status.databases[0];
        assert_eq!(disk_database.lifecycle, ServerLifecycleState::Ready);
        assert!(disk_database.ready);
        assert!(disk_database.artifacts.complete);
        assert_eq!(disk_database.artifacts.scan_errors, 0);
        assert_eq!(disk_database.artifacts.table_dirs, 1);
        assert!(disk_database.artifacts.wal_files >= 1);
        assert!(disk_database.artifacts.artifact_files >= 1);
        assert!(disk_database.artifacts.manifest_files >= 1);

        assert!(
            client.select_database("broken").is_err(),
            "explicit selection must expose deterministic recovery failure"
        );
        let failed_status = client
            .database_status("broken")
            .expect("startup failure must remain observable as status");
        assert_eq!(failed_status.lifecycle, ServerLifecycleState::Emergency);
        assert!(!failed_status.ready);
        assert_eq!(failed_status.databases.len(), 1);
        assert_eq!(
            failed_status.databases[0].lifecycle,
            ServerLifecycleState::Emergency
        );
        assert!(
            failed_status.databases[0].message.contains("wal")
                || failed_status.databases[0].message.contains("directory")
        );

        let select_error = client
            .select_database("broken")
            .expect_err("failed recovery must not select the database");
        match select_error {
            ClientError::Server(failure) => {
                assert_eq!(failure.code, ProtocolErrorCode::ServerError)
            }
            other => panic!("expected typed server startup error, got: {other}"),
        }

        client
            .select_database("ready_db")
            .expect("select/open ready db");
        assert!(
            matches!(
                client
                    .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                    .expect("create table"),
                ExecuteResult::CommandComplete { .. }
            ),
            "DDL should complete"
        );

        let ready_status = client
            .database_status("ready_db")
            .expect("ready database status");
        assert_eq!(ready_status.lifecycle, ServerLifecycleState::Ready);
        assert!(ready_status.ready);
        assert_eq!(
            ready_status.databases[0].lifecycle,
            ServerLifecycleState::Ready
        );
        assert!(ready_status.databases[0].ready);

        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}
