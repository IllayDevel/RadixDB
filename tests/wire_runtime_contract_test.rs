use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use radixdb::client::{ClientError, Connection, ExecuteResult, WireValue};
use radixdb::server::{Server, ServerConfig};

fn config(root: std::path::PathBuf, port: u16) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port,
        data_dir: root,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 8,
        max_inflight_frame_bytes: 32 * 1024 * 1024,
        max_databases: 2,
        max_database_name_bytes: 32,
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
        target_volume_rows: radixdb::server::default_target_volume_rows(),
        seal_hot_bytes_threshold: radixdb::server::default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold:
            radixdb::server::default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

fn connect(address: std::net::SocketAddr, database: &str) -> Connection {
    let mut connection = Connection::connect(address).expect("connect");
    connection.authenticate("root", None).expect("authenticate");
    connection
        .select_database(database)
        .expect("select database");
    connection
}

#[test]
fn r9_wire_prepared_savepoint_cursor_and_lifecycle_contracts() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::bind_ephemeral(&config(temp.path().join("server"), 0)).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut client = connect(address, "wire_contract");
        client
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
            .unwrap();

        let raw = client.execute("BEGIN").unwrap_err();
        assert!(matches!(raw, ClientError::Server(_)));
        assert!(!client.in_transaction());
        assert!(client.is_reusable());

        client.begin().unwrap();
        client.savepoint("before_insert").unwrap();
        let insert = client.prepare("INSERT INTO items VALUES ($1, $2)").unwrap();
        client
            .execute_prepared(
                &insert,
                vec![WireValue::Int(1), WireValue::String("rolled".into())],
            )
            .unwrap();
        client.rollback_to_savepoint("before_insert").unwrap();
        client.release_savepoint("before_insert").unwrap();
        client.commit().unwrap();
        client.close_prepared(insert).unwrap();

        let named = client
            .prepare("INSERT INTO items VALUES (:id, :note)")
            .unwrap();
        client
            .execute_prepared_with_bindings(
                &named,
                Vec::new(),
                BTreeMap::from([
                    ("id".into(), WireValue::Int(2)),
                    ("note".into(), WireValue::String("kept".into())),
                ]),
            )
            .unwrap();
        client.close_prepared(named).unwrap();

        let ExecuteResult::Cursor(cursor) = client.execute("SELECT id, note FROM items").unwrap()
        else {
            panic!("cursor")
        };
        drop(cursor);
        assert!(client.discard_active_cursor().unwrap());
        assert!(!client.discard_active_cursor().unwrap());

        let status = client.server_status().unwrap();
        assert_eq!(status.runtime.max_databases, 2);
        assert!(status.runtime.open_databases >= 1);
        assert!(status.runtime.retained_databases >= status.runtime.open_databases);
        assert!(status.runtime.active_connections >= 1);

        client.close_database("wire_contract").unwrap();
        client.select_database("wire_contract").unwrap();
        let ExecuteResult::Cursor(cursor) = client.execute("SELECT COUNT(*) FROM items").unwrap()
        else {
            panic!("cursor")
        };
        let batch = client.fetch(&cursor).unwrap();
        assert_eq!(batch.rows[0].values, vec![WireValue::Int(1)]);

        client.shutdown().unwrap();
        assert!(!client.is_reusable());
        assert!(matches!(
            client.server_status(),
            Err(ClientError::ConnectionClosed)
        ));
        shutdown.store(true, Ordering::Release);
        worker.join().unwrap().unwrap();
    });

    assert!(
        server.run_until(&AtomicBool::new(true)).is_err(),
        "served instance must reject reuse explicitly"
    );
}

#[test]
fn r9_server_config_rejects_unusable_remote_path_and_frame_contracts() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = config(temp.path().join("safe"), 5440);
    cfg.max_frame_bytes = 1;
    assert!(cfg.validate().is_err());
    cfg.max_frame_bytes = 4096;
    cfg.max_inflight_frame_bytes = 1024;
    assert!(cfg.validate().is_err());
    cfg.max_inflight_frame_bytes = 32 * 1024 * 1024;
    cfg.bind_ip = "0.0.0.0".parse().unwrap();
    assert!(cfg.validate().is_err());
    cfg.bind_ip = "127.0.0.1".parse().unwrap();
    cfg.data_dir = temp.path().join("unsafe?dsn");
    assert!(cfg.validate().is_err());
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        cfg.data_dir = temp
            .path()
            .join(std::ffi::OsString::from_vec(vec![b'n', b'o', b'n', 0xff]));
        assert!(cfg.validate().is_err());
    }
}

#[test]
fn r9_out_of_band_request_cancellation_is_exact_and_connection_local() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::bind_ephemeral(&config(temp.path().join("cancel-server"), 0)).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut control = connect(address, "cancel_contract");
        let mut executor = connect(address, "cancel_contract");
        assert!(matches!(
            control.close_database("cancel_contract"),
            Err(ClientError::Server(failure))
                if failure.code == radixdb::client::ProtocolErrorCode::CommandsOutOfSync
        ));
        let request_id = executor.reserve_request_id().unwrap();
        let execution = scope.spawn(move || {
            let cancelled = executor.execute_with_request_id(
                request_id,
                "SELECT SLEEP(30)",
                Vec::new(),
                BTreeMap::new(),
            );
            assert!(matches!(cancelled, Err(ClientError::Server(_))));
            let ExecuteResult::Cursor(cursor) = executor.execute("SELECT 1").unwrap() else {
                panic!("connection must remain usable after request-local cancellation")
            };
            let batch = executor.fetch(&cursor).unwrap();
            assert_eq!(batch.rows[0].values, vec![WireValue::Int(1)]);
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if control.cancel_execution(request_id).unwrap() {
                break;
            }
            assert!(Instant::now() < deadline, "request never entered registry");
            thread::sleep(Duration::from_millis(5));
        }
        execution.join().unwrap();
        assert!(!control.cancel_execution(request_id).unwrap());
        shutdown.store(true, Ordering::Release);
        server_worker.join().unwrap().unwrap();
    });
}
