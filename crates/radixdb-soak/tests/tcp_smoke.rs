use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use radixdb::server::{Server, ServerConfig};
use radixdb_soak::{
    config::{DatabaseEngine, ResolvedDatabaseConfig},
    diagnostics::EngineSnapshotWorker,
    runtime::RuntimeMetrics,
    workload::{
        check_invariants, connect, install_schema, logical_digest, restore, seed_cold_rows,
        snapshot, worker_loop,
    },
};

fn server_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 16,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: 4,
        max_database_name_bytes: 64,
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 256,
        cursor_batch_max_bytes: 2 * 1024 * 1024,
        max_frame_bytes: 8 * 1024 * 1024,
        copy_max_transaction_bytes: radixdb::server::default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: radixdb::server::default_target_volume_rows(),
        seal_hot_bytes_threshold: 16 * 1024,
        seal_incremental_hot_bytes_threshold: 4 * 1024,
        read_queue_depth: 2,
    }
}

#[test]
fn real_tcp_server_accepts_schema_workload_views_and_invariants() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("server-data");
    let server = Server::bind_ephemeral(&server_config(data_dir.clone())).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let server_thread = thread::spawn(move || server.run_until(&server_shutdown));

    let database = ResolvedDatabaseConfig {
        engine: DatabaseEngine::Radixdb,
        address,
        name: "remote_soak_smoke".into(),
        login: "root".into(),
        password_file: None,
        connect_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(10),
    };
    let mut coordinator = connect(&database).unwrap();
    install_schema(&mut coordinator, 2).unwrap();
    seed_cold_rows(
        &mut coordinator,
        &data_dir,
        1_000,
        database.read_timeout,
        |_, _| Ok(()),
    )
    .unwrap();

    let mut engine_observer =
        EngineSnapshotWorker::start(database.clone(), Duration::from_secs(2)).unwrap();
    assert!(engine_observer.try_request(1, 1, 1).unwrap());
    let diagnostic_deadline = Instant::now() + Duration::from_secs(5);
    let diagnostic = loop {
        if let Some(sample) = engine_observer.poll().unwrap() {
            break sample;
        }
        assert!(
            Instant::now() < diagnostic_deadline,
            "bounded runtime snapshot did not complete"
        );
        thread::sleep(Duration::from_millis(5));
    };
    let runtime_snapshot = diagnostic.snapshot.as_ref().unwrap();
    assert!(runtime_snapshot["server_runtime"]["active_connections"]
        .as_u64()
        .is_some_and(|connections| connections >= 2));
    assert!(runtime_snapshot["runtime_owners"]["authenticated_sessions"]
        .as_u64()
        .is_some_and(|sessions| sessions >= 2));
    assert!(runtime_snapshot["runtime_owners"]["active_executions"]
        .as_u64()
        .is_some_and(|executions| executions >= 1));
    engine_observer.shutdown().unwrap();

    let metrics = Arc::new(RuntimeMetrics::default());
    let workers_stop = Arc::new(AtomicBool::new(false));
    let recovering = Arc::new(AtomicBool::new(false));
    let recovery_epoch = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut workers = Vec::new();
    for worker in 0..2 {
        let database = database.clone();
        let stop = Arc::clone(&workers_stop);
        let metrics = Arc::clone(&metrics);
        let recovering = Arc::clone(&recovering);
        let recovery_epoch = Arc::clone(&recovery_epoch);
        let history = temp.path().join(format!("worker-{worker}.csv"));
        workers.push(thread::spawn(move || {
            worker_loop(
                database,
                worker,
                7,
                0,
                history,
                deadline,
                stop,
                recovering,
                recovery_epoch,
                metrics,
            )
        }));
    }
    let concurrent_check_deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < concurrent_check_deadline {
        let invariants = check_invariants(&mut coordinator, 1_000).unwrap();
        assert!(
            invariants.iter().all(|item| item.failures == 0),
            "concurrent invariant snapshot mixed committed states: {invariants:#?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
    for worker in workers {
        worker.join().unwrap().unwrap();
    }
    let invariants = check_invariants(&mut coordinator, 1_000).unwrap();
    assert!(
        invariants.iter().all(|item| item.failures == 0),
        "{invariants:#?}"
    );
    assert!(metrics.counters(0).transactions_committed > 0);
    let expected_digest = logical_digest(&mut coordinator).unwrap();
    snapshot(&mut coordinator).unwrap();
    coordinator
        .command("INSERT INTO soak_cold_rows VALUES (1001, 1, 1)")
        .unwrap();
    assert_ne!(logical_digest(&mut coordinator).unwrap(), expected_digest);
    restore(&mut coordinator).unwrap();
    assert_eq!(logical_digest(&mut coordinator).unwrap(), expected_digest);

    drop(coordinator);
    shutdown.store(true, Ordering::Release);
    server_thread.join().unwrap().unwrap();

    let reopened = Server::bind_ephemeral(&server_config(data_dir)).unwrap();
    let reopen_address = reopened.local_addr().unwrap();
    let reopen_shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&reopen_shutdown);
    let server_thread = thread::spawn(move || reopened.run_until(&server_shutdown));
    let mut reopened_database = database;
    reopened_database.address = reopen_address;
    let mut connection = connect(&reopened_database).unwrap();
    assert_eq!(logical_digest(&mut connection).unwrap(), expected_digest);
    assert!(check_invariants(&mut connection, 1_000)
        .unwrap()
        .iter()
        .all(|item| item.failures == 0));
    drop(connection);
    reopen_shutdown.store(true, Ordering::Release);
    server_thread.join().unwrap().unwrap();
}
