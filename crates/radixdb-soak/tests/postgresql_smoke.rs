use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use postgres::{Config, NoTls};
use radixdb_soak::{
    config::{DatabaseEngine, ResolvedDatabaseConfig},
    database,
    diagnostics::EngineSnapshotWorker,
    runtime::RuntimeMetrics,
    workload,
};

struct TemporaryPostgres {
    data_dir: PathBuf,
    log: PathBuf,
    socket_dir: PathBuf,
    port: u16,
}

impl Drop for TemporaryPostgres {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .args([
                "-D",
                self.data_dir.to_string_lossy().as_ref(),
                "-m",
                "fast",
                "stop",
            ])
            .status();
    }
}

impl TemporaryPostgres {
    fn start(&self) {
        run(
            "pg_ctl",
            &[
                "-D",
                self.data_dir.to_string_lossy().as_ref(),
                "-l",
                self.log.to_string_lossy().as_ref(),
                "-o",
                &format!(
                    "-h 127.0.0.1 -k {} -p {} -c fsync=on -c full_page_writes=on -c synchronous_commit=on -c logging_collector=off",
                    self.socket_dir.display(),
                    self.port
                ),
                "-w",
                "start",
            ],
        );
    }

    fn stop_fast(&self) {
        run(
            "pg_ctl",
            &[
                "-D",
                self.data_dir.to_string_lossy().as_ref(),
                "-m",
                "fast",
                "-w",
                "stop",
            ],
        );
    }

    fn kill(&self) {
        let pid = fs::read_to_string(self.data_dir.join("postmaster.pid"))
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .parse::<libc::pid_t>()
            .unwrap();
        // SAFETY: the PID comes from this test-owned cluster's postmaster.pid.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        let process = PathBuf::from(format!("/proc/{pid}"));
        while process.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !process.exists(),
            "killed PostgreSQL postmaster did not exit"
        );
    }
}

#[test]
#[ignore = "focused gate: requires PostgreSQL server binaries"]
fn postgresql_adapter_installs_seeds_checks_and_checkpoints() {
    let temporary = tempfile::tempdir().unwrap();
    let data_dir = temporary.path().join("pgdata");
    run(
        "initdb",
        &[
            "-D",
            data_dir.to_string_lossy().as_ref(),
            "-A",
            "trust",
            "-U",
            "soak",
            "--no-locale",
            "--encoding=UTF8",
        ],
    );
    let port = unused_port();
    let server = TemporaryPostgres {
        data_dir: data_dir.clone(),
        log: temporary.path().join("postgres.log"),
        socket_dir: temporary.path().to_path_buf(),
        port,
    };
    server.start();

    let mut bootstrap = Config::new();
    bootstrap
        .host("127.0.0.1")
        .port(port)
        .user("soak")
        .dbname("postgres");
    bootstrap
        .connect(NoTls)
        .unwrap()
        .batch_execute("CREATE DATABASE soak_test")
        .unwrap();

    let config = ResolvedDatabaseConfig {
        engine: DatabaseEngine::Postgresql,
        address: SocketAddr::from(([127, 0, 0, 1], port)),
        name: "soak_test".into(),
        login: "soak".into(),
        password_file: None,
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
    };
    let identity = database::server_identity(&config).unwrap();
    assert!(identity.to_ascii_lowercase().contains("postgresql"));
    let mut observer = EngineSnapshotWorker::start(config.clone(), Duration::from_secs(2)).unwrap();
    assert!(observer.try_request(1, 1, 1).unwrap());
    let observer_deadline = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        if let Some(snapshot) = observer.poll().unwrap() {
            break snapshot;
        }
        assert!(Instant::now() < observer_deadline);
        thread::sleep(Duration::from_millis(5));
    };
    assert!(snapshot.available(), "{snapshot:#?}");
    assert_eq!(
        snapshot
            .snapshot
            .as_ref()
            .and_then(|value| value.get("engine_kind"))
            .and_then(serde_json::Value::as_str),
        Some("postgresql")
    );
    observer.shutdown().unwrap();

    let mut connection = database::connect(&config).unwrap();
    workload::install_schema(&mut connection, 4).unwrap();
    let import_dir = temporary.path().join("import");
    workload::seed_cold_rows(
        &mut connection,
        &import_dir,
        20_000,
        config.read_timeout,
        |_, _| Ok(()),
    )
    .unwrap();
    let metrics = Arc::new(RuntimeMetrics::default());
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut workers = Vec::new();
    for worker_id in 0..4 {
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let history = temporary.path().join(format!("worker-{worker_id}.csv"));
        workers.push(thread::spawn(move || {
            workload::worker_loop(
                config,
                worker_id,
                7,
                0,
                history,
                deadline,
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicU64::new(0)),
                metrics,
            )
        }));
    }
    for worker in workers {
        worker.join().unwrap().unwrap();
    }
    assert!(metrics.counters(0).transactions_committed > 0);
    let invariants = workload::check_invariants(&mut connection, 20_000).unwrap();
    assert!(invariants.iter().all(|invariant| invariant.failures == 0));
    assert_eq!(
        connection
            .scalar_i64("SELECT COUNT(*)::bigint FROM soak_cold_rows")
            .unwrap(),
        20_000
    );
    workload::checkpoint(&mut connection).unwrap();

    connection.shutdown().unwrap();
    server.stop_fast();
    server.start();
    let mut connection = database::connect(&config).unwrap();
    assert!(workload::check_invariants(&mut connection, 20_000)
        .unwrap()
        .iter()
        .all(|invariant| invariant.failures == 0));
    connection.shutdown().unwrap();

    server.kill();
    server.start();
    let mut connection = database::connect(&config).unwrap();
    assert!(workload::check_invariants(&mut connection, 20_000)
        .unwrap()
        .iter()
        .all(|invariant| invariant.failures == 0));
}

fn unused_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn run(program: &str, arguments: &[&str]) {
    let output = Command::new(program).args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "{program} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
