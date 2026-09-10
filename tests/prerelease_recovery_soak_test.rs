// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0.

#![cfg(all(feature = "stress-tests", feature = "test-failpoints"))]

mod common;

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use common::prerelease::{
    tcp_command, tcp_connect, with_tcp_server, OwnedFixture, ResourceSlope, ResourceSnapshot,
    TimedResourceSample,
};
use radixdb::{
    storage::v6::{open_snapshot_manifest, SNAPSHOT_MANIFEST_FILE},
    test_failpoints::{InterleaveGuard, InterleavePoint},
    Database,
};
use radixdb_client::ExecuteResult;

const CHILD_MODE: &str = "RADIXDB_PRERELEASE_B8_CHILD_MODE";
const CHILD_DATA: &str = "RADIXDB_PRERELEASE_B8_CHILD_DATA";
const CHILD_VALUE: &str = "RADIXDB_PRERELEASE_B8_CHILD_VALUE";
const CRASH_POINT: &str = "RADIXDB_PRERELEASE_CRASH_POINT";
const CRASH_READY: &str = "RADIXDB_PRERELEASE_CRASH_READY";
const SOAK_PROFILE: &str = "RADIXDB_PRERELEASE_SOAK_PROFILE";
const SOAK_DURATION_SECS: &str = "RADIXDB_PRERELEASE_SOAK_DURATION_SECS";
const ISOLATED_TEST_CHILD: &str = "RADIXDB_PRERELEASE_B8_ISOLATED_TEST_CHILD";
const DATABASE: &str = "prerelease_b8";

fn file_dsn(path: &Path) -> String {
    format!(
        "file://{}?checkpoint_on_close=off&sync_mode=full&keep_snapshots=8",
        path.display()
    )
}

/// Run process-global timing/resource assertions without unrelated tests in
/// the same integration-test process. The ordinary Rust test harness executes
/// tests concurrently, so another test may legitimately own file descriptors
/// or initialize runtime workers while these assertions are sampling them.
fn run_in_isolated_test_process(test_name: &str) -> bool {
    if std::env::var_os(ISOLATED_TEST_CHILD).is_some() {
        return false;
    }
    let status = Command::new(std::env::current_exe().expect("resolve B8 test binary"))
        .arg("--exact")
        .arg(test_name)
        .arg("--test-threads=1")
        .env(ISOLATED_TEST_CHILD, "1")
        .status()
        .expect("spawn isolated B8 test process");
    assert!(status.success(), "isolated B8 test failed: {test_name}");
    true
}

fn child_entrypoint() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let data = PathBuf::from(std::env::var_os(CHILD_DATA).expect("B8 child data path"));
    match mode.as_str() {
        "commit" | "roulette" => {
            let value = std::env::var(CHILD_VALUE)
                .expect("B8 child value")
                .parse::<i64>()
                .expect("B8 child INTEGER value");
            let db = Database::open(&file_dsn(&data)).expect("B8 child opens database");
            let mut transaction = db.begin().expect("B8 child begins transaction");
            transaction
                .execute(
                    &format!("INSERT INTO durable_trace VALUES ({value}, {})", value * 10),
                    (),
                )
                .expect("B8 child appends trace row");
            transaction.commit().expect("B8 child commits trace row");
        }
        "checkpoint" => {
            let db = Database::open(&file_dsn(&data)).expect("B8 child opens database");
            db.execute("PRAGMA CHECKPOINT", ())
                .expect("B8 child runs checkpoint");
        }
        "view" => {
            let db = Database::open(&file_dsn(&data)).expect("B8 child opens database");
            db.execute(
                "CREATE VIEW durable_view AS SELECT id, value FROM durable_trace WHERE value >= 10",
                (),
            )
            .expect("B8 child creates VIEW");
        }
        "active_transaction" => with_tcp_server(data, 8, |address| {
            let mut connection = tcp_connect(address, DATABASE).expect("B8 child TCP connect");
            connection.begin().expect("B8 child begins TCP transaction");
            unreachable!("active transaction crash barrier was not reached");
        }),
        "cursor" => with_tcp_server(data, 8, |address| {
            let mut connection = tcp_connect(address, DATABASE).expect("B8 child TCP connect");
            tcp_command(
                &mut connection,
                "CREATE TABLE cursor_rows (id INTEGER PRIMARY KEY, value TEXT)",
            )
            .expect("B8 child creates cursor fixture");
            tcp_command(
                &mut connection,
                "INSERT INTO cursor_rows VALUES (1, 'one'), (2, 'two')",
            )
            .expect("B8 child seeds cursor fixture");
            let _ = connection
                .execute("SELECT * FROM cursor_rows ORDER BY id")
                .expect("B8 child opens cursor");
            unreachable!("cursor crash barrier was not reached");
        }),
        other => panic!("unknown B8 child mode: {other}"),
    }
    panic!("B8 child operation completed without reaching its crash barrier");
}

fn spawn_crash_child(
    mode: &str,
    data: &Path,
    point: InterleavePoint,
    value: Option<i64>,
) -> (Child, PathBuf) {
    let ready = data.with_extension(format!("{}.ready", point.crash_name()));
    let _ = fs::remove_file(&ready);
    let mut command = Command::new(std::env::current_exe().expect("resolve B8 test binary"));
    command
        .arg("--exact")
        .arg("b8_child_kill_barriers_preserve_exact_durable_state")
        .arg("--quiet")
        .env(CHILD_MODE, mode)
        .env(CHILD_DATA, data)
        .env(CRASH_POINT, point.crash_name())
        .env(CRASH_READY, &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(value) = value {
        command.env(CHILD_VALUE, value.to_string());
    }
    (command.spawn().expect("spawn B8 crash child"), ready)
}

fn kill_at_ready(mut child: Child, ready: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll B8 crash child") {
            panic!("B8 crash child exited before barrier: {status}");
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.exists(), "B8 crash barrier was not published");
    child.kill().expect("kill B8 child at exact barrier");
    let status = child.wait().expect("reap B8 crash child");
    assert!(!status.success(), "B8 child must not close gracefully");
}

fn bootstrap_trace(path: &Path) {
    let db = Database::open(&file_dsn(path)).expect("open B8 trace fixture");
    db.execute(
        "CREATE TABLE durable_trace (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        (),
    )
    .expect("create B8 durable trace");
    db.execute("INSERT INTO durable_trace VALUES (1, 10)", ())
        .expect("seed B8 durable trace");
    db.close().expect("close B8 trace fixture");
}

fn trace_count(path: &Path) -> i64 {
    let db = Database::open(&file_dsn(path)).expect("reopen B8 trace fixture");
    let count = db
        .query_one::<i64, _>("SELECT COUNT(*) FROM durable_trace", ())
        .expect("count B8 durable trace");
    db.close().expect("close recovered B8 fixture");
    count
}

#[test]
fn b8_child_kill_barriers_preserve_exact_durable_state() {
    child_entrypoint();

    for (point, expected_count) in [
        (InterleavePoint::WalBeforeCommitMarker, 1),
        (InterleavePoint::WalCommitMarkerDurable, 2),
    ] {
        let fixture = OwnedFixture::new("radixdb-prerelease-b8-commit-").unwrap();
        let path = fixture.child("database").unwrap();
        bootstrap_trace(&path);
        let (child, ready) = spawn_crash_child("commit", &path, point, Some(2));
        kill_at_ready(child, &ready);
        assert_eq!(trace_count(&path), expected_count, "barrier {point:?}");
    }

    for point in [
        InterleavePoint::SealBeforePublish,
        InterleavePoint::CheckpointBeforePublish,
        InterleavePoint::ManifestBeforePublish,
    ] {
        let fixture = OwnedFixture::new("radixdb-prerelease-b8-checkpoint-").unwrap();
        let path = fixture.child("database").unwrap();
        bootstrap_trace(&path);
        let (child, ready) = spawn_crash_child("checkpoint", &path, point, None);
        kill_at_ready(child, &ready);
        assert_eq!(trace_count(&path), 1, "barrier {point:?}");
    }

    for (point, view_must_exist) in [
        (InterleavePoint::CatalogBeforePublish, true),
        (InterleavePoint::CatalogPublished, true),
    ] {
        let fixture = OwnedFixture::new("radixdb-prerelease-b8-view-").unwrap();
        let path = fixture.child("database").unwrap();
        bootstrap_trace(&path);
        let (child, ready) = spawn_crash_child("view", &path, point, None);
        kill_at_ready(child, &ready);
        let db = Database::open(&file_dsn(&path)).expect("reopen B8 VIEW fixture");
        let result = db.query_one::<i64, _>("SELECT COUNT(*) FROM durable_view", ());
        assert_eq!(result.is_ok(), view_must_exist, "barrier {point:?}");
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM durable_trace", ())
                .unwrap(),
            1
        );
        db.close().unwrap();
    }

    for (mode, point) in [
        (
            "active_transaction",
            InterleavePoint::ActiveTransactionBegan,
        ),
        ("cursor", InterleavePoint::CursorPublished),
    ] {
        let fixture = OwnedFixture::new("radixdb-prerelease-b8-session-").unwrap();
        let data = fixture.child("server-data").unwrap();
        let (child, ready) = spawn_crash_child(mode, &data, point, None);
        kill_at_ready(child, &ready);
        with_tcp_server(data, 8, |address| {
            let mut connection = tcp_connect(address, DATABASE).unwrap();
            tcp_command(&mut connection, "SELECT 1").unwrap();
        });
    }
}

#[test]
fn b8_restart_roulette_preserves_one_200_cycle_trace() {
    if std::env::var_os(CHILD_MODE).is_some() {
        child_entrypoint();
    }
    let fixture = OwnedFixture::new("radixdb-prerelease-b8-roulette-").unwrap();
    let path = fixture.child("database").unwrap();
    bootstrap_trace(&path);

    for id in 2..=201i64 {
        let (child, ready) = spawn_crash_child(
            "roulette",
            &path,
            InterleavePoint::WalCommitMarkerDurable,
            Some(id),
        );
        kill_at_ready(child, &ready);
        let db = Database::open(&file_dsn(&path)).expect("roulette recovery opens");
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM durable_trace", ())
            .expect("roulette row-count oracle");
        let sum: i64 = db
            .query_one("SELECT SUM(value) FROM durable_trace", ())
            .expect("roulette checksum oracle");
        assert_eq!(count, id);
        assert_eq!(sum, id * (id + 1) * 5);
        db.close().expect("roulette recovery closes");
    }
}

fn snapshot_ids_by_creation(path: &Path) -> Vec<String> {
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(path.join("snapshots")).expect("enumerate backup generations") {
        let entry = entry.expect("read backup generation entry");
        if !entry
            .file_type()
            .expect("inspect backup generation")
            .is_dir()
            || !entry.path().join(SNAPSHOT_MANIFEST_FILE).is_file()
        {
            continue;
        }
        let manifest = open_snapshot_manifest(entry.path()).expect("open physical snapshot");
        let snapshot_id = manifest.snapshot_id().to_string();
        assert_eq!(
            entry.file_name().to_string_lossy(),
            snapshot_id,
            "snapshot directory identity must match its manifest"
        );
        snapshots.push((manifest.created_unix_ns(), snapshot_id));
    }
    snapshots.sort_unstable();
    snapshots.into_iter().map(|(_, id)| id).collect()
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create restore copy directory");
    for entry in fs::read_dir(source).expect("enumerate source database") {
        let entry = entry.expect("read source database entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type().expect("inspect source entry").is_dir() {
            copy_tree(&source_path, &destination_path);
        } else {
            fs::copy(&source_path, &destination_path).expect("copy restore artifact");
        }
    }
}

fn verify_backup_epoch(db: &Database, epoch: i64) {
    let metadata_epoch: i64 = db
        .query_one("SELECT MAX(epoch) FROM backup_epochs", ())
        .expect("read durable backup epoch");
    let expected_rows: i64 = db
        .query_one(
            &format!("SELECT expected_rows FROM backup_epochs WHERE epoch = {metadata_epoch}"),
            (),
        )
        .expect("read durable backup row count");
    let expected_sum: i64 = db
        .query_one(
            &format!("SELECT expected_sum FROM backup_epochs WHERE epoch = {metadata_epoch}"),
            (),
        )
        .expect("read durable backup checksum");
    assert_eq!(metadata_epoch, epoch);
    let rows: i64 = db
        .query_one("SELECT COUNT(*) FROM backup_children", ())
        .expect("read restored backup row count");
    let sum: i64 = db
        .query_one("SELECT SUM(value) FROM backup_children", ())
        .expect("read restored backup checksum");
    assert_eq!((rows, sum), (expected_rows, expected_sum));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM backup_view", ())
            .expect("read restored VIEW"),
        expected_rows
    );
    assert_eq!(
        db.query_one::<i64, _>(
            &format!("SELECT COUNT(*) FROM backup_children WHERE parent_id = {epoch}"),
            (),
        )
        .expect("verify restored index lookup"),
        2
    );
}

fn commit_backup_epoch(db: &Database, epoch: i64) {
    let mut transaction = db.begin().unwrap();
    transaction
        .execute(
            &format!("INSERT INTO backup_parents VALUES ({epoch}, 'epoch-{epoch}')"),
            (),
        )
        .unwrap();
    for offset in 0..2i64 {
        let id = epoch * 10 + offset;
        transaction
            .execute(
                &format!("INSERT INTO backup_children VALUES ({id}, {epoch}, {id})"),
                (),
            )
            .unwrap();
    }
    let rows = epoch * 2;
    let sum = (1..=epoch)
        .flat_map(|value| [value * 10, value * 10 + 1])
        .sum::<i64>();
    transaction
        .execute(
            &format!("INSERT INTO backup_epochs VALUES ({epoch}, {rows}, {sum})"),
            (),
        )
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn b8_continuous_backups_restore_whole_durable_epochs() {
    if run_in_isolated_test_process("b8_continuous_backups_restore_whole_durable_epochs") {
        return;
    }
    let fixture = OwnedFixture::new("radixdb-prerelease-b8-backups-").unwrap();
    let source = fixture.child("source").unwrap();
    let dsn = file_dsn(&source);
    let db = Database::open(&dsn).unwrap();
    for sql in [
        "CREATE TABLE backup_parents (id INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE)",
        "CREATE TABLE backup_children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES backup_parents(id), value INTEGER NOT NULL)",
        "CREATE TABLE backup_epochs (epoch INTEGER PRIMARY KEY, expected_rows INTEGER NOT NULL, expected_sum INTEGER NOT NULL)",
        "CREATE VIEW backup_view AS SELECT c.id, p.label, c.value FROM backup_children c LEFT JOIN backup_parents p ON c.parent_id = p.id",
    ] {
        db.execute(sql, ()).unwrap();
    }

    for epoch in 1..=4i64 {
        if epoch == 2 {
            let guard = InterleaveGuard::install([InterleavePoint::VisibilityBeforePublish]);
            let controller = guard.controller();
            let commit_db = db.clone();
            let commit = thread::spawn(move || commit_backup_epoch(&commit_db, epoch));
            let arrival = controller
                .wait_for(
                    InterleavePoint::VisibilityBeforePublish,
                    None,
                    Duration::from_secs(10),
                )
                .unwrap();
            let snapshot_db = db.clone();
            let (snapshot_tx, snapshot_rx) = mpsc::channel();
            let snapshot = thread::spawn(move || {
                snapshot_tx.send(snapshot_db.create_snapshot()).unwrap();
            });
            // CONTROL and its WAL suffix cannot be selected between a durable
            // commit marker and publication of that transaction's visibility.
            assert!(snapshot_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err());
            controller.release(arrival);
            drop(guard);
            commit.join().unwrap();
            snapshot_rx
                .recv_timeout(Duration::from_secs(20))
                .unwrap()
                .unwrap();
            snapshot.join().unwrap();
        } else {
            commit_backup_epoch(&db, epoch);
        }

        if epoch == 3 {
            let guard = InterleaveGuard::install([InterleavePoint::CheckpointBeforePublish]);
            let controller = guard.controller();
            let checkpoint_db = db.clone();
            let checkpoint = thread::spawn(move || checkpoint_db.execute("PRAGMA CHECKPOINT", ()));
            let arrival = controller
                .wait_for(
                    InterleavePoint::CheckpointBeforePublish,
                    None,
                    Duration::from_secs(10),
                )
                .unwrap();
            let snapshot_db = db.clone();
            let (snapshot_tx, snapshot_rx) = mpsc::channel();
            let snapshot = thread::spawn(move || {
                snapshot_tx.send(snapshot_db.create_snapshot()).unwrap();
            });
            assert!(snapshot_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err());
            controller.release(arrival);
            drop(guard);
            checkpoint.join().unwrap().unwrap();
            snapshot_rx
                .recv_timeout(Duration::from_secs(20))
                .unwrap()
                .unwrap();
            snapshot.join().unwrap();
        } else if epoch != 2 {
            db.create_snapshot().unwrap();
        }
    }
    let snapshot_ids = snapshot_ids_by_creation(&source);
    assert_eq!(snapshot_ids.len(), 4);
    db.close().unwrap();

    for (index, snapshot_id) in snapshot_ids.iter().enumerate() {
        let restore = fixture.child(format!("restore-{}", index + 1)).unwrap();
        copy_tree(&source, &restore);
        let restored = Database::open(&file_dsn(&restore)).unwrap();
        restored.restore_snapshot(Some(snapshot_id)).unwrap();
        verify_backup_epoch(&restored, index as i64 + 1);
        restored.close().unwrap();
    }
}

#[test]
fn b8_resource_slope_detects_leak_and_quiescence_returns_to_corridor() {
    if run_in_isolated_test_process(
        "b8_resource_slope_detects_leak_and_quiescence_returns_to_corridor",
    ) {
        return;
    }
    let stable = (0..5u64)
        .map(|index| TimedResourceSample {
            elapsed_millis: index * 1_000,
            resources: ResourceSnapshot {
                rss_bytes: 10_000_000 + (index % 2) * 4_096,
                open_file_descriptors: 8,
                active_workers: 2,
                active_sessions: 0,
                active_cursors: 0,
            },
        })
        .collect::<Vec<_>>();
    assert!(!ResourceSlope::from_samples(&stable)
        .unwrap()
        .exceeds(1_000_000.0, 0.25, 0.25));

    let leaking = (0..5u64)
        .map(|index| TimedResourceSample {
            elapsed_millis: index * 1_000,
            resources: ResourceSnapshot {
                rss_bytes: 10_000_000 + index * 2_000_000,
                open_file_descriptors: 8 + index,
                active_workers: 2,
                active_sessions: index,
                active_cursors: index,
            },
        })
        .collect::<Vec<_>>();
    assert!(ResourceSlope::from_samples(&leaking)
        .unwrap()
        .exceeds(1_000_000.0, 0.25, 0.25));

    // The global Rayon pool is stable process infrastructure, initialized on
    // first parallel work. Admit it before defining the quiescent baseline so
    // lazy initialization is not misreported as a server thread leak.
    let _ = rayon::current_num_threads();
    let baseline = ResourceSnapshot::capture_linux(0, 0).unwrap();
    let fixture = OwnedFixture::new("radixdb-prerelease-b8-quiescence-").unwrap();
    let data = fixture.child("server-data").unwrap();
    with_tcp_server(data, 64, |address| {
        let mut clients = Vec::new();
        for client_id in 0..32 {
            let mut connection = tcp_connect(address, DATABASE).unwrap();
            if client_id == 0 {
                tcp_command(
                    &mut connection,
                    "CREATE TABLE resource_rows (id INTEGER PRIMARY KEY, value TEXT)",
                )
                .unwrap();
                tcp_command(
                    &mut connection,
                    "INSERT INTO resource_rows VALUES (1, 'one'), (2, 'two')",
                )
                .unwrap();
            }
            let ExecuteResult::Cursor(_cursor) = connection
                .execute("SELECT * FROM resource_rows ORDER BY id")
                .unwrap()
            else {
                panic!("resource soak SELECT did not open cursor");
            };
            clients.push(connection);
        }
        drop(clients);
    });
    thread::sleep(Duration::from_millis(100));
    let after = ResourceSnapshot::capture_linux(0, 0).unwrap();
    after
        .within_quiescent_corridor(&baseline, 64 * 1024 * 1024, 4, 2)
        .unwrap();

    let start = Instant::now();
    let mut samples = Vec::new();
    for _ in 0..5 {
        samples
            .push(TimedResourceSample::capture(start.elapsed().as_millis() as u64, 0, 0).unwrap());
        thread::sleep(Duration::from_millis(25));
    }
    let slope = ResourceSlope::from_samples(&samples).unwrap();
    assert!(slope.file_descriptors_per_second <= 1.0);
    assert!(slope.workers_per_second <= 1.0);
}

fn configured_soak_duration() -> Duration {
    if let Ok(value) = std::env::var(SOAK_DURATION_SECS) {
        return Duration::from_secs(
            value
                .parse::<u64>()
                .expect("RADIXDB_PRERELEASE_SOAK_DURATION_SECS must be an integer")
                .max(1),
        );
    }
    Duration::from_secs(match std::env::var(SOAK_PROFILE).as_deref() {
        Ok("30m") => 30 * 60,
        Ok("6h") => 6 * 60 * 60,
        Ok("12h") => 12 * 60 * 60,
        Ok("24h") => 24 * 60 * 60,
        Ok(other) => panic!("unsupported B8 soak profile: {other}"),
        // A two-second least-squares window can classify one transient
        // checkpoint descriptor as a sustained >1 FD/s trend. Ten seconds is
        // still a short local gate, but long enough to distinguish a bounded
        // open/close pulse from monotonic resource growth.
        Err(_) => 10,
    })
}

#[test]
fn b8_configurable_soak_profile_runs_transactional_view_workload() {
    let duration = configured_soak_duration();
    let fixture = OwnedFixture::new("radixdb-prerelease-b8-soak-").unwrap();
    let path = fixture.child("database").unwrap();
    let db = Database::open(&file_dsn(&path)).unwrap();
    for sql in [
        "CREATE TABLE soak_parents (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
        "CREATE TABLE soak_children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES soak_parents(id), value INTEGER NOT NULL)",
        "CREATE VIEW soak_view AS SELECT c.id, c.value, p.value AS parent_value FROM soak_children c LEFT JOIN soak_parents p ON c.parent_id = p.id",
    ] {
        db.execute(sql, ()).unwrap();
    }

    let started = Instant::now();
    let warmup = duration.min(Duration::from_millis(500));
    let mut samples = Vec::new();
    let mut operation = 0i64;
    let mut last_sample = Instant::now();
    while started.elapsed() < duration {
        operation += 1;
        let mut transaction = db.begin().unwrap();
        transaction
            .execute(
                &format!("INSERT INTO soak_parents VALUES ({operation}, {operation})"),
                (),
            )
            .unwrap();
        transaction
            .execute(
                &format!(
                    "INSERT INTO soak_children VALUES ({operation}, {operation}, {})",
                    operation * 10
                ),
                (),
            )
            .unwrap();
        if operation > 1 {
            transaction
                .execute(
                    &format!(
                        "UPDATE soak_children SET value = value + 1 WHERE id = {}",
                        operation - 1
                    ),
                    (),
                )
                .unwrap();
        }
        if operation > 64 {
            // The preceding operation checkpoints the first cold generation
            // and may leave compaction queued for the table seal fence.  These
            // indexed DELETEs therefore also guard against recursively taking
            // that shared fence from an already fenced scan planner.
            transaction
                .execute(
                    &format!("DELETE FROM soak_children WHERE id = {}", operation - 64),
                    (),
                )
                .unwrap();
            transaction
                .execute(
                    &format!("DELETE FROM soak_parents WHERE id = {}", operation - 64),
                    (),
                )
                .unwrap();
        }
        transaction.commit().unwrap();

        if operation % 16 == 0 {
            let visible: i64 = db.query_one("SELECT COUNT(*) FROM soak_view", ()).unwrap();
            assert_eq!(visible, operation.min(64));
        }
        if operation % 64 == 0 {
            db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        }
        if started.elapsed() >= warmup && last_sample.elapsed() >= Duration::from_millis(250) {
            samples.push(
                TimedResourceSample::capture(started.elapsed().as_millis() as u64, 0, 0).unwrap(),
            );
            last_sample = Instant::now();
        }
    }
    assert!(operation > 0);
    let final_count: i64 = db.query_one("SELECT COUNT(*) FROM soak_view", ()).unwrap();
    assert_eq!(final_count, operation.min(64));
    db.close().unwrap();

    if samples.len() >= 3 {
        let slope = ResourceSlope::from_samples(&samples).unwrap();
        assert!(slope.file_descriptors_per_second <= 1.0);
        assert!(slope.workers_per_second <= 1.0);
    }
}
