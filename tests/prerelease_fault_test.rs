// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0.

#![cfg(all(feature = "stress-tests", feature = "test-failpoints"))]

mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    process::Command,
    sync::{atomic::AtomicBool, mpsc},
    thread,
    time::{Duration, Instant, UNIX_EPOCH},
};

use common::prerelease::{
    b6_corruption_corpus, b6_fault_schedule, tcp_connect, tcp_scalar_i64, tcp_server_config,
    CorruptionMutation, DurableArtifact, OwnedFixture, RecoveryClass, ResourceLimits,
    ResourceSnapshot,
};
use radixdb::{
    common::time_compat::TestWallClockGuard, server::Server, storage::mvcc::get_fast_timestamp,
    Database,
};
use radixdb_client::{
    protocol::{encode_payload, read_frame, write_frame, ClientMessage, ServerMessage},
    Connection, ExecuteResult, ProtocolCapability, WireValue, PROTOCOL_VERSION,
};

const DATABASE: &str = "prerelease_b6";
const RESOURCE_CORRIDOR_CHILD_ENV: &str = "RADIXDB_PRERELEASE_B6_RESOURCE_CORRIDOR_CHILD";

fn framed(message: &ClientMessage, limit: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_frame(&mut bytes, message, limit).unwrap();
    bytes
}

fn raw_stream(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
}

fn raw_ready(address: SocketAddr, fragmented_handshake: bool) -> TcpStream {
    let mut stream = raw_stream(address);
    let handshake = ClientMessage::Handshake {
        protocol_version: PROTOCOL_VERSION,
        max_frame_bytes: 1024 * 1024,
        capabilities: vec![ProtocolCapability::BuildIdentityV1],
    };
    let frame = framed(&handshake, 1024 * 1024);
    if fragmented_handshake {
        for byte in frame {
            stream.write_all(&[byte]).unwrap();
        }
    } else {
        stream.write_all(&frame).unwrap();
    }
    assert!(matches!(
        read_frame::<ServerMessage>(&mut stream, 1024 * 1024).unwrap(),
        ServerMessage::HandshakeAccepted { .. }
    ));

    let mut coalesced = framed(
        &ClientMessage::Authenticate {
            login: "root".into(),
            password: None,
        },
        1024 * 1024,
    );
    coalesced.extend(framed(
        &ClientMessage::SelectDatabase {
            database: DATABASE.into(),
        },
        1024 * 1024,
    ));
    stream.write_all(&coalesced).unwrap();
    assert!(matches!(
        read_frame::<ServerMessage>(&mut stream, 1024 * 1024).unwrap(),
        ServerMessage::AuthenticationAccepted
    ));
    assert!(matches!(
        read_frame::<ServerMessage>(&mut stream, 1024 * 1024).unwrap(),
        ServerMessage::DatabaseSelected { .. }
    ));
    stream
}

fn assert_neighbor_usable(address: SocketAddr) {
    let mut neighbor = tcp_connect(address, DATABASE).unwrap();
    assert_eq!(tcp_scalar_i64(&mut neighbor, "SELECT 1").unwrap(), 1);
}

fn assert_protocol_peer_closed(mut stream: TcpStream) {
    stream.shutdown(Shutdown::Write).ok();
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) | Err(_) => {}
        Ok(_) => panic!("invalid protocol peer received an unexpected response"),
    }
}

#[test]
fn b6_manifest_covers_fault_corruption_and_resource_contracts() {
    let schedule = b6_fault_schedule();
    schedule.validate().unwrap();
    assert_eq!(schedule.activations.len(), 15);
    assert_eq!(
        schedule
            .activations
            .iter()
            .map(|activation| activation.point)
            .collect::<BTreeSet<_>>()
            .len(),
        schedule.activations.len()
    );

    let corpus = b6_corruption_corpus();
    assert_eq!(corpus.len(), 10);
    assert_eq!(
        corpus
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        corpus.len()
    );
    let mutations = corpus
        .iter()
        .map(|case| case.mutation)
        .collect::<BTreeSet<_>>();
    for required in [
        CorruptionMutation::BitFlip,
        CorruptionMutation::TruncatedTail,
        CorruptionMutation::TruncatedHeader,
        CorruptionMutation::StaleManifest,
        CorruptionMutation::LostNewestGeneration,
        CorruptionMutation::WrongChecksum,
        CorruptionMutation::WrongVersion,
        CorruptionMutation::WrongLength,
    ] {
        assert!(mutations.contains(&required), "missing {required:?}");
    }
    assert!(corpus.iter().any(|case| {
        case.artifact == DurableArtifact::Backup && case.expected == RecoveryClass::FailClosed
    }));
    assert!(corpus
        .iter()
        .any(|case| { case.expected == RecoveryClass::RecoverPreviousGeneration }));
    assert!(corpus
        .iter()
        .any(|case| { case.expected == RecoveryClass::IgnoreUnpublishedArtifact }));

    ResourceLimits {
        memory_bytes: 512 * 1024 * 1024,
        file_descriptors: 128,
        threads: 32,
        cpu_percent: 50,
        io_bytes_per_second: 32 * 1024 * 1024,
        connections: 12,
        inflight_frame_bytes: 2 * 1024 * 1024,
    }
    .validate()
    .unwrap();
}

#[test]
fn b6_wall_clock_rollback_jump_and_equal_ticks_preserve_commit_order() {
    let base = UNIX_EPOCH + Duration::from_secs(1_730_613_600);
    let clock = TestWallClockGuard::install(base);
    let first = get_fast_timestamp();
    let equal_tick = get_fast_timestamp();
    assert!(equal_tick > first);

    clock.set(base - Duration::from_secs(7_200));
    let after_rollback = get_fast_timestamp();
    assert!(after_rollback > equal_tick);

    clock.set(base + Duration::from_secs(86_400 * 365));
    let after_jump = get_fast_timestamp();
    assert!(after_jump > after_rollback);

    let monotonic_deadline = Instant::now() + Duration::from_millis(20);
    clock.set(UNIX_EPOCH + Duration::from_secs(1_615_705_199));
    assert!(Instant::now() < monotonic_deadline);
    clock.set(UNIX_EPOCH + Duration::from_secs(1_615_708_801));
    assert!(Instant::now() < monotonic_deadline);

    let db = Database::open("memory://prerelease-b6-clock").unwrap();
    db.execute(
        "CREATE TABLE clock_rows (id INTEGER PRIMARY KEY, observed TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        (),
    )
    .unwrap();
    clock.set(base);
    assert!(db
        .query_one::<bool, _>("SELECT NOW() = CURRENT_TIMESTAMP", ())
        .unwrap());
    db.execute("INSERT INTO clock_rows (id) VALUES (1), (2)", ())
        .unwrap();
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT COUNT(*) FROM clock_rows a JOIN clock_rows b ON a.observed = b.observed",
            (),
        )
        .unwrap(),
        4
    );
    clock.set(base - Duration::from_secs(3_600));
    db.execute("INSERT INTO clock_rows (id) VALUES (3)", ())
        .unwrap();
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM clock_rows", ())
            .unwrap(),
        3
    );
}

#[test]
fn b6_protocol_fragmentation_invalid_lengths_slow_reader_and_reset_are_isolated() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b6-wire-").unwrap();
    let mut config = tcp_server_config(fixture.child("server-data").unwrap(), 8);
    config.net_read_timeout_secs = 1;
    config.net_write_timeout_secs = 1;
    config.max_frame_bytes = 1024 * 1024;
    config.cursor_batch_max_bytes = 512 * 1024;
    let server = Server::bind_ephemeral(&config).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut setup = tcp_connect(address, DATABASE).unwrap();
        setup
            .execute("CREATE TABLE wire_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            .unwrap();
        let payload = "x".repeat(32 * 1024);
        for id in 0..32 {
            setup
                .execute_with_positional_parameters(
                    "INSERT INTO wire_rows VALUES ($1, $2)",
                    vec![WireValue::Int(id), WireValue::String(payload.clone())],
                )
                .unwrap();
        }

        let mut fragmented = raw_ready(address, true);
        fragmented
            .write_all(&framed(
                &ClientMessage::Execute {
                    request_id: 1,
                    sql: "SELECT 1".into(),
                    positional: Vec::new(),
                    named: BTreeMap::new(),
                },
                1024 * 1024,
            ))
            .unwrap();
        assert!(matches!(
            read_frame::<ServerMessage>(&mut fragmented, 1024 * 1024).unwrap(),
            ServerMessage::CursorOpened { .. }
        ));
        drop(fragmented);

        for bytes in [
            vec![0, 0, 0, 0],
            (2 * 1024 * 1024_u32).to_be_bytes().to_vec(),
            vec![0, 1],
            {
                let mut value = 64_u32.to_be_bytes().to_vec();
                value.extend_from_slice(&[1, 2]);
                value
            },
        ] {
            let mut peer = raw_stream(address);
            peer.write_all(&bytes).unwrap();
            assert_protocol_peer_closed(peer);
            assert_neighbor_usable(address);
        }

        let mut slow = raw_ready(address, false);
        slow.write_all(&framed(
            &ClientMessage::Execute {
                request_id: 2,
                sql: "SELECT payload FROM wire_rows ORDER BY id".into(),
                positional: Vec::new(),
                named: BTreeMap::new(),
            },
            1024 * 1024,
        ))
        .unwrap();
        let cursor = match read_frame::<ServerMessage>(&mut slow, 1024 * 1024).unwrap() {
            ServerMessage::CursorOpened { cursor_id, .. } => cursor_id,
            other => panic!("expected cursor, got {other:?}"),
        };
        let mut fetch_burst = Vec::new();
        for _ in 0..8 {
            fetch_burst.extend(framed(
                &ClientMessage::Fetch { cursor_id: cursor },
                1024 * 1024,
            ));
        }
        slow.write_all(&fetch_burst).unwrap();
        thread::sleep(Duration::from_millis(50));
        drop(slow);
        assert_neighbor_usable(address);

        shutdown.store(true, std::sync::atomic::Ordering::Release);
        server_worker.join().unwrap().unwrap();
    });
}

#[test]
fn b6_cancellation_and_connection_pressure_return_to_resource_corridor() {
    if std::env::var_os(RESOURCE_CORRIDOR_CHILD_ENV).is_none() {
        let executable = std::env::current_exe().unwrap();
        let output = Command::new(executable)
            .args([
                "--exact",
                "b6_cancellation_and_connection_pressure_return_to_resource_corridor",
                "--test-threads=1",
            ])
            .env(RESOURCE_CORRIDOR_CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "resource corridor child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    struct ShutdownOnDrop<'a>(&'a AtomicBool);

    impl Drop for ShutdownOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let fixture = OwnedFixture::new("radixdb-prerelease-b6-resource-").unwrap();
    let mut config = tcp_server_config(fixture.child("server-data").unwrap(), 12);
    config.max_frame_bytes = 256 * 1024;
    config.max_inflight_frame_bytes = 512 * 1024;
    config.cursor_batch_max_bytes = 128 * 1024;
    let server = Server::bind_ephemeral(&config).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        // A failed assertion must not leave the scoped TCP server alive: the
        // scope waits for every child before it can report the original panic.
        // Without this guard, a useful gate failure is converted into an
        // apparently silent infinite test.
        let _shutdown_on_drop = ShutdownOnDrop(&shutdown);
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        // The process-wide Rayon pool is initialized lazily. Other tests in
        // this binary may legitimately start it after our first snapshot,
        // which would look like a 32-thread leak owned by this server. Admit
        // that stable process infrastructure before defining the corridor.
        let _ = rayon::current_num_threads();
        let baseline = ResourceSnapshot::capture_linux(0, 0).unwrap();
        let mut observer = tcp_connect(address, DATABASE).unwrap();
        observer
            .execute("CREATE TABLE pressure_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
            .unwrap();
        for id in 0..128 {
            observer
                .execute(format!("INSERT INTO pressure_rows VALUES ({id}, {id})"))
                .unwrap();
        }

        let mut control = tcp_connect(address, DATABASE).unwrap();
        let cancellation_queries = [
            "SELECT SLEEP(5)",
            "SELECT COUNT(*) FROM pressure_rows WHERE SLEEP(5) = 0",
            "SELECT COUNT(*) FROM pressure_rows a JOIN pressure_rows b ON a.id = b.id WHERE SLEEP(5) = 0",
            "SELECT id FROM pressure_rows ORDER BY SLEEP(5), id",
        ];
        for query in cancellation_queries.into_iter().cycle().take(16) {
            let (request_tx, request_rx) = mpsc::channel();
            let worker = scope.spawn(move || {
                let mut connection = tcp_connect(address, DATABASE).unwrap();
                let request_id = connection.reserve_request_id().unwrap();
                request_tx.send(request_id).unwrap();
                let result = connection.execute_with_request_id(
                    request_id,
                    query,
                    Vec::new(),
                    BTreeMap::new(),
                );
                assert!(result.is_err());
                assert_eq!(tcp_scalar_i64(&mut connection, "SELECT 1").unwrap(), 1);
            });
            let request_id = request_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if control.cancel_execution(request_id).unwrap() {
                    break;
                }
                assert!(Instant::now() < deadline, "request was never cancellable");
                thread::sleep(Duration::from_millis(2));
            }
            worker.join().unwrap();
        }

        for sql in [
            "SELEC broken",
            "SELECT * FROM missing_bind WHERE id = $1",
            "SELECT (1 + )",
        ] {
            assert!(observer.execute(sql).is_err());
        }
        assert_eq!(tcp_scalar_i64(&mut observer, "SELECT 1").unwrap(), 1);

        let ExecuteResult::Cursor(cursor) = observer
            .execute("SELECT * FROM pressure_rows ORDER BY id")
            .unwrap()
        else {
            panic!("fetch cancellation query did not open a cursor");
        };
        observer.cancel(cursor).unwrap();
        assert_eq!(tcp_scalar_i64(&mut observer, "SELECT 1").unwrap(), 1);

        let mut holders = Vec::<Connection>::new();
        while holders.len() < 10 {
            match tcp_connect(address, DATABASE) {
                Ok(connection) => holders.push(connection),
                Err(_) => break,
            }
        }
        assert!(
            tcp_connect(address, DATABASE).is_err(),
            "connection limit was not enforced under pressure"
        );
        drop(holders);
        drop(control);

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let status = observer.server_status().unwrap();
            if status.runtime.active_connections == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "connection permits did not quiesce"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let after = ResourceSnapshot::capture_linux(1, 0).unwrap();
        after
            .within_quiescent_corridor(&baseline, 128 * 1024 * 1024, 12, 6)
            .unwrap();
        assert_eq!(tcp_scalar_i64(&mut observer, "SELECT 1").unwrap(), 1);

        shutdown.store(true, std::sync::atomic::Ordering::Release);
        server_worker.join().unwrap().unwrap();
    });
}

#[test]
fn b6_resource_limited_child() {
    if std::env::var_os("RADIXDB_PRERELEASE_B6_LIMITED_CHILD").is_none() {
        return;
    }
    let fixture = OwnedFixture::new("radixdb-prerelease-b6-limited-child-").unwrap();
    let mut config = tcp_server_config(fixture.child("server-data").unwrap(), 4);
    config.max_frame_bytes = 128 * 1024;
    config.max_inflight_frame_bytes = 256 * 1024;
    config.cursor_batch_max_bytes = 64 * 1024;
    let server = Server::bind_ephemeral(&config).unwrap();
    let address = server.local_addr().unwrap();
    let shutdown = AtomicBool::new(false);
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut connection = tcp_connect(address, DATABASE).unwrap();
        connection
            .execute("CREATE TABLE limited_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            .unwrap();
        for id in 0..256 {
            connection
                .execute(format!("INSERT INTO limited_rows VALUES ({id}, 'bounded')"))
                .unwrap();
        }
        assert_eq!(
            tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM limited_rows").unwrap(),
            256
        );
        connection.execute("PRAGMA CHECKPOINT").unwrap();
        shutdown.store(true, std::sync::atomic::Ordering::Release);
        worker.join().unwrap().unwrap();
    });
}

#[test]
fn b6_os_resource_limits_are_exercised_in_an_owned_child() {
    let executable = std::env::current_exe().unwrap();
    let output = Command::new("prlimit")
        .args([
            "--as=8589934592",
            "--nofile=128",
            "--nproc=4096",
            "--cpu=30",
            "--",
        ])
        .arg(executable)
        .args(["--exact", "b6_resource_limited_child", "--test-threads=1"])
        .env("RADIXDB_PRERELEASE_B6_LIMITED_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "limited child failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn b6_encoded_payload_boundary_is_format_aware() {
    let payload = encode_payload(&ClientMessage::Handshake {
        protocol_version: PROTOCOL_VERSION,
        max_frame_bytes: 4096,
        capabilities: Vec::new(),
    })
    .unwrap();
    assert!(!payload.is_empty());
    assert!(payload.len() < 4096);
}
