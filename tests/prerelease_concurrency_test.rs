// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

mod common;

use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::{atomic::AtomicBool, Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use common::prerelease::{
    initialize_concurrency_actor_rows, initialize_concurrency_fixture,
    materialize_large_concurrency_fixture, run_capacity_probe, run_concurrency_step,
    run_concurrency_step_in_keyspace, run_concurrency_step_in_keyspace_with_query_timeout,
    run_overload_wave, run_overload_wave_for_fixture, tcp_command, tcp_connect,
    tcp_connect_with_read_timeout, tcp_rows, tcp_scalar_i64, tcp_server_config,
    verify_concurrency_state, verify_concurrency_state_in_keyspace, with_tcp_server,
    ConcurrencyKeyspace, ConcurrencyPlan, ConcurrencyRuntimeSummary, LargeFixtureEvidence,
    MessengerFixtureScale, MessengerSeedPlan, OwnedChildProcess, OwnedFixture,
    CONCURRENCY_DATABASE, LARGE_CONCURRENCY_QUERY_TIMEOUT,
};
use radixdb::server::Server;
use serde::Serialize;

const CHILD_DATA_DIR: &str = "RADIXDB_PRERELEASE_B4_CHILD_DATA_DIR";
const CHILD_ADDRESS_FILE: &str = "RADIXDB_PRERELEASE_B4_CHILD_ADDRESS_FILE";
const CHILD_MAX_CONNECTIONS: &str = "RADIXDB_PRERELEASE_B4_CHILD_MAX_CONNECTIONS";
const RUNNER_CLIENTS: &str = "RADIXDB_PRERELEASE_CLIENTS";
const RUNNER_SEED: &str = "RADIXDB_PRERELEASE_SEED";
const RUNNER_EVIDENCE: &str = "RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE";
const RUNNER_LARGE_FIXTURE: &str = "RADIXDB_PRERELEASE_LARGE_FIXTURE";
const RUNNER_LARGE_ROOT: &str = "RADIXDB_PRERELEASE_LARGE_ROOT";
const EXISTING_LARGE_DATA_DIR: &str = "RADIXDB_PRERELEASE_EXISTING_LARGE_DATA_DIR";
const EXISTING_LARGE_KEYSPACE_BASE: &str = "RADIXDB_PRERELEASE_EXISTING_LARGE_KEYSPACE_BASE";
const EXISTING_LARGE_REPLAY_CLIENTS: &str = "RADIXDB_PRERELEASE_EXISTING_LARGE_REPLAY_CLIENTS";

#[derive(Serialize)]
struct ConcurrencyGateEvidence<'a> {
    clients: usize,
    seed: u64,
    max_connections: usize,
    overload_accepted: usize,
    overload_refused: usize,
    committed: usize,
    rejected: usize,
    ambiguous: usize,
    conflicts: usize,
    disjoint_commits: usize,
    reader_checks: usize,
    reopened: bool,
    large_fixture: Option<&'a LargeFixtureEvidence>,
}

#[derive(Serialize)]
struct CapacityBoundaryEvidence {
    attempted_clients: usize,
    accepted: usize,
    refused: usize,
    server_max_connections: usize,
    controlled: bool,
    reopened: bool,
}

fn write_runner_evidence(
    summary: &ConcurrencyRuntimeSummary,
    clients: usize,
    seed: u64,
    max_connections: usize,
    large_fixture: Option<&LargeFixtureEvidence>,
) {
    let Some(path) = std::env::var_os(RUNNER_EVIDENCE).map(PathBuf::from) else {
        return;
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create concurrency evidence parent");
    }
    let evidence = ConcurrencyGateEvidence {
        clients,
        seed,
        max_connections,
        overload_accepted: summary.overload_accepted,
        overload_refused: summary.overload_refused,
        committed: summary.committed_ids.len(),
        rejected: summary.rejected_ids.len(),
        ambiguous: summary.ambiguous_ids.len(),
        conflicts: summary.conflicts,
        disjoint_commits: summary.disjoint_commits,
        reader_checks: summary.reader_checks,
        reopened: true,
        large_fixture,
    };
    fs::write(
        path,
        serde_json::to_vec_pretty(&evidence).expect("serialize concurrency evidence"),
    )
    .expect("write concurrency evidence");
}

fn write_capacity_evidence(evidence: &CapacityBoundaryEvidence) {
    let Some(path) = std::env::var_os(RUNNER_EVIDENCE).map(PathBuf::from) else {
        return;
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create capacity evidence parent");
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(evidence).expect("serialize capacity evidence"),
    )
    .expect("write capacity evidence");
}

#[test]
fn b4_runner_configured_tcp_step_preserves_contracts() {
    let Some(clients) = std::env::var(RUNNER_CLIENTS).ok().map(|value| {
        value
            .parse::<usize>()
            .expect("runner clients must be usize")
    }) else {
        return;
    };
    assert!(
        clients >= 16 && clients.is_power_of_two(),
        "runner clients must be a power of two and at least 16"
    );
    let seed = std::env::var(RUNNER_SEED)
        .ok()
        .map(|value| value.parse::<u64>().expect("runner seed must be u64"))
        .unwrap_or(0x5eed_4004 + clients as u64);
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b4-runner-").expect("create runner fixture");
    let data_dir = fixture
        .child("server-data")
        .expect("resolve runner data dir");
    let max_connections = clients + 8;
    let plan = ConcurrencyPlan::generate(clients, 2, seed).expect("generate runner plan");
    let summary = with_tcp_server(data_dir.clone(), max_connections, |address| {
        initialize_concurrency_fixture(address, clients).expect("initialize runner fixture");
        let mut summary = run_concurrency_step(address, &plan).expect("run configured step");
        let (accepted, refused) =
            run_overload_wave(address, max_connections).expect("run configured overload wave");
        summary.overload_accepted = accepted;
        summary.overload_refused = refused;
        assert!(summary.invalid_rejected > 0);
        assert!(summary.disjoint_commits > 0);
        assert!(summary.overload_refused > 0);
        summary
    });
    with_tcp_server(data_dir, max_connections, |address| {
        verify_concurrency_state(address, &summary)
            .expect("configured state survives graceful stop/reopen");
    });
    write_runner_evidence(&summary, clients, seed, max_connections, None);
}

#[test]
fn b4_runner_capacity_boundary_is_controlled_and_reopenable() {
    let Some(clients) = std::env::var(RUNNER_CLIENTS).ok().map(|value| {
        value
            .parse::<usize>()
            .expect("runner clients must be usize")
    }) else {
        return;
    };
    assert!(clients >= 128 && clients.is_power_of_two());
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b4-capacity-").expect("create capacity fixture");
    let data_dir = fixture
        .child("server-data")
        .expect("resolve capacity data dir");
    let server_max_connections = clients + 8;
    let (accepted, refused) =
        with_tcp_server(data_dir.clone(), server_max_connections, |address| {
            initialize_concurrency_fixture(address, 16).expect("initialize capacity probe fixture");
            run_capacity_probe(address, clients, 2).expect("run capacity connection wave")
        });
    assert_eq!(accepted + refused, clients);
    assert!(accepted > 0, "capacity boundary admitted no client");
    assert!(
        refused > 0,
        "configured rung remains stable; probe the next power of two"
    );
    with_tcp_server(data_dir, server_max_connections, |address| {
        let mut connection = tcp_connect(address, CONCURRENCY_DATABASE)
            .expect("capacity fixture accepts a client after reopen");
        assert_eq!(
            tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM tenants").unwrap(),
            2
        );
    });
    write_capacity_evidence(&CapacityBoundaryEvidence {
        attempted_clients: clients,
        accepted,
        refused,
        server_max_connections,
        controlled: true,
        reopened: true,
    });
}

#[test]
fn b4_runner_large_fixture_128_clients_preserves_contracts() {
    if std::env::var(RUNNER_LARGE_FIXTURE).as_deref() != Ok("1") {
        return;
    }
    let clients = std::env::var(RUNNER_CLIENTS)
        .expect("large runner receives client count")
        .parse::<usize>()
        .expect("large runner clients must be usize");
    assert!(clients >= 128 && clients.is_power_of_two());
    let seed = std::env::var(RUNNER_SEED)
        .expect("large runner receives seed")
        .parse::<u64>()
        .expect("large runner seed must be u64");
    let root = PathBuf::from(
        std::env::var_os(RUNNER_LARGE_ROOT).expect("large runner receives fixture root"),
    );
    let fixture = OwnedFixture::new_in(&root, "radixdb-prerelease-b4-large-")
        .expect("create large runner fixture");
    let data_dir = fixture
        .child("server-data")
        .expect("resolve large data dir");
    let seed_plan = MessengerSeedPlan::new(MessengerFixtureScale::Large, seed);
    let keyspace =
        ConcurrencyKeyspace::after_large_fixture(&seed_plan).expect("derive post-fixture keyspace");
    let max_connections = clients + 8;
    let plan = ConcurrencyPlan::generate(clients, 2, seed).expect("generate large runner plan");

    let (large_fixture, summary) = with_tcp_server(data_dir.clone(), max_connections, |address| {
        let large_fixture = materialize_large_concurrency_fixture(address, &fixture, seed)
            .expect("materialize 100M messenger fixture");
        initialize_concurrency_actor_rows(address, clients, keyspace)
            .expect("initialize large-fixture actor rows");
        let mut summary = run_concurrency_step_in_keyspace_with_query_timeout(
            address,
            &plan,
            keyspace,
            LARGE_CONCURRENCY_QUERY_TIMEOUT,
        )
        .expect("run 128 clients against 100M fixture");
        let (accepted, refused) =
            run_overload_wave_for_fixture(address, max_connections, keyspace.expected_tenants)
                .expect("run large-fixture overload wave");
        summary.overload_accepted = accepted;
        summary.overload_refused = refused;
        (large_fixture, summary)
    });
    with_tcp_server(data_dir, max_connections, |address| {
        // Reopening a fresh 100M fixture legitimately spends more than the
        // ordinary 30-second interactive timeout rebuilding the catalog and
        // volume topology. Open it once with the same bounded timeout used by
        // the large-fixture workload; subsequent verification queries remain
        // on the ordinary client contract.
        let mut reopened = tcp_connect_with_read_timeout(
            address,
            CONCURRENCY_DATABASE,
            LARGE_CONCURRENCY_QUERY_TIMEOUT,
        )
        .expect("open large fixture after graceful restart");
        verify_concurrency_state_in_keyspace(address, &summary, keyspace)
            .expect("large fixture survives graceful stop/reopen");
        assert_eq!(
            tcp_scalar_i64(&mut reopened, "SELECT COUNT(*) FROM tenants").unwrap(),
            keyspace.expected_tenants
        );
    });
    assert!(summary.overload_refused > 0);
    write_runner_evidence(
        &summary,
        clients,
        seed,
        max_connections,
        Some(&large_fixture),
    );
}

#[test]
fn b4_existing_large_fixture_keeps_point_lookup_responsive_during_view_scans() {
    let Some(data_dir) = std::env::var_os(EXISTING_LARGE_DATA_DIR).map(PathBuf::from) else {
        return;
    };
    assert!(
        data_dir
            .join("databases")
            .join(CONCURRENCY_DATABASE)
            .is_dir(),
        "existing large fixture must contain the concurrency database"
    );

    with_tcp_server(data_dir, 16, |address| {
        let open_deadline = Instant::now() + Duration::from_secs(2 * 60);
        let mut warm = loop {
            match tcp_connect_with_read_timeout(
                address,
                CONCURRENCY_DATABASE,
                LARGE_CONCURRENCY_QUERY_TIMEOUT,
            ) {
                Ok(connection) => break connection,
                Err(error)
                    if error.contains("opening/recovering") && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("reopen existing large fixture: {error}"),
            }
        };
        assert_eq!(
            tcp_scalar_i64(&mut warm, "SELECT COUNT(*) FROM tenants")
                .expect("warm large fixture after reopen"),
            128
        );
        drop(warm);

        let readers = 4usize;
        let start = Arc::new(Barrier::new(readers + 1));
        let mut handles = Vec::with_capacity(readers);
        for _ in 0..readers {
            let start = Arc::clone(&start);
            handles.push(thread::spawn(move || {
                let mut connection = tcp_connect_with_read_timeout(
                    address,
                    CONCURRENCY_DATABASE,
                    LARGE_CONCURRENCY_QUERY_TIMEOUT,
                )?;
                start.wait();
                tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM pending_outbox_v")
            }));
        }

        let mut point =
            tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, Duration::from_secs(30))
                .expect("connect point-lookup probe");
        start.wait();
        thread::sleep(Duration::from_millis(100));
        let point_started = Instant::now();
        let count = tcp_scalar_i64(
            &mut point,
            "SELECT COUNT(*) FROM outbox_jobs WHERE message_id = 1",
        )
        .expect("cold unique-key point lookup must not starve behind view scans");
        assert_eq!(count, 1);
        eprintln!(
            "100M point lookup under four view scans elapsed_ms={}",
            point_started.elapsed().as_millis()
        );

        for handle in handles {
            assert!(
                handle
                    .join()
                    .expect("large view reader joins")
                    .expect("large view reader succeeds")
                    > 0
            );
        }
    });
}

#[test]
fn b4_existing_large_fixture_profiles_single_transaction_ryw_lookup() {
    let Some(data_dir) = std::env::var_os(EXISTING_LARGE_DATA_DIR).map(PathBuf::from) else {
        return;
    };
    let message_id = std::env::var(EXISTING_LARGE_KEYSPACE_BASE)
        .expect("single-transaction profile receives a unique message id")
        .parse::<i64>()
        .expect("single-transaction message id must be i64");
    let outbox_id = message_id.saturating_add(100_000_000);

    with_tcp_server(data_dir, 8, |address| {
        let open_deadline = Instant::now() + Duration::from_secs(5 * 60);
        let mut connection = loop {
            match tcp_connect_with_read_timeout(
                address,
                CONCURRENCY_DATABASE,
                LARGE_CONCURRENCY_QUERY_TIMEOUT,
            ) {
                Ok(connection) => break connection,
                Err(error)
                    if error.contains("opening/recovering") && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("reopen single-transaction profile fixture: {error}"),
            }
        };
        connection.begin().expect("begin single RYW transaction");
        tcp_command(
            &mut connection,
            format!(
                "INSERT INTO messages VALUES ({message_id}, 10, {message_id}, 1, 'single-ryw-{message_id}', false)"
            ),
        )
        .expect("insert single RYW message");
        tcp_command(
            &mut connection,
            format!(
                "INSERT INTO outbox_jobs VALUES ({outbox_id}, {message_id}, 'pending', 0, NULL, true)"
            ),
        )
        .expect("insert single RYW outbox row");

        let sql = format!("SELECT COUNT(*) FROM outbox_jobs WHERE message_id = {message_id}");
        let explain_sql = format!("EXPLAIN {sql}");
        let explain = tcp_rows(&mut connection, &explain_sql).expect("explain single RYW lookup");
        eprintln!("single RYW explain={explain:?}");
        let started = Instant::now();
        assert_eq!(tcp_scalar_i64(&mut connection, &sql).unwrap(), 1);
        eprintln!(
            "single RYW lookup elapsed_ms={}",
            started.elapsed().as_millis()
        );
        connection.rollback().expect("rollback single RYW profile");
    });
}

#[test]
fn b4_existing_large_fixture_profiles_count_antijoin_without_join_materialization() {
    let Some(data_dir) = std::env::var_os(EXISTING_LARGE_DATA_DIR).map(PathBuf::from) else {
        return;
    };

    with_tcp_server(data_dir, 8, |address| {
        let open_deadline = Instant::now() + Duration::from_secs(5 * 60);
        let mut connection = loop {
            match tcp_connect_with_read_timeout(
                address,
                CONCURRENCY_DATABASE,
                LARGE_CONCURRENCY_QUERY_TIMEOUT,
            ) {
                Ok(connection) => break connection,
                Err(error)
                    if error.contains("opening/recovering") && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("reopen anti-join profile fixture: {error}"),
            }
        };

        for (sql, expected) in [
            (
                "SELECT COUNT(*) FROM messages m LEFT JOIN outbox_jobs o \
                 ON o.message_id = m.id WHERE o.id IS NULL",
                41_000_000,
            ),
            (
                "SELECT COUNT(*) FROM outbox_jobs o LEFT JOIN messages m \
                 ON m.id = o.message_id WHERE m.id IS NULL",
                0,
            ),
            (
                "SELECT COUNT(*) FROM sync_events s LEFT JOIN messages m \
                 ON m.id = s.message_id WHERE s.message_id IS NOT NULL AND m.id IS NULL",
                0,
            ),
        ] {
            let started = Instant::now();
            assert_eq!(
                tcp_scalar_i64(&mut connection, sql).unwrap(),
                expected,
                "{sql}"
            );
            eprintln!(
                "100M count anti-join expected={expected} elapsed_ms={}",
                started.elapsed().as_millis()
            );
        }
    });
}

#[test]
fn b4_existing_large_fixture_replays_128_client_phase() {
    let Some(data_dir) = std::env::var_os(EXISTING_LARGE_DATA_DIR).map(PathBuf::from) else {
        return;
    };
    let keyspace_base = std::env::var(EXISTING_LARGE_KEYSPACE_BASE)
        .expect("existing large phase replay receives a unique keyspace base")
        .parse::<i64>()
        .expect("existing large phase keyspace base must be i64");
    assert!(keyspace_base >= 2_000_000_000);
    let clients = std::env::var(EXISTING_LARGE_REPLAY_CLIENTS)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("existing large replay clients must be usize")
        })
        .unwrap_or(128);
    assert!((16..=128).contains(&clients) && clients.is_power_of_two());
    let seed = 1_592_595_072u64;
    let keyspace = ConcurrencyKeyspace {
        message_base: keyspace_base,
        disjoint_user_base: keyspace_base.saturating_add(100_000_000),
        disjoint_conversation_base: keyspace_base.saturating_add(200_000_000),
        disjoint_membership_base: keyspace_base.saturating_add(300_000_000),
        expected_tenants: 128,
    };
    let plan = ConcurrencyPlan::generate(clients, 2, seed).expect("generate replay plan");

    with_tcp_server(data_dir, clients + 8, |address| {
        let open_deadline = Instant::now() + Duration::from_secs(2 * 60);
        loop {
            match tcp_connect_with_read_timeout(
                address,
                CONCURRENCY_DATABASE,
                LARGE_CONCURRENCY_QUERY_TIMEOUT,
            ) {
                Ok(mut connection) => {
                    assert_eq!(
                        tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM tenants")
                            .expect("warm replay fixture"),
                        keyspace.expected_tenants
                    );
                    break;
                }
                Err(error)
                    if error.contains("opening/recovering") && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("reopen replay fixture: {error}"),
            }
        }

        initialize_concurrency_actor_rows(address, clients, keyspace)
            .expect("initialize isolated replay actors");
        let summary = run_concurrency_step_in_keyspace_with_query_timeout(
            address,
            &plan,
            keyspace,
            LARGE_CONCURRENCY_QUERY_TIMEOUT,
        )
        .expect("replay 128-client phase on existing 100M fixture");
        verify_concurrency_state_in_keyspace(address, &summary, keyspace)
            .expect("verify isolated replay state");
    });
}

#[test]
fn b4_tcp_ladder_16_and_32_preserves_atomicity_progress_and_permits() {
    for clients in [16usize, 32] {
        let fixture =
            OwnedFixture::new("radixdb-prerelease-b4-ladder-").expect("create B4 ladder fixture");
        let data_dir = fixture.child("server-data").expect("resolve B4 data dir");
        let max_connections = clients + 8;
        let plan = ConcurrencyPlan::generate(clients, 2, 0x5eed_4004 + clients as u64)
            .expect("generate B4 concurrency plan");
        let summary = with_tcp_server(data_dir.clone(), max_connections, |address| {
            initialize_concurrency_fixture(address, clients).expect("initialize B4 fixture");
            let mut summary = run_concurrency_step(address, &plan).expect("run B4 ladder step");
            let (accepted, refused) =
                run_overload_wave(address, max_connections).expect("run overload wave");
            summary.overload_accepted = accepted;
            summary.overload_refused = refused;
            assert!(summary.invalid_rejected > 0);
            assert!(summary.rolled_back > 0 || summary.disconnected > 0);
            assert!(summary.disjoint_commits > 0);
            assert!(summary.reader_checks >= 4 * 8 * 6);
            assert!(summary.overload_refused > 0);
            summary
        });
        with_tcp_server(data_dir, max_connections, |address| {
            verify_concurrency_state(address, &summary)
                .expect("B4 state survives graceful stop/reopen");
        });
    }
}

#[test]
fn b4_post_fixture_keyspace_keeps_seed_rows_disjoint_and_survives_reopen() {
    let clients = 16usize;
    let seed = 0x5eed_4f00;
    let fixture =
        OwnedFixture::new("radixdb-prerelease-b4-keyspace-").expect("create keyspace fixture");
    let data_dir = fixture
        .child("server-data")
        .expect("resolve keyspace data dir");
    let seed_plan = MessengerSeedPlan::new(MessengerFixtureScale::Small, seed);
    let keyspace = ConcurrencyKeyspace {
        message_base: 1_000_000_000,
        disjoint_user_base: 100_000,
        disjoint_conversation_base: 200_000,
        disjoint_membership_base: 300_000,
        expected_tenants: 2,
    };
    let max_connections = clients + 8;
    let plan = ConcurrencyPlan::generate(clients, 2, seed).expect("generate keyspace plan");
    let summary = with_tcp_server(data_dir.clone(), max_connections, |address| {
        initialize_concurrency_fixture(address, clients).expect("initialize seed fixture");
        initialize_concurrency_actor_rows(address, clients, keyspace)
            .expect("initialize isolated actor rows");
        run_concurrency_step_in_keyspace(address, &plan, keyspace)
            .expect("run isolated-keyspace step")
    });
    with_tcp_server(data_dir, max_connections, |address| {
        verify_concurrency_state_in_keyspace(address, &summary, keyspace)
            .expect("isolated keyspace survives reopen");
        let mut connection = tcp_connect(address, CONCURRENCY_DATABASE).unwrap();
        assert_eq!(
            tcp_scalar_i64(
                &mut connection,
                "SELECT COUNT(*) FROM messages WHERE id < 1000000000"
            )
            .unwrap(),
            seed_plan.messages as i64
        );
    });
}

#[test]
fn b4_child_server_entrypoint() {
    let Some(data_dir) = std::env::var_os(CHILD_DATA_DIR) else {
        return;
    };
    let address_file =
        std::env::var_os(CHILD_ADDRESS_FILE).expect("child server receives address-file path");
    let max_connections = std::env::var(CHILD_MAX_CONNECTIONS)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("child max connections is usize")
        })
        .unwrap_or(48);
    let config = tcp_server_config(data_dir.into(), max_connections);
    let server = Server::bind_ephemeral(&config).expect("bind B4 child server");
    let address = server.local_addr().expect("resolve B4 child address");
    fs::write(&address_file, address.to_string()).expect("publish B4 child address");
    let shutdown = AtomicBool::new(false);
    server
        .run_until(&shutdown)
        .expect("B4 child server only exits when killed");
}

#[test]
fn b4_process_kill_with_32_live_clients_recovers_a_committed_prefix() {
    if std::env::var_os(CHILD_DATA_DIR).is_some() {
        return;
    }
    run_process_kill_with_live_clients(32);
}

#[test]
fn b4_runner_max_capacity_kill_reopen_is_fail_closed() {
    if std::env::var_os(CHILD_DATA_DIR).is_some() {
        return;
    }
    let Some(clients) = std::env::var(RUNNER_CLIENTS).ok().map(|value| {
        value
            .parse::<usize>()
            .expect("runner clients must be usize")
    }) else {
        return;
    };
    assert!(clients >= 128 && clients.is_power_of_two());
    run_process_kill_with_live_clients(clients);
}

fn run_process_kill_with_live_clients(clients: usize) {
    let fixture = OwnedFixture::new("radixdb-prerelease-b4-kill-").expect("create B4 kill fixture");
    let data_dir = fixture
        .child("server-data")
        .expect("resolve child data dir");
    let address_file = fixture
        .child("child-address")
        .expect("resolve address file");
    let executable = std::env::current_exe().expect("resolve B4 test executable");
    let mut command = Command::new(executable);
    command
        .arg("--exact")
        .arg("b4_child_server_entrypoint")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_DATA_DIR, &data_dir)
        .env(CHILD_ADDRESS_FILE, &address_file)
        .env(CHILD_MAX_CONNECTIONS, (clients + 16).to_string());
    let child = OwnedChildProcess::spawn(&fixture, &mut command).expect("spawn B4 child server");

    let deadline = Instant::now() + Duration::from_secs(10);
    let address: SocketAddr = loop {
        if let Ok(text) = fs::read_to_string(&address_file) {
            break text.trim().parse().expect("parse child server address");
        }
        assert!(
            Instant::now() < deadline,
            "B4 child did not publish its address"
        );
        thread::sleep(Duration::from_millis(25));
    };

    let mut owner = tcp_connect(address, CONCURRENCY_DATABASE).expect("connect B4 kill owner");
    tcp_command(
        &mut owner,
        "CREATE TABLE kill_ledger (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    )
    .expect("create kill ledger");
    tcp_command(
        &mut owner,
        "INSERT INTO kill_ledger VALUES (1, 'committed')",
    )
    .expect("insert committed prefix");
    tcp_command(&mut owner, "PRAGMA CHECKPOINT").expect("checkpoint committed prefix");

    let mut live_clients = Vec::with_capacity(clients);
    for _ in 0..clients {
        live_clients
            .push(tcp_connect(address, CONCURRENCY_DATABASE).expect("open sustained B4 client"));
    }
    live_clients[0].begin().expect("begin killed transaction");
    tcp_command(
        &mut live_clients[0],
        "INSERT INTO kill_ledger VALUES (2, 'must-disappear')",
    )
    .expect("insert uncommitted row before kill");
    assert_eq!(
        tcp_scalar_i64(
            &mut live_clients[0],
            "SELECT COUNT(*) FROM kill_ledger WHERE id = 2",
        )
        .unwrap(),
        1
    );

    let status = child.terminate().expect("kill B4 child server");
    assert!(!status.success(), "child kill unexpectedly exited cleanly");
    drop(live_clients);
    drop(owner);

    with_tcp_server(data_dir, clients + 16, |restart_address| {
        let mut verifier =
            tcp_connect(restart_address, CONCURRENCY_DATABASE).expect("reopen killed database");
        assert_eq!(
            tcp_scalar_i64(&mut verifier, "SELECT COUNT(*) FROM kill_ledger").unwrap(),
            1
        );
        let rows = tcp_rows(&mut verifier, "SELECT id FROM kill_ledger ORDER BY id").unwrap();
        assert_eq!(rows.len(), 1);
        tcp_command(
            &mut verifier,
            "INSERT INTO kill_ledger VALUES (3, 'after-reopen')",
        )
        .expect("write after B4 reopen");
        assert_eq!(
            tcp_scalar_i64(&mut verifier, "SELECT COUNT(*) FROM kill_ledger").unwrap(),
            2
        );
    });
}
