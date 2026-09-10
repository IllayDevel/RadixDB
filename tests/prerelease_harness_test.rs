// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

mod common;

use std::{collections::BTreeMap, fs, process::Command, thread, time::Duration};

use common::prerelease::{
    historical_workload_signature, messenger_schema_plan, messenger_view_plan, sha256_file,
    ActorPlan, ArtifactStore, CandidateIdentity, ConcurrencyOperationKind, ConcurrencyPlan,
    ConnectionSnapshot, CounterSet, DeterministicStream, FaultActivation, FaultPoint,
    FaultSchedule, HostProfile, KeyDistribution, LatencySummary, MessengerFixtureScale,
    MessengerMessageModel, MessengerSeedPlan, MessengerSeedTable, MessengerStateModel,
    MessengerStorageTier, ModelLedger, ModelTransactionState, OperationTrace, OracleReport,
    OwnedChildProcess, OwnedFixture, ProcessSnapshot, ResourceSnapshot, RunConfig, RunManifest,
    RunProfile, RunReport, RunStatus, SchemaPlan, SchemaStatement, StageResult, StageStatus,
    ThreadSnapshot, TimeoutSnapshot, TraceEvent, TraceOutcome, TraceValue, TransactionCohort,
    TransactionTerminal, Watchdog, WatchdogOutcome,
};

const CHILD_PROCESS_TEST_ENV: &str = "RADIXDB_PRERELEASE_CHILD_PROCESS_TEST";

fn manifest(seed: u64) -> RunManifest {
    let mut labels = BTreeMap::new();
    labels.insert("suite".to_string(), "prerelease".to_string());
    let config = RunConfig {
        labels,
        ..RunConfig::historical_core("b0-harness", seed)
    };
    RunManifest::new(
        config,
        CandidateIdentity {
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            dirty: false,
            cargo_lock_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            release_binary_sha256:
                "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
            server_config_sha256:
                "3333333333333333333333333333333333333333333333333333333333333333".to_string(),
        },
        HostProfile {
            hostname: "test-host".to_string(),
            operating_system: "test-os".to_string(),
            kernel: "test-kernel".to_string(),
            filesystem: "test-fs".to_string(),
            cpu_count: 16,
            memory_bytes: 32 * 1024 * 1024 * 1024,
        },
        "rustc test",
    )
    .expect("valid test manifest")
}

#[test]
fn b0_manifest_and_seed_stream_are_deterministic() {
    let manifest = manifest(0x5eed);
    let first = manifest.to_pretty_json().expect("serialize manifest");
    let second = manifest.to_pretty_json().expect("serialize manifest again");
    assert_eq!(first, second);
    assert_eq!(
        RunManifest::from_json(&first).expect("deserialize manifest"),
        manifest
    );

    let mut left = DeterministicStream::new(manifest.config.seed);
    let mut right = DeterministicStream::new(manifest.config.seed);
    let left_values: Vec<_> = (0..128).map(|_| left.next_u64()).collect();
    let right_values: Vec<_> = (0..128).map(|_| right.next_u64()).collect();
    assert_eq!(left_values, right_values);

    assert!(RunProfile::ConcurrencyLadder { clients: 16 }
        .validate()
        .is_ok());
    assert!(RunProfile::ConcurrencyLadder { clients: 48 }
        .validate()
        .is_err());
}

#[test]
fn b0_fixture_rejects_paths_outside_its_owned_root() {
    let fixture = OwnedFixture::new("radixdb-prerelease-").expect("create fixture");
    assert!(fixture.child("data/server").is_ok());
    for unsafe_path in ["", "..", "../outside", "/tmp/outside", "data/../../outside"] {
        assert!(
            fixture.child(unsafe_path).is_err(),
            "unsafe path was accepted: {unsafe_path}"
        );
        assert!(
            fixture.remove_child(unsafe_path).is_err(),
            "unsafe destructive path was accepted: {unsafe_path}"
        );
    }

    let child = fixture.child("owned").expect("owned child path");
    fs::create_dir_all(&child).expect("create owned child");
    fs::write(child.join("proof"), b"owned").expect("write owned proof");
    fixture.remove_child("owned").expect("remove owned child");
    assert!(!child.exists());
}

#[test]
fn b0_large_fixture_owner_uses_explicit_parent_without_owning_parent() {
    let parent = tempfile::tempdir().expect("create explicit fixture parent");
    let sibling = parent.path().join("must-survive");
    fs::write(&sibling, b"not owned").expect("write sibling proof");
    let owned_root = {
        let fixture = OwnedFixture::new_in(parent.path(), "radixdb-prerelease-large-")
            .expect("create fixture under explicit parent");
        assert_eq!(fixture.root().parent(), Some(parent.path()));
        let root = fixture.root().to_path_buf();
        fs::write(fixture.child("proof").unwrap(), b"owned").expect("write owned proof");
        root
    };
    assert!(!owned_root.exists());
    assert_eq!(fs::read(&sibling).unwrap(), b"not owned");
}

#[test]
fn b0_child_process_can_only_be_terminated_through_its_owner() {
    if std::env::var_os(CHILD_PROCESS_TEST_ENV).is_some() {
        thread::sleep(Duration::from_secs(60));
        return;
    }

    let fixture = OwnedFixture::new("radixdb-prerelease-").expect("create fixture");
    let executable = std::env::current_exe().expect("resolve current test executable");
    let mut command = Command::new(executable);
    command
        .arg("--exact")
        .arg("b0_child_process_can_only_be_terminated_through_its_owner")
        .arg("--nocapture")
        .env(CHILD_PROCESS_TEST_ENV, "1");
    let child = OwnedChildProcess::spawn(&fixture, &mut command).expect("spawn owned child");
    assert!(child.pid() > 0);
    let status = child.terminate().expect("terminate owned child");
    assert!(!status.success(), "terminated child exited successfully");
}

#[test]
fn b0_trace_timeout_and_metrics_round_trip_without_losing_evidence() {
    let fixture = OwnedFixture::new("radixdb-prerelease-").expect("create fixture");
    let store = ArtifactStore::create(&fixture, "artifacts").expect("create artifact store");
    let manifest = manifest(7);
    store.write_manifest(&manifest).expect("write manifest");

    let mut trace = OperationTrace::new(&manifest.config.run_id, manifest.config.seed);
    trace
        .push(TraceEvent {
            sequence: u64::MAX,
            logical_epoch: 3,
            actor_id: 2,
            session_id: 9,
            transaction_id: Some(11),
            operation: "execute".to_string(),
            statement: Some("INSERT INTO t VALUES (?)".to_string()),
            parameters: vec![TraceValue::Integer(42), TraceValue::FloatBits(1)],
            started_tick: 100,
            finished_tick: 105,
            expected_outcome: TraceOutcome::Committed,
            outcome: TraceOutcome::Committed,
        })
        .expect("append trace event");
    let trace_path = store.write_trace(&trace).expect("write trace");

    let mut counters = CounterSet::default();
    counters.add("connections", 16).unwrap();
    counters.increment("timeouts").unwrap();
    let timeout = TimeoutSnapshot {
        run_id: manifest.config.run_id.clone(),
        stage: "synthetic-timeout".to_string(),
        trace_events: trace.events.len(),
        trace_artifact: "trace.json".to_string(),
        trace_sha256: sha256_file(&trace_path).expect("hash trace"),
        process: ProcessSnapshot {
            pid: std::process::id(),
            executable: "prerelease_harness_test".to_string(),
        },
        threads: vec![ThreadSnapshot {
            name: "test-worker".to_string(),
            state: "waiting-for-timeout".to_string(),
        }],
        connections: vec![ConnectionSnapshot {
            session_id: 9,
            state: "active".to_string(),
            transaction_id: Some(11),
            open_cursors: 2,
        }],
        resources: ResourceSnapshot {
            rss_bytes: 1024,
            open_file_descriptors: 8,
            active_workers: 3,
            active_sessions: 4,
            active_cursors: 2,
        },
        counters: counters.clone(),
    };
    let watchdog = Watchdog::start(Duration::from_millis(10), store.clone(), {
        let timeout = timeout.clone();
        move || timeout
    })
    .expect("start watchdog");
    let outcome = watchdog.wait().expect("wait for synthetic timeout");
    assert!(matches!(outcome, WatchdogOutcome::TimedOut { .. }));

    let restored_manifest: RunManifest = store.read_json("manifest.json").unwrap();
    let restored_trace: OperationTrace = store.read_json("trace.json").unwrap();
    let restored_timeout: TimeoutSnapshot = store.read_json("timeout.json").unwrap();
    assert_eq!(restored_manifest, manifest);
    assert_eq!(restored_trace, trace);
    assert_eq!(restored_timeout, timeout);
    assert_eq!(restored_timeout.counters.get("connections"), 16);
    assert_eq!(restored_timeout.connections[0].transaction_id, Some(11));
    assert_eq!(restored_timeout.resources.active_cursors, 2);

    assert_eq!(
        LatencySummary::from_samples(&[1, 2, 3, 4, 100]).unwrap(),
        LatencySummary {
            samples: 5,
            min_micros: 1,
            p50_micros: 3,
            p95_micros: 100,
            p99_micros: 100,
            max_micros: 100,
        }
    );
}

#[test]
fn b0_common_owner_modules_produce_one_validated_report() {
    let schema = SchemaPlan {
        version: 1,
        statements: vec![SchemaStatement {
            id: "create-users".to_string(),
            sql: "CREATE TABLE users (id INTEGER PRIMARY KEY)".to_string(),
        }],
    };
    schema.validate().expect("validate schema plan");
    assert_eq!(schema.fingerprint().unwrap().len(), 64);

    let config = RunConfig {
        operation_budget: 64,
        ..RunConfig::historical_core("b0-owner-graph", 99)
    };
    let first_plan = ActorPlan::generate(&config).expect("generate actor plan");
    let second_plan = ActorPlan::generate(&config).expect("regenerate actor plan");
    first_plan.validate().expect("validate actor plan");
    assert_eq!(first_plan, second_plan, "same seed changed logical stream");

    let schedule = FaultSchedule {
        activations: vec![
            FaultActivation {
                after_event: 4,
                point: FaultPoint::WalSync,
            },
            FaultActivation {
                after_event: 8,
                point: FaultPoint::CommitVisibility,
            },
        ],
    };
    schedule.validate().expect("validate fault schedule");
    assert_eq!(
        schedule.at(4).collect::<Vec<_>>(),
        vec![FaultPoint::WalSync]
    );

    let mut ledger = ModelLedger::default();
    ledger.begin(1, 7).unwrap();
    ledger
        .record(1, "insert user", "users/42", TraceValue::Integer(42))
        .unwrap();
    assert_eq!(ledger.commit(1).unwrap(), 1);
    ledger.begin(2, 8).unwrap();
    ledger.finish(2, ModelTransactionState::RolledBack).unwrap();
    ledger.validate().expect("validate model ledger");
    assert_eq!(
        ledger.transaction(1).unwrap().state,
        ModelTransactionState::Committed
    );

    let mut oracle = OracleReport::default();
    oracle
        .record(
            "model-ledger",
            true,
            "committed and rollback states are exact",
        )
        .unwrap();
    oracle.validate().expect("validate oracle report");

    let mut counters = CounterSet::default();
    counters.add("planned_operations", 64).unwrap();
    let report = RunReport {
        manifest: manifest(config.seed),
        status: RunStatus::Passed,
        stages: vec![StageResult {
            name: "b0-owner-graph".to_string(),
            status: StageStatus::Passed,
            wall_millis: 1,
            detail: "schema|model trace is stable".to_string(),
        }],
        counters,
        latencies: BTreeMap::from([(
            "operation".to_string(),
            LatencySummary::from_samples(&[1, 2, 3]).unwrap(),
        )]),
        oracle,
    };
    report.validate().expect("validate run report");
    let markdown = report.to_markdown().expect("render report");
    assert!(markdown.contains("schema\\|model trace is stable"));

    let fixture = OwnedFixture::new("radixdb-prerelease-").expect("create fixture");
    let store = ArtifactStore::create(&fixture, "artifacts").expect("create artifact store");
    let artifacts = store.write_report(&report).expect("write report artifacts");
    assert!(artifacts.json.exists());
    assert!(artifacts.markdown.exists());
    let restored: RunReport = store.read_json("report.json").expect("read report JSON");
    assert_eq!(restored, report);
}

#[test]
fn b1_historical_workload_signature_is_unchanged_after_harness_move() {
    let signature = historical_workload_signature();
    assert_eq!(signature.transaction_attempts, 512);
    assert_eq!(signature.planned_commits, 308);
    assert_eq!(signature.planned_rollbacks, 101);
    assert_eq!(signature.planned_disconnects, 103);
    assert_eq!(signature.reader_connections, 256);
    assert_eq!(signature.raw_disconnects, 256);
    assert_eq!(signature.checkpoint_attempts, 8);
    assert_eq!(
        signature.planned_commits + signature.planned_rollbacks + signature.planned_disconnects,
        signature.transaction_attempts
    );
}

#[test]
fn b2_schema_seed_and_sparse_model_are_deterministic_and_independent() {
    let schema = messenger_schema_plan();
    schema.validate().expect("validate messenger schema");
    assert_eq!(
        schema
            .statements
            .iter()
            .filter(|statement| statement.sql.starts_with("CREATE TABLE"))
            .count(),
        18
    );
    assert!(schema
        .statements
        .iter()
        .any(|statement| statement.sql.contains("UNIQUE (conversation_id, sequence)")));
    assert!(schema
        .statements
        .iter()
        .any(|statement| statement.sql.contains("REFERENCES attachments(id)")));

    let views = messenger_view_plan();
    views.validate().expect("validate messenger views");
    assert_eq!(views.statements.len(), 8);
    assert!(views
        .statements
        .iter()
        .any(|statement| statement.id == "user_inbox_v"
            && statement.sql.contains("active_conversations_v")));

    for scale in [
        MessengerFixtureScale::Small,
        MessengerFixtureScale::Medium,
        MessengerFixtureScale::Large,
    ] {
        let left = MessengerSeedPlan::new(scale, 0x5eed);
        let right = MessengerSeedPlan::new(scale, 0x5eed);
        assert_eq!(left, right);
        assert_eq!(left.deterministic_owner(42), right.deterministic_owner(42));
        assert_eq!(
            left.deterministic_conversation(42),
            right.deterministic_conversation(42)
        );
        for table in MessengerSeedTable::ALL {
            assert_eq!(left.row_count(table), right.row_count(table));
            assert_eq!(
                left.cold_rows(table) + left.hot_rows(table),
                left.row_count(table)
            );
            let first = left.record(table, 0).expect("table has a first seed row");
            let same = right.record(table, 0).expect("same deterministic row");
            assert_eq!(first, same);
            assert!((1..=left.tenants).contains(&first.tenant_id));
            assert!((1..=left.users).contains(&first.owner_id));
            assert!((1..=left.conversations).contains(&first.conversation_id));
            let last = left
                .record(table, left.row_count(table) - 1)
                .expect("table has a last seed row");
            if left.row_count(table) > 1 {
                assert_eq!(first.storage_tier, MessengerStorageTier::Cold);
                assert_eq!(last.storage_tier, MessengerStorageTier::Hot);
            }
            assert!(left.record(table, left.row_count(table)).is_none());
        }
        if scale == MessengerFixtureScale::Small {
            for table in MessengerSeedTable::ALL {
                assert_eq!(
                    left.rows(table).count() as u64,
                    left.row_count(table),
                    "{} seed iterator changed cardinality",
                    table.name()
                );
            }
        }
    }
    let large = MessengerSeedPlan::new(MessengerFixtureScale::Large, 0x5eed);
    assert_eq!(large.total_rows(), 100_000_128);

    let mut model = MessengerStateModel::new(&large);
    model.register_user(1).unwrap();
    model.register_user(2).unwrap();
    model.register_conversation(10).unwrap();
    model.begin(7).unwrap();
    model
        .stage_message(
            7,
            MessengerMessageModel {
                id: 100,
                conversation_id: 10,
                sequence: 1,
                sender_id: 1,
                body: "first".to_string(),
            },
            1,
            "command-100",
        )
        .unwrap();
    assert!(model
        .stage_message(
            7,
            MessengerMessageModel {
                id: 101,
                conversation_id: 10,
                sequence: 3,
                sender_id: 2,
                body: "gap".to_string(),
            },
            2,
            "command-gap",
        )
        .is_err());
    let epoch = model.commit(7).unwrap();
    model.mark_durable(epoch).unwrap();
    model.validate().unwrap();
    assert_eq!(model.committed_message_count(), 1);
    assert_eq!(model.durable_epoch(), 1);
    assert_eq!(model.sparse_deltas.len(), 1);
    assert_eq!(model.baseline.row_counts.len(), 18);
    assert_eq!(model.baseline.row_counts["messages"], 46_000_000);

    model.begin(8).unwrap();
    assert!(model
        .stage_message(
            8,
            MessengerMessageModel {
                id: 102,
                conversation_id: 10,
                sequence: 2,
                sender_id: 1,
                body: "duplicate command".to_string(),
            },
            1,
            "command-100",
        )
        .is_err());
    model.rollback(8).unwrap();
    assert_eq!(model.committed_message_count(), 1);
}

#[test]
fn b4_concurrency_plan_is_deterministic_and_covers_the_full_ladder_contract() {
    for clients in [16, 32, 64, 128, 256, 512] {
        let left = ConcurrencyPlan::generate(clients, 5, 0x5eed_4004).unwrap();
        let right = ConcurrencyPlan::generate(clients, 5, 0x5eed_4004).unwrap();
        assert_eq!(left, right);
        left.validate().unwrap();
        assert_eq!(left.transactions.len(), clients * 5);
        assert!(left.large_fixture.total_rows() >= 100_000_000);
        let (hot, disjoint, cold) = left.distribution_counts();
        assert_eq!(hot + disjoint + cold, clients * 5);
        assert_eq!(
            (hot, disjoint, cold),
            (clients * 3, clients * 3 / 2, clients / 2)
        );
        assert!(left.transactions.iter().any(|transaction| {
            transaction.cohort == TransactionCohort::InvalidFinalState
                && transaction.terminal == TransactionTerminal::Commit
        }));
        assert!(left.transactions.iter().any(|transaction| {
            transaction.distribution == KeyDistribution::Disjoint
                && transaction.cohort == TransactionCohort::ValidFinalState
        }));
        let kinds: std::collections::BTreeSet<_> = left
            .transactions
            .iter()
            .flat_map(|transaction| transaction.operations.iter())
            .map(|operation| operation.kind)
            .collect();
        assert_eq!(
            kinds,
            [
                ConcurrencyOperationKind::Insert,
                ConcurrencyOperationKind::Update,
                ConcurrencyOperationKind::Delete,
                ConcurrencyOperationKind::Upsert,
                ConcurrencyOperationKind::ReadYourWrites,
            ]
            .into()
        );
    }
}
