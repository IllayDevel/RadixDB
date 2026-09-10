// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use common::prerelease::{
    execute_state_machine_plan, replay_artifacts, replay_trace, shrink_failing_trace,
    trace_has_outcome_mismatch, ArtifactStore, CandidateIdentity, HostProfile, OperationTrace,
    OwnedFixture, ReplayModel, RunConfig, RunManifest, RunProfile, StateMachinePlan, TraceOutcome,
    TraceValue,
};

const RUN_ID: &str = "b3-state-machine";
const SEED: u64 = 0x5eed_3003;
const RUNNER_REPLAY_TRACE: &str = "RADIXDB_PRERELEASE_REPLAY_TRACE";

fn manifest() -> RunManifest {
    RunManifest::new(
        RunConfig {
            run_id: RUN_ID.to_string(),
            seed: SEED,
            profile: RunProfile::Replay {
                source_run_id: RUN_ID.to_string(),
            },
            max_connections: 16,
            operation_budget: 64,
            timeout_secs: 30,
            labels: BTreeMap::from([("suite".to_string(), "state-machine".to_string())]),
        },
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
    .expect("valid B3 manifest")
}

fn completed_trace() -> OperationTrace {
    let first = StateMachinePlan::generate(RUN_ID, SEED).expect("generate B3 plan");
    let second = StateMachinePlan::generate(RUN_ID, SEED).expect("regenerate B3 plan");
    assert_eq!(first, second, "same seed changed the pre-generated trace");
    assert!(first
        .trace
        .events
        .iter()
        .all(|event| event.outcome == TraceOutcome::Pending));
    execute_state_machine_plan(&first)
        .expect("execute deterministic B3 plan")
        .trace
}

fn corrupt_expected_select(trace: &mut OperationTrace) {
    let select = trace
        .events
        .iter_mut()
        .find(|event| event.operation == "select.key")
        .expect("B3 corpus has a SELECT");
    select.expected_outcome = TraceOutcome::Rows {
        count: 1,
        values: vec![TraceValue::Text("deliberately-wrong".to_string())],
    };
}

#[test]
fn b3_pre_generated_corpus_covers_all_outcomes_and_validates_model() {
    let plan = StateMachinePlan::generate(RUN_ID, SEED).expect("generate B3 corpus");
    let operation_names: BTreeSet<_> = plan
        .trace
        .events
        .iter()
        .map(|event| event.operation.as_str())
        .collect();
    for required in [
        "dml.insert",
        "dml.update",
        "dml.delete",
        "select.key",
        "transaction.begin",
        "transaction.commit",
        "transaction.rollback",
        "ddl.create_view",
        "ddl.drop_view",
        "checkpoint",
        "backup",
        "disconnect",
        "cancel",
        "restart",
    ] {
        assert!(
            operation_names.contains(required),
            "missing operation {required}"
        );
    }
    let expected: BTreeSet<&'static str> = plan
        .trace
        .events
        .iter()
        .map(|event| match event.expected_outcome {
            TraceOutcome::Rows { .. } => "rows",
            TraceOutcome::Committed => "commit",
            TraceOutcome::RolledBack => "rollback",
            TraceOutcome::DeclaredConflict { .. } => "conflict",
            TraceOutcome::DeclaredError { .. } => "error",
            TraceOutcome::AmbiguousDisconnect => "ambiguous",
            TraceOutcome::Pending => "pending",
        })
        .collect();
    assert_eq!(
        expected,
        [
            "ambiguous",
            "commit",
            "conflict",
            "error",
            "rollback",
            "rows"
        ]
        .into()
    );

    let run = execute_state_machine_plan(&plan).expect("execute B3 corpus");
    run.model.validate().expect("validate final model");
    let key = i64::try_from(SEED % 1_000_000).unwrap() + 1;
    assert_eq!(
        run.model.committed_value(key),
        Some(&TraceValue::Text("alpha".to_string()))
    );
    assert_eq!(run.model.committed_epoch(), 1);
    assert!(run
        .trace
        .events
        .iter()
        .all(|event| event.outcome != TraceOutcome::Pending));
}

#[test]
fn b3_standalone_replay_reproduces_a_deliberately_wrong_expected_row() {
    let fixture = OwnedFixture::new("radixdb-prerelease-b3-").expect("create B3 fixture");
    let store = ArtifactStore::create(&fixture, "replay").expect("create replay store");
    store
        .write_manifest(&manifest())
        .expect("write replay manifest");
    let trace = completed_trace();
    store.write_trace(&trace).expect("write completed trace");
    replay_artifacts(&store).expect("standalone replay succeeds for original trace");

    let mut corrupted = trace;
    corrupt_expected_select(&mut corrupted);
    store
        .write_trace(&corrupted)
        .expect("write deliberately corrupted expectation");
    let error = replay_artifacts(&store).expect_err("replay must reproduce expected-row drift");
    assert!(
        error.contains("expected") && error.contains("observed"),
        "unexpected replay error: {error}"
    );
}

#[test]
fn b3_runner_replays_supplied_trace() {
    let Some(trace_path) = std::env::var_os(RUNNER_REPLAY_TRACE).map(std::path::PathBuf::from)
    else {
        return;
    };
    let manifest_path = trace_path
        .parent()
        .expect("replay trace must have a parent directory")
        .join("manifest.json");
    let manifest: RunManifest = serde_json::from_slice(
        &fs::read(&manifest_path).expect("read replay manifest beside supplied trace"),
    )
    .expect("decode replay manifest");
    let trace: OperationTrace =
        serde_json::from_slice(&fs::read(&trace_path).expect("read supplied replay trace"))
            .expect("decode supplied replay trace");
    let model = replay_trace(&manifest, &trace).expect("supplied trace replay must succeed");
    model.validate().expect("validate replayed model");
    println!(
        "replayed {} events from {} at committed epoch {}",
        trace.events.len(),
        trace_path.display(),
        model.committed_epoch()
    );
}

#[test]
fn b3_shrinker_reduces_failure_and_preserves_original_and_minimized_traces() {
    let mut failing = completed_trace();
    corrupt_expected_select(&mut failing);
    assert!(trace_has_outcome_mismatch(&failing));
    let shrunk = shrink_failing_trace(&failing, trace_has_outcome_mismatch)
        .expect("shrink synthetic B3 failure");
    assert!(shrunk.minimized.events.len() < failing.events.len());
    assert!(trace_has_outcome_mismatch(&shrunk.minimized));
    assert_eq!(
        shrunk
            .stages
            .iter()
            .map(|stage| stage.name.as_str())
            .collect::<Vec<_>>(),
        ["actors", "transactions", "statements", "data_cardinality"]
    );

    let fixture =
        OwnedFixture::new("radixdb-prerelease-b3-shrink-").expect("create shrink artifact fixture");
    let store = ArtifactStore::create(&fixture, "artifacts").expect("create shrink store");
    let artifacts = store
        .write_trace_pair_once(&failing, &shrunk.minimized)
        .expect("preserve both traces");
    assert!(artifacts.original.exists());
    assert!(artifacts.minimized.exists());
    assert!(store
        .write_trace_pair_once(&failing, &shrunk.minimized)
        .is_err());
    let restored_original: OperationTrace = store.read_json("trace-original.json").unwrap();
    let restored_minimized: OperationTrace = store.read_json("trace-minimized.json").unwrap();
    assert_eq!(restored_original, failing);
    assert_eq!(restored_minimized, shrunk.minimized);
}

#[test]
fn b3_unexpected_success_does_not_transition_the_authoritative_model() {
    let plan = StateMachinePlan::generate(RUN_ID, SEED).expect("generate B3 plan");
    let begin = &plan.trace.events[0];
    let mut unexpected = plan.trace.events[1].clone();
    unexpected.expected_outcome = TraceOutcome::DeclaredError {
        class: "invalid_transaction".to_string(),
    };

    let mut model = ReplayModel::default();
    model
        .apply_verified_event(begin)
        .expect("begin transaction");
    let before = model.clone();
    let error = model
        .apply_verified_event(&unexpected)
        .expect_err("unexpected successful INSERT must fail the harness");
    assert!(error.contains("expected") && error.contains("observed"));
    assert_eq!(model, before, "failed verification mutated the model");
}
