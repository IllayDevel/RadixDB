#![allow(dead_code, unused_imports)]

pub mod actors;
pub mod artifacts;
pub mod concurrency;
pub mod concurrency_runtime;
pub mod config;
#[cfg(all(feature = "stress-tests", feature = "test-failpoints"))]
pub mod epoch_chaos;
pub mod fault;
pub mod historical;
pub mod large_fixture;
pub mod metrics;
pub mod model;
pub mod oracle;
#[cfg(all(feature = "stress-tests", feature = "test-failpoints"))]
pub mod race;
pub mod report;
pub mod schema;
pub mod state_machine;
pub mod tcp;
pub mod trace;
pub mod watchdog;

pub use actors::{ActorPlan, OperationKind, PlannedOperation};
pub use artifacts::{
    sha256_file, ArtifactStore, ConnectionSnapshot, OwnedChildProcess, OwnedFixture,
    ProcessSnapshot, ReportArtifacts, ResourceSnapshot, ThreadSnapshot, TimeoutSnapshot,
    TracePairArtifacts,
};
pub use concurrency::{
    ConcurrencyOperation, ConcurrencyOperationKind, ConcurrencyPlan, ConcurrencyTable,
    ConcurrencyTransaction, KeyDistribution, TransactionCohort, TransactionTerminal,
};
pub use concurrency_runtime::{
    initialize_concurrency_actor_rows, initialize_concurrency_fixture, run_capacity_probe,
    run_concurrency_step, run_concurrency_step_in_keyspace,
    run_concurrency_step_in_keyspace_with_query_timeout, run_overload_wave,
    run_overload_wave_for_fixture, verify_concurrency_state, verify_concurrency_state_in_keyspace,
    ConcurrencyKeyspace, ConcurrencyRuntimeSummary, CONCURRENCY_DATABASE,
    LARGE_CONCURRENCY_QUERY_TIMEOUT,
};
pub use config::{CandidateIdentity, HostProfile, RunConfig, RunManifest, RunProfile};
pub use fault::{
    b6_corruption_corpus, b6_fault_schedule, CorruptionCase, CorruptionMutation, DurableArtifact,
    FaultActivation, FaultPoint, FaultSchedule, RecoveryClass, ResourceLimits,
};
pub use historical::{
    historical_terminal_decision, historical_workload_signature, run_historical_messenger_chaos,
    HistoricalChaosSummary, HistoricalTerminalDecision, HistoricalWorkloadSignature,
};
#[cfg(feature = "test-mutations")]
pub use historical::{
    prove_historical_mixed_epoch_oracle_rejects_disabled_fence,
    run_historical_mixed_epoch_invariant,
};
pub use large_fixture::{
    materialize_large_concurrency_fixture, LargeFixtureEvidence, LargeFixtureTableEvidence,
};
pub use metrics::{CounterSet, LatencySummary, ResourceSlope, TimedResourceSample};
pub use model::{
    ImmutableMessengerBaseline, MessengerMessageModel, MessengerStateModel, ModelLedger,
    ModelTransaction, ModelTransactionState, SparseModelDelta,
};
pub use oracle::{OracleCheck, OracleReport};
#[cfg(all(feature = "stress-tests", feature = "test-failpoints"))]
pub use race::{DeterministicScheduler, RawProtocolClient};
pub use report::{RunReport, RunStatus, StageResult, StageStatus};
pub use schema::{
    messenger_schema_plan, messenger_small_seed_plan, messenger_view_plan, MessengerFixtureScale,
    MessengerSeedPlan, MessengerSeedRecord, MessengerSeedRows, MessengerSeedTable,
    MessengerStorageTier, SchemaPlan, SchemaStatement,
};
pub use state_machine::{
    execute_state_machine_plan, replay_artifacts, replay_trace, shrink_failing_trace,
    trace_has_outcome_mismatch, ReplayModel, ShrinkResult, ShrinkStage, StateMachinePlan,
    StateMachineRun, StateOperation,
};
pub use tcp::{
    tcp_command, tcp_connect, tcp_connect_with_read_timeout, tcp_rows, tcp_scalar_i64,
    tcp_server_config, with_tcp_server,
};
pub use trace::{DeterministicStream, OperationTrace, TraceEvent, TraceOutcome, TraceValue};
pub use watchdog::{Watchdog, WatchdogOutcome};
