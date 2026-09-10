use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use radixdb::storage::v6::{
    discover_staging_publications, StagingCompletion, StagingDiscoveryLimits, StagingDisposition,
};
use radixdb::test_failpoints::InterleavePoint;
use radixdb_client::WireValue;
use serde::{Deserialize, Serialize};

use crate::common::prerelease::{
    run_historical_messenger_chaos, sha256_file, tcp_command, tcp_connect_with_read_timeout,
    tcp_rows, tcp_scalar_i64, HistoricalChaosSummary,
};

use super::{
    config::{ChaosConfig, ProfileKind, DATABASE},
    fixture::{self, FixtureEvidence},
    high_concurrency_random,
    journal::{self, JournalSummary},
    large_transactions, micro_transactions,
    model::{LayerContext, LayerSummary, MaintenanceActor},
    oracle::{self, OracleSnapshot},
    server::ServerSupervisor,
    stage_bundle::{self, BundleStage, PublishInput, RestoredPrefix, StageSourceEvidence},
    telemetry::{merge_reports, storage_snapshot, StorageSnapshot, Telemetry, TelemetryReport},
    wide_transaction,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct CandidateIdentity {
    pub commit: String,
    pub dirty: bool,
    pub cargo_lock_sha256: String,
    pub server_binary_sha256: String,
    pub server_binary: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct ProfileEvidence {
    pub kind: String,
    pub acceptance: bool,
    pub wide_actions: u64,
    pub large_workers: usize,
    pub large_actions_per_worker: u64,
    pub micro_transactions: u64,
    pub random_rungs: Vec<usize>,
    pub random_operations_per_rung: u64,
    pub cold_rows: u64,
    pub hot_rows: u64,
    pub stage_timeout_millis: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct CrashEvidence {
    pub layer: String,
    pub point: String,
    pub expected: String,
    pub observed: String,
}

#[derive(Clone, Debug, Serialize)]
struct StagingEvidence {
    writer_directories: u64,
    publications: u64,
    complete: u64,
    incomplete: u64,
    invalid: u64,
    active: u64,
    aged_orphan_candidates: u64,
    quarantine_required: u64,
    bytes: u64,
}

impl StagingEvidence {
    fn validate(&self) -> Result<(), String> {
        if self.publications != self.complete + self.incomplete + self.invalid {
            return Err("staging completion accounting does not balance".to_string());
        }
        if self.publications != self.active + self.aged_orphan_candidates + self.quarantine_required
        {
            return Err("staging disposition accounting does not balance".to_string());
        }
        if self.invalid != 0 || self.incomplete != 0 || self.quarantine_required != 0 {
            return Err(format!("invalid final staging state: {self:?}"));
        }
        // ManifestBeforePublish deliberately leaves at most one freshly owned,
        // complete and unreachable publication. The format contract forbids
        // deleting it until staging_orphan_min_age has elapsed; treating its
        // presence as corruption would contradict conservative recovery.
        if self.publications > 1
            || self.complete != self.publications
            || self.active != self.publications
            || self.aged_orphan_candidates != 0
        {
            return Err(format!("unexpected final staging residue: {self:?}"));
        }
        if self.writer_directories > 4_096 || self.bytes > 64 * 1024 * 1024 {
            return Err(format!("final staging residue is not bounded: {self:?}"));
        }
        Ok(())
    }

    fn validate_owner_finalization(&self, source: &Self) -> Result<(), String> {
        let source_classified = source.active + source.aged_orphan_candidates;
        if source.publications != 1
            || source.complete != 0
            || source.incomplete != 1
            || source.invalid != 0
            || source.quarantine_required != 0
            || source_classified != 1
        {
            return Err(format!(
                "owner-accepted source has unexpected staging evidence: {source:?}"
            ));
        }

        let classified = self.active + self.aged_orphan_candidates;
        if self.publications != source.publications + 1
            || self.complete != source.complete + 1
            || self.incomplete != source.incomplete
            || self.invalid != 0
            || self.quarantine_required != 0
            || classified != self.publications
        {
            return Err(format!(
                "final staging differs from the accepted source plus one manifest crash residue: source={source:?} final={self:?}"
            ));
        }
        if self.writer_directories > 4_096
            || self.writer_directories > source.writer_directories.saturating_add(16)
            || self.bytes > source.bytes.saturating_add(64 * 1024 * 1024)
        {
            return Err(format!(
                "owner-finalization staging residue is not bounded: source={source:?} final={self:?}"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct EpochChaosReport {
    run_id: String,
    status: String,
    seed: u64,
    elapsed_millis: u64,
    identity: CandidateIdentity,
    profile: ProfileEvidence,
    historical: HistoricalEvidence,
    fixture: FixtureEvidence,
    layers: Vec<LayerSummary>,
    reopen_oracles: Vec<OracleSnapshot>,
    crashes: Vec<CrashEvidence>,
    final_oracle: OracleSnapshot,
    telemetry: TelemetryReport,
    final_storage: StorageSnapshot,
    final_staging: StagingEvidence,
    resumed_from: Option<String>,
    stage_sources: Vec<StageSourceEvidence>,
}

#[derive(Debug, Serialize)]
struct CapacityReport {
    run_id: String,
    status: String,
    seed: u64,
    elapsed_millis: u64,
    requested_clients: usize,
    requested_cpu_set: String,
    effective_server_cpu_set: String,
    identity: CandidateIdentity,
    profile: ProfileEvidence,
    source_bundle: String,
    source_run_id: String,
    source_elapsed_millis: u64,
    source_stage: BundleStage,
    source_oracle: OracleSnapshot,
    layer: LayerSummary,
    final_oracle: OracleSnapshot,
    telemetry: TelemetryReport,
    source_telemetry: TelemetryReport,
    final_storage: StorageSnapshot,
    final_staging: StagingEvidence,
}

#[derive(Debug, Serialize)]
struct ExistingRunFinalizationReport {
    run_id: String,
    status: String,
    source_root: String,
    source_commit: String,
    remediation_commit: Option<String>,
    remediation_paths: Vec<String>,
    finalizer_identity: CandidateIdentity,
    profile: ProfileEvidence,
    elapsed_millis: u64,
    completed_layers: Vec<LayerSummary>,
    random_32: JournalSummary,
    random_64: JournalSummary,
    random_128_unsealed_shards: usize,
    initial_reopen_oracle: OracleSnapshot,
    checkpoint_reopen_oracle: OracleSnapshot,
    crash: CrashEvidence,
    post_crash_oracle: OracleSnapshot,
    final_reopen_oracle: OracleSnapshot,
    telemetry: TelemetryReport,
    source_staging: StagingEvidence,
    final_storage: StorageSnapshot,
    final_staging: StagingEvidence,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct HistoricalEvidence {
    pub committed: usize,
    pub rolled_back: usize,
    pub disconnected: usize,
    pub conflicts: usize,
    pub checkpoint_busy: usize,
}

pub fn run() -> Result<(), String> {
    let started = Instant::now();
    let config = ChaosConfig::from_env()?;
    if config.finalize_source.is_some() {
        finalize_existing_run(config, started)
    } else if config.capacity_rung.is_some() {
        run_capacity(config, started)
    } else {
        run_gate(config, started)
    }
}

fn run_gate(config: ChaosConfig, started: Instant) -> Result<(), String> {
    let run_root = prepare_run_root(&config)?;
    let identity = candidate_identity()?;
    if config.profile.is_acceptance() && identity.dirty {
        return Err(
            "full epoch-chaos acceptance requires a clean exact git revision; commit the harness first"
                .to_string(),
        );
    }
    if config.resume_bundle.is_some() && !config.profile.is_acceptance() {
        return Err("stage-bundle resume is available only for the full profile".to_string());
    }
    let profile = profile_evidence(&config);
    write_json(&run_root.join("config.json"), &profile)?;
    eprintln!(
        "epoch-chaos run={} profile={:?} root={}",
        config.run_id,
        config.profile.kind,
        run_root.display()
    );
    if !config.profile.is_acceptance() {
        eprintln!("epoch-chaos smoke is diagnostic only and cannot satisfy CA-80.8");
    }

    let restored = config
        .resume_bundle
        .as_ref()
        .map(|source| stage_bundle::restore(source, &run_root, &identity, &profile, config.seed))
        .transpose()?;
    let data_root = run_root.join("server-data");
    let telemetry = Telemetry::start(data_root);
    let progress = telemetry.progress_counter();
    let journal_root = run_root.join("journals");
    let mut supervisor = ServerSupervisor::new(run_root.clone(), &telemetry)?;
    supervisor.start(None)?;

    let (
        historical,
        fixture,
        mut reopen_oracles,
        mut crashes,
        mut layers,
        next_stage,
        elapsed_prefix,
        prefix_telemetry,
        resumed_from,
        mut stage_sources,
    ) = match restored {
        Some(RestoredPrefix {
            source_bundle,
            source_run_id,
            elapsed_millis,
            stage,
            historical,
            fixture,
            layers,
            mut reopen_oracles,
            crashes,
            post_stage_oracle,
            telemetry,
            stage_sources,
        }) => {
            for layer in &layers {
                layer.validate()?;
            }
            for snapshot in &reopen_oracles {
                snapshot.validate()?;
            }
            let observed = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
            if observed != post_stage_oracle {
                return Err(format!(
                    "resumed stage bundle changed cross-table state: expected={post_stage_oracle:?} observed={observed:?}"
                ));
            }
            reopen_oracles.push(observed);
            eprintln!(
                "epoch-chaos resume source={} run={} completed_stage={}",
                source_bundle.display(),
                source_run_id,
                stage.name()
            );
            (
                historical,
                fixture,
                reopen_oracles,
                crashes,
                layers,
                stage.index() + 1,
                elapsed_millis,
                Some(telemetry),
                Some(source_bundle.display().to_string()),
                stage_sources,
            )
        }
        None => {
            let historical = historical_evidence(run_historical_messenger_chaos());
            let fixture = fixture::initialize(&mut supervisor, &run_root, &config.profile)?;
            let initial_oracle =
                oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
            (
                historical,
                fixture,
                vec![initial_oracle],
                Vec::new(),
                Vec::new(),
                0,
                0,
                None,
                None,
                Vec::new(),
            )
        }
    };

    let mut context = layer_context(supervisor.address()?, &config, &journal_root, &progress);
    if next_stage <= BundleStage::WideTransaction.index() {
        let wide = run_layer(
            "wide-transaction",
            &run_root,
            &context,
            &telemetry,
            Some((context.profile.wide_actions / 16).max(100)),
            wide_transaction::run,
        )?;
        layers.push(wide);
        reopen_oracles.push(checkpoint_reopen(&mut supervisor, &config)?);
        crashes.push(crash_commit_durable(
            &mut supervisor,
            &config,
            &run_root,
            "wide-transaction",
            1,
        )?);
        reopen_oracles.push(oracle::capture(
            supervisor.address()?,
            config.profile.stage_timeout,
        )?);
        publish_stage_bundle(
            &mut supervisor,
            &config,
            &run_root,
            &identity,
            &profile,
            &historical,
            &fixture,
            &layers,
            &mut reopen_oracles,
            &crashes,
            &mut stage_sources,
            prefix_telemetry.as_ref(),
            &telemetry,
            BundleStage::WideTransaction,
            elapsed_prefix.saturating_add(started.elapsed().as_millis() as u64),
        )?;
    }

    if next_stage <= BundleStage::LargeTransactions.index() {
        context.address = supervisor.address()?;
        let large = run_layer(
            "large-transactions",
            &run_root,
            &context,
            &telemetry,
            Some(
                (context.profile.large_workers as u64 * context.profile.large_actions_per_worker
                    / 32)
                    .max(100),
            ),
            large_transactions::run,
        )?;
        layers.push(large);
        reopen_oracles.push(checkpoint_reopen(&mut supervisor, &config)?);
        crashes.push(crash_checkpoint(
            &mut supervisor,
            &config,
            &run_root,
            "large-transactions",
            2,
            InterleavePoint::CheckpointBeforePublish,
        )?);
        reopen_oracles.push(oracle::capture(
            supervisor.address()?,
            config.profile.stage_timeout,
        )?);
        publish_stage_bundle(
            &mut supervisor,
            &config,
            &run_root,
            &identity,
            &profile,
            &historical,
            &fixture,
            &layers,
            &mut reopen_oracles,
            &crashes,
            &mut stage_sources,
            prefix_telemetry.as_ref(),
            &telemetry,
            BundleStage::LargeTransactions,
            elapsed_prefix.saturating_add(started.elapsed().as_millis() as u64),
        )?;
    }

    if next_stage <= BundleStage::MicroTransactions.index() {
        context.address = supervisor.address()?;
        let micro = run_layer(
            "micro-transactions",
            &run_root,
            &context,
            &telemetry,
            None,
            micro_transactions::run,
        )?;
        layers.push(micro);
        reopen_oracles.push(checkpoint_reopen(&mut supervisor, &config)?);
        crashes.push(crash_before_commit(
            &mut supervisor,
            &config,
            &run_root,
            "micro-transactions",
            3,
        )?);
        reopen_oracles.push(oracle::capture(
            supervisor.address()?,
            config.profile.stage_timeout,
        )?);
        publish_stage_bundle(
            &mut supervisor,
            &config,
            &run_root,
            &identity,
            &profile,
            &historical,
            &fixture,
            &layers,
            &mut reopen_oracles,
            &crashes,
            &mut stage_sources,
            prefix_telemetry.as_ref(),
            &telemetry,
            BundleStage::MicroTransactions,
            elapsed_prefix.saturating_add(started.elapsed().as_millis() as u64),
        )?;
    }

    if next_stage <= BundleStage::HighConcurrencyRandom.index() {
        if config.profile.is_acceptance() {
            context.profile.random_rungs = config.profile.acceptance_random_rungs().to_vec();
        }
        context.address = supervisor.address()?;
        let random = run_layer(
            "high-concurrency-random",
            &run_root,
            &context,
            &telemetry,
            None,
            high_concurrency_random::run,
        )?;
        layers.push(random);
        reopen_oracles.push(checkpoint_reopen(&mut supervisor, &config)?);
        crashes.push(crash_checkpoint(
            &mut supervisor,
            &config,
            &run_root,
            "high-concurrency-random",
            4,
            InterleavePoint::ManifestBeforePublish,
        )?);
        reopen_oracles.push(oracle::capture(
            supervisor.address()?,
            config.profile.stage_timeout,
        )?);
        publish_stage_bundle(
            &mut supervisor,
            &config,
            &run_root,
            &identity,
            &profile,
            &historical,
            &fixture,
            &layers,
            &mut reopen_oracles,
            &crashes,
            &mut stage_sources,
            prefix_telemetry.as_ref(),
            &telemetry,
            BundleStage::HighConcurrencyRandom,
            elapsed_prefix.saturating_add(started.elapsed().as_millis() as u64),
        )?;
    }

    let final_oracle = checkpoint_reopen(&mut supervisor, &config)?;
    supervisor.stop_graceful(Duration::from_secs(120))?;
    drop(supervisor);
    let telemetry = merge_reports(prefix_telemetry.as_ref(), telemetry.stop());
    let final_storage = storage_snapshot(&run_root.join("server-data"))?;
    let final_staging = staging_evidence(&run_root.join("server-data"), &final_storage)?;
    final_staging.validate()?;
    for layer in &layers {
        layer.validate()?;
    }
    for snapshot in &reopen_oracles {
        snapshot.validate()?;
    }
    final_oracle.validate()?;
    let report = EpochChaosReport {
        run_id: config.run_id.clone(),
        status: if config.profile.is_acceptance() {
            "accepted".to_string()
        } else {
            "smoke_passed_non_acceptance".to_string()
        },
        seed: config.seed,
        elapsed_millis: elapsed_prefix.saturating_add(started.elapsed().as_millis() as u64),
        identity,
        profile,
        historical,
        fixture,
        layers,
        reopen_oracles,
        crashes,
        final_oracle,
        telemetry,
        final_storage,
        final_staging,
        resumed_from,
        stage_sources,
    };
    write_json(&run_root.join("report.json"), &report)?;
    write_markdown(&run_root.join("REPORT.md"), &report)?;
    sync_directory(&run_root)?;
    eprintln!(
        "epoch-chaos status={} elapsed_ms={} report={}",
        report.status,
        report.elapsed_millis,
        run_root.join("REPORT.md").display()
    );
    Ok(())
}

fn run_capacity(config: ChaosConfig, started: Instant) -> Result<(), String> {
    let clients = config
        .capacity_rung
        .ok_or_else(|| "capacity run has no selected rung".to_string())?;
    let rung_index = config
        .profile
        .capacity_rung_index(clients)
        .ok_or_else(|| format!("unsupported capacity rung {clients}"))?;
    let source = config
        .resume_bundle
        .as_ref()
        .ok_or_else(|| "capacity run has no post-64 stage bundle".to_string())?;
    let run_root = prepare_run_root(&config)?;
    let identity = candidate_identity()?;
    if identity.dirty {
        return Err("capacity run requires a clean exact git revision".to_string());
    }
    let profile = profile_evidence(&config);
    write_json(&run_root.join("config.json"), &profile)?;
    let restored = stage_bundle::restore(source, &run_root, &identity, &profile, config.seed)?;
    if restored.stage != BundleStage::HighConcurrencyRandom {
        return Err(format!(
            "capacity run requires the post-64 high-concurrency bundle, got {}",
            restored.stage.name()
        ));
    }

    let requested_cpu_set = cpu_allowed_list(None)?;
    let data_root = run_root.join("server-data");
    let telemetry = Telemetry::start(data_root);
    let progress = telemetry.progress_counter();
    let journal_root = run_root.join("journals");
    let mut supervisor = ServerSupervisor::new(run_root.clone(), &telemetry)?;
    supervisor.start(None)?;
    let effective_server_cpu_set = cpu_allowed_list(Some(supervisor.pid()?))?;
    if effective_server_cpu_set != requested_cpu_set {
        return Err(format!(
            "capacity server CPU set {effective_server_cpu_set} differs from invoking process {requested_cpu_set}"
        ));
    }
    let observed_source = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    if observed_source != restored.post_stage_oracle {
        return Err(format!(
            "capacity source changed before workload: expected={:?} observed={observed_source:?}",
            restored.post_stage_oracle
        ));
    }
    eprintln!(
        "epoch-chaos capacity clients={clients} cpus={effective_server_cpu_set} source={} state=started",
        source.display()
    );

    let context = layer_context(supervisor.address()?, &config, &journal_root, &progress);
    let layer_name = format!("high-concurrency-random-capacity-{clients}");
    let layer = run_layer(
        &layer_name,
        &run_root,
        &context,
        &telemetry,
        None,
        |context| high_concurrency_random::run_capacity(context, rung_index, clients),
    )?;
    let final_oracle = checkpoint_reopen(&mut supervisor, &config)?;
    final_oracle.validate()?;
    supervisor.stop_graceful(Duration::from_secs(120))?;
    drop(supervisor);
    let telemetry = telemetry.stop();
    let final_storage = storage_snapshot(&run_root.join("server-data"))?;
    let final_staging = staging_evidence(&run_root.join("server-data"), &final_storage)?;
    final_staging.validate()?;
    let report = CapacityReport {
        run_id: config.run_id.clone(),
        status: "capacity_passed".to_string(),
        seed: config.seed,
        elapsed_millis: started.elapsed().as_millis() as u64,
        requested_clients: clients,
        requested_cpu_set,
        effective_server_cpu_set,
        identity,
        profile,
        source_bundle: restored.source_bundle.display().to_string(),
        source_run_id: restored.source_run_id,
        source_elapsed_millis: restored.elapsed_millis,
        source_stage: restored.stage,
        source_oracle: restored.post_stage_oracle,
        layer,
        final_oracle,
        telemetry,
        source_telemetry: restored.telemetry,
        final_storage,
        final_staging,
    };
    write_json(&run_root.join("capacity-report.json"), &report)?;
    write_capacity_markdown(&run_root.join("CAPACITY_REPORT.md"), &report)?;
    sync_directory(&run_root)?;
    eprintln!(
        "epoch-chaos capacity clients={} status={} elapsed_ms={} report={}",
        report.requested_clients,
        report.status,
        report.elapsed_millis,
        run_root.join("CAPACITY_REPORT.md").display()
    );
    Ok(())
}

fn finalize_existing_run(config: ChaosConfig, started: Instant) -> Result<(), String> {
    let source = config
        .finalize_source
        .as_ref()
        .ok_or_else(|| "existing-run finalization has no source".to_string())?
        .canonicalize()
        .map_err(|error| format!("canonicalize finalization source: {error}"))?;
    let source_commit = config
        .finalize_source_commit
        .as_deref()
        .ok_or_else(|| "existing-run finalization has no source commit".to_string())?;
    let source_validation =
        validate_finalization_source(source_commit, config.finalize_remediation_commit.as_deref())?;
    let identity = candidate_identity()?;
    if identity.dirty {
        return Err("existing-run finalization requires a clean exact git revision".to_string());
    }
    let profile = profile_evidence(&config);
    validate_existing_profile(&source.join("config.json"), &profile)?;
    let completed_layers = read_completed_layers(&source)?;
    let random_32 = inspect_random_rung(&source.join("journals"), 0, 32, 400_000)?;
    let random_64 = inspect_random_rung(&source.join("journals"), 1, 64, 400_000)?;
    let random_128_unsealed_shards =
        inspect_unsealed_random_rung(&source.join("journals"), 2, 128)?;
    let source_storage = storage_snapshot(&source.join("server-data"))?;
    let source_staging = staging_evidence(&source.join("server-data"), &source_storage)?;

    let run_root = prepare_run_root(&config)?;
    if run_root.starts_with(&source) {
        return Err("finalization destination must not be inside its source".to_string());
    }
    write_json(&run_root.join("config.json"), &profile)?;
    stage_bundle::copy_durable_tree(&source.join("server-data"), &run_root.join("server-data"))?;

    let telemetry = Telemetry::start(run_root.join("server-data"));
    let mut supervisor = ServerSupervisor::new(run_root.clone(), &telemetry)?;
    supervisor.start(None)?;
    let initial_reopen_oracle =
        oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    let checkpoint_reopen_oracle = checkpoint_reopen(&mut supervisor, &config)?;
    if checkpoint_reopen_oracle != initial_reopen_oracle {
        return Err("existing run changed during initial checkpoint/reopen".to_string());
    }
    let crash = crash_checkpoint(
        &mut supervisor,
        &config,
        &run_root,
        "high-concurrency-random-owner-accepted",
        4,
        InterleavePoint::ManifestBeforePublish,
    )?;
    let post_crash_oracle = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    let final_reopen_oracle = checkpoint_reopen(&mut supervisor, &config)?;
    if final_reopen_oracle != post_crash_oracle {
        return Err("existing run changed during final checkpoint/reopen".to_string());
    }
    supervisor.stop_graceful(Duration::from_secs(120))?;
    drop(supervisor);
    let telemetry = telemetry.stop();
    let final_storage = storage_snapshot(&run_root.join("server-data"))?;
    let final_staging = staging_evidence(&run_root.join("server-data"), &final_storage)?;
    final_staging.validate_owner_finalization(&source_staging)?;
    let report = ExistingRunFinalizationReport {
        run_id: config.run_id.clone(),
        status: "owner_accepted_random_finalized".to_string(),
        source_root: source.display().to_string(),
        source_commit: source_commit.to_string(),
        remediation_commit: source_validation.remediation_commit,
        remediation_paths: source_validation.remediation_paths,
        finalizer_identity: identity,
        profile,
        elapsed_millis: started.elapsed().as_millis() as u64,
        completed_layers,
        random_32,
        random_64,
        random_128_unsealed_shards,
        initial_reopen_oracle,
        checkpoint_reopen_oracle,
        crash,
        post_crash_oracle,
        final_reopen_oracle,
        telemetry,
        source_staging,
        final_storage,
        final_staging,
    };
    write_json(&run_root.join("finalization-report.json"), &report)?;
    write_finalization_markdown(&run_root.join("FINALIZATION_REPORT.md"), &report)?;
    sync_directory(&run_root)?;
    eprintln!(
        "epoch-chaos existing-run status={} elapsed_ms={} report={}",
        report.status,
        report.elapsed_millis,
        run_root.join("FINALIZATION_REPORT.md").display()
    );
    Ok(())
}

#[derive(Debug)]
struct FinalizationSourceValidation {
    remediation_commit: Option<String>,
    remediation_paths: Vec<String>,
}

const DEEP_VERSION_HISTORY_REMEDIATION_PATHS: [&str; 2] = [
    "crates/radixdb-storage/src/mvcc/version_store/mod.rs",
    "crates/radixdb-storage/src/mvcc/version_store/tests.rs",
];

fn validate_finalization_source(
    source_commit: &str,
    remediation_commit: Option<&str>,
) -> Result<FinalizationSourceValidation, String> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    ensure_commit_exists(&manifest, source_commit, "existing-run source")?;
    let Some(remediation_commit) = remediation_commit else {
        ensure_source_range_unchanged(&manifest, source_commit, "HEAD")?;
        return Ok(FinalizationSourceValidation {
            remediation_commit: None,
            remediation_paths: Vec::new(),
        });
    };

    ensure_commit_exists(&manifest, remediation_commit, "finalization remediation")?;
    let ancestor = Command::new("git")
        .args([
            "merge-base",
            "--is-ancestor",
            source_commit,
            remediation_commit,
        ])
        .current_dir(&manifest)
        .status()
        .map_err(|error| error.to_string())?;
    if !ancestor.success() {
        return Err(format!(
            "remediation commit {remediation_commit} does not descend from source commit {source_commit}"
        ));
    }

    let paths = server_source_diff_paths(&manifest, source_commit, remediation_commit)?;
    validate_deep_version_history_remediation_paths(&paths)?;
    ensure_source_range_unchanged(&manifest, remediation_commit, "HEAD")?;
    Ok(FinalizationSourceValidation {
        remediation_commit: Some(remediation_commit.to_string()),
        remediation_paths: paths.into_iter().collect(),
    })
}

fn ensure_commit_exists(manifest: &Path, commit: &str, label: &str) -> Result<(), String> {
    let object = format!("{commit}^{{commit}}");
    let status = Command::new("git")
        .args(["cat-file", "-e", &object])
        .current_dir(manifest)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("unknown {label} commit {commit}"))
    }
}

fn ensure_source_range_unchanged(manifest: &Path, from: &str, to: &str) -> Result<(), String> {
    let status = Command::new("git")
        .arg("diff")
        .arg("--quiet")
        .arg(format!("{from}..{to}"))
        .arg("--")
        .args(["Cargo.toml", "Cargo.lock", "build.rs", "src", "crates"])
        .current_dir(manifest)
        .status()
        .map_err(|error| error.to_string())?;
    if !status.success() {
        return Err(format!(
            "server source or lockfile changed between {from} and {to}"
        ));
    }
    Ok(())
}

fn server_source_diff_paths(
    manifest: &Path,
    from: &str,
    to: &str,
) -> Result<BTreeSet<String>, String> {
    let range = format!("{from}..{to}");
    let output = Command::new("git")
        .args(["diff", "--name-only", &range, "--"])
        .args(["Cargo.toml", "Cargo.lock", "build.rs", "src", "crates"])
        .current_dir(manifest)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!("cannot inspect server source range {range}"));
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| format!("server source range {range} returned non-UTF-8 paths"))?;
    Ok(stdout.lines().map(str::to_string).collect())
}

fn validate_deep_version_history_remediation_paths(paths: &BTreeSet<String>) -> Result<(), String> {
    let allowed: BTreeSet<String> = DEEP_VERSION_HISTORY_REMEDIATION_PATHS
        .into_iter()
        .map(str::to_string)
        .collect();
    if paths != &allowed {
        return Err(format!(
            "historical finalization remediation has unexpected server paths: expected={allowed:?} observed={paths:?}"
        ));
    }
    Ok(())
}

fn validate_existing_profile(path: &Path, expected: &ProfileEvidence) -> Result<(), String> {
    let value: serde_json::Value = serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", path.display()))?;
    let checks = [
        ("acceptance", serde_json::json!(expected.acceptance)),
        ("wide_actions", serde_json::json!(expected.wide_actions)),
        ("large_workers", serde_json::json!(expected.large_workers)),
        (
            "large_actions_per_worker",
            serde_json::json!(expected.large_actions_per_worker),
        ),
        (
            "micro_transactions",
            serde_json::json!(expected.micro_transactions),
        ),
        ("random_rungs", serde_json::json!(expected.random_rungs)),
        (
            "random_operations_per_rung",
            serde_json::json!(expected.random_operations_per_rung),
        ),
        ("cold_rows", serde_json::json!(expected.cold_rows)),
        ("hot_rows", serde_json::json!(expected.hot_rows)),
    ];
    for (name, expected_value) in checks {
        if value.get(name) != Some(&expected_value) {
            return Err(format!(
                "existing-run profile field {name} differs: expected={expected_value} observed={:?}",
                value.get(name)
            ));
        }
    }
    Ok(())
}

fn read_completed_layers(source: &Path) -> Result<Vec<LayerSummary>, String> {
    let names = [
        "wide-transaction",
        "large-transactions",
        "micro-transactions",
    ];
    names
        .into_iter()
        .map(|name| {
            let path = source.join(format!("layer-{name}.json"));
            let layer: LayerSummary = serde_json::from_slice(
                &fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?,
            )
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
            layer.validate()?;
            if layer.name != name {
                return Err(format!(
                    "existing-run layer {} is named {}",
                    path.display(),
                    layer.name
                ));
            }
            Ok(layer)
        })
        .collect()
}

fn inspect_random_rung(
    journal_root: &Path,
    rung_index: usize,
    clients: usize,
    expected_operations: u64,
) -> Result<JournalSummary, String> {
    let summaries = (0..clients)
        .map(|actor| {
            journal::inspect(
                &journal_root.join(format!("random-{:04}.journal", rung_index * 1024 + actor)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let summary = super::model::merge_journals(summaries)?;
    if summary.terminal.count != expected_operations {
        return Err(format!(
            "random {clients}-client rung has {} terminal operations instead of {expected_operations}",
            summary.terminal.count
        ));
    }
    Ok(summary)
}

fn inspect_unsealed_random_rung(
    journal_root: &Path,
    rung_index: usize,
    clients: usize,
) -> Result<usize, String> {
    let mut found = 0;
    for actor in 0..clients {
        let path = journal_root.join(format!("random-{:04}.journal", rung_index * 1024 + actor));
        let metadata = fs::metadata(&path)
            .map_err(|error| format!("inspect owner-accepted shard {}: {error}", path.display()))?;
        if !metadata.is_file() || metadata.len() != 8 {
            return Err(format!(
                "owner-accepted random shard is not an unsealed 8-byte header: {}",
                path.display()
            ));
        }
        found += 1;
    }
    Ok(found)
}

#[allow(clippy::too_many_arguments)]
fn publish_stage_bundle(
    supervisor: &mut ServerSupervisor<'_>,
    config: &ChaosConfig,
    run_root: &Path,
    identity: &CandidateIdentity,
    profile: &ProfileEvidence,
    historical: &HistoricalEvidence,
    fixture: &FixtureEvidence,
    layers: &[LayerSummary],
    reopen_oracles: &mut Vec<OracleSnapshot>,
    crashes: &[CrashEvidence],
    stage_sources: &mut Vec<StageSourceEvidence>,
    prefix_telemetry: Option<&TelemetryReport>,
    telemetry: &Telemetry,
    stage: BundleStage,
    elapsed_millis: u64,
) -> Result<(), String> {
    if !config.profile.is_acceptance() {
        return Ok(());
    }
    if stage_sources.len() != stage.index() {
        return Err(format!(
            "stage source chain has {} records before {}",
            stage_sources.len(),
            stage.name()
        ));
    }
    stage_sources.push(StageSourceEvidence {
        stage,
        source_run_id: config.run_id.clone(),
        source_commit: identity.commit.clone(),
        cargo_lock_sha256: identity.cargo_lock_sha256.clone(),
        server_binary_sha256: identity.server_binary_sha256.clone(),
    });
    let post_stage_oracle = reopen_oracles
        .last()
        .cloned()
        .ok_or_else(|| "stage bundle has no post-stage oracle".to_string())?;
    supervisor.stop_graceful(Duration::from_secs(120))?;
    let combined_telemetry = merge_reports(prefix_telemetry, telemetry.report());
    let bundle = stage_bundle::publish(PublishInput {
        run_root,
        run_id: &config.run_id,
        elapsed_millis,
        stage,
        identity,
        profile,
        seed: config.seed,
        historical,
        fixture,
        layers,
        reopen_oracles,
        crashes,
        post_stage_oracle: &post_stage_oracle,
        telemetry: &combined_telemetry,
        stage_sources,
    })?;
    supervisor.start(None)?;
    let observed = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    if observed != post_stage_oracle {
        return Err(format!(
            "stage bundle publication changed cross-table state: expected={post_stage_oracle:?} observed={observed:?}"
        ));
    }
    reopen_oracles.push(observed);
    eprintln!(
        "epoch-chaos stage={} bundle={} state=published",
        stage.name(),
        bundle.display()
    );
    Ok(())
}

fn run_layer<F>(
    name: &str,
    run_root: &Path,
    context: &LayerContext,
    telemetry: &Telemetry,
    external_maintenance_interval: Option<u64>,
    operation: F,
) -> Result<LayerSummary, String>
where
    F: FnOnce(&LayerContext) -> Result<LayerSummary, String>,
{
    let runtime_before = runtime_stats(context.address, context.profile.stage_timeout)?;
    telemetry.begin_layer()?;
    let watchdog = SemanticWatchdog::start(
        name,
        Arc::clone(&context.progress),
        context.profile.stage_timeout,
        if context.profile.is_acceptance() {
            Duration::from_secs(20 * 60)
        } else {
            Duration::from_secs(2 * 60)
        },
        run_root.join(format!("timeout-{name}.json")),
    )?;
    let maintenance = external_maintenance_interval
        .map(|interval| MaintenanceActor::start(context, interval))
        .transpose()?;
    eprintln!("epoch-chaos stage={name} state=started");
    let mut summary = operation(context)?;
    if let Some(maintenance) = maintenance {
        let maintenance = maintenance.stop()?;
        summary.merge_maintenance(maintenance);
    }
    let runtime_after = runtime_stats(context.address, context.profile.stage_timeout)?;
    summary.telemetry = telemetry.end_layer(summary.semantic_operations)?;
    summary.details.insert(
        "runtime_stats_before".to_string(),
        runtime_before.to_string(),
    );
    summary
        .details
        .insert("runtime_stats_after".to_string(), runtime_after.to_string());
    watchdog.stop()?;
    summary.validate()?;
    write_json(&run_root.join(format!("layer-{name}.json")), &summary)?;
    eprintln!(
        "epoch-chaos stage={name} state=passed operations={} transactions={} wall_ms={}",
        summary.semantic_operations, summary.transactions, summary.wall_millis
    );
    Ok(summary)
}

fn checkpoint_reopen(
    supervisor: &mut ServerSupervisor<'_>,
    config: &ChaosConfig,
) -> Result<OracleSnapshot, String> {
    let before = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    let mut connection = supervisor.connect(config.profile.stage_timeout)?;
    let deadline = Instant::now() + config.profile.stage_timeout;
    loop {
        match tcp_command(&mut connection, "PRAGMA CHECKPOINT") {
            Ok(()) => break,
            Err(error) if super::model::expected_busy(&error) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(format!("quiescent checkpoint: {error}")),
        }
    }
    drop(connection);
    supervisor.stop_graceful(Duration::from_secs(120))?;
    supervisor.start(None)?;
    let after = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    if before != after {
        return Err(format!(
            "clean reopen changed cross-table state: before={before:?} after={after:?}"
        ));
    }
    Ok(after)
}

fn crash_commit_durable(
    supervisor: &mut ServerSupervisor<'_>,
    config: &ChaosConfig,
    run_root: &Path,
    layer: &str,
    id: i64,
) -> Result<CrashEvidence, String> {
    let point = InterleavePoint::WalCommitMarkerDurable;
    record_crash_schedule(run_root, layer, point, "planned", "committed", "pending")?;
    restart_with_point(supervisor, point)?;
    let address = supervisor.address()?;
    let timeout = config.profile.stage_timeout;
    let layer_name = layer.to_string();
    let trigger = thread::spawn(move || {
        let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
        tcp_command(
            &mut connection,
            format!("INSERT INTO crash_ledger VALUES ({id}, '{layer_name}', 1)"),
        )
    });
    supervisor.wait_barrier_and_kill(Duration::from_secs(60))?;
    let _ = trigger.join();
    supervisor.start(None)?;
    let mut connection = supervisor.connect(config.profile.stage_timeout)?;
    let observed = tcp_scalar_i64(
        &mut connection,
        &format!("SELECT COUNT(*) FROM crash_ledger WHERE id = {id}"),
    )?;
    if observed != 1 {
        return Err(format!("durable commit crash oracle returned {observed}"));
    }
    record_crash_schedule(run_root, layer, point, "reopened", "committed", "committed")?;
    Ok(CrashEvidence {
        layer: layer.to_string(),
        point: InterleavePoint::WalCommitMarkerDurable
            .crash_name()
            .to_string(),
        expected: "committed".to_string(),
        observed: "committed".to_string(),
    })
}

fn crash_before_commit(
    supervisor: &mut ServerSupervisor<'_>,
    config: &ChaosConfig,
    run_root: &Path,
    layer: &str,
    id: i64,
) -> Result<CrashEvidence, String> {
    let point = InterleavePoint::WalBeforeCommitMarker;
    record_crash_schedule(run_root, layer, point, "planned", "absent", "pending")?;
    restart_with_point(supervisor, point)?;
    let address = supervisor.address()?;
    let timeout = config.profile.stage_timeout;
    let layer_name = layer.to_string();
    let trigger = thread::spawn(move || {
        let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
        tcp_command(
            &mut connection,
            format!("INSERT INTO crash_ledger VALUES ({id}, '{layer_name}', 1)"),
        )
    });
    supervisor.wait_barrier_and_kill(Duration::from_secs(60))?;
    let _ = trigger.join();
    supervisor.start(None)?;
    let mut connection = supervisor.connect(config.profile.stage_timeout)?;
    let observed = tcp_scalar_i64(
        &mut connection,
        &format!("SELECT COUNT(*) FROM crash_ledger WHERE id = {id}"),
    )?;
    if observed != 0 {
        return Err(format!("pre-marker crash published row {id}"));
    }
    record_crash_schedule(run_root, layer, point, "reopened", "absent", "absent")?;
    Ok(CrashEvidence {
        layer: layer.to_string(),
        point: InterleavePoint::WalBeforeCommitMarker
            .crash_name()
            .to_string(),
        expected: "absent".to_string(),
        observed: "absent".to_string(),
    })
}

fn crash_checkpoint(
    supervisor: &mut ServerSupervisor<'_>,
    config: &ChaosConfig,
    run_root: &Path,
    layer: &str,
    id: i64,
    point: InterleavePoint,
) -> Result<CrashEvidence, String> {
    record_crash_schedule(
        run_root,
        layer,
        point,
        "planned",
        "same_logical_state",
        "pending",
    )?;
    restart_with_point(supervisor, point)?;
    let address = supervisor.address()?;
    let timeout = config.profile.stage_timeout;
    let mut prepare = supervisor.connect(timeout)?;
    tcp_command(
        &mut prepare,
        format!("INSERT INTO crash_ledger VALUES ({id}, '{layer}', 1)"),
    )?;
    drop(prepare);
    let before = oracle::capture(address, timeout)?;
    let trigger = thread::spawn(move || {
        let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
        tcp_command(&mut connection, "PRAGMA CHECKPOINT")
    });
    supervisor.wait_barrier_and_kill(Duration::from_secs(120))?;
    let _ = trigger.join();
    supervisor.start(None)?;
    let after = oracle::capture(supervisor.address()?, config.profile.stage_timeout)?;
    if before != after {
        return Err(format!(
            "checkpoint crash at {point:?} changed logical state"
        ));
    }
    let mut connection = supervisor.connect(config.profile.stage_timeout)?;
    let observed = tcp_scalar_i64(
        &mut connection,
        &format!("SELECT COUNT(*) FROM crash_ledger WHERE id = {id} AND value = 1"),
    )?;
    if observed != 1 {
        return Err(format!(
            "checkpoint crash at {point:?} lost prepared row {id}"
        ));
    }
    record_crash_schedule(
        run_root,
        layer,
        point,
        "reopened",
        "same_logical_state",
        "same_logical_state",
    )?;
    Ok(CrashEvidence {
        layer: layer.to_string(),
        point: point.crash_name().to_string(),
        expected: "same_logical_state".to_string(),
        observed: "same_logical_state".to_string(),
    })
}

#[derive(Serialize)]
struct CrashScheduleRecord<'a> {
    layer: &'a str,
    point: &'a str,
    state: &'a str,
    expected: &'a str,
    observed: &'a str,
}

fn record_crash_schedule(
    run_root: &Path,
    layer: &str,
    point: InterleavePoint,
    state: &str,
    expected: &str,
    observed: &str,
) -> Result<(), String> {
    let path = run_root.join("crash-schedule.journal");
    let created = !path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| error.to_string())?;
    let record = CrashScheduleRecord {
        layer,
        point: point.crash_name(),
        state,
        expected,
        observed,
    };
    serde_json::to_writer(&mut file, &record).map_err(|error| error.to_string())?;
    file.write_all(b"\n").map_err(|error| error.to_string())?;
    file.flush().map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    if created {
        sync_directory(run_root)?;
    }
    Ok(())
}

fn runtime_stats(address: SocketAddr, timeout: Duration) -> Result<serde_json::Value, String> {
    let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
    let mut rows = tcp_rows(&mut connection, "PRAGMA RUNTIME_STATS")?;
    if rows.len() != 1 || rows[0].values.len() != 1 {
        return Err(format!(
            "PRAGMA RUNTIME_STATS returned invalid shape: {rows:?}"
        ));
    }
    let WireValue::String(payload) = rows.remove(0).values.remove(0) else {
        return Err("PRAGMA RUNTIME_STATS returned a non-text payload".to_string());
    };
    let value: serde_json::Value = serde_json::from_str(&payload)
        .map_err(|error| format!("parse PRAGMA RUNTIME_STATS: {error}"))?;
    if !value.is_object()
        || value
            .get("format")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        || value
            .get("sequence")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        || value.get("counters").is_none()
    {
        return Err("PRAGMA RUNTIME_STATS omitted required bounded evidence".to_string());
    }
    Ok(value)
}

fn restart_with_point(
    supervisor: &mut ServerSupervisor<'_>,
    point: InterleavePoint,
) -> Result<(), String> {
    supervisor.stop_graceful(Duration::from_secs(120))?;
    supervisor.start(Some(point.crash_name()))?;
    Ok(())
}

fn layer_context(
    address: SocketAddr,
    config: &ChaosConfig,
    journal_root: &Path,
    progress: &Arc<AtomicU64>,
) -> LayerContext {
    LayerContext {
        address,
        seed: config.seed,
        profile: config.profile.clone(),
        journal_root: journal_root.to_path_buf(),
        progress: Arc::clone(progress),
    }
}

fn prepare_run_root(config: &ChaosConfig) -> Result<PathBuf, String> {
    fs::create_dir_all(&config.root).map_err(|error| error.to_string())?;
    let run_root = config.root.join(&config.run_id);
    fs::create_dir(&run_root).map_err(|error| {
        format!(
            "create unique epoch chaos run root {}: {error}",
            run_root.display()
        )
    })?;
    sync_directory(&config.root)?;
    Ok(run_root)
}

fn candidate_identity() -> Result<CandidateIdentity, String> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let server = PathBuf::from(env!("CARGO_BIN_EXE_radixdb-server"));
    let commit = command_output(Command::new("git").arg("rev-parse").arg("HEAD"))?;
    let dirty = !command_output(
        Command::new("git")
            .arg("status")
            .arg("--porcelain")
            .arg("--untracked-files=normal"),
    )?
    .is_empty();
    Ok(CandidateIdentity {
        commit,
        dirty,
        cargo_lock_sha256: sha256_file(&manifest.join("Cargo.lock"))?,
        server_binary_sha256: sha256_file(&server)?,
        server_binary: server.display().to_string(),
    })
}

fn command_output(command: &mut Command) -> Result<String, String> {
    let output = command.output().map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!("identity command failed: {}", output.status));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_string())
        .map_err(|error| error.to_string())
}

fn profile_evidence(config: &ChaosConfig) -> ProfileEvidence {
    ProfileEvidence {
        kind: match config.profile.kind {
            ProfileKind::Full => "full",
            ProfileKind::Smoke => "smoke",
        }
        .to_string(),
        acceptance: config.profile.is_acceptance(),
        wide_actions: config.profile.wide_actions,
        large_workers: config.profile.large_workers,
        large_actions_per_worker: config.profile.large_actions_per_worker,
        micro_transactions: config.profile.micro_transactions,
        random_rungs: config.profile.random_rungs.clone(),
        random_operations_per_rung: config.profile.random_operations_per_rung,
        cold_rows: config.profile.cold_rows,
        hot_rows: config.profile.hot_rows,
        stage_timeout_millis: config
            .profile
            .stage_timeout
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    }
}

fn historical_evidence(summary: HistoricalChaosSummary) -> HistoricalEvidence {
    HistoricalEvidence {
        committed: summary.committed,
        rolled_back: summary.rolled_back,
        disconnected: summary.disconnected,
        conflicts: summary.conflicts,
        checkpoint_busy: summary.checkpoint_busy,
    }
}

fn staging_evidence(
    data_root: &Path,
    storage: &StorageSnapshot,
) -> Result<StagingEvidence, String> {
    let root = data_root.join("databases").join(DATABASE).join("staging");
    let mut writer_directories = 0_u64;
    for entry in fs::read_dir(&root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "staging root contains a non-directory writer entry: {}",
                entry.path().display()
            ));
        }
        writer_directories += 1;
    }
    let now_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    let discovery = discover_staging_publications(
        &root,
        now_unix_ns,
        Duration::from_secs(60 * 60).as_nanos() as u64,
        StagingDiscoveryLimits::default(),
    )
    .map_err(|error| error.to_string())?;
    let mut evidence = StagingEvidence {
        writer_directories,
        publications: discovery.publications().len() as u64,
        complete: 0,
        incomplete: 0,
        invalid: 0,
        active: 0,
        aged_orphan_candidates: 0,
        quarantine_required: 0,
        bytes: storage.staging_bytes,
    };
    for publication in discovery.publications() {
        match publication.completion() {
            StagingCompletion::Complete(_) => evidence.complete += 1,
            StagingCompletion::Incomplete => evidence.incomplete += 1,
            StagingCompletion::Invalid => evidence.invalid += 1,
        }
        match publication.disposition() {
            StagingDisposition::Active => evidence.active += 1,
            StagingDisposition::AgedOrphanCandidate => evidence.aged_orphan_candidates += 1,
            StagingDisposition::QuarantineRequired => evidence.quarantine_required += 1,
        }
    }
    Ok(evidence)
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())
}

fn write_markdown(path: &Path, report: &EpochChaosReport) -> Result<(), String> {
    let mut text = format!(
        "# Epoch chaos `{}`\n\n- Status: `{}`\n- Commit: `{}`\n- Seed: `{}`\n- Elapsed: `{}` ms\n- Acceptance profile: `{}`\n- Clean candidate: `{}`\n- Reopens: `{}`\n- Resumed from: `{}`\n\n## Layers\n\n| Layer | Operations | Transactions | Committed | Rolled back | Disconnected | Conflicted | Wall, ms | Ops/s | Active HWM | Max tx, ms | Peak RSS, MiB | Write B/op |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",
        report.run_id,
        report.status,
        report.identity.commit,
        report.seed,
        report.elapsed_millis,
        report.profile.acceptance,
        !report.identity.dirty,
        report.reopen_oracles.len(),
        report.resumed_from.as_deref().unwrap_or("none"),
    );
    for layer in &report.layers {
        text.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {:.3} | {:.2} | {:.2} |\n",
            layer.name,
            layer.semantic_operations,
            layer.transactions,
            layer.committed_transactions,
            layer.rolled_back_transactions,
            layer.disconnected_transactions,
            layer.conflicted_transactions,
            layer.wall_millis,
            layer.throughput_operations_per_second,
            layer.active_clients_high_watermark,
            layer.transaction_latency.max_nanos as f64 / 1_000_000.0,
            layer.telemetry.peak_rss_bytes as f64 / 1024.0 / 1024.0,
            layer.telemetry.write_amplification_bytes_per_operation,
        ));
    }
    text.push_str("\n## Stage sources\n\n");
    if report.stage_sources.is_empty() {
        text.push_str("- Non-acceptance smoke run; no resumable stage bundle.\n");
    } else {
        for source in &report.stage_sources {
            text.push_str(&format!(
                "- `{}`: run `{}`, commit `{}`, server `{}`\n",
                source.stage.name(),
                source.source_run_id,
                source.source_commit,
                source.server_binary_sha256
            ));
        }
    }
    text.push_str(&format!(
        "\n## Resources\n\n- Peak RSS: `{}` bytes\n- Peak swap: `{}` bytes\n- Peak threads: `{}`\n- Peak FDs: `{}`\n- Final storage: `{}` bytes\n- Staging: `{}` bytes in `{}` complete active publications across `{}` writer directories\n\n## Crash/reopen\n\n",
        report.telemetry.peak_rss_bytes,
        report.telemetry.peak_swap_bytes,
        report.telemetry.peak_threads,
        report.telemetry.peak_open_fds,
        report.final_storage.total_bytes,
        report.final_storage.staging_bytes,
        report.final_staging.complete,
        report.final_staging.writer_directories,
    ));
    for crash in &report.crashes {
        text.push_str(&format!(
            "- `{}` at `{}`: expected `{}`, observed `{}`\n",
            crash.layer, crash.point, crash.expected, crash.observed
        ));
    }
    let mut file = File::create(path).map_err(|error| error.to_string())?;
    file.write_all(text.as_bytes())
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn write_capacity_markdown(path: &Path, report: &CapacityReport) -> Result<(), String> {
    let text = format!(
        "# Epoch chaos capacity `{}`\n\n- Status: `{}`\n- Commit: `{}`\n- Seed: `{}`\n- Source bundle: `{}`\n- Source run: `{}`\n- Clients: `{}`\n- Requested CPU set: `{}`\n- Effective server CPU set: `{}`\n- Elapsed: `{}` ms\n- Operations: `{}`\n- Transactions: `{}`\n- Ops/s: `{:.2}`\n- Peak RSS: `{}` bytes\n- Peak swap: `{}` bytes\n- Final storage: `{}` bytes\n",
        report.run_id,
        report.status,
        report.identity.commit,
        report.seed,
        report.source_bundle,
        report.source_run_id,
        report.requested_clients,
        report.requested_cpu_set,
        report.effective_server_cpu_set,
        report.elapsed_millis,
        report.layer.semantic_operations,
        report.layer.transactions,
        report.layer.throughput_operations_per_second,
        report.telemetry.peak_rss_bytes,
        report.telemetry.peak_swap_bytes,
        report.final_storage.total_bytes,
    );
    let mut file = File::create(path).map_err(|error| error.to_string())?;
    file.write_all(text.as_bytes())
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn write_finalization_markdown(
    path: &Path,
    report: &ExistingRunFinalizationReport,
) -> Result<(), String> {
    let text = format!(
        "# Epoch chaos existing-run finalization `{}`\n\n- Status: `{}`\n- Source root: `{}`\n- Source commit: `{}`\n- Remediation commit: `{:?}`\n- Remediation paths: `{:?}`\n- Finalizer commit: `{}`\n- Elapsed: `{}` ms\n- Completed full layers: `{}`\n- Random 32 terminal operations: `{}`\n- Random 64 terminal operations: `{}`\n- Random 128 unsealed shards preserved as owner-accepted evidence: `{}`\n- Initial reopen oracle: `passed`\n- Checkpoint/reopen oracle: `passed`\n- Manifest crash/reopen oracle: `{}`\n- Final checkpoint/reopen oracle: `passed`\n- Source staging: `{}` incomplete / `{}` complete / `{}` bytes\n- Final staging: `{}` incomplete / `{}` complete / `{}` bytes\n- Peak RSS: `{}` bytes\n- Peak swap: `{}` bytes\n- Final storage: `{}` bytes\n\nThis report finalizes an owner-accepted historical run on a verified copy. It preserves the exact incomplete staging set left by the interrupted 128-client attempt and permits only one additional bounded COMPLETE residue from the injected manifest crash. It does not manufacture terminal journals and is not a canonical stage bundle.\n",
        report.run_id,
        report.status,
        report.source_root,
        report.source_commit,
        report.remediation_commit,
        report.remediation_paths,
        report.finalizer_identity.commit,
        report.elapsed_millis,
        report.completed_layers.len(),
        report.random_32.terminal.count,
        report.random_64.terminal.count,
        report.random_128_unsealed_shards,
        report.crash.observed,
        report.source_staging.incomplete,
        report.source_staging.complete,
        report.source_staging.bytes,
        report.final_staging.incomplete,
        report.final_staging.complete,
        report.final_staging.bytes,
        report.telemetry.peak_rss_bytes,
        report.telemetry.peak_swap_bytes,
        report.final_storage.total_bytes,
    );
    let mut file = File::create(path).map_err(|error| error.to_string())?;
    file.write_all(text.as_bytes())
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn cpu_allowed_list(pid: Option<u32>) -> Result<String, String> {
    let path = pid
        .map(|pid| PathBuf::from(format!("/proc/{pid}/status")))
        .unwrap_or_else(|| PathBuf::from("/proc/self/status"));
    let status = fs::read_to_string(&path)
        .map_err(|error| format!("read CPU affinity from {}: {error}", path.display()))?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{} has no Cpus_allowed_list", path.display()))
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

struct SemanticWatchdog {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SemanticWatchdog {
    fn start(
        stage: &str,
        progress: Arc<AtomicU64>,
        max_duration: Duration,
        max_stall: Duration,
        artifact: PathBuf,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let stage = stage.to_string();
        let worker = thread::Builder::new()
            .name(format!("epoch-watchdog-{stage}"))
            .spawn(move || {
                let started = Instant::now();
                let mut last_progress = progress.load(Ordering::Acquire);
                let mut last_change = Instant::now();
                while !worker_stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_secs(1));
                    let current = progress.load(Ordering::Acquire);
                    if current != last_progress {
                        last_progress = current;
                        last_change = Instant::now();
                    }
                    if started.elapsed() > max_duration || last_change.elapsed() > max_stall {
                        let payload = format!(
                            "{{\"stage\":\"{stage}\",\"semantic_progress\":{current},\"elapsed_millis\":{},\"stall_millis\":{}}}\n",
                            started.elapsed().as_millis(),
                            last_change.elapsed().as_millis()
                        );
                        if let Ok(mut file) = File::create(&artifact) {
                            let _ = file.write_all(payload.as_bytes());
                            let _ = file.sync_all();
                        }
                        std::process::abort();
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }

    fn stop(mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        self.worker
            .take()
            .ok_or_else(|| "semantic watchdog was already joined".to_string())?
            .join()
            .map_err(|_| "semantic watchdog panicked".to_string())
    }
}

impl Drop for SemanticWatchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_remediation_accepts_only_the_reviewed_version_store_pair() {
        let exact = DEEP_VERSION_HISTORY_REMEDIATION_PATHS
            .into_iter()
            .map(str::to_string)
            .collect();
        validate_deep_version_history_remediation_paths(&exact).unwrap();
    }

    #[test]
    fn historical_remediation_rejects_missing_or_additional_server_paths() {
        let mut missing = BTreeSet::from([DEEP_VERSION_HISTORY_REMEDIATION_PATHS[0].to_string()]);
        assert!(validate_deep_version_history_remediation_paths(&missing).is_err());

        missing.insert(DEEP_VERSION_HISTORY_REMEDIATION_PATHS[1].to_string());
        missing.insert("crates/radixdb-storage/src/lib.rs".to_string());
        assert!(validate_deep_version_history_remediation_paths(&missing).is_err());
    }

    #[test]
    fn owner_finalization_preserves_interrupted_source_and_one_crash_residue() {
        let source = StagingEvidence {
            writer_directories: 1_225,
            publications: 1,
            complete: 0,
            incomplete: 1,
            invalid: 0,
            active: 1,
            aged_orphan_candidates: 0,
            quarantine_required: 0,
            bytes: 43 * 1024 * 1024,
        };
        let final_state = StagingEvidence {
            writer_directories: 1_230,
            publications: 2,
            complete: 1,
            incomplete: 1,
            invalid: 0,
            active: 2,
            aged_orphan_candidates: 0,
            quarantine_required: 0,
            bytes: 47 * 1024 * 1024,
        };
        final_state.validate_owner_finalization(&source).unwrap();
    }

    #[test]
    fn owner_finalization_rejects_an_extra_incomplete_publication() {
        let source = StagingEvidence {
            writer_directories: 10,
            publications: 1,
            complete: 0,
            incomplete: 1,
            invalid: 0,
            active: 1,
            aged_orphan_candidates: 0,
            quarantine_required: 0,
            bytes: 1024,
        };
        let final_state = StagingEvidence {
            writer_directories: 12,
            publications: 3,
            complete: 1,
            incomplete: 2,
            invalid: 0,
            active: 3,
            aged_orphan_candidates: 0,
            quarantine_required: 0,
            bytes: 2048,
        };
        assert!(final_state.validate_owner_finalization(&source).is_err());
    }
}
