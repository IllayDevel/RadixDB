use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use signal_hook::consts::{SIGINT, SIGTERM};

use crate::{
    artifacts::ArtifactWriter,
    auth::BasicAuth,
    build_identity,
    config::{DatabaseEngine, ResolvedSoakConfig},
    diagnostics::{AgentTelemetryClient, SemanticProgressTracker},
    http::{StatusHub, StatusServer},
    metrics::ResourceSampler,
    runtime::RuntimeMetrics,
    status::{
        BuildIdentity, Counters, InvariantState, InvariantStatus, LatencySnapshot, ResourceSlopes,
        ResourceSnapshot, RunEvent, RunState, ServerRuntimeSnapshot, StatusSnapshot, WatchdogState,
    },
    workload, BUILD_PROFILE, BUILD_TARGET, CARGO_LOCK_SHA256, GIT_COMMIT,
};

const LOOP_POLL: Duration = Duration::from_millis(100);
const INTERRUPTED: &str = "run interrupted by SIGINT/SIGTERM";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RunMode {
    Full,
    SeedOnly,
}

impl RunMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::SeedOnly => "seed-only",
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    run_id: String,
    mode: RunMode,
    profile: String,
    duration_millis: u64,
    seed: u64,
    database_address: String,
    database_name: String,
    database_engine: DatabaseEngine,
    status_bind: String,
    artifacts_root: String,
    client_steps: Vec<usize>,
    active_rows: u64,
    import_dir: String,
    checkpoint_interval_millis: u64,
    sample_interval_millis: u64,
    invariant_interval_millis: u64,
    diagnostics_enabled: bool,
    diagnostics_bind: String,
    diagnostics_normal_interval_millis: u64,
    diagnostics_workload_stall_timeout_millis: u64,
    diagnostics_agent_heartbeat_timeout_millis: u64,
    diagnostics_artifact_quota_bytes: u64,
    diagnostics_max_incidents: usize,
    graceful_reopen_at_millis: Option<u64>,
    kill_reopen_at_millis: Option<u64>,
    recovery_timeout_millis: u64,
    server_executable: String,
    data_dir: String,
    soak_identity: String,
    server_identity: String,
    started_unix_millis: u64,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct FinalReport {
    format: u32,
    run_id: String,
    mode: RunMode,
    profile: String,
    database_engine: DatabaseEngine,
    state: RunState,
    elapsed_millis: u64,
    counters: Counters,
    latency: LatencySnapshot,
    resources: ResourceSnapshot,
    resource_slopes: ResourceSlopes,
    server_runtime: ServerRuntimeSnapshot,
    invariants: BTreeMap<String, InvariantStatus>,
    logical_digest: Option<String>,
    failure: Option<String>,
    telemetry_dropped: u64,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    sequence: u64,
    unix_millis: u64,
    elapsed_millis: u64,
    active_clients: u64,
    current_tps: f64,
    counters: Counters,
    latency: LatencySnapshot,
    resources: ResourceSnapshot,
    resource_slopes: ResourceSlopes,
    server_runtime: ServerRuntimeSnapshot,
}

#[derive(Default)]
struct FaultRuntime {
    graceful_done: bool,
    kill_done: bool,
}

struct TpsWindow {
    at: Instant,
    completed: u64,
}

impl TpsWindow {
    fn new() -> Self {
        Self {
            at: Instant::now(),
            completed: 0,
        }
    }

    fn observe(&mut self, completed: u64) -> f64 {
        let now = Instant::now();
        let seconds = now.duration_since(self.at).as_secs_f64();
        let value = if seconds > 0.0 {
            completed.saturating_sub(self.completed) as f64 / seconds
        } else {
            0.0
        };
        self.at = now;
        self.completed = completed;
        value
    }
}

#[derive(Clone, Copy)]
enum FaultKind {
    Graceful,
    Kill,
}

impl FaultKind {
    const fn signal(self, engine: DatabaseEngine) -> libc::c_int {
        match (self, engine) {
            // PostgreSQL reserves SIGTERM for a "smart" shutdown, which waits
            // for every workload client to disconnect. The soak coordinator
            // deliberately keeps those clients alive to verify reconnect, so
            // use PostgreSQL's safe "fast" shutdown (SIGINT) instead.
            (Self::Graceful, DatabaseEngine::Postgresql) => libc::SIGINT,
            (Self::Graceful, DatabaseEngine::Radixdb) => libc::SIGTERM,
            (Self::Kill, _) => libc::SIGKILL,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Graceful => "graceful_reopen",
            Self::Kill => "kill_reopen",
        }
    }
}

pub fn run(config: ResolvedSoakConfig, run_id: &str) -> Result<RunState, String> {
    run_with_mode(config, run_id, RunMode::Full)
}

pub fn seed(config: ResolvedSoakConfig, run_id: &str) -> Result<RunState, String> {
    run_with_mode(config, run_id, RunMode::SeedOnly)
}

fn run_with_mode(
    config: ResolvedSoakConfig,
    run_id: &str,
    mode: RunMode,
) -> Result<RunState, String> {
    require_release_identity()?;
    let auth = BasicAuth::load(&config.status.auth_file).map_err(|error| error.to_string())?;
    let started_wall = unix_millis()?;
    let started = Instant::now();
    let mut writer = ArtifactWriter::create(&config.artifacts.root, run_id)
        .map_err(|error| format!("create run artifacts: {error}"))?;
    let mut progress = SemanticProgressTracker::new(started_wall, "starting")?;
    if config.diagnostics.enabled {
        let telemetry = AgentTelemetryClient::connect(&config.diagnostics.channel_path, run_id)
            .map_err(|error| format!("connect diagnostics observer: {error}"))?;
        writer
            .attach_telemetry(
                telemetry,
                progress.snapshot(),
                config.diagnostics.normal_interval,
            )
            .map_err(|error| format!("start diagnostics heartbeat: {error}"))?;
    }
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("write initial semantic progress: {error}"))?;
    let initial = initial_status(&config, run_id, started_wall);
    let hub = StatusHub::new(initial, config.status.event_capacity)?;
    let status_server = StatusServer::start(config.status.bind, auth, hub.clone())
        .map_err(|error| format!("start status server: {error}"))?;
    let result = run_inner(
        &config,
        run_id,
        mode,
        started,
        started_wall,
        &hub,
        &mut writer,
        &mut progress,
    );
    let terminal = match result {
        Ok(state) => state,
        Err(error) => {
            let now = unix_millis().unwrap_or(started_wall);
            let state = if error == INTERRUPTED {
                RunState::Interrupted
            } else {
                RunState::Failed
            };
            let terminal_phase = if state == RunState::Interrupted {
                "interrupted"
            } else {
                "failed"
            };
            let _ = progress.advance_phase(now, terminal_phase);
            let _ = writer.append_semantic_progress(progress.snapshot());
            let _ = hub.update(|status| {
                status.state = state;
                status.phase = if state == RunState::Interrupted {
                    "interrupted".into()
                } else {
                    "failed".into()
                };
                status.updated_unix_millis = now;
                status.elapsed_millis = elapsed_millis(started);
                status.remaining_millis = 0;
                status.failure = Some(error.clone());
            });
            let _ = record_event(
                &hub,
                &mut writer,
                now,
                if state == RunState::Interrupted {
                    "interrupted"
                } else {
                    "failed"
                },
                &error,
            );
            state
        }
    };
    let snapshot = hub.snapshot();
    writer
        .publish_status(&snapshot)
        .map_err(|error| format!("publish terminal status: {error}"))?;
    let report = FinalReport {
        format: 1,
        run_id: snapshot.run_id.clone(),
        mode,
        profile: snapshot.profile.clone(),
        database_engine: config.database.engine,
        state: snapshot.state,
        elapsed_millis: snapshot.elapsed_millis,
        counters: snapshot.counters.clone(),
        latency: snapshot.latency.clone(),
        resources: snapshot.resources.clone(),
        resource_slopes: snapshot.resource_slopes.clone(),
        server_runtime: snapshot.server_runtime.clone(),
        invariants: snapshot.invariants.clone(),
        logical_digest: snapshot.logical_digest.clone(),
        failure: snapshot.failure.clone(),
        telemetry_dropped: writer.telemetry_dropped(),
    };
    let markdown = markdown_report(&report);
    writer
        .write_report(&report, &markdown)
        .map_err(|error| format!("finalize run artifacts: {error}"))?;
    status_server
        .shutdown()
        .map_err(|error| format!("stop status server: {error}"))?;
    if terminal == RunState::Passed {
        Ok(terminal)
    } else {
        Err(snapshot
            .failure
            .unwrap_or_else(|| format!("run ended as {terminal:?}")))
    }
}

#[allow(clippy::too_many_arguments)]
fn run_inner(
    config: &ResolvedSoakConfig,
    run_id: &str,
    mode: RunMode,
    started: Instant,
    started_wall: u64,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
) -> Result<RunState, String> {
    update_phase(
        hub,
        writer,
        progress,
        started,
        "preflight",
        RunState::Preparing,
    )?;
    let server_identity = verify_server_identity(config)?;
    let manifest = manifest(config, run_id, mode, started_wall, &server_identity);
    writer
        .write_manifest(&manifest)
        .map_err(|error| format!("write manifest: {error}"))?;
    hub.update(|status| status.identity.server = Some(server_identity))?;
    let global_stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGINT, Arc::clone(&global_stop))
        .map_err(|error| error.to_string())?;
    signal_hook::flag::register(SIGTERM, Arc::clone(&global_stop))
        .map_err(|error| error.to_string())?;
    let mut coordinator = workload::connect(&config.database)?;
    let metrics = Arc::new(RuntimeMetrics::default());
    let mut resources = ResourceSampler::new(
        config.monitor.server_executable.clone(),
        config.monitor.data_dir.clone(),
        config.database.engine,
    );
    let mut tps_window = TpsWindow::new();
    let mut sample_sequence = 0u64;
    let mut seeded_rows = 0u64;
    workload::install_schema(
        &mut coordinator,
        *config
            .load
            .client_steps
            .last()
            .ok_or("empty client ladder")?,
    )?;
    record_event(
        hub,
        writer,
        unix_millis()?,
        "seed_started",
        &format!("target_rows={}", config.load.active_rows),
    )?;
    let mut next_seed_sample = Instant::now();
    let mut previous_seed_engine = workload::seed_engine_counters(&mut coordinator)?;
    workload::seed_cold_rows(
        &mut coordinator,
        &config.load.import_dir,
        config.load.active_rows,
        config.database.read_timeout,
        |connection, milestone| {
            if global_stop.load(Ordering::Acquire) {
                return Err(INTERRUPTED.into());
            }
            let wall = unix_millis()?;
            let loaded = match milestone {
                workload::SeedMilestone::RowsLoaded(metrics) => metrics.loaded_rows,
                workload::SeedMilestone::CopyBackpressure { loaded_rows, .. } => loaded_rows,
                workload::SeedMilestone::CheckpointStarted(loaded)
                | workload::SeedMilestone::CheckpointCompleted(loaded) => loaded,
            };
            let seed_chunk_metrics = match milestone {
                workload::SeedMilestone::RowsLoaded(metrics) => Some(metrics),
                _ => None,
            };
            let seed_engine_delta = if matches!(milestone, workload::SeedMilestone::RowsLoaded(_)) {
                let current = workload::seed_engine_counters(connection)?;
                let delta = current.delta(previous_seed_engine);
                previous_seed_engine = current;
                Some(delta)
            } else {
                None
            };
            progress.heartbeat(wall);
            if matches!(milestone, workload::SeedMilestone::RowsLoaded(_)) {
                let delta = loaded.saturating_sub(seeded_rows);
                if progress.advance_workload(wall, delta)?.is_some() {
                    seeded_rows = loaded;
                    writer
                        .append_semantic_progress(progress.snapshot())
                        .map_err(|error| format!("append seed progress: {error}"))?;
                }
            }
            match milestone {
                workload::SeedMilestone::CheckpointStarted(_) => {
                    let deadline = wall.saturating_add(hub.snapshot().watchdog_timeout_millis);
                    progress.begin_operation(wall, "seed_checkpoint", Some(deadline))?;
                    writer
                        .append_semantic_progress(progress.snapshot())
                        .map_err(|error| format!("append seed checkpoint start: {error}"))?;
                }
                workload::SeedMilestone::CheckpointCompleted(_) => {
                    progress.complete_operation(wall)?;
                    writer
                        .append_semantic_progress(progress.snapshot())
                        .map_err(|error| format!("append seed checkpoint end: {error}"))?;
                }
                workload::SeedMilestone::RowsLoaded(_) => {}
                workload::SeedMilestone::CopyBackpressure { .. } => {}
            }
            hub.update(|status| {
                status.updated_unix_millis = wall;
                status.last_progress_unix_millis =
                    progress.snapshot().last_workload_progress_unix_millis;
                status.phase = match milestone {
                    workload::SeedMilestone::RowsLoaded(_) => format!("seeding-{loaded}"),
                    workload::SeedMilestone::CopyBackpressure { retry_attempt, .. } => {
                        format!("seed-backpressure-{loaded}-retry-{retry_attempt}")
                    }
                    workload::SeedMilestone::CheckpointStarted(_) => {
                        format!("seed-checkpoint-{loaded}")
                    }
                    workload::SeedMilestone::CheckpointCompleted(_) => {
                        format!("seeding-{loaded}")
                    }
                };
            })?;
            match milestone {
                workload::SeedMilestone::RowsLoaded(_) => {
                    let metrics = seed_chunk_metrics.expect("row milestone must carry metrics");
                    let engine = seed_engine_delta.expect("row milestone must carry engine delta");
                    record_event(
                        hub,
                        writer,
                        wall,
                        "seed_progress",
                        &seed_progress_detail(
                            loaded,
                            metrics.chunk_rows,
                            metrics.csv_nanos,
                            metrics.copy_nanos,
                            metrics.total_nanos,
                            &engine,
                        ),
                    )?;
                }
                workload::SeedMilestone::CopyBackpressure {
                    retry_attempt,
                    elapsed_nanos,
                    ..
                } => record_event(
                    hub,
                    writer,
                    wall,
                    "seed_copy_backpressure",
                    &format!(
                        "loaded_rows={loaded} retry_attempt={retry_attempt} elapsed_ms={}",
                        elapsed_nanos / 1_000_000
                    ),
                )?,
                workload::SeedMilestone::CheckpointStarted(_) => record_event(
                    hub,
                    writer,
                    wall,
                    "seed_checkpoint_started",
                    &format!("loaded_rows={loaded}"),
                )?,
                workload::SeedMilestone::CheckpointCompleted(_) => {
                    metrics.checkpoint();
                    record_event(
                        hub,
                        writer,
                        wall,
                        "seed_checkpoint_completed",
                        &format!("loaded_rows={loaded}"),
                    )?;
                }
            }
            if Instant::now() >= next_seed_sample {
                sample_sequence = sample_sequence.saturating_add(1);
                publish_sample(
                    started,
                    config.duration,
                    &metrics,
                    &mut resources,
                    connection,
                    &mut tps_window,
                    hub,
                    writer,
                    progress,
                    sample_sequence,
                )?;
                next_seed_sample = Instant::now() + config.load.sample_interval;
            } else {
                writer
                    .publish_status(&hub.snapshot())
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        },
    )?;
    if mode == RunMode::SeedOnly {
        update_phase(
            hub,
            writer,
            progress,
            started,
            "seed-verifying",
            RunState::Quiescing,
        )?;
        check_and_publish_invariants(
            &mut coordinator,
            config.load.active_rows,
            &metrics,
            hub,
            writer,
            progress,
        )?;
        sample_sequence = sample_sequence.saturating_add(1);
        publish_sample(
            started,
            Duration::ZERO,
            &metrics,
            &mut resources,
            &mut coordinator,
            &mut tps_window,
            hub,
            writer,
            progress,
            sample_sequence,
        )?;
        let now = unix_millis()?;
        update_phase(hub, writer, progress, started, "passed", RunState::Passed)?;
        hub.update(|status| {
            status.state = RunState::Passed;
            status.updated_unix_millis = now;
            status.elapsed_millis = elapsed_millis(started);
            status.remaining_millis = 0;
            status.last_progress_unix_millis =
                progress.snapshot().last_workload_progress_unix_millis;
        })?;
        record_event(
            hub,
            writer,
            now,
            "passed",
            "seed, checkpoint and invariants passed",
        )?;
        return Ok(RunState::Passed);
    }
    let recovering = Arc::new(AtomicBool::new(false));
    let recovery_epoch = Arc::new(AtomicU64::new(0));
    let workload_started = Instant::now();
    let deadline = workload_started + config.duration;
    let phase_count =
        u32::try_from(config.load.client_steps.len()).map_err(|_| "phase overflow")?;
    let phase_duration = config.duration / phase_count;
    let mut fault_runtime = FaultRuntime::default();

    for (phase_index, clients) in config.load.client_steps.iter().copied().enumerate() {
        if global_stop.load(Ordering::Acquire) {
            return interrupt(hub, writer, started);
        }
        let phase_name = format!("clients-{clients}");
        update_phase(
            hub,
            writer,
            progress,
            started,
            &phase_name,
            RunState::Running,
        )?;
        let phase_stop = Arc::new(AtomicBool::new(false));
        let phase_deadline = if phase_index + 1 == config.load.client_steps.len() {
            deadline
        } else {
            (workload_started + phase_duration * u32::try_from(phase_index + 1).unwrap())
                .min(deadline)
        };
        let (errors_tx, errors_rx) = mpsc::channel::<String>();
        let mut workers = Vec::with_capacity(clients);
        for worker_id in 0..clients {
            let database = config.database.clone();
            let seed = config.seed;
            let sequence_start = u64::try_from(phase_index)
                .map_err(|_| "phase index does not fit u64")?
                .checked_mul(100_000_000)
                .ok_or("phase sequence offset overflow")?;
            let history_path = writer.run_dir().join(format!(
                "histories/phase-{phase_index:02}-worker-{worker_id:03}.csv"
            ));
            let stop = Arc::clone(&phase_stop);
            let metrics = Arc::clone(&metrics);
            let recovering = Arc::clone(&recovering);
            let recovery_epoch = Arc::clone(&recovery_epoch);
            let errors = errors_tx.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("radixdb-soak-{worker_id}"))
                    .spawn(move || {
                        if let Err(error) = workload::worker_loop(
                            database,
                            worker_id,
                            seed,
                            sequence_start,
                            history_path,
                            phase_deadline,
                            stop,
                            recovering,
                            recovery_epoch,
                            metrics,
                        ) {
                            let _ = errors.send(error);
                        }
                    })
                    .map_err(|error| format!("spawn worker {worker_id}: {error}"))?,
            );
        }
        drop(errors_tx);
        hub.update(|status| {
            status.active_clients = clients as u64;
            status.target_clients = clients as u64;
        })?;
        let phase_result = observe_phase(
            config,
            started,
            workload_started,
            phase_deadline,
            &global_stop,
            &phase_stop,
            &metrics,
            &recovering,
            &recovery_epoch,
            &mut resources,
            &mut coordinator,
            &mut fault_runtime,
            &mut tps_window,
            &mut sample_sequence,
            &errors_rx,
            hub,
            writer,
            progress,
        );
        phase_stop.store(true, Ordering::Release);
        let mut panic = None;
        for worker in workers {
            if worker.join().is_err() {
                panic = Some("workload worker panicked".to_string());
            }
        }
        hub.update(|status| status.active_clients = 0)?;
        if let Err(error) = phase_result {
            if global_stop.load(Ordering::Acquire) {
                return interrupt(hub, writer, started);
            }
            return Err(error);
        }
        if let Some(error) = panic {
            return Err(error);
        }
    }

    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-checkpoint",
        RunState::Quiescing,
    )?;
    workload::checkpoint(&mut coordinator).map_err(|error| format!("final checkpoint: {error}"))?;
    metrics.checkpoint();
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-invariants",
        RunState::Quiescing,
    )?;
    check_and_publish_invariants(
        &mut coordinator,
        config.load.active_rows,
        &metrics,
        hub,
        writer,
        progress,
    )
    .map_err(|error| format!("final invariants: {error}"))?;
    let logical_digest = backup_restore_gate(
        config,
        &mut coordinator,
        &metrics,
        hub,
        writer,
        progress,
        started,
    )?;
    hub.update(|status| status.logical_digest = Some(logical_digest))?;
    sample_sequence = sample_sequence.saturating_add(1);
    publish_sample(
        started,
        Duration::ZERO,
        &metrics,
        &mut resources,
        &mut coordinator,
        &mut tps_window,
        hub,
        writer,
        progress,
        sample_sequence,
    )?;
    let now = unix_millis()?;
    update_phase(hub, writer, progress, started, "passed", RunState::Passed)?;
    hub.update(|status| {
        status.state = RunState::Passed;
        status.updated_unix_millis = now;
        status.elapsed_millis = elapsed_millis(started);
        status.remaining_millis = 0;
        status.last_progress_unix_millis = progress.snapshot().last_workload_progress_unix_millis;
    })?;
    record_event(
        hub,
        writer,
        now,
        "passed",
        "all workload phases and invariants passed",
    )?;
    Ok(RunState::Passed)
}

#[allow(clippy::too_many_arguments)]
fn observe_phase(
    config: &ResolvedSoakConfig,
    started: Instant,
    workload_started: Instant,
    phase_deadline: Instant,
    global_stop: &AtomicBool,
    phase_stop: &AtomicBool,
    metrics: &RuntimeMetrics,
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    resources: &mut ResourceSampler,
    coordinator: &mut workload::DatabaseConnection,
    faults: &mut FaultRuntime,
    tps_window: &mut TpsWindow,
    sample_sequence: &mut u64,
    errors: &mpsc::Receiver<String>,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
) -> Result<(), String> {
    let mut next_sample = Instant::now();
    let mut next_invariant = Instant::now();
    let mut next_checkpoint = Instant::now() + config.load.checkpoint_interval;
    while Instant::now() < phase_deadline {
        if global_stop.load(Ordering::Acquire) {
            phase_stop.store(true, Ordering::Release);
            return Err(INTERRUPTED.into());
        }
        if let Ok(error) = errors.try_recv() {
            phase_stop.store(true, Ordering::Release);
            return Err(error);
        }
        let mut now = Instant::now();
        if let Some(kind) = due_fault(config, faults, workload_started, now) {
            controlled_reopen(
                config,
                kind,
                recovering,
                recovery_epoch,
                resources,
                coordinator,
                metrics,
                hub,
                writer,
                progress,
            )?;
            // controlled_reopen already performs a full invariant snapshot.
            // Restart maintenance intervals from recovery completion instead
            // of replaying overdue work against a cold, reconnecting server.
            now = Instant::now();
            next_invariant = now + config.load.invariant_interval;
            next_checkpoint = now + config.load.checkpoint_interval;
        }
        if now >= next_invariant {
            check_and_publish_invariants(
                coordinator,
                config.load.active_rows,
                metrics,
                hub,
                writer,
                progress,
            )?;
            now = Instant::now();
            next_invariant = now + config.load.invariant_interval;
        }
        if now >= next_checkpoint {
            let wall = unix_millis()?;
            let checkpoint_deadline = wall.saturating_add(hub.snapshot().watchdog_timeout_millis);
            progress.begin_operation(wall, "checkpoint", Some(checkpoint_deadline))?;
            writer
                .append_semantic_progress(progress.snapshot())
                .map_err(|error| format!("append checkpoint start: {error}"))?;
            match workload::checkpoint_during_load(coordinator)? {
                workload::CheckpointOutcome::Completed => {
                    metrics.checkpoint();
                    progress.complete_operation(unix_millis()?)?;
                    writer
                        .append_semantic_progress(progress.snapshot())
                        .map_err(|error| format!("append checkpoint completion: {error}"))?;
                    record_event(hub, writer, wall, "checkpoint", "checkpoint completed")?;
                }
                workload::CheckpointOutcome::Deferred(reason) => {
                    metrics.checkpoint_deferred();
                    progress.complete_operation(unix_millis()?)?;
                    writer
                        .append_semantic_progress(progress.snapshot())
                        .map_err(|error| format!("append checkpoint deferral: {error}"))?;
                    record_event(hub, writer, wall, "checkpoint_deferred", &reason)?;
                }
            }
            now = Instant::now();
            next_checkpoint = now + config.load.checkpoint_interval;
        }
        if now >= next_sample {
            *sample_sequence = (*sample_sequence).saturating_add(1);
            publish_sample(
                started,
                config
                    .duration
                    .saturating_sub(now.duration_since(workload_started)),
                metrics,
                resources,
                coordinator,
                tps_window,
                hub,
                writer,
                progress,
                *sample_sequence,
            )?;
            now = Instant::now();
            next_sample = now + config.load.sample_interval;
        }
        thread::sleep(LOOP_POLL.min(phase_deadline.saturating_duration_since(Instant::now())));
    }
    if let Ok(error) = errors.try_recv() {
        return Err(error);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn publish_sample(
    started: Instant,
    remaining: Duration,
    metrics: &RuntimeMetrics,
    resources: &mut ResourceSampler,
    coordinator: &mut workload::DatabaseConnection,
    tps_window: &mut TpsWindow,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
    sequence: u64,
) -> Result<(), String> {
    let wall = unix_millis()?;
    let elapsed = elapsed_millis(started);
    let (resource_snapshot, slopes) = resources
        .sample()
        .map_err(|error| format!("sample server resources: {error}"))?;
    let server_runtime = crate::database::server_runtime(coordinator)?;
    let previous = hub.snapshot();
    let counters = metrics.counters(previous.counters.auth_failures);
    let previous_completed = previous
        .counters
        .transactions_committed
        .saturating_add(previous.counters.transactions_rolled_back)
        .saturating_add(previous.counters.disconnects)
        .saturating_add(previous.counters.conflicts);
    let completed = counters
        .transactions_committed
        .saturating_add(counters.transactions_rolled_back)
        .saturating_add(counters.disconnects)
        .saturating_add(counters.conflicts);
    let workload_advanced = completed > previous_completed;
    progress.heartbeat(wall);
    let semantic_advanced = progress
        .advance_workload(wall, completed.saturating_sub(previous_completed))?
        .is_some();
    if semantic_advanced {
        writer
            .append_semantic_progress(progress.snapshot())
            .map_err(|error| format!("append workload progress: {error}"))?;
    } else {
        writer.publish_agent_heartbeat(wall, progress.snapshot());
    }
    let current_tps = tps_window.observe(completed);
    let latency = metrics.latency();
    hub.update(|status| {
        status.updated_unix_millis = wall;
        status.elapsed_millis = elapsed;
        status.remaining_millis = millis(remaining);
        if workload_advanced {
            status.last_progress_unix_millis =
                progress.snapshot().last_workload_progress_unix_millis;
        }
        status.current_tps = current_tps;
        status.counters = counters.clone();
        status.latency = latency.clone();
        status.resources = resource_snapshot.clone();
        status.resource_slopes = slopes.clone();
        status.server_runtime = server_runtime.clone();
    })?;
    let sample = Sample {
        sequence,
        unix_millis: wall,
        elapsed_millis: elapsed,
        active_clients: hub.snapshot().active_clients,
        current_tps,
        counters,
        latency,
        resources: resource_snapshot,
        resource_slopes: slopes,
        server_runtime,
    };
    writer
        .append_sample(&sample)
        .map_err(|error| format!("append sample: {error}"))?;
    writer
        .publish_status(&hub.snapshot())
        .map_err(|error| format!("publish status: {error}"))?;
    writer
        .sync()
        .map_err(|error| format!("sync samples: {error}"))
}

fn check_and_publish_invariants(
    coordinator: &mut workload::DatabaseConnection,
    expected_cold_rows: u64,
    metrics: &RuntimeMetrics,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
) -> Result<(), String> {
    let wall = unix_millis()?;
    let deadline = wall.saturating_add(hub.snapshot().watchdog_timeout_millis);
    progress.begin_operation(wall, "invariant_scan", Some(deadline))?;
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("append invariant start: {error}"))?;
    let observations = workload::check_invariants(coordinator, expected_cold_rows)?;
    for observation in observations {
        let failed = observation.failures > 0;
        if failed {
            metrics.invariant_failed();
        } else {
            metrics.invariant_passed();
        }
        hub.update(|status| {
            let invariant = status
                .invariants
                .entry(observation.name.clone())
                .or_insert_with(|| InvariantStatus {
                    name: observation.name.clone(),
                    state: InvariantState::Unknown,
                    checks: 0,
                    failures: 0,
                    last_checked_unix_millis: None,
                    detail: String::new(),
                });
            invariant.checks = invariant.checks.saturating_add(1);
            invariant.failures = invariant.failures.saturating_add(observation.failures);
            invariant.state = if failed {
                InvariantState::Failed
            } else {
                InvariantState::Passed
            };
            invariant.last_checked_unix_millis = Some(wall);
            invariant.detail = observation.detail.clone();
            if failed {
                status.failure = Some(format!(
                    "invariant {} failed: {}",
                    observation.name, observation.detail
                ));
                status.counters.invariant_failures = status
                    .counters
                    .invariant_failures
                    .saturating_add(observation.failures);
            }
        })?;
        record_event(
            hub,
            writer,
            wall,
            if failed {
                "invariant_failed"
            } else {
                "invariant_passed"
            },
            &format!("{}: {}", observation.name, observation.detail),
        )?;
        if failed {
            return Err(format!(
                "invariant {} failed: {}",
                observation.name, observation.detail
            ));
        }
    }
    progress.complete_operation(unix_millis()?)?;
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("append invariant completion: {error}"))?;
    Ok(())
}

fn initial_status(config: &ResolvedSoakConfig, run_id: &str, now: u64) -> StatusSnapshot {
    StatusSnapshot {
        format: 1,
        run_id: run_id.into(),
        profile: config.profile.as_str().into(),
        seed: config.seed,
        state: RunState::Starting,
        phase: "starting".into(),
        started_unix_millis: now,
        updated_unix_millis: now,
        elapsed_millis: 0,
        remaining_millis: config.duration.as_millis().min(u128::from(u64::MAX)) as u64,
        last_progress_unix_millis: now,
        watchdog_timeout_millis: millis(
            config
                .faults
                .recovery_timeout
                .saturating_add(Duration::from_secs(60))
                .max(config.load.invariant_interval.saturating_mul(3)),
        ),
        watchdog_silence_millis: 0,
        watchdog_state: WatchdogState::Healthy,
        active_clients: 0,
        target_clients: 0,
        current_tps: 0.0,
        identity: BuildIdentity {
            soak: build_identity(),
            server: None,
            cargo_lock_sha256: CARGO_LOCK_SHA256.into(),
        },
        counters: Counters::default(),
        latency: LatencySnapshot::default(),
        resources: ResourceSnapshot::default(),
        resource_slopes: ResourceSlopes::default(),
        server_runtime: ServerRuntimeSnapshot::default(),
        invariants: BTreeMap::new(),
        logical_digest: None,
        failure: None,
    }
}

fn manifest(
    config: &ResolvedSoakConfig,
    run_id: &str,
    mode: RunMode,
    started_unix_millis: u64,
    server_identity: &str,
) -> Manifest {
    Manifest {
        format: config.format,
        run_id: run_id.into(),
        mode,
        profile: config.profile.as_str().into(),
        duration_millis: config.duration.as_millis().min(u128::from(u64::MAX)) as u64,
        seed: config.seed,
        database_address: config.database.address.to_string(),
        database_name: config.database.name.clone(),
        database_engine: config.database.engine,
        status_bind: config.status.bind.to_string(),
        artifacts_root: config.artifacts.root.display().to_string(),
        client_steps: config.load.client_steps.clone(),
        active_rows: config.load.active_rows,
        import_dir: config.load.import_dir.display().to_string(),
        checkpoint_interval_millis: millis(config.load.checkpoint_interval),
        sample_interval_millis: millis(config.load.sample_interval),
        invariant_interval_millis: millis(config.load.invariant_interval),
        diagnostics_enabled: config.diagnostics.enabled,
        diagnostics_bind: config.diagnostics.bind.to_string(),
        diagnostics_normal_interval_millis: millis(config.diagnostics.normal_interval),
        diagnostics_workload_stall_timeout_millis: millis(
            config.diagnostics.workload_stall_timeout,
        ),
        diagnostics_agent_heartbeat_timeout_millis: millis(
            config.diagnostics.agent_heartbeat_timeout,
        ),
        diagnostics_artifact_quota_bytes: config.diagnostics.artifact_quota_bytes,
        diagnostics_max_incidents: config.diagnostics.max_incidents,
        graceful_reopen_at_millis: config.faults.graceful_reopen_at.map(millis),
        kill_reopen_at_millis: config.faults.kill_reopen_at.map(millis),
        recovery_timeout_millis: millis(config.faults.recovery_timeout),
        server_executable: config.monitor.server_executable.display().to_string(),
        data_dir: config.monitor.data_dir.display().to_string(),
        soak_identity: build_identity(),
        server_identity: server_identity.into(),
        started_unix_millis,
    }
}

fn due_fault(
    config: &ResolvedSoakConfig,
    runtime: &mut FaultRuntime,
    workload_started: Instant,
    now: Instant,
) -> Option<FaultKind> {
    let elapsed = now.duration_since(workload_started);
    if !runtime.graceful_done
        && config
            .faults
            .graceful_reopen_at
            .is_some_and(|at| elapsed >= at)
    {
        runtime.graceful_done = true;
        return Some(FaultKind::Graceful);
    }
    if !runtime.kill_done && config.faults.kill_reopen_at.is_some_and(|at| elapsed >= at) {
        runtime.kill_done = true;
        return Some(FaultKind::Kill);
    }
    None
}

fn backup_restore_gate(
    config: &ResolvedSoakConfig,
    source: &mut workload::DatabaseConnection,
    metrics: &RuntimeMetrics,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
    started: Instant,
) -> Result<String, String> {
    if config.database.engine == DatabaseEngine::Postgresql {
        update_phase(
            hub,
            writer,
            progress,
            started,
            "final-digest",
            RunState::Quiescing,
        )?;
        return workload::logical_digest(source)
            .map_err(|error| format!("final PostgreSQL logical digest: {error}"));
    }
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-snapshot",
        RunState::Quiescing,
    )?;
    workload::snapshot(source).map_err(|error| format!("final snapshot publication: {error}"))?;
    metrics.backup();
    let source_digest = workload::logical_digest(source)
        .map_err(|error| format!("final source logical digest: {error}"))?;
    let source_snapshots = config
        .monitor
        .data_dir
        .join("databases")
        .join(&config.database.name)
        .join("snapshots");
    let evidence = writer.run_dir().join("backups/final-snapshot");
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-snapshot-evidence",
        RunState::Quiescing,
    )?;
    copy_tree_new(&source_snapshots, &evidence)
        .map_err(|error| format!("final snapshot evidence copy: {error}"))?;

    let restore_name = format!("{}_restore", config.database.name);
    if restore_name.len() > 64 {
        return Err("restore database name exceeds 64 bytes".into());
    }
    let restore_root = config
        .monitor
        .data_dir
        .join("databases")
        .join(&restore_name);
    if restore_root.exists() {
        return Err(format!(
            "restore database already exists: {}",
            restore_root.display()
        ));
    }
    fs::create_dir(&restore_root).map_err(|error| format!("create final restore root: {error}"))?;
    copy_tree_new(&evidence, &restore_root.join("snapshots"))
        .map_err(|error| format!("stage final restore snapshot: {error}"))?;
    let mut restore_config = config.database.clone();
    restore_config.name = restore_name;
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-restore",
        RunState::Quiescing,
    )?;
    let mut restored = workload::connect(&restore_config)
        .map_err(|error| format!("connect final restore database: {error}"))?;
    workload::restore(&mut restored).map_err(|error| format!("final snapshot restore: {error}"))?;
    metrics.restore();
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-restore-invariants",
        RunState::Quiescing,
    )?;
    for observation in workload::check_invariants(&mut restored, config.load.active_rows)
        .map_err(|error| format!("final restored invariants: {error}"))?
    {
        if observation.failures > 0 {
            return Err(format!(
                "restored invariant {} failed: {}",
                observation.name, observation.detail
            ));
        }
    }
    update_phase(
        hub,
        writer,
        progress,
        started,
        "final-digest",
        RunState::Quiescing,
    )?;
    let restored_digest = workload::logical_digest(&mut restored)
        .map_err(|error| format!("final restored logical digest: {error}"))?;
    if source_digest != restored_digest {
        return Err(format!(
            "restored logical digest differs: source={source_digest} restored={restored_digest}"
        ));
    }
    Ok(source_digest)
}

fn copy_tree_new(source: &Path, target: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| error.to_string())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(format!(
            "source is not a plain directory: {}",
            source.display()
        ));
    }
    fs::create_dir(target).map_err(|error| error.to_string())?;
    for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_type = entry.file_type().map_err(|error| error.to_string())?;
        if entry_type.is_symlink() {
            return Err(format!(
                "snapshot contains forbidden symlink: {}",
                entry.path().display()
            ));
        }
        let destination = target.join(entry.file_name());
        if entry_type.is_dir() {
            copy_tree_new(&entry.path(), &destination)?;
        } else if entry_type.is_file() {
            fs::copy(entry.path(), destination).map_err(|error| error.to_string())?;
        } else {
            return Err(format!(
                "snapshot contains non-file entry: {}",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn controlled_reopen(
    config: &ResolvedSoakConfig,
    kind: FaultKind,
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    resources: &mut ResourceSampler,
    coordinator: &mut workload::DatabaseConnection,
    metrics: &RuntimeMetrics,
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
) -> Result<(), String> {
    recovering.store(true, Ordering::Release);
    recovery_epoch.fetch_add(1, Ordering::AcqRel);
    let old_pid = resources.server_pid().map_err(|error| error.to_string())?;
    let started = Instant::now();
    record_event(
        hub,
        writer,
        unix_millis()?,
        kind.label(),
        &format!(
            "signal={} old_pid={old_pid}",
            kind.signal(config.database.engine)
        ),
    )?;
    let wall = unix_millis()?;
    let deadline = wall.saturating_add(millis(config.faults.recovery_timeout));
    progress.begin_operation(wall, kind.label(), Some(deadline))?;
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("append recovery start: {error}"))?;
    let _ = coordinator.shutdown();
    // SAFETY: the PID was resolved by exact canonical executable identity and
    // the agent runs under the same dedicated Unix identity as the soak DB.
    let signal_result =
        unsafe { libc::kill(old_pid as libc::pid_t, kind.signal(config.database.engine)) };
    if signal_result != 0 {
        return Err(format!(
            "{} signal failed: {}",
            kind.label(),
            std::io::Error::last_os_error()
        ));
    }
    resources.forget_process();
    let deadline = started + config.faults.recovery_timeout;
    let mut replacement = None;
    let mut new_pid = None;
    while Instant::now() < deadline {
        if let Ok(pid) = resources.server_pid() {
            if pid != old_pid {
                if let Ok(connection) = workload::connect(&config.database) {
                    replacement = Some(connection);
                    new_pid = Some(pid);
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    let Some(connection) = replacement else {
        return Err(format!(
            "{} did not recover within {} seconds",
            kind.label(),
            config.faults.recovery_timeout.as_secs()
        ));
    };
    *coordinator = connection;
    verify_server_identity(config)?;
    metrics.reopen();
    recovery_epoch.fetch_add(1, Ordering::AcqRel);
    recovering.store(false, Ordering::Release);
    progress.complete_operation(unix_millis()?)?;
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("append recovery completion: {error}"))?;
    record_event(
        hub,
        writer,
        unix_millis()?,
        "reopened",
        &format!(
            "kind={} old_pid={old_pid} new_pid={} elapsed_millis={}",
            kind.label(),
            new_pid.unwrap(),
            elapsed_millis(started)
        ),
    )?;
    check_and_publish_invariants(
        coordinator,
        config.load.active_rows,
        metrics,
        hub,
        writer,
        progress,
    )
}

fn verify_server_identity(config: &ResolvedSoakConfig) -> Result<String, String> {
    let executable = fs::canonicalize(&config.monitor.server_executable)
        .map_err(|error| format!("resolve server executable: {error}"))?;
    let output = Command::new(&executable)
        .arg("--version")
        .output()
        .map_err(|error| format!("run server identity: {error}"))?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err("server --version failed or wrote stderr".into());
    }
    let line = std::str::from_utf8(&output.stdout)
        .map_err(|_| "server identity is not UTF-8")?
        .trim();
    if config.database.engine == DatabaseEngine::Radixdb {
        for required in [
            format!("git={GIT_COMMIT}"),
            format!("protocol={}", radixdb_client::PROTOCOL_VERSION),
            format!("profile={BUILD_PROFILE}"),
            format!("target={BUILD_TARGET}"),
            format!("lock={CARGO_LOCK_SHA256}"),
        ] {
            if !line.split_ascii_whitespace().any(|field| field == required) {
                return Err(format!("server identity mismatch: missing `{required}`"));
            }
        }
        let mut connection = workload::connect(&config.database)?;
        let connection = connection
            .radixdb_mut()
            .ok_or_else(|| "expected RadixDB connection".to_string())?;
        let status = connection
            .server_status()
            .map_err(|error| format!("server status: {error}"))?;
        let identity = status
            .build
            .ok_or_else(|| "server status omitted build identity".to_string())?;
        if identity.git_revision != GIT_COMMIT
            || identity.protocol_version != radixdb_client::PROTOCOL_VERSION
            || identity.build_profile != BUILD_PROFILE
            || identity.target != BUILD_TARGET
        {
            return Err(format!(
                "negotiated server identity differs from soak binary: {identity:?}"
            ));
        }
        return Ok(line.into());
    }
    if !line.to_ascii_lowercase().contains("postgres") {
        return Err(format!(
            "PostgreSQL executable identity is unexpected: `{line}`"
        ));
    }
    let negotiated = workload::server_identity(&config.database)?;
    if !negotiated.to_ascii_lowercase().contains("postgres") {
        return Err(format!(
            "PostgreSQL negotiated identity is unexpected: `{negotiated}`"
        ));
    }
    Ok(format!("{line}; negotiated={negotiated}"))
}

fn require_release_identity() -> Result<(), String> {
    if GIT_COMMIT.ends_with("-dirty") {
        return Err("soak binary was built from a dirty source tree".into());
    }
    if BUILD_PROFILE != "release" {
        return Err(format!(
            "soak binary profile is `{BUILD_PROFILE}`, expected `release`"
        ));
    }
    Ok(())
}

fn update_phase(
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    progress: &mut SemanticProgressTracker,
    started: Instant,
    phase: &str,
    state: RunState,
) -> Result<(), String> {
    let wall = unix_millis()?;
    progress.advance_phase(wall, phase)?;
    writer
        .append_semantic_progress(progress.snapshot())
        .map_err(|error| format!("append semantic phase: {error}"))?;
    hub.update(|status| {
        status.state = state;
        status.phase = phase.into();
        status.updated_unix_millis = wall;
        status.elapsed_millis = elapsed_millis(started);
        status.last_progress_unix_millis = progress.snapshot().last_workload_progress_unix_millis;
    })?;
    record_event(hub, writer, wall, "phase", phase)
}

fn interrupt(
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    started: Instant,
) -> Result<RunState, String> {
    let wall = unix_millis()?;
    hub.update(|status| {
        status.state = RunState::Interrupted;
        status.phase = "interrupted".into();
        status.updated_unix_millis = wall;
        status.elapsed_millis = elapsed_millis(started);
        status.remaining_millis = 0;
        status.failure = Some("run interrupted by signal".into());
    })?;
    record_event(
        hub,
        writer,
        wall,
        "interrupted",
        "run interrupted by signal",
    )?;
    Ok(RunState::Interrupted)
}

fn record_event(
    hub: &StatusHub,
    writer: &mut ArtifactWriter,
    wall: u64,
    kind: &str,
    detail: &str,
) -> Result<(), String> {
    let event: RunEvent = hub.push_event(wall, kind, detail)?;
    writer
        .append_event(&event)
        .map_err(|error| format!("append event: {error}"))
}

fn seed_progress_detail(
    loaded_rows: u64,
    chunk_rows: u64,
    csv_nanos: u64,
    copy_nanos: u64,
    total_nanos: u64,
    engine: &workload::SeedEngineCounters,
) -> String {
    let chunk_elapsed_millis = total_nanos / 1_000_000;
    let chunk_rows_per_second = chunk_rows.saturating_mul(1_000_000_000) / total_nanos.max(1);
    format!(
        "loaded_rows={loaded_rows} chunk_rows={chunk_rows} \
         chunk_elapsed_millis={chunk_elapsed_millis} \
         chunk_rows_per_second={chunk_rows_per_second} \
         csv_millis={} copy_millis={} engine_copy_parse_millis={} \
         engine_copy_commit_millis={} cold_constraints_millis={} \
         cold_pk_millis={} wal_write_millis={} wal_sync_millis={} \
         wal_validation_millis={} wal_validation_mib={:.3} \
         wal_retention_millis={} wal_retention_identity_checks={} \
         wal_retention_files_deleted={} \
         seal_millis={} manifest_millis={} \
         compaction_millis={} runtime_wait_millis={} backpressure_millis={} \
         read_mib={:.3} wal_mib={:.3} \
         seal_rows={} seal_output_mib={:.3} cold_segments={} \
         storage_cpu_effective={} storage_cpu_in_use={} storage_cpu_peak={} \
         storage_cpu_reserved={} storage_cpu_reserved_peak={} \
         storage_cpu_leases={} storage_cpu_parallel_leases={}",
        csv_nanos / 1_000_000,
        copy_nanos / 1_000_000,
        engine.copy_parse_nanos / 1_000_000,
        engine.copy_commit_nanos / 1_000_000,
        engine.cold_constraint_nanos / 1_000_000,
        engine.cold_pk_nanos / 1_000_000,
        engine.wal_write_nanos / 1_000_000,
        engine.wal_sync_nanos / 1_000_000,
        engine.wal_generation_validation_nanos / 1_000_000,
        engine.wal_generation_validation_bytes as f64 / 1_048_576.0,
        engine.wal_retention_nanos / 1_000_000,
        engine.wal_retention_identity_checks,
        engine.wal_retention_files_deleted,
        engine.seal_nanos / 1_000_000,
        engine.manifest_publication_nanos / 1_000_000,
        engine.compaction_nanos / 1_000_000,
        engine.runtime_wait_nanos / 1_000_000,
        engine.backpressure_wait_millis,
        engine.volume_read_bytes as f64 / 1_048_576.0,
        engine.wal_write_bytes as f64 / 1_048_576.0,
        engine.seal_rows,
        engine.seal_output_bytes as f64 / 1_048_576.0,
        engine.cold_segments,
        engine.storage_cpu_workers_effective,
        engine.storage_cpu_workers_in_use,
        engine.storage_cpu_peak_workers_in_use,
        engine.storage_cpu_workers_reserved,
        engine.storage_cpu_peak_workers_reserved,
        engine.storage_cpu_leases,
        engine.storage_cpu_parallel_leases,
    )
}

fn unix_millis() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis();
    u64::try_from(millis).map_err(|_| "Unix timestamp overflow".into())
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn markdown_report(report: &FinalReport) -> String {
    format!(
        concat!(
            "# Database remote soak report\n\n",
            "- Run: `{}`\n",
            "- Mode: `{}`\n",
            "- Profile: `{}`\n",
            "- Engine: `{}`\n",
            "- State: `{:?}`\n",
            "- Elapsed: `{}` ms\n",
            "- Commits: `{}`\n",
            "- Rollbacks: `{}`\n",
            "- Conflicts: `{}`\n",
            "- Invariant failures: `{}`\n",
            "- Observer telemetry drops: `{}`\n",
            "- Logical digest: `{}`\n",
            "- Failure: `{}`\n"
        ),
        report.run_id,
        report.mode.as_str(),
        report.profile,
        report.database_engine.as_str(),
        report.state,
        report.elapsed_millis,
        report.counters.transactions_committed,
        report.counters.transactions_rolled_back,
        report.counters.conflicts,
        report.counters.invariant_failures,
        report.telemetry_dropped,
        report.logical_digest.as_deref().unwrap_or("not-produced"),
        report.failure.as_deref().unwrap_or("none"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_guard_rejects_development_build() {
        if BUILD_PROFILE != "release" {
            assert!(require_release_identity().is_err());
        }
    }

    #[test]
    fn graceful_reopen_uses_each_server_safe_shutdown_signal() {
        assert_eq!(
            FaultKind::Graceful.signal(DatabaseEngine::Radixdb),
            libc::SIGTERM
        );
        assert_eq!(
            FaultKind::Graceful.signal(DatabaseEngine::Postgresql),
            libc::SIGINT
        );
        assert_eq!(
            FaultKind::Kill.signal(DatabaseEngine::Postgresql),
            libc::SIGKILL
        );
    }

    #[test]
    fn seed_progress_event_carries_per_chunk_cost() {
        let engine = workload::SeedEngineCounters {
            copy_parse_nanos: 7_000_000,
            copy_commit_nanos: 11_000_000,
            cold_constraint_nanos: 3_000_000,
            cold_pk_nanos: 1_000_000,
            cold_segments: 4,
            ..Default::default()
        };
        let detail = seed_progress_detail(
            500_000,
            250_000,
            2_000_000_000,
            10_000_000_000,
            12_500_000_000,
            &engine,
        );
        assert!(detail.contains("loaded_rows=500000 chunk_rows=250000"));
        assert!(detail.contains("chunk_elapsed_millis=12500"));
        assert!(detail.contains("chunk_rows_per_second=20000"));
        assert!(detail.contains("csv_millis=2000 copy_millis=10000"));
        assert!(detail.contains("engine_copy_parse_millis=7"));
        assert!(detail.contains("engine_copy_commit_millis=11"));
        assert!(detail.contains("cold_constraints_millis=3 cold_pk_millis=1"));
        assert!(detail.contains("cold_segments=4"));
    }

    #[test]
    fn run_modes_have_stable_artifact_spelling() {
        assert_eq!(RunMode::Full.as_str(), "full");
        assert_eq!(RunMode::SeedOnly.as_str(), "seed-only");
        assert_eq!(serde_json::to_string(&RunMode::Full).unwrap(), "\"full\"");
        assert_eq!(
            serde_json::to_string(&RunMode::SeedOnly).unwrap(),
            "\"seed-only\""
        );
    }

    #[test]
    fn manifest_and_report_never_contain_authentication_values() {
        let names = std::any::type_name::<Manifest>();
        assert!(!names.contains("password"));
        let report = FinalReport {
            format: 1,
            run_id: "run".into(),
            mode: RunMode::SeedOnly,
            profile: "smoke".into(),
            database_engine: DatabaseEngine::Radixdb,
            state: RunState::Passed,
            elapsed_millis: 1,
            counters: Counters::default(),
            latency: LatencySnapshot::default(),
            resources: ResourceSnapshot::default(),
            resource_slopes: ResourceSlopes::default(),
            server_runtime: ServerRuntimeSnapshot::default(),
            invariants: BTreeMap::new(),
            logical_digest: None,
            failure: None,
            telemetry_dropped: 0,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("password"));
        assert!(!json.contains("authorization"));
    }

    #[test]
    fn agent_survives_the_database_restart_it_intentionally_triggers() {
        let unit = include_str!("../deploy/radixdb-soak-agent.service");
        let wants = unit
            .lines()
            .find_map(|line| line.strip_prefix("Wants="))
            .unwrap();
        assert!(wants
            .split_ascii_whitespace()
            .any(|unit| unit == "radixdb-soak-db.service"));
        assert!(wants
            .split_ascii_whitespace()
            .any(|unit| unit == "radixdb-soak-observer.service"));
        assert!(!unit
            .lines()
            .any(|line| line == "Requires=radixdb-soak-db.service"));
    }
}
