use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use signal_hook::consts::{SIGINT, SIGTERM};

use super::incident::{ArtifactBudget, IncidentEvidence};
use crate::diagnostics::http::{ObserverHttpHub, ObserverHttpServer};
use crate::{
    auth::BasicAuth,
    config::ResolvedSoakConfig,
    diagnostics::{
        boot_id, collect_disk, collect_host, collect_processes_with_depth, collect_smart,
        smart_degradation, AgentTelemetryMessage, AgentTelemetryReceiver, DetectorConfig,
        DiagnosticAlertV2, DiagnosticDetector, DiagnosticMetricFrameV2, DiskSampleV2,
        EngineSampleV2, EngineSnapshotWorker, HeartbeatSnapshot, HostSampleV2, IncidentRecorder,
        KernelHealthSnapshotV2, KernelHealthWorker, ProcessCollectionDepth, ProcessSampleV2,
        RateDeriver, SmartSnapshotV2, DIAGNOSTIC_FORMAT_V2,
    },
};

const RUN_DIRECTORY_WAIT: Duration = Duration::from_secs(60);
const TERMINAL_LINGER: Duration = Duration::from_secs(10);
const KERNEL_HEALTH_INTERVAL: Duration = Duration::from_secs(30);
const KERNEL_HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const KERNEL_HEALTH_OVERLAP_MILLIS: u64 = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserverStatus {
    format: u32,
    run_id: String,
    observer_unix_millis: u64,
    agent_unix_millis: Option<u64>,
    server_unix_millis: Option<u64>,
    samples: u64,
    telemetry_sequence: Option<u64>,
    telemetry_gaps: u64,
    active_alerts: Vec<DiagnosticAlertV2>,
    active_incidents: usize,
    suppressed_incidents: u64,
    retention_dropped_records: u64,
    retention_saturated: bool,
    sampling_mode: String,
    disk_degradation_evidence: Vec<String>,
    disk_degradation_collection_errors: Vec<String>,
    last_error: Option<String>,
    terminal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HardwareInventory {
    format: u32,
    boot_id: String,
    kernel_release: String,
    cpu_model: Option<String>,
    logical_cpus: usize,
    memory_total_kib: Option<u64>,
    data_directory: String,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(deny_unknown_fields)]
struct ObserverResumeState {
    resumed: bool,
    truncated_tail_bytes: u64,
    previous_boot_id: Option<String>,
    boot_identity_changed: bool,
    previous_frame: Option<DiagnosticMetricFrameV2>,
    previous_telemetry: Option<AgentTelemetryMessage>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticReport {
    format: u32,
    run_id: String,
    terminal: bool,
    samples: u64,
    telemetry_gaps: u64,
    active_alerts: Vec<DiagnosticAlertV2>,
    incidents: Vec<crate::diagnostics::DiagnosticIncidentV2>,
    suppressed_incidents: u64,
    retention_dropped_records: u64,
    retention_saturated: bool,
    disk_degradation_evidence: Vec<String>,
    disk_degradation_collection_errors: Vec<String>,
    last_error: Option<String>,
}

#[derive(Default)]
struct ProgressRateBaseline {
    samples: u64,
    mean: f64,
}

impl ProgressRateBaseline {
    fn observe(&mut self, rate: f64) {
        if !rate.is_finite() || rate <= 0.0 {
            return;
        }
        self.samples = self.samples.saturating_add(1);
        let weight = 1.0 / self.samples.min(1_024) as f64;
        self.mean += (rate - self.mean) * weight;
    }

    fn expected(&self) -> Option<f64> {
        (self.samples >= 3).then_some(self.mean)
    }
}

struct ObserverArtifacts {
    run_dir: PathBuf,
    budget: Arc<ArtifactBudget>,
    frames: BufWriter<File>,
    host: BufWriter<File>,
    process: BufWriter<File>,
    disk: BufWriter<File>,
    engine: BufWriter<File>,
    smart: BufWriter<File>,
    kernel: BufWriter<File>,
    alerts: BufWriter<File>,
    telemetry: BufWriter<File>,
    resume: ObserverResumeState,
    _lock: File,
}

impl Drop for ObserverArtifacts {
    fn drop(&mut self) {
        // Closing the final descriptor also releases flock(2), but make the
        // ownership transition explicit before any surrounding test/runtime
        // can attempt an immediate resume. This also releases the shared open
        // file description if a diagnostic child temporarily inherited a
        // duplicate descriptor before exec closed it.
        // SAFETY: `_lock` remains a live descriptor until this Drop returns.
        let _ = unsafe { libc::flock(self._lock.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl ObserverArtifacts {
    fn open(run_dir: &Path, quota_bytes: u64) -> io::Result<Self> {
        if !run_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "agent run directory is not ready",
            ));
        }
        if run_dir.join("SHA256SUMS").exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "completed diagnostic evidence cannot be resumed",
            ));
        }
        let observer_lock = acquire_observer_lock(run_dir)?;
        fs::create_dir_all(run_dir.join("incidents"))?;
        let names = [
            "diagnostic-frames.jsonl",
            "host-samples.jsonl",
            "process-samples.jsonl",
            "disk-samples.jsonl",
            "engine-samples.jsonl",
            "smart-samples.jsonl",
            "kernel-health.jsonl",
            "alerts.jsonl",
            "agent-telemetry.jsonl",
        ];
        let resumed = names.iter().any(|name| run_dir.join(name).exists());
        let mut truncated_tail_bytes = 0_u64;
        for name in names {
            truncated_tail_bytes =
                truncated_tail_bytes.saturating_add(recover_jsonl_tail(&run_dir.join(name))?);
        }
        let previous_frame = read_last_json_line(&run_dir.join("diagnostic-frames.jsonl"))?;
        let previous_telemetry = read_last_json_line(&run_dir.join("agent-telemetry.jsonl"))?;
        let previous_hardware =
            read_optional_json::<HardwareInventory>(&run_dir.join("hardware.json"))?;
        let bytes_written = directory_bytes(run_dir)?;
        let budget = Arc::new(
            ArtifactBudget::new(quota_bytes, bytes_written).map_err(artifact_budget_error)?,
        );
        Ok(Self {
            run_dir: run_dir.to_path_buf(),
            budget,
            frames: BufWriter::new(open_append(run_dir, "diagnostic-frames.jsonl")?),
            host: BufWriter::new(open_append(run_dir, "host-samples.jsonl")?),
            process: BufWriter::new(open_append(run_dir, "process-samples.jsonl")?),
            disk: BufWriter::new(open_append(run_dir, "disk-samples.jsonl")?),
            engine: BufWriter::new(open_append(run_dir, "engine-samples.jsonl")?),
            smart: BufWriter::new(open_append(run_dir, "smart-samples.jsonl")?),
            kernel: BufWriter::new(open_append(run_dir, "kernel-health.jsonl")?),
            alerts: BufWriter::new(open_append(run_dir, "alerts.jsonl")?),
            telemetry: BufWriter::new(open_append(run_dir, "agent-telemetry.jsonl")?),
            resume: ObserverResumeState {
                resumed,
                truncated_tail_bytes,
                previous_boot_id: previous_hardware.map(|inventory| inventory.boot_id),
                boot_identity_changed: false,
                previous_frame,
                previous_telemetry,
            },
            _lock: observer_lock,
        })
    }

    fn resume_state(&self) -> &ObserverResumeState {
        &self.resume
    }

    fn budget(&self) -> Arc<ArtifactBudget> {
        Arc::clone(&self.budget)
    }

    fn set_boot_identity(&mut self, current_boot_id: &str) {
        self.resume.boot_identity_changed = self
            .resume
            .previous_boot_id
            .as_deref()
            .is_some_and(|previous| previous != current_boot_id);
    }

    fn publish_resume(&mut self) -> io::Result<()> {
        if self.resume.resumed {
            let payload = serde_json::to_vec(&self.resume)?;
            self.reserve(payload.len().saturating_add(1))?;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.run_dir.join("observer-resumes.jsonl"))?;
            file.write_all(&payload)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
        }
        Ok(())
    }

    fn write_inventory(&mut self, inventory: &HardwareInventory) -> io::Result<()> {
        let payload = serde_json::to_vec_pretty(inventory)?;
        self.reserve(payload.len().saturating_add(1))?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.run_dir.join("hardware.json"))?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()
    }

    fn write_named_json<T: Serialize>(&mut self, name: &str, value: &T) -> io::Result<()> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid observer artifact name",
            ));
        }
        let payload = serde_json::to_vec_pretty(value)?;
        self.reserve(payload.len().saturating_add(1))?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.run_dir.join(name))?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()
    }

    fn write_named_bytes(&mut self, name: &str, value: &[u8]) -> io::Result<()> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid observer artifact name",
            ));
        }
        self.reserve(value.len())?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.run_dir.join(name))?;
        file.write_all(value)?;
        file.sync_all()
    }

    fn append_telemetry(&mut self, value: &AgentTelemetryMessage) -> io::Result<bool> {
        append_retained(&mut self.telemetry, &self.budget, value)
    }

    fn append_engine(&mut self, value: &EngineSampleV2) -> io::Result<bool> {
        let retained = append_retained(&mut self.engine, &self.budget, value)?;
        if retained {
            self.engine.flush()?;
        }
        Ok(retained)
    }

    fn append_smart(&mut self, value: &SmartSnapshotV2) -> io::Result<bool> {
        let retained = append_retained(&mut self.smart, &self.budget, value)?;
        if retained {
            self.smart.flush()?;
        }
        Ok(retained)
    }

    fn append_kernel(&mut self, value: &KernelHealthSnapshotV2) -> io::Result<bool> {
        let retained = append_retained(&mut self.kernel, &self.budget, value)?;
        if retained {
            self.kernel.flush()?;
        }
        Ok(retained)
    }

    fn append_sample(
        &mut self,
        frame: &DiagnosticMetricFrameV2,
        host: &HostSampleV2,
        process: &ProcessSampleV2,
        disk: &DiskSampleV2,
    ) -> io::Result<bool> {
        let payloads = [
            encode_json_line(frame)?,
            encode_json_line(host)?,
            encode_json_line(process)?,
            encode_json_line(disk)?,
        ];
        let bytes = payloads
            .iter()
            .try_fold(0_usize, |total, payload| total.checked_add(payload.len()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "sample size overflow"))?;
        if !self
            .budget
            .reserve_retained(bytes)
            .map_err(artifact_budget_error)?
        {
            return Ok(false);
        }
        for (writer, payload) in [
            (&mut self.frames, &payloads[0]),
            (&mut self.host, &payloads[1]),
            (&mut self.process, &payloads[2]),
            (&mut self.disk, &payloads[3]),
        ] {
            writer.write_all(payload)?;
        }
        self.flush()?;
        Ok(true)
    }

    fn append_alerts(&mut self, alerts: &[DiagnosticAlertV2]) -> io::Result<()> {
        for alert in alerts {
            append_bounded(&mut self.alerts, &self.budget, alert)?;
        }
        self.alerts.flush()
    }

    fn publish_status(&mut self, status: &ObserverStatus) -> io::Result<()> {
        atomic_json(
            &self.run_dir.join("observer-status.json"),
            status,
            &self.budget,
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        self.frames.flush()?;
        self.host.flush()?;
        self.process.flush()?;
        self.disk.flush()?;
        self.engine.flush()?;
        self.smart.flush()?;
        self.kernel.flush()?;
        self.telemetry.flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        for file in [
            self.frames.get_ref(),
            self.host.get_ref(),
            self.process.get_ref(),
            self.disk.get_ref(),
            self.engine.get_ref(),
            self.smart.get_ref(),
            self.kernel.get_ref(),
            self.alerts.get_ref(),
            self.telemetry.get_ref(),
        ] {
            file.sync_data()?;
        }
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> io::Result<()> {
        self.budget.reserve(bytes).map_err(artifact_budget_error)
    }
}

pub fn run(
    config: ResolvedSoakConfig,
    run_id: &str,
    max_samples: Option<u64>,
) -> Result<(), String> {
    if !config.diagnostics.enabled {
        return Err("diagnostics.enabled must be true for observer".into());
    }
    validate_run_id(run_id)?;
    let history_capacity = history_capacity(
        config.diagnostics.history_window,
        config.diagnostics.normal_interval,
        config.diagnostics.history_max_samples,
    );
    let receiver =
        AgentTelemetryReceiver::bind_replacing_stale(&config.diagnostics.channel_path, run_id)
            .map_err(|error| format!("bind agent telemetry: {error}"))?;
    let http_hub = ObserverHttpHub::with_timeline_capacity(history_capacity);
    let http_server = ObserverHttpServer::start(
        config.diagnostics.bind,
        BasicAuth::load(&config.diagnostics.auth_file).map_err(|error| error.to_string())?,
        http_hub.clone(),
    )
    .map_err(|error| format!("start observer HTTP: {error}"))?;
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGINT, Arc::clone(&stop)).map_err(|error| error.to_string())?;
    signal_hook::flag::register(SIGTERM, Arc::clone(&stop)).map_err(|error| error.to_string())?;
    let run_dir = config.artifacts.root.join(run_id);
    wait_for_run_directory(&run_dir, &stop)?;
    let mut artifacts = ObserverArtifacts::open(&run_dir, config.diagnostics.artifact_quota_bytes)
        .map_err(|error| format!("open observer artifacts: {error}"))?;
    let boot = boot_id().map_err(|error| format!("read boot identity: {error}"))?;
    artifacts.set_boot_identity(&boot);
    let resume_state = artifacts.resume_state().clone();
    if resume_state.previous_boot_id.is_none() {
        artifacts
            .write_inventory(&hardware_inventory(&boot, &config.monitor.data_dir))
            .map_err(|error| format!("write hardware inventory: {error}"))?;
    }
    artifacts
        .publish_resume()
        .map_err(|error| format!("publish observer resume: {error}"))?;
    let smart_preflight_path = run_dir.join("smart-preflight.json");
    let smart_preflight = if let Some(snapshot) =
        read_optional_json::<SmartSnapshotV2>(&smart_preflight_path)
            .map_err(|error| format!("read SMART preflight: {error}"))?
    {
        snapshot
    } else {
        let snapshot = collect_smart(unix_millis()?, &config.monitor.data_dir);
        artifacts
            .write_named_json("smart-preflight.json", &snapshot)
            .map_err(|error| format!("write SMART preflight: {error}"))?;
        snapshot
    };
    let mut latest_smart_snapshot = smart_preflight.clone();
    let observer_started = Instant::now();
    let agent_executable = std::env::current_exe()
        .map_err(|error| error.to_string())?
        .parent()
        .ok_or_else(|| "observer executable has no parent".to_string())?
        .join("radixdb-soak");
    let expected_processes = [
        ("server", config.monitor.server_executable.as_path()),
        ("agent", agent_executable.as_path()),
    ];
    let mut detector = DiagnosticDetector::new(DetectorConfig {
        workload_stall_millis: duration_millis(config.diagnostics.workload_stall_timeout),
        agent_heartbeat_timeout_millis: duration_millis(config.diagnostics.agent_heartbeat_timeout),
    })?;
    let mut frames = restore_frame_history(
        &run_dir.join("diagnostic-frames.jsonl"),
        history_capacity,
        &mut detector,
    )?;
    http_hub.seed_timeline(frames.iter().cloned());
    let mut incident_recorder = IncidentRecorder::new_with_budget(
        &run_dir,
        config.diagnostics.max_incidents,
        artifacts.budget(),
    )?;
    let mut rate_deriver = RateDeriver::default();
    if let Some(previous) = frames.back() {
        rate_deriver.observe(previous.monotonic_millis, &previous.counters)?;
    }
    let mut engine_worker = Some(EngineSnapshotWorker::start(
        config.database.clone(),
        config.diagnostics.engine_snapshot_timeout,
    )?);
    let mut kernel_worker = Some(KernelHealthWorker::start(KERNEL_HEALTH_TIMEOUT)?);
    let mut latest_engine_sample: Option<EngineSampleV2> = None;
    let mut next_engine_snapshot = Instant::now();
    let mut next_smart_snapshot = Instant::now() + config.diagnostics.smart_interval;
    let mut next_kernel_snapshot = Instant::now() + KERNEL_HEALTH_INTERVAL;
    let mut last_kernel_request_unix_millis = unix_millis()?;
    let mut next_full_process_snapshot = Instant::now();
    let mut latest_full_process_sample: Option<ProcessSampleV2> = None;
    let mut smart_degradation_evidence = Vec::new();
    let mut kernel_degradation_evidence = Vec::new();
    let mut filesystem_degradation_evidence = Vec::new();
    let mut disk_degradation_evidence = Vec::new();
    let mut disk_degradation_collection_errors = Vec::new();
    let mut engine_snapshot_skips = 0_u64;
    let mut last_telemetry = resume_state.previous_telemetry;
    let mut last_telemetry_sequence = last_telemetry.as_ref().map(|value| value.sequence);
    let mut telemetry_gaps = 0u64;
    let mut progress_rate_baselines = BTreeMap::<String, ProgressRateBaseline>::new();
    let mut last_progress_rate_phase: Option<String> = None;
    let mut sample_sequence = resume_state
        .previous_frame
        .as_ref()
        .map_or(0, |frame| frame.sequence);
    let first_sequence_after_resume = sample_sequence.saturating_add(1);
    let monotonic_offset = resume_state.previous_frame.as_ref().map_or(0, |frame| {
        frame.monotonic_millis.saturating_add(
            config
                .diagnostics
                .normal_interval
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        )
    });
    let mut terminal_since = last_telemetry
        .as_ref()
        .is_some_and(|message| is_terminal_phase(&message.progress.phase))
        .then(Instant::now);
    let mut next_sample = Instant::now();
    let mut status = ObserverStatus {
        format: DIAGNOSTIC_FORMAT_V2,
        run_id: run_id.into(),
        observer_unix_millis: unix_millis()?,
        agent_unix_millis: None,
        server_unix_millis: None,
        samples: 0,
        telemetry_sequence: None,
        telemetry_gaps: 0,
        active_alerts: Vec::new(),
        active_incidents: 0,
        suppressed_incidents: 0,
        retention_dropped_records: 0,
        retention_saturated: false,
        sampling_mode: "normal".into(),
        disk_degradation_evidence: Vec::new(),
        disk_degradation_collection_errors: Vec::new(),
        last_error: None,
        terminal: false,
    };
    let mut retention_dropped_records = 0_u64;

    while !stop.load(Ordering::Acquire) {
        loop {
            match receiver.receive() {
                Ok(Some(message)) => {
                    if last_telemetry_sequence
                        .is_some_and(|previous| message.sequence != previous + 1)
                    {
                        telemetry_gaps = telemetry_gaps.saturating_add(1);
                    }
                    last_telemetry_sequence = Some(message.sequence);
                    let retained = artifacts
                        .append_telemetry(&message)
                        .map_err(|error| format!("append agent telemetry: {error}"))?;
                    account_retention_drop(&mut retention_dropped_records, retained, 1);
                    if is_terminal_phase(&message.progress.phase) {
                        terminal_since.get_or_insert_with(Instant::now);
                    }
                    last_telemetry = Some(message);
                }
                Ok(None) => break,
                Err(error) => {
                    status.last_error = Some(format!("receive agent telemetry: {error}"));
                    break;
                }
            }
        }

        let mut disable_engine_worker = false;
        if let Some(worker) = engine_worker.as_mut() {
            match worker.poll() {
                Ok(Some(sample)) => {
                    let retained = artifacts
                        .append_engine(&sample)
                        .map_err(|error| format!("append engine sample: {error}"))?;
                    account_retention_drop(&mut retention_dropped_records, retained, 1);
                    latest_engine_sample = Some(sample);
                }
                Ok(None) => {}
                Err(error) => {
                    status.last_error = Some(error);
                    disable_engine_worker = true;
                }
            }
        }
        if disable_engine_worker {
            engine_worker.take();
        }

        let mut disable_kernel_worker = false;
        if let Some(worker) = kernel_worker.as_ref() {
            match worker.poll() {
                Ok(Some(snapshot)) => {
                    let retained = artifacts
                        .append_kernel(&snapshot)
                        .map_err(|error| format!("append kernel health sample: {error}"))?;
                    account_retention_drop(&mut retention_dropped_records, retained, 1);
                    if let Some(error) = snapshot.error {
                        disk_degradation_collection_errors =
                            vec![format!("kernel journal: {error}")];
                    } else {
                        disk_degradation_collection_errors.clear();
                        merge_bounded_evidence(&mut kernel_degradation_evidence, snapshot.evidence);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    disk_degradation_collection_errors = vec![error];
                    disable_kernel_worker = true;
                }
            }
        }
        if disable_kernel_worker {
            if let Some(worker) = kernel_worker.take() {
                let _ = worker.shutdown();
            }
        }

        let now = Instant::now();
        if now >= next_sample {
            sample_sequence = sample_sequence
                .checked_add(1)
                .ok_or_else(|| "observer sample sequence exhausted".to_string())?;
            let wall = unix_millis()?;
            let monotonic = monotonic_offset.saturating_add(elapsed_millis(observer_started));
            let host = collect_host(sample_sequence, monotonic, wall, &boot);
            let full_process_snapshot = now >= next_full_process_snapshot;
            let mut process = collect_processes_with_depth(
                sample_sequence,
                monotonic,
                wall,
                &expected_processes,
                if full_process_snapshot {
                    ProcessCollectionDepth::Full
                } else {
                    ProcessCollectionDepth::Light
                },
            );
            if full_process_snapshot {
                latest_full_process_sample = Some(process.clone());
                next_full_process_snapshot = now
                    + config
                        .diagnostics
                        .normal_interval
                        .max(Duration::from_secs(5));
            } else if let Some(full) = &latest_full_process_sample {
                inherit_heavy_process_fields(&mut process, full);
            }
            let disk = collect_disk(sample_sequence, monotonic, wall, &config.monitor.data_dir);
            if now >= next_smart_snapshot {
                let smart = collect_smart(wall, &config.monitor.data_dir);
                let retained = artifacts
                    .append_smart(&smart)
                    .map_err(|error| format!("append SMART sample: {error}"))?;
                account_retention_drop(&mut retention_dropped_records, retained, 1);
                smart_degradation_evidence = smart_degradation(&smart_preflight, &smart);
                latest_smart_snapshot = smart;
                next_smart_snapshot = now + config.diagnostics.smart_interval;
            }
            if now >= next_kernel_snapshot {
                let mut disable_kernel_worker = false;
                if let Some(worker) = kernel_worker.as_ref() {
                    match worker.try_request(
                        last_kernel_request_unix_millis
                            .saturating_sub(KERNEL_HEALTH_OVERLAP_MILLIS),
                        wall,
                    ) {
                        Ok(true) => {
                            last_kernel_request_unix_millis = wall;
                            next_kernel_snapshot = now + KERNEL_HEALTH_INTERVAL;
                        }
                        Ok(false) => {
                            next_kernel_snapshot = now + Duration::from_secs(1);
                        }
                        Err(error) => {
                            disk_degradation_collection_errors = vec![error];
                            disable_kernel_worker = true;
                        }
                    }
                }
                if disable_kernel_worker {
                    if let Some(worker) = kernel_worker.take() {
                        let _ = worker.shutdown();
                    }
                }
            }
            if disk.filesystem_read_only {
                merge_bounded_evidence(
                    &mut filesystem_degradation_evidence,
                    vec!["database filesystem is mounted read-only".into()],
                );
            }
            disk_degradation_evidence.clear();
            merge_bounded_evidence(
                &mut disk_degradation_evidence,
                smart_degradation_evidence.clone(),
            );
            merge_bounded_evidence(
                &mut disk_degradation_evidence,
                kernel_degradation_evidence.clone(),
            );
            merge_bounded_evidence(
                &mut disk_degradation_evidence,
                filesystem_degradation_evidence.clone(),
            );
            let server_alive = process.roles.contains_key("server");
            if now >= next_engine_snapshot {
                let mut disable_engine_worker = false;
                if let Some(worker) = engine_worker.as_mut() {
                    match worker.try_request(sample_sequence, monotonic, wall) {
                        Ok(true) => {
                            next_engine_snapshot =
                                now + config.diagnostics.engine_snapshot_interval;
                        }
                        Ok(false) => {
                            engine_snapshot_skips = engine_snapshot_skips.saturating_add(1);
                        }
                        Err(error) => {
                            status.last_error = Some(error);
                            disable_engine_worker = true;
                        }
                    }
                }
                if disable_engine_worker {
                    engine_worker.take();
                }
            }
            let mut latest_frame = None;
            let active_alerts = if let Some(telemetry) = &last_telemetry {
                let mut frame = build_frame(
                    sample_sequence,
                    monotonic,
                    wall,
                    &boot,
                    run_id,
                    telemetry,
                    server_alive,
                    &host,
                    &process,
                    &disk,
                    latest_engine_sample.as_ref(),
                );
                let sampling_gap_millis = frames
                    .back()
                    .map_or(0, |previous| wall.saturating_sub(previous.unix_millis));
                let sampling_gap_limit = duration_millis(config.diagnostics.normal_interval)
                    .saturating_mul(3)
                    .max(1_000);
                frame.gauges.insert(
                    "observer.sampling_gap_exceeded".into(),
                    f64::from(sampling_gap_millis > sampling_gap_limit),
                );
                frame.gauges.insert(
                    "observer.sampling_gap_millis".into(),
                    sampling_gap_millis as f64,
                );
                frame.gauges.insert(
                    "observer.resumed_after_interruption".into(),
                    f64::from(
                        resume_state.resumed && sample_sequence == first_sequence_after_resume,
                    ),
                );
                frame.counters.insert(
                    "observer.engine_snapshot_skips".into(),
                    saturating_i64(engine_snapshot_skips),
                );
                frame.gauges.insert(
                    "host.disk_degradation".into(),
                    f64::from(!disk_degradation_evidence.is_empty()),
                );
                frame.gauges.insert(
                    "observer.boot_identity_changed".into(),
                    f64::from(resume_state.boot_identity_changed),
                );
                frame.gauges.insert(
                    "observer.artifact_bytes".into(),
                    artifacts.budget.used() as f64,
                );
                frame.gauges.insert(
                    "observer.artifact_quota_bytes".into(),
                    artifacts.budget.limit() as f64,
                );
                frame.gauges.insert(
                    "observer.artifact_retained_limit_bytes".into(),
                    artifacts.budget.retained_limit() as f64,
                );
                frame.counters.insert(
                    "observer.retention_dropped_records".into(),
                    saturating_i64(retention_dropped_records),
                );
                frame.gauges.insert(
                    "observer.retention_saturated".into(),
                    f64::from(retention_dropped_records > 0),
                );
                let rates = rate_deriver.observe(frame.monotonic_millis, &frame.counters)?;
                let same_rate_phase =
                    last_progress_rate_phase.as_deref() == Some(frame.progress.phase.as_str());
                let actual_progress_rate = same_rate_phase
                    .then(|| rates.per_second.get("agent.workload_units").copied())
                    .flatten()
                    .unwrap_or(0.0);
                last_progress_rate_phase = Some(frame.progress.phase.clone());
                for (name, rate) in rates.per_second {
                    frame.gauges.insert(format!("rate.{name}_per_second"), rate);
                }
                frame.gauges.insert(
                    "progress.actual_units_per_second".into(),
                    actual_progress_rate,
                );
                if progress_rate_baselines.len() < 128
                    || progress_rate_baselines.contains_key(&frame.progress.phase)
                {
                    let baseline = progress_rate_baselines
                        .entry(frame.progress.phase.clone())
                        .or_default();
                    let expected = baseline.expected();
                    baseline.observe(actual_progress_rate);
                    if let Some(expected) = expected.or_else(|| baseline.expected()) {
                        frame
                            .gauges
                            .insert("progress.expected_units_per_second".into(), expected);
                        frame.gauges.insert(
                            "progress.rate_ratio".into(),
                            actual_progress_rate / expected.max(f64::EPSILON),
                        );
                    }
                }
                frame.counters.insert(
                    "observer.counter_resets".into(),
                    saturating_i64(u64::try_from(rates.resets.len()).unwrap_or(u64::MAX)),
                );
                frame.validate()?;
                if frames.len() == history_capacity {
                    frames.pop_front();
                }
                frames.push_back(frame.clone());
                let transitions = detector.observe(&frame)?;
                let alerts = detector.active_alerts();
                let pre_trigger = if transitions
                    .iter()
                    .any(|alert| alert.state == crate::diagnostics::DiagnosticState::Active)
                {
                    frames.iter().cloned().collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                incident_recorder.record(
                    &transitions,
                    &pre_trigger,
                    IncidentEvidence {
                        host: &host,
                        process: &process,
                        disk: &disk,
                        engine: latest_engine_sample.as_ref(),
                        smart: Some(&latest_smart_snapshot),
                    },
                )?;
                if incident_recorder
                    .burst_active(wall, duration_millis(config.diagnostics.burst_duration))
                {
                    let dropped = incident_recorder.append_burst(
                        &frame,
                        &host,
                        &process,
                        &disk,
                        latest_engine_sample.as_ref(),
                    )?;
                    retention_dropped_records = retention_dropped_records.saturating_add(dropped);
                }
                let retained = artifacts
                    .append_sample(&frame, &host, &process, &disk)
                    .map_err(|error| format!("append diagnostic sample: {error}"))?;
                account_retention_drop(&mut retention_dropped_records, retained, 4);
                artifacts
                    .append_alerts(&transitions)
                    .map_err(|error| format!("append diagnostic alert: {error}"))?;
                latest_frame = Some(frame);
                alerts
            } else {
                Vec::new()
            };
            status.observer_unix_millis = wall;
            status.agent_unix_millis = last_telemetry.as_ref().map(|value| value.agent_unix_millis);
            status.server_unix_millis = server_alive.then_some(wall);
            status.samples = sample_sequence;
            status.telemetry_sequence = last_telemetry_sequence;
            status.telemetry_gaps = telemetry_gaps;
            status.active_alerts = active_alerts;
            status.active_incidents = incident_recorder.active_incident_count();
            status.suppressed_incidents = incident_recorder.suppressed_count();
            status.retention_dropped_records = retention_dropped_records;
            status.retention_saturated = retention_dropped_records > 0;
            status.terminal = terminal_since.is_some();
            let sampling_interval = if incident_recorder.burst_active(
                wall,
                config
                    .diagnostics
                    .burst_duration
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ) {
                status.sampling_mode = "burst".into();
                config.diagnostics.burst_interval
            } else if incident_recorder.has_active_incident() {
                status.sampling_mode = "alert".into();
                config.diagnostics.alert_interval
            } else {
                status.sampling_mode = "normal".into();
                config.diagnostics.normal_interval
            };
            status.disk_degradation_evidence = disk_degradation_evidence.clone();
            status.disk_degradation_collection_errors = disk_degradation_collection_errors.clone();
            artifacts
                .publish_status(&status)
                .map_err(|error| format!("publish observer status: {error}"))?;
            http_hub.publish(
                &status,
                latest_frame.as_ref(),
                &status.active_alerts,
                incident_recorder.incidents(),
            )?;
            next_sample = now + sampling_interval;
        }

        if max_samples.is_some_and(|limit| sample_sequence >= limit)
            || terminal_since.is_some_and(|at| at.elapsed() >= TERMINAL_LINGER)
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let smart_final = collect_smart(unix_millis()?, &config.monitor.data_dir);
    let final_degradation = smart_degradation(&smart_preflight, &smart_final);
    if !final_degradation.is_empty() {
        smart_degradation_evidence = final_degradation;
    }
    artifacts
        .write_named_json("smart-final.json", &smart_final)
        .map_err(|error| format!("write SMART final: {error}"))?;
    if let Some(worker) = engine_worker.take() {
        for sample in worker.shutdown()? {
            let retained = artifacts
                .append_engine(&sample)
                .map_err(|error| format!("append final engine sample: {error}"))?;
            account_retention_drop(&mut retention_dropped_records, retained, 1);
        }
    }
    if let Some(worker) = kernel_worker.take() {
        for snapshot in worker.shutdown()? {
            let retained = artifacts
                .append_kernel(&snapshot)
                .map_err(|error| format!("append final kernel health sample: {error}"))?;
            account_retention_drop(&mut retention_dropped_records, retained, 1);
            if let Some(error) = snapshot.error {
                disk_degradation_collection_errors = vec![format!("kernel journal: {error}")];
            } else {
                disk_degradation_collection_errors.clear();
                merge_bounded_evidence(&mut kernel_degradation_evidence, snapshot.evidence);
            }
        }
    }
    merge_bounded_evidence(&mut disk_degradation_evidence, smart_degradation_evidence);
    merge_bounded_evidence(&mut disk_degradation_evidence, kernel_degradation_evidence);
    merge_bounded_evidence(
        &mut disk_degradation_evidence,
        filesystem_degradation_evidence,
    );
    status.retention_dropped_records = retention_dropped_records;
    status.retention_saturated = retention_dropped_records > 0;
    artifacts
        .publish_status(&status)
        .map_err(|error| format!("publish final observer status: {error}"))?;
    artifacts
        .sync()
        .map_err(|error| format!("sync observer artifacts: {error}"))?;
    incident_recorder.finalize()?;
    let report = DiagnosticReport {
        format: DIAGNOSTIC_FORMAT_V2,
        run_id: run_id.into(),
        terminal: status.terminal,
        samples: status.samples,
        telemetry_gaps: status.telemetry_gaps,
        active_alerts: status.active_alerts.clone(),
        incidents: incident_recorder.incidents().to_vec(),
        suppressed_incidents: incident_recorder.suppressed_count(),
        retention_dropped_records,
        retention_saturated: retention_dropped_records > 0,
        disk_degradation_evidence,
        disk_degradation_collection_errors,
        last_error: status.last_error.clone(),
    };
    artifacts
        .write_named_json("DIAGNOSTIC_REPORT.json", &report)
        .map_err(|error| format!("write diagnostic report: {error}"))?;
    artifacts
        .write_named_bytes(
            "DIAGNOSTIC_REPORT.md",
            diagnostic_markdown(&report).as_bytes(),
        )
        .map_err(|error| format!("write diagnostic markdown: {error}"))?;
    http_server
        .shutdown()
        .map_err(|error| format!("stop observer HTTP: {error}"))?;
    write_checksums(&run_dir, &artifacts.budget)
        .map_err(|error| format!("write final evidence checksums: {error}"))
}

fn inherit_heavy_process_fields(current: &mut ProcessSampleV2, full: &ProcessSampleV2) {
    for (role, owner) in &mut current.roles {
        let Some(previous) = full.roles.get(role) else {
            continue;
        };
        if owner.pid != previous.pid || owner.start_ticks != previous.start_ticks {
            continue;
        }
        owner.pss_kib = previous.pss_kib;
        owner.anonymous_kib = previous.anonymous_kib;
        owner.swap_kib = previous.swap_kib;
        owner.open_fds = previous.open_fds;
        owner.socket_fds = previous.socket_fds;
        owner.cgroup_path.clone_from(&previous.cgroup_path);
        owner.cgroup.clone_from(&previous.cgroup);
    }
}

#[allow(clippy::too_many_arguments)]
fn build_frame(
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
    boot_id: &str,
    run_id: &str,
    telemetry: &AgentTelemetryMessage,
    server_alive: bool,
    host: &HostSampleV2,
    process: &ProcessSampleV2,
    disk: &DiskSampleV2,
    engine: Option<&EngineSampleV2>,
) -> DiagnosticMetricFrameV2 {
    let mut counters = BTreeMap::new();
    let mut gauges = BTreeMap::new();
    for (name, value) in &host.vmstat {
        counters.insert(format!("host.vmstat.{name}"), saturating_i64(*value));
    }
    for (name, value) in &host.network {
        counters.insert(format!("host.network.{name}"), saturating_i64(*value));
    }
    for (name, value) in &host.meminfo_kib {
        gauges.insert(format!("host.meminfo.{name}_kib"), *value as f64);
    }
    gauges.insert("host.load.1".into(), host.load_1);
    gauges.insert("host.load.5".into(), host.load_5);
    gauges.insert("host.load.15".into(), host.load_15);
    gauges.insert("host.runnable_tasks".into(), host.runnable_tasks as f64);
    for (name, value) in &host.psi {
        if let Some(avg10) = value.some_avg10 {
            gauges.insert(format!("host.psi.{name}.some_avg10"), avg10);
        }
        if let Some(avg10) = value.full_avg10 {
            gauges.insert(format!("host.psi.{name}.full_avg10"), avg10);
        }
    }
    for (role, value) in &process.roles {
        gauges.insert(format!("process.{role}.rss_bytes"), value.rss_bytes as f64);
        gauges.insert(
            format!("process.{role}.pss_kib"),
            value.pss_kib.unwrap_or(0) as f64,
        );
        gauges.insert(
            format!("process.{role}.swap_kib"),
            value.swap_kib.unwrap_or(0) as f64,
        );
        counters.insert(
            format!("process.{role}.read_bytes"),
            saturating_i64(value.read_bytes),
        );
        counters.insert(
            format!("process.{role}.write_bytes"),
            saturating_i64(value.write_bytes),
        );
        for (name, counter) in [
            ("minor_faults", value.minor_faults),
            ("major_faults", value.major_faults),
            ("user_ticks", value.user_ticks),
            ("system_ticks", value.system_ticks),
            (
                "voluntary_context_switches",
                value.voluntary_context_switches,
            ),
            (
                "involuntary_context_switches",
                value.involuntary_context_switches,
            ),
        ] {
            counters.insert(format!("process.{role}.{name}"), saturating_i64(counter));
        }
        gauges.insert(format!("process.{role}.threads"), value.threads as f64);
        gauges.insert(format!("process.{role}.open_fds"), value.open_fds as f64);
        gauges.insert(
            format!("process.{role}.socket_fds"),
            value.socket_fds as f64,
        );
        gauges.insert(
            format!("process.{role}.state_stopped"),
            f64::from(matches!(value.state.as_str(), "T" | "t")),
        );
        if let Some(cgroup) = &value.cgroup {
            if let Some(current) = cgroup.memory_current {
                gauges.insert(format!("cgroup.{role}.memory_current"), current as f64);
            }
            if let Some(peak) = cgroup.memory_peak {
                gauges.insert(format!("cgroup.{role}.memory_peak"), peak as f64);
            }
            for (name, value) in &cgroup.memory_events {
                counters.insert(
                    format!("cgroup.{role}.memory_events.{name}"),
                    saturating_i64(*value),
                );
            }
            for (name, value) in &cgroup.cpu_stat {
                counters.insert(
                    format!("cgroup.{role}.cpu_stat.{name}"),
                    saturating_i64(*value),
                );
            }
            for (name, value) in &cgroup.io_stat {
                counters.insert(
                    format!("cgroup.{role}.io_stat.{name}"),
                    saturating_i64(*value),
                );
            }
        }
    }
    for (name, value) in [
        ("disk.reads_completed", disk.reads_completed),
        ("disk.writes_completed", disk.writes_completed),
        ("disk.sectors_read", disk.sectors_read),
        ("disk.sectors_written", disk.sectors_written),
        ("disk.read_millis", disk.read_millis),
        ("disk.write_millis", disk.write_millis),
        ("disk.io_millis", disk.io_millis),
        ("disk.weighted_io_millis", disk.weighted_io_millis),
    ] {
        counters.insert(name.into(), saturating_i64(value));
    }
    gauges.insert("disk.io_in_progress".into(), disk.io_in_progress as f64);
    gauges.insert(
        "filesystem.available_bytes".into(),
        disk.filesystem_available_bytes as f64,
    );
    gauges.insert(
        "filesystem.read_only".into(),
        f64::from(disk.filesystem_read_only),
    );
    gauges.insert("observer.server_identity_checked".into(), 1.0);
    gauges.insert("observer.agent_identity_checked".into(), 1.0);
    gauges.insert(
        "process.agent.present".into(),
        f64::from(process.roles.contains_key("agent")),
    );
    counters.insert(
        "agent.workload_units".into(),
        saturating_i64(telemetry.progress.workload_units),
    );
    append_engine_metrics(engine, unix_millis, &mut counters, &mut gauges);
    DiagnosticMetricFrameV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        sequence,
        monotonic_millis,
        unix_millis,
        boot_id: boot_id.into(),
        run_id: run_id.into(),
        heartbeats: HeartbeatSnapshot {
            observer_unix_millis: Some(unix_millis),
            agent_unix_millis: Some(telemetry.agent_unix_millis),
            server_unix_millis: server_alive.then_some(unix_millis),
        },
        progress: telemetry.progress.clone(),
        counters,
        gauges,
    }
}

fn append_engine_metrics(
    sample: Option<&EngineSampleV2>,
    now_unix_millis: u64,
    counters: &mut BTreeMap<String, i64>,
    gauges: &mut BTreeMap<String, f64>,
) {
    let Some(sample) = sample else {
        gauges.insert("engine.sample_available".into(), 0.0);
        return;
    };
    gauges.insert(
        "engine.sample_available".into(),
        f64::from(sample.available()),
    );
    gauges.insert("engine.query_millis".into(), sample.query_millis as f64);
    gauges.insert(
        "engine.sample_age_millis".into(),
        now_unix_millis.saturating_sub(sample.unix_millis) as f64,
    );
    gauges.insert(
        "engine.sample_error".into(),
        f64::from(sample.error.is_some()),
    );
    let Some(snapshot) = sample
        .snapshot
        .as_ref()
        .and_then(serde_json::Value::as_object)
    else {
        return;
    };
    if snapshot
        .get("engine_kind")
        .and_then(serde_json::Value::as_str)
        == Some("postgresql")
    {
        if let Some(value) = snapshot
            .get("postgres")
            .and_then(|value| value.get("counters"))
        {
            flatten_engine_counters("postgres.counter", value, counters);
        }
        if let Some(value) = snapshot
            .get("postgres")
            .and_then(|value| value.get("gauges"))
        {
            flatten_engine_gauges("postgres", value, gauges);
        }
        if let Some(value) = snapshot.get("server_runtime") {
            flatten_engine_gauges("engine.server", value, gauges);
        }
        return;
    }
    for name in [
        "active_transactions",
        "oldest_transaction_age_millis",
        "transaction_wait_edges",
        "hot_tables",
        "hot_rows",
        "hot_bytes",
        "staging_transactions",
        "staging_tables",
        "staging_rows",
        "cold_tables",
        "cold_segments",
        "cold_unleveled_segments",
        "cold_l0_segments",
        "cold_l1_segments",
        "cold_l0_debt_physical_bytes",
        "cold_rows",
        "cold_resident_bytes",
        "cold_metadata_bytes",
        "cold_row_id_bytes",
        "cold_exact_index_bytes",
        "cold_ordered_index_bytes",
        "cold_descriptor_bytes",
        "cold_column_payload_bytes",
        "cold_tombstones",
        "read_queue_depth",
        "max_compaction_jobs",
        "compaction_active_jobs",
        "compaction_peak_active_jobs",
        "max_compaction_input_segments",
        "max_compaction_input_bytes",
        "max_compaction_output_bytes",
        "compaction_job_time_budget_ms",
        "compaction_io_bytes_per_sec",
        "compaction_disk_reserve_bytes",
        "compaction_retry_cooldown_ms",
        "compaction_retry_cooldown_until_unix_millis",
        "l0_soft_limit_segments",
        "l0_hard_limit_segments",
        "l0_soft_limit_bytes",
        "l0_hard_limit_bytes",
        "wal_current_file_bytes",
        "wal_max_file_bytes",
        "wal_pending_durability_bytes",
        "last_checkpoint_unix_nanos",
        "snapshot_nanos",
    ] {
        if let Some(value) = snapshot.get(name).and_then(json_number) {
            gauges.insert(format!("engine.{name}"), value);
        }
    }
    for name in [
        "accepting_transactions",
        "complete",
        "pressure_seal_requested",
        "compaction_requested",
        "compaction_retry_cooldown_active",
        "seal_running",
        "compaction_running",
        "checkpoint_running",
        "checkpoint_mutex_busy",
        "wal_running",
    ] {
        if let Some(value) = snapshot.get(name).and_then(serde_json::Value::as_bool) {
            gauges.insert(format!("engine.{name}"), f64::from(value));
        }
    }
    if let Some(value) = snapshot
        .get("wal_current_lsn")
        .and_then(serde_json::Value::as_u64)
    {
        counters.insert("engine.wal_current_lsn".into(), saturating_i64(value));
    }
    for name in [
        "compaction_soft_backpressure_waits",
        "compaction_soft_backpressure_wait_millis",
        "compaction_hard_backpressure_rejections",
        "compaction_retry_suppressed",
    ] {
        if let Some(value) = snapshot.get(name).and_then(serde_json::Value::as_u64) {
            counters.insert(format!("engine.{name}"), saturating_i64(value));
        }
    }
    if let Some(value) = snapshot.get("counters") {
        flatten_engine_counters("engine.counter", value, counters);
    }
    if let Some(value) = snapshot.get("runtime_owners") {
        flatten_engine_gauges("engine.owner", value, gauges);
    }
    if let Some(value) = snapshot.get("maintenance") {
        flatten_engine_maintenance("engine.maintenance", value, counters, gauges);
    }
    if let Some(value) = snapshot.get("server_runtime") {
        flatten_engine_gauges("engine.server", value, gauges);
    }
}

fn flatten_engine_counters(
    prefix: &str,
    value: &serde_json::Value,
    counters: &mut BTreeMap<String, i64>,
) {
    let Some(object) = value.as_object() else {
        return;
    };
    for (name, value) in object {
        let metric = format!("{prefix}.{name}");
        if let Some(value) = value.as_u64() {
            counters.insert(metric, saturating_i64(value));
        } else if let Some(value) = value.as_i64() {
            counters.insert(metric, value);
        } else if value.is_object() {
            flatten_engine_counters(&metric, value, counters);
        }
    }
}

fn flatten_engine_gauges(
    prefix: &str,
    value: &serde_json::Value,
    gauges: &mut BTreeMap<String, f64>,
) {
    let Some(object) = value.as_object() else {
        return;
    };
    for (name, value) in object {
        let metric = format!("{prefix}.{name}");
        if let Some(value) = json_number(value) {
            gauges.insert(metric, value);
        } else if let Some(value) = value.as_bool() {
            gauges.insert(metric, f64::from(value));
        } else if value.is_object() {
            flatten_engine_gauges(&metric, value, gauges);
        }
    }
}

fn flatten_engine_maintenance(
    prefix: &str,
    value: &serde_json::Value,
    counters: &mut BTreeMap<String, i64>,
    gauges: &mut BTreeMap<String, f64>,
) {
    let Some(object) = value.as_object() else {
        return;
    };
    for (name, value) in object {
        let metric = format!("{prefix}.{name}");
        if value.is_object() {
            flatten_engine_maintenance(&metric, value, counters, gauges);
            continue;
        }
        if let Some(value) = value.as_bool() {
            gauges.insert(metric, f64::from(value));
            continue;
        }
        let is_counter = matches!(
            name.as_str(),
            "epoch"
                | "calls"
                | "completed"
                | "failed"
                | "total_input_rows"
                | "total_input_bytes"
                | "total_output_rows"
                | "total_output_bytes"
                | "total_reclaimed_rows"
                | "total_reclaimed_bytes"
                | "last_result_marker"
                | "background_loop_epoch"
                | "jobs_selected_sub_target_merge"
                | "jobs_selected_tombstone_cleanup"
                | "jobs_selected_oversized_segment_split"
                | "jobs_planned"
                | "jobs_deferred"
                | "jobs_waited_retry_cooldown"
                | "jobs_waited_other"
                | "jobs_published"
                | "jobs_invalidated"
                | "jobs_invalidated_topology"
                | "jobs_invalidated_other"
                | "jobs_cancelled_schema_epoch"
                | "jobs_cancelled_input_snapshot"
                | "jobs_cancelled_budget"
                | "jobs_cancelled_other"
                | "jobs_failed"
                | "total_logical_input_rows"
                | "total_logical_input_bytes"
                | "total_generated_output_rows"
                | "total_generated_output_bytes"
                | "total_published_input_rows"
                | "total_published_input_bytes"
                | "total_published_output_rows"
                | "total_published_output_bytes"
                | "total_wasted_input_rows"
                | "total_wasted_input_bytes"
                | "total_wasted_output_rows"
                | "total_wasted_output_bytes"
                | "posting_outputs_generated"
                | "posting_outputs_published"
                | "posting_outputs_wasted"
                | "manifest_publications"
                | "total_manifest_publication_nanos"
        );
        if is_counter {
            if let Some(value) = value.as_u64() {
                counters.insert(metric, saturating_i64(value));
            } else if let Some(value) = value.as_i64() {
                counters.insert(metric, value);
            }
        } else if let Some(value) = json_number(value) {
            gauges.insert(metric, value);
        }
    }
}

fn json_number(value: &serde_json::Value) -> Option<f64> {
    value
        .as_u64()
        .map(|value| value as f64)
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_f64())
}

fn hardware_inventory(boot_id: &str, data_directory: &Path) -> HardwareInventory {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let cpu_model = cpuinfo.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == "model name").then(|| value.trim().to_string())
    });
    let memory_total_kib = fs::read_to_string("/proc/meminfo").ok().and_then(|value| {
        value.lines().find_map(|line| {
            line.strip_prefix("MemTotal:")?
                .split_ascii_whitespace()
                .next()?
                .parse()
                .ok()
        })
    });
    HardwareInventory {
        format: DIAGNOSTIC_FORMAT_V2,
        boot_id: boot_id.into(),
        kernel_release: fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
            .into(),
        cpu_model,
        logical_cpus: thread::available_parallelism().map_or(0, usize::from),
        memory_total_kib,
        data_directory: data_directory.display().to_string(),
    }
}

fn wait_for_run_directory(run_dir: &Path, stop: &AtomicBool) -> Result<(), String> {
    let deadline = Instant::now() + RUN_DIRECTORY_WAIT;
    while Instant::now() < deadline {
        if run_dir.is_dir() {
            return Ok(());
        }
        if stop.load(Ordering::Acquire) {
            return Err("observer interrupted before agent run directory appeared".into());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "agent run directory did not appear within {} seconds",
        RUN_DIRECTORY_WAIT.as_secs()
    ))
}

fn open_append(run_dir: &Path, name: &str) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(run_dir.join(name))
}

fn acquire_observer_lock(run_dir: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(run_dir.join("observer.lock"))?;
    // SAFETY: `file` owns a live descriptor for the duration of the call and
    // remains stored in `ObserverArtifacts`, retaining the advisory lock.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("another observer owns run '{}'", run_dir.display()),
        ));
    }
    Ok(file)
}

fn recover_jsonl_tail(path: &Path) -> io::Result<u64> {
    const MAX_RECORD_BYTES: u64 = 1024 * 1024;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(0);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut final_byte = [0_u8; 1];
    file.read_exact(&mut final_byte)?;
    if final_byte[0] == b'\n' {
        return Ok(0);
    }
    let read_bytes = length.min(MAX_RECORD_BYTES);
    file.seek(SeekFrom::Start(length - read_bytes))?;
    let mut tail = vec![0_u8; usize::try_from(read_bytes).unwrap_or(usize::MAX)];
    file.read_exact(&mut tail)?;
    let recovered_length = tail
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|position| length - read_bytes + position as u64 + 1)
        .unwrap_or(0);
    if recovered_length == 0 && length > MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} has an unterminated record above 1 MiB", path.display()),
        ));
    }
    file.set_len(recovered_length)?;
    file.sync_data()?;
    Ok(length - recovered_length)
}

fn read_last_json_line<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    const MAX_RECORD_BYTES: u64 = 1024 * 1024;
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(None);
    }
    let read_bytes = length.min(MAX_RECORD_BYTES);
    file.seek(SeekFrom::Start(length - read_bytes))?;
    let mut tail = vec![0_u8; usize::try_from(read_bytes).unwrap_or(usize::MAX)];
    file.read_exact(&mut tail)?;
    let text = std::str::from_utf8(&tail)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let line = text
        .trim_end_matches(['\r', '\n'])
        .rsplit('\n')
        .next()
        .unwrap_or_default();
    if line.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(line)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn read_optional_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn directory_bytes(root: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                total = total.saturating_add(entry.metadata()?.len());
            }
        }
    }
    Ok(total)
}

fn append_bounded<T: Serialize>(
    writer: &mut BufWriter<File>,
    budget: &ArtifactBudget,
    value: &T,
) -> io::Result<()> {
    let payload = encode_json_line(value)?;
    budget
        .reserve(payload.len())
        .map_err(artifact_budget_error)?;
    writer.write_all(&payload)?;
    Ok(())
}

fn append_retained<T: Serialize>(
    writer: &mut BufWriter<File>,
    budget: &ArtifactBudget,
    value: &T,
) -> io::Result<bool> {
    let payload = encode_json_line(value)?;
    if !budget
        .reserve_retained(payload.len())
        .map_err(artifact_budget_error)?
    {
        return Ok(false);
    }
    writer.write_all(&payload)?;
    Ok(true)
}

fn encode_json_line<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let mut payload = serde_json::to_vec(value)?;
    payload.push(b'\n');
    Ok(payload)
}

fn account_retention_drop(total: &mut u64, retained: bool, records: u64) {
    if !retained {
        *total = total.saturating_add(records);
    }
}

fn atomic_json<T: Serialize>(path: &Path, value: &T, budget: &ArtifactBudget) -> io::Result<()> {
    let mut payload = serde_json::to_vec(value)?;
    payload.push(b'\n');
    let reservation = budget
        .reserve_replacement(path, payload.len())
        .map_err(artifact_budget_error)?;
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
    {
        Ok(file) => file,
        Err(error) => {
            budget.cancel_replacement(reservation);
            return Err(error);
        }
    };
    let result = (|| {
        file.write_all(&payload)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_ok() {
        budget.commit_replacement(reservation);
    } else {
        let _ = fs::remove_file(&temporary);
        budget.cancel_replacement(reservation);
    }
    result
}

fn artifact_budget_error(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, error)
}

fn history_capacity(window: Duration, interval: Duration, max_samples: usize) -> usize {
    let capacity = window.as_millis() / interval.as_millis().max(1);
    usize::try_from(capacity)
        .unwrap_or(usize::MAX)
        .clamp(1, max_samples.max(1))
}

fn restore_frame_history(
    path: &Path,
    capacity: usize,
    detector: &mut DiagnosticDetector,
) -> Result<VecDeque<DiagnosticMetricFrameV2>, String> {
    const MAX_RESTORE_HISTORY_BYTES: u64 = 32 * 1024 * 1024;
    restore_frame_history_with_limit(path, capacity, MAX_RESTORE_HISTORY_BYTES, detector)
}

fn restore_frame_history_with_limit(
    path: &Path,
    capacity: usize,
    max_bytes: u64,
    detector: &mut DiagnosticDetector,
) -> Result<VecDeque<DiagnosticMetricFrameV2>, String> {
    let mut file = File::open(path).map_err(|error| format!("open diagnostic history: {error}"))?;
    let length = file
        .metadata()
        .map_err(|error| format!("stat diagnostic history: {error}"))?
        .len();
    let start = length.saturating_sub(max_bytes.max(1));
    let discard_partial_line = if start == 0 {
        false
    } else {
        file.seek(SeekFrom::Start(start - 1))
            .map_err(|error| format!("seek diagnostic history: {error}"))?;
        let mut previous = [0_u8; 1];
        file.read_exact(&mut previous)
            .map_err(|error| format!("read diagnostic history boundary: {error}"))?;
        previous[0] != b'\n'
    };
    file.seek(SeekFrom::Start(start))
        .map_err(|error| format!("seek diagnostic history tail: {error}"))?;
    let mut reader = BufReader::new(file);
    if discard_partial_line {
        let mut discarded = Vec::new();
        reader
            .read_until(b'\n', &mut discarded)
            .map_err(|error| format!("discard partial diagnostic history record: {error}"))?;
    }
    let mut history = VecDeque::with_capacity(capacity);
    for (index, line) in reader.lines().enumerate() {
        let line_number = index.saturating_add(1);
        let line =
            line.map_err(|error| format!("read diagnostic history {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let frame: DiagnosticMetricFrameV2 = serde_json::from_str(&line)
            .map_err(|error| format!("parse diagnostic history {line_number}: {error}"))?;
        frame
            .validate()
            .map_err(|error| format!("validate diagnostic history {line_number}: {error}"))?;
        detector.observe(&frame)?;
        if history.len() == capacity {
            history.pop_front();
        }
        history.push_back(frame);
    }
    Ok(history)
}

fn is_terminal_phase(phase: &str) -> bool {
    matches!(phase, "passed" | "failed" | "interrupted")
}

fn validate_run_id(run_id: &str) -> Result<(), String> {
    if run_id.is_empty()
        || run_id.len() > 128
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err("invalid observer run id".into());
    }
    Ok(())
}

fn unix_millis() -> Result<u64, String> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis();
    u64::try_from(value).map_err(|_| "Unix timestamp overflow".into())
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn merge_bounded_evidence(target: &mut Vec<String>, evidence: Vec<String>) {
    const MAX_EVIDENCE: usize = 32;
    const MAX_EVIDENCE_BYTES: usize = 512;

    for mut item in evidence {
        if item.len() > MAX_EVIDENCE_BYTES {
            item.truncate(MAX_EVIDENCE_BYTES);
        }
        if !item.is_empty() && !target.contains(&item) && target.len() < MAX_EVIDENCE {
            target.push(item);
        }
    }
}

fn diagnostic_markdown(report: &DiagnosticReport) -> String {
    let mut output = format!(
        concat!(
            "# RadixDB intelligent soak diagnostic report\n\n",
            "- Run: `{}`\n",
            "- Terminal event observed: `{}`\n",
            "- Samples: `{}`\n",
            "- Telemetry gaps: `{}`\n",
            "- Active alerts: `{}`\n",
            "- Incidents: `{}`\n",
            "- Suppressed incidents: `{}`\n",
            "- Retention saturated: `{}`\n",
            "- Retention records dropped: `{}`\n",
            "- Disk degradation signals: `{}`\n",
            "- Last observer error: `{}`\n"
        ),
        report.run_id,
        report.terminal,
        report.samples,
        report.telemetry_gaps,
        report.active_alerts.len(),
        report.incidents.len(),
        report.suppressed_incidents,
        report.retention_saturated,
        report.retention_dropped_records,
        report.disk_degradation_evidence.len(),
        report.last_error.as_deref().unwrap_or("none"),
    );
    if !report.incidents.is_empty() {
        output.push_str("\n## Incidents\n\n");
        for incident in &report.incidents {
            let state = if incident.closed_unix_millis.is_some() {
                "recovered"
            } else {
                "active"
            };
            output.push_str(&format!(
                "- `{}`: `{}` / `{:?}` / `{state}`; missing evidence: `{}`\n",
                incident.id,
                incident.candidate_cause,
                incident.confidence,
                incident.missing_evidence.len(),
            ));
        }
    }
    output
}

fn write_checksums(run_dir: &Path, budget: &ArtifactBudget) -> io::Result<()> {
    let mut files = Vec::new();
    collect_files(run_dir, run_dir, &mut files)?;
    files.retain(|path| path != Path::new("SHA256SUMS"));
    files.sort();
    let mut output = Vec::new();
    for relative in files {
        let mut input = File::open(run_dir.join(&relative))?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        writeln!(output, "{:x}  {}", digest.finalize(), relative.display())?;
    }
    budget
        .reserve(output.len())
        .map_err(artifact_budget_error)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(run_dir.join("SHA256SUMS"))?;
    file.write_all(&output)?;
    file.sync_all()
}

fn collect_files(root: &Path, directory: &Path, output: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_files(root, &entry.path(), output)?;
        } else if file_type.is_file() {
            output.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(io::Error::other)?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_cost_ledger_is_flattened_as_monotonic_counters() {
        let value = serde_json::json!({
            "compaction_cost": {
                "jobs_selected_sub_target_merge": 5,
                "jobs_planned": 3,
                "jobs_deferred": 4,
                "jobs_waited_retry_cooldown": 4,
                "jobs_published": 2,
                "jobs_invalidated": 1,
                "jobs_cancelled_input_snapshot": 1,
                "total_wasted_output_bytes": 4096,
                "total_manifest_publication_nanos": 500,
                "last_manifest_publication_nanos": 125,
                "last_outcome": "topology_publication_rejected"
            }
        });
        let mut counters = BTreeMap::new();
        let mut gauges = BTreeMap::new();

        flatten_engine_maintenance("engine.maintenance", &value, &mut counters, &mut gauges);

        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.jobs_selected_sub_target_merge"),
            Some(&5)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.jobs_planned"),
            Some(&3)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.jobs_deferred"),
            Some(&4)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.jobs_waited_retry_cooldown"),
            Some(&4)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.jobs_cancelled_input_snapshot"),
            Some(&1)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.total_wasted_output_bytes"),
            Some(&4096)
        );
        assert_eq!(
            counters.get("engine.maintenance.compaction_cost.total_manifest_publication_nanos"),
            Some(&500)
        );
        assert_eq!(
            gauges.get("engine.maintenance.compaction_cost.last_manifest_publication_nanos"),
            Some(&125.0)
        );
        assert!(!counters
            .keys()
            .any(|metric| metric.ends_with("last_outcome")));
    }
    use crate::{
        config::SoakConfig,
        diagnostics::{
            AgentTelemetryClient, CgroupSnapshot, ProcessSnapshot, SemanticProgressTracker,
        },
    };

    #[test]
    fn history_and_run_identifiers_are_bounded() {
        assert_eq!(
            history_capacity(Duration::from_secs(30), Duration::from_secs(5), 512),
            6
        );
        assert_eq!(
            history_capacity(Duration::from_secs(30), Duration::from_millis(1), 128),
            128
        );
        assert!(validate_run_id("run-1").is_ok());
        assert!(validate_run_id("../escape").is_err());
    }

    #[test]
    fn history_resume_reads_only_a_bounded_tail() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("frames.jsonl");
        let records = (1..=10)
            .map(|sequence| {
                encode_json_line(&DiagnosticMetricFrameV2 {
                    format: DIAGNOSTIC_FORMAT_V2,
                    sequence,
                    monotonic_millis: sequence * 1_000,
                    unix_millis: sequence * 1_000,
                    boot_id: "boot".into(),
                    run_id: "run".into(),
                    heartbeats: HeartbeatSnapshot::default(),
                    progress: crate::diagnostics::SemanticProgressSnapshot {
                        format: DIAGNOSTIC_FORMAT_V2,
                        sequence,
                        phase_epoch: 1,
                        phase: "clients-2".into(),
                        phase_started_unix_millis: 1,
                        workload_epoch: sequence,
                        workload_units: sequence,
                        last_workload_progress_unix_millis: sequence * 1_000,
                        operation_epoch: 0,
                        active_operation: None,
                        last_operation_progress_unix_millis: sequence * 1_000,
                        planned_silence: None,
                    },
                    counters: BTreeMap::new(),
                    gauges: BTreeMap::new(),
                })
                .unwrap()
            })
            .collect::<Vec<_>>();
        fs::write(&path, records.concat()).unwrap();
        let max_bytes = records[8].len() + records[9].len() + records[7].len() / 2;
        let mut detector = DiagnosticDetector::new(DetectorConfig::default()).unwrap();
        let history = restore_frame_history_with_limit(
            &path,
            16,
            u64::try_from(max_bytes).unwrap(),
            &mut detector,
        )
        .unwrap();
        assert_eq!(
            history
                .iter()
                .map(|frame| frame.sequence)
                .collect::<Vec<_>>(),
            [9, 10]
        );
    }

    #[test]
    fn burst_process_sampling_reuses_rate_limited_heavy_fields_only_for_same_process() {
        let sample = |sequence: u64, pid: u32, start_ticks: u64, open_fds: bool, cgroup: bool| {
            ProcessSampleV2 {
                format: DIAGNOSTIC_FORMAT_V2,
                sequence,
                monotonic_millis: sequence,
                unix_millis: sequence,
                roles: BTreeMap::from([(
                    "observer".into(),
                    ProcessSnapshot {
                        pid,
                        start_ticks,
                        executable: "/test/radixdb-soak-observer".into(),
                        state: "S".into(),
                        rss_bytes: 1024,
                        virtual_bytes: 2048,
                        pss_kib: open_fds.then_some(11),
                        anonymous_kib: open_fds.then_some(7),
                        swap_kib: open_fds.then_some(3),
                        minor_faults: 1,
                        major_faults: 0,
                        user_ticks: 2,
                        system_ticks: 1,
                        threads: 1,
                        open_fds: u64::from(open_fds) * 9,
                        socket_fds: u64::from(open_fds) * 2,
                        read_bytes: 0,
                        write_bytes: 0,
                        cancelled_write_bytes: 0,
                        voluntary_context_switches: 1,
                        involuntary_context_switches: 0,
                        cgroup_path: cgroup.then(|| "/radixdb-soak.slice".into()),
                        cgroup: cgroup.then(CgroupSnapshot::default),
                        wchan: Some("futex_wait_queue".into()),
                    },
                )]),
                errors: Vec::new(),
            }
        };
        let full = sample(1, 42, 1_000, true, true);
        let mut light = sample(2, 42, 1_000, false, false);

        inherit_heavy_process_fields(&mut light, &full);

        let inherited = light.roles.get("observer").unwrap();
        let source = full.roles.get("observer").unwrap();
        assert_eq!(inherited.pid, source.pid);
        assert_eq!(inherited.start_ticks, source.start_ticks);
        assert_eq!(inherited.open_fds, source.open_fds);
        assert_eq!(inherited.socket_fds, source.socket_fds);
        assert_eq!(inherited.cgroup, source.cgroup);
        assert_eq!(inherited.pss_kib, source.pss_kib);

        let mut reused_pid = sample(3, 42, 2_000, false, false);
        inherit_heavy_process_fields(&mut reused_pid, &full);
        let replacement = reused_pid.roles.get("observer").unwrap();
        assert_eq!(replacement.open_fds, 0);
        assert!(replacement.cgroup.is_none());
        assert!(replacement.pss_kib.is_none());
    }

    #[test]
    fn observer_artifacts_refuse_reuse_and_enforce_quota() {
        let directory = tempfile::tempdir().unwrap();
        let run_dir = directory.path().join("run-1");
        fs::create_dir(&run_dir).unwrap();
        let mut writer = ObserverArtifacts::open(&run_dir, 64 * 1024 * 1024).unwrap();
        writer
            .write_inventory(&hardware_inventory("boot", directory.path()))
            .unwrap();
        assert!(ObserverArtifacts::open(&run_dir, 64 * 1024 * 1024).is_err());
    }

    #[test]
    fn observer_artifacts_resume_after_crash_and_truncate_partial_jsonl_tail() {
        let directory = tempfile::tempdir().unwrap();
        let run_dir = directory.path().join("run-1");
        fs::create_dir(&run_dir).unwrap();
        let mut writer = ObserverArtifacts::open(&run_dir, 64 * 1024 * 1024).unwrap();
        writer
            .write_inventory(&hardware_inventory("boot-1", directory.path()))
            .unwrap();
        drop(writer);
        fs::write(run_dir.join("diagnostic-frames.jsonl"), b"{partial").unwrap();

        let mut resumed = ObserverArtifacts::open(&run_dir, 64 * 1024 * 1024).unwrap();
        assert!(resumed.resume_state().resumed);
        assert_eq!(resumed.resume_state().truncated_tail_bytes, 8);
        assert_eq!(
            resumed.resume_state().previous_boot_id.as_deref(),
            Some("boot-1")
        );
        resumed.set_boot_identity("boot-2");
        assert!(resumed.resume_state().boot_identity_changed);
        resumed.publish_resume().unwrap();
        assert_eq!(
            fs::metadata(run_dir.join("diagnostic-frames.jsonl"))
                .unwrap()
                .len(),
            0
        );
        assert!(run_dir.join("observer-resumes.jsonl").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn observer_keeps_sampling_after_agent_channel_disappears() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let runs = root.join("runs");
        let run_dir = runs.join("run-1");
        let data = root.join("data");
        fs::create_dir_all(&run_dir).unwrap();
        fs::create_dir(&data).unwrap();
        let auth = root.join("auth.env");
        fs::write(
            &auth,
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        let socket = root.join("agent.sock");
        let config_path = root.join("soak.toml");
        fs::write(
            &config_path,
            format!(
                r#"format = 1
profile = "smoke"
duration = "10m"
seed = 1
[database]
address = "127.0.0.1:25441"
name = "observer_test"
[status]
bind = "127.0.0.1:0"
auth_file = "{}"
[artifacts]
root = "{}"
[monitor]
server_executable = "/bin/sleep"
data_dir = "{}"
[diagnostics]
enabled = true
bind = "127.0.0.1:0"
auth_file = "{}"
channel_path = "{}"
normal_interval = "1s"
alert_interval = "250ms"
burst_interval = "100ms"
burst_duration = "1s"
history_window = "2s"
history_max_samples = 16
artifact_quota_bytes = 67108864
max_incidents = 8
engine_snapshot_interval = "1s"
engine_snapshot_timeout = "20ms"
smart_interval = "30m"
[load]
client_steps = [2]
active_rows = 1
checkpoint_interval = "30s"
sample_interval = "1s"
invariant_interval = "5s"
"#,
                auth.display(),
                runs.display(),
                data.display(),
                auth.display(),
                socket.display(),
            ),
        )
        .unwrap();
        let config = SoakConfig::load(&config_path).unwrap().resolve().unwrap();
        let observer = thread::spawn(move || run(config, "run-1", Some(3)));
        for _ in 0..200 {
            if socket.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        let mut client = AgentTelemetryClient::connect(&socket, "run-1").unwrap();
        let tracker = SemanticProgressTracker::new(unix_millis().unwrap(), "clients-2").unwrap();
        client.publish(unix_millis().unwrap(), tracker.snapshot());
        drop(client);

        observer.join().unwrap().unwrap();

        let frames = fs::read_to_string(run_dir.join("diagnostic-frames.jsonl")).unwrap();
        assert!(frames.lines().count() >= 2);
        let last: DiagnosticMetricFrameV2 =
            serde_json::from_str(frames.lines().last().unwrap()).unwrap();
        assert_eq!(last.sequence, 3);
        assert!(run_dir.join("observer-status.json").is_file());
        assert!(run_dir.join("smart-final.json").is_file());
    }
}
