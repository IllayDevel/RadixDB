use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    DiagnosticAlertV2, DiagnosticIncidentV2, DiagnosticMetricFrameV2, DiagnosticState,
    DiskSampleV2, EngineSampleV2, HostSampleV2, ProcessSampleV2, SmartSnapshotV2,
    DIAGNOSTIC_FORMAT_V2,
};

const INCIDENT_COALESCE_MILLIS: u64 = 30_000;
const INCIDENT_COOLDOWN_MILLIS: u64 = 60_000;
const MAX_BURST_SAMPLES_PER_INCIDENT: u64 = 16_384;
const FORENSIC_QUEUE_CAPACITY: usize = 16;
const FORENSIC_CAPTURE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_JOURNAL_BYTES: usize = 256 * 1024;
const MAX_KERNEL_STACK_BYTES: u64 = 16 * 1024;
const MIN_FINALIZATION_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_FINALIZATION_RESERVE_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct ArtifactBudget {
    limit: u64,
    retained_limit: u64,
    used: AtomicU64,
}

pub(super) struct ReplacementReservation {
    previous_bytes: u64,
    staged_bytes: u64,
}

impl ArtifactBudget {
    pub(super) fn new(limit: u64, used: u64) -> Result<Self, String> {
        if used > limit {
            return Err("existing diagnostic evidence exceeds configured quota".into());
        }
        let finalization_reserve = (limit / 16)
            .clamp(
                MIN_FINALIZATION_RESERVE_BYTES,
                MAX_FINALIZATION_RESERVE_BYTES,
            )
            .min(limit / 2);
        Ok(Self {
            limit,
            retained_limit: limit.saturating_sub(finalization_reserve),
            used: AtomicU64::new(used),
        })
    }

    pub(super) fn reserve(&self, bytes: usize) -> Result<(), String> {
        let bytes = u64::try_from(bytes).map_err(|_| "artifact size overflow".to_string())?;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| "diagnostic artifact quota exhausted".to_string())
    }

    /// Reserve space for repeatable telemetry. `false` means that the soft
    /// retention ceiling was reached and the caller must drop this record
    /// while keeping the observer alive. The remaining quota is reserved for
    /// status, incident metadata, checksums and the final report.
    pub(super) fn reserve_retained(&self, bytes: usize) -> Result<bool, String> {
        let bytes = u64::try_from(bytes).map_err(|_| "artifact size overflow".to_string())?;
        match self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.retained_limit)
            }) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Account for the temporary and previous file simultaneously. The caller
    /// must settle the reservation after either publishing or removing the
    /// temporary file, so repeated atomic replacements cannot leak quota.
    pub(super) fn reserve_replacement(
        &self,
        path: &Path,
        new_bytes: usize,
    ) -> Result<ReplacementReservation, String> {
        let previous_bytes = fs::metadata(path).map_or(0, |metadata| metadata.len());
        let staged_bytes = u64::try_from(new_bytes).map_err(|_| "artifact size overflow")?;
        self.reserve(new_bytes)?;
        Ok(ReplacementReservation {
            previous_bytes,
            staged_bytes,
        })
    }

    pub(super) fn commit_replacement(&self, reservation: ReplacementReservation) {
        self.release(reservation.previous_bytes);
    }

    pub(super) fn cancel_replacement(&self, reservation: ReplacementReservation) {
        self.release(reservation.staged_bytes);
    }

    pub(super) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub(super) fn limit(&self) -> u64 {
        self.limit
    }

    pub(super) fn retained_limit(&self) -> u64 {
        self.retained_limit
    }

    fn release(&self, bytes: u64) {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            })
            .expect("artifact budget release exceeds reservation");
    }
}

#[derive(Clone, Copy, Debug)]
enum ForensicStage {
    Before,
    After,
}

impl ForensicStage {
    fn threads_name(self) -> &'static str {
        match self {
            Self::Before => "threads.json",
            Self::After => "threads-after.json",
        }
    }

    fn journal_name(self) -> Option<&'static str> {
        matches!(self, Self::Before).then_some("journal.txt")
    }
}

struct ForensicJob {
    incident_id: String,
    directory: PathBuf,
    stage: ForensicStage,
    process: ProcessSampleV2,
    budget: Arc<ArtifactBudget>,
}

struct ForensicResult {
    incident_id: String,
    completed: Vec<String>,
    missing: Vec<String>,
}

enum ForensicCommand {
    Capture(ForensicJob),
    Shutdown,
}

struct ForensicWorker {
    sender: SyncSender<ForensicCommand>,
    results: Receiver<ForensicResult>,
    join: Option<JoinHandle<()>>,
}

impl ForensicWorker {
    fn start() -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel(FORENSIC_QUEUE_CAPACITY);
        let (result_sender, results) = mpsc::channel();
        let join = thread::Builder::new()
            .name("soak-forensic".into())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        ForensicCommand::Capture(job) => {
                            let result = execute_forensic_job(job);
                            if result_sender.send(result).is_err() {
                                break;
                            }
                        }
                        ForensicCommand::Shutdown => break,
                    }
                }
            })
            .map_err(|error| format!("start forensic worker: {error}"))?;
        Ok(Self {
            sender,
            results,
            join: Some(join),
        })
    }

    fn try_capture(&self, job: ForensicJob) -> Result<(), String> {
        self.sender
            .try_send(ForensicCommand::Capture(job))
            .map_err(|error| match error {
                TrySendError::Full(_) => "forensic queue is full".to_string(),
                TrySendError::Disconnected(_) => "forensic worker is unavailable".to_string(),
            })
    }

    fn drain(&self) -> Vec<ForensicResult> {
        self.results.try_iter().collect()
    }

    fn shutdown(&mut self) -> Result<Vec<ForensicResult>, String> {
        if self.join.is_none() {
            return Ok(self.drain());
        }
        self.sender
            .send(ForensicCommand::Shutdown)
            .map_err(|_| "forensic worker stopped before shutdown".to_string())?;
        self.join
            .take()
            .expect("forensic join handle checked")
            .join()
            .map_err(|_| "forensic worker panicked".to_string())?;
        Ok(self.drain())
    }
}

impl Drop for ForensicWorker {
    fn drop(&mut self) {
        if self.join.is_some() {
            let _ = self.sender.send(ForensicCommand::Shutdown);
            if let Some(join) = self.join.take() {
                let _ = join.join();
            }
        }
    }
}

pub struct IncidentRecorder {
    root: PathBuf,
    max_incidents: usize,
    next_sequence: u64,
    active_alerts: BTreeMap<String, String>,
    active_incidents: BTreeMap<String, DiagnosticIncidentV2>,
    closed_by_kind: BTreeMap<String, u64>,
    incidents: Vec<DiagnosticIncidentV2>,
    burst_samples: BTreeMap<String, u64>,
    suppressed: u64,
    forensic: ForensicWorker,
    budget: Arc<ArtifactBudget>,
}

pub(super) struct IncidentEvidence<'a> {
    pub host: &'a HostSampleV2,
    pub process: &'a ProcessSampleV2,
    pub disk: &'a DiskSampleV2,
    pub engine: Option<&'a EngineSampleV2>,
    pub smart: Option<&'a SmartSnapshotV2>,
}

impl IncidentRecorder {
    pub fn new(run_dir: &Path, max_incidents: usize) -> Result<Self, String> {
        let budget = Arc::new(ArtifactBudget::new(
            u64::MAX,
            existing_artifact_bytes(run_dir)?,
        )?);
        Self::new_with_budget(run_dir, max_incidents, budget)
    }

    pub(super) fn new_with_budget(
        run_dir: &Path,
        max_incidents: usize,
        budget: Arc<ArtifactBudget>,
    ) -> Result<Self, String> {
        if !(1..=1024).contains(&max_incidents) {
            return Err("incident limit must be in 1..=1024".into());
        }
        let root = run_dir.join("incidents");
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        let mut recorder = Self {
            root,
            max_incidents,
            next_sequence: 1,
            active_alerts: BTreeMap::new(),
            active_incidents: BTreeMap::new(),
            closed_by_kind: BTreeMap::new(),
            incidents: Vec::new(),
            burst_samples: BTreeMap::new(),
            suppressed: 0,
            forensic: ForensicWorker::start()?,
            budget,
        };
        recorder.restore()?;
        Ok(recorder)
    }

    pub(super) fn record(
        &mut self,
        transitions: &[DiagnosticAlertV2],
        pre_trigger: &[DiagnosticMetricFrameV2],
        evidence: IncidentEvidence<'_>,
    ) -> Result<(), String> {
        self.drain_forensics()?;
        for alert in transitions {
            match alert.state {
                DiagnosticState::Active => {
                    if self.active_alerts.contains_key(&alert.id) {
                        continue;
                    }
                    let incident_id = if let Some(incident_id) = self.coalescing_target(alert) {
                        incident_id
                    } else {
                        if self.cooldown_active(alert) || self.incidents.len() >= self.max_incidents
                        {
                            self.record_suppressed(alert)?;
                            continue;
                        }
                        self.open_incident(alert, pre_trigger, &evidence)?
                    };
                    self.attach_alert(&incident_id, alert)?;
                }
                DiagnosticState::Recovered => {
                    let Some(incident_id) = self.active_alerts.remove(&alert.id) else {
                        continue;
                    };
                    let directory = self.incident_directory(&incident_id)?;
                    append_json_line(&directory.join("alerts.jsonl"), alert, &self.budget)?;
                    if !self
                        .active_alerts
                        .values()
                        .any(|active| active == &incident_id)
                    {
                        self.close_incident(
                            &incident_id,
                            alert,
                            evidence.process,
                            evidence.engine,
                        )?;
                    }
                }
            }
        }
        self.drain_forensics()?;
        Ok(())
    }

    pub fn append_burst(
        &mut self,
        frame: &DiagnosticMetricFrameV2,
        host: &HostSampleV2,
        process: &ProcessSampleV2,
        disk: &DiskSampleV2,
        engine: Option<&EngineSampleV2>,
    ) -> Result<u64, String> {
        self.drain_forensics()?;
        let active = self.active_incidents.keys().cloned().collect::<Vec<_>>();
        let mut dropped_records = 0_u64;
        for incident_id in active {
            let count = self.burst_samples.get(&incident_id).copied().unwrap_or(0);
            if count >= MAX_BURST_SAMPLES_PER_INCIDENT {
                continue;
            }
            let directory = self.incident_directory(&incident_id)?;
            let mut records = vec![
                (directory.join("burst-frames.jsonl"), json_line(frame)?),
                (directory.join("burst-host.jsonl"), json_line(host)?),
                (directory.join("burst-process.jsonl"), json_line(process)?),
                (directory.join("burst-disk.jsonl"), json_line(disk)?),
            ];
            if let Some(engine) = engine {
                records.push((directory.join("burst-engine.jsonl"), json_line(engine)?));
            }
            if !append_retained_json_lines(&records, &self.budget)? {
                dropped_records = dropped_records
                    .saturating_add(u64::try_from(records.len()).unwrap_or(u64::MAX));
                continue;
            }
            self.burst_samples
                .insert(incident_id, count.saturating_add(1));
        }
        Ok(dropped_records)
    }

    pub fn has_active_incident(&self) -> bool {
        !self.active_incidents.is_empty()
    }

    pub fn active_incident_count(&self) -> usize {
        self.active_incidents.len()
    }

    pub fn burst_active(&self, now_unix_millis: u64, burst_duration_millis: u64) -> bool {
        self.active_incidents.values().any(|incident| {
            now_unix_millis.saturating_sub(incident.opened_unix_millis) <= burst_duration_millis
        })
    }

    pub fn suppressed_count(&self) -> u64 {
        self.suppressed
    }

    pub fn incidents(&self) -> &[DiagnosticIncidentV2] {
        &self.incidents
    }

    pub fn finalize(&mut self) -> Result<(), String> {
        let results = self.forensic.shutdown()?;
        self.apply_forensic_results(results)?;
        for incident in &self.incidents {
            let directory = self
                .root
                .parent()
                .unwrap()
                .join(&incident.artifact_directory);
            write_checksums(&directory, &self.budget)?;
        }
        Ok(())
    }

    fn cooldown_active(&self, alert: &DiagnosticAlertV2) -> bool {
        self.closed_by_kind.get(&alert.kind).is_some_and(|closed| {
            alert.first_unix_millis.saturating_sub(*closed) < INCIDENT_COOLDOWN_MILLIS
        })
    }

    fn coalescing_target(&self, alert: &DiagnosticAlertV2) -> Option<String> {
        self.active_incidents
            .values()
            .filter(|incident| {
                alert
                    .first_unix_millis
                    .saturating_sub(incident.opened_unix_millis)
                    <= INCIDENT_COALESCE_MILLIS
            })
            .max_by_key(|incident| incident.opened_unix_millis)
            .map(|incident| incident.id.clone())
    }

    fn open_incident(
        &mut self,
        alert: &DiagnosticAlertV2,
        pre_trigger: &[DiagnosticMetricFrameV2],
        evidence: &IncidentEvidence<'_>,
    ) -> Result<String, String> {
        let host = evidence.host;
        let process = evidence.process;
        let disk = evidence.disk;
        let engine = evidence.engine;
        let smart = evidence.smart;
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| "incident sequence exhausted".to_string())?;
        let incident_id = format!("incident-{sequence:06}");
        let directory_name = format!("{sequence:06}-{}", alert.kind.replace('_', "-"));
        let directory = self.root.join(&directory_name);
        fs::create_dir(&directory).map_err(|error| error.to_string())?;
        write_json_lines(
            &directory.join("pre-trigger-samples.jsonl"),
            pre_trigger,
            &self.budget,
        )?;
        write_json(&directory.join("host.json"), host, &self.budget)?;
        write_json(&directory.join("process.json"), process, &self.budget)?;
        write_json(&directory.join("disk.json"), disk, &self.budget)?;
        write_json(
            &directory.join("cgroup.json"),
            &incident_cgroups(process),
            &self.budget,
        )?;
        write_json(
            &directory.join("filesystem.json"),
            &incident_filesystem(disk),
            &self.budget,
        )?;
        write_json(&directory.join("network.json"), &host.network, &self.budget)?;
        let mut missing_evidence = vec![
            "engine-before.json".into(),
            "engine-after.json".into(),
            "threads.json".into(),
            "threads-after.json".into(),
            "journal.txt".into(),
            "smart.json".into(),
        ];
        if let Some(engine) = engine {
            write_json(&directory.join("engine-before.json"), engine, &self.budget)?;
            missing_evidence.retain(|item| item != "engine-before.json");
            if !engine.available() {
                missing_evidence.push("engine-before snapshot unavailable".into());
            }
        }
        if let Some(smart) = smart {
            write_json(&directory.join("smart.json"), smart, &self.budget)?;
            missing_evidence.retain(|item| item != "smart.json");
            if let Some(error) = &smart.error {
                missing_evidence.push(format!("smart evidence unavailable: {error}"));
            }
        }
        let incident = DiagnosticIncidentV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            id: incident_id.clone(),
            alert_ids: Vec::new(),
            opened_unix_millis: alert.first_unix_millis,
            closed_unix_millis: None,
            candidate_cause: alert.kind.clone(),
            confidence: alert.confidence,
            artifact_directory: format!("incidents/{directory_name}"),
            missing_evidence,
        };
        self.active_incidents
            .insert(incident_id.clone(), incident.clone());
        self.incidents.push(incident);
        self.burst_samples.insert(incident_id.clone(), 0);
        if let Err(error) = self.forensic.try_capture(ForensicJob {
            incident_id: incident_id.clone(),
            directory,
            stage: ForensicStage::Before,
            process: process.clone(),
            budget: Arc::clone(&self.budget),
        }) {
            self.add_missing_evidence(&incident_id, format!("forensic capture: {error}"))?;
        }
        Ok(incident_id)
    }

    fn attach_alert(&mut self, incident_id: &str, alert: &DiagnosticAlertV2) -> Result<(), String> {
        let directory = self.incident_directory(incident_id)?;
        let updated = {
            let incident = self
                .active_incidents
                .get_mut(incident_id)
                .ok_or_else(|| format!("active incident `{incident_id}` disappeared"))?;
            if !incident.alert_ids.contains(&alert.id) {
                incident.alert_ids.push(alert.id.clone());
                incident.alert_ids.sort();
            }
            incident.confidence = stronger_confidence(incident.confidence, alert.confidence);
            incident.validate()?;
            atomic_json(&directory.join("incident.json"), incident, &self.budget)?;
            incident.clone()
        };
        append_json_line(&directory.join("alerts.jsonl"), alert, &self.budget)?;
        self.active_alerts
            .insert(alert.id.clone(), incident_id.to_string());
        self.replace_history(updated);
        Ok(())
    }

    fn close_incident(
        &mut self,
        incident_id: &str,
        alert: &DiagnosticAlertV2,
        process: &ProcessSampleV2,
        engine: Option<&EngineSampleV2>,
    ) -> Result<(), String> {
        let directory = self.incident_directory(incident_id)?;
        let mut incident = self
            .active_incidents
            .remove(incident_id)
            .ok_or_else(|| format!("active incident `{incident_id}` disappeared"))?;
        incident.closed_unix_millis = Some(alert.last_unix_millis);
        if let Some(engine) = engine {
            write_json(&directory.join("engine-after.json"), engine, &self.budget)?;
            incident
                .missing_evidence
                .retain(|item| item != "engine-after.json");
            if !engine.available() {
                incident
                    .missing_evidence
                    .push("engine-after snapshot unavailable".into());
            }
        }
        incident.validate()?;
        atomic_json(&directory.join("incident.json"), &incident, &self.budget)?;
        self.closed_by_kind
            .insert(incident.candidate_cause.clone(), alert.last_unix_millis);
        self.burst_samples.remove(incident_id);
        self.replace_history(incident);
        if let Err(error) = self.forensic.try_capture(ForensicJob {
            incident_id: incident_id.to_string(),
            directory,
            stage: ForensicStage::After,
            process: process.clone(),
            budget: Arc::clone(&self.budget),
        }) {
            self.add_missing_evidence(incident_id, format!("forensic after capture: {error}"))?;
        }
        Ok(())
    }

    fn incident_directory(&self, incident_id: &str) -> Result<PathBuf, String> {
        let incident = self
            .active_incidents
            .get(incident_id)
            .or_else(|| self.incidents.iter().find(|item| item.id == incident_id))
            .ok_or_else(|| format!("incident `{incident_id}` not found"))?;
        Ok(self
            .root
            .parent()
            .expect("incident root has a run parent")
            .join(&incident.artifact_directory))
    }

    fn replace_history(&mut self, incident: DiagnosticIncidentV2) {
        if let Some(entry) = self
            .incidents
            .iter_mut()
            .find(|item| item.id == incident.id)
        {
            *entry = incident;
        }
    }

    fn record_suppressed(&mut self, alert: &DiagnosticAlertV2) -> Result<(), String> {
        self.suppressed = self.suppressed.saturating_add(1);
        append_json_line(
            &self.root.join("suppressed-alerts.jsonl"),
            alert,
            &self.budget,
        )
    }

    fn drain_forensics(&mut self) -> Result<(), String> {
        let results = self.forensic.drain();
        self.apply_forensic_results(results)
    }

    fn apply_forensic_results(&mut self, results: Vec<ForensicResult>) -> Result<(), String> {
        for result in results {
            let index = self
                .incidents
                .iter()
                .position(|incident| incident.id == result.incident_id)
                .ok_or_else(|| {
                    format!(
                        "forensic result references unknown incident `{}`",
                        result.incident_id
                    )
                })?;
            let incident = &mut self.incidents[index];
            for completed in &result.completed {
                incident
                    .missing_evidence
                    .retain(|missing| missing != completed);
            }
            for missing in result.missing {
                if !incident.missing_evidence.contains(&missing) {
                    incident.missing_evidence.push(missing);
                }
            }
            incident.missing_evidence.sort();
            incident.validate()?;
            let updated = incident.clone();
            if self.active_incidents.contains_key(&result.incident_id) {
                self.active_incidents
                    .insert(result.incident_id.clone(), updated.clone());
            }
            let directory = self
                .root
                .parent()
                .expect("incident root has a run parent")
                .join(&updated.artifact_directory);
            atomic_json(&directory.join("incident.json"), &updated, &self.budget)?;
        }
        Ok(())
    }

    fn add_missing_evidence(&mut self, incident_id: &str, missing: String) -> Result<(), String> {
        self.apply_forensic_results(vec![ForensicResult {
            incident_id: incident_id.to_string(),
            completed: Vec::new(),
            missing: vec![missing],
        }])
    }

    fn restore(&mut self) -> Result<(), String> {
        let mut directories = fs::read_dir(&self.root)
            .map_err(|error| error.to_string())?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .collect::<Vec<_>>();
        directories.sort_by_key(|entry| entry.file_name());
        for directory in directories {
            if let Some(sequence) = directory
                .file_name()
                .to_string_lossy()
                .split('-')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
            {
                self.next_sequence = self.next_sequence.max(sequence.saturating_add(1));
            }
            let path = directory.path().join("incident.json");
            if !path.is_file() {
                continue;
            }
            let mut incident: DiagnosticIncidentV2 =
                serde_json::from_slice(&fs::read(&path).map_err(|error| error.to_string())?)
                    .map_err(|error| format!("restore {}: {error}", path.display()))?;
            incident.validate()?;
            let before_missing = incident.missing_evidence.clone();
            reconcile_existing_evidence(&mut incident, &directory.path());
            if incident.missing_evidence != before_missing {
                atomic_json(&path, &incident, &self.budget)?;
            }
            if let Some(sequence) = incident
                .id
                .strip_prefix("incident-")
                .and_then(|value| value.parse::<u64>().ok())
            {
                self.next_sequence = self.next_sequence.max(sequence.saturating_add(1));
            }
            if incident.closed_unix_millis.is_none() {
                for alert_id in &incident.alert_ids {
                    self.active_alerts
                        .insert(alert_id.clone(), incident.id.clone());
                }
                self.active_incidents
                    .insert(incident.id.clone(), incident.clone());
                self.burst_samples.insert(
                    incident.id.clone(),
                    count_lines(&directory.path().join("burst-frames.jsonl"))
                        .min(MAX_BURST_SAMPLES_PER_INCIDENT),
                );
            } else if let Some(closed) = incident.closed_unix_millis {
                self.closed_by_kind
                    .entry(incident.candidate_cause.clone())
                    .and_modify(|value| *value = (*value).max(closed))
                    .or_insert(closed);
            }
            self.incidents.push(incident);
        }
        self.suppressed = count_lines(&self.root.join("suppressed-alerts.jsonl"));
        Ok(())
    }
}

fn reconcile_existing_evidence(incident: &mut DiagnosticIncidentV2, directory: &Path) {
    for artifact in [
        "engine-before.json",
        "engine-after.json",
        "threads.json",
        "threads-after.json",
        "journal.txt",
        "smart.json",
    ] {
        if directory.join(artifact).is_file() {
            incident.missing_evidence.retain(|missing| {
                missing != artifact && !missing.starts_with(&format!("{artifact}:"))
            });
        }
    }
    incident.missing_evidence.sort();
}

#[derive(Serialize)]
struct IncidentFilesystem<'a> {
    device: &'a str,
    total_bytes: u64,
    free_bytes: u64,
    available_bytes: u64,
    total_inodes: u64,
    free_inodes: u64,
    read_only: bool,
    mount_options: &'a [String],
    mount_options_error: &'a Option<String>,
    error: &'a Option<String>,
}

fn incident_filesystem(disk: &DiskSampleV2) -> IncidentFilesystem<'_> {
    IncidentFilesystem {
        device: &disk.device,
        total_bytes: disk.filesystem_total_bytes,
        free_bytes: disk.filesystem_free_bytes,
        available_bytes: disk.filesystem_available_bytes,
        total_inodes: disk.filesystem_total_inodes,
        free_inodes: disk.filesystem_free_inodes,
        read_only: disk.filesystem_read_only,
        mount_options: &disk.filesystem_mount_options,
        mount_options_error: &disk.filesystem_mount_options_error,
        error: &disk.error,
    }
}

fn incident_cgroups(process: &ProcessSampleV2) -> BTreeMap<String, serde_json::Value> {
    process
        .roles
        .iter()
        .map(|(role, owner)| {
            (
                role.clone(),
                serde_json::json!({
                    "path": owner.cgroup_path,
                    "snapshot": owner.cgroup,
                }),
            )
        })
        .collect()
}

fn execute_forensic_job(job: ForensicJob) -> ForensicResult {
    let deadline = Instant::now() + FORENSIC_CAPTURE_TIMEOUT;
    let mut completed = Vec::new();
    let mut missing = Vec::new();
    let threads_name = job.stage.threads_name();
    match capture_threads(&job.process, deadline)
        .and_then(|threads| write_json(&job.directory.join(threads_name), &threads, &job.budget))
    {
        Ok(()) => completed.push(threads_name.to_string()),
        Err(error) => missing.push(format!("{threads_name}: {error}")),
    }
    if let Some(journal_name) = job.stage.journal_name() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match capture_kernel_journal(remaining).and_then(|journal| {
            write_bytes(&job.directory.join(journal_name), &journal, &job.budget)
        }) {
            Ok(()) => completed.push(journal_name.to_string()),
            Err(error) => missing.push(format!("{journal_name}: {error}")),
        }
    }
    ForensicResult {
        incident_id: job.incident_id,
        completed,
        missing,
    }
}

fn count_lines(path: &Path) -> u64 {
    use std::io::BufRead;
    File::open(path).map_or(0, |file| {
        std::io::BufReader::new(file)
            .lines()
            .fold(0_u64, |count, line| {
                count.saturating_add(u64::from(line.is_ok()))
            })
    })
}

#[derive(Debug, Serialize)]
struct ThreadInventory {
    format: u32,
    truncated: bool,
    roles: BTreeMap<String, Vec<ThreadSnapshot>>,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ThreadSnapshot {
    tid: u32,
    state: String,
    user_ticks: u64,
    system_ticks: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
    wchan: Option<String>,
    kernel_stack: Option<Vec<String>>,
}

fn capture_threads(
    process: &ProcessSampleV2,
    deadline: Instant,
) -> Result<ThreadInventory, String> {
    const MAX_THREADS: usize = 4_096;
    let mut roles = BTreeMap::new();
    let mut errors = Vec::new();
    let mut remaining = MAX_THREADS;
    let mut truncated = false;
    for (role, owner) in &process.roles {
        if Instant::now() >= deadline {
            truncated = true;
            errors.push("thread capture deadline expired".into());
            break;
        }
        if remaining == 0 {
            truncated = true;
            break;
        }
        let directory = PathBuf::from(format!("/proc/{}/task", owner.pid));
        let mut tids = fs::read_dir(&directory)
            .map_err(|error| format!("read {}: {error}", directory.display()))?
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u32>().ok())
            .collect::<Vec<_>>();
        tids.sort_unstable();
        if tids.len() > remaining {
            tids.truncate(remaining);
            truncated = true;
        }
        remaining = remaining.saturating_sub(tids.len());
        let mut snapshots = Vec::with_capacity(tids.len());
        for tid in tids {
            if Instant::now() >= deadline {
                truncated = true;
                errors.push(format!("{role}: thread capture deadline expired"));
                break;
            }
            match read_thread(owner.pid, tid) {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => errors.push(format!("{role}/{tid}: {error}")),
            }
        }
        roles.insert(role.clone(), snapshots);
    }
    Ok(ThreadInventory {
        format: DIAGNOSTIC_FORMAT_V2,
        truncated,
        roles,
        errors,
    })
}

fn read_thread(pid: u32, tid: u32) -> Result<ThreadSnapshot, String> {
    let root = PathBuf::from(format!("/proc/{pid}/task/{tid}"));
    let stat = fs::read_to_string(root.join("stat")).map_err(|error| error.to_string())?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| "thread stat has no command terminator".to_string())?;
    let fields = stat[end + 1..].split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() <= 12 {
        return Err("thread stat is truncated".into());
    }
    let state = fields[0].to_string();
    let user_ticks = fields[11]
        .parse::<u64>()
        .map_err(|_| "invalid thread user ticks".to_string())?;
    let system_ticks = fields[12]
        .parse::<u64>()
        .map_err(|_| "invalid thread system ticks".to_string())?;
    let status = fs::read_to_string(root.join("status")).unwrap_or_default();
    let voluntary_context_switches = named_status_u64(&status, "voluntary_ctxt_switches");
    let involuntary_context_switches = named_status_u64(&status, "nonvoluntary_ctxt_switches");
    let wchan = fs::read_to_string(root.join("wchan"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let kernel_stack = File::open(root.join("stack")).ok().and_then(|file| {
        let mut bytes = Vec::new();
        file.take(MAX_KERNEL_STACK_BYTES)
            .read_to_end(&mut bytes)
            .ok()?;
        let text = String::from_utf8_lossy(&bytes);
        let lines = text
            .lines()
            .take(64)
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        (!lines.is_empty()).then_some(lines)
    });
    Ok(ThreadSnapshot {
        tid,
        state,
        user_ticks,
        system_ticks,
        voluntary_context_switches,
        involuntary_context_switches,
        wchan,
        kernel_stack,
    })
}

fn named_status_u64(status: &str, name: &str) -> u64 {
    status
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key == name)
                .then(|| value.split_ascii_whitespace().next()?.parse().ok())
                .flatten()
        })
        .unwrap_or(0)
}

fn stronger_confidence(
    left: super::DiagnosticConfidence,
    right: super::DiagnosticConfidence,
) -> super::DiagnosticConfidence {
    use super::DiagnosticConfidence::{Confirmed, Possible, Probable};
    match (left, right) {
        (Confirmed, _) | (_, Confirmed) => Confirmed,
        (Probable, _) | (_, Probable) => Probable,
        (Possible, Possible) => Possible,
    }
}

fn capture_kernel_journal(timeout: Duration) -> Result<Vec<u8>, String> {
    if timeout.is_zero() {
        return Err("forensic capture deadline expired".into());
    }
    let mut child = Command::new("journalctl")
        .args([
            "--no-pager",
            "--quiet",
            "-k",
            "--priority=warning..alert",
            "--lines=256",
            "--since=-10min",
            "--output=short-monotonic",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("start journalctl: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "journalctl stdout is unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "journalctl stderr is unavailable".to_string())?;
    let stdout_reader = thread::spawn(move || read_limited(stdout, MAX_JOURNAL_BYTES));
    let stderr_reader = thread::spawn(move || read_limited(stderr, 16 * 1024));
    let started = Instant::now();
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (Some(status), false),
            Ok(None) if started.elapsed() < timeout => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                break (child.wait().ok(), true);
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("wait journalctl: {error}"));
            }
        }
    };
    let (mut stdout, stdout_truncated) = stdout_reader
        .join()
        .map_err(|_| "journalctl stdout reader panicked".to_string())??;
    let (stderr, _) = stderr_reader
        .join()
        .map_err(|_| "journalctl stderr reader panicked".to_string())??;
    if timed_out {
        return Err(format!("journalctl exceeded {} ms", timeout.as_millis()));
    }
    let status = status.ok_or_else(|| "journalctl exit status is unavailable".to_string())?;
    if !status.success() {
        return Err(format!(
            "journalctl exited with {status}: {}",
            String::from_utf8_lossy(&stderr).trim()
        ));
    }
    if stdout_truncated {
        stdout.extend_from_slice(b"\n[truncated at 262144 bytes]\n");
    }
    Ok(stdout)
}

fn read_limited(mut reader: impl Read, limit: usize) -> Result<(Vec<u8>, bool), String> {
    let read_limit = u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX);
    let mut bytes = Vec::with_capacity(limit.min(16 * 1024));
    reader
        .by_ref()
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    let truncated = bytes.len() > limit;
    bytes.truncate(limit);
    Ok((bytes, truncated))
}

fn existing_artifact_bytes(root: &Path) -> Result<u64, String> {
    if !root.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| format!("stat {}: {error}", entry.path().display()))?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| "artifact size overflow".to_string())?;
            }
        }
    }
    Ok(total)
}

fn write_json<T: Serialize>(path: &Path, value: &T, budget: &ArtifactBudget) -> Result<(), String> {
    let mut payload = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    payload.push(b'\n');
    budget.reserve(payload.len())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&payload)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn write_bytes(path: &Path, value: &[u8], budget: &ArtifactBudget) -> Result<(), String> {
    budget.reserve(value.len())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(value).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn write_json_lines<T: Serialize>(
    path: &Path,
    values: &[T],
    budget: &ArtifactBudget,
) -> Result<(), String> {
    let mut payload = Vec::new();
    for value in values {
        serde_json::to_writer(&mut payload, value).map_err(|error| error.to_string())?;
        payload.push(b'\n');
    }
    budget.reserve(payload.len())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&payload)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn append_json_line<T: Serialize>(
    path: &Path,
    value: &T,
    budget: &ArtifactBudget,
) -> Result<(), String> {
    let mut payload = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    payload.push(b'\n');
    budget.reserve(payload.len())?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&payload)
        .map_err(|error| error.to_string())?;
    file.flush().map_err(|error| error.to_string())
}

fn json_line<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let mut payload = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    payload.push(b'\n');
    Ok(payload)
}

fn append_retained_json_lines(
    records: &[(PathBuf, Vec<u8>)],
    budget: &ArtifactBudget,
) -> Result<bool, String> {
    let bytes = records
        .iter()
        .try_fold(0_usize, |total, (_, payload)| {
            total.checked_add(payload.len())
        })
        .ok_or_else(|| "artifact size overflow".to_string())?;
    if !budget.reserve_retained(bytes)? {
        return Ok(false);
    }
    for (path, payload) in records {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(payload).map_err(|error| error.to_string())?;
        file.flush().map_err(|error| error.to_string())?;
    }
    Ok(true)
}

fn atomic_json<T: Serialize>(
    path: &Path,
    value: &T,
    budget: &ArtifactBudget,
) -> Result<(), String> {
    let mut payload = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    payload.push(b'\n');
    let reservation = budget.reserve_replacement(path, payload.len())?;
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
    {
        Ok(file) => file,
        Err(error) => {
            budget.cancel_replacement(reservation);
            return Err(error.to_string());
        }
    };
    let result = (|| -> Result<(), String> {
        file.write_all(&payload)
            .map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, path).map_err(|error| error.to_string())
    })();
    if result.is_ok() {
        budget.commit_replacement(reservation);
    } else {
        let _ = fs::remove_file(&temporary);
        budget.cancel_replacement(reservation);
    }
    result
}

fn write_checksums(directory: &Path, budget: &ArtifactBudget) -> Result<(), String> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| error.to_string())?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .filter(|entry| entry.file_name() != "SHA256SUMS")
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    let mut output = Vec::new();
    for entry in entries {
        let mut file = File::open(entry.path()).map_err(|error| error.to_string())?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        writeln!(
            output,
            "{:x}  {}",
            digest.finalize(),
            entry.file_name().to_string_lossy()
        )
        .map_err(|error| error.to_string())?;
    }
    budget.reserve(output.len())?;
    let mut file =
        File::create_new(directory.join("SHA256SUMS")).map_err(|error| error.to_string())?;
    file.write_all(&output).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::diagnostics::{
        DiagnosticConfidence, DiagnosticSeverity, EvidenceSignal, HeartbeatSnapshot,
        SemanticProgressSnapshot,
    };

    fn alert(id: &str, kind: &str, state: DiagnosticState, now: u64) -> DiagnosticAlertV2 {
        DiagnosticAlertV2 {
            format: 2,
            id: id.into(),
            kind: kind.into(),
            severity: DiagnosticSeverity::Incident,
            confidence: DiagnosticConfidence::Confirmed,
            state,
            first_unix_millis: 1,
            last_unix_millis: now,
            first_frame_sequence: 1,
            last_frame_sequence: now,
            evidence: vec![EvidenceSignal {
                name: "progress".into(),
                observed: 0.0,
                expected: Some(1.0),
                unit: "epoch".into(),
                detail: "frozen".into(),
            }],
            counter_evidence: Vec::new(),
        }
    }

    fn frame() -> DiagnosticMetricFrameV2 {
        DiagnosticMetricFrameV2 {
            format: 2,
            sequence: 1,
            monotonic_millis: 1,
            unix_millis: 1,
            boot_id: "boot".into(),
            run_id: "run".into(),
            heartbeats: HeartbeatSnapshot::default(),
            progress: SemanticProgressSnapshot {
                format: 2,
                sequence: 1,
                phase_epoch: 1,
                phase: "clients-2".into(),
                phase_started_unix_millis: 1,
                workload_epoch: 0,
                workload_units: 0,
                last_workload_progress_unix_millis: 1,
                operation_epoch: 0,
                active_operation: None,
                last_operation_progress_unix_millis: 1,
                planned_silence: None,
            },
            counters: BTreeMap::new(),
            gauges: BTreeMap::new(),
        }
    }

    fn samples() -> (HostSampleV2, ProcessSampleV2, DiskSampleV2) {
        let boot = super::super::collectors::boot_id().unwrap();
        (
            super::super::collectors::collect_host(1, 1, 1, &boot),
            ProcessSampleV2 {
                format: 2,
                sequence: 1,
                monotonic_millis: 1,
                unix_millis: 1,
                roles: BTreeMap::new(),
                errors: Vec::new(),
            },
            super::super::collectors::collect_disk(1, 1, 1, Path::new("/tmp")),
        )
    }

    #[test]
    fn active_incident_captures_pretrigger_and_final_checksums() {
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = IncidentRecorder::new(directory.path(), 2).unwrap();
        let alert = alert(
            "workload-stall-1",
            "workload_stall",
            DiagnosticState::Active,
            1,
        );
        let frame = frame();
        let (host, process, disk) = samples();
        let smart = SmartSnapshotV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            unix_millis: 1,
            device: Some("test-device".into()),
            smartctl_status: Some(0),
            data: Some(serde_json::json!({"smart_status": {"passed": true}})),
            error: None,
        };
        recorder
            .record(
                &[alert],
                &[frame],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: Some(&smart),
                },
            )
            .unwrap();
        recorder.finalize().unwrap();
        let incident = &recorder.incidents()[0];
        let incident_dir = directory.path().join(&incident.artifact_directory);
        for expected in [
            "incident.json",
            "host.json",
            "process.json",
            "disk.json",
            "cgroup.json",
            "filesystem.json",
            "network.json",
            "smart.json",
            "threads.json",
            "SHA256SUMS",
        ] {
            assert!(
                incident_dir.join(expected).is_file(),
                "missing forensic artifact {expected}"
            );
        }
        assert!(
            incident_dir.join("journal.txt").is_file()
                || incident
                    .missing_evidence
                    .iter()
                    .any(|missing| missing.starts_with("journal.txt:"))
        );
        assert!(!incident
            .missing_evidence
            .iter()
            .any(|missing| missing == "threads.json" || missing == "smart.json"));
    }

    #[test]
    fn causal_alerts_coalesce_burst_and_quota_suppression_never_abort_recorder() {
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = IncidentRecorder::new(directory.path(), 1).unwrap();
        let frame = frame();
        let (host, process, disk) = samples();
        let root = alert(
            "workload-stall-1",
            "workload_stall",
            DiagnosticState::Active,
            1,
        );
        let cause = alert(
            "storage-bound-1",
            "storage_bound_engine",
            DiagnosticState::Active,
            1,
        );
        recorder
            .record(
                &[root.clone(), cause.clone()],
                std::slice::from_ref(&frame),
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .unwrap();
        assert_eq!(recorder.incidents().len(), 1);
        assert_eq!(recorder.incidents()[0].alert_ids.len(), 2);
        assert_eq!(recorder.active_incident_count(), 1);
        recorder
            .append_burst(&frame, &host, &process, &disk, None)
            .unwrap();

        let mut root_recovered = root;
        root_recovered.state = DiagnosticState::Recovered;
        root_recovered.last_unix_millis = 2;
        root_recovered.last_frame_sequence = 2;
        recorder
            .record(
                &[root_recovered],
                &[],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .unwrap();
        assert_eq!(recorder.active_incident_count(), 1);
        let mut cause_recovered = cause;
        cause_recovered.state = DiagnosticState::Recovered;
        cause_recovered.last_unix_millis = 2;
        cause_recovered.last_frame_sequence = 2;
        recorder
            .record(
                &[cause_recovered],
                &[],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .unwrap();
        assert_eq!(recorder.active_incident_count(), 0);
        assert_eq!(recorder.incidents()[0].closed_unix_millis, Some(2));

        let retry = alert(
            "workload-stall-3",
            "workload_stall",
            DiagnosticState::Active,
            3,
        );
        recorder
            .record(
                &[retry],
                &[],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .unwrap();
        assert_eq!(recorder.suppressed_count(), 1);
        assert_eq!(recorder.incidents().len(), 1);
        let incident_dir = directory
            .path()
            .join(&recorder.incidents()[0].artifact_directory);
        assert!(incident_dir.join("burst-frames.jsonl").is_file());
        assert!(directory
            .path()
            .join("incidents/suppressed-alerts.jsonl")
            .is_file());
        let restored = IncidentRecorder::new(directory.path(), 1).unwrap();
        assert_eq!(restored.incidents().len(), 1);
        assert_eq!(restored.suppressed_count(), 1);
        assert_eq!(restored.active_incident_count(), 0);
    }

    #[test]
    fn incident_writes_share_one_fail_closed_artifact_budget() {
        let directory = tempfile::tempdir().unwrap();
        let budget = Arc::new(ArtifactBudget::new(1_024, 900).unwrap());
        let mut recorder =
            IncidentRecorder::new_with_budget(directory.path(), 2, Arc::clone(&budget)).unwrap();
        let frame = frame();
        let (host, process, disk) = samples();
        let error = recorder
            .record(
                &[alert(
                    "workload-stall-1",
                    "workload_stall",
                    DiagnosticState::Active,
                    1,
                )],
                &[frame],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .expect_err("incident evidence must not exceed the shared quota");
        assert!(error.contains("quota exhausted"));
        assert!(budget.used() <= budget.limit());
    }

    #[test]
    fn repetitive_retention_cannot_consume_finalization_reserve() {
        let limit = 64 * 1024 * 1024;
        let budget = ArtifactBudget::new(limit, 60 * 1024 * 1024).unwrap();
        assert_eq!(budget.retained_limit(), 60 * 1024 * 1024);
        assert!(!budget.reserve_retained(1).unwrap());
        budget.reserve(1).unwrap();
        assert_eq!(budget.used(), 60 * 1024 * 1024 + 1);
    }

    #[test]
    fn replacement_accounting_tracks_staging_and_releases_previous_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("status.json");
        fs::write(&path, vec![0_u8; 128]).unwrap();
        let budget = ArtifactBudget::new(1_024, 128).unwrap();

        let reservation = budget.reserve_replacement(&path, 96).unwrap();
        assert_eq!(budget.used(), 224);
        fs::write(&path, vec![0_u8; 96]).unwrap();
        budget.commit_replacement(reservation);
        assert_eq!(budget.used(), 96);

        let reservation = budget.reserve_replacement(&path, 160).unwrap();
        assert_eq!(budget.used(), 256);
        budget.cancel_replacement(reservation);
        assert_eq!(budget.used(), 96);
    }

    #[test]
    fn active_incident_resumes_and_reconciles_completed_forensics() {
        let directory = tempfile::tempdir().unwrap();
        let frame = frame();
        let (host, process, disk) = samples();
        let root = alert(
            "workload-stall-1",
            "workload_stall",
            DiagnosticState::Active,
            1,
        );
        {
            let mut recorder = IncidentRecorder::new(directory.path(), 2).unwrap();
            recorder
                .record(
                    std::slice::from_ref(&root),
                    std::slice::from_ref(&frame),
                    IncidentEvidence {
                        host: &host,
                        process: &process,
                        disk: &disk,
                        engine: None,
                        smart: None,
                    },
                )
                .unwrap();
        }

        let mut resumed = IncidentRecorder::new(directory.path(), 2).unwrap();
        assert_eq!(resumed.active_incident_count(), 1);
        assert!(!resumed.incidents()[0]
            .missing_evidence
            .iter()
            .any(|missing| missing == "threads.json"));
        let mut recovered = root;
        recovered.state = DiagnosticState::Recovered;
        recovered.last_unix_millis = 2;
        recovered.last_frame_sequence = 2;
        resumed
            .record(
                &[recovered],
                &[],
                IncidentEvidence {
                    host: &host,
                    process: &process,
                    disk: &disk,
                    engine: None,
                    smart: None,
                },
            )
            .unwrap();
        resumed.finalize().unwrap();
        assert_eq!(resumed.active_incident_count(), 0);
        assert_eq!(resumed.incidents()[0].closed_unix_millis, Some(2));
    }

    #[test]
    fn corrupted_incident_metadata_fails_closed_on_restart() {
        let directory = tempfile::tempdir().unwrap();
        let frame = frame();
        let (host, process, disk) = samples();
        let incident_path = {
            let mut recorder = IncidentRecorder::new(directory.path(), 2).unwrap();
            recorder
                .record(
                    &[alert(
                        "workload-stall-1",
                        "workload_stall",
                        DiagnosticState::Active,
                        1,
                    )],
                    &[frame],
                    IncidentEvidence {
                        host: &host,
                        process: &process,
                        disk: &disk,
                        engine: None,
                        smart: None,
                    },
                )
                .unwrap();
            directory
                .path()
                .join(&recorder.incidents()[0].artifact_directory)
                .join("incident.json")
        };
        fs::write(&incident_path, b"{truncated").unwrap();
        let error = IncidentRecorder::new(directory.path(), 2)
            .err()
            .expect("corrupt incident metadata must stop evidence reuse");
        assert!(error.contains("restore"));
    }
}
