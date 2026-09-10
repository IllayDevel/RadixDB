use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

const LIVE_SAMPLE_INTERVAL: u64 = 10;
const LIVE_SAMPLE_FILE: &str = "semantic-progress.json";

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageSnapshot {
    pub total_bytes: u64,
    pub wal_bytes: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub catalog_bytes: u64,
    pub manifest_bytes: u64,
    pub staging_bytes: u64,
    pub files: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TelemetrySample {
    pub elapsed_millis: u64,
    pub semantic_progress: u64,
    pub pid: u32,
    pub rss_bytes: u64,
    pub swap_bytes: u64,
    pub threads: u64,
    pub open_fds: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub storage: StorageSnapshot,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TelemetryReport {
    pub samples_taken: u64,
    pub peak_rss_bytes: u64,
    pub peak_swap_bytes: u64,
    pub peak_threads: u64,
    pub peak_open_fds: u64,
    pub peak_storage_bytes: u64,
    pub last: Option<TelemetrySample>,
    pub bounded_history: Vec<TelemetrySample>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LayerTelemetryReport {
    pub samples_taken: u64,
    pub peak_rss_bytes: u64,
    pub peak_swap_bytes: u64,
    pub peak_threads: u64,
    pub peak_open_fds: u64,
    pub read_bytes_delta: u64,
    pub write_bytes_delta: u64,
    pub storage_growth_bytes: i64,
    pub write_amplification_bytes_per_operation: f64,
    pub first: Option<TelemetrySample>,
    pub last: Option<TelemetrySample>,
}

impl LayerTelemetryReport {
    pub fn finalize(&mut self, semantic_operations: u64) {
        if let (Some(first), Some(last)) = (&self.first, &self.last) {
            self.read_bytes_delta = last.read_bytes.saturating_sub(first.read_bytes);
            self.write_bytes_delta = last.write_bytes.saturating_sub(first.write_bytes);
            self.storage_growth_bytes =
                signed_delta(last.storage.total_bytes, first.storage.total_bytes);
        }
        self.write_amplification_bytes_per_operation =
            self.write_bytes_delta as f64 / semantic_operations.max(1) as f64;
    }
}

pub fn merge_reports(
    prefix: Option<&TelemetryReport>,
    mut current: TelemetryReport,
) -> TelemetryReport {
    let Some(prefix) = prefix else {
        return current;
    };
    let elapsed_offset = prefix
        .last
        .as_ref()
        .map(|sample| sample.elapsed_millis)
        .unwrap_or(0);
    let progress_offset = prefix
        .last
        .as_ref()
        .map(|sample| sample.semantic_progress)
        .unwrap_or(0);
    let shift = |sample: &mut TelemetrySample| {
        sample.elapsed_millis = sample.elapsed_millis.saturating_add(elapsed_offset);
        sample.semantic_progress = sample.semantic_progress.saturating_add(progress_offset);
    };
    if let Some(last) = current.last.as_mut() {
        shift(last);
    }
    for sample in &mut current.bounded_history {
        shift(sample);
    }
    let mut history = prefix.bounded_history.clone();
    history.extend(current.bounded_history);
    if history.len() > 256 {
        history.drain(..history.len() - 256);
    }
    TelemetryReport {
        samples_taken: prefix.samples_taken.saturating_add(current.samples_taken),
        peak_rss_bytes: prefix.peak_rss_bytes.max(current.peak_rss_bytes),
        peak_swap_bytes: prefix.peak_swap_bytes.max(current.peak_swap_bytes),
        peak_threads: prefix.peak_threads.max(current.peak_threads),
        peak_open_fds: prefix.peak_open_fds.max(current.peak_open_fds),
        peak_storage_bytes: prefix.peak_storage_bytes.max(current.peak_storage_bytes),
        last: current.last.or_else(|| prefix.last.clone()),
        bounded_history: history,
    }
}

#[derive(Default)]
struct TelemetryState {
    report: TelemetryReport,
    layer: LayerTelemetryReport,
    retained: VecDeque<TelemetrySample>,
}

pub struct Telemetry {
    pid: Arc<AtomicU32>,
    progress: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<TelemetryState>>,
    data_root: PathBuf,
    live_path: PathBuf,
    started: Instant,
    worker: Option<JoinHandle<()>>,
}

impl Telemetry {
    pub fn start(data_root: PathBuf) -> Self {
        let pid = Arc::new(AtomicU32::new(0));
        let progress = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(TelemetryState::default()));
        let started = Instant::now();
        let worker_pid = Arc::clone(&pid);
        let worker_progress = Arc::clone(&progress);
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let worker_data_root = data_root.clone();
        let live_path = data_root
            .parent()
            .unwrap_or(data_root.as_path())
            .join(LIVE_SAMPLE_FILE);
        let worker_live_path = live_path.clone();
        let worker_started = started;
        let worker = thread::Builder::new()
            .name("epoch-chaos-telemetry".to_string())
            .spawn(move || {
                let mut samples_since_live_write = 0_u64;
                while !worker_stop.load(Ordering::Acquire) {
                    let current_pid = worker_pid.load(Ordering::Acquire);
                    let sample = capture(
                        current_pid,
                        worker_progress.load(Ordering::Acquire),
                        worker_started.elapsed(),
                        &worker_data_root,
                    );
                    if let Ok(sample) = sample {
                        {
                            let mut guard = worker_state
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            record_sample(&mut guard, sample.clone());
                        }
                        samples_since_live_write = samples_since_live_write.saturating_add(1);
                        if samples_since_live_write >= LIVE_SAMPLE_INTERVAL {
                            let _ = write_live_sample(&worker_live_path, &sample);
                            samples_since_live_write = 0;
                        }
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            })
            .expect("spawn epoch chaos telemetry");
        Self {
            pid,
            progress,
            stop,
            state,
            data_root,
            live_path,
            started,
            worker: Some(worker),
        }
    }

    pub fn set_pid(&self, pid: u32) {
        self.pid.store(pid, Ordering::Release);
    }

    pub fn clear_pid(&self) {
        self.pid.store(0, Ordering::Release);
    }

    pub fn progress_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.progress)
    }

    pub fn begin_layer(&self) -> Result<(), String> {
        {
            let mut guard = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.layer = LayerTelemetryReport::default();
        }
        self.sample_now()
    }

    pub fn end_layer(&self, semantic_operations: u64) -> Result<LayerTelemetryReport, String> {
        self.sample_now()?;
        let mut report = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .layer
            .clone();
        report.finalize(semantic_operations);
        Ok(report)
    }

    pub fn report(&self) -> TelemetryReport {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .report
            .clone()
    }

    fn sample_now(&self) -> Result<(), String> {
        let sample = capture(
            self.pid.load(Ordering::Acquire),
            self.progress.load(Ordering::Acquire),
            self.started.elapsed(),
            &self.data_root,
        )?;
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        record_sample(&mut guard, sample.clone());
        drop(guard);
        write_live_sample(&self.live_path, &sample)?;
        Ok(())
    }

    pub fn stop(mut self) -> TelemetryReport {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        self.report()
    }
}

fn write_live_sample(path: &Path, sample: &TelemetrySample) -> Result<(), String> {
    let bytes = serde_json::to_vec(sample).map_err(|error| error.to_string())?;
    fs::write(path, bytes).map_err(|error| error.to_string())
}

fn record_sample(state: &mut TelemetryState, sample: TelemetrySample) {
    state.report.samples_taken += 1;
    state.report.peak_rss_bytes = state.report.peak_rss_bytes.max(sample.rss_bytes);
    state.report.peak_swap_bytes = state.report.peak_swap_bytes.max(sample.swap_bytes);
    state.report.peak_threads = state.report.peak_threads.max(sample.threads);
    state.report.peak_open_fds = state.report.peak_open_fds.max(sample.open_fds);
    state.report.peak_storage_bytes = state
        .report
        .peak_storage_bytes
        .max(sample.storage.total_bytes);
    state.report.last = Some(sample.clone());
    if state.report.samples_taken == 1 || state.report.samples_taken.is_multiple_of(10) {
        if state.retained.len() == 256 {
            state.retained.pop_front();
        }
        state.retained.push_back(sample.clone());
        state.report.bounded_history = state.retained.iter().cloned().collect();
    }

    state.layer.samples_taken += 1;
    state.layer.peak_rss_bytes = state.layer.peak_rss_bytes.max(sample.rss_bytes);
    state.layer.peak_swap_bytes = state.layer.peak_swap_bytes.max(sample.swap_bytes);
    state.layer.peak_threads = state.layer.peak_threads.max(sample.threads);
    state.layer.peak_open_fds = state.layer.peak_open_fds.max(sample.open_fds);
    if state.layer.first.is_none() {
        state.layer.first = Some(sample.clone());
    }
    state.layer.last = Some(sample);
}

fn signed_delta(after: u64, before: u64) -> i64 {
    if after >= before {
        after.saturating_sub(before).min(i64::MAX as u64) as i64
    } else {
        -(before.saturating_sub(after).min(i64::MAX as u64) as i64)
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn capture(
    pid: u32,
    semantic_progress: u64,
    elapsed: Duration,
    data_root: &Path,
) -> Result<TelemetrySample, String> {
    let storage = storage_snapshot(data_root)?;
    if pid == 0 {
        return Ok(TelemetrySample {
            elapsed_millis: elapsed.as_millis() as u64,
            semantic_progress,
            storage,
            ..TelemetrySample::default()
        });
    }
    let status =
        fs::read_to_string(format!("/proc/{pid}/status")).map_err(|error| error.to_string())?;
    let rss_bytes = status_value_kib(&status, "VmRSS:").saturating_mul(1024);
    let swap_bytes = status_value_kib(&status, "VmSwap:").saturating_mul(1024);
    let threads = status_value(&status, "Threads:");
    let open_fds = fs::read_dir(format!("/proc/{pid}/fd"))
        .map_err(|error| error.to_string())?
        .count() as u64;
    let io = fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    Ok(TelemetrySample {
        elapsed_millis: elapsed.as_millis() as u64,
        semantic_progress,
        pid,
        rss_bytes,
        swap_bytes,
        threads,
        open_fds,
        read_bytes: io_value(&io, "read_bytes:"),
        write_bytes: io_value(&io, "write_bytes:"),
        storage,
    })
}

fn status_value_kib(status: &str, name: &str) -> u64 {
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn status_value(status: &str, name: &str) -> u64 {
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or(0)
}

fn io_value(io: &str, name: &str) -> u64 {
    io.lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or(0)
}

pub fn storage_snapshot(root: &Path) -> Result<StorageSnapshot, String> {
    let mut snapshot = StorageSnapshot::default();
    if !root.exists() {
        return Ok(snapshot);
    }
    visit_storage(root, &mut snapshot)?;
    Ok(snapshot)
}

fn visit_storage(path: &Path, snapshot: &mut StorageSnapshot) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            visit_storage(&entry.path(), snapshot)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let bytes = entry.metadata().map_err(|error| error.to_string())?.len();
        snapshot.files += 1;
        snapshot.total_bytes = snapshot.total_bytes.saturating_add(bytes);
        let path = entry.path();
        let text = path.to_string_lossy();
        match path.extension().and_then(|value| value.to_str()) {
            Some("wal") => snapshot.wal_bytes += bytes,
            Some("data") => snapshot.data_bytes += bytes,
            Some("idx") => snapshot.index_bytes += bytes,
            Some("cat") => snapshot.catalog_bytes += bytes,
            Some("manifest") => snapshot.manifest_bytes += bytes,
            _ => {}
        }
        if text.contains("/staging/") || text.contains(".tmp") {
            snapshot.staging_bytes += bytes;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_report_preserves_prefix_peaks_and_shifts_current_progress() {
        let prefix_last = sample(1_000, 10, 100, 20);
        let prefix = TelemetryReport {
            samples_taken: 3,
            peak_rss_bytes: 500,
            peak_swap_bytes: 7,
            peak_threads: 8,
            peak_open_fds: 9,
            peak_storage_bytes: 600,
            last: Some(prefix_last.clone()),
            bounded_history: vec![prefix_last],
        };
        let current_last = sample(250, 4, 300, 40);
        let current = TelemetryReport {
            samples_taken: 2,
            peak_rss_bytes: 300,
            peak_swap_bytes: 0,
            peak_threads: 12,
            peak_open_fds: 5,
            peak_storage_bytes: 400,
            last: Some(current_last.clone()),
            bounded_history: vec![current_last],
        };

        let merged = merge_reports(Some(&prefix), current);
        assert_eq!(merged.samples_taken, 5);
        assert_eq!(merged.peak_rss_bytes, 500);
        assert_eq!(merged.peak_swap_bytes, 7);
        assert_eq!(merged.peak_threads, 12);
        assert_eq!(merged.peak_open_fds, 9);
        assert_eq!(merged.peak_storage_bytes, 600);
        assert_eq!(merged.last.as_ref().unwrap().elapsed_millis, 1_250);
        assert_eq!(merged.last.as_ref().unwrap().semantic_progress, 14);
        assert_eq!(merged.bounded_history.len(), 2);
        assert_eq!(merged.bounded_history[1].elapsed_millis, 1_250);
        assert_eq!(merged.bounded_history[1].semantic_progress, 14);
    }

    #[test]
    fn live_sample_is_single_bounded_replaceable_file() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(LIVE_SAMPLE_FILE);
        write_live_sample(&path, &sample(10, 1, 100, 20)).unwrap();
        write_live_sample(&path, &sample(20, 2, 200, 40)).unwrap();

        let bytes = fs::read(&path).unwrap();
        assert!(bytes.len() < 4 * 1024);
        let observed: TelemetrySample = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(observed.elapsed_millis, 20);
        assert_eq!(observed.semantic_progress, 2);
        assert_eq!(observed.rss_bytes, 200);
        assert_eq!(observed.storage.total_bytes, 40);
        assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 1);
    }

    fn sample(
        elapsed_millis: u64,
        semantic_progress: u64,
        rss_bytes: u64,
        storage_bytes: u64,
    ) -> TelemetrySample {
        TelemetrySample {
            elapsed_millis,
            semantic_progress,
            rss_bytes,
            storage: StorageSnapshot {
                total_bytes: storage_bytes,
                ..StorageSnapshot::default()
            },
            ..TelemetrySample::default()
        }
    }
}
