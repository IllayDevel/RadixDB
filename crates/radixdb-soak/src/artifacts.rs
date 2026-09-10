use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    diagnostics::{AgentTelemetryClient, SemanticProgressSnapshot},
    status::{RunEvent, StatusSnapshot},
};

const TELEMETRY_QUEUE_CAPACITY: usize = 64;

enum TelemetryCommand {
    Publish {
        unix_millis: u64,
        progress: SemanticProgressSnapshot,
    },
    Shutdown,
}

struct AgentTelemetryHeartbeat {
    sender: SyncSender<TelemetryCommand>,
    queue_dropped: Arc<AtomicU64>,
    client_dropped: Arc<AtomicU64>,
    worker: Option<JoinHandle<()>>,
}

impl AgentTelemetryHeartbeat {
    fn start(
        client: AgentTelemetryClient,
        initial_progress: SemanticProgressSnapshot,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "agent telemetry heartbeat interval must be non-zero",
            ));
        }
        let (sender, receiver) = sync_channel(TELEMETRY_QUEUE_CAPACITY);
        let queue_dropped = Arc::new(AtomicU64::new(0));
        let client_dropped = Arc::new(AtomicU64::new(0));
        let worker_client_dropped = Arc::clone(&client_dropped);
        let worker = thread::Builder::new()
            .name("radixdb-soak-agent-heartbeat".into())
            .spawn(move || {
                run_telemetry_heartbeat(
                    client,
                    receiver,
                    initial_progress,
                    interval,
                    &worker_client_dropped,
                );
            })?;
        Ok(Self {
            sender,
            queue_dropped,
            client_dropped,
            worker: Some(worker),
        })
    }

    fn publish(&self, unix_millis: u64, progress: &SemanticProgressSnapshot) {
        let command = TelemetryCommand::Publish {
            unix_millis,
            progress: progress.clone(),
        };
        if matches!(
            self.sender.try_send(command),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_))
        ) {
            self.queue_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped(&self) -> u64 {
        self.queue_dropped
            .load(Ordering::Relaxed)
            .saturating_add(self.client_dropped.load(Ordering::Relaxed))
    }
}

impl Drop for AgentTelemetryHeartbeat {
    fn drop(&mut self) {
        let _ = self.sender.send(TelemetryCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run_telemetry_heartbeat(
    mut client: AgentTelemetryClient,
    receiver: Receiver<TelemetryCommand>,
    mut latest: SemanticProgressSnapshot,
    interval: Duration,
    client_dropped: &AtomicU64,
) {
    client.publish(current_unix_millis(), &latest);
    client_dropped.store(client.dropped(), Ordering::Relaxed);
    loop {
        let unix_millis = match receiver.recv_timeout(interval) {
            Ok(TelemetryCommand::Publish {
                unix_millis,
                progress,
            }) => {
                latest = progress;
                unix_millis
            }
            Ok(TelemetryCommand::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                client.publish(current_unix_millis(), &latest);
                client_dropped.store(client.dropped(), Ordering::Relaxed);
                break;
            }
            Err(RecvTimeoutError::Timeout) => current_unix_millis(),
        };
        client.publish(unix_millis, &latest);
        client_dropped.store(client.dropped(), Ordering::Relaxed);
    }
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| {
            value.as_millis().min(u128::from(u64::MAX)) as u64
        })
}

pub struct ArtifactWriter {
    run_dir: PathBuf,
    events: BufWriter<File>,
    samples: BufWriter<File>,
    semantic_progress: BufWriter<File>,
    telemetry: Option<AgentTelemetryHeartbeat>,
}

impl ArtifactWriter {
    pub fn create(root: &Path, run_id: &str) -> io::Result<Self> {
        validate_run_id(run_id)?;
        fs::create_dir_all(root)?;
        let run_dir = root.join(run_id);
        fs::create_dir(&run_dir)?;
        for directory in ["histories", "traces", "backups", "failures"] {
            fs::create_dir(run_dir.join(directory))?;
        }
        let events = append_new(&run_dir.join("events.jsonl"))?;
        let samples = append_new(&run_dir.join("samples.jsonl"))?;
        let semantic_progress = append_new(&run_dir.join("semantic-progress.jsonl"))?;
        Ok(Self {
            run_dir,
            events: BufWriter::new(events),
            samples: BufWriter::new(samples),
            semantic_progress: BufWriter::new(semantic_progress),
            telemetry: None,
        })
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn write_manifest<T: Serialize>(&self, manifest: &T) -> io::Result<()> {
        write_new_json(&self.run_dir.join("manifest.json"), manifest)
    }

    pub fn publish_status(&self, status: &StatusSnapshot) -> io::Result<()> {
        status
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        atomic_json(&self.run_dir.join("status.json"), status)
    }

    pub fn append_event(&mut self, event: &RunEvent) -> io::Result<()> {
        append_json_line(&mut self.events, event)
    }

    pub fn append_sample<T: Serialize>(&mut self, sample: &T) -> io::Result<()> {
        append_json_line(&mut self.samples, sample)
    }

    pub fn append_semantic_progress(
        &mut self,
        progress: &SemanticProgressSnapshot,
    ) -> io::Result<()> {
        progress
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        append_json_line(&mut self.semantic_progress, progress)?;
        let heartbeat = progress
            .last_workload_progress_unix_millis
            .max(progress.last_operation_progress_unix_millis)
            .max(progress.phase_started_unix_millis);
        if let Some(telemetry) = self.telemetry.as_mut() {
            telemetry.publish(heartbeat, progress);
        }
        Ok(())
    }

    pub fn attach_telemetry(
        &mut self,
        telemetry: AgentTelemetryClient,
        initial_progress: &SemanticProgressSnapshot,
        heartbeat_interval: Duration,
    ) -> io::Result<()> {
        if self.telemetry.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "agent telemetry heartbeat is already attached",
            ));
        }
        self.telemetry = Some(AgentTelemetryHeartbeat::start(
            telemetry,
            initial_progress.clone(),
            heartbeat_interval,
        )?);
        Ok(())
    }

    pub fn publish_agent_heartbeat(
        &mut self,
        unix_millis: u64,
        progress: &SemanticProgressSnapshot,
    ) {
        if let Some(telemetry) = self.telemetry.as_mut() {
            telemetry.publish(unix_millis, progress);
        }
    }

    pub fn telemetry_dropped(&self) -> u64 {
        self.telemetry
            .as_ref()
            .map_or(0, AgentTelemetryHeartbeat::dropped)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.events.flush()?;
        self.events.get_ref().sync_data()?;
        self.samples.flush()?;
        self.samples.get_ref().sync_data()?;
        self.semantic_progress.flush()?;
        self.semantic_progress.get_ref().sync_data()
    }

    pub fn write_report<T: Serialize>(&mut self, report: &T, markdown: &str) -> io::Result<()> {
        self.sync()?;
        write_new_json(&self.run_dir.join("REPORT.json"), report)?;
        write_new_bytes(&self.run_dir.join("REPORT.md"), markdown.as_bytes())?;
        if self.telemetry.is_none() {
            write_checksums(&self.run_dir)?;
        }
        Ok(())
    }
}

fn validate_run_id(run_id: &str) -> io::Result<()> {
    if run_id.is_empty()
        || run_id.len() > 128
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid run id",
        ));
    }
    Ok(())
}

fn append_new(path: &Path) -> io::Result<File> {
    OpenOptions::new().create_new(true).append(true).open(path)
}

fn write_new_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

fn write_new_bytes(path: &Path, value: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(value)?;
    file.sync_all()
}

fn write_checksums(run_dir: &Path) -> io::Result<()> {
    let mut files = Vec::new();
    collect_files(run_dir, run_dir, &mut files)?;
    files.retain(|path| path != Path::new("SHA256SUMS"));
    files.sort();
    let target = run_dir.join("SHA256SUMS");
    let temporary = run_dir.join(format!(".SHA256SUMS.tmp.{}", std::process::id()));
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let result = (|| -> io::Result<()> {
        let mut output = BufWriter::new(file);
        let mut buffer = [0_u8; 64 * 1024];
        for relative in files {
            let mut input = File::open(run_dir.join(&relative))?;
            let mut digest = Sha256::new();
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
            writeln!(output, "{:x}  {}", digest.finalize(), relative.display())?;
        }
        output.flush()?;
        output.get_ref().sync_all()?;
        drop(output);
        fs::rename(&temporary, &target)?;
        File::open(run_dir)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn collect_files(root: &Path, directory: &Path, output: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_files(root, &entry.path(), output)?;
        } else if metadata.is_file() {
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

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let result = (|| {
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn append_json_line<T: Serialize>(writer: &mut BufWriter<File>, value: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn run_directory_is_exclusive_and_status_is_atomic() {
        let root = std::env::temp_dir().join(format!(
            "radixdb-soak-artifacts-{}-{}",
            std::process::id(),
            crate::status::RunState::Starting as u8
        ));
        let _ = fs::remove_dir_all(&root);
        let mut writer = ArtifactWriter::create(&root, "run-1").unwrap();
        writer.write_manifest(&json!({"format": 1})).unwrap();
        writer.append_sample(&json!({"elapsed_millis": 1})).unwrap();
        let tracker = crate::diagnostics::SemanticProgressTracker::new(1, "starting").unwrap();
        writer.append_semantic_progress(tracker.snapshot()).unwrap();
        writer.sync().unwrap();
        assert!(ArtifactWriter::create(&root, "run-1").is_err());
        assert!(root.join("run-1/manifest.json").is_file());
        assert!(root.join("run-1/semantic-progress.jsonl").is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn telemetry_heartbeat_continues_without_semantic_progress() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("agent.sock");
        let receiver = crate::diagnostics::AgentTelemetryReceiver::bind(&socket, "run-1").unwrap();
        let client = AgentTelemetryClient::connect(&socket, "run-1").unwrap();
        let tracker = crate::diagnostics::SemanticProgressTracker::new(1, "preflight").unwrap();
        let mut writer = ArtifactWriter::create(directory.path(), "run-1").unwrap();
        writer
            .attach_telemetry(client, tracker.snapshot(), Duration::from_millis(10))
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut messages = Vec::new();
        while messages.len() < 2 && std::time::Instant::now() < deadline {
            if let Some(message) = receiver.receive().unwrap() {
                messages.push(message);
            } else {
                thread::sleep(Duration::from_millis(2));
            }
        }

        assert_eq!(messages.len(), 2);
        assert!(messages[1].sequence > messages[0].sequence);
        assert!(messages[1].agent_unix_millis >= messages[0].agent_unix_millis);
        assert_eq!(messages[0].progress, messages[1].progress);
    }

    #[test]
    fn final_checksums_stream_large_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let payload = vec![0x5a; 3 * 64 * 1024 + 17];
        fs::write(directory.path().join("large.bin"), &payload).unwrap();
        write_checksums(directory.path()).unwrap();

        let sums = fs::read_to_string(directory.path().join("SHA256SUMS")).unwrap();
        assert_eq!(sums, format!("{:x}  large.bin\n", Sha256::digest(&payload)));
        assert!(fs::read_dir(directory.path())
            .unwrap()
            .flatten()
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp.")));
    }
}
