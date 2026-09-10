use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{CounterSet, OperationTrace, RunManifest, RunReport};

#[derive(Debug)]
pub struct OwnedFixture {
    directory: tempfile::TempDir,
}

impl OwnedFixture {
    pub fn new(prefix: &str) -> Result<Self, String> {
        Self::validate_prefix(prefix)?;
        let directory = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .map_err(|error| error.to_string())?;
        Ok(Self { directory })
    }

    /// Create an owned fixture below an explicit harness-owned parent.
    ///
    /// Large prerelease fixtures must not land on the small system tmpfs.  The
    /// returned owner still deletes only the random directory created by
    /// `tempdir_in`; it never treats `parent` itself as an owned target.
    pub fn new_in(parent: &Path, prefix: &str) -> Result<Self, String> {
        Self::validate_prefix(prefix)?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let directory = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(parent)
            .map_err(|error| error.to_string())?;
        Ok(Self { directory })
    }

    fn validate_prefix(prefix: &str) -> Result<(), String> {
        if prefix.is_empty()
            || !prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("fixture prefix contains unsafe characters".to_string());
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        self.directory.path()
    }

    pub fn child(&self, relative: impl AsRef<Path>) -> Result<PathBuf, String> {
        let relative = relative.as_ref();
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(format!(
                "fixture path `{}` must be a non-empty relative path without traversal",
                relative.display()
            ));
        }
        Ok(self.root().join(relative))
    }

    pub fn remove_child(&self, relative: impl AsRef<Path>) -> Result<(), String> {
        let target = self.child(relative)?;
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
                fs::remove_file(&target).map_err(|error| error.to_string())
            }
            Ok(metadata) if metadata.is_dir() => {
                fs::remove_dir_all(&target).map_err(|error| error.to_string())
            }
            Ok(_) => Err(format!("unsupported fixture entry `{}`", target.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct OwnedChildProcess {
    child: Option<Child>,
    expected_pid: u32,
}

impl OwnedChildProcess {
    pub fn spawn(fixture: &OwnedFixture, command: &mut Command) -> Result<Self, String> {
        command
            .current_dir(fixture.root())
            .env("RADIXDB_PRERELEASE_FIXTURE_ROOT", fixture.root());
        let child = command.spawn().map_err(|error| error.to_string())?;
        let expected_pid = child.id();
        Ok(Self {
            child: Some(child),
            expected_pid,
        })
    }

    pub fn pid(&self) -> u32 {
        self.expected_pid
    }

    pub fn terminate(mut self) -> Result<ExitStatus, String> {
        self.terminate_inner()
    }

    fn terminate_inner(&mut self) -> Result<ExitStatus, String> {
        let mut child = self
            .child
            .take()
            .ok_or_else(|| "owned child process was already reaped".to_string())?;
        if child.id() != self.expected_pid {
            return Err("owned child PID changed unexpectedly".to_string());
        }
        if child
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_none()
        {
            child.kill().map_err(|error| error.to_string())?;
        }
        child.wait().map_err(|error| error.to_string())
    }
}

impl Drop for OwnedChildProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.id() == self.expected_pid {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn create(fixture: &OwnedFixture, relative: impl AsRef<Path>) -> Result<Self, String> {
        let root = fixture.child(relative)?;
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_manifest(&self, manifest: &RunManifest) -> Result<PathBuf, String> {
        self.write_json("manifest.json", manifest)
    }

    pub fn write_trace(&self, trace: &OperationTrace) -> Result<PathBuf, String> {
        self.write_json("trace.json", trace)
    }

    pub fn write_trace_pair_once(
        &self,
        original: &OperationTrace,
        minimized: &OperationTrace,
    ) -> Result<TracePairArtifacts, String> {
        let original_path = checked_artifact_path(&self.root, Path::new("trace-original.json"))?;
        let minimized_path = checked_artifact_path(&self.root, Path::new("trace-minimized.json"))?;
        if original_path.exists() || minimized_path.exists() {
            return Err("original/minimized trace artifacts already exist".to_string());
        }
        let original = self.write_json("trace-original.json", original)?;
        let minimized = self.write_json("trace-minimized.json", minimized)?;
        Ok(TracePairArtifacts {
            original,
            minimized,
        })
    }

    pub fn write_timeout(&self, timeout: &TimeoutSnapshot) -> Result<PathBuf, String> {
        self.write_json("timeout.json", timeout)
    }

    pub fn write_report(&self, report: &RunReport) -> Result<ReportArtifacts, String> {
        report.validate()?;
        let json = self.write_json("report.json", report)?;
        let markdown = self.write_text("REPORT.md", &report.to_markdown()?)?;
        Ok(ReportArtifacts { json, markdown })
    }

    pub fn write_json<T: Serialize>(
        &self,
        relative: impl AsRef<Path>,
        value: &T,
    ) -> Result<PathBuf, String> {
        let target = checked_artifact_path(&self.root, relative.as_ref())?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let json = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
        let temporary = target.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
        file.write_all(&json).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, &target).map_err(|error| error.to_string())?;
        Ok(target)
    }

    pub fn write_text(&self, relative: impl AsRef<Path>, value: &str) -> Result<PathBuf, String> {
        let target = checked_artifact_path(&self.root, relative.as_ref())?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let temporary = target.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
        file.write_all(value.as_bytes())
            .map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, &target).map_err(|error| error.to_string())?;
        Ok(target)
    }

    pub fn read_json<T: DeserializeOwned>(&self, relative: impl AsRef<Path>) -> Result<T, String> {
        let target = checked_artifact_path(&self.root, relative.as_ref())?;
        let bytes = fs::read(&target).map_err(|error| error.to_string())?;
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutSnapshot {
    pub run_id: String,
    pub stage: String,
    pub trace_events: usize,
    pub trace_artifact: String,
    pub trace_sha256: String,
    pub process: ProcessSnapshot,
    pub threads: Vec<ThreadSnapshot>,
    pub connections: Vec<ConnectionSnapshot>,
    pub resources: ResourceSnapshot,
    pub counters: CounterSet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub executable: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadSnapshot {
    pub name: String,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionSnapshot {
    pub session_id: u64,
    pub state: String,
    pub transaction_id: Option<u64>,
    pub open_cursors: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSnapshot {
    pub rss_bytes: u64,
    pub open_file_descriptors: u64,
    pub active_workers: u64,
    pub active_sessions: u64,
    pub active_cursors: u64,
}

impl ResourceSnapshot {
    pub fn capture_linux(active_sessions: u64, active_cursors: u64) -> Result<Self, String> {
        let status = fs::read_to_string("/proc/self/status").map_err(|error| error.to_string())?;
        let rss_kib = status
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .ok_or_else(|| "VmRSS is absent from /proc/self/status".to_string())?;
        let open_file_descriptors = fs::read_dir("/proc/self/fd")
            .map_err(|error| error.to_string())?
            .count() as u64;
        let active_workers = fs::read_dir("/proc/self/task")
            .map_err(|error| error.to_string())?
            .count() as u64;
        Ok(Self {
            rss_bytes: rss_kib.saturating_mul(1024),
            open_file_descriptors,
            active_workers,
            active_sessions,
            active_cursors,
        })
    }

    pub fn within_quiescent_corridor(
        &self,
        baseline: &Self,
        rss_slack_bytes: u64,
        fd_slack: u64,
        worker_slack: u64,
    ) -> Result<(), String> {
        if self.rss_bytes > baseline.rss_bytes.saturating_add(rss_slack_bytes) {
            return Err(format!(
                "RSS {} exceeds baseline {} + slack {}",
                self.rss_bytes, baseline.rss_bytes, rss_slack_bytes
            ));
        }
        if self.open_file_descriptors > baseline.open_file_descriptors.saturating_add(fd_slack) {
            return Err(format!(
                "FD count {} exceeds baseline {} + slack {}",
                self.open_file_descriptors, baseline.open_file_descriptors, fd_slack
            ));
        }
        if self.active_workers > baseline.active_workers.saturating_add(worker_slack) {
            return Err(format!(
                "thread count {} exceeds baseline {} + slack {}",
                self.active_workers, baseline.active_workers, worker_slack
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportArtifacts {
    pub json: PathBuf,
    pub markdown: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TracePairArtifacts {
    pub original: PathBuf,
    pub minimized: PathBuf,
}

pub fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn checked_artifact_path(root: &Path, relative: &Path) -> Result<PathBuf, String> {
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "artifact path `{}` must be a non-empty relative path without traversal",
            relative.display()
        ));
    }
    Ok(root.join(relative))
}
