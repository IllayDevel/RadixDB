use std::{
    fs::{File, OpenOptions},
    io,
    os::fd::AsRawFd,
    os::unix::fs::FileTypeExt,
    os::unix::net::UnixDatagram,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{SemanticProgressSnapshot, DIAGNOSTIC_FORMAT_V2};

const MAX_TELEMETRY_DATAGRAM: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTelemetryMessage {
    pub format: u32,
    pub sequence: u64,
    pub run_id: String,
    pub agent_unix_millis: u64,
    pub progress: SemanticProgressSnapshot,
}

impl AgentTelemetryMessage {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 || self.sequence == 0 {
            return Err("invalid agent telemetry version or sequence".into());
        }
        if self.run_id.is_empty()
            || self.run_id.len() > 128
            || !self
                .run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err("invalid agent telemetry run id".into());
        }
        self.progress.validate()
    }
}

pub struct AgentTelemetryClient {
    socket: UnixDatagram,
    path: PathBuf,
    run_id: String,
    sequence: u64,
    dropped: u64,
}

impl AgentTelemetryClient {
    pub fn connect(path: &Path, run_id: &str) -> io::Result<Self> {
        validate_socket_path(path)?;
        let socket = UnixDatagram::unbound()?;
        socket.connect(path)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            path: path.to_path_buf(),
            run_id: run_id.into(),
            sequence: 0,
            dropped: 0,
        })
    }

    pub fn publish(&mut self, agent_unix_millis: u64, progress: &SemanticProgressSnapshot) {
        let Some(sequence) = self.sequence.checked_add(1) else {
            self.dropped = self.dropped.saturating_add(1);
            return;
        };
        let message = AgentTelemetryMessage {
            format: DIAGNOSTIC_FORMAT_V2,
            sequence,
            run_id: self.run_id.clone(),
            agent_unix_millis,
            progress: progress.clone(),
        };
        let Ok(payload) = serde_json::to_vec(&message) else {
            self.dropped = self.dropped.saturating_add(1);
            return;
        };
        if payload.len() > MAX_TELEMETRY_DATAGRAM {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        if self.socket.send(&payload).is_err() {
            let reconnected = UnixDatagram::unbound().and_then(|socket| {
                socket.connect(&self.path)?;
                socket.set_nonblocking(true)?;
                Ok(socket)
            });
            let Ok(socket) = reconnected else {
                self.dropped = self.dropped.saturating_add(1);
                return;
            };
            self.socket = socket;
            if self.socket.send(&payload).is_err() {
                self.dropped = self.dropped.saturating_add(1);
                return;
            }
        }
        self.sequence = sequence;
    }

    pub const fn dropped(&self) -> u64 {
        self.dropped
    }
}

pub struct AgentTelemetryReceiver {
    socket: UnixDatagram,
    path: PathBuf,
    expected_run_id: String,
    _owner_lock: Option<File>,
}

impl AgentTelemetryReceiver {
    pub fn bind(path: &Path, expected_run_id: &str) -> io::Result<Self> {
        validate_socket_path(path)?;
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "telemetry socket path already exists",
            ));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let socket = UnixDatagram::bind(path)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            path: path.to_path_buf(),
            expected_run_id: expected_run_id.into(),
            _owner_lock: None,
        })
    }

    /// Bind after an observer crash where the filesystem socket survived but
    /// the owning process did not. The systemd unit provides single-instance
    /// exclusion; non-socket paths are never removed.
    pub fn bind_replacing_stale(path: &Path, expected_run_id: &str) -> io::Result<Self> {
        validate_socket_path(path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension("observer-lock");
        let owner_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        // SAFETY: the descriptor stays owned by the receiver and therefore
        // retains the single-observer advisory lock for its full lifetime.
        if unsafe { libc::flock(owner_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "another telemetry observer owns this socket",
            ));
        }
        if path.exists() {
            let file_type = std::fs::symlink_metadata(path)?.file_type();
            if !file_type.is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "telemetry path exists and is not a Unix socket",
                ));
            }
            std::fs::remove_file(path)?;
        }
        let socket = UnixDatagram::bind(path)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            path: path.to_path_buf(),
            expected_run_id: expected_run_id.into(),
            _owner_lock: Some(owner_lock),
        })
    }

    pub fn receive(&self) -> io::Result<Option<AgentTelemetryMessage>> {
        let mut payload = [0u8; MAX_TELEMETRY_DATAGRAM];
        let size = match self.socket.recv(&mut payload) {
            Ok(size) => size,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error),
        };
        let message: AgentTelemetryMessage = serde_json::from_slice(&payload[..size])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        message
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if message.run_id != self.expected_run_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "telemetry message belongs to another run",
            ));
        }
        Ok(Some(message))
    }
}

impl Drop for AgentTelemetryReceiver {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn validate_socket_path(path: &Path) -> io::Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "telemetry socket must be an absolute path without parent traversal",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::SemanticProgressTracker;

    #[test]
    fn bounded_unix_datagram_preserves_run_and_progress_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent.sock");
        let receiver = AgentTelemetryReceiver::bind(&path, "run-1").unwrap();
        let mut client = AgentTelemetryClient::connect(&path, "run-1").unwrap();
        let tracker = SemanticProgressTracker::new(1_000, "starting").unwrap();

        client.publish(1_001, tracker.snapshot());
        let message = receiver.receive().unwrap().unwrap();

        assert_eq!(message.sequence, 1);
        assert_eq!(message.run_id, "run-1");
        assert_eq!(message.agent_unix_millis, 1_001);
        assert_eq!(message.progress, *tracker.snapshot());
        assert_eq!(client.dropped(), 0);
    }

    #[test]
    fn client_reconnects_after_observer_socket_is_rebound() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent.sock");
        let receiver = AgentTelemetryReceiver::bind(&path, "run-1").unwrap();
        let mut client = AgentTelemetryClient::connect(&path, "run-1").unwrap();
        let tracker = SemanticProgressTracker::new(1_000, "starting").unwrap();
        client.publish(1_001, tracker.snapshot());
        assert!(receiver.receive().unwrap().is_some());
        drop(receiver);

        let rebound = AgentTelemetryReceiver::bind_replacing_stale(&path, "run-1").unwrap();
        client.publish(1_002, tracker.snapshot());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let message = loop {
            if let Some(message) = rebound.receive().unwrap() {
                break message;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        };
        assert_eq!(message.sequence, 2);
        assert_eq!(client.dropped(), 0);
    }
}
