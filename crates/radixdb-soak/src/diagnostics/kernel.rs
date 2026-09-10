use std::{
    io::Read,
    process::{Command, Stdio},
    sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use super::DIAGNOSTIC_FORMAT_V2;

const MAX_JOURNAL_BYTES: usize = 256 * 1024;
const MAX_STDERR_BYTES: usize = 16 * 1024;
const MAX_EVIDENCE: usize = 16;
const MAX_EVIDENCE_BYTES: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelHealthSnapshotV2 {
    pub format: u32,
    pub unix_millis: u64,
    pub evidence: Vec<String>,
    pub error: Option<String>,
}

struct KernelHealthRequest {
    since_unix_millis: u64,
    until_unix_millis: u64,
}

pub struct KernelHealthWorker {
    request: Option<SyncSender<KernelHealthRequest>>,
    response: Receiver<KernelHealthSnapshotV2>,
    join: Option<JoinHandle<()>>,
}

impl KernelHealthWorker {
    pub fn start(timeout: Duration) -> Result<Self, String> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err("kernel health timeout must be in 1ms..=30s".into());
        }
        let (request_tx, request_rx) = sync_channel::<KernelHealthRequest>(1);
        let (response_tx, response_rx) = sync_channel::<KernelHealthSnapshotV2>(1);
        let join = thread::Builder::new()
            .name("radixdb-soak-kernel-health".into())
            .spawn(move || {
                while let Ok(request) = request_rx.recv() {
                    let snapshot = collect_kernel_health(
                        request.since_unix_millis,
                        request.until_unix_millis,
                        timeout,
                    );
                    if response_tx.send(snapshot).is_err() {
                        break;
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            request: Some(request_tx),
            response: response_rx,
            join: Some(join),
        })
    }

    pub fn try_request(
        &self,
        since_unix_millis: u64,
        until_unix_millis: u64,
    ) -> Result<bool, String> {
        let request = KernelHealthRequest {
            since_unix_millis,
            until_unix_millis,
        };
        match self
            .request
            .as_ref()
            .ok_or_else(|| "kernel health worker is shut down".to_string())?
            .try_send(request)
        {
            Ok(()) => Ok(true),
            Err(TrySendError::Full(_)) => Ok(false),
            Err(TrySendError::Disconnected(_)) => Err("kernel health worker disconnected".into()),
        }
    }

    pub fn poll(&self) -> Result<Option<KernelHealthSnapshotV2>, String> {
        match self.response.try_recv() {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err("kernel health response channel disconnected".into())
            }
        }
    }

    pub fn shutdown(mut self) -> Result<Vec<KernelHealthSnapshotV2>, String> {
        self.request.take();
        let join = self
            .join
            .take()
            .ok_or_else(|| "kernel health worker join handle is missing".to_string())?;
        join.join()
            .map_err(|_| "kernel health worker panicked".to_string())?;
        Ok(self.response.try_iter().collect())
    }
}

fn collect_kernel_health(
    since_unix_millis: u64,
    until_unix_millis: u64,
    timeout: Duration,
) -> KernelHealthSnapshotV2 {
    let mut snapshot = KernelHealthSnapshotV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        unix_millis: until_unix_millis,
        evidence: Vec::new(),
        error: None,
    };
    let result = (|| -> Result<Vec<String>, String> {
        let since = format_journal_time(since_unix_millis);
        let until = format_journal_time(until_unix_millis);
        let mut child = Command::new("journalctl")
            .args([
                "--no-pager",
                "--quiet",
                "-k",
                "--priority=warning..alert",
                "--lines=256",
                "--output=short-monotonic",
                "--since",
                &since,
                "--until",
                &until,
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
        let stderr_reader = thread::spawn(move || read_limited(stderr, MAX_STDERR_BYTES));
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() < timeout => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("journalctl exceeded {} ms", timeout.as_millis()));
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("wait journalctl: {error}"));
                }
            }
        };
        let (stdout, stdout_truncated) = stdout_reader
            .join()
            .map_err(|_| "journalctl stdout reader panicked".to_string())??;
        let (stderr, _) = stderr_reader
            .join()
            .map_err(|_| "journalctl stderr reader panicked".to_string())??;
        if !status.success() {
            return Err(format!(
                "journalctl exited with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            ));
        }
        if stdout_truncated {
            return Err("kernel journal exceeds 256 KiB diagnostic bound".into());
        }
        Ok(kernel_disk_evidence(&String::from_utf8_lossy(&stdout)))
    })();
    match result {
        Ok(evidence) => snapshot.evidence = evidence,
        Err(error) => snapshot.error = Some(error),
    }
    snapshot
}

fn format_journal_time(unix_millis: u64) -> String {
    format!("@{}.{:03}", unix_millis / 1_000, unix_millis % 1_000)
}

fn kernel_disk_evidence(journal: &str) -> Vec<String> {
    const SIGNALS: [&str; 12] = [
        "buffer i/o error",
        "blk_update_request: i/o error",
        "blk_print_req_error",
        "critical medium error",
        "end_request: i/o error",
        "uncorrectable error",
        "remounting filesystem read-only",
        "read-only file system",
        "ext4-fs error",
        "xfs (",
        "btrfs error",
        "ata bus error",
    ];
    let mut evidence = Vec::new();
    for line in journal.lines() {
        let lower = line.to_ascii_lowercase();
        let xfs_corruption = lower.contains("xfs (")
            && (lower.contains("corruption") || lower.contains("metadata i/o error"));
        if !xfs_corruption
            && !SIGNALS
                .iter()
                .filter(|signal| **signal != "xfs (")
                .any(|signal| lower.contains(signal))
        {
            continue;
        }
        let mut line = line.trim().to_string();
        if line.len() > MAX_EVIDENCE_BYTES {
            line.truncate(MAX_EVIDENCE_BYTES);
        }
        if !line.is_empty() && !evidence.contains(&line) {
            evidence.push(line);
        }
        if evidence.len() == MAX_EVIDENCE {
            break;
        }
    }
    evidence
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_disk_parser_is_bounded_and_ignores_unrelated_warnings() {
        let journal = "kernel: thermal warning\n\
            kernel: Buffer I/O error on dev sda, logical block 4\n\
            kernel: EXT4-fs error (device sda1): bad metadata\n";
        let evidence = kernel_disk_evidence(journal);
        assert_eq!(evidence.len(), 2);
        assert!(evidence[0].contains("Buffer I/O error"));
        assert!(evidence[1].contains("EXT4-fs error"));

        let repeated = (0..64)
            .map(|index| format!("kernel: read-only file system {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(kernel_disk_evidence(&repeated).len(), MAX_EVIDENCE);
    }

    #[test]
    fn journal_time_is_exact_to_millis() {
        assert_eq!(format_journal_time(1_234), "@1.234");
    }
}
