use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Starting,
    Preparing,
    Running,
    Quiescing,
    Passed,
    Failed,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogState {
    Healthy,
    Stalled,
    Terminal,
}

impl RunState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Passed | Self::Failed | Self::Interrupted)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    pub soak: String,
    pub server: Option<String>,
    pub cargo_lock_sha256: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    pub transactions_planned: u64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub conflicts: u64,
    pub disconnects: u64,
    pub ambiguous_resolved: u64,
    pub operations: u64,
    pub invariant_passes: u64,
    pub invariant_failures: u64,
    pub checkpoints: u64,
    pub checkpoint_deferred: u64,
    pub backups: u64,
    pub restores: u64,
    pub reopens: u64,
    pub auth_failures: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySnapshot {
    pub samples: u64,
    pub min_micros: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceSnapshot {
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    pub open_fds: u64,
    pub socket_fds: u64,
    pub threads: u64,
    pub cpu_user_millis: u64,
    pub cpu_system_millis: u64,
    pub process_read_bytes: u64,
    pub process_write_bytes: u64,
    pub database_bytes: u64,
    pub database_files: u64,
    pub database_data_bytes: u64,
    pub database_index_bytes: u64,
    pub database_metadata_bytes: u64,
    pub database_wal_bytes: u64,
    pub database_other_bytes: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSlopes {
    pub window_millis: u64,
    pub rss_bytes_per_hour: f64,
    pub database_bytes_per_hour: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerRuntimeSnapshot {
    pub open_databases: u64,
    pub retained_databases: u64,
    pub max_databases: u64,
    pub active_connections: u64,
    pub max_connections: u64,
    pub inflight_frame_bytes: u64,
    pub max_inflight_frame_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvariantState {
    Unknown,
    Passed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvariantStatus {
    pub name: String,
    pub state: InvariantState,
    pub checks: u64,
    pub failures: u64,
    pub last_checked_unix_millis: Option<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSnapshot {
    pub format: u32,
    pub run_id: String,
    pub profile: String,
    pub seed: u64,
    pub state: RunState,
    pub phase: String,
    pub started_unix_millis: u64,
    pub updated_unix_millis: u64,
    pub elapsed_millis: u64,
    pub remaining_millis: u64,
    pub last_progress_unix_millis: u64,
    pub watchdog_timeout_millis: u64,
    pub watchdog_silence_millis: u64,
    pub watchdog_state: WatchdogState,
    pub active_clients: u64,
    pub target_clients: u64,
    pub current_tps: f64,
    pub identity: BuildIdentity,
    pub counters: Counters,
    pub latency: LatencySnapshot,
    pub resources: ResourceSnapshot,
    pub resource_slopes: ResourceSlopes,
    pub server_runtime: ServerRuntimeSnapshot,
    pub invariants: BTreeMap<String, InvariantStatus>,
    pub logical_digest: Option<String>,
    pub failure: Option<String>,
}

impl StatusSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != 1 {
            return Err("unsupported status format".into());
        }
        validate_label("run_id", &self.run_id)?;
        validate_label("profile", &self.profile)?;
        if self.phase.is_empty() || self.phase.len() > 128 {
            return Err("phase must contain 1..=128 bytes".into());
        }
        if self.updated_unix_millis < self.started_unix_millis
            || self.last_progress_unix_millis < self.started_unix_millis
        {
            return Err("status timestamps precede run start".into());
        }
        if self.watchdog_timeout_millis == 0 {
            return Err("watchdog timeout must be non-zero".into());
        }
        if self.counters.invariant_failures > 0 && self.failure.is_none() {
            return Err("invariant failure requires public failure detail".into());
        }
        if self.state == RunState::Passed && self.failure.is_some() {
            return Err("passed run cannot contain failure detail".into());
        }
        Ok(())
    }
}

fn validate_label(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(format!("invalid {name}"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    pub sequence: u64,
    pub unix_millis: u64,
    pub kind: String,
    pub detail: String,
}

#[derive(Debug)]
pub struct EventBuffer {
    capacity: usize,
    next_sequence: u64,
    events: VecDeque<RunEvent>,
}

impl EventBuffer {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("event capacity must be non-zero".into());
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            events: VecDeque::with_capacity(capacity.min(4096)),
        })
    }

    pub fn push(&mut self, unix_millis: u64, kind: &str, detail: &str) -> Result<RunEvent, String> {
        if kind.is_empty() || kind.len() > 64 {
            return Err("event kind must contain 1..=64 bytes".into());
        }
        if detail.len() > 4096 {
            return Err("event detail exceeds 4096 bytes".into());
        }
        let event = RunEvent {
            sequence: self.next_sequence,
            unix_millis,
            kind: kind.into(),
            detail: detail.into(),
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| "event sequence exhausted".to_string())?;
        if self.events.len() == self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event.clone());
        Ok(event)
    }

    pub fn after(&self, sequence: u64) -> Vec<RunEvent> {
        self.events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_buffer_is_bounded_and_sequence_is_stable() {
        let mut events = EventBuffer::new(2).unwrap();
        events.push(1, "one", "1").unwrap();
        events.push(2, "two", "2").unwrap();
        events.push(3, "three", "3").unwrap();
        let tail = events.after(0);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].sequence, 2);
        assert_eq!(events.after(2)[0].detail, "3");
    }
}
