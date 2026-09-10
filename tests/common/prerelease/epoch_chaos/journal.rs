use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 8] = b"RDXEPOCH";
const RECORD_BYTES: usize = 32;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    Planned = 1,
    Started = 2,
    Committed = 3,
    RolledBack = 4,
    Disconnected = 5,
    Conflicted = 6,
    Rejected = 7,
    Ambiguous = 8,
}

impl RecordState {
    fn from_byte(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Planned),
            2 => Ok(Self::Started),
            3 => Ok(Self::Committed),
            4 => Ok(Self::RolledBack),
            5 => Ok(Self::Disconnected),
            6 => Ok(Self::Conflicted),
            7 => Ok(Self::Rejected),
            8 => Ok(Self::Ambiguous),
            _ => Err(format!("unknown epoch journal state {value}")),
        }
    }

    fn is_terminal(self) -> bool {
        !matches!(self, Self::Planned | Self::Started)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct IdentityDigest {
    pub count: u64,
    pub xor: u64,
    pub sum: u64,
    pub mixed_sum: u64,
}

impl IdentityDigest {
    fn add(&mut self, operation_id: u64) {
        self.count = self.count.saturating_add(1);
        self.xor ^= operation_id;
        self.sum = self.sum.wrapping_add(operation_id);
        self.mixed_sum = self.mixed_sum.wrapping_add(mix(operation_id));
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct JournalSummary {
    pub planned: IdentityDigest,
    pub started: IdentityDigest,
    pub terminal: IdentityDigest,
    pub committed: u64,
    pub rolled_back: u64,
    pub disconnected: u64,
    pub conflicted: u64,
    pub rejected: u64,
    pub ambiguous: u64,
    pub bytes: u64,
}

impl JournalSummary {
    fn record(&mut self, state: RecordState, operation_id: u64) {
        match state {
            RecordState::Planned => self.planned.add(operation_id),
            RecordState::Started => self.started.add(operation_id),
            RecordState::Committed => {
                self.terminal.add(operation_id);
                self.committed += 1;
            }
            RecordState::RolledBack => {
                self.terminal.add(operation_id);
                self.rolled_back += 1;
            }
            RecordState::Disconnected => {
                self.terminal.add(operation_id);
                self.disconnected += 1;
            }
            RecordState::Conflicted => {
                self.terminal.add(operation_id);
                self.conflicted += 1;
            }
            RecordState::Rejected => {
                self.terminal.add(operation_id);
                self.rejected += 1;
            }
            RecordState::Ambiguous => {
                self.terminal.add(operation_id);
                self.ambiguous += 1;
            }
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.planned != self.started || self.planned != self.terminal {
            return Err(format!(
                "journal identities differ: planned={:?} started={:?} terminal={:?}",
                self.planned, self.started, self.terminal
            ));
        }
        let outcomes = self
            .committed
            .saturating_add(self.rolled_back)
            .saturating_add(self.disconnected)
            .saturating_add(self.conflicted)
            .saturating_add(self.rejected)
            .saturating_add(self.ambiguous);
        if outcomes != self.terminal.count {
            return Err(format!(
                "terminal outcomes {outcomes} differ from terminal digest {}",
                self.terminal.count
            ));
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) {
        merge_digest(&mut self.planned, other.planned);
        merge_digest(&mut self.started, other.started);
        merge_digest(&mut self.terminal, other.terminal);
        self.committed += other.committed;
        self.rolled_back += other.rolled_back;
        self.disconnected += other.disconnected;
        self.conflicted += other.conflicted;
        self.rejected += other.rejected;
        self.ambiguous += other.ambiguous;
        self.bytes += other.bytes;
    }
}

fn merge_digest(target: &mut IdentityDigest, source: IdentityDigest) {
    target.count += source.count;
    target.xor ^= source.xor;
    target.sum = target.sum.wrapping_add(source.sum);
    target.mixed_sum = target.mixed_sum.wrapping_add(source.mixed_sum);
}

pub struct JournalShard {
    path: PathBuf,
    writer: BufWriter<File>,
    summary: JournalSummary,
}

impl JournalShard {
    pub fn create(root: &Path, layer: &str, shard: usize) -> Result<Self, String> {
        validate_layer(layer)?;
        fs::create_dir_all(root).map_err(|error| error.to_string())?;
        let path = root.join(format!("{layer}-{shard:04}.journal"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("create {}: {error}", path.display()))?;
        file.write_all(MAGIC).map_err(|error| error.to_string())?;
        Ok(Self {
            path,
            writer: BufWriter::with_capacity(1024 * 1024, file),
            summary: JournalSummary {
                bytes: MAGIC.len() as u64,
                ..JournalSummary::default()
            },
        })
    }

    pub fn record(
        &mut self,
        state: RecordState,
        operation_id: u64,
        value: i64,
    ) -> Result<(), String> {
        let mut record = [0_u8; RECORD_BYTES];
        record[0] = state as u8;
        record[8..16].copy_from_slice(&operation_id.to_le_bytes());
        record[16..24].copy_from_slice(&value.to_le_bytes());
        let checksum = crc32fast::hash(&record[..24]);
        record[24..28].copy_from_slice(&checksum.to_le_bytes());
        self.writer
            .write_all(&record)
            .map_err(|error| error.to_string())?;
        self.summary.record(state, operation_id);
        self.summary.bytes += RECORD_BYTES as u64;
        Ok(())
    }

    pub fn plan_and_start(&mut self, operation_id: u64, value: i64) -> Result<(), String> {
        self.record(RecordState::Planned, operation_id, value)?;
        self.record(RecordState::Started, operation_id, value)
    }

    pub fn terminal(
        &mut self,
        operation_id: u64,
        value: i64,
        state: RecordState,
    ) -> Result<(), String> {
        if !state.is_terminal() {
            return Err("journal terminal requires a terminal state".to_string());
        }
        self.record(state, operation_id, value)
    }

    pub fn finish(
        mut self,
        outcomes: impl IntoIterator<Item = (u64, i64, RecordState)>,
    ) -> Result<JournalSummary, String> {
        for (operation_id, value, state) in outcomes {
            if !state.is_terminal() {
                return Err("journal finish requires terminal outcomes".to_string());
            }
            self.record(state, operation_id, value)?;
        }
        self.writer.flush().map_err(|error| error.to_string())?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        self.summary.validate()?;
        let observed = inspect(&self.path)?;
        if observed != self.summary {
            return Err(format!(
                "journal reread differs for {}: memory={:?} disk={observed:?}",
                self.path.display(),
                self.summary
            ));
        }
        Ok(self.summary)
    }

    pub fn seal(self) -> Result<JournalSummary, String> {
        self.finish(std::iter::empty())
    }
}

pub fn inspect(path: &Path) -> Result<JournalSummary, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let length = file.metadata().map_err(|error| error.to_string())?.len();
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut magic = [0_u8; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|error| error.to_string())?;
    if &magic != MAGIC {
        return Err(format!("invalid epoch journal magic in {}", path.display()));
    }
    let mut summary = JournalSummary {
        bytes: MAGIC.len() as u64,
        ..JournalSummary::default()
    };
    let mut record = [0_u8; RECORD_BYTES];
    while summary.bytes < length {
        reader
            .read_exact(&mut record)
            .map_err(|error| format!("truncated {}: {error}", path.display()))?;
        let expected = u32::from_le_bytes(record[24..28].try_into().unwrap());
        let actual = crc32fast::hash(&record[..24]);
        if expected != actual || record[28..].iter().any(|byte| *byte != 0) {
            return Err(format!("corrupt journal record in {}", path.display()));
        }
        let state = RecordState::from_byte(record[0])?;
        let operation_id = u64::from_le_bytes(record[8..16].try_into().unwrap());
        summary.record(state, operation_id);
        summary.bytes += RECORD_BYTES as u64;
    }
    if summary.bytes != length {
        return Err(format!("misaligned journal length {length}"));
    }
    summary.validate()?;
    Ok(summary)
}

fn validate_layer(layer: &str) -> Result<(), String> {
    if layer.is_empty()
        || !layer
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(format!("invalid journal layer `{layer}`"));
    }
    Ok(())
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_round_trip_proves_terminal_identity_set() {
        let temporary = tempfile::tempdir().unwrap();
        let mut shard = JournalShard::create(temporary.path(), "wide", 0).unwrap();
        for id in 1..=3 {
            shard.plan_and_start(id, id as i64).unwrap();
        }
        let summary = shard
            .finish([
                (1, 1, RecordState::Committed),
                (2, 2, RecordState::RolledBack),
                (3, 3, RecordState::Disconnected),
            ])
            .unwrap();
        assert_eq!(summary.terminal.count, 3);
    }
}
