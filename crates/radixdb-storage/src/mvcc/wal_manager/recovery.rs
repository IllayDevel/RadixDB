use super::*;

/// Information returned from two-phase WAL recovery.
#[derive(Debug, Clone)]
pub struct TwoPhaseRecoveryInfo {
    /// Last LSN processed.
    pub last_lsn: u64,
    /// Number of committed transactions found.
    pub committed_transactions: usize,
    /// Number of aborted transactions found.
    pub aborted_transactions: usize,
    /// Number of WAL entries applied from committed transactions.
    pub applied_entries: u64,
    /// Number of WAL entries skipped from aborted or in-doubt transactions.
    pub skipped_entries: u64,
    /// Highest positive transaction ID observed anywhere in retained WAL.
    pub max_transaction_id: i64,
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub type WalAppendTestHook = std::sync::Arc<dyn Fn(&WALEntry) + Send + Sync>;

/// Batch-C recovery oracle. Production uses a larger fixed memory budget;
/// tests deliberately cross the boundary with only a handful of markers.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub const TEST_RECOVERY_OUTCOME_MEMORY_LIMIT: usize = 8;
#[cfg(any(test, feature = "test-hooks"))]
static RECOVERY_OUTCOME_SPILL_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct WalAppendTestHookGuard<'a> {
    hook: &'a Mutex<Option<WalAppendTestHook>>,
    _owner: std::sync::MutexGuard<'a, ()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl<'a> WalAppendTestHookGuard<'a> {
    pub fn install(wal: &'a WALManager, hook: WalAppendTestHook) -> Self {
        let owner = wal
            .append_test_hook_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *wal.append_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        Self {
            hook: &wal.append_test_hook,
            _owner: owner,
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for WalAppendTestHookGuard<'_> {
    fn drop(&mut self) {
        *self
            .hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn reset_recovery_outcome_spill_count() {
    RECOVERY_OUTCOME_SPILL_COUNT.store(0, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn recovery_outcome_spill_count() -> u64 {
    RECOVERY_OUTCOME_SPILL_COUNT.load(Ordering::Acquire)
}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) const RECOVERY_OUTCOME_MEMORY_LIMIT: usize = TEST_RECOVERY_OUTCOME_MEMORY_LIMIT;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) const RECOVERY_OUTCOME_MEMORY_LIMIT: usize = 262_144;

pub(super) const RECOVERY_OUTCOME_COMMITTED: u8 = 1;
pub(super) const RECOVERY_OUTCOME_ABORTED: u8 = 2;
const RECOVERY_OUTCOME_SLOT_SIZE: u64 = 16;
static RECOVERY_OUTCOME_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct DiskRecoveryOutcomes {
    file: Option<File>,
    pub(super) path: PathBuf,
    capacity: u64,
    committed: usize,
    aborted: usize,
}

impl DiskRecoveryOutcomes {
    pub(super) fn create(wal_dir: &Path, outcome_count: usize) -> Result<Self> {
        let required = outcome_count
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| Error::internal("WAL recovery outcome count overflow"))?;
        let capacity = required
            .checked_next_power_of_two()
            .ok_or_else(|| Error::internal("WAL recovery outcome index capacity overflow"))?
            .max(16) as u64;
        let byte_len = capacity
            .checked_mul(RECOVERY_OUTCOME_SLOT_SIZE)
            .ok_or_else(|| Error::internal("WAL recovery outcome index byte size overflow"))?;

        let mut selected = None;
        for _ in 0..32 {
            let sequence = RECOVERY_OUTCOME_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = wal_dir.join(format!(
                ".recovery-outcomes-{}-{sequence}.tmp",
                std::process::id()
            ));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    selected = Some((file, path));
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(Error::internal(format!(
                        "failed to create WAL recovery outcome spill: {error}"
                    )))
                }
            }
        }
        let (file, path) = selected.ok_or_else(|| {
            Error::internal("failed to allocate a unique WAL recovery outcome spill path")
        })?;
        if let Err(error) = file.set_len(byte_len) {
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(Error::internal(format!(
                "failed to size WAL recovery outcome spill to {byte_len} bytes: {error}"
            )));
        }

        #[cfg(any(test, feature = "test-hooks"))]
        RECOVERY_OUTCOME_SPILL_COUNT.fetch_add(1, Ordering::AcqRel);

        Ok(Self {
            file: Some(file),
            path,
            capacity,
            committed: 0,
            aborted: 0,
        })
    }

    #[inline]
    pub(super) fn initial_slot(&self, txn_id: i64) -> u64 {
        let mut value = txn_id as u64;
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        value & (self.capacity - 1)
    }

    pub(super) fn read_slot(&mut self, slot: u64) -> Result<(u8, i64)> {
        let offset = slot
            .checked_mul(RECOVERY_OUTCOME_SLOT_SIZE)
            .ok_or_else(|| Error::internal("WAL recovery outcome slot offset overflow"))?;
        let file = self.file.as_mut().expect("recovery spill file is open");
        file.seek(SeekFrom::Start(offset)).map_err(|error| {
            Error::internal(format!(
                "failed to seek WAL recovery outcome spill: {error}"
            ))
        })?;
        let mut encoded = [0u8; RECOVERY_OUTCOME_SLOT_SIZE as usize];
        file.read_exact(&mut encoded).map_err(|error| {
            Error::internal(format!(
                "failed to read WAL recovery outcome spill: {error}"
            ))
        })?;
        Ok((
            encoded[0],
            i64::from_le_bytes(encoded[8..16].try_into().unwrap()),
        ))
    }

    pub(super) fn write_slot(&mut self, slot: u64, txn_id: i64, outcome: u8) -> Result<()> {
        let offset = slot
            .checked_mul(RECOVERY_OUTCOME_SLOT_SIZE)
            .ok_or_else(|| Error::internal("WAL recovery outcome slot offset overflow"))?;
        let mut encoded = [0u8; RECOVERY_OUTCOME_SLOT_SIZE as usize];
        encoded[0] = outcome;
        encoded[8..16].copy_from_slice(&txn_id.to_le_bytes());
        let file = self.file.as_mut().expect("recovery spill file is open");
        file.seek(SeekFrom::Start(offset)).map_err(|error| {
            Error::internal(format!(
                "failed to seek WAL recovery outcome spill: {error}"
            ))
        })?;
        file.write_all(&encoded).map_err(|error| {
            Error::internal(format!(
                "failed to write WAL recovery outcome spill: {error}"
            ))
        })
    }

    pub(super) fn insert(&mut self, txn_id: i64, outcome: u8) -> Result<()> {
        let start = self.initial_slot(txn_id);
        for probe in 0..self.capacity {
            let slot = (start + probe) & (self.capacity - 1);
            let (stored_outcome, stored_txn_id) = self.read_slot(slot)?;
            if stored_outcome == 0 {
                self.write_slot(slot, txn_id, outcome)?;
                if outcome == RECOVERY_OUTCOME_COMMITTED {
                    self.committed += 1;
                } else {
                    self.aborted += 1;
                }
                return Ok(());
            }
            if stored_txn_id == txn_id {
                if stored_outcome != outcome {
                    return Err(Error::internal(format!(
                        "WAL transaction {txn_id} has both commit and abort outcomes"
                    )));
                }
                return Ok(());
            }
        }
        Err(Error::internal("WAL recovery outcome spill is full"))
    }

    pub(super) fn get(&mut self, txn_id: i64) -> Result<Option<u8>> {
        let start = self.initial_slot(txn_id);
        for probe in 0..self.capacity {
            let slot = (start + probe) & (self.capacity - 1);
            let (stored_outcome, stored_txn_id) = self.read_slot(slot)?;
            if stored_outcome == 0 {
                return Ok(None);
            }
            if stored_txn_id == txn_id {
                return Ok(Some(stored_outcome));
            }
        }
        Ok(None)
    }
}

impl Drop for DiskRecoveryOutcomes {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) enum RecoveryOutcomes {
    Memory(rustc_hash::FxHashMap<i64, u8>),
    Disk(DiskRecoveryOutcomes),
}

impl RecoveryOutcomes {
    pub(super) fn get(&mut self, txn_id: i64) -> Result<Option<u8>> {
        match self {
            Self::Memory(outcomes) => Ok(outcomes.get(&txn_id).copied()),
            Self::Disk(outcomes) => outcomes.get(txn_id),
        }
    }

    pub(super) fn counts(&self) -> (usize, usize) {
        match self {
            Self::Memory(outcomes) => outcomes.values().fold((0, 0), |mut counts, outcome| {
                if *outcome == RECOVERY_OUTCOME_COMMITTED {
                    counts.0 += 1;
                } else {
                    counts.1 += 1;
                }
                counts
            }),
            Self::Disk(outcomes) => (outcomes.committed, outcomes.aborted),
        }
    }
}

#[cfg(test)]
pub(super) type WalCloseTestHook = std::sync::Arc<dyn Fn(&Path) + Send + Sync>;

#[cfg(test)]
pub(super) struct WalCloseTestHookGuard<'a> {
    hook: &'a Mutex<Option<WalCloseTestHook>>,
    _owner: std::sync::MutexGuard<'a, ()>,
}

#[cfg(test)]
impl<'a> WalCloseTestHookGuard<'a> {
    pub(super) fn install(wal: &'a WALManager, hook: WalCloseTestHook) -> Self {
        let owner = wal
            .close_test_hook_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *wal.close_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        Self {
            hook: &wal.close_test_hook,
            _owner: owner,
        }
    }
}

#[cfg(test)]
impl Drop for WalCloseTestHookGuard<'_> {
    fn drop(&mut self) {
        *self
            .hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[derive(Debug, Clone, Copy)]
struct ValidatedWalHeader {
    version: u8,
    flags: WalFlags,
    pub(super) lsn: u64,
    previous_lsn: u64,
    entry_size: usize,
}

impl ValidatedWalHeader {
    pub(super) fn decode(
        bytes: &[u8; WAL_HEADER_SIZE as usize],
        path: &Path,
        offset: u64,
    ) -> Result<Self> {
        let context = || format!("{} at byte {}", path.display(), offset);
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != WAL_ENTRY_MAGIC {
            return Err(Error::internal(format!(
                "invalid WAL magic in {}: {:#x}",
                context(),
                magic
            )));
        }
        let version = bytes[4];
        if version != WAL_FORMAT_VERSION {
            return Err(Error::internal(format!(
                "unsupported WAL version in {}: {} (supported: {})",
                context(),
                version,
                WAL_FORMAT_VERSION
            )));
        }
        let flags = WalFlags::from_byte(bytes[5]);
        if flags.as_byte() & !WAL_KNOWN_FLAGS != 0 {
            return Err(Error::internal(format!(
                "unknown WAL flags in {}: {:#x}",
                context(),
                flags.as_byte()
            )));
        }
        let header_size = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        if header_size != WAL_HEADER_SIZE {
            return Err(Error::internal(format!(
                "invalid WAL header size in {}: {} (expected {})",
                context(),
                header_size,
                WAL_HEADER_SIZE
            )));
        }
        if bytes[28..32] != [0u8; 4] {
            return Err(Error::internal(format!(
                "non-zero WAL reserved bytes in {}",
                context()
            )));
        }

        let lsn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let previous_lsn = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        if lsn == 0 || previous_lsn >= lsn {
            return Err(Error::internal(format!(
                "invalid WAL LSN chain in {}: lsn={}, previous_lsn={}",
                context(),
                lsn,
                previous_lsn
            )));
        }

        let entry_size = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        if !(MIN_WAL_RECORD_DATA_SIZE..=MAX_WAL_RECORD_DATA_SIZE).contains(&entry_size) {
            return Err(Error::internal(format!(
                "invalid WAL record size in {}: {} (allowed {}..={})",
                context(),
                entry_size,
                MIN_WAL_RECORD_DATA_SIZE,
                MAX_WAL_RECORD_DATA_SIZE
            )));
        }

        Ok(Self {
            version,
            flags,
            lsn,
            previous_lsn,
            entry_size,
        })
    }
}

pub(super) struct ValidatedWalReader {
    file: BufReader<File>,
    pub(super) path: PathBuf,
    offset: u64,
    previous_record_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WalLifecycle {
    Running,
    Failed,
    Closing,
    Closed,
}

#[derive(Debug)]
pub(super) struct WalTransitionState {
    pub(super) lifecycle: WalLifecycle,
}

#[derive(Debug)]
pub(super) struct WalWriteFailure {
    pub(super) error: Error,
    pub(super) written: usize,
}

pub(super) struct PreparedWalEntry {
    pub(super) lsn: u64,
    pub(super) txn_id: i64,
    pub(super) operation: WALOperationType,
    pub(super) encoded: Vec<u8>,
}

impl PreparedWalEntry {
    pub(super) fn encode(entry: &WALEntry) -> Result<Self> {
        let encoded = entry.encode()?;
        Ok(Self {
            lsn: entry.lsn,
            txn_id: entry.txn_id,
            operation: entry.operation,
            encoded,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedWalGeneration {
    pub(super) path: PathBuf,
    pub(super) name: String,
    pub(super) start_lsn: u64,
    pub(super) end_lsn: u64,
    pub(super) sequence: u64,
    pub(super) identity: WalFileIdentity,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct WalFileIdentity {
    pub(super) len: u64,
    pub(super) modified_nanos: Option<u128>,
    #[cfg(unix)]
    pub(super) device: u64,
    #[cfg(unix)]
    pub(super) inode: u64,
    #[cfg(unix)]
    ctime_seconds: i64,
    #[cfg(unix)]
    ctime_nanos: i64,
}

impl WalFileIdentity {
    pub(super) fn read(path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path).map_err(|error| {
            Error::internal(format!(
                "failed to stat WAL generation {}: {}",
                path.display(),
                error
            ))
        })?;
        let modified_nanos = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                len: metadata.len(),
                modified_nanos,
                device: metadata.dev(),
                inode: metadata.ino(),
                ctime_seconds: metadata.ctime(),
                ctime_nanos: metadata.ctime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                len: metadata.len(),
                modified_nanos,
            })
        }
    }
}

impl ValidatedWalReader {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|e| {
            Error::internal(format!("failed to open WAL file {}: {}", path.display(), e))
        })?;
        Ok(Self {
            file: BufReader::with_capacity(WAL_VALIDATION_READ_BUFFER_SIZE, file),
            path: path.to_path_buf(),
            offset: 0,
            previous_record_lsn: None,
        })
    }

    pub(super) fn next_entry(&mut self) -> Result<Option<WALEntry>> {
        let record_offset = self.offset;
        let mut header_bytes = [0u8; WAL_HEADER_SIZE as usize];
        match self.file.read(&mut header_bytes[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => {}
            Ok(_) => unreachable!(),
            Err(e) => {
                return Err(Error::internal(format!(
                    "failed to read WAL header from {} at byte {}: {}",
                    self.path.display(),
                    record_offset,
                    e
                )));
            }
        }
        self.file.read_exact(&mut header_bytes[1..]).map_err(|e| {
            Error::internal(format!(
                "truncated WAL header in {} at byte {}: {}",
                self.path.display(),
                record_offset,
                e
            ))
        })?;

        let header = ValidatedWalHeader::decode(&header_bytes, &self.path, record_offset)?;
        if let Some(previous) = self.previous_record_lsn {
            if header.previous_lsn != previous || header.lsn <= previous {
                return Err(Error::internal(format!(
                    "discontinuous WAL chain in {} at byte {}: lsn={}, previous_lsn={}, expected_previous={}",
                    self.path.display(),
                    record_offset,
                    header.lsn,
                    header.previous_lsn,
                    previous
                )));
            }
        }

        let data_size = header
            .entry_size
            .checked_add(4)
            .ok_or_else(|| Error::internal("WAL record size overflow"))?;
        let mut data = vec![0u8; data_size];
        self.file.read_exact(&mut data).map_err(|e| {
            Error::internal(format!(
                "truncated WAL record in {} at byte {} (lsn={}): {}",
                self.path.display(),
                record_offset,
                header.lsn,
                e
            ))
        })?;
        let entry = WALEntry::decode_versioned(
            header.version,
            &header_bytes,
            header.lsn,
            header.previous_lsn,
            header.flags,
            &data,
        )?;

        self.offset = record_offset
            .checked_add(WAL_HEADER_SIZE as u64)
            .and_then(|value| value.checked_add(data_size as u64))
            .ok_or_else(|| Error::internal("WAL file offset overflow"))?;
        self.previous_record_lsn = Some(header.lsn);
        Ok(Some(entry))
    }
}
