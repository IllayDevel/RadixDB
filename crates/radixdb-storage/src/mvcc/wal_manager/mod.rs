// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Write-Ahead Log (WAL) Manager
//!
//! Provides durable logging of database operations for crash recovery.
//! Implements the WAL protocol with configurable sync modes.
//!

use radixdb_core::time_compat::{system_time_now, UNIX_EPOCH};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crate::cpu_runtime::StorageCpuLease;
use crate::instrumentation;
use crate::{PersistenceConfig, SyncMode};
use radixdb_catalog::ObjectId;
use radixdb_core::{Error, Result};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// Magic bytes for WAL entry marker ("WALE" in ASCII)
/// Used to detect entry boundaries and partial writes
const WAL_ENTRY_MAGIC: u32 = 0x454C4157;

/// Default maximum WAL file size before rotation (64MB)
pub const DEFAULT_WAL_MAX_SIZE: u64 = 64 * 1024 * 1024;

/// Default flush trigger size (32KB)
pub const DEFAULT_WAL_FLUSH_TRIGGER: u64 = 32 * 1024;

/// Default buffer size (64KB)
pub const DEFAULT_WAL_BUFFER_SIZE: usize = 64 * 1024;

/// Sequential validation/recovery reads every WAL record header and payload.
/// A large bounded buffer prevents two kernel reads per small record while
/// preserving the exact CRC, chain and semantic validation performed by the
/// reader. This is working memory only; it does not change WAL framing.
const WAL_VALIDATION_READ_BUFFER_SIZE: usize = 1024 * 1024;

// ============================================================================
// WAL Entry Header Format V4 (32 bytes)
// ============================================================================
// Provides extensible header with version field and reserved space for future growth.
//
// Layout:
// ┌─────────────────────────────────────────────────────────────────┐
// │ Magic          (4 bytes)  0x454C4157 "WALE"                     │
// │ Version        (1 byte)   Format version (currently 4)          │
// │ Flags          (1 byte)   Bit flags for entry properties        │
// │ Header Size    (2 bytes)  Total header size (allows growth)     │
// │ LSN            (8 bytes)  Log sequence number                   │
// │ Previous LSN   (8 bytes)  LSN of previous entry (chain link)    │
// │ Entry Size     (4 bytes)  Size of data payload                  │
// │ Reserved       (4 bytes)  Reserved for future use               │
// └─────────────────────────────────────────────────────────────────┘

/// Current WAL entry format. V4 replaces mutable table names in row records
/// with the stable catalog ObjectId while retaining the fully checksummed
/// 32-byte framing.
const WAL_FORMAT_VERSION: u8 = 4;

/// WAL entry header size in bytes
const WAL_HEADER_SIZE: u16 = 32;

/// Maximum encoded data portion and maximum logical row payload accepted by
/// both the writer and recovery decoder. Keeping one shared budget prevents a
/// highly-compressible record from being accepted at append time but rejected
/// (or allocating without bound) during recovery.
const MAX_WAL_RECORD_DATA_SIZE: usize = 64 * 1024 * 1024;

/// Fixed fields in the V4 data portion, excluding the logical row payload.
const MIN_WAL_RECORD_DATA_SIZE: usize = 8 + 16 + 8 + 1 + 8 + 4;

/// Every flag currently defined by WAL V4. Bit 7 is not assigned and must not
/// silently select the current decoder.
const WAL_KNOWN_FLAGS: u8 = 0x7f;

/// Write-Ahead Log Manager
pub struct WALManager {
    /// Base path for WAL files
    path: PathBuf,
    /// Current WAL file
    wal_file: Mutex<Option<File>>,
    /// Current WAL file name
    current_wal_file: Mutex<String>,
    /// Current Log Sequence Number
    current_lsn: AtomicU64,
    /// Previous LSN for entry chaining (enables backward traversal)
    previous_lsn: AtomicU64,
    /// Write buffer
    buffer: Mutex<Vec<u8>>,
    /// Flush trigger size
    flush_trigger: u64,
    /// Maximum WAL file size
    max_wal_size: u64,
    /// Last checkpoint LSN
    last_checkpoint: AtomicU64,
    /// Greatest positive generated transaction ID admitted to durable history.
    /// Checkpoint metadata preserves it when the carrying WAL is retired.
    transaction_high_water: AtomicI64,
    /// Sync mode
    sync_mode: SyncMode,
    /// Running flag
    running: AtomicBool,
    /// Owns admission, LSN/chain assignment, buffer transfer, durability
    /// outcome and close. No checkpoint or shutdown boundary may observe a
    /// half-admitted append outside this mutex.
    transition: Mutex<WalTransitionState>,
    /// Process-monotonic origin for SyncMode::Normal deadlines. Wall-clock
    /// rollback must never postpone WAL durability.
    sync_clock_origin: Instant,
    /// Last successful sync offset from `sync_clock_origin`, in nanoseconds.
    last_sync_elapsed_nanos: AtomicU64,
    /// Sync interval in monotonic nanoseconds.
    sync_interval_nanos: u64,
    /// Current file position (for rotation check)
    current_file_position: AtomicU64,
    /// File position covered by the most recent successful fsync.
    last_synced_file_position: AtomicU64,
    /// WAL file sequence number (for rotation)
    wal_sequence: AtomicU64,
    /// CONTROL-owned replay boundary. Generation and LSN are one value so a
    /// concurrent diagnostic replay can never observe a torn checkpoint
    /// publication.
    replay_floor: Mutex<crate::v6::WalReplayFloor>,
    /// Fully validated immutable generations. A generation enters this set
    /// exactly once, immediately before its append owner rotates away. Runtime
    /// retention checks only file identity and never rescans unchanged payload.
    validated_closed_generations: Mutex<Vec<ValidatedWalGeneration>>,
    #[cfg(any(test, feature = "test-hooks"))]
    runtime_generation_validation_bytes: AtomicU64,
    /// Instance-local append hook. A process-global hook can intercept entries
    /// from an unrelated database whose transaction IDs happen to collide.
    #[cfg(any(test, feature = "test-hooks"))]
    append_test_hook: Mutex<Option<WalAppendTestHook>>,
    #[cfg(any(test, feature = "test-hooks"))]
    append_test_hook_owner: Mutex<()>,
    /// Instance-local close hook for the same cross-database isolation rule.
    #[cfg(test)]
    close_test_hook: Mutex<Option<WalCloseTestHook>>,
    #[cfg(test)]
    close_test_hook_owner: Mutex<()>,
}

/// One immutable WAL generation frozen for a physical snapshot.
///
/// The path is supplied by the WAL owner after a durable rotation. Callers do
/// not discover snapshot membership by scanning the WAL directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotWalGeneration {
    generation: crate::v6::WalGeneration,
    path: PathBuf,
    byte_length: u64,
}

impl SnapshotWalGeneration {
    pub(crate) const fn generation(&self) -> crate::v6::WalGeneration {
        self.generation
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) const fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

mod append;
mod checkpoint;
mod open;
mod record;
mod recovery;
mod replay;
mod rotation;

pub use record::{WALEntry, WALOperationType, WalFlags};
pub use recovery::TwoPhaseRecoveryInfo;
use recovery::*;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use recovery::{
    recovery_outcome_spill_count, reset_recovery_outcome_spill_count, WalAppendTestHook,
    WalAppendTestHookGuard, TEST_RECOVERY_OUTCOME_MEMORY_LIMIT,
};

#[cfg(test)]
mod tests;
