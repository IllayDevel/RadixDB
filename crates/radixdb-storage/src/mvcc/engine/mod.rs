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

//! MVCC Storage Engine
//!
//! Provides the main MVCC storage engine implementation.
//!

use arc_swap::{ArcSwap, ArcSwapOption};
use radixdb_catalog::{CatalogPublisher, ObjectId};
use radixdb_core::{CompactArc, I64Map, SmartString};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::Duration;

use radixdb_core::time_compat::Instant;

/// Returns lowercase version of string, avoiding allocation if already lowercase.
/// This is a hot-path optimization - most table names are already lowercase.
#[inline]
fn to_lowercase_cow(s: &str) -> Cow<'_, str> {
    // Fast path: check if all bytes are already lowercase ASCII
    // This avoids allocation for the common case
    if s.bytes().all(|b| !b.is_ascii_uppercase()) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(s.to_lowercase())
    }
}

use super::file_lock::FileLock;

use crate::config::Config;
use crate::expression::Expression;
use crate::index::BTreeIndex;
use crate::instrumentation;
use crate::mvcc::persistence::PendingDmlWalOperation;
use crate::mvcc::wal_manager::WALOperationType;
use crate::mvcc::{
    get_fast_timestamp, CatalogWriteFenceGuard, DdlFenceGuard, MVCCTable, MvccTransaction,
    PersistenceManager, PkIndex, RowVersion, SealFenceGuard, TransactionEngineOperations,
    TransactionRegistry, TransactionVersionStore, TransactionalDdlPreparation,
    TransactionalDdlPublication, VersionStore, VisibilityFenceGuard, INVALID_TRANSACTION_ID,
};
#[cfg(test)]
use crate::traits::PendingIndexDefinition;
use crate::traits::{
    Engine, Index, PendingIndexDrop, PendingSchemaChange, Scanner, SchemaPhysicalTransition, Table,
    Transaction,
};
use crate::volume::column::ROW_GROUP_SIZE;
use crate::volume::manifest::SegmentLevel;
use radixdb_core::{
    DataType, Error, ForeignKeyConstraint, IsolationLevel, Result, Row, Schema,
    SchemaConstraintKind, Value, ValueSet,
};

type StringMap<V> = ahash::AHashMap<String, V>;

mod catalog;
mod catalog_runtime;
mod checkpoint;
mod cleanup;
mod compaction;
mod engine_trait;
mod lifecycle;
mod open;
mod operations;
mod physical_publication;
mod physical_snapshot;
mod recovery;
mod runtime;
mod seal;
mod views;

pub use catalog_runtime::{
    CatalogRuntime, CatalogRuntimeBinder, CatalogRuntimeTable, IntoCatalogRuntimeBinder,
};
pub use cleanup::CleanupHandle;
use operations::EngineOperations;
use runtime::{BackgroundWorkerRuntimeGuard, EngineMaintenanceState, PendingTable};
pub use runtime::{
    EngineCompactionCostSnapshot, EngineLifecycleState, EngineMaintenanceSnapshot,
    EngineRuntimeOperationDetail, EngineRuntimeOperationSnapshot, EngineRuntimeStatsV2,
    EngineRuntimeVisitLimits,
};
#[cfg(test)]
use runtime::{EngineCompactionCostState, RuntimeOperationState};

/// Type alias for a single table entry in the transaction version store
type TxnTableEntry = (SmartString, Arc<RwLock<TransactionVersionStore>>);

/// Type alias for the transaction version store map
/// Structured as txn_id -> [(table_name, store)] for efficient lookup per transaction
/// Uses SmallVec<[T; 2]> since most transactions access 1-2 tables, avoiding heap allocation
type TxnVersionStoreMap = I64Map<SmallVec<[TxnTableEntry; 2]>>;

type CheckpointCandidate = (
    String,
    CompactArc<Schema>,
    Arc<VersionStore>,
    bool,
    usize,
    u64,
);

fn frozen_volume_physical_bytes(volume: &crate::volume::writer::FrozenVolume) -> u64 {
    let data = volume
        .artifact_source()
        .map_or(0, |source| source.layout().reference().byte_length());
    let index = volume
        .artifact_index_source()
        .map_or(0, |source| source.layout().reference().byte_length());
    data.saturating_add(index)
}

const FORCED_CHECKPOINT_HOT_ROWS_UNSEALED: &str =
    "forced checkpoint left committed hot rows unsealed";
const SEAL_ROW_THRESHOLD: usize = 100_000;
const SEAL_INCREMENTAL_THRESHOLD: usize = 10_000;

// A per-table byte threshold cannot bound a database made of many small hot
// owners.  Keep the aggregate policy derived from the existing configured
// first-seal budget so deployments do not gain a second, contradictory knob:
// the soft watermark starts background draining and the hard watermark is the
// point where a later writer must wait for one completed drain cycle.
const TOTAL_HOT_SOFT_MULTIPLIER: usize = 4;
const TOTAL_HOT_HARD_MULTIPLIER: usize = 8;

const fn total_hot_soft_threshold(first_seal_bytes: usize) -> usize {
    first_seal_bytes.saturating_mul(TOTAL_HOT_SOFT_MULTIPLIER)
}

const fn total_hot_hard_threshold(first_seal_bytes: usize) -> usize {
    first_seal_bytes.saturating_mul(TOTAL_HOT_HARD_MULTIPLIER)
}
const RUNTIME_STATS_MAX_TABLES: usize = 4_096;
const RUNTIME_STATS_MAX_SEGMENTS: usize = 16_384;
const RUNTIME_STATS_MAX_TRANSACTIONS: usize = 4_096;
const RUNTIME_STATS_MAX_STAGING_TABLES: usize = 16_384;
const RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS: usize = 64;

fn runtime_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Stable, process-independent identifier for an engine owner name.
///
/// Runtime artifacts need to correlate one table across samples, but must not
/// publish schema names into metric labels. FNV-1a is deliberately used as an
/// identifier rather than as a security primitive.
fn runtime_owner_id(name: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Coordinates the independent hot-memory pressure seal with committing
/// writers. A writer which crossed a configured hot threshold publishes the
/// request after its commit is visible. Later writers wait before acquiring
/// the shared seal fence, so the maintenance worker can drain the committed
/// hot generation instead of letting producers outrun it indefinitely.
struct PressureSealControl {
    requested: AtomicBool,
    worker_active: AtomicBool,
    completed_cycles: AtomicU64,
    wait_lock: Mutex<()>,
    wait_condvar: Condvar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PressureSealCycleOutcome {
    Idle,
    Completed,
    Superseded,
}

impl PressureSealCycleOutcome {
    const fn keeps_request_armed(self, pressure_remains: bool) -> bool {
        matches!(self, Self::Completed) && pressure_remains
    }
}

impl PressureSealControl {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            worker_active: AtomicBool::new(false),
            completed_cycles: AtomicU64::new(0),
            wait_lock: Mutex::new(()),
            wait_condvar: Condvar::new(),
        }
    }

    fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn wait_before_commit(&self) {
        if !self.requested.load(Ordering::Acquire) || !self.worker_active.load(Ordering::Acquire) {
            return;
        }

        let observed_cycle = self.completed_cycles.load(Ordering::Acquire);
        let mut guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        while self.requested.load(Ordering::Acquire)
            && self.worker_active.load(Ordering::Acquire)
            && self.completed_cycles.load(Ordering::Acquire) == observed_cycle
        {
            guard = self
                .wait_condvar
                .wait(guard)
                .unwrap_or_else(|e| e.into_inner());
        }
    }

    fn finish_cycle(&self, still_requested: bool) {
        let _guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.requested.store(still_requested, Ordering::Release);
        // Foreground admission is bounded by one maintenance cycle, not by
        // the time needed to drain every table below its pressure threshold.
        // The request may remain armed for the next cycle while writers that
        // already waited are allowed to make progress and re-arm pressure
        // after their own commit if necessary.
        self.completed_cycles.fetch_add(1, Ordering::Release);
        self.wait_condvar.notify_all();
    }

    fn worker_started(&self) {
        self.worker_active.store(true, Ordering::Release);
    }

    fn worker_stopped(&self) {
        let _guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.worker_active.store(false, Ordering::Release);
        self.requested.store(false, Ordering::Release);
        self.wait_condvar.notify_all();
    }
}

struct PressureSealWorkerGuard(Arc<PressureSealControl>);

impl Drop for PressureSealWorkerGuard {
    fn drop(&mut self) {
        self.0.worker_stopped();
    }
}

/// Admit only sub-tier segments to an ordinary merge. Canonical DATA/INDEX
/// writers own their independent byte ceilings and split an output
/// adaptively, so the retired monolithic metadata envelope is not part of
/// this decision.
#[inline]
fn compaction_segment_is_mergeable(row_count: usize, target_volume_rows: usize) -> bool {
    row_count < target_volume_rows
}

/// Stable L1 segments are coalesced size-tiered instead of remaining one
/// target-sized file per ingest window forever. Keeping the factor identical
/// to the existing compaction threshold bounds the number of sub-tier files
/// without introducing a second public tuning knob in the first release.
#[inline]
fn size_tier_target_rows(target_volume_rows: usize, compact_threshold: usize) -> usize {
    target_volume_rows.saturating_mul(compact_threshold.max(2))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionCandidate {
    manifest_index: usize,
    physical_bytes: u64,
    must_rewrite: bool,
}

/// Pick one bounded contiguous manifest run. Replacing non-contiguous inputs
/// with one output cannot preserve that output's recency relative to an
/// untouched segment between them, so contiguity is a correctness invariant,
/// not merely a scheduling preference.
fn select_bounded_compaction_run(
    candidates: &[CompactionCandidate],
    max_input_segments: usize,
    max_input_bytes: u64,
) -> Vec<usize> {
    let max_input_segments = max_input_segments.max(1);
    let max_input_bytes = max_input_bytes.max(1);

    for start in 0..candidates.len() {
        let mut selected = Vec::new();
        let mut selected_bytes = 0_u64;
        let mut previous_index = None;
        let mut contains_rewrite = false;

        for candidate in &candidates[start..] {
            if previous_index.is_some_and(|previous| candidate.manifest_index != previous + 1) {
                break;
            }
            if selected.len() >= max_input_segments {
                break;
            }
            let next_bytes = selected_bytes.saturating_add(candidate.physical_bytes);
            if next_bytes > max_input_bytes {
                break;
            }

            selected.push(candidate.manifest_index);
            selected_bytes = next_bytes;
            contains_rewrite |= candidate.must_rewrite;
            previous_index = Some(candidate.manifest_index);
        }

        if contains_rewrite || selected.len() >= 2 {
            return selected;
        }
    }

    Vec::new()
}

fn effective_compaction_input_budget(
    max_input_bytes: u64,
    max_output_bytes: u64,
    time_budget_ms: u64,
    io_bytes_per_sec: u64,
) -> u64 {
    let time_limited_input = if time_budget_ms == 0 || io_bytes_per_sec == 0 {
        u64::MAX
    } else {
        io_bytes_per_sec
            .saturating_mul(time_budget_ms)
            .checked_div(2_000)
            .unwrap_or(u64::MAX)
            .max(1)
    };
    max_input_bytes
        .max(1)
        .min(max_output_bytes.max(1))
        .min(time_limited_input)
}

fn compaction_io_rate_per_job(total_bytes_per_sec: u64, concurrent_jobs: usize) -> u64 {
    if total_bytes_per_sec == 0 {
        return 0;
    }
    total_bytes_per_sec
        .checked_div(concurrent_jobs.max(1) as u64)
        .unwrap_or(total_bytes_per_sec)
        .max(1)
}

#[cfg(unix)]
fn compaction_filesystem_available_bytes(path: &Path) -> Result<Option<u64>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        Error::internal(format!(
            "cannot inspect compaction filesystem for path containing NUL: {}",
            path.display()
        ))
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a live NUL-terminated string and a successful statvfs
    // initializes the complete output structure.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(Error::internal(format!(
            "cannot inspect compaction filesystem free space: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: the successful call above initialized `stats`.
    let stats = unsafe { stats.assume_init() };
    Ok(Some(stats.f_bavail.saturating_mul(stats.f_frsize)))
}

#[cfg(not(unix))]
fn compaction_filesystem_available_bytes(_path: &Path) -> Result<Option<u64>> {
    Ok(None)
}

#[derive(Default)]
struct CompactionDiskReservationPool {
    remaining_output_bytes: Mutex<u64>,
}

impl CompactionDiskReservationPool {
    fn reserve(
        self: &Arc<Self>,
        output_dir: &Path,
        max_output_bytes: u64,
        disk_reserve_bytes: u64,
    ) -> Result<CompactionDiskReservation> {
        let requested = max_output_bytes.max(1);
        let mut remaining = self
            .remaining_output_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let new_remaining = remaining.checked_add(requested).ok_or_else(|| {
            compaction_abort(
                "disk_reserve_exhausted",
                "aggregate compaction output reservation overflow",
            )
        })?;
        let required = disk_reserve_bytes
            .checked_add(new_remaining)
            .ok_or_else(|| {
                compaction_abort(
                    "disk_reserve_exhausted",
                    "aggregate compaction disk reserve overflow",
                )
            })?;
        if let Some(available) = compaction_filesystem_available_bytes(output_dir)? {
            if available < required {
                return Err(compaction_abort(
                    "disk_reserve_exhausted",
                    format_args!("available {available} bytes, required {required} bytes"),
                ));
            }
        }
        *remaining = new_remaining;
        Ok(CompactionDiskReservation {
            pool: Arc::clone(self),
            remaining: requested,
        })
    }
}

struct CompactionDiskReservation {
    pool: Arc<CompactionDiskReservationPool>,
    remaining: u64,
}

impl CompactionDiskReservation {
    fn ensure_headroom(&self, output_dir: &Path, disk_reserve_bytes: u64) -> Result<()> {
        let remaining = self
            .pool
            .remaining_output_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let required = disk_reserve_bytes.checked_add(*remaining).ok_or_else(|| {
            compaction_abort(
                "disk_reserve_exhausted",
                "aggregate compaction disk reserve overflow",
            )
        })?;
        if let Some(available) = compaction_filesystem_available_bytes(output_dir)? {
            if available < required {
                return Err(compaction_abort(
                    "disk_reserve_exhausted",
                    format_args!("available {available} bytes, required {required} bytes"),
                ));
            }
        }
        Ok(())
    }

    fn account_output(&mut self, bytes: u64) {
        let consumed = bytes.min(self.remaining);
        self.remaining = self.remaining.saturating_sub(consumed);
        let mut aggregate = self
            .pool
            .remaining_output_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *aggregate = aggregate.saturating_sub(consumed);
    }
}

impl Drop for CompactionDiskReservation {
    fn drop(&mut self) {
        let mut aggregate = self
            .pool
            .remaining_output_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *aggregate = aggregate.saturating_sub(self.remaining);
    }
}

struct CompactionExecutionBudget {
    started: Instant,
    time_budget: Option<Duration>,
    io_bytes_per_sec: u64,
    max_output_bytes: u64,
    disk_reserve_bytes: u64,
    read_bytes: u64,
    output_bytes: u64,
    disk_reservation: Option<CompactionDiskReservation>,
}

impl CompactionExecutionBudget {
    fn new(
        time_budget_ms: u64,
        io_bytes_per_sec: u64,
        max_output_bytes: u64,
        disk_reserve_bytes: u64,
    ) -> Self {
        Self {
            started: Instant::now(),
            time_budget: (time_budget_ms > 0).then(|| Duration::from_millis(time_budget_ms)),
            io_bytes_per_sec,
            max_output_bytes: max_output_bytes.max(1),
            disk_reserve_bytes,
            read_bytes: 0,
            output_bytes: 0,
            disk_reservation: None,
        }
    }

    fn attach_disk_reservation(&mut self, reservation: CompactionDiskReservation) {
        self.disk_reservation = Some(reservation);
    }

    fn ensure_time(&self) -> Result<()> {
        if self
            .time_budget
            .is_some_and(|budget| self.started.elapsed() >= budget)
        {
            return Err(compaction_abort(
                "time_budget_exceeded",
                format_args!("elapsed {} ms", self.started.elapsed().as_millis()),
            ));
        }
        Ok(())
    }

    fn throttle(&self, ensure_live: &(dyn Fn() -> Result<()> + Send + Sync)) -> Result<()> {
        self.ensure_time()?;
        ensure_live()?;
        if self.io_bytes_per_sec == 0 {
            return Ok(());
        }

        let accounted_bytes = self.read_bytes.saturating_add(self.output_bytes);
        let desired_nanos = (u128::from(accounted_bytes))
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(self.io_bytes_per_sec))
            .unwrap_or(u128::MAX)
            .min(u128::from(u64::MAX)) as u64;
        let desired = Duration::from_nanos(desired_nanos);
        while self.started.elapsed() < desired {
            self.ensure_time()?;
            ensure_live()?;
            let remaining = desired.saturating_sub(self.started.elapsed());
            std::thread::sleep(remaining.min(Duration::from_millis(50)));
        }
        self.ensure_time()
    }

    fn account_read_bytes(
        &mut self,
        bytes: u64,
        ensure_live: &(dyn Fn() -> Result<()> + Send + Sync),
    ) -> Result<()> {
        self.read_bytes = self.read_bytes.max(bytes);
        self.throttle(ensure_live)
    }

    fn ensure_output_headroom(&self, output_dir: &Path) -> Result<()> {
        self.ensure_time()?;
        if let Some(reservation) = &self.disk_reservation {
            return reservation.ensure_headroom(output_dir, self.disk_reserve_bytes);
        }
        let remaining_output = self.max_output_bytes.saturating_sub(self.output_bytes);
        let required = self.disk_reserve_bytes.saturating_add(remaining_output);
        if let Some(available) = compaction_filesystem_available_bytes(output_dir)? {
            if available < required {
                return Err(compaction_abort(
                    "disk_reserve_exhausted",
                    format_args!("available {available} bytes, required {required} bytes"),
                ));
            }
        }
        Ok(())
    }

    fn account_output_bytes(
        &mut self,
        bytes: u64,
        ensure_live: &(dyn Fn() -> Result<()> + Send + Sync),
    ) -> Result<()> {
        self.output_bytes = self.output_bytes.saturating_add(bytes);
        if self.output_bytes > self.max_output_bytes {
            return Err(compaction_abort(
                "output_budget_exceeded",
                format_args!(
                    "generated {} bytes, limit {} bytes",
                    self.output_bytes, self.max_output_bytes
                ),
            ));
        }
        self.throttle(ensure_live)?;
        if let Some(reservation) = &mut self.disk_reservation {
            reservation.account_output(bytes);
        }
        Ok(())
    }
}

fn compaction_job_signature(table_name: &str, schema_epoch: u64, segment_ids: &[u64]) -> u64 {
    let mut signature = runtime_owner_id(table_name) ^ schema_epoch.rotate_left(17);
    for segment_id in segment_ids {
        signature = signature
            .wrapping_mul(0x100_0000_01b3)
            .wrapping_add(*segment_id);
    }
    signature.max(1)
}

struct CompactionRetryEntry {
    signature: u64,
    until: Instant,
    until_unix_millis: u64,
    reason: String,
}

const MAX_COMPACTION_RETRY_ENTRIES: usize = 64;

#[derive(Default)]
struct CompactionRetryCooldown {
    active: Mutex<Vec<CompactionRetryEntry>>,
    suppressed: AtomicU64,
}

impl CompactionRetryCooldown {
    fn should_defer(&self, signature: u64) -> bool {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        active.retain(|entry| entry.until > now);
        if active.iter().any(|entry| entry.signature == signature) {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    fn record(&self, signature: u64, cooldown_ms: u64, reason: &str) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cooldown_ms == 0 {
            active.retain(|entry| entry.signature != signature);
            return;
        }
        let now = Instant::now();
        active.retain(|entry| entry.until > now && entry.signature != signature);
        if active.len() == MAX_COMPACTION_RETRY_ENTRIES {
            active.remove(0);
        }
        active.push(CompactionRetryEntry {
            signature,
            until: now + Duration::from_millis(cooldown_ms),
            until_unix_millis: runtime_unix_millis().saturating_add(cooldown_ms),
            reason: reason.chars().take(96).collect(),
        });
    }

    fn clear(&self, signature: u64) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.retain(|entry| entry.signature != signature);
    }

    fn snapshot(&self) -> (bool, u64, u64, String) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        active.retain(|entry| entry.until > now);
        active.last().map_or_else(
            || {
                (
                    false,
                    0,
                    self.suppressed.load(Ordering::Relaxed),
                    String::new(),
                )
            },
            |entry| {
                (
                    true,
                    entry.until_unix_millis,
                    self.suppressed.load(Ordering::Relaxed),
                    entry.reason.clone(),
                )
            },
        )
    }
}

struct CompactionRetryJobGuard<'a> {
    cooldown: &'a CompactionRetryCooldown,
    signature: u64,
    cooldown_ms: u64,
    reason: String,
    finished: bool,
}

impl<'a> CompactionRetryJobGuard<'a> {
    fn new(cooldown: &'a CompactionRetryCooldown, signature: u64, cooldown_ms: u64) -> Self {
        Self {
            cooldown,
            signature,
            cooldown_ms,
            reason: "failed_unclassified".to_string(),
            finished: false,
        }
    }

    fn set_reason(&mut self, reason: &str) {
        self.reason.clear();
        self.reason.extend(reason.chars().take(96));
    }

    fn success(mut self) {
        self.cooldown.clear(self.signature);
        self.finished = true;
    }
}

impl Drop for CompactionRetryJobGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.cooldown
                .record(self.signature, self.cooldown_ms, &self.reason);
        }
    }
}

#[derive(Default)]
struct CompactionJobConcurrencyState {
    active: AtomicU64,
    peak: AtomicU64,
}

impl CompactionJobConcurrencyState {
    fn start_job(&self) -> CompactionJobConcurrencyGuard<'_> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        CompactionJobConcurrencyGuard { state: self }
    }

    fn active(&self) -> u64 {
        self.active.load(Ordering::Acquire)
    }

    fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }
}

struct CompactionJobConcurrencyGuard<'a> {
    state: &'a CompactionJobConcurrencyState,
}

impl Drop for CompactionJobConcurrencyGuard<'_> {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn run_bounded_compaction_jobs<F>(
    tables: &[String],
    max_jobs: usize,
    compact_table: &F,
) -> Result<()>
where
    F: Fn(&str) -> Result<()> + Sync,
{
    let mut seen = FxHashSet::default();
    if tables.iter().any(|table| !seen.insert(table.as_str())) {
        return Err(Error::internal(
            "compaction scheduler received a duplicate table owner",
        ));
    }

    let max_jobs = max_jobs
        .clamp(1, crate::config::MAX_COMPACTION_JOBS)
        .min(tables.len().max(1));
    if max_jobs == 1 {
        for table in tables {
            run_compaction_job_with_stale_replans(table, compact_table)?;
        }
        return Ok(());
    }

    for batch in tables.chunks(max_jobs) {
        let results = std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|table| scope.spawn(move || compact_table(table)))
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(std::thread::ScopedJoinHandle::join)
                .collect::<Vec<_>>()
        });
        let mut first_error = None;
        let mut stale_tables = Vec::new();
        for (table, result) in batch.iter().zip(results) {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) if compaction_publication_is_stale(&error) => {
                    stale_tables.push(table.as_str());
                }
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Ok(Err(_)) => {}
                Err(_) if first_error.is_none() => {
                    first_error = Some(Error::internal("compaction worker panicked"));
                }
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        // Artifact construction may run in parallel, but CONTROL has exactly
        // one physical publisher. A sibling job that loses that optimistic
        // race must discard its private staging tree and re-plan from the new
        // generation instead of publishing a stale replacement graph.
        for table in stale_tables {
            run_compaction_job_with_stale_replans(table, compact_table)?;
        }
    }
    Ok(())
}

const COMPACTION_STALE_PUBLICATION_REASON: &str = "physical_generation_stale";
const COMPACTION_STALE_REPLAN_LIMIT: usize = 2;

fn compaction_publication_is_stale(error: &Error) -> bool {
    compaction_abort_reason(error) == Some(COMPACTION_STALE_PUBLICATION_REASON)
}

fn run_compaction_job_with_stale_replans<F>(table: &str, compact_table: &F) -> Result<()>
where
    F: Fn(&str) -> Result<()> + Sync,
{
    for replan in 0..=COMPACTION_STALE_REPLAN_LIMIT {
        match compact_table(table) {
            Ok(()) => return Ok(()),
            Err(error) if compaction_publication_is_stale(&error) => {
                if replan == COMPACTION_STALE_REPLAN_LIMIT {
                    // The bounded wave is exhausted, but it did not publish
                    // progress. Preserve the final stale result so background
                    // coordination can request a later wave and synchronous
                    // callers cannot mistake retained L0 debt for success.
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded compaction replan loop always returns")
}

#[derive(Debug, Clone, Copy)]
struct L0PressureLimits {
    soft_segments: u64,
    hard_segments: u64,
    soft_bytes: u64,
    hard_bytes: u64,
    soft_wait: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum L0PressureLevel {
    Normal,
    Soft,
    Hard,
}

fn prioritize_compaction_tables(
    candidates: &mut [(String, crate::volume::manifest::L0DebtSnapshot)],
    limits: L0PressureLimits,
) {
    candidates.sort_unstable_by(|(left_name, left_debt), (right_name, right_debt)| {
        classify_l0_pressure(*right_debt, limits)
            .cmp(&classify_l0_pressure(*left_debt, limits))
            .then_with(|| right_debt.segments.cmp(&left_debt.segments))
            .then_with(|| right_debt.physical_bytes.cmp(&left_debt.physical_bytes))
            .then_with(|| left_name.cmp(right_name))
    });
}

/// Bound one advisory background wave without losing knowledge that another
/// pressure snapshot is required. Forced compaction passes `None` and retains
/// its complete-drain behavior.
fn limit_compaction_wave<T>(candidates: &mut Vec<T>, max_tables: Option<usize>) -> bool {
    let Some(max_tables) = max_tables else {
        return false;
    };
    let max_tables = max_tables.max(1);
    let more_candidates = candidates.len() > max_tables;
    candidates.truncate(max_tables);
    more_candidates
}

fn classify_l0_pressure(
    debt: crate::volume::manifest::L0DebtSnapshot,
    limits: L0PressureLimits,
) -> L0PressureLevel {
    if debt.segments >= limits.hard_segments || debt.physical_bytes >= limits.hard_bytes {
        L0PressureLevel::Hard
    } else if debt.segments >= limits.soft_segments || debt.physical_bytes >= limits.soft_bytes {
        L0PressureLevel::Soft
    } else {
        L0PressureLevel::Normal
    }
}

fn is_artifact_adaptive_capacity_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Internal { message }
            if message.starts_with("physical generation publication failed: V6 data artifact ")
                || message.starts_with(
                    "physical generation publication failed: V6 index artifact "
                )
    )
}

/// Process-local scope for generation-bound schema identities. Zero remains
/// reserved for unbound/synthetic identities.
static NEXT_SCHEMA_SCOPE_ID: AtomicU64 = AtomicU64::new(1);

const VIEW_DEFINITION_MARKER_V1: &[u8; 4] = b"RVW1";
const VIEW_DEFINITION_MARKER_V2: &[u8; 4] = b"RVW2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompactionRowRef {
    row_id: i64,
    vol_idx: u32,
    row_idx: u32,
}

const COMPACTION_ROW_REF_ENCODED_LEN: usize = 16;

impl CompactionRowRef {
    fn new(row_id: i64, vol_idx: usize, row_idx: usize) -> Result<Self> {
        let vol_idx = u32::try_from(vol_idx).map_err(|_| {
            Error::internal(format!(
                "compaction volume index {} exceeds artifact-backed metadata limit {}",
                vol_idx,
                u32::MAX
            ))
        })?;
        let row_idx = u32::try_from(row_idx).map_err(|_| {
            Error::internal(format!(
                "compaction row index {} exceeds artifact-backed metadata limit {}",
                row_idx,
                u32::MAX
            ))
        })?;

        Ok(Self {
            row_id,
            vol_idx,
            row_idx,
        })
    }

    fn vol_idx(&self) -> usize {
        self.vol_idx as usize
    }

    fn row_idx(&self) -> usize {
        self.row_idx as usize
    }

    fn encode(self) -> [u8; COMPACTION_ROW_REF_ENCODED_LEN] {
        let mut out = [0u8; COMPACTION_ROW_REF_ENCODED_LEN];
        out[0..8].copy_from_slice(&self.row_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.vol_idx.to_le_bytes());
        out[12..16].copy_from_slice(&self.row_idx.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8; COMPACTION_ROW_REF_ENCODED_LEN]) -> Self {
        Self {
            row_id: i64::from_le_bytes(bytes[0..8].try_into().expect("slice len checked")),
            vol_idx: u32::from_le_bytes(bytes[8..12].try_into().expect("slice len checked")),
            row_idx: u32::from_le_bytes(bytes[12..16].try_into().expect("slice len checked")),
        }
    }
}

enum CompactionRowRefs<'a> {
    #[cfg(test)]
    Slice(&'a [CompactionRowRef]),
    Spool {
        spool: &'a mut CompactionRowRefSpool,
        range: Range<usize>,
    },
}

impl CompactionRowRefs<'_> {
    fn len(&self) -> usize {
        match self {
            #[cfg(test)]
            Self::Slice(refs) => refs.len(),
            Self::Spool { range, .. } => range.len(),
        }
    }

    fn ref_at(&mut self, index: usize) -> Result<CompactionRowRef> {
        match self {
            #[cfg(test)]
            Self::Slice(refs) => refs.get(index).copied().ok_or_else(|| {
                Error::internal(format!("compaction row ref {} out of bounds", index))
            }),
            Self::Spool { spool, range } => {
                if index >= range.len() {
                    return Err(Error::internal(format!(
                        "compaction row ref {} out of bounds",
                        index
                    )));
                }
                spool.ref_at(range.start + index)
            }
        }
    }
}

struct CompactionRowRefSpool {
    path: PathBuf,
    file: File,
    len: usize,
    written_len: usize,
    append_buffer: Vec<u8>,
    cached_range: Option<Range<usize>>,
    cached_refs: Vec<CompactionRowRef>,
    #[cfg(test)]
    append_flushes: usize,
}

const COMPACTION_SPOOL_PREFIX: &str = ".compaction_refs_";
const COMPACTION_SPOOL_SUFFIX: &str = ".tmp";
const COMPACTION_SPOOL_APPEND_BYTES: usize = 64 * 1024;
const COMPACTION_ABORT_PREFIX: &str = "compaction_cooperative_abort:";
static NEXT_COMPACTION_SPOOL_ID: AtomicU64 = AtomicU64::new(1);

fn compaction_abort(reason: &str, detail: impl std::fmt::Display) -> Error {
    Error::internal(format!("{COMPACTION_ABORT_PREFIX}{reason}: {detail}"))
}

fn compaction_abort_reason(error: &Error) -> Option<&str> {
    let Error::Internal { message } = error else {
        return None;
    };
    let marker = message.find(COMPACTION_ABORT_PREFIX)?;
    message[marker + COMPACTION_ABORT_PREFIX.len()..]
        .split(':')
        .next()
}

impl CompactionRowRefSpool {
    fn create(volume_dir: &Path, table_name: &str) -> Result<Self> {
        let table_dir = volume_dir.join(table_name);
        fs::create_dir_all(&table_dir).map_err(|e| {
            Error::internal(format!(
                "compaction row ref spool: create {:?}: {}",
                table_dir, e
            ))
        })?;

        for _ in 0..16 {
            let path = table_dir.join(format!(
                "{}{:016x}{}",
                COMPACTION_SPOOL_PREFIX,
                NEXT_COMPACTION_SPOOL_ID.fetch_add(1, Ordering::Relaxed),
                COMPACTION_SPOOL_SUFFIX
            ));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file,
                        len: 0,
                        written_len: 0,
                        append_buffer: Vec::with_capacity(COMPACTION_SPOOL_APPEND_BYTES),
                        cached_range: None,
                        cached_refs: Vec::new(),
                        #[cfg(test)]
                        append_flushes: 0,
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(Error::internal(format!(
                        "compaction row ref spool: create {:?}: {}",
                        path, err
                    )));
                }
            }
        }

        Err(Error::internal(format!(
            "compaction row ref spool: failed to allocate unique temp file in {:?}",
            table_dir
        )))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn append(&mut self, row_ref: CompactionRowRef) -> Result<()> {
        self.append_buffer.extend_from_slice(&row_ref.encode());
        self.len = self
            .len
            .checked_add(1)
            .ok_or_else(|| Error::internal("compaction row ref spool length overflows"))?;
        self.cached_range = None;
        self.cached_refs.clear();
        if self.append_buffer.len() >= COMPACTION_SPOOL_APPEND_BYTES {
            self.flush_appends()?;
        }
        Ok(())
    }

    fn flush_appends(&mut self) -> Result<()> {
        if self.append_buffer.is_empty() {
            return Ok(());
        }
        let byte_offset = self
            .written_len
            .checked_mul(COMPACTION_ROW_REF_ENCODED_LEN)
            .and_then(|offset| u64::try_from(offset).ok())
            .ok_or_else(|| Error::internal("compaction row ref spool write offset overflows"))?;
        self.file
            .seek(SeekFrom::Start(byte_offset))
            .map_err(|error| Error::internal(format!("compaction spool seek: {error}")))?;
        let write_started = Instant::now();
        self.file.write_all(&self.append_buffer).map_err(|error| {
            Error::internal(format!("compaction spool buffered append: {error}"))
        })?;
        crate::instrumentation::record_compaction_spool_write(
            self.append_buffer.len() as u64,
            write_started.elapsed(),
        );
        let refs = self.append_buffer.len() / COMPACTION_ROW_REF_ENCODED_LEN;
        self.written_len = self
            .written_len
            .checked_add(refs)
            .ok_or_else(|| Error::internal("compaction written row count overflows"))?;
        self.append_buffer.clear();
        #[cfg(test)]
        {
            self.append_flushes += 1;
        }
        Ok(())
    }

    fn ref_at(&mut self, index: usize) -> Result<CompactionRowRef> {
        if index >= self.len {
            return Err(Error::internal(format!(
                "compaction row ref {} out of bounds",
                index
            )));
        }

        if self
            .cached_range
            .as_ref()
            .is_some_and(|range| range.start <= index && index < range.end)
        {
            let range = self.cached_range.as_ref().expect("range checked above");
            return self
                .cached_refs
                .get(index - range.start)
                .copied()
                .ok_or_else(|| Error::internal("compaction row ref spool cache missing entry"));
        }

        self.load_ref_group(index)?;
        self.ref_at(index)
    }

    fn load_ref_group(&mut self, index: usize) -> Result<()> {
        self.flush_appends()?;
        self.file
            .flush()
            .map_err(|e| Error::internal(format!("compaction row ref spool: flush: {}", e)))?;
        let start = (index / ROW_GROUP_SIZE) * ROW_GROUP_SIZE;
        let end = (start + ROW_GROUP_SIZE).min(self.len);
        let count = end - start;
        let byte_len = count
            .checked_mul(COMPACTION_ROW_REF_ENCODED_LEN)
            .ok_or_else(|| Error::internal("compaction row ref spool read length overflows"))?;
        let byte_offset = start
            .checked_mul(COMPACTION_ROW_REF_ENCODED_LEN)
            .and_then(|offset| u64::try_from(offset).ok())
            .ok_or_else(|| Error::internal("compaction row ref spool read offset overflows"))?;

        let mut bytes = vec![0u8; byte_len];
        self.file
            .seek(SeekFrom::Start(byte_offset))
            .map_err(|e| Error::internal(format!("compaction row ref spool: seek: {}", e)))?;
        let read_started = Instant::now();
        self.file
            .read_exact(&mut bytes)
            .map_err(|e| Error::internal(format!("compaction row ref spool: read: {}", e)))?;
        crate::instrumentation::record_compaction_spool_read(
            byte_len as u64,
            read_started.elapsed(),
        );

        let mut refs = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(COMPACTION_ROW_REF_ENCODED_LEN) {
            refs.push(CompactionRowRef::decode(
                chunk
                    .try_into()
                    .expect("chunks_exact yields exact ref bytes"),
            ));
        }
        self.cached_refs = refs;
        self.cached_range = Some(start..end);
        Ok(())
    }
}

impl Drop for CompactionRowRefSpool {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct CompactionSealRowSource<'a> {
    refs: CompactionRowRefs<'a>,
    volumes: &'a [(u64, Arc<crate::volume::writer::FrozenVolume>)],
    mappings: &'a [crate::volume::writer::ColumnMapping],
    cache: &'a mut crate::volume::writer::CompactionBlockCache,
}

/// Replayable logical row source used only by the canonical DATA artifact
/// compaction writer. This is deliberately local to the engine: the retired
/// seal format no longer owns the production compaction contract.
trait CompactionRowSource {
    fn len(&self) -> usize;
    fn row_id(&mut self, index: usize) -> Result<i64>;
    fn with_value<T>(
        &mut self,
        row_idx: usize,
        col_idx: usize,
        f: impl FnOnce(&Value) -> Result<T>,
    ) -> Result<T>;
}

impl<'a> CompactionSealRowSource<'a> {
    #[cfg(test)]
    fn new(
        refs: &'a [CompactionRowRef],
        volumes: &'a [(u64, Arc<crate::volume::writer::FrozenVolume>)],
        mappings: &'a [crate::volume::writer::ColumnMapping],
        cache: &'a mut crate::volume::writer::CompactionBlockCache,
    ) -> Self {
        Self {
            refs: CompactionRowRefs::Slice(refs),
            volumes,
            mappings,
            cache,
        }
    }

    #[cfg(test)]
    fn new_spooled(
        refs: &'a mut CompactionRowRefSpool,
        volumes: &'a [(u64, Arc<crate::volume::writer::FrozenVolume>)],
        mappings: &'a [crate::volume::writer::ColumnMapping],
        cache: &'a mut crate::volume::writer::CompactionBlockCache,
    ) -> Self {
        let len = refs.len();
        Self::new_spooled_range(refs, 0..len, volumes, mappings, cache)
    }

    fn new_spooled_range(
        refs: &'a mut CompactionRowRefSpool,
        range: Range<usize>,
        volumes: &'a [(u64, Arc<crate::volume::writer::FrozenVolume>)],
        mappings: &'a [crate::volume::writer::ColumnMapping],
        cache: &'a mut crate::volume::writer::CompactionBlockCache,
    ) -> Self {
        Self {
            refs: CompactionRowRefs::Spool { spool: refs, range },
            volumes,
            mappings,
            cache,
        }
    }
}

impl CompactionRowSource for CompactionSealRowSource<'_> {
    fn len(&self) -> usize {
        self.refs.len()
    }

    fn row_id(&mut self, index: usize) -> Result<i64> {
        self.refs.ref_at(index).map(|row_ref| row_ref.row_id)
    }

    fn with_value<T>(
        &mut self,
        row_idx: usize,
        col_idx: usize,
        f: impl FnOnce(&Value) -> Result<T>,
    ) -> Result<T> {
        let row_ref = self.refs.ref_at(row_idx)?;
        let vol_idx = row_ref.vol_idx();
        let input_row_idx = row_ref.row_idx();
        let (_, volume) = self.volumes.get(vol_idx).ok_or_else(|| {
            Error::internal(format!(
                "compaction row ref {} has invalid volume index {}",
                row_idx, vol_idx
            ))
        })?;
        let mapping = self.mappings.get(vol_idx).ok_or_else(|| {
            Error::internal(format!(
                "compaction row ref {} has no column mapping for volume {}",
                row_idx, vol_idx
            ))
        })?;
        let value =
            volume.get_value_for_compaction_cached(input_row_idx, col_idx, mapping, self.cache)?;
        f(&value)
    }
}

/// Single-pass adapter from the bounded compaction spool to the canonical DATA
/// writer. It materializes exactly one logical row at a time while the
/// operation-wide block cache retains at most one row group per input segment.
struct CompactionArtifactRows<'a> {
    source: CompactionSealRowSource<'a>,
    column_count: usize,
    next_row: usize,
}

impl<'a> CompactionArtifactRows<'a> {
    fn new(source: CompactionSealRowSource<'a>, column_count: usize) -> Self {
        Self {
            source,
            column_count,
            next_row: 0,
        }
    }
}

impl Iterator for CompactionArtifactRows<'_> {
    type Item = crate::v6::FormatResult<crate::v6::SourceRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row >= CompactionRowSource::len(&self.source) {
            return None;
        }
        let row_index = self.next_row;
        self.next_row += 1;
        Some((|| {
            let row_id = crate::v6::encode_runtime_row_id(
                CompactionRowSource::row_id(&mut self.source, row_index)
                    .map_err(compaction_source_format_error)?,
            );
            let mut values = Vec::with_capacity(self.column_count);
            for column_index in 0..self.column_count {
                let value = CompactionRowSource::with_value(
                    &mut self.source,
                    row_index,
                    column_index,
                    |value| Ok(value.clone()),
                )
                .map_err(compaction_source_format_error)?;
                values.push(value);
            }
            Ok(crate::v6::SourceRow::new(row_id, values))
        })())
    }
}

fn compaction_source_format_error(error: Error) -> crate::v6::FormatError {
    crate::v6::FormatError::InvalidPublicationGraph {
        detail: format!("compaction source read failed: {error}"),
    }
}

/// Clone the concrete registry used by the production visibility path.
#[inline]
fn registry_as_visibility_checker(registry: &Arc<TransactionRegistry>) -> Arc<TransactionRegistry> {
    Arc::clone(registry)
}

/// Remove FK constraints referencing `parent_table_lower` from all child schemas.
/// Updates both the schemas map and each affected VersionStore's schema.
/// Caller must hold schemas as write-locked and version_stores as read-locked.
fn strip_fk_references(
    schemas: &mut FxHashMap<String, CompactArc<Schema>>,
    version_stores: &FxHashMap<String, Arc<VersionStore>>,
    parent_table_lower: &str,
) {
    let children_to_update: Vec<String> = schemas
        .iter()
        .filter(|(_, schema)| {
            schema
                .foreign_keys
                .iter()
                .any(|fk| fk.referenced_table == parent_table_lower)
        })
        .map(|(name, _)| name.clone())
        .collect();

    for child_name in &children_to_update {
        if let Some(old_schema_arc) = schemas.get(child_name) {
            let old_schema: &Schema = old_schema_arc;
            let mut new_schema = old_schema.clone();
            new_schema
                .foreign_keys
                .retain(|fk| fk.referenced_table != parent_table_lower);
            if new_schema.foreign_keys.len() < old_schema.foreign_keys.len() {
                new_schema.constraints.retain(|constraint| {
                    !matches!(
                        &constraint.kind,
                        SchemaConstraintKind::ForeignKey {
                            referenced_table,
                            ..
                        } if referenced_table == parent_table_lower
                    )
                });
                new_schema
                    .finish_catalog_mutation()
                    .expect("dropping a referenced table must preserve child schema invariants");
                let new_arc = CompactArc::new(new_schema);

                if let Some(vs) = version_stores.get(child_name.as_str()) {
                    *vs.schema_mut() = new_arc.clone();
                }

                schemas.insert(child_name.clone(), new_arc);
            }
        }
    }
}

/// Try to parse a default expression as a simple literal value.
/// Handles integers, floats, booleans, strings, and NULL without requiring the full SQL parser.
/// Returns None for complex expressions (function calls, etc.) that can't be precomputed.
fn try_parse_default_literal(expr: &str, data_type: DataType) -> Option<Value> {
    let expr = expr.trim();

    // NULL
    if expr.eq_ignore_ascii_case("null") {
        return Some(Value::null(data_type));
    }

    // Boolean
    if expr.eq_ignore_ascii_case("true") {
        return Some(Value::Boolean(true));
    }
    if expr.eq_ignore_ascii_case("false") {
        return Some(Value::Boolean(false));
    }

    // String literal (single-quoted) — coerce to target data type so that
    // e.g. TIMESTAMP DEFAULT '2024-01-01' is restored as a Timestamp, not Text.
    if expr.len() >= 2 && expr.starts_with('\'') && expr.ends_with('\'') {
        let inner = &expr[1..expr.len() - 1];
        // Unescape doubled single quotes
        let unescaped = inner.replace("''", "'");
        let text_val = Value::from(unescaped.as_str());
        return Some(text_val.coerce_to_type(data_type));
    }

    // Integer — coerce to target type (e.g. FLOAT DEFAULT 42 should be Float(42.0))
    if let Ok(v) = expr.parse::<i64>() {
        return Some(Value::Integer(v).coerce_to_type(data_type));
    }

    // Float — coerce to target type
    if let Ok(v) = expr.parse::<f64>() {
        return Some(Value::Float(v).coerce_to_type(data_type));
    }

    // Complex expressions (NOW(), CURRENT_TIMESTAMP, etc.) cannot be recovered
    // from old WAL/snapshot formats — they require default_value persistence.
    None
}

/// Register the authoritative uniqueness owner for every declared primary key.
/// INTEGER keeps the virtual row-id PkIndex fast path; other single-column keys
/// use a unique BTreeIndex. The index is schema-derived and rebuilt on every
/// create/recovery path without separate WAL metadata.
fn schema_derived_pk_index_name(schema: &Schema) -> Option<String> {
    let pk_indices = schema.primary_key_indices();
    if pk_indices.is_empty() {
        return None;
    }
    debug_assert_eq!(pk_indices.len(), 1, "schema validation owns PK arity");
    let pk_col = &schema.columns[pk_indices[0]];
    Some(format!("__pk_{}_{}", schema.table_name_lower, pk_col.name))
}

fn is_schema_derived_pk_index(schema: &Schema, index: &dyn Index) -> bool {
    if index.index_type() == radixdb_core::IndexType::PrimaryKey {
        return true;
    }
    let Some(_index_name) = schema_derived_pk_index_name(schema) else {
        return false;
    };
    let pk_indices = schema.primary_key_indices();
    let pk_col = &schema.columns[pk_indices[0]];
    // A table rename deliberately does not rewrite the private index name.
    // The reserved prefix plus the unchanged PK-column suffix keeps the
    // schema-derived BTree owner identifiable across that rename.
    index.name().starts_with("__pk_")
        && index.name().ends_with(&format!("_{}", pk_col.name_lower))
        && index.is_unique()
        && index.column_ids() == [pk_col.id as i32]
}

fn register_pk_index(schema: &Schema, version_store: &Arc<VersionStore>) -> Result<()> {
    let Some(index_name) = schema_derived_pk_index_name(schema) else {
        return Ok(());
    };
    let pk_indices = schema.primary_key_indices();
    let pk_col = &schema.columns[pk_indices[0]];
    let pk_index: Arc<dyn Index> = if pk_col.data_type == DataType::Integer {
        Arc::new(PkIndex::new(
            index_name.clone(),
            schema.table_name.clone(),
            pk_col.id as i32,
            pk_col.name.clone(),
        ))
    } else {
        Arc::new(BTreeIndex::new(
            index_name.clone(),
            schema.table_name.clone(),
            pk_col.id as i32,
            pk_col.name.clone(),
            pk_col.data_type,
            true,
            0,
        ))
    };
    version_store.add_index(index_name, pk_index)
}

/// View definition storing the query that defines the view
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    /// View name (lowercase for case-insensitive lookup)
    pub name: String,
    /// Original view name (preserves case)
    pub original_name: String,
    /// The SQL query string that defines the view
    pub query: String,
    /// Normalized tables/views referenced by the bound SELECT tree.
    pub dependencies: Vec<String>,
    bound_query: String,
    bound_dependencies: Vec<String>,
}

/// Composition callback used to rebind persisted SQL above the storage layer.
pub type ViewDependencyBinder = fn(&str) -> Result<Vec<String>>;

fn validate_bound_view_dependencies(dependencies: &[String]) -> Result<()> {
    if let Some(dependency) = dependencies.iter().find(|dependency| {
        dependency.is_empty() || dependency.as_str() != dependency.to_lowercase()
    }) {
        return Err(Error::invalid_argument(format!(
            "view dependency '{dependency}' is not a normalized non-empty name"
        )));
    }
    if dependencies
        .windows(2)
        .any(|pair| pair[0].as_str() >= pair[1].as_str())
    {
        return Err(Error::invalid_argument(
            "view dependencies must be sorted and unique",
        ));
    }
    Ok(())
}

impl ViewDefinition {
    /// Construct a definition from an SQL-layer-bound dependency descriptor.
    pub fn from_bound_query(name: &str, query: String, dependencies: Vec<String>) -> Result<Self> {
        if query.trim().is_empty() {
            return Err(Error::invalid_argument("view query must not be empty"));
        }
        validate_bound_view_dependencies(&dependencies)?;
        Ok(Self {
            name: name.to_lowercase(),
            original_name: name.to_string(),
            bound_query: query.clone(),
            bound_dependencies: dependencies.clone(),
            query,
            dependencies,
        })
    }

    /// Serialize view definition to binary format for WAL
    pub fn serialize(&self) -> Result<Vec<u8>> {
        use super::persistence::{checked_u16_len, checked_u32_len};

        validate_bound_view_dependencies(&self.dependencies)?;
        if self.query != self.bound_query || self.dependencies != self.bound_dependencies {
            return Err(Error::invalid_argument(format!(
                "view '{}' carries a stale dependency graph",
                self.original_name
            )));
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(VIEW_DEFINITION_MARKER_V2);

        // Original name (length-prefixed)
        buf.extend_from_slice(
            &checked_u16_len("view name", self.original_name.len())?.to_le_bytes(),
        );
        buf.extend_from_slice(self.original_name.as_bytes());

        // Query (length-prefixed, using u32 for longer queries)
        buf.extend_from_slice(&checked_u32_len("view query", self.query.len())?.to_le_bytes());
        buf.extend_from_slice(self.query.as_bytes());

        buf.extend_from_slice(
            &checked_u16_len("view dependency count", self.dependencies.len())?.to_le_bytes(),
        );
        for dependency in &self.dependencies {
            buf.extend_from_slice(
                &checked_u16_len("view dependency", dependency.len())?.to_le_bytes(),
            );
            buf.extend_from_slice(dependency.as_bytes());
        }

        Ok(buf)
    }

    /// Deserialize view definition from binary format
    pub fn deserialize(
        data: &[u8],
        dependency_binder: ViewDependencyBinder,
    ) -> radixdb_core::Result<Self> {
        let is_v1 = data.starts_with(VIEW_DEFINITION_MARKER_V1);
        if !is_v1 && !data.starts_with(VIEW_DEFINITION_MARKER_V2) {
            return Err(Error::internal(
                "unsupported unversioned view definition; expected RVW1/RVW2 marker",
            ));
        }
        let mut pos = VIEW_DEFINITION_MARKER_V2.len();

        // Original name
        if pos + 2 > data.len() {
            return Err(radixdb_core::Error::internal(
                "invalid view: missing name length",
            ));
        }
        let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + name_len > data.len() {
            return Err(radixdb_core::Error::internal("invalid view: missing name"));
        }
        let original_name = String::from_utf8(data[pos..pos + name_len].to_vec())
            .map_err(|e| radixdb_core::Error::internal(format!("invalid view name: {}", e)))?;
        pos += name_len;

        // Query
        if pos + 4 > data.len() {
            return Err(radixdb_core::Error::internal(
                "invalid view: missing query length",
            ));
        }
        let query_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + query_len > data.len() {
            return Err(radixdb_core::Error::internal("invalid view: missing query"));
        }
        let query = String::from_utf8(data[pos..pos + query_len].to_vec())
            .map_err(|e| radixdb_core::Error::internal(format!("invalid view query: {}", e)))?;
        pos += query_len;
        let bound_dependencies = dependency_binder(&query)?;
        validate_bound_view_dependencies(&bound_dependencies)?;
        let dependencies = if is_v1 {
            bound_dependencies
        } else {
            if pos + 2 > data.len() {
                return Err(Error::internal("invalid view: missing dependency count"));
            }
            let count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let mut dependencies = Vec::with_capacity(count);
            for _ in 0..count {
                if pos + 2 > data.len() {
                    return Err(Error::internal("invalid view: missing dependency length"));
                }
                let len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                let end = pos
                    .checked_add(len)
                    .filter(|&end| end <= data.len())
                    .ok_or_else(|| Error::internal("invalid view: missing dependency"))?;
                let dependency = String::from_utf8(data[pos..end].to_vec()).map_err(|error| {
                    Error::internal(format!("invalid view dependency: {error}"))
                })?;
                dependencies.push(dependency.to_lowercase());
                pos = end;
            }
            dependencies.sort_unstable();
            dependencies.dedup();
            if dependencies != bound_dependencies {
                return Err(Error::internal(
                    "invalid view definition: dependency graph does not match query",
                ));
            }
            dependencies
        };
        if pos != data.len() {
            return Err(Error::internal("invalid view definition: trailing bytes"));
        }

        Self::from_bound_query(&original_name, query, dependencies)
    }
}

/// Cached reverse FK mapping: (schema_epoch, parent_table → Arc<Vec<(child_table, constraint)>>)
/// Arc-wrapped so lookups are a ref-count bump (no Vec clone on every FK check).
type FkReverseCache = (u64, StringMap<Arc<Vec<(String, ForeignKeyConstraint)>>>);

/// A table created by an explicit transaction but not published yet.
///
/// Keeping it outside `schemas`/`version_stores` is the actual visibility
/// boundary for transactional CREATE TABLE: the owning transaction can use the
/// store immediately, while every other transaction continues to see the old
/// catalog until commit publishes the entry.
/// MVCC Storage Engine
///
/// Provides multi-version concurrency control with snapshot isolation.
pub struct MVCCEngine {
    /// Database path (empty for in-memory)
    path: String,
    /// Configuration
    config: RwLock<Config>,
    /// Composition port for rebuilding persisted partial-index predicates.
    /// The default constructor is intentionally fail-closed; SQL-facing
    /// facades install the executor-owned binder before recovery begins.
    partial_index_predicate_binder: crate::index::PartialIndexPredicateBinder,
    /// Composition port for SQL CHECK evaluation at commit certification.
    /// Storage sees only the prepared row-validation contract.
    row_validator_binder: OnceLock<crate::validation::RowValidatorBinder>,
    /// Composition port for rebinding persisted view SQL to a neutral catalog
    /// dependency descriptor without importing the SQL parser into storage.
    view_dependency_binder: OnceLock<ViewDependencyBinder>,
    /// Composition port which binds one recovered immutable catalog before
    /// any DML WAL record is allowed to mutate runtime state.
    catalog_runtime_binder: OnceLock<CatalogRuntimeBinder>,
    /// Table schemas (Arc-wrapped for safe sharing with transactions)
    /// Each schema is also Arc-wrapped to avoid cloning on lookup (critical for PK fast path)
    schemas: Arc<RwLock<FxHashMap<String, CompactArc<Schema>>>>,
    /// Version stores for each table (Arc-wrapped for safe sharing with transactions)
    version_stores: Arc<RwLock<FxHashMap<String, Arc<VersionStore>>>>,
    /// Transaction-local CREATE TABLE reservations, keyed by lowercase name.
    pending_tables: Arc<RwLock<FxHashMap<String, PendingTable>>>,
    /// Transaction registry
    registry: Arc<TransactionRegistry>,
    /// Whether the engine is open
    open: AtomicBool,
    /// Set only after the first successful Ready publication. A closed engine
    /// owns terminal resources (closed WAL/stores) and is not reusable; callers
    /// must construct a new instance for a later open.
    opened_once: AtomicBool,
    /// Typed lifecycle outcome. `open == true` is only the Ready fast-path bit;
    /// diagnostics and server projection read this owner.
    lifecycle: RwLock<EngineLifecycleState>,
    /// Per-engine identity for bounded runtime snapshots. This is diagnostic
    /// sequence state only and is never persisted.
    runtime_snapshot_sequence: AtomicU64,
    /// Lock-free operation ownership and outcome telemetry for seal,
    /// compaction, checkpoint and the background coordinator.
    runtime_maintenance: Arc<EngineMaintenanceState>,
    /// True only after the current close transition has completed its final
    /// checkpoint boundary. A WAL-close retry must not attempt to checkpoint
    /// again through a manager that has already stopped admission.
    shutdown_checkpoint_complete: AtomicBool,
    /// Cache of transaction version stores per (txn_id, table_name) for proper commit/rollback
    /// (Arc-wrapped for safe sharing with transactions)
    txn_version_stores: Arc<RwLock<TxnVersionStoreMap>>,
    /// View definitions (Arc for cheap cloning on lookup)
    views: Arc<RwLock<FxHashMap<String, Arc<ViewDefinition>>>>,
    /// Single immutable logical-catalog publication owner.
    ///
    /// The surrounding `ArcSwap` is replaced only while opening or restoring
    /// a complete database generation. Ordinary DDL publishes successors
    /// through the inner `CatalogPublisher`; readers pin one immutable
    /// generation and never observe partially updated schema maps.
    catalog_publisher: Arc<ArcSwap<CatalogPublisher>>,
    /// CONTROL-selected immutable physical generation owner.
    physical_generation: Arc<ArcSwapOption<crate::v6::PhysicalGenerationPublisher>>,
    /// Persistence manager for WAL and snapshot operations.
    ///
    /// Persistent construction is deliberately deferred until `open_engine()`
    /// owns the database lock. `ArcSwapOption` lets failed startup discard a
    /// partially initialized backend while transaction callbacks retain safe
    /// snapshots of a ready manager.
    persistence: Arc<ArcSwapOption<PersistenceManager>>,
    /// Flag to indicate we're loading from disk to avoid triggering redundant WAL writes
    /// (Arc-wrapped for safe sharing with transactions)
    loading_from_disk: Arc<AtomicBool>,
    /// File lock to prevent multiple processes from accessing the same database
    file_lock: Mutex<Option<FileLock>>,
    /// Serializes the closed -> opening -> ready transition.
    startup_mutex: Mutex<()>,
    /// Schema epoch counter - increments on any CREATE/ALTER/DROP TABLE
    /// Used for fast cache invalidation without HashMap lookup
    schema_epoch: Arc<AtomicU64>,
    /// Process-local owner of generation-scoped schema IDs. This is never
    /// persisted and is regenerated on reopen by design.
    schema_scope_id: u64,
    /// Handle for the background cleanup thread (None if not started)
    cleanup_handle: Mutex<Option<CleanupHandle>>,
    /// Independent bounded I/O worker for proactive operating-system page
    /// cache population. It never owns query locks or database payload bytes.
    page_cache_warmup: Arc<crate::page_cache::PageCacheWarmupController>,
    page_cache_warmup_handle: Mutex<Option<crate::page_cache::PageCacheWarmupHandle>>,
    /// Independent memory-pressure seal coordinator. Checkpoint cadence is a
    /// durability policy and must not be the trigger that bounds hot MVCC RAM.
    pressure_seal: Arc<PressureSealControl>,
    /// Cached reverse FK mapping: parent_table → Vec<(child_table, FK constraint)>
    /// Rebuilt lazily on schema_epoch change. Zero cost for non-FK databases.
    fk_reverse_cache: RwLock<FkReverseCache>,
    /// Snapshot timestamps loaded per table — used to pair HNSW graph files with their
    /// matching data snapshots during WAL replay.
    /// Per-table segment managers (owns segments, delete vectors, manifest).
    /// Key: lowercase table name. Replaces frozen_volumes + volume_tombstones.
    segment_managers: Arc<RwLock<FxHashMap<String, Arc<crate::volume::manifest::SegmentManager>>>>,
    /// Database-local admission owner shared by every CPU-heavy seal and
    /// compaction output stage.
    storage_cpu_runtime: Arc<crate::cpu_runtime::StorageCpuRuntime>,
    /// When true, seal_hot_buffers bypasses thresholds and seals all rows.
    /// Set during close_engine to ensure all data is in volumes before shutdown.
    force_seal_all: AtomicBool,
    /// Prevents concurrent checkpoint cycles (background thread vs PRAGMA SNAPSHOT).
    /// Without this, two concurrent seal+compact runs can each read the same old
    /// segments, produce overlapping compacted volumes, and delete each other's data.
    checkpoint_mutex: Mutex<()>,
    /// Seal fence: commits acquire READ (shared, ~5ns), micro-seal acquires WRITE
    /// (exclusive, brief ~100ms) to create a quiet moment where all_hot_empty can
    /// be true. This enables WAL truncation under continuous writes.
    seal_fence: Arc<parking_lot::RwLock<()>>,
    /// Serializes catalog publication against statement planning/execution.
    ddl_fence: Arc<parking_lot::RwLock<()>>,
    /// Admits one auto-commit catalog mutation from pin through commit.
    /// Kept separate from `ddl_fence` because storage DDL takes that lock
    /// internally while staging and publishing physical objects.
    catalog_write_fence: Arc<parking_lot::Mutex<()>>,
    /// Serializes the logical cross-table commit visibility point against
    /// general SELECT construction.
    visibility_fence: Arc<parking_lot::RwLock<()>>,
    /// Linearizes Snapshot Isolation admission with physical maintenance.
    ///
    /// A snapshot holds the exclusive side only while its immutable registry
    /// boundary is installed. Seal and compaction hold the shared side, so
    /// their established concurrency remains intact, and then decline work if
    /// an older snapshot is still active. This prevents a snapshot from
    /// starting inside a physical-generation rewrite without pinning a lock
    /// for the transaction lifetime.
    snapshot_maintenance_fence: Arc<parking_lot::RwLock<()>>,
    /// True while a background compaction thread is running.
    /// Background checkpoint skips compaction when set. Forced compaction
    /// (PRAGMA CHECKPOINT, close, restore) waits for it to finish first.
    compaction_running: Arc<AtomicBool>,
    /// Cooperative request observed by the independent background owner.
    /// Writers only publish this bit before taking commit fences; they never
    /// execute compaction inline.
    compaction_requested: Arc<AtomicBool>,
    /// Bounded evidence for recently failed exact compaction snapshots.
    compaction_retry_cooldown: Arc<CompactionRetryCooldown>,
    compaction_job_concurrency: Arc<CompactionJobConcurrencyState>,
    compaction_soft_backpressure_waits: Arc<AtomicU64>,
    compaction_soft_backpressure_wait_millis: Arc<AtomicU64>,
    compaction_hard_backpressure_rejections: Arc<AtomicU64>,
    /// Global epoch counter for volume-cache eviction. Incremented each
    /// checkpoint cycle. The epoch ranks candidates inside the byte-budget
    /// policy; it is not itself the eviction trigger.
    eviction_epoch: AtomicU64,
}

/// RAII guard that clears an AtomicBool on drop. Used to release the
/// compaction_running flag even on early returns or panics.
struct AtomicBoolGuard<'a>(&'a AtomicBool);
impl Drop for AtomicBoolGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn conventional_segment_file_path(table_name: &str, segment_id: u64) -> PathBuf {
    PathBuf::from(table_name).join(format!("segment_{:016x}.data", segment_id))
}

fn missing_partial_index_predicate_binder(
    canonical_sql: &str,
    _schema: &Schema,
) -> Result<crate::index::PartialIndexPredicate> {
    Err(Error::NotSupported(format!(
        "cannot recover partial index predicate '{}' without a configured composition binder",
        canonical_sql
    )))
}

#[cfg(test)]
pub(crate) mod tests;
