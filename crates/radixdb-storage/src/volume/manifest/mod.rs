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

//! Process-local immutable-segment topology and read-path state.
//!
//! Durable membership is owned by the CONTROL-selected artifact generation.
//! Nothing in this module has a filesystem codec or can publish a second
//! manifest authority.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::instrumentation;
use crate::timestamp::get_fast_timestamp;
use crate::traits::Index;
use radixdb_core::SmartString;
use radixdb_core::{Error, Result, Row, Value};

use super::writer::{ColumnMapping, FrozenVolume};

mod access;
mod lookup;
mod publication;
mod rows;
mod table;
mod topology;
mod transactions;
mod visibility;

#[cfg(test)]
mod tests;

pub use table::{CompactionToken, SegmentLevel, SegmentMeta, SegmentRegistration, TableManifest};

use table::ReplacementExpectation;
use visibility::{compute_visibility_bitmaps, selected_ranges_are_isolated};

/// A cold segment: immutable volume + pre-computed column mapping.
/// The mapping is computed once at registration (seal/compaction/load) and
/// recomputed on ALTER TABLE. No per-scan computation, no lock contention.
#[derive(Clone)]
pub struct ColdSegment {
    pub volume: Arc<FrozenVolume>,
    pub mapping: super::writer::ColumnMapping,
    /// Schema version when this volume was created. Used with dropped_columns
    /// to correctly mask stale data only from volumes older than a column drop.
    pub schema_version: u64,
    /// Per-row visibility bitmap: bit i is set when row i is the authoritative
    /// (newest) version across all overlapping volumes. None when this is the
    /// only segment (all rows visible) or when there is no overlap.
    /// Arc so ColdSegment::clone() is O(1) — scanners share the same bitmap.
    pub visible: Option<Arc<Vec<u64>>>,
}

impl ColdSegment {
    /// Build a cold segment through the only normal constructor.
    ///
    /// A registered durable segment must carry its canonical DATA source.
    pub fn new(
        volume: Arc<FrozenVolume>,
        mapping: ColumnMapping,
        schema_version: u64,
        visible: Option<Arc<Vec<u64>>>,
    ) -> Result<Self> {
        if let Some(bits) = visible.as_ref() {
            let expected_words = volume.meta.row_count.div_ceil(64);
            if bits.len() != expected_words {
                return Err(Error::internal(format!(
                    "visibility bitmap has {} words for {} rows; expected {}",
                    bits.len(),
                    volume.meta.row_count,
                    expected_words
                )));
            }
            let trailing = volume.meta.row_count % 64;
            if trailing != 0
                && bits
                    .last()
                    .is_some_and(|word| word & (!0u64 << trailing) != 0)
            {
                return Err(Error::internal(
                    "visibility bitmap has nonzero bits beyond row count",
                ));
            }
        }
        if volume.artifact_source().is_none() {
            #[cfg(any(test, feature = "test-hooks"))]
            if volume.columns.is_eager() {
                return Ok(Self {
                    volume,
                    mapping,
                    schema_version,
                    visible,
                });
            }
            return Err(Error::internal(
                "cannot register cold segment without an artifact source",
            ));
        }
        Ok(Self {
            volume,
            mapping,
            schema_version,
            visible,
        })
    }

    /// Build a segment whose physical columns match the current logical schema.
    pub fn new_identity_mapping(
        volume: Arc<FrozenVolume>,
        schema_version: u64,
        visible: Option<Arc<Vec<u64>>>,
    ) -> Result<Self> {
        let mapping = ColumnMapping::identity(&volume);
        Self::new(volume, mapping, schema_version, visible)
    }

    /// Check whether row at position `idx` in this volume is the authoritative
    /// (newest) copy across all overlapping volumes.
    #[inline]
    pub fn is_visible(&self, idx: usize) -> bool {
        match &self.visible {
            None => true,
            Some(bits) => bits
                .get(idx >> 6)
                .is_some_and(|word| (word >> (idx & 63)) & 1 == 1),
        }
    }
}

/// Atomic snapshot of cold segment state for batch constraint checking.
/// Captures manifest seg_ids + segments Arc + tombstones Arc once,
/// eliminating 3 lock reads per row in batch INSERT/upsert.
pub struct ColdSnapshot {
    pub seg_ids: smallvec::SmallVec<[u64; 4]>,
    pub segs: Arc<FxHashMap<u64, ColdSegment>>,
    pub ts: Arc<FxHashMap<i64, u64>>,
}

/// Immutable view of one table's committed tombstones at a publication
/// generation.  The generation is independent from row-segment topology so a
/// checkpoint can reuse an unchanged durable tombstone artifact instead of
/// rewriting a table-sized delete set on every WAL advance.
#[derive(Clone)]
pub(crate) struct TombstonePublicationSnapshot {
    generation: u64,
    tombstones: Arc<FxHashMap<i64, u64>>,
}

impl TombstonePublicationSnapshot {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn tombstones(&self) -> &FxHashMap<i64, u64> {
        self.tombstones.as_ref()
    }
}

/// Per-table segment manager.
///
/// Owns process-local topology, loaded segments, and tombstones for one table.
/// Tombstones track cold row_ids that have been deleted or superseded
/// by hot buffer versions and are used during scans to skip stale cold rows.
///
/// Thread safety: the manager uses interior mutability via RwLock for
/// concurrent read access (queries) and exclusive write access (seal, compaction).
pub struct SegmentManager {
    /// Table name.
    table_name: RwLock<SmartString>,
    /// Process-local segment topology reconstructed from the selected
    /// artifact generation at startup.
    manifest: RwLock<TableManifest>,
    /// Loaded segments with pre-computed column mappings, keyed by segment_id.
    /// CoW via ArcSwap: readers load the Arc without taking a lock,
    /// writers clone the inner map under `segments_update`, modify, and
    /// atomically publish the new Arc.
    /// The ColumnMapping is computed once at registration and recomputed on ALTER TABLE.
    /// This eliminates per-scan compute_column_mapping overhead and lock contention.
    segments: ArcSwap<FxHashMap<u64, ColdSegment>>,
    /// Serializes copy-on-write updates to `segments` so independent writers
    /// cannot publish stale clones over each other. Readers never take it.
    segments_update: parking_lot::Mutex<()>,
    /// Base directory for volume files (None for memory-only databases).
    volume_dir: Option<PathBuf>,
    /// Fast atomic flag: true if any segments are loaded.
    has_segments_flag: std::sync::atomic::AtomicBool,
    /// Current eviction epoch. Updated by evict_idle_volumes.
    pub current_eviction_epoch: std::sync::atomic::AtomicU64,
    /// Committed tombstone map: cold row_id → commit_seq (when the tombstone was created).
    /// Built from manifest tombstones on startup, updated at commit time.
    /// Wrapped in ArcSwap for cheap O(1) reads — most callers only need to
    /// check membership, not mutate. Writers clone and swap the Arc on mutation.
    /// The commit_seq enables snapshot isolation: a snapshot at begin_seq=N
    /// only sees tombstones with commit_seq <= N.
    tombstones: ArcSwap<FxHashMap<i64, u64>>,
    /// Serializes copy-on-write updates to `tombstones`. Readers never take it.
    tombstones_update: parking_lot::Mutex<()>,
    /// Monotonic generation of the exact committed tombstone set.  It changes
    /// only when membership or a tombstone visibility sequence changes.
    tombstone_generation: std::sync::atomic::AtomicU64,
    /// Last tombstone generation included in a CONTROL-selected table
    /// manifest.  A mismatch makes checkpoint publication replace the complete
    /// tombstone descriptor set before it is allowed to advance the WAL floor.
    durable_tombstone_generation: std::sync::atomic::AtomicU64,
    /// Per-transaction pending tombstones with an append-only savepoint
    /// journal. Membership stays a compact hash set on the read hot path;
    /// mutation timestamps live only in the rollback journal.
    ///
    /// The timestamp makes `ROLLBACK TO SAVEPOINT` able to discard only cold
    /// tombstones created after the savepoint. Repeated mutation of the same
    /// row keeps its earliest timestamp so a pre-savepoint tombstone survives.
    /// Applied to the shared tombstone set on commit, discarded on rollback.
    /// This lives on the SegmentManager (not SegmentedTable) because the commit
    /// path in engine.rs creates fresh MVCCTable instances that don't have
    /// access to SegmentedTable state.
    pending_txn_tombstones: RwLock<FxHashMap<i64, PendingTxnTombstones>>,
    /// Eager removals from cold-populated partial/HNSW indexes, grouped by
    /// transaction. This state must live beside the shared cold tombstones:
    /// executor savepoint rollback can recreate `SegmentedTable` wrappers,
    /// while the underlying indexes and segment manager remain shared.
    pending_txn_index_removals: parking_lot::Mutex<FxHashMap<i64, Vec<PendingColdIndexRemoval>>>,
    /// Ordinary/partial indexes whose in-memory object was explicitly
    /// backfilled from every currently published cold segment in this engine
    /// lifetime. This is deliberately runtime-only: after restart the catalog
    /// definition remains, but optimizers must use persisted postings or scan
    /// fallback instead of treating a hot-only object as complete.
    cold_populated_indexes: RwLock<FxHashSet<SmartString>>,
    // Unique constraint checks use per-volume hash indices (on FrozenVolume).
    // No global cache needed. Each volume builds its index lazily on first
    // unique check and never invalidates (volumes are immutable).
    // Zone maps + bloom filters prune volumes before hash lookup.
    /// Cached deduplicated row count. It is valid only when
    /// `cached_deduped_generation == topology_generation`.
    cached_deduped_count: std::sync::atomic::AtomicU64,
    cached_deduped_generation: std::sync::atomic::AtomicU64,
    /// Monotonic generation of the published segment/tombstone topology.
    /// Exact-count readers use it to prevent an older recomputation from
    /// overwriting a concurrent mutation's invalidation.
    topology_generation: std::sync::atomic::AtomicU64,
    /// Monotonic generation of the published segment set only. Compaction
    /// snapshots use this narrower generation: concurrent UPDATE/DELETE may
    /// publish tombstones while a compaction is being built, and those newer
    /// tombstones remain valid across an otherwise unchanged segment rewrite.
    segment_generation: std::sync::atomic::AtomicU64,
    /// Per-table fence that serializes seal with cold-check + hot insert.
    /// INSERTs take a shared guard while checking cold constraints and
    /// publishing into hot; seal takes the exclusive guard while moving rows.
    seal_fence: RwLock<()>,
    /// Reusable scratch set for overlapping topology publication. Disjoint
    /// append batches bypass it entirely; overlap/compaction keeps it alive
    /// across calls to avoid repeated allocation. Protected by Mutex since
    /// visibility computation is serialized under the segment writer lock.
    visibility_seen: parking_lot::Mutex<rustc_hash::FxHashSet<i64>>,
    /// Monotonic counter incremented on every register_segment. Used at
    /// commit time to detect whether a seal happened since statement time.
    /// If unchanged, the commit-time cold recheck is skipped (fast path).
    seal_generation: std::sync::atomic::AtomicU64,
    /// Per-txn seal generation at INSERT time. Small map — only active
    /// transactions with pending inserts on this table.
    txn_seal_gens: parking_lot::Mutex<rustc_hash::FxHashMap<i64, u64>>,
    /// Number of rows currently being sealed (exist in both hot and cold).
    /// Set to N before register_segment, cleared after remove_sealed_rows.
    /// Subtracted from row_count() to prevent double-counting during the seal window.
    seal_overlap_count: std::sync::atomic::AtomicUsize,
}

/// Bounded, allocation-free ownership summary used by read-only runtime
/// diagnostics. It deliberately observes only already-published segment
/// metadata and resident owners; it never opens payload files or builds an
/// index. `truncated` means that the caller-provided segment budget was
/// exhausted and the numeric totals are a lower bound.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct SegmentRuntimeOwnerSnapshot {
    pub segments: u64,
    pub unleveled_segments: u64,
    pub l0_segments: u64,
    pub l1_segments: u64,
    pub l0_debt_physical_bytes: u64,
    pub rows: u64,
    pub resident_bytes: u64,
    pub metadata_bytes: u64,
    pub row_id_bytes: u64,
    pub exact_index_bytes: u64,
    pub ordered_index_bytes: u64,
    pub descriptor_bytes: u64,
    pub column_payload_bytes: u64,
    pub tombstones: u64,
    pub level_metadata_busy: bool,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L0DebtSnapshot {
    pub segments: u64,
    pub physical_bytes: u64,
}

#[derive(Default)]
struct PendingTxnTombstones {
    ids: FxHashSet<i64>,
    /// First mutation of each ID, in monotonically increasing timestamp order.
    journal: Vec<(i64, i64)>,
}

struct PendingColdIndexRemoval {
    index: Arc<dyn Index>,
    values: Vec<Value>,
    row_id: i64,
    removed_at: i64,
}
