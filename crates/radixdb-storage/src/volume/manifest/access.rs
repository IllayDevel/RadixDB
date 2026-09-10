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

//! Segment-manager construction and bounded physical block access.

use super::*;

impl SegmentManager {
    /// Create a new segment manager for a table.
    pub fn new(table_name: &str, volume_dir: Option<PathBuf>) -> Self {
        Self {
            table_name: RwLock::new(SmartString::from(table_name)),
            manifest: RwLock::new(TableManifest::new(table_name)),
            segments: ArcSwap::new(Arc::new(FxHashMap::default())),
            segments_update: parking_lot::Mutex::new(()),
            volume_dir,
            has_segments_flag: std::sync::atomic::AtomicBool::new(false),
            current_eviction_epoch: std::sync::atomic::AtomicU64::new(0),
            tombstones: ArcSwap::new(Arc::new(FxHashMap::default())),
            tombstones_update: parking_lot::Mutex::new(()),
            tombstone_generation: std::sync::atomic::AtomicU64::new(0),
            durable_tombstone_generation: std::sync::atomic::AtomicU64::new(0),
            pending_txn_tombstones: RwLock::new(FxHashMap::default()),
            pending_txn_index_removals: parking_lot::Mutex::new(FxHashMap::default()),
            cold_populated_indexes: RwLock::new(FxHashSet::default()),
            cached_deduped_count: std::sync::atomic::AtomicU64::new(0),
            cached_deduped_generation: std::sync::atomic::AtomicU64::new(u64::MAX),
            topology_generation: std::sync::atomic::AtomicU64::new(0),
            segment_generation: std::sync::atomic::AtomicU64::new(0),
            seal_fence: RwLock::new(()),
            visibility_seen: parking_lot::Mutex::new(rustc_hash::FxHashSet::default()),
            seal_generation: std::sync::atomic::AtomicU64::new(0),
            txn_seal_gens: parking_lot::Mutex::new(rustc_hash::FxHashMap::default()),
            seal_overlap_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get the table name.
    pub fn table_name(&self) -> SmartString {
        self.table_name.read().clone()
    }

    /// Read selected physical columns from one bounded cold row group.
    /// Missing canonical storage is never interpreted as absence.
    pub(super) fn read_cold_blocks_from_volume(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        col_indices: &[usize],
        row_group_idx: usize,
    ) -> Result<Vec<(usize, super::super::column::ColumnData)>> {
        let source = volume
            .artifact_source()
            .ok_or_else(|| Error::internal(format!("cold segment {seg_id} has no DATA source")))?;
        volume.mark_accessed();
        source
            .read_columns(row_group_idx, col_indices)
            .map(|batch| batch.into_columns())
    }

    pub(super) fn read_cold_value_at(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        col_idx: usize,
        row_idx: usize,
    ) -> Result<Value> {
        if !volume.is_cold() {
            return Err(Error::internal("cold value read called for eager volume"));
        }
        let (group_idx, range) = self.cold_group_for_row(seg_id, volume, row_idx)?;
        let local_idx = row_idx - range.start;
        let mut blocks =
            self.read_cold_blocks_from_volume(seg_id, volume, &[col_idx], group_idx)?;
        let (_, block) = blocks.pop().ok_or_else(|| {
            Error::internal(format!(
                "cold segment {seg_id} returned no block for column {col_idx}"
            ))
        })?;
        if local_idx >= block.len() {
            return Err(Error::internal(format!(
                "cold segment {seg_id} block row {local_idx} exceeds decoded length {}",
                block.len()
            )));
        }
        Ok(block.get_value(local_idx))
    }

    pub(super) fn cold_group_count(&self, seg_id: u64, volume: &FrozenVolume) -> Result<usize> {
        if !volume.is_cold() {
            return Err(Error::internal(
                "cold group count requested for eager volume",
            ));
        }
        let source = volume
            .artifact_source()
            .ok_or_else(|| Error::internal(format!("cold segment {seg_id} has no DATA source")))?;
        Ok(source.row_group_count())
    }

    pub(super) fn cold_group_range(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        row_group_idx: usize,
    ) -> Result<std::ops::Range<usize>> {
        if !volume.is_cold() {
            return Err(Error::internal(
                "cold group range requested for eager volume",
            ));
        }
        let source = volume
            .artifact_source()
            .ok_or_else(|| Error::internal(format!("cold segment {seg_id} has no DATA source")))?;
        source.row_group_range(row_group_idx)
    }

    fn cold_group_for_row(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        row_idx: usize,
    ) -> Result<(usize, std::ops::Range<usize>)> {
        let source = volume
            .artifact_source()
            .ok_or_else(|| Error::internal(format!("cold segment {seg_id} has no DATA source")))?;
        let group_idx = source.row_group_for_row(row_idx)?;
        Ok((group_idx, source.row_group_range(group_idx)?))
    }

    /// Materialize one physical row from a metadata-only volume without
    /// reloading the whole segment. Used by direct cold row fetch fallbacks
    /// from the MVCC engine.
    pub(super) fn read_cold_row_at(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        row_idx: usize,
    ) -> Result<Row> {
        if !volume.is_cold() {
            return Err(Error::internal("cold row read called for eager volume"));
        }
        let (group_idx, range) = self.cold_group_for_row(seg_id, volume, row_idx)?;
        let local_idx = row_idx - range.start;
        let col_indices: Vec<usize> = (0..volume.columns.len()).collect();
        let blocks = self.read_cold_blocks_from_volume(seg_id, volume, &col_indices, group_idx)?;
        let mut values = Vec::with_capacity(volume.columns.len());
        for (_, block) in blocks {
            values.push(block.get_value(local_idx));
        }
        let value_count = values.len();
        let row = Row::from_values(values);
        crate::instrumentation::record_row_materialization_count(1, value_count as u64);
        Ok(row)
    }

    /// Materialize one row from a metadata-only volume through the current
    /// schema mapping. This preserves ALTER TABLE defaults/renames while still
    /// reading only the row-group blocks needed for this row.
    pub(super) fn read_cold_mapped_row_at(
        &self,
        seg_id: u64,
        volume: &FrozenVolume,
        row_idx: usize,
        mapping: &super::super::writer::ColumnMapping,
    ) -> Result<Row> {
        if !volume.is_cold() {
            return Err(Error::internal(
                "cold mapped row read called for eager volume",
            ));
        }
        let (group_idx, range) = self.cold_group_for_row(seg_id, volume, row_idx)?;
        let local_idx = row_idx - range.start;
        let mut phys_indices = Vec::new();
        for src in &mapping.sources {
            if let super::super::writer::ColSource::Volume(phys_idx) = src {
                if !phys_indices.contains(phys_idx) {
                    phys_indices.push(*phys_idx);
                }
            }
        }
        let blocks = self.read_cold_blocks_from_volume(seg_id, volume, &phys_indices, group_idx)?;
        let block_cache: FxHashMap<usize, super::super::column::ColumnData> =
            blocks.into_iter().collect();
        let mut values = Vec::with_capacity(mapping.sources.len());
        for src in &mapping.sources {
            match src {
                super::super::writer::ColSource::Volume(phys_idx) => {
                    let block = block_cache.get(phys_idx).ok_or_else(|| {
                        Error::internal("cold mapped row block missing after cache insert")
                    })?;
                    values.push(block.get_value(local_idx));
                }
                super::super::writer::ColSource::Default(value) => values.push(value.clone()),
            }
        }
        let value_count = values.len();
        let row = Row::from_values(values);
        crate::instrumentation::record_row_materialization_count(1, value_count as u64);
        Ok(row)
    }
}
