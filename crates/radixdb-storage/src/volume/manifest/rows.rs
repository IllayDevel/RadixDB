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

//! Authoritative row visibility, mappings, counters, and topology generations.

use super::*;

impl SegmentManager {
    /// Check if a row_id is tombstoned (any commit_seq).
    pub fn is_tombstoned(&self, row_id: i64) -> bool {
        self.tombstones.load().contains_key(&row_id)
    }

    /// Check if a row_id exists in any segment (not tombstoned).
    ///
    /// Used for constraint checking (PK/UNIQUE).
    pub fn row_exists(&self, row_id: i64) -> bool {
        if self.tombstones.load().contains_key(&row_id) {
            return false;
        }
        // Metadata-only check (binary search on row_ids). Does not access
        // column data, so no mark_accessed — should not pin volumes.
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<(u64, i64, i64)> = manifest
                .segments
                .iter()
                .map(|m| (m.segment_id, m.min_row_id, m.max_row_id))
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for (seg_id, min_id, max_id) in &seg_ids {
            if row_id < *min_id || row_id > *max_id {
                continue;
            }
            if let Some(cold) = segments.get(seg_id) {
                if cold.volume.meta.row_ids.binary_search(&row_id).is_ok() {
                    return true;
                }
            }
        }
        false
    }

    /// Get a cold row by row_id. Returns the Row if found and not tombstoned.
    /// Iterates newest-first so overlapping row_ids return the newest version.
    /// Uses metadata-only search; cold volumes read only target artifact-backed row groups.
    pub fn get_cold_row(&self, row_id: i64) -> Result<Option<radixdb_core::Row>> {
        if self.tombstones.load().contains_key(&row_id) {
            return Ok(None);
        }
        let topology_generation = self.topology_generation();
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<(u64, i64, i64)> = manifest
                .segments
                .iter()
                .rev()
                .map(|m| (m.segment_id, m.min_row_id, m.max_row_id))
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for (seg_id, min_id, max_id) in &seg_ids {
            if row_id < *min_id || row_id > *max_id {
                continue;
            }
            if let Some(cold) = segments.get(seg_id) {
                if let Ok(idx) = cold.volume.meta.row_ids.binary_search(&row_id) {
                    if cold.volume.is_cold() {
                        cold.volume.mark_accessed();
                        return match self.read_cold_row_at(*seg_id, &cold.volume, idx) {
                            Ok(row) => Ok(Some(row)),
                            Err(_) if self.topology_generation() != topology_generation => {
                                self.get_cold_row_retry(row_id)
                            }
                            Err(err) => Err(err),
                        };
                    }
                    cold.volume.mark_accessed();
                    return Ok(Some(cold.volume.get_row(idx)));
                }
            }
        }
        Ok(None)
    }

    /// Retry get_cold_row with a fresh consistent snapshot after compaction.
    pub(super) fn get_cold_row_retry(&self, row_id: i64) -> Result<Option<radixdb_core::Row>> {
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<u64> = manifest
                .segments
                .iter()
                .rev()
                .map(|m| m.segment_id)
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for seg_id in &seg_ids {
            if let Some(cold) = segments.get(seg_id) {
                if let Ok(idx) = cold.volume.meta.row_ids.binary_search(&row_id) {
                    if cold.volume.is_cold() {
                        cold.volume.mark_accessed();
                        let row = self.read_cold_row_at(*seg_id, &cold.volume, idx)?;
                        return Ok(Some(row));
                    }
                    cold.volume.mark_accessed();
                    return Ok(Some(cold.volume.get_row(idx)));
                }
            }
        }
        Ok(None)
    }

    /// Get a cold row by row_id, normalized to the current schema.
    /// After ALTER TABLE ADD COLUMN, cold volumes may have fewer columns.
    /// This variant fills in defaults for missing columns.
    /// Iterates newest-first so overlapping row_ids return the newest version.
    /// Uses metadata-only search; cold volumes read only target artifact-backed row groups.
    pub fn get_cold_row_normalized(
        &self,
        row_id: i64,
        schema: &radixdb_core::Schema,
    ) -> Result<Option<radixdb_core::Row>> {
        if self.tombstones.load().contains_key(&row_id) {
            return Ok(None);
        }
        let topology_generation = self.topology_generation();
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<(u64, i64, i64)> = manifest
                .segments
                .iter()
                .rev()
                .map(|m| (m.segment_id, m.min_row_id, m.max_row_id))
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for (seg_id, min_id, max_id) in &seg_ids {
            if row_id < *min_id || row_id > *max_id {
                continue;
            }
            if let Some(cold) = segments.get(seg_id) {
                if let Ok(idx) = cold.volume.meta.row_ids.binary_search(&row_id) {
                    let mapping = self.get_cold_segment_mapping(cold, schema);
                    if cold.volume.is_cold() {
                        cold.volume.mark_accessed();
                        return match self.read_cold_mapped_row_at(
                            *seg_id,
                            &cold.volume,
                            idx,
                            &mapping,
                        ) {
                            Ok(row) => Ok(Some(row)),
                            Err(_) if self.topology_generation() != topology_generation => {
                                self.get_cold_row_normalized_retry(row_id, schema)
                            }
                            Err(err) => Err(err),
                        };
                    }
                    let vol = if cold.volume.is_cold() {
                        return self.get_cold_row_normalized_retry(row_id, schema);
                    } else {
                        cold.volume.mark_accessed();
                        Arc::clone(&cold.volume)
                    };
                    if mapping.is_identity {
                        return Ok(Some(vol.get_row(idx)));
                    }
                    return Ok(Some(vol.get_row_mapped(idx, &mapping)));
                }
            }
        }
        Ok(None)
    }

    /// Retry get_cold_row_normalized with a fresh consistent snapshot after compaction.
    pub(super) fn get_cold_row_normalized_retry(
        &self,
        row_id: i64,
        schema: &radixdb_core::Schema,
    ) -> Result<Option<radixdb_core::Row>> {
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<u64> = manifest
                .segments
                .iter()
                .rev()
                .map(|m| m.segment_id)
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for seg_id in &seg_ids {
            if let Some(cold) = segments.get(seg_id) {
                if let Ok(idx) = cold.volume.meta.row_ids.binary_search(&row_id) {
                    if cold.volume.is_cold() {
                        let mapping = self.get_cold_segment_mapping(cold, schema);
                        cold.volume.mark_accessed();
                        let row =
                            self.read_cold_mapped_row_at(*seg_id, &cold.volume, idx, &mapping)?;
                        return Ok(Some(row));
                    }
                    cold.volume.mark_accessed();
                    let mapping = self.get_cold_segment_mapping(cold, schema);
                    if mapping.is_identity {
                        return Ok(Some(cold.volume.get_row(idx)));
                    }
                    return Ok(Some(cold.volume.get_row_mapped(idx, &mapping)));
                }
            }
        }
        Ok(None)
    }

    /// Check if a row_id actually exists in any loaded volume.
    /// Does NOT check tombstones. Used for idempotent WAL replay.
    /// Uses binary search on the volume's row_ids for O(log n) per segment.
    pub fn is_row_id_in_volume(&self, row_id: i64) -> bool {
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<(u64, i64, i64)> = manifest
                .segments
                .iter()
                .map(|m| (m.segment_id, m.min_row_id, m.max_row_id))
                .collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        for (seg_id, min_id, max_id) in &seg_ids {
            if row_id < *min_id || row_id > *max_id {
                continue;
            }
            if let Some(cold) = segments.get(seg_id) {
                if cold.volume.meta.row_ids.binary_search(&row_id).is_ok() {
                    return true;
                }
            }
        }
        false
    }

    /// Get the number of physical row ordinals across all published segments.
    ///
    /// This deliberately includes tombstoned and superseded ordinals.  Owners
    /// that place a pre-visibility limit on immutable index scans must use the
    /// physical cardinality; a live-row estimate can stop before later valid
    /// postings after earlier candidates are removed by tombstone/shadow
    /// filtering.
    pub fn total_physical_row_count(&self) -> usize {
        self.manifest
            .read()
            .segments
            .iter()
            .fold(0usize, |total, segment| {
                total.saturating_add(segment.row_count)
            })
    }

    /// Get the total live row count across all segments (minus tombstones).
    /// NOTE: This is a fast estimate that does not deduplicate overlapping row_ids.
    /// Use `deduped_row_count()` for an exact count.
    pub fn total_row_count(&self) -> usize {
        let ts_count = self.tombstones.load().len();
        self.total_physical_row_count().saturating_sub(ts_count)
    }

    /// Get the exact deduplicated row count across all segments.
    /// A generation-tagged cache prevents an older concurrent recomputation
    /// from overwriting a newer segment/tombstone publication.
    pub fn deduped_row_count(&self) -> usize {
        loop {
            let generation = self
                .topology_generation
                .load(std::sync::atomic::Ordering::Acquire);
            if self
                .cached_deduped_generation
                .load(std::sync::atomic::Ordering::Acquire)
                == generation
            {
                return self
                    .cached_deduped_count
                    .load(std::sync::atomic::Ordering::Relaxed) as usize;
            }

            let count = self.compute_deduped_row_count();
            if self
                .topology_generation
                .load(std::sync::atomic::Ordering::Acquire)
                != generation
            {
                continue;
            }
            self.cached_deduped_count
                .store(count as u64, std::sync::atomic::Ordering::Relaxed);
            self.cached_deduped_generation
                .store(generation, std::sync::atomic::Ordering::Release);
            if self
                .topology_generation
                .load(std::sync::atomic::Ordering::Acquire)
                == generation
            {
                return count;
            }
        }
    }

    #[inline]
    pub(super) fn mark_topology_changed(&self) {
        self.topology_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    #[inline]
    pub(super) fn mark_segment_topology_changed(&self) {
        self.segment_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.mark_topology_changed();
    }

    #[inline]
    pub(super) fn mark_tombstones_changed(&self) {
        self.tombstone_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.mark_topology_changed();
    }

    #[inline]
    pub fn topology_generation(&self) -> u64 {
        self.topology_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn segment_generation(&self) -> u64 {
        self.segment_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Acquire a shared guard while performing a cold check + hot insert.
    /// Seal takes the exclusive guard so it cannot move rows between the
    /// cold visibility check and hot publication.
    #[inline]
    pub fn acquire_seal_read(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.seal_fence.read()
    }

    /// Acquire the exclusive guard for the seal critical section.
    #[inline]
    pub fn acquire_seal_write(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.seal_fence.write()
    }

    #[cfg(test)]
    pub(crate) fn try_acquire_seal_write_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<parking_lot::RwLockWriteGuard<'_, ()>> {
        self.seal_fence.try_write_for(timeout)
    }

    /// Current seal generation. Incremented on every register_segment.
    #[inline]
    pub fn seal_generation(&self) -> u64 {
        self.seal_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Record the current seal generation for a transaction. Called under
    /// the seal read fence during INSERT so the value is consistent.
    /// Stores the minimum (earliest) generation seen by this txn, so that
    /// a later INSERT within the same txn cannot hide an earlier seal.
    #[inline]
    pub fn record_txn_seal_generation(&self, txn_id: i64) {
        let gen = self.seal_generation();
        let mut map = self.txn_seal_gens.lock();
        map.entry(txn_id)
            .and_modify(|existing| {
                if gen < *existing {
                    *existing = gen;
                }
            })
            .or_insert(gen);
    }

    /// Replace a transaction's recorded generation after a complete cold
    /// revalidation performed under the seal-read fence.  Unlike INSERT-time
    /// recording, this deliberately advances the baseline: all earlier cold
    /// generations have just been certified, while any later seal still
    /// changes the generation and forces the publication-time recheck.
    #[inline]
    pub fn refresh_txn_seal_generation_after_validation(&self, txn_id: i64) {
        let generation = self.seal_generation();
        self.txn_seal_gens.lock().insert(txn_id, generation);
    }

    /// Get the seal generation recorded for a transaction.
    #[inline]
    pub fn get_txn_seal_generation(&self, txn_id: i64) -> Option<u64> {
        self.txn_seal_gens.lock().get(&txn_id).copied()
    }

    /// Remove the seal generation record for a transaction (on commit/rollback).
    #[inline]
    pub fn clear_txn_seal_generation(&self, txn_id: i64) {
        self.txn_seal_gens.lock().remove(&txn_id);
    }

    /// Count visible rows using pre-computed visibility bitmaps.
    /// Falls back to hash-based dedup only when bitmaps are not available.
    pub(super) fn compute_deduped_row_count(&self) -> usize {
        let segments = self.segments.load_full();
        if segments.is_empty() {
            return 0;
        }
        let tombstones = self.tombstones.load_full();
        if segments.len() == 1 {
            let total: usize = segments.values().map(|cs| cs.volume.meta.row_count).sum();
            return total.saturating_sub(tombstones.len());
        }

        // Fast path: use visibility bitmaps (O(1) per row, zero allocation).
        // visible=None means all rows visible (no overlap), is_visible() handles both.
        {
            let mut count = 0usize;
            for cs in segments.values() {
                let vol = &cs.volume;
                for i in 0..vol.meta.row_count {
                    if !cs.is_visible(i) {
                        continue;
                    }
                    if !tombstones.is_empty() && tombstones.contains_key(&vol.meta.row_ids.at(i)) {
                        continue;
                    }
                    count += 1;
                }
            }
            count
        }
    }

    /// Get the cached column mapping for a volume. Computes on first call,
    /// returns cached on subsequent calls. Handles dropped columns + renames
    /// automatically. Call invalidate_mappings() on ALTER TABLE.
    pub fn get_volume_mapping(
        &self,
        seg_id: u64,
        schema: &radixdb_core::Schema,
    ) -> super::super::writer::ColumnMapping {
        let segs = self.segments.load_full();
        if let Some(cold) = segs.get(&seg_id) {
            self.get_cold_segment_mapping(cold, schema)
        } else {
            super::super::writer::ColumnMapping::empty()
        }
    }

    /// Resolve a mapping from the same immutable segment snapshot that owns
    /// the volume being read.
    ///
    /// A compaction publication replaces the segment map atomically. Readers
    /// may legitimately retain the previous `ColdSegment` after that swap; a
    /// second lookup by segment id would then return an empty mapping and make
    /// the retained volume appear to contain NULLs. Keeping volume and mapping
    /// in one snapshot prevents that mixed-topology read.
    pub fn get_cold_segment_mapping(
        &self,
        cold: &ColdSegment,
        schema: &radixdb_core::Schema,
    ) -> super::super::writer::ColumnMapping {
        // The cached mapping follows the published catalog. An explicit
        // transaction may own a wider private ALTER overlay; compute that
        // mapping locally without exposing or caching it for observers.
        if cold.mapping.sources.len() == schema.columns.len() {
            cold.mapping.clone()
        } else {
            let manifest = self.manifest.read();
            super::super::writer::compute_column_mapping_with_drops(
                schema,
                &cold.volume,
                &manifest.dropped_columns,
                cold.schema_version,
                &manifest.column_renames,
            )
        }
    }

    /// Recompute all column mappings for loaded volumes.
    /// Called on ALTER TABLE (rename/drop/add column).
    pub fn invalidate_mappings(&self, schema: &radixdb_core::Schema) {
        let manifest = self.manifest.read();
        let drops = manifest.dropped_columns.clone();
        let renames = manifest.column_renames.clone();
        drop(manifest);
        let _segments_guard = self.segments_update.lock();
        let mut new_map = (*self.segments.load_full()).clone();
        for cold in new_map.values_mut() {
            cold.mapping = super::super::writer::compute_column_mapping_with_drops(
                schema,
                &cold.volume,
                &drops,
                cold.schema_version,
                &renames,
            );
        }
        self.segments.store(Arc::new(new_map));
    }

    /// Record a column drop so old volumes don't leak stale data.
    /// `schema_version` is the current schema epoch at drop time. Only volumes
    /// with schema_version <= this value will have the column masked.
    pub fn record_column_drop(&self, col_name: &str, schema_version: u64) {
        let lower = SmartString::from(col_name.to_lowercase());
        let mut manifest = self.manifest.write();
        // Remove any existing entry for this column name before adding the new one.
        // This handles DROP + ADD + DROP sequences correctly.
        manifest
            .dropped_columns
            .retain(|(name, _)| name.as_str() != lower.as_str());
        manifest.dropped_columns.push((lower, schema_version));
    }

    // Note: record_column_readd was removed. dropped_columns is permanent
    // until compaction rewrites all old volumes. After ADD COLUMN re-adds a
    // dropped name, compute_column_mapping_with_drops handles it correctly:
    // old volumes have the column at an old position (blocked by drop mask),
    // new volumes don't have it at all (mapped to Default).

    pub fn set_seal_overlap(&self, count: usize) {
        self.seal_overlap_count
            .store(count, std::sync::atomic::Ordering::Release);
    }

    /// Clear the seal overlap count. Called AFTER remove_sealed_rows completes.
    pub fn clear_seal_overlap(&self) {
        self.seal_overlap_count
            .store(0, std::sync::atomic::Ordering::Release);
    }

    /// Get the current seal overlap count (for row_count correction).
    pub fn seal_overlap(&self) -> usize {
        self.seal_overlap_count
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
