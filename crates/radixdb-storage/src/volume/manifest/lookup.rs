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

//! Exact, ordered, and fallback lookup paths over published cold segments.

use super::*;

impl SegmentManager {
    /// Get segments in order, metadata only (no cold volume reload).
    /// Use when callers only need vol.meta (stats, zone maps, row_ids).
    /// Does NOT mark volumes as accessed — metadata reads should not
    /// prevent eviction of column data.
    pub fn get_segments_ordered_meta(&self) -> Vec<Arc<FrozenVolume>> {
        let (seg_ids, segments) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
            let segments = self.segments.load_full();
            (seg_ids, segments)
        };
        seg_ids
            .iter()
            .filter_map(|id| segments.get(id).map(|cs| Arc::clone(&cs.volume)))
            .collect()
    }

    /// Return newest-first artifact-backed segments without materializing column bodies.
    /// Scanner paths prune by metadata and read surviving row-group blocks.
    /// Does NOT mark volumes — only volumes that survive pruning get marked
    /// by the scanner constructor or explicit per-volume mark_accessed.
    pub fn get_volumes_newest_first_lazy(&self) -> Arc<Vec<(u64, ColdSegment)>> {
        let (seg_ids, segs) = {
            let manifest = self.manifest.read();
            let seg_ids: Vec<u64> = manifest.segments.iter().map(|m| m.segment_id).collect();
            let segs = self.segments.load_full();
            (seg_ids, segs)
        };
        let mut result: Vec<(u64, ColdSegment)> = seg_ids
            .iter()
            .filter_map(|&id| segs.get(&id).map(|cs| (id, cs.clone())))
            .collect();
        result.reverse();
        Arc::new(result)
    }

    /// Check if there are any segments. O(1) atomic read, no lock.
    pub fn has_segments(&self) -> bool {
        self.has_segments_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Capture an atomic snapshot of segment state for batch constraint checking.
    /// Acquires manifest + segments + tombstones locks once. All per-row checks
    /// within the batch reuse this snapshot with zero lock overhead.
    pub fn cold_snapshot(&self) -> ColdSnapshot {
        // Zone-map/bloom pruning uses resident metadata; matching values are
        // read from addressable artifact-backed row-group blocks.
        let manifest = self.manifest.read();
        let seg_ids: smallvec::SmallVec<[u64; 4]> = manifest
            .segments
            .iter()
            .rev()
            .map(|m| m.segment_id)
            .collect();
        let segs = self.segments.load_full();
        let ts = self.tombstones.load_full();
        drop(manifest);
        ColdSnapshot { seg_ids, segs, ts }
    }

    /// Merge metadata-only cold row visibility into an aligned membership
    /// bitmap that was already populated by the hot table.
    ///
    /// The method acquires the pending-tombstone lock once for the whole
    /// bounded batch. It never loads column blocks, marks a volume as accessed,
    /// or materializes a row.
    pub fn merge_visible_row_ids_from_cold_snapshot(
        &self,
        snapshot: &ColdSnapshot,
        txn_id: i64,
        snapshot_seq: Option<u64>,
        row_ids: &[i64],
        matches: &mut [bool],
    ) -> usize {
        debug_assert_eq!(row_ids.len(), matches.len());

        let pending_guard = self.pending_txn_tombstones.read();
        let pending = pending_guard.get(&txn_id);
        let mut hit_count = matches.iter().filter(|&&matched| matched).count();

        for (position, &row_id) in row_ids.iter().enumerate() {
            if matches[position] {
                continue;
            }
            if pending.is_some_and(|pending| pending.ids.contains(&row_id)) {
                continue;
            }
            if snapshot
                .ts
                .get(&row_id)
                .is_some_and(|&commit_seq| snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff))
            {
                continue;
            }

            for segment_id in &snapshot.seg_ids {
                let Some(cold) = snapshot.segs.get(segment_id) else {
                    continue;
                };
                let row_ids = &cold.volume.meta.row_ids;
                let Some((min_id, max_id)) = row_ids.first().zip(row_ids.last()) else {
                    continue;
                };
                if row_id < min_id || row_id > max_id {
                    continue;
                }
                if let Ok(index) = row_ids.binary_search(&row_id) {
                    if cold.is_visible(index) {
                        matches[position] = true;
                        hit_count += 1;
                        break;
                    }
                }
            }
        }

        hit_count
    }

    /// Check if a value exists using a pre-captured snapshot (no lock acquisition).
    pub fn check_value_exists_with_snapshot(
        &self,
        snapshot: &ColdSnapshot,
        col_idx: usize,
        value: &radixdb_core::Value,
    ) -> Result<Option<i64>> {
        self.check_value_exists_impl(
            &snapshot.seg_ids,
            &snapshot.segs,
            &snapshot.ts,
            col_idx,
            value,
        )
    }

    /// Return the first visible cold row whose physical row id is present in
    /// the sorted candidate set.
    ///
    /// `INTEGER PRIMARY KEY` is the physical row identity in RadixDB.  COPY
    /// therefore does not need to probe every immutable segment once per
    /// incoming row: intersect the complete candidate batch with each
    /// segment's resident row-id metadata instead.  The operation performs no
    /// payload reads and costs O(segments * log(candidates) + actual overlaps).
    pub fn first_visible_row_id_in_sorted_batch(
        &self,
        snapshot: &ColdSnapshot,
        sorted_row_ids: &[i64],
        skip_row_ids: &FxHashSet<i64>,
    ) -> Option<i64> {
        if sorted_row_ids.is_empty() || snapshot.seg_ids.is_empty() {
            return None;
        }

        for segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(segment_id) else {
                continue;
            };
            let segment_row_ids = &cold.volume.meta.row_ids;
            let Some((minimum, maximum)) = segment_row_ids.first().zip(segment_row_ids.last())
            else {
                continue;
            };
            let start = sorted_row_ids.partition_point(|row_id| *row_id < minimum);
            let end = sorted_row_ids.partition_point(|row_id| *row_id <= maximum);
            for row_id in &sorted_row_ids[start..end] {
                if skip_row_ids.contains(row_id) || snapshot.ts.contains_key(row_id) {
                    continue;
                }
                let Ok(index) = segment_row_ids.binary_search(row_id) else {
                    continue;
                };
                if cold.is_visible(index) {
                    return Some(*row_id);
                }
            }
        }
        None
    }

    /// Check if a value exists in cold segments (acquires locks per call).
    /// For batch operations, prefer cold_snapshot() + check_value_exists_with_snapshot().
    pub fn check_value_exists_in_segments(
        &self,
        col_idx: usize,
        value: &radixdb_core::Value,
    ) -> Result<Option<i64>> {
        let snapshot = self.cold_snapshot();
        self.check_value_exists_impl(
            &snapshot.seg_ids,
            &snapshot.segs,
            &snapshot.ts,
            col_idx,
            value,
        )
    }

    /// Resolve the current value at a logical column for a given row_id.
    /// Iterates newest-first with per-volume physical mapping. Used by
    /// overlap verification to check the authoritative version after
    /// UPDATE changes a PK value + seal (schema-evolution safe).
    pub(super) fn get_authoritative_value(
        &self,
        row_id: i64,
        col_idx: usize,
    ) -> Result<Option<radixdb_core::Value>> {
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
                    let pi = if cold.mapping.is_identity {
                        col_idx
                    } else if col_idx < cold.mapping.sources.len() {
                        match &cold.mapping.sources[col_idx] {
                            super::super::writer::ColSource::Volume(vi) => *vi,
                            super::super::writer::ColSource::Default(val) => {
                                return Ok(Some(val.clone()));
                            }
                        }
                    } else {
                        return Ok(None);
                    };
                    if cold.volume.is_cold() {
                        return self
                            .read_cold_value_at(*seg_id, &cold.volume, pi, idx)
                            .map(Some);
                    }
                    cold.volume.mark_accessed();
                    return Ok(Some(cold.volume.columns[pi].get_value(idx)));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn check_value_exists_impl(
        &self,
        seg_ids: &[u64],
        segs: &FxHashMap<u64, ColdSegment>,
        ts: &FxHashMap<i64, u64>,
        col_idx: usize,
        value: &radixdb_core::Value,
    ) -> Result<Option<i64>> {
        let mut seen = FxHashSet::default();

        for &seg_id in seg_ids {
            let Some(cold) = segs.get(&seg_id) else {
                continue;
            };
            let vol = &cold.volume;
            // Resolve logical col_idx to physical index via per-volume mapping.
            // After DROP COLUMN, older volumes may store the column at a
            // different position than the current schema ordinal.
            let pi = if cold.mapping.is_identity {
                if col_idx >= vol.columns.len() {
                    continue;
                }
                col_idx
            } else if col_idx < cold.mapping.sources.len() {
                match &cold.mapping.sources[col_idx] {
                    super::super::writer::ColSource::Volume(vi) => *vi,
                    super::super::writer::ColSource::Default(_) => continue,
                }
            } else {
                continue;
            };
            if pi >= vol.meta.zone_maps.len() || !vol.meta.zone_maps[pi].may_contain_eq(value) {
                continue;
            }
            let target = match value {
                radixdb_core::Value::Integer(int_val) => Some(*int_val),
                radixdb_core::Value::Timestamp(ts_val) => Some(
                    ts_val
                        .timestamp_nanos_opt()
                        .unwrap_or(ts_val.timestamp() * 1_000_000_000),
                ),
                _ => None,
            };
            if let Some(target) = target {
                {
                    let group_count = self.cold_group_count(seg_id, vol)?;
                    for group_idx in 0..group_count {
                        let mut blocks =
                            self.read_cold_blocks_from_volume(seg_id, vol, &[pi], group_idx)?;
                        let Some((_, column)) = blocks.pop() else {
                            continue;
                        };
                        let group_range = self.cold_group_range(seg_id, vol, group_idx)?;
                        for local_idx in 0..group_range.len() {
                            if column.is_null(local_idx) || column.get_i64(local_idx) != target {
                                continue;
                            }
                            let i = group_range.start + local_idx;
                            let rid = vol.meta.row_ids.at(i);
                            if !seen.insert(rid) || ts.contains_key(&rid) {
                                continue;
                            }
                            if seg_ids.len() > 1 {
                                if let Some(current_val) =
                                    self.get_authoritative_value(rid, col_idx)?
                                {
                                    if &current_val != value {
                                        continue;
                                    }
                                }
                            }
                            return Ok(Some(rid));
                        }
                    }
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// Find a visible cold row ID matching the given column values.
    /// Uses a three-tier pruning strategy per volume (newest first):
    ///   1. Zone map: skip if value outside [min, max] for any column
    ///   2. Bloom filter: skip if any column says "definitely not"
    ///   3. Per-volume hash index: O(1) lookup (lazily built, never invalidated)
    ///
    /// No global cache. Each volume's hash index is built once on first use
    /// and lives on the immutable FrozenVolume. Zero invalidation cost.
    /// Find row by values using a pre-captured snapshot (no lock acquisition).
    pub fn find_row_id_by_values_with_snapshot(
        &self,
        snapshot: &ColdSnapshot,
        col_indices: &[usize],
        values: &[&Value],
        column_defaults: &[Value],
    ) -> Result<Option<i64>> {
        self.find_row_id_by_values_impl(
            &snapshot.seg_ids,
            &snapshot.segs,
            &snapshot.ts,
            col_indices,
            values,
            column_defaults,
        )
    }

    pub fn find_row_id_by_values(
        &self,
        col_indices: &[usize],
        values: &[&Value],
        column_defaults: &[Value],
    ) -> Result<Option<i64>> {
        let snapshot = self.cold_snapshot();
        self.find_row_id_by_values_impl(
            &snapshot.seg_ids,
            &snapshot.segs,
            &snapshot.ts,
            col_indices,
            values,
            column_defaults,
        )
    }

    pub(super) fn find_row_id_by_values_impl(
        &self,
        seg_ids: &[u64],
        segs: &FxHashMap<u64, ColdSegment>,
        ts: &FxHashMap<i64, u64>,
        col_indices: &[usize],
        values: &[&Value],
        column_defaults: &[Value],
    ) -> Result<Option<i64>> {
        if col_indices.is_empty() || col_indices.len() != values.len() {
            return Ok(None);
        }

        if seg_ids.is_empty() {
            return Ok(None);
        }

        let bloom_hashes: smallvec::SmallVec<[u64; 4]> = values
            .iter()
            .map(|v| super::super::column::ColumnBloomFilter::hash_value_static(v))
            .collect();

        // Track seen row_ids for newest-first dedup across overlapping volumes.
        let mut seen = FxHashSet::default();

        for &seg_id in seg_ids {
            let Some(cold) = segs.get(&seg_id) else {
                continue;
            };
            let vol = &cold.volume;
            // Derive remap from ColdSegment.mapping (already cached per volume).
            // Maps schema column indices to volume column indices.
            // Missing columns (ColSource::Default) are usize::MAX.
            let mut vol_col_indices: smallvec::SmallVec<[usize; 4]> =
                smallvec::SmallVec::with_capacity(col_indices.len());
            let mut has_missing = false;
            let mut skip_vol = false;
            for (i, &ci) in col_indices.iter().enumerate() {
                if ci < cold.mapping.sources.len() {
                    match &cold.mapping.sources[ci] {
                        super::super::writer::ColSource::Volume(vi) => {
                            vol_col_indices.push(*vi);
                        }
                        super::super::writer::ColSource::Default(_) => {
                            has_missing = true;
                            // Column missing from volume. If searched value
                            // doesn't match default, no row can match.
                            if *values[i] != column_defaults[i] {
                                skip_vol = true;
                                break;
                            }
                            vol_col_indices.push(usize::MAX);
                        }
                    }
                } else {
                    // Schema column index out of range for this mapping
                    has_missing = true;
                    if *values[i] != column_defaults[i] {
                        skip_vol = true;
                        break;
                    }
                    vol_col_indices.push(usize::MAX);
                }
            }
            if skip_vol {
                continue;
            }

            // Tier 1: Zone map pruning
            let mut zone_skip = false;
            for (i, &vi) in vol_col_indices.iter().enumerate() {
                if vi < vol.meta.zone_maps.len()
                    && !vol.meta.zone_maps[vi].may_contain_eq(values[i])
                {
                    zone_skip = true;
                    break;
                }
            }
            if zone_skip {
                continue;
            }

            // Tier 2: Bloom filter pruning
            let mut bloom_skip = false;
            for (i, &vi) in vol_col_indices.iter().enumerate() {
                if vol.meta.column_types.get(vi).is_some_and(|data_type| {
                    super::super::column::ColumnBloomFilter::supports_definitive_pruning(
                        *data_type, values[i],
                    )
                }) && vi < vol.meta.bloom_filters.len()
                    && !vol.meta.bloom_filters[vi].might_contain_hash(bloom_hashes[i])
                {
                    bloom_skip = true;
                    break;
                }
            }
            if bloom_skip {
                continue;
            }

            // Tier 3: Per-volume hash index
            let mut vol_result: Option<i64> = None;

            {
                // artifact-backed metadata-only fast path: use the persisted posting source
                // without materializing full columns or rebuilding a resident
                // per-volume index. Only candidates are read back to verify
                // hash collisions. Volumes predating the posting set fall back
                // to a direct participating-column scan below.
                let mut used_prebuilt_index = false;
                let mut prebuilt_index_read_failed = false;
                if !has_missing {
                    let mut query_hasher = super::super::index_hash::PersistedIndexHasher::new();
                    for (value_idx, &vi) in vol_col_indices.iter().enumerate() {
                        let Some(&data_type) = vol.meta.column_types.get(vi) else {
                            prebuilt_index_read_failed = true;
                            break;
                        };
                        if !super::super::index_hash::persisted_index_hash_is_compatible(
                            data_type,
                            Some(values[value_idx]),
                        ) {
                            prebuilt_index_read_failed = true;
                            break;
                        }
                        query_hasher.add_value(data_type, values[value_idx]);
                    }
                    if !prebuilt_index_read_failed {
                        let query_hash = query_hasher.finish();

                        if vol
                            .visit_exact_posting_candidates(
                                vol_col_indices.as_slice(),
                                query_hash,
                                |row_idx| {
                                    let row_idx = row_idx as usize;
                                    let Some(rid) = vol.meta.row_ids.get(row_idx) else {
                                        prebuilt_index_read_failed = true;
                                        return Ok(true);
                                    };
                                    if ts.contains_key(&rid) {
                                        return Ok(false);
                                    }
                                    let mut matches = true;
                                    for (value_idx, &vi) in vol_col_indices.iter().enumerate() {
                                        let candidate_value =
                                            self.read_cold_value_at(seg_id, vol, vi, row_idx)?;
                                        if candidate_value.is_null()
                                            || &candidate_value != values[value_idx]
                                        {
                                            matches = false;
                                            break;
                                        }
                                    }
                                    if matches && seen.insert(rid) {
                                        vol_result = Some(rid);
                                        return Ok(true);
                                    }
                                    Ok(false)
                                },
                            )?
                            .is_some()
                        {
                            used_prebuilt_index = true;
                        }
                    }
                }

                if used_prebuilt_index && !prebuilt_index_read_failed {
                    if vol_result.is_some() {
                        instrumentation::record_ram_accelerator_hit();
                    } else {
                        instrumentation::record_ram_accelerator_miss();
                    }
                    if vol_result.is_none() {
                        continue;
                    }
                } else {
                    instrumentation::record_ram_accelerator_fallback();
                    // artifact-backed metadata-only fallback path: read participating
                    // physical columns by row-group blocks, not as whole
                    // materialized columns.
                    let present_cols: smallvec::SmallVec<[(usize, usize); 4]> = vol_col_indices
                        .iter()
                        .enumerate()
                        .filter_map(|(value_idx, &vi)| {
                            if vi == usize::MAX {
                                None
                            } else {
                                Some((value_idx, vi))
                            }
                        })
                        .collect();
                    let physical_cols: smallvec::SmallVec<[usize; 4]> =
                        present_cols.iter().map(|(_, vi)| *vi).collect();
                    let group_count = self.cold_group_count(seg_id, vol)?;
                    for group_idx in 0..group_count {
                        let blocks = self.read_cold_blocks_from_volume(
                            seg_id,
                            vol,
                            physical_cols.as_slice(),
                            group_idx,
                        )?;
                        let group_range = self.cold_group_range(seg_id, vol, group_idx)?;
                        for local_idx in 0..group_range.len() {
                            let i = group_range.start + local_idx;
                            let rid = vol.meta.row_ids.at(i);
                            if ts.contains_key(&rid) || !seen.insert(rid) {
                                continue;
                            }
                            let mut matches = true;
                            for (value_idx, vi) in &present_cols {
                                let Some((_, column)) =
                                    blocks.iter().find(|(block_col, _)| block_col == vi)
                                else {
                                    matches = false;
                                    break;
                                };
                                let v = column.get_value(local_idx);
                                if v.is_null() || v != *values[*value_idx] {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                vol_result = Some(rid);
                                break;
                            }
                        }
                        if vol_result.is_some() {
                            break;
                        }
                    }
                }
            }
            if let Some(rid) = vol_result {
                // Verify this is the authoritative version. After UPDATE old→new
                // + seal, overlapping volumes can have the same row_id with
                // different values. The older volume's stale value is not
                // tombstoned (tombstone cleared when row_id appeared in the newer
                // volume). get_cold_row returns the newest version (newest-first).
                // Use column_defaults for columns missing from schema-evolved volumes.
                if seg_ids.len() > 1 {
                    let mut still_matches = true;
                    for (i, &ci) in col_indices.iter().enumerate() {
                        if let Some(v) = self.get_authoritative_value(rid, ci)? {
                            if v.is_null() || v != *values[i] {
                                still_matches = false;
                                break;
                            }
                        } else if column_defaults[i] != *values[i] {
                            still_matches = false;
                            break;
                        }
                    }
                    if !still_matches {
                        continue; // stale value in older volume, skip
                    }
                }
                return Ok(Some(rid));
            }
        }
        Ok(None)
    }

    /// Resolve every visible cold row matching one exact composite key through
    /// persisted per-volume postings.
    ///
    /// `Ok(None)` means at least one published volume does not carry the
    /// requested posting set (for example, it predates the index). Callers must
    /// then use the ordinary artifact-backed scan and must not advertise an index access path.
    /// `Ok(Some(ids))` is a complete candidate set, including the empty set.
    pub fn find_row_ids_by_exact_index(
        &self,
        col_indices: &[usize],
        values: &[&Value],
        snapshot_seq: Option<u64>,
        max_candidates: Option<usize>,
    ) -> Result<Option<Vec<i64>>> {
        if col_indices.is_empty() || col_indices.len() != values.len() {
            return Ok(None);
        }

        let snapshot = self.cold_snapshot();
        if snapshot.seg_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Prove completeness before reading candidates. A partial set is worse
        // than a scan because it can silently omit rows from older volumes.
        let mut volume_columns = Vec::with_capacity(snapshot.seg_ids.len());
        for &segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(&segment_id) else {
                return Ok(None);
            };
            let mut physical = smallvec::SmallVec::<[usize; 4]>::new();
            for &schema_col in col_indices {
                let Some(source) = cold.mapping.sources.get(schema_col) else {
                    return Ok(None);
                };
                let super::super::writer::ColSource::Volume(volume_col) = source else {
                    // A schema default represents every row in this volume; it
                    // cannot be served by a bounded persisted posting list.
                    return Ok(None);
                };
                physical.push(*volume_col);
            }
            if !cold.volume.has_exact_postings(physical.as_slice()) {
                return Ok(None);
            }
            for (value_idx, &volume_col) in physical.iter().enumerate() {
                let Some(&data_type) = cold.volume.meta.column_types.get(volume_col) else {
                    return Err(Error::internal(format!(
                        "cold segment {} exact index column {} has no stored type",
                        segment_id, volume_col
                    )));
                };
                if !super::super::index_hash::persisted_index_hash_is_compatible(
                    data_type,
                    Some(values[value_idx]),
                ) {
                    return Ok(None);
                }
            }
            volume_columns.push((segment_id, physical));
        }

        instrumentation::record_posting_exact_lookup_fanout(volume_columns.len());

        let mut result = Vec::new();
        let mut seen = FxHashSet::default();
        for (segment_id, physical) in volume_columns {
            let cold = snapshot
                .segs
                .get(&segment_id)
                .expect("exact-index coverage was checked above");
            let volume = &cold.volume;

            if let Some(source) = volume.artifact_index_source() {
                let owned_values = values
                    .iter()
                    .map(|value| (*value).clone())
                    .collect::<Vec<_>>();
                let remaining = max_candidates.map(|limit| limit.saturating_sub(result.len()));
                let Some(row_ordinals) =
                    source.lookup_equality(physical.as_slice(), &owned_values, remaining)?
                else {
                    return Ok(None);
                };
                for row_ordinal in row_ordinals {
                    let row_idx = usize::try_from(row_ordinal).map_err(|_| {
                        Error::internal(format!(
                            "cold segment {segment_id} INDEX row ordinal exceeds usize"
                        ))
                    })?;
                    if !cold.is_visible(row_idx) {
                        continue;
                    }
                    let row_id = volume.row_id_at(row_idx)?;
                    if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                        snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                    }) {
                        continue;
                    }
                    let mut matches = true;
                    for (value_idx, &volume_col) in physical.iter().enumerate() {
                        let candidate =
                            self.read_cold_value_at(segment_id, volume, volume_col, row_idx)?;
                        if candidate.is_null() || &candidate != values[value_idx] {
                            matches = false;
                            break;
                        }
                    }
                    if matches && seen.insert(row_id) {
                        result.push(row_id);
                        if max_candidates.is_some_and(|limit| result.len() > limit) {
                            return Ok(None);
                        }
                    }
                }
                continue;
            }

            let mut query_hasher = super::super::index_hash::PersistedIndexHasher::new();
            for (value_idx, &volume_col) in physical.iter().enumerate() {
                let Some(&data_type) = volume.meta.column_types.get(volume_col) else {
                    return Err(Error::internal(format!(
                        "cold segment {} exact index column {} has no stored type",
                        segment_id, volume_col
                    )));
                };
                query_hasher.add_value(data_type, values[value_idx]);
            }
            let query_hash = query_hasher.finish();
            let mut candidate_limit_exceeded = false;
            volume
                .visit_exact_posting_candidates(physical.as_slice(), query_hash, |row_idx| {
                    let row_idx = row_idx as usize;
                    if !cold.is_visible(row_idx) {
                        return Ok(false);
                    }
                    let Some(row_id) = volume.meta.row_ids.get(row_idx) else {
                        return Err(Error::internal(format!(
                            "cold segment {} exact index row {} exceeds row-id metadata",
                            segment_id, row_idx
                        )));
                    };
                    if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                        snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                    }) {
                        return Ok(false);
                    }

                    for (value_idx, &volume_col) in physical.iter().enumerate() {
                        let candidate =
                            self.read_cold_value_at(segment_id, volume, volume_col, row_idx)?;
                        if candidate.is_null() || &candidate != values[value_idx] {
                            return Ok(false);
                        }
                    }
                    if seen.insert(row_id) {
                        result.push(row_id);
                        if max_candidates.is_some_and(|limit| result.len() > limit) {
                            candidate_limit_exceeded = true;
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })?
                .ok_or_else(|| {
                    Error::internal("exact posting source disappeared after coverage check")
                })?;
            if candidate_limit_exceeded {
                return Ok(None);
            }
        }

        result.sort_unstable();
        Ok(Some(result))
    }

    /// Resolve bounded posting-hash candidates for one exact composite key.
    ///
    /// Unlike [`Self::find_row_ids_by_exact_index`], this method deliberately
    /// does not decode participating column blocks to verify every candidate.
    /// It is only for table scan paths that immediately materialize the
    /// authoritative rows and re-evaluate the complete predicate. Keeping that
    /// single verification at the table boundary avoids one random artifact-backed decode
    /// per matching posting while preserving collision and shadow safety.
    pub fn find_candidate_row_ids_by_exact_index(
        &self,
        col_indices: &[usize],
        values: &[&Value],
        snapshot_seq: Option<u64>,
        max_candidates: Option<usize>,
    ) -> Result<Option<Vec<i64>>> {
        if col_indices.is_empty() || col_indices.len() != values.len() {
            return Ok(None);
        }

        let snapshot = self.cold_snapshot();
        if snapshot.seg_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut volume_columns = Vec::with_capacity(snapshot.seg_ids.len());
        for &segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(&segment_id) else {
                return Ok(None);
            };
            let mut physical = smallvec::SmallVec::<[usize; 4]>::new();
            for &schema_col in col_indices {
                let Some(super::super::writer::ColSource::Volume(volume_col)) =
                    cold.mapping.sources.get(schema_col)
                else {
                    return Ok(None);
                };
                physical.push(*volume_col);
            }
            if !cold.volume.has_exact_postings(physical.as_slice()) {
                return Ok(None);
            }
            for (value_idx, &volume_col) in physical.iter().enumerate() {
                let Some(&data_type) = cold.volume.meta.column_types.get(volume_col) else {
                    return Err(Error::internal(format!(
                        "cold segment {} exact index column {} has no stored type",
                        segment_id, volume_col
                    )));
                };
                if !super::super::index_hash::persisted_index_hash_is_compatible(
                    data_type,
                    Some(values[value_idx]),
                ) {
                    return Ok(None);
                }
            }
            volume_columns.push((segment_id, physical));
        }

        instrumentation::record_posting_exact_lookup_fanout(volume_columns.len());

        let mut result = Vec::new();
        let mut seen = FxHashSet::default();
        for (segment_id, physical) in volume_columns {
            let cold = snapshot
                .segs
                .get(&segment_id)
                .expect("exact-index candidate coverage was checked above");
            if let Some(source) = cold.volume.artifact_index_source() {
                let owned_values = values
                    .iter()
                    .map(|value| (*value).clone())
                    .collect::<Vec<_>>();
                let remaining = max_candidates.map(|limit| limit.saturating_sub(result.len()));
                let Some(row_ordinals) =
                    source.lookup_equality(physical.as_slice(), &owned_values, remaining)?
                else {
                    return Ok(None);
                };
                for row_ordinal in row_ordinals {
                    let row_idx = usize::try_from(row_ordinal).map_err(|_| {
                        Error::internal(format!(
                            "cold segment {segment_id} INDEX row ordinal exceeds usize"
                        ))
                    })?;
                    if !cold.is_visible(row_idx) {
                        continue;
                    }
                    let row_id = cold.volume.row_id_at(row_idx)?;
                    if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                        snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                    }) {
                        continue;
                    }
                    if seen.insert(row_id) {
                        result.push(row_id);
                        if max_candidates.is_some_and(|limit| result.len() > limit) {
                            return Ok(None);
                        }
                    }
                }
                continue;
            }
            let mut query_hasher = super::super::index_hash::PersistedIndexHasher::new();
            for (value_idx, &volume_col) in physical.iter().enumerate() {
                let data_type = cold.volume.meta.column_types[volume_col];
                query_hasher.add_value(data_type, values[value_idx]);
            }
            let query_hash = query_hasher.finish();
            let mut candidate_limit_exceeded = false;
            cold.volume
                .visit_exact_posting_candidates(physical.as_slice(), query_hash, |row_idx| {
                    let row_idx = row_idx as usize;
                    if !cold.is_visible(row_idx) {
                        return Ok(false);
                    }
                    let Some(row_id) = cold.volume.meta.row_ids.get(row_idx) else {
                        return Err(Error::internal(format!(
                            "cold segment {} exact index row {} exceeds row-id metadata",
                            segment_id, row_idx
                        )));
                    };
                    if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                        snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                    }) {
                        return Ok(false);
                    }
                    if seen.insert(row_id) {
                        result.push(row_id);
                        if max_candidates.is_some_and(|limit| result.len() > limit) {
                            candidate_limit_exceeded = true;
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })?
                .ok_or_else(|| {
                    Error::internal(
                        "exact posting candidate source disappeared after coverage check",
                    )
                })?;
            if candidate_limit_exceeded {
                return Ok(None);
            }
        }

        result.sort_unstable();
        Ok(Some(result))
    }

    /// Resolve bounded physical candidates for several values of one exact
    /// indexed column without materializing candidate values one row at a
    /// time.
    ///
    /// The returned IDs are posting-hash candidates, not yet an exact logical
    /// result. The table layer must perform one authoritative projected fetch
    /// and value recheck after merging hot and cold IDs. Keeping that recheck
    /// at the table boundary both handles hash collisions/newer hot versions
    /// and lets artifact-backed decode each participating row group once per key batch.
    pub fn find_candidate_row_ids_by_exact_index_values(
        &self,
        col_index: usize,
        values: &[Value],
        snapshot_seq: Option<u64>,
        max_candidates: Option<usize>,
    ) -> Result<Option<Vec<i64>>> {
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let snapshot = self.cold_snapshot();
        if snapshot.seg_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Prove complete posting coverage before returning any candidates.
        let mut volume_columns = Vec::with_capacity(snapshot.seg_ids.len());
        for &segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(&segment_id) else {
                return Ok(None);
            };
            let Some(super::super::writer::ColSource::Volume(volume_col)) =
                cold.mapping.sources.get(col_index)
            else {
                return Ok(None);
            };
            if !cold.volume.has_exact_postings(&[*volume_col]) {
                return Ok(None);
            }
            let Some(&data_type) = cold.volume.meta.column_types.get(*volume_col) else {
                return Err(Error::internal(format!(
                    "cold segment {} exact index column {} has no stored type",
                    segment_id, volume_col
                )));
            };
            if values.iter().any(|value| {
                !super::super::index_hash::persisted_index_hash_is_compatible(
                    data_type,
                    Some(value),
                )
            }) {
                return Ok(None);
            }
            volume_columns.push((segment_id, *volume_col, data_type));
        }

        instrumentation::record_posting_exact_lookup_fanout(volume_columns.len());

        let mut result = Vec::new();
        let mut seen = FxHashSet::default();
        for (segment_id, volume_col, data_type) in volume_columns {
            let cold = snapshot
                .segs
                .get(&segment_id)
                .expect("exact-index candidate coverage was checked above");
            for value in values {
                if let Some(source) = cold.volume.artifact_index_source() {
                    let remaining = max_candidates.map(|limit| limit.saturating_sub(result.len()));
                    let Some(row_ordinals) = source.lookup_equality(
                        &[volume_col],
                        std::slice::from_ref(value),
                        remaining,
                    )?
                    else {
                        return Ok(None);
                    };
                    for row_ordinal in row_ordinals {
                        let row_idx = usize::try_from(row_ordinal).map_err(|_| {
                            Error::internal(format!(
                                "cold segment {segment_id} INDEX row ordinal exceeds usize"
                            ))
                        })?;
                        if !cold.is_visible(row_idx) {
                            continue;
                        }
                        let row_id = cold.volume.row_id_at(row_idx)?;
                        if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                            snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                        }) {
                            continue;
                        }
                        if seen.insert(row_id) {
                            result.push(row_id);
                            if max_candidates.is_some_and(|limit| result.len() > limit) {
                                return Ok(None);
                            }
                        }
                    }
                    continue;
                }
                let mut query_hasher = super::super::index_hash::PersistedIndexHasher::new();
                query_hasher.add_value(data_type, value);
                let query_hash = query_hasher.finish();
                let mut candidate_limit_exceeded = false;
                cold.volume
                    .visit_exact_posting_candidates(&[volume_col], query_hash, |row_idx| {
                        let row_idx = row_idx as usize;
                        if !cold.is_visible(row_idx) {
                            return Ok(false);
                        }
                        let Some(row_id) = cold.volume.meta.row_ids.get(row_idx) else {
                            return Err(Error::internal(format!(
                                "cold segment {} exact index row {} exceeds row-id metadata",
                                segment_id, row_idx
                            )));
                        };
                        if snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                            snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                        }) {
                            return Ok(false);
                        }
                        if seen.insert(row_id) {
                            result.push(row_id);
                            if max_candidates.is_some_and(|limit| result.len() > limit) {
                                candidate_limit_exceeded = true;
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    })?
                    .ok_or_else(|| {
                        Error::internal(
                            "exact posting candidate source disappeared after coverage check",
                        )
                    })?;
                if candidate_limit_exceeded {
                    return Ok(None);
                }
            }
        }

        result.sort_unstable();
        Ok(Some(result))
    }

    /// Resolve a bounded, ordered cold candidate set for an equality prefix
    /// followed by one INTEGER or TIMESTAMP range column. The equality prefix
    /// may be empty for a single-column ordered index. Every published volume must carry
    /// the posting set; otherwise the caller receives `Ok(None)` and uses the
    /// ordinary scan path without advertising an index access path.
    #[allow(clippy::too_many_arguments)]
    pub fn find_row_ids_by_ordered_index(
        &self,
        col_indices: &[usize],
        prefix_values: &[&Value],
        min: Option<(i64, bool)>,
        max: Option<(i64, bool)>,
        ascending: bool,
        limit: usize,
        snapshot_seq: Option<u64>,
        skip_row_ids: &FxHashSet<i64>,
    ) -> Result<Option<Vec<i64>>> {
        if col_indices.is_empty() || prefix_values.len() + 1 != col_indices.len() {
            return Ok(None);
        }
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        // A full-key V6 ordered accelerator can express an open bound for a
        // single-column range. For a composite equality-prefix range, an open
        // end would require prefix sentinels that are not part of the current
        // canonical-key contract; fall back to the ordinary scan instead of
        // returning a truncated cross-prefix candidate set.
        if !prefix_values.is_empty() && (min.is_none() || max.is_none()) {
            return Ok(None);
        }

        let snapshot = self.cold_snapshot();
        if snapshot.seg_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut volume_columns = Vec::with_capacity(snapshot.seg_ids.len());
        for &segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(&segment_id) else {
                return Ok(None);
            };
            let physical: Option<smallvec::SmallVec<[usize; 4]>> = col_indices
                .iter()
                .map(|&schema_col| match cold.mapping.sources.get(schema_col) {
                    Some(super::super::writer::ColSource::Volume(volume_col)) => Some(*volume_col),
                    _ => None,
                })
                .collect();
            let Some(physical) = physical else {
                return Ok(None);
            };
            if !cold.volume.has_ordered_postings(physical.as_slice()) {
                return Ok(None);
            }
            for (value_idx, &volume_col) in physical[..physical.len() - 1].iter().enumerate() {
                let Some(&data_type) = cold.volume.meta.column_types.get(volume_col) else {
                    return Err(Error::internal(format!(
                        "cold segment {} ordered index column {} has no stored type",
                        segment_id, volume_col
                    )));
                };
                if !super::super::index_hash::persisted_index_hash_is_compatible(
                    data_type,
                    Some(prefix_values[value_idx]),
                ) {
                    return Ok(None);
                }
            }
            volume_columns.push((segment_id, physical));
        }

        instrumentation::record_posting_ordered_lookup_fanout(volume_columns.len());

        let mut candidates = Vec::with_capacity(limit.saturating_mul(volume_columns.len()));
        for (segment_id, physical) in volume_columns {
            let cold = snapshot
                .segs
                .get(&segment_id)
                .expect("ordered-index coverage was checked above");
            let volume = &cold.volume;

            if let Some(source) = volume.artifact_index_source() {
                let range_column = *physical.last().expect("ordered physical key is non-empty");
                let range_type = *volume.meta.column_types.get(range_column).ok_or_else(|| {
                    Error::internal(format!(
                        "cold segment {segment_id} ordered range column has no stored type"
                    ))
                })?;
                let mut lower_values = prefix_values
                    .iter()
                    .map(|value| (*value).clone())
                    .collect::<Vec<_>>();
                let mut upper_values = lower_values.clone();
                let lower_inclusive = min.map(|(_, inclusive)| inclusive).unwrap_or(true);
                let upper_inclusive = max.map(|(_, inclusive)| inclusive).unwrap_or(true);
                if let Some((bound, _)) = min {
                    lower_values.push(ordered_runtime_bound_value(range_type, bound)?);
                }
                if let Some((bound, _)) = max {
                    upper_values.push(ordered_runtime_bound_value(range_type, bound)?);
                }
                let lower = min.map(|_| (lower_values.as_slice(), lower_inclusive));
                let upper = max.map(|_| (upper_values.as_slice(), upper_inclusive));
                let Some(row_ordinals) =
                    source.scan_ordered(physical.as_slice(), lower, upper, ascending, limit)?
                else {
                    return Ok(None);
                };
                for row_ordinal in row_ordinals {
                    let row_idx = usize::try_from(row_ordinal).map_err(|_| {
                        Error::internal(format!(
                            "cold segment {segment_id} INDEX row ordinal exceeds usize"
                        ))
                    })?;
                    if !cold.is_visible(row_idx) {
                        continue;
                    }
                    let row_id = volume.row_id_at(row_idx)?;
                    if skip_row_ids.contains(&row_id)
                        || snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                            snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                        })
                    {
                        continue;
                    }
                    let order_key = self
                        .read_cold_value_at(segment_id, volume, range_column, row_idx)?
                        .as_int64()
                        .ok_or_else(|| {
                            Error::internal("ordered INDEX returned a non-integral range key")
                        })?;
                    candidates.push((order_key, row_id));
                }
                continue;
            }
            let mut query_hasher = super::super::index_hash::PersistedIndexHasher::new();
            for (value_idx, &volume_col) in physical[..physical.len() - 1].iter().enumerate() {
                let Some(&data_type) = volume.meta.column_types.get(volume_col) else {
                    return Err(Error::internal(format!(
                        "cold segment {} ordered index column {} has no stored type",
                        segment_id, volume_col
                    )));
                };
                query_hasher.add_value(data_type, prefix_values[value_idx]);
            }
            let prefix_hash = query_hasher.finish();
            let mut accepted = 0usize;
            let mut visit =
                |entry: &super::super::index_metadata::OrderedIndexEntry| -> Result<bool> {
                    if accepted >= limit {
                        return Ok(true);
                    }
                    let row_idx = entry.row_idx as usize;
                    if !cold.is_visible(row_idx) {
                        return Ok(false);
                    }
                    let Some(row_id) = volume.meta.row_ids.get(row_idx) else {
                        return Err(Error::internal(format!(
                            "cold segment {} ordered index row {} exceeds row-id metadata",
                            segment_id, row_idx
                        )));
                    };
                    if skip_row_ids.contains(&row_id)
                        || snapshot.ts.get(&row_id).is_some_and(|&commit_seq| {
                            snapshot_seq.is_none_or(|cutoff| commit_seq <= cutoff)
                        })
                    {
                        return Ok(false);
                    }

                    // The prefix hash is only a candidate selector. Re-read the
                    // participating prefix values to make collisions harmless.
                    for (value_idx, &volume_col) in
                        physical[..physical.len() - 1].iter().enumerate()
                    {
                        let candidate =
                            self.read_cold_value_at(segment_id, volume, volume_col, row_idx)?;
                        if candidate.is_null() || &candidate != prefix_values[value_idx] {
                            return Ok(false);
                        }
                    }
                    candidates.push((entry.order_key, row_id));
                    accepted += 1;
                    Ok(accepted >= limit)
                };

            volume
                .visit_ordered_posting_candidates(
                    physical.as_slice(),
                    prefix_hash,
                    min,
                    max,
                    ascending,
                    |entry| visit(&entry),
                )?
                .ok_or_else(|| {
                    Error::internal("ordered posting source disappeared after coverage check")
                })?;
        }

        candidates.sort_unstable_by(|left, right| {
            let ordering = (left.0, left.1).cmp(&(right.0, right.1));
            if ascending {
                ordering
            } else {
                ordering.reverse()
            }
        });
        let mut seen = FxHashSet::default();
        Ok(Some(
            candidates
                .into_iter()
                .filter_map(|(_, row_id)| seen.insert(row_id).then_some(row_id))
                .take(limit)
                .collect(),
        ))
    }

    /// Return true only when every currently published cold volume can answer
    /// the exact lookup from persisted postings. This metadata-only predicate is
    /// used by EXPLAIN and never opens payload blocks.
    pub fn has_exact_index_columns(&self, col_indices: &[usize]) -> bool {
        if col_indices.is_empty() {
            return false;
        }
        let snapshot = self.cold_snapshot();
        !snapshot.seg_ids.is_empty()
            && snapshot.seg_ids.iter().all(|segment_id| {
                let Some(cold) = snapshot.segs.get(segment_id) else {
                    return false;
                };
                let physical: Option<smallvec::SmallVec<[usize; 4]>> = col_indices
                    .iter()
                    .map(|&schema_col| match cold.mapping.sources.get(schema_col) {
                        Some(super::super::writer::ColSource::Volume(volume_col)) => {
                            Some(*volume_col)
                        }
                        _ => None,
                    })
                    .collect();
                physical.is_some_and(|columns| {
                    columns.iter().all(|&column| {
                        cold.volume
                            .meta
                            .column_types
                            .get(column)
                            .is_some_and(|data_type| {
                                super::super::index_hash::persisted_index_hash_is_compatible(
                                    *data_type, None,
                                )
                            })
                    }) && cold.volume.has_exact_postings(columns.as_slice())
                })
            })
    }

    /// Return true only when every published cold volume carries one complete
    /// ordered posting set for the requested logical index prefix.
    pub fn has_ordered_index_columns(&self, col_indices: &[usize]) -> bool {
        if col_indices.is_empty() {
            return false;
        }
        let snapshot = self.cold_snapshot();
        !snapshot.seg_ids.is_empty()
            && snapshot.seg_ids.iter().all(|segment_id| {
                let Some(cold) = snapshot.segs.get(segment_id) else {
                    return false;
                };
                let physical: Option<smallvec::SmallVec<[usize; 4]>> = col_indices
                    .iter()
                    .map(|&schema_col| match cold.mapping.sources.get(schema_col) {
                        Some(super::super::writer::ColSource::Volume(volume_col)) => {
                            Some(*volume_col)
                        }
                        _ => None,
                    })
                    .collect();
                physical.is_some_and(|columns| {
                    columns.iter().all(|&column| {
                        cold.volume
                            .meta
                            .column_types
                            .get(column)
                            .is_some_and(|data_type| {
                                super::super::index_hash::persisted_index_hash_is_compatible(
                                    *data_type, None,
                                )
                            })
                    }) && cold.volume.has_ordered_postings(columns.as_slice())
                })
            })
    }
}

fn ordered_runtime_bound_value(data_type: radixdb_core::DataType, value: i64) -> Result<Value> {
    match data_type {
        radixdb_core::DataType::Integer => Ok(Value::Integer(value)),
        radixdb_core::DataType::Timestamp => Ok(Value::timestamp(
            chrono::DateTime::from_timestamp_nanos(value),
        )),
        _ => Err(Error::internal(
            "ordered runtime bound is not INTEGER or TIMESTAMP",
        )),
    }
}
