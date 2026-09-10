//! Immutable-segment pruning, scanner construction and row materialization.

use super::*;

impl SegmentedTable {
    /// Pre-compute bloom filter hashes for equality comparisons.
    /// Call once before the volume loop to avoid redundant hashing per volume.
    pub(super) fn precompute_bloom_hashes(
        comparisons: &[(&str, radixdb_core::Operator, &Value)],
    ) -> Vec<Option<u64>> {
        comparisons
            .iter()
            .map(|&(_, op, value)| {
                if op == radixdb_core::Operator::Eq {
                    Some(crate::volume::column::ColumnBloomFilter::hash_value_static(
                        value,
                    ))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Zone map pruning + binary search narrowing on a single volume.
    /// Returns (should_skip, start, end).
    /// `bloom_hashes` are pre-computed per-comparison to avoid redundant hashing.
    pub(super) fn prune_volume(
        vol: &FrozenVolume,
        comparisons: &[(&str, radixdb_core::Operator, &Value)],
        bloom_hashes: &[Option<u64>],
    ) -> (bool, usize, usize) {
        let mut start = 0usize;
        let mut end = vol.meta.row_count;

        for (comp_idx, &(col_name, op, value)) in comparisons.iter().enumerate() {
            if let Some(col_idx) = vol.column_index(col_name) {
                let zm = &vol.meta.zone_maps[col_idx];
                let dominated = match op {
                    radixdb_core::Operator::Gt => !zm.may_contain_gt(value),
                    radixdb_core::Operator::Gte => !zm.may_contain_gte(value),
                    radixdb_core::Operator::Lt => !zm.may_contain_lt(value),
                    radixdb_core::Operator::Lte => !zm.may_contain_lte(value),
                    radixdb_core::Operator::Eq => !zm.may_contain_eq(value)
                        || (vol.meta.column_types.get(col_idx).is_some_and(|data_type| {
                            crate::volume::column::ColumnBloomFilter::supports_definitive_pruning(
                                *data_type, value,
                            )
                        }) && col_idx < vol.meta.bloom_filters.len()
                            && bloom_hashes
                                .get(comp_idx)
                                .and_then(|h| *h)
                                .is_some_and(|h| {
                                    !vol.meta.bloom_filters[col_idx].might_contain_hash(h)
                                })),
                    _ => false,
                };
                if dominated {
                    return (true, 0, 0);
                }

                // Binary search on sorted eager columns.
                // Skip for artifact-backed metadata-only cold volumes: zone maps already
                // pruned, and exact range narrowing happens inside the
                // descriptor-backed scanner/block-source path.
                if vol.is_sorted(col_idx) && !vol.is_cold() {
                    let target = match value {
                        Value::Integer(i) => Some(*i),
                        Value::Timestamp(ts) => Some(
                            ts.timestamp_nanos_opt()
                                .unwrap_or(ts.timestamp() * 1_000_000_000),
                        ),
                        _ => None,
                    };
                    if let Some(target) = target {
                        match op {
                            radixdb_core::Operator::Gte => {
                                let idx = vol.columns[col_idx].binary_search_ge(target);
                                if idx > start {
                                    start = idx;
                                }
                            }
                            radixdb_core::Operator::Gt => {
                                let idx = vol.columns[col_idx].binary_search_gt(target);
                                if idx > start {
                                    start = idx;
                                }
                            }
                            radixdb_core::Operator::Lte => {
                                let idx = vol.columns[col_idx].binary_search_gt(target);
                                if idx < end {
                                    end = idx;
                                }
                            }
                            radixdb_core::Operator::Lt => {
                                let idx = vol.columns[col_idx].binary_search_ge(target);
                                if idx < end {
                                    end = idx;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        (start >= end, start, end)
    }

    /// Refine a cold scan range through immutable row-id metadata when the
    /// predicate targets the current INTEGER PRIMARY KEY.
    ///
    /// For RadixDB's volume engine `row_id == INTEGER PRIMARY KEY` is a storage
    /// invariant. artifact-backed metadata-only volumes already keep sorted row_ids in memory,
    /// so point/range predicates on the PK can be narrowed without reading a
    /// row-group block. This is deliberately lower-level than a planner facade:
    /// the scanner still owns filtering/materialization, but it receives a tight
    /// row window instead of linearly walking a full row group.
    pub(super) fn refine_pk_metadata_range(
        vol: &FrozenVolume,
        schema: &Schema,
        mapping: &crate::volume::writer::ColumnMapping,
        comparisons: &[(&str, radixdb_core::Operator, &Value)],
        mut start: usize,
        mut end: usize,
    ) -> (bool, usize, usize) {
        let Some(pk_idx) = schema.pk_column_index() else {
            return (start >= end, start, end);
        };
        let Some(pk_col) = schema.columns.get(pk_idx) else {
            return (start >= end, start, end);
        };
        if pk_col.data_type != DataType::Integer {
            return (start >= end, start, end);
        }
        // The current PK must be backed by this volume, not by a schema-evolution
        // default. We do not need the physical column for the lookup: row_ids are
        // the authoritative sorted PK values.
        if !matches!(
            mapping.sources.get(pk_idx),
            Some(crate::volume::writer::ColSource::Volume(_))
        ) {
            return (start >= end, start, end);
        }
        if vol.meta.row_ids.is_empty() || start >= end || end > vol.meta.row_ids.len() {
            return (start >= end, start, end);
        }

        let pk_name = pk_col.name_lower.as_str();
        let Some(range) =
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(comparisons, pk_name, false)
        else {
            return (start >= end, start, end);
        };
        let (range_start, range_end) =
            range.bounds_with(vol.meta.row_ids.len(), |index| vol.meta.row_ids.at(index));
        start = start.max(range_start);
        end = end.min(range_end);
        if start >= end {
            return (true, 0, 0);
        }

        (false, start, end)
    }

    /// Create lazy segment scanners for a read query, applying zone map pruning
    /// and binary search on sorted columns for range predicates.
    ///
    /// Uses the per-row visibility bitmap on each ColdSegment for inter-volume
    /// dedup. The caller-provided `hot_skip` (hot row_ids and pending tombstones)
    /// is passed to every scanner unchanged — no per-volume accumulation needed.
    /// Committed tombstones are shared via Arc (no clone per volume).
    ///
    /// The `hot_skip` MUST be captured through `collect_shadow_row_ids_into`
    /// while the shared membership and seal fences are held. That snapshot
    /// includes transaction-local overrides without materializing payloads.
    pub(super) fn create_segment_scanners_filtered(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        hot_skip: FxHashSet<i64>,
    ) -> Vec<Box<dyn Scanner>> {
        self.create_segment_scanners_filtered_with_projection_mode(
            column_indices,
            where_expr,
            hot_skip,
            true,
            None,
        )
    }

    pub(super) fn create_segment_scanners_filtered_exact_projection(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        hot_skip: FxHashSet<i64>,
    ) -> Vec<Box<dyn Scanner>> {
        self.create_segment_scanners_filtered_with_projection_mode(
            column_indices,
            where_expr,
            hot_skip,
            false,
            None,
        )
    }

    pub(super) fn create_segment_scanners_filtered_in_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        hot_skip: FxHashSet<i64>,
        row_id_ranges: Option<Arc<Vec<(i64, i64)>>>,
        empty_projection_means_all: bool,
    ) -> Vec<Box<dyn Scanner>> {
        self.create_segment_scanners_filtered_with_projection_mode(
            column_indices,
            where_expr,
            hot_skip,
            empty_projection_means_all,
            row_id_ranges,
        )
    }

    pub(super) fn create_segment_scanners_filtered_with_projection_mode(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        hot_skip: FxHashSet<i64>,
        empty_projection_means_all: bool,
        row_id_ranges: Option<Arc<Vec<(i64, i64)>>>,
    ) -> Vec<Box<dyn Scanner>> {
        let comparisons = where_expr
            .map(|e| e.collect_comparisons())
            .unwrap_or_default();

        // Lazy: no full-column bridge upfront. Zone-map/bloom prune runs on
        // metadata (available on cold volumes). Surviving artifact-backed cold volumes are
        // scanned through VolumeScanner row-group block reads.
        let volumes = self.segment_mgr.get_volumes_newest_first_lazy();

        if volumes.is_empty() {
            return Vec::new();
        }

        // Committed tombstones are kept as a shared Arc (no clone).
        let tombstones_arc = self.segment_mgr.tombstone_set_arc();

        // hot_skip is shared across all scanners via Arc — no clone per volume.
        let hot_skip_arc = Arc::new(hot_skip);

        let bloom_hashes = Self::precompute_bloom_hashes(&comparisons);
        let mut scanners_reverse: Vec<Box<dyn Scanner>> = Vec::with_capacity(volumes.len());

        for (_seg_id, cs) in volumes.iter() {
            let vol = &cs.volume;
            let (should_skip, start, end) = Self::prune_volume(vol, &comparisons, &bloom_hashes);
            if should_skip {
                continue;
            }
            let current_schema = self.hot.schema();
            let mapping = self
                .segment_mgr
                .get_cold_segment_mapping(cs, current_schema);
            let (should_skip, start, end) = Self::refine_pk_metadata_range(
                vol,
                current_schema,
                &mapping,
                &comparisons,
                start,
                end,
            );
            if should_skip {
                continue;
            }
            // artifact-backed cold volumes stay metadata-only; VolumeScanner reads only
            // the required row-group blocks.
            let (vol, start, end) = if vol.is_cold() {
                vol.mark_accessed();
                (vol, start, end)
            } else {
                (vol, start, end)
            };

            // VolumeScanner constructor calls mark_accessed.
            let mut scanner = if start > 0 || end < vol.meta.row_count {
                if empty_projection_means_all {
                    VolumeScanner::with_range(
                        Arc::clone(vol),
                        column_indices.to_vec(),
                        start,
                        end,
                        None,
                    )
                } else {
                    VolumeScanner::with_range_exact_projection(
                        Arc::clone(vol),
                        column_indices.to_vec(),
                        start,
                        end,
                    )
                }
            } else if empty_projection_means_all {
                VolumeScanner::new(Arc::clone(vol), column_indices.to_vec(), None)
            } else {
                VolumeScanner::new_exact_projection(Arc::clone(vol), column_indices.to_vec())
            };
            // Each scanner gets the same small hot_skip Arc (no clone of hot IDs).
            // Inter-volume dedup is handled by the per-volume visibility bitmap.
            scanner.set_skip_sets(Arc::clone(&tombstones_arc), Arc::clone(&hot_skip_arc));
            scanner.set_visibility_bitmap(cs.visible.clone());
            scanner.snapshot_seq = self.snapshot_seq;
            scanner
                .set_column_mapping(mapping)
                .expect("segment manager publishes validated column mappings");

            if let Some(expr) = where_expr {
                let filter = expr.with_aliases(&Default::default());
                let mut prepared = filter;
                prepared.prepare_for_schema(current_schema);
                scanner.set_filter(prepared);
            }
            if let Some(ranges) = row_id_ranges.as_ref() {
                scanner.set_row_id_ranges(Arc::clone(ranges));
            }
            scanners_reverse.push(Box::new(scanner) as Box<dyn Scanner>);
        }

        // Reverse so oldest segments come first (consistent iteration order)
        scanners_reverse.reverse();
        scanners_reverse
    }

    /// Collect rows from segments into a RowVec, with zone map pruning
    /// and binary search on sorted columns.
    ///
    /// Uses the per-row visibility bitmap on each ColdSegment for inter-volume
    /// dedup. The caller-provided `hot_skip` (hot row_ids and pending tombstones)
    /// filters rows that are shadowed by the hot buffer.
    ///
    /// The `hot_skip` must come from the same fenced lightweight shadow snapshot
    /// used by the hot source.
    pub(super) fn collect_cold_rows(
        &self,
        where_expr: Option<&dyn Expression>,
        hot_skip: FxHashSet<i64>,
    ) -> Result<RowVec> {
        let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
        let total: usize = volumes.iter().map(|(_, cs)| cs.volume.meta.row_count).sum();
        let mut rows = RowVec::with_capacity(total.min(64_000));

        let column_indices: Vec<usize> = (0..self.hot.schema().columns.len()).collect();
        let mut scanners =
            self.create_segment_scanners_filtered(&column_indices, where_expr, hot_skip);

        for scanner in scanners.iter_mut() {
            while scanner.next() {
                rows.push(scanner.take_row_with_id()?);
            }
            if let Some(err) = scanner.err() {
                return Err(err.clone());
            }
            scanner.close()?;
        }
        Ok(rows)
    }

    /// Read-only row lookup. Unlike `find_segment_row`, artifact-backed descriptor-backed
    /// cold volumes are returned metadata-only so callers can materialize from
    /// individual column blocks instead of forcing a full volume reload.
    /// Cold volumes without a artifact-backed block source are still returned by row-id
    /// metadata; payload materialization will surface a storage error instead
    /// of calling the removed full-column bridge.
    pub(super) fn find_segment_row_for_read(
        &self,
        row_id: i64,
    ) -> Option<(u64, ColdSegment, usize)> {
        if self.hot.has_row_id(row_id) {
            return None;
        }
        {
            let ts = self.segment_mgr.tombstone_set_arc();
            if self.is_row_tombstoned(&ts, row_id) {
                return None;
            }
        }
        if self.segment_mgr.is_pending_tombstone(self.txn_id(), row_id) {
            return None;
        }

        let (seg_ids, segs) = {
            let manifest = self.segment_mgr.manifest();
            let seg_ids: Vec<u64> = manifest
                .segments
                .iter()
                .rev()
                .map(|m| m.segment_id)
                .collect();
            let segs = self.segment_mgr.segments_raw();
            (seg_ids, segs)
        };

        for &seg_id in &seg_ids {
            let Some(cold) = segs.get(&seg_id) else {
                continue;
            };
            let vol = &cold.volume;
            if vol.meta.row_ids.is_empty() {
                continue;
            }
            let min_id = vol.meta.row_ids.at(0);
            let max_id = vol.meta.row_ids.at(vol.meta.row_count - 1);
            if row_id < min_id || row_id > max_id {
                continue;
            }
            if let Ok(idx) = vol.meta.row_ids.binary_search(&row_id) {
                if vol.is_cold() {
                    vol.mark_accessed();
                    return Some((seg_id, cold.clone(), idx));
                }
                vol.mark_accessed();
                return Some((seg_id, cold.clone(), idx));
            }
        }
        None
    }

    pub(super) fn find_segment_row_in_snapshot(
        &self,
        snapshot: &crate::volume::manifest::ColdSnapshot,
        row_id: i64,
    ) -> Option<(u64, ColdSegment, usize)> {
        if self.is_row_tombstoned(&snapshot.ts, row_id)
            || self.segment_mgr.is_pending_tombstone(self.txn_id(), row_id)
        {
            return None;
        }
        for &segment_id in &snapshot.seg_ids {
            let Some(cold) = snapshot.segs.get(&segment_id) else {
                continue;
            };
            let ids = &cold.volume.meta.row_ids;
            let Some((min_id, max_id)) = ids.first().zip(ids.last()) else {
                continue;
            };
            if row_id < min_id || row_id > max_id {
                continue;
            }
            if let Ok(index) = ids.binary_search(&row_id) {
                if cold.is_visible(index) {
                    cold.volume.mark_accessed();
                    return Some((segment_id, cold.clone(), index));
                }
            }
        }
        None
    }

    pub(super) fn materialize_volume_row_for_read(
        &self,
        vol: &FrozenVolume,
        idx: usize,
        mapping: &crate::volume::writer::ColumnMapping,
    ) -> Result<Row> {
        vol.get_row_for_compaction(idx, mapping)
    }
}
