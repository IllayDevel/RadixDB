macro_rules! segmented_table_aggregate_methods {
    () => {
        // =========================================================================
        // Row count
        // =========================================================================

        fn row_count(&self) -> usize {
            // Snapshot isolation: deduped_row_count and the fast path subtract ALL
            // tombstones, but a snapshot may not see newer ones. Use a zero-width
            // streaming scan rather than retaining every visible Row.
            if self.snapshot_seq.is_some() {
                return self.count_visible_rows_streaming().unwrap_or(0);
            }
            let seal_guard = self.segment_mgr.acquire_seal_read();
            if self.segment_mgr.seal_overlap() > 0 {
                drop(seal_guard);
                return self.count_visible_rows_streaming().unwrap_or(0);
            }
            // Outside seal overlap the deduplicated cold count and hot count have
            // disjoint publication ownership and can be combined in O(1).
            let seg = self.segment_mgr.deduped_row_count();
            let pending = self.segment_mgr.pending_tombstone_count(self.txn_id());
            seg.saturating_sub(pending) + self.hot.row_count()
        }

        fn row_count_hint(&self) -> usize {
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let seg = self.segment_row_count_hint();
            let pending = self.segment_mgr.pending_tombstone_count(self.txn_id());
            let overlap = self.segment_mgr.seal_overlap();
            seg.saturating_sub(pending) + self.hot.row_count_hint().saturating_sub(overlap)
        }

        fn fast_row_count(&self) -> Option<usize> {
            // Snapshot isolation: deduped_row_count subtracts ALL tombstones, but
            // this snapshot may not see newer tombstones. Fall back to scan which
            // correctly filters by snapshot_seq.
            if self.snapshot_seq.is_some() {
                return None;
            }
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }
            let hot_count = self.hot.fast_row_count()?;
            let seg = self.segment_mgr.deduped_row_count();
            let pending = self.segment_mgr.pending_tombstone_count(self.txn_id());
            Some(seg.saturating_sub(pending) + hot_count)
        }

        // =========================================================================
        // Aggregation pushdown
        // =========================================================================

        fn sum_column(&self, col_idx: usize) -> Option<DeferredSum> {
            // Snapshot isolation: cold aggregation uses tombstones without snapshot
            // filtering. Bail so the executor falls back to full scan.
            if self.snapshot_seq.is_some() {
                return None;
            }
            let hot_result = self.hot.sum_column(col_idx);

            if !self.segment_mgr.has_segments() {
                return hot_result;
            }

            // During seal, hot+cold overlap — can't reliably sum
            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let mut total_sum = hot_result?;

            // Pre-compute default contribution for schema-evolved volumes
            // that are missing this column (added via ALTER TABLE ADD COLUMN).
            let default_val = self.column_default(col_idx);
            let default_numeric = match &default_val {
                Value::Integer(v) => Some((Some(*v), None)),
                Value::Float(v) => Some((None, Some(*v))),
                _ => None, // NULL or non-numeric default → no contribution
            };

            // Resolve column name for per-volume physical index lookup.
            let schema = self.hot.schema();
            // Column resolution uses mapping (handles renames/drops) not col_name.

            // Stats fast path is only safe when there are no tombstones AND no
            // overlapping row_ids between volumes. After UPDATE + seal, tombstones
            // are cleared but overlap remains (two volumes share the same row_id).
            // Compare raw vs deduped count to detect this (cached, O(1)).
            let can_use_stats = self.segment_mgr.is_tombstone_set_empty()
                && !self.segment_mgr.has_pending_tombstones(self.txn_id())
                && self.segment_mgr.total_row_count() == self.segment_mgr.deduped_row_count();

            if can_use_stats {
                // Fast path: use pre-computed volume stats.
                // Accumulate integer and float parts separately to avoid precision
                // loss from per-volume i128→f64 conversion. Final publication
                // retains Integer/DECIMAL identity unless Float input participated.
                // Use segments_raw (metadata only, no cold volume reload).
                let segs = self.segment_mgr.segments_raw();
                let mut statistics_sum = total_sum;
                let mut statistics_complete = true;
                for cs in segs.values() {
                    let mapping = self.segment_mgr.get_cold_segment_mapping(cs, schema);
                    let phys = if col_idx < mapping.sources.len() {
                        match &mapping.sources[col_idx] {
                            crate::volume::writer::ColSource::Volume(vi) => Some(*vi),
                            crate::volume::writer::ColSource::Default(_) => None,
                        }
                    } else {
                        None
                    };
                    if let Some(pi) = phys {
                        if pi < cs.volume.meta.stats.columns.len() {
                            let column_stats = &cs.volume.meta.stats.columns[pi];
                            if matches!(
                                cs.volume.meta.column_types.get(pi),
                                Some(DataType::Integer | DataType::Float)
                            ) && column_stats.numeric_count != column_stats.non_null_count
                            {
                                // Statistics are optional at the format layer.
                                // A missing numeric SUM must fall back to the
                                // bounded one-column scanner, never masquerade
                                // as an exact zero.
                                statistics_complete = false;
                                break;
                            }
                            let (int_part, float_part) = column_stats.sum_parts();
                            let numeric_count = column_stats.numeric_count as usize;
                            match cs.volume.meta.column_types.get(pi) {
                                Some(DataType::Integer) => {
                                    statistics_sum.add_integer(int_part, numeric_count)
                                }
                                Some(DataType::Float) => {
                                    statistics_sum.add_float(float_part, numeric_count)
                                }
                                _ => {}
                            }
                        } else {
                            statistics_complete = false;
                            break;
                        }
                    } else if let Some((integer, float)) = default_numeric {
                        if let Some(value) = integer {
                            statistics_sum.add_integer(
                                value as i128 * cs.volume.meta.row_count as i128,
                                cs.volume.meta.row_count,
                            );
                        } else if let Some(value) = float {
                            statistics_sum.add_float(
                                value * cs.volume.meta.row_count as f64,
                                cs.volume.meta.row_count,
                            );
                        }
                    }
                }
                if statistics_complete {
                    return Some(statistics_sum);
                }
            }

            // Tombstones/overlap exist: use the lazy one-column scanner so
            // artifact-backed-backed cold volumes stay metadata-only and read only this
            // column's row-group blocks.
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut scanners = self.create_segment_scanners_filtered(&[col_idx], None, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    if let Some(value @ (Value::Integer(_) | Value::Float(_))) =
                        scanner.row().get(0)
                    {
                        total_sum.add_value(value);
                    }
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }
            Some(total_sum)
        }

        fn min_column(&self, col_idx: usize) -> Option<Option<Value>> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let hot_result = self.hot.min_column(col_idx);

            if !self.segment_mgr.has_segments() {
                return hot_result;
            }

            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let hot_min = hot_result?;

            let schema = self.hot.schema();
            // Column resolution uses mapping (handles renames/drops) not col_name.
            let default_val = self.column_default(col_idx);
            let has_non_null_default = !default_val.is_null();

            let can_use_stats = self.segment_mgr.is_tombstone_set_empty()
                && !self.segment_mgr.has_pending_tombstones(self.txn_id())
                && self.segment_mgr.total_row_count() == self.segment_mgr.deduped_row_count();

            if can_use_stats {
                // Fast path: use pre-computed volume stats (zone map min)
                let segs = self.segment_mgr.segments_raw();
                let mut overall_min = hot_min;
                for cs in segs.values() {
                    let vol = &cs.volume;
                    let mapping = self.segment_mgr.get_cold_segment_mapping(cs, schema);
                    let phys = if col_idx < mapping.sources.len() {
                        match &mapping.sources[col_idx] {
                            crate::volume::writer::ColSource::Volume(vi) => Some(*vi),
                            crate::volume::writer::ColSource::Default(_) => None,
                        }
                    } else {
                        None
                    };
                    let vol_min = if let Some(pi) = phys {
                        if pi < vol.meta.stats.columns.len() {
                            let m = &vol.meta.stats.columns[pi].min;
                            if m.is_null() {
                                None
                            } else {
                                Some(m)
                            }
                        } else {
                            None
                        }
                    } else if has_non_null_default && vol.meta.row_count > 0 {
                        Some(&default_val)
                    } else {
                        None
                    };
                    if let Some(vm) = vol_min {
                        match &overall_min {
                            None => overall_min = Some(vm.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Less) = vm.compare(current) {
                                    overall_min = Some(vm.clone());
                                }
                            }
                        }
                    }
                }
                return Some(overall_min);
            }

            // Tombstones/overlap exist: use the lazy one-column scanner so
            // artifact-backed-backed cold volumes stay metadata-only and read only this
            // column's row-group blocks.
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut overall_min = hot_min;
            let mut scanners = self.create_segment_scanners_filtered(&[col_idx], None, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let Some(value) = scanner.row().get(0) else {
                        continue;
                    };
                    if value.is_null() {
                        continue;
                    }
                    match &overall_min {
                        None => overall_min = Some(value.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Less) = value.compare(current) {
                                overall_min = Some(value.clone());
                            }
                        }
                    }
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }
            Some(overall_min)
        }

        fn max_column(&self, col_idx: usize) -> Option<Option<Value>> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let hot_result = self.hot.max_column(col_idx);

            if !self.segment_mgr.has_segments() {
                return hot_result;
            }

            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let hot_max = hot_result?;

            let schema = self.hot.schema();
            // Column resolution uses mapping (handles renames/drops) not col_name.
            let default_val = self.column_default(col_idx);
            let has_non_null_default = !default_val.is_null();

            let can_use_stats = self.segment_mgr.is_tombstone_set_empty()
                && !self.segment_mgr.has_pending_tombstones(self.txn_id())
                && self.segment_mgr.total_row_count() == self.segment_mgr.deduped_row_count();

            if can_use_stats {
                // Fast path: use pre-computed volume stats (zone map max)
                // Use segments_raw (metadata only, no cold volume reload).
                let segs = self.segment_mgr.segments_raw();
                let mut overall_max = hot_max;
                for cs in segs.values() {
                    let vol = &cs.volume;
                    let mapping = self.segment_mgr.get_cold_segment_mapping(cs, schema);
                    let phys = if col_idx < mapping.sources.len() {
                        match &mapping.sources[col_idx] {
                            crate::volume::writer::ColSource::Volume(vi) => Some(*vi),
                            crate::volume::writer::ColSource::Default(_) => None,
                        }
                    } else {
                        None
                    };
                    let vol_max = if let Some(pi) = phys {
                        if pi < vol.meta.stats.columns.len() {
                            let m = &vol.meta.stats.columns[pi].max;
                            if m.is_null() {
                                None
                            } else {
                                Some(m)
                            }
                        } else {
                            None
                        }
                    } else if has_non_null_default && vol.meta.row_count > 0 {
                        Some(&default_val)
                    } else {
                        None
                    };
                    if let Some(vm) = vol_max {
                        match &overall_max {
                            None => overall_max = Some(vm.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Greater) = vm.compare(current) {
                                    overall_max = Some(vm.clone());
                                }
                            }
                        }
                    }
                }
                return Some(overall_max);
            }

            // Tombstones/overlap exist: use the lazy one-column scanner so
            // artifact-backed-backed cold volumes stay metadata-only and read only this
            // column's row-group blocks.
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut overall_max = hot_max;
            let mut scanners = self.create_segment_scanners_filtered(&[col_idx], None, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let Some(value) = scanner.row().get(0) else {
                        continue;
                    };
                    if value.is_null() {
                        continue;
                    }
                    match &overall_max {
                        None => overall_max = Some(value.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Greater) = value.compare(current) {
                                overall_max = Some(value.clone());
                            }
                        }
                    }
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }
            Some(overall_max)
        }

        // =========================================================================
        // Partition and index-based pushdowns
        // =========================================================================

        fn get_partition_count(&self, column_name: &str) -> Option<usize> {
            self.get_partition_values(column_name)
                .map(|values| values.len())
        }

        fn get_partition_values(&self, column_name: &str) -> Option<Vec<Value>> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            if !self.segment_mgr.has_segments() {
                return self.hot.get_partition_values(column_name);
            }

            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let schema = self.hot.schema();
            let col_idx = *schema.column_index_map().get(&column_name.to_lowercase())?;

            // Bail if hot has no index on this column — can't enumerate hot values
            // without a full scan. Returning Some with only cold values would be wrong.
            let mut distinct: ValueSet = ValueSet::default();
            let hot_values = self.hot.get_partition_values(column_name)?;
            for v in hot_values {
                distinct.insert(v);
            }

            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut scanners = self.create_segment_scanners_filtered(&[col_idx], None, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    if let Some(value) = scanner.row().get(0).cloned() {
                        distinct.insert(value);
                    }
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }

            Some(distinct.into_iter().collect())
        }

        fn compute_distinct_values(&self, col_idx: usize) -> Option<Vec<Value>> {
            let schema = self.hot.schema();
            if col_idx >= schema.columns.len() {
                return None;
            }
            let col_name = schema.columns[col_idx].name.clone();
            self.get_partition_values(&col_name)
        }

        fn collect_rows_grouped_by_partition(
            &self,
            column_name: &str,
        ) -> Option<Vec<(Value, RowVec)>> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_grouped_by_partition(column_name);
            }

            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let schema = self.hot.schema();
            let col_idx = *schema.column_index_map().get(&column_name.to_lowercase())?;

            // Start from hot grouped data
            let mut groups: ValueMap<RowVec> = ValueMap::default();
            let hot_groups = match self.hot.collect_rows_grouped_by_partition(column_name) {
                Some(groups) => groups,
                None if self.hot.row_count() == 0 => Vec::new(),
                None => return None,
            };
            for (val, rows) in hot_groups {
                groups.insert(val, rows);
            }

            // Build hot_skip: hot row_ids + pending tombstones. The scanner path
            // applies committed tombstones and per-volume visibility itself.
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let column_indices: Vec<usize> = (0..schema.columns.len()).collect();
            let mut scanners =
                self.create_segment_scanners_filtered(&column_indices, None, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let (rid, row) = scanner.take_row_with_id().ok()?;
                    let Some(val) = row.get(col_idx).cloned() else {
                        continue;
                    };
                    groups.entry(val).or_default().push((rid, row));
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }

            Some(groups.into_iter().collect())
        }

        fn get_rows_for_partition_value(
            &self,
            column_name: &str,
            partition_value: &Value,
        ) -> Option<RowVec> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .get_rows_for_partition_value(column_name, partition_value);
            }

            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }

            let schema = self.hot.schema();
            let col_idx = *schema.column_index_map().get(&column_name.to_lowercase())?;

            // Get hot rows for this partition value
            let mut result = match self
                .hot
                .get_rows_for_partition_value(column_name, partition_value)
            {
                Some(rows) => rows,
                None if self.hot.row_count() == 0 => RowVec::new(),
                None => return None,
            };

            // Build hot_skip: hot row_ids + pending tombstones. The scanner path
            // applies committed tombstones and per-volume visibility itself.
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let column_indices: Vec<usize> = (0..schema.columns.len()).collect();
            let filter = (!partition_value.is_null()).then(|| {
                crate::expression::ComparisonExpr::eq(column_name, partition_value.clone())
            });
            let where_expr = filter
                .as_ref()
                .map(|value| value as &dyn crate::expression::Expression);
            let mut scanners =
                self.create_segment_scanners_filtered(&column_indices, where_expr, hot_skip);
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let row = scanner.take_row_with_id().ok()?;
                    if !partition_value.is_null() || row.1.get(col_idx).is_some_and(Value::is_null)
                    {
                        result.push(row);
                    }
                }
                if scanner.err().is_some() {
                    return None;
                }
                if scanner.close().is_err() {
                    return None;
                }
            }

            Some(result)
        }
    };
}

pub(super) use segmented_table_aggregate_methods;
