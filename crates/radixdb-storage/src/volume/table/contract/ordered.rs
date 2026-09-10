macro_rules! segmented_table_ordered_methods {
    () => {
        fn collect_rows_ordered_by_index(
            &self,
            column_name: &str,
            ascending: bool,
            limit: usize,
            offset: usize,
        ) -> Option<RowVec> {
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_ordered_by_index(
                    column_name,
                    ascending,
                    limit,
                    offset,
                );
            }

            // Snapshot isolation: the merge path doesn't filter by snapshot_seq.
            // Fall back to the full scan + sort path which handles MVCC correctly.
            if self.snapshot_seq.is_some() {
                return None;
            }

            // Only optimize when ORDER BY column is the INTEGER PRIMARY KEY.
            // For PK, row_id order == value order, so we can merge sorted sources.
            let schema = self.hot.schema().clone();
            let pk_idx = schema.pk_column_index()?;
            let pk_col = &schema.columns[pk_idx];
            if pk_col.name_lower != column_name.to_lowercase() {
                return None;
            }

            let needed = limit.saturating_add(offset);
            if needed == 0 {
                return Some(RowVec::new());
            }

            // 1. Collect hot rows in PK order. Only materialize `needed` rows
            //    (not all hot rows). The skip set uses row IDs only (no materialization).
            let hot_rows =
                match self
                    .hot
                    .collect_rows_ordered_by_index(column_name, ascending, needed, 0)
                {
                    Some(rows) => rows,
                    None => {
                        // Local changes prevent ordered iteration — fall back to collect + sort.
                        let mut rows = match self.hot.collect_all_rows(None) {
                            Ok(r) => r,
                            Err(_) => return None,
                        };
                        if ascending {
                            rows.sort_unstable_by_key(|&(id, _)| id);
                        } else {
                            rows.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
                        }
                        rows.truncate(needed);
                        rows
                    }
                };

            // 2. Build skip set from ALL hot row IDs (no Row materialization).
            //    Hot rows shadow cold rows regardless of whether they're in the merge set.
            let mut skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);
            let tombstones_arc = self.segment_mgr.tombstone_set_arc();

            // 3. Get volumes lazily (oldest first — segment_id ascending order).
            //    The merge itself needs only row_ids metadata; materialization goes
            //    through materialize_volume_row_for_read(), so artifact-backed-backed cold
            //    volumes can stay metadata-only and read individual column blocks.
            let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
            // volumes is oldest-first after the reverse inside get_volumes_newest_first_lazy().

            // 4. K-way merge using per-source cursors.
            //    Sources: hot_rows (already sorted) + each volume's row_ids (sorted ascending).
            //    For DESC, we iterate each source from the end.

            // Pre-compute column mappings for each volume.
            struct VolSource {
                row_ids: crate::volume::writer::RowIds,
                cursor: usize,
                mapping: crate::volume::writer::ColumnMapping,
                volume: Arc<FrozenVolume>,
                visible: Option<Arc<Vec<u64>>>,
            }

            let mut vol_sources: Vec<VolSource> = Vec::with_capacity(volumes.len());
            for (_seg_id, cs) in volumes.iter() {
                // Filter out empty or fully-skipped volumes early via zone-map on row_id range.
                let vol = &cs.volume;
                if vol.meta.row_count == 0 {
                    continue;
                }
                let mapping = self.segment_mgr.get_cold_segment_mapping(cs, &schema);
                vol_sources.push(VolSource {
                    row_ids: vol.meta.row_ids.clone(),
                    cursor: if ascending { 0 } else { vol.meta.row_count },
                    mapping,
                    volume: Arc::clone(&cs.volume),
                    visible: cs.visible.clone(),
                });
            }

            let mut result = RowVec::with_capacity(needed.min(1024));
            let mut skipped = 0usize;
            let mut hot_cursor: usize = 0;

            loop {
                if result.len() >= limit {
                    break;
                }

                // Find the source with the next row_id to emit.
                // For ASC: smallest row_id. For DESC: largest row_id.
                let mut best_row_id: Option<i64> = None;
                // 0 = hot, 1..=num_vol = vol_sources[idx-1]
                let mut best_source: usize = usize::MAX;

                // Check hot source
                if hot_cursor < hot_rows.len() {
                    let (rid, _) = &hot_rows[hot_cursor];
                    best_row_id = Some(*rid);
                    best_source = 0;
                }

                // Check each volume source
                for (vi, vs) in vol_sources.iter().enumerate() {
                    let rid = if ascending {
                        if vs.cursor >= vs.row_ids.len() {
                            continue;
                        }
                        vs.row_ids.at(vs.cursor)
                    } else {
                        if vs.cursor == 0 {
                            continue;
                        }
                        vs.row_ids.at(vs.cursor - 1)
                    };

                    let dominated = match best_row_id {
                        None => false,
                        Some(best) => {
                            if ascending {
                                best <= rid
                            } else {
                                best >= rid
                            }
                        }
                    };
                    if !dominated {
                        best_row_id = Some(rid);
                        best_source = vi + 1;
                    }
                }

                // No more rows from any source.
                if best_source == usize::MAX {
                    break;
                }

                if best_source == 0 {
                    // Hot source — row is already materialized and visible.
                    let (rid, row) = hot_rows[hot_cursor].clone();
                    hot_cursor += 1;
                    // Hot rows don't need tombstone/skip checks — they ARE the authoritative version.
                    if skipped < offset {
                        skipped += 1;
                    } else {
                        result.push((rid, row));
                    }
                } else {
                    // Volume source
                    let vs = &mut vol_sources[best_source - 1];
                    let idx = if ascending {
                        let i = vs.cursor;
                        vs.cursor += 1;
                        i
                    } else {
                        vs.cursor -= 1;
                        vs.cursor
                    };

                    let rid = vs.row_ids.at(idx);

                    // Visibility check: inter-volume dedup bitmap
                    if let Some(ref bits) = vs.visible {
                        if (bits[idx >> 6] >> (idx & 63)) & 1 == 0 {
                            continue;
                        }
                    }

                    // Skip if hot shadows this row or if tombstoned
                    if skip.contains(&rid) {
                        continue;
                    }
                    if self.is_row_tombstoned(&tombstones_arc, rid) {
                        continue;
                    }

                    let row =
                        match self.materialize_volume_row_for_read(&vs.volume, idx, &vs.mapping) {
                            Ok(row) => row,
                            Err(_) => return None,
                        };

                    if skipped < offset {
                        skipped += 1;
                    } else {
                        result.push((rid, row));
                    }
                }
            }

            Some(result)
        }

        fn collect_rows_composite_ordered_range(
            &self,
            where_expr: &dyn Expression,
            order_column: &str,
            ascending: bool,
            limit: usize,
            offset: usize,
        ) -> Option<Result<RowVec>> {
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_composite_ordered_range(
                    where_expr,
                    order_column,
                    ascending,
                    limit,
                    offset,
                );
            }
            let plan = self.plan_cold_composite_ordered(where_expr)?;
            if !plan
                .columns
                .last()
                .is_some_and(|column| column.eq_ignore_ascii_case(order_column))
            {
                return None;
            }
            let comparisons = where_expr.collect_comparisons();
            if comparisons
                .iter()
                .any(|(column, _, _)| !plan.covered_columns.contains(*column))
                || !where_expr.collect_null_check_infos().is_empty()
            {
                return None;
            }

            let target = limit.saturating_add(offset);
            let mut cold_rows = match self
                .collect_cold_composite_ordered_rows(&plan, where_expr, ascending, target)
            {
                Ok(Some(rows)) => rows,
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            };
            let hot_rows = match self.hot.collect_rows_composite_ordered_range(
                where_expr,
                order_column,
                ascending,
                target,
                0,
            ) {
                Some(Ok(rows)) => rows,
                Some(Err(error)) => return Some(Err(error)),
                None if self.hot.row_count_hint() == 0 => RowVec::new(),
                None => return None,
            };
            cold_rows.extend(hot_rows);

            let order_idx = *plan
                .column_indices
                .last()
                .expect("ordered plan always has a range column");
            cold_rows.sort_unstable_by(|left, right| {
                let ordering = left
                    .1
                    .get(order_idx)
                    .cmp(&right.1.get(order_idx))
                    .then_with(|| left.0.cmp(&right.0));
                if ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            });
            cold_rows.dedup_by_key(|entry| entry.0);
            Some(Ok(cold_rows.into_iter().skip(offset).take(limit).collect()))
        }

        fn collect_rows_pk_keyset(
            &self,
            start_after: Option<i64>,
            start_from: Option<i64>,
            ascending: bool,
            limit: usize,
        ) -> Option<RowVec> {
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .collect_rows_pk_keyset(start_after, start_from, ascending, limit);
            }
            // Hot PK index doesn't cover cold data — can't use keyset pagination
            None
        }
    };
}

pub(super) use segmented_table_ordered_methods;
