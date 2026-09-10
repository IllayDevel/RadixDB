macro_rules! segmented_table_read_methods {
    () => {
        // =========================================================================
        // Read operations — merge segments + hot buffer
        // =========================================================================

        fn visit_visible_rows(
            &self,
            visitor: &mut dyn FnMut(i64, Row) -> Result<()>,
        ) -> Result<()> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();

            if !self.segment_mgr.has_segments() {
                return self.hot.visit_visible_rows(visitor);
            }

            let mut skip = FxHashSet::with_capacity_and_hasher(1024, Default::default());
            self.hot.collect_shadow_row_ids_into(&mut skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);
            let projection: Vec<usize> = (0..self.hot.schema().columns.len()).collect();
            let mut cold_scanners = self.create_segment_scanners_filtered_in_row_id_ranges(
                &projection,
                None,
                skip,
                None,
                true,
            );
            for scanner in &mut cold_scanners {
                while scanner.next() {
                    let (row_id, row) = scanner.take_row_with_id()?;
                    visitor(row_id, row)?;
                }
                if let Some(error) = scanner.err().cloned() {
                    let _ = scanner.close();
                    return Err(error);
                }
                scanner.close()?;
            }
            self.hot.visit_visible_rows(visitor)
        }

        fn scan(
            &self,
            column_indices: &[usize],
            where_expr: Option<&dyn Expression>,
        ) -> Result<Box<dyn Scanner>> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let row_id_ranges = self.zone_map_row_id_ranges(where_expr);

            #[cfg(any(test, feature = "test-failpoints"))]
            if !self.segment_mgr.has_segments() {
                crate::test_failpoints::record_execution_path(3);
            } else if self.hot.row_count_hint() == 0 {
                crate::test_failpoints::record_execution_path(4);
            } else {
                crate::test_failpoints::record_execution_path(5);
            }

            if !self.segment_mgr.has_segments() {
                return match row_id_ranges.as_ref() {
                    Some(ranges) => self.hot.scan_with_row_id_ranges(
                        column_indices,
                        where_expr,
                        ranges.as_slice(),
                    ),
                    None => self.hot.scan(column_indices, where_expr),
                };
            }

            #[cfg(any(test, feature = "test-failpoints"))]
            let indexes_allowed = !crate::test_failpoints::decline_indexes();
            #[cfg(not(any(test, feature = "test-failpoints")))]
            let indexes_allowed = true;
            if let Some(expr) = where_expr.filter(|_| indexes_allowed) {
                if let Some(scanner) = self.scan_cold_multi_index_or(column_indices, expr, false) {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_exact_set(column_indices, expr, false) {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_composite_ordered(column_indices, expr, false)
                {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_composite_exact(column_indices, expr, false) {
                    return scanner;
                }
            }

            let mut skip = FxHashSet::with_capacity_and_hasher(1024, Default::default());
            self.hot.collect_shadow_row_ids_into(&mut skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);

            let mut hot_scanner = match row_id_ranges.as_ref() {
                Some(ranges) => self.hot.scan_with_row_id_ranges(
                    column_indices,
                    where_expr,
                    ranges.as_slice(),
                )?,
                None => self.hot.scan(column_indices, where_expr)?,
            };

            // Create lazy cold scanners with the skip set (no eager collection).
            // This avoids O(total_cold_rows) memory allocation that was making
            // ALL queries slow during checkpoint.
            let cold_scanners = self.create_segment_scanners_filtered_in_row_id_ranges(
                column_indices,
                where_expr,
                skip,
                row_id_ranges,
                true,
            );

            // Chain: cold scanners (lazy, streamed) + hot rows (already collected).
            // Wrap the hot snapshot in the normal MVCC scanner so the mixed path
            // uses the same projected scan contract and row-id boundary as hot-only
            // scans. The skip set above still uses original row IDs from the
            // unfiltered hot snapshot.
            let mut sources: Vec<Box<dyn Scanner>> = cold_scanners;
            if hot_scanner.estimated_count() == Some(0) {
                hot_scanner.close()?;
            } else {
                let schema = self.typed_adapter_schema(column_indices, true);
                sources.push(match schema {
                    Some(schema) => Box::new(crate::volume::scanner::RowTypedScanner::new(
                        hot_scanner,
                        schema,
                    )),
                    None => hot_scanner,
                });
            }

            Ok(Box::new(crate::volume::scanner::MergingScanner::new(
                sources,
            )))
        }

        fn scan_exact_projection(
            &self,
            column_indices: &[usize],
            where_expr: Option<&dyn Expression>,
        ) -> Result<Box<dyn Scanner>> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            self.scan_exact_projection_unfenced(column_indices, where_expr)
        }

        fn scan_exact_projection_unfenced(
            &self,
            column_indices: &[usize],
            where_expr: Option<&dyn Expression>,
        ) -> Result<Box<dyn Scanner>> {
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let row_id_ranges = self.zone_map_row_id_ranges(where_expr);

            #[cfg(any(test, feature = "test-failpoints"))]
            if !self.segment_mgr.has_segments() {
                crate::test_failpoints::record_execution_path(3);
            } else if self.hot.row_count_hint() == 0 {
                crate::test_failpoints::record_execution_path(4);
            } else {
                crate::test_failpoints::record_execution_path(5);
            }

            if !self.segment_mgr.has_segments() {
                return match row_id_ranges.as_ref() {
                    Some(ranges) => self.hot.scan_exact_projection_with_row_id_ranges(
                        column_indices,
                        where_expr,
                        ranges.as_slice(),
                    ),
                    None => self.hot.scan_exact_projection(column_indices, where_expr),
                };
            }

            #[cfg(any(test, feature = "test-failpoints"))]
            let indexes_allowed = !crate::test_failpoints::decline_indexes();
            #[cfg(not(any(test, feature = "test-failpoints")))]
            let indexes_allowed = true;
            if let Some(expr) = where_expr.filter(|_| indexes_allowed) {
                if let Some(scanner) = self.scan_cold_multi_index_or(column_indices, expr, true) {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_exact_set(column_indices, expr, true) {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_composite_ordered(column_indices, expr, true)
                {
                    return scanner;
                }
                if let Some(scanner) = self.scan_cold_composite_exact(column_indices, expr, true) {
                    return scanner;
                }
            }

            let mut skip = FxHashSet::with_capacity_and_hasher(1024, Default::default());
            self.hot.collect_shadow_row_ids_into(&mut skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);
            let mut hot_scanner = match row_id_ranges.as_ref() {
                Some(ranges) => self.hot.scan_exact_projection_with_row_id_ranges(
                    column_indices,
                    where_expr,
                    ranges.as_slice(),
                )?,
                None => self.hot.scan_exact_projection(column_indices, where_expr)?,
            };

            let cold_scanners = self.create_segment_scanners_filtered_in_row_id_ranges(
                column_indices,
                where_expr,
                skip,
                row_id_ranges,
                false,
            );

            let mut sources: Vec<Box<dyn Scanner>> = cold_scanners;
            if hot_scanner.estimated_count() == Some(0) {
                hot_scanner.close()?;
            } else {
                let schema = self.typed_adapter_schema(column_indices, false);
                sources.push(match schema {
                    Some(schema) => Box::new(crate::volume::scanner::RowTypedScanner::new(
                        hot_scanner,
                        schema,
                    )),
                    None => hot_scanner,
                });
            }

            Ok(Box::new(crate::volume::scanner::MergingScanner::new(
                sources,
            )))
        }

        fn collect_all_rows(&self, where_expr: Option<&dyn Expression>) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_all_rows(where_expr);
            }

            let mut skip = FxHashSet::with_capacity_and_hasher(1024, Default::default());
            self.hot.collect_shadow_row_ids_into(&mut skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);
            let hot_rows = self.hot.collect_all_rows(where_expr)?;

            let mut all_rows = self.collect_cold_rows(where_expr, skip)?;
            for entry in hot_rows {
                all_rows.push(entry);
            }
            Ok(all_rows)
        }

        fn collect_all_rows_unsorted(&self) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_all_rows_unsorted();
            }

            let hot_rows = self.hot.collect_all_rows_unsorted()?;

            let mut skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(hot_rows.len(), Default::default());
            for &(id, _) in &hot_rows {
                skip.insert(id);
            }
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut skip);

            let mut all_rows = self.collect_cold_rows(None, skip)?;
            for entry in hot_rows {
                all_rows.push(entry);
            }
            Ok(all_rows)
        }

        fn collect_rows_by_ids(&self, row_ids: &[i64]) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_by_ids(row_ids);
            }
            self.collect_rows_by_ids_grouped_unfenced(row_ids, None)
        }

        fn collect_rows_by_ids_projected(
            &self,
            row_ids: &[i64],
            column_indices: &[usize],
        ) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .collect_rows_by_ids_projected(row_ids, column_indices);
            }
            self.collect_rows_by_ids_grouped_unfenced(row_ids, Some(column_indices))
        }

        fn fetch_rows_by_ids(&self, row_ids: &[i64], filter: &dyn Expression) -> Result<RowVec> {
            let candidates = self.collect_rows_by_ids(row_ids)?;
            let mut results = RowVec::with_capacity(candidates.len());
            for (row_id, row) in candidates {
                if filter.evaluate(&row)? {
                    results.push((row_id, row));
                }
            }
            Ok(results)
        }

        fn fetch_rows_by_ids_into(
            &self,
            row_ids: &[i64],
            filter: &dyn Expression,
            buffer: &mut RowVec,
        ) -> Result<()> {
            buffer.extend(self.fetch_rows_by_ids(row_ids, filter)?);
            Ok(())
        }

        // =========================================================================
        // LIMIT pushdowns
        // =========================================================================

        fn collect_rows_with_limit(
            &self,
            where_expr: Option<&dyn Expression>,
            limit: usize,
            offset: usize,
        ) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_with_limit(where_expr, limit, offset);
            }

            let target = limit + offset;
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            // Cold rows come first for the ordered LIMIT path. Use the same lazy
            // scanner path as full scans so artifact-backed-backed cold volumes remain
            // metadata-only and read only required row-group blocks.
            let mut cold_rows = RowVec::with_capacity(target.min(1024));
            let column_indices: Vec<usize> = (0..self.hot.schema().columns.len()).collect();
            let mut scanners =
                self.create_segment_scanners_filtered(&column_indices, where_expr, hot_skip);
            'cold: for scanner in scanners.iter_mut() {
                while scanner.next() {
                    cold_rows.push(scanner.take_row_with_id()?);
                    if cold_rows.len() >= target {
                        break 'cold;
                    }
                }
                if let Some(err) = scanner.err() {
                    return Err(err.clone());
                }
                scanner.close()?;
            }

            // If cold didn't fill the target, materialize hot rows for the
            // remainder only.
            if cold_rows.len() < target {
                let remaining = target - cold_rows.len();
                let hot_rows = self.hot.collect_rows_with_limit(where_expr, remaining, 0)?;
                cold_rows.extend(hot_rows);
            }

            Ok(cold_rows.into_iter().skip(offset).take(limit).collect())
        }

        fn collect_rows_with_limit_unordered(
            &self,
            where_expr: Option<&dyn Expression>,
            limit: usize,
            offset: usize,
        ) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .collect_rows_with_limit_unordered(where_expr, limit, offset);
            }

            let target = limit + offset;

            // Unordered: hot rows first with early termination.
            // Only scan up to `target` hot rows — avoids O(hot_rows) for small LIMITs.
            let hot_rows = self
                .hot
                .collect_rows_with_limit_unordered(where_expr, target, 0)?;
            if hot_rows.len() >= target {
                return Ok(hot_rows.into_iter().skip(offset).take(limit).collect());
            }

            // Need cold rows. Build hot_skip and scan with early termination.
            // Optimization: avoid materializing rows that fall within the offset.
            // Hot rows already collected cover some of the offset+limit range.
            // For the cold scan, track a skip counter to avoid get_row() for offset rows.
            let hot_count = hot_rows.len();
            let cold_skip = offset.saturating_sub(hot_count);
            let mut result: RowVec = if hot_count > offset {
                hot_rows.into_iter().skip(offset).collect()
            } else {
                RowVec::new()
            };
            let remaining = limit.saturating_sub(result.len()) + cold_skip;

            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut collected = 0usize;
            let mut cold_skipped = 0usize;
            let column_indices: Vec<usize> = (0..self.hot.schema().columns.len()).collect();
            let mut scanners =
                self.create_segment_scanners_filtered(&column_indices, where_expr, hot_skip);

            'outer: for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let row = scanner.take_row_with_id()?;
                    if cold_skipped < cold_skip {
                        cold_skipped += 1;
                    } else {
                        result.push(row);
                    }
                    collected += 1;
                    if collected >= remaining {
                        break 'outer;
                    }
                }
                if let Some(err) = scanner.err() {
                    return Err(err.clone());
                }
                scanner.close()?;
            }

            Ok(result)
        }

        fn collect_rows_with_limit_unordered_projected(
            &self,
            column_indices: &[usize],
            where_expr: Option<&dyn Expression>,
            limit: usize,
            offset: usize,
        ) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_with_limit_unordered_projected(
                    column_indices,
                    where_expr,
                    limit,
                    offset,
                );
            }

            let target = limit.saturating_add(offset);

            // Keep the same unordered contract as collect_rows_with_limit_unordered:
            // hot rows first with early termination, then cold segments for the
            // remaining window. The difference is that both sources return projected
            // rows at the table boundary.
            let hot_rows = self.hot.collect_rows_with_limit_unordered_projected(
                column_indices,
                where_expr,
                target,
                0,
            )?;
            if hot_rows.len() >= target {
                return Ok(hot_rows.into_iter().skip(offset).take(limit).collect());
            }

            let hot_count = hot_rows.len();
            let cold_skip = offset.saturating_sub(hot_count);
            let mut result: RowVec = if hot_count > offset {
                hot_rows.into_iter().skip(offset).collect()
            } else {
                RowVec::new()
            };
            let remaining = limit.saturating_sub(result.len()) + cold_skip;

            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut collected = 0usize;
            let mut cold_skipped = 0usize;
            let mut scanners =
                self.create_segment_scanners_filtered(column_indices, where_expr, hot_skip);

            'outer: for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let row = scanner.take_row_with_id()?;
                    if cold_skipped < cold_skip {
                        cold_skipped += 1;
                    } else {
                        result.push(row);
                    }
                    collected += 1;
                    if collected >= remaining {
                        break 'outer;
                    }
                }
                if let Some(err) = scanner.err() {
                    return Err(err.clone());
                }
                scanner.close()?;
            }

            Ok(result)
        }

        fn collect_rows_with_limit_unordered_exact_projected(
            &self,
            column_indices: &[usize],
            where_expr: Option<&dyn Expression>,
            limit: usize,
            offset: usize,
        ) -> Result<RowVec> {
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_with_limit_unordered_exact_projected(
                    column_indices,
                    where_expr,
                    limit,
                    offset,
                );
            }

            let target = limit.saturating_add(offset);

            let hot_rows = self.hot.collect_rows_with_limit_unordered_exact_projected(
                column_indices,
                where_expr,
                target,
                0,
            )?;
            if hot_rows.len() >= target {
                return Ok(hot_rows.into_iter().skip(offset).take(limit).collect());
            }

            let hot_count = hot_rows.len();
            let cold_skip = offset.saturating_sub(hot_count);
            let mut result: RowVec = if hot_count > offset {
                hot_rows.into_iter().skip(offset).collect()
            } else {
                RowVec::new()
            };
            let remaining = limit.saturating_sub(result.len()) + cold_skip;

            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            self.hot.collect_hot_row_ids_into(&mut hot_skip);
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut collected = 0usize;
            let mut cold_skipped = 0usize;
            let mut scanners = self.create_segment_scanners_filtered_exact_projection(
                column_indices,
                where_expr,
                hot_skip,
            );

            'outer: for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let row = scanner.take_row_with_id()?;
                    if cold_skipped < cold_skip {
                        cold_skipped += 1;
                    } else {
                        result.push(row);
                    }
                    collected += 1;
                    if collected >= remaining {
                        break 'outer;
                    }
                }
                if let Some(err) = scanner.err() {
                    return Err(err.clone());
                }
                scanner.close()?;
            }

            Ok(result)
        }

        fn collect_rows_sorted_with_limit(
            &self,
            sort_col_idx: usize,
            ascending: bool,
            limit: usize,
            offset: usize,
        ) -> Result<Vec<Row>> {
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_sorted_with_limit(
                    sort_col_idx,
                    ascending,
                    limit,
                    offset,
                );
            }
            // Collect all merged rows, sort, take limit
            let mut rows = self.collect_all_rows(None)?;
            rows.sort_by(|(_, a), (_, b)| {
                let va = a.get(sort_col_idx);
                let vb = b.get(sort_col_idx);
                let cmp = match (va, vb) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (Some(va), Some(vb)) => va.compare(vb).unwrap_or(std::cmp::Ordering::Equal),
                };
                if ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
            });
            Ok(rows
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, row)| row)
                .collect())
        }

        fn has_row_id(&self, row_id: i64) -> bool {
            if self.hot.has_row_id(row_id) {
                return true;
            }
            if !self.segment_mgr.has_segments() {
                return false;
            }
            // Snapshot-aware: row_exists() checks all tombstones unconditionally,
            // but a snapshot txn should still see rows tombstoned after its begin_seq.
            if self.snapshot_seq.is_some() {
                let ts = self.segment_mgr.tombstone_set_arc();
                if self.is_row_tombstoned(&ts, row_id) {
                    return false;
                }
                return self.segment_mgr.is_row_id_in_volume(row_id);
            }
            self.segment_mgr.row_exists(row_id)
        }

        fn membership_fence(&self) -> Option<Arc<parking_lot::RwLock<()>>> {
            self.hot.membership_fence()
        }

        fn try_claim_row(&self, row_id: i64) -> Result<()> {
            self.hot.try_claim_row(row_id)
        }

        fn try_claim_rows(&self, row_ids: &[i64]) -> Result<()> {
            self.hot.try_claim_rows(row_ids)
        }

        fn try_claim_rows_for_delete(&self, row_ids: &[i64]) -> Result<()> {
            self.hot.try_claim_rows_for_delete(row_ids)
        }

        fn probe_visible_row_ids_unfenced(
            &self,
            row_ids: &[i64],
            matches: &mut [bool],
        ) -> Result<usize> {
            // The Table default acquired the hot VersionStore's publication fence
            // before entering this hook. Keep that one shared guard across both
            // sources: a cold-row UPDATE publishes its hot replacement and its
            // cold tombstone as one logical membership transition.
            // Seal moves rows from hot to cold. Hold the shared fence across both
            // snapshots so a row cannot disappear between the two membership
            // checks or be counted through an inconsistent topology.
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let hot_hits = self.hot.probe_visible_row_ids_unfenced(row_ids, matches)?;
            if hot_hits == row_ids.len() || !self.segment_mgr.has_segments() {
                return Ok(hot_hits);
            }

            let cold_snapshot = self.segment_mgr.cold_snapshot();
            Ok(self.segment_mgr.merge_visible_row_ids_from_cold_snapshot(
                &cold_snapshot,
                self.txn_id(),
                self.snapshot_seq,
                row_ids,
                matches,
            ))
        }

        fn collect_hot_row_ids_into(&self, dest: &mut FxHashSet<i64>) {
            self.hot.collect_hot_row_ids_into(dest);
        }

        fn count_visible_integer_primary_key_range(
            &self,
            range: &IntegerPrimaryKeyRange,
        ) -> Option<Result<usize>> {
            crate::instrumentation::record_metadata_pk_count_attempt();
            // An impossible normalized range has an exact answer without touching
            // hot/cold topology.
            if range.is_empty() {
                crate::instrumentation::record_metadata_pk_count_applied(0, 0);
                return Some(Ok(0));
            }

            // Transaction-local INSERT/UPDATE/DELETE versions are merged by the
            // regular scanner, but are not a complete part of the cold row-id
            // metadata candidate set below.  Applying the metadata operator here
            // can therefore return a false zero for a row just inserted by the
            // current transaction.  Preserve read-your-writes by declining until
            // the transaction has no local changes.
            // Hot transaction-local versions are not represented completely by
            // `get_active_row_ids()`, so INSERT/UPDATE must decline. Pending cold
            // tombstones are different: the metadata membership probe below is
            // explicitly transaction-aware and can account for them without a
            // payload read. Using the composite SegmentedTable flag here disabled
            // that proven DELETE path as well.
            if self.hot.has_local_changes() {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Unsupported,
                );
                return None;
            }

            // Snapshot visibility and a concurrent seal have contracts broader
            // than the current row-id metadata proof.  Keep the fast path honest:
            // the regular scanner is the correctness fallback for both cases.
            if self.snapshot_seq.is_some() {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Snapshot,
                );
                return None;
            }
            if self.segment_mgr.seal_overlap() > 0 {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::SealOverlap,
                );
                return None;
            }
            if !self.segment_mgr.has_segments() {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Unsupported,
                );
                return None;
            }

            let schema = self.hot.schema();
            let Some(pk_idx) = schema.pk_column_index() else {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Unsupported,
                );
                return None;
            };
            let Some(pk_column) = schema.columns.get(pk_idx) else {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Unsupported,
                );
                return None;
            };
            if pk_column.data_type != DataType::Integer {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::Unsupported,
                );
                return None;
            }

            // Preserve the same membership publication boundary as metadata probes:
            // a cold-row update publishes its hot replacement and cold tombstone as
            // one logical change.  Holding both guards also prevents a seal from
            // moving a candidate between hot and cold while its set is built.
            let publication_fence = self.hot.membership_fence();
            let _publication_guard = publication_fence.as_ref().map(|fence| fence.read());
            let _seal_guard = self.segment_mgr.acquire_seal_read();

            let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
            let mut candidate_ids = FxHashSet::with_capacity_and_hasher(
                METADATA_PK_COUNT_CANDIDATE_LIMIT.min(1024),
                Default::default(),
            );

            for (_segment_id, cold) in volumes.iter() {
                let mapping = self.segment_mgr.get_cold_segment_mapping(cold, schema);
                // `row_id == current INTEGER PK` is only valid when this volume
                // physically contains that PK.  Schema-evolution defaults must
                // use the regular path rather than inventing an answer.
                if !matches!(
                    mapping.sources.get(pk_idx),
                    Some(crate::volume::writer::ColSource::Volume(_))
                ) {
                    crate::instrumentation::record_metadata_pk_count_fallback(
                        crate::instrumentation::MetadataPkCountFallback::Unsupported,
                    );
                    return None;
                }

                let row_ids = &cold.volume.meta.row_ids;
                let (start, end) = range.bounds_with(row_ids.len(), |index| row_ids.at(index));
                crate::instrumentation::record_metadata_pk_count_interval();
                if end.saturating_sub(start)
                    > METADATA_PK_COUNT_CANDIDATE_LIMIT.saturating_sub(candidate_ids.len())
                {
                    crate::instrumentation::record_metadata_pk_count_fallback(
                        crate::instrumentation::MetadataPkCountFallback::CandidateLimit,
                    );
                    return None;
                }
                for (row_index, row_id) in row_ids.iter().enumerate().take(end).skip(start) {
                    if cold.is_visible(row_index) {
                        candidate_ids.insert(row_id);
                        if candidate_ids.len() > METADATA_PK_COUNT_CANDIDATE_LIMIT {
                            crate::instrumentation::record_metadata_pk_count_fallback(
                                crate::instrumentation::MetadataPkCountFallback::CandidateLimit,
                            );
                            return None;
                        }
                    }
                }
            }

            // A small hot tail may contain inserts absent from all cold metadata,
            // or replacements which must shadow an older cold row.  Avoid scanning
            // an arbitrarily large hot store; broad hot ranges retain the generic
            // filtered-aggregate path.
            if self.hot.row_count_hint()
                > METADATA_PK_COUNT_CANDIDATE_LIMIT.saturating_sub(candidate_ids.len())
            {
                crate::instrumentation::record_metadata_pk_count_fallback(
                    crate::instrumentation::MetadataPkCountFallback::CandidateLimit,
                );
                return None;
            }
            let mut hot_candidate_count = 0usize;
            for row_id in self.hot.get_active_row_ids() {
                if range.contains(row_id) {
                    hot_candidate_count += 1;
                    candidate_ids.insert(row_id);
                    if candidate_ids.len() > METADATA_PK_COUNT_CANDIDATE_LIMIT {
                        crate::instrumentation::record_metadata_pk_count_hot_candidates(
                            hot_candidate_count,
                        );
                        crate::instrumentation::record_metadata_pk_count_fallback(
                            crate::instrumentation::MetadataPkCountFallback::CandidateLimit,
                        );
                        return None;
                    }
                }
            }
            crate::instrumentation::record_metadata_pk_count_hot_candidates(hot_candidate_count);

            if candidate_ids.is_empty() {
                crate::instrumentation::record_metadata_pk_count_applied(0, 0);
                return Some(Ok(0));
            }

            // Reuse the established metadata membership probe, but keep the
            // publication/seal guards held across candidate discovery and probing.
            // It reads MVCC/tombstone metadata only; no artifact-backed payload block is opened.
            let candidate_ids: Vec<i64> = candidate_ids.into_iter().collect();
            let candidate_count = candidate_ids.len();
            let mut matches = vec![false; candidate_ids.len()];
            let result = (|| {
                let hot_hits = self
                    .hot
                    .probe_visible_row_ids_unfenced(&candidate_ids, &mut matches)?;
                if hot_hits == candidate_ids.len() {
                    return Ok(hot_hits);
                }
                let cold_snapshot = self.segment_mgr.cold_snapshot();
                Ok(self.segment_mgr.merge_visible_row_ids_from_cold_snapshot(
                    &cold_snapshot,
                    self.txn_id(),
                    self.snapshot_seq,
                    &candidate_ids,
                    &mut matches,
                ))
            })();
            match result {
                Ok(visible_count) => {
                    crate::instrumentation::record_metadata_pk_count_applied(
                        candidate_count,
                        visible_count,
                    );
                    Some(Ok(visible_count))
                }
                Err(error) => Some(Err(error)),
            }
        }
    };
}

pub(super) use segmented_table_read_methods;
