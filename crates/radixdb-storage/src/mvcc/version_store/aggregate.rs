use super::*;

impl VersionStore {
    /// Compute COUNT(*) without materializing any rows
    ///
    /// This is the FASTEST path for `SELECT COUNT(*) FROM table`:
    /// - No row data is loaded
    /// - Only visibility checks are performed
    /// - O(n) time, O(1) memory
    #[inline]
    pub fn count_visible(&self, txn_id: i64) -> usize {
        self.count_visible_rows(txn_id)
    }

    /// Compute SUM(column) without materializing full rows
    ///
    /// OPTIMIZATION: Single-pass approach that combines visibility checking with summing.
    /// Avoids Vec allocation and second iteration over indices.
    /// Returns (sum, count_non_null) for proper NULL handling.
    pub fn sum_column(&self, txn_id: i64, col_idx: usize) -> crate::traits::table::DeferredSum {
        use crate::traits::table::DeferredSum;
        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return DeferredSum::new(),
        };

        // OPTIMIZATION: Separate accumulators to avoid i64->f64 conversion per row.
        // Uses i128 for integer accumulation to prevent overflow (i128 holds sum of
        // 2^63 rows of i64::MAX without overflow).
        let mut sum = DeferredSum::new();

        // Helper to accumulate numeric value
        #[inline(always)]
        fn accumulate_sum(sum: &mut DeferredSum, val: &Value) {
            sum.add_value(val);
        }

        // FAST PATH: If uncommitted_writes is empty, scan arena directly (single pass).
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        //
        // LOCK ORDERING: Check uncommitted_writes BEFORE acquiring arena to maintain
        // consistent ordering with truncate_all (uncommitted_writes → arena).
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();

        {
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_data = arena_guard.data();
            let arena_len = arena_guard.len();

            if uncommitted_empty && arena_len > 0 && !checker.needs_snapshot_isolation(txn_id) {
                // OPTIMIZATION: Cache visibility result for repeated txn_ids
                // When rows are inserted in batches, consecutive rows often have the same txn_id.
                // Caching avoids repeated thread-local access overhead (~27ms per 100K rows).
                let mut last_txn_id: i64 = 0;
                let mut last_visible: bool = false;

                let mut idx = 0;
                while idx < arena_len {
                    let meta = &arena_meta[idx];
                    if meta.txn_id != 0 && meta.deleted_at_txn_id == 0 {
                        // Check visibility with cache
                        let version_txn_id = meta.txn_id;
                        let is_vis = if version_txn_id == last_txn_id {
                            last_visible
                        } else {
                            let vis = checker.is_visible(version_txn_id, txn_id);
                            last_txn_id = version_txn_id;
                            last_visible = vis;
                            vis
                        };

                        if is_vis {
                            if let Some(val) = arena_data[idx].get(col_idx) {
                                accumulate_sum(&mut sum, val);
                            }
                        }
                    }
                    idx += 1;
                }
                return sum;
            }
            // arena_guard dropped here — no need to hold it for slow path
        }

        // SLOW PATH: Full iteration over version chains (single pass)
        let versions = self.versions.read().clone();

        // Cache visibility for slow path too
        let mut last_txn_id: i64 = 0;
        let mut last_visible: bool = false;

        for chain in versions.values() {
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                let is_vis = if version_txn_id == last_txn_id {
                    last_visible
                } else {
                    let vis = checker.is_visible(version_txn_id, txn_id);
                    last_txn_id = version_txn_id;
                    last_visible = vis;
                    vis
                };

                if is_vis {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        // This version is visible and not deleted - accumulate its value
                        if let Some(val) = e.version.data.get(col_idx) {
                            accumulate_sum(&mut sum, val);
                        }
                    }
                    break; // Always break on first visible version
                }

                current = e.prev.as_deref();
            }
        }

        sum
    }

    /// Compute MIN(column) without materializing full rows
    ///
    /// OPTIMIZATION: Single-pass approach that combines visibility checking with min computation.
    pub fn min_column(&self, txn_id: i64, col_idx: usize) -> Option<Value> {
        let checker = self.visibility_checker.as_ref()?;

        let mut min_val: Option<Value> = None;

        // Helper to update min value
        #[inline(always)]
        fn update_min(min_val: &mut Option<Value>, val: &Value) {
            if !val.is_null() {
                match min_val {
                    None => *min_val = Some(val.clone()),
                    Some(ref current) => {
                        if let Ok(std::cmp::Ordering::Less) = val.compare(current) {
                            *min_val = Some(val.clone());
                        }
                    }
                }
            }
        }

        // FAST PATH: If uncommitted_writes is empty, scan arena directly (single pass).
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        //
        // LOCK ORDERING: Check uncommitted_writes BEFORE acquiring arena to maintain
        // consistent ordering with truncate_all (uncommitted_writes → arena).
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();

        {
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_data = arena_guard.data();
            let arena_len = arena_guard.len();

            if uncommitted_empty && arena_len > 0 && !checker.needs_snapshot_isolation(txn_id) {
                // OPTIMIZATION: Cache visibility result for repeated txn_ids
                let mut last_txn_id: i64 = 0;
                let mut last_visible: bool = false;

                let mut idx = 0;
                while idx < arena_len {
                    let meta = &arena_meta[idx];
                    if meta.txn_id != 0 && meta.deleted_at_txn_id == 0 {
                        let version_txn_id = meta.txn_id;
                        let is_vis = if version_txn_id == last_txn_id {
                            last_visible
                        } else {
                            let vis = checker.is_visible(version_txn_id, txn_id);
                            last_txn_id = version_txn_id;
                            last_visible = vis;
                            vis
                        };

                        if is_vis {
                            if let Some(val) = arena_data[idx].get(col_idx) {
                                update_min(&mut min_val, val);
                            }
                        }
                    }
                    idx += 1;
                }
                return min_val;
            }
            // arena_guard dropped here — no need to hold it for slow path
        }

        // SLOW PATH: Full iteration over version chains (single pass)
        let versions = self.versions.read().clone();

        // Cache visibility for slow path too
        let mut last_txn_id: i64 = 0;
        let mut last_visible: bool = false;

        for chain in versions.values() {
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                let is_vis = if version_txn_id == last_txn_id {
                    last_visible
                } else {
                    let vis = checker.is_visible(version_txn_id, txn_id);
                    last_txn_id = version_txn_id;
                    last_visible = vis;
                    vis
                };

                if is_vis {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        if let Some(val) = e.version.data.get(col_idx) {
                            update_min(&mut min_val, val);
                        }
                    }
                    break; // Always break on first visible version
                }

                current = e.prev.as_deref();
            }
        }

        min_val
    }

    /// Compute MAX(column) without materializing full rows
    ///
    /// OPTIMIZATION: Single-pass approach that combines visibility checking with max computation.
    pub fn max_column(&self, txn_id: i64, col_idx: usize) -> Option<Value> {
        let checker = self.visibility_checker.as_ref()?;

        let mut max_val: Option<Value> = None;

        // Helper to update max value
        #[inline(always)]
        fn update_max(max_val: &mut Option<Value>, val: &Value) {
            if !val.is_null() {
                match max_val {
                    None => *max_val = Some(val.clone()),
                    Some(ref current) => {
                        if let Ok(std::cmp::Ordering::Greater) = val.compare(current) {
                            *max_val = Some(val.clone());
                        }
                    }
                }
            }
        }

        // FAST PATH: If uncommitted_writes is empty, scan arena directly (single pass).
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        //
        // LOCK ORDERING: Check uncommitted_writes BEFORE acquiring arena to maintain
        // consistent ordering with truncate_all (uncommitted_writes → arena).
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();

        {
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_data = arena_guard.data();
            let arena_len = arena_guard.len();

            if uncommitted_empty && arena_len > 0 && !checker.needs_snapshot_isolation(txn_id) {
                // OPTIMIZATION: Cache visibility result for repeated txn_ids
                let mut last_txn_id: i64 = 0;
                let mut last_visible: bool = false;

                let mut idx = 0;
                while idx < arena_len {
                    let meta = &arena_meta[idx];
                    if meta.txn_id != 0 && meta.deleted_at_txn_id == 0 {
                        let version_txn_id = meta.txn_id;
                        let is_vis = if version_txn_id == last_txn_id {
                            last_visible
                        } else {
                            let vis = checker.is_visible(version_txn_id, txn_id);
                            last_txn_id = version_txn_id;
                            last_visible = vis;
                            vis
                        };

                        if is_vis {
                            if let Some(val) = arena_data[idx].get(col_idx) {
                                update_max(&mut max_val, val);
                            }
                        }
                    }
                    idx += 1;
                }
                return max_val;
            }
            // arena_guard dropped here — no need to hold it for slow path
        }

        // SLOW PATH: Full iteration over version chains (single pass)
        let versions = self.versions.read().clone();

        // Cache visibility for slow path too
        let mut last_txn_id: i64 = 0;
        let mut last_visible: bool = false;

        for chain in versions.values() {
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                let is_vis = if version_txn_id == last_txn_id {
                    last_visible
                } else {
                    let vis = checker.is_visible(version_txn_id, txn_id);
                    last_txn_id = version_txn_id;
                    last_visible = vis;
                    vis
                };

                if is_vis {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        if let Some(val) = e.version.data.get(col_idx) {
                            update_max(&mut max_val, val);
                        }
                    }
                    break; // Always break on first visible version
                }

                current = e.prev.as_deref();
            }
        }

        max_val
    }

    /// Compute multiple column aggregates in a single pass
    ///
    /// This is the most efficient path for queries like:
    /// `SELECT SUM(a), AVG(b), MIN(c), MAX(d) FROM table`
    ///
    /// Returns aggregates in the order requested.
    pub fn compute_aggregates(
        &self,
        txn_id: i64,
        aggregates: &[(AggregateOp, usize)], // (operation, column_index)
    ) -> Vec<AggregateResult> {
        let empty_result = || {
            aggregates
                .iter()
                .map(|(op, _)| match op {
                    AggregateOp::Count | AggregateOp::CountStar => AggregateResult::Count(0),
                    AggregateOp::Sum => AggregateResult::Sum(0.0, 0),
                    AggregateOp::Min => AggregateResult::Min(None),
                    AggregateOp::Max => AggregateResult::Max(None),
                    AggregateOp::Avg => AggregateResult::Avg(0.0, 0),
                })
                .collect()
        };

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return empty_result(),
        };

        // Initialize accumulators
        let mut results: Vec<AggregateAccumulator> = aggregates
            .iter()
            .map(|(op, _)| match op {
                AggregateOp::Count | AggregateOp::CountStar => AggregateAccumulator::Count(0),
                AggregateOp::Sum => AggregateAccumulator::Sum(0, 0.0, 0),
                AggregateOp::Min => AggregateAccumulator::Min(None),
                AggregateOp::Max => AggregateAccumulator::Max(None),
                AggregateOp::Avg => AggregateAccumulator::Avg(0, 0.0, 0),
            })
            .collect();

        // Helper to update accumulator with a value
        fn update_accumulator(acc: &mut AggregateAccumulator, op: &AggregateOp, val: &Value) {
            match (acc, op) {
                (AggregateAccumulator::Count(c), AggregateOp::Count) if !val.is_null() => {
                    *c += 1;
                }
                (AggregateAccumulator::Count(c), AggregateOp::CountStar) => {
                    *c += 1;
                }
                (AggregateAccumulator::Sum(int_sum, float_sum, cnt), AggregateOp::Sum) => match val
                {
                    Value::Integer(i) => {
                        *int_sum += *i as i128;
                        *cnt += 1;
                    }
                    Value::Float(f) => {
                        *float_sum += *f;
                        *cnt += 1;
                    }
                    _ => {}
                },
                (AggregateAccumulator::Min(min), AggregateOp::Min) if !val.is_null() => match min {
                    None => *min = Some(val.clone()),
                    Some(current) => {
                        if let Ok(std::cmp::Ordering::Less) = val.compare(current) {
                            *min = Some(val.clone());
                        }
                    }
                },
                (AggregateAccumulator::Max(max), AggregateOp::Max) if !val.is_null() => match max {
                    None => *max = Some(val.clone()),
                    Some(current) => {
                        if let Ok(std::cmp::Ordering::Greater) = val.compare(current) {
                            *max = Some(val.clone());
                        }
                    }
                },
                (AggregateAccumulator::Avg(int_sum, float_sum, cnt), AggregateOp::Avg) => match val
                {
                    Value::Integer(i) => {
                        *int_sum += *i as i128;
                        *cnt += 1;
                    }
                    Value::Float(f) => {
                        *float_sum += *f;
                        *cnt += 1;
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        // Helper macro: accumulate values from a row-like source by column index
        macro_rules! accumulate_from {
            ($src:expr, $results:expr, $aggregates:expr) => {
                for (i, (op, col_idx)) in $aggregates.iter().enumerate() {
                    if let Some(val) = $src.get(*col_idx) {
                        update_accumulator(&mut $results[i], op, val);
                    }
                }
            };
        }

        // FAST PATH: Scan arena directly when no uncommitted writes.
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();
        if uncommitted_empty && !checker.needs_snapshot_isolation(txn_id) {
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_data = arena_guard.data();
            let arena_len = arena_guard.len();

            if arena_len > 0 {
                for (idx, meta) in arena_meta.iter().enumerate() {
                    if meta.txn_id != 0
                        && meta.deleted_at_txn_id == 0
                        && checker.is_visible(meta.txn_id, txn_id)
                    {
                        if let Some(arc_row) = arena_data.get(idx) {
                            accumulate_from!(arc_row, results, aggregates);
                        }
                    }
                }

                return results
                    .into_iter()
                    .map(|acc| match acc {
                        AggregateAccumulator::Count(c) => AggregateResult::Count(c),
                        AggregateAccumulator::Sum(is, fs, c) => {
                            AggregateResult::Sum(is as f64 + fs, c)
                        }
                        AggregateAccumulator::Min(v) => AggregateResult::Min(v),
                        AggregateAccumulator::Max(v) => AggregateResult::Max(v),
                        AggregateAccumulator::Avg(is, fs, c) => {
                            AggregateResult::Avg(is as f64 + fs, c)
                        }
                    })
                    .collect();
            }
        }

        // SLOW PATH: CowBTree iteration with arena data retrieval
        let versions = self.versions.read().clone();
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        for chain in versions.values() {
            let mut current: Option<&VersionChainEntry> = Some(chain);
            while let Some(e) = current {
                if checker.is_visible(e.version.txn_id, txn_id) {
                    if e.version.deleted_at_txn_id == 0
                        || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                    {
                        if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                            if let Some(arc_row) = arena_data.get(idx) {
                                accumulate_from!(arc_row, results, aggregates);
                            } else {
                                accumulate_from!(e.version.data, results, aggregates);
                            }
                        } else {
                            accumulate_from!(e.version.data, results, aggregates);
                        }
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        // Convert accumulators to results
        results
            .into_iter()
            .map(|acc| match acc {
                AggregateAccumulator::Count(c) => AggregateResult::Count(c),
                AggregateAccumulator::Sum(is, fs, c) => AggregateResult::Sum(is as f64 + fs, c),
                AggregateAccumulator::Min(v) => AggregateResult::Min(v),
                AggregateAccumulator::Max(v) => AggregateResult::Max(v),
                AggregateAccumulator::Avg(is, fs, c) => AggregateResult::Avg(is as f64 + fs, c),
            })
            .collect()
    }
}
