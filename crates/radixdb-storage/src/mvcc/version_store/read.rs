use super::*;

impl VersionStore {
    /// Returns all visible rows for a transaction (optimized batch operation)
    ///
    /// This is more efficient than calling get_visible_version for each row
    /// because it batches the visibility checks and avoids repeated map lookups.
    /// Results are sorted by row_id.
    #[inline]
    pub fn get_all_visible_rows(&self, txn_id: i64) -> RowVec {
        self.get_all_visible_rows_internal(txn_id)
    }

    /// Extract visible rows AND the CowBTree snapshot at extraction time.
    /// Used by seal: the snapshot records each row's `txn_id` so that
    /// `remove_sealed_rows` can detect concurrent commits and skip them.
    pub fn extract_for_seal(&self, txn_id: i64) -> (RowVec, ExtractionSnapshot) {
        let snapshot = self.versions.read().clone();
        let rows = self.get_all_visible_rows_internal(txn_id);
        (rows, ExtractionSnapshot { inner: snapshot })
    }

    /// Extract committed rows for seal, filtered by commit_seq cutoff.
    /// Only includes rows committed before `commit_seq_cutoff`, ensuring
    /// snapshot isolation transactions that began before the cutoff can still
    /// see those rows after they move to cold storage.
    pub fn extract_for_seal_with_cutoff(
        &self,
        commit_seq_cutoff: i64,
    ) -> (RowVec, ExtractionSnapshot) {
        let snapshot = self.versions.read().clone();
        let mut rows = RowVec::with_capacity(self.committed_row_count());
        self.for_each_committed_version_with_cutoff(
            |row_id, version| {
                rows.push((row_id, version.data.clone()));
                true
            },
            commit_seq_cutoff,
        );
        (rows, ExtractionSnapshot { inner: snapshot })
    }

    /// Extract rows for seal in bounded chunks while preserving the same
    /// extraction snapshot used later by `remove_sealed_rows`.
    ///
    /// Rows are produced from the captured CowBTree snapshot, not from a second
    /// clone, so the extracted row set and the removal guard describe the same
    /// heads. `min_rows` lets the caller keep threshold semantics without
    /// writing sub-threshold volumes: chunks are not emitted unless at least
    /// `min_rows` rows are found. Before the threshold is reached, at most
    /// `max(min_rows, chunk_rows)` rows are buffered.
    ///
    /// Returns `(eligible_rows, extraction_snapshot, completed)`. `completed`
    /// is false only when `on_chunk` requested an early stop, for example after
    /// an I/O error while persisting a chunk.
    pub fn extract_for_seal_chunks<F>(
        &self,
        txn_id: i64,
        commit_seq_cutoff: Option<i64>,
        min_rows: usize,
        chunk_rows: usize,
        mut on_chunk: F,
    ) -> (usize, ExtractionSnapshot, bool)
    where
        F: FnMut(RowVec) -> bool,
    {
        let snapshot = self.versions.read().clone();
        // Capture registry holes after the immutable row-head snapshot. Any
        // transaction still in flight here must remain hot even if it completes
        // while the volume is being built.
        let seal_visibility = self
            .visibility_checker
            .as_ref()
            .map(|checker| checker.capture_seal_visibility())
            .unwrap_or_default();
        if self.closed.load(Ordering::Acquire) {
            return (0, ExtractionSnapshot { inner: snapshot }, true);
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return (0, ExtractionSnapshot { inner: snapshot }, true),
        };

        let min_rows = min_rows.max(1);
        let chunk_rows = chunk_rows.max(1);
        let initial_capacity = chunk_rows.min(min_rows.max(16));
        let mut pending = RowVec::with_capacity(initial_capacity);
        let mut total = 0usize;
        let mut completed = true;

        {
            if let Some(cutoff) = commit_seq_cutoff {
                let mut append_row = |row_id: i64, row: Row| -> bool {
                    total += 1;
                    pending.push((row_id, row));
                    if total >= min_rows && pending.len() >= chunk_rows {
                        let chunk =
                            std::mem::replace(&mut pending, RowVec::with_capacity(chunk_rows));
                        if !on_chunk(chunk) {
                            return false;
                        }
                    }
                    true
                };
                let snapshot_txn_id = i64::MAX;
                for (&row_id, chain_entry) in snapshot.iter() {
                    let mut current: Option<&VersionChainEntry> = Some(chain_entry);
                    while let Some(e) = current {
                        let version_txn_id = e.version.txn_id;
                        let deleted_at_txn_id = e.version.deleted_at_txn_id;

                        if checker.is_visible(version_txn_id, snapshot_txn_id) {
                            if !checker.is_committed_before(version_txn_id, cutoff) {
                                break;
                            }
                            if !seal_visibility.is_visible(version_txn_id) {
                                break;
                            }
                            if deleted_at_txn_id != 0
                                && checker.is_visible(deleted_at_txn_id, snapshot_txn_id)
                                && checker.is_committed_before(deleted_at_txn_id, cutoff)
                                && seal_visibility.is_visible(deleted_at_txn_id)
                            {
                                break;
                            }
                            if !append_row(row_id, e.version.data.clone()) {
                                completed = false;
                                break;
                            }
                            break;
                        }

                        current = e.prev.as_ref().map(|arc| arc.as_ref());
                    }
                    if !completed {
                        break;
                    }
                }
            } else {
                // Capture one bounded chunk of immutable row owners under the
                // arena read lock, then release it before invoking the caller.
                // The callback performs compression and durable I/O during a
                // seal, and must never extend the arena lock across that work.
                let mut entries = snapshot.iter();
                loop {
                    let mut ready_chunk = None;
                    let mut exhausted = false;
                    {
                        let arena_guard = self.arena.read_guard();
                        let arena_data = arena_guard.data();
                        let get_row = |entry: &VersionChainEntry| -> Row {
                            if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                                if let Some(arc_row) = arena_data.get(idx) {
                                    return Row::from_arc(CompactArc::clone(arc_row));
                                }
                            }
                            entry.version.data.clone()
                        };

                        loop {
                            let Some((&row_id, chain)) = entries.next() else {
                                exhausted = true;
                                break;
                            };
                            let head_txn_id = chain.version.txn_id;
                            let head_deleted_at = chain.version.deleted_at_txn_id;

                            // Never seal an older chain member while the HEAD is
                            // uncommitted or excluded by a live snapshot. The
                            // removal phase compares against the captured HEAD;
                            // extracting `prev` here could otherwise delete the
                            // newer HEAD after it commits during volume I/O.
                            let visible = if checker.is_visible(head_txn_id, txn_id)
                                && seal_visibility.is_visible(head_txn_id)
                            {
                                (head_deleted_at == 0
                                    || !checker.is_visible(head_deleted_at, txn_id))
                                .then_some(chain)
                                .filter(|_| {
                                    head_deleted_at == 0
                                        || seal_visibility.is_visible(head_deleted_at)
                                })
                            } else {
                                None
                            };

                            if let Some(entry) = visible {
                                total += 1;
                                pending.push((row_id, get_row(entry)));
                                if total >= min_rows && pending.len() >= chunk_rows {
                                    ready_chunk = Some(std::mem::replace(
                                        &mut pending,
                                        RowVec::with_capacity(chunk_rows),
                                    ));
                                    break;
                                }
                            }
                        }
                    }

                    if let Some(chunk) = ready_chunk {
                        if !on_chunk(chunk) {
                            completed = false;
                            break;
                        }
                    }
                    if exhausted {
                        break;
                    }
                }
            }
        }

        if completed && total >= min_rows && !pending.is_empty() {
            completed = on_chunk(pending);
        }

        (total, ExtractionSnapshot { inner: snapshot }, completed)
    }

    /// Internal implementation for getting all visible rows
    #[inline]
    pub(super) fn get_all_visible_rows_internal(&self, txn_id: i64) -> RowVec {
        if self.closed.load(Ordering::Acquire) {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock for O(1) Arc clones
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper: get row from arena (O(1)) or version (O(n) clone)
        let get_row = |entry: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            entry.version.data.clone()
        };
        let mut results = RowVec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    results.push((row_id, get_row(chain)));
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        results.push((row_id, get_row(e)));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        results
    }

    /// Returns all visible rows using arena for zero-copy scanning
    ///
    /// This method provides 50x+ faster full table scans by:
    /// 1. Pre-acquiring arena locks once
    /// 2. Reading directly during visibility iteration (single pass)
    /// 3. Using contiguous arena memory for cache locality
    #[inline]
    pub fn get_all_visible_rows_arena(&self, txn_id: i64) -> RowVec {
        if self.closed.load(Ordering::Acquire) {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Single-pass: read directly from arena during visibility check
        let mut result = RowVec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    result.push((row_id, get_row_data(chain)));
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        result.push((row_id, get_row_data(e)));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        result
    }

    /// Returns all visible rows using RowVec for zero-allocation reuse.
    ///
    /// Same as `get_all_visible_rows_arena` but uses cached RowVec.
    /// The returned `RowVec` auto-returns to cache on drop.
    #[inline]
    pub fn get_all_visible_rows_cached(&self, txn_id: i64) -> RowVec {
        let mut result = RowVec::new();

        if self.closed.load(Ordering::Acquire) {
            return result;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return result,
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Single-pass: read directly from arena during visibility check

        // Ensure capacity
        let current_capacity = result.capacity();
        let needed = versions.len();
        if current_capacity < needed {
            result.reserve(needed - current_capacity);
        }

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    result.push((row_id, get_row_data(chain)));
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        result.push((row_id, get_row_data(e)));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        #[cfg(test)]
        {
            let values = result.iter().map(|(_, row)| row.len() as u64).sum();
            crate::instrumentation::record_row_materialization_count(result.len() as u64, values);
        }

        result
    }

    /// Returns all visible rows with their original RowVersions for UPDATE operations.
    ///
    /// This is optimized for UPDATE operations that need to track the original version
    /// for conflict detection. By returning the RowVersion along with the row data,
    /// callers can use `put_batch_with_originals()` to avoid redundant `get_visible_version()`
    /// calls during the put phase.
    ///
    /// Returns: Vec of (row_id, row_data, original_version) tuples.
    #[inline]
    pub fn get_all_visible_rows_for_update(&self, txn_id: i64) -> Vec<(i64, Row, RowVersion)> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let current_seq = checker.get_current_sequence();

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Single-pass: read directly from arena during visibility check
        let mut result: Vec<(i64, Row, RowVersion)> = Vec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    let mut version_copy = chain.version.clone();
                    version_copy.create_time = current_seq;
                    result.push((row_id, get_row_data(chain), version_copy));
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        let mut version_copy = e.version.clone();
                        version_copy.create_time = current_seq;
                        result.push((row_id, get_row_data(e), version_copy));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        result
    }

    /// Get all visible rows for UPDATE with filter applied BEFORE cloning.
    ///
    /// This is a performance optimization for UPDATE operations with WHERE clauses.
    /// Instead of fetching all rows and then filtering, we filter during the scan
    /// to avoid allocating Row objects for non-matching rows.
    ///
    /// Returns (row_id, Row, RowVersion) tuples - the RowVersion is needed for
    /// MVCC conflict detection during the update.
    pub fn get_all_visible_rows_for_update_filtered(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
    ) -> radixdb_core::Result<Vec<(i64, Row, RowVersion)>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let current_seq = checker.get_current_sequence();

        // Compile the filter once at the start for speedup in hot loop
        let schema = self.schema.read();
        let compiled_filter = CompiledFilter::compile(filter, &schema);
        drop(schema); // Release lock early

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Single-pass: read, filter, and collect in one loop
        let mut result: Vec<(i64, Row, RowVersion)> = Vec::with_capacity(versions.len() / 4);

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    // Try arena path first (zero-copy filter check using matches_arc_slice)
                    if let Some(idx) = unpack_arena_idx(chain.arena_idx) {
                        if let Some(arc_row) = arena_data.get(idx) {
                            // Filter directly on Arc slice - no Row allocation for non-matching rows
                            if compiled_filter.matches_arc_slice_checked(arc_row.as_ref())? {
                                let mut version_copy = chain.version.clone();
                                version_copy.create_time = current_seq;
                                result.push((
                                    row_id,
                                    Row::from_arc(CompactArc::clone(arc_row)),
                                    version_copy,
                                ));
                            }
                            continue;
                        }
                    }
                    // Fallback: filter on version data
                    if compiled_filter.matches_checked(&chain.version.data)? {
                        let mut version_copy = chain.version.clone();
                        version_copy.create_time = current_seq;
                        result.push((row_id, chain.version.data.clone(), version_copy));
                    }
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        // Try arena path first (zero-copy filter check using matches_arc_slice)
                        if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                            if let Some(arc_row) = arena_data.get(idx) {
                                // Filter directly on Arc slice - no Row allocation for non-matching rows
                                if compiled_filter.matches_arc_slice_checked(arc_row.as_ref())? {
                                    let mut version_copy = e.version.clone();
                                    version_copy.create_time = current_seq;
                                    result.push((
                                        row_id,
                                        Row::from_arc(CompactArc::clone(arc_row)),
                                        version_copy,
                                    ));
                                }
                                break;
                            }
                        }
                        // Fallback: filter on version data
                        if compiled_filter.matches_checked(&e.version.data)? {
                            let mut version_copy = e.version.clone();
                            version_copy.create_time = current_seq;
                            result.push((row_id, e.version.data.clone(), version_copy));
                        }
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        Ok(result)
    }

    /// Returns all visible rows.
    ///
    /// Note: Functionally identical to get_all_visible_rows_arena (iteration is ordered).
    #[inline]
    pub fn get_all_visible_rows_unsorted(&self, txn_id: i64) -> RowVec {
        if self.closed.load(Ordering::Acquire) {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Single-pass: read directly from arena during visibility check
        let mut result = RowVec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    result.push((row_id, get_row_data(chain)));
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        result.push((row_id, get_row_data(e)));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        result
    }

    /// Get visible rows with limit and offset applied at the storage layer.
    ///
    /// # True Early Termination
    /// Iteration is ordered by row_id, enabling true
    /// early termination: skip `offset` visible rows, then collect `limit` rows
    /// and stop iterating.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility check
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip before collecting
    pub fn get_visible_rows_with_limit(&self, txn_id: i64, limit: usize, offset: usize) -> RowVec {
        if self.closed.load(Ordering::Acquire) || limit == 0 {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for the entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Collect with early termination
        let mut result = RowVec::with_capacity(limit);
        let mut skipped = 0usize;

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            // Track the actual visible entry, not just a boolean
            let visible_entry: Option<&VersionChainEntry> = if checker
                .is_visible(head_txn_id, txn_id)
            {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    Some(chain)
                } else {
                    None
                }
            } else {
                // SLOW PATH: HEAD not visible - traverse chain for older versions
                let mut found_entry: Option<&VersionChainEntry> = None;
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            found_entry = Some(e);
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
                found_entry
            };

            if let Some(entry) = visible_entry {
                if skipped < offset {
                    skipped += 1;
                } else {
                    result.push((row_id, get_row_data(entry)));
                    if result.len() >= limit {
                        break; // Early termination!
                    }
                }
            }
        }

        result
    }

    /// Get visible rows with LIMIT (with early termination).
    ///
    /// Note: Functionally identical to get_visible_rows_with_limit. Kept for API compatibility.
    #[inline]
    pub fn get_visible_rows_with_limit_unordered(
        &self,
        txn_id: i64,
        limit: usize,
        offset: usize,
    ) -> RowVec {
        // Delegate to the sorted version
        self.get_visible_rows_with_limit(txn_id, limit, offset)
    }

    /// Get a batch of visible rows starting after a given row_id (cursor-based pagination).
    ///
    /// This is designed for lazy/streaming scanners that fetch rows in batches.
    /// Uses range() for efficient cursor-based iteration.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility check
    /// * `after_row_id` - Start fetching rows AFTER this row_id (use i64::MIN to start from beginning)
    /// * `batch_size` - Maximum number of rows to return in this batch
    ///
    /// # Returns
    /// A tuple of (rows, has_more) where has_more indicates if there are more rows to fetch
    pub fn get_visible_rows_batch(
        &self,
        txn_id: i64,
        after_row_id: i64,
        batch_size: usize,
    ) -> (RowVec, bool) {
        if self.closed.load(Ordering::Acquire) || batch_size == 0 {
            return (RowVec::new(), false);
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return (RowVec::new(), false),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for this batch
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Use range for efficient cursor-based iteration
        let mut result = RowVec::with_capacity(batch_size);
        let mut has_more = false;

        // Use range to start after the cursor row_id
        for (&row_id, chain) in versions.range((
            std::ops::Bound::Excluded(after_row_id),
            std::ops::Bound::Unbounded::<i64>,
        )) {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            // Track the actual visible entry, not just a boolean
            let visible_entry: Option<&VersionChainEntry> = if checker
                .is_visible(head_txn_id, txn_id)
            {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    Some(chain)
                } else {
                    None
                }
            } else {
                // SLOW PATH: HEAD not visible - traverse chain for older versions
                let mut found_entry: Option<&VersionChainEntry> = None;
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            found_entry = Some(e);
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
                found_entry
            };

            if let Some(entry) = visible_entry {
                if result.len() >= batch_size {
                    has_more = true;
                    break; // Early termination - found one more than needed
                }
                result.push((row_id, get_row_data(entry)));
            }
        }

        (result, has_more)
    }

    /// Fetch visible rows into an existing buffer (avoids allocation)
    ///
    /// This is the same as `get_visible_rows_batch` but reuses the provided buffer
    /// instead of allocating a new Vec. The buffer is cleared before filling.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility checks
    /// * `after_row_id` - Cursor position (exclusive lower bound)
    /// * `batch_size` - Maximum number of rows to fetch
    /// * `buffer` - Existing buffer to fill (will be cleared first)
    ///
    /// # Returns
    /// `has_more` - true if there are more rows to fetch after this batch
    pub fn get_visible_rows_batch_into(
        &self,
        txn_id: i64,
        after_row_id: i64,
        batch_size: usize,
        buffer: &mut RowVec,
    ) -> bool {
        buffer.clear();

        if self.closed.load(Ordering::Acquire) || batch_size == 0 {
            return false;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return false,
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for this batch
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper closure to get row data from arena or version
        let get_row_data = |e: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            e.version.data.clone()
        };

        // Use range for efficient cursor-based iteration
        buffer.reserve(batch_size);
        let mut has_more = false;

        // Use range to start after the cursor row_id
        for (&row_id, chain) in versions.range((
            std::ops::Bound::Excluded(after_row_id),
            std::ops::Bound::Unbounded::<i64>,
        )) {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            // Track the actual visible entry, not just a boolean
            let visible_entry: Option<&VersionChainEntry> = if checker
                .is_visible(head_txn_id, txn_id)
            {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    Some(chain)
                } else {
                    None
                }
            } else {
                // SLOW PATH: HEAD not visible - traverse chain for older versions
                let mut found_entry: Option<&VersionChainEntry> = None;
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            found_entry = Some(e);
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
                found_entry
            };

            if let Some(entry) = visible_entry {
                if buffer.len() >= batch_size {
                    has_more = true;
                    break; // Early termination - found one more than needed
                }
                buffer.push((row_id, get_row_data(entry)));
            }
        }

        has_more
    }

    /// Collect visible rows ordered by row_id (PRIMARY KEY) with efficient OFFSET/LIMIT
    ///
    /// This method uses range iteration to efficiently skip OFFSET rows
    /// without cloning them, providing O(offset + limit) complexity instead of
    /// O(n) for full materialization.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility checks
    /// * `ascending` - If true, iterate from smallest to largest row_id
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of visible rows to skip before collecting
    ///
    /// # Returns
    /// Vector of rows in row_id order, or None if iteration fails
    pub fn collect_rows_pk_ordered(
        &self,
        txn_id: i64,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<RowVec> {
        if self.closed.load(Ordering::Acquire) || limit == 0 {
            return Some(RowVec::new());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Some(RowVec::new()),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for this entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper to get row data from an entry
        let get_row_from_entry = |entry: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            entry.version.data.clone()
        };

        // Collect with offset/limit
        // Cap capacity to avoid overflow when limit is usize::MAX
        let capacity = limit.min(versions.len()).min(10_000);
        let mut result = RowVec::with_capacity(capacity);
        let mut skipped = 0usize;

        if ascending {
            // Forward iteration with early termination
            for (row_id, chain) in versions.iter() {
                // Inline visibility check
                let mut current: Option<&VersionChainEntry> = Some(chain);
                while let Some(entry) = current {
                    if checker.is_visible(entry.version.txn_id, txn_id) {
                        if entry.version.deleted_at_txn_id == 0
                            || !checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
                        {
                            // Found visible entry
                            if skipped < offset {
                                skipped += 1;
                            } else {
                                result.push((*row_id, get_row_from_entry(entry)));
                                if result.len() >= limit {
                                    return Some(result);
                                }
                            }
                        }
                        break;
                    }
                    current = entry.prev.as_ref().map(|b| b.as_ref());
                }
            }
        } else {
            // Reverse iteration with early termination: O(limit + offset) instead of O(n)
            for (&row_id, chain) in versions.iter_rev() {
                let mut current: Option<&VersionChainEntry> = Some(chain);
                while let Some(entry) = current {
                    if checker.is_visible(entry.version.txn_id, txn_id) {
                        if entry.version.deleted_at_txn_id == 0
                            || !checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
                        {
                            if skipped < offset {
                                skipped += 1;
                            } else {
                                result.push((row_id, get_row_from_entry(entry)));
                                if result.len() >= limit {
                                    return Some(result);
                                }
                            }
                        }
                        break;
                    }
                    current = entry.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }

        Some(result)
    }

    /// Collect visible rows using keyset pagination (WHERE id > X ORDER BY id LIMIT Y)
    ///
    /// This method uses range iteration starting from a specific row_id,
    /// providing O(limit) complexity instead of O(n) for full table scans.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility checks
    /// * `start_after_row_id` - Start iteration after this row_id (exclusive, for id > X)
    /// * `start_from_row_id` - Start iteration from this row_id (inclusive, for id >= X)
    /// * `ascending` - If true, iterate from smallest to largest row_id
    /// * `limit` - Maximum number of rows to return
    ///
    /// # Returns
    /// RowVec of (row_id, row) pairs in row_id order
    pub fn collect_rows_keyset(
        &self,
        txn_id: i64,
        start_after_row_id: Option<i64>,
        start_from_row_id: Option<i64>,
        ascending: bool,
        limit: usize,
    ) -> RowVec {
        if self.closed.load(Ordering::Acquire) || limit == 0 {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE for this entire operation
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper to find visible version and get row data
        let find_visible_row = |chain: &VersionChainEntry| -> Option<Row> {
            let mut current: Option<&VersionChainEntry> = Some(chain);
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id != 0 && checker.is_visible(deleted_at_txn_id, txn_id) {
                        break; // Row is deleted
                    }

                    // Read row data from arena or version
                    let row_data = if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                        if let Some(arc_row) = arena_data.get(idx) {
                            Row::from_arc(CompactArc::clone(arc_row))
                        } else {
                            e.version.data.clone()
                        }
                    } else {
                        e.version.data.clone()
                    };
                    return Some(row_data);
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
            None
        };

        // Determine range bounds
        let start_bound = if let Some(after_id) = start_after_row_id {
            std::ops::Bound::Excluded(after_id)
        } else if let Some(from_id) = start_from_row_id {
            std::ops::Bound::Included(from_id)
        } else {
            std::ops::Bound::Unbounded
        };

        // Collect with early termination
        let mut result = RowVec::with_capacity(limit);

        if ascending {
            for (&row_id, chain) in versions.range((start_bound, std::ops::Bound::Unbounded::<i64>))
            {
                if let Some(row_data) = find_visible_row(chain) {
                    result.push((row_id, row_data));
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        } else {
            // Reverse range iteration with early termination: O(limit) instead of O(n)
            for (&row_id, chain) in
                versions.range_rev((start_bound, std::ops::Bound::Unbounded::<i64>))
            {
                if let Some(row_data) = find_visible_row(chain) {
                    result.push((row_id, row_data));
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        result
    }

    /// Get all visible rows with filter applied during collection
    /// This saves memory by not allocating space for non-matching rows
    ///
    /// # Performance
    ///
    /// The filter expression is compiled into a `CompiledFilter` at the start
    /// to eliminate virtual dispatch overhead in the hot loop. This provides
    /// ~3-5x speedup for filter-heavy queries.
    pub fn get_all_visible_rows_filtered(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
    ) -> radixdb_core::Result<RowVec> {
        self.get_all_visible_rows_filtered_internal(txn_id, filter, None)
    }

    /// Filter visible rows inside stable inclusive row-ID ranges. The range
    /// test runs before payload access, so pruned logical segments do not
    /// materialize or evaluate their rows.
    pub fn get_all_visible_rows_filtered_in_ranges(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
        row_id_ranges: &[(i64, i64)],
    ) -> radixdb_core::Result<RowVec> {
        self.get_all_visible_rows_filtered_internal(txn_id, filter, Some(row_id_ranges))
    }

    pub(super) fn get_all_visible_rows_filtered_internal(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
        row_id_ranges: Option<&[(i64, i64)]>,
    ) -> radixdb_core::Result<RowVec> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(RowVec::new());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Ok(RowVec::new()),
        };

        // Compile the filter once at the start for ~3-5x speedup in hot loop
        // CompiledFilter eliminates virtual dispatch via enum-based specialization
        let schema = self.schema.read();
        let compiled_filter = CompiledFilter::compile(filter, &schema);
        drop(schema); // Release lock early

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Single-pass: read, filter, and collect in one loop
        let mut result = RowVec::with_capacity(versions.len() / 4);

        for (&row_id, chain) in versions.iter() {
            if row_id_ranges.is_some_and(|ranges| {
                !ranges
                    .iter()
                    .any(|(min, max)| row_id >= *min && row_id <= *max)
            }) {
                continue;
            }
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id != 0 && checker.is_visible(deleted_at_txn_id, txn_id) {
                        break; // Row is deleted
                    }

                    // OPTIMIZATION: Filter BEFORE cloning to avoid allocation for non-matching rows
                    // Try arena path first (zero-copy filter check using matches_arc_slice)
                    if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                        if let Some(arc_row) = arena_data.get(idx) {
                            // Filter directly on Arc slice - no Row allocation for non-matching rows
                            if compiled_filter.matches_arc_slice_checked(arc_row.as_ref())? {
                                result.push((row_id, Row::from_arc(CompactArc::clone(arc_row))));
                            }
                            break;
                        }
                    }
                    // Fallback: filter on version data (already allocated)
                    if compiled_filter.matches_checked(&e.version.data)? {
                        result.push((row_id, e.version.data.clone()));
                    }
                    break;
                }
                current = e.prev.as_deref();
            }
        }

        Ok(result)
    }

    /// Iterate every visible row without first materializing a `RowVec`.
    ///
    /// The callback owns only the current cheap `Row`/arena Arc handle. Locks
    /// remain in the normal versions -> arena order for the duration of the
    /// snapshot walk, so callers must not re-enter this VersionStore.
    pub fn for_each_all_visible<F>(&self, txn_id: i64, mut callback: F)
    where
        F: FnMut(i64, Row) -> bool,
    {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(checker) => checker,
            None => return,
        };

        let versions = self.versions.read();
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        for (&row_id, chain) in versions.iter() {
            let mut current: Option<&VersionChainEntry> = Some(chain);
            while let Some(entry) = current {
                if checker.is_visible(entry.version.txn_id, txn_id) {
                    if entry.version.deleted_at_txn_id != 0
                        && checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
                    {
                        break;
                    }
                    let row = unpack_arena_idx(entry.arena_idx)
                        .and_then(|index| arena_data.get(index))
                        .map(|row| Row::from_arc(CompactArc::clone(row)))
                        .unwrap_or_else(|| entry.version.data.clone());
                    if !callback(row_id, row) {
                        return;
                    }
                    break;
                }
                current = entry.prev.as_deref();
            }
        }
    }

    /// Iterate visible rows matching a filter with early termination via callback.
    ///
    /// Calls `callback(row_id, row_data)` for each matching row. The callback
    /// returns `true` to continue or `false` to stop iteration.
    /// This avoids materializing all matching rows into a Vec.
    pub fn for_each_visible_filtered<F>(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
        mut callback: F,
    ) -> radixdb_core::Result<()>
    where
        F: FnMut(i64, Row) -> bool,
    {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Ok(()),
        };

        let schema = self.schema.read();
        let compiled_filter = CompiledFilter::compile(filter, &schema);
        drop(schema);

        let versions = self.versions.read().clone();
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        for (&row_id, chain) in versions.iter() {
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id != 0 && checker.is_visible(deleted_at_txn_id, txn_id) {
                        break;
                    }

                    if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                        if let Some(arc_row) = arena_data.get(idx) {
                            if compiled_filter.matches_arc_slice_checked(arc_row.as_ref())? {
                                let row = Row::from_arc(CompactArc::clone(arc_row));
                                if !callback(row_id, row) {
                                    return Ok(());
                                }
                            }
                            break;
                        }
                    }
                    if compiled_filter.matches_checked(&e.version.data)?
                        && !callback(row_id, e.version.data.clone())
                    {
                        return Ok(());
                    }
                    break;
                }
                current = e.prev.as_deref();
            }
        }

        Ok(())
    }

    /// Get visible rows with filter, limit and offset applied at the storage layer.
    ///
    /// # True Early Termination
    /// Iteration is ordered. After collecting `limit` matching rows
    /// (after skipping `offset`), we can stop iterating.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility check
    /// * `filter` - Expression filter to apply to rows
    /// * `limit` - Maximum number of matching rows to return
    /// * `offset` - Number of matching rows to skip before collecting
    pub fn get_visible_rows_filtered_with_limit(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
        limit: usize,
        offset: usize,
    ) -> radixdb_core::Result<RowVec> {
        if self.closed.load(Ordering::Acquire) || limit == 0 {
            return Ok(RowVec::new());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Ok(RowVec::new()),
        };

        // Compile the filter once at the start
        let schema = self.schema.read();
        let compiled_filter = CompiledFilter::compile(filter, &schema);
        drop(schema);

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock ONCE
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Collect with offset/limit and early termination
        let mut result = RowVec::with_capacity(limit);
        let mut skipped = 0usize;

        for (&row_id, chain) in versions.iter() {
            let mut current: Option<&VersionChainEntry> = Some(chain);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id != 0 && checker.is_visible(deleted_at_txn_id, txn_id) {
                        break; // Row is deleted
                    }

                    // OPTIMIZATION: Filter BEFORE cloning to avoid allocation for non-matching rows
                    // Try arena path first (zero-copy filter check using matches_arc_slice)
                    if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                        if let Some(arc_row) = arena_data.get(idx) {
                            // Filter directly on Arc slice - no Row allocation for non-matching rows
                            if compiled_filter.matches_arc_slice_checked(arc_row.as_ref())? {
                                if skipped < offset {
                                    skipped += 1;
                                } else {
                                    result
                                        .push((row_id, Row::from_arc(CompactArc::clone(arc_row))));
                                    if result.len() >= limit {
                                        return Ok(result); // Early termination!
                                    }
                                }
                            }
                            break;
                        }
                    }
                    // Fallback: filter on version data (already allocated)
                    if compiled_filter.matches_checked(&e.version.data)? {
                        if skipped < offset {
                            skipped += 1;
                        } else {
                            result.push((row_id, e.version.data.clone()));
                            if result.len() >= limit {
                                return Ok(result); // Early termination!
                            }
                        }
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        Ok(result)
    }

    /// Get visible rows with filter and LIMIT (with early termination).
    ///
    /// Note: Functionally identical to get_visible_rows_filtered_with_limit. Kept for API compatibility.
    #[inline]
    pub fn get_visible_rows_filtered_with_limit_unordered(
        &self,
        txn_id: i64,
        filter: &dyn crate::expression::Expression,
        limit: usize,
        offset: usize,
    ) -> radixdb_core::Result<RowVec> {
        // Delegate to the sorted version
        self.get_visible_rows_filtered_with_limit(txn_id, filter, limit, offset)
    }

    /// Get visible row indices without materializing row data (ZERO ALLOCATION SCAN!)
    ///
    /// This method returns lightweight `RowIndex` structs instead of cloning row data.
    /// Callers can then filter/sort/limit these indices and only materialize the final
    /// set of rows needed using `materialize_rows()`.
    ///
    /// # Performance
    /// For `SELECT * FROM t WHERE x > 100 LIMIT 10` on 100K rows:
    /// - Old approach: Clone 100K rows, filter, limit (100K allocations)
    /// - New approach: Get 100K indices (0 allocations), filter, limit, clone 10 rows
    ///
    /// # Returns
    /// Vector of `RowIndex` structs containing row_id and arena location
    pub fn get_visible_row_indices(&self, txn_id: i64) -> Vec<RowIndex> {
        let checker = self.visibility_checker.as_ref();

        // FAST PATH: If uncommitted_writes is empty, scan arena directly.
        // SAFETY: This fast path is only valid under ReadCommitted isolation.
        // Under SnapshotIsolation, HEAD versions may not be visible and the correct
        // behavior requires walking version chains to find older visible versions.
        //
        // LOCK ORDERING: Check uncommitted_writes BEFORE acquiring arena to maintain
        // consistent ordering with truncate_all (uncommitted_writes → arena).
        if let Some(checker) = checker {
            let uncommitted_empty = self.uncommitted_writes.read().is_empty();

            // Get arena metadata for fast path detection
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_len = arena_guard.len();

            if uncommitted_empty && arena_len > 0 && !checker.needs_snapshot_isolation(txn_id) {
                let mut indices: Vec<RowIndex> = Vec::with_capacity(arena_len);

                for (idx, meta) in arena_meta.iter().enumerate() {
                    if meta.txn_id != 0
                        && meta.deleted_at_txn_id == 0
                        && checker.is_visible(meta.txn_id, txn_id)
                    {
                        indices.push(RowIndex::new(meta.row_id, Some(idx)));
                    }
                }

                // Sort by row_id for consistent ordering
                indices.sort_unstable_by_key(|idx| idx.row_id);
                return indices;
            }
            // arena_guard dropped here — no need to hold it for slow path
        }

        // SLOW PATH: Full iteration
        let versions = self.versions.read().clone();
        let mut indices: Vec<RowIndex> = Vec::with_capacity(versions.len());

        if let Some(checker) = checker {
            for (&row_id, chain) in versions.iter() {
                let mut current: Option<&VersionChainEntry> = Some(chain);

                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            // Found visible, non-deleted version
                            indices.push(RowIndex::new(row_id, unpack_arena_idx(e.arena_idx)));
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }

        indices
    }

    /// Get visible row indices without guaranteed ordering.
    ///
    /// This is optimized for aggregation operations (SUM, MIN, MAX, COUNT) that
    /// don't need row ordering. It skips the expensive sort in the arena fast path.
    #[inline]
    pub fn get_visible_row_indices_unordered(&self, txn_id: i64) -> Vec<RowIndex> {
        let checker = self.visibility_checker.as_ref();

        // FAST PATH: If uncommitted_writes is empty, scan arena directly (NO SORT!)
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        //
        // LOCK ORDERING: Check uncommitted_writes BEFORE acquiring arena to maintain
        // consistent ordering with truncate_all (uncommitted_writes → arena).
        if let Some(checker) = checker {
            let uncommitted_empty = self.uncommitted_writes.read().is_empty();

            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_len = arena_guard.len();

            if uncommitted_empty && arena_len > 0 && !checker.needs_snapshot_isolation(txn_id) {
                let mut indices: Vec<RowIndex> = Vec::with_capacity(arena_len);

                for (idx, meta) in arena_meta.iter().enumerate() {
                    if meta.txn_id != 0
                        && meta.deleted_at_txn_id == 0
                        && checker.is_visible(meta.txn_id, txn_id)
                    {
                        indices.push(RowIndex::new(meta.row_id, Some(idx)));
                    }
                }

                // Skip sorting - aggregations don't need ordering
                return indices;
            }
            // arena_guard dropped here — no need to hold it for slow path
        }

        // SLOW PATH: Full iteration
        let versions = self.versions.read().clone();
        let mut indices: Vec<RowIndex> = Vec::with_capacity(versions.len());

        if let Some(checker) = checker {
            for (&row_id, chain) in versions.iter() {
                let mut current: Option<&VersionChainEntry> = Some(chain);

                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            indices.push(RowIndex::new(row_id, unpack_arena_idx(e.arena_idx)));
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }

        indices
    }

    /// Materialize selected row indices into actual Row data
    ///
    /// This is the second step of deferred materialization. After filtering/limiting
    /// `RowIndex` values, call this to get the actual row data.
    ///
    /// Same HEAD-fallback caveat as `materialize_row` — see its doc comment.
    ///
    /// # Performance
    /// - Only clones the rows you actually need
    /// - Falls back to version chain for non-arena rows
    pub fn materialize_rows(&self, indices: &[RowIndex]) -> RowVec {
        let started = Instant::now();
        if indices.is_empty() {
            return RowVec::new();
        }

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();
        let arena_len = arena_guard.len();

        let mut result = RowVec::with_capacity(indices.len());

        for idx in indices {
            if let Some(arena_idx) = idx.arena_idx() {
                // Fast path: get from arena
                if arena_idx < arena_len {
                    if let Some(arc_row) = arena_data.get(arena_idx) {
                        result.push((idx.row_id, Row::from_arc(CompactArc::clone(arc_row))));
                        continue;
                    }
                }
            }

            // Slow path: look up in version chain (CowBTree clone already held)
            if let Some(entry) = versions.get(idx.row_id) {
                result.push((idx.row_id, entry.version.data.clone()));
            }
        }

        instrumentation::record_row_materialization(result.len() as u64, 0, started.elapsed());
        result
    }

    /// Materialize a single row by index (for use in iterators)
    ///
    /// NOTE: When `arena_idx` is `None` (chain entries under snapshot isolation),
    /// the fallback reads `chain.version.data` which is HEAD. If a concurrent
    /// transaction updated the row after the RowIndex was collected, this returns
    /// the newer HEAD instead of the older visible version. This is a known
    /// limitation; `get_visible_rows_sorted_limit` avoids it with inline
    /// materialization during collection. Narrow scenario: snapshot isolation
    /// with concurrent updates between collect and materialize.
    #[inline]
    pub fn materialize_row(&self, idx: &RowIndex) -> Option<(i64, Row)> {
        if let Some(arena_idx) = idx.arena_idx() {
            let arena_guard = self.arena.read_guard();
            let arena_data = arena_guard.data();
            if let Some(arc_row) = arena_data.get(arena_idx) {
                let row = Row::from_arc(CompactArc::clone(arc_row));
                instrumentation::record_row_materialization_count(1, 0);
                return Some((idx.row_id, row));
            }
        }

        // Slow path: look up in version chain (returns HEAD — see doc note above)
        let result = self
            .versions
            .read()
            .get(idx.row_id)
            .map(|chain| (idx.row_id, chain.version.data.clone()));
        if result.is_some() {
            instrumentation::record_row_materialization_count(1, 0);
        }
        result
    }

    /// Get a single column value from a row index WITHOUT full row materialization
    ///
    /// This is the key optimization for ORDER BY + LIMIT queries:
    /// - Load only the sort column, not all columns
    /// - Enables sorting indices by column value before materializing
    ///
    /// # Performance
    /// For `SELECT * FROM t ORDER BY col LIMIT 10` on 100K rows with 20 columns:
    /// - Old: Clone 100K rows (2M values), sort, take 10
    /// - New: Load 100K single values, sort indices, clone 10 rows (200 values)
    #[inline]
    pub fn get_column_value(&self, idx: &RowIndex, col_idx: usize) -> Option<Value> {
        if let Some(arena_idx) = idx.arena_idx() {
            let arena_guard = self.arena.read_guard();
            let arena_data = arena_guard.data();
            if let Some(arc_row) = arena_data.get(arena_idx) {
                return arc_row.get(col_idx).cloned();
            }
        }

        // Slow path: look up in version chain (returns HEAD — same caveat as materialize_row)
        self.versions
            .read()
            .get(idx.row_id)
            .and_then(|entry| entry.version.data.get(col_idx).cloned())
    }

    /// Batch get column values for multiple indices
    ///
    /// Optimized for ORDER BY: loads sort key values for all indices at once.
    #[inline]
    pub fn get_column_values_batch(
        &self,
        indices: &[RowIndex],
        col_idx: usize,
    ) -> Vec<Option<Value>> {
        if indices.is_empty() {
            return Vec::new();
        }

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();
        let arena_len = arena_guard.len();
        indices
            .iter()
            .map(|idx| {
                if let Some(arena_idx) = idx.arena_idx() {
                    if arena_idx < arena_len {
                        if let Some(arc_row) = arena_data.get(arena_idx) {
                            return arc_row.get(col_idx).cloned();
                        }
                    }
                }
                // Slow path: version chain lookup (returns HEAD — same caveat as materialize_row)
                versions
                    .get(idx.row_id)
                    .and_then(|entry| entry.version.data.get(col_idx).cloned())
            })
            .collect()
    }

    /// Optimized ORDER BY + LIMIT scan using deferred materialization
    ///
    /// This is the FAST PATH for queries like `SELECT * FROM t ORDER BY col LIMIT 10`:
    /// 1. Get row indices (no cloning)
    /// 2. Load only sort column values (not full rows)
    /// 3. Sort indices by sort values
    /// 4. Take top N indices
    /// 5. Materialize only N rows
    ///
    /// # Performance
    /// For 100K rows with 20 columns, LIMIT 10:
    /// - Old: Clone 2M values, sort 100K rows, take 10 → ~100ms
    /// - New: Load 100K values, sort indices, clone 200 values → ~10ms
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility
    /// * `sort_col_idx` - Column index to sort by
    /// * `ascending` - Sort direction
    /// * `limit` - Maximum rows to return
    /// * `offset` - Rows to skip before collecting
    pub fn get_visible_rows_sorted_limit(
        &self,
        txn_id: i64,
        sort_col_idx: usize,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> RowVec {
        if limit == 0 {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Steps 1+2 combined: collect (RowIndex, sort_value) in a single lock scope
        let mut paired: Vec<(RowIndex, Option<Value>)>;

        // FAST PATH: Scan arena directly when no uncommitted writes.
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();
        if uncommitted_empty && !checker.needs_snapshot_isolation(txn_id) {
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            let arena_data = arena_guard.data();
            let arena_len = arena_guard.len();

            if arena_len > 0 {
                paired = Vec::with_capacity(arena_len);
                for (idx, meta) in arena_meta.iter().enumerate() {
                    if meta.txn_id != 0
                        && meta.deleted_at_txn_id == 0
                        && checker.is_visible(meta.txn_id, txn_id)
                    {
                        let sort_val = arena_data
                            .get(idx)
                            .and_then(|row| row.get(sort_col_idx).cloned());
                        paired.push((RowIndex::new(meta.row_id, Some(idx)), sort_val));
                    }
                }

                if paired.is_empty() {
                    return RowVec::new();
                }

                // Drop arena guard before sort and materialize
                drop(arena_guard);

                // Sort, take limit, materialize
                paired.sort_by(|(_, a), (_, b)| {
                    let cmp = match (a, b) {
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

                let selected: Vec<RowIndex> = paired
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(idx, _)| idx)
                    .collect();

                return self.materialize_rows(&selected);
            }
        }

        // SLOW PATH: CowBTree iteration — materialize during collection because
        // the visible version may be a chain entry (not HEAD), and RowIndex-based
        // deferred materialization would lose track of which version was visible.
        let versions = self.versions.read().clone();
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        let mut materialized: Vec<(i64, Row, Option<Value>)> = Vec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            let mut current: Option<&VersionChainEntry> = Some(chain);
            while let Some(e) = current {
                if checker.is_visible(e.version.txn_id, txn_id) {
                    if e.version.deleted_at_txn_id == 0
                        || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                    {
                        let row = if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                            if let Some(arc_row) = arena_data.get(idx) {
                                Row::from_arc(CompactArc::clone(arc_row))
                            } else {
                                e.version.data.clone()
                            }
                        } else {
                            e.version.data.clone()
                        };
                        let sort_val = row.get(sort_col_idx).cloned();
                        materialized.push((row_id, row, sort_val));
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        // Drop locks before sort
        drop(arena_guard);
        drop(versions);

        if materialized.is_empty() {
            return RowVec::new();
        }

        // Sort by sort values
        materialized.sort_by(|(_, _, a), (_, _, b)| {
            let cmp = match (a, b) {
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

        // Apply offset/limit and strip sort values
        materialized
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(row_id, row, _)| (row_id, row))
            .collect()
    }
}
