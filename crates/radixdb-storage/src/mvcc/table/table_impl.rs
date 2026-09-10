use super::*;

impl Table for MVCCTable {
    fn name(&self) -> &str {
        self.version_store.table_name()
    }

    fn schema(&self) -> &Schema {
        &self.cached_schema
    }

    fn txn_id(&self) -> i64 {
        self.txn_id
    }

    fn stage_external_index_removal(
        &mut self,
        index: Arc<dyn Index>,
        values: Vec<Value>,
        row_id: i64,
    ) -> Result<()> {
        self.txn_versions
            .write()
            .unwrap()
            .stage_external_index_removal(index, values, row_id)
    }

    fn materialize_insert_values(&mut self, row: &mut Row) -> Result<()> {
        self.materialize_auto_increment_values(row)
    }

    /// Fetch rows by their IDs, applying filter
    ///
    /// Fetch rows into a reusable RowVec buffer.
    /// Optimized to use batch fetch for global store rows,
    /// reducing lock contention from O(n) to O(1).
    fn fetch_rows_by_ids(&self, row_ids: &[i64], filter: &dyn Expression) -> Result<RowVec> {
        self.fetch_rows_by_ids_checked(row_ids, filter)
    }

    fn fetch_rows_by_ids_into(
        &self,
        row_ids: &[i64],
        filter: &dyn Expression,
        rows: &mut RowVec,
    ) -> Result<()> {
        rows.extend(self.fetch_rows_by_ids_checked(row_ids, filter)?);
        Ok(())
    }

    fn create_column(&mut self, name: &str, column_type: DataType, nullable: bool) -> Result<()> {
        self.create_column_with_default(name, column_type, nullable, None)
    }

    fn create_column_with_default(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
    ) -> Result<()> {
        self.create_column_with_default_value(name, column_type, nullable, default_expr, None)
    }

    fn create_column_with_default_value(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
        default_value: Option<Value>,
    ) -> Result<()> {
        // Create a SchemaColumn and add to both version store and cached schema
        // Get the next column ID
        let next_id = self.cached_schema.columns.len();
        let column = SchemaColumn::with_default_value(
            next_id,
            name,
            column_type,
            nullable,
            false, // primary_key
            false, // auto_increment
            default_expr,
            default_value,
            None, // check_expr
        );
        {
            let mut schema_guard = self.version_store.schema_mut();
            CompactArc::make_mut(&mut *schema_guard).add_column(column.clone())?;
        }
        self.version_store.require_row_normalization();
        CompactArc::make_mut(&mut self.cached_schema).add_column(column)?;
        Ok(())
    }

    fn drop_column(&mut self, name: &str) -> Result<()> {
        let column_index = self
            .cached_schema
            .get_column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;
        let mut post_drop_schema = self.cached_schema.as_ref().clone();
        post_drop_schema.remove_column(name)?;

        // Direct Table callers do not pass through Executor's catalog helper,
        // so keep this table's shared and transaction-local row layouts aligned
        // before exposing the reduced schema.
        self.version_store
            .remove_column_from_hot_versions(column_index);
        self.txn_versions
            .write()
            .unwrap()
            .remove_column_from_local_versions(column_index);
        *self.version_store.schema_mut() = CompactArc::new(post_drop_schema.clone());
        self.version_store.require_row_normalization();
        self.cached_schema = CompactArc::new(post_drop_schema);
        Ok(())
    }

    fn insert(&mut self, mut row: Row) -> Result<Row> {
        let row_id = self.prepare_insert(&mut row)?;
        let inserted_row = row.clone();
        self.txn_versions.write().unwrap().put(row_id, row, false)?;
        Ok(inserted_row)
    }

    fn insert_discard(&mut self, mut row: Row) -> Result<()> {
        let row_id = self.prepare_insert(&mut row)?;
        self.txn_versions.write().unwrap().put(row_id, row, false)?;
        Ok(())
    }

    fn insert_batch(&mut self, rows: Vec<Row>) -> Result<()> {
        let statement_boundary = get_fast_timestamp();
        let result = (|| {
            // Use insert_discard since we don't need returned rows
            for row in rows {
                self.insert_discard(row)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.txn_versions
                .write()
                .unwrap()
                .rollback_to_timestamp(statement_boundary);
        }
        result
    }

    fn update(
        &mut self,
        where_expr: Option<&dyn Expression>,
        setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32> {
        let statement_boundary = get_fast_timestamp();
        let result = (|| {
            // OPTIMIZATION: Borrow schema instead of cloning - saves allocation per update
            let schema = &self.cached_schema;

            // Fast path: Check if this is a primary key equality lookup (WHERE id = X)
            if let Some(expr) = where_expr {
                if let Some(pk_id) = self.try_pk_lookup(expr, schema) {
                    // Direct O(1) lookup by primary key. Local versions already own
                    // the claim. A committed candidate must be claimed before it is
                    // read/evaluated: a waiter then observes the owner's committed
                    // value instead of computing from the stale pre-wait row.
                    let local_row = {
                        let txn_versions = self.txn_versions.read().unwrap();
                        if let Some(local) = txn_versions.get_local_version(pk_id) {
                            if local.is_deleted() {
                                Some(None)
                            } else {
                                Some(Some(local.data.clone()))
                            }
                        } else {
                            None
                        }
                    };

                    if let Some(local_row) = local_row {
                        let Some(row) = local_row else {
                            return Ok(0);
                        };
                        let row = self.normalize_row_to_schema(row, schema);
                        let (updated_row, changed) = self.apply_validated_setter(row, setter)?;
                        if changed {
                            self.txn_versions
                                .write()
                                .unwrap()
                                .put(pk_id, updated_row, false)?;
                            return Ok(1);
                        }
                        return Ok(0);
                    }

                    self.txn_versions
                        .write()
                        .unwrap()
                        .claim_rows_for_update(&[pk_id])?;
                    if let Some(original_version) =
                        self.version_store.get_visible_version(pk_id, self.txn_id)
                    {
                        if original_version.is_deleted() {
                            return Ok(0);
                        }
                        let row = original_version.data.clone();
                        // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
                        let row = self.normalize_row_to_schema(row, schema);
                        let (updated_row, changed) = self.apply_validated_setter(row, setter)?;
                        if !changed {
                            return Ok(0);
                        }
                        self.txn_versions.write().unwrap().put_with_original(
                            pk_id,
                            updated_row,
                            original_version,
                            false,
                        )?;
                        return Ok(1);
                    }
                    return Ok(0);
                }

                // Fast path: PK range lookup (WHERE id >= X AND id < Y)
                if let Some(pk_range_ids) = self.try_pk_range_lookup(expr, schema) {
                    // OPTIMIZATION: Track original versions to avoid redundant lookups
                    let mut local_rows = RowVec::with_capacity(pk_range_ids.len() / 4);
                    let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                        Vec::with_capacity(pk_range_ids.len());

                    // Step 1: Check local versions first (single lock acquisition)
                    // Use get_local_version to distinguish "no local version" from "locally deleted"
                    let mut remaining_row_ids: Vec<i64> = Vec::with_capacity(pk_range_ids.len());
                    {
                        let txn_versions = self.txn_versions.read().unwrap();
                        for row_id in pk_range_ids {
                            if let Some(local) = txn_versions.get_local_version(row_id) {
                                if !local.is_deleted() {
                                    let row =
                                        self.normalize_row_to_schema(local.data.clone(), schema);
                                    let (updated_row, changed) =
                                        self.apply_validated_setter(row, setter)?;
                                    if changed {
                                        local_rows.push((row_id, updated_row));
                                    }
                                }
                                // Locally deleted — skip, don't fall through
                            } else {
                                remaining_row_ids.push(row_id);
                            }
                        }
                    }

                    // Step 2: Batch fetch remaining from version store (1 lock for N rows)
                    if !remaining_row_ids.is_empty() {
                        self.txn_versions
                            .write()
                            .unwrap()
                            .claim_rows_for_update(&remaining_row_ids)?;
                        let batch_rows = self
                            .version_store
                            .get_visible_versions_for_update(&remaining_row_ids, self.txn_id);
                        for (row_id, row, version) in batch_rows {
                            let row = self.normalize_row_to_schema(row, schema);
                            let (updated_row, changed) =
                                self.apply_validated_setter(row, setter)?;
                            if changed {
                                rows_with_originals.push((row_id, updated_row, version));
                            }
                        }
                    }

                    let update_count = (local_rows.len() + rows_with_originals.len()) as i32;
                    if !local_rows.is_empty() || !rows_with_originals.is_empty() {
                        let mut txn_versions = self.txn_versions.write().unwrap();
                        if !local_rows.is_empty() {
                            txn_versions.put_batch_for_update(local_rows)?;
                        }
                        if !rows_with_originals.is_empty() {
                            txn_versions.put_batch_with_originals(rows_with_originals)?;
                        }
                    }
                    return Ok(update_count);
                }

                // A secondary index does not contain transaction-local INSERTs or
                // key changes until commit. When this transaction already has
                // local versions, the index candidate set is incomplete and the
                // merged fallback below must drive the UPDATE.
                let indexed_row_ids = if self.txn_versions.read().unwrap().has_local_changes() {
                    None
                } else {
                    self.try_index_lookup(expr, schema)?
                };
                if let Some(filtered_row_ids) = indexed_row_ids {
                    // Step 1: Check local versions first (these don't need write-set tracking)
                    // Use get_local_version to distinguish "no local version" from "locally deleted"
                    let mut local_rows_to_update =
                        RowVec::with_capacity(filtered_row_ids.len() / 4);
                    let mut remaining_row_ids: Vec<i64> =
                        Vec::with_capacity(filtered_row_ids.len());

                    {
                        let txn_versions = self.txn_versions.read().unwrap();
                        for &row_id in &filtered_row_ids {
                            if let Some(local) = txn_versions.get_local_version(row_id) {
                                if !local.is_deleted() {
                                    let row =
                                        self.normalize_row_to_schema(local.data.clone(), schema);
                                    // Re-apply filter
                                    if expr.evaluate(&row)? {
                                        local_rows_to_update.push((row_id, row));
                                    }
                                }
                                // Locally deleted — skip, don't fall through
                            } else {
                                remaining_row_ids.push(row_id);
                            }
                        }
                    }

                    // Step 2: Batch fetch remaining rows from version store WITH original versions
                    // This avoids redundant get_visible_version() calls during put
                    // OPTIMIZATION: Pre-allocate with known capacity
                    let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                        Vec::with_capacity(remaining_row_ids.len());
                    if !remaining_row_ids.is_empty() {
                        self.txn_versions
                            .write()
                            .unwrap()
                            .claim_rows_for_update(&remaining_row_ids)?;
                        let batch_rows = self
                            .version_store
                            .get_visible_versions_for_update(&remaining_row_ids, self.txn_id);
                        for (row_id, row, original) in batch_rows {
                            // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
                            let row = self.normalize_row_to_schema(row, schema);
                            // Re-apply filter (index may be partial match)
                            if expr.evaluate(&row)? {
                                rows_with_originals.push((row_id, row, original));
                            }
                        }
                    }

                    // Step 3: Apply setter to all rows, filtering out unchanged rows
                    // Setter returns Result — on error, abort BEFORE batch put
                    // to guarantee statement-level atomicity.
                    let mut setter_error: Option<radixdb_core::Error> = None;
                    local_rows_to_update.retain_mut(|(_, row)| {
                        if setter_error.is_some() {
                            return false;
                        }
                        match self.apply_validated_setter(std::mem::take(row), setter) {
                            Ok((updated_row, changed)) => {
                                *row = updated_row;
                                changed
                            }
                            Err(e) => {
                                setter_error = Some(e);
                                false
                            }
                        }
                    });

                    // Update rows from version store with pre-fetched originals
                    if setter_error.is_none() {
                        rows_with_originals.retain_mut(|(_, row, _)| {
                            if setter_error.is_some() {
                                return false;
                            }
                            match self.apply_validated_setter(std::mem::take(row), setter) {
                                Ok((updated_row, changed)) => {
                                    *row = updated_row;
                                    changed
                                }
                                Err(e) => {
                                    setter_error = Some(e);
                                    false
                                }
                            }
                        });
                    }

                    // Abort before batch put if setter reported an error
                    if let Some(err) = setter_error {
                        return Err(err);
                    }

                    let update_count = local_rows_to_update.len() + rows_with_originals.len();

                    // Batch put - first the local rows (use regular put)
                    {
                        let mut txn_versions = self.txn_versions.write().unwrap();
                        txn_versions.put_batch_for_update(local_rows_to_update)?;
                        // Then the rows with originals (use optimized put)
                        txn_versions.put_batch_with_originals(rows_with_originals)?;
                    }
                    return Ok(update_count as i32);
                }
            }

            // Fall back to full scan - use batch fetch WITH original versions for O(1) lock
            // OPTIMIZATION: When WHERE clause exists, push filter to storage layer
            // This avoids allocating Row objects for non-matching rows
            let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                if let Some(expr) = where_expr {
                    // Use filtered scan - filter is applied BEFORE cloning rows
                    self.version_store
                        .get_all_visible_rows_for_update_filtered(self.txn_id, expr)?
                        .into_iter()
                        .map(|(row_id, row, orig)| {
                            // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
                            (row_id, self.normalize_row_to_schema(row, schema), orig)
                        })
                        .collect()
                } else {
                    // No filter - get all rows
                    self.version_store
                        .get_all_visible_rows_for_update(self.txn_id)
                        .into_iter()
                        .map(|(row_id, row, orig)| {
                            // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
                            (row_id, self.normalize_row_to_schema(row, schema), orig)
                        })
                        .collect()
                };

            // The first scan identifies statement candidates. Claim them in stable
            // row-id order, then re-read current committed versions before applying
            // the setter. This is required for read-modify-write expressions: a
            // waiter must not compute from payload captured before the owner commit.
            if !rows_with_originals.is_empty() {
                let candidate_ids: Vec<i64> = rows_with_originals
                    .iter()
                    .map(|(row_id, _, _)| *row_id)
                    .collect();
                self.txn_versions
                    .write()
                    .unwrap()
                    .claim_rows_for_update(&candidate_ids)?;
                rows_with_originals = self
                    .version_store
                    .get_visible_versions_for_update(&candidate_ids, self.txn_id)
                    .into_iter()
                    .filter_map(|(row_id, row, original)| {
                        let row = self.normalize_row_to_schema(row, schema);
                        match where_expr {
                            Some(expr) => match expr.evaluate(&row) {
                                Ok(true) => Some(Ok((row_id, row, original))),
                                Ok(false) => None,
                                Err(error) => Some(Err(error)),
                            },
                            None => Some(Ok((row_id, row, original))),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
            }

            // Overlay local transaction changes onto the global rows:
            // - Rows locally updated: use local data instead of stale global data
            // - Rows locally deleted: remove from update set
            // - Rows locally inserted (not in global store): add to update set
            let local_rows_to_update: RowVec = {
                let txn_versions = self.txn_versions.read().unwrap();
                let mut filter_error: Option<radixdb_core::Error> = None;
                if txn_versions.has_local_changes() {
                    // Fix up global rows that have local modifications
                    rows_with_originals.retain_mut(|(row_id, row, _orig)| {
                        if filter_error.is_some() {
                            return false;
                        }
                        if let Some(local_version) = txn_versions.get_latest_local(*row_id) {
                            if local_version.is_deleted() {
                                // Locally deleted — exclude from update
                                return false;
                            }
                            // Locally modified — use local data instead of stale global data
                            *row = self.normalize_row_to_schema(local_version.data.clone(), schema);
                            // Re-check filter against local data
                            if let Some(expr) = where_expr {
                                match expr.evaluate(row) {
                                    Ok(true) => {}
                                    Ok(false) => return false,
                                    Err(error) => {
                                        filter_error = Some(error);
                                        return false;
                                    }
                                }
                            }
                        }
                        true
                    });
                }

                // Collect local-only inserts (not in global store)
                let mut local_rows = RowVec::new();
                if filter_error.is_none() {
                    for (row_id, version) in txn_versions.iter_local() {
                        // Skip if already in global store (processed above)
                        if self.version_store.quick_check_row_existence(row_id) {
                            continue;
                        }
                        if version.is_deleted() {
                            continue;
                        }
                        let row = self.normalize_row_to_schema(version.data.clone(), schema);
                        if let Some(expr) = where_expr {
                            match expr.evaluate(&row) {
                                Ok(true) => {}
                                Ok(false) => continue,
                                Err(error) => {
                                    filter_error = Some(error);
                                    break;
                                }
                            }
                        }
                        local_rows.push((row_id, row));
                    }
                }
                if let Some(error) = filter_error {
                    return Err(error);
                }
                local_rows
            };

            // Apply setter to rows with originals (from version store), filtering out unchanged
            // Setter returns Result — on error, abort BEFORE batch put for statement atomicity.
            let mut setter_error: Option<radixdb_core::Error> = None;
            rows_with_originals.retain_mut(|(_, row, _)| {
                if setter_error.is_some() {
                    return false;
                }
                match self.apply_validated_setter(std::mem::take(row), setter) {
                    Ok((updated_row, changed)) => {
                        *row = updated_row;
                        changed
                    }
                    Err(e) => {
                        setter_error = Some(e);
                        false
                    }
                }
            });

            // Apply setter to local rows, filtering out unchanged
            let mut local_updated: RowVec = RowVec::new();
            if setter_error.is_none() {
                for (row_id, row) in local_rows_to_update {
                    match self.apply_validated_setter(row, setter) {
                        Ok((updated_row, changed)) => {
                            if changed {
                                local_updated.push((row_id, updated_row));
                            }
                        }
                        Err(e) => {
                            setter_error = Some(e);
                            break;
                        }
                    }
                }
            }

            // Abort before batch put if setter reported an error
            if let Some(err) = setter_error {
                return Err(err);
            }

            // Batch update all rows at once
            let update_count = rows_with_originals.len() + local_updated.len();
            {
                let mut txn_versions = self.txn_versions.write().unwrap();
                // Use optimized put for rows from version store (avoids O(N) get_visible_version calls)
                txn_versions.put_batch_with_originals(rows_with_originals)?;
                // Use regular put for local rows (already tracked in local store)
                txn_versions.put_batch_for_update(local_updated)?;
            }

            Ok(update_count as i32)
        })();
        if result.is_err() {
            self.txn_versions
                .write()
                .unwrap()
                .rollback_to_timestamp(statement_boundary);
        }
        result
    }

    fn update_by_row_ids(
        &mut self,
        row_ids: &[i64],
        setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32> {
        let statement_boundary = get_fast_timestamp();
        let result = (|| {
            let schema = &self.cached_schema;

            // Step 1: Check local versions first (single lock acquisition)
            // Use get_local_version to distinguish "no local version" from "locally deleted"
            let mut local_rows = RowVec::with_capacity(row_ids.len() / 4);
            let mut remaining_row_ids: Vec<i64> = Vec::with_capacity(row_ids.len());

            {
                let txn_versions = self.txn_versions.read().unwrap();
                for &row_id in row_ids {
                    if let Some(local) = txn_versions.get_local_version(row_id) {
                        if !local.is_deleted() {
                            let row = self.normalize_row_to_schema(local.data.clone(), schema);
                            let (updated_row, changed) =
                                self.apply_validated_setter(row, setter)?;
                            if changed {
                                local_rows.push((row_id, updated_row));
                            }
                        }
                        // Locally deleted — skip, don't fall through
                    } else {
                        remaining_row_ids.push(row_id);
                    }
                }
            }

            // Step 2: Batch fetch remaining from version store with original versions
            let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                Vec::with_capacity(remaining_row_ids.len());
            if !remaining_row_ids.is_empty() {
                self.txn_versions
                    .write()
                    .unwrap()
                    .claim_rows_for_update(&remaining_row_ids)?;
                let batch_rows = self
                    .version_store
                    .get_visible_versions_for_update(&remaining_row_ids, self.txn_id);
                for (row_id, row, version) in batch_rows {
                    let row = self.normalize_row_to_schema(row, schema);
                    let (updated_row, changed) = self.apply_validated_setter(row, setter)?;
                    if changed {
                        rows_with_originals.push((row_id, updated_row, version));
                    }
                }
            }

            // Step 3: Batch put all updates
            let update_count = (local_rows.len() + rows_with_originals.len()) as i32;
            if !local_rows.is_empty() || !rows_with_originals.is_empty() {
                let mut txn_versions = self.txn_versions.write().unwrap();
                if !local_rows.is_empty() {
                    txn_versions.put_batch_for_update(local_rows)?;
                }
                if !rows_with_originals.is_empty() {
                    txn_versions.put_batch_with_originals(rows_with_originals)?;
                }
            }

            Ok(update_count)
        })();
        if result.is_err() {
            self.txn_versions
                .write()
                .unwrap()
                .rollback_to_timestamp(statement_boundary);
        }
        result
    }

    fn delete_by_row_ids(&mut self, row_ids: &[i64]) -> Result<i32> {
        let mut deleted_row_ids = Vec::new();
        self.delete_candidate_row_ids_collect(row_ids, None, &mut deleted_row_ids)
    }

    fn delete_candidate_row_ids_collect(
        &mut self,
        row_ids: &[i64],
        _recheck_expr: Option<&dyn Expression>,
        deleted_row_ids: &mut Vec<i64>,
    ) -> Result<i32> {
        let statement_boundary = get_fast_timestamp();
        let initial_deleted_len = deleted_row_ids.len();
        let result = (|| {
            let schema = &self.cached_schema;

            // Step 1: Check local versions first
            // Use get_local_version to distinguish "no local version" from "locally deleted"
            let mut local_deletes = RowVec::with_capacity(row_ids.len() / 4);
            let mut remaining_row_ids: Vec<i64> = Vec::with_capacity(row_ids.len());

            {
                let txn_versions = self.txn_versions.read().unwrap();
                for &row_id in row_ids {
                    if let Some(local) = txn_versions.get_local_version(row_id) {
                        if !local.is_deleted() {
                            let row = self.normalize_row_to_schema(local.data.clone(), schema);
                            local_deletes.push((row_id, row));
                        }
                        // Already locally deleted — skip, don't fall through
                    } else {
                        remaining_row_ids.push(row_id);
                    }
                }
            }

            // Step 2: Claim and batch-fetch remaining rows from the hot version
            // store. A row ID absent from this store is not fabricated as a local
            // delete: segmented cold storage owns its tombstone/WAL contract.
            let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                Vec::with_capacity(remaining_row_ids.len());
            if !remaining_row_ids.is_empty() {
                self.txn_versions
                    .write()
                    .unwrap()
                    .claim_rows_for_update(&remaining_row_ids)?;
                let batch_rows = self
                    .version_store
                    .get_visible_versions_for_update(&remaining_row_ids, self.txn_id);
                for (row_id, row, version) in batch_rows {
                    let row = self.normalize_row_to_schema(row, schema);
                    rows_with_originals.push((row_id, row, version));
                }
            }

            // Step 3: Batch-delete rows that actually exist in this MVCC store.
            let delete_count = (local_deletes.len() + rows_with_originals.len()) as i32;
            deleted_row_ids.reserve(delete_count as usize);
            deleted_row_ids.extend(local_deletes.iter().map(|(row_id, _)| *row_id));
            deleted_row_ids.extend(rows_with_originals.iter().map(|(row_id, _, _)| *row_id));
            if !local_deletes.is_empty() || !rows_with_originals.is_empty() {
                let mut txn_versions = self.txn_versions.write().unwrap();
                for (row_id, row) in local_deletes {
                    txn_versions.put(row_id, row, true)?;
                }
                for (row_id, row, orig) in rows_with_originals {
                    txn_versions.put_with_original(row_id, row, orig, true)?;
                }
            }

            Ok(delete_count)
        })();
        if result.is_err() {
            self.txn_versions
                .write()
                .unwrap()
                .rollback_to_timestamp(statement_boundary);
            deleted_row_ids.truncate(initial_deleted_len);
        }
        result
    }

    fn get_active_row_ids(&self) -> Vec<i64> {
        self.version_store.get_all_row_ids()
    }

    fn collect_hot_row_ids_into(&self, dest: &mut rustc_hash::FxHashSet<i64>) {
        self.version_store.collect_row_ids_into(dest);
    }

    fn collect_shadow_row_ids_into(&self, dest: &mut rustc_hash::FxHashSet<i64>) {
        self.version_store
            .collect_visible_row_ids_into(self.txn_id, dest);
        let txn_versions = self.txn_versions.read().unwrap();
        for (row_id, _) in txn_versions.iter_local() {
            dest.insert(row_id);
        }
    }

    fn has_row_id(&self, row_id: i64) -> bool {
        self.version_store.has_committed_row(row_id)
    }

    fn membership_fence(&self) -> Option<Arc<parking_lot::RwLock<()>>> {
        Some(self.version_store.membership_fence())
    }

    fn probe_visible_row_ids_unfenced(
        &self,
        row_ids: &[i64],
        matches: &mut [bool],
    ) -> Result<usize> {
        if row_ids.len() != matches.len() {
            return Err(Error::invalid_argument(format!(
                "row ID probe output length mismatch: expected {}, got {}",
                row_ids.len(),
                matches.len()
            )));
        }

        // Keep the transaction-local snapshot stable while probing committed
        // metadata. This follows the established lock ordering used by
        // fetch_rows_by_ids_into: txn_versions first, then VersionStore.
        let txn_versions = self.txn_versions.read().unwrap();
        let mut count =
            self.version_store
                .probe_visible_row_ids_batch(row_ids, self.txn_id, matches);

        // Read-your-writes: the latest local version is authoritative for this
        // transaction. Inserts/updates make the ID visible; deletes hide it.
        for (position, &row_id) in row_ids.iter().enumerate() {
            let Some(local_version) = txn_versions.get_local_version(row_id) else {
                continue;
            };

            let local_visible = !local_version.is_deleted();
            if matches[position] != local_visible {
                if local_visible {
                    count += 1;
                } else {
                    count -= 1;
                }
                matches[position] = local_visible;
            }
        }

        Ok(count)
    }

    fn try_claim_row(&self, row_id: i64) -> Result<()> {
        self.version_store.try_claim_row(row_id, self.txn_id)?;
        // Track this claim in TransactionVersionStore's write_set so that
        // commit/rollback releases it. Without this, claims made directly
        // on VersionStore (for cold row UPDATE/DELETE) are never released
        // because TransactionVersionStore::commit() only drains write_set.
        if let Err(error) = self
            .txn_versions
            .write()
            .unwrap()
            .track_external_claim(row_id)
        {
            self.version_store.release_row_claim(row_id, self.txn_id);
            return Err(error);
        }
        Ok(())
    }

    fn try_claim_rows(&self, row_ids: &[i64]) -> Result<()> {
        self.txn_versions
            .write()
            .unwrap()
            .claim_rows_for_update(row_ids)
    }

    fn try_claim_rows_for_delete(&self, row_ids: &[i64]) -> Result<()> {
        self.txn_versions
            .write()
            .unwrap()
            .claim_rows_for_delete(row_ids)
    }

    fn delete(&mut self, where_expr: Option<&dyn Expression>) -> Result<i32> {
        let statement_boundary = get_fast_timestamp();
        let result = (|| {
            // OPTIMIZATION: Borrow schema instead of cloning - saves allocation per delete
            let schema = &self.cached_schema;

            // Fast path: Check if this is a primary key equality lookup (WHERE id = X)
            if let Some(expr) = where_expr {
                if let Some(pk_id) = self.try_pk_lookup(expr, schema) {
                    // Direct O(1) lookup by primary key
                    // Use get_local_version to preserve delete signal. txn_versions.get()
                    // swallows deletes as None, causing fallback to committed store and
                    // missing uncommitted deletes in the current transaction.
                    let row_with_original = {
                        let txn_versions = self.txn_versions.read().unwrap();
                        if let Some(local) = txn_versions.get_local_version(pk_id) {
                            if local.is_deleted() {
                                None // Already locally deleted — don't double-count
                            } else {
                                // Local version - no need to track original (already in write-set)
                                Some((local.data.clone(), None))
                            }
                        } else if let Some(version) =
                            self.version_store.get_visible_version(pk_id, self.txn_id)
                        {
                            if !version.is_deleted() {
                                let data = version.data.clone();
                                Some((data, Some(version)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };

                    if let Some((row, original_version)) = row_with_original {
                        if let Some(orig) = original_version {
                            // Use optimized put that skips redundant get_visible_version
                            self.txn_versions
                                .write()
                                .unwrap()
                                .put_with_original(pk_id, row, orig, true)?;
                        } else {
                            // Local version - use regular put
                            self.txn_versions.write().unwrap().put(pk_id, row, true)?;
                        }
                        return Ok(1);
                    }
                    return Ok(0);
                }

                // Fast path: PK range lookup (WHERE id >= X AND id < Y)
                // PK IS the row_id, so we can generate the range directly
                if let Some(pk_range_ids) = self.try_pk_range_lookup(expr, schema) {
                    // OPTIMIZATION: Track original versions to avoid redundant lookups
                    let mut local_rows = RowVec::with_capacity(pk_range_ids.len() / 4);
                    let mut rows_with_originals: Vec<(i64, Row, crate::mvcc::RowVersion)> =
                        Vec::with_capacity(pk_range_ids.len());

                    // Step 1: Check local versions first (single lock acquisition)
                    // Use get_local_version to distinguish "no local version" from "locally deleted"
                    let mut remaining_row_ids: Vec<i64> = Vec::with_capacity(pk_range_ids.len());
                    {
                        let txn_versions = self.txn_versions.read().unwrap();
                        for row_id in pk_range_ids {
                            if let Some(local) = txn_versions.get_local_version(row_id) {
                                if !local.is_deleted() {
                                    local_rows.push((row_id, local.data.clone()));
                                }
                                // Already locally deleted — skip, don't fall through
                            } else {
                                remaining_row_ids.push(row_id);
                            }
                        }
                    }

                    // Step 2: Batch fetch remaining from version store (1 lock for N rows)
                    if !remaining_row_ids.is_empty() {
                        let batch_rows = self
                            .version_store
                            .get_visible_versions_for_update(&remaining_row_ids, self.txn_id);
                        for (row_id, row, version) in batch_rows {
                            rows_with_originals.push((row_id, row, version));
                        }
                    }

                    let delete_count = (local_rows.len() + rows_with_originals.len()) as i32;
                    if !local_rows.is_empty() || !rows_with_originals.is_empty() {
                        let mut txn_versions = self.txn_versions.write().unwrap();
                        if !local_rows.is_empty() {
                            txn_versions.put_batch_deleted(local_rows)?;
                        }
                        if !rows_with_originals.is_empty() {
                            txn_versions.put_batch_deleted_with_originals(rows_with_originals)?;
                        }
                    }
                    return Ok(delete_count);
                }

                // See update(): local inserts and local index-key changes are not
                // committed index entries and require the merged fallback path.
                let indexed_row_ids = if self.txn_versions.read().unwrap().has_local_changes() {
                    None
                } else {
                    self.try_index_lookup(expr, schema)?
                };
                if let Some(filtered_row_ids) = indexed_row_ids {
                    // OPTIMIZATION: Batch collect rows to delete, then batch put
                    // This reduces lock contention from 2N locks to 2 locks for N rows
                    let mut rows_to_delete = RowVec::with_capacity(filtered_row_ids.len());

                    // Step 1: Check local versions first (single lock acquisition)
                    // Use get_local_version to distinguish "no local version" from "locally deleted"
                    let mut remaining_row_ids: Vec<i64> =
                        Vec::with_capacity(filtered_row_ids.len());
                    {
                        let txn_versions = self.txn_versions.read().unwrap();
                        for row_id in filtered_row_ids {
                            if let Some(local) = txn_versions.get_local_version(row_id) {
                                if !local.is_deleted() {
                                    let row = local.data.clone();
                                    // Re-apply filter (index may be partial match)
                                    if expr.evaluate(&row)? {
                                        rows_to_delete.push((row_id, row));
                                    }
                                }
                                // Already locally deleted — skip, don't fall through
                            } else {
                                remaining_row_ids.push(row_id);
                            }
                        }
                    }

                    // Step 2: Batch fetch remaining from version store (1 lock for N rows)
                    if !remaining_row_ids.is_empty() {
                        let batch_rows = self
                            .version_store
                            .get_visible_versions_batch(&remaining_row_ids, self.txn_id);
                        for (row_id, row) in batch_rows {
                            // Re-apply filter (index may be partial match)
                            if expr.evaluate(&row)? {
                                rows_to_delete.push((row_id, row));
                            }
                        }
                    }

                    // Single batch write for all deletes
                    let delete_count = rows_to_delete.len() as i32;
                    if !rows_to_delete.is_empty() {
                        self.txn_versions
                            .write()
                            .unwrap()
                            .put_batch_deleted(rows_to_delete)?;
                    }
                    return Ok(delete_count);
                }
            }

            // Fall back to full scan
            let mut delete_count = 0;

            // Get all visible rows
            let row_ids = self.version_store.get_all_row_ids();

            for row_id in row_ids {
                // OPTIMIZATION: Check filter BEFORE cloning to avoid wasted allocations
                // For DELETE with selective WHERE, this can save 90%+ of clones

                // First, check local versions
                // Use get_local_version to distinguish "no local version" from "locally deleted"
                let local_version = {
                    let txn_versions = self.txn_versions.read().unwrap();
                    txn_versions
                        .get_local_version(row_id)
                        .map(|v| (v.is_deleted(), v.data.clone()))
                };

                if let Some((is_deleted, row)) = local_version {
                    if is_deleted {
                        continue; // Already locally deleted — skip
                    }
                    // Apply filter on local row
                    if let Some(expr) = where_expr {
                        match expr.evaluate(&row) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(error) => return Err(error),
                        }
                    }
                    // Mark as deleted
                    self.txn_versions.write().unwrap().put(row_id, row, true)?;
                    delete_count += 1;
                } else if let Some(version) =
                    self.version_store.get_visible_version(row_id, self.txn_id)
                {
                    if version.is_deleted() {
                        continue;
                    }

                    // Apply filter BEFORE cloning - evaluate on reference
                    if let Some(expr) = where_expr {
                        match expr.evaluate(&version.data) {
                            Ok(true) => {}
                            Ok(false) => continue, // Skip clone entirely!
                            Err(error) => return Err(error),
                        }
                    }

                    // Only clone AFTER filter passes
                    self.txn_versions
                        .write()
                        .unwrap()
                        .put(row_id, version.data.clone(), true)?;
                    delete_count += 1;
                }
            }

            // Also check local inserts that might not be in global store
            let local_ids: Vec<i64> = {
                let txn_versions = self.txn_versions.read().unwrap();
                txn_versions.iter_local().map(|(id, _)| id).collect()
            };
            for row_id in local_ids {
                // Skip if already processed
                if self.version_store.quick_check_row_existence(row_id) {
                    continue;
                }

                let row = {
                    let txn_versions = self.txn_versions.read().unwrap();
                    txn_versions.get(row_id)
                };
                if let Some(row) = row {
                    // Apply filter
                    if let Some(expr) = where_expr {
                        match expr.evaluate(&row) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(error) => return Err(error),
                        }
                    }

                    // Row is already owned from txn_versions.get(), no extra clone needed
                    self.txn_versions.write().unwrap().put(row_id, row, true)?;
                    delete_count += 1;
                }
            }

            Ok(delete_count)
        })();
        if result.is_err() {
            self.txn_versions
                .write()
                .unwrap()
                .rollback_to_timestamp(statement_boundary);
        }
        result
    }

    fn truncate(&mut self) -> Result<i32> {
        // Fast path: drop all storage directly, bypassing per-row MVCC versioning.
        // This is O(1) instead of O(N) for delete-all.
        // Fails if other transactions have uncommitted writes on this table.
        self.version_store.truncate_all()
    }

    fn truncate_after(&mut self, before_clear: &mut dyn FnMut() -> Result<()>) -> Result<i32> {
        self.version_store.truncate_all_after(before_clear)
    }

    fn visit_visible_rows(&self, visitor: &mut dyn FnMut(i64, Row) -> Result<()>) -> Result<()> {
        if self.txn_versions.read().unwrap().has_local_changes()
            || self.logical_rows_need_normalization()
        {
            let projection: Vec<usize> = (0..self.cached_schema.columns.len()).collect();
            let mut scanner = self.scan(&projection, None)?;
            while scanner.next() {
                let (row_id, row) = scanner.take_row_with_id()?;
                visitor(row_id, row)?;
            }
            if let Some(error) = scanner.err().cloned() {
                let _ = scanner.close();
                return Err(error);
            }
            return scanner.close();
        }

        let mut callback_error = None;
        self.version_store
            .for_each_all_visible(self.txn_id, |row_id, row| {
                if let Err(error) = visitor(row_id, row) {
                    callback_error = Some(error);
                    false
                } else {
                    true
                }
            });
        callback_error.map_or(Ok(()), Err)
    }

    fn scan(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        // CompactArc clone - O(1) reference count increment instead of full Schema clone
        let schema = self.cached_schema.clone();

        // Fast path: Check if this is a primary key equality lookup (WHERE id = X)
        if let Some(expr) = where_expr {
            #[cfg(any(test, feature = "test-failpoints"))]
            let indexes_allowed = !crate::test_failpoints::decline_indexes();
            #[cfg(not(any(test, feature = "test-failpoints")))]
            let indexes_allowed = true;
            if let Some(pk_lookup) = indexes_allowed
                .then(|| self.try_pk_lookup(expr, &schema))
                .flatten()
            {
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::record_execution_path(0);
                // Direct O(1) lookup by primary key
                // Use get_local_version to preserve delete signal. txn_versions.get()
                // swallows deletes as None, causing fallback to committed store and
                // missing uncommitted deletes in the current transaction.
                let row = {
                    let txn_versions = self.txn_versions.read().unwrap();
                    if let Some(local) = txn_versions.get_local_version(pk_lookup) {
                        if local.is_deleted() {
                            None // Locally deleted in this transaction
                        } else {
                            Some(local.data.clone())
                        }
                    } else if let Some(version) = self
                        .version_store
                        .get_visible_version(pk_lookup, self.txn_id)
                    {
                        if !version.is_deleted() {
                            Some(version.data.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                if let Some(row) = row {
                    // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
                    let row = self.normalize_row_to_schema(row, &schema);
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((pk_lookup, row));
                    let scanner = MVCCScanner::from_rows(rows, schema, column_indices.to_vec());
                    return Ok(Box::new(scanner));
                } else {
                    // Row not found - return empty scanner
                    let scanner = MVCCScanner::empty(schema, column_indices.to_vec());
                    return Ok(Box::new(scanner));
                }
            }

            // Shared secondary indexes contain committed rows only. Merge the
            // small transaction-private row-id set into their candidates and
            // re-evaluate the complete predicate against authoritative local
            // versions. This preserves RYW without degrading to a table scan.
            if let Some(filtered_row_ids) = self.try_index_lookup(expr, &schema)? {
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::record_execution_path(1);
                let filtered_row_ids = self.merge_local_index_candidates(filtered_row_ids);
                let rows = self.fetch_rows_by_ids_checked(&filtered_row_ids, expr)?;
                let scanner = MVCCScanner::from_rows(rows, schema, column_indices.to_vec());
                return Ok(Box::new(scanner));
            }
        }

        // Fall back to full scan - use MVCCScanner with RowVec for cache reuse
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::record_execution_path(2);
        let rows = self.collect_visible_rows(where_expr)?;
        let scanner = MVCCScanner::from_rows(rows, schema, column_indices.to_vec());
        Ok(Box::new(scanner))
    }

    fn scan_exact_projection(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        // CompactArc clone - O(1) reference count increment instead of full Schema clone
        let schema = self.cached_schema.clone();

        // Fast path: Check if this is a primary key equality lookup (WHERE id = X)
        if let Some(expr) = where_expr {
            #[cfg(any(test, feature = "test-failpoints"))]
            let indexes_allowed = !crate::test_failpoints::decline_indexes();
            #[cfg(not(any(test, feature = "test-failpoints")))]
            let indexes_allowed = true;
            if let Some(pk_lookup) = indexes_allowed
                .then(|| self.try_pk_lookup(expr, &schema))
                .flatten()
            {
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::record_execution_path(0);
                let row = {
                    let txn_versions = self.txn_versions.read().unwrap();
                    if let Some(local) = txn_versions.get_local_version(pk_lookup) {
                        if local.is_deleted() {
                            None
                        } else {
                            Some(local.data.clone())
                        }
                    } else if let Some(version) = self
                        .version_store
                        .get_visible_version(pk_lookup, self.txn_id)
                    {
                        if !version.is_deleted() {
                            Some(version.data.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                if let Some(row) = row {
                    let row = self.normalize_row_to_schema(row, &schema);
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((pk_lookup, row));
                    let scanner = MVCCScanner::from_rows_exact_projection(
                        rows,
                        schema,
                        column_indices.to_vec(),
                    );
                    return Ok(Box::new(scanner));
                } else {
                    let scanner = MVCCScanner::empty(schema, column_indices.to_vec());
                    return Ok(Box::new(scanner));
                }
            }

            // See scan(): committed candidates plus transaction-private row IDs
            // form a complete RYW candidate set.
            if let Some(filtered_row_ids) = self.try_index_lookup(expr, &schema)? {
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::record_execution_path(1);
                let filtered_row_ids = self.merge_local_index_candidates(filtered_row_ids);
                let rows = self.fetch_rows_by_ids_checked(&filtered_row_ids, expr)?;
                let scanner =
                    MVCCScanner::from_rows_exact_projection(rows, schema, column_indices.to_vec());
                return Ok(Box::new(scanner));
            }
        }

        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::record_execution_path(2);
        let rows = self.collect_visible_rows(where_expr)?;
        let scanner =
            MVCCScanner::from_rows_exact_projection(rows, schema, column_indices.to_vec());
        Ok(Box::new(scanner))
    }

    fn scan_with_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        row_id_ranges: &[(i64, i64)],
    ) -> Result<Box<dyn Scanner>> {
        let Some(filter) = where_expr else {
            return self.scan(column_indices, None);
        };
        if self.txn_versions.read().unwrap().has_local_changes()
            || self.logical_rows_need_normalization()
        {
            return self.scan(column_indices, where_expr);
        }
        let schema = self.cached_schema.clone();
        if row_id_ranges.is_empty() {
            return Ok(Box::new(MVCCScanner::empty(
                schema,
                column_indices.to_vec(),
            )));
        }
        let rows = self.version_store.get_all_visible_rows_filtered_in_ranges(
            self.txn_id,
            filter,
            row_id_ranges,
        )?;
        Ok(Box::new(MVCCScanner::from_rows(
            rows,
            schema,
            column_indices.to_vec(),
        )))
    }

    fn scan_exact_projection_with_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        row_id_ranges: &[(i64, i64)],
    ) -> Result<Box<dyn Scanner>> {
        let Some(filter) = where_expr else {
            return self.scan_exact_projection(column_indices, None);
        };
        if self.txn_versions.read().unwrap().has_local_changes()
            || self.logical_rows_need_normalization()
        {
            return self.scan_exact_projection(column_indices, where_expr);
        }
        let schema = self.cached_schema.clone();
        if row_id_ranges.is_empty() {
            return Ok(Box::new(MVCCScanner::empty(
                schema,
                column_indices.to_vec(),
            )));
        }
        let rows = self.version_store.get_all_visible_rows_filtered_in_ranges(
            self.txn_id,
            filter,
            row_id_ranges,
        )?;
        Ok(Box::new(MVCCScanner::from_rows_exact_projection(
            rows,
            schema,
            column_indices.to_vec(),
        )))
    }

    fn collect_all_rows(&self, where_expr: Option<&dyn Expression>) -> Result<RowVec> {
        // Return cached row vector directly - caller iterates (i64, Row) tuples
        self.collect_visible_rows(where_expr)
    }

    fn collect_all_rows_unsorted(&self) -> Result<RowVec> {
        self.collect_visible_rows_unsorted()
    }

    fn collect_rows_by_ids(&self, row_ids: &[i64]) -> Result<RowVec> {
        let mut rows = RowVec::with_capacity(row_ids.len());
        let txn_versions = self.txn_versions.read().unwrap();
        let schema = &self.cached_schema;
        for &row_id in row_ids {
            // Read-your-writes is authoritative. A local update must replace
            // the committed/cold payload and a local delete must hide it.
            if let Some(local) = txn_versions.get_local_version(row_id) {
                if !local.is_deleted() {
                    rows.push((
                        row_id,
                        self.normalize_row_to_schema(local.data.clone(), schema),
                    ));
                }
                continue;
            }
            if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                if !version.is_deleted() {
                    rows.push((
                        row_id,
                        self.normalize_row_to_schema(version.data.clone(), schema),
                    ));
                }
            }
        }
        Ok(rows)
    }

    fn collect_rows_by_ids_projected(
        &self,
        row_ids: &[i64],
        column_indices: &[usize],
    ) -> Result<RowVec> {
        let txn_versions = self.txn_versions.read().unwrap();
        let schema = &self.cached_schema;
        let mut rows_by_position = vec![None; row_ids.len()];
        let mut global_row_ids = Vec::with_capacity(row_ids.len().min(4096));

        for (position, &row_id) in row_ids.iter().enumerate() {
            if let Some(version) = txn_versions.get_local_version(row_id) {
                if !version.is_deleted() {
                    let row = self
                        .normalize_row_to_schema(version.data.clone(), schema)
                        .take_columns(column_indices)?;
                    rows_by_position[position] = Some(row);
                }
            } else {
                global_row_ids.push(row_id);
            }
        }

        if !global_row_ids.is_empty() {
            let global_rows = self
                .version_store
                .get_visible_versions_batch(&global_row_ids, self.txn_id);
            let mut global_by_id = FxHashMap::default();
            for (row_id, row) in global_rows {
                let row = self
                    .normalize_row_to_schema(row, schema)
                    .take_columns(column_indices)?;
                global_by_id.insert(row_id, row);
            }
            for (position, &row_id) in row_ids.iter().enumerate() {
                if rows_by_position[position].is_none() {
                    rows_by_position[position] = global_by_id.get(&row_id).cloned();
                }
            }
        }

        let mut rows = RowVec::with_capacity(row_ids.len());
        for (&row_id, row) in row_ids.iter().zip(rows_by_position) {
            if let Some(row) = row {
                rows.push((row_id, row));
            }
        }
        Ok(rows)
    }

    fn collect_rows_with_limit(
        &self,
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        // Use the optimized version with limit/offset
        self.collect_visible_rows_with_limit(where_expr, limit, offset)
    }

    fn collect_rows_with_limit_unordered(
        &self,
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        // Use the optimized unordered version with true early termination
        self.collect_visible_rows_with_limit_unordered(where_expr, limit, offset)
    }

    fn collect_rows_with_limit_unordered_exact_projected(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        let rows = self.collect_visible_rows_with_limit_unordered(where_expr, limit, offset)?;
        Ok(MVCCScanner::project_rows_for_scan_exact(
            rows,
            self.cached_schema.columns.len(),
            column_indices,
        ))
    }

    fn collect_rows_sorted_with_limit(
        &self,
        sort_col_idx: usize,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Row>> {
        // DEFERRED MATERIALIZATION OPTIMIZATION
        // Instead of cloning all rows, sorting, and taking limit:
        // 1. Get row indices (no cloning)
        // 2. Load only sort column values
        // 3. Sort indices by values
        // 4. Take top N indices
        // 5. Materialize only N rows
        //
        // Performance: For 100K rows with 20 columns, LIMIT 10:
        // - Old: Clone 2M values, sort, take 10
        // - New: Load 100K sort values, sort indices, clone 200 values

        // Check for local versions - if present, fall back to default implementation
        let has_local = self.txn_versions.read().unwrap().has_local_changes();
        if has_local {
            // Local changes exist - use slower but correct path
            let mut rows = self.collect_visible_rows(None)?;
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
            return Ok(rows
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, row)| row)
                .collect());
        }

        // FAST PATH: Use deferred materialization from version_store
        let rows = self.version_store.get_visible_rows_sorted_limit(
            self.txn_id,
            sort_col_idx,
            ascending,
            limit,
            offset,
        );

        // Normalize rows to schema and return
        let schema = &self.cached_schema;
        Ok(rows
            .into_iter()
            .map(|(_, row)| self.normalize_row_to_schema(row, schema))
            .collect())
    }

    fn close(&mut self) -> Result<()> {
        // Rollback any uncommitted changes
        self.txn_versions.write().unwrap().rollback();
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        // Call inherent method which handles index updates
        MVCCTable::commit(self)
    }

    fn rollback(&mut self) {
        self.txn_versions.write().unwrap().rollback();
    }

    fn rollback_to_timestamp(&self, timestamp: i64) {
        self.txn_versions
            .write()
            .unwrap()
            .rollback_to_timestamp(timestamp);
    }

    fn has_local_changes(&self) -> bool {
        self.txn_versions.read().unwrap().has_local_changes()
    }

    fn get_pending_versions(&self) -> Vec<(i64, Row, bool, i64, i64)> {
        let txn_versions = self.txn_versions.read().unwrap();
        txn_versions
            .iter_local()
            .map(|(row_id, version)| {
                (
                    row_id,
                    version.data.clone(),
                    version.is_deleted(),
                    version.txn_id,
                    version.create_time,
                )
            })
            .collect()
    }

    fn create_index(&self, name: &str, columns: &[&str], is_unique: bool) -> Result<()> {
        // Delegate to create_index_with_type with auto-selection
        self.create_index_with_type(name, columns, is_unique, None)
    }

    fn create_index_with_type(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
    ) -> Result<()> {
        self.ensure_index_ddl_authorized()?;
        let index = self.build_index_with_optional_predicate(
            name, columns, is_unique, index_type, None, None,
        )?;
        self.publish_detached_index(index)
    }

    fn build_index_with_type_detached(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
    ) -> Result<Arc<dyn Index>> {
        self.ensure_index_ddl_authorized()?;
        self.build_index_with_optional_predicate(name, columns, is_unique, index_type, None, None)
    }

    fn publish_detached_index(&self, index: Arc<dyn Index>) -> Result<()> {
        self.ensure_index_ddl_authorized()?;
        let name = index.name().to_string();
        if let Some(existing) = self.version_store.get_index(&name) {
            if Arc::ptr_eq(&existing, &index) {
                return Ok(());
            }
            return Err(Error::IndexAlreadyExists(name));
        }
        self.version_store
            .add_index_for_schema(name, index, &self.cached_schema)
    }

    fn create_index_with_key_encoder(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: IndexType,
        encoder: crate::index::PreparedIndexKeyEncoder,
    ) -> Result<()> {
        self.ensure_index_ddl_authorized()?;
        let index = self.build_index_with_optional_predicate(
            name,
            columns,
            is_unique,
            Some(index_type),
            None,
            Some(encoder),
        )?;
        self.publish_detached_index(index)
    }

    fn create_partial_index_with_type(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
        predicate: PartialIndexPredicate,
    ) -> Result<()> {
        self.ensure_index_ddl_authorized()?;
        let index = self.build_index_with_optional_predicate(
            name,
            columns,
            is_unique,
            index_type,
            Some(predicate),
            None,
        )?;
        self.publish_detached_index(index)
    }

    fn create_hnsw_index(
        &self,
        name: &str,
        column: &str,
        is_unique: bool,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: crate::index::HnswDistanceMetric,
    ) -> Result<()> {
        self.ensure_index_ddl_authorized()?;
        // HNSW participates in the same transactional DDL contract as the
        // ordinary index builder and must therefore see a staged schema.
        let schema = &self.cached_schema;
        let (col_idx, col) = schema
            .find_column(column)
            .ok_or(Error::ColumnNotFound(column.to_string()))?;

        if col.data_type != DataType::Vector {
            return Err(Error::internal(format!(
                "HNSW index requires a VECTOR column, got {:?}",
                col.data_type
            )));
        }
        let dims = col.vector_dimensions as usize;
        if dims == 0 {
            return Err(Error::internal(
                "HNSW index requires a VECTOR column with specified dimensions".to_string(),
            ));
        }

        // Check for duplicate
        if self.version_store.index_exists(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }
        if let Some(existing_idx) = self.version_store.get_index_by_column(column) {
            if existing_idx.index_type() == IndexType::PrimaryKey {
                return Ok(());
            }
            return Err(Error::internal(format!(
                "an index already exists on column '{}'",
                column
            )));
        }

        let mut hnsw = HnswIndex::new(
            name.to_string(),
            self.name().to_string(),
            col.name.clone(),
            col.id as i32,
            dims,
            m,
            ef_construction,
            ef_search,
            metric,
        )?;
        hnsw.set_unique(is_unique)?;
        let index: Arc<dyn Index> = Arc::new(hnsw);

        // Populate from the merged transaction view so an UPDATE made before
        // CREATE INDEX is represented by its local version rather than by the
        // previous committed payload.
        let mut entries: Vec<(i64, Vec<Value>)> = Vec::new();
        for (row_id, row) in self.collect_visible_rows(None)? {
            let val = row
                .get(col_idx)
                .cloned()
                .unwrap_or(Value::Null(DataType::Null));
            entries.push((row_id, vec![val]));
        }
        if !entries.is_empty() {
            let entry_refs: Vec<(i64, &[Value])> = entries
                .iter()
                .map(|(row_id, values)| (*row_id, values.as_slice()))
                .collect();
            index.add_batch_slice(&entry_refs)?;
        }

        self.version_store
            .add_index_for_schema(name.to_string(), index, &self.cached_schema)
    }

    fn drop_index(&self, name: &str) -> Result<()> {
        // Check if index exists
        if !self.version_store.index_exists(name) {
            return Err(Error::IndexNotFound(name.to_string()));
        }

        // Remove from version store
        self.version_store.remove_index(name);
        Ok(())
    }

    fn rename_index(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.version_store.rename_index(old_name, new_name)
    }

    fn has_index_on_column(&self, column_name: &str) -> bool {
        self.get_full_index_by_column(column_name).is_some()
    }

    fn get_index_on_column(&self, column_name: &str) -> Option<std::sync::Arc<dyn Index>> {
        self.get_full_index_by_column(column_name)
    }

    fn collect_row_ids_by_index_values(
        &self,
        column_name: &str,
        values: &[Value],
    ) -> Option<Result<Vec<i64>>> {
        let index = self.get_full_index_by_column(column_name)?;
        let (column_index, column) = self.cached_schema.find_column(column_name)?;
        if values.is_empty() {
            // Empty input is also the capability probe used by segmented JOIN
            // planning. It must never expose transaction-local row IDs as if
            // they had matched an actual key.
            return Some(Ok(Vec::new()));
        }
        let cross_numeric = crate::expression::in_list::has_cross_numeric_physical_variant(
            column.data_type,
            values,
        );
        let probe_values: Vec<Value> = values
            .iter()
            .map(|value| {
                if cross_numeric {
                    value.clone()
                } else {
                    value.coerce_to_type(column.data_type)
                }
            })
            .filter(|value| !value.is_null())
            .collect();

        // The shared index represents current committed keys. It is not a
        // complete candidate source for a historical snapshot after another
        // transaction changes an indexed key. Cross-numeric probes likewise
        // cannot be coerced into one physical index representation without
        // changing SQL equality. Keep the selected lookup edge, but resolve
        // these uncommon cases from the authoritative transaction view.
        if self.version_store.needs_snapshot_isolation(self.txn_id) || cross_numeric {
            return Some((|| {
                let mut row_ids = Vec::new();
                for (row_id, row) in self.collect_visible_rows(None)? {
                    if row.get(column_index).is_some_and(|actual| {
                        !actual.is_null() && probe_values.iter().any(|probe| actual == probe)
                    }) {
                        row_ids.push(row_id);
                    }
                }
                row_ids.sort_unstable();
                row_ids.dedup();
                Ok(row_ids)
            })());
        }

        let mut row_ids = Vec::new();
        for value in &probe_values {
            if let Err(error) =
                index.get_row_ids_equal_into(std::slice::from_ref(value), &mut row_ids)
            {
                return Some(Err(error));
            }
        }

        // Shared secondary indexes are published only at commit. Add the
        // transaction-private delta as candidates; the lookup operator fetches
        // the private version and rechecks the projected key, which removes
        // local deletes and old-key entries while admitting inserts/key moves.
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            row_ids.extend(txn_versions.iter_local().map(|(row_id, _)| row_id));
        }
        drop(txn_versions);
        row_ids.sort_unstable();
        row_ids.dedup();
        Some(Ok(row_ids))
    }

    fn get_index(&self, name: &str) -> Option<std::sync::Arc<dyn Index>> {
        self.version_store.get_index(name)
    }

    fn get_unique_indexes(&self) -> Vec<(String, Vec<String>)> {
        self.version_store
            .get_all_indexes()
            .into_iter()
            .filter(|idx| idx.is_unique())
            .map(|idx| (idx.name().to_string(), idx.column_names().to_vec()))
            .collect()
    }

    fn get_indexes(&self) -> Vec<std::sync::Arc<dyn Index>> {
        self.version_store.get_all_indexes().into_iter().collect()
    }

    fn for_each_unique_non_pk_index(
        &self,
        f: &mut dyn FnMut(&str, &[String]) -> Result<()>,
    ) -> Result<()> {
        let pk_col = self
            .cached_schema
            .pk_column_index()
            .map(|i| &self.cached_schema.columns[i].name_lower);
        let indexes = self.version_store.indexes_read();
        for idx in indexes.values() {
            if !idx.is_unique() {
                continue;
            }
            let names = idx.column_names();
            // Skip single-column indexes that match the PK column
            if names.len() == 1 {
                if let Some(pk) = pk_col {
                    if names[0].eq_ignore_ascii_case(pk) {
                        continue;
                    }
                }
            }
            f(idx.name(), names)?;
        }
        Ok(())
    }

    fn get_unique_non_pk_indexes(&self) -> Vec<std::sync::Arc<dyn Index>> {
        let pk_col = self
            .cached_schema
            .pk_column_index()
            .map(|i| &self.cached_schema.columns[i].name_lower);
        let indexes = self.version_store.indexes_read();
        indexes
            .values()
            .filter(|idx| {
                if !idx.is_unique() {
                    return false;
                }
                let names = idx.column_names();
                if names.len() == 1 {
                    if let Some(pk) = pk_col {
                        return !names[0].eq_ignore_ascii_case(pk);
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    fn has_unique_non_pk_indexes(&self) -> bool {
        let pk_col = self
            .cached_schema
            .pk_column_index()
            .map(|i| &self.cached_schema.columns[i].name_lower);
        let indexes = self.version_store.indexes_read();
        indexes.values().any(|idx| {
            if !idx.is_unique() {
                return false;
            }
            let names = idx.column_names();
            if names.len() == 1 {
                if let Some(pk) = pk_col {
                    return !names[0].eq_ignore_ascii_case(pk);
                }
            }
            true
        })
    }

    fn get_multi_column_index(
        &self,
        predicate_columns: &[&str],
    ) -> Option<(std::sync::Arc<dyn Index>, usize)> {
        self.get_full_multi_column_index(predicate_columns)
    }

    fn create_btree_index(
        &self,
        column_name: &str,
        is_unique: bool,
        custom_name: Option<&str>,
    ) -> Result<()> {
        let index_name = custom_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("idx_{}_{}_btree", self.name(), column_name));
        self.create_index_with_type(
            &index_name,
            &[column_name],
            is_unique,
            Some(IndexType::BTree),
        )
    }

    /// Create a multi-column index using MultiColumnIndex
    fn create_multi_column_index(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
    ) -> Result<()> {
        self.create_index_with_type(name, columns, is_unique, Some(IndexType::MultiColumn))
    }

    fn drop_btree_index(&self, column_name: &str) -> Result<()> {
        // Generate default index name
        let index_name = format!("idx_{}_{}_btree", self.name(), column_name);

        // Check if index exists
        if !self.version_store.index_exists(&index_name) {
            return Err(Error::internal(format!(
                "btree index not found for column: {}",
                column_name
            )));
        }

        // Remove from version store
        self.version_store.remove_index(&index_name);
        Ok(())
    }

    fn get_index_min_value(&self, column_name: &str) -> Option<Value> {
        // Try to find an index on this column and get its minimum value
        if let Some(index) = self.get_full_index_by_column(column_name) {
            return index.get_min_value();
        }
        None
    }

    fn get_index_max_value(&self, column_name: &str) -> Option<Value> {
        // Try to find an index on this column and get its maximum value
        if let Some(index) = self.get_full_index_by_column(column_name) {
            return index.get_max_value();
        }
        None
    }

    fn row_count(&self) -> usize {
        // Try O(1) fast path first
        if let Some(count) = MVCCTable::fast_row_count(self) {
            return count;
        }
        // Fall back to optimized single-pass counting
        MVCCTable::row_count(self)
    }

    fn row_count_hint(&self) -> usize {
        // O(1) - just return the committed row count without any lock checks
        self.version_store.committed_row_count()
    }

    fn fast_row_count(&self) -> Option<usize> {
        MVCCTable::fast_row_count(self)
    }

    fn collect_rows_ordered_by_index(
        &self,
        column_name: &str,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<RowVec> {
        // The global index reflects current committed keys. Under snapshot
        // isolation those keys are not a valid candidate order for historical
        // row versions, so use the visibility-aware scan + sort path instead.
        if self.version_store.needs_snapshot_isolation(self.txn_id) {
            return None;
        }

        // If transaction has local changes (inserts/updates/deletes), fall back to
        // the regular path which correctly merges local changes via collect_visible_rows.
        // This optimization only reads from the global version store and indexes.
        {
            let txn_versions = self.txn_versions.read().unwrap();
            if txn_versions.has_local_changes() {
                return None;
            }
        }

        // OPTIMIZATION: Handle PRIMARY KEY column specially
        // For INTEGER PRIMARY KEY, the row_id IS the value, so we can iterate
        // directly in order without any sorting using skip/take semantics.
        if let Some(pk_idx) = self.cached_schema.pk_column_index() {
            let pk_col = &self.cached_schema.columns[pk_idx];
            if pk_col.name_lower == column_name.to_lowercase() {
                return self.version_store.collect_rows_pk_ordered(
                    self.txn_id,
                    ascending,
                    limit,
                    offset,
                );
            }
        }

        // Check if column has an index
        let index = self.get_full_index_by_column(column_name)?;

        // Try using the efficient ordered iteration method (available in B-tree indexes)
        // We request more row IDs than needed to account for invisible rows
        let batch_size = (limit + offset) * 2 + 100; // Request extra to handle filtered rows

        if let Some(ordered_row_ids) = index.get_row_ids_ordered(ascending, batch_size, 0) {
            // Fast path: B-tree index supports ordered iteration
            let mut rows = RowVec::with_capacity(limit.min(100));
            let mut skipped = 0;

            for row_id in ordered_row_ids {
                // Check visibility and get row
                if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                    if version.is_deleted() {
                        continue;
                    }

                    // Handle offset
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }

                    rows.push((row_id, version.data.clone()));

                    // Check if we've reached the limit
                    if rows.len() >= limit {
                        return Some(rows);
                    }
                }
            }

            // If we got all needed rows, return them
            // If not, we may need to fetch more (rare case with many invisible rows)
            if !rows.is_empty() {
                return Some(rows);
            }
        }

        // Fallback path: Get all values and sort (for non-B-tree indexes)
        let all_values = index.get_all_values();

        // If the index doesn't support get_all_values (returns empty), return None
        // to let the regular query execution path handle ORDER BY + LIMIT
        if all_values.is_empty() {
            return None;
        }

        // Sort values using partial_cmp (Value implements PartialOrd)
        let mut sorted_values = all_values;
        sorted_values.sort_by(|a, b| {
            if ascending {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
            }
        });

        // Collect rows by iterating through sorted values
        let mut rows = RowVec::with_capacity(limit.min(100));
        let mut skipped = 0;
        let mut row_ids = Vec::new();

        for value in sorted_values {
            // Get all row IDs for this value - reuse buffer to avoid allocation per iteration
            row_ids.clear();
            index.get_row_ids_equal_into(&[value], &mut row_ids).ok()?;

            for row_id in &row_ids {
                let row_id = *row_id;
                // Check visibility and get row
                if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                    if version.is_deleted() {
                        continue;
                    }

                    // Handle offset
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }

                    rows.push((row_id, version.data.clone()));

                    // Check if we've reached the limit
                    if rows.len() >= limit {
                        return Some(rows);
                    }
                }
            }
        }

        Some(rows)
    }

    fn collect_rows_composite_ordered_range(
        &self,
        where_expr: &dyn Expression,
        order_column: &str,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<Result<RowVec>> {
        if self.txn_versions.read().unwrap().has_local_changes() {
            return None;
        }
        let plan = self.plan_composite_index_lookup(where_expr, where_expr)?;
        if plan.range.is_none()
            || !plan
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
        let entries = plan.lookup_ordered_limited(ascending, target)?;
        let mut rows = RowVec::with_capacity(limit.min(entries.len()));
        let mut skipped = 0usize;
        for entry in entries {
            let Some(version) = self
                .version_store
                .get_visible_version(entry.row_id, self.txn_id)
            else {
                continue;
            };
            if version.is_deleted() || !where_expr.evaluate_fast(&version.data) {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            rows.push((entry.row_id, version.data.clone()));
            if rows.len() == limit {
                break;
            }
        }
        Some(Ok(rows))
    }

    fn collect_rows_pk_keyset(
        &self,
        start_after: Option<i64>,
        start_from: Option<i64>,
        ascending: bool,
        limit: usize,
    ) -> Option<RowVec> {
        // Only works if table has a single-column INTEGER PRIMARY KEY
        self.cached_schema.pk_column_index()?;

        // If transaction has local changes, fall back to regular path
        {
            let txn_versions = self.txn_versions.read().unwrap();
            if txn_versions.has_local_changes() {
                return None;
            }
        }

        // Use the efficient keyset iteration from version store
        // Returns RowVec with (row_id, Row) tuples
        Some(self.version_store.collect_rows_keyset(
            self.txn_id,
            start_after,
            start_from,
            ascending,
            limit,
        ))
    }

    fn collect_rows_grouped_by_partition(&self, column_name: &str) -> Option<Vec<(Value, RowVec)>> {
        // If transaction has local changes, fall back to regular path
        {
            let txn_versions = self.txn_versions.read().unwrap();
            if txn_versions.has_local_changes() {
                return None;
            }
        }

        // Check if column has an index
        let index = self.get_full_index_by_column(column_name)?;

        // Get all unique values from the index (partition keys)
        let all_values = index.get_all_values();
        if all_values.is_empty() {
            return Some(Vec::new());
        }

        // Collect rows grouped by partition value
        let mut result: Vec<(Value, RowVec)> = Vec::with_capacity(all_values.len());
        let mut row_ids = Vec::new();

        for partition_value in all_values {
            // Get all row IDs for this partition value - reuse buffer to avoid allocation per iteration
            row_ids.clear();
            index
                .get_row_ids_equal_into(std::slice::from_ref(&partition_value), &mut row_ids)
                .ok()?;

            // Collect visible rows for this partition
            let mut partition_rows = RowVec::with_capacity(row_ids.len());
            for &row_id in &row_ids {
                if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                    if !version.is_deleted() {
                        partition_rows.push((row_id, version.data.clone()));
                    }
                }
            }

            if !partition_rows.is_empty() {
                result.push((partition_value, partition_rows));
            }
        }

        Some(result)
    }

    fn get_partition_values(&self, column_name: &str) -> Option<Vec<Value>> {
        // Only use index-based distinct values if no uncommitted local changes
        // (local changes are in txn_versions, not reflected in the index)
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        // Get index for the column
        let index = self.get_full_index_by_column(column_name)?;
        // Return all distinct values from the index
        Some(index.get_all_values())
    }

    fn get_partition_count(&self, column_name: &str) -> Option<usize> {
        // Only use index-based count if no uncommitted local changes
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        // Get index for the column
        let index = self.get_full_index_by_column(column_name)?;
        // Return count of distinct non-null values without cloning
        index.get_distinct_count_excluding_null()
    }

    fn get_rows_for_partition_value(
        &self,
        column_name: &str,
        partition_value: &Value,
    ) -> Option<RowVec> {
        // If transaction has local changes, fall back to regular path
        {
            let txn_versions = self.txn_versions.read().unwrap();
            if txn_versions.has_local_changes() {
                return None;
            }
        }

        // Get index for the column
        let index = self.get_full_index_by_column(column_name)?;

        // Get row IDs for this partition value
        let row_ids = index
            .get_row_ids_equal(std::slice::from_ref(partition_value))
            .ok()?;

        // Collect visible rows for this partition
        let mut rows = RowVec::with_capacity(row_ids.len());
        for &row_id in row_ids.iter() {
            if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                if !version.is_deleted() {
                    rows.push((row_id, version.data.clone()));
                }
            }
        }

        Some(rows)
    }

    fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        // Rename column in both version store and cached schema
        {
            let mut schema_guard = self.version_store.schema_mut();
            CompactArc::make_mut(&mut *schema_guard).rename_column(old_name, new_name)?;
        }
        CompactArc::make_mut(&mut self.cached_schema).rename_column(old_name, new_name)?;
        Ok(())
    }

    fn modify_column(&mut self, name: &str, column_type: DataType, nullable: bool) -> Result<()> {
        // Modify column in both version store and cached schema
        {
            let mut schema_guard = self.version_store.schema_mut();
            CompactArc::make_mut(&mut *schema_guard).modify_column(
                name,
                Some(column_type),
                Some(nullable),
            )?;
        }
        CompactArc::make_mut(&mut self.cached_schema).modify_column(
            name,
            Some(column_type),
            Some(nullable),
        )?;
        Ok(())
    }

    fn modify_column_with_default(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
        default_value: Option<Value>,
    ) -> Result<()> {
        // Modify column in both version store and cached schema, then replace
        // the default metadata. Existing rows are not rewritten: the default is
        // used for subsequent INSERT/DEFAULT evaluation, matching the current
        // RadixDB schema-evolution contract for MODIFY COLUMN.
        {
            let mut schema_guard = self.version_store.schema_mut();
            let schema = CompactArc::make_mut(&mut *schema_guard);
            schema.modify_column(name, Some(column_type), Some(nullable))?;
            schema.set_column_default(name, default_expr.clone(), default_value.clone())?;
        }
        let schema = CompactArc::make_mut(&mut self.cached_schema);
        schema.modify_column(name, Some(column_type), Some(nullable))?;
        schema.set_column_default(name, default_expr, default_value)?;
        Ok(())
    }

    fn select(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn QueryResult>> {
        // Convert column names to indices
        let column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|name| self.cached_schema.find_column(name).map(|(idx, _)| idx))
            .collect();

        // Scan and collect results
        let mut scanner = self.scan(&column_indices, expr)?;
        let mut rows = Vec::new();

        while scanner.next() {
            rows.push(scanner.take_row());
        }

        scanner.close()?;

        // Create result
        let result_columns: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let result = MemoryResult::with_rows(result_columns, rows);

        Ok(Box::new(result))
    }

    fn select_with_aliases(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
        aliases: &FxHashMap<String, String>,
    ) -> Result<Box<dyn QueryResult>> {
        // Get base result
        let result = self.select(columns, expr)?;

        // Apply aliases
        Ok(result.with_aliases(aliases.clone()))
    }

    fn select_as_of(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
        temporal_type: &str,
        temporal_value: i64,
    ) -> Result<Box<dyn QueryResult>> {
        // Convert column names to indices
        let column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|name| self.cached_schema.find_column(name).map(|(idx, _)| idx))
            .collect();

        // Get all row IDs
        let row_ids = self.version_store.get_all_row_ids();

        // Collect temporal rows
        let mut rows = Vec::new();
        for row_id in row_ids {
            let version = match temporal_type.to_uppercase().as_str() {
                "TRANSACTION" => self
                    .version_store
                    .get_visible_version_as_of_transaction(row_id, temporal_value),
                "TIMESTAMP" => self
                    .version_store
                    .get_visible_version_as_of_timestamp(row_id, temporal_value),
                _ => {
                    return Err(Error::internal(format!(
                        "unsupported temporal type: {}",
                        temporal_type
                    )))
                }
            };

            if let Some(v) = version {
                if !v.is_deleted() {
                    // Apply filter
                    if let Some(e) = expr {
                        match e.evaluate(&v.data) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(error) => return Err(error),
                        }
                    }

                    // Project columns
                    let projected: Vec<Value> = column_indices
                        .iter()
                        .map(|&idx| v.data.get(idx).cloned().unwrap_or(Value::null_unknown()))
                        .collect();
                    rows.push(Row::from_values(projected));
                }
            }
        }

        // Create result
        let result_columns: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let result = MemoryResult::with_rows(result_columns, rows);

        Ok(Box::new(result))
    }

    fn explain_scan(&self, where_expr: Option<&dyn Expression>) -> ScanPlan {
        use radixdb_core::Operator;

        let table_name = self.cached_schema.table_name.clone();
        let schema = &self.cached_schema;

        // No WHERE clause - always Seq Scan
        let Some(expr) = where_expr else {
            return ScanPlan::SeqScan {
                table: table_name,
                filter: None,
            };
        };

        // Check for PK lookup
        let pk_indices = schema.primary_key_indices();
        if pk_indices.len() == 1 {
            let pk_col_idx = pk_indices[0];
            let pk_col = &schema.columns[pk_col_idx];

            if pk_col.data_type == DataType::Integer {
                if let Some((col_name, operator, value)) = expr.get_comparison_info() {
                    if col_name.eq_ignore_ascii_case(&pk_col.name) && operator == Operator::Eq {
                        return ScanPlan::PkLookup {
                            table: table_name,
                            pk_column: pk_col.name.clone(),
                            pk_value: format!("{}", value),
                        };
                    }
                }
            }
        }

        // Check for single column index lookup
        if let Some((col_name, operator, value)) = expr.get_comparison_info() {
            // Skip boolean index (low cardinality)
            if !matches!(value, Value::Boolean(_))
                || !matches!(operator, Operator::Eq | Operator::Ne)
            {
                if let Some(index) = self.get_index_by_column_for_query(col_name, expr) {
                    let condition = format!("{} {}", operator_to_string(operator), value);
                    return ScanPlan::IndexScan {
                        table: table_name,
                        index_name: index.name().to_string(),
                        column: col_name.to_string(),
                        condition,
                        filter: None,
                    };
                }
            }
        }

        // Check for LIKE prefix pattern
        if let Some((col_name, prefix, negated)) = expr.get_like_prefix_info() {
            if !negated {
                if let Some(index) = self.get_index_by_column_for_query(col_name, expr) {
                    return ScanPlan::IndexScan {
                        table: table_name,
                        index_name: index.name().to_string(),
                        column: col_name.to_string(),
                        condition: format!("LIKE '{}%'", prefix),
                        filter: None,
                    };
                }
            }
        }

        // Check for OR expressions (union of indexes)
        if let Some(or_operands) = expr.get_or_operands() {
            let mut indexed_info: Vec<(String, String, String)> = Vec::new();
            let mut residual_parts = Vec::new();
            let mut non_indexed_parts = Vec::new();

            for operand in or_operands {
                if let Some((info, residuals)) = self.describe_indexed_or_branch(operand.as_ref()) {
                    indexed_info.push(info);
                    residual_parts.extend(residuals);
                } else {
                    non_indexed_parts.push(format!("{:?}", operand));
                }
            }

            if !indexed_info.is_empty() {
                let all_indexed = non_indexed_parts.is_empty();
                if all_indexed {
                    if indexed_info.len() == 1 {
                        let (idx_name, col, cond) = indexed_info.into_iter().next().unwrap();
                        return ScanPlan::IndexScan {
                            table: table_name,
                            index_name: idx_name,
                            column: col,
                            condition: cond,
                            filter: (!residual_parts.is_empty()).then(|| residual_parts.join("; ")),
                        };
                    }
                    return ScanPlan::MultiIndexScan {
                        table: table_name,
                        indexes: indexed_info,
                        operation: "OR".to_string(),
                        filter: (!residual_parts.is_empty()).then(|| residual_parts.join("; ")),
                    };
                }
                // Mixed OR: some operands indexed, some not — hybrid scan
                // Show as Multi-Index Scan with a Filter for the non-indexed operands
                residual_parts.extend(non_indexed_parts);
                return ScanPlan::MultiIndexScan {
                    table: table_name,
                    indexes: indexed_info,
                    operation: "OR".to_string(),
                    filter: Some(residual_parts.join(" OR ")),
                };
            }
        }

        // Check for multi-column (composite) index before single-column index intersection
        let comparisons = expr.collect_comparisons();
        if let Some(plan) = self.plan_composite_index_lookup(expr, expr) {
            let mut residual: Vec<String> = comparisons
                .iter()
                .filter(|(column, _, _)| !plan.covered_columns.contains(*column))
                .map(|(column, operator, value)| {
                    format!("{} {} {}", column, operator_to_string(*operator), value)
                })
                .collect();
            residual.extend(expr.collect_null_check_infos().into_iter().map(
                |(column, is_null)| {
                    format!("{} IS {}NULL", column, if is_null { "" } else { "NOT " })
                },
            ));
            return ScanPlan::CompositeIndexScan {
                table: table_name,
                index_name: plan.index.name().to_string(),
                columns: plan.columns,
                conditions: plan.conditions,
                filter: (!residual.is_empty()).then(|| residual.join(" AND ")),
            };
        }

        // Check for AND expressions (intersection of indexes)
        if !comparisons.is_empty() {
            let mut indexed_info: Vec<(String, String, String)> = Vec::new();

            // Re-group by column for single-column index checks
            let mut column_conditions: FxHashMap<&str, Vec<(Operator, &Value)>> =
                FxHashMap::default();
            for (col_name, op, val) in &comparisons {
                column_conditions
                    .entry(*col_name)
                    .or_default()
                    .push((*op, *val));
            }

            let mut indexed_columns: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for (col_name, ops) in &column_conditions {
                if let Some(index) = self.get_index_by_column_for_query(col_name, expr) {
                    // Simplify to a single condition string
                    let condition = if ops.len() == 1 {
                        let (op, val) = ops[0];
                        format!("{} {}", operator_to_string(op), val)
                    } else {
                        // Multiple conditions on same column (e.g., col >= 5 AND col <= 10)
                        let parts: Vec<String> = ops
                            .iter()
                            .map(|(op, val)| format!("{} {}", operator_to_string(*op), val))
                            .collect();
                        parts.join(" AND ")
                    };
                    indexed_info.push((index.name().to_string(), col_name.to_string(), condition));
                    indexed_columns.insert(col_name);
                }
            }

            if !indexed_info.is_empty() {
                // Collect residual predicates for non-indexed columns
                let residual: Vec<String> = column_conditions
                    .iter()
                    .filter(|(col, _)| !indexed_columns.contains(*col))
                    .map(|(col, ops)| {
                        ops.iter()
                            .map(|(op, val)| format!("{} {} {}", col, operator_to_string(*op), val))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    })
                    .collect();
                let filter = if residual.is_empty() {
                    None
                } else {
                    Some(residual.join(" AND "))
                };

                if indexed_info.len() == 1 {
                    let (idx_name, col, cond) = indexed_info.into_iter().next().unwrap();
                    return ScanPlan::IndexScan {
                        table: table_name,
                        index_name: idx_name,
                        column: col,
                        condition: cond,
                        filter,
                    };
                }
                return ScanPlan::MultiIndexScan {
                    table: table_name,
                    indexes: indexed_info,
                    operation: "AND".to_string(),
                    filter,
                };
            }
        }

        // Default: Seq Scan with filter
        ScanPlan::SeqScan {
            table: table_name,
            filter: Some(format!("{:?}", expr)),
        }
    }

    fn set_zone_maps(&self, zone_maps: crate::volume::zonemap::TableZoneMap) {
        self.version_store.set_zone_maps(zone_maps);
    }

    fn zone_map_generation(&self) -> u64 {
        self.version_store.zone_map_generation()
    }

    fn get_zone_maps(&self) -> Option<std::sync::Arc<crate::volume::zonemap::TableZoneMap>> {
        self.version_store.get_zone_maps()
    }

    fn get_segments_to_scan(
        &self,
        column: &str,
        operator: radixdb_core::Operator,
        value: &Value,
    ) -> Option<Vec<u32>> {
        self.version_store
            .get_segments_to_scan(column, operator, value)
    }

    fn sum_column(&self, col_idx: usize) -> Option<DeferredSum> {
        // Only use deferred aggregation if no uncommitted local changes
        // (local changes are in txn_versions, not the main version_store)
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        Some(self.version_store.sum_column(self.txn_id, col_idx))
    }

    fn min_column(&self, col_idx: usize) -> Option<Option<Value>> {
        // Only use deferred aggregation if no uncommitted local changes
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        Some(self.version_store.min_column(self.txn_id, col_idx))
    }

    fn max_column(&self, col_idx: usize) -> Option<Option<Value>> {
        // Only use deferred aggregation if no uncommitted local changes
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        Some(self.version_store.max_column(self.txn_id, col_idx))
    }

    fn compute_grouped_aggregates(
        &self,
        group_by_indices: &[usize],
        aggregates: &[(crate::mvcc::version_store::AggregateOp, usize)],
    ) -> Option<Vec<crate::mvcc::version_store::GroupedAggregateResult>> {
        // Only use storage-level aggregation if no uncommitted local changes
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        drop(txn_versions);

        self.version_store
            .compute_grouped_aggregates(self.txn_id, group_by_indices, aggregates)
    }
}
