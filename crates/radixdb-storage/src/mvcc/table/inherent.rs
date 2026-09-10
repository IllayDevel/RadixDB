use super::*;

impl MVCCTable {
    pub(super) fn ensure_index_ddl_authorized(&self) -> Result<()> {
        if self.allow_direct_commit || self.txn_versions.read().unwrap().index_ddl_authorized() {
            Ok(())
        } else {
            Err(Error::NotSupported(
                "engine-managed indexes must be created through transactional CREATE INDEX"
                    .to_string(),
            ))
        }
    }

    /// Creates a new MVCC table with an owned transaction version store
    /// (wraps it in Arc<RwLock> internally)
    #[cfg(test)]
    pub(crate) fn new(
        txn_id: i64,
        version_store: Arc<VersionStore>,
        txn_versions: TransactionVersionStore,
    ) -> Self {
        // CompactArc clone - O(1) reference count increment, not full schema clone
        let cached_schema = version_store.schema().clone();
        Self {
            txn_id,
            version_store,
            txn_versions: Arc::new(RwLock::new(txn_versions)),
            cached_schema,
            allow_direct_commit: true,
        }
    }

    /// Creates a new MVCC table with a shared transaction version store
    /// (used by the engine's get_table_for_transaction to share stores)
    pub(crate) fn new_with_shared_store(
        txn_id: i64,
        version_store: Arc<VersionStore>,
        txn_versions: Arc<RwLock<TransactionVersionStore>>,
    ) -> Self {
        Self::new_with_shared_store_and_schema(txn_id, version_store, txn_versions, None)
    }

    /// Creates a table handle using a transaction-private schema overlay.
    ///
    /// Row versions remain in the ordinary transaction-local store; only
    /// planning, row normalization and subsequent DML see the staged schema.
    pub(crate) fn new_with_shared_store_and_schema(
        txn_id: i64,
        version_store: Arc<VersionStore>,
        txn_versions: Arc<RwLock<TransactionVersionStore>>,
        schema_override: Option<CompactArc<Schema>>,
    ) -> Self {
        // CompactArc clone - O(1) reference count increment, not full schema clone.
        let cached_schema = schema_override.unwrap_or_else(|| version_store.schema().clone());
        Self {
            txn_id,
            version_store,
            txn_versions,
            cached_schema,
            allow_direct_commit: false,
        }
    }

    /// Auto-selects the optimal index type based on column data types
    ///
    /// # Type-Based Index Selection Rules:
    /// - TEXT/JSON columns → Hash index (avoids O(strlen) comparisons per B-tree node)
    /// - BOOLEAN columns → Bitmap index (only 2 values, fast AND/OR operations)
    /// - INTEGER/FLOAT/TIMESTAMP/UUID columns → BTree index (supports range queries)
    /// - Mixed types → BTree as safe default
    ///
    /// For multi-column indexes, the first column's type determines the index type
    /// unless there's a BOOLEAN (which always gets Bitmap for AND/OR optimization).
    pub(super) fn auto_select_index_type(data_types: &[DataType]) -> IndexType {
        if data_types.is_empty() {
            return IndexType::BTree;
        }

        // Check if any column is BOOLEAN - use Bitmap for fast AND/OR
        let has_boolean = data_types.iter().any(|dt| matches!(dt, DataType::Boolean));
        if has_boolean && data_types.len() == 1 {
            return IndexType::Bitmap;
        }

        // Check the primary (first) column type
        match data_types[0] {
            // TEXT/JSON/BYTES - use Hash for O(1) lookups, avoid O(strlen) comparisons
            DataType::Text | DataType::Json | DataType::Bytes => IndexType::Hash,

            // BOOLEAN - use Bitmap for fast AND/OR/NOT operations
            DataType::Boolean => IndexType::Bitmap,

            // Numeric/time/UUID types - use BTree for range query support and UUIDv7 locality
            DataType::Integer
            | DataType::Float
            | DataType::Timestamp
            | DataType::Uuid
            | DataType::Decimal
            | DataType::Date => IndexType::BTree,

            // Vector columns - use HNSW for approximate nearest neighbor search
            DataType::Vector => IndexType::Hnsw,

            // NULL type - use BTree as safe default
            DataType::Null => IndexType::BTree,
        }
    }

    pub(super) fn build_index_with_optional_predicate(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
        partial_predicate: Option<PartialIndexPredicate>,
        key_encoder: Option<crate::index::PreparedIndexKeyEncoder>,
    ) -> Result<Arc<dyn Index>> {
        if columns.is_empty() {
            return Err(Error::internal("index must have at least one column"));
        }

        // Index DDL is prepared before transaction publication.  When the same
        // transaction staged ALTER TABLE ADD COLUMN, the shared VersionStore
        // still exposes the catalog schema while this table owns the private
        // schema overlay.  Resolve columns against the table view, not the
        // published store.
        let schema = &self.cached_schema;
        let is_partial = partial_predicate.is_some();

        // Collect column info.
        let mut column_names = Vec::with_capacity(columns.len());
        let mut column_ids = Vec::with_capacity(columns.len());
        let mut data_types = Vec::with_capacity(columns.len());

        for col_name in columns {
            let (_, col) = schema
                .find_column(col_name)
                .ok_or(Error::ColumnNotFound(col_name.to_string()))?;
            column_names.push(col.name.clone());
            column_ids.push(col.id as i32);
            data_types.push(col.data_type);
        }

        // Determine index type: use explicit type or auto-select based on column types.
        // For multi-column indexes, always use MultiColumn (hash+btree hybrid).
        let chosen_type = if columns.len() > 1 {
            IndexType::MultiColumn
        } else {
            index_type.unwrap_or_else(|| Self::auto_select_index_type(&data_types))
        };

        if key_encoder.is_some() && columns.len() != 1 {
            return Err(Error::invalid_argument(
                "encoded operator-class indexes require one column",
            ));
        }

        let logical_data_types = data_types.clone();
        if let Some(encoder) = &key_encoder {
            data_types[0] = encoder.physical_data_type();
        }

        if is_partial && chosen_type == IndexType::Hnsw {
            return Err(Error::invalid_argument(
                "partial HNSW indexes are not supported",
            ));
        }

        // Check if index with same name already exists.
        if self.version_store.index_exists(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }

        // Full indexes keep the historical duplicate-column guard. Partial indexes
        // are intentionally allowed to coexist with full indexes and with other
        // partial indexes on the same columns, because the predicate is part of
        // the logical index identity.
        if !is_partial {
            if columns.len() == 1 {
                if let Some(existing_idx) = self.version_store.get_index_by_column(columns[0]) {
                    if existing_idx.partial_predicate().is_some() {
                        // A partial index on this column does not cover all rows,
                        // so it must not block creating the full index.
                    } else {
                        // If the existing index is a PkIndex, silently skip -
                        // the PK column is already covered.
                        if existing_idx.index_type() == IndexType::PrimaryKey {
                            return Ok(existing_idx);
                        }
                        if is_unique && !existing_idx.is_unique() {
                            return Err(Error::internal(format!(
                                "cannot create unique index on column '{}': a non-unique index already exists",
                                columns[0]
                            )));
                        } else if !is_unique && existing_idx.is_unique() {
                            return Err(Error::internal(format!(
                                "cannot create non-unique index on column '{}': a unique index already exists",
                                columns[0]
                            )));
                        }
                        return Err(Error::internal(format!(
                            "an index already exists on column '{}'",
                            columns[0]
                        )));
                    }
                }
            } else {
                for existing_idx in self.version_store.get_all_indexes() {
                    if existing_idx.partial_predicate().is_some() {
                        continue;
                    }

                    let existing_cols = existing_idx.column_names();
                    if existing_cols.len() == columns.len() {
                        let same_cols = existing_cols
                            .iter()
                            .zip(columns.iter())
                            .all(|(a, b)| a == *b);
                        if same_cols {
                            if is_unique && !existing_idx.is_unique() {
                                return Err(Error::internal(format!(
                                    "cannot create unique index on columns {:?}: a non-unique index already exists",
                                    columns
                                )));
                            } else if !is_unique && existing_idx.is_unique() {
                                return Err(Error::internal(format!(
                                    "cannot create non-unique index on columns {:?}: a unique index already exists",
                                    columns
                                )));
                            }
                            return Err(Error::internal(format!(
                                "an index already exists on columns {:?}",
                                columns
                            )));
                        }
                    }
                }
            }
        }

        let expected_rows = self.version_store.row_count();

        let inner_index: Arc<dyn Index> = match chosen_type {
            IndexType::Hash => Arc::new(HashIndex::new(
                name.to_string(),
                self.version_store.table_name().to_string(),
                column_names,
                column_ids,
                data_types,
                is_unique,
                expected_rows,
            )),
            IndexType::Bitmap => Arc::new(BitmapIndex::new(
                name.to_string(),
                self.version_store.table_name().to_string(),
                column_names,
                column_ids,
                data_types,
                is_unique,
                expected_rows,
            )),
            IndexType::BTree => {
                if columns.len() == 1 {
                    Arc::new(BTreeIndex::new(
                        name.to_string(),
                        self.version_store.table_name().to_string(),
                        column_ids[0],
                        column_names[0].clone(),
                        data_types[0],
                        is_unique,
                        expected_rows,
                    ))
                } else {
                    Arc::new(MultiColumnIndex::new(
                        name.to_string(),
                        self.version_store.table_name().to_string(),
                        column_names,
                        column_ids,
                        data_types,
                        is_unique,
                        expected_rows,
                    ))
                }
            }
            IndexType::MultiColumn => Arc::new(MultiColumnIndex::new(
                name.to_string(),
                self.version_store.table_name().to_string(),
                column_names,
                column_ids,
                data_types,
                is_unique,
                expected_rows,
            )),
            IndexType::PrimaryKey => {
                return Err(Error::internal(
                    "cannot explicitly create a primary key index; use PRIMARY KEY constraint instead"
                        .to_string(),
                ));
            }
            IndexType::Hnsw => {
                if columns.len() != 1 {
                    return Err(Error::internal(
                        "HNSW index must be on a single vector column".to_string(),
                    ));
                }
                if data_types[0] != DataType::Vector {
                    return Err(Error::internal(format!(
                        "HNSW index requires a VECTOR column, got {:?}",
                        data_types[0]
                    )));
                }
                let (_, col) = schema
                    .find_column(columns[0])
                    .ok_or(Error::ColumnNotFound(columns[0].to_string()))?;
                let dims = col.vector_dimensions as usize;
                if dims == 0 {
                    return Err(Error::internal(
                        "HNSW index requires a VECTOR column with specified dimensions".to_string(),
                    ));
                }
                let default_m = crate::index::default_m_for_dims(dims);
                let mut hnsw = HnswIndex::new(
                    name.to_string(),
                    self.version_store.table_name().to_string(),
                    column_names[0].clone(),
                    column_ids[0],
                    dims,
                    default_m,
                    crate::index::default_ef_construction(default_m),
                    crate::index::default_ef_search(default_m),
                    crate::index::HnswDistanceMetric::L2,
                )?;
                hnsw.set_unique(is_unique)?;
                Arc::new(hnsw)
            }
        };

        let inner_index: Arc<dyn Index> = if let Some(encoder) = key_encoder {
            Arc::new(crate::index::EncodedIndex::new(
                inner_index,
                logical_data_types,
                encoder,
            )?)
        } else {
            inner_index
        };

        let index: Arc<dyn Index> = if let Some(predicate) = partial_predicate {
            Arc::new(PartialIndex::new(inner_index, predicate))
        } else {
            inner_index
        };

        // Populate from the complete transaction-visible row set.  This merges
        // committed rows with local INSERT/UPDATE/DELETE versions and normalizes
        // older rows to the private schema overlay.  Building from the raw
        // VersionStore here would both miss an UPDATE performed earlier in the
        // transaction and address staged columns against the old row layout.
        let mut entries: Vec<(i64, Vec<Value>)> = Vec::new();
        for (row_id, row) in self.collect_visible_rows(None)? {
            if let Some(values) =
                crate::mvcc::version_store::index_values_for_row(index.as_ref(), &row)?
            {
                entries.push((row_id, values));
            }
        }

        if !entries.is_empty() {
            let entry_refs: Vec<(i64, &[Value])> = entries
                .iter()
                .map(|(row_id, values)| (*row_id, values.as_slice()))
                .collect();
            index.add_batch_slice(&entry_refs)?;
        }

        Ok(index)
    }

    /// Normalize a row to match the current schema
    ///
    /// This handles schema evolution (ALTER TABLE ADD/DROP COLUMN):
    /// - If row has fewer columns than schema, append default values (or NULLs) for missing columns
    /// - If row has more columns than schema, truncate the row
    pub(super) fn normalize_row_to_schema(&self, mut row: Row, schema: &Schema) -> Row {
        let schema_cols = schema.columns.len();
        let row_cols = row.len();

        if row_cols < schema_cols {
            // Row has fewer columns - add default values (or NULLs) for new columns
            for i in row_cols..schema_cols {
                let col = &schema.columns[i];
                // Use pre-computed default value if available, otherwise use NULL
                if let Some(ref default_val) = col.default_value {
                    row.push(default_val.clone());
                } else {
                    row.push(Value::null(col.data_type));
                }
            }
        } else if row_cols > schema_cols {
            // Row has more columns - truncate (columns were dropped)
            row.truncate(schema_cols);
        }

        row
    }

    #[inline]
    pub(super) fn logical_rows_need_normalization(&self) -> bool {
        self.version_store.requires_row_normalization()
            || self.cached_schema.columns.len() != self.version_store.schema().columns.len()
    }

    /// Try to extract a primary key lookup from the expression
    ///
    /// Returns Some(row_id) if the expression is a simple equality on the PK column
    pub(super) fn try_pk_lookup(&self, expr: &dyn Expression, schema: &Schema) -> Option<i64> {
        use radixdb_core::Operator;

        // Get PK column info
        let pk_indices = schema.primary_key_indices();
        if pk_indices.len() != 1 {
            return None; // Only support single-column PK for now
        }
        let pk_col_idx = pk_indices[0];
        let pk_col = &schema.columns[pk_col_idx];
        if pk_col.data_type != DataType::Integer {
            return None;
        }

        // Use the new get_comparison_info method (no downcasting required)
        let (col_name, operator, value) = expr.get_comparison_info()?;

        // Check if it's an equality on the PK column (case-insensitive comparison)
        if !col_name.eq_ignore_ascii_case(&pk_col.name) || operator != Operator::Eq {
            return None;
        }

        // Get the integer value (PKs are always integers in our system)
        match value {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to identify a PK range lookup (WHERE id >= X AND id < Y)
    /// Returns Some(RowIdVec) if this is a PK range query (pooled for efficient memory reuse)
    pub(super) fn try_pk_range_lookup(
        &self,
        expr: &dyn Expression,
        schema: &Schema,
    ) -> Option<RowIdVec> {
        use radixdb_core::Operator;

        // Get PK column info
        let pk_indices = schema.primary_key_indices();
        if pk_indices.len() != 1 {
            return None;
        }
        let pk_col_idx = pk_indices[0];
        if schema.columns[pk_col_idx].data_type != DataType::Integer {
            return None;
        }
        let pk_col_name = &schema.columns[pk_col_idx].name;

        // Check for AND with two comparisons on PK
        let and_operands = expr.get_and_operands()?;
        if and_operands.len() != 2 {
            return None;
        }

        let (info1, info2) = (
            and_operands[0].get_comparison_info(),
            and_operands[1].get_comparison_info(),
        );

        let ((col1, op1, val1), (col2, op2, val2)) = match (info1, info2) {
            (Some(a), Some(b)) => (a, b),
            _ => return None,
        };

        // Both must be on PK column
        if !col1.eq_ignore_ascii_case(pk_col_name) || !col2.eq_ignore_ascii_case(pk_col_name) {
            return None;
        }

        // Extract integer values
        let (v1, v2) = match (val1, val2) {
            (Value::Integer(a), Value::Integer(b)) => (*a, *b),
            _ => return None,
        };

        // Identify lower and upper bounds
        let (min_id, min_inclusive, max_id, max_inclusive) = match (op1, op2) {
            (Operator::Gte, Operator::Lt) => (v1, true, v2, false),
            (Operator::Gte, Operator::Lte) => (v1, true, v2, true),
            (Operator::Gt, Operator::Lt) => (v1, false, v2, false),
            (Operator::Gt, Operator::Lte) => (v1, false, v2, true),
            (Operator::Lt, Operator::Gte) => (v2, true, v1, false),
            (Operator::Lt, Operator::Gt) => (v2, false, v1, false),
            (Operator::Lte, Operator::Gte) => (v2, true, v1, true),
            (Operator::Lte, Operator::Gt) => (v2, false, v1, true),
            _ => return None,
        };

        // Adjust for inclusive/exclusive bounds using saturating arithmetic to prevent overflow
        let start = if min_inclusive {
            min_id
        } else {
            min_id.saturating_add(1)
        };
        let end = if max_inclusive {
            max_id
        } else {
            max_id.saturating_sub(1)
        };

        // Check for invalid range (start > end means empty result)
        if start > end {
            return Some(RowIdVec::new());
        }

        // SAFETY: Limit range size to prevent memory explosion
        // For ranges larger than threshold, return None to fall back to index/full scan
        const MAX_PK_RANGE_SIZE: i64 = 100_000;
        let range_size = end.saturating_sub(start).saturating_add(1);
        if range_size > MAX_PK_RANGE_SIZE {
            return None; // Fall back to index scan or full scan
        }

        // Generate row_ids directly (no index needed - PK IS the row_id)
        let mut row_ids = RowIdVec::with_capacity(range_size as usize);
        for id in start..=end {
            row_ids.push(id);
        }
        Some(row_ids)
    }

    pub(super) fn query_can_use_index(
        index: &dyn Index,
        implication_expr: &dyn Expression,
    ) -> bool {
        index
            .partial_predicate()
            .is_none_or(|predicate| predicate.is_implied_by(implication_expr))
    }

    pub(super) fn get_full_index_by_column(&self, column_name: &str) -> Option<Arc<dyn Index>> {
        let indexes = self.version_store.indexes_read();
        for index in indexes.values() {
            let column_names = index.column_names();
            if index.index_type() != IndexType::Hnsw
                && index.partial_predicate().is_none()
                && column_names.len() == 1
                && column_names[0] == column_name
            {
                return Some(index.clone());
            }
        }
        None
    }

    pub(super) fn get_full_multi_column_index(
        &self,
        predicate_columns: &[&str],
    ) -> Option<(Arc<dyn Index>, usize)> {
        if predicate_columns.is_empty() {
            return None;
        }

        let indexes = self.version_store.indexes_read();
        let pred_set: FxHashSet<&str> = predicate_columns.iter().copied().collect();
        let mut best: Option<(Arc<dyn Index>, usize)> = None;

        for index in indexes.values() {
            if index.partial_predicate().is_some() {
                continue;
            }
            let index_columns = index.column_names();
            if index_columns.len() < 2 {
                continue;
            }

            let mut matched = 0;
            for idx_col in index_columns.iter() {
                if pred_set.contains(idx_col.as_str()) {
                    matched += 1;
                } else {
                    break;
                }
            }
            if matched >= 1
                && best
                    .as_ref()
                    .is_none_or(|(_, best_match)| matched > *best_match)
            {
                best = Some((index.clone(), matched));
            }
        }

        best
    }

    pub(super) fn get_index_by_column_for_query(
        &self,
        column_name: &str,
        implication_expr: &dyn Expression,
    ) -> Option<Arc<dyn Index>> {
        let indexes = self.version_store.indexes_read();
        let mut implied_partial: Option<Arc<dyn Index>> = None;
        for index in indexes.values() {
            let column_names = index.column_names();
            if index.index_type() == IndexType::Hnsw
                || column_names.len() != 1
                || column_names[0] != column_name
            {
                continue;
            }
            if index.partial_predicate().is_none() {
                return Some(index.clone());
            }
            if implied_partial.is_none()
                && Self::query_can_use_index(index.as_ref(), implication_expr)
            {
                implied_partial = Some(index.clone());
            }
        }
        implied_partial
    }

    pub(super) fn get_multi_column_index_for_query(
        &self,
        predicate_columns: &[&str],
        implication_expr: &dyn Expression,
    ) -> Option<(Arc<dyn Index>, usize)> {
        if predicate_columns.is_empty() {
            return None;
        }

        let indexes = self.version_store.indexes_read();
        let pred_set: FxHashSet<&str> = predicate_columns.iter().copied().collect();
        let mut best_full: Option<(Arc<dyn Index>, usize)> = None;
        let mut best_partial: Option<(Arc<dyn Index>, usize)> = None;

        for index in indexes.values() {
            let index_columns = index.column_names();
            if index_columns.len() < 2 {
                continue;
            }

            let mut matched = 0;
            for idx_col in index_columns.iter() {
                if pred_set.contains(idx_col.as_str()) {
                    matched += 1;
                } else {
                    break;
                }
            }
            if matched < 1 {
                continue;
            }

            if index.partial_predicate().is_none() {
                if best_full.as_ref().is_none_or(|(_, best)| matched > *best) {
                    best_full = Some((index.clone(), matched));
                }
            } else if Self::query_can_use_index(index.as_ref(), implication_expr)
                && best_partial
                    .as_ref()
                    .is_none_or(|(_, best)| matched > *best)
            {
                best_partial = Some((index.clone(), matched));
            }
        }

        best_partial.or(best_full)
    }

    pub(super) fn plan_composite_index_lookup(
        &self,
        expr: &dyn Expression,
        implication_expr: &dyn Expression,
    ) -> Option<CompositeIndexLookupPlan> {
        use radixdb_core::Operator;

        let comparisons = expr.collect_comparisons();
        if comparisons.is_empty() {
            return None;
        }

        let mut column_comparisons: FxHashMap<&str, Vec<(Operator, &Value)>> = FxHashMap::default();
        for (column, operator, value) in &comparisons {
            column_comparisons
                .entry(*column)
                .or_default()
                .push((*operator, *value));
        }

        let predicate_columns: Vec<&str> = column_comparisons.keys().copied().collect();
        let (index, _) =
            self.get_multi_column_index_for_query(&predicate_columns, implication_expr)?;
        let index_columns = index.column_names();

        let mut equality_values = Vec::with_capacity(index_columns.len());
        let mut columns = Vec::with_capacity(index_columns.len());
        let mut conditions = Vec::with_capacity(index_columns.len());
        let mut covered_columns = FxHashSet::default();

        for index_column in index_columns {
            let Some(ops) = column_comparisons.get(index_column.as_str()) else {
                break;
            };
            let Some((_, value)) = ops.iter().find(|(op, _)| *op == Operator::Eq) else {
                break;
            };
            equality_values.push((*value).clone());
            columns.push(index_column.clone());
            conditions.push(format!("= {value}"));
            covered_columns.insert(index_column.clone());
        }

        let equality_prefix_len = equality_values.len();
        let range = index_columns
            .get(equality_prefix_len)
            .and_then(|column| {
                column_comparisons
                    .get(column.as_str())
                    .map(|ops| (column, ops))
            })
            .and_then(|(column, ops)| {
                let mut min: Option<(Value, bool)> = None;
                let mut max: Option<(Value, bool)> = None;
                let mut range_conditions = Vec::new();
                for (operator, value) in ops {
                    match operator {
                        Operator::Gt => min = Some(((*value).clone(), false)),
                        Operator::Gte => min = Some(((*value).clone(), true)),
                        Operator::Lt => max = Some(((*value).clone(), false)),
                        Operator::Lte => max = Some(((*value).clone(), true)),
                        _ => continue,
                    }
                    range_conditions.push(format!("{} {}", operator_to_string(*operator), value));
                }
                if min.is_none() && max.is_none() {
                    return None;
                }
                columns.push(column.clone());
                conditions.push(range_conditions.join(" AND "));
                covered_columns.insert(column.clone());
                Some(CompositeRangeBounds { min, max })
            });

        if equality_values.is_empty() && range.is_none() {
            return None;
        }

        Some(CompositeIndexLookupPlan {
            index,
            equality_values,
            range,
            columns,
            conditions,
            covered_columns,
        })
    }

    pub(super) fn describe_indexed_or_branch(
        &self,
        expr: &dyn Expression,
    ) -> Option<((String, String, String), Vec<String>)> {
        use radixdb_core::Operator;

        if let Some((column, operator, value)) = expr.get_comparison_info() {
            if matches!(value, Value::Boolean(_)) && matches!(operator, Operator::Eq | Operator::Ne)
            {
                return None;
            }
            let index = self.get_index_by_column_for_query(column, expr)?;
            return Some((
                (
                    index.name().to_string(),
                    column.to_string(),
                    format!("{} {}", operator_to_string(operator), value),
                ),
                Vec::new(),
            ));
        }

        let plan = self.plan_composite_index_lookup(expr, expr)?;
        let comparisons = expr.collect_comparisons();
        let mut residuals: Vec<String> = comparisons
            .iter()
            .filter(|(column, _, _)| !plan.covered_columns.contains(*column))
            .map(|(column, operator, value)| {
                format!("{} {} {}", column, operator_to_string(*operator), value)
            })
            .collect();
        residuals.extend(
            expr.collect_null_check_infos()
                .into_iter()
                .map(|(column, is_null)| {
                    format!("{} IS {}NULL", column, if is_null { "" } else { "NOT " })
                }),
        );

        Some((
            (
                plan.index.name().to_string(),
                format!("({})", plan.columns.join(", ")),
                plan.conditions.join(" AND "),
            ),
            residuals,
        ))
    }

    /// Try to use an index to filter row IDs
    ///
    /// Returns Some(row_ids) if an index can be used, None otherwise
    /// Returns a pooled RowIdVec for efficient memory reuse.
    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn try_index_lookup(
        &self,
        expr: &dyn Expression,
        schema: &Schema,
    ) -> Result<Option<RowIdVec>> {
        #[cfg(any(test, feature = "test-failpoints"))]
        if crate::test_failpoints::decline_indexes() {
            return Ok(None);
        }
        self.try_index_lookup_with_context(expr, expr, schema)
    }

    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn try_index_lookup_with_context(
        &self,
        expr: &dyn Expression,
        implication_expr: &dyn Expression,
        schema: &Schema,
    ) -> Result<Option<RowIdVec>> {
        use crate::index::intersect_sorted_ids;
        use radixdb_core::Operator;

        // First, try simple comparison on a single column
        if let Some((col_name, operator, value)) = expr.get_comparison_info() {
            // Skip index for boolean equality - low cardinality (2 values) means ~50% selectivity
            // which makes full scan faster than index lookup + row fetch
            if matches!(value, Value::Boolean(_)) && matches!(operator, Operator::Eq | Operator::Ne)
            {
                return Ok(None);
            }

            if let Some(index) = self.get_index_by_column_for_query(col_name, implication_expr) {
                return self.query_index_with_operator(&*index, operator, value);
            }
        }

        // OPTIMIZATION: Handle OR expressions with HYBRID index optimization
        // For (indexed_col = 'a' OR non_indexed_col = 'b'):
        // - Use index for indexed_col operands
        // - Return None only if ALL operands are non-indexed (full scan needed)
        // - If at least one operand uses index but others don't, we still return
        //   the indexed row_ids (the executor will handle memory filtering for others)
        if let Some(or_operands) = expr.get_or_operands() {
            let mut indexed_row_ids: Vec<RowIdVec> = Vec::with_capacity(or_operands.len());
            let mut has_unindexed_operand = false;

            for operand in or_operands {
                // Recursively try index lookup for each OR operand
                if let Some(row_ids) =
                    self.try_index_lookup_with_context(operand.as_ref(), operand.as_ref(), schema)?
                {
                    indexed_row_ids.push(row_ids);
                } else {
                    // This operand can't use an index
                    has_unindexed_operand = true;
                }
            }

            // HYBRID OPTIMIZATION: If some operands use indexes but not all,
            // we can't use pure index lookup (would miss rows from unindexed operands).
            // For now, fall back to full scan - the memory filter will handle it.
            // Future: Could return indexed row_ids + flag for partial optimization
            if has_unindexed_operand || indexed_row_ids.is_empty() {
                return Ok(None);
            }

            // All operands indexed - return the union of all row IDs
            if indexed_row_ids.len() == 1 {
                return Ok(Some(indexed_row_ids.into_iter().next().unwrap()));
            }

            // Bitset union: build a bitset for deduplication, collect unique row IDs
            // Cap at 131072 words (~1MB) to prevent OOM with large/sparse row IDs.
            // Negative row IDs (from user-specified PKs) cannot use the bitset path
            // because `id as usize` wraps to a huge value.
            const MAX_BITSET_WORDS: usize = 131_072;
            let mut max_id = 0i64;
            let mut min_id = 0i64;
            for ids in &indexed_row_ids {
                for &id in ids.iter() {
                    if id > max_id {
                        max_id = id;
                    }
                    if id < min_id {
                        min_id = id;
                    }
                }
            }
            let num_words = (max_id as usize / 64) + 1;
            let total_len: usize = indexed_row_ids.iter().map(|v| v.len()).sum();
            if min_id >= 0 && num_words <= MAX_BITSET_WORDS {
                // Fast bitset path
                let mut bits = vec![0u64; num_words];
                let mut result = RowIdVec::with_capacity(total_len);
                for ids in &indexed_row_ids {
                    for &id in ids.iter() {
                        let idx = id as usize;
                        let word = idx / 64;
                        let mask = 1u64 << (idx % 64);
                        if (bits[word] & mask) == 0 {
                            bits[word] |= mask;
                            result.push(id);
                        }
                    }
                }
                return Ok(Some(result));
            } else {
                // Fallback: I64Set-based union for large/sparse row IDs
                let mut seen = I64Set::with_capacity(total_len);
                let mut has_i64_min = false;
                let mut result = RowIdVec::with_capacity(total_len);
                for ids in &indexed_row_ids {
                    for &id in ids.iter() {
                        if id == i64::MIN {
                            if !has_i64_min {
                                has_i64_min = true;
                                result.push(id);
                            }
                        } else if seen.insert(id) {
                            result.push(id);
                        }
                    }
                }
                return Ok(Some(result));
            }
        }

        // OPTIMIZATION: Detect BETWEEN/range pattern (col >= X AND col <= Y) for single range scan
        // This avoids two separate index scans + sort + intersection
        if let Some(and_operands) = expr.get_and_operands() {
            // Check for range pattern: exactly 2 comparisons on the same indexed column
            if and_operands.len() == 2 {
                if let (Some((col1, op1, val1)), Some((col2, op2, val2))) = (
                    and_operands[0].get_comparison_info(),
                    and_operands[1].get_comparison_info(),
                ) {
                    // Same column?
                    if col1 == col2 {
                        // Check for range pattern: one lower bound + one upper bound
                        let (lower_op, lower_val, upper_op, upper_val) = match (op1, op2) {
                            (Operator::Gt | Operator::Gte, Operator::Lt | Operator::Lte) => {
                                (op1, val1, op2, val2)
                            }
                            (Operator::Lt | Operator::Lte, Operator::Gt | Operator::Gte) => {
                                (op2, val2, op1, val1)
                            }
                            _ => (Operator::Eq, val1, Operator::Eq, val2), // Not a range
                        };

                        // If we have a valid range pattern, use single range scan
                        if matches!(lower_op, Operator::Gt | Operator::Gte)
                            && matches!(upper_op, Operator::Lt | Operator::Lte)
                        {
                            if let Some(index) =
                                self.get_index_by_column_for_query(col1, implication_expr)
                            {
                                // Only B-tree and PrimaryKey indexes support range queries
                                // Hash and Bitmap indexes return empty for range queries,
                                // which would incorrectly indicate "no matches" instead of
                                // "can't handle this query"
                                if !matches!(
                                    index.index_type(),
                                    IndexType::BTree | IndexType::PrimaryKey
                                ) {
                                    return Ok(None); // Fall back to full scan
                                }
                                let min_inclusive = matches!(lower_op, Operator::Gte);
                                let max_inclusive = matches!(upper_op, Operator::Lte);
                                let row_ids = index.get_row_ids_in_range(
                                    std::slice::from_ref(lower_val),
                                    std::slice::from_ref(upper_val),
                                    min_inclusive,
                                    max_inclusive,
                                );
                                // Return result even if empty - we successfully used the index
                                // Empty result is valid (no rows match the range)
                                return Ok(Some(row_ids?));
                            }
                        }
                    }
                }
            }
        }

        // OPTIMIZATION: Handle AND expressions with recursive index lookup
        // For '(col1 IN (...)) AND (col2 IN (...))', process each operand and intersect results
        // This is critical for semi-join optimization where we combine right_filter with IN clause
        if let Some(and_operands) = expr.get_and_operands() {
            let mut indexed_row_ids: Vec<RowIdVec> = Vec::with_capacity(and_operands.len());

            for operand in and_operands {
                // Recursively try index lookup for each AND operand
                // OPTIMIZATION: Don't sort here - defer sorting until we know intersection is needed
                if let Some(row_ids) =
                    self.try_index_lookup_with_context(operand.as_ref(), implication_expr, schema)?
                {
                    indexed_row_ids.push(row_ids);
                }
                // If an operand can't use an index, we'll still proceed with available indexes
                // and the executor will handle memory filtering for the rest
            }

            // If at least one operand used an index, intersect and return the most restrictive result
            // The executor will handle memory filtering for operands without index support
            if !indexed_row_ids.is_empty() {
                if indexed_row_ids.len() == 1 {
                    // OPTIMIZATION: Single operand - no intersection needed
                    return Ok(Some(indexed_row_ids.into_iter().next().unwrap()));
                }

                // Bitset intersection: build a bitset from the larger list for O(1) lookups,
                // then retain only matching elements from the smaller list.
                // Cap at MAX_BITSET_WORDS (~1MB) to prevent OOM with large/sparse row IDs.
                // Negative row IDs skip the bitset path (id as usize wraps).
                const MAX_BITSET_WORDS: usize = 131_072;
                indexed_row_ids.sort_by_key(|v| v.len());
                let mut result = indexed_row_ids.swap_remove(0); // smallest
                for other in &indexed_row_ids {
                    let mut max_id = 0i64;
                    let mut min_id = 0i64;
                    for &id in other.iter() {
                        if id > max_id {
                            max_id = id;
                        }
                        if id < min_id {
                            min_id = id;
                        }
                    }
                    let num_words = (max_id as usize / 64) + 1;
                    if min_id >= 0 && num_words <= MAX_BITSET_WORDS {
                        // Fast bitset path
                        let mut bits = vec![0u64; num_words];
                        for &id in other.iter() {
                            let idx = id as usize;
                            bits[idx / 64] |= 1u64 << (idx % 64);
                        }
                        result.retain(|id| {
                            let idx = *id as usize;
                            idx < num_words * 64 && (bits[idx / 64] & (1u64 << (idx % 64))) != 0
                        });
                    } else {
                        // Fallback: I64Set-based intersection for large/sparse row IDs
                        let mut set = I64Set::with_capacity(other.len());
                        let mut has_i64_min = false;
                        for &id in other.iter() {
                            if id == i64::MIN {
                                has_i64_min = true;
                            } else {
                                set.insert(id);
                            }
                        }
                        result.retain(|id| {
                            if *id == i64::MIN {
                                has_i64_min
                            } else {
                                set.contains(*id)
                            }
                        });
                    }
                    if result.is_empty() {
                        return Ok(Some(RowIdVec::new()));
                    }
                }
                return Ok(Some(result));
            }

            // No operands could use indexes - fall through to other strategies
            // Note: We don't return None here because the subsequent code might
            // still be able to extract simple comparisons from the AND expression
        }

        // OPTIMIZATION: Handle IN list expressions with direct index lookup
        // For 'col IN (a, b, c)', use get_row_ids_in for efficient multi-value lookup
        if let Some(in_list) = expr
            .as_any()
            .downcast_ref::<crate::expression::InListExpr>()
        {
            // Only handle positive IN (not NOT IN)
            if !in_list.is_not() {
                if let Some(col_name) = in_list.get_column_name() {
                    let Some((_, column)) = schema.find_column(col_name) else {
                        return Ok(None);
                    };
                    let values = in_list.get_values();
                    if crate::expression::in_list::has_cross_numeric_physical_variant(
                        column.data_type,
                        values,
                    ) {
                        return Ok(None);
                    }
                    if let Some(index) =
                        self.get_index_by_column_for_query(col_name, implication_expr)
                    {
                        // Use the efficient get_row_ids_in method
                        let mut row_ids = index.get_row_ids_in(values)?;
                        // SQL IN is a membership predicate, not a bag-producing
                        // operator. Repeated values may make an index append the
                        // same candidate more than once, but the table row must
                        // still be evaluated and returned only once.
                        row_ids.sort();
                        row_ids.dedup();
                        return Ok(Some(row_ids));
                    }
                }
            }
        }

        // OPTIMIZATION: Handle LIKE prefix patterns with index range scan
        // For 'name LIKE 'John%'', use index range scan from 'John' to 'John\xff'
        if let Some((col_name, prefix, negated)) = expr.get_like_prefix_info() {
            // Don't optimize NOT LIKE (would need complement of range)
            if negated {
                return Ok(None);
            }

            if let Some(index) = self.get_index_by_column_for_query(col_name, implication_expr) {
                if !matches!(index.index_type(), IndexType::BTree | IndexType::PrimaryKey) {
                    return Ok(None);
                }
                // Create range from prefix to prefix + '\xff' (highest byte)
                // This captures all strings starting with the prefix
                let min_value = Value::text(&prefix);
                let mut max_prefix = prefix.clone();
                max_prefix.push('\u{FFFF}'); // Highest unicode char
                let max_value = Value::text(&max_prefix);

                // Use index range query
                let entries = index.find_range(
                    &[min_value],
                    &[max_value],
                    true,  // include min
                    false, // exclude max
                )?;
                let mut row_ids = RowIdVec::with_capacity(entries.len());
                for entry in entries {
                    row_ids.push(entry.row_id);
                }
                return Ok(Some(row_ids));
            }
        }

        // Try to extract comparisons from AND expressions
        let comparisons = expr.collect_comparisons();
        if comparisons.is_empty() {
            return Ok(None);
        }

        // Group comparisons by column name
        let mut column_comparisons: FxHashMap<&str, Vec<(Operator, &Value)>> = FxHashMap::default();
        for (col_name, op, val) in &comparisons {
            column_comparisons
                .entry(*col_name)
                .or_default()
                .push((*op, *val));
        }

        // OPTIMIZATION: Try the shared composite access descriptor first. A
        // usable prefix consists of equality columns followed by at most one
        // range column; the same descriptor is rendered by EXPLAIN.
        if let Some(plan) = self.plan_composite_index_lookup(expr, implication_expr) {
            let row_ids = plan
                .lookup_row_ids()?
                .expect("planned lookup is executable");
            if row_ids.is_empty() {
                return Ok(Some(RowIdVec::new()));
            }

            let uncovered_columns: Vec<&str> = column_comparisons
                .keys()
                .filter(|column| !plan.covered_columns.contains(**column))
                .copied()
                .collect();
            if uncovered_columns.is_empty() {
                return Ok(Some(row_ids));
            }

            // Intersect any additional indexed equality predicates. Predicates
            // without their own index remain residuals and the final full
            // expression is always re-evaluated on candidate rows.
            let mut all_row_ids: Vec<RowIdVec> = vec![row_ids];
            for column in uncovered_columns {
                if let Some(single_index) =
                    self.get_index_by_column_for_query(column, implication_expr)
                {
                    if let Some(ops) = column_comparisons.get(column) {
                        if let Some((_, value)) =
                            ops.iter().find(|(operator, _)| *operator == Operator::Eq)
                        {
                            let ids =
                                single_index.get_row_ids_equal(std::slice::from_ref(value))?;
                            if ids.is_empty() {
                                return Ok(Some(RowIdVec::new()));
                            }
                            all_row_ids.push(ids);
                        }
                    }
                }
            }

            if all_row_ids.len() == 1 {
                return Ok(Some(all_row_ids.into_iter().next().unwrap()));
            }
            for ids in &mut all_row_ids {
                ids.sort_unstable();
            }
            let mut result = all_row_ids.swap_remove(0);
            for other in &all_row_ids {
                result = intersect_sorted_ids(&result, other);
                if result.is_empty() {
                    return Ok(Some(RowIdVec::new()));
                }
            }
            return Ok(Some(result));
        }

        // Fall back to single-column index strategy
        // Collect row IDs from all indexed columns
        let mut all_row_ids: Vec<RowIdVec> = Vec::new();

        for (col_name, ops) in &column_comparisons {
            if let Some(index) = self.get_index_by_column_for_query(col_name, implication_expr) {
                // Check for range pattern: col >= min AND col <= max
                let mut min_val: Option<(&Value, bool)> = None; // (value, inclusive)
                let mut max_val: Option<(&Value, bool)> = None;
                let mut eq_val: Option<&Value> = None;

                for (op, val) in ops {
                    match op {
                        Operator::Eq => eq_val = Some(val),
                        Operator::Gt => min_val = Some((val, false)),
                        Operator::Gte => min_val = Some((val, true)),
                        Operator::Lt => max_val = Some((val, false)),
                        Operator::Lte => max_val = Some((val, true)),
                        _ => {}
                    }
                }

                // Equality takes precedence - but skip boolean (low cardinality)
                if let Some(val) = eq_val {
                    // Skip boolean equality - ~50% selectivity makes full scan faster
                    if matches!(val, Value::Boolean(_)) {
                        continue;
                    }
                    // OPTIMIZATION: Use from_ref to avoid clone
                    let row_ids = index.get_row_ids_equal(std::slice::from_ref(val))?;
                    if row_ids.is_empty() {
                        // If any index returns empty, the AND result is empty
                        return Ok(Some(RowIdVec::new()));
                    }
                    // Don't sort yet - only sort when intersection is needed
                    all_row_ids.push(row_ids);
                    continue;
                }

                // Range query - but skip Hash indexes (they don't support range queries)
                if min_val.is_some() || max_val.is_some() {
                    // Hash indexes don't support range queries - skip them
                    // and let the query fall back to a full scan
                    if matches!(index.index_type(), IndexType::Hash) {
                        continue;
                    }

                    let row_ids =
                        if let (Some((min, min_inc)), Some((max, max_inc))) = (min_val, max_val) {
                            // OPTIMIZATION: Use from_ref to avoid clone
                            index.get_row_ids_in_range(
                                std::slice::from_ref(min),
                                std::slice::from_ref(max),
                                min_inc,
                                max_inc,
                            )?
                        } else if let Some((val, inclusive)) = min_val {
                            let op = if inclusive {
                                Operator::Gte
                            } else {
                                Operator::Gt
                            };
                            self.query_index_with_operator(&*index, op, val)?
                                .unwrap_or_default()
                        } else if let Some((val, inclusive)) = max_val {
                            let op = if inclusive {
                                Operator::Lte
                            } else {
                                Operator::Lt
                            };
                            self.query_index_with_operator(&*index, op, val)?
                                .unwrap_or_default()
                        } else {
                            RowIdVec::new()
                        };

                    if row_ids.is_empty() {
                        // If any index returns empty, the AND result is empty
                        return Ok(Some(RowIdVec::new()));
                    }
                    // Don't sort yet - only sort when intersection is needed
                    all_row_ids.push(row_ids);
                }
            }
        }

        // If we have no indexed results, return None
        if all_row_ids.is_empty() {
            return Ok(None);
        }

        // If we have only one index, return its results (no sort needed)
        if all_row_ids.len() == 1 {
            return Ok(Some(all_row_ids.into_iter().next().unwrap()));
        }

        // Range query results from BTree are in VALUE order, not row_id order.
        // intersect_sorted_ids requires row_id-sorted input (uses binary_search).
        // Sort all sets before intersection.
        for ids in &mut all_row_ids {
            ids.sort_unstable();
        }

        // Intersect all row ID sets for multi-column filtering
        let mut result = all_row_ids.swap_remove(0);
        for other in &all_row_ids {
            result = intersect_sorted_ids(&result, other);
            if result.is_empty() {
                return Ok(Some(RowIdVec::new()));
            }
        }

        Ok(Some(result))
    }

    /// Query an index with a specific operator
    /// Returns a pooled RowIdVec for efficient memory reuse.
    pub(super) fn query_index_with_operator(
        &self,
        index: &dyn crate::traits::Index,
        operator: radixdb_core::Operator,
        value: &Value,
    ) -> Result<Option<RowIdVec>> {
        use radixdb_core::Operator;

        match operator {
            Operator::Eq => {
                let row_ids = index.get_row_ids_equal(std::slice::from_ref(value))?;
                // Reaching this branch means the selected index supports the
                // predicate. An empty posting list is therefore an exact empty
                // candidate set, not a reason to abandon the index plan. This
                // distinction is essential for read-your-writes: a value may
                // be absent from the committed index but present in this
                // transaction's private INSERT/UPDATE set.
                Ok(Some(row_ids))
            }
            Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
                if !matches!(index.index_type(), IndexType::BTree | IndexType::PrimaryKey) {
                    return Ok(None);
                }
                // Use find_with_operator for range queries
                let entries = index.find_with_operator(operator, std::slice::from_ref(value))?;
                let mut row_ids = RowIdVec::with_capacity(entries.len());
                for entry in entries {
                    row_ids.push(entry.row_id);
                }
                Ok(Some(row_ids))
            }
            _ => Ok(None),
        }
    }

    /// Add every transaction-private row ID to a committed index candidate
    /// set. Shared secondary indexes are intentionally updated only at commit,
    /// so the final predicate evaluation must see local INSERTs and key
    /// changes as additional candidates. Local UPDATEs/DELETEs that no longer
    /// match are harmless: `fetch_rows_by_ids_checked` resolves the private
    /// version first and re-evaluates the complete predicate.
    pub(super) fn merge_local_index_candidates(&self, mut row_ids: RowIdVec) -> RowIdVec {
        let txn_versions = self.txn_versions.read().unwrap();
        if !txn_versions.has_local_changes() {
            return row_ids;
        }
        row_ids.extend(txn_versions.iter_local().map(|(row_id, _)| row_id));
        row_ids.sort_unstable();
        row_ids.dedup();
        row_ids
    }

    /// Fast path for index-based row fetching when there are no local transaction changes.
    /// Returns Some(rows) if we can serve from index, None if we should fall through to full path.
    /// Takes RowIdVec for efficient pooled memory reuse.
    #[inline]
    pub(super) fn try_fetch_from_index(
        &self,
        row_ids: RowIdVec,
        expr: &dyn Expression,
        schema: &Schema,
        limit: usize,
        offset: usize,
    ) -> Result<Option<RowVec>> {
        if limit == 0 {
            return Ok(Some(RowVec::new()));
        }

        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            drop(txn_versions);
            let row_ids = self.merge_local_index_candidates(row_ids);
            let rows = self.fetch_rows_by_ids_checked(&row_ids, expr)?;
            return Ok(Some(rows.into_iter().skip(offset).take(limit).collect()));
        }

        // No local changes - use for_each_visible for single lock acquisition + early termination
        let mut result = RowVec::with_capacity(limit.min(row_ids.len()));
        let mut skipped = 0usize;
        let mut filter_error: Option<Error> = None;

        self.version_store
            .for_each_visible(&row_ids, self.txn_id, |row_id, row_data| {
                if filter_error.is_some() {
                    return false;
                }
                // Re-apply filter (index may return superset for complex expressions)
                let row = self.normalize_row_to_schema(row_data, schema);
                match expr.evaluate(&row) {
                    Ok(true) => {
                        if skipped < offset {
                            skipped += 1;
                        } else {
                            result.push((row_id, row));
                            if result.len() >= limit {
                                return false; // Stop: LIMIT reached
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(error) => {
                        filter_error = Some(error);
                        return false;
                    }
                }
                true // Continue
            });
        if let Some(error) = filter_error {
            return Err(error);
        }
        Ok(Some(result))
    }

    /// Hybrid optimization for mixed OR predicates (some indexed, some not).
    ///
    /// For `(indexed_col = X OR non_indexed_col = Y)`:
    /// 1. Fetch rows matching indexed branches via index lookup
    /// 2. Scan all rows evaluating only the non-indexed branches (skip already-found rows)
    /// 3. Union deduplicated results
    ///
    /// Returns `Some(rows)` when we can handle this, `None` to fall through.
    pub(super) fn try_mixed_or_fetch(
        &self,
        expr: &dyn Expression,
        schema: &Schema,
        limit: usize,
        offset: usize,
    ) -> Result<Option<RowVec>> {
        if limit == 0 {
            return Ok(Some(RowVec::new()));
        }
        // This optimization evaluates its unindexed branch inside VersionStore,
        // before logical schema defaults can be appended.
        if self.logical_rows_need_normalization() {
            return Ok(None);
        }

        let Some(or_operands) = expr.get_or_operands() else {
            return Ok(None);
        };

        // Partition operands into indexed and non-indexed
        let mut indexed_row_ids: Vec<RowIdVec> = Vec::new();
        let mut unindexed_operands: Vec<Box<dyn Expression>> = Vec::new();

        for operand in or_operands {
            if let Some(row_ids) = self.try_index_lookup(operand.as_ref(), schema)? {
                indexed_row_ids.push(row_ids);
            } else {
                unindexed_operands.push(operand.clone_box());
            }
        }

        // Only useful when we have BOTH indexed and non-indexed operands
        if indexed_row_ids.is_empty() || unindexed_operands.is_empty() {
            return Ok(None);
        }

        // Bail if there are local transaction changes (complex merge needed)
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return Ok(None);
        }
        drop(txn_versions);

        // Phase 1: Fetch rows matching indexed branches
        // Build a bitset/set of indexed row IDs for dedup
        const MAX_BITSET_WORDS: usize = 131_072;
        let mut max_id = 0i64;
        let mut min_id = 0i64;
        let total_indexed: usize = indexed_row_ids.iter().map(|v| v.len()).sum();
        for ids in &indexed_row_ids {
            for &id in ids.iter() {
                if id > max_id {
                    max_id = id;
                }
                if id < min_id {
                    min_id = id;
                }
            }
        }

        // Deduplicate indexed row IDs into a single RowIdVec + build lookup set
        let num_words = if max_id >= 0 {
            (max_id as usize / 64) + 1
        } else {
            0
        };
        let use_bitset = min_id >= 0 && num_words <= MAX_BITSET_WORDS;

        let mut deduped_ids = RowIdVec::with_capacity(total_indexed);
        let mut bitset = if use_bitset {
            vec![0u64; num_words]
        } else {
            vec![]
        };
        let mut id_set = if !use_bitset {
            I64Set::with_capacity(total_indexed)
        } else {
            I64Set::new()
        };

        for ids in &indexed_row_ids {
            for &id in ids.iter() {
                if use_bitset {
                    let idx = id as usize;
                    let word = idx / 64;
                    let mask = 1u64 << (idx % 64);
                    if (bitset[word] & mask) == 0 {
                        bitset[word] |= mask;
                        deduped_ids.push(id);
                    }
                } else if id_set.insert(id) {
                    deduped_ids.push(id);
                }
            }
        }

        // Fetch indexed rows
        let mut result = RowVec::with_capacity(limit.min(total_indexed + 64));
        let mut skipped = 0usize;
        let mut filter_error: Option<Error> = None;

        self.version_store
            .for_each_visible(&deduped_ids, self.txn_id, |row_id, row_data| {
                if filter_error.is_some() {
                    return false;
                }
                let row = self.normalize_row_to_schema(row_data, schema);
                // Re-apply full OR filter to ensure correctness (index may return superset)
                match expr.evaluate(&row) {
                    Ok(true) => {
                        if skipped < offset {
                            skipped += 1;
                        } else {
                            result.push((row_id, row));
                            if result.len() >= limit {
                                return false;
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(error) => {
                        filter_error = Some(error);
                        return false;
                    }
                }
                true
            });
        if let Some(error) = filter_error {
            return Err(error);
        }

        if result.len() >= limit {
            return Ok(Some(result));
        }

        // Phase 2: Scan for rows matching non-indexed branches (skip already-found rows)
        // Build filter from non-indexed operands only
        let mut unindexed_filter: Box<dyn Expression> = if unindexed_operands.len() == 1 {
            unindexed_operands.into_iter().next().unwrap()
        } else {
            Box::new(OrExpr::new(unindexed_operands))
        };
        unindexed_filter.prepare_for_schema(schema);

        let remaining_offset = offset.saturating_sub(skipped);
        let mut scan_skipped = 0usize;

        // Streaming scan: filter + dedup + limit inline, no bulk materialization
        self.version_store.for_each_visible_filtered(
            self.txn_id,
            unindexed_filter.as_ref(),
            |row_id, row_data| {
                // Skip rows already found by index
                let already_found = if use_bitset {
                    let idx = row_id as usize;
                    if idx / 64 < bitset.len() {
                        (bitset[idx / 64] & (1u64 << (idx % 64))) != 0
                    } else {
                        false
                    }
                } else {
                    id_set.contains(row_id)
                };
                if already_found {
                    return true; // continue
                }

                let row = self.normalize_row_to_schema(row_data, schema);
                if scan_skipped < remaining_offset {
                    scan_skipped += 1;
                } else {
                    result.push((row_id, row));
                    if result.len() >= limit {
                        return false; // stop
                    }
                }
                true // continue
            },
        )?;

        Ok(Some(result))
    }

    /// Validates and coerces a row against the schema
    /// Returns the coerced row if successful
    pub(super) fn validate_and_coerce_row(&self, row: &mut Row) -> Result<()> {
        // OPTIMIZATION: Use cached_schema instead of version_store.schema() to avoid clone
        let schema = &self.cached_schema;

        // Check column count
        if row.len() != schema.columns.len() {
            return Err(Error::internal(format!(
                "invalid column count: expected {}, got {}",
                schema.columns.len(),
                row.len()
            )));
        }

        // Validate and coerce each column
        for (i, col) in schema.columns.iter().enumerate() {
            let value = row.get(i).ok_or_else(|| {
                Error::internal(format!("nil value at index {} (column '{}')", i, col.name))
            })?;

            // Check NULL constraint
            if !col.nullable && value.is_null() {
                return Err(Error::internal(format!(
                    "NULL value in non-nullable column '{}'",
                    col.name
                )));
            }

            // Check type compatibility for non-NULL values
            if !value.is_null() {
                value.validate_shape().map_err(|error| {
                    Error::invalid_argument(format!(
                        "invalid value shape in column '{}': {}",
                        col.name, error
                    ))
                })?;
                let actual_type = value.data_type();
                if actual_type != col.data_type {
                    let allowed = matches!(
                        (actual_type, col.data_type),
                        (DataType::Text, DataType::Json)
                            | (DataType::Integer, DataType::Float)
                            | (DataType::Float, DataType::Integer)
                            | (DataType::Integer, DataType::Boolean)
                            | (DataType::Text, DataType::Vector)
                            | (DataType::Text, DataType::Uuid)
                    );
                    if !allowed {
                        return Err(Error::internal(format!(
                            "type mismatch in column '{}': expected {:?}, got {:?}",
                            col.name, col.data_type, actual_type
                        )));
                    }
                    let coerced = value.try_coerce_to_type(col.data_type).map_err(|error| {
                        Error::invalid_argument(format!(
                            "cannot coerce column '{}' from {:?} to {:?}: {}",
                            col.name, actual_type, col.data_type, error
                        ))
                    })?;
                    let _ = row.set(i, coerced);
                }
            }

            if col.data_type == DataType::Timestamp {
                if let Some(value) = row.get(i) {
                    if matches!(value, Value::Timestamp(_))
                        && value.artifact_timestamp_nanos().is_none()
                    {
                        return Err(Error::invalid_argument(format!(
                            "timestamp in column '{}' is outside the exact artifact-backed nanosecond range",
                            col.name
                        )));
                    }
                }
            }

            if let Some(value) = row.get(i) {
                col.validate_declared_value(value)?;
            }

            if col.data_type == DataType::Vector && col.vector_dimensions > 0 {
                let got = match row.get(i) {
                    Some(Value::Extension(data))
                        if data.first() == Some(&(DataType::Vector as u8)) =>
                    {
                        let payload_bytes = data.len().saturating_sub(1);
                        let decoded_dims = u16::try_from(payload_bytes / 4).unwrap_or(u16::MAX);
                        if payload_bytes == usize::from(col.vector_dimensions) * 4 {
                            decoded_dims
                        } else if decoded_dims == col.vector_dimensions {
                            // Preserve an unmistakable mismatch when malformed
                            // payload bytes happen to truncate to the declared
                            // f32 element count.
                            u16::MAX
                        } else {
                            decoded_dims
                        }
                    }
                    Some(value) if value.is_null() => continue,
                    _ => u16::MAX,
                };
                if got != col.vector_dimensions {
                    return Err(Error::VectorDimensionMismatch {
                        expected: col.vector_dimensions,
                        got,
                    });
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub(super) fn apply_validated_setter(
        &self,
        row: Row,
        setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<(Row, bool)> {
        let (mut updated_row, changed) = setter(row)?;
        if changed {
            self.validate_and_coerce_row(&mut updated_row)?;
        }
        Ok((updated_row, changed))
    }

    /// Extracts the primary key value from a row
    /// OPTIMIZATION: Uses cached_schema and pk_column_index to avoid iteration
    pub(super) fn extract_row_pk(&self, row: &Row) -> Result<i64> {
        // Fast path: use cached PK index if available
        if let Some(pk_idx) = self.cached_schema.pk_column_index() {
            if let Some(value) = row.get(pk_idx) {
                if let Some(pk) = value.as_int64() {
                    return Ok(pk);
                }
            }
        }

        // Fallback: If no primary key or not an integer, generate a synthetic row ID
        self.version_store.get_next_auto_increment_id()
    }

    /// Finds the primary key column index
    /// OPTIMIZATION: Uses cached pk_column_index from schema
    #[inline]
    pub(super) fn find_pk_column_index(&self) -> Option<usize> {
        self.cached_schema.pk_column_index()
    }

    /// Check unique index constraints for a row being inserted.
    pub(super) fn check_unique_constraints(&self, row: &Row, row_id: i64) -> Result<()> {
        let schema = &self.cached_schema;
        let disabled_indexes = self
            .txn_versions
            .read()
            .unwrap()
            .disabled_parent_index_names();
        self.version_store
            .for_each_unique_index(|index_name, index| {
                if disabled_indexes.contains(&index_name.to_lowercase()) {
                    return Ok(());
                }
                let Some(values) =
                    crate::mvcc::version_store::index_values_for_row(index.as_ref(), row)?
                else {
                    return Ok(());
                };
                if values.iter().any(|v| v.is_null()) {
                    return Ok(());
                }
                // The parent index intentionally remains unchanged until the
                // transaction publishes. Resolve parent conflicts through the
                // local write set: a row deleted here no longer owns its old
                // key, and an updated row owns only its new key. Local-local
                // duplicates keep the existing commit-time batch validation.
                if index.index_type() == IndexType::Hnsw {
                    let hnsw = index.as_any().downcast_ref::<HnswIndex>().ok_or_else(|| {
                        Error::internal(format!(
                            "index '{}' advertised HNSW type but cannot be downcast",
                            index.name()
                        ))
                    })?;
                    if let Some(value) = values.first() {
                        let ignored = {
                            let txn_versions = self.txn_versions.read().unwrap();
                            let mut ignored = radixdb_core::I64Set::new();
                            for (local_row_id, local_version) in txn_versions.iter_local() {
                                let still_owns_key = !local_version.is_deleted()
                                    && crate::mvcc::version_store::index_values_for_row(
                                        index.as_ref(),
                                        &local_version.data,
                                    )?
                                    .is_some_and(|local_values| local_values == values);
                                if !still_owns_key {
                                    ignored.insert(local_row_id);
                                }
                            }
                            ignored
                        };
                        if let Some(conflict_row_id) =
                            hnsw.find_exact_duplicate(value, row_id, Some(&ignored))
                        {
                            return Err(Error::UniqueConstraint {
                                index: index_name.to_string(),
                                column: index.column_names().join(", "),
                                value: format!("{:?}", values),
                                row_id: conflict_row_id,
                            });
                        }
                    }
                    return Ok(());
                }
                let entries = index.find(&values)?;
                let conflict = {
                    let txn_versions = self.txn_versions.read().unwrap();
                    let mut conflict = None;
                    for entry in &entries {
                        if entry.row_id == row_id {
                            continue;
                        }
                        let still_owns_key = match txn_versions.get_local_version(entry.row_id) {
                            None => true,
                            Some(local_version) if local_version.is_deleted() => false,
                            Some(local_version) => {
                                crate::mvcc::version_store::index_values_for_row(
                                    index.as_ref(),
                                    &local_version.data,
                                )?
                                .is_some_and(|local_values| local_values == values)
                            }
                        };
                        if still_owns_key {
                            conflict = Some(entry);
                            break;
                        }
                    }
                    conflict
                };
                if let Some(entry) = conflict {
                    let column_ids = index.column_ids();
                    let col_names: Vec<&str> = column_ids
                        .iter()
                        .map(|&col_id| {
                            schema
                                .columns
                                .get(col_id as usize)
                                .map(|c| c.name.as_str())
                                .unwrap_or("unknown")
                        })
                        .collect();
                    return Err(Error::UniqueConstraint {
                        index: index_name.to_string(),
                        column: col_names.join(", "),
                        value: format!("{:?}", values),
                        row_id: entry.row_id,
                    });
                }
                Ok(())
            })
    }

    /// Fill storage-owned generated values before statement constraints run.
    /// Repeated calls are harmless because only NULL placeholders are replaced.
    pub(super) fn materialize_auto_increment_values(&mut self, row: &mut Row) -> Result<()> {
        for (idx, col) in self.cached_schema.columns.iter().enumerate() {
            if !col.auto_increment {
                continue;
            }
            let Some(value) = row.get(idx) else {
                continue;
            };
            if !value.is_null() {
                continue;
            }
            match col.data_type {
                DataType::Integer => {
                    let next_id = self.version_store.get_next_auto_increment_id()?;
                    let _ = row.set(idx, Value::Integer(next_id));
                }
                DataType::Uuid => {
                    let _ = row.set(idx, Value::uuid_v7());
                }
                _ => {
                    return Err(Error::internal(format!(
                        "AUTO_INCREMENT column '{}' must be INTEGER or UUID, got {:?}",
                        col.name, col.data_type
                    )));
                }
            }
        }
        Ok(())
    }

    /// Prepares a row for insertion: handles auto-increment, validates, and checks constraints.
    /// Returns the row_id on success. The row is modified in-place with auto-increment values.
    #[inline]
    pub(super) fn prepare_insert(&mut self, row: &mut Row) -> Result<i64> {
        // Direct Table callers may not have invoked materialize_insert_values.
        self.materialize_auto_increment_values(row)?;

        // Keep the INTEGER PRIMARY KEY row_id counter in sync with explicit IDs.
        if let Some(pk_idx) = self.find_pk_column_index() {
            if let Some(value) = row.get(pk_idx) {
                if let Some(pk_val) = value.as_int64() {
                    // Track max row_id for ORDER BY contiguous iteration optimization.
                    // Uses the auto-increment counter as a convenient max-row-id tracker.
                    let current = self.version_store.get_auto_increment_counter();
                    if pk_val > current {
                        self.version_store.set_auto_increment_counter(pk_val);
                    }
                }
            }
        }

        // Validate and coerce row AFTER auto-increment has filled in the primary key
        self.validate_and_coerce_row(row)?;

        // Extract row ID
        let row_id = self.extract_row_pk(row)?;

        // Check if row already exists in local versions
        {
            let txn_versions = self.txn_versions.read().unwrap();
            if txn_versions.has_locally_seen(row_id) && txn_versions.get(row_id).is_some() {
                return Err(Error::primary_key_constraint(row_id));
            }
        }

        // Check if row exists in global store
        if self.version_store.quick_check_row_existence(row_id) {
            if let Some(version) = self.version_store.get_visible_version(row_id, self.txn_id) {
                if !version.is_deleted() {
                    return Err(Error::primary_key_constraint(row_id));
                }
            }
        }

        // Check unique index constraints (READ lock).
        self.check_unique_constraints(row, row_id)?;

        Ok(row_id)
    }

    /// Commits the transaction's local changes
    ///
    /// Index updates are handled by TransactionVersionStore::commit() using batch
    /// operations to reduce lock acquisitions from O(rows × indexes) to O(indexes).
    pub fn commit(&mut self) -> Result<()> {
        if !self.allow_direct_commit {
            return Err(Error::NotSupported(
                "engine-managed table handles must be committed through Transaction::commit"
                    .to_string(),
            ));
        }
        self.commit_internal(false)
    }

    /// Engine commit path: keep claims until the transaction registry has made
    /// the committed versions visible to other transactions.
    pub(crate) fn commit_retaining_claims(&mut self) -> Result<()> {
        self.commit_internal(true)
    }

    pub(super) fn commit_internal(&mut self, retain_claims: bool) -> Result<()> {
        // Check if there are local changes before committing
        let has_changes = {
            let txn_versions = self.txn_versions.read().unwrap();
            txn_versions.has_local_changes()
        };

        // Commit versions to the version store (this also updates indexes)
        let mut txn_versions = self.txn_versions.write().unwrap();
        if retain_claims {
            txn_versions.commit_retaining_claims()?;
        } else {
            txn_versions.commit()?;
        }
        drop(txn_versions);

        // Mark zone maps as stale if we had any data changes
        // This ensures the optimizer won't use outdated pruning info
        if has_changes {
            self.version_store.mark_zone_maps_stale();
        }

        Ok(())
    }

    /// Returns the row count visible to this transaction
    ///
    /// OPTIMIZATION: Uses single-pass counting instead of per-row visibility checks.
    /// Reduces lock acquisitions from O(N) to O(1) for the global store.
    pub fn row_count(&self) -> usize {
        // Count global visible versions in single pass (O(1) lock instead of O(N))
        let mut count = self.version_store.count_visible_rows(self.txn_id);

        // Adjust for local changes (uncommitted in this transaction)
        let txn_versions = self.txn_versions.read().unwrap();
        for (row_id, version) in txn_versions.iter_local() {
            // Check if this row exists in global store
            let exists_in_global = self.version_store.quick_check_row_existence(row_id);

            if version.is_deleted() {
                // If deleted locally and existed in global, subtract from count
                if exists_in_global {
                    count = count.saturating_sub(1);
                }
            } else {
                // If inserted locally and not in global, add to count
                if !exists_in_global {
                    count += 1;
                }
            }
        }

        count
    }

    /// Fast O(1) row count for COUNT(*) queries without local changes
    ///
    /// Returns Some(count) if the fast path can be used (no local changes),
    /// Returns None if the caller should fall back to the full row_count() method.
    ///
    /// OPTIMIZATION: This uses the pre-computed committed_row_count which is O(1)
    /// instead of iterating all rows with visibility checks.
    #[inline]
    pub fn fast_row_count(&self) -> Option<usize> {
        // Check if there are any local changes
        let txn_versions = self.txn_versions.read().unwrap();
        if txn_versions.has_local_changes() {
            return None;
        }
        // Under snapshot isolation, committed_row_count includes rows committed
        // after our snapshot point, so the O(1) count would be wrong.
        if self.version_store.needs_snapshot_isolation(self.txn_id) {
            return None;
        }
        // No local changes, read-committed - use the O(1) committed row count
        Some(self.version_store.committed_row_count())
    }

    /// Collect all visible rows, optionally filtered
    ///
    /// Optimized to use batch fetch even when there are local changes,
    /// then merge the results.
    /// Returns RowVec for zero-allocation reuse across queries.
    #[inline]
    pub(super) fn collect_visible_rows(&self, filter: Option<&dyn Expression>) -> Result<RowVec> {
        let txn_versions = self.txn_versions.read().unwrap();
        let schema = &self.cached_schema;

        // Check if we have local versions (uncommitted changes in this transaction)
        let has_local = txn_versions.has_local_changes();

        if !has_local {
            // No local versions - use thread-local cached Vec for zero-allocation reuse
            // Arena storage provides 50x+ faster scans via contiguous memory access
            if let Some(expr) = filter {
                if self.logical_rows_need_normalization() {
                    let raw_rows = self.version_store.get_all_visible_rows_cached(self.txn_id);
                    let mut rows = RowVec::with_capacity(raw_rows.len());
                    for (row_id, row) in raw_rows {
                        let row = self.normalize_row_to_schema(row, schema);
                        if expr.evaluate(&row)? {
                            rows.push((row_id, row));
                        }
                    }
                    return Ok(rows);
                }
                // Use filtered version - returns RowVec directly
                return self
                    .version_store
                    .get_all_visible_rows_filtered(self.txn_id, expr);
            }

            // Use cached version for unfiltered scans (main optimization)
            let raw_rows = self.version_store.get_all_visible_rows_cached(self.txn_id);

            // OPTIMIZATION: Skip normalization if first row matches schema column count
            // This is the common case when no ALTER TABLE ADD/DROP COLUMN has occurred
            let schema_cols = schema.columns.len();
            if !self.logical_rows_need_normalization()
                && raw_rows
                    .first()
                    .is_none_or(|(_, row)| row.len() == schema_cols)
            {
                // The first row alone cannot prove a homogeneous layout after
                // ADD COLUMN: older rows may be shorter while later inserts
                // already use the new width. The explicit store/schema signal
                // is the authority for taking this fast path.
                return Ok(raw_rows);
            }

            // Slow path: need to normalize rows for schema evolution
            // Iterate over cached vec (drains it), collect into RowVec
            return Ok(raw_rows
                .into_iter()
                .map(|(row_id, row)| (row_id, self.normalize_row_to_schema(row, schema)))
                .collect());
        }

        // Has local versions - use batch fetch then merge
        // Step 1: Get all global rows in one batch (single lock acquisition)
        // Uses thread-local cached Vec for zero-allocation reuse
        let global_rows = self.version_store.get_all_visible_rows_cached(self.txn_id);

        // Step 2: Build set of local row IDs for quick lookup (I64Set for fast i64 lookups)
        let mut local_row_ids = I64Set::new();
        let mut local_has_i64_min = false;
        for (row_id, _) in txn_versions.iter_local() {
            if row_id == i64::MIN {
                local_has_i64_min = true;
            } else {
                local_row_ids.insert(row_id);
            }
        }

        // Step 3: Pre-allocate result
        let mut rows = RowVec::with_capacity(global_rows.len() + local_row_ids.len());

        // Step 4: Add global rows that don't have local overrides
        for (row_id, row) in global_rows {
            if (row_id == i64::MIN && local_has_i64_min) || local_row_ids.contains(row_id) {
                continue; // Local version takes precedence
            }
            // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
            let row = self.normalize_row_to_schema(row, schema);
            if let Some(expr) = filter {
                if !expr.evaluate(&row)? {
                    continue;
                }
            }
            rows.push((row_id, row));
        }

        // Step 5: Add local versions (both updates and inserts)
        for (row_id, version) in txn_versions.iter_local() {
            if version.is_deleted() {
                continue;
            }
            // Normalize row to match current schema (handles ALTER TABLE ADD/DROP COLUMN)
            let row = self.normalize_row_to_schema(version.data.clone(), schema);
            if let Some(expr) = filter {
                if !expr.evaluate(&row)? {
                    continue;
                }
            }
            rows.push((row_id, row));
        }

        Ok(rows)
    }

    /// Collect visible rows WITHOUT sorting for GROUP BY optimization.
    ///
    /// This skips the O(n log n) sort since GROUP BY doesn't care about row order.
    /// Returns rows in version store iteration order.
    #[inline]
    pub(super) fn collect_visible_rows_unsorted(&self) -> Result<RowVec> {
        // For GROUP BY, order doesn't matter - use same cached path
        self.collect_visible_rows(None)
    }

    /// Collect visible rows with early termination when limit is reached
    /// This is the LIMIT pushdown optimization
    pub(super) fn collect_visible_rows_with_limit(
        &self,
        filter: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        let schema = &self.cached_schema;

        // FAST PATH: Check if this is a primary key equality lookup (WHERE id = X)
        // This is O(1) and skips all scanning
        if let Some(expr) = filter {
            if let Some(pk_id) = self.try_pk_lookup(expr, schema) {
                // Direct O(1) lookup by primary key
                let txn_versions = self.txn_versions.read().unwrap();
                // Check local versions first via get_local_version (preserves
                // delete signal). txn_versions.get() swallows deletes as None,
                // which would cause a fallback to the committed store and miss
                // uncommitted deletes in the current transaction.
                let row = if let Some(local) = txn_versions.get_local_version(pk_id) {
                    if local.is_deleted() {
                        None // Locally deleted in this transaction
                    } else {
                        Some((pk_id, local.data.clone()))
                    }
                } else if let Some(version) =
                    self.version_store.get_visible_version(pk_id, self.txn_id)
                {
                    if !version.is_deleted() {
                        Some((pk_id, version.data.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                };

                return Ok(match row {
                    Some((row_id, r)) if offset == 0 && limit >= 1 => {
                        let mut rv = RowVec::with_capacity(1);
                        rv.push((row_id, self.normalize_row_to_schema(r, schema)));
                        rv
                    }
                    _ => RowVec::new(),
                });
            }

            // OPTIMIZATION: Try secondary index lookup for OR/AND expressions on indexed columns
            // This handles queries like: WHERE age = 25 OR age = 50 OR age = 75
            if let Some(row_ids) = self.try_index_lookup(expr, schema)? {
                if let Some(result) =
                    self.try_fetch_from_index(row_ids, expr, schema, limit, offset)?
                {
                    return Ok(result);
                }
                // Has local changes - fall through to full path
            }

            // OPTIMIZATION: Mixed OR (indexed OR non-indexed) hybrid scan
            if let Some(result) = self.try_mixed_or_fetch(expr, schema, limit, offset)? {
                return Ok(result);
            }
        }

        let txn_versions = self.txn_versions.read().unwrap();

        // Check if we have local versions (uncommitted changes in this transaction)
        let has_local = txn_versions.has_local_changes();

        if !has_local {
            if filter.is_some() && self.logical_rows_need_normalization() {
                drop(txn_versions);
                let rows = self.collect_visible_rows(filter)?;
                return Ok(rows.into_iter().skip(offset).take(limit).collect());
            }
            // No local versions - use optimized path with true LIMIT pushdown
            let raw_rows = if let Some(expr) = filter {
                // With filter + limit: use filtered limit pushdown
                // This early-terminates when limit is reached after filtering
                self.version_store.get_visible_rows_filtered_with_limit(
                    self.txn_id,
                    expr,
                    limit,
                    offset,
                )?
            } else {
                // No filter: use simple LIMIT pushdown
                // This avoids scanning all 10K rows for LIMIT 10 queries - ~30x speedup
                self.version_store
                    .get_visible_rows_with_limit(self.txn_id, limit, offset)
            };

            // Keep row IDs in RowVec
            return Ok(raw_rows
                .into_iter()
                .map(|(row_id, row)| (row_id, self.normalize_row_to_schema(row, schema)))
                .collect());
        }

        // Has local versions - use batch fetch then merge with early termination
        // Uses thread-local cached Vec for zero-allocation reuse
        let global_rows = self.version_store.get_all_visible_rows_cached(self.txn_id);

        // Build set of local row IDs for quick lookup
        let mut local_row_ids = I64Set::new();
        let mut local_has_i64_min = false;
        for (row_id, _) in txn_versions.iter_local() {
            if row_id == i64::MIN {
                local_has_i64_min = true;
            } else {
                local_row_ids.insert(row_id);
            }
        }

        let mut result = RowVec::with_capacity(limit);
        let mut count = 0;

        // Add global rows that don't have local overrides
        for (row_id, row) in global_rows {
            if (row_id == i64::MIN && local_has_i64_min) || local_row_ids.contains(row_id) {
                continue; // Local version takes precedence
            }
            let row = self.normalize_row_to_schema(row, schema);
            if let Some(expr) = filter {
                if !expr.evaluate(&row)? {
                    continue;
                }
            }
            if count >= offset {
                result.push((row_id, row));
                if result.len() >= limit {
                    return Ok(result);
                }
            }
            count += 1;
        }

        // Add local versions (both updates and inserts)
        for (row_id, version) in txn_versions.iter_local() {
            if version.is_deleted() {
                continue;
            }
            let row = self.normalize_row_to_schema(version.data.clone(), schema);
            if let Some(expr) = filter {
                if !expr.evaluate(&row)? {
                    continue;
                }
            }
            if count >= offset {
                result.push((row_id, row));
                if result.len() >= limit {
                    return Ok(result);
                }
            }
            count += 1;
        }

        Ok(result)
    }

    /// Collect visible rows with LIMIT without guaranteeing deterministic order.
    /// This is an optimization for queries with LIMIT but without ORDER BY.
    /// Since SQL doesn't guarantee order for LIMIT without ORDER BY, we can
    /// skip sorting and return rows in arbitrary order, enabling true early termination.
    pub(super) fn collect_visible_rows_with_limit_unordered(
        &self,
        filter: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        let schema = &self.cached_schema;

        // FAST PATH: Check if this is a primary key equality lookup (WHERE id = X)
        // This is O(1) and skips all scanning
        if let Some(expr) = filter {
            if let Some(pk_id) = self.try_pk_lookup(expr, schema) {
                // Direct O(1) lookup by primary key
                let txn_versions = self.txn_versions.read().unwrap();
                // Check local versions first via get_local_version (preserves
                // delete signal). txn_versions.get() swallows deletes as None,
                // which would cause a fallback to the committed store and miss
                // uncommitted deletes in the current transaction.
                let row = if let Some(local) = txn_versions.get_local_version(pk_id) {
                    if local.is_deleted() {
                        None // Locally deleted in this transaction
                    } else {
                        Some((pk_id, local.data.clone()))
                    }
                } else if let Some(version) =
                    self.version_store.get_visible_version(pk_id, self.txn_id)
                {
                    if !version.is_deleted() {
                        Some((pk_id, version.data.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                };

                return Ok(match row {
                    Some((row_id, r)) if offset == 0 && limit >= 1 => {
                        let mut rv = RowVec::with_capacity(1);
                        rv.push((row_id, self.normalize_row_to_schema(r, schema)));
                        rv
                    }
                    _ => RowVec::new(),
                });
            }

            // OPTIMIZATION: Try secondary index lookup for OR/AND expressions on indexed columns
            // This handles queries like: WHERE age = 25 OR age = 50 OR age = 75
            if let Some(row_ids) = self.try_index_lookup(expr, schema)? {
                if let Some(result) =
                    self.try_fetch_from_index(row_ids, expr, schema, limit, offset)?
                {
                    return Ok(result);
                }
                // Has local changes - fall through to full path
            }

            // OPTIMIZATION: Mixed OR (indexed OR non-indexed) hybrid scan
            if let Some(result) = self.try_mixed_or_fetch(expr, schema, limit, offset)? {
                return Ok(result);
            }
        }

        let txn_versions = self.txn_versions.read().unwrap();

        // Check if we have local versions (uncommitted changes in this transaction)
        let has_local = txn_versions.has_local_changes();

        if !has_local {
            if filter.is_some() && self.logical_rows_need_normalization() {
                drop(txn_versions);
                let rows = self.collect_visible_rows(filter)?;
                return Ok(rows.into_iter().skip(offset).take(limit).collect());
            }
            // No local versions - use optimized unordered path with true early termination
            let raw_rows = if let Some(expr) = filter {
                self.version_store
                    .get_visible_rows_filtered_with_limit_unordered(
                        self.txn_id,
                        expr,
                        limit,
                        offset,
                    )?
            } else {
                self.version_store
                    .get_visible_rows_with_limit_unordered(self.txn_id, limit, offset)
            };

            // Keep row IDs in RowVec
            return Ok(raw_rows
                .into_iter()
                .map(|(row_id, row)| (row_id, self.normalize_row_to_schema(row, schema)))
                .collect());
        }

        // Has local versions - use same path as ordered (local changes are rare)
        // Early termination is already implemented in collect_visible_rows_with_limit
        // when there are local changes
        drop(txn_versions);
        self.collect_visible_rows_with_limit(filter, limit, offset)
    }

    pub(super) fn fetch_rows_by_ids_checked(
        &self,
        row_ids: &[i64],
        filter: &dyn Expression,
    ) -> Result<RowVec> {
        let candidates = self.collect_rows_by_ids(row_ids)?;
        let mut rows = RowVec::with_capacity(candidates.len());
        for (row_id, row) in candidates {
            if filter.evaluate(&row)? {
                rows.push((row_id, row));
            }
        }
        Ok(rows)
    }
}
