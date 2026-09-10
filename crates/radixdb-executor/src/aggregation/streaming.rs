use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Fast SUM implementation that bypasses the generic aggregate function
    /// Uses loop unrolling for better performance
    #[inline]
    pub(super) fn fast_sum_column(&self, rows: &[(i64, Row)], col_idx: usize) -> Value {
        if rows.is_empty() {
            return Value::null_unknown();
        }

        // Use parallel processing for large datasets
        #[cfg(feature = "parallel")]
        if rows.len() >= 10_000 {
            return self.fast_sum_column_parallel(rows, col_idx);
        }

        let mut sum_int: i64 = 0;
        let mut sum_float: f64 = 0.0;
        let mut has_float = false;
        let mut has_value = false;

        // Unroll loop by 4 for better CPU pipelining
        let chunks = rows.chunks_exact(4);
        let remainder = chunks.remainder();

        for chunk in chunks {
            for (_, row) in chunk {
                if let Some(val) = row.get(col_idx) {
                    match val {
                        Value::Integer(i) => {
                            has_value = true;
                            if has_float {
                                sum_float += *i as f64;
                            } else {
                                match sum_int.checked_add(*i) {
                                    Some(value) => sum_int = value,
                                    None => {
                                        has_float = true;
                                        sum_float = sum_int as f64 + *i as f64;
                                    }
                                }
                            }
                        }
                        Value::Float(f) => {
                            has_value = true;
                            if !has_float {
                                has_float = true;
                                sum_float = sum_int as f64;
                            }
                            sum_float += f;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Handle remainder
        for (_, row) in remainder {
            if let Some(val) = row.get(col_idx) {
                match val {
                    Value::Integer(i) => {
                        has_value = true;
                        if has_float {
                            sum_float += *i as f64;
                        } else {
                            match sum_int.checked_add(*i) {
                                Some(value) => sum_int = value,
                                None => {
                                    has_float = true;
                                    sum_float = sum_int as f64 + *i as f64;
                                }
                            }
                        }
                    }
                    Value::Float(f) => {
                        has_value = true;
                        if !has_float {
                            has_float = true;
                            sum_float = sum_int as f64;
                        }
                        sum_float += f;
                    }
                    _ => {}
                }
            }
        }

        if !has_value {
            Value::null_unknown()
        } else if has_float {
            Value::Float(sum_float)
        } else {
            Value::Integer(sum_int)
        }
    }

    /// Parallel SUM implementation using Rayon
    #[cfg(feature = "parallel")]
    #[inline]
    pub(super) fn fast_sum_column_parallel(&self, rows: &[(i64, Row)], col_idx: usize) -> Value {
        let chunk_size = (rows.len() / rayon::current_num_threads()).max(1000);

        // Process in parallel, collecting (sum_int, sum_float, has_float, has_value)
        let results: Vec<(i64, f64, bool, bool)> = rows
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut sum_int: i64 = 0;
                let mut sum_float: f64 = 0.0;
                let mut has_float = false;
                let mut has_value = false;

                for (_, row) in chunk {
                    if let Some(val) = row.get(col_idx) {
                        match val {
                            Value::Integer(i) => {
                                has_value = true;
                                if has_float {
                                    sum_float += *i as f64;
                                } else {
                                    match sum_int.checked_add(*i) {
                                        Some(value) => sum_int = value,
                                        None => {
                                            has_float = true;
                                            sum_float = sum_int as f64 + *i as f64;
                                        }
                                    }
                                }
                            }
                            Value::Float(f) => {
                                has_value = true;
                                if !has_float {
                                    has_float = true;
                                    sum_float = sum_int as f64;
                                }
                                sum_float += f;
                            }
                            _ => {}
                        }
                    }
                }

                (sum_int, sum_float, has_float, has_value)
            })
            .collect();

        // Merge results
        let mut total_int: i64 = 0;
        let mut total_float: f64 = 0.0;
        let mut any_float = false;
        let mut any_value = false;

        for (si, sf, hf, hv) in results {
            if hv {
                any_value = true;
                if hf || any_float {
                    any_float = true;
                    if hf {
                        total_float += sf;
                    } else {
                        total_float += si as f64;
                    }
                } else {
                    match total_int.checked_add(si) {
                        Some(value) => total_int = value,
                        None => {
                            any_float = true;
                            total_float += total_int as f64 + si as f64;
                            total_int = 0;
                        }
                    }
                }
            }
        }

        // If we switched to float mid-way, add the integer total
        if any_float && total_int != 0 {
            total_float += total_int as f64;
        }

        if !any_value {
            Value::null_unknown()
        } else if any_float {
            Value::Float(total_float)
        } else {
            Value::Integer(total_int)
        }
    }

    /// Fast AVG implementation
    #[inline]
    pub(super) fn fast_avg_column(&self, rows: &[(i64, Row)], col_idx: usize) -> Value {
        if rows.is_empty() {
            return Value::null_unknown();
        }

        // Use parallel processing for large datasets
        #[cfg(feature = "parallel")]
        if rows.len() >= 10_000 {
            return self.fast_avg_column_parallel(rows, col_idx);
        }

        let mut sum: f64 = 0.0;
        let mut count: i64 = 0;

        for (_, row) in rows {
            if let Some(val) = row.get(col_idx) {
                match val {
                    Value::Integer(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
        }

        if count == 0 {
            Value::null_unknown()
        } else {
            Value::Float(sum / count as f64)
        }
    }

    /// Parallel AVG implementation
    #[cfg(feature = "parallel")]
    #[inline]
    pub(super) fn fast_avg_column_parallel(&self, rows: &[(i64, Row)], col_idx: usize) -> Value {
        let chunk_size = (rows.len() / rayon::current_num_threads()).max(1000);

        // Process in parallel, collecting (sum, count)
        let results: Vec<(f64, i64)> = rows
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut sum: f64 = 0.0;
                let mut count: i64 = 0;

                for (_, row) in chunk {
                    if let Some(val) = row.get(col_idx) {
                        match val {
                            Value::Integer(i) => {
                                sum += *i as f64;
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            _ => {}
                        }
                    }
                }

                (sum, count)
            })
            .collect();

        // Merge results
        let mut total_sum: f64 = 0.0;
        let mut total_count: i64 = 0;

        for (s, c) in results {
            total_sum += s;
            total_count += c;
        }

        if total_count == 0 {
            Value::null_unknown()
        } else {
            Value::Float(total_sum / total_count as f64)
        }
    }

    /// Check if an expression is a PURE aggregate function call.
    ///
    /// Returns true only for:
    /// - `COUNT(*)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)` directly
    /// - Same with alias: `COUNT(*) AS cnt`
    ///
    /// Returns false for:
    /// - `SUM(col) + 10` (wrapped in Infix)
    /// - `-SUM(col)` (wrapped in Prefix)
    /// - `col * 2` (not an aggregate)
    pub(super) fn is_pure_aggregate_expression(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall(func) => {
                // Check if it's an aggregate function
                matches!(
                    func.function.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "MIN" | "MAX" | "AVG"
                )
            }
            Expression::Aliased(aliased) => {
                // Check the inner expression
                Self::is_pure_aggregate_expression(&aliased.expression)
            }
            _ => false,
        }
    }

    /// Try to compute aggregates directly on the table without materializing rows.
    ///
    /// This is the "deferred aggregation" optimization for simple queries like:
    /// - `SELECT COUNT(*) FROM table`
    /// - `SELECT SUM(col), MIN(col), MAX(col) FROM table`
    ///
    /// Returns the original source untouched when the optimization cannot be
    /// applied. If the first row has already been inspected, the rejection
    /// wraps it so the caller can continue the same source exactly once.
    /// Returns Some(result) if the aggregates were computed directly.
    ///
    /// # Eligibility
    /// - No WHERE clause (or simplified to nothing)
    /// - No GROUP BY clause
    /// - No HAVING clause
    /// - No window functions
    /// - Simple column aggregates only (no expressions like SUM(a+b))
    /// - No DISTINCT
    /// - No ORDER BY on aggregates (like STRING_AGG with ORDER BY)
    /// - No FILTER clause on aggregates
    pub(crate) fn try_aggregation_pushdown(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        stmt: &SelectStatement,
        _ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> Result<Option<Box<dyn radixdb_storage::traits::QueryResult>>> {
        // classification is passed from caller to avoid redundant cache lookups

        // Quick eligibility checks using cached classification
        if classification.has_where {
            return Ok(None);
        }
        if classification.has_group_by {
            return Ok(None);
        }
        if classification.has_having {
            return Ok(None);
        }
        if classification.has_window_functions {
            return Ok(None);
        }

        // CRITICAL: Check that each column expression is a PURE aggregate function
        // We cannot pushdown expressions like SUM(val) + 10, -SUM(val), etc.
        // Only handle direct function calls: SUM(val), COUNT(*), MAX(val), etc.
        for col in &stmt.columns {
            if !Self::is_pure_aggregate_expression(col) {
                return Ok(None);
            }
        }

        // Parse aggregations
        let (aggregations, non_agg_columns) = self.parse_aggregations(stmt)?;

        // Must have only aggregations, no regular columns
        if !non_agg_columns.is_empty() {
            return Ok(None);
        }
        if aggregations.is_empty() {
            return Ok(None);
        }

        // Check all aggregations are simple (no expression, no ORDER BY, no FILTER)
        // COUNT(DISTINCT col) is allowed if column has an index
        for agg in &aggregations {
            if agg.expression.is_some() || !agg.order_by.is_empty() || agg.filter.is_some() {
                return Ok(None);
            }
            // COUNT(DISTINCT col) is allowed, other DISTINCT aggregates are not
            if agg.distinct && agg.name != "COUNT" {
                return Ok(None);
            }
            // Only support COUNT, SUM, MIN, MAX, AVG
            match agg.name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => {}
                _ => return Ok(None),
            }
        }

        // Build column index map using schema's cached lowercase column names
        let schema_lower = table.schema().column_names_lower_arc();
        let col_index_map: StringMap<usize> = schema_lower
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i))
            .collect();

        // Compute each aggregate
        // Use CompactVec directly to avoid Vec→CompactVec conversion
        let mut result_values: CompactVec<Value> = CompactVec::with_capacity(aggregations.len());
        let mut result_columns: Vec<String> = Vec::with_capacity(aggregations.len());

        for agg in &aggregations {
            result_columns.push(agg.get_column_name());

            match agg.name.as_str() {
                "COUNT" => {
                    if agg.distinct {
                        // COUNT(DISTINCT col) - try to get count from index without cloning values
                        if let Some(count) = table.get_partition_count(&agg.column_lower) {
                            // get_partition_count already excludes NULL values per SQL standard
                            result_values.push(Value::Integer(count as i64));
                        } else {
                            // No index on this column, can't pushdown
                            return Ok(None);
                        }
                    } else if agg.column == "*" {
                        // COUNT(*) may use metadata only when storage can prove
                        // the exact count for this transaction. Snapshot-backed
                        // segmented tables deliberately decline that proof:
                        // calling row_count() there used to materialize the
                        // entire visible table before returning one scalar.
                        // Decline this pushdown so the bounded exact-empty
                        // streaming path below owns the fallback instead.
                        let Some(count) = table.fast_row_count() else {
                            return Ok(None);
                        };
                        result_values.push(Value::Integer(count as i64));
                    } else {
                        // COUNT(col) - need to count non-null values, can't pushdown easily
                        return Ok(None);
                    }
                }
                "SUM" => {
                    let col_idx = col_index_map.get(&agg.column_lower).copied();
                    if let Some(idx) = col_idx {
                        if let Some(sum) = table.sum_column(idx) {
                            if sum.count() == 0 {
                                result_values.push(Value::null(radixdb_core::DataType::Float));
                            } else {
                                result_values.push(sum.into_value()?);
                            }
                        } else {
                            return Ok(None); // Pushdown not available
                        }
                    } else {
                        return Ok(None); // Column not found
                    }
                }
                "AVG" => {
                    let col_idx = col_index_map.get(&agg.column_lower).copied();
                    if let Some(idx) = col_idx {
                        if let Some(sum) = table.avg_column(idx) {
                            if sum.count() == 0 {
                                result_values.push(Value::null(radixdb_core::DataType::Float));
                            } else {
                                result_values.push(Value::Float(sum.as_f64() / sum.count() as f64));
                            }
                        } else {
                            return Ok(None); // Pushdown not available
                        }
                    } else {
                        return Ok(None); // Column not found
                    }
                }
                "MIN" => {
                    let col_idx = col_index_map.get(&agg.column_lower).copied();
                    if let Some(idx) = col_idx {
                        if let Some(min_val) = table.min_column(idx) {
                            result_values.push(
                                min_val.unwrap_or_else(|| {
                                    Value::null(radixdb_core::DataType::Integer)
                                }),
                            );
                        } else {
                            return Ok(None); // Pushdown not available
                        }
                    } else {
                        return Ok(None); // Column not found
                    }
                }
                "MAX" => {
                    let col_idx = col_index_map.get(&agg.column_lower).copied();
                    if let Some(idx) = col_idx {
                        if let Some(max_val) = table.max_column(idx) {
                            result_values.push(
                                max_val.unwrap_or_else(|| {
                                    Value::null(radixdb_core::DataType::Integer)
                                }),
                            );
                        } else {
                            return Ok(None); // Pushdown not available
                        }
                    } else {
                        return Ok(None); // Column not found
                    }
                }
                _ => return Ok(None),
            }
        }

        // Build result
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));
        Ok(Some(Box::new(crate::result::ExecutorResult::new(
            result_columns,
            rows,
        ))))
    }

    /// Try to push filtered aggregation (WHERE + aggregates) directly to the storage layer.
    ///
    /// This handles queries like:
    /// - `SELECT COUNT(*) FROM orders WHERE status = 'shipped'`
    /// - `SELECT SUM(amount), AVG(amount) FROM sales WHERE region = 'US'`
    ///
    /// The WHERE clause is converted to a storage expression and passed alongside the
    /// aggregate operations to `Table::compute_filtered_aggregates`, which can scan and
    /// aggregate in a single pass without materializing Row objects in the executor.
    ///
    /// # Eligibility
    /// - Must have WHERE and aggregation, no HAVING/window functions/joins/GROUP BY
    /// - No DISTINCT aggregates, no subqueries or parameters in WHERE
    /// - All SELECT columns must be pure aggregate function calls
    ///
    /// # Returns
    /// - `Ok(Some(result))` if the pushdown was applied
    /// - `Ok(None)` if the query is not eligible (falls through to next path)
    pub(crate) fn try_filtered_aggregation_pushdown(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
        columns: &[String],
    ) -> Result<Option<Box<dyn radixdb_storage::traits::QueryResult>>> {
        use radixdb_storage::mvcc::version_store::AggregateOp;

        // --- Eligibility checks ---

        if !classification.has_where {
            return Ok(None);
        }
        if !classification.has_aggregation {
            return Ok(None);
        }
        if classification.has_having {
            return Ok(None);
        }
        if classification.has_window_functions {
            return Ok(None);
        }
        if classification.has_joins {
            return Ok(None);
        }
        if classification.has_group_by {
            return Ok(None);
        }
        // Parameter syntax alone is not a reason to reject the storage path:
        // `try_pushdown(..., Some(ctx))` below resolves only values that are
        // actually bound in this execution context. Missing/unresolved values
        // cannot become a StorageExpr and therefore still fall back safely.
        // Subqueries in WHERE cannot be pushed to storage
        if classification.where_has_subqueries {
            return Ok(None);
        }

        // All SELECT columns must be pure aggregate function calls
        for col in &stmt.columns {
            if !Self::is_pure_aggregate_expression(col) {
                return Ok(None);
            }
        }

        // Parse aggregations to validate and extract details
        let (aggregations, non_agg_columns) = self.parse_aggregations(stmt)?;

        // Must have only aggregations, no regular columns
        if !non_agg_columns.is_empty() || aggregations.is_empty() {
            return Ok(None);
        }

        // Check all aggregations are simple (no expression, no ORDER BY, no FILTER, no DISTINCT)
        for agg in &aggregations {
            if agg.expression.is_some() || !agg.order_by.is_empty() || agg.filter.is_some() {
                return Ok(None);
            }
            if agg.distinct {
                return Ok(None);
            }
            match agg.name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => {}
                _ => return Ok(None),
            }
        }

        // --- Convert WHERE clause to storage expression ---

        let where_expr = match stmt.where_clause.as_ref() {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let schema = table.schema();
        let (storage_expr, needs_memory_filter) =
            crate::pushdown::try_pushdown(where_expr, schema, Some(ctx));

        // Bail when the WHERE clause is only partially pushed to storage.
        // The residual predicate would be ignored, producing wrong aggregates.
        if needs_memory_filter {
            return Ok(None);
        }

        let storage_expr = match storage_expr {
            Some(expr) => expr,
            None => return Ok(None), // Cannot convert WHERE to storage expression
        };

        // --- Build (AggregateOp, column_index) pairs ---

        let col_map: FxHashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        let mut agg_ops: Vec<(AggregateOp, usize)> = Vec::with_capacity(aggregations.len());
        let mut result_columns: Vec<String> = Vec::with_capacity(aggregations.len());

        for agg in &aggregations {
            result_columns.push(agg.get_column_name());

            let (op, col_idx) = match agg.name.as_str() {
                "COUNT" => {
                    if agg.column == "*" {
                        (AggregateOp::CountStar, 0)
                    } else if let Some(&idx) = col_map.get(agg.column_lower.as_str()) {
                        (AggregateOp::Count, idx)
                    } else {
                        return Ok(None);
                    }
                }
                "SUM" => {
                    if let Some(&idx) = col_map.get(agg.column_lower.as_str()) {
                        (AggregateOp::Sum, idx)
                    } else {
                        return Ok(None);
                    }
                }
                "AVG" => {
                    if let Some(&idx) = col_map.get(agg.column_lower.as_str()) {
                        (AggregateOp::Avg, idx)
                    } else {
                        return Ok(None);
                    }
                }
                "MIN" => {
                    if let Some(&idx) = col_map.get(agg.column_lower.as_str()) {
                        (AggregateOp::Min, idx)
                    } else {
                        return Ok(None);
                    }
                }
                "MAX" => {
                    if let Some(&idx) = col_map.get(agg.column_lower.as_str()) {
                        (AggregateOp::Max, idx)
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            };
            agg_ops.push((op, col_idx));
        }

        // Narrow metadata-only operator for the benchmark-critical shape:
        // `COUNT(*)` over an exact conjunction of INTEGER primary-key bounds.
        // The Table contract returns None when its current MVCC/segment state
        // cannot prove the result, so all broader SQL shapes keep the existing
        // filtered aggregate path below.
        if agg_ops.as_slice() == [(AggregateOp::CountStar, 0)] {
            if let Some(pk_idx) = schema.pk_column_index() {
                if let Some(pk_column) = schema.columns.get(pk_idx) {
                    if pk_column.data_type == radixdb_core::DataType::Integer {
                        let comparisons = storage_expr.collect_comparisons();
                        if let Some(range) = radixdb_storage::traits::table::IntegerPrimaryKeyRange::from_conjunctive_comparisons(
                            &comparisons,
                            pk_column.name_lower.as_str(),
                            true,
                        ) {
                            if let Some(metadata_count) =
                                table.count_visible_integer_primary_key_range(&range)
                            {
                                let count = metadata_count?;
                                let count = i64::try_from(count).map_err(|_| {
                                    radixdb_core::Error::internal(
                                        "metadata primary-key count exceeds INTEGER result range",
                                    )
                                })?;
                                let mut rows = RowVec::with_capacity(1);
                                rows.push((
                                    0,
                                    Row::from_compact_vec(CompactVec::from(vec![Value::Integer(
                                        count,
                                    )])),
                                ));
                                return Ok(Some(Box::new(crate::result::ExecutorResult::new(
                                    result_columns,
                                    rows,
                                ))));
                            }
                        }
                    }
                }
            }
        }

        // --- Call storage-level filtered aggregation ---

        let values = match table.compute_filtered_aggregates(&agg_ops, storage_expr.as_ref()) {
            Some(v) => v,
            None => return Ok(None),
        };

        // --- Build result ---

        let mut result_values: CompactVec<Value> = CompactVec::with_capacity(values.len());
        for v in values {
            result_values.push(v);
        }
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));
        Ok(Some(Box::new(crate::result::ExecutorResult::new(
            result_columns,
            rows,
        ))))
    }

    /// Try to compute global aggregates using streaming (no row materialization).
    ///
    /// This is a fallback for queries that can't use direct aggregation pushdown,
    /// but can still avoid collecting all rows by streaming through a scanner.
    ///
    /// Examples of eligible queries:
    /// - `SELECT AVG(col) * 100 FROM table`  (expression wrapping aggregate)
    /// - `SELECT SUM(col), COUNT(*) FROM table`  (multiple simple aggregates)
    ///
    /// # Returns
    /// - `Some(result)` if streaming aggregation was used
    /// - `None` if the query is not eligible for streaming
    pub(crate) fn try_streaming_global_aggregation(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> Result<Option<Box<dyn radixdb_storage::traits::QueryResult>>> {
        // Quick eligibility checks using cached classification
        if classification.has_where {
            return Ok(None);
        }
        if classification.has_group_by {
            return Ok(None);
        }
        if classification.has_having {
            return Ok(None);
        }
        if classification.has_window_functions {
            return Ok(None);
        }
        if classification.has_order_by {
            return Ok(None);
        }
        if classification.has_limit {
            return Ok(None);
        }

        // Parse aggregations - allow non-pure expressions (like AVG(col) * 100)
        let (aggregations, non_agg_columns) = self.parse_aggregations(stmt)?;

        // Must have only aggregations, no regular columns
        if !non_agg_columns.is_empty() {
            return Ok(None);
        }
        if aggregations.is_empty() {
            return Ok(None);
        }

        // Check all aggregations are simple enough for streaming
        // (no ORDER BY, no FILTER, no DISTINCT except COUNT, no expression arguments)
        for agg in &aggregations {
            if !agg.order_by.is_empty() || agg.filter.is_some() {
                return Ok(None);
            }
            if agg.distinct && agg.name != "COUNT" {
                return Ok(None);
            }
            // Can't handle expression arguments like SUM(a + b) - need full evaluation
            if agg.expression.is_some() {
                return Ok(None);
            }
            // Only support COUNT, SUM, MIN, MAX, AVG for streaming
            match agg.name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => {}
                _ => return Ok(None),
            }
        }

        // Build column index map using schema's cached lowercase column names
        let schema_lower = table.schema().column_names_lower_arc();
        let col_index_map: StringMap<usize> = schema_lower
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i))
            .collect();

        // Pre-compute column indices for each aggregation
        let agg_col_indices: Vec<Option<usize>> = aggregations
            .iter()
            .map(|agg| {
                if agg.column == "*" || agg.expression.is_some() {
                    None
                } else {
                    Self::lookup_column_index(&agg.column_lower, &col_index_map)
                }
            })
            .collect();

        let (scan_columns, agg_projected_indices) =
            Self::build_streaming_aggregate_projection(&agg_col_indices);

        // FAST PATH: Try deferred aggregation (no row materialization)
        // This bypasses the lazy scanner entirely for simple aggregates
        let can_use_deferred = aggregations.iter().all(|agg| !agg.distinct);
        if can_use_deferred {
            let mut deferred_values: Vec<Option<Value>> = Vec::with_capacity(aggregations.len());
            let mut all_succeeded = true;

            for (i, agg) in aggregations.iter().enumerate() {
                let col_idx = agg_col_indices[i];
                let value = match agg.name.as_str() {
                    "COUNT" => {
                        if agg.column == "*" {
                            // Only metadata-proven counts belong in the
                            // deferred path. Otherwise keep the scanner-based
                            // exact-empty fallback bounded and fallible.
                            table
                                .fast_row_count()
                                .map(|count| Value::Integer(count as i64))
                        } else {
                            // COUNT(col) - need scanner for NULL checking
                            // Could optimize with a dedicated count_non_null method
                            None
                        }
                    }
                    "SUM" => {
                        if let Some(idx) = col_idx {
                            if let Some(sum) = table.sum_column(idx) {
                                if sum.count() == 0 {
                                    Some(Value::null(radixdb_core::DataType::Float))
                                } else {
                                    Some(sum.into_value()?)
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "AVG" => {
                        if let Some(idx) = col_idx {
                            if let Some(sum) = table.avg_column(idx) {
                                if sum.count() == 0 {
                                    Some(Value::null(radixdb_core::DataType::Float))
                                } else {
                                    Some(Value::Float(sum.as_f64() / sum.count() as f64))
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "MIN" => col_idx.and_then(|idx| {
                        table.min_column(idx).map(|min_opt| {
                            min_opt.unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer))
                        })
                    }),
                    "MAX" => col_idx.and_then(|idx| {
                        table.max_column(idx).map(|max_opt| {
                            max_opt.unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer))
                        })
                    }),
                    _ => None,
                };

                if let Some(v) = value {
                    deferred_values.push(Some(v));
                } else {
                    all_succeeded = false;
                    break;
                }
            }

            if all_succeeded && deferred_values.len() == aggregations.len() {
                // Build result from deferred values
                // Use CompactVec directly to avoid Vec→CompactVec conversion
                let mut agg_result_values: CompactVec<Value> =
                    CompactVec::with_capacity(aggregations.len());
                let mut agg_result_columns: Vec<String> = Vec::with_capacity(aggregations.len());

                for (i, agg) in aggregations.iter().enumerate() {
                    agg_result_columns.push(agg.get_column_name());
                    agg_result_values.push(deferred_values[i].take().unwrap());
                }

                // Apply post-aggregation expressions if needed
                let agg_row = Row::from_compact_vec(agg_result_values);
                let mut agg_rows = RowVec::with_capacity(1);
                agg_rows.push((0, agg_row));
                let (final_columns, final_rows) = self.apply_post_aggregation_expressions(
                    stmt,
                    ctx,
                    agg_result_columns,
                    agg_rows,
                )?;

                return Ok(Some(Box::new(crate::result::ExecutorResult::new(
                    final_columns,
                    final_rows,
                ))));
            }
        }

        // SLOW PATH: Fall back to scanner-based streaming
        // Initialize aggregate states
        struct AggState {
            sum: f64,
            count: i64,
            min: Option<Value>,
            max: Option<Value>,
            distinct_set: Option<ValueSet>,
        }

        let mut states: Vec<AggState> = aggregations
            .iter()
            .map(|agg| AggState {
                sum: 0.0,
                count: 0,
                min: None,
                max: None,
                distinct_set: agg.distinct.then(ValueSet::default),
            })
            .collect();

        // Get a scanner and stream through rows. Request only columns used by
        // aggregate arguments; COUNT(*) has no column dependency and is usually
        // handled by the deferred fast path above. If the fallback still reaches
        // this point with no column dependencies, use the exact-empty scan
        // boundary rather than the historical `scan([]) == SELECT *` contract.
        let mut scanner = if scan_columns.is_empty() {
            table.scan_exact_projection(&scan_columns, None)?
        } else {
            table.scan(&scan_columns, None)?
        };

        while scanner.next() {
            let row = scanner.row();

            for (i, agg) in aggregations.iter().enumerate() {
                let state = &mut states[i];

                if agg.column == "*" {
                    // COUNT(*)
                    state.count += 1;
                    continue;
                }

                let projected_col_idx = match agg_projected_indices[i] {
                    Some(projected) => projected,
                    None => continue,
                };

                let value = match row.get(projected_col_idx) {
                    Some(v) if !v.is_null() => v,
                    _ => continue, // Skip NULL values
                };

                // Handle DISTINCT
                if let Some(ref mut distinct_set) = state.distinct_set {
                    if !track_distinct_value(distinct_set, value) {
                        continue; // Already seen this value
                    }
                }

                match agg.name.as_str() {
                    "COUNT" => {
                        state.count += 1;
                    }
                    "SUM" | "AVG" => {
                        let num = match value {
                            Value::Integer(i) => *i as f64,
                            Value::Float(f) => *f,
                            _ => continue,
                        };
                        state.sum += num;
                        state.count += 1;
                    }
                    "MIN" => {
                        let is_smaller = match (&state.min, value) {
                            (None, _) => true,
                            (Some(current), new) => {
                                new.compare(current).unwrap_or(std::cmp::Ordering::Equal)
                                    == std::cmp::Ordering::Less
                            }
                        };
                        if is_smaller {
                            state.min = Some(value.clone());
                        }
                    }
                    "MAX" => {
                        let is_larger = match (&state.max, value) {
                            (None, _) => true,
                            (Some(current), new) => {
                                new.compare(current).unwrap_or(std::cmp::Ordering::Equal)
                                    == std::cmp::Ordering::Greater
                            }
                        };
                        if is_larger {
                            state.max = Some(value.clone());
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(e) = scanner.err() {
            return Err(radixdb_core::Error::internal(format!("scan error: {}", e)));
        }
        scanner.close()?;

        // Build intermediate result columns (raw aggregate values)
        // Use CompactVec directly to avoid Vec→CompactVec conversion
        let mut agg_result_values: CompactVec<Value> =
            CompactVec::with_capacity(aggregations.len());
        let mut agg_result_columns: Vec<String> = Vec::with_capacity(aggregations.len());

        for (i, agg) in aggregations.iter().enumerate() {
            let state = &states[i];
            agg_result_columns.push(agg.get_column_name());

            let value = match agg.name.as_str() {
                "COUNT" => Value::Integer(state.count),
                "SUM" => {
                    if state.count == 0 {
                        Value::null(radixdb_core::DataType::Float)
                    } else if state.sum.fract() == 0.0 && state.sum.abs() < i64::MAX as f64 {
                        Value::Integer(state.sum as i64)
                    } else {
                        Value::Float(state.sum)
                    }
                }
                "AVG" => {
                    if state.count == 0 {
                        Value::null(radixdb_core::DataType::Float)
                    } else {
                        Value::Float(state.sum / state.count as f64)
                    }
                }
                "MIN" => state
                    .min
                    .clone()
                    .unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer)),
                "MAX" => state
                    .max
                    .clone()
                    .unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer)),
                _ => Value::null(radixdb_core::DataType::Integer),
            };
            agg_result_values.push(value);
        }

        // Apply post-aggregation expressions if needed
        // This handles cases like AVG(col) * 100
        let agg_row = Row::from_compact_vec(agg_result_values);
        let mut agg_rows = RowVec::with_capacity(1);
        agg_rows.push((0, agg_row));
        let (final_columns, final_rows) =
            self.apply_post_aggregation_expressions(stmt, ctx, agg_result_columns, agg_rows)?;

        Ok(Some(Box::new(crate::result::ExecutorResult::new(
            final_columns,
            final_rows,
        ))))
    }

    pub(super) fn build_streaming_aggregate_projection(
        agg_col_indices: &[Option<usize>],
    ) -> (Vec<usize>, Vec<Option<usize>>) {
        let mut scan_columns = Vec::new();
        let mut projected_positions: FxHashMap<usize, usize> = FxHashMap::default();
        let agg_projected_indices: Vec<Option<usize>> = agg_col_indices
            .iter()
            .map(|col_idx| {
                col_idx.map(|idx| {
                    if let Some(pos) = projected_positions.get(&idx) {
                        *pos
                    } else {
                        let pos = scan_columns.len();
                        scan_columns.push(idx);
                        projected_positions.insert(idx, pos);
                        pos
                    }
                })
            })
            .collect();

        (scan_columns, agg_projected_indices)
    }

    pub(super) fn plan_streaming_derived_table_aggregation(
        &self,
        stmt: &SelectStatement,
        classification: &std::sync::Arc<QueryClassification>,
        source_columns: &[String],
    ) -> Result<Option<DerivedAggregationPlan>> {
        // Quick eligibility checks
        if !classification.has_group_by {
            return Ok(None);
        }
        if classification.has_having {
            return Ok(None);
        }
        if classification.has_window_functions {
            return Ok(None);
        }
        if classification.has_order_by
            || classification.has_limit
            || classification.has_offset
            || classification.has_distinct
            || classification.has_distinct_on
            || !stmt.set_operations.is_empty()
        {
            return Ok(None);
        }
        // Skip ROLLUP/CUBE/GROUPING SETS
        if stmt.group_by.modifier != radixdb_sql::ast::GroupByModifier::None {
            return Ok(None);
        }

        // Only single-column GROUP BY for this optimization
        if stmt.group_by.columns.len() != 1 {
            return Ok(None);
        }

        // GROUP BY column must be a simple column reference (identifier)
        let group_col_name = match &stmt.group_by.columns[0] {
            radixdb_sql::ast::Expression::Identifier(id) => id.value_lower.to_string(),
            radixdb_sql::ast::Expression::QualifiedIdentifier(qid) => {
                qid.name.value_lower.to_string()
            }
            _ => return Ok(None),
        };

        // Build column index map from result columns
        let col_index_map: StringMap<usize> = source_columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.to_lowercase(), i))
            .collect();

        // Find GROUP BY column index
        let group_col_idx = match col_index_map.get(&group_col_name) {
            Some(&idx) => idx,
            None => return Ok(None),
        };

        // Parse aggregations
        let (aggregations, non_agg_columns) = self.parse_aggregations(stmt)?;

        // Must have only aggregations plus the GROUP BY column (no other regular columns)
        // Non-agg columns must be exactly the GROUP BY column
        if non_agg_columns.len() > 1 {
            return Ok(None);
        }
        if non_agg_columns.len() == 1 && !non_agg_columns[0].eq_ignore_ascii_case(&group_col_name) {
            return Ok(None);
        }

        // Check all aggregations are simple enough for streaming
        let simple_aggs: Vec<Option<SimpleAgg>> = aggregations
            .iter()
            .map(|agg| {
                // Must not have DISTINCT, FILTER, ORDER BY, or expression.
                // COUNT(DISTINCT col) is deliberately rejected too: the
                // current streaming state has no per-group distinct set.
                if agg.distinct
                    || agg.filter.is_some()
                    || !agg.order_by.is_empty()
                    || agg.expression.is_some()
                {
                    return None;
                }

                match agg.name.to_uppercase().as_str() {
                    "COUNT" => {
                        if agg.column == "*" {
                            Some(SimpleAgg::Count(None))
                        } else {
                            Self::lookup_column_index(&agg.column_lower, &col_index_map)
                                .map(|idx| SimpleAgg::Count(Some(idx)))
                        }
                    }
                    "SUM" => {
                        if agg.column == "*" {
                            None
                        } else {
                            Self::lookup_column_index(&agg.column_lower, &col_index_map)
                                .map(SimpleAgg::Sum)
                        }
                    }
                    "AVG" => {
                        if agg.column == "*" {
                            None
                        } else {
                            Self::lookup_column_index(&agg.column_lower, &col_index_map)
                                .map(SimpleAgg::Avg)
                        }
                    }
                    "MIN" => {
                        if agg.column == "*" {
                            None
                        } else {
                            Self::lookup_column_index(&agg.column_lower, &col_index_map)
                                .map(SimpleAgg::Min)
                        }
                    }
                    "MAX" => {
                        if agg.column == "*" {
                            None
                        } else {
                            Self::lookup_column_index(&agg.column_lower, &col_index_map)
                                .map(SimpleAgg::Max)
                        }
                    }
                    _ => None,
                }
            })
            .collect();

        // All aggregates must be resolved for streaming path
        if simple_aggs.iter().any(|a| a.is_none()) {
            return Ok(None);
        }

        let simple_aggs: Vec<SimpleAgg> = simple_aggs.into_iter().map(|a| a.unwrap()).collect();
        Ok(Some(DerivedAggregationPlan {
            group_col_name,
            group_col_idx,
            aggregations,
            simple_aggs,
        }))
    }

    /// Try streaming aggregation for derived tables (FROM subqueries).
    ///
    /// OPTIMIZATION: For simple GROUP BY + COUNT(*) on derived tables without WHERE clause,
    /// stream directly to aggregation HashMap without materializing all rows first.
    /// This reduces memory allocations from O(N) to O(groups).
    ///
    /// Returns the original source untouched when the optimization cannot be
    /// applied. If the first row has already been inspected, the rejection
    /// preserves that row so the caller can continue the same source exactly
    /// once.
    ///
    /// Supported patterns:
    /// - Single-column GROUP BY (column reference)
    /// - Simple aggregates: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX
    /// - No HAVING, DISTINCT, FILTER, aggregate ORDER BY, outer ORDER BY/LIMIT
    pub(crate) fn try_streaming_derived_table_aggregation(
        &self,
        mut result: Box<dyn QueryResult>,
        stmt: &SelectStatement,
        classification: &std::sync::Arc<QueryClassification>,
        ctx: &ExecutionContext,
    ) -> Result<DerivedAggregationAttempt> {
        use radixdb_core::SmartString;
        use smallvec::SmallVec;

        let source_columns = result.columns().to_vec();
        let Some(plan) =
            self.plan_streaming_derived_table_aggregation(stmt, classification, &source_columns)?
        else {
            return Ok(DerivedAggregationAttempt::Rejected(result));
        };
        let DerivedAggregationPlan {
            group_col_name,
            group_col_idx,
            aggregations,
            simple_aggs,
        } = plan;
        let num_aggs = simple_aggs.len();

        // State for streaming aggregation
        type AggVec<T> = SmallVec<[T; 4]>;

        #[derive(Clone)]
        struct StreamGroupState {
            numeric_states: AggVec<NumericAccumulator>,
            counts: AggVec<i64>,
            min_values: AggVec<Option<Value>>,
            max_values: AggVec<Option<Value>>,
        }

        // Template for new group state
        let state_template = StreamGroupState {
            numeric_states: smallvec::smallvec![NumericAccumulator::default(); num_aggs],
            counts: smallvec::smallvec![0; num_aggs],
            min_values: smallvec::smallvec![None; num_aggs],
            max_values: smallvec::smallvec![None; num_aggs],
        };

        // Sample first row to detect key type
        if !result.next() {
            if let Some(err) = result.last_error() {
                return Err(err);
            }
            // Empty result - return empty aggregation
            let mut result_columns = Vec::with_capacity(1 + aggregations.len());
            result_columns.push(group_col_name.clone());
            for agg in &aggregations {
                let col_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    agg.get_expression_name()
                };
                result_columns.push(col_name);
            }
            return Ok(DerivedAggregationAttempt::Applied(Box::new(
                crate::result::ExecutorResult::new(result_columns, RowVec::new()),
            )));
        }

        // Use string fast path for Text values (common for derived tables with CASE expressions)
        let use_string_path = result
            .row()
            .get(group_col_idx)
            .map(|v| matches!(v, Value::Text(_)))
            .unwrap_or(false);

        if !use_string_path {
            let prefetched = result.take_row();
            return Ok(DerivedAggregationAttempt::Rejected(Box::new(
                crate::result::PrefetchedResult::new(prefetched, result),
            )));
        }

        {
            // String GROUP BY streaming path
            // OPTIMIZATION: Use hashbrown::HashMap with raw_entry_mut to avoid SmartString allocation on lookup
            type FxBuildHasher = std::hash::BuildHasherDefault<FxHasher>;
            let mut groups: hashbrown::HashMap<SmartString, StreamGroupState, FxBuildHasher> =
                hashbrown::HashMap::with_capacity_and_hasher(64, FxBuildHasher::default());
            let mut null_group: Option<StreamGroupState> = None;
            let first_row = result.row();

            // Process first row
            let process_row =
                |row: &Row,
                 groups: &mut hashbrown::HashMap<SmartString, StreamGroupState, FxBuildHasher>,
                 null_group: &mut Option<StreamGroupState>,
                 template: &StreamGroupState| {
                    let key_opt = match row.get(group_col_idx) {
                        Some(Value::Text(s)) => Some(s),
                        Some(Value::Null(_)) | None => None,
                        _ => return, // Skip non-text, non-NULL
                    };

                    let state = if let Some(key_str) = key_opt {
                        // OPTIMIZATION: Use raw_entry_mut to avoid SmartString allocation on lookup
                        // Only create SmartString when inserting a new group
                        let mut hasher = FxHasher::default();
                        std::hash::Hash::hash(key_str, &mut hasher);
                        let hash = hasher.finish();

                        let entry = groups
                            .raw_entry_mut()
                            .from_hash(hash, |k| k.as_str() == key_str);
                        match entry {
                            RawEntryMut::Occupied(o) => o.into_mut(),
                            RawEntryMut::Vacant(v) => {
                                v.insert_hashed_nocheck(
                                    hash,
                                    SmartString::new(key_str),
                                    template.clone(),
                                )
                                .1
                            }
                        }
                    } else {
                        if null_group.is_none() {
                            *null_group = Some(template.clone());
                        }
                        null_group.as_mut().unwrap()
                    };

                    // Accumulate aggregates
                    for (i, agg) in simple_aggs.iter().enumerate() {
                        match agg {
                            SimpleAgg::Count(_) => {
                                if agg.count_includes_row(row) {
                                    state.counts[i] += 1;
                                }
                            }
                            SimpleAgg::Sum(col_idx) | SimpleAgg::Avg(col_idx) => {
                                if let Some(value) = row.get(*col_idx) {
                                    state.numeric_states[i].accumulate(value);
                                }
                            }
                            SimpleAgg::Min(col_idx) => {
                                if let Some(value) = row.get(*col_idx) {
                                    if !value.is_null() {
                                        match &state.min_values[i] {
                                            None => state.min_values[i] = Some(value.clone()),
                                            Some(current) if value < current => {
                                                state.min_values[i] = Some(value.clone())
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            SimpleAgg::Max(col_idx) => {
                                if let Some(value) = row.get(*col_idx) {
                                    if !value.is_null() {
                                        match &state.max_values[i] {
                                            None => state.max_values[i] = Some(value.clone()),
                                            Some(current) if value > current => {
                                                state.max_values[i] = Some(value.clone())
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                };

            // Process first row (already fetched)
            process_row(first_row, &mut groups, &mut null_group, &state_template);

            // Stream through remaining rows
            let mut streamed_rows = 1u64;
            while result.next() {
                if streamed_rows.is_multiple_of(100) {
                    ctx.check_cancelled()?;
                }
                let row = result.row();
                process_row(row, &mut groups, &mut null_group, &state_template);
                streamed_rows += 1;
            }
            if let Some(err) = result.last_error() {
                return Err(err);
            }

            // Build result columns
            let mut result_columns = Vec::with_capacity(1 + aggregations.len());
            result_columns.push(group_col_name.clone());
            for agg in &aggregations {
                let col_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    agg.get_expression_name()
                };
                result_columns.push(col_name);
            }

            // Build result rows
            let build_row = |key_value: Value, state: StreamGroupState| -> Result<Row> {
                let mut values: radixdb_core::CompactVec<Value> =
                    radixdb_core::CompactVec::with_capacity(1 + simple_aggs.len());
                values.push(key_value);
                for (i, agg) in simple_aggs.iter().enumerate() {
                    let value = match agg {
                        SimpleAgg::Count(_) => Value::Integer(state.counts[i]),
                        SimpleAgg::Sum(_) => state.numeric_states[i].sum_result()?,
                        SimpleAgg::Avg(_) => state.numeric_states[i].average_result()?,
                        SimpleAgg::Min(_) => state.min_values[i]
                            .clone()
                            .unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer)),
                        SimpleAgg::Max(_) => state.max_values[i]
                            .clone()
                            .unwrap_or_else(|| Value::null(radixdb_core::DataType::Integer)),
                    };
                    values.push(value);
                }
                Ok(Row::from_compact_vec(values))
            };

            let mut result_rows = RowVec::with_capacity(groups.len() + 1);
            let mut row_id = 0i64;
            for (key, state) in groups.into_iter() {
                result_rows.push((row_id, build_row(Value::Text(key), state)?));
                row_id += 1;
            }
            if let Some(ng) = null_group {
                result_rows.push((row_id, build_row(Value::null_unknown(), ng)?));
            }

            Ok(DerivedAggregationAttempt::Applied(Box::new(
                crate::result::ExecutorResult::new(result_columns, result_rows),
            )))
        }
    }
}
