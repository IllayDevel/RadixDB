use super::*;

impl<'host, H: SubqueryHost + ?Sized> SubqueryExecutor<'host, H> {
    /// Check if index-nested-loop would be more efficient than semi-join for EXISTS.
    ///
    /// Returns true if:
    /// 1. There's a small LIMIT (< 500)
    /// 2. Inner table has index on correlation column
    ///
    /// With small LIMIT and early termination at the outer level, per-row EXISTS
    /// evaluation is faster because:
    /// - O(LIMIT × log(inner_size)) for index probe vs O(inner_size) for hash build
    /// - Example: LIMIT 100, inner=30K → 100×15=1500 ops vs 30000 ops
    ///
    /// For EXISTS with additional predicate (e.g., EXISTS ... WHERE o.user_id = u.id AND o.amount > 500):
    /// - Index lookup gets candidate row_ids for correlation
    /// - Rows are fetched in batches and predicate is evaluated with early exit
    /// - This is O(LIMIT × avg_rows_per_key × predicate_selectivity) which is still efficient
    pub(super) fn should_use_index_nested_loop(
        &self,
        info: &SemiJoinInfo,
        outer_limit: Option<i64>,
    ) -> bool {
        // Use index-nested-loop for small LIMIT queries WITHOUT additional predicates.
        //
        // IMPORTANT: If there's a non-correlated predicate (e.g., status = 'cancelled'),
        // semi-join is FASTER because:
        // 1. Semi-join executes the filtered inner query ONCE, builds a hash set
        // 2. Index NL would probe the index for EACH outer row, then filter
        //
        // Benchmark shows semi-join is 3x faster when additional predicates exist:
        // - Semi-join: ~290μs (execute filtered query once, O(1) hash lookups)
        // - Index NL:  ~940μs (per-row index probe + filter)
        //
        // Only use Index NL when:
        // 1. Small LIMIT (early termination benefit)
        // 2. NO additional predicates (pure correlation only)
        // 3. Index exists on correlation column
        const SMALL_LIMIT_THRESHOLD: i64 = 500;

        // If there's a non-correlated predicate, always use semi-join
        // The semi-join can efficiently filter by predicate in bulk
        if info.non_correlated_where.is_some() {
            return false;
        }

        // For pure correlation (no additional predicate), check if index NL is worth it
        if let Some(limit) = outer_limit {
            if limit > 0 && limit <= SMALL_LIMIT_THRESHOLD {
                // Check if inner table has an index on correlation column
                // Without index, per-row evaluation would be slow
                let table = match self.host.subquery_open_table(&info.inner_table) {
                    Ok(handle) => handle,
                    Err(_) => return false,
                };

                // Check for index on correlation column
                if table
                    .table
                    .get_index_on_column(&info.inner_column)
                    .is_some()
                {
                    return true;
                }
            }
        }

        // For larger queries or no index, use semi-join
        false
    }

    /// Check if index-nested-loop should be preferred over anti-join for NOT EXISTS.
    ///
    /// For NOT EXISTS, anti-join using HashJoinOperator is almost always more efficient
    /// than both index-nested-loop and InHashSet because:
    /// 1. HashJoinOperator does bulk hash table build/probe (cache-efficient)
    /// 2. No per-row expression evaluation overhead
    /// 3. Even with LIMIT, the bulk operation is faster than per-row checking
    ///
    /// The only case where we might prefer index-nested-loop is for VERY small LIMIT
    /// (e.g., LIMIT 10) with a highly selective index, but benchmarks show hash join
    /// is still faster in most cases.
    pub fn should_use_index_nested_loop_for_anti_join(
        &self,
        _info: &SemiJoinInfo,
        outer_limit: Option<i64>,
    ) -> bool {
        // For very small LIMIT (<= 10), index-nested-loop might be faster
        // because it can terminate very early
        if let Some(limit) = outer_limit {
            if limit <= 10 {
                return true;
            }
        }
        // For all other cases, prefer anti-join for NOT EXISTS
        false
    }

    /// Execute the semi-join optimization for an EXISTS subquery.
    ///
    /// Instead of executing the subquery for each outer row, we:
    /// 1. Execute the inner query once with non-correlated predicates
    /// 2. Collect all distinct values of the inner correlation column
    /// 3. Return an FxHashSet for fast O(1) lookups
    ///
    /// Results are cached to avoid re-execution for the same query within a single
    /// top-level query execution.
    pub fn execute_semi_join_optimization(
        &self,
        info: &SemiJoinInfo,
        ctx: &ExecutionContext,
    ) -> Result<CompactArc<ValueSet>> {
        // Build cache key hash from inner table, column, and WHERE predicate hash
        // Uses u64 hash to avoid any string allocation
        let pred_hash = info
            .non_correlated_where
            .as_ref()
            .map(|arc| compute_expression_hash(arc.as_ref()))
            .unwrap_or(0);
        let cache_key =
            compute_semi_join_cache_key(&info.inner_table, &info.inner_column, pred_hash);

        // Check cache first - return Arc directly (no clone needed)
        if let Some(cached) = get_cached_semi_join(cache_key) {
            return Ok(cached);
        }

        // Build SELECT inner_column FROM inner_table WHERE non_correlated_predicates
        // Use dummy_token_clone() to avoid allocations - token literal is not used during execution
        let inner_col_expr = Expression::Identifier(Identifier::new(
            dummy_token_clone(),
            info.inner_column.clone(),
        ));

        let table_source = Expression::TableSource(Box::new(SimpleTableSource {
            token: dummy_token_clone(),
            name: Identifier::new(dummy_token_clone(), info.inner_table.clone()),
            alias: info
                .inner_alias
                .as_ref()
                .map(|a| Identifier::new(dummy_token_clone(), a.clone())),
            as_of: None,
        }));

        let select_stmt = SelectStatement {
            token: dummy_token_clone(),
            // Don't use DISTINCT here - it's slower in RadixDB because it requires
            // additional hashing/sorting overhead. Instead, we collect into HashSet
            // which deduplicates more efficiently for this use case.
            distinct: false,
            distinct_on: vec![],
            columns: vec![inner_col_expr],
            with: None,
            table_expr: Some(Box::new(table_source)),
            where_clause: info
                .non_correlated_where
                .as_ref()
                .map(|arc| Box::new(arc.as_ref().clone())),
            group_by: GroupByClause {
                columns: vec![],
                modifier: GroupByModifier::None,
            },
            having: None,
            window_defs: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            set_operations: vec![],
        };

        // Execute the query with incremented depth to avoid creating new TimeoutGuard
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self
            .host
            .subquery_execute_select(&select_stmt, &subquery_ctx)?;

        // Collect values into Vec first (faster than direct FxHashSet insertion),
        // then convert to FxHashSet for deduplication and O(1) lookups
        let mut values_vec = Vec::with_capacity(10_000);
        while result.next() {
            let row = result.row();
            if let Some(value) = row.get(0) {
                if !value.is_null() {
                    values_vec.push(value.clone());
                }
            }
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }
        // Build FxHashSet from Vec - this deduplicates automatically
        let hash_set: ValueSet = values_vec.into_iter().collect();

        // Wrap in CompactArc once - no cloning needed
        let hash_set_arc = CompactArc::new(hash_set);

        // Cache for subsequent calls within this query (CompactArc clone is cheap)
        cache_semi_join_arc(
            cache_key,
            &info.inner_table,
            CompactArc::clone(&hash_set_arc),
        );

        Ok(hash_set_arc)
    }

    /// Execute NOT EXISTS as a true anti-join using HashJoinOperator.
    ///
    /// This is more efficient than the InHashSet approach because:
    /// 1. HashJoinOperator builds hash table once and probes in bulk
    /// 2. No per-row expression evaluation overhead
    /// 3. Better cache efficiency due to batch processing
    /// 4. Direct table access without going through full query pipeline
    ///
    /// # Arguments
    /// * `info` - SemiJoinInfo extracted from the NOT EXISTS subquery
    /// * `outer_rows` - Pre-materialized outer table rows
    /// * `outer_columns` - Column names for outer table
    /// * `_ctx` - Execution context (not used but kept for API consistency)
    ///
    /// # Returns
    /// Rows from outer table that have NO match in inner table (anti-join result)
    pub fn execute_anti_join(
        &self,
        info: &SemiJoinInfo,
        outer_rows: CompactArc<Vec<radixdb_core::Row>>,
        outer_columns: &[String],
        _ctx: &ExecutionContext,
    ) -> Result<radixdb_core::RowVec> {
        // Direct table access - much faster than going through execute_select
        let inner_handle = self.host.subquery_open_table(&info.inner_table)?;
        let inner_table = &inner_handle.table;

        // Convert non-correlated WHERE to storage expression for pushdown
        let storage_expr = info
            .non_correlated_where
            .as_ref()
            .and_then(|arc| convert_ast_to_storage_expr(arc.as_ref()));

        // Find the inner column index for join key extraction
        // Use schema's cached lowercase column names to avoid computing to_lowercase()
        let inner_schema = inner_table.schema();
        let inner_columns = inner_schema.column_names_arc();
        let inner_columns_lower = inner_schema.column_names_lower_arc();

        let inner_key_source_idx = {
            let search_col = info.inner_column.to_lowercase();
            inner_columns_lower
                .iter()
                .position(|c| c == &search_col)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "Anti-join inner key column '{}' not found in table columns: {:?}",
                        info.inner_column, inner_columns
                    ))
                })?
        };

        // Extract only the join key values (deduplicated) and convert to single-column rows.
        // The anti-join build side never needs the rest of the inner row, so keep
        // the scanner projection at the key column boundary instead of
        // materializing `collect_all_rows()`.
        let mut inner_key_scanner = inner_table.scan(
            &[inner_key_source_idx],
            storage_expr.as_ref().map(|e| e.as_ref()),
        )?;

        // Use a HashSet for deduplication to minimize the build side
        // Cap initial capacity to avoid over-allocation when many rows have few unique keys
        let estimated_unique = inner_key_scanner
            .estimated_count()
            .unwrap_or(10000)
            .min(10000);
        let mut seen: ValueSet = ValueSet::with_capacity(estimated_unique);
        let mut inner_rows: Vec<radixdb_core::Row> = Vec::with_capacity(estimated_unique);

        while inner_key_scanner.next() {
            if let Some(value) = inner_key_scanner.row().get(0) {
                if !value.is_null() {
                    // Clone once and reuse for both HashSet and Row to avoid double allocation
                    let cloned = value.clone();
                    if seen.insert(cloned.clone()) {
                        inner_rows.push(radixdb_core::Row::from_values(vec![cloned]));
                    }
                }
            }
        }
        if let Some(err) = inner_key_scanner.err() {
            return Err(err.clone());
        }
        inner_key_scanner.close()?;

        // Find the outer column index for join key
        // OPTIMIZATION: Pre-compute lowercase column names once to avoid per-column to_lowercase()
        let outer_columns_lower: Vec<String> =
            outer_columns.iter().map(|c| c.to_lowercase()).collect();

        let outer_key_idx = {
            let search_col = info.outer_column.to_lowercase();
            let search_suffix = format!(".{}", search_col); // Pre-compute once outside loop
            outer_columns_lower
                .iter()
                .position(|c| {
                    c == &search_col
                        || c.ends_with(&search_suffix)
                        || c.split('.').next_back() == Some(search_col.as_str())
                })
                .ok_or_else(|| {
                    Error::internal(format!(
                        "Anti-join outer key column '{}' not found in columns: {:?}",
                        info.outer_column, outer_columns
                    ))
                })?
        };

        // Inner column is always index 0 (we extracted only the join key)
        let inner_key_idx = 0;

        // Create schemas for operators
        let outer_schema: Vec<ColumnInfo> = outer_columns.iter().map(ColumnInfo::new).collect();
        let inner_schema = vec![ColumnInfo::new(&info.inner_column)];

        // Create MaterializedOperators
        let outer_op = Box::new(MaterializedOperator::from_arc(
            outer_rows,
            outer_schema.clone(),
        ));
        let inner_op = Box::new(MaterializedOperator::new(inner_rows, inner_schema));

        // Create anti-join operator
        // Anti-join: return outer rows that have NO match in inner
        let mut join_op = HashJoinOperator::new(
            outer_op,
            inner_op,
            JoinType::Anti,
            vec![outer_key_idx],
            vec![inner_key_idx],
            JoinSide::Right, // Build on smaller (inner) side
        );

        // Execute the join with synthetic row IDs
        if let Err(error) = join_op.open() {
            let _ = join_op.close();
            return Err(error);
        }
        let execution_result = (|| {
            let mut result_rows = radixdb_core::RowVec::new();
            let mut row_id = 0i64;
            while let Some(row_ref) = join_op.next()? {
                result_rows.push((row_id, row_ref.into_owned()));
                row_id += 1;
            }
            Ok(result_rows)
        })();
        let close_result = join_op.close();
        let result_rows = match (execution_result, close_result) {
            (Ok(rows), Ok(())) => rows,
            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
        };

        Ok(result_rows)
    }

    /// Try to extract SemiJoinInfo from a NOT EXISTS expression.
    /// Returns None if the expression is not a valid NOT EXISTS pattern.
    pub fn try_extract_not_exists_info(
        expr: &Expression,
        outer_tables: &[String],
    ) -> Option<SemiJoinInfo> {
        if let Expression::Prefix(prefix) = expr {
            if prefix.operator.eq_ignore_ascii_case("NOT") {
                if let Expression::Exists(exists) = prefix.right.as_ref() {
                    return Self::try_extract_semi_join_info(exists, true, outer_tables);
                }
            }
        }
        None
    }

    /// Transform a WHERE clause with EXISTS into one using a pre-computed hash set.
    ///
    /// Replaces: EXISTS (SELECT ...) with: outer_col IN (hash_set_values)
    pub fn transform_exists_to_in_list(
        info: &SemiJoinInfo,
        hash_set: CompactArc<ValueSet>,
    ) -> Expression {
        // For empty hash set, return FALSE (no matches exist)
        // For NOT EXISTS with empty set, return TRUE (nothing exists to negate)
        if hash_set.is_empty() {
            return Expression::BooleanLiteral(BooleanLiteral {
                token: dummy_token_clone(),
                value: info.is_negated,
            });
        }

        // Build the outer column expression using dummy_token_clone() to avoid allocations
        let outer_col_expr = if let Some(ref tbl) = info.outer_table {
            Expression::QualifiedIdentifier(QualifiedIdentifier {
                token: dummy_token_clone(),
                qualifier: Box::new(Identifier::new(dummy_token_clone(), tbl.clone())),
                intermediate: None,
                name: Box::new(Identifier::new(
                    dummy_token_clone(),
                    info.outer_column.clone(),
                )),
            })
        } else {
            Expression::Identifier(Identifier::new(
                dummy_token_clone(),
                info.outer_column.clone(),
            ))
        };

        // Use InHashSet with Arc for O(1) lookup and cheap cloning in parallel execution
        Expression::InHashSet(InHashSetExpression {
            token: dummy_token_clone(),
            column: Box::new(outer_col_expr),
            values: hash_set, // Already Arc, no wrapping needed
            not: info.is_negated,
        })
    }

    /// Try to optimize correlated EXISTS subqueries to semi-join.
    /// Returns Some(optimized_expression) if successful, None if not applicable.
    ///
    /// Note: This function now checks if index-nested-loop would be more efficient
    /// and skips the semi-join transformation in that case, allowing per-row index probing.
    ///
    /// The `outer_limit` parameter helps decide between strategies:
    /// - With small LIMIT + index: prefer index-nested-loop (per-row probing with early termination)
    /// - Without LIMIT: prefer semi-join (scan inner once, hash lookup per outer row)
    pub fn try_optimize_exists_to_semi_join(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
        outer_limit: Option<i64>,
    ) -> Result<Option<Expression>> {
        match expr {
            Expression::Exists(exists) => {
                if let Some(info) = Self::try_extract_semi_join_info(exists, false, outer_tables) {
                    // Check if index-nested-loop would be more efficient
                    // (index exists + no additional predicates, OR index exists + small LIMIT)
                    if self.should_use_index_nested_loop(&info, outer_limit) {
                        return Ok(None); // Skip semi-join, use index probing per row
                    }
                    // Semi-join optimization: execute inner query once, collect into hash set
                    // This enables InHashSet optimization to probe outer table's PK directly
                    let hash_set = self.execute_semi_join_optimization(&info, ctx)?;
                    return Ok(Some(Self::transform_exists_to_in_list(&info, hash_set)));
                }
                Ok(None)
            }

            Expression::Prefix(prefix) if prefix.operator.eq_ignore_ascii_case("NOT") => {
                if let Expression::Exists(exists) = prefix.right.as_ref() {
                    if let Some(info) = Self::try_extract_semi_join_info(exists, true, outer_tables)
                    {
                        // Check if index-nested-loop would be more efficient
                        if self.should_use_index_nested_loop(&info, outer_limit) {
                            return Ok(None); // Skip semi-join, use index probing per row
                        }
                        let hash_set = self.execute_semi_join_optimization(&info, ctx)?;
                        return Ok(Some(Self::transform_exists_to_in_list(&info, hash_set)));
                    }
                }
                Ok(None)
            }

            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("AND") => {
                // Try to optimize EXISTS in either branch of AND
                let left_opt = self.try_optimize_exists_to_semi_join(
                    &infix.left,
                    ctx,
                    outer_tables,
                    outer_limit,
                )?;
                let right_opt = self.try_optimize_exists_to_semi_join(
                    &infix.right,
                    ctx,
                    outer_tables,
                    outer_limit,
                )?;

                match (left_opt, right_opt) {
                    (Some(new_left), Some(new_right)) => {
                        Ok(Some(Expression::Infix(InfixExpression {
                            token: dummy_token_clone(),
                            left: Box::new(new_left),
                            operator: "AND".into(),
                            op_type: InfixOperator::And,
                            right: Box::new(new_right),
                        })))
                    }
                    (Some(new_left), None) => Ok(Some(Expression::Infix(InfixExpression {
                        token: dummy_token_clone(),
                        left: Box::new(new_left),
                        operator: "AND".into(),
                        op_type: InfixOperator::And,
                        right: infix.right.clone(),
                    }))),
                    (None, Some(new_right)) => Ok(Some(Expression::Infix(InfixExpression {
                        token: dummy_token_clone(),
                        left: infix.left.clone(),
                        operator: "AND".into(),
                        op_type: InfixOperator::And,
                        right: Box::new(new_right),
                    }))),
                    (None, None) => Ok(None),
                }
            }

            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("OR") => {
                // For OR, both branches must be optimizable for benefit
                // But we can still optimize individual EXISTS clauses
                let left_opt = self.try_optimize_exists_to_semi_join(
                    &infix.left,
                    ctx,
                    outer_tables,
                    outer_limit,
                )?;
                let right_opt = self.try_optimize_exists_to_semi_join(
                    &infix.right,
                    ctx,
                    outer_tables,
                    outer_limit,
                )?;

                match (left_opt, right_opt) {
                    (Some(new_left), Some(new_right)) => {
                        Ok(Some(Expression::Infix(InfixExpression {
                            token: dummy_token_clone(),
                            left: Box::new(new_left),
                            operator: "OR".into(),
                            op_type: InfixOperator::Or,
                            right: Box::new(new_right),
                        })))
                    }
                    (Some(new_left), None) => Ok(Some(Expression::Infix(InfixExpression {
                        token: dummy_token_clone(),
                        left: Box::new(new_left),
                        operator: "OR".into(),
                        op_type: InfixOperator::Or,
                        right: infix.right.clone(),
                    }))),
                    (None, Some(new_right)) => Ok(Some(Expression::Infix(InfixExpression {
                        token: dummy_token_clone(),
                        left: infix.left.clone(),
                        operator: "OR".into(),
                        op_type: InfixOperator::Or,
                        right: Box::new(new_right),
                    }))),
                    (None, None) => Ok(None),
                }
            }

            _ => Ok(None),
        }
    }

    // ============================================================================
    // IN Subquery Semi-Join Optimization
    // ============================================================================

    /// Try to optimize IN subqueries to semi-join (execute once, hash lookup per row).
    ///
    /// This transforms:
    /// ```sql
    /// WHERE outer.col IN (SELECT inner_col FROM t WHERE non_correlated_pred)
    /// ```
    /// Into:
    /// ```sql
    /// WHERE outer.col IN (hash_set_of_inner_col_values)
    /// ```
    ///
    /// # Optimization Criteria
    ///
    /// 1. IN right side must be a scalar subquery
    /// 2. Subquery must SELECT exactly one column
    /// 3. Subquery must have a simple table source (no joins)
    /// 4. Subquery WHERE clause must NOT reference outer tables (non-correlated)
    ///
    /// # Performance Impact
    ///
    /// - **Before**: O(N×M) - executes subquery for each outer row
    /// - **After**: O(N+M) - executes subquery once, O(1) hash lookup per row
    pub fn try_optimize_in_to_semi_join(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
    ) -> Result<Option<Expression>> {
        match expr {
            Expression::In(in_expr) => {
                // Check if right side is a scalar subquery
                if let Expression::ScalarSubquery(subquery) = in_expr.right.as_ref() {
                    if let Some(info) = Self::try_extract_in_semi_join_info(
                        in_expr,
                        &subquery.subquery,
                        outer_tables,
                    ) {
                        // Execute subquery once and build hash set
                        let hash_set = self.execute_semi_join_optimization(&info, ctx)?;
                        return Ok(Some(Self::transform_exists_to_in_list(&info, hash_set)));
                    }
                }
                Ok(None)
            }

            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("AND") => {
                // Try to optimize IN in either branch of AND
                let left_opt = self.try_optimize_in_to_semi_join(&infix.left, ctx, outer_tables)?;
                let right_opt =
                    self.try_optimize_in_to_semi_join(&infix.right, ctx, outer_tables)?;

                match (left_opt, right_opt) {
                    (Some(new_left), Some(new_right)) => {
                        Ok(Some(Expression::Infix(InfixExpression {
                            token: infix.token.clone(),
                            left: Box::new(new_left),
                            operator: infix.operator.clone(),
                            op_type: infix.op_type,
                            right: Box::new(new_right),
                        })))
                    }
                    (Some(new_left), None) => Ok(Some(Expression::Infix(InfixExpression {
                        token: infix.token.clone(),
                        left: Box::new(new_left),
                        operator: infix.operator.clone(),
                        op_type: infix.op_type,
                        right: infix.right.clone(),
                    }))),
                    (None, Some(new_right)) => Ok(Some(Expression::Infix(InfixExpression {
                        token: infix.token.clone(),
                        left: infix.left.clone(),
                        operator: infix.operator.clone(),
                        op_type: infix.op_type,
                        right: Box::new(new_right),
                    }))),
                    (None, None) => Ok(None),
                }
            }

            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("OR") => {
                // Try to optimize IN in either branch of OR
                let left_opt = self.try_optimize_in_to_semi_join(&infix.left, ctx, outer_tables)?;
                let right_opt =
                    self.try_optimize_in_to_semi_join(&infix.right, ctx, outer_tables)?;

                match (left_opt, right_opt) {
                    (Some(new_left), Some(new_right)) => {
                        Ok(Some(Expression::Infix(InfixExpression {
                            token: infix.token.clone(),
                            left: Box::new(new_left),
                            operator: infix.operator.clone(),
                            op_type: infix.op_type,
                            right: Box::new(new_right),
                        })))
                    }
                    (Some(new_left), None) => Ok(Some(Expression::Infix(InfixExpression {
                        token: infix.token.clone(),
                        left: Box::new(new_left),
                        operator: infix.operator.clone(),
                        op_type: infix.op_type,
                        right: infix.right.clone(),
                    }))),
                    (None, Some(new_right)) => Ok(Some(Expression::Infix(InfixExpression {
                        token: infix.token.clone(),
                        left: infix.left.clone(),
                        operator: infix.operator.clone(),
                        op_type: infix.op_type,
                        right: Box::new(new_right),
                    }))),
                    (None, None) => Ok(None),
                }
            }

            _ => Ok(None),
        }
    }

    /// Extract semi-join info from an IN expression with subquery.
    ///
    /// Pattern: `outer.col IN (SELECT inner_col FROM t WHERE pred)`
    ///
    /// Returns None if:
    /// - Subquery has more than one SELECT column
    /// - Subquery has joins or derived tables
    /// - WHERE clause references outer tables (correlated)
    pub(super) fn try_extract_in_semi_join_info(
        in_expr: &InExpression,
        subquery: &SelectStatement,
        outer_tables: &[String],
    ) -> Option<SemiJoinInfo> {
        // The EXISTS-oriented semi-join collector intentionally discards NULL.
        // That is not equivalent for NOT IN: a single NULL makes every
        // non-matching comparison UNKNOWN. Keep NOT IN on the exhaustive
        // subquery rewriter, which records NULL in the compiled set.
        if in_expr.not {
            return None;
        }
        if subquery.with.is_some()
            || subquery.distinct
            || !subquery.distinct_on.is_empty()
            || !subquery.group_by.columns.is_empty()
            || subquery.group_by.modifier != GroupByModifier::None
            || subquery.having.is_some()
            || !subquery.window_defs.is_empty()
            || !subquery.order_by.is_empty()
            || subquery.limit.is_some()
            || subquery.offset.is_some()
            || !subquery.set_operations.is_empty()
        {
            return None;
        }

        // 1. Extract outer column from left side of IN
        let (outer_column, outer_table): (String, Option<String>) = match in_expr.left.as_ref() {
            Expression::QualifiedIdentifier(qid) => (
                qid.name.value.to_string(),
                Some(qid.qualifier.value.to_string()),
            ),
            Expression::Identifier(id) => (id.value.to_string(), None),
            _ => return None, // Complex expression on left side, can't optimize
        };

        // 2. Subquery must SELECT exactly one column (not *)
        if subquery.columns.len() != 1 {
            return None;
        }

        // Extract inner column name from SELECT
        let inner_column: String = match &subquery.columns[0] {
            Expression::Identifier(id) => id.value.to_string(),
            Expression::QualifiedIdentifier(qid) => qid.name.value.to_string(),
            Expression::Aliased(a) => match a.expression.as_ref() {
                Expression::Identifier(id) => id.value.to_string(),
                Expression::QualifiedIdentifier(qid) => qid.name.value.to_string(),
                _ => return None,
            },
            _ => return None, // Can't handle expressions in SELECT
        };

        // 3. Check for simple table source (not a join)
        let (inner_table, inner_alias): (String, Option<String>) =
            match subquery.table_expr.as_ref().map(|b| b.as_ref()) {
                Some(Expression::TableSource(ts)) => {
                    if ts.as_of.is_some() {
                        return None;
                    }
                    let alias = ts.alias.as_ref().map(|a| a.value.to_string());
                    (ts.name.value.to_string(), alias)
                }
                _ => return None, // Can't optimize subquery joins or derived tables
            };

        // 4. Get inner table identifiers
        let inner_table_lower: String = inner_alias
            .clone()
            .unwrap_or_else(|| inner_table.to_lowercase());
        let inner_tables = vec![inner_table_lower.to_lowercase()];

        // 5. Check if WHERE clause references outer tables
        if let Some(ref where_clause) = subquery.where_clause {
            if Self::expression_references_outer_tables(where_clause, outer_tables, &inner_tables) {
                return None; // Correlated WHERE, can't optimize
            }
        }

        Some(SemiJoinInfo {
            outer_column,
            outer_table,
            inner_column,
            inner_table,
            inner_alias,
            non_correlated_where: subquery
                .where_clause
                .as_ref()
                .map(|b| Arc::new(b.as_ref().clone())),
            is_negated: in_expr.not,
        })
    }

    /// Get outer table names from a table expression (for semi-join optimization).
    pub fn collect_outer_table_names(table_expr: &Option<Box<Expression>>) -> Vec<String> {
        let mut tables = Vec::new();
        if let Some(ref expr) = table_expr {
            Self::collect_table_names_from_source(expr.as_ref(), &mut tables);
        }
        tables
    }
}
