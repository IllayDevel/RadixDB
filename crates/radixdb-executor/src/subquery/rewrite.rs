use super::*;

impl<'host, H: SubqueryHost + ?Sized> SubqueryExecutor<'host, H> {
    /// Process subqueries in WHERE clause, replacing EXISTS with boolean literals
    ///
    /// This function walks the expression tree and executes any EXISTS subqueries,
    /// replacing them with boolean literal values.
    pub(super) fn process_where_subqueries(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        match expr {
            Expression::Exists(exists) => {
                // Execute the EXISTS subquery
                let exists_result = self.execute_exists_subquery(&exists.subquery, ctx)?;
                Ok(Expression::BooleanLiteral(BooleanLiteral {
                    token: dummy_token(
                        if exists_result { "TRUE" } else { "FALSE" },
                        TokenType::Keyword,
                    ),
                    value: exists_result,
                }))
            }

            Expression::AllAny(all_any) => {
                // Execute the subquery to get all values
                let values = self.execute_in_subquery(&all_any.subquery, ctx)?;
                let mut processed = all_any.clone();
                *processed.left = self.process_where_subqueries(&all_any.left, ctx)?;

                // Convert ALL/ANY to an equivalent expression that the evaluator can handle
                self.convert_all_any_to_expression(&processed, values)
            }

            Expression::Prefix(prefix) => {
                // Handle NOT EXISTS
                if prefix.operator.eq_ignore_ascii_case("NOT") {
                    if let Expression::Exists(exists) = prefix.right.as_ref() {
                        // Execute the EXISTS subquery and negate the result
                        let exists_result = self.execute_exists_subquery(&exists.subquery, ctx)?;
                        return Ok(Expression::BooleanLiteral(BooleanLiteral {
                            token: dummy_token(
                                if !exists_result { "TRUE" } else { "FALSE" },
                                TokenType::Keyword,
                            ),
                            value: !exists_result,
                        }));
                    }
                }

                // Process the inner expression recursively
                let processed_right = self.process_where_subqueries(&prefix.right, ctx)?;
                Ok(Expression::Prefix(PrefixExpression {
                    token: prefix.token.clone(),
                    operator: prefix.operator.clone(),
                    op_type: prefix.op_type,
                    right: Box::new(processed_right),
                }))
            }

            Expression::Infix(infix) => {
                // Process both sides recursively
                let processed_left = self.process_where_subqueries(&infix.left, ctx)?;
                let processed_right = self.process_where_subqueries(&infix.right, ctx)?;

                Ok(Expression::Infix(InfixExpression {
                    token: infix.token.clone(),
                    left: Box::new(processed_left),
                    operator: infix.operator.clone(),
                    op_type: infix.op_type,
                    right: Box::new(processed_right),
                }))
            }

            Expression::In(in_expr) => {
                // Process the left expression
                let processed_left = self.process_where_subqueries(&in_expr.left, ctx)?;

                // Check if the right side is a scalar subquery
                if let Expression::ScalarSubquery(subquery) = in_expr.right.as_ref() {
                    // Check if left side is a tuple (multi-column IN)
                    let tuple_width = match &processed_left {
                        Expression::ExpressionList(list) => Some(list.expressions.len()),
                        _ => None,
                    };

                    if let Some(tuple_width) = tuple_width {
                        // Multi-column IN: (a, b) IN (SELECT x, y FROM t)
                        let rows =
                            self.execute_in_subquery_rows(&subquery.subquery, tuple_width, ctx)?;

                        // Pre-allocate with known capacity for better performance
                        let mut expressions = Vec::with_capacity(rows.len());
                        let paren_token = dummy_token("(", TokenType::Punctuator);

                        // Convert each row to an ExpressionList (tuple)
                        for row in rows {
                            let col_count = row.len();
                            let mut tuple_exprs = Vec::with_capacity(col_count);
                            for value in &row {
                                tuple_exprs.push(value_to_expression(value));
                            }
                            expressions.push(Expression::ExpressionList(Box::new(
                                ExpressionList {
                                    token: paren_token.clone(),
                                    expressions: tuple_exprs,
                                },
                            )));
                        }

                        return Ok(Expression::In(InExpression {
                            token: in_expr.token.clone(),
                            left: Box::new(processed_left),
                            right: Box::new(Expression::ExpressionList(Box::new(ExpressionList {
                                token: paren_token,
                                expressions,
                            }))),
                            not: in_expr.not,
                        }));
                    } else {
                        // Single-column IN - use InHashSet for O(1) lookups
                        let values = self.execute_in_subquery(&subquery.subquery, ctx)?;

                        // Collect into FxHashSet for O(1) membership testing (optimized for Value types with WyMix)
                        let hash_set: ValueSet = values.into_iter().collect();

                        // Use InHashSet with Arc for fast O(1) lookup per row
                        return Ok(Expression::InHashSet(InHashSetExpression {
                            token: in_expr.token.clone(),
                            column: Box::new(processed_left),
                            values: CompactArc::new(hash_set),
                            not: in_expr.not,
                        }));
                    }
                }

                let processed_right = self.process_where_subqueries(&in_expr.right, ctx)?;
                Ok(Expression::In(InExpression {
                    token: in_expr.token.clone(),
                    left: Box::new(processed_left),
                    right: Box::new(processed_right),
                    not: in_expr.not,
                }))
            }

            Expression::Between(between) => {
                let processed_expr = self.process_where_subqueries(&between.expr, ctx)?;
                let processed_lower = self.process_where_subqueries(&between.lower, ctx)?;
                let processed_upper = self.process_where_subqueries(&between.upper, ctx)?;

                Ok(Expression::Between(BetweenExpression {
                    token: between.token.clone(),
                    expr: Box::new(processed_expr),
                    not: between.not,
                    lower: Box::new(processed_lower),
                    upper: Box::new(processed_upper),
                }))
            }

            Expression::Like(like) => Ok(Expression::Like(LikeExpression {
                token: like.token.clone(),
                left: Box::new(self.process_where_subqueries(&like.left, ctx)?),
                operator: like.operator.clone(),
                pattern: Box::new(self.process_where_subqueries(&like.pattern, ctx)?),
                escape: like
                    .escape
                    .as_ref()
                    .map(|escape| self.process_where_subqueries(escape, ctx).map(Box::new))
                    .transpose()?,
            })),

            Expression::List(list) => Ok(Expression::List(Box::new(ListExpression {
                token: list.token.clone(),
                elements: list
                    .elements
                    .iter()
                    .map(|item| self.process_where_subqueries(item, ctx))
                    .collect::<Result<Vec<_>>>()?,
            }))),

            Expression::ExpressionList(list) => {
                Ok(Expression::ExpressionList(Box::new(ExpressionList {
                    token: list.token.clone(),
                    expressions: list
                        .expressions
                        .iter()
                        .map(|item| self.process_where_subqueries(item, ctx))
                        .collect::<Result<Vec<_>>>()?,
                })))
            }

            Expression::Distinct(distinct) => Ok(Expression::Distinct(DistinctExpression {
                token: distinct.token.clone(),
                expr: Box::new(self.process_where_subqueries(&distinct.expr, ctx)?),
            })),

            Expression::InHashSet(in_hash) => Ok(Expression::InHashSet(InHashSetExpression {
                token: in_hash.token.clone(),
                column: Box::new(self.process_where_subqueries(&in_hash.column, ctx)?),
                values: in_hash.values.clone(),
                not: in_hash.not,
            })),

            Expression::ScalarSubquery(subquery) => {
                // Execute scalar subquery and replace with literal value
                let value = self.execute_scalar_subquery(&subquery.subquery, ctx)?;
                Ok(value_to_expression(&value))
            }

            Expression::Case(case) => {
                // Process the operand (if present)
                let processed_value = if let Some(ref value) = case.value {
                    Some(Box::new(self.process_where_subqueries(value, ctx)?))
                } else {
                    None
                };

                // Process each WHEN clause
                let processed_whens: Result<Vec<WhenClause>> = case
                    .when_clauses
                    .iter()
                    .map(|when| {
                        Ok(WhenClause {
                            token: when.token.clone(),
                            condition: self.process_where_subqueries(&when.condition, ctx)?,
                            then_result: self.process_where_subqueries(&when.then_result, ctx)?,
                        })
                    })
                    .collect();

                // Process the ELSE clause (if present)
                let processed_else = if let Some(ref else_val) = case.else_value {
                    Some(Box::new(self.process_where_subqueries(else_val, ctx)?))
                } else {
                    None
                };

                Ok(Expression::Case(Box::new(CaseExpression {
                    token: case.token.clone(),
                    value: processed_value,
                    when_clauses: processed_whens?,
                    else_value: processed_else,
                })))
            }

            Expression::Cast(cast) => {
                let processed_expr = self.process_where_subqueries(&cast.expr, ctx)?;
                Ok(Expression::Cast(CastExpression {
                    token: cast.token.clone(),
                    expr: Box::new(processed_expr),
                    type_name: cast.type_name.clone(),
                }))
            }

            Expression::FunctionCall(func) => {
                // Process function arguments to handle any nested subqueries
                let processed_args: Result<Vec<Expression>> = func
                    .arguments
                    .iter()
                    .map(|arg| self.process_where_subqueries(arg, ctx))
                    .collect();

                let order_by = func
                    .order_by
                    .iter()
                    .map(|order| {
                        Ok(OrderByExpression {
                            expression: self.process_where_subqueries(&order.expression, ctx)?,
                            ascending: order.ascending,
                            nulls_first: order.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let filter = func
                    .filter
                    .as_ref()
                    .map(|filter| self.process_where_subqueries(filter, ctx).map(Box::new))
                    .transpose()?;

                Ok(Expression::FunctionCall(Box::new(FunctionCall {
                    token: func.token.clone(),
                    function: func.function.clone(),
                    arguments: processed_args?,
                    is_distinct: func.is_distinct,
                    order_by,
                    filter,
                })))
            }

            Expression::Aliased(aliased) => Ok(Expression::Aliased(AliasedExpression {
                token: aliased.token.clone(),
                expression: Box::new(self.process_where_subqueries(&aliased.expression, ctx)?),
                alias: aliased.alias.clone(),
            })),

            Expression::Window(window) => {
                let processed_function = self.process_where_subqueries(
                    &Expression::FunctionCall(window.function.clone()),
                    ctx,
                )?;
                let Expression::FunctionCall(function) = processed_function else {
                    return Err(Error::internal(
                        "window function rewrite did not return a function",
                    ));
                };
                let partition_by = window
                    .partition_by
                    .iter()
                    .map(|item| self.process_where_subqueries(item, ctx))
                    .collect::<Result<Vec<_>>>()?;
                let order_by = window
                    .order_by
                    .iter()
                    .map(|order| {
                        Ok(OrderByExpression {
                            expression: self.process_where_subqueries(&order.expression, ctx)?,
                            ascending: order.ascending,
                            nulls_first: order.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let frame = window
                    .frame
                    .as_ref()
                    .map(|frame| self.process_window_frame_subqueries(frame, ctx))
                    .transpose()?;
                Ok(Expression::Window(Box::new(WindowExpression {
                    token: window.token.clone(),
                    function,
                    window_ref: window.window_ref.clone(),
                    partition_by,
                    order_by,
                    frame,
                })))
            }

            // For all other expression types, return as-is
            _ => Ok(expr.clone()),
        }
    }

    pub(super) fn process_window_frame_subqueries(
        &self,
        frame: &WindowFrame,
        ctx: &ExecutionContext,
    ) -> Result<WindowFrame> {
        let mut process_bound = |bound: &WindowFrameBound| -> Result<WindowFrameBound> {
            Ok(match bound {
                WindowFrameBound::Preceding(expression) => WindowFrameBound::Preceding(Box::new(
                    self.process_where_subqueries(expression, ctx)?,
                )),
                WindowFrameBound::Following(expression) => WindowFrameBound::Following(Box::new(
                    self.process_where_subqueries(expression, ctx)?,
                )),
                other => other.clone(),
            })
        };
        Ok(WindowFrame {
            unit: frame.unit,
            start: process_bound(&frame.start)?,
            end: frame.end.as_ref().map(&mut process_bound).transpose()?,
        })
    }

    /// Execute an EXISTS subquery and return true if any rows exist
    pub(super) fn execute_exists_subquery(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<bool> {
        // Try index-nested-loop optimization for correlated EXISTS
        if let Some(exists) = self.try_execute_exists_with_index_probe(subquery, ctx)? {
            return Ok(exists);
        }

        // Fall back to full subquery execution
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self.host.subquery_execute_select(subquery, &subquery_ctx)?;

        // Check if there's at least one row
        if result.next() {
            return Ok(true);
        }
        // Check for runtime filter errors before treating as "no rows"
        if let Some(err) = result.last_error() {
            return Err(err);
        }
        Ok(false)
    }

    /// Try to execute EXISTS using index-nested-loop optimization.
    ///
    /// This optimization is used when:
    /// 1. The subquery has a simple correlation: inner.col = outer.col
    /// 2. The inner table has an index on the correlation column
    /// 3. There's an outer row value available in the context
    ///
    /// Instead of running a full query, we probe the index directly for O(log n) or O(1) lookup.
    pub(super) fn try_execute_exists_with_index_probe(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Option<bool>> {
        // Need outer row context for correlated subquery
        let outer_row = match ctx.outer_row() {
            Some(row) => row,
            None => return Ok(None), // Not a correlated context
        };

        // OPTIMIZATION: Cache correlation info to avoid per-row extraction
        // The subquery pointer is stable within a query execution, so we use it as cache key.
        // Using pointer address directly as usize avoids format! allocation entirely.
        let subquery_ptr = subquery as *const SelectStatement as usize;

        let correlation = match get_cached_exists_correlation(subquery_ptr) {
            Some(Some(info)) => info,
            Some(None) => return Ok(None), // Previously determined not extractable
            None => {
                // First probe for this subquery - extract and cache
                let info = Self::extract_index_nested_loop_info(subquery);
                let cached_info = info.map(|i| {
                    // Pre-compute index cache key once to avoid per-probe format! allocation
                    let index_cache_key = format!("{}:{}", i.inner_table, i.inner_column);
                    ExistsCorrelationInfo {
                        outer_column: i.outer_column.clone(),
                        outer_table: i.outer_table.clone(),
                        inner_column: i.inner_column.clone(),
                        inner_table: i.inner_table.clone(),
                        outer_column_lower: i.outer_column.to_lowercase(),
                        outer_qualified_lower: i.outer_table.as_ref().map(|tbl| {
                            format!("{}.{}", tbl.to_lowercase(), i.outer_column.to_lowercase())
                        }),
                        additional_predicate: i.additional_predicate.clone(),
                        index_cache_key,
                    }
                });
                // cache_exists_correlation returns the Arc-wrapped version
                match cache_exists_correlation(subquery_ptr, cached_info) {
                    Some(arc) => arc,
                    None => return Ok(None),
                }
            }
        };

        // Get the outer value from the outer row hashmap using pre-computed lowercase keys
        // This avoids per-row to_lowercase() calls
        // Use .as_str() for lookups since map now uses CompactArc<str> keys
        let outer_value = if let Some(ref qualified) = correlation.outer_qualified_lower {
            outer_row
                .get(qualified.as_str())
                .or_else(|| outer_row.get(correlation.outer_column_lower.as_str()))
        } else {
            outer_row.get(correlation.outer_column_lower.as_str())
        };

        let outer_value = match outer_value {
            Some(v) if !v.is_null() => v.clone(),
            Some(_) => return Ok(Some(false)), // NULL never matches in EXISTS
            None => {
                return Ok(None); // Column not found, fall back
            }
        };
        // OPTIMIZATION: Cache index reference to avoid repeated lookups
        // This reduces the ~2-5μs overhead per EXISTS probe to nearly zero for subsequent probes
        // Uses pre-computed index_cache_key from ExistsCorrelationInfo to avoid per-probe format!
        let index = match get_cached_exists_index(&correlation.index_cache_key) {
            Some(idx) => idx,
            None => {
                // First time: get index from engine and cache it
                let indexes = match self
                    .host
                    .subquery_engine()
                    .get_all_indexes(&correlation.inner_table)
                {
                    Ok(idxs) => idxs,
                    Err(_) => return Ok(None), // Table not found, fall back
                };

                // Find the index on the correlation column
                let idx = indexes
                    .into_iter()
                    .find(|idx| idx.column_names().contains(&correlation.inner_column));

                match idx {
                    Some(idx) => {
                        cache_exists_index(correlation.index_cache_key.clone(), idx.clone());
                        idx
                    }
                    None => return Ok(None), // No index, fall back to full query
                }
            }
        };

        // Probe the index for matching row IDs
        let row_ids = index.get_row_ids_equal(std::slice::from_ref(&outer_value))?;

        if row_ids.is_empty() {
            return Ok(Some(false)); // No matches from index
        }

        // If there's no additional predicate, check if at least one row is visible
        // Note: Index may contain row_ids for deleted rows, so we must verify visibility
        if correlation.additional_predicate.is_none() {
            let row_fetcher = match self.get_or_create_row_fetcher(&correlation.inner_table) {
                Some(f) => f,
                None => return Ok(None), // Fall back if fetcher creation fails
            };
            // Check batches until we find a visible row or exhaust all row_ids
            // We can't stop after first batch because deleted rows may precede visible ones
            for chunk in row_ids.chunks(VISIBILITY_CHECK_BATCH_SIZE) {
                let visible = row_fetcher(chunk)?;
                if !visible.is_empty() {
                    return Ok(Some(true)); // Found at least one visible row
                }
            }
            return Ok(Some(false)); // No visible rows in any batch
        }

        // With additional predicate, we need to check each matching row
        // OPTIMIZATION: Directly fetch rows by row_ids and evaluate the predicate
        // This avoids the overhead of building and executing a full SELECT query
        let additional_pred = correlation.additional_predicate.as_ref().unwrap();

        // SAFETY: If the additional predicate has references to tables other than
        // the inner table (outer column references) or contains subqueries (EXISTS, etc.),
        // the RowFilter cannot evaluate them correctly because:
        // 1. strip_table_alias_from_expr strips ALL qualifiers, merging outer refs with inner cols
        // 2. RowFilter has no subquery_executor, so EXISTS always returns false
        // Fall back to full query execution which handles these correctly.
        // Check against both the inner table name and alias (predicates use the alias).
        let inner_alias_name = match subquery.table_expr.as_ref().map(|b| b.as_ref()) {
            Some(Expression::TableSource(ts)) => ts
                .alias
                .as_ref()
                .map(|a| a.value.to_string())
                .unwrap_or_else(|| correlation.inner_table.clone()),
            _ => correlation.inner_table.clone(),
        };
        if Self::predicate_has_outer_refs_or_subqueries(
            additional_pred,
            &correlation.inner_table,
            &inner_alias_name,
        ) {
            return Ok(None);
        }

        // OPTIMIZATION: Cache schema column names to avoid repeated get_table_schema() calls
        // This reduces the ~1μs overhead per EXISTS probe
        let columns = match get_cached_exists_schema(&correlation.inner_table) {
            Some(cols) => cols,
            None => {
                let schema = match self
                    .host
                    .subquery_engine()
                    .get_table_schema(&correlation.inner_table)
                {
                    Ok(s) => s,
                    Err(_) => return Ok(None), // Fall back if schema not found
                };
                // Use schema's cached column names - O(1) Arc clone
                let cols = schema.column_names_arc();
                cache_exists_schema(correlation.inner_table.clone(), CompactArc::clone(&cols));
                cols
            }
        };

        // Try to get cached predicate filter using the cached predicate cache key
        // (reuse subquery_ptr from correlation cache above)
        let predicate_filter = match get_cached_exists_pred_key(subquery_ptr) {
            Some(cache_key) => {
                // Fast path: we have a cached predicate cache key
                match get_cached_exists_predicate(&cache_key) {
                    Some(filter) => filter,
                    None => {
                        // Cache key exists but filter was evicted - this shouldn't happen normally
                        // but handle it by recompiling
                        let stripped_pred = Self::strip_table_alias_from_expr(additional_pred);
                        match crate::expression::RowFilter::new(&stripped_pred, &columns) {
                            Ok(filter) => {
                                cache_exists_predicate(cache_key, filter.clone());
                                filter
                            }
                            Err(_) => return Ok(None),
                        }
                    }
                }
            }
            None => {
                // First probe for this subquery - compute and cache the predicate cache key
                let stripped_pred = Self::strip_table_alias_from_expr(additional_pred);
                // Use expression hash instead of Debug formatting to avoid expensive string allocation
                let pred_hash = compute_expression_hash(&stripped_pred);
                let cache_key = format!("{}:{}", correlation.inner_table, pred_hash);

                // Cache the predicate cache key for subsequent probes
                cache_exists_pred_key(subquery_ptr, cache_key.clone());

                match get_cached_exists_predicate(&cache_key) {
                    Some(filter) => filter,
                    None => match crate::expression::RowFilter::new(&stripped_pred, &columns) {
                        Ok(filter) => {
                            cache_exists_predicate(cache_key, filter.clone());
                            filter
                        }
                        Err(_) => return Ok(None),
                    },
                }
            }
        };

        // Apply execution context for parameter resolution ($1, named params, etc.)
        // The filter is cached without context, so we clone and apply per-probe.
        let predicate_filter = predicate_filter.with_context(ctx);

        // Get or create a cached row fetcher for this table
        let row_fetcher = match self.get_or_create_row_fetcher(&correlation.inner_table) {
            Some(f) => f,
            None => return Ok(None), // Fall back if fetcher creation fails
        };

        // Fetch rows by their IDs using the cached row fetcher
        const BATCH_SIZE: usize = 100;
        for batch in row_ids.chunks(BATCH_SIZE) {
            let fetched = row_fetcher(batch)?;

            // Check each row against the predicate
            for (_row_id, row) in fetched {
                if predicate_filter.matches_checked(&row)? {
                    return Ok(Some(true));
                }
            }
        }

        // No rows matched the predicate
        Ok(Some(false))
    }

    /// Extract index-nested-loop correlation info from a subquery.
    ///
    /// Looks for patterns like:
    /// SELECT 1 FROM orders WHERE orders.user_id = u.id [AND additional_predicates]
    pub(super) fn extract_index_nested_loop_info(
        subquery: &SelectStatement,
    ) -> Option<IndexNestedLoopInfo> {
        // Must have a simple table source
        let (inner_table, inner_alias) = match subquery.table_expr.as_ref().map(|b| b.as_ref()) {
            Some(Expression::TableSource(ts)) => {
                let alias = ts.alias.as_ref().map(|a| a.value.clone());
                (ts.name.value.clone(), alias)
            }
            _ => return None,
        };

        // Must have a WHERE clause
        let where_clause = subquery.where_clause.as_ref()?;

        // Extract correlation condition
        let inner_table_lower: String = inner_alias
            .clone()
            .unwrap_or_else(|| inner_table.to_lowercase())
            .to_lowercase()
            .into();
        let inner_tables = vec![inner_table_lower];

        Self::extract_correlation_for_index(where_clause, &inner_tables, &inner_table)
    }

    /// Extract correlation info suitable for index-nested-loop from a WHERE clause.
    pub(super) fn extract_correlation_for_index(
        expr: &Expression,
        inner_tables: &[String],
        inner_table_name: &str,
    ) -> Option<IndexNestedLoopInfo> {
        match expr {
            Expression::Infix(infix) if infix.operator == "=" => {
                // Try to match: inner.col = outer.col or outer.col = inner.col
                if let Some((inner_col, outer_col, outer_tbl)) =
                    Self::extract_correlation_pair(&infix.left, &infix.right, inner_tables)
                {
                    return Some(IndexNestedLoopInfo {
                        outer_column: outer_col,
                        outer_table: outer_tbl,
                        inner_column: inner_col,
                        inner_table: inner_table_name.to_string(),
                        additional_predicate: None,
                    });
                }
                if let Some((inner_col, outer_col, outer_tbl)) =
                    Self::extract_correlation_pair(&infix.right, &infix.left, inner_tables)
                {
                    return Some(IndexNestedLoopInfo {
                        outer_column: outer_col,
                        outer_table: outer_tbl,
                        inner_column: inner_col,
                        inner_table: inner_table_name.to_string(),
                        additional_predicate: None,
                    });
                }
                None
            }

            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("AND") => {
                // Try left side for correlation
                if let Some(mut info) =
                    Self::extract_correlation_for_index(&infix.left, inner_tables, inner_table_name)
                {
                    // Right side becomes additional predicate
                    info.additional_predicate = Some((*infix.right).clone());
                    return Some(info);
                }
                // Try right side for correlation
                if let Some(mut info) = Self::extract_correlation_for_index(
                    &infix.right,
                    inner_tables,
                    inner_table_name,
                ) {
                    // Left side becomes additional predicate
                    info.additional_predicate = Some((*infix.left).clone());
                    return Some(info);
                }
                None
            }

            _ => None,
        }
    }

    /// Try to execute a correlated scalar COUNT subquery using index probe.
    ///
    /// This optimization handles patterns like:
    /// `(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id)`
    ///
    /// Instead of running a full query for each outer row, we:
    /// 1. Extract the correlation info (user_id = u.id)
    /// 2. Probe the index on user_id with the outer value
    /// 3. Return the count of matching row IDs directly
    ///
    /// This is O(1) index lookup per row vs O(n) query execution.
    pub(super) fn try_execute_scalar_count_with_index(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Option<i64>> {
        // Need outer row context for correlated subquery
        let outer_row = match ctx.outer_row() {
            Some(row) => row,
            None => return Ok(None), // Not a correlated context
        };

        // Check if this is a COUNT(*) or COUNT(col) aggregate
        if subquery.columns.len() != 1 {
            return Ok(None);
        }

        let is_count = match &subquery.columns[0] {
            Expression::Aliased(a) => Self::is_count_expression(&a.expression),
            expr => Self::is_count_expression(expr),
        };

        if !is_count {
            return Ok(None);
        }

        // Must not have GROUP BY (scalar COUNT must return single value)
        if !subquery.group_by.columns.is_empty() {
            return Ok(None);
        }

        // OPTIMIZATION: Use the same ExistsCorrelationInfo caching as EXISTS optimization
        // This avoids per-probe format! allocations for qualified names and index cache keys
        let subquery_ptr = subquery as *const SelectStatement as usize;

        let correlation = match get_cached_exists_correlation(subquery_ptr) {
            Some(Some(info)) => info,
            Some(None) => return Ok(None), // Previously determined not extractable
            None => {
                // First probe for this subquery - extract and cache
                let info = Self::extract_index_nested_loop_info(subquery);
                let cached_info = info.map(|i| {
                    // Pre-compute index cache key once to avoid per-probe format! allocation
                    let index_cache_key = format!("{}:{}", i.inner_table, i.inner_column);
                    ExistsCorrelationInfo {
                        outer_column: i.outer_column.clone(),
                        outer_table: i.outer_table.clone(),
                        inner_column: i.inner_column.clone(),
                        inner_table: i.inner_table.clone(),
                        outer_column_lower: i.outer_column.to_lowercase(),
                        outer_qualified_lower: i.outer_table.as_ref().map(|tbl| {
                            format!("{}.{}", tbl.to_lowercase(), i.outer_column.to_lowercase())
                        }),
                        additional_predicate: i.additional_predicate.clone(),
                        index_cache_key,
                    }
                });
                // cache_exists_correlation returns the Arc-wrapped version
                match cache_exists_correlation(subquery_ptr, cached_info) {
                    Some(arc) => arc,
                    None => return Ok(None),
                }
            }
        };

        // Get the outer value from the outer row hashmap using pre-computed lowercase keys
        // This avoids per-probe to_lowercase() calls
        // Use .as_str() for lookups since map now uses CompactArc<str> keys
        let outer_value = if let Some(ref qualified) = correlation.outer_qualified_lower {
            outer_row
                .get(qualified.as_str())
                .or_else(|| outer_row.get(correlation.outer_column_lower.as_str()))
        } else {
            outer_row.get(correlation.outer_column_lower.as_str())
        };

        let outer_value = match outer_value {
            Some(v) if !v.is_null() => v.clone(),
            Some(_) => return Ok(Some(0)), // NULL never matches, count is 0
            None => return Ok(None),       // Column not found, fall back
        };

        // Use cached index lookup with pre-computed index_cache_key
        let index = match get_cached_exists_index(&correlation.index_cache_key) {
            Some(idx) => idx,
            None => {
                // First time: get index from engine and cache it
                let indexes = match self
                    .host
                    .subquery_engine()
                    .get_all_indexes(&correlation.inner_table)
                {
                    Ok(idxs) => idxs,
                    Err(_) => return Ok(None), // Table not found, fall back
                };

                // Find the index on the correlation column
                let idx = indexes
                    .into_iter()
                    .find(|idx| idx.column_names().contains(&correlation.inner_column));

                match idx {
                    Some(idx) => {
                        cache_exists_index(correlation.index_cache_key.clone(), idx.clone());
                        idx
                    }
                    None => return Ok(None), // No index, fall back to full query
                }
            }
        };

        // Probe the index for matching row IDs
        let row_ids = index.get_row_ids_equal(std::slice::from_ref(&outer_value))?;

        // If there's no additional predicate, count only visible rows
        // Note: Index may contain row_ids for deleted rows, so we must verify visibility
        if correlation.additional_predicate.is_none() {
            if row_ids.is_empty() {
                return Ok(Some(0));
            }
            // Use row_counter for COUNT (avoids cloning row data)
            let row_counter = match get_cached_count_counter(&correlation.inner_table) {
                Some(c) => c,
                None => match self.get_or_create_row_counter(&correlation.inner_table) {
                    Some(c) => c,
                    None => {
                        // Fall back to row_fetcher if counter not available
                        let row_fetcher = match get_cached_exists_fetcher(&correlation.inner_table)
                        {
                            Some(f) => f,
                            None => {
                                match self.get_or_create_row_fetcher(&correlation.inner_table) {
                                    Some(f) => f,
                                    None => return Ok(None),
                                }
                            }
                        };
                        let visible_rows = row_fetcher(&row_ids)?;
                        return Ok(Some(visible_rows.len() as i64));
                    }
                },
            };
            // Count visible rows without cloning
            let count = row_counter(&row_ids);
            return Ok(Some(count as i64));
        }

        // With additional predicate, we need to filter the matching rows
        // Fall back to full query execution for complex cases
        Ok(None)
    }

    /// Check if an expression is a COUNT aggregate function.
    pub(super) fn is_count_expression(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall(func) => func.function.eq_ignore_ascii_case("COUNT"),
            Expression::Aliased(a) => Self::is_count_expression(&a.expression),
            _ => false,
        }
    }

    /// Extract the aggregate function name from an expression.
    /// Returns Some(func_name) for COUNT, SUM, AVG, MIN, MAX.
    pub(super) fn extract_aggregate_function(expr: &Expression) -> Option<String> {
        match expr {
            Expression::FunctionCall(func) => {
                let name = func.function.to_uppercase();
                if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    Some(name.into())
                } else {
                    None
                }
            }
            Expression::Aliased(a) => Self::extract_aggregate_function(&a.expression),
            _ => None,
        }
    }

    /// Try to look up a batch aggregate result from cache.
    /// Returns Some(value) if found in cache, None otherwise.
    /// Uses cached lookup info to avoid per-row allocations.
    pub(super) fn try_lookup_batch_aggregate(
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Option<radixdb_core::Value> {
        // Need outer row context for correlated subquery
        let outer_row = ctx.outer_row()?;

        // Use pointer address as cache key (O(1) vs O(n) for to_string())
        let subquery_ptr = subquery as *const _ as usize;
        let lookup_info = match get_cached_batch_aggregate_info(subquery_ptr) {
            Some(cached) => cached?,
            None => {
                // First time: compute and cache the lookup info
                let info = Self::compute_batch_aggregate_info(subquery);
                cache_batch_aggregate_info(subquery_ptr, info)?
            }
        };

        // Get the outer value from the outer row hashmap (no allocation)
        // Use .as_str() for lookups since map now uses CompactArc<str> keys
        let outer_value = if let Some(ref qualified) = lookup_info.outer_qualified_lower {
            outer_row
                .get(qualified.as_str())
                .or_else(|| outer_row.get(lookup_info.outer_column_lower.as_str()))
        } else {
            outer_row.get(lookup_info.outer_column_lower.as_str())
        };

        let outer_value = match outer_value {
            Some(v) if !v.is_null() => v.clone(),
            // NULL never matches - for COUNT return 0, for other aggregates return NULL
            Some(_) => {
                return if lookup_info.is_count {
                    Some(Value::Integer(0))
                } else {
                    Some(Value::null_unknown())
                }
            }
            None => return None, // Column not found, can't optimize
        };

        // Look up in cache
        let cache = get_cached_batch_aggregate(&lookup_info.cache_key)?;
        let result = cache.get(&outer_value).cloned();

        // Return 0 for COUNT if key not found (no matching rows)
        if result.is_none() && lookup_info.is_count {
            return Some(Value::Integer(0));
        }

        result
    }

    /// Compute batch aggregate lookup info for a subquery.
    /// Returns None if the subquery is not batchable.
    pub(super) fn compute_batch_aggregate_info(
        subquery: &SelectStatement,
    ) -> Option<BatchAggregateLookupInfo> {
        // Build cache key - returns None if not a batchable aggregate
        let cache_key = Self::build_batch_aggregate_key(subquery)?;

        // Extract correlation info
        let correlation = Self::extract_index_nested_loop_info(subquery)?;

        // Pre-compute lowercase column names
        let outer_column_lower = correlation.outer_column.to_lowercase();
        let outer_qualified_lower = correlation
            .outer_table
            .as_ref()
            .map(|tbl| format!("{}.{}", tbl.to_lowercase(), outer_column_lower));

        let is_count = Self::is_count_expression(&subquery.columns[0]);

        Some(BatchAggregateLookupInfo {
            cache_key,
            outer_column_lower,
            outer_qualified_lower,
            is_count,
        })
    }

    /// Build a cache key for batch aggregate based on subquery structure.
    /// The key identifies the subquery pattern (table, correlation column, aggregate function).
    pub(super) fn build_batch_aggregate_key(subquery: &SelectStatement) -> Option<String> {
        // Extract table name
        let table_name = match subquery.table_expr.as_ref().map(|b| b.as_ref()) {
            Some(Expression::TableSource(ts)) => ts.name.value.clone(),
            _ => return None,
        };

        // Reject non-aggregate or ambiguous output shapes.
        if subquery.columns.len() != 1 {
            return None;
        }
        Self::extract_aggregate_function(&subquery.columns[0])?;

        Self::extract_index_nested_loop_info(subquery)?;

        // Cache identity must include arguments, DISTINCT/FILTER/ORDER and the
        // complete predicate, not just table/column/function name.
        Some(format!(
            "batch_agg:{}:{}",
            table_name.to_lowercase(),
            subquery
        ))
    }

    /// Try to execute and cache a batch aggregate query.
    /// This executes `SELECT correlation_col, AGG() FROM table GROUP BY correlation_col`
    /// and caches the results for O(1) lookup per outer row.
    pub(super) fn try_execute_and_cache_batch_aggregate(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Option<CompactArc<ValueMap<Value>>>> {
        // Must have single aggregate column
        if subquery.columns.len() != 1 {
            return Ok(None);
        }

        // Verify this is an aggregate function (COUNT, SUM, AVG, MIN, MAX)
        if Self::extract_aggregate_function(&subquery.columns[0]).is_none() {
            return Ok(None);
        }

        // Must not have GROUP BY (we'll add our own)
        if !subquery.group_by.columns.is_empty() {
            return Ok(None);
        }

        // Must not have HAVING, LIMIT, OFFSET (these would change results)
        if subquery.having.is_some() || subquery.limit.is_some() || subquery.offset.is_some() {
            return Ok(None);
        }

        // Extract correlation info
        let correlation = match Self::extract_index_nested_loop_info(subquery) {
            Some(c) => c,
            None => return Ok(None),
        };

        // Build cache key (includes additional predicate in key for uniqueness)
        let cache_key = match Self::build_batch_aggregate_key(subquery) {
            Some(k) => k,
            None => return Ok(None),
        };

        // Check if already cached
        if let Some(cached) = get_cached_batch_aggregate(&cache_key) {
            return Ok(Some(cached));
        }

        // Build the batch aggregate query:
        // SELECT correlation_column, AGG(...) FROM table GROUP BY correlation_column
        let inner_col_val: SmartString = correlation.inner_column.clone().into();
        let inner_col_expr = Expression::Identifier(Identifier {
            token: dummy_token(&correlation.inner_column, TokenType::Identifier),
            value: inner_col_val.clone(),
            value_lower: inner_col_val.to_lowercase(),
        });

        // Clone the aggregate expression from the original subquery
        let agg_expr = subquery.columns[0].clone();

        // Build list of inner table names for outer reference detection
        let inner_tables = vec![
            correlation.inner_table.to_lowercase(),
            correlation.inner_table.clone(), // Also check original case
        ];

        // Include additional predicate in WHERE clause ONLY if it doesn't reference outer columns.
        // If the predicate contains outer references (like "o.amount > c.id * 50"),
        // we can't use batch caching because those references can't be resolved.
        let where_clause = correlation.additional_predicate.as_ref().and_then(|pred| {
            if Self::expression_has_outer_reference(pred, &inner_tables) {
                None // Can't use batch caching with outer references in predicate
            } else {
                Some(Box::new(pred.clone()))
            }
        });

        // If additional predicate has outer references, we can't use batch caching
        if correlation.additional_predicate.is_some() && where_clause.is_none() {
            return Ok(None);
        }

        let batch_query = SelectStatement {
            token: dummy_token("SELECT", TokenType::Keyword),
            columns: vec![inner_col_expr.clone(), agg_expr],
            table_expr: subquery.table_expr.clone(),
            where_clause, // Include additional predicate for filtering
            group_by: GroupByClause {
                columns: vec![inner_col_expr],
                modifier: GroupByModifier::None,
            },
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            distinct: false,
            distinct_on: vec![],
            with: None,
            window_defs: vec![],
            set_operations: vec![],
        };

        // Execute the batch query
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self
            .host
            .subquery_execute_select(&batch_query, &subquery_ctx)?;

        // Build the result map - use take_row() to avoid cloning
        let mut result_map: ValueMap<Value> = ValueMap::default();
        while result.next() {
            let row = result.take_row();
            if row.len() >= 2 {
                // into_values() uses Arc::try_unwrap() to move without cloning when sole owner
                let mut values = row.into_values();
                if values.len() >= 2 {
                    let value = values.pop().unwrap();
                    let key = values.swap_remove(0);
                    if !key.is_null() {
                        result_map.insert(key, value);
                    }
                }
            }
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        // Cache the results
        cache_batch_aggregate(cache_key.clone(), result_map);

        // Return the cached Arc
        Ok(get_cached_batch_aggregate(&cache_key))
    }

    /// Get or create a cached row fetcher for the given table.
    ///
    /// This helper reduces code duplication for the pattern of:
    /// 1. Check cache for existing fetcher
    /// 2. If not found, create from engine and cache it
    /// 3. Return the fetcher or None if creation fails
    pub(super) fn get_or_create_row_fetcher(
        &self,
        table_name: &str,
    ) -> Option<std::sync::Arc<crate::context::RowFetcher>> {
        if let Some(f) = get_cached_exists_fetcher(table_name) {
            return Some(f);
        }

        let fetcher = match self.host.subquery_engine().get_row_fetcher(table_name) {
            Ok(f) => f,
            Err(_) => return None, // Fall back if fetcher creation fails
        };
        cache_exists_fetcher(table_name.to_string(), fetcher);
        get_cached_exists_fetcher(table_name)
    }

    /// Get or create a cached row counter for a table.
    ///
    /// This is similar to get_or_create_row_fetcher but for COUNT operations.
    /// It returns a function that counts visible rows without cloning row data.
    pub(super) fn get_or_create_row_counter(
        &self,
        table_name: &str,
    ) -> Option<std::sync::Arc<crate::context::RowCounter>> {
        if let Some(c) = get_cached_count_counter(table_name) {
            return Some(c);
        }

        let counter = match self.host.subquery_engine().get_row_counter(table_name) {
            Ok(c) => c,
            Err(_e) => {
                // Fall back to slower path if counter creation fails
                #[cfg(debug_assertions)]
                eprintln!(
                    "[WARN] get_row_counter failed for '{}': {:?}",
                    table_name, _e
                );
                return None;
            }
        };
        cache_count_counter(table_name.to_string(), counter);
        get_cached_count_counter(table_name)
    }

    /// Extract correlation pair from two expressions.
    /// Returns (inner_column, outer_column, outer_table) if one side is inner ref and other is outer ref.
    pub(super) fn extract_correlation_pair(
        left: &Expression,
        right: &Expression,
        inner_tables: &[String],
    ) -> Option<(String, String, Option<String>)> {
        // left should be inner column, right should be outer column
        let inner_col = Self::get_inner_column_name(left, inner_tables)?;
        let (outer_col, outer_tbl) = Self::get_outer_column_name(right, inner_tables)?;
        Some((inner_col, outer_col, outer_tbl))
    }

    /// Get column name if expression is an inner table column reference.
    pub(super) fn get_inner_column_name(
        expr: &Expression,
        inner_tables: &[String],
    ) -> Option<String> {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                let table = qid.qualifier.value_lower.as_str();
                if inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
                    Some(qid.name.value.to_string())
                } else {
                    None
                }
            }
            Expression::Identifier(id) => {
                // Unqualified identifier assumed to be inner if in context
                Some(id.value.to_string())
            }
            _ => None,
        }
    }

    /// Get column name if expression is an outer table column reference.
    /// Returns (column_name, table_alias).
    pub(super) fn get_outer_column_name(
        expr: &Expression,
        inner_tables: &[String],
    ) -> Option<(String, Option<String>)> {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                let table = qid.qualifier.value_lower.as_str();
                // If NOT in inner_tables, it's an outer reference
                if !inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
                    Some((
                        qid.name.value.to_string(),
                        Some(qid.qualifier.value.to_string()),
                    ))
                } else {
                    None
                }
            }
            // Unqualified identifiers could be outer, but we can't be sure without schema
            _ => None,
        }
    }

    /// Check if an expression contains any outer column references.
    /// Returns true if the expression references columns from tables not in inner_tables.
    pub(super) fn expression_has_outer_reference(
        expr: &Expression,
        inner_tables: &[String],
    ) -> bool {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                let table = qid.qualifier.value_lower.as_str();
                // If NOT in inner_tables, it's an outer reference
                !inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table))
            }
            Expression::Infix(infix) => {
                Self::expression_has_outer_reference(&infix.left, inner_tables)
                    || Self::expression_has_outer_reference(&infix.right, inner_tables)
            }
            Expression::Prefix(prefix) => {
                Self::expression_has_outer_reference(&prefix.right, inner_tables)
            }
            Expression::FunctionCall(func) => func
                .arguments
                .iter()
                .any(|arg| Self::expression_has_outer_reference(arg, inner_tables)),
            Expression::In(in_expr) => {
                Self::expression_has_outer_reference(&in_expr.left, inner_tables)
                    || Self::expression_has_outer_reference(&in_expr.right, inner_tables)
            }
            Expression::Between(between) => {
                Self::expression_has_outer_reference(&between.expr, inner_tables)
                    || Self::expression_has_outer_reference(&between.lower, inner_tables)
                    || Self::expression_has_outer_reference(&between.upper, inner_tables)
            }
            Expression::Case(case) => {
                case.value
                    .as_ref()
                    .map(|op| Self::expression_has_outer_reference(op, inner_tables))
                    .unwrap_or(false)
                    || case.when_clauses.iter().any(|wc| {
                        Self::expression_has_outer_reference(&wc.condition, inner_tables)
                            || Self::expression_has_outer_reference(&wc.then_result, inner_tables)
                    })
                    || case
                        .else_value
                        .as_ref()
                        .map(|el| Self::expression_has_outer_reference(el, inner_tables))
                        .unwrap_or(false)
            }
            // Cast and Aliased have inner expressions
            Expression::Cast(cast) => {
                Self::expression_has_outer_reference(&cast.expr, inner_tables)
            }
            Expression::Aliased(aliased) => {
                Self::expression_has_outer_reference(&aliased.expression, inner_tables)
            }
            // Like has left expression (pattern is usually a literal)
            Expression::Like(like) => {
                Self::expression_has_outer_reference(&like.left, inner_tables)
                    || Self::expression_has_outer_reference(&like.pattern, inner_tables)
            }
            // Expression lists can contain outer references
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .any(|e| Self::expression_has_outer_reference(e, inner_tables)),
            Expression::List(list) => list
                .elements
                .iter()
                .any(|e| Self::expression_has_outer_reference(e, inner_tables)),
            // InHashSet references a column (which could be qualified)
            Expression::InHashSet(_) => {
                // InHashSet.column is a String (column name), not an Expression
                // The column reference is already resolved, so no outer ref possible here
                false
            }
            // Window functions can have outer refs in partition/order expressions
            Expression::Window(window) => {
                window
                    .partition_by
                    .iter()
                    .any(|e| Self::expression_has_outer_reference(e, inner_tables))
                    || window.order_by.iter().any(|ob| {
                        Self::expression_has_outer_reference(&ob.expression, inner_tables)
                    })
            }
            // Literals and unqualified identifiers don't count as outer references
            Expression::Identifier(_)
            | Expression::IntegerLiteral(_)
            | Expression::FloatLiteral(_)
            | Expression::StringLiteral(_)
            | Expression::BooleanLiteral(_)
            | Expression::NullLiteral(_)
            | Expression::IntervalLiteral(_)
            | Expression::BoundValue(_)
            | Expression::Parameter(_)
            | Expression::Star(_)
            | Expression::QualifiedStar(_) => false,
            // Subqueries and other complex expressions - conservatively return true
            // (ScalarSubquery, Exists, AllAny, etc. could have correlated outer refs)
            _ => true,
        }
    }

    /// Check if an expression contains references to tables other than the inner table,
    /// or contains subqueries (EXISTS, ScalarSubquery, etc.).
    /// Used to bail out of index probe when the additional predicate can't be evaluated
    /// as a simple RowFilter against inner table rows.
    pub(super) fn predicate_has_outer_refs_or_subqueries(
        expr: &Expression,
        inner_table: &str,
        inner_alias: &str,
    ) -> bool {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                let table = qid.qualifier.value_lower.as_str();
                // If qualifier doesn't match inner table name or alias, it's an outer ref
                !table.eq_ignore_ascii_case(inner_table) && !table.eq_ignore_ascii_case(inner_alias)
            }
            Expression::Infix(infix) => {
                Self::predicate_has_outer_refs_or_subqueries(&infix.left, inner_table, inner_alias)
                    || Self::predicate_has_outer_refs_or_subqueries(
                        &infix.right,
                        inner_table,
                        inner_alias,
                    )
            }
            Expression::Prefix(prefix) => Self::predicate_has_outer_refs_or_subqueries(
                &prefix.right,
                inner_table,
                inner_alias,
            ),
            Expression::FunctionCall(func) => func.arguments.iter().any(|arg| {
                Self::predicate_has_outer_refs_or_subqueries(arg, inner_table, inner_alias)
            }),
            Expression::In(in_expr) => {
                Self::predicate_has_outer_refs_or_subqueries(
                    &in_expr.left,
                    inner_table,
                    inner_alias,
                ) || Self::predicate_has_outer_refs_or_subqueries(
                    &in_expr.right,
                    inner_table,
                    inner_alias,
                )
            }
            Expression::Between(between) => {
                Self::predicate_has_outer_refs_or_subqueries(
                    &between.expr,
                    inner_table,
                    inner_alias,
                ) || Self::predicate_has_outer_refs_or_subqueries(
                    &between.lower,
                    inner_table,
                    inner_alias,
                ) || Self::predicate_has_outer_refs_or_subqueries(
                    &between.upper,
                    inner_table,
                    inner_alias,
                )
            }
            Expression::Like(like) => {
                Self::predicate_has_outer_refs_or_subqueries(&like.left, inner_table, inner_alias)
                    || Self::predicate_has_outer_refs_or_subqueries(
                        &like.pattern,
                        inner_table,
                        inner_alias,
                    )
            }
            Expression::Cast(cast) => {
                Self::predicate_has_outer_refs_or_subqueries(&cast.expr, inner_table, inner_alias)
            }
            Expression::Case(case) => {
                if let Some(ref val) = case.value {
                    if Self::predicate_has_outer_refs_or_subqueries(val, inner_table, inner_alias) {
                        return true;
                    }
                }
                for when in &case.when_clauses {
                    if Self::predicate_has_outer_refs_or_subqueries(
                        &when.condition,
                        inner_table,
                        inner_alias,
                    ) || Self::predicate_has_outer_refs_or_subqueries(
                        &when.then_result,
                        inner_table,
                        inner_alias,
                    ) {
                        return true;
                    }
                }
                if let Some(ref else_val) = case.else_value {
                    if Self::predicate_has_outer_refs_or_subqueries(
                        else_val,
                        inner_table,
                        inner_alias,
                    ) {
                        return true;
                    }
                }
                false
            }
            // Subqueries cannot be evaluated by RowFilter
            Expression::Exists(_) | Expression::ScalarSubquery(_) | Expression::AllAny(_) => true,
            // Identifiers, literals, etc. are fine
            _ => false,
        }
    }

    /// Strip table alias from column references in an expression.
    /// Converts "o.amount" to "amount", "t.name" to "name", etc.
    /// This is needed when evaluating predicates against rows from fetch_rows_by_ids,
    /// which uses unqualified column names from the table schema.
    pub(super) fn strip_table_alias_from_expr(expr: &Expression) -> Expression {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                // Convert qualified identifier to unqualified
                Expression::Identifier(Identifier::new(
                    qid.name.token.clone(),
                    qid.name.value.clone(),
                ))
            }
            Expression::Infix(infix) => {
                // Recursively strip aliases from both sides
                Expression::Infix(InfixExpression::new(
                    infix.token.clone(),
                    Box::new(Self::strip_table_alias_from_expr(&infix.left)),
                    infix.operator.clone(),
                    Box::new(Self::strip_table_alias_from_expr(&infix.right)),
                ))
            }
            Expression::Prefix(prefix) => {
                // Recursively strip aliases from the inner expression
                Expression::Prefix(PrefixExpression {
                    token: prefix.token.clone(),
                    operator: prefix.operator.clone(),
                    op_type: prefix.op_type,
                    right: Box::new(Self::strip_table_alias_from_expr(&prefix.right)),
                })
            }
            Expression::FunctionCall(func) => {
                // Recursively strip aliases from function arguments
                let new_args: Vec<Expression> = func
                    .arguments
                    .iter()
                    .map(Self::strip_table_alias_from_expr)
                    .collect();
                Expression::FunctionCall(Box::new(FunctionCall {
                    token: func.token.clone(),
                    function: func.function.clone(),
                    arguments: new_args,
                    is_distinct: func.is_distinct,
                    order_by: func.order_by.clone(),
                    filter: func.filter.clone(),
                }))
            }
            Expression::Case(case) => {
                let new_value = case
                    .value
                    .as_ref()
                    .map(|v| Box::new(Self::strip_table_alias_from_expr(v)));
                let new_whens: Vec<WhenClause> = case
                    .when_clauses
                    .iter()
                    .map(|w| WhenClause {
                        token: w.token.clone(),
                        condition: Self::strip_table_alias_from_expr(&w.condition),
                        then_result: Self::strip_table_alias_from_expr(&w.then_result),
                    })
                    .collect();
                let new_else = case
                    .else_value
                    .as_ref()
                    .map(|e| Box::new(Self::strip_table_alias_from_expr(e)));
                Expression::Case(Box::new(CaseExpression {
                    token: case.token.clone(),
                    value: new_value,
                    when_clauses: new_whens,
                    else_value: new_else,
                }))
            }
            // For other expressions, return as-is
            _ => expr.clone(),
        }
    }
}
