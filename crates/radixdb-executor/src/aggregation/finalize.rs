use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Apply post-aggregation expressions to the result
    /// This handles expressions like `CASE WHEN SUM(x) > 100 THEN 'big' ELSE 'small' END`
    pub(super) fn apply_post_aggregation_expressions(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        agg_columns: Vec<String>,
        agg_rows: RowVec,
    ) -> Result<(Vec<String>, RowVec)> {
        // Parse GROUP BY items for GROUPING() function support
        let group_by_columns = self.parse_group_by(stmt, &agg_columns)?;

        // Check which original columns have correlated subqueries
        // These need per-row evaluation with outer row context, not pre-processing
        let correlated_flags: Vec<bool> = stmt
            .columns
            .iter()
            .map(|expression| self.host.aggregation_has_correlated_subqueries(expression))
            .collect();
        let has_any_correlated = correlated_flags.iter().any(|&f| f);

        // Pre-process scalar subqueries in SELECT columns (only non-correlated ones)
        // This executes subqueries like (SELECT SUM(amount) FROM sales) and replaces them
        // with their literal values before we process the column sources
        let processed_columns = if has_any_correlated {
            // Don't pre-process if we have correlated subqueries - handle them per-row
            None
        } else {
            self.host
                .aggregation_try_process_select_subqueries(&stmt.columns, ctx)?
        };
        let columns_to_use = processed_columns.as_ref().unwrap_or(&stmt.columns);

        // Build column index map for aggregate result columns
        let mut agg_col_index_map = build_column_index_map(&agg_columns);

        // Build additional mappings for aggregate expressions to handle deduplication
        // When aggregates are deduplicated (e.g., SUM(value) and SUM(value) AS total),
        // we need to map the expression "sum(value)" to the index even if the column
        // is named by its alias "total"
        for col_expr in &stmt.columns {
            if let Expression::Aliased(aliased) = col_expr {
                if let Expression::FunctionCall(func) = aliased.expression.as_ref() {
                    if is_aggregate_function(&func.function) {
                        let expr_name: String = self.get_aggregate_column_name(func).to_lowercase();
                        let alias_lower: String = aliased.alias.value_lower.to_string();
                        // If the alias exists in the map but the expression doesn't, add the expression
                        if let Some(&idx) = agg_col_index_map.get(&alias_lower) {
                            agg_col_index_map.entry(expr_name).or_insert(idx);
                        }
                        // If the expression exists in the map but the alias doesn't, add the alias
                        if let Some(&idx) = agg_col_index_map
                            .get(&self.get_aggregate_column_name(func).to_lowercase())
                        {
                            agg_col_index_map.entry(alias_lower).or_insert(idx);
                        }
                    }
                }
            } else if let Expression::FunctionCall(func) = col_expr {
                if is_aggregate_function(&func.function) {
                    let expr_name: String = self.get_aggregate_column_name(func).to_lowercase();
                    // Check if any aliased version exists for this expression
                    for other_col in &stmt.columns {
                        if let Expression::Aliased(other_aliased) = other_col {
                            if let Expression::FunctionCall(other_func) =
                                other_aliased.expression.as_ref()
                            {
                                if is_aggregate_function(&other_func.function) {
                                    let other_expr: String =
                                        self.get_aggregate_column_name(other_func).to_lowercase();
                                    if other_expr == expr_name {
                                        let alias_lower: String =
                                            other_aliased.alias.value_lower.to_string();
                                        if let Some(&idx) = agg_col_index_map.get(&alias_lower) {
                                            agg_col_index_map
                                                .entry(expr_name.clone())
                                                .or_insert(idx);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check if we have any expressions that need post-processing
        // This includes CASE, Prefix (-SUM(x)), Infix (SUM(x) + 1), and non-aggregate
        // functions wrapping aggregates like COALESCE(SUM(x), 0)
        let has_post_agg_exprs = columns_to_use.iter().any(|col| match col {
            Expression::Case(_) | Expression::Prefix(_) | Expression::Infix(_) => true,
            Expression::FunctionCall(func) => {
                // Non-aggregate function wrapping aggregate (e.g., COALESCE(SUM(val), 0))
                !is_aggregate_function(&func.function)
                    && func.arguments.iter().any(expression_contains_aggregate)
            }
            Expression::Aliased(a) => match a.expression.as_ref() {
                Expression::Case(_) | Expression::Prefix(_) | Expression::Infix(_) => true,
                Expression::FunctionCall(func) => {
                    // Non-aggregate function wrapping aggregate (e.g., COALESCE(SUM(val), 0) AS x)
                    !is_aggregate_function(&func.function)
                        && func.arguments.iter().any(expression_contains_aggregate)
                }
                _ => false,
            },
            _ => false,
        });

        // Check if SELECT columns match the aggregation columns (group by + aggregates)
        // If they differ, we need to project the result to match SELECT order
        let select_col_count = columns_to_use.len();
        let agg_col_count = agg_columns.len();
        let needs_projection = select_col_count != agg_col_count || has_post_agg_exprs;

        // Can't return early if we have correlated subqueries - they need per-row evaluation
        if !needs_projection && !has_any_correlated {
            // Check if columns are in the same order
            let mut columns_match = true;
            for (i, col_expr) in columns_to_use.iter().enumerate() {
                let expected_name: String = match col_expr {
                    Expression::Identifier(id) => id.value_lower.to_string(),
                    Expression::QualifiedIdentifier(qid) => {
                        format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower)
                    }
                    Expression::Aliased(a) => a.alias.value_lower.to_string(),
                    Expression::FunctionCall(func) if is_aggregate_function(&func.function) => {
                        self.get_aggregate_column_name(func).to_lowercase()
                    }
                    _ => continue, // Can't easily compare, assume mismatch
                };
                if i >= agg_columns.len() || agg_columns[i].to_lowercase() != expected_name {
                    columns_match = false;
                    break;
                }
            }
            if columns_match {
                // The physical aggregation columns may remain qualified so HAVING and
                // post-aggregation lookup can resolve JOIN inputs (for example `a.id`).
                // They are not, however, the public SELECT labels: SQL exposes the base
                // name of a qualified projection unless the user supplied an alias.
                let output_columns =
                    self.host
                        .aggregation_output_column_names(columns_to_use, &agg_columns, None);
                return Ok((output_columns, agg_rows));
            }
        }

        // Build new result with all SELECT columns in order
        let mut final_columns = Vec::new();
        let mut column_sources: Vec<ColumnSource> = Vec::new();

        // Helper to create the right ColumnSource based on whether expression has correlated subquery
        let make_source = |expr: &Expression, is_correlated: bool| -> ColumnSource {
            if is_correlated {
                ColumnSource::CorrelatedExpression(Box::new(expr.clone()))
            } else {
                ColumnSource::Expression(Box::new(expr.clone()))
            }
        };

        for (i, col_expr) in columns_to_use.iter().enumerate() {
            // Check if this column has a correlated subquery (use original column)
            let is_correlated = correlated_flags.get(i).copied().unwrap_or(false);
            // If correlated, we need to use the original expression
            let original_expr = &stmt.columns[i];

            match col_expr {
                Expression::Identifier(id) => {
                    final_columns.push(id.value.to_string());
                    column_sources.push(ColumnSource::AggColumn(id.value_lower.to_string()));
                }
                Expression::QualifiedIdentifier(qid) => {
                    // Use only the base column name (strip table alias prefix)
                    // e.g., u.username -> username
                    final_columns.push(qid.name.value.to_string());
                    // Try qualified name first, then fall back to unqualified column name
                    let qualified_lower =
                        format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    let unqualified_lower: String = qid.name.value_lower.to_string();
                    if agg_col_index_map.contains_key(&qualified_lower) {
                        column_sources.push(ColumnSource::AggColumn(qualified_lower));
                    } else {
                        column_sources.push(ColumnSource::AggColumn(unqualified_lower));
                    }
                }
                Expression::FunctionCall(func) => {
                    if is_aggregate_function(&func.function) {
                        let col_name = self.get_aggregate_column_name(func);
                        final_columns.push(col_name.clone());
                        column_sources.push(ColumnSource::AggColumn(col_name.to_lowercase()));
                    } else if func.function.eq_ignore_ascii_case("GROUPING") {
                        // GROUPING() function - map to the appropriate grouping flag column
                        final_columns.push(self.get_aggregate_column_name(func));
                        if let Some(idx) =
                            self.find_grouping_column_index(func, &group_by_columns, &agg_columns)
                        {
                            column_sources.push(ColumnSource::GroupingFlag(idx));
                        } else {
                            // If column not found, return 0 (treated as regularly grouped)
                            column_sources.push(ColumnSource::Expression(Box::new(
                                Expression::IntegerLiteral(radixdb_sql::ast::IntegerLiteral {
                                    token: radixdb_sql::token::Token::new(
                                        radixdb_sql::token::TokenType::Integer,
                                        "0",
                                        radixdb_sql::token::Position::new(0, 0, 0),
                                    ),
                                    value: 0,
                                }),
                            )));
                        }
                    } else {
                        // Non-aggregate function - evaluate it
                        final_columns.push(format!("{}(...)", func.function));
                        column_sources.push(make_source(col_expr, is_correlated));
                    }
                }
                Expression::Aliased(aliased) => {
                    final_columns.push(aliased.alias.value.to_string());
                    let alias_lower: String = aliased.alias.value_lower.to_string();

                    // First check if the alias matches an aggregation column name
                    // This handles GROUP BY expression columns like UPPER(name) AS upper_name
                    if agg_col_index_map.contains_key(&alias_lower) && !is_correlated {
                        column_sources.push(ColumnSource::AggColumn(alias_lower));
                    } else if is_correlated {
                        // For correlated subqueries, use the original expression for per-row eval
                        column_sources.push(ColumnSource::CorrelatedExpression(Box::new(
                            original_expr.clone(),
                        )));
                    } else {
                        match aliased.expression.as_ref() {
                            Expression::FunctionCall(func)
                                if is_aggregate_function(&func.function) =>
                            {
                                // Aliased aggregate - look up by expression name (e.g., "SUM(value)")
                                // not by alias, since aggregates may be deduplicated
                                let agg_name = self.get_aggregate_column_name(func).to_lowercase();
                                if agg_col_index_map.contains_key(&agg_name) {
                                    column_sources.push(ColumnSource::AggColumn(agg_name));
                                } else if agg_col_index_map.contains_key(&alias_lower) {
                                    // Fall back to alias if expression name not found
                                    column_sources.push(ColumnSource::AggColumn(alias_lower));
                                } else {
                                    // Last resort: evaluate the expression
                                    column_sources.push(ColumnSource::Expression(Box::new(
                                        aliased.expression.as_ref().clone(),
                                    )));
                                }
                            }
                            Expression::FunctionCall(func)
                                if func.function.eq_ignore_ascii_case("GROUPING") =>
                            {
                                // Aliased GROUPING() function
                                if let Some(idx) = self.find_grouping_column_index(
                                    func,
                                    &group_by_columns,
                                    &agg_columns,
                                ) {
                                    column_sources.push(ColumnSource::GroupingFlag(idx));
                                } else {
                                    column_sources.push(ColumnSource::Expression(Box::new(
                                        Expression::IntegerLiteral(
                                            radixdb_sql::ast::IntegerLiteral {
                                                token: radixdb_sql::token::Token::new(
                                                    radixdb_sql::token::TokenType::Integer,
                                                    "0",
                                                    radixdb_sql::token::Position::new(0, 0, 0),
                                                ),
                                                value: 0,
                                            },
                                        ),
                                    )));
                                }
                            }
                            Expression::Case(_) => {
                                // CASE with aggregates - needs evaluation
                                column_sources.push(ColumnSource::Expression(Box::new(
                                    aliased.expression.as_ref().clone(),
                                )));
                            }
                            _ => {
                                // Try to find it in agg columns by expression string,
                                // otherwise evaluate
                                let expr_str =
                                    self.expression_to_string(aliased.expression.as_ref());
                                let expr_lower = expr_str.to_lowercase();
                                if agg_col_index_map.contains_key(&expr_lower) {
                                    column_sources.push(ColumnSource::AggColumn(expr_lower));
                                } else {
                                    column_sources.push(ColumnSource::Expression(Box::new(
                                        aliased.expression.as_ref().clone(),
                                    )));
                                }
                            }
                        }
                    }
                }
                Expression::Case(_) => {
                    // Unnamed CASE - check if expression string matches an agg column
                    let expr_str = self.expression_to_string(col_expr);
                    let expr_lower = expr_str.to_lowercase();
                    final_columns.push(expr_str);
                    if agg_col_index_map.contains_key(&expr_lower) && !is_correlated {
                        // CASE is a GROUP BY column - use existing value
                        column_sources.push(ColumnSource::AggColumn(expr_lower));
                    } else {
                        // CASE with aggregates or correlated - needs evaluation
                        column_sources.push(make_source(col_expr, is_correlated));
                    }
                }
                _ => {
                    // Other expressions
                    final_columns.push(self.expression_to_string(col_expr));
                    column_sources.push(make_source(col_expr, is_correlated));
                }
            }
        }

        // Check if we have any correlated expressions that need special handling
        let has_correlated_sources = column_sources
            .iter()
            .any(|s| matches!(s, ColumnSource::CorrelatedExpression(_)));

        // Evaluate each row
        let mut final_rows = RowVec::with_capacity(agg_rows.len());
        let mut evaluator = CompiledEvaluator::new(radixdb_functions::registry::global_registry());
        evaluator.init_columns(&agg_columns);

        // Add aggregate expression aliases so COALESCE(SUM(val), 0) can find the "sum(val)" column
        // when the aggregate is named by its alias (e.g., "raw" from SUM(val) AS raw)
        let agg_aliases: Vec<(String, usize)> = agg_col_index_map
            .iter()
            .map(|(name, &idx)| (name.clone(), idx))
            .collect();
        evaluator.add_aggregate_aliases(&agg_aliases);
        let group_expression_aliases: Vec<(String, usize)> = group_by_columns
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                GroupByItem::Expression { expr, .. } => {
                    Some((self.expression_to_string(expr), index))
                }
                _ => None,
            })
            .collect();
        evaluator.add_expression_aliases(&group_expression_aliases);

        // Pre-compute outer row column names if we have correlated expressions
        let outer_col_names: Option<CompactArc<Vec<String>>> = if has_correlated_sources {
            Some(CompactArc::new(agg_columns.clone()))
        } else {
            None
        };

        // Extract table alias from FROM clause for qualified column names in correlated subqueries
        let table_alias: Option<String> = if has_correlated_sources {
            if let Some(ref table_expr) = stmt.table_expr {
                match table_expr.as_ref() {
                    Expression::TableSource(source) => {
                        if let Some(ref alias) = source.alias {
                            Some(alias.value_lower.to_string())
                        } else {
                            Some(source.name.value_lower.to_string())
                        }
                    }
                    Expression::Aliased(aliased) => Some(aliased.alias.value_lower.to_string()),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };

        // OPTIMIZATION: Pre-compute lowercase and qualified column names for correlated expressions
        // This avoids repeated to_lowercase() and format!() allocations per row
        // Uses CompactArc<str> for zero-cost cloning in the per-row loop
        #[allow(clippy::type_complexity)]
        let correlated_col_names: Option<Vec<(CompactArc<str>, Option<CompactArc<str>>)>> =
            if has_correlated_sources {
                Some(
                    agg_columns
                        .iter()
                        .map(|col_name| {
                            let col_lower: CompactArc<str> =
                                CompactArc::from(col_name.to_lowercase().as_str());
                            let qualified = table_alias.as_ref().map(|alias| {
                                CompactArc::from(format!("{}.{}", alias, col_lower).as_str())
                            });
                            (col_lower, qualified)
                        })
                        .collect(),
                )
            } else {
                None
            };

        // Reusable map for correlated expressions
        // Uses CompactArc<str> keys for zero-cost cloning
        let estimated_entries = agg_columns.len() * 2;
        let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
            FxHashMap::with_capacity_and_hasher(estimated_entries, Default::default());

        for (id, row) in agg_rows {
            // Use CompactVec directly to avoid Vec→CompactVec conversion
            let mut new_values: CompactVec<Value> = CompactVec::with_capacity(column_sources.len());
            evaluator.set_row_array(&row);

            for source in &column_sources {
                let value = match source {
                    ColumnSource::AggColumn(col_name) => {
                        let &idx = agg_col_index_map.get(col_name).ok_or_else(|| {
                            Error::internal(format!(
                                "aggregate projection column {col_name} is missing"
                            ))
                        })?;
                        row.get(idx).cloned().ok_or_else(|| {
                            Error::internal(format!(
                                "aggregate projection index {idx} is outside row width {}",
                                row.len()
                            ))
                        })?
                    }
                    ColumnSource::Expression(expr) => {
                        // Evaluate the expression using the aggregated row as context
                        evaluator.evaluate(expr)?
                    }
                    ColumnSource::CorrelatedExpression(expr) => {
                        // Build outer row context using pre-computed column names
                        outer_row_map.clear();
                        if let Some(ref col_names) = correlated_col_names {
                            for (idx, (col_lower, qualified)) in col_names.iter().enumerate() {
                                let val = row.get(idx).cloned().unwrap_or(Value::null_unknown());
                                outer_row_map.insert(col_lower.clone(), val.clone());
                                if let Some(q) = qualified {
                                    outer_row_map.insert(q.clone(), val);
                                }
                            }
                        }

                        // Create context with outer row (move map, take it back after)
                        let mut correlated_ctx = ctx.with_outer_row(
                            std::mem::take(&mut outer_row_map),
                            outer_col_names.clone().unwrap(),
                        );

                        // Process the correlated expression with the outer row context
                        let result = self
                            .host
                            .aggregation_process_correlated_expression(expr, &correlated_ctx)
                            .and_then(|processed_expr| {
                                // Evaluate the processed expression
                                let mut corr_eval = CompiledEvaluator::new(
                                    self.host.aggregation_function_registry(),
                                )
                                .with_context(&correlated_ctx);
                                corr_eval.init_columns(&agg_columns);
                                corr_eval.set_row_array(&row);
                                corr_eval.evaluate(&processed_expr)
                            });

                        // Take back map for reuse
                        outer_row_map = correlated_ctx.take_outer_row().unwrap_or_default();
                        result?
                    }
                    ColumnSource::GroupingFlag(idx) => {
                        // Look up the grouping flag from the hidden __grouping_N__ columns
                        // These columns are at the end of the row, after aggregate columns
                        let grouping_col_name = format!("__grouping_{}__", idx);
                        if let Some(&col_idx) = agg_col_index_map.get(&grouping_col_name) {
                            row.get(col_idx).cloned().unwrap_or(Value::Integer(0))
                        } else {
                            // Fallback: column is grouped normally
                            Value::Integer(0)
                        }
                    }
                };
                new_values.push(value);
            }

            final_rows.push((id, Row::from_compact_vec(new_values)));
        }

        Ok((final_columns, final_rows))
    }

    /// Get the column name for an aggregate function
    pub(super) fn get_aggregate_column_name(
        &self,
        func: &radixdb_sql::ast::FunctionCall,
    ) -> String {
        let args_str: Vec<String> = func
            .arguments
            .iter()
            .map(|a| self.expression_to_string(a))
            .collect();
        format!("{}({})", func.function, args_str.join(", "))
    }

    /// Find the GROUP BY column index for a GROUPING() function call
    /// Returns the index (0-based) of the GROUP BY column that matches the GROUPING() argument
    pub(super) fn find_grouping_column_index(
        &self,
        func: &radixdb_sql::ast::FunctionCall,
        group_by_columns: &[GroupByItem],
        columns: &[String],
    ) -> Option<usize> {
        // GROUPING() takes one argument - the column name
        if func.arguments.is_empty() {
            return None;
        }

        let arg = &func.arguments[0];
        let arg_name: &str = match arg {
            Expression::Identifier(id) => id.value_lower.as_str(),
            Expression::QualifiedIdentifier(qid) => qid.name.value_lower.as_str(),
            _ => return None,
        };

        // Find the matching GROUP BY column
        for (idx, item) in group_by_columns.iter().enumerate() {
            let matches = match item {
                GroupByItem::Column(col_name) => col_name.to_lowercase() == arg_name,
                GroupByItem::Position(pos) => {
                    // Position is 1-indexed, convert to 0-indexed
                    let col_idx = pos.saturating_sub(1);
                    if col_idx < columns.len() {
                        columns[col_idx].to_lowercase() == arg_name
                    } else {
                        false
                    }
                }
                GroupByItem::Expression { display_name, .. } => {
                    display_name.to_lowercase() == arg_name
                }
            };
            if matches {
                return Some(idx);
            }
        }

        None
    }

    /// Execute GROUP BY aggregation and return raw columns/rows for window function processing
    /// This is used when both GROUP BY and window functions are present in the query.
    /// Window functions operate on the aggregated result.
    pub(crate) fn execute_aggregation_for_window(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
    ) -> Result<(Vec<String>, RowVec)> {
        // Parse aggregations and group by columns
        let (aggregations, _non_agg_columns) = self.parse_aggregations(stmt)?;
        let group_by_columns = self.parse_group_by(stmt, base_columns)?;

        // Create column index map for fast lookup
        let col_index_map = build_column_index_map(base_columns);

        // Build result
        // Note: No limit pushdown here because window functions need all rows
        let (result_columns, result_rows) = if group_by_columns.is_empty() {
            // Global aggregation (no GROUP BY)
            self.execute_global_aggregation(
                &aggregations,
                base_rows,
                base_columns,
                &col_index_map,
                ctx,
            )?
        } else {
            // Grouped aggregation - no limit since window functions need all groups
            // Discard the having_applied flag - window functions apply HAVING separately
            let (cols, rows, _having_applied) = self.execute_grouped_aggregation(
                &aggregations,
                &group_by_columns,
                base_rows,
                base_columns,
                &col_index_map,
                stmt,
                ctx,
                None, // Window functions need all groups
            )?;
            (cols, rows)
        };

        // Apply HAVING clause filter (in-place)
        let mut result_rows_with_ids = RowVec::with_capacity(result_rows.len());
        if let Some(ref having) = stmt.having {
            // Build aggregate expression aliases for HAVING clause
            // This maps "SUM(price)" to its column index even if aliased as "total"
            // IMPORTANT: Include ALL aggregates, not just aliased ones,
            // because the evaluator needs expression_aliases to match FunctionCall expressions
            let group_by_count = group_by_columns.len();
            let mut all_aliases: Vec<(String, usize)> = aggregations
                .iter()
                .enumerate()
                .map(|(i, agg)| (agg.get_expression_name(), group_by_count + i))
                .collect();

            // Build GROUP BY expression aliases for HAVING clause
            // This maps expressions like "x + y" to their GROUP BY column indices
            // allowing HAVING x + y > 20 to work when GROUP BY x + y
            for (i, item) in group_by_columns.iter().enumerate() {
                if let GroupByItem::Expression { expr, .. } = item {
                    all_aliases.push((self.expression_to_string(expr), i));
                }
            }

            // Create RowFilter with all aliases and context
            let having_filter =
                RowFilter::with_aliases_and_context(having, &result_columns, &all_aliases, ctx)?;

            // Filter rows using the pre-compiled filter
            for (id, row) in result_rows {
                if having_filter.matches_checked(&row)? {
                    result_rows_with_ids.push((id, row));
                }
            }
        } else {
            for (id, row) in result_rows {
                result_rows_with_ids.push((id, row));
            }
        }

        Ok((result_columns, result_rows_with_ids))
    }
}
