use super::*;

impl<'host, H: SubqueryHost + ?Sized> SubqueryExecutor<'host, H> {
    /// Convert ALL/ANY expression to an equivalent expression that the evaluator can handle.
    ///
    /// This executes the subquery once and converts:
    /// - `x = ANY (values)` → `x IN (values)`
    /// - `x <> ALL (values)` → `x NOT IN (values)`
    /// - `x op ANY (values)` → `x op v1 OR x op v2 OR ...` (or optimized MIN/MAX)
    /// - `x op ALL (values)` → `x op v1 AND x op v2 AND ...` (or optimized MIN/MAX)
    pub(super) fn convert_all_any_to_expression(
        &self,
        all_any: &AllAnyExpression,
        values: Vec<radixdb_core::Value>,
    ) -> Result<Expression> {
        use radixdb_sql::ast::AllAnyType;

        let op = all_any.operator.as_str();

        // Handle empty result set
        if values.is_empty() {
            return match all_any.all_any_type {
                AllAnyType::All => {
                    // ALL with empty set is vacuously TRUE
                    Ok(Expression::BooleanLiteral(BooleanLiteral {
                        token: dummy_token("TRUE", TokenType::Keyword),
                        value: true,
                    }))
                }
                AllAnyType::Any => {
                    // ANY with empty set is FALSE (no value satisfies the condition)
                    Ok(Expression::BooleanLiteral(BooleanLiteral {
                        token: dummy_token("FALSE", TokenType::Keyword),
                        value: false,
                    }))
                }
            };
        }

        // Convert values to expressions
        let value_exprs: Vec<Expression> = values.iter().map(value_to_expression).collect();

        // Special case: = ANY is equivalent to IN
        if op == "=" && matches!(all_any.all_any_type, AllAnyType::Any) {
            return Ok(Expression::In(InExpression {
                token: all_any.token.clone(),
                left: all_any.left.clone(),
                right: Box::new(Expression::ExpressionList(Box::new(ExpressionList {
                    token: dummy_token("(", TokenType::Punctuator),
                    expressions: value_exprs,
                }))),
                not: false,
            }));
        }

        // Special case: <> ALL is equivalent to NOT IN
        if (op == "<>" || op == "!=") && matches!(all_any.all_any_type, AllAnyType::All) {
            return Ok(Expression::In(InExpression {
                token: all_any.token.clone(),
                left: all_any.left.clone(),
                right: Box::new(Expression::ExpressionList(Box::new(ExpressionList {
                    token: dummy_token("(", TokenType::Punctuator),
                    expressions: value_exprs,
                }))),
                not: true,
            }));
        }

        // Fold every comparison explicitly. MIN/MAX rewrites discard NULLs,
        // but FALSE OR UNKNOWN and TRUE AND UNKNOWN must remain UNKNOWN.
        let logical_op = match all_any.all_any_type {
            AllAnyType::All => "AND",
            AllAnyType::Any => "OR",
        };

        // Build: (left op v1) AND/OR (left op v2) AND/OR ...
        let mut result_expr: Option<Expression> = None;

        for value_expr in value_exprs {
            let comparison = Expression::Infix(InfixExpression::new(
                all_any.token.clone(),
                all_any.left.clone(),
                op.to_string(),
                Box::new(value_expr),
            ));

            result_expr = Some(match result_expr {
                None => comparison,
                Some(prev) => Expression::Infix(InfixExpression::new(
                    all_any.token.clone(),
                    Box::new(prev),
                    logical_op.to_string(),
                    Box::new(comparison),
                )),
            });
        }

        Ok(result_expr.unwrap_or_else(|| {
            Expression::BooleanLiteral(BooleanLiteral {
                token: dummy_token("TRUE", TokenType::Keyword),
                value: true,
            })
        }))
    }

    /// Execute a scalar subquery and return its single value.
    /// For non-correlated subqueries (no outer row context), results are cached
    /// to avoid re-execution when the same subquery appears multiple times.
    pub(super) fn execute_scalar_subquery(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<radixdb_core::Value> {
        // Check if this is a non-correlated subquery (no outer row context)
        // Non-correlated subqueries can be cached since they return the same result
        let is_non_correlated =
            ctx.outer_row().is_none() && !Self::is_subquery_correlated(subquery);

        // For non-correlated subqueries, check cache first using SQL string as key
        let cache_key = if is_non_correlated {
            let key = subquery.to_string();
            if let Some(cached_value) = get_cached_scalar_subquery(&key) {
                return Ok(cached_value);
            }
            Some(key)
        } else {
            None
        };

        // OPTIMIZATION: For correlated scalar subqueries with LIMIT, index-based is faster
        // because it only checks rows for the limited outer rows.
        // Batch aggregate is faster for large outer result sets (no LIMIT or large LIMIT).
        if !is_non_correlated {
            // First check if batch aggregate cache already exists (O(1) lookup)
            if let Some(value) = Self::try_lookup_batch_aggregate(subquery, ctx) {
                return Ok(value);
            }

            // Try index-based COUNT first (faster for LIMIT queries)
            if let Some(count) = self.try_execute_scalar_count_with_index(subquery, ctx)? {
                return Ok(radixdb_core::Value::Integer(count));
            }

            // Fall back to batch aggregate for cases index doesn't handle
            if let Some(batch_cache) = self.try_execute_and_cache_batch_aggregate(subquery, ctx)? {
                if let Some(value) = Self::try_lookup_batch_aggregate(subquery, ctx) {
                    return Ok(value);
                }
                if Self::is_count_expression(&subquery.columns[0]) {
                    return Ok(Value::Integer(0));
                }
                drop(batch_cache);
            }
        }

        // Execute the subquery with incremented depth to avoid creating new TimeoutGuard
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self.host.subquery_execute_select(subquery, &subquery_ctx)?;
        if result.columns().len() != 1 {
            return Err(Error::InvalidArgument(format!(
                "scalar subquery must return exactly one column; got {}",
                result.columns().len()
            )));
        }

        // Get the first row
        if !result.next() {
            // Check for runtime filter errors before treating as empty result
            if let Some(err) = result.last_error() {
                return Err(err);
            }
            let null_value = radixdb_core::Value::null_unknown();
            // Cache the result for non-correlated subqueries
            if let Some(key) = cache_key {
                cache_scalar_subquery(
                    key,
                    extract_table_names_for_cache(subquery),
                    null_value.clone(),
                );
            }
            return Ok(null_value);
        }

        let row = result.take_row();
        // take_first_value() is more efficient than get(0).cloned()
        let first_value = match row.take_first_value() {
            Some(v) => v,
            None => {
                let null_value = radixdb_core::Value::null_unknown();
                if let Some(key) = cache_key {
                    cache_scalar_subquery(
                        key,
                        extract_table_names_for_cache(subquery),
                        null_value.clone(),
                    );
                }
                return Ok(null_value);
            }
        };

        // Check that there's only one row (scalar subquery should return single value)
        if result.next() {
            return Err(Error::Internal {
                message: "scalar subquery returned more than one row".to_string(),
            });
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        // Cache the result for non-correlated subqueries
        if let Some(key) = cache_key {
            cache_scalar_subquery(
                key,
                extract_table_names_for_cache(subquery),
                first_value.clone(),
            );
        }

        Ok(first_value)
    }

    /// Execute an IN subquery and return its values.
    /// For non-correlated subqueries (no outer row context), results are cached
    /// to avoid re-execution when the same subquery appears multiple times.
    pub(super) fn execute_in_subquery(
        &self,
        subquery: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Vec<radixdb_core::Value>> {
        // Check if this is a non-correlated subquery (no outer row context)
        // Non-correlated subqueries can be cached since they return the same result
        let is_non_correlated =
            ctx.outer_row().is_none() && !Self::is_subquery_correlated(subquery);

        // For non-correlated subqueries, check cache first using SQL string as key
        let cache_key = if is_non_correlated {
            let key = subquery.to_string();
            if let Some(cached_values) = get_cached_in_subquery(&key) {
                return Ok(cached_values);
            }
            Some(key)
        } else {
            None
        };

        // Execute the subquery with incremented depth to avoid creating new TimeoutGuard
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self.host.subquery_execute_select(subquery, &subquery_ctx)?;
        if result.columns().len() != 1 {
            return Err(Error::InvalidArgument(format!(
                "IN/ANY/ALL subquery must return exactly one column; got {}",
                result.columns().len()
            )));
        }

        // Collect all values from the first column - use take_row() to avoid cloning
        let mut values = Vec::new();
        while result.next() {
            let row = result.take_row();
            // take_first_value() is more efficient than into_values().swap_remove(0)
            if let Some(value) = row.take_first_value() {
                values.push(value);
            }
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        // Cache the result for non-correlated subqueries
        if let Some(key) = cache_key {
            cache_in_subquery(key, extract_table_names_for_cache(subquery), values.clone());
        }

        Ok(values)
    }

    /// Execute an IN subquery and return all rows (for multi-column IN)
    pub(super) fn execute_in_subquery_rows(
        &self,
        subquery: &SelectStatement,
        expected_width: usize,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Vec<radixdb_core::Value>>> {
        // Execute the subquery with incremented depth to avoid creating new TimeoutGuard
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self.host.subquery_execute_select(subquery, &subquery_ctx)?;
        if result.columns().len() != expected_width {
            return Err(Error::InvalidArgument(format!(
                "tuple IN width {} does not match subquery width {}",
                expected_width,
                result.columns().len()
            )));
        }

        // Collect all values from all columns - use take_row() to avoid cloning
        let mut rows = Vec::new();
        while result.next() {
            let row = result.take_row();
            if !row.is_empty() {
                // into_values() uses Arc::try_unwrap() to move without cloning when sole owner
                rows.push(row.into_values());
            }
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        Ok(rows)
    }

    /// Check if an expression contains EXISTS or other subqueries that need processing
    pub(super) fn has_subqueries(expr: &Expression) -> bool {
        let mut found = false;
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| {
            found |= matches!(
                expression,
                Expression::Exists(_) | Expression::ScalarSubquery(_) | Expression::AllAny(_)
            );
        });
        found
    }

    /// Process subqueries in SELECT column expressions (single-pass optimization)
    ///
    /// Returns `None` if no subqueries were found (caller should use original columns).
    /// Returns `Some(processed)` if any subqueries were found and processed.
    ///
    /// This combines the check and processing into a single traversal to avoid
    /// walking the expression tree twice.
    pub(super) fn try_process_select_subqueries(
        &self,
        columns: &[Expression],
        ctx: &ExecutionContext,
    ) -> Result<Option<Vec<Expression>>> {
        let mut result: Option<Vec<Expression>> = None;

        for (i, col) in columns.iter().enumerate() {
            if let Some(processed) = self.try_process_expression_subqueries(col, ctx)? {
                // Lazily initialize result vec, copying prior columns
                let vec = result.get_or_insert_with(|| columns[..i].to_vec());
                vec.push(processed);
            } else if let Some(ref mut vec) = result {
                // No subquery in this column, but we're already building a new vec
                vec.push(col.clone());
            }
            // If result is None and no subquery found, do nothing (use original)
        }

        Ok(result)
    }

    /// Try to process subqueries in an expression (single-pass optimization)
    ///
    /// Returns `None` if no subqueries were found (expression unchanged).
    /// Returns `Some(processed)` if any subqueries were found and processed.
    pub(super) fn try_process_expression_subqueries(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Option<Expression>> {
        if Self::has_subqueries(expr) {
            return Ok(Some(self.process_where_subqueries(expr, ctx)?));
        }
        match expr {
            Expression::ScalarSubquery(subquery) => {
                // Execute scalar subquery and replace with literal value
                let value = self.execute_scalar_subquery(&subquery.subquery, ctx)?;
                Ok(Some(value_to_expression(&value)))
            }

            Expression::Exists(exists) => {
                // Execute EXISTS subquery and replace with boolean literal
                let exists_result = self.execute_exists_subquery(&exists.subquery, ctx)?;
                Ok(Some(Expression::BooleanLiteral(BooleanLiteral {
                    token: dummy_token(
                        if exists_result { "TRUE" } else { "FALSE" },
                        TokenType::Keyword,
                    ),
                    value: exists_result,
                })))
            }

            Expression::Aliased(aliased) => {
                // Only create new expression if inner has subqueries
                if let Some(processed) =
                    self.try_process_expression_subqueries(&aliased.expression, ctx)?
                {
                    Ok(Some(Expression::Aliased(AliasedExpression {
                        token: aliased.token.clone(),
                        expression: Box::new(processed),
                        alias: aliased.alias.clone(),
                    })))
                } else {
                    Ok(None)
                }
            }

            Expression::Infix(infix) => {
                // Process both sides, only create new expression if either changed
                let left = self.try_process_expression_subqueries(&infix.left, ctx)?;
                let right = self.try_process_expression_subqueries(&infix.right, ctx)?;

                if left.is_some() || right.is_some() {
                    Ok(Some(Expression::Infix(InfixExpression {
                        token: infix.token.clone(),
                        left: Box::new(left.unwrap_or_else(|| (*infix.left).clone())),
                        operator: infix.operator.clone(),
                        op_type: infix.op_type,
                        right: Box::new(right.unwrap_or_else(|| (*infix.right).clone())),
                    })))
                } else {
                    Ok(None)
                }
            }

            Expression::Prefix(prefix) => {
                if let Some(processed) =
                    self.try_process_expression_subqueries(&prefix.right, ctx)?
                {
                    Ok(Some(Expression::Prefix(PrefixExpression {
                        token: prefix.token.clone(),
                        operator: prefix.operator.clone(),
                        op_type: prefix.op_type,
                        right: Box::new(processed),
                    })))
                } else {
                    Ok(None)
                }
            }

            Expression::FunctionCall(func) => {
                // Process arguments, only create new expression if any changed
                let mut any_changed = false;
                let mut processed_args: Vec<Option<Expression>> =
                    Vec::with_capacity(func.arguments.len());

                for arg in &func.arguments {
                    let processed = self.try_process_expression_subqueries(arg, ctx)?;
                    if processed.is_some() {
                        any_changed = true;
                    }
                    processed_args.push(processed);
                }

                if any_changed {
                    let final_args: Vec<Expression> = func
                        .arguments
                        .iter()
                        .zip(processed_args)
                        .map(|(orig, processed)| processed.unwrap_or_else(|| orig.clone()))
                        .collect();

                    Ok(Some(Expression::FunctionCall(Box::new(FunctionCall {
                        token: func.token.clone(),
                        function: func.function.clone(),
                        arguments: final_args,
                        is_distinct: func.is_distinct,
                        order_by: func.order_by.clone(),
                        filter: func.filter.clone(),
                    }))))
                } else {
                    Ok(None)
                }
            }

            Expression::Case(case) => {
                // Process CASE expression to handle subqueries in any part
                let mut any_changed = false;

                // Process the operand (if present)
                let processed_value = if let Some(ref value) = case.value {
                    let processed = self.try_process_expression_subqueries(value, ctx)?;
                    if processed.is_some() {
                        any_changed = true;
                    }
                    processed.map(Box::new)
                } else {
                    None
                };

                // Process each WHEN clause
                let mut processed_whens: Vec<(Option<Expression>, Option<Expression>)> =
                    Vec::with_capacity(case.when_clauses.len());
                for when in &case.when_clauses {
                    let cond = self.try_process_expression_subqueries(&when.condition, ctx)?;
                    let then = self.try_process_expression_subqueries(&when.then_result, ctx)?;
                    if cond.is_some() || then.is_some() {
                        any_changed = true;
                    }
                    processed_whens.push((cond, then));
                }

                // Process the ELSE clause (if present)
                let processed_else = if let Some(ref else_val) = case.else_value {
                    let processed = self.try_process_expression_subqueries(else_val, ctx)?;
                    if processed.is_some() {
                        any_changed = true;
                    }
                    processed.map(Box::new)
                } else {
                    None
                };

                if any_changed {
                    let final_whens: Vec<WhenClause> = case
                        .when_clauses
                        .iter()
                        .zip(processed_whens)
                        .map(|(orig, (cond, then))| WhenClause {
                            token: orig.token.clone(),
                            condition: cond.unwrap_or_else(|| orig.condition.clone()),
                            then_result: then.unwrap_or_else(|| orig.then_result.clone()),
                        })
                        .collect();

                    Ok(Some(Expression::Case(Box::new(CaseExpression {
                        token: case.token.clone(),
                        value: processed_value.or_else(|| case.value.clone()),
                        when_clauses: final_whens,
                        else_value: processed_else.or_else(|| case.else_value.clone()),
                    }))))
                } else {
                    Ok(None)
                }
            }

            Expression::Cast(cast) => {
                // Process inner expression for subqueries
                if let Some(processed) = self.try_process_expression_subqueries(&cast.expr, ctx)? {
                    Ok(Some(Expression::Cast(CastExpression {
                        token: cast.token.clone(),
                        expr: Box::new(processed),
                        type_name: cast.type_name.clone(),
                    })))
                } else {
                    Ok(None)
                }
            }

            Expression::AllAny(all_any) => {
                // Execute the subquery to get all values
                let values = self.execute_in_subquery(&all_any.subquery, ctx)?;

                // Convert ALL/ANY to an equivalent expression that the evaluator can handle
                Ok(Some(self.convert_all_any_to_expression(all_any, values)?))
            }

            // No subqueries possible in other expression types
            _ => Ok(None),
        }
    }

    // ============================================================================
    // Correlated Subquery Support
    // ============================================================================

    /// Check if an expression contains correlated subqueries that reference outer columns.
    /// A correlated subquery references columns from outer tables that are not defined
    /// in the subquery's own FROM clause.
    pub(super) fn has_correlated_subqueries(expr: &Expression) -> bool {
        let mut correlated = false;
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| {
            if correlated {
                return;
            }
            correlated = match expression {
                Expression::Exists(exists) => Self::is_subquery_correlated(&exists.subquery),
                Expression::ScalarSubquery(subquery) => {
                    Self::is_subquery_correlated(&subquery.subquery)
                }
                Expression::AllAny(all_any) => Self::is_subquery_correlated(&all_any.subquery),
                _ => false,
            };
        });
        correlated
    }

    /// Check if any SELECT column expressions contain correlated subqueries
    pub(super) fn has_correlated_select_subqueries(columns: &[Expression]) -> bool {
        columns.iter().any(Self::has_correlated_subqueries)
    }

    /// Process a single expression with correlated subqueries, replacing scalar subqueries
    /// with their evaluated values using the provided outer row context.
    pub(super) fn process_correlated_expression(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        if Self::has_subqueries(expr) {
            return self.process_where_subqueries(expr, ctx);
        }
        match expr {
            Expression::ScalarSubquery(subquery) => {
                // Execute scalar subquery with outer row context
                let value = self.execute_scalar_subquery(&subquery.subquery, ctx)?;
                Ok(value_to_expression(&value))
            }

            Expression::Exists(exists) => {
                // Execute EXISTS subquery with outer row context
                let exists_result = self.execute_exists_subquery(&exists.subquery, ctx)?;
                Ok(Expression::BooleanLiteral(BooleanLiteral {
                    token: dummy_token(
                        if exists_result { "TRUE" } else { "FALSE" },
                        TokenType::Keyword,
                    ),
                    value: exists_result,
                }))
            }

            Expression::Aliased(aliased) => {
                let processed = self.process_correlated_expression(&aliased.expression, ctx)?;
                Ok(Expression::Aliased(AliasedExpression {
                    token: aliased.token.clone(),
                    expression: Box::new(processed),
                    alias: aliased.alias.clone(),
                }))
            }

            Expression::Infix(infix) => {
                let left = self.process_correlated_expression(&infix.left, ctx)?;
                let right = self.process_correlated_expression(&infix.right, ctx)?;
                Ok(Expression::Infix(InfixExpression {
                    token: infix.token.clone(),
                    left: Box::new(left),
                    operator: infix.operator.clone(),
                    op_type: infix.op_type,
                    right: Box::new(right),
                }))
            }

            Expression::Prefix(prefix) => {
                let right = self.process_correlated_expression(&prefix.right, ctx)?;
                Ok(Expression::Prefix(PrefixExpression {
                    token: prefix.token.clone(),
                    operator: prefix.operator.clone(),
                    op_type: prefix.op_type,
                    right: Box::new(right),
                }))
            }

            Expression::FunctionCall(func) => {
                let processed_args: Result<Vec<Expression>> = func
                    .arguments
                    .iter()
                    .map(|arg| self.process_correlated_expression(arg, ctx))
                    .collect();

                Ok(Expression::FunctionCall(Box::new(FunctionCall {
                    token: func.token.clone(),
                    function: func.function.clone(),
                    arguments: processed_args?,
                    is_distinct: func.is_distinct,
                    order_by: func.order_by.clone(),
                    filter: func.filter.clone(),
                })))
            }

            Expression::In(in_expr) => {
                let processed_left = self.process_correlated_expression(&in_expr.left, ctx)?;

                if let Expression::ScalarSubquery(subquery) = in_expr.right.as_ref() {
                    // Use InHashSet for O(1) lookups with FxHash (optimized for Value types with WyMix)
                    let values = self.execute_in_subquery(&subquery.subquery, ctx)?;
                    let hash_set: ValueSet = values.into_iter().collect();

                    return Ok(Expression::InHashSet(InHashSetExpression {
                        token: in_expr.token.clone(),
                        column: Box::new(processed_left),
                        values: CompactArc::new(hash_set),
                        not: in_expr.not,
                    }));
                }

                let processed_right = self.process_correlated_expression(&in_expr.right, ctx)?;
                Ok(Expression::In(InExpression {
                    token: in_expr.token.clone(),
                    left: Box::new(processed_left),
                    right: Box::new(processed_right),
                    not: in_expr.not,
                }))
            }

            Expression::Between(between) => {
                let processed_expr = self.process_correlated_expression(&between.expr, ctx)?;
                let processed_lower = self.process_correlated_expression(&between.lower, ctx)?;
                let processed_upper = self.process_correlated_expression(&between.upper, ctx)?;

                Ok(Expression::Between(BetweenExpression {
                    token: between.token.clone(),
                    expr: Box::new(processed_expr),
                    not: between.not,
                    lower: Box::new(processed_lower),
                    upper: Box::new(processed_upper),
                }))
            }

            Expression::Case(case) => {
                let processed_value = if let Some(ref value) = case.value {
                    Some(Box::new(self.process_correlated_expression(value, ctx)?))
                } else {
                    None
                };

                let processed_whens: Result<Vec<WhenClause>> = case
                    .when_clauses
                    .iter()
                    .map(|when| {
                        Ok(WhenClause {
                            token: when.token.clone(),
                            condition: self.process_correlated_expression(&when.condition, ctx)?,
                            then_result: self
                                .process_correlated_expression(&when.then_result, ctx)?,
                        })
                    })
                    .collect();

                let processed_else = if let Some(ref else_val) = case.else_value {
                    Some(Box::new(self.process_correlated_expression(else_val, ctx)?))
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
                let processed_expr = self.process_correlated_expression(&cast.expr, ctx)?;
                Ok(Expression::Cast(CastExpression {
                    token: cast.token.clone(),
                    expr: Box::new(processed_expr),
                    type_name: cast.type_name.clone(),
                }))
            }

            // For all other expression types, return as-is
            _ => Ok(expr.clone()),
        }
    }

    /// Check if a subquery is correlated (references outer columns)
    pub(super) fn is_subquery_correlated(subquery: &SelectStatement) -> bool {
        // Get table/alias names defined in the subquery's FROM clause
        let subquery_tables = Self::collect_subquery_table_columns(subquery);
        let mut correlated = false;
        radixdb_sql::ast::walk_select_tree(subquery, &mut |expression| {
            if correlated {
                return;
            }
            match expression {
                Expression::QualifiedIdentifier(qid) => {
                    correlated = !subquery_tables
                        .iter()
                        .any(|table| table.eq_ignore_ascii_case(&qid.qualifier.value_lower));
                }
                // Without a bound schema an unqualified name cannot safely be
                // proven inner. Treat it as correlated; this may disable a cache
                // but cannot reuse one outer row's result for another.
                Expression::Identifier(_) => correlated = true,
                _ => {}
            }
        });
        correlated
    }

    /// Collect table/alias names from a subquery's FROM clause
    pub(super) fn collect_subquery_table_columns(subquery: &SelectStatement) -> Vec<String> {
        let mut tables = Vec::new();

        if let Some(ref table_expr) = subquery.table_expr {
            Self::collect_table_names_from_source(table_expr, &mut tables);
        }

        tables
    }

    /// Recursively collect table names from a table source expression
    pub(super) fn collect_table_names_from_source(source: &Expression, tables: &mut Vec<String>) {
        match source {
            Expression::TableSource(ts) => {
                // When a table has an alias, SQL semantics require using the alias,
                // not the original table name. So for `FROM t t2`, only `t2` is valid.
                // If there's no alias, use the table name.
                if let Some(ref alias) = ts.alias {
                    tables.push(alias.value_lower.to_string());
                } else {
                    tables.push(ts.name.value_lower.to_string());
                }
            }
            Expression::JoinSource(js) => {
                Self::collect_table_names_from_source(&js.left, tables);
                Self::collect_table_names_from_source(&js.right, tables);
            }
            Expression::SubquerySource(ss) => {
                // Subquery source has an optional alias
                if let Some(ref alias) = ss.alias {
                    tables.push(alias.value_lower.to_string());
                }
            }
            Expression::FunctionTableSource(fs) => {
                if let Some(ref alias) = fs.alias {
                    tables.push(alias.value_lower.to_string());
                } else {
                    tables.push(fs.function.value_lower.to_string());
                }
            }
            _ => {}
        }
    }

    /// Check if an expression references columns from outer scope.
    ///
    /// For simple identifiers, we cannot reliably determine if they reference outer columns
    /// since the same column name might exist in both inner and outer scopes. The inner
    /// scope takes precedence per SQL semantics, so simple identifiers are NOT considered
    /// outer references (they will resolve to inner scope if available).
    ///
    /// For qualified identifiers (e.g., c.id), we check if the qualifier (table/alias)
    /// is NOT defined in the subquery's FROM clause - if so, it must be an outer reference.
    #[allow(dead_code)]
    pub(super) fn references_outer_columns(expr: &Expression, subquery_tables: &[String]) -> bool {
        match expr {
            Expression::Identifier(_id) => {
                // Simple identifiers are ambiguous - they resolve to inner scope first per SQL semantics.
                // We cannot determine if this is an outer reference without knowing the inner schema.
                // Conservative approach: don't mark as correlated based on simple identifiers alone.
                // Users should use qualified names (e.g., c.id) for outer references in correlated subqueries.
                false
            }
            Expression::QualifiedIdentifier(qid) => {
                // Qualified identifier like "c.id" or "outer_table.column"
                let table_name = &qid.qualifier.value_lower;

                // If the table/alias is NOT in subquery tables, it's an outer reference
                // This is the key check: if "c" is not in ["orders", "o"], then c.id is outer
                !subquery_tables
                    .iter()
                    .any(|t| t.eq_ignore_ascii_case(table_name))
            }
            Expression::Infix(infix) => {
                Self::references_outer_columns(&infix.left, subquery_tables)
                    || Self::references_outer_columns(&infix.right, subquery_tables)
            }
            Expression::Prefix(prefix) => {
                Self::references_outer_columns(&prefix.right, subquery_tables)
            }
            Expression::FunctionCall(func) => func
                .arguments
                .iter()
                .any(|arg| Self::references_outer_columns(arg, subquery_tables)),
            Expression::In(in_expr) => {
                Self::references_outer_columns(&in_expr.left, subquery_tables)
                    || Self::references_outer_columns(&in_expr.right, subquery_tables)
            }
            Expression::Between(between) => {
                Self::references_outer_columns(&between.expr, subquery_tables)
                    || Self::references_outer_columns(&between.lower, subquery_tables)
                    || Self::references_outer_columns(&between.upper, subquery_tables)
            }
            Expression::Case(case) => {
                if let Some(ref value) = case.value {
                    if Self::references_outer_columns(value, subquery_tables) {
                        return true;
                    }
                }
                for when in &case.when_clauses {
                    if Self::references_outer_columns(&when.condition, subquery_tables)
                        || Self::references_outer_columns(&when.then_result, subquery_tables)
                    {
                        return true;
                    }
                }
                if let Some(ref else_val) = case.else_value {
                    if Self::references_outer_columns(else_val, subquery_tables) {
                        return true;
                    }
                }
                false
            }
            Expression::Aliased(aliased) => {
                Self::references_outer_columns(&aliased.expression, subquery_tables)
            }
            Expression::Cast(cast) => Self::references_outer_columns(&cast.expr, subquery_tables),
            _ => false,
        }
    }

    /// Process WHERE clause with correlated subqueries for a specific outer row.
    /// This evaluates correlated subqueries using the outer row context.
    pub(super) fn process_correlated_where(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        // Correlation changes only the execution context, not the AST edges that
        // must be traversed. Reuse the exhaustive subquery rewriter so ALL/ANY,
        // LIKE, function modifiers, windows and later expression variants cannot
        // diverge from the uncorrelated path again.
        self.process_where_subqueries(expr, ctx)
    }

    // ============================================================================
    // Semi-Join Optimization Methods
    // ============================================================================

    /// Try to extract semi-join information from a correlated EXISTS subquery.
    ///
    /// For semi-join optimization, we need:
    /// 1. A simple table source (no joins in subquery)
    /// 2. A WHERE clause with `inner.col = outer.col` equality
    /// 3. Optional additional non-correlated predicates
    ///
    /// Returns None if the subquery cannot be optimized as a semi-join.
    pub fn try_extract_semi_join_info(
        exists: &ExistsExpression,
        is_negated: bool,
        outer_tables: &[String],
    ) -> Option<SemiJoinInfo> {
        let subquery = &exists.subquery;

        // 1. Check for simple table source (not a join)
        let (inner_table, inner_alias): (String, Option<String>) =
            match subquery.table_expr.as_ref().map(|b| b.as_ref()) {
                Some(Expression::TableSource(ts)) => {
                    let alias = ts.alias.as_ref().map(|a| a.value.to_string());
                    (ts.name.value.to_string(), alias)
                }
                _ => return None, // Can't optimize subquery joins or derived tables
            };

        // 2. Parse WHERE clause to find correlation condition
        let where_clause = subquery.where_clause.as_ref()?;

        // Get inner table identifiers for distinguishing inner vs outer references
        let inner_table_lower: String = inner_alias
            .clone()
            .unwrap_or_else(|| inner_table.to_lowercase());
        let inner_tables = vec![inner_table_lower.to_lowercase()];

        // Try to extract: outer.col = inner.col (or inner.col = outer.col)
        let extraction =
            Self::extract_equality_correlation(where_clause, outer_tables, &inner_tables);
        let (outer_col, outer_tbl, inner_col, remaining) = extraction?;

        // IMPORTANT: Check if the remaining predicates reference outer tables.
        // If they do, we cannot use semi-join optimization because those predicates
        // cannot be evaluated on the inner table alone.
        // Example: WHERE o.customer_id = c.id AND c.country = 'USA'
        // The "c.country = 'USA'" references outer table and can't be pushed to inner query.
        if let Some(ref rem) = remaining {
            if Self::expression_references_outer_tables(rem.as_ref(), outer_tables, &inner_tables) {
                return None;
            }
            convert_ast_to_storage_expr(rem.as_ref())?;
        }

        Some(SemiJoinInfo {
            outer_column: outer_col,
            outer_table: outer_tbl,
            inner_column: inner_col,
            inner_table,
            inner_alias,
            non_correlated_where: remaining,
            is_negated,
        })
    }

    /// Extract an equality correlation from a WHERE clause.
    ///
    /// Looks for patterns like:
    /// - `o.user_id = u.id` → inner_col="user_id", outer_col="id", outer_table="u"
    /// - `o.user_id = u.id AND o.amount > 500` → same, with remaining predicate
    ///
    /// Returns: (outer_column, outer_table, inner_column, remaining_predicates)
    /// Uses Arc<Expression> for remaining predicates to avoid cloning expression trees.
    pub(super) fn extract_equality_correlation(
        expr: &Expression,
        outer_tables: &[String],
        inner_tables: &[String],
    ) -> Option<CorrelationExtraction> {
        match expr {
            // Direct equality: inner.col = outer.col
            Expression::Infix(infix) if infix.operator == "=" => Self::try_extract_equality_pair(
                &infix.left,
                &infix.right,
                outer_tables,
                inner_tables,
            )
            .map(|(outer_col, outer_tbl, inner_col)| (outer_col, outer_tbl, inner_col, None)),

            // AND expression: look for equality in one branch
            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("AND") => {
                // Try left side first
                if let Some((outer_col, outer_tbl, inner_col, left_remaining)) =
                    Self::extract_equality_correlation(&infix.left, outer_tables, inner_tables)
                {
                    // Combine remaining from left with right (Arc avoids later clones)
                    let remaining = Self::combine_and_predicates_arc(left_remaining, &infix.right);
                    return Some((outer_col, outer_tbl, inner_col, remaining));
                }

                // Try right side
                if let Some((outer_col, outer_tbl, inner_col, right_remaining)) =
                    Self::extract_equality_correlation(&infix.right, outer_tables, inner_tables)
                {
                    // Combine left with remaining from right (Arc avoids later clones)
                    let remaining = Self::combine_and_predicates_arc(right_remaining, &infix.left);
                    return Some((outer_col, outer_tbl, inner_col, remaining));
                }

                None
            }

            _ => None,
        }
    }

    /// Try to extract an equality pair from two expressions.
    /// One should reference outer table, one should reference inner table.
    pub(super) fn try_extract_equality_pair(
        left: &Expression,
        right: &Expression,
        outer_tables: &[String],
        inner_tables: &[String],
    ) -> Option<(String, Option<String>, String)> {
        // Try left=outer, right=inner
        if let (Some((outer_col, outer_tbl)), Some(inner_col)) = (
            Self::extract_outer_column(left, outer_tables, inner_tables),
            Self::extract_inner_column(right, inner_tables),
        ) {
            return Some((outer_col, outer_tbl, inner_col));
        }

        // Try left=inner, right=outer
        if let (Some(inner_col), Some((outer_col, outer_tbl))) = (
            Self::extract_inner_column(left, inner_tables),
            Self::extract_outer_column(right, outer_tables, inner_tables),
        ) {
            return Some((outer_col, outer_tbl, inner_col));
        }

        None
    }

    /// Extract column name if expression references outer table.
    /// Returns (column_name, table_alias) where table_alias may be None.
    pub(super) fn extract_outer_column(
        expr: &Expression,
        outer_tables: &[String],
        inner_tables: &[String],
    ) -> Option<(String, Option<String>)> {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                // Use pre-computed value_lower to avoid allocation
                let table = qid.qualifier.value_lower.as_str();
                // Must be in outer tables and NOT in inner tables
                if outer_tables.iter().any(|t| t.eq_ignore_ascii_case(table))
                    && !inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table))
                {
                    Some((
                        qid.name.value.to_string(),
                        Some(qid.qualifier.value.to_string()),
                    ))
                } else {
                    None
                }
            }
            // Simple identifier could be outer if not inner table column
            // But we can't reliably determine this without schema info
            _ => None,
        }
    }

    /// Extract column name if expression references inner table.
    pub(super) fn extract_inner_column(
        expr: &Expression,
        inner_tables: &[String],
    ) -> Option<String> {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                // Use pre-computed value_lower to avoid allocation
                let table = qid.qualifier.value_lower.as_str();
                if inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
                    Some(qid.name.value.to_string())
                } else {
                    None
                }
            }
            Expression::Identifier(id) => {
                // Unqualified identifier - assume it's inner table column
                // This is safe because outer refs should be qualified in correlated subqueries
                Some(id.value.to_string())
            }
            _ => None,
        }
    }

    /// Check if an expression references any outer tables.
    /// Used to determine if a predicate can be pushed to the inner query in semi-join optimization.
    pub(super) fn expression_references_outer_tables(
        expr: &Expression,
        outer_tables: &[String],
        inner_tables: &[String],
    ) -> bool {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                // Use pre-computed value_lower to avoid allocation
                let table = &qid.qualifier.value_lower;
                // References outer if it's in outer_tables and NOT in inner_tables
                outer_tables.iter().any(|t| t.eq_ignore_ascii_case(table))
                    && !inner_tables.iter().any(|t| t.eq_ignore_ascii_case(table))
            }
            Expression::Infix(infix) => {
                Self::expression_references_outer_tables(&infix.left, outer_tables, inner_tables)
                    || Self::expression_references_outer_tables(
                        &infix.right,
                        outer_tables,
                        inner_tables,
                    )
            }
            Expression::Prefix(prefix) => {
                Self::expression_references_outer_tables(&prefix.right, outer_tables, inner_tables)
            }
            Expression::FunctionCall(func) => func.arguments.iter().any(|arg| {
                Self::expression_references_outer_tables(arg, outer_tables, inner_tables)
            }),
            Expression::In(in_expr) => {
                Self::expression_references_outer_tables(&in_expr.left, outer_tables, inner_tables)
                    || Self::expression_references_outer_tables(
                        &in_expr.right,
                        outer_tables,
                        inner_tables,
                    )
            }
            Expression::Between(between) => {
                Self::expression_references_outer_tables(&between.expr, outer_tables, inner_tables)
                    || Self::expression_references_outer_tables(
                        &between.lower,
                        outer_tables,
                        inner_tables,
                    )
                    || Self::expression_references_outer_tables(
                        &between.upper,
                        outer_tables,
                        inner_tables,
                    )
            }
            Expression::Case(case) => {
                case.value.as_ref().is_some_and(|op| {
                    Self::expression_references_outer_tables(
                        op.as_ref(),
                        outer_tables,
                        inner_tables,
                    )
                }) || case.when_clauses.iter().any(|wc| {
                    Self::expression_references_outer_tables(
                        &wc.condition,
                        outer_tables,
                        inner_tables,
                    ) || Self::expression_references_outer_tables(
                        &wc.then_result,
                        outer_tables,
                        inner_tables,
                    )
                }) || case.else_value.as_ref().is_some_and(|el| {
                    Self::expression_references_outer_tables(
                        el.as_ref(),
                        outer_tables,
                        inner_tables,
                    )
                })
            }
            // For subqueries, check if their WHERE clause references outer tables.
            // A nested EXISTS that only references its own scope and the immediate
            // parent (inner_tables) is safe for semi-join. But if it references
            // grandparent scope (outer_tables), semi-join cannot handle it.
            Expression::Exists(exists) => {
                Self::subquery_references_outer_tables(&exists.subquery, outer_tables, inner_tables)
            }
            Expression::ScalarSubquery(sq) => {
                Self::subquery_references_outer_tables(&sq.subquery, outer_tables, inner_tables)
            }
            Expression::AllAny(aa) => {
                Self::subquery_references_outer_tables(&aa.subquery, outer_tables, inner_tables)
            }
            // Literals and other expressions don't reference tables
            _ => false,
        }
    }

    /// Check if a subquery's WHERE clause references any of the outer tables.
    /// This is used to determine if a nested EXISTS/ScalarSubquery can be safely
    /// handled by the semi-join optimization (which only provides inner table context).
    pub(super) fn subquery_references_outer_tables(
        subquery: &SelectStatement,
        outer_tables: &[String],
        inner_tables: &[String],
    ) -> bool {
        if let Some(ref where_clause) = subquery.where_clause {
            // Collect the subquery's own tables to extend inner_tables
            let mut sub_tables: Vec<String> = inner_tables.to_vec();
            Self::collect_table_names_from_source_if_present(&subquery.table_expr, &mut sub_tables);
            // Check if the WHERE references outer tables (grandparent scope)
            if Self::expression_references_outer_tables(where_clause, outer_tables, &sub_tables) {
                return true;
            }
        }
        false
    }

    /// Collect table names from an optional table expression.
    pub(super) fn collect_table_names_from_source_if_present(
        table_expr: &Option<Box<Expression>>,
        tables: &mut Vec<String>,
    ) {
        if let Some(ref expr) = table_expr {
            Self::collect_table_names_from_source(expr.as_ref(), tables);
        }
    }

    /// Combine two optional predicates with AND.
    /// Returns Arc<Expression> to avoid cloning when the result is used multiple times.
    pub(super) fn combine_and_predicates_arc(
        left: Option<Arc<Expression>>,
        right: &Expression,
    ) -> Option<Arc<Expression>> {
        match left {
            None => Some(Arc::new(right.clone())),
            Some(l) => {
                // Unwrap Arc if we're the only owner, otherwise clone
                let left_expr = Arc::try_unwrap(l).unwrap_or_else(|arc| (*arc).clone());
                Some(Arc::new(Expression::Infix(InfixExpression {
                    token: dummy_token_clone(),
                    left: Box::new(left_expr),
                    operator: "AND".into(),
                    op_type: InfixOperator::And,
                    right: Box::new(right.clone()),
                })))
            }
        }
    }
}
