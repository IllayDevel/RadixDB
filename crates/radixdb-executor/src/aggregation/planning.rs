use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Parse aggregate functions from SELECT list
    /// Returns: (aggregations, non_agg_columns, post_agg_expressions)
    pub(super) fn parse_aggregations(
        &self,
        stmt: &SelectStatement,
    ) -> Result<(Vec<SqlAggregateFunction>, Vec<String>)> {
        let mut aggregations = Vec::new();
        let mut non_agg_columns = Vec::new();

        for col_expr in &stmt.columns {
            self.extract_aggregates_from_expr(col_expr, &mut aggregations, &mut non_agg_columns)?;
        }

        // Also extract aggregates from HAVING clause
        // These need to be computed even if they're not in SELECT
        if let Some(ref having) = stmt.having {
            self.extract_aggregates_from_expr(having, &mut aggregations, &mut non_agg_columns)?;
        }

        // Also extract aggregates from ORDER BY clause
        // These need to be computed even if they're not in SELECT (marked as hidden)
        // Record the count before adding ORDER BY aggregates
        let visible_count = aggregations.len();
        for order_expr in &stmt.order_by {
            self.extract_aggregates_from_expr(
                &order_expr.expression,
                &mut aggregations,
                &mut non_agg_columns,
            )?;
        }
        // Mark any new aggregates (from ORDER BY) as hidden, but only if they're truly new
        // Helper to create a signature string for an aggregate including its filter
        let make_sig = |agg: &SqlAggregateFunction| -> (String, String, bool, String) {
            let filter_sig = agg
                .filter
                .as_ref()
                .map(|f| format!("{:?}", f))
                .unwrap_or_default();
            (
                agg.name.to_uppercase(),
                agg.column.to_lowercase(),
                agg.distinct,
                filter_sig,
            )
        };

        // Check by comparing the expression signature to avoid duplicates
        for i in visible_count..aggregations.len() {
            // Check if this aggregate already exists in the visible portion
            let new_sig = make_sig(&aggregations[i]);
            let already_exists = aggregations[..visible_count]
                .iter()
                .any(|existing| make_sig(existing) == new_sig);
            if !already_exists {
                aggregations[i].hidden = true;
            }
        }
        // Remove duplicates (aggregates that exist in both SELECT and ORDER BY)
        // Include the filter in the signature so aggregates with different filters are kept
        let mut seen: FxHashSet<(String, String, bool, String)> = FxHashSet::default();
        aggregations.retain(|agg| seen.insert(make_sig(agg)));

        Ok((aggregations, non_agg_columns))
    }

    /// Extract aggregate functions from an expression (recursively)
    pub(super) fn extract_aggregates_from_expr(
        &self,
        expr: &Expression,
        aggregations: &mut Vec<SqlAggregateFunction>,
        non_agg_columns: &mut Vec<String>,
    ) -> Result<()> {
        match expr {
            Expression::FunctionCall(func) => {
                if is_aggregate_function(&func.function) {
                    if let Some(info) = self
                        .host
                        .aggregation_function_registry()
                        .get_info(&func.function)
                    {
                        info.signature.validate_arg_count(func.arguments.len())?;
                    }
                    // Check for nested aggregates - this is invalid SQL
                    // e.g., SUM(COUNT(*)) or AVG(SUM(x)) should return an error
                    for arg in &func.arguments {
                        if expression_contains_aggregate(arg) {
                            return Err(radixdb_core::Error::InvalidArgument(format!(
                                "aggregate function calls cannot be nested: {}",
                                func.function
                            )));
                        }
                    }

                    let (column, distinct, extra_args, expression) =
                        self.extract_agg_column(&func.arguments)?;
                    let column_lower = column.to_lowercase();
                    aggregations.push(SqlAggregateFunction {
                        name: func.function.to_string(),
                        column,
                        column_lower,
                        alias: None,
                        distinct: distinct || func.is_distinct,
                        extra_args,
                        expression,
                        order_by: func.order_by.clone(),
                        filter: func.filter.as_ref().map(|f| (**f).clone()),
                        hidden: false,
                    });
                } else {
                    // Non-aggregate function: recursively check arguments for nested aggregates
                    // e.g., COALESCE(SUM(val), 0), ABS(SUM(val)), etc.
                    for arg in &func.arguments {
                        self.extract_aggregates_from_expr(arg, aggregations, non_agg_columns)?;
                    }
                }
            }
            Expression::Aliased(aliased) => {
                // For aliased expressions, extract aggregates from the inner expression
                self.extract_aggregates_from_aliased(aliased, aggregations, non_agg_columns)?;
            }
            Expression::Identifier(id) => {
                non_agg_columns.push(id.value.to_string());
            }
            Expression::Case(case) => {
                // Extract aggregates from CASE expression
                for when_clause in &case.when_clauses {
                    self.extract_aggregates_from_expr(
                        &when_clause.condition,
                        aggregations,
                        non_agg_columns,
                    )?;
                    self.extract_aggregates_from_expr(
                        &when_clause.then_result,
                        aggregations,
                        non_agg_columns,
                    )?;
                }
                if let Some(ref else_val) = case.else_value {
                    self.extract_aggregates_from_expr(else_val, aggregations, non_agg_columns)?;
                }
            }
            Expression::Infix(infix) => {
                self.extract_aggregates_from_expr(&infix.left, aggregations, non_agg_columns)?;
                self.extract_aggregates_from_expr(&infix.right, aggregations, non_agg_columns)?;
            }
            Expression::Prefix(prefix) => {
                self.extract_aggregates_from_expr(&prefix.right, aggregations, non_agg_columns)?;
            }
            Expression::Cast(cast) => {
                self.extract_aggregates_from_expr(&cast.expr, aggregations, non_agg_columns)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Extract aggregates from an aliased expression
    pub(super) fn extract_aggregates_from_aliased(
        &self,
        aliased: &radixdb_sql::ast::AliasedExpression,
        aggregations: &mut Vec<SqlAggregateFunction>,
        non_agg_columns: &mut Vec<String>,
    ) -> Result<()> {
        match aliased.expression.as_ref() {
            Expression::FunctionCall(func) => {
                if is_aggregate_function(&func.function) {
                    if let Some(info) = self
                        .host
                        .aggregation_function_registry()
                        .get_info(&func.function)
                    {
                        info.signature.validate_arg_count(func.arguments.len())?;
                    }
                    // Check for nested aggregates - this is invalid SQL
                    // e.g., SUM(COUNT(*)) AS total should return an error
                    for arg in &func.arguments {
                        if expression_contains_aggregate(arg) {
                            return Err(radixdb_core::Error::InvalidArgument(format!(
                                "aggregate function calls cannot be nested: {}",
                                func.function
                            )));
                        }
                    }

                    let (column, distinct, extra_args, expression) =
                        self.extract_agg_column(&func.arguments)?;
                    let column_lower = column.to_lowercase();
                    aggregations.push(SqlAggregateFunction {
                        name: func.function.to_string(),
                        column,
                        column_lower,
                        alias: Some(aliased.alias.value.to_string()),
                        distinct: distinct || func.is_distinct,
                        extra_args,
                        expression,
                        order_by: func.order_by.clone(),
                        filter: func.filter.as_ref().map(|f| (**f).clone()),
                        hidden: false,
                    });
                } else {
                    // Non-aggregate function: recursively check arguments for nested aggregates
                    // e.g., COALESCE(SUM(val), 0) AS total, ABS(SUM(val)) AS abs_sum
                    for arg in &func.arguments {
                        self.extract_aggregates_from_expr(arg, aggregations, non_agg_columns)?;
                    }
                }
            }
            Expression::Case(case) => {
                // Extract aggregates from CASE, but keep the alias as the column name
                for when_clause in &case.when_clauses {
                    self.extract_aggregates_from_expr(
                        &when_clause.condition,
                        aggregations,
                        non_agg_columns,
                    )?;
                    self.extract_aggregates_from_expr(
                        &when_clause.then_result,
                        aggregations,
                        non_agg_columns,
                    )?;
                }
                if let Some(ref else_val) = case.else_value {
                    self.extract_aggregates_from_expr(else_val, aggregations, non_agg_columns)?;
                }
            }
            Expression::Cast(cast) => {
                // Extract aggregates from CAST expression (e.g., CAST(SUM(val) AS TEXT) AS sum_text)
                self.extract_aggregates_from_expr(&cast.expr, aggregations, non_agg_columns)?;
            }
            _ => {
                self.extract_aggregates_from_expr(
                    &aliased.expression,
                    aggregations,
                    non_agg_columns,
                )?;
            }
        }
        Ok(())
    }

    /// Extract column name, expression, and extra arguments from aggregate function arguments
    ///
    /// Returns: (column_name, distinct, extra_args, expression)
    /// - column_name: The column or expression string the aggregate operates on (* for COUNT(*))
    /// - distinct: Whether DISTINCT was found in arguments
    /// - extra_args: Additional arguments (e.g., separator for STRING_AGG)
    /// - expression: The expression to evaluate (Some for complex expressions like val * 2)
    pub(super) fn extract_agg_column(
        &self,
        args: &[Expression],
    ) -> Result<(String, bool, Vec<Value>, Option<Expression>)> {
        if args.is_empty() {
            return Ok(("*".to_string(), false, Vec::new(), None));
        }

        let (column, distinct, expression) = match &args[0] {
            Expression::Star(_) => ("*".to_string(), false, None),
            Expression::Identifier(id) => (id.value.to_string(), false, None),
            Expression::QualifiedIdentifier(qid) => {
                // Use full qualified name (e.g., "p.price" instead of just "price")
                // This is needed for JOIN queries where columns are qualified with table aliases
                let qualified_name = format!("{}.{}", qid.qualifier.value, qid.name.value);
                (qualified_name, false, None)
            }
            // For expressions like val * 2, a + b, etc. - store the expression
            expr => {
                let expr_str = self.expression_to_string(expr);
                (expr_str, false, Some(expr.clone()))
            }
        };

        // Extract extra arguments (starting from index 1)
        let mut extra_args = Vec::new();
        for arg in args.iter().skip(1) {
            match arg {
                Expression::StringLiteral(lit) => {
                    extra_args.push(Value::text(lit.value.as_str()));
                }
                Expression::IntegerLiteral(lit) => {
                    extra_args.push(Value::Integer(lit.value));
                }
                Expression::FloatLiteral(lit) => {
                    extra_args.push(Value::Float(lit.value));
                }
                Expression::BooleanLiteral(b) => {
                    extra_args.push(Value::Boolean(b.value));
                }
                Expression::NullLiteral(_) => {
                    extra_args.push(Value::null_unknown());
                }
                Expression::Identifier(id) if id.token.quoted => {
                    extra_args.push(Value::text(id.value.as_str()));
                }
                _ => {}
            }
        }

        Ok((column, distinct, extra_args, expression))
    }

    /// Parse GROUP BY clause
    pub(super) fn parse_group_by(
        &self,
        stmt: &SelectStatement,
        _base_columns: &[String],
    ) -> Result<Vec<GroupByItem>> {
        let mut group_items = Vec::new();

        // Build a map of aliases to their expressions from SELECT clause
        let alias_map: FxHashMap<String, Expression> = stmt
            .columns
            .iter()
            .filter_map(|col| {
                if let Expression::Aliased(aliased) = col {
                    Some((
                        aliased.alias.value_lower.to_string(),
                        (*aliased.expression).clone(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        // For GROUPING SETS, extract all unique columns from all sets
        let columns_to_parse: Vec<&Expression> =
            if let GroupByModifier::GroupingSets(ref sets) = stmt.group_by.modifier {
                // Collect all unique columns from all grouping sets
                // Use canonical key for uniqueness (handles case-insensitivity and structural matching)
                let mut seen = FxHashSet::default();
                let mut unique_cols = Vec::new();
                for set in sets {
                    for expr in set {
                        let key = expression_canonical_key(expr);
                        if seen.insert(key) {
                            unique_cols.push(expr);
                        }
                    }
                }
                unique_cols
            } else {
                // Regular GROUP BY, ROLLUP, or CUBE - use columns directly
                stmt.group_by.columns.iter().collect()
            };

        for expr in columns_to_parse {
            match expr {
                Expression::Identifier(id) => {
                    // Check if this identifier is an alias defined in SELECT
                    let id_lower: &str = id.value_lower.as_str();
                    if let Some(aliased_expr) = alias_map.get(id_lower) {
                        // Use the aliased expression, with the alias as the display name
                        group_items.push(GroupByItem::Expression {
                            expr: aliased_expr.clone(),
                            display_name: id.value.to_string(),
                        });
                    } else {
                        // Regular column reference
                        group_items.push(GroupByItem::Column(id.value.to_string()));
                    }
                }
                Expression::QualifiedIdentifier(qid) => {
                    // Use full qualified name (e.g., "c.name" instead of just "name")
                    let qualified_name = format!("{}.{}", qid.qualifier.value, qid.name.value);
                    group_items.push(GroupByItem::Column(qualified_name));
                }
                Expression::IntegerLiteral(lit) => {
                    // GROUP BY 1 refers to first SELECT column (1-indexed)
                    let pos = lit.value as usize;
                    if pos > 0 && pos <= stmt.columns.len() {
                        // Convert position to the actual SELECT column expression
                        let select_col = &stmt.columns[pos - 1];
                        match select_col {
                            Expression::Identifier(id) => {
                                // Simple column reference - use the column name
                                group_items.push(GroupByItem::Column(id.value.to_string()));
                            }
                            Expression::Aliased(aliased) => {
                                // Aliased expression - extract the underlying expression
                                match aliased.expression.as_ref() {
                                    Expression::Identifier(id) => {
                                        // Aliased column reference
                                        group_items.push(GroupByItem::Column(id.value.to_string()));
                                    }
                                    expr => {
                                        // Complex expression with alias
                                        group_items.push(GroupByItem::Expression {
                                            expr: expr.clone(),
                                            display_name: aliased.alias.value.to_string(),
                                        });
                                    }
                                }
                            }
                            expr => {
                                // Other expressions (e.g., function calls)
                                let display_name = self.find_expression_alias(stmt, expr);
                                group_items.push(GroupByItem::Expression {
                                    expr: expr.clone(),
                                    display_name,
                                });
                            }
                        }
                    } else {
                        // Invalid position, fall back to storing position
                        group_items.push(GroupByItem::Position(pos));
                    }
                }
                Expression::FunctionCall(_) => {
                    // For function expressions, find a matching alias in SELECT
                    let display_name = self.find_expression_alias(stmt, expr);
                    group_items.push(GroupByItem::Expression {
                        expr: expr.clone(),
                        display_name,
                    });
                }
                _ => {
                    // Try to handle other expressions generically
                    let display_name = self.find_expression_alias(stmt, expr);
                    group_items.push(GroupByItem::Expression {
                        expr: expr.clone(),
                        display_name,
                    });
                }
            }
        }

        Ok(group_items)
    }

    /// Find the alias for an expression in the SELECT list
    pub(super) fn find_expression_alias(
        &self,
        stmt: &SelectStatement,
        target_expr: &Expression,
    ) -> String {
        // Check if this expression has an alias in SELECT
        for col_expr in &stmt.columns {
            if let Expression::Aliased(aliased) = col_expr {
                // Compare expressions by converting to canonical string representation
                let aliased_str = self.expression_to_string(&aliased.expression);
                let target_str = self.expression_to_string(target_expr);
                if aliased_str == target_str {
                    return aliased.alias.value.to_string();
                }
            }
        }
        // No alias found, generate a name from the expression
        self.expression_to_string(target_expr)
    }

    /// Convert an expression to a display string
    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn expression_to_string(&self, expr: &Expression) -> String {
        match expr {
            Expression::FunctionCall(func) => {
                let args: Vec<String> = func
                    .arguments
                    .iter()
                    .map(|a| self.expression_to_string(a))
                    .collect();
                format!("{}({})", func.function, args.join(", "))
            }
            Expression::Identifier(id) => id.value.to_string(),
            Expression::QualifiedIdentifier(qid) => {
                format!("{}.{}", qid.qualifier.value, qid.name.value)
            }
            Expression::StringLiteral(lit) => format!("'{}'", lit.value),
            Expression::IntegerLiteral(lit) => lit.value.to_string(),
            Expression::FloatLiteral(lit) => lit.value.to_string(),
            Expression::BooleanLiteral(lit) => lit.value.to_string(),
            Expression::Case(case) => {
                // Use the Display implementation for CaseExpression
                format!("{}", case)
            }
            Expression::Infix(infix) => {
                format!(
                    "{} {} {}",
                    self.expression_to_string(&infix.left),
                    infix.operator,
                    self.expression_to_string(&infix.right)
                )
            }
            Expression::Prefix(prefix) => {
                format!(
                    "{}{}",
                    prefix.operator,
                    self.expression_to_string(&prefix.right)
                )
            }
            Expression::Cast(cast) => {
                format!(
                    "CAST({} AS {})",
                    self.expression_to_string(&cast.expr),
                    cast.type_name
                )
            }
            // For any other expression type, use the Display trait if implemented
            _ => format!("{}", expr),
        }
    }
}
