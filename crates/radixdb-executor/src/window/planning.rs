use super::*;

impl<'host, H: WindowHost + ?Sized> WindowExecutor<'host, H> {
    /// Parse SELECT list to determine output order and sources
    pub(super) fn parse_select_list_for_window(
        &self,
        stmt: &SelectStatement,
        base_columns: &[String],
        window_functions: &[WindowFunctionInfo],
    ) -> Vec<SelectItem> {
        let col_index_map = build_column_index_map(base_columns);

        let mut items = Vec::new();
        let mut wf_idx = 0;

        for col_expr in &stmt.columns {
            match col_expr {
                Expression::Window(window_expr) => {
                    // Window function - use pre-computed values
                    let wf_name = if wf_idx < window_functions.len() {
                        window_functions[wf_idx].column_name.clone()
                    } else {
                        format!("{}()", window_expr.function.function)
                    };
                    items.push(SelectItem {
                        output_name: wf_name.clone(),
                        // OPTIMIZATION: Store lowercase for O(1) lookup in window_value_map
                        source: SelectItemSource::WindowFunction(wf_name.to_lowercase()),
                    });
                    wf_idx += 1;
                }
                Expression::Aliased(aliased) => {
                    if let Expression::Window(_) = aliased.expression.as_ref() {
                        // Aliased window function
                        let alias = aliased.alias.value.to_string();
                        items.push(SelectItem {
                            output_name: alias.clone(),
                            // OPTIMIZATION: Store lowercase for O(1) lookup in window_value_map
                            source: SelectItemSource::WindowFunction(alias.to_lowercase()),
                        });
                        wf_idx += 1;
                    } else if let Expression::Identifier(id) = aliased.expression.as_ref() {
                        // Aliased column reference - use pre-computed lowercase
                        if let Some(&idx) = col_index_map.get(id.value_lower.as_str()) {
                            items.push(SelectItem {
                                output_name: aliased.alias.value.to_string(),
                                source: SelectItemSource::BaseColumn(idx),
                            });
                        } else {
                            items.push(SelectItem {
                                output_name: aliased.alias.value.to_string(),
                                source: SelectItemSource::Expression(
                                    aliased.expression.as_ref().clone(),
                                ),
                            });
                        }
                    } else {
                        // Collect all embedded window functions
                        let mut embedded_windows = Vec::new();
                        Self::collect_all_windows_in_expression(
                            aliased.expression.as_ref(),
                            &mut embedded_windows,
                        );
                        if !embedded_windows.is_empty() {
                            let mut wf_names = Vec::with_capacity(embedded_windows.len());
                            for _ in &embedded_windows {
                                let name = if wf_idx < window_functions.len() {
                                    window_functions[wf_idx].column_name.to_lowercase()
                                } else {
                                    format!("__wf_{}", wf_idx)
                                };
                                wf_names.push(name);
                                wf_idx += 1;
                            }
                            items.push(SelectItem {
                                output_name: aliased.alias.value.to_string(),
                                source: SelectItemSource::ExpressionWithWindow(
                                    aliased.expression.as_ref().clone(),
                                    wf_names,
                                ),
                            });
                        } else if let Expression::FunctionCall(_) = aliased.expression.as_ref() {
                            // Aliased function call - check if it's an aggregate that's already in base_columns
                            let alias_lower = aliased.alias.value_lower.as_str();
                            if let Some(&idx) = col_index_map.get(alias_lower) {
                                items.push(SelectItem {
                                    output_name: aliased.alias.value.to_string(),
                                    source: SelectItemSource::BaseColumn(idx),
                                });
                            } else {
                                items.push(SelectItem {
                                    output_name: aliased.alias.value.to_string(),
                                    source: SelectItemSource::Expression(
                                        aliased.expression.as_ref().clone(),
                                    ),
                                });
                            }
                        } else {
                            items.push(SelectItem {
                                output_name: aliased.alias.value.to_string(),
                                source: SelectItemSource::Expression(
                                    aliased.expression.as_ref().clone(),
                                ),
                            });
                        }
                    }
                }
                Expression::Identifier(id) => {
                    // Simple column reference - use pre-computed lowercase
                    if let Some(&idx) = col_index_map.get(id.value_lower.as_str()) {
                        items.push(SelectItem {
                            output_name: id.value.to_string(),
                            source: SelectItemSource::BaseColumn(idx),
                        });
                    } else {
                        // Column not found directly - try to match against qualified columns (e.g., "name" matches "e.name")
                        // This handles JOIN cases where columns have table prefixes
                        let suffix = format!(".{}", id.value_lower);
                        let mut found = false;
                        for (col_name, &idx) in &col_index_map {
                            if col_name.ends_with(&suffix) {
                                items.push(SelectItem {
                                    output_name: id.value.to_string(),
                                    source: SelectItemSource::BaseColumn(idx),
                                });
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            // Still not found - treat as expression and let evaluator handle it
                            items.push(SelectItem {
                                output_name: id.value.to_string(),
                                source: SelectItemSource::Expression(col_expr.clone()),
                            });
                        }
                    }
                }
                Expression::QualifiedIdentifier(qid) => {
                    // Qualified column reference (table.column)
                    // Try full qualified name first (e.g., "c.name" for JOINs)
                    let full_name =
                        format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    if let Some(&idx) = col_index_map.get(&full_name) {
                        items.push(SelectItem {
                            output_name: format!("{}.{}", qid.qualifier.value, qid.name.value),
                            source: SelectItemSource::BaseColumn(idx),
                        });
                    } else if let Some(&idx) = col_index_map.get(qid.name.value_lower.as_str()) {
                        // Fall back to unqualified name
                        items.push(SelectItem {
                            output_name: qid.name.value.to_string(),
                            source: SelectItemSource::BaseColumn(idx),
                        });
                    } else {
                        // Column not found - treat as expression and let evaluator handle it
                        items.push(SelectItem {
                            output_name: format!("{}.{}", qid.qualifier.value, qid.name.value),
                            source: SelectItemSource::Expression(col_expr.clone()),
                        });
                    }
                }
                Expression::Star(_) | Expression::QualifiedStar(_) => {
                    // SELECT * or t.* - include all base columns
                    for (idx, col) in base_columns.iter().enumerate() {
                        items.push(SelectItem {
                            output_name: col.clone(),
                            source: SelectItemSource::BaseColumn(idx),
                        });
                    }
                }
                Expression::FunctionCall(func) => {
                    // First check for embedded window functions (e.g., ABS(ROW_NUMBER() OVER (...)))
                    let mut embedded_windows = Vec::new();
                    Self::collect_all_windows_in_expression(col_expr, &mut embedded_windows);
                    if !embedded_windows.is_empty() {
                        let mut wf_names = Vec::with_capacity(embedded_windows.len());
                        for _ in &embedded_windows {
                            let name = if wf_idx < window_functions.len() {
                                window_functions[wf_idx].column_name.to_lowercase()
                            } else {
                                format!("__wf_{}", wf_idx)
                            };
                            wf_names.push(name);
                            wf_idx += 1;
                        }
                        items.push(SelectItem {
                            output_name: format!("expr_{}", items.len()),
                            source: SelectItemSource::ExpressionWithWindow(
                                col_expr.clone(),
                                wf_names,
                            ),
                        });
                    } else {
                        // Check if this is an aggregate function already in base columns
                        let func_col_name = if func.arguments.is_empty()
                            || matches!(func.arguments.first(), Some(Expression::Star(_)))
                        {
                            format!("{}(*)", func.function)
                        } else if func.arguments.len() == 1 {
                            if let Expression::Identifier(id) = &func.arguments[0] {
                                format!("{}({})", func.function, id.value)
                            } else {
                                format!("{}(expr)", func.function)
                            }
                        } else {
                            format!("{}(...)", func.function)
                        };

                        if let Some(&idx) = col_index_map.get(&func_col_name.to_lowercase()) {
                            items.push(SelectItem {
                                output_name: func_col_name,
                                source: SelectItemSource::BaseColumn(idx),
                            });
                        } else {
                            items.push(SelectItem {
                                output_name: format!("expr_{}", items.len()),
                                source: SelectItemSource::Expression(col_expr.clone()),
                            });
                        }
                    }
                }
                _ => {
                    // Other expressions - collect all embedded window functions
                    let mut embedded_windows = Vec::new();
                    Self::collect_all_windows_in_expression(col_expr, &mut embedded_windows);
                    if !embedded_windows.is_empty() {
                        let mut wf_names = Vec::with_capacity(embedded_windows.len());
                        for _ in &embedded_windows {
                            let name = if wf_idx < window_functions.len() {
                                window_functions[wf_idx].column_name.to_lowercase()
                            } else {
                                format!("__wf_{}", wf_idx)
                            };
                            wf_names.push(name);
                            wf_idx += 1;
                        }
                        items.push(SelectItem {
                            output_name: format!("expr_{}", items.len()),
                            source: SelectItemSource::ExpressionWithWindow(
                                col_expr.clone(),
                                wf_names,
                            ),
                        });
                    } else {
                        items.push(SelectItem {
                            output_name: format!("expr_{}", items.len()),
                            source: SelectItemSource::Expression(col_expr.clone()),
                        });
                    }
                }
            }
        }

        items
    }

    /// Parse window functions from SELECT list.
    /// Collects ALL window functions, including multiple ones nested in a single expression.
    pub(super) fn parse_window_functions(
        &self,
        stmt: &SelectStatement,
        _base_columns: &[String],
    ) -> Result<Vec<WindowFunctionInfo>> {
        let mut window_functions = Vec::new();
        let mut col_idx = 0;

        for col_expr in &stmt.columns {
            match col_expr {
                Expression::Window(window_expr) => {
                    let wf_info =
                        self.extract_window_function_info(window_expr, &stmt.window_defs)?;
                    window_functions.push(wf_info);
                    col_idx += 1;
                }
                Expression::Aliased(aliased) => {
                    if let Expression::Window(window_expr) = aliased.expression.as_ref() {
                        let mut wf_info =
                            self.extract_window_function_info(window_expr, &stmt.window_defs)?;
                        wf_info.column_name = aliased.alias.value.to_string();
                        window_functions.push(wf_info);
                    } else {
                        // Collect ALL window functions in the expression
                        let mut windows = Vec::new();
                        Self::collect_all_windows_in_expression(
                            aliased.expression.as_ref(),
                            &mut windows,
                        );
                        for (i, window_expr) in windows.into_iter().enumerate() {
                            let mut wf_info =
                                self.extract_window_function_info(window_expr, &stmt.window_defs)?;
                            wf_info.column_name = format!("__wf_{}_{}", col_idx, i);
                            window_functions.push(wf_info);
                        }
                    }
                    col_idx += 1;
                }
                _ => {
                    let mut windows = Vec::new();
                    Self::collect_all_windows_in_expression(col_expr, &mut windows);
                    for (i, window_expr) in windows.into_iter().enumerate() {
                        let mut wf_info =
                            self.extract_window_function_info(window_expr, &stmt.window_defs)?;
                        wf_info.column_name = format!("__wf_{}_{}", col_idx, i);
                        window_functions.push(wf_info);
                    }
                    col_idx += 1;
                }
            }
        }

        Ok(window_functions)
    }

    /// Recursively find a window expression inside an expression
    /// Returns the first Window expression found, if any
    /// Collect all window expressions found anywhere in an expression tree (depth-first order).
    pub(super) fn collect_all_windows_in_expression<'a>(
        expr: &'a Expression,
        out: &mut Vec<&'a WindowExpression>,
    ) {
        match expr {
            Expression::Window(w) => out.push(w),
            Expression::Infix(infix) => {
                Self::collect_all_windows_in_expression(&infix.left, out);
                Self::collect_all_windows_in_expression(&infix.right, out);
            }
            Expression::Prefix(prefix) => {
                Self::collect_all_windows_in_expression(&prefix.right, out);
            }
            Expression::FunctionCall(f) => {
                for arg in &f.arguments {
                    Self::collect_all_windows_in_expression(arg, out);
                }
            }
            Expression::Aliased(a) => {
                Self::collect_all_windows_in_expression(&a.expression, out);
            }
            Expression::Cast(cast) => {
                Self::collect_all_windows_in_expression(&cast.expr, out);
            }
            Expression::Distinct(d) => {
                Self::collect_all_windows_in_expression(&d.expr, out);
            }
            Expression::Case(c) => {
                if let Some(v) = &c.value {
                    Self::collect_all_windows_in_expression(v, out);
                }
                for clause in &c.when_clauses {
                    Self::collect_all_windows_in_expression(&clause.condition, out);
                    Self::collect_all_windows_in_expression(&clause.then_result, out);
                }
                if let Some(else_val) = &c.else_value {
                    Self::collect_all_windows_in_expression(else_val, out);
                }
            }
            Expression::Between(b) => {
                Self::collect_all_windows_in_expression(&b.expr, out);
                Self::collect_all_windows_in_expression(&b.lower, out);
                Self::collect_all_windows_in_expression(&b.upper, out);
            }
            Expression::In(i) => {
                Self::collect_all_windows_in_expression(&i.left, out);
                Self::collect_all_windows_in_expression(&i.right, out);
            }
            Expression::Like(l) => {
                Self::collect_all_windows_in_expression(&l.left, out);
                Self::collect_all_windows_in_expression(&l.pattern, out);
                if let Some(esc) = &l.escape {
                    Self::collect_all_windows_in_expression(esc, out);
                }
            }
            Expression::List(l) => {
                for elem in &l.elements {
                    Self::collect_all_windows_in_expression(elem, out);
                }
            }
            Expression::ExpressionList(l) => {
                for elem in &l.expressions {
                    Self::collect_all_windows_in_expression(elem, out);
                }
            }
            _ => {}
        }
    }

    /// Replace window expressions in an expression tree with identifier references.
    /// Each Window node is replaced with a unique placeholder from `wf_names` in
    /// depth-first order (matching `collect_all_windows_in_expression`).
    pub(super) fn replace_windows_with_identifiers(
        expr: &Expression,
        wf_names: &[String],
        counter: &mut usize,
    ) -> Expression {
        use radixdb_sql::token::{Position, Token, TokenType};

        match expr {
            Expression::Window(_) => {
                let name = wf_names
                    .get(*counter)
                    .map_or("__wf_unknown", |s| s.as_str());
                *counter += 1;
                let dummy_token = Token::new(TokenType::Identifier, name, Position::new(0, 0, 0));
                Expression::Identifier(Identifier::new(dummy_token, name.to_string()))
            }
            Expression::Infix(infix) => Expression::Infix(InfixExpression {
                token: infix.token.clone(),
                left: Box::new(Self::replace_windows_with_identifiers(
                    &infix.left,
                    wf_names,
                    counter,
                )),
                operator: infix.operator.clone(),
                op_type: infix.op_type,
                right: Box::new(Self::replace_windows_with_identifiers(
                    &infix.right,
                    wf_names,
                    counter,
                )),
            }),
            Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression {
                token: prefix.token.clone(),
                operator: prefix.operator.clone(),
                op_type: prefix.op_type,
                right: Box::new(Self::replace_windows_with_identifiers(
                    &prefix.right,
                    wf_names,
                    counter,
                )),
            }),
            Expression::FunctionCall(f) => Expression::FunctionCall(Box::new(FunctionCall {
                token: f.token.clone(),
                function: f.function.clone(),
                arguments: f
                    .arguments
                    .iter()
                    .map(|a| Self::replace_windows_with_identifiers(a, wf_names, counter))
                    .collect(),
                is_distinct: f.is_distinct,
                order_by: f.order_by.clone(),
                filter: f.filter.clone(),
            })),
            Expression::Aliased(a) => Expression::Aliased(AliasedExpression {
                token: a.token.clone(),
                expression: Box::new(Self::replace_windows_with_identifiers(
                    &a.expression,
                    wf_names,
                    counter,
                )),
                alias: a.alias.clone(),
            }),
            Expression::Cast(cast) => Expression::Cast(CastExpression {
                token: cast.token.clone(),
                expr: Box::new(Self::replace_windows_with_identifiers(
                    &cast.expr, wf_names, counter,
                )),
                type_name: cast.type_name.clone(),
            }),
            Expression::Distinct(d) => Expression::Distinct(DistinctExpression {
                token: d.token.clone(),
                expr: Box::new(Self::replace_windows_with_identifiers(
                    &d.expr, wf_names, counter,
                )),
            }),
            Expression::Case(case) => {
                let new_value = case.value.as_ref().map(|v| {
                    Box::new(Self::replace_windows_with_identifiers(v, wf_names, counter))
                });
                let new_whens: Vec<WhenClause> = case
                    .when_clauses
                    .iter()
                    .map(|w| WhenClause {
                        token: w.token.clone(),
                        condition: Self::replace_windows_with_identifiers(
                            &w.condition,
                            wf_names,
                            counter,
                        ),
                        then_result: Self::replace_windows_with_identifiers(
                            &w.then_result,
                            wf_names,
                            counter,
                        ),
                    })
                    .collect();
                let new_else = case.else_value.as_ref().map(|e| {
                    Box::new(Self::replace_windows_with_identifiers(e, wf_names, counter))
                });
                Expression::Case(Box::new(CaseExpression {
                    token: case.token.clone(),
                    value: new_value,
                    when_clauses: new_whens,
                    else_value: new_else,
                }))
            }
            Expression::Between(b) => Expression::Between(BetweenExpression {
                token: b.token.clone(),
                expr: Box::new(Self::replace_windows_with_identifiers(
                    &b.expr, wf_names, counter,
                )),
                lower: Box::new(Self::replace_windows_with_identifiers(
                    &b.lower, wf_names, counter,
                )),
                upper: Box::new(Self::replace_windows_with_identifiers(
                    &b.upper, wf_names, counter,
                )),
                not: b.not,
            }),
            Expression::In(i) => Expression::In(InExpression {
                token: i.token.clone(),
                left: Box::new(Self::replace_windows_with_identifiers(
                    &i.left, wf_names, counter,
                )),
                right: Box::new(Self::replace_windows_with_identifiers(
                    &i.right, wf_names, counter,
                )),
                not: i.not,
            }),
            Expression::Like(l) => Expression::Like(LikeExpression {
                token: l.token.clone(),
                left: Box::new(Self::replace_windows_with_identifiers(
                    &l.left, wf_names, counter,
                )),
                pattern: Box::new(Self::replace_windows_with_identifiers(
                    &l.pattern, wf_names, counter,
                )),
                operator: l.operator.clone(),
                escape: l.escape.as_ref().map(|e| {
                    Box::new(Self::replace_windows_with_identifiers(e, wf_names, counter))
                }),
            }),
            Expression::List(l) => Expression::List(Box::new(ListExpression {
                token: l.token.clone(),
                elements: l
                    .elements
                    .iter()
                    .map(|e| Self::replace_windows_with_identifiers(e, wf_names, counter))
                    .collect(),
            })),
            Expression::ExpressionList(l) => Expression::ExpressionList(Box::new(ExpressionList {
                token: l.token.clone(),
                expressions: l
                    .expressions
                    .iter()
                    .map(|e| Self::replace_windows_with_identifiers(e, wf_names, counter))
                    .collect(),
            })),
            _ => expr.clone(),
        }
    }

    /// Extract window function info from WindowExpression
    pub(super) fn extract_window_function_info(
        &self,
        window_expr: &WindowExpression,
        window_defs: &[WindowDefinition],
    ) -> Result<WindowFunctionInfo> {
        let func = &window_expr.function;

        if let Some(info) = self
            .host
            .window_function_registry()
            .get_info(&func.function)
        {
            info.signature.validate_arg_count(func.arguments.len())?;
        }

        // Resolve named window reference if present
        let (partition_by_exprs, order_by, frame) = if let Some(ref win_ref) =
            window_expr.window_ref
        {
            // Look up the named window definition
            let win_def = window_defs
                .iter()
                .find(|wd| wd.name.eq_ignore_ascii_case(win_ref))
                .ok_or_else(|| Error::NotSupported(format!("Unknown window name: {}", win_ref)))?;
            (
                win_def.partition_by.clone(),
                win_def.order_by.clone(),
                win_def.frame.clone(),
            )
        } else {
            (
                window_expr.partition_by.clone(),
                window_expr.order_by.clone(),
                window_expr.frame.clone(),
            )
        };

        // Extract partition by column names (identifiers only, for fast-path)
        // For qualified identifiers (e.g., l.grp), keep the full qualified name
        // to properly match columns from JOINs
        let partition_by: Vec<String> = partition_by_exprs
            .iter()
            .filter_map(|e| match e {
                Expression::Identifier(id) => Some(id.value.to_string()),
                Expression::QualifiedIdentifier(qid) => {
                    Some(format!("{}.{}", qid.qualifier.value, qid.name.value))
                }
                _ => None,
            })
            .collect();

        // Build column name
        let column_name = format!("{}()", func.function);

        // OPTIMIZATION: func.function is already uppercase from parsing
        Ok(WindowFunctionInfo {
            name: func.function.to_string(),
            arguments: func.arguments.clone(),
            partition_by,
            partition_by_exprs: partition_by_exprs.to_vec(),
            order_by,
            frame,
            column_name,
            is_distinct: func.is_distinct,
        })
    }

    /// Create a cache key for ORDER BY expressions using their string representation.
    /// This enables semantic comparison (ignoring token positions) for ORDER BY clause deduplication.
    #[inline]
    pub(super) fn order_by_cache_key(order_by: &[OrderByExpression]) -> String {
        use std::fmt::Write;
        let mut key = String::with_capacity(64);
        for (i, ob) in order_by.iter().enumerate() {
            if i > 0 {
                key.push(',');
            }
            // Use Display trait to get semantic string representation
            let _ = write!(key, "{}", ob);
        }
        key
    }

    /// Check that ALL window functions share the exact same PARTITION BY clause.
    /// Returns false if any window function is unpartitioned (global) or uses a
    /// different partition scheme, because the streaming path builds only one
    /// partition map and computes per-partition subsets.
    pub(super) fn all_partitions_match(window_functions: &[WindowFunctionInfo]) -> bool {
        use std::fmt::Write;
        let mut canonical: Option<String> = None;
        for wf in window_functions {
            // A global (unpartitioned) window must see all rows, so
            // per-partition streaming is unsafe.
            if wf.partition_by_exprs.is_empty() {
                return false;
            }
            let mut key = String::with_capacity(64);
            for (i, expr) in wf.partition_by_exprs.iter().enumerate() {
                if i > 0 {
                    key.push(',');
                }
                let _ = write!(key, "{}", expr);
            }
            match &canonical {
                None => canonical = Some(key),
                Some(c) if c != &key => return false,
                _ => {}
            }
        }
        canonical.is_some()
    }

    /// Build partition map from PARTITION BY expressions.
    /// Handles both simple column references (fast path) and complex expressions like
    /// function calls (e.g., TIME_TRUNC('30m', timestamp)) via MultiExpressionEval.
    pub(super) fn build_partition_map(
        wf_info: &WindowFunctionInfo,
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
    ) -> Result<FxHashMap<PartitionKey, Vec<usize>>> {
        let mut partitions: FxHashMap<PartitionKey, Vec<usize>> = FxHashMap::default();

        // Check if any PARTITION BY expression is complex (not a simple column reference)
        let has_complex_expr = wf_info.partition_by_exprs.iter().any(|e| {
            !matches!(
                e,
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
            )
        });

        if has_complex_expr && !wf_info.partition_by_exprs.is_empty() {
            // Complex path: use MultiExpressionEval to evaluate expressions per row
            let agg_aliases: Vec<(String, usize)> =
                col_index_map.iter().map(|(k, v)| (k.clone(), *v)).collect();

            let eval = MultiExpressionEval::compile_with_aliases(
                &wf_info.partition_by_exprs,
                columns,
                &agg_aliases,
            )
            .map_err(|e| {
                Error::internal(format!("Failed to compile PARTITION BY expression: {}", e))
            })?;
            let mut eval = eval.with_context(ctx);
            for (i, (_, row)) in rows.iter().enumerate() {
                let values = eval.eval_all(row).map_err(|e| {
                    Error::internal(format!("Failed to evaluate PARTITION BY expression: {}", e))
                })?;
                let key: PartitionKey = SmallVec::from_vec(values);
                partitions.entry(key).or_default().push(i);
            }
        } else {
            // Fast path: simple column references
            let partition_indices: SmallVec<[Option<usize>; 4]> = wf_info
                .partition_by
                .iter()
                .map(|part_col| {
                    let lower = part_col.to_lowercase();
                    col_index_map.get(&lower).copied().or_else(|| {
                        if let Some(dot_pos) = lower.rfind('.') {
                            col_index_map.get(&lower[dot_pos + 1..]).copied()
                        } else {
                            None
                        }
                    })
                })
                .collect();

            for (i, (_, row)) in rows.iter().enumerate() {
                let mut key: PartitionKey = SmallVec::with_capacity(partition_indices.len());
                for idx_opt in &partition_indices {
                    let value = if let Some(&idx) = idx_opt.as_ref() {
                        row.get(idx).cloned().unwrap_or_else(Value::null_unknown)
                    } else {
                        Value::null_unknown()
                    };
                    key.push(value);
                }
                partitions.entry(key).or_default().push(i);
            }
        }

        Ok(partitions)
    }
}
