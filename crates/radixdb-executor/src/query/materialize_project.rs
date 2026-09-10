impl Executor {
    /// Materialize a result into a RowVec
    pub(crate) fn materialize_result(mut result: Box<dyn QueryResult>) -> Result<RowVec> {
        // Pre-allocate based on estimate to avoid reallocations
        let mut rows = if let Some(estimate) = result.estimated_count() {
            RowVec::with_capacity(estimate)
        } else {
            RowVec::new()
        };
        let mut row_id = 0i64;
        while result.next() {
            rows.push((row_id, result.take_row()));
            row_id += 1;
        }
        // Surface runtime filter errors (e.g., invalid parameterized REGEXP)
        if let Some(err) = result.last_error() {
            return Err(err);
        }
        Ok(rows)
    }

    /// Materialize a result into an CompactArc<Vec<Row>> for zero-copy sharing with joins
    ///
    /// This method first tries to extract an Arc directly from the result (e.g., from CTE cache),
    /// falling back to iterating and collecting rows if not possible.
    pub(crate) fn materialize_result_arc(
        mut result: Box<dyn QueryResult>,
    ) -> Result<CompactArc<Vec<Row>>> {
        // Try to extract Arc directly (zero-copy path for CTEs)
        if let Some(arc_rows) = result.try_into_arc_rows() {
            return Ok(arc_rows);
        }

        // Pre-allocate based on estimate to avoid reallocations
        let mut rows = if let Some(estimate) = result.estimated_count() {
            Vec::with_capacity(estimate)
        } else {
            Vec::new()
        };
        while result.next() {
            rows.push(result.take_row());
        }
        // Surface runtime filter errors (e.g., invalid parameterized REGEXP)
        if let Some(err) = result.last_error() {
            return Err(err);
        }
        Ok(CompactArc::new(rows))
    }

    /// Project rows by evaluating SELECT expressions
    ///
    /// Optimized to:
    /// 1. Use FxHashMap for O(1) column lookup instead of O(n) position()
    /// 2. Avoid allocating lowercase strings per call
    /// 3. Use direct indexing for simple column references
    pub(crate) fn project_rows(
        &self,
        select_exprs: &[Expression],
        rows: RowVec,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        self.project_rows_with_alias(select_exprs, rows, all_columns, None, ctx, None)
    }

    /// Project rows with optional table alias for correlated subquery support
    ///
    /// If `all_columns_lower` is provided, it will be used directly instead of
    /// computing lowercase column names per-query. Pass pre-cached values from
    /// `schema.column_names_lower_arc()` for optimal performance.
    pub(crate) fn project_rows_with_alias(
        &self,
        select_exprs: &[Expression],
        rows: RowVec,
        all_columns: &[String],
        all_columns_lower: Option<&[String]>,
        ctx: &ExecutionContext,
        table_alias: Option<&str>,
    ) -> Result<RowVec> {
        // Check if this is SELECT * (no projection needed)
        // Note: QualifiedStar (t.*) DOES need projection to filter columns
        if select_exprs.len() == 1 && matches!(&select_exprs[0], Expression::Star(_)) {
            return Ok(rows); // Pass through directly - pooled
        }

        // OPTIMIZATION: Compute lowercase columns once at the start
        // This avoids per-column to_lowercase() calls in loops below
        let computed_lower_early: Vec<String>;
        let columns_lower: &[String] = if let Some(lower) = all_columns_lower {
            lower
        } else {
            computed_lower_early = all_columns.iter().map(|c| c.to_lowercase()).collect();
            &computed_lower_early
        };

        // Handle SELECT t.* - filter to only columns matching the qualifier
        if select_exprs.len() == 1 {
            if let Expression::QualifiedStar(qs) = &select_exprs[0] {
                // Compute lowercase once and reuse
                let qualifier_lower: String = qs.qualifier.to_lowercase().into();
                let prefix_lower_len = qualifier_lower.len() + 1; // "qualifier."

                // Find indices of columns matching the qualifier (use pre-computed lowercase)
                let matching_indices: Vec<usize> = columns_lower
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        c.len() > prefix_lower_len
                            && c.starts_with(&qualifier_lower)
                            && c.as_bytes().get(qualifier_lower.len()) == Some(&b'.')
                    })
                    .map(|(i, _)| i)
                    .collect();

                // If no columns matched the prefix (single-table query), check if
                // the qualifier matches the table alias - if so, include all columns
                let indices_to_use = if matching_indices.is_empty() {
                    if let Some(alias) = table_alias {
                        if alias.eq_ignore_ascii_case(&qualifier_lower) {
                            (0..all_columns.len()).collect()
                        } else {
                            matching_indices
                        }
                    } else {
                        matching_indices
                    }
                } else {
                    matching_indices
                };

                // OPTIMIZATION: If selecting all columns in order (identity), pass through directly
                let is_identity = indices_to_use.len() == all_columns.len()
                    && indices_to_use.iter().enumerate().all(|(i, &idx)| i == idx);

                if is_identity {
                    return Ok(rows); // Pass through directly - no allocation needed
                }

                // Project rows to only include matching columns
                let mut projected = RowVec::with_capacity(rows.len());
                for (id, row) in rows.into_iter() {
                    projected.push((id, row.clone_subset(&indices_to_use)?));
                }
                return Ok(projected);
            }
        }

        // Check if SELECT columns contain correlated scalar subqueries
        let has_correlated_select = Self::has_correlated_select_subqueries(select_exprs);

        // Process scalar subqueries in SELECT columns before evaluation (single-pass)
        // but only if they're NOT correlated (correlated ones must be evaluated per-row)
        let processed = if has_correlated_select {
            None
        } else {
            self.try_process_select_subqueries(select_exprs, ctx)?
        };
        let select_exprs_cow = match &processed {
            Some(p) => std::borrow::Cow::Borrowed(p.as_slice()),
            None => std::borrow::Cow::Borrowed(select_exprs),
        };
        let select_exprs = select_exprs_cow.as_ref();

        // OPTIMIZATION: For fast path check, use linear search to avoid HashMap allocation.
        // Linear search with eq_ignore_ascii_case is faster than HashMap for small N
        // due to cache locality and zero allocation overhead.
        // HashMap will be built lazily only if we need project_rows_optimized.

        // Helper function for column lookup using linear search
        let find_column_index = |name: &str, columns: &[String]| -> Option<usize> {
            // Linear search with case-insensitive comparison
            columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
                .or_else(|| {
                    // Try unqualified match for qualified column names
                    columns.iter().position(|c| {
                        if let Some(dot_idx) = c.rfind('.') {
                            c[dot_idx + 1..].eq_ignore_ascii_case(name)
                        } else {
                            false
                        }
                    })
                })
        };

        // Fast path: Check if all expressions are simple column references
        // If so, we can use direct index-based projection without creating Evaluators
        let mut simple_column_indices: Vec<usize> = Vec::with_capacity(select_exprs.len());
        let mut all_simple = true;

        for expr in select_exprs.iter() {
            match expr {
                Expression::Star(_) => {
                    // Expand all columns
                    for idx in 0..all_columns.len() {
                        simple_column_indices.push(idx);
                    }
                }
                Expression::QualifiedStar(qs) => {
                    // Expand columns for specific table/alias
                    // Compute lowercase once and reuse
                    let qualifier_lower: String = qs.qualifier.to_lowercase().into();
                    let mut found_any = false;
                    // Use pre-computed lowercase columns to avoid per-column to_lowercase()
                    for (idx, col_lower) in columns_lower.iter().enumerate() {
                        // Check if column starts with "qualifier."
                        if col_lower.len() > qualifier_lower.len()
                            && col_lower.starts_with(&qualifier_lower)
                            && col_lower.as_bytes().get(qualifier_lower.len()) == Some(&b'.')
                        {
                            simple_column_indices.push(idx);
                            found_any = true;
                        }
                    }
                    // If no columns matched the prefix (single-table query), check if
                    // the qualifier matches the table alias - if so, include all columns
                    if !found_any {
                        if let Some(alias) = table_alias {
                            if alias.eq_ignore_ascii_case(&qualifier_lower) {
                                for idx in 0..all_columns.len() {
                                    simple_column_indices.push(idx);
                                }
                            }
                        }
                    }
                }
                Expression::Identifier(id) => {
                    // Use linear search for fast path check
                    if let Some(idx) = find_column_index(&id.value_lower, all_columns) {
                        simple_column_indices.push(idx);
                    } else {
                        all_simple = false;
                        break;
                    }
                }
                Expression::QualifiedIdentifier(qid) => {
                    // Try full qualified name first (table.column)
                    let full_name =
                        format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    if let Some(idx) = find_column_index(&full_name, all_columns) {
                        simple_column_indices.push(idx);
                    } else if let Some(idx) = find_column_index(&qid.name.value_lower, all_columns)
                    {
                        simple_column_indices.push(idx);
                    } else {
                        all_simple = false;
                        break;
                    }
                }
                Expression::Aliased(aliased) => {
                    // Check if the inner expression is a simple column reference
                    match &*aliased.expression {
                        Expression::Identifier(id) => {
                            if let Some(idx) = find_column_index(&id.value_lower, all_columns) {
                                simple_column_indices.push(idx);
                            } else {
                                all_simple = false;
                                break;
                            }
                        }
                        Expression::QualifiedIdentifier(qid) => {
                            let full_name =
                                format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                            if let Some(idx) = find_column_index(&full_name, all_columns) {
                                simple_column_indices.push(idx);
                            } else if let Some(idx) =
                                find_column_index(&qid.name.value_lower, all_columns)
                            {
                                simple_column_indices.push(idx);
                            } else {
                                all_simple = false;
                                break;
                            }
                        }
                        _ => {
                            all_simple = false;
                            break;
                        }
                    }
                }
                _ => {
                    all_simple = false;
                    break;
                }
            }
        }

        // Use fast path if all columns are simple references
        // IMPORTANT: Only use take_columns if no index appears twice, otherwise
        // the second take of the same index would get null (value already moved)
        if all_simple && !simple_column_indices.is_empty() {
            // Check for duplicate indices (can happen with ambiguous column names after JOIN)
            let mut seen_indices: FxHashSet<usize> = FxHashSet::default();
            let has_duplicates = simple_column_indices
                .iter()
                .any(|&idx| !seen_indices.insert(idx));

            if !has_duplicates {
                // OPTIMIZATION: Check if this is an identity projection (all columns in order)
                // If so, skip the map entirely - no transformation needed
                let is_identity = simple_column_indices.len() == all_columns.len()
                    && simple_column_indices
                        .iter()
                        .enumerate()
                        .all(|(i, &idx)| i == idx);

                if is_identity {
                    return Ok(rows); // Pass through directly - pooled
                }

                // OPTIMIZATION: Use take_columns to move values instead of cloning
                let mut projected = RowVec::with_capacity(rows.len());
                for (id, row) in rows.into_iter() {
                    projected.push((id, row.take_columns(&simple_column_indices)?));
                }
                return Ok(projected);
            }
            // Fall through to slow path if there are duplicates
        }

        // OPTIMIZATION: Pre-compute table alias lowercase
        let table_alias_lower: Option<String> = table_alias.map(|a| a.to_lowercase());

        // Pre-size HashMap to avoid rehashing (estimate 1.5x for qualified names)
        // Use columns_lower which was computed early in the function
        let mut col_index_map_lower: StringMap<usize> =
            StringMap::with_capacity(all_columns.len() * 3 / 2);
        for (i, lower) in columns_lower.iter().enumerate() {
            col_index_map_lower.insert(lower.clone(), i);
            // Also add unqualified column names for qualified columns
            if let Some(dot_idx) = lower.rfind('.') {
                let column_part = &lower[dot_idx + 1..];
                col_index_map_lower
                    .entry(column_part.to_string())
                    .or_insert(i);
            }
        }

        // For non-correlated queries, use the optimized path with pre-compiled expressions
        if !has_correlated_select {
            return self.project_rows_optimized(
                select_exprs,
                rows,
                all_columns,
                columns_lower,
                &col_index_map_lower,
                table_alias_lower.as_deref(),
                ctx,
            );
        }

        // Correlated subquery path: expressions change per-row, use CompiledEvaluator
        let mut projected = RowVec::with_capacity(rows.len());

        // Create evaluator once and reuse for all rows
        let mut evaluator = CompiledEvaluator::new(&self.function_registry);
        evaluator = evaluator.with_context(ctx);
        evaluator.init_columns(all_columns);

        // OPTIMIZATION: Pre-compute column name mappings outside the loop for correlated subqueries
        let column_keys = ColumnKeyMapping::build_mappings(all_columns, table_alias);

        // OPTIMIZATION: Pre-allocate outer_row_map with capacity and reuse
        // Uses CompactArc<str> keys for zero-cost cloning in the per-row loop
        let base_capacity = all_columns.len() * 2;
        let mut outer_row_map: FxHashMap<CompactArc<str>, Value> = FxHashMap::default();
        outer_row_map.reserve(base_capacity);

        // OPTIMIZATION: Wrap all_columns in CompactArc once, reuse for all rows
        let all_columns_arc: CompactArc<Vec<String>> = CompactArc::new(all_columns.to_vec());

        // OPTIMIZATION: Pre-compute QualifiedStar lowercase qualifiers to avoid per-row to_lowercase()
        // Key: original qualifier string, Value: qualifier_lower (no format! allocation needed)
        let qualified_star_cache: FxHashMap<String, String> = select_exprs
            .iter()
            .filter_map(|expr| {
                if let Expression::QualifiedStar(qs) = expr {
                    Some((
                        qs.qualifier.to_string(),
                        qs.qualifier.to_lowercase().to_string(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        for (id, row) in rows.into_iter() {
            // Use CompactVec directly instead of Vec to avoid Vec->CompactVec conversion
            let mut values: CompactVec<Value> = CompactVec::with_capacity(select_exprs.len());

            evaluator.set_row_array(&row);

            // OPTIMIZATION: Clear and reuse outer_row_map instead of creating new
            outer_row_map.clear();

            // Use pre-computed column mappings
            for mapping in &column_keys {
                if let Some(value) = row.get(mapping.index) {
                    // OPTIMIZATION: Clone value once and reuse for all key insertions
                    let cloned_value = value.clone();

                    // Insert with unqualified part first (if column had a dot)
                    if let Some(ref upart) = mapping.unqualified_part {
                        outer_row_map.insert(upart.clone(), cloned_value.clone());
                    }

                    // Insert with qualified name if available
                    if let Some(ref qname) = mapping.qualified_name {
                        outer_row_map.insert(qname.clone(), cloned_value.clone());
                    }

                    // Insert with lowercase column name (move, no clone)
                    outer_row_map.insert(mapping.col_lower.clone(), cloned_value);
                }
            }

            // Create context with outer row (cheap due to Arc)
            let mut correlated_ctx = ctx.with_outer_row(
                std::mem::take(&mut outer_row_map),
                all_columns_arc.clone(), // Arc clone = cheap
            );

            // Process correlated SELECT expressions for this row
            let processed_exprs: Result<Vec<Expression>> = select_exprs
                .iter()
                .map(|expr| self.process_correlated_expression(expr, &correlated_ctx))
                .collect();

            // Take back the map for reuse
            outer_row_map = correlated_ctx.take_outer_row().unwrap_or_default();

            let exprs_to_eval = processed_exprs?;

            for expr in exprs_to_eval.iter() {
                match expr {
                    Expression::Star(_) => {
                        // Expand all columns
                        for val in row.iter() {
                            values.push(val.clone());
                        }
                    }
                    Expression::QualifiedStar(qs) => {
                        // Expand columns for specific table/alias
                        // Use pre-computed cache to avoid per-row to_lowercase()
                        let qualifier_lower = qualified_star_cache
                            .get(qs.qualifier.as_str())
                            .map(|s| s.as_str())
                            .unwrap_or("");
                        let qualifier_len = qualifier_lower.len();
                        let mut found_any = false;
                        for (idx, col_lower) in columns_lower.iter().enumerate() {
                            // Inline prefix check: "qualifier." without format! allocation
                            if col_lower.len() > qualifier_len
                                && col_lower.starts_with(qualifier_lower)
                                && col_lower.as_bytes()[qualifier_len] == b'.'
                            {
                                if let Some(val) = row.get(idx) {
                                    values.push(val.clone());
                                    found_any = true;
                                }
                            }
                        }
                        if !found_any {
                            if let Some(ref alias_lower) = table_alias_lower {
                                if alias_lower.as_str() == qualifier_lower {
                                    for val in row.iter() {
                                        values.push(val.clone());
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        let value = self.evaluate_select_expr(
                            &mut evaluator,
                            expr,
                            &row,
                            &col_index_map_lower,
                        )?;
                        values.push(value);
                    }
                }
            }

            projected.push((id, Row::from_compact_vec(values)));
        }

        Ok(projected)
    }

    /// Optimized projection for non-correlated queries.
    /// Pre-compiles all expressions once and uses direct VM execution.
    #[allow(clippy::too_many_arguments)]
    fn project_rows_optimized(
        &self,
        select_exprs: &[Expression],
        rows: RowVec,
        all_columns: &[String],
        all_columns_lower: &[String],
        col_index_map_lower: &StringMap<usize>,
        table_alias_lower: Option<&str>,
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        // Analyze expressions and pre-compile complex ones
        // Each element represents how to evaluate each SELECT expression:
        // - SimpleColumn: direct column index lookup
        // - StarExpand: expand all columns
        // - QualifiedStarExpand: expand columns for specific qualifier
        // - Coalesce: inline COALESCE evaluation (no VM overhead)
        // - Compiled: pre-compiled program to execute via VM

        // Argument source for inline function evaluation
        enum ArgSource {
            Column(usize),
            Const(Value),
        }

        // Simple condition for inline CASE: column comparisons
        enum CaseCondition {
            Equals { col_idx: usize, value: Value },
            NotEquals { col_idx: usize, value: Value },
            GreaterThan { col_idx: usize, value: Value },
            GreaterOrEqual { col_idx: usize, value: Value },
            LessThan { col_idx: usize, value: Value },
            LessOrEqual { col_idx: usize, value: Value },
            IsNull { col_idx: usize },
        }

        // A single WHEN branch for inline CASE
        struct CaseBranch {
            condition: CaseCondition,
            result: ArgSource,
        }

        enum ExprAction {
            SimpleColumn(usize),
            StarExpand,
            QualifiedStarExpand {
                qualifier_lower: String,
            },
            /// Inline COALESCE - bypasses VM for 7x speedup
            Coalesce(smallvec::SmallVec<[ArgSource; 4]>),
            /// Inline CASE - bypasses VM for simple equality/null checks
            Case {
                branches: smallvec::SmallVec<[CaseBranch; 4]>,
                else_result: Option<ArgSource>,
            },
            /// Inline string concatenation (||) - bypasses VM
            Concat(smallvec::SmallVec<[ArgSource; 6]>),
            Compiled(SharedProgram),
        }

        // Helper to try building inline COALESCE action (bypasses VM for 7x speedup)
        fn try_build_coalesce_action(
            func: &radixdb_sql::ast::FunctionCall,
            col_index_map_lower: &StringMap<usize>,
        ) -> Option<ExprAction> {
            if !func.function.eq_ignore_ascii_case("COALESCE") {
                return None;
            }
            if func.arguments.is_empty() {
                return None;
            }

            let mut args: smallvec::SmallVec<[ArgSource; 4]> = smallvec::SmallVec::new();
            for arg in &func.arguments {
                match arg {
                    Expression::Identifier(id) => {
                        let idx = *col_index_map_lower.get(id.value_lower.as_str())?;
                        args.push(ArgSource::Column(idx));
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        // Try full name first, then just column name
                        let full_name =
                            format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                        if let Some(&idx) = col_index_map_lower.get(&full_name) {
                            args.push(ArgSource::Column(idx));
                        } else if let Some(&idx) =
                            col_index_map_lower.get(qid.name.value_lower.as_str())
                        {
                            args.push(ArgSource::Column(idx));
                        } else {
                            return None; // Unknown column
                        }
                    }
                    // Handle all literal types
                    Expression::IntegerLiteral(lit) => {
                        args.push(ArgSource::Const(Value::Integer(lit.value)));
                    }
                    Expression::FloatLiteral(lit) => {
                        args.push(ArgSource::Const(Value::Float(lit.value)));
                    }
                    Expression::StringLiteral(lit) => {
                        args.push(ArgSource::Const(Value::Text(lit.value.clone())));
                    }
                    Expression::BooleanLiteral(lit) => {
                        args.push(ArgSource::Const(Value::Boolean(lit.value)));
                    }
                    Expression::NullLiteral(_) => {
                        args.push(ArgSource::Const(Value::null_unknown()));
                    }
                    _ => return None, // Complex arg - fall back to VM
                }
            }
            Some(ExprAction::Coalesce(args))
        }

        // Helper to extract column index from identifier expressions
        fn get_col_idx(expr: &Expression, col_index_map_lower: &StringMap<usize>) -> Option<usize> {
            match expr {
                Expression::Identifier(id) => {
                    col_index_map_lower.get(id.value_lower.as_str()).copied()
                }
                Expression::QualifiedIdentifier(qid) => {
                    let full = format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    col_index_map_lower
                        .get(&full)
                        .or_else(|| col_index_map_lower.get(qid.name.value_lower.as_str()))
                        .copied()
                }
                _ => None,
            }
        }

        // Helper to convert expression to ArgSource (column or literal)
        fn expr_to_arg_source(
            expr: &Expression,
            col_index_map_lower: &StringMap<usize>,
        ) -> Option<ArgSource> {
            match expr {
                Expression::Identifier(id) => {
                    let idx = *col_index_map_lower.get(id.value_lower.as_str())?;
                    Some(ArgSource::Column(idx))
                }
                Expression::QualifiedIdentifier(qid) => {
                    let full = format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    let idx = col_index_map_lower
                        .get(&full)
                        .or_else(|| col_index_map_lower.get(qid.name.value_lower.as_str()))?;
                    Some(ArgSource::Column(*idx))
                }
                Expression::IntegerLiteral(lit) => {
                    Some(ArgSource::Const(Value::Integer(lit.value)))
                }
                Expression::FloatLiteral(lit) => Some(ArgSource::Const(Value::Float(lit.value))),
                Expression::StringLiteral(lit) => {
                    Some(ArgSource::Const(Value::Text(lit.value.clone())))
                }
                Expression::BooleanLiteral(lit) => {
                    Some(ArgSource::Const(Value::Boolean(lit.value)))
                }
                Expression::NullLiteral(_) => Some(ArgSource::Const(Value::null_unknown())),
                _ => None,
            }
        }

        // Helper to convert expression to Value (for condition comparison)
        fn expr_to_value(expr: &Expression) -> Option<Value> {
            match expr {
                Expression::IntegerLiteral(lit) => Some(Value::Integer(lit.value)),
                Expression::FloatLiteral(lit) => Some(Value::Float(lit.value)),
                Expression::StringLiteral(lit) => Some(Value::Text(lit.value.clone())),
                Expression::BooleanLiteral(lit) => Some(Value::Boolean(lit.value)),
                Expression::NullLiteral(_) => Some(Value::null_unknown()),
                _ => None,
            }
        }

        // Helper to try building inline CASE action (bypasses VM)
        fn try_build_case_action(
            case: &radixdb_sql::ast::CaseExpression,
            col_index_map_lower: &StringMap<usize>,
        ) -> Option<ExprAction> {
            // Only handle searched CASE (no value expression)
            if case.value.is_some() {
                return None;
            }

            let mut branches: smallvec::SmallVec<[CaseBranch; 4]> = smallvec::SmallVec::new();

            for when_clause in &case.when_clauses {
                // Parse condition: support comparisons and IS NULL
                let condition = match &when_clause.condition {
                    Expression::Infix(infix) => {
                        // Handle column op literal (e.g., balance > 50000)
                        if let Some(col_idx) = get_col_idx(&infix.left, col_index_map_lower) {
                            if let Some(value) = expr_to_value(&infix.right) {
                                match infix.operator.as_str() {
                                    "=" => CaseCondition::Equals { col_idx, value },
                                    "!=" | "<>" => CaseCondition::NotEquals { col_idx, value },
                                    ">" => CaseCondition::GreaterThan { col_idx, value },
                                    ">=" => CaseCondition::GreaterOrEqual { col_idx, value },
                                    "<" => CaseCondition::LessThan { col_idx, value },
                                    "<=" => CaseCondition::LessOrEqual { col_idx, value },
                                    _ if infix.operator.eq_ignore_ascii_case("IS") => {
                                        if matches!(&*infix.right, Expression::NullLiteral(_)) {
                                            CaseCondition::IsNull { col_idx }
                                        } else {
                                            return None;
                                        }
                                    }
                                    _ => return None,
                                }
                            } else if infix.operator.eq_ignore_ascii_case("IS") {
                                if matches!(&*infix.right, Expression::NullLiteral(_)) {
                                    CaseCondition::IsNull { col_idx }
                                } else {
                                    return None;
                                }
                            } else {
                                return None;
                            }
                        // Handle literal op column (reversed: 50000 < balance)
                        } else {
                            let col_idx = get_col_idx(&infix.right, col_index_map_lower)?;
                            let value = expr_to_value(&infix.left)?;
                            // Reverse the operator since column is on right
                            match infix.operator.as_str() {
                                "=" => CaseCondition::Equals { col_idx, value },
                                "!=" | "<>" => CaseCondition::NotEquals { col_idx, value },
                                ">" => CaseCondition::LessThan { col_idx, value }, // 5 > col means col < 5
                                ">=" => CaseCondition::LessOrEqual { col_idx, value }, // 5 >= col means col <= 5
                                "<" => CaseCondition::GreaterThan { col_idx, value }, // 5 < col means col > 5
                                "<=" => CaseCondition::GreaterOrEqual { col_idx, value }, // 5 <= col means col >= 5
                                _ => return None,
                            }
                        }
                    }
                    _ => return None, // Complex condition - fall back to VM
                };

                // Parse result
                let result = expr_to_arg_source(&when_clause.then_result, col_index_map_lower)?;

                branches.push(CaseBranch { condition, result });
            }

            // Parse ELSE
            let else_result = match &case.else_value {
                Some(expr) => Some(expr_to_arg_source(expr, col_index_map_lower)?),
                None => None,
            };

            Some(ExprAction::Case {
                branches,
                else_result,
            })
        }

        // Helper to flatten nested || concatenations into a list
        fn flatten_concat(
            expr: &Expression,
            col_index_map_lower: &StringMap<usize>,
            parts: &mut smallvec::SmallVec<[ArgSource; 6]>,
        ) -> bool {
            match expr {
                Expression::Infix(infix) if infix.operator == "||" => {
                    // Recursively flatten left and right
                    flatten_concat(&infix.left, col_index_map_lower, parts)
                        && flatten_concat(&infix.right, col_index_map_lower, parts)
                }
                _ => {
                    // Try to convert to ArgSource
                    if let Some(arg) = expr_to_arg_source(expr, col_index_map_lower) {
                        parts.push(arg);
                        true
                    } else {
                        false
                    }
                }
            }
        }

        // Helper to try building inline Concat action (bypasses VM)
        fn try_build_concat_action(
            expr: &Expression,
            col_index_map_lower: &StringMap<usize>,
        ) -> Option<ExprAction> {
            // Only handle || operator
            if let Expression::Infix(infix) = expr {
                if infix.operator == "||" {
                    let mut parts: smallvec::SmallVec<[ArgSource; 6]> = smallvec::SmallVec::new();
                    if flatten_concat(expr, col_index_map_lower, &mut parts) && parts.len() >= 2 {
                        return Some(ExprAction::Concat(parts));
                    }
                }
            }
            None
        }

        // Analyze and pre-compile all expressions ONCE before the row loop
        let mut actions: Vec<ExprAction> = Vec::with_capacity(select_exprs.len());
        let compile_expression = |expression: &Expression, columns: &[String]| {
            compile_expression_with_context(
                expression,
                columns,
                ctx.outer_columns(),
                &self.function_registry,
            )
        };

        for expr in select_exprs.iter() {
            let action = match expr {
                Expression::Star(_) => ExprAction::StarExpand,
                Expression::QualifiedStar(qs) => ExprAction::QualifiedStarExpand {
                    qualifier_lower: qs.qualifier.to_lowercase().to_string(),
                },
                Expression::Identifier(id) => {
                    if let Some(&idx) = col_index_map_lower.get(id.value_lower.as_str()) {
                        ExprAction::SimpleColumn(idx)
                    } else {
                        // Unknown identifier - compile as expression (might be alias)
                        let program = compile_expression(expr, all_columns)?;
                        ExprAction::Compiled(program)
                    }
                }
                Expression::QualifiedIdentifier(qid) => {
                    let full_name =
                        format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    if let Some(&idx) = col_index_map_lower.get(&full_name) {
                        ExprAction::SimpleColumn(idx)
                    } else if let Some(&idx) =
                        col_index_map_lower.get(qid.name.value_lower.as_str())
                    {
                        ExprAction::SimpleColumn(idx)
                    } else {
                        let program = compile_expression(expr, all_columns)?;
                        ExprAction::Compiled(program)
                    }
                }
                Expression::Aliased(aliased) => {
                    // Recurse into the inner expression
                    match &*aliased.expression {
                        Expression::Identifier(id) => {
                            if let Some(&idx) = col_index_map_lower.get(id.value_lower.as_str()) {
                                ExprAction::SimpleColumn(idx)
                            } else {
                                let program = compile_expression(&aliased.expression, all_columns)?;
                                ExprAction::Compiled(program)
                            }
                        }
                        Expression::QualifiedIdentifier(qid) => {
                            let full_name =
                                format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                            if let Some(&idx) = col_index_map_lower.get(&full_name) {
                                ExprAction::SimpleColumn(idx)
                            } else if let Some(&idx) =
                                col_index_map_lower.get(qid.name.value_lower.as_str())
                            {
                                ExprAction::SimpleColumn(idx)
                            } else {
                                let program = compile_expression(&aliased.expression, all_columns)?;
                                ExprAction::Compiled(program)
                            }
                        }
                        Expression::FunctionCall(func) => {
                            // Try inline COALESCE optimization
                            if let Some(action) =
                                try_build_coalesce_action(func, col_index_map_lower)
                            {
                                action
                            } else {
                                let program = compile_expression(&aliased.expression, all_columns)?;
                                ExprAction::Compiled(program)
                            }
                        }
                        Expression::Case(case) => {
                            // Try inline CASE optimization
                            if let Some(action) = try_build_case_action(case, col_index_map_lower) {
                                action
                            } else {
                                let program = compile_expression(&aliased.expression, all_columns)?;
                                ExprAction::Compiled(program)
                            }
                        }
                        Expression::Infix(_) => {
                            // Try inline concat optimization for ||
                            if let Some(action) =
                                try_build_concat_action(&aliased.expression, col_index_map_lower)
                            {
                                action
                            } else {
                                let program = compile_expression(&aliased.expression, all_columns)?;
                                ExprAction::Compiled(program)
                            }
                        }
                        _ => {
                            // Compile the inner expression (not the Aliased wrapper)
                            let program = compile_expression(&aliased.expression, all_columns)?;
                            ExprAction::Compiled(program)
                        }
                    }
                }
                Expression::FunctionCall(func) => {
                    // Try inline COALESCE optimization
                    if let Some(action) = try_build_coalesce_action(func, col_index_map_lower) {
                        action
                    } else {
                        let program = compile_expression(expr, all_columns)?;
                        ExprAction::Compiled(program)
                    }
                }
                Expression::Case(case) => {
                    // Try inline CASE optimization
                    if let Some(action) = try_build_case_action(case, col_index_map_lower) {
                        action
                    } else {
                        let program = compile_expression(expr, all_columns)?;
                        ExprAction::Compiled(program)
                    }
                }
                Expression::Infix(_) => {
                    // Try inline concat optimization for ||
                    if let Some(action) = try_build_concat_action(expr, col_index_map_lower) {
                        action
                    } else {
                        let program = compile_expression(expr, all_columns)?;
                        ExprAction::Compiled(program)
                    }
                }
                _ => {
                    // Complex expression - compile it
                    let program = compile_expression(expr, all_columns)?;
                    ExprAction::Compiled(program)
                }
            };
            actions.push(action);
        }

        // Pre-fetch parameters for VM context
        let params = ctx.params();
        let named_params = ctx.named_params();
        let transaction_id = ctx.transaction_id();

        // Create reusable VM
        let mut vm = ExprVM::new();

        // Check if all actions are SimpleColumn (common fast path)
        let all_simple_columns = actions
            .iter()
            .all(|a| matches!(a, ExprAction::SimpleColumn(_)));

        // Project rows using pre-analyzed actions
        let mut projected = RowVec::with_capacity(rows.len());

        if all_simple_columns {
            // Super fast path: just extract column indices and use take_columns
            let indices: Vec<usize> = actions
                .iter()
                .filter_map(|a| {
                    if let ExprAction::SimpleColumn(idx) = a {
                        Some(*idx)
                    } else {
                        None
                    }
                })
                .collect();

            for (id, row) in rows.into_iter() {
                projected.push((id, row.take_columns(&indices)?));
            }
        } else {
            // General path: mixed actions
            // Pre-compute named_params Option to avoid repeated checks
            let named_params_opt = if named_params.is_empty() {
                None
            } else {
                Some(named_params)
            };

            // Pre-compute capacity outside loop to avoid repeated len() calls
            let values_capacity = actions.len();

            for (id, row) in rows.into_iter() {
                // Use CompactVec directly instead of Vec to avoid Vec->CompactVec conversion
                let mut values: CompactVec<Value> = CompactVec::with_capacity(values_capacity);

                // Build VM context with all params in one call (faster than builder chain)
                let mut vm_ctx = ExecuteContext::with_common_params(
                    &row,
                    params,
                    named_params_opt,
                    transaction_id,
                )
                .with_stored_function_invoker(ctx.stored_function_invoker());
                if let Some(outer_row) = ctx.outer_row() {
                    vm_ctx = vm_ctx.with_outer_row(outer_row);
                }

                for action in &actions {
                    match action {
                        ExprAction::SimpleColumn(idx) => {
                            // Direct index access with bounds check
                            if let Some(v) = row.get(*idx) {
                                values.push(v.clone());
                            } else {
                                values.push(Value::null_unknown());
                            }
                        }
                        ExprAction::StarExpand => {
                            for val in row.iter() {
                                values.push(val.clone());
                            }
                        }
                        ExprAction::QualifiedStarExpand { qualifier_lower } => {
                            let qualifier_len = qualifier_lower.len();
                            let mut found_any = false;
                            for (idx, col_lower) in all_columns_lower.iter().enumerate() {
                                // Inline prefix check: "qualifier." without format! allocation
                                if col_lower.len() > qualifier_len
                                    && col_lower.starts_with(qualifier_lower)
                                    && col_lower.as_bytes()[qualifier_len] == b'.'
                                {
                                    if let Some(val) = row.get(idx) {
                                        values.push(val.clone());
                                        found_any = true;
                                    }
                                }
                            }
                            if !found_any {
                                if let Some(alias_lower) = table_alias_lower {
                                    if alias_lower == qualifier_lower {
                                        for val in row.iter() {
                                            values.push(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                        ExprAction::Coalesce(args) => {
                            // Inline COALESCE: direct loop avoids iterator overhead
                            let mut found = false;
                            for arg in args.iter() {
                                let val: Option<&Value> = match arg {
                                    ArgSource::Column(idx) => row.get(*idx),
                                    ArgSource::Const(v) => Some(v),
                                };
                                if let Some(v) = val {
                                    if !v.is_null() {
                                        values.push(v.clone());
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if !found {
                                values.push(Value::null_unknown());
                            }
                        }
                        ExprAction::Case {
                            branches,
                            else_result,
                        } => {
                            // Inline CASE: direct loop avoids iterator overhead
                            let mut matched = false;
                            for branch in branches.iter() {
                                let col_val: Option<&Value> = match &branch.condition {
                                    CaseCondition::Equals { col_idx, .. }
                                    | CaseCondition::NotEquals { col_idx, .. }
                                    | CaseCondition::GreaterThan { col_idx, .. }
                                    | CaseCondition::GreaterOrEqual { col_idx, .. }
                                    | CaseCondition::LessThan { col_idx, .. }
                                    | CaseCondition::LessOrEqual { col_idx, .. }
                                    | CaseCondition::IsNull { col_idx } => row.get(*col_idx),
                                };
                                let cond_matches = match (&branch.condition, col_val) {
                                    (CaseCondition::Equals { value, .. }, Some(v)) => v == value,
                                    (CaseCondition::NotEquals { value, .. }, Some(v)) => v != value,
                                    (CaseCondition::GreaterThan { value, .. }, Some(v)) => {
                                        v > value
                                    }
                                    (CaseCondition::GreaterOrEqual { value, .. }, Some(v)) => {
                                        v >= value
                                    }
                                    (CaseCondition::LessThan { value, .. }, Some(v)) => v < value,
                                    (CaseCondition::LessOrEqual { value, .. }, Some(v)) => {
                                        v <= value
                                    }
                                    (CaseCondition::IsNull { .. }, Some(v)) => v.is_null(),
                                    (_, None) => false,
                                };
                                if cond_matches {
                                    match &branch.result {
                                        ArgSource::Column(idx) => {
                                            if let Some(v) = row.get(*idx) {
                                                values.push(v.clone());
                                            } else {
                                                values.push(Value::null_unknown());
                                            }
                                        }
                                        ArgSource::Const(v) => values.push(v.clone()),
                                    }
                                    matched = true;
                                    break;
                                }
                            }
                            if !matched {
                                // No branch matched - use ELSE or NULL
                                match else_result {
                                    Some(ArgSource::Column(idx)) => {
                                        if let Some(v) = row.get(*idx) {
                                            values.push(v.clone());
                                        } else {
                                            values.push(Value::null_unknown());
                                        }
                                    }
                                    Some(ArgSource::Const(v)) => values.push(v.clone()),
                                    None => values.push(Value::null_unknown()),
                                }
                            }
                        }
                        ExprAction::Concat(parts) => {
                            // First pass: calculate exact length for text-only (common case)
                            // and check for nulls/non-text values
                            let mut total_len = 0usize;
                            let mut any_null = false;
                            let mut all_text = true;
                            for part in parts.iter() {
                                let val: Option<&Value> = match part {
                                    ArgSource::Column(idx) => row.get(*idx),
                                    ArgSource::Const(v) => Some(v),
                                };
                                match val {
                                    Some(Value::Text(s)) => total_len += s.len(),
                                    Some(Value::Null(_)) | None => {
                                        any_null = true;
                                        break;
                                    }
                                    Some(_) => {
                                        all_text = false;
                                        break;
                                    }
                                }
                            }

                            if any_null {
                                values.push(Value::null_unknown());
                            } else if all_text {
                                // Fast path: all text, exact capacity, no shrink_to_fit realloc
                                let mut result = String::with_capacity(total_len);
                                for part in parts.iter() {
                                    let val: Option<&Value> = match part {
                                        ArgSource::Column(idx) => row.get(*idx),
                                        ArgSource::Const(v) => Some(v),
                                    };
                                    if let Some(Value::Text(s)) = val {
                                        result.push_str(s.as_str());
                                    }
                                }
                                // len == capacity, so into_boxed_str is O(1)
                                values.push(Value::Text(result.into()));
                            } else {
                                // Slow path: mixed types, use Arc to avoid shrink_to_fit
                                let mut result = String::with_capacity(64);
                                for part in parts.iter() {
                                    let val: Option<&Value> = match part {
                                        ArgSource::Column(idx) => row.get(*idx),
                                        ArgSource::Const(v) => Some(v),
                                    };
                                    match val {
                                        Some(Value::Text(s)) => result.push_str(s.as_str()),
                                        Some(Value::Integer(i)) => {
                                            use std::fmt::Write;
                                            let _ = write!(result, "{}", i);
                                        }
                                        Some(Value::Float(f)) => {
                                            use std::fmt::Write;
                                            let _ = write!(result, "{}", f);
                                        }
                                        Some(Value::Boolean(b)) => {
                                            result.push_str(if *b { "true" } else { "false" });
                                        }
                                        Some(v) => result.push_str(&v.to_string()),
                                        None => {}
                                    }
                                }
                                values.push(Value::Text(SmartString::from_string_shared(result)));
                            }
                        }
                        ExprAction::Compiled(program) => {
                            let value = vm.execute_cow(program, &vm_ctx)?;
                            values.push(value);
                        }
                    }
                }

                projected.push((id, Row::from_compact_vec(values)));
            }
        }

        Ok(projected)
    }

    /// Project rows including ORDER BY columns not in SELECT
    /// Returns rows with SELECT columns followed by ORDER BY columns
    fn project_rows_with_order_by(
        &self,
        select_exprs: &[Expression],
        order_by: &[radixdb_sql::ast::OrderByExpression],
        distinct_on: &[Expression],
        mut rows: RowVec,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<(RowVec, Vec<String>)> {
        // Non-correlated scalar subqueries can be resolved once. Correlated
        // subqueries must be evaluated against each source row below.
        let has_correlated_select = Self::has_correlated_select_subqueries(select_exprs);
        let processed = if has_correlated_select {
            None
        } else {
            self.try_process_select_subqueries(select_exprs, ctx)?
        };
        let select_exprs = match &processed {
            Some(p) => std::borrow::Cow::Borrowed(p.as_slice()),
            None => std::borrow::Cow::Borrowed(select_exprs),
        };

        // Build column index map ONCE with FxHashMap for O(1) lookup
        let col_index_map_lower = build_column_index_map(all_columns);

        // Get SELECT column names (lowercase) for checking duplicates
        let select_column_names: Vec<String> = select_exprs
            .iter()
            .flat_map(|expr| {
                let mut names = Vec::new();
                // Add unqualified name
                if let Some(name) = self.extract_select_column_name(expr) {
                    names.push(name.to_lowercase());
                }
                // Also add fully qualified name for disambiguation
                match expr {
                    Expression::QualifiedIdentifier(qi) => {
                        names.push(format!(
                            "{}.{}",
                            qi.qualifier.value_lower, qi.name.value_lower
                        ));
                    }
                    Expression::Aliased(a) => {
                        if let Expression::QualifiedIdentifier(qi) = &*a.expression {
                            names.push(format!(
                                "{}.{}",
                                qi.qualifier.value_lower, qi.name.value_lower
                            ));
                        }
                    }
                    _ => {}
                }
                names
            })
            .collect();

        // Find ORDER BY columns not in SELECT
        // Preserve the SQL binding that requested every hidden key. JOIN source
        // columns may expose the underlying table name while ORDER BY /
        // DISTINCT ON uses an alias (for example `c.name`). Publishing only the
        // physical source name loses that identity and makes two same-named
        // JOIN columns ambiguous during the outer DISTINCT ON step.
        let mut extra_order_columns: Vec<(usize, String)> = Vec::new();
        let mut extra_computed_expressions: Vec<(String, &Expression)> = Vec::new();
        for ob in order_by {
            match &ob.expression {
                Expression::Identifier(id)
                    if !select_column_names
                        .iter()
                        .any(|s| s == id.value_lower.as_str()) =>
                {
                    if let Some(&idx) = col_index_map_lower.get(id.value_lower.as_str()) {
                        if !extra_order_columns
                            .iter()
                            .any(|(existing, _)| *existing == idx)
                        {
                            extra_order_columns
                                .push((idx, expression_binding_name(&ob.expression)));
                        }
                    }
                }
                Expression::QualifiedIdentifier(qi) => {
                    let full_name = format!("{}.{}", qi.qualifier.value_lower, qi.name.value_lower);
                    // Only match against full qualified name in SELECT to avoid ambiguity
                    if !select_column_names.contains(&full_name) {
                        // Prefer fully qualified lookup
                        let idx_opt = col_index_map_lower
                            .get(full_name.as_str())
                            .or_else(|| col_index_map_lower.get(qi.name.value_lower.as_str()));
                        if let Some(&idx) = idx_opt {
                            if !extra_order_columns
                                .iter()
                                .any(|(existing, _)| *existing == idx)
                            {
                                extra_order_columns
                                    .push((idx, expression_binding_name(&ob.expression)));
                            }
                        }
                    }
                }
                _ if !Self::expression_is_selected(&ob.expression, select_exprs.as_ref()) => {
                    let expr_name = expression_binding_name(&ob.expression);
                    if !extra_computed_expressions
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case(&expr_name))
                    {
                        extra_computed_expressions.push((expr_name, &ob.expression));
                    }
                }
                _ => {}
            }
        }

        // Find DISTINCT ON columns not in SELECT and not already in extra ORDER BY columns
        // Also collect computed DISTINCT ON expressions that need evaluation
        for expr in distinct_on {
            match expr {
                Expression::Identifier(id) => {
                    if !select_column_names
                        .iter()
                        .any(|s| s == id.value_lower.as_str())
                    {
                        if let Some(&idx) = col_index_map_lower.get(id.value_lower.as_str()) {
                            if !extra_order_columns
                                .iter()
                                .any(|(existing, _)| *existing == idx)
                            {
                                extra_order_columns.push((idx, expression_binding_name(expr)));
                            }
                        }
                    }
                }
                Expression::QualifiedIdentifier(qi) => {
                    let full_name = format!("{}.{}", qi.qualifier.value_lower, qi.name.value_lower);
                    // For qualified identifiers, only match against the full qualified name
                    // in SELECT to avoid ambiguity (e.g., c.name vs p.name)
                    if !select_column_names.contains(&full_name) {
                        // Prefer fully qualified lookup to avoid binding to wrong column
                        let idx_opt = col_index_map_lower
                            .get(full_name.as_str())
                            .or_else(|| col_index_map_lower.get(qi.name.value_lower.as_str()));
                        if let Some(&idx) = idx_opt {
                            if !extra_order_columns
                                .iter()
                                .any(|(existing, _)| *existing == idx)
                            {
                                extra_order_columns.push((idx, expression_binding_name(expr)));
                            }
                        }
                    }
                }
                _ => {
                    // Computed expression — needs evaluation and appending.
                    // De-duplicate against computed ORDER BY keys so result
                    // shape matches the output column list used by the outer
                    // sorter/DISTINCT ON step.
                    let expr_name = expression_binding_name(expr);
                    if !Self::expression_is_selected(expr, select_exprs.as_ref())
                        && !extra_computed_expressions
                            .iter()
                            .any(|(name, _)| name.eq_ignore_ascii_case(&expr_name))
                    {
                        extra_computed_expressions.push((expr_name, expr));
                    }
                }
            }
        }

        // Build column indices for SELECT expressions
        let mut select_column_indices: Vec<Option<usize>> = Vec::with_capacity(select_exprs.len());
        for expr in select_exprs.iter() {
            match expr {
                Expression::Identifier(id) => {
                    select_column_indices
                        .push(col_index_map_lower.get(id.value_lower.as_str()).copied());
                }
                Expression::QualifiedIdentifier(qid) => {
                    // Prefer full qualified name to handle same-named columns across tables
                    let full = format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                    select_column_indices.push(
                        col_index_map_lower
                            .get(full.as_str())
                            .or_else(|| col_index_map_lower.get(qid.name.value_lower.as_str()))
                            .copied(),
                    );
                }
                Expression::Aliased(aliased) => match &*aliased.expression {
                    Expression::Identifier(id) => {
                        select_column_indices
                            .push(col_index_map_lower.get(id.value_lower.as_str()).copied());
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        let full =
                            format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                        select_column_indices.push(
                            col_index_map_lower
                                .get(full.as_str())
                                .or_else(|| col_index_map_lower.get(qid.name.value_lower.as_str()))
                                .copied(),
                        );
                    }
                    _ => select_column_indices.push(None),
                },
                _ => select_column_indices.push(None),
            }
        }

        // Check if we can use fast path (all simple column refs, no computed extra keys)
        let all_simple = select_column_indices.iter().all(|idx| idx.is_some())
            && extra_computed_expressions.is_empty();

        if all_simple {
            // Fast path
            let mut projected = RowVec::with_capacity(rows.len());
            let num_select_cols = select_column_indices.len();
            let num_extra_cols = extra_order_columns.len();

            for (row_id, row) in rows.drain_rows().enumerate() {
                let mut values = Vec::with_capacity(num_select_cols + num_extra_cols);
                // Add SELECT columns
                for idx in &select_column_indices {
                    values.push(
                        row.get(idx.unwrap())
                            .cloned()
                            .unwrap_or(Value::null_unknown()),
                    );
                }
                // Add extra ORDER BY columns
                for &(idx, _) in &extra_order_columns {
                    values.push(row.get(idx).cloned().unwrap_or(Value::null_unknown()));
                }
                projected.push((row_id as i64, Row::from_values(values)));
            }

            let extra_columns = extra_order_columns
                .iter()
                .map(|(_, name)| name.clone())
                .chain(
                    extra_computed_expressions
                        .iter()
                        .map(|(name, _)| name.clone()),
                )
                .collect();
            Ok((projected, extra_columns))
        } else {
            // Slow path: Use Evaluator for complex expressions
            let mut projected = RowVec::with_capacity(rows.len());

            // Create evaluator once and reuse for all rows
            let mut evaluator = CompiledEvaluator::new(&self.function_registry);
            evaluator = evaluator.with_context(ctx);
            evaluator.init_columns(all_columns);

            let total_extra = extra_order_columns.len() + extra_computed_expressions.len();
            let all_columns_arc = CompactArc::new(all_columns.to_vec());

            // OPTIMIZATION: Reuse col_index_map_lower for O(1) lookup
            for (row_id, row) in rows.drain_rows().enumerate() {
                let mut values = Vec::with_capacity(select_exprs.len() + total_extra);

                evaluator.set_row_array(&row);

                let correlated_ctx = if has_correlated_select {
                    let mut outer_row = FxHashMap::default();
                    for (index, column) in all_columns.iter().enumerate() {
                        let value = row.get(index).cloned().unwrap_or_else(Value::null_unknown);
                        let lower = column.to_lowercase();
                        if let Some(dot) = lower.rfind('.') {
                            outer_row
                                .entry(CompactArc::from(&lower[dot + 1..]))
                                .or_insert_with(|| value.clone());
                        }
                        outer_row.insert(CompactArc::from(lower.as_str()), value);
                    }
                    Some(ctx.with_outer_row(outer_row, CompactArc::clone(&all_columns_arc)))
                } else {
                    None
                };

                // Evaluate SELECT expressions
                for expr in select_exprs.iter() {
                    let processed_expr;
                    let expr = if Self::has_correlated_subqueries(expr) {
                        processed_expr = self.process_correlated_expression(
                            expr,
                            correlated_ctx.as_ref().ok_or_else(|| {
                                Error::internal("correlated projection lost its outer-row context")
                            })?,
                        )?;
                        &processed_expr
                    } else {
                        expr
                    };
                    let value = self.evaluate_select_expr(
                        &mut evaluator,
                        expr,
                        &row,
                        &col_index_map_lower,
                    )?;
                    values.push(value);
                }

                // Add extra ORDER BY columns
                for &(idx, _) in &extra_order_columns {
                    values.push(row.get(idx).cloned().unwrap_or(Value::null_unknown()));
                }

                // Evaluate computed ORDER BY / DISTINCT ON expressions
                for (_, expr) in &extra_computed_expressions {
                    let value = evaluator.evaluate(expr)?;
                    values.push(value);
                }

                projected.push((row_id as i64, Row::from_values(values)));
            }

            let extra_columns = extra_order_columns
                .iter()
                .map(|(_, name)| name.clone())
                .chain(
                    extra_computed_expressions
                        .iter()
                        .map(|(name, _)| name.clone()),
                )
                .collect();
            Ok((projected, extra_columns))
        }
    }

    /// Evaluate one expression against the current physical row shape.
    fn evaluate_select_expr(
        &self,
        evaluator: &mut CompiledEvaluator,
        expr: &Expression,
        row: &Row,
        col_index_map: &StringMap<usize>,
    ) -> Result<Value> {
        pipeline_projection::evaluate_expression(evaluator, expr, row, col_index_map)
    }

    /// Derive the public result column names from the SELECT projection.
    pub(crate) fn get_output_column_names(
        &self,
        select_exprs: &[Expression],
        all_columns: &[String],
        table_alias: Option<&str>,
    ) -> Vec<String> {
        pipeline_projection::output_column_names(select_exprs, all_columns, table_alias)
    }

    /// Get simple column projection indices (returns None if any expression is complex)
    ///
    /// Returns (column_indices, output_names) for simple SELECT with only column references.
    /// Returns None if there are computed expressions that require Evaluator.
    fn get_simple_projection_indices(
        &self,
        select_exprs: &[Expression],
        all_columns: &[String],
    ) -> Option<(Vec<usize>, Vec<String>)> {
        access_projection::simple_projection_indices(select_exprs, all_columns)
    }

    fn build_filtered_simple_projection_scan_plan(
        &self,
        filter_expr: &Expression,
        output_indices: &[usize],
        output_columns: &[String],
        all_columns: &[String],
    ) -> Option<access_projection::ProjectionScanPlan> {
        access_projection::filtered_simple_projection(
            filter_expr,
            output_indices,
            output_columns,
            all_columns,
        )
    }

    fn build_narrow_key_stream_plan(
        &self,
        filter_expr: Option<&Expression>,
        key_column: &str,
        all_columns: &[String],
    ) -> Option<access_projection::NarrowKeyStreamPlan> {
        access_projection::narrow_key_stream(filter_expr, key_column, all_columns)
    }

    fn build_filtered_expression_projection_scan_plan(
        &self,
        filter_expr: &Expression,
        select_exprs: &[Expression],
        output_columns: &[String],
        all_columns: &[String],
    ) -> Option<access_projection::ProjectionScanPlan> {
        access_projection::filtered_expression_projection(
            filter_expr,
            select_exprs,
            output_columns,
            all_columns,
        )
    }

    fn collect_scanner_rows(
        &self,
        scanner: Box<dyn radixdb_storage::traits::Scanner>,
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        access_scan::collect_scanner_rows(scanner, ctx)
    }

    fn build_ordered_distinct_projection_scan_plan(
        &self,
        stmt: &SelectStatement,
        output_columns: &[String],
        all_columns: &[String],
    ) -> Option<access_projection::ProjectionScanPlan> {
        access_projection::ordered_distinct_projection(None, stmt, output_columns, all_columns)
    }

    fn build_filtered_ordered_distinct_projection_scan_plan(
        &self,
        filter_expr: &Expression,
        stmt: &SelectStatement,
        output_columns: &[String],
        all_columns: &[String],
    ) -> Option<access_projection::ProjectionScanPlan> {
        access_projection::ordered_distinct_projection(
            Some(filter_expr),
            stmt,
            output_columns,
            all_columns,
        )
    }

    fn build_expression_projection_scan_plan(
        &self,
        select_exprs: &[Expression],
        output_columns: &[String],
        all_columns: &[String],
    ) -> Option<access_projection::ProjectionScanPlan> {
        access_projection::expression_projection(select_exprs, output_columns, all_columns)
    }

    fn evaluate_complex_order_keys(
        &self,
        context: &ComplexOrderKeyContext<'_>,
        row: &Row,
        evaluator: &mut CompiledEvaluator<'_>,
    ) -> Result<Vec<Value>> {
        evaluator.set_row_array(row);
        if !context.correlated {
            return context
                .stmt
                .order_by
                .iter()
                .map(|order| {
                    evaluator.evaluate(&order.expression).map_err(|source| {
                        Error::internal(format!(
                            "ORDER BY expression `{}` failed against output columns {:?}: {}",
                            order.expression, context.columns, source
                        ))
                    })
                })
                .collect();
        }

        let mut outer_row_map: FxHashMap<CompactArc<str>, Value> = FxHashMap::default();
        for (index, column) in context.columns_lower.iter().enumerate() {
            let value = row.get(index).cloned().unwrap_or_else(Value::null_unknown);
            if let Some(qualified) = context.qualified_names {
                outer_row_map.insert(qualified[index].clone(), value.clone());
            }
            outer_row_map.insert(column.clone(), value);
        }
        let correlated_ctx = context
            .execution
            .with_outer_row(outer_row_map, CompactArc::clone(context.columns));

        context
            .stmt
            .order_by
            .iter()
            .map(|order| {
                if Self::has_correlated_subqueries(&order.expression) {
                    let expression =
                        self.process_correlated_expression(&order.expression, &correlated_ctx)?;
                    let mut correlated_evaluator = CompiledEvaluator::new(&self.function_registry)
                        .with_context(&correlated_ctx);
                    correlated_evaluator.init_columns(context.columns);
                    correlated_evaluator.set_row_array(row);
                    correlated_evaluator.evaluate(&expression)
                } else {
                    evaluator.evaluate(&order.expression)
                }
            })
            .collect()
    }
}
