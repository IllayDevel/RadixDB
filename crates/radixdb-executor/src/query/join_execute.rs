impl Executor {
    /// Execute a JOIN source
    fn execute_join_source(
        &self,
        join_source: &JoinTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // classification is passed from caller to avoid redundant cache lookups
        let _join_index_order_scope = JoinIndexOrderScope::install(stmt, classification);

        // The parser preserves textual left-deep JOIN syntax. Before opening a
        // source, rewrite only complete, table-only INNER components using the
        // bound relation graph. Outer/USING/CROSS/derived boundaries are left
        // byte-for-byte in place. Reclassification makes the transformation
        // idempotent: the already cost-ordered tree produces no second rewrite.
        let _planned_execution_scope = if classification.reorderable_join_count >= 2
            && classification.join_projection_dependencies.is_some()
            && !join_subtree_has_physical_plan(join_source)
        {
            let physical_planning_started =
                radixdb_storage::instrumentation::join_planning_probe_active()
                    .then(radixdb_core::time_compat::Instant::now);
            let planned_table =
                self.plan_join_table_expression(join_source, stmt, classification)?;
            if let Some(started) = physical_planning_started {
                radixdb_storage::instrumentation::record_join_physical_planning(started.elapsed());
            }
            if planned_table != Expression::JoinSource(Box::new(join_source.clone())) {
                let _scope = PlannedJoinExecutionScope::install(&planned_table);
                let mut planned = stmt.clone();
                planned.table_expr = Some(Box::new(planned_table));
                let planned_classification = get_classification(&planned);
                return self.execute_select_internal(&planned, ctx, &planned_classification);
            }
            Some(PlannedJoinExecutionScope::install(&planned_table))
        } else {
            None
        };

        // Bind the query before a constant-false short-circuit. Invalid or
        // ambiguous references are SQL errors even when no row can survive.
        if let Some(ref where_clause) = stmt.where_clause {
            if let Ok(eval) = ExpressionEval::compile(where_clause, &[]) {
                if let Ok(Value::Boolean(false)) = eval.with_context(ctx).eval_slice(&Row::new()) {
                    let col_names: Vec<String> = stmt
                        .columns
                        .iter()
                        .enumerate()
                        .filter_map(|(i, expr)| match expr {
                            Expression::Aliased(a) => Some(a.alias.value.to_string()),
                            Expression::Identifier(id) => Some(id.value.to_string()),
                            Expression::QualifiedIdentifier(qi) => Some(qi.name.value.to_string()),
                            Expression::Star(_) | Expression::QualifiedStar(_) => None,
                            _ => Some(format!("column{}", i + 1)),
                        })
                        .collect();
                    let columns = CompactArc::new(col_names);
                    let result = ExecutorResult::with_arc_columns(
                        CompactArc::clone(&columns),
                        RowVec::new(),
                    );
                    return Ok((Box::new(result), columns, false, None));
                }
            }
        }

        // Get table aliases for filter pushdown
        let left_alias = get_table_alias_from_expr(&join_source.left);
        let right_alias = get_table_alias_from_expr(&join_source.right);
        let mut left_relation_aliases = FxHashSet::default();
        let mut right_relation_aliases = FxHashSet::default();
        collect_join_relation_aliases(&join_source.left, &mut left_relation_aliases);
        collect_join_relation_aliases(&join_source.right, &mut right_relation_aliases);
        let mut left_binding_columns = FxHashMap::default();
        let mut right_binding_columns = FxHashMap::default();
        self.collect_join_binding_columns(&join_source.left, ctx, &mut left_binding_columns, 0)?;
        self.collect_join_binding_columns(&join_source.right, ctx, &mut right_binding_columns, 0)?;

        let dependency_projections = join_dependency_projections(
            classification
                .join_projection_dependencies
                .as_deref()
                .map(Vec::as_slice),
            join_source,
            &left_relation_aliases,
            &right_relation_aliases,
            &left_binding_columns,
            &right_binding_columns,
        );
        let left_dependency_projection = dependency_projections.input_left;
        let right_dependency_projection = dependency_projections.input_right;
        let left_output_dependency_projection = dependency_projections.output_left;
        let right_output_dependency_projection = dependency_projections.output_right;
        // Determine join type early for filter pushdown decisions
        let join_type = join_source.join_type.to_uppercase();
        let reference_navigation_join = is_internal_reference_navigation_join(join_source);

        // Partition WHERE clause predicates for pushdown
        // Note: For OUTER JOINs, we must be careful:
        // - LEFT JOIN: Can push filters to left (preserved), but NOT to right (may have NULLs)
        // - RIGHT JOIN: Can push filters to right (preserved), but NOT to left
        // - FULL OUTER JOIN: Cannot push filters to either side
        let (left_filter, right_filter, cross_filter) =
            if let Some(ref where_clause) = stmt.where_clause {
                if !left_relation_aliases.is_empty() && !right_relation_aliases.is_empty() {
                    let (l, r, c) = partition_where_for_join(
                        where_clause,
                        join_source,
                        &join_source.left,
                        &join_source.right,
                        &left_binding_columns,
                        &right_binding_columns,
                    )?;

                    // For OUTER JOINs, we can't push filters to the NULL-padded side
                    // because rows that don't match need to appear with NULLs
                    let can_push_left = !join_type.contains("RIGHT") && !join_type.contains("FULL");
                    let can_push_right = !join_type.contains("LEFT") && !join_type.contains("FULL");

                    let safe_left = if can_push_left {
                        localize_join_side_filter(l.as_ref(), &left_relation_aliases)
                    } else {
                        None
                    };

                    let safe_right = if can_push_right {
                        localize_join_side_filter(r.as_ref(), &right_relation_aliases)
                    } else {
                        None
                    };

                    // Any filters we couldn't push need to be applied post-join
                    // If we pushed a filter, don't include it in remaining; otherwise include it
                    let unpushed_left = if !can_push_left { l } else { None };
                    let unpushed_right = if !can_push_right { r } else { None };

                    let remaining = match (unpushed_left, unpushed_right, c) {
                        (Some(l), Some(r), Some(c)) => combine_predicates_with_and(vec![l, r, c]),
                        (Some(l), Some(r), None) => combine_predicates_with_and(vec![l, r]),
                        (Some(l), None, Some(c)) => combine_predicates_with_and(vec![l, c]),
                        (None, Some(r), Some(c)) => combine_predicates_with_and(vec![r, c]),
                        (Some(l), None, None) => Some(l),
                        (None, Some(r), None) => Some(r),
                        (None, None, c) => c,
                    };

                    (safe_left, safe_right, remaining)
                } else {
                    (None, None, Some((**where_clause).clone()))
                }
            } else {
                (None, None, None)
            };

        if !reference_navigation_join {
            if let Some(result) = self.try_execute_count_integer_antijoin(
                join_source,
                stmt,
                ctx,
                classification,
                &join_type,
                left_alias.as_deref(),
                right_alias.as_deref(),
                left_filter.as_ref(),
                right_filter.as_ref(),
                cross_filter.as_ref(),
            )? {
                return Ok(result);
            }
            if let Some(result) = self.try_execute_count_pk_semijoin(
                join_source,
                stmt,
                ctx,
                classification,
                &join_type,
                left_alias.as_deref(),
                right_alias.as_deref(),
                left_filter.as_ref(),
                right_filter.as_ref(),
                cross_filter.as_ref(),
            )? {
                return Ok(result);
            }
        }

        // Semi-join reduction optimization for LEFT JOIN + GROUP BY + LIMIT
        // Pattern: LEFT JOIN + GROUP BY on left columns only + LIMIT N + no ORDER BY
        // Optimization: limit left side first, filter right side with IN clause (uses index)
        // This reduces materialization from O(L + R) to O(N + N*avg_matches)
        let semijoin_limit = (!reference_navigation_join)
            .then(|| {
                self.get_semijoin_reduction_limit(
                    &join_type,
                    stmt,
                    left_alias.as_deref(),
                    &join_source.condition,
                )
            })
            .flatten();

        let (left_rows, left_columns, right_rows, right_columns) = if let Some((
            limit_n,
            left_key_col,
            right_key_col,
        )) = semijoin_limit
        {
            // Semi-join reduction for INNER/LEFT JOIN + GROUP BY
            // Step 1: Execute and materialize left side with limit (pushdown for efficiency)
            let (left_result, left_cols) = self.execute_table_expression_with_filter_limit(
                &join_source.left,
                ctx,
                left_filter.as_ref(),
                Some(limit_n),
            )?;
            let left_rows = Self::materialize_result_arc(left_result)?;

            // Step 2: Extract join key values from limited left rows
            let left_key_idx = Self::find_column_index_by_name(&left_key_col, &left_cols);
            let join_key_values: Vec<Value> = if let Some(idx) = left_key_idx {
                left_rows
                    .iter()
                    .filter_map(|row| row.get(idx).cloned())
                    .filter(|v| !v.is_null())
                    .collect()
            } else {
                Vec::new()
            };

            // Step 3: Build combined filter for right side with IN clause
            let right_filter_with_in = if !join_key_values.is_empty() {
                // Create IN expression: right_key_col IN (v1, v2, ..., vN)
                let in_expr = self.build_in_filter_expression(&right_key_col, &join_key_values);
                // Combine with existing right filter if any
                match (right_filter.clone(), in_expr) {
                    (Some(existing), Some(in_filter)) => {
                        Some(Expression::Infix(InfixExpression::new(
                            Token::new(TokenType::Keyword, "AND", Position::default()),
                            Box::new(existing),
                            "AND".to_string(),
                            Box::new(in_filter),
                        )))
                    }
                    (None, Some(in_filter)) => Some(in_filter),
                    (existing, None) => existing,
                }
            } else {
                right_filter.clone()
            };

            // Step 4: Execute right side with IN filter (uses index on right_key_col)
            let (right_result, right_cols) = self.execute_table_expression_with_filter(
                &join_source.right,
                ctx,
                right_filter_with_in.as_ref(),
            )?;
            let right_rows = Self::materialize_result_arc(right_result)?;

            (left_rows, left_cols, right_rows, right_cols)
        } else {
            'join_inputs: {
                // A window consumer still uses the general path because its finalization
                // contract is tied to that path. GROUP BY and aggregate consumers do need
                // the complete JOIN result, but that is no longer a reason to reject an
                // indexed edge: BatchIndexNL deduplicates keys and fetches them in bounded
                // batches. The cost model below decides whether that batch is cheaper than
                // scanning/materializing the complete inner relation.
                let has_window = classification.has_window_functions;
                let requires_complete_join =
                    has_window || classification.has_aggregation || classification.has_group_by;
                let cross_filter_has_subqueries =
                    cross_filter.as_ref().is_some_and(Self::has_subqueries);

                // Check if we can use Index Nested Loop Join
                // This optimization avoids materializing the right side entirely
                // NOTE: Window queries still fall through to standard path; allowing them here
                // would require a separate window finalization branch.
                let index_nl_info =
                    if has_window || cross_filter_has_subqueries || reference_navigation_join {
                        None
                    } else {
                        self.check_index_nested_loop_opportunity(
                            &join_source.right,
                            join_source.condition.as_ref().map(|c| c.as_ref()),
                            &join_type,
                            left_alias.as_deref(),
                            right_alias.as_deref(),
                        )
                    };

                // Subquery join optimization: When right side is a subquery (not a table)
                // and left side is a table with PK/index, use swapped Index NL.
                // This handles inlined CTEs: (table) JOIN (subquery) -> subquery outer, table inner
                let (index_nl_info, force_swap) = if index_nl_info.is_none()
                    && !requires_complete_join
                    && !cross_filter_has_subqueries
                    && !reference_navigation_join
                    && (join_type == "INNER" || join_type == "LEFT")
                    && !matches!(join_source.right.as_ref(), Expression::TableSource(_))
                    && !matches!(
                        join_source.right.as_ref(),
                        Expression::Aliased(a) if matches!(a.expression.as_ref(), Expression::TableSource(_))
                    ) {
                    // Right is subquery/CTE - check if left side has Index NL opportunity
                    let left_as_inner = self.check_index_nested_loop_opportunity(
                        &join_source.left,
                        join_source.condition.as_ref().map(|c| c.as_ref()),
                        &join_type,
                        right_alias.as_deref(), // Swap aliases for the check
                        left_alias.as_deref(),
                    );
                    if left_as_inner.is_some() {
                        (left_as_inner, true) // Force swap
                    } else {
                        (None, false)
                    }
                } else {
                    (index_nl_info, false)
                };

                // Join reordering optimization for INNER JOINs:
                // When one side has a filter, prefer putting filtered side as outer (left)
                // This reduces the number of probes into the inner table.
                // Swap if: right has filter, left doesn't, and swapped order gives Index NL on PK
                let (index_nl_info, nl_left_filter, nl_right_filter, swapped) = if force_swap {
                    // Subquery join optimization: swap is forced (right is subquery, left is table)
                    (
                        index_nl_info,
                        right_filter.clone(), // Subquery becomes outer, apply its filter
                        left_filter.clone(),  // Table becomes inner
                        true,
                    )
                } else if !requires_complete_join
                    && join_type == "INNER"
                    && right_filter.is_some()  // Right side has a filter
                    && left_filter.is_none()
                // Left side doesn't have a filter
                {
                    // Check if swapping gives Index NL opportunity with PK lookup
                    // (which is more efficient than secondary index lookup)
                    let swapped_info = self.check_index_nested_loop_opportunity(
                        &join_source.left, // Left becomes inner (right)
                        join_source.condition.as_ref().map(|c| c.as_ref()),
                        &join_type,
                        right_alias.as_deref(), // Swap aliases
                        left_alias.as_deref(),
                    );

                    // Prefer swapped if it gives PK lookup (most efficient)
                    let prefer_swap = matches!(
                        &swapped_info,
                        Some((_, IndexLookupStrategy::PrimaryKey, _, _, _))
                    );

                    if prefer_swap {
                        // Swap: right filter becomes outer filter
                        (
                            swapped_info,
                            right_filter.clone(),
                            left_filter.clone(),
                            true,
                        )
                    } else {
                        (
                            index_nl_info,
                            left_filter.clone(),
                            right_filter.clone(),
                            false,
                        )
                    }
                } else {
                    (
                        index_nl_info,
                        left_filter.clone(),
                        right_filter.clone(),
                        false,
                    )
                };

                if let Some((table_name, lookup_strategy, inner_col, outer_col, lookup_unique)) =
                    index_nl_info
                {
                    // Index Nested Loop path: stream outer side for early termination
                    // When swapped, execute right side as outer (with original right filter, now in nl_left_filter)
                    let outer_expr = if swapped {
                        &join_source.right
                    } else {
                        &join_source.left
                    };

                    // JOIN KEY EQUIVALENCE OPTIMIZATION:
                    // When right filter references the inner join key column, we can push an
                    // equivalent filter to the outer side. This dramatically reduces iterations.
                    //
                    // Example: SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.user_id IN (1,2,3)
                    //
                    // Without optimization: Scan ALL 10000 users, lookup orders for each, filter by user_id
                    // With optimization: Scan only users with id IN (1,2,3), then lookup their orders
                    //
                    // The join condition u.id = o.user_id means:
                    //   Filter "o.user_id IN (1,2,3)" is equivalent to "u.id IN (1,2,3)" for join results
                    let nl_left_filter = if let Some(ref right_f) = nl_right_filter {
                        // Check if right filter references the inner join key column
                        let references = filter_references_column(right_f, &inner_col);
                        if references {
                            // Create equivalent filter for outer side by substituting the column
                            if let Some(outer_filter) =
                                substitute_filter_column(right_f, &inner_col, &outer_col)
                            {
                                // Combine with existing left filter if any
                                match nl_left_filter {
                                    Some(existing) => {
                                        Some(Expression::Infix(InfixExpression::new(
                                            Token::new(
                                                TokenType::Keyword,
                                                "AND",
                                                Position::default(),
                                            ),
                                            Box::new(existing),
                                            "AND".to_string(),
                                            Box::new(outer_filter),
                                        )))
                                    }
                                    None => Some(outer_filter),
                                }
                            } else {
                                nl_left_filter
                            }
                        } else {
                            nl_left_filter
                        }
                    } else {
                        nl_left_filter
                    };

                    // Compute join limit EARLY so we can use it for outer table optimization
                    let can_push_limit = !join_type.contains("FULL")
                        && !classification.has_order_by
                        && !classification.has_group_by
                        && !classification.has_aggregation
                        && !classification.has_distinct
                        && cross_filter.is_none();

                    let requested_limit = if can_push_limit {
                        stmt.limit
                            .as_ref()
                            .map(|expr| Self::evaluate_page_expression(expr, ctx, "LIMIT"))
                            .transpose()?
                    } else {
                        None
                    };
                    let pushed_offset = if requested_limit.is_some() {
                        stmt.offset
                            .as_ref()
                            .map(|expr| Self::evaluate_page_expression(expr, ctx, "OFFSET"))
                            .transpose()?
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    let join_limit = requested_limit.map(|limit| {
                        u64::try_from(limit.saturating_add(pushed_offset)).unwrap_or(u64::MAX)
                    });

                    // A heuristic multiplier cannot prove join selectivity. Scan the
                    // complete outer input; the join operator may still stop once it
                    // has produced LIMIT + OFFSET actual matches.
                    let outer_projection = if swapped {
                        right_dependency_projection.as_deref()
                    } else {
                        left_dependency_projection.as_deref()
                    };
                    let (outer_result, outer_cols) = self
                        .execute_table_expression_with_filter_projection(
                            outer_expr,
                            ctx,
                            nl_left_filter.as_ref(),
                            outer_projection,
                        )?;

                    // Find the outer key index in outer columns
                    // OPTIMIZATION: Pre-compute lowercase column names to avoid per-column to_lowercase()
                    let outer_cols_lower: Vec<String> =
                        outer_cols.iter().map(|c| c.to_lowercase()).collect();
                    let outer_col_lower = outer_col.to_lowercase();
                    let outer_key_idx = outer_cols_lower
                        .iter()
                        .position(|c| c == &outer_col_lower)
                        .or_else(|| {
                            // Try unqualified match
                            let outer_unqualified = outer_col_lower
                                .rfind('.')
                                .map(|p| &outer_col_lower[p + 1..])
                                .unwrap_or(&outer_col_lower);
                            outer_cols_lower.iter().position(|c_lower| {
                                let c_unqualified = c_lower
                                    .rfind('.')
                                    .map(|p| &c_lower[p + 1..])
                                    .unwrap_or(c_lower);
                                c_unqualified == outer_unqualified
                            })
                        });

                    if let Some(outer_idx) = outer_key_idx {
                        // Obtain the lookup table from the active transaction
                        // when one exists. The table handle merges private
                        // inserts/deletes/key transitions with the committed
                        // index domain, so the same physical edge preserves
                        // read-your-writes instead of disabling IndexNL for the
                        // complete explicit transaction.
                        let (inner_table, standalone_lookup_transaction, _) = open_query_table_raw(
                            &self.engine,
                            &self.active_transaction,
                            &table_name,
                        )?
                        .into_parts();
                        let inner_schema = inner_table.schema();

                        // Build inner columns list (qualified)
                        // When swapped, inner table alias is the original left alias
                        let inner_alias = if swapped {
                            left_alias.as_deref().unwrap_or(&table_name)
                        } else {
                            right_alias.as_deref().unwrap_or(&table_name)
                        };
                        let inner_cols: Vec<String> = inner_schema
                            .columns
                            .iter()
                            .map(|col| format!("{}.{}", inner_alias, col.name))
                            .collect();

                        // Build all_columns to match physical row order (outer, inner)
                        // This avoids expensive per-row rotation - projections find columns by name
                        let all_columns = {
                            let mut all = outer_cols.clone();
                            all.extend(inner_cols.clone());
                            all
                        };
                        let post_join_projection =
                            self.streaming_join_post_projection(stmt, &all_columns, classification);

                        // The index proves only one equality edge. Every additional
                        // ON conjunct remains a match predicate inside IndexNL;
                        // applying it after a LEFT JOIN would incorrectly remove
                        // the NULL-extended row. Inner-side WHERE predicates also
                        // stay here because this path bypasses a normal inner scan.
                        let mut residual_expressions = Vec::new();
                        if !is_single_equality_join_condition(join_source.condition.as_deref()) {
                            if let Some(condition) = join_source.condition.as_ref() {
                                residual_expressions.push((**condition).clone());
                            }
                        }
                        if let Some(ref rf) = nl_right_filter {
                            residual_expressions.push(add_table_qualifier(rf, inner_alias));
                        }
                        let residual_filter = combine_predicates_with_and(residual_expressions)
                            .map(|residual| {
                                JoinFilter::new(
                                    &residual,
                                    &outer_cols,
                                    &inner_cols,
                                    &self.function_registry,
                                )
                                .map(|filter| filter.with_context(ctx))
                            })
                            .transpose()?;

                        // Try to push projection into the join operator for ~2.3x speedup.
                        // The same dependency width also feeds the physical cost model;
                        // SELECT * retains the complete schema width.
                        let final_projection_pushdown = cross_filter
                            .is_none()
                            .then(|| {
                                post_join_projection.as_ref().and_then(
                                    |(expressions, output_columns)| {
                                        let mut projection = compute_join_projection(
                                            expressions,
                                            &outer_cols,
                                            &inner_cols,
                                        )?;
                                        projection.output_columns = output_columns.clone();
                                        Some(projection)
                                    },
                                )
                            })
                            .flatten();
                        let (
                            outer_output_dependency_projection,
                            inner_output_dependency_projection,
                        ) = if swapped {
                            (
                                right_output_dependency_projection.as_deref(),
                                left_output_dependency_projection.as_deref(),
                            )
                        } else {
                            (
                                left_output_dependency_projection.as_deref(),
                                right_output_dependency_projection.as_deref(),
                            )
                        };
                        let dependency_projection_pushdown =
                            if final_projection_pushdown.is_none() && !reference_navigation_join {
                                cached_join_dependency_projection(
                                    classification,
                                    outer_output_dependency_projection,
                                    inner_output_dependency_projection,
                                    &outer_cols,
                                    &inner_cols,
                                )
                            } else {
                                None
                            };
                        let projection_pushdown = final_projection_pushdown
                            .as_ref()
                            .or(dependency_projection_pushdown.as_deref());

                        // A certified outer order is a physical property, not a
                        // cardinality estimate. A unique lookup preserves both
                        // that order and the 0..1 output bound for every outer
                        // row; replacing it with a hash join would force a later
                        // Top-N/sort and discard the certificate. Do not attach
                        // LIMIT here: an INNER lookup can still reject rows, so
                        // the complete ordered outer stream must remain visible
                        // until the final edge applies the statement page.
                        let requires_order_preserving_lookup =
                            lookup_unique && outer_result.ascending_nulls_last_ordering().is_some();

                        // An available index is not automatically the cheapest path.
                        // For complete-result joins, compare bounded lookup work with
                        // scanning/building the whole inner relation before opening it.
                        // LIMIT keeps the streaming INL path because first-row latency
                        // and early termination dominate complete-input cost there.
                        if join_limit.is_none() {
                            let scan_upper_bound = outer_result
                                .estimated_count()
                                .map(|rows| rows as u64)
                                .unwrap_or_else(|| {
                                    self.estimate_table_expr_cardinality(
                                        outer_expr,
                                        nl_left_filter.as_ref(),
                                    )
                                });
                            // Tighten the physical cold-scan upper bound with predicate
                            // selectivity. The helper remains bounded even when ANALYZE
                            // statistics are absent, which is the normal bulk-import state.
                            let outer_rows = self.estimate_filtered_rows_with_upper_bound(
                                outer_expr,
                                nl_left_filter.as_ref(),
                                scan_upper_bound,
                            );
                            if outer_rows != u64::MAX {
                                let planner = self.get_query_planner();
                                let hinted_inner_rows = inner_table.row_count_hint() as u64;
                                let analyzed_stats = planner.get_table_stats(&table_name);
                                let inner_rows =
                                    analyzed_stats.as_ref().map_or(hinted_inner_rows, |stats| {
                                        stats.row_count.max(hinted_inner_rows)
                                    });
                                let schema_row_width = estimated_schema_row_width(inner_schema);
                                let inner_row_width = analyzed_stats
                                    .as_ref()
                                    .map(|stats| stats.avg_row_size)
                                    .filter(|width| *width > 0)
                                    .unwrap_or(schema_row_width);
                                let byte_pages =
                                    inner_rows.saturating_mul(inner_row_width).div_ceil(4096);
                                let inner_pages = analyzed_stats
                                    .as_ref()
                                    .map_or(byte_pages, |stats| stats.page_count.max(byte_pages));
                                let inner_distinct_keys = planner
                                    .get_column_stats(&table_name, &inner_col)
                                    .map(|stats| stats.distinct_count)
                                    .filter(|count| *count > 0);
                                let decision =
                                    planner.plan_indexed_join_access(IndexedJoinCostInput {
                                        outer_rows,
                                        inner_rows,
                                        inner_pages,
                                        inner_distinct_keys,
                                        inner_row_width,
                                        projected_inner_width: estimated_projected_inner_width(
                                            inner_schema,
                                            projection_pushdown,
                                        ),
                                        lookup_unique,
                                        limit: join_limit,
                                    });

                                // A filtered self-join relation is an explicit
                                // key producer. Scanning the same physical table
                                // again would make unrelated rows part of the
                                // query cost. Keep the SQL outer-row stream (and
                                // therefore its multiplicity), while the bounded
                                // BatchIndexNL operator deduplicates only lookup
                                // keys inside each physical batch.
                                let outer_alias = get_table_alias_from_expr(outer_expr);
                                let use_keyed_self_join = join_type == "INNER"
                                    && nl_left_filter.is_some()
                                    && outer_rows < inner_rows
                                    && base_table_source(outer_expr).is_some_and(|source| {
                                        source
                                            .name
                                            .value_lower
                                            .as_str()
                                            .eq_ignore_ascii_case(&table_name)
                                    })
                                    && outer_alias.as_deref().is_some_and(|alias| {
                                        !alias.eq_ignore_ascii_case(inner_alias)
                                    });

                                if !decision.use_index_lookup
                                    && !requires_order_preserving_lookup
                                    && !use_keyed_self_join
                                {
                                    let outer_deferred = outer_result.preserves_deferred_rows();
                                    drop(inner_table);
                                    drop(standalone_lookup_transaction);

                                    #[cfg(feature = "test-mutations")]
                                    crate::test_mutations::pause_between_join_sources();

                                    let inner_expr = if swapped {
                                        &join_source.left
                                    } else {
                                        &join_source.right
                                    };
                                    let inner_projection = if swapped {
                                        left_dependency_projection.as_deref()
                                    } else {
                                        right_dependency_projection.as_deref()
                                    };
                                    let (inner_result, inner_columns) = self
                                        .execute_table_expression_with_filter_projection(
                                            inner_expr,
                                            ctx,
                                            nl_right_filter.as_ref(),
                                            inner_projection,
                                        )?;
                                    let inner_rows = Self::materialize_result_arc(inner_result)?;

                                    // The cost model may reject point lookups after a
                                    // recursive edge has already produced compact
                                    // rows. Falling back through RowVec here used to
                                    // copy that entire result before HashStreaming.
                                    // Keep the deferred outer side as the probe and
                                    // materialize only the table/build side.
                                    let equality_only = join_source
                                        .condition
                                        .as_deref()
                                        .map(|condition| {
                                            let (left_keys, _, residual) =
                                                extract_join_keys_and_residual(
                                                    condition,
                                                    &outer_cols,
                                                    &inner_columns,
                                                );
                                            !left_keys.is_empty() && residual.is_empty()
                                        })
                                        .unwrap_or(false);
                                    if outer_deferred
                                        && !swapped
                                        && equality_only
                                        && cross_filter.is_none()
                                    {
                                        // The cold inner scan may have narrowed its
                                        // physical row after the initial projection was
                                        // bound against the complete table schema. Rebind
                                        // by name before handing the map to HashStreaming;
                                        // stale full-schema ordinals are invalid here.
                                        let physical_projection =
                                            final_projection_pushdown.as_ref().and_then(|_| {
                                                post_join_projection.as_ref().and_then(
                                                    |(expressions, output_columns)| {
                                                        let mut projection =
                                                            compute_join_projection(
                                                                expressions,
                                                                &outer_cols,
                                                                &inner_columns,
                                                            )?;
                                                        projection.output_columns =
                                                            output_columns.clone();
                                                        Some(projection)
                                                    },
                                                )
                                            });
                                        if let Some(projection) = physical_projection {
                                            let probe_source: Box<dyn Operator> =
                                                Box::new(QueryResultOperator::new(
                                                    outer_result,
                                                    outer_cols.clone(),
                                                ));
                                            let (result, output_columns) = JoinExecutor::new()
                                                .execute_streaming_result(StreamingJoinRequest {
                                                    build_rows: inner_rows,
                                                    build_columns: &inner_columns,
                                                    probe_source,
                                                    probe_columns: outer_cols,
                                                    condition: join_source.condition.as_deref(),
                                                    join_type: &join_type,
                                                    build_is_left: swapped,
                                                    limit: None,
                                                    ctx,
                                                    pre_built_hash_state: None,
                                                    projection: Some(&projection),
                                                })?;
                                            return Ok((result, output_columns, false, None));
                                        }
                                    }

                                    let outer_rows = Self::materialize_result_arc(outer_result)?;
                                    break 'join_inputs if swapped {
                                        (inner_rows, inner_columns, outer_rows, outer_cols)
                                    } else {
                                        (outer_rows, outer_cols, inner_rows, inner_columns)
                                    };
                                }
                            }
                        }

                        // Execute Index Nested Loop Join using operators
                        // Use batch version for NO LIMIT (reduces lock overhead from O(N) to O(1))
                        // Use streaming version for LIMIT queries (supports early termination)

                        // Convert to operator types
                        let outer_op: Box<dyn Operator> =
                            Box::new(QueryResultOperator::new(outer_result, outer_cols.clone()));
                        let inner_schema_info: Vec<ColumnInfo> =
                            inner_cols.iter().map(ColumnInfo::new).collect();
                        let op_join_type = OperatorJoinType::parse(&join_type);

                        let join_op: Box<dyn Operator> = if join_limit.is_some() {
                            // Streaming INL for early termination with LIMIT
                            let op = IndexNestedLoopJoinOperator::new(
                                outer_op,
                                inner_table,
                                inner_schema_info,
                                op_join_type,
                                outer_idx,
                                lookup_strategy.clone(),
                                residual_filter,
                            )
                            .with_cancellation(ctx.cancellation_handle());

                            // Apply projection pushdown if available
                            if let Some(proj) = projection_pushdown {
                                let projected_schema: Vec<ColumnInfo> =
                                    proj.output_columns.iter().map(ColumnInfo::new).collect();
                                Box::new(op.with_projection(proj.columns.clone(), projected_schema))
                            } else {
                                Box::new(op)
                            }
                        } else {
                            // Batch INL for NO LIMIT - single batch fetch, O(1) lock overhead
                            let op = BatchIndexNestedLoopJoinOperator::new(
                                outer_op,
                                inner_table,
                                inner_schema_info,
                                op_join_type,
                                outer_idx,
                                lookup_strategy.clone(),
                                residual_filter,
                            )
                            .with_cancellation(ctx.cancellation_handle());

                            // Apply projection pushdown if available
                            if let Some(proj) = projection_pushdown {
                                let projected_schema: Vec<ColumnInfo> =
                                    proj.output_columns.iter().map(ColumnInfo::new).collect();
                                Box::new(op.with_projection(proj.columns.clone(), projected_schema))
                            } else {
                                Box::new(op)
                            }
                        };

                        // No rotation needed - all_columns matches physical order.
                        let output_columns = projection_pushdown
                            .map(|projection| projection.output_columns.clone())
                            .unwrap_or(all_columns);

                        // A simple projected or identity INL result is already a
                        // complete public/recursive result. Keep the live operator
                        // behind QueryResult so neither LIMIT nor the next JOIN
                        // edge waits for a full Vec<DeferredRow> collection.
                        let identity_projection = stmt.columns.len() == 1
                            && matches!(stmt.columns.first(), Some(Expression::Star(_)))
                            && !classification.has_group_by
                            && !classification.has_aggregation
                            && !classification.has_window_functions
                            && !classification.has_distinct
                            && !classification.select_has_correlated_subqueries
                            && !classification.where_has_correlated_subqueries
                            && !classification.order_by_has_correlated_subqueries;
                        let direct_cursor_supported = identity_projection
                            || final_projection_pushdown.is_some()
                            || (dependency_projection_pushdown.is_some()
                                && post_join_projection.is_some())
                            || (cross_filter.is_some() && post_join_projection.is_some());
                        if direct_cursor_supported {
                            let cursor_columns = CompactArc::new(output_columns);
                            let operator_result = OperatorExecutorResult::open(
                                CompactArc::clone(&cursor_columns),
                                join_op,
                                ctx.cancellation_handle(),
                                join_limit,
                            )?;
                            let mut result: Box<dyn QueryResult> = Box::new(operator_result);
                            if let Some(cross) = cross_filter.as_ref() {
                                let alias_map = Self::build_alias_map_excluding(
                                    &stmt.columns,
                                    Some(cursor_columns.as_ref()),
                                );
                                let resolved_cross = if alias_map.is_empty() {
                                    cross.clone()
                                } else {
                                    Self::substitute_aliases(cross, &alias_map)
                                };
                                let filter = RowFilter::new(&resolved_cross, &cursor_columns)?
                                    .with_context(ctx);
                                result =
                                    Box::new(DeferredFilteredResult::from_filter(result, filter));
                            }
                            let final_columns = if identity_projection {
                                cursor_columns.as_ref().clone()
                            } else if final_projection_pushdown.is_some() {
                                post_join_projection
                                    .as_ref()
                                    .expect("pushed final projection requires output bindings")
                                    .1
                                    .clone()
                            } else {
                                let (expressions, columns) = post_join_projection
                                    .expect("dependency projection requires final mapper");
                                result = Box::new(ExprMappedResult::with_context(
                                    result,
                                    expressions,
                                    columns.clone(),
                                    ctx,
                                )?);
                                columns
                            };
                            let ordered_page_applied = Self::join_result_proves_statement_order(
                                stmt,
                                result.as_ref(),
                                &final_columns,
                            );
                            if ordered_page_applied {
                                let limit = stmt
                                    .limit
                                    .as_ref()
                                    .map(|expression| {
                                        Self::evaluate_page_expression(expression, ctx, "LIMIT")
                                    })
                                    .transpose()?;
                                let offset = stmt
                                    .offset
                                    .as_ref()
                                    .map(|expression| {
                                        Self::evaluate_page_expression(expression, ctx, "OFFSET")
                                    })
                                    .transpose()?
                                    .unwrap_or(0);
                                result = Box::new(LimitedResult::new(result, limit, offset));
                                radixdb_storage::instrumentation::record_join_ordering_skip();
                            } else if let Some(limit) = requested_limit {
                                result = Box::new(LimitedResult::new(
                                    result,
                                    Some(limit),
                                    pushed_offset,
                                ));
                            }
                            let final_columns = CompactArc::new(final_columns);
                            return Ok((
                                result,
                                final_columns,
                                ordered_page_applied || requested_limit.is_some(),
                                None,
                            ));
                        }

                        // Execute and collect results with synthetic row IDs for
                        // consumers that still need post-JOIN filtering, sorting,
                        // aggregation, or expression projection.
                        let mut join_op = join_op;
                        if let Err(error) = join_op.open() {
                            let _ = join_op.close();
                            return Err(error);
                        }
                        let collect_result = (|| {
                            let mut result_rows = Vec::new();
                            loop {
                                if result_rows.len() & 0xff == 0 {
                                    ctx.check_cancelled()?;
                                }
                                let Some(row_ref) = join_op.next()? else {
                                    break;
                                };
                                result_rows.push(row_ref.into_deferred());
                                if let Some(lim) = join_limit {
                                    if result_rows.len() >= lim as usize {
                                        break;
                                    }
                                }
                            }
                            Ok(result_rows)
                        })();
                        let close_result = join_op.close();
                        let result_rows = match (collect_result, close_result) {
                            (Ok(rows), Ok(())) => rows,
                            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
                        };

                        // Cross-table WHERE remains post-JOIN, but the expression VM
                        // can read the compact projection directly. Reject rows before
                        // constructing owned payload rows and preserve qualifying rows
                        // for a following recursive JOIN edge.
                        let mut result_rows = result_rows;
                        if let Some(ref cross) = cross_filter {
                            let filter = RowFilter::new(cross, &output_columns)?.with_context(ctx);
                            let mut filtered = Vec::with_capacity(result_rows.len());
                            for row in result_rows {
                                if filter.matches_deferred_checked(&row)? {
                                    filtered.push(row);
                                }
                            }
                            result_rows = filtered;
                        }

                        if dependency_projection_pushdown.is_some()
                            && stmt.order_by.is_empty()
                            && stmt.offset.is_none()
                            && stmt.limit.is_none()
                            && !classification.has_group_by
                            && !classification.has_aggregation
                            && !classification.has_window_functions
                            && !classification.has_distinct
                            && !classification.select_has_correlated_subqueries
                            && !classification.where_has_correlated_subqueries
                            && !classification.order_by_has_correlated_subqueries
                        {
                            let simple_projection =
                                self.get_simple_projection_indices(&stmt.columns, &output_columns);
                            if let Some((indices, output_names)) = simple_projection {
                                let identity = indices.len() == output_columns.len()
                                    && indices
                                        .iter()
                                        .enumerate()
                                        .all(|(output, source)| output == *source);
                                if !identity {
                                    let columns = CompactArc::from(indices);
                                    result_rows = result_rows
                                        .into_iter()
                                        .map(|row| {
                                            radixdb_storage::DeferredRow::remapped(
                                                row,
                                                CompactArc::clone(&columns),
                                            )
                                        })
                                        .collect();
                                }
                                let output_columns = CompactArc::new(output_names);
                                let result = DeferredExecutorResult::with_arc_columns(
                                    CompactArc::clone(&output_columns),
                                    result_rows,
                                );
                                return Ok((Box::new(result), output_columns, false, None));
                            }
                        }

                        // Expressions, sorting, aggregation, and final public
                        // output still require the established owned Row shape.
                        // Materialize once here, after the selective JOIN chain and
                        // after its post-JOIN predicate.
                        let mut final_rows = RowVec::with_capacity(result_rows.len());
                        for (row_id, row) in result_rows.into_iter().enumerate() {
                            final_rows.push((row_id as i64, row.into_owned()));
                        }

                        // Apply ORDER BY if present
                        if !stmt.order_by.is_empty() {
                            // Build sort specs by evaluating ORDER BY expressions
                            let mut evaluator = CompiledEvaluator::new(&self.function_registry);
                            evaluator = evaluator.with_context(ctx);
                            evaluator.init_columns(&output_columns);

                            // Compute sort keys and indices
                            let sort_keys: Vec<Vec<Value>> = final_rows
                                .iter()
                                .map(|(_, row)| {
                                    evaluator.set_row_array(row);
                                    stmt.order_by
                                        .iter()
                                        .map(|ob| {
                                            evaluator
                                                .evaluate(&ob.expression)
                                                .unwrap_or(Value::null_unknown())
                                        })
                                        .collect()
                                })
                                .collect();

                            // Sort by indices using sort_unstable_by for ~10-20% speedup
                            let mut indices: Vec<usize> = (0..final_rows.len()).collect();
                            indices.sort_unstable_by(|&a, &b| {
                                for (i, ob) in stmt.order_by.iter().enumerate() {
                                    let av = &sort_keys[a][i];
                                    let bv = &sort_keys[b][i];
                                    let asc = ob.ascending;
                                    let nulls_first = ob.nulls_first.unwrap_or(!asc);

                                    let cmp = if av.is_null() || bv.is_null() {
                                        if av.is_null() && bv.is_null() {
                                            Ordering::Equal
                                        } else if av.is_null() == nulls_first {
                                            Ordering::Less
                                        } else {
                                            Ordering::Greater
                                        }
                                    } else {
                                        let cmp = compare_values(av, bv);
                                        if asc {
                                            cmp
                                        } else {
                                            cmp.reverse()
                                        }
                                    };
                                    if cmp != Ordering::Equal {
                                        return cmp;
                                    }
                                }
                                Ordering::Equal
                            });

                            // Reorder rows
                            final_rows =
                                indices.into_iter().map(|i| final_rows[i].clone()).collect();

                            // Apply LIMIT/OFFSET after sorting
                            let offset = stmt
                                .offset
                                .as_ref()
                                .and_then(|e| {
                                    ExpressionEval::compile(e, &[])
                                        .ok()
                                        .and_then(|eval| {
                                            eval.with_context(ctx).eval_slice(&Row::new()).ok()
                                        })
                                        .and_then(|v| match v {
                                            Value::Integer(n) if n >= 0 => Some(n as usize),
                                            _ => None,
                                        })
                                })
                                .unwrap_or(0);

                            let limit = stmt
                                .limit
                                .as_ref()
                                .and_then(|e| {
                                    ExpressionEval::compile(e, &[])
                                        .ok()
                                        .and_then(|eval| {
                                            eval.with_context(ctx).eval_slice(&Row::new()).ok()
                                        })
                                        .and_then(|v| match v {
                                            Value::Integer(n) if n >= 0 => Some(n as usize),
                                            _ => None,
                                        })
                                })
                                .unwrap_or(usize::MAX);

                            final_rows = final_rows.into_iter().skip(offset).take(limit).collect();
                        } else if let Some(limit) = requested_limit {
                            if pushed_offset > 0 && pushed_offset < final_rows.len() {
                                final_rows.drain(..pushed_offset);
                            } else if pushed_offset >= final_rows.len() {
                                final_rows.clear();
                            }
                            final_rows.truncate(limit);
                        }

                        let paging_applied = !stmt.order_by.is_empty() || requested_limit.is_some();

                        // Check for aggregation/window functions that need special handling
                        let has_agg = classification.has_aggregation;
                        let has_window = classification.has_window_functions;

                        if has_agg && !has_window {
                            let result = self.execute_select_with_aggregation(
                                stmt,
                                ctx,
                                final_rows,
                                &output_columns,
                            )?;
                            let columns = CompactArc::new(result.columns().to_vec());
                            return Ok((result, columns, false, None));
                        } else if has_window {
                            // Fall through to standard path for window handling.
                            // Don't return early - let the standard path handle these.
                        } else if let Some(proj) = final_projection_pushdown {
                            // Projection was pushed down - rows are already projected
                            let output_columns = CompactArc::new(proj.output_columns);
                            let result = ExecutorResult::with_arc_columns(
                                CompactArc::clone(&output_columns),
                                final_rows,
                            );
                            return Ok((Box::new(result), output_columns, paging_applied, None));
                        } else {
                            // Project rows according to SELECT expressions
                            let projected_rows = self.project_rows_with_alias(
                                &stmt.columns,
                                final_rows,
                                &output_columns,
                                None,
                                ctx,
                                None,
                            )?;
                            let output_columns = CompactArc::new(self.get_output_column_names(
                                &stmt.columns,
                                &output_columns,
                                None,
                            ));

                            // Return with projected results
                            let result = ExecutorResult::with_arc_columns(
                                CompactArc::clone(&output_columns),
                                projected_rows,
                            );
                            return Ok((Box::new(result), output_columns, paging_applied, None));
                        }
                    }
                }

                // =================================================================
                // STREAMING HASH JOIN OPTIMIZATION
                // =================================================================
                // For queries with small LIMIT, use streaming execution to avoid
                // materializing the probe side. This enables true early termination.
                //
                // Eligibility:
                // - LIMIT ≤ 100 (small limit benefits from early termination)
                // - INNER JOIN (simpler, no NULL padding needed)
                // - Has equality keys (hash join applicable)
                // - No ORDER BY, GROUP BY, aggregation (would negate early termination)
                // - No cross-table predicates (applied after join)

                let streaming_limit = if !join_type.contains("FULL")
                    && !join_type.contains("LEFT")
                    && !join_type.contains("RIGHT")
                    && !classification.has_order_by
                    && !classification.has_group_by
                    && !classification.has_aggregation
                    && cross_filter.is_none()
                {
                    // Compute limit value
                    stmt.limit.as_ref().and_then(|limit_expr| {
                        ExpressionEval::compile(limit_expr, &[])
                            .ok()
                            .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                            .and_then(|v| match v {
                                Value::Integer(n) if (0..=100).contains(&n) => Some(n as u64),
                                _ => None,
                            })
                    })
                } else {
                    None
                };

                // Check if we have equality keys for hash join using AST analysis
                let has_equality_keys_for_streaming = join_source
                    .condition
                    .as_ref()
                    .map(|cond| Self::has_equality_condition(cond))
                    .unwrap_or(false);

                // Use streaming path if eligible
                if let Some(limit) = streaming_limit {
                    if has_equality_keys_for_streaming {
                        // Estimate cardinalities to choose optimal build side (smaller = build)
                        let left_card = self.estimate_table_expr_cardinality(
                            &join_source.left,
                            left_filter.as_ref(),
                        );
                        let right_card = self.estimate_table_expr_cardinality(
                            &join_source.right,
                            right_filter.as_ref(),
                        );
                        // Execute both sides
                        let (left_result, left_cols) = self
                            .execute_table_expression_with_filter_projection(
                                &join_source.left,
                                ctx,
                                left_filter.as_ref(),
                                left_dependency_projection.as_deref(),
                            )?;

                        let (right_result, right_cols) = self
                            .execute_table_expression_with_filter_projection(
                                &join_source.right,
                                ctx,
                                right_filter.as_ref(),
                                right_dependency_projection.as_deref(),
                            )?;

                        // A recursive JOIN result may carry a compact projected
                        // row graph. Keep that side as the streaming probe so the
                        // next edge can consume it without first copying every
                        // selected value into a complete owned Row. When neither
                        // side has this capability, retain the cardinality-based
                        // build-side decision.
                        let left_deferred = left_result.preserves_deferred_rows();
                        let right_deferred = right_result.preserves_deferred_rows();
                        let build_left = match (left_deferred, right_deferred) {
                            (true, false) => false,
                            (false, true) => true,
                            _ => left_card < right_card,
                        };

                        // Extract join keys BEFORE moving columns (uses original left/right positions)
                        let (left_key_indices, right_key_indices, residual_conditions) =
                            if let Some(cond) = join_source.condition.as_ref() {
                                extract_join_keys_and_residual(cond, &left_cols, &right_cols)
                            } else {
                                (Vec::new(), Vec::new(), Vec::new())
                            };

                        let projection_pushdown = if residual_conditions.is_empty()
                            && stmt.order_by.is_empty()
                            && !classification.has_aggregation
                            && !classification.has_window_functions
                            && !classification.select_has_correlated_subqueries
                            && !classification.where_has_correlated_subqueries
                            && !classification.order_by_has_correlated_subqueries
                        {
                            compute_join_projection(&stmt.columns, &left_cols, &right_cols)
                        } else {
                            None
                        };

                        // Build combined columns (always left-first for consistent output schema)
                        let mut all_cols = left_cols.clone();
                        all_cols.extend(right_cols.iter().cloned());

                        // Materialize the smaller side as build, stream the larger as probe
                        let (build_rows, build_cols, probe_result, probe_cols, build_is_left) =
                            if build_left {
                                let build = Self::materialize_result_arc(left_result)?;
                                (build, left_cols, right_result, right_cols, true)
                            } else {
                                let build = Self::materialize_result_arc(right_result)?;
                                (build, right_cols, left_result, left_cols, false)
                            };

                        // Map key indices based on which side is build
                        let (build_key_indices, probe_key_indices) = if build_is_left {
                            (left_key_indices, right_key_indices)
                        } else {
                            (right_key_indices, left_key_indices)
                        };

                        // Convert probe side to streaming operator
                        let probe_source: Box<dyn Operator> =
                            Box::new(QueryResultOperator::new(probe_result, probe_cols.clone()));

                        // Build hash table and bloom filter together in a single pass
                        // This avoids iterating build_rows twice (once for bloom, once for hash table)
                        let shared_build_batch = CompactArc::strong_count(&build_rows) > 1;
                        let (bloom_filter, pre_built_hash_state) =
                            if shared_build_batch && !build_key_indices.is_empty() {
                                // Repeated references to one immutable CTE/cache
                                // relation reuse one request-local hash state. A
                                // bloom rebuild would defeat that proof, so reuse
                                // takes priority on this uncommon shared path.
                                (
                                    None,
                                    ctx.join_hash_state_for(
                                        CompactArc::clone(&build_rows),
                                        &build_key_indices,
                                        true,
                                    ),
                                )
                            } else if build_rows.len() >= 100 && !build_key_indices.is_empty() {
                                let mut builder = BloomFilterBuilder::new(
                                    "join_key".to_string(),
                                    "build".to_string(),
                                    build_rows.len(),
                                );
                                // Single-pass: build hash table and populate bloom filter
                                let hash_state = ctx.join_hash_state_with_bloom_for(
                                    CompactArc::clone(&build_rows),
                                    &build_key_indices,
                                    &mut builder,
                                );
                                if let Some(hash_state) = hash_state {
                                    let bf = builder.build();
                                    let bloom = if bf.is_effective() { Some(bf) } else { None };
                                    (bloom, Some(hash_state))
                                } else {
                                    (None, None)
                                }
                            } else if !build_key_indices.is_empty() {
                                // No bloom filter, but still pre-build hash table
                                (
                                    None,
                                    ctx.join_hash_state_for(
                                        CompactArc::clone(&build_rows),
                                        &build_key_indices,
                                        false,
                                    ),
                                )
                            } else {
                                (None, None)
                            };

                        // Wrap probe with bloom filter if available
                        let probe_source: Box<dyn Operator> = if let Some(ref bf) = bloom_filter {
                            if !probe_key_indices.is_empty() {
                                Box::new(BloomFilterOperator::new(
                                    probe_source,
                                    bf.clone(),
                                    probe_key_indices.clone(),
                                ))
                            } else {
                                probe_source
                            }
                        } else {
                            probe_source
                        };

                        // Execute streaming hash join
                        let join_executor = JoinExecutor::new();
                        let streaming_request = StreamingJoinRequest {
                            build_rows,
                            build_columns: &build_cols,
                            probe_source,
                            probe_columns: probe_cols.clone(),
                            condition: join_source.condition.as_ref().map(|c| c.as_ref()),
                            join_type: &join_type,
                            build_is_left,
                            limit: Some(limit),
                            ctx,
                            pre_built_hash_state,
                            projection: projection_pushdown.as_ref(),
                        };

                        if projection_pushdown.is_some() {
                            let (result, output_columns) =
                                join_executor.execute_streaming_result(streaming_request)?;
                            return Ok((result, output_columns, false, None));
                        }

                        let join_result = join_executor.execute_streaming(streaming_request)?;

                        // Project and return results
                        let projected_rows = self.project_rows_with_alias(
                            &stmt.columns,
                            join_result.rows.into_owned(),
                            &all_cols,
                            None,
                            ctx,
                            None,
                        )?;
                        let output_columns = CompactArc::new(self.get_output_column_names(
                            &stmt.columns,
                            &all_cols,
                            None,
                        ));

                        let result = ExecutorResult::with_arc_columns(
                            CompactArc::clone(&output_columns),
                            projected_rows,
                        );
                        return Ok((Box::new(result), output_columns, false, None));
                    }
                }

                // =================================================================
                // STANDARD PATH: Materialize both sides
                // =================================================================

                // Execute both sides
                let (left_result, left_cols) = self
                    .execute_table_expression_with_filter_projection(
                        &join_source.left,
                        ctx,
                        left_filter.as_ref(),
                        left_dependency_projection.as_deref(),
                    )?;

                #[cfg(feature = "test-mutations")]
                crate::test_mutations::pause_between_join_sources();

                let (right_result, right_cols) = self
                    .execute_table_expression_with_filter_projection(
                        &join_source.right,
                        ctx,
                        right_filter.as_ref(),
                        right_dependency_projection.as_deref(),
                    )?;

                // Keep a simple equality chain as one pull pipeline. Exactly one
                // side becomes the bounded hash build; the other remains a live
                // probe cursor. Recursive deferred state is preferred as probe,
                // while a first binary edge chooses the smaller estimated side
                // as build instead of materializing both inputs.
                let left_deferred = left_result.preserves_deferred_rows();
                let right_deferred = right_result.preserves_deferred_rows();
                let left_estimated =
                    left_result.estimated_count().unwrap_or_else(|| {
                        usize::try_from(self.estimate_table_expr_cardinality(
                            &join_source.left,
                            left_filter.as_ref(),
                        ))
                        .unwrap_or(usize::MAX)
                    });
                let right_estimated =
                    right_result.estimated_count().unwrap_or_else(|| {
                        usize::try_from(self.estimate_table_expr_cardinality(
                            &join_source.right,
                            right_filter.as_ref(),
                        ))
                        .unwrap_or(usize::MAX)
                    });
                let streaming_hash_join_supported = join_source
                    .condition
                    .as_deref()
                    .map(|condition| {
                        let (left_keys, _, _) =
                            extract_join_keys_and_residual(condition, &left_cols, &right_cols);
                        !left_keys.is_empty()
                    })
                    .unwrap_or(false);
                let direct_pipeline_supported = streaming_hash_join_supported
                    && join_source.using_columns.is_empty()
                    && !join_type.contains("NATURAL")
                    && !reference_navigation_join
                    && (join_type.contains("INNER") || join_type.contains("LEFT"))
                    && cross_filter.is_none()
                    && !classification.has_group_by
                    && !classification.has_aggregation
                    && !classification.has_window_functions
                    && !classification.select_has_scalar_subqueries
                    && !classification.select_has_correlated_subqueries
                    && !classification.where_has_correlated_subqueries
                    && !classification.order_by_has_correlated_subqueries;
                let mut direct_source_columns = left_cols.clone();
                direct_source_columns.extend(right_cols.iter().cloned());
                let post_join_projection = if direct_pipeline_supported {
                    self.streaming_join_post_projection(
                        stmt,
                        &direct_source_columns,
                        classification,
                    )
                } else {
                    None
                };
                let direct_projection =
                    post_join_projection
                        .as_ref()
                        .and_then(|(expressions, output_columns)| {
                            let mut projection =
                                compute_join_projection(expressions, &left_cols, &right_cols)?;
                            projection.output_columns = output_columns.clone();
                            Some(projection)
                        });
                let dependency_projection = if direct_pipeline_supported
                    && direct_projection.is_none()
                    && !reference_navigation_join
                {
                    cached_join_dependency_projection(
                        classification,
                        left_output_dependency_projection.as_deref(),
                        right_output_dependency_projection.as_deref(),
                        &left_cols,
                        &right_cols,
                    )
                } else {
                    None
                };
                let identity_projection = direct_pipeline_supported
                    && stmt.columns.len() == 1
                    && matches!(stmt.columns.first(), Some(Expression::Star(_)));

                if direct_projection.is_some()
                    || dependency_projection.is_some()
                    || identity_projection
                {
                    let probe_left = if join_type.contains("LEFT") {
                        true
                    } else {
                        match (left_deferred, right_deferred) {
                            (true, false) => true,
                            (false, true) => false,
                            _ => left_estimated >= right_estimated,
                        }
                    };
                    let (build_rows, build_columns, probe_result, probe_columns, build_is_left) =
                        if probe_left {
                            (
                                Self::materialize_result_arc(right_result)?,
                                &right_cols,
                                left_result,
                                left_cols.clone(),
                                false,
                            )
                        } else {
                            (
                                Self::materialize_result_arc(left_result)?,
                                &left_cols,
                                right_result,
                                right_cols.clone(),
                                true,
                            )
                        };
                    let probe_source: Box<dyn Operator> = Box::new(QueryResultOperator::new(
                        probe_result,
                        probe_columns.clone(),
                    ));
                    let can_push_limit = stmt.order_by.is_empty()
                        && stmt.offset.is_none()
                        && !classification.has_distinct;
                    let limit = if can_push_limit {
                        stmt.limit
                            .as_ref()
                            .map(|expression| {
                                Self::evaluate_page_expression(expression, ctx, "LIMIT")
                            })
                            .transpose()?
                            .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX))
                    } else {
                        None
                    };
                    let physical_projection = direct_projection
                        .as_ref()
                        .or(dependency_projection.as_deref());
                    let (mut result, cursor_columns) = JoinExecutor::new()
                        .execute_streaming_result(StreamingJoinRequest {
                            build_rows,
                            build_columns,
                            probe_source,
                            probe_columns,
                            condition: join_source.condition.as_deref(),
                            join_type: &join_type,
                            build_is_left,
                            limit,
                            ctx,
                            pre_built_hash_state: None,
                            projection: physical_projection,
                        })?;
                    let output_columns = if identity_projection {
                        cursor_columns
                    } else if direct_projection.is_some() {
                        CompactArc::new(
                            post_join_projection
                                .as_ref()
                                .expect("pushed direct projection requires output bindings")
                                .1
                                .clone(),
                        )
                    } else {
                        let (expressions, columns) = post_join_projection
                            .expect("dependency projection requires final mapper");
                        result = Box::new(ExprMappedResult::with_context(
                            result,
                            expressions,
                            columns.clone(),
                            ctx,
                        )?);
                        CompactArc::new(columns)
                    };
                    return Ok((result, output_columns, limit.is_some(), None));
                }

                let left_rows = Self::materialize_result_arc(left_result)?;
                let right_rows = Self::materialize_result_arc(right_result)?;

                break 'join_inputs (left_rows, left_cols, right_rows, right_cols);
            }
        };

        // Keep post-materialization work in a separate stack frame.  A textual
        // left-deep JOIN recursively enters `execute_join_source` once per
        // edge.  In unoptimized builds the large collection of finalization
        // temporaries used to be reserved in every recursive frame, so a
        // perfectly valid 12+ edge JOIN could exhaust the standard 2 MiB
        // server worker stack before it reached the leaf scan.  The closure is
        // called only after both recursive inputs have returned; its frame is
        // therefore paid once instead of once per JOIN edge.
        let finish_join = move || -> SelectResult {
            // Combine column names (qualified with table aliases)
            let mut all_columns = left_columns.clone();
            all_columns.extend(right_columns.clone());

            // Handle NATURAL JOIN or USING clause by automatically finding common columns
            let natural_join_condition =
                if join_type.contains("NATURAL") || !join_source.using_columns.is_empty() {
                    // Find common columns between left and right tables
                    // For this, we extract the base column name (without table qualifier)
                    let left_base_cols: Vec<(usize, String)> = left_columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (i, extract_base_column_name(c)))
                        .collect();

                    let right_base_cols: Vec<(usize, String)> = right_columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (i, extract_base_column_name(c)))
                        .collect();

                    // Determine which columns to match
                    // For NATURAL JOIN: all common columns
                    // For USING clause: only specified columns
                    let using_col_names: Vec<String> = join_source
                        .using_columns
                        .iter()
                        .map(|c| c.value_lower.to_string())
                        .collect();

                    // Find matching column pairs and track excluded right-side columns
                    // Also track left columns to rename to unqualified names per SQL standard
                    let mut conditions: Vec<Expression> = Vec::new();
                    let mut excluded_right_indices: Vec<usize> = Vec::new();
                    let mut join_column_renames: Vec<(usize, String)> = Vec::new(); // (left_idx, base_name)
                    for (left_idx, left_base) in &left_base_cols {
                        for (right_idx, right_base) in &right_base_cols {
                            // For NATURAL JOIN, match all common columns
                            // For USING, only match specified columns
                            let should_match = if !using_col_names.is_empty() {
                                // USING clause - match only specified columns
                                using_col_names.contains(left_base) && left_base == right_base
                            } else {
                                // NATURAL JOIN - match all common columns
                                left_base == right_base
                            };

                            if should_match {
                                // Track right-side columns to exclude from SELECT *
                                // The index in all_columns is left_columns.len() + right_idx
                                excluded_right_indices.push(left_columns.len() + *right_idx);
                                // Track left column to rename to unqualified name
                                join_column_renames.push((*left_idx, left_base.clone()));

                                // Create equality condition: left_col = right_col
                                let left_col_name = left_columns[*left_idx].clone();
                                let right_col_name = right_columns[*right_idx].clone();
                                let left_col = Expression::Identifier(Identifier::new(
                                    Token::new(
                                        TokenType::Identifier,
                                        left_col_name.clone(),
                                        Position::default(),
                                    ),
                                    left_col_name,
                                ));
                                let right_col = Expression::Identifier(Identifier::new(
                                    Token::new(
                                        TokenType::Identifier,
                                        right_col_name.clone(),
                                        Position::default(),
                                    ),
                                    right_col_name,
                                ));
                                conditions.push(Expression::Infix(InfixExpression::new(
                                    Token::new(TokenType::Operator, "=", Position::default()),
                                    Box::new(left_col),
                                    "=".to_string(),
                                    Box::new(right_col),
                                )));
                            }
                        }
                    }

                    // Combine conditions with AND
                    if conditions.is_empty() {
                        (None, Vec::new(), Vec::new())
                    } else {
                        let mut combined = conditions.remove(0);
                        for cond in conditions {
                            combined = Expression::Infix(InfixExpression::new(
                                Token::new(TokenType::Keyword, "AND", Position::default()),
                                Box::new(combined),
                                "AND".to_string(),
                                Box::new(cond),
                            ));
                        }
                        (Some(combined), excluded_right_indices, join_column_renames)
                    }
                } else {
                    (None, Vec::new(), Vec::new())
                };

            // Destructure the tuple: (condition, excluded_column_indices, column_renames)
            let (natural_join_cond, excluded_column_indices, join_col_renames) =
                natural_join_condition;

            // Use natural join condition if present, otherwise use explicit condition
            let effective_condition = natural_join_cond
                .as_ref()
                .or(join_source.condition.as_ref().map(|c| c.as_ref()));

            // =================================================================
            // Execute JOIN using streaming JoinExecutor
            // =================================================================
            // JoinExecutor handles:
            // - Algorithm selection (Hash Join, Merge Join, Nested Loop)
            // - Build/probe side optimization for hash joins
            // - Merge join for pre-sorted inputs
            // - Residual filter application (non-equality conditions)
            // - Early termination with LIMIT

            // Compute LIMIT for early termination pushdown
            // Safe to push when: no ORDER BY, no GROUP BY/aggregation, no FULL OUTER
            let can_push_limit = !join_type.contains("FULL")
                && !classification.has_order_by
                && cross_filter.is_none()
                && !classification.has_group_by
                && !classification.has_aggregation
                && !classification.has_window_functions
                && !classification.has_distinct;

            let join_limit = if can_push_limit {
                let limit = stmt
                    .limit
                    .as_ref()
                    .map(|expression| Self::evaluate_page_expression(expression, ctx, "LIMIT"))
                    .transpose()?;
                if let Some(limit) = limit {
                    let offset = stmt
                        .offset
                        .as_ref()
                        .map(|expression| Self::evaluate_page_expression(expression, ctx, "OFFSET"))
                        .transpose()?
                        .unwrap_or(0);
                    Some(u64::try_from(limit.saturating_add(offset)).unwrap_or(u64::MAX))
                } else {
                    None
                }
            } else {
                None
            };

            // Get join algorithm decision from QueryPlanner
            // This uses cost-based optimization with edge-aware heuristics
            let (left_key_indices, right_key_indices, _residual_conditions) =
                if let Some(cond) = effective_condition {
                    extract_join_keys_and_residual(cond, &left_columns, &right_columns)
                } else {
                    (Vec::new(), Vec::new(), Vec::new())
                };
            if reference_navigation_join {
                let target_key_index = *right_key_indices.first().ok_or_else(|| {
                    Error::internal("generated reference join has no target equality key")
                })?;
                validate_reference_target_uniqueness(
                    right_rows.as_slice(),
                    target_key_index,
                    &right_columns[target_key_index],
                )?;
            }
            let final_projection_pushdown = if cross_filter.is_none()
                && stmt.order_by.is_empty()
                && stmt.offset.is_none()
                && !classification.has_group_by
                && !classification.has_aggregation
                && !classification.has_window_functions
                && !classification.has_distinct
                && !classification.select_has_correlated_subqueries
                && !classification.where_has_correlated_subqueries
                && !classification.order_by_has_correlated_subqueries
            {
                compute_join_projection(&stmt.columns, &left_columns, &right_columns)
            } else {
                None
            };
            let dependency_projection_pushdown =
                if final_projection_pushdown.is_none() && !reference_navigation_join {
                    cached_join_dependency_projection(
                        classification,
                        left_output_dependency_projection.as_deref(),
                        right_output_dependency_projection.as_deref(),
                        &left_columns,
                        &right_columns,
                    )
                } else {
                    None
                };
            let projection_pushdown = final_projection_pushdown
                .as_ref()
                .or(dependency_projection_pushdown.as_deref());
            let has_equality_keys = !left_key_indices.is_empty();

            // This materialized boundary carries no physical ordering certificate.
            // Do not add a complete O(N) pass merely to discover sortedness; the
            // fail-closed general equality path is hash join.
            let algorithm_decision = self.get_query_planner().plan_runtime_join(
                left_rows.len(),
                right_rows.len(),
                has_equality_keys,
            );

            // Execute join using JoinExecutor (takes ownership of rows)
            let join_executor = JoinExecutor::new();
            let join_request = super::join_executor::JoinRequest {
                left_rows,
                right_rows,
                left_columns: &left_columns,
                right_columns: &right_columns,
                condition: effective_condition,
                join_type: &join_type,
                limit: join_limit,
                ctx,
                algorithm_hint: Some(&algorithm_decision),
                ordering: JoinInputOrderings::default(),
                projection: projection_pushdown,
            };

            let join_result = join_executor.execute(join_request)?;
            if let Some(ref proj) = final_projection_pushdown {
                if join_result.columns == proj.output_columns {
                    let output_columns = CompactArc::new(join_result.columns);
                    let result = DeferredExecutorResult::with_arc_columns(
                        CompactArc::clone(&output_columns),
                        join_result.rows.into_deferred(),
                    );
                    return Ok((Box::new(result), output_columns, false, None));
                }
            }
            if dependency_projection_pushdown.is_some()
                && stmt.order_by.is_empty()
                && stmt.offset.is_none()
                && stmt.limit.is_none()
                && !classification.has_group_by
                && !classification.has_aggregation
                && !classification.has_window_functions
                && !classification.has_distinct
                && !classification.select_has_correlated_subqueries
                && !classification.where_has_correlated_subqueries
                && !classification.order_by_has_correlated_subqueries
            {
                if let Some(cross) = cross_filter.as_ref().filter(|filter| {
                    !Self::has_subqueries(filter) && !Self::has_correlated_subqueries(filter)
                }) {
                    if let Some((indices, output_names)) =
                        self.get_simple_projection_indices(&stmt.columns, &join_result.columns)
                    {
                        let alias_map = Self::build_alias_map_excluding(
                            &stmt.columns,
                            Some(&join_result.columns),
                        );
                        let resolved_cross = if alias_map.is_empty() {
                            cross.clone()
                        } else {
                            Self::substitute_aliases(cross, &alias_map)
                        };
                        let filter = RowFilter::new(&resolved_cross, &join_result.columns)?
                            .with_context(ctx);
                        let identity = indices.len() == join_result.columns.len()
                            && indices
                                .iter()
                                .enumerate()
                                .all(|(output, source)| output == *source);
                        let remap_columns = (!identity).then(|| CompactArc::from(indices));
                        let mut deferred_rows = Vec::with_capacity(join_result.rows.len());
                        for row in join_result.rows.into_deferred() {
                            if filter.matches_deferred_checked(&row)? {
                                let row = match remap_columns.as_ref() {
                                    Some(columns) => radixdb_storage::DeferredRow::remapped(
                                        row,
                                        CompactArc::clone(columns),
                                    ),
                                    None => row,
                                };
                                deferred_rows.push(row);
                            }
                        }
                        let output_columns = CompactArc::new(output_names);
                        let result = DeferredExecutorResult::with_arc_columns(
                            CompactArc::clone(&output_columns),
                            deferred_rows,
                        );
                        return Ok((Box::new(result), output_columns, false, None));
                    }
                }
            }
            let result_rows = join_result.rows.into_owned();
            let all_columns = if dependency_projection_pushdown.is_some() {
                join_result.columns.clone()
            } else {
                all_columns
            };
            if reference_navigation_join {
                let source_key_index = *left_key_indices.first().ok_or_else(|| {
                    Error::internal("generated reference join has no source equality key")
                })?;
                let target_key_index = *right_key_indices.first().ok_or_else(|| {
                    Error::internal("generated reference join has no target equality key")
                })?;
                let joined_target_key_index = left_columns.len() + target_key_index;
                validate_reference_join_matches(
                    &result_rows,
                    source_key_index,
                    joined_target_key_index,
                    &left_columns[source_key_index],
                    &right_columns[target_key_index],
                )?;
            }

            // Build alias map for alias substitution, excluding aliases that shadow real columns.
            let alias_map = Self::build_alias_map_excluding(&stmt.columns, Some(&all_columns));

            // Apply remaining WHERE clause if present (after filter pushdown)
            // IMPORTANT: When predicates were pushed to left/right (left_filter or right_filter is Some),
            // we should ONLY apply cross_filter (predicates that reference both tables).
            // If cross_filter is None and any pushdown happened, we don't need post-join filtering.
            // Only use stmt.where_clause when NO pushdown happened at all.
            let did_any_pushdown = left_filter.is_some() || right_filter.is_some();
            let effective_where = if did_any_pushdown {
                // Pushdown happened - only apply cross predicates (if any)
                cross_filter.clone()
            } else {
                // No pushdown - apply full WHERE clause
                stmt.where_clause.as_ref().map(|wc| (**wc).clone())
            };

            let resolved_where_clause = if !alias_map.is_empty() {
                effective_where
                    .as_ref()
                    .map(|where_expr| Box::new(Self::substitute_aliases(where_expr, &alias_map)))
            } else {
                effective_where.map(Box::new)
            };

            // Apply WHERE clause if present
            let filtered_rows = if let Some(ref where_clause) = resolved_where_clause {
                // `resolved_where_clause` may be a planner-produced rewrite, so its
                // subquery shape is not necessarily identical to the cached source
                // statement classification.
                let where_has_correlated_subqueries = Self::has_correlated_subqueries(where_clause);
                let where_has_subqueries = Self::has_subqueries(where_clause);
                if where_has_correlated_subqueries {
                    // A correlated predicate over a JOIN must see the complete joined
                    // row. Executing the subquery once against the parent context loses
                    // aliases/columns introduced by this JOIN and can either fail binding
                    // or reuse the wrong outer value.
                    let column_keys = ColumnKeyMapping::build_mappings(&all_columns, None);
                    let all_columns_arc = CompactArc::new(all_columns.clone());
                    let mut outer_row = FxHashMap::default();
                    outer_row
                        .reserve(all_columns.len() * 2 + ctx.outer_row().map_or(0, |m| m.len()));
                    let mut filtered = RowVec::with_capacity(result_rows.len());

                    for (id, row) in result_rows {
                        outer_row.clear();
                        if let Some(parent) = ctx.outer_row() {
                            outer_row.extend(
                                parent
                                    .iter()
                                    .map(|(key, value)| (key.clone(), value.clone())),
                            );
                        }
                        for mapping in &column_keys {
                            if let Some(value) = row.get(mapping.index) {
                                let value = value.clone();
                                if let Some(unqualified) = &mapping.unqualified_part {
                                    outer_row.insert(unqualified.clone(), value.clone());
                                }
                                if let Some(qualified) = &mapping.qualified_name {
                                    outer_row.insert(qualified.clone(), value.clone());
                                }
                                outer_row.insert(mapping.col_lower.clone(), value);
                            }
                        }

                        let mut correlated_ctx = ctx.with_outer_row(
                            std::mem::take(&mut outer_row),
                            CompactArc::clone(&all_columns_arc),
                        );
                        let matches = if let Expression::Exists(exists) = where_clause.as_ref() {
                            self.execute_exists_subquery(&exists.subquery, &correlated_ctx)?
                        } else if let Expression::Prefix(prefix) = where_clause.as_ref() {
                            if prefix.operator.eq_ignore_ascii_case("NOT") {
                                if let Expression::Exists(exists) = prefix.right.as_ref() {
                                    !self.execute_exists_subquery(
                                        &exists.subquery,
                                        &correlated_ctx,
                                    )?
                                } else {
                                    let processed = self
                                        .process_correlated_where(where_clause, &correlated_ctx)?;
                                    RowFilter::new(&processed, &all_columns)?
                                        .with_context(&correlated_ctx)
                                        .matches_checked(&row)?
                                }
                            } else {
                                let processed =
                                    self.process_correlated_where(where_clause, &correlated_ctx)?;
                                RowFilter::new(&processed, &all_columns)?
                                    .with_context(&correlated_ctx)
                                    .matches_checked(&row)?
                            }
                        } else {
                            let processed =
                                self.process_correlated_where(where_clause, &correlated_ctx)?;
                            RowFilter::new(&processed, &all_columns)?
                                .with_context(&correlated_ctx)
                                .matches_checked(&row)?
                        };
                        outer_row = correlated_ctx.take_outer_row().unwrap_or_default();
                        if matches {
                            filtered.push((id, row));
                        }
                    }
                    filtered
                } else {
                    // Process uncorrelated subqueries once before filtering.
                    let processed_where = if where_has_subqueries {
                        self.process_where_subqueries(where_clause, ctx)?
                    } else {
                        (**where_clause).clone()
                    };
                    let where_filter =
                        RowFilter::new(&processed_where, &all_columns)?.with_context(ctx);
                    let mut filtered = RowVec::with_capacity(result_rows.len());
                    for (id, row) in result_rows {
                        if where_filter.matches_checked(&row)? {
                            filtered.push((id, row));
                        }
                    }
                    filtered
                }
            } else {
                result_rows
            };

            // For NATURAL JOIN and JOIN USING, filter out duplicate columns from right side
            // when SELECT * is used. We need to remove these columns from both all_columns
            // and the row values to maintain consistency.
            // Also rename join columns to unqualified names per SQL standard.
            let (final_columns, final_rows) = if !excluded_column_indices.is_empty() {
                // Check if SELECT * is used (need to deduplicate columns)
                let has_star = stmt
                    .columns
                    .iter()
                    .any(|c| matches!(c, Expression::Star(_)));

                if has_star {
                    // Create a set for O(1) lookup
                    let excluded_set: FxHashSet<usize> =
                        excluded_column_indices.iter().copied().collect();

                    // Create rename map for join columns
                    let rename_map: FxHashMap<usize, String> =
                        join_col_renames.iter().cloned().collect();

                    // Build list of indices to keep (all except excluded)
                    let kept_indices: Vec<usize> = (0..all_columns.len())
                        .filter(|i| !excluded_set.contains(i))
                        .collect();

                    // Filter columns and apply renames for join columns
                    let filtered_columns: Vec<String> = kept_indices
                        .iter()
                        .map(|&i| {
                            if let Some(base_name) = rename_map.get(&i) {
                                // Use unqualified name for join columns per SQL standard
                                base_name.clone()
                            } else {
                                all_columns[i].clone()
                            }
                        })
                        .collect();

                    // Filter row values using clone_subset, preserving row IDs
                    let filtered_rows: RowVec = filtered_rows
                        .into_iter()
                        .map(|(row_id, row)| {
                            row.clone_subset(&kept_indices)
                                .map(|projected| (row_id, projected))
                        })
                        .collect::<Result<RowVec>>()?;

                    (filtered_columns, filtered_rows)
                } else {
                    (all_columns.clone(), filtered_rows)
                }
            } else {
                (all_columns.clone(), filtered_rows)
            };

            let has_agg = classification.has_aggregation;
            let has_window = classification.has_window_functions;

            // Check if we have both aggregation and window functions
            if has_agg && has_window {
                // 1. First apply GROUP BY aggregation
                // 2. Then apply window functions on the aggregated result
                let agg_result =
                    self.execute_aggregation_for_window(stmt, ctx, &final_rows, &final_columns)?;
                let agg_columns = agg_result.0.clone();
                let agg_rows = agg_result.1;

                // Apply window functions on aggregated rows (agg_rows is already RowVec with IDs)
                let result =
                    self.execute_select_with_window_functions(stmt, ctx, &agg_rows, &agg_columns)?;
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }

            // Check if we need window functions only (no aggregation)
            if has_window {
                let result = self.execute_select_with_window_functions(
                    stmt,
                    ctx,
                    &final_rows,
                    &final_columns,
                )?;
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }

            // Check if we need aggregation only
            if has_agg {
                let result =
                    self.execute_select_with_aggregation(stmt, ctx, final_rows, &final_columns)?;
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }

            // Check if ORDER BY or DISTINCT ON references columns not in SELECT
            let join_needs_extra_columns = self.order_by_needs_extra_columns(stmt, &final_columns);

            // Project rows according to SELECT expressions
            let (projected_rows, output_columns) = if join_needs_extra_columns {
                // Use projection that preserves extra ORDER BY / DISTINCT ON columns
                let (projected_rows, extra_columns) = self.project_rows_with_order_by(
                    &stmt.columns,
                    &stmt.order_by,
                    &stmt.distinct_on,
                    final_rows,
                    &final_columns,
                    ctx,
                )?;
                let mut output_columns =
                    self.get_output_column_names(&stmt.columns, &final_columns, None);
                output_columns.extend(extra_columns);
                (projected_rows, CompactArc::new(output_columns))
            } else {
                let projected_rows = self.project_rows_with_alias(
                    &stmt.columns,
                    final_rows,
                    &final_columns,
                    None,
                    ctx,
                    None,
                )?;
                // Determine output column names
                // Note: For JOIN results, columns are already qualified (e.g., "a.id", "b.id"),
                // so we pass None for table_alias - the prefix matching will work
                let output_columns = CompactArc::new(self.get_output_column_names(
                    &stmt.columns,
                    &final_columns,
                    None,
                ));
                (projected_rows, output_columns)
            };

            let result = ExecutorResult::with_arc_columns(
                CompactArc::clone(&output_columns),
                projected_rows,
            );
            Ok((Box::new(result), output_columns, false, None))
        };
        finish_join()
    }
}
