impl Executor {
    fn plan_join_table_expression(
        &self,
        join_source: &JoinTableSource,
        statement: &SelectStatement,
        classification: &QueryClassification,
    ) -> Result<Expression> {
        let Some(graph) = classification.logical_join_graph.as_deref() else {
            return Ok(Expression::JoinSource(Box::new(join_source.clone())));
        };
        let safe_limit = if !classification.has_order_by
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_distinct
            && !classification.has_distinct_on
            && !classification.has_offset
        {
            statement.limit.as_deref().and_then(|limit| match limit {
                Expression::IntegerLiteral(value) if value.value >= 0 => Some(value.value as u64),
                Expression::BoundValue(value) => value
                    .as_int64()
                    .filter(|value| *value >= 0)
                    .map(|value| value as u64),
                _ => None,
            })
        } else {
            None
        };
        self.rewrite_reorderable_inner_components(
            &Expression::JoinSource(Box::new(join_source.clone())),
            statement.where_clause.as_deref(),
            classification
                .join_projection_dependencies
                .as_deref()
                .map(Vec::as_slice),
            graph,
            safe_limit,
        )
    }

    fn rewrite_reorderable_inner_components(
        &self,
        expression: &Expression,
        where_clause: Option<&Expression>,
        dependencies: Option<&[Expression]>,
        graph: &LogicalJoinGraph,
        limit: Option<u64>,
    ) -> Result<Expression> {
        if is_reorderable_inner_component(expression)
            && graph_certifies_inner_component(expression, graph)
        {
            return self.reorder_inner_component(expression, where_clause, dependencies, limit);
        }
        let Expression::JoinSource(join) = expression else {
            return Ok(expression.clone());
        };
        let mut rewritten = join.clone();
        rewritten.left = Box::new(self.rewrite_reorderable_inner_components(
            &join.left,
            where_clause,
            dependencies,
            graph,
            limit,
        )?);
        rewritten.right = Box::new(self.rewrite_reorderable_inner_components(
            &join.right,
            where_clause,
            dependencies,
            graph,
            limit,
        )?);
        Ok(Expression::JoinSource(rewritten))
    }

    fn reorder_inner_component(
        &self,
        expression: &Expression,
        where_clause: Option<&Expression>,
        dependencies: Option<&[Expression]>,
        limit: Option<u64>,
    ) -> Result<Expression> {
        let mut leaves = Vec::new();
        let mut conditions = Vec::new();
        let mut join_token = None;
        flatten_reorderable_inner_component(
            expression,
            &mut leaves,
            &mut conditions,
            &mut join_token,
        );
        if leaves.len() < 3 {
            return Ok(expression.clone());
        }

        let aliases = leaves
            .iter()
            .map(get_table_alias_from_expr)
            .collect::<Option<Vec<_>>>();
        let Some(aliases) = aliases else {
            return Ok(expression.clone());
        };
        let all_aliases: FxHashSet<String> = aliases.iter().cloned().collect();
        if all_aliases.len() != leaves.len() {
            return Ok(expression.clone());
        }

        let condition_aliases = conditions
            .iter()
            .map(collect_table_qualifiers)
            .collect::<Vec<_>>();
        if condition_aliases.iter().any(|relations| {
            relations.is_empty() || !relations.iter().all(|alias| all_aliases.contains(alias))
        }) {
            return Ok(expression.clone());
        }

        let estimates = leaves
            .iter()
            .zip(&aliases)
            .map(|(leaf, alias)| {
                let filter = local_reorder_filter(where_clause, alias);
                let mut required_columns = FxHashSet::default();
                for condition in &conditions {
                    collect_qualified_columns_for_alias(condition, alias, &mut required_columns);
                }
                if let Some(where_clause) = where_clause {
                    collect_qualified_columns_for_alias(where_clause, alias, &mut required_columns);
                }
                if let Some(dependencies) = dependencies {
                    for dependency in dependencies {
                        collect_qualified_columns_for_alias(
                            dependency,
                            alias,
                            &mut required_columns,
                        );
                    }
                }
                self.estimate_reorder_leaf(leaf, filter.as_ref(), &required_columns)
            })
            .collect::<Vec<_>>();

        // Evaluate one connected greedy expansion from every possible root.
        // This is bounded O(R^3) for the accepted table-only component and is
        // enough for the 10-13 edge consumer chains without a 2^R memo table.
        let mut best: Option<(bool, u64, bool, u64, usize, Vec<usize>)> = None;
        for root in 0..leaves.len() {
            let mut order = Vec::with_capacity(leaves.len());
            let mut joined = FxHashSet::default();
            let mut remaining = (0..leaves.len()).collect::<FxHashSet<_>>();
            let mut current_rows = estimates[root].rows;
            let mut current_width = estimates[root].projected_width;
            let mut total_cost = estimates[root].root_cost();
            order.push(root);
            joined.insert(aliases[root].clone());
            remaining.remove(&root);

            while !remaining.is_empty() {
                let next = remaining
                    .iter()
                    .copied()
                    .filter_map(|candidate| {
                        let edge_conditions = conditions
                            .iter()
                            .zip(&condition_aliases)
                            .filter(|(_, relations)| {
                                relations.contains(&aliases[candidate])
                                    && relations.iter().any(|alias| joined.contains(alias))
                                    && relations.iter().all(|alias| {
                                        joined.contains(alias) || alias == &aliases[candidate]
                                    })
                            })
                            .map(|(condition, _)| condition.clone())
                            .collect::<Vec<_>>();
                        let condition = combine_predicates_with_and(edge_conditions)?;
                        let edge = self.estimate_reorder_edge(
                            &leaves[candidate],
                            &aliases[candidate],
                            &joined,
                            &condition,
                            current_rows,
                            current_width,
                            &estimates[candidate],
                            limit,
                        );
                        Some((candidate, edge))
                    })
                    .min_by_key(|(candidate, edge)| (edge.cost, edge.output_rows, *candidate));
                let Some((next, edge)) = next else {
                    order.clear();
                    break;
                };
                total_cost = total_cost.saturating_add(edge.cost);
                current_rows = edge.output_rows;
                current_width = current_width.saturating_add(estimates[next].projected_width);
                order.push(next);
                joined.insert(aliases[next].clone());
                remaining.remove(&next);
            }
            if order.len() != leaves.len() {
                continue;
            }
            // A locally indexed equality relation that feeds another alias of
            // the same physical table is a selective key producer. Prefer it
            // before broad dictionary/full-table roots: otherwise Q5-shaped
            // components scan the complete self-joined table and only apply
            // the selective relation at the end. This changes physical order
            // only; ordinary INNER JOIN multiplicity remains unchanged.
            let filtered_self_key_producer = estimates[root].filter_indexed
                && estimates.iter().enumerate().any(|(candidate, estimate)| {
                    candidate != root
                        && !estimate.table_name.is_empty()
                        && estimate
                            .table_name
                            .eq_ignore_ascii_case(&estimates[root].table_name)
                });

            // Outside that explicit self-key shape, root cardinality dominates
            // downstream work. For equal estimates,
            // prefer a proven indexed local predicate before comparing the
            // complete-chain byte cost. This prevents broad `IS NULL` filters
            // with weak fallback estimates from displacing an indexed job-id
            // root merely because the broad table happens to be narrow.
            let rank = (
                !filtered_self_key_producer,
                estimates[root].rows,
                !estimates[root].filter_indexed,
                total_cost,
                root,
            );
            if best
                .as_ref()
                .is_none_or(|(not_self_key, rows, unindexed, cost, ordinal, _)| {
                    rank < (*not_self_key, *rows, *unindexed, *cost, *ordinal)
                })
            {
                best = Some((rank.0, rank.1, rank.2, rank.3, rank.4, order));
            }
        }
        let Some((_, root_estimated_rows, _, estimated_cost, _, order)) = best else {
            return Ok(expression.clone());
        };

        radixdb_storage::instrumentation::record_join_planning(
            radixdb_storage::instrumentation::JoinPlanningRecord {
                original_order: aliases.clone(),
                planned_order: order
                    .iter()
                    .map(|ordinal| aliases[*ordinal].clone())
                    .collect(),
                root_estimated_rows,
                estimated_cost,
                safe_limit: limit,
            },
        );

        if order.iter().copied().eq(0..leaves.len()) {
            return Ok(expression.clone());
        }

        let mut leaves = leaves.into_iter().map(Some).collect::<Vec<_>>();
        let mut pending = conditions
            .into_iter()
            .zip(condition_aliases)
            .map(Some)
            .collect::<Vec<_>>();
        let mut joined = FxHashSet::default();
        let root = order[0];
        let mut tree = leaves[root].take().expect("reorder root consumed once");
        joined.insert(aliases[root].clone());
        let token = join_token.expect("three-leaf inner component has a JOIN token");

        for next in order.into_iter().skip(1) {
            joined.insert(aliases[next].clone());
            let mut edge_conditions = Vec::new();
            for pending_condition in &mut pending {
                let Some((condition, relations)) = pending_condition.as_ref() else {
                    continue;
                };
                if relations.iter().all(|alias| joined.contains(alias))
                    && relations.contains(&aliases[next])
                {
                    edge_conditions.push(condition.clone());
                    *pending_condition = None;
                }
            }
            if edge_conditions.is_empty() {
                return Ok(expression.clone());
            }
            tree = Expression::JoinSource(Box::new(JoinTableSource {
                token: token.clone(),
                left: Box::new(tree),
                join_type: SmartString::from("INNER"),
                right: Box::new(leaves[next].take().expect("reorder leaf consumed once")),
                condition: combine_predicates_with_and(edge_conditions).map(Box::new),
                using_columns: Vec::new(),
            }));
        }
        if pending.iter().any(Option::is_some) {
            return Ok(expression.clone());
        }
        Ok(tree)
    }

    fn estimate_reorder_leaf(
        &self,
        expression: &Expression,
        filter: Option<&Expression>,
        required_columns: &FxHashSet<String>,
    ) -> ReorderLeafEstimate {
        let Expression::TableSource(source) = expression else {
            return ReorderLeafEstimate {
                table_name: String::new(),
                rows: u64::MAX,
                base_rows: u64::MAX,
                pages: u64::MAX,
                row_width: u64::MAX,
                projected_width: u64::MAX,
                filter_indexed: false,
            };
        };
        let table_name = source.name.value_lower.to_string();
        let transaction = self.engine.begin_transaction().ok();
        let table = transaction
            .as_ref()
            .and_then(|transaction| transaction.get_table(&table_name).ok());
        let planner = self.get_query_planner();
        let hint_rows = table
            .as_ref()
            .map_or(0, |table| table.row_count_hint() as u64);
        let analyzed = planner.get_table_stats(&table_name);
        let base_rows = analyzed
            .as_ref()
            .map_or(hint_rows, |stats| stats.row_count.max(hint_rows));
        let rows = self.estimate_filtered_rows_with_upper_bound(expression, filter, base_rows);
        let schema_width = table
            .as_ref()
            .map_or(1, |table| estimated_schema_row_width(table.schema()));
        let row_width = analyzed
            .as_ref()
            .map(|stats| stats.avg_row_size)
            .filter(|width| *width > 0)
            .unwrap_or(schema_width);
        let byte_pages = base_rows.saturating_mul(row_width).div_ceil(4096).max(1);
        let pages = analyzed
            .as_ref()
            .map_or(byte_pages, |stats| stats.page_count.max(byte_pages));
        let projected_width = table.as_ref().map_or(row_width, |table| {
            required_columns
                .iter()
                .filter_map(|name| table.schema().find_column(name).map(|(_, column)| column))
                .map(|column| {
                    super::planner::estimated_schema_column_width(
                        column.data_type,
                        column.vector_dimensions,
                    )
                    .saturating_add(u64::from(column.nullable))
                })
                .sum::<u64>()
                .max(1)
        });
        let filter_indexed = filter.is_some_and(|filter| {
            local_equality_filter_columns(filter).iter().any(|column| {
                table.as_ref().is_some_and(|table| {
                    let schema = table.schema();
                    schema.pk_column_index().is_some_and(|index| {
                        schema.columns[index].name.eq_ignore_ascii_case(column)
                    }) || table.has_index_on_column(column)
                })
            })
        });
        ReorderLeafEstimate {
            table_name,
            rows,
            base_rows,
            pages,
            row_width,
            projected_width,
            filter_indexed,
        }
    }

    /// Bound a filtered table estimate even when ANALYZE statistics are absent.
    ///
    /// Cold segmented scans intentionally expose a physical upper bound rather
    /// than guessing predicate selectivity. That bound is useful for allocation,
    /// but feeding it unchanged into a JOIN access decision makes every filtered
    /// cold relation look like a full scan. Prefer planner statistics when they
    /// exist; otherwise apply the same conservative predicate-shape fallback used
    /// by costed JOIN reordering.
    fn estimate_filtered_rows_with_upper_bound(
        &self,
        expression: &Expression,
        filter: Option<&Expression>,
        upper_bound: u64,
    ) -> u64 {
        let Some(filter) = filter else {
            return upper_bound;
        };
        if upper_bound == 0 || upper_bound == u64::MAX {
            return upper_bound;
        }

        let analyzed = self.estimate_table_expr_cardinality(expression, Some(filter));
        if analyzed != u64::MAX {
            return upper_bound.min(analyzed);
        }

        flatten_and_predicates(filter)
            .iter()
            .fold(upper_bound, |rows, predicate| {
                let divisor = if Self::has_equality_condition(predicate) {
                    16
                } else {
                    4
                };
                rows.div_ceil(divisor).max(1)
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn estimate_reorder_edge(
        &self,
        candidate: &Expression,
        candidate_alias: &str,
        joined: &FxHashSet<String>,
        condition: &Expression,
        outer_rows: u64,
        outer_width: u64,
        inner: &ReorderLeafEstimate,
        limit: Option<u64>,
    ) -> ReorderEdgeEstimate {
        if let Some((_, _, inner_column, _, lookup_unique)) = self
            .check_index_nested_loop_opportunity(
                candidate,
                Some(condition),
                "INNER",
                None,
                Some(candidate_alias),
            )
        {
            let distinct = self
                .get_query_planner()
                .get_column_stats(&inner.table_name, &inner_column)
                .map(|stats| stats.distinct_count)
                .filter(|count| *count > 0);
            let decision =
                self.get_query_planner()
                    .plan_indexed_join_access(IndexedJoinCostInput {
                        outer_rows,
                        inner_rows: inner.base_rows,
                        inner_pages: inner.pages,
                        inner_distinct_keys: distinct,
                        inner_row_width: inner.row_width,
                        projected_inner_width: inner.projected_width,
                        lookup_unique,
                        limit,
                    });
            return ReorderEdgeEstimate {
                cost: decision
                    .lookup_cost
                    .min(decision.scan_hash_cost)
                    .saturating_add(outer_rows.saturating_mul(outer_width)),
                output_rows: decision.expected_matches,
            };
        }

        let equality_columns = equality_columns_for_alias(condition, candidate_alias, joined);
        let distinct = equality_columns
            .iter()
            .filter_map(|column| {
                self.get_query_planner()
                    .get_column_stats(&inner.table_name, column)
                    .map(|stats| stats.distinct_count)
            })
            .filter(|count| *count > 0)
            .max()
            .unwrap_or_else(|| inner.base_rows.max(1).isqrt().max(1));
        let fanout = if inner.base_rows == 0 {
            0
        } else {
            inner.base_rows.div_ceil(distinct).max(1)
        };
        let output_rows = outer_rows.saturating_mul(fanout);
        ReorderEdgeEstimate {
            cost: inner
                .pages
                .saturating_mul(4096)
                .saturating_add(inner.base_rows.saturating_mul(inner.projected_width))
                .saturating_add(outer_rows.saturating_mul(outer_width))
                .saturating_add(output_rows.saturating_mul(inner.projected_width)),
            output_rows,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn try_execute_count_integer_antijoin(
        &self,
        join_source: &JoinTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
        join_type: &str,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
        left_filter: Option<&Expression>,
        right_filter: Option<&Expression>,
        cross_filter: Option<&Expression>,
    ) -> Result<CountPkSemijoinAttempt> {
        // This rewrite is deliberately proof-driven and narrow. The WHERE
        // predicate must be exactly `right.not_null IS NULL`; every other
        // outer-join form continues through the general executor.
        if join_type != "LEFT"
            || !is_count_star_select(stmt)
            || !is_single_equality_join_condition(join_source.condition.as_deref())
            || classification.has_window_functions
            || classification.has_group_by
            || classification.has_order_by
            || classification.has_distinct
            || !stmt.set_operations.is_empty()
            || stmt.having.is_some()
            || right_filter.is_some()
            || classification.where_has_subqueries
        {
            return Ok(None);
        }
        let (Some(left_alias), Some(right_alias), Some(cross_filter)) =
            (left_alias, right_alias, cross_filter)
        else {
            return Ok(None);
        };
        let Some(null_rejected_column) = extract_right_is_null_column(cross_filter, right_alias)
        else {
            return Ok(None);
        };
        let Some(condition) = join_source.condition.as_deref() else {
            return Ok(None);
        };
        let Some((outer_key, inner_key)) =
            extract_qualified_join_columns(condition, left_alias, right_alias)
        else {
            return Ok(None);
        };

        let left_table = match base_table_source(join_source.left.as_ref()) {
            Some(table) if table.as_of.is_none() => table,
            _ => return Ok(None),
        };
        let right_table = match base_table_source(join_source.right.as_ref()) {
            Some(table) if table.as_of.is_none() => table,
            _ => return Ok(None),
        };
        let left_name = left_table.name.value_lower.to_string();
        let right_name = right_table.name.value_lower.to_string();

        // Obtain both handles from the same transaction so the narrow plan has
        // exactly the snapshot/read-your-writes semantics of the general join.
        let pair = open_query_table_pair(
            &self.engine,
            &self.active_transaction,
            &left_name,
            &right_name,
        )?;
        let outer = pair.left;
        let inner = pair.right;
        let mut standalone_transaction = pair.statement_transaction;

        let inner_schema = inner.schema();
        let Some((null_rejected_index, null_rejected)) =
            inner_schema.find_column(&null_rejected_column)
        else {
            return Ok(None);
        };
        let _ = null_rejected_index;
        if null_rejected.nullable {
            // A real matched row could itself contain NULL, so LEFT JOIN + IS
            // NULL would no longer be equivalent to anti-join.
            return Ok(None);
        }
        let Some((inner_key_index, inner_key_column)) = inner_schema.find_column(&inner_key) else {
            return Ok(None);
        };
        if inner_key_column.data_type != radixdb_core::DataType::Integer {
            return Ok(None);
        }

        let outer_schema = outer.schema();
        let Some((_, outer_key_column)) = outer_schema.find_column(&outer_key) else {
            return Ok(None);
        };
        if outer_key_column.data_type != radixdb_core::DataType::Integer {
            return Ok(None);
        }
        let outer_columns = outer_schema.column_names_owned().to_vec();
        let Some(plan) = self.build_narrow_key_stream_plan(left_filter, &outer_key, &outer_columns)
        else {
            return Ok(None);
        };
        let (storage_filter, needs_memory_filter) =
            access_predicate::prepare_bound_predicate(left_filter, outer_schema, ctx);
        if needs_memory_filter {
            return Ok(None);
        }

        let scanner = access_scan::open_exact_projection_scan(
            outer.as_ref(),
            &plan.scan_indices,
            storage_filter.as_deref(),
            ctx,
        )?;
        let outer_result: Box<dyn QueryResult> =
            Box::new(ScannerResult::new(scanner, plan.scan_columns.clone()));
        let outer_operator: Box<dyn Operator> =
            Box::new(QueryResultOperator::new(outer_result, plan.scan_columns));
        let lookup = if inner_schema.pk_column_index() == Some(inner_key_index) {
            IntegerAntiJoinLookup::PrimaryKey
        } else {
            IntegerAntiJoinLookup::Column(inner_key_index)
        };
        let mut operator = CountIntegerAntiJoinOperator::new(
            outer_operator,
            inner,
            plan.key_index_in_scan,
            lookup,
        )
        .with_context(ctx);
        if let Err(error) = operator.open() {
            let _ = operator.close();
            return Err(error);
        }
        let execution_result = operator.next().and_then(|row| {
            row.ok_or_else(|| Error::internal("count integer anti-join produced no scalar row"))
                .map(|row| row.into_owned())
        });
        let close_result = operator.close();
        let result_row = match (execution_result, close_result) {
            (Ok(row), Ok(())) => row,
            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
        };
        drop(operator);
        if let Some(transaction) = standalone_transaction.as_mut() {
            transaction.rollback()?;
        }

        let columns = CompactArc::new(self.get_output_column_names(&stmt.columns, &[], None));
        let result = ExecutorResult::with_arc_columns(
            CompactArc::clone(&columns),
            RowVec::from_vec(vec![(0, result_row)]),
        );
        Ok(Some((Box::new(result), columns, false, None)))
    }

    #[allow(clippy::too_many_arguments)]
    fn try_execute_count_pk_semijoin(
        &self,
        join_source: &JoinTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
        join_type: &str,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
        left_filter: Option<&Expression>,
        right_filter: Option<&Expression>,
        cross_filter: Option<&Expression>,
    ) -> Result<CountPkSemijoinAttempt> {
        // This is an intentionally narrow physical rewrite. Every excluded
        // shape continues through the established JOIN executor below.
        let count_star = is_count_star_select(stmt);
        let count_column = count_qualified_column_select(stmt);
        let supported_count = (join_type == "INNER" && count_star)
            || (matches!(join_type, "INNER" | "LEFT") && count_column.is_some());
        if !supported_count
            || !is_single_equality_join_condition(join_source.condition.as_deref())
            || classification.has_window_functions
            || classification.has_group_by
            || classification.has_order_by
            || classification.has_distinct
            || !stmt.set_operations.is_empty()
            || stmt.having.is_some()
            || right_filter.is_some()
            || cross_filter.is_some()
            || classification.where_has_subqueries
        {
            return Ok(None);
        }

        let left_table = match base_table_source(join_source.left.as_ref()) {
            Some(table) if table.as_of.is_none() => table,
            _ => return Ok(None),
        };
        let right_table = match base_table_source(join_source.right.as_ref()) {
            Some(table) if table.as_of.is_none() => table,
            _ => return Ok(None),
        };
        let Some((parent_name, lookup, _parent_key, child_key, _lookup_unique)) = self
            .check_index_nested_loop_opportunity(
                join_source.right.as_ref(),
                join_source.condition.as_deref(),
                join_type,
                left_alias,
                right_alias,
            )
        else {
            return Ok(None);
        };
        if !matches!(lookup, IndexLookupStrategy::PrimaryKey) {
            return Ok(None);
        }

        let child_name = left_table.name.value_lower.to_string();
        // `check_index_nested_loop_opportunity` resolves the physical inner
        // table. Keep this equality as a defensive guard against a future
        // expansion of that helper to views/subqueries.
        if parent_name != right_table.name.value_lower {
            return Ok(None);
        }
        let child_key = extract_base_column_name(&child_key).to_string();

        // An eligible count semi-join must preserve read-your-writes inside an
        // explicit transaction. `Table` handles are owned, so obtain them
        // under the short-lived transaction mutex and release it before scan
        // execution. Without this branch the general index-NL fallback opens
        // an unrelated snapshot for its inner lookup and misses local rows.
        let pair = open_query_table_pair(
            &self.engine,
            &self.active_transaction,
            &child_name,
            &parent_name,
        )?;
        let child = pair.left;
        let parent = pair.right;
        let mut standalone_transaction = pair.statement_transaction;
        let parent_schema = parent.schema();
        let Some(parent_pk_index) = parent_schema.pk_column_index() else {
            return Ok(None);
        };
        if parent_schema.columns[parent_pk_index].data_type != radixdb_core::DataType::Integer {
            return Ok(None);
        }
        if let Some(counted) = count_column {
            let Some(right_alias) = right_alias else {
                return Ok(None);
            };
            if !counted.qualifier.value.eq_ignore_ascii_case(right_alias) {
                return Ok(None);
            }
            let Some((_, counted_column)) = parent_schema.find_column(&counted.name.value_lower)
            else {
                return Ok(None);
            };
            // For a unique 0..1 lookup, COUNT(right.not_null_column) is exactly
            // the number of matching outer rows for both INNER and LEFT JOIN.
            // Nullable targets require fetching/evaluating the value and stay
            // on the general path.
            if counted_column.nullable {
                return Ok(None);
            }
        }

        let child_schema = child.schema();
        let child_columns = child_schema.column_names_owned().to_vec();
        let Some(plan) = self.build_narrow_key_stream_plan(left_filter, &child_key, &child_columns)
        else {
            return Ok(None);
        };
        let (storage_filter, needs_memory_filter) =
            access_predicate::prepare_bound_predicate(left_filter, child_schema, ctx);
        // A residual filter would need a separate, explicitly compiled narrow
        // evaluator. Do not silently drop it to obtain a faster count.
        if needs_memory_filter {
            return Ok(None);
        }

        let scanner = access_scan::open_exact_projection_scan(
            child.as_ref(),
            &plan.scan_indices,
            storage_filter.as_deref(),
            ctx,
        )?;
        let child_result: Box<dyn QueryResult> =
            Box::new(ScannerResult::new(scanner, plan.scan_columns.clone()));
        let child_operator: Box<dyn Operator> =
            Box::new(QueryResultOperator::new(child_result, plan.scan_columns));
        let mut operator =
            CountPkSemiJoinOperator::new(child_operator, parent, plan.key_index_in_scan)
                .with_context(ctx);
        if let Err(error) = operator.open() {
            let _ = operator.close();
            return Err(error);
        }
        let execution_result = operator.next().and_then(|row| {
            row.ok_or_else(|| Error::internal("count PK semi-join produced no scalar row"))
                .map(|row| row.into_owned())
        });
        let close_result = operator.close();
        let result_row = match (execution_result, close_result) {
            (Ok(row), Ok(())) => row,
            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
        };
        drop(operator);
        if let Some(transaction) = standalone_transaction.as_mut() {
            transaction.rollback()?;
        }

        let columns = CompactArc::new(self.get_output_column_names(&stmt.columns, &[], None));
        let result = ExecutorResult::with_arc_columns(
            CompactArc::clone(&columns),
            RowVec::from_vec(vec![(0, result_row)]),
        );
        Ok(Some((Box::new(result), columns, false, None)))
    }

    /// Execute a subquery source
    fn execute_subquery_source(
        &self,
        subquery_source: &SubqueryTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        radixdb_storage::instrumentation::record_derived_subquery_execute();
        // classification is passed from caller for the outer stmt
        // Note: The inner subquery will get its own classification via execute_select

        // Execute subquery with incremented depth to avoid creating new TimeoutGuard
        let subquery_ctx = ctx.with_incremented_query_depth();
        let mut result = self.execute_select(&subquery_source.subquery, &subquery_ctx)?;
        let columns = result.columns().to_vec();

        // OPTIMIZATION: For simple GROUP BY aggregation without WHERE clause, try streaming
        // directly to aggregation HashMap without materializing all rows first.
        // This reduces memory allocations from O(N) to O(groups).
        if stmt.where_clause.is_none()
            && classification.has_aggregation
            && classification.has_group_by
            && !classification.has_window_functions
        {
            match self.try_streaming_derived_table_aggregation(result, stmt, classification, ctx)? {
                super::aggregation::DerivedAggregationAttempt::Applied(agg_result) => {
                    let out_columns = CompactArc::new(agg_result.columns().to_vec());
                    return Ok((agg_result, out_columns, false, None));
                }
                super::aggregation::DerivedAggregationAttempt::Rejected(source) => {
                    result = source;
                }
            }
        }

        // Materialize the subquery result directly with synthetic row IDs. A
        // rejected streaming aggregate path returns this very source (and, if
        // needed, its prefetched first row), so it is never executed twice.
        let mut rows = RowVec::new();
        let mut row_id = 0i64;
        while result.next() {
            if row_id % 100 == 0 {
                ctx.check_cancelled()?;
            }
            rows.push((row_id, result.take_row()));
            row_id += 1;
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        // Apply WHERE clause if present
        let filtered_rows: RowVec = if let Some(ref where_clause) = stmt.where_clause {
            let where_filter = RowFilter::new(where_clause, &columns)?.with_context(ctx);

            let mut filtered = RowVec::with_capacity(rows.len());
            for (id, row) in rows {
                if where_filter.matches_checked(&row)? {
                    filtered.push((id, row));
                }
            }
            filtered
        } else {
            rows
        };

        let has_agg = classification.has_aggregation;
        let has_window = classification.has_window_functions;

        // Check if we have both aggregation and window functions. Window
        // functions operate on the aggregated result, not on the raw derived
        // source rows.
        if has_agg && has_window {
            let agg_result =
                self.execute_aggregation_for_window(stmt, ctx, &filtered_rows, &columns)?;
            let agg_columns = agg_result.0.clone();
            let agg_rows = agg_result.1;
            let result =
                self.execute_select_with_window_functions(stmt, ctx, &agg_rows, &agg_columns)?;
            let out_columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, out_columns, false, None));
        }

        // Check if we need window functions only
        if has_window {
            let result =
                self.execute_select_with_window_functions(stmt, ctx, &filtered_rows, &columns)?;
            let out_columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, out_columns, false, None));
        }

        // Check if we need aggregation
        if has_agg {
            let result =
                self.execute_select_with_aggregation(stmt, ctx, filtered_rows, &columns)?;
            let out_columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, out_columns, false, None));
        }

        // A derived table is a projection boundary just like a base table or
        // view. Keep ORDER BY / DISTINCT ON dependencies that are not part of
        // the public SELECT list until the outer executor has consumed them.
        if self.order_by_needs_extra_columns(stmt, &columns) {
            let (projected_rows, extra_columns) = self.project_rows_with_order_by(
                &stmt.columns,
                &stmt.order_by,
                &stmt.distinct_on,
                filtered_rows,
                &columns,
                ctx,
            )?;
            let subquery_alias = subquery_source
                .alias
                .as_ref()
                .map(|alias| alias.value_lower.as_str());
            let mut output_columns =
                self.get_output_column_names(&stmt.columns, &columns, subquery_alias);
            output_columns.extend(extra_columns);
            let output_columns = CompactArc::new(output_columns);
            let result = ExecutorResult::with_arc_columns(
                CompactArc::clone(&output_columns),
                projected_rows,
            );
            return Ok((Box::new(result), output_columns, false, None));
        }

        // Project rows according to SELECT expressions
        let projected_rows =
            self.project_rows_with_alias(&stmt.columns, filtered_rows, &columns, None, ctx, None)?;

        // Determine output column names
        let subquery_alias = subquery_source
            .alias
            .as_ref()
            .map(|a| a.value_lower.as_str());
        let output_columns =
            CompactArc::new(self.get_output_column_names(&stmt.columns, &columns, subquery_alias));

        let result =
            ExecutorResult::with_arc_columns(CompactArc::clone(&output_columns), projected_rows);
        Ok((Box::new(result), output_columns, false, None))
    }

    /// Execute a view as a subquery
    fn execute_view_query(
        &self,
        view_def: &ViewDefinition,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // classification is passed from caller for the outer stmt
        // Note: The view's inner query will get its own classification via execute_select

        // Check view depth to prevent stack overflow from deeply nested views
        let depth = ctx.view_depth();
        if depth >= MAX_VIEW_DEPTH {
            return Err(Error::InvalidArgument(format!(
                "Maximum view nesting depth ({}) exceeded",
                MAX_VIEW_DEPTH
            )));
        }

        // Parse the view's query using cache (returns Arc<Statement>, no clone)
        let view_stmt = self.parse_view_statement(&view_def.query)?;
        let view_select = match view_stmt.as_ref() {
            Statement::Select(s) => s,
            _ => unreachable!("parse_view_statement validates this is a SELECT"),
        };

        // Execute the view's query with incremented depth
        let nested_ctx = ctx.with_incremented_view_depth();
        let result = self.execute_select(view_select, &nested_ctx)?;
        let view_columns = result.columns().to_vec();

        // Apply outer query's WHERE clause if present
        // OPTIMIZATION: FilteredResult owns a pre-compiled RowFilter and reuses it for each row,
        // avoiding repeated expression compilation per row.
        // CRITICAL: Must pass ctx for parameter resolution ($1, named params, etc.)
        let mut result: Box<dyn QueryResult> = result;
        if let Some(ref where_clause) = stmt.where_clause {
            result = pipeline_filter::apply(result, where_clause, ctx)?;
        }

        // Aggregate+window is one pipeline: aggregate first, then evaluate the
        // window over aggregate rows. Test this combined shape before either
        // single-stage early return.
        if classification.has_aggregation && classification.has_window_functions {
            let mut rows = RowVec::with_capacity(64);
            let mut idx = 0i64;
            while result.next() {
                rows.push((idx, result.take_row()));
                idx += 1;
            }
            if let Some(err) = result.last_error() {
                return Err(err);
            }
            let (aggregate_columns, aggregate_rows) =
                self.execute_aggregation_for_window(stmt, ctx, &rows, &view_columns)?;
            let result = self.execute_select_with_window_functions(
                stmt,
                ctx,
                &aggregate_rows,
                &aggregate_columns,
            )?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Handle aggregation: if outer query has aggregates, materialize view result and aggregate
        if classification.has_aggregation {
            // Materialize the view result into rows with synthetic IDs
            let mut rows = RowVec::with_capacity(64);
            let mut idx = 0i64;
            while result.next() {
                rows.push((idx, result.take_row()));
                idx += 1;
            }
            if let Some(err) = result.last_error() {
                return Err(err);
            }

            // Execute aggregation on the view's rows
            let agg_result =
                self.execute_select_with_aggregation(stmt, ctx, rows, &view_columns)?;
            let columns = CompactArc::new(agg_result.columns().to_vec());
            return Ok((agg_result, columns, false, None));
        }

        // Handle window functions: materialize view result and delegate
        if classification.has_window_functions {
            let mut rows = RowVec::with_capacity(64);
            let mut idx = 0i64;
            while result.next() {
                rows.push((idx, result.take_row()));
                idx += 1;
            }
            if let Some(err) = result.last_error() {
                return Err(err);
            }

            let result =
                self.execute_select_with_window_functions(stmt, ctx, &rows, &view_columns)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Preserve source columns (or computed keys) that the outer ORDER BY
        // and DISTINCT ON need even when they are absent from the public
        // SELECT list. The regular table path uses the same projection helper;
        // views must keep the identical row shape until the outer sorter has
        // consumed these hidden values.
        if self.order_by_needs_extra_columns(stmt, &view_columns) {
            let mut rows = RowVec::with_capacity(64);
            let mut row_id = 0i64;
            while result.next() {
                rows.push((row_id, result.take_row()));
                row_id += 1;
            }
            if let Some(error) = result.last_error() {
                return Err(error);
            }

            let (projected_rows, extra_columns) = self.project_rows_with_order_by(
                &stmt.columns,
                &stmt.order_by,
                &stmt.distinct_on,
                rows,
                &view_columns,
                ctx,
            )?;
            let mut output_columns =
                self.get_output_column_names(&stmt.columns, &view_columns, None);
            output_columns.extend(extra_columns);

            let output_columns = CompactArc::new(output_columns);
            return Ok((
                Box::new(ExecutorResult::with_arc_columns(
                    CompactArc::clone(&output_columns),
                    projected_rows,
                )),
                output_columns,
                false,
                None,
            ));
        }

        // Handle projection: apply outer query's column selection
        // Check if SELECT * or t.* - if so, return all view columns
        let is_select_star = stmt.columns.len() == 1
            && matches!(
                &stmt.columns[0],
                Expression::Star(_) | Expression::QualifiedStar(_)
            );

        if is_select_star {
            // For SELECT *, just return the view result with WHERE applied
            // DISTINCT, ORDER BY, LIMIT/OFFSET are handled by execute_select
            return Ok((result, CompactArc::new(view_columns), false, None));
        }

        // Determine if we have any complex expressions (not just column references)
        // If all expressions are simple column references, use fast StreamingProjectionResult
        // Otherwise, use ExprMappedResult with pre-compiled expression evaluation
        let mut has_complex_expressions = false;
        let mut column_indices = Vec::with_capacity(stmt.columns.len());
        let mut output_columns = Vec::with_capacity(stmt.columns.len());

        // OPTIMIZATION: Pre-compute lowercase view column names once
        let view_columns_lower: Vec<String> =
            view_columns.iter().map(|c| c.to_lowercase()).collect();

        // First pass: check if all are simple column references and build indices
        for col in &stmt.columns {
            match col {
                Expression::Star(_) => {
                    // Expand all columns
                    for (idx, name) in view_columns.iter().enumerate() {
                        column_indices.push(idx);
                        output_columns.push(name.clone());
                    }
                }
                Expression::QualifiedStar(qs) => {
                    // Expand columns for specific table/alias
                    let qualifier_lower = qs.qualifier.to_lowercase();
                    let qualifier_len = qualifier_lower.len();
                    for (idx, col_lower) in view_columns_lower.iter().enumerate() {
                        // Inline prefix check: "qualifier." without format! allocation
                        if col_lower.len() > qualifier_len
                            && col_lower.starts_with(qualifier_lower.as_str())
                            && col_lower.as_bytes()[qualifier_len] == b'.'
                        {
                            column_indices.push(idx);
                            // Strip "qualifier." from the column name for the output
                            output_columns.push(view_columns[idx][qualifier_len + 1..].to_string());
                        }
                    }
                }
                Expression::Identifier(id) => {
                    // Simple column reference
                    output_columns.push(id.value.to_string());
                    let name_lower = &id.value_lower;
                    if let Some(idx) = view_columns
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(name_lower))
                    {
                        column_indices.push(idx);
                    } else {
                        return Err(Error::ColumnNotFound(id.value.to_string()));
                    }
                }
                Expression::Aliased(aliased) => {
                    output_columns.push(aliased.alias.value.to_string());
                    // Check if inner is a simple identifier
                    if let Expression::Identifier(id) = aliased.expression.as_ref() {
                        let name_lower = &id.value_lower;
                        if let Some(idx) = view_columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(name_lower))
                        {
                            column_indices.push(idx);
                        } else {
                            return Err(Error::ColumnNotFound(id.value.to_string()));
                        }
                    } else {
                        // Complex expression in alias - need ExprMappedResult
                        has_complex_expressions = true;
                    }
                }
                _ => {
                    // Any other expression type requires evaluation
                    has_complex_expressions = true;
                    // We'll compute the output column name later
                    let col_name = Self::get_expression_column_name(col);
                    output_columns.push(col_name);
                }
            }
        }

        // Apply projection based on complexity
        // OPTIMIZATION: ExprMappedResult owns the Evaluator and reuses it for each row,
        // avoiding 7 HashMap allocations per row that the closure-based approach had.
        let result: Box<dyn QueryResult> = if has_complex_expressions {
            // Rebuild output columns to handle Star expansion
            let mut final_output_columns = Vec::with_capacity(stmt.columns.len());
            for col in &stmt.columns {
                match col {
                    Expression::Star(_) => {
                        for name in &view_columns {
                            final_output_columns.push(name.clone());
                        }
                    }
                    Expression::QualifiedStar(qs) => {
                        let qualifier_lower = qs.qualifier.to_lowercase();
                        let qualifier_len = qualifier_lower.len();
                        // Use pre-computed lowercase columns from earlier
                        for (idx, col_lower) in view_columns_lower.iter().enumerate() {
                            // Inline prefix check: "qualifier." without format! allocation
                            if col_lower.len() > qualifier_len
                                && col_lower.starts_with(qualifier_lower.as_str())
                                && col_lower.as_bytes()[qualifier_len] == b'.'
                            {
                                // Strip "qualifier." from the column name for the output
                                final_output_columns
                                    .push(view_columns[idx][qualifier_len + 1..].to_string());
                            }
                        }
                    }
                    Expression::Aliased(aliased) => {
                        final_output_columns.push(aliased.alias.value.to_string());
                    }
                    _ => {
                        final_output_columns.push(Self::get_expression_column_name(col));
                    }
                }
            }

            Box::new(ExprMappedResult::with_context(
                result,
                stmt.columns.clone(),
                final_output_columns.clone(),
                ctx,
            )?)
        } else {
            // Simple column references only: use fast StreamingProjectionResult
            Box::new(StreamingProjectionResult::new(
                result,
                column_indices,
                output_columns.clone(),
            ))
        };

        // Rebuild output_columns if we used complex expressions
        let final_columns = if has_complex_expressions {
            let mut cols = Vec::with_capacity(stmt.columns.len());
            for col in &stmt.columns {
                match col {
                    Expression::Star(_) | Expression::QualifiedStar(_) => {
                        for name in &view_columns {
                            cols.push(name.clone());
                        }
                    }
                    Expression::Aliased(aliased) => {
                        cols.push(aliased.alias.value.to_string());
                    }
                    _ => {
                        cols.push(Self::get_expression_column_name(col));
                    }
                }
            }
            cols
        } else {
            output_columns
        };

        Ok((result, CompactArc::new(final_columns), false, None))
    }

    /// Execute a VALUES source (e.g., (VALUES (1, 'a'), (2, 'b')) AS t(col1, col2))
    fn execute_values_source(
        &self,
        values_source: &ValuesTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // classification is passed from caller to avoid redundant cache lookups

        // Determine column names
        let num_columns = if values_source.rows.is_empty() {
            0
        } else {
            values_source.rows[0].len()
        };
        if let Some((row_index, row)) = values_source
            .rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.len() != num_columns)
        {
            return Err(Error::InvalidArgument(format!(
                "VALUES row {} has {} columns; expected {}",
                row_index + 1,
                row.len(),
                num_columns
            )));
        }
        if !values_source.column_aliases.is_empty()
            && values_source.column_aliases.len() != num_columns
        {
            return Err(Error::InvalidArgument(format!(
                "VALUES source has {} columns but alias declares {}",
                num_columns,
                values_source.column_aliases.len()
            )));
        }

        // Get the table alias for qualified name resolution
        let table_alias = values_source
            .alias
            .as_ref()
            .map(|a| a.value.to_string())
            .unwrap_or_default();

        let column_names: Vec<String> = if !values_source.column_aliases.is_empty() {
            // Use provided column aliases
            values_source
                .column_aliases
                .iter()
                .map(|id| id.value.to_string())
                .collect()
        } else {
            // Generate default column names: column1, column2, ...
            (1..=num_columns).map(|i| format!("column{}", i)).collect()
        };

        // Build a map of both simple and qualified column names to indices
        let mut col_index_map = build_column_index_map(&column_names);

        // Also add qualified names (e.g., "v.id" for table alias "v")
        // OPTIMIZATION: Pre-compute lowercase table alias once outside the loop
        if !table_alias.is_empty() {
            let alias_lower = table_alias.to_lowercase();
            for (i, name) in column_names.iter().enumerate() {
                let name_lower = name.to_lowercase();
                let mut qualified = String::with_capacity(alias_lower.len() + 1 + name_lower.len());
                qualified.push_str(&alias_lower);
                qualified.push('.');
                qualified.push_str(&name_lower);
                col_index_map.insert(qualified, i);
            }
        }

        // OPTIMIZATION: Pre-create RowFilters outside the loop if WHERE clause exists
        let (where_filter, qualified_filter) = if let Some(ref where_clause) = stmt.where_clause {
            // Pre-compute qualified column names once
            let qualified_cols: Vec<String> = column_names
                .iter()
                .map(|c| format!("{}.{}", table_alias, c))
                .collect();

            // Create filters for simple and qualified column names
            let filter = RowFilter::new(where_clause, &column_names)?.with_context(ctx);
            let qual_filter = RowFilter::new(where_clause, &qualified_cols)?.with_context(ctx);

            (Some(filter), Some(qual_filter))
        } else {
            (None, None)
        };

        // Evaluate all rows
        let mut result_rows = RowVec::with_capacity(values_source.rows.len());
        let mut row_id = 0i64;
        for row_exprs in &values_source.rows {
            let mut row_values = Vec::with_capacity(row_exprs.len());
            for expr in row_exprs {
                let value = ExpressionEval::compile(expr, &[])?
                    .with_context(ctx)
                    .eval_slice(&Row::new())?;
                row_values.push(value);
            }
            let row = Row::from_values(row_values);

            // Apply WHERE clause filtering
            if let (Some(wf), Some(qf)) = (&where_filter, &qualified_filter) {
                // Try with simple column names first, then qualified
                if wf.matches_checked(&row)? || qf.matches_checked(&row)? {
                    result_rows.push((row_id, row));
                    row_id += 1;
                }
            } else {
                result_rows.push((row_id, row));
                row_id += 1;
            }
        }

        // Check if we need window functions
        if classification.has_window_functions {
            let result =
                self.execute_select_with_window_functions(stmt, ctx, &result_rows, &column_names)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Check if we need aggregation
        if classification.has_aggregation {
            let result =
                self.execute_select_with_aggregation(stmt, ctx, result_rows, &column_names)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Create the result - use CompactArc for column names
        let column_names_arc = CompactArc::new(column_names);
        let values_result =
            ExecutorResult::with_arc_columns(CompactArc::clone(&column_names_arc), result_rows);

        // If the SELECT has projections, apply them
        if stmt.columns.len() == 1
            && matches!(
                &stmt.columns[0],
                Expression::Star(_) | Expression::QualifiedStar(_)
            )
        {
            // SELECT * or t.* - return all columns
            return Ok((Box::new(values_result), column_names_arc, false, None));
        }

        // Apply column projection
        let mut projected_columns = Vec::new();
        let mut projected_rows = RowVec::new();
        let mut proj_row_id = 0i64;

        // Determine output columns
        for (i, col_expr) in stmt.columns.iter().enumerate() {
            match col_expr {
                Expression::Star(_) => {
                    projected_columns.extend(column_names_arc.iter().cloned());
                }
                Expression::QualifiedStar(_) => {
                    // For single table, t.* is equivalent to *
                    projected_columns.extend(column_names_arc.iter().cloned());
                }
                Expression::Aliased(a) => {
                    projected_columns.push(a.alias.value.to_string());
                }
                Expression::Identifier(id) => {
                    projected_columns.push(id.value.to_string());
                }
                Expression::QualifiedIdentifier(qi) => {
                    projected_columns.push(qi.name.value.to_string());
                }
                _ => {
                    projected_columns.push(format!("expr{}", i + 1));
                }
            }
        }

        // Build extended columns for evaluator - just use simple column names
        // The evaluator handles qualified references by stripping the qualifier
        let extended_columns = column_names_arc.to_vec();

        // OPTIMIZATION: Check if we need an evaluator for complex expressions
        let needs_evaluator = stmt.columns.iter().any(|c| {
            matches!(c, Expression::Aliased(_))
                || !matches!(
                    c,
                    Expression::Star(_)
                        | Expression::QualifiedStar(_)
                        | Expression::Identifier(_)
                        | Expression::QualifiedIdentifier(_)
                )
        });

        let mut proj_eval = if needs_evaluator {
            let mut eval = CompiledEvaluator::new(&self.function_registry);
            eval = eval.with_context(ctx);
            eval.init_columns(&extended_columns);
            Some(eval)
        } else {
            None
        };

        // OPTIMIZATION: Pre-compute qualified column lookups to avoid format! per row
        let mut qualified_col_indices: Vec<Option<usize>> = Vec::new();
        for col_expr in &stmt.columns {
            if let Expression::QualifiedIdentifier(qi) = col_expr {
                // Build qualified key once
                let mut key = String::with_capacity(
                    qi.qualifier.value_lower.len() + 1 + qi.name.value_lower.len(),
                );
                key.push_str(&qi.qualifier.value_lower);
                key.push('.');
                key.push_str(&qi.name.value_lower);
                qualified_col_indices.push(
                    col_index_map
                        .get(&key)
                        .copied()
                        .or_else(|| col_index_map.get(qi.name.value_lower.as_str()).copied()),
                );
            } else {
                qualified_col_indices.push(None);
            }
        }

        // Materialize and project
        // OPTIMIZATION: Pre-compute output size to avoid reallocation per row
        let output_cols = projected_columns.len();
        let mut result_box: Box<dyn QueryResult> = Box::new(values_result);
        while result_box.next() {
            let row = result_box.row();
            let mut new_values = Vec::with_capacity(output_cols);

            for (col_idx, col_expr) in stmt.columns.iter().enumerate() {
                match col_expr {
                    Expression::Star(_) | Expression::QualifiedStar(_) => {
                        // Extend with all values from the row
                        new_values.extend(row.iter().cloned());
                    }
                    Expression::Identifier(id) => {
                        // Use pre-computed lowercase
                        if let Some(&idx) = col_index_map.get(id.value_lower.as_str()) {
                            if let Some(val) = row.get(idx) {
                                new_values.push(val.clone());
                            } else {
                                new_values.push(Value::null_unknown());
                            }
                        } else {
                            new_values.push(Value::null_unknown());
                        }
                    }
                    Expression::QualifiedIdentifier(_) => {
                        // Use pre-computed index
                        if let Some(idx) = qualified_col_indices[col_idx] {
                            if let Some(val) = row.get(idx) {
                                new_values.push(val.clone());
                            } else {
                                new_values.push(Value::null_unknown());
                            }
                        } else {
                            new_values.push(Value::null_unknown());
                        }
                    }
                    Expression::Aliased(a) => {
                        // Evaluate the underlying expression
                        let eval = proj_eval.as_mut().unwrap();
                        eval.set_row_array(row);

                        // Check if expression contains EXISTS subqueries
                        let expr_to_eval = if Self::has_subqueries(&a.expression) {
                            // Check if it's correlated (references outer columns)
                            if Self::has_correlated_subqueries(&a.expression) {
                                // Build outer row context for correlated subquery
                                let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
                                    FxHashMap::default();
                                for (i, col_name) in extended_columns.iter().enumerate() {
                                    if let Some(value) = row.get(i) {
                                        outer_row_map.insert(
                                            CompactArc::from(col_name.to_lowercase().as_str()),
                                            value.clone(),
                                        );
                                    }
                                }
                                let correlated_ctx = ctx.with_outer_row(
                                    outer_row_map,
                                    CompactArc::new(extended_columns.clone()),
                                );
                                self.process_correlated_where(&a.expression, &correlated_ctx)?
                            } else {
                                self.process_where_subqueries(&a.expression, ctx)?
                            }
                        } else {
                            (*a.expression).clone()
                        };

                        let val = eval.evaluate(&expr_to_eval)?;
                        new_values.push(val);
                    }
                    other => {
                        // Evaluate the expression
                        let eval = proj_eval.as_mut().unwrap();
                        eval.set_row_array(row);

                        // Check if expression contains EXISTS subqueries
                        let expr_to_eval = if Self::has_subqueries(other) {
                            // Check if it's correlated (references outer columns)
                            if Self::has_correlated_subqueries(other) {
                                // Build outer row context for correlated subquery
                                let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
                                    FxHashMap::default();
                                for (i, col_name) in extended_columns.iter().enumerate() {
                                    if let Some(value) = row.get(i) {
                                        outer_row_map.insert(
                                            CompactArc::from(col_name.to_lowercase().as_str()),
                                            value.clone(),
                                        );
                                    }
                                }
                                let correlated_ctx = ctx.with_outer_row(
                                    outer_row_map,
                                    CompactArc::new(extended_columns.clone()),
                                );
                                self.process_correlated_where(other, &correlated_ctx)?
                            } else {
                                self.process_where_subqueries(other, ctx)?
                            }
                        } else {
                            other.clone()
                        };

                        let val = eval.evaluate(&expr_to_eval)?;
                        new_values.push(val);
                    }
                }
            }

            projected_rows.push((proj_row_id, Row::from_values(new_values)));
            proj_row_id += 1;
        }

        let projected_columns_arc = CompactArc::new(projected_columns);
        let final_result = ExecutorResult::with_arc_columns(
            CompactArc::clone(&projected_columns_arc),
            projected_rows,
        );
        Ok((Box::new(final_result), projected_columns_arc, false, None))
    }

    fn try_execute_certified_ordered_join_leaf(
        &self,
        source: &SimpleTableSource,
        ctx: &ExecutionContext,
        filter: Option<&Expression>,
    ) -> Result<Option<CertifiedJoinLeaf>> {
        let Some(requirement) = active_join_index_order_requirement() else {
            return Ok(None);
        };
        let relation_alias = source
            .alias
            .as_ref()
            .map_or(source.name.value_lower.as_str(), |alias| {
                alias.value_lower.as_str()
            });
        if relation_alias != requirement.qualifier {
            return Ok(None);
        }

        let transaction = self.engine.begin_transaction()?;
        let table = transaction.get_table(&source.name.value_lower)?;
        let schema = table.schema().clone();
        let Some((order_index, order_column)) = schema.find_column(&requirement.column) else {
            return Ok(None);
        };
        // The current certificate represents ASC NULLS LAST. A nullable index
        // has a storage ordering, but not necessarily the SQL NULL placement
        // requested by the statement, so it must fail closed here.
        if order_column.nullable {
            return Ok(None);
        }
        let fetch_limit = table.row_count_hint();
        let Some(rows) =
            table.collect_rows_ordered_by_index(&requirement.column, true, fetch_limit, 0)
        else {
            return Ok(None);
        };
        let source_columns: Vec<String> = schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let qualified_columns: Vec<String> = source_columns
            .iter()
            .map(|column| format!("{}.{}", relation_alias, column))
            .collect();
        let columns = CompactArc::new(source_columns);
        let mut result: Box<dyn QueryResult> = Box::new(ExecutorResult::with_arc_columns(
            CompactArc::clone(&columns),
            rows,
        ));
        if let Some(filter) = filter {
            let filter = RowFilter::new(filter, &qualified_columns)?.with_context(ctx);
            result = Box::new(DeferredFilteredResult::from_filter(result, filter));
        }
        result = Box::new(CertifiedOrderedResult::ascending_nulls_last(
            result,
            vec![order_index],
        ));
        Ok(Some((result, qualified_columns)))
    }

    /// Execute a table expression with optional filter pushdown
    /// This is used by JOIN to push WHERE predicates to individual table scans
    pub(crate) fn execute_table_expression_with_filter(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
        filter: Option<&Expression>,
    ) -> Result<(Box<dyn QueryResult>, Vec<String>)> {
        self.execute_table_expression_with_filter_projection(expr, ctx, filter, None)
    }

    /// Dependency projection is an I/O optimization for immutable cold data.
    /// Hot MVCC rows already exist as complete in-memory rows; projecting them
    /// at this leaf would allocate and clone values before the JOIN can consume
    /// them. Fail closed on metadata errors and let the normal scan report the
    /// authoritative error.
    fn should_project_join_leaf(
        &self,
        source: &SimpleTableSource,
        projection: Option<&[Expression]>,
    ) -> bool {
        if projection.is_none_or(<[Expression]>::is_empty) || source.as_of.is_some() {
            return false;
        }

        query_table_has_cold_segments(
            &self.engine,
            &self.active_transaction,
            &source.name.value_lower,
        )
    }

    fn execute_table_expression_with_filter_projection(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
        filter: Option<&Expression>,
        projection: Option<&[Expression]>,
    ) -> Result<(Box<dyn QueryResult>, Vec<String>)> {
        match expr {
            Expression::TableSource(ts) => {
                // Check if this is a CTE from context (for subqueries referencing outer CTEs)
                let table_name = &ts.name.value_lower;
                if let Some((columns, _, _)) = ctx.get_cte_by_lower(table_name) {
                    // Get the alias for column prefixing
                    let table_alias = ts
                        .alias
                        .as_ref()
                        .map(|a| a.value.clone())
                        .unwrap_or_else(|| ts.name.value.clone());
                    let qualified_columns: Vec<String> = columns
                        .iter()
                        // A CTE is a relation boundary: outer qualifiers bind
                        // to the CTE's result-column name, never to a source
                        // qualifier retained by its defining SELECT. Without
                        // stripping that inner qualifier, `v.id` cannot match
                        // a result labelled `source.id`, and an equality JOIN
                        // silently falls back to a cartesian residual path.
                        .map(|col| format!("{}.{}", table_alias, extract_base_column_name(col)))
                        .collect();

                    let shared_rows = ctx
                        .get_cte_materialized_rows_by_lower(table_name)
                        .expect("CTE disappeared from one immutable execution context");
                    let shared: Box<dyn QueryResult> =
                        Box::new(ExecutorResult::with_arc_columns_shared_rows(
                            CompactArc::clone(columns),
                            shared_rows,
                        ));
                    // Keep the materialized CTE relation shared. A pushed filter
                    // clones only rows that actually pass; an unfiltered JOIN
                    // clones rows only as the common operator graph consumes them,
                    // rather than copying the complete RowVec at this boundary.
                    let result: Box<dyn QueryResult> = if let Some(filter_expr) = filter {
                        let row_filter =
                            RowFilter::new(filter_expr, &qualified_columns)?.with_context(ctx);
                        Box::new(FilteredResult::from_filter(shared, row_filter))
                    } else {
                        shared
                    };

                    return Ok((result, qualified_columns));
                }

                // Check if this is actually a view (for JOINs that reference views)
                if let Some(view_def) = self.visible_view_lowercase(table_name)? {
                    // Check view depth to prevent stack overflow
                    let depth = ctx.view_depth();
                    if depth >= MAX_VIEW_DEPTH {
                        return Err(Error::InvalidArgument(format!(
                            "Maximum view nesting depth ({}) exceeded",
                            MAX_VIEW_DEPTH
                        )));
                    }

                    // Parse view query using cache (returns Arc<Statement>, no clone)
                    let view_stmt = self.parse_view_statement(&view_def.query)?;
                    let view_select = match view_stmt.as_ref() {
                        Statement::Select(s) => s,
                        _ => unreachable!("parse_view_statement validates this is a SELECT"),
                    };

                    // Execute with incremented depth
                    let nested_ctx = ctx.with_incremented_view_depth();
                    let result = self.execute_select(view_select, &nested_ctx)?;
                    let columns = result.columns().to_vec();

                    // Prefix column names with view alias (or view name if no alias)
                    let table_alias = ts
                        .alias
                        .as_ref()
                        .map(|a| a.value.clone())
                        .unwrap_or_else(|| ts.name.value.clone());
                    let qualified_columns: Vec<String> = columns
                        .iter()
                        // A VIEW is a relation boundary: outer qualifiers bind
                        // to the VIEW's result-column name, never to a source
                        // qualifier retained by its defining SELECT.
                        .map(|col| format!("{}.{}", table_alias, extract_base_column_name(col)))
                        .collect();
                    let result: Box<dyn QueryResult> = if let Some(filter_expr) = filter {
                        let row_filter =
                            RowFilter::new(filter_expr, &qualified_columns)?.with_context(ctx);
                        let mut materialized = Self::materialize_result(result)?;
                        row_filter.retain_checked(&mut materialized)?;
                        Box::new(ExecutorResult::new(columns, materialized))
                    } else {
                        result
                    };
                    return Ok((result, qualified_columns));
                }

                if let Some(ordered) =
                    self.try_execute_certified_ordered_join_leaf(ts, ctx, filter)?
                {
                    return Ok(ordered);
                }

                let columns = if self.should_project_join_leaf(ts, projection) {
                    projection.expect("projection eligibility checked").to_vec()
                } else {
                    vec![Expression::Star(StarExpression {
                        token: dummy_token("*", TokenType::Punctuator),
                    })]
                };
                let select_all = SelectStatement {
                    token: dummy_token("SELECT", TokenType::Keyword),
                    distinct: false,
                    distinct_on: vec![],
                    columns,
                    with: None,
                    table_expr: Some(Box::new(Expression::TableSource(ts.clone()))),
                    where_clause: filter.map(|f| Box::new(f.clone())),
                    group_by: GroupByClause::default(),
                    having: None,
                    window_defs: vec![],
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    set_operations: vec![],
                };
                // Get classification for the synthetic SELECT statement
                let classification = get_classification(&select_all);
                let (result, columns, _, _) =
                    self.execute_simple_table_scan(ts, &select_all, ctx, &classification)?;

                // Prefix column names with table alias (or table name if no alias)
                // This is needed for proper qualified identifier resolution in JOINs
                let table_alias = ts
                    .alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| ts.name.value.clone());

                let qualified_columns = columns
                    .iter()
                    .map(|col| format!("{}.{}", table_alias, col))
                    .collect();

                Ok((result, qualified_columns))
            }
            Expression::JoinSource(js) => {
                let columns = projection.map_or_else(
                    || {
                        vec![Expression::Star(StarExpression {
                            token: dummy_token("*", TokenType::Punctuator),
                        })]
                    },
                    <[Expression]>::to_vec,
                );
                let select_all = SelectStatement {
                    token: dummy_token("SELECT", TokenType::Keyword),
                    distinct: false,
                    distinct_on: vec![],
                    columns,
                    with: None,
                    table_expr: Some(Box::new(Expression::JoinSource(js.clone()))),
                    where_clause: filter.map(|expression| Box::new(expression.clone())),
                    group_by: GroupByClause::default(),
                    having: None,
                    window_defs: vec![],
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    set_operations: vec![],
                };
                // Get classification for the synthetic SELECT statement
                let classification = get_classification(&select_all);
                let (result, columns, _, _) =
                    self.execute_join_source(js, &select_all, ctx, &classification)?;
                let binding_columns = if let Some(projection) = projection {
                    projection
                        .iter()
                        .zip(columns.iter())
                        .map(|(expression, actual)| match expression {
                            Expression::QualifiedIdentifier(identifier) => identifier.to_string(),
                            _ => actual.clone(),
                        })
                        .collect()
                } else {
                    columns.to_vec()
                };
                Ok((result, binding_columns))
            }
            Expression::SubquerySource(ss) => {
                // Execute subquery with incremented depth to avoid creating new TimeoutGuard
                let subquery_ctx = ctx.with_incremented_query_depth();
                let result = self.execute_select(&ss.subquery, &subquery_ctx)?;
                let columns = result.columns().to_vec();

                // Prefix column names with subquery alias (required for proper ON condition resolution)
                // Without this, ON a.id = b.id cannot resolve qualified column names
                let qualified_columns: Vec<String> = if let Some(alias) = &ss.alias {
                    columns
                        .iter()
                        .map(|col| format!("{}.{}", alias.value, col))
                        .collect()
                } else {
                    columns.clone()
                };

                // Apply filter to subquery result if present
                // This is needed when WHERE clause conditions are pushed down to subquery sources
                if let Some(filter_expr) = filter {
                    // Use qualified_columns for filter column resolution since WHERE clause
                    // has qualified names like "ds.avg_salary" not just "avg_salary"
                    let row_filter =
                        RowFilter::new(filter_expr, &qualified_columns)?.with_context(ctx);
                    // Materialize and filter - materialized is RowVec = Vec<(i64, Row)>
                    let mut materialized = Self::materialize_result(result)?;
                    row_filter.retain_checked(&mut materialized)?;
                    let filtered_result: Box<dyn QueryResult> =
                        Box::new(super::result::ExecutorResult::new(columns, materialized));
                    return Ok((filtered_result, qualified_columns));
                }

                Ok((result, qualified_columns))
            }
            Expression::ValuesSource(vs) => {
                // Create a simple SELECT * statement to execute the VALUES
                let select_all = SelectStatement {
                    token: dummy_token("SELECT", TokenType::Keyword),
                    distinct: false,
                    distinct_on: vec![],
                    columns: vec![Expression::Star(StarExpression {
                        token: dummy_token("*", TokenType::Punctuator),
                    })],
                    with: None,
                    table_expr: Some(Box::new(Expression::ValuesSource(vs.clone()))),
                    where_clause: None,
                    group_by: GroupByClause::default(),
                    having: None,
                    window_defs: vec![],
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    set_operations: vec![],
                };
                // Get classification for the synthetic SELECT statement
                let classification = get_classification(&select_all);
                let (result, columns, _, _) =
                    self.execute_values_source(vs, &select_all, ctx, &classification)?;
                let columns = columns.to_vec();
                if let Some(filter_expr) = filter {
                    let row_filter = RowFilter::new(filter_expr, &columns)?.with_context(ctx);
                    let mut materialized = Self::materialize_result(result)?;
                    row_filter.retain_checked(&mut materialized)?;
                    return Ok((
                        Box::new(ExecutorResult::new(columns.clone(), materialized)),
                        columns,
                    ));
                }
                Ok((result, columns))
            }
            Expression::FunctionTableSource(tvf_source) => {
                Self::execute_tvf_for_join(tvf_source, ctx, filter, None)
            }
            _ => Err(Error::NotSupported(
                "Unsupported table expression type".to_string(),
            )),
        }
    }

    /// Execute a table expression with optional filter and row limit.
    /// For simple table sources, adds LIMIT to enable true early termination.
    /// This is optimized for Index Nested Loop joins where we need only enough rows
    /// to produce the requested LIMIT results.
    pub(crate) fn execute_table_expression_with_filter_limit(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
        filter: Option<&Expression>,
        row_limit: Option<usize>,
    ) -> Result<(Box<dyn QueryResult>, Vec<String>)> {
        // Handle FunctionTableSource with limit passthrough
        let tvf_source = match expr {
            Expression::FunctionTableSource(fts) => Some(fts.as_ref()),
            Expression::Aliased(aliased) => {
                if let Expression::FunctionTableSource(fts) = aliased.expression.as_ref() {
                    Some(fts.as_ref())
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(tvf_source) = tvf_source {
            return Self::execute_tvf_for_join(tvf_source, ctx, filter, row_limit);
        }

        // Extract TableSource from the expression (handles both direct and aliased)
        let (ts, custom_alias) = match expr {
            Expression::TableSource(ts) => (ts, None),
            Expression::Aliased(aliased) => {
                if let Expression::TableSource(ts) = aliased.expression.as_ref() {
                    (ts, Some(aliased.alias.value.clone()))
                } else {
                    // Not a table source, fall back to standard execution
                    return self.execute_table_expression_with_filter(expr, ctx, filter);
                }
            }
            _ => {
                // Not a table source, fall back to standard execution
                return self.execute_table_expression_with_filter(expr, ctx, filter);
            }
        };

        // Only optimize if we have a limit
        if let Some(limit) = row_limit {
            let table_name = ts.name.value_lower.as_str();

            // Skip if this is a CTE reference - CTEs are already materialized
            if ctx.get_cte_by_lower(table_name).is_some() {
                return self.execute_table_expression_with_filter(expr, ctx, filter);
            }

            // Skip if this is a view
            if self.visible_view_lowercase(table_name)?.is_some() {
                return self.execute_table_expression_with_filter(expr, ctx, filter);
            }

            // Create a SELECT * statement with WHERE clause AND LIMIT
            // This triggers the LIMIT pushdown optimization in execute_simple_table_scan
            let select_all = SelectStatement {
                token: dummy_token("SELECT", TokenType::Keyword),
                distinct: false,
                distinct_on: vec![],
                columns: vec![Expression::Star(StarExpression {
                    token: dummy_token("*", TokenType::Punctuator),
                })],
                with: None,
                table_expr: Some(Box::new(Expression::TableSource(ts.clone()))),
                where_clause: filter.map(|f| Box::new(f.clone())),
                group_by: GroupByClause::default(),
                having: None,
                window_defs: vec![],
                order_by: vec![],
                limit: Some(Box::new(Expression::IntegerLiteral(
                    radixdb_sql::ast::IntegerLiteral {
                        token: dummy_token(&limit.to_string(), TokenType::Integer),
                        value: limit as i64,
                    },
                ))),
                offset: None,
                set_operations: vec![],
            };
            // Get classification for the synthetic SELECT statement
            let classification = get_classification(&select_all);
            let (result, columns, _, _) =
                self.execute_simple_table_scan(ts, &select_all, ctx, &classification)?;

            // Prefix column names with table alias (or table name if no alias)
            // Use custom_alias from Aliased expression if provided
            let table_alias = custom_alias.unwrap_or_else(|| {
                ts.alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| ts.name.value.clone())
            });

            let qualified_columns: Vec<String> = columns
                .iter()
                .map(|col| format!("{}.{}", table_alias, col))
                .collect();

            return Ok((result, qualified_columns));
        }

        // Fall back to standard execution for other cases
        self.execute_table_expression_with_filter(expr, ctx, filter)
    }
}
