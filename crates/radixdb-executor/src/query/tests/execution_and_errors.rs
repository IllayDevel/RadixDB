    #[test]
    fn projected_index_join_stays_lazy_until_result_pull() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE lazy_parent (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE lazy_child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL REFERENCES lazy_parent(id)
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO lazy_parent VALUES (1, 'one'), (2, 'two')")
            .unwrap();
        executor
            .execute("INSERT INTO lazy_child VALUES (10, 1), (20, 2)")
            .unwrap();

        instrumentation::begin_join_execution_probe();
        let mut result = executor
            .execute(
                "SELECT c.id, p.payload
                 FROM lazy_child c
                 JOIN lazy_parent p ON c.parent_id = p.id
                 LIMIT 1",
            )
            .unwrap();
        let construction = instrumentation::end_join_execution_probe();
        assert_eq!(construction.operator_calls, 0);
        assert!(result.preserves_deferred_rows());

        instrumentation::begin_join_execution_probe();
        assert!(result.next());
        let row = result.take_deferred_row();
        assert_eq!(row.get(0), Some(&Value::Integer(10)));
        assert_eq!(row.get(1), Some(&Value::text("one")));
        drop(result);
        let execution = instrumentation::end_join_execution_probe();
        assert_eq!(execution.index_nested_loop_calls, 1);
        assert_eq!(execution.output_rows, 1);

        instrumentation::begin_join_execution_probe();
        let mut identity = executor
            .execute(
                "SELECT *
                 FROM lazy_child c
                 JOIN lazy_parent p ON c.parent_id = p.id
                 LIMIT 1",
            )
            .unwrap();
        assert_eq!(
            instrumentation::end_join_execution_probe().operator_calls,
            0
        );
        assert_eq!(identity.columns().len(), 4);
        instrumentation::begin_join_execution_probe();
        assert!(identity.next());
        assert_eq!(identity.row().len(), 4);
        drop(identity);
        assert_eq!(
            instrumentation::end_join_execution_probe().index_nested_loop_calls,
            1
        );
    }
    #[test]
    fn general_equality_join_materializes_only_build_before_result_pull() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE pull_left (id INTEGER PRIMARY KEY, join_key INTEGER)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE pull_right (
                    id INTEGER PRIMARY KEY,
                    join_key INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO pull_left VALUES (1, 10), (2, 20)")
            .unwrap();
        executor
            .execute("INSERT INTO pull_right VALUES (100, 10, 'ten'), (200, 20, 'twenty')")
            .unwrap();

        instrumentation::begin_join_execution_probe();
        let mut result = executor
            .execute(
                "SELECT l.id, r.payload
                 FROM pull_left l
                 JOIN pull_right r ON l.join_key = r.join_key",
            )
            .unwrap();
        assert_eq!(
            instrumentation::end_join_execution_probe().operator_calls,
            0,
            "the probe side must not be consumed while constructing the result"
        );
        assert!(result.preserves_deferred_rows());

        instrumentation::begin_join_execution_probe();
        assert!(result.next());
        assert_eq!(result.row().len(), 2);
        drop(result);
        let execution = instrumentation::end_join_execution_probe();
        assert_eq!(execution.hash_streaming_calls, 1);
        assert_eq!(execution.output_rows, 1);
    }

    #[test]
    fn two_edge_equality_chain_opens_without_materializing_prefix() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE chain_a (id INTEGER PRIMARY KEY, key_b INTEGER)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE chain_b (
                    id INTEGER PRIMARY KEY,
                    join_key INTEGER,
                    key_c INTEGER
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE chain_c (
                    id INTEGER PRIMARY KEY,
                    join_key INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO chain_a VALUES (1, 10), (2, 20)")
            .unwrap();
        executor
            .execute("INSERT INTO chain_b VALUES (100, 10, 1000), (200, 20, 2000)")
            .unwrap();
        executor
            .execute("INSERT INTO chain_c VALUES (10000, 1000, 'one'), (20000, 2000, 'two')")
            .unwrap();

        instrumentation::begin_join_execution_probe();
        let mut result = executor
            .execute(
                "SELECT a.id, c.payload
                 FROM chain_a a
                 JOIN chain_b b ON a.key_b = b.join_key
                 JOIN chain_c c ON b.key_c = c.join_key",
            )
            .unwrap();
        assert_eq!(
            instrumentation::end_join_execution_probe().operator_calls,
            0,
            "no binary edge may be completed before the final cursor is pulled"
        );
        assert!(result.preserves_deferred_rows());

        instrumentation::begin_join_execution_probe();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(1)));
        assert_eq!(result.row().get(1), Some(&Value::text("one")));
        drop(result);
        let execution = instrumentation::end_join_execution_probe();
        assert_eq!(execution.hash_streaming_calls, 2);
        assert_eq!(execution.output_rows, 2);
    }

    #[test]
    fn equality_chain_depth_sweep_keeps_first_row_lazy_and_width_bounded() {
        let executor = create_test_executor();
        const MAX_DEPTH: usize = 13;

        for depth in 0..=MAX_DEPTH {
            executor
                .execute(&format!(
                    "CREATE TABLE depth_{depth} (
                        id INTEGER PRIMARY KEY,
                        join_key INTEGER NOT NULL,
                        next_key INTEGER NOT NULL,
                        payload TEXT NOT NULL
                    )"
                ))
                .unwrap();
            let key = 10 + depth * 10;
            executor
                .execute(&format!(
                    "INSERT INTO depth_{depth} VALUES
                        (1, {key}, {}, 'v{depth}'),
                        (2, {}, {}, 'x{depth}')",
                    key + 10,
                    key + 1,
                    key + 11,
                ))
                .unwrap();
        }

        let mut one_edge_retained = None;
        for depth in [1_usize, 2, 4, 8, 10, 13] {
            let mut sql = "SELECT d0.id, ".to_string();
            sql.push_str(&format!("d{depth}.payload FROM depth_0 d0"));
            for edge in 1..=depth {
                sql.push_str(&format!(
                    " JOIN depth_{edge} d{edge} ON d{}.next_key = d{edge}.join_key",
                    edge - 1
                ));
            }
            sql.push_str(" LIMIT 1");

            let ctx = ExecutionContextBuilder::new()
                .join_hash_state_max_bytes(1024 * 1024)
                .build();
            instrumentation::begin_join_execution_probe();
            let mut result = executor.execute_with_context(&sql, &ctx).unwrap();
            let construction = instrumentation::end_join_execution_probe();
            assert_eq!(
                construction.operator_calls, 0,
                "depth {depth}: constructing the result must not pull any edge"
            );
            assert!(result.preserves_deferred_rows(), "depth {depth}");

            instrumentation::begin_join_execution_probe();
            assert!(result.next(), "depth {depth}");
            assert_eq!(result.row().get(0), Some(&Value::Integer(1)));
            assert_eq!(result.row().get(1), Some(&Value::text(format!("v{depth}"))));
            drop(result);
            let execution = instrumentation::end_join_execution_probe();
            assert_eq!(execution.operator_calls, depth as u64, "depth {depth}");
            assert_eq!(
                execution.operator_calls,
                execution
                    .hash_streaming_calls
                    .saturating_add(execution.hash_parallel_calls)
                    .saturating_add(execution.merge_calls)
                    .saturating_add(execution.nested_loop_calls)
                    .saturating_add(execution.index_nested_loop_calls)
                    .saturating_add(execution.batch_index_nested_loop_calls),
                "depth {depth}: physical operator buckets do not add up"
            );
            assert_eq!(execution.output_rows, depth as u64, "depth {depth}");
            assert_eq!(
                execution.owned_rows_constructed, 0,
                "depth {depth}: a JOIN edge materialized a full Row"
            );
            assert_eq!(
                execution.copied_values, 2,
                "depth {depth}: only the final two-column public row may copy values"
            );
            assert!(
                execution.copied_bytes > 0,
                "depth {depth}: final public row copy was not observed"
            );
            let source_open_calls = construction
                .source_open_calls
                .saturating_add(execution.source_open_calls);
            let source_rescans = construction
                .source_rescans
                .saturating_add(execution.source_rescans);
            assert_eq!(source_open_calls, depth as u64, "depth {depth}");
            assert_eq!(
                source_rescans, 0,
                "depth {depth}: an already traversed source was reopened"
            );
            assert_eq!(
                execution.lookup_calls, 0,
                "depth {depth}: hash-streaming chain unexpectedly issued lookup batches"
            );
            assert_eq!(
                execution.max_output_width, 2,
                "depth {depth}: intermediate rows widened beyond the final projection"
            );
            let peak = ctx.peak_join_memory_bytes();
            assert!(peak > 0, "depth {depth}");
            let per_edge = *one_edge_retained.get_or_insert(peak);
            assert!(
                peak <= per_edge.saturating_mul(depth),
                "depth {depth}: peak JOIN state grew faster than linearly: \
                 {peak} > {per_edge} * {depth}"
            );
            assert!(peak <= 1024 * 1024, "depth {depth}: common budget exceeded");
            assert_eq!(ctx.retained_join_memory_bytes(), 0, "depth {depth}");
            eprintln!(
                "depth={depth} max_width={} owned_rows={} copied_values={} copied_bytes={} source_opens={} source_rescans={} lookup_batches={} peak_bytes={peak}",
                execution.max_output_width,
                execution.owned_rows_constructed,
                execution.copied_values,
                execution.copied_bytes,
                source_open_calls,
                source_rescans,
                execution.lookup_calls,
            );
        }
    }

    #[test]
    fn blocking_post_join_clauses_do_not_receive_early_limit_pushdown() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE blocking_left (id INTEGER PRIMARY KEY, join_key INTEGER)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE blocking_right (
                    id INTEGER PRIMARY KEY,
                    join_key INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO blocking_left VALUES (1, 10), (2, 10), (3, 20)")
            .unwrap();
        executor
            .execute("INSERT INTO blocking_right VALUES (100, 10, 'a'), (200, 20, 'z')")
            .unwrap();
        executor
            .execute("CREATE TABLE blocking_fk (id INTEGER PRIMARY KEY, target_id INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO blocking_fk VALUES (1, 100), (2, 100), (3, 200)")
            .unwrap();

        let ordered = drain_rows(
            executor
                .execute(
                    "SELECT r.payload
                     FROM blocking_left l
                     JOIN blocking_right r ON l.join_key = r.join_key
                     ORDER BY r.payload DESC
                     LIMIT 1",
                )
                .unwrap(),
        );
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].get(0), Some(&Value::text("z")));

        let distinct = drain_rows(
            executor
                .execute(
                    "SELECT DISTINCT r.payload
                     FROM blocking_fk l
                     JOIN blocking_right r ON l.target_id = r.id
                     ORDER BY r.payload
                     LIMIT 2",
                )
                .unwrap(),
        );
        assert_eq!(distinct.len(), 2);
        assert_eq!(distinct[0].get(0), Some(&Value::text("a")));
        assert_eq!(distinct[1].get(0), Some(&Value::text("z")));

        let grouped = drain_rows(
            executor
                .execute(
                    "SELECT r.payload, COUNT(*)
                     FROM blocking_left l
                     JOIN blocking_right r ON l.join_key = r.join_key
                     GROUP BY r.payload
                     ORDER BY r.payload",
                )
                .unwrap(),
        );
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].get(0), Some(&Value::text("a")));
        assert_eq!(grouped[0].get(1), Some(&Value::Integer(2)));
        assert_eq!(grouped[1].get(0), Some(&Value::text("z")));
        assert_eq!(grouped[1].get(1), Some(&Value::Integer(1)));
    }

    #[test]
    fn ordered_complex_projection_consumes_one_pull_join_graph() {
        let executor = create_test_executor();
        const DEPTH: usize = 10;
        for depth in 0..=DEPTH {
            executor
                .execute(&format!(
                    "CREATE TABLE ordered_depth_{depth} (
                        id INTEGER PRIMARY KEY,
                        join_key INTEGER NOT NULL,
                        next_key INTEGER NOT NULL,
                        payload TEXT NOT NULL
                    )"
                ))
                .unwrap();
            let key = 100 + depth * 10;
            executor
                .execute(&format!(
                    "INSERT INTO ordered_depth_{depth} VALUES
                        (1, {key}, {}, 'a'),
                        (2, {}, {}, 'z')",
                    key + 10,
                    key + 1,
                    key + 11,
                ))
                .unwrap();
        }

        let mut sql = format!(
            "SELECT CASE WHEN d{DEPTH}.payload = 'a' THEN d0.id ELSE -1 END AS chosen \
             FROM ordered_depth_0 d0"
        );
        for edge in 1..=DEPTH {
            sql.push_str(&format!(
                " JOIN ordered_depth_{edge} d{edge} \
                 ON d{}.next_key = d{edge}.join_key",
                edge - 1
            ));
        }
        sql.push_str(&format!(" ORDER BY d{DEPTH}.payload DESC LIMIT 1"));

        let ctx = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(1024 * 1024)
            .build();
        instrumentation::begin_join_execution_probe();
        let rows = drain_rows(executor.execute_with_context(&sql, &ctx).unwrap());
        let execution = instrumentation::end_join_execution_probe();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Integer(-1)));
        assert_eq!(execution.hash_streaming_calls, DEPTH as u64);
        assert_eq!(execution.operator_calls, DEPTH as u64);
        assert_eq!(execution.output_rows, (DEPTH * 2) as u64);
        assert_eq!(execution.max_output_width, 2);
        assert_eq!(execution.owned_rows_constructed, 0);
        assert_eq!(execution.deferred_boundary_rows, 0);
        assert!(execution.deferred_rows >= (DEPTH * 2) as u64);
        assert_eq!(execution.top_n_calls, 1);
        assert_eq!(execution.top_n_input_rows, 2);
        assert_eq!(execution.top_n_output_rows, 1);
        assert_eq!(execution.top_n_peak_candidates, 1);
        assert!(execution.top_n_peak_bytes > 0);
        assert!(ctx.peak_join_memory_bytes() > 0);
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn non_equality_join_limit_offset_caps_final_outputs_not_preserved_input() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE limit_left (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("CREATE TABLE limit_right (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("INSERT INTO limit_left VALUES (1), (2)")
            .unwrap();
        executor
            .execute("INSERT INTO limit_right VALUES (2), (3), (4)")
            .unwrap();

        instrumentation::begin_join_execution_probe();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT l.id, r.id
                     FROM limit_left l
                     JOIN limit_right r ON l.id < r.id
                     LIMIT 2 OFFSET 2",
                )
                .unwrap(),
        );
        let execution = instrumentation::end_join_execution_probe();
        assert_eq!(rows.len(), 2);
        assert_eq!(execution.output_rows, 4);
    }

    #[test]
    fn left_join_nullable_side_filter_preserves_certified_index_order() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE recipients (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE recipient_settings (
                    recipient_id INTEGER PRIMARY KEY,
                    enabled INTEGER
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO recipients VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
            .unwrap();
        executor
            .execute("INSERT INTO recipient_settings VALUES (2, 0), (3, 1), (5, 0), (6, 1)")
            .unwrap();

        instrumentation::begin_join_execution_probe();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT r.id
                     FROM recipients r
                     LEFT JOIN recipient_settings s ON r.id = s.recipient_id
                     WHERE s.enabled IS NULL OR s.enabled = 1
                     ORDER BY r.id
                     LIMIT 3 OFFSET 1",
                )
                .unwrap(),
        );
        let execution = instrumentation::end_join_execution_probe();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(0), Some(&Value::Integer(3)));
        assert_eq!(rows[1].get(0), Some(&Value::Integer(4)));
        assert_eq!(rows[2].get(0), Some(&Value::Integer(6)));
        assert_eq!(execution.ordering_skip_calls, 1);
        assert_eq!(execution.top_n_calls, 0);
        assert_eq!(execution.deferred_boundary_rows, 0);
    }

    #[test]
    fn certified_root_index_order_survives_unique_join_chain() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE ordered_roots (
                    id INTEGER PRIMARY KEY,
                    first_id INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE ordered_first (
                    id INTEGER PRIMARY KEY,
                    second_id INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE ordered_second (
                    id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                )",
            )
            .unwrap();
        for id in 1..=100 {
            executor
                .execute(&format!(
                    "INSERT INTO ordered_second VALUES ({id}, 'payload-{id}')"
                ))
                .unwrap();
            executor
                .execute(&format!("INSERT INTO ordered_first VALUES ({id}, {id})"))
                .unwrap();
        }
        for id in (1..=20).rev() {
            executor
                .execute(&format!("INSERT INTO ordered_roots VALUES ({id}, {id})"))
                .unwrap();
        }

        instrumentation::begin_join_execution_probe();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT s.payload, r.id
                     FROM ordered_roots r
                     JOIN ordered_first f ON f.id = r.first_id
                     JOIN ordered_second s ON s.id = f.second_id
                     ORDER BY r.id
                     LIMIT 3 OFFSET 2",
                )
                .unwrap(),
        );
        let execution = instrumentation::end_join_execution_probe();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(1), Some(&Value::Integer(3)));
        assert_eq!(rows[1].get(1), Some(&Value::Integer(4)));
        assert_eq!(rows[2].get(1), Some(&Value::Integer(5)));
        assert_eq!(execution.operator_calls, 2);
        assert_eq!(execution.ordering_skip_calls, 1);
        assert_eq!(execution.top_n_calls, 0);
        assert_eq!(execution.deferred_boundary_rows, 0);

        let explain = drain_rows(
            executor
                .execute(
                    "EXPLAIN ANALYZE
                     SELECT s.payload, r.id
                     FROM ordered_roots r
                     JOIN ordered_first f ON f.id = r.first_id
                     JOIN ordered_second s ON s.id = f.second_id
                     ORDER BY r.id
                     LIMIT 3 OFFSET 2",
                )
                .unwrap(),
        );
        assert!(explain.iter().any(|row| {
            matches!(
                row.get(0),
                Some(Value::Text(line))
                    if line.contains("Physical JOIN Ordering: certified_index_order_skips=1")
            )
        }));
        assert!(!explain.iter().any(|row| {
            matches!(row.get(0), Some(Value::Text(line)) if line.contains("Physical JOIN Top-N"))
        }));
    }

    #[test]
    fn non_unique_join_edge_drops_index_order_certificate() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE non_unique_roots (
                    id INTEGER PRIMARY KEY,
                    group_id INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE non_unique_items (
                    id INTEGER PRIMARY KEY,
                    group_id INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute("CREATE INDEX non_unique_items_group_idx ON non_unique_items(group_id)")
            .unwrap();
        for id in (1..=5).rev() {
            executor
                .execute(&format!("INSERT INTO non_unique_roots VALUES ({id}, {id})"))
                .unwrap();
        }
        for group_id in 1..=5 {
            executor
                .execute(&format!(
                    "INSERT INTO non_unique_items VALUES ({}, {group_id}), ({}, {group_id})",
                    group_id * 10,
                    group_id * 10 + 1
                ))
                .unwrap();
        }

        instrumentation::begin_join_execution_probe();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT r.id, i.id
                     FROM non_unique_roots r
                     JOIN non_unique_items i ON i.group_id = r.group_id
                     ORDER BY r.id
                     LIMIT 3 OFFSET 1",
                )
                .unwrap(),
        );
        let execution = instrumentation::end_join_execution_probe();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(0), Some(&Value::Integer(1)));
        assert_eq!(rows[1].get(0), Some(&Value::Integer(2)));
        assert_eq!(rows[2].get(0), Some(&Value::Integer(2)));
        assert_eq!(execution.ordering_skip_calls, 0);
        assert_eq!(execution.top_n_calls, 1);
    }

    fn execute_sql_error(executor: &Executor, sql: &str) -> String {
        match executor.execute(sql) {
            Ok(mut result) => {
                while result.next() {}
                result
                    .last_error()
                    .unwrap_or_else(|| panic!("{sql}: expected query error"))
                    .to_string()
            }
            Err(error) => error.to_string(),
        }
    }

    fn parse_select_statement(sql: &str) -> SelectStatement {
        let mut statements = radixdb_sql::parse_sql(sql).unwrap();
        match statements.pop().unwrap() {
            radixdb_sql::Statement::Select(stmt) => stmt,
            other => panic!("{sql}: expected SELECT, got {other:?}"),
        }
    }

    fn planned_join_leaf_aliases(expression: &Expression) -> Vec<String> {
        fn collect(expression: &Expression, aliases: &mut Vec<String>) {
            match expression {
                Expression::JoinSource(join) => {
                    collect(&join.left, aliases);
                    collect(&join.right, aliases);
                }
                Expression::TableSource(source) => aliases.push(
                    source
                        .alias
                        .as_ref()
                        .unwrap_or(&source.name)
                        .value_lower()
                        .to_string(),
                ),
                other => panic!("expected table-only JOIN plan, got {other:?}"),
            }
        }

        let mut aliases = Vec::new();
        collect(expression, &mut aliases);
        aliases
    }

    fn seed_costed_reorder_tables(executor: &Executor) {
        executor
            .execute("CREATE TABLE reorder_a (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE reorder_b (
                    id INTEGER PRIMARY KEY,
                    a_id INTEGER NOT NULL REFERENCES reorder_a(id),
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE reorder_root (
                    id INTEGER PRIMARY KEY,
                    b_id INTEGER NOT NULL REFERENCES reorder_b(id)
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE reorder_optional (
                    id INTEGER PRIMARY KEY,
                    b_id INTEGER NOT NULL REFERENCES reorder_b(id)
                )",
            )
            .unwrap();

        let a_rows = (1..=64)
            .map(|id| format!("({id}, 'a-{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        let b_rows = (1..=64)
            .map(|id| format!("({id}, {id}, 'b-{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        let root_rows = (1..=64)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO reorder_a VALUES {a_rows}"))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO reorder_b VALUES {b_rows}"))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO reorder_root VALUES {root_rows}"))
            .unwrap();
        executor
            .execute("INSERT INTO reorder_optional VALUES (1, 1)")
            .unwrap();
    }

    fn text_key(row: &Row, index: usize) -> String {
        match row.get(index) {
            Some(Value::Text(value)) => value.to_string(),
            value => panic!("expected TEXT at {index}, got {value:?}"),
        }
    }

    fn integer_value(row: &Row, index: usize) -> i64 {
        match row.get(index) {
            Some(Value::Integer(value)) => *value,
            value => panic!("expected INTEGER at {index}, got {value:?}"),
        }
    }

    #[test]
    fn runtime_stats_is_one_versioned_bounded_metadata_row() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE runtime_probe (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO runtime_probe VALUES (1, 'resident')")
            .unwrap();

        let rows = drain_rows(executor.execute("PRAGMA RUNTIME_STATS").unwrap());
        assert_eq!(rows.len(), 1);
        let payload = text_key(&rows[0], 0);
        assert!(
            payload.len() < 64 * 1024,
            "runtime snapshot must stay bounded"
        );
        let snapshot: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(snapshot["format"], 2);
        assert_eq!(snapshot["lifecycle"], "ready");
        assert_eq!(snapshot["hot_tables"], 1);
        assert_eq!(snapshot["hot_rows"], 1);
        assert!(snapshot["hot_bytes"].as_u64().unwrap() > 0);
        assert_eq!(snapshot["owner_visit_limits"]["max_segments"], 16_384);
        assert!(snapshot["snapshot_nanos"].as_u64().unwrap() < 1_000_000_000);
    }

    #[test]
    fn page_cache_pragmas_expose_control_status_and_wait_contract() {
        let executor = create_test_executor();

        let rows = drain_rows(executor.execute("PRAGMA PAGE_CACHE_STATUS").unwrap());
        let snapshot: serde_json::Value = serde_json::from_str(&text_key(&rows[0], 0)).unwrap();
        assert_eq!(snapshot["state"], "disabled");
        assert_eq!(snapshot["requested_level"], 0);

        let rows = drain_rows(
            executor
                .execute("PRAGMA PAGE_CACHE_WARMUP_WAIT = 0")
                .unwrap(),
        );
        let snapshot: serde_json::Value = serde_json::from_str(&text_key(&rows[0], 0)).unwrap();
        assert_eq!(snapshot["state"], "disabled");

        let error = match executor.execute("PRAGMA PAGE_CACHE_WARMUP = 1") {
            Ok(_) => panic!("PAGE_CACHE_WARMUP value must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not accept values"));
    }

    #[test]
    fn generated_reference_join_integrity_checks_fail_closed() {
        let duplicate_targets = vec![
            Row::from_values(vec![Value::Integer(10)]),
            Row::from_values(vec![Value::Integer(10)]),
        ];
        let duplicate =
            validate_reference_target_uniqueness(&duplicate_targets, 0, "departments.id")
                .unwrap_err();
        assert_eq!(
            duplicate.navigation_code(),
            Some(NavigationErrorCode::TargetNotUnique)
        );

        let joined = RowVec::from_vec(vec![
            (
                0,
                Row::from_values(vec![Value::Integer(10), Value::Integer(10)]),
            ),
            (
                1,
                Row::from_values(vec![
                    Value::null(radixdb_core::DataType::Integer),
                    Value::null(radixdb_core::DataType::Integer),
                ]),
            ),
            (
                2,
                Row::from_values(vec![
                    Value::Integer(20),
                    Value::null(radixdb_core::DataType::Integer),
                ]),
            ),
        ]);
        let missing = validate_reference_join_matches(
            &joined,
            0,
            1,
            "employees.department_id",
            "departments.id",
        )
        .unwrap_err();
        assert_eq!(
            missing.navigation_code(),
            Some(NavigationErrorCode::TargetMissing)
        );
        assert!(!missing.to_string().contains("20"));
    }

    #[test]
    fn indexed_join_proves_full_composite_unique_cardinality_but_not_partial_unique() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE outer_keys (id INTEGER PRIMARY KEY, scope INTEGER, code INTEGER)",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE full_unique_target (
                    id INTEGER PRIMARY KEY,
                    scope INTEGER NOT NULL,
                    code INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute("CREATE INDEX full_unique_scope_idx ON full_unique_target(scope)")
            .unwrap();
        executor
            .execute("CREATE UNIQUE INDEX full_unique_pair_uq ON full_unique_target(scope, code)")
            .unwrap();

        let stmt = parse_select_statement(
            "SELECT o.id
             FROM outer_keys o
             JOIN full_unique_target t ON t.scope=o.scope AND t.code=o.code",
        );
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let (_, strategy, inner_column, _, lookup_unique) = executor
            .check_index_nested_loop_opportunity(
                &join.right,
                join.condition.as_deref(),
                &join.join_type,
                Some("o"),
                Some("t"),
            )
            .expect("composite UNIQUE edge must retain its indexed lookup candidate");
        assert!(matches!(strategy, IndexLookupStrategy::SecondaryIndex(_)));
        assert_eq!(inner_column, "scope");
        assert!(
            lookup_unique,
            "full composite UNIQUE proves 0..1 cardinality"
        );
        let explain = drain_rows(
            executor
                .execute(
                    "EXPLAIN SELECT o.id
                     FROM outer_keys o
                     JOIN full_unique_target t ON t.scope=o.scope AND t.code=o.code",
                )
                .unwrap(),
        );
        assert!(explain.iter().any(|row| {
            matches!(
                row.get(0),
                Some(Value::Text(line)) if line.contains("Join Lookup Cardinality: 0..1")
            )
        }));

        executor
            .execute(
                "CREATE TABLE partial_unique_target (
                    id INTEGER PRIMARY KEY,
                    scope INTEGER NOT NULL,
                    code INTEGER NOT NULL,
                    active BOOLEAN NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute("CREATE INDEX partial_unique_scope_idx ON partial_unique_target(scope)")
            .unwrap();
        executor
            .execute(
                "CREATE UNIQUE INDEX partial_unique_pair_uq
                 ON partial_unique_target(scope, code) WHERE active=TRUE",
            )
            .unwrap();
        let stmt = parse_select_statement(
            "SELECT o.id
             FROM outer_keys o
             JOIN partial_unique_target t ON t.scope=o.scope AND t.code=o.code",
        );
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let (_, _, _, _, lookup_unique) = executor
            .check_index_nested_loop_opportunity(
                &join.right,
                join.condition.as_deref(),
                &join.join_type,
                Some("o"),
                Some("t"),
            )
            .expect("non-unique constituent index remains a valid lookup candidate");
        assert!(
            !lookup_unique,
            "partial UNIQUE cannot prove whole-edge cardinality without implication"
        );
        let explain = drain_rows(
            executor
                .execute(
                    "EXPLAIN SELECT o.id
                     FROM outer_keys o
                     JOIN partial_unique_target t ON t.scope=o.scope AND t.code=o.code",
                )
                .unwrap(),
        );
        assert!(explain.iter().any(|row| {
            matches!(
                row.get(0),
                Some(Value::Text(line)) if line.contains("Join Lookup Cardinality: 0..N")
            )
        }));
    }

    #[test]
    fn r8_l01_batch_h_complex_topn_retains_only_requested_rows() {
        let executor = create_test_executor();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT * FROM generate_series(1, 100000) AS g(value) \
                     ORDER BY -value LIMIT 3 OFFSET 2",
                )
                .unwrap(),
        );
        let values: Vec<i64> = rows
            .iter()
            .map(|row| row.get(0).and_then(Value::as_int64).unwrap())
            .collect();
        assert_eq!(values, vec![99_998, 99_997, 99_996]);
    }

    #[test]
    fn test_show_tables() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .unwrap();

        let mut result = executor.execute("SHOW TABLES").unwrap();
        assert_eq!(result.columns().len(), 1);

        let mut found = false;
        while result.next() {
            let row = result.row();
            if let Some(Value::Text(name)) = row.get(0) {
                if &**name == "test" {
                    found = true;
                }
            }
        }
        assert!(found);
    }

    #[test]
    fn derived_aggregate_fallback_preserves_prefetched_numeric_row() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, bucket INTEGER, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 10, 5), (2, 10, 7), (3, 20, 11)")
            .unwrap();

        // The derived streaming optimization only specializes text group keys.
        // An INTEGER key therefore inspects the first row and falls back. The
        // fallback must reuse that source, including the inspected row.
        instrumentation::begin_derived_subquery_probe();
        let mut result = executor
            .execute(
                "SELECT bucket, COUNT(*) AS c
                 FROM (SELECT bucket, amount FROM events) q
                 GROUP BY bucket
                 ORDER BY bucket",
            )
            .unwrap();
        let mut groups = Vec::new();
        while result.next() {
            groups.push((
                result.row().get(0).cloned().unwrap(),
                result.row().get(1).cloned().unwrap(),
            ));
        }
        assert_eq!(
            groups,
            vec![
                (Value::Integer(10), Value::Integer(2)),
                (Value::Integer(20), Value::Integer(1)),
            ]
        );
        let probe = instrumentation::end_derived_subquery_probe();
        assert_eq!(
            probe.executes, 1,
            "rejected derived streaming path must continue the same source"
        );

        instrumentation::begin_derived_subquery_probe();
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*)
                 FROM (
                   SELECT bucket, COUNT(*) AS c, SUM(amount) AS s
                   FROM events
                   GROUP BY bucket
                   HAVING COUNT(*) > 0
                 ) q",
            ),
            2,
            "outer global aggregate consumes the inner source once"
        );
        let probe = instrumentation::end_derived_subquery_probe();
        assert_eq!(
            probe.executes, 1,
            "outer global aggregate over grouped derived source must execute the source once"
        );

        instrumentation::begin_derived_subquery_probe();
        let rows = drain_rows(
            executor
                .execute(
                    "SELECT bucket, c
                     FROM (
                       SELECT bucket, COUNT(*) AS c
                       FROM events
                       GROUP BY bucket
                     ) q
                     ORDER BY bucket",
                )
                .unwrap(),
        );
        assert_eq!(rows.len(), 2);
        let probe = instrumentation::end_derived_subquery_probe();
        assert_eq!(
            probe.executes, 1,
            "outer projection over grouped derived source must execute the source once"
        );
    }

    #[test]
    fn derived_aggregate_edge_cases_execute_source_once() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    bucket TEXT,
                    amount INTEGER
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO events VALUES
                    (1, 'alpha', 5),
                    (2, 'alpha', 5),
                    (3, 'alpha', 7),
                    (4, 'beta', 11),
                    (5, 'gamma', 13)",
            )
            .unwrap();

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c, SUM(amount) AS s
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket",
        );
        let mut groups = BTreeMap::new();
        for row in &rows {
            groups.insert(
                text_key(row, 0),
                (integer_value(row, 1), integer_value(row, 2)),
            );
        }
        assert_eq!(
            groups,
            BTreeMap::from([
                ("alpha".to_string(), (3, 17)),
                ("beta".to_string(), (1, 11)),
                ("gamma".to_string(), (1, 13)),
            ])
        );

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket, amount FROM events WHERE amount > 100) q
             GROUP BY bucket",
        );
        assert!(rows.is_empty());

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket
             HAVING COUNT(*) >= 2",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(text_key(&rows[0], 0), "alpha");
        assert_eq!(integer_value(&rows[0], 1), 3);

        let rows = execute_derived_once(
            &executor,
            "SELECT DISTINCT bucket, COUNT(*) AS c
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket",
        );
        assert_eq!(rows.len(), 3);

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(DISTINCT amount) AS c
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket",
        );
        let mut distinct_counts = BTreeMap::new();
        for row in &rows {
            distinct_counts.insert(text_key(row, 0), integer_value(row, 1));
        }
        assert_eq!(
            distinct_counts,
            BTreeMap::from([
                ("alpha".to_string(), 2),
                ("beta".to_string(), 1),
                ("gamma".to_string(), 1),
            ])
        );

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket
             ORDER BY bucket
             LIMIT 1
             OFFSET 1",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(text_key(&rows[0], 0), "beta");
        assert_eq!(integer_value(&rows[0], 1), 1);

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY bucket) AS rn
             FROM (SELECT bucket, amount FROM events) q
             GROUP BY bucket
             ORDER BY bucket",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(text_key(&rows[0], 0), "alpha");
        assert_eq!(integer_value(&rows[0], 2), 1);
        assert_eq!(text_key(&rows[2], 0), "gamma");
        assert_eq!(integer_value(&rows[2], 2), 3);
    }

    #[test]
    fn derived_aggregate_volatile_source_executes_once() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    bucket TEXT,
                    amount INTEGER
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO events VALUES
                    (1, 'alpha', 5),
                    (2, 'alpha', 7),
                    (3, 'beta', 11)",
            )
            .unwrap();

        let rows = execute_derived_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c, MIN(r) AS min_r, MAX(r) AS max_r
             FROM (SELECT bucket, RANDOM() AS r FROM events) q
             GROUP BY bucket",
        );
        let mut counts = BTreeMap::new();
        for row in &rows {
            counts.insert(text_key(row, 0), integer_value(row, 1));
            assert!(
                matches!(row.get(2), Some(Value::Float(_))),
                "MIN(RANDOM()) must remain a floating-point aggregate, got {:?}",
                row.get(2)
            );
            assert!(
                matches!(row.get(3), Some(Value::Float(_))),
                "MAX(RANDOM()) must remain a floating-point aggregate, got {:?}",
                row.get(3)
            );
        }
        assert_eq!(
            counts,
            BTreeMap::from([("alpha".to_string(), 2), ("beta".to_string(), 1)])
        );
    }

    #[test]
    fn derived_aggregate_inner_error_executes_source_once() {
        let executor = create_test_executor();

        let error = execute_derived_error_once(
            &executor,
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket FROM missing_events) q
             GROUP BY bucket",
        );
        assert!(
            error.contains("missing_events"),
            "unexpected derived source error: {error}"
        );
    }

    #[test]
    fn derived_aggregate_cancelled_source_executes_source_once() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, bucket TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 'alpha'), (2, 'beta')")
            .unwrap();

        let stmt = parse_select_statement(
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket FROM events) q
             GROUP BY bucket",
        );
        let classification = get_classification(&stmt);
        let ctx = ExecutionContext::new();
        ctx.cancel();

        instrumentation::begin_derived_subquery_probe();
        let error = match executor.execute_select_internal(&stmt, &ctx, &classification) {
            Ok(_) => panic!("cancelled derived source must return an error"),
            Err(error) => error,
        };
        let probe = instrumentation::end_derived_subquery_probe();

        assert_eq!(probe.executes, 1);
        assert!(
            matches!(error, radixdb_core::Error::QueryCancelled),
            "unexpected cancellation error: {error}"
        );
    }

    #[test]
    fn derived_aggregate_timed_out_source_executes_source_once() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, bucket TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 'alpha'), (2, 'beta')")
            .unwrap();

        let stmt = parse_select_statement(
            "SELECT bucket, COUNT(*) AS c
             FROM (SELECT bucket FROM events) q
             GROUP BY bucket",
        );
        let classification = get_classification(&stmt);
        let mut ctx = ExecutionContext::new();
        ctx.set_timeout_ms(1);
        let _timeout_guard = crate::context::TimeoutGuard::new(&ctx);
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        while !ctx.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(ctx.is_cancelled(), "test timeout guard did not fire");

        instrumentation::begin_derived_subquery_probe();
        let error = match executor.execute_select_internal(&stmt, &ctx, &classification) {
            Ok(_) => panic!("timed out derived source must return an error"),
            Err(error) => error,
        };
        let probe = instrumentation::end_derived_subquery_probe();

        assert_eq!(probe.executes, 1);
        assert!(
            matches!(error, radixdb_core::Error::QueryCancelled),
            "unexpected timeout cancellation error: {error}"
        );
    }
