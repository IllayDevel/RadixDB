    #[test]
    fn aggregate_left_join_uses_costed_batch_lookup_instead_of_full_inner_scan() {
        let directory = tempfile::tempdir().unwrap();
        let (executor, engine, _) =
            create_persistent_test_executor(&directory.path().join("aggregate_batch_join"));
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER,
                    bucket INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO parent
                 SELECT value, 'parent-' || value FROM generate_series(1, 4096)",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO child VALUES
                    (1, 1, 10),
                    (2, 2, 10),
                    (3, 9000, 10),
                    (4, 3, 20)",
            )
            .unwrap();
        engine.force_checkpoint_cycle().unwrap();

        let sql = "SELECT COUNT(p.payload)
                   FROM child c
                   LEFT JOIN parent p ON c.parent_id = p.id
                   WHERE c.bucket = 10";
        assert_eq!(
            scalar_count(&executor, sql),
            2,
            "COUNT(inner.payload) must retain LEFT JOIN NULL semantics"
        );

        let explain = drain_rows(executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap())
            .iter()
            .map(|row| text_key(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain.contains("batch_index_nested_loop=1"),
            "the aggregate consumer must not force a complete inner scan:\n{explain}"
        );
        assert!(
            explain.contains("hash_parallel=0"),
            "the selective unique edge unexpectedly fell back to hash scan:\n{explain}"
        );
        assert!(
            explain.contains("lookups=1, lookup_candidates=2"),
            "unrelated parent rows entered the aggregate JOIN work:\n{explain}"
        );

        drop(executor);
        engine.close_engine().unwrap();
    }

    #[test]
    fn filtered_join_cost_is_bounded_without_analyze_statistics() {
        let executor = create_test_executor();
        let stmt = parse_select_statement(
            "SELECT COUNT(p.payload)
             FROM child c
             LEFT JOIN parent p ON c.parent_id = p.id
             WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20",
        );
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };

        let estimated = executor.estimate_filtered_rows_with_upper_bound(
            &join.left,
            stmt.where_clause.as_deref(),
            833_333,
        );
        assert_eq!(
            estimated, 13_021,
            "missing ANALYZE statistics must not turn a selective cold edge into a full scan",
        );
        assert_eq!(
            executor.estimate_filtered_rows_with_upper_bound(
                &join.left,
                stmt.where_clause.as_deref(),
                u64::MAX,
            ),
            u64::MAX,
            "an unknown complex-source bound must remain fail-closed",
        );
    }

    #[test]
    fn join_leaf_dependency_projection_is_cold_only() {
        let directory = tempfile::tempdir().unwrap();
        let (executor, engine, _) =
            create_persistent_test_executor(&directory.path().join("cold_leaf_projection"));
        executor
            .execute(
                "CREATE TABLE cold_leaf (
                    id INTEGER PRIMARY KEY,
                    bucket INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    unused TEXT NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO cold_leaf VALUES
                    (1, 10, 'one', 'wide-one'),
                    (2, 20, 'two', 'wide-two')",
            )
            .unwrap();

        let stmt = parse_select_statement("SELECT c.payload FROM cold_leaf c WHERE c.bucket = 10");
        let source = stmt.table_expr.as_deref().unwrap();
        let context = ExecutionContext::new();

        let (hot_result, hot_columns) = executor
            .execute_table_expression_with_filter_projection(
                source,
                &context,
                stmt.where_clause.as_deref(),
                Some(&stmt.columns),
            )
            .unwrap();
        let hot_rows = Executor::materialize_result(hot_result).unwrap();
        assert_eq!(hot_columns.len(), 4, "hot rows must retain deferred width");
        assert_eq!(hot_rows[0].1.len(), 4);

        engine.force_checkpoint_cycle().unwrap();
        let (cold_result, cold_columns) = executor
            .execute_table_expression_with_filter_projection(
                source,
                &context,
                stmt.where_clause.as_deref(),
                Some(&stmt.columns),
            )
            .unwrap();
        let cold_rows = Executor::materialize_result(cold_result).unwrap();
        assert_eq!(cold_columns.as_slice(), &["c.payload".to_string()]);
        assert_eq!(cold_rows.len(), 1);
        assert_eq!(cold_rows[0].1.as_slice(), &[Value::text("one")]);

        drop(executor);
        engine.close_engine().unwrap();
    }

    #[test]
    fn batch_index_join_preserves_historical_secondary_key_snapshot() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let reader = Executor::new(Arc::clone(&engine));
        let writer = Executor::new(Arc::clone(&engine));
        writer
            .execute(
                "CREATE TABLE parent (
                    id INTEGER PRIMARY KEY,
                    lookup_key INTEGER NOT NULL UNIQUE,
                    payload TEXT NOT NULL
                )",
            )
            .unwrap();
        writer
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_key INTEGER NOT NULL)")
            .unwrap();
        writer
            .execute("INSERT INTO parent VALUES (10, 10010, 'before')")
            .unwrap();
        writer
            .execute(
                "INSERT INTO parent
                 SELECT value, value + 20000, 'unrelated' FROM generate_series(100, 1100)",
            )
            .unwrap();
        writer
            .execute("INSERT INTO child VALUES (1, 10010)")
            .unwrap();

        let sql = "SELECT c.id, p.payload FROM child c \
                   INNER JOIN parent p ON p.lookup_key = c.parent_key ORDER BY c.id";
        reader.execute("BEGIN ISOLATION LEVEL SNAPSHOT").unwrap();
        writer
            .execute("UPDATE parent SET lookup_key = 10011, payload = 'after' WHERE id = 10")
            .unwrap();

        let mut rows = reader.execute(sql).unwrap();
        assert!(rows.next());
        assert_eq!(rows.row().get(0), Some(&Value::Integer(1)));
        assert_eq!(rows.row().get(1), Some(&Value::text("before")));
        assert!(!rows.next());

        let mut explain = reader.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap();
        let mut plan = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                plan.push(line.to_string());
            }
        }
        assert!(
            plan.iter()
                .any(|line| line.contains("batch_index_nested_loop=1")),
            "snapshot transaction silently disabled BatchIndexNL: {plan:#?}"
        );

        reader.execute("ROLLBACK").unwrap();
        let mut rows = reader.execute(sql).unwrap();
        assert!(
            !rows.next(),
            "new statement must observe the key transition"
        );
        drop(reader);
        drop(writer);
        engine.close_engine().unwrap();
    }

    #[test]
    fn persistent_join_paths_retain_one_explicit_snapshot_across_cold_and_hot_rows() {
        let directory = tempfile::tempdir().unwrap();
        let (reader, engine, _) =
            create_persistent_test_executor(&directory.path().join("snapshot_join_insert"));
        let writer = Executor::new(Arc::clone(&engine));
        writer
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        writer
            .execute(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL UNIQUE REFERENCES parent(id)
                )",
            )
            .unwrap();
        writer
            .execute(
                "CREATE VIEW child_chain AS
                 SELECT c.id FROM child c INNER JOIN parent p ON p.id = c.parent_id",
            )
            .unwrap();
        writer.execute("INSERT INTO parent VALUES (1)").unwrap();
        writer.execute("INSERT INTO child VALUES (1, 1)").unwrap();
        engine.force_checkpoint_cycle().unwrap();

        // Leave one parent unmatched at the exact reader boundary. The
        // anti-join inner side must not open a newer standalone snapshot after
        // the matching child commits.
        writer.execute("INSERT INTO parent VALUES (2)").unwrap();

        reader.execute("BEGIN ISOLATION LEVEL SNAPSHOT").unwrap();
        assert_eq!(scalar_count(&reader, "SELECT COUNT(*) FROM child"), 1);
        assert_eq!(scalar_count(&reader, "SELECT COUNT(*) FROM parent"), 2);
        writer.execute("INSERT INTO child VALUES (2, 2)").unwrap();
        for id in 3..=16 {
            writer
                .execute(&format!("INSERT INTO parent VALUES ({id})"))
                .unwrap();
            writer
                .execute(&format!("INSERT INTO child VALUES ({id}, {id})"))
                .unwrap();
        }
        let checkpoint = engine.force_checkpoint_cycle();
        assert!(checkpoint.is_err(), "post-snapshot rows must remain hot");
        assert_eq!(scalar_count(&reader, "SELECT COUNT(*) FROM child"), 1);
        assert_eq!(scalar_count(&reader, "SELECT COUNT(*) FROM child_chain"), 1);
        assert_eq!(
            scalar_count(
                &reader,
                "SELECT COUNT(*) FROM parent p LEFT JOIN child c ON c.parent_id = p.id WHERE c.id IS NULL",
            ),
            1
        );
        reader.execute("ROLLBACK").unwrap();

        drop(reader);
        drop(writer);
        engine.close_engine().unwrap();
    }

    #[test]
    fn count_pk_semijoin_preserves_artifact_cold_mixed_tombstone_and_restart_semantics() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("count_pk_semijoin_v5");
        let (executor, engine, config) = create_persistent_test_executor(&db_path);
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, bucket INTEGER, payload TEXT)",
            )
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 'cold parent'), (20, 'cold parent')")
            .unwrap();
        executor
            .execute(
                "INSERT INTO child VALUES
                    (1, 10, 1, 'cold child'),
                    (2, 20, 1, 'cold child'),
                    (3, 99, 1, 'cold orphan')",
            )
            .unwrap();
        engine.force_checkpoint_cycle().unwrap();

        let sql = "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id WHERE c.bucket = 1";
        let left_count_sql = "SELECT COUNT(p.payload) FROM child c LEFT JOIN parent p \
                              ON c.parent_id = p.id WHERE c.bucket = 1";
        assert_eq!(scalar_count(&executor, sql), 2, "fully cold artifact-backed rows");
        assert_eq!(
            scalar_count(&executor, left_count_sql),
            2,
            "fully cold artifact-backed LEFT count"
        );

        // Leave one valid parent/child pair hot while the earlier pairs stay
        // descriptor-backed cold. This exercises mixed membership visibility.
        executor
            .execute("INSERT INTO parent VALUES (30, 'hot parent')")
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (4, 30, 1, 'hot child')")
            .unwrap();
        assert_eq!(scalar_count(&executor, sql), 3, "mixed hot+cold rows");
        assert_eq!(
            scalar_count(&executor, left_count_sql),
            3,
            "mixed hot+cold LEFT count"
        );

        // A committed deletion must make the cold parent disappear even though
        // the child remains visible in its cold segment.
        executor
            .execute("DELETE FROM parent WHERE id = 20")
            .unwrap();
        assert_eq!(
            scalar_count(&executor, sql),
            2,
            "committed parent tombstone"
        );
        assert_eq!(
            scalar_count(&executor, left_count_sql),
            2,
            "committed parent tombstone LEFT count"
        );

        engine.force_checkpoint_cycle().unwrap();
        drop(executor);
        engine.close_engine().unwrap();
        drop(engine);

        let reopened = Arc::new(create_composed_test_engine(config));
        reopened.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&reopened));
        assert_eq!(scalar_count(&executor, sql), 2, "restart/reopen");
        assert_eq!(
            scalar_count(&executor, left_count_sql),
            2,
            "restart/reopen LEFT count"
        );
        drop(executor);
        reopened.close_engine().unwrap();
    }

    #[test]
    fn count_pk_semijoin_honors_long_lived_snapshot_across_parent_delete() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let reader = Executor::new(Arc::clone(&engine));
        let writer = Executor::new(Arc::clone(&engine));
        writer
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        writer
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap();
        writer.execute("INSERT INTO parent VALUES (10)").unwrap();
        writer.execute("INSERT INTO child VALUES (1, 10)").unwrap();

        let sql = "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id";
        reader.execute("BEGIN ISOLATION LEVEL SNAPSHOT").unwrap();
        writer.execute("DELETE FROM parent WHERE id = 10").unwrap();

        assert_eq!(
            scalar_count(&reader, sql),
            1,
            "the reader transaction must retain its pre-delete snapshot"
        );
        reader.execute("ROLLBACK").unwrap();
        assert_eq!(
            scalar_count(&reader, sql),
            0,
            "a new snapshot must observe the committed parent deletion"
        );

        drop(reader);
        drop(writer);
        engine.close_engine().unwrap();
    }

    #[test]
    fn count_pk_semijoin_matches_general_join_on_deterministic_random_datasets() {
        // The residual `c.id = c.id` is deliberately SQL-neutral because id
        // is the non-null PK. It prevents the narrow rewrite, so it gives us
        // a differential oracle against the established general JOIN path.
        for seed in 0_u64..32 {
            let executor = create_test_executor();
            executor
                .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT)")
                .unwrap();
            executor
                .execute(
                    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, bucket INTEGER)",
                )
                .unwrap();

            let mut state = seed.wrapping_add(1);
            let mut next = || {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                state
            };
            for parent_id in 1_i64..=12 {
                if next() % 3 != 0 {
                    executor
                        .execute(&format!(
                            "INSERT INTO parent VALUES ({parent_id}, 'parent-{parent_id}')"
                        ))
                        .unwrap();
                }
            }
            for child_id in 1_i64..=48 {
                let bucket = (next() % 4) as i64;
                let parent = match next() % 8 {
                    0 => "NULL".to_string(),
                    // Includes out-of-range and missing keys as well as
                    // duplicates, because parent_id is independently drawn.
                    _ => ((next() % 18) as i64 + 1).to_string(),
                };
                executor
                    .execute(&format!(
                        "INSERT INTO child VALUES ({child_id}, {parent}, {bucket})"
                    ))
                    .unwrap();
            }

            for bucket in 0..4 {
                let fast = scalar_count(
                    &executor,
                    &format!(
                        "SELECT COUNT(*) FROM child c INNER JOIN parent p \
                         ON c.parent_id = p.id WHERE c.bucket = {bucket}"
                    ),
                );
                let fallback = scalar_count(
                    &executor,
                    &format!(
                        "SELECT COUNT(*) FROM child c INNER JOIN parent p \
                         ON c.parent_id = p.id AND c.id = c.id WHERE c.bucket = {bucket}"
                    ),
                );
                assert_eq!(fast, fallback, "seed={seed}, bucket={bucket}");
            }
        }
    }

    #[test]
    fn explain_labels_count_pk_semijoin_only_for_the_eligible_shape() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, tag INTEGER)")
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, tag INTEGER)")
            .unwrap();

        let mut explain = executor
            .execute(
                "EXPLAIN SELECT COUNT(*)
                 FROM child c INNER JOIN parent p ON c.parent_id = p.id",
            )
            .unwrap();
        let mut lines = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                lines.push(line.to_string());
            }
        }
        assert!(lines
            .iter()
            .any(|line| line.contains("Join Access Path: join.count_pk_semijoin")));
        assert!(lines.iter().any(|line| {
            line.contains("Join Projection Boundary: join.count_pk_semijoin.scalar_count")
        }));
        assert!(lines.iter().any(|line| {
            line.contains("Child Projection: exact filter dependencies + parent join key")
        }));
        assert!(lines.iter().any(|line| {
            line.contains("Parent Membership: INTEGER PRIMARY KEY, metadata-only bounded batches")
        }));

        for sql in [
            "EXPLAIN SELECT COUNT(*) FROM child c INNER JOIN parent p \
             ON c.parent_id = p.id AND c.tag = p.tag",
            "EXPLAIN SELECT COUNT(*) FROM child c INNER JOIN parent p \
             ON c.parent_id = p.id WHERE p.tag = 1",
            "EXPLAIN SELECT COUNT(*) FROM child c LEFT JOIN parent p \
             ON c.parent_id = p.id",
            "EXPLAIN SELECT COUNT(DISTINCT c.parent_id) FROM child c INNER JOIN parent p \
             ON c.parent_id = p.id",
            "EXPLAIN SELECT c.tag, COUNT(*) FROM child c INNER JOIN parent p \
             ON c.parent_id = p.id GROUP BY c.tag",
            "EXPLAIN SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.tag = p.tag",
        ] {
            let mut explain = executor.execute(sql).unwrap();
            let mut lines = Vec::new();
            while explain.next() {
                if let Some(Value::Text(line)) = explain.row().get(0) {
                    lines.push(line.to_string());
                }
            }
            assert!(
                !lines
                    .iter()
                    .any(|line| line.contains("join.count_pk_semijoin")),
                "ineligible query chose count PK semi-join: {sql}"
            );
        }
    }

    #[test]
    fn count_pk_semijoin_returns_zero_for_empty_child_or_parent() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap();
        let sql = "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id";
        assert_eq!(scalar_count(&executor, sql), 0, "both tables empty");

        executor.execute("INSERT INTO parent VALUES (10)").unwrap();
        assert_eq!(scalar_count(&executor, sql), 0, "child empty");
        executor
            .execute("DELETE FROM parent WHERE id = 10")
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (1, 10)")
            .unwrap();
        assert_eq!(scalar_count(&executor, sql), 0, "parent empty");
    }

    #[test]
    fn ineligible_count_join_forms_keep_general_join_semantics() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, tag INTEGER)")
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, tag INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 1), (20, 2)")
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (1, 10, 1), (2, 10, 9), (3, 99, 1)")
            .unwrap();

        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c INNER JOIN parent p \
                 ON c.parent_id = p.id WHERE p.tag = 1",
            ),
            2,
            "parent-side predicate must be evaluated by the fallback"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
            ),
            3,
            "LEFT JOIN preserves the orphan child"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(DISTINCT c.parent_id) FROM child c INNER JOIN parent p \
                 ON c.parent_id = p.id",
            ),
            1,
            "COUNT(DISTINCT) is not reduced to row multiplicity"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.tag = p.tag",
            ),
            2,
            "non-PK equality uses the general join"
        );

        let mut result = executor
            .execute(
                "SELECT c.tag, COUNT(*) FROM child c INNER JOIN parent p \
                 ON c.parent_id = p.id GROUP BY c.tag ORDER BY c.tag",
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
                (Value::Integer(1), Value::Integer(1)),
                (Value::Integer(9), Value::Integer(1)),
            ]
        );
    }

    #[test]
    fn test_memory_filter_limit_simple_projection_returns_projected_rows() {
        let executor = create_test_executor();

        executor
            .execute(
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    value INTEGER,
                    status TEXT,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (1, 10, 'ok', 'a')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (2, 30, 'ok', 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (3, 40, 'no', 'c')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (4, 50, 'ok', 'd')")
            .unwrap();

        let mut result = executor
            .execute(
                "SELECT value AS v
                 FROM items
                 WHERE status = 'ok' AND value + 1 > 20
                 LIMIT 2",
            )
            .unwrap();

        assert_eq!(result.columns(), &["v".to_string()]);

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(30), Value::Integer(50)]);
    }

    #[test]
    fn test_filtered_expression_projection_scan_plan_prunes_unused_columns() {
        let executor = create_test_executor();
        let all_columns = vec![
            "id".to_string(),
            "value".to_string(),
            "status".to_string(),
            "payload".to_string(),
        ];
        let statements = radixdb_sql::parse_sql(
            "SELECT value + 1 AS next_value FROM items WHERE status = 'ok' AND id > 1",
        )
        .unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };
        let output_columns = executor.get_output_column_names(&stmt.columns, &all_columns, None);

        let plan = executor
            .build_filtered_expression_projection_scan_plan(
                stmt.where_clause.as_ref().unwrap(),
                &stmt.columns,
                &output_columns,
                &all_columns,
            )
            .unwrap();

        assert_eq!(plan.scan_indices, vec![0, 1, 2]);
        assert_eq!(
            plan.scan_columns,
            vec!["id".to_string(), "value".to_string(), "status".to_string()]
        );
        assert!(plan.output_indices_in_scan.is_empty());
        assert_eq!(plan.output_columns, vec!["next_value".to_string()]);
    }

    #[test]
    fn test_memory_filter_limit_expression_projection_returns_projected_rows() {
        let executor = create_test_executor();

        executor
            .execute(
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    value INTEGER,
                    status TEXT,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (1, 10, 'ok', 'a')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (2, 30, 'ok', 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (3, 40, 'no', 'c')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, status, payload) VALUES (4, 50, 'ok', 'd')")
            .unwrap();

        let mut result = executor
            .execute(
                "SELECT value + 1 AS next_value
                 FROM items
                 WHERE status = 'ok' AND value + 1 > 20
                 LIMIT 2",
            )
            .unwrap();

        assert_eq!(result.columns(), &["next_value".to_string()]);

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(31), Value::Integer(51)]);
    }

    #[test]
    fn costed_inner_reorder_selects_filtered_root_independent_of_textual_order() {
        let executor = create_test_executor();
        seed_costed_reorder_tables(&executor);
        let sql = "SELECT r.id, b.id, a.id
                   FROM reorder_a a
                   INNER JOIN reorder_b b ON b.a_id = a.id
                   INNER JOIN reorder_root r ON r.b_id = b.id
                   WHERE r.id = 64";
        let stmt = parse_select_statement(sql);
        let classification = get_classification(&stmt);
        assert_eq!(classification.reorderable_join_count, 2);
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let planned = executor
            .plan_join_table_expression(join, &stmt, &classification)
            .unwrap();
        assert_eq!(
            planned_join_leaf_aliases(&planned),
            ["r", "b", "a"],
            "physical root must follow estimated cardinality, not SQL text"
        );

        let rows = drain_rows(executor.execute(sql).unwrap());
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].as_slice(),
            &[Value::Integer(64), Value::Integer(64), Value::Integer(64)]
        );

        let explain = drain_rows(executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap())
            .iter()
            .map(|row| text_key(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain.contains("Physical JOIN Costed Order[0]: r -> b -> a"),
            "{explain}"
        );
        assert!(
            explain.contains("table/column statistics + selectivity + distinctness + index availability + projected width + LIMIT"),
            "{explain}"
        );
        assert!(
            explain.contains("Physical JOIN Estimate Error[0]"),
            "{explain}"
        );
        assert_eq!(
            explain.matches("Physical JOIN Costed Order[").count(),
            1,
            "nested left-deep prefixes are not independent planning components:\n{explain}"
        );
    }

    #[test]
    fn costed_inner_reorder_keeps_indexed_self_join_key_producer_as_root() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE self_users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE self_members (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    left_at INTEGER
                )",
            )
            .unwrap();
        executor
            .execute("CREATE INDEX self_members_user_idx ON self_members(user_id)")
            .unwrap();
        executor
            .execute(
                "CREATE INDEX self_members_conversation_idx
                 ON self_members(conversation_id)",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO self_users VALUES
                 (1, 'u1'), (2, 'u2'), (3, 'u3'), (4, 'u4')",
            )
            .unwrap();

        let members = (1i64..=1024)
            .map(|id| {
                let conversation_id = (id + 3) / 4;
                let user_id = if conversation_id <= 8 && id % 4 == 1 {
                    1
                } else {
                    2 + id % 3
                };
                format!("({id}, {conversation_id}, {user_id}, NULL)")
            })
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO self_members VALUES {members}"))
            .unwrap();

        let sql = "SELECT member.id, person.name
                   FROM self_members own
                   INNER JOIN self_members member
                     ON member.conversation_id = own.conversation_id
                   INNER JOIN self_users person ON person.id = member.user_id
                   WHERE own.user_id = 1 AND own.left_at IS NULL";
        let stmt = parse_select_statement(sql);
        let classification = get_classification(&stmt);
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let planned = executor
            .plan_join_table_expression(join, &stmt, &classification)
            .unwrap();
        assert_eq!(
            planned_join_leaf_aliases(&planned),
            ["own", "member", "person"],
            "the indexed self-join filter must remain the conversation-key producer"
        );

        let explain = drain_rows(executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap())
            .iter()
            .map(|row| text_key(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain.contains("Physical JOIN Costed Order[0]: own -> member -> person"),
            "{explain}"
        );
        assert!(
            explain.contains("batch_index_nested_loop=2"),
            "the self edge and terminal PK edge must use bounded key batches:\n{explain}"
        );
    }

    #[test]
    fn self_join_key_batch_preserves_multiplicity_and_ignores_unrelated_rows() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE batch_members (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    left_at INTEGER
                )",
            )
            .unwrap();
        executor
            .execute("CREATE INDEX batch_members_user_idx ON batch_members(user_id)")
            .unwrap();
        executor
            .execute(
                "CREATE INDEX batch_members_conversation_idx
                 ON batch_members(conversation_id)",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO batch_members VALUES
                 (1, 10, 1, NULL), (2, 10, 1, NULL),
                 (3, 10, 2, NULL), (4, 10, 3, NULL),
                 (5, 20, 1, NULL), (6, 20, 4, NULL)",
            )
            .unwrap();

        let sql = "SELECT member.id
                   FROM batch_members own
                   INNER JOIN batch_members member
                     ON member.conversation_id = own.conversation_id
                   WHERE own.user_id = 1 AND own.left_at IS NULL
                   ORDER BY member.conversation_id, member.id";
        let expected = vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 6];
        let ids = drain_rows(executor.execute(sql).unwrap())
            .iter()
            .map(|row| match row.get(0) {
                Some(Value::Integer(id)) => *id,
                value => panic!("expected member id, got {value:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids, expected,
            "lookup-key dedup must not imply SQL DISTINCT"
        );

        let unrelated = (0..1000)
            .map(|offset| {
                let id = 10_000 + offset;
                let conversation_id = 1_000 + offset / 2;
                let user_id = 100 + offset % 7;
                format!("({id}, {conversation_id}, {user_id}, NULL)")
            })
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO batch_members VALUES {unrelated}"))
            .unwrap();

        let ids_after_growth = drain_rows(executor.execute(sql).unwrap())
            .iter()
            .map(|row| match row.get(0) {
                Some(Value::Integer(id)) => *id,
                value => panic!("expected member id, got {value:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids_after_growth, expected);

        let explain = drain_rows(executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap())
            .iter()
            .map(|row| text_key(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(explain.contains("batch_index_nested_loop=1"), "{explain}");
        assert!(
            explain.contains(
                "Physical JOIN Key Batches: key_rows=3, distinct_keys=2, repeated_keys_eliminated=1"
            ),
            "{explain}"
        );
        assert!(
            explain.contains("candidates=6, lookups=1, lookup_candidates=6"),
            "unrelated conversations must not enter the physical candidate set:\n{explain}"
        );
    }

    #[test]
    fn costed_inner_reorder_does_not_change_select_star_column_order() {
        let executor = create_test_executor();
        seed_costed_reorder_tables(&executor);
        let sql = "SELECT *
                   FROM reorder_a a
                   INNER JOIN reorder_b b ON b.a_id = a.id
                   INNER JOIN reorder_root r ON r.b_id = b.id
                   WHERE r.id = 64";

        let mut result = executor.execute(sql).unwrap();
        assert!(result.next());
        assert_eq!(
            result.row().as_slice(),
            &[
                Value::Integer(64),
                Value::Text("a-64".into()),
                Value::Integer(64),
                Value::Integer(64),
                Value::Text("b-64".into()),
                Value::Integer(64),
                Value::Integer(64),
            ]
        );
        assert!(!result.next());

        let explain = drain_rows(executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap())
            .iter()
            .map(|row| text_key(row, 0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !explain.contains("Physical JOIN Costed Order["),
            "SELECT * must preserve textual JOIN column order:\n{explain}"
        );
    }

    #[test]
    fn costed_inner_reorder_reports_extreme_root_estimate_miss() {
        let executor = create_test_executor();
        seed_costed_reorder_tables(&executor);
        let explain = drain_rows(
            executor
                .execute(
                    "EXPLAIN ANALYZE
                     SELECT r.id, b.id, a.id
                     FROM reorder_a a
                     INNER JOIN reorder_b b ON b.a_id = a.id
                     INNER JOIN reorder_root r ON r.b_id = b.id
                     WHERE r.b_id > 0 AND r.id > 0",
                )
                .unwrap(),
        )
        .iter()
        .map(|row| text_key(row, 0))
        .collect::<Vec<_>>()
        .join("\n");
        assert!(
            explain.contains("diagnostic=join.estimate_extreme_miss"),
            "{explain}"
        );
    }

    #[test]
    fn costed_inner_reorder_prices_index_and_projected_width() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE cost_root (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE cost_wide (
                    id INTEGER PRIMARY KEY,
                    root_id INTEGER NOT NULL REFERENCES cost_root(id),
                    p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, p5 TEXT, p6 TEXT
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE cost_narrow (
                    id INTEGER PRIMARY KEY,
                    root_id INTEGER NOT NULL UNIQUE REFERENCES cost_root(id),
                    payload TEXT
                )",
            )
            .unwrap();

        let roots = (1..=512)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        let wide = (1..=512)
            .map(|id| {
                format!(
                    "({id}, {id}, 'p1-{id}', 'p2-{id}', 'p3-{id}', 'p4-{id}', 'p5-{id}', 'p6-{id}')"
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let narrow = (1..=512)
            .map(|id| format!("({id}, {id}, 'n-{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO cost_root VALUES {roots}"))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO cost_wide VALUES {wide}"))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO cost_narrow VALUES {narrow}"))
            .unwrap();

        let stmt = parse_select_statement(
            "SELECT r.id, w.p1, w.p2, w.p3, w.p4, w.p5, w.p6, n.payload
             FROM cost_root r
             INNER JOIN cost_wide w ON w.root_id = r.id
             INNER JOIN cost_narrow n ON n.root_id = r.id
             WHERE r.id = 1",
        );
        let classification = get_classification(&stmt);
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let planned = executor
            .plan_join_table_expression(join, &stmt, &classification)
            .unwrap();
        assert_eq!(
            planned_join_leaf_aliases(&planned),
            ["r", "n", "w"],
            "the indexed narrow edge must precede the wide scan edge"
        );
    }

    #[test]
    fn costed_inner_reorder_stays_inside_left_join_barrier() {
        let executor = create_test_executor();
        seed_costed_reorder_tables(&executor);
        let sql = "SELECT r.id, b.id, a.id, optional.id
                   FROM reorder_a a
                   INNER JOIN reorder_b b ON b.a_id = a.id
                   INNER JOIN reorder_root r ON r.b_id = b.id
                   LEFT JOIN reorder_optional optional ON optional.b_id = b.id
                   WHERE r.id = 64";
        let stmt = parse_select_statement(sql);
        let classification = get_classification(&stmt);
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let planned = executor
            .plan_join_table_expression(join, &stmt, &classification)
            .unwrap();
        let Expression::JoinSource(barrier) = &planned else {
            panic!("LEFT barrier must remain the plan root");
        };
        assert_eq!(barrier.join_type.to_uppercase(), "LEFT");
        assert_eq!(planned_join_leaf_aliases(&barrier.left), ["r", "b", "a"]);
        assert_eq!(planned_join_leaf_aliases(&barrier.right), ["optional"]);

        let rows = drain_rows(executor.execute(sql).unwrap());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Integer(64)));
        assert!(rows[0].get(3).is_some_and(Value::is_null));
    }

    #[test]
    fn costed_inner_reorder_never_crosses_non_inner_barriers() {
        let executor = create_test_executor();
        seed_costed_reorder_tables(&executor);
        for (barrier_sql, expected_type) in [
            (
                "RIGHT JOIN reorder_optional optional ON optional.b_id = b.id",
                "RIGHT",
            ),
            (
                "FULL JOIN reorder_optional optional ON optional.b_id = b.id",
                "FULL",
            ),
            ("CROSS JOIN reorder_optional optional", "CROSS"),
        ] {
            let stmt = parse_select_statement(&format!(
                "SELECT r.id, b.id, a.id, optional.id
                 FROM reorder_a a
                 INNER JOIN reorder_b b ON b.a_id = a.id
                 INNER JOIN reorder_root r ON r.b_id = b.id
                 {barrier_sql}"
            ));
            let classification = get_classification(&stmt);
            let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
                panic!("expected JOIN source");
            };
            let planned = executor
                .plan_join_table_expression(join, &stmt, &classification)
                .unwrap();
            let Expression::JoinSource(barrier) = planned else {
                panic!("{expected_type} barrier must remain the plan root");
            };
            assert_eq!(barrier.join_type.to_uppercase(), expected_type);
            assert_eq!(planned_join_leaf_aliases(&barrier.right), ["optional"]);
        }

        let stmt = parse_select_statement(
            "SELECT r.id, b.id, a.id, optional.id
             FROM reorder_a a
             INNER JOIN reorder_b b ON b.a_id = a.id
             INNER JOIN reorder_root r ON r.b_id = b.id
             INNER JOIN reorder_optional optional USING (id)",
        );
        let classification = get_classification(&stmt);
        let Expression::JoinSource(join) = stmt.table_expr.as_deref().unwrap() else {
            panic!("expected JOIN source");
        };
        let planned = executor
            .plan_join_table_expression(join, &stmt, &classification)
            .unwrap();
        let Expression::JoinSource(barrier) = planned else {
            panic!("USING barrier must remain the plan root");
        };
        assert!(!barrier.using_columns.is_empty());
        assert_eq!(planned_join_leaf_aliases(&barrier.right), ["optional"]);
    }
