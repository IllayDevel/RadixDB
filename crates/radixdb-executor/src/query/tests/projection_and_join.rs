    #[test]
    fn scalar_function_error_in_select_is_not_silent_null() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(&executor, "SELECT SLEEP(0 - 1) FROM events");
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected scalar SELECT error: {error}"
        );
    }
    #[test]
    fn scalar_function_error_in_where_is_not_silent_false() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(&executor, "SELECT id FROM events WHERE SLEEP(0 - 1) > 0");
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected scalar WHERE error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_delete_where_is_not_silent_false() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(&executor, "DELETE FROM events WHERE SLEEP(0 - 1) > 0");
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected DELETE WHERE error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_order_by_is_not_silent_null() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(&executor, "SELECT id FROM events ORDER BY SLEEP(0 - 1)");
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected scalar ORDER BY error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_aggregate_filter_is_not_silent_false() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(
            &executor,
            "SELECT COUNT(*) FILTER (WHERE SLEEP(0 - 1) > 0) FROM events",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected aggregate FILTER error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_group_by_is_not_silent_null_key() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(
            &executor,
            "SELECT COUNT(*) FROM events GROUP BY SLEEP(0 - 1)",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected GROUP BY expression error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_having_is_not_silent_fallback() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(
            &executor,
            "SELECT amount, COUNT(*) FROM events GROUP BY amount HAVING SLEEP(0 - 1) > 0",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected HAVING expression error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_aggregate_order_by_is_not_silent_skip() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(
            &executor,
            "SELECT ARRAY_AGG(amount ORDER BY SLEEP(0 - 1)) FROM events",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected aggregate ORDER BY error: {error}"
        );
    }

    #[test]
    fn scalar_function_error_in_window_aggregate_arg_is_not_silent_null() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        let error = execute_sql_error(&executor, "SELECT SUM(SLEEP(0 - 1)) OVER () FROM events");
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected window aggregate arg error: {error}"
        );
    }

    #[test]
    fn row_dependent_scalar_function_errors_are_not_silent() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();

        for sql in [
            "SELECT SLEEP(0 - amount) FROM events",
            "SELECT id FROM events WHERE SLEEP(0 - amount) > 0",
            "SELECT COUNT(*) FROM events GROUP BY SLEEP(0 - amount)",
            "SELECT amount, COUNT(*) FROM events GROUP BY amount HAVING SLEEP(0 - amount) > 0",
            "SELECT COUNT(*) FILTER (WHERE SLEEP(0 - amount) > 0) FROM events",
            "SELECT SUM(SLEEP(0 - amount)) OVER () FROM events",
        ] {
            let error = execute_sql_error(&executor, sql);
            assert!(
                error.contains("SLEEP duration cannot be negative"),
                "{sql}: unexpected row-dependent error: {error}"
            );
        }
    }

    #[test]
    fn indexed_row_dependent_filter_errors_are_not_silent() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("CREATE INDEX idx_events_amount ON events(amount)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 5)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (2, 6)")
            .unwrap();

        for sql in [
            "SELECT id FROM events WHERE amount = 5 AND SLEEP(0 - amount) > 0",
            "SELECT id FROM events WHERE amount = 5 OR SLEEP(0 - amount) > 0",
        ] {
            let error = execute_sql_error(&executor, sql);
            assert!(
                error.contains("SLEEP duration cannot be negative"),
                "{sql}: unexpected indexed row-dependent error: {error}"
            );
        }
    }

    #[test]
    fn invalid_cast_errors_are_not_silent_null() {
        let executor = create_test_executor();

        let error = execute_sql_error(&executor, "SELECT CAST('abc' AS INTEGER)");
        assert!(
            error.contains("cannot convert value 'abc'"),
            "unexpected invalid SELECT cast error: {error}"
        );

        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 'bad')")
            .unwrap();

        let error = execute_sql_error(
            &executor,
            "SELECT id FROM events WHERE CAST(amount AS INTEGER) > 0",
        );
        assert!(
            error.contains("cannot convert value 'bad'"),
            "unexpected invalid WHERE cast error: {error}"
        );
    }

    #[test]
    fn dml_invalid_column_coercion_errors_are_not_silent_null() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();

        let error = execute_sql_error(&executor, "INSERT INTO events VALUES (1, 'bad')");
        assert!(
            error.contains("cannot convert value 'bad'"),
            "unexpected invalid INSERT coercion error: {error}"
        );

        executor
            .execute("INSERT INTO events VALUES (1, 10)")
            .unwrap();

        let error = execute_sql_error(&executor, "UPDATE events SET amount = 'bad' WHERE id = 1");
        assert!(
            error.contains("cannot convert value 'bad'"),
            "unexpected invalid UPDATE coercion error: {error}"
        );

        let error = execute_sql_error(
            &executor,
            "UPDATE events SET amount = 20 WHERE SLEEP(0 - amount) > 0",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected UPDATE WHERE expression error: {error}"
        );

        let error = execute_sql_error(
            &executor,
            "UPDATE events SET amount = SLEEP(0 - amount) WHERE id = 1",
        );
        assert!(
            error.contains("SLEEP duration cannot be negative"),
            "unexpected UPDATE SET expression error: {error}"
        );
    }

    #[test]
    fn fast_pk_update_invalid_column_coercion_errors_are_not_silent_null() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO events VALUES (1, 10)")
            .unwrap();

        let sql = "UPDATE events SET amount = $1 WHERE id = $2";
        executor
            .execute_with_params(
                sql,
                smallvec::smallvec![Value::Integer(20), Value::Integer(1)],
            )
            .unwrap();

        let params = [Value::text("bad"), Value::Integer(1)];
        let fast_result = executor
            .try_fast_path_with_params(sql, &params)
            .expect("expected cached fast PK update path");
        let error = match fast_result {
            Ok(_) => panic!("expected invalid fast UPDATE coercion error"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("cannot convert value 'bad'"),
            "unexpected invalid fast UPDATE coercion error: {error}"
        );
    }

    #[test]
    fn default_expression_invalid_coercion_errors_are_not_silent_null() {
        let executor = create_test_executor();

        let error = execute_sql_error(
            &executor,
            "CREATE TABLE bad_defaults (id INTEGER PRIMARY KEY, amount INTEGER DEFAULT 'bad')",
        );
        assert!(
            error.contains("cannot convert value 'bad'"),
            "unexpected invalid DEFAULT coercion error: {error}"
        );

        executor
            .execute(
                "CREATE TABLE good_defaults (id INTEGER PRIMARY KEY, amount INTEGER DEFAULT '123')",
            )
            .unwrap();
        executor
            .execute("INSERT INTO good_defaults (id) VALUES (1)")
            .unwrap();
        let mut result = executor
            .execute("SELECT amount FROM good_defaults WHERE id = 1")
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(123)));
        assert!(!result.next());
    }

    #[test]
    fn test_limit_pushdown_simple_projection_returns_projected_rows() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, unused TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (1, 10, 'a')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (2, 20, 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (3, 30, 'c')")
            .unwrap();

        let mut result = executor
            .execute("SELECT value AS v FROM items LIMIT 2 OFFSET 1")
            .unwrap();

        assert_eq!(result.columns(), &["v".to_string()]);

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(20), Value::Integer(30)]);
    }

    #[test]
    fn test_expression_projection_scan_plan_prunes_unused_columns() {
        let executor = create_test_executor();
        let all_columns = vec!["id".to_string(), "value".to_string(), "payload".to_string()];
        let statements =
            radixdb_sql::parse_sql("SELECT value + 1 AS next_value FROM items LIMIT 2").unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };
        let output_columns = executor.get_output_column_names(&stmt.columns, &all_columns, None);

        let plan = executor
            .build_expression_projection_scan_plan(&stmt.columns, &output_columns, &all_columns)
            .unwrap();

        assert_eq!(plan.scan_indices, vec![1]);
        assert_eq!(plan.scan_columns, vec!["value".to_string()]);
        assert!(plan.output_indices_in_scan.is_empty());
        assert_eq!(plan.output_columns, vec!["next_value".to_string()]);
    }

    #[test]
    fn test_ordered_expression_projection_scan_plan_keeps_sort_dependency_only() {
        let executor = create_test_executor();
        let all_columns = vec![
            "id".to_string(),
            "value".to_string(),
            "sort_key".to_string(),
            "payload".to_string(),
        ];
        let statements = radixdb_sql::parse_sql(
            "SELECT value + 1 AS next_value FROM items ORDER BY sort_key LIMIT 2",
        )
        .unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };
        let output_columns = executor.get_output_column_names(&stmt.columns, &all_columns, None);

        let plan = executor
            .build_ordered_distinct_projection_scan_plan(stmt, &output_columns, &all_columns)
            .unwrap();

        assert_eq!(plan.scan_indices, vec![1, 2]);
        assert_eq!(
            plan.scan_columns,
            vec!["value".to_string(), "sort_key".to_string()]
        );
        assert_eq!(plan.output_columns, vec!["next_value".to_string()]);
    }

    #[test]
    fn test_distinct_on_expression_projection_scan_plan_keeps_distinct_dependencies_only() {
        let executor = create_test_executor();
        let all_columns = vec![
            "id".to_string(),
            "value".to_string(),
            "group_id".to_string(),
            "sort_key".to_string(),
            "payload".to_string(),
        ];
        let statements = radixdb_sql::parse_sql(
            "SELECT DISTINCT ON (group_id) value + 1 AS next_value FROM items ORDER BY group_id, sort_key LIMIT 2",
        )
        .unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };
        let output_columns = executor.get_output_column_names(&stmt.columns, &all_columns, None);

        let plan = executor
            .build_ordered_distinct_projection_scan_plan(stmt, &output_columns, &all_columns)
            .unwrap();

        assert_eq!(plan.scan_indices, vec![1, 2, 3]);
        assert_eq!(
            plan.scan_columns,
            vec![
                "value".to_string(),
                "group_id".to_string(),
                "sort_key".to_string()
            ]
        );
        assert_eq!(plan.output_columns, vec!["next_value".to_string()]);
    }

    #[test]
    fn test_constant_expression_projection_scan_plan_uses_exact_empty_dependencies() {
        let executor = create_test_executor();
        let all_columns = vec!["id".to_string(), "value".to_string(), "payload".to_string()];
        let statements = radixdb_sql::parse_sql("SELECT 1 + 2 AS three FROM items LIMIT 2")
            .expect("parse constant projection");
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };
        let output_columns = executor.get_output_column_names(&stmt.columns, &all_columns, None);

        let plan = executor
            .build_expression_projection_scan_plan(&stmt.columns, &output_columns, &all_columns)
            .unwrap();

        assert!(
            plan.scan_indices.is_empty(),
            "constant projection must request exact-empty dependencies"
        );
        assert!(plan.scan_columns.is_empty());
        assert_eq!(plan.output_columns, vec!["three".to_string()]);
    }

    #[test]
    fn test_limit_pushdown_expression_projection_returns_projected_rows() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, unused TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (1, 10, 'a')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (2, 20, 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (3, 30, 'c')")
            .unwrap();

        let mut result = executor
            .execute("SELECT value + 1 AS next_value FROM items LIMIT 2 OFFSET 1")
            .unwrap();

        assert_eq!(result.columns(), &["next_value".to_string()]);

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(21), Value::Integer(31)]);
    }

    #[test]
    fn test_ordered_limit_expression_projection_sorts_by_hidden_dependency() {
        let executor = create_test_executor();

        executor
            .execute(
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    value INTEGER,
                    sort_key INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (1, 100, 30, 'unused-a')")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (2, 200, 10, 'unused-b')")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (3, 300, 20, 'unused-c')")
            .unwrap();

        let mut result = executor
            .execute("SELECT value + 1 AS next_value FROM items ORDER BY sort_key LIMIT 2")
            .unwrap();

        assert_eq!(result.columns(), &["next_value".to_string()]);
        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(201), Value::Integer(301)]);
    }

    #[test]
    fn test_ordered_limit_projection_sorts_by_computed_hidden_key() {
        let executor = create_test_executor();

        executor
            .execute(
                "CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    sort_key INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (1, 30, 'unused-a')")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (2, 10, 'unused-b')")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (3, 20, 'unused-c')")
            .unwrap();

        let mut result = executor
            .execute("SELECT id FROM items ORDER BY sort_key + 1 LIMIT 2")
            .unwrap();

        assert_eq!(result.columns(), &["id".to_string()]);
        let mut ids = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            ids.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(ids, vec![Value::Integer(2), Value::Integer(3)]);
    }

    #[test]
    fn test_ordered_limit_reuses_unaliased_select_expression_as_sort_key() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (1, 30)")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (2, 10)")
            .unwrap();
        executor
            .execute("INSERT INTO items VALUES (3, 20)")
            .unwrap();

        let mut result = executor
            .execute("SELECT value + 1 FROM items ORDER BY value + 1 LIMIT 2")
            .unwrap();

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(11), Value::Integer(21)]);
    }

    #[test]
    fn test_limit_pushdown_constant_expression_projection_returns_projected_rows() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER, unused TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (1, 10, 'a')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (2, 20, 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO items (id, value, unused) VALUES (3, 30, 'c')")
            .unwrap();

        let mut result = executor
            .execute("SELECT 1 + 2 AS three FROM items LIMIT 2")
            .unwrap();

        assert_eq!(result.columns(), &["three".to_string()]);

        let mut values = Vec::new();
        while result.next() {
            let row = result.row();
            assert_eq!(row.len(), 1);
            values.push(row.get(0).cloned().unwrap());
        }

        assert_eq!(values, vec![Value::Integer(3), Value::Integer(3)]);
    }

    #[test]
    fn test_filtered_simple_projection_scan_plan_uses_predicate_and_output_columns() {
        let executor = create_test_executor();
        let all_columns = vec![
            "id".to_string(),
            "value".to_string(),
            "status".to_string(),
            "payload".to_string(),
        ];
        let statements = radixdb_sql::parse_sql(
            "SELECT value AS v FROM items WHERE status = 'ok' AND value + 1 > 20",
        )
        .unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };

        let (output_indices, output_columns) = executor
            .get_simple_projection_indices(&stmt.columns, &all_columns)
            .unwrap();
        let plan = executor
            .build_filtered_simple_projection_scan_plan(
                stmt.where_clause.as_ref().unwrap(),
                &output_indices,
                &output_columns,
                &all_columns,
            )
            .unwrap();

        assert_eq!(plan.scan_indices, vec![1, 2]);
        assert_eq!(
            plan.scan_columns,
            vec!["value".to_string(), "status".to_string()]
        );
        assert_eq!(plan.output_indices_in_scan, vec![0]);
        assert_eq!(plan.output_columns, vec!["v".to_string()]);
    }

    #[test]
    fn narrow_key_stream_plan_keeps_only_filter_and_join_key_columns() {
        let executor = create_test_executor();
        let columns = vec![
            "id".to_string(),
            "parent_table".to_string(),
            "bucket".to_string(),
            "parent_id".to_string(),
            "payload".to_string(),
            "amount".to_string(),
        ];
        let statements = radixdb_sql::parse_sql(
            "SELECT parent_id FROM child WHERE parent_table = 59 AND bucket BETWEEN 10 AND 20",
        )
        .unwrap();
        let stmt = match &statements[0] {
            radixdb_sql::Statement::Select(stmt) => stmt,
            _ => panic!("expected SELECT"),
        };

        let plan = executor
            .build_narrow_key_stream_plan(stmt.where_clause.as_deref(), "parent_id", &columns)
            .expect("narrow key stream plan");

        assert_eq!(plan.scan_indices, vec![1, 2, 3]);
        assert_eq!(
            plan.scan_columns,
            vec![
                "parent_table".to_string(),
                "bucket".to_string(),
                "parent_id".to_string(),
            ]
        );
        assert_eq!(plan.key_index_in_scan, 2);
    }

    #[test]
    fn count_pk_semijoin_uses_narrow_child_stream_and_metadata_parent_probe() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_table INTEGER,
                    bucket INTEGER,
                    parent_id INTEGER,
                    payload TEXT,
                    amount FLOAT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 'parent payload'), (20, 'parent payload')")
            .unwrap();
        executor
            .execute(
                "INSERT INTO child VALUES
                    (1, 59, 10, 10, 'child payload', 1.0),
                    (2, 59, 11, 10, 'child payload', 2.0),
                    (3, 59, 20, 20, 'child payload', 3.0),
                    (4, 59, 10, 99, 'child payload', 4.0),
                    (5, 60, 10, 10, 'child payload', 5.0)",
            )
            .unwrap();

        let mut result = executor
            .execute(
                "SELECT COUNT(*)
                 FROM child c
                 INNER JOIN parent p ON c.parent_id = p.id
                 WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20",
            )
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(3)));
        assert!(!result.next());
    }

    #[test]
    fn count_integer_antijoin_handles_secondary_key_pk_key_and_left_filter() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE outbox_jobs (
                    id INTEGER PRIMARY KEY,
                    message_id INTEGER NOT NULL UNIQUE,
                    state TEXT NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE sync_events (
                    id INTEGER PRIMARY KEY,
                    message_id INTEGER
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO messages VALUES
                    (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
            )
            .unwrap();
        executor
            .execute("INSERT INTO outbox_jobs VALUES (101, 1, 'done'), (103, 3, 'done')")
            .unwrap();
        executor
            .execute("INSERT INTO sync_events VALUES (201, 1), (202, NULL), (203, 99), (204, 99)")
            .unwrap();

        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM messages m
                 LEFT JOIN outbox_jobs o ON o.message_id = m.id
                 WHERE o.id IS NULL",
            ),
            3,
            "secondary integer key anti-count"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM messages m
                 LEFT JOIN outbox_jobs o ON m.id = o.message_id
                 WHERE m.id >= 3 AND o.id IS NULL",
            ),
            2,
            "outer filter and swapped equality"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM sync_events s
                 LEFT JOIN messages m ON m.id = s.message_id
                 WHERE s.message_id IS NOT NULL AND m.id IS NULL",
            ),
            2,
            "integer PK probe preserves duplicate outer keys"
        );
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM sync_events s
                 LEFT JOIN messages m ON m.id = s.message_id
                 WHERE m.id IS NULL",
            ),
            3,
            "integer PK probe preserves NULL outer keys"
        );
    }

    #[test]
    fn count_integer_antijoin_requires_non_nullable_null_rejection_column() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, marker INTEGER)")
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, NULL), (20, 1)")
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (1, 10), (2, 20), (3, 99)")
            .unwrap();

        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c
                 LEFT JOIN parent p ON c.parent_id = p.id
                 WHERE p.marker IS NULL",
            ),
            2,
            "matched rows with a real NULL must not be rewritten as anti-join"
        );
    }

    #[test]
    fn count_integer_antijoin_sees_transaction_local_inner_changes() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE messages (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE outbox_jobs (
                    id INTEGER PRIMARY KEY,
                    message_id INTEGER NOT NULL UNIQUE
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO messages VALUES (1), (2)")
            .unwrap();
        executor
            .execute("INSERT INTO outbox_jobs VALUES (101, 1)")
            .unwrap();
        let sql = "SELECT COUNT(*) FROM messages m LEFT JOIN outbox_jobs o \
                   ON o.message_id = m.id WHERE o.id IS NULL";

        executor.execute("BEGIN").unwrap();
        assert_eq!(scalar_count(&executor, sql), 1);
        executor
            .execute("INSERT INTO outbox_jobs VALUES (102, 2)")
            .unwrap();
        assert_eq!(scalar_count(&executor, sql), 0);
        executor
            .execute("DELETE FROM outbox_jobs WHERE id = 101")
            .unwrap();
        assert_eq!(scalar_count(&executor, sql), 1);
        executor.execute("ROLLBACK").unwrap();
        assert_eq!(scalar_count(&executor, sql), 1);
    }

    #[test]
    fn count_pk_semijoin_rejects_residual_on_predicate() {
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
            .execute("INSERT INTO child VALUES (1, 10, 1), (2, 10, 9), (3, 20, 2)")
            .unwrap();

        let mut result = executor
            .execute(
                "SELECT COUNT(*)
                 FROM child c
                 INNER JOIN parent p ON c.parent_id = p.id AND c.tag = p.tag",
            )
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
    }

    #[test]
    fn count_pk_semijoin_preserves_sql_multiplicity_and_swapped_equality() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, tag INTEGER)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER,
                    bucket INTEGER,
                    payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 1), (20, 2)")
            .unwrap();
        executor
            .execute(
                "INSERT INTO child VALUES
                    (1, 10, 1, 'duplicate one'),
                    (2, 10, 1, 'duplicate two'),
                    (3, 20, 2, 'match'),
                    (4, 99, 1, 'orphan'),
                    (5, NULL, 1, 'null key')",
            )
            .unwrap();

        for sql in [
            "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
            "SELECT COUNT(*) FROM child c INNER JOIN parent p ON p.id = c.parent_id",
            "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id WHERE c.bucket = 1",
        ] {
            let mut result = executor.execute(sql).unwrap();
            assert!(result.next(), "{sql}");
            let expected = if sql.contains("bucket") { 2 } else { 3 };
            assert_eq!(result.row().get(0), Some(&Value::Integer(expected)), "{sql}");
            assert!(!result.next(), "{sql}");
        }
    }

    #[test]
    fn count_pk_semijoin_fuses_non_null_right_column_for_left_join() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE parent (
                    id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL,
                    optional_payload TEXT
                )",
            )
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 'ten', NULL), (20, 'twenty', 'present')")
            .unwrap();
        executor
            .execute(
                "INSERT INTO child VALUES
                    (1, 10),
                    (2, 10),
                    (3, 20),
                    (4, 99),
                    (5, NULL)",
            )
            .unwrap();

        for sql in [
            "SELECT COUNT(p.payload) FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
            "SELECT COUNT(p.payload) FROM child c LEFT JOIN parent p ON p.id = c.parent_id",
            "SELECT COUNT(p.id) FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
            "SELECT COUNT(p.payload) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
        ] {
            assert_eq!(scalar_count(&executor, sql), 3, "{sql}");
            let explain = drain_rows(executor.execute(&format!("EXPLAIN {sql}")).unwrap())
                .iter()
                .map(|row| text_key(row, 0))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                explain.contains("Join Access Path: join.count_pk_semijoin"),
                "eligible COUNT(right.not_null_column) was not fused:\n{explain}"
            );
        }

        for (sql, expected) in [
            (
                "SELECT COUNT(p.optional_payload) FROM child c LEFT JOIN parent p \
                 ON c.parent_id = p.id",
                1,
            ),
            (
                "SELECT COUNT(c.id) FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
                5,
            ),
            (
                "SELECT COUNT(*) FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
                5,
            ),
        ] {
            assert_eq!(scalar_count(&executor, sql), expected, "{sql}");
            let explain = drain_rows(executor.execute(&format!("EXPLAIN {sql}")).unwrap())
                .iter()
                .map(|row| text_key(row, 0))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !explain.contains("join.count_pk_semijoin"),
                "ineligible COUNT shape was fused:\n{explain}"
            );
        }
    }

    #[test]
    fn count_pk_semijoin_sees_local_rows_inside_explicit_transaction() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
            .unwrap();
        executor.execute("INSERT INTO parent VALUES (10)").unwrap();
        executor
            .execute("INSERT INTO child VALUES (1, 10)")
            .unwrap();

        executor.execute("BEGIN").unwrap();
        executor.execute("INSERT INTO parent VALUES (20)").unwrap();
        executor
            .execute("INSERT INTO child VALUES (2, 20)")
            .unwrap();
        let mut result = executor
            .execute("SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id")
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
        executor
            .execute("DELETE FROM parent WHERE id = 20")
            .unwrap();
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
            ),
            1,
            "transaction-local parent delete"
        );
        executor
            .execute("UPDATE child SET parent_id = 20 WHERE id = 1")
            .unwrap();
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
            ),
            0,
            "transaction-local child update"
        );
        executor.execute("INSERT INTO parent VALUES (30)").unwrap();
        executor
            .execute("UPDATE child SET parent_id = 30 WHERE id = 1")
            .unwrap();
        assert_eq!(
            scalar_count(
                &executor,
                "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
            ),
            1,
            "transaction-local insert after update"
        );
        executor.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn batch_index_join_stays_enabled_and_sees_explicit_transaction_changes() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE parent (
                    id INTEGER PRIMARY KEY,
                    lookup_key INTEGER NOT NULL UNIQUE,
                    payload TEXT NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_key INTEGER NOT NULL)")
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (10, 10010, 'committed')")
            .unwrap();
        executor
            .execute(
                "INSERT INTO parent
                 SELECT value, value + 20000, 'unrelated' FROM generate_series(100, 1100)",
            )
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (1, 10010)")
            .unwrap();

        let sql = "SELECT c.id, p.payload FROM child c \
                   INNER JOIN parent p ON p.lookup_key = c.parent_key ORDER BY c.id";
        executor.execute("BEGIN").unwrap();
        executor
            .execute("INSERT INTO parent VALUES (20, 10020, 'private')")
            .unwrap();
        executor
            .execute("INSERT INTO child VALUES (2, 10020)")
            .unwrap();

        let mut rows = executor.execute(sql).unwrap();
        let mut actual = Vec::new();
        while rows.next() {
            actual.push((
                rows.row().get(0).cloned().unwrap(),
                rows.row().get(1).cloned().unwrap(),
            ));
        }
        assert_eq!(
            actual,
            vec![
                (Value::Integer(1), Value::text("committed")),
                (Value::Integer(2), Value::text("private")),
            ]
        );

        let mut explain = executor.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap();
        let mut plan = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                plan.push(line.to_string());
            }
        }
        assert!(
            plan.iter()
                .any(|line| line.contains("batch_index_nested_loop=1")),
            "explicit transaction silently disabled BatchIndexNL: {plan:#?}"
        );

        executor
            .execute("DELETE FROM parent WHERE id = 20")
            .unwrap();
        let mut rows = executor.execute(sql).unwrap();
        assert!(rows.next());
        assert_eq!(rows.row().get(0), Some(&Value::Integer(1)));
        assert!(!rows.next());

        executor
            .execute("UPDATE child SET parent_key = 10030 WHERE id = 1")
            .unwrap();
        executor
            .execute("INSERT INTO parent VALUES (30, 10030, 'transitioned')")
            .unwrap();
        let mut rows = executor.execute(sql).unwrap();
        assert!(rows.next());
        assert_eq!(rows.row().get(1), Some(&Value::text("transitioned")));
        assert!(!rows.next());
        executor.execute("ROLLBACK").unwrap();
    }
