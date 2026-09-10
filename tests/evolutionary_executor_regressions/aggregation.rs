#[allow(unused_imports)]
pub use radixdb_executor::aggregation::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::context::ExecutionContext;
    use crate::executor::query_classification::QueryClassification;
    use crate::executor::Executor;
    use radixdb_core::Value;
    use radixdb_storage::config::Config;
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use std::path::Path;
    use std::sync::Arc;

    fn create_test_executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn create_persistent_test_executor(path: &Path) -> Executor {
        let mut config = Config::with_path(path.to_string_lossy().to_string());
        config.persistence.target_volume_rows = 2;
        config.persistence.compact_threshold = 100;
        config.persistence.checkpoint_on_close = false;
        let engine = MVCCEngine::new(config);
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn setup_test_data(executor: &Executor) {
        executor
            .execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO sales VALUES (1, 'electronics', 100)")
            .unwrap();
        executor
            .execute("INSERT INTO sales VALUES (2, 'electronics', 200)")
            .unwrap();
        executor
            .execute("INSERT INTO sales VALUES (3, 'clothing', 50)")
            .unwrap();
        executor
            .execute("INSERT INTO sales VALUES (4, 'clothing', 75)")
            .unwrap();
    }

    #[test]
    fn test_count_star() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor.execute("SELECT COUNT(*) FROM sales").unwrap();
        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(4)));
    }

    #[test]
    fn snapshot_count_star_streams_zero_width_cold_rows() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        executor
            .execute("CREATE TABLE cold_count (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        executor
            .execute(
                "INSERT INTO cold_count SELECT value, 'payload' FROM generate_series(1, 10000)",
            )
            .unwrap();
        executor.engine().force_checkpoint_cycle().unwrap();
        executor.execute("BEGIN ISOLATION LEVEL SNAPSHOT").unwrap();

        radixdb_storage::instrumentation::begin_row_materialization_probe();
        let mut result = executor.execute("SELECT COUNT(*) FROM cold_count").unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(10_000)));
        assert!(!result.next());
        result.close().unwrap();
        let materialization = radixdb_storage::instrumentation::end_row_materialization_probe();

        assert!(
            materialization.rows > 0,
            "snapshot COUNT must exercise the cold streaming fallback"
        );
        assert_eq!(
            materialization.values, 0,
            "COUNT(*) must not retain or decode payload columns for visible cold rows"
        );
        executor.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn grouped_count_column_ignores_null_extended_join_rows() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE count_parents (id INTEGER PRIMARY KEY, grp TEXT NOT NULL)")
            .unwrap();
        executor
            .execute("CREATE TABLE count_children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES count_parents(id))")
            .unwrap();
        executor
            .execute("INSERT INTO count_parents VALUES (1, 'a'), (2, 'a'), (3, 'b')")
            .unwrap();
        executor
            .execute("INSERT INTO count_children VALUES (10, 1)")
            .unwrap();

        let mut single = executor
            .execute("SELECT p.grp, COUNT(c.id), COUNT(*) FROM count_parents p LEFT JOIN count_children c ON c.parent_id = p.id GROUP BY p.grp")
            .unwrap();
        let mut single_counts = std::collections::BTreeMap::new();
        while single.next() {
            single_counts.insert(
                single.row().get(0).unwrap().as_str().unwrap().to_string(),
                (
                    single.row().get(1).unwrap().as_int64().unwrap(),
                    single.row().get(2).unwrap().as_int64().unwrap(),
                ),
            );
        }
        assert_eq!(single_counts.get("a"), Some(&(1, 2)));
        assert_eq!(single_counts.get("b"), Some(&(0, 1)));

        let mut multi = executor
            .execute("SELECT p.grp, p.id, COUNT(c.id), COUNT(*) FROM count_parents p LEFT JOIN count_children c ON c.parent_id = p.id GROUP BY p.grp, p.id")
            .unwrap();
        let mut multi_counts = std::collections::BTreeMap::new();
        while multi.next() {
            multi_counts.insert(
                multi.row().get(1).unwrap().as_int64().unwrap(),
                (
                    multi.row().get(2).unwrap().as_int64().unwrap(),
                    multi.row().get(3).unwrap().as_int64().unwrap(),
                ),
            );
        }
        assert_eq!(multi_counts, [(1, (1, 1)), (2, (0, 1)), (3, (0, 1))].into());
    }

    #[test]
    fn test_sum_column() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor.execute("SELECT SUM(amount) FROM sales").unwrap();
        assert!(result.next());
        let row = result.row();
        // SUM(100 + 200 + 50 + 75) = 425
        let value = row.get(0).unwrap();
        match value {
            Value::Integer(n) => assert_eq!(*n, 425),
            Value::Float(f) => assert!((f - 425.0).abs() < 0.01),
            _ => panic!("Expected numeric value, got {:?}", value),
        }
    }

    #[test]
    fn grouped_integer_sum_preserves_type_and_precision_above_f64_boundary() {
        let executor = create_test_executor();
        executor
            .execute(
                "CREATE TABLE exact_group_sum (
                    id INTEGER PRIMARY KEY,
                    bucket INTEGER NOT NULL,
                    amount INTEGER NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO exact_group_sum VALUES
                    (1, 7, 9007199254740993),
                    (2, 7, 2),
                    (3, 8, -9007199254740993)",
            )
            .unwrap();

        let mut result = executor
            .execute(
                "SELECT bucket, SUM(amount)
                 FROM exact_group_sum
                 GROUP BY bucket
                 ORDER BY bucket",
            )
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(7)));
        assert_eq!(
            result.row().get(1),
            Some(&Value::Integer(9_007_199_254_740_995))
        );
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(8)));
        assert_eq!(
            result.row().get(1),
            Some(&Value::Integer(-9_007_199_254_740_993))
        );
        assert!(!result.next());
    }

    #[test]
    fn test_avg_column() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor.execute("SELECT AVG(amount) FROM sales").unwrap();
        assert!(result.next());
        let row = result.row();
        // AVG = 425 / 4 = 106.25
        let value = row.get(0).unwrap();
        match value {
            Value::Float(f) => assert!((f - 106.25).abs() < 0.01),
            Value::Integer(n) => assert_eq!(*n, 106), // Might truncate
            _ => panic!("Expected numeric value, got {:?}", value),
        }
    }

    #[test]
    fn test_min_max() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT MIN(amount), MAX(amount) FROM sales")
            .unwrap();
        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(50)));
        assert_eq!(row.get(1), Some(&Value::Integer(200)));
    }

    #[test]
    fn test_group_by_simple() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT category, SUM(amount) FROM sales GROUP BY category")
            .unwrap();

        let mut found = ahash::AHashMap::new();
        while result.next() {
            let row = result.row();
            if let Some(Value::Text(cat)) = row.get(0) {
                let sum = row.get(1).cloned().unwrap();
                found.insert(cat.clone(), sum);
            }
        }

        // electronics: 100 + 200 = 300
        // clothing: 50 + 75 = 125
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_storage_group_by_having_on_selected_aggregate() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute(
                "SELECT category, SUM(amount) FROM sales \
                 GROUP BY category HAVING SUM(amount) > 150",
            )
            .unwrap();

        let mut rows = Vec::new();
        while result.next() {
            let row = result.row();
            rows.push((row.get(0).cloned().unwrap(), row.get(1).cloned().unwrap()));
        }

        assert_eq!(
            rows,
            vec![(Value::text("electronics"), Value::Integer(300))]
        );
    }

    #[test]
    fn test_storage_group_by_having_on_selected_alias() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute(
                "SELECT category, SUM(amount) AS total FROM sales \
                 GROUP BY category HAVING total >= 300",
            )
            .unwrap();

        let mut rows = Vec::new();
        while result.next() {
            let row = result.row();
            rows.push((row.get(0).cloned().unwrap(), row.get(1).cloned().unwrap()));
        }

        assert_eq!(
            rows,
            vec![(Value::text("electronics"), Value::Integer(300))]
        );
    }

    #[test]
    fn test_storage_group_by_having_path_is_selected() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut statements = parse_sql(
            "SELECT category, SUM(amount) FROM sales \
             GROUP BY category HAVING SUM(amount) > 150",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect("GROUP BY HAVING over selected aggregate should use storage aggregation");

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::text("electronics")));
        assert_eq!(row.get(1), Some(&Value::Integer(300)));
        assert!(!result.next());
    }

    #[test]
    fn test_storage_group_by_having_hidden_aggregate_is_not_projected() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut statements = parse_sql(
            "SELECT category FROM sales \
             GROUP BY category HAVING SUM(amount) > 150",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect("HAVING-only aggregate should be computed as hidden storage aggregate");

        assert!(result.next());
        let row = result.row();
        assert_eq!(result.columns(), &["category".to_string()]);
        assert_eq!(row.len(), 1, "hidden HAVING aggregate must not leak");
        assert_eq!(row.get(0), Some(&Value::text("electronics")));
        assert!(!result.next());
    }

    #[test]
    fn test_storage_group_by_having_group_only_path_is_selected() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut statements = parse_sql(
            "SELECT category AS c FROM sales \
             GROUP BY category HAVING category = 'electronics'",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect("GROUP BY without aggregate should still use storage grouping");

        assert!(result.next());
        let row = result.row();
        assert_eq!(result.columns(), &["c".to_string()]);
        assert_eq!(row.len(), 1);
        assert_eq!(row.get(0), Some(&Value::text("electronics")));
        assert!(!result.next());
    }

    #[test]
    fn test_storage_group_by_where_having_path_is_selected_on_cold_artifact() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut statements = parse_sql(
            "SELECT category, SUM(amount) FROM sales \
             WHERE amount >= 100 \
             GROUP BY category HAVING SUM(amount) >= 300",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect("GROUP BY WHERE HAVING over cold artifact-backed data should use storage aggregation");

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::text("electronics")));
        assert_eq!(row.get(1), Some(&Value::Integer(300)));
        assert!(!result.next());
    }

    #[test]
    fn test_artifact_columnar_group_by_having_benchmark_shape_is_selected_on_cold_artifact() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        executor
            .execute(
                "CREATE TABLE metrics (id INTEGER PRIMARY KEY, bucket INTEGER, amount INTEGER)",
            )
            .unwrap();
        for (id, bucket, amount) in [(1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 2, 7), (5, 3, 100)] {
            executor
                .execute(&format!(
                    "INSERT INTO metrics VALUES ({id}, {bucket}, {amount})"
                ))
                .unwrap();
        }
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut statements = parse_sql(
            "SELECT bucket, SUM(amount) FROM metrics \
             GROUP BY bucket HAVING SUM(amount) >= 25",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("metrics").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        radixdb_storage::instrumentation::begin_artifact_columnar_group_probe();
        radixdb_storage::instrumentation::begin_row_materialization_probe();
        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect(
                "integer GROUP BY + HAVING should use storage aggregation on cold artifact-backed",
            );
        let materialization = radixdb_storage::instrumentation::end_row_materialization_probe();
        let shape = radixdb_storage::instrumentation::end_artifact_columnar_group_probe();

        let mut rows = Vec::new();
        while result.next() {
            let row = result.row();
            rows.push((row.get(0).cloned().unwrap(), row.get(1).cloned().unwrap()));
        }
        rows.sort_by_key(|(bucket, _)| match bucket {
            Value::Integer(value) => *value,
            other => panic!("unexpected bucket value: {other:?}"),
        });

        assert_eq!(
            rows,
            vec![
                (Value::Integer(1), Value::Integer(30)),
                (Value::Integer(3), Value::Integer(100)),
            ]
        );
        assert_eq!(
            shape.applies, 1,
            "integer GROUP BY + HAVING must execute the artifact-backed columnar aggregate operator"
        );
        assert_eq!(shape.output_groups, 3);
        assert_eq!(
            materialization.rows, 0,
            "HAVING integration must not force storage input row materialization"
        );
        assert_eq!(materialization.values, 0);
    }

    #[test]
    fn explain_analyze_reports_executed_artifact_columnar_group_by() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        executor
            .execute(
                "CREATE TABLE metrics (id INTEGER PRIMARY KEY, bucket INTEGER, amount INTEGER)",
            )
            .unwrap();
        for (id, bucket, amount) in [(1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 2, 7), (5, 3, 100)] {
            executor
                .execute(&format!(
                    "INSERT INTO metrics VALUES ({id}, {bucket}, {amount})"
                ))
                .unwrap();
        }
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut explain = executor
            .execute(
                "EXPLAIN ANALYZE SELECT bucket, SUM(amount) FROM metrics \
                 GROUP BY bucket HAVING SUM(amount) >= 25",
            )
            .unwrap();
        let mut lines = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                lines.push(line.to_string());
            }
        }

        assert!(
            lines
                .iter()
                .any(|line| line
                    .contains("Aggregation Path: aggregation.artifact_columnar_group_by")),
            "EXPLAIN ANALYZE must report the artifact-backed columnar aggregate actually selected: {lines:#?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Access Path: aggregation.artifact_columnar_group_by")),
            "EXPLAIN ANALYZE must expose the physical artifact-backed columnar aggregate boundary: {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| {
                line.contains("artifact-backed Columnar Group: applies=1")
                    && line.contains("input_rows=5")
                    && line.contains("output_groups=3")
                    && line.contains("accumulator=direct_array")
                    && line.contains("scheduler=serial")
                    && line.contains("scheduled_segments=0")
                    && line.contains("local_merges=1")
                    && line.contains("merged_groups=3")
            }),
            "EXPLAIN ANALYZE must expose request-local artifact-backed group facts: {lines:#?}"
        );
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("Access Path: scan.cold_artifact")),
            "an applied artifact-backed columnar aggregate must not be rendered as a row scan: {lines:#?}"
        );
    }

    #[test]
    fn explain_analyze_reports_artifact_columnar_group_fallback_reason() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut explain = executor
            .execute(
                "EXPLAIN ANALYZE SELECT category, SUM(amount) FROM sales \
                 GROUP BY category HAVING SUM(amount) >= 100",
            )
            .unwrap();
        let mut lines = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                lines.push(line.to_string());
            }
        }

        assert!(
            lines
                .iter()
                .any(|line| line.contains("Aggregation Path: aggregation.storage_group_by")),
            "EXPLAIN ANALYZE should identify the storage fallback boundary: {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| {
                line.contains(
                    "Aggregation Fallback: aggregation.artifact_columnar_group_by:group_key",
                )
            }),
            "EXPLAIN ANALYZE must expose the artifact-backed columnar group fallback reason: {lines:#?}"
        );
        assert!(
            !lines
                .iter()
                .any(|line| line
                    .contains("Aggregation Path: aggregation.artifact_columnar_group_by")),
            "a rejected artifact-backed columnar aggregate must not be rendered as accepted: {lines:#?}"
        );
    }

    #[test]
    fn filtered_count_on_cold_artifact_integer_pk_uses_metadata_only_operator() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut result = executor
            .execute("SELECT COUNT(*) FROM sales WHERE id >= 2 AND id <= 3")
            .unwrap();
        let metadata_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();

        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
        assert!(!result.next());
        assert_eq!(metadata_probe.attempts, 1);
        assert_eq!(metadata_probe.applied, 1);
        assert_eq!(metadata_probe.intervals, 1);
        assert_eq!(metadata_probe.candidate_rows, 2);
        assert_eq!(metadata_probe.visible_rows, 2);

        // A fractional bound cannot enter the exact INTEGER metadata domain.
        // It must decline the optimization and preserve the generic evaluator's
        // non-lossy comparison result.
        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut fractional = executor
            .execute("SELECT COUNT(*) FROM sales WHERE id = 2.5")
            .unwrap();
        let fractional_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
        assert!(fractional.next());
        assert_eq!(fractional.row().get(0), Some(&Value::Integer(0)));
        assert_eq!(fractional_probe.applied, 0);

        // Comparison pushdown canonicalizes literal-on-the-left forms before
        // this operator sees them. Keep the routing contract explicit: these
        // must use the same metadata-only range, not silently drop to a scan.
        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        for (sql, expected) in [
            (
                "SELECT COUNT(*) FROM sales WHERE 2 <= id AND 3 >= id",
                Value::Integer(2),
            ),
            ("SELECT COUNT(*) FROM sales WHERE 2 = id", Value::Integer(1)),
        ] {
            let mut swapped = executor.execute(sql).unwrap();
            assert!(
                swapped.next(),
                "swapped comparison produced no count: {sql}"
            );
            assert_eq!(swapped.row().get(0), Some(&expected), "{sql}");
            assert!(
                !swapped.next(),
                "swapped comparison produced extra rows: {sql}"
            );
        }
        let swapped_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
        assert_eq!(swapped_probe.attempts, 2);
        assert_eq!(swapped_probe.applied, 2);
        assert_eq!(swapped_probe.intervals, 2);
        assert_eq!(swapped_probe.candidate_rows, 3);
        assert_eq!(swapped_probe.visible_rows, 3);

        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut bound = executor
            .execute_with_params(
                "SELECT COUNT(*) FROM sales WHERE id BETWEEN $1 AND $2",
                smallvec::smallvec![Value::Integer(2), Value::Integer(3)],
            )
            .unwrap();
        let bound_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
        assert!(bound.next());
        assert_eq!(bound.row().get(0), Some(&Value::Integer(2)));
        assert!(!bound.next());
        assert_eq!(bound_probe.attempts, 1);
        assert_eq!(bound_probe.applied, 1);
        assert_eq!(bound_probe.intervals, 1);
        assert_eq!(bound_probe.candidate_rows, 2);
        assert_eq!(bound_probe.visible_rows, 2);

        let mut named_params = rustc_hash::FxHashMap::default();
        named_params.insert("target".to_string(), Value::Integer(3));
        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut named = executor
            .execute_with_named_params(
                "SELECT COUNT(*) FROM sales WHERE :target = id",
                named_params,
            )
            .unwrap();
        let named_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
        assert!(named.next());
        assert_eq!(named.row().get(0), Some(&Value::Integer(1)));
        assert!(!named.next());
        assert_eq!(named_probe.attempts, 1);
        assert_eq!(named_probe.applied, 1);
        assert_eq!(named_probe.candidate_rows, 1);
    }

    #[test]
    fn filtered_metadata_count_declines_for_transaction_local_rows() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        executor.execute("BEGIN").unwrap();
        executor
            .execute("INSERT INTO sales VALUES (99, 'local', 990)")
            .unwrap();

        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut result = executor
            .execute("SELECT COUNT(*) FROM sales WHERE id = 99")
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();

        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(1)));
        assert!(!result.next());
        assert_eq!(probe.attempts, 1);
        assert_eq!(probe.applied, 0);
        assert_eq!(probe.fallbacks, 1);
        assert_eq!(probe.fallback_unsupported, 1);

        executor.execute("ROLLBACK").unwrap();
        let mut rolled_back = executor
            .execute("SELECT COUNT(*) FROM sales WHERE id = 99")
            .unwrap();
        assert!(rolled_back.next());
        assert_eq!(rolled_back.row().get(0), Some(&Value::Integer(0)));
        assert!(!rolled_back.next());
    }

    #[test]
    fn metadata_primary_key_count_survives_artifact_restart_reopen() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::with_path(dir.path().to_string_lossy().to_string());
        config.persistence.target_volume_rows = 2;
        config.persistence.compact_threshold = 100;
        config.persistence.checkpoint_on_close = false;

        let engine = Arc::new(MVCCEngine::new(config.clone()));
        engine.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&engine));
        setup_test_data(&executor);
        engine.force_checkpoint_cycle().unwrap();
        drop(executor);
        engine.close_engine().unwrap();
        drop(engine);

        let reopened = Arc::new(MVCCEngine::new(config));
        reopened.install_catalog_runtime_binder(radixdb_executor::bind_runtime_catalog);
        reopened.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&reopened));
        radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
        let mut result = executor
            .execute("SELECT COUNT(*) FROM sales WHERE id BETWEEN 2 AND 3")
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
        assert!(!result.next());
        assert_eq!(probe.attempts, 1);
        assert_eq!(probe.applied, 1);
        assert_eq!(probe.intervals, 1);
        assert_eq!(probe.candidate_rows, 2);
        assert_eq!(probe.visible_rows, 2);
        drop(executor);
        reopened.close_engine().unwrap();
    }

    #[test]
    fn metadata_primary_key_count_matches_independent_row_oracle() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        fn scalar_count(executor: &Executor, sql: &str) -> i64 {
            let mut result = executor.execute(sql).unwrap();
            assert!(result.next(), "{sql}");
            let count = match result.row().get(0) {
                Some(Value::Integer(count)) => *count,
                value => panic!("{sql}: expected integer count, got {value:?}"),
            };
            assert!(!result.next(), "{sql}: expected exactly one count row");
            count
        }

        fn row_oracle_count(executor: &Executor, predicate: &str) -> i64 {
            let mut rows = executor
                .execute(&format!("SELECT id FROM sales WHERE {predicate}"))
                .unwrap();
            let mut count = 0i64;
            while rows.next() {
                count += 1;
            }
            assert!(rows.last_error().is_none());
            count
        }

        fn assert_metadata_matches_row_oracle(executor: &Executor, predicate: &str) {
            radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
            let metadata = scalar_count(
                executor,
                &format!("SELECT COUNT(*) FROM sales WHERE {predicate}"),
            );
            let metadata_probe = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
            let oracle = row_oracle_count(executor, predicate);
            assert_eq!(metadata, oracle, "predicate: {predicate}");
            assert_eq!(metadata_probe.attempts, 1, "predicate: {predicate}");
            assert_eq!(metadata_probe.applied, 1, "predicate: {predicate}");
        }

        for predicate in [
            "id = 2",
            "id BETWEEN 2 AND 3",
            "id = 999",
            "id > 3 AND id < 2",
        ] {
            assert_metadata_matches_row_oracle(&executor, predicate);
        }

        // A hot shadow of a cold row must be accounted once, with the same
        // answer as an independent row-producing query.
        executor
            .execute("UPDATE sales SET amount = 250 WHERE id = 2")
            .unwrap();
        assert_metadata_matches_row_oracle(&executor, "id BETWEEN 1 AND 4");

        // A committed tombstone must disappear from both paths.
        executor.execute("DELETE FROM sales WHERE id = 3").unwrap();
        assert_metadata_matches_row_oracle(&executor, "id BETWEEN 1 AND 4");
    }

    #[test]
    fn explain_analyze_reports_executed_metadata_primary_key_count() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut explain = executor
            .execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM sales WHERE id >= 2 AND id <= 3")
            .unwrap();
        let mut lines = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                lines.push(line.to_string());
            }
        }
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Aggregation Path: aggregation.pk_metadata_count")),
            "EXPLAIN ANALYZE must report the operator actually selected: {lines:#?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Access Path: aggregation.pk_metadata_count")),
            "EXPLAIN ANALYZE must expose the physical metadata boundary: {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| {
                line.contains("Metadata PK Count: intervals=1, candidates=2, visible=2")
            }),
            "EXPLAIN ANALYZE must expose request-local metadata count facts: {lines:#?}"
        );
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("Access Path: scan.cold_artifact")),
            "an applied metadata-only count must not be rendered as a artifact-backed payload scan: {lines:#?}"
        );
    }

    #[test]
    fn explain_marks_metadata_count_as_candidate_with_normalized_bounds() {
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut explain = executor
            .execute("EXPLAIN SELECT COUNT(*) FROM sales WHERE 3 >= id AND id >= 2")
            .unwrap();
        let mut lines = Vec::new();
        while explain.next() {
            if let Some(Value::Text(line)) = explain.row().get(0) {
                lines.push(line.to_string());
            }
        }
        assert!(lines
            .iter()
            .any(|line| { line.contains("Aggregation Candidate: aggregation.pk_metadata_count") }));
        assert!(lines
            .iter()
            .any(|line| { line.contains("Normalized INTEGER PK Bounds: lower=[2], upper=[3]") }));
        assert!(lines
            .iter()
            .any(|line| line.contains("Aggregation Path: aggregation.scalar_or_executor")));
    }

    #[test]
    fn test_storage_group_by_where_having_hidden_aggregate_on_cold_artifact() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut statements = parse_sql(
            "SELECT category FROM sales \
             WHERE amount >= 100 \
             GROUP BY category HAVING SUM(amount) >= 300",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
            .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
            .expect(
                "cold artifact-backed WHERE + GROUP BY hidden HAVING aggregate should use storage aggregation",
            );

        assert!(result.next());
        let row = result.row();
        assert_eq!(result.columns(), &["category".to_string()]);
        assert_eq!(
            row.len(),
            1,
            "hidden aggregate must not leak from cold artifact-backed path"
        );
        assert_eq!(row.get(0), Some(&Value::text("electronics")));
        assert!(!result.next());
    }

    #[test]
    fn test_storage_multi_group_by_where_having_path_is_selected_on_cold_artifact() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut statements = parse_sql(
            "SELECT category, id, SUM(amount) FROM sales \
             WHERE amount >= 100 \
             GROUP BY category, id HAVING SUM(amount) >= 100",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        let mut result = executor
        .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
        .expect(
            "multi-column GROUP BY WHERE HAVING over cold artifact-backed data should use storage aggregation",
        );

        let mut rows = Vec::new();
        while result.next() {
            let row = result.row();
            rows.push((
                row.get(0).cloned().unwrap(),
                row.get(1).cloned().unwrap(),
                row.get(2).cloned().unwrap(),
            ));
        }
        rows.sort_by_key(|(_, id, _)| match id {
            Value::Integer(id) => *id,
            other => panic!("unexpected id value: {other:?}"),
        });
        assert_eq!(
            rows,
            vec![
                (
                    Value::text("electronics"),
                    Value::Integer(1),
                    Value::Integer(100)
                ),
                (
                    Value::text("electronics"),
                    Value::Integer(2),
                    Value::Integer(200)
                ),
            ]
        );
    }

    #[test]
    fn test_storage_multi_group_by_requires_select_group_order() {
        use radixdb_sql::{parse_sql, Statement};
        use radixdb_storage::traits::Engine;

        let dir = tempfile::tempdir().unwrap();
        let executor = create_persistent_test_executor(dir.path());
        setup_test_data(&executor);
        executor.engine().force_checkpoint_cycle().unwrap();

        let mut statements = parse_sql(
            "SELECT id, category, SUM(amount) FROM sales \
             GROUP BY category, id",
        )
        .unwrap();
        let stmt = match statements.remove(0) {
            Statement::Select(stmt) => stmt,
            other => panic!("unexpected parsed statement: {other:?}"),
        };
        let classification = QueryClassification::classify(&stmt);
        let tx = executor.engine().begin_transaction().unwrap();
        let table = tx.get_table("sales").unwrap();
        let all_columns: Vec<String> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.name_lower.clone())
            .collect();
        let ctx = ExecutionContext::new();

        assert!(
            executor
                .try_storage_aggregation(table.as_ref(), &stmt, &ctx, &all_columns, &classification)
                .is_none(),
            "storage aggregation must not reorder SELECT group columns implicitly"
        );
    }

    #[test]
    fn test_aggregate_with_alias() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT COUNT(*) AS cnt FROM sales")
            .unwrap();
        let columns = result.columns();
        assert!(columns.contains(&"cnt".to_string()));

        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(4)));
    }

    #[test]
    fn test_multiple_aggregates() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute(
                "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales",
            )
            .unwrap();

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(4))); // COUNT
    }

    #[test]
    fn test_is_aggregate_function() {
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("count"));
        assert!(is_aggregate_function("SUM"));
        assert!(is_aggregate_function("AVG"));
        assert!(is_aggregate_function("MIN"));
        assert!(is_aggregate_function("MAX"));
        assert!(!is_aggregate_function("UPPER"));
        assert!(!is_aggregate_function("CONCAT"));
    }

    #[test]
    fn test_min_max_with_index_optimization() {
        // Test that MIN/MAX queries can use index optimization
        let executor = create_test_executor();

        // Create table with an index
        executor
            .execute("CREATE TABLE indexed_values (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();

        // Create index on the value column
        executor
            .execute("CREATE INDEX idx_value ON indexed_values (value)")
            .unwrap();

        // Insert some data
        executor
            .execute("INSERT INTO indexed_values VALUES (1, 100)")
            .unwrap();
        executor
            .execute("INSERT INTO indexed_values VALUES (2, 50)")
            .unwrap();
        executor
            .execute("INSERT INTO indexed_values VALUES (3, 200)")
            .unwrap();
        executor
            .execute("INSERT INTO indexed_values VALUES (4, 75)")
            .unwrap();
        executor
            .execute("INSERT INTO indexed_values VALUES (5, 150)")
            .unwrap();

        // Test MIN with index
        let mut result = executor
            .execute("SELECT MIN(value) FROM indexed_values")
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(50)));

        // Test MAX with index
        let mut result = executor
            .execute("SELECT MAX(value) FROM indexed_values")
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(200)));

        // Test MIN with alias
        let mut result = executor
            .execute("SELECT MIN(value) AS min_val FROM indexed_values")
            .unwrap();
        let columns = result.columns();
        assert!(columns.contains(&"min_val".to_string()));
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(50)));

        // Test MAX with alias
        let mut result = executor
            .execute("SELECT MAX(value) AS max_val FROM indexed_values")
            .unwrap();
        let columns = result.columns();
        assert!(columns.contains(&"max_val".to_string()));
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(200)));
    }
}
