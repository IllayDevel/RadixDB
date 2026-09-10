//! Compatibility facade and integration tests for executor-owned CTE logic.

#[allow(unused_imports)]
pub use radixdb_executor::cte::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExecutionContext, Executor};
    use radixdb_core::row_vec::RowVec;
    use radixdb_core::{CompactArc, Row, Value};
    use radixdb_sql::ast::Statement;
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use std::sync::Arc;

    fn make_rows(rows: Vec<Row>) -> RowVec {
        let mut rv = RowVec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            rv.push((i as i64, row));
        }
        rv
    }

    fn create_test_executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn setup_test_data(executor: &Executor) {
        executor
            .execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO products VALUES (1, 'Widget', 10)")
            .unwrap();
        executor
            .execute("INSERT INTO products VALUES (2, 'Gadget', 20)")
            .unwrap();
        executor
            .execute("INSERT INTO products VALUES (3, 'Doodad', 15)")
            .unwrap();
    }

    #[test]
    fn r8_l01_batch_h_recursive_cte_budget_preserves_normal_fixpoint() {
        let executor = create_test_executor();
        let mut result = executor
            .execute(
                "WITH RECURSIVE seq(n) AS (\
                    SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 1000\
                 ) SELECT COUNT(*), MAX(n) FROM seq",
            )
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(1_000)));
        assert_eq!(result.row().get(1), Some(&Value::Integer(1_000)));
    }

    #[test]
    fn test_cte_registry() {
        let mut registry = CteRegistry::new();

        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = make_rows(vec![Row::from_values(vec![
            Value::Integer(1),
            Value::text("test"),
        ])]);

        registry.store("my_cte", columns.clone(), rows);

        assert!(registry.get("my_cte").is_some());
        assert!(registry.get("MY_CTE").is_some()); // case-insensitive

        let (cols, retrieved_rows, _) = registry.get("my_cte").unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(retrieved_rows.len(), 1);

        let ctx = ExecutionContext::new().with_cte_data(registry.data());
        let first = ctx.get_cte_materialized_rows_by_lower("my_cte").unwrap();
        let second = ctx.get_cte_materialized_rows_by_lower("my_cte").unwrap();
        assert!(CompactArc::ptr_eq(&first, &second));
        assert_eq!(first.len(), 1);
    }

    #[test]
    fn repeated_cte_join_reuses_one_request_local_hash_state() {
        let executor = create_test_executor();
        executor
            .execute("CREATE TABLE dictionary (id INTEGER, label TEXT)")
            .unwrap();
        executor
            .execute("CREATE TABLE facts (id INTEGER, first_id INTEGER, second_id INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO dictionary VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            .unwrap();
        for id in 0..100 {
            executor
                .execute(&format!(
                    "INSERT INTO facts VALUES ({id}, {}, {})",
                    id % 3 + 1,
                    (id + 1) % 3 + 1
                ))
                .unwrap();
        }

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let mut result = executor
            .execute(
                "WITH d AS (SELECT * FROM dictionary) \
                 SELECT f.id, d1.label, d2.label FROM facts f \
                 JOIN d d1 ON f.first_id = d1.id \
                 JOIN d d2 ON f.second_id = d2.id",
            )
            .unwrap();
        let mut rows = 0;
        while result.next() {
            rows += 1;
        }
        assert!(result.last_error().is_none());
        assert_eq!(rows, 100);
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(probe.hash_state_builds, 1);
        assert_eq!(probe.hash_state_reuses, 1);
    }

    #[test]
    fn test_simple_cte() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("WITH expensive AS (SELECT * FROM products WHERE price > 12) SELECT * FROM expensive")
            .unwrap();

        let columns = result.columns();
        assert_eq!(columns.len(), 3);

        let mut count = 0;
        while result.next() {
            count += 1;
        }
        // Should have 2 products with price > 12 (Gadget=20, Doodad=15)
        assert_eq!(count, 2);
    }

    #[test]
    fn test_cte_with_aggregation() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute(
                "WITH all_products AS (SELECT * FROM products) SELECT COUNT(*) FROM all_products",
            )
            .unwrap();

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_has_cte() {
        let executor = create_test_executor();

        // With CTE
        let mut parser = radixdb_sql::Parser::new("WITH x AS (SELECT 1) SELECT * FROM x");
        if let Ok(program) = parser.parse_program() {
            if let Statement::Select(stmt) = &program.statements[0] {
                assert!(executor.has_cte(stmt));
            }
        }

        // Without CTE
        let mut parser2 = radixdb_sql::Parser::new("SELECT * FROM test");
        if let Ok(program) = parser2.parse_program() {
            if let Statement::Select(stmt) = &program.statements[0] {
                assert!(!executor.has_cte(stmt));
            }
        }
    }
}
