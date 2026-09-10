#[allow(unused_imports)]
pub use radixdb_executor::window::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use std::sync::Arc;

    fn create_test_executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn setup_test_data(executor: &Executor) {
        executor
        .execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
        )
        .unwrap();
        executor
            .execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)")
            .unwrap();
        executor
            .execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000)")
            .unwrap();
        executor
            .execute("INSERT INTO employees VALUES (3, 'Carol', 'Sales', 80000)")
            .unwrap();
        executor
            .execute("INSERT INTO employees VALUES (4, 'Dave', 'Sales', 85000)")
            .unwrap();
        executor
            .execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 95000)")
            .unwrap();
    }

    #[test]
    fn test_row_number_basic() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT name, ROW_NUMBER() OVER () FROM employees")
            .unwrap();

        let columns = result.columns();
        assert!(columns.len() >= 2);

        let mut count = 0;
        while result.next() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_row_number_with_order() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees")
            .unwrap();

        let mut found_rows = false;
        while result.next() {
            found_rows = true;
        }
        assert!(found_rows);
    }

    #[test]
    fn test_row_number_with_partition() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept) FROM employees")
            .unwrap();

        let mut count = 0;
        while result.next() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_has_window_functions() {
        use crate::executor::query_classification::get_classification;

        // Test with window function
        let mut parser = radixdb_sql::Parser::new("SELECT ROW_NUMBER() OVER () FROM test");
        if let Ok(program) = parser.parse_program() {
            if let radixdb_sql::ast::Statement::Select(stmt) = &program.statements[0] {
                let classification = get_classification(stmt);
                assert!(classification.has_window_functions);
            }
        }

        // Test without window function
        let mut parser2 = radixdb_sql::Parser::new("SELECT * FROM test");
        if let Ok(program) = parser2.parse_program() {
            if let radixdb_sql::ast::Statement::Select(stmt) = &program.statements[0] {
                let classification = get_classification(stmt);
                assert!(!classification.has_window_functions);
            }
        }
    }

    #[test]
    fn test_window_function_info() {
        let info = WindowFunctionInfo {
            name: "ROW_NUMBER".to_string(),
            arguments: vec![],
            partition_by: vec!["dept".to_string()],
            partition_by_exprs: vec![],
            order_by: vec![],
            frame: None,
            column_name: "rn".to_string(),
            is_distinct: false,
        };

        assert_eq!(info.name, "ROW_NUMBER");
        assert_eq!(info.partition_by.len(), 1);
        assert_eq!(info.column_name, "rn");
    }

    #[test]
    fn test_percent_rank_with_order() {
        let executor = create_test_executor();
        setup_test_data(&executor);

        let mut result = executor
            .execute("SELECT salary, PERCENT_RANK() OVER (ORDER BY salary) AS pct FROM employees ORDER BY salary")
            .unwrap();

        let mut pct_ranks = Vec::new();
        let mut row_count = 0;
        while result.next() {
            let row = result.row();
            if let Some(pct) = row.get(1) {
                match pct {
                    radixdb_core::Value::Float(f) => pct_ranks.push(*f),
                    radixdb_core::Value::Integer(i) => pct_ranks.push(*i as f64),
                    _ => {}
                }
            }
            row_count += 1;
        }

        eprintln!("DEBUG: pct_ranks = {:?}", pct_ranks);
        eprintln!("DEBUG: row_count = {}", row_count);
        eprintln!("DEBUG: columns = {:?}", result.columns());

        // First row should have pct_rank = 0.0
        assert!(
            (pct_ranks[0] - 0.0).abs() < 0.001,
            "First pct_rank should be 0.0, got {}",
            pct_ranks[0]
        );

        // Verify monotonically non-decreasing
        for i in 1..pct_ranks.len() {
            assert!(
                pct_ranks[i] >= pct_ranks[i - 1],
                "pct_ranks should be non-decreasing"
            );
        }
    }
}
