use super::*;
use crate::context::ExecutionContextBuilder;
use radixdb_storage::config::Config;
use radixdb_storage::instrumentation;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::Engine;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

fn create_test_executor() -> Executor {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    Executor::new(Arc::new(engine))
}

fn create_composed_test_engine(config: Config) -> MVCCEngine {
    MVCCEngine::new_with_composition_binders(
        config,
        crate::mutation::partial_index::bind_from_sql,
        crate::mutation::row_validation::bind,
        crate::mutation::view_binding::bind_from_sql,
        radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(
            crate::catalog::bind_runtime_catalog,
        ),
    )
}

fn create_persistent_test_executor(path: &Path) -> (Executor, Arc<MVCCEngine>, Config) {
    let mut config = Config::with_path(path.to_string_lossy().to_string());
    config.persistence.target_volume_rows = 2;
    config.persistence.compact_threshold = 100;
    config.persistence.checkpoint_on_close = false;
    let engine = Arc::new(create_composed_test_engine(config.clone()));
    engine.open_engine().unwrap();
    (Executor::new(Arc::clone(&engine)), engine, config)
}

fn scalar_count(executor: &Executor, sql: &str) -> i64 {
    let mut result = executor.execute(sql).unwrap();
    assert!(result.next(), "{sql}");
    let count = match result.row().get(0) {
        Some(Value::Integer(value)) => *value,
        value => panic!("{sql}: expected integer COUNT, got {value:?}"),
    };
    assert!(!result.next(), "{sql}");
    count
}

fn drain_rows(mut result: Box<dyn QueryResult>) -> Vec<Row> {
    let mut rows = Vec::new();
    while result.next() {
        rows.push(result.row().clone());
    }
    rows
}

fn execute_derived_once(executor: &Executor, sql: &str) -> Vec<Row> {
    instrumentation::begin_derived_subquery_probe();
    let rows = drain_rows(executor.execute(sql).unwrap());
    let probe = instrumentation::end_derived_subquery_probe();
    assert_eq!(probe.executes, 1, "{sql}");
    rows
}

fn execute_derived_error_once(executor: &Executor, sql: &str) -> String {
    instrumentation::begin_derived_subquery_probe();
    let error = match executor.execute(sql) {
        Ok(mut result) => {
            while result.next() {}
            result
                .last_error()
                .unwrap_or_else(|| panic!("{sql}: expected derived source error"))
                .to_string()
        }
        Err(error) => error.to_string(),
    };
    let probe = instrumentation::end_derived_subquery_probe();
    assert_eq!(probe.executes, 1, "{sql}: {error}");
    error
}

include!("execution_and_errors.rs");
include!("projection_and_join.rs");
include!("cost_and_recovery.rs");
