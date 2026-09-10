//! Compatibility facade and integration tests for executor-owned navigation.

#[allow(unused_imports)]
pub use radixdb_executor::navigation::*;

#[cfg(test)]
pub(crate) use radixdb_executor::navigation::SourceMaterializedTestHookGuard;

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::{ExecutionContext, Executor};
    use radixdb_core::{
        DataType, Error, NavigationErrorCode, ReferenceTargetKey, Result, Row, RowVec,
        SchemaColumnId, Value,
    };
    use radixdb_executor::navigation::verify_unique_lookup_integrity;
    use radixdb_executor::operators::reference_unique_lookup::{
        materialize_unique_lookup_candidates, LookupEdgeCardinality, UniqueLookupRows,
    };
    use radixdb_sql::{Parser, SelectStatement, Statement};
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use radixdb_storage::traits::{Engine, QueryResult};

    fn fixture() -> (Arc<MVCCEngine>, Executor) {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&engine));
        executor
            .execute(
                "CREATE TABLE profiles (
                    id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE departments (
                    id INTEGER PRIMARY KEY,
                    label TEXT NOT NULL,
                    profile_id INTEGER REFERENCES profiles(id)
                )",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TABLE employees (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    department_id INTEGER REFERENCES departments(id),
                    required_department_id INTEGER NOT NULL REFERENCES departments(id)
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO profiles VALUES (100, 'profile')")
            .unwrap();
        executor
            .execute(
                "INSERT INTO departments VALUES
                    (10, 'finance', 100),
                    (20, 'engineering', NULL),
                    (30, 'operations', NULL)",
            )
            .unwrap();
        executor
            .execute(
                "INSERT INTO employees VALUES
                    (1, 'alice', 10, 10),
                    (2, 'bob', 10, 10),
                    (3, 'carol', 20, 20),
                    (4, 'dave', NULL, 30)",
            )
            .unwrap();
        (engine, executor)
    }

    fn select(sql: &str) -> SelectStatement {
        let mut parser = Parser::new(sql);
        let program = parser.parse_program().unwrap();
        let Statement::Select(select) = program.statements.into_iter().next().unwrap() else {
            panic!("expected SELECT")
        };
        select
    }

    fn bind(engine: &MVCCEngine, sql: &str) -> Result<Vec<NavigationExpr>> {
        bind_navigation_paths(engine, &select(sql))
    }

    type SourceMaterializedGate = (
        SourceMaterializedTestHookGuard,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
        Arc<Mutex<Option<thread::ThreadId>>>,
    );

    fn source_materialized_gate() -> SourceMaterializedGate {
        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let owner = Arc::new(Mutex::new(None));
        let hook_owner = Arc::clone(&owner);
        let hook = SourceMaterializedTestHookGuard::install(Arc::new(move |_, _| {
            if hook_owner.lock().expect("source hook owner lock").as_ref()
                != Some(&thread::current().id())
            {
                return;
            }
            reached_tx
                .send(())
                .expect("report materialized navigation source");
            release_rx
                .lock()
                .expect("source gate receiver lock")
                .recv()
                .expect("release navigation target lookup");
        }));
        (hook, reached_rx, release_tx, owner)
    }

    fn first_value(mut result: Box<dyn QueryResult>) -> Result<Value> {
        if !result.next() {
            if let Some(error) = result.last_error() {
                return Err(error);
            }
            return Err(Error::internal("navigation query returned no rows"));
        }
        result
            .row()
            .get(0)
            .cloned()
            .ok_or_else(|| Error::internal("navigation query returned an empty row"))
    }

    #[test]
    fn binds_explicit_shorthand_and_transitive_paths_to_one_identity() {
        let (engine, _executor) = fixture();
        let paths = bind(
            &engine,
            "SELECT e.department_id.label,
                    department_id.label,
                    e.department_id.profile_id.display_name
             FROM employees e",
        )
        .unwrap();

        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].identity(), paths[1].identity());
        assert_eq!(paths[0].root_relation().ordinal(), 0);
        assert_eq!(paths[0].steps().len(), 1);
        assert_eq!(paths[0].steps()[0].target_key_column().ordinal(), 0);
        assert!(paths[0].steps()[0].source_nullable());
        assert_eq!(paths[0].terminal_type(), DataType::Text);
        assert!(paths[0].nullable());
        assert_eq!(paths[2].steps().len(), 2);
        assert_eq!(
            paths[2].display_path(),
            "e.department_id.profile_id.display_name"
        );
        assert_eq!(
            paths[2].steps()[0].target_key(),
            ReferenceTargetKey::PrimaryKey
        );

        let required = bind(
            &engine,
            "SELECT e.required_department_id.label FROM employees e",
        )
        .unwrap();
        assert!(!required[0].nullable());
    }

    #[test]
    fn alias_first_and_relation_instance_identity_handle_self_joins() {
        let (engine, _executor) = fixture();
        let ordinary = bind(
            &engine,
            "SELECT department_id.label
             FROM employees e
             JOIN departments department_id ON e.department_id = department_id.id",
        )
        .unwrap();
        assert!(ordinary.is_empty(), "visible alias must win over shorthand");

        let paths = bind(
            &engine,
            "SELECT e1.department_id.label, e2.department_id.label
             FROM employees e1 JOIN employees e2 ON e1.id = e2.id",
        )
        .unwrap();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0].identity(), paths[1].identity());
        assert_eq!(
            paths[0].root_relation().table(),
            paths[1].root_relation().table()
        );

        let error = bind(
            &engine,
            "SELECT department_id.label
             FROM employees e1 JOIN employees e2 ON e1.id = e2.id",
        )
        .unwrap_err();
        assert_eq!(
            error.navigation_code(),
            Some(NavigationErrorCode::AmbiguousRoot)
        );
    }

    #[test]
    fn aggregate_navigation_uses_batch_graph_without_planner_join_rows() {
        let (engine, executor) = fixture();
        let statement =
            select("SELECT COUNT(department_id.profile_id.display_name) FROM employees");
        let plan = bind_reference_expand_plan(engine.as_ref(), &statement)
            .unwrap()
            .unwrap();
        assert_eq!(
            plan.graph_source_projection_len(&statement, engine.as_ref())
                .unwrap(),
            1
        );

        let (result, metrics) = executor
            .execute_reference_projection_with_metrics(&statement, &plan, &ExecutionContext::new())
            .unwrap();

        assert_eq!(first_value(result).unwrap(), Value::Integer(2));
        assert_eq!(metrics.paths_executed(), 1);
        assert_eq!(metrics.planner_left_join_edges(), 0);
        assert_eq!(metrics.lookup_batches(), 2);
        assert_eq!(metrics.source_rows(), 4);
        assert!(metrics.repeated_keys_eliminated() >= 1);
    }

    #[test]
    fn reports_stable_binding_categories_and_rejects_navigation_wildcard() {
        let (engine, _executor) = fixture();
        let cases = [
            (
                "SELECT ghost.department_id.label FROM employees e",
                NavigationErrorCode::UnknownRoot,
            ),
            (
                "SELECT e.name.value FROM employees e",
                NavigationErrorCode::NotAReference,
            ),
            (
                "SELECT e.department_id.missing FROM employees e",
                NavigationErrorCode::TargetColumnNotFound,
            ),
            (
                "SELECT department_id.* FROM employees e",
                NavigationErrorCode::UnsupportedReferenceShape,
            ),
        ];
        for (sql, expected) in cases {
            let error = bind(&engine, sql).unwrap_err();
            assert_eq!(error.navigation_code(), Some(expected), "{sql}: {error}");
        }
    }

    #[test]
    fn bound_identity_rejects_schema_generation_change() {
        let (engine, executor) = fixture();
        let path = bind(&engine, "SELECT e.department_id.label FROM employees e")
            .unwrap()
            .pop()
            .unwrap();
        executor
            .execute("ALTER TABLE departments ADD COLUMN code TEXT")
            .unwrap();
        let error = path.validate(engine.as_ref()).unwrap_err();
        assert_eq!(
            error.navigation_code(),
            Some(NavigationErrorCode::SchemaChanged)
        );
    }

    #[test]
    fn reference_expand_deduplicates_paths_edges_and_required_columns() {
        let (engine, _executor) = fixture();
        let plan = ReferenceExpandPlan::build(
            bind(
                &engine,
                "SELECT e.department_id.id,
                        e.department_id.label,
                        department_id.label,
                        e.department_id.profile_id
                 FROM employees e",
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(plan.paths().len(), 3);
        assert_eq!(plan.edges().len(), 1);
        let edge = &plan.edges()[0];
        assert_eq!(edge.required_columns().len(), 3);
        assert_eq!(edge.semantics(), ReferenceSemantics::Left);
        assert_eq!(edge.integrity_check(), ReferenceIntegrityCheck::Required);
        assert_eq!(edge.target_key(), ReferenceTargetKey::PrimaryKey);
        assert_eq!(plan.paths()[1].display_paths().len(), 2);
        assert_eq!(plan.paths()[0].edge_indices(), &[0]);
        assert_eq!(plan.paths()[0].terminal_type(), DataType::Integer);
        assert!(plan.paths()[0].nullable());
        assert_eq!(plan.schema_generation(), engine.schema_epoch());
        assert_eq!(edge.identity().depth(), 1);
    }

    #[test]
    fn reference_expand_builds_one_edge_per_explicit_transitive_step() {
        let (engine, _executor) = fixture();
        let plan = bind_reference_expand_plan(
            engine.as_ref(),
            &select(
                "SELECT e.department_id.label,
                        e.department_id.profile_id.display_name
                 FROM employees e",
            ),
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.paths().len(), 2);
        assert_eq!(plan.edges().len(), 2);
        assert_eq!(plan.paths()[1].edge_indices(), &[0, 1]);
        let first_required: Vec<usize> = plan.edges()[0]
            .required_columns()
            .iter()
            .map(SchemaColumnId::ordinal)
            .collect();
        assert!(first_required.contains(&1)); // departments.label
        assert!(first_required.contains(&2)); // departments.profile_id for edge 2

        let lines = plan.explain_lines(engine.as_ref()).unwrap().join("\n");
        assert!(lines.contains("Semantics: LEFT"));
        assert!(lines.contains("Physical Strategy: adaptive_unique_lookup"));
        assert!(lines.contains("Merge Join: disabled_without_ordering_certificate"));
        assert!(lines.contains("employees.department_id -> departments.id"));
        assert!(lines.contains("departments.profile_id -> profiles.id"));
        assert!(lines.contains("Integrity Check: enabled"));
    }

    #[test]
    fn reference_expand_rejects_statements_above_the_documented_path_limit() {
        let (engine, _executor) = fixture();
        let path = bind(&engine, "SELECT e.department_id.label FROM employees e")
            .unwrap()
            .pop()
            .unwrap();
        let error = ReferenceExpandPlan::build(vec![path; MAX_NAVIGATION_PATHS + 1]).unwrap_err();

        assert_eq!(
            error.navigation_code(),
            Some(NavigationErrorCode::UnsupportedReferenceShape)
        );
        assert!(error
            .to_string()
            .contains("statement contains 257 navigable paths; maximum is 256"));
    }

    #[test]
    fn cached_and_parameter_fast_paths_bind_before_physical_planning() {
        let (engine, executor) = fixture();
        let sql = "SELECT department_id.label FROM employees WHERE id = $1";
        let cached = executor.get_or_create_plan(sql).unwrap();
        let context = ExecutionContext::with_params(vec![radixdb_core::Value::Integer(1)].into());

        let mut result = executor
            .execute_with_cached_plan(&cached, &context)
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::text("finance")));
        assert!(!result.next());
        let first_generation = match &*cached.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => plan.schema_generation(),
            state => panic!("expected cached reference plan, got {state:?}"),
        };

        executor
            .execute("ALTER TABLE departments ADD COLUMN code TEXT")
            .unwrap();
        assert_ne!(first_generation, engine.schema_epoch());
        let mut result = executor
            .execute_with_cached_plan(&cached, &context)
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::text("finance")));
        assert!(!result.next());
        match &*cached.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => {
                assert_eq!(plan.schema_generation(), engine.schema_epoch())
            }
            state => panic!("expected rebound reference plan, got {state:?}"),
        }

        assert!(executor
            .try_fast_path_with_params(sql, &[radixdb_core::Value::Integer(1)])
            .is_none());
    }

    #[test]
    fn cached_navigation_rebinds_across_rename_constraint_and_object_replacement() {
        let (engine, executor) = fixture();
        let context = ExecutionContext::with_params(vec![Value::Integer(1)].into());
        let original_sql = "SELECT department_id.label FROM employees WHERE id = $1";
        let original = executor.get_or_create_plan(original_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&original, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("finance")
        );
        let original_generation = match &*original.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => plan.schema_generation(),
            state => panic!("expected cached reference plan, got {state:?}"),
        };

        executor
            .execute("ALTER TABLE employees RENAME TO staff")
            .unwrap();
        assert!(engine.schema_epoch() > original_generation);
        let renamed_root = match executor.execute_with_cached_plan(&original, &context) {
            Err(error) => error,
            Ok(_) => panic!("renamed root unexpectedly used a stale cached plan"),
        };
        assert!(
            matches!(
                &renamed_root,
                Error::TableNotFound(_) | Error::TableOrViewNotFound(_)
            ) || renamed_root.navigation_code() == Some(NavigationErrorCode::UnknownRoot),
            "unexpected renamed-root error: {renamed_root}"
        );

        let renamed_root_sql = "SELECT department_id.label FROM staff WHERE id = $1";
        let renamed_root_plan = executor.get_or_create_plan(renamed_root_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&renamed_root_plan, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("finance")
        );

        executor
            .execute("ALTER TABLE departments RENAME COLUMN label TO title")
            .unwrap();
        let missing_terminal = match executor.execute_with_cached_plan(&renamed_root_plan, &context)
        {
            Err(error) => error,
            Ok(_) => panic!("renamed terminal unexpectedly used a stale cached plan"),
        };
        assert_eq!(
            missing_terminal.navigation_code(),
            Some(NavigationErrorCode::TargetColumnNotFound)
        );

        let current_sql = "SELECT department_id.title FROM staff WHERE id = $1";
        let current = executor.get_or_create_plan(current_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&current, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("finance")
        );

        // A same-name table is a new schema object. First recreate the root
        // without a reference and prove that stale navigation cannot execute;
        // then recreate both endpoints with a replacement FK and prove clean
        // rebind. DROP CONSTRAINT is not a supported DDL form, so replacement
        // intentionally uses the engine's supported DROP/CREATE lifecycle.
        executor.execute("DROP TABLE staff").unwrap();
        executor
            .execute(
                "CREATE TABLE staff (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    department_id INTEGER
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO staff VALUES (1, 'plain', 10)")
            .unwrap();
        let no_reference = match executor.execute_with_cached_plan(&current, &context) {
            Err(error) => error,
            Ok(_) => panic!("recreated non-reference root unexpectedly used a stale cached plan"),
        };
        assert_eq!(
            no_reference.navigation_code(),
            Some(NavigationErrorCode::NotAReference)
        );

        executor.execute("DROP TABLE staff").unwrap();
        executor.execute("DROP TABLE departments").unwrap();
        executor
            .execute(
                "CREATE TABLE departments (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    profile_id INTEGER REFERENCES profiles(id)
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO departments VALUES (10, 'replacement', 100)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE staff (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    department_id INTEGER REFERENCES departments(id)
                )",
            )
            .unwrap();
        executor
            .execute("INSERT INTO staff VALUES (1, 'rebound', 10)")
            .unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&current, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("replacement")
        );
        match &*current.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => {
                assert_eq!(plan.schema_generation(), engine.schema_epoch())
            }
            state => panic!("expected final rebound reference plan, got {state:?}"),
        };
    }

    #[test]
    fn cached_transitive_navigation_validates_every_path_step_generation() {
        let (engine, executor) = fixture();
        let context = ExecutionContext::with_params(vec![Value::Integer(1)].into());
        let original_sql =
            "SELECT department_id.profile_id.display_name FROM employees WHERE id = $1";
        let original = executor.get_or_create_plan(original_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&original, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("profile")
        );
        let generation = match &*original.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => {
                assert_eq!(plan.edges().len(), 2);
                plan.schema_generation()
            }
            state => panic!("expected transitive cached plan, got {state:?}"),
        };

        executor
            .execute("ALTER TABLE profiles RENAME COLUMN display_name TO title")
            .unwrap();
        assert!(engine.schema_epoch() > generation);
        let stale_terminal = match executor.execute_with_cached_plan(&original, &context) {
            Err(error) => error,
            Ok(_) => panic!("terminal rename unexpectedly used a stale transitive plan"),
        };
        assert_eq!(
            stale_terminal.navigation_code(),
            Some(NavigationErrorCode::TargetColumnNotFound)
        );

        let renamed_terminal_sql =
            "SELECT department_id.profile_id.title FROM employees WHERE id = $1";
        let renamed_terminal = executor.get_or_create_plan(renamed_terminal_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&renamed_terminal, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("profile")
        );

        executor
            .execute("ALTER TABLE departments RENAME COLUMN profile_id TO owner_profile_id")
            .unwrap();
        let stale_middle_step = match executor.execute_with_cached_plan(&renamed_terminal, &context)
        {
            Err(error) => error,
            Ok(_) => panic!("middle-step rename unexpectedly used a stale transitive plan"),
        };
        assert!(
            matches!(&stale_middle_step, Error::ColumnNotFound(_))
                || stale_middle_step.navigation_code() == Some(NavigationErrorCode::NotAReference),
            "unexpected middle-step error: {stale_middle_step}"
        );

        let rebound_sql =
            "SELECT department_id.owner_profile_id.title FROM employees WHERE id = $1";
        let rebound = executor.get_or_create_plan(rebound_sql).unwrap();
        assert_eq!(
            first_value(
                executor
                    .execute_with_cached_plan(&rebound, &context)
                    .unwrap()
            )
            .unwrap(),
            Value::text("profile")
        );
        match &*rebound.reference_expand.read().unwrap() {
            CachedReferenceExpand::Plan(plan) => {
                assert_eq!(plan.edges().len(), 2);
                assert_eq!(plan.schema_generation(), engine.schema_epoch());
            }
            state => panic!("expected rebound transitive plan, got {state:?}"),
        };
    }

    #[test]
    fn statement_visibility_fence_keeps_direct_navigation_on_one_commit_epoch() {
        let (engine, _executor) = fixture();
        let (hook, reached_rx, release_tx, hook_owner) = source_materialized_gate();
        let context = ExecutionContext::new();
        let observer = context.clone();

        let query_engine = Arc::clone(&engine);
        let query = thread::spawn(move || {
            *hook_owner.lock().expect("record source hook owner") = Some(thread::current().id());
            let executor = Executor::new(query_engine);
            first_value(executor.execute_with_context(
                "SELECT department_id.label FROM employees WHERE id = 1",
                &context,
            )?)
        });
        reached_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("navigation source materialized");
        assert_eq!(observer.active_reference_expands(), 1);

        let writer_engine = Arc::clone(&engine);
        let (writer_started_tx, writer_started_rx) = mpsc::channel();
        let (writer_done_tx, writer_done_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let executor = Executor::new(writer_engine);
            writer_started_tx.send(()).expect("report writer start");
            let outcome = executor
                .execute("UPDATE departments SET label = 'new-finance' WHERE id = 10")
                .map(|_| ());
            writer_done_tx
                .send(outcome.clone())
                .expect("report writer outcome");
            outcome
        });
        writer_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer started");
        assert!(
            writer_done_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "writer published while navigation statement was between root and target reads"
        );

        release_tx.send(()).expect("release navigation lookup");
        assert_eq!(
            query.join().expect("query thread").unwrap(),
            Value::text("finance")
        );
        writer_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer completes after SELECT")
            .unwrap();
        writer.join().expect("writer thread").unwrap();
        drop(hook);
        assert_eq!(observer.active_reference_expands(), 0);

        let executor = Executor::new(engine);
        assert_eq!(
            first_value(
                executor
                    .execute("SELECT department_id.label FROM employees WHERE id = 1")
                    .unwrap()
            )
            .unwrap(),
            Value::text("new-finance")
        );
    }

    #[test]
    fn concurrent_target_delete_cannot_mix_cascade_or_set_null_state() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let setup = Executor::new(Arc::clone(&engine));
        setup
            .execute("CREATE TABLE nav_parents (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            .unwrap();
        setup
            .execute(
                "CREATE TABLE nav_roots (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES nav_parents(id) ON DELETE SET NULL
                )",
            )
            .unwrap();
        setup
            .execute(
                "CREATE TABLE nav_dependents (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES nav_parents(id) ON DELETE CASCADE
                )",
            )
            .unwrap();
        setup
            .execute("INSERT INTO nav_parents VALUES (1, 'before-delete')")
            .unwrap();
        setup
            .execute("INSERT INTO nav_roots VALUES (1, 1)")
            .unwrap();
        setup
            .execute("INSERT INTO nav_dependents VALUES (1, 1)")
            .unwrap();

        let (hook, reached_rx, release_tx, hook_owner) = source_materialized_gate();
        let context = ExecutionContext::new();
        let observer = context.clone();
        let query_engine = Arc::clone(&engine);
        let query = thread::spawn(move || {
            *hook_owner.lock().expect("record source hook owner") = Some(thread::current().id());
            let executor = Executor::new(query_engine);
            first_value(executor.execute_with_context(
                "SELECT parent_id.label FROM nav_roots
                 WHERE parent_id.label = 'before-delete'",
                &context,
            )?)
        });
        reached_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("predicate navigation source materialized");

        let writer_engine = Arc::clone(&engine);
        let (writer_started_tx, writer_started_rx) = mpsc::channel();
        let (writer_done_tx, writer_done_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let executor = Executor::new(writer_engine);
            writer_started_tx.send(()).expect("report delete start");
            let outcome = executor
                .execute("DELETE FROM nav_parents WHERE id = 1")
                .map(|_| ());
            writer_done_tx
                .send(outcome.clone())
                .expect("report delete outcome");
            outcome
        });
        writer_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("delete started");
        assert!(
            writer_done_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "delete published cascade/SET NULL state inside the navigation SELECT"
        );

        release_tx.send(()).expect("release predicate lookup");
        assert_eq!(
            query.join().expect("query thread").unwrap(),
            Value::text("before-delete")
        );
        writer_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("delete completes after SELECT")
            .unwrap();
        writer.join().expect("writer thread").unwrap();
        drop(hook);
        assert_eq!(observer.active_reference_expands(), 0);

        assert_eq!(
            first_value(
                setup
                    .execute("SELECT parent_id FROM nav_roots WHERE id = 1")
                    .unwrap()
            )
            .unwrap(),
            Value::null(DataType::Integer)
        );
        assert_eq!(
            first_value(
                setup
                    .execute("SELECT COUNT(*) FROM nav_dependents")
                    .unwrap()
            )
            .unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn cancellation_and_timeout_release_navigation_execution_resources() {
        fn run_cancel_case(engine: &Arc<MVCCEngine>, timeout_ms: Option<u64>) {
            let (hook, reached_rx, release_tx, hook_owner) = source_materialized_gate();
            let mut context = ExecutionContext::new();
            if let Some(timeout_ms) = timeout_ms {
                context.set_timeout_ms(timeout_ms);
            }
            let observer = context.clone();
            let cancellation = context.cancellation_handle();
            let query_engine = Arc::clone(engine);
            let query = thread::spawn(move || {
                *hook_owner.lock().expect("record source hook owner") =
                    Some(thread::current().id());
                Executor::new(query_engine).execute_with_context(
                    "SELECT department_id.label FROM employees WHERE id = 1",
                    &context,
                )
            });
            reached_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("navigation source materialized");
            assert_eq!(observer.active_reference_expands(), 1);
            assert_eq!(
                crate::executor::context::pending_timeout_count_for(&observer),
                usize::from(timeout_ms.is_some()),
                "navigation query did not own the expected timeout registration"
            );

            if timeout_ms.is_some() {
                let deadline = Instant::now() + Duration::from_secs(2);
                while !observer.is_cancelled() && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(2));
                }
                assert!(observer.is_cancelled(), "navigation timeout did not fire");
                assert!(observer.did_time_out());
            } else {
                cancellation.cancel();
                assert!(!observer.did_time_out());
            }
            release_tx.send(()).expect("release cancelled lookup");
            let error = match query.join().expect("query thread") {
                Err(error) => error,
                Ok(_) => panic!("cancelled navigation query unexpectedly succeeded"),
            };
            assert_eq!(error, Error::QueryCancelled);
            drop(hook);
            assert_eq!(observer.active_reference_expands(), 0);
            assert_eq!(
                crate::executor::context::pending_timeout_count_for(&observer),
                0,
                "timeout registration leaked after navigation cancellation"
            );
        }

        let (engine, _executor) = fixture();
        let counters_before = radixdb_storage::instrumentation::snapshot();
        run_cancel_case(&engine, None);
        run_cancel_case(&engine, Some(20));
        let counters_after = radixdb_storage::instrumentation::snapshot();
        assert!(counters_after.navigation_cancellations > counters_before.navigation_cancellations);
        assert!(counters_after.navigation_timeouts > counters_before.navigation_timeouts);
    }

    #[test]
    fn projection_execution_deduplicates_batch_keys_and_preserves_rows() {
        let (engine, executor) = fixture();
        let statement = select(
            "SELECT id, department_id.label, department_id.id,
                        department_id.profile_id
             FROM employees ORDER BY id",
        );
        let plan = bind_reference_expand_plan(engine.as_ref(), &statement)
            .unwrap()
            .unwrap();
        let counters_before = radixdb_storage::instrumentation::snapshot();
        let (mut result, metrics) = executor
            .execute_reference_projection_with_metrics(&statement, &plan, &ExecutionContext::new())
            .unwrap();

        let mut rows = Vec::new();
        while result.next() {
            rows.push(result.take_row());
        }
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get(1), Some(&Value::text("finance")));
        assert_eq!(rows[1].get(1), Some(&Value::text("finance")));
        assert_eq!(rows[2].get(1), Some(&Value::text("engineering")));
        assert_eq!(rows[3].get(1), Some(&Value::null(DataType::Text)));
        assert_eq!(rows[0].get(2), Some(&Value::Integer(10)));
        assert_eq!(rows[0].get(3), Some(&Value::Integer(100)));
        assert_eq!(rows[2].get(3), Some(&Value::null(DataType::Integer)));
        assert_eq!(rows[3].get(3), Some(&Value::null(DataType::Integer)));
        assert_eq!(metrics.source_rows(), 4);
        assert_eq!(metrics.paths_planned(), 3);
        assert_eq!(metrics.paths_executed(), 3);
        assert_eq!(metrics.null_source_keys(), 1);
        assert_eq!(metrics.distinct_keys(), 2);
        assert_eq!(metrics.repeated_keys_eliminated(), 1);
        assert_eq!(metrics.lookup_batches(), 1);
        assert_eq!(metrics.lookup_hits(), 2);
        assert_eq!(metrics.lookup_misses(), 0);
        assert_eq!(metrics.direct_edges(), 0);
        assert_eq!(metrics.index_nested_loop_edges(), 1);
        assert_eq!(metrics.batch_edges(), 0);
        assert_eq!(metrics.hash_edges(), 0);
        assert_eq!(metrics.fallback_edges(), 0);
        let counters_after = radixdb_storage::instrumentation::snapshot();
        assert!(
            counters_after.navigation_paths_executed
                >= counters_before.navigation_paths_executed + metrics.paths_executed() as u64
        );
        assert!(
            counters_after.navigation_repeated_keys_eliminated
                >= counters_before.navigation_repeated_keys_eliminated
                    + metrics.repeated_keys_eliminated() as u64
        );
    }

    #[test]
    fn projection_execution_uses_direct_lookup_for_one_key_and_honours_cancellation() {
        let (engine, executor) = fixture();
        let statement = select("SELECT department_id.label FROM employees WHERE id = 1");
        let plan = bind_reference_expand_plan(engine.as_ref(), &statement)
            .unwrap()
            .unwrap();
        let (mut result, metrics) = executor
            .execute_reference_projection_with_metrics(&statement, &plan, &ExecutionContext::new())
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::text("finance")));
        assert_eq!(metrics.direct_edges(), 1);
        assert_eq!(metrics.batch_edges(), 0);
        assert_eq!(metrics.fallback_edges(), 0);

        let null_statement = select("SELECT department_id.label FROM employees WHERE id = 4");
        let null_plan = bind_reference_expand_plan(engine.as_ref(), &null_statement)
            .unwrap()
            .unwrap();
        let (mut null_result, null_metrics) = executor
            .execute_reference_projection_with_metrics(
                &null_statement,
                &null_plan,
                &ExecutionContext::new(),
            )
            .unwrap();
        assert!(null_result.next());
        assert_eq!(null_result.row().get(0), Some(&Value::null(DataType::Text)));
        assert_eq!(null_metrics.null_source_keys(), 1);
        assert_eq!(null_metrics.distinct_keys(), 0);
        assert_eq!(null_metrics.lookup_batches(), 0);

        let required_statement =
            select("SELECT required_department_id.label FROM employees ORDER BY id");
        let required_plan = bind_reference_expand_plan(engine.as_ref(), &required_statement)
            .unwrap()
            .unwrap();
        let (mut required_result, required_metrics) = executor
            .execute_reference_projection_with_metrics(
                &required_statement,
                &required_plan,
                &ExecutionContext::new(),
            )
            .unwrap();
        let mut labels = Vec::new();
        while required_result.next() {
            labels.push(required_result.take_row().get(0).cloned().unwrap());
        }
        assert_eq!(
            labels,
            vec![
                Value::text("finance"),
                Value::text("finance"),
                Value::text("engineering"),
                Value::text("operations"),
            ]
        );
        assert_eq!(required_metrics.null_source_keys(), 0);
        assert_eq!(required_metrics.lookup_batches(), 1);
        assert_eq!(required_metrics.index_nested_loop_edges(), 1);

        let cancelled = ExecutionContext::new();
        cancelled.cancel();
        let cancelled_error = match executor
            .execute_reference_projection_with_metrics(&statement, &plan, &cancelled)
        {
            Err(error) => error,
            Ok(_) => panic!("cancelled navigation query unexpectedly succeeded"),
        };
        assert_eq!(cancelled_error, Error::QueryCancelled);
    }

    #[test]
    fn large_dense_reference_input_uses_target_first_projected_hash_scan() {
        let dense_keys = (1..=160).map(Value::Integer).collect::<Vec<_>>();
        assert!(matches!(
            UniqueLookupRows::new(&dense_keys),
            UniqueLookupRows::DenseInteger { .. }
        ));
        assert!(matches!(
            UniqueLookupRows::new(&[Value::Integer(1), Value::Integer(10_000)]),
            UniqueLookupRows::Hash { .. }
        ));

        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&engine));
        executor
            .execute("CREATE TABLE targets (id INTEGER PRIMARY KEY, label TEXT, unused TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE roots (
                    id INTEGER PRIMARY KEY,
                    target_id INTEGER REFERENCES targets(id)
                )",
            )
            .unwrap();

        let targets = (1..=160)
            .map(|id| format!("({id}, 'label-{id}', 'unused-{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        let mut root_values = (1..=160)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>();
        root_values.push("(161, NULL)".to_string());
        let roots = root_values.join(",");
        executor
            .execute(&format!("INSERT INTO targets VALUES {targets}"))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO roots VALUES {roots}"))
            .unwrap();

        let statement = select("SELECT r.target_id.label FROM roots r ORDER BY r.id");
        let plan = bind_reference_expand_plan(engine.as_ref(), &statement)
            .unwrap()
            .unwrap();
        let (mut result, metrics) = executor
            .execute_reference_projection_with_metrics(&statement, &plan, &ExecutionContext::new())
            .unwrap();
        let mut labels = Vec::new();
        while result.next() {
            labels.push(result.take_row().get(0).cloned().unwrap());
        }

        assert_eq!(labels.len(), 161);
        assert_eq!(labels.first(), Some(&Value::text("label-1")));
        assert_eq!(labels[159], Value::text("label-160"));
        assert_eq!(labels.last(), Some(&Value::null(DataType::Text)));
        assert_eq!(metrics.hash_edges(), 1);
        assert_eq!(metrics.target_first_edges(), 1);
        assert_eq!(metrics.merge_edges(), 0);
        assert_eq!(metrics.hot_edges(), 1);
        assert_eq!(metrics.edge_projected_columns(0), Some(2));
        assert_eq!(metrics.edge_reverse_source_index_eligible(0), Some(true));

        let batch_statement = select(
            "SELECT r.target_id.label
             FROM roots r
             WHERE r.id <= 64
             ORDER BY r.id",
        );
        let batch_plan = bind_reference_expand_plan(engine.as_ref(), &batch_statement)
            .unwrap()
            .unwrap();
        let (_batch_result, batch_metrics) = executor
            .execute_reference_projection_with_metrics(
                &batch_statement,
                &batch_plan,
                &ExecutionContext::new(),
            )
            .unwrap();
        assert_eq!(batch_metrics.batch_edges(), 1);
        assert_eq!(batch_metrics.index_nested_loop_edges(), 0);
        assert_eq!(batch_metrics.hash_edges(), 0);

        executor
            .execute("CREATE TABLE ordered_targets (id INTEGER PRIMARY KEY, label TEXT)")
            .unwrap();
        executor
            .execute(
                "CREATE TABLE ordered_roots (
                    id INTEGER PRIMARY KEY,
                    target_id INTEGER REFERENCES ordered_targets(id)
                )",
            )
            .unwrap();
        let ordered_targets = (1..=160)
            .map(|id| format!("({id}, 'label-{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!(
                "INSERT INTO ordered_targets VALUES {ordered_targets}"
            ))
            .unwrap();
        executor
            .execute(&format!("INSERT INTO ordered_roots VALUES {roots}"))
            .unwrap();
        let ordered_statement =
            select("SELECT r.target_id.label FROM ordered_roots r ORDER BY r.id");
        let ordered_plan = bind_reference_expand_plan(engine.as_ref(), &ordered_statement)
            .unwrap()
            .unwrap();
        let (mut ordered_result, ordered_metrics) = executor
            .execute_reference_projection_with_metrics(
                &ordered_statement,
                &ordered_plan,
                &ExecutionContext::new(),
            )
            .unwrap();
        let mut ordered_count = 0usize;
        while ordered_result.next() {
            ordered_count += 1;
        }
        assert_eq!(ordered_count, 161);
        assert_eq!(ordered_metrics.merge_edges(), 1);
        assert_eq!(ordered_metrics.hash_edges(), 0);
        assert_eq!(ordered_metrics.target_first_edges(), 1);
    }

    #[test]
    fn target_candidate_verifier_fails_closed_for_missing_and_duplicate_rows() {
        let (engine, _executor) = fixture();
        let plan = bind_reference_expand_plan(
            engine.as_ref(),
            &select("SELECT department_id.label FROM employees"),
        )
        .unwrap()
        .unwrap();
        let edge = &plan.edges()[0];
        let counters_before = radixdb_storage::instrumentation::snapshot();

        let missing_batch = materialize_unique_lookup_candidates(
            &[Value::Integer(99)],
            RowVec::new(),
            0,
            LookupEdgeCardinality::ExactlyOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();
        let missing = verify_unique_lookup_integrity(edge, missing_batch.integrity).unwrap_err();
        assert_eq!(
            missing.navigation_code(),
            Some(NavigationErrorCode::TargetMissing)
        );

        let duplicate_rows = RowVec::from_vec(vec![
            (
                1,
                Row::from_values(vec![Value::Integer(10), Value::text("a")]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(10), Value::text("b")]),
            ),
        ]);
        let duplicate_batch = materialize_unique_lookup_candidates(
            &[Value::Integer(10)],
            duplicate_rows,
            0,
            LookupEdgeCardinality::ExactlyOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();
        let duplicate =
            verify_unique_lookup_integrity(edge, duplicate_batch.integrity).unwrap_err();
        assert_eq!(
            duplicate.navigation_code(),
            Some(NavigationErrorCode::TargetNotUnique)
        );
        let counters_after = radixdb_storage::instrumentation::snapshot();
        assert!(
            counters_after.navigation_integrity_failures
                >= counters_before.navigation_integrity_failures + 2
        );
    }
}
