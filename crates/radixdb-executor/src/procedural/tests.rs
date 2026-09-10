use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use radixdb_catalog::{
    CatalogDataType, CatalogObject, CatalogPayload, JobSchedule, ObjectId, ObjectKind,
    ResourcePolicy,
};
use radixdb_core::{DataType, Value};
use radixdb_procedural::{
    verify, BasicBlock, BlockId, CursorId, DiagnosticKind, ExceptionRoute, Instruction,
    PrincipalContext, ProceduralResult, Program, ProgramIdentity, RuntimeType, RuntimeValue,
    SlotDefinition, SlotId, SpannedInstruction, SpannedTerminator, Terminator,
};
use radixdb_sql::{parse_sql, InfixOperator, Statement};
use radixdb_storage::config::Config;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::QueryResult;

use crate::{ExecutionContext, Executor};

use super::{JobAttemptMetadata, ProceduralResultStage};

#[derive(Default)]
struct TestStage {
    staged: Vec<Vec<RuntimeValue>>,
    published: Vec<Vec<RuntimeValue>>,
    begin_count: u64,
    publish_count: u64,
    discard_count: u64,
}

impl ProceduralResultStage for TestStage {
    fn begin(&mut self) -> ProceduralResult<()> {
        self.begin_count += 1;
        self.staged.clear();
        Ok(())
    }

    fn stage_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        self.staged.push(row);
        Ok(())
    }

    fn publish(&mut self) {
        self.publish_count += 1;
        self.published.append(&mut self.staged);
    }

    fn discard(&mut self) {
        self.discard_count += 1;
        self.staged.clear();
    }
}

fn executor() -> Executor {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    Executor::new(Arc::new(engine))
}

fn persistent_executor(path: &std::path::Path) -> (Executor, Arc<MVCCEngine>) {
    let mut config = Config::with_path(path.to_string_lossy().to_string());
    config.persistence.checkpoint_on_close = true;
    let engine = Arc::new(MVCCEngine::new_with_composition_binders(
        config,
        crate::mutation::partial_index::bind_from_sql,
        crate::mutation::row_validation::bind,
        crate::mutation::view_binding::bind_from_sql,
        radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(
            crate::catalog::bind_runtime_catalog,
        ),
    ));
    engine.open_engine().unwrap();
    (Executor::new(Arc::clone(&engine)), engine)
}

fn routine(executor: &Executor, kind: ObjectKind, name: &str) -> Option<CatalogObject> {
    executor
        .engine
        .pin_catalog()
        .unwrap()
        .find_routine(ObjectId::BOOTSTRAP_NAMESPACE, kind, name, &[])
        .unwrap()
        .cloned()
}

fn routine_named(executor: &Executor, kind: ObjectKind, name: &str) -> Option<CatalogObject> {
    executor
        .engine
        .pin_catalog()
        .unwrap()
        .objects_of_kind(kind)
        .find(|object| object.name().normalized().as_str() == name)
        .cloned()
}

fn named_object(executor: &Executor, kind: ObjectKind, name: &str) -> Option<CatalogObject> {
    executor
        .engine
        .pin_catalog()
        .unwrap()
        .objects_of_kind(kind)
        .find(|object| object.name().normalized().as_str() == name)
        .cloned()
}

fn principal_id(executor: &Executor, name: &str) -> ObjectId {
    named_object(executor, ObjectKind::Principal, name)
        .expect("named Principal")
        .id()
}

mod jobs_lifecycle;
#[test]
fn trigger_record_fields_bind_typed_static_sql_leaves() {
    let executor = executor();
    executor
        .execute(
            "CREATE TABLE trigger_field_rows ( \
                 id INTEGER PRIMARY KEY, \"Quoted Value\" INTEGER, marker TEXT NOT NULL)",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TABLE trigger_field_audit ( \
                 id INTEGER PRIMARY KEY, inserted_value INTEGER, old_value INTEGER, \
                 new_value INTEGER, selected_value INTEGER)",
        )
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION audit_insert_fields() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS DECLARE selected_value INTEGER; BEGIN \
                 SELECT \"Quoted Value\" INTO STRICT selected_value \
                   FROM trigger_field_rows WHERE id = :NEW.id; \
                 INSERT INTO trigger_field_audit VALUES \
                   (:NEW.id, :NEW.\"Quoted Value\", NULL, NULL, :selected_value); \
                 RETURN NULL; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION audit_update_fields() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN \
                 UPDATE trigger_field_audit \
                    SET old_value = :OLD.\"Quoted Value\", \
                        new_value = :NEW.\"Quoted Value\" \
                  WHERE id = :NEW.id; \
                 RETURN NULL; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER audit_insert AFTER INSERT ON trigger_field_rows FOR EACH ROW \
             EXECUTE FUNCTION audit_insert_fields();",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER audit_update AFTER UPDATE OF \"Quoted Value\" ON trigger_field_rows \
             FOR EACH ROW EXECUTE FUNCTION audit_update_fields();",
        )
        .unwrap();

    executor
        .execute("INSERT INTO trigger_field_rows VALUES (1, 10, 'ok')")
        .unwrap();
    executor
        .execute("UPDATE trigger_field_rows SET \"Quoted Value\" = 20 WHERE id = 1")
        .unwrap();
    assert_eq!(
        integer_column(
            executor
                .execute(
                    "SELECT inserted_value FROM trigger_field_audit WHERE id = 1 \
                     UNION ALL SELECT old_value FROM trigger_field_audit WHERE id = 1 \
                     UNION ALL SELECT new_value FROM trigger_field_audit WHERE id = 1 \
                     UNION ALL SELECT selected_value FROM trigger_field_audit WHERE id = 1",
                )
                .unwrap(),
        ),
        vec![10, 10, 20, 10]
    );

    for (function_name, body) in [
        (
            "invalid_old_on_insert",
            "INSERT INTO trigger_field_audit (id) VALUES (:OLD.id)",
        ),
        (
            "invalid_whole_new",
            "INSERT INTO trigger_field_audit (id) VALUES (:NEW)",
        ),
    ] {
        executor
            .execute(&format!(
                "CREATE FUNCTION {function_name}() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
                 SECURITY INVOKER AS BEGIN {body}; RETURN NULL; END;"
            ))
            .unwrap();
        let error = match executor.execute(&format!(
            "CREATE TRIGGER {function_name}_trigger AFTER INSERT ON trigger_field_rows \
                 FOR EACH ROW EXECUTE FUNCTION {function_name}();"
        )) {
            Ok(_) => panic!("invalid trigger parameter unexpectedly attached"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("trigger attachment compilation failed"),
            "{error}"
        );
    }
}

#[test]
fn trigger_ddl_specializes_old_new_against_the_target_table() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION touch_trigger() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN \
             NEW.version := OLD.version + 1; RETURN NEW; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER touch BEFORE UPDATE OF version ON trigger_rows \
             FOR EACH ROW PRIORITY 10 EXECUTE FUNCTION touch_trigger();",
        )
        .unwrap();

    let catalog = executor.engine.pin_catalog().unwrap();
    let trigger = catalog
        .objects_of_kind(ObjectKind::Trigger)
        .find(|object| object.name().normalized().as_str() == "touch")
        .unwrap();
    let CatalogPayload::Trigger(payload) = trigger.payload() else {
        panic!("trigger object has wrong payload")
    };
    assert_eq!(payload.priority(), 10);
    assert_eq!(payload.update_column_ids().len(), 1);
}

#[test]
fn invalid_trigger_specialization_is_not_published() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION bad_trigger() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN NEW.missing := 1; RETURN NEW; END;",
        )
        .unwrap();

    let error = match executor.execute(
        "CREATE TRIGGER bad BEFORE INSERT ON trigger_rows \
         FOR EACH ROW EXECUTE FUNCTION bad_trigger();",
    ) {
        Ok(_) => panic!("invalid trigger attachment unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("trigger attachment compilation failed"));
    assert_eq!(
        executor
            .engine
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Trigger)
            .count(),
        0
    );
}

#[test]
fn dml_trigger_runtime_orders_hooks_and_mutates_new_atomically() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .unwrap();
    executor
        .execute("CREATE TABLE trigger_log (id INTEGER PRIMARY KEY AUTO_INCREMENT, marker INTEGER)")
        .unwrap();
    for definition in [
        "CREATE FUNCTION log_before_statement() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN INSERT INTO trigger_log (marker) VALUES (1); RETURN NULL; END;",
        "CREATE FUNCTION mutate_before_row() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN INSERT INTO trigger_log (marker) VALUES (2); \
         NEW.value := NEW.value + 1; RETURN NEW; END;",
        "CREATE FUNCTION log_after_row() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN INSERT INTO trigger_log (marker) VALUES (3); RETURN NULL; END;",
        "CREATE FUNCTION log_after_statement() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN INSERT INTO trigger_log (marker) VALUES (4); RETURN NULL; END;",
    ] {
        executor.execute(definition).unwrap();
    }
    for definition in [
        "CREATE TRIGGER insert_bs BEFORE INSERT ON trigger_rows FOR EACH STATEMENT \
         PRIORITY 10 EXECUTE FUNCTION log_before_statement();",
        "CREATE TRIGGER insert_br BEFORE INSERT ON trigger_rows FOR EACH ROW \
         PRIORITY 10 EXECUTE FUNCTION mutate_before_row();",
        "CREATE TRIGGER insert_ar AFTER INSERT ON trigger_rows FOR EACH ROW \
         PRIORITY 10 EXECUTE FUNCTION log_after_row();",
        "CREATE TRIGGER insert_as AFTER INSERT ON trigger_rows FOR EACH STATEMENT \
         PRIORITY 10 EXECUTE FUNCTION log_after_statement();",
    ] {
        executor.execute(definition).unwrap();
    }

    executor
        .execute("INSERT INTO trigger_rows VALUES (1, 10), (2, 20)")
        .unwrap();
    assert_eq!(
        integer_column(
            executor
                .execute("SELECT marker FROM trigger_log ORDER BY id")
                .unwrap()
        ),
        vec![1, 2, 3, 2, 3, 4]
    );
    assert_eq!(
        integer_column(
            executor
                .execute("SELECT value FROM trigger_rows ORDER BY id")
                .unwrap()
        ),
        vec![11, 21]
    );
}

#[test]
fn before_row_trigger_can_suppress_insert_update_and_delete() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION suppress_negative_insert() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN IF NEW.value < CAST(0 AS INTEGER) THEN RETURN NULL; END IF; RETURN NEW; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION suppress_large_update() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN IF NEW.value > CAST(50 AS INTEGER) THEN RETURN NULL; END IF; \
             NEW.value := OLD.value + 10; RETURN NEW; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION keep_first_delete() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN IF OLD.id = CAST(1 AS INTEGER) THEN RETURN NULL; END IF; RETURN OLD; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER suppress_insert BEFORE INSERT ON trigger_rows FOR EACH ROW \
             EXECUTE FUNCTION suppress_negative_insert();",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER suppress_update BEFORE UPDATE OF value ON trigger_rows FOR EACH ROW \
             WHEN (OLD.value < NEW.value) EXECUTE FUNCTION suppress_large_update();",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER suppress_delete BEFORE DELETE ON trigger_rows FOR EACH ROW \
             EXECUTE FUNCTION keep_first_delete();",
        )
        .unwrap();

    executor
        .execute("INSERT INTO trigger_rows VALUES (1, 1), (2, -1), (3, 3)")
        .unwrap();
    executor
        .execute("UPDATE trigger_rows SET value = 20 WHERE id = 1")
        .unwrap();
    executor
        .execute("UPDATE trigger_rows SET value = 100 WHERE id = 3")
        .unwrap();
    executor.execute("DELETE FROM trigger_rows").unwrap();

    assert_eq!(
        integer_column(
            executor
                .execute("SELECT id FROM trigger_rows ORDER BY id")
                .unwrap()
        ),
        vec![1]
    );
    assert_eq!(
        integer_column(executor.execute("SELECT value FROM trigger_rows").unwrap()),
        vec![11]
    );
}

#[test]
fn trigger_failure_rolls_back_outer_dml_and_trigger_effects() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .unwrap();
    executor
        .execute("CREATE TABLE trigger_log (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION fail_after_effect() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN INSERT INTO trigger_log VALUES (1); \
             RAISE invalid_state('trigger failed'); END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER fail_insert BEFORE INSERT ON trigger_rows FOR EACH ROW \
             EXECUTE FUNCTION fail_after_effect();",
        )
        .unwrap();

    assert!(executor
        .execute("INSERT INTO trigger_rows VALUES (1, 10)")
        .is_err());
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM trigger_rows")
                .unwrap()
        ),
        0
    );
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM trigger_log")
                .unwrap()
        ),
        0
    );
}

#[test]
fn recursive_trigger_cycle_is_typed_and_rolls_back() {
    let executor = executor();
    executor
        .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION recursive_insert() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
             SECURITY INVOKER AS BEGIN INSERT INTO trigger_rows VALUES (2, 2); RETURN NEW; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE TRIGGER recurse BEFORE INSERT ON trigger_rows FOR EACH ROW \
             EXECUTE FUNCTION recursive_insert();",
        )
        .unwrap();

    let error = match executor.execute("INSERT INTO trigger_rows VALUES (1, 1)") {
        Ok(_) => panic!("recursive trigger unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("PL_TRIGGER_CYCLE"), "{error}");
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM trigger_rows")
                .unwrap()
        ),
        0
    );
}

#[test]
fn durable_trigger_rebuilds_from_source_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("trigger-reopen");
    {
        let (executor, engine) = persistent_executor(&database);
        executor
            .execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION durable_touch() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
                 SECURITY INVOKER AS BEGIN NEW.value := NEW.value + 1; RETURN NEW; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TRIGGER durable_touch BEFORE INSERT ON trigger_rows FOR EACH ROW \
                 EXECUTE FUNCTION durable_touch();",
            )
            .unwrap();
        assert_eq!(executor.procedural_cache.len(), 0);
        executor
            .execute("INSERT INTO trigger_rows VALUES (1, 10)")
            .unwrap();
        assert_eq!(executor.procedural_cache.len(), 1);
        engine.close_engine().unwrap();
    }
    {
        let (executor, engine) = persistent_executor(&database);
        assert_eq!(executor.procedural_cache.len(), 0);
        executor
            .execute("INSERT INTO trigger_rows VALUES (2, 20)")
            .unwrap();
        assert_eq!(executor.procedural_cache.len(), 1);
        assert_eq!(
            integer_column(
                executor
                    .execute("SELECT value FROM trigger_rows ORDER BY id")
                    .unwrap()
            ),
            vec![11, 21]
        );
        engine.close_engine().unwrap();
    }
}

#[test]
fn published_procedure_rebuilds_from_source_and_dispatches_nested_out_arguments() {
    let executor = executor();
    executor
        .execute("CREATE TABLE procedure_effects (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE inner_value( \
                 IN input_value INTEGER NOT NULL, \
                 OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 INSERT INTO procedure_effects VALUES (:input_value, :input_value + 1); \
                 output_value := input_value + 1; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE outer_value( \
                 IN input_value INTEGER NOT NULL, \
                 OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL inner_value(input_value, output_value); \
             END;",
        )
        .unwrap();

    let outer = routine_named(&executor, ObjectKind::Procedure, "outer_value").unwrap();
    let mut stage = TestStage::default();
    let outcome = executor
        .execute_procedure(
            outer.id(),
            vec![RuntimeValue::scalar(Value::Integer(41))],
            &ExecutionContext::new(),
            principals(),
            &mut stage,
        )
        .unwrap();

    assert_eq!(
        outcome.execution().output_values,
        vec![RuntimeValue::scalar(Value::Integer(42))]
    );
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT value FROM procedure_effects WHERE id = 41")
                .unwrap()
        ),
        42
    );
    assert_eq!(stage.publish_count, 1);
}

#[test]
fn non_null_out_argument_must_be_assigned_on_every_return_path() {
    let executor = executor();
    let error = match executor.execute(
        "CREATE PROCEDURE missing_output(OUT output_value INTEGER NOT NULL) \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
    ) {
        Ok(_) => panic!("unassigned non-null OUT parameter unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));
    assert!(routine(&executor, ObjectKind::Procedure, "missing_output").is_none());
}

#[test]
fn durable_procedure_rebuilds_with_identical_semantics_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("procedural-reopen");
    let procedure_id;
    {
        let (executor, engine) = persistent_executor(&database);
        executor
            .execute(
                "CREATE PROCEDURE durable_increment( \
                     IN input_value INTEGER NOT NULL, \
                     OUT output_value INTEGER NOT NULL \
                 ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                     output_value := input_value + 1; \
                 END;",
            )
            .unwrap();
        procedure_id = routine_named(&executor, ObjectKind::Procedure, "durable_increment")
            .unwrap()
            .id();
        let mut stage = TestStage::default();
        let outcome = executor
            .execute_procedure(
                procedure_id,
                vec![RuntimeValue::scalar(Value::Integer(6))],
                &ExecutionContext::new(),
                principals(),
                &mut stage,
            )
            .unwrap();
        assert_eq!(
            outcome.execution().output_values,
            vec![RuntimeValue::scalar(Value::Integer(7))]
        );
        engine.close_engine().unwrap();
    }
    {
        let (executor, engine) = persistent_executor(&database);
        assert_eq!(
            routine_named(&executor, ObjectKind::Procedure, "durable_increment")
                .unwrap()
                .id(),
            procedure_id
        );
        let mut stage = TestStage::default();
        let outcome = executor
            .execute_procedure(
                procedure_id,
                vec![RuntimeValue::scalar(Value::Integer(40))],
                &ExecutionContext::new(),
                principals(),
                &mut stage,
            )
            .unwrap();
        assert_eq!(
            outcome.execution().output_values,
            vec![RuntimeValue::scalar(Value::Integer(41))]
        );
        engine.close_engine().unwrap();
    }
}

#[test]
fn compiled_program_cache_key_rejects_replaced_definition() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE cached_value(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 1; END;",
        )
        .unwrap();
    let first = routine(&executor, ObjectKind::Procedure, "cached_value").unwrap();
    let mut stage = TestStage::default();
    let first_outcome = executor
        .execute_procedure(
            first.id(),
            Vec::new(),
            &ExecutionContext::new(),
            principals(),
            &mut stage,
        )
        .unwrap();
    assert_eq!(
        first_outcome.execution().output_values,
        vec![RuntimeValue::scalar(Value::Integer(1))]
    );
    assert_eq!(executor.procedural_cache.len(), 1);

    executor
        .execute(
            "CREATE OR REPLACE PROCEDURE cached_value(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 2; END;",
        )
        .unwrap();
    let replacement = routine(&executor, ObjectKind::Procedure, "cached_value").unwrap();
    assert_eq!(replacement.id(), first.id());
    assert_eq!(replacement.definition_revision(), 2);
    let second_outcome = executor
        .execute_procedure(
            replacement.id(),
            Vec::new(),
            &ExecutionContext::new(),
            principals(),
            &mut stage,
        )
        .unwrap();
    assert_eq!(
        second_outcome.execution().output_values,
        vec![RuntimeValue::scalar(Value::Integer(2))]
    );
    assert_eq!(executor.procedural_cache.len(), 2);
}

#[test]
fn immutable_context_values_cross_definer_and_dynamic_sql_frames_without_spoofing() {
    let executor = executor();
    executor
        .execute(
            "CREATE TABLE context_log ( \
                 id INTEGER PRIMARY KEY AUTO_INCREMENT, kind TEXT NOT NULL, \
                 session_id UUID NOT NULL, effective_id UUID NOT NULL, \
                 transaction_id INTEGER, request_id INTEGER, statement_at TIMESTAMP NOT NULL, \
                 job_id UUID, job_attempt INTEGER, scheduled_at TIMESTAMP, idempotency_key TEXT \
             )",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE capture_context(input_kind TEXT NOT NULL) \
             LANGUAGE RADIX SECURITY DEFINER SEARCH PATH (public) AS BEGIN \
                 EXECUTE 'INSERT INTO context_log \
                     (kind, session_id, effective_id, transaction_id, request_id, statement_at, \
                      job_id, job_attempt, scheduled_at, idempotency_key) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)' \
                 USING input_kind, CURRENT_PRINCIPAL, CURRENT_EFFECTIVE_PRINCIPAL, \
                     CURRENT_TRANSACTION_ID, CURRENT_REQUEST_ID, CURRENT_STATEMENT_TIMESTAMP, \
                     CURRENT_JOB_ID, CURRENT_JOB_ATTEMPT, CURRENT_JOB_SCHEDULED_AT, \
                     CURRENT_IDEMPOTENCY_KEY; \
             END;",
        )
        .unwrap();
    executor.execute("CREATE PRINCIPAL context_alice").unwrap();
    executor
        .execute("GRANT CONNECT ON DATABASE test TO context_alice")
        .unwrap();
    executor
        .execute("GRANT USAGE ON SCHEMA public TO context_alice")
        .unwrap();
    executor
        .execute("GRANT EXECUTE ON PROCEDURE capture_context(TEXT) TO context_alice")
        .unwrap();
    let alice = principal_id(&executor, "context_alice");

    let mut direct = ExecutionContext::new().with_principal_id(alice);
    direct.set_named_param("CURRENT_PRINCIPAL", Value::uuid([9; 16]));
    direct.set_named_param("current_request_id", Value::Integer(9_999));
    direct.set_request_id(41).unwrap();
    direct.set_transaction_id(73);
    let direct_timestamp = direct
        .get_named_param("CURRENT_STATEMENT_TIMESTAMP")
        .cloned()
        .unwrap();
    executor
        .execute_with_context("CALL capture_context('direct')", &direct)
        .unwrap()
        .close()
        .unwrap();

    let job_id = ObjectId::from_user_bytes([7; 16]).unwrap();
    let scheduled_at = Utc::now();
    let mut job = direct.clone();
    job.set_request_id(42).unwrap();
    job.set_job_context("job-7-attempt-3", job_id, 3, scheduled_at);
    executor
        .execute_with_context("CALL capture_context('job')", &job)
        .unwrap()
        .close()
        .unwrap();

    let mut rows = executor
        .execute(
            "SELECT kind, session_id, effective_id, transaction_id, request_id, statement_at, \
                    job_id, job_attempt, scheduled_at, idempotency_key \
             FROM context_log ORDER BY kind",
        )
        .unwrap();
    assert!(rows.next());
    let direct_row = rows.take_row();
    assert_eq!(direct_row.get(0), Some(&Value::text("direct")));
    assert_eq!(direct_row.get(1), Some(&Value::uuid(alice.into_bytes())));
    assert_eq!(
        direct_row.get(2),
        Some(&Value::uuid(ObjectId::BOOTSTRAP_OWNER.into_bytes()))
    );
    assert_eq!(direct_row.get(3), Some(&Value::Integer(73)));
    assert_eq!(direct_row.get(4), Some(&Value::Integer(41)));
    assert_eq!(direct_row.get(5), Some(&direct_timestamp));
    for index in 6..=9 {
        assert!(
            direct_row.get(index).is_some_and(Value::is_null),
            "direct invocation context field {index} must be NULL"
        );
    }

    assert!(rows.next());
    let job_row = rows.take_row();
    assert_eq!(job_row.get(0), Some(&Value::text("job")));
    assert_eq!(job_row.get(1), Some(&Value::uuid(alice.into_bytes())));
    assert_eq!(
        job_row.get(2),
        Some(&Value::uuid(ObjectId::BOOTSTRAP_OWNER.into_bytes()))
    );
    assert_eq!(job_row.get(3), Some(&Value::Integer(73)));
    assert_eq!(job_row.get(4), Some(&Value::Integer(42)));
    assert_eq!(job_row.get(5), Some(&direct_timestamp));
    assert_eq!(job_row.get(6), Some(&Value::uuid(job_id.into_bytes())));
    assert_eq!(job_row.get(7), Some(&Value::Integer(3)));
    assert_eq!(job_row.get(8), Some(&Value::Timestamp(scheduled_at)));
    assert_eq!(job_row.get(9), Some(&Value::text("job-7-attempt-3")));
    assert!(!rows.next());
    assert!(rows.last_error().is_none());
    rows.close().unwrap();
}

fn principals() -> PrincipalContext {
    PrincipalContext {
        session_principal: ObjectId::BOOTSTRAP_OWNER,
        invoker_principal: ObjectId::BOOTSTRAP_OWNER,
        effective_principal: ObjectId::BOOTSTRAP_OWNER,
    }
}

fn integer(nullable: bool) -> RuntimeType {
    RuntimeType::scalar(
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        nullable,
    )
}

fn text(nullable: bool) -> RuntimeType {
    RuntimeType::scalar(CatalogDataType::scalar(DataType::Text).unwrap(), nullable)
}

fn statement(sql: &str) -> Box<Statement> {
    let mut statements = parse_sql(sql).unwrap();
    assert_eq!(statements.len(), 1);
    Box::new(statements.remove(0))
}

fn block(instructions: Vec<Instruction>, terminator: Terminator) -> BasicBlock {
    BasicBlock::new(
        instructions
            .into_iter()
            .map(SpannedInstruction::unspanned)
            .collect(),
        SpannedTerminator::unspanned(terminator),
    )
}

fn program(
    marker: u8,
    slots: Vec<SlotDefinition>,
    result_type: Option<RuntimeType>,
    result_columns: Vec<RuntimeType>,
    blocks: Vec<BasicBlock>,
) -> radixdb_procedural::VerifiedProgram {
    verify(
        Program::new(
            ProgramIdentity::new(
                ObjectId::from_user_bytes([marker; 16]).unwrap(),
                1,
                "bridge",
            ),
            slots,
            Vec::new(),
            result_type,
            blocks,
            BlockId(0),
        )
        .with_result_columns(result_columns),
    )
    .unwrap()
}

fn execute(
    executor: &Executor,
    program: &radixdb_procedural::VerifiedProgram,
    stage: &mut TestStage,
) -> ProceduralResult<radixdb_procedural::ExecutionOutcome> {
    executor
        .execute_procedural_program(
            program,
            Vec::new(),
            &ExecutionContext::new(),
            principals(),
            ResourcePolicy::default_call(),
            stage,
        )
        .map(|outcome| outcome.execution().clone())
}

fn scalar_integer(mut result: Box<dyn QueryResult>) -> i64 {
    assert!(result.next());
    let value = match result.take_row().get(0) {
        Some(Value::Integer(value)) => *value,
        value => panic!("expected INTEGER, got {value:?}"),
    };
    assert!(!result.next());
    assert!(result.last_error().is_none());
    result.close().unwrap();
    value
}

fn integer_column(mut result: Box<dyn QueryResult>) -> Vec<i64> {
    let mut values = Vec::new();
    while result.next() {
        match result.take_row().get(0) {
            Some(Value::Integer(value)) => values.push(*value),
            value => panic!("expected INTEGER, got {value:?}"),
        }
    }
    assert!(result.last_error().is_none());
    result.close().unwrap();
    values
}

#[path = "tests/transaction_runtime.rs"]
mod transaction_runtime;

#[path = "tests/application_runtime.rs"]
mod application_runtime;
