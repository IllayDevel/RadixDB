use super::*;

#[test]
fn sql_identifier_is_typed_canonical_and_safe_across_nested_calls() {
    let executor = executor();
    executor
        .execute("CREATE TABLE identifier_victim (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute("INSERT INTO identifier_victim VALUES (1)")
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE identifier_inner( \
                 IN alias_name TEXT NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 EXECUTE 'SELECT 17 AS ' || SQL_IDENTIFIER(alias_name) \
                     INTO STRICT output_value; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE identifier_outer( \
                 IN alias_name TEXT NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL identifier_inner(alias_name, output_value); \
             END;",
        )
        .unwrap();
    let outer = routine_named(&executor, ObjectKind::Procedure, "identifier_outer").unwrap();
    for identifier in [
        "ordinary",
        "quoted\"name",
        "таблица_世界",
        "x; DELETE FROM identifier_victim; --",
        "x/*comment*/y",
        "x.y",
        &"m".repeat(radixdb_catalog::MAX_NORMALIZED_NAME_BYTES),
    ] {
        let mut stage = TestStage::default();
        let outcome = executor
            .execute_procedure(
                outer.id(),
                vec![RuntimeValue::scalar(Value::Text(identifier.into()))],
                &ExecutionContext::new(),
                principals(),
                &mut stage,
            )
            .unwrap_or_else(|error| panic!("identifier {identifier:?}: {error}"));
        assert_eq!(
            outcome.execution().output_values,
            vec![RuntimeValue::scalar(Value::Integer(17))]
        );
    }
    assert_eq!(
        integer_column(
            executor
                .execute("SELECT id FROM identifier_victim")
                .unwrap()
        ),
        vec![1],
        "quoted identifier input changed statement structure"
    );

    for invalid in [
        "".to_string(),
        "x".repeat(radixdb_catalog::MAX_NORMALIZED_NAME_BYTES + 1),
    ] {
        let mut stage = TestStage::default();
        let error = executor
            .execute_procedure(
                outer.id(),
                vec![RuntimeValue::scalar(Value::Text(invalid.into()))],
                &ExecutionContext::new(),
                principals(),
                &mut stage,
            )
            .unwrap_err();
        assert!(error.to_string().contains("SQL_IDENTIFIER"), "{error}");
    }

    let error = match executor.execute(
        "CREATE FUNCTION identifier_as_value(input_value TEXT NOT NULL) RETURNS TEXT \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN \
             RETURN SQL_IDENTIFIER(input_value); END;",
    ) {
        Ok(_) => panic!("typed SQL identifier unexpectedly became a SQL value"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("type"), "{error}");
}

#[test]
fn job_ddl_freezes_arguments_and_attempt_executes_as_catalog_principal() {
    let executor = executor();
    executor
        .execute("CREATE TABLE job_effects (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE record_job( \
                 IN input_id INTEGER NOT NULL, \
                 IN input_value INTEGER NOT NULL DEFAULT 41 \
             ) LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN \
                 INSERT INTO job_effects VALUES (:input_id, :input_value); \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE JOB every_five SCHEDULE EVERY INTERVAL '5 minute' \
             RUN AS radix_system CALL record_job(7) ENABLE;",
        )
        .unwrap();

    let job = named_object(&executor, ObjectKind::Job, "every_five").unwrap();
    let CatalogPayload::Job(payload) = job.payload() else {
        panic!("job kind has wrong payload")
    };
    assert_eq!(payload.schedule(), JobSchedule::EveryNs(300_000_000_000));
    assert_eq!(payload.arguments().len(), 2);
    assert_eq!(payload.principal_id(), ObjectId::BOOTSTRAP_OWNER);

    let outcome = executor
        .execute_job_attempt(
            job.id(),
            JobAttemptMetadata {
                scheduled_at_unix_ns: 1_725_000_000_000_000_000,
                attempt: 2,
                idempotency_key: "every_five/1725000000000000000".to_string(),
            },
            &ExecutionContext::new(),
        )
        .unwrap();
    assert_eq!(outcome.job_id, job.id());
    assert_eq!(outcome.attempt, 2);
    assert_eq!(
        integer_column(
            executor
                .execute("SELECT value FROM job_effects WHERE id = 7")
                .unwrap()
        ),
        vec![41]
    );
}

#[test]
fn job_attempt_rejects_disabled_job_without_leaking_a_transaction() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE idle_job() LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN PERFORM 1; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE JOB disabled_job SCHEDULE AT TIMESTAMP '2026-09-08T00:00:00Z' \
             RUN AS radix_system CALL idle_job() DISABLE;",
        )
        .unwrap();
    let job = named_object(&executor, ObjectKind::Job, "disabled_job").unwrap();
    let failure = executor
        .execute_job_attempt(
            job.id(),
            JobAttemptMetadata {
                scheduled_at_unix_ns: 1,
                attempt: 1,
                idempotency_key: "disabled/1".to_string(),
            },
            &ExecutionContext::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind(), DiagnosticKind::JobAttemptFailed);
    assert_eq!(
        failure.cause(),
        Some(DiagnosticKind::RuntimeInvalidArgument)
    );
    assert!(failure.retryable());
    assert!(failure
        .details()
        .iter()
        .any(|detail| detail.key == "scheduler_retryable" && detail.value == "false"));
    executor.execute("BEGIN").unwrap();
    executor.execute("ROLLBACK").unwrap();
}

#[test]
fn job_attempt_wraps_procedure_failure_with_bounded_observable_cause() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE failing_job() LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN RAISE invalid_argument('expected job failure'); END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE JOB failing_job_schedule SCHEDULE EVERY INTERVAL '1 minute' \
             RUN AS radix_system CALL failing_job() ENABLE;",
        )
        .unwrap();
    let job = named_object(&executor, ObjectKind::Job, "failing_job_schedule").unwrap();
    let failure = executor
        .execute_job_attempt(
            job.id(),
            JobAttemptMetadata {
                scheduled_at_unix_ns: 42,
                attempt: 3,
                idempotency_key: "must-not-leak-through-diagnostic".to_string(),
            },
            &ExecutionContext::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind(), DiagnosticKind::JobAttemptFailed);
    assert_eq!(failure.category().as_str(), "job");
    assert_eq!(
        failure.cause(),
        Some(DiagnosticKind::RuntimeInvalidArgument)
    );
    assert!(
        failure
            .details()
            .iter()
            .any(|detail| { detail.key == "cause_message" && detail.value == "procedural RAISE" }),
        "{:?}",
        failure.details()
    );
    assert!(!failure
        .details()
        .iter()
        .any(|detail| detail.value.contains("must-not-leak")));
}

#[test]
fn job_definition_and_attempt_survive_persistent_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let job_id = {
        let (executor, engine) = persistent_executor(directory.path());
        executor
            .execute("CREATE TABLE reopened_job_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE PROCEDURE reopened_job(IN input_id INTEGER NOT NULL) \
                 LANGUAGE RADIX SECURITY INVOKER AS \
                 BEGIN INSERT INTO reopened_job_effects VALUES (:input_id); END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE JOB durable_job SCHEDULE EVERY INTERVAL '1 hour' \
                 RUN AS radix_system CALL reopened_job(9) ENABLE;",
            )
            .unwrap();
        let id = named_object(&executor, ObjectKind::Job, "durable_job")
            .unwrap()
            .id();
        engine.close_engine().unwrap();
        id
    };

    let (executor, engine) = persistent_executor(directory.path());
    executor
        .execute_job_attempt(
            job_id,
            JobAttemptMetadata {
                scheduled_at_unix_ns: 1_725_000_000_000_000_000,
                attempt: 1,
                idempotency_key: "durable/1".to_string(),
            },
            &ExecutionContext::new(),
        )
        .unwrap();
    assert_eq!(
        integer_column(
            executor
                .execute("SELECT id FROM reopened_job_effects")
                .unwrap()
        ),
        vec![9]
    );
    engine.close_engine().unwrap();
}

#[test]
fn program_object_lifecycle_is_exact_atomic_authorized_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("program-lifecycle");
    {
        let (executor, engine) = persistent_executor(&database);
        executor
            .execute("CREATE TABLE lifecycle_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION overloaded(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
                 LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN value; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION overloaded(value TEXT NOT NULL) RETURNS TEXT NOT NULL \
                 LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN value; END;",
            )
            .unwrap();
        executor
            .execute("DROP FUNCTION overloaded(INTEGER)")
            .unwrap();
        let generation = executor.engine.pin_catalog().unwrap();
        assert!(generation
            .find_routine(
                ObjectId::BOOTSTRAP_NAMESPACE,
                ObjectKind::Function,
                "overloaded",
                &[CatalogDataType::scalar(DataType::Integer).unwrap()],
            )
            .unwrap()
            .is_none());
        assert!(generation
            .find_routine(
                ObjectId::BOOTSTRAP_NAMESPACE,
                ObjectKind::Function,
                "overloaded",
                &[CatalogDataType::scalar(DataType::Text).unwrap()],
            )
            .unwrap()
            .is_some());
        drop(generation);

        executor
            .execute(
                "CREATE FUNCTION lifecycle_trigger() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
                 SECURITY INVOKER AS BEGIN RETURN NEW; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE TRIGGER lifecycle_insert BEFORE INSERT ON lifecycle_rows FOR EACH ROW \
                 EXECUTE FUNCTION lifecycle_trigger();",
            )
            .unwrap();
        let restrict = match executor.execute("DROP FUNCTION lifecycle_trigger() RESTRICT") {
            Ok(_) => panic!("RESTRICT unexpectedly removed a referenced trigger function"),
            Err(error) => error.to_string(),
        };
        assert!(restrict.contains("depends on it"), "{restrict}");
        executor
            .execute("DROP FUNCTION lifecycle_trigger() CASCADE")
            .unwrap();
        assert!(named_object(&executor, ObjectKind::Function, "lifecycle_trigger").is_none());
        assert!(named_object(&executor, ObjectKind::Trigger, "lifecycle_insert").is_none());

        executor
            .execute(
                "CREATE PROCEDURE lifecycle_job_proc() LANGUAGE RADIX SECURITY INVOKER AS \
                 BEGIN PERFORM 1; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE JOB lifecycle_job SCHEDULE EVERY INTERVAL '1 minute' \
                 RUN AS radix_system CALL lifecycle_job_proc() DISABLE;",
            )
            .unwrap();
        executor.execute("ALTER JOB lifecycle_job ENABLE").unwrap();
        let job = named_object(&executor, ObjectKind::Job, "lifecycle_job").unwrap();
        let CatalogPayload::Job(job_payload) = job.payload() else {
            panic!("job object has non-job payload")
        };
        assert!(job_payload.enabled());
        assert_eq!(job_payload.definition_version(), 2);
        assert!(executor
            .execute("DROP PROCEDURE lifecycle_job_proc() RESTRICT")
            .is_err());

        executor.execute("BEGIN").unwrap();
        executor.execute("DROP JOB lifecycle_job").unwrap();
        executor.execute("ROLLBACK").unwrap();
        assert!(named_object(&executor, ObjectKind::Job, "lifecycle_job").is_some());

        executor
            .execute("CREATE PRINCIPAL lifecycle_alice")
            .unwrap();
        executor.execute("CREATE PRINCIPAL lifecycle_bob").unwrap();
        for name in ["lifecycle_alice", "lifecycle_bob"] {
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT USAGE ON SCHEMA public TO {name}"))
                .unwrap();
        }
        executor
            .execute("GRANT CREATE ON SCHEMA public TO lifecycle_alice")
            .unwrap();
        let alice = principal_id(&executor, "lifecycle_alice");
        let bob = principal_id(&executor, "lifecycle_bob");
        executor
            .execute_with_context(
                "CREATE FUNCTION alice_owned() RETURNS INTEGER NOT NULL LANGUAGE RADIX \
                 IMMUTABLE SECURITY INVOKER AS BEGIN RETURN 1; END;",
                &ExecutionContext::new().with_principal_id(alice),
            )
            .unwrap();
        assert!(executor
            .execute_with_context(
                "DROP FUNCTION alice_owned()",
                &ExecutionContext::new().with_principal_id(bob),
            )
            .is_err());
        executor
            .execute_with_context(
                "DROP FUNCTION alice_owned()",
                &ExecutionContext::new().with_principal_id(alice),
            )
            .unwrap();

        executor
            .execute("DROP PROCEDURE lifecycle_job_proc() CASCADE")
            .unwrap();
        assert!(named_object(&executor, ObjectKind::Job, "lifecycle_job").is_none());
        executor
            .execute("DROP JOB IF EXISTS lifecycle_job")
            .unwrap();
        executor.execute("DROP FUNCTION overloaded(TEXT)").unwrap();
        engine.close_engine().unwrap();
    }

    let (executor, engine) = persistent_executor(&database);
    assert!(named_object(&executor, ObjectKind::Job, "lifecycle_job").is_none());
    assert!(named_object(&executor, ObjectKind::Function, "overloaded").is_none());
    assert!(named_object(&executor, ObjectKind::Function, "alice_owned").is_none());
    engine.close_engine().unwrap();
}

#[test]
fn routine_ddl_is_compiled_before_atomic_catalog_publication() {
    let executor = executor();
    executor
        .execute(
            "CREATE FUNCTION answer() RETURNS INTEGER NOT NULL \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             BEGIN RETURN 42; END;",
        )
        .unwrap();

    let first = routine(&executor, ObjectKind::Function, "answer").unwrap();
    let first_id = first.id();
    assert_eq!(first.definition_revision(), 1);
    let CatalogPayload::Function(payload) = first.payload() else {
        panic!("function kind has wrong payload")
    };
    assert!(payload
        .procedural_definition()
        .unwrap()
        .source()
        .as_str()
        .contains("RETURN 42"));

    executor
        .execute(
            "CREATE OR REPLACE FUNCTION answer() RETURNS INTEGER NOT NULL \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             BEGIN RETURN 43; END;",
        )
        .unwrap();
    let replacement = routine(&executor, ObjectKind::Function, "answer").unwrap();
    assert_eq!(replacement.id(), first_id);
    assert_eq!(replacement.definition_revision(), 2);

    let error = match executor.execute(
        "CREATE OR REPLACE FUNCTION answer() RETURNS INTEGER NOT NULL \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             BEGIN RETURN 'wrong'; END;",
    ) {
        Ok(_) => panic!("invalid replacement unexpectedly compiled"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));
    let after_failure = routine(&executor, ObjectKind::Function, "answer").unwrap();
    assert_eq!(after_failure.id(), first_id);
    assert_eq!(after_failure.definition_revision(), 2);
}

#[test]
fn routine_ddl_obeys_explicit_transaction_visibility() {
    let executor = executor();
    executor.execute("BEGIN").unwrap();
    executor
        .execute(
            "CREATE PROCEDURE staged() LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN RETURN; END;",
        )
        .unwrap();
    assert!(routine(&executor, ObjectKind::Procedure, "staged").is_none());
    executor.execute("ROLLBACK").unwrap();
    assert!(routine(&executor, ObjectKind::Procedure, "staged").is_none());

    executor.execute("BEGIN").unwrap();
    executor
        .execute(
            "CREATE PROCEDURE staged() LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN RETURN; END;",
        )
        .unwrap();
    executor.execute("COMMIT").unwrap();
    assert!(routine(&executor, ObjectKind::Procedure, "staged").is_some());
}

#[test]
fn immutable_function_dml_is_rejected_without_catalog_side_effect() {
    let executor = executor();
    executor
        .execute("CREATE TABLE protected_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    let error = match executor.execute(
        "CREATE FUNCTION forbidden_write() RETURNS INTEGER NOT NULL \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN \
             INSERT INTO protected_rows VALUES (1); RETURN 1; END;",
    ) {
        Ok(_) => panic!("immutable DML function unexpectedly compiled"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("IMMUTABLE and STABLE functions cannot execute DML"));
    assert!(routine(&executor, ObjectKind::Function, "forbidden_write").is_none());
}

#[test]
fn immutable_function_relation_read_is_rejected_without_catalog_side_effect() {
    let executor = executor();
    executor
        .execute("CREATE TABLE protected_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    let error = match executor.execute(
        "CREATE FUNCTION forbidden_read() RETURNS INTEGER NOT NULL \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             DECLARE result INTEGER := 0; BEGIN \
             SELECT COUNT(*) INTO result FROM protected_rows; RETURN result; END;",
    ) {
        Ok(_) => panic!("immutable table read unexpectedly compiled"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("IMMUTABLE functions cannot read tables or views"));
    assert!(routine(&executor, ObjectKind::Function, "forbidden_read").is_none());
}

#[test]
fn function_builtin_volatility_is_checked_at_admission() {
    let executor = executor();
    let error = match executor.execute(
        "CREATE FUNCTION forbidden_random() RETURNS FLOAT \
             LANGUAGE RADIX STABLE SECURITY INVOKER AS BEGIN \
             RETURN RANDOM(); END;",
    ) {
        Ok(_) => panic!("STABLE random call unexpectedly compiled"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("Stable function cannot invoke Volatile built-in RANDOM"));

    executor
        .execute(
            "CREATE FUNCTION admitted_time() RETURNS TIMESTAMP \
             LANGUAGE RADIX STABLE SECURITY INVOKER AS BEGIN \
             RETURN CURRENT_TIMESTAMP; END;",
        )
        .unwrap();
    assert!(routine(&executor, ObjectKind::Function, "admitted_time").is_some());
}
