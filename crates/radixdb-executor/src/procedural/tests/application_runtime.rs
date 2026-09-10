use super::*;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use radixdb_catalog::ObjectKind;
use radixdb_storage::config::PersistenceConfig;
use radixdb_storage::test_failpoints;

const APPLICATION_CRASH_CHILD: &str = "RADIXDB_APPLICATION_CRASH_CHILD";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn durable_executor(path: &Path) -> (Executor, Arc<MVCCEngine>) {
    let mut config = Config::with_path(path.to_string_lossy().to_string());
    config.persistence = PersistenceConfig::durable();
    config.persistence.checkpoint_interval = 0;
    config.persistence.checkpoint_on_close = false;
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

fn principal(executor: &Executor, name: &str) -> ObjectId {
    executor
        .engine()
        .pin_catalog()
        .unwrap()
        .objects_of_kind(ObjectKind::Principal)
        .find(|object| object.name().normalized().as_str() == name)
        .unwrap()
        .id()
}

fn call_context(principal_id: ObjectId, id: i64) -> ExecutionContext {
    let mut context = ExecutionContext::new().with_principal_id(principal_id);
    context.set_named_param("input_id", Value::Integer(id));
    context.set_named_param("fingerprint", Value::bytes(vec![7; 32]));
    context.set_named_param(
        "metadata",
        Value::json(format!(r#"{{"operation":"crash-oracle","id":{id}}}"#)),
    );
    context.set_named_param("message_key", Value::text(format!("crash-oracle-{id}")));
    context.set_named_param("payload", Value::json(format!(r#"{{"id":{id}}}"#)));
    context
}

fn call_publish(executor: &Executor, principal_id: ObjectId, id: i64) -> radixdb_core::Result<()> {
    let mut result = executor.execute_with_context(
        "CALL app.crash_publish(\
            :input_id, :fingerprint, :metadata, :message_key, :payload\
        )",
        &call_context(principal_id, id),
    )?;
    while result.next() {}
    if let Some(error) = result.last_error() {
        return Err(error);
    }
    result.close()
}

fn create_crash_fixture(executor: &Executor) {
    executor.install_application_relations().unwrap();
    executor.execute("CREATE SCHEMA app").unwrap();
    executor
        .execute("CREATE TABLE app.business_record (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION app.crash_plus(input_value INTEGER NOT NULL) \
             RETURNS INTEGER LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN \
                 RETURN input_value + 1; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE app.crash_publish( \
                 input_id INTEGER NOT NULL, fingerprint BYTES NOT NULL, \
                 metadata JSON NOT NULL, message_key TEXT NOT NULL, \
                 payload JSON NOT NULL \
             ) LANGUAGE RADIX SECURITY DEFINER SEARCH PATH (app, public) AS BEGIN \
                 INSERT INTO app.business_record VALUES (:input_id); \
                 CALL system.append_audit(fingerprint, metadata); \
                 CALL system.append_outbox(message_key, 1, payload); \
             END;",
        )
        .unwrap();
    executor.execute("PRAGMA CHECKPOINT").unwrap();

    executor.execute("CREATE PRINCIPAL crash_alice").unwrap();
    executor.execute("CREATE ROLE crash_runner").unwrap();
    executor
        .execute("GRANT CONNECT ON DATABASE test TO crash_alice")
        .unwrap();
    executor
        .execute("GRANT USAGE ON SCHEMA app TO crash_alice")
        .unwrap();
    executor
        .execute("GRANT EXECUTE ON PROCEDURE app.crash_publish(INTEGER, BYTES, JSON, TEXT, JSON) TO crash_runner")
        .unwrap();
    executor
        .execute("GRANT EXECUTE ON FUNCTION app.crash_plus(INTEGER) TO crash_runner")
        .unwrap();
    executor
        .execute("GRANT crash_runner TO crash_alice")
        .unwrap();
}

fn application_counts(executor: &Executor) -> [i64; 3] {
    ["app.business_record", "audit.event", "outbox.message"].map(|relation| {
        scalar_integer(
            executor
                .execute(&format!("SELECT COUNT(*) FROM {relation}"))
                .unwrap(),
        )
    })
}

#[test]
fn procedure_atomically_writes_business_audit_and_outbox_relations() {
    let executor = executor();
    executor.install_application_relations().unwrap();
    executor
        .execute("CREATE TABLE business_header (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute("CREATE TABLE business_detail (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE publish_business( \
                 input_id INTEGER NOT NULL, should_fail BOOLEAN NOT NULL, \
                 fingerprint BYTES NOT NULL, metadata JSON NOT NULL, \
                 message_key TEXT NOT NULL, schema_version INTEGER NOT NULL, \
                 payload JSON NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 INSERT INTO business_header VALUES (:input_id); \
                 INSERT INTO business_detail VALUES (:input_id); \
                 CALL system.append_audit(fingerprint, metadata); \
                 CALL system.append_outbox(message_key, schema_version, payload); \
                 IF should_fail THEN \
                     INSERT INTO business_header VALUES (:input_id); \
                 END IF; \
             END;",
        )
        .unwrap();
    let procedure = routine_named(&executor, ObjectKind::Procedure, "publish_business").unwrap();
    let invoke = |id, should_fail, key: &str| {
        let mut stage = TestStage::default();
        executor.execute_procedure(
            procedure.id(),
            vec![
                RuntimeValue::scalar(Value::Integer(id)),
                RuntimeValue::scalar(Value::Boolean(should_fail)),
                RuntimeValue::scalar(Value::bytes(vec![5; 32])),
                RuntimeValue::scalar(Value::json(r#"{"operation":"publish"}"#)),
                RuntimeValue::scalar(Value::text(key)),
                RuntimeValue::scalar(Value::Integer(1)),
                RuntimeValue::scalar(Value::json(format!(r#"{{"id":{id}}}"#))),
            ],
            &ExecutionContext::new(),
            principals(),
            &mut stage,
        )
    };

    invoke(1, false, "business-1").unwrap();
    for relation in [
        "business_header",
        "business_detail",
        "audit.event",
        "outbox.message",
    ] {
        assert_eq!(
            scalar_integer(
                executor
                    .execute(&format!("SELECT COUNT(*) FROM {relation}"))
                    .unwrap()
            ),
            1,
            "unexpected committed row count for {relation}"
        );
    }

    assert!(invoke(2, true, "business-2").is_err());
    for relation in [
        "business_header",
        "business_detail",
        "audit.event",
        "outbox.message",
    ] {
        assert_eq!(
            scalar_integer(
                executor
                    .execute(&format!("SELECT COUNT(*) FROM {relation}"))
                    .unwrap()
            ),
            1,
            "failed call leaked a row into {relation}"
        );
    }
}

#[test]
fn stable_function_cannot_admit_system_append_primitives() {
    let executor = executor();
    let error = match executor.execute(
        "CREATE FUNCTION forbidden_append(fingerprint BYTES NOT NULL, metadata JSON NOT NULL) \
             RETURNS INTEGER LANGUAGE RADIX STABLE SECURITY INVOKER AS BEGIN \
                 CALL system.append_audit(fingerprint, metadata); \
                 RETURN 1; \
             END;",
    ) {
        Ok(_) => panic!("STABLE system append unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("cannot append audit or outbox records"));
}

#[test]
fn publication_failpoints_preserve_catalog_acl_and_application_atomicity() {
    let _guard = test_failpoints::FailpointGuard::new();
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("application-failpoints");
    let (executor, engine) = durable_executor(&database_path);
    create_crash_fixture(&executor);
    let alice = principal(&executor, "crash_alice");
    call_publish(&executor, alice, 1).unwrap();
    assert_eq!(application_counts(&executor), [1, 1, 1]);

    test_failpoints::WAL_WRITE_FAIL.store(true, std::sync::atomic::Ordering::Release);
    assert!(executor
        .execute(
            "CREATE FUNCTION app.must_not_publish() RETURNS INTEGER \
             LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN 1; END;",
        )
        .is_err());
    test_failpoints::WAL_WRITE_FAIL.store(false, std::sync::atomic::Ordering::Release);
    assert!(routine_named(&executor, ObjectKind::Function, "must_not_publish").is_none());

    test_failpoints::WAL_WRITE_FAIL.store(true, std::sync::atomic::Ordering::Release);
    assert!(executor
        .execute("REVOKE EXECUTE ON PROCEDURE app.crash_publish(INTEGER, BYTES, JSON, TEXT, JSON) FROM crash_runner")
        .is_err());
    test_failpoints::WAL_WRITE_FAIL.store(false, std::sync::atomic::Ordering::Release);
    call_publish(&executor, alice, 2).unwrap();
    assert_eq!(application_counts(&executor), [2, 2, 2]);

    for (offset, failpoint) in [
        &test_failpoints::WAL_WRITE_FAIL,
        &test_failpoints::WAL_SYNC_FAIL,
        &test_failpoints::FILESYSTEM_FULL_FAIL,
    ]
    .into_iter()
    .enumerate()
    {
        failpoint.store(true, std::sync::atomic::Ordering::Release);
        assert!(call_publish(&executor, alice, 10 + offset as i64).is_err());
        failpoint.store(false, std::sync::atomic::Ordering::Release);
        let counts = application_counts(&executor);
        assert_eq!(counts[0], counts[1]);
        assert_eq!(counts[1], counts[2]);
    }

    test_failpoints::CHECKPOINT_WRITE_FAIL.store(true, std::sync::atomic::Ordering::Release);
    assert!(executor.execute("PRAGMA CHECKPOINT").is_err());
    test_failpoints::CHECKPOINT_WRITE_FAIL.store(false, std::sync::atomic::Ordering::Release);
    let before_reopen = application_counts(&executor);
    engine.close_engine().unwrap();

    let (reopened, reopened_engine) = durable_executor(&database_path);
    assert_eq!(application_counts(&reopened), before_reopen);
    let reopened_alice = principal(&reopened, "crash_alice");
    call_publish(&reopened, reopened_alice, 20).unwrap();
    let after_reopen = application_counts(&reopened);
    assert_eq!(after_reopen[0], before_reopen[0] + 1);
    assert_eq!(after_reopen[0], after_reopen[1]);
    assert_eq!(after_reopen[1], after_reopen[2]);
    reopened_engine.close_engine().unwrap();
}

#[test]
fn process_crash_reopens_definitions_acl_audit_and_outbox() {
    if let Ok(database_path) = std::env::var(APPLICATION_CRASH_CHILD) {
        let database_path = PathBuf::from(database_path);
        let ready_path = database_path.with_extension("ready");
        let (executor, _engine) = durable_executor(&database_path);
        create_crash_fixture(&executor);
        let alice = principal(&executor, "crash_alice");
        call_publish(&executor, alice, 1).unwrap();
        executor
            .execute("CREATE PRINCIPAL crash_lifecycle")
            .unwrap();
        executor
            .execute("ALTER PRINCIPAL crash_lifecycle RENAME TO crash_lifecycle_live")
            .unwrap();
        executor
            .execute("ALTER PRINCIPAL crash_lifecycle_live DISABLE")
            .unwrap();
        executor.execute("CREATE ROLE crash_role").unwrap();
        executor.execute("ALTER ROLE crash_role DISABLE").unwrap();
        executor
            .execute("ALTER ROLE crash_role RENAME TO crash_role_live")
            .unwrap();
        executor
            .execute("ALTER ROLE crash_role_live ENABLE")
            .unwrap();
        executor.execute("CREATE PRINCIPAL crash_dropped").unwrap();
        executor.execute("DROP PRINCIPAL crash_dropped").unwrap();
        executor.execute("CREATE ROLE crash_role_dropped").unwrap();
        executor.execute("DROP ROLE crash_role_dropped").unwrap();
        fs::write(&ready_path, b"ready").unwrap();
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("application-crash");
    let ready_path = database_path.with_extension("ready");
    let current_exe = std::env::current_exe().unwrap();
    let child = Command::new(current_exe)
        .arg("--exact")
        .arg("procedural::tests::application_runtime::process_crash_reopens_definitions_acl_audit_and_outbox")
        .arg("--nocapture")
        .env(APPLICATION_CRASH_CHILD, &database_path)
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);

    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready_path.exists() && Instant::now() < deadline {
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("application crash child exited before kill barrier: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists(),
        "child did not reach durable kill barrier"
    );
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert!(
        !status.success(),
        "child must terminate without engine close"
    );

    let (executor, engine) = durable_executor(&database_path);
    let alice = principal(&executor, "crash_alice");
    let lifecycle = principal(&executor, "crash_lifecycle_live");
    let catalog = executor.engine().pin_catalog().unwrap();
    assert!(matches!(
        catalog.object(lifecycle).unwrap().payload(),
        radixdb_catalog::CatalogPayload::Principal(payload) if !payload.login_enabled()
    ));
    let role = catalog
        .objects_of_kind(ObjectKind::Role)
        .find(|role| role.name().normalized().as_str() == "crash_role_live")
        .unwrap();
    assert!(matches!(
        role.payload(),
        radixdb_catalog::CatalogPayload::Role(payload) if payload.enabled()
    ));
    assert!(catalog
        .objects_of_kind(ObjectKind::Principal)
        .all(|principal| principal.name().normalized().as_str() != "crash_dropped"));
    assert!(catalog.objects_of_kind(ObjectKind::Role).all(|role| role
        .name()
        .normalized()
        .as_str()
        != "crash_role_dropped"));
    drop(catalog);
    assert!(routine_named(&executor, ObjectKind::Function, "crash_plus").is_some());
    assert!(routine_named(&executor, ObjectKind::Procedure, "crash_publish").is_some());
    for relation in ["app.business_record", "audit.event", "outbox.message"] {
        assert_eq!(
            scalar_integer(
                executor
                    .execute(&format!("SELECT COUNT(*) FROM {relation}"))
                    .unwrap()
            ),
            1,
            "recovery lost or duplicated rows in {relation}"
        );
    }

    call_publish(&executor, alice, 2).unwrap();
    for relation in ["app.business_record", "audit.event", "outbox.message"] {
        assert_eq!(
            scalar_integer(
                executor
                    .execute(&format!("SELECT COUNT(*) FROM {relation}"))
                    .unwrap()
            ),
            2,
            "reopened execution did not publish exactly once to {relation}"
        );
    }

    executor
        .execute("REVOKE EXECUTE ON PROCEDURE app.crash_publish(INTEGER, BYTES, JSON, TEXT, JSON) FROM crash_runner")
        .unwrap();
    assert!(call_publish(&executor, alice, 3).is_err());
    engine.close_engine().unwrap();
}
