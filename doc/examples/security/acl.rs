use std::sync::Arc;

use radixdb_catalog::{ObjectId, ObjectKind};
use radixdb_core::{Error, Value};
use radixdb_executor::{procedural::JobAttemptMetadata, ExecutionContext, Executor};
use radixdb_storage::mvcc::engine::MVCCEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = MVCCEngine::in_memory();
    engine.open_engine()?;
    let executor = Executor::new(Arc::new(engine));

    execute(
        &executor,
        "CREATE TABLE acl_documents (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        secret TEXT NOT NULL
    )",
    )?;
    execute(
        &executor,
        "INSERT INTO acl_documents VALUES (1, 'draft', 'classified')",
    )?;
    for name in ["acl_alice", "acl_bob", "acl_carol"] {
        execute(&executor, &format!("CREATE PRINCIPAL {name}"))?;
        execute(
            &executor,
            &format!("GRANT CONNECT ON DATABASE test TO {name}"),
        )?;
        execute(
            &executor,
            &format!("GRANT USAGE ON SCHEMA public TO {name}"),
        )?;
    }
    execute(&executor, "CREATE ROLE acl_reader")?;
    execute(
        &executor,
        "GRANT SELECT (id, title), UPDATE (title)
         ON TABLE acl_documents TO acl_reader",
    )?;
    execute(&executor, "GRANT acl_reader TO acl_alice")?;

    let alice_id = object_id(&executor, ObjectKind::Principal, "acl_alice");
    let bob_id = object_id(&executor, ObjectKind::Principal, "acl_bob");
    let carol_id = object_id(&executor, ObjectKind::Principal, "acl_carol");
    let alice = context(alice_id);
    let bob = context(bob_id);
    let carol = context(carol_id);

    assert_eq!(
        query_value(
            &executor,
            "SELECT title FROM acl_documents WHERE id = 1",
            &alice,
        )?,
        Value::text("draft"),
    );
    expect_denied(executor.execute_with_context("SELECT secret FROM acl_documents", &alice));
    expect_denied(executor.execute_with_context(
        "SELECT left_doc.id FROM acl_documents AS left_doc
         JOIN acl_documents AS right_doc ON left_doc.id = right_doc.id",
        &alice,
    ));
    execute_as(
        &executor,
        "UPDATE acl_documents SET title = 'review' WHERE id = 1",
        &alice,
    )?;
    expect_denied(executor.execute_with_context(
        "UPDATE acl_documents SET secret = 'leaked' WHERE id = 1",
        &alice,
    ));

    execute(&executor, "REVOKE acl_reader FROM acl_alice")?;
    expect_denied(
        executor.execute_with_context("SELECT title FROM acl_documents WHERE id = 1", &alice),
    );
    execute(&executor, "GRANT acl_reader TO acl_alice")?;

    // USAGE resolves names but does not grant CREATE in the namespace.
    expect_denied(executor.execute_with_context(
        "CREATE TABLE acl_alice_owned (id INTEGER PRIMARY KEY)",
        &alice,
    ));
    execute(&executor, "GRANT CREATE ON SCHEMA public TO acl_alice")?;
    execute_as(
        &executor,
        "CREATE TABLE acl_alice_owned (id INTEGER PRIMARY KEY)",
        &alice,
    )?;
    expect_denied(executor.execute_with_context(
        "ALTER TABLE acl_alice_owned ADD COLUMN denied INTEGER",
        &bob,
    ));
    execute(&executor, "ALTER TABLE acl_alice_owned OWNER TO acl_bob")?;
    execute_as(
        &executor,
        "ALTER TABLE acl_alice_owned ADD COLUMN accepted INTEGER",
        &bob,
    )?;

    execute(&executor, "CREATE ROLE acl_first")?;
    execute(&executor, "CREATE ROLE acl_second")?;
    execute(&executor, "GRANT acl_first TO acl_second")?;
    assert!(executor.execute("GRANT acl_second TO acl_first").is_err());

    execute(&executor, "CREATE ROLE acl_delegated")?;
    execute(
        &executor,
        "GRANT SELECT ON TABLE acl_documents TO acl_delegated",
    )?;
    execute(
        &executor,
        "GRANT acl_delegated TO acl_alice WITH ADMIN OPTION",
    )?;
    execute_as(&executor, "GRANT acl_delegated TO acl_carol", &alice)?;
    assert!(
        executor
            .execute("REVOKE acl_delegated FROM acl_alice RESTRICT")
            .is_err(),
        "RESTRICT must expose the delegated membership dependency",
    );
    execute(
        &executor,
        "REVOKE acl_delegated FROM acl_alice CASCADE",
    )?;
    expect_denied(executor.execute_with_context(
        "SELECT secret FROM acl_documents WHERE id = 1",
        &carol,
    ));

    execute(
        &executor,
        "GRANT SELECT (id) ON TABLE acl_documents TO acl_alice WITH GRANT OPTION",
    )?;
    execute_as(
        &executor,
        "GRANT SELECT (id) ON TABLE acl_documents TO acl_bob",
        &alice,
    )?;
    assert_eq!(
        query_value(&executor, "SELECT id FROM acl_documents", &bob)?,
        Value::Integer(1),
    );

    execute(
        &executor,
        "CREATE TABLE acl_protected_rows (id INTEGER PRIMARY KEY)",
    )?;
    execute(&executor, "INSERT INTO acl_protected_rows VALUES (7)")?;
    execute(
        &executor,
        "CREATE PROCEDURE acl_invoker_read(OUT output_value INTEGER NOT NULL)
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN
             SELECT id INTO STRICT output_value
             FROM acl_protected_rows WHERE id = 7;
         END;",
    )?;
    execute(
        &executor,
        "CREATE PROCEDURE acl_definer_read(OUT output_value INTEGER NOT NULL)
         LANGUAGE RADIX SECURITY DEFINER SEARCH PATH (public) AS BEGIN
             SELECT id INTO STRICT output_value
             FROM acl_protected_rows WHERE id = 7;
         END;",
    )?;
    execute(
        &executor,
        "GRANT EXECUTE ON PROCEDURE acl_invoker_read() TO acl_alice",
    )?;
    execute(
        &executor,
        "GRANT EXECUTE ON PROCEDURE acl_definer_read() TO acl_alice",
    )?;
    expect_denied(executor.execute_with_context("CALL acl_invoker_read()", &alice));
    assert_eq!(
        query_value(&executor, "CALL acl_definer_read()", &alice)?,
        Value::Integer(7),
    );
    expect_denied(executor.execute_with_context("SELECT id FROM acl_protected_rows", &alice));
    execute(
        &executor,
        "REVOKE EXECUTE ON PROCEDURE acl_definer_read() FROM acl_alice",
    )?;
    expect_denied(executor.execute_with_context("CALL acl_definer_read()", &alice));

    // Trigger attachment and every firing require exact function EXECUTE.
    execute(
        &executor,
        "CREATE TABLE acl_trigger_effects (id INTEGER PRIMARY KEY)",
    )?;
    execute(
        &executor,
        "CREATE FUNCTION acl_root_trigger() RETURNS TRIGGER
         LANGUAGE RADIX VOLATILE SECURITY DEFINER SEARCH PATH (public) AS
         DECLARE copied_id INTEGER;
         BEGIN
             copied_id := NEW.id;
             INSERT INTO acl_trigger_effects VALUES (:copied_id);
             RETURN NEW;
         END;",
    )?;
    let create_trigger = "CREATE TRIGGER acl_attached_without_execute
         BEFORE INSERT ON acl_alice_owned FOR EACH ROW
         EXECUTE FUNCTION acl_root_trigger();";
    expect_denied(executor.execute_with_context(create_trigger, &bob));
    execute(
        &executor,
        "GRANT EXECUTE ON FUNCTION acl_root_trigger() TO acl_bob",
    )?;
    execute_as(&executor, create_trigger, &bob)?;
    execute_as(
        &executor,
        "INSERT INTO acl_alice_owned (id) VALUES (42)",
        &bob,
    )?;
    assert_eq!(
        query_value(
            &executor,
            "SELECT COUNT(*) FROM acl_trigger_effects WHERE id = 42",
            &ExecutionContext::new(),
        )?,
        Value::Integer(1),
    );

    execute(
        &executor,
        "CREATE FUNCTION acl_context_value() RETURNS UUID NOT NULL
         LANGUAGE RADIX STABLE SECURITY INVOKER AS BEGIN
             RETURN CURRENT_PRINCIPAL;
         END;",
    )?;
    execute(
        &executor,
        "GRANT EXECUTE ON FUNCTION acl_context_value() TO acl_alice",
    )?;
    assert_eq!(
        query_value(&executor, "SELECT acl_context_value()", &alice)?,
        Value::uuid(alice_id.into_bytes()),
    );

    // Job RUN AS must have entry EXECUTE on the bound procedure.
    execute(
        &executor,
        "CREATE TABLE acl_job_effects (id INTEGER PRIMARY KEY)",
    )?;
    execute(
        &executor,
        "CREATE PROCEDURE acl_job_definer()
         LANGUAGE RADIX SECURITY DEFINER SEARCH PATH (public) AS BEGIN
             INSERT INTO acl_job_effects VALUES (1);
         END;",
    )?;
    execute(
        &executor,
        "CREATE JOB acl_job_without_execute
         SCHEDULE AT TIMESTAMP '2026-09-08T00:00:00Z'
         RUN AS acl_alice CALL acl_job_definer() ENABLE;",
    )?;
    let job_id = object_id(&executor, ObjectKind::Job, "acl_job_without_execute");
    let job_error = executor
        .execute_job_attempt(
            job_id,
            JobAttemptMetadata {
                scheduled_at_unix_ns: 0,
                attempt: 1,
                idempotency_key: "docs-acl-job".to_owned(),
            },
            &ExecutionContext::new(),
        )
        .expect_err("job execution without procedure EXECUTE unexpectedly succeeded");
    assert_eq!(job_error.kind().as_str(), "PL_JOB_ATTEMPT_FAILED");
    assert_eq!(
        job_error.cause().map(|kind| kind.as_str()),
        Some("PL_SECURITY_OBJECT_DENIED"),
    );
    assert!(job_error
        .details()
        .iter()
        .any(|detail| detail.key == "cause_category" && detail.value == "security"));
    assert_eq!(
        query_value(
            &executor,
            "SELECT COUNT(*) FROM acl_job_effects",
            &ExecutionContext::new(),
        )?,
        Value::Integer(0),
    );

    // Keep the IDs used above observable so the validation cannot optimize them away.
    assert_ne!(alice_id, bob_id);
    assert_ne!(bob_id, carol_id);
    println!(
        "acl-ok columns=enforced role-revoke=immediate definer=bounded \
         usage-create=separate delegated-grant=cascade trigger-execute=enforced \
         context-values=verified job-execute=enforced"
    );
    Ok(())
}

fn context(principal: ObjectId) -> ExecutionContext {
    ExecutionContext::new().with_principal_id(principal)
}

fn object_id(executor: &Executor, kind: ObjectKind, name: &str) -> ObjectId {
    executor
        .engine()
        .pin_catalog()
        .expect("catalog")
        .objects_of_kind(kind)
        .find(|object| object.name().normalized().as_str() == name)
        .unwrap_or_else(|| panic!("missing {kind:?} {name}"))
        .id()
}

fn execute(executor: &Executor, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut result = executor.execute(sql)?;
    while result.next() {}
    if let Some(error) = result.last_error() {
        return Err(error.into());
    }
    result.close()?;
    Ok(())
}

fn execute_as(
    executor: &Executor,
    sql: &str,
    context: &ExecutionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut result = executor.execute_with_context(sql, context)?;
    while result.next() {}
    if let Some(error) = result.last_error() {
        return Err(error.into());
    }
    result.close()?;
    Ok(())
}

fn query_value(
    executor: &Executor,
    sql: &str,
    context: &ExecutionContext,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut result = executor.execute_with_context(sql, context)?;
    if !result.next() {
        return Err(result
            .last_error()
            .unwrap_or_else(|| Error::internal("query returned no row"))
            .into());
    }
    let value = result
        .row()
        .get(0)
        .cloned()
        .ok_or_else(|| Error::internal("query returned no column"))?;
    assert!(!result.next());
    if let Some(error) = result.last_error() {
        return Err(error.into());
    }
    result.close()?;
    Ok(value)
}

fn expect_denied(result: Result<radixdb_executor::result::ExecutionResult, Error>) {
    let error = match result {
        Err(error) => error,
        Ok(mut result) => {
            while result.next() {}
            result
                .last_error()
                .expect("statement unexpectedly completed without an authorization error")
        }
    };
    assert!(
        matches!(error, Error::AuthorizationDenied(_)),
        "unexpected denial error: {error:?}"
    );
}
