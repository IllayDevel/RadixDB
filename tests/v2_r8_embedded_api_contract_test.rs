use chrono::{NaiveDate, TimeZone, Utc};
use radixdb::{
    named_params, params, ApiTransaction, Database, DecimalValue, Error, FromValue, Params,
    StorageTransaction, ToParam, Transaction, Value,
};

#[test]
fn cached_plan_is_read_only_and_closed_database_rejects_all_execution_entrypoints() {
    let db = Database::open_in_memory().unwrap();
    let plan = db.cached_plan("SELECT $1").unwrap();
    assert!(plan.has_params());
    assert_eq!(plan.param_count(), 1);
    assert_eq!(plan.parameter_contract().positional_count(), 1);
    assert!(matches!(
        plan.statement(),
        radixdb::parser::ast::Statement::Select(_)
    ));

    db.close().unwrap();
    assert!(matches!(
        db.execute("SELECT 1", ()),
        Err(Error::EngineNotOpen)
    ));
    assert!(matches!(
        db.query("SELECT 1", ()),
        Err(Error::EngineNotOpen)
    ));
    assert!(matches!(db.prepare("SELECT 1"), Err(Error::EngineNotOpen)));
    assert!(matches!(
        db.cached_plan("SELECT 1"),
        Err(Error::EngineNotOpen)
    ));
    assert!(matches!(
        db.query_plan(&plan, (1,)),
        Err(Error::EngineNotOpen)
    ));
    assert!(matches!(
        db.semantic_cache_stats(),
        Err(Error::EngineNotOpen)
    ));
}

#[test]
fn file_registry_rejects_conflicting_effective_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("configured");
    let first_dsn = format!(
        "file://{}?sync_mode=full&cleanup=off&checkpoint_interval=77",
        path.display()
    );
    let equivalent_dsn = format!(
        "file://{}?checkpoint_interval=77&cleanup=off&sync_mode=2",
        path.display()
    );
    let conflicting_dsn = format!(
        "file://{}?checkpoint_interval=77&cleanup=off&sync_mode=none",
        path.display()
    );

    let first = Database::open(&first_dsn).unwrap();
    assert_eq!(first.dsn(), first_dsn);
    let equivalent = Database::open(&equivalent_dsn).unwrap();
    assert_eq!(equivalent.dsn(), first_dsn);
    let error = match Database::open(&conflicting_dsn) {
        Ok(_) => panic!("config conflict must be rejected"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("different effective configuration"));
    drop(equivalent);
    first.close().unwrap();
}

#[test]
fn transaction_retains_owner_preserves_id_and_blocks_explicit_close_only_while_active() {
    let db = Database::open_in_memory().unwrap();
    let mut transaction: Transaction = db.begin().unwrap();
    let id = transaction.id();
    assert!(id > 0);
    assert!(db.close().is_err());
    drop(db);

    let value: i64 = transaction.query_one("SELECT 42", ()).unwrap();
    assert_eq!(value, 42);
    transaction.commit().unwrap();
    assert_eq!(transaction.id(), id);

    let _: &ApiTransaction = &transaction;
}

#[test]
fn high_level_statement_executes_inside_its_transaction_without_reparse() {
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE prepared_r8 (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    let multi = db
        .prepare("INSERT INTO prepared_r8 VALUES ($1); INSERT INTO prepared_r8 VALUES ($2)")
        .unwrap();
    let mut transaction = db.begin().unwrap();
    transaction.execute_prepared(&multi, (1, 2)).unwrap();
    let count: i64 = transaction
        .query_one("SELECT COUNT(*) FROM prepared_r8", ())
        .unwrap();
    assert_eq!(count, 2);
    transaction.commit().unwrap();

    let other = Database::open_in_memory().unwrap();
    let other_statement = other.prepare("SELECT 1").unwrap();
    let mut transaction = db.begin().unwrap();
    assert!(transaction.query_prepared(&other_statement, ()).is_err());
    transaction.rollback().unwrap();
}

#[test]
fn transaction_rejects_surplus_positional_and_named_bindings() {
    let db = Database::open_in_memory().unwrap();
    let mut transaction = db.begin().unwrap();
    assert!(transaction.query("SELECT $1", (1, 2)).is_err());
    assert!(transaction
        .query_named("SELECT :value", named_params! { value: 1, surplus: 2 })
        .is_err());
    transaction.rollback().unwrap();
}

#[test]
fn transaction_timeout_remains_owned_by_the_returned_cursor() {
    let db = Database::open_in_memory().unwrap();
    let mut transaction = db.begin().unwrap();
    let result = transaction.query_with_timeout(
        "SELECT value FROM generate_series(1, 10000000) AS g(value)",
        (),
        1,
    );
    match result {
        Err(Error::QueryCancelled) => {}
        Err(error) => panic!("unexpected timeout error: {error}"),
        Ok(mut rows) => {
            let mut cancelled = false;
            for _ in 0..1_000_000 {
                match rows.next() {
                    Some(Ok(_)) => {}
                    Some(Err(Error::QueryCancelled)) => {
                        cancelled = true;
                        break;
                    }
                    Some(Err(error)) => panic!("unexpected cursor error: {error}"),
                    None => break,
                }
            }
            assert!(cancelled, "transaction cursor deadline did not fire");
        }
    }
    transaction.rollback().unwrap();
}

#[test]
fn params_macro_and_typed_value_families_have_no_hidden_facade_gap() {
    let many = params![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    assert_eq!(many.len(), 16);
    let array = [
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
        Value::Integer(4),
    ]
    .into_params();
    let slice = (&[Value::Integer(5), Value::Integer(6), Value::Integer(7)][..]).into_params();
    assert_eq!(array.len(), 4);
    assert_eq!(slice.len(), 3);

    let timestamp = Utc.with_ymd_and_hms(2026, 8, 15, 12, 34, 56).unwrap();
    let date = NaiveDate::from_ymd_opt(2026, 8, 15).unwrap();
    let uuid = uuid::Uuid::from_u128(0x12345678_1234_5678_9abc_def012345678);
    let json = serde_json::json!({"r8": [1, true, null]});
    let vector = vec![1.25_f32, -2.5, 3.75];
    let bytes = vec![0_u8, 1, 2, 255];
    let decimal = DecimalValue::try_new(12345, 8, 2).unwrap();

    let cases = [
        timestamp.to_param(),
        date.to_param(),
        uuid.to_param(),
        json.to_param(),
        vector.to_param(),
        bytes.to_param(),
        decimal.to_param(),
    ];
    assert_eq!(
        chrono::DateTime::<Utc>::from_value(&cases[0]).unwrap(),
        timestamp
    );
    assert_eq!(NaiveDate::from_value(&cases[1]).unwrap(), date);
    assert_eq!(uuid::Uuid::from_value(&cases[2]).unwrap(), uuid);
    assert_eq!(serde_json::Value::from_value(&cases[3]).unwrap(), json);
    assert_eq!(Vec::<f32>::from_value(&cases[4]).unwrap(), vector);
    assert_eq!(Vec::<u8>::from_value(&cases[5]).unwrap(), bytes);
    assert_eq!(DecimalValue::from_value(&cases[6]).unwrap(), decimal);

    fn accepts_storage_transaction<T: StorageTransaction>() {}
    accepts_storage_transaction::<radixdb::MvccTransaction>();
    let _: Value = decimal.to_param();
}
