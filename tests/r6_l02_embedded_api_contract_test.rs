use chrono::{TimeZone, Utc};
use radixdb::{named_params, Database, FromValue, NamedParams, Value};

fn assert_error<T>(result: radixdb::Result<T>, context: &str) {
    assert!(result.is_err(), "{context}");
}

#[test]
fn embedded_api_preserves_value_row_plan_parameter_and_transaction_identity() -> radixdb::Result<()>
{
    // AUD-CONTRACT-274/275: public typed extraction is checked and lossless.
    let rejected_i64 = [
        Value::Float(1.5),
        Value::Float(f64::INFINITY),
        Value::Float(i64::MAX as f64),
    ];
    for value in &rejected_i64 {
        assert_error(i64::from_value(value), "lossy Float -> i64 conversion");
    }
    assert_error(
        i32::from_value(&Value::Integer(i64::MAX)),
        "wrapping i64 -> i32 conversion",
    );
    assert_error(
        f64::from_value(&Value::Integer((1_i64 << 53) + 1)),
        "rounded i64 -> f64 conversion",
    );
    assert_error(
        String::from_value(&Value::null_unknown()),
        "SQL NULL must not become an empty Rust String",
    );
    let timestamp = Utc.timestamp_nanos(1_712_345_678_123_456_789);
    let timestamp_text = String::from_value(&Value::timestamp(timestamp))?;
    assert!(
        timestamp_text.contains(".123456789"),
        "timestamp precision was lost: {timestamp_text}"
    );

    let db = Database::open("memory://r6-l02-embedded-contract")?;
    db.execute(
        "CREATE TABLE left_items (id INTEGER PRIMARY KEY, payload TEXT);\
         CREATE TABLE right_items (id INTEGER PRIMARY KEY, payload TEXT);\
         INSERT INTO left_items VALUES (1, 'left');\
         INSERT INTO right_items VALUES (1, 'right')",
        (),
    )?;

    // AUD-CONTRACT-276/277: duplicate labels and cursor preconditions are errors.
    let row = db
        .query(
            "SELECT left_items.id, right_items.id FROM left_items \
             JOIN right_items ON left_items.id = right_items.id",
            (),
        )?
        .next()
        .expect("one joined row")?;
    assert_error(
        row.get_by_name::<i64>("id"),
        "duplicate result labels must be ambiguous",
    );
    assert_error(row.is_null(99), "out-of-range NULL lookup must fail");

    let mut cursor = db.query("SELECT id FROM left_items", ())?;
    assert_error(
        cursor.current_row().map(|_| ()),
        "cursor before advance must not expose a row",
    );
    assert!(cursor.advance());
    assert_eq!(cursor.current_row()?.get(0), Some(&Value::Integer(1)));
    assert!(!cursor.advance());
    assert_error(
        cursor.current_row().map(|_| ()),
        "cursor after EOF must not expose a stale row",
    );

    // AUD-CONTRACT-278: absence is the only error mapped to false.
    assert!(!db.table_exists("missing_table")?);
    let closed = Database::open("memory://r6-l02-table-exists-error")?;
    closed.close()?;
    assert_error(
        closed.table_exists("anything"),
        "closed-engine error must not become false",
    );

    // AUD-CONTRACT-279: a compiled plan belongs to exactly one Database owner.
    let plan_a = Database::open("memory://r6-l02-plan-owner-a")?;
    let plan_b = Database::open("memory://r6-l02-plan-owner-b")?;
    for owner in [&plan_a, &plan_b] {
        owner.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)",
            (),
        )?;
    }
    plan_a.execute("INSERT INTO items VALUES (1, 'a')", ())?;
    plan_b.execute("INSERT INTO items VALUES (1, 'b')", ())?;
    let foreign_plan = plan_a.cached_plan("SELECT * FROM items WHERE id = $1")?;
    assert_eq!(
        plan_a
            .query_plan(&foreign_plan, (1_i64,))?
            .next()
            .expect("owner row")?
            .get::<String>(1)?,
        "a"
    );
    assert_error(
        plan_b.query_plan(&foreign_plan, (1_i64,)),
        "cross-database cached plan execution must fail",
    );

    // AUD-CONTRACT-280: positional and named bindings have an exact shape.
    let positional = db.cached_plan("SELECT payload FROM left_items WHERE id = $1")?;
    assert_error(
        db.query_plan(&positional, (1_i64, 2_i64)),
        "surplus positional parameters must fail",
    );
    let anonymous = db.cached_plan("SELECT payload FROM left_items WHERE id = ?")?;
    assert_error(
        db.query_plan(&anonymous, ()),
        "missing anonymous parameter must fail",
    );
    let named = db.cached_plan("SELECT payload FROM left_items WHERE id = :id")?;
    assert_error(
        db.query_named_plan(&named, NamedParams::new()),
        "missing named parameter must fail",
    );
    assert_error(
        db.query_named_plan(&named, named_params! { id: 1_i64, extra: 2_i64 }),
        "surplus named parameter must fail",
    );

    // AUD-CONTRACT-281: the Transaction facade delegates the complete SQL
    // statement surface to the same executor instead of maintaining a DDL list.
    db.execute("CREATE TABLE transaction_drop (id INTEGER PRIMARY KEY)", ())?;
    let mut transaction = db.begin()?;
    transaction.execute("DROP TABLE transaction_drop", ())?;
    transaction.commit()?;
    assert!(!db.table_exists("transaction_drop")?);
    assert_error(db.execute("", ()), "empty SQL must have one public outcome");
    assert_error(
        db.execute("1", ()),
        "bare expressions are not SQL statements",
    );
    assert_error(db.prepare("1"), "prepared expression must match direct SQL");

    Ok(())
}
