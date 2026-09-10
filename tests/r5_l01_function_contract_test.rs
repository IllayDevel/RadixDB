// Copyright 2026 RadixDB Contributors

use radixdb::functions::scalar::{value_to_string, CastFunction, JsonArrayFunction};
use radixdb::functions::ScalarFunction;
use radixdb::optimizer::simplify::ExpressionSimplifier;
use radixdb::parser::ast::{Expression, Statement};
use radixdb::parser::parse_sql;
use radixdb::{Database, Value};

fn where_expression(sql: &str) -> Expression {
    let statements = parse_sql(sql).expect("parse batch-B expression");
    let Statement::Select(select) = &statements[0] else {
        panic!("expected SELECT")
    };
    select
        .where_clause
        .as_deref()
        .expect("expected WHERE expression")
        .clone()
}

#[test]
fn r5_l01_batch_b_function_contract_matrix() {
    // Optimizer volatility must come from FunctionInfo, not a second name list.
    let volatile = where_expression("SELECT 1 WHERE SLEEP(0) = SLEEP(0)");
    let simplified = ExpressionSimplifier::new().simplify(&volatile);
    assert!(matches!(simplified, Expression::Infix(_)));

    let db = Database::open("memory://r5_l01_batch_b").unwrap();

    // IIF is CASE shorthand: an unselected failing branch is never evaluated.
    let selected: i64 = db
        .query_one("SELECT IIF(TRUE, 7, CAST('not-an-int' AS INTEGER))", ())
        .unwrap();
    assert_eq!(selected, 7);

    // Signature metadata is an admission contract, not introspection-only data.
    assert!(db.query_one::<i64, _>("SELECT ABS(1, 2)", ()).is_err());
    assert!(db
        .query_one::<i64, _>("SELECT NTILE('bad') OVER (ORDER BY 1)", ())
        .is_err());

    // Valid typed extensions must retain their value in conversion, hashing and JSON.
    let uuid_a = Value::uuid([0x11; 16]);
    let uuid_b = Value::uuid([0x22; 16]);
    let uuid_text = value_to_string(&uuid_a);
    assert!(!uuid_text.is_empty());
    assert_ne!(uuid_text, value_to_string(&uuid_b));

    let cast = CastFunction;
    assert_eq!(
        cast.evaluate(&[uuid_a.clone(), Value::text("TEXT")])
            .unwrap(),
        Value::text(uuid_text.clone())
    );

    let json = JsonArrayFunction
        .evaluate(&[
            uuid_a,
            Value::try_decimal(100, 3, 2).unwrap(),
            Value::date(1),
            Value::bytes(vec![0xde, 0xad]),
            Value::vector(vec![1.0, 2.0]),
        ])
        .unwrap();
    let json = json.as_json().unwrap();
    assert!(json.contains(&uuid_text));
    assert!(json.contains("1.00"));
    assert!(json.contains("dead"));
    assert!(json.contains("1"));
}
