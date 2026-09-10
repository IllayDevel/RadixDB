// Copyright 2026 RadixDB Contributors

use std::panic::{catch_unwind, AssertUnwindSafe};

use radixdb::functions::aggregate::{ArrayAggFunction, CompiledAggregate};
use radixdb::functions::AggregateFunction;
use radixdb::{Database, Value};

#[test]
fn r5_l01_batch_c_aggregate_state_contract_matrix() {
    // ARRAY_AGG follows PostgreSQL/SQL collection semantics and retains NULL.
    let mut array = ArrayAggFunction::default();
    array.accumulate(&Value::Integer(1), false);
    array.accumulate(&Value::null_unknown(), false);
    array.accumulate(&Value::Integer(2), false);
    assert_eq!(
        array.try_result().unwrap(),
        Value::try_json("[1,null,2]").unwrap()
    );

    // Compiled SUM preserves exact overflow in DECIMAL instead of rounding it.
    let compiled = catch_unwind(AssertUnwindSafe(|| {
        let mut sum = CompiledAggregate::sum(false);
        sum.accumulate(&Value::Integer(i64::MAX));
        sum.accumulate(&Value::Integer(1));
        sum.result()
    }));
    assert!(compiled.is_ok(), "compiled SUM overflow panicked");
    assert_eq!(
        compiled.unwrap().as_decimal_parts(),
        Some((i64::MAX as i128 + 1, 19, 0))
    );

    // A multi-aggregate query crosses the 100k parallel threshold. Its result
    // must be identical to one logical state, including uneven final chunks.
    let db = Database::open("memory://r5_l01_batch_c").unwrap();
    let mut result = db
        .query(
            "SELECT AVG(value), VAR_POP(value), STDDEV_POP(value) \
             FROM generate_series(1, 100003) AS g(value)",
            (),
        )
        .unwrap();
    let row = result.next().unwrap().unwrap();
    let avg: f64 = row.get(0).unwrap();
    let variance: f64 = row.get(1).unwrap();
    let stddev: f64 = row.get(2).unwrap();
    assert!((avg - 50_002.0).abs() < 1e-9);
    let expected_variance = (100_003_f64.powi(2) - 1.0) / 12.0;
    assert!((variance - expected_variance).abs() < 1e-3);
    assert!((stddev - expected_variance.sqrt()).abs() < 1e-6);

    db.execute(
        "CREATE TABLE exact_aggregates (id INTEGER PRIMARY KEY, amount INTEGER, marker TEXT)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO exact_aggregates VALUES
         (1, 9007199254740992, '*'),
         (2, 1, '*'),
         (3, NULL, 'x')",
        (),
    )
    .unwrap();
    let mut exact = db
        .query(
            "SELECT SUM(amount), COUNT(DISTINCT marker), ARRAY_AGG(marker ORDER BY id)
             FROM exact_aggregates",
            (),
        )
        .unwrap();
    let row = exact.next().unwrap().unwrap();
    assert_eq!(
        row.get_value(0),
        Some(&Value::Integer(9_007_199_254_740_993))
    );
    assert_eq!(row.get_value(1), Some(&Value::Integer(2)));
    assert_eq!(
        row.get_value(2).and_then(Value::as_json),
        Some(r#"["*","*","x"]"#)
    );
}
