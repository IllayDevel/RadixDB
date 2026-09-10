// Copyright 2026 RadixDB Contributors

use radixdb::Database;

#[test]
fn r5_l01_batch_d_window_contract_matrix() {
    let db = Database::open("memory://r5_l01_batch_d").unwrap();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER, s TEXT)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 1, 10, 'a'), (2, 1, 20, 'b'), (3, 2, 30, 'c')",
        (),
    )
    .unwrap();

    let range_values: Vec<i64> = db
        .query(
            "SELECT LAST_VALUE(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW) \
             FROM t ORDER BY id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect();
    assert_eq!(range_values, vec![20, 20, 30]);

    let lead_values: Vec<Option<i64>> = db
        .query(
            "SELECT LEAD(v + 10) OVER (ORDER BY id) FROM t ORDER BY id",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get::<Option<i64>>(0).unwrap())
        .collect();
    assert_eq!(lead_values, vec![Some(30), Some(40), None]);

    let order_error = db.query(
        "SELECT ROW_NUMBER() OVER (ORDER BY s REGEXP $1) FROM t",
        ("[invalid",),
    );
    assert!(order_error.is_err() || order_error.unwrap().any(|row| row.is_err()));

    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, k INTEGER)", ())
        .unwrap();
    // The TVF keeps this oracle compact while crossing the old quadratic region.
    db.execute(
        "INSERT INTO big SELECT value, value / 100 FROM generate_series(1, 10000)",
        (),
    )
    .unwrap();
    let mut tail_result = db
        .query(
            "SELECT PERCENT_RANK() OVER (ORDER BY k), CUME_DIST() OVER (ORDER BY k) \
             FROM big ORDER BY id DESC LIMIT 1",
            (),
        )
        .unwrap();
    let tail = tail_result.next().unwrap().unwrap();
    assert!(tail.get::<f64>(0).unwrap() > 0.99);
    assert_eq!(tail.get::<f64>(1).unwrap(), 1.0);
}
