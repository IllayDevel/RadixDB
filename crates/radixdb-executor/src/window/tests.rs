use super::*;

struct TestHost {
    registry: FunctionRegistry,
}

impl WindowHost for TestHost {
    fn window_function_registry(&self) -> &FunctionRegistry {
        &self.registry
    }
}
#[test]
fn test_columnar_window_sort_preserves_exact_values_and_null_policy() {
    let values = ColumnarOrderByValues {
        columns: vec![vec![
            Value::null_unknown(),
            Value::Integer(9_007_199_254_740_993),
            Value::Float(9_007_199_254_740_992.0),
            Value::Integer(i64::MAX),
        ]],
        ascending: vec![false],
        nulls_first: vec![false],
        num_rows: 4,
    };
    let mut indices = vec![0, 1, 2, 3];
    WindowExecutor::<TestHost>::sort_by_order_values(&mut indices, &values);
    assert_eq!(indices, vec![3, 1, 2, 0]);

    let values = ColumnarOrderByValues {
        columns: values.columns,
        ascending: vec![false],
        nulls_first: vec![true],
        num_rows: 4,
    };
    let mut indices = vec![0, 1, 2, 3];
    WindowExecutor::<TestHost>::sort_by_order_values(&mut indices, &values);
    assert_eq!(indices, vec![0, 3, 1, 2]);
}

#[test]
fn test_parallel_threshold_window_sort_preserves_multikey_numeric_contract() {
    const ROW_COUNT: usize = 10_000;

    let mut primary = Vec::with_capacity(ROW_COUNT);
    let mut secondary = Vec::with_capacity(ROW_COUNT);

    primary.extend([
        Value::Integer(9_007_199_254_740_993),
        Value::Float(9_007_199_254_740_992.0),
        Value::Integer(7),
        Value::Integer(7),
        Value::Integer(7),
        Value::null_unknown(),
        Value::Integer(7),
    ]);
    secondary.extend([
        Value::decimal(100, 3, 2),
        Value::decimal(10, 2, 1),
        Value::decimal(90, 2, 2),
        Value::decimal(100, 3, 2),
        Value::decimal(10, 2, 1),
        Value::decimal(0, 1, 0),
        Value::null_unknown(),
    ]);

    for row_idx in 7..ROW_COUNT {
        primary.push(Value::Integer(1_000 + row_idx as i64));
        secondary.push(Value::decimal(0, 1, 0));
    }

    let values = ColumnarOrderByValues {
        columns: vec![primary, secondary],
        ascending: vec![true, true],
        nulls_first: vec![false, true],
        num_rows: ROW_COUNT,
    };
    let mut indices: Vec<usize> = (0..ROW_COUNT).rev().collect();
    WindowExecutor::<TestHost>::sort_by_order_values(&mut indices, &values);

    assert_eq!(
        indices[0], 6,
        "secondary NULLS FIRST must lead the peer group"
    );
    assert_eq!(indices[1], 2, "0.90 must sort before canonical Decimal one");
    assert!(
        matches!((indices[2], indices[3]), (3, 4) | (4, 3)),
        "1.00 and 1.0 are canonical peers"
    );
    assert!(values.rows_equal(3, 4));

    let float_position = indices.iter().position(|&idx| idx == 1).unwrap();
    let integer_position = indices.iter().position(|&idx| idx == 0).unwrap();
    assert!(
        float_position < integer_position,
        "2^53 Float must sort before the exact 2^53+1 Integer"
    );
    assert_eq!(
        indices.last(),
        Some(&5),
        "primary NULLS LAST must be preserved"
    );
}
