use super::*;
use crate::context::{ExecutionContextBuilder, TimeoutGuard};
use radixdb_core::row_vec::RowVec;
use radixdb_storage::DeferredColumnSource;

#[test]
fn deferred_executor_result_preserves_projection_until_requested() {
    let shared_rows = CompactArc::new(vec![Row::from_values(vec![
        Value::integer(7),
        Value::text("payload 'kept whole'"),
    ])]);
    let deferred = DeferredRow::projected(
        DeferredRow::shared(shared_rows, 0),
        DeferredRow::owned(Row::from_values(vec![Value::text("dictionary")])),
        CompactArc::from(vec![
            DeferredColumnSource::Left(1),
            DeferredColumnSource::Right(0),
        ]),
    );
    let columns = CompactArc::new(vec!["payload".to_string(), "name".to_string()]);
    let mut result = DeferredExecutorResult::with_arc_columns(columns, vec![deferred]);

    assert!(result.next());
    let deferred = result.take_deferred_row();
    assert!(deferred.is_deferred());
    assert_eq!(deferred.get(0), Some(&Value::text("payload 'kept whole'")));
    assert_eq!(deferred.get(1), Some(&Value::text("dictionary")));
    assert!(!result.next());
}

#[test]
fn deferred_executor_result_materializes_once_for_public_row_contract() {
    let deferred = DeferredRow::projected(
        DeferredRow::owned(Row::from_values(vec![Value::integer(11)])),
        DeferredRow::owned(Row::from_values(vec![Value::integer(22)])),
        CompactArc::from(vec![
            DeferredColumnSource::Right(0),
            DeferredColumnSource::Left(0),
        ]),
    );
    let columns = CompactArc::new(vec!["right".to_string(), "left".to_string()]);
    let mut result = DeferredExecutorResult::with_arc_columns(columns, vec![deferred]);

    assert!(result.next());
    assert_eq!(
        result.row(),
        &Row::from_values(vec![Value::integer(22), Value::integer(11)])
    );
    assert_eq!(
        result.take_row(),
        Row::from_values(vec![Value::integer(22), Value::integer(11)])
    );
}

#[test]
fn top_n_rejects_unbounded_capacity_before_allocating_the_heap() {
    let source = Box::new(ExecutorResult::new(Vec::new(), RowVec::new()));
    let result = TopNResult::new(
        source,
        |_left: &Row, _right: &Row| std::cmp::Ordering::Equal,
        RetainedRowsBudget::DEFAULT_MAX_ROWS + 1,
        0,
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("oversized TOP-N capacity must fail before allocation"),
    };
    assert!(error.to_string().contains("TOP-N retained-row budget"));
}

/// Helper to create RowVec from Vec<Row> for tests
fn make_rows(rows: Vec<Row>) -> RowVec {
    let mut rv = RowVec::with_capacity(rows.len());
    for (i, row) in rows.into_iter().enumerate() {
        rv.push((i as i64, row));
    }
    rv
}

#[test]
fn test_exec_result() {
    let result = ExecResult::new(5, 10);
    assert_eq!(result.rows_affected(), 5);
    assert_eq!(result.last_insert_id(), 10);
    assert_eq!(result.columns().len(), 0);
}

#[test]
fn test_exec_result_empty() {
    let result = ExecResult::empty();
    assert_eq!(result.rows_affected(), 0);
    assert_eq!(result.last_insert_id(), 0);
}

#[test]
fn r6_l01_b_timeout_remains_owned_by_streaming_result() {
    let ctx = ExecutionContextBuilder::new().timeout_ms(20).build();
    let guard = TimeoutGuard::new(&ctx);
    let mut result = TimedQueryResult::wrap(
        Box::new(ExecResult::empty()),
        guard,
        ctx.cancellation_handle(),
    );

    std::thread::sleep(std::time::Duration::from_millis(60));
    assert!(ctx.is_cancelled());
    assert!(!result.next());
    assert!(matches!(
        result.last_error(),
        Some(radixdb_core::Error::QueryCancelled)
    ));
    result.close().unwrap();
}

#[test]
fn test_memory_result() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(1), Value::text("Alice")]),
        Row::from_values(vec![Value::Integer(2), Value::text("Bob")]),
    ]);

    let mut result = ExecutorResult::new(columns, rows);
    assert_eq!(result.columns().len(), 2);
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.estimated_count(), Some(2));

    // First row
    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(1)));
    assert_eq!(result.estimated_count(), Some(1));

    // Second row
    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
    assert_eq!(result.estimated_count(), Some(0));

    // No more rows
    assert!(!result.next());
}

#[test]
fn test_filtered_result() {
    let columns = vec!["id".to_string(), "value".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        Row::from_values(vec![Value::Integer(3), Value::Integer(30)]),
    ]);

    let inner = Box::new(ExecutorResult::new(columns, rows));

    // Filter for value > 15 using Expression
    use radixdb_sql::ast::{Identifier, InfixExpression, InfixOperator, IntegerLiteral};
    use radixdb_sql::token::{Position, Token, TokenType};

    let dummy_token =
        |literal: &str, token_type| Token::new(token_type, literal, Position::default());

    let filter_expr = Expression::Infix(InfixExpression {
        token: dummy_token(">", TokenType::Operator),
        left: Box::new(Expression::Identifier(Identifier {
            token: dummy_token("value", TokenType::Identifier),
            value: "value".into(),
            value_lower: "value".into(),
        })),
        operator: ">".into(),
        op_type: InfixOperator::GreaterThan,
        right: Box::new(Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("15", TokenType::Integer),
            value: 15,
        })),
    });

    let mut result = FilteredResult::with_defaults(inner, filter_expr).unwrap();

    // Should get rows with value 20 and 30
    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(2)));

    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(3)));

    assert!(!result.next());
}

#[test]
fn prefetched_result_replays_inspected_row_before_source() {
    let columns = vec!["id".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(1)]),
        Row::from_values(vec![Value::Integer(2)]),
        Row::from_values(vec![Value::Integer(3)]),
    ]);
    let mut inner = ExecutorResult::new(columns, rows);

    assert!(inner.next(), "optimizer inspects the first source row");
    let inspected = inner.take_row();
    let mut result = PrefetchedResult::new(inspected, Box::new(inner));

    let mut values = Vec::new();
    while result.next() {
        values.push(result.take_row().get(0).cloned().unwrap());
    }

    assert_eq!(
        values,
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        "a rejected optimizer probe must not drop or duplicate the inspected row"
    );
}

#[test]
fn test_limited_result() {
    let columns = vec!["id".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(1)]),
        Row::from_values(vec![Value::Integer(2)]),
        Row::from_values(vec![Value::Integer(3)]),
        Row::from_values(vec![Value::Integer(4)]),
        Row::from_values(vec![Value::Integer(5)]),
    ]);

    let inner = Box::new(ExecutorResult::new(columns, rows));
    let mut result = LimitedResult::new(inner, Some(2), 1);

    // Skip first row (offset 1), then take 2 rows
    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(2)));

    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(3)));

    assert!(!result.next()); // Limit reached
}

#[test]
fn test_ordered_result() {
    let columns = vec!["id".to_string(), "value".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(3), Value::Integer(30)]),
        Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
    ]);

    let inner = Box::new(ExecutorResult::new(columns, rows));

    // Sort by id ascending
    let mut result = OrderedResult::new(inner, |a, b| {
        let a_id = a.get(0).and_then(|v| v.as_int64()).unwrap_or(0);
        let b_id = b.get(0).and_then(|v| v.as_int64()).unwrap_or(0);
        a_id.cmp(&b_id)
    })
    .unwrap();

    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(1)));

    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(2)));

    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&Value::Integer(3)));

    assert!(!result.next());
}

#[test]
fn uuid_radix_sort_preserves_lexicographic_order_in_both_directions() {
    let uuid = |high: u64, low: u64| {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..].copy_from_slice(&low.to_be_bytes());
        Value::uuid(bytes)
    };
    let input = vec![
        Row::from_values(vec![uuid(1, 0)]),
        Row::from_values(vec![uuid(0, u64::MAX)]),
        Row::from_values(vec![uuid(0, 1)]),
        Row::from_values(vec![uuid(u64::MAX, 0)]),
    ];

    let mut ascending = make_rows(input.clone());
    assert!(OrderedResult::try_radix_sort_single_uuid(
        &mut ascending,
        0,
        true
    ));
    let ascending_keys: Vec<_> = ascending
        .iter()
        .map(|(_, row)| row[0].as_uuid_bytes().unwrap())
        .collect();
    assert!(ascending_keys.windows(2).all(|pair| pair[0] <= pair[1]));

    let mut descending = make_rows(input);
    assert!(OrderedResult::try_radix_sort_single_uuid(
        &mut descending,
        0,
        false
    ));
    let descending_keys: Vec<_> = descending
        .iter()
        .map(|(_, row)| row[0].as_uuid_bytes().unwrap())
        .collect();
    assert!(descending_keys.windows(2).all(|pair| pair[0] >= pair[1]));
}

#[test]
fn ordered_result_spills_bounded_runs_and_streams_exact_order() {
    const ROWS: i64 = 140_000;
    let columns = vec!["id".to_string()];
    let rows = make_rows(
        (0..ROWS)
            .rev()
            .map(|id| Row::from_values(vec![Value::Integer(id)]))
            .collect(),
    );

    radixdb_storage::instrumentation::begin_join_execution_probe();
    let inner = Box::new(ExecutorResult::new(columns, rows));
    let mut result = OrderedResult::new(inner, |left, right| {
        left.get(0)
            .and_then(Value::as_int64)
            .cmp(&right.get(0).and_then(Value::as_int64))
    })
    .unwrap();

    let mut expected = 0;
    while result.next() {
        assert_eq!(result.row().get(0), Some(&Value::Integer(expected)));
        expected += 1;
    }
    assert_eq!(expected, ROWS);
    assert!(result.last_error().is_none());
    drop(result);

    let probe = radixdb_storage::instrumentation::end_join_execution_probe();
    assert_eq!(probe.ordered_sort_calls, 1);
    assert_eq!(probe.ordered_sort_input_rows, ROWS as u64);
    assert_eq!(probe.ordered_sort_spill_runs, 2);
    assert!(probe.ordered_sort_peak_rows <= ORDERED_RUN_MAX_ROWS as u64);
    assert!(probe.ordered_sort_peak_bytes <= ORDERED_RUN_MAX_BYTES as u64);
}

#[test]
fn test_distinct_result() {
    let columns = vec!["name".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::text("Alice")]),
        Row::from_values(vec![Value::text("Bob")]),
        Row::from_values(vec![Value::text("Alice")]), // Duplicate
        Row::from_values(vec![Value::text("Charlie")]),
    ]);

    let inner = Box::new(ExecutorResult::new(columns, rows));
    let mut result = DistinctResult::new(inner);

    let mut names = Vec::new();
    while result.next() {
        if let Some(Value::Text(name)) = result.row().get(0) {
            names.push(name.to_string());
        }
    }

    assert_eq!(names.len(), 3);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_distinct_result_uses_exact_integer_float_identity() {
    let boundary = 1_i64 << 53;
    let columns = vec!["number".to_string()];
    let rows = make_rows(vec![
        Row::from_values(vec![Value::Integer(boundary)]),
        Row::from_values(vec![Value::Float(boundary as f64)]),
        Row::from_values(vec![Value::Integer(boundary + 1)]),
    ]);

    let inner = Box::new(ExecutorResult::new(columns, rows));
    let mut result = DistinctResult::new(inner);
    let mut values = Vec::new();
    while result.next() {
        values.push(result.row().get(0).cloned().unwrap());
    }

    assert_eq!(values.len(), 2);
    assert_eq!(
        values
            .iter()
            .filter(|value| **value == Value::Integer(boundary))
            .count(),
        1
    );
    assert!(values
        .iter()
        .any(|value| matches!(value, Value::Integer(integer) if *integer == boundary + 1)));
}

#[test]
fn r5_l04_batch_h_distinct_wrappers_require_exact_scan_state_and_width() {
    let columns = vec!["a".to_string(), "b".to_string()];
    let rows = make_rows(vec![Row::from_values(vec![
        Value::Integer(1),
        Value::Integer(2),
    ])]);
    let mut distinct =
        DistinctResult::new(Box::new(ExecutorResult::new(columns.clone(), rows.clone())));
    assert!(distinct.scan(&mut vec![Value::null_unknown(); 2]).is_err());
    assert!(distinct.next());
    assert!(distinct.scan(&mut vec![Value::null_unknown(); 1]).is_err());

    let mut distinct_on =
        DistinctOnResult::new(Box::new(ExecutorResult::new(columns, rows)), vec![0]);
    assert!(distinct_on
        .scan(&mut vec![Value::null_unknown(); 2])
        .is_err());
    assert!(distinct_on.next());
    assert!(distinct_on
        .scan(&mut vec![Value::null_unknown(); 1])
        .is_err());
}

#[test]
fn test_aliased_result() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = make_rows(vec![Row::from_values(vec![
        Value::Integer(1),
        Value::text("Alice"),
    ])]);

    let inner = Box::new(ExecutorResult::new(columns, rows));

    let mut aliases = FxHashMap::default();
    aliases.insert("user_name".to_string(), "name".to_string());

    let mut result = AliasedResult::new(inner, aliases);

    assert_eq!(result.columns(), &["id", "user_name"]);

    assert!(result.next());
    assert_eq!(result.row().get(1), Some(&Value::text("Alice")));
}
