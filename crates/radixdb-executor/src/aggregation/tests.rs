use super::*;

struct TestHost {
    engine: Arc<MVCCEngine>,
    registry: FunctionRegistry,
    active: Mutex<Option<ActiveTransaction>>,
}

impl TestHost {
    fn new() -> Self {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        Self {
            engine,
            registry: FunctionRegistry::new(),
            active: Mutex::new(None),
        }
    }
}

impl AggregationHost for TestHost {
    fn aggregation_engine(&self) -> &Arc<MVCCEngine> {
        &self.engine
    }
    fn aggregation_function_registry(&self) -> &FunctionRegistry {
        &self.registry
    }
    fn aggregation_active_transaction(&self) -> &Mutex<Option<ActiveTransaction>> {
        &self.active
    }
    fn aggregation_process_where_subqueries(
        &self,
        expression: &Expression,
        _context: &ExecutionContext,
    ) -> Result<Expression> {
        Ok(expression.clone())
    }
    fn aggregation_try_process_select_subqueries(
        &self,
        _columns: &[Expression],
        _context: &ExecutionContext,
    ) -> Result<Option<Vec<Expression>>> {
        Ok(None)
    }
    fn aggregation_has_correlated_subqueries(&self, _expression: &Expression) -> bool {
        false
    }
    fn aggregation_process_correlated_expression(
        &self,
        expression: &Expression,
        _context: &ExecutionContext,
    ) -> Result<Expression> {
        Ok(expression.clone())
    }
    fn aggregation_output_column_names(
        &self,
        expressions: &[Expression],
        columns: &[String],
        alias: Option<&str>,
    ) -> Vec<String> {
        crate::pipeline::projection::output_column_names(expressions, columns, alias)
    }
}

fn count_single_column_groups(values: Vec<Value>) -> Vec<(Value, i64)> {
    let host = TestHost::new();
    let executor = AggregationExecutor::new(&host);
    let rows: RowVec = values
        .into_iter()
        .enumerate()
        .map(|(id, value)| (id as i64, Row::from_values(vec![value])))
        .collect();
    let aggregate = SqlAggregateFunction {
        name: "COUNT".to_string(),
        column: "*".to_string(),
        column_lower: "*".to_string(),
        alias: None,
        distinct: false,
        extra_args: Vec::new(),
        expression: None,
        order_by: Vec::new(),
        filter: None,
        hidden: false,
    };
    let (_, grouped) = executor
        .try_fast_aggregation_single_column(
            &0,
            &[SimpleAgg::Count(None)],
            &[aggregate],
            &[GroupByItem::Column("key".to_string())],
            &rows,
            None,
            None,
        )
        .unwrap()
        .expect("single-column grouping should be applicable");

    grouped
        .into_iter()
        .map(|(_, row)| {
            let key = row.get(0).unwrap().clone();
            let count = match row.get(1) {
                Some(Value::Integer(count)) => *count,
                other => panic!("unexpected COUNT result: {other:?}"),
            };
            (key, count)
        })
        .collect()
}

#[test]
fn test_single_column_grouping_mixed_numeric_identity_and_late_mismatch() {
    let boundary = 1_i64 << 53;
    let mut values = vec![Value::Integer(boundary); 16];
    values.push(Value::Float(boundary as f64));
    values.push(Value::Integer(boundary + 1));

    let groups = count_single_column_groups(values);
    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Integer(boundary) && *count == 17));
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Integer(boundary + 1) && *count == 1));
}

#[test]
fn test_single_column_grouping_canonical_signed_zero_and_nan() {
    let groups = count_single_column_groups(vec![
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Float(f64::NAN),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
    ]);

    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Float(0.0) && *count == 2));
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Float(f64::NAN) && *count == 2));
}

#[test]
fn test_single_column_grouping_i64_min_uses_full_domain_path() {
    let groups = count_single_column_groups(vec![
        Value::Integer(i64::MIN),
        Value::Integer(i64::MIN),
        Value::Integer(0),
    ]);

    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Integer(i64::MIN) && *count == 2));
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Integer(0) && *count == 1));
}

#[test]
fn test_single_column_grouping_text_fast_path_does_not_drop_late_mismatch() {
    let mut values = vec![Value::text("same"); 16];
    values.push(Value::Integer(17));

    let groups = count_single_column_groups(values);
    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::text("same") && *count == 16));
    assert!(groups
        .iter()
        .any(|(key, count)| key == &Value::Integer(17) && *count == 1));
}

#[test]
fn test_streaming_count_distinct_uses_canonical_mixed_numeric_identity() {
    let boundary = 1_i64 << 53;
    let mut seen = ValueSet::default();

    assert!(track_distinct_value(&mut seen, &Value::Integer(boundary)));
    assert!(!track_distinct_value(
        &mut seen,
        &Value::Float(boundary as f64)
    ));
    assert!(track_distinct_value(
        &mut seen,
        &Value::Integer(boundary + 1)
    ));
    assert!(track_distinct_value(&mut seen, &Value::Float(0.0)));
    assert!(!track_distinct_value(&mut seen, &Value::Float(-0.0)));
    assert!(track_distinct_value(&mut seen, &Value::Float(f64::NAN)));
    assert!(!track_distinct_value(
        &mut seen,
        &Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))
    ));
    assert_eq!(seen.len(), 4);
}

#[test]
fn test_streaming_aggregate_projection_deduplicates_columns() {
    let (scan_columns, projected) =
        AggregationExecutor::<TestHost>::build_streaming_aggregate_projection(&[
            Some(4),
            None,
            Some(2),
            Some(4),
            Some(2),
        ]);

    assert_eq!(scan_columns, vec![4, 2]);
    assert_eq!(projected, vec![Some(0), None, Some(1), Some(0), Some(1)]);
}
