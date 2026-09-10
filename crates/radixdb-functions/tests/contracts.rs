use radixdb_core::Value;
use radixdb_functions::{
    AggregateFunction, AggregateOrderBySpec, FunctionDataType, FunctionInfo, FunctionSignature,
    FunctionType,
};

#[derive(Default)]
struct LegacyOrderedAggregate {
    directions: Vec<bool>,
}

impl AggregateFunction for LegacyOrderedAggregate {
    fn name(&self) -> &str {
        "LEGACY_ORDERED"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            self.name(),
            FunctionType::Aggregate,
            "legacy hook contract",
            FunctionSignature::new(FunctionDataType::Any, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn set_order_by(&mut self, directions: Vec<bool>) {
        self.directions = directions;
    }

    fn accumulate(&mut self, _value: &Value, _distinct: bool) {}
    fn result(&self) -> Value {
        Value::null_unknown()
    }
    fn reset(&mut self) {}
}

#[test]
fn signature_validation_preserves_count_and_runtime_type_contracts() {
    let signature = FunctionSignature::new(
        FunctionDataType::Integer,
        vec![FunctionDataType::Integer],
        1,
        1,
    );
    assert!(signature.validate_values(&[Value::Integer(7)]).is_ok());
    assert!(signature.validate_values(&[]).is_err());
    assert!(signature.validate_values(&[Value::text("wrong")]).is_err());
}

#[test]
fn legacy_order_hook_remains_the_default_adapter() {
    let mut aggregate = LegacyOrderedAggregate::default();
    aggregate.set_order_by_specs(vec![
        AggregateOrderBySpec::new(false, Some(false)),
        AggregateOrderBySpec::new(true, Some(true)),
    ]);
    assert_eq!(aggregate.directions, vec![false, true]);
}
