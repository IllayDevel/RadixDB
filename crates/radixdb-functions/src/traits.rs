use radixdb_core::{Result, Value};

use crate::{AggregateOrderBySpec, FunctionInfo};

/// Aggregate function implementation contract.
pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn info(&self) -> FunctionInfo;

    fn configure(&mut self, _options: &[Value]) {}

    fn set_order_by(&mut self, _directions: Vec<bool>) {}

    fn set_order_by_specs(&mut self, specs: Vec<AggregateOrderBySpec>) {
        self.set_order_by(specs.into_iter().map(|spec| spec.ascending).collect());
    }

    fn accumulate(&mut self, value: &Value, distinct: bool);

    fn accumulate_with_sort_key(&mut self, value: &Value, sort_keys: Vec<Value>, distinct: bool) {
        let _ = sort_keys;
        self.accumulate(value, distinct);
    }

    fn supports_order_by(&self) -> bool {
        false
    }

    fn result(&self) -> Value;

    fn try_result(&self) -> Result<Value> {
        Ok(self.result())
    }

    fn reset(&mut self);
}

/// Direct single-argument scalar implementation.
pub type NativeFn1 = fn(&mut Value);

/// Neutral query-lifetime signal supplied by the execution owner.
pub trait FunctionCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

/// Scalar function implementation contract.
pub trait ScalarFunction: Send + Sync {
    fn name(&self) -> &str;
    fn info(&self) -> FunctionInfo;
    fn evaluate(&self, args: &[Value]) -> Result<Value>;

    fn evaluate_with_cancellation(
        &self,
        args: &[Value],
        _cancellation: Option<&dyn FunctionCancellation>,
    ) -> Result<Value> {
        self.evaluate(args)
    }

    fn native_fn1(&self) -> Option<NativeFn1> {
        None
    }
}

/// Window function implementation contract.
pub trait WindowFunction: Send + Sync {
    fn name(&self) -> &str;
    fn info(&self) -> FunctionInfo;
    fn process(&self, partition: &[Value], order_by: &[Value], current_row: usize)
        -> Result<Value>;
}
