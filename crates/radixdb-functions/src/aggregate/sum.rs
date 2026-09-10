// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SUM aggregate function

use radixdb_core::{Result, Value};

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

use super::{numeric::NumericAccumulator, DistinctTracker};

/// SUM aggregate function
///
/// Returns the sum of all non-NULL values in the specified column.
/// Returns int64 for integer inputs, float64 for floating-point inputs.
#[derive(Default)]
pub struct SumFunction {
    state: NumericAccumulator,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "SUM"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "SUM",
            FunctionType::Aggregate,
            "Returns the sum of all non-NULL values in the specified column",
            FunctionSignature::new(
                FunctionDataType::Any, // can return either int64 or float64
                vec![FunctionDataType::Any],
                1,
                1,
            )
            .returns_arguments(&[0]),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        // Handle NULL values - SUM ignores NULLs
        if value.is_null() {
            return;
        }

        // Handle DISTINCT case
        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return; // Already seen this value
            }
        }

        self.state.accumulate(value);
    }

    fn result(&self) -> Value {
        self.try_result().unwrap_or_else(|_| Value::null_unknown())
    }

    fn try_result(&self) -> Result<Value> {
        self.state.sum_result()
    }

    fn reset(&mut self) {
        self.state.reset();
        self.distinct_tracker = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_integers() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(1), false);
        sum.accumulate(&Value::Integer(2), false);
        sum.accumulate(&Value::Integer(3), false);
        assert_eq!(sum.result(), Value::Integer(6));
    }

    #[test]
    fn test_sum_floats() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Float(1.5), false);
        sum.accumulate(&Value::Float(2.5), false);
        sum.accumulate(&Value::Float(3.0), false);
        assert_eq!(sum.result(), Value::Float(7.0));
    }

    #[test]
    fn test_sum_mixed() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(1), false);
        sum.accumulate(&Value::Float(2.5), false);
        sum.accumulate(&Value::Integer(3), false);
        assert_eq!(sum.result(), Value::Float(6.5));
    }

    #[test]
    fn test_sum_ignores_null() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(1), false);
        sum.accumulate(&Value::null_unknown(), false);
        sum.accumulate(&Value::Integer(3), false);
        assert_eq!(sum.result(), Value::Integer(4));
    }

    #[test]
    fn test_sum_distinct() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(1), true);
        sum.accumulate(&Value::Integer(1), true); // duplicate
        sum.accumulate(&Value::Integer(2), true);
        sum.accumulate(&Value::Integer(2), true); // duplicate
        assert_eq!(sum.result(), Value::Integer(3)); // 1 + 2
    }

    #[test]
    fn test_sum_empty() {
        let sum = SumFunction::default();
        assert!(sum.result().is_null());
    }

    #[test]
    fn test_sum_reset() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(1), false);
        sum.accumulate(&Value::Integer(2), false);
        sum.reset();
        assert!(sum.result().is_null());
    }

    #[test]
    fn test_sum_negative() {
        let mut sum = SumFunction::default();
        sum.accumulate(&Value::Integer(-5), false);
        sum.accumulate(&Value::Integer(10), false);
        sum.accumulate(&Value::Integer(-3), false);
        assert_eq!(sum.result(), Value::Integer(2));
    }

    #[test]
    fn sum_preserves_exact_integer_and_decimal_domains() {
        let mut integer = SumFunction::default();
        integer.accumulate(&Value::Integer(9_007_199_254_740_992), false);
        integer.accumulate(&Value::Integer(1), false);
        assert_eq!(
            integer.try_result().unwrap(),
            Value::Integer(9_007_199_254_740_993)
        );

        let mut decimal = SumFunction::default();
        decimal.accumulate(&Value::try_decimal(120, 3, 2).unwrap(), false);
        decimal.accumulate(&Value::try_decimal(23, 2, 1).unwrap(), false);
        assert_eq!(
            decimal.try_result().unwrap().as_decimal_parts(),
            Some((350, 3, 2))
        );
    }
}
