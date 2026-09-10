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

//! AVG aggregate function

use radixdb_core::{Result, Value};

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

use super::{numeric::NumericAccumulator, DistinctTracker};

/// AVG aggregate function
///
/// Returns the average of all non-NULL values in the specified column.
/// Always returns a float64.
#[derive(Default)]
pub struct AvgFunction {
    state: NumericAccumulator,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str {
        "AVG"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "AVG",
            FunctionType::Aggregate,
            "Returns the average of all non-NULL values in the specified column",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        // Handle NULL values - AVG ignores NULLs
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
        self.state.average_result()
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
    fn test_avg_integers() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(1), false);
        avg.accumulate(&Value::Integer(2), false);
        avg.accumulate(&Value::Integer(3), false);
        assert_eq!(avg.result(), Value::Float(2.0));
    }

    #[test]
    fn test_avg_floats() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Float(1.0), false);
        avg.accumulate(&Value::Float(2.0), false);
        avg.accumulate(&Value::Float(3.0), false);
        assert_eq!(avg.result(), Value::Float(2.0));
    }

    #[test]
    fn test_avg_mixed() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(1), false);
        avg.accumulate(&Value::Float(2.0), false);
        avg.accumulate(&Value::Integer(3), false);
        assert_eq!(avg.result(), Value::Float(2.0));
    }

    #[test]
    fn test_avg_ignores_null() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(1), false);
        avg.accumulate(&Value::null_unknown(), false);
        avg.accumulate(&Value::Integer(3), false);
        assert_eq!(avg.result(), Value::Float(2.0)); // (1 + 3) / 2
    }

    #[test]
    fn test_avg_distinct() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(1), true);
        avg.accumulate(&Value::Integer(1), true); // duplicate
        avg.accumulate(&Value::Integer(3), true);
        avg.accumulate(&Value::Integer(3), true); // duplicate
        assert_eq!(avg.result(), Value::Float(2.0)); // (1 + 3) / 2
    }

    #[test]
    fn test_avg_empty() {
        let avg = AvgFunction::default();
        assert!(avg.result().is_null());
    }

    #[test]
    fn test_avg_reset() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(1), false);
        avg.accumulate(&Value::Integer(2), false);
        avg.reset();
        assert!(avg.result().is_null());
    }

    #[test]
    fn test_avg_single_value() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::Integer(42), false);
        assert_eq!(avg.result(), Value::Float(42.0));
    }

    #[test]
    fn avg_accepts_decimal_values_in_the_shared_numeric_state() {
        let mut avg = AvgFunction::default();
        avg.accumulate(&Value::try_decimal(10, 2, 1).unwrap(), false);
        avg.accumulate(&Value::try_decimal(20, 2, 1).unwrap(), false);
        assert_eq!(avg.try_result().unwrap(), Value::Float(1.5));
    }
}
