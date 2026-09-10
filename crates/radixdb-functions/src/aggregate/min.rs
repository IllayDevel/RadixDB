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

//! MIN aggregate function

use radixdb_core::Value;

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

/// MIN aggregate function
///
/// Returns the minimum value of all non-NULL values in the specified column.
/// Works with any comparable type (numbers, strings, timestamps, etc.)
#[derive(Default)]
pub struct MinFunction {
    min_value: Option<Value>,
}

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "MIN"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "MIN",
            FunctionType::Aggregate,
            "Returns the minimum value of all non-NULL values in the specified column",
            FunctionSignature::new(
                FunctionDataType::Any, // MIN returns the same type as input
                vec![FunctionDataType::Any],
                1,
                1,
            )
            .returns_arguments(&[0]),
        )
    }

    fn accumulate(&mut self, value: &Value, _distinct: bool) {
        // Handle NULL values - MIN ignores NULLs
        if value.is_null() {
            return;
        }

        // If this is the first non-NULL value, set it as minimum
        if self.min_value.is_none() {
            self.min_value = Some(value.clone());
            return;
        }

        // Compare and update minimum
        if is_less_than(value, self.min_value.as_ref().unwrap()) {
            self.min_value = Some(value.clone());
        }
    }

    fn result(&self) -> Value {
        self.min_value.clone().unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.min_value = None;
    }
}

/// Compare two values, returns true if a < b
fn is_less_than(a: &Value, b: &Value) -> bool {
    matches!(a.compare(b), Ok(std::cmp::Ordering::Less))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_integers() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Integer(5), false);
        min.accumulate(&Value::Integer(2), false);
        min.accumulate(&Value::Integer(8), false);
        assert_eq!(min.result(), Value::Integer(2));
    }

    #[test]
    fn test_min_floats() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Float(5.5), false);
        min.accumulate(&Value::Float(2.2), false);
        min.accumulate(&Value::Float(8.8), false);
        assert_eq!(min.result(), Value::Float(2.2));
    }

    #[test]
    fn test_min_strings() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::text("banana"), false);
        min.accumulate(&Value::text("apple"), false);
        min.accumulate(&Value::text("cherry"), false);
        assert_eq!(min.result(), Value::text("apple"));
    }

    #[test]
    fn test_min_ignores_null() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Integer(5), false);
        min.accumulate(&Value::null_unknown(), false);
        min.accumulate(&Value::Integer(2), false);
        assert_eq!(min.result(), Value::Integer(2));
    }

    #[test]
    fn test_min_empty() {
        let min = MinFunction::default();
        assert!(min.result().is_null());
    }

    #[test]
    fn test_min_reset() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Integer(5), false);
        min.accumulate(&Value::Integer(2), false);
        min.reset();
        assert!(min.result().is_null());
    }

    #[test]
    fn test_min_single_value() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Integer(42), false);
        assert_eq!(min.result(), Value::Integer(42));
    }

    #[test]
    fn test_min_negative() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Integer(-5), false);
        min.accumulate(&Value::Integer(10), false);
        min.accumulate(&Value::Integer(-10), false);
        assert_eq!(min.result(), Value::Integer(-10));
    }

    #[test]
    fn test_min_booleans() {
        let mut min = MinFunction::default();
        min.accumulate(&Value::Boolean(true), false);
        min.accumulate(&Value::Boolean(false), false);
        min.accumulate(&Value::Boolean(true), false);
        assert_eq!(min.result(), Value::Boolean(false));
    }

    #[test]
    fn min_uses_canonical_decimal_and_mixed_numeric_ordering() {
        let mut decimal_min = MinFunction::default();
        decimal_min.accumulate(&Value::decimal(200, 3, 2), false);
        decimal_min.accumulate(&Value::decimal(10, 2, 1), false);
        decimal_min.accumulate(&Value::decimal(100, 3, 2), false);
        assert_eq!(decimal_min.result().as_decimal_parts(), Some((10, 2, 1)));

        let exact = 1_i64 << 53;
        let mut mixed_min = MinFunction::default();
        mixed_min.accumulate(&Value::Integer(exact + 1), false);
        mixed_min.accumulate(&Value::Float(exact as f64), false);
        assert_eq!(mixed_min.result(), Value::Float(exact as f64));
    }
}
