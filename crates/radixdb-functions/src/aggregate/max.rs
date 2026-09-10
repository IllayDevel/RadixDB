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

//! MAX aggregate function

use radixdb_core::Value;

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

/// MAX aggregate function
///
/// Returns the maximum value of all non-NULL values in the specified column.
/// Works with any comparable type (numbers, strings, timestamps, etc.)
#[derive(Default)]
pub struct MaxFunction {
    max_value: Option<Value>,
}

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "MAX"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "MAX",
            FunctionType::Aggregate,
            "Returns the maximum value of all non-NULL values in the specified column",
            FunctionSignature::new(
                FunctionDataType::Any, // MAX returns the same type as input
                vec![FunctionDataType::Any],
                1,
                1,
            )
            .returns_arguments(&[0]),
        )
    }

    fn accumulate(&mut self, value: &Value, _distinct: bool) {
        // Handle NULL values - MAX ignores NULLs
        if value.is_null() {
            return;
        }

        // If this is the first non-NULL value, set it as maximum
        if self.max_value.is_none() {
            self.max_value = Some(value.clone());
            return;
        }

        // Compare and update maximum
        if is_greater_than(value, self.max_value.as_ref().unwrap()) {
            self.max_value = Some(value.clone());
        }
    }

    fn result(&self) -> Value {
        self.max_value.clone().unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.max_value = None;
    }
}

/// Compare two values, returns true if a > b
fn is_greater_than(a: &Value, b: &Value) -> bool {
    matches!(a.compare(b), Ok(std::cmp::Ordering::Greater))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_integers() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Integer(5), false);
        max.accumulate(&Value::Integer(2), false);
        max.accumulate(&Value::Integer(8), false);
        assert_eq!(max.result(), Value::Integer(8));
    }

    #[test]
    fn test_max_floats() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Float(5.5), false);
        max.accumulate(&Value::Float(2.2), false);
        max.accumulate(&Value::Float(8.8), false);
        assert_eq!(max.result(), Value::Float(8.8));
    }

    #[test]
    fn test_max_strings() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::text("banana"), false);
        max.accumulate(&Value::text("apple"), false);
        max.accumulate(&Value::text("cherry"), false);
        assert_eq!(max.result(), Value::text("cherry"));
    }

    #[test]
    fn test_max_ignores_null() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Integer(5), false);
        max.accumulate(&Value::null_unknown(), false);
        max.accumulate(&Value::Integer(2), false);
        assert_eq!(max.result(), Value::Integer(5));
    }

    #[test]
    fn test_max_empty() {
        let max = MaxFunction::default();
        assert!(max.result().is_null());
    }

    #[test]
    fn test_max_reset() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Integer(5), false);
        max.accumulate(&Value::Integer(8), false);
        max.reset();
        assert!(max.result().is_null());
    }

    #[test]
    fn test_max_single_value() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Integer(42), false);
        assert_eq!(max.result(), Value::Integer(42));
    }

    #[test]
    fn test_max_negative() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Integer(-5), false);
        max.accumulate(&Value::Integer(-10), false);
        max.accumulate(&Value::Integer(-3), false);
        assert_eq!(max.result(), Value::Integer(-3));
    }

    #[test]
    fn test_max_booleans() {
        let mut max = MaxFunction::default();
        max.accumulate(&Value::Boolean(false), false);
        max.accumulate(&Value::Boolean(true), false);
        max.accumulate(&Value::Boolean(false), false);
        assert_eq!(max.result(), Value::Boolean(true));
    }

    #[test]
    fn max_uses_canonical_decimal_and_mixed_numeric_ordering() {
        let mut decimal_max = MaxFunction::default();
        decimal_max.accumulate(&Value::decimal(10, 2, 1), false);
        decimal_max.accumulate(&Value::decimal(200, 3, 2), false);
        decimal_max.accumulate(&Value::decimal(20, 2, 1), false);
        assert_eq!(decimal_max.result().as_decimal_parts(), Some((200, 3, 2)));

        let exact = 1_i64 << 53;
        let mut mixed_max = MaxFunction::default();
        mixed_max.accumulate(&Value::Float(exact as f64), false);
        mixed_max.accumulate(&Value::Integer(exact + 1), false);
        assert_eq!(mixed_max.result(), Value::Integer(exact + 1));
    }
}
