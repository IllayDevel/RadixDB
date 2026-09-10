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

//! ARRAY_AGG aggregate function

use radixdb_core::{DataType, Error, Result, Value};

use crate::{
    AggregateFunction, AggregateOrderBySpec, FunctionDataType, FunctionInfo, FunctionSignature,
    FunctionType,
};

use super::{compare_sort_keys, DistinctTracker};

/// Entry for ordered aggregation - stores value with its sort keys
#[derive(Clone)]
struct OrderedEntry {
    value: Value,
    sort_keys: Vec<Value>,
}

/// ARRAY_AGG aggregate function
///
/// Collects all values into a JSON array.
/// Similar to PostgreSQL's ARRAY_AGG.
///
/// Usage:
///   ARRAY_AGG(column)
///   ARRAY_AGG(DISTINCT column)
///   ARRAY_AGG(column ORDER BY expr)
#[derive(Default)]
pub struct ArrayAggFunction {
    /// Values collected (used when no ORDER BY)
    values: Vec<Value>,
    /// Values with sort keys (used when ORDER BY is specified)
    ordered_entries: Vec<OrderedEntry>,
    /// Complete resolved ORDER BY contract.
    order_specs: Vec<AggregateOrderBySpec>,
    /// Whether ORDER BY is active
    has_order_by: bool,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for ArrayAggFunction {
    fn name(&self) -> &str {
        "ARRAY_AGG"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "ARRAY_AGG",
            FunctionType::Aggregate,
            "Collects all values into a JSON array",
            FunctionSignature::new(FunctionDataType::Json, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn configure(&mut self, _options: &[Value]) {
        // No configuration options
    }

    fn set_order_by_specs(&mut self, specs: Vec<AggregateOrderBySpec>) {
        self.order_specs = specs;
        self.has_order_by = true;
    }

    fn set_order_by(&mut self, directions: Vec<bool>) {
        self.set_order_by_specs(
            directions
                .into_iter()
                .map(|ascending| AggregateOrderBySpec::new(ascending, None))
                .collect(),
        );
    }

    fn supports_order_by(&self) -> bool {
        true
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        // Handle DISTINCT case
        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return; // Already seen this value
            }
        }

        self.values.push(value.clone());
    }

    fn accumulate_with_sort_key(&mut self, value: &Value, sort_keys: Vec<Value>, distinct: bool) {
        // Handle DISTINCT case
        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return; // Already seen this value
            }
        }

        self.ordered_entries.push(OrderedEntry {
            value: value.clone(),
            sort_keys,
        });
    }

    fn result(&self) -> Value {
        self.try_result()
            .unwrap_or_else(|_| Value::Null(DataType::Json))
    }

    fn try_result(&self) -> Result<Value> {
        let values_to_output = self.values_to_output();
        if values_to_output.is_empty() {
            return Ok(Value::Null(DataType::Json));
        }
        let json_values = values_to_output
            .into_iter()
            .map(value_to_json)
            .collect::<Result<Vec<_>>>()?;
        let encoded = serde_json::to_string(&json_values)
            .map_err(|error| Error::invalid_argument(format!("ARRAY_AGG JSON error: {error}")))?;
        Value::try_json(encoded)
    }

    fn reset(&mut self) {
        self.values.clear();
        self.ordered_entries.clear();
        self.distinct_tracker = None;
        // Note: order_specs and has_order_by are kept as they're configuration
    }
}

impl ArrayAggFunction {
    fn values_to_output(&self) -> Vec<&Value> {
        if self.has_order_by && !self.ordered_entries.is_empty() {
            let mut entries: Vec<&OrderedEntry> = self.ordered_entries.iter().collect();
            entries
                .sort_by(|a, b| compare_sort_keys(&a.sort_keys, &b.sort_keys, &self.order_specs));
            entries.into_iter().map(|entry| &entry.value).collect()
        } else if !self.values.is_empty() {
            self.values.iter().collect()
        } else {
            self.ordered_entries
                .iter()
                .map(|entry| &entry.value)
                .collect()
        }
    }
}

fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    match value {
        Value::Null(_) => Ok(serde_json::Value::Null),
        Value::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        Value::Integer(value) => Ok(serde_json::Value::Number((*value).into())),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| Error::invalid_argument("ARRAY_AGG cannot encode non-finite FLOAT")),
        Value::Text(value) => Ok(serde_json::Value::String(value.to_string())),
        Value::Timestamp(value) => Ok(serde_json::Value::String(value.to_rfc3339())),
        Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
            serde_json::from_slice(&data[1..]).map_err(|error| {
                Error::invalid_argument(format!("ARRAY_AGG contains malformed JSON: {error}"))
            })
        }
        value if value.as_decimal_parts().is_some() => serde_json::from_str(&value.to_string())
            .map_err(|error| Error::invalid_argument(format!("invalid DECIMAL JSON: {error}"))),
        value => Ok(serde_json::Value::String(value.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_agg_basic() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::Integer(1), false);
        agg.accumulate(&Value::Integer(2), false);
        agg.accumulate(&Value::Integer(3), false);
        assert_eq!(agg.result(), Value::json("[1,2,3]"));
    }

    #[test]
    fn test_array_agg_strings() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::text("a"), false);
        agg.accumulate(&Value::text("b"), false);
        agg.accumulate(&Value::text("c"), false);
        assert_eq!(agg.result(), Value::json("[\"a\",\"b\",\"c\"]"));
    }

    #[test]
    fn test_array_agg_preserves_null() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::Integer(1), false);
        agg.accumulate(&Value::null_unknown(), false);
        agg.accumulate(&Value::Integer(3), false);
        assert_eq!(agg.result(), Value::json("[1,null,3]"));
    }

    #[test]
    fn test_array_agg_distinct() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::Integer(1), true);
        agg.accumulate(&Value::Integer(2), true);
        agg.accumulate(&Value::Integer(1), true); // duplicate
        agg.accumulate(&Value::Integer(3), true);
        assert_eq!(agg.result(), Value::json("[1,2,3]"));
    }

    #[test]
    fn test_array_agg_empty() {
        let agg = ArrayAggFunction::default();
        assert!(agg.result().is_null());
    }

    #[test]
    fn test_array_agg_mixed_types() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::text("str"), false);
        agg.accumulate(&Value::Integer(42), false);
        agg.accumulate(&Value::Float(3.5), false);
        agg.accumulate(&Value::Boolean(true), false);
        assert_eq!(agg.result(), Value::json("[\"str\",42,3.5,true]"));
    }

    #[test]
    fn test_array_agg_reset() {
        let mut agg = ArrayAggFunction::default();
        agg.accumulate(&Value::Integer(1), false);
        agg.accumulate(&Value::Integer(2), false);
        agg.reset();
        assert!(agg.result().is_null());
    }

    #[test]
    fn test_array_agg_preserves_explicit_null_ordering() {
        let mut agg = ArrayAggFunction::default();
        agg.set_order_by_specs(vec![AggregateOrderBySpec::new(false, Some(false))]);
        agg.accumulate_with_sort_key(&Value::text("null-key"), vec![Value::null_unknown()], false);
        agg.accumulate_with_sort_key(&Value::text("low"), vec![Value::Integer(1)], false);
        agg.accumulate_with_sort_key(&Value::text("high"), vec![Value::Integer(2)], false);

        assert_eq!(agg.result(), Value::json("[\"high\",\"low\",\"null-key\"]"));
    }

    #[test]
    fn array_agg_returns_typed_json_and_rejects_non_json_numbers() {
        let mut valid = ArrayAggFunction::default();
        valid.accumulate(&Value::Integer(1), false);
        let result = valid.try_result().unwrap();
        assert_eq!(result.data_type(), DataType::Json);
        assert_eq!(result.as_json(), Some("[1]"));

        let mut invalid = ArrayAggFunction::default();
        invalid.accumulate(&Value::Float(f64::NAN), false);
        assert!(invalid.try_result().is_err());
    }
}
