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

//! COUNT aggregate function

use radixdb_core::Value;

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

use super::DistinctTracker;

/// COUNT aggregate function
///
/// Returns the number of rows matching the query criteria.
/// - COUNT(*) counts all rows including NULLs
/// - COUNT(column) counts non-NULL values
/// - COUNT(DISTINCT column) counts distinct non-NULL values
#[derive(Default)]
pub struct CountFunction {
    count: i64,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "COUNT"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "COUNT",
            FunctionType::Aggregate,
            "Returns the number of rows matching the query criteria",
            FunctionSignature::new(
                FunctionDataType::Integer,
                vec![FunctionDataType::Any],
                0, // COUNT(*) has no actual argument
                1, // But can be COUNT(column)
            ),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        // Handle NULL values - COUNT ignores NULLs except for COUNT(*)
        if value.is_null() {
            return;
        }

        // Handle DISTINCT case
        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            self.distinct_tracker.as_mut().unwrap().check_and_add(value);
        } else {
            // Regular COUNT
            self.count += 1;
        }
    }

    fn result(&self) -> Value {
        if let Some(ref tracker) = self.distinct_tracker {
            Value::Integer(tracker.count() as i64)
        } else {
            Value::Integer(self.count)
        }
    }

    fn reset(&mut self) {
        self.count = 0;
        self.distinct_tracker = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_basic() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::Integer(1), false);
        count.accumulate(&Value::Integer(2), false);
        count.accumulate(&Value::Integer(3), false);
        assert_eq!(count.result(), Value::Integer(3));
    }

    #[test]
    fn test_count_star() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::Integer(1), false);
        count.accumulate(&Value::Integer(1), false);
        count.accumulate(&Value::Integer(1), false);
        assert_eq!(count.result(), Value::Integer(3));
    }

    #[test]
    fn text_asterisk_is_ordinary_distinct_data() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::text("*"), true);
        count.accumulate(&Value::text("*"), true);
        count.accumulate(&Value::text("other"), true);
        assert_eq!(count.result(), Value::Integer(2));
    }

    #[test]
    fn test_count_ignores_null() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::Integer(1), false);
        count.accumulate(&Value::null_unknown(), false);
        count.accumulate(&Value::Integer(3), false);
        assert_eq!(count.result(), Value::Integer(2));
    }

    #[test]
    fn test_count_distinct() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::Integer(1), true);
        count.accumulate(&Value::Integer(1), true); // duplicate
        count.accumulate(&Value::Integer(2), true);
        count.accumulate(&Value::Integer(2), true); // duplicate
        count.accumulate(&Value::Integer(3), true);
        assert_eq!(count.result(), Value::Integer(3));
    }

    #[test]
    fn test_count_reset() {
        let mut count = CountFunction::default();
        count.accumulate(&Value::Integer(1), false);
        count.accumulate(&Value::Integer(2), false);
        count.reset();
        assert_eq!(count.result(), Value::Integer(0));
    }

    #[test]
    fn test_count_empty() {
        let count = CountFunction::default();
        assert_eq!(count.result(), Value::Integer(0));
    }
}
