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

//! Aggregate Functions
//!
//! This module provides aggregate functions for SQL queries:
//!
//! - [`CountFunction`] - COUNT(*) and COUNT(column)
//! - [`SumFunction`] - SUM(column)
//! - [`AvgFunction`] - AVG(column)
//! - [`MinFunction`] - MIN(column)
//! - [`MaxFunction`] - MAX(column)
//! - [`FirstFunction`] - FIRST(column)
//! - [`LastFunction`] - LAST(column)
//! - [`StringAggFunction`] - STRING_AGG(column, separator)
//! - [`GroupConcatFunction`] - GROUP_CONCAT(column, separator)
//! - [`ArrayAggFunction`] - ARRAY_AGG(column)
//! - [`StddevPopFunction`] - STDDEV_POP(column)
//! - [`StddevFunction`] - STDDEV(column)
//! - [`StddevSampFunction`] - STDDEV_SAMP(column)
//! - [`VarPopFunction`] - VAR_POP(column)
//! - [`VarianceFunction`] - VARIANCE(column)
//! - [`VarSampFunction`] - VAR_SAMP(column)
//! - [`MedianFunction`] - MEDIAN(column)

mod array_agg;
mod avg;
pub mod compiled;
mod count;
mod first;
mod last;
mod max;
mod min;
#[doc(hidden)]
pub mod numeric;
mod statistics;
mod string_agg;
mod sum;

pub use array_agg::ArrayAggFunction;
pub use avg::AvgFunction;
pub use compiled::CompiledAggregate;
pub use count::CountFunction;
pub use first::FirstFunction;
pub use last::LastFunction;
pub use max::MaxFunction;
pub use min::MinFunction;
pub use statistics::{
    MedianFunction, StddevFunction, StddevPopFunction, StddevSampFunction, VarPopFunction,
    VarSampFunction, VarianceFunction,
};
pub use string_agg::{GroupConcatFunction, StringAggFunction};
pub use sum::SumFunction;

use radixdb_core::{Value, ValueSet};

use crate::AggregateOrderBySpec;
use std::cmp::Ordering;

/// Compare aggregate ORDER BY keys using the complete resolved sort contract.
pub(crate) fn compare_sort_keys(
    a: &[Value],
    b: &[Value],
    specs: &[AggregateOrderBySpec],
) -> Ordering {
    for (i, (key_a, key_b)) in a.iter().zip(b.iter()).enumerate() {
        let spec = specs
            .get(i)
            .copied()
            .unwrap_or_else(|| AggregateOrderBySpec::new(true, None));
        let cmp = compare_values_for_sort(key_a, key_b, spec);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Compare two values without coupling NULL placement to value direction.
fn compare_values_for_sort(a: &Value, b: &Value, spec: AggregateOrderBySpec) -> Ordering {
    match (a, b) {
        (Value::Null(_), Value::Null(_)) => Ordering::Equal,
        (Value::Null(_), _) => {
            if spec.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null(_)) => {
            if spec.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let cmp = a.cmp(b);
            if spec.ascending {
                cmp
            } else {
                cmp.reverse()
            }
        }
    }
}

/// Helper struct for tracking distinct values
///
/// Uses ValueSet directly instead of converting to strings,
/// avoiding allocation overhead for each value.
#[derive(Default, Debug)]
pub struct DistinctTracker {
    seen: ValueSet,
}

impl DistinctTracker {
    /// Check if a value has been seen before (returns true if new)
    /// Note: Caller must ensure value is not NULL before calling
    #[inline]
    pub fn check_and_add(&mut self, value: &Value) -> bool {
        self.seen.insert(value.clone())
    }

    /// Check if a value has been seen before, with null handling (returns true if new)
    #[inline]
    pub fn check_and_add_with_null_check(&mut self, value: &Value) -> bool {
        if value.is_null() {
            return false;
        }
        self.seen.insert(value.clone())
    }

    /// Get the count of distinct values
    #[inline]
    pub fn count(&self) -> usize {
        self.seen.len()
    }

    /// Reset the tracker
    pub fn reset(&mut self) {
        self.seen.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distinct_tracker() {
        let mut tracker = DistinctTracker::default();

        assert!(tracker.check_and_add(&Value::Integer(1)));
        assert!(!tracker.check_and_add(&Value::Integer(1))); // duplicate
        assert!(tracker.check_and_add(&Value::Integer(2)));
        assert!(tracker.check_and_add(&Value::text("hello")));

        assert_eq!(tracker.count(), 3);

        tracker.reset();
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn test_null_handling() {
        let mut tracker = DistinctTracker::default();

        // Using the with_null_check variant for null handling
        assert!(!tracker.check_and_add_with_null_check(&Value::null_unknown())); // NULL returns false
        assert!(!tracker.check_and_add_with_null_check(&Value::null_unknown())); // NULL again
        assert_eq!(tracker.count(), 0); // NULLs not counted
    }

    #[test]
    fn test_ordered_aggregate_sort_contract_is_exact_and_null_independent() {
        let null = Value::null_unknown();
        let rounded = Value::Float(9_007_199_254_740_992.0);
        let exact_neighbor = Value::Integer(9_007_199_254_740_993);

        let desc_nulls_last = AggregateOrderBySpec::new(false, Some(false));
        assert_eq!(
            compare_sort_keys(
                std::slice::from_ref(&null),
                std::slice::from_ref(&exact_neighbor),
                &[desc_nulls_last],
            ),
            Ordering::Greater,
            "explicit NULLS LAST must not reverse with DESC",
        );
        assert_eq!(
            compare_sort_keys(
                std::slice::from_ref(&exact_neighbor),
                std::slice::from_ref(&rounded),
                &[desc_nulls_last],
            ),
            Ordering::Less,
            "the exact integer neighbour sorts before rounded Float in DESC",
        );

        let desc_default = AggregateOrderBySpec::new(false, None);
        assert!(desc_default.nulls_first);
        assert_eq!(
            compare_sort_keys(&[null], &[rounded], &[desc_default]),
            Ordering::Less,
        );
    }
}
