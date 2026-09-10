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

//! Compiled aggregate functions for zero-dispatch hot path execution
//!
//! This module provides `CompiledAggregate`, an enum-based specialization of aggregate
//! functions that eliminates virtual dispatch overhead from `Box<dyn AggregateFunction>`.
//!
//! # Performance
//!
//! Traditional aggregate functions use trait objects:
//! ```text
//! for row in rows {
//!     func.accumulate(value, distinct);  // vtable lookup per row
//! }
//! ```
//!
//! With `CompiledAggregate`, the dispatch becomes a direct enum match:
//! ```text
//! for row in rows {
//!     compiled_agg.accumulate(value);  // inline match, no vtable
//! }
//! ```
//!
//! Expected speedup: 2-5x for aggregation-heavy queries.

use radixdb_core::{Result, Value};

use crate::AggregateFunction;

use super::{numeric::NumericAccumulator, DistinctTracker};

/// Shared exact numeric state used by compiled SUM/AVG.
pub type SumState = NumericAccumulator;

/// Compiled aggregate function - enum-based specialization for zero virtual dispatch
///
/// This enum provides specialized implementations for the most common aggregate
/// functions (COUNT, SUM, AVG, MIN, MAX), with a `Dynamic` fallback for complex
/// or rare aggregates (STRING_AGG, ARRAY_AGG, MEDIAN, etc.).
pub enum CompiledAggregate {
    /// COUNT(*) - counts all rows
    CountStar { count: i64 },

    /// COUNT(column) - counts non-NULL values
    Count { count: i64 },

    /// COUNT(DISTINCT column) - counts distinct non-NULL values
    CountDistinct { distinct_tracker: DistinctTracker },

    /// SUM(column) - sums numeric values
    Sum { state: SumState },

    /// SUM(DISTINCT column) - sums distinct numeric values
    SumDistinct {
        state: SumState,
        distinct_tracker: DistinctTracker,
    },

    /// AVG(column) - average of numeric values
    Avg { state: NumericAccumulator },

    /// AVG(DISTINCT column) - average of distinct numeric values
    AvgDistinct {
        state: NumericAccumulator,
        distinct_tracker: DistinctTracker,
    },

    /// MIN(column) - minimum value (type-generic using Value comparison)
    Min { min_value: Option<Value> },

    /// MAX(column) - maximum value (type-generic using Value comparison)
    Max { max_value: Option<Value> },

    /// Fallback to dynamic dispatch for complex aggregates
    Dynamic(Box<dyn AggregateFunction>),
}

impl std::fmt::Debug for CompiledAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompiledAggregate::CountStar { count } => {
                f.debug_struct("CountStar").field("count", count).finish()
            }
            CompiledAggregate::Count { count } => {
                f.debug_struct("Count").field("count", count).finish()
            }
            CompiledAggregate::CountDistinct { distinct_tracker } => f
                .debug_struct("CountDistinct")
                .field("distinct_tracker", distinct_tracker)
                .finish(),
            CompiledAggregate::Sum { state } => {
                f.debug_struct("Sum").field("state", state).finish()
            }
            CompiledAggregate::SumDistinct {
                state,
                distinct_tracker,
            } => f
                .debug_struct("SumDistinct")
                .field("state", state)
                .field("distinct_tracker", distinct_tracker)
                .finish(),
            CompiledAggregate::Avg { state } => {
                f.debug_struct("Avg").field("state", state).finish()
            }
            CompiledAggregate::AvgDistinct {
                state,
                distinct_tracker,
            } => f
                .debug_struct("AvgDistinct")
                .field("state", state)
                .field("distinct_tracker", distinct_tracker)
                .finish(),
            CompiledAggregate::Min { min_value } => {
                f.debug_struct("Min").field("min_value", min_value).finish()
            }
            CompiledAggregate::Max { max_value } => {
                f.debug_struct("Max").field("max_value", max_value).finish()
            }
            CompiledAggregate::Dynamic(func) => {
                f.debug_tuple("Dynamic").field(&func.name()).finish()
            }
        }
    }
}

impl CompiledAggregate {
    /// Create a compiled COUNT(*) aggregate
    pub fn count_star() -> Self {
        CompiledAggregate::CountStar { count: 0 }
    }

    /// Create a compiled COUNT(column) aggregate
    pub fn count(distinct: bool) -> Self {
        if distinct {
            CompiledAggregate::CountDistinct {
                distinct_tracker: DistinctTracker::default(),
            }
        } else {
            CompiledAggregate::Count { count: 0 }
        }
    }

    /// Create a compiled SUM aggregate
    pub fn sum(distinct: bool) -> Self {
        if distinct {
            CompiledAggregate::SumDistinct {
                state: NumericAccumulator::default(),
                distinct_tracker: DistinctTracker::default(),
            }
        } else {
            CompiledAggregate::Sum {
                state: NumericAccumulator::default(),
            }
        }
    }

    /// Create a compiled AVG aggregate
    pub fn avg(distinct: bool) -> Self {
        if distinct {
            CompiledAggregate::AvgDistinct {
                state: NumericAccumulator::default(),
                distinct_tracker: DistinctTracker::default(),
            }
        } else {
            CompiledAggregate::Avg {
                state: NumericAccumulator::default(),
            }
        }
    }

    /// Create a compiled MIN aggregate (generic)
    pub fn min() -> Self {
        CompiledAggregate::Min { min_value: None }
    }

    /// Create a compiled MAX aggregate (generic)
    pub fn max() -> Self {
        CompiledAggregate::Max { max_value: None }
    }

    /// Create from a dynamic aggregate function (fallback)
    pub fn dynamic(func: Box<dyn AggregateFunction>) -> Self {
        CompiledAggregate::Dynamic(func)
    }

    /// Compile from a function name and configuration
    ///
    /// Returns a compiled aggregate for common functions, or wraps
    /// the provided dynamic function for complex/rare aggregates.
    pub fn compile(
        name: &str,
        is_count_star: bool,
        distinct: bool,
        dynamic_fallback: Option<Box<dyn AggregateFunction>>,
    ) -> Option<Self> {
        if name.eq_ignore_ascii_case("COUNT") {
            if is_count_star {
                Some(CompiledAggregate::count_star())
            } else {
                Some(CompiledAggregate::count(distinct))
            }
        } else if name.eq_ignore_ascii_case("SUM") {
            Some(CompiledAggregate::sum(distinct))
        } else if name.eq_ignore_ascii_case("AVG") {
            Some(CompiledAggregate::avg(distinct))
        } else if name.eq_ignore_ascii_case("MIN") {
            Some(CompiledAggregate::min())
        } else if name.eq_ignore_ascii_case("MAX") {
            Some(CompiledAggregate::max())
        } else {
            // Complex aggregates use dynamic fallback
            dynamic_fallback.map(CompiledAggregate::Dynamic)
        }
    }

    /// Accumulate a value into the aggregate
    ///
    /// This is the hot path - all code here should be as fast as possible.
    #[inline(always)]
    pub fn accumulate(&mut self, value: &Value) {
        match self {
            // COUNT(*) - always increment
            CompiledAggregate::CountStar { count } => {
                *count += 1;
            }

            // COUNT(column) - increment for non-NULL
            CompiledAggregate::Count { count } => {
                if !value.is_null() {
                    *count += 1;
                }
            }

            // COUNT(DISTINCT column) - track distinct non-NULL values
            CompiledAggregate::CountDistinct { distinct_tracker } => {
                if !value.is_null() {
                    distinct_tracker.check_and_add(value);
                }
            }

            // SUM(column) - add numeric values
            CompiledAggregate::Sum { state } => {
                if !value.is_null() {
                    Self::accumulate_sum(state, value);
                }
            }

            // SUM(DISTINCT column) - add distinct numeric values
            CompiledAggregate::SumDistinct {
                state,
                distinct_tracker,
            } => {
                if !value.is_null() && distinct_tracker.check_and_add(value) {
                    Self::accumulate_sum(state, value);
                }
            }

            // AVG(column) - track sum and count
            CompiledAggregate::Avg { state } => {
                state.accumulate(value);
            }

            // AVG(DISTINCT column) - track sum and count for distinct values
            CompiledAggregate::AvgDistinct {
                state,
                distinct_tracker,
            } => {
                if !value.is_null() && distinct_tracker.check_and_add(value) {
                    state.accumulate(value);
                }
            }

            // MIN(column) - generic comparison
            CompiledAggregate::Min { min_value } => {
                if !value.is_null() {
                    match min_value {
                        None => *min_value = Some(value.clone()),
                        Some(current) => {
                            if Self::is_less_than(value, current) {
                                *min_value = Some(value.clone());
                            }
                        }
                    }
                }
            }

            // MAX(column) - generic comparison
            CompiledAggregate::Max { max_value } => {
                if !value.is_null() {
                    match max_value {
                        None => *max_value = Some(value.clone()),
                        Some(current) => {
                            if Self::is_greater_than(value, current) {
                                *max_value = Some(value.clone());
                            }
                        }
                    }
                }
            }

            // Dynamic fallback
            CompiledAggregate::Dynamic(func) => {
                func.accumulate(value, false);
            }
        }
    }

    /// Accumulate with DISTINCT flag (for dynamic fallback compatibility)
    #[inline(always)]
    pub fn accumulate_with_distinct(&mut self, value: &Value, distinct: bool) {
        if let CompiledAggregate::Dynamic(func) = self {
            func.accumulate(value, distinct);
        } else {
            // For compiled variants, DISTINCT is handled by the variant type
            self.accumulate(value);
        }
    }

    /// Get the result of the aggregation
    #[inline]
    pub fn result(&self) -> Value {
        match self {
            CompiledAggregate::CountStar { count } => Value::Integer(*count),
            CompiledAggregate::Count { count } => Value::Integer(*count),
            CompiledAggregate::CountDistinct { distinct_tracker } => {
                Value::Integer(distinct_tracker.count() as i64)
            }

            CompiledAggregate::Sum { state } | CompiledAggregate::SumDistinct { state, .. } => {
                state.sum_result().unwrap_or_else(|_| Value::null_unknown())
            }

            CompiledAggregate::Avg { state } | CompiledAggregate::AvgDistinct { state, .. } => {
                state
                    .average_result()
                    .unwrap_or_else(|_| Value::null_unknown())
            }

            CompiledAggregate::Min { min_value } => {
                min_value.clone().unwrap_or_else(Value::null_unknown)
            }
            CompiledAggregate::Max { max_value } => {
                max_value.clone().unwrap_or_else(Value::null_unknown)
            }

            CompiledAggregate::Dynamic(func) => func.result(),
        }
    }

    /// Fallible result path for dynamic aggregates with bounded spill state.
    #[inline]
    pub fn try_result(&self) -> Result<Value> {
        match self {
            CompiledAggregate::Dynamic(function) => function.try_result(),
            CompiledAggregate::Sum { state } | CompiledAggregate::SumDistinct { state, .. } => {
                state.sum_result()
            }
            CompiledAggregate::Avg { state } | CompiledAggregate::AvgDistinct { state, .. } => {
                state.average_result()
            }
            _ => Ok(self.result()),
        }
    }

    /// Reset the aggregate state
    pub fn reset(&mut self) {
        match self {
            CompiledAggregate::CountStar { count } => *count = 0,
            CompiledAggregate::Count { count } => *count = 0,
            CompiledAggregate::CountDistinct { distinct_tracker } => distinct_tracker.reset(),

            CompiledAggregate::Sum { state } => state.reset(),
            CompiledAggregate::SumDistinct {
                state,
                distinct_tracker,
            } => {
                state.reset();
                distinct_tracker.reset();
            }

            CompiledAggregate::Avg { state } => state.reset(),
            CompiledAggregate::AvgDistinct {
                state,
                distinct_tracker,
            } => {
                state.reset();
                distinct_tracker.reset();
            }

            CompiledAggregate::Min { min_value } => *min_value = None,
            CompiledAggregate::Max { max_value } => *max_value = None,

            CompiledAggregate::Dynamic(func) => func.reset(),
        }
    }

    /// Helper: accumulate into SumState
    #[inline(always)]
    fn accumulate_sum(state: &mut SumState, value: &Value) {
        state.accumulate(value);
    }

    /// Helper: compare values (a < b)
    #[inline(always)]
    fn is_less_than(a: &Value, b: &Value) -> bool {
        matches!(a.compare(b), Ok(std::cmp::Ordering::Less))
    }

    /// Helper: compare values (a > b)
    #[inline(always)]
    fn is_greater_than(a: &Value, b: &Value) -> bool {
        matches!(a.compare(b), Ok(std::cmp::Ordering::Greater))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_star() {
        let mut agg = CompiledAggregate::count_star();
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::null_unknown());
        agg.accumulate(&Value::Integer(3));
        assert_eq!(agg.result(), Value::Integer(3)); // Counts all rows including NULL
    }

    #[test]
    fn test_count() {
        let mut agg = CompiledAggregate::count(false);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::null_unknown());
        agg.accumulate(&Value::Integer(3));
        assert_eq!(agg.result(), Value::Integer(2)); // Skips NULL
    }

    #[test]
    fn test_count_distinct() {
        let mut agg = CompiledAggregate::count(true);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Integer(1)); // duplicate
        agg.accumulate(&Value::Integer(2));
        agg.accumulate(&Value::null_unknown()); // NULL ignored
        assert_eq!(agg.result(), Value::Integer(2));
    }

    #[test]
    fn test_sum_integers() {
        let mut agg = CompiledAggregate::sum(false);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Integer(2));
        agg.accumulate(&Value::Integer(3));
        assert_eq!(agg.result(), Value::Integer(6));
    }

    #[test]
    fn test_sum_floats() {
        let mut agg = CompiledAggregate::sum(false);
        agg.accumulate(&Value::Float(1.5));
        agg.accumulate(&Value::Float(2.5));
        assert_eq!(agg.result(), Value::Float(4.0));
    }

    #[test]
    fn test_sum_mixed() {
        let mut agg = CompiledAggregate::sum(false);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Float(2.5));
        assert_eq!(agg.result(), Value::Float(3.5));
    }

    #[test]
    fn test_sum_distinct() {
        let mut agg = CompiledAggregate::sum(true);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Integer(1)); // duplicate
        agg.accumulate(&Value::Integer(2));
        assert_eq!(agg.result(), Value::Integer(3)); // 1 + 2
    }

    #[test]
    fn test_avg() {
        let mut agg = CompiledAggregate::avg(false);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Integer(2));
        agg.accumulate(&Value::Integer(3));
        assert_eq!(agg.result(), Value::Float(2.0));
    }

    #[test]
    fn test_avg_distinct() {
        let mut agg = CompiledAggregate::avg(true);
        agg.accumulate(&Value::Integer(1));
        agg.accumulate(&Value::Integer(1)); // duplicate
        agg.accumulate(&Value::Integer(3));
        assert_eq!(agg.result(), Value::Float(2.0)); // (1 + 3) / 2
    }

    #[test]
    fn compiled_numeric_aggregates_share_exact_dynamic_state() {
        let mut sum = CompiledAggregate::sum(false);
        sum.accumulate(&Value::Integer(9_007_199_254_740_992));
        sum.accumulate(&Value::Integer(1));
        assert_eq!(
            sum.try_result().unwrap(),
            Value::Integer(9_007_199_254_740_993)
        );

        let mut avg = CompiledAggregate::avg(false);
        avg.accumulate(&Value::try_decimal(10, 2, 1).unwrap());
        avg.accumulate(&Value::try_decimal(20, 2, 1).unwrap());
        assert_eq!(avg.try_result().unwrap(), Value::Float(1.5));
    }

    #[test]
    fn test_min_integers() {
        let mut agg = CompiledAggregate::min();
        agg.accumulate(&Value::Integer(5));
        agg.accumulate(&Value::Integer(2));
        agg.accumulate(&Value::Integer(8));
        assert_eq!(agg.result(), Value::Integer(2));
    }

    #[test]
    fn test_max_integers() {
        let mut agg = CompiledAggregate::max();
        agg.accumulate(&Value::Integer(5));
        agg.accumulate(&Value::Integer(2));
        agg.accumulate(&Value::Integer(8));
        assert_eq!(agg.result(), Value::Integer(8));
    }

    #[test]
    fn test_min_strings() {
        let mut agg = CompiledAggregate::min();
        agg.accumulate(&Value::text("banana"));
        agg.accumulate(&Value::text("apple"));
        agg.accumulate(&Value::text("cherry"));
        assert_eq!(agg.result(), Value::text("apple"));
    }

    #[test]
    fn test_max_strings() {
        let mut agg = CompiledAggregate::max();
        agg.accumulate(&Value::text("banana"));
        agg.accumulate(&Value::text("apple"));
        agg.accumulate(&Value::text("cherry"));
        assert_eq!(agg.result(), Value::text("cherry"));
    }

    #[test]
    fn compiled_min_uses_canonical_decimal_and_mixed_numeric_ordering() {
        let mut decimal_min = CompiledAggregate::min();
        decimal_min.accumulate(&Value::decimal(200, 3, 2));
        decimal_min.accumulate(&Value::decimal(10, 2, 1));
        decimal_min.accumulate(&Value::decimal(100, 3, 2));
        assert_eq!(decimal_min.result().as_decimal_parts(), Some((10, 2, 1)));

        let exact = 1_i64 << 53;
        let mut mixed_min = CompiledAggregate::min();
        mixed_min.accumulate(&Value::Integer(exact + 1));
        mixed_min.accumulate(&Value::Float(exact as f64));
        assert_eq!(mixed_min.result(), Value::Float(exact as f64));
    }

    #[test]
    fn compiled_max_uses_canonical_decimal_and_mixed_numeric_ordering() {
        let mut decimal_max = CompiledAggregate::max();
        decimal_max.accumulate(&Value::decimal(10, 2, 1));
        decimal_max.accumulate(&Value::decimal(200, 3, 2));
        decimal_max.accumulate(&Value::decimal(20, 2, 1));
        assert_eq!(decimal_max.result().as_decimal_parts(), Some((200, 3, 2)));

        let exact = 1_i64 << 53;
        let mut mixed_max = CompiledAggregate::max();
        mixed_max.accumulate(&Value::Float(exact as f64));
        mixed_max.accumulate(&Value::Integer(exact + 1));
        assert_eq!(mixed_max.result(), Value::Integer(exact + 1));
    }

    #[test]
    fn test_empty_aggregates() {
        assert!(CompiledAggregate::sum(false).result().is_null());
        assert!(CompiledAggregate::avg(false).result().is_null());
        assert!(CompiledAggregate::min().result().is_null());
        assert!(CompiledAggregate::max().result().is_null());
        assert_eq!(CompiledAggregate::count(false).result(), Value::Integer(0));
        assert_eq!(CompiledAggregate::count_star().result(), Value::Integer(0));
    }

    #[test]
    fn test_reset() {
        let mut agg = CompiledAggregate::sum(false);
        agg.accumulate(&Value::Integer(10));
        agg.reset();
        assert!(agg.result().is_null());

        let mut agg = CompiledAggregate::count(false);
        agg.accumulate(&Value::Integer(10));
        agg.reset();
        assert_eq!(agg.result(), Value::Integer(0));
    }

    #[test]
    fn test_compile() {
        let agg = CompiledAggregate::compile("count", true, false, None);
        assert!(matches!(agg, Some(CompiledAggregate::CountStar { .. })));

        let agg = CompiledAggregate::compile("COUNT", false, false, None);
        assert!(matches!(agg, Some(CompiledAggregate::Count { .. })));

        let agg = CompiledAggregate::compile("sum", false, true, None);
        assert!(matches!(agg, Some(CompiledAggregate::SumDistinct { .. })));

        let agg = CompiledAggregate::compile("avg", false, false, None);
        assert!(matches!(agg, Some(CompiledAggregate::Avg { .. })));

        let agg = CompiledAggregate::compile("min", false, false, None);
        assert!(matches!(agg, Some(CompiledAggregate::Min { .. })));

        let agg = CompiledAggregate::compile("max", false, false, None);
        assert!(matches!(agg, Some(CompiledAggregate::Max { .. })));

        // Unknown without fallback returns None
        let agg = CompiledAggregate::compile("unknown", false, false, None);
        assert!(agg.is_none());
    }
}
