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

//! Range expression for RadixDB
//!

use std::any::Any;
use std::cmp::Ordering;

use rustc_hash::FxHashMap;

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{Operator, Result, Row, Schema, Value};

/// Range expression for custom inclusivity patterns
///
/// This is used for optimizing patterns like:
/// "column > min AND column <= max" into a single expression
#[derive(Debug, Clone)]
pub struct RangeExpr {
    /// Column name
    column: String,
    /// Minimum (lower) bound
    min_value: Value,
    /// Maximum (upper) bound
    max_value: Value,
    /// Whether to include the minimum bound (>= vs >)
    include_min: bool,
    /// Whether to include the maximum bound (<= vs <)
    include_max: bool,

    /// Pre-computed column index
    col_index: Option<usize>,

    /// Column aliases
    aliases: FxHashMap<String, String>,
    /// Original column name if using alias
    original_column: Option<String>,
}

impl RangeExpr {
    /// Create a new range expression with custom inclusivity flags
    pub fn new(
        column: impl Into<String>,
        min_value: Value,
        max_value: Value,
        include_min: bool,
        include_max: bool,
    ) -> Self {
        Self {
            column: column.into(),
            min_value,
            max_value,
            include_min,
            include_max,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Create an inclusive range (>= min AND <= max)
    pub fn inclusive(column: impl Into<String>, min_value: Value, max_value: Value) -> Self {
        Self::new(column, min_value, max_value, true, true)
    }

    /// Create an exclusive range (> min AND < max)
    pub fn exclusive(column: impl Into<String>, min_value: Value, max_value: Value) -> Self {
        Self::new(column, min_value, max_value, false, false)
    }

    /// Create a half-open range (>= min AND < max)
    pub fn half_open(column: impl Into<String>, min_value: Value, max_value: Value) -> Self {
        Self::new(column, min_value, max_value, true, false)
    }

    /// Get whether min is included
    pub fn includes_min(&self) -> bool {
        self.include_min
    }

    /// Get whether max is included
    pub fn includes_max(&self) -> bool {
        self.include_max
    }

    fn check_value(&self, value: &Value) -> Result<bool> {
        let lower = value.compare(&self.min_value)?;
        let upper = value.compare(&self.max_value)?;
        Ok((if self.include_min {
            lower != Ordering::Less
        } else {
            lower == Ordering::Greater
        }) && (if self.include_max {
            upper != Ordering::Greater
        } else {
            upper == Ordering::Less
        }))
    }
}

impl Expression for RangeExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return Ok(false),
        };

        let col_value = &row[col_idx];

        // NULL in range check is always false
        if col_value.is_null() {
            return Ok(false);
        }

        self.check_value(col_value)
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return false,
        };

        let col_value = &row[col_idx];

        if col_value.is_null() {
            return false;
        }

        self.check_value(col_value).unwrap_or(false)
    }

    fn with_aliases(&self, aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        let resolved = resolve_alias(&self.column, aliases);
        let mut expr = self.clone();

        if resolved != self.column {
            expr.original_column = Some(self.column.clone());
            expr.column = resolved.to_string();
        }

        expr.aliases = aliases.clone();
        expr.col_index = None;
        Box::new(expr)
    }

    fn prepare_for_schema(&mut self, schema: &Schema) {
        if self.col_index.is_some() {
            return;
        }
        self.col_index = find_column_index(schema, &self.column);
    }

    fn collect_column_indices(&self, out: &mut Vec<usize>) -> bool {
        if let Some(idx) = self.col_index {
            out.push(idx);
            true
        } else {
            false
        }
    }

    fn is_prepared(&self) -> bool {
        self.col_index.is_some()
    }

    fn get_column_name(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn can_use_index(&self) -> bool {
        true
    }

    fn get_range_info(&self) -> Option<(&str, &Value, &Value, bool, bool)> {
        Some((
            &self.column,
            &self.min_value,
            &self.max_value,
            self.include_min,
            self.include_max,
        ))
    }

    fn collect_comparisons(&self) -> Vec<(&str, Operator, &Value)> {
        let lower_op = if self.include_min {
            Operator::Gte
        } else {
            Operator::Gt
        };
        let upper_op = if self.include_max {
            Operator::Lte
        } else {
            Operator::Lt
        };
        vec![
            (&self.column, lower_op, &self.min_value),
            (&self.column, upper_op, &self.max_value),
        ]
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        self.min_value.is_null()
            || self.max_value.is_null()
            || self
                .col_index
                .and_then(|index| row.get(index))
                .is_some_and(Value::is_null)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{DataType, SchemaBuilder};

    fn test_schema() -> Schema {
        SchemaBuilder::new("test")
            .add_primary_key("id", DataType::Integer)
            .add("score", DataType::Float)
            .add("name", DataType::Text)
            .build()
    }

    #[test]
    fn test_inclusive_range() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(5),
            Value::float(75.0),
            Value::text("Alice"),
        ]);

        // 5 >= 1 AND 5 <= 10 (inclusive)
        let mut expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));
    }

    #[test]
    fn test_inclusive_on_boundary() {
        let schema = test_schema();

        // Test on lower boundary
        let row = Row::from_values(vec![Value::integer(1), Value::float(0.0), Value::text("a")]);

        let mut expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        // Test on upper boundary
        let row = Row::from_values(vec![
            Value::integer(10),
            Value::float(0.0),
            Value::text("a"),
        ]);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_exclusive_range() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(5),
            Value::float(75.0),
            Value::text("Alice"),
        ]);

        // 5 > 1 AND 5 < 10 (exclusive)
        let mut expr = RangeExpr::exclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));
    }

    #[test]
    fn test_exclusive_on_boundary() {
        let schema = test_schema();

        // Test on lower boundary (should fail)
        let row = Row::from_values(vec![Value::integer(1), Value::float(0.0), Value::text("a")]);

        let mut expr = RangeExpr::exclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);
        assert!(!expr.evaluate(&row).unwrap());

        // Test on upper boundary (should fail)
        let row = Row::from_values(vec![
            Value::integer(10),
            Value::float(0.0),
            Value::text("a"),
        ]);
        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_half_open_range() {
        let schema = test_schema();

        // >= 1 AND < 10 (half-open)
        let mut expr = RangeExpr::half_open("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        // On lower boundary (should pass)
        let row = Row::from_values(vec![Value::integer(1), Value::float(0.0), Value::text("a")]);
        assert!(expr.evaluate(&row).unwrap());

        // On upper boundary (should fail)
        let row = Row::from_values(vec![
            Value::integer(10),
            Value::float(0.0),
            Value::text("a"),
        ]);
        assert!(!expr.evaluate(&row).unwrap());

        // Inside range (should pass)
        let row = Row::from_values(vec![Value::integer(5), Value::float(0.0), Value::text("a")]);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_out_of_range() {
        let schema = test_schema();

        let mut expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        // Below range
        let row = Row::from_values(vec![Value::integer(0), Value::float(0.0), Value::text("a")]);
        assert!(!expr.evaluate(&row).unwrap());

        // Above range
        let row = Row::from_values(vec![
            Value::integer(11),
            Value::float(0.0),
            Value::text("a"),
        ]);
        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_float_range() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::float(75.0),
            Value::text("Alice"),
        ]);

        // 75.0 >= 0.0 AND 75.0 <= 100.0
        let mut expr = RangeExpr::inclusive("score", Value::float(0.0), Value::float(100.0));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_string_range() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::float(0.0),
            Value::text("Bob"),
        ]);

        // "Bob" >= "Alice" AND "Bob" <= "Charlie"
        let mut expr = RangeExpr::inclusive("name", Value::text("Alice"), Value::text("Charlie"));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_in_range() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::null(DataType::Integer),
            Value::float(0.0),
            Value::text("Alice"),
        ]);

        let mut expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        // NULL in range check is always false
        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_unprepared() {
        let row = Row::from_values(vec![Value::integer(5)]);
        let expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));

        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));
    }

    #[test]
    fn test_with_aliases() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(5),
            Value::float(0.0),
            Value::text("Alice"),
        ]);

        let mut aliases = FxHashMap::default();
        aliases.insert("i".to_string(), "id".to_string());

        let expr = RangeExpr::inclusive("i", Value::integer(1), Value::integer(10));
        let mut aliased = expr.with_aliases(&aliases);
        aliased.prepare_for_schema(&schema);

        assert!(aliased.evaluate(&row).unwrap());
    }

    #[test]
    fn test_custom_inclusivity() {
        let schema = test_schema();
        let row = Row::from_values(vec![Value::integer(5), Value::float(0.0), Value::text("a")]);

        // > 1 AND <= 10 (custom: min exclusive, max inclusive)
        let mut expr = RangeExpr::new(
            "id",
            Value::integer(1),
            Value::integer(10),
            false, // exclude min
            true,  // include max
        );
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(!expr.includes_min());
        assert!(expr.includes_max());
    }

    #[test]
    fn test_can_use_index() {
        let expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        assert!(expr.can_use_index());
    }

    #[test]
    fn test_get_column_name() {
        let expr = RangeExpr::inclusive("id", Value::integer(1), Value::integer(10));
        assert_eq!(expr.get_column_name(), Some("id"));
    }

    #[test]
    fn range_uses_original_bounds_without_lossy_secondary_domain() {
        let schema = test_schema();
        let row = Row::from_values(vec![Value::integer(0), Value::float(0.0), Value::text("a")]);
        let mut expr = RangeExpr::exclusive("id", Value::float(-0.5), Value::float(0.5));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));

        let mut malformed =
            RangeExpr::inclusive("id", Value::text("not-an-integer"), Value::integer(10));
        malformed.prepare_for_schema(&schema);
        assert!(malformed.evaluate(&row).is_err());
        assert!(!malformed.evaluate_fast(&row));
    }
}
