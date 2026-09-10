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

//! BETWEEN expression for RadixDB
//!

use std::any::Any;

use rustc_hash::FxHashMap;

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{Operator, Result, Row, Schema, Value};

/// BETWEEN expression (column BETWEEN low AND high)
///
/// By default, BETWEEN is inclusive (>= low AND <= high).
#[derive(Debug, Clone)]
pub struct BetweenExpr {
    /// Column name
    column: String,
    /// Lower bound
    lower_bound: Value,
    /// Upper bound
    upper_bound: Value,
    /// Whether bounds are inclusive (true for standard BETWEEN)
    inclusive: bool,
    /// Whether this is a NOT BETWEEN expression
    /// When true and the value is NULL, returns false (SQL standard: NOT NULL = NULL = false in WHERE)
    not: bool,

    /// Pre-computed column index
    col_index: Option<usize>,

    /// Column aliases
    aliases: FxHashMap<String, String>,
    /// Original column name if using alias
    original_column: Option<String>,
}

impl BetweenExpr {
    /// Create a new BETWEEN expression (inclusive by default)
    pub fn new(column: impl Into<String>, lower: Value, upper: Value) -> Self {
        Self {
            column: column.into(),
            lower_bound: lower,
            upper_bound: upper,
            inclusive: true,
            not: false,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Create a NOT BETWEEN expression
    pub fn not_between(column: impl Into<String>, lower: Value, upper: Value) -> Self {
        Self {
            column: column.into(),
            lower_bound: lower,
            upper_bound: upper,
            inclusive: true,
            not: true,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Create a BETWEEN expression with custom inclusivity
    pub fn with_inclusivity(
        column: impl Into<String>,
        lower: Value,
        upper: Value,
        inclusive: bool,
    ) -> Self {
        Self {
            column: column.into(),
            lower_bound: lower,
            upper_bound: upper,
            inclusive,
            not: false,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Check if inclusive
    pub fn is_inclusive(&self) -> bool {
        self.inclusive
    }

    /// Get the bounds (for expression compilation)
    pub fn get_bounds(&self) -> (&Value, &Value) {
        (&self.lower_bound, &self.upper_bound)
    }

    /// Check if this is a NOT BETWEEN expression
    pub fn is_negated(&self) -> bool {
        self.not
    }

    fn check_value(&self, value: &Value) -> Result<Option<bool>> {
        if value.is_null() || self.lower_bound.is_null() || self.upper_bound.is_null() {
            return Ok(None);
        }
        let lower = value.compare(&self.lower_bound)?;
        let upper = value.compare(&self.upper_bound)?;
        let in_range = if self.inclusive {
            lower != std::cmp::Ordering::Less && upper != std::cmp::Ordering::Greater
        } else {
            lower == std::cmp::Ordering::Greater && upper == std::cmp::Ordering::Less
        };
        Ok(Some(if self.not { !in_range } else { in_range }))
    }
}

impl Expression for BetweenExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return Ok(false),
        };

        let col_value = &row[col_idx];

        Ok(self.check_value(col_value)?.unwrap_or(false))
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return false,
        };

        let col_value = &row[col_idx];

        self.check_value(col_value).ok().flatten().unwrap_or(false)
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

    fn get_between_info(&self) -> Option<(&str, &Value, &Value, bool, bool)> {
        Some((
            &self.column,
            &self.lower_bound,
            &self.upper_bound,
            self.inclusive,
            self.not,
        ))
    }

    fn collect_comparisons(&self) -> Vec<(&str, Operator, &Value)> {
        // NOT BETWEEN cannot be decomposed into simple range comparisons
        // for index use (it's a disjunction: col < low OR col > high)
        if self.not {
            return vec![];
        }
        if self.inclusive {
            // BETWEEN low AND high  =>  col >= low AND col <= high
            vec![
                (&self.column, Operator::Gte, &self.lower_bound),
                (&self.column, Operator::Lte, &self.upper_bound),
            ]
        } else {
            // Exclusive BETWEEN  =>  col > low AND col < high
            vec![
                (&self.column, Operator::Gt, &self.lower_bound),
                (&self.column, Operator::Lt, &self.upper_bound),
            ]
        }
    }

    fn is_conjunctive_simple(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        self.lower_bound.is_null()
            || self.upper_bound.is_null()
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
    fn test_integer_between() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(5),
            Value::float(75.0),
            Value::text("Alice"),
        ]);

        // 5 BETWEEN 1 AND 10
        let mut expr = BetweenExpr::new("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));

        // 5 BETWEEN 1 AND 4 (out of range)
        let mut expr = BetweenExpr::new("id", Value::integer(1), Value::integer(4));
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_inclusive_bounds() {
        let schema = test_schema();
        let row = Row::from_values(vec![Value::integer(1), Value::float(0.0), Value::text("a")]);

        // 1 BETWEEN 1 AND 10 (inclusive, on lower bound)
        let mut expr = BetweenExpr::new("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let row = Row::from_values(vec![
            Value::integer(10),
            Value::float(0.0),
            Value::text("a"),
        ]);

        // 10 BETWEEN 1 AND 10 (inclusive, on upper bound)
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_exclusive_bounds() {
        let schema = test_schema();
        let row = Row::from_values(vec![Value::integer(1), Value::float(0.0), Value::text("a")]);

        // 1 BETWEEN 1 AND 10 (exclusive - should fail)
        let mut expr =
            BetweenExpr::with_inclusivity("id", Value::integer(1), Value::integer(10), false);
        expr.prepare_for_schema(&schema);
        assert!(!expr.evaluate(&row).unwrap());

        let row = Row::from_values(vec![Value::integer(5), Value::float(0.0), Value::text("a")]);
        // 5 BETWEEN 1 AND 10 (exclusive - should pass)
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_float_between() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::float(75.0),
            Value::text("Alice"),
        ]);

        // 75.0 BETWEEN 0.0 AND 100.0
        let mut expr = BetweenExpr::new("score", Value::float(0.0), Value::float(100.0));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_mixed_numeric_between_preserves_exact_identity() {
        const EXACT: i64 = 1_i64 << 53;
        let integer_schema = SchemaBuilder::new("integer_boundary")
            .add("value", DataType::Integer)
            .build();

        let exact = Row::from_values(vec![Value::Integer(EXACT)]);
        let neighbor = Row::from_values(vec![Value::Integer(EXACT + 1)]);
        let mut exact_float_bounds = BetweenExpr::new(
            "value",
            Value::Float(EXACT as f64),
            Value::Float(EXACT as f64),
        );
        exact_float_bounds.prepare_for_schema(&integer_schema);
        assert!(exact_float_bounds.evaluate(&exact).unwrap());
        assert!(exact_float_bounds.evaluate_fast(&exact));
        assert!(!exact_float_bounds.evaluate(&neighbor).unwrap());
        assert!(!exact_float_bounds.evaluate_fast(&neighbor));

        let zero = Row::from_values(vec![Value::Integer(0)]);
        let mut fractional = BetweenExpr::new("value", Value::Float(0.5), Value::Float(0.5));
        fractional.prepare_for_schema(&integer_schema);
        assert!(!fractional.evaluate(&zero).unwrap());
        assert!(!fractional.evaluate_fast(&zero));

        let maximum = Row::from_values(vec![Value::Integer(i64::MAX)]);
        let rounded_out_of_range = i64::MAX as f64;
        let mut out_of_range = BetweenExpr::new(
            "value",
            Value::Float(rounded_out_of_range),
            Value::Float(rounded_out_of_range),
        );
        out_of_range.prepare_for_schema(&integer_schema);
        assert!(!out_of_range.evaluate(&maximum).unwrap());
        assert!(!out_of_range.evaluate_fast(&maximum));

        let mut reversed = BetweenExpr::new(
            "value",
            Value::Integer(EXACT + 1),
            Value::Float(EXACT as f64),
        );
        reversed.prepare_for_schema(&integer_schema);
        assert!(!reversed.evaluate(&exact).unwrap());
        assert!(!reversed.evaluate_fast(&exact));

        let mut decimal_fraction =
            BetweenExpr::new("value", Value::decimal(5, 1, 1), Value::decimal(5, 1, 1));
        decimal_fraction.prepare_for_schema(&integer_schema);
        assert!(!decimal_fraction.evaluate(&zero).unwrap());
        assert!(!decimal_fraction.evaluate_fast(&zero));

        let float_schema = SchemaBuilder::new("float_boundary")
            .add("value", DataType::Float)
            .build();
        let rounded_float = Row::from_values(vec![Value::Float(EXACT as f64)]);
        let mut integer_bounds =
            BetweenExpr::new("value", Value::Integer(EXACT), Value::Integer(EXACT + 1));
        integer_bounds.prepare_for_schema(&float_schema);
        assert!(integer_bounds.evaluate(&rounded_float).unwrap());
        assert!(integer_bounds.evaluate_fast(&rounded_float));

        let mut not_exact = BetweenExpr::not_between(
            "value",
            Value::Float(EXACT as f64),
            Value::Float(EXACT as f64),
        );
        not_exact.prepare_for_schema(&integer_schema);
        assert!(!not_exact.evaluate(&exact).unwrap());
        assert!(!not_exact.evaluate_fast(&exact));
        assert!(not_exact.evaluate(&neighbor).unwrap());
        assert!(not_exact.evaluate_fast(&neighbor));
    }

    #[test]
    fn test_same_float_nan_between_uses_canonical_order() {
        let schema = SchemaBuilder::new("float_nan")
            .add("value", DataType::Float)
            .build();
        let nan_a = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
        let nan_b = Value::Float(f64::from_bits(0x7ff8_0000_0000_0042));
        let row = Row::from_values(vec![nan_a]);

        let mut between = BetweenExpr::new("value", nan_b.clone(), nan_b.clone());
        between.prepare_for_schema(&schema);
        assert!(between.evaluate(&row).unwrap());
        assert!(between.evaluate_fast(&row));

        let mut not_between = BetweenExpr::not_between("value", nan_b.clone(), nan_b);
        not_between.prepare_for_schema(&schema);
        assert!(!not_between.evaluate(&row).unwrap());
        assert!(!not_between.evaluate_fast(&row));
    }

    #[test]
    fn test_string_between() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::float(0.0),
            Value::text("Bob"),
        ]);

        // "Bob" BETWEEN "Alice" AND "Charlie"
        let mut expr = BetweenExpr::new("name", Value::text("Alice"), Value::text("Charlie"));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());

        let row = Row::from_values(vec![
            Value::integer(1),
            Value::float(0.0),
            Value::text("Zack"),
        ]);

        // "Zack" BETWEEN "Alice" AND "Charlie" (out of range)
        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_in_between() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::null(DataType::Integer),
            Value::float(0.0),
            Value::text("Alice"),
        ]);

        // NULL BETWEEN 1 AND 10 is always false
        let mut expr = BetweenExpr::new("id", Value::integer(1), Value::integer(10));
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_unprepared() {
        let row = Row::from_values(vec![Value::integer(5)]);
        let expr = BetweenExpr::new("id", Value::integer(1), Value::integer(10));

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

        let expr = BetweenExpr::new("i", Value::integer(1), Value::integer(10));
        let mut aliased = expr.with_aliases(&aliases);
        aliased.prepare_for_schema(&schema);

        assert!(aliased.evaluate(&row).unwrap());
    }

    #[test]
    fn null_bounds_are_unknown_in_all_modes() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(5),
            Value::float(0.0),
            Value::text("Alice"),
        ]);
        for mut expr in [
            BetweenExpr::new("id", Value::null(DataType::Integer), Value::integer(10)),
            BetweenExpr::not_between("id", Value::integer(1), Value::null(DataType::Integer)),
        ] {
            expr.prepare_for_schema(&schema);
            assert!(!expr.evaluate(&row).unwrap());
            assert!(!expr.evaluate_fast(&row));
            assert!(expr.is_unknown_due_to_null(&row));
        }
    }
}
