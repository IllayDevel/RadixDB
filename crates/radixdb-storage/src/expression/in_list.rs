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

//! IN list expression for RadixDB
//!
//!
//! ## Optimization: HashSet for O(1) Lookup
//!
//! Instead of linear search through the IN list for each row (O(n)),
//! we pre-compute HashSets at prepare time for O(1) lookup.
//! This is the same optimization PostgreSQL uses for "hashed IN lists".

use std::any::Any;

use rustc_hash::{FxHashMap, FxHashSet};

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{DataType, I64Set, Result, Row, Schema, Value};

/// Return true when an exact physical index/typed-column IN lookup would need
/// to change a numeric value's representation. The canonical scalar contract
/// permits cross-numeric equality, but a representation-specific index must
/// not round, truncate or saturate the probe. Callers decline the physical
/// shortcut and retain the complete row-level predicate instead.
#[doc(hidden)]
pub fn has_cross_numeric_physical_variant(target_type: DataType, values: &[Value]) -> bool {
    values.iter().any(|value| match target_type {
        DataType::Integer => matches!(value, Value::Float(_)) || value.as_decimal_parts().is_some(),
        DataType::Float => matches!(value, Value::Integer(_)) || value.as_decimal_parts().is_some(),
        DataType::Decimal => matches!(value, Value::Integer(_) | Value::Float(_)),
        _ => false,
    })
}

/// Pre-computed hash sets for O(1) IN list lookup
#[derive(Debug, Clone)]
enum HashedValues {
    /// Not yet computed
    None,
    /// Integer hash set for O(1) lookup
    Integers(I64Set),
    /// String hash set for O(1) lookup
    Strings(FxHashSet<String>),
    /// Boolean set (only 2 possible values)
    Booleans { has_true: bool, has_false: bool },
    /// Mixed types - fall back to linear search
    Mixed,
}

/// IN list expression (column IN (v1, v2, ...))
///
/// ## Performance
///
/// Uses O(1) HashSet lookup instead of O(n) linear search for each row.
/// Hash sets are pre-computed at `prepare_for_schema` time.
#[derive(Debug, Clone)]
pub struct InListExpr {
    /// Column name
    column: String,
    /// List of values to check against
    values: Vec<Value>,
    /// True for NOT IN
    not: bool,

    /// Pre-computed column index
    col_index: Option<usize>,

    /// Pre-computed hash sets for O(1) lookup
    hashed: HashedValues,

    /// Pre-computed: whether the values list contains NULL
    /// Used for SQL three-valued logic: x NOT IN (a, NULL) returns UNKNOWN if x != a
    has_null: bool,

    /// Column aliases
    aliases: FxHashMap<String, String>,
    /// Original column name if using alias
    original_column: Option<String>,

    /// Cached min/max of non-null values for zone-map pruning.
    /// Computed once in build_hash_sets. Enables volume-level skipping.
    cached_min: Option<Value>,
    cached_max: Option<Value>,
}

impl InListExpr {
    /// Admit a value to the physical i64 set only when it has exactly the
    /// same canonical scalar identity as that Integer. Rust's float cast
    /// truncates fractions and saturates out-of-range values, which is not a
    /// valid IN-key conversion.
    #[inline]
    fn exact_integer_key(value: &Value) -> Option<i64> {
        value
            .exact_integer_identity()
            .filter(|integer| *integer != i64::MIN)
    }

    #[inline]
    fn scalar_values_equal(left: &Value, right: &Value) -> bool {
        if left.is_null() || right.is_null() {
            return false;
        }
        match left.compare(right) {
            Ok(std::cmp::Ordering::Equal) => true,
            Ok(_) => false,
            Err(_) => left == right,
        }
    }

    /// Create a new IN expression
    pub fn new(column: impl Into<String>, values: Vec<Value>) -> Self {
        let has_null = values.iter().any(|v| v.is_null());
        Self {
            column: column.into(),
            values,
            not: false,
            col_index: None,
            hashed: HashedValues::None,
            has_null,
            aliases: FxHashMap::default(),
            cached_min: None,
            cached_max: None,
            original_column: None,
        }
    }

    /// Create a NOT IN expression
    pub fn not_in(column: impl Into<String>, values: Vec<Value>) -> Self {
        let has_null = values.iter().any(|v| v.is_null());
        Self {
            column: column.into(),
            values,
            not: true,
            col_index: None,
            hashed: HashedValues::None,
            has_null,
            aliases: FxHashMap::default(),
            original_column: None,
            cached_min: None,
            cached_max: None,
        }
    }

    /// Check if this is a NOT IN expression
    pub fn is_not(&self) -> bool {
        self.not
    }

    /// Get the values
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Get the values (alias for values(), for expression compilation)
    pub fn get_values(&self) -> &[Value] {
        &self.values
    }

    /// Build hash sets for O(1) lookup
    fn build_hash_sets(&mut self) {
        if self.values.is_empty() {
            self.hashed = HashedValues::None;
            return;
        }

        // Detect the type from the first non-null value
        let first_type = self.values.iter().find_map(|v| match v {
            Value::Integer(_) => Some("int"),
            Value::Float(_) => Some("float"),
            Value::Text(_) => Some("text"),
            Value::Boolean(_) => Some("bool"),
            Value::Null(_) => None,
            _ => Some("other"),
        });

        match first_type {
            Some("int") => {
                // Check if all values are integers (or convertible floats)
                let mut set = I64Set::new();
                let mut all_int = true;
                for v in &self.values {
                    match v {
                        Value::Integer(_) | Value::Float(_) => {
                            if let Some(integer) = Self::exact_integer_key(v) {
                                set.insert(integer);
                            } else {
                                all_int = false;
                                break;
                            }
                        }
                        Value::Null(_) => {} // Skip nulls
                        _ => {
                            all_int = false;
                            break;
                        }
                    }
                }
                if all_int {
                    self.hashed = HashedValues::Integers(set);
                } else {
                    self.hashed = HashedValues::Mixed;
                }
            }
            Some("text") => {
                let mut set = FxHashSet::default();
                let mut all_text = true;
                for v in &self.values {
                    match v {
                        Value::Text(s) => {
                            set.insert(s.to_string());
                        }
                        Value::Null(_) => {}
                        _ => {
                            all_text = false;
                            break;
                        }
                    }
                }
                if all_text {
                    self.hashed = HashedValues::Strings(set);
                } else {
                    self.hashed = HashedValues::Mixed;
                }
            }
            Some("bool") => {
                let mut has_true = false;
                let mut has_false = false;
                for v in &self.values {
                    match v {
                        Value::Boolean(true) => has_true = true,
                        Value::Boolean(false) => has_false = true,
                        Value::Null(_) => {}
                        _ => {}
                    }
                }
                self.hashed = HashedValues::Booleans {
                    has_true,
                    has_false,
                };
            }
            _ => {
                self.hashed = HashedValues::Mixed;
            }
        }

        // Compute min/max for zone-map pruning
        let mut min_val: Option<Value> = None;
        let mut max_val: Option<Value> = None;
        let mut comparable = true;
        for v in &self.values {
            if v.is_null() {
                continue;
            }
            match (&min_val, &max_val) {
                (None, _) => {
                    min_val = Some(v.clone());
                    max_val = Some(v.clone());
                }
                (Some(cur_min), Some(cur_max)) => match (v.compare(cur_min), v.compare(cur_max)) {
                    (Ok(min_order), Ok(max_order)) => {
                        if min_order == std::cmp::Ordering::Less {
                            min_val = Some(v.clone());
                        }
                        if max_order == std::cmp::Ordering::Greater {
                            max_val = Some(v.clone());
                        }
                    }
                    _ => {
                        comparable = false;
                        break;
                    }
                },
                _ => {}
            }
        }
        if comparable {
            self.cached_min = min_val;
            self.cached_max = max_val;
        } else {
            self.cached_min = None;
            self.cached_max = None;
        }
    }

    /// Check if integer is in list - O(1) with hash set, O(n) fallback
    #[inline]
    fn check_integer(&self, val: i64) -> bool {
        match &self.hashed {
            HashedValues::Integers(set) if val != i64::MIN => set.contains(val),
            _ => self
                .values
                .iter()
                .any(|value| Self::scalar_values_equal(&Value::Integer(val), value)),
        }
    }

    /// Check if float is in list
    #[inline]
    fn check_float(&self, val: f64) -> bool {
        // Floats use linear search, but comparison still follows the exact
        // canonical Value contract (including signed zero and NaN).
        self.values
            .iter()
            .any(|value| Self::scalar_values_equal(&Value::Float(val), value))
    }

    /// Check if string is in list - O(1) with hash set, O(n) fallback
    #[inline]
    fn check_string(&self, val: &str) -> bool {
        match &self.hashed {
            HashedValues::Strings(set) => set.contains(val),
            _ => {
                // Fallback to linear search
                for v in &self.values {
                    if let Some(list_val) = v.as_string() {
                        if val == list_val {
                            return true;
                        }
                    }
                }
                false
            }
        }
    }

    /// Check if boolean is in list - O(1)
    #[inline]
    fn check_boolean(&self, val: bool) -> bool {
        match &self.hashed {
            HashedValues::Booleans {
                has_true,
                has_false,
            } => {
                if val {
                    *has_true
                } else {
                    *has_false
                }
            }
            _ => {
                // Fallback
                self.values.iter().any(|v| v.as_boolean() == Some(val))
            }
        }
    }

    /// Check scalar types without a specialized hash representation.
    ///
    /// UUID, TIMESTAMP, DECIMAL, DATE, BYTES and other extension-backed
    /// scalars still have the same SQL equality contract as `Value::compare`.
    /// Falling back to `false` for them makes an `IN` predicate disagree with
    /// the equivalent equality predicate, especially after parameter values
    /// have been coerced to the column type.
    #[inline]
    fn check_scalar(&self, val: &Value) -> bool {
        self.values
            .iter()
            .any(|list_val| Self::scalar_values_equal(val, list_val))
    }
}

impl Expression for InListExpr {
    /// Evaluate IN/NOT IN expression with proper SQL NULL semantics
    ///
    /// SQL Standard three-valued logic:
    /// - `x IN (a, b, NULL)`: TRUE if x matches, UNKNOWN (treated as false for filtering) if not
    /// - `x NOT IN (a, b, NULL)`: FALSE if x matches, UNKNOWN (treated as false for filtering) if not
    ///
    /// For storage-layer filtering, UNKNOWN is treated as false (don't return the row)
    fn evaluate(&self, row: &Row) -> Result<bool> {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return Ok(false),
        };

        let col_value = &row[col_idx];

        // NULL IN (...) is UNKNOWN (false for filtering)
        // NULL NOT IN (...) is UNKNOWN (false for filtering)
        if col_value.is_null() {
            return Ok(false);
        }

        // O(1) lookup using pre-computed hash sets!
        let found = match col_value {
            Value::Integer(val) => self.check_integer(*val),
            Value::Float(val) => self.check_float(*val),
            Value::Text(val) => self.check_string(val),
            Value::Boolean(val) => self.check_boolean(*val),
            _ => self.check_scalar(col_value),
        };

        if found {
            // Found a match
            Ok(!self.not) // IN returns true, NOT IN returns false
        } else if self.has_null {
            // No match found, but list contains NULL (pre-computed)
            // Result is UNKNOWN, which for filtering purposes means false
            Ok(false)
        } else {
            // No match, no NULL in list - definitive answer
            Ok(self.not) // IN returns false, NOT IN returns true
        }
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return false, // Unknown column, return false for safety
        };

        let col_value = &row[col_idx];

        // NULL comparisons result in UNKNOWN (false for filtering)
        if col_value.is_null() {
            return false;
        }

        // O(1) lookup using pre-computed hash sets!
        let found = match col_value {
            Value::Integer(val) => self.check_integer(*val),
            Value::Float(val) => self.check_float(*val),
            Value::Text(val) => self.check_string(val),
            Value::Boolean(val) => self.check_boolean(*val),
            _ => self.check_scalar(col_value),
        };

        if found {
            !self.not // IN returns true, NOT IN returns false
        } else if self.has_null {
            // No match but list has NULL (pre-computed) - result is UNKNOWN (false for filtering)
            false
        } else {
            self.not // IN returns false, NOT IN returns true
        }
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
        expr.hashed = HashedValues::None; // Reset hash sets
        Box::new(expr)
    }

    fn prepare_for_schema(&mut self, schema: &Schema) {
        if self.col_index.is_some() {
            return;
        }
        self.col_index = find_column_index(schema, &self.column);
        // Build hash sets for O(1) lookup
        self.build_hash_sets();
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

    fn get_in_list_info(&self) -> Option<(&str, &[Value], bool, bool)> {
        Some((&self.column, &self.values, self.not, self.has_null))
    }

    fn collect_comparisons(&self) -> Vec<(&str, radixdb_core::Operator, &Value)> {
        // For NOT IN, pruning is not safe (we want rows NOT matching)
        if self.not {
            return vec![];
        }
        // Return Gte(min) + Lte(max) bounds from the IN values.
        // This enables zone-map pruning: volumes entirely below min or above max
        // are skipped. For sorted columns, binary search narrows the scan range
        // to [min, max] within surviving volumes.
        match (&self.cached_min, &self.cached_max) {
            (Some(min), Some(max)) => {
                vec![
                    (&self.column, radixdb_core::Operator::Gte, min),
                    (&self.column, radixdb_core::Operator::Lte, max),
                ]
            }
            _ => vec![],
        }
    }

    fn can_use_index(&self) -> bool {
        true
    }

    fn is_conjunctive_simple(&self) -> bool {
        // IN list collect_comparisons returns min/max bounds, not exact values.
        // Cannot be used for columnar aggregate pushdown (would over-match).
        false
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        let Some(value) = self.col_index.and_then(|index| row.get(index)) else {
            return false;
        };
        if value.is_null() {
            return true;
        }
        self.has_null && !self.check_scalar(value)
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
            .add("name", DataType::Text)
            .add("status", DataType::Text)
            .build()
    }

    #[test]
    fn test_integer_in() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(2),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 2 IN (1, 2, 3)
        let mut expr = InListExpr::new(
            "id",
            vec![Value::integer(1), Value::integer(2), Value::integer(3)],
        );
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));

        // 2 IN (5, 6, 7)
        let mut expr = InListExpr::new(
            "id",
            vec![Value::integer(5), Value::integer(6), Value::integer(7)],
        );
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_string_in() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // "active" IN ("active", "inactive", "pending")
        let mut expr = InListExpr::new(
            "status",
            vec![
                Value::text("active"),
                Value::text("inactive"),
                Value::text("pending"),
            ],
        );
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_cross_numeric_physical_variant_matrix() {
        let decimal = Value::decimal(15, 2, 1);
        assert!(!has_cross_numeric_physical_variant(
            DataType::Integer,
            &[Value::Integer(1)],
        ));
        assert!(has_cross_numeric_physical_variant(
            DataType::Integer,
            &[Value::Float(1.0)],
        ));
        assert!(has_cross_numeric_physical_variant(
            DataType::Integer,
            std::slice::from_ref(&decimal),
        ));
        assert!(has_cross_numeric_physical_variant(
            DataType::Float,
            &[Value::Integer(1)],
        ));
        assert!(!has_cross_numeric_physical_variant(
            DataType::Float,
            &[Value::Float(1.0)],
        ));
        assert!(has_cross_numeric_physical_variant(
            DataType::Decimal,
            &[Value::Float(1.0)],
        ));
        assert!(!has_cross_numeric_physical_variant(
            DataType::Decimal,
            &[decimal],
        ));
    }

    #[test]
    fn test_not_in() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(4),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 4 NOT IN (1, 2, 3)
        let mut expr = InListExpr::not_in(
            "id",
            vec![Value::integer(1), Value::integer(2), Value::integer(3)],
        );
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());

        // 4 NOT IN (4, 5, 6)
        let mut expr = InListExpr::not_in(
            "id",
            vec![Value::integer(4), Value::integer(5), Value::integer(6)],
        );
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_in() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::null(DataType::Integer),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // NULL IN (1, 2, 3) is false
        let mut expr = InListExpr::new(
            "id",
            vec![Value::integer(1), Value::integer(2), Value::integer(3)],
        );
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());

        // NULL NOT IN (1, 2, 3) is also false
        let mut expr = InListExpr::not_in(
            "id",
            vec![Value::integer(1), Value::integer(2), Value::integer(3)],
        );
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_empty_list() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 1 IN () is always false
        let mut expr = InListExpr::new("id", vec![]);
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());

        // 1 NOT IN () is always true
        let mut expr = InListExpr::not_in("id", vec![]);
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_mixed_numeric_types() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(2),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 2 IN (1.0, 2.0, 3.0) - mixed int/float
        let mut expr = InListExpr::new(
            "id",
            vec![Value::float(1.0), Value::float(2.0), Value::float(3.0)],
        );
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_in_list_exact_mixed_numeric_identity_boundaries() {
        let schema = test_schema();
        let two53 = 1_i64 << 53;

        let mut exact = InListExpr::new(
            "id",
            vec![Value::Integer(two53), Value::Float(two53 as f64)],
        );
        exact.prepare_for_schema(&schema);
        let exact_row = Row::from_values(vec![
            Value::Integer(two53),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(exact.evaluate(&exact_row).unwrap());
        assert!(exact.evaluate_fast(&exact_row));

        let neighbor_row = Row::from_values(vec![
            Value::Integer(two53 + 1),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(!exact.evaluate(&neighbor_row).unwrap());
        assert!(!exact.evaluate_fast(&neighbor_row));

        let mut out_of_range = InListExpr::new(
            "id",
            vec![Value::Integer(i64::MIN), Value::Float(2_f64.powi(63))],
        );
        out_of_range.prepare_for_schema(&schema);
        let min_row = Row::from_values(vec![
            Value::Integer(i64::MIN),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(out_of_range.evaluate(&min_row).unwrap());
        assert!(out_of_range.evaluate_fast(&min_row));
        let max_row = Row::from_values(vec![
            Value::Integer(i64::MAX),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(!out_of_range.evaluate(&max_row).unwrap());
        assert!(!out_of_range.evaluate_fast(&max_row));

        let mut fractional = InListExpr::new(
            "id",
            vec![Value::Integer(0), Value::Float(1.5), Value::Float(-0.0)],
        );
        fractional.prepare_for_schema(&schema);
        let one_row = Row::from_values(vec![
            Value::Integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(!fractional.evaluate(&one_row).unwrap());
        assert!(!fractional.evaluate_fast(&one_row));
        let zero_row = Row::from_values(vec![
            Value::Integer(0),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(fractional.evaluate(&zero_row).unwrap());
        assert!(fractional.evaluate_fast(&zero_row));

        let mut nan = InListExpr::new("id", vec![Value::Float(f64::NAN)]);
        nan.prepare_for_schema(&schema);
        let nan_row = Row::from_values(vec![
            Value::Float(f64::NAN),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(nan.evaluate(&nan_row).unwrap());
        assert!(nan.evaluate_fast(&nan_row));
    }

    #[test]
    fn test_uuid_in_uses_scalar_equality_contract() {
        let uuid_a = [0x11; 16];
        let uuid_b = [0x22; 16];
        let uuid_c = [0x33; 16];
        let schema = SchemaBuilder::new("uuid_values")
            .add_primary_key("row_id", DataType::Integer)
            .add("id", DataType::Uuid)
            .build();
        let row = Row::from_values(vec![Value::integer(1), Value::uuid(uuid_b)]);

        let mut matching = InListExpr::new("id", vec![Value::uuid(uuid_a), Value::uuid(uuid_b)]);
        matching.prepare_for_schema(&schema);
        assert!(matching.evaluate(&row).unwrap());
        assert!(matching.evaluate_fast(&row));

        let mut missing = InListExpr::new("id", vec![Value::uuid(uuid_a), Value::uuid(uuid_c)]);
        missing.prepare_for_schema(&schema);
        assert!(!missing.evaluate(&row).unwrap());
        assert!(!missing.evaluate_fast(&row));

        let mut duplicate = InListExpr::new(
            "id",
            vec![
                Value::uuid(uuid_b),
                Value::uuid(uuid_b),
                Value::null(DataType::Uuid),
            ],
        );
        duplicate.prepare_for_schema(&schema);
        assert!(duplicate.evaluate(&row).unwrap());
        assert!(duplicate.evaluate_fast(&row));
    }

    #[test]
    fn test_with_aliases() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        let mut aliases = FxHashMap::default();
        aliases.insert("i".to_string(), "id".to_string());

        let expr = InListExpr::new("i", vec![Value::integer(1), Value::integer(2)]);
        let mut aliased = expr.with_aliases(&aliases);
        aliased.prepare_for_schema(&schema);

        assert!(aliased.evaluate(&row).unwrap());
    }

    #[test]
    fn test_is_not() {
        let expr = InListExpr::new("id", vec![Value::integer(1)]);
        assert!(!expr.is_not());

        let expr = InListExpr::not_in("id", vec![Value::integer(1)]);
        assert!(expr.is_not());
    }

    #[test]
    fn test_not_in_with_null_in_list() {
        // SQL Standard: x NOT IN (a, NULL) returns UNKNOWN if x != a
        // For storage-layer filtering, UNKNOWN is treated as false
        let schema = test_schema();

        // id = 1, check NOT IN (2, NULL)
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 1 NOT IN (2, NULL) - 1 != 2 is TRUE, but 1 != NULL is UNKNOWN
        // TRUE AND UNKNOWN = UNKNOWN, so for filtering this returns false
        let mut expr = InListExpr::not_in(
            "id",
            vec![Value::integer(2), Value::null(DataType::Integer)],
        );
        expr.prepare_for_schema(&schema);

        // For storage-layer filtering, UNKNOWN means false (exclude row)
        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));

        // 2 NOT IN (2, NULL) - 2 == 2 means FALSE (value is in list)
        let row2 = Row::from_values(vec![
            Value::integer(2),
            Value::text("Bob"),
            Value::text("active"),
        ]);
        assert!(!expr.evaluate(&row2).unwrap());
        assert!(!expr.evaluate_fast(&row2));
    }

    #[test]
    fn test_in_with_null_in_list() {
        // SQL Standard: x IN (a, NULL) returns TRUE if x = a, UNKNOWN otherwise
        let schema = test_schema();

        // id = 1, check IN (2, NULL)
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 1 IN (2, NULL) - 1 != 2 and 1 = NULL is UNKNOWN
        // FALSE OR UNKNOWN = UNKNOWN
        let mut expr = InListExpr::new(
            "id",
            vec![Value::integer(2), Value::null(DataType::Integer)],
        );
        expr.prepare_for_schema(&schema);

        // For storage-layer filtering, UNKNOWN means false
        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));

        // 2 IN (2, NULL) - 2 == 2 means TRUE (value is in list)
        let row2 = Row::from_values(vec![
            Value::integer(2),
            Value::text("Bob"),
            Value::text("active"),
        ]);
        assert!(expr.evaluate(&row2).unwrap());
        assert!(expr.evaluate_fast(&row2));
    }

    #[test]
    fn test_not_in_without_null() {
        // Without NULL in list, NOT IN works normally
        let schema = test_schema();

        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);

        // 1 NOT IN (2, 3) - 1 != 2 AND 1 != 3 = TRUE
        let mut expr = InListExpr::not_in("id", vec![Value::integer(2), Value::integer(3)]);
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));
    }

    #[test]
    fn heterogeneous_list_never_exports_partial_bounds() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("match"),
            Value::text("active"),
        ]);
        let mut expr = InListExpr::new("name", vec![Value::integer(100), Value::text("match")]);
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.collect_comparisons().is_empty());
    }

    #[test]
    fn in_list_reports_unknown_for_null_input_or_null_tail() {
        let schema = test_schema();
        let mut expr = InListExpr::new(
            "id",
            vec![Value::integer(2), Value::null(DataType::Integer)],
        );
        expr.prepare_for_schema(&schema);
        let miss = Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        let null = Row::from_values(vec![
            Value::null(DataType::Integer),
            Value::text("Alice"),
            Value::text("active"),
        ]);
        assert!(expr.is_unknown_due_to_null(&miss));
        assert!(expr.is_unknown_due_to_null(&null));
    }
}
