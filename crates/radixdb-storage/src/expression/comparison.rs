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

//! Comparison expression for RadixDB
//!
//!
//! This is the most commonly used expression type, handling simple comparisons
//! like `column = value`, `column > value`, etc.

use std::{any::Any, cmp::Ordering};

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{DataType, Error, Operator, Result, Row, Schema, SmartString, Value};

/// Pre-computed typed value for fast comparison
///
/// This avoids runtime type checking during evaluation by storing
/// the comparison value in its native type.
#[derive(Debug, Clone)]
pub enum ComparisonValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Uuid([u8; 16]),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: u8,
    },
}

impl ComparisonValue {
    /// Create a ComparisonValue from a Value
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null(_) => ComparisonValue::Null,
            Value::Integer(i) => ComparisonValue::Integer(*i),
            Value::Float(f) => ComparisonValue::Float(*f),
            Value::Text(s) => ComparisonValue::Text(s.to_string()),
            Value::Boolean(b) => ComparisonValue::Boolean(*b),
            Value::Timestamp(t) => ComparisonValue::Timestamp(*t),
            Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => value
                .as_decimal_parts()
                .map(|(unscaled, precision, scale)| ComparisonValue::Decimal {
                    unscaled,
                    precision,
                    scale,
                })
                .unwrap_or(ComparisonValue::Null),
            Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => value
                .as_uuid_bytes()
                .map(ComparisonValue::Uuid)
                .unwrap_or_else(|| {
                    ComparisonValue::Text(String::from_utf8_lossy(&data[1..]).into_owned())
                }),
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                ComparisonValue::Text(std::str::from_utf8(&data[1..]).unwrap_or("").to_string())
            }
            Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                ComparisonValue::Text(radixdb_core::value::format_vector_bytes(&data[1..]))
            }
            Value::Extension(data) => {
                ComparisonValue::Text(String::from_utf8_lossy(&data[1..]).into_owned())
            }
        }
    }

    /// Get the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            ComparisonValue::Null => DataType::Text, // NULL has no specific type
            ComparisonValue::Integer(_) => DataType::Integer,
            ComparisonValue::Float(_) => DataType::Float,
            ComparisonValue::Text(_) => DataType::Text,
            ComparisonValue::Boolean(_) => DataType::Boolean,
            ComparisonValue::Timestamp(_) => DataType::Timestamp,
            ComparisonValue::Uuid(_) => DataType::Uuid,
            ComparisonValue::Decimal { .. } => DataType::Decimal,
        }
    }

    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, ComparisonValue::Null)
    }

    /// Convert to a Value
    pub fn to_value(&self) -> Value {
        match self {
            ComparisonValue::Null => Value::Null(DataType::Text),
            ComparisonValue::Integer(i) => Value::Integer(*i),
            ComparisonValue::Float(f) => Value::Float(*f),
            ComparisonValue::Text(s) => Value::Text(SmartString::new(s)),
            ComparisonValue::Boolean(b) => Value::Boolean(*b),
            ComparisonValue::Timestamp(t) => Value::Timestamp(*t),
            ComparisonValue::Uuid(bytes) => Value::uuid(*bytes),
            ComparisonValue::Decimal {
                unscaled,
                precision,
                scale,
            } => Value::decimal(*unscaled, *precision, *scale),
        }
    }
}

/// Comparison expression (column op value)
///
/// # Examples
/// - `id = 1`
/// - `name = 'Alice'`
/// - `age > 18`
/// - `price <= 99.99`
#[derive(Debug, Clone)]
pub struct ComparisonExpr {
    /// Column name to compare
    column: String,
    /// Comparison operator
    operator: Operator,
    /// Pre-computed comparison value (for fast evaluation)
    value: ComparisonValue,
    /// Original value (for get_comparison_info)
    original_value: Value,

    /// Pre-computed column index (None if not prepared)
    col_index: Option<usize>,

    /// Column aliases
    aliases: FxHashMap<String, String>,
    /// Original column name if using alias
    original_column: Option<String>,
}

impl ComparisonExpr {
    /// Create a new comparison expression
    pub fn new(column: impl Into<String>, operator: Operator, value: Value) -> Self {
        Self {
            column: column.into(),
            operator,
            value: ComparisonValue::from_value(&value),
            original_value: value,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Create an equality expression (column = value)
    pub fn eq(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Eq, value)
    }

    /// Create a not-equal expression (column != value)
    pub fn ne(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Ne, value)
    }

    /// Create a greater-than expression (column > value)
    pub fn gt(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Gt, value)
    }

    /// Create a greater-than-or-equal expression (column >= value)
    pub fn gte(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Gte, value)
    }

    /// Create a less-than expression (column < value)
    pub fn lt(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Lt, value)
    }

    /// Create a less-than-or-equal expression (column <= value)
    pub fn lte(column: impl Into<String>, value: Value) -> Self {
        Self::new(column, Operator::Lte, value)
    }

    /// Get the column name
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Get the operator
    pub fn operator(&self) -> Operator {
        self.operator
    }

    /// Get the comparison value
    pub fn value(&self) -> &ComparisonValue {
        &self.value
    }

    /// Get the integer value if this is an integer comparison
    pub fn integer_value(&self) -> Option<i64> {
        match &self.value {
            ComparisonValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Compare two integers with the configured operator
    #[inline]
    fn compare_integers(&self, col_val: i64, cmp_val: i64) -> bool {
        match self.operator {
            Operator::Eq => col_val == cmp_val,
            Operator::Ne => col_val != cmp_val,
            Operator::Gt => col_val > cmp_val,
            Operator::Gte => col_val >= cmp_val,
            Operator::Lt => col_val < cmp_val,
            Operator::Lte => col_val <= cmp_val,
            _ => false,
        }
    }

    /// Compare two floats with the canonical scalar ordering.
    ///
    /// Primitive IEEE comparisons make every NaN unequal and unordered, while
    /// `Value` deliberately treats all NaN payloads as one canonical-last
    /// class. Storage predicates must use the same contract as the VM and key
    /// consumers, including for same-domain Float comparisons.
    #[inline]
    fn compare_floats(&self, col_val: f64, cmp_val: f64) -> bool {
        self.compare_mixed_numeric(&Value::Float(col_val), &Value::Float(cmp_val))
    }

    /// Compare an uncoerced mixed numeric pair without changing representation.
    ///
    /// Integer same-domain comparisons remain primitive. Float and mixed
    /// numeric values delegate to the canonical `Value` ordering established
    /// by the scalar contract.
    #[inline]
    fn compare_mixed_numeric(&self, col_value: &Value, cmp_value: &Value) -> bool {
        let Ok(ordering) = col_value.compare(cmp_value) else {
            return false;
        };
        match self.operator {
            Operator::Eq => ordering == Ordering::Equal,
            Operator::Ne => ordering != Ordering::Equal,
            Operator::Gt => ordering == Ordering::Greater,
            Operator::Gte => ordering != Ordering::Less,
            Operator::Lt => ordering == Ordering::Less,
            Operator::Lte => ordering != Ordering::Greater,
            _ => false,
        }
    }

    /// Compare two strings with the configured operator
    #[inline]
    fn compare_strings(&self, col_val: &str, cmp_val: &str) -> bool {
        match self.operator {
            Operator::Eq => col_val == cmp_val,
            Operator::Ne => col_val != cmp_val,
            Operator::Gt => col_val > cmp_val,
            Operator::Gte => col_val >= cmp_val,
            Operator::Lt => col_val < cmp_val,
            Operator::Lte => col_val <= cmp_val,
            _ => false,
        }
    }

    /// Compare two booleans with the configured operator
    #[inline]
    fn compare_booleans(&self, col_val: bool, cmp_val: bool) -> bool {
        match self.operator {
            Operator::Eq => col_val == cmp_val,
            Operator::Ne => col_val != cmp_val,
            _ => false, // Other operators don't make sense for booleans
        }
    }

    /// Compare two timestamps with the configured operator
    #[inline]
    fn compare_timestamps(&self, col_val: DateTime<Utc>, cmp_val: DateTime<Utc>) -> bool {
        match self.operator {
            Operator::Eq => col_val == cmp_val,
            Operator::Ne => col_val != cmp_val,
            Operator::Gt => col_val > cmp_val,
            Operator::Gte => col_val >= cmp_val,
            Operator::Lt => col_val < cmp_val,
            Operator::Lte => col_val <= cmp_val,
            _ => false,
        }
    }

    /// Compare two UUIDs by their canonical 16-byte representation.
    #[inline]
    fn compare_uuids(&self, col_val: &[u8; 16], cmp_val: &[u8; 16]) -> bool {
        match self.operator {
            Operator::Eq => col_val == cmp_val,
            Operator::Ne => col_val != cmp_val,
            Operator::Gt => col_val > cmp_val,
            Operator::Gte => col_val >= cmp_val,
            Operator::Lt => col_val < cmp_val,
            Operator::Lte => col_val <= cmp_val,
            _ => false,
        }
    }

    /// Compare exact decimal payloads without converting them to Text or f64.
    #[inline]
    fn compare_decimals(
        &self,
        col_unscaled: i128,
        col_scale: u8,
        cmp_unscaled: i128,
        cmp_scale: u8,
    ) -> bool {
        let ordering = radixdb_core::value::compare_decimal_parts(
            col_unscaled,
            col_scale,
            cmp_unscaled,
            cmp_scale,
        );
        match self.operator {
            Operator::Eq => ordering == Ordering::Equal,
            Operator::Ne => ordering != Ordering::Equal,
            Operator::Gt => ordering == Ordering::Greater,
            Operator::Gte => ordering != Ordering::Less,
            Operator::Lt => ordering == Ordering::Less,
            Operator::Lte => ordering != Ordering::Greater,
            _ => false,
        }
    }
}

impl Expression for ComparisonExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        // Must be prepared for schema
        let col_idx = match self.col_index {
            Some(idx) => idx,
            None => return Ok(false),
        };

        // Bounds check
        if col_idx >= row.len() {
            return Ok(false);
        }

        let col_value = &row[col_idx];

        // Handle NULL column value
        if col_value.is_null() {
            return Ok(matches!(self.operator, Operator::IsNull));
        }

        // Handle NULL check operators
        match self.operator {
            Operator::IsNull => return Ok(col_value.is_null()),
            Operator::IsNotNull => return Ok(!col_value.is_null()),
            _ => {}
        }

        // Handle NULL comparison value (all comparisons with NULL are false except IS NULL)
        if self.value.is_null() {
            return Ok(false);
        }

        // Type-specific comparisons
        match (&self.value, col_value) {
            // Integer comparisons
            (ComparisonValue::Integer(cmp_val), Value::Integer(col_val)) => {
                Ok(self.compare_integers(*col_val, *cmp_val))
            }

            // Float comparisons
            (ComparisonValue::Float(cmp_val), Value::Float(col_val)) => {
                Ok(self.compare_floats(*col_val, *cmp_val))
            }

            // String comparisons
            (ComparisonValue::Text(cmp_val), Value::Text(col_val)) => {
                Ok(self.compare_strings(col_val, cmp_val))
            }

            // Boolean comparisons
            (ComparisonValue::Boolean(cmp_val), Value::Boolean(col_val)) => {
                Ok(self.compare_booleans(*col_val, *cmp_val))
            }

            // Timestamp comparisons
            (ComparisonValue::Timestamp(cmp_val), Value::Timestamp(col_val)) => {
                Ok(self.compare_timestamps(*col_val, *cmp_val))
            }

            // UUID comparisons
            (ComparisonValue::Uuid(cmp_val), Value::Extension(_)) => col_value
                .as_uuid_bytes()
                .map(|col_val| self.compare_uuids(&col_val, cmp_val))
                .ok_or_else(|| Error::type_conversion(format!("{:?}", col_value), "UUID")),

            // Exact DECIMAL comparisons. prepare_for_schema coerces finite
            // INTEGER/FLOAT literals to this representation before scanning.
            (
                ComparisonValue::Decimal {
                    unscaled: cmp_unscaled,
                    scale: cmp_scale,
                    ..
                },
                Value::Extension(_),
            ) => col_value
                .as_decimal_parts()
                .map(|(col_unscaled, _, col_scale)| {
                    self.compare_decimals(col_unscaled, col_scale, *cmp_unscaled, *cmp_scale)
                })
                .ok_or_else(|| Error::type_conversion(format!("{:?}", col_value), "DECIMAL")),

            // Cross-type numeric comparisons (integer vs float)
            (ComparisonValue::Integer(cmp_val), Value::Float(_)) => {
                Ok(self.compare_mixed_numeric(col_value, &Value::Integer(*cmp_val)))
            }
            (ComparisonValue::Float(cmp_val), Value::Integer(_)) => {
                Ok(self.compare_mixed_numeric(col_value, &Value::Float(*cmp_val)))
            }
            (ComparisonValue::Decimal { .. }, Value::Integer(_) | Value::Float(_)) => {
                Ok(self.compare_mixed_numeric(col_value, &self.original_value))
            }
            (ComparisonValue::Integer(_) | ComparisonValue::Float(_), Value::Extension(_))
                if col_value.as_decimal_parts().is_some() =>
            {
                Ok(self.compare_mixed_numeric(col_value, &self.original_value))
            }

            // Type mismatch
            _ => Err(Error::type_conversion(
                format!("{:?}", col_value.data_type()),
                format!("{:?}", self.value.data_type()),
            )),
        }
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        // Must be prepared and have valid index
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return false,
        };

        let col_value = &row[col_idx];

        // NULL handling
        if col_value.is_null() {
            return matches!(self.operator, Operator::IsNull);
        }

        match self.operator {
            Operator::IsNull => return col_value.is_null(),
            Operator::IsNotNull => return !col_value.is_null(),
            _ => {}
        }

        if self.value.is_null() {
            return false;
        }

        // Fast path for same-type comparisons
        match (&self.value, col_value) {
            (ComparisonValue::Integer(cmp_val), Value::Integer(col_val)) => {
                self.compare_integers(*col_val, *cmp_val)
            }
            (ComparisonValue::Float(cmp_val), Value::Float(col_val)) => {
                self.compare_floats(*col_val, *cmp_val)
            }
            (ComparisonValue::Text(cmp_val), Value::Text(col_val)) => {
                self.compare_strings(col_val, cmp_val)
            }
            (ComparisonValue::Boolean(cmp_val), Value::Boolean(col_val)) => {
                self.compare_booleans(*col_val, *cmp_val)
            }
            (ComparisonValue::Timestamp(cmp_val), Value::Timestamp(col_val)) => {
                self.compare_timestamps(*col_val, *cmp_val)
            }
            (ComparisonValue::Uuid(cmp_val), Value::Extension(_)) => col_value
                .as_uuid_bytes()
                .map(|col_val| self.compare_uuids(&col_val, cmp_val))
                .unwrap_or(false),
            (
                ComparisonValue::Decimal {
                    unscaled: cmp_unscaled,
                    scale: cmp_scale,
                    ..
                },
                Value::Extension(_),
            ) => col_value
                .as_decimal_parts()
                .map(|(col_unscaled, _, col_scale)| {
                    self.compare_decimals(col_unscaled, col_scale, *cmp_unscaled, *cmp_scale)
                })
                .unwrap_or(false),
            // Cross-type numeric
            (ComparisonValue::Integer(cmp_val), Value::Float(_)) => {
                self.compare_mixed_numeric(col_value, &Value::Integer(*cmp_val))
            }
            (ComparisonValue::Float(cmp_val), Value::Integer(_)) => {
                self.compare_mixed_numeric(col_value, &Value::Float(*cmp_val))
            }
            (ComparisonValue::Decimal { .. }, Value::Integer(_) | Value::Float(_)) => {
                self.compare_mixed_numeric(col_value, &self.original_value)
            }
            (ComparisonValue::Integer(_) | ComparisonValue::Float(_), Value::Extension(_))
                if col_value.as_decimal_parts().is_some() =>
            {
                self.compare_mixed_numeric(col_value, &self.original_value)
            }
            _ => false,
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
        expr.col_index = None; // Reset preparation
        Box::new(expr)
    }

    fn prepare_for_schema(&mut self, schema: &Schema) {
        if self.col_index.is_some() {
            return; // Already prepared
        }

        self.col_index = find_column_index(schema, &self.column);
        if let Some(idx) = self.col_index {
            let column_type = schema.columns[idx].data_type;
            if matches!(column_type, DataType::Uuid | DataType::Decimal) {
                let coerced = self.original_value.coerce_to_type(column_type);
                if !coerced.is_null() {
                    self.value = ComparisonValue::from_value(&coerced);
                    self.original_value = coerced;
                }
            }
        }
    }

    fn is_prepared(&self) -> bool {
        self.col_index.is_some()
    }

    fn get_column_name(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn collect_column_indices(&self, out: &mut Vec<usize>) -> bool {
        if let Some(idx) = self.col_index {
            out.push(idx);
            true
        } else {
            false
        }
    }

    fn can_use_index(&self) -> bool {
        matches!(
            self.operator,
            Operator::Eq | Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte
        )
    }

    fn get_comparison_info(&self) -> Option<(&str, Operator, &Value)> {
        Some((&self.column, self.operator, &self.original_value))
    }

    fn is_conjunctive_simple(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        // Check if this comparison would return UNKNOWN due to NULL
        // This happens when:
        // 1. The column value is NULL (and operator is not IS NULL/IS NOT NULL)
        // 2. The comparison value is NULL

        // Must be prepared for schema
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return false,
        };

        let col_value = &row[col_idx];

        // IS NULL and IS NOT NULL operators don't produce UNKNOWN
        if matches!(self.operator, Operator::IsNull | Operator::IsNotNull) {
            return false;
        }

        // If column is NULL, comparison is UNKNOWN
        if col_value.is_null() {
            return true;
        }

        // If comparison value is NULL, comparison is UNKNOWN
        if self.value.is_null() {
            return true;
        }

        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::SchemaBuilder;

    fn test_schema() -> Schema {
        SchemaBuilder::new("test")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .add("score", DataType::Float)
            .add("active", DataType::Boolean)
            .build()
    }

    fn test_row() -> Row {
        Row::from_values(vec![
            Value::integer(1),
            Value::text("Alice"),
            Value::float(95.5),
            Value::boolean(true),
        ])
    }

    #[test]
    fn test_integer_equality() {
        let schema = test_schema();
        let row = test_row();

        let mut expr = ComparisonExpr::eq("id", Value::integer(1));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));

        let mut expr = ComparisonExpr::eq("id", Value::integer(2));
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));
    }

    #[test]
    fn test_integer_comparison() {
        let schema = test_schema();
        let row = test_row(); // id = 1

        let mut expr = ComparisonExpr::gt("id", Value::integer(0));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::gte("id", Value::integer(1));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::lt("id", Value::integer(2));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::lte("id", Value::integer(1));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::ne("id", Value::integer(2));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_string_equality() {
        let schema = test_schema();
        let row = test_row();

        let mut expr = ComparisonExpr::eq("name", Value::text("Alice"));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::eq("name", Value::text("Bob"));
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_float_comparison() {
        let schema = test_schema();
        let row = test_row(); // score = 95.5

        let mut expr = ComparisonExpr::gt("score", Value::float(90.0));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::lt("score", Value::float(100.0));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_boolean_comparison() {
        let schema = test_schema();
        let row = test_row(); // active = true

        let mut expr = ComparisonExpr::eq("active", Value::boolean(true));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        let mut expr = ComparisonExpr::ne("active", Value::boolean(false));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_handling() {
        let schema = SchemaBuilder::new("test")
            .add_nullable("value", DataType::Integer)
            .build();

        let null_row = Row::from_values(vec![Value::null(DataType::Integer)]);

        // NULL = 1 should be false
        let mut expr = ComparisonExpr::eq("value", Value::integer(1));
        expr.prepare_for_schema(&schema);
        assert!(!expr.evaluate(&null_row).unwrap());
    }

    #[test]
    fn test_cross_type_numeric() {
        let schema = test_schema();

        // Compare integer column with float value
        let row = test_row(); // id = 1
        let mut expr = ComparisonExpr::eq("id", Value::float(1.0));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());

        // Compare float column with integer value
        let mut expr = ComparisonExpr::gt("score", Value::integer(90));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_cross_type_numeric_preserves_exact_integer_boundary() {
        const EXACT: i64 = 1_i64 << 53;
        let integer_schema = SchemaBuilder::new("integer_boundary")
            .add("value", DataType::Integer)
            .build();
        let integer_neighbor = Row::from_values(vec![Value::Integer(EXACT + 1)]);

        let mut equal = ComparisonExpr::eq("value", Value::Float(EXACT as f64));
        equal.prepare_for_schema(&integer_schema);
        assert!(!equal.evaluate(&integer_neighbor).unwrap());
        assert!(!equal.evaluate_fast(&integer_neighbor));

        let mut greater = ComparisonExpr::gt("value", Value::Float(EXACT as f64));
        greater.prepare_for_schema(&integer_schema);
        assert!(greater.evaluate(&integer_neighbor).unwrap());
        assert!(greater.evaluate_fast(&integer_neighbor));

        let float_schema = SchemaBuilder::new("float_boundary")
            .add("value", DataType::Float)
            .build();
        let rounded_float = Row::from_values(vec![Value::Float(EXACT as f64)]);
        let mut less = ComparisonExpr::lt("value", Value::Integer(EXACT + 1));
        less.prepare_for_schema(&float_schema);
        assert!(less.evaluate(&rounded_float).unwrap());
        assert!(less.evaluate_fast(&rounded_float));
    }

    #[test]
    fn test_cross_decimal_integer_float_uses_canonical_numeric_identity() {
        let integer_schema = SchemaBuilder::new("integer_decimal")
            .add("value", DataType::Integer)
            .build();
        let integer_row = Row::from_values(vec![Value::Integer(1)]);

        let mut equal_fraction = ComparisonExpr::eq("value", Value::decimal(15, 2, 1));
        equal_fraction.prepare_for_schema(&integer_schema);
        assert!(!equal_fraction.evaluate(&integer_row).unwrap());
        assert!(!equal_fraction.evaluate_fast(&integer_row));

        let mut less_fraction = ComparisonExpr::lt("value", Value::decimal(15, 2, 1));
        less_fraction.prepare_for_schema(&integer_schema);
        assert!(less_fraction.evaluate(&integer_row).unwrap());
        assert!(less_fraction.evaluate_fast(&integer_row));

        const EXACT: i64 = 1_i64 << 53;
        let float_schema = SchemaBuilder::new("float_decimal")
            .add("value", DataType::Float)
            .build();
        let rounded_float = Row::from_values(vec![Value::Float(EXACT as f64)]);
        let mut less_exact_neighbor =
            ComparisonExpr::lt("value", Value::decimal(i128::from(EXACT + 1), 16, 0));
        less_exact_neighbor.prepare_for_schema(&float_schema);
        assert!(less_exact_neighbor.evaluate(&rounded_float).unwrap());
        assert!(less_exact_neighbor.evaluate_fast(&rounded_float));
    }

    #[test]
    fn test_same_float_nan_uses_canonical_comparison_contract() {
        let schema = SchemaBuilder::new("float_nan")
            .add("value", DataType::Float)
            .build();
        let row = Row::from_values(vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))]);
        let other_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0042));

        for (operator, expected) in [
            (Operator::Eq, true),
            (Operator::Ne, false),
            (Operator::Gt, false),
            (Operator::Gte, true),
            (Operator::Lt, false),
            (Operator::Lte, true),
        ] {
            let mut expression = ComparisonExpr::new("value", operator, other_nan.clone());
            expression.prepare_for_schema(&schema);
            assert_eq!(expression.evaluate(&row).unwrap(), expected, "{operator:?}");
            assert_eq!(
                expression.evaluate_fast(&row),
                expected,
                "fast {operator:?}"
            );
        }

        let finite = Row::from_values(vec![Value::Float(1.0)]);
        let mut less = ComparisonExpr::lt("value", other_nan);
        less.prepare_for_schema(&schema);
        assert!(less.evaluate(&finite).unwrap());
        assert!(less.evaluate_fast(&finite));
    }

    #[test]
    fn test_decimal_all_comparison_operators_and_scales() {
        let schema = SchemaBuilder::new("decimal_test")
            .add("amount", DataType::Decimal)
            .build();
        let row = Row::from_values(vec![Value::decimal(12_345, 5, 2)]);

        let cases = [
            (Operator::Eq, Value::decimal(1_234_500, 7, 4), true),
            (Operator::Ne, Value::decimal(12_344, 5, 2), true),
            (Operator::Gt, Value::decimal(12_344, 5, 2), true),
            (Operator::Gte, Value::decimal(12_345, 5, 2), true),
            (Operator::Lt, Value::decimal(12_346, 5, 2), true),
            (Operator::Lte, Value::decimal(12_345, 5, 2), true),
        ];

        for (operator, value, expected) in cases {
            let mut expr = ComparisonExpr::new("amount", operator, value);
            expr.prepare_for_schema(&schema);
            assert_eq!(expr.evaluate(&row).unwrap(), expected);
            assert_eq!(expr.evaluate_fast(&row), expected);
        }
    }

    #[test]
    fn test_decimal_preparation_coerces_integer_and_float_literals() {
        let schema = SchemaBuilder::new("decimal_test")
            .add_nullable("amount", DataType::Decimal)
            .build();
        let decimal_row = Row::from_values(vec![Value::decimal(5_050, 4, 2)]);

        let mut integer = ComparisonExpr::gt("amount", Value::integer(50));
        integer.prepare_for_schema(&schema);
        assert!(matches!(integer.value(), ComparisonValue::Decimal { .. }));
        assert!(integer.evaluate(&decimal_row).unwrap());

        let mut float = ComparisonExpr::eq("amount", Value::float(50.5));
        float.prepare_for_schema(&schema);
        assert!(matches!(float.value(), ComparisonValue::Decimal { .. }));
        assert!(float.evaluate(&decimal_row).unwrap());

        let null_row = Row::from_values(vec![Value::null(DataType::Decimal)]);
        assert!(!float.evaluate(&null_row).unwrap());
        assert!(float.is_unknown_due_to_null(&null_row));
    }

    #[test]
    fn test_unprepared_expression() {
        let row = test_row();

        let expr = ComparisonExpr::eq("id", Value::integer(1));
        // Not prepared - should return false
        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));
    }

    #[test]
    fn test_with_aliases() {
        let schema = test_schema();
        let row = test_row();

        let mut aliases = FxHashMap::default();
        aliases.insert("n".to_string(), "name".to_string());

        let expr = ComparisonExpr::eq("n", Value::text("Alice"));
        let mut aliased = expr.with_aliases(&aliases);
        aliased.prepare_for_schema(&schema);

        assert!(aliased.evaluate(&row).unwrap());
    }

    #[test]
    fn test_can_use_index() {
        let expr = ComparisonExpr::eq("id", Value::integer(1));
        assert!(expr.can_use_index());

        let expr = ComparisonExpr::gt("id", Value::integer(1));
        assert!(expr.can_use_index());
    }
}
