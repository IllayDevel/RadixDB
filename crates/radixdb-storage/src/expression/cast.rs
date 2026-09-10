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

//! CAST expression for RadixDB
//!

use rustc_hash::FxHashMap;

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{DataType, Operator, Result, Row, Schema, SmartString, Value};

/// CAST expression (CAST(column AS type))
///

#[derive(Debug, Clone)]
pub struct CastExpr {
    /// Column name to cast
    column: String,
    /// Target data type
    target_type: DataType,

    /// Pre-computed column index
    col_index: Option<usize>,

    /// Column aliases
    aliases: FxHashMap<String, String>,
    /// Original column name if using alias
    original_column: Option<String>,
}

impl CastExpr {
    /// Create a new CAST expression
    pub fn new(column: impl Into<String>, target_type: DataType) -> Self {
        Self {
            column: column.into(),
            target_type,
            col_index: None,
            aliases: FxHashMap::default(),
            original_column: None,
        }
    }

    /// Get the target type
    pub fn target_type(&self) -> DataType {
        self.target_type
    }

    /// Perform the cast operation on a value
    pub fn perform_cast(&self, value: &Value) -> Result<Value> {
        if value.is_null() {
            return Ok(Value::null(self.target_type));
        }

        match self.target_type {
            DataType::Integer => cast_to_integer(value),
            DataType::Float => cast_to_float(value),
            DataType::Text => cast_to_string(value),
            DataType::Boolean => cast_to_boolean(value),
            DataType::Timestamp => cast_to_timestamp(value),
            DataType::Json => cast_to_json(value),
            DataType::Uuid => cast_to_uuid(value),
            DataType::Decimal | DataType::Date | DataType::Bytes => {
                value.try_coerce_to_type(self.target_type)
            }
            DataType::Vector => Err(radixdb_core::Error::type_conversion(
                format!("{:?}", value),
                "VECTOR",
            )),
            DataType::Null => Ok(Value::null(DataType::Null)),
        }
    }
}

impl Expression for CastExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, row: &Row) -> Result<bool> {
        let col_idx = match self.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return Ok(false),
        };

        let col_value = &row[col_idx];

        if col_value.is_null() {
            return Ok(false);
        }

        // Perform the cast - if it succeeds, return true
        // (CAST by itself doesn't filter, parent expression handles comparison)
        self.perform_cast(col_value).map(|_| true)
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

        self.perform_cast(col_value).is_ok()
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

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        self.col_index
            .and_then(|index| row.get(index))
            .is_some_and(Value::is_null)
    }
}

/// Compound expression for CAST with comparison
///
/// This handles WHERE clauses like: WHERE CAST(column AS INTEGER) > 100
#[derive(Debug, Clone)]
pub struct CompoundExpr {
    /// The CAST expression
    cast_expr: CastExpr,
    /// The comparison operator
    operator: Operator,
    /// The value to compare against
    value: Value,

    /// Whether prepared for schema
    is_optimized: bool,
}

impl CompoundExpr {
    /// Create a new compound expression
    pub fn new(cast_expr: CastExpr, operator: Operator, value: Value) -> Self {
        Self {
            cast_expr,
            operator,
            value,
            is_optimized: false,
        }
    }

    /// Get the operator
    pub fn operator(&self) -> Operator {
        self.operator
    }

    /// Get the comparison value
    pub fn comparison_value(&self) -> &Value {
        &self.value
    }
}

impl Expression for CompoundExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        let col_idx = match self.cast_expr.col_index {
            Some(idx) if idx < row.len() => idx,
            _ => return Ok(false),
        };

        let col_value = &row[col_idx];

        if col_value.is_null() {
            return Ok(false);
        }

        // Cast the column value
        let casted = self.cast_expr.perform_cast(col_value)?;

        // Convert comparison value to target type if needed
        let comp_value = self.cast_expr.perform_cast(&self.value)?;

        if casted.is_null() || comp_value.is_null() {
            return Ok(false);
        }

        let cmp = casted.compare(&comp_value)?;

        let result = match self.operator {
            Operator::Eq => cmp == std::cmp::Ordering::Equal,
            Operator::Ne => cmp != std::cmp::Ordering::Equal,
            Operator::Gt => cmp == std::cmp::Ordering::Greater,
            Operator::Gte => cmp != std::cmp::Ordering::Less,
            Operator::Lt => cmp == std::cmp::Ordering::Less,
            Operator::Lte => cmp != std::cmp::Ordering::Greater,
            _ => false,
        };

        Ok(result)
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        self.evaluate(row).unwrap_or(false)
    }

    fn with_aliases(&self, aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        let aliased_cast = self.cast_expr.with_aliases(aliases);
        let cast_expr = if let Some(cast) = aliased_cast.as_any().downcast_ref::<CastExpr>() {
            cast.clone()
        } else {
            self.cast_expr.clone()
        };

        Box::new(CompoundExpr {
            cast_expr,
            operator: self.operator,
            value: self.value.clone(),
            is_optimized: false,
        })
    }

    fn prepare_for_schema(&mut self, schema: &Schema) {
        if self.is_optimized {
            return;
        }
        self.cast_expr.prepare_for_schema(schema);
        self.is_optimized = true;
    }

    fn collect_column_indices(&self, out: &mut Vec<usize>) -> bool {
        self.cast_expr.collect_column_indices(out)
    }

    fn is_prepared(&self) -> bool {
        self.is_optimized
    }

    fn get_column_name(&self) -> Option<&str> {
        self.cast_expr.get_column_name()
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        self.value.is_null()
            || self
                .cast_expr
                .col_index
                .and_then(|index| row.get(index))
                .is_some_and(Value::is_null)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Cast helper functions

fn cast_to_integer(value: &Value) -> Result<Value> {
    value.try_coerce_to_type(DataType::Integer)
}

fn cast_to_float(value: &Value) -> Result<Value> {
    value.try_coerce_to_type(DataType::Float)
}

fn cast_to_string(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Text(SmartString::from_string(v.to_string()))),
        Value::Float(v) => Ok(Value::Text(SmartString::from_string(v.to_string()))),
        Value::Text(s) => Ok(Value::Text(s.clone())),
        Value::Boolean(b) => Ok(Value::Text(SmartString::from(if *b {
            "true"
        } else {
            "false"
        }))),
        Value::Timestamp(t) => Ok(Value::Text(SmartString::from_string(t.to_rfc3339()))),
        Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
            let s = std::str::from_utf8(&data[1..]).unwrap_or("");
            Ok(Value::Text(SmartString::from(s)))
        }
        Value::Extension(_) => Ok(value
            .as_string()
            .map(|s| Value::Text(SmartString::from_string(s)))
            .unwrap_or_else(|| Value::Text(SmartString::from("")))),
        Value::Null(_) => Ok(Value::null(DataType::Text)),
    }
}

fn cast_to_boolean(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Boolean(*v != 0)),
        Value::Float(v) => Ok(Value::Boolean(*v != 0.0)),
        Value::Text(s) => {
            // Use case-insensitive comparison to avoid allocation
            let b = s.eq_ignore_ascii_case("true")
                || s == "1"
                || s.eq_ignore_ascii_case("t")
                || s.eq_ignore_ascii_case("yes")
                || s.eq_ignore_ascii_case("y");
            Ok(Value::Boolean(b))
        }
        Value::Boolean(b) => Ok(Value::Boolean(*b)),
        Value::Null(_) => Ok(Value::null(DataType::Boolean)),
        _ => Ok(Value::Boolean(false)),
    }
}

fn cast_to_timestamp(value: &Value) -> Result<Value> {
    value.try_coerce_to_type(DataType::Timestamp)
}

fn cast_to_json(value: &Value) -> Result<Value> {
    value.try_coerce_to_type(DataType::Json)
}

fn cast_to_uuid(value: &Value) -> Result<Value> {
    match value {
        Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => {
            if data.len() == 17 {
                Ok(value.clone())
            } else {
                Err(radixdb_core::Error::type_conversion(
                    format!("{:?}", value),
                    "UUID",
                ))
            }
        }
        Value::Text(s) => radixdb_core::value::parse_uuid_str(s.as_ref())
            .map(Value::uuid)
            .ok_or_else(|| radixdb_core::Error::type_conversion(s.to_string(), "UUID")),
        Value::Null(_) => Ok(Value::null(DataType::Uuid)),
        _ => Err(radixdb_core::Error::type_conversion(
            format!("{:?}", value),
            "UUID",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::SchemaBuilder;

    fn test_schema() -> Schema {
        SchemaBuilder::new("test")
            .add_primary_key("id", DataType::Integer)
            .add("value", DataType::Text)
            .add("score", DataType::Float)
            .build()
    }

    #[test]
    fn test_cast_to_integer() {
        let result = cast_to_integer(&Value::text("42")).unwrap();
        assert_eq!(result, Value::integer(42));

        let result = cast_to_integer(&Value::float(3.5)).unwrap();
        assert_eq!(result, Value::integer(3));

        let result = cast_to_integer(&Value::Boolean(true)).unwrap();
        assert_eq!(result, Value::integer(1));
    }

    #[test]
    fn test_cast_to_float() {
        let result = cast_to_float(&Value::text("3.5")).unwrap();
        assert_eq!(result, Value::float(3.5));

        let result = cast_to_float(&Value::integer(42)).unwrap();
        assert_eq!(result, Value::float(42.0));
    }

    #[test]
    fn test_cast_to_string() {
        let result = cast_to_string(&Value::integer(42)).unwrap();
        assert_eq!(result, Value::text("42"));

        let result = cast_to_string(&Value::Boolean(true)).unwrap();
        assert_eq!(result, Value::text("true"));
    }

    #[test]
    fn test_cast_to_boolean() {
        let result = cast_to_boolean(&Value::text("true")).unwrap();
        assert_eq!(result, Value::Boolean(true));

        let result = cast_to_boolean(&Value::text("yes")).unwrap();
        assert_eq!(result, Value::Boolean(true));

        let result = cast_to_boolean(&Value::integer(0)).unwrap();
        assert_eq!(result, Value::Boolean(false));

        let result = cast_to_boolean(&Value::integer(1)).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_cast_expr_evaluate() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("42"),
            Value::float(3.5),
        ]);

        let mut expr = CastExpr::new("value", DataType::Integer);
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));
    }

    #[test]
    fn test_compound_expr_integer_comparison() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("42"),
            Value::float(3.5),
        ]);

        // CAST(value AS INTEGER) > 40
        let cast = CastExpr::new("value", DataType::Integer);
        let mut expr = CompoundExpr::new(cast, Operator::Gt, Value::integer(40));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
        assert!(expr.evaluate_fast(&row));

        // CAST(value AS INTEGER) < 40
        let cast = CastExpr::new("value", DataType::Integer);
        let mut expr = CompoundExpr::new(cast, Operator::Lt, Value::integer(40));
        expr.prepare_for_schema(&schema);

        assert!(!expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_compound_expr_float_comparison() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("3.14"),
            Value::float(3.5),
        ]);

        // CAST(value AS FLOAT) >= 3.0
        let cast = CastExpr::new("value", DataType::Float);
        let mut expr = CompoundExpr::new(cast, Operator::Gte, Value::float(3.0));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_compound_expr_string_comparison() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(42),
            Value::text("hello"),
            Value::float(3.5),
        ]);

        // CAST(id AS TEXT) = '42'
        let cast = CastExpr::new("id", DataType::Text);
        let mut expr = CompoundExpr::new(cast, Operator::Eq, Value::text("42"));
        expr.prepare_for_schema(&schema);

        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_cast() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::null(DataType::Text),
            Value::float(3.5),
        ]);

        let mut expr = CastExpr::new("value", DataType::Integer);
        expr.prepare_for_schema(&schema);

        // NULL values should return false
        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));
    }

    #[test]
    fn test_with_aliases() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("42"),
            Value::float(3.5),
        ]);

        let mut aliases = FxHashMap::default();
        aliases.insert("v".to_string(), "value".to_string());

        let expr = CastExpr::new("v", DataType::Integer);
        let mut aliased = expr.with_aliases(&aliases);
        aliased.prepare_for_schema(&schema);

        assert!(aliased.evaluate(&row).unwrap());
    }

    #[test]
    fn compound_dynamic_and_fast_share_canonical_comparison() {
        let schema = test_schema();
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::text("NaN"),
            Value::float(3.5),
        ]);
        let cast = CastExpr::new("value", DataType::Float);
        let mut expr = CompoundExpr::new(cast, Operator::Eq, Value::float(f64::NAN));
        expr.prepare_for_schema(&schema);
        assert!(expr.evaluate(&row).unwrap());
        assert_eq!(expr.evaluate_fast(&row), expr.evaluate(&row).unwrap());

        let null_row = Row::from_values(vec![
            Value::integer(1),
            Value::null(DataType::Text),
            Value::float(3.5),
        ]);
        assert!(expr.is_unknown_due_to_null(&null_row));
    }

    #[test]
    fn test_get_column_name() {
        let expr = CastExpr::new("id", DataType::Integer);
        assert_eq!(expr.get_column_name(), Some("id"));
    }

    #[test]
    fn test_target_type() {
        let expr = CastExpr::new("id", DataType::Integer);
        assert_eq!(expr.target_type(), DataType::Integer);
    }

    #[test]
    fn test_cast_invalid_string_to_integer() {
        assert!(cast_to_integer(&Value::text("not_a_number")).is_err());
    }

    #[test]
    fn test_cast_float_string_to_integer() {
        let result = cast_to_integer(&Value::text("3.7")).unwrap();
        assert_eq!(result, Value::integer(3)); // Truncates to integer
    }

    #[test]
    fn casts_fail_closed_instead_of_inventing_scalar_values() {
        for value in [
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(i64::MAX as f64),
        ] {
            assert!(cast_to_integer(&value).is_err());
        }

        assert!(cast_to_timestamp(&Value::text("not-a-timestamp")).is_err());
        assert!(cast_to_timestamp(&Value::Boolean(true)).is_err());
        assert!(cast_to_json(&Value::text("{broken")).is_err());

        let timestamp = cast_to_timestamp(&Value::Integer(-1)).unwrap();
        let Value::Timestamp(timestamp) = timestamp else {
            panic!("integer timestamp cast must remain typed")
        };
        assert_eq!(timestamp.timestamp(), -1);
        assert_eq!(timestamp.timestamp_subsec_nanos(), 999_999_999);
    }
}
