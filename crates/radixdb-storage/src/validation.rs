// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Neutral commit-time row validation contracts.

use radixdb_core::{DataType, Error, Result, Row, Schema, Value};

/// Immutable-schema validator prepared by the composition layer and reused
/// while storage certifies a batch of rows at the commit boundary.
pub trait PreparedRowValidator: Send {
    fn validate(&mut self, row: &Row) -> Result<()>;
}

/// Composition port for preparing a row validator without exposing parser,
/// compiler, registry, or VM types to storage.
pub type RowValidatorBinder = fn(&Schema) -> Result<Box<dyn PreparedRowValidator>>;

struct SchemaOnlyRowValidator {
    schema: Schema,
}

impl PreparedRowValidator for SchemaOnlyRowValidator {
    fn validate(&mut self, row: &Row) -> Result<()> {
        validate_row_shape(&self.schema, row)
    }
}

/// Default storage-only binder. Schemas containing SQL CHECK expressions need
/// an upper-layer binder; silently omitting those expressions is forbidden.
#[doc(hidden)]
pub fn bind_schema_only_row_validator(schema: &Schema) -> Result<Box<dyn PreparedRowValidator>> {
    if schema
        .columns
        .iter()
        .any(|column| column.check_expr.is_some())
        || !schema.table_checks.is_empty()
    {
        return Err(Error::NotSupported(format!(
            "table '{}' requires a configured CHECK-expression validator",
            schema.table_name
        )));
    }
    Ok(Box::new(SchemaOnlyRowValidator {
        schema: schema.clone(),
    }))
}

/// Validate the storage-owned structural and declared-type portion of a row.
/// SQL CHECK evaluation is supplied separately through [`PreparedRowValidator`].
pub fn validate_row_shape(schema: &Schema, row: &Row) -> Result<()> {
    if row.len() != schema.columns.len() {
        return Err(Error::InvalidArgument(format!(
            "row for table '{}' has {} values but schema requires {}",
            schema.table_name,
            row.len(),
            schema.columns.len()
        )));
    }

    for (index, column) in schema.columns.iter().enumerate() {
        let value = row.get(index).ok_or_else(|| {
            Error::InvalidArgument(format!("missing value for column '{}'", column.name))
        })?;
        if value.is_null() {
            if !column.nullable {
                return Err(Error::not_null_constraint(column.name.clone()));
            }
            continue;
        }
        if value.data_type() != column.data_type {
            return Err(Error::Type(format!(
                "value for column '{}' has type {:?}, expected {:?}",
                column.name,
                value.data_type(),
                column.data_type
            )));
        }
        if column.data_type == DataType::Vector && column.vector_dimensions > 0 {
            let got = match value {
                Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                    u16::try_from((data.len() - 1) / 4).unwrap_or(u16::MAX)
                }
                _ => u16::MAX,
            };
            if got != column.vector_dimensions {
                return Err(Error::VectorDimensionMismatch {
                    expected: column.vector_dimensions,
                    got,
                });
            }
        }
        column.validate_declared_value(value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::SchemaBuilder;

    #[test]
    fn schema_only_binder_is_fail_closed_for_check_expressions() {
        let mut schema = SchemaBuilder::new("items")
            .add_primary_key("id", DataType::Integer)
            .build();
        schema.table_checks.push("id > 0".to_string());
        let error = bind_schema_only_row_validator(&schema)
            .err()
            .expect("CHECK expressions require an upper binder");
        assert!(error.to_string().contains("CHECK-expression validator"));
    }

    #[test]
    fn schema_only_validator_enforces_intrinsic_shape() {
        let schema = SchemaBuilder::new("items")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .build();
        let mut validator = bind_schema_only_row_validator(&schema).unwrap();
        validator
            .validate(&Row::from_values(vec![
                Value::Integer(1),
                Value::text("one"),
            ]))
            .unwrap();
        assert!(validator
            .validate(&Row::from_values(vec![Value::Integer(1)]))
            .is_err());
    }
}
