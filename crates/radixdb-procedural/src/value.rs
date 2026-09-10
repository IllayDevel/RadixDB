use std::mem::size_of;

use radixdb_catalog::{CatalogDataType, CatalogName};
use radixdb_core::{DataType, Value};

use crate::{Diagnostic, DiagnosticKind, ProceduralResult};

pub const MAX_RECORD_FIELDS: usize = 4096;
pub const MAX_COLLECTION_CAPACITY: u32 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordField {
    name: CatalogName,
    data_type: CatalogDataType,
    nullable: bool,
}

impl RecordField {
    pub const fn new(name: CatalogName, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }

    pub fn name(&self) -> &CatalogName {
        &self.name
    }
    pub const fn data_type(&self) -> CatalogDataType {
        self.data_type
    }
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeType {
    Scalar {
        data_type: CatalogDataType,
        nullable: bool,
    },
    Record {
        fields: Vec<RecordField>,
        nullable: bool,
    },
    Collection {
        element_type: CatalogDataType,
        capacity: u32,
    },
    /// Canonically quoted SQL identifier fragment. This is deliberately not
    /// a SQL scalar and can only be consumed by explicit dynamic-SQL text
    /// composition.
    SqlIdentifier,
}

impl RuntimeType {
    pub const fn scalar(data_type: CatalogDataType, nullable: bool) -> Self {
        Self::Scalar {
            data_type,
            nullable,
        }
    }

    pub fn record(fields: Vec<RecordField>) -> ProceduralResult<Self> {
        Self::record_with_nullability(fields, false)
    }

    pub fn nullable_record(fields: Vec<RecordField>) -> ProceduralResult<Self> {
        Self::record_with_nullability(fields, true)
    }

    fn record_with_nullability(fields: Vec<RecordField>, nullable: bool) -> ProceduralResult<Self> {
        if fields.is_empty() || fields.len() > MAX_RECORD_FIELDS {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "record field count is outside the admitted range",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        if fields
            .iter()
            .any(|field| !names.insert(field.name().normalized().as_str().to_owned()))
        {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "record field names are not unique",
            ));
        }
        Ok(Self::Record { fields, nullable })
    }

    pub fn collection(element_type: CatalogDataType, capacity: u32) -> ProceduralResult<Self> {
        if !(1..=MAX_COLLECTION_CAPACITY).contains(&capacity) {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "collection capacity is outside 1..=65536",
            ));
        }
        Ok(Self::Collection {
            element_type,
            capacity,
        })
    }

    pub fn validate(&self) -> ProceduralResult<()> {
        match self {
            Self::Scalar { .. } => Ok(()),
            Self::Record { fields, .. } => {
                if fields.is_empty() || fields.len() > MAX_RECORD_FIELDS {
                    return Err(Diagnostic::new(
                        DiagnosticKind::RuntimeInvalidIr,
                        "record field count is outside the admitted range",
                    ));
                }
                let mut names = std::collections::BTreeSet::new();
                if fields
                    .iter()
                    .any(|field| !names.insert(field.name().normalized().as_str()))
                {
                    return Err(Diagnostic::new(
                        DiagnosticKind::RuntimeInvalidIr,
                        "record field names are not unique",
                    ));
                }
                Ok(())
            }
            Self::Collection { capacity, .. }
                if (1..=MAX_COLLECTION_CAPACITY).contains(capacity) =>
            {
                Ok(())
            }
            Self::Collection { .. } => Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "collection capacity is outside 1..=65536",
            )),
            Self::SqlIdentifier => Ok(()),
        }
    }

    pub fn accepts(&self, value: &RuntimeValue) -> bool {
        match (self, value) {
            (
                Self::Scalar {
                    data_type,
                    nullable,
                },
                RuntimeValue::Scalar(value),
            ) => scalar_accepts(*data_type, *nullable, value),
            (Self::Record { fields, .. }, RuntimeValue::Record(values)) => {
                fields.len() == values.len()
                    && fields.iter().zip(values).all(|(field, value)| {
                        value.as_ref().is_none_or(|value| {
                            scalar_accepts(field.data_type(), field.nullable(), value)
                        })
                    })
            }
            (Self::Record { nullable: true, .. }, RuntimeValue::NullRecord) => true,
            (
                Self::Collection {
                    element_type,
                    capacity,
                },
                RuntimeValue::Collection(values),
            ) => {
                values.len() <= *capacity as usize
                    && values
                        .iter()
                        .all(|value| scalar_accepts(*element_type, true, value))
            }
            (Self::SqlIdentifier, RuntimeValue::SqlIdentifier(_)) => true,
            _ => false,
        }
    }

    pub fn null_value(&self) -> ProceduralResult<RuntimeValue> {
        match self {
            Self::Scalar {
                data_type,
                nullable: true,
            } => Ok(RuntimeValue::Scalar(Value::null(data_type.logical_type()))),
            Self::Scalar {
                nullable: false, ..
            } => Err(Diagnostic::new(
                DiagnosticKind::RuntimeNullNotAllowed,
                "NULL cannot be assigned to a NOT NULL slot",
            )),
            // A declared row starts with independently uninitialized fields.
            // This is deliberately distinct from assigning SQL NULL to a
            // NOT NULL field: field mutation still validates the descriptor.
            Self::Record { nullable: true, .. } => Ok(RuntimeValue::NullRecord),
            Self::Record {
                fields,
                nullable: false,
            } => Ok(RuntimeValue::Record(vec![None; fields.len()])),
            Self::Collection { .. } => Ok(RuntimeValue::Collection(Vec::new())),
            Self::SqlIdentifier => Err(Diagnostic::new(
                DiagnosticKind::RuntimeNullNotAllowed,
                "SQL identifier fragments cannot be NULL",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeValue {
    Scalar(Value),
    /// `None` is an uninitialized local field, not a SQL NULL value.
    Record(Vec<Option<Value>>),
    /// SQL NULL for a nullable record contract, used by trigger suppression.
    NullRecord,
    Collection(Vec<Value>),
    SqlIdentifier(String),
}

impl RuntimeValue {
    pub const fn scalar(value: Value) -> Self {
        Self::Scalar(value)
    }

    pub fn owned_bytes(&self) -> u64 {
        match self {
            Self::Scalar(value) => scalar_owned_bytes(value),
            Self::Record(values) => {
                let container = values.capacity().saturating_mul(size_of::<Option<Value>>()) as u64;
                values.iter().flatten().fold(container, |total, value| {
                    total.saturating_add(scalar_owned_bytes(value))
                })
            }
            Self::NullRecord => 0,
            Self::Collection(values) => {
                let container = values.capacity().saturating_mul(size_of::<Value>()) as u64;
                values.iter().fold(container, |total, value| {
                    total.saturating_add(scalar_owned_bytes(value))
                })
            }
            Self::SqlIdentifier(value) => value.len() as u64,
        }
    }
}

pub(crate) fn scalar_accepts(expected: CatalogDataType, nullable: bool, value: &Value) -> bool {
    if value.is_null() {
        return nullable
            && (matches!(value.data_type(), DataType::Null)
                || value.data_type() == expected.logical_type());
    }
    value.validate_shape().is_ok() && value.data_type() == expected.logical_type()
}

pub(crate) fn scalar_owned_bytes(value: &Value) -> u64 {
    match value {
        Value::Text(text) => text.len() as u64,
        Value::Extension(bytes) => bytes.len() as u64,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_capacity_is_part_of_runtime_admission() {
        let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
        assert!(RuntimeType::collection(integer, 0).is_err());
        assert!(RuntimeType::collection(integer, MAX_COLLECTION_CAPACITY).is_ok());
        assert!(RuntimeType::collection(integer, MAX_COLLECTION_CAPACITY + 1).is_err());
    }

    #[test]
    fn exact_scalar_type_and_nullability_are_enforced() {
        let integer =
            RuntimeType::scalar(CatalogDataType::scalar(DataType::Integer).unwrap(), false);
        assert!(integer.accepts(&RuntimeValue::scalar(Value::Integer(1))));
        assert!(!integer.accepts(&RuntimeValue::scalar(Value::Float(1.0))));
        assert!(!integer.accepts(&RuntimeValue::scalar(Value::null(DataType::Integer))));
    }

    #[test]
    fn direct_enum_construction_cannot_bypass_type_validation() {
        let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
        assert!(RuntimeType::Collection {
            element_type: integer,
            capacity: 0,
        }
        .validate()
        .is_err());
        assert!(RuntimeType::Record {
            fields: Vec::new(),
            nullable: false,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn declared_record_fields_start_uninitialized_without_losing_constraints() {
        let record = RuntimeType::record(vec![RecordField::new(
            CatalogName::new("id").unwrap(),
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        )])
        .unwrap();
        let initial = record.null_value().unwrap();
        assert_eq!(initial, RuntimeValue::Record(vec![None]));
        assert!(record.accepts(&initial));
        assert!(!record.accepts(&RuntimeValue::Record(vec![Some(Value::null(
            DataType::Integer,
        ))])));
    }
}
