use std::collections::BTreeMap;
use std::fmt::Write;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_DESCRIPTOR_VERSION: &str = "radixdb.schema.v1";

#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    #[error("unsupported schema descriptor version '{0}'")]
    UnsupportedVersion(String),
    #[error("schema descriptor kind mismatch: expected {expected:?}, got {actual:?}")]
    KindMismatch {
        expected: DescriptorKind,
        actual: DescriptorKind,
    },
    #[error("schema descriptor JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorKind {
    Table,
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorEnvelope<T> {
    pub descriptor: String,
    pub kind: DescriptorKind,
    pub payload: T,
}

impl<T> DescriptorEnvelope<T>
where
    T: Serialize,
{
    pub fn new(kind: DescriptorKind, payload: T) -> Self {
        Self {
            descriptor: SCHEMA_DESCRIPTOR_VERSION.to_string(),
            kind,
            payload,
        }
    }

    pub fn to_json(&self) -> Result<String, DescriptorError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn to_pretty_json(&self) -> Result<String, DescriptorError> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl<T> DescriptorEnvelope<T>
where
    T: for<'de> Deserialize<'de>,
{
    pub fn from_json(json: &str, expected: DescriptorKind) -> Result<Self, DescriptorError> {
        let envelope: Self = serde_json::from_str(json)?;
        if envelope.descriptor != SCHEMA_DESCRIPTOR_VERSION {
            return Err(DescriptorError::UnsupportedVersion(envelope.descriptor));
        }
        if envelope.kind != expected {
            return Err(DescriptorError::KindMismatch {
                expected,
                actual: envelope.kind,
            });
        }
        Ok(envelope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    pub schema_generation: u64,
    pub fingerprint: String,
    pub tables: Vec<TableDescriptor>,
    pub views: Vec<ViewDescriptor>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub catalog_id: String,
    pub name: String,
    pub schema_generation: u64,
    pub fingerprint: String,
    pub created_at: String,
    pub updated_at: String,
    pub columns: Vec<ColumnDescriptor>,
    pub constraints: Vec<ConstraintDescriptor>,
    pub indexes: Vec<IndexDescriptor>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub ordinal: u32,
    pub name: String,
    pub data_type: DataTypeDescriptor,
    pub nullable: bool,
    pub auto_increment: bool,
    pub default_expression: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataTypeDescriptor {
    Null,
    Integer,
    Float,
    Text,
    Boolean,
    Timestamp,
    Date,
    Json,
    Uuid,
    Bytes,
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Vector {
        dimensions: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintDescriptor {
    pub id: u64,
    pub name: String,
    #[serde(flatten)]
    pub definition: ConstraintDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "constraint_type", rename_all = "snake_case")]
pub enum ConstraintDefinition {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
        owned_index: String,
    },
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: ForeignKeyActionDescriptor,
        on_update: ForeignKeyActionDescriptor,
    },
    Check {
        column: Option<String>,
        expression: String,
        ordinal: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyActionDescriptor {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub name: String,
    pub method: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub predicate: Option<String>,
    pub options: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDescriptor {
    pub name: String,
    pub query: String,
    pub dependencies: Vec<String>,
    pub result_columns: Vec<ResultColumnDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultColumnDescriptor {
    pub name: String,
    pub data_type: DataTypeDescriptor,
    pub nullable: bool,
}

impl TableDescriptor {
    pub fn to_json(&self) -> Result<String, DescriptorError> {
        DescriptorEnvelope::new(DescriptorKind::Table, self.clone()).to_json()
    }

    pub fn from_json(json: &str) -> Result<Self, DescriptorError> {
        Ok(DescriptorEnvelope::<Self>::from_json(json, DescriptorKind::Table)?.payload)
    }

    pub fn form_descriptor(&self) -> crate::TableFormDescriptor {
        crate::TableFormDescriptor::from_table(self)
    }

    /// Compute the schema fingerprint with the fingerprint field itself blank.
    pub fn computed_fingerprint(&self) -> Result<String, DescriptorError> {
        let mut canonical = self.clone();
        canonical.fingerprint.clear();
        canonical_fingerprint(&canonical)
    }

    pub fn refresh_fingerprint(&mut self) -> Result<(), DescriptorError> {
        self.fingerprint = self.computed_fingerprint()?;
        Ok(())
    }
}

impl DatabaseDescriptor {
    pub fn to_json(&self) -> Result<String, DescriptorError> {
        DescriptorEnvelope::new(DescriptorKind::Database, self.clone()).to_json()
    }

    pub fn from_json(json: &str) -> Result<Self, DescriptorError> {
        Ok(DescriptorEnvelope::<Self>::from_json(json, DescriptorKind::Database)?.payload)
    }

    /// Compute the database fingerprint from its ordered, already-fingerprinted
    /// table/view catalog while excluding the fingerprint field itself.
    pub fn computed_fingerprint(&self) -> Result<String, DescriptorError> {
        let mut canonical = self.clone();
        canonical.fingerprint.clear();
        canonical_fingerprint(&canonical)
    }

    pub fn refresh_fingerprint(&mut self) -> Result<(), DescriptorError> {
        self.fingerprint = self.computed_fingerprint()?;
        Ok(())
    }
}

pub fn canonical_fingerprint<T: Serialize>(value: &T) -> Result<String, DescriptorError> {
    let encoded = serde_json::to_vec(value)?;
    let digest = Sha256::digest(encoded);
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut fingerprint, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(fingerprint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_envelope_round_trip_is_versioned_and_canonical() {
        let descriptor = TableDescriptor {
            catalog_id: "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e01".to_string(),
            name: "people".to_string(),
            schema_generation: 7,
            fingerprint: "abc".to_string(),
            created_at: "2026-08-21T00:00:00Z".to_string(),
            updated_at: "2026-08-21T00:00:00Z".to_string(),
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
            extensions: BTreeMap::new(),
        };
        let envelope = DescriptorEnvelope::new(DescriptorKind::Table, descriptor.clone());
        let json = envelope.to_json().unwrap();
        assert_eq!(envelope.to_json().unwrap(), json);
        let decoded =
            DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table).unwrap();
        assert_eq!(decoded.payload, descriptor);
        assert_eq!(
            TableDescriptor::from_json(&descriptor.to_json().unwrap()).unwrap(),
            descriptor
        );
        assert!(
            DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Database)
                .is_err()
        );
    }
}
