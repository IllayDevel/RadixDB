use std::collections::BTreeSet;

use radixdb_core::DataType;

use crate::payload::common::{validate_flags, validate_version};
use crate::{AccessMethod, CatalogDataType, CatalogError, CatalogResult, ObjectId};

pub const MAX_OPERATOR_CLASS_LOCAL_ID_BYTES: usize = 255;
pub const MAX_OPERATOR_CLASS_BINDINGS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperatorBinding {
    slot: u16,
    object_id: ObjectId,
}

impl OperatorBinding {
    pub const fn new(slot: u16, object_id: ObjectId) -> Self {
        Self { slot, object_id }
    }
    pub const fn slot(self) -> u16 {
        self.slot
    }
    pub const fn object_id(self) -> ObjectId {
        self.object_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorClassPayload {
    extension_binding_id: ObjectId,
    local_id: String,
    semantic_revision: u32,
    access_method: AccessMethod,
    input_type: CatalogDataType,
    key_type: CatalogDataType,
    strategies: Vec<OperatorBinding>,
    supports: Vec<OperatorBinding>,
    key_codec_revision: u32,
    fingerprint: [u8; 32],
}

impl OperatorClassPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extension_binding_id: ObjectId,
        local_id: impl Into<String>,
        semantic_revision: u32,
        access_method: AccessMethod,
        input_type: CatalogDataType,
        key_type: CatalogDataType,
        strategies: Vec<OperatorBinding>,
        supports: Vec<OperatorBinding>,
        key_codec_revision: u32,
        fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            extension_binding_id,
            local_id.into(),
            semantic_revision,
            access_method,
            input_type,
            key_type,
            strategies,
            supports,
            key_codec_revision,
            fingerprint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        extension_binding_id: ObjectId,
        local_id: String,
        semantic_revision: u32,
        access_method: AccessMethod,
        input_type: CatalogDataType,
        key_type: CatalogDataType,
        strategies: Vec<OperatorBinding>,
        supports: Vec<OperatorBinding>,
        key_codec_revision: u32,
        fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        validate_version("operator class", version)?;
        validate_flags("operator class", flags)?;
        if local_id.is_empty()
            || local_id.len() > MAX_OPERATOR_CLASS_LOCAL_ID_BYTES
            || local_id.contains('\0')
        {
            return Err(CatalogError::InvalidOperatorClassPayload {
                detail: "local id must be 1..=255 UTF-8 bytes without NUL",
            });
        }
        if semantic_revision == 0 || key_codec_revision == 0 {
            return Err(CatalogError::InvalidOperatorClassPayload {
                detail: "semantic and key codec revisions must be at least one",
            });
        }
        if !input_type.is_external() {
            return Err(CatalogError::InvalidOperatorClassPayload {
                detail: "v1 operator class input must be an external type",
            });
        }
        if key_type.is_external() || !physical_key_supported(access_method, key_type.logical_type())
        {
            return Err(CatalogError::InvalidOperatorClassPayload {
                detail: "physical key must be a core scalar supported by the access method",
            });
        }
        validate_bindings("strategy", &strategies)?;
        validate_bindings("support", &supports)?;
        Ok(Self {
            extension_binding_id,
            local_id,
            semantic_revision,
            access_method,
            input_type,
            key_type,
            strategies,
            supports,
            key_codec_revision,
            fingerprint,
        })
    }

    pub const fn flags(&self) -> u64 {
        0
    }
    pub const fn extension_binding_id(&self) -> ObjectId {
        self.extension_binding_id
    }
    pub fn local_id(&self) -> &str {
        &self.local_id
    }
    pub const fn semantic_revision(&self) -> u32 {
        self.semantic_revision
    }
    pub const fn access_method(&self) -> AccessMethod {
        self.access_method
    }
    pub const fn input_type(&self) -> CatalogDataType {
        self.input_type
    }
    pub const fn key_type(&self) -> CatalogDataType {
        self.key_type
    }
    pub fn strategies(&self) -> &[OperatorBinding] {
        &self.strategies
    }
    pub fn supports(&self) -> &[OperatorBinding] {
        &self.supports
    }
    pub const fn key_codec_revision(&self) -> u32 {
        self.key_codec_revision
    }
    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

fn validate_bindings(role: &'static str, values: &[OperatorBinding]) -> CatalogResult<()> {
    if values.len() > MAX_OPERATOR_CLASS_BINDINGS {
        return Err(CatalogError::InvalidOperatorClassPayload {
            detail: "operator-class binding table exceeds 1024 entries",
        });
    }
    let mut previous = None;
    let mut ids = BTreeSet::new();
    for value in values {
        if value.slot == 0
            || previous.is_some_and(|slot| value.slot <= slot)
            || !ids.insert(value.object_id)
        {
            let _ = role;
            return Err(CatalogError::InvalidOperatorClassPayload {
                detail: "operator-class bindings must have sorted unique non-zero slots and unique object IDs",
            });
        }
        previous = Some(value.slot);
    }
    Ok(())
}

fn physical_key_supported(access_method: AccessMethod, data_type: DataType) -> bool {
    matches!(
        access_method,
        AccessMethod::Btree | AccessMethod::Hash | AccessMethod::Bitmap
    ) && matches!(
        data_type,
        DataType::Integer
            | DataType::Float
            | DataType::Boolean
            | DataType::Text
            | DataType::Timestamp
            | DataType::Uuid
            | DataType::Decimal
            | DataType::Bytes
            | DataType::Date
    )
}
