use crate::payload::common::{validate_flags, validate_version};
use crate::{CatalogError, CatalogResult, ObjectId};

pub const MAX_EXTERNAL_TYPE_LOCAL_ID_BYTES: usize = 255;
pub const MAX_EXTERNAL_VALUE_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ExternalStorageKind {
    Fixed = 1,
    Variable = 2,
}

impl ExternalStorageKind {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for ExternalStorageKind {
    type Error = CatalogError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Fixed),
            2 => Ok(Self::Variable),
            tag => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "external type storage kind",
                tag,
            }),
        }
    }
}

/// Durable metadata for one package-defined scalar type.
///
/// Callback pointers and platform paths are resolved only from the immutable
/// process registry and never enter this payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTypePayload {
    extension_binding_id: ObjectId,
    local_id: String,
    write_codec_version: u32,
    semantic_revision: u32,
    storage_kind: ExternalStorageKind,
    fixed_bytes: Option<u32>,
    max_canonical_payload_bytes: u32,
    codec_fingerprint: [u8; 32],
    capabilities: u64,
}

impl ExternalTypePayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        extension_binding_id: ObjectId,
        local_id: impl Into<String>,
        write_codec_version: u32,
        semantic_revision: u32,
        storage_kind: ExternalStorageKind,
        fixed_bytes: Option<u32>,
        max_canonical_payload_bytes: u32,
        codec_fingerprint: [u8; 32],
        capabilities: u64,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            extension_binding_id,
            local_id.into(),
            write_codec_version,
            semantic_revision,
            storage_kind,
            fixed_bytes,
            max_canonical_payload_bytes,
            codec_fingerprint,
            capabilities,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        payload_version: u16,
        flags: u64,
        extension_binding_id: ObjectId,
        local_id: String,
        write_codec_version: u32,
        semantic_revision: u32,
        storage_kind: ExternalStorageKind,
        fixed_bytes: Option<u32>,
        max_canonical_payload_bytes: u32,
        codec_fingerprint: [u8; 32],
        capabilities: u64,
    ) -> CatalogResult<Self> {
        validate_version("external type", payload_version)?;
        validate_flags("external type", flags)?;
        if local_id.is_empty()
            || local_id.len() > MAX_EXTERNAL_TYPE_LOCAL_ID_BYTES
            || local_id.contains('\0')
        {
            return Err(CatalogError::InvalidExternalTypePayload {
                detail: "local id must be 1..=255 UTF-8 bytes without NUL",
            });
        }
        if write_codec_version == 0 {
            return Err(CatalogError::InvalidExternalTypePayload {
                detail: "write codec version must be at least one",
            });
        }
        if semantic_revision == 0 {
            return Err(CatalogError::InvalidExternalTypePayload {
                detail: "semantic revision must be at least one",
            });
        }
        if !(1..=MAX_EXTERNAL_VALUE_BYTES).contains(&max_canonical_payload_bytes) {
            return Err(CatalogError::InvalidExternalTypePayload {
                detail: "maximum canonical payload is outside 1..=16 MiB",
            });
        }
        match (storage_kind, fixed_bytes) {
            (ExternalStorageKind::Fixed, Some(bytes))
                if bytes > 0 && bytes <= max_canonical_payload_bytes => {}
            (ExternalStorageKind::Variable, None) => {}
            (ExternalStorageKind::Fixed, _) => {
                return Err(CatalogError::InvalidExternalTypePayload {
                    detail: "fixed storage requires fixed bytes inside payload bound",
                });
            }
            (ExternalStorageKind::Variable, Some(_)) => {
                return Err(CatalogError::InvalidExternalTypePayload {
                    detail: "variable storage must not declare fixed bytes",
                });
            }
        }
        Ok(Self {
            extension_binding_id,
            local_id,
            write_codec_version,
            semantic_revision,
            storage_kind,
            fixed_bytes,
            max_canonical_payload_bytes,
            codec_fingerprint,
            capabilities,
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
    pub const fn write_codec_version(&self) -> u32 {
        self.write_codec_version
    }
    pub const fn semantic_revision(&self) -> u32 {
        self.semantic_revision
    }
    pub const fn storage_kind(&self) -> ExternalStorageKind {
        self.storage_kind
    }
    pub const fn fixed_bytes(&self) -> Option<u32> {
        self.fixed_bytes
    }
    pub const fn max_canonical_payload_bytes(&self) -> u32 {
        self.max_canonical_payload_bytes
    }
    pub const fn codec_fingerprint(&self) -> &[u8; 32] {
        &self.codec_fingerprint
    }
    pub const fn capabilities(&self) -> u64 {
        self.capabilities
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extension_id() -> ObjectId {
        ObjectId::from_user_bytes([8; 16]).unwrap()
    }

    #[test]
    fn fixed_and_variable_contracts_are_exact() {
        let fixed = ExternalTypePayload::new(
            extension_id(),
            "point",
            1,
            1,
            ExternalStorageKind::Fixed,
            Some(16),
            16,
            [3; 32],
            7,
        )
        .unwrap();
        assert_eq!(fixed.fixed_bytes(), Some(16));
        assert_eq!(fixed.storage_kind(), ExternalStorageKind::Fixed);

        let variable = ExternalTypePayload::new(
            extension_id(),
            "polygon",
            2,
            3,
            ExternalStorageKind::Variable,
            None,
            4096,
            [4; 32],
            0,
        )
        .unwrap();
        assert_eq!(variable.fixed_bytes(), None);
        assert_eq!(variable.max_canonical_payload_bytes(), 4096);
    }

    #[test]
    fn malformed_bounds_fail_closed() {
        for (kind, fixed, max) in [
            (ExternalStorageKind::Fixed, None, 16),
            (ExternalStorageKind::Fixed, Some(17), 16),
            (ExternalStorageKind::Variable, Some(16), 16),
            (ExternalStorageKind::Variable, None, 0),
        ] {
            assert!(ExternalTypePayload::new(
                extension_id(),
                "point",
                1,
                1,
                kind,
                fixed,
                max,
                [3; 32],
                0,
            )
            .is_err());
        }
    }
}
