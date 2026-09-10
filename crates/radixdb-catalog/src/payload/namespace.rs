use crate::payload::common::{validate_flags, validate_version};
use crate::CatalogResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NamespacePayload;

impl NamespacePayload {
    pub const fn new() -> Self {
        Self
    }

    pub fn from_fields(version: u16, flags: u64) -> CatalogResult<Self> {
        validate_version("namespace", version)?;
        validate_flags("namespace", flags)?;
        Ok(Self)
    }

    pub const fn version(self) -> u16 {
        super::PAYLOAD_VERSION
    }

    pub const fn flags(self) -> u64 {
        0
    }
}
