use super::{
    CatalogGeneration, CatalogId, FormatError, FormatResult, ManifestGeneration, ManifestId,
};

pub const FORMAT_VERSION: FormatVersion = FormatVersion::new_unchecked(6, 0);
const MIN_VARIABLE_FILE_BYTES: u64 = 256 + 48;
pub const MAX_CATALOG_FILE_BYTES: u64 = radixdb_catalog::MAX_CATALOG_FILE_BYTES;
pub const MAX_MANIFEST_FILE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FormatVersion {
    major: u16,
    minor: u16,
}

impl FormatVersion {
    const fn new_unchecked(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub fn require_supported(owner: &'static str, major: u16, minor: u16) -> FormatResult<Self> {
        if (major, minor) != (6, 0) {
            return Err(FormatError::UnsupportedFormatVersion {
                owner,
                major,
                minor,
            });
        }
        Ok(FORMAT_VERSION)
    }

    pub const fn major(self) -> u16 {
        self.major
    }

    pub const fn minor(self) -> u16 {
        self.minor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogRef {
    id: CatalogId,
    generation: CatalogGeneration,
    format: FormatVersion,
    byte_length: u64,
    body_sha256: [u8; 32],
}

impl CatalogRef {
    pub fn new(
        id: CatalogId,
        generation: CatalogGeneration,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        Self::from_persisted(id, generation, 6, 0, byte_length, body_sha256)
    }

    pub fn from_persisted(
        id: CatalogId,
        generation: CatalogGeneration,
        format_major: u16,
        format_minor: u16,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        if byte_length < MIN_VARIABLE_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "catalog",
                detail: "byte length is smaller than header plus footer",
            });
        }
        if byte_length > MAX_CATALOG_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "catalog",
                detail: "byte length exceeds the V6 catalog file ceiling",
            });
        }
        Ok(Self {
            id,
            generation,
            format: FormatVersion::require_supported("catalog", format_major, format_minor)?,
            byte_length,
            body_sha256,
        })
    }

    pub const fn id(self) -> CatalogId {
        self.id
    }

    pub const fn generation(self) -> CatalogGeneration {
        self.generation
    }

    pub const fn format(self) -> FormatVersion {
        self.format
    }

    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManifestKind {
    Database,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ManifestRef {
    id: ManifestId,
    kind: ManifestKind,
    generation: ManifestGeneration,
    format: FormatVersion,
    byte_length: u64,
    body_sha256: [u8; 32],
}

impl ManifestRef {
    pub fn new(
        id: ManifestId,
        kind: ManifestKind,
        generation: ManifestGeneration,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        Self::from_persisted(id, kind, generation, 6, 0, byte_length, body_sha256)
    }

    pub fn from_persisted(
        id: ManifestId,
        kind: ManifestKind,
        generation: ManifestGeneration,
        format_major: u16,
        format_minor: u16,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        if byte_length < MIN_VARIABLE_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "manifest",
                detail: "byte length is smaller than header plus footer",
            });
        }
        if byte_length > MAX_MANIFEST_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "manifest",
                detail: "byte length exceeds the V6 manifest file ceiling",
            });
        }
        Ok(Self {
            id,
            kind,
            generation,
            format: FormatVersion::require_supported("manifest", format_major, format_minor)?,
            byte_length,
            body_sha256,
        })
    }

    pub const fn id(self) -> ManifestId {
        self.id
    }

    pub const fn kind(self) -> ManifestKind {
        self.kind
    }

    pub const fn generation(self) -> ManifestGeneration {
        self.generation
    }

    pub const fn format(self) -> FormatVersion {
        self.format
    }

    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }
}
