use std::path::PathBuf;

use radixdb_catalog::ObjectId;

use super::{ArtifactId, DatabaseGeneration, FormatError, FormatResult};

pub const ARTIFACT_CODEC_VERSION: u16 = 1;
const MIN_ARTIFACT_FILE_BYTES: u64 = 256 + 48;
pub const MAX_ARTIFACT_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const COMMON_FOOTER_BYTES: u64 = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ArtifactKind {
    Data = 1,
    Index = 2,
}

impl ArtifactKind {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Data),
            2 => Ok(Self::Index),
            _ => Err(FormatError::UnknownArtifactKind { tag }),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn suffix(self) -> ArtifactSuffix {
        match self {
            Self::Data => ArtifactSuffix::Data,
            Self::Index => ArtifactSuffix::Index,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ArtifactSuffix {
    Data = 1,
    Index = 2,
}

impl ArtifactSuffix {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Data),
            2 => Ok(Self::Index),
            _ => Err(FormatError::InvalidArtifactLocator {
                detail: "unknown locator suffix",
            }),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn extension(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "idx",
        }
    }

    const fn directory(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Index => "index",
        }
    }
}

/// Validated physical locator derived from artifact identity and kind.
///
/// It contains no persisted path string. The pathname is a deterministic
/// locator only and never becomes membership or logical identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactLocator {
    shard: u8,
    suffix: ArtifactSuffix,
}

impl ArtifactLocator {
    pub fn derive(id: ArtifactId, kind: ArtifactKind) -> Self {
        Self {
            shard: id.as_bytes()[0],
            suffix: kind.suffix(),
        }
    }

    pub fn from_persisted(
        id: ArtifactId,
        kind: ArtifactKind,
        shard: u16,
        suffix_tag: u16,
    ) -> FormatResult<Self> {
        let shard = u8::try_from(shard).map_err(|_| FormatError::InvalidArtifactLocator {
            detail: "locator shard is outside 0..=255",
        })?;
        let suffix = ArtifactSuffix::from_tag(suffix_tag)?;
        let expected = Self::derive(id, kind);
        if shard != expected.shard {
            return Err(FormatError::InvalidArtifactLocator {
                detail: "locator shard differs from the first artifact ID byte",
            });
        }
        if suffix != expected.suffix {
            return Err(FormatError::InvalidArtifactLocator {
                detail: "locator suffix differs from artifact kind",
            });
        }
        Ok(expected)
    }

    pub const fn shard(self) -> u8 {
        self.shard
    }

    pub const fn suffix(self) -> ArtifactSuffix {
        self.suffix
    }

    pub(crate) fn relative_path(self, id: ArtifactId) -> PathBuf {
        PathBuf::from("artifacts")
            .join(self.suffix.directory())
            .join(format!("{:02x}", self.shard))
            .join(format!("{id}.{}", self.suffix.extension()))
    }
}

/// Complete immutable identity of one V6 `.data` or `.idx` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactRef {
    id: ArtifactId,
    kind: ArtifactKind,
    codec_version: u16,
    creation_generation: DatabaseGeneration,
    byte_length: u64,
    body_sha256: [u8; 32],
    locator: ArtifactLocator,
}

impl ArtifactRef {
    pub fn new(
        id: ArtifactId,
        kind: ArtifactKind,
        creation_generation: DatabaseGeneration,
        byte_length: u64,
        body_sha256: [u8; 32],
    ) -> FormatResult<Self> {
        Self::from_persisted(
            id,
            kind,
            ARTIFACT_CODEC_VERSION,
            0,
            creation_generation,
            byte_length,
            body_sha256,
            u16::from(id.as_bytes()[0]),
            kind.suffix().tag(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_persisted(
        id: ArtifactId,
        kind: ArtifactKind,
        codec_version: u16,
        flags: u32,
        creation_generation: DatabaseGeneration,
        byte_length: u64,
        body_sha256: [u8; 32],
        locator_shard: u16,
        locator_suffix: u16,
    ) -> FormatResult<Self> {
        if codec_version != ARTIFACT_CODEC_VERSION {
            return Err(FormatError::UnsupportedArtifactVersion {
                version: codec_version,
            });
        }
        if flags != 0 {
            return Err(FormatError::UnknownArtifactFlags { flags });
        }
        if byte_length < MIN_ARTIFACT_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "artifact",
                detail: "byte length is smaller than header plus footer",
            });
        }
        if byte_length > MAX_ARTIFACT_FILE_BYTES {
            return Err(FormatError::InvalidReference {
                owner: "artifact",
                detail: "byte length exceeds the V6 artifact file ceiling",
            });
        }
        Ok(Self {
            id,
            kind,
            codec_version,
            creation_generation,
            byte_length,
            body_sha256,
            locator: ArtifactLocator::from_persisted(id, kind, locator_shard, locator_suffix)?,
        })
    }

    pub const fn id(self) -> ArtifactId {
        self.id
    }

    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }

    pub const fn codec_version(self) -> u16 {
        self.codec_version
    }

    pub const fn creation_generation(self) -> DatabaseGeneration {
        self.creation_generation
    }

    pub const fn byte_length(self) -> u64 {
        self.byte_length
    }

    pub const fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }

    pub const fn locator(self) -> ArtifactLocator {
        self.locator
    }

    pub fn relative_path(self) -> PathBuf {
        self.locator.relative_path(self.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum IndexSectionKind {
    KeyDescriptor = 1,
    ExactPages = 2,
    OrderedPages = 3,
    HnswMetadata = 4,
    HnswNodes = 5,
    HnswAdjacency = 6,
}

impl IndexSectionKind {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::KeyDescriptor),
            2 => Ok(Self::ExactPages),
            3 => Ok(Self::OrderedPages),
            4 => Ok(Self::HnswMetadata),
            5 => Ok(Self::HnswNodes),
            6 => Ok(Self::HnswAdjacency),
            _ => Err(FormatError::UnknownIndexSectionKind { tag }),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

/// A checksummed section range inside one immutable index artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactSliceRef {
    artifact: ArtifactRef,
    section_index: u32,
    section_kind: IndexSectionKind,
    section_version: u16,
    offset: u64,
    stored_length: u64,
    logical_length: u64,
    item_count: u64,
    stored_crc32: u32,
}

impl ArtifactSliceRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        artifact: ArtifactRef,
        section_index: u32,
        section_kind: IndexSectionKind,
        section_version: u16,
        offset: u64,
        stored_length: u64,
        logical_length: u64,
        item_count: u64,
        stored_crc32: u32,
    ) -> FormatResult<Self> {
        if artifact.kind() != ArtifactKind::Index {
            return Err(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "index section points to a non-index artifact",
            });
        }
        if section_version != 1 {
            return Err(FormatError::UnsupportedIndexSectionVersion {
                version: section_version,
            });
        }
        if !offset.is_multiple_of(8) {
            return Err(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "section offset is not 8-byte aligned",
            });
        }
        if stored_length == 0 || logical_length == 0 {
            return Err(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "section range cannot be empty",
            });
        }
        let end = offset
            .checked_add(stored_length)
            .ok_or(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "section range overflows",
            })?;
        let body_end = artifact
            .byte_length()
            .checked_sub(COMMON_FOOTER_BYTES)
            .ok_or(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "artifact has no footer boundary",
            })?;
        if offset < 256 || end > body_end {
            return Err(FormatError::InvalidReference {
                owner: "artifact slice",
                detail: "section range is outside artifact body",
            });
        }
        Ok(Self {
            artifact,
            section_index,
            section_kind,
            section_version,
            offset,
            stored_length,
            logical_length,
            item_count,
            stored_crc32,
        })
    }

    pub const fn artifact(self) -> ArtifactRef {
        self.artifact
    }

    pub const fn section_index(self) -> u32 {
        self.section_index
    }

    pub const fn section_kind(self) -> IndexSectionKind {
        self.section_kind
    }

    pub const fn section_version(self) -> u16 {
        self.section_version
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn stored_length(self) -> u64 {
        self.stored_length
    }

    pub const fn logical_length(self) -> u64 {
        self.logical_length
    }

    pub const fn item_count(self) -> u64 {
        self.item_count
    }

    pub const fn stored_crc32(self) -> u32 {
        self.stored_crc32
    }
}

/// Binds a stable logical catalog index to one physical section identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexSectionRef {
    logical_index_id: ObjectId,
    slice: ArtifactSliceRef,
}

impl IndexSectionRef {
    pub const fn new(logical_index_id: ObjectId, slice: ArtifactSliceRef) -> Self {
        Self {
            logical_index_id,
            slice,
        }
    }

    pub const fn logical_index_id(self) -> ObjectId {
        self.logical_index_id
    }

    pub const fn slice(self) -> ArtifactSliceRef {
        self.slice
    }
}
