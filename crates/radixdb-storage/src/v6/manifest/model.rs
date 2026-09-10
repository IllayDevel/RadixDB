use radixdb_catalog::ObjectId;

use super::super::{
    ArtifactKind, ArtifactRef, CatalogGeneration, CatalogRef, DatabaseGeneration, DatabaseId,
    FormatError, FormatResult, ManifestGeneration, ManifestId, ManifestKind, ManifestRef,
    SegmentId, WalReplayFloor,
};

pub const MAX_TABLES_PER_DATABASE: usize = 262_144;
pub const MAX_SEGMENTS_PER_TABLE_MANIFEST: usize = 1_048_576;
pub const MAX_ROWS_PER_DATA_ARTIFACT: u64 = u32::MAX as u64;
const MAX_TRANSACTION_HIGH_WATER: u64 = i64::MAX as u64 - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableManifestRef {
    table_id: ObjectId,
    manifest: ManifestRef,
}

impl TableManifestRef {
    pub fn new(table_id: ObjectId, manifest: ManifestRef) -> FormatResult<Self> {
        if manifest.kind() != ManifestKind::Table {
            return Err(FormatError::InvalidManifest {
                kind: "database manifest",
                detail: "table entry refers to a non-table manifest",
            });
        }
        Ok(Self { table_id, manifest })
    }

    pub const fn table_id(self) -> ObjectId {
        self.table_id
    }

    pub const fn manifest(self) -> ManifestRef {
        self.manifest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseManifest {
    database_id: DatabaseId,
    manifest_id: ManifestId,
    generation: DatabaseGeneration,
    catalog: CatalogRef,
    wal_replay_floor: WalReplayFloor,
    transaction_high_water: u64,
    tables: Vec<TableManifestRef>,
    created_unix_ns: u64,
}

impl DatabaseManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        manifest_id: ManifestId,
        generation: DatabaseGeneration,
        catalog: CatalogRef,
        wal_replay_floor: WalReplayFloor,
        transaction_high_water: u64,
        mut tables: Vec<TableManifestRef>,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        if transaction_high_water > MAX_TRANSACTION_HIGH_WATER {
            return Err(FormatError::ManifestLimitExceeded {
                kind: "database manifest",
                field: "transaction high-water",
                actual: transaction_high_water,
                limit: MAX_TRANSACTION_HIGH_WATER,
            });
        }
        if tables.len() > MAX_TABLES_PER_DATABASE {
            return Err(FormatError::ManifestLimitExceeded {
                kind: "database manifest",
                field: "table count",
                actual: tables.len() as u64,
                limit: MAX_TABLES_PER_DATABASE as u64,
            });
        }
        tables.sort_unstable_by_key(|entry| entry.table_id());
        if tables
            .windows(2)
            .any(|pair| pair[0].table_id() == pair[1].table_id())
        {
            return Err(FormatError::InvalidManifest {
                kind: "database manifest",
                detail: "duplicate table ID",
            });
        }
        if tables
            .iter()
            .any(|entry| entry.manifest().generation().get() > generation.get())
        {
            return Err(FormatError::InvalidManifest {
                kind: "database manifest",
                detail: "table manifest generation is newer than database generation",
            });
        }
        Ok(Self {
            database_id,
            manifest_id,
            generation,
            catalog,
            wal_replay_floor,
            transaction_high_water,
            tables,
            created_unix_ns,
        })
    }

    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub const fn manifest_id(&self) -> ManifestId {
        self.manifest_id
    }

    pub const fn generation(&self) -> DatabaseGeneration {
        self.generation
    }

    pub const fn catalog(&self) -> CatalogRef {
        self.catalog
    }

    pub const fn wal_replay_floor(&self) -> WalReplayFloor {
        self.wal_replay_floor
    }

    pub const fn transaction_high_water(&self) -> u64 {
        self.transaction_high_water
    }

    pub fn tables(&self) -> &[TableManifestRef] {
        &self.tables
    }

    pub const fn created_unix_ns(&self) -> u64 {
        self.created_unix_ns
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum SegmentKind {
    Rows = 1,
    Tombstones = 2,
}

impl SegmentKind {
    pub fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Rows),
            2 => Ok(Self::Tombstones),
            _ => Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "unknown segment kind",
            }),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

/// Durable placement tier of one immutable physical segment.
///
/// This is persisted independently from [`SegmentKind`]: the kind describes
/// the payload, while the tier controls whether ordinary compaction may
/// promote the segment again after recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SegmentTier {
    /// Fresh seal output, eligible for ordinary L0 compaction.
    L0 = 0,
    /// Bounded stable output of compaction.
    L1 = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentDescriptor {
    id: SegmentId,
    kind: SegmentKind,
    tier: SegmentTier,
    min_transaction_id: u64,
    max_transaction_id: u64,
    row_count: u64,
    first_row_id: u64,
    last_row_id: u64,
    data_artifact: ArtifactRef,
    index_artifact: Option<ArtifactRef>,
}

impl SegmentDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: SegmentId,
        kind: SegmentKind,
        min_transaction_id: u64,
        max_transaction_id: u64,
        row_count: u64,
        first_row_id: u64,
        last_row_id: u64,
        data_artifact: ArtifactRef,
        index_artifact: Option<ArtifactRef>,
    ) -> FormatResult<Self> {
        Self::new_at_tier(
            id,
            kind,
            SegmentTier::L0,
            min_transaction_id,
            max_transaction_id,
            row_count,
            first_row_id,
            last_row_id,
            data_artifact,
            index_artifact,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_at_tier(
        id: SegmentId,
        kind: SegmentKind,
        tier: SegmentTier,
        min_transaction_id: u64,
        max_transaction_id: u64,
        row_count: u64,
        first_row_id: u64,
        last_row_id: u64,
        data_artifact: ArtifactRef,
        index_artifact: Option<ArtifactRef>,
    ) -> FormatResult<Self> {
        if max_transaction_id < min_transaction_id {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "segment transaction range is reversed",
            });
        }
        if row_count > MAX_ROWS_PER_DATA_ARTIFACT {
            return Err(FormatError::ManifestLimitExceeded {
                kind: "table manifest",
                field: "segment row count",
                actual: row_count,
                limit: MAX_ROWS_PER_DATA_ARTIFACT,
            });
        }
        if row_count == 0 {
            if first_row_id != 0 || last_row_id != 0 {
                return Err(FormatError::InvalidManifest {
                    kind: "table manifest",
                    detail: "empty segment has a non-zero row-ID range",
                });
            }
        } else {
            if min_transaction_id == 0 {
                return Err(FormatError::InvalidManifest {
                    kind: "table manifest",
                    detail: "non-empty segment has zero minimum transaction ID",
                });
            }
            if last_row_id < first_row_id {
                return Err(FormatError::InvalidManifest {
                    kind: "table manifest",
                    detail: "non-empty segment has an invalid row-ID range",
                });
            }
        }
        if data_artifact.kind() != ArtifactKind::Data {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "segment data reference is not a DATA artifact",
            });
        }
        if index_artifact.is_some_and(|artifact| artifact.kind() != ArtifactKind::Index) {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "segment index reference is not an INDEX artifact",
            });
        }
        Ok(Self {
            id,
            kind,
            tier,
            min_transaction_id,
            max_transaction_id,
            row_count,
            first_row_id,
            last_row_id,
            data_artifact,
            index_artifact,
        })
    }

    pub const fn id(self) -> SegmentId {
        self.id
    }

    pub const fn kind(self) -> SegmentKind {
        self.kind
    }

    pub const fn tier(self) -> SegmentTier {
        self.tier
    }

    pub const fn min_transaction_id(self) -> u64 {
        self.min_transaction_id
    }

    pub const fn max_transaction_id(self) -> u64 {
        self.max_transaction_id
    }

    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    pub const fn first_row_id(self) -> u64 {
        self.first_row_id
    }

    pub const fn last_row_id(self) -> u64 {
        self.last_row_id
    }

    pub const fn data_artifact(self) -> ArtifactRef {
        self.data_artifact
    }

    pub const fn index_artifact(self) -> Option<ArtifactRef> {
        self.index_artifact
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableManifest {
    database_id: DatabaseId,
    table_id: ObjectId,
    manifest_id: ManifestId,
    generation: ManifestGeneration,
    catalog_generation: CatalogGeneration,
    row_id_high_water: u64,
    next_segment_sequence: u64,
    segments: Vec<SegmentDescriptor>,
    created_unix_ns: u64,
}

impl TableManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        table_id: ObjectId,
        manifest_id: ManifestId,
        generation: ManifestGeneration,
        catalog_generation: CatalogGeneration,
        row_id_high_water: u64,
        next_segment_sequence: u64,
        mut segments: Vec<SegmentDescriptor>,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        if next_segment_sequence == 0 {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "next segment sequence is zero",
            });
        }
        if segments.len() > MAX_SEGMENTS_PER_TABLE_MANIFEST {
            return Err(FormatError::ManifestLimitExceeded {
                kind: "table manifest",
                field: "segment count",
                actual: segments.len() as u64,
                limit: MAX_SEGMENTS_PER_TABLE_MANIFEST as u64,
            });
        }
        segments.sort_unstable_by_key(|segment| segment.id());
        if segments.windows(2).any(|pair| pair[0].id() == pair[1].id()) {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "duplicate segment ID",
            });
        }
        let mut artifact_ids = Vec::with_capacity(segments.len().saturating_mul(2));
        for segment in &segments {
            artifact_ids.push(segment.data_artifact().id());
            if let Some(index) = segment.index_artifact() {
                artifact_ids.push(index.id());
            }
        }
        artifact_ids.sort_unstable();
        if artifact_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "duplicate artifact ID",
            });
        }
        if segments.iter().any(|segment| {
            segment.data_artifact().creation_generation().get() > generation.get()
                || segment
                    .index_artifact()
                    .is_some_and(|artifact| artifact.creation_generation().get() > generation.get())
        }) {
            return Err(FormatError::InvalidManifest {
                kind: "table manifest",
                detail: "artifact creation generation is newer than table manifest",
            });
        }
        Ok(Self {
            database_id,
            table_id,
            manifest_id,
            generation,
            catalog_generation,
            row_id_high_water,
            next_segment_sequence,
            segments,
            created_unix_ns,
        })
    }

    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub const fn table_id(&self) -> ObjectId {
        self.table_id
    }

    pub const fn manifest_id(&self) -> ManifestId {
        self.manifest_id
    }

    pub const fn generation(&self) -> ManifestGeneration {
        self.generation
    }

    pub const fn catalog_generation(&self) -> CatalogGeneration {
        self.catalog_generation
    }

    pub const fn row_id_high_water(&self) -> u64 {
        self.row_id_high_water
    }

    pub const fn next_segment_sequence(&self) -> u64 {
        self.next_segment_sequence
    }

    pub fn segments(&self) -> &[SegmentDescriptor] {
        &self.segments
    }

    pub const fn created_unix_ns(&self) -> u64 {
        self.created_unix_ns
    }
}
