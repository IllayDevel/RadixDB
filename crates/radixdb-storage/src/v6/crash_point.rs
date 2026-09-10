#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GenerationCrashPoint {
    StageOwnerBeforeWrite,
    StageOwnerAfterSync,
    DataAfterBodyBeforeFooter,
    DataAfterFooterBeforeSync,
    DataAfterFileSync,
    DataAfterFinalRenameBeforeDirSync,
    DataFinalDirDurable,
    IndexAfterBodyBeforeFooter,
    IndexAfterFooterBeforeSync,
    IndexAfterFileSync,
    IndexAfterFinalRenameBeforeDirSync,
    IndexFinalDirDurable,
    WalBeforeRecordWrite,
    WalAfterRecordWriteBeforeSync,
    WalBeforeCommitMarker,
    WalAfterCommitMarkerWriteBeforeSync,
    WalCommitMarkerDurable,
    CatalogRuntimeBeforePublish,
    CatalogRuntimePublished,
    TableManifestAfterBodyBeforeFooter,
    TableManifestAfterFileSync,
    TableManifestAfterRenameBeforeDirSync,
    TableManifestDirDurable,
    CatalogPackAfterBodyBeforeFooter,
    CatalogPackAfterFileSync,
    CatalogPackAfterRenameBeforeDirSync,
    CatalogPackDirDurable,
    WalSuccessorAfterCreateBeforeSync,
    WalSuccessorDurable,
    DatabaseManifestAfterBodyBeforeFooter,
    DatabaseManifestAfterFileSync,
    DatabaseManifestAfterRenameBeforeDirSync,
    DatabaseManifestDirDurable,
    ControlAfterPartialWrite,
    ControlAfterWriteBeforeFdatasync,
    ControlDurable,
    ControlAfterRootDirSync,
    RuntimeGenerationBeforePublish,
    RuntimeGenerationPublished,
    WalBeforeTruncate,
    WalAfterRenameToRetired,
    WalAfterUnlinkBeforeDirSync,
    WalTruncateDirDurable,
    SnapshotMemberAfterWriteBeforeSync,
    SnapshotMemberDurable,
    SnapshotManifestAfterSyncBeforeRename,
    SnapshotManifestAfterRenameBeforeDirSync,
    SnapshotManifestDirDurable,
    RestoreStageValidated,
    RestoreAfterRootRenameBeforeParentSync,
    RestoreParentDirDurable,
    GcAfterRootSnapshot,
    GcEnumerationError,
    GcBeforeQuarantineRename,
    GcAfterQuarantineRenameBeforeSync,
    GcQuarantineDirDurable,
    GcAfterSecondRootProof,
    GcBeforeUnlink,
    GcAfterUnlinkBeforeDirSync,
    GcDeleteDirDurable,
    GcLeaseAppeared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalGenerationExpectation {
    Old,
    OldOrNew,
    New,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticExpectation {
    Source,
    SourceOrCommitted,
    Committed,
}

impl GenerationCrashPoint {
    pub const LIFECYCLE_PUBLICATION_POINTS: [Self; 43] = [
        Self::StageOwnerBeforeWrite,
        Self::StageOwnerAfterSync,
        Self::DataAfterBodyBeforeFooter,
        Self::DataAfterFooterBeforeSync,
        Self::DataAfterFileSync,
        Self::DataAfterFinalRenameBeforeDirSync,
        Self::DataFinalDirDurable,
        Self::IndexAfterBodyBeforeFooter,
        Self::IndexAfterFooterBeforeSync,
        Self::IndexAfterFileSync,
        Self::IndexAfterFinalRenameBeforeDirSync,
        Self::IndexFinalDirDurable,
        Self::WalBeforeRecordWrite,
        Self::WalAfterRecordWriteBeforeSync,
        Self::WalBeforeCommitMarker,
        Self::WalAfterCommitMarkerWriteBeforeSync,
        Self::WalCommitMarkerDurable,
        Self::CatalogRuntimeBeforePublish,
        Self::CatalogRuntimePublished,
        Self::TableManifestAfterBodyBeforeFooter,
        Self::TableManifestAfterFileSync,
        Self::TableManifestAfterRenameBeforeDirSync,
        Self::TableManifestDirDurable,
        Self::CatalogPackAfterBodyBeforeFooter,
        Self::CatalogPackAfterFileSync,
        Self::CatalogPackAfterRenameBeforeDirSync,
        Self::CatalogPackDirDurable,
        Self::WalSuccessorAfterCreateBeforeSync,
        Self::WalSuccessorDurable,
        Self::DatabaseManifestAfterBodyBeforeFooter,
        Self::DatabaseManifestAfterFileSync,
        Self::DatabaseManifestAfterRenameBeforeDirSync,
        Self::DatabaseManifestDirDurable,
        Self::ControlAfterPartialWrite,
        Self::ControlAfterWriteBeforeFdatasync,
        Self::ControlDurable,
        Self::ControlAfterRootDirSync,
        Self::RuntimeGenerationBeforePublish,
        Self::RuntimeGenerationPublished,
        Self::WalBeforeTruncate,
        Self::WalAfterRenameToRetired,
        Self::WalAfterUnlinkBeforeDirSync,
        Self::WalTruncateDirDurable,
    ];

    pub const LIFECYCLE_GC_POINTS: [Self; 10] = [
        Self::GcAfterRootSnapshot,
        Self::GcEnumerationError,
        Self::GcBeforeQuarantineRename,
        Self::GcAfterQuarantineRenameBeforeSync,
        Self::GcQuarantineDirDurable,
        Self::GcAfterSecondRootProof,
        Self::GcBeforeUnlink,
        Self::GcAfterUnlinkBeforeDirSync,
        Self::GcDeleteDirDurable,
        Self::GcLeaseAppeared,
    ];

    pub const SNAPSHOT_PUBLICATION_POINTS: [Self; 5] = [
        Self::SnapshotMemberAfterWriteBeforeSync,
        Self::SnapshotMemberDurable,
        Self::SnapshotManifestAfterSyncBeforeRename,
        Self::SnapshotManifestAfterRenameBeforeDirSync,
        Self::SnapshotManifestDirDurable,
    ];

    pub const RESTORE_PUBLICATION_POINTS: [Self; 3] = [
        Self::RestoreStageValidated,
        Self::RestoreAfterRootRenameBeforeParentSync,
        Self::RestoreParentDirDurable,
    ];

    pub const STRUCTURAL_PUBLICATION_POINTS: [Self; 31] = [
        Self::WalBeforeRecordWrite,
        Self::WalAfterRecordWriteBeforeSync,
        Self::WalBeforeCommitMarker,
        Self::WalAfterCommitMarkerWriteBeforeSync,
        Self::WalCommitMarkerDurable,
        Self::CatalogRuntimeBeforePublish,
        Self::CatalogRuntimePublished,
        Self::TableManifestAfterBodyBeforeFooter,
        Self::TableManifestAfterFileSync,
        Self::TableManifestAfterRenameBeforeDirSync,
        Self::TableManifestDirDurable,
        Self::CatalogPackAfterBodyBeforeFooter,
        Self::CatalogPackAfterFileSync,
        Self::CatalogPackAfterRenameBeforeDirSync,
        Self::CatalogPackDirDurable,
        Self::WalSuccessorAfterCreateBeforeSync,
        Self::WalSuccessorDurable,
        Self::DatabaseManifestAfterBodyBeforeFooter,
        Self::DatabaseManifestAfterFileSync,
        Self::DatabaseManifestAfterRenameBeforeDirSync,
        Self::DatabaseManifestDirDurable,
        Self::ControlAfterPartialWrite,
        Self::ControlAfterWriteBeforeFdatasync,
        Self::ControlDurable,
        Self::ControlAfterRootDirSync,
        Self::RuntimeGenerationBeforePublish,
        Self::RuntimeGenerationPublished,
        Self::WalBeforeTruncate,
        Self::WalAfterRenameToRetired,
        Self::WalAfterUnlinkBeforeDirSync,
        Self::WalTruncateDirDurable,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::StageOwnerBeforeWrite => "V6_STAGE_OWNER_BEFORE_WRITE",
            Self::StageOwnerAfterSync => "V6_STAGE_OWNER_AFTER_SYNC",
            Self::DataAfterBodyBeforeFooter => "V6_DATA_AFTER_BODY_BEFORE_FOOTER",
            Self::DataAfterFooterBeforeSync => "V6_DATA_AFTER_FOOTER_BEFORE_SYNC",
            Self::DataAfterFileSync => "V6_DATA_AFTER_FILE_SYNC",
            Self::DataAfterFinalRenameBeforeDirSync => "V6_DATA_AFTER_FINAL_RENAME_BEFORE_DIR_SYNC",
            Self::DataFinalDirDurable => "V6_DATA_FINAL_DIR_DURABLE",
            Self::IndexAfterBodyBeforeFooter => "V6_INDEX_AFTER_BODY_BEFORE_FOOTER",
            Self::IndexAfterFooterBeforeSync => "V6_INDEX_AFTER_FOOTER_BEFORE_SYNC",
            Self::IndexAfterFileSync => "V6_INDEX_AFTER_FILE_SYNC",
            Self::IndexAfterFinalRenameBeforeDirSync => {
                "V6_INDEX_AFTER_FINAL_RENAME_BEFORE_DIR_SYNC"
            }
            Self::IndexFinalDirDurable => "V6_INDEX_FINAL_DIR_DURABLE",
            Self::WalBeforeRecordWrite => "V6_WAL_BEFORE_RECORD_WRITE",
            Self::WalAfterRecordWriteBeforeSync => "V6_WAL_AFTER_RECORD_WRITE_BEFORE_SYNC",
            Self::WalBeforeCommitMarker => "V6_WAL_BEFORE_COMMIT_MARKER",
            Self::WalAfterCommitMarkerWriteBeforeSync => {
                "V6_WAL_AFTER_COMMIT_MARKER_WRITE_BEFORE_SYNC"
            }
            Self::WalCommitMarkerDurable => "V6_WAL_COMMIT_MARKER_DURABLE",
            Self::CatalogRuntimeBeforePublish => "V6_CATALOG_RUNTIME_BEFORE_PUBLISH",
            Self::CatalogRuntimePublished => "V6_CATALOG_RUNTIME_PUBLISHED",
            Self::TableManifestAfterBodyBeforeFooter => {
                "V6_TABLE_MANIFEST_AFTER_BODY_BEFORE_FOOTER"
            }
            Self::TableManifestAfterFileSync => "V6_TABLE_MANIFEST_AFTER_FILE_SYNC",
            Self::TableManifestAfterRenameBeforeDirSync => {
                "V6_TABLE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC"
            }
            Self::TableManifestDirDurable => "V6_TABLE_MANIFEST_DIR_DURABLE",
            Self::CatalogPackAfterBodyBeforeFooter => "V6_CATALOG_PACK_AFTER_BODY_BEFORE_FOOTER",
            Self::CatalogPackAfterFileSync => "V6_CATALOG_PACK_AFTER_FILE_SYNC",
            Self::CatalogPackAfterRenameBeforeDirSync => {
                "V6_CATALOG_PACK_AFTER_RENAME_BEFORE_DIR_SYNC"
            }
            Self::CatalogPackDirDurable => "V6_CATALOG_PACK_DIR_DURABLE",
            Self::WalSuccessorAfterCreateBeforeSync => "V6_WAL_SUCCESSOR_AFTER_CREATE_BEFORE_SYNC",
            Self::WalSuccessorDurable => "V6_WAL_SUCCESSOR_DURABLE",
            Self::DatabaseManifestAfterBodyBeforeFooter => {
                "V6_DATABASE_MANIFEST_AFTER_BODY_BEFORE_FOOTER"
            }
            Self::DatabaseManifestAfterFileSync => "V6_DATABASE_MANIFEST_AFTER_FILE_SYNC",
            Self::DatabaseManifestAfterRenameBeforeDirSync => {
                "V6_DATABASE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC"
            }
            Self::DatabaseManifestDirDurable => "V6_DATABASE_MANIFEST_DIR_DURABLE",
            Self::ControlAfterPartialWrite => "V6_CONTROL_AFTER_PARTIAL_WRITE",
            Self::ControlAfterWriteBeforeFdatasync => "V6_CONTROL_AFTER_WRITE_BEFORE_FDATASYNC",
            Self::ControlDurable => "V6_CONTROL_DURABLE",
            Self::ControlAfterRootDirSync => "V6_CONTROL_AFTER_ROOT_DIR_SYNC",
            Self::RuntimeGenerationBeforePublish => "V6_RUNTIME_GENERATION_BEFORE_PUBLISH",
            Self::RuntimeGenerationPublished => "V6_RUNTIME_GENERATION_PUBLISHED",
            Self::WalBeforeTruncate => "V6_WAL_BEFORE_TRUNCATE",
            Self::WalAfterRenameToRetired => "V6_WAL_AFTER_RENAME_TO_RETIRED",
            Self::WalAfterUnlinkBeforeDirSync => "V6_WAL_AFTER_UNLINK_BEFORE_DIR_SYNC",
            Self::WalTruncateDirDurable => "V6_WAL_TRUNCATE_DIR_DURABLE",
            Self::SnapshotMemberAfterWriteBeforeSync => {
                "V6_SNAPSHOT_MEMBER_AFTER_WRITE_BEFORE_SYNC"
            }
            Self::SnapshotMemberDurable => "V6_SNAPSHOT_MEMBER_DURABLE",
            Self::SnapshotManifestAfterSyncBeforeRename => {
                "V6_SNAPSHOT_MANIFEST_AFTER_SYNC_BEFORE_RENAME"
            }
            Self::SnapshotManifestAfterRenameBeforeDirSync => {
                "V6_SNAPSHOT_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC"
            }
            Self::SnapshotManifestDirDurable => "V6_SNAPSHOT_MANIFEST_DIR_DURABLE",
            Self::RestoreStageValidated => "V6_RESTORE_STAGE_VALIDATED",
            Self::RestoreAfterRootRenameBeforeParentSync => {
                "V6_RESTORE_AFTER_ROOT_RENAME_BEFORE_PARENT_SYNC"
            }
            Self::RestoreParentDirDurable => "V6_RESTORE_PARENT_DIR_DURABLE",
            Self::GcAfterRootSnapshot => "V6_GC_AFTER_ROOT_SNAPSHOT",
            Self::GcEnumerationError => "V6_GC_ENUMERATION_ERROR",
            Self::GcBeforeQuarantineRename => "V6_GC_BEFORE_QUARANTINE_RENAME",
            Self::GcAfterQuarantineRenameBeforeSync => "V6_GC_AFTER_QUARANTINE_RENAME_BEFORE_SYNC",
            Self::GcQuarantineDirDurable => "V6_GC_QUARANTINE_DIR_DURABLE",
            Self::GcAfterSecondRootProof => "V6_GC_AFTER_SECOND_ROOT_PROOF",
            Self::GcBeforeUnlink => "V6_GC_BEFORE_UNLINK",
            Self::GcAfterUnlinkBeforeDirSync => "V6_GC_AFTER_UNLINK_BEFORE_DIR_SYNC",
            Self::GcDeleteDirDurable => "V6_GC_DELETE_DIR_DURABLE",
            Self::GcLeaseAppeared => "V6_GC_LEASE_APPEARED",
        }
    }

    pub const fn physical_expectation(self) -> PhysicalGenerationExpectation {
        match self {
            Self::ControlAfterWriteBeforeFdatasync => PhysicalGenerationExpectation::OldOrNew,
            Self::ControlDurable
            | Self::ControlAfterRootDirSync
            | Self::RuntimeGenerationBeforePublish
            | Self::RuntimeGenerationPublished
            | Self::WalBeforeTruncate
            | Self::WalAfterRenameToRetired
            | Self::WalAfterUnlinkBeforeDirSync
            | Self::WalTruncateDirDurable => PhysicalGenerationExpectation::New,
            _ => PhysicalGenerationExpectation::Old,
        }
    }

    pub const fn semantic_expectation(self) -> SemanticExpectation {
        match self {
            Self::WalBeforeRecordWrite
            | Self::WalAfterRecordWriteBeforeSync
            | Self::WalBeforeCommitMarker => SemanticExpectation::Source,
            Self::WalAfterCommitMarkerWriteBeforeSync => SemanticExpectation::SourceOrCommitted,
            _ => SemanticExpectation::Committed,
        }
    }
}
