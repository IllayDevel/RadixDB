use std::fmt;

use radixdb_catalog::CatalogError;

pub type FormatResult<T> = Result<T, FormatError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    InvalidIdentityHex {
        kind: &'static str,
    },
    ZeroIdentity {
        kind: &'static str,
    },
    ZeroGeneration {
        kind: &'static str,
    },
    GenerationOverflow {
        kind: &'static str,
    },
    UnsupportedFormatVersion {
        owner: &'static str,
        major: u16,
        minor: u16,
    },
    UnknownArtifactKind {
        tag: u16,
    },
    UnsupportedArtifactVersion {
        version: u16,
    },
    UnknownArtifactFlags {
        flags: u32,
    },
    InvalidArtifactLocator {
        detail: &'static str,
    },
    InvalidReference {
        owner: &'static str,
        detail: &'static str,
    },
    UnknownIndexSectionKind {
        tag: u16,
    },
    UnsupportedIndexSectionVersion {
        version: u16,
    },
    InvalidControlRecord {
        detail: &'static str,
    },
    ControlChecksumMismatch,
    NoValidControlSlot,
    NoCompleteControlGeneration,
    ControlSplitBrain {
        generation: u64,
    },
    LegacyDatabaseRoot,
    InvalidDatabaseRoot {
        detail: &'static str,
    },
    DatabaseRootIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidManifest {
        kind: &'static str,
        detail: &'static str,
    },
    ManifestLimitExceeded {
        kind: &'static str,
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    ManifestChecksumMismatch {
        kind: &'static str,
        scope: &'static str,
    },
    InvalidDataArtifact {
        detail: &'static str,
    },
    DataArtifactLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    DataArtifactChecksumMismatch {
        scope: &'static str,
    },
    UnknownIndexAcceleratorKind {
        tag: u16,
    },
    UnsupportedIndexAcceleratorVersion {
        version: u16,
    },
    InvalidIndexArtifact {
        detail: &'static str,
    },
    IndexArtifactLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    IndexArtifactChecksumMismatch {
        scope: &'static str,
    },
    ArtifactIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidStagingRecord {
        record: &'static str,
        detail: &'static str,
    },
    StagingChecksumMismatch {
        record: &'static str,
    },
    StagingLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    StagingIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidPublication {
        detail: &'static str,
    },
    InvalidFilesystemOwner {
        detail: &'static str,
    },
    FilesystemOwnerIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidPublicationGraph {
        detail: String,
    },
    PublicationIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    PublicationRecoveryRequired {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidCheckpoint {
        detail: &'static str,
    },
    WalRetirementIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidMaintenance {
        detail: &'static str,
    },
    InvalidLease {
        detail: &'static str,
    },
    InvalidLeaseLimit {
        field: &'static str,
        requested: usize,
        hard_limit: usize,
    },
    LeaseLimitExceeded {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    InvalidCleanup {
        detail: &'static str,
    },
    CleanupBusy,
    CleanupLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    CleanupIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    CleanupRecoveryRequired {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidRecovery {
        detail: &'static str,
    },
    InvalidRecoveryGraph {
        detail: String,
    },
    MetadataOpenLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    RecoveryIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidSnapshot {
        detail: &'static str,
    },
    SnapshotLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    SnapshotChecksumMismatch {
        scope: &'static str,
    },
    SnapshotIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    SnapshotRecoveryRequired {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    InvalidCatalogWal {
        detail: &'static str,
    },
    CatalogWalLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    CatalogWalChecksumMismatch {
        scope: &'static str,
    },
    CatalogWalIo {
        operation: &'static str,
        kind: std::io::ErrorKind,
    },
    CatalogMutation {
        source: CatalogError,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentityHex { kind } => write!(
                formatter,
                "{kind} must contain exactly 32 hexadecimal digits"
            ),
            Self::ZeroIdentity { kind } => write!(formatter, "{kind} cannot be all zero"),
            Self::ZeroGeneration { kind } => write!(formatter, "{kind} cannot be zero"),
            Self::GenerationOverflow { kind } => {
                write!(formatter, "{kind} cannot advance beyond u64::MAX")
            }
            Self::UnsupportedFormatVersion {
                owner,
                major,
                minor,
            } => write!(
                formatter,
                "unsupported {owner} format version {major}.{minor}"
            ),
            Self::UnknownArtifactKind { tag } => write!(formatter, "unknown artifact kind {tag}"),
            Self::UnsupportedArtifactVersion { version } => {
                write!(formatter, "unsupported artifact codec version {version}")
            }
            Self::UnknownArtifactFlags { flags } => {
                write!(formatter, "unknown artifact flags 0x{flags:08x}")
            }
            Self::InvalidArtifactLocator { detail } => {
                write!(formatter, "invalid artifact locator: {detail}")
            }
            Self::InvalidReference { owner, detail } => {
                write!(formatter, "invalid {owner} reference: {detail}")
            }
            Self::UnknownIndexSectionKind { tag } => {
                write!(formatter, "unknown index section kind {tag}")
            }
            Self::UnsupportedIndexSectionVersion { version } => {
                write!(formatter, "unsupported index section version {version}")
            }
            Self::InvalidControlRecord { detail } => {
                write!(formatter, "invalid CONTROL record: {detail}")
            }
            Self::ControlChecksumMismatch => formatter.write_str("CONTROL checksum mismatch"),
            Self::NoValidControlSlot => formatter.write_str("neither CONTROL slot is valid"),
            Self::NoCompleteControlGeneration => {
                formatter.write_str("no valid CONTROL slot references a complete generation")
            }
            Self::ControlSplitBrain { generation } => write!(
                formatter,
                "CONTROL slots disagree at database generation {generation}"
            ),
            Self::LegacyDatabaseRoot => formatter.write_str(
                "legacy database root is unsupported; export it with the frozen old binary and import the SQL dump with the current binary",
            ),
            Self::InvalidDatabaseRoot { detail } => {
                write!(formatter, "invalid database root: {detail}")
            }
            Self::DatabaseRootIo { operation, kind } => {
                write!(formatter, "database root {operation} failed: {kind}")
            }
            Self::InvalidManifest { kind, detail } => {
                write!(formatter, "invalid {kind}: {detail}")
            }
            Self::ManifestLimitExceeded {
                kind,
                field,
                actual,
                limit,
            } => write!(formatter, "{kind} {field} is {actual}; V6 limit is {limit}"),
            Self::ManifestChecksumMismatch { kind, scope } => {
                write!(formatter, "{kind} {scope} checksum mismatch")
            }
            Self::InvalidDataArtifact { detail } => {
                write!(formatter, "invalid V6 data artifact: {detail}")
            }
            Self::DataArtifactLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "V6 data artifact {field} is {actual}; hard limit is {limit}"
            ),
            Self::DataArtifactChecksumMismatch { scope } => {
                write!(formatter, "V6 data artifact {scope} checksum mismatch")
            }
            Self::UnknownIndexAcceleratorKind { tag } => {
                write!(formatter, "unknown index accelerator kind {tag}")
            }
            Self::UnsupportedIndexAcceleratorVersion { version } => {
                write!(formatter, "unsupported index accelerator version {version}")
            }
            Self::InvalidIndexArtifact { detail } => {
                write!(formatter, "invalid V6 index artifact: {detail}")
            }
            Self::IndexArtifactLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "V6 index artifact {field} is {actual}; hard/runtime limit is {limit}"
            ),
            Self::IndexArtifactChecksumMismatch { scope } => {
                write!(formatter, "V6 index artifact {scope} checksum mismatch")
            }
            Self::ArtifactIo { operation, kind } => {
                write!(formatter, "artifact {operation} failed: {kind}")
            }
            Self::InvalidStagingRecord { record, detail } => {
                write!(formatter, "invalid staging {record}: {detail}")
            }
            Self::StagingChecksumMismatch { record } => {
                write!(formatter, "staging {record} checksum mismatch")
            }
            Self::StagingLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "staging {field} is {actual}; hard/runtime limit is {limit}"
            ),
            Self::StagingIo { operation, kind } => {
                write!(formatter, "staging {operation} failed: {kind}")
            }
            Self::InvalidPublication { detail } => {
                write!(formatter, "invalid generation publication: {detail}")
            }
            Self::InvalidFilesystemOwner { detail } => {
                write!(formatter, "invalid database filesystem owner: {detail}")
            }
            Self::FilesystemOwnerIo { operation, kind } => {
                write!(formatter, "database filesystem owner {operation} failed: {kind}")
            }
            Self::InvalidPublicationGraph { detail } => {
                write!(formatter, "invalid generation publication graph: {detail}")
            }
            Self::PublicationIo { operation, kind } => {
                write!(
                    formatter,
                    "generation publication {operation} failed: {kind}"
                )
            }
            Self::PublicationRecoveryRequired { operation, kind } => write!(
                formatter,
                "generation publication requires recovery after {operation} failed: {kind}"
            ),
            Self::InvalidCheckpoint { detail } => {
                write!(formatter, "invalid checkpoint: {detail}")
            }
            Self::WalRetirementIo { operation, kind } => {
                write!(formatter, "WAL retirement {operation} failed: {kind}")
            }
            Self::InvalidMaintenance { detail } => {
                write!(formatter, "invalid maintenance publication: {detail}")
            }
            Self::InvalidLease { detail } => write!(formatter, "invalid lease: {detail}"),
            Self::InvalidLeaseLimit {
                field,
                requested,
                hard_limit,
            } => write!(
                formatter,
                "lease {field} limit {requested} is outside 1..={hard_limit}"
            ),
            Self::LeaseLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "active lease {field} is {actual}; configured limit is {limit}"
            ),
            Self::InvalidCleanup { detail } => {
                write!(formatter, "invalid artifact cleanup state: {detail}")
            }
            Self::CleanupBusy => formatter.write_str("artifact cleanup is already running"),
            Self::CleanupLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "artifact cleanup {field} is {actual}; configured limit is {limit}"
            ),
            Self::CleanupIo { operation, kind } => {
                write!(formatter, "artifact cleanup {operation} failed: {kind}")
            }
            Self::CleanupRecoveryRequired { operation, kind } => write!(
                formatter,
                "artifact cleanup requires recovery after {operation} failed: {kind}"
            ),
            Self::InvalidRecovery { detail } => {
                write!(formatter, "invalid recovery state: {detail}")
            }
            Self::InvalidRecoveryGraph { detail } => {
                write!(formatter, "invalid recovery graph: {detail}")
            }
            Self::MetadataOpenLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "database-open metadata {field} is {actual}; configured limit is {limit}"
            ),
            Self::RecoveryIo { operation, kind } => {
                write!(formatter, "recovery {operation} failed: {kind}")
            }
            Self::InvalidSnapshot { detail } => {
                write!(formatter, "invalid physical snapshot: {detail}")
            }
            Self::SnapshotLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "physical snapshot {field} is {actual}; hard limit is {limit}"
            ),
            Self::SnapshotChecksumMismatch { scope } => {
                write!(formatter, "physical snapshot {scope} checksum mismatch")
            }
            Self::SnapshotIo { operation, kind } => {
                write!(formatter, "physical snapshot {operation} failed: {kind}")
            }
            Self::SnapshotRecoveryRequired { operation, kind } => write!(
                formatter,
                "physical snapshot requires recovery after {operation} failed: {kind}"
            ),
            Self::InvalidCatalogWal { detail } => {
                write!(formatter, "invalid V6 catalog WAL: {detail}")
            }
            Self::CatalogWalLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "V6 catalog WAL {field} is {actual}; hard/runtime limit is {limit}"
            ),
            Self::CatalogWalChecksumMismatch { scope } => {
                write!(formatter, "V6 catalog WAL {scope} checksum mismatch")
            }
            Self::CatalogWalIo { operation, kind } => {
                write!(formatter, "catalog WAL {operation} failed: {kind}")
            }
            Self::CatalogMutation { source } => {
                write!(formatter, "V6 catalog WAL mutation is invalid: {source}")
            }
        }
    }
}

impl std::error::Error for FormatError {}

impl From<CatalogError> for FormatError {
    fn from(source: CatalogError) -> Self {
        Self::CatalogMutation { source }
    }
}
