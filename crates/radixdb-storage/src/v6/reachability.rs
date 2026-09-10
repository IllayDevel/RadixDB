use std::collections::HashSet;
use std::fmt;
use std::mem::size_of;

use radixdb_catalog::{decode_catalog_pack, CatalogPack, ObjectId, ObjectKind};

use super::{
    decode_database_manifest, decode_table_manifest, ArtifactId, ArtifactKind, ArtifactRef,
    CatalogGeneration, CatalogRef, ControlRecord, DatabaseId, DatabaseManifest,
    DatabaseManifestRootRef, SegmentDescriptor, SegmentId, SegmentKind, TableManifest,
    TableManifestRef, MAX_MANIFEST_FILE_BYTES,
};

pub const MAX_REACHABLE_IDENTITIES: u64 = 8_388_608;
pub const MAX_REACHABILITY_BYTES: u64 = 1024 * 1024 * 1024;
/// Hard ceiling for all metadata owned while one database generation is opened.
pub const MAX_OPEN_METADATA_BYTES: u64 = 512 * 1024 * 1024;
/// One database manifest plus every table manifest in an opened generation.
pub const MAX_TOTAL_MANIFESTS_PER_OPEN: u64 = 262_145;
/// Sum of segment descriptors across every table manifest in one generation.
pub const MAX_TOTAL_SEGMENTS_PER_OPEN: u64 = 4_194_304;
const ACCOUNTED_IDENTITY_BYTES: u64 = 64;
// The binary codecs retain more than their encoded input: typed values,
// directories and validation maps all coexist during admission.  These bounds
// are deliberately conservative and are charged before decode; exact artifact
// decoder metrics replace estimates where those codecs expose them.
const MANIFEST_DECODED_BOUND_MULTIPLIER: u64 = 2;
const CATALOG_DECODED_BOUND_MULTIPLIER: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReachableNodeKind {
    DatabaseManifest,
    Catalog,
    TableManifest,
    SegmentDescriptor,
    DataArtifact,
    IndexArtifact,
}

impl fmt::Display for ReachableNodeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DatabaseManifest => "database manifest",
            Self::Catalog => "catalog",
            Self::TableManifest => "table manifest",
            Self::SegmentDescriptor => "segment descriptor",
            Self::DataArtifact => "data artifact",
            Self::IndexArtifact => "index artifact",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReachabilityError {
    SourceFailure {
        node: ReachableNodeKind,
        detail: String,
    },
    MissingRequiredNode {
        node: ReachableNodeKind,
        identity: String,
    },
    InvalidRequiredNode {
        node: ReachableNodeKind,
        detail: String,
    },
    CrossReferenceMismatch {
        edge: &'static str,
        detail: String,
    },
    DuplicateIdentity {
        node: ReachableNodeKind,
        identity: String,
    },
    ReachabilityLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    InvalidReachabilityLimit {
        field: &'static str,
        requested: u64,
        hard_limit: u64,
    },
}

impl ReachabilityError {
    pub fn source_failure(node: ReachableNodeKind, detail: impl Into<String>) -> ReachabilityError {
        Self::SourceFailure {
            node,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for ReachabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceFailure { node, detail } => {
                write!(formatter, "failed to read {node}: {detail}")
            }
            Self::MissingRequiredNode { node, identity } => {
                write!(formatter, "required {node} {identity} is missing")
            }
            Self::InvalidRequiredNode { node, detail } => {
                write!(formatter, "invalid required {node}: {detail}")
            }
            Self::CrossReferenceMismatch { edge, detail } => {
                write!(formatter, "cross-reference mismatch on {edge}: {detail}")
            }
            Self::DuplicateIdentity { node, identity } => {
                write!(formatter, "duplicate {node} identity {identity}")
            }
            Self::ReachabilityLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "reachability {field} is {actual}; configured limit is {limit}"
            ),
            Self::InvalidReachabilityLimit {
                field,
                requested,
                hard_limit,
            } => write!(
                formatter,
                "reachability {field} limit {requested} is outside 1..={hard_limit}"
            ),
        }
    }
}

impl std::error::Error for ReachabilityError {}

pub type ReachabilityResult<T> = Result<T, ReachabilityError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReachabilityLimits {
    max_identities: u64,
    max_accounted_bytes: u64,
    max_manifests_per_open: u64,
    max_segments_per_open: u64,
}

impl ReachabilityLimits {
    pub fn new(max_identities: u64, max_accounted_bytes: u64) -> ReachabilityResult<Self> {
        validate_lowered_limit("identity count", max_identities, MAX_REACHABLE_IDENTITIES)?;
        validate_lowered_limit(
            "accounted bytes",
            max_accounted_bytes,
            MAX_REACHABILITY_BYTES,
        )?;
        Ok(Self {
            max_identities,
            max_accounted_bytes,
            max_manifests_per_open: MAX_TOTAL_MANIFESTS_PER_OPEN,
            max_segments_per_open: MAX_TOTAL_SEGMENTS_PER_OPEN,
        })
    }

    /// Lower the database-generation shape limits without changing the wider
    /// reachability identity/byte budget used by GC.
    pub fn with_generation_counts(
        mut self,
        max_manifests_per_open: u64,
        max_segments_per_open: u64,
    ) -> ReachabilityResult<Self> {
        validate_lowered_limit(
            "manifest count",
            max_manifests_per_open,
            MAX_TOTAL_MANIFESTS_PER_OPEN,
        )?;
        validate_lowered_limit(
            "segment count",
            max_segments_per_open,
            MAX_TOTAL_SEGMENTS_PER_OPEN,
        )?;
        self.max_manifests_per_open = max_manifests_per_open;
        self.max_segments_per_open = max_segments_per_open;
        Ok(self)
    }

    pub const fn max_identities(self) -> u64 {
        self.max_identities
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }

    pub const fn max_manifests_per_open(self) -> u64 {
        self.max_manifests_per_open
    }

    pub const fn max_segments_per_open(self) -> u64 {
        self.max_segments_per_open
    }
}

impl Default for ReachabilityLimits {
    fn default() -> Self {
        Self {
            max_identities: MAX_REACHABLE_IDENTITIES,
            max_accounted_bytes: MAX_REACHABILITY_BYTES,
            max_manifests_per_open: MAX_TOTAL_MANIFESTS_PER_OPEN,
            max_segments_per_open: MAX_TOTAL_SEGMENTS_PER_OPEN,
        }
    }
}

fn validate_lowered_limit(
    field: &'static str,
    requested: u64,
    hard_limit: u64,
) -> ReachabilityResult<()> {
    if requested == 0 || requested > hard_limit {
        return Err(ReachabilityError::InvalidReachabilityLimit {
            field,
            requested,
            hard_limit,
        });
    }
    Ok(())
}

fn require_generation_count(
    field: &'static str,
    actual: u64,
    limit: u64,
) -> ReachabilityResult<()> {
    if actual > limit {
        return Err(ReachabilityError::ReachabilityLimitExceeded {
            field,
            actual,
            limit,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataArtifactMetadata {
    reference: ArtifactRef,
    database_id: DatabaseId,
    table_id: ObjectId,
    segment_id: SegmentId,
    catalog_generation: CatalogGeneration,
    segment_kind: SegmentKind,
    min_transaction_id: u64,
    max_transaction_id: u64,
    row_count: u64,
}

impl DataArtifactMetadata {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        reference: ArtifactRef,
        database_id: DatabaseId,
        table_id: ObjectId,
        segment_id: SegmentId,
        catalog_generation: CatalogGeneration,
        segment_kind: SegmentKind,
        min_transaction_id: u64,
        max_transaction_id: u64,
        row_count: u64,
    ) -> Self {
        Self {
            reference,
            database_id,
            table_id,
            segment_id,
            catalog_generation,
            segment_kind,
            min_transaction_id,
            max_transaction_id,
            row_count,
        }
    }

    pub const fn reference(self) -> ArtifactRef {
        self.reference
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexArtifactMetadata {
    reference: ArtifactRef,
    database_id: DatabaseId,
    table_id: ObjectId,
    segment_id: SegmentId,
    catalog_generation: CatalogGeneration,
    data_artifact_id: ArtifactId,
    data_body_sha256: [u8; 32],
}

impl IndexArtifactMetadata {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        reference: ArtifactRef,
        database_id: DatabaseId,
        table_id: ObjectId,
        segment_id: SegmentId,
        catalog_generation: CatalogGeneration,
        data_artifact_id: ArtifactId,
        data_body_sha256: [u8; 32],
    ) -> Self {
        Self {
            reference,
            database_id,
            table_id,
            segment_id,
            catalog_generation,
            data_artifact_id,
            data_body_sha256,
        }
    }

    pub const fn reference(self) -> ArtifactRef {
        self.reference
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactMetadata {
    Data(DataArtifactMetadata),
    Index(IndexArtifactMetadata),
}

impl ArtifactMetadata {
    pub const fn reference(self) -> ArtifactRef {
        match self {
            Self::Data(metadata) => metadata.reference(),
            Self::Index(metadata) => metadata.reference(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactInspection {
    Present {
        metadata: ArtifactMetadata,
        accounted_bytes: u64,
    },
    Missing,
    Invalid {
        detail: String,
    },
}

impl ArtifactInspection {
    pub const fn present(metadata: ArtifactMetadata) -> Self {
        Self::Present {
            metadata,
            accounted_bytes: 0,
        }
    }

    pub const fn present_accounted(metadata: ArtifactMetadata, accounted_bytes: u64) -> Self {
        Self::Present {
            metadata,
            accounted_bytes,
        }
    }

    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::Invalid {
            detail: detail.into(),
        }
    }
}

/// Exact-identity source for a single CONTROL-root traversal.
///
/// Implementations must resolve only the supplied typed identity. The byte
/// budget is an allocation ceiling for that call. Artifact inspection reads
/// and checks fixed metadata plus the streamed body digest, but must not decode
/// or materialize row payload.
pub trait ReachabilitySource {
    fn read_database_manifest(
        &mut self,
        reference: DatabaseManifestRootRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>>;

    fn read_catalog(
        &mut self,
        reference: CatalogRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>>;

    fn read_table_manifest(
        &mut self,
        reference: TableManifestRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>>;

    fn inspect_artifact(
        &mut self,
        reference: ArtifactRef,
        allowance: ReachabilityAllowance,
    ) -> ReachabilityResult<ArtifactInspection>;
}

/// Remaining share of the one database-generation metadata budget.
///
/// Artifact sources must lower their per-file decoder limit to this allowance
/// before any count-derived allocation.  The returned inspection then reports
/// the allocation charged by that decoder so the traversal owner can consume
/// it exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReachabilityAllowance {
    accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl ReachabilityAllowance {
    pub(crate) const fn new(accounted_bytes: u64, max_accounted_bytes: u64) -> Self {
        Self {
            accounted_bytes,
            max_accounted_bytes,
        }
    }

    pub const fn accounted_bytes(self) -> u64 {
        self.accounted_bytes
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }

    pub const fn remaining_bytes(self) -> u64 {
        self.max_accounted_bytes - self.accounted_bytes
    }

    pub fn exceeded(self, additional_bytes: u64) -> ReachabilityError {
        let actual = self.accounted_bytes.saturating_add(additional_bytes);
        ReachabilityError::ReachabilityLimitExceeded {
            field: "accounted bytes",
            actual,
            limit: self.max_accounted_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnavailableIndexReason {
    Missing,
    Invalid(String),
    CrossReferenceMismatch(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableIndex {
    reference: ArtifactRef,
    reason: UnavailableIndexReason,
}

impl UnavailableIndex {
    pub const fn reference(&self) -> ArtifactRef {
        self.reference
    }

    pub const fn reason(&self) -> &UnavailableIndexReason {
        &self.reason
    }
}

#[derive(Debug)]
pub struct ValidatedGeneration {
    control: ControlRecord,
    database_manifest: DatabaseManifest,
    catalog: CatalogPack,
    table_manifests: Vec<TableManifest>,
    data_artifacts: Vec<DataArtifactMetadata>,
    index_artifacts: Vec<IndexArtifactMetadata>,
    unavailable_indexes: Vec<UnavailableIndex>,
    accounted_identities: u64,
    accounted_bytes: u64,
    structural_accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl ValidatedGeneration {
    pub const fn control(&self) -> ControlRecord {
        self.control
    }

    pub const fn database_manifest(&self) -> &DatabaseManifest {
        &self.database_manifest
    }

    pub const fn catalog(&self) -> &CatalogPack {
        &self.catalog
    }

    pub fn table_manifests(&self) -> &[TableManifest] {
        &self.table_manifests
    }

    pub fn data_artifacts(&self) -> &[DataArtifactMetadata] {
        &self.data_artifacts
    }

    pub fn index_artifacts(&self) -> &[IndexArtifactMetadata] {
        &self.index_artifacts
    }

    pub fn unavailable_indexes(&self) -> &[UnavailableIndex] {
        &self.unavailable_indexes
    }

    pub const fn accounted_identities(&self) -> u64 {
        self.accounted_identities
    }

    pub const fn accounted_bytes(&self) -> u64 {
        self.accounted_bytes
    }

    pub const fn structural_accounted_bytes(&self) -> u64 {
        self.structural_accounted_bytes
    }

    pub(crate) const fn runtime_metadata_allowance(&self) -> ReachabilityAllowance {
        ReachabilityAllowance::new(self.structural_accounted_bytes, self.max_accounted_bytes)
    }

    /// Transfer the validated owners into recovery without cloning the
    /// catalog graph or every table manifest. The compact artifact metadata
    /// vectors are validation-only and are dropped here; runtime layouts are
    /// reopened under the returned shared allowance.
    pub(crate) fn into_runtime_parts(
        self,
    ) -> (
        DatabaseManifest,
        Vec<TableManifest>,
        CatalogPack,
        Vec<UnavailableIndex>,
        ReachabilityAllowance,
    ) {
        let allowance = self.runtime_metadata_allowance();
        (
            self.database_manifest,
            self.table_manifests,
            self.catalog,
            self.unavailable_indexes,
            allowance,
        )
    }
}

pub fn validate_control_generation(
    control: ControlRecord,
    source: &mut impl ReachabilitySource,
) -> ReachabilityResult<ValidatedGeneration> {
    validate_control_generation_with_limits(control, source, ReachabilityLimits::default())
}

pub fn validate_control_generation_with_limits(
    control: ControlRecord,
    source: &mut impl ReachabilitySource,
    limits: ReachabilityLimits,
) -> ReachabilityResult<ValidatedGeneration> {
    let mut budget = ReachabilityBudget::new(limits);
    budget.add_identity()?;
    let database_bytes = source
        .read_database_manifest(
            control.database_manifest(),
            budget.remaining_bytes().min(MAX_MANIFEST_FILE_BYTES),
        )?
        .ok_or_else(|| ReachabilityError::MissingRequiredNode {
            node: ReachableNodeKind::DatabaseManifest,
            identity: control.database_manifest().id().to_string(),
        })?;
    budget.add_bytes(database_bytes.len() as u64)?;
    let database_decoded_bound = decoded_allocation_bound(
        database_bytes.len(),
        MANIFEST_DECODED_BOUND_MULTIPLIER,
        budget.limits.max_accounted_bytes,
    )?;
    budget.add_bytes(database_decoded_bound)?;
    let database_body_sha = footer_sha(&database_bytes);
    let database_manifest = decode_database_manifest(&database_bytes)
        .map_err(|error| invalid_node(ReachableNodeKind::DatabaseManifest, error.to_string()))?;
    validate_database_root(control, &database_manifest, database_body_sha)?;
    let manifest_count = u64::try_from(database_manifest.tables().len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(ReachabilityError::ReachabilityLimitExceeded {
            field: "manifest count",
            actual: u64::MAX,
            limit: limits.max_manifests_per_open,
        })?;
    require_generation_count(
        "manifest count",
        manifest_count,
        limits.max_manifests_per_open,
    )?;
    let database_input_bytes = database_bytes.len() as u64;
    drop(database_bytes);
    budget.release_bytes(database_input_bytes);

    budget.add_identity()?;
    let catalog_bytes_expected = database_manifest.catalog().byte_length();
    budget.add_bytes(catalog_bytes_expected)?;
    let catalog_bytes = source
        .read_catalog(database_manifest.catalog(), catalog_bytes_expected)?
        .ok_or_else(|| ReachabilityError::MissingRequiredNode {
            node: ReachableNodeKind::Catalog,
            identity: database_manifest.catalog().id().to_string(),
        })?;
    require_exact_length(
        ReachableNodeKind::Catalog,
        database_manifest.catalog().byte_length(),
        catalog_bytes.len(),
    )?;
    let catalog_decoded_bound = decoded_allocation_bound(
        catalog_bytes.len(),
        CATALOG_DECODED_BOUND_MULTIPLIER,
        budget.limits.max_accounted_bytes,
    )?;
    budget.add_bytes(catalog_decoded_bound)?;
    let catalog = decode_catalog_pack(&catalog_bytes)
        .map_err(|error| invalid_node(ReachableNodeKind::Catalog, error.to_string()))?;
    let catalog_input_bytes = catalog_bytes.len() as u64;
    drop(catalog_bytes);
    budget.release_bytes(catalog_input_bytes);
    validate_catalog(control, &database_manifest, &catalog)?;
    validate_catalog_table_set(&database_manifest, &catalog)?;

    let mut manifest_ids = HashSet::new();
    let mut segment_ids = HashSet::new();
    let mut artifact_ids = HashSet::new();
    let mut table_manifests = Vec::with_capacity(database_manifest.tables().len());
    let mut data_artifacts = Vec::new();
    let mut index_artifacts = Vec::new();
    let mut unavailable_indexes = Vec::new();
    let mut artifact_accounted_bytes = 0_u64;
    let mut segment_count = 0_u64;

    for table_reference in database_manifest.tables() {
        let manifest_reference = table_reference.manifest();
        if !manifest_ids.insert(manifest_reference.id()) {
            return Err(ReachabilityError::DuplicateIdentity {
                node: ReachableNodeKind::TableManifest,
                identity: manifest_reference.id().to_string(),
            });
        }
        budget.add_identity()?;
        let table_bytes_expected = manifest_reference.byte_length();
        budget.add_bytes(table_bytes_expected)?;
        let table_bytes = source
            .read_table_manifest(*table_reference, table_bytes_expected)?
            .ok_or_else(|| ReachabilityError::MissingRequiredNode {
                node: ReachableNodeKind::TableManifest,
                identity: manifest_reference.id().to_string(),
            })?;
        require_exact_length(
            ReachableNodeKind::TableManifest,
            manifest_reference.byte_length(),
            table_bytes.len(),
        )?;
        let table_decoded_bound = decoded_allocation_bound(
            table_bytes.len(),
            MANIFEST_DECODED_BOUND_MULTIPLIER,
            budget.limits.max_accounted_bytes,
        )?;
        budget.add_bytes(table_decoded_bound)?;
        let table_body_sha = footer_sha(&table_bytes);
        let table_manifest = decode_table_manifest(&table_bytes)
            .map_err(|error| invalid_node(ReachableNodeKind::TableManifest, error.to_string()))?;
        validate_table_manifest(
            control,
            &database_manifest,
            *table_reference,
            &table_manifest,
            table_body_sha,
        )?;
        let table_input_bytes = table_bytes.len() as u64;
        drop(table_bytes);
        budget.release_bytes(table_input_bytes);

        let table_segment_count = u64::try_from(table_manifest.segments().len()).map_err(|_| {
            ReachabilityError::ReachabilityLimitExceeded {
                field: "segment count",
                actual: u64::MAX,
                limit: limits.max_segments_per_open,
            }
        })?;
        segment_count = segment_count.checked_add(table_segment_count).ok_or(
            ReachabilityError::ReachabilityLimitExceeded {
                field: "segment count",
                actual: u64::MAX,
                limit: limits.max_segments_per_open,
            },
        )?;
        require_generation_count("segment count", segment_count, limits.max_segments_per_open)?;

        for segment in table_manifest.segments() {
            // Segment descriptors already live in the retained decoded
            // manifest bound.  This additional owner covers the global
            // segment-ID validation set that is not represented by a node
            // returned in ValidatedGeneration.
            budget.add_bytes(ACCOUNTED_IDENTITY_BYTES)?;
            if segment.kind() == SegmentKind::Rows {
                require_edge(
                    "database manifest -> segment descriptor",
                    segment.max_transaction_id() <= database_manifest.transaction_high_water(),
                    "row-segment transaction high-water exceeds database high-water",
                )?;
            }
            if !segment_ids.insert(segment.id()) {
                return Err(ReachabilityError::DuplicateIdentity {
                    node: ReachableNodeKind::SegmentDescriptor,
                    identity: segment.id().to_string(),
                });
            }
            validate_segment_artifacts(
                source,
                &mut budget,
                &mut artifact_ids,
                &table_manifest,
                *segment,
                &mut data_artifacts,
                &mut index_artifacts,
                &mut unavailable_indexes,
                &mut artifact_accounted_bytes,
            )?;
        }
        table_manifests.push(table_manifest);
    }

    let structural_accounted_bytes = budget
        .bytes
        .checked_sub(artifact_accounted_bytes)
        .expect("artifact accounting is a subset of the shared metadata budget");
    Ok(ValidatedGeneration {
        control,
        database_manifest,
        catalog,
        table_manifests,
        data_artifacts,
        index_artifacts,
        unavailable_indexes,
        accounted_identities: budget.identities,
        accounted_bytes: budget.bytes,
        structural_accounted_bytes,
        max_accounted_bytes: budget.limits.max_accounted_bytes,
    })
}

fn validate_database_root(
    control: ControlRecord,
    manifest: &DatabaseManifest,
    body_sha: [u8; 32],
) -> ReachabilityResult<()> {
    require_edge(
        "CONTROL -> database manifest",
        manifest.database_id() == control.database_id(),
        "database ID differs",
    )?;
    require_edge(
        "CONTROL -> database manifest",
        manifest.manifest_id() == control.database_manifest().id(),
        "manifest ID differs",
    )?;
    require_edge(
        "CONTROL -> database manifest",
        manifest.generation() == control.database_generation(),
        "database generation differs",
    )?;
    require_edge(
        "CONTROL -> database manifest",
        body_sha == *control.database_manifest().body_sha256(),
        "body SHA-256 differs",
    )?;
    require_edge(
        "CONTROL -> database manifest",
        manifest.catalog().id() == control.catalog().id()
            && manifest.catalog().generation() == control.catalog().generation()
            && manifest.catalog().body_sha256() == control.catalog().body_sha256(),
        "catalog identity/generation/SHA differs",
    )?;
    require_edge(
        "CONTROL -> database manifest",
        manifest.wal_replay_floor() == control.wal_replay_floor(),
        "WAL replay floor differs",
    )
}

fn validate_catalog(
    control: ControlRecord,
    database_manifest: &DatabaseManifest,
    catalog: &CatalogPack,
) -> ReachabilityResult<()> {
    let meta = catalog.meta();
    let reference = database_manifest.catalog();
    require_edge(
        "database manifest -> catalog",
        meta.database_id() == control.database_id().into_bytes(),
        "database ID differs",
    )?;
    require_edge(
        "database manifest -> catalog",
        meta.catalog_id() == reference.id().into_bytes(),
        "catalog ID differs",
    )?;
    require_edge(
        "database manifest -> catalog",
        meta.catalog_generation() == reference.generation().get(),
        "catalog generation differs",
    )?;
    require_edge(
        "database manifest -> catalog",
        catalog.body_sha256() == reference.body_sha256(),
        "body SHA-256 differs",
    )
}

fn validate_catalog_table_set(
    database_manifest: &DatabaseManifest,
    catalog: &CatalogPack,
) -> ReachabilityResult<()> {
    let mut catalog_tables = catalog
        .graph()
        .objects()
        .filter(|object| object.kind() == ObjectKind::Table)
        .map(|object| object.id())
        .collect::<Vec<_>>();
    catalog_tables.sort_unstable();
    let manifest_tables = database_manifest
        .tables()
        .iter()
        .map(|entry| entry.table_id())
        .collect::<Vec<_>>();
    if catalog_tables != manifest_tables {
        let detail = if let Some(id) = catalog_tables
            .iter()
            .find(|id| manifest_tables.binary_search(id).is_err())
        {
            format!("catalog table {id} has no table-manifest reference")
        } else if let Some(id) = manifest_tables
            .iter()
            .find(|id| catalog_tables.binary_search(id).is_err())
        {
            format!("table-manifest reference {id} has no catalog Table")
        } else {
            "catalog and database-manifest table sets differ".to_owned()
        };
        return Err(ReachabilityError::CrossReferenceMismatch {
            edge: "database manifest -> catalog",
            detail,
        });
    }
    Ok(())
}

fn validate_table_manifest(
    control: ControlRecord,
    database_manifest: &DatabaseManifest,
    reference: TableManifestRef,
    manifest: &TableManifest,
    body_sha: [u8; 32],
) -> ReachabilityResult<()> {
    let manifest_reference = reference.manifest();
    require_edge(
        "database manifest -> table manifest",
        manifest.database_id() == control.database_id(),
        "database ID differs",
    )?;
    require_edge(
        "database manifest -> table manifest",
        manifest.table_id() == reference.table_id(),
        "table ID differs",
    )?;
    require_edge(
        "database manifest -> table manifest",
        manifest.manifest_id() == manifest_reference.id(),
        "manifest ID differs",
    )?;
    require_edge(
        "database manifest -> table manifest",
        manifest.generation() == manifest_reference.generation(),
        "manifest generation differs",
    )?;
    require_edge(
        "database manifest -> table manifest",
        body_sha == *manifest_reference.body_sha256(),
        "body SHA-256 differs",
    )?;
    require_edge(
        "table manifest -> catalog",
        manifest.catalog_generation().get() <= database_manifest.catalog().generation().get(),
        "table manifest is bound to a future catalog generation",
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_segment_artifacts(
    source: &mut impl ReachabilitySource,
    budget: &mut ReachabilityBudget,
    artifact_ids: &mut HashSet<ArtifactId>,
    table_manifest: &TableManifest,
    segment: SegmentDescriptor,
    data_artifacts: &mut Vec<DataArtifactMetadata>,
    index_artifacts: &mut Vec<IndexArtifactMetadata>,
    unavailable_indexes: &mut Vec<UnavailableIndex>,
    artifact_accounted_bytes: &mut u64,
) -> ReachabilityResult<()> {
    let data_reference = segment.data_artifact();
    register_artifact_identity(
        artifact_ids,
        data_reference,
        ReachableNodeKind::DataArtifact,
    )?;
    budget.add_identity()?;
    budget.add_bytes(size_of::<DataArtifactMetadata>() as u64)?;
    let inspection = source.inspect_artifact(data_reference, budget.allowance())?;
    let data = match inspection {
        ArtifactInspection::Present {
            metadata: ArtifactMetadata::Data(metadata),
            accounted_bytes,
        } => {
            account_artifact_bytes(budget, artifact_accounted_bytes, accounted_bytes)?;
            metadata
        }
        ArtifactInspection::Present {
            metadata: ArtifactMetadata::Index(_),
            accounted_bytes,
        } => {
            account_artifact_bytes(budget, artifact_accounted_bytes, accounted_bytes)?;
            return Err(invalid_node(
                ReachableNodeKind::DataArtifact,
                "metadata reports INDEX for required DATA",
            ));
        }
        ArtifactInspection::Missing => {
            return Err(ReachabilityError::MissingRequiredNode {
                node: ReachableNodeKind::DataArtifact,
                identity: data_reference.id().to_string(),
            });
        }
        ArtifactInspection::Invalid { detail } => {
            return Err(invalid_node(ReachableNodeKind::DataArtifact, detail));
        }
    };
    validate_data_metadata(table_manifest, segment, data)?;
    data_artifacts.push(data);

    if let Some(index_reference) = segment.index_artifact() {
        register_artifact_identity(
            artifact_ids,
            index_reference,
            ReachableNodeKind::IndexArtifact,
        )?;
        budget.add_identity()?;
        budget.add_bytes(size_of::<IndexArtifactMetadata>() as u64)?;
        match source.inspect_artifact(index_reference, budget.allowance())? {
            ArtifactInspection::Present {
                metadata: ArtifactMetadata::Index(metadata),
                accounted_bytes,
            } => {
                account_artifact_bytes(budget, artifact_accounted_bytes, accounted_bytes)?;
                match validate_index_metadata(table_manifest, segment, metadata) {
                    Ok(()) => index_artifacts.push(metadata),
                    Err(detail) => unavailable_indexes.push(UnavailableIndex {
                        reference: index_reference,
                        reason: UnavailableIndexReason::CrossReferenceMismatch(detail),
                    }),
                }
            }
            ArtifactInspection::Present {
                metadata: ArtifactMetadata::Data(_),
                accounted_bytes,
            } => {
                account_artifact_bytes(budget, artifact_accounted_bytes, accounted_bytes)?;
                unavailable_indexes.push(UnavailableIndex {
                    reference: index_reference,
                    reason: UnavailableIndexReason::CrossReferenceMismatch(
                        "metadata reports DATA for optional INDEX".to_owned(),
                    ),
                });
            }
            ArtifactInspection::Missing => unavailable_indexes.push(UnavailableIndex {
                reference: index_reference,
                reason: UnavailableIndexReason::Missing,
            }),
            ArtifactInspection::Invalid { detail } => {
                unavailable_indexes.push(UnavailableIndex {
                    reference: index_reference,
                    reason: UnavailableIndexReason::Invalid(detail),
                });
            }
        }
    }
    Ok(())
}

fn account_artifact_bytes(
    budget: &mut ReachabilityBudget,
    artifact_accounted_bytes: &mut u64,
    amount: u64,
) -> ReachabilityResult<()> {
    budget.add_bytes(amount)?;
    *artifact_accounted_bytes = artifact_accounted_bytes.checked_add(amount).ok_or(
        ReachabilityError::ReachabilityLimitExceeded {
            field: "accounted bytes",
            actual: u64::MAX,
            limit: budget.limits.max_accounted_bytes,
        },
    )?;
    Ok(())
}

fn validate_data_metadata(
    table_manifest: &TableManifest,
    segment: SegmentDescriptor,
    metadata: DataArtifactMetadata,
) -> ReachabilityResult<()> {
    let valid = metadata.reference == segment.data_artifact()
        && metadata.reference.kind() == ArtifactKind::Data
        && metadata.database_id == table_manifest.database_id()
        && metadata.table_id == table_manifest.table_id()
        && metadata.segment_id == segment.id()
        // DATA is immutable and records the catalog generation whose column
        // identities/types were used when it was written.  A later catalog
        // generation may rename the table/columns or add/drop metadata without
        // rewriting the segment.  Only a binding from the future is invalid;
        // runtime admission validates the persisted column identities against
        // the selected catalog before exposing the segment.
        && metadata.catalog_generation.get() <= table_manifest.catalog_generation().get()
        && metadata.segment_kind == segment.kind()
        && metadata.min_transaction_id == segment.min_transaction_id()
        && metadata.max_transaction_id == segment.max_transaction_id()
        && metadata.row_count == segment.row_count();
    require_edge(
        "table manifest -> data artifact",
        valid,
        "identity/kind/length/SHA, future catalog binding or repeated segment metadata differs",
    )
}

fn validate_index_metadata(
    table_manifest: &TableManifest,
    segment: SegmentDescriptor,
    metadata: IndexArtifactMetadata,
) -> Result<(), String> {
    let expected = segment
        .index_artifact()
        .expect("index metadata is checked only for a referenced index");
    if metadata.reference != expected || metadata.reference.kind() != ArtifactKind::Index {
        return Err("identity/kind/generation/length/SHA differs".to_owned());
    }
    if metadata.database_id != table_manifest.database_id()
        || metadata.table_id != table_manifest.table_id()
        || metadata.segment_id != segment.id()
        // INDEX packs are rebuildable and may predate the selected catalog.
        // Their logical accelerator definitions are checked against the
        // current catalog at access admission.  A future binding is never
        // reachable from an older table manifest.
        || metadata.catalog_generation.get() > table_manifest.catalog_generation().get()
    {
        return Err(
            "repeated database/table/segment identity or future catalog binding differs".to_owned(),
        );
    }
    if metadata.data_artifact_id != segment.data_artifact().id()
        || metadata.data_body_sha256 != *segment.data_artifact().body_sha256()
    {
        return Err("source DATA identity/SHA differs".to_owned());
    }
    Ok(())
}

fn register_artifact_identity(
    identities: &mut HashSet<ArtifactId>,
    reference: ArtifactRef,
    node: ReachableNodeKind,
) -> ReachabilityResult<()> {
    if !identities.insert(reference.id()) {
        return Err(ReachabilityError::DuplicateIdentity {
            node,
            identity: reference.id().to_string(),
        });
    }
    Ok(())
}

fn require_exact_length(
    node: ReachableNodeKind,
    expected: u64,
    actual: usize,
) -> ReachabilityResult<()> {
    if expected != actual as u64 {
        return Err(invalid_node(
            node,
            format!("byte length is {actual}, expected {expected}"),
        ));
    }
    Ok(())
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..]
        .try_into()
        .expect("decoded V6 file has a complete common footer")
}

fn require_edge(
    edge: &'static str,
    condition: bool,
    detail: impl Into<String>,
) -> ReachabilityResult<()> {
    if !condition {
        return Err(ReachabilityError::CrossReferenceMismatch {
            edge,
            detail: detail.into(),
        });
    }
    Ok(())
}

fn invalid_node(node: ReachableNodeKind, detail: impl Into<String>) -> ReachabilityError {
    ReachabilityError::InvalidRequiredNode {
        node,
        detail: detail.into(),
    }
}

fn decoded_allocation_bound(
    encoded_bytes: usize,
    multiplier: u64,
    limit: u64,
) -> ReachabilityResult<u64> {
    (encoded_bytes as u64).checked_mul(multiplier).ok_or(
        ReachabilityError::ReachabilityLimitExceeded {
            field: "accounted bytes",
            actual: u64::MAX,
            limit,
        },
    )
}

struct ReachabilityBudget {
    limits: ReachabilityLimits,
    identities: u64,
    bytes: u64,
}

impl ReachabilityBudget {
    const fn new(limits: ReachabilityLimits) -> Self {
        // Reachability/GC may use its wider 1 GiB identity-set ceiling, but
        // opening one database generation is governed by the stricter shared
        // metadata ceiling from LIM_OPEN_METADATA_BYTES.
        let limits = ReachabilityLimits {
            max_identities: limits.max_identities,
            max_accounted_bytes: if limits.max_accounted_bytes < MAX_OPEN_METADATA_BYTES {
                limits.max_accounted_bytes
            } else {
                MAX_OPEN_METADATA_BYTES
            },
            max_manifests_per_open: limits.max_manifests_per_open,
            max_segments_per_open: limits.max_segments_per_open,
        };
        Self {
            limits,
            identities: 0,
            bytes: 0,
        }
    }

    fn add_identity(&mut self) -> ReachabilityResult<()> {
        self.identities =
            self.identities
                .checked_add(1)
                .ok_or(ReachabilityError::ReachabilityLimitExceeded {
                    field: "identity count",
                    actual: u64::MAX,
                    limit: self.limits.max_identities,
                })?;
        if self.identities > self.limits.max_identities {
            return Err(ReachabilityError::ReachabilityLimitExceeded {
                field: "identity count",
                actual: self.identities,
                limit: self.limits.max_identities,
            });
        }
        self.add_bytes(ACCOUNTED_IDENTITY_BYTES)
    }

    fn add_bytes(&mut self, amount: u64) -> ReachabilityResult<()> {
        self.bytes =
            self.bytes
                .checked_add(amount)
                .ok_or(ReachabilityError::ReachabilityLimitExceeded {
                    field: "accounted bytes",
                    actual: u64::MAX,
                    limit: self.limits.max_accounted_bytes,
                })?;
        if self.bytes > self.limits.max_accounted_bytes {
            return Err(ReachabilityError::ReachabilityLimitExceeded {
                field: "accounted bytes",
                actual: self.bytes,
                limit: self.limits.max_accounted_bytes,
            });
        }
        Ok(())
    }

    fn release_bytes(&mut self, amount: u64) {
        self.bytes = self
            .bytes
            .checked_sub(amount)
            .expect("reachability releases only previously accounted temporary bytes");
    }

    const fn remaining_bytes(&self) -> u64 {
        self.limits.max_accounted_bytes - self.bytes
    }

    const fn allowance(&self) -> ReachabilityAllowance {
        ReachabilityAllowance::new(self.bytes, self.limits.max_accounted_bytes)
    }
}

#[cfg(test)]
mod metadata_budget_tests {
    use super::*;

    #[test]
    fn database_open_is_stricter_than_the_general_reachability_ceiling() {
        let general = ReachabilityLimits::default();
        assert_eq!(general.max_accounted_bytes(), MAX_REACHABILITY_BYTES);

        let open = ReachabilityBudget::new(general);
        assert_eq!(open.limits.max_accounted_bytes(), MAX_OPEN_METADATA_BYTES);

        let lowered = ReachabilityLimits::new(100, 4096).unwrap();
        assert_eq!(ReachabilityBudget::new(lowered).remaining_bytes(), 4096);
    }

    #[test]
    fn generation_count_limits_are_lower_only_at_the_hard_boundary() {
        let below = ReachabilityLimits::default()
            .with_generation_counts(
                MAX_TOTAL_MANIFESTS_PER_OPEN - 1,
                MAX_TOTAL_SEGMENTS_PER_OPEN - 1,
            )
            .unwrap();
        assert_eq!(
            below.max_manifests_per_open(),
            MAX_TOTAL_MANIFESTS_PER_OPEN - 1
        );
        assert_eq!(
            below.max_segments_per_open(),
            MAX_TOTAL_SEGMENTS_PER_OPEN - 1
        );

        let exact = ReachabilityLimits::default()
            .with_generation_counts(MAX_TOTAL_MANIFESTS_PER_OPEN, MAX_TOTAL_SEGMENTS_PER_OPEN)
            .unwrap();
        assert_eq!(exact.max_manifests_per_open(), MAX_TOTAL_MANIFESTS_PER_OPEN);
        assert_eq!(exact.max_segments_per_open(), MAX_TOTAL_SEGMENTS_PER_OPEN);

        assert!(matches!(
            ReachabilityLimits::default().with_generation_counts(
                MAX_TOTAL_MANIFESTS_PER_OPEN + 1,
                MAX_TOTAL_SEGMENTS_PER_OPEN,
            ),
            Err(ReachabilityError::InvalidReachabilityLimit {
                field: "manifest count",
                requested,
                hard_limit: MAX_TOTAL_MANIFESTS_PER_OPEN,
            }) if requested == MAX_TOTAL_MANIFESTS_PER_OPEN + 1
        ));
        assert!(matches!(
            ReachabilityLimits::default().with_generation_counts(
                MAX_TOTAL_MANIFESTS_PER_OPEN,
                MAX_TOTAL_SEGMENTS_PER_OPEN + 1,
            ),
            Err(ReachabilityError::InvalidReachabilityLimit {
                field: "segment count",
                requested,
                hard_limit: MAX_TOTAL_SEGMENTS_PER_OPEN,
            }) if requested == MAX_TOTAL_SEGMENTS_PER_OPEN + 1
        ));
    }
}
