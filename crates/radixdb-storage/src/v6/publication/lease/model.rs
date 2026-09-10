use std::sync::Arc;

use crate::v6::{
    ArtifactRef, CompleteStagingSet, DatabaseGeneration, FormatError, FormatResult, ManifestId,
    SegmentId, StagedIndexReplacement, StagingOwner, WriterInstanceId,
};
use radixdb_catalog::ObjectId;

use super::super::{PhysicalGenerationSnapshot, PreparedIndexReplacement};
use super::registry::LeaseRegistry;

pub const MAX_ACTIVE_GENERATION_LEASES: usize = 4_096;
pub const MAX_ACTIVE_ARTIFACT_BUILDS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseLimits {
    max_generation_leases: usize,
    max_artifact_builds: usize,
}

impl LeaseLimits {
    pub fn new(max_generation_leases: usize, max_artifact_builds: usize) -> FormatResult<Self> {
        validate_limit(
            "physical generations",
            max_generation_leases,
            MAX_ACTIVE_GENERATION_LEASES,
        )?;
        validate_limit(
            "artifact builds",
            max_artifact_builds,
            MAX_ACTIVE_ARTIFACT_BUILDS,
        )?;
        Ok(Self {
            max_generation_leases,
            max_artifact_builds,
        })
    }

    pub const fn max_generation_leases(self) -> usize {
        self.max_generation_leases
    }

    pub const fn max_artifact_builds(self) -> usize {
        self.max_artifact_builds
    }
}

impl Default for LeaseLimits {
    fn default() -> Self {
        Self {
            max_generation_leases: MAX_ACTIVE_GENERATION_LEASES,
            max_artifact_builds: MAX_ACTIVE_ARTIFACT_BUILDS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactBuildKind {
    Seal,
    DdlPublication,
    Compaction,
    Snapshot,
    IndexRebuild,
}

#[derive(Debug)]
pub(crate) struct GenerationLeaseRecord {
    snapshot: Arc<PhysicalGenerationSnapshot>,
}

impl GenerationLeaseRecord {
    pub(crate) const fn new(snapshot: Arc<PhysicalGenerationSnapshot>) -> Self {
        Self { snapshot }
    }

    pub(crate) fn snapshot(&self) -> &Arc<PhysicalGenerationSnapshot> {
        &self.snapshot
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalGenerationLease {
    pub(crate) record: Arc<GenerationLeaseRecord>,
    pub(crate) registry: Arc<LeaseRegistry>,
}

impl PhysicalGenerationLease {
    pub fn snapshot(&self) -> &PhysicalGenerationSnapshot {
        self.record.snapshot()
    }

    pub fn database_generation(&self) -> DatabaseGeneration {
        self.snapshot().control().database_generation()
    }

    pub fn index_artifact(&self, table_id: ObjectId, segment_id: SegmentId) -> Option<ArtifactRef> {
        self.snapshot()
            .segment(table_id, segment_id)
            .and_then(crate::v6::SegmentDescriptor::index_artifact)
    }

    pub fn pinned_artifacts(&self) -> Vec<ArtifactRef> {
        self.snapshot().artifact_references()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_index_replacement(
        &self,
        staged: StagedIndexReplacement,
        table_manifest_id: ManifestId,
        database_manifest_id: ManifestId,
        writer_instance_id: WriterInstanceId,
        created_unix_ns: u64,
    ) -> FormatResult<PreparedIndexReplacement> {
        PreparedIndexReplacement::new(
            self,
            staged,
            table_manifest_id,
            database_manifest_id,
            writer_instance_id,
            created_unix_ns,
        )
    }
}

#[derive(Debug)]
pub(crate) struct BuildLeaseRecord {
    kind: ArtifactBuildKind,
    source: PhysicalGenerationLease,
    staging_owner: StagingOwner,
}

impl BuildLeaseRecord {
    pub(crate) const fn new(
        kind: ArtifactBuildKind,
        source: PhysicalGenerationLease,
        staging_owner: StagingOwner,
    ) -> Self {
        Self {
            kind,
            source,
            staging_owner,
        }
    }

    pub(crate) const fn kind(&self) -> ArtifactBuildKind {
        self.kind
    }

    pub(crate) const fn source(&self) -> &PhysicalGenerationLease {
        &self.source
    }

    pub(crate) const fn staging_owner(&self) -> StagingOwner {
        self.staging_owner
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactBuildLease {
    pub(crate) record: Arc<BuildLeaseRecord>,
}

impl ArtifactBuildLease {
    pub fn kind(&self) -> ArtifactBuildKind {
        self.record.kind()
    }

    pub fn source(&self) -> &PhysicalGenerationLease {
        self.record.source()
    }

    pub fn source_artifacts(&self) -> Vec<ArtifactRef> {
        self.record.source.pinned_artifacts()
    }

    pub fn staging_owner(&self) -> StagingOwner {
        self.record.staging_owner()
    }

    pub(crate) fn validate_publication(
        &self,
        source: &PhysicalGenerationSnapshot,
        staging: &CompleteStagingSet,
        expected_kind: Option<ArtifactBuildKind>,
    ) -> FormatResult<()> {
        if expected_kind.is_some_and(|kind| kind != self.kind()) {
            return invalid("artifact build kind differs from publication kind");
        }
        if self.source().snapshot().control() != source.control() {
            return invalid("artifact build source differs from publication source");
        }
        if self.staging_owner() != staging.owner() {
            return invalid("artifact build staging owner differs from publication staging set");
        }
        Ok(())
    }
}

/// Stable lease roots for one reachability/GC decision. The contained strong
/// registrations prevent a root from disappearing while the decision runs.
#[derive(Debug)]
pub struct ActiveLeaseSet {
    generation_leases: Vec<PhysicalGenerationLease>,
    artifact_builds: Vec<ArtifactBuildLease>,
}

impl ActiveLeaseSet {
    pub(crate) const fn new(
        generation_leases: Vec<PhysicalGenerationLease>,
        artifact_builds: Vec<ArtifactBuildLease>,
    ) -> Self {
        Self {
            generation_leases,
            artifact_builds,
        }
    }

    pub fn generations(&self) -> &[PhysicalGenerationLease] {
        &self.generation_leases
    }

    pub fn artifact_builds(&self) -> &[ArtifactBuildLease] {
        &self.artifact_builds
    }

    pub fn pinned_artifacts(&self) -> Vec<ArtifactRef> {
        let mut artifacts = self
            .generation_leases
            .iter()
            .flat_map(PhysicalGenerationLease::pinned_artifacts)
            .collect::<Vec<_>>();
        artifacts.sort_unstable_by_key(|reference| {
            (reference.kind().tag(), reference.id().into_bytes())
        });
        artifacts.dedup();
        artifacts
    }

    pub fn staging_owners(&self) -> Vec<StagingOwner> {
        let mut owners = self
            .artifact_builds
            .iter()
            .map(ArtifactBuildLease::staging_owner)
            .collect::<Vec<_>>();
        owners.sort_unstable_by_key(|owner| {
            (
                owner.writer_instance_id().into_bytes(),
                owner.publication_id().into_bytes(),
            )
        });
        owners
    }
}

fn validate_limit(field: &'static str, requested: usize, hard_limit: usize) -> FormatResult<()> {
    if requested == 0 || requested > hard_limit {
        return Err(FormatError::InvalidLeaseLimit {
            field,
            requested,
            hard_limit,
        });
    }
    Ok(())
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidLease { detail })
}
