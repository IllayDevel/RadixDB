use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::v6::{FormatError, FormatResult, StagingOwner};

use super::super::PhysicalGenerationSnapshot;
use super::model::{
    ActiveLeaseSet, ArtifactBuildKind, ArtifactBuildLease, BuildLeaseRecord, GenerationLeaseRecord,
    LeaseLimits, PhysicalGenerationLease,
};

#[derive(Debug)]
pub(crate) struct LeaseRegistry {
    limits: LeaseLimits,
    state: Mutex<LeaseRegistryState>,
}

#[derive(Debug, Default)]
struct LeaseRegistryState {
    generations: Vec<Weak<GenerationLeaseRecord>>,
    artifact_builds: Vec<Weak<BuildLeaseRecord>>,
}

impl LeaseRegistry {
    pub(crate) fn new(limits: LeaseLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(LeaseRegistryState::default()),
        })
    }

    pub(crate) fn register_generation(
        self: &Arc<Self>,
        snapshot: Arc<PhysicalGenerationSnapshot>,
    ) -> FormatResult<PhysicalGenerationLease> {
        let mut state = self.state.lock();
        let live = collect_live(&mut state.generations);
        if let Some(record) = live.iter().find(|record| {
            Arc::ptr_eq(record.snapshot(), &snapshot)
                || record.snapshot().control() == snapshot.control()
        }) {
            if record.snapshot().control() == snapshot.control()
                && !Arc::ptr_eq(record.snapshot(), &snapshot)
                && (record.snapshot().database_manifest() != snapshot.database_manifest()
                    || record.snapshot().table_manifests() != snapshot.table_manifests())
            {
                return invalid("one CONTROL identity resolves to different runtime graphs");
            }
            return Ok(PhysicalGenerationLease {
                record: Arc::clone(record),
                registry: Arc::clone(self),
            });
        }
        let actual = live.len().saturating_add(1);
        if actual > self.limits.max_generation_leases() {
            return Err(FormatError::LeaseLimitExceeded {
                field: "physical generations",
                actual,
                limit: self.limits.max_generation_leases(),
            });
        }
        let record = Arc::new(GenerationLeaseRecord::new(snapshot));
        state.generations.push(Arc::downgrade(&record));
        Ok(PhysicalGenerationLease {
            record,
            registry: Arc::clone(self),
        })
    }

    pub(crate) fn register_build(
        self: &Arc<Self>,
        kind: ArtifactBuildKind,
        source: &PhysicalGenerationLease,
        staging_owner: StagingOwner,
    ) -> FormatResult<ArtifactBuildLease> {
        if !Arc::ptr_eq(self, &source.registry) {
            return invalid("artifact build source belongs to another publisher");
        }
        validate_target_generation(kind, source, staging_owner)?;
        let mut state = self.state.lock();
        let live = collect_live(&mut state.artifact_builds);
        if live
            .iter()
            .any(|record| record.staging_owner() == staging_owner)
        {
            return invalid("staging owner already has an active artifact build");
        }
        let actual = live.len().saturating_add(1);
        if actual > self.limits.max_artifact_builds() {
            return Err(FormatError::LeaseLimitExceeded {
                field: "artifact builds",
                actual,
                limit: self.limits.max_artifact_builds(),
            });
        }
        let record = Arc::new(BuildLeaseRecord::new(kind, source.clone(), staging_owner));
        state.artifact_builds.push(Arc::downgrade(&record));
        Ok(ArtifactBuildLease { record })
    }

    pub(crate) fn active(self: &Arc<Self>) -> ActiveLeaseSet {
        let mut state = self.state.lock();
        let generations = collect_live(&mut state.generations)
            .into_iter()
            .map(|record| PhysicalGenerationLease {
                record,
                registry: Arc::clone(self),
            })
            .collect();
        let artifact_builds = collect_live(&mut state.artifact_builds)
            .into_iter()
            .map(|record| ArtifactBuildLease { record })
            .collect();
        ActiveLeaseSet::new(generations, artifact_builds)
    }
}

fn validate_target_generation(
    kind: ArtifactBuildKind,
    source: &PhysicalGenerationLease,
    owner: StagingOwner,
) -> FormatResult<()> {
    let expected = match kind {
        ArtifactBuildKind::Snapshot => source.database_generation(),
        ArtifactBuildKind::Seal
        | ArtifactBuildKind::DdlPublication
        | ArtifactBuildKind::Compaction
        | ArtifactBuildKind::IndexRebuild => source.database_generation().checked_next()?,
    };
    if owner.intended_generation() != expected {
        return invalid("staging owner targets the wrong database generation");
    }
    Ok(())
}

fn collect_live<T>(entries: &mut Vec<Weak<T>>) -> Vec<Arc<T>> {
    let live = entries.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
    entries.retain(|entry| entry.strong_count() > 0);
    live
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidLease { detail })
}
