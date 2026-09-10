use crate::v6::{
    ArtifactBuildKind, ArtifactBuildLease, ArtifactRef, CompleteStagingSet, ControlRecord,
    FormatError, FormatResult, PhysicalGenerationLease,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceKind {
    Compaction,
    IndexRebuild,
}

#[derive(Debug)]
pub struct FrozenMaintenance {
    kind: MaintenanceKind,
    expected_control: ControlRecord,
    target_control: ControlRecord,
    staging: CompleteStagingSet,
    build: ArtifactBuildLease,
}

impl FrozenMaintenance {
    pub fn new(
        kind: MaintenanceKind,
        expected_control: ControlRecord,
        target_control: ControlRecord,
        staging: CompleteStagingSet,
        build: ArtifactBuildLease,
    ) -> FormatResult<Self> {
        let expected_kind = match kind {
            MaintenanceKind::Compaction => ArtifactBuildKind::Compaction,
            MaintenanceKind::IndexRebuild => ArtifactBuildKind::IndexRebuild,
        };
        if build.kind() != expected_kind {
            return invalid("maintenance kind differs from artifact build lease");
        }
        if build.source().snapshot().control() != expected_control {
            return invalid("maintenance build source differs from expected CONTROL");
        }
        if build.staging_owner() != staging.owner() {
            return invalid("maintenance build lease targets another staging owner");
        }
        if target_control.database_id() != expected_control.database_id() {
            return invalid("maintenance target belongs to another database");
        }
        if target_control.database_generation()
            != expected_control.database_generation().checked_next()?
        {
            return invalid("maintenance target is not the next database generation");
        }
        if target_control.catalog() != expected_control.catalog() {
            return invalid("maintenance publication changes catalog authority");
        }
        if target_control.wal_replay_floor() != expected_control.wal_replay_floor() {
            return invalid("maintenance publication changes WAL replay floor");
        }
        Ok(Self {
            kind,
            expected_control,
            target_control,
            staging,
            build,
        })
    }

    pub const fn kind(&self) -> MaintenanceKind {
        self.kind
    }

    pub const fn expected_control(&self) -> ControlRecord {
        self.expected_control
    }

    pub const fn target_control(&self) -> ControlRecord {
        self.target_control
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        MaintenanceKind,
        ControlRecord,
        ControlRecord,
        CompleteStagingSet,
        ArtifactBuildLease,
    ) {
        (
            self.kind,
            self.expected_control,
            self.target_control,
            self.staging,
            self.build,
        )
    }
}

#[derive(Debug)]
pub struct MaintenanceOutcome {
    kind: MaintenanceKind,
    lease: PhysicalGenerationLease,
    retired_artifacts: Vec<ArtifactRef>,
}

impl MaintenanceOutcome {
    pub(crate) fn new(
        kind: MaintenanceKind,
        lease: PhysicalGenerationLease,
        retired_artifacts: Vec<ArtifactRef>,
    ) -> Self {
        Self {
            kind,
            lease,
            retired_artifacts,
        }
    }

    pub const fn kind(&self) -> MaintenanceKind {
        self.kind
    }

    pub const fn lease(&self) -> &PhysicalGenerationLease {
        &self.lease
    }

    /// Exact immutable artifacts removed from the new manifest graph. They
    /// remain valid for pinned readers and are only GC candidates later.
    pub fn retired_artifacts(&self) -> &[ArtifactRef] {
        &self.retired_artifacts
    }
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidMaintenance { detail })
}
