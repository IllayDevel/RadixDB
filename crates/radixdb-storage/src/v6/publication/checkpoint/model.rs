use std::path::{Path, PathBuf};

use crate::v6::{
    ArtifactBuildKind, ArtifactBuildLease, CompleteStagingSet, ControlRecord, FormatError,
    FormatResult, PhysicalGenerationLease, WalGeneration,
};

use super::retire_obsolete_wal;

#[derive(Debug)]
pub struct FrozenCheckpoint {
    expected_control: ControlRecord,
    target_control: ControlRecord,
    staging: CompleteStagingSet,
    build: ArtifactBuildLease,
    obsolete_wal: Vec<WalGeneration>,
}

impl FrozenCheckpoint {
    pub fn new(
        expected_control: ControlRecord,
        target_control: ControlRecord,
        staging: CompleteStagingSet,
        build: ArtifactBuildLease,
        obsolete_wal: Vec<WalGeneration>,
    ) -> FormatResult<Self> {
        if build.kind() != ArtifactBuildKind::Seal {
            return invalid("checkpoint requires a seal build lease");
        }
        if build.source().snapshot().control() != expected_control {
            return invalid("checkpoint build source differs from expected CONTROL");
        }
        if build.staging_owner() != staging.owner() {
            return invalid("checkpoint build lease targets another staging owner");
        }
        if target_control.database_id() != expected_control.database_id() {
            return invalid("checkpoint target belongs to another database");
        }
        if target_control.database_generation()
            != expected_control.database_generation().checked_next()?
        {
            return invalid("checkpoint target is not the next database generation");
        }
        if target_control.wal_replay_floor().generation()
            <= expected_control.wal_replay_floor().generation()
        {
            return invalid("checkpoint target does not advance the WAL generation");
        }
        if target_control.wal_replay_floor().lsn() < expected_control.wal_replay_floor().lsn() {
            return invalid("checkpoint WAL replay floor moves backwards");
        }
        if obsolete_wal.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid("obsolete WAL generations are not strictly increasing");
        }
        let retained_floor = expected_control.wal_replay_floor().generation();
        if obsolete_wal
            .iter()
            .any(|generation| *generation >= retained_floor)
        {
            return invalid("WAL generation is still required by retained CONTROL");
        }
        Ok(Self {
            expected_control,
            target_control,
            staging,
            build,
            obsolete_wal,
        })
    }

    pub const fn expected_control(&self) -> ControlRecord {
        self.expected_control
    }

    pub const fn target_control(&self) -> ControlRecord {
        self.target_control
    }

    pub fn obsolete_wal(&self) -> &[WalGeneration] {
        &self.obsolete_wal
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ControlRecord,
        ControlRecord,
        CompleteStagingSet,
        ArtifactBuildLease,
        Vec<WalGeneration>,
    ) {
        (
            self.expected_control,
            self.target_control,
            self.staging,
            self.build,
            self.obsolete_wal,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRetirementReport {
    requested: usize,
    renamed: usize,
    unlinked: usize,
    already_absent: usize,
}

impl WalRetirementReport {
    pub(crate) const fn new(
        requested: usize,
        renamed: usize,
        unlinked: usize,
        already_absent: usize,
    ) -> Self {
        Self {
            requested,
            renamed,
            unlinked,
            already_absent,
        }
    }

    pub const fn requested(self) -> usize {
        self.requested
    }

    pub const fn renamed(self) -> usize {
        self.renamed
    }

    pub const fn unlinked(self) -> usize {
        self.unlinked
    }

    pub const fn already_absent(self) -> usize {
        self.already_absent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRetirementStatus {
    NotRequired,
    Complete(WalRetirementReport),
    Deferred(FormatError),
}

#[derive(Debug)]
pub struct CheckpointOutcome {
    lease: PhysicalGenerationLease,
    fence: WalRetirementFence,
    wal_retirement: WalRetirementStatus,
}

impl CheckpointOutcome {
    pub(crate) fn finish(
        lease: PhysicalGenerationLease,
        root: &Path,
        source_control: ControlRecord,
        target_control: ControlRecord,
        obsolete_wal: Vec<WalGeneration>,
    ) -> Self {
        let fence = WalRetirementFence {
            root: root.to_path_buf(),
            source_control,
            target_control,
            obsolete_wal,
        };
        let wal_retirement = if fence.obsolete_wal.is_empty() {
            WalRetirementStatus::NotRequired
        } else {
            match retire_obsolete_wal(&fence) {
                Ok(report) => WalRetirementStatus::Complete(report),
                Err(error) => WalRetirementStatus::Deferred(error),
            }
        };
        Self {
            lease,
            fence,
            wal_retirement,
        }
    }

    pub const fn lease(&self) -> &PhysicalGenerationLease {
        &self.lease
    }

    pub const fn wal_retirement(&self) -> &WalRetirementStatus {
        &self.wal_retirement
    }

    /// Retry only the post-commit WAL retirement phase. The durable checkpoint
    /// remains successful even when cleanup is deferred.
    pub fn retry_wal_retirement(&mut self) -> FormatResult<WalRetirementReport> {
        if self.fence.obsolete_wal.is_empty() {
            let report = WalRetirementReport::new(0, 0, 0, 0);
            self.wal_retirement = WalRetirementStatus::NotRequired;
            return Ok(report);
        }
        match retire_obsolete_wal(&self.fence) {
            Ok(report) => {
                self.wal_retirement = WalRetirementStatus::Complete(report);
                Ok(report)
            }
            Err(error) => {
                self.wal_retirement = WalRetirementStatus::Deferred(error.clone());
                Err(error)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct WalRetirementFence {
    pub(crate) root: PathBuf,
    pub(crate) source_control: ControlRecord,
    pub(crate) target_control: ControlRecord,
    pub(crate) obsolete_wal: Vec<WalGeneration>,
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidCheckpoint { detail })
}
