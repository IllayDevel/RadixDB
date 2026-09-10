use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultPoint {
    WalAppend,
    WalSync,
    ConstraintValidation,
    IndexPublication,
    CommitVisibility,
    SnapshotWrite,
    SnapshotSync,
    SnapshotRename,
    CheckpointManifest,
    VolumeSeal,
    VolumePublish,
    CatalogPublication,
    BackupWrite,
    TcpResponse,
    FilesystemCapacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableArtifact {
    Wal,
    Snapshot,
    Checkpoint,
    Manifest,
    Volume,
    Catalog,
    Backup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorruptionMutation {
    BitFlip,
    TruncatedTail,
    TruncatedHeader,
    StaleManifest,
    LostNewestGeneration,
    WrongChecksum,
    WrongVersion,
    WrongLength,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryClass {
    RecoverPreviousGeneration,
    RecoverCommittedPrefix,
    IgnoreUnpublishedArtifact,
    FailClosed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorruptionCase {
    pub id: String,
    pub artifact: DurableArtifact,
    pub mutation: CorruptionMutation,
    pub expected: RecoveryClass,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    pub memory_bytes: u64,
    pub file_descriptors: u64,
    pub threads: u64,
    pub cpu_percent: u8,
    pub io_bytes_per_second: u64,
    pub connections: u64,
    pub inflight_frame_bytes: u64,
}

impl ResourceLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.memory_bytes == 0
            || self.file_descriptors == 0
            || self.threads == 0
            || self.io_bytes_per_second == 0
            || self.connections == 0
            || self.inflight_frame_bytes == 0
        {
            return Err("resource limits must all be non-zero".to_string());
        }
        if !(1..=100).contains(&self.cpu_percent) {
            return Err("cpu_percent must be in 1..=100".to_string());
        }
        Ok(())
    }
}

pub fn b6_fault_schedule() -> FaultSchedule {
    FaultSchedule {
        activations: vec![
            FaultActivation {
                after_event: 1,
                point: FaultPoint::WalAppend,
            },
            FaultActivation {
                after_event: 2,
                point: FaultPoint::WalSync,
            },
            FaultActivation {
                after_event: 3,
                point: FaultPoint::ConstraintValidation,
            },
            FaultActivation {
                after_event: 4,
                point: FaultPoint::IndexPublication,
            },
            FaultActivation {
                after_event: 5,
                point: FaultPoint::CommitVisibility,
            },
            FaultActivation {
                after_event: 6,
                point: FaultPoint::SnapshotWrite,
            },
            FaultActivation {
                after_event: 7,
                point: FaultPoint::SnapshotSync,
            },
            FaultActivation {
                after_event: 8,
                point: FaultPoint::SnapshotRename,
            },
            FaultActivation {
                after_event: 9,
                point: FaultPoint::CheckpointManifest,
            },
            FaultActivation {
                after_event: 10,
                point: FaultPoint::VolumeSeal,
            },
            FaultActivation {
                after_event: 11,
                point: FaultPoint::VolumePublish,
            },
            FaultActivation {
                after_event: 12,
                point: FaultPoint::CatalogPublication,
            },
            FaultActivation {
                after_event: 13,
                point: FaultPoint::BackupWrite,
            },
            FaultActivation {
                after_event: 14,
                point: FaultPoint::FilesystemCapacity,
            },
            FaultActivation {
                after_event: 15,
                point: FaultPoint::TcpResponse,
            },
        ],
    }
}

pub fn b6_corruption_corpus() -> Vec<CorruptionCase> {
    vec![
        CorruptionCase {
            id: "wal-bit-flip".into(),
            artifact: DurableArtifact::Wal,
            mutation: CorruptionMutation::BitFlip,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "wal-truncated-tail".into(),
            artifact: DurableArtifact::Wal,
            mutation: CorruptionMutation::TruncatedTail,
            expected: RecoveryClass::RecoverCommittedPrefix,
        },
        CorruptionCase {
            id: "snapshot-wrong-checksum".into(),
            artifact: DurableArtifact::Snapshot,
            mutation: CorruptionMutation::WrongChecksum,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "manifest-truncated-header".into(),
            artifact: DurableArtifact::Manifest,
            mutation: CorruptionMutation::TruncatedHeader,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "manifest-stale-generation".into(),
            artifact: DurableArtifact::Manifest,
            mutation: CorruptionMutation::StaleManifest,
            expected: RecoveryClass::RecoverPreviousGeneration,
        },
        CorruptionCase {
            id: "manifest-lost-newest-generation".into(),
            artifact: DurableArtifact::Manifest,
            mutation: CorruptionMutation::LostNewestGeneration,
            expected: RecoveryClass::RecoverPreviousGeneration,
        },
        CorruptionCase {
            id: "volume-wrong-version".into(),
            artifact: DurableArtifact::Volume,
            mutation: CorruptionMutation::WrongVersion,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "volume-wrong-length".into(),
            artifact: DurableArtifact::Volume,
            mutation: CorruptionMutation::WrongLength,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "backup-truncated-member".into(),
            artifact: DurableArtifact::Backup,
            mutation: CorruptionMutation::TruncatedTail,
            expected: RecoveryClass::FailClosed,
        },
        CorruptionCase {
            id: "orphan-unpublished-volume".into(),
            artifact: DurableArtifact::Volume,
            mutation: CorruptionMutation::LostNewestGeneration,
            expected: RecoveryClass::IgnoreUnpublishedArtifact,
        },
    ]
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultActivation {
    pub after_event: u64,
    pub point: FaultPoint,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultSchedule {
    pub activations: Vec<FaultActivation>,
}

impl FaultSchedule {
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = BTreeSet::new();
        let mut previous = None;
        for activation in &self.activations {
            if let Some(previous) = previous {
                if activation.after_event < previous {
                    return Err("fault schedule must be ordered by event".to_string());
                }
            }
            if !seen.insert((activation.after_event, activation.point)) {
                return Err(format!(
                    "duplicate fault {:?} after event {}",
                    activation.point, activation.after_event
                ));
            }
            previous = Some(activation.after_event);
        }
        Ok(())
    }

    pub fn at(&self, event: u64) -> impl Iterator<Item = FaultPoint> + '_ {
        self.activations
            .iter()
            .filter(move |activation| activation.after_event == event)
            .map(|activation| activation.point)
    }
}
