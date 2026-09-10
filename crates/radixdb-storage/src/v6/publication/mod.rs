//! Single-source artifact construction.
//!
//! One authoritative source-row iterator feeds bounded data row groups and
//! spillable accelerator sort runs. Final `.idx` pages are produced by a
//! bounded merge and never by reopening the newly built `.data` artifact.

mod checkpoint;
pub(crate) mod diagnostics;
mod fanout;
pub(crate) mod filesystem;
mod generation;
mod lease;
mod maintenance;
mod model;
mod prepare;
mod rebuild;
mod resources;
mod runs;

pub use checkpoint::{
    CheckpointOutcome, FrozenCheckpoint, WalRetirementReport, WalRetirementStatus,
};
#[cfg(feature = "test-hooks")]
pub use diagnostics::{
    publication_diagnostics, reset_publication_diagnostics, PublicationDiagnostics,
};
pub use fanout::{
    build_artifact_pair, build_data_artifact, write_artifact_pair, write_data_artifact,
};
pub use generation::{
    IndexReplacementPublicationSink, PhysicalGenerationPublisher, PhysicalGenerationSnapshot,
    PreparedIndexReplacement,
};
pub(crate) use lease::LeaseRegistry;
pub use lease::{
    ActiveLeaseSet, ArtifactBuildKind, ArtifactBuildLease, LeaseLimits, PhysicalGenerationLease,
    MAX_ACTIVE_ARTIFACT_BUILDS, MAX_ACTIVE_GENERATION_LEASES,
};
pub use maintenance::{FrozenMaintenance, MaintenanceKind, MaintenanceOutcome};
pub use model::{
    AcceleratorBuildSpec, ArtifactPairBuildRequest, BuiltArtifactPair, BuiltDataArtifact,
    ColumnBuildPolicy, DataArtifactBuildRequest, FanoutBuildLimits, SourceRow, WrittenArtifactPair,
    WrittenDataArtifact, DEFAULT_FANOUT_RESIDENT_BYTES, DEFAULT_FANOUT_SPILL_BYTES,
    DEFAULT_MERGE_FAN_IN, DEFAULT_SORT_RUN_BYTES, DEFAULT_SORT_RUN_RECORDS,
    MAX_ACCELERATOR_PREPARATION_WORKERS, MAX_FANOUT_RESIDENT_BYTES, MAX_FANOUT_SPILL_BYTES,
    MAX_MERGE_FAN_IN, MAX_SORT_RUN_BYTES, MAX_SORT_RUN_DESCRIPTOR_SLOTS, MAX_SORT_RUN_FILES,
    MAX_SORT_RUN_RECORDS,
};
pub(crate) use rebuild::write_index_replacement_reusing;
pub use rebuild::{
    stage_index_replacement, write_index_replacement, IndexReplacementBuildRequest,
    StagedIndexReplacement, WrittenIndexReplacement,
};
