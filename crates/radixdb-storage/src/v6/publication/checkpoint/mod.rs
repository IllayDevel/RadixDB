mod model;
mod retirement;

pub use model::{CheckpointOutcome, FrozenCheckpoint, WalRetirementReport, WalRetirementStatus};
pub(crate) use retirement::retire_obsolete_wal;
