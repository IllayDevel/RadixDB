mod collector;
mod discovery;
mod filesystem;
mod member;
mod model;
mod reachability;

#[cfg(test)]
mod tests;

pub use collector::ArtifactGarbageCollector;
pub(crate) use member::{ImmutableMemberLocator, ImmutableMemberRef};
pub use model::{
    ArtifactCleanupLimits, ArtifactCleanupReport, CleanupGeneration, MAX_CLEANUP_ACCOUNTED_BYTES,
    MAX_CLEANUP_FILES, MAX_CLEANUP_MUTATIONS, MAX_CLEANUP_WALL_TIME,
};
pub(crate) use reachability::ArtifactReachability;
