//! Durable private publication sets and read-only recovery classification.

mod discovery;
mod limits;
mod members;
mod record;
mod set;

pub use discovery::{
    discover_staging_publications, StagingCompletion, StagingDiscovery, StagingDisposition,
    StagingPublication,
};
pub use limits::{
    StagingDiscoveryLimits, MAX_STAGED_FILES_PER_PUBLICATION, MAX_STAGED_PATH_BYTES,
    MAX_STAGED_PATH_COMPONENT_BYTES, MAX_STAGING_DISCOVERY_BYTES, MAX_STAGING_PUBLICATIONS,
    MAX_STAGING_RECURSION_DEPTH,
};
pub use record::{
    decode_staging_complete, decode_staging_owner, encode_staging_complete, encode_staging_owner,
    StagingComplete, StagingOwner, STAGING_COMPLETE_BYTES, STAGING_OWNER_BYTES,
};
pub use set::{CompleteStagingSet, StagedArtifactSet, StagedMemberRole};

pub(crate) const OWNER_FILE: &str = "OWNER";
pub(crate) const COMPLETE_FILE: &str = "COMPLETE";
pub(crate) const COMPLETE_PENDING_FILE: &str = "COMPLETE.pending";
