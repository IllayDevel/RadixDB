use super::super::{FormatError, FormatResult};

pub const MAX_STAGING_PUBLICATIONS: u64 = 65_536;
pub const MAX_STAGED_FILES_PER_PUBLICATION: u64 = 8_388_608;
pub const MAX_STAGED_PATH_BYTES: u64 = 4_096;
pub const MAX_STAGED_PATH_COMPONENT_BYTES: usize = 255;
pub const MAX_STAGING_RECURSION_DEPTH: u64 = 256;
pub const MAX_STAGING_DISCOVERY_BYTES: u64 = 1 << 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingDiscoveryLimits {
    max_publications: u64,
    max_files_per_publication: u64,
    max_path_bytes: u64,
    max_recursion_depth: u64,
    max_accounted_bytes: u64,
}

impl StagingDiscoveryLimits {
    pub fn new(
        max_publications: u64,
        max_files_per_publication: u64,
        max_path_bytes: u64,
        max_recursion_depth: u64,
        max_accounted_bytes: u64,
    ) -> FormatResult<Self> {
        validate(
            "publication count",
            max_publications,
            MAX_STAGING_PUBLICATIONS,
        )?;
        validate(
            "files per publication",
            max_files_per_publication,
            MAX_STAGED_FILES_PER_PUBLICATION,
        )?;
        validate("relative path bytes", max_path_bytes, MAX_STAGED_PATH_BYTES)?;
        validate(
            "directory recursion depth",
            max_recursion_depth,
            MAX_STAGING_RECURSION_DEPTH,
        )?;
        validate(
            "discovery accounted bytes",
            max_accounted_bytes,
            MAX_STAGING_DISCOVERY_BYTES,
        )?;
        Ok(Self {
            max_publications,
            max_files_per_publication,
            max_path_bytes,
            max_recursion_depth,
            max_accounted_bytes,
        })
    }

    pub const fn max_publications(self) -> u64 {
        self.max_publications
    }

    pub const fn max_files_per_publication(self) -> u64 {
        self.max_files_per_publication
    }

    pub const fn max_path_bytes(self) -> u64 {
        self.max_path_bytes
    }

    pub const fn max_recursion_depth(self) -> u64 {
        self.max_recursion_depth
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }
}

impl Default for StagingDiscoveryLimits {
    fn default() -> Self {
        Self {
            max_publications: MAX_STAGING_PUBLICATIONS,
            max_files_per_publication: MAX_STAGED_FILES_PER_PUBLICATION,
            max_path_bytes: MAX_STAGED_PATH_BYTES,
            max_recursion_depth: MAX_STAGING_RECURSION_DEPTH,
            max_accounted_bytes: MAX_STAGING_DISCOVERY_BYTES,
        }
    }
}

fn validate(field: &'static str, value: u64, hard_limit: u64) -> FormatResult<()> {
    if value == 0 || value > hard_limit {
        return Err(FormatError::StagingLimitExceeded {
            field,
            actual: value,
            limit: hard_limit,
        });
    }
    Ok(())
}
