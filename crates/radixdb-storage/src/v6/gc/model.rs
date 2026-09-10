use std::time::Duration;

use crate::v6::{FormatError, FormatResult};

pub const MAX_CLEANUP_FILES: u64 = 1_000_000;
pub const MAX_CLEANUP_MUTATIONS: u64 = 1_000_000;
pub const MAX_CLEANUP_ACCOUNTED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
pub const MAX_CLEANUP_WALL_TIME: Duration = Duration::from_secs(60 * 60);

const DEFAULT_CLEANUP_MUTATIONS: u64 = 100_000;
const DEFAULT_ORPHAN_MIN_AGE: Duration = Duration::from_secs(60 * 60);
const DEFAULT_CLEANUP_WALL_TIME: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CleanupGeneration(u64);

impl CleanupGeneration {
    pub fn new(value: u64) -> FormatResult<Self> {
        if value == 0 {
            return Err(FormatError::InvalidCleanup {
                detail: "cleanup generation cannot be zero",
            });
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> FormatResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(FormatError::InvalidCleanup {
                detail: "cleanup generation overflow",
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactCleanupLimits {
    max_files: u64,
    max_accounted_bytes: u64,
    max_renames: u64,
    max_unlinks: u64,
    max_mutation_bytes: u64,
    orphan_min_age: Duration,
    max_wall_time: Duration,
}

impl ArtifactCleanupLimits {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_files: u64,
        max_accounted_bytes: u64,
        max_renames: u64,
        max_unlinks: u64,
        max_mutation_bytes: u64,
        orphan_min_age: Duration,
        max_wall_time: Duration,
    ) -> FormatResult<Self> {
        validate_limit("file count", max_files, MAX_CLEANUP_FILES)?;
        validate_limit(
            "accounted bytes",
            max_accounted_bytes,
            MAX_CLEANUP_ACCOUNTED_BYTES,
        )?;
        validate_limit("renames", max_renames, MAX_CLEANUP_MUTATIONS)?;
        validate_limit("unlinks", max_unlinks, MAX_CLEANUP_MUTATIONS)?;
        validate_limit(
            "mutation bytes",
            max_mutation_bytes,
            MAX_CLEANUP_ACCOUNTED_BYTES,
        )?;
        if max_wall_time.is_zero() || max_wall_time > MAX_CLEANUP_WALL_TIME {
            return Err(FormatError::CleanupLimitExceeded {
                field: "wall-time nanoseconds",
                actual: duration_ns(max_wall_time),
                limit: duration_ns(MAX_CLEANUP_WALL_TIME),
            });
        }
        Ok(Self {
            max_files,
            max_accounted_bytes,
            max_renames,
            max_unlinks,
            max_mutation_bytes,
            orphan_min_age,
            max_wall_time,
        })
    }

    pub const fn max_files(self) -> u64 {
        self.max_files
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }

    pub const fn max_renames(self) -> u64 {
        self.max_renames
    }

    pub const fn max_unlinks(self) -> u64 {
        self.max_unlinks
    }

    pub const fn max_mutation_bytes(self) -> u64 {
        self.max_mutation_bytes
    }

    pub const fn orphan_min_age(self) -> Duration {
        self.orphan_min_age
    }

    pub const fn max_wall_time(self) -> Duration {
        self.max_wall_time
    }

    pub(crate) fn publication_retirement() -> Self {
        Self {
            orphan_min_age: Duration::ZERO,
            max_wall_time: Duration::from_secs(2),
            ..Self::default()
        }
    }
}

impl Default for ArtifactCleanupLimits {
    fn default() -> Self {
        Self {
            max_files: MAX_CLEANUP_FILES,
            max_accounted_bytes: MAX_CLEANUP_ACCOUNTED_BYTES,
            max_renames: DEFAULT_CLEANUP_MUTATIONS,
            max_unlinks: DEFAULT_CLEANUP_MUTATIONS,
            max_mutation_bytes: MAX_CLEANUP_ACCOUNTED_BYTES,
            orphan_min_age: DEFAULT_ORPHAN_MIN_AGE,
            max_wall_time: DEFAULT_CLEANUP_WALL_TIME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCleanupReport {
    generation: CleanupGeneration,
    reachable: u64,
    inspected_final: u64,
    inspected_quarantine: u64,
    quarantined: u64,
    restored: u64,
    deleted: u64,
    deferred: u64,
    mutation_bytes: u64,
}

impl ArtifactCleanupReport {
    pub(crate) const fn new(generation: CleanupGeneration, reachable: u64) -> Self {
        Self {
            generation,
            reachable,
            inspected_final: 0,
            inspected_quarantine: 0,
            quarantined: 0,
            restored: 0,
            deleted: 0,
            deferred: 0,
            mutation_bytes: 0,
        }
    }

    pub const fn generation(&self) -> CleanupGeneration {
        self.generation
    }
    pub const fn reachable(&self) -> u64 {
        self.reachable
    }
    pub const fn inspected_final(&self) -> u64 {
        self.inspected_final
    }
    pub const fn inspected_quarantine(&self) -> u64 {
        self.inspected_quarantine
    }
    pub const fn quarantined(&self) -> u64 {
        self.quarantined
    }
    pub const fn restored(&self) -> u64 {
        self.restored
    }
    pub const fn deleted(&self) -> u64 {
        self.deleted
    }
    pub const fn deferred(&self) -> u64 {
        self.deferred
    }
    pub const fn mutation_bytes(&self) -> u64 {
        self.mutation_bytes
    }

    pub(crate) fn inspect_final(&mut self) {
        self.inspected_final += 1;
    }
    pub(crate) fn inspect_quarantine(&mut self) {
        self.inspected_quarantine += 1;
    }
    pub(crate) fn quarantine(&mut self, bytes: u64) {
        self.quarantined += 1;
        self.mutation_bytes += bytes;
    }
    pub(crate) fn restore(&mut self, bytes: u64) {
        self.restored += 1;
        self.mutation_bytes += bytes;
    }
    pub(crate) fn delete(&mut self, bytes: u64) {
        self.deleted += 1;
        self.mutation_bytes += bytes;
    }
    pub(crate) fn defer(&mut self) {
        self.deferred += 1;
    }
}

fn validate_limit(field: &'static str, value: u64, hard_limit: u64) -> FormatResult<()> {
    if value == 0 || value > hard_limit {
        return Err(FormatError::CleanupLimitExceeded {
            field,
            actual: value,
            limit: hard_limit,
        });
    }
    Ok(())
}

const fn duration_ns(duration: Duration) -> u64 {
    let nanos = duration.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}
