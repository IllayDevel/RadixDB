//! Storage-local mutation controls used by prerelease proof tests.

use std::sync::atomic::{AtomicBool, Ordering};

const MUTATION_ENV: &str = "RADIXDB_TEST_MUTATION";

/// Deliberately bypass index publication for the bounded mutation-proof run.
pub fn index_publication_disabled() -> bool {
    std::env::var(MUTATION_ENV).is_ok_and(|name| name == "index_publication")
}

/// Deliberately bypass WAL checksum verification for the bounded mutation-proof run.
pub fn checksum_verification_disabled() -> bool {
    std::env::var(MUTATION_ENV).is_ok_and(|name| name == "checksum_verification")
}

/// Deliberately retain a dropped view dependency for the mutation-proof run.
pub fn view_invalidation_disabled() -> bool {
    std::env::var(MUTATION_ENV).is_ok_and(|name| name == "view_invalidation")
}

static STATEMENT_VISIBILITY_FENCE_DISABLED: AtomicBool = AtomicBool::new(false);

/// Scoped control used to prove the statement-visibility fence contract.
pub struct StatementVisibilityFenceMutation {
    armed: bool,
}

impl StatementVisibilityFenceMutation {
    pub fn disable() -> Result<Self, String> {
        STATEMENT_VISIBILITY_FENCE_DISABLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| "statement visibility fence mutation is already armed".to_string())?;
        Ok(Self { armed: true })
    }
}

impl Drop for StatementVisibilityFenceMutation {
    fn drop(&mut self) {
        if self.armed {
            STATEMENT_VISIBILITY_FENCE_DISABLED.store(false, Ordering::Release);
        }
    }
}

pub fn statement_visibility_fence_disabled() -> bool {
    STATEMENT_VISIBILITY_FENCE_DISABLED.load(Ordering::Acquire)
        || std::env::var(MUTATION_ENV).is_ok_and(|name| name == "visibility_fence")
}
