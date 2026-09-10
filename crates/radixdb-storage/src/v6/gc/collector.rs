use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use crate::v6::{
    fault::reach_generation_boundary, ArtifactCleanupLimits, ArtifactCleanupReport,
    ArtifactReachability, FormatError, FormatResult, GenerationCrashPoint, ImmutableMemberLocator,
};

use super::discovery::{discover_artifacts, DiscoveredLocation, DiscoveredMember};
use super::filesystem::{
    begin_cycle, delete_member, quarantine_member, remove_empty_generation, restore_member,
};

static ACTIVE_ROOTS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct ArtifactGarbageCollector {
    root: PathBuf,
    limits: ArtifactCleanupLimits,
}

impl ArtifactGarbageCollector {
    pub fn new(root: impl Into<PathBuf>, limits: ArtifactCleanupLimits) -> Self {
        Self {
            root: root.into(),
            limits,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn limits(&self) -> ArtifactCleanupLimits {
        self.limits
    }

    pub(crate) fn run_cycle(
        &self,
        reachable: &ArtifactReachability,
        now_unix_ns: u64,
    ) -> FormatResult<ArtifactCleanupReport> {
        let _admission = RootAdmission::acquire(&self.root)?;
        let started = Instant::now();

        // Bounded shell-identity discovery deliberately precedes the first
        // filesystem mutation. Payload integrity belongs to lazy block/page
        // checks and explicit scrub; GC never rereads every reachable body
        // while it owns the publication fence.
        reach_generation_boundary(GenerationCrashPoint::GcEnumerationError).map_err(|error| {
            FormatError::CleanupIo {
                operation: "inject cleanup enumeration failure",
                kind: error.kind(),
            }
        })?;
        let discovery = discover_artifacts(&self.root, self.limits, started)?;
        let final_by_id = validate_unique_final(&discovery.final_artifacts)?;
        validate_unique_quarantine(&discovery.quarantined_artifacts)?;

        let mut restores = Vec::new();
        let mut deletions = Vec::new();
        for artifact in &discovery.quarantined_artifacts {
            match artifact.location {
                DiscoveredLocation::Quarantine(_) => {}
                DiscoveredLocation::Final => {
                    return invalid("final artifact appeared in quarantine discovery")
                }
            }
            if reachable.contains(artifact.reference) {
                let canonical_present = final_by_id
                    .get(&artifact.reference.locator())
                    .is_some_and(|candidate| candidate.reference == artifact.reference);
                restores.push((artifact, canonical_present));
            } else if reachable.conflicting_reference(artifact.reference) {
                return invalid("quarantined artifact conflicts with a reachable identity");
            } else {
                deletions.push(artifact);
            }
        }
        let restore_cost = validate_restore_budget(&restores, self.limits)?;

        if !restores.is_empty() {
            reach_generation_boundary(GenerationCrashPoint::GcLeaseAppeared).map_err(|error| {
                FormatError::CleanupIo {
                    operation: "inject cleanup root reappearance",
                    kind: error.kind(),
                }
            })?;
        }
        if !deletions.is_empty() {
            reach_generation_boundary(GenerationCrashPoint::GcAfterSecondRootProof).map_err(
                |error| FormatError::CleanupIo {
                    operation: "inject after second cleanup proof",
                    kind: error.kind(),
                },
            )?;
        }

        let generation = begin_cycle(&self.root, &discovery.quarantine_generations)?;
        let mut report = ArtifactCleanupReport::new(generation, reachable.len() as u64);
        for _ in &discovery.final_artifacts {
            report.inspect_final();
        }
        for _ in &discovery.quarantined_artifacts {
            report.inspect_quarantine();
        }

        // A root that reappeared after a prior proof wins over every other
        // cleanup action. Restore it before applying soft work budgets.
        for (artifact, canonical_present) in restores {
            restore_member(&self.root, artifact, canonical_present)?;
            report.restore(artifact.reference.byte_length());
        }

        let mut budget = MutationBudget::new(self.limits, started, restore_cost);
        for artifact in deletions {
            let DiscoveredLocation::Quarantine(quarantined_at) = artifact.location else {
                return invalid("delete candidate is not quarantined");
            };
            if quarantined_at >= generation {
                report.defer();
                continue;
            }
            if !budget.admit_unlink(artifact.reference.byte_length()) {
                report.defer();
                continue;
            }
            delete_member(artifact)?;
            report.delete(artifact.reference.byte_length());
        }

        for artifact in &discovery.final_artifacts {
            if reachable.contains(artifact.reference) {
                continue;
            }
            if reachable.conflicting_reference(artifact.reference) {
                return invalid("final artifact conflicts with a reachable identity");
            }
            if !is_old_enough(artifact, now_unix_ns, self.limits) {
                report.defer();
                continue;
            }
            if !budget.admit_rename(artifact.reference.byte_length()) {
                report.defer();
                continue;
            }
            quarantine_member(&self.root, artifact, generation)?;
            report.quarantine(artifact.reference.byte_length());
        }

        for old_generation in discovery.quarantine_generations {
            remove_empty_generation(&self.root, old_generation)?;
        }
        Ok(report)
    }
}

fn validate_unique_final(
    artifacts: &[DiscoveredMember],
) -> FormatResult<HashMap<ImmutableMemberLocator, &DiscoveredMember>> {
    let mut by_id = HashMap::new();
    for artifact in artifacts {
        if by_id
            .insert(artifact.reference.locator(), artifact)
            .is_some()
        {
            return invalid("one final immutable locator has multiple identities");
        }
    }
    Ok(by_id)
}

fn validate_unique_quarantine(artifacts: &[DiscoveredMember]) -> FormatResult<()> {
    let mut identities = HashSet::new();
    for artifact in artifacts {
        if !identities.insert(artifact.reference.locator()) {
            return invalid("one immutable locator appears in multiple quarantine locations");
        }
    }
    Ok(())
}

fn validate_restore_budget(
    restores: &[(&DiscoveredMember, bool)],
    limits: ArtifactCleanupLimits,
) -> FormatResult<RestoreCost> {
    let renames = restores.iter().filter(|(_, present)| !*present).count() as u64;
    let unlinks = restores.iter().filter(|(_, present)| *present).count() as u64;
    let bytes = restores.iter().try_fold(0_u64, |total, (artifact, _)| {
        total
            .checked_add(artifact.reference.byte_length())
            .ok_or(FormatError::InvalidCleanup {
                detail: "restore byte accounting overflow",
            })
    })?;
    for (field, actual, limit) in [
        ("root-restoration renames", renames, limits.max_renames()),
        ("root-restoration unlinks", unlinks, limits.max_unlinks()),
        ("root-restoration bytes", bytes, limits.max_mutation_bytes()),
    ] {
        if actual > limit {
            return Err(FormatError::CleanupLimitExceeded {
                field,
                actual,
                limit,
            });
        }
    }
    Ok(RestoreCost {
        renames,
        unlinks,
        bytes,
    })
}

fn is_old_enough(
    artifact: &DiscoveredMember,
    now_unix_ns: u64,
    limits: ArtifactCleanupLimits,
) -> bool {
    let minimum_age = u64::try_from(limits.orphan_min_age().as_nanos()).unwrap_or(u64::MAX);
    now_unix_ns >= artifact.modified_unix_ns
        && now_unix_ns.saturating_sub(artifact.modified_unix_ns) >= minimum_age
}

struct MutationBudget {
    limits: ArtifactCleanupLimits,
    started: Instant,
    renames: u64,
    unlinks: u64,
    bytes: u64,
}

#[derive(Clone, Copy)]
struct RestoreCost {
    renames: u64,
    unlinks: u64,
    bytes: u64,
}

impl MutationBudget {
    const fn new(
        limits: ArtifactCleanupLimits,
        started: Instant,
        restore_cost: RestoreCost,
    ) -> Self {
        Self {
            limits,
            started,
            renames: restore_cost.renames,
            unlinks: restore_cost.unlinks,
            bytes: restore_cost.bytes,
        }
    }

    fn admit_rename(&mut self, bytes: u64) -> bool {
        if self.renames >= self.limits.max_renames() || !self.admit_bytes(bytes) {
            return false;
        }
        self.renames += 1;
        true
    }

    fn admit_unlink(&mut self, bytes: u64) -> bool {
        if self.unlinks >= self.limits.max_unlinks() || !self.admit_bytes(bytes) {
            return false;
        }
        self.unlinks += 1;
        true
    }

    fn admit_bytes(&mut self, bytes: u64) -> bool {
        if self.started.elapsed() >= self.limits.max_wall_time() {
            return false;
        }
        let Some(next) = self.bytes.checked_add(bytes) else {
            return false;
        };
        if next > self.limits.max_mutation_bytes() {
            return false;
        }
        self.bytes = next;
        true
    }
}

struct RootAdmission {
    root: PathBuf,
}

impl RootAdmission {
    fn acquire(root: &Path) -> FormatResult<Self> {
        let root = std::fs::canonicalize(root).map_err(|error| FormatError::CleanupIo {
            operation: "canonicalize database root",
            kind: error.kind(),
        })?;
        let mut roots = active_roots()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !roots.insert(root.clone()) {
            return Err(FormatError::CleanupBusy);
        }
        Ok(Self { root })
    }
}

impl Drop for RootAdmission {
    fn drop(&mut self) {
        active_roots()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.root);
    }
}

fn active_roots() -> &'static Mutex<HashSet<PathBuf>> {
    ACTIVE_ROOTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidCleanup { detail })
}
