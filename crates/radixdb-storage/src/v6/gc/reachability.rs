use std::collections::HashMap;

use crate::v6::{
    ActiveLeaseSet, ArtifactRef, FormatError, FormatResult, ImmutableMemberLocator,
    ImmutableMemberRef, PhysicalGenerationSnapshot, ReachabilityLimits,
};

const ACCOUNTED_MEMBER_IDENTITY_BYTES: u64 = 128;

/// Complete exact artifact roots used by one cleanup decision.
///
/// The set owns no directory-derived membership. Its inputs are validated
/// CONTROL graphs, retained snapshot members and strong runtime/build leases.
#[derive(Debug, Clone)]
pub struct ArtifactReachability {
    references: HashMap<ImmutableMemberLocator, ImmutableMemberRef>,
    accounted_bytes: u64,
}

impl ArtifactReachability {
    pub(crate) fn build(
        control_roots: &[PhysicalGenerationSnapshot],
        retained_snapshot_artifacts: &[ArtifactRef],
        leases: &ActiveLeaseSet,
        limits: ReachabilityLimits,
    ) -> FormatResult<Self> {
        let mut reachable = Self {
            references: HashMap::new(),
            accounted_bytes: 0,
        };
        for root in control_roots {
            reachable.extend(root.immutable_member_references()?, limits)?;
        }
        reachable.extend(
            retained_snapshot_artifacts
                .iter()
                .copied()
                .map(ImmutableMemberRef::Artifact),
            limits,
        )?;
        for lease in leases.generations() {
            reachable.extend(lease.snapshot().immutable_member_references()?, limits)?;
        }
        Ok(reachable)
    }

    #[cfg(test)]
    pub(crate) fn from_artifacts(
        artifacts: impl IntoIterator<Item = ArtifactRef>,
        limits: ReachabilityLimits,
    ) -> FormatResult<Self> {
        let mut reachable = Self {
            references: HashMap::new(),
            accounted_bytes: 0,
        };
        reachable.extend(
            artifacts.into_iter().map(ImmutableMemberRef::Artifact),
            limits,
        )?;
        Ok(reachable)
    }

    #[cfg(test)]
    pub(crate) fn from_members(
        members: impl IntoIterator<Item = ImmutableMemberRef>,
        limits: ReachabilityLimits,
    ) -> FormatResult<Self> {
        let mut reachable = Self {
            references: HashMap::new(),
            accounted_bytes: 0,
        };
        reachable.extend(members, limits)?;
        Ok(reachable)
    }

    pub(crate) fn contains(&self, reference: ImmutableMemberRef) -> bool {
        self.references.get(&reference.locator()) == Some(&reference)
    }

    #[cfg(test)]
    pub(crate) fn contains_artifact(&self, reference: ArtifactRef) -> bool {
        self.contains(ImmutableMemberRef::Artifact(reference))
    }

    pub(crate) fn conflicting_reference(&self, reference: ImmutableMemberRef) -> bool {
        self.references
            .get(&reference.locator())
            .is_some_and(|reachable| *reachable != reference)
    }

    pub(crate) fn len(&self) -> usize {
        self.references.len()
    }

    fn extend(
        &mut self,
        members: impl IntoIterator<Item = ImmutableMemberRef>,
        limits: ReachabilityLimits,
    ) -> FormatResult<()> {
        for reference in members {
            if let Some(existing) = self.references.get(&reference.locator()) {
                if *existing != reference {
                    return Err(FormatError::InvalidCleanup {
                        detail: "one immutable locator resolves to different reachable identities",
                    });
                }
                continue;
            }
            let next_identities = self.references.len() as u64 + 1;
            if next_identities > limits.max_identities() {
                return Err(FormatError::CleanupLimitExceeded {
                    field: "reachable artifact identities",
                    actual: next_identities,
                    limit: limits.max_identities(),
                });
            }
            let next_bytes = self
                .accounted_bytes
                .checked_add(ACCOUNTED_MEMBER_IDENTITY_BYTES)
                .ok_or(FormatError::InvalidCleanup {
                    detail: "reachable artifact accounting overflow",
                })?;
            if next_bytes > limits.max_accounted_bytes() {
                return Err(FormatError::CleanupLimitExceeded {
                    field: "reachable artifact bytes",
                    actual: next_bytes,
                    limit: limits.max_accounted_bytes(),
                });
            }
            self.references.insert(reference.locator(), reference);
            self.accounted_bytes = next_bytes;
        }
        Ok(())
    }
}
