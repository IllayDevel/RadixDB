use crate::{
    CatalogEdge, CatalogError, CatalogGeneration, CatalogGraph, CatalogName, CatalogObject,
    CatalogResult, ObjectId, ObjectKind, PreparedCatalogMutation,
};

pub const MAX_CATALOG_MUTATIONS_PER_SET: usize = 262_144;
pub const MAX_CATALOG_EDGE_DELTAS_PER_SET: usize = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPrecondition {
    object_id: ObjectId,
    expected_kind: ObjectKind,
    expected_definition_revision: u64,
}

impl ObjectPrecondition {
    pub fn new(
        object_id: ObjectId,
        expected_kind: ObjectKind,
        expected_definition_revision: u64,
    ) -> CatalogResult<Self> {
        if expected_definition_revision == 0 {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "expected object definition revision is zero",
            });
        }
        Ok(Self {
            object_id,
            expected_kind,
            expected_definition_revision,
        })
    }

    pub const fn object_id(self) -> ObjectId {
        self.object_id
    }

    pub const fn expected_kind(self) -> ObjectKind {
        self.expected_kind
    }

    pub const fn expected_definition_revision(self) -> u64 {
        self.expected_definition_revision
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogMutation {
    Create {
        object: CatalogObject,
    },
    Alter {
        expected: ObjectPrecondition,
        replacement: CatalogObject,
    },
    Drop {
        expected: ObjectPrecondition,
    },
    Rename {
        expected: ObjectPrecondition,
        new_name: CatalogName,
    },
}

impl CatalogMutation {
    pub fn create(object: CatalogObject) -> Self {
        Self::Create { object }
    }

    pub fn alter(expected: ObjectPrecondition, replacement: CatalogObject) -> Self {
        Self::Alter {
            expected,
            replacement,
        }
    }

    pub const fn drop(expected: ObjectPrecondition) -> Self {
        Self::Drop { expected }
    }

    pub fn rename(expected: ObjectPrecondition, new_name: CatalogName) -> Self {
        Self::Rename { expected, new_name }
    }

    pub const fn target_id(&self) -> ObjectId {
        match self {
            Self::Create { object } => object.id(),
            Self::Alter { expected, .. }
            | Self::Drop { expected }
            | Self::Rename { expected, .. } => expected.object_id(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMutationSet {
    format_minor: u16,
    expected_database_id: [u8; 16],
    expected_catalog_id: [u8; 16],
    expected_catalog_generation: u64,
    mutations: Vec<CatalogMutation>,
    edge_removals: Vec<CatalogEdge>,
    edge_additions: Vec<CatalogEdge>,
}

impl CatalogMutationSet {
    /// Build a mutation against an already opened generation while preserving
    /// its exact catalog minor unless the mutation requires a newer admitted
    /// minor. The first mutation from 6.0 that requires the procedural
    /// registry also performs bootstrap-owner promotion atomically.
    pub fn for_generation(
        current: &CatalogGeneration,
        mutations: Vec<CatalogMutation>,
        edge_removals: Vec<CatalogEdge>,
        edge_additions: Vec<CatalogEdge>,
    ) -> CatalogResult<Self> {
        let required_minor = required_format_minor(&mutations, &edge_removals, &edge_additions);
        if current.format_minor() == crate::BASELINE_CATALOG_MINOR
            && required_minor >= crate::PROCEDURAL_CATALOG_MINOR
        {
            return Self::upgrade_from_baseline(
                current,
                required_minor,
                mutations,
                edge_removals,
                edge_additions,
            );
        }
        let meta = current.meta();
        Self::new_for_minor(
            current.format_minor().max(required_minor),
            meta.database_id(),
            meta.catalog_id(),
            meta.catalog_generation(),
            mutations,
            edge_removals,
            edge_additions,
        )
    }

    pub fn new(
        expected_database_id: [u8; 16],
        expected_catalog_id: [u8; 16],
        expected_catalog_generation: u64,
        mutations: Vec<CatalogMutation>,
        edge_removals: Vec<CatalogEdge>,
        edge_additions: Vec<CatalogEdge>,
    ) -> CatalogResult<Self> {
        let format_minor = required_format_minor(&mutations, &edge_removals, &edge_additions);
        Self::new_for_minor(
            format_minor,
            expected_database_id,
            expected_catalog_id,
            expected_catalog_generation,
            mutations,
            edge_removals,
            edge_additions,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_for_minor(
        format_minor: u16,
        expected_database_id: [u8; 16],
        expected_catalog_id: [u8; 16],
        expected_catalog_generation: u64,
        mut mutations: Vec<CatalogMutation>,
        mut edge_removals: Vec<CatalogEdge>,
        mut edge_additions: Vec<CatalogEdge>,
    ) -> CatalogResult<Self> {
        if format_minor > crate::LATEST_CATALOG_MINOR {
            return Err(CatalogError::UnsupportedCatalogMinor {
                major: 6,
                minor: format_minor,
            });
        }
        let required_minor = required_format_minor(&mutations, &edge_removals, &edge_additions);
        if required_minor > format_minor {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "mutation payload requires a newer catalog minor",
            });
        }
        if expected_database_id == [0; 16] || expected_catalog_id == [0; 16] {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "expected database/catalog identity is zero",
            });
        }
        if expected_catalog_generation == 0 {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "expected catalog generation is zero",
            });
        }
        if mutations.is_empty() && edge_removals.is_empty() && edge_additions.is_empty() {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "mutation set is empty",
            });
        }
        enforce_count(
            "object mutation count",
            mutations.len(),
            MAX_CATALOG_MUTATIONS_PER_SET,
        )?;
        enforce_count(
            "edge removal count",
            edge_removals.len(),
            MAX_CATALOG_EDGE_DELTAS_PER_SET,
        )?;
        enforce_count(
            "edge addition count",
            edge_additions.len(),
            MAX_CATALOG_EDGE_DELTAS_PER_SET,
        )?;
        let edge_delta_count = edge_removals
            .len()
            .checked_add(edge_additions.len())
            .ok_or(CatalogError::CatalogMutationLimitExceeded {
                field: "total edge delta count",
                actual: usize::MAX,
                limit: MAX_CATALOG_EDGE_DELTAS_PER_SET,
            })?;
        enforce_count(
            "total edge delta count",
            edge_delta_count,
            MAX_CATALOG_EDGE_DELTAS_PER_SET,
        )?;

        mutations.sort_unstable_by_key(CatalogMutation::target_id);
        if mutations
            .windows(2)
            .any(|pair| pair[0].target_id() == pair[1].target_id())
        {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "multiple object mutations target the same object ID",
            });
        }
        edge_removals.sort_unstable();
        edge_additions.sort_unstable();
        if has_duplicate(&edge_removals) || has_duplicate(&edge_additions) {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "edge delta contains a duplicate",
            });
        }
        if edge_removals
            .iter()
            .any(|edge| edge_additions.binary_search(edge).is_ok())
        {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "the same edge is both removed and added",
            });
        }

        Ok(Self {
            format_minor,
            expected_database_id,
            expected_catalog_id,
            expected_catalog_generation,
            mutations,
            edge_removals,
            edge_additions,
        })
    }

    pub const fn format_minor(&self) -> u16 {
        self.format_minor
    }

    /// Build the first procedural mutation over an exact 6.0 generation.
    ///
    /// Bootstrap-owner promotion and all missing ownership edges are part of
    /// the same mutation set as the caller's requested DDL. Validation and
    /// publication therefore cannot expose a half-promoted graph.
    pub fn procedural_upgrade(
        current: &CatalogGeneration,
        requested_mutations: Vec<CatalogMutation>,
        edge_removals: Vec<CatalogEdge>,
        edge_additions: Vec<CatalogEdge>,
    ) -> CatalogResult<Self> {
        Self::upgrade_from_baseline(
            current,
            crate::PROCEDURAL_CATALOG_MINOR,
            requested_mutations,
            edge_removals,
            edge_additions,
        )
    }

    fn upgrade_from_baseline(
        current: &CatalogGeneration,
        target_minor: u16,
        mut requested_mutations: Vec<CatalogMutation>,
        edge_removals: Vec<CatalogEdge>,
        mut edge_additions: Vec<CatalogEdge>,
    ) -> CatalogResult<Self> {
        if current.format_minor() != crate::BASELINE_CATALOG_MINOR {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "procedural upgrade requires an exact 6.0 source generation",
            });
        }
        if requested_mutations
            .iter()
            .any(|mutation| mutation.target_id() == ObjectId::BOOTSTRAP_OWNER)
        {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "requested mutation collides with bootstrap principal promotion",
            });
        }
        let bootstrap = CatalogObject::new(
            ObjectId::BOOTSTRAP_OWNER,
            None,
            None,
            ObjectId::BOOTSTRAP_OWNER,
            CatalogName::new("radix_system")?,
            1,
            crate::CatalogPayload::Principal(crate::PrincipalPayload::new(false, true)),
        )?;
        requested_mutations.push(CatalogMutation::create(bootstrap));

        let mut owners = current
            .graph()
            .objects()
            .map(|object| (object.id(), object.owner_principal_id()))
            .collect::<std::collections::BTreeMap<_, _>>();
        for mutation in &requested_mutations {
            match mutation {
                CatalogMutation::Create { object }
                | CatalogMutation::Alter {
                    replacement: object,
                    ..
                } => {
                    owners.insert(object.id(), object.owner_principal_id());
                }
                CatalogMutation::Drop { expected } => {
                    owners.remove(&expected.object_id());
                }
                CatalogMutation::Rename { .. } => {}
            }
        }
        for (id, owner) in owners {
            if id == ObjectId::BOOTSTRAP_OWNER {
                continue;
            }
            let edge = CatalogEdge::new(id, owner, crate::EdgeKind::OwnedBy, 0);
            if !edge_additions.contains(&edge) {
                edge_additions.push(edge);
            }
        }
        let meta = current.meta();
        Self::new_for_minor(
            target_minor,
            meta.database_id(),
            meta.catalog_id(),
            meta.catalog_generation(),
            requested_mutations,
            edge_removals,
            edge_additions,
        )
    }

    pub const fn expected_database_id(&self) -> &[u8; 16] {
        &self.expected_database_id
    }

    pub const fn expected_catalog_id(&self) -> &[u8; 16] {
        &self.expected_catalog_id
    }

    pub const fn expected_catalog_generation(&self) -> u64 {
        self.expected_catalog_generation
    }

    pub fn mutations(&self) -> &[CatalogMutation] {
        &self.mutations
    }

    pub fn edge_removals(&self) -> &[CatalogEdge] {
        &self.edge_removals
    }

    pub fn edge_additions(&self) -> &[CatalogEdge] {
        &self.edge_additions
    }

    pub fn apply(&self, current: &CatalogGeneration) -> CatalogResult<CatalogGraph> {
        super::apply::apply_mutation_set(self, current)
    }

    pub fn prepare(
        &self,
        current: &CatalogGeneration,
        next_meta: crate::CatalogPackMeta,
    ) -> CatalogResult<PreparedCatalogMutation> {
        let graph = self.apply(current)?;
        PreparedCatalogMutation::new(self, current, next_meta, graph)
    }
}

fn required_format_minor(
    mutations: &[CatalogMutation],
    edge_removals: &[CatalogEdge],
    edge_additions: &[CatalogEdge],
) -> u16 {
    mutations
        .iter()
        .map(|mutation| match mutation {
            CatalogMutation::Create { object }
            | CatalogMutation::Alter {
                replacement: object,
                ..
            } => object.kind().minimum_catalog_minor(),
            CatalogMutation::Drop { expected } | CatalogMutation::Rename { expected, .. } => {
                expected.expected_kind().minimum_catalog_minor()
            }
        })
        .chain(
            edge_removals
                .iter()
                .chain(edge_additions)
                .map(|edge| edge.kind().minimum_catalog_minor()),
        )
        .max()
        .unwrap_or(crate::BASELINE_CATALOG_MINOR)
}

fn has_duplicate<T: PartialEq>(items: &[T]) -> bool {
    items.windows(2).any(|pair| pair[0] == pair[1])
}

fn enforce_count(field: &'static str, actual: usize, limit: usize) -> CatalogResult<()> {
    if actual > limit {
        return Err(CatalogError::CatalogMutationLimitExceeded {
            field,
            actual,
            limit,
        });
    }
    Ok(())
}
