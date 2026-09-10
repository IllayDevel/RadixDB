use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CatalogError, CatalogGeneration, CatalogGraph, CatalogMutation, CatalogMutationSet,
    CatalogObject, CatalogResult, ObjectId, ObjectPrecondition,
};

pub(super) fn apply_mutation_set(
    mutation_set: &CatalogMutationSet,
    current: &CatalogGeneration,
) -> CatalogResult<CatalogGraph> {
    validate_generation_precondition(mutation_set, current)?;
    for mutation in mutation_set.mutations() {
        validate_object_precondition(mutation, current)?;
    }

    let mut objects = current
        .graph()
        .objects()
        .cloned()
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let mut edges = current
        .graph()
        .edges()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut dropped = BTreeSet::new();

    for mutation in mutation_set.mutations() {
        match mutation {
            CatalogMutation::Create { object } => {
                if object.definition_revision() != 1 {
                    return precondition_failure(
                        object.id(),
                        "new object definition revision is not one",
                    );
                }
                if objects.insert(object.id(), object.clone()).is_some() {
                    return precondition_failure(object.id(), "object already exists");
                }
            }
            CatalogMutation::Alter {
                expected,
                replacement,
            } => {
                let current_object = current_object(current, *expected)?;
                validate_alter_replacement(current_object, *expected, replacement)?;
                objects.insert(replacement.id(), replacement.clone());
            }
            CatalogMutation::Drop { expected } => {
                objects.remove(&expected.object_id());
                dropped.insert(expected.object_id());
            }
            CatalogMutation::Rename { expected, new_name } => {
                let current_object = current_object(current, *expected)?;
                if current_object.name() == new_name {
                    return precondition_failure(
                        current_object.id(),
                        "rename does not change the catalog name",
                    );
                }
                let revision = current_object.definition_revision().checked_add(1).ok_or(
                    CatalogError::InvalidCatalogMutation {
                        detail: "object definition revision overflow",
                    },
                )?;
                let renamed = CatalogObject::new(
                    current_object.id(),
                    current_object.namespace_id(),
                    current_object.parent_id(),
                    current_object.owner_principal_id(),
                    new_name.clone(),
                    revision,
                    current_object.payload().clone(),
                )?;
                objects.insert(renamed.id(), renamed);
            }
        }
    }

    for edge in mutation_set.edge_removals() {
        if !edges.remove(edge) {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "edge removal precondition is absent",
            });
        }
    }
    edges.retain(|edge| {
        !dropped.contains(&edge.source_object_id()) && !dropped.contains(&edge.target_object_id())
    });
    for edge in mutation_set.edge_additions() {
        if !edges.insert(*edge) {
            return Err(CatalogError::InvalidCatalogMutation {
                detail: "edge addition already exists",
            });
        }
    }

    let graph = CatalogGraph::build(objects.into_values().collect(), edges.into_iter().collect())?;
    let required_minor = current.format_minor().max(graph.required_format_minor());
    if mutation_set.format_minor() != required_minor {
        return Err(CatalogError::InvalidCatalogMutation {
            detail:
                "mutation-set format minor would upgrade or downgrade outside its declared envelope",
        });
    }
    Ok(graph)
}

fn validate_generation_precondition(
    mutation_set: &CatalogMutationSet,
    current: &CatalogGeneration,
) -> CatalogResult<()> {
    let meta = current.meta();
    require_expected(
        "database identity",
        hex(mutation_set.expected_database_id()),
        hex(&meta.database_id()),
    )?;
    require_expected(
        "catalog identity",
        hex(mutation_set.expected_catalog_id()),
        hex(&meta.catalog_id()),
    )?;
    require_expected(
        "catalog generation",
        mutation_set.expected_catalog_generation().to_string(),
        meta.catalog_generation().to_string(),
    )
}

fn validate_object_precondition(
    mutation: &CatalogMutation,
    current: &CatalogGeneration,
) -> CatalogResult<()> {
    match mutation {
        CatalogMutation::Create { object } => {
            if current.object(object.id()).is_some() {
                return precondition_failure(object.id(), "object already exists");
            }
            Ok(())
        }
        CatalogMutation::Alter { expected, .. }
        | CatalogMutation::Drop { expected }
        | CatalogMutation::Rename { expected, .. } => {
            current_object(current, *expected).map(|_| ())
        }
    }
}

fn current_object(
    current: &CatalogGeneration,
    expected: ObjectPrecondition,
) -> CatalogResult<&CatalogObject> {
    let object = current.object(expected.object_id()).ok_or_else(|| {
        CatalogError::CatalogObjectPreconditionFailed {
            id: expected.object_id().to_string(),
            detail: "object is absent",
        }
    })?;
    if object.kind() != expected.expected_kind() {
        return precondition_failure(object.id(), "object kind differs");
    }
    if object.definition_revision() != expected.expected_definition_revision() {
        return precondition_failure(object.id(), "definition revision differs");
    }
    Ok(object)
}

fn validate_alter_replacement(
    current: &CatalogObject,
    expected: ObjectPrecondition,
    replacement: &CatalogObject,
) -> CatalogResult<()> {
    if replacement.id() != current.id() {
        return precondition_failure(expected.object_id(), "ALTER replacement changes object ID");
    }
    if replacement.kind() != current.kind() {
        return precondition_failure(
            expected.object_id(),
            "ALTER replacement changes object kind",
        );
    }
    if replacement.namespace_id() != current.namespace_id() {
        return precondition_failure(expected.object_id(), "ALTER changes namespace");
    }
    if replacement.parent_id() != current.parent_id() {
        return precondition_failure(expected.object_id(), "ALTER changes containment parent");
    }
    let next_revision = expected
        .expected_definition_revision()
        .checked_add(1)
        .ok_or(CatalogError::InvalidCatalogMutation {
            detail: "object definition revision overflow",
        })?;
    if replacement.definition_revision() != next_revision {
        return precondition_failure(
            expected.object_id(),
            "ALTER replacement revision is not expected revision plus one",
        );
    }
    Ok(())
}

fn require_expected(field: &'static str, expected: String, actual: String) -> CatalogResult<()> {
    if expected != actual {
        return Err(CatalogError::StaleCatalogMutation {
            field,
            expected,
            actual,
        });
    }
    Ok(())
}

fn precondition_failure<T>(id: ObjectId, detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::CatalogObjectPreconditionFailed {
        id: id.to_string(),
        detail,
    })
}

fn hex(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
