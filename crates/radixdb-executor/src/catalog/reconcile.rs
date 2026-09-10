use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    AccessMethod, CatalogDataType, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName,
    CatalogObject, CatalogPayload, ColumnPayload, ConstraintPayload, EdgeKind,
    ForeignKeyAction as CatalogForeignKeyAction, ForeignKeyMatch, IndexPayload, ObjectId,
    ObjectKind, ObjectPrecondition, TablePayload,
};
use radixdb_core::{DataType, Error, ForeignKeyAction, Result, Schema, SchemaConstraintKind};

use super::table::require_table;
use super::transaction::{catalog_argument, DdlDelta, ObjectIdSource};

pub(super) fn reconcile_table_schema(
    schema: &Schema,
    renamed_column: Option<(&str, &str)>,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    let table = require_table(generation, &schema.table_name)?;
    let CatalogPayload::Table(_) = table.payload() else {
        return Err(Error::internal("catalog table has a non-table payload"));
    };
    let current_children = generation
        .graph()
        .children(table.id())
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let current_columns = current_children
        .values()
        .filter(|object| object.kind() == ObjectKind::Column)
        .map(|object| (object.name().normalized().as_str().to_owned(), object.id()))
        .collect::<BTreeMap<_, _>>();

    let mut desired = BTreeMap::new();
    let mut column_ids = Vec::with_capacity(schema.columns.len());
    let mut columns_by_name = BTreeMap::new();
    for (ordinal, column) in schema.columns.iter().enumerate() {
        let normalized = normalize(&column.name)?;
        let renamed_from = renamed_column.and_then(|(old, new)| {
            new.eq_ignore_ascii_case(&column.name)
                .then(|| old.to_ascii_lowercase())
        });
        let id = current_columns
            .get(&normalized)
            .or_else(|| {
                renamed_from
                    .as_ref()
                    .and_then(|old| current_columns.get(old))
            })
            .copied()
            .map(Ok)
            .unwrap_or_else(|| ids.next(generation))?;
        let payload = ColumnPayload::new_with_auto_increment(
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many table columns".to_owned()))?,
            catalog_type(column)?,
            column.nullable,
            column.auto_increment,
            column.default_expr.clone(),
            None,
        )
        .map_err(catalog_argument)?;
        desired.insert(
            id,
            desired_object(
                current_children.get(&id).copied(),
                id,
                table.id(),
                &column.name,
                CatalogPayload::Column(payload),
            )?,
        );
        column_ids.push(id);
        columns_by_name.insert(normalized, id);
    }

    let existing_constraints = current_children
        .values()
        .filter(|object| object.kind() == ObjectKind::Constraint)
        .map(|object| (object.name().normalized().as_str().to_owned(), *object))
        .collect::<BTreeMap<_, _>>();
    let mut constraint_ids = Vec::new();
    let mut primary_key_id = None;
    let mut reference_edges = Vec::new();
    let mut constraint_index_names = BTreeMap::new();

    for constraint in schema.constraints() {
        let normalized = normalize(&constraint.name)?;
        let existing = existing_constraints.get(&normalized).copied();
        let id = existing
            .map(CatalogObject::id)
            .map(Ok)
            .unwrap_or_else(|| ids.next(generation))?;
        let (payload, references, index_name) =
            constraint_payload(&constraint.kind, table.id(), &columns_by_name, generation)?;
        if matches!(payload, ConstraintPayload::PrimaryKey { .. })
            && primary_key_id.replace(id).is_some()
        {
            return Err(Error::InvalidArgument(
                "table has more than one primary key".to_owned(),
            ));
        }
        if let Some(index_name) = index_name {
            constraint_index_names.insert(id, index_name);
        }
        desired.insert(
            id,
            desired_object(
                existing,
                id,
                table.id(),
                &constraint.name,
                CatalogPayload::Constraint(payload),
            )?,
        );
        constraint_ids.push(id);
        for (ordinal, target) in references.into_iter().enumerate() {
            reference_edges.push(CatalogEdge::new(
                id,
                target,
                EdgeKind::References,
                u32::try_from(ordinal)
                    .map_err(|_| Error::InvalidArgument("too many FK targets".to_owned()))?,
            ));
        }
    }

    // Runtime Schema intentionally folds NOT NULL into the column shape. Keep
    // its catalog object stable when present and create one for a newly
    // non-null, non-PK column.
    for column in &schema.columns {
        if column.nullable || column.primary_key {
            continue;
        }
        let column_id = columns_by_name[&normalize(&column.name)?];
        let existing = existing_constraints.values().copied().find(|object| {
            matches!(
                object.payload(),
                CatalogPayload::Constraint(ConstraintPayload::NotNull { local_column_id })
                    if *local_column_id == column_id
            )
        });
        let name = existing
            .map(|object| object.name().display().as_str().to_owned())
            .unwrap_or_else(|| format!("nn_{}_{}", schema.table_name_lower, column.name));
        let id = existing
            .map(CatalogObject::id)
            .map(Ok)
            .unwrap_or_else(|| ids.next(generation))?;
        desired.insert(
            id,
            desired_object(
                existing,
                id,
                table.id(),
                &name,
                CatalogPayload::Constraint(ConstraintPayload::not_null(column_id)),
            )?,
        );
        constraint_ids.push(id);
    }

    let mut index_ids = Vec::new();
    let mut dependency_edges = Vec::new();
    let existing_constraint_by_index = current_children
        .values()
        .filter(|object| object.kind() == ObjectKind::Index)
        .filter_map(|index| {
            generation
                .graph()
                .outgoing_edges(index.id())
                .find(|edge| edge.kind() == EdgeKind::DependsOn)
                .map(|edge| (edge.target_object_id(), *index))
        })
        .collect::<BTreeMap<_, _>>();

    // Explicit indexes are unchanged by schema-only ALTER. A column drop is
    // rejected by storage preflight if such an index still references it.
    for index in current_children
        .values()
        .filter(|object| object.kind() == ObjectKind::Index)
        .filter(|index| {
            generation
                .graph()
                .outgoing_edges(index.id())
                .all(|edge| edge.kind() != EdgeKind::DependsOn)
        })
    {
        desired.insert(index.id(), (*index).clone());
        index_ids.push(index.id());
    }

    for constraint_id in constraint_ids.iter().copied() {
        let Some(constraint) = desired.get(&constraint_id) else {
            continue;
        };
        let key_columns = match constraint.payload() {
            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids })
            | CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => {
                local_column_ids.clone()
            }
            _ => continue,
        };
        let existing = existing_constraint_by_index.get(&constraint_id).copied();
        let name = constraint_index_names
            .get(&constraint_id)
            .cloned()
            .or_else(|| existing.map(|index| index.name().display().as_str().to_owned()))
            .unwrap_or_else(|| format!("{}_idx", constraint.name().normalized().as_str()));
        let id = existing
            .map(CatalogObject::id)
            .map(Ok)
            .unwrap_or_else(|| ids.next(generation))?;
        if existing.is_none()
            && generation
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, &name)
                .map_err(catalog_argument)?
                .is_some()
        {
            return Err(Error::IndexAlreadyExists(name));
        }
        let payload = IndexPayload::new(AccessMethod::Btree, true, key_columns, vec![], None, None)
            .map_err(catalog_argument)?;
        desired.insert(
            id,
            desired_object(
                existing,
                id,
                table.id(),
                &name,
                CatalogPayload::Index(payload),
            )?,
        );
        index_ids.push(id);
        dependency_edges.push(CatalogEdge::new(id, constraint_id, EdgeKind::DependsOn, 0));
    }

    constraint_ids.sort_unstable();
    constraint_ids.dedup();
    index_ids.sort_unstable();
    index_ids.dedup();
    let created_unix_ns = schema_timestamp_nanos(schema.created_at(), "creation")?;
    let updated_unix_ns = schema_timestamp_nanos(schema.updated_at(), "update")?;
    let table_payload = TablePayload::new_with_timestamps(
        column_ids.clone(),
        constraint_ids.clone(),
        index_ids.clone(),
        primary_key_id,
        created_unix_ns,
        updated_unix_ns,
    )
    .map_err(catalog_argument)?;
    let replacement_table = desired_object(
        Some(table),
        table.id(),
        ObjectId::BOOTSTRAP_NAMESPACE,
        table.name().display().as_str(),
        CatalogPayload::Table(table_payload),
    )?;

    let mut desired_edges = Vec::new();
    for (ordinal, id) in column_ids.iter().copied().enumerate() {
        desired_edges.push(CatalogEdge::new(
            table.id(),
            id,
            EdgeKind::Contains,
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many table columns".to_owned()))?,
        ));
    }
    for (ordinal, id) in constraint_ids.iter().copied().enumerate() {
        desired_edges.push(CatalogEdge::new(
            table.id(),
            id,
            EdgeKind::Contains,
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many table constraints".to_owned()))?,
        ));
    }
    for (ordinal, id) in index_ids.iter().copied().enumerate() {
        desired_edges.push(CatalogEdge::new(
            table.id(),
            id,
            EdgeKind::Contains,
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many table indexes".to_owned()))?,
        ));
    }
    desired_edges.extend(reference_edges);
    desired_edges.extend(dependency_edges);

    build_delta(
        generation,
        table,
        replacement_table,
        current_children,
        desired,
        desired_edges,
    )
}

fn schema_timestamp_nanos(
    timestamp: chrono::DateTime<chrono::Utc>,
    role: &'static str,
) -> Result<u64> {
    timestamp
        .timestamp_nanos_opt()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "schema {role} timestamp is outside the catalog domain"
            ))
        })
}

fn build_delta(
    generation: &CatalogGeneration,
    table: &CatalogObject,
    replacement_table: CatalogObject,
    current: BTreeMap<ObjectId, &CatalogObject>,
    desired: BTreeMap<ObjectId, CatalogObject>,
    mut desired_edges: Vec<CatalogEdge>,
) -> Result<DdlDelta> {
    let mut mutations = Vec::new();
    if table.payload() != replacement_table.payload() {
        mutations.push(CatalogMutation::alter(
            precondition(table)?,
            with_revision(&replacement_table, table.definition_revision() + 1)?,
        ));
    }
    let dropped = current
        .keys()
        .filter(|id| !desired.contains_key(id))
        .copied()
        .collect::<BTreeSet<_>>();
    for id in &dropped {
        mutations.push(CatalogMutation::drop(precondition(current[id])?));
    }
    for (id, object) in &desired {
        match current.get(id) {
            None => mutations.push(CatalogMutation::create(with_revision(object, 1)?)),
            Some(before) if **before != *object => mutations.push(CatalogMutation::alter(
                precondition(before)?,
                with_revision(object, before.definition_revision() + 1)?,
            )),
            Some(_) => {}
        }
    }

    let current_edges = generation
        .graph()
        .edges()
        .iter()
        .copied()
        .filter(|edge| {
            edge.source_object_id() == table.id() || current.contains_key(&edge.source_object_id())
        })
        .collect::<BTreeSet<_>>();
    // Schema reconciliation owns containment, reference and dependency
    // topology. ACL-era ownership is orthogonal metadata and must survive a
    // column/constraint rewrite unchanged.
    desired_edges.extend(
        current_edges
            .iter()
            .filter(|edge| edge.kind() == EdgeKind::OwnedBy)
            .copied(),
    );
    let desired_edges = desired_edges.into_iter().collect::<BTreeSet<_>>();
    let edge_removals = current_edges
        .difference(&desired_edges)
        .filter(|edge| {
            !dropped.contains(&edge.source_object_id())
                && !dropped.contains(&edge.target_object_id())
        })
        .copied()
        .collect();
    let edge_additions = desired_edges.difference(&current_edges).copied().collect();
    Ok(DdlDelta {
        mutations,
        edge_removals,
        edge_additions,
    })
}

fn desired_object(
    existing: Option<&CatalogObject>,
    id: ObjectId,
    parent_id: ObjectId,
    name: &str,
    payload: CatalogPayload,
) -> Result<CatalogObject> {
    CatalogObject::new(
        id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(parent_id),
        existing
            .map(CatalogObject::owner_principal_id)
            .unwrap_or(ObjectId::BOOTSTRAP_OWNER),
        CatalogName::new(name).map_err(catalog_argument)?,
        existing
            .map(CatalogObject::definition_revision)
            .unwrap_or(1),
        payload,
    )
    .map_err(catalog_argument)
}

fn constraint_payload(
    kind: &SchemaConstraintKind,
    table_id: ObjectId,
    columns: &BTreeMap<String, ObjectId>,
    generation: &CatalogGeneration,
) -> Result<(ConstraintPayload, Vec<ObjectId>, Option<String>)> {
    let local = |names: &[String]| {
        names
            .iter()
            .map(|name| {
                columns
                    .get(&normalize(name)?)
                    .copied()
                    .ok_or_else(|| Error::ColumnNotFound(name.clone()))
            })
            .collect::<Result<Vec<_>>>()
    };
    match kind {
        SchemaConstraintKind::PrimaryKey { columns: names } => Ok((
            ConstraintPayload::primary_key(local(names)?).map_err(catalog_argument)?,
            vec![],
            None,
        )),
        SchemaConstraintKind::Unique {
            columns: names,
            index_name,
        } => Ok((
            ConstraintPayload::unique(local(names)?).map_err(catalog_argument)?,
            vec![],
            Some(index_name.clone()),
        )),
        SchemaConstraintKind::Check {
            column_name,
            expression,
            ..
        } => {
            let payload = match column_name {
                Some(column_name) => ConstraintPayload::column_check(
                    columns
                        .get(&normalize(column_name)?)
                        .copied()
                        .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?,
                    expression.clone(),
                ),
                None => ConstraintPayload::check(expression.clone()),
            }
            .map_err(catalog_argument)?;
            Ok((payload, vec![], None))
        }
        SchemaConstraintKind::ForeignKey {
            columns: names,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            let local_ids = local(names)?;
            let target = if referenced_table.eq_ignore_ascii_case(
                generation
                    .object(table_id)
                    .expect("owning table exists")
                    .name()
                    .display()
                    .as_str(),
            ) {
                generation.object(table_id).expect("owning table exists")
            } else {
                require_table(generation, referenced_table)?
            };
            let referenced_ids = referenced_columns
                .iter()
                .map(|name| {
                    if target.id() == table_id {
                        columns
                            .get(&normalize(name)?)
                            .copied()
                            .ok_or_else(|| Error::ColumnNotFound(name.clone()))
                    } else {
                        generation
                            .find_column(target.id(), name)
                            .map_err(catalog_argument)?
                            .map(CatalogObject::id)
                            .ok_or_else(|| Error::ColumnNotFound(name.clone()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let payload = ConstraintPayload::foreign_key(
                local_ids,
                target.id(),
                referenced_ids.clone(),
                ForeignKeyMatch::Simple,
                foreign_key_action(*on_update),
                foreign_key_action(*on_delete),
            )
            .map_err(catalog_argument)?;
            let mut references = vec![target.id()];
            references.extend(referenced_ids);
            Ok((payload, references, None))
        }
    }
}

fn foreign_key_action(action: ForeignKeyAction) -> CatalogForeignKeyAction {
    match action {
        ForeignKeyAction::NoAction => CatalogForeignKeyAction::NoAction,
        ForeignKeyAction::Restrict => CatalogForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade => CatalogForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull => CatalogForeignKeyAction::SetNull,
    }
}

fn catalog_type(column: &radixdb_core::SchemaColumn) -> Result<CatalogDataType> {
    match column.data_type {
        DataType::Decimal => {
            CatalogDataType::decimal(column.decimal_precision, column.decimal_scale)
        }
        DataType::Vector => CatalogDataType::vector(column.vector_dimensions),
        data_type => CatalogDataType::scalar(data_type),
    }
    .map_err(catalog_argument)
}

fn normalize(name: &str) -> Result<String> {
    Ok(CatalogName::new(name)
        .map_err(catalog_argument)?
        .normalized()
        .as_str()
        .to_owned())
}

fn precondition(object: &CatalogObject) -> Result<ObjectPrecondition> {
    ObjectPrecondition::new(object.id(), object.kind(), object.definition_revision())
        .map_err(catalog_argument)
}

fn with_revision(object: &CatalogObject, revision: u64) -> Result<CatalogObject> {
    CatalogObject::new(
        object.id(),
        object.namespace_id(),
        object.parent_id(),
        object.owner_principal_id(),
        object.name().clone(),
        revision,
        object.payload().clone(),
    )
    .map_err(catalog_argument)
}
