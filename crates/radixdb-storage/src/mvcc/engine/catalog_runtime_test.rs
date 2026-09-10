//! Independent structural catalog oracle for storage unit tests.
//!
//! Production recovery receives its SQL-aware binder from `radixdb-executor`.
//! Storage unit tests cannot depend back on that crate, so this module binds
//! the same persisted object graph without parsing SQL. Keeping the oracle
//! test-only prevents it from becoming a second production catalog authority.

use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    AccessMethod, CatalogGeneration, CatalogObject, CatalogPayload, ConstraintPayload, EdgeKind,
    ForeignKeyAction as CatalogForeignKeyAction, HnswDistanceMetric, IndexPayload, ObjectId,
    ObjectKind, TablePayload,
};
use radixdb_core::{
    DataType, Error, ForeignKeyAction, ForeignKeyConstraint, IndexType, Result, SchemaBuilder,
    SchemaConstraint, SchemaConstraintKind,
};

use crate::index::PartialIndexPredicateMetadata;
use crate::mvcc::{CatalogRuntime, CatalogRuntimeTable, IndexDefinition, ViewDefinition};

#[derive(Debug, Clone)]
struct RuntimeColumn {
    name: String,
    ordinal: usize,
    data_type: DataType,
}

pub(crate) fn bind_test_catalog_runtime(generation: &CatalogGeneration) -> Result<CatalogRuntime> {
    let tables = generation
        .objects_of_kind(ObjectKind::Table)
        .map(|table| bind_table(generation, table))
        .collect::<Result<Vec<_>>>()?;
    let views = generation
        .objects_of_kind(ObjectKind::View)
        .map(|view| bind_view(generation, view))
        .collect::<Result<Vec<_>>>()?;
    CatalogRuntime::new(tables, views)
}

fn bind_table(
    generation: &CatalogGeneration,
    table: &CatalogObject,
) -> Result<CatalogRuntimeTable> {
    let CatalogPayload::Table(payload) = table.payload() else {
        return Err(Error::internal("catalog table has a non-table payload"));
    };
    let primary_key_columns = primary_key_columns(generation, payload)?;
    let mut builder = SchemaBuilder::new(table.name().display().as_str());
    let mut columns = BTreeMap::new();

    for (expected_ordinal, column_id) in payload.column_ids().iter().enumerate() {
        let column = generation
            .object(*column_id)
            .ok_or_else(|| Error::internal("catalog table column disappeared"))?;
        let CatalogPayload::Column(column_payload) = column.payload() else {
            return Err(Error::internal("catalog table column has wrong payload"));
        };
        if usize::try_from(column_payload.ordinal()).ok() != Some(expected_ordinal) {
            return Err(Error::internal(
                "catalog column has a non-canonical ordinal",
            ));
        }
        if column_payload.generated_sql().is_some() {
            return Err(Error::NotSupported(
                "generated-column catalog recovery is not implemented".to_owned(),
            ));
        }
        let name = column.name().display().as_str().to_owned();
        let catalog_type = column_payload.data_type();
        let data_type = catalog_type.logical_type();
        builder = builder.add_with_constraints(
            name.clone(),
            data_type,
            column_payload.nullable(),
            primary_key_columns.contains(column_id),
            column_payload.auto_increment(),
            column_payload
                .default_sql()
                .map(|sql| sql.as_str().to_owned()),
            None,
        );
        if data_type == DataType::Vector {
            builder = builder.set_last_vector_dimensions(catalog_type.parameter_1() as u16);
        }
        if data_type == DataType::Decimal {
            builder = builder.set_last_decimal_parameters(
                catalog_type.parameter_1() as u8,
                catalog_type.parameter_2() as u8,
            );
        }
        columns.insert(
            *column_id,
            RuntimeColumn {
                name,
                ordinal: expected_ordinal,
                data_type,
            },
        );
    }

    let constraints = bind_constraints(generation, table, payload, &columns)?;
    for foreign_key in constraints.foreign_keys {
        builder = builder.add_foreign_key(foreign_key);
    }
    for check in constraints.table_checks {
        builder = builder.add_table_check(check);
    }
    let mut schema = builder.build();
    for (column_name, check) in constraints.column_checks {
        schema.set_column_check(&column_name, Some(check))?;
    }
    schema.install_catalog_identity(table.id().into_bytes())?;
    schema.install_constraint_catalog(
        constraints.catalog,
        constraints.next_id,
        constraints.next_check_ordinal,
    )?;

    let indexes = payload
        .index_ids()
        .iter()
        .map(|id| bind_index(generation, table, *id, &columns))
        .collect::<Result<Vec<_>>>()?;
    CatalogRuntimeTable::new(schema, indexes)
}

fn primary_key_columns(
    generation: &CatalogGeneration,
    table: &TablePayload,
) -> Result<BTreeSet<ObjectId>> {
    let Some(id) = table.primary_key_constraint_id() else {
        return Ok(BTreeSet::new());
    };
    let object = generation
        .object(id)
        .ok_or_else(|| Error::internal("primary-key catalog object disappeared"))?;
    let CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) =
        object.payload()
    else {
        return Err(Error::internal(
            "table primary-key identity has a non-primary-key payload",
        ));
    };
    Ok(local_column_ids.iter().copied().collect())
}

#[derive(Debug, Default)]
struct BoundConstraints {
    foreign_keys: Vec<ForeignKeyConstraint>,
    column_checks: Vec<(String, String)>,
    table_checks: Vec<String>,
    catalog: Vec<SchemaConstraint>,
    next_id: u64,
    next_check_ordinal: u32,
}

fn bind_constraints(
    generation: &CatalogGeneration,
    table_object: &CatalogObject,
    table: &TablePayload,
    columns: &BTreeMap<ObjectId, RuntimeColumn>,
) -> Result<BoundConstraints> {
    let mut output = BoundConstraints {
        next_id: 1,
        next_check_ordinal: 1,
        ..BoundConstraints::default()
    };
    for constraint_id in ordered_constraint_ids(generation, table_object, table)? {
        let object = generation
            .object(constraint_id)
            .ok_or_else(|| Error::internal("catalog constraint disappeared"))?;
        let CatalogPayload::Constraint(payload) = object.payload() else {
            return Err(Error::internal("catalog constraint has wrong payload"));
        };
        let kind = match payload {
            ConstraintPayload::PrimaryKey { local_column_ids } => {
                SchemaConstraintKind::PrimaryKey {
                    columns: resolve_column_names(columns, local_column_ids)?,
                }
            }
            ConstraintPayload::Unique { local_column_ids } => SchemaConstraintKind::Unique {
                columns: resolve_column_names(columns, local_column_ids)?,
                index_name: constraint_index_name(generation, constraint_id)?,
            },
            ConstraintPayload::ForeignKey {
                local_column_ids,
                referenced_table_id,
                referenced_column_ids,
                on_update_action,
                on_delete_action,
                ..
            } => bind_foreign_key(
                generation,
                columns,
                local_column_ids,
                *referenced_table_id,
                referenced_column_ids,
                *on_update_action,
                *on_delete_action,
                &mut output.foreign_keys,
            )?,
            ConstraintPayload::Check {
                local_column_id,
                check_sql,
            } => {
                let ordinal = output.next_check_ordinal;
                output.next_check_ordinal = output
                    .next_check_ordinal
                    .checked_add(1)
                    .ok_or_else(|| Error::internal("CHECK ordinal overflow"))?;
                let expression = check_sql.as_str().to_owned();
                let column_name = local_column_id
                    .map(|column_id| {
                        columns
                            .get(&column_id)
                            .map(|column| column.name.clone())
                            .ok_or_else(|| Error::internal("CHECK column disappeared"))
                    })
                    .transpose()?;
                if let Some(column_name) = &column_name {
                    if output
                        .column_checks
                        .iter()
                        .any(|(existing, _)| existing.eq_ignore_ascii_case(column_name))
                    {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' has more than one CHECK constraint",
                            column_name
                        )));
                    }
                    output
                        .column_checks
                        .push((column_name.clone(), expression.clone()));
                } else {
                    output.table_checks.push(expression.clone());
                }
                SchemaConstraintKind::Check {
                    column_name,
                    expression,
                    ordinal,
                }
            }
            ConstraintPayload::NotNull { .. } => continue,
        };
        output.catalog.push(SchemaConstraint {
            id: output.next_id,
            name: object.name().display().as_str().to_owned(),
            kind,
        });
        output.next_id = output
            .next_id
            .checked_add(1)
            .ok_or_else(|| Error::internal("runtime constraint identity overflow"))?;
    }
    Ok(output)
}

fn ordered_constraint_ids(
    generation: &CatalogGeneration,
    table_object: &CatalogObject,
    table: &TablePayload,
) -> Result<Vec<ObjectId>> {
    let admitted = table
        .constraint_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut ordered = generation
        .graph()
        .outgoing_edges(table_object.id())
        .filter(|edge| edge.kind() == EdgeKind::Contains)
        .filter(|edge| admitted.contains(&edge.target_object_id()))
        .map(|edge| (edge.ordinal(), edge.target_object_id()))
        .collect::<Vec<_>>();
    ordered.sort_unstable();
    if ordered.len() != admitted.len() {
        return Err(Error::internal(
            "table constraint list differs from containment graph",
        ));
    }
    Ok(ordered.into_iter().map(|(_, id)| id).collect())
}

#[allow(clippy::too_many_arguments)]
fn bind_foreign_key(
    generation: &CatalogGeneration,
    columns: &BTreeMap<ObjectId, RuntimeColumn>,
    local_column_ids: &[ObjectId],
    referenced_table_id: ObjectId,
    referenced_column_ids: &[ObjectId],
    on_update_action: CatalogForeignKeyAction,
    on_delete_action: CatalogForeignKeyAction,
    foreign_keys: &mut Vec<ForeignKeyConstraint>,
) -> Result<SchemaConstraintKind> {
    if local_column_ids.len() != 1 || referenced_column_ids.len() != 1 {
        return Err(Error::NotSupported(
            "composite foreign-key catalog recovery is not implemented".to_owned(),
        ));
    }
    let local = columns
        .get(&local_column_ids[0])
        .ok_or_else(|| Error::internal("foreign-key local column disappeared"))?;
    let referenced_table = generation
        .object(referenced_table_id)
        .ok_or_else(|| Error::internal("foreign-key target table disappeared"))?;
    let referenced_column = generation
        .object(referenced_column_ids[0])
        .ok_or_else(|| Error::internal("foreign-key target column disappeared"))?;
    let on_update = bind_foreign_key_action(on_update_action)?;
    let on_delete = bind_foreign_key_action(on_delete_action)?;
    foreign_keys.push(ForeignKeyConstraint {
        column_index: local.ordinal,
        column_name: local.name.clone(),
        referenced_table: referenced_table.name().normalized().as_str().to_owned(),
        referenced_column: referenced_column.name().normalized().as_str().to_owned(),
        on_delete,
        on_update,
    });
    Ok(SchemaConstraintKind::ForeignKey {
        columns: vec![local.name.clone()],
        referenced_table: referenced_table.name().display().as_str().to_owned(),
        referenced_columns: vec![referenced_column.name().display().as_str().to_owned()],
        on_delete,
        on_update,
    })
}

fn resolve_column_names(
    columns: &BTreeMap<ObjectId, RuntimeColumn>,
    ids: &[ObjectId],
) -> Result<Vec<String>> {
    ids.iter()
        .map(|id| {
            columns
                .get(id)
                .map(|column| column.name.clone())
                .ok_or_else(|| Error::internal("constraint references a non-table column"))
        })
        .collect()
}

fn constraint_index_name(generation: &CatalogGeneration, id: ObjectId) -> Result<String> {
    let mut names = generation
        .graph()
        .incoming_edges(id)
        .filter(|edge| edge.kind() == EdgeKind::DependsOn)
        .filter_map(|edge| generation.object(edge.source_object_id()))
        .filter(|object| object.kind() == ObjectKind::Index)
        .map(|index| index.name().display().as_str().to_owned());
    let name = names
        .next()
        .ok_or_else(|| Error::internal("UNIQUE constraint has no owning index"))?;
    if names.next().is_some() {
        return Err(Error::internal(
            "UNIQUE constraint has more than one owning index",
        ));
    }
    Ok(name)
}

fn bind_foreign_key_action(action: CatalogForeignKeyAction) -> Result<ForeignKeyAction> {
    match action {
        CatalogForeignKeyAction::NoAction => Ok(ForeignKeyAction::NoAction),
        CatalogForeignKeyAction::Restrict => Ok(ForeignKeyAction::Restrict),
        CatalogForeignKeyAction::Cascade => Ok(ForeignKeyAction::Cascade),
        CatalogForeignKeyAction::SetNull => Ok(ForeignKeyAction::SetNull),
        CatalogForeignKeyAction::SetDefault => Err(Error::NotSupported(
            "SET DEFAULT foreign-key catalog recovery is not implemented".to_owned(),
        )),
    }
}

fn bind_index(
    generation: &CatalogGeneration,
    table: &CatalogObject,
    index_id: ObjectId,
    columns: &BTreeMap<ObjectId, RuntimeColumn>,
) -> Result<IndexDefinition> {
    let index = generation
        .object(index_id)
        .ok_or_else(|| Error::internal("catalog index disappeared"))?;
    let CatalogPayload::Index(payload) = index.payload() else {
        return Err(Error::internal("catalog index has wrong payload"));
    };
    if payload.expression_sql().is_some() || !payload.include_column_ids().is_empty() {
        return Err(Error::NotSupported(
            "expression and INCLUDE index catalog recovery is not implemented".to_owned(),
        ));
    }
    let mut names = Vec::with_capacity(payload.key_column_ids().len());
    let mut ids = Vec::with_capacity(payload.key_column_ids().len());
    let mut data_types = Vec::with_capacity(payload.key_column_ids().len());
    for id in payload.key_column_ids() {
        let column = columns
            .get(id)
            .ok_or_else(|| Error::internal("index references a non-table column"))?;
        names.push(column.name.clone());
        ids.push(i32::try_from(column.ordinal).map_err(|_| {
            Error::NotSupported("index column ordinal exceeds runtime bounds".to_owned())
        })?);
        data_types.push(column.data_type);
    }
    let parameters = payload.hnsw_parameters();
    Ok(IndexDefinition {
        name: index.name().display().as_str().to_owned(),
        table_name: table.name().display().as_str().to_owned(),
        column_names: names,
        column_ids: ids,
        data_types,
        is_unique: payload.unique(),
        index_type: bind_index_type(generation, index, payload)?,
        hnsw_m: parameters.map(|value| value.m()),
        hnsw_ef_construction: parameters.map(|value| value.ef_construction()),
        hnsw_ef_search: parameters.map(|value| value.ef_search()),
        hnsw_distance_metric: parameters.map(|value| match value.distance_metric() {
            HnswDistanceMetric::L2 => 0,
            HnswDistanceMetric::Cosine => 1,
            HnswDistanceMetric::Dot => 2,
        }),
        partial_predicate: payload
            .predicate_sql()
            .map(|sql| PartialIndexPredicateMetadata::new(sql.as_str())),
        key_encoder: None,
    })
}

fn bind_index_type(
    generation: &CatalogGeneration,
    index: &CatalogObject,
    payload: &IndexPayload,
) -> Result<IndexType> {
    if generation
        .graph()
        .outgoing_edges(index.id())
        .filter(|edge| edge.kind() == EdgeKind::DependsOn)
        .filter_map(|edge| generation.object(edge.target_object_id()))
        .any(|dependency| {
            matches!(
                dependency.payload(),
                CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { .. })
            )
        })
    {
        return Ok(IndexType::PrimaryKey);
    }
    if payload.key_column_ids().len() > 1 {
        return if payload.access_method() == AccessMethod::Btree {
            Ok(IndexType::MultiColumn)
        } else {
            Err(Error::NotSupported(
                "multi-column non-B-tree catalog recovery is not implemented".to_owned(),
            ))
        };
    }
    Ok(match payload.access_method() {
        AccessMethod::Btree => IndexType::BTree,
        AccessMethod::Hash => IndexType::Hash,
        AccessMethod::Bitmap => IndexType::Bitmap,
        AccessMethod::Hnsw => IndexType::Hnsw,
    })
}

fn bind_view(generation: &CatalogGeneration, view: &CatalogObject) -> Result<ViewDefinition> {
    let CatalogPayload::View(payload) = view.payload() else {
        return Err(Error::internal("catalog view has a non-view payload"));
    };
    let mut dependencies = payload
        .dependency_ids()
        .iter()
        .map(|id| {
            generation
                .object(*id)
                .map(|object| object.name().normalized().as_str().to_owned())
                .ok_or_else(|| Error::internal("catalog view dependency disappeared"))
        })
        .collect::<Result<Vec<_>>>()?;
    dependencies.sort_unstable();
    dependencies.dedup();
    ViewDefinition::from_bound_query(
        view.name().display().as_str(),
        payload.canonical_sql().as_str().to_owned(),
        dependencies,
    )
}
