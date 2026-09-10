use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    AccessMethod, CatalogDataType, CatalogEdge, CatalogGeneration, CatalogMutation,
    CatalogMutationSet, CatalogName, CatalogObject, CatalogPayload, ColumnPayload,
    ConstraintPayload, EdgeKind, ForeignKeyAction as CatalogForeignKeyAction, ForeignKeyMatch,
    HnswDistanceMetric, HnswParameters, IndexPayload, ObjectId, ObjectKind, ObjectPrecondition,
    TablePayload, ViewPayload,
};
use radixdb_core::{
    sha256_digest, DataType, Error, ForeignKeyAction, IndexType, Result, Schema,
    SchemaConstraintKind,
};

use crate::traits::{Engine, PendingIndexDefinition, PendingIndexRename};

use super::super::MVCCEngine;

/// Commit one runtime schema through the same typed catalog boundary required
/// by production DDL. This is deliberately test-only: SQL binding remains an
/// executor responsibility and storage never invents catalog mutations.
pub(crate) fn create_catalog_test_table(engine: &MVCCEngine, mut schema: Schema) -> Result<Schema> {
    schema.ensure_catalog_identity();
    schema.ensure_constraint_catalog()?;
    let source = engine.pin_catalog()?;
    let mutation = create_table_mutation(source.as_ref(), &schema)?;
    let table_name = schema.table_name.clone();
    let mut transaction = engine.begin_transaction()?;
    transaction.create_table(&table_name, schema.clone())?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()?;
    Ok(schema)
}

/// Bind one explicit test index into the authoritative catalog generation.
///
/// The helper exists because storage unit tests cannot depend upward on the
/// executor-owned SQL binder. It deliberately accepts the already bound
/// storage definition and produces only the typed catalog half of the same
/// transaction; production code never calls this structural test adapter.
pub(crate) fn create_catalog_test_index_mutation(
    engine: &MVCCEngine,
    definition: &PendingIndexDefinition,
) -> Result<CatalogMutationSet> {
    let generation = engine.pin_catalog()?;
    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, &definition.table_name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(definition.table_name.clone()))?;
    if generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, &definition.index_name)
        .map_err(catalog_error)?
        .is_some()
    {
        return Err(Error::IndexAlreadyExists(definition.index_name.clone()));
    }
    let CatalogPayload::Table(table_payload) = table.payload() else {
        return Err(Error::internal(
            "catalog test table has a non-table payload",
        ));
    };

    if definition.columns.is_empty() {
        return Err(Error::invalid_argument(
            "test index must reference at least one column",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut key_column_ids = Vec::with_capacity(definition.columns.len());
    for name in &definition.columns {
        let column = generation
            .find_column(table.id(), name)
            .map_err(catalog_error)?
            .ok_or_else(|| Error::ColumnNotFound(name.clone()))?;
        if !seen.insert(column.id()) {
            return Err(Error::invalid_argument(format!(
                "test index references column '{name}' more than once"
            )));
        }
        key_column_ids.push(column.id());
    }

    let index_type = definition.index_type.unwrap_or(resolve_default_index_type(
        generation.as_ref(),
        &key_column_ids,
    )?);
    let payload = index_payload(generation.as_ref(), definition, index_type, &key_column_ids)?;
    let index_id = ObjectId::new();
    let index = CatalogObject::new(
        index_id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(table.id()),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(&definition.index_name).map_err(catalog_error)?,
        1,
        CatalogPayload::Index(payload),
    )
    .map_err(catalog_error)?;

    let mut index_ids = table_payload.index_ids().to_vec();
    let ordinal = u32::try_from(index_ids.len())
        .map_err(|_| Error::invalid_argument("too many test table indexes"))?;
    index_ids.push(index_id);
    let replacement_revision = table
        .definition_revision()
        .checked_add(1)
        .ok_or_else(|| Error::internal("catalog test table revision overflow"))?;
    let replacement = CatalogObject::new(
        table.id(),
        table.namespace_id(),
        table.parent_id(),
        table.owner_principal_id(),
        table.name().clone(),
        replacement_revision,
        CatalogPayload::Table(
            TablePayload::new(
                table_payload.column_ids().to_vec(),
                table_payload.constraint_ids().to_vec(),
                index_ids,
                table_payload.primary_key_constraint_id(),
            )
            .map_err(catalog_error)?,
        ),
    )
    .map_err(catalog_error)?;
    let precondition =
        ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
            .map_err(catalog_error)?;
    let meta = generation.meta();
    CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        vec![
            CatalogMutation::alter(precondition, replacement),
            CatalogMutation::create(index),
        ],
        Vec::new(),
        vec![CatalogEdge::new(
            table.id(),
            index_id,
            EdgeKind::Contains,
            ordinal,
        )],
    )
    .map_err(catalog_error)
}

pub(crate) fn drop_catalog_test_table(engine: &MVCCEngine, table_name: &str) -> Result<()> {
    let generation = engine.pin_catalog()?;
    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
    if let Some(dependent) = generation.graph().dependents(table.id()).next() {
        return Err(Error::invalid_argument(format!(
            "cannot drop table '{table_name}': catalog object '{}' depends on it",
            dependent.name().display().as_str()
        )));
    }
    let mut mutations = generation
        .graph()
        .children(table.id())
        .map(|child| {
            ObjectPrecondition::new(child.id(), child.kind(), child.definition_revision())
                .map(CatalogMutation::drop)
                .map_err(catalog_error)
        })
        .collect::<Result<Vec<_>>>()?;
    mutations.push(CatalogMutation::drop(
        ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
            .map_err(catalog_error)?,
    ));
    let meta = generation.meta();
    let mutation = CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        mutations,
        Vec::new(),
        Vec::new(),
    )
    .map_err(catalog_error)?;
    let mut transaction = engine.begin_transaction()?;
    transaction.drop_table(table_name)?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()
}

pub(crate) fn rename_catalog_test_table(
    engine: &MVCCEngine,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let generation = engine.pin_catalog()?;
    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, old_name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(old_name.to_owned()))?;
    if generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, new_name)
        .map_err(catalog_error)?
        .is_some()
    {
        return Err(Error::TableAlreadyExists(new_name.to_owned()));
    }
    if let Some(view) = generation
        .graph()
        .dependents(table.id())
        .find(|object| object.kind() == ObjectKind::View)
    {
        return Err(Error::invalid_argument(format!(
            "cannot rename table '{old_name}': view '{}' persists SQL that depends on its name",
            view.name().display().as_str()
        )));
    }
    let mutation = single_catalog_mutation(
        generation.as_ref(),
        CatalogMutation::rename(
            ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
                .map_err(catalog_error)?,
            CatalogName::new(new_name).map_err(catalog_error)?,
        ),
    )?;
    let mut transaction = engine.begin_transaction()?;
    transaction.rename_table(old_name, new_name)?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()
}

pub(crate) fn create_catalog_test_view(
    engine: &MVCCEngine,
    name: &str,
    canonical_sql: &str,
    dependency_names: &[&str],
) -> Result<()> {
    let generation = engine.pin_catalog()?;
    if generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_error)?
        .is_some()
    {
        return Err(Error::ViewAlreadyExists(name.to_owned()));
    }
    let mut dependency_ids = dependency_names
        .iter()
        .map(|dependency| {
            generation
                .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, dependency)
                .map_err(catalog_error)?
                .map(CatalogObject::id)
                .ok_or_else(|| Error::TableNotFound((*dependency).to_owned()))
        })
        .collect::<Result<Vec<_>>>()?;
    dependency_ids.sort_unstable();
    dependency_ids.dedup();
    let view_id = ObjectId::new();
    let view = CatalogObject::new(
        view_id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).map_err(catalog_error)?,
        1,
        CatalogPayload::View(
            ViewPayload::new(
                canonical_sql,
                dependency_ids.clone(),
                sha256_digest(canonical_sql.as_bytes()),
            )
            .map_err(catalog_error)?,
        ),
    )
    .map_err(catalog_error)?;
    let mut edges = vec![CatalogEdge::new(
        ObjectId::BOOTSTRAP_NAMESPACE,
        view_id,
        EdgeKind::Contains,
        next_namespace_ordinal(generation.as_ref())?,
    )];
    edges.extend(
        dependency_ids.into_iter().enumerate().map(|(ordinal, id)| {
            CatalogEdge::new(view_id, id, EdgeKind::DependsOn, ordinal as u32)
        }),
    );
    let meta = generation.meta();
    let mutation = CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        vec![CatalogMutation::create(view)],
        Vec::new(),
        edges,
    )
    .map_err(catalog_error)?;
    let mut transaction = engine.begin_transaction()?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()
}

pub(crate) fn drop_catalog_test_view(engine: &MVCCEngine, name: &str) -> Result<()> {
    let generation = engine.pin_catalog()?;
    let view = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::View)
        .ok_or_else(|| Error::ViewNotFound(name.to_owned()))?;
    if let Some(dependent) = generation.graph().dependents(view.id()).next() {
        return Err(Error::invalid_argument(format!(
            "cannot drop view '{name}': catalog object '{}' depends on it",
            dependent.name().display().as_str()
        )));
    }
    let mutation = single_catalog_mutation(
        generation.as_ref(),
        CatalogMutation::drop(
            ObjectPrecondition::new(view.id(), ObjectKind::View, view.definition_revision())
                .map_err(catalog_error)?,
        ),
    )?;
    let mut transaction = engine.begin_transaction()?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()
}

/// Commit an explicit index rename with one physical and catalog outcome.
pub(crate) fn rename_catalog_test_index(
    engine: &MVCCEngine,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let generation = engine.pin_catalog()?;
    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
    let index = generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, old_name)
        .map_err(catalog_error)?
        .filter(|object| object.parent_id() == Some(table.id()))
        .ok_or_else(|| Error::IndexNotFound(old_name.to_owned()))?;
    if generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, new_name)
        .map_err(catalog_error)?
        .is_some()
    {
        return Err(Error::IndexAlreadyExists(new_name.to_owned()));
    }
    let precondition =
        ObjectPrecondition::new(index.id(), ObjectKind::Index, index.definition_revision())
            .map_err(catalog_error)?;
    let meta = generation.meta();
    let mutation = CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        vec![CatalogMutation::rename(
            precondition,
            CatalogName::new(new_name).map_err(catalog_error)?,
        )],
        Vec::new(),
        Vec::new(),
    )
    .map_err(catalog_error)?;
    let mut transaction = engine.begin_transaction()?;
    transaction.stage_rename_index(PendingIndexRename {
        table_name: table_name.to_owned(),
        old_index_name: old_name.to_owned(),
        new_index_name: new_name.to_owned(),
    })?;
    transaction.stage_catalog_mutation(mutation)?;
    transaction.commit()
}

fn resolve_default_index_type(
    generation: &CatalogGeneration,
    key_column_ids: &[ObjectId],
) -> Result<IndexType> {
    if key_column_ids.len() != 1 {
        return Ok(IndexType::MultiColumn);
    }
    let column = generation
        .object(key_column_ids[0])
        .ok_or_else(|| Error::internal("catalog test index column disappeared"))?;
    let CatalogPayload::Column(payload) = column.payload() else {
        return Err(Error::internal(
            "catalog test index key has a non-column payload",
        ));
    };
    Ok(match payload.data_type().logical_type() {
        DataType::Text | DataType::Json | DataType::Bytes => IndexType::Hash,
        DataType::Boolean => IndexType::Bitmap,
        DataType::Vector => IndexType::Hnsw,
        _ => IndexType::BTree,
    })
}

fn index_payload(
    generation: &CatalogGeneration,
    definition: &PendingIndexDefinition,
    index_type: IndexType,
    key_column_ids: &[ObjectId],
) -> Result<IndexPayload> {
    if index_type == IndexType::PrimaryKey {
        return Err(Error::invalid_argument(
            "primary-key indexes are constraint-owned",
        ));
    }
    if index_type == IndexType::Hnsw {
        if definition.is_unique || key_column_ids.len() != 1 {
            return Err(Error::invalid_argument(
                "HNSW test index requires one non-unique key",
            ));
        }
        if definition.partial_predicate.is_some() {
            return Err(Error::invalid_argument(
                "partial HNSW test indexes are not supported",
            ));
        }
        let column = generation
            .object(key_column_ids[0])
            .ok_or_else(|| Error::internal("catalog test HNSW column disappeared"))?;
        let CatalogPayload::Column(column_payload) = column.payload() else {
            return Err(Error::internal(
                "catalog test HNSW key has a non-column payload",
            ));
        };
        if column_payload.data_type().logical_type() != DataType::Vector {
            return Err(Error::invalid_argument(
                "HNSW test index key must be a VECTOR column",
            ));
        }
        let dimensions = usize::try_from(column_payload.data_type().parameter_1())
            .map_err(|_| Error::invalid_argument("test vector dimensions exceed usize"))?;
        let m = definition
            .hnsw_m
            .unwrap_or(crate::index::default_m_for_dims(dimensions) as u16);
        let ef_construction = definition
            .hnsw_ef_construction
            .unwrap_or(crate::index::default_ef_construction(usize::from(m)) as u16);
        let ef_search = definition
            .hnsw_ef_search
            .unwrap_or(crate::index::default_ef_search(usize::from(m)) as u16);
        let metric = match definition.hnsw_distance_metric.unwrap_or(0) {
            0 => HnswDistanceMetric::L2,
            1 => HnswDistanceMetric::Cosine,
            2 => HnswDistanceMetric::Dot,
            value => {
                return Err(Error::invalid_argument(format!(
                    "unknown test HNSW distance metric: {value}"
                )))
            }
        };
        let parameters =
            HnswParameters::new(m, ef_construction, ef_search, metric).map_err(catalog_error)?;
        return IndexPayload::new_hnsw(key_column_ids[0], Vec::new(), parameters)
            .map_err(catalog_error);
    }

    let method = match index_type {
        IndexType::BTree | IndexType::MultiColumn => AccessMethod::Btree,
        IndexType::Hash => AccessMethod::Hash,
        IndexType::Bitmap => AccessMethod::Bitmap,
        IndexType::Hnsw | IndexType::PrimaryKey => unreachable!("handled above"),
    };
    IndexPayload::new(
        method,
        definition.is_unique,
        key_column_ids.to_vec(),
        Vec::new(),
        None,
        definition
            .partial_predicate
            .as_ref()
            .map(|predicate| predicate.canonical_sql().to_owned()),
    )
    .map_err(catalog_error)
}

fn create_table_mutation(
    generation: &CatalogGeneration,
    schema: &Schema,
) -> Result<CatalogMutationSet> {
    if generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, &schema.table_name)
        .map_err(catalog_error)?
        .is_some()
    {
        return Err(Error::TableAlreadyExists(schema.table_name.clone()));
    }

    let table_id = ObjectId::from_user_bytes(schema.catalog_id)
        .map_err(|error| Error::invalid_argument(error.to_string()))?;
    let mut column_ids = Vec::with_capacity(schema.columns.len());
    let mut columns_by_name = BTreeMap::new();
    let mut objects = Vec::new();
    let mut edges = Vec::new();

    for (ordinal, column) in schema.columns.iter().enumerate() {
        let column_id = ObjectId::new();
        let payload = ColumnPayload::new_with_auto_increment(
            u32::try_from(ordinal)
                .map_err(|_| Error::invalid_argument("too many test table columns"))?,
            catalog_data_type(column)?,
            column.nullable,
            column.auto_increment,
            column.default_expr.clone(),
            None,
        )
        .map_err(catalog_error)?;
        objects.push(
            CatalogObject::new(
                column_id,
                Some(ObjectId::BOOTSTRAP_NAMESPACE),
                Some(table_id),
                ObjectId::BOOTSTRAP_OWNER,
                CatalogName::new(&column.name).map_err(catalog_error)?,
                1,
                CatalogPayload::Column(payload),
            )
            .map_err(catalog_error)?,
        );
        edges.push(CatalogEdge::new(
            table_id,
            column_id,
            EdgeKind::Contains,
            ordinal as u32,
        ));
        column_ids.push(column_id);
        columns_by_name.insert(column.name_lower.clone(), column_id);
    }

    let mut constraint_ids = Vec::new();
    let mut index_ids = Vec::new();
    let mut primary_key_id = None;
    for constraint in schema.constraints() {
        let constraint_id = ObjectId::new();
        let (payload, references) = constraint_payload(
            generation,
            schema,
            table_id,
            &columns_by_name,
            &constraint.kind,
        )?;
        if matches!(payload, ConstraintPayload::PrimaryKey { .. }) {
            primary_key_id = Some(constraint_id);
        }
        objects.push(
            CatalogObject::new(
                constraint_id,
                Some(ObjectId::BOOTSTRAP_NAMESPACE),
                Some(table_id),
                ObjectId::BOOTSTRAP_OWNER,
                CatalogName::new(&constraint.name).map_err(catalog_error)?,
                1,
                CatalogPayload::Constraint(payload.clone()),
            )
            .map_err(catalog_error)?,
        );
        let constraint_ordinal = u32::try_from(constraint_ids.len())
            .map_err(|_| Error::invalid_argument("too many test table constraints"))?;
        edges.push(CatalogEdge::new(
            table_id,
            constraint_id,
            EdgeKind::Contains,
            constraint_ordinal,
        ));
        for (ordinal, referenced_id) in references.into_iter().enumerate() {
            edges.push(CatalogEdge::new(
                constraint_id,
                referenced_id,
                EdgeKind::References,
                ordinal as u32,
            ));
        }
        constraint_ids.push(constraint_id);

        let key_columns = match payload {
            ConstraintPayload::PrimaryKey { local_column_ids }
            | ConstraintPayload::Unique { local_column_ids } => Some(local_column_ids),
            _ => None,
        };
        if let Some(key_columns) = key_columns {
            let index_id = ObjectId::new();
            let index_name = format!("{}_idx", constraint.name);
            let payload = IndexPayload::new(
                AccessMethod::Btree,
                true,
                key_columns,
                Vec::new(),
                None,
                None,
            )
            .map_err(catalog_error)?;
            objects.push(
                CatalogObject::new(
                    index_id,
                    Some(ObjectId::BOOTSTRAP_NAMESPACE),
                    Some(table_id),
                    ObjectId::BOOTSTRAP_OWNER,
                    CatalogName::new(index_name).map_err(catalog_error)?,
                    1,
                    CatalogPayload::Index(payload),
                )
                .map_err(catalog_error)?,
            );
            edges.push(CatalogEdge::new(
                table_id,
                index_id,
                EdgeKind::Contains,
                index_ids.len() as u32,
            ));
            edges.push(CatalogEdge::new(
                index_id,
                constraint_id,
                EdgeKind::DependsOn,
                0,
            ));
            index_ids.push(index_id);
        }
    }

    let table = CatalogObject::new(
        table_id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(&schema.table_name).map_err(catalog_error)?,
        1,
        CatalogPayload::Table(
            TablePayload::new(column_ids, constraint_ids, index_ids, primary_key_id)
                .map_err(catalog_error)?,
        ),
    )
    .map_err(catalog_error)?;
    edges.push(CatalogEdge::new(
        ObjectId::BOOTSTRAP_NAMESPACE,
        table_id,
        EdgeKind::Contains,
        generation.objects_of_kind(ObjectKind::Table).count() as u32,
    ));

    let mut mutations = Vec::with_capacity(objects.len() + 1);
    mutations.push(CatalogMutation::create(table));
    mutations.extend(objects.into_iter().map(CatalogMutation::create));
    let meta = generation.meta();
    CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        mutations,
        Vec::new(),
        edges,
    )
    .map_err(catalog_error)
}

pub(crate) fn catalog_data_type(column: &radixdb_core::SchemaColumn) -> Result<CatalogDataType> {
    match column.data_type {
        DataType::Decimal => {
            if column.decimal_precision == 0 {
                CatalogDataType::unconstrained_decimal()
            } else {
                CatalogDataType::decimal(column.decimal_precision, column.decimal_scale)
            }
        }
        DataType::Vector => CatalogDataType::vector(column.vector_dimensions),
        data_type => CatalogDataType::scalar(data_type),
    }
    .map_err(catalog_error)
}

fn constraint_payload(
    generation: &CatalogGeneration,
    schema: &Schema,
    table_id: ObjectId,
    columns: &BTreeMap<String, ObjectId>,
    kind: &SchemaConstraintKind,
) -> Result<(ConstraintPayload, Vec<ObjectId>)> {
    let local = |names: &[String]| {
        names
            .iter()
            .map(|name| {
                columns
                    .get(&name.to_lowercase())
                    .copied()
                    .ok_or_else(|| Error::ColumnNotFound(name.clone()))
            })
            .collect::<Result<Vec<_>>>()
    };
    match kind {
        SchemaConstraintKind::PrimaryKey { columns: names } => Ok((
            ConstraintPayload::primary_key(local(names)?).map_err(catalog_error)?,
            Vec::new(),
        )),
        SchemaConstraintKind::Unique { columns: names, .. } => Ok((
            ConstraintPayload::unique(local(names)?).map_err(catalog_error)?,
            Vec::new(),
        )),
        SchemaConstraintKind::Check {
            column_name,
            expression,
            ..
        } => {
            let payload = match column_name {
                Some(column_name) => ConstraintPayload::column_check(
                    columns
                        .get(&column_name.to_lowercase())
                        .copied()
                        .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?,
                    expression.clone(),
                ),
                None => ConstraintPayload::check(expression.clone()),
            }
            .map_err(catalog_error)?;
            Ok((payload, Vec::new()))
        }
        SchemaConstraintKind::ForeignKey {
            columns: names,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            let local_ids = local(names)?;
            let (referenced_table_id, referenced_column_ids) =
                if referenced_table.eq_ignore_ascii_case(&schema.table_name) {
                    (
                        table_id,
                        referenced_columns
                            .iter()
                            .map(|name| {
                                columns
                                    .get(&name.to_lowercase())
                                    .copied()
                                    .ok_or_else(|| Error::ColumnNotFound(name.clone()))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                } else {
                    let table = generation
                        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, referenced_table)
                        .map_err(catalog_error)?
                        .filter(|object| object.kind() == ObjectKind::Table)
                        .ok_or_else(|| Error::TableNotFound(referenced_table.clone()))?;
                    let ids = referenced_columns
                        .iter()
                        .map(|name| {
                            generation
                                .find_column(table.id(), name)
                                .map_err(catalog_error)?
                                .map(CatalogObject::id)
                                .ok_or_else(|| Error::ColumnNotFound(name.clone()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    (table.id(), ids)
                };
            let payload = ConstraintPayload::foreign_key(
                local_ids,
                referenced_table_id,
                referenced_column_ids.clone(),
                ForeignKeyMatch::Simple,
                foreign_key_action(*on_update),
                foreign_key_action(*on_delete),
            )
            .map_err(catalog_error)?;
            let mut references = vec![referenced_table_id];
            references.extend(referenced_column_ids);
            Ok((payload, references))
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

fn single_catalog_mutation(
    generation: &CatalogGeneration,
    mutation: CatalogMutation,
) -> Result<CatalogMutationSet> {
    let meta = generation.meta();
    CatalogMutationSet::new(
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        vec![mutation],
        Vec::new(),
        Vec::new(),
    )
    .map_err(catalog_error)
}

pub(crate) fn next_namespace_ordinal(generation: &CatalogGeneration) -> Result<u32> {
    generation
        .graph()
        .outgoing_edges(ObjectId::BOOTSTRAP_NAMESPACE)
        .filter(|edge| edge.kind() == EdgeKind::Contains)
        .map(|edge| edge.ordinal())
        .max()
        .map_or(Ok(0), |ordinal| {
            ordinal
                .checked_add(1)
                .ok_or_else(|| Error::invalid_argument("catalog namespace ordinal overflow"))
        })
}

fn catalog_error(error: impl std::fmt::Display) -> Error {
    Error::invalid_argument(format!("test catalog mutation rejected: {error}"))
}
