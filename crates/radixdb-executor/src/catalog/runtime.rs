use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use radixdb_catalog::{
    AccessMethod, CatalogGeneration, CatalogObject, CatalogPayload, ConstraintPayload, EdgeKind,
    ForeignKeyAction as CatalogForeignKeyAction, HnswDistanceMetric, IndexPayload, ObjectId,
    ObjectKind, TablePayload,
};
use radixdb_core::{
    DataType, Error, ForeignKeyAction, ForeignKeyConstraint, IndexType, Result, SchemaBuilder,
    SchemaConstraint, SchemaConstraintKind,
};
use radixdb_plugin_host::{InvocationLimits, PluginRegistry, RegisteredTypeRef};
use radixdb_storage::index::PartialIndexPredicateMetadata;
use radixdb_storage::mvcc::{
    CatalogRuntime, CatalogRuntimeBinder, CatalogRuntimeTable, IndexDefinition, ViewDefinition,
};
use radixdb_storage::PreparedIndexKeyEncoder;

#[derive(Debug, Clone)]
struct RuntimeColumn {
    name: String,
    ordinal: usize,
    data_type: DataType,
}

/// Build the complete runtime schema projection for one validated durable
/// catalog generation. This adapter retains no authority of its own.
#[doc(hidden)]
pub fn bind_runtime_catalog(generation: &CatalogGeneration) -> Result<CatalogRuntime> {
    bind_runtime_catalog_inner(generation, None)
}

/// Build a storage composition binder pinned to one immutable startup plugin
/// registry. Storage receives only prepared value-to-key callbacks.
#[doc(hidden)]
pub fn plugin_catalog_runtime_binder(registry: Arc<PluginRegistry>) -> CatalogRuntimeBinder {
    CatalogRuntimeBinder::new(move |generation| {
        bind_runtime_catalog_inner(generation, Some(registry.as_ref()))
    })
}

fn bind_runtime_catalog_inner(
    generation: &CatalogGeneration,
    plugin_registry: Option<&PluginRegistry>,
) -> Result<CatalogRuntime> {
    let tables = generation
        .objects_of_kind(ObjectKind::Table)
        .map(|table| bind_table(generation, table, plugin_registry))
        .collect::<Result<Vec<_>>>()?;
    let views = generation
        .objects_of_kind(ObjectKind::View)
        .map(|view| bind_view(generation, view))
        .collect::<Result<Vec<_>>>()?;
    CatalogRuntime::new(tables, views)
}

/// Resolve one view from an immutable catalog generation without consulting
/// the globally published runtime projection. Explicit SQL transactions use
/// this narrow adapter so their private CREATE/DROP VIEW state is visible only
/// to the owning executor until COMMIT.
pub(crate) fn bind_runtime_view(
    generation: &CatalogGeneration,
    view_name: &str,
) -> Result<Option<ViewDefinition>> {
    let Some(view) = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, view_name)
        .map_err(|error| Error::InvalidArgument(error.to_string()))?
    else {
        return Ok(None);
    };
    if view.kind() != ObjectKind::View {
        return Ok(None);
    }
    bind_view(generation, view).map(Some)
}

/// List view display names from one immutable generation. The result is
/// deterministic and does not leak a partially staged generation globally.
pub(crate) fn list_runtime_views(generation: &CatalogGeneration) -> Vec<String> {
    let mut views = generation
        .objects_of_kind(ObjectKind::View)
        .map(|view| view.name().display().as_str().to_owned())
        .collect::<Vec<_>>();
    views.sort_unstable_by_key(|name| name.to_lowercase());
    views
}

fn bind_table(
    generation: &CatalogGeneration,
    table: &CatalogObject,
    plugin_registry: Option<&PluginRegistry>,
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
            return Err(Error::internal(format!(
                "catalog column '{}' has a non-canonical ordinal",
                column.name().display().as_str()
            )));
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
        if let Some(default_sql) = column_payload.default_sql() {
            let value = crate::mutation::evaluate_default_expr(default_sql.as_str(), data_type)?;
            builder = builder.set_last_default_value((!value.is_null()).then_some(value));
        }
        if data_type == DataType::Vector {
            builder = builder.set_last_vector_dimensions(catalog_type.parameter_1() as u16);
        }
        if data_type == DataType::Decimal {
            builder = builder.set_last_decimal_parameters(
                catalog_type.parameter_1() as u8,
                catalog_type.parameter_2() as u8,
            );
        }
        if let Some(type_ref) = catalog_type.external_type_ref() {
            let type_id = catalog_type
                .type_object_id()
                .ok_or_else(|| Error::internal("external descriptor lost its type identity"))?;
            let type_object = generation
                .object(type_id)
                .ok_or_else(|| Error::internal("external type object disappeared"))?;
            builder = builder
                .set_last_external_type(type_ref, qualified_catalog_name(generation, type_object)?);
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

    let bound_constraints = bind_constraints(generation, table, payload, &columns)?;
    for foreign_key in bound_constraints.foreign_keys {
        builder = builder.add_foreign_key(foreign_key);
    }
    for check in bound_constraints.table_checks {
        builder = builder.add_table_check(check);
    }
    let mut schema = builder.build();
    for (column_name, check) in bound_constraints.column_checks {
        schema.set_column_check(&column_name, Some(check))?;
    }
    schema.install_catalog_identity(table.id().into_bytes())?;
    schema.install_constraint_catalog(
        bound_constraints.catalog,
        bound_constraints.next_id,
        bound_constraints.next_check_ordinal,
    )?;
    let fallback = generation.meta().created_unix_ns();
    schema.install_catalog_timestamps(
        catalog_timestamp(payload.created_unix_ns(), fallback)?,
        catalog_timestamp(payload.updated_unix_ns(), fallback)?,
    )?;

    let indexes = payload
        .index_ids()
        .iter()
        .map(|id| bind_index(generation, table, *id, &columns, plugin_registry))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    CatalogRuntimeTable::new(schema, indexes)
}

pub(crate) fn qualified_catalog_name(
    generation: &CatalogGeneration,
    object: &CatalogObject,
) -> Result<String> {
    let mut names = vec![object.name().display().as_str().to_owned()];
    let mut namespace_id = object.namespace_id();
    while let Some(id) = namespace_id {
        let namespace = generation
            .object(id)
            .ok_or_else(|| Error::internal("catalog namespace disappeared"))?;
        if id == ObjectId::BOOTSTRAP_NAMESPACE {
            if names.len() == 1 {
                names.push(namespace.name().display().as_str().to_owned());
            }
            break;
        }
        names.push(namespace.name().display().as_str().to_owned());
        namespace_id = namespace.namespace_id();
    }
    names.reverse();
    Ok(names.join("."))
}

fn catalog_timestamp(value: u64, fallback: u64) -> Result<chrono::DateTime<chrono::Utc>> {
    let nanos = if value == 0 { fallback } else { value };
    let seconds = i64::try_from(nanos / 1_000_000_000)
        .map_err(|_| Error::internal("catalog timestamp exceeds the runtime clock domain"))?;
    let subsecond = (nanos % 1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, subsecond)
        .ok_or_else(|| Error::internal("catalog timestamp is outside the runtime clock domain"))
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
            ConstraintPayload::ForeignKey { .. } => {
                bind_foreign_key(generation, columns, payload, &mut output.foreign_keys)?
            }
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

fn bind_foreign_key(
    generation: &CatalogGeneration,
    columns: &BTreeMap<ObjectId, RuntimeColumn>,
    payload: &ConstraintPayload,
    foreign_keys: &mut Vec<ForeignKeyConstraint>,
) -> Result<SchemaConstraintKind> {
    let ConstraintPayload::ForeignKey {
        local_column_ids,
        referenced_table_id,
        referenced_column_ids,
        on_update_action,
        on_delete_action,
        ..
    } = payload
    else {
        return Err(Error::internal("catalog constraint is not a foreign key"));
    };
    if local_column_ids.len() != 1 || referenced_column_ids.len() != 1 {
        return Err(Error::NotSupported(
            "composite foreign-key catalog recovery is not implemented".to_owned(),
        ));
    }
    let local = columns
        .get(&local_column_ids[0])
        .ok_or_else(|| Error::internal("foreign-key local column disappeared"))?;
    let referenced_table = generation
        .object(*referenced_table_id)
        .ok_or_else(|| Error::internal("foreign-key target table disappeared"))?;
    let referenced_column = generation
        .object(referenced_column_ids[0])
        .ok_or_else(|| Error::internal("foreign-key target column disappeared"))?;
    let on_update = bind_foreign_key_action(*on_update_action)?;
    let on_delete = bind_foreign_key_action(*on_delete_action)?;
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
    plugin_registry: Option<&PluginRegistry>,
) -> Result<Option<IndexDefinition>> {
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
    let key_encoder = bind_index_key_encoder(generation, payload, plugin_registry)?;
    if payload.operator_class_id().is_some() && key_encoder.is_none() {
        // Missing packages keep the database openable for restricted catalog
        // diagnostics. Ordinary access is rejected by plugin admission before
        // this deliberately unavailable index could be selected.
        return Ok(None);
    }
    Ok(Some(IndexDefinition {
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
        key_encoder,
    }))
}

fn bind_index_key_encoder(
    generation: &CatalogGeneration,
    index: &IndexPayload,
    plugin_registry: Option<&PluginRegistry>,
) -> Result<Option<PreparedIndexKeyEncoder>> {
    let Some(operator_class_id) = index.operator_class_id() else {
        return Ok(None);
    };
    let operator_class = generation
        .object(operator_class_id)
        .ok_or_else(|| Error::internal("index operator class disappeared"))?;
    let CatalogPayload::OperatorClass(payload) = operator_class.payload() else {
        return Err(Error::internal("index operator class has wrong payload"));
    };
    let Some(registry) = plugin_registry else {
        return Ok(None);
    };
    let Some(descriptor) = registry.operator_class(&operator_class_id.into_bytes()) else {
        return Ok(None);
    };
    if descriptor.semantic_revision != payload.semantic_revision()
        || descriptor.access_method != payload.access_method().tag()
        || descriptor.key_codec_revision != payload.key_codec_revision()
        || descriptor.fingerprint != *payload.fingerprint()
        || registered_type_ref(descriptor.input_type)? != payload.input_type()
        || registered_type_ref(descriptor.key_type)? != payload.key_type()
    {
        // Runtime accelerators are rebuildable and never an admission
        // authority. Omit stale semantics here; the executor's exact plugin
        // admission check exposes the database in restricted diagnostic mode.
        return Ok(None);
    }
    let physical_data_type = payload.key_type().logical_type();
    let registry = Arc::new(registry.clone());
    PreparedIndexKeyEncoder::new(
        operator_class_id.into_bytes(),
        payload.semantic_revision(),
        payload.key_codec_revision(),
        *payload.fingerprint(),
        physical_data_type,
        move |value| {
            registry
                .encode_operator_class_key(
                    operator_class_id.into_bytes(),
                    value,
                    InvocationLimits::default(),
                )
                .map_err(|error| {
                    Error::InvalidArgument(format!("operator-class key encoding failed: {error}"))
                })
        },
    )
    .map(Some)
}

pub(crate) fn bind_pending_index_semantics(
    generation: &CatalogGeneration,
    index_name: &str,
    plugin_registry: &PluginRegistry,
) -> Result<(IndexType, Option<PreparedIndexKeyEncoder>)> {
    let index = generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, index_name)
        .map_err(|error| Error::InvalidArgument(error.to_string()))?
        .ok_or_else(|| Error::internal("staged catalog index disappeared"))?;
    let CatalogPayload::Index(payload) = index.payload() else {
        return Err(Error::internal("staged catalog index has wrong payload"));
    };
    let index_type = match payload.access_method() {
        AccessMethod::Btree => IndexType::BTree,
        AccessMethod::Hash => IndexType::Hash,
        AccessMethod::Bitmap => IndexType::Bitmap,
        AccessMethod::Hnsw => IndexType::Hnsw,
    };
    Ok((
        index_type,
        bind_index_key_encoder(generation, payload, Some(plugin_registry))?,
    ))
}

fn registered_type_ref(value: RegisteredTypeRef) -> Result<radixdb_catalog::CatalogDataType> {
    match value {
        RegisteredTypeRef::Builtin(tag) => u8::try_from(tag)
            .ok()
            .and_then(DataType::from_u8)
            .and_then(|data_type| radixdb_catalog::CatalogDataType::scalar(data_type).ok())
            .ok_or_else(|| Error::internal("plugin descriptor contains an invalid built-in type")),
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => radixdb_catalog::CatalogDataType::external(
            ObjectId::from_user_bytes(object_id)
                .map_err(|error| Error::internal(error.to_string()))?,
            codec_version,
        )
        .map_err(|error| Error::internal(error.to_string())),
    }
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
        // The catalog index records the durable physical support relationship.
        // Runtime primary-key enforcement remains schema-derived, so recovery
        // must not install the same owner again as a public secondary B-tree.
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
