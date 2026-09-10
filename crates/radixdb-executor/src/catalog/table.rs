use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    CatalogDataType, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName, CatalogObject,
    CatalogPayload, ColumnPayload, ConstraintPayload, EdgeKind, ObjectId, ObjectKind,
    ObjectPrecondition, TablePayload,
};
use radixdb_core::{DataType, Error, Result};
use radixdb_sql::ast::{AlterTableOperation, AlterTableStatement, CreateTableStatement};

use crate::binding::types::parse_data_type;

use super::constraints::{bind_constraints, BoundColumn};
use super::index::bind_constraint_indexes;
use super::transaction::{catalog_argument, DdlDelta, ObjectIdSource};

pub(super) fn bind_create_table(
    statement: &CreateTableStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    let namespace_id = ObjectId::BOOTSTRAP_NAMESPACE;
    let table_name = statement.table_name.value.as_str();
    if let Some(existing) = generation
        .find_relation(namespace_id, table_name)
        .map_err(catalog_argument)?
    {
        if statement.if_not_exists && existing.kind() == ObjectKind::Table {
            return Ok(DdlDelta::default());
        }
        return Err(Error::TableAlreadyExists(table_name.to_owned()));
    }
    if statement.as_select.is_some() {
        return Err(Error::NotSupported(
            "CREATE TABLE AS SELECT is outside the CA-30.1 catalog-only adapter".to_owned(),
        ));
    }
    if statement.columns.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "table '{table_name}' must declare at least one column"
        )));
    }
    let table_id = ids.next(generation)?;
    let mut seen_names = BTreeSet::new();
    let mut columns = Vec::with_capacity(statement.columns.len());
    for definition in &statement.columns {
        let column_id = ids.next(generation)?;
        let column = BoundColumn::bind(
            definition,
            column_id,
            bind_catalog_type_in_generation(definition.data_type.as_str(), generation)?,
        )?;
        if !seen_names.insert(column.name.normalized().as_str().to_owned()) {
            return Err(Error::DuplicateColumn);
        }
        columns.push(column);
    }
    let constraints = bind_constraints(statement, table_id, &columns, generation, ids)?;
    let indexes =
        bind_constraint_indexes(table_id, &constraints.objects, &columns, generation, ids)?;

    let mut column_objects = Vec::with_capacity(columns.len());
    let mut edges =
        Vec::with_capacity(columns.len() + constraints.edges.len() + indexes.edges.len() + 1);
    for (ordinal, column) in columns.iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| {
            Error::InvalidArgument(
                "table has more columns than the catalog can represent".to_owned(),
            )
        })?;
        let payload = ColumnPayload::new_with_auto_increment(
            ordinal,
            column.data_type,
            column.is_nullable(&constraints.primary_key_columns),
            column.auto_increment,
            column.default_sql.clone(),
            None,
        )
        .map_err(catalog_argument)?;
        column_objects.push(
            CatalogObject::new(
                column.id,
                Some(namespace_id),
                Some(table_id),
                ObjectId::BOOTSTRAP_OWNER,
                column.name.clone(),
                1,
                CatalogPayload::Column(payload),
            )
            .map_err(catalog_argument)?,
        );
        edges.push(CatalogEdge::new(
            table_id,
            column.id,
            EdgeKind::Contains,
            ordinal,
        ));
        if let Some(type_id) = column.data_type.type_object_id() {
            edges.push(CatalogEdge::new(column.id, type_id, EdgeKind::DependsOn, 0));
        }
    }

    let column_ids = columns.iter().map(|column| column.id).collect();
    let created_unix_ns = catalog_unix_time_nanos();
    let table_payload = TablePayload::new_with_timestamps(
        column_ids,
        constraints.ids,
        indexes.ids,
        constraints.primary_key_id,
        created_unix_ns,
        created_unix_ns,
    )
    .map_err(catalog_argument)?;
    let table = CatalogObject::new(
        table_id,
        Some(namespace_id),
        Some(namespace_id),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(table_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::Table(table_payload),
    )
    .map_err(catalog_argument)?;
    edges.push(CatalogEdge::new(
        namespace_id,
        table_id,
        EdgeKind::Contains,
        0,
    ));
    edges.extend(constraints.edges);
    edges.extend(indexes.edges);
    let mut mutations = Vec::with_capacity(
        column_objects.len() + constraints.objects.len() + indexes.objects.len() + 1,
    );
    mutations.push(CatalogMutation::create(table));
    mutations.extend(column_objects.into_iter().map(CatalogMutation::create));
    mutations.extend(constraints.objects.into_iter().map(CatalogMutation::create));
    mutations.extend(indexes.objects.into_iter().map(CatalogMutation::create));
    Ok(DdlDelta {
        mutations,
        edge_additions: edges,
        ..DdlDelta::default()
    })
}

pub(super) fn bind_drop_table(
    table_name: &str,
    if_exists: bool,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let namespace_id = ObjectId::BOOTSTRAP_NAMESPACE;
    let Some(table) = generation
        .find_relation(namespace_id, table_name)
        .map_err(catalog_argument)?
    else {
        return if if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::TableNotFound(table_name.to_owned()))
        };
    };
    if table.kind() != ObjectKind::Table {
        return Err(Error::TableNotFound(table_name.to_owned()));
    }
    let mut mutations = generation
        .graph()
        .children(table.id())
        .map(|child| {
            ObjectPrecondition::new(child.id(), child.kind(), child.definition_revision())
                .map(CatalogMutation::drop)
                .map_err(catalog_argument)
        })
        .collect::<Result<Vec<_>>>()?;
    mutations.extend(drop_external_foreign_keys(table_name, table, generation)?);
    mutations.push(CatalogMutation::drop(
        ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
            .map_err(catalog_argument)?,
    ));
    Ok(DdlDelta {
        mutations,
        ..DdlDelta::default()
    })
}

/// Remove foreign-key metadata in surviving child tables when their referenced
/// table is dropped. Row-level DROP safety is checked by the mutation owner
/// before catalog staging; the logical catalog must then remove the exact FK
/// objects atomically with the table instead of retaining dangling references.
fn drop_external_foreign_keys(
    table_name: &str,
    table: &CatalogObject,
    generation: &CatalogGeneration,
) -> Result<Vec<CatalogMutation>> {
    let mut by_child = BTreeMap::<ObjectId, BTreeSet<ObjectId>>::new();
    for dependent in generation.graph().dependents(table.id()) {
        // A self-referencing constraint is already a child of the table and is
        // removed with the rest of that table's owned objects.
        if dependent.parent_id() == Some(table.id()) {
            continue;
        }
        match dependent.payload() {
            CatalogPayload::Constraint(ConstraintPayload::ForeignKey {
                referenced_table_id,
                ..
            }) if *referenced_table_id == table.id() => {
                let child_id = dependent.parent_id().ok_or_else(|| {
                    Error::internal("foreign-key catalog object has no owning table")
                })?;
                by_child.entry(child_id).or_default().insert(dependent.id());
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "cannot drop table '{table_name}': catalog object '{}' depends on it",
                    dependent.name().display().as_str()
                )));
            }
        }
    }

    let mut mutations = Vec::new();
    for (child_id, dropped_constraints) in by_child {
        let child = generation
            .object(child_id)
            .ok_or_else(|| Error::internal("foreign-key owner table disappeared"))?;
        let CatalogPayload::Table(payload) = child.payload() else {
            return Err(Error::internal(
                "foreign-key catalog object is not owned by a table",
            ));
        };
        if dropped_constraints
            .iter()
            .any(|id| !payload.constraint_ids().contains(id))
        {
            return Err(Error::internal(
                "foreign-key owner table does not list its constraint",
            ));
        }
        let (created_unix_ns, updated_unix_ns) = updated_table_timestamps(payload);
        let replacement_payload = TablePayload::new_with_timestamps(
            payload.column_ids().to_vec(),
            payload
                .constraint_ids()
                .iter()
                .copied()
                .filter(|id| !dropped_constraints.contains(id))
                .collect(),
            payload.index_ids().to_vec(),
            payload.primary_key_constraint_id(),
            created_unix_ns,
            updated_unix_ns,
        )
        .map_err(catalog_argument)?;
        let replacement = CatalogObject::new(
            child.id(),
            child.namespace_id(),
            child.parent_id(),
            child.owner_principal_id(),
            child.name().clone(),
            child
                .definition_revision()
                .checked_add(1)
                .ok_or_else(|| Error::internal("catalog object revision overflow"))?,
            CatalogPayload::Table(replacement_payload),
        )
        .map_err(catalog_argument)?;
        mutations.push(CatalogMutation::alter(
            ObjectPrecondition::new(child.id(), ObjectKind::Table, child.definition_revision())
                .map_err(catalog_argument)?,
            replacement,
        ));
        for constraint_id in dropped_constraints {
            let constraint = generation
                .object(constraint_id)
                .ok_or_else(|| Error::internal("foreign-key catalog object disappeared"))?;
            mutations.push(CatalogMutation::drop(
                ObjectPrecondition::new(
                    constraint.id(),
                    ObjectKind::Constraint,
                    constraint.definition_revision(),
                )
                .map_err(catalog_argument)?,
            ));
        }
    }
    Ok(mutations)
}

fn catalog_unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn updated_table_timestamps(payload: &TablePayload) -> (u64, u64) {
    let created = payload.created_unix_ns();
    if created == 0 {
        (0, 0)
    } else {
        (created, catalog_unix_time_nanos().max(created))
    }
}

pub(super) fn bind_alter_table(
    statement: &AlterTableStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    if statement.operation != AlterTableOperation::RenameTable {
        return Err(Error::NotSupported(format!(
            "ALTER TABLE {:?} belongs to the CA-30.2 catalog vertical",
            statement.operation
        )));
    }
    let table_name = statement.table_name.value.as_str();
    let table = require_table(generation, table_name)?;
    if let Some(dependent) = generation
        .graph()
        .dependents(table.id())
        .find(|object| object.kind() == ObjectKind::View)
    {
        return Err(Error::InvalidArgument(format!(
            "cannot rename table '{table_name}': view '{}' persists SQL that depends on its name",
            dependent.name().display().as_str()
        )));
    }
    let new_name = statement
        .new_table_name
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("RENAME TABLE requires new table name".to_owned()))?;
    if generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, new_name.value.as_str())
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::TableAlreadyExists(new_name.value.to_string()));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::rename(
            ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
                .map_err(catalog_argument)?,
            CatalogName::new(new_name.value.as_str()).map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}

pub(super) fn require_table<'a>(
    generation: &'a CatalogGeneration,
    table_name: &str,
) -> Result<&'a CatalogObject> {
    generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
        .map_err(catalog_argument)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))
}

pub(crate) fn bind_catalog_type(type_name: &str) -> Result<CatalogDataType> {
    let data_type = parse_data_type(type_name)?;
    match data_type {
        DataType::Decimal => bind_decimal(type_name),
        DataType::Vector => bind_vector(type_name),
        _ => CatalogDataType::scalar(data_type).map_err(catalog_argument),
    }
}

pub(crate) fn bind_catalog_type_in_generation(
    type_name: &str,
    generation: &CatalogGeneration,
) -> Result<CatalogDataType> {
    match bind_catalog_type(type_name) {
        Ok(data_type) => Ok(data_type),
        Err(Error::Type(_)) if type_name.contains('.') => {
            let mut components = type_name.split('.').collect::<Vec<_>>();
            let name = components
                .pop()
                .ok_or_else(|| Error::InvalidArgument("external type name is empty".to_owned()))?;
            if name.is_empty() || components.iter().any(|part| part.is_empty()) {
                return Err(Error::InvalidArgument(format!(
                    "invalid external type name '{type_name}'"
                )));
            }
            let namespace = super::resolve_namespace_path(generation, components)?;
            let object = generation
                .find_external_type(namespace, name)
                .map_err(catalog_argument)?
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("external type '{type_name}' does not exist"))
                })?;
            let CatalogPayload::ExternalType(payload) = object.payload() else {
                return Err(Error::internal(
                    "type lookup resolved to another object kind",
                ));
            };
            CatalogDataType::external(object.id(), payload.write_codec_version())
                .map_err(catalog_argument)
        }
        Err(error) => Err(error),
    }
}

fn bind_decimal(type_name: &str) -> Result<CatalogDataType> {
    let upper = type_name.trim().to_ascii_uppercase();
    if matches!(upper.as_str(), "DECIMAL" | "NUMERIC") {
        return CatalogDataType::unconstrained_decimal().map_err(catalog_argument);
    }
    let Some(parameters) = upper
        .strip_prefix("DECIMAL(")
        .or_else(|| upper.strip_prefix("NUMERIC("))
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(Error::InvalidArgument(format!(
            "invalid DECIMAL type declaration '{type_name}'"
        )));
    };
    let parts = parameters.split(',').map(str::trim).collect::<Vec<_>>();
    if !(1..=2).contains(&parts.len()) {
        return Err(Error::InvalidArgument(format!(
            "DECIMAL requires precision and optional scale, got '{parameters}'"
        )));
    }
    let precision = parts[0]
        .parse::<u8>()
        .map_err(|_| Error::InvalidArgument(format!("invalid DECIMAL precision '{}'", parts[0])))?;
    if precision == 0 {
        return Err(Error::InvalidArgument(
            "DECIMAL precision must be inside 1..=38".to_owned(),
        ));
    }
    let scale = parts
        .get(1)
        .map(|value| {
            value
                .parse::<u8>()
                .map_err(|_| Error::InvalidArgument(format!("invalid DECIMAL scale '{value}'")))
        })
        .transpose()?
        .unwrap_or(0);
    CatalogDataType::decimal(precision, scale).map_err(catalog_argument)
}

fn bind_vector(type_name: &str) -> Result<CatalogDataType> {
    let upper = type_name.trim().to_ascii_uppercase();
    let dimensions = upper
        .strip_prefix("VECTOR(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "catalog storage requires VECTOR(dimensions), got '{type_name}'"
            ))
        })?;
    CatalogDataType::vector(dimensions).map_err(catalog_argument)
}
