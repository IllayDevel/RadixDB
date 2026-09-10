use std::collections::BTreeSet;

use radixdb_catalog::{
    AccessMethod, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName, CatalogObject,
    CatalogPayload, ConstraintPayload, EdgeKind, HnswDistanceMetric, HnswParameters, IndexPayload,
    ObjectId, ObjectKind, ObjectPrecondition, TablePayload,
};
use radixdb_core::{DataType, Error, Result, Row, Schema, SchemaBuilder, Value};
use radixdb_sql::ast::{
    AlterIndexStatement, CreateIndexStatement, DropIndexStatement, Expression, IndexMethod,
};

use crate::expression::ExpressionEval;

use super::constraints::BoundColumn;
use super::procedural::resolve_object_scope;
use super::table::require_table;
use super::transaction::{catalog_argument, DdlDelta, ObjectIdSource};

#[derive(Debug, Default)]
pub(super) struct BoundIndexes {
    pub objects: Vec<CatalogObject>,
    pub edges: Vec<CatalogEdge>,
    pub ids: Vec<ObjectId>,
}

pub(super) fn bind_constraint_indexes(
    table_id: ObjectId,
    constraints: &[CatalogObject],
    columns: &[BoundColumn],
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<BoundIndexes> {
    let mut output = BoundIndexes::default();
    let mut local_names = BTreeSet::new();
    for constraint in constraints {
        let (key_column_ids, base_name) = match constraint.payload() {
            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) => (
                local_column_ids.clone(),
                format!("{}_idx", constraint.name().normalized().as_str()),
            ),
            CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => (
                local_column_ids.clone(),
                constraint.name().display().as_str().to_owned(),
            ),
            _ => continue,
        };
        let index_id = ids.next(generation)?;
        let name = allocate_index_name(base_name, index_id, generation, &mut local_names)?;
        let payload = IndexPayload::new(
            constraint_index_access_method(&key_column_ids, columns)?,
            true,
            key_column_ids,
            vec![],
            None,
            None,
        )
        .map_err(catalog_argument)?;
        output.objects.push(
            CatalogObject::new(
                index_id,
                Some(ObjectId::BOOTSTRAP_NAMESPACE),
                Some(table_id),
                ObjectId::BOOTSTRAP_OWNER,
                name,
                1,
                CatalogPayload::Index(payload),
            )
            .map_err(catalog_argument)?,
        );
        output.ids.push(index_id);
        output.edges.push(CatalogEdge::new(
            table_id,
            index_id,
            EdgeKind::Contains,
            u32::try_from(output.ids.len() - 1)
                .map_err(|_| Error::InvalidArgument("too many indexes on table".to_owned()))?,
        ));
        output.edges.push(CatalogEdge::new(
            index_id,
            constraint.id(),
            EdgeKind::DependsOn,
            0,
        ));
    }
    Ok(output)
}

fn constraint_index_access_method(
    key_column_ids: &[ObjectId],
    columns: &[BoundColumn],
) -> Result<AccessMethod> {
    if key_column_ids.len() != 1 {
        return Ok(AccessMethod::Btree);
    }
    let column = columns
        .iter()
        .find(|column| column.id == key_column_ids[0])
        .ok_or_else(|| Error::internal("constraint index column disappeared"))?;
    if column.data_type.is_external() {
        return Err(Error::NotSupported(
            "external PRIMARY KEY/UNIQUE columns require a bound operator class".to_owned(),
        ));
    }
    Ok(match column.data_type.logical_type() {
        DataType::Text | DataType::Json | DataType::Bytes => AccessMethod::Hash,
        DataType::Boolean => AccessMethod::Bitmap,
        DataType::Vector => AccessMethod::Hnsw,
        _ => AccessMethod::Btree,
    })
}

pub(super) fn bind_create_index(
    statement: &CreateIndexStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    let table = require_table(generation, statement.table_name.value.as_str())?;
    let table_payload = table_payload(table)?;
    let (payload, dependencies) = bind_index_payload(statement, generation, table)?;
    let namespace_id = ObjectId::BOOTSTRAP_NAMESPACE;
    let index_name = statement.index_name.value.as_str();
    if let Some(existing) = generation
        .find_index(namespace_id, index_name)
        .map_err(catalog_argument)?
    {
        if statement.if_not_exists
            && existing.parent_id() == Some(table.id())
            && existing.payload() == &CatalogPayload::Index(payload)
        {
            return Ok(DdlDelta::default());
        }
        return Err(Error::IndexAlreadyExists(index_name.to_owned()));
    }

    let index_id = ids.next(generation)?;
    let index = CatalogObject::new(
        index_id,
        Some(namespace_id),
        Some(table.id()),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(index_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::Index(payload),
    )
    .map_err(catalog_argument)?;
    let mut index_ids = table_payload.index_ids().to_vec();
    index_ids.push(index_id);
    let replacement = replace_table_indexes(table, table_payload, index_ids)?;
    let ordinal = u32::try_from(table_payload.index_ids().len())
        .map_err(|_| Error::InvalidArgument("too many indexes on table".to_owned()))?;
    Ok(DdlDelta {
        mutations: vec![
            CatalogMutation::alter(table_precondition(table)?, replacement),
            CatalogMutation::create(index),
        ],
        edge_additions: std::iter::once(CatalogEdge::new(
            table.id(),
            index_id,
            EdgeKind::Contains,
            ordinal,
        ))
        .chain(
            dependencies
                .into_iter()
                .enumerate()
                .map(|(ordinal, target)| {
                    CatalogEdge::new(index_id, target, EdgeKind::DependsOn, ordinal as u32)
                }),
        )
        .collect(),
        ..DdlDelta::default()
    })
}

pub(super) fn bind_drop_index(
    statement: &DropIndexStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let index_name = statement.index_name.value.as_str();
    let Some(index) = generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, index_name)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::IndexNotFound(index_name.to_owned()))
        };
    };
    let table_id = index
        .parent_id()
        .ok_or_else(|| Error::internal("catalog index has no owning table"))?;
    let table = generation
        .object(table_id)
        .ok_or_else(|| Error::internal("catalog index owner is absent"))?;
    if let Some(expected_table) = &statement.table_name {
        if !table
            .name()
            .display()
            .as_str()
            .eq_ignore_ascii_case(expected_table.value.as_str())
        {
            return if statement.if_exists {
                Ok(DdlDelta::default())
            } else {
                Err(Error::IndexNotFound(index_name.to_owned()))
            };
        }
    }
    if generation
        .graph()
        .outgoing_edges(index.id())
        .filter(|edge| edge.kind() == EdgeKind::DependsOn)
        .any(|edge| {
            generation
                .object(edge.target_object_id())
                .is_some_and(|target| target.kind() == ObjectKind::Constraint)
        })
    {
        return Err(Error::InvalidArgument(format!(
            "cannot drop constraint-owned index '{index_name}' independently"
        )));
    }
    let payload = table_payload(table)?;
    let index_ids = payload
        .index_ids()
        .iter()
        .copied()
        .filter(|id| *id != index.id())
        .collect();
    Ok(DdlDelta {
        mutations: vec![
            CatalogMutation::alter(
                table_precondition(table)?,
                replace_table_indexes(table, payload, index_ids)?,
            ),
            CatalogMutation::drop(
                ObjectPrecondition::new(index.id(), ObjectKind::Index, index.definition_revision())
                    .map_err(catalog_argument)?,
            ),
        ],
        ..DdlDelta::default()
    })
}

pub(super) fn bind_alter_index(
    statement: &AlterIndexStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let old_name = statement.index_name.value.as_str();
    let index = generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, old_name)
        .map_err(catalog_argument)?
        .ok_or_else(|| Error::IndexNotFound(old_name.to_owned()))?;
    let new_name = statement.new_index_name.value.as_str();
    if generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, new_name)
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::IndexAlreadyExists(new_name.to_owned()));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::rename(
            ObjectPrecondition::new(index.id(), ObjectKind::Index, index.definition_revision())
                .map_err(catalog_argument)?,
            CatalogName::new(new_name).map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}

fn bind_index_payload(
    statement: &CreateIndexStatement,
    generation: &CatalogGeneration,
    table: &CatalogObject,
) -> Result<(IndexPayload, Vec<ObjectId>)> {
    if statement.columns.is_empty() {
        return Err(Error::InvalidArgument(
            "index must reference at least one column".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut key_column_ids = Vec::with_capacity(statement.columns.len());
    for name in &statement.columns {
        let column = generation
            .find_column(table.id(), name.value.as_str())
            .map_err(catalog_argument)?
            .ok_or_else(|| Error::ColumnNotFound(name.value.to_string()))?;
        if !seen.insert(column.id()) {
            return Err(Error::InvalidArgument(format!(
                "index references column '{}' more than once",
                name.value
            )));
        }
        let CatalogPayload::Column(_) = column.payload() else {
            return Err(Error::internal(
                "resolved catalog index key is not a column",
            ));
        };
        key_column_ids.push(column.id());
    }

    let key_type = if key_column_ids.len() == 1 {
        let column = generation
            .object(key_column_ids[0])
            .expect("bound column remains present");
        let CatalogPayload::Column(payload) = column.payload() else {
            unreachable!()
        };
        Some(payload.data_type())
    } else {
        None
    };
    let operator_class = match (&statement.operator_class, key_type) {
        (Some(name), Some(key_type)) if key_type.is_external() => {
            let (namespace, name) = resolve_object_scope(generation, name)?;
            let object = generation
                .find_operator_class(namespace, name)
                .map_err(catalog_argument)?
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("operator class '{}' does not exist", name))
                })?;
            let CatalogPayload::OperatorClass(payload) = object.payload() else {
                unreachable!()
            };
            if payload.input_type() != key_type {
                return Err(Error::InvalidArgument(
                    "operator class input type differs from index key type".to_owned(),
                ));
            }
            Some((object.id(), payload.access_method()))
        }
        (None, Some(key_type)) if key_type.is_external() => {
            return Err(Error::NotSupported(
                "external index keys require an explicit bound operator class".to_owned(),
            ));
        }
        (Some(_), _) => {
            return Err(Error::InvalidArgument(
                "operator class can be used only with one external index key".to_owned(),
            ));
        }
        _ => None,
    };
    let method = operator_class
        .map(|(_, method)| method)
        .unwrap_or(resolve_access_method(
            statement,
            generation,
            &key_column_ids,
        )?);
    if let Some(explicit) = statement.index_method {
        let explicit = match explicit {
            IndexMethod::BTree => AccessMethod::Btree,
            IndexMethod::Hash => AccessMethod::Hash,
            IndexMethod::Bitmap => AccessMethod::Bitmap,
            IndexMethod::Hnsw => AccessMethod::Hnsw,
        };
        if explicit != method {
            return Err(Error::InvalidArgument(
                "CREATE INDEX access method differs from its operator class".to_owned(),
            ));
        }
    }
    let predicate_sql = if let Some(predicate) = &statement.where_clause {
        if operator_class.is_some() {
            return Err(Error::NotSupported(
                "partial indexes over external operator classes are not supported in v1.2"
                    .to_owned(),
            ));
        }
        if method == AccessMethod::Hnsw {
            return Err(Error::InvalidArgument(
                "partial HNSW indexes are not supported".to_owned(),
            ));
        }
        let schema = schema_for_table(generation, table)?;
        Some(
            crate::mutation::partial_index::bind_from_ast(predicate, &schema)?
                .canonical_sql()
                .to_owned(),
        )
    } else {
        None
    };
    let hnsw_parameters = if method == AccessMethod::Hnsw {
        if statement.is_unique {
            return Err(Error::InvalidArgument(
                "HNSW indexes cannot be UNIQUE".to_owned(),
            ));
        }
        if key_column_ids.len() != 1 {
            return Err(Error::InvalidArgument(
                "HNSW index must reference exactly one column".to_owned(),
            ));
        }
        let column = generation
            .object(key_column_ids[0])
            .expect("resolved catalog index column must remain present");
        let CatalogPayload::Column(column_payload) = column.payload() else {
            return Err(Error::internal("catalog index key is not a column"));
        };
        if column_payload.data_type().logical_type() != DataType::Vector {
            return Err(Error::InvalidArgument(
                "HNSW index key must be a VECTOR column".to_owned(),
            ));
        }
        Some(bind_hnsw_parameters(
            &statement.options,
            column_payload.data_type().parameter_1() as usize,
        )?)
    } else {
        if !statement.options.is_empty() {
            return Err(Error::InvalidArgument(
                "CREATE INDEX WITH options are supported only for HNSW indexes".to_owned(),
            ));
        }
        None
    };
    if let Some((operator_class_id, _)) = operator_class {
        let type_id = key_type
            .and_then(|value| value.type_object_id())
            .expect("external index key has type object identity");
        let mut dependencies = vec![operator_class_id, type_id];
        dependencies.sort_unstable();
        return IndexPayload::new_external(
            method,
            statement.is_unique,
            key_column_ids[0],
            predicate_sql,
            operator_class_id,
        )
        .map(|payload| (payload, dependencies))
        .map_err(catalog_argument);
    }
    IndexPayload::from_fields(
        radixdb_catalog::PAYLOAD_VERSION,
        0,
        method,
        statement.is_unique,
        key_column_ids,
        vec![],
        None,
        predicate_sql,
        hnsw_parameters,
        None,
    )
    .map(|payload| (payload, Vec::new()))
    .map_err(catalog_argument)
}

fn resolve_access_method(
    statement: &CreateIndexStatement,
    generation: &CatalogGeneration,
    keys: &[ObjectId],
) -> Result<AccessMethod> {
    if let Some(method) = statement.index_method {
        return Ok(match method {
            IndexMethod::BTree => AccessMethod::Btree,
            IndexMethod::Hash => AccessMethod::Hash,
            IndexMethod::Bitmap => AccessMethod::Bitmap,
            IndexMethod::Hnsw => AccessMethod::Hnsw,
        });
    }
    if keys.len() != 1 {
        return Ok(AccessMethod::Btree);
    }
    let column = generation
        .object(keys[0])
        .ok_or_else(|| Error::internal("resolved catalog index key disappeared"))?;
    let CatalogPayload::Column(payload) = column.payload() else {
        return Err(Error::internal(
            "resolved catalog index key is not a column",
        ));
    };
    Ok(match payload.data_type().logical_type() {
        DataType::Text | DataType::Json | DataType::Bytes => AccessMethod::Hash,
        DataType::Boolean => AccessMethod::Bitmap,
        DataType::Vector => AccessMethod::Hnsw,
        _ => AccessMethod::Btree,
    })
}

fn bind_hnsw_parameters(
    options: &[(String, Expression)],
    dimensions: usize,
) -> Result<HnswParameters> {
    let mut seen = BTreeSet::new();
    let mut m = None;
    let mut ef_construction = None;
    let mut ef_search = None;
    let mut metric = None;
    for (name, expression) in options {
        if !seen.insert(name.as_str()) {
            return Err(Error::InvalidArgument(format!(
                "HNSW option '{name}' is specified more than once"
            )));
        }
        let value = constant_option_value(expression)?;
        match name.as_str() {
            "m" => m = Some(parse_u16_option(name, &value)?),
            "ef_construction" => ef_construction = Some(parse_u16_option(name, &value)?),
            "ef_search" => ef_search = Some(parse_u16_option(name, &value)?),
            "metric" | "distance" => {
                metric = Some(match value.to_ascii_lowercase().as_str() {
                    "cosine" => HnswDistanceMetric::Cosine,
                    "l2" => HnswDistanceMetric::L2,
                    "ip" | "dot" => HnswDistanceMetric::Dot,
                    _ => {
                        return Err(Error::InvalidArgument(format!(
                            "unknown HNSW distance metric '{value}' (expected l2, cosine, or ip)"
                        )));
                    }
                });
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "unknown HNSW index option '{name}'"
                )));
            }
        }
    }
    let m = m.unwrap_or_else(|| radixdb_storage::index::default_m_for_dims(dimensions) as u16);
    let ef_construction = ef_construction
        .unwrap_or_else(|| radixdb_storage::index::default_ef_construction(m as usize) as u16);
    let ef_search =
        ef_search.unwrap_or_else(|| radixdb_storage::index::default_ef_search(m as usize) as u16);
    HnswParameters::new(
        m,
        ef_construction,
        ef_search,
        metric.unwrap_or(HnswDistanceMetric::L2),
    )
    .map_err(catalog_argument)
}

fn constant_option_value(expression: &Expression) -> Result<String> {
    if let Expression::Identifier(identifier) = expression {
        return Ok(identifier.value_lower.to_string());
    }
    let value = ExpressionEval::compile(expression, &[])?.eval_slice(&Row::new())?;
    match value {
        Value::Integer(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        Value::Text(value) => Ok(value.to_string()),
        Value::Boolean(value) => Ok(value.to_string()),
        other => Err(Error::InvalidArgument(format!(
            "index option must be a finite scalar value, got {other:?}"
        ))),
    }
}

fn parse_u16_option(name: &str, value: &str) -> Result<u16> {
    value.parse::<u16>().map_err(|_| {
        Error::InvalidArgument(format!("invalid value for HNSW option '{name}': '{value}'"))
    })
}

fn schema_for_table(generation: &CatalogGeneration, table: &CatalogObject) -> Result<Schema> {
    let payload = table_payload(table)?;
    let mut builder = SchemaBuilder::new(table.name().display().as_str());
    for column_id in payload.column_ids() {
        let column = generation
            .object(*column_id)
            .ok_or_else(|| Error::internal("catalog table column disappeared"))?;
        let CatalogPayload::Column(payload) = column.payload() else {
            return Err(Error::internal("catalog table column has wrong payload"));
        };
        builder = builder.add_with_constraints(
            column.name().display().as_str(),
            payload.data_type().logical_type(),
            payload.nullable(),
            false,
            payload.auto_increment(),
            payload.default_sql().map(|sql| sql.as_str().to_owned()),
            None,
        );
        if payload.data_type().logical_type() == DataType::Vector {
            builder = builder.set_last_vector_dimensions(payload.data_type().parameter_1() as u16);
        }
        if payload.data_type().logical_type() == DataType::Decimal {
            builder = builder.set_last_decimal_parameters(
                payload.data_type().parameter_1() as u8,
                payload.data_type().parameter_2() as u8,
            );
        }
    }
    Ok(builder.build())
}

fn replace_table_indexes(
    table: &CatalogObject,
    payload: &TablePayload,
    index_ids: Vec<ObjectId>,
) -> Result<CatalogObject> {
    let revision = table
        .definition_revision()
        .checked_add(1)
        .ok_or_else(|| Error::internal("catalog table revision overflow"))?;
    CatalogObject::new(
        table.id(),
        table.namespace_id(),
        table.parent_id(),
        table.owner_principal_id(),
        table.name().clone(),
        revision,
        CatalogPayload::Table(
            TablePayload::new_with_timestamps(
                payload.column_ids().to_vec(),
                payload.constraint_ids().to_vec(),
                index_ids,
                payload.primary_key_constraint_id(),
                payload.created_unix_ns(),
                if payload.created_unix_ns() == 0 {
                    0
                } else {
                    catalog_unix_time_nanos().max(payload.created_unix_ns())
                },
            )
            .map_err(catalog_argument)?,
        ),
    )
    .map_err(catalog_argument)
}

fn catalog_unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn table_precondition(table: &CatalogObject) -> Result<ObjectPrecondition> {
    ObjectPrecondition::new(table.id(), ObjectKind::Table, table.definition_revision())
        .map_err(catalog_argument)
}

fn table_payload(table: &CatalogObject) -> Result<&TablePayload> {
    let CatalogPayload::Table(payload) = table.payload() else {
        return Err(Error::internal("catalog table has a non-table payload"));
    };
    Ok(payload)
}

fn allocate_index_name(
    base_name: String,
    id: ObjectId,
    generation: &CatalogGeneration,
    local_names: &mut BTreeSet<String>,
) -> Result<CatalogName> {
    if let Ok(candidate) = CatalogName::new(base_name) {
        let normalized = candidate.normalized().as_str();
        if !local_names.contains(normalized)
            && generation
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, normalized)
                .map_err(catalog_argument)?
                .is_none()
        {
            local_names.insert(normalized.to_owned());
            return Ok(candidate);
        }
    }
    let fallback = CatalogName::new(format!("index_{id}")).map_err(catalog_argument)?;
    if !local_names.insert(fallback.normalized().as_str().to_owned()) {
        return Err(Error::internal(
            "fresh catalog index identities produced the same fallback name",
        ));
    }
    Ok(fallback)
}
