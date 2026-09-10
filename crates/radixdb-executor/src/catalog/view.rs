use radixdb_catalog::{
    CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName, CatalogObject, CatalogPayload,
    EdgeKind, ObjectId, ObjectKind, ObjectPrecondition, TablePayload, ViewPayload,
};
use radixdb_core::{sha256_digest, Error, Result};
use radixdb_sql::ast::{CreateViewStatement, DropViewStatement, SelectStatement, Statement};

use super::transaction::{catalog_argument, DdlDelta, ObjectIdSource};

const OUTPUT_SIGNATURE_DOMAIN: &[u8] = b"radixdb.view.output.v1\0";

#[derive(Debug, Clone, Copy)]
pub struct ViewCatalog<'generation> {
    generation: &'generation CatalogGeneration,
    view: &'generation CatalogObject,
    payload: &'generation ViewPayload,
}

impl<'generation> ViewCatalog<'generation> {
    pub fn load(generation: &'generation CatalogGeneration, view_name: &str) -> Result<Self> {
        let view = generation
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, view_name)
            .map_err(catalog_argument)?
            .filter(|object| object.kind() == ObjectKind::View)
            .ok_or_else(|| Error::ViewNotFound(view_name.to_owned()))?;
        let CatalogPayload::View(payload) = view.payload() else {
            return Err(Error::internal(
                "catalog admitted a view with a non-view payload",
            ));
        };
        validate_all_views(generation)?;
        Ok(Self {
            generation,
            view,
            payload,
        })
    }

    pub const fn object(&self) -> &'generation CatalogObject {
        self.view
    }

    pub const fn id(&self) -> ObjectId {
        self.view.id()
    }

    pub const fn payload(&self) -> &'generation ViewPayload {
        self.payload
    }

    pub fn dependencies(&self) -> impl ExactSizeIterator<Item = &'generation CatalogObject> + '_ {
        self.payload.dependency_ids().iter().map(|id| {
            self.generation
                .object(*id)
                .expect("validated view dependency ID must resolve")
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BoundViewDefinition {
    canonical_sql: String,
    dependency_ids: Vec<ObjectId>,
    output_signature: [u8; 32],
}

pub(super) fn bind_create_view(
    statement: &CreateViewStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    validate_all_views(generation)?;
    let namespace_id = ObjectId::BOOTSTRAP_NAMESPACE;
    let view_name = statement.view_name.value.as_str();
    let bound = bind_select(&statement.query, generation)?;
    if let Some(existing) = generation
        .find_relation(namespace_id, view_name)
        .map_err(catalog_argument)?
    {
        if statement.if_not_exists && existing.kind() == ObjectKind::View {
            let CatalogPayload::View(payload) = existing.payload() else {
                unreachable!("catalog kind/payload equality was validated")
            };
            let exact = payload.canonical_sql().as_str() == bound.canonical_sql
                && payload.dependency_ids() == bound.dependency_ids
                && payload.output_signature() == &bound.output_signature;
            if exact {
                ViewCatalog::load(generation, view_name)?;
                return Ok(DdlDelta::default());
            }
        }
        return Err(Error::ViewAlreadyExists(view_name.to_owned()));
    }

    let view_id = ids.next(generation)?;
    let payload = ViewPayload::new(
        bound.canonical_sql,
        bound.dependency_ids.clone(),
        bound.output_signature,
    )
    .map_err(catalog_argument)?;
    let view = CatalogObject::new(
        view_id,
        Some(namespace_id),
        Some(namespace_id),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(view_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::View(payload),
    )
    .map_err(catalog_argument)?;
    let mut edges = Vec::with_capacity(bound.dependency_ids.len() + 1);
    edges.push(CatalogEdge::new(
        namespace_id,
        view_id,
        EdgeKind::Contains,
        0,
    ));
    for (ordinal, dependency_id) in bound.dependency_ids.into_iter().enumerate() {
        edges.push(CatalogEdge::new(
            view_id,
            dependency_id,
            EdgeKind::DependsOn,
            u32::try_from(ordinal).map_err(|_| {
                Error::InvalidArgument("view has too many catalog dependencies".to_owned())
            })?,
        ));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(view)],
        edge_additions: edges,
        ..DdlDelta::default()
    })
}

pub(super) fn bind_drop_view(
    statement: &DropViewStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let view_name = statement.view_name.value.as_str();
    let Some(view) = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, view_name)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::ViewNotFound(view_name.to_owned()))
        };
    };
    if view.kind() != ObjectKind::View {
        return Err(Error::ViewNotFound(view_name.to_owned()));
    }
    if let Some(dependent) = generation.graph().dependents(view.id()).next() {
        return Err(Error::InvalidArgument(format!(
            "cannot drop view '{view_name}': catalog object '{}' depends on it",
            dependent.name().display().as_str()
        )));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::drop(
            ObjectPrecondition::new(view.id(), ObjectKind::View, view.definition_revision())
                .map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}

fn bind_persisted_view(
    canonical_sql: &str,
    generation: &CatalogGeneration,
) -> Result<BoundViewDefinition> {
    let statements = radixdb_sql::parse_sql(canonical_sql)
        .map_err(|error| Error::Parse(format!("invalid persisted view SQL: {error}")))?;
    let [Statement::Select(select)] = statements.as_slice() else {
        return Err(Error::InvalidArgument(
            "persisted view definition must contain exactly one SELECT".to_owned(),
        ));
    };
    let rebound = bind_select(select, generation)?;
    if rebound.canonical_sql != canonical_sql {
        return Err(Error::InvalidArgument(
            "persisted view SQL is not canonical".to_owned(),
        ));
    }
    Ok(rebound)
}

pub(super) fn validate_all_views(generation: &CatalogGeneration) -> Result<()> {
    for view in generation.objects_of_kind(ObjectKind::View) {
        let CatalogPayload::View(payload) = view.payload() else {
            return Err(Error::internal(
                "catalog admitted a view with a non-view payload",
            ));
        };
        let rebound = bind_persisted_view(payload.canonical_sql().as_str(), generation)?;
        if rebound.dependency_ids != payload.dependency_ids() {
            return Err(Error::InvalidArgument(format!(
                "view '{}' dependency IDs do not match rebound canonical SQL",
                view.name().display().as_str()
            )));
        }
        if rebound.output_signature != *payload.output_signature() {
            return Err(Error::InvalidArgument(format!(
                "view '{}' output signature does not match rebound canonical SQL",
                view.name().display().as_str()
            )));
        }
    }
    Ok(())
}

fn bind_select(
    select: &SelectStatement,
    generation: &CatalogGeneration,
) -> Result<BoundViewDefinition> {
    let canonical_sql = select.to_string();
    let dependency_names = crate::mutation::view_binding::bind_from_select(select);
    let mut dependencies = dependency_names
        .into_iter()
        .map(|name| {
            generation
                .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, &name)
                .map_err(catalog_argument)?
                .filter(|object| matches!(object.kind(), ObjectKind::Table | ObjectKind::View))
                .ok_or(Error::TableOrViewNotFound(name))
        })
        .collect::<Result<Vec<_>>>()?;
    dependencies.sort_unstable_by_key(|object| object.id());
    let dependency_ids = dependencies.iter().map(|object| object.id()).collect();
    let output_signature = compute_output_signature(&canonical_sql, &dependencies, generation)?;
    Ok(BoundViewDefinition {
        canonical_sql,
        dependency_ids,
        output_signature,
    })
}

fn compute_output_signature(
    canonical_sql: &str,
    dependencies: &[&CatalogObject],
    generation: &CatalogGeneration,
) -> Result<[u8; 32]> {
    let mut input = Vec::new();
    input.extend_from_slice(OUTPUT_SIGNATURE_DOMAIN);
    append_bytes(&mut input, canonical_sql.as_bytes());
    append_u64(&mut input, dependencies.len())?;
    for dependency in dependencies {
        input.extend_from_slice(dependency.id().as_bytes());
        input.extend_from_slice(&dependency.kind().tag().to_le_bytes());
        append_bytes(
            &mut input,
            dependency.name().normalized().as_str().as_bytes(),
        );
        match dependency.payload() {
            CatalogPayload::Table(payload) => {
                append_table_shape(&mut input, payload, generation)?;
            }
            CatalogPayload::View(payload) => {
                input.extend_from_slice(payload.output_signature());
            }
            _ => {
                return Err(Error::internal(
                    "view dependency is neither a table nor a view",
                ))
            }
        }
    }
    Ok(sha256_digest(&input))
}

fn append_table_shape(
    output: &mut Vec<u8>,
    table: &TablePayload,
    generation: &CatalogGeneration,
) -> Result<()> {
    append_u64(output, table.column_ids().len())?;
    for column_id in table.column_ids() {
        let column = generation
            .object(*column_id)
            .ok_or_else(|| Error::internal("table shape references a missing column"))?;
        let CatalogPayload::Column(payload) = column.payload() else {
            return Err(Error::internal(
                "table shape references a non-column payload",
            ));
        };
        output.extend_from_slice(column.id().as_bytes());
        append_bytes(output, column.name().normalized().as_str().as_bytes());
        output.push(payload.data_type().logical_type().as_u8());
        output.extend_from_slice(&payload.data_type().parameter_1().to_le_bytes());
        output.extend_from_slice(&payload.data_type().parameter_2().to_le_bytes());
        output.push(u8::from(payload.nullable()));
    }
    Ok(())
}

fn append_bytes(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

fn append_u64(output: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value)
        .map_err(|_| Error::InvalidArgument("view shape exceeds u64".to_owned()))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}
