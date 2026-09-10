//! Fail-closed admission for untrusted, read-only ORM requests.
//!
//! The public boundary is deliberately separate from ordinary embedded SQL.
//! A policy is bound from names to durable catalog identities once, then every
//! execution revalidates those identities and consumes the complete result
//! while holding one immutable catalog fence.

use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    CatalogGeneration, CatalogName, CatalogPayload, ConstraintPayload, ObjectId, ObjectKind,
};
use radixdb_core::{DataType, Error, ParamVec, Result, Row, Value};
use radixdb_sql::{
    walk_expression_tree, walk_statement_physical_table_sources, walk_statement_tree, Expression,
    SelectStatement, SimpleTableSource, Statement,
};

use crate::context::ExecutionContext;
use crate::navigation::{bind_reference_expand_plan, ReferenceExpandPlan};
use crate::procedural::transaction_visible_catalog;
use crate::Executor;

#[cfg(any(test, feature = "test-hooks"))]
type PublicReadFenceTestHook = ([u8; 16], ObjectId, std::sync::Arc<dyn Fn() + Send + Sync>);

#[cfg(any(test, feature = "test-hooks"))]
static PUBLIC_READ_FENCE_TEST_HOOK: std::sync::LazyLock<
    std::sync::Mutex<Option<PublicReadFenceTestHook>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(any(test, feature = "test-hooks"))]
static PUBLIC_READ_FENCE_TEST_HOOK_OWNER: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct PublicReadFenceTestHookGuard {
    _owner: std::sync::MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl PublicReadFenceTestHookGuard {
    pub fn install(
        database_id: [u8; 16],
        relation_id: ObjectId,
        hook: std::sync::Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        let owner = PUBLIC_READ_FENCE_TEST_HOOK_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *PUBLIC_READ_FENCE_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((database_id, relation_id, hook));
        Self { _owner: owner }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PublicReadFenceTestHookGuard {
    fn drop(&mut self) {
        *PUBLIC_READ_FENCE_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn run_public_read_fence_test_hook(
    database_id: [u8; 16],
    accesses: &BTreeMap<ObjectId, BTreeSet<ObjectId>>,
) {
    let hook = PUBLIC_READ_FENCE_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some((expected_database_id, relation_id, hook)) = hook {
        if expected_database_id == database_id && accesses.contains_key(&relation_id) {
            hook();
        }
    }
}

pub const PUBLIC_READ_MAX_PROJECTION: usize = 128;
pub const PUBLIC_READ_MAX_FILTER_NODES: usize = 512;
pub const PUBLIC_READ_MAX_NAVIGATION_DEPTH: usize = 8;
pub const PUBLIC_READ_MAX_JOINS: usize = 8;
pub const PUBLIC_READ_MAX_PAGE_SIZE: usize = 1_000;
pub const PUBLIC_READ_MAX_SCANNED_ROWS: usize = 100_000;
pub const PUBLIC_READ_MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;
pub const PUBLIC_READ_MAX_PARAMETER_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicReadLimits {
    pub max_projection: usize,
    pub max_filter_nodes: usize,
    pub max_navigation_depth: usize,
    pub max_joins: usize,
    pub max_page_size: usize,
    pub max_scanned_rows: usize,
    pub max_result_bytes: usize,
    pub max_parameter_bytes: usize,
}

impl Default for PublicReadLimits {
    fn default() -> Self {
        Self {
            max_projection: 64,
            max_filter_nodes: 256,
            max_navigation_depth: 4,
            max_joins: 4,
            max_page_size: 256,
            max_scanned_rows: 25_000,
            max_result_bytes: 2 * 1024 * 1024,
            max_parameter_bytes: 256 * 1024,
        }
    }
}

impl PublicReadLimits {
    pub fn validate(self) -> Result<Self> {
        validate_limit(
            "projection",
            self.max_projection,
            PUBLIC_READ_MAX_PROJECTION,
        )?;
        validate_limit(
            "filter nodes",
            self.max_filter_nodes,
            PUBLIC_READ_MAX_FILTER_NODES,
        )?;
        validate_limit(
            "navigation depth",
            self.max_navigation_depth,
            PUBLIC_READ_MAX_NAVIGATION_DEPTH,
        )?;
        validate_limit("JOIN count", self.max_joins, PUBLIC_READ_MAX_JOINS)?;
        validate_limit("page size", self.max_page_size, PUBLIC_READ_MAX_PAGE_SIZE)?;
        validate_limit(
            "scanned rows",
            self.max_scanned_rows,
            PUBLIC_READ_MAX_SCANNED_ROWS,
        )?;
        validate_limit(
            "result bytes",
            self.max_result_bytes,
            PUBLIC_READ_MAX_RESULT_BYTES,
        )?;
        validate_limit(
            "parameter bytes",
            self.max_parameter_bytes,
            PUBLIC_READ_MAX_PARAMETER_BYTES,
        )?;
        Ok(self)
    }
}

fn validate_limit(label: &str, value: usize, ceiling: usize) -> Result<()> {
    if value == 0 || value > ceiling {
        return Err(Error::invalid_argument(format!(
            "public read {label} limit {value} is outside 1..={ceiling}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicReadRelationSpec {
    pub name: String,
    pub columns: Vec<String>,
}

impl PublicReadRelationSpec {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicReadColumnBinding {
    pub object_id: ObjectId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicReadRelationBinding {
    pub object_id: ObjectId,
    pub name: String,
    pub definition_revision: u64,
    pub columns: Vec<PublicReadColumnBinding>,
    pub primary_key: Vec<PublicReadColumnBinding>,
}

impl PublicReadRelationBinding {
    fn column(&self, name: &str) -> Option<&PublicReadColumnBinding> {
        let normalized = CatalogName::new(name).ok()?;
        self.columns
            .iter()
            .find(|column| column.name == normalized.normalized().as_str())
    }

    fn admits_column_id(&self, id: ObjectId) -> bool {
        self.columns.iter().any(|column| column.object_id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPublicReadPolicy {
    relations: BTreeMap<String, PublicReadRelationBinding>,
    functions: BTreeSet<String>,
}

impl BoundPublicReadPolicy {
    pub fn relations(&self) -> impl ExactSizeIterator<Item = &PublicReadRelationBinding> {
        self.relations.values()
    }

    pub fn functions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.functions.iter().map(String::as_str)
    }

    pub fn relation(&self, name: &str) -> Option<&PublicReadRelationBinding> {
        normalized_name(name)
            .ok()
            .and_then(|name| self.relations.get(&name))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicReadMaterialized {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub database_id: [u8; 16],
    pub catalog_id: [u8; 16],
    pub catalog_generation: u64,
    pub relations: Vec<PublicReadRelationBinding>,
}

impl Executor {
    /// Bind operator-facing names to durable catalog identities. Only ordinary
    /// tables with an ordered primary key are publishable through this generic
    /// boundary; sensitive row-scoped data remains procedure/view mediated.
    #[doc(hidden)]
    pub fn bind_public_read_policy(
        &self,
        relations: &[PublicReadRelationSpec],
        functions: &[String],
    ) -> Result<BoundPublicReadPolicy> {
        if self.has_active_transaction() {
            return Err(Error::invalid_argument(
                "public read policy cannot be bound inside an explicit transaction",
            ));
        }
        let _fence = self.engine.acquire_ddl_statement_fence(false);
        let (catalog, _) = transaction_visible_catalog(self)?;
        let mut bound = BTreeMap::new();
        for spec in relations {
            let relation = bind_relation(catalog.as_ref(), spec)?;
            if bound.insert(relation.name.clone(), relation).is_some() {
                return Err(Error::invalid_argument(
                    "public read policy contains a duplicate relation",
                ));
            }
        }
        let mut bound_functions = BTreeSet::new();
        for function in functions {
            let name = normalized_name(function)?;
            if !self.function_registry.exists(&name) {
                return Err(Error::invalid_argument(format!(
                    "public read function '{name}' is not a registered builtin"
                )));
            }
            bound_functions.insert(name);
        }
        Ok(BoundPublicReadPolicy {
            relations: bound,
            functions: bound_functions,
        })
    }

    /// Execute one already-rendered ORM SELECT. This seam is hidden so no
    /// transport can expose public raw SQL; the API crate owns IR admission.
    #[doc(hidden)]
    pub fn execute_public_read_sql(
        &self,
        sql: &str,
        params: ParamVec,
        context: &ExecutionContext,
        policy: &BoundPublicReadPolicy,
        limits: PublicReadLimits,
    ) -> Result<PublicReadMaterialized> {
        let limits = limits.validate()?;
        if self.has_active_transaction() {
            return Err(Error::invalid_argument(
                "public read cannot run inside an explicit transaction",
            ));
        }
        if value_bytes(&params) > limits.max_parameter_bytes {
            return Err(Error::invalid_argument(
                "public read parameter-byte budget exceeded",
            ));
        }

        let _fence = self.engine.acquire_ddl_statement_fence(false);
        let (catalog, _) = transaction_visible_catalog(self)?;
        revalidate_policy(catalog.as_ref(), policy)?;

        let mut program = crate::dispatch::program::parse_program(sql)?;
        if program.statements.len() != 1 {
            return Err(public_shape("exactly one SELECT is required"));
        }
        let statement = program
            .statements
            .pop()
            .expect("single public statement was checked");
        let Statement::Select(select) = &statement else {
            return Err(public_shape("only SELECT is admitted"));
        };
        let accesses = validate_public_select(select, policy, limits)?;
        let navigation = bind_reference_expand_plan(self.engine.as_ref(), select)?;
        let accesses =
            bind_navigation_accesses(catalog.as_ref(), policy, accesses, navigation.as_ref())?;
        crate::authorization::authorize_public_read_accesses(catalog.as_ref(), context, &accesses)?;

        #[cfg(any(test, feature = "test-hooks"))]
        run_public_read_fence_test_hook(catalog.meta().database_id(), &accesses);

        let mut owned = self.fork_for_stored_function();
        owned.ddl_fence_already_held = true;
        let mut bounded = context.with_public_scan_limit(limits.max_scanned_rows);
        bounded.set_params(params);
        let mut result = owned.execute_with_context(sql, &bounded)?;
        let columns = result.columns().to_vec();
        let mut rows = Vec::new();
        let mut bytes = columns.iter().map(String::len).sum::<usize>();
        while result.next() {
            if rows.len() == limits.max_page_size.saturating_add(1) {
                let _ = result.close();
                return Err(public_shape("result row limit exceeded"));
            }
            let row = result.take_row();
            bytes = bytes.saturating_add(row_bytes(&row));
            if bytes > limits.max_result_bytes {
                let _ = result.close();
                return Err(Error::invalid_argument(
                    "public read result-byte budget exceeded",
                ));
            }
            rows.push(row);
        }
        if let Some(error) = result.last_error() {
            let _ = result.close();
            return Err(error);
        }
        result.close()?;
        let meta = catalog.meta();
        Ok(PublicReadMaterialized {
            columns,
            rows,
            database_id: meta.database_id(),
            catalog_id: meta.catalog_id(),
            catalog_generation: meta.catalog_generation(),
            relations: accesses
                .keys()
                .filter_map(|id| {
                    policy
                        .relations
                        .values()
                        .find(|relation| relation.object_id == *id)
                        .cloned()
                })
                .collect(),
        })
    }
}

fn bind_relation(
    catalog: &CatalogGeneration,
    spec: &PublicReadRelationSpec,
) -> Result<PublicReadRelationBinding> {
    let name = normalized_name(&spec.name)?;
    let object = catalog
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, &name)
        .map_err(catalog_error)?
        .ok_or_else(|| Error::TableNotFound(spec.name.clone()))?;
    if object.kind() != ObjectKind::Table {
        return Err(public_shape("generic public reads publish tables only"));
    }
    let CatalogPayload::Table(table) = object.payload() else {
        return Err(Error::internal("table object has non-table payload"));
    };
    if spec.columns.is_empty() {
        return Err(public_shape("published relation must expose columns"));
    }
    let mut columns = Vec::with_capacity(spec.columns.len());
    let mut seen = BTreeSet::new();
    for requested in &spec.columns {
        let column = catalog
            .find_column(object.id(), requested)
            .map_err(catalog_error)?
            .ok_or_else(|| Error::ColumnNotFound(requested.clone()))?;
        if !seen.insert(column.id()) {
            return Err(public_shape("published column list contains a duplicate"));
        }
        columns.push(column_binding(column)?);
    }
    let primary_id = table.primary_key_constraint_id().ok_or_else(|| {
        public_shape("generic public read relation requires an ordered primary key")
    })?;
    let primary = catalog
        .object(primary_id)
        .ok_or_else(|| Error::internal("primary-key catalog object is missing"))?;
    let CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) =
        primary.payload()
    else {
        return Err(Error::internal(
            "table primary-key pointer does not reference a primary key",
        ));
    };
    let mut primary_key = Vec::with_capacity(local_column_ids.len());
    for id in local_column_ids {
        let column = columns
            .iter()
            .find(|column| column.object_id == *id)
            .ok_or_else(|| public_shape("every primary-key column must be explicitly published"))?;
        if !column.data_type.is_orderable() {
            return Err(public_shape("public pagination key is not orderable"));
        }
        primary_key.push(column.clone());
    }
    Ok(PublicReadRelationBinding {
        object_id: object.id(),
        name,
        definition_revision: object.definition_revision(),
        columns,
        primary_key,
    })
}

fn column_binding(object: &radixdb_catalog::CatalogObject) -> Result<PublicReadColumnBinding> {
    let CatalogPayload::Column(column) = object.payload() else {
        return Err(Error::internal("column object has non-column payload"));
    };
    Ok(PublicReadColumnBinding {
        object_id: object.id(),
        name: object.name().normalized().as_str().to_owned(),
        data_type: column.data_type().logical_type(),
        nullable: column.nullable(),
    })
}

fn revalidate_policy(catalog: &CatalogGeneration, policy: &BoundPublicReadPolicy) -> Result<()> {
    for relation in policy.relations.values() {
        let current = catalog
            .object(relation.object_id)
            .ok_or_else(|| public_shape("published relation identity is stale or was dropped"))?;
        if current.kind() != ObjectKind::Table
            || current.name().normalized().as_str() != relation.name
            || current.definition_revision() != relation.definition_revision
        {
            return Err(public_shape("published relation identity is stale"));
        }
        for column in relation.columns.iter().chain(&relation.primary_key) {
            let current = catalog
                .object(column.object_id)
                .ok_or_else(|| public_shape("published column identity is stale or was dropped"))?;
            let CatalogPayload::Column(payload) = current.payload() else {
                return Err(public_shape("published column identity changed kind"));
            };
            if current.parent_id() != Some(relation.object_id)
                || current.name().normalized().as_str() != column.name
                || payload.data_type().logical_type() != column.data_type
                || payload.nullable() != column.nullable
            {
                return Err(public_shape("published column identity is stale"));
            }
        }
    }
    Ok(())
}

type BoundAccesses = BTreeMap<ObjectId, BTreeSet<ObjectId>>;

fn validate_public_select(
    select: &SelectStatement,
    policy: &BoundPublicReadPolicy,
    limits: PublicReadLimits,
) -> Result<BoundAccesses> {
    if select.with.is_some()
        || select.distinct
        || !select.distinct_on.is_empty()
        || !select.group_by.columns.is_empty()
        || select.having.is_some()
        || !select.window_defs.is_empty()
        || !select.set_operations.is_empty()
        || select.offset.is_some()
    {
        return Err(public_shape(
            "query feature is outside the public read subset",
        ));
    }
    if select.columns.is_empty() || select.columns.len() > limits.max_projection {
        return Err(public_shape(
            "projection width is outside the admitted limit",
        ));
    }
    let limit = match select.limit.as_deref() {
        Some(Expression::IntegerLiteral(value)) if value.value > 0 => value.value as usize,
        _ => return Err(public_shape("a positive constant LIMIT is required")),
    };
    if limit > limits.max_page_size.saturating_add(1) {
        return Err(public_shape("page size exceeds the admitted limit"));
    }

    let mut sources = Vec::<SimpleTableSource>::new();
    walk_statement_physical_table_sources(&Statement::Select(select.clone()), &mut |source| {
        sources.push(source.clone());
    });
    if sources.is_empty() || sources.len() > limits.max_joins.saturating_add(1) {
        return Err(public_shape(
            "relation/JOIN count is outside the admitted limit",
        ));
    }
    let mut aliases = BTreeMap::<String, &PublicReadRelationBinding>::new();
    for source in &sources {
        if source.as_of.is_some() {
            return Err(public_shape(
                "temporal table sources are not publicly admitted",
            ));
        }
        let relation = policy
            .relation(source.name.value())
            .ok_or_else(|| public_shape("relation is not explicitly published"))?;
        let alias = source.alias.as_ref().map_or_else(
            || source.name.value_lower().to_owned(),
            |alias| alias.value_lower().to_owned(),
        );
        if aliases.insert(alias, relation).is_some() {
            return Err(public_shape("relation alias is ambiguous"));
        }
    }

    let mut filter_nodes = 0usize;
    if let Some(filter) = &select.where_clause {
        walk_expression_tree(filter, &mut |_| {
            filter_nodes = filter_nodes.saturating_add(1)
        });
    }
    if let Some(source) = &select.table_expr {
        count_public_join_predicate_nodes(source, &mut filter_nodes);
    }
    if filter_nodes > limits.max_filter_nodes {
        return Err(public_shape("filter complexity exceeds the admitted limit"));
    }

    let mut error = None;
    let mut joins = 0usize;
    let mut accesses = BoundAccesses::new();
    walk_statement_tree(&Statement::Select(select.clone()), &mut |expression| {
        if error.is_some() {
            return;
        }
        let result = match expression {
            Expression::TableSource(_) => Ok(()),
            Expression::JoinSource(join) => {
                joins = joins.saturating_add(1);
                let kind = join.join_type.to_uppercase();
                if !matches!(kind.as_str(), "INNER" | "CROSS")
                    || !join.using_columns.is_empty()
                    || (kind == "INNER" && join.condition.is_none())
                {
                    Err(public_shape("only INNER/CROSS JOIN ... ON is admitted"))
                } else {
                    Ok(())
                }
            }
            Expression::Identifier(identifier) => {
                if aliases.len() != 1 {
                    Err(public_shape(
                        "unqualified columns require one relation source",
                    ))
                } else {
                    admit_named_column(
                        aliases.values().next().expect("one alias"),
                        identifier.value(),
                        &mut accesses,
                    )
                }
            }
            Expression::QualifiedIdentifier(identifier) => {
                let depth = identifier.component_count().saturating_sub(1);
                if depth > limits.max_navigation_depth {
                    Err(public_shape("navigation depth exceeds the admitted limit"))
                } else if identifier.component_count() == 2 {
                    aliases
                        .get(identifier.qualifier.value_lower())
                        .ok_or_else(|| public_shape("column qualifier is not a relation alias"))
                        .and_then(|relation| {
                            admit_named_column(relation, identifier.name.value(), &mut accesses)
                        })
                } else if aliases.contains_key(identifier.qualifier.value_lower()) {
                    Ok(())
                } else {
                    Err(public_shape("navigation root is not a relation alias"))
                }
            }
            Expression::FunctionCall(function) => {
                let name = function.function.to_lowercase();
                if policy.functions.contains(name.as_str()) {
                    Ok(())
                } else {
                    Err(public_shape("function is not explicitly allowlisted"))
                }
            }
            Expression::Star(_)
            | Expression::QualifiedStar(_)
            | Expression::SubquerySource(_)
            | Expression::ValuesSource(_)
            | Expression::CteReference(_)
            | Expression::FunctionTableSource(_)
            | Expression::Exists(_)
            | Expression::AllAny(_)
            | Expression::ScalarSubquery(_)
            | Expression::Window(_) => Err(public_shape(
                "stars, subqueries, derived sources and windows are not publicly admitted",
            )),
            _ => Ok(()),
        };
        if let Err(value) = result {
            error = Some(value);
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    if joins > limits.max_joins {
        return Err(public_shape("JOIN count exceeds the admitted limit"));
    }
    Ok(accesses)
}

fn count_public_join_predicate_nodes(expression: &Expression, nodes: &mut usize) {
    if let Expression::JoinSource(join) = expression {
        count_public_join_predicate_nodes(&join.left, nodes);
        count_public_join_predicate_nodes(&join.right, nodes);
        if let Some(condition) = &join.condition {
            walk_expression_tree(condition, &mut |_| *nodes = nodes.saturating_add(1));
        }
    }
}

fn admit_named_column(
    relation: &PublicReadRelationBinding,
    name: &str,
    accesses: &mut BoundAccesses,
) -> Result<()> {
    let column = relation
        .column(name)
        .ok_or_else(|| public_shape("column is not explicitly published"))?;
    accesses
        .entry(relation.object_id)
        .or_default()
        .insert(column.object_id);
    Ok(())
}

fn bind_navigation_accesses(
    catalog: &CatalogGeneration,
    policy: &BoundPublicReadPolicy,
    mut accesses: BoundAccesses,
    plan: Option<&ReferenceExpandPlan>,
) -> Result<BoundAccesses> {
    let Some(plan) = plan else {
        return Ok(accesses);
    };
    for edge in plan.edges() {
        for column in std::iter::once(edge.source_column())
            .chain(std::iter::once(edge.target_key_column()))
            .chain(edge.required_columns())
        {
            let table = catalog
                .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, column.table().table_name())
                .map_err(catalog_error)?
                .ok_or_else(|| public_shape("navigation relation is absent from catalog"))?;
            let published = policy
                .relations
                .values()
                .find(|published| published.object_id == table.id())
                .ok_or_else(|| public_shape("navigation target relation is not published"))?;
            let CatalogPayload::Table(table_payload) = table.payload() else {
                return Err(public_shape("navigation target is not a table"));
            };
            let id = *table_payload
                .column_ids()
                .get(column.ordinal())
                .ok_or_else(|| public_shape("navigation column ordinal is stale"))?;
            if !published.admits_column_id(id) {
                return Err(public_shape("navigation column is not published"));
            }
            accesses.entry(table.id()).or_default().insert(id);
        }
    }
    Ok(accesses)
}

fn normalized_name(value: &str) -> Result<String> {
    CatalogName::new(value)
        .map(|name| name.normalized().as_str().to_owned())
        .map_err(catalog_error)
}

fn catalog_error(error: radixdb_catalog::CatalogError) -> Error {
    Error::invalid_argument(format!("catalog binding failed: {error}"))
}

fn public_shape(detail: &str) -> Error {
    Error::invalid_argument(format!("public read rejected: {detail}"))
}

fn value_bytes(values: &[Value]) -> usize {
    values.iter().map(single_value_bytes).sum()
}

fn row_bytes(row: &Row) -> usize {
    row.iter().map(single_value_bytes).sum::<usize>()
        + row.len().saturating_mul(std::mem::size_of::<Value>())
}

fn single_value_bytes(value: &Value) -> usize {
    match value {
        Value::Text(value) => value.len(),
        Value::Extension(value) => value.len(),
        _ => std::mem::size_of::<Value>(),
    }
}
