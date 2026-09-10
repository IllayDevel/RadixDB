use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use radixdb_catalog::{
    CatalogEdge, CatalogError, CatalogGeneration, CatalogMutation, CatalogMutationSet,
    CatalogObject, ObjectId, ObjectPrecondition,
};
use radixdb_core::{Error, Result};
use radixdb_plugin_host::PluginRegistry;
use radixdb_procedural::CompileIdentity;
use radixdb_sql::ast::{CreateJobStatement, CreateRoutineStatement, Statement};

use super::extension::{bind_create_extension, bind_drop_extension};
use super::external_type::{bind_create_external_type, bind_drop_external_type};
use super::index::{bind_alter_index, bind_create_index, bind_drop_index};
use super::native_function::bind_create_native_function;
use super::operator::{
    bind_create_operator, bind_create_operator_class, bind_create_planner_support,
    bind_drop_operator, bind_drop_operator_class, bind_drop_planner_support,
};
use super::procedural::{
    bind_alter_job, bind_create_job, bind_create_routine, bind_create_trigger, bind_drop_job,
    bind_drop_routine, bind_drop_trigger, bind_search_path, routine_compile_identity,
    BoundJobDefinition,
};
use super::reconcile::reconcile_table_schema;
use super::security::{
    bind_alter_owner, bind_alter_security_subject, bind_create_principal, bind_create_role,
    bind_create_schema, bind_drop_security_subject, bind_grant, bind_revoke,
};
use super::table::{bind_alter_table, bind_create_table, bind_drop_table};
use super::view::{bind_create_view, bind_drop_view};
use super::{TableCatalog, ViewCatalog};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlTransactionState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DdlDelta {
    pub mutations: Vec<CatalogMutation>,
    pub edge_removals: Vec<CatalogEdge>,
    pub edge_additions: Vec<CatalogEdge>,
}

impl DdlDelta {
    fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.edge_removals.is_empty() && self.edge_additions.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObjectIdSource {
    prescribed: VecDeque<ObjectId>,
    issued: BTreeSet<ObjectId>,
}

impl ObjectIdSource {
    fn with_ids(ids: impl IntoIterator<Item = ObjectId>) -> Self {
        Self {
            prescribed: ids.into_iter().collect(),
            issued: BTreeSet::new(),
        }
    }

    pub fn next(&mut self, generation: &CatalogGeneration) -> Result<ObjectId> {
        if let Some(id) = self.prescribed.pop_front() {
            if generation.object(id).is_some() || !self.issued.insert(id) {
                return Err(Error::InvalidArgument(format!(
                    "prescribed catalog object ID {id} is not fresh"
                )));
            }
            return Ok(id);
        }
        for _ in 0..1024 {
            let id = ObjectId::new();
            if generation.object(id).is_none() && self.issued.insert(id) {
                return Ok(id);
            }
        }
        Err(Error::internal(
            "catalog object ID source did not produce a fresh identity",
        ))
    }
}

/// One private catalog transaction pinned to an immutable source generation.
///
/// Each statement is validated against a private working generation. `commit`
/// emits one coalesced mutation set against the original source; `rollback`
/// emits nothing. Neither operation publishes a catalog generation.
#[derive(Debug, Clone)]
pub struct DdlTransaction {
    source: Arc<CatalogGeneration>,
    working: Arc<CatalogGeneration>,
    ids: ObjectIdSource,
    plugin_registry: Arc<PluginRegistry>,
    state: DdlTransactionState,
}

impl DdlTransaction {
    pub fn begin(source: &CatalogGeneration) -> Self {
        Self::begin_with_object_ids(source, [])
    }

    /// Pin an already shared immutable catalog generation without rebuilding
    /// its graph or lookup maps. Private DDL replaces only `working`; DML-only
    /// transactions and savepoints keep sharing the exact source generation.
    pub fn begin_shared(source: Arc<CatalogGeneration>) -> Self {
        Self::begin_shared_with_object_ids(source, [])
    }

    pub(crate) fn begin_shared_with_plugin_registry(
        source: Arc<CatalogGeneration>,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Self {
        Self {
            working: Arc::clone(&source),
            source,
            ids: ObjectIdSource::default(),
            plugin_registry,
            state: DdlTransactionState::Active,
        }
    }

    pub fn begin_with_object_ids(
        source: &CatalogGeneration,
        ids: impl IntoIterator<Item = ObjectId>,
    ) -> Self {
        Self::begin_shared_with_object_ids(Arc::new(clone_generation(source)), ids)
    }

    fn begin_shared_with_object_ids(
        source: Arc<CatalogGeneration>,
        ids: impl IntoIterator<Item = ObjectId>,
    ) -> Self {
        Self {
            working: Arc::clone(&source),
            source,
            ids: ObjectIdSource::with_ids(ids),
            plugin_registry: Arc::new(PluginRegistry::empty()),
            state: DdlTransactionState::Active,
        }
    }

    pub const fn state(&self) -> DdlTransactionState {
        self.state
    }

    pub fn working_generation(&self) -> &CatalogGeneration {
        self.working.as_ref()
    }

    pub(crate) fn working_generation_shared(&self) -> Arc<CatalogGeneration> {
        Arc::clone(&self.working)
    }

    pub(crate) fn shares_working_generation(&self, generation: &Arc<CatalogGeneration>) -> bool {
        Arc::ptr_eq(&self.working, generation)
    }

    pub(crate) fn has_pending_catalog_changes(&self) -> bool {
        !Arc::ptr_eq(&self.source, &self.working)
    }

    pub(crate) fn prepare_routine_compile(
        &mut self,
        statement: &CreateRoutineStatement,
    ) -> Result<(CompileIdentity, Vec<ObjectId>)> {
        self.require_active()?;
        let candidate_id = self.ids.next(self.working.as_ref())?;
        let identity = routine_compile_identity(statement, self.working.as_ref(), candidate_id)?;
        let search_path = bind_search_path(statement, self.working.as_ref())?;
        Ok((identity, search_path))
    }

    pub fn table(&self, table_name: &str) -> Result<TableCatalog<'_>> {
        TableCatalog::load(&self.working, table_name)
    }

    pub fn view(&self, view_name: &str) -> Result<ViewCatalog<'_>> {
        ViewCatalog::load(&self.working, view_name)
    }

    pub fn stage_sql(&mut self, sql: &str) -> Result<()> {
        self.require_active()?;
        let mut statements =
            radixdb_sql::parse_sql(sql).map_err(|error| Error::Parse(error.to_string()))?;
        if statements.len() != 1 {
            return Err(Error::Parse(
                "catalog DDL adapter accepts exactly one statement at a time".to_owned(),
            ));
        }
        self.stage_statement(
            statements
                .pop()
                .expect("single-statement length was checked"),
        )
    }

    pub fn stage_statement(&mut self, statement: Statement) -> Result<()> {
        self.stage_statement_as(statement, ObjectId::BOOTSTRAP_OWNER, None)
    }

    pub(crate) fn stage_statement_as(
        &mut self,
        statement: Statement,
        actor: ObjectId,
        current_database: Option<&str>,
    ) -> Result<()> {
        self.require_active()?;
        super::security::require_stage_authority(self.working.as_ref(), actor, &statement)?;
        let mut delta = match statement {
            Statement::CreateExtension(statement) => {
                bind_create_extension(&statement, self.working.as_ref(), &self.plugin_registry)?
            }
            Statement::DropExtension(statement) => {
                bind_drop_extension(&statement, self.working.as_ref())?
            }
            Statement::CreateExternalType(statement) => {
                bind_create_external_type(&statement, self.working.as_ref(), &self.plugin_registry)?
            }
            Statement::CreateRoutine(statement) if statement.native.is_some() => {
                bind_create_native_function(
                    &statement,
                    self.working.as_ref(),
                    &self.plugin_registry,
                )?
            }
            Statement::DropExternalType(statement) => {
                bind_drop_external_type(&statement, self.working.as_ref())?
            }
            Statement::CreateOperator(statement) => {
                bind_create_operator(&statement, self.working.as_ref(), &self.plugin_registry)?
            }
            Statement::DropOperator(statement) => {
                bind_drop_operator(&statement, self.working.as_ref())?
            }
            Statement::CreateOperatorClass(statement) => bind_create_operator_class(
                &statement,
                self.working.as_ref(),
                &self.plugin_registry,
            )?,
            Statement::DropOperatorClass(statement) => {
                bind_drop_operator_class(&statement, self.working.as_ref())?
            }
            Statement::CreatePlannerSupport(statement) => bind_create_planner_support(
                &statement,
                self.working.as_ref(),
                &self.plugin_registry,
            )?,
            Statement::DropPlannerSupport(statement) => {
                bind_drop_planner_support(&statement, self.working.as_ref())?
            }
            Statement::CreateTable(statement) => {
                bind_create_table(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::DropTable(statement) => bind_drop_table(
                statement.table_name.value.as_str(),
                statement.if_exists,
                self.working.as_ref(),
            )?,
            Statement::AlterTable(statement) => {
                bind_alter_table(&statement, self.working.as_ref())?
            }
            Statement::CreateIndex(statement) => {
                bind_create_index(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::DropIndex(statement) => bind_drop_index(&statement, self.working.as_ref())?,
            Statement::AlterIndex(statement) => {
                bind_alter_index(&statement, self.working.as_ref())?
            }
            Statement::CreateView(statement) => {
                bind_create_view(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::CreateTrigger(statement) => {
                bind_create_trigger(&statement, actor, self.working.as_ref(), &mut self.ids)?
            }
            Statement::DropRoutine(statement) => {
                bind_drop_routine(&statement, self.working.as_ref())?
            }
            Statement::DropTrigger(statement) => {
                bind_drop_trigger(&statement, self.working.as_ref())?
            }
            Statement::DropJob(statement) => bind_drop_job(&statement, self.working.as_ref())?,
            Statement::AlterJob(statement) => bind_alter_job(&statement, self.working.as_ref())?,
            Statement::CreateSchema(statement) => {
                bind_create_schema(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::CreatePrincipal(statement) => {
                bind_create_principal(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::CreateRole(statement) => {
                bind_create_role(&statement, self.working.as_ref(), &mut self.ids)?
            }
            Statement::AlterSecuritySubject(statement) => {
                bind_alter_security_subject(&statement, self.working.as_ref())?
            }
            Statement::DropSecuritySubject(statement) => {
                bind_drop_security_subject(&statement, self.working.as_ref())?
            }
            Statement::Grant(statement) => bind_grant(
                &statement,
                actor,
                current_database,
                self.working.as_ref(),
                &mut self.ids,
            )?,
            Statement::Revoke(statement) => {
                bind_revoke(&statement, actor, current_database, self.working.as_ref())?
            }
            Statement::AlterOwner(statement) => {
                bind_alter_owner(&statement, actor, self.working.as_ref())?
            }
            Statement::DropView(statement) => bind_drop_view(&statement, self.working.as_ref())?,
            other => {
                return Err(Error::NotSupported(format!(
                    "statement '{}' is outside the catalog DDL adapter",
                    other
                )))
            }
        };
        assign_created_owner(&mut delta, actor)?;
        if delta.is_empty() {
            return Ok(());
        }
        let mutation = mutation_set_for_generation(self.working.as_ref(), delta)?;
        let graph = mutation
            .apply(self.working.as_ref())
            .map_err(catalog_argument)?;
        let format_minor = self
            .working
            .format_minor()
            .max(graph.required_format_minor());
        self.working = Arc::new(
            CatalogGeneration::new_for_minor(format_minor, self.working.meta(), graph)
                .map_err(catalog_argument)?,
        );
        Ok(())
    }

    /// Stage a routine only after the executor semantic compiler has accepted
    /// its body and returned the complete stable dependency set.
    pub(crate) fn stage_compiled_routine_as(
        &mut self,
        statement: &radixdb_sql::CreateRoutineStatement,
        object_id: ObjectId,
        dependencies: Vec<ObjectId>,
        actor: ObjectId,
    ) -> Result<()> {
        self.require_active()?;
        super::security::require_routine_create_authority(
            self.working.as_ref(),
            actor,
            &statement.name,
        )?;
        let mut delta =
            bind_create_routine(statement, self.working.as_ref(), object_id, dependencies)?;
        assign_created_owner(&mut delta, actor)?;
        if delta.is_empty() {
            return Ok(());
        }
        let mutation = mutation_set_for_generation(self.working.as_ref(), delta)?;
        let graph = mutation
            .apply(self.working.as_ref())
            .map_err(catalog_argument)?;
        let format_minor = self
            .working
            .format_minor()
            .max(graph.required_format_minor());
        self.working = Arc::new(
            CatalogGeneration::new_for_minor(format_minor, self.working.meta(), graph)
                .map_err(catalog_argument)?,
        );
        Ok(())
    }

    pub(crate) fn stage_bound_job_as(
        &mut self,
        statement: &CreateJobStatement,
        definition: BoundJobDefinition,
        actor: ObjectId,
    ) -> Result<()> {
        self.require_active()?;
        super::security::require_principal(self.working.as_ref(), actor)?;
        if actor != ObjectId::BOOTSTRAP_OWNER {
            return Err(Error::authorization_denied(
                "only the bootstrap owner may create a durable job",
            ));
        }
        let mut delta =
            bind_create_job(statement, definition, self.working.as_ref(), &mut self.ids)?;
        assign_created_owner(&mut delta, actor)?;
        let mutation = mutation_set_for_generation(self.working.as_ref(), delta)?;
        let graph = mutation
            .apply(self.working.as_ref())
            .map_err(catalog_argument)?;
        let format_minor = self
            .working
            .format_minor()
            .max(graph.required_format_minor());
        self.working = Arc::new(
            CatalogGeneration::new_for_minor(format_minor, self.working.meta(), graph)
                .map_err(catalog_argument)?,
        );
        Ok(())
    }

    /// Replace one table's logical definition from the executor's already
    /// validated complete runtime schema. Existing object identities are
    /// retained by name; an explicit column rename preserves the renamed
    /// column identity as well.
    pub fn stage_table_schema(
        &mut self,
        schema: &radixdb_core::Schema,
        renamed_column: Option<(&str, &str)>,
    ) -> Result<bool> {
        self.stage_table_schema_as(schema, renamed_column, ObjectId::BOOTSTRAP_OWNER)
    }

    pub(crate) fn stage_table_schema_as(
        &mut self,
        schema: &radixdb_core::Schema,
        renamed_column: Option<(&str, &str)>,
        actor: ObjectId,
    ) -> Result<bool> {
        self.require_active()?;
        super::security::require_table_schema_authority(
            self.working.as_ref(),
            actor,
            schema.table_name.as_str(),
        )?;
        let mut delta =
            reconcile_table_schema(schema, renamed_column, self.working.as_ref(), &mut self.ids)?;
        assign_created_owner(&mut delta, actor)?;
        if delta.is_empty() {
            return Ok(false);
        }
        let mutation = mutation_set_for_generation(self.working.as_ref(), delta)?;
        let graph = mutation
            .apply(self.working.as_ref())
            .map_err(catalog_argument)?;
        let format_minor = self
            .working
            .format_minor()
            .max(graph.required_format_minor());
        self.working = Arc::new(
            CatalogGeneration::new_for_minor(format_minor, self.working.meta(), graph)
                .map_err(catalog_argument)?,
        );
        Ok(true)
    }

    /// Stage a statement whose first newly-created objects already have
    /// durable identities assigned by another runtime owner. The IDs are
    /// consumed in normal catalog allocation order and must all be consumed by
    /// this one statement; they never remain queued for a later statement.
    pub fn stage_statement_with_object_ids(
        &mut self,
        statement: Statement,
        ids: impl IntoIterator<Item = ObjectId>,
    ) -> Result<()> {
        self.stage_statement_with_object_ids_as(statement, ids, ObjectId::BOOTSTRAP_OWNER, None)
    }

    pub(crate) fn stage_statement_with_object_ids_as(
        &mut self,
        statement: Statement,
        ids: impl IntoIterator<Item = ObjectId>,
        actor: ObjectId,
        current_database: Option<&str>,
    ) -> Result<()> {
        self.require_active()?;
        if !self.ids.prescribed.is_empty() {
            return Err(Error::internal(
                "catalog object ID source retained an earlier prescription",
            ));
        }
        self.ids.prescribed.extend(ids);
        let result = self.stage_statement_as(statement, actor, current_database);
        let unconsumed = self.ids.prescribed.len();
        self.ids.prescribed.clear();
        result?;
        if unconsumed != 0 {
            return Err(Error::internal(format!(
                "catalog statement left {unconsumed} prescribed object IDs unused"
            )));
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<Option<CatalogMutationSet>> {
        self.require_active()?;
        let mutation = self.pending_mutation()?;
        self.state = DdlTransactionState::Committed;
        Ok(mutation)
    }

    /// Return the coalesced catalog delta without ending the private
    /// transaction. Storage may reject a commit preflight and allow a retry;
    /// the executor must therefore keep this owner active until the shared
    /// storage transaction reaches its terminal state.
    pub fn pending_mutation(&self) -> Result<Option<CatalogMutationSet>> {
        self.require_active()?;
        if Arc::ptr_eq(&self.source, &self.working) {
            return Ok(None);
        }
        diff_generations(self.source.as_ref(), self.working.as_ref())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.require_active()?;
        self.working = Arc::clone(&self.source);
        self.state = DdlTransactionState::RolledBack;
        Ok(())
    }

    fn require_active(&self) -> Result<()> {
        match self.state {
            DdlTransactionState::Active => Ok(()),
            DdlTransactionState::Committed => Err(Error::TransactionCommitted),
            DdlTransactionState::RolledBack => Err(Error::TransactionEnded),
        }
    }
}

fn mutation_set_for_generation(
    generation: &CatalogGeneration,
    mut delta: DdlDelta,
) -> Result<CatalogMutationSet> {
    let unchanged_edges = delta
        .edge_removals
        .iter()
        .filter(|edge| delta.edge_additions.contains(edge))
        .copied()
        .collect::<BTreeSet<_>>();
    if !unchanged_edges.is_empty() {
        delta
            .edge_removals
            .retain(|edge| !unchanged_edges.contains(edge));
        delta
            .edge_additions
            .retain(|edge| !unchanged_edges.contains(edge));
    }
    if generation.format_minor() >= radixdb_catalog::PROCEDURAL_CATALOG_MINOR {
        for mutation in &delta.mutations {
            match mutation {
                CatalogMutation::Create { object } if object.id() != ObjectId::BOOTSTRAP_OWNER => {
                    push_edge_if_absent(
                        &mut delta.edge_additions,
                        CatalogEdge::new(
                            object.id(),
                            object.owner_principal_id(),
                            radixdb_catalog::EdgeKind::OwnedBy,
                            0,
                        ),
                    );
                }
                CatalogMutation::Alter {
                    expected,
                    replacement,
                } => {
                    let current = generation.object(expected.object_id()).ok_or_else(|| {
                        Error::internal("catalog ALTER precondition object disappeared")
                    })?;
                    if current.owner_principal_id() != replacement.owner_principal_id() {
                        push_edge_if_absent(
                            &mut delta.edge_removals,
                            CatalogEdge::new(
                                current.id(),
                                current.owner_principal_id(),
                                radixdb_catalog::EdgeKind::OwnedBy,
                                0,
                            ),
                        );
                        push_edge_if_absent(
                            &mut delta.edge_additions,
                            CatalogEdge::new(
                                replacement.id(),
                                replacement.owner_principal_id(),
                                radixdb_catalog::EdgeKind::OwnedBy,
                                0,
                            ),
                        );
                    }
                }
                _ => {}
            }
        }
    }
    CatalogMutationSet::for_generation(
        generation,
        delta.mutations,
        delta.edge_removals,
        delta.edge_additions,
    )
    .map_err(catalog_argument)
}

fn assign_created_owner(delta: &mut DdlDelta, actor: ObjectId) -> Result<()> {
    for mutation in &mut delta.mutations {
        let CatalogMutation::Create { object } = mutation else {
            continue;
        };
        if object.id() == ObjectId::BOOTSTRAP_OWNER
            || object.owner_principal_id() == actor
            || object.kind() == radixdb_catalog::ObjectKind::ExternalType
        {
            continue;
        }
        *object = CatalogObject::new(
            object.id(),
            object.namespace_id(),
            object.parent_id(),
            actor,
            object.name().clone(),
            object.definition_revision(),
            object.payload().clone(),
        )
        .map_err(catalog_argument)?;
    }
    Ok(())
}

fn push_edge_if_absent(edges: &mut Vec<CatalogEdge>, edge: CatalogEdge) {
    if !edges.contains(&edge) {
        edges.push(edge);
    }
}

fn diff_generations(
    source: &CatalogGeneration,
    working: &CatalogGeneration,
) -> Result<Option<CatalogMutationSet>> {
    let source_objects = source
        .graph()
        .objects()
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let working_objects = working
        .graph()
        .objects()
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let dropped = source_objects
        .keys()
        .filter(|id| !working_objects.contains_key(id))
        .copied()
        .collect::<BTreeSet<_>>();
    let mut mutations = Vec::new();
    for (id, source_object) in &source_objects {
        let Some(working_object) = working_objects.get(id) else {
            mutations.push(CatalogMutation::drop(object_precondition(source_object)?));
            continue;
        };
        if *source_object == *working_object {
            continue;
        }
        if name_only_change(source_object, working_object) {
            mutations.push(CatalogMutation::rename(
                object_precondition(source_object)?,
                working_object.name().clone(),
            ));
        } else {
            mutations.push(CatalogMutation::alter(
                object_precondition(source_object)?,
                object_with_revision(
                    working_object,
                    source_object
                        .definition_revision()
                        .checked_add(1)
                        .ok_or_else(|| Error::internal("catalog object revision overflow"))?,
                )?,
            ));
        }
    }
    for (id, working_object) in &working_objects {
        if !source_objects.contains_key(id) {
            mutations.push(CatalogMutation::create(object_with_revision(
                working_object,
                1,
            )?));
        }
    }

    let source_edges = source
        .graph()
        .edges()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let working_edges = working
        .graph()
        .edges()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let edge_removals = source_edges
        .difference(&working_edges)
        .filter(|edge| {
            !dropped.contains(&edge.source_object_id())
                && !dropped.contains(&edge.target_object_id())
        })
        .copied()
        .collect::<Vec<_>>();
    let edge_additions = working_edges
        .difference(&source_edges)
        .copied()
        .collect::<Vec<_>>();
    if mutations.is_empty() && edge_removals.is_empty() && edge_additions.is_empty() {
        return Ok(None);
    }
    let meta = source.meta();
    CatalogMutationSet::new_for_minor(
        working.format_minor(),
        meta.database_id(),
        meta.catalog_id(),
        meta.catalog_generation(),
        mutations,
        edge_removals,
        edge_additions,
    )
    .map(Some)
    .map_err(catalog_argument)
}

fn object_precondition(object: &CatalogObject) -> Result<ObjectPrecondition> {
    ObjectPrecondition::new(object.id(), object.kind(), object.definition_revision())
        .map_err(catalog_argument)
}

fn object_with_revision(object: &CatalogObject, revision: u64) -> Result<CatalogObject> {
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

fn name_only_change(source: &CatalogObject, working: &CatalogObject) -> bool {
    source.name() != working.name()
        && source.id() == working.id()
        && source.kind() == working.kind()
        && source.namespace_id() == working.namespace_id()
        && source.parent_id() == working.parent_id()
        && source.owner_principal_id() == working.owner_principal_id()
        && source.payload() == working.payload()
}

fn clone_generation(generation: &CatalogGeneration) -> CatalogGeneration {
    CatalogGeneration::new_for_minor(
        generation.format_minor(),
        generation.meta(),
        generation.graph().clone(),
    )
    .expect("an admitted catalog generation keeps its format minor")
}

pub(super) fn catalog_argument(error: CatalogError) -> Error {
    Error::InvalidArgument(format!("catalog DDL rejected: {error}"))
}
