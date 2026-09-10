//! Concrete SQL executor and public execution entry points.

use std::sync::{Arc, Mutex, OnceLock};

use crate::catalog::DdlTransaction;
use radixdb_catalog::ObjectId;
use radixdb_core::{Error, ParamVec, Result, Value};
use radixdb_functions::FunctionRegistry;
use radixdb_plugin_host::{
    DatabasePluginAdmission, ObjectKind as PluginObjectKind, ObjectRequirement, PackageRequirement,
    PluginRegistry, RequirementIssue,
};
use radixdb_sql::ast::{Program, Statement};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::mvcc::ViewDefinition;
use radixdb_storage::traits::{Engine, QueryResult, Transaction};
use rustc_hash::FxHashMap;

use crate::aggregation::AggregationExecutorExt;
use crate::context::{ExecutionContext, TimeoutGuard};
use crate::mutation::dml::DmlExecutorExt;
use crate::mutation::dml_fast_path::DmlFastPathExt;
use crate::mutation::host::ActiveTransaction;
use crate::mutation::pk_fast_path::PkFastPathExt;
use crate::navigation;
use crate::planner::QueryPlanner;
use crate::procedural::{prepare_dml_triggers, DmlTriggerEvent};
use crate::query_cache::{CacheStats, CachedPlanRef, QueryCache};
use crate::result::{self, ExecutionResult};
use crate::semantic_cache::{SemanticCache, SemanticCacheStatsSnapshot};

#[cfg(any(test, feature = "test-hooks"))]
thread_local! {
    static TEST_EXECUTOR_CONSTRUCTIONS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn test_executor_construction_count() -> usize {
    TEST_EXECUTOR_CONSTRUCTIONS.with(std::cell::Cell::get)
}

#[cfg(any(test, feature = "test-hooks"))]
fn record_test_executor_construction() {
    TEST_EXECUTOR_CONSTRUCTIONS.with(|count| count.set(count.get() + 1));
}

static DEFAULT_FUNCTION_REGISTRY: OnceLock<Arc<FunctionRegistry>> = OnceLock::new();
static DEFAULT_PLUGIN_REGISTRY: OnceLock<Arc<PluginRegistry>> = OnceLock::new();

#[inline]
fn default_function_registry() -> Arc<FunctionRegistry> {
    DEFAULT_FUNCTION_REGISTRY
        .get_or_init(|| Arc::new(FunctionRegistry::new()))
        .clone()
}

#[inline]
fn default_plugin_registry() -> Arc<PluginRegistry> {
    DEFAULT_PLUGIN_REGISTRY
        .get_or_init(|| Arc::new(PluginRegistry::empty()))
        .clone()
}

fn append_plugin_object_requirement(
    catalog: &radixdb_catalog::CatalogGeneration,
    requirements: &mut [PackageRequirement],
    extension_binding_id: ObjectId,
    requirement: ObjectRequirement,
) -> Result<()> {
    let extension = catalog.object(extension_binding_id).ok_or_else(|| {
        Error::internal("plugin-backed catalog object references a missing extension binding")
    })?;
    let radixdb_catalog::CatalogPayload::Extension(extension) = extension.payload() else {
        return Err(Error::internal(
            "plugin-backed catalog object references a non-extension catalog object",
        ));
    };
    let package_id = extension.package_id().into_bytes();
    let package = requirements
        .iter_mut()
        .find(|candidate| candidate.package_id == package_id)
        .ok_or_else(|| Error::internal("plugin package requirement was not constructed"))?;
    package.objects.push(requirement);
    Ok(())
}

/// SQL Query Executor
///
/// The executor is the main entry point for executing SQL statements.
/// It coordinates between the parser, storage engine, and function registry.
pub struct Executor {
    /// Storage engine
    pub(crate) engine: Arc<MVCCEngine>,
    /// Function registry for scalar, aggregate, and window functions
    pub(crate) function_registry: Arc<FunctionRegistry>,
    /// Immutable process startup registry used to admit durable extension bindings.
    pub(crate) plugin_registry: Arc<PluginRegistry>,
    /// Query cache for parsed statements
    pub(crate) query_cache: QueryCache,
    /// Semantic cache for query results with subsumption detection
    pub(crate) semantic_cache: Arc<SemanticCache>,
    /// Cardinality feedback shared by connections to the same engine owner.
    pub(crate) feedback_cache: Arc<crate::optimizer::FeedbackCache>,
    /// Rebuildable cache of source-verified procedural programs.
    pub(crate) procedural_cache: Arc<crate::procedural::ProceduralProgramCache>,
    /// Active transaction for explicit transaction control (BEGIN/COMMIT/ROLLBACK)
    pub(crate) active_transaction: Arc<Mutex<Option<ActiveTransaction>>>,
    /// Default isolation for future transactions created by this SQL
    /// connection. Forked executors share it only when they are nested work of
    /// the same connection; independent Database handles never share it.
    pub(crate) default_isolation_level: Arc<Mutex<radixdb_core::IsolationLevel>>,
    /// A logical export owns one shared catalog fence for its complete
    /// lifetime. Its nested read statements must not recursively reacquire the
    /// same lock, which can deadlock once an exclusive DDL waiter is queued.
    pub(crate) ddl_fence_already_held: bool,
    /// Query planner for cost-based optimization (lazily initialized)
    pub(crate) query_planner: std::sync::OnceLock<QueryPlanner>,
}

impl Executor {
    /// Resolve and verify a durable catalog Principal for a network session.
    /// The returned stable ID is the only identity accepted by later request
    /// contexts; credentials never leave this boundary.
    pub fn authenticate_principal(&self, login: &str, password: &str) -> Result<ObjectId> {
        if self.has_active_transaction() {
            return Err(Error::invalid_argument(
                "authentication cannot use a connection with an active transaction",
            ));
        }
        self.require_normal_plugin_admission("principal authentication")?;
        let catalog = self.engine.pin_catalog()?;
        crate::catalog::security::authenticate_catalog_principal(catalog.as_ref(), login, password)
    }
    pub(crate) fn fork_for_stored_function(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
            function_registry: Arc::clone(&self.function_registry),
            plugin_registry: Arc::clone(&self.plugin_registry),
            query_cache: QueryCache::default(),
            semantic_cache: Arc::clone(&self.semantic_cache),
            feedback_cache: Arc::clone(&self.feedback_cache),
            procedural_cache: Arc::clone(&self.procedural_cache),
            active_transaction: Arc::clone(&self.active_transaction),
            default_isolation_level: Arc::clone(&self.default_isolation_level),
            ddl_fence_already_held: self.ddl_fence_already_held,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Fork an executor for nested work while the caller owns a shared catalog
    /// fence for the complete outer statement/call. Nested SQL must reuse that
    /// ownership: recursively taking a read lock can deadlock when a writer is
    /// queued between the outer and inner acquisition.
    pub(crate) fn fork_with_owned_ddl_fence(&self) -> Self {
        let mut executor = self.fork_for_stored_function();
        executor.ddl_fence_already_held = true;
        executor
    }

    fn install_storage_binders(engine: &MVCCEngine, plugin_registry: Arc<PluginRegistry>) {
        engine.install_row_validator_binder(crate::mutation::row_validation::bind);
        engine.install_view_dependency_binder(crate::mutation::view_binding::bind_from_sql);
        engine.install_catalog_runtime_binder(crate::catalog::plugin_catalog_runtime_binder(
            plugin_registry,
        ));
    }

    /// Resolve a view through the transaction-private catalog when one is
    /// active, otherwise through the published runtime projection.
    pub(crate) fn visible_view_lowercase(
        &self,
        name_lower: &str,
    ) -> Result<Option<Arc<ViewDefinition>>> {
        let active = self.active_transaction.lock().unwrap();
        if let Some(state) = active.as_ref() {
            return crate::catalog::bind_runtime_view(
                state.catalog.working_generation(),
                name_lower,
            )
            .map(|view| view.map(Arc::new));
        }
        drop(active);
        self.engine.get_view_lowercase(name_lower)
    }

    pub(crate) fn visible_view(&self, name: &str) -> Result<Option<Arc<ViewDefinition>>> {
        self.visible_view_lowercase(&name.to_lowercase())
    }

    pub(crate) fn visible_view_names(&self) -> Result<Vec<String>> {
        let active = self.active_transaction.lock().unwrap();
        if let Some(state) = active.as_ref() {
            return Ok(crate::catalog::list_runtime_views(
                state.catalog.working_generation(),
            ));
        }
        drop(active);
        self.engine.list_views()
    }

    #[doc(hidden)]
    pub fn describe_query_output(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<crate::binding::output::QueryOutputColumn>>> {
        crate::binding::output::OutputBindingExt::describe_query_output(self, sql)
    }

    /// Create a new executor with the given storage engine
    pub fn new(engine: Arc<MVCCEngine>) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        let plugin_registry = default_plugin_registry();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry: default_function_registry(),
            plugin_registry,
            query_cache: QueryCache::default(),
            semantic_cache: Arc::new(SemanticCache::default()),
            feedback_cache: Arc::new(crate::optimizer::FeedbackCache::new()),
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Create a new executor with a custom function registry
    pub fn with_function_registry(
        engine: Arc<MVCCEngine>,
        function_registry: Arc<FunctionRegistry>,
    ) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        let plugin_registry = default_plugin_registry();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry,
            plugin_registry,
            query_cache: QueryCache::default(),
            semantic_cache: Arc::new(SemanticCache::default()),
            feedback_cache: Arc::new(crate::optimizer::FeedbackCache::new()),
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Create a new executor with a custom cache size
    pub fn with_cache_size(engine: Arc<MVCCEngine>, cache_size: usize) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        let plugin_registry = default_plugin_registry();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry: default_function_registry(),
            plugin_registry,
            query_cache: QueryCache::new(cache_size),
            semantic_cache: Arc::new(SemanticCache::default()),
            feedback_cache: Arc::new(crate::optimizer::FeedbackCache::new()),
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Create a connection-local executor with engine-owner-scoped runtime
    /// caches. Transaction state and parsed-plan cache remain connection-local.
    #[doc(hidden)]
    pub fn with_shared_runtime_caches(
        engine: Arc<MVCCEngine>,
        semantic_cache: Arc<SemanticCache>,
        feedback_cache: Arc<crate::optimizer::FeedbackCache>,
    ) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        let plugin_registry = default_plugin_registry();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry: default_function_registry(),
            plugin_registry,
            query_cache: QueryCache::default(),
            semantic_cache,
            feedback_cache,
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Create a connection-local executor with owner-scoped runtime caches and
    /// the immutable process startup plugin registry.
    #[doc(hidden)]
    pub fn with_shared_runtime_caches_and_plugin_registry(
        engine: Arc<MVCCEngine>,
        semantic_cache: Arc<SemanticCache>,
        feedback_cache: Arc<crate::optimizer::FeedbackCache>,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry: default_function_registry(),
            plugin_registry,
            query_cache: QueryCache::default(),
            semantic_cache,
            feedback_cache,
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Create a standalone executor against an immutable startup registry.
    #[doc(hidden)]
    pub fn with_plugin_registry(
        engine: Arc<MVCCEngine>,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Self {
        #[cfg(any(test, feature = "test-hooks"))]
        record_test_executor_construction();
        Self::install_storage_binders(&engine, Arc::clone(&plugin_registry));
        let default_isolation_level = engine.registry().get_global_isolation_level();
        Self {
            engine,
            function_registry: default_function_registry(),
            plugin_registry,
            query_cache: QueryCache::default(),
            semantic_cache: Arc::new(SemanticCache::default()),
            feedback_cache: Arc::new(crate::optimizer::FeedbackCache::new()),
            procedural_cache: Arc::default(),
            active_transaction: Arc::new(Mutex::new(None)),
            default_isolation_level: Arc::new(Mutex::new(default_isolation_level)),
            ddl_fence_already_held: false,
            query_planner: std::sync::OnceLock::new(),
        }
    }

    /// Assess durable package bindings and every external type identity already
    /// referenced by the database catalog.
    #[doc(hidden)]
    pub fn plugin_admission(&self) -> Result<DatabasePluginAdmission> {
        let catalog = self.engine.pin_catalog()?;
        let mut requirements = catalog
            .objects_of_kind(radixdb_catalog::ObjectKind::Extension)
            .map(|object| {
                let radixdb_catalog::CatalogPayload::Extension(payload) = object.payload() else {
                    return Err(Error::internal(
                        "catalog admitted Extension with a different payload",
                    ));
                };
                PackageRequirement::for_package_binding(
                    payload.package_id().into_bytes(),
                    payload.version(),
                    payload.abi_major(),
                    payload.abi_min_minor(),
                    payload.abi_max_minor(),
                    *payload.descriptor_fingerprint(),
                )
                .map_err(|detail| {
                    Error::internal(format!(
                        "catalog admitted an invalid extension requirement: {detail}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::ExternalType) {
            let radixdb_catalog::CatalogPayload::ExternalType(payload) = object.payload() else {
                return Err(Error::internal(
                    "catalog admitted ExternalType with a different payload",
                ));
            };
            append_plugin_object_requirement(
                catalog.as_ref(),
                &mut requirements,
                payload.extension_binding_id(),
                ObjectRequirement {
                    object_id: object.id().into_bytes(),
                    kind: PluginObjectKind::ExternalType,
                    codec_version: Some(payload.write_codec_version()),
                    semantic_revision: Some(payload.semantic_revision()),
                },
            )?;
        }
        for object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::Function) {
            let radixdb_catalog::CatalogPayload::Function(payload) = object.payload() else {
                return Err(Error::internal(
                    "catalog admitted Function with a different payload",
                ));
            };
            let Some(native) = payload.native_definition() else {
                continue;
            };
            append_plugin_object_requirement(
                catalog.as_ref(),
                &mut requirements,
                native.extension_binding_id(),
                ObjectRequirement {
                    object_id: object.id().into_bytes(),
                    kind: PluginObjectKind::Function,
                    codec_version: None,
                    semantic_revision: Some(native.semantic_revision()),
                },
            )?;
        }
        for object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::Operator) {
            let radixdb_catalog::CatalogPayload::Operator(payload) = object.payload() else {
                return Err(Error::internal(
                    "catalog admitted Operator with a different payload",
                ));
            };
            append_plugin_object_requirement(
                catalog.as_ref(),
                &mut requirements,
                payload.extension_binding_id(),
                ObjectRequirement {
                    object_id: object.id().into_bytes(),
                    kind: PluginObjectKind::Operator,
                    codec_version: None,
                    semantic_revision: Some(payload.semantic_revision()),
                },
            )?;
        }
        for object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::OperatorClass) {
            let radixdb_catalog::CatalogPayload::OperatorClass(payload) = object.payload() else {
                return Err(Error::internal(
                    "catalog admitted OperatorClass with a different payload",
                ));
            };
            append_plugin_object_requirement(
                catalog.as_ref(),
                &mut requirements,
                payload.extension_binding_id(),
                ObjectRequirement {
                    object_id: object.id().into_bytes(),
                    kind: PluginObjectKind::OperatorClass,
                    codec_version: Some(payload.key_codec_revision()),
                    semantic_revision: Some(payload.semantic_revision()),
                },
            )?;
        }
        for object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::PlannerSupport) {
            let radixdb_catalog::CatalogPayload::PlannerSupport(payload) = object.payload() else {
                return Err(Error::internal(
                    "catalog admitted PlannerSupport with a different payload",
                ));
            };
            append_plugin_object_requirement(
                catalog.as_ref(),
                &mut requirements,
                payload.extension_binding_id(),
                ObjectRequirement {
                    object_id: object.id().into_bytes(),
                    kind: PluginObjectKind::PlannerSupport,
                    codec_version: None,
                    semantic_revision: Some(payload.semantic_revision()),
                },
            )?;
        }
        Ok(self.plugin_registry.assess_requirements(&requirements))
    }

    #[doc(hidden)]
    pub fn plugin_admission_diagnostic(&self) -> Result<Option<String>> {
        Ok(match self.plugin_admission()? {
            DatabasePluginAdmission::Normal => None,
            DatabasePluginAdmission::Restricted { issues } => {
                Some(restricted_plugin_error("ordinary access", &issues).to_string())
            }
        })
    }

    fn require_normal_plugin_admission(&self, operation: &str) -> Result<()> {
        match self.plugin_admission()? {
            DatabasePluginAdmission::Normal => Ok(()),
            DatabasePluginAdmission::Restricted { issues } => {
                Err(restricted_plugin_error(operation, &issues))
            }
        }
    }

    fn admit_statement_for_plugins(
        &self,
        statement: &Statement,
        context: &ExecutionContext,
    ) -> Result<()> {
        match self.plugin_admission()? {
            DatabasePluginAdmission::Normal => Ok(()),
            DatabasePluginAdmission::Restricted { .. }
                if matches!(statement, Statement::DropExtension(_))
                    && context.effective_principal_id() == ObjectId::BOOTSTRAP_OWNER =>
            {
                Ok(())
            }
            DatabasePluginAdmission::Restricted { issues } => {
                Err(restricted_plugin_error("SQL execution", &issues))
            }
        }
    }

    /// Construct the executor used by a logical export transaction.
    ///
    /// The caller owns a shared [`DdlFenceGuard`] for the executor's complete
    /// lifetime, so ordinary read statements reuse that catalog generation.
    #[doc(hidden)]
    pub fn new_with_owned_ddl_fence(
        engine: Arc<MVCCEngine>,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Self {
        let mut executor = Self::with_plugin_registry(engine, plugin_registry);
        executor.ddl_fence_already_held = true;
        executor
    }

    #[doc(hidden)]
    pub fn plugin_registry(&self) -> Arc<PluginRegistry> {
        Arc::clone(&self.plugin_registry)
    }

    /// Stream one transaction-consistent table snapshot without constructing
    /// an owning `RowVec` for the whole table.
    #[doc(hidden)]
    pub fn visit_logical_export_rows(
        &self,
        table_name: &str,
        visitor: &mut dyn FnMut(i64, radixdb_core::Row) -> Result<()>,
    ) -> Result<()> {
        if !self.ddl_fence_already_held {
            return Err(Error::internal(
                "logical export row scan requires an owned catalog fence",
            ));
        }
        let active = self.active_transaction.lock().unwrap();
        let state = active.as_ref().ok_or(Error::TransactionNotStarted)?;
        state
            .transaction
            .get_table(table_name)?
            .visit_visible_rows(visitor)
    }

    /// Check if there is an active explicit transaction
    pub fn has_active_transaction(&self) -> bool {
        self.active_transaction.lock().unwrap().is_some()
    }

    /// Return the storage transaction id owned by this executor, if any.
    #[doc(hidden)]
    pub fn active_transaction_id(&self) -> Option<i64> {
        self.active_transaction
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| state.transaction.id())
    }

    #[doc(hidden)]
    pub fn create_active_savepoint(&self, name: &str) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        active
            .as_mut()
            .ok_or(Error::TransactionNotStarted)?
            .create_savepoint(name)
    }

    #[doc(hidden)]
    pub fn rollback_active_to_savepoint(&self, name: &str) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        active
            .as_mut()
            .ok_or(Error::TransactionNotStarted)?
            .rollback_to_savepoint(name)
    }

    #[doc(hidden)]
    pub fn release_active_savepoint(&self, name: &str) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        active
            .as_mut()
            .ok_or(Error::TransactionNotStarted)?
            .release_savepoint(name)
    }

    /// Commit the externally installed transaction while retaining the whole
    /// executor-owned state when a recoverable preflight error leaves it active.
    #[doc(hidden)]
    pub fn commit_installed_transaction(&self) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        let mut state = active.take().ok_or(Error::TransactionNotStarted)?;
        let _catalog_write_fence = state
            .has_pending_catalog_changes()
            .then(|| self.engine.acquire_catalog_write_fence());
        if let Err(error) = state.stage_catalog_for_commit() {
            *active = Some(state);
            return Err(error);
        }
        match state.transaction.commit() {
            Ok(()) => Ok(()),
            Err(error) => {
                if state.transaction.is_active() {
                    *active = Some(state);
                }
                Err(error)
            }
        }
    }

    #[doc(hidden)]
    pub fn rollback_installed_transaction(&self) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        let mut state = active.take().ok_or(Error::TransactionNotStarted)?;
        state.rollback()
    }

    /// Get the query planner (lazily initialized)
    pub(crate) fn get_query_planner(&self) -> &QueryPlanner {
        self.query_planner.get_or_init(|| {
            QueryPlanner::with_feedback_cache(
                Arc::clone(&self.engine),
                Arc::clone(&self.feedback_cache),
            )
        })
    }

    pub(crate) fn bind_cached_reference_expand(
        &self,
        statement: &Statement,
        cached: &Arc<std::sync::RwLock<navigation::CachedReferenceExpand>>,
    ) -> Result<Option<navigation::ReferenceExpandPlan>> {
        let select = match statement {
            Statement::Select(select) => select,
            Statement::Explain(explain) => match explain.statement.as_ref() {
                Statement::Select(select) => select,
                _ => {
                    navigation::reject_navigation_in_write_statement(
                        self.engine.as_ref(),
                        statement,
                    )?;
                    return Ok(None);
                }
            },
            _ => {
                navigation::reject_navigation_in_write_statement(self.engine.as_ref(), statement)?;
                return Ok(None);
            }
        };
        let schema_scope_id = self.engine.schema_scope_id();
        let schema_generation = self.engine.schema_epoch();

        {
            let binding = cached
                .read()
                .map_err(|_| Error::LockAcquisitionFailed("reference expand cache".to_string()))?;
            match &*binding {
                navigation::CachedReferenceExpand::NoPaths {
                    schema_scope_id: cached_scope,
                    schema_generation: cached_generation,
                } if *cached_scope == schema_scope_id
                    && *cached_generation == schema_generation =>
                {
                    return Ok(None);
                }
                navigation::CachedReferenceExpand::Plan(plan)
                    if plan.schema_scope_id() == schema_scope_id
                        && plan.schema_generation() == schema_generation =>
                {
                    return Ok(Some(plan.clone()));
                }
                _ => {}
            }
        }

        let plan = navigation::bind_reference_expand_plan(self.engine.as_ref(), select)?;
        let mut binding = cached
            .write()
            .map_err(|_| Error::LockAcquisitionFailed("reference expand cache".to_string()))?;
        *binding = match &plan {
            Some(plan) => navigation::CachedReferenceExpand::Plan(plan.clone()),
            None => navigation::CachedReferenceExpand::NoPaths {
                schema_scope_id,
                schema_generation,
            },
        };
        Ok(plan)
    }

    /// Set the default isolation level for new transactions
    pub fn set_default_isolation_level(&self, level: radixdb_core::IsolationLevel) {
        *self.default_isolation_level.lock().unwrap() = level;
    }

    /// Return this connection's default isolation for future transactions.
    pub fn default_isolation_level(&self) -> radixdb_core::IsolationLevel {
        *self.default_isolation_level.lock().unwrap()
    }

    /// Get the storage engine
    pub fn engine(&self) -> &Arc<MVCCEngine> {
        &self.engine
    }

    /// Get the function registry
    pub fn function_registry(&self) -> &Arc<FunctionRegistry> {
        &self.function_registry
    }

    /// Execute a SQL query string
    ///
    /// This is the main entry point for executing SQL statements.
    /// It parses the query and executes each statement in order.
    /// Uses the query cache to avoid re-parsing identical queries.
    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        let ctx = ExecutionContext::new();
        self.execute_with_context(sql, &ctx)
    }

    /// Execute a SQL query with positional parameters
    ///
    /// Parameters are substituted for $1, $2, etc. placeholders in the query.
    /// Uses the query cache and selects any eligible borrowed-parameter fast
    /// path internally, so public facades do not own execution policy.
    pub fn execute_with_params(&self, sql: &str, params: ParamVec) -> Result<ExecutionResult> {
        if params.is_empty() {
            return self.execute(sql);
        }
        if !params.iter().any(|value| value.as_external().is_some()) {
            if let Some(result) = self.try_fast_path_with_params(sql, &params) {
                return result;
            }
        }
        let ctx = ExecutionContext::with_params(params);
        self.execute_with_context(sql, &ctx)
    }

    /// Try fast path execution with borrowed params slice
    /// Returns None if fast path doesn't apply, Some(result) otherwise
    pub fn try_fast_path_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Option<Result<ExecutionResult>> {
        crate::dispatch::program::try_fast_path_with_params(self, sql, params)
    }

    /// Execute a SQL query with named parameters
    ///
    /// Parameters are substituted for :name placeholders in the query.
    /// Uses the query cache for efficient re-execution of parameterized queries.
    pub fn execute_with_named_params(
        &self,
        sql: &str,
        params: FxHashMap<String, Value>,
    ) -> Result<ExecutionResult> {
        let ctx = ExecutionContext::with_named_params(params);
        self.execute_with_context(sql, &ctx)
    }

    /// Execute a SQL query with a full execution context
    /// Uses the query cache for efficient re-execution.
    pub fn execute_with_context(
        &self,
        sql: &str,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        let timeout_guard = TimeoutGuard::new(ctx);
        let result = self.execute_cached(sql, ctx)?;
        Ok(result::TimedQueryResult::wrap_with_workload(
            result,
            timeout_guard,
            ctx.cancellation_handle(),
            sql,
        ))
    }

    /// Execute a SQL query using the query cache
    ///
    /// This method first checks the cache for a previously parsed statement.
    /// If found, it uses the cached AST. Otherwise, it parses the query
    /// and caches the result for future use.
    fn execute_cached(&self, sql: &str, ctx: &ExecutionContext) -> Result<Box<dyn QueryResult>> {
        crate::dispatch::program::execute_sql(self, sql, ctx)
    }

    /// Get the query cache
    pub fn query_cache(&self) -> &QueryCache {
        &self.query_cache
    }

    /// Get query cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.query_cache.stats()
    }

    /// Clear the query cache
    pub fn clear_cache(&self) {
        self.query_cache.clear();
        self.procedural_cache.clear();
    }

    /// Get the semantic cache
    pub fn semantic_cache(&self) -> &SemanticCache {
        &self.semantic_cache
    }

    /// Get semantic cache statistics
    pub fn semantic_cache_stats(&self) -> SemanticCacheStatsSnapshot {
        self.semantic_cache.stats()
    }

    /// Clear the semantic cache
    pub fn clear_semantic_cache(&self) {
        self.semantic_cache.clear();
        self.feedback_cache.clear();
    }

    /// Invalidate semantic cache for a specific table
    ///
    /// Call this after INSERT, UPDATE, DELETE, or TRUNCATE on a table.
    pub fn invalidate_semantic_cache(&self, table_name: &str) {
        self.semantic_cache.invalidate_table(table_name);
        self.feedback_cache.invalidate_table(table_name);
    }

    /// Execute a parsed program
    pub fn execute_program(&self, program: &Program) -> Result<ExecutionResult> {
        let ctx = ExecutionContext::new();
        self.execute_program_with_context(program, &ctx)
    }

    /// Execute a parsed program with context
    pub fn execute_program_with_context(
        &self,
        program: &Program,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        crate::dispatch::program::execute_program(self, program, ctx)
    }

    /// Execute a single statement
    pub fn execute_statement(
        &self,
        statement: &Statement,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        self.execute_statement_inner(statement, ctx, false, None)
    }

    fn execute_statement_after_navigation(
        &self,
        statement: &Statement,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        self.execute_statement_inner(statement, ctx, true, None)
    }

    fn execute_statement_with_navigation_plan(
        &self,
        statement: &Statement,
        ctx: &ExecutionContext,
        plan: Option<navigation::ReferenceExpandPlan>,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_statement_inner(statement, ctx, true, plan)
    }

    fn execute_statement_inner(
        &self,
        statement: &Statement,
        ctx: &ExecutionContext,
        navigation_checked: bool,
        reference_expand: Option<navigation::ReferenceExpandPlan>,
    ) -> Result<Box<dyn QueryResult>> {
        self.admit_statement_for_plugins(statement, ctx)?;
        for value in ctx.params().iter().chain(ctx.named_params().values()) {
            if value.as_external().is_some() {
                self.plugin_registry
                    .validate_external_value(value)
                    .map_err(|error| Error::invalid_argument(error.to_string()))?;
            }
        }
        let bound_context;
        let ctx = if ctx.stored_function_invoker().is_some() {
            ctx
        } else {
            let invoker: Arc<dyn crate::context::StoredFunctionInvoker> = Arc::new(
                crate::procedural::function::ExecutorStoredFunctionInvoker::new(self, ctx),
            );
            bound_context = ctx.clone().with_stored_function_invoker(invoker);
            &bound_context
        };

        // A statement that invokes a durable function owns one transaction.
        // Individual function invocations use savepoints inside it, so a
        // VOLATILE function never commits independently of its caller. SELECT
        // retains the boundary until clean cursor exhaustion; eager DML can
        // complete it as soon as dispatch succeeds.
        let function_boundary = if !self.has_active_transaction()
            && crate::procedural::function::statement_calls_stored_function(self, statement)?
        {
            Some(self.begin_procedural_boundary()?)
        } else {
            None
        };
        let result = crate::dispatch::statement::execute_statement(
            self,
            statement,
            ctx,
            navigation_checked,
            reference_expand,
        );
        match (result, function_boundary) {
            (Ok(result), Some(boundary)) if matches!(statement, Statement::Select(_)) => Ok(
                crate::procedural::function::wrap_function_statement_result(self, result, boundary),
            ),
            (Ok(result), Some(boundary)) => match self.complete_procedural_boundary(&boundary) {
                Ok(()) => Ok(result),
                Err(error) => {
                    let _ = self.abort_procedural_boundary(&boundary);
                    Err(error)
                }
            },
            (Err(error), Some(boundary)) => {
                let _ = self.abort_procedural_boundary(&boundary);
                Err(error)
            }
            (Ok(result), None) => Ok(result),
            (Err(error), None) => Err(error),
        }
    }

    /// Install an external storage transaction as the active transaction.
    ///
    /// Used by the programmatic Transaction API to delegate SELECT queries
    /// to the full executor pipeline (aggregates, JOINs, window functions, etc.)
    /// while keeping the transaction's uncommitted changes visible.
    #[doc(hidden)]
    pub fn install_transaction(&self, tx: Box<dyn Transaction>) {
        let mut active_tx = self.active_transaction.lock().unwrap();
        let catalog = self
            .engine
            .pin_catalog()
            .expect("an installed transaction belongs to an open catalog owner");
        *active_tx = Some(ActiveTransaction::new(
            tx,
            DdlTransaction::begin_shared_with_plugin_registry(
                catalog,
                Arc::clone(&self.plugin_registry),
            ),
        ));
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.engine
            .begin_transaction_with_level(self.default_isolation_level())
    }

    /// Begin a new transaction with a specific isolation level
    pub fn begin_transaction_with_isolation(
        &self,
        isolation: radixdb_core::IsolationLevel,
    ) -> Result<Box<dyn Transaction>> {
        self.engine.begin_transaction_with_level(isolation)
    }

    /// Get or create a cached plan for a SQL statement.
    ///
    /// Parses the SQL and caches the plan if not already cached.
    /// Returns a lightweight CachedPlanRef that can be stored and reused
    /// for repeated execution without re-parsing or cache lookup overhead.
    pub fn get_or_create_plan(&self, sql: &str) -> Result<CachedPlanRef> {
        crate::dispatch::program::get_or_create_plan(self, sql)
    }

    /// Execute a pre-cached plan directly, skipping cache lookup.
    ///
    /// This is the fast path for prepared statements: the caller holds a
    /// `CachedPlanRef` obtained from `get_or_create_plan()` and passes it
    /// here on every execution, avoiding normalize + hash + RwLock read
    /// per call.
    pub fn execute_with_cached_plan(
        &self,
        plan: &CachedPlanRef,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        crate::dispatch::program::execute_prepared_plan(self, plan, ctx)
    }

    pub(crate) fn execute_bound_cached_plan(
        &self,
        plan: &CachedPlanRef,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        self.admit_statement_for_plugins(plan.statement.as_ref(), ctx)?;
        for value in ctx.params().iter().chain(ctx.named_params().values()) {
            if value.as_external().is_some() {
                self.plugin_registry
                    .validate_external_value(value)
                    .map_err(|error| Error::invalid_argument(error.to_string()))?;
            }
        }
        let bound_context;
        let ctx = if ctx.stored_function_invoker().is_some() {
            ctx
        } else {
            let invoker: Arc<dyn crate::context::StoredFunctionInvoker> = Arc::new(
                crate::procedural::function::ExecutorStoredFunctionInvoker::new(self, ctx),
            );
            bound_context = ctx.clone().with_stored_function_invoker(invoker);
            &bound_context
        };
        // Cached/compiled paths are execution accelerators, never an
        // authorization authority. Check before any SELECT/DML fast path;
        // fallback statement dispatch intentionally checks again on its own
        // immutable catalog generation.
        crate::authorization::authorize_statement(self, plan.statement.as_ref(), ctx)?;
        let reference_expand =
            self.bind_cached_reference_expand(plan.statement.as_ref(), &plan.reference_expand)?;
        if reference_expand.is_some() && matches!(plan.statement.as_ref(), Statement::Select(_)) {
            return self.execute_statement_with_navigation_plan(
                &plan.statement,
                ctx,
                reference_expand,
            );
        }

        // Try compiled fast paths based on statement type
        match plan.statement.as_ref() {
            Statement::Select(stmt) => {
                if let Some(result) = self.try_fast_pk_lookup_compiled(stmt, ctx, &plan.compiled) {
                    return result;
                }
                if let Some(result) = self.try_fast_count_distinct_compiled(stmt, &plan.compiled) {
                    return result;
                }
                if let Some(result) = self.try_fast_count_star_compiled(stmt, &plan.compiled) {
                    return result;
                }
            }
            Statement::Update(stmt) => {
                let updated_columns = stmt
                    .updates
                    .keys()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                let triggers = prepare_dml_triggers(
                    self,
                    stmt.table_name.value_lower.as_str(),
                    DmlTriggerEvent::Update,
                    &updated_columns,
                    ctx,
                )?;
                if triggers.is_empty() && self.active_transaction.lock().unwrap().is_none() {
                    if let Some(result) =
                        self.try_fast_pk_update_compiled(stmt, ctx, &plan.compiled)
                    {
                        return result;
                    }
                }
            }
            Statement::Delete(stmt) => {
                let triggers = prepare_dml_triggers(
                    self,
                    stmt.table_name.value_lower.as_str(),
                    DmlTriggerEvent::Delete,
                    &[],
                    ctx,
                )?;
                if triggers.is_empty() && self.active_transaction.lock().unwrap().is_none() {
                    if let Some(result) =
                        self.try_fast_pk_delete_compiled(stmt, ctx, &plan.compiled)
                    {
                        return result;
                    }
                }
            }
            Statement::Insert(stmt) if self.active_transaction.lock().unwrap().is_none() => {
                return self.execute_insert_with_compiled_cache(stmt, ctx, &plan.compiled);
            }
            _ => {}
        }

        self.execute_statement_after_navigation(&plan.statement, ctx)
    }
}

fn restricted_plugin_error(operation: &str, issues: &[RequirementIssue]) -> Error {
    const MAX_REPORTED_ISSUES: usize = 16;
    let mut details = issues
        .iter()
        .take(MAX_REPORTED_ISSUES)
        .map(|issue| match issue {
            RequirementIssue::MissingPackage { package_id } => {
                format!("missing package {}", hex_package_id(package_id))
            }
            RequirementIssue::PackageVersion {
                package_id,
                required,
                active,
            } => format!(
                "package {} requires version {required}, active version is {active}",
                hex_package_id(package_id)
            ),
            RequirementIssue::PackageAbi {
                package_id,
                required_major,
                required_min_minor,
                required_max_minor,
                active_major,
                active_min_minor,
                active_max_minor,
            } => format!(
                "package {} requires ABI {required_major}.{required_min_minor}..={required_major}.{required_max_minor}, active package declares ABI {active_major}.{active_min_minor}..={active_major}.{active_max_minor}",
                hex_package_id(package_id)
            ),
            RequirementIssue::DescriptorFingerprint { package_id } => format!(
                "package {} descriptor fingerprint differs",
                hex_package_id(package_id)
            ),
            RequirementIssue::MissingOrStaleObject {
                package_id,
                object_id,
                kind,
            } => format!(
                "package {} has missing/stale {:?} object {}",
                hex_package_id(package_id),
                kind,
                hex_package_id(object_id)
            ),
        })
        .collect::<Vec<_>>();
    if issues.len() > MAX_REPORTED_ISSUES {
        details.push(format!(
            "{} additional dependency issues omitted",
            issues.len() - MAX_REPORTED_ISSUES
        ));
    }
    Error::NotSupported(format!(
        "database is in restricted plugin diagnostic mode; {operation} is unavailable: {}",
        details.join("; ")
    ))
}

fn hex_package_id(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Count the number of parameter placeholders in a statement
///
/// Returns (has_params, max_param_index)
#[doc(hidden)]
pub fn count_parameters(stmt: &Statement) -> (bool, usize) {
    crate::dispatch::program::count_parameters(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_storage::mvcc::engine::MVCCEngine;

    fn create_test_executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    #[test]
    fn test_executor_creation() {
        let executor = create_test_executor();
        assert!(executor.function_registry().exists("COUNT"));
        assert!(executor.function_registry().exists("UPPER"));
    }

    #[test]
    fn test_empty_program() {
        let executor = create_test_executor();
        match executor.execute("") {
            Err(error) => assert_eq!(error, Error::NoStatementsToExecute),
            Ok(_) => panic!("empty SQL must be rejected"),
        }
    }

    #[test]
    fn test_create_table() {
        let executor = create_test_executor();
        let result = executor
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        assert_eq!(result.rows_affected(), 0);
    }

    #[test]
    fn test_insert_and_select() {
        let executor = create_test_executor();

        // Create table
        executor
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        // Insert data
        let result = executor
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        assert_eq!(result.rows_affected(), 1);

        // Select data
        let mut result = executor.execute("SELECT * FROM users").unwrap();
        let columns = result.columns();
        assert_eq!(columns.len(), 2);

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert_eq!(row.get(1), Some(&Value::text("Alice")));

        assert!(!result.next());
    }

    #[test]
    fn test_parameterized_query() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();

        let mut result = executor
            .execute_with_params(
                "SELECT * FROM users WHERE id = $1",
                smallvec::smallvec![Value::Integer(1)],
            )
            .unwrap();

        assert!(result.next());
        let row = result.row();
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert!(!result.next());
    }

    #[test]
    fn test_query_cache_basic() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();

        // First execution - should parse and cache
        let stats_before = executor.cache_stats();
        executor.execute("SELECT * FROM users").unwrap();
        let stats_after = executor.cache_stats();
        assert!(stats_after.size > stats_before.size);

        // Second execution - should use cache
        let size_before = executor.cache_stats().size;
        executor.execute("SELECT * FROM users").unwrap();
        let size_after = executor.cache_stats().size;
        assert_eq!(size_before, size_after); // No new entries
    }

    #[test]
    fn test_query_cache_parameterized() {
        let executor = create_test_executor();

        executor
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();

        // Execute with different parameters - should reuse cached plan
        let query = "SELECT * FROM users WHERE id = $1";

        // First execution
        let mut result = executor
            .execute_with_params(query, smallvec::smallvec![Value::Integer(1)])
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(1)));

        // Second execution with different param - should use cache
        let mut result = executor
            .execute_with_params(query, smallvec::smallvec![Value::Integer(2)])
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));
    }

    #[test]
    fn test_query_cache_clear() {
        let executor = create_test_executor();

        executor.execute("SELECT 1").unwrap();
        executor.execute("SELECT 2").unwrap();
        assert!(executor.cache_stats().size > 0);

        executor.clear_cache();
        assert_eq!(executor.cache_stats().size, 0);
    }

    #[test]
    fn test_query_cache_uses_exact_source_identity() {
        let executor = create_test_executor();

        executor.execute("SELECT  1").unwrap();
        let size = executor.cache_stats().size;

        // Distinct source text gets a distinct key unless normalization is lexical.
        executor.execute("SELECT 1").unwrap();
        assert_eq!(executor.cache_stats().size, size + 1);
    }
}
