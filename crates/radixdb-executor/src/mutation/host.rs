//! Internal mutation port used by bounded executor modules.
//!
//! Mutation implementations own their bind/validate/execute/finalize phases
//! here. The host supplies shared executor state and calls back into the single
//! SELECT owner without duplicating that owner in every mutation module.

use std::sync::{Arc, Mutex};

use radixdb_catalog::{CatalogMutationSet, TriggerTiming};
use radixdb_core::{DataType, IsolationLevel, Result, Row, RowVec};
use radixdb_functions::FunctionRegistry;
use radixdb_plugin_host::PluginRegistry;
use radixdb_sql::ast::{
    CreateJobStatement, CreateRoutineStatement, CreateTriggerStatement, Expression,
    SelectStatement, Statement,
};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{QueryResult, Table, Transaction};
use rustc_hash::FxHashMap;

use crate::catalog::DdlTransaction;
use crate::context::ExecutionContext;
use crate::procedural::{CallBoundary, DmlTriggerEvent, DmlTriggerPlan};

/// Explicit transaction state shared by statement routing and mutation owners.
#[doc(hidden)]
pub struct ActiveTransaction {
    pub transaction: Box<dyn Transaction>,
    pub tables: FxHashMap<String, Box<dyn Table>>,
    pub catalog: DdlTransaction,
    catalog_savepoints: FxHashMap<String, DdlTransaction>,
}

impl ActiveTransaction {
    pub fn new(transaction: Box<dyn Transaction>, catalog: DdlTransaction) -> Self {
        Self {
            transaction,
            tables: FxHashMap::default(),
            catalog,
            catalog_savepoints: FxHashMap::default(),
        }
    }

    pub fn create_savepoint(&mut self, name: &str) -> Result<()> {
        self.transaction.create_savepoint(name)?;
        self.catalog_savepoints
            .insert(name.to_owned(), self.catalog.clone());
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        self.transaction.release_savepoint(name)?;
        self.catalog_savepoints.remove(name);
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let catalog = self.catalog_savepoints.get(name).cloned().ok_or_else(|| {
            radixdb_core::Error::invalid_argument(format!(
                "savepoint '{name}' has no catalog snapshot"
            ))
        })?;
        let timestamp = self
            .transaction
            .get_savepoint_timestamp(name)
            .ok_or_else(|| {
                radixdb_core::Error::invalid_argument(format!(
                    "savepoint '{name}' has no storage timestamp"
                ))
            })?;
        for table in self.tables.values() {
            table.rollback_to_timestamp(timestamp);
        }
        self.transaction.rollback_to_savepoint(name)?;
        self.catalog = catalog;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        // The storage transaction must classify the unit before table handles
        // discard their private versions. Otherwise a real DML rollback looks
        // read-only, no abort marker reaches WAL, and reopen may reuse its ID.
        let result = self.transaction.rollback();
        for (_, mut table) in self.tables.drain() {
            table.rollback();
        }
        result
    }

    pub fn stage_catalog_for_commit(&mut self) -> Result<()> {
        if let Some(mutation) = self.catalog.pending_mutation()? {
            self.transaction.stage_catalog_mutation(mutation)?;
        }
        Ok(())
    }

    pub fn has_pending_catalog_changes(&self) -> bool {
        self.catalog.has_pending_catalog_changes()
    }

    pub fn stage_catalog_statement(&mut self, statement: Statement) -> Result<()> {
        self.catalog.stage_statement(statement)
    }
}

/// Neutral SELECT output metadata consumed by CTAS binding.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundQueryColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Narrow callbacks supplied by the concrete executor composition owner.
#[doc(hidden)]
pub trait MutationHost {
    fn mutation_engine(&self) -> &Arc<MVCCEngine>;

    fn mutation_default_isolation_level(&self) -> IsolationLevel;

    fn mutation_function_registry(&self) -> &Arc<FunctionRegistry>;

    fn mutation_plugin_registry(&self) -> &Arc<PluginRegistry>;

    fn mutation_active_transaction(&self) -> &Mutex<Option<ActiveTransaction>>;

    fn mutation_execute_select(
        &self,
        statement: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;

    fn mutation_describe_select_output(
        &self,
        statement: &SelectStatement,
    ) -> Result<Vec<BoundQueryColumn>>;

    fn mutation_compile_routine(
        &self,
        statement: &CreateRoutineStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
        identity: radixdb_procedural::CompileIdentity,
        search_path: Vec<radixdb_catalog::ObjectId>,
    ) -> Result<Vec<radixdb_catalog::ObjectId>>;

    fn mutation_validate_trigger(
        &self,
        statement: &CreateTriggerStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
    ) -> Result<Vec<radixdb_catalog::ObjectId>>;

    fn mutation_bind_job(
        &self,
        statement: &CreateJobStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
        context: &ExecutionContext,
    ) -> Result<crate::catalog::BoundJobDefinition>;

    fn mutation_prepare_dml_triggers(
        &self,
        table_name: &str,
        event: DmlTriggerEvent,
        updated_columns: &[String],
        context: &ExecutionContext,
    ) -> Result<DmlTriggerPlan>;

    fn mutation_begin_trigger_boundary(&self) -> Result<CallBoundary>;

    fn mutation_complete_trigger_boundary(&self, boundary: &CallBoundary) -> Result<()>;

    fn mutation_abort_trigger_boundary(&self, boundary: &CallBoundary) -> Result<()>;

    fn mutation_fire_statement_triggers(
        &self,
        plan: &DmlTriggerPlan,
        timing: TriggerTiming,
        context: &ExecutionContext,
    ) -> Result<()>;

    fn mutation_fire_before_row_triggers(
        &self,
        plan: &DmlTriggerPlan,
        old: Option<&Row>,
        new: Option<Row>,
        row_identity: Option<i64>,
        context: &ExecutionContext,
    ) -> Result<Option<Row>>;

    fn mutation_fire_after_row_triggers(
        &self,
        plan: &DmlTriggerPlan,
        old: Option<&Row>,
        new: Option<&Row>,
        row_identity: Option<i64>,
        context: &ExecutionContext,
    ) -> Result<()>;

    fn mutation_process_where_subqueries(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression>;

    fn mutation_process_correlated_expression(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression>;

    fn mutation_process_correlated_where(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression>;

    fn mutation_optimize_exists_to_semi_join(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
        outer_limit: Option<i64>,
    ) -> Result<Option<Expression>>;

    fn mutation_optimize_in_to_semi_join(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
    ) -> Result<Option<Expression>>;

    fn mutation_has_subqueries(expression: &Expression) -> bool;

    fn mutation_has_correlated_subqueries(expression: &Expression) -> bool;

    fn mutation_materialize_result(result: Box<dyn QueryResult>) -> Result<RowVec>;

    fn mutation_invalidate_query_cache(&self, table_name: &str);

    fn mutation_invalidate_semantic_cache(&self, table_name: &str);

    fn mutation_invalidate_authorization_caches(&self);

    fn mutation_active_transaction_id(&self) -> Option<i64> {
        self.mutation_active_transaction()
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| state.transaction.id())
    }

    fn mutation_has_active_transaction(&self) -> bool {
        self.mutation_active_transaction().lock().unwrap().is_some()
    }

    /// Validate one DDL statement against the transaction-pinned catalog.
    /// Explicit transactions retain the private generation until COMMIT;
    /// auto-commit callers receive the exact mutation to attach to their
    /// storage transaction.
    fn mutation_stage_catalog_statement(
        &self,
        statement: Statement,
    ) -> Result<Option<CatalogMutationSet>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        if let Some(state) = active.as_mut() {
            state.stage_catalog_statement(statement)?;
            return Ok(None);
        }
        drop(active);

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = DdlTransaction::begin_shared_with_plugin_registry(
            generation,
            Arc::clone(self.mutation_plugin_registry()),
        );
        catalog.stage_statement(statement)?;
        catalog.pending_mutation()
    }

    fn mutation_stage_catalog_statement_as(
        &self,
        statement: Statement,
        actor: radixdb_catalog::ObjectId,
        current_database: Option<&str>,
    ) -> Result<Option<CatalogMutationSet>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        if let Some(state) = active.as_mut() {
            state
                .catalog
                .stage_statement_as(statement, actor, current_database)?;
            return Ok(None);
        }
        drop(active);

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = DdlTransaction::begin_shared_with_plugin_registry(
            generation,
            Arc::clone(self.mutation_plugin_registry()),
        );
        catalog.stage_statement_as(statement, actor, current_database)?;
        catalog.pending_mutation()
    }
}
