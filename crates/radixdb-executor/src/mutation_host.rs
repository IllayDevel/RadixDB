//! Internal composition seam for executor-owned mutation phases.

use std::sync::{Arc, Mutex};

use crate::binding::output::OutputBindingExt;
use crate::mutation::host::{ActiveTransaction, BoundQueryColumn, MutationHost};
use crate::subquery::SubqueryExecutorExt;
use radixdb_catalog::TriggerTiming;
use radixdb_core::{IsolationLevel, Result, Row, RowVec};
use radixdb_functions::FunctionRegistry;
use radixdb_plugin_host::PluginRegistry;
use radixdb_sql::ast::{Expression, SelectStatement};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::QueryResult;

use super::Executor;
use crate::context::ExecutionContext;
use crate::procedural::{CallBoundary, DmlTriggerEvent, DmlTriggerPlan};

impl MutationHost for Executor {
    fn mutation_engine(&self) -> &Arc<MVCCEngine> {
        &self.engine
    }

    fn mutation_default_isolation_level(&self) -> IsolationLevel {
        self.default_isolation_level()
    }

    fn mutation_function_registry(&self) -> &Arc<FunctionRegistry> {
        &self.function_registry
    }

    fn mutation_plugin_registry(&self) -> &Arc<PluginRegistry> {
        &self.plugin_registry
    }

    fn mutation_active_transaction(&self) -> &Mutex<Option<ActiveTransaction>> {
        &self.active_transaction
    }

    fn mutation_execute_select(
        &self,
        statement: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select(statement, ctx)
    }

    fn mutation_describe_select_output(
        &self,
        statement: &SelectStatement,
    ) -> Result<Vec<BoundQueryColumn>> {
        self.bind_select_output(statement, &[], 0).map(|columns| {
            columns
                .into_iter()
                .map(|column| BoundQueryColumn {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                })
                .collect()
        })
    }

    fn mutation_compile_routine(
        &self,
        statement: &radixdb_sql::CreateRoutineStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
        identity: radixdb_procedural::CompileIdentity,
        search_path: Vec<radixdb_catalog::ObjectId>,
    ) -> Result<Vec<radixdb_catalog::ObjectId>> {
        crate::procedural::compile_catalog_routine(self, statement, catalog, identity, search_path)
    }

    fn mutation_validate_trigger(
        &self,
        statement: &radixdb_sql::CreateTriggerStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
    ) -> Result<Vec<radixdb_catalog::ObjectId>> {
        crate::procedural::validate_trigger_attachment(self, statement, catalog)
    }

    fn mutation_bind_job(
        &self,
        statement: &radixdb_sql::CreateJobStatement,
        catalog: &radixdb_catalog::CatalogGeneration,
        context: &ExecutionContext,
    ) -> Result<crate::catalog::BoundJobDefinition> {
        crate::procedural::bind_job_definition(self, statement, catalog, context)
    }

    fn mutation_prepare_dml_triggers(
        &self,
        table_name: &str,
        event: DmlTriggerEvent,
        updated_columns: &[String],
        context: &ExecutionContext,
    ) -> Result<DmlTriggerPlan> {
        crate::procedural::prepare_dml_triggers(self, table_name, event, updated_columns, context)
    }

    fn mutation_begin_trigger_boundary(&self) -> Result<CallBoundary> {
        self.begin_procedural_boundary()
    }

    fn mutation_complete_trigger_boundary(&self, boundary: &CallBoundary) -> Result<()> {
        self.complete_procedural_boundary(boundary)
    }

    fn mutation_abort_trigger_boundary(&self, boundary: &CallBoundary) -> Result<()> {
        self.abort_procedural_boundary(boundary)
    }

    fn mutation_fire_statement_triggers(
        &self,
        plan: &DmlTriggerPlan,
        timing: TriggerTiming,
        context: &ExecutionContext,
    ) -> Result<()> {
        crate::procedural::fire_statement_triggers(self, plan, timing, context)
    }

    fn mutation_fire_before_row_triggers(
        &self,
        plan: &DmlTriggerPlan,
        old: Option<&Row>,
        new: Option<Row>,
        row_identity: Option<i64>,
        context: &ExecutionContext,
    ) -> Result<Option<Row>> {
        crate::procedural::fire_before_row_triggers(self, plan, old, new, row_identity, context)
    }

    fn mutation_fire_after_row_triggers(
        &self,
        plan: &DmlTriggerPlan,
        old: Option<&Row>,
        new: Option<&Row>,
        row_identity: Option<i64>,
        context: &ExecutionContext,
    ) -> Result<()> {
        crate::procedural::fire_after_row_triggers(self, plan, old, new, row_identity, context)
    }

    fn mutation_process_where_subqueries(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_where_subqueries(expression, ctx)
    }

    fn mutation_process_correlated_expression(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_correlated_expression(expression, ctx)
    }

    fn mutation_process_correlated_where(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_correlated_where(expression, ctx)
    }

    fn mutation_optimize_exists_to_semi_join(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
        outer_limit: Option<i64>,
    ) -> Result<Option<Expression>> {
        self.try_optimize_exists_to_semi_join(expression, ctx, outer_tables, outer_limit)
    }

    fn mutation_optimize_in_to_semi_join(
        &self,
        expression: &Expression,
        ctx: &ExecutionContext,
        outer_tables: &[String],
    ) -> Result<Option<Expression>> {
        self.try_optimize_in_to_semi_join(expression, ctx, outer_tables)
    }

    fn mutation_has_subqueries(expression: &Expression) -> bool {
        Executor::has_subqueries(expression)
    }

    fn mutation_has_correlated_subqueries(expression: &Expression) -> bool {
        Executor::has_correlated_subqueries(expression)
    }

    fn mutation_materialize_result(result: Box<dyn QueryResult>) -> Result<RowVec> {
        Executor::materialize_result(result)
    }

    fn mutation_invalidate_query_cache(&self, table_name: &str) {
        self.query_cache.invalidate_table(table_name);
    }

    fn mutation_invalidate_semantic_cache(&self, table_name: &str) {
        self.invalidate_semantic_cache(table_name);
    }

    fn mutation_invalidate_authorization_caches(&self) {
        self.query_cache.clear();
        self.semantic_cache.clear();
        self.procedural_cache.clear();
        crate::context::clear_all_thread_local_caches();
    }
}
