//! Internal composition callbacks for executor-owned statement routing.

use crate::aggregation::AggregationHost;
use crate::binding::output::{NavigationOutputBinding, OutputBindingHost};
use crate::binding::source::SourceBindingHost;
use crate::cte::CteHost;
use crate::dispatch::program::{CachedExecutionHost, CachedFastPathHost};
use crate::dispatch::statement::StatementDispatchHost;
use crate::mutation::dml_fast_path::DmlFastPathExt;
use crate::mutation::pk_fast_path::PkFastPathExt;
use crate::navigation::{NavigationExecutorExt, NavigationHost};
use crate::subquery::{SubqueryExecutorExt, SubqueryHost};
use crate::window::WindowHost;
use radixdb_core::{Result, Value};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::*;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{Engine, QueryResult};

use super::navigation::{self, ReferenceExpandPlan};
use super::query_cache::{CachedPlanRef, QueryCache};
use super::Executor;
use crate::context::ExecutionContext;

impl SubqueryHost for Executor {
    fn subquery_engine(&self) -> &std::sync::Arc<MVCCEngine> {
        &self.engine
    }

    fn subquery_open_table(
        &self,
        table_name: &str,
    ) -> Result<crate::access::handle::QueryTableHandle> {
        crate::access::handle::open_query_table_raw(
            &self.engine,
            &self.active_transaction,
            table_name,
        )
    }

    fn subquery_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn radixdb_storage::traits::QueryResult>> {
        self.execute_select(statement, context)
    }
}

impl CteHost for Executor {
    fn cte_function_registry(&self) -> &FunctionRegistry {
        self.function_registry.as_ref()
    }

    fn cte_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select(statement, context)
    }
}

impl NavigationHost for Executor {
    fn navigation_engine(&self) -> &std::sync::Arc<MVCCEngine> {
        &self.engine
    }

    fn navigation_active_transaction(
        &self,
    ) -> &std::sync::Mutex<Option<crate::mutation::host::ActiveTransaction>> {
        &self.active_transaction
    }

    fn navigation_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select(statement, context)
    }

    fn navigation_project_rows_with_alias(
        &self,
        select_expressions: &[Expression],
        rows: radixdb_core::RowVec,
        columns: &[String],
        columns_lower: Option<&[String]>,
        context: &ExecutionContext,
        table_alias: Option<&str>,
    ) -> Result<radixdb_core::RowVec> {
        self.project_rows_with_alias(
            select_expressions,
            rows,
            columns,
            columns_lower,
            context,
            table_alias,
        )
    }

    fn navigation_source_materialized(
        &self,
        plan: &crate::navigation::ReferenceExpandPlan,
        context: &ExecutionContext,
    ) {
        #[cfg(any(test, feature = "test-hooks"))]
        navigation::run_source_materialized_test_hook(plan, context);
        #[cfg(not(any(test, feature = "test-hooks")))]
        let _ = (plan, context);
    }
}

impl WindowHost for Executor {
    fn window_function_registry(&self) -> &FunctionRegistry {
        self.function_registry.as_ref()
    }
}

impl AggregationHost for Executor {
    fn aggregation_engine(&self) -> &std::sync::Arc<MVCCEngine> {
        &self.engine
    }

    fn aggregation_function_registry(&self) -> &FunctionRegistry {
        self.function_registry.as_ref()
    }

    fn aggregation_active_transaction(
        &self,
    ) -> &std::sync::Mutex<Option<crate::mutation::host::ActiveTransaction>> {
        &self.active_transaction
    }

    fn aggregation_process_where_subqueries(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_where_subqueries(expression, context)
    }

    fn aggregation_try_process_select_subqueries(
        &self,
        columns: &[Expression],
        context: &ExecutionContext,
    ) -> Result<Option<Vec<Expression>>> {
        self.try_process_select_subqueries(columns, context)
    }

    fn aggregation_has_correlated_subqueries(&self, expression: &Expression) -> bool {
        Self::has_correlated_subqueries(expression)
    }

    fn aggregation_process_correlated_expression(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_correlated_expression(expression, context)
    }

    fn aggregation_output_column_names(
        &self,
        select_expressions: &[Expression],
        source_columns: &[String],
        table_alias: Option<&str>,
    ) -> Vec<String> {
        self.get_output_column_names(select_expressions, source_columns, table_alias)
    }
}

impl OutputBindingHost for Executor {
    fn output_binding_engine(&self) -> &MVCCEngine {
        self.engine.as_ref()
    }

    fn output_binding_functions(&self) -> &FunctionRegistry {
        self.function_registry.as_ref()
    }

    fn output_binding_type_name(
        &self,
        name: &str,
    ) -> Result<(radixdb_core::DataType, radixdb_core::LogicalTypeRef, String)> {
        let (catalog, _) = crate::procedural::transaction_visible_catalog(self)?;
        let data_type = crate::catalog::bind_catalog_type_in_generation(name, catalog.as_ref())?;
        Ok((
            data_type.logical_type(),
            data_type.logical_type_ref(),
            crate::procedural::catalog_type_name(catalog.as_ref(), data_type),
        ))
    }

    fn output_binding_stored_function(
        &self,
        name: &str,
        argument_types: &[Option<radixdb_core::LogicalTypeRef>],
    ) -> Result<
        Option<(
            radixdb_core::DataType,
            radixdb_core::LogicalTypeRef,
            String,
            bool,
        )>,
    > {
        crate::procedural::function::bind_stored_function_result(self, name, argument_types).map(
            |bound| {
                bound.map(|bound| {
                    (
                        bound.result_type,
                        bound.logical_type,
                        bound.type_name,
                        bound.nullable,
                    )
                })
            },
        )
    }

    fn output_binding_table_schema(
        &self,
        name_lower: &str,
    ) -> Result<radixdb_core::CompactArc<radixdb_core::Schema>> {
        let transaction_id = self
            .active_transaction
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| state.transaction.id());
        transaction_id.map_or_else(
            || self.engine.get_table_schema(name_lower),
            |transaction_id| {
                self.engine
                    .get_table_schema_for_txn(transaction_id, name_lower)
            },
        )
    }

    fn output_binding_view(
        &self,
        name_lower: &str,
    ) -> Result<Option<std::sync::Arc<radixdb_storage::mvcc::ViewDefinition>>> {
        self.visible_view_lowercase(name_lower)
    }

    fn output_binding_navigation(
        &self,
        select: &SelectStatement,
    ) -> Result<Vec<NavigationOutputBinding>> {
        navigation::bind_navigation_paths(self.engine.as_ref(), select).map(|paths| {
            paths
                .into_iter()
                .map(|path| NavigationOutputBinding {
                    display_path: path.display_path().to_string(),
                    terminal_type: path.terminal_type(),
                    nullable: path.nullable(),
                })
                .collect()
        })
    }
}

impl SourceBindingHost for Executor {
    type BindingCache = navigation::CachedReferenceExpand;

    fn source_binding_engine(&self) -> &MVCCEngine {
        self.engine.as_ref()
    }

    fn source_binding_functions(&self) -> &FunctionRegistry {
        self.function_registry.as_ref()
    }

    fn source_binding_query_cache(&self) -> &QueryCache {
        &self.query_cache
    }

    fn source_binding_view(
        &self,
        name_lower: &str,
    ) -> Result<Option<std::sync::Arc<radixdb_storage::mvcc::ViewDefinition>>> {
        self.visible_view_lowercase(name_lower)
    }
}

impl CachedExecutionHost<navigation::CachedReferenceExpand> for Executor {
    fn dispatch_query_cache(&self) -> &QueryCache {
        &self.query_cache
    }

    fn dispatch_execute_bound_plan(
        &self,
        plan: &CachedPlanRef,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_bound_cached_plan(plan, context)
    }

    fn dispatch_execute_statement(
        &self,
        statement: &Statement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_statement(statement, context)
    }
}

impl CachedFastPathHost<navigation::CachedReferenceExpand> for Executor {
    fn dispatch_fast_path_blocked(&self) -> bool {
        self.active_transaction
            .try_lock()
            .map_or(true, |active| active.is_some())
    }

    fn dispatch_fast_path_binding_is_nonempty(&self, plan: &CachedPlanRef) -> Result<bool> {
        Ok(self
            .bind_cached_reference_expand(plan.statement(), plan.binding_cache())?
            .is_some()
            && matches!(plan.statement(), Statement::Select(_)))
    }

    fn dispatch_try_borrowed_param_fast_path(
        &self,
        plan: &CachedPlanRef,
        params: &[Value],
    ) -> Option<Result<Box<dyn QueryResult>>> {
        match plan.statement() {
            Statement::Select(statement) => {
                self.try_fast_pk_lookup_with_params(statement, params, plan.compiled_state())
            }
            Statement::Update(statement) => {
                self.try_fast_pk_update_with_params(statement, params, plan.compiled_state())
            }
            Statement::Delete(statement) => {
                self.try_fast_pk_delete_with_params(statement, params, plan.compiled_state())
            }
            _ => None,
        }
    }
}

impl StatementDispatchHost for Executor {
    type NavigationPlan = ReferenceExpandPlan;

    fn dispatch_ddl_fence_already_held(&self) -> bool {
        self.ddl_fence_already_held
    }

    fn dispatch_authorize_statement(
        &self,
        statement: &Statement,
        context: &ExecutionContext,
    ) -> Result<()> {
        crate::authorization::authorize_statement(self, statement, context)
    }

    fn dispatch_bind_navigation(
        &self,
        statement: &Statement,
    ) -> Result<Option<Self::NavigationPlan>> {
        match statement {
            Statement::Select(select) => {
                navigation::bind_reference_expand_plan(self.engine.as_ref(), select)
            }
            Statement::Explain(explain) => {
                if let Statement::Select(select) = explain.statement.as_ref() {
                    navigation::bind_reference_expand_plan(self.engine.as_ref(), select)?;
                } else {
                    navigation::reject_navigation_in_write_statement(
                        self.engine.as_ref(),
                        statement,
                    )?;
                }
                Ok(None)
            }
            _ => {
                navigation::reject_navigation_in_write_statement(self.engine.as_ref(), statement)?;
                Ok(None)
            }
        }
    }

    fn dispatch_select(
        &self,
        statement: &SelectStatement,
        navigation: Option<&Self::NavigationPlan>,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if let Some(plan) = navigation {
            return self.execute_reference_projection(statement, plan, context);
        }
        if let Some(result) = self.try_fast_pk_lookup(statement, context) {
            return result;
        }
        self.execute_select(statement, context)
    }

    fn dispatch_call(
        &self,
        statement: &CallStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_call_statement(statement, context)
    }

    fn dispatch_set(
        &self,
        statement: &SetStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_set(statement, context)
    }

    fn dispatch_show_tables(
        &self,
        statement: &ShowTablesStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_show_tables(statement, context)
    }

    fn dispatch_show_views(
        &self,
        statement: &ShowViewsStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_show_views(statement, context)
    }

    fn dispatch_show_create_table(
        &self,
        statement: &ShowCreateTableStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_show_create_table(statement, context)
    }

    fn dispatch_show_create_view(
        &self,
        statement: &ShowCreateViewStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_show_create_view(statement, context)
    }

    fn dispatch_show_indexes(
        &self,
        statement: &ShowIndexesStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_show_indexes(statement, context)
    }

    fn dispatch_describe(
        &self,
        statement: &DescribeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_describe(statement, context)
    }

    fn dispatch_pragma(
        &self,
        statement: &PragmaStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_pragma(statement, context)
    }

    fn dispatch_expression(
        &self,
        statement: &ExpressionStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_expression_stmt(statement, context)
    }

    fn dispatch_explain(
        &self,
        statement: &ExplainStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_explain(statement, context)
    }

    fn dispatch_analyze(
        &self,
        statement: &AnalyzeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_analyze(statement, context)
    }

    fn dispatch_vacuum(
        &self,
        statement: &VacuumStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_vacuum(statement, context)
    }
}
