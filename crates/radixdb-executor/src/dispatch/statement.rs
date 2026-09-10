//! Statement classification, fencing, routing, and statement atomicity.

use std::sync::atomic::{AtomicU64, Ordering};

use radixdb_core::{Error, Result};
use radixdb_sql::ast::*;
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;
use crate::mutation::copy::CopyExecutorExt;
use crate::mutation::ddl::DdlExecutorExt;
use crate::mutation::dml::DmlExecutorExt;
use crate::mutation::extension::ExtensionDdlExecutorExt;
use crate::mutation::external_type::ExternalTypeDdlExecutorExt;
use crate::mutation::host::MutationHost;
use crate::mutation::operator::OperatorDdlExecutorExt;

use super::transaction::TransactionControlExt;

static NEXT_STATEMENT_SAVEPOINT_ID: AtomicU64 = AtomicU64::new(1);

/// Internal dispatch seam between statement coordination and concrete owners.
pub trait StatementDispatchHost: MutationHost {
    type NavigationPlan;

    fn dispatch_ddl_fence_already_held(&self) -> bool;

    fn dispatch_authorize_statement(
        &self,
        statement: &Statement,
        context: &ExecutionContext,
    ) -> Result<()>;

    fn dispatch_bind_navigation(
        &self,
        statement: &Statement,
    ) -> Result<Option<Self::NavigationPlan>>;

    fn dispatch_select(
        &self,
        statement: &SelectStatement,
        navigation: Option<&Self::NavigationPlan>,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_call(
        &self,
        statement: &CallStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;

    fn dispatch_set(
        &self,
        statement: &SetStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_show_tables(
        &self,
        statement: &ShowTablesStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_show_views(
        &self,
        statement: &ShowViewsStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_show_create_table(
        &self,
        statement: &ShowCreateTableStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_show_create_view(
        &self,
        statement: &ShowCreateViewStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_show_indexes(
        &self,
        statement: &ShowIndexesStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_describe(
        &self,
        statement: &DescribeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_pragma(
        &self,
        statement: &PragmaStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_expression(
        &self,
        statement: &ExpressionStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_explain(
        &self,
        statement: &ExplainStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_analyze(
        &self,
        statement: &AnalyzeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn dispatch_vacuum(
        &self,
        statement: &VacuumStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
}

pub fn execute_statement<H: StatementDispatchHost + ?Sized>(
    host: &H,
    statement: &Statement,
    context: &ExecutionContext,
    navigation_checked: bool,
    mut navigation: Option<H::NavigationPlan>,
) -> Result<Box<dyn QueryResult>> {
    if matches!(statement, Statement::Expression(_)) {
        return Err(Error::parse(format!(
            "invalid SQL: unrecognised statement: {statement}"
        )));
    }

    let explicit_transaction = host.mutation_has_active_transaction();
    let transaction_end = matches!(statement, Statement::Commit(_) | Statement::Rollback(_));
    let engine_owns_fence = matches!(
        statement,
        Statement::Pragma(value)
            if value.name.value.eq_ignore_ascii_case("SNAPSHOT")
                || value.name.value.eq_ignore_ascii_case("RESTORE")
                || value.name.value.eq_ignore_ascii_case("CHECKPOINT")
    );
    let transaction_owns_fence = matches!(statement, Statement::CreateTable(_))
        || (!explicit_transaction
            && matches!(
                statement,
                Statement::CreateExtension(_)
                    | Statement::DropExtension(_)
                    | Statement::CreateExternalType(_)
                    | Statement::DropExternalType(_)
                    | Statement::CreateOperator(_)
                    | Statement::DropOperator(_)
                    | Statement::CreateOperatorClass(_)
                    | Statement::DropOperatorClass(_)
                    | Statement::CreatePlannerSupport(_)
                    | Statement::DropPlannerSupport(_)
                    | Statement::CreateRoutine(_)
                    | Statement::CreateTrigger(_)
                    | Statement::CreateJob(_)
                    | Statement::DropRoutine(_)
                    | Statement::DropTrigger(_)
                    | Statement::DropJob(_)
                    | Statement::AlterJob(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreatePrincipal(_)
                    | Statement::CreateRole(_)
                    | Statement::AlterSecuritySubject(_)
                    | Statement::DropSecuritySubject(_)
                    | Statement::Grant(_)
                    | Statement::Revoke(_)
                    | Statement::AlterOwner(_)
                    | Statement::CreateIndex(_)
                    | Statement::DropTable(_)
                    | Statement::DropIndex(_)
                    | Statement::AlterIndex(_)
                    | Statement::CreateView(_)
                    | Statement::DropView(_)
            ))
        || matches!(statement, Statement::AlterTable(_));
    let coordinates_nested_statements = matches!(statement, Statement::Analyze(_));
    // Physical DDL takes `ddl_fence` internally, so auto-commit catalog
    // writers use an independent admission fence across the complete pin ->
    // stage -> commit interval. Otherwise two writers can pin the same
    // generation and the loser reports a stale catalog mutation.
    let _catalog_write_fence = (is_catalog_mutation(statement) && !explicit_transaction)
        .then(|| host.mutation_engine().acquire_catalog_write_fence());
    let _catalog_fence = (!host.dispatch_ddl_fence_already_held()
        && !transaction_end
        && !coordinates_nested_statements
        && !engine_owns_fence
        && !transaction_owns_fence)
        .then(|| {
            let exclusive = (is_ddl(statement) && !explicit_transaction)
                || matches!(statement, Statement::Truncate(_));
            host.mutation_engine()
                .acquire_ddl_statement_fence(exclusive)
        });

    host.dispatch_authorize_statement(statement, context)?;

    if !navigation_checked {
        navigation = host.dispatch_bind_navigation(statement)?;
    }

    let _statement_scope = context.enter_statement_scope();
    let _visibility_fence = matches!(statement, Statement::Select(_))
        .then(|| host.mutation_engine().acquire_statement_visibility_fence())
        .flatten();
    let context = host.mutation_active_transaction_id().map_or_else(
        || context.clone(),
        |id| context.with_transaction_id(id as u64),
    );

    let statement_savepoint = create_statement_savepoint(host, statement, explicit_transaction)?;
    let result = route_statement(host, statement, &context, navigation.as_ref());
    finalize_statement_savepoint(host, statement_savepoint, result)
}

fn is_catalog_mutation(statement: &Statement) -> bool {
    is_ddl(statement) && !matches!(statement, Statement::Truncate(_))
}

fn is_ddl(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::CreateExtension(_)
            | Statement::DropExtension(_)
            | Statement::CreateExternalType(_)
            | Statement::DropExternalType(_)
            | Statement::CreateOperator(_)
            | Statement::DropOperator(_)
            | Statement::CreateOperatorClass(_)
            | Statement::DropOperatorClass(_)
            | Statement::CreateRoutine(_)
            | Statement::CreateTrigger(_)
            | Statement::CreateJob(_)
            | Statement::DropRoutine(_)
            | Statement::DropTrigger(_)
            | Statement::DropJob(_)
            | Statement::AlterJob(_)
            | Statement::CreateSchema(_)
            | Statement::CreatePrincipal(_)
            | Statement::CreateRole(_)
            | Statement::AlterSecuritySubject(_)
            | Statement::DropSecuritySubject(_)
            | Statement::Grant(_)
            | Statement::Revoke(_)
            | Statement::AlterOwner(_)
            | Statement::DropTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterIndex(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::Truncate(_)
    )
}

fn route_statement<H: StatementDispatchHost + ?Sized>(
    host: &H,
    statement: &Statement,
    context: &ExecutionContext,
    navigation: Option<&H::NavigationPlan>,
) -> Result<Box<dyn QueryResult>> {
    match statement {
        Statement::CreateExtension(value) => host.execute_create_extension(value, context),
        Statement::DropExtension(value) => host.execute_drop_extension(value, context),
        Statement::CreateExternalType(value) => host.execute_create_external_type(value, context),
        Statement::DropExternalType(value) => host.execute_drop_external_type(value, context),
        Statement::CreateOperator(value) => host.execute_create_operator(value, context),
        Statement::DropOperator(value) => host.execute_drop_operator(value, context),
        Statement::CreateOperatorClass(value) => host.execute_create_operator_class(value, context),
        Statement::DropOperatorClass(value) => host.execute_drop_operator_class(value, context),
        Statement::CreatePlannerSupport(value) => {
            host.execute_create_planner_support(value, context)
        }
        Statement::DropPlannerSupport(value) => host.execute_drop_planner_support(value, context),
        Statement::CreateTable(value) => host.execute_create_table(value, context),
        Statement::CreateRoutine(value) => host.execute_create_routine(value, context),
        Statement::CreateTrigger(value) => host.execute_create_trigger(value, context),
        Statement::CreateJob(value) => host.execute_create_job(value, context),
        Statement::DropRoutine(value) => host.execute_drop_routine(value, context),
        Statement::DropTrigger(value) => host.execute_drop_trigger(value, context),
        Statement::DropJob(value) => host.execute_drop_job(value, context),
        Statement::AlterJob(value) => host.execute_alter_job(value, context),
        Statement::CreateSchema(value) => host.execute_create_schema(value, context),
        Statement::CreatePrincipal(value) => host.execute_create_principal(value, context),
        Statement::CreateRole(value) => host.execute_create_role(value, context),
        Statement::AlterSecuritySubject(value) => {
            host.execute_alter_security_subject(value, context)
        }
        Statement::DropSecuritySubject(value) => host.execute_drop_security_subject(value, context),
        Statement::Grant(value) => host.execute_grant(value, context),
        Statement::Revoke(value) => host.execute_revoke(value, context),
        Statement::AlterOwner(value) => host.execute_alter_owner(value, context),
        Statement::DropTable(value) => host.execute_drop_table(value, context),
        Statement::CreateIndex(value) => host.execute_create_index(value, context),
        Statement::DropIndex(value) => host.execute_drop_index(value, context),
        Statement::AlterTable(value) => host.execute_alter_table(value, context),
        Statement::AlterIndex(value) => host.execute_alter_index(value, context),
        Statement::CreateView(value) => host.execute_create_view(value, context),
        Statement::DropView(value) => host.execute_drop_view(value, context),
        Statement::Insert(value) => host.execute_insert(value, context),
        Statement::Update(value) => host.execute_update(value, context),
        Statement::Delete(value) => host.execute_delete(value, context),
        Statement::Truncate(value) => host.execute_truncate(value, context),
        Statement::Select(value) => host.dispatch_select(value, navigation, context),
        Statement::Call(value) => host.dispatch_call(value, context),
        Statement::Begin(value) => host.execute_begin(value, context),
        Statement::Commit(value) => host.execute_commit_stmt(value, context),
        Statement::Rollback(value) => host.execute_rollback_stmt(value, context),
        Statement::Savepoint(value) => host.execute_savepoint(value, context),
        Statement::ReleaseSavepoint(value) => host.execute_release_savepoint(value, context),
        Statement::Set(value) => host.dispatch_set(value, context),
        Statement::ShowTables(value) => host.dispatch_show_tables(value, context),
        Statement::ShowViews(value) => host.dispatch_show_views(value, context),
        Statement::ShowCreateTable(value) => host.dispatch_show_create_table(value, context),
        Statement::ShowCreateView(value) => host.dispatch_show_create_view(value, context),
        Statement::ShowIndexes(value) => host.dispatch_show_indexes(value, context),
        Statement::Describe(value) => host.dispatch_describe(value, context),
        Statement::Pragma(value) => host.dispatch_pragma(value, context),
        Statement::Expression(value) => host.dispatch_expression(value, context),
        Statement::Explain(value) => host.dispatch_explain(value, context),
        Statement::Analyze(value) => host.dispatch_analyze(value, context),
        Statement::Vacuum(value) => host.dispatch_vacuum(value, context),
        Statement::Copy(value) => host.execute_copy(value, context),
    }
}

fn create_statement_savepoint<H: StatementDispatchHost + ?Sized>(
    host: &H,
    statement: &Statement,
    explicit_transaction: bool,
) -> Result<Option<String>> {
    if !explicit_transaction
        || !matches!(
            statement,
            Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::AlterTable(_)
        )
    {
        return Ok(None);
    }

    let id = NEXT_STATEMENT_SAVEPOINT_ID.fetch_add(1, Ordering::Relaxed);
    let name = format!("\0radixdb-statement-{id}");
    let mut active = host.mutation_active_transaction().lock().unwrap();
    let state = active.as_mut().ok_or_else(|| {
        Error::internal("explicit transaction disappeared before statement execution")
    })?;
    state.create_savepoint(&name)?;
    Ok(Some(name))
}

fn finalize_statement_savepoint<H: StatementDispatchHost + ?Sized>(
    host: &H,
    savepoint: Option<String>,
    result: Result<Box<dyn QueryResult>>,
) -> Result<Box<dyn QueryResult>> {
    let Some(name) = savepoint else {
        return result;
    };
    let mut active = host.mutation_active_transaction().lock().unwrap();
    let state = active.as_mut().ok_or_else(|| {
        Error::internal("explicit transaction disappeared during statement execution")
    })?;

    match result {
        Ok(value) => match state.release_savepoint(&name) {
            Ok(()) => Ok(value),
            Err(release_error) => {
                let rollback_error = state.rollback_to_savepoint(&name).err();
                Err(Error::internal(format!(
                    "statement completed but its atomic savepoint could not be released: {release_error}{}",
                    rollback_error.map_or_else(String::new, |error| format!(
                        "; rollback also failed: {error}"
                    ))
                )))
            }
        },
        Err(statement_error) => {
            if let Err(rollback_error) = state.rollback_to_savepoint(&name) {
                return Err(Error::internal(format!(
                    "statement failed: {statement_error}; atomic rollback failed: {rollback_error}"
                )));
            }
            if let Err(release_error) = state.release_savepoint(&name) {
                return Err(Error::internal(format!(
                    "statement failed: {statement_error}; rolled back but could not release its savepoint: {release_error}"
                )));
            }
            Err(statement_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use radixdb_storage::mvcc::MVCCEngine;
    use radixdb_storage::traits::Engine;

    use crate::Executor;

    #[test]
    fn concurrent_autocommit_catalog_writers_serialize_pin_to_commit() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut writers = Vec::new();

        for writer in 0..2 {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            writers.push(std::thread::spawn(move || {
                let executor = Executor::new(engine);
                barrier.wait();
                for table in 0..64 {
                    executor
                        .execute(&format!(
                            "CREATE TABLE concurrent_catalog_{writer}_{table} (id INTEGER PRIMARY KEY)"
                        ))
                        .unwrap();
                }
            }));
        }

        barrier.wait();
        for writer in writers {
            writer.join().unwrap();
        }

        for writer in 0..2 {
            for table in 0..64 {
                assert!(engine
                    .table_exists(&format!("concurrent_catalog_{writer}_{table}"))
                    .unwrap());
            }
        }
    }
}
