// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! DDL Statement Execution
//!
//! This module implements execution of Data Definition Language (DDL) statements:
//! - CREATE TABLE
//! - DROP TABLE
//! - CREATE INDEX
//! - DROP INDEX
//! - ALTER TABLE
//! - CREATE VIEW
//! - DROP VIEW

use std::sync::Arc;

use super::persistent_value::persistent_value_expression;
use crate::utils::dummy_token_clone;
use radixdb_catalog::ObjectId;
use radixdb_core::{
    DataType, Error, ForeignKeyAction, ForeignKeyConstraint, Result, Row, Schema, SchemaBuilder,
    SchemaColumn, SchemaConstraintKind, Value, ValueSet,
};
use radixdb_sql::ast::*;
use radixdb_storage::traits::{
    Engine, PendingIndexDefinition, PendingIndexDrop, PendingIndexRename, QueryResult,
    SchemaPhysicalTransition, Table, Transaction,
};

fn walk_persistent_expression_mut(
    expression: &mut Expression,
    visitor: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    visitor(expression)?;
    match expression {
        Expression::Prefix(value) => walk_persistent_expression_mut(&mut value.right, visitor)?,
        Expression::Infix(value) => {
            walk_persistent_expression_mut(&mut value.left, visitor)?;
            walk_persistent_expression_mut(&mut value.right, visitor)?;
        }
        Expression::List(value) => {
            for expression in &mut value.elements {
                walk_persistent_expression_mut(expression, visitor)?;
            }
        }
        Expression::Distinct(value) => walk_persistent_expression_mut(&mut value.expr, visitor)?,
        Expression::In(value) => {
            walk_persistent_expression_mut(&mut value.left, visitor)?;
            walk_persistent_expression_mut(&mut value.right, visitor)?;
        }
        Expression::InHashSet(value) => walk_persistent_expression_mut(&mut value.column, visitor)?,
        Expression::Between(value) => {
            walk_persistent_expression_mut(&mut value.expr, visitor)?;
            walk_persistent_expression_mut(&mut value.lower, visitor)?;
            walk_persistent_expression_mut(&mut value.upper, visitor)?;
        }
        Expression::Like(value) => {
            walk_persistent_expression_mut(&mut value.left, visitor)?;
            walk_persistent_expression_mut(&mut value.pattern, visitor)?;
            if let Some(escape) = &mut value.escape {
                walk_persistent_expression_mut(escape, visitor)?;
            }
        }
        Expression::ExpressionList(value) => {
            for expression in &mut value.expressions {
                walk_persistent_expression_mut(expression, visitor)?;
            }
        }
        Expression::Case(value) => {
            if let Some(expression) = &mut value.value {
                walk_persistent_expression_mut(expression, visitor)?;
            }
            for clause in &mut value.when_clauses {
                walk_persistent_expression_mut(&mut clause.condition, visitor)?;
                walk_persistent_expression_mut(&mut clause.then_result, visitor)?;
            }
            if let Some(expression) = &mut value.else_value {
                walk_persistent_expression_mut(expression, visitor)?;
            }
        }
        Expression::Cast(value) => walk_persistent_expression_mut(&mut value.expr, visitor)?,
        Expression::FunctionCall(value) => {
            for argument in &mut value.arguments {
                walk_persistent_expression_mut(argument, visitor)?;
            }
            for order in &mut value.order_by {
                walk_persistent_expression_mut(&mut order.expression, visitor)?;
            }
            if let Some(filter) = &mut value.filter {
                walk_persistent_expression_mut(filter, visitor)?;
            }
        }
        Expression::Aliased(value) => {
            walk_persistent_expression_mut(&mut value.expression, visitor)?
        }
        Expression::Window(value) => {
            for argument in &mut value.function.arguments {
                walk_persistent_expression_mut(argument, visitor)?;
            }
            for order in &mut value.function.order_by {
                walk_persistent_expression_mut(&mut order.expression, visitor)?;
            }
            if let Some(filter) = &mut value.function.filter {
                walk_persistent_expression_mut(filter, visitor)?;
            }
            for partition in &mut value.partition_by {
                walk_persistent_expression_mut(partition, visitor)?;
            }
            for order in &mut value.order_by {
                walk_persistent_expression_mut(&mut order.expression, visitor)?;
            }
        }
        Expression::Exists(_)
        | Expression::AllAny(_)
        | Expression::ScalarSubquery(_)
        | Expression::TableSource(_)
        | Expression::JoinSource(_)
        | Expression::SubquerySource(_)
        | Expression::ValuesSource(_)
        | Expression::CteReference(_)
        | Expression::FunctionTableSource(_) => {
            return Err(Error::NotSupported(
                "subqueries and table sources are not supported in persistent schema expressions"
                    .to_string(),
            ));
        }
        Expression::Identifier(_)
        | Expression::QualifiedIdentifier(_)
        | Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::Default(_) => {}
    }
    Ok(())
}

fn materialize_persistent_expression(
    expression: &Expression,
    ctx: &ExecutionContext,
) -> Result<Expression> {
    let mut expression = expression.clone();
    walk_persistent_expression_mut(&mut expression, &mut |expression| {
        let (token, value) = match expression {
            Expression::Parameter(parameter) => {
                let value = if parameter.name.starts_with(':') {
                    let name = &parameter.name[1..];
                    ctx.get_named_param(name).cloned().ok_or_else(|| {
                        Error::invalid_argument(format!("missing named parameter :{name}"))
                    })?
                } else {
                    ctx.get_param(parameter.index).cloned().ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "missing positional parameter ${}",
                            parameter.index
                        ))
                    })?
                };
                (parameter.token.clone(), value)
            }
            Expression::BoundValue(value) => {
                return persistent_value_expression(value, &dummy_token_clone()).map(|bound| {
                    *expression = bound;
                });
            }
            _ => return Ok(()),
        };
        *expression = persistent_value_expression(&value, &token)?;
        Ok(())
    })?;
    Ok(expression)
}

fn bind_persistent_expression(expression: &Expression, ctx: &ExecutionContext) -> Result<String> {
    Ok(materialize_persistent_expression(expression, ctx)?.to_string())
}

fn bind_index_option_value(expression: &Expression, ctx: &ExecutionContext) -> Result<String> {
    let expression = materialize_persistent_expression(expression, ctx)?;
    if let Expression::Identifier(identifier) = &expression {
        return Ok(identifier.value_lower.to_string());
    }
    let value = ExpressionEval::compile(&expression, &[])?
        .with_context(ctx)
        .eval_slice(&Row::new())?;
    match value {
        Value::Integer(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        Value::Text(value) => Ok(value.to_string()),
        Value::Boolean(value) => Ok(value.to_string()),
        other => Err(Error::invalid_argument(format!(
            "index option must be a finite scalar value, got {other:?}"
        ))),
    }
}

fn materialize_catalog_create_table(
    statement: &CreateTableStatement,
    ctx: &ExecutionContext,
) -> Result<CreateTableStatement> {
    let mut statement = statement.clone();
    for column in &mut statement.columns {
        for constraint in &mut column.constraints {
            match constraint {
                ColumnConstraint::Default(expression) | ColumnConstraint::Check(expression) => {
                    *expression = materialize_persistent_expression(expression, ctx)?;
                }
                _ => {}
            }
        }
    }
    for constraint in &mut statement.table_constraints {
        if let TableConstraint::Check(expression) = constraint {
            **expression = materialize_persistent_expression(expression, ctx)?;
        }
    }
    Ok(statement)
}

fn materialize_catalog_create_index(
    statement: &CreateIndexStatement,
    ctx: &ExecutionContext,
) -> Result<CreateIndexStatement> {
    let mut statement = statement.clone();
    for (_, expression) in &mut statement.options {
        *expression = materialize_persistent_expression(expression, ctx)?;
    }
    if let Some(predicate) = &mut statement.where_clause {
        **predicate = materialize_persistent_expression(predicate, ctx)?;
    }
    Ok(statement)
}

fn generated_catalog_index(table_name: &str, index_name: &str, columns: &[String]) -> Statement {
    Statement::CreateIndex(CreateIndexStatement {
        token: dummy_token_clone(),
        index_name: Identifier::new(dummy_token_clone(), index_name.to_owned()),
        table_name: Identifier::new(dummy_token_clone(), table_name.to_owned()),
        columns: columns
            .iter()
            .map(|column| Identifier::new(dummy_token_clone(), column.clone()))
            .collect(),
        is_unique: false,
        if_not_exists: false,
        index_method: None,
        options: Vec::new(),
        where_clause: None,
        operator_class: None,
    })
}

fn stage_generated_foreign_key_indexes(
    catalog: &mut crate::catalog::DdlTransaction,
    table_name: &str,
    columns: &[String],
    actor: ObjectId,
    current_database: Option<&str>,
) -> Result<()> {
    for column in columns {
        let index_name = format!("fk_{table_name}_{column}");
        catalog.stage_statement_as(
            generated_catalog_index(table_name, &index_name, std::slice::from_ref(column)),
            actor,
            current_database,
        )?;
    }
    Ok(())
}

/// Validate a foreign key reference and build a `ForeignKeyConstraint`.
///
/// Checks: parent table exists, referenced column exists and is PK/UNIQUE,
/// FK column exists in the schema being built.
#[allow(clippy::too_many_arguments)]
fn validate_fk_reference(
    engine: &dyn Engine,
    transaction_parent: Option<&dyn Table>,
    schema_builder: &SchemaBuilder,
    fk_col_name: &str,
    fk_col_display: &str,
    ref_table_lower: &str,
    ref_table_display: &str,
    current_table_lower: &str,
    ref_col_opt: Option<&str>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
) -> Result<ForeignKeyConstraint> {
    let fk_col_idx = schema_builder.column_index(fk_col_name).ok_or_else(|| {
        Error::internal(format!(
            "foreign key column '{}' not found in table definition",
            fk_col_display
        ))
    })?;
    let fk_col_def = schema_builder
        .column_definition(fk_col_idx)
        .ok_or_else(|| {
            Error::internal(format!(
                "foreign key column '{}' has no declared type",
                fk_col_display
            ))
        })?;

    if ref_table_lower == current_table_lower {
        let (ref_col_idx, ref_col_def) = if let Some(ref_col_name) = ref_col_opt {
            let ref_col_idx = schema_builder.column_index(ref_col_name).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "foreign key references non-existent column '{}' in table '{}'",
                    ref_col_name, ref_table_display
                ))
            })?;
            let ref_col_def = schema_builder
                .column_definition(ref_col_idx)
                .ok_or_else(|| {
                    Error::internal("self-referencing FK column disappeared during schema binding")
                })?;
            (ref_col_idx, ref_col_def)
        } else {
            schema_builder.primary_key_column().ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "table '{}' has no single primary key for FK reference default",
                    ref_table_display
                ))
            })?
        };
        if !ref_col_def.primary_key {
            return Err(Error::InvalidArgument(format!(
                "self-referencing foreign key on '{}' must reference the table PRIMARY KEY; '{}' is not PRIMARY KEY",
                fk_col_display, ref_col_def.name
            )));
        }
        if !fk_col_def.has_same_declared_type(ref_col_def) {
            return Err(Error::InvalidArgument(format!(
                "foreign key column '{}' has type {}, but referenced column '{}.{}' has type {}",
                fk_col_display,
                fk_col_def.formatted_data_type(),
                ref_table_display,
                ref_col_def.name,
                ref_col_def.formatted_data_type()
            )));
        }
        if (matches!(on_delete, ForeignKeyAction::SetNull)
            || matches!(on_update, ForeignKeyAction::SetNull))
            && !schema_builder.is_column_nullable(fk_col_idx)
        {
            return Err(Error::InvalidArgument(format!(
                "foreign key column '{}' has SET NULL action but is NOT NULL",
                fk_col_display
            )));
        }
        return Ok(ForeignKeyConstraint {
            column_index: fk_col_idx,
            column_name: fk_col_name.to_string(),
            referenced_table: current_table_lower.to_string(),
            referenced_column: schema_builder
                .column_definition(ref_col_idx)
                .expect("bound self-reference column")
                .name_lower
                .clone(),
            on_delete,
            on_update,
        });
    }

    // Validate parent table exists
    if transaction_parent.is_none() && !engine.table_exists(ref_table_lower)? {
        return Err(Error::internal(format!(
            "foreign key on column '{}' references non-existent table '{}'",
            fk_col_display, ref_table_display
        )));
    }

    let parent_schema = if let Some(parent) = transaction_parent {
        parent.schema().clone()
    } else {
        engine.get_table_schema(ref_table_lower)?.as_ref().clone()
    };

    // Resolve referenced column (defaults to PK if not specified)
    let ref_col_name = if let Some(rc) = ref_col_opt {
        rc.to_string()
    } else {
        let pk_indices = parent_schema.primary_key_indices();
        if pk_indices.len() == 1 {
            parent_schema.columns[pk_indices[0]].name.to_lowercase()
        } else {
            return Err(Error::internal(format!(
                "table '{}' has no primary key for FK reference default",
                ref_table_display
            )));
        }
    };

    // Validate referenced column exists and is PK or has unique index
    let (ref_col_idx, ref_col_def) = parent_schema.find_column(&ref_col_name).ok_or_else(|| {
        Error::internal(format!(
            "foreign key references non-existent column '{}' in table '{}'",
            ref_col_name, ref_table_display
        ))
    })?;

    if !ref_col_def.primary_key {
        let has_unique = if let Some(parent) = transaction_parent {
            parent.get_indexes().iter().any(|idx| {
                idx.is_unique()
                    && idx.partial_predicate().is_none()
                    && idx.column_ids().len() == 1
                    && idx.column_ids()[0] as usize == ref_col_idx
            })
        } else {
            engine
                .get_all_indexes(ref_table_lower)
                .map(|indexes| {
                    indexes.iter().any(|idx| {
                        idx.is_unique()
                            && idx.partial_predicate().is_none()
                            && idx.column_ids().len() == 1
                            && idx.column_ids()[0] as usize == ref_col_idx
                    })
                })
                .unwrap_or(false)
        };

        if !has_unique {
            return Err(Error::internal(format!(
                "foreign key on '{}' references column '{}' in '{}' which is neither PRIMARY KEY nor UNIQUE",
                fk_col_display, ref_col_name, ref_table_display
            )));
        }
    }

    if !fk_col_def.has_same_declared_type(ref_col_def) {
        return Err(Error::InvalidArgument(format!(
            "foreign key column '{}' has type {}, but referenced column '{}.{}' has type {}",
            fk_col_display,
            fk_col_def.formatted_data_type(),
            ref_table_display,
            ref_col_name,
            ref_col_def.formatted_data_type()
        )));
    }

    // Reject SET NULL action on NOT NULL columns (would always fail at runtime)
    if (matches!(on_delete, ForeignKeyAction::SetNull)
        || matches!(on_update, ForeignKeyAction::SetNull))
        && !schema_builder.is_column_nullable(fk_col_idx)
    {
        return Err(Error::internal(format!(
            "foreign key column '{}' has ON {} SET NULL but is NOT NULL",
            fk_col_display,
            if matches!(on_delete, ForeignKeyAction::SetNull) {
                "DELETE"
            } else {
                "UPDATE"
            }
        )));
    }

    Ok(ForeignKeyConstraint {
        column_index: fk_col_idx,
        column_name: fk_col_name.to_string(),
        referenced_table: ref_table_lower.to_string(),
        referenced_column: ref_col_name,
        on_delete,
        on_update,
    })
}

fn catalog_create_table_statement(schema: &Schema) -> Result<Statement> {
    fn quote_identifier(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\"\""))
    }

    let columns = schema
        .columns
        .iter()
        .map(|column| {
            format!(
                "{} {}{}",
                quote_identifier(&column.name),
                column.formatted_data_type(),
                if column.nullable { "" } else { " NOT NULL" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "CREATE TABLE {} ({columns})",
        quote_identifier(&schema.table_name)
    );
    let mut statements =
        radixdb_sql::parse_sql(&sql).map_err(|error| Error::Parse(error.to_string()))?;
    if statements.len() != 1 || !matches!(statements.first(), Some(Statement::CreateTable(_))) {
        return Err(Error::internal(
            "CTAS schema did not produce one catalog CREATE TABLE statement",
        ));
    }
    Ok(statements.pop().expect("single catalog statement exists"))
}

use crate::context::{
    invalidate_in_subquery_cache_for_table, invalidate_scalar_subquery_cache_for_table,
    invalidate_semi_join_cache_for_table, ExecutionContext,
};
use crate::expression::ExpressionEval;
use crate::mutation::host::MutationHost;
use crate::mutation::validation::{
    compile_table_check_constraints, validate_resulting_row_constraints,
};
use crate::result::ExecResult;

#[doc(hidden)]
pub trait DdlExecutorExt: MutationHost {
    fn execute_create_schema(
        &self,
        stmt: &CreateSchemaStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::CreateSchema(stmt.clone()), ctx)
    }

    fn execute_create_principal(
        &self,
        stmt: &CreatePrincipalStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::CreatePrincipal(stmt.clone()), ctx)
    }

    fn execute_create_role(
        &self,
        stmt: &CreateRoleStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::CreateRole(stmt.clone()), ctx)
    }

    fn execute_alter_security_subject(
        &self,
        stmt: &AlterSecuritySubjectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::AlterSecuritySubject(stmt.clone()), ctx)
    }

    fn execute_drop_security_subject(
        &self,
        stmt: &DropSecuritySubjectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::DropSecuritySubject(stmt.clone()), ctx)
    }

    fn execute_grant(
        &self,
        stmt: &GrantStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::Grant(Box::new(stmt.clone())), ctx)
    }

    fn execute_revoke(
        &self,
        stmt: &RevokeStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::Revoke(Box::new(stmt.clone())), ctx)
    }

    fn execute_alter_owner(
        &self,
        stmt: &AlterOwnerStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::AlterOwner(Box::new(stmt.clone())), ctx)
    }

    fn execute_drop_routine(
        &self,
        stmt: &DropRoutineStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::DropRoutine(Box::new(stmt.clone())), ctx)
    }

    fn execute_drop_trigger(
        &self,
        stmt: &DropTriggerStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::DropTrigger(Box::new(stmt.clone())), ctx)
    }

    fn execute_drop_job(
        &self,
        stmt: &DropJobStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::DropJob(Box::new(stmt.clone())), ctx)
    }

    fn execute_alter_job(
        &self,
        stmt: &AlterJobStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_security_catalog_statement(Statement::AlterJob(Box::new(stmt.clone())), ctx)
    }

    fn execute_security_catalog_statement(
        &self,
        statement: Statement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mutation = self.mutation_stage_catalog_statement_as(
            statement,
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        if let Some(mutation) = mutation {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }
        self.mutation_invalidate_authorization_caches();
        Ok(Box::new(ExecResult::empty()))
    }

    /// Compile and publish a stored function or procedure definition.
    ///
    /// The compiler sees exactly the immutable generation that will receive
    /// the definition. A failed bind/verify step therefore cannot leak a
    /// catalog object, and an explicit transaction keeps the accepted object
    /// private until its ordinary commit boundary.
    fn execute_create_routine(
        &self,
        stmt: &CreateRoutineStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if stmt.native.is_some() {
            return self.execute_create_native_function(stmt, ctx);
        }
        let active_catalog = {
            let active = self.mutation_active_transaction().lock().unwrap();
            active.as_ref().map(|state| {
                (
                    state.catalog.clone(),
                    state.catalog.working_generation_shared(),
                )
            })
        };

        if let Some((mut catalog, pinned_working)) = active_catalog {
            self.compile_and_stage_routine(stmt, &mut catalog, ctx.effective_principal_id())?;
            let mut active = self.mutation_active_transaction().lock().unwrap();
            let state = active.as_mut().ok_or_else(|| {
                Error::internal("explicit transaction disappeared during routine compilation")
            })?;
            if !state.catalog.shares_working_generation(&pinned_working) {
                return Err(Error::InvalidArgument(
                    "transaction-private catalog changed during routine compilation; retry the statement"
                        .to_string(),
                ));
            }
            state.catalog = catalog;
            return Ok(Box::new(ExecResult::empty()));
        }

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        self.compile_and_stage_routine(stmt, &mut catalog, ctx.effective_principal_id())?;
        if let Some(mutation) = catalog.pending_mutation()? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }
        Ok(Box::new(ExecResult::empty()))
    }

    fn execute_create_native_function(
        &self,
        stmt: &CreateRoutineStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let statement = Statement::CreateRoutine(Box::new(stmt.clone()));
        let active_catalog = {
            let active = self.mutation_active_transaction().lock().unwrap();
            active.as_ref().map(|state| {
                (
                    state.catalog.clone(),
                    state.catalog.working_generation_shared(),
                )
            })
        };
        if let Some((mut catalog, pinned_working)) = active_catalog {
            catalog.stage_statement_as(
                statement,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut active = self.mutation_active_transaction().lock().unwrap();
            let state = active.as_mut().ok_or_else(|| {
                Error::internal("explicit transaction disappeared during native function binding")
            })?;
            if !state.catalog.shares_working_generation(&pinned_working) {
                return Err(Error::InvalidArgument(
                    "transaction-private catalog changed during native function binding; retry the statement"
                        .to_owned(),
                ));
            }
            state.catalog = catalog;
            return Ok(Box::new(ExecResult::empty()));
        }

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared_with_plugin_registry(
            generation,
            Arc::clone(self.mutation_plugin_registry()),
        );
        catalog.stage_statement_as(
            statement,
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        if let Some(mutation) = catalog.pending_mutation()? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }
        Ok(Box::new(ExecResult::empty()))
    }

    fn compile_and_stage_routine(
        &self,
        stmt: &CreateRoutineStatement,
        catalog: &mut crate::catalog::DdlTransaction,
        actor: ObjectId,
    ) -> Result<()> {
        let (identity, search_path) = catalog.prepare_routine_compile(stmt)?;
        let object_id = identity.object_id;
        let dependencies = self.mutation_compile_routine(
            stmt,
            catalog.working_generation(),
            identity,
            search_path,
        )?;
        catalog.stage_compiled_routine_as(stmt, object_id, dependencies, actor)
    }

    fn execute_create_trigger(
        &self,
        stmt: &CreateTriggerStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let active_catalog = {
            let active = self.mutation_active_transaction().lock().unwrap();
            active.as_ref().map(|state| {
                (
                    state.catalog.clone(),
                    state.catalog.working_generation_shared(),
                )
            })
        };
        if let Some((mut catalog, pinned_working)) = active_catalog {
            self.mutation_validate_trigger(stmt, catalog.working_generation())?;
            catalog.stage_statement_as(
                Statement::CreateTrigger(Box::new(stmt.clone())),
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut active = self.mutation_active_transaction().lock().unwrap();
            let state = active.as_mut().ok_or_else(|| {
                Error::internal("explicit transaction disappeared during trigger compilation")
            })?;
            if !state.catalog.shares_working_generation(&pinned_working) {
                return Err(Error::InvalidArgument(
                    "transaction-private catalog changed during trigger compilation; retry the statement"
                        .to_string(),
                ));
            }
            state.catalog = catalog;
            return Ok(Box::new(ExecResult::empty()));
        }

        let generation = self.mutation_engine().pin_catalog()?;
        self.mutation_validate_trigger(stmt, generation.as_ref())?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        catalog.stage_statement_as(
            Statement::CreateTrigger(Box::new(stmt.clone())),
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        if let Some(mutation) = catalog.pending_mutation()? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }
        Ok(Box::new(ExecResult::empty()))
    }

    fn execute_create_job(
        &self,
        stmt: &CreateJobStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let active_catalog = {
            let active = self.mutation_active_transaction().lock().unwrap();
            active.as_ref().map(|state| {
                (
                    state.catalog.clone(),
                    state.catalog.working_generation_shared(),
                )
            })
        };
        if let Some((mut catalog, pinned_working)) = active_catalog {
            let definition = self.mutation_bind_job(stmt, catalog.working_generation(), ctx)?;
            catalog.stage_bound_job_as(stmt, definition, ctx.effective_principal_id())?;
            let mut active = self.mutation_active_transaction().lock().unwrap();
            let state = active.as_mut().ok_or_else(|| {
                Error::internal("explicit transaction disappeared during job binding")
            })?;
            if !state.catalog.shares_working_generation(&pinned_working) {
                return Err(Error::InvalidArgument(
                    "transaction-private catalog changed during job binding; retry the statement"
                        .to_string(),
                ));
            }
            state.catalog = catalog;
            return Ok(Box::new(ExecResult::empty()));
        }

        let generation = self.mutation_engine().pin_catalog()?;
        let definition = self.mutation_bind_job(stmt, generation.as_ref(), ctx)?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        catalog.stage_bound_job_as(stmt, definition, ctx.effective_principal_id())?;
        if let Some(mutation) = catalog.pending_mutation()? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }
        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute a CREATE TABLE statement
    fn execute_create_table(
        &self,
        stmt: &CreateTableStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;

        // Check both the published catalog and this transaction's private
        // CREATE TABLE overlay. Other transactions cannot see the latter.
        let exists_in_active_transaction = {
            let active_tx = self.mutation_active_transaction().lock().unwrap();
            active_tx
                .as_ref()
                .is_some_and(|tx_state| tx_state.transaction.get_table(table_name).is_ok())
        };
        if exists_in_active_transaction || self.mutation_engine().table_exists(table_name)? {
            if stmt.if_not_exists {
                return Ok(Box::new(ExecResult::empty()));
            }
            return Err(Error::TableAlreadyExists(table_name.to_string()));
        }

        // Check if a view with the same name exists
        if self.mutation_engine().view_exists(table_name)? {
            return Err(Error::internal(format!(
                "cannot create table '{}': a view with the same name exists",
                table_name
            )));
        }

        // Handle CREATE TABLE ... AS SELECT ...
        if let Some(ref select_stmt) = stmt.as_select {
            if self.mutation_has_active_transaction() {
                return Err(Error::NotSupported(
                    "CREATE TABLE AS SELECT is not supported inside an explicit transaction"
                        .to_string(),
                ));
            }
            return self.execute_create_table_as_select(
                table_name,
                select_stmt,
                stmt.if_not_exists,
                ctx,
            );
        }

        // Build schema from column definitions
        let mut schema_builder = SchemaBuilder::new(table_name.as_str());

        let mut table_primary_key: Option<String> = None;
        for constraint in &stmt.table_constraints {
            if let TableConstraint::PrimaryKey(columns) = constraint {
                if columns.len() != 1 {
                    return Err(Error::NotSupported(
                        "composite PRIMARY KEY is not supported; use a UNIQUE multi-column constraint"
                            .to_string(),
                    ));
                }
                if table_primary_key
                    .replace(columns[0].value_lower.to_string())
                    .is_some()
                {
                    return Err(Error::InvalidArgument(
                        "table declares more than one PRIMARY KEY constraint".to_string(),
                    ));
                }
            }
        }
        let column_primary_keys = stmt
            .columns
            .iter()
            .filter(|column| {
                column
                    .constraints
                    .iter()
                    .any(|constraint| matches!(constraint, ColumnConstraint::PrimaryKey))
            })
            .count();
        if column_primary_keys + usize::from(table_primary_key.is_some()) > 1 {
            return Err(Error::InvalidArgument(
                "table declares more than one PRIMARY KEY".to_string(),
            ));
        }
        let mut table_primary_key_found = table_primary_key.is_none();

        // Collect columns with UNIQUE constraints to create indexes after table creation
        let mut unique_columns: Vec<String> = Vec::new();

        for col_def in &stmt.columns {
            let col_name = &col_def.name.value;
            let (data_type, vector_dimensions, decimal_precision, decimal_scale, external_type) =
                super::type_binding::parse_schema_column_type(self, &col_def.data_type)?;
            let nullable = !col_def
                .constraints
                .iter()
                .any(|c| matches!(c, ColumnConstraint::NotNull));
            let is_primary_key = col_def
                .constraints
                .iter()
                .any(|c| matches!(c, ColumnConstraint::PrimaryKey))
                || table_primary_key
                    .as_deref()
                    .is_some_and(|primary_key| primary_key == col_def.name.value_lower.as_str());
            table_primary_key_found |= table_primary_key
                .as_deref()
                .is_some_and(|primary_key| primary_key == col_def.name.value_lower.as_str());

            // Validate PRIMARY KEY type. INTEGER remains the physical row_id fast path;
            // UUID is a logical primary key backed by an auto-created unique index.
            if is_primary_key && !matches!(data_type, DataType::Integer | DataType::Uuid) {
                return Err(Error::Parse(format!(
                    "PRIMARY KEY column '{}' must be INTEGER or UUID type, got {:?}.",
                    col_name, data_type
                )));
            }

            let is_unique = col_def
                .constraints
                .iter()
                .any(|c| matches!(c, ColumnConstraint::Unique));

            let is_auto_increment = col_def
                .constraints
                .iter()
                .any(|c| matches!(c, ColumnConstraint::AutoIncrement));

            // DEFAULT is a singleton column constraint. Keeping the first of
            // two declarations would publish a different schema than the SQL.
            let mut default_expr = None;
            for constraint in &col_def.constraints {
                if let ColumnConstraint::Default(expr) = constraint {
                    if default_expr
                        .replace(bind_persistent_expression(expr, ctx)?)
                        .is_some()
                    {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' has more than one DEFAULT constraint",
                            col_name
                        )));
                    }
                }
            }

            let default_value = if let Some(ref expr_str) = default_expr {
                if external_type.is_some() {
                    return Err(Error::NotSupported(format!(
                        "DEFAULT for external column '{}' requires an explicit native/text constructor",
                        col_name
                    )));
                }
                let val = self.evaluate_default_expression(expr_str, data_type)?;
                if val.is_null() {
                    None
                } else {
                    Some(val)
                }
            } else {
                None
            };

            // A SchemaColumn has one CHECK owner. Silently retaining only the
            // first expression would publish a weaker schema than the SQL.
            let mut check_expr = None;
            for constraint in &col_def.constraints {
                if let ColumnConstraint::Check(expr) = constraint {
                    if check_expr
                        .replace(bind_persistent_expression(expr, ctx)?)
                        .is_some()
                    {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' has more than one CHECK constraint",
                            col_name
                        )));
                    }
                }
            }

            // Use add_with_constraints to include DEFAULT and CHECK
            schema_builder = schema_builder.add_with_constraints(
                col_name.as_str(),
                data_type,
                nullable && !is_primary_key,
                is_primary_key,
                is_auto_increment,
                default_expr,
                check_expr,
            );
            schema_builder = schema_builder.set_last_default_value(default_value);

            if let Some((type_ref, sql_name)) = external_type {
                schema_builder = schema_builder.set_last_external_type(type_ref, sql_name);
            }

            if vector_dimensions > 0 {
                schema_builder = schema_builder.set_last_vector_dimensions(vector_dimensions);
            }
            if decimal_precision > 0 {
                schema_builder =
                    schema_builder.set_last_decimal_parameters(decimal_precision, decimal_scale);
            }

            if is_auto_increment && !matches!(data_type, DataType::Integer | DataType::Uuid) {
                return Err(Error::Parse(format!(
                    "AUTO_INCREMENT column '{}' must be INTEGER or UUID type, got {:?}.",
                    col_name, data_type
                )));
            }

            // PRIMARY KEY uniqueness is owned by storage for every admitted
            // physical type. Only an independent UNIQUE constraint needs a
            // second index here.
            if is_unique && !is_primary_key {
                unique_columns.push(col_name.to_string());
            }
        }
        if !table_primary_key_found {
            return Err(Error::ColumnNotFound(
                table_primary_key.expect("missing table primary-key name"),
            ));
        }

        // Collect foreign key constraints from column-level REFERENCES
        for col_def in &stmt.columns {
            for constraint in &col_def.constraints {
                if let ColumnConstraint::References {
                    table: ref ref_table,
                    column: ref ref_col,
                    on_delete,
                    on_update,
                } = constraint
                {
                    let transaction_parent = {
                        let active_tx = self.mutation_active_transaction().lock().unwrap();
                        active_tx.as_ref().and_then(|tx_state| {
                            tx_state.transaction.get_table(&ref_table.value_lower).ok()
                        })
                    };
                    let fk = validate_fk_reference(
                        self.mutation_engine().as_ref(),
                        transaction_parent.as_deref(),
                        &schema_builder,
                        col_def.name.value_lower.as_str(),
                        &col_def.name.value,
                        &ref_table.value_lower,
                        &ref_table.value,
                        &stmt.table_name.value_lower,
                        ref_col.as_ref().map(|rc| rc.value_lower.as_str()),
                        *on_delete,
                        *on_update,
                    )?;
                    schema_builder = schema_builder.add_foreign_key(fk);
                }
            }
        }

        // Collect table-level constraints. CHECK expressions are schema-owned
        // full-row predicates; keeping them out of SchemaColumn prevents the
        // historic single-column evaluator from silently changing semantics.
        let mut table_unique_constraints: Vec<Vec<String>> = Vec::new();
        for constraint in &stmt.table_constraints {
            match constraint {
                TableConstraint::Unique(cols) => {
                    let col_names: Vec<String> = cols.iter().map(|c| c.value.to_string()).collect();
                    table_unique_constraints.push(col_names);
                }
                TableConstraint::ForeignKey(fk) => {
                    let transaction_parent = {
                        let active_tx = self.mutation_active_transaction().lock().unwrap();
                        active_tx.as_ref().and_then(|tx_state| {
                            tx_state
                                .transaction
                                .get_table(&fk.ref_table.value_lower)
                                .ok()
                        })
                    };
                    let fk_constraint = validate_fk_reference(
                        self.mutation_engine().as_ref(),
                        transaction_parent.as_deref(),
                        &schema_builder,
                        fk.column.value_lower.as_str(),
                        &fk.column.value,
                        &fk.ref_table.value_lower,
                        &fk.ref_table.value,
                        &stmt.table_name.value_lower,
                        fk.ref_column.as_ref().map(|rc| rc.value_lower.as_str()),
                        fk.on_delete,
                        fk.on_update,
                    )?;
                    schema_builder = schema_builder.add_foreign_key(fk_constraint);
                }
                TableConstraint::Check(expression) => {
                    schema_builder = schema_builder
                        .add_table_check(bind_persistent_expression(expression, ctx)?);
                }
                TableConstraint::PrimaryKey(_) => {}
            }
        }

        let mut schema = schema_builder.build();
        if let Some(primary_key) = schema.primary_key_columns().first() {
            schema.register_primary_key_constraint(vec![primary_key.name.clone()])?;
        }
        for column in schema.columns.clone() {
            if let Some(expression) = column.check_expr {
                schema.register_check_constraint(Some(column.name), expression)?;
            }
        }
        let mut named_unique_constraints = Vec::new();
        for column in &unique_columns {
            let columns = vec![column.clone()];
            let name = schema.register_unique_constraint(columns.clone())?;
            named_unique_constraints.push((name, columns));
        }
        for columns in &table_unique_constraints {
            let name = schema.register_unique_constraint(columns.clone())?;
            named_unique_constraints.push((name, columns.clone()));
        }
        for foreign_key in schema.foreign_keys.clone() {
            schema.register_foreign_key_constraint(&foreign_key)?;
        }
        for expression in schema.table_checks.clone() {
            schema.register_check_constraint(None, expression)?;
        }
        schema.validate_structural_invariants()?;
        // DDL must fail closed: an invalid/unknown-column CHECK is rejected
        // before a catalog or WAL entry can be created.
        compile_table_check_constraints(&schema)?;
        // The runtime schema and logical catalog must name the table with one
        // durable identity. Storage otherwise assigns an independent identity
        // while reserving the pending table, making the first V6 seal unable to
        // resolve its DATA column layout.
        schema.ensure_catalog_identity();
        let table_catalog_id = ObjectId::from_user_bytes(schema.catalog_id()).map_err(|error| {
            Error::internal(format!(
                "generated table catalog identity was rejected: {error}"
            ))
        })?;

        // Collect FK columns that need auto-created indexes (skip PK and UNIQUE columns)
        let mut fk_index_columns: Vec<String> = Vec::new();
        for fk in &schema.foreign_keys {
            let col = &schema.columns[fk.column_index];
            // Skip if the column is already a PK (has PkIndex) or UNIQUE (gets a unique index above)
            if col.primary_key {
                continue;
            }
            let col_lower = col.name.to_lowercase();
            if unique_columns.iter().any(|u| u.to_lowercase() == col_lower) {
                continue;
            }
            fk_index_columns.push(col.name.clone());
        }

        let catalog_statement =
            Statement::CreateTable(materialize_catalog_create_table(stmt, ctx)?);

        // Check if there's an active transaction
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();

        if let Some(ref mut tx_state) = *active_tx {
            // Bind against a private clone first. A failed physical statement
            // must not advance transaction-local catalog visibility.
            let mut catalog = tx_state.catalog.clone();
            catalog.stage_statement_with_object_ids_as(
                catalog_statement,
                [table_catalog_id],
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            stage_generated_foreign_key_indexes(
                &mut catalog,
                table_name,
                &fk_index_columns,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;

            // Use the active transaction for DDL (allows rollback)
            tx_state.transaction.create_table(table_name, schema)?;

            // Create unique indexes for columns with UNIQUE constraint
            for (index_name, columns) in &named_unique_constraints {
                tx_state
                    .transaction
                    .create_table_index(table_name, index_name, columns, true)?;
            }

            // Auto-create indexes on FK columns for efficient referential integrity checks
            for col_name in &fk_index_columns {
                let index_name = format!("fk_{}_{}", table_name, col_name);
                tx_state.transaction.create_table_index(
                    table_name,
                    &index_name,
                    std::slice::from_ref(col_name),
                    false,
                )?;
            }
            tx_state.catalog = catalog;
        } else {
            // One private transaction owns the table and every generated index
            // until a single commit marker is durable. Any validation/build/WAL
            // error before that marker rolls the whole pending object back.
            let generation = self.mutation_engine().pin_catalog()?;
            let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
            catalog.stage_statement_with_object_ids_as(
                catalog_statement,
                [table_catalog_id],
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            stage_generated_foreign_key_indexes(
                &mut catalog,
                table_name,
                &fk_index_columns,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.create_table(table_name, schema)?;

            for (index_name, columns) in &named_unique_constraints {
                transaction.create_table_index(table_name, index_name, columns, true)?;
            }
            for col_name in &fk_index_columns {
                let index_name = format!("fk_{}_{}", table_name, col_name);
                transaction.create_table_index(
                    table_name,
                    &index_name,
                    std::slice::from_ref(col_name),
                    false,
                )?;
            }
            if let Some(mutation) = catalog.pending_mutation()? {
                transaction.stage_catalog_mutation(mutation)?;
            }
            transaction.commit()?;
        }

        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute CREATE TABLE ... AS SELECT ...
    fn execute_create_table_as_select(
        &self,
        table_name: &str,
        select_stmt: &SelectStatement,
        _if_not_exists: bool,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        use radixdb_core::Row;

        if self.mutation_has_active_transaction() {
            return Err(Error::NotSupported(
                "CREATE TABLE AS SELECT is not supported inside an explicit transaction"
                    .to_string(),
            ));
        }

        // Bind output types from the SELECT graph before executing a row. This
        // makes empty/all-NULL/first-NULL results deterministic and prevents a
        // late row from changing or invalidating an already published schema.
        let bound_columns = self.mutation_describe_select_output(select_stmt)?;

        // Execute the SELECT query to get the result
        // Use execute_select for full query processing (DISTINCT, ORDER BY, etc.)
        let mut result = self.mutation_execute_select(select_stmt, ctx)?;
        let columns: Vec<String> = result.columns().to_vec();
        if columns.len() != bound_columns.len() {
            return Err(Error::internal(format!(
                "CTAS binder produced {} columns but SELECT produced {}",
                bound_columns.len(),
                columns.len()
            )));
        }

        // Build schema from bind-time metadata; runtime values never own type
        // selection, even when the first or every row is NULL.
        let mut schema_builder = SchemaBuilder::new(table_name);

        for (col_name, bound) in columns.iter().zip(&bound_columns) {
            // Extract base column name (without table prefix)
            let base_name = if let Some(pos) = col_name.rfind('.') {
                &col_name[pos + 1..]
            } else {
                col_name.as_str()
            };

            schema_builder = schema_builder.add_nullable(base_name, bound.data_type);
        }

        let mut schema = schema_builder.build();
        schema.ensure_catalog_identity();
        let table_catalog_id = ObjectId::from_user_bytes(schema.catalog_id()).map_err(|error| {
            Error::internal(format!(
                "generated CTAS catalog identity was rejected: {error}"
            ))
        })?;
        let catalog_statement = catalog_create_table_statement(&schema)?;

        // Table publication and CTAS rows share one transaction marker. A row
        // failure cannot leave an empty durable table behind.
        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        catalog.stage_statement_with_object_ids_as(
            catalog_statement,
            [table_catalog_id],
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        let mut tx = self.mutation_engine().begin_transaction()?;
        let mut table = tx.create_table(table_name, schema)?;
        let mut rows_count = 0usize;
        while result.next() {
            let row: Row = result.take_row();
            let _ = table.insert(row)?;
            rows_count += 1;
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }
        if let Some(mutation) = catalog.pending_mutation()? {
            tx.stage_catalog_mutation(mutation)?;
        }
        tx.commit()?;

        Ok(Box::new(ExecResult::with_rows_affected(rows_count as i64)))
    }

    /// Execute a DROP TABLE statement
    fn execute_drop_table(
        &self,
        stmt: &DropTableStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;

        // Resolve transaction-private CREATE before the shared catalog. This
        // lets CREATE→DROP collapse to a net-zero journal entry.
        let private_exists = {
            let active_tx = self.mutation_active_transaction().lock().unwrap();
            active_tx
                .as_ref()
                .is_some_and(|state| state.transaction.get_table(table_name).is_ok())
        };
        if !private_exists && !self.mutation_engine().table_exists(table_name)? {
            if stmt.if_exists {
                return Ok(Box::new(ExecResult::empty()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }

        // Check if there's an active transaction (peek at txn_id for FK visibility)
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();
        let txn_id = active_tx.as_ref().map(|s| s.transaction.id());

        // Check FK constraints: block DROP if child tables reference this table
        // Uses the caller's transaction (if any) so uncommitted child deletes are visible
        crate::mutation::foreign_key::check_no_referencing_rows(
            self.mutation_engine(),
            table_name,
            txn_id,
        )?;

        let catalog_statement = Statement::DropTable(stmt.clone());

        if let Some(ref mut tx_state) = *active_tx {
            let mut catalog = tx_state.catalog.clone();
            catalog.stage_statement_as(
                catalog_statement,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            // The transaction records a private DROP intent. Shared catalog,
            // rows and cold artifacts remain intact until its commit marker is
            // durable; rollback simply discards the intent.
            tx_state.transaction.drop_table(table_name)?;
            tx_state.catalog = catalog;
        } else {
            let generation = self.mutation_engine().pin_catalog()?;
            let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
            catalog.stage_statement_as(
                catalog_statement,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.drop_table(table_name)?;
            if let Some(mutation) = catalog.pending_mutation()? {
                transaction.stage_catalog_mutation(mutation)?;
            }
            transaction.commit()?;
        }

        // Invalidate query cache for this table (schema no longer exists)
        self.mutation_invalidate_query_cache(table_name);
        self.mutation_invalidate_semantic_cache(table_name);
        invalidate_semi_join_cache_for_table(table_name);
        invalidate_scalar_subquery_cache_for_table(table_name);
        invalidate_in_subquery_cache_for_table(table_name);

        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute a CREATE INDEX statement
    fn execute_create_index(
        &self,
        stmt: &CreateIndexStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;
        let index_name = &stmt.index_name.value;

        // Resolve through the owning transaction first so CREATE INDEX can
        // target a CREATE TABLE that is still private to the same transaction.
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();
        let mut validation_tx = if active_tx.is_none() {
            Some(self.mutation_engine().begin_transaction()?)
        } else {
            None
        };

        // Determine index type
        let is_unique = stmt.is_unique;

        // Get table to validate columns exist
        let table = if let Some(tx_state) = active_tx.as_ref() {
            tx_state.transaction.get_table(table_name)?
        } else {
            validation_tx
                .as_ref()
                .expect("validation transaction exists")
                .get_table(table_name)?
        };
        let schema = table.schema();

        // Validate columns
        for col_id in &stmt.columns {
            let col_name = &col_id.value;
            if !schema
                .column_index_map()
                .contains_key(col_id.value_lower.as_str())
            {
                return Err(Error::ColumnNotFound(col_name.to_string()));
            }
        }

        // Collect column names
        let column_names: Vec<String> = stmt.columns.iter().map(|c| c.value.to_string()).collect();

        // Convert USING clause IndexMethod to core IndexType
        let requested_index_type = stmt.index_method.map(|method| match method {
            radixdb_sql::ast::IndexMethod::BTree => radixdb_core::IndexType::BTree,
            radixdb_sql::ast::IndexMethod::Hash => radixdb_core::IndexType::Hash,
            radixdb_sql::ast::IndexMethod::Bitmap => radixdb_core::IndexType::Bitmap,
            radixdb_sql::ast::IndexMethod::Hnsw => radixdb_core::IndexType::Hnsw,
        });
        let normalized_index_type = if column_names.len() > 1 {
            radixdb_core::IndexType::MultiColumn
        } else if let Some(index_type) = requested_index_type {
            index_type
        } else {
            match schema
                .find_column(&column_names[0])
                .expect("CREATE INDEX column was validated")
                .1
                .data_type
            {
                DataType::Text | DataType::Json | DataType::Bytes => radixdb_core::IndexType::Hash,
                DataType::Boolean => radixdb_core::IndexType::Bitmap,
                DataType::Vector => radixdb_core::IndexType::Hnsw,
                _ => radixdb_core::IndexType::BTree,
            }
        };

        // HNSW only supports single-column indexes — reject multi-column early
        if requested_index_type == Some(radixdb_core::IndexType::Hnsw) && column_names.len() > 1 {
            return Err(Error::invalid_argument(
                "HNSW index must be on a single vector column; multi-column HNSW indexes are not supported",
            ));
        }

        let partial_predicate = if let Some(where_clause) = &stmt.where_clause {
            if normalized_index_type == radixdb_core::IndexType::Hnsw {
                return Err(Error::invalid_argument(
                    "partial HNSW indexes are not supported",
                ));
            }
            let where_clause = materialize_persistent_expression(where_clause, ctx)?;
            Some(crate::mutation::partial_index::bind_from_ast(
                &where_clause,
                schema,
            )?)
        } else {
            None
        };
        // Extract HNSW-specific options from WITH clause
        let mut hnsw_m: Option<u16> = None;
        let mut hnsw_ef_construction: Option<u16> = None;
        let mut hnsw_ef_search: Option<u16> = None;
        let mut hnsw_distance_metric: Option<u8> = None;
        let is_hnsw = normalized_index_type == radixdb_core::IndexType::Hnsw;
        if !stmt.options.is_empty() && !is_hnsw {
            return Err(Error::invalid_argument(
                "CREATE INDEX WITH options are supported only for HNSW indexes",
            ));
        }
        for (key, expression) in &stmt.options {
            let value = bind_index_option_value(expression, ctx)?;
            match key.as_str() {
                "m" => {
                    let v = value.parse::<u16>().map_err(|_| {
                        Error::invalid_argument(format!(
                            "invalid value for HNSW option 'm': '{}' (expected integer >= 2)",
                            value
                        ))
                    })?;
                    if v < 2 {
                        return Err(Error::invalid_argument(format!(
                            "HNSW option 'm' must be >= 2, got {}",
                            v
                        )));
                    }
                    hnsw_m = Some(v);
                }
                "ef_construction" => {
                    let parsed = value.parse::<u16>().map_err(|_| {
                        Error::invalid_argument(format!(
                            "invalid value for HNSW option 'ef_construction': '{}' (expected positive integer)",
                            value
                        ))
                    })?;
                    if parsed == 0 {
                        return Err(Error::invalid_argument(
                            "HNSW option 'ef_construction' must be greater than zero",
                        ));
                    }
                    hnsw_ef_construction = Some(parsed);
                }
                "ef_search" => {
                    let parsed = value.parse::<u16>().map_err(|_| {
                        Error::invalid_argument(format!(
                            "invalid value for HNSW option 'ef_search': '{}' (expected positive integer)",
                            value
                        ))
                    })?;
                    if parsed == 0 {
                        return Err(Error::invalid_argument(
                            "HNSW option 'ef_search' must be greater than zero",
                        ));
                    }
                    hnsw_ef_search = Some(parsed);
                }
                "metric" | "distance" => {
                    let metric = radixdb_storage::index::HnswDistanceMetric::from_name(
                        &value.to_lowercase(),
                    )
                    .ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "unknown HNSW distance metric '{}' (expected: l2, cosine, or ip)",
                            value
                        ))
                    })?;
                    hnsw_distance_metric = Some(metric.as_u8());
                }
                other if is_hnsw => {
                    return Err(Error::invalid_argument(format!(
                        "unknown HNSW index option '{}' (valid options: m, ef_construction, ef_search, metric)",
                        other
                    )));
                }
                _ => {}
            }
        }

        if is_hnsw {
            if hnsw_m.is_none() {
                let dims = schema
                    .find_column(&column_names[0])
                    .map(|(_, col)| col.vector_dimensions as usize)
                    .unwrap_or(0);
                hnsw_m = Some(radixdb_storage::index::default_m_for_dims(dims) as u16);
            }
            let m = hnsw_m.unwrap() as usize;
            hnsw_ef_construction
                .get_or_insert(radixdb_storage::index::default_ef_construction(m) as u16);
            hnsw_ef_search.get_or_insert(radixdb_storage::index::default_ef_search(m) as u16);
            hnsw_distance_metric.get_or_insert(0);
        }

        // IF NOT EXISTS suppresses only the exact normalized definition under
        // the requested name. Column coverage by another object is irrelevant.
        if let Some(existing) = table.get_index(index_name) {
            if stmt.if_not_exists {
                let requested_predicate = partial_predicate.as_ref().map(|p| p.canonical_sql());
                let existing_predicate = existing.partial_predicate().map(|p| p.canonical_sql());
                let requested_columns = column_names
                    .iter()
                    .map(|name| name.to_lowercase())
                    .collect::<Vec<_>>();
                let existing_columns = existing
                    .column_names()
                    .iter()
                    .map(|name| name.to_lowercase())
                    .collect::<Vec<_>>();
                let hnsw_options_match = normalized_index_type != radixdb_core::IndexType::Hnsw
                    || (existing.hnsw_m() == hnsw_m
                        && existing.hnsw_ef_construction() == hnsw_ef_construction
                        && existing
                            .default_ef_search()
                            .and_then(|value| u16::try_from(value).ok())
                            == hnsw_ef_search
                        && existing.hnsw_distance_metric() == hnsw_distance_metric);
                if existing_columns == requested_columns
                    && existing.is_unique() == is_unique
                    && existing.index_type() == normalized_index_type
                    && existing_predicate == requested_predicate
                    && hnsw_options_match
                {
                    return Ok(Box::new(ExecResult::empty()));
                }
                return Err(Error::internal(format!(
                    "index already exists with different definition: {}",
                    index_name
                )));
            }
            return Err(Error::internal(format!(
                "index already exists: {}",
                index_name
            )));
        }

        let definition = radixdb_storage::traits::PendingIndexDefinition {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            columns: column_names,
            is_unique,
            index_type: Some(normalized_index_type),
            hnsw_m,
            hnsw_ef_construction,
            hnsw_ef_search,
            hnsw_distance_metric,
            partial_predicate,
            key_encoder: None,
        };
        let catalog_statement =
            Statement::CreateIndex(materialize_catalog_create_index(stmt, ctx)?);
        if let Some(tx_state) = active_tx.as_mut() {
            let mut catalog = tx_state.catalog.clone();
            catalog.stage_statement_as(
                catalog_statement,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut definition = definition;
            let (index_type, key_encoder) = crate::catalog::bind_pending_index_semantics(
                catalog.working_generation(),
                index_name,
                self.mutation_plugin_registry().as_ref(),
            )?;
            definition.index_type = Some(index_type);
            definition.key_encoder = key_encoder;
            tx_state.transaction.stage_create_index(definition)?;
            tx_state.catalog = catalog;
        } else {
            let generation = self.mutation_engine().pin_catalog()?;
            let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
            catalog.stage_statement_as(
                catalog_statement,
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            let mut definition = definition;
            let (index_type, key_encoder) = crate::catalog::bind_pending_index_semantics(
                catalog.working_generation(),
                index_name,
                self.mutation_plugin_registry().as_ref(),
            )?;
            definition.index_type = Some(index_type);
            definition.key_encoder = key_encoder;
            let mut transaction = validation_tx
                .take()
                .expect("autocommit CREATE INDEX owns a validation transaction");
            transaction.stage_create_index(definition)?;
            if let Some(mutation) = catalog.pending_mutation()? {
                transaction.stage_catalog_mutation(mutation)?;
            }
            transaction.commit()?;
        }

        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute a DROP INDEX statement
    fn execute_drop_index(
        &self,
        stmt: &DropIndexStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if self.mutation_active_transaction().lock().unwrap().is_some() {
            return Err(Error::NotSupported(
                "DROP INDEX is not supported inside an explicit transaction".to_string(),
            ));
        }
        let index_name = &stmt.index_name.value;

        // Get table name if specified
        let table_name = match &stmt.table_name {
            Some(t) => t.value.to_string(),
            None => {
                return Err(Error::InvalidArgument(
                    "DROP INDEX requires table name".to_string(),
                ))
            }
        };

        // Check if table exists
        if !self.mutation_engine().table_exists(&table_name)? {
            if stmt.if_exists {
                return Ok(Box::new(ExecResult::empty()));
            }
            return Err(Error::TableNotFound(table_name));
        }

        // Check if index exists
        if !self
            .mutation_engine()
            .index_exists(index_name, &table_name)?
        {
            if stmt.if_exists {
                return Ok(Box::new(ExecResult::empty()));
            }
            return Err(Error::IndexNotFound(index_name.to_string()));
        }

        let mut tx = self.mutation_engine().begin_transaction()?;
        let table = tx.get_table(&table_name)?;
        let dropped_index = table
            .get_index(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // A referenced non-PK key must retain at least one full UNIQUE
        // constraint for the complete parent domain. Partial indexes are not
        // FK backing, and dropping the last full owner would make parent
        // identity ambiguous for checks and cascades.
        if dropped_index.is_unique()
            && dropped_index.partial_predicate().is_none()
            && dropped_index.column_ids().len() == 1
        {
            let schema = table.schema();
            let dropped_column = dropped_index.column_ids()[0] as usize;
            if let Some(column) = schema.columns.get(dropped_column) {
                let referenced = self
                    .mutation_engine()
                    .find_referencing_fks(&table_name.to_lowercase())
                    .iter()
                    .any(|(_, fk)| {
                        fk.referenced_column
                            .eq_ignore_ascii_case(column.name.as_str())
                    });
                let has_other_backing = column.primary_key
                    || table.get_indexes().iter().any(|candidate| {
                        candidate.name() != dropped_index.name()
                            && candidate.is_unique()
                            && candidate.partial_predicate().is_none()
                            && candidate.column_ids() == dropped_index.column_ids()
                    });
                if referenced && !has_other_backing {
                    return Err(Error::InvalidArgument(format!(
                        "cannot drop UNIQUE index '{}' because foreign keys reference '{}.{}'",
                        index_name, table_name, column.name
                    )));
                }
            }
        }

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        catalog.stage_statement_as(
            Statement::DropIndex(stmt.clone()),
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        tx.stage_drop_index(PendingIndexDrop {
            table_name,
            index_name: index_name.to_string(),
            schema_owned: false,
        })?;
        if let Some(mutation) = catalog.pending_mutation()? {
            tx.stage_catalog_mutation(mutation)?;
        }
        tx.commit()?;

        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute an ALTER INDEX statement.
    ///
    /// Current supported form:
    /// ALTER INDEX old_name RENAME TO new_name
    ///
    /// Index names are resolved globally because the syntax intentionally does
    /// not require `ON table`. If the old name is ambiguous, or the new name is
    /// already used anywhere, the operation is rejected.
    fn execute_alter_index(
        &self,
        stmt: &AlterIndexStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if self.mutation_active_transaction().lock().unwrap().is_some() {
            return Err(Error::NotSupported(
                "ALTER INDEX is not supported inside an explicit transaction".to_string(),
            ));
        }
        let old_index_name = &stmt.index_name.value;
        let new_index_name = &stmt.new_index_name.value;

        let mut tx = self.mutation_engine().begin_transaction()?;
        let tables = tx.list_tables()?;

        let mut owner_table: Option<String> = None;
        for table_name in &tables {
            if self
                .mutation_engine()
                .index_exists(new_index_name, table_name)?
            {
                return Err(Error::InvalidArgument(format!(
                    "index already exists: {}",
                    new_index_name
                )));
            }

            if self
                .mutation_engine()
                .index_exists(old_index_name, table_name)?
            {
                if owner_table.is_some() {
                    return Err(Error::InvalidArgument(format!(
                        "index name is ambiguous: {}",
                        old_index_name
                    )));
                }
                owner_table = Some(table_name.clone());
            }
        }

        let table_name =
            owner_table.ok_or_else(|| Error::IndexNotFound(old_index_name.to_string()))?;

        let generation = self.mutation_engine().pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        catalog.stage_statement_as(
            Statement::AlterIndex(stmt.clone()),
            ctx.effective_principal_id(),
            ctx.current_database(),
        )?;
        tx.stage_rename_index(PendingIndexRename {
            table_name,
            old_index_name: old_index_name.to_string(),
            new_index_name: new_index_name.to_string(),
        })?;
        if let Some(mutation) = catalog.pending_mutation()? {
            tx.stage_catalog_mutation(mutation)?;
        }
        tx.commit()?;

        Ok(Box::new(ExecResult::empty()))
    }

    fn alter_index_definition(
        table_name: &str,
        name: String,
        columns: Vec<String>,
        is_unique: bool,
    ) -> PendingIndexDefinition {
        PendingIndexDefinition {
            table_name: table_name.to_string(),
            index_name: name,
            columns,
            is_unique,
            index_type: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_distance_metric: None,
            partial_predicate: None,
            key_encoder: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_alter_foreign_key(
        &self,
        transaction: &dyn Transaction,
        schema: &Schema,
        column_name: &str,
        referenced_table: &Identifier,
        referenced_column: Option<&Identifier>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    ) -> Result<ForeignKeyConstraint> {
        let schema_builder = SchemaBuilder::from_schema(schema);
        let transaction_parent = transaction.get_table(&referenced_table.value_lower).ok();
        validate_fk_reference(
            self.mutation_engine().as_ref(),
            transaction_parent.as_deref(),
            &schema_builder,
            &column_name.to_lowercase(),
            column_name,
            &referenced_table.value_lower,
            &referenced_table.value,
            &schema.table_name_lower,
            referenced_column.map(|column| column.value_lower.as_str()),
            on_delete,
            on_update,
        )
    }

    fn validate_alter_schema_rows(
        &self,
        transaction: &dyn Transaction,
        table: &dyn Table,
        schema: &Schema,
    ) -> Result<()> {
        let compiled_checks = compile_table_check_constraints(schema)?;
        let mut parent_domains = Vec::with_capacity(schema.foreign_keys.len());
        for fk in &schema.foreign_keys {
            let parent = transaction.get_table(&fk.referenced_table)?;
            let parent_column = parent
                .schema()
                .get_column_index(&fk.referenced_column)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "foreign key references missing column '{}.{}'",
                        fk.referenced_table, fk.referenced_column
                    ))
                })?;
            let mut scanner = parent.scan_exact_projection(&[parent_column], None)?;
            let mut values = ValueSet::default();
            while scanner.next() {
                let (_, row) = scanner.take_row_with_id()?;
                if let Some(value) = row.get(0).filter(|value| !value.is_null()) {
                    values.insert(value.clone());
                }
            }
            if let Some(error) = scanner.err() {
                return Err(error.clone());
            }
            parent_domains.push(values);
        }

        let active_projection: Vec<usize> = (0..table.schema().columns.len()).collect();
        let mut scanner = table.scan_exact_projection(&active_projection, None)?;
        let mut vm = crate::expression::ExprVM::new();
        while scanner.next() {
            let (_, mut row) = scanner.take_row_with_id()?;
            while row.len() < schema.columns.len() {
                let column = &schema.columns[row.len()];
                row.push(
                    column
                        .default_value
                        .clone()
                        .unwrap_or_else(|| Value::Null(column.data_type)),
                );
            }
            validate_resulting_row_constraints(schema, &compiled_checks, &row, &mut vm)?;
            for (fk, parent_values) in schema.foreign_keys.iter().zip(&parent_domains) {
                let Some(value) = row.get(fk.column_index).filter(|value| !value.is_null()) else {
                    continue;
                };
                if !parent_values.contains(value) {
                    return Err(Error::foreign_key_violation(
                        &schema.table_name,
                        &fk.column_name,
                        &fk.referenced_table,
                        &fk.referenced_column,
                        format!(
                            "referenced row with {} = {} does not exist",
                            fk.referenced_column, value
                        ),
                    ));
                }
            }
        }
        if let Some(error) = scanner.err() {
            return Err(error.clone());
        }
        Ok(())
    }

    fn stage_alter_schema_operation(
        &self,
        transaction: &mut dyn Transaction,
        catalog: &mut crate::catalog::DdlTransaction,
        stmt: &AlterTableStatement,
        ctx: &ExecutionContext,
    ) -> Result<()> {
        let table_name = stmt.table_name.value.as_str();
        if stmt.operation == AlterTableOperation::RenameTable {
            let new_name = stmt.new_table_name.as_ref().ok_or_else(|| {
                Error::InvalidArgument("RENAME TABLE requires new table name".to_string())
            })?;
            catalog.stage_statement_as(
                Statement::AlterTable(Box::new(stmt.clone())),
                ctx.effective_principal_id(),
                ctx.current_database(),
            )?;
            transaction.rename_table(table_name, new_name.value.as_str())?;
            return Ok(());
        }
        let table = transaction.get_table(table_name)?;
        let mut schema = table.schema().clone();
        let table_is_empty = table
            .collect_rows_with_limit_unordered(None, 1, 0)?
            .is_empty();
        let mut indexes = Vec::new();
        let mut requires_row_normalization = false;

        match stmt.operation {
            AlterTableOperation::AddColumn | AlterTableOperation::ModifyColumn => {
                let col_def = stmt.column_def.as_ref().ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "{:?} requires column definition",
                        stmt.operation
                    ))
                })?;
                let (data_type, vector_dimensions, decimal_precision, decimal_scale, external_type) =
                    super::type_binding::parse_schema_column_type(self, &col_def.data_type)?;
                let is_add = stmt.operation == AlterTableOperation::AddColumn;
                let existing_index = schema.get_column_index(&col_def.name.value);
                if is_add && existing_index.is_some() {
                    return Err(Error::DuplicateColumn);
                }
                let target_index = if is_add {
                    schema.columns.len()
                } else {
                    existing_index
                        .ok_or_else(|| Error::ColumnNotFound(col_def.name.value.to_string()))?
                };
                let old_column = (!is_add).then(|| schema.columns[target_index].clone());
                let previous_check_expr = old_column
                    .as_ref()
                    .and_then(|column| column.check_expr.clone());

                let mut not_null = false;
                let mut primary_key = false;
                let mut unique = false;
                let mut auto_increment = false;
                let mut default_expr = None;
                let mut check_expr = None;
                let mut reference = None;
                for constraint in &col_def.constraints {
                    match constraint {
                        ColumnConstraint::NotNull => not_null = true,
                        ColumnConstraint::PrimaryKey => primary_key = true,
                        ColumnConstraint::Unique => unique = true,
                        ColumnConstraint::AutoIncrement => auto_increment = true,
                        ColumnConstraint::Default(expression) => {
                            if default_expr
                                .replace(bind_persistent_expression(expression, ctx)?)
                                .is_some()
                            {
                                return Err(Error::InvalidArgument(format!(
                                    "column '{}' has more than one DEFAULT constraint",
                                    col_def.name.value
                                )));
                            }
                        }
                        ColumnConstraint::Check(expression) => {
                            if check_expr
                                .replace(bind_persistent_expression(expression, ctx)?)
                                .is_some()
                            {
                                return Err(Error::InvalidArgument(format!(
                                    "column '{}' has more than one CHECK constraint",
                                    col_def.name.value
                                )));
                            }
                        }
                        ColumnConstraint::References {
                            table,
                            column,
                            on_delete,
                            on_update,
                        } => {
                            if reference
                                .replace((table, column.as_ref(), *on_delete, *on_update))
                                .is_some()
                            {
                                return Err(Error::InvalidArgument(format!(
                                    "column '{}' has more than one REFERENCES constraint",
                                    col_def.name.value
                                )));
                            }
                        }
                    }
                }
                let adds_primary_key =
                    primary_key && old_column.as_ref().is_none_or(|column| !column.primary_key);
                let adds_auto_increment = auto_increment
                    && old_column
                        .as_ref()
                        .is_none_or(|column| !column.auto_increment);
                if (adds_primary_key || adds_auto_increment) && !table_is_empty {
                    return Err(Error::InvalidArgument(format!(
                        "ALTER TABLE cannot add PRIMARY KEY or AUTO_INCREMENT to populated table '{}'; rebuild the table explicitly",
                        table_name
                    )));
                }
                if adds_primary_key
                    && schema
                        .primary_key_indices()
                        .iter()
                        .any(|&index| index != target_index)
                {
                    return Err(Error::InvalidArgument(
                        "table already has a PRIMARY KEY".to_string(),
                    ));
                }
                if primary_key && !matches!(data_type, DataType::Integer | DataType::Uuid) {
                    return Err(Error::InvalidArgument(format!(
                        "PRIMARY KEY column '{}' must be INTEGER or UUID",
                        col_def.name.value
                    )));
                }
                if auto_increment && !matches!(data_type, DataType::Integer | DataType::Uuid) {
                    return Err(Error::InvalidArgument(format!(
                        "AUTO_INCREMENT column '{}' must be INTEGER or UUID",
                        col_def.name.value
                    )));
                }
                let changes_type_contract = !is_add
                    && (schema.columns[target_index].data_type != data_type
                        || schema.columns[target_index].external_type
                            != external_type.as_ref().map(|(type_ref, _)| *type_ref)
                        || schema.columns[target_index].vector_dimensions != vector_dimensions
                        || schema.columns[target_index].decimal_precision != decimal_precision
                        || schema.columns[target_index].decimal_scale != decimal_scale);
                if changes_type_contract && !table_is_empty {
                    return Err(Error::InvalidArgument(format!(
                        "ALTER TABLE cannot change the declared type of populated column '{}'; rebuild the table explicitly",
                        col_def.name.value
                    )));
                }

                let default_value = if let Some(expression) = &default_expr {
                    if external_type.is_some() {
                        return Err(Error::NotSupported(format!(
                            "DEFAULT for external column '{}' requires an explicit native/text constructor",
                            col_def.name.value
                        )));
                    }
                    let value = self.evaluate_default_expression(expression, data_type)?;
                    (!value.is_null()).then_some(value)
                } else {
                    None
                };
                if is_add {
                    let mut column = SchemaColumn::with_default_value(
                        target_index,
                        col_def.name.value.clone(),
                        data_type,
                        !not_null && !primary_key,
                        primary_key,
                        auto_increment,
                        default_expr.clone(),
                        default_value,
                        check_expr.clone(),
                    );
                    if data_type == DataType::Vector {
                        column.vector_dimensions = vector_dimensions;
                    }
                    if data_type == DataType::Decimal {
                        column.decimal_precision = decimal_precision;
                        column.decimal_scale = decimal_scale;
                    }
                    if let Some((type_ref, sql_name)) = &external_type {
                        column.external_type = Some(*type_ref);
                        column.external_type_name = Some(sql_name.clone());
                    }
                    schema.add_column(column)?;
                    requires_row_normalization = true;
                } else {
                    let old_column = old_column.expect("MODIFY target was resolved");
                    let effective_primary_key = old_column.primary_key || primary_key;
                    let effective_auto_increment = old_column.auto_increment || auto_increment;
                    let column = &mut schema.columns[target_index];
                    column.data_type = data_type;
                    column.external_type = external_type.as_ref().map(|(type_ref, _)| *type_ref);
                    column.external_type_name =
                        external_type.as_ref().map(|(_, sql_name)| sql_name.clone());
                    column.nullable = !not_null && !effective_primary_key;
                    column.primary_key = effective_primary_key;
                    column.auto_increment = effective_auto_increment;
                    // MODIFY is a complete replacement definition. Omitted
                    // DEFAULT/CHECK clauses must not retain expressions bound
                    // to the previous physical type.
                    column.default_expr = default_expr.clone();
                    column.default_value = default_value;
                    column.check_expr = check_expr.clone();
                    if data_type == DataType::Vector {
                        column.vector_dimensions = vector_dimensions;
                    } else {
                        column.vector_dimensions = 0;
                    }
                    if data_type == DataType::Decimal {
                        column.decimal_precision = decimal_precision;
                        column.decimal_scale = decimal_scale;
                    } else {
                        column.decimal_precision = 0;
                        column.decimal_scale = 0;
                    }
                }

                if adds_primary_key {
                    schema.register_primary_key_constraint(vec![col_def.name.value.to_string()])?;
                }
                if previous_check_expr != check_expr {
                    let previous_name =
                        schema
                            .constraints()
                            .iter()
                            .find_map(|constraint| match &constraint.kind {
                                SchemaConstraintKind::Check {
                                    column_name: Some(column_name),
                                    ..
                                } if column_name.eq_ignore_ascii_case(&col_def.name.value) => {
                                    Some(constraint.name.clone())
                                }
                                _ => None,
                            });
                    if let Some(previous_name) = previous_name {
                        schema.take_constraint(&previous_name);
                    }
                    if let Some(expression) = check_expr.clone() {
                        schema.register_check_constraint(
                            Some(col_def.name.value.to_string()),
                            expression,
                        )?;
                    }
                }

                if let Some((parent, parent_column, on_delete, on_update)) = reference {
                    if schema
                        .foreign_keys
                        .iter()
                        .any(|fk| fk.column_index == target_index)
                    {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' already has a FOREIGN KEY",
                            col_def.name.value
                        )));
                    }
                    let fk = self.bind_alter_foreign_key(
                        transaction,
                        &schema,
                        &col_def.name.value,
                        parent,
                        parent_column,
                        on_delete,
                        on_update,
                    )?;
                    schema.foreign_keys.push(fk.clone());
                    schema.register_foreign_key_constraint(&fk)?;
                    // A child-side index is an optimization, not part of FK
                    // correctness. Do not synthesize one here: a later
                    // CREATE UNIQUE INDEX on the same column must remain a
                    // valid migration step (Messenger RDB-0026/0033).
                }
                if unique && !primary_key {
                    let columns = vec![col_def.name.value.to_string()];
                    let index_name = schema.register_unique_constraint(columns.clone())?;
                    indexes.push(Self::alter_index_definition(
                        table_name, index_name, columns, true,
                    ));
                }
                schema.finish_catalog_mutation()?;
            }
            AlterTableOperation::AddConstraint => {
                let constraint = stmt.table_constraint.as_ref().ok_or_else(|| {
                    Error::InvalidArgument("ADD CONSTRAINT requires a constraint".to_string())
                })?;
                match constraint {
                    TableConstraint::Check(expression) => {
                        let expression = bind_persistent_expression(expression, ctx)?;
                        schema.table_checks.push(expression.clone());
                        schema.register_check_constraint(None, expression)?;
                    }
                    TableConstraint::Unique(columns) => {
                        let names: Vec<String> = columns
                            .iter()
                            .map(|column| {
                                if schema.find_column(&column.value).is_none() {
                                    Err(Error::ColumnNotFound(column.value.to_string()))
                                } else {
                                    Ok(column.value.to_string())
                                }
                            })
                            .collect::<Result<_>>()?;
                        let index_name = schema.register_unique_constraint(names.clone())?;
                        indexes.push(Self::alter_index_definition(
                            table_name, index_name, names, true,
                        ));
                    }
                    TableConstraint::PrimaryKey(columns) => {
                        if columns.len() != 1 {
                            return Err(Error::NotSupported(
                                "composite PRIMARY KEY is not supported; use UNIQUE instead"
                                    .to_string(),
                            ));
                        }
                        if schema.has_primary_key() {
                            return Err(Error::InvalidArgument(
                                "table already has a PRIMARY KEY".to_string(),
                            ));
                        }
                        if !table_is_empty {
                            return Err(Error::InvalidArgument(
                                "ALTER TABLE cannot add PRIMARY KEY to a populated table; rebuild it explicitly"
                                    .to_string(),
                            ));
                        }
                        let index = schema
                            .get_column_index(&columns[0].value)
                            .ok_or_else(|| Error::ColumnNotFound(columns[0].value.to_string()))?;
                        if !matches!(
                            schema.columns[index].data_type,
                            DataType::Integer | DataType::Uuid
                        ) {
                            return Err(Error::InvalidArgument(
                                "PRIMARY KEY must be INTEGER or UUID".to_string(),
                            ));
                        }
                        schema.columns[index].primary_key = true;
                        schema.columns[index].nullable = false;
                        schema
                            .register_primary_key_constraint(vec![columns[0].value.to_string()])?;
                    }
                    TableConstraint::ForeignKey(foreign_key) => {
                        let local_index = schema
                            .get_column_index(&foreign_key.column.value)
                            .ok_or_else(|| {
                                Error::ColumnNotFound(foreign_key.column.value.to_string())
                            })?;
                        if schema
                            .foreign_keys
                            .iter()
                            .any(|fk| fk.column_index == local_index)
                        {
                            return Err(Error::InvalidArgument(format!(
                                "column '{}' already has a FOREIGN KEY",
                                foreign_key.column.value
                            )));
                        }
                        let fk = self.bind_alter_foreign_key(
                            transaction,
                            &schema,
                            &foreign_key.column.value,
                            &foreign_key.ref_table,
                            foreign_key.ref_column.as_ref(),
                            foreign_key.on_delete,
                            foreign_key.on_update,
                        )?;
                        schema.foreign_keys.push(fk.clone());
                        schema.register_foreign_key_constraint(&fk)?;
                        // See the inline REFERENCES branch above. The parent
                        // uniqueness owner is validated by bind; a child-side
                        // helper index must not pre-empt a later UNIQUE index.
                    }
                }
                schema.finish_catalog_mutation()?;
            }
            AlterTableOperation::DropConstraint => {
                let constraint_name = stmt.constraint_name.as_ref().ok_or_else(|| {
                    Error::InvalidArgument("DROP CONSTRAINT requires a constraint name".to_string())
                })?;
                let Some(constraint) = schema.find_constraint(&constraint_name.value).cloned()
                else {
                    if stmt.if_exists {
                        return Ok(());
                    }
                    return Err(Error::InvalidArgument(format!(
                        "constraint '{}' does not exist on table '{}'",
                        constraint_name.value, table_name
                    )));
                };

                let mut owned_index: Option<(String, bool)> = None;
                match &constraint.kind {
                    SchemaConstraintKind::PrimaryKey { columns }
                    | SchemaConstraintKind::Unique { columns, .. } => {
                        let referenced_column = columns
                            .first()
                            .ok_or_else(|| Error::internal("key constraint has no column"))?;
                        let catalog_alternative = schema.constraints().iter().any(|candidate| {
                            candidate.id != constraint.id
                                && match &candidate.kind {
                                    SchemaConstraintKind::PrimaryKey { columns } => {
                                        columns.as_slice().first().is_some_and(|column| {
                                            columns.len() == 1
                                                && column.eq_ignore_ascii_case(referenced_column)
                                        })
                                    }
                                    SchemaConstraintKind::Unique { columns, .. } => {
                                        columns.len() == 1
                                            && columns[0].eq_ignore_ascii_case(referenced_column)
                                            && schema
                                                .get_column_by_name(referenced_column)
                                                .is_some_and(|column| !column.nullable)
                                    }
                                    _ => false,
                                }
                        });
                        let target_is_not_null = schema
                            .get_column_by_name(referenced_column)
                            .is_some_and(|column| !column.nullable);
                        let is_current_physical_owner =
                            |name: &str, index_type: radixdb_core::IndexType| match &constraint.kind
                            {
                                SchemaConstraintKind::PrimaryKey { .. } => {
                                    index_type == radixdb_core::IndexType::PrimaryKey
                                        || name.starts_with("__pk_")
                                }
                                SchemaConstraintKind::Unique { index_name, .. } => {
                                    name.eq_ignore_ascii_case(index_name)
                                }
                                _ => false,
                            };
                        let physical_alternative = target_is_not_null
                            && table.get_indexes().into_iter().any(|index| {
                                index.is_unique()
                                    && index.partial_predicate().is_none()
                                    && index.column_names().len() == 1
                                    && index.column_names()[0]
                                        .eq_ignore_ascii_case(referenced_column)
                                    && !is_current_physical_owner(index.name(), index.index_type())
                            });
                        let staged_alternative = target_is_not_null
                            && transaction.staged_index_definitions(table_name).iter().any(
                                |pending| {
                                    pending.is_unique
                                        && pending.partial_predicate.is_none()
                                        && pending.columns.len() == 1
                                        && pending.columns[0]
                                            .eq_ignore_ascii_case(referenced_column)
                                        && !match &constraint.kind {
                                            SchemaConstraintKind::Unique { index_name, .. } => {
                                                pending.index_name.eq_ignore_ascii_case(index_name)
                                            }
                                            SchemaConstraintKind::PrimaryKey { .. } => false,
                                            _ => false,
                                        }
                                },
                            );
                        let has_alternative_target =
                            catalog_alternative || physical_alternative || staged_alternative;
                        let referencing = self
                            .mutation_engine()
                            .find_referencing_fks_for_txn(transaction.id(), table_name);
                        if !has_alternative_target {
                            if let Some((child_table, _)) = referencing.iter().find(|(_, fk)| {
                                fk.referenced_column.eq_ignore_ascii_case(referenced_column)
                            }) {
                                return Err(Error::InvalidArgument(format!(
                                    "cannot drop constraint '{}' because table '{}' has a dependent foreign key",
                                    constraint.name, child_table
                                )));
                            }
                        }
                        match &constraint.kind {
                            SchemaConstraintKind::PrimaryKey { columns } => {
                                let column = schema
                                    .get_column_index(&columns[0])
                                    .ok_or_else(|| Error::ColumnNotFound(columns[0].clone()))?;
                                let derived_index = table.get_indexes().into_iter().find(|index| {
                                    (index.index_type() == radixdb_core::IndexType::PrimaryKey
                                        || index.name().starts_with("__pk_"))
                                        && index.column_ids() == [column as i32]
                                });
                                if let Some(derived_index) = derived_index {
                                    owned_index = Some((derived_index.name().to_string(), true));
                                } else {
                                    let existed_before_transaction = self
                                        .mutation_engine()
                                        .get_table_schema(table_name)
                                        .ok()
                                        .is_some_and(|catalog_schema| {
                                            catalog_schema
                                                .constraints()
                                                .iter()
                                                .any(|candidate| candidate.id == constraint.id)
                                        });
                                    if existed_before_transaction {
                                        return Err(Error::internal(format!(
                                            "constraint '{}' lost its derived PRIMARY KEY index",
                                            constraint.name
                                        )));
                                    }
                                    // ADD PRIMARY KEY followed by DROP of the
                                    // same new identity in one transaction is
                                    // a catalog net-zero: no physical PK index
                                    // has crossed the commit boundary yet.
                                }
                                schema.columns[column].primary_key = false;
                            }
                            SchemaConstraintKind::Unique { index_name, .. } => {
                                if table.get_index(index_name).is_none()
                                    && !transaction.staged_index_definitions(table_name).iter().any(
                                        |pending| {
                                            pending.index_name.eq_ignore_ascii_case(index_name)
                                        },
                                    )
                                {
                                    return Err(Error::IndexNotFound(index_name.clone()));
                                }
                                owned_index = Some((index_name.clone(), false));
                            }
                            _ => unreachable!(),
                        }
                    }
                    SchemaConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } => {
                        let index = schema
                            .foreign_keys
                            .iter()
                            .position(|foreign_key| {
                                columns.len() == 1
                                    && referenced_columns.len() == 1
                                    && foreign_key.column_name.eq_ignore_ascii_case(&columns[0])
                                    && foreign_key
                                        .referenced_table
                                        .eq_ignore_ascii_case(referenced_table)
                                    && foreign_key
                                        .referenced_column
                                        .eq_ignore_ascii_case(&referenced_columns[0])
                                    && foreign_key.on_delete == *on_delete
                                    && foreign_key.on_update == *on_update
                            })
                            .ok_or_else(|| {
                                Error::internal(format!(
                                    "constraint '{}' lost its FOREIGN KEY owner",
                                    constraint.name
                                ))
                            })?;
                        schema.foreign_keys.remove(index);
                    }
                    SchemaConstraintKind::Check {
                        column_name,
                        expression,
                        ..
                    } => {
                        if let Some(column_name) = column_name {
                            let column = schema
                                .get_column_index(column_name)
                                .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?;
                            if schema.columns[column].check_expr.as_ref() != Some(expression) {
                                return Err(Error::internal(format!(
                                    "constraint '{}' lost its column CHECK owner",
                                    constraint.name
                                )));
                            }
                            schema.columns[column].check_expr = None;
                        } else {
                            let occurrence = schema
                                .constraints()
                                .iter()
                                .take_while(|candidate| candidate.id != constraint.id)
                                .filter(|candidate| {
                                    matches!(
                                        &candidate.kind,
                                        SchemaConstraintKind::Check {
                                            column_name: None,
                                            expression: candidate_expression,
                                            ..
                                        } if candidate_expression == expression
                                    )
                                })
                                .count();
                            let index = schema
                                .table_checks
                                .iter()
                                .enumerate()
                                .filter(|(_, candidate)| *candidate == expression)
                                .nth(occurrence)
                                .map(|(index, _)| index)
                                .ok_or_else(|| {
                                    Error::internal(format!(
                                        "constraint '{}' lost its table CHECK owner",
                                        constraint.name
                                    ))
                                })?;
                            schema.table_checks.remove(index);
                        }
                    }
                }
                schema.take_constraint(&constraint.name);
                schema.finish_catalog_mutation()?;
                if let Some((index_name, schema_owned)) = owned_index {
                    transaction.stage_drop_index(PendingIndexDrop {
                        table_name: table_name.to_string(),
                        index_name,
                        schema_owned,
                    })?;
                }
            }
            AlterTableOperation::DropColumn => {
                let column_name = stmt.column_name.as_ref().ok_or_else(|| {
                    Error::InvalidArgument("DROP COLUMN requires column name".to_string())
                })?;
                if let Some((child_table, _)) = self
                    .mutation_engine()
                    .find_referencing_fks_for_txn(transaction.id(), table_name)
                    .iter()
                    .find(|(_, fk)| {
                        fk.referenced_column
                            .eq_ignore_ascii_case(column_name.value.as_str())
                    })
                {
                    return Err(Error::InvalidArgument(format!(
                        "cannot drop referenced column '{}.{}'; foreign key exists in table '{}'",
                        table_name, column_name.value, child_table
                    )));
                }
                let (column_index, column) = schema
                    .find_column(&column_name.value)
                    .ok_or_else(|| Error::ColumnNotFound(column_name.value.to_string()))?;
                if column.primary_key {
                    return Err(Error::CannotDropPrimaryKey);
                }
                schema.remove_column(&column_name.value)?;
                compile_table_check_constraints(&schema).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "cannot drop column '{}' because a table CHECK would become invalid: {}",
                        column_name.value, error
                    ))
                })?;
                catalog.stage_table_schema_as(&schema, None, ctx.effective_principal_id())?;
                transaction.stage_table_schema_transition(
                    table_name,
                    schema,
                    true,
                    SchemaPhysicalTransition::DropColumn {
                        column_name: column_name.value.to_string(),
                        column_index,
                    },
                )?;
                return Ok(());
            }
            AlterTableOperation::RenameColumn => {
                let (old_name, new_name) = stmt
                    .column_name
                    .as_ref()
                    .zip(stmt.new_column_name.as_ref())
                    .ok_or_else(|| {
                        Error::InvalidArgument(
                            "RENAME COLUMN requires old and new column names".to_string(),
                        )
                    })?;
                if let Some((child_table, _)) = self
                    .mutation_engine()
                    .find_referencing_fks_for_txn(transaction.id(), table_name)
                    .iter()
                    .find(|(_, fk)| {
                        fk.referenced_column
                            .eq_ignore_ascii_case(old_name.value.as_str())
                    })
                {
                    return Err(Error::InvalidArgument(format!(
                        "cannot rename referenced column '{}.{}'; foreign key exists in table '{}'",
                        table_name, old_name.value, child_table
                    )));
                }
                schema.rename_column(&old_name.value, new_name.value.as_str())?;
                compile_table_check_constraints(&schema).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "cannot rename column '{}' because a table CHECK would become invalid: {}",
                        old_name.value, error
                    ))
                })?;
                catalog.stage_table_schema_as(
                    &schema,
                    Some((old_name.value.as_str(), new_name.value.as_str())),
                    ctx.effective_principal_id(),
                )?;
                transaction.stage_table_schema_transition(
                    table_name,
                    schema,
                    false,
                    SchemaPhysicalTransition::RenameColumn {
                        old_name: old_name.value.to_string(),
                        new_name: new_name.value.to_string(),
                    },
                )?;
                return Ok(());
            }
            _ => {
                return Err(Error::NotSupported(
                    "this ALTER TABLE operation cannot use the transactional schema path"
                        .to_string(),
                ));
            }
        }

        compile_table_check_constraints(&schema)?;
        self.validate_alter_schema_rows(transaction, table.as_ref(), &schema)?;
        // A complete MODIFY definition may be semantically identical to the
        // current definition. In that case the catalog owns an empty delta and
        // the physical schema must remain untouched as well; staging a
        // timestamp-only runtime rewrite would violate the single-authority
        // DDL contract at commit.
        if !catalog.stage_table_schema_as(&schema, None, ctx.effective_principal_id())? {
            return Ok(());
        }
        transaction.stage_table_schema_change(table_name, schema, requires_row_normalization)?;
        for index in indexes {
            transaction.stage_create_index(index)?;
        }
        Ok(())
    }

    /// Execute an ALTER TABLE statement
    fn execute_alter_table(
        &self,
        stmt: &AlterTableStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;
        let transactional_schema_operation = matches!(
            stmt.operation,
            AlterTableOperation::AddColumn
                | AlterTableOperation::ModifyColumn
                | AlterTableOperation::AddConstraint
                | AlterTableOperation::DropConstraint
                | AlterTableOperation::DropColumn
                | AlterTableOperation::RenameColumn
                | AlterTableOperation::RenameTable
        );

        // Constraint-bearing ALTERs share one private complete-schema path in
        // explicit and auto-commit transactions. CREATE and ALTER therefore
        // bind the same constraint families, and rollback never exposes an
        // intermediate catalog state.
        if transactional_schema_operation {
            let mut active_tx = self.mutation_active_transaction().lock().unwrap();
            if let Some(tx_state) = active_tx.as_mut() {
                let mut catalog = tx_state.catalog.clone();
                self.stage_alter_schema_operation(
                    tx_state.transaction.as_mut(),
                    &mut catalog,
                    stmt,
                    ctx,
                )?;
                tx_state.catalog = catalog;
                self.mutation_invalidate_query_cache(table_name);
                self.mutation_invalidate_semantic_cache(table_name);
                invalidate_semi_join_cache_for_table(table_name);
                invalidate_scalar_subquery_cache_for_table(table_name);
                invalidate_in_subquery_cache_for_table(table_name);
                if let Some(new_name) = &stmt.new_table_name {
                    self.mutation_invalidate_query_cache(&new_name.value);
                    self.mutation_invalidate_semantic_cache(&new_name.value);
                    invalidate_semi_join_cache_for_table(&new_name.value);
                    invalidate_scalar_subquery_cache_for_table(&new_name.value);
                    invalidate_in_subquery_cache_for_table(&new_name.value);
                }
                return Ok(Box::new(ExecResult::empty()));
            }
            drop(active_tx);

            if !self.mutation_engine().table_exists(table_name)? {
                return Err(Error::TableNotFound(table_name.to_string()));
            }
            let mut transaction = self.mutation_engine().begin_transaction()?;
            let generation = self.mutation_engine().pin_catalog()?;
            let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
            self.stage_alter_schema_operation(transaction.as_mut(), &mut catalog, stmt, ctx)?;
            if let Some(mutation) = catalog.pending_mutation()? {
                transaction.stage_catalog_mutation(mutation)?;
            }
            transaction.commit()?;
            self.mutation_invalidate_query_cache(table_name);
            self.mutation_invalidate_semantic_cache(table_name);
            invalidate_semi_join_cache_for_table(table_name);
            invalidate_scalar_subquery_cache_for_table(table_name);
            invalidate_in_subquery_cache_for_table(table_name);
            if let Some(new_name) = &stmt.new_table_name {
                self.mutation_invalidate_query_cache(&new_name.value);
                self.mutation_invalidate_semantic_cache(&new_name.value);
                invalidate_semi_join_cache_for_table(&new_name.value);
                invalidate_scalar_subquery_cache_for_table(&new_name.value);
                invalidate_in_subquery_cache_for_table(&new_name.value);
            }
            return Ok(Box::new(ExecResult::empty()));
        }

        unreachable!("every ALTER TABLE operation uses the transactional catalog path")
    }

    /// Execute a CREATE VIEW statement
    fn execute_create_view(
        &self,
        stmt: &CreateViewStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let view_name = &stmt.view_name.value;
        if let Some(mutation) = self.mutation_stage_catalog_statement_as(
            Statement::CreateView(stmt.clone()),
            ctx.effective_principal_id(),
            ctx.current_database(),
        )? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }

        self.mutation_invalidate_query_cache(view_name);
        self.mutation_invalidate_semantic_cache(view_name);
        invalidate_semi_join_cache_for_table(view_name);
        invalidate_scalar_subquery_cache_for_table(view_name);
        invalidate_in_subquery_cache_for_table(view_name);

        Ok(Box::new(ExecResult::empty()))
    }

    /// Execute a DROP VIEW statement
    fn execute_drop_view(
        &self,
        stmt: &DropViewStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let view_name = &stmt.view_name.value;
        if let Some(mutation) = self.mutation_stage_catalog_statement_as(
            Statement::DropView(stmt.clone()),
            ctx.effective_principal_id(),
            ctx.current_database(),
        )? {
            let mut transaction = self.mutation_engine().begin_transaction()?;
            transaction.stage_catalog_mutation(mutation)?;
            transaction.commit()?;
        }

        // Invalidate subquery caches that may reference this view
        self.mutation_invalidate_query_cache(view_name);
        self.mutation_invalidate_semantic_cache(view_name);
        invalidate_semi_join_cache_for_table(view_name);
        invalidate_scalar_subquery_cache_for_table(view_name);
        invalidate_in_subquery_cache_for_table(view_name);

        Ok(Box::new(ExecResult::empty()))
    }

    /// Evaluate a default expression string and return the resulting Value
    fn evaluate_default_expression(
        &self,
        default_expr: &str,
        target_type: DataType,
    ) -> Result<Value> {
        use radixdb_sql::parse_sql;

        // Parse the default expression as a SELECT expression
        let sql = format!("SELECT {}", default_expr);
        let stmts = parse_sql(&sql)
            .map_err(|error| Error::Parse(format!("invalid default expression: {}", error)))?;
        if stmts.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "default expression '{}' produced no statement",
                default_expr
            )));
        }

        // Extract the expression from the SELECT statement
        if let Statement::Select(select) = &stmts[0] {
            if let Some(expr) = select.columns.first() {
                let mut eval = ExpressionEval::compile(expr, &[])?;
                let value = eval.eval_slice(&Row::new())?;
                return value.try_coerce_to_type(target_type);
            }
        }

        Err(Error::InvalidArgument(format!(
            "default expression '{}' is not a SELECT expression",
            default_expr
        )))
    }
}

impl<T: MutationHost + ?Sized> DdlExecutorExt for T {}
