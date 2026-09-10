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

//! SELECT output-shape, source, and expression metadata binding.

use radixdb_core::{DataType, Error, LogicalTypeRef, Result};
use radixdb_sql::ast::*;

pub use super::output_contract::{
    BoundOutputColumn, NavigationOutputBinding, OutputBindingHost, QueryOutputColumn,
};
use super::types::parse_data_type;

/// Schema-only SELECT output binder owned by the executor crate.
#[doc(hidden)]
pub trait OutputBindingExt: OutputBindingHost {
    /// Bind public result metadata from the same schema/type owner used by
    /// CTAS. `None` means the SQL is not one SELECT result.
    fn describe_query_output(&self, sql: &str) -> Result<Option<Vec<QueryOutputColumn>>> {
        let statements =
            radixdb_sql::parse_sql(sql).map_err(|error| Error::Parse(error.to_string()))?;
        let [Statement::Select(select)] = statements.as_slice() else {
            return Ok(None);
        };
        self.bind_select_output(select, &[], 0).map(|columns| {
            Some(
                columns
                    .into_iter()
                    .map(|column| QueryOutputColumn {
                        name: column.name,
                        type_name: column.type_name,
                        data_type: column.data_type,
                        logical_type: column.logical_type,
                        nullable: column.nullable,
                    })
                    .collect(),
            )
        })
    }

    /// Bind one table-free scalar expression through the same type owner used
    /// by SELECT. Stored-routine locals must already be rewritten to typed
    /// casts over positional parameters before entering this seam.
    fn bind_scalar_output(&self, expression: &Expression) -> Result<(DataType, bool)> {
        let (data_type, _, _, nullable) = self.bind_scalar_output_metadata(expression)?;
        Ok((data_type, nullable))
    }

    /// Bind the complete logical identity of one table-free scalar expression.
    /// This is used by PL dependency binding so external-type overloads cannot
    /// collapse into the closed built-in `DataType` representation.
    fn bind_scalar_output_metadata(
        &self,
        expression: &Expression,
    ) -> Result<(DataType, LogicalTypeRef, String, bool)> {
        let (data_type, logical_type, type_name) = self
            .bind_expression_output_type(expression, &[], &[], &[], 0)?
            .ok_or_else(|| {
                Error::NotSupported(
                    "scalar expression has no bind-time type; add an explicit CAST".to_string(),
                )
            })?;
        Ok((
            data_type,
            logical_type,
            type_name,
            self.bind_expression_nullable(expression, &[], &[], &[], 0)?,
        ))
    }

    /// Bind a DML RETURNING list against the target relation's runtime schema.
    fn bind_returning_output(
        &self,
        table_name: &str,
        qualifier: &str,
        returning: &[Expression],
    ) -> Result<Vec<BoundOutputColumn>> {
        let schema = self.output_binding_table_schema(table_name)?;
        let scope = schema
            .columns
            .iter()
            .map(|column| BoundOutputColumn {
                name: column.name.clone(),
                qualifier: Some(qualifier.to_lowercase()),
                data_type: column.data_type,
                logical_type: column.logical_type(),
                type_name: column.formatted_data_type(),
                nullable: column.nullable,
            })
            .collect::<Vec<_>>();
        let mut output = Vec::new();
        for (ordinal, expression) in returning.iter().enumerate() {
            match expression {
                Expression::Star(_) => output.extend(scope.iter().cloned().map(|mut column| {
                    column.qualifier = None;
                    column
                })),
                Expression::QualifiedStar(star)
                    if star.qualifier.eq_ignore_ascii_case(qualifier) =>
                {
                    output.extend(scope.iter().cloned().map(|mut column| {
                        column.qualifier = None;
                        column
                    }));
                }
                Expression::QualifiedStar(star) => {
                    return Err(Error::InvalidArgument(format!(
                        "RETURNING cannot bind qualified star '{}.*' for table '{table_name}'",
                        star.qualifier
                    )));
                }
                expression => {
                    let (data_type, logical_type, type_name) = self
                        .bind_expression_output_type(expression, &scope, &[], &[], 0)?
                        .ok_or_else(|| {
                            Error::NotSupported(format!(
                                "RETURNING column {} has no bind-time type; add an explicit CAST",
                                ordinal + 1
                            ))
                        })?;
                    output.push(BoundOutputColumn {
                        name: Self::bound_expression_name(expression, ordinal),
                        qualifier: None,
                        data_type,
                        logical_type,
                        type_name,
                        nullable: self.bind_expression_nullable(expression, &scope, &[], &[], 0)?,
                    });
                }
            }
        }
        Ok(output)
    }

    fn bind_select_output(
        &self,
        select: &SelectStatement,
        inherited_ctes: &[(String, Vec<BoundOutputColumn>)],
        depth: usize,
    ) -> Result<Vec<BoundOutputColumn>> {
        if depth > 32 {
            return Err(Error::NotSupported(
                "CTAS metadata binding exceeded 32 nested query levels".to_string(),
            ));
        }

        let mut ctes = inherited_ctes.to_vec();
        if let Some(with) = &select.with {
            for cte in &with.ctes {
                let mut columns = self.bind_select_output(&cte.query, &ctes, depth + 1)?;
                if !cte.column_names.is_empty() {
                    if cte.column_names.len() != columns.len() {
                        return Err(Error::InvalidArgument(format!(
                            "CTE '{}' declares {} columns but query returns {}",
                            cte.name.value,
                            cte.column_names.len(),
                            columns.len()
                        )));
                    }
                    for (column, alias) in columns.iter_mut().zip(&cte.column_names) {
                        column.name = alias.value.to_string();
                    }
                }
                ctes.push((cte.name.value_lower.to_string(), columns));
            }
        }

        let scope = match select.table_expr.as_deref() {
            Some(source) => self.bind_table_source(source, &ctes, depth + 1)?,
            None => Vec::new(),
        };
        let navigation = self.output_binding_navigation(select)?;
        let mut output = Vec::new();
        for (ordinal, expression) in select.columns.iter().enumerate() {
            match expression {
                Expression::Star(_) => output.extend(scope.iter().cloned().map(|mut column| {
                    column.qualifier = None;
                    column
                })),
                Expression::QualifiedStar(star) => {
                    let qualifier = star.qualifier.to_lowercase();
                    let before = output.len();
                    output.extend(
                        scope
                            .iter()
                            .filter(|column| column.qualifier.as_deref() == Some(&qualifier))
                            .cloned()
                            .map(|mut column| {
                                column.qualifier = None;
                                column
                            }),
                    );
                    if output.len() == before {
                        return Err(Error::InvalidArgument(format!(
                            "CTAS cannot bind qualified star '{}.*'",
                            star.qualifier
                        )));
                    }
                }
                _ => {
                    let (data_type, logical_type, type_name) = self
                        .bind_expression_output_type(
                            expression,
                            &scope,
                            &ctes,
                            &navigation,
                            depth + 1,
                        )?
                        .ok_or_else(|| {
                            Error::NotSupported(format!(
                                "CTAS output column {} has no bind-time type; add an explicit CAST",
                                ordinal + 1
                            ))
                        })?;
                    output.push(BoundOutputColumn {
                        name: Self::bound_expression_name(expression, ordinal),
                        qualifier: None,
                        data_type,
                        logical_type,
                        type_name,
                        nullable: self.bind_expression_nullable(
                            expression,
                            &scope,
                            &ctes,
                            &navigation,
                            depth + 1,
                        )?,
                    });
                }
            }
        }

        for set_operation in &select.set_operations {
            let right = self.bind_select_output(&set_operation.right, &ctes, depth + 1)?;
            if right.len() != output.len() {
                return Err(Error::InvalidArgument(
                    "set-operation branches have different column counts".to_string(),
                ));
            }
            for (left, right) in output.iter_mut().zip(right) {
                if left.logical_type != right.logical_type {
                    let merged = match (left.logical_type, right.logical_type) {
                        (LogicalTypeRef::Builtin(left), LogicalTypeRef::Builtin(right)) => {
                            Self::merge_bound_types(left, right)?
                        }
                        _ => {
                            return Err(Error::InvalidArgument(
                                "set-operation branches have different external types".to_string(),
                            ))
                        }
                    };
                    left.data_type = merged;
                    left.logical_type = LogicalTypeRef::Builtin(merged);
                    left.type_name = merged.to_string();
                }
                left.nullable |= right.nullable;
            }
        }
        Ok(output)
    }

    fn bind_table_source(
        &self,
        source: &Expression,
        ctes: &[(String, Vec<BoundOutputColumn>)],
        depth: usize,
    ) -> Result<Vec<BoundOutputColumn>> {
        match source {
            Expression::TableSource(table) => {
                let name = table.name.value_lower.to_string();
                if let Some((_, columns)) = ctes.iter().rev().find(|(cte, _)| cte == &name) {
                    return Ok(Self::qualify_bound_columns(
                        columns.clone(),
                        table
                            .alias
                            .as_ref()
                            .map_or(&name, |alias| alias.value_lower.as_str()),
                    ));
                }
                if let Ok(schema) = self.output_binding_table_schema(&name) {
                    let qualifier = table
                        .alias
                        .as_ref()
                        .map_or(name.as_str(), |alias| alias.value_lower.as_str());
                    return Ok(schema
                        .columns
                        .iter()
                        .map(|column| BoundOutputColumn {
                            name: column.name.clone(),
                            qualifier: Some(qualifier.to_string()),
                            data_type: column.data_type,
                            logical_type: column.logical_type(),
                            type_name: column.formatted_data_type(),
                            nullable: column.nullable,
                        })
                        .collect());
                }
                if let Some(view) = self.output_binding_view(&name)? {
                    let statements = radixdb_sql::parse_sql(&view.query).map_err(|error| {
                        Error::Parse(format!(
                            "cannot bind view '{}': {}",
                            view.original_name, error
                        ))
                    })?;
                    let [Statement::Select(query)] = statements.as_slice() else {
                        return Err(Error::Parse(format!(
                            "view '{}' does not contain one SELECT",
                            view.original_name
                        )));
                    };
                    let columns = self.bind_select_output(query, ctes, depth + 1)?;
                    let qualifier = table
                        .alias
                        .as_ref()
                        .map_or(name.as_str(), |alias| alias.value_lower.as_str());
                    return Ok(Self::qualify_bound_columns(columns, qualifier));
                }
                Err(Error::TableNotFound(name))
            }
            Expression::JoinSource(join) => {
                let mut left = self.bind_table_source(&join.left, ctes, depth + 1)?;
                let mut right = self.bind_table_source(&join.right, ctes, depth + 1)?;

                // Result nullability belongs to the join result, not to the
                // underlying table alone. An outer join can synthesize NULLs
                // for every column on its null-extended side even when those
                // base columns are declared NOT NULL. Preserve nullability
                // already introduced by nested joins and widen the complete
                // affected side here.
                let join_type = join.join_type.to_ascii_uppercase();
                if join_type.contains("RIGHT") || join_type.contains("FULL") {
                    left.iter_mut().for_each(|column| column.nullable = true);
                }
                if join_type.contains("LEFT") || join_type.contains("FULL") {
                    right.iter_mut().for_each(|column| column.nullable = true);
                }

                left.extend(right);
                Ok(left)
            }
            Expression::SubquerySource(subquery) => {
                let columns = self.bind_select_output(&subquery.subquery, ctes, depth + 1)?;
                let qualifier = subquery
                    .alias
                    .as_ref()
                    .map(|alias| alias.value_lower.as_str())
                    .unwrap_or("subquery");
                Ok(Self::qualify_bound_columns(columns, qualifier))
            }
            Expression::CteReference(reference) => {
                let name = reference.name.value_lower.as_str();
                let columns = ctes
                    .iter()
                    .rev()
                    .find(|(cte, _)| cte == name)
                    .map(|(_, columns)| columns.clone())
                    .ok_or_else(|| Error::TableNotFound(name.to_string()))?;
                let qualifier = reference
                    .alias
                    .as_ref()
                    .map_or(name, |alias| alias.value_lower.as_str());
                Ok(Self::qualify_bound_columns(columns, qualifier))
            }
            Expression::ValuesSource(values) => {
                let width = values.rows.first().map_or(0, Vec::len);
                if values.rows.iter().any(|row| row.len() != width) {
                    return Err(Error::InvalidArgument(
                        "VALUES rows have different column counts".to_string(),
                    ));
                }
                let mut types = vec![None; width];
                for row in &values.rows {
                    for (index, expression) in row.iter().enumerate() {
                        if let Some(data_type) =
                            self.bind_expression_type(expression, &[], ctes, &[], depth + 1)?
                        {
                            types[index] = Some(match types[index] {
                                Some(current) => Self::merge_bound_types(current, data_type)?,
                                None => data_type,
                            });
                        }
                    }
                }
                let qualifier = values
                    .alias
                    .as_ref()
                    .map(|alias| alias.value_lower.to_string());
                types
                    .into_iter()
                    .enumerate()
                    .map(|(index, data_type)| {
                        Ok(BoundOutputColumn {
                            name: values.column_aliases.get(index).map_or_else(
                                || format!("column{}", index + 1),
                                |alias| alias.value.to_string(),
                            ),
                            qualifier: qualifier.clone(),
                            nullable: values.rows.iter().any(|row| {
                                row.get(index).is_some_and(|value| {
                                    matches!(value, Expression::NullLiteral(_))
                                })
                            }),
                            data_type: data_type.ok_or_else(|| {
                                Error::NotSupported(format!(
                                    "VALUES column {} has no bind-time type",
                                    index + 1
                                ))
                            })?,
                            logical_type: LogicalTypeRef::Builtin(data_type.ok_or_else(|| {
                                Error::NotSupported(format!(
                                    "VALUES column {} has no bind-time type",
                                    index + 1
                                ))
                            })?),
                            type_name: data_type
                                .map(|data_type| data_type.to_string())
                                .unwrap_or_else(|| "NULL".to_string()),
                        })
                    })
                    .collect()
            }
            _ => Err(Error::NotSupported(format!(
                "CTAS metadata binding does not support table source {source}"
            ))),
        }
    }

    fn bind_expression_output_type(
        &self,
        expression: &Expression,
        scope: &[BoundOutputColumn],
        ctes: &[(String, Vec<BoundOutputColumn>)],
        navigation: &[NavigationOutputBinding],
        depth: usize,
    ) -> Result<Option<(DataType, LogicalTypeRef, String)>> {
        let direct = match expression {
            Expression::Identifier(identifier) => {
                let matches = scope
                    .iter()
                    .filter(|column| column.name.eq_ignore_ascii_case(&identifier.value_lower))
                    .collect::<Vec<_>>();
                match matches.as_slice() {
                    [column] => Some((
                        column.data_type,
                        column.logical_type,
                        column.type_name.clone(),
                    )),
                    [] => return Err(Error::ColumnNotFound(identifier.value.to_string())),
                    _ => {
                        return Err(Error::InvalidArgument(format!(
                            "ambiguous CTAS column '{}'",
                            identifier.value
                        )))
                    }
                }
            }
            Expression::QualifiedIdentifier(identifier) => scope
                .iter()
                .find(|column| {
                    column.qualifier.as_deref() == Some(identifier.qualifier.value_lower.as_str())
                        && column
                            .name
                            .eq_ignore_ascii_case(&identifier.name.value_lower)
                })
                .map(|column| {
                    (
                        column.data_type,
                        column.logical_type,
                        column.type_name.clone(),
                    )
                }),
            Expression::Aliased(alias) => {
                return self.bind_expression_output_type(
                    &alias.expression,
                    scope,
                    ctes,
                    navigation,
                    depth + 1,
                )
            }
            Expression::Cast(cast) => Some(self.output_binding_type_name(&cast.type_name)?),
            Expression::FunctionCall(function)
                if !self.output_binding_functions().exists(&function.function) =>
            {
                let argument_types = function
                    .arguments
                    .iter()
                    .map(|argument| {
                        self.bind_expression_logical_type(
                            argument,
                            scope,
                            ctes,
                            navigation,
                            depth + 1,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.output_binding_stored_function(&function.function, &argument_types)?
                    .map(|(data_type, logical_type, type_name, _)| {
                        (data_type, logical_type, type_name)
                    })
            }
            _ => None,
        };
        if direct.is_some() {
            return Ok(direct);
        }
        Ok(self
            .bind_expression_type(expression, scope, ctes, navigation, depth)?
            .map(|data_type| {
                (
                    data_type,
                    LogicalTypeRef::Builtin(data_type),
                    data_type.to_string(),
                )
            }))
    }

    fn bind_expression_type(
        &self,
        expression: &Expression,
        scope: &[BoundOutputColumn],
        ctes: &[(String, Vec<BoundOutputColumn>)],
        navigation: &[NavigationOutputBinding],
        depth: usize,
    ) -> Result<Option<DataType>> {
        use radixdb_sql::ast::{InfixOperator, PrefixOperator};
        let bound = match expression {
            Expression::Identifier(identifier) => {
                let matches: Vec<_> = scope
                    .iter()
                    .filter(|column| column.name.eq_ignore_ascii_case(&identifier.value_lower))
                    .collect();
                match matches.as_slice() {
                    [column] => Some(column.data_type),
                    [] => return Err(Error::ColumnNotFound(identifier.value.to_string())),
                    _ => {
                        return Err(Error::InvalidArgument(format!(
                            "ambiguous CTAS column '{}'",
                            identifier.value
                        )))
                    }
                }
            }
            Expression::QualifiedIdentifier(identifier) => navigation
                .iter()
                .find(|path| {
                    path.display_path
                        .as_str()
                        .eq_ignore_ascii_case(&identifier.to_string())
                })
                .map(|path| path.terminal_type)
                .or_else(|| {
                    scope
                        .iter()
                        .find(|column| {
                            column.qualifier.as_deref()
                                == Some(identifier.qualifier.value_lower.as_str())
                                && column
                                    .name
                                    .eq_ignore_ascii_case(&identifier.name.value_lower)
                        })
                        .map(|column| column.data_type)
                })
                .ok_or_else(|| {
                    Error::ColumnNotFound(format!(
                        "{}.{}",
                        identifier.qualifier.value, identifier.name.value
                    ))
                })
                .map(Some)?,
            Expression::IntegerLiteral(_) => Some(DataType::Integer),
            Expression::FloatLiteral(_) => Some(DataType::Float),
            Expression::StringLiteral(literal) => Some(match literal.type_hint.as_deref() {
                Some(type_hint) => parse_data_type(type_hint)?,
                None => DataType::Text,
            }),
            Expression::BooleanLiteral(_) => Some(DataType::Boolean),
            Expression::NullLiteral(_) => None,
            Expression::Aliased(alias) => {
                return self.bind_expression_type(
                    &alias.expression,
                    scope,
                    ctes,
                    navigation,
                    depth + 1,
                )
            }
            Expression::Cast(cast) => Some(parse_data_type(&cast.type_name)?),
            Expression::Prefix(prefix) => match prefix.op_type {
                PrefixOperator::Not => Some(DataType::Boolean),
                _ => {
                    self.bind_expression_type(&prefix.right, scope, ctes, navigation, depth + 1)?
                }
            },
            Expression::Infix(infix) => match infix.op_type {
                InfixOperator::Equal
                | InfixOperator::NotEqual
                | InfixOperator::LessThan
                | InfixOperator::LessEqual
                | InfixOperator::GreaterThan
                | InfixOperator::GreaterEqual
                | InfixOperator::And
                | InfixOperator::Or
                | InfixOperator::Xor
                | InfixOperator::Like
                | InfixOperator::ILike
                | InfixOperator::NotLike
                | InfixOperator::NotILike
                | InfixOperator::Glob
                | InfixOperator::NotGlob
                | InfixOperator::Regexp
                | InfixOperator::NotRegexp
                | InfixOperator::Is
                | InfixOperator::IsNot
                | InfixOperator::IsDistinctFrom
                | InfixOperator::IsNotDistinctFrom => Some(DataType::Boolean),
                InfixOperator::Concat | InfixOperator::JsonAccessText => Some(DataType::Text),
                InfixOperator::JsonAccess => Some(DataType::Json),
                InfixOperator::VectorDistance => Some(DataType::Float),
                InfixOperator::BitwiseAnd
                | InfixOperator::BitwiseOr
                | InfixOperator::BitwiseXor
                | InfixOperator::LeftShift
                | InfixOperator::RightShift => Some(DataType::Integer),
                InfixOperator::Add
                | InfixOperator::Subtract
                | InfixOperator::Multiply
                | InfixOperator::Divide
                | InfixOperator::Modulo => {
                    let left =
                        self.bind_expression_type(&infix.left, scope, ctes, navigation, depth + 1)?;
                    let right = self.bind_expression_type(
                        &infix.right,
                        scope,
                        ctes,
                        navigation,
                        depth + 1,
                    )?;
                    match (left, right) {
                        (Some(left), Some(right)) => Some(Self::merge_bound_types(left, right)?),
                        (left, right) => left.or(right),
                    }
                }
                InfixOperator::Index | InfixOperator::Other => None,
            },
            Expression::FunctionCall(function) => {
                self.bind_function_return_type(function, scope, ctes, navigation, depth + 1)?
            }
            Expression::Window(window) => self.bind_function_return_type(
                &window.function,
                scope,
                ctes,
                navigation,
                depth + 1,
            )?,
            Expression::Case(case) => {
                let mut result_type = case
                    .when_clauses
                    .iter()
                    .map(|clause| {
                        self.bind_expression_type(
                            &clause.then_result,
                            scope,
                            ctes,
                            navigation,
                            depth + 1,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .next();
                for clause in &case.when_clauses {
                    if let Some(next) = self.bind_expression_type(
                        &clause.then_result,
                        scope,
                        ctes,
                        navigation,
                        depth + 1,
                    )? {
                        result_type = Some(match result_type {
                            Some(current) => Self::merge_bound_types(current, next)?,
                            None => next,
                        });
                    }
                }
                if let Some(else_value) = &case.else_value {
                    if let Some(next) =
                        self.bind_expression_type(else_value, scope, ctes, navigation, depth + 1)?
                    {
                        result_type = Some(match result_type {
                            Some(current) => Self::merge_bound_types(current, next)?,
                            None => next,
                        });
                    }
                }
                result_type
            }
            Expression::ScalarSubquery(subquery) => self
                .bind_select_output(&subquery.subquery, ctes, depth + 1)?
                .first()
                .map(|column| column.data_type),
            Expression::Exists(_)
            | Expression::AllAny(_)
            | Expression::In(_)
            | Expression::InHashSet(_)
            | Expression::Between(_)
            | Expression::Like(_) => Some(DataType::Boolean),
            _ => None,
        };
        Ok(bound)
    }

    fn bind_expression_logical_type(
        &self,
        expression: &Expression,
        scope: &[BoundOutputColumn],
        ctes: &[(String, Vec<BoundOutputColumn>)],
        navigation: &[NavigationOutputBinding],
        depth: usize,
    ) -> Result<Option<LogicalTypeRef>> {
        match expression {
            Expression::NullLiteral(_) => Ok(None),
            Expression::Identifier(identifier) => {
                let matches = scope
                    .iter()
                    .filter(|column| column.name.eq_ignore_ascii_case(&identifier.value_lower))
                    .collect::<Vec<_>>();
                match matches.as_slice() {
                    [column] => Ok(Some(column.logical_type)),
                    [] => Err(Error::ColumnNotFound(identifier.value.to_string())),
                    _ => Err(Error::InvalidArgument(format!(
                        "ambiguous column '{}'",
                        identifier.value
                    ))),
                }
            }
            Expression::QualifiedIdentifier(identifier) => scope
                .iter()
                .find(|column| {
                    column.qualifier.as_deref() == Some(identifier.qualifier.value_lower.as_str())
                        && column
                            .name
                            .eq_ignore_ascii_case(&identifier.name.value_lower)
                })
                .map(|column| Some(column.logical_type))
                .ok_or_else(|| Error::ColumnNotFound(identifier.to_string())),
            Expression::Aliased(alias) => self.bind_expression_logical_type(
                &alias.expression,
                scope,
                ctes,
                navigation,
                depth + 1,
            ),
            Expression::Cast(cast) => self
                .output_binding_type_name(&cast.type_name)
                .map(|(_, logical_type, _)| Some(logical_type)),
            Expression::FunctionCall(function)
                if !self.output_binding_functions().exists(&function.function) =>
            {
                let argument_types = function
                    .arguments
                    .iter()
                    .map(|argument| {
                        self.bind_expression_logical_type(
                            argument,
                            scope,
                            ctes,
                            navigation,
                            depth + 1,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.output_binding_stored_function(&function.function, &argument_types)
                    .and_then(|bound| {
                        bound
                            .map(|(_, logical, _, _)| logical)
                            .map(Some)
                            .ok_or_else(|| {
                                Error::InvalidArgument(format!(
                                    "unknown function {}",
                                    function.function
                                ))
                            })
                    })
            }
            _ => self
                .bind_expression_type(expression, scope, ctes, navigation, depth)
                .map(|value| value.map(LogicalTypeRef::Builtin)),
        }
    }

    fn bind_expression_nullable(
        &self,
        expression: &Expression,
        scope: &[BoundOutputColumn],
        ctes: &[(String, Vec<BoundOutputColumn>)],
        navigation: &[NavigationOutputBinding],
        depth: usize,
    ) -> Result<bool> {
        let nullable = match expression {
            Expression::Identifier(identifier) => scope
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&identifier.value_lower))
                .is_none_or(|column| column.nullable),
            Expression::QualifiedIdentifier(identifier) => navigation
                .iter()
                .find(|path| {
                    path.display_path
                        .as_str()
                        .eq_ignore_ascii_case(&identifier.to_string())
                })
                .map(|path| path.nullable)
                .or_else(|| {
                    scope
                        .iter()
                        .find(|column| {
                            column.qualifier.as_deref()
                                == Some(identifier.qualifier.value_lower.as_str())
                                && column
                                    .name
                                    .eq_ignore_ascii_case(&identifier.name.value_lower)
                        })
                        .map(|column| column.nullable)
                })
                .unwrap_or(true),
            Expression::Aliased(alias) => {
                return self.bind_expression_nullable(
                    &alias.expression,
                    scope,
                    ctes,
                    navigation,
                    depth,
                )
            }
            Expression::FunctionCall(function)
                if !self.output_binding_functions().exists(&function.function) =>
            {
                let argument_types = function
                    .arguments
                    .iter()
                    .map(|argument| {
                        self.bind_expression_logical_type(
                            argument,
                            scope,
                            ctes,
                            navigation,
                            depth + 1,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.output_binding_stored_function(&function.function, &argument_types)?
                    .is_none_or(|(_, _, _, nullable)| nullable)
            }
            Expression::IntegerLiteral(_)
            | Expression::FloatLiteral(_)
            | Expression::StringLiteral(_)
            | Expression::BooleanLiteral(_) => false,
            Expression::NullLiteral(_) => true,
            _ => true,
        };
        Ok(nullable)
    }

    fn bind_function_return_type(
        &self,
        function: &FunctionCall,
        scope: &[BoundOutputColumn],
        ctes: &[(String, Vec<BoundOutputColumn>)],
        navigation: &[NavigationOutputBinding],
        depth: usize,
    ) -> Result<Option<DataType>> {
        use radixdb_functions::{FunctionDataType, FunctionReturnRule};
        let Some(info) = self.output_binding_functions().get_info(&function.function) else {
            let argument_types = function
                .arguments
                .iter()
                .map(|argument| {
                    self.bind_expression_logical_type(argument, scope, ctes, navigation, depth + 1)
                })
                .collect::<Result<Vec<_>>>()?;
            return self
                .output_binding_stored_function(&function.function, &argument_types)
                .and_then(|bound| {
                    bound
                        .map(|(data_type, _, _, _)| data_type)
                        .map(Some)
                        .ok_or_else(|| {
                            Error::InvalidArgument(format!(
                                "unknown function {}",
                                function.function
                            ))
                        })
                });
        };
        let direct = match info.signature.return_type {
            FunctionDataType::Integer => Some(DataType::Integer),
            FunctionDataType::Float => Some(DataType::Float),
            FunctionDataType::String => Some(DataType::Text),
            FunctionDataType::Boolean => Some(DataType::Boolean),
            FunctionDataType::Timestamp | FunctionDataType::Time | FunctionDataType::DateTime => {
                Some(DataType::Timestamp)
            }
            FunctionDataType::Date => Some(DataType::Date),
            FunctionDataType::Json => Some(DataType::Json),
            FunctionDataType::Vector => Some(DataType::Vector),
            FunctionDataType::Any | FunctionDataType::Unknown => None,
        };
        if direct.is_some() {
            return Ok(direct);
        }
        let argument_indices: Vec<usize> = match &info.signature.return_rule {
            FunctionReturnRule::Declared => return Ok(None),
            FunctionReturnRule::AllArguments => (0..function.arguments.len()).collect(),
            FunctionReturnRule::Arguments(arguments) => arguments.clone(),
        };
        let mut inferred = None;
        for index in argument_indices {
            let Some(argument) = function.arguments.get(index) else {
                continue;
            };
            if matches!(argument, Expression::Star(_)) {
                continue;
            }
            if let Some(data_type) =
                self.bind_expression_type(argument, scope, ctes, navigation, depth + 1)?
            {
                inferred = Some(match inferred {
                    Some(current) => Self::merge_bound_types(current, data_type)?,
                    None => data_type,
                });
            }
        }
        Ok(inferred)
    }

    fn merge_bound_types(left: DataType, right: DataType) -> Result<DataType> {
        if left == right {
            return Ok(left);
        }
        if matches!(
            left,
            DataType::Integer | DataType::Float | DataType::Decimal
        ) && matches!(
            right,
            DataType::Integer | DataType::Float | DataType::Decimal
        ) {
            return Ok(if left == DataType::Decimal || right == DataType::Decimal {
                DataType::Decimal
            } else if left == DataType::Float || right == DataType::Float {
                DataType::Float
            } else {
                DataType::Integer
            });
        }
        Err(Error::Type(format!(
            "CTAS expression has incompatible bind-time types {left:?} and {right:?}"
        )))
    }

    fn qualify_bound_columns(
        mut columns: Vec<BoundOutputColumn>,
        qualifier: &str,
    ) -> Vec<BoundOutputColumn> {
        let qualifier = qualifier.to_lowercase();
        for column in &mut columns {
            column.qualifier = Some(qualifier.clone());
        }
        columns
    }

    fn bound_expression_name(expression: &Expression, ordinal: usize) -> String {
        match expression {
            Expression::Identifier(identifier) => identifier.value.to_string(),
            Expression::QualifiedIdentifier(identifier) => identifier.name.value.to_string(),
            Expression::Aliased(alias) => alias.alias.value.to_string(),
            Expression::FunctionCall(function) => function.function.to_string(),
            Expression::Cast(cast) => format!("cast_{}", cast.type_name),
            _ => format!("column{}", ordinal + 1),
        }
    }
}

impl<T: OutputBindingHost + ?Sized> OutputBindingExt for T {}
