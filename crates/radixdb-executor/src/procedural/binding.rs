use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{
    ArgumentMode, CatalogDataType, CatalogGeneration, CatalogName, CatalogObject, CatalogPayload,
    ObjectId, ObjectKind, Volatility,
};
use radixdb_core::{DataType, Error};
use radixdb_procedural::{
    admit_embedded_sql, BoundCallArgument, BoundExpression, BoundResultColumn, BoundRoutineCall,
    BoundSqlParameter, BoundSqlStatement, BoundType, CallSiteArgument, CallSiteArgumentValue,
    Diagnostic, DiagnosticKind, LocalBinding, ProceduralResult, RecordField, RuntimeType,
    SemanticResolver,
};
use radixdb_sql::{
    walk_expression_tree, walk_expression_tree_mut, walk_statement_physical_table_sources,
    walk_statement_tree, walk_statement_tree_mut, CastExpression, Expression, InfixExpression,
    InfixOperator, ObjectName, Parser, Position, Precedence, ProceduralType, Statement, Token,
    TokenType,
};

use crate::binding::output::OutputBindingExt;
use crate::Executor;

use super::{
    binding_types::{
        catalog_type_spelling, context_value_type, typed_context_parameter, typed_parameter,
        typed_parameter_in_catalog,
    },
    host::operator_spelling,
};

/// Production semantic adapter from the procedural compiler to the canonical
/// SQL binder and one caller-pinned immutable catalog generation.
pub(crate) struct ExecutorSemanticResolver<'a> {
    executor: &'a Executor,
    catalog: &'a CatalogGeneration,
    search_path: Vec<ObjectId>,
    function_volatility: Option<Volatility>,
}

impl<'a> ExecutorSemanticResolver<'a> {
    pub(crate) fn with_search_path(
        executor: &'a Executor,
        catalog: &'a CatalogGeneration,
        search_path: Vec<ObjectId>,
        function_volatility: Option<Volatility>,
    ) -> Self {
        Self {
            executor,
            catalog,
            search_path,
            function_volatility,
        }
    }

    fn relation(&self, name: &ObjectName) -> ProceduralResult<&CatalogObject> {
        let (namespace, relation) = self.resolve_object_scope(name)?;
        self.catalog
            .find_relation(namespace, relation)
            .map_err(bind_catalog_error)?
            .ok_or_else(|| bind_unknown(format!("relation {name} does not exist")))
    }

    fn resolve_object_scope<'name>(
        &self,
        name: &'name ObjectName,
    ) -> ProceduralResult<(ObjectId, &'name str)> {
        let (last, namespace_path) = name.components.split_last().ok_or_else(|| {
            Diagnostic::new(DiagnosticKind::BindUnknownObject, "object name is empty")
        })?;
        if namespace_path.is_empty() {
            let namespace = self
                .search_path
                .first()
                .copied()
                .ok_or_else(|| bind_unknown("routine search path is empty"))?;
            return Ok((namespace, last.value.as_str()));
        }
        let namespace = self.resolve_namespace(namespace_path)?;
        Ok((namespace, last.value.as_str()))
    }

    fn resolve_namespace(&self, path: &[radixdb_sql::Identifier]) -> ProceduralResult<ObjectId> {
        crate::catalog::resolve_namespace_path(
            self.catalog,
            path.iter().map(|component| component.value.as_str()),
        )
        .map_err(|error| bind_unknown(error.to_string()))
    }

    fn relation_dependency(&self, name: &str) -> ProceduralResult<ObjectId> {
        self.catalog
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
            .map_err(bind_catalog_error)?
            .filter(|object| matches!(object.kind(), ObjectKind::Table | ObjectKind::View))
            .map(CatalogObject::id)
            .ok_or_else(|| bind_unknown(format!("relation '{name}' does not exist")))
    }

    fn bind_statement_dependencies(
        &self,
        statement: &Statement,
    ) -> ProceduralResult<Vec<ObjectId>> {
        let mut dependencies = BTreeSet::new();
        let direct = match statement {
            Statement::Insert(value) => Some(value.table_name.value.as_str()),
            Statement::Update(value) => Some(value.table_name.value.as_str()),
            Statement::Delete(value) => Some(value.table_name.value.as_str()),
            _ => None,
        };
        if let Some(name) = direct {
            dependencies.insert(self.relation_dependency(name)?);
        }
        let mut names = BTreeSet::new();
        walk_statement_physical_table_sources(statement, &mut |source| {
            names.insert(source.name.value.to_string());
        });
        for name in names {
            dependencies.insert(self.relation_dependency(&name)?);
        }
        Ok(dependencies.into_iter().collect())
    }

    fn bind_statement_output(
        &self,
        statement: &Statement,
    ) -> ProceduralResult<Vec<BoundResultColumn>> {
        let columns = match statement {
            Statement::Select(select) => self
                .executor
                .bind_select_output(select, &[], 0)
                .map_err(bind_sql_error)?,
            Statement::Insert(insert) if !insert.returning.is_empty() => self
                .executor
                .bind_returning_output(
                    insert.table_name.value_lower.as_str(),
                    insert.table_name.value_lower.as_str(),
                    &insert.returning,
                )
                .map_err(bind_sql_error)?,
            Statement::Update(update) if !update.returning.is_empty() => self
                .executor
                .bind_returning_output(
                    update.table_name.value_lower.as_str(),
                    update.table_name.value_lower.as_str(),
                    &update.returning,
                )
                .map_err(bind_sql_error)?,
            Statement::Delete(delete) if !delete.returning.is_empty() => self
                .executor
                .bind_returning_output(
                    delete.table_name.value_lower.as_str(),
                    delete
                        .alias
                        .as_ref()
                        .map_or(delete.table_name.value_lower.as_str(), |alias| {
                            alias.value_lower.as_str()
                        }),
                    &delete.returning,
                )
                .map_err(bind_sql_error)?,
            _ => Vec::new(),
        };
        columns
            .into_iter()
            .map(|column| {
                Ok(BoundResultColumn::new(
                    CatalogName::new(column.name).map_err(bind_catalog_error)?,
                    RuntimeType::scalar(
                        CatalogDataType::scalar(column.data_type).map_err(bind_catalog_error)?,
                        column.nullable,
                    ),
                ))
            })
            .collect()
    }

    fn check_expression_capability(&self, expression: &Expression) -> ProceduralResult<()> {
        let Some(caller) = self.function_volatility else {
            return Ok(());
        };
        let mut denied = None;
        walk_expression_tree(expression, &mut |node| {
            let Expression::FunctionCall(function) = node else {
                return;
            };
            let Some(info) = self.executor.function_registry.get_info(&function.function) else {
                return;
            };
            let target = match info.volatility {
                radixdb_functions::FunctionVolatility::Immutable => Volatility::Immutable,
                radixdb_functions::FunctionVolatility::Stable => Volatility::Stable,
                radixdb_functions::FunctionVolatility::Volatile => Volatility::Volatile,
            };
            if !volatility_allows(caller, target) {
                denied = Some(Diagnostic::new(
                    DiagnosticKind::VerifyCapabilityDenied,
                    format!(
                        "{caller:?} function cannot invoke {target:?} built-in {}",
                        function.function
                    ),
                ));
            }
        });
        denied.map_or(Ok(()), Err)
    }

    fn check_statement_expression_capabilities(
        &self,
        statement: &Statement,
    ) -> ProceduralResult<()> {
        let mut error = None;
        walk_statement_tree(statement, &mut |expression| {
            if error.is_none() {
                error = self.check_expression_capability(expression).err();
            }
        });
        error.map_or(Ok(()), Err)
    }
}

impl SemanticResolver for ExecutorSemanticResolver<'_> {
    fn resolve_type(&mut self, syntax: &ProceduralType) -> ProceduralResult<BoundType> {
        match syntax {
            ProceduralType::Scalar(name) => {
                let data_type =
                    crate::catalog::bind_catalog_type_in_generation(name.as_str(), self.catalog)
                        .map_err(bind_sql_error)?;
                Ok(BoundType {
                    runtime_type: RuntimeType::scalar(data_type, true),
                    dependencies: data_type.type_object_id().into_iter().collect(),
                })
            }
            ProceduralType::RowType(table) => {
                let table = self.relation(table)?;
                let CatalogPayload::Table(payload) = table.payload() else {
                    return Err(bind_unknown("%ROWTYPE target is not a table"));
                };
                let fields = payload
                    .column_ids()
                    .iter()
                    .map(|id| {
                        let column = self.catalog.object(*id).ok_or_else(|| {
                            bind_unknown("%ROWTYPE column disappeared from pinned catalog")
                        })?;
                        let CatalogPayload::Column(payload) = column.payload() else {
                            return Err(bind_unknown("%ROWTYPE child is not a column"));
                        };
                        Ok(RecordField::new(
                            column.name().clone(),
                            payload.data_type(),
                            payload.nullable(),
                        ))
                    })
                    .collect::<ProceduralResult<Vec<_>>>()?;
                Ok(BoundType {
                    runtime_type: RuntimeType::record(fields)?,
                    dependencies: vec![table.id()],
                })
            }
        }
    }

    fn bind_expression(
        &mut self,
        expression: &Expression,
        locals: &[LocalBinding],
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<BoundExpression> {
        let locals = locals
            .iter()
            .map(|local| (local.name.as_str(), local))
            .collect::<BTreeMap<_, _>>();
        let source_nullable = expression_may_be_null(expression, &locals, true);
        let mut expression = expression.clone();
        let mut parameters = Vec::new();
        let mut rewrite_error = None;
        walk_expression_tree_mut(&mut expression, &mut |node| {
            let Expression::Identifier(identifier) = node else {
                return;
            };
            if let Some((data_type, _)) = context_value_type(identifier.value_lower()) {
                *node = typed_context_parameter(
                    identifier.value_lower(),
                    data_type,
                    identifier.token.clone(),
                );
                return;
            }
            let Some(local) = locals.get(identifier.value_lower()) else {
                return;
            };
            let RuntimeType::Scalar { data_type, .. } = &local.runtime_type else {
                rewrite_error = Some(Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    "record and collection locals require their typed procedural operators",
                ));
                return;
            };
            parameters.push(local.slot);
            *node = typed_parameter_in_catalog(
                parameters.len(),
                *data_type,
                identifier.token.clone(),
                self.catalog,
            );
        });
        if let Some(error) = rewrite_error {
            return Err(error);
        }
        self.check_expression_capability(&expression)?;

        let (data_type, logical_type, _, inferred_nullable) = self
            .executor
            .bind_scalar_output_metadata(&expression)
            .map_err(bind_sql_error)?;
        let catalog_type = match logical_type {
            radixdb_core::LogicalTypeRef::Builtin(data_type) => {
                CatalogDataType::scalar(data_type).map_err(bind_catalog_error)?
            }
            radixdb_core::LogicalTypeRef::External(external) => CatalogDataType::external(
                radixdb_catalog::ObjectId::from_user_bytes(external.type_object_id())
                    .map_err(bind_catalog_error)?,
                external.codec_version(),
            )
            .map_err(bind_catalog_error)?,
        };
        let result_type = if let Some(expected) = expected {
            let RuntimeType::Scalar {
                data_type: expected_type,
                nullable: expected_nullable,
            } = expected
            else {
                return Err(Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    "scalar expression cannot target a record or collection",
                ));
            };
            if expected_type.logical_type_ref() != logical_type {
                return Err(Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    format!(
                        "expression type {data_type} differs from expected {}",
                        expected_type.logical_type()
                    ),
                ));
            }
            if !expected_nullable && source_nullable && inferred_nullable {
                return Err(Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    "possibly NULL expression cannot target NOT NULL",
                ));
            }
            expected.clone()
        } else {
            RuntimeType::scalar(catalog_type, source_nullable && inferred_nullable)
        };

        let dependencies = super::native_function::bind_expression_dependencies(
            self.executor,
            self.function_volatility,
            &expression,
        )?;
        Ok(BoundExpression {
            expression,
            parameters,
            result_type,
            dependencies,
        })
    }

    fn bind_binary_operator(
        &mut self,
        operator: InfixOperator,
        left: &RuntimeType,
        right: &RuntimeType,
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<RuntimeType> {
        let RuntimeType::Scalar {
            data_type: left_type,
            nullable: left_nullable,
        } = left
        else {
            return Err(type_mismatch("left SQL operator operand is not scalar"));
        };
        let RuntimeType::Scalar {
            data_type: right_type,
            nullable: right_nullable,
        } = right
        else {
            return Err(type_mismatch("right SQL operator operand is not scalar"));
        };
        let spelling = operator_spelling(operator)
            .ok_or_else(|| type_mismatch("unsupported SQL binary operator"))?;
        let token = Token::new(TokenType::Operator, spelling, Position::default());
        let expression = Expression::Infix(InfixExpression::new(
            token.clone(),
            Box::new(typed_parameter_in_catalog(
                1,
                *left_type,
                token.clone(),
                self.catalog,
            )),
            spelling,
            Box::new(typed_parameter_in_catalog(
                2,
                *right_type,
                token,
                self.catalog,
            )),
        ));
        let (data_type, _) = self
            .executor
            .bind_scalar_output(&expression)
            .map_err(bind_sql_error)?;
        let nullable = match operator {
            InfixOperator::Is
            | InfixOperator::IsNot
            | InfixOperator::IsDistinctFrom
            | InfixOperator::IsNotDistinctFrom => false,
            _ => *left_nullable || *right_nullable,
        };
        let actual = RuntimeType::scalar(
            CatalogDataType::scalar(data_type).map_err(bind_catalog_error)?,
            nullable,
        );
        if let Some(expected) = expected {
            let compatible = match (expected, &actual) {
                (
                    RuntimeType::Scalar {
                        data_type: expected_type,
                        nullable: expected_nullable,
                    },
                    RuntimeType::Scalar {
                        data_type: actual_type,
                        nullable: actual_nullable,
                    },
                ) => expected_type == actual_type && (*expected_nullable || !actual_nullable),
                _ => expected == &actual,
            };
            if !compatible {
                return Err(type_mismatch(
                    "SQL binary operator result differs from expected type",
                ));
            }
        }
        Ok(actual)
    }

    fn bind_statement(
        &mut self,
        statement: &Statement,
        locals: &[LocalBinding],
    ) -> ProceduralResult<BoundSqlStatement> {
        admit_embedded_sql(statement)?;
        if self
            .function_volatility
            .is_some_and(|volatility| volatility != Volatility::Volatile)
            && matches!(
                statement,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            )
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot execute DML",
            ));
        }
        let locals = locals
            .iter()
            .map(|local| (local.name.as_str(), local))
            .collect::<BTreeMap<_, _>>();
        let mut statement = statement.clone();
        let mut parameters = Vec::new();
        let mut rewrite_error = None;
        walk_statement_tree_mut(&mut statement, &mut |node| {
            if rewrite_error.is_some() {
                return;
            }
            let Expression::Parameter(parameter) = node else {
                return;
            };
            let Some(name) = parameter.name.strip_prefix(':') else {
                rewrite_error = Some(Diagnostic::new(
                    DiagnosticKind::ParseUnsupportedSyntax,
                    "static stored SQL accepts only :local parameters",
                ));
                return;
            };
            if let Some((data_type, _)) = context_value_type(name) {
                if parameter.field.is_some() {
                    rewrite_error =
                        Some(type_mismatch("context values do not expose record fields"));
                    return;
                }
                *node = typed_context_parameter(name, data_type, parameter.token.clone());
                return;
            }
            let normalized_name = match CatalogName::new(name) {
                Ok(name) => name.normalized().as_str().to_owned(),
                Err(_) => {
                    rewrite_error = Some(Diagnostic::new(
                        DiagnosticKind::BindUnknownLocal,
                        "named SQL parameter is not a valid procedural local",
                    ));
                    return;
                }
            };
            let Some(local) = locals.get(normalized_name.as_str()) else {
                rewrite_error = Some(Diagnostic::new(
                    DiagnosticKind::BindUnknownLocal,
                    format!(":{name} does not name a visible procedural local"),
                ));
                return;
            };
            if let Some(field_name) = &parameter.field {
                let RuntimeType::Record { fields, .. } = &local.runtime_type else {
                    rewrite_error = Some(type_mismatch(
                        "field-qualified SQL parameter source is not a record",
                    ));
                    return;
                };
                let normalized_field = match CatalogName::new(field_name.value()) {
                    Ok(name) => name.normalized().as_str().to_owned(),
                    Err(_) => {
                        rewrite_error = Some(Diagnostic::new(
                            DiagnosticKind::BindUnknownLocal,
                            "record field name is not a valid catalog identifier",
                        ));
                        return;
                    }
                };
                let Some((field, descriptor)) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name().normalized().as_str() == normalized_field)
                else {
                    rewrite_error = Some(Diagnostic::new(
                        DiagnosticKind::BindUnknownLocal,
                        format!("unknown record field {name}.{normalized_field}"),
                    ));
                    return;
                };
                let Ok(field) = u32::try_from(field) else {
                    rewrite_error = Some(Diagnostic::new(
                        DiagnosticKind::ParseLimitExceeded,
                        "record field ordinal exceeds u32",
                    ));
                    return;
                };
                let runtime_type =
                    RuntimeType::scalar(descriptor.data_type(), descriptor.nullable());
                parameters.push(BoundSqlParameter::RecordField {
                    record: local.slot,
                    field,
                    runtime_type,
                });
                *node = typed_parameter_in_catalog(
                    parameters.len(),
                    descriptor.data_type(),
                    parameter.token.clone(),
                    self.catalog,
                );
                return;
            }
            let RuntimeType::Scalar { data_type, .. } = &local.runtime_type else {
                rewrite_error = Some(type_mismatch(
                    "record and collection SQL parameters require an explicit scalar field or element",
                ));
                return;
            };
            parameters.push(BoundSqlParameter::Scalar(local.slot));
            *node = typed_parameter_in_catalog(
                parameters.len(),
                *data_type,
                parameter.token.clone(),
                self.catalog,
            );
        });
        if let Some(error) = rewrite_error {
            return Err(error);
        }
        self.check_statement_expression_capabilities(&statement)?;
        let result_columns = self.bind_statement_output(&statement)?;
        let dependencies = self.bind_statement_dependencies(&statement)?;
        if self.function_volatility == Some(Volatility::Immutable) && !dependencies.is_empty() {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE functions cannot read tables or views",
            ));
        }
        Ok(BoundSqlStatement {
            statement,
            parameters,
            result_columns,
            dependencies,
        })
    }

    fn bind_procedure_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[CallSiteArgument],
    ) -> ProceduralResult<BoundRoutineCall> {
        if self
            .function_volatility
            .is_some_and(|volatility| volatility != Volatility::Volatile)
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot call procedures",
            ));
        }
        let (explicit_namespace, routine_name) = if routine.components.len() > 1 {
            let (namespace, name) = self.resolve_object_scope(routine)?;
            (Some(namespace), name)
        } else {
            let name = routine
                .components
                .first()
                .ok_or_else(|| bind_unknown("procedure name is empty"))?
                .value
                .as_str();
            (None, name)
        };
        let namespaces = explicit_namespace
            .map(|namespace| vec![namespace])
            .unwrap_or_else(|| self.search_path.clone());
        for namespace in namespaces {
            let mut matches = Vec::new();
            for object in self.catalog.objects_of_kind(ObjectKind::Procedure) {
                if object.namespace_id() != Some(namespace)
                    || !object
                        .name()
                        .normalized()
                        .as_str()
                        .eq_ignore_ascii_case(routine_name)
                {
                    continue;
                }
                let CatalogPayload::Procedure(payload) = object.payload() else {
                    unreachable!("catalog kind/payload invariant")
                };
                if let Some(bound) = bind_call_candidate(object, payload.definition(), arguments)? {
                    matches.push(bound);
                }
            }
            match matches.len() {
                0 => continue,
                _ => {
                    let minimum_cost = matches
                        .iter()
                        .map(|candidate| candidate.cost)
                        .min()
                        .expect("non-empty candidate set");
                    matches.retain(|candidate| candidate.cost == minimum_cost);
                    if matches.len() == 1 {
                        return Ok(matches.remove(0));
                    }
                    return Err(Diagnostic::new(
                        DiagnosticKind::BindAmbiguousRoutine,
                        format!("procedure call {routine} has multiple equal-cost overloads"),
                    ));
                }
            }
        }
        Err(bind_unknown(format!(
            "no procedure overload matches call {routine}"
        )))
    }

    fn admit_transactional_side_effect(&mut self) -> ProceduralResult<()> {
        if self
            .function_volatility
            .is_some_and(|volatility| volatility != Volatility::Volatile)
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot append audit or outbox records",
            ));
        }
        Ok(())
    }
}

fn volatility_allows(caller: Volatility, target: Volatility) -> bool {
    match caller {
        Volatility::Volatile => true,
        Volatility::Stable => target != Volatility::Volatile,
        Volatility::Immutable => target == Volatility::Immutable,
    }
}

fn bind_call_candidate(
    object: &CatalogObject,
    definition: &radixdb_catalog::RoutineDefinition,
    arguments: &[CallSiteArgument],
) -> ProceduralResult<Option<BoundRoutineCall>> {
    if arguments.len() > definition.arguments().len() {
        return Ok(None);
    }
    let mut bound = vec![None; definition.arguments().len()];
    let mut positional = 0;
    for argument in arguments {
        let index = if let Some(name) = &argument.name {
            definition
                .arguments()
                .iter()
                .position(|declared| declared.name().normalized().as_str() == name)
                .unwrap_or(definition.arguments().len())
        } else {
            let index = positional;
            positional += 1;
            index
        };
        if index >= bound.len() || bound[index].replace(argument).is_some() {
            return Ok(None);
        }
    }
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut cost = 0u32;
    for (declared, actual) in definition.arguments().iter().zip(bound) {
        let Some(actual) = actual else {
            if declared.mode() == ArgumentMode::In {
                if let Some(default) = declared.default_sql() {
                    inputs.push(BoundCallArgument::Default {
                        declared_name: declared.name().normalized().as_str().to_owned(),
                        expression: parse_default_expression(default.as_str())?,
                        runtime_type: RuntimeType::scalar(
                            declared.data_type(),
                            declared.nullable(),
                        ),
                    });
                    continue;
                }
            }
            return Ok(None);
        };
        match declared.mode() {
            ArgumentMode::In => match &actual.value {
                CallSiteArgumentValue::UntypedNull if declared.nullable() => {
                    inputs.push(contextual_null_argument(declared)?);
                }
                CallSiteArgumentValue::UntypedNull => return Ok(None),
                CallSiteArgumentValue::Bound {
                    slot,
                    runtime_type:
                        RuntimeType::Scalar {
                            data_type,
                            nullable,
                        },
                    ..
                } => {
                    if *nullable && !declared.nullable() {
                        return Ok(None);
                    }
                    let declared_name = declared.name().normalized().as_str().to_owned();
                    if *data_type == declared.data_type() {
                        inputs.push(BoundCallArgument::Provided {
                            declared_name,
                            slot: *slot,
                        });
                    } else if lossless_call_conversion(*data_type, declared.data_type()) {
                        inputs.push(BoundCallArgument::ContextualExpression {
                            declared_name,
                            expression: checked_conversion_expression(
                                *slot,
                                *data_type,
                                declared.data_type(),
                                declared.nullable(),
                            ),
                        });
                        cost = cost.saturating_add(1);
                    } else {
                        return Ok(None);
                    }
                }
                CallSiteArgumentValue::Bound { .. } => return Ok(None),
            },
            ArgumentMode::Out => {
                let CallSiteArgumentValue::Bound {
                    slot,
                    runtime_type:
                        RuntimeType::Scalar {
                            data_type,
                            nullable,
                        },
                    assignable: true,
                } = &actual.value
                else {
                    return Ok(None);
                };
                if *data_type != declared.data_type() || (declared.nullable() && !*nullable) {
                    return Ok(None);
                }
                outputs.push(*slot);
            }
            ArgumentMode::InOut => {
                let CallSiteArgumentValue::Bound {
                    slot,
                    runtime_type:
                        RuntimeType::Scalar {
                            data_type,
                            nullable,
                        },
                    assignable: true,
                } = &actual.value
                else {
                    return Ok(None);
                };
                if *data_type != declared.data_type() || *nullable != declared.nullable() {
                    return Ok(None);
                }
                inputs.push(BoundCallArgument::Provided {
                    declared_name: declared.name().normalized().as_str().to_owned(),
                    slot: *slot,
                });
                outputs.push(*slot);
            }
        }
    }
    Ok(Some(BoundRoutineCall {
        routine: object.id(),
        arguments: inputs,
        results: outputs,
        dependencies: definition.dependency_ids().to_vec(),
        cost,
    }))
}

fn contextual_null_argument(
    declared: &radixdb_catalog::RoutineArgument,
) -> ProceduralResult<BoundCallArgument> {
    Ok(BoundCallArgument::ContextualExpression {
        declared_name: declared.name().normalized().as_str().to_owned(),
        expression: BoundExpression {
            expression: parse_default_expression("NULL")?,
            parameters: Vec::new(),
            result_type: RuntimeType::scalar(declared.data_type(), true),
            dependencies: Vec::new(),
        },
    })
}

fn lossless_call_conversion(source: CatalogDataType, target: CatalogDataType) -> bool {
    matches!(
        (source.logical_type(), target.logical_type()),
        (DataType::Integer, DataType::Decimal) | (DataType::Date, DataType::Timestamp)
    )
}

fn checked_conversion_expression(
    slot: radixdb_procedural::SlotId,
    source: CatalogDataType,
    target: CatalogDataType,
    nullable: bool,
) -> BoundExpression {
    let token = Token::new(TokenType::Keyword, "CAST", Position::default());
    BoundExpression {
        expression: Expression::Cast(CastExpression {
            token: token.clone(),
            expr: Box::new(typed_parameter(1, source, token)),
            type_name: catalog_type_spelling(target).into(),
        }),
        parameters: vec![slot],
        result_type: RuntimeType::scalar(target, nullable),
        dependencies: Vec::new(),
    }
}

fn parse_default_expression(source: &str) -> ProceduralResult<Expression> {
    let mut parser = Parser::new(source);
    let expression = parser.parse_expression(Precedence::Lowest).ok_or_else(|| {
        Diagnostic::new(
            DiagnosticKind::ParseUnsupportedSyntax,
            "stored routine default is not an expression",
        )
    })?;
    if let Some(error) = parser.errors().first() {
        return Err(Diagnostic::new(
            DiagnosticKind::ParseUnsupportedSyntax,
            format!("stored routine default cannot be parsed: {error}"),
        ));
    }
    Ok(expression)
}

fn expression_may_be_null(
    expression: &Expression,
    locals: &BTreeMap<&str, &LocalBinding>,
    binder_default: bool,
) -> bool {
    match expression {
        Expression::Identifier(identifier) => context_value_type(identifier.value_lower())
            .map(|(_, nullable)| nullable)
            .or_else(|| {
                locals
                    .get(identifier.value_lower())
                    .and_then(|local| match local.runtime_type {
                        RuntimeType::Scalar { nullable, .. } => Some(nullable),
                        _ => None,
                    })
            })
            .unwrap_or(binder_default),
        Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_) => false,
        Expression::NullLiteral(_) => true,
        Expression::BoundValue(value) => value.is_null(),
        Expression::Cast(value) => expression_may_be_null(&value.expr, locals, binder_default),
        Expression::Prefix(value) => expression_may_be_null(&value.right, locals, binder_default),
        Expression::Infix(value) => match value.op_type {
            InfixOperator::Is
            | InfixOperator::IsNot
            | InfixOperator::IsDistinctFrom
            | InfixOperator::IsNotDistinctFrom => false,
            _ => {
                expression_may_be_null(&value.left, locals, binder_default)
                    || expression_may_be_null(&value.right, locals, binder_default)
            }
        },
        Expression::Aliased(value) => {
            expression_may_be_null(&value.expression, locals, binder_default)
        }
        Expression::Distinct(value) => expression_may_be_null(&value.expr, locals, binder_default),
        Expression::Case(value) => {
            value
                .else_value
                .as_ref()
                .is_none_or(|expression| expression_may_be_null(expression, locals, binder_default))
                || value.when_clauses.iter().any(|clause| {
                    expression_may_be_null(&clause.then_result, locals, binder_default)
                })
        }
        // SQL functions and subqueries remain nullable until their canonical
        // binder supplies a stronger result contract.
        _ => binder_default,
    }
}

fn bind_unknown(message: impl Into<String>) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::BindUnknownObject, message)
}

fn type_mismatch(message: impl Into<String>) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::BindTypeMismatch, message)
}

fn bind_catalog_error(error: radixdb_catalog::CatalogError) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::BindUnknownObject, error.to_string())
}

fn bind_sql_error(error: Error) -> Diagnostic {
    let kind = match error {
        Error::TableNotFound(_)
        | Error::TableOrViewNotFound(_)
        | Error::ColumnNotFound(_)
        | Error::ViewNotFound(_)
        | Error::IndexNotFound(_) => DiagnosticKind::BindUnknownObject,
        Error::Type(_) | Error::TypeConversion { .. } | Error::InvalidColumnType => {
            DiagnosticKind::BindTypeMismatch
        }
        _ => DiagnosticKind::BindTypeMismatch,
    };
    Diagnostic::new(kind, error.to_string())
}
