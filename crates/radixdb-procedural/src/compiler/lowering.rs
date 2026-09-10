use std::collections::{BTreeMap, BTreeSet};

use radixdb_catalog::{CatalogName, ObjectId};
use radixdb_core::DataType;
use radixdb_sql::{
    AssignmentTargetSyntax, CreateRoutineStatement, DynamicExecuteSyntax, ExceptionHandlerSyntax,
    ExceptionPatternSyntax, Expression, ForSourceSyntax, Identifier, InfixOperator,
    LoopControlKind, ObjectName, ProceduralBlock, ProceduralDeclaration, ProceduralStatement,
    ReturnSyntax, RoutineArgumentMode, RoutineKindSyntax, RoutineReturnSyntax, SourceRange,
};

use super::{
    BoundCallArgument, BoundRoutineCall, BoundSqlParameter, BoundSqlStatement, CallSiteArgument,
    CallSiteArgumentValue, CompileIdentity, CompiledRoutine, LocalBinding, SemanticResolver,
};
use crate::{
    BasicBlock, BlockId, CursorId, CursorStatusAttribute, Diagnostic, DiagnosticKind,
    ExceptionRoute, Instruction, ProceduralResult, Program, ProgramIdentity, RecordField,
    RuntimeType, SlotDefinition, SlotId, SourceSpan, SpannedInstruction, SpannedTerminator,
    SqlStatusAttribute, Terminator, TriggerCompileContext, TriggerReturnRecord,
};

mod helpers;

use helpers::{bind_error, handler_observes_or_rethrows, reachable_labels, span, statement_span};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Label(usize);

#[derive(Debug)]
enum DraftTerminator {
    Jump(Label),
    Branch {
        condition: SlotId,
        when_true: Label,
        when_false: Label,
    },
    Return(Option<SlotId>),
    Raise(DiagnosticKind),
    Rethrow,
}

#[derive(Debug, Clone)]
struct DraftExceptionRoute {
    kinds: Vec<DiagnosticKind>,
    handler: Label,
    error_slot: Option<SlotId>,
}

#[derive(Debug, Clone)]
enum DraftInstruction {
    Concrete(SpannedInstruction),
    EnterExceptionRegion {
        routes: Vec<DraftExceptionRoute>,
        span: Option<SourceSpan>,
    },
}

#[derive(Debug, Default)]
struct DraftBlock {
    instructions: Vec<DraftInstruction>,
    terminator: Option<(DraftTerminator, Option<SourceSpan>)>,
}

#[derive(Debug)]
struct LoopFrame {
    break_target: Label,
    continue_target: Label,
    has_break: bool,
}

#[derive(Debug, Clone)]
struct CursorBinding {
    cursor: CursorId,
    argument_slots: Vec<SlotId>,
    argument_types: Vec<RuntimeType>,
    query: BoundSqlStatement,
}

struct Lowering<'a, R> {
    identity: CompileIdentity,
    resolver: &'a mut R,
    slots: Vec<SlotDefinition>,
    parameter_slots: Vec<SlotId>,
    output_slots: Vec<SlotId>,
    scopes: Vec<BTreeMap<String, LocalBinding>>,
    cursor_scopes: Vec<BTreeMap<String, CursorBinding>>,
    next_cursor: u32,
    blocks: Vec<DraftBlock>,
    current: Option<Label>,
    loops: Vec<LoopFrame>,
    dependencies: BTreeSet<ObjectId>,
    result_type: Option<RuntimeType>,
    result_columns: Vec<RuntimeType>,
    handler_depth: usize,
    trigger_return_record: Option<Option<TriggerReturnRecord>>,
}

pub fn compile_routine<R: SemanticResolver>(
    syntax: &CreateRoutineStatement,
    identity: CompileIdentity,
    resolver: &mut R,
) -> ProceduralResult<CompiledRoutine> {
    if identity.definition_revision == 0 || identity.display_name.is_empty() {
        return Err(bind_error(
            DiagnosticKind::BindUnknownObject,
            "routine compile identity is invalid",
            None,
        ));
    }
    let mut lowering = Lowering::new(identity, resolver, syntax, None)?;
    lowering.compile_root(syntax)?;
    lowering.finish()
}

pub fn compile_trigger_routine<R: SemanticResolver>(
    syntax: &CreateRoutineStatement,
    identity: CompileIdentity,
    resolver: &mut R,
    context: &TriggerCompileContext,
) -> ProceduralResult<CompiledRoutine> {
    context.validate()?;
    if !syntax.arguments.is_empty()
        || syntax.kind != RoutineKindSyntax::Function
        || !matches!(syntax.returns, Some(RoutineReturnSyntax::Trigger))
    {
        return Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "trigger entrypoint must be a zero-argument Function returning TRIGGER",
            Some(span(&identity, &syntax.span)?),
        ));
    }
    let mut lowering = Lowering::new(identity, resolver, syntax, Some(context))?;
    lowering.compile_root(syntax)?;
    lowering.finish()
}

impl<'a, R: SemanticResolver> Lowering<'a, R> {
    fn new(
        identity: CompileIdentity,
        resolver: &'a mut R,
        syntax: &CreateRoutineStatement,
        trigger: Option<&TriggerCompileContext>,
    ) -> ProceduralResult<Self> {
        let mut dependencies = BTreeSet::new();
        let (result_type, result_columns) = match (&syntax.kind, &syntax.returns) {
            (RoutineKindSyntax::Procedure, None) => (None, Vec::new()),
            (
                RoutineKindSyntax::Function | RoutineKindSyntax::Procedure,
                Some(RoutineReturnSyntax::Scalar {
                    data_type,
                    nullable,
                }),
            ) => {
                let resolved = resolver.resolve_type(data_type)?;
                dependencies.extend(resolved.dependencies);
                let resolved =
                    with_nullability(resolved.runtime_type, *nullable, &syntax.span, &identity)?;
                (Some(resolved), Vec::new())
            }
            (
                RoutineKindSyntax::Function | RoutineKindSyntax::Procedure,
                Some(RoutineReturnSyntax::Table(columns)),
            ) => {
                let mut result_columns = Vec::with_capacity(columns.len());
                for column in columns {
                    let resolved = resolver.resolve_type(&column.data_type)?;
                    dependencies.extend(resolved.dependencies);
                    result_columns.push(with_nullability(
                        resolved.runtime_type,
                        column.nullable,
                        &syntax.span,
                        &identity,
                    )?);
                }
                (None, result_columns)
            }
            (RoutineKindSyntax::Function, Some(RoutineReturnSyntax::Trigger)) => {
                let context = trigger.ok_or_else(|| {
                    bind_error(
                        DiagnosticKind::ParseUnsupportedSyntax,
                        "TRIGGER result requires a table-specialized compile context",
                        Some(span(&identity, &syntax.span).expect("validated routine span")),
                    )
                })?;
                (
                    Some(RuntimeType::nullable_record(context.record_fields.clone())?),
                    Vec::new(),
                )
            }
            (RoutineKindSyntax::Function, None)
            | (RoutineKindSyntax::Procedure, Some(RoutineReturnSyntax::Trigger)) => {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "routine kind and result contract are inconsistent",
                    Some(span(&identity, &syntax.span)?),
                ));
            }
        };
        let mut lowering = Self {
            identity,
            resolver,
            slots: Vec::new(),
            parameter_slots: Vec::new(),
            output_slots: Vec::new(),
            scopes: vec![BTreeMap::new()],
            cursor_scopes: vec![BTreeMap::new()],
            next_cursor: 0,
            blocks: vec![DraftBlock::default()],
            current: Some(Label(0)),
            loops: Vec::new(),
            dependencies,
            result_type,
            result_columns,
            handler_depth: 0,
            trigger_return_record: trigger.map(|context| context.return_record),
        };
        if let Some(context) = trigger {
            lowering.install_trigger_context(context)?;
        }
        Ok(lowering)
    }

    fn install_trigger_context(&mut self, context: &TriggerCompileContext) -> ProceduralResult<()> {
        let record_type = RuntimeType::nullable_record(context.record_fields.clone())?;
        if context.old_available {
            self.declare_intrinsic_parameter("old", record_type.clone(), true)?;
        }
        if context.new_available {
            self.declare_intrinsic_parameter("new", record_type, !context.new_writable)?;
        }
        let text = scalar_runtime_type(DataType::Text, false)?;
        for name in ["trigger_event", "trigger_level", "trigger_table"] {
            self.declare_intrinsic_parameter(name, text.clone(), true)?;
        }
        Ok(())
    }

    fn declare_intrinsic_parameter(
        &mut self,
        name: &str,
        runtime_type: RuntimeType,
        constant: bool,
    ) -> ProceduralResult<()> {
        let normalized = CatalogName::new(name)
            .map_err(|_| {
                bind_error(
                    DiagnosticKind::BindUnknownObject,
                    "invalid intrinsic name",
                    None,
                )
            })?
            .normalized()
            .as_str()
            .to_owned();
        let scope = self.scopes.last_mut().expect("root lexical scope");
        if scope.contains_key(&normalized) {
            return Err(bind_error(
                DiagnosticKind::BindAmbiguousRoutine,
                "duplicate trigger context name",
                None,
            ));
        }
        let slot = SlotId(u32::try_from(self.slots.len()).map_err(|_| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "procedural slot count exceeds u32",
                None,
            )
        })?);
        self.slots.push(SlotDefinition::new(
            normalized.clone(),
            runtime_type.clone(),
        ));
        scope.insert(
            normalized.clone(),
            LocalBinding {
                name: normalized,
                slot,
                runtime_type,
                constant,
            },
        );
        self.parameter_slots.push(slot);
        Ok(())
    }

    fn compile_root(&mut self, syntax: &CreateRoutineStatement) -> ProceduralResult<()> {
        for argument in &syntax.arguments {
            let resolved = self.resolver.resolve_type(&argument.data_type)?;
            self.dependencies.extend(resolved.dependencies);
            let runtime_type = declaration_type(
                resolved.runtime_type,
                argument.nullable,
                &argument.data_type,
                &argument.span,
                &self.identity,
            )?;
            if let Some(default) = &argument.default {
                if argument.mode == RoutineArgumentMode::Out {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "OUT argument cannot have a default",
                        Some(span(&self.identity, &argument.span)?),
                    ));
                }
                let bound = self.resolver.bind_expression(
                    default,
                    &self.visible_locals(),
                    Some(&runtime_type),
                )?;
                if bound.result_type != runtime_type {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "argument default differs from its declared type",
                        Some(span(&self.identity, &argument.span)?),
                    ));
                }
                self.dependencies.extend(bound.dependencies);
            }
            let slot = self.declare(&argument.name, runtime_type, false, &argument.span)?;
            match argument.mode {
                RoutineArgumentMode::In => self.parameter_slots.push(slot),
                RoutineArgumentMode::InOut => {
                    self.parameter_slots.push(slot);
                    self.output_slots.push(slot);
                }
                RoutineArgumentMode::Out if argument.nullable => self.emit(
                    Instruction::InitializeNull { destination: slot },
                    Some(span(&self.identity, &argument.span)?),
                )?,
                RoutineArgumentMode::Out => {}
            }
            if argument.mode == RoutineArgumentMode::Out {
                self.output_slots.push(slot);
            }
        }
        let body = syntax.body.as_ref().ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::ParseUnsupportedSyntax,
                "LANGUAGE NATIVE functions are bound by the database host, not the PL compiler",
            )
        })?;
        self.compile_block_contents(body, false)?;
        if let Some(current) = self.current {
            if self.result_type.is_some() {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "function has an execution path without RETURN",
                    Some(span(&self.identity, &body.span)?),
                ));
            }
            self.terminate_at(current, DraftTerminator::Return(None), None)?;
            self.current = None;
        }
        Ok(())
    }

    fn compile_block_contents(
        &mut self,
        block: &ProceduralBlock,
        nested_scope: bool,
    ) -> ProceduralResult<()> {
        if nested_scope {
            self.scopes.push(BTreeMap::new());
            self.cursor_scopes.push(BTreeMap::new());
        }
        for declaration in &block.declarations {
            self.compile_declaration(declaration)?;
        }
        if block.handlers.is_empty() {
            self.compile_statements(&block.statements)?;
        } else {
            self.compile_exception_block(&block.statements, &block.handlers, &block.span)?;
        }
        if nested_scope {
            self.scopes.pop();
            self.cursor_scopes.pop();
        }
        Ok(())
    }

    fn compile_exception_block(
        &mut self,
        statements: &[ProceduralStatement],
        handlers: &[ExceptionHandlerSyntax],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let mut routes = Vec::with_capacity(handlers.len());
        let error_type = exception_record_type()?;
        let mut catch_all_seen = false;
        for (index, handler) in handlers.iter().enumerate() {
            let mut kinds = Vec::new();
            for pattern in &handler.patterns {
                match pattern {
                    ExceptionPatternSyntax::Named(name) => {
                        if catch_all_seen {
                            return Err(bind_error(
                                DiagnosticKind::BindTypeMismatch,
                                "named exception handler follows OTHERS",
                                Some(span(&self.identity, &handler.span)?),
                            ));
                        }
                        kinds.push(exception_kind(name, &self.identity, &handler.span)?);
                    }
                    ExceptionPatternSyntax::Others => {
                        if catch_all_seen
                            || handler.patterns.len() != 1
                            || index + 1 != handlers.len()
                        {
                            return Err(bind_error(
                                DiagnosticKind::BindTypeMismatch,
                                "OTHERS must be the sole pattern of the final handler",
                                Some(span(&self.identity, &handler.span)?),
                            ));
                        }
                        catch_all_seen = true;
                    }
                }
            }
            if kinds.is_empty() && !handler_observes_or_rethrows(self.resolver, &handler.statements)
            {
                return Err(bind_error(
                    DiagnosticKind::VerifyCapabilityDenied,
                    "catch-all handler must emit system observability or rethrow",
                    Some(span(&self.identity, &handler.span)?),
                ));
            }
            let error_slot = handler
                .alias
                .as_ref()
                .map(|_| self.new_temp("caught_error", error_type.clone()));
            routes.push(DraftExceptionRoute {
                kinds,
                handler: self.new_label(),
                error_slot,
            });
        }
        self.emit_exception_region(routes.clone(), Some(span(&self.identity, source)?))?;
        self.compile_statements(statements)?;
        let mut fallthrough = Vec::new();
        if let Some(current) = self.current.take() {
            self.current = Some(current);
            self.emit(
                Instruction::LeaveExceptionRegion,
                Some(span(&self.identity, source)?),
            )?;
            fallthrough.push(current);
            self.current = None;
        }
        for (handler, route) in handlers.iter().zip(routes) {
            self.current = Some(route.handler);
            self.scopes.push(BTreeMap::new());
            if let (Some(alias), Some(error_slot)) = (&handler.alias, route.error_slot) {
                let name = normalized_identifier(alias)?;
                self.scopes.last_mut().expect("handler scope").insert(
                    name.clone(),
                    LocalBinding {
                        name,
                        slot: error_slot,
                        runtime_type: error_type.clone(),
                        constant: true,
                    },
                );
            }
            self.handler_depth += 1;
            self.compile_statements(&handler.statements)?;
            self.handler_depth -= 1;
            self.scopes.pop();
            if let Some(current) = self.current.take() {
                self.current = Some(current);
                self.emit(
                    Instruction::LeaveExceptionRegion,
                    Some(span(&self.identity, &handler.span)?),
                )?;
                fallthrough.push(current);
                self.current = None;
            }
        }
        if fallthrough.is_empty() {
            return Ok(());
        }
        let join = self.new_label();
        for label in fallthrough {
            self.terminate_at(label, DraftTerminator::Jump(join), None)?;
        }
        self.current = Some(join);
        Ok(())
    }

    fn compile_declaration(&mut self, declaration: &ProceduralDeclaration) -> ProceduralResult<()> {
        match declaration {
            ProceduralDeclaration::Variable {
                name,
                constant,
                data_type,
                nullable,
                initializer,
                span: source,
                ..
            } => {
                let resolved = self.resolver.resolve_type(data_type)?;
                self.dependencies.extend(resolved.dependencies);
                let runtime_type = declaration_type(
                    resolved.runtime_type,
                    *nullable,
                    data_type,
                    source,
                    &self.identity,
                )?;
                let destination = self.declare(name, runtime_type.clone(), *constant, source)?;
                if let Some(initializer) = initializer {
                    let value =
                        self.compile_expression(initializer, Some(&runtime_type), source)?;
                    self.require_same_type(destination, value, source)?;
                    self.emit(
                        Instruction::Copy {
                            destination,
                            source: value,
                        },
                        Some(span(&self.identity, source)?),
                    )?;
                } else if *constant {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "CONSTANT declaration requires an initializer",
                        Some(span(&self.identity, source)?),
                    ));
                } else if *nullable {
                    self.emit(
                        Instruction::InitializeNull { destination },
                        Some(span(&self.identity, source)?),
                    )?;
                } else {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "NOT NULL declaration requires an initializer",
                        Some(span(&self.identity, source)?),
                    ));
                }
            }
            ProceduralDeclaration::Collection {
                name,
                element_type,
                capacity,
                span: source,
                ..
            } => {
                let element = self.resolver.resolve_type(element_type)?;
                self.dependencies.extend(element.dependencies);
                let RuntimeType::Scalar { data_type, .. } = element.runtime_type else {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "collection element type must be scalar",
                        Some(span(&self.identity, source)?),
                    ));
                };
                let collection = RuntimeType::collection(data_type, *capacity)?;
                self.declare(name, collection, false, source)?;
            }
            ProceduralDeclaration::Cursor {
                name,
                arguments,
                query,
                span: source,
                ..
            } => self.compile_cursor_declaration(name, arguments, query, source)?,
        }
        Ok(())
    }

    fn compile_statements(&mut self, statements: &[ProceduralStatement]) -> ProceduralResult<()> {
        for statement in statements {
            if self.current.is_none() {
                return Err(bind_error(
                    DiagnosticKind::VerifyCapabilityDenied,
                    "unreachable procedural statement",
                    statement_span(statement)
                        .map(|source| span(&self.identity, source))
                        .transpose()?,
                ));
            }
            self.compile_statement(statement)?;
        }
        Ok(())
    }

    fn compile_statement(&mut self, statement: &ProceduralStatement) -> ProceduralResult<()> {
        match statement {
            ProceduralStatement::Assignment {
                target,
                value,
                span: source,
                ..
            } => self.compile_assignment(target, value, source),
            ProceduralStatement::Perform {
                expression,
                span: source,
                ..
            } => {
                self.compile_expression(expression, None, source)?;
                Ok(())
            }
            ProceduralStatement::Call {
                routine,
                arguments,
                span: source,
                ..
            } => self.compile_call(routine, arguments, source),
            ProceduralStatement::If {
                branches,
                otherwise,
                span: source,
                ..
            } => self.compile_if(branches, otherwise, source),
            ProceduralStatement::Case {
                operand: None,
                arms,
                otherwise,
                span: source,
                ..
            } => {
                let branches = arms
                    .iter()
                    .map(|arm| (arm.condition.clone(), arm.statements.clone()))
                    .collect::<Vec<_>>();
                self.compile_if(&branches, otherwise, source)
            }
            ProceduralStatement::Case {
                operand: Some(operand),
                arms,
                otherwise,
                span: source,
                ..
            } => self.compile_simple_case(operand, arms, otherwise, source),
            ProceduralStatement::Loop {
                statements,
                span: source,
                ..
            } => self.compile_loop(statements, source),
            ProceduralStatement::While {
                condition,
                statements,
                span: source,
                ..
            } => self.compile_while(condition, statements, source),
            ProceduralStatement::For {
                variable,
                source: for_source,
                statements,
                span: source,
                ..
            } => self.compile_for(variable, for_source, statements, source),
            ProceduralStatement::LoopControl {
                kind,
                condition,
                span: source,
                ..
            } => self.compile_loop_control(kind.clone(), condition.as_ref(), source),
            ProceduralStatement::Return {
                value,
                span: source,
                ..
            } => self.compile_return(value, source),
            ProceduralStatement::Sql(sql) => {
                self.compile_sql(sql.statement.as_ref(), &sql.into, sql.strict, &sql.span)
            }
            ProceduralStatement::DynamicExecute {
                execute,
                span: source,
                ..
            } => self.compile_dynamic(execute, source),
            ProceduralStatement::Raise {
                kind,
                arguments,
                span: source,
                ..
            } => self.compile_raise(kind.as_ref(), arguments, source),
            ProceduralStatement::Block(block) => self.compile_block_contents(block, true),
            ProceduralStatement::OpenCursor {
                cursor,
                arguments,
                span: source,
                ..
            } => self.compile_open_cursor(cursor, arguments, source),
            ProceduralStatement::FetchCursor {
                cursor,
                into,
                span: source,
                ..
            } => self.compile_fetch_cursor(cursor, into, source),
            ProceduralStatement::CloseCursor {
                cursor,
                span: source,
                ..
            } => self.compile_close_cursor(cursor, source),
        }
    }

    fn compile_cursor_declaration(
        &mut self,
        name: &Identifier,
        arguments: &[radixdb_sql::RoutineArgumentSyntax],
        query: &radixdb_sql::Statement,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        if !matches!(query, radixdb_sql::Statement::Select(_)) {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "cursor declaration requires SELECT",
                Some(span(&self.identity, source)?),
            ));
        }
        let name = normalized_identifier(name)?;
        if self
            .cursor_scopes
            .last()
            .expect("cursor scope")
            .contains_key(&name)
        {
            return Err(bind_error(
                DiagnosticKind::BindAmbiguousRoutine,
                "duplicate cursor name in one lexical scope",
                Some(span(&self.identity, source)?),
            ));
        }
        let mut cursor_locals = self.visible_locals();
        let mut argument_slots = Vec::with_capacity(arguments.len());
        let mut argument_types = Vec::with_capacity(arguments.len());
        for argument in arguments {
            if argument.mode != RoutineArgumentMode::In || argument.default.is_some() {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "cursor arguments are required IN parameters",
                    Some(span(&self.identity, &argument.span)?),
                ));
            }
            let resolved = self.resolver.resolve_type(&argument.data_type)?;
            self.dependencies.extend(resolved.dependencies);
            let runtime_type = with_nullability(
                resolved.runtime_type,
                argument.nullable,
                &argument.span,
                &self.identity,
            )?;
            let slot = self.new_temp("cursor_argument", runtime_type.clone());
            cursor_locals.push(LocalBinding {
                name: normalized_identifier(&argument.name)?,
                slot,
                runtime_type: runtime_type.clone(),
                constant: true,
            });
            argument_slots.push(slot);
            argument_types.push(runtime_type);
        }
        let bound = self.resolver.bind_statement(query, &cursor_locals)?;
        if !matches!(bound.statement, radixdb_sql::Statement::Select(_))
            || bound.result_columns.is_empty()
        {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "cursor binding must produce a non-empty SELECT row",
                Some(span(&self.identity, source)?),
            ));
        }
        self.dependencies.extend(bound.dependencies.iter().copied());
        let cursor = CursorId(self.next_cursor);
        self.next_cursor = self.next_cursor.checked_add(1).ok_or_else(|| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "cursor count exceeds u32",
                span(&self.identity, source).ok(),
            )
        })?;
        self.cursor_scopes.last_mut().expect("cursor scope").insert(
            name,
            CursorBinding {
                cursor,
                argument_slots,
                argument_types,
                query: bound,
            },
        );
        Ok(())
    }

    fn compile_open_cursor(
        &mut self,
        name: &Identifier,
        arguments: &[radixdb_sql::Expression],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let cursor = self.resolve_cursor(name)?.clone();
        if cursor.argument_slots.len() != arguments.len() {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "OPEN argument count differs from cursor declaration",
                Some(span(&self.identity, source)?),
            ));
        }
        for ((argument, expected), destination) in arguments
            .iter()
            .zip(&cursor.argument_types)
            .zip(&cursor.argument_slots)
        {
            let value = self.compile_expression(argument, Some(expected), source)?;
            self.emit(
                Instruction::Copy {
                    destination: *destination,
                    source: value,
                },
                Some(span(&self.identity, source)?),
            )?;
        }
        let parameters = self.materialize_sql_parameters(&cursor.query.parameters, source)?;
        self.emit(
            Instruction::OpenCursor {
                cursor: cursor.cursor,
                statement: Box::new(cursor.query.statement),
                parameters,
            },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_fetch_cursor(
        &mut self,
        name: &Identifier,
        into: &[Identifier],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let cursor = self.resolve_cursor(name)?.clone();
        let targets = self.resolve_cursor_targets(into, &cursor.query.result_columns, source)?;
        let found = self.boolean_temp("cursor_found")?;
        self.emit(
            Instruction::FetchCursor {
                cursor: cursor.cursor,
                into: targets,
                found,
            },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_close_cursor(
        &mut self,
        name: &Identifier,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let cursor = self.resolve_cursor(name)?.cursor;
        self.emit(
            Instruction::CloseCursor { cursor },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_assignment(
        &mut self,
        target: &AssignmentTargetSyntax,
        value: &radixdb_sql::Expression,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        match target {
            AssignmentTargetSyntax::Name(name) => {
                if name.components.len() == 2 {
                    let record = self
                        .resolve_assignable(&name.components[0], source)?
                        .clone();
                    let (field, field_type) = record_field(
                        &record.runtime_type,
                        &name.components[1],
                        source,
                        &self.identity,
                    )?;
                    let value = self.compile_expression(value, Some(&field_type), source)?;
                    self.emit(
                        Instruction::WriteRecordField {
                            record: record.slot,
                            field,
                            value,
                        },
                        Some(span(&self.identity, source)?),
                    )
                } else {
                    let target = self.resolve_assignable_name(name, source)?.clone();
                    let value =
                        self.compile_expression(value, Some(&target.runtime_type), source)?;
                    self.require_same_type(target.slot, value, source)?;
                    self.emit(
                        Instruction::Copy {
                            destination: target.slot,
                            source: value,
                        },
                        Some(span(&self.identity, source)?),
                    )
                }
            }
            AssignmentTargetSyntax::Index { collection, index } => {
                let collection = self.resolve_assignable_name(collection, source)?.clone();
                let RuntimeType::Collection { element_type, .. } = collection.runtime_type else {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "indexed assignment target is not a collection",
                        Some(span(&self.identity, source)?),
                    ));
                };
                let index = self.compile_integer_expression(index, source)?;
                let expected = RuntimeType::scalar(element_type, true);
                let value = self.compile_expression(value, Some(&expected), source)?;
                self.emit(
                    Instruction::CollectionSet {
                        collection: collection.slot,
                        one_based_index: index,
                        value,
                    },
                    Some(span(&self.identity, source)?),
                )
            }
        }
    }

    fn compile_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[radixdb_sql::CallArgumentSyntax],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        if self.compile_system_call(routine, arguments, source)? {
            return Ok(());
        }
        if self.compile_collection_call(routine, arguments, source)? {
            return Ok(());
        }
        let mut call_arguments = Vec::with_capacity(arguments.len());
        for argument in arguments {
            if matches!(argument.value, Expression::NullLiteral(_)) {
                call_arguments.push(CallSiteArgument {
                    name: argument
                        .name
                        .as_ref()
                        .map(normalized_identifier)
                        .transpose()?,
                    value: CallSiteArgumentValue::UntypedNull,
                });
                continue;
            }
            let local = expression_local(&argument.value)
                .and_then(|name| self.resolve_local(name).ok())
                .cloned();
            let slot = if let Some(local) = &local {
                local.slot
            } else {
                self.compile_expression(&argument.value, None, source)?
            };
            let binding = self.slot(slot)?.clone();
            let assignable = local.is_some_and(|local| !local.constant);
            call_arguments.push(CallSiteArgument {
                name: argument
                    .name
                    .as_ref()
                    .map(normalized_identifier)
                    .transpose()?,
                value: CallSiteArgumentValue::Bound {
                    slot,
                    runtime_type: binding.runtime_type().clone(),
                    assignable,
                },
            });
        }
        let bound = self
            .resolver
            .bind_procedure_call(routine, &call_arguments)?;
        let BoundRoutineCall {
            routine,
            arguments,
            results,
            dependencies,
            cost: _,
        } = bound;
        self.dependencies.insert(routine);
        self.dependencies.extend(dependencies);
        self.scopes.push(BTreeMap::new());
        let mut input_slots = Vec::with_capacity(arguments.len());
        for argument in arguments {
            let (declared_name, slot, runtime_type) = match argument {
                BoundCallArgument::Provided {
                    declared_name,
                    slot,
                } => {
                    let runtime_type = self.slot(slot)?.runtime_type().clone();
                    (declared_name, slot, runtime_type)
                }
                BoundCallArgument::Default {
                    declared_name,
                    expression,
                    runtime_type,
                } => {
                    let slot = self.compile_expression(&expression, Some(&runtime_type), source)?;
                    (declared_name, slot, runtime_type)
                }
                BoundCallArgument::ContextualExpression {
                    declared_name,
                    expression,
                } => {
                    let runtime_type = expression.result_type.clone();
                    let slot = self.emit_bound_expression(expression, None, source)?;
                    (declared_name, slot, runtime_type)
                }
            };
            self.scopes.last_mut().expect("call default scope").insert(
                declared_name.clone(),
                LocalBinding {
                    name: declared_name,
                    slot,
                    runtime_type,
                    constant: true,
                },
            );
            input_slots.push(slot);
        }
        self.scopes.pop();
        self.emit(
            Instruction::Call {
                routine,
                arguments: input_slots,
                results,
            },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_system_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[radixdb_sql::CallArgumentSyntax],
        source: &SourceRange,
    ) -> ProceduralResult<bool> {
        if routine.components.len() != 2 {
            return Ok(false);
        }
        let namespace = normalized_identifier(&routine.components[0])?;
        if namespace != "system" {
            return Ok(false);
        }
        let operation = normalized_identifier(&routine.components[1])?;
        if !matches!(operation.as_str(), "append_audit" | "append_outbox") {
            return Ok(false);
        }
        if arguments.iter().any(|argument| argument.name.is_some()) {
            return Err(bind_error(
                DiagnosticKind::ParseUnsupportedSyntax,
                "system append primitives accept positional arguments only",
                Some(span(&self.identity, source)?),
            ));
        }
        self.resolver.admit_transactional_side_effect()?;
        match operation.as_str() {
            "append_audit" if arguments.len() == 2 => {
                let command_fingerprint = self.compile_expression(
                    &arguments[0].value,
                    Some(&scalar_runtime_type(DataType::Bytes, true)?),
                    source,
                )?;
                let metadata = self.compile_expression(
                    &arguments[1].value,
                    Some(&scalar_runtime_type(DataType::Json, true)?),
                    source,
                )?;
                self.emit(
                    Instruction::AppendAudit {
                        object_id: self.identity.object_id,
                        command_fingerprint,
                        metadata,
                    },
                    Some(span(&self.identity, source)?),
                )?;
            }
            "append_outbox" if arguments.len() == 3 => {
                let idempotency_key = self.compile_expression(
                    &arguments[0].value,
                    Some(&scalar_runtime_type(DataType::Text, true)?),
                    source,
                )?;
                let schema_version = self.compile_expression(
                    &arguments[1].value,
                    Some(&scalar_runtime_type(DataType::Integer, true)?),
                    source,
                )?;
                let payload = self.compile_expression(
                    &arguments[2].value,
                    Some(&scalar_runtime_type(DataType::Json, true)?),
                    source,
                )?;
                self.emit(
                    Instruction::AppendOutbox {
                        idempotency_key,
                        schema_version,
                        payload,
                    },
                    Some(span(&self.identity, source)?),
                )?;
            }
            _ => {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "SYSTEM.APPEND_AUDIT expects 2 arguments and SYSTEM.APPEND_OUTBOX expects 3",
                    Some(span(&self.identity, source)?),
                ));
            }
        }
        Ok(true)
    }

    fn compile_collection_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[radixdb_sql::CallArgumentSyntax],
        source: &SourceRange,
    ) -> ProceduralResult<bool> {
        if routine.components.len() != 2 {
            return Ok(false);
        }
        let Some(collection) = self
            .resolve_local(&normalized_identifier(&routine.components[0])?)
            .ok()
            .cloned()
        else {
            return Ok(false);
        };
        if !matches!(collection.runtime_type, RuntimeType::Collection { .. }) {
            return Ok(false);
        }
        let method = normalized_identifier(&routine.components[1])?;
        match method.as_str() {
            "append" if arguments.len() == 1 && arguments[0].name.is_none() => {
                let RuntimeType::Collection { element_type, .. } = collection.runtime_type else {
                    unreachable!()
                };
                let expected = RuntimeType::scalar(element_type, true);
                let value =
                    self.compile_expression(&arguments[0].value, Some(&expected), source)?;
                self.emit(
                    Instruction::CollectionAppend {
                        collection: collection.slot,
                        value,
                    },
                    Some(span(&self.identity, source)?),
                )?;
            }
            "clear" if arguments.is_empty() => self.emit(
                Instruction::CollectionClear {
                    collection: collection.slot,
                },
                Some(span(&self.identity, source)?),
            )?,
            _ => {
                return Err(bind_error(
                    DiagnosticKind::BindUnknownObject,
                    "unknown collection method or invalid argument count",
                    Some(span(&self.identity, source)?),
                ));
            }
        }
        Ok(true)
    }

    fn compile_if(
        &mut self,
        branches: &[(radixdb_sql::Expression, Vec<ProceduralStatement>)],
        otherwise: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let mut fallthrough = Vec::new();
        for (condition, statements) in branches {
            let condition = self.compile_condition(condition, source)?;
            let body = self.new_label();
            let next = self.new_label();
            self.terminate_current(
                DraftTerminator::Branch {
                    condition,
                    when_true: body,
                    when_false: next,
                },
                Some(span(&self.identity, source)?),
            )?;
            self.current = Some(body);
            self.compile_statements(statements)?;
            if let Some(label) = self.current.take() {
                fallthrough.push(label);
            }
            self.current = Some(next);
        }
        self.compile_statements(otherwise)?;
        if let Some(label) = self.current.take() {
            fallthrough.push(label);
        }
        if fallthrough.is_empty() {
            self.current = None;
            return Ok(());
        }
        let join = self.new_label();
        for label in fallthrough {
            self.terminate_at(label, DraftTerminator::Jump(join), None)?;
        }
        self.current = Some(join);
        Ok(())
    }

    fn compile_simple_case(
        &mut self,
        operand: &Expression,
        arms: &[radixdb_sql::CaseArmSyntax],
        otherwise: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let operand = self.compile_expression(operand, None, source)?;
        let operand_type = self.slot(operand)?.runtime_type().clone();
        let mut fallthrough = Vec::new();
        for arm in arms {
            let candidate = self.compile_expression(&arm.condition, Some(&operand_type), source)?;
            let boolean = scalar_runtime_type(DataType::Boolean, true)?;
            let candidate_type = self.slot(candidate)?.runtime_type().clone();
            let result_type = self.resolver.bind_binary_operator(
                InfixOperator::Equal,
                &operand_type,
                &candidate_type,
                Some(&boolean),
            )?;
            let result_type =
                admit_result_type(Some(&boolean), result_type, source, &self.identity)?;
            let condition = self.new_temp("case_match", result_type);
            self.emit(
                Instruction::EvaluateSqlBinary {
                    destination: condition,
                    left: operand,
                    right: candidate,
                    operator: InfixOperator::Equal,
                },
                Some(span(&self.identity, source)?),
            )?;
            let body = self.new_label();
            let next = self.new_label();
            self.terminate_current(
                DraftTerminator::Branch {
                    condition,
                    when_true: body,
                    when_false: next,
                },
                Some(span(&self.identity, source)?),
            )?;
            self.current = Some(body);
            self.compile_statements(&arm.statements)?;
            if let Some(label) = self.current.take() {
                fallthrough.push(label);
            }
            self.current = Some(next);
        }
        self.compile_statements(otherwise)?;
        if let Some(label) = self.current.take() {
            fallthrough.push(label);
        }
        if fallthrough.is_empty() {
            return Ok(());
        }
        let join = self.new_label();
        for label in fallthrough {
            self.terminate_at(label, DraftTerminator::Jump(join), None)?;
        }
        self.current = Some(join);
        Ok(())
    }

    fn compile_loop(
        &mut self,
        statements: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let header = self.new_label();
        let exit = self.new_label();
        self.terminate_current(DraftTerminator::Jump(header), None)?;
        self.loops.push(LoopFrame {
            break_target: exit,
            continue_target: header,
            has_break: false,
        });
        self.current = Some(header);
        self.compile_statements(statements)?;
        if let Some(current) = self.current.take() {
            self.terminate_at(current, DraftTerminator::Jump(header), None)?;
        }
        let frame = self.loops.pop().expect("loop frame");
        self.current = frame.has_break.then_some(exit);
        if self.current.is_none() && !statements.is_empty() {
            let _ = source;
        }
        Ok(())
    }

    fn compile_while(
        &mut self,
        condition: &radixdb_sql::Expression,
        statements: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let test = self.new_label();
        let body = self.new_label();
        let exit = self.new_label();
        self.terminate_current(DraftTerminator::Jump(test), None)?;
        self.current = Some(test);
        let condition = self.compile_condition(condition, source)?;
        self.terminate_current(
            DraftTerminator::Branch {
                condition,
                when_true: body,
                when_false: exit,
            },
            Some(span(&self.identity, source)?),
        )?;
        self.loops.push(LoopFrame {
            break_target: exit,
            continue_target: test,
            has_break: true,
        });
        self.current = Some(body);
        self.compile_statements(statements)?;
        if let Some(current) = self.current.take() {
            self.terminate_at(current, DraftTerminator::Jump(test), None)?;
        }
        self.loops.pop();
        self.current = Some(exit);
        Ok(())
    }

    fn compile_for(
        &mut self,
        variable: &Identifier,
        for_source: &ForSourceSyntax,
        statements: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let ForSourceSyntax::Numeric {
            reverse,
            start,
            end,
            step,
        } = for_source
        else {
            let ForSourceSyntax::Query(query) = for_source else {
                unreachable!()
            };
            return self.compile_query_for(variable, query, statements, source);
        };
        let integer = RuntimeType::scalar(
            radixdb_catalog::CatalogDataType::scalar(DataType::Integer).map_err(|_| {
                bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "INTEGER catalog type is unavailable",
                    Some(span(&self.identity, source).expect("valid parser span")),
                )
            })?,
            false,
        );
        let start = self.compile_expression(start, Some(&integer), source)?;
        let end = self.compile_expression(end, Some(&integer), source)?;
        let step = if let Some(step) = step {
            self.compile_expression(step, Some(&integer), source)?
        } else {
            self.compile_integer_constant(1, source)?
        };
        let zero = self.compile_integer_constant(0, source)?;
        let positive = self.boolean_temp("for_step_positive")?;
        self.emit(
            Instruction::IntegerLess {
                destination: positive,
                left: zero,
                right: step,
            },
            Some(span(&self.identity, source)?),
        )?;
        let valid_step = self.new_label();
        let invalid_step = self.new_label();
        self.terminate_current(
            DraftTerminator::Branch {
                condition: positive,
                when_true: valid_step,
                when_false: invalid_step,
            },
            Some(span(&self.identity, source)?),
        )?;
        self.terminate_at(
            invalid_step,
            DraftTerminator::Raise(DiagnosticKind::RuntimeInvalidArgument),
            Some(span(&self.identity, source)?),
        )?;
        self.current = Some(valid_step);
        self.scopes.push(BTreeMap::new());
        let variable_slot = self.declare(variable, integer.clone(), false, source)?;
        self.emit(
            Instruction::Copy {
                destination: variable_slot,
                source: start,
            },
            Some(span(&self.identity, source)?),
        )?;
        let test = self.new_label();
        let body = self.new_label();
        let exit = self.new_label();
        self.terminate_current(DraftTerminator::Jump(test), None)?;
        self.current = Some(test);
        let after_end = self.new_temp(
            "for_after_end",
            RuntimeType::scalar(
                radixdb_catalog::CatalogDataType::scalar(DataType::Boolean).map_err(|_| {
                    bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "BOOLEAN catalog type is unavailable",
                        None,
                    )
                })?,
                false,
            ),
        );
        self.emit(
            Instruction::IntegerLess {
                destination: after_end,
                left: if *reverse { variable_slot } else { end },
                right: if *reverse { end } else { variable_slot },
            },
            Some(span(&self.identity, source)?),
        )?;
        self.terminate_current(
            DraftTerminator::Branch {
                condition: after_end,
                when_true: exit,
                when_false: body,
            },
            Some(span(&self.identity, source)?),
        )?;
        let increment = self.new_label();
        self.loops.push(LoopFrame {
            break_target: exit,
            continue_target: increment,
            has_break: true,
        });
        self.current = Some(body);
        self.compile_statements(statements)?;
        if let Some(current) = self.current.take() {
            self.terminate_at(current, DraftTerminator::Jump(increment), None)?;
        }
        self.current = Some(increment);
        self.emit(
            if *reverse {
                Instruction::IntegerSubtractChecked {
                    destination: variable_slot,
                    left: variable_slot,
                    right: step,
                }
            } else {
                Instruction::IntegerAddChecked {
                    destination: variable_slot,
                    left: variable_slot,
                    right: step,
                }
            },
            Some(span(&self.identity, source)?),
        )?;
        self.terminate_current(DraftTerminator::Jump(test), None)?;
        self.loops.pop();
        self.scopes.pop();
        self.current = Some(exit);
        Ok(())
    }

    fn compile_query_for(
        &mut self,
        variable: &Identifier,
        query: &radixdb_sql::Statement,
        statements: &[ProceduralStatement],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let locals = self.visible_locals();
        let bound = self.resolver.bind_statement(query, &locals)?;
        if !matches!(bound.statement, radixdb_sql::Statement::Select(_))
            || bound.result_columns.is_empty()
        {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "query FOR requires a non-empty SELECT result",
                Some(span(&self.identity, source)?),
            ));
        }
        self.dependencies.extend(bound.dependencies.iter().copied());
        let fields = bound
            .result_columns
            .iter()
            .map(|column| {
                let RuntimeType::Scalar {
                    data_type,
                    nullable,
                } = &column.runtime_type
                else {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "query FOR result column is not scalar",
                        Some(span(&self.identity, source)?),
                    ));
                };
                Ok(RecordField::new(column.name.clone(), *data_type, *nullable))
            })
            .collect::<ProceduralResult<Vec<_>>>()?;
        let record_type = RuntimeType::record(fields)?;
        self.scopes.push(BTreeMap::new());
        self.cursor_scopes.push(BTreeMap::new());
        let row = self.declare(variable, record_type, false, source)?;
        let cursor = CursorId(self.next_cursor);
        self.next_cursor = self.next_cursor.checked_add(1).ok_or_else(|| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "cursor count exceeds u32",
                span(&self.identity, source).ok(),
            )
        })?;
        let parameters = self.materialize_sql_parameters(&bound.parameters, source)?;
        self.emit(
            Instruction::OpenCursor {
                cursor,
                statement: Box::new(bound.statement),
                parameters,
            },
            Some(span(&self.identity, source)?),
        )?;
        let fetch = self.new_label();
        let body = self.new_label();
        let close = self.new_label();
        let exit = self.new_label();
        self.terminate_current(DraftTerminator::Jump(fetch), None)?;
        self.current = Some(fetch);
        let found = self.boolean_temp("query_for_found")?;
        self.emit(
            Instruction::FetchCursor {
                cursor,
                into: vec![row],
                found,
            },
            Some(span(&self.identity, source)?),
        )?;
        self.terminate_current(
            DraftTerminator::Branch {
                condition: found,
                when_true: body,
                when_false: close,
            },
            Some(span(&self.identity, source)?),
        )?;
        self.loops.push(LoopFrame {
            break_target: close,
            continue_target: fetch,
            has_break: true,
        });
        self.current = Some(body);
        self.compile_statements(statements)?;
        if let Some(current) = self.current.take() {
            self.terminate_at(current, DraftTerminator::Jump(fetch), None)?;
        }
        self.loops.pop();
        self.current = Some(close);
        self.emit(
            Instruction::CloseCursor { cursor },
            Some(span(&self.identity, source)?),
        )?;
        self.terminate_current(DraftTerminator::Jump(exit), None)?;
        self.scopes.pop();
        self.cursor_scopes.pop();
        self.current = Some(exit);
        Ok(())
    }

    fn compile_loop_control(
        &mut self,
        kind: LoopControlKind,
        condition: Option<&radixdb_sql::Expression>,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let (target, is_break) = self
            .loops
            .last()
            .map(|frame| match kind {
                LoopControlKind::Exit => (frame.break_target, true),
                LoopControlKind::Continue => (frame.continue_target, false),
            })
            .ok_or_else(|| {
                bind_error(
                    DiagnosticKind::VerifyCapabilityDenied,
                    "EXIT/CONTINUE appears outside a loop",
                    span(&self.identity, source).ok(),
                )
            })?;
        if is_break {
            self.loops.last_mut().expect("loop frame").has_break = true;
        }
        if let Some(condition) = condition {
            let condition = self.compile_condition(condition, source)?;
            let next = self.new_label();
            self.terminate_current(
                DraftTerminator::Branch {
                    condition,
                    when_true: target,
                    when_false: next,
                },
                Some(span(&self.identity, source)?),
            )?;
            self.current = Some(next);
        } else {
            self.terminate_current(
                DraftTerminator::Jump(target),
                Some(span(&self.identity, source)?),
            )?;
            self.current = None;
        }
        Ok(())
    }

    fn compile_return(
        &mut self,
        value: &ReturnSyntax,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        if let Some(required_record) = self.trigger_return_record {
            let valid = match (required_record, value) {
                (_, ReturnSyntax::Value(Expression::NullLiteral(_))) => true,
                (Some(required), ReturnSyntax::Value(Expression::Identifier(identifier))) => {
                    let expected = match required {
                        TriggerReturnRecord::Old => "old",
                        TriggerReturnRecord::New => "new",
                    };
                    normalized_identifier(identifier)? == expected
                }
                _ => false,
            };
            if !valid {
                return Err(bind_error(
                    DiagnosticKind::TriggerInvalidReturn,
                    match required_record {
                        Some(TriggerReturnRecord::Old) => {
                            "BEFORE DELETE row trigger must RETURN OLD or NULL"
                        }
                        Some(TriggerReturnRecord::New) => {
                            "BEFORE INSERT/UPDATE row trigger must RETURN NEW or NULL"
                        }
                        None => "AFTER and statement triggers must RETURN NULL",
                    },
                    Some(span(&self.identity, source)?),
                ));
            }
        }
        let returned = match (self.result_type.clone(), value) {
            (None, ReturnSyntax::Void) => None,
            (Some(expected), ReturnSyntax::Value(value)) => {
                let slot = self.compile_expression(value, Some(&expected), source)?;
                if self.slot(slot)?.runtime_type() != &expected {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "RETURN expression differs from routine result type",
                        Some(span(&self.identity, source)?),
                    ));
                }
                Some(slot)
            }
            (None, ReturnSyntax::Next(values)) if !self.result_columns.is_empty() => {
                if values.len() != self.result_columns.len() {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "RETURN NEXT width differs from routine result contract",
                        Some(span(&self.identity, source)?),
                    ));
                }
                let expected = self.result_columns.clone();
                let values = values
                    .iter()
                    .zip(expected)
                    .map(|(value, expected)| {
                        self.compile_expression(value, Some(&expected), source)
                    })
                    .collect::<ProceduralResult<Vec<_>>>()?;
                self.emit(
                    Instruction::EmitResultRow { values },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(());
            }
            (None, ReturnSyntax::Query(query)) if !self.result_columns.is_empty() => {
                let locals = self.visible_locals();
                let bound = self.resolver.bind_statement(query, &locals)?;
                if !matches!(bound.statement, radixdb_sql::Statement::Select(_))
                    || bound
                        .result_columns
                        .iter()
                        .map(|column| &column.runtime_type)
                        .ne(self.result_columns.iter())
                {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "RETURN QUERY shape differs from routine result contract",
                        Some(span(&self.identity, source)?),
                    ));
                }
                let parameters = self.materialize_sql_parameters(&bound.parameters, source)?;
                self.dependencies.extend(bound.dependencies);
                self.emit(
                    Instruction::EmitResultQuery {
                        statement: Box::new(bound.statement),
                        parameters,
                    },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(());
            }
            _ => {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "RETURN shape differs from routine result contract",
                    Some(span(&self.identity, source)?),
                ));
            }
        };
        self.terminate_current(
            DraftTerminator::Return(returned),
            Some(span(&self.identity, source)?),
        )?;
        self.current = None;
        Ok(())
    }

    fn compile_sql(
        &mut self,
        statement: &radixdb_sql::Statement,
        into: &[Identifier],
        strict: bool,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let locals = self.visible_locals();
        let bound = self.resolver.bind_statement(statement, &locals)?;
        let targets = self.resolve_into(into, &bound, strict, source)?;
        let parameters = self.materialize_sql_parameters(&bound.parameters, source)?;
        self.dependencies.extend(bound.dependencies);
        self.emit(
            Instruction::ExecuteSql {
                statement: Box::new(bound.statement),
                parameters,
                into: targets,
                strict,
            },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_dynamic(
        &mut self,
        execute: &DynamicExecuteSyntax,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        let text = RuntimeType::scalar(
            radixdb_catalog::CatalogDataType::scalar(DataType::Text).map_err(|_| {
                bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "TEXT catalog type is unavailable",
                    None,
                )
            })?,
            false,
        );
        let sql_source = self.compile_expression(&execute.source, Some(&text), source)?;
        let mut parameters = Vec::with_capacity(execute.using.len());
        for argument in &execute.using {
            parameters.push(self.compile_expression(argument, None, source)?);
        }
        let targets = execute
            .into
            .iter()
            .map(|target| {
                self.resolve_assignable(target, source)
                    .map(|local| local.slot)
            })
            .collect::<ProceduralResult<Vec<_>>>()?;
        if !execute.strict
            && targets.iter().any(|slot| {
                matches!(
                    self.slot(*slot).map(SlotDefinition::runtime_type),
                    Ok(RuntimeType::Scalar {
                        nullable: false,
                        ..
                    })
                )
            })
        {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "non-STRICT dynamic INTO requires nullable targets",
                Some(span(&self.identity, source)?),
            ));
        }
        self.emit(
            Instruction::ExecuteDynamicSql {
                source: sql_source,
                parameters,
                into: targets,
                strict: execute.strict,
            },
            Some(span(&self.identity, source)?),
        )
    }

    fn compile_raise(
        &mut self,
        kind: Option<&Identifier>,
        arguments: &[radixdb_sql::Expression],
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        for argument in arguments {
            self.compile_expression(argument, None, source)?;
        }
        let Some(kind) = kind else {
            if self.handler_depth == 0 {
                return Err(bind_error(
                    DiagnosticKind::ParseUnsupportedSyntax,
                    "RAISE rethrow requires an active exception handler",
                    Some(span(&self.identity, source)?),
                ));
            }
            self.terminate_current(
                DraftTerminator::Rethrow,
                Some(span(&self.identity, source)?),
            )?;
            self.current = None;
            return Ok(());
        };
        let kind = exception_kind(kind, &self.identity, source)?;
        self.terminate_current(
            DraftTerminator::Raise(kind),
            Some(span(&self.identity, source)?),
        )?;
        self.current = None;
        Ok(())
    }

    fn compile_condition(
        &mut self,
        expression: &radixdb_sql::Expression,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        let boolean = RuntimeType::scalar(
            radixdb_catalog::CatalogDataType::scalar(DataType::Boolean).map_err(|_| {
                bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "BOOLEAN catalog type is unavailable",
                    None,
                )
            })?,
            true,
        );
        self.compile_expression(expression, Some(&boolean), source)
    }

    fn compile_expression(
        &mut self,
        expression: &radixdb_sql::Expression,
        expected: Option<&RuntimeType>,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        if matches!(expression, Expression::NullLiteral(_))
            && matches!(expected, Some(RuntimeType::Record { nullable: true, .. }))
        {
            let destination = self.new_temp(
                "null_record",
                expected.expect("nullable record expectation").clone(),
            );
            self.emit(
                Instruction::InitializeNull { destination },
                Some(span(&self.identity, source)?),
            )?;
            return Ok(destination);
        }
        if let Some(value) = self.compile_intrinsic_expression(expression, expected, source)? {
            return Ok(value);
        }
        let locals = self.visible_locals();
        let bound = self
            .resolver
            .bind_expression(expression, &locals, expected)?;
        self.emit_bound_expression(bound, expected, source)
    }

    fn emit_bound_expression(
        &mut self,
        bound: super::BoundExpression,
        expected: Option<&RuntimeType>,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        if let Some(expected) = expected {
            if &bound.result_type != expected {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "bound expression differs from expected procedural type",
                    Some(span(&self.identity, source)?),
                ));
            }
        }
        if !matches!(bound.result_type, RuntimeType::Scalar { .. }) {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "procedural expression must produce one scalar value",
                Some(span(&self.identity, source)?),
            ));
        }
        self.dependencies.extend(bound.dependencies);
        let destination = self.new_temp("expression", bound.result_type);
        self.emit(
            Instruction::EvaluateExpression {
                expression: Box::new(bound.expression),
                parameters: bound.parameters,
                destination,
            },
            Some(span(&self.identity, source)?),
        )?;
        Ok(destination)
    }

    fn compile_intrinsic_expression(
        &mut self,
        expression: &Expression,
        expected: Option<&RuntimeType>,
        source: &SourceRange,
    ) -> ProceduralResult<Option<SlotId>> {
        if let Expression::FunctionCall(call) = expression {
            if call.function.eq_ignore_ascii_case("SQL_IDENTIFIER") {
                if call.arguments.len() != 1
                    || call.is_distinct
                    || !call.order_by.is_empty()
                    || call.filter.is_some()
                {
                    return Err(bind_error(
                        DiagnosticKind::RuntimeInvalidArgument,
                        "SQL_IDENTIFIER requires exactly one TEXT argument",
                        Some(span(&self.identity, source)?),
                    ));
                }
                let input = self.compile_expression(&call.arguments[0], None, source)?;
                if !matches!(
                    self.slot(input)?.runtime_type(),
                    RuntimeType::Scalar { data_type, .. }
                        if data_type.logical_type() == DataType::Text
                ) {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "SQL_IDENTIFIER argument must be TEXT",
                        Some(span(&self.identity, source)?),
                    ));
                }
                let result_type = admit_result_type(
                    expected,
                    RuntimeType::SqlIdentifier,
                    source,
                    &self.identity,
                )?;
                let destination = self.new_temp("sql_identifier", result_type);
                self.emit(
                    Instruction::QuoteSqlIdentifier {
                        destination,
                        source: input,
                    },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(Some(destination));
            }
        }
        if let Expression::Identifier(identifier) = expression {
            let name = normalized_identifier(identifier)?;
            if let Some(local) = self
                .scopes
                .iter()
                .rev()
                .find_map(|scope| scope.get(&name))
                .filter(|local| matches!(local.runtime_type, RuntimeType::Record { .. }))
            {
                admit_result_type(expected, local.runtime_type.clone(), source, &self.identity)?;
                return Ok(Some(local.slot));
            }
        }
        if let Expression::QualifiedIdentifier(path) = expression {
            if path.intermediate.is_none() {
                let owner = normalized_identifier(&path.qualifier)?;
                if let Some(local) = self
                    .scopes
                    .iter()
                    .rev()
                    .find_map(|scope| scope.get(&owner))
                    .filter(|local| matches!(local.runtime_type, RuntimeType::Record { .. }))
                    .cloned()
                {
                    let (field, field_type) =
                        record_field(&local.runtime_type, &path.name, source, &self.identity)?;
                    let result_type =
                        admit_result_type(expected, field_type, source, &self.identity)?;
                    let destination = self.new_temp("record_field", result_type);
                    self.emit(
                        Instruction::ReadRecordField {
                            destination,
                            record: local.slot,
                            field,
                        },
                        Some(span(&self.identity, source)?),
                    )?;
                    return Ok(Some(destination));
                }
            }
        }
        if let Some((collection_name, index)) = collection_index_expression(expression) {
            let collection = self.resolve_local(collection_name)?.clone();
            let RuntimeType::Collection { element_type, .. } = collection.runtime_type else {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "indexed expression source is not a collection",
                    Some(span(&self.identity, source)?),
                ));
            };
            let result_type = admit_result_type(
                expected,
                RuntimeType::scalar(element_type, true),
                source,
                &self.identity,
            )?;
            let index = self.compile_integer_expression(index, source)?;
            let destination = self.new_temp("collection_element", result_type);
            self.emit(
                Instruction::CollectionGet {
                    destination,
                    collection: collection.slot,
                    one_based_index: index,
                },
                Some(span(&self.identity, source)?),
            )?;
            return Ok(Some(destination));
        }
        if let Some(collection_name) = collection_count_expression(expression) {
            let collection = self.resolve_local(collection_name)?.clone();
            if !matches!(collection.runtime_type, RuntimeType::Collection { .. }) {
                return Err(bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "COUNT source is not a collection",
                    Some(span(&self.identity, source)?),
                ));
            }
            let result_type = admit_result_type(
                expected,
                scalar_runtime_type(DataType::Integer, false)?,
                source,
                &self.identity,
            )?;
            let destination = self.new_temp("collection_count", result_type);
            self.emit(
                Instruction::CollectionCount {
                    destination,
                    collection: collection.slot,
                },
                Some(span(&self.identity, source)?),
            )?;
            return Ok(Some(destination));
        }
        if let Some((owner, attribute)) = status_expression(expression) {
            let (instruction, destination) = if owner.eq_ignore_ascii_case("sql") {
                let (attribute, data_type) = match attribute {
                    "rowcount" => (SqlStatusAttribute::RowCount, DataType::Integer),
                    "found" => (SqlStatusAttribute::Found, DataType::Boolean),
                    "notfound" => (SqlStatusAttribute::NotFound, DataType::Boolean),
                    _ => return Ok(None),
                };
                let result_type = admit_result_type(
                    expected,
                    scalar_runtime_type(data_type, false)?,
                    source,
                    &self.identity,
                )?;
                let destination = self.new_temp("sql_status", result_type);
                (
                    Instruction::ReadSqlStatus {
                        destination,
                        attribute,
                    },
                    destination,
                )
            } else {
                let Some(cursor) = self.resolve_cursor_name(owner) else {
                    return Ok(None);
                };
                let (attribute, data_type, nullable) = match attribute {
                    "isopen" => (CursorStatusAttribute::IsOpen, DataType::Boolean, false),
                    "found" => (CursorStatusAttribute::Found, DataType::Boolean, true),
                    "notfound" => (CursorStatusAttribute::NotFound, DataType::Boolean, true),
                    "rowcount" => (CursorStatusAttribute::RowCount, DataType::Integer, false),
                    _ => return Ok(None),
                };
                let result_type = admit_result_type(
                    expected,
                    scalar_runtime_type(data_type, nullable)?,
                    source,
                    &self.identity,
                )?;
                let destination = self.new_temp("cursor_status", result_type);
                (
                    Instruction::ReadCursorStatus {
                        destination,
                        cursor,
                        attribute,
                    },
                    destination,
                )
            };
            self.emit(instruction, Some(span(&self.identity, source)?))?;
            return Ok(Some(destination));
        }
        if let Expression::Infix(infix) = expression {
            if infix.op_type == InfixOperator::Concat
                && (self.contains_sql_identifier(&infix.left)
                    || self.contains_sql_identifier(&infix.right))
            {
                let left = self.compile_expression(&infix.left, None, source)?;
                let right = self.compile_expression(&infix.right, None, source)?;
                let operand_nullable = |runtime_type: &RuntimeType| match runtime_type {
                    RuntimeType::SqlIdentifier => Ok(false),
                    RuntimeType::Scalar {
                        data_type,
                        nullable,
                    } if data_type.logical_type() == DataType::Text => Ok(*nullable),
                    _ => Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "SQL identifier fragments can only be concatenated with TEXT",
                        span(&self.identity, source).ok(),
                    )),
                };
                let nullable = operand_nullable(self.slot(left)?.runtime_type())?
                    || operand_nullable(self.slot(right)?.runtime_type())?;
                let result_type = admit_result_type(
                    expected,
                    scalar_runtime_type(DataType::Text, nullable)?,
                    source,
                    &self.identity,
                )?;
                let destination = self.new_temp("dynamic_sql_text", result_type);
                self.emit(
                    Instruction::ConcatenateSqlText {
                        destination,
                        left,
                        right,
                    },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(Some(destination));
            }
            if self.contains_intrinsic(&infix.left) || self.contains_intrinsic(&infix.right) {
                let left = self.compile_expression(&infix.left, None, source)?;
                let right = self.compile_expression(&infix.right, None, source)?;
                let left_type = self.slot(left)?.runtime_type().clone();
                let right_type = self.slot(right)?.runtime_type().clone();
                let result_type = self.resolver.bind_binary_operator(
                    infix.op_type,
                    &left_type,
                    &right_type,
                    expected,
                )?;
                let result_type = admit_result_type(expected, result_type, source, &self.identity)?;
                let destination = self.new_temp("sql_binary", result_type);
                self.emit(
                    Instruction::EvaluateSqlBinary {
                        destination,
                        left,
                        right,
                        operator: infix.op_type,
                    },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(Some(destination));
            }
        }
        if let Expression::Prefix(prefix) = expression {
            if prefix.op_type() == radixdb_sql::PrefixOperator::Not
                && self.contains_intrinsic(&prefix.right)
            {
                let value = self.compile_expression(&prefix.right, None, source)?;
                let RuntimeType::Scalar {
                    data_type,
                    nullable,
                } = self.slot(value)?.runtime_type()
                else {
                    unreachable!("compile_expression returns scalar slots")
                };
                if data_type.logical_type() != DataType::Boolean {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "NOT operand is not BOOLEAN",
                        Some(span(&self.identity, source)?),
                    ));
                }
                let result_type = admit_result_type(
                    expected,
                    scalar_runtime_type(DataType::Boolean, *nullable)?,
                    source,
                    &self.identity,
                )?;
                let destination = self.new_temp("sql_not", result_type);
                self.emit(
                    Instruction::BooleanNot {
                        destination,
                        source: value,
                    },
                    Some(span(&self.identity, source)?),
                )?;
                return Ok(Some(destination));
            }
        }
        Ok(None)
    }

    fn contains_intrinsic(&self, expression: &Expression) -> bool {
        collection_index_expression(expression).is_some()
            || collection_count_expression(expression).is_some()
            || status_expression(expression).is_some()
            || match expression {
                Expression::Identifier(identifier) => {
                    let name = identifier.value_lower();
                    self.scopes.iter().rev().any(|scope| {
                        scope.get(name).is_some_and(|local| {
                            matches!(local.runtime_type, RuntimeType::Record { .. })
                        })
                    })
                }
                Expression::QualifiedIdentifier(path) if path.intermediate.is_none() => {
                    let owner = path.qualifier.value_lower();
                    self.scopes.iter().rev().any(|scope| {
                        scope.get(owner).is_some_and(|local| {
                            matches!(local.runtime_type, RuntimeType::Record { .. })
                        })
                    })
                }
                Expression::Prefix(prefix) => self.contains_intrinsic(&prefix.right),
                Expression::Infix(infix) => {
                    self.contains_intrinsic(&infix.left) || self.contains_intrinsic(&infix.right)
                }
                Expression::FunctionCall(call) => {
                    call.function.eq_ignore_ascii_case("SQL_IDENTIFIER")
                        || call
                            .arguments
                            .iter()
                            .any(|argument| self.contains_intrinsic(argument))
                }
                _ => false,
            }
    }

    fn contains_sql_identifier(&self, expression: &Expression) -> bool {
        match expression {
            Expression::FunctionCall(call) => {
                call.function.eq_ignore_ascii_case("SQL_IDENTIFIER")
                    || call
                        .arguments
                        .iter()
                        .any(|argument| self.contains_sql_identifier(argument))
            }
            Expression::Prefix(prefix) => self.contains_sql_identifier(&prefix.right),
            Expression::Infix(infix) => {
                self.contains_sql_identifier(&infix.left)
                    || self.contains_sql_identifier(&infix.right)
            }
            _ => false,
        }
    }

    fn compile_integer_expression(
        &mut self,
        expression: &Expression,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        self.compile_expression(
            expression,
            Some(&scalar_runtime_type(DataType::Integer, true)?),
            source,
        )
    }

    fn compile_integer_constant(
        &mut self,
        value: i64,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        let runtime_type = RuntimeType::scalar(
            radixdb_catalog::CatalogDataType::scalar(DataType::Integer).map_err(|_| {
                bind_error(
                    DiagnosticKind::BindTypeMismatch,
                    "INTEGER catalog type is unavailable",
                    None,
                )
            })?,
            false,
        );
        let slot = self.new_temp("integer_constant", runtime_type);
        self.emit(
            Instruction::LoadConstant {
                destination: slot,
                value: crate::RuntimeValue::scalar(radixdb_core::Value::Integer(value)),
            },
            Some(span(&self.identity, source)?),
        )?;
        Ok(slot)
    }

    fn resolve_into(
        &self,
        into: &[Identifier],
        bound: &BoundSqlStatement,
        strict: bool,
        source: &SourceRange,
    ) -> ProceduralResult<Vec<SlotId>> {
        if into.len() != bound.result_columns.len() {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "SQL result width differs from INTO target count",
                Some(span(&self.identity, source)?),
            ));
        }
        into.iter()
            .zip(&bound.result_columns)
            .map(|(target, result)| {
                let local = self.resolve_assignable(target, source)?;
                if local.runtime_type != result.runtime_type {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "SQL result type differs from INTO target",
                        Some(span(&self.identity, source)?),
                    ));
                }
                if !strict
                    && matches!(
                        local.runtime_type,
                        RuntimeType::Scalar {
                            nullable: false,
                            ..
                        }
                    )
                {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "non-STRICT SQL INTO requires nullable targets",
                        Some(span(&self.identity, source)?),
                    ));
                }
                Ok(local.slot)
            })
            .collect()
    }

    fn resolve_cursor_targets(
        &self,
        into: &[Identifier],
        result_columns: &[super::BoundResultColumn],
        source: &SourceRange,
    ) -> ProceduralResult<Vec<SlotId>> {
        if into.len() != result_columns.len() {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "cursor row width differs from FETCH target count",
                Some(span(&self.identity, source)?),
            ));
        }
        into.iter()
            .zip(result_columns)
            .map(|(target, result)| {
                let local = self.resolve_assignable(target, source)?;
                if local.runtime_type != result.runtime_type {
                    return Err(bind_error(
                        DiagnosticKind::BindTypeMismatch,
                        "cursor column type differs from FETCH target",
                        Some(span(&self.identity, source)?),
                    ));
                }
                Ok(local.slot)
            })
            .collect()
    }

    fn resolve_cursor(&self, name: &Identifier) -> ProceduralResult<&CursorBinding> {
        let name = normalized_identifier(name)?;
        self.cursor_scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(&name))
            .ok_or_else(|| {
                bind_error(
                    DiagnosticKind::BindUnknownLocal,
                    format!("unknown cursor {name}"),
                    None,
                )
            })
    }

    fn resolve_cursor_name(&self, name: &str) -> Option<CursorId> {
        self.cursor_scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .map(|cursor| cursor.cursor)
    }

    fn boolean_temp(&mut self, name: &str) -> ProceduralResult<SlotId> {
        self.boolean_temp_with_nullability(name, false)
    }

    fn boolean_temp_with_nullability(
        &mut self,
        name: &str,
        nullable: bool,
    ) -> ProceduralResult<SlotId> {
        Ok(self.new_temp(name, scalar_runtime_type(DataType::Boolean, nullable)?))
    }

    fn declare(
        &mut self,
        name: &Identifier,
        runtime_type: RuntimeType,
        constant: bool,
        source: &SourceRange,
    ) -> ProceduralResult<SlotId> {
        let name = normalized_identifier(name)?;
        let scope = self.scopes.last_mut().expect("root lexical scope");
        if scope.contains_key(&name) {
            return Err(bind_error(
                DiagnosticKind::BindAmbiguousRoutine,
                "duplicate name in one lexical scope",
                Some(span(&self.identity, source)?),
            ));
        }
        let slot = SlotId(u32::try_from(self.slots.len()).map_err(|_| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "procedural slot count exceeds u32",
                span(&self.identity, source).ok(),
            )
        })?);
        self.slots
            .push(SlotDefinition::new(name.clone(), runtime_type.clone()));
        scope.insert(
            name.clone(),
            LocalBinding {
                name,
                slot,
                runtime_type,
                constant,
            },
        );
        Ok(slot)
    }

    fn new_temp(&mut self, prefix: &str, runtime_type: RuntimeType) -> SlotId {
        let slot = SlotId(self.slots.len() as u32);
        self.slots.push(SlotDefinition::new(
            format!("${prefix}_{}", slot.0),
            runtime_type,
        ));
        slot
    }

    fn resolve_local(&self, name: &str) -> ProceduralResult<&LocalBinding> {
        self.scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .ok_or_else(|| {
                bind_error(
                    DiagnosticKind::BindUnknownLocal,
                    format!("unknown procedural local {name}"),
                    None,
                )
            })
    }

    fn resolve_assignable(
        &self,
        name: &Identifier,
        source: &SourceRange,
    ) -> ProceduralResult<&LocalBinding> {
        let name = normalized_identifier(name)?;
        let local = self.resolve_local(&name)?;
        if local.constant {
            return Err(bind_error(
                DiagnosticKind::VerifyCapabilityDenied,
                "assignment to CONSTANT is forbidden",
                Some(span(&self.identity, source)?),
            ));
        }
        Ok(local)
    }

    fn resolve_assignable_name(
        &self,
        name: &ObjectName,
        source: &SourceRange,
    ) -> ProceduralResult<&LocalBinding> {
        if name.components.len() != 1 {
            return Err(bind_error(
                DiagnosticKind::ParseUnsupportedSyntax,
                "record-field assignment requires typed record mutation IR",
                Some(span(&self.identity, source)?),
            ));
        }
        self.resolve_assignable(&name.components[0], source)
    }

    fn visible_locals(&self) -> Vec<LocalBinding> {
        let mut visible = BTreeMap::new();
        for scope in self.scopes.iter().rev() {
            for (name, binding) in scope {
                visible
                    .entry(name.clone())
                    .or_insert_with(|| binding.clone());
            }
        }
        visible.into_values().collect()
    }

    fn materialize_sql_parameters(
        &mut self,
        parameters: &[BoundSqlParameter],
        source: &SourceRange,
    ) -> ProceduralResult<Vec<SlotId>> {
        parameters
            .iter()
            .map(|parameter| match parameter {
                BoundSqlParameter::Scalar(slot) => Ok(*slot),
                BoundSqlParameter::RecordField {
                    record,
                    field,
                    runtime_type,
                } => {
                    let destination = self.new_temp("sql_record_field", runtime_type.clone());
                    self.emit(
                        Instruction::ReadRecordField {
                            destination,
                            record: *record,
                            field: *field,
                        },
                        Some(span(&self.identity, source)?),
                    )?;
                    Ok(destination)
                }
            })
            .collect()
    }

    fn slot(&self, slot: SlotId) -> ProceduralResult<&SlotDefinition> {
        self.slots.get(slot.0 as usize).ok_or_else(|| {
            bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler produced an invalid slot",
                None,
            )
        })
    }

    fn require_same_type(
        &self,
        destination: SlotId,
        source_slot: SlotId,
        source: &SourceRange,
    ) -> ProceduralResult<()> {
        if self.slot(destination)?.runtime_type() != self.slot(source_slot)?.runtime_type() {
            return Err(bind_error(
                DiagnosticKind::BindTypeMismatch,
                "assignment source and destination types differ",
                Some(span(&self.identity, source)?),
            ));
        }
        Ok(())
    }

    fn new_label(&mut self) -> Label {
        let label = Label(self.blocks.len());
        self.blocks.push(DraftBlock::default());
        label
    }

    fn emit(
        &mut self,
        instruction: Instruction,
        source_span: Option<SourceSpan>,
    ) -> ProceduralResult<()> {
        let current = self.current.ok_or_else(|| {
            bind_error(
                DiagnosticKind::VerifyCapabilityDenied,
                "cannot emit into a terminated control-flow path",
                source_span.clone(),
            )
        })?;
        self.blocks[current.0]
            .instructions
            .push(DraftInstruction::Concrete(SpannedInstruction::new(
                instruction,
                source_span,
            )));
        Ok(())
    }

    fn emit_exception_region(
        &mut self,
        routes: Vec<DraftExceptionRoute>,
        source_span: Option<SourceSpan>,
    ) -> ProceduralResult<()> {
        let current = self.current.ok_or_else(|| {
            bind_error(
                DiagnosticKind::VerifyCapabilityDenied,
                "cannot enter exception region on a terminated path",
                source_span.clone(),
            )
        })?;
        self.blocks[current.0]
            .instructions
            .push(DraftInstruction::EnterExceptionRegion {
                routes,
                span: source_span,
            });
        Ok(())
    }

    fn terminate_current(
        &mut self,
        terminator: DraftTerminator,
        source_span: Option<SourceSpan>,
    ) -> ProceduralResult<()> {
        let current = self.current.ok_or_else(|| {
            bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler path is already terminated",
                source_span.clone(),
            )
        })?;
        self.terminate_at(current, terminator, source_span)
    }

    fn terminate_at(
        &mut self,
        label: Label,
        terminator: DraftTerminator,
        source_span: Option<SourceSpan>,
    ) -> ProceduralResult<()> {
        let block = self.blocks.get_mut(label.0).ok_or_else(|| {
            bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler produced an invalid block label",
                source_span.clone(),
            )
        })?;
        if block.terminator.is_some() {
            return Err(bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler attempted to terminate one block twice",
                source_span,
            ));
        }
        block.terminator = Some((terminator, source_span));
        Ok(())
    }

    fn finish(self) -> ProceduralResult<CompiledRoutine> {
        let reachable = reachable_labels(&self.blocks, Label(0))?;
        let mapping = reachable
            .iter()
            .enumerate()
            .map(|(index, label)| (*label, BlockId(index as u32)))
            .collect::<BTreeMap<_, _>>();
        let blocks = reachable
            .iter()
            .map(|label| {
                let draft = &self.blocks[label.0];
                let (terminator, source_span) = draft.terminator.as_ref().ok_or_else(|| {
                    bind_error(
                        DiagnosticKind::RuntimeInvalidIr,
                        "reachable compiler block has no terminator",
                        None,
                    )
                })?;
                let terminator = match terminator {
                    DraftTerminator::Jump(target) => Terminator::Jump(mapping[target]),
                    DraftTerminator::Branch {
                        condition,
                        when_true,
                        when_false,
                    } => Terminator::Branch {
                        condition: *condition,
                        when_true: mapping[when_true],
                        when_false: mapping[when_false],
                    },
                    DraftTerminator::Return(value) => Terminator::Return(*value),
                    DraftTerminator::Raise(kind) => Terminator::Raise(*kind),
                    DraftTerminator::Rethrow => Terminator::Rethrow,
                };
                let instructions = draft
                    .instructions
                    .iter()
                    .map(|instruction| match instruction {
                        DraftInstruction::Concrete(instruction) => instruction.clone(),
                        DraftInstruction::EnterExceptionRegion { routes, span } => {
                            SpannedInstruction::new(
                                Instruction::EnterExceptionRegion {
                                    routes: routes
                                        .iter()
                                        .map(|route| ExceptionRoute {
                                            kinds: route.kinds.clone(),
                                            handler: mapping[&route.handler],
                                            error_slot: route.error_slot,
                                        })
                                        .collect(),
                                },
                                span.clone(),
                            )
                        }
                    })
                    .collect();
                Ok(BasicBlock::new(
                    instructions,
                    SpannedTerminator::new(terminator, source_span.clone()),
                ))
            })
            .collect::<ProceduralResult<Vec<_>>>()?;
        let program = Program::new(
            ProgramIdentity::new(
                self.identity.object_id,
                self.identity.definition_revision,
                self.identity.display_name,
            ),
            self.slots,
            self.parameter_slots,
            self.result_type,
            blocks,
            BlockId(0),
        )
        .with_output_slots(self.output_slots)
        .with_result_columns(self.result_columns);
        Ok(CompiledRoutine {
            program,
            dependencies: self.dependencies.into_iter().collect(),
        })
    }
}

fn with_nullability(
    runtime_type: RuntimeType,
    nullable: bool,
    source: &SourceRange,
    identity: &CompileIdentity,
) -> ProceduralResult<RuntimeType> {
    match runtime_type {
        RuntimeType::Scalar { data_type, .. } => Ok(RuntimeType::scalar(data_type, nullable)),
        _ => Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "variable declaration type must be scalar or %ROWTYPE",
            Some(span(identity, source)?),
        )),
    }
}

fn declaration_type(
    runtime_type: RuntimeType,
    nullable: bool,
    syntax: &radixdb_sql::ProceduralType,
    source: &SourceRange,
    identity: &CompileIdentity,
) -> ProceduralResult<RuntimeType> {
    match (syntax, runtime_type) {
        (radixdb_sql::ProceduralType::Scalar(_), runtime_type) => {
            with_nullability(runtime_type, nullable, source, identity)
        }
        (radixdb_sql::ProceduralType::RowType(_), runtime_type @ RuntimeType::Record { .. })
            if nullable =>
        {
            Ok(runtime_type)
        }
        (radixdb_sql::ProceduralType::RowType(_), RuntimeType::Record { .. }) => Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "%ROWTYPE declarations cannot be declared NOT NULL",
            Some(span(identity, source)?),
        )),
        (radixdb_sql::ProceduralType::RowType(_), _) => Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "%ROWTYPE did not resolve to an ordered record descriptor",
            Some(span(identity, source)?),
        )),
    }
}

fn record_field(
    runtime_type: &RuntimeType,
    field: &Identifier,
    source: &SourceRange,
    identity: &CompileIdentity,
) -> ProceduralResult<(u32, RuntimeType)> {
    let RuntimeType::Record { fields, .. } = runtime_type else {
        return Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "field access target is not a record",
            Some(span(identity, source)?),
        ));
    };
    let name = normalized_identifier(field)?;
    let (ordinal, field) = fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name().normalized().as_str() == name)
        .ok_or_else(|| {
            bind_error(
                DiagnosticKind::BindUnknownLocal,
                format!("unknown record field {name}"),
                span(identity, source).ok(),
            )
        })?;
    let ordinal = u32::try_from(ordinal).map_err(|_| {
        bind_error(
            DiagnosticKind::ParseLimitExceeded,
            "record field ordinal exceeds u32",
            span(identity, source).ok(),
        )
    })?;
    Ok((
        ordinal,
        RuntimeType::scalar(field.data_type(), field.nullable()),
    ))
}

fn scalar_runtime_type(data_type: DataType, nullable: bool) -> ProceduralResult<RuntimeType> {
    Ok(RuntimeType::scalar(
        radixdb_catalog::CatalogDataType::scalar(data_type).map_err(|_| {
            bind_error(
                DiagnosticKind::BindTypeMismatch,
                "catalog scalar type is unavailable",
                None,
            )
        })?,
        nullable,
    ))
}

fn admit_result_type(
    expected: Option<&RuntimeType>,
    actual: RuntimeType,
    source: &SourceRange,
    identity: &CompileIdentity,
) -> ProceduralResult<RuntimeType> {
    let compatible = match (expected, &actual) {
        (None, _) => true,
        (
            Some(RuntimeType::Scalar {
                data_type: expected_type,
                nullable: expected_nullable,
            }),
            RuntimeType::Scalar {
                data_type: actual_type,
                nullable: actual_nullable,
            },
        ) => expected_type == actual_type && (*expected_nullable || !actual_nullable),
        (Some(expected), actual) => expected == actual,
    };
    if compatible {
        Ok(expected.cloned().unwrap_or(actual))
    } else {
        Err(bind_error(
            DiagnosticKind::BindTypeMismatch,
            "procedural intrinsic differs from expected expression type",
            Some(span(identity, source)?),
        ))
    }
}

fn collection_index_expression(expression: &Expression) -> Option<(&str, &Expression)> {
    let Expression::Infix(index) = expression else {
        return None;
    };
    if index.op_type != InfixOperator::Index {
        return None;
    }
    let Expression::Identifier(collection) = index.left.as_ref() else {
        return None;
    };
    Some((collection.value_lower(), index.right.as_ref()))
}

fn collection_count_expression(expression: &Expression) -> Option<&str> {
    let Expression::QualifiedIdentifier(attribute) = expression else {
        return None;
    };
    (attribute.intermediate.is_none() && attribute.name.value_lower() == "count")
        .then(|| attribute.qualifier.value_lower())
}

fn status_expression(expression: &Expression) -> Option<(&str, &str)> {
    let Expression::Infix(attribute) = expression else {
        return None;
    };
    if attribute.op_type != InfixOperator::Modulo {
        return None;
    }
    let (Expression::Identifier(owner), Expression::Identifier(name)) =
        (attribute.left.as_ref(), attribute.right.as_ref())
    else {
        return None;
    };
    Some((owner.value_lower(), name.value_lower()))
}

fn normalized_identifier(identifier: &Identifier) -> ProceduralResult<String> {
    CatalogName::new(identifier.value())
        .map(|name| name.normalized().as_str().to_owned())
        .map_err(|_| {
            bind_error(
                DiagnosticKind::BindUnknownObject,
                "identifier cannot be represented as a catalog name",
                None,
            )
        })
}

fn expression_local(expression: &radixdb_sql::Expression) -> Option<&str> {
    match expression {
        radixdb_sql::Expression::Identifier(identifier) => Some(identifier.value()),
        _ => None,
    }
}

fn exception_record_type() -> ProceduralResult<RuntimeType> {
    let text = radixdb_catalog::CatalogDataType::scalar(DataType::Text).map_err(|_| {
        bind_error(
            DiagnosticKind::BindTypeMismatch,
            "TEXT catalog type is unavailable",
            None,
        )
    })?;
    let boolean = radixdb_catalog::CatalogDataType::scalar(DataType::Boolean).map_err(|_| {
        bind_error(
            DiagnosticKind::BindTypeMismatch,
            "BOOLEAN catalog type is unavailable",
            None,
        )
    })?;
    RuntimeType::record(vec![
        RecordField::new(CatalogName::new("kind").unwrap(), text, false),
        RecordField::new(CatalogName::new("category").unwrap(), text, false),
        RecordField::new(CatalogName::new("message").unwrap(), text, false),
        RecordField::new(CatalogName::new("retryable").unwrap(), boolean, false),
    ])
}

fn exception_kind(
    identifier: &Identifier,
    identity: &CompileIdentity,
    source: &SourceRange,
) -> ProceduralResult<DiagnosticKind> {
    match normalized_identifier(identifier)?.as_str() {
        "no_data_found" => Ok(DiagnosticKind::CardinalityNoDataFound),
        "too_many_rows" => Ok(DiagnosticKind::CardinalityTooManyRows),
        "invalid_argument" => Ok(DiagnosticKind::RuntimeInvalidArgument),
        "conflict" => Ok(DiagnosticKind::RuntimeConflict),
        "not_found" => Ok(DiagnosticKind::RuntimeNotFound),
        "invalid_state" => Ok(DiagnosticKind::RuntimeInvalidState),
        "unique_violation" => Ok(DiagnosticKind::RuntimeUniqueViolation),
        "raise_exception" => Ok(DiagnosticKind::RuntimeInvalidIr),
        _ => Err(bind_error(
            DiagnosticKind::BindUnknownObject,
            "unknown exception kind",
            Some(span(identity, source)?),
        )),
    }
}

#[cfg(test)]
mod tests;
