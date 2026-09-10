// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct SourceRange {
    pub start: Position,
    pub end: Position,
}

impl SourceRange {
    pub const fn new(start: Position, end: Position) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectName {
    pub components: Vec<Identifier>,
}

impl ObjectName {
    pub fn new(components: Vec<Identifier>) -> Self {
        Self { components }
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, component) in self.components.iter().enumerate() {
            if index > 0 {
                formatter.write_str(".")?;
            }
            write!(formatter, "{component}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProceduralType {
    Scalar(SmartString),
    RowType(ObjectName),
}

impl fmt::Display for ProceduralType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(name) => formatter.write_str(name),
            Self::RowType(table) => write!(formatter, "{table}%ROWTYPE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineArgumentMode {
    In,
    Out,
    InOut,
}

impl fmt::Display for RoutineArgumentMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::In => "IN",
            Self::Out => "OUT",
            Self::InOut => "INOUT",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutineArgumentSyntax {
    pub token: Token,
    pub mode: RoutineArgumentMode,
    pub name: Identifier,
    pub data_type: ProceduralType,
    pub nullable: bool,
    pub default: Option<Expression>,
    pub span: SourceRange,
}

impl fmt::Display for RoutineArgumentSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mode != RoutineArgumentMode::In {
            write!(formatter, "{} ", self.mode)?;
        }
        write!(formatter, "{} {}", self.name, self.data_type)?;
        if !self.nullable {
            formatter.write_str(" NOT NULL")?;
        }
        if let Some(default) = &self.default {
            write!(formatter, " DEFAULT {default}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultColumnSyntax {
    pub name: Identifier,
    pub data_type: ProceduralType,
    pub nullable: bool,
}

impl fmt::Display for ResultColumnSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.name, self.data_type)?;
        if !self.nullable {
            formatter.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RoutineReturnSyntax {
    Scalar {
        data_type: ProceduralType,
        nullable: bool,
    },
    Table(Vec<ResultColumnSyntax>),
    Trigger,
}

impl fmt::Display for RoutineReturnSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar {
                data_type,
                nullable,
            } => {
                write!(formatter, "{data_type}")?;
                if !nullable {
                    formatter.write_str(" NOT NULL")?;
                }
                Ok(())
            }
            Self::Table(columns) => {
                formatter.write_str("TABLE (")?;
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{column}")?;
                }
                formatter.write_str(")")
            }
            Self::Trigger => formatter.write_str("TRIGGER"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineVolatilitySyntax {
    Immutable,
    Stable,
    Volatile,
}

impl fmt::Display for RoutineVolatilitySyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Immutable => "IMMUTABLE",
            Self::Stable => "STABLE",
            Self::Volatile => "VOLATILE",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineSecuritySyntax {
    Invoker,
    Definer,
}

impl fmt::Display for RoutineSecuritySyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Invoker => "SECURITY INVOKER",
            Self::Definer => "SECURITY DEFINER",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineKindSyntax {
    Function,
    Procedure,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NativeFunctionBindingSyntax {
    pub extension: ObjectName,
    pub local_id: SmartString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoutineStatement {
    pub token: Token,
    pub or_replace: bool,
    pub kind: RoutineKindSyntax,
    pub name: ObjectName,
    pub arguments: Vec<RoutineArgumentSyntax>,
    pub returns: Option<RoutineReturnSyntax>,
    pub volatility: Option<RoutineVolatilitySyntax>,
    pub security: RoutineSecuritySyntax,
    pub search_path: Vec<ObjectName>,
    pub resource_policy: Option<ObjectName>,
    pub body: Option<ProceduralBlock>,
    pub native: Option<NativeFunctionBindingSyntax>,
    pub normalized_source: String,
    pub span: SourceRange,
}

impl fmt::Display for CreateRoutineStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CREATE ")?;
        if self.or_replace {
            formatter.write_str("OR REPLACE ")?;
        }
        let kind = match self.kind {
            RoutineKindSyntax::Function => "FUNCTION",
            RoutineKindSyntax::Procedure => "PROCEDURE",
        };
        write!(formatter, "{kind} {}(", self.name)?;
        for (index, argument) in self.arguments.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{argument}")?;
        }
        formatter.write_str(")")?;
        if let Some(returns) = &self.returns {
            write!(formatter, " RETURNS {returns}")?;
        }
        if let Some(native) = &self.native {
            write!(
                formatter,
                " LANGUAGE NATIVE FROM EXTENSION {} AS '{}'",
                native.extension,
                escape_sql_string(native.local_id.as_str())
            )?;
            return Ok(());
        }
        formatter.write_str(" LANGUAGE RADIX")?;
        if let Some(volatility) = self.volatility {
            write!(formatter, " {volatility}")?;
        }
        write!(formatter, " {}", self.security)?;
        if !self.search_path.is_empty() {
            formatter.write_str(" SEARCH PATH (")?;
            for (index, namespace) in self.search_path.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{namespace}")?;
            }
            formatter.write_str(")")?;
        }
        if let Some(policy) = &self.resource_policy {
            write!(formatter, " RESOURCE POLICY {policy}")?;
        }
        write!(
            formatter,
            " AS {}",
            self.body
                .as_ref()
                .expect("RADIX routine requires a procedural body")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTimingSyntax {
    Before,
    After,
}

impl fmt::Display for TriggerTimingSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerLevelSyntax {
    Row,
    Statement,
}

impl fmt::Display for TriggerLevelSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Row => "ROW",
            Self::Statement => "STATEMENT",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEventSyntax {
    Insert,
    Update { columns: Vec<Identifier> },
    Delete,
}

impl fmt::Display for TriggerEventSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => formatter.write_str("INSERT"),
            Self::Update { columns } => {
                formatter.write_str("UPDATE")?;
                if !columns.is_empty() {
                    formatter.write_str(" OF ")?;
                    write_identifiers(formatter, columns)?;
                }
                Ok(())
            }
            Self::Delete => formatter.write_str("DELETE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutineSignatureSyntax {
    pub name: ObjectName,
    pub argument_types: Vec<ProceduralType>,
}

impl fmt::Display for RoutineSignatureSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}(", self.name)?;
        for (index, argument_type) in self.argument_types.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{argument_type}")?;
        }
        formatter.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStatement {
    pub token: Token,
    pub or_replace: bool,
    pub name: ObjectName,
    pub timing: TriggerTimingSyntax,
    pub events: Vec<TriggerEventSyntax>,
    pub table: ObjectName,
    pub level: TriggerLevelSyntax,
    pub priority: i32,
    pub when: Option<Expression>,
    pub function: RoutineSignatureSyntax,
    pub span: SourceRange,
}

impl fmt::Display for CreateTriggerStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CREATE ")?;
        if self.or_replace {
            formatter.write_str("OR REPLACE ")?;
        }
        write!(formatter, "TRIGGER {} {} ", self.name, self.timing)?;
        for (index, event) in self.events.iter().enumerate() {
            if index > 0 {
                formatter.write_str(" OR ")?;
            }
            write!(formatter, "{event}")?;
        }
        write!(
            formatter,
            " ON {} FOR EACH {} PRIORITY {}",
            self.table, self.level, self.priority
        )?;
        if let Some(condition) = &self.when {
            write!(formatter, " WHEN ({condition})")?;
        }
        write!(formatter, " EXECUTE FUNCTION {}", self.function)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobScheduleSyntax {
    Every(Expression),
    At(Expression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateJobStatement {
    pub token: Token,
    pub name: ObjectName,
    pub schedule: JobScheduleSyntax,
    pub principal: ObjectName,
    pub procedure: ObjectName,
    pub arguments: Vec<CallArgumentSyntax>,
    pub enabled: bool,
    pub span: SourceRange,
}

impl fmt::Display for CreateJobStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CREATE JOB {} SCHEDULE ", self.name)?;
        match &self.schedule {
            JobScheduleSyntax::Every(interval) => write!(formatter, "EVERY {interval}")?,
            JobScheduleSyntax::At(timestamp) => write!(formatter, "AT {timestamp}")?,
        }
        write!(
            formatter,
            " RUN AS {} CALL {}(",
            self.principal, self.procedure
        )?;
        for (index, argument) in self.arguments.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{argument}")?;
        }
        formatter.write_str(if self.enabled {
            ") ENABLE"
        } else {
            ") DISABLE"
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropRoutineStatement {
    pub token: Token,
    pub kind: RoutineKindSyntax,
    pub signature: RoutineSignatureSyntax,
    pub if_exists: bool,
    pub behavior: DropBehaviorSyntax,
}

impl fmt::Display for DropRoutineStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            RoutineKindSyntax::Function => "DROP FUNCTION ",
            RoutineKindSyntax::Procedure => "DROP PROCEDURE ",
        })?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(formatter, "{} {}", self.signature, self.behavior)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStatement {
    pub token: Token,
    pub name: ObjectName,
    pub table: ObjectName,
    pub if_exists: bool,
    pub behavior: DropBehaviorSyntax,
}

impl fmt::Display for DropTriggerStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DROP TRIGGER ")?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(
            formatter,
            "{} ON {} {}",
            self.name, self.table, self.behavior
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropJobStatement {
    pub token: Token,
    pub name: ObjectName,
    pub if_exists: bool,
    pub behavior: DropBehaviorSyntax,
}

impl fmt::Display for DropJobStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DROP JOB ")?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(formatter, "{} {}", self.name, self.behavior)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterJobStatement {
    pub token: Token,
    pub name: ObjectName,
    pub enabled: bool,
}

impl fmt::Display for AlterJobStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ALTER JOB {} {}",
            self.name,
            if self.enabled { "ENABLE" } else { "DISABLE" }
        )
    }
}

fn write_identifiers(
    formatter: &mut fmt::Formatter<'_>,
    identifiers: &[Identifier],
) -> fmt::Result {
    for (index, identifier) in identifiers.iter().enumerate() {
        if index > 0 {
            formatter.write_str(", ")?;
        }
        write!(formatter, "{identifier}")?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProceduralDeclaration {
    Variable {
        token: Token,
        name: Identifier,
        constant: bool,
        data_type: ProceduralType,
        nullable: bool,
        initializer: Option<Expression>,
        span: SourceRange,
    },
    Collection {
        token: Token,
        name: Identifier,
        element_type: ProceduralType,
        capacity: u32,
        span: SourceRange,
    },
    Cursor {
        token: Token,
        name: Identifier,
        arguments: Vec<RoutineArgumentSyntax>,
        query: Box<Statement>,
        span: SourceRange,
    },
}

impl fmt::Display for ProceduralDeclaration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable {
                name,
                constant,
                data_type,
                nullable,
                initializer,
                ..
            } => {
                write!(formatter, "{name} ")?;
                if *constant {
                    formatter.write_str("CONSTANT ")?;
                }
                write!(formatter, "{data_type}")?;
                if !nullable {
                    formatter.write_str(" NOT NULL")?;
                }
                if let Some(initializer) = initializer {
                    write!(formatter, " := {initializer}")?;
                }
                Ok(())
            }
            Self::Collection {
                name,
                element_type,
                capacity,
                ..
            } => write!(formatter, "{name} ARRAY<{element_type}, {capacity}>"),
            Self::Cursor {
                name,
                arguments,
                query,
                ..
            } => {
                write!(formatter, "CURSOR {name}(")?;
                for (index, argument) in arguments.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{} {}", argument.name, argument.data_type)?;
                }
                write!(formatter, ") FOR {query}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallArgumentSyntax {
    pub name: Option<Identifier>,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallStatement {
    pub token: Token,
    pub routine: ObjectName,
    pub arguments: Vec<CallArgumentSyntax>,
}

impl fmt::Display for CallStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CALL {}(", self.routine)?;
        for (index, argument) in self.arguments.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{argument}")?;
        }
        formatter.write_str(")")
    }
}

impl fmt::Display for CallArgumentSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(formatter, "{name} => ")?;
        }
        write!(formatter, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentTargetSyntax {
    Name(ObjectName),
    Index {
        collection: ObjectName,
        index: Expression,
    },
}

impl fmt::Display for AssignmentTargetSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => write!(formatter, "{name}"),
            Self::Index { collection, index } => write!(formatter, "{collection}[{index}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralSqlStatement {
    pub statement: Box<Statement>,
    pub into: Vec<Identifier>,
    pub strict: bool,
    pub span: SourceRange,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnSyntax {
    Void,
    Value(Expression),
    Next(Vec<Expression>),
    Query(Box<Statement>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoopControlKind {
    Exit,
    Continue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ForSourceSyntax {
    Numeric {
        reverse: bool,
        start: Box<Expression>,
        end: Box<Expression>,
        step: Option<Box<Expression>>,
    },
    Query(Box<Statement>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseArmSyntax {
    pub condition: Expression,
    pub statements: Vec<ProceduralStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicExecuteSyntax {
    pub source: Expression,
    pub into: Vec<Identifier>,
    pub strict: bool,
    pub using: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProceduralStatement {
    Assignment {
        token: Token,
        target: AssignmentTargetSyntax,
        value: Expression,
        span: SourceRange,
    },
    Call {
        token: Token,
        routine: ObjectName,
        arguments: Vec<CallArgumentSyntax>,
        span: SourceRange,
    },
    Perform {
        token: Token,
        expression: Expression,
        span: SourceRange,
    },
    If {
        token: Token,
        branches: Vec<(Expression, Vec<ProceduralStatement>)>,
        otherwise: Vec<ProceduralStatement>,
        span: SourceRange,
    },
    Case {
        token: Token,
        operand: Option<Expression>,
        arms: Vec<CaseArmSyntax>,
        otherwise: Vec<ProceduralStatement>,
        span: SourceRange,
    },
    Loop {
        token: Token,
        statements: Vec<ProceduralStatement>,
        span: SourceRange,
    },
    While {
        token: Token,
        condition: Expression,
        statements: Vec<ProceduralStatement>,
        span: SourceRange,
    },
    For {
        token: Token,
        variable: Identifier,
        source: ForSourceSyntax,
        statements: Vec<ProceduralStatement>,
        span: SourceRange,
    },
    LoopControl {
        token: Token,
        kind: LoopControlKind,
        condition: Option<Expression>,
        span: SourceRange,
    },
    Return {
        token: Token,
        value: ReturnSyntax,
        span: SourceRange,
    },
    Sql(ProceduralSqlStatement),
    DynamicExecute {
        token: Token,
        execute: DynamicExecuteSyntax,
        span: SourceRange,
    },
    OpenCursor {
        token: Token,
        cursor: Identifier,
        arguments: Vec<Expression>,
        span: SourceRange,
    },
    FetchCursor {
        token: Token,
        cursor: Identifier,
        into: Vec<Identifier>,
        span: SourceRange,
    },
    CloseCursor {
        token: Token,
        cursor: Identifier,
        span: SourceRange,
    },
    Raise {
        token: Token,
        kind: Option<Identifier>,
        arguments: Vec<Expression>,
        span: SourceRange,
    },
    Block(Box<ProceduralBlock>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExceptionPatternSyntax {
    Named(Identifier),
    Others,
}

impl fmt::Display for ExceptionPatternSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => write!(formatter, "{name}"),
            Self::Others => formatter.write_str("OTHERS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExceptionHandlerSyntax {
    pub token: Token,
    pub patterns: Vec<ExceptionPatternSyntax>,
    pub alias: Option<Identifier>,
    pub statements: Vec<ProceduralStatement>,
    pub span: SourceRange,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralBlock {
    pub token: Token,
    pub declarations: Vec<ProceduralDeclaration>,
    pub statements: Vec<ProceduralStatement>,
    pub handlers: Vec<ExceptionHandlerSyntax>,
    pub span: SourceRange,
}

impl fmt::Display for ProceduralBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.declarations.is_empty() {
            formatter.write_str("DECLARE ")?;
            for declaration in &self.declarations {
                write!(formatter, "{declaration}; ")?;
            }
        }
        formatter.write_str("BEGIN ")?;
        for statement in &self.statements {
            write!(formatter, "{statement}; ")?;
        }
        if !self.handlers.is_empty() {
            formatter.write_str("EXCEPTION ")?;
            for handler in &self.handlers {
                formatter.write_str("WHEN ")?;
                for (index, pattern) in handler.patterns.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(" OR ")?;
                    }
                    write!(formatter, "{pattern}")?;
                }
                if let Some(alias) = &handler.alias {
                    write!(formatter, " AS {alias}")?;
                }
                formatter.write_str(" THEN ")?;
                for statement in &handler.statements {
                    write!(formatter, "{statement}; ")?;
                }
            }
        }
        formatter.write_str("END")
    }
}

impl fmt::Display for ProceduralStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Assignment { target, value, .. } => write!(formatter, "{target} := {value}"),
            Self::Call {
                routine, arguments, ..
            } => {
                write!(formatter, "CALL {routine}(")?;
                for (index, argument) in arguments.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{argument}")?;
                }
                formatter.write_str(")")
            }
            Self::Perform { expression, .. } => write!(formatter, "PERFORM {expression}"),
            Self::If {
                branches,
                otherwise,
                ..
            } => {
                for (index, (condition, statements)) in branches.iter().enumerate() {
                    if index == 0 {
                        write!(formatter, "IF {condition} THEN ")?;
                    } else {
                        write!(formatter, "ELSIF {condition} THEN ")?;
                    }
                    for statement in statements {
                        write!(formatter, "{statement}; ")?;
                    }
                }
                if !otherwise.is_empty() {
                    formatter.write_str("ELSE ")?;
                    for statement in otherwise {
                        write!(formatter, "{statement}; ")?;
                    }
                }
                formatter.write_str("END IF")
            }
            Self::Case {
                operand,
                arms,
                otherwise,
                ..
            } => {
                formatter.write_str("CASE")?;
                if let Some(operand) = operand {
                    write!(formatter, " {operand}")?;
                }
                formatter.write_str(" ")?;
                for arm in arms {
                    write!(formatter, "WHEN {} THEN ", arm.condition)?;
                    for statement in &arm.statements {
                        write!(formatter, "{statement}; ")?;
                    }
                }
                if !otherwise.is_empty() {
                    formatter.write_str("ELSE ")?;
                    for statement in otherwise {
                        write!(formatter, "{statement}; ")?;
                    }
                }
                formatter.write_str("END CASE")
            }
            Self::Loop { statements, .. } => {
                formatter.write_str("LOOP ")?;
                for statement in statements {
                    write!(formatter, "{statement}; ")?;
                }
                formatter.write_str("END LOOP")
            }
            Self::While {
                condition,
                statements,
                ..
            } => {
                write!(formatter, "WHILE {condition} LOOP ")?;
                for statement in statements {
                    write!(formatter, "{statement}; ")?;
                }
                formatter.write_str("END LOOP")
            }
            Self::For {
                variable,
                source,
                statements,
                ..
            } => {
                write!(formatter, "FOR {variable} IN ")?;
                match source {
                    ForSourceSyntax::Numeric {
                        reverse,
                        start,
                        end,
                        step,
                    } => {
                        if *reverse {
                            formatter.write_str("REVERSE ")?;
                        }
                        write!(formatter, "{start} TO {end}")?;
                        if let Some(step) = step {
                            write!(formatter, " BY {step}")?;
                        }
                    }
                    ForSourceSyntax::Query(query) => write!(formatter, "({query})")?,
                }
                formatter.write_str(" LOOP ")?;
                for statement in statements {
                    write!(formatter, "{statement}; ")?;
                }
                formatter.write_str("END LOOP")
            }
            Self::LoopControl {
                kind, condition, ..
            } => {
                formatter.write_str(match kind {
                    LoopControlKind::Exit => "EXIT",
                    LoopControlKind::Continue => "CONTINUE",
                })?;
                if let Some(condition) = condition {
                    write!(formatter, " WHEN {condition}")?;
                }
                Ok(())
            }
            Self::Return { value, .. } => match value {
                ReturnSyntax::Void => formatter.write_str("RETURN"),
                ReturnSyntax::Value(value) => write!(formatter, "RETURN {value}"),
                ReturnSyntax::Next(values) => {
                    formatter.write_str("RETURN NEXT (")?;
                    for (index, value) in values.iter().enumerate() {
                        if index > 0 {
                            formatter.write_str(", ")?;
                        }
                        write!(formatter, "{value}")?;
                    }
                    formatter.write_str(")")
                }
                ReturnSyntax::Query(query) => write!(formatter, "RETURN QUERY {query}"),
            },
            Self::Sql(sql) => {
                let rendered = sql.statement.to_string();
                if sql.into.is_empty() {
                    return formatter.write_str(&rendered);
                }
                if matches!(sql.statement.as_ref(), Statement::Select(_)) {
                    if let Some(offset) = top_level_from_offset(&rendered) {
                        formatter.write_str(rendered[..offset].trim_end())?;
                        write_into_targets(formatter, sql.strict, &sql.into)?;
                        formatter.write_str(" ")?;
                        formatter.write_str(&rendered[offset..])?;
                    } else {
                        formatter.write_str(&rendered)?;
                        write_into_targets(formatter, sql.strict, &sql.into)?;
                    }
                } else {
                    formatter.write_str(&rendered)?;
                    write_into_targets(formatter, sql.strict, &sql.into)?;
                }
                Ok(())
            }
            Self::DynamicExecute { execute, .. } => {
                write!(formatter, "EXECUTE {}", execute.source)?;
                if !execute.into.is_empty() {
                    formatter.write_str(" INTO ")?;
                    if execute.strict {
                        formatter.write_str("STRICT ")?;
                    }
                    for (index, target) in execute.into.iter().enumerate() {
                        if index > 0 {
                            formatter.write_str(", ")?;
                        }
                        write!(formatter, "{target}")?;
                    }
                }
                if !execute.using.is_empty() {
                    formatter.write_str(" USING ")?;
                    for (index, argument) in execute.using.iter().enumerate() {
                        if index > 0 {
                            formatter.write_str(", ")?;
                        }
                        write!(formatter, "{argument}")?;
                    }
                }
                Ok(())
            }
            Self::OpenCursor {
                cursor, arguments, ..
            } => {
                write!(formatter, "OPEN {cursor}(")?;
                for (index, argument) in arguments.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{argument}")?;
                }
                formatter.write_str(")")
            }
            Self::FetchCursor { cursor, into, .. } => {
                write!(formatter, "FETCH {cursor} INTO ")?;
                for (index, target) in into.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{target}")?;
                }
                Ok(())
            }
            Self::CloseCursor { cursor, .. } => write!(formatter, "CLOSE {cursor}"),
            Self::Raise {
                kind, arguments, ..
            } => {
                formatter.write_str("RAISE")?;
                if let Some(kind) = kind {
                    write!(formatter, " {kind}(")?;
                    for (index, argument) in arguments.iter().enumerate() {
                        if index > 0 {
                            formatter.write_str(", ")?;
                        }
                        write!(formatter, "{argument}")?;
                    }
                    formatter.write_str(")")?;
                }
                Ok(())
            }
            Self::Block(block) => write!(formatter, "{block}"),
        }
    }
}

fn write_into_targets(
    formatter: &mut fmt::Formatter<'_>,
    strict: bool,
    targets: &[Identifier],
) -> fmt::Result {
    formatter.write_str(" INTO ")?;
    if strict {
        formatter.write_str("STRICT ")?;
    }
    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            formatter.write_str(", ")?;
        }
        write!(formatter, "{target}")?;
    }
    Ok(())
}

fn top_level_from_offset(sql: &str) -> Option<usize> {
    let mut lexer = crate::Lexer::new(sql);
    let mut depth = 0usize;
    loop {
        let token = lexer.next_token();
        if token.is_eof() {
            return None;
        }
        if token.is_punctuator("(") {
            depth += 1;
        } else if token.is_punctuator(")") {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_keyword("FROM") {
            return Some(token.position.offset);
        }
    }
}
