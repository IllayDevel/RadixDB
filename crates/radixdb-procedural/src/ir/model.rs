use radixdb_catalog::ObjectId;
use radixdb_sql::{Expression, InfixOperator, Statement};

use crate::{DiagnosticKind, RuntimeType, RuntimeValue, SourceSpan};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CursorId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlStatusAttribute {
    RowCount,
    Found,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorStatusAttribute {
    IsOpen,
    Found,
    NotFound,
    RowCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionRoute {
    /// Empty means `OTHERS`; non-empty kinds are matched in declaration order.
    pub kinds: Vec<DiagnosticKind>,
    pub handler: BlockId,
    pub error_slot: Option<SlotId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramIdentity {
    object_id: ObjectId,
    definition_revision: u64,
    name: String,
}

impl ProgramIdentity {
    pub fn new(object_id: ObjectId, definition_revision: u64, name: impl Into<String>) -> Self {
        Self {
            object_id,
            definition_revision,
            name: name.into(),
        }
    }

    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }
    pub const fn definition_revision(&self) -> u64 {
        self.definition_revision
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDefinition {
    name: String,
    runtime_type: RuntimeType,
}

impl SlotDefinition {
    pub fn new(name: impl Into<String>, runtime_type: RuntimeType) -> Self {
        Self {
            name: name.into(),
            runtime_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn runtime_type(&self) -> &RuntimeType {
        &self.runtime_type
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Instruction {
    InitializeNull {
        destination: SlotId,
    },
    LoadConstant {
        destination: SlotId,
        value: RuntimeValue,
    },
    /// Evaluate one already-bound SQL expression through the executor host.
    ///
    /// `expression` may contain only executor-created positional parameters;
    /// source-level external parameters are rejected by the procedural parser.
    /// `parameters` supplies those values in positional order. Keeping the SQL
    /// AST here prevents the procedural runtime from becoming a second owner
    /// of scalar SQL semantics.
    EvaluateExpression {
        expression: Box<Expression>,
        parameters: Vec<SlotId>,
        destination: SlotId,
    },
    Copy {
        destination: SlotId,
        source: SlotId,
    },
    IntegerAddChecked {
        destination: SlotId,
        left: SlotId,
        right: SlotId,
    },
    IntegerSubtractChecked {
        destination: SlotId,
        left: SlotId,
        right: SlotId,
    },
    IntegerLess {
        destination: SlotId,
        left: SlotId,
        right: SlotId,
    },
    EvaluateSqlBinary {
        destination: SlotId,
        left: SlotId,
        right: SlotId,
        operator: InfixOperator,
    },
    BooleanNot {
        destination: SlotId,
        source: SlotId,
    },
    QuoteSqlIdentifier {
        destination: SlotId,
        source: SlotId,
    },
    ConcatenateSqlText {
        destination: SlotId,
        left: SlotId,
        right: SlotId,
    },
    MakeRecord {
        destination: SlotId,
        fields: Vec<SlotId>,
    },
    ReadRecordField {
        destination: SlotId,
        record: SlotId,
        field: u32,
    },
    WriteRecordField {
        record: SlotId,
        field: u32,
        value: SlotId,
    },
    CollectionAppend {
        collection: SlotId,
        value: SlotId,
    },
    CollectionClear {
        collection: SlotId,
    },
    CollectionGet {
        destination: SlotId,
        collection: SlotId,
        one_based_index: SlotId,
    },
    CollectionSet {
        collection: SlotId,
        one_based_index: SlotId,
        value: SlotId,
    },
    CollectionCount {
        destination: SlotId,
        collection: SlotId,
    },
    ReadSqlStatus {
        destination: SlotId,
        attribute: SqlStatusAttribute,
    },
    ReadCursorStatus {
        destination: SlotId,
        cursor: CursorId,
        attribute: CursorStatusAttribute,
    },
    ExecuteSql {
        statement: Box<Statement>,
        parameters: Vec<SlotId>,
        into: Vec<SlotId>,
        strict: bool,
    },
    ExecuteDynamicSql {
        source: SlotId,
        parameters: Vec<SlotId>,
        into: Vec<SlotId>,
        strict: bool,
    },
    OpenCursor {
        cursor: CursorId,
        statement: Box<Statement>,
        parameters: Vec<SlotId>,
    },
    FetchCursor {
        cursor: CursorId,
        into: Vec<SlotId>,
        found: SlotId,
    },
    CloseCursor {
        cursor: CursorId,
    },
    EmitResultRow {
        values: Vec<SlotId>,
    },
    EmitResultQuery {
        statement: Box<Statement>,
        parameters: Vec<SlotId>,
    },
    AppendAudit {
        object_id: ObjectId,
        command_fingerprint: SlotId,
        metadata: SlotId,
    },
    AppendOutbox {
        idempotency_key: SlotId,
        schema_version: SlotId,
        payload: SlotId,
    },
    EnterExceptionRegion {
        routes: Vec<ExceptionRoute>,
    },
    LeaveExceptionRegion,
    Call {
        routine: ObjectId,
        arguments: Vec<SlotId>,
        results: Vec<SlotId>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedInstruction {
    instruction: Instruction,
    span: Option<SourceSpan>,
}

impl SpannedInstruction {
    pub const fn new(instruction: Instruction, span: Option<SourceSpan>) -> Self {
        Self { instruction, span }
    }

    pub const fn unspanned(instruction: Instruction) -> Self {
        Self::new(instruction, None)
    }

    pub const fn instruction(&self) -> &Instruction {
        &self.instruction
    }
    pub const fn span(&self) -> Option<&SourceSpan> {
        self.span.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Terminator {
    Jump(BlockId),
    Branch {
        condition: SlotId,
        when_true: BlockId,
        when_false: BlockId,
    },
    Return(Option<SlotId>),
    Raise(DiagnosticKind),
    Rethrow,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedTerminator {
    terminator: Terminator,
    span: Option<SourceSpan>,
}

impl SpannedTerminator {
    pub const fn new(terminator: Terminator, span: Option<SourceSpan>) -> Self {
        Self { terminator, span }
    }

    pub const fn unspanned(terminator: Terminator) -> Self {
        Self::new(terminator, None)
    }

    pub const fn terminator(&self) -> &Terminator {
        &self.terminator
    }
    pub const fn span(&self) -> Option<&SourceSpan> {
        self.span.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicBlock {
    instructions: Vec<SpannedInstruction>,
    terminator: SpannedTerminator,
}

impl BasicBlock {
    pub const fn new(instructions: Vec<SpannedInstruction>, terminator: SpannedTerminator) -> Self {
        Self {
            instructions,
            terminator,
        }
    }

    pub fn instructions(&self) -> &[SpannedInstruction] {
        &self.instructions
    }
    pub const fn terminator(&self) -> &SpannedTerminator {
        &self.terminator
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    identity: ProgramIdentity,
    slots: Vec<SlotDefinition>,
    parameter_slots: Vec<SlotId>,
    output_slots: Vec<SlotId>,
    result_type: Option<RuntimeType>,
    result_columns: Vec<RuntimeType>,
    blocks: Vec<BasicBlock>,
    entry: BlockId,
}

impl Program {
    pub const fn new(
        identity: ProgramIdentity,
        slots: Vec<SlotDefinition>,
        parameter_slots: Vec<SlotId>,
        result_type: Option<RuntimeType>,
        blocks: Vec<BasicBlock>,
        entry: BlockId,
    ) -> Self {
        Self {
            identity,
            slots,
            parameter_slots,
            output_slots: Vec::new(),
            result_type,
            result_columns: Vec::new(),
            blocks,
            entry,
        }
    }

    pub const fn identity(&self) -> &ProgramIdentity {
        &self.identity
    }

    pub fn slots(&self) -> &[SlotDefinition] {
        &self.slots
    }
    pub fn parameter_slots(&self) -> &[SlotId] {
        &self.parameter_slots
    }
    pub fn output_slots(&self) -> &[SlotId] {
        &self.output_slots
    }
    pub fn with_output_slots(mut self, output_slots: Vec<SlotId>) -> Self {
        self.output_slots = output_slots;
        self
    }
    pub const fn result_type(&self) -> Option<&RuntimeType> {
        self.result_type.as_ref()
    }
    pub fn result_columns(&self) -> &[RuntimeType] {
        &self.result_columns
    }
    pub fn with_result_columns(mut self, result_columns: Vec<RuntimeType>) -> Self {
        self.result_columns = result_columns;
        self
    }
    pub fn blocks(&self) -> &[BasicBlock] {
        &self.blocks
    }
    pub const fn entry(&self) -> BlockId {
        self.entry
    }
}
