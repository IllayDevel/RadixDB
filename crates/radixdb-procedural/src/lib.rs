//! Verified, bounded procedural execution for RadixDB.
//!
//! Source parsing remains owned by `radixdb-sql`; storage and query execution
//! remain behind the host traits in this crate.  This boundary deliberately
//! exposes no storage handle and no network, filesystem, process or native
//! extension capability.

pub mod budget;
pub mod compiler;
pub mod diagnostic;
pub mod host;
pub mod ir;
pub mod runtime;
pub mod value;

pub use budget::{BudgetOwner, BudgetSnapshot, CancellationHandle, CancellationProbe};
pub use compiler::{
    compile_routine, compile_trigger_routine, BoundCallArgument, BoundExpression,
    BoundResultColumn, BoundRoutineCall, BoundSqlParameter, BoundSqlStatement, BoundType,
    CallSiteArgument, CallSiteArgumentValue, CompileIdentity, CompiledRoutine, LocalBinding,
    SemanticResolver, TriggerCompileContext, TriggerReturnRecord,
};
pub use diagnostic::{
    Diagnostic, DiagnosticCategory, DiagnosticDetail, DiagnosticFrame, DiagnosticKind,
    SecondarySpan, SourcePosition, SourceSpan,
};
pub use host::{
    AuditEvent, AuditHost, CursorHost, CursorToken, OutboxHost, OutboxMessage, PrincipalContext,
    PrincipalHost, RoutineCallHost, RuntimeHost, SavepointToken, SqlHost, SqlOutcome, SqlRowSink,
    TransactionHost,
};
pub use ir::{
    admit_embedded_sql, verify, BasicBlock, BlockId, CursorId, CursorStatusAttribute,
    ExceptionRoute, Instruction, Program, ProgramIdentity, SlotDefinition, SlotId,
    SpannedInstruction, SpannedTerminator, SqlStatusAttribute, Terminator, VerifiedProgram,
    MAX_STATIC_IR_INSTRUCTIONS, MAX_STATIC_IR_NODES,
};
pub use runtime::{ExecutionOutcome, Interpreter, SqlStatus};
pub use value::{RecordField, RuntimeType, RuntimeValue};

pub type ProceduralResult<T> = Result<T, Diagnostic>;
