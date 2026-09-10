use radixdb_catalog::ObjectId;
use radixdb_core::Value;
use radixdb_sql::{Expression, InfixOperator, Statement};

use crate::{BudgetOwner, ProceduralResult, RuntimeValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SavepointToken(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorToken(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrincipalContext {
    pub session_principal: ObjectId,
    pub invoker_principal: ObjectId,
    pub effective_principal: ObjectId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditEvent {
    pub object_id: ObjectId,
    pub command_fingerprint: [u8; 32],
    /// Validated JSON object. The executor enforces size and secret-bearing
    /// key restrictions before inserting the system-owned row.
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxMessage {
    pub idempotency_key: String,
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlOutcome {
    pub affected_rows: u64,
}

/// Pull-free row sink used by the executor bridge. The host may produce rows
/// incrementally and never needs to materialize the full result in this crate.
pub trait SqlRowSink {
    fn push_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()>;
}

pub trait SqlHost {
    /// Evaluate a semantically bound SQL expression using the same expression
    /// compiler/evaluator as ordinary SQL execution.
    fn evaluate_expression(
        &mut self,
        expression: &Expression,
        parameters: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue>;

    /// Evaluate one already-admitted SQL binary operator over materialized
    /// operands. This is used when a procedural intrinsic (for example
    /// `SQL%ROWCOUNT`) appears inside a larger expression. The executor remains
    /// the sole owner of SQL NULL, comparison and arithmetic semantics.
    fn evaluate_binary(
        &mut self,
        _operator: InfixOperator,
        _left: &RuntimeValue,
        _right: &RuntimeValue,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue> {
        Err(crate::Diagnostic::new(
            crate::DiagnosticKind::VerifyCapabilityDenied,
            "SQL binary evaluation is not implemented by this executor host",
        ))
    }

    fn execute_sql(
        &mut self,
        statement: &Statement,
        parameters: &[RuntimeValue],
        rows: &mut dyn SqlRowSink,
        budget: &BudgetOwner,
    ) -> ProceduralResult<SqlOutcome>;

    /// Parse and execute one dynamically produced SQL statement. Executor
    /// implementations must route this through the shared SQL parser, binder,
    /// ACL checks and statement executor. The default implementation is fail
    /// closed so a host cannot accidentally admit a second SQL path.
    fn execute_dynamic_sql(
        &mut self,
        _source: &str,
        _parameters: &[RuntimeValue],
        _rows: &mut dyn SqlRowSink,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<SqlOutcome> {
        Err(crate::Diagnostic::new(
            crate::DiagnosticKind::VerifyCapabilityDenied,
            "dynamic SQL is not implemented by this executor host",
        ))
    }
}

/// Streaming cursor boundary. Cursor state remains owned by the executor and
/// tied to the caller transaction; the procedural frame retains only an
/// opaque token.
pub trait CursorHost {
    fn open_cursor(
        &mut self,
        _statement: &Statement,
        _parameters: &[RuntimeValue],
        _budget: &BudgetOwner,
    ) -> ProceduralResult<CursorToken> {
        Err(crate::Diagnostic::new(
            crate::DiagnosticKind::VerifyCapabilityDenied,
            "streaming cursors are not implemented by this executor host",
        ))
    }

    fn fetch_cursor(
        &mut self,
        _cursor: CursorToken,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<Option<Vec<RuntimeValue>>> {
        Err(crate::Diagnostic::new(
            crate::DiagnosticKind::VerifyCapabilityDenied,
            "streaming cursors are not implemented by this executor host",
        ))
    }

    fn close_cursor(&mut self, _cursor: CursorToken) -> ProceduralResult<()> {
        Err(crate::Diagnostic::new(
            crate::DiagnosticKind::VerifyCapabilityDenied,
            "streaming cursors are not implemented by this executor host",
        ))
    }
}

pub trait TransactionHost {
    fn create_savepoint(&mut self) -> ProceduralResult<SavepointToken>;
    fn rollback_savepoint(&mut self, savepoint: SavepointToken) -> ProceduralResult<()>;
    fn release_savepoint(&mut self, savepoint: SavepointToken) -> ProceduralResult<()>;
}

pub trait PrincipalHost {
    fn principal_context(&self) -> PrincipalContext;
    fn push_definer(&mut self, owner: ObjectId) -> ProceduralResult<()>;
    fn pop_definer(&mut self) -> ProceduralResult<()>;
}

pub trait RoutineCallHost {
    fn call_routine(
        &mut self,
        routine: ObjectId,
        arguments: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<Vec<RuntimeValue>>;
}

pub trait AuditHost {
    fn append_audit(&mut self, event: AuditEvent) -> ProceduralResult<()>;
}

pub trait OutboxHost {
    fn append_outbox(&mut self, message: OutboxMessage) -> ProceduralResult<()>;
}

pub trait RuntimeHost:
    SqlHost + CursorHost + TransactionHost + PrincipalHost + RoutineCallHost + AuditHost + OutboxHost
{
}

impl<T> RuntimeHost for T where
    T: SqlHost
        + CursorHost
        + TransactionHost
        + PrincipalHost
        + RoutineCallHost
        + AuditHost
        + OutboxHost
{
}
