use radixdb_catalog::{CatalogName, ObjectId};
use radixdb_sql::{Expression, InfixOperator, ObjectName, ProceduralType, Statement};

use crate::{ProceduralResult, Program, RecordField, RuntimeType, SlotId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerReturnRecord {
    Old,
    New,
}

/// Table-specialized bindings used to compile a `RETURNS TRIGGER` function.
///
/// A trigger function has no durable SQL arguments: its typed row descriptor
/// and availability/mutability matrix come from the trigger attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerCompileContext {
    pub record_fields: Vec<RecordField>,
    pub old_available: bool,
    pub new_available: bool,
    pub new_writable: bool,
    pub return_record: Option<TriggerReturnRecord>,
}

impl TriggerCompileContext {
    pub fn validate(&self) -> ProceduralResult<()> {
        RuntimeType::record(self.record_fields.clone())?;
        if self.new_writable && !self.new_available {
            return Err(crate::Diagnostic::new(
                crate::DiagnosticKind::RuntimeInvalidIr,
                "writable NEW requires an available NEW record",
            ));
        }
        if matches!(self.return_record, Some(TriggerReturnRecord::Old)) && !self.old_available {
            return Err(crate::Diagnostic::new(
                crate::DiagnosticKind::RuntimeInvalidIr,
                "OLD trigger return requires an available OLD record",
            ));
        }
        if matches!(self.return_record, Some(TriggerReturnRecord::New)) && !self.new_available {
            return Err(crate::Diagnostic::new(
                crate::DiagnosticKind::RuntimeInvalidIr,
                "NEW trigger return requires an available NEW record",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileIdentity {
    pub object_id: ObjectId,
    pub definition_revision: u64,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBinding {
    pub name: String,
    pub slot: SlotId,
    pub runtime_type: RuntimeType,
    pub constant: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundExpression {
    pub expression: Expression,
    pub parameters: Vec<SlotId>,
    pub result_type: RuntimeType,
    pub dependencies: Vec<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundType {
    pub runtime_type: RuntimeType,
    pub dependencies: Vec<ObjectId>,
}

impl BoundType {
    pub fn scalar(runtime_type: RuntimeType) -> Self {
        Self {
            runtime_type,
            dependencies: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundResultColumn {
    pub name: CatalogName,
    pub runtime_type: RuntimeType,
}

impl BoundResultColumn {
    pub const fn new(name: CatalogName, runtime_type: RuntimeType) -> Self {
        Self { name, runtime_type }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundSqlStatement {
    pub statement: Statement,
    pub parameters: Vec<BoundSqlParameter>,
    pub result_columns: Vec<BoundResultColumn>,
    pub dependencies: Vec<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundSqlParameter {
    Scalar(SlotId),
    RecordField {
        record: SlotId,
        field: u32,
        runtime_type: RuntimeType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallSiteArgument {
    pub name: Option<String>,
    pub value: CallSiteArgumentValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallSiteArgumentValue {
    Bound {
        slot: SlotId,
        runtime_type: RuntimeType,
        assignable: bool,
    },
    UntypedNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundCallArgument {
    Provided {
        declared_name: String,
        slot: SlotId,
    },
    Default {
        declared_name: String,
        expression: Expression,
        runtime_type: RuntimeType,
    },
    ContextualExpression {
        declared_name: String,
        expression: BoundExpression,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundRoutineCall {
    pub routine: ObjectId,
    /// Input slots in declared parameter order after named/default argument
    /// resolution.
    pub arguments: Vec<BoundCallArgument>,
    /// OUT/INOUT target slots in declared result order.
    pub results: Vec<SlotId>,
    pub dependencies: Vec<ObjectId>,
    pub cost: u32,
}

pub trait SemanticResolver {
    fn resolve_type(&mut self, syntax: &ProceduralType) -> ProceduralResult<BoundType>;

    /// Bind one scalar expression against lexical locals and ordinary SQL
    /// semantics. Implementations replace local references with internal
    /// positional parameters and return those parameter slots in order.
    fn bind_expression(
        &mut self,
        expression: &Expression,
        locals: &[LocalBinding],
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<BoundExpression>;

    /// Bind an operator whose operands include VM-provided intrinsic values.
    /// The executor-side SQL binder remains the authority for operator
    /// admission, coercion and result type; the procedural compiler never
    /// carries a parallel SQL type table.
    fn bind_binary_operator(
        &mut self,
        operator: InfixOperator,
        left: &RuntimeType,
        right: &RuntimeType,
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<RuntimeType>;

    /// Bind an embedded query/DML statement using the ordinary SQL binder.
    fn bind_statement(
        &mut self,
        statement: &Statement,
        locals: &[LocalBinding],
    ) -> ProceduralResult<BoundSqlStatement>;

    /// Resolve procedure overload, named/default arguments and OUT/INOUT
    /// targets on the caller's pinned catalog generation.
    fn bind_procedure_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[CallSiteArgument],
    ) -> ProceduralResult<BoundRoutineCall>;

    /// Admit a system-owned transactional write such as audit/outbox append.
    /// Executor resolvers use this hook to reject the operation for
    /// IMMUTABLE/STABLE functions at DDL admission.
    fn admit_transactional_side_effect(&mut self) -> ProceduralResult<()> {
        Ok(())
    }

    /// Return true only for an expression bound to the system observability
    /// capability. This is used to admit a catch-all handler that intentionally
    /// consumes an error instead of rethrowing it.
    fn is_observability_expression(&mut self, _expression: &Expression) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledRoutine {
    pub program: Program,
    pub dependencies: Vec<ObjectId>,
}
