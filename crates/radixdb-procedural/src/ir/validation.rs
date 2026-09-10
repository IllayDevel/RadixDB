use std::collections::BTreeSet;

use radixdb_core::DataType;

use super::{BlockId, Instruction, Program, SlotDefinition, SlotId};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeType, SourceSpan};

pub(super) fn slot<'a>(
    program: &'a Program,
    id: SlotId,
    span: Option<&SourceSpan>,
) -> ProceduralResult<&'a SlotDefinition> {
    program
        .slots()
        .get(id.0 as usize)
        .ok_or_else(|| invalid(span, "IR slot ID is out of bounds"))
}

pub(super) fn block_index(
    program: &Program,
    id: BlockId,
    span: Option<&SourceSpan>,
) -> ProceduralResult<usize> {
    let index = id.0 as usize;
    if index >= program.blocks().len() {
        Err(invalid(span, "IR block ID is out of bounds"))
    } else {
        Ok(index)
    }
}

pub(super) fn scalar(
    program: &Program,
    id: SlotId,
    logical_type: DataType,
    span: Option<&SourceSpan>,
) -> ProceduralResult<(radixdb_catalog::CatalogDataType, bool)> {
    let RuntimeType::Scalar {
        data_type,
        nullable,
    } = slot(program, id, span)?.runtime_type()
    else {
        return Err(invalid(span, "IR operand is not scalar"));
    };
    if data_type.logical_type() != logical_type {
        return Err(invalid(
            span,
            "IR scalar operand has the wrong logical type",
        ));
    }
    Ok((*data_type, *nullable))
}

pub(super) fn require_same_type(
    program: &Program,
    left: SlotId,
    right: SlotId,
    span: Option<&SourceSpan>,
) -> ProceduralResult<()> {
    if slot(program, left, span)?.runtime_type() != slot(program, right, span)?.runtime_type() {
        Err(invalid(span, "IR source and destination slot types differ"))
    } else {
        Ok(())
    }
}

pub(super) fn require_unique_slots(
    slots: &[SlotId],
    span: Option<&SourceSpan>,
    message: &'static str,
) -> ProceduralResult<()> {
    let mut unique = BTreeSet::new();
    if slots.iter().any(|slot| !unique.insert(*slot)) {
        Err(invalid(span, message))
    } else {
        Ok(())
    }
}

pub(super) fn require_scalar_operands(
    program: &Program,
    operands: &[(SlotId, DataType)],
    span: Option<&SourceSpan>,
) -> ProceduralResult<()> {
    for (slot_id, data_type) in operands {
        scalar(program, *slot_id, *data_type, span)?;
    }
    Ok(())
}

pub(super) fn instruction_operand_nodes(instruction: &Instruction) -> usize {
    match instruction {
        Instruction::MakeRecord { fields, .. } => fields.len(),
        Instruction::EvaluateExpression { parameters, .. }
        | Instruction::EmitResultRow { values: parameters }
        | Instruction::EmitResultQuery { parameters, .. }
        | Instruction::OpenCursor { parameters, .. } => parameters.len(),
        Instruction::AppendAudit { .. } => 2,
        Instruction::AppendOutbox { .. } => 3,
        Instruction::ExecuteSql {
            parameters, into, ..
        }
        | Instruction::ExecuteDynamicSql {
            parameters, into, ..
        } => parameters.len().saturating_add(into.len()),
        Instruction::FetchCursor { into, .. } => into.len(),
        Instruction::EnterExceptionRegion { routes } => {
            routes.iter().fold(routes.len(), |total, route| {
                total
                    .saturating_add(route.kinds.len())
                    .saturating_add(usize::from(route.error_slot.is_some()))
            })
        }
        Instruction::Call {
            arguments, results, ..
        } => arguments.len().saturating_add(results.len()),
        Instruction::InitializeNull { .. }
        | Instruction::LoadConstant { .. }
        | Instruction::Copy { .. }
        | Instruction::IntegerAddChecked { .. }
        | Instruction::IntegerSubtractChecked { .. }
        | Instruction::IntegerLess { .. }
        | Instruction::EvaluateSqlBinary { .. }
        | Instruction::BooleanNot { .. }
        | Instruction::QuoteSqlIdentifier { .. }
        | Instruction::ConcatenateSqlText { .. }
        | Instruction::ReadRecordField { .. }
        | Instruction::WriteRecordField { .. }
        | Instruction::CollectionAppend { .. }
        | Instruction::CollectionClear { .. }
        | Instruction::CollectionGet { .. }
        | Instruction::CollectionSet { .. }
        | Instruction::CollectionCount { .. }
        | Instruction::ReadSqlStatus { .. }
        | Instruction::ReadCursorStatus { .. }
        | Instruction::CloseCursor { .. }
        | Instruction::LeaveExceptionRegion => 0,
    }
}

pub(super) fn invalid(span: Option<&SourceSpan>, message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message).with_primary_span(span.cloned())
}

pub(super) fn limit(field: &'static str, actual: usize, limit: usize) -> Diagnostic {
    Diagnostic::new(
        DiagnosticKind::ParseLimitExceeded,
        "static IR limit exceeded",
    )
    .with_detail("field", field)
    .with_detail("actual", actual.to_string())
    .with_detail("limit", limit.to_string())
}
