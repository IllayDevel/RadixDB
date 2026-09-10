use std::collections::VecDeque;

use super::{BlockId, Instruction, Program, SlotId, Terminator};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeType};

pub(super) fn verify_definite_initialization(
    program: &Program,
    entry: usize,
    predecessors: &[Vec<usize>],
) -> ProceduralResult<()> {
    let slot_count = program.slots().len();
    let mut initial = vec![false; slot_count];
    for slot_id in program.parameter_slots() {
        initial[slot_id.0 as usize] = true;
    }
    for (index, definition) in program.slots().iter().enumerate() {
        if matches!(definition.runtime_type(), RuntimeType::Collection { .. }) {
            initial[index] = true;
        }
    }
    let mut incoming = vec![vec![true; slot_count]; program.blocks().len()];
    incoming[entry] = initial.clone();
    let mut outgoing = incoming.clone();
    let mut changed = true;
    while changed {
        changed = false;
        for index in 0..program.blocks().len() {
            let next_incoming = if index == entry {
                initial.clone()
            } else {
                intersect_predecessors(predecessors[index].as_slice(), &outgoing, slot_count)
            };
            let next_outgoing =
                apply_writes(program.blocks()[index].instructions(), &next_incoming);
            if incoming[index] != next_incoming || outgoing[index] != next_outgoing {
                incoming[index] = next_incoming;
                outgoing[index] = next_outgoing;
                changed = true;
            }
        }
    }
    for (index, block) in program.blocks().iter().enumerate() {
        let mut initialized = incoming[index].clone();
        for instruction in block.instructions() {
            for read in instruction_reads(instruction.instruction()) {
                if !initialized[read.0 as usize] {
                    return Err(invalid(
                        "IR reads a slot that is not initialized on every incoming path",
                    ));
                }
            }
            mark_writes(instruction.instruction(), &mut initialized);
        }
        for read in terminator_reads(block.terminator().terminator()) {
            if !initialized[read.0 as usize] {
                return Err(invalid("IR terminator reads an uninitialized slot"));
            }
        }
        if matches!(block.terminator().terminator(), Terminator::Return(_)) {
            for output in program.output_slots() {
                if !initialized[output.0 as usize] {
                    return Err(invalid(
                        "IR returns with an uninitialized OUT/INOUT parameter",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn intersect_predecessors(
    predecessors: &[usize],
    outgoing: &[Vec<bool>],
    slot_count: usize,
) -> Vec<bool> {
    let mut result = vec![true; slot_count];
    for predecessor in predecessors {
        for (result, initialized) in result.iter_mut().zip(&outgoing[*predecessor]) {
            *result &= *initialized;
        }
    }
    result
}

fn apply_writes(instructions: &[super::SpannedInstruction], incoming: &[bool]) -> Vec<bool> {
    let mut initialized = incoming.to_vec();
    for instruction in instructions {
        mark_writes(instruction.instruction(), &mut initialized);
    }
    initialized
}

fn mark_writes(instruction: &Instruction, initialized: &mut [bool]) {
    match instruction {
        Instruction::InitializeNull { destination }
        | Instruction::LoadConstant { destination, .. }
        | Instruction::EvaluateExpression { destination, .. }
        | Instruction::Copy { destination, .. }
        | Instruction::IntegerAddChecked { destination, .. }
        | Instruction::IntegerSubtractChecked { destination, .. }
        | Instruction::IntegerLess { destination, .. }
        | Instruction::EvaluateSqlBinary { destination, .. }
        | Instruction::BooleanNot { destination, .. }
        | Instruction::QuoteSqlIdentifier { destination, .. }
        | Instruction::ConcatenateSqlText { destination, .. }
        | Instruction::MakeRecord { destination, .. }
        | Instruction::ReadRecordField { destination, .. }
        | Instruction::CollectionGet { destination, .. }
        | Instruction::CollectionCount { destination, .. }
        | Instruction::ReadSqlStatus { destination, .. }
        | Instruction::ReadCursorStatus { destination, .. } => {
            initialized[destination.0 as usize] = true
        }
        Instruction::ExecuteSql { into, .. } | Instruction::ExecuteDynamicSql { into, .. } => {
            for destination in into {
                initialized[destination.0 as usize] = true;
            }
        }
        Instruction::FetchCursor { into, found, .. } => {
            initialized[found.0 as usize] = true;
            for destination in into {
                initialized[destination.0 as usize] = true;
            }
        }
        Instruction::EnterExceptionRegion { routes } => {
            for route in routes {
                if let Some(error_slot) = route.error_slot {
                    initialized[error_slot.0 as usize] = true;
                }
            }
        }
        Instruction::Call { results, .. } => {
            for destination in results {
                initialized[destination.0 as usize] = true;
            }
        }
        Instruction::WriteRecordField { record, .. } => {
            initialized[record.0 as usize] = true;
        }
        Instruction::CollectionAppend { .. }
        | Instruction::CollectionClear { .. }
        | Instruction::CollectionSet { .. }
        | Instruction::OpenCursor { .. }
        | Instruction::CloseCursor { .. }
        | Instruction::EmitResultRow { .. }
        | Instruction::EmitResultQuery { .. }
        | Instruction::AppendAudit { .. }
        | Instruction::AppendOutbox { .. }
        | Instruction::LeaveExceptionRegion => {}
    }
}

fn instruction_reads(instruction: &Instruction) -> Vec<SlotId> {
    match instruction {
        Instruction::InitializeNull { .. } | Instruction::LoadConstant { .. } => Vec::new(),
        Instruction::EvaluateExpression { parameters, .. } => parameters.clone(),
        Instruction::Copy { source, .. }
        | Instruction::BooleanNot { source, .. }
        | Instruction::QuoteSqlIdentifier { source, .. } => vec![*source],
        Instruction::IntegerAddChecked { left, right, .. }
        | Instruction::IntegerSubtractChecked { left, right, .. }
        | Instruction::IntegerLess { left, right, .. }
        | Instruction::EvaluateSqlBinary { left, right, .. }
        | Instruction::ConcatenateSqlText { left, right, .. } => vec![*left, *right],
        Instruction::MakeRecord { fields, .. } => fields.clone(),
        Instruction::ReadRecordField { record, .. } => vec![*record],
        Instruction::WriteRecordField { record, value, .. } => vec![*record, *value],
        Instruction::CollectionAppend { collection, value } => vec![*collection, *value],
        Instruction::CollectionClear { collection } => vec![*collection],
        Instruction::CollectionSet {
            collection,
            one_based_index,
            value,
        } => vec![*collection, *one_based_index, *value],
        Instruction::CollectionGet {
            collection,
            one_based_index,
            ..
        } => {
            vec![*collection, *one_based_index]
        }
        Instruction::CollectionCount { collection, .. } => vec![*collection],
        Instruction::ReadSqlStatus { .. } | Instruction::ReadCursorStatus { .. } => Vec::new(),
        Instruction::ExecuteSql { parameters, .. } => parameters.clone(),
        Instruction::ExecuteDynamicSql {
            source, parameters, ..
        } => {
            let mut reads = Vec::with_capacity(parameters.len() + 1);
            reads.push(*source);
            reads.extend(parameters.iter().copied());
            reads
        }
        Instruction::OpenCursor { parameters, .. } => parameters.clone(),
        Instruction::FetchCursor { .. }
        | Instruction::CloseCursor { .. }
        | Instruction::EnterExceptionRegion { .. }
        | Instruction::LeaveExceptionRegion => Vec::new(),
        Instruction::EmitResultRow { values } => values.clone(),
        Instruction::EmitResultQuery { parameters, .. } => parameters.clone(),
        Instruction::AppendAudit {
            command_fingerprint,
            metadata,
            ..
        } => vec![*command_fingerprint, *metadata],
        Instruction::AppendOutbox {
            idempotency_key,
            schema_version,
            payload,
        } => vec![*idempotency_key, *schema_version, *payload],
        Instruction::Call { arguments, .. } => arguments.clone(),
    }
}

fn terminator_reads(terminator: &Terminator) -> Vec<SlotId> {
    match terminator {
        Terminator::Branch { condition, .. } => vec![*condition],
        Terminator::Return(Some(value)) => vec![*value],
        Terminator::Jump(_)
        | Terminator::Return(None)
        | Terminator::Raise(_)
        | Terminator::Rethrow => Vec::new(),
    }
}

pub(super) fn reachable_blocks(entry: usize, successors: &[Vec<usize>]) -> Vec<bool> {
    let mut reachable = vec![false; successors.len()];
    let mut queue = VecDeque::from([entry]);
    while let Some(block) = queue.pop_front() {
        if std::mem::replace(&mut reachable[block], true) {
            continue;
        }
        queue.extend(successors[block].iter().copied());
    }
    reachable
}

pub(super) fn terminator_targets(terminator: &Terminator) -> Vec<BlockId> {
    match terminator {
        Terminator::Jump(target) => vec![*target],
        Terminator::Branch {
            when_true,
            when_false,
            ..
        } => vec![*when_true, *when_false],
        Terminator::Return(_) | Terminator::Raise(_) | Terminator::Rethrow => Vec::new(),
    }
}

pub(super) fn instruction_targets(instruction: &Instruction) -> Vec<BlockId> {
    match instruction {
        Instruction::EnterExceptionRegion { routes } => {
            routes.iter().map(|route| route.handler).collect()
        }
        _ => Vec::new(),
    }
}

fn invalid(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message)
}
