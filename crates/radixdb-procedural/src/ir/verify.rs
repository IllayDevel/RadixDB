use std::collections::BTreeSet;

use radixdb_core::DataType;
use radixdb_sql::Statement;

use super::admission::verify_sql_statement;
use super::flow::{
    instruction_targets, reachable_blocks, terminator_targets, verify_definite_initialization,
};
use super::validation::{
    block_index, instruction_operand_nodes, invalid, limit, require_same_type,
    require_scalar_operands, require_unique_slots, scalar, slot,
};
use super::{BasicBlock, Instruction, Program, Terminator};
use crate::{ProceduralResult, RuntimeType};

mod types;

use types::{verify_concatenate_sql_text, verify_quote_sql_identifier};

pub const MAX_STATIC_IR_INSTRUCTIONS: usize = 1_000_000;
pub const MAX_STATIC_IR_NODES: usize = 1_000_000;

#[derive(Debug, Clone)]
pub struct VerifiedProgram(Program);

impl VerifiedProgram {
    pub const fn program(&self) -> &Program {
        &self.0
    }
}

pub fn verify(program: Program) -> ProceduralResult<VerifiedProgram> {
    if program.identity().definition_revision() == 0
        || program.identity().name().is_empty()
        || program.identity().name().len() > 4096
    {
        return Err(invalid(None, "IR program identity is invalid"));
    }
    if program.blocks().is_empty() {
        return Err(invalid(None, "IR program has no basic blocks"));
    }
    for definition in program.slots() {
        definition.runtime_type().validate()?;
    }
    if let Some(result_type) = program.result_type() {
        result_type.validate()?;
    }
    for result_column in program.result_columns() {
        let RuntimeType::Scalar { .. } = result_column else {
            return Err(invalid(None, "IR result column is not scalar"));
        };
        result_column.validate()?;
    }
    if program.result_type().is_some() && !program.result_columns().is_empty() {
        return Err(invalid(
            None,
            "IR program cannot mix scalar and table result contracts",
        ));
    }
    let entry = block_index(&program, program.entry(), None)?;
    let instruction_count = program
        .blocks()
        .iter()
        .try_fold(0usize, |total, block| {
            total.checked_add(block.instructions().len())
        })
        .ok_or_else(|| invalid(None, "IR instruction count overflow"))?;
    if instruction_count > MAX_STATIC_IR_INSTRUCTIONS {
        return Err(limit(
            "IR instructions",
            instruction_count,
            MAX_STATIC_IR_INSTRUCTIONS,
        ));
    }
    let nested_node_count = program
        .slots()
        .iter()
        .try_fold(0usize, |total, definition| {
            total.checked_add(match definition.runtime_type() {
                RuntimeType::Record { fields, .. } => fields.len(),
                RuntimeType::Scalar { .. }
                | RuntimeType::Collection { .. }
                | RuntimeType::SqlIdentifier => 0,
            })
        })
        .and_then(|total| {
            program.blocks().iter().try_fold(total, |total, block| {
                block
                    .instructions()
                    .iter()
                    .try_fold(total, |total, instruction| {
                        total.checked_add(instruction_operand_nodes(instruction.instruction()))
                    })
            })
        })
        .ok_or_else(|| invalid(None, "IR nested node count overflow"))?;
    let node_count = program
        .slots()
        .len()
        .checked_add(program.blocks().len())
        .and_then(|count| count.checked_add(instruction_count))
        .and_then(|count| count.checked_add(nested_node_count))
        .ok_or_else(|| invalid(None, "IR node count overflow"))?;
    if node_count > MAX_STATIC_IR_NODES {
        return Err(limit("IR nodes", node_count, MAX_STATIC_IR_NODES));
    }

    let mut parameters = BTreeSet::new();
    for parameter in program.parameter_slots() {
        slot(&program, *parameter, None)?;
        if !parameters.insert(*parameter) {
            return Err(invalid(None, "IR parameter slot is repeated"));
        }
    }
    let mut outputs = BTreeSet::new();
    for output in program.output_slots() {
        slot(&program, *output, None)?;
        if !outputs.insert(*output) {
            return Err(invalid(None, "IR output slot is repeated"));
        }
    }

    let mut successors = vec![Vec::new(); program.blocks().len()];
    let mut predecessors = vec![Vec::new(); program.blocks().len()];
    for (index, block) in program.blocks().iter().enumerate() {
        verify_block_types(&program, block)?;
        for instruction in block.instructions() {
            for target in instruction_targets(instruction.instruction()) {
                let target = block_index(&program, target, instruction.span())?;
                successors[index].push(target);
                predecessors[target].push(index);
            }
        }
        for target in terminator_targets(block.terminator().terminator()) {
            let target = block_index(&program, target, block.terminator().span())?;
            successors[index].push(target);
            predecessors[target].push(index);
        }
    }

    let reachable = reachable_blocks(entry, &successors);
    if reachable.iter().any(|reachable| !reachable) {
        return Err(invalid(None, "IR contains an unreachable basic block"));
    }
    verify_definite_initialization(&program, entry, &predecessors)?;
    Ok(VerifiedProgram(program))
}

fn verify_block_types(program: &Program, block: &BasicBlock) -> ProceduralResult<()> {
    for instruction in block.instructions() {
        let span = instruction.span();
        match instruction.instruction() {
            Instruction::InitializeNull { destination } => {
                let destination = slot(program, *destination, span)?;
                match destination.runtime_type() {
                    RuntimeType::Scalar { nullable: true, .. }
                    | RuntimeType::Record { .. }
                    | RuntimeType::Collection { .. } => {}
                    RuntimeType::SqlIdentifier => {
                        return Err(invalid(
                            span,
                            "SQL identifier fragments cannot be NULL-initialized",
                        ));
                    }
                    RuntimeType::Scalar {
                        nullable: false, ..
                    } => {
                        return Err(invalid(
                            span,
                            "NULL initialization requires a nullable destination",
                        ));
                    }
                }
            }
            Instruction::LoadConstant { destination, value } => {
                let destination = slot(program, *destination, span)?;
                if !destination.runtime_type().accepts(value) {
                    return Err(invalid(
                        span,
                        "constant does not match its destination slot",
                    ));
                }
            }
            Instruction::EvaluateExpression {
                parameters,
                destination,
                ..
            } => {
                for parameter in parameters {
                    let RuntimeType::Scalar { .. } =
                        slot(program, *parameter, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "SQL expression parameter is not scalar"));
                    };
                }
                let RuntimeType::Scalar { .. } = slot(program, *destination, span)?.runtime_type()
                else {
                    return Err(invalid(span, "SQL expression destination is not scalar"));
                };
            }
            Instruction::Copy {
                destination,
                source,
            } => require_same_type(program, *destination, *source, span)?,
            Instruction::IntegerAddChecked {
                destination,
                left,
                right,
            }
            | Instruction::IntegerSubtractChecked {
                destination,
                left,
                right,
            } => {
                let (left_type, left_nullable) = scalar(program, *left, DataType::Integer, span)?;
                let (right_type, right_nullable) =
                    scalar(program, *right, DataType::Integer, span)?;
                let (destination_type, destination_nullable) =
                    scalar(program, *destination, DataType::Integer, span)?;
                if left_type != right_type
                    || left_type != destination_type
                    || ((left_nullable || right_nullable) && !destination_nullable)
                {
                    return Err(invalid(
                        span,
                        "integer addition type contract is inconsistent",
                    ));
                }
            }
            Instruction::IntegerLess {
                destination,
                left,
                right,
            } => {
                let (left_type, left_nullable) = scalar(program, *left, DataType::Integer, span)?;
                let (right_type, right_nullable) =
                    scalar(program, *right, DataType::Integer, span)?;
                let (_, destination_nullable) =
                    scalar(program, *destination, DataType::Boolean, span)?;
                if left_type != right_type
                    || ((left_nullable || right_nullable) && !destination_nullable)
                {
                    return Err(invalid(
                        span,
                        "integer comparison type contract is inconsistent",
                    ));
                }
            }
            Instruction::EvaluateSqlBinary {
                destination,
                left,
                right,
                operator,
            } => {
                let RuntimeType::Scalar {
                    data_type: left_type,
                    nullable: left_nullable,
                } = slot(program, *left, span)?.runtime_type()
                else {
                    return Err(invalid(span, "SQL equality left operand is not scalar"));
                };
                let RuntimeType::Scalar {
                    data_type: right_type,
                    nullable: right_nullable,
                } = slot(program, *right, span)?.runtime_type()
                else {
                    return Err(invalid(span, "SQL equality right operand is not scalar"));
                };
                let RuntimeType::Scalar {
                    data_type: destination_type,
                    nullable: destination_nullable,
                } = slot(program, *destination, span)?.runtime_type()
                else {
                    return Err(invalid(span, "SQL binary destination is not scalar"));
                };
                if matches!(
                    operator,
                    radixdb_sql::InfixOperator::Equal
                        | radixdb_sql::InfixOperator::NotEqual
                        | radixdb_sql::InfixOperator::LessThan
                        | radixdb_sql::InfixOperator::LessEqual
                        | radixdb_sql::InfixOperator::GreaterThan
                        | radixdb_sql::InfixOperator::GreaterEqual
                ) {
                    let boolean = radixdb_catalog::CatalogDataType::scalar(DataType::Boolean)
                        .map_err(|_| invalid(span, "BOOLEAN catalog type is unavailable"))?;
                    if left_type != right_type || destination_type != &boolean {
                        return Err(invalid(
                            span,
                            "SQL comparison type contract is inconsistent",
                        ));
                    }
                } else if matches!(
                    operator,
                    radixdb_sql::InfixOperator::Add
                        | radixdb_sql::InfixOperator::Subtract
                        | radixdb_sql::InfixOperator::Multiply
                        | radixdb_sql::InfixOperator::Divide
                        | radixdb_sql::InfixOperator::Modulo
                        | radixdb_sql::InfixOperator::Concat
                        | radixdb_sql::InfixOperator::BitwiseAnd
                        | radixdb_sql::InfixOperator::BitwiseOr
                        | radixdb_sql::InfixOperator::BitwiseXor
                        | radixdb_sql::InfixOperator::LeftShift
                        | radixdb_sql::InfixOperator::RightShift
                ) {
                    if left_type != right_type || destination_type != left_type {
                        return Err(invalid(span, "SQL binary type contract is inconsistent"));
                    }
                } else {
                    return Err(invalid(span, "SQL binary operator is not admitted"));
                }
                if (*left_nullable || *right_nullable) && !destination_nullable {
                    return Err(invalid(
                        span,
                        "nullable SQL operands require a nullable result",
                    ));
                }
            }
            Instruction::BooleanNot {
                destination,
                source,
            } => {
                let (_, source_nullable) = scalar(program, *source, DataType::Boolean, span)?;
                let (_, destination_nullable) =
                    scalar(program, *destination, DataType::Boolean, span)?;
                if source_nullable && !destination_nullable {
                    return Err(invalid(
                        span,
                        "nullable boolean NOT requires a nullable result",
                    ));
                }
            }
            Instruction::QuoteSqlIdentifier {
                destination,
                source,
            } => verify_quote_sql_identifier(program, *destination, *source, span)?,
            Instruction::ConcatenateSqlText {
                destination,
                left,
                right,
            } => verify_concatenate_sql_text(program, *destination, *left, *right, span)?,
            Instruction::MakeRecord {
                destination,
                fields,
            } => {
                let RuntimeType::Record {
                    fields: record_fields,
                    ..
                } = slot(program, *destination, span)?.runtime_type()
                else {
                    return Err(invalid(
                        span,
                        "record construction destination is not a record",
                    ));
                };
                if record_fields.len() != fields.len() {
                    return Err(invalid(span, "record construction field count differs"));
                }
                for (source, field) in fields.iter().zip(record_fields) {
                    let RuntimeType::Scalar {
                        data_type,
                        nullable,
                    } = slot(program, *source, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "record field source is not scalar"));
                    };
                    if *data_type != field.data_type() || (*nullable && !field.nullable()) {
                        return Err(invalid(span, "record field source type differs"));
                    }
                }
            }
            Instruction::ReadRecordField {
                destination,
                record,
                field,
            } => {
                let RuntimeType::Record { fields, .. } =
                    slot(program, *record, span)?.runtime_type()
                else {
                    return Err(invalid(span, "record-field source is not a record"));
                };
                let field = fields
                    .get(*field as usize)
                    .ok_or_else(|| invalid(span, "record-field ordinal is out of bounds"))?;
                let RuntimeType::Scalar {
                    data_type,
                    nullable,
                } = slot(program, *destination, span)?.runtime_type()
                else {
                    return Err(invalid(span, "record-field destination is not scalar"));
                };
                if *data_type != field.data_type() || (field.nullable() && !nullable) {
                    return Err(invalid(span, "record-field destination type differs"));
                }
            }
            Instruction::WriteRecordField {
                record,
                field,
                value,
            } => {
                let RuntimeType::Record { fields, .. } =
                    slot(program, *record, span)?.runtime_type()
                else {
                    return Err(invalid(span, "record-field target is not a record"));
                };
                let field = fields
                    .get(*field as usize)
                    .ok_or_else(|| invalid(span, "record-field ordinal is out of bounds"))?;
                let RuntimeType::Scalar {
                    data_type,
                    nullable,
                } = slot(program, *value, span)?.runtime_type()
                else {
                    return Err(invalid(span, "record-field value is not scalar"));
                };
                if *data_type != field.data_type() || (*nullable && !field.nullable()) {
                    return Err(invalid(span, "record-field value type differs"));
                }
            }
            Instruction::CollectionAppend { collection, value } => {
                let RuntimeType::Collection { element_type, .. } =
                    slot(program, *collection, span)?.runtime_type()
                else {
                    return Err(invalid(span, "APPEND target is not a collection"));
                };
                let RuntimeType::Scalar {
                    data_type,
                    nullable: _,
                } = slot(program, *value, span)?.runtime_type()
                else {
                    return Err(invalid(span, "APPEND value is not scalar"));
                };
                if data_type != element_type {
                    return Err(invalid(
                        span,
                        "APPEND value type differs from collection element",
                    ));
                }
            }
            Instruction::CollectionClear { collection } => {
                if !matches!(
                    slot(program, *collection, span)?.runtime_type(),
                    RuntimeType::Collection { .. }
                ) {
                    return Err(invalid(span, "CLEAR target is not a collection"));
                }
            }
            Instruction::CollectionGet {
                destination,
                collection,
                one_based_index,
            } => {
                let RuntimeType::Collection { element_type, .. } =
                    slot(program, *collection, span)?.runtime_type()
                else {
                    return Err(invalid(span, "indexed source is not a collection"));
                };
                scalar(program, *one_based_index, DataType::Integer, span)?;
                if slot(program, *destination, span)?.runtime_type()
                    != &RuntimeType::scalar(*element_type, true)
                {
                    return Err(invalid(
                        span,
                        "collection read destination must be a nullable element slot",
                    ));
                }
            }
            Instruction::CollectionSet {
                collection,
                one_based_index,
                value,
            } => {
                let RuntimeType::Collection { element_type, .. } =
                    slot(program, *collection, span)?.runtime_type()
                else {
                    return Err(invalid(span, "indexed target is not a collection"));
                };
                scalar(program, *one_based_index, DataType::Integer, span)?;
                let RuntimeType::Scalar { data_type, .. } =
                    slot(program, *value, span)?.runtime_type()
                else {
                    return Err(invalid(span, "indexed assignment value is not scalar"));
                };
                if data_type != element_type {
                    return Err(invalid(
                        span,
                        "indexed assignment value type differs from collection element",
                    ));
                }
            }
            Instruction::CollectionCount {
                destination,
                collection,
            } => {
                if !matches!(
                    slot(program, *collection, span)?.runtime_type(),
                    RuntimeType::Collection { .. }
                ) {
                    return Err(invalid(span, "COUNT source is not a collection"));
                }
                scalar(program, *destination, DataType::Integer, span)?;
            }
            Instruction::ReadSqlStatus {
                destination,
                attribute,
            } => {
                let expected = match attribute {
                    super::SqlStatusAttribute::RowCount => DataType::Integer,
                    super::SqlStatusAttribute::Found | super::SqlStatusAttribute::NotFound => {
                        DataType::Boolean
                    }
                };
                scalar(program, *destination, expected, span)?;
            }
            Instruction::ReadCursorStatus {
                destination,
                attribute,
                ..
            } => {
                let (expected, expected_nullable) = match attribute {
                    super::CursorStatusAttribute::IsOpen => (DataType::Boolean, false),
                    super::CursorStatusAttribute::Found
                    | super::CursorStatusAttribute::NotFound => (DataType::Boolean, true),
                    super::CursorStatusAttribute::RowCount => (DataType::Integer, false),
                };
                let (_, nullable) = scalar(program, *destination, expected, span)?;
                if expected_nullable && !nullable {
                    return Err(invalid(span, "cursor status destination type differs"));
                }
            }
            Instruction::ExecuteSql {
                statement,
                parameters,
                into,
                strict,
            } => {
                verify_sql_statement(statement, span)?;
                for parameter in parameters {
                    slot(program, *parameter, span)?;
                }
                require_unique_slots(into, span, "SQL INTO destination is repeated")?;
                for destination in into {
                    let RuntimeType::Scalar { nullable, .. } =
                        slot(program, *destination, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "SQL INTO destination is not scalar"));
                    };
                    if !strict && !nullable {
                        return Err(invalid(
                            span,
                            "non-STRICT SQL INTO requires nullable destinations for zero rows",
                        ));
                    }
                }
            }
            Instruction::ExecuteDynamicSql {
                source,
                parameters,
                into,
                strict,
            } => {
                scalar(program, *source, DataType::Text, span)?;
                for parameter in parameters {
                    let RuntimeType::Scalar { .. } =
                        slot(program, *parameter, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "dynamic SQL parameter is not scalar"));
                    };
                }
                require_unique_slots(into, span, "dynamic SQL INTO destination is repeated")?;
                for destination in into {
                    let RuntimeType::Scalar { nullable, .. } =
                        slot(program, *destination, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "dynamic SQL INTO destination is not scalar"));
                    };
                    if !strict && !nullable {
                        return Err(invalid(
                            span,
                            "non-STRICT dynamic SQL INTO requires nullable destinations",
                        ));
                    }
                }
            }
            Instruction::OpenCursor {
                statement,
                parameters,
                ..
            } => {
                if !matches!(statement.as_ref(), Statement::Select(_)) {
                    return Err(invalid(span, "cursor query is not a SELECT"));
                }
                for parameter in parameters {
                    let RuntimeType::Scalar { .. } =
                        slot(program, *parameter, span)?.runtime_type()
                    else {
                        return Err(invalid(span, "cursor parameter is not scalar"));
                    };
                }
            }
            Instruction::FetchCursor { into, found, .. } => {
                scalar(program, *found, DataType::Boolean, span)?;
                require_unique_slots(into, span, "cursor FETCH destination is repeated")?;
                for destination in into {
                    slot(program, *destination, span)?;
                }
            }
            Instruction::CloseCursor { .. } => {}
            Instruction::EmitResultRow { values } => {
                if values.len() != program.result_columns().len()
                    || values
                        .iter()
                        .zip(program.result_columns())
                        .any(|(value, expected)| {
                            slot(program, *value, span)
                                .map_or(true, |definition| definition.runtime_type() != expected)
                        })
                {
                    return Err(invalid(
                        span,
                        "RETURN NEXT row differs from the program result contract",
                    ));
                }
            }
            Instruction::EmitResultQuery {
                statement,
                parameters,
            } => {
                if program.result_columns().is_empty()
                    || !matches!(statement.as_ref(), Statement::Select(_))
                {
                    return Err(invalid(
                        span,
                        "RETURN QUERY requires a table result and SELECT",
                    ));
                }
                for parameter in parameters {
                    slot(program, *parameter, span)?;
                }
            }
            Instruction::AppendAudit {
                command_fingerprint,
                metadata,
                ..
            } => {
                require_scalar_operands(
                    program,
                    &[
                        (*command_fingerprint, DataType::Bytes),
                        (*metadata, DataType::Json),
                    ],
                    span,
                )?;
            }
            Instruction::AppendOutbox {
                idempotency_key,
                schema_version,
                payload,
            } => {
                require_scalar_operands(
                    program,
                    &[
                        (*idempotency_key, DataType::Text),
                        (*schema_version, DataType::Integer),
                        (*payload, DataType::Json),
                    ],
                    span,
                )?;
            }
            Instruction::EnterExceptionRegion { routes } => {
                if routes.is_empty() {
                    return Err(invalid(span, "exception region has no handlers"));
                }
                let mut kinds = BTreeSet::new();
                let mut catch_all_seen = false;
                for route in routes {
                    if route.kinds.is_empty() {
                        if catch_all_seen {
                            return Err(invalid(span, "exception region repeats OTHERS"));
                        }
                        catch_all_seen = true;
                    } else {
                        if catch_all_seen {
                            return Err(invalid(span, "exception handler follows OTHERS"));
                        }
                        if route.kinds.iter().any(|kind| !kinds.insert(*kind)) {
                            return Err(invalid(span, "exception kind is handled twice"));
                        }
                    }
                    if let Some(error_slot) = route.error_slot {
                        if !matches!(
                            slot(program, error_slot, span)?.runtime_type(),
                            RuntimeType::Record { .. }
                        ) {
                            return Err(invalid(span, "exception alias slot is not a record"));
                        }
                    }
                }
            }
            Instruction::LeaveExceptionRegion => {}
            Instruction::Call {
                arguments, results, ..
            } => {
                for argument in arguments {
                    slot(program, *argument, span)?;
                }
                require_unique_slots(results, span, "CALL result destination is repeated")?;
                for result in results {
                    slot(program, *result, span)?;
                }
            }
        }
    }

    match block.terminator().terminator() {
        Terminator::Jump(_) | Terminator::Raise(_) | Terminator::Rethrow => {}
        Terminator::Branch { condition, .. } => {
            scalar(
                program,
                *condition,
                DataType::Boolean,
                block.terminator().span(),
            )?;
        }
        Terminator::Return(value) => match (program.result_type(), value) {
            (None, None) => {}
            (Some(expected), Some(value))
                if slot(program, *value, block.terminator().span())?.runtime_type() == expected => {
            }
            _ => {
                return Err(invalid(
                    block.terminator().span(),
                    "RETURN value differs from the program result contract",
                ));
            }
        },
    }
    Ok(())
}
