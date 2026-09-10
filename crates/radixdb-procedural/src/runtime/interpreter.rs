use radixdb_core::{DataType, Value};

use super::cursor::{assign_cursor_row, close_all_cursors, close_all_cursors_checked, CursorState};
use super::exception::{
    abort_all_exception_frames, leave_exception_frame, release_all_exception_frames,
    route_exception, ExceptionFrame,
};
use super::frame::{scalar, Frame};
use super::record::{make_record, read_record_field, write_record_field};
use super::scalar::{collection_index, integer_add_checked, integer_subtract_checked};
use super::sinks::{charge_result_row, ForwardResultSink, IntoSink, RejectResultRows};
use super::state::{ExecutionOutcome, ExecutionState, SqlStatus};
use crate::host::{RuntimeHost, SqlRowSink};
use crate::ir::{
    CursorStatusAttribute, Instruction, SqlStatusAttribute, Terminator, VerifiedProgram,
};
use crate::{
    BudgetOwner, Diagnostic, DiagnosticFrame, DiagnosticKind, ProceduralResult, RuntimeType,
    RuntimeValue,
};

use super::{
    application::{append_audit, append_outbox},
    attach_span, invalid_runtime,
};

mod sql_text;

use sql_text::{concatenate_sql_text, quote_sql_identifier};

#[derive(Debug, Default)]
pub struct Interpreter;

impl Interpreter {
    pub fn execute<H: RuntimeHost>(
        &self,
        program: &VerifiedProgram,
        arguments: Vec<RuntimeValue>,
        host: &mut H,
        budget: &BudgetOwner,
    ) -> ProceduralResult<ExecutionOutcome> {
        let mut sink = RejectResultRows;
        self.execute_with_result_sink(program, arguments, host, budget, &mut sink)
    }

    pub fn execute_with_result_sink<H: RuntimeHost>(
        &self,
        program: &VerifiedProgram,
        arguments: Vec<RuntimeValue>,
        host: &mut H,
        budget: &BudgetOwner,
        result_sink: &mut dyn SqlRowSink,
    ) -> ProceduralResult<ExecutionOutcome> {
        let identity = program.program().identity();
        self.execute_inner(program, arguments, host, budget, result_sink)
            .map_err(|error| {
                error.with_outer_frame(DiagnosticFrame {
                    object_id: identity.object_id(),
                    definition_revision: identity.definition_revision(),
                    name: identity.name().to_owned(),
                    call_span: None,
                })
            })
    }

    fn execute_inner<H: RuntimeHost>(
        &self,
        verified: &VerifiedProgram,
        arguments: Vec<RuntimeValue>,
        host: &mut H,
        budget: &BudgetOwner,
        result_sink: &mut dyn SqlRowSink,
    ) -> ProceduralResult<ExecutionOutcome> {
        let program = verified.program();
        if arguments.len() != program.parameter_slots().len() {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "runtime argument count differs from verified parameter slots",
            ));
        }
        let _frame_lease = budget.enter_frame()?;
        let mut frame = Frame::new(program.slots(), budget.clone());
        frame.assign_many(program.parameter_slots(), arguments, None)?;
        let mut state = ExecutionState::new(program.result_columns());
        let mut exception_frames = Vec::new();
        budget.check_boundary()?;

        let outcome = self.execute_frame(
            program,
            &mut frame,
            host,
            budget,
            result_sink,
            &mut state,
            &mut exception_frames,
        );
        if let Err(error) = &outcome {
            let cleanup = abort_all_exception_frames(host, &mut exception_frames, error);
            close_all_cursors(host, &mut state.cursors);
            cleanup?;
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_frame<H: RuntimeHost>(
        &self,
        program: &crate::Program,
        frame: &mut Frame,
        host: &mut H,
        budget: &BudgetOwner,
        result_sink: &mut dyn SqlRowSink,
        state: &mut ExecutionState<'_>,
        exception_frames: &mut Vec<ExceptionFrame>,
    ) -> ProceduralResult<ExecutionOutcome> {
        let mut current = program.entry();
        'execution: loop {
            let block = program.blocks().get(current.0 as usize).ok_or_else(|| {
                Diagnostic::new(
                    DiagnosticKind::RuntimeInvalidIr,
                    "verified block disappeared at runtime",
                )
            })?;
            for spanned in block.instructions() {
                budget.charge_instructions(1)?;
                let result = match spanned.instruction() {
                    Instruction::EnterExceptionRegion { routes } => {
                        host.create_savepoint().map(|savepoint| {
                            exception_frames.push(ExceptionFrame {
                                savepoint,
                                routes: routes.clone(),
                                cursors_at_entry: state.cursors.keys().copied().collect(),
                                sql_status_at_entry: state.sql_status,
                                handling: None,
                            });
                        })
                    }
                    Instruction::LeaveExceptionRegion => {
                        leave_exception_frame(host, exception_frames)
                    }
                    instruction => self.execute_instruction(
                        instruction,
                        frame,
                        host,
                        budget,
                        state,
                        result_sink,
                    ),
                };
                if let Err(error) = result {
                    let error = attach_span(error, spanned.span());
                    match route_exception(
                        error,
                        frame,
                        host,
                        &mut state.cursors,
                        &mut state.sql_status,
                        exception_frames,
                    ) {
                        Ok(target) => {
                            current = target;
                            continue 'execution;
                        }
                        Err(error) => {
                            close_all_cursors(host, &mut state.cursors);
                            return Err(error);
                        }
                    }
                }
            }

            budget.charge_instructions(1)?;
            match block.terminator().terminator() {
                Terminator::Jump(target) => {
                    budget.check_boundary()?;
                    current = *target;
                }
                Terminator::Branch {
                    condition,
                    when_true,
                    when_false,
                } => {
                    let condition = scalar(frame, *condition)?;
                    let taken = matches!(condition, Value::Boolean(true));
                    budget.check_boundary()?;
                    current = if taken { *when_true } else { *when_false };
                }
                Terminator::Return(value) => {
                    budget.check_boundary()?;
                    let return_value = value.map(|slot| frame.read(slot).cloned()).transpose()?;
                    if let Some(value) = &return_value {
                        budget.charge_result_bytes(value.owned_bytes())?;
                    }
                    let output_values = program
                        .output_slots()
                        .iter()
                        .map(|slot| frame.read(*slot).cloned())
                        .collect::<ProceduralResult<Vec<_>>>()?;
                    for value in &output_values {
                        budget.charge_result_bytes(value.owned_bytes())?;
                    }
                    release_all_exception_frames(host, exception_frames)?;
                    close_all_cursors_checked(host, &mut state.cursors)?;
                    return Ok(ExecutionOutcome {
                        return_value,
                        output_values,
                        result_rows: state.result_rows,
                        sql_status: state.sql_status,
                    });
                }
                Terminator::Raise(kind) => {
                    let error = attach_span(
                        Diagnostic::new(*kind, "procedural RAISE"),
                        block.terminator().span(),
                    );
                    match route_exception(
                        error,
                        frame,
                        host,
                        &mut state.cursors,
                        &mut state.sql_status,
                        exception_frames,
                    ) {
                        Ok(target) => current = target,
                        Err(error) => {
                            close_all_cursors(host, &mut state.cursors);
                            return Err(error);
                        }
                    }
                }
                Terminator::Rethrow => {
                    let error = exception_frames
                        .last()
                        .and_then(|frame| frame.handling.clone())
                        .ok_or_else(|| invalid_runtime("RETHROW has no active exception"))?;
                    match route_exception(
                        error,
                        frame,
                        host,
                        &mut state.cursors,
                        &mut state.sql_status,
                        exception_frames,
                    ) {
                        Ok(target) => current = target,
                        Err(error) => {
                            close_all_cursors(host, &mut state.cursors);
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    fn execute_instruction<H: RuntimeHost>(
        &self,
        instruction: &Instruction,
        frame: &mut Frame,
        host: &mut H,
        budget: &BudgetOwner,
        state: &mut ExecutionState<'_>,
        result_sink: &mut dyn SqlRowSink,
    ) -> ProceduralResult<()> {
        match instruction {
            Instruction::InitializeNull { destination } => {
                let value = frame.slot_type(*destination)?.null_value()?;
                frame.assign(*destination, value, None)
            }
            Instruction::LoadConstant { destination, value } => {
                frame.assign(*destination, value.clone(), None)
            }
            Instruction::EvaluateExpression {
                expression,
                parameters,
                destination,
            } => {
                budget.check_boundary()?;
                let parameters = parameters
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let value = host.evaluate_expression(expression, &parameters, budget)?;
                frame.assign(*destination, value, None)?;
                budget.check_boundary()
            }
            Instruction::Copy {
                destination,
                source,
            } => frame.assign(*destination, frame.read(*source)?.clone(), None),
            Instruction::IntegerAddChecked {
                destination,
                left,
                right,
            } => frame.assign(
                *destination,
                integer_add_checked(frame.read(*left)?, frame.read(*right)?)?,
                None,
            ),
            Instruction::IntegerSubtractChecked {
                destination,
                left,
                right,
            } => frame.assign(
                *destination,
                integer_subtract_checked(frame.read(*left)?, frame.read(*right)?)?,
                None,
            ),
            Instruction::IntegerLess {
                destination,
                left,
                right,
            } => {
                let left = scalar(frame, *left)?;
                let right = scalar(frame, *right)?;
                let value = match (left, right) {
                    (Value::Null(_), _) | (_, Value::Null(_)) => Value::null(DataType::Boolean),
                    (Value::Integer(left), Value::Integer(right)) => Value::Boolean(left < right),
                    _ => return Err(invalid_runtime("verified INTEGER operand changed type")),
                };
                frame.assign(*destination, RuntimeValue::Scalar(value), None)
            }
            Instruction::EvaluateSqlBinary {
                destination,
                left,
                right,
                operator,
            } => {
                budget.check_boundary()?;
                let value = host.evaluate_binary(
                    *operator,
                    frame.read(*left)?,
                    frame.read(*right)?,
                    budget,
                )?;
                frame.assign(*destination, value, None)?;
                budget.check_boundary()
            }
            Instruction::BooleanNot {
                destination,
                source,
            } => {
                let value = match scalar(frame, *source)? {
                    Value::Null(_) => Value::null(DataType::Boolean),
                    Value::Boolean(value) => Value::Boolean(!value),
                    _ => return Err(invalid_runtime("verified BOOLEAN operand changed type")),
                };
                frame.assign(*destination, RuntimeValue::Scalar(value), None)
            }
            Instruction::QuoteSqlIdentifier {
                destination,
                source,
            } => quote_sql_identifier(frame, *destination, *source),
            Instruction::ConcatenateSqlText {
                destination,
                left,
                right,
            } => concatenate_sql_text(frame, *destination, *left, *right),
            Instruction::MakeRecord {
                destination,
                fields,
            } => make_record(frame, *destination, fields),
            Instruction::ReadRecordField {
                destination,
                record,
                field,
            } => read_record_field(frame, *destination, *record, *field),
            Instruction::WriteRecordField {
                record,
                field,
                value,
            } => write_record_field(frame, *record, *field, *value),
            Instruction::CollectionAppend { collection, value } => {
                let value = scalar(frame, *value)?.clone();
                let capacity = match frame.slot_type(*collection)? {
                    RuntimeType::Collection { capacity, .. } => *capacity as usize,
                    _ => return Err(invalid_runtime("verified collection slot changed type")),
                };
                let RuntimeValue::Collection(current) = frame.read(*collection)? else {
                    return Err(invalid_runtime("verified collection value changed shape"));
                };
                if current.len() >= capacity {
                    return Err(Diagnostic::new(
                        DiagnosticKind::RuntimeArrayBounds,
                        "collection capacity exceeded",
                    ));
                }
                let mut replacement = current.clone();
                replacement.try_reserve_exact(1).map_err(|_| {
                    Diagnostic::new(DiagnosticKind::ResourceHeap, "collection allocation failed")
                })?;
                replacement.push(value);
                frame.assign(*collection, RuntimeValue::Collection(replacement), None)
            }
            Instruction::CollectionClear { collection } => {
                frame.assign(*collection, RuntimeValue::Collection(Vec::new()), None)
            }
            Instruction::CollectionGet {
                destination,
                collection,
                one_based_index,
            } => {
                let index = match scalar(frame, *one_based_index)? {
                    Value::Integer(index) if *index > 0 => (*index as usize) - 1,
                    _ => {
                        return Err(Diagnostic::new(
                            DiagnosticKind::RuntimeArrayBounds,
                            "collection index must be a positive INTEGER",
                        ));
                    }
                };
                let RuntimeValue::Collection(values) = frame.read(*collection)? else {
                    return Err(invalid_runtime("verified collection value changed shape"));
                };
                let value = values.get(index).ok_or_else(|| {
                    Diagnostic::new(
                        DiagnosticKind::RuntimeArrayBounds,
                        "collection index exceeds COUNT",
                    )
                })?;
                frame.assign(*destination, RuntimeValue::Scalar(value.clone()), None)
            }
            Instruction::CollectionSet {
                collection,
                one_based_index,
                value,
            } => {
                let index = collection_index(frame, *one_based_index)?;
                let value = scalar(frame, *value)?.clone();
                let RuntimeValue::Collection(values) = frame.read(*collection)? else {
                    return Err(invalid_runtime("verified collection value changed shape"));
                };
                if index >= values.len() {
                    return Err(Diagnostic::new(
                        DiagnosticKind::RuntimeArrayBounds,
                        "collection index exceeds COUNT",
                    ));
                }
                let mut changed = values.clone();
                changed[index] = value;
                frame.assign(*collection, RuntimeValue::Collection(changed), None)
            }
            Instruction::CollectionCount {
                destination,
                collection,
            } => {
                let RuntimeValue::Collection(values) = frame.read(*collection)? else {
                    return Err(invalid_runtime("verified collection value changed shape"));
                };
                let count = i64::try_from(values.len())
                    .map_err(|_| invalid_runtime("verified collection COUNT exceeds INTEGER"))?;
                frame.assign(
                    *destination,
                    RuntimeValue::scalar(Value::Integer(count)),
                    None,
                )
            }
            Instruction::ReadSqlStatus {
                destination,
                attribute,
            } => {
                let value = match attribute {
                    SqlStatusAttribute::RowCount => Value::Integer(
                        i64::try_from(state.sql_status.row_count)
                            .map_err(|_| invalid_runtime("SQL ROWCOUNT exceeds INTEGER"))?,
                    ),
                    SqlStatusAttribute::Found => Value::Boolean(state.sql_status.found),
                    SqlStatusAttribute::NotFound => Value::Boolean(!state.sql_status.found),
                };
                frame.assign(*destination, RuntimeValue::scalar(value), None)
            }
            Instruction::ReadCursorStatus {
                destination,
                cursor,
                attribute,
            } => {
                let cursor_state = state.cursors.get(cursor);
                let value = match attribute {
                    CursorStatusAttribute::IsOpen => Value::Boolean(cursor_state.is_some()),
                    CursorStatusAttribute::RowCount => Value::Integer(
                        i64::try_from(cursor_state.map_or(0, |state| state.row_count))
                            .map_err(|_| invalid_runtime("cursor ROWCOUNT exceeds INTEGER"))?,
                    ),
                    CursorStatusAttribute::Found => cursor_state
                        .and_then(|state| state.found)
                        .map_or_else(|| Value::null(DataType::Boolean), Value::Boolean),
                    CursorStatusAttribute::NotFound => {
                        cursor_state.and_then(|state| state.found).map_or_else(
                            || Value::null(DataType::Boolean),
                            |found| Value::Boolean(!found),
                        )
                    }
                };
                frame.assign(*destination, RuntimeValue::scalar(value), None)
            }
            Instruction::ExecuteSql {
                statement,
                parameters,
                into,
                strict,
            } => {
                budget.check_boundary()?;
                budget.charge_sql_statement()?;
                let parameters = parameters
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let mut sink = IntoSink::new(!into.is_empty(), budget.clone());
                let outcome = host.execute_sql(statement, &parameters, &mut sink, budget)?;
                if sink.row_count == 0 {
                    budget.charge_rows(outcome.affected_rows)?;
                }
                state.sql_status = SqlStatus {
                    row_count: outcome.affected_rows.max(sink.row_count),
                    found: outcome.affected_rows > 0 || sink.row_count > 0,
                };
                if !into.is_empty() {
                    let values = match sink.first_row {
                        Some(values) => values,
                        None if *strict => {
                            return Err(Diagnostic::new(
                                DiagnosticKind::CardinalityNoDataFound,
                                "STRICT SQL INTO produced no rows",
                            ));
                        }
                        None => into
                            .iter()
                            .map(|slot| frame.slot_type(*slot)?.null_value())
                            .collect::<ProceduralResult<Vec<_>>>()?,
                    };
                    if values.len() != into.len() {
                        return Err(invalid_runtime(
                            "SQL row width differs from verified INTO destinations",
                        ));
                    }
                    frame.assign_many(into, values, None)?;
                }
                budget.check_boundary()
            }
            Instruction::ExecuteDynamicSql {
                source,
                parameters,
                into,
                strict,
            } => {
                budget.check_boundary()?;
                budget.charge_sql_statement()?;
                let source = match scalar(frame, *source)? {
                    Value::Text(source) => source.clone(),
                    Value::Null(_) => {
                        return Err(Diagnostic::new(
                            DiagnosticKind::RuntimeNullNotAllowed,
                            "dynamic SQL source cannot be NULL",
                        ));
                    }
                    _ => return Err(invalid_runtime("dynamic SQL source is not TEXT")),
                };
                let parameters = parameters
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let mut sink = IntoSink::new(!into.is_empty(), budget.clone());
                let outcome = host.execute_dynamic_sql(&source, &parameters, &mut sink, budget)?;
                if sink.row_count == 0 {
                    budget.charge_rows(outcome.affected_rows)?;
                }
                state.sql_status = SqlStatus {
                    row_count: outcome.affected_rows.max(sink.row_count),
                    found: outcome.affected_rows > 0 || sink.row_count > 0,
                };
                if !into.is_empty() {
                    let values = match sink.first_row {
                        Some(values) => values,
                        None if *strict => {
                            return Err(Diagnostic::new(
                                DiagnosticKind::CardinalityNoDataFound,
                                "STRICT dynamic SQL INTO produced no rows",
                            ));
                        }
                        None => into
                            .iter()
                            .map(|slot| frame.slot_type(*slot)?.null_value())
                            .collect::<ProceduralResult<Vec<_>>>()?,
                    };
                    if values.len() != into.len() {
                        return Err(invalid_runtime(
                            "dynamic SQL row width differs from INTO destinations",
                        ));
                    }
                    frame.assign_many(into, values, None)?;
                }
                budget.check_boundary()
            }
            Instruction::OpenCursor {
                cursor,
                statement,
                parameters,
            } => {
                if state.cursors.contains_key(cursor) {
                    return Err(invalid_runtime("cursor is already open"));
                }
                budget.check_boundary()?;
                budget.charge_sql_statement()?;
                let parameters = parameters
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let token = host.open_cursor(statement, &parameters, budget)?;
                state.cursors.insert(
                    *cursor,
                    CursorState {
                        token,
                        found: None,
                        row_count: 0,
                    },
                );
                budget.check_boundary()
            }
            Instruction::FetchCursor {
                cursor,
                into,
                found,
            } => {
                let token = state
                    .cursors
                    .get(cursor)
                    .ok_or_else(|| invalid_runtime("cursor is not open"))?;
                budget.check_boundary()?;
                let row = host.fetch_cursor(token.token, budget)?;
                let found_value = RuntimeValue::scalar(Value::Boolean(row.is_some()));
                frame.assign(*found, found_value, None)?;
                if let Some(row) = row {
                    budget.charge_rows(1)?;
                    budget.charge_result_bytes(row.iter().fold(0u64, |total, value| {
                        total.saturating_add(value.owned_bytes())
                    }))?;
                    assign_cursor_row(frame, into, row)?;
                    state.sql_status = SqlStatus {
                        row_count: 1,
                        found: true,
                    };
                    if let Some(cursor_state) = state.cursors.get_mut(cursor) {
                        cursor_state.found = Some(true);
                        cursor_state.row_count = cursor_state.row_count.saturating_add(1);
                    }
                } else {
                    state.sql_status = SqlStatus::default();
                    if let Some(cursor_state) = state.cursors.get_mut(cursor) {
                        cursor_state.found = Some(false);
                    }
                }
                budget.check_boundary()
            }
            Instruction::CloseCursor { cursor } => {
                let token = state
                    .cursors
                    .remove(cursor)
                    .ok_or_else(|| invalid_runtime("cursor is not open"))?;
                host.close_cursor(token.token)
            }
            Instruction::EmitResultRow { values } => {
                let row = values
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                charge_result_row(budget, &row)?;
                result_sink.push_row(row)?;
                state.result_rows = state.result_rows.saturating_add(1);
                Ok(())
            }
            Instruction::EmitResultQuery {
                statement,
                parameters,
            } => {
                budget.check_boundary()?;
                budget.charge_sql_statement()?;
                let parameters = parameters
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let mut sink = ForwardResultSink::new(
                    result_sink,
                    budget,
                    &mut state.result_rows,
                    state.result_columns,
                );
                let outcome = host.execute_sql(statement, &parameters, &mut sink, budget)?;
                let produced_rows = sink.row_count();
                if produced_rows == 0 {
                    budget.charge_rows(outcome.affected_rows)?;
                }
                state.sql_status = SqlStatus {
                    row_count: outcome.affected_rows.max(produced_rows),
                    found: outcome.affected_rows > 0 || produced_rows > 0,
                };
                budget.check_boundary()
            }
            Instruction::AppendAudit {
                object_id,
                command_fingerprint,
                metadata,
            } => append_audit(
                host,
                frame,
                budget,
                *object_id,
                *command_fingerprint,
                *metadata,
            ),
            Instruction::AppendOutbox {
                idempotency_key,
                schema_version,
                payload,
            } => append_outbox(
                host,
                frame,
                budget,
                *idempotency_key,
                *schema_version,
                *payload,
            ),
            Instruction::EnterExceptionRegion { .. } | Instruction::LeaveExceptionRegion => {
                unreachable!("exception region instructions are handled by the execution loop")
            }
            Instruction::Call {
                routine,
                arguments,
                results,
            } => {
                budget.check_boundary()?;
                let arguments = arguments
                    .iter()
                    .map(|slot| frame.read(*slot).cloned())
                    .collect::<ProceduralResult<Vec<_>>>()?;
                let values = host.call_routine(*routine, &arguments, budget)?;
                if values.len() != results.len() {
                    return Err(invalid_runtime(
                        "nested routine result width differs from verified destinations",
                    ));
                }
                frame.assign_many(results, values, None)?;
                budget.check_boundary()
            }
        }
    }
}
