use radixdb_catalog::{CatalogDataType, CatalogName, ObjectId};
use radixdb_core::DataType;
use radixdb_sql::{parse_sql, Expression, ObjectName, ProceduralType, Statement};

use super::*;
use crate::{
    verify, BoundExpression, BoundResultColumn, BoundRoutineCall, BoundType, CallSiteArgumentValue,
    Diagnostic, RecordField, RuntimeType,
};

#[derive(Default)]
struct Resolver;

impl SemanticResolver for Resolver {
    fn resolve_type(&mut self, syntax: &ProceduralType) -> ProceduralResult<BoundType> {
        let ProceduralType::Scalar(name) = syntax else {
            return Ok(BoundType {
                runtime_type: RuntimeType::record(vec![
                    RecordField::new(
                        CatalogName::new("id").unwrap(),
                        CatalogDataType::scalar(DataType::Integer).unwrap(),
                        false,
                    ),
                    RecordField::new(
                        CatalogName::new("note").unwrap(),
                        CatalogDataType::scalar(DataType::Text).unwrap(),
                        true,
                    ),
                ])?,
                dependencies: vec![object(43)],
            });
        };
        let data_type = match name.to_ascii_lowercase().as_str() {
            "integer" => DataType::Integer,
            "boolean" => DataType::Boolean,
            "text" => DataType::Text,
            "uuid" => DataType::Uuid,
            "bytes" => DataType::Bytes,
            "json" => DataType::Json,
            _ => {
                return Err(Diagnostic::new(
                    DiagnosticKind::BindUnknownObject,
                    "unknown test type",
                ));
            }
        };
        Ok(BoundType::scalar(RuntimeType::scalar(
            CatalogDataType::scalar(data_type).unwrap(),
            true,
        )))
    }

    fn bind_expression(
        &mut self,
        expression: &Expression,
        locals: &[LocalBinding],
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<BoundExpression> {
        let inferred = expected
            .cloned()
            .or_else(|| match expression {
                Expression::Identifier(identifier) => locals
                    .iter()
                    .find(|local| local.name == identifier.value_lower())
                    .map(|local| local.runtime_type.clone()),
                Expression::IntegerLiteral(_) => Some(scalar(DataType::Integer, false)),
                Expression::BooleanLiteral(_) => Some(scalar(DataType::Boolean, false)),
                Expression::StringLiteral(_) => Some(scalar(DataType::Text, false)),
                _ => None,
            })
            .ok_or_else(|| {
                Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    "test resolver cannot infer expression",
                )
            })?;
        Ok(BoundExpression {
            expression: expression.clone(),
            parameters: Vec::new(),
            result_type: inferred,
            dependencies: Vec::new(),
        })
    }

    fn bind_binary_operator(
        &mut self,
        operator: InfixOperator,
        left: &RuntimeType,
        right: &RuntimeType,
        expected: Option<&RuntimeType>,
    ) -> ProceduralResult<RuntimeType> {
        let (
            RuntimeType::Scalar {
                data_type: left_type,
                nullable: left_nullable,
            },
            RuntimeType::Scalar {
                data_type: right_type,
                nullable: right_nullable,
            },
        ) = (left, right)
        else {
            return Err(Diagnostic::new(
                DiagnosticKind::BindTypeMismatch,
                "test SQL binder expects scalar operands",
            ));
        };
        if left_type != right_type {
            return Err(Diagnostic::new(
                DiagnosticKind::BindTypeMismatch,
                "test SQL binder requires equal operand types",
            ));
        }
        let nullable = *left_nullable || *right_nullable;
        let result = match operator {
            InfixOperator::Equal
            | InfixOperator::NotEqual
            | InfixOperator::LessThan
            | InfixOperator::LessEqual
            | InfixOperator::GreaterThan
            | InfixOperator::GreaterEqual => scalar(DataType::Boolean, nullable),
            InfixOperator::Add
            | InfixOperator::Subtract
            | InfixOperator::Multiply
            | InfixOperator::Divide
            | InfixOperator::Modulo
            | InfixOperator::Concat
            | InfixOperator::BitwiseAnd
            | InfixOperator::BitwiseOr
            | InfixOperator::BitwiseXor
            | InfixOperator::LeftShift
            | InfixOperator::RightShift => RuntimeType::scalar(*left_type, nullable),
            _ => {
                return Err(Diagnostic::new(
                    DiagnosticKind::ParseUnsupportedSyntax,
                    "test SQL binder rejected binary operator",
                ));
            }
        };
        if let Some(expected) = expected {
            let compatible = match (expected, &result) {
                (
                    RuntimeType::Scalar {
                        data_type: expected_type,
                        nullable: expected_nullable,
                    },
                    RuntimeType::Scalar {
                        data_type: actual_type,
                        nullable: actual_nullable,
                    },
                ) => expected_type == actual_type && (*expected_nullable || !actual_nullable),
                _ => expected == &result,
            };
            if !compatible {
                return Err(Diagnostic::new(
                    DiagnosticKind::BindTypeMismatch,
                    "test SQL binder result differs from expected type",
                ));
            }
        }
        Ok(result)
    }

    fn bind_statement(
        &mut self,
        statement: &Statement,
        _locals: &[LocalBinding],
    ) -> ProceduralResult<BoundSqlStatement> {
        Ok(BoundSqlStatement {
            statement: statement.clone(),
            parameters: Vec::new(),
            result_columns: vec![BoundResultColumn::new(
                CatalogName::new("id").unwrap(),
                scalar(DataType::Integer, true),
            )],
            dependencies: vec![object(41)],
        })
    }

    fn bind_procedure_call(
        &mut self,
        routine: &ObjectName,
        arguments: &[CallSiteArgument],
    ) -> ProceduralResult<BoundRoutineCall> {
        if routine.to_string() == "app.write_output" {
            let CallSiteArgumentValue::Bound { slot: input, .. } = arguments[0].value else {
                panic!("test call input must be bound")
            };
            let CallSiteArgumentValue::Bound { slot: output, .. } = arguments[1].value else {
                panic!("test call output must be bound")
            };
            return Ok(BoundRoutineCall {
                routine: object(42),
                arguments: vec![BoundCallArgument::Provided {
                    declared_name: "input_value".to_owned(),
                    slot: input,
                }],
                results: vec![output],
                dependencies: Vec::new(),
                cost: 0,
            });
        }
        Ok(BoundRoutineCall {
            routine: object(42),
            arguments: arguments
                .iter()
                .enumerate()
                .map(|(index, argument)| {
                    let CallSiteArgumentValue::Bound { slot, .. } = argument.value else {
                        panic!("test call argument must be bound")
                    };
                    BoundCallArgument::Provided {
                        declared_name: format!("argument_{index}"),
                        slot,
                    }
                })
                .collect(),
            results: Vec::new(),
            dependencies: Vec::new(),
            cost: 0,
        })
    }
}

#[test]
fn out_and_inout_arguments_are_explicit_program_outputs_and_call_destinations() {
    let compiled = compile(
        "CREATE PROCEDURE app.outputs( \
             IN input_value INTEGER NOT NULL, \
             OUT output_value INTEGER NOT NULL, \
             INOUT running_value INTEGER NOT NULL \
         ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
             CALL app.write_output(input_value, output_value); \
             running_value := running_value + output_value; \
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    assert_eq!(verified.program().parameter_slots().len(), 2);
    assert_eq!(verified.program().output_slots().len(), 2);
    let call = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .find_map(|instruction| match instruction.instruction() {
            Instruction::Call {
                arguments, results, ..
            } => Some((arguments, results)),
            _ => None,
        })
        .unwrap();
    assert_eq!(call.0.len(), 1);
    assert_eq!(call.1, &verified.program().output_slots()[..1]);
}

fn object(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn scalar(data_type: DataType, nullable: bool) -> RuntimeType {
    RuntimeType::scalar(CatalogDataType::scalar(data_type).unwrap(), nullable)
}

fn routine(source: &str) -> radixdb_sql::CreateRoutineStatement {
    let statements = parse_sql(source).unwrap();
    let [Statement::CreateRoutine(routine)] = statements.as_slice() else {
        panic!("expected routine")
    };
    routine.as_ref().clone()
}

fn compile(source: &str) -> ProceduralResult<CompiledRoutine> {
    let syntax = routine(source);
    compile_routine(
        &syntax,
        CompileIdentity {
            object_id: object(40),
            definition_revision: 7,
            display_name: "app.test".to_owned(),
        },
        &mut Resolver,
    )
}

#[test]
fn system_audit_and_outbox_calls_lower_to_typed_host_instructions() {
    let compiled = compile(
        "CREATE PROCEDURE app.publish( \
             fingerprint BYTES, metadata JSON, key_value TEXT, version_value INTEGER, payload JSON \
         ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
             CALL system.append_audit(fingerprint, metadata); \
             CALL system.append_outbox(key_value, version_value, payload); \
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    let instructions = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .map(|instruction| instruction.instruction())
        .collect::<Vec<_>>();
    assert!(instructions.iter().any(|instruction| matches!(
        instruction,
        Instruction::AppendAudit { object_id, .. } if *object_id == object(40)
    )));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::AppendOutbox { .. })));
    assert!(!instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::Call { .. })));
}

fn trigger_context(
    old_available: bool,
    new_available: bool,
    new_writable: bool,
    return_record: Option<TriggerReturnRecord>,
) -> TriggerCompileContext {
    TriggerCompileContext {
        record_fields: vec![
            RecordField::new(
                CatalogName::new("id").unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            ),
            RecordField::new(
                CatalogName::new("version").unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            ),
        ],
        old_available,
        new_available,
        new_writable,
        return_record,
    }
}

#[test]
fn before_update_trigger_has_typed_mutable_new_and_exact_return_contract() {
    let syntax = routine(
        "CREATE FUNCTION app.touch() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN \
         NEW.version := OLD.version + 1; RETURN NEW; END;",
    );
    let compiled = compile_trigger_routine(
        &syntax,
        CompileIdentity {
            object_id: object(44),
            definition_revision: 1,
            display_name: "app.touch".to_owned(),
        },
        &mut Resolver,
        &trigger_context(true, true, true, Some(TriggerReturnRecord::New)),
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    assert_eq!(verified.program().parameter_slots().len(), 5);
    assert!(matches!(
        verified.program().result_type(),
        Some(RuntimeType::Record { nullable: true, .. })
    ));
}

#[test]
fn after_trigger_rejects_row_mutation_and_non_null_return() {
    let mutable = routine(
        "CREATE FUNCTION app.after_touch() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN NEW.version := 2; RETURN NULL; END;",
    );
    let context = trigger_context(true, true, false, None);
    let error = compile_trigger_routine(
        &mutable,
        CompileIdentity {
            object_id: object(45),
            definition_revision: 1,
            display_name: "app.after_touch".to_owned(),
        },
        &mut Resolver,
        &context,
    )
    .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::VerifyCapabilityDenied);

    let wrong_return = routine(
        "CREATE FUNCTION app.after_return() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN RETURN NEW; END;",
    );
    let error = compile_trigger_routine(
        &wrong_return,
        CompileIdentity {
            object_id: object(46),
            definition_revision: 1,
            display_name: "app.after_return".to_owned(),
        },
        &mut Resolver,
        &context,
    )
    .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::TriggerInvalidReturn);
}

#[test]
fn statement_trigger_accepts_only_nullable_trigger_return() {
    let syntax = routine(
        "CREATE FUNCTION app.statement_touch() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN RETURN NULL; END;",
    );
    let compiled = compile_trigger_routine(
        &syntax,
        CompileIdentity {
            object_id: object(47),
            definition_revision: 1,
            display_name: "app.statement_touch".to_owned(),
        },
        &mut Resolver,
        &trigger_context(false, false, false, None),
    )
    .unwrap();
    verify(compiled.program).unwrap();
}

#[test]
fn scalar_function_lowers_to_verified_cfg_with_source_spans() {
    let compiled = compile(
        "CREATE FUNCTION app.increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
         LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
         BEGIN RETURN value + 1; END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    assert_eq!(verified.program().parameter_slots().len(), 1);
    assert!(verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .all(|instruction| instruction.span().is_some()));
}

#[test]
fn control_flow_static_dynamic_sql_and_calls_share_one_ir() {
    let compiled = compile(
        "CREATE PROCEDURE app.exercise(value INTEGER) LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE current INTEGER := 0; found INTEGER;
         BEGIN
           IF value > 0 THEN current := value; ELSE current := 1; END IF;
           WHILE current < 3 LOOP current := current + 1; END LOOP;
           FOR item IN 1 TO 2 LOOP CONTINUE WHEN item = 1; END LOOP;
           SELECT id INTO found FROM route WHERE id = :current;
           EXECUTE 'DELETE FROM route WHERE id = ?' USING current;
           CALL app.record_value(current);
         END;",
    )
    .unwrap();
    assert_eq!(compiled.dependencies, vec![object(41), object(42)]);
    verify(compiled.program).unwrap();
}

#[test]
fn explicit_and_query_cursors_lower_to_bounded_streaming_ir() {
    let compiled = compile(
        "CREATE PROCEDURE app.scan(limit_value INTEGER) LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE
           CURSOR routes(max_id INTEGER) FOR SELECT id FROM route WHERE id < :max_id;
           found_id INTEGER;
         BEGIN
           OPEN routes(limit_value);
           FETCH routes INTO found_id;
           CLOSE routes;
           FOR route_row IN (SELECT id FROM route LIMIT 10) LOOP
             PERFORM route_row.id;
             EXIT;
           END LOOP;
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    let instructions = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .map(|instruction| instruction.instruction())
        .collect::<Vec<_>>();
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::OpenCursor { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::FetchCursor { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::CloseCursor { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::ReadRecordField { .. })));
}

#[test]
fn rowtype_keeps_catalog_shape_and_supports_field_and_row_assignment() {
    let compiled = compile(
        "CREATE FUNCTION app.row_copy() RETURNS INTEGER NOT NULL \
         LANGUAGE RADIX STABLE SECURITY INVOKER AS
         DECLARE
           source_row erp.route_document%ROWTYPE;
           target_row erp.route_document%ROWTYPE;
         BEGIN
           source_row.id := 7;
           source_row.note := 'ready';
           target_row := source_row;
           RETURN target_row.id;
         END;",
    )
    .unwrap();
    assert!(compiled.dependencies.contains(&object(43)));
    let verified = verify(compiled.program).unwrap();
    let instructions = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .map(|instruction| instruction.instruction())
        .collect::<Vec<_>>();
    assert_eq!(
        instructions
            .iter()
            .filter(|instruction| matches!(instruction, Instruction::WriteRecordField { .. }))
            .count(),
        2
    );
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::Copy { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::ReadRecordField { .. })));
}

#[test]
fn rowtype_rejects_unknown_fields_and_whole_row_not_null() {
    for source in [
        "CREATE PROCEDURE app.bad() LANGUAGE RADIX SECURITY INVOKER AS \
         DECLARE row_value erp.route_document%ROWTYPE; \
         BEGIN row_value.missing := 1; END;",
        "CREATE PROCEDURE app.bad() LANGUAGE RADIX SECURITY INVOKER AS \
         DECLARE row_value erp.route_document%ROWTYPE NOT NULL; \
         BEGIN RETURN; END;",
    ] {
        let error = compile(source).unwrap_err();
        assert!(matches!(
            error.kind(),
            DiagnosticKind::BindUnknownLocal | DiagnosticKind::BindTypeMismatch
        ));
        assert!(error.primary_span().is_some());
    }
}

#[test]
fn numeric_for_lowers_reverse_and_explicit_positive_step() {
    let compiled = compile(
        "CREATE PROCEDURE app.walk() LANGUAGE RADIX SECURITY INVOKER AS
         BEGIN
           FOR item IN REVERSE 5 TO 1 BY 2 LOOP PERFORM item; END LOOP;
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    let instructions = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .map(|instruction| instruction.instruction())
        .collect::<Vec<_>>();
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::IntegerSubtractChecked { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::IntegerLess { .. })));
    assert!(verified.program().blocks().iter().any(|block| matches!(
        block.terminator().terminator(),
        Terminator::Raise(DiagnosticKind::RuntimeInvalidArgument)
    )));
}

#[test]
fn admission_rejects_deferred_or_unsafe_shapes() {
    for (source, kind) in [
        (
            "CREATE FUNCTION app.f() RETURNS INTEGER LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN PERFORM 1; END;",
            DiagnosticKind::BindTypeMismatch,
        ),
        (
            "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS DECLARE value CONSTANT INTEGER := 1; BEGIN value := 2; END;",
            DiagnosticKind::VerifyCapabilityDenied,
        ),
        (
            "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS BEGIN BEGIN PERFORM 1; EXCEPTION WHEN OTHERS THEN RETURN; END; END;",
            DiagnosticKind::VerifyCapabilityDenied,
        ),
    ] {
        let error = compile(source).unwrap_err();
        assert_eq!(error.kind(), kind, "{error}");
        assert!(error.primary_span().is_some());
    }
}

#[test]
fn exception_handlers_lower_to_typed_savepoint_routes() {
    let compiled = compile(
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE found_id INTEGER;
         BEGIN
           BEGIN
             SELECT id INTO STRICT found_id FROM route;
           EXCEPTION
             WHEN no_data_found OR too_many_rows THEN RAISE conflict('cardinality');
             WHEN OTHERS AS caught_error THEN RAISE;
           END;
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    assert!(verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .any(|instruction| matches!(
            instruction.instruction(),
            Instruction::EnterExceptionRegion { .. }
        )));
}

#[test]
fn case_collections_status_and_table_returns_lower_to_one_verified_ir() {
    let compiled = compile(
        "CREATE PROCEDURE app.surface() RETURNS TABLE (value INTEGER) \
         LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE
           items ARRAY<INTEGER, 8>;
           current INTEGER := 1;
           found_id INTEGER;
           CURSOR routes(max_id INTEGER) FOR SELECT id FROM route WHERE id < :max_id;
         BEGIN
           items.APPEND(current);
           items[1] := current;
           current := items[1];
           CASE current
             WHEN 1 THEN current := items.COUNT;
             ELSE current := 2;
           END CASE;
           SELECT id INTO found_id FROM route WHERE id = :current;
           IF SQL%ROWCOUNT = 1 THEN current := found_id; END IF;
           IF SQL%FOUND THEN current := current; END IF;
           OPEN routes(current);
           FETCH routes INTO found_id;
           IF routes%ISOPEN THEN current := routes%ROWCOUNT; END IF;
           IF routes%FOUND THEN current := found_id; END IF;
           IF routes%NOTFOUND THEN current := 0; END IF;
           CLOSE routes;
           RETURN NEXT (current);
           RETURN QUERY SELECT id FROM route;
         END;",
    )
    .unwrap();
    let verified = verify(compiled.program).unwrap();
    assert_eq!(
        verified.program().result_columns(),
        &[scalar(DataType::Integer, true)]
    );
    let instructions = verified
        .program()
        .blocks()
        .iter()
        .flat_map(|block| block.instructions())
        .map(|instruction| instruction.instruction())
        .collect::<Vec<_>>();
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::EvaluateSqlBinary { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::CollectionSet { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::CollectionGet { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::CollectionCount { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::ReadSqlStatus { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::ReadCursorStatus { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::EmitResultRow { .. })));
    assert!(instructions
        .iter()
        .any(|instruction| matches!(instruction, Instruction::EmitResultQuery { .. })));
}
