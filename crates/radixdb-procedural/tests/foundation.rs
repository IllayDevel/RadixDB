use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use radixdb_catalog::{CatalogDataType, CatalogName, ObjectId, ResourcePolicy};
use radixdb_core::{DataType, Value};
use radixdb_procedural::{
    verify, AuditEvent, AuditHost, BasicBlock, BlockId, BudgetOwner, CursorHost, CursorId,
    CursorStatusAttribute, CursorToken, DiagnosticKind, ExceptionRoute, ExecutionOutcome,
    Instruction, Interpreter, OutboxHost, OutboxMessage, PrincipalContext, PrincipalHost,
    ProceduralResult, Program, ProgramIdentity, RecordField, RoutineCallHost, RuntimeType,
    RuntimeValue, SavepointToken, SlotDefinition, SlotId, SourceSpan, SpannedInstruction,
    SpannedTerminator, SqlHost, SqlOutcome, SqlRowSink, SqlStatusAttribute, Terminator,
    TransactionHost, VerifiedProgram,
};
use radixdb_sql::{parse_sql, InfixOperator, Statement};

#[derive(Default)]
struct TestHost {
    sql_rows: Vec<Vec<RuntimeValue>>,
    cursor_rows: Vec<Vec<RuntimeValue>>,
    affected_rows: u64,
    routines: BTreeMap<ObjectId, VerifiedProgram>,
    savepoints_created: u64,
    savepoints_rolled_back: u64,
    savepoints_released: u64,
    audit_events: Vec<AuditEvent>,
    outbox_messages: Vec<OutboxMessage>,
    cursors_closed: u64,
}

impl SqlHost for TestHost {
    fn evaluate_expression(
        &mut self,
        expression: &radixdb_sql::Expression,
        _parameters: &[RuntimeValue],
        _budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue> {
        match expression {
            radixdb_sql::Expression::IntegerLiteral(value) => {
                Ok(RuntimeValue::scalar(Value::Integer(value.value)))
            }
            _ => Err(radixdb_procedural::Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "test expression host admits integer literals only",
            )),
        }
    }

    fn evaluate_binary(
        &mut self,
        operator: InfixOperator,
        left: &RuntimeValue,
        right: &RuntimeValue,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue> {
        let (RuntimeValue::Scalar(left), RuntimeValue::Scalar(right)) = (left, right) else {
            return Err(radixdb_procedural::Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "test binary host expects scalar operands",
            ));
        };
        let value = match operator {
            InfixOperator::Equal if left.is_null() || right.is_null() => {
                Value::null(DataType::Boolean)
            }
            InfixOperator::Equal => Value::Boolean(left == right),
            _ => {
                return Err(radixdb_procedural::Diagnostic::new(
                    DiagnosticKind::VerifyCapabilityDenied,
                    "test binary host admits equality only",
                ));
            }
        };
        Ok(RuntimeValue::scalar(value))
    }

    fn execute_sql(
        &mut self,
        _statement: &Statement,
        _parameters: &[RuntimeValue],
        rows: &mut dyn SqlRowSink,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<SqlOutcome> {
        for row in self.sql_rows.clone() {
            rows.push_row(row)?;
        }
        Ok(SqlOutcome {
            affected_rows: self.affected_rows,
        })
    }
}

#[derive(Default)]
struct CollectRows(Vec<Vec<RuntimeValue>>);

impl SqlRowSink for CollectRows {
    fn push_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        self.0.push(row);
        Ok(())
    }
}

impl CursorHost for TestHost {
    fn open_cursor(
        &mut self,
        _statement: &Statement,
        _parameters: &[RuntimeValue],
        _budget: &BudgetOwner,
    ) -> ProceduralResult<CursorToken> {
        Ok(CursorToken(1))
    }

    fn fetch_cursor(
        &mut self,
        _cursor: CursorToken,
        _budget: &BudgetOwner,
    ) -> ProceduralResult<Option<Vec<RuntimeValue>>> {
        if self.cursor_rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.cursor_rows.remove(0)))
        }
    }

    fn close_cursor(&mut self, _cursor: CursorToken) -> ProceduralResult<()> {
        self.cursors_closed += 1;
        Ok(())
    }
}

impl TransactionHost for TestHost {
    fn create_savepoint(&mut self) -> ProceduralResult<SavepointToken> {
        self.savepoints_created += 1;
        Ok(SavepointToken(self.savepoints_created))
    }

    fn rollback_savepoint(&mut self, _savepoint: SavepointToken) -> ProceduralResult<()> {
        self.savepoints_rolled_back += 1;
        Ok(())
    }

    fn release_savepoint(&mut self, _savepoint: SavepointToken) -> ProceduralResult<()> {
        self.savepoints_released += 1;
        Ok(())
    }
}

impl PrincipalHost for TestHost {
    fn principal_context(&self) -> PrincipalContext {
        PrincipalContext {
            session_principal: ObjectId::BOOTSTRAP_OWNER,
            invoker_principal: ObjectId::BOOTSTRAP_OWNER,
            effective_principal: ObjectId::BOOTSTRAP_OWNER,
        }
    }

    fn push_definer(&mut self, _owner: ObjectId) -> ProceduralResult<()> {
        Ok(())
    }

    fn pop_definer(&mut self) -> ProceduralResult<()> {
        Ok(())
    }
}

impl RoutineCallHost for TestHost {
    fn call_routine(
        &mut self,
        routine: ObjectId,
        arguments: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<Vec<RuntimeValue>> {
        let program = self.routines.remove(&routine).expect("registered routine");
        let result = Interpreter.execute(&program, arguments.to_vec(), self, budget);
        self.routines.insert(routine, program);
        Ok(result?.return_value.into_iter().collect())
    }
}

impl AuditHost for TestHost {
    fn append_audit(&mut self, event: AuditEvent) -> ProceduralResult<()> {
        self.audit_events.push(event);
        Ok(())
    }
}

impl OutboxHost for TestHost {
    fn append_outbox(&mut self, message: OutboxMessage) -> ProceduralResult<()> {
        self.outbox_messages.push(message);
        Ok(())
    }
}

fn object(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn scalar(data_type: DataType, nullable: bool) -> RuntimeType {
    RuntimeType::scalar(CatalogDataType::scalar(data_type).unwrap(), nullable)
}

fn slot(name: &str, runtime_type: RuntimeType) -> SlotDefinition {
    SlotDefinition::new(name, runtime_type)
}

fn instruction(instruction: Instruction) -> SpannedInstruction {
    SpannedInstruction::unspanned(instruction)
}

fn block(instructions: Vec<Instruction>, terminator: Terminator) -> BasicBlock {
    BasicBlock::new(
        instructions.into_iter().map(instruction).collect(),
        SpannedTerminator::unspanned(terminator),
    )
}

fn program(
    marker: u8,
    name: &str,
    slots: Vec<SlotDefinition>,
    result_type: Option<RuntimeType>,
    blocks: Vec<BasicBlock>,
) -> Program {
    Program::new(
        ProgramIdentity::new(object(marker), 1, name),
        slots,
        Vec::new(),
        result_type,
        blocks,
        BlockId(0),
    )
}

fn execute(program: Program, policy: ResourcePolicy) -> ProceduralResult<ExecutionOutcome> {
    let program = verify(program)?;
    Interpreter.execute(
        &program,
        Vec::new(),
        &mut TestHost::default(),
        &BudgetOwner::new(policy)?,
    )
}

fn statement(sql: &str) -> Box<Statement> {
    let mut statements = parse_sql(sql).unwrap();
    assert_eq!(statements.len(), 1);
    Box::new(statements.remove(0))
}

#[test]
fn audit_and_outbox_instructions_cross_only_the_typed_host_boundary() {
    let candidate = program(
        31,
        "publish_side_effects",
        vec![
            slot("fingerprint", scalar(DataType::Bytes, false)),
            slot("metadata", scalar(DataType::Json, false)),
            slot("key", scalar(DataType::Text, false)),
            slot("version", scalar(DataType::Integer, false)),
            slot("payload", scalar(DataType::Json, false)),
        ],
        None,
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::bytes(vec![9; 32])),
                },
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::json(r#"{"operation":"publish"}"#)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(2),
                    value: RuntimeValue::scalar(Value::text("event-31")),
                },
                Instruction::LoadConstant {
                    destination: SlotId(3),
                    value: RuntimeValue::scalar(Value::Integer(1)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(4),
                    value: RuntimeValue::scalar(Value::json(r#"{"id":31}"#)),
                },
                Instruction::AppendAudit {
                    object_id: object(31),
                    command_fingerprint: SlotId(0),
                    metadata: SlotId(1),
                },
                Instruction::AppendOutbox {
                    idempotency_key: SlotId(2),
                    schema_version: SlotId(3),
                    payload: SlotId(4),
                },
            ],
            Terminator::Return(None),
        )],
    );
    let candidate = verify(candidate).unwrap();
    let mut host = TestHost::default();
    Interpreter
        .execute(
            &candidate,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
        )
        .unwrap();
    assert_eq!(host.audit_events.len(), 1);
    assert_eq!(host.audit_events[0].object_id, object(31));
    assert_eq!(host.audit_events[0].command_fingerprint, [9; 32]);
    assert_eq!(host.outbox_messages.len(), 1);
    assert_eq!(host.outbox_messages[0].idempotency_key, "event-31");
    assert_eq!(host.outbox_messages[0].schema_version, 1);
}

#[test]
fn pure_cfg_program_is_deterministic() {
    let integer = scalar(DataType::Integer, false);
    let boolean = scalar(DataType::Boolean, false);
    let make_program = || {
        program(
            7,
            "count_to_one_hundred",
            vec![
                slot("counter", integer.clone()),
                slot("one", integer.clone()),
                slot("limit", integer.clone()),
                slot("condition", boolean.clone()),
            ],
            Some(integer.clone()),
            vec![
                block(
                    vec![
                        Instruction::LoadConstant {
                            destination: SlotId(0),
                            value: RuntimeValue::scalar(Value::Integer(0)),
                        },
                        Instruction::LoadConstant {
                            destination: SlotId(1),
                            value: RuntimeValue::scalar(Value::Integer(1)),
                        },
                        Instruction::LoadConstant {
                            destination: SlotId(2),
                            value: RuntimeValue::scalar(Value::Integer(100)),
                        },
                    ],
                    Terminator::Jump(BlockId(1)),
                ),
                block(
                    vec![Instruction::IntegerLess {
                        destination: SlotId(3),
                        left: SlotId(0),
                        right: SlotId(2),
                    }],
                    Terminator::Branch {
                        condition: SlotId(3),
                        when_true: BlockId(2),
                        when_false: BlockId(3),
                    },
                ),
                block(
                    vec![Instruction::IntegerAddChecked {
                        destination: SlotId(0),
                        left: SlotId(0),
                        right: SlotId(1),
                    }],
                    Terminator::Jump(BlockId(1)),
                ),
                block(Vec::new(), Terminator::Return(Some(SlotId(0)))),
            ],
        )
    };

    let first = execute(make_program(), ResourcePolicy::default_call()).unwrap();
    let second = execute(make_program(), ResourcePolicy::default_call()).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.return_value,
        Some(RuntimeValue::scalar(Value::Integer(100)))
    );
}

#[test]
fn cursor_rows_stream_through_opaque_transaction_bound_tokens() {
    let integer = scalar(DataType::Integer, true);
    let boolean = scalar(DataType::Boolean, false);
    let candidate = program(
        19,
        "cursor_once",
        vec![slot("value", integer.clone()), slot("found", boolean)],
        Some(integer),
        vec![block(
            vec![
                Instruction::OpenCursor {
                    cursor: CursorId(0),
                    statement: statement("SELECT 7"),
                    parameters: Vec::new(),
                },
                Instruction::FetchCursor {
                    cursor: CursorId(0),
                    into: vec![SlotId(0)],
                    found: SlotId(1),
                },
                Instruction::CloseCursor {
                    cursor: CursorId(0),
                },
            ],
            Terminator::Return(Some(SlotId(0))),
        )],
    );
    let candidate = verify(candidate).unwrap();
    let mut host = TestHost {
        cursor_rows: vec![vec![RuntimeValue::scalar(Value::Integer(7))]],
        ..TestHost::default()
    };
    let outcome = Interpreter
        .execute(
            &candidate,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::Integer(7)))
    );
}

#[test]
fn exception_region_rolls_back_then_runs_typed_handler() {
    let integer = scalar(DataType::Integer, true);
    let candidate = program(
        20,
        "catch_no_data",
        vec![slot("value", integer.clone())],
        Some(integer),
        vec![
            block(
                vec![
                    Instruction::EnterExceptionRegion {
                        routes: vec![ExceptionRoute {
                            kinds: vec![DiagnosticKind::CardinalityNoDataFound],
                            handler: BlockId(1),
                            error_slot: None,
                        }],
                    },
                    Instruction::ExecuteSql {
                        statement: statement("SELECT 7"),
                        parameters: Vec::new(),
                        into: vec![SlotId(0)],
                        strict: true,
                    },
                    Instruction::LeaveExceptionRegion,
                ],
                Terminator::Return(Some(SlotId(0))),
            ),
            block(
                vec![
                    Instruction::LoadConstant {
                        destination: SlotId(0),
                        value: RuntimeValue::scalar(Value::Integer(9)),
                    },
                    Instruction::LeaveExceptionRegion,
                ],
                Terminator::Return(Some(SlotId(0))),
            ),
        ],
    );
    let candidate = verify(candidate).unwrap();
    let mut host = TestHost::default();
    let outcome = Interpreter
        .execute(
            &candidate,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::Integer(9)))
    );
    assert_eq!(host.savepoints_created, 1);
    assert_eq!(host.savepoints_rolled_back, 1);
    assert_eq!(host.savepoints_released, 1);
}

#[test]
fn records_collections_and_frame_heap_are_bounded() {
    let integer_catalog = CatalogDataType::scalar(DataType::Integer).unwrap();
    let integer = scalar(DataType::Integer, false);
    let nullable_integer = scalar(DataType::Integer, true);
    let record = RuntimeType::record(vec![
        RecordField::new(CatalogName::new("first").unwrap(), integer_catalog, false),
        RecordField::new(CatalogName::new("second").unwrap(), integer_catalog, false),
    ])
    .unwrap();
    let collection = RuntimeType::collection(integer_catalog, 2).unwrap();
    let candidate = program(
        8,
        "record_and_collection",
        vec![
            slot("first", integer.clone()),
            slot("second", integer.clone()),
            slot("index", integer.clone()),
            slot("record", record),
            slot("extracted", integer.clone()),
            slot("items", collection),
            slot("indexed", nullable_integer),
        ],
        Some(integer),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::Integer(7)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::Integer(9)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(2),
                    value: RuntimeValue::scalar(Value::Integer(1)),
                },
                Instruction::MakeRecord {
                    destination: SlotId(3),
                    fields: vec![SlotId(0), SlotId(1)],
                },
                Instruction::WriteRecordField {
                    record: SlotId(3),
                    field: 1,
                    value: SlotId(0),
                },
                Instruction::ReadRecordField {
                    destination: SlotId(4),
                    record: SlotId(3),
                    field: 1,
                },
                Instruction::CollectionAppend {
                    collection: SlotId(5),
                    value: SlotId(0),
                },
                Instruction::CollectionAppend {
                    collection: SlotId(5),
                    value: SlotId(4),
                },
                Instruction::CollectionGet {
                    destination: SlotId(6),
                    collection: SlotId(5),
                    one_based_index: SlotId(2),
                },
                Instruction::CollectionClear {
                    collection: SlotId(5),
                },
            ],
            Terminator::Return(Some(SlotId(4))),
        )],
    );
    let verified = verify(candidate).unwrap();
    let budget = BudgetOwner::new(ResourcePolicy::default_call()).unwrap();
    let outcome = Interpreter
        .execute(&verified, Vec::new(), &mut TestHost::default(), &budget)
        .unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::Integer(7)))
    );
    assert_eq!(budget.snapshot().heap_bytes, 0);
    assert_eq!(budget.snapshot().live_frames, 0);
}

#[test]
fn collections_status_attributes_and_result_rows_execute_as_verified() {
    let integer_catalog = CatalogDataType::scalar(DataType::Integer).unwrap();
    let collection = RuntimeType::collection(integer_catalog, 4).unwrap();
    let integer = scalar(DataType::Integer, false);
    let nullable_integer = scalar(DataType::Integer, true);
    let boolean = scalar(DataType::Boolean, false);
    let nullable_boolean = scalar(DataType::Boolean, true);
    let result_columns = vec![
        nullable_integer.clone(),
        nullable_integer.clone(),
        nullable_boolean.clone(),
        nullable_integer.clone(),
        nullable_boolean.clone(),
        nullable_integer.clone(),
        nullable_boolean.clone(),
        nullable_boolean.clone(),
        nullable_integer.clone(),
    ];
    let candidate = program(
        23,
        "collection_status_stream",
        vec![
            slot("items", collection),
            slot("index", integer),
            slot("first", nullable_integer.clone()),
            slot("replacement", nullable_integer.clone()),
            slot("indexed", nullable_integer.clone()),
            slot("count", nullable_integer.clone()),
            slot("equal", nullable_boolean.clone()),
            slot("sql_count", nullable_integer.clone()),
            slot("sql_found", nullable_boolean.clone()),
            slot("cursor_value", nullable_integer.clone()),
            slot("fetch_found", boolean),
            slot("cursor_is_open", nullable_boolean.clone()),
            slot("cursor_found", nullable_boolean.clone()),
            slot("cursor_count", nullable_integer),
        ],
        None,
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::Integer(1)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(2),
                    value: RuntimeValue::scalar(Value::Integer(7)),
                },
                Instruction::CollectionAppend {
                    collection: SlotId(0),
                    value: SlotId(2),
                },
                Instruction::LoadConstant {
                    destination: SlotId(3),
                    value: RuntimeValue::scalar(Value::Integer(8)),
                },
                Instruction::CollectionSet {
                    collection: SlotId(0),
                    one_based_index: SlotId(1),
                    value: SlotId(3),
                },
                Instruction::CollectionGet {
                    destination: SlotId(4),
                    collection: SlotId(0),
                    one_based_index: SlotId(1),
                },
                Instruction::CollectionCount {
                    destination: SlotId(5),
                    collection: SlotId(0),
                },
                Instruction::EvaluateSqlBinary {
                    destination: SlotId(6),
                    left: SlotId(4),
                    right: SlotId(3),
                    operator: InfixOperator::Equal,
                },
                Instruction::ExecuteSql {
                    statement: statement("UPDATE routes SET value = 1"),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
                Instruction::ReadSqlStatus {
                    destination: SlotId(7),
                    attribute: SqlStatusAttribute::RowCount,
                },
                Instruction::ReadSqlStatus {
                    destination: SlotId(8),
                    attribute: SqlStatusAttribute::Found,
                },
                Instruction::OpenCursor {
                    cursor: CursorId(0),
                    statement: statement("SELECT value FROM routes"),
                    parameters: Vec::new(),
                },
                Instruction::FetchCursor {
                    cursor: CursorId(0),
                    into: vec![SlotId(9)],
                    found: SlotId(10),
                },
                Instruction::ReadCursorStatus {
                    destination: SlotId(11),
                    cursor: CursorId(0),
                    attribute: CursorStatusAttribute::IsOpen,
                },
                Instruction::ReadCursorStatus {
                    destination: SlotId(12),
                    cursor: CursorId(0),
                    attribute: CursorStatusAttribute::Found,
                },
                Instruction::ReadCursorStatus {
                    destination: SlotId(13),
                    cursor: CursorId(0),
                    attribute: CursorStatusAttribute::RowCount,
                },
                Instruction::EmitResultRow {
                    values: vec![
                        SlotId(4),
                        SlotId(5),
                        SlotId(6),
                        SlotId(7),
                        SlotId(8),
                        SlotId(9),
                        SlotId(11),
                        SlotId(12),
                        SlotId(13),
                    ],
                },
                Instruction::CloseCursor {
                    cursor: CursorId(0),
                },
            ],
            Terminator::Return(None),
        )],
    )
    .with_result_columns(result_columns);
    let verified = verify(candidate).unwrap();
    let mut host = TestHost {
        affected_rows: 3,
        cursor_rows: vec![vec![RuntimeValue::scalar(Value::Integer(11))]],
        ..TestHost::default()
    };
    let mut rows = CollectRows::default();
    let outcome = Interpreter
        .execute_with_result_sink(
            &verified,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
            &mut rows,
        )
        .unwrap();
    assert_eq!(outcome.result_rows, 1);
    assert_eq!(
        rows.0,
        vec![vec![
            RuntimeValue::scalar(Value::Integer(8)),
            RuntimeValue::scalar(Value::Integer(1)),
            RuntimeValue::scalar(Value::Boolean(true)),
            RuntimeValue::scalar(Value::Integer(3)),
            RuntimeValue::scalar(Value::Boolean(true)),
            RuntimeValue::scalar(Value::Integer(11)),
            RuntimeValue::scalar(Value::Boolean(true)),
            RuntimeValue::scalar(Value::Boolean(true)),
            RuntimeValue::scalar(Value::Integer(1)),
        ]]
    );
}

#[test]
fn return_query_streams_rows_and_publishes_affected_state() {
    let nullable_integer = scalar(DataType::Integer, true);
    let candidate = program(
        24,
        "return_query_stream",
        Vec::new(),
        None,
        vec![block(
            vec![Instruction::EmitResultQuery {
                statement: statement("SELECT value FROM routes"),
                parameters: Vec::new(),
            }],
            Terminator::Return(None),
        )],
    )
    .with_result_columns(vec![nullable_integer]);
    let verified = verify(candidate).unwrap();
    let mut host = TestHost {
        sql_rows: vec![
            vec![RuntimeValue::scalar(Value::Integer(5))],
            vec![RuntimeValue::scalar(Value::Integer(6))],
        ],
        ..TestHost::default()
    };
    let mut rows = CollectRows::default();
    let outcome = Interpreter
        .execute_with_result_sink(
            &verified,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
            &mut rows,
        )
        .unwrap();
    assert_eq!(outcome.result_rows, 2);
    assert_eq!(outcome.sql_status.row_count, 2);
    assert!(outcome.sql_status.found);
    assert_eq!(rows.0, host.sql_rows);
}

#[test]
fn malformed_ir_is_rejected_before_execution() {
    let integer = scalar(DataType::Integer, false);
    let invalid_collection = program(
        18,
        "invalid_collection",
        vec![slot(
            "items",
            RuntimeType::Collection {
                element_type: CatalogDataType::scalar(DataType::Integer).unwrap(),
                capacity: 0,
            },
        )],
        None,
        vec![block(Vec::new(), Terminator::Return(None))],
    );
    assert_eq!(
        verify(invalid_collection).unwrap_err().kind(),
        DiagnosticKind::RuntimeInvalidIr
    );

    let invalid_jump = program(
        9,
        "invalid_jump",
        Vec::new(),
        None,
        vec![block(Vec::new(), Terminator::Jump(BlockId(99)))],
    );
    assert_eq!(
        verify(invalid_jump).unwrap_err().kind(),
        DiagnosticKind::RuntimeInvalidIr
    );

    let wrong_constant = program(
        10,
        "wrong_constant",
        vec![slot("integer", integer.clone())],
        Some(integer.clone()),
        vec![block(
            vec![Instruction::LoadConstant {
                destination: SlotId(0),
                value: RuntimeValue::scalar(Value::Boolean(true)),
            }],
            Terminator::Return(Some(SlotId(0))),
        )],
    );
    assert_eq!(
        verify(wrong_constant).unwrap_err().kind(),
        DiagnosticKind::RuntimeInvalidIr
    );

    let uninitialized = program(
        11,
        "uninitialized",
        vec![slot("value", integer.clone())],
        Some(integer),
        vec![block(Vec::new(), Terminator::Return(Some(SlotId(0))))],
    );
    assert_eq!(
        verify(uninitialized).unwrap_err().kind(),
        DiagnosticKind::RuntimeInvalidIr
    );

    let unreachable = program(
        12,
        "unreachable",
        Vec::new(),
        None,
        vec![
            block(Vec::new(), Terminator::Return(None)),
            block(Vec::new(), Terminator::Return(None)),
        ],
    );
    assert_eq!(
        verify(unreachable).unwrap_err().kind(),
        DiagnosticKind::RuntimeInvalidIr
    );

    let transaction_control = program(
        13,
        "transaction_control",
        Vec::new(),
        None,
        vec![block(
            vec![Instruction::ExecuteSql {
                statement: statement("BEGIN"),
                parameters: Vec::new(),
                into: Vec::new(),
                strict: false,
            }],
            Terminator::Return(None),
        )],
    );
    assert_eq!(
        verify(transaction_control).unwrap_err().kind(),
        DiagnosticKind::VerifyTransactionControlForbidden
    );
}

#[test]
fn sql_cardinality_error_keeps_source_span_and_call_frame() {
    let nullable_integer = scalar(DataType::Integer, true);
    let source_span = SourceSpan::new(object(14), 1, 10, 18, 2, 5, 2, 13).unwrap();
    let candidate = program(
        14,
        "strict_lookup",
        vec![slot("result", nullable_integer.clone())],
        Some(nullable_integer),
        vec![BasicBlock::new(
            vec![SpannedInstruction::new(
                Instruction::ExecuteSql {
                    statement: statement("SELECT 1"),
                    parameters: Vec::new(),
                    into: vec![SlotId(0)],
                    strict: true,
                },
                Some(source_span.clone()),
            )],
            SpannedTerminator::unspanned(Terminator::Return(Some(SlotId(0)))),
        )],
    );
    let verified = verify(candidate).unwrap();
    let mut host = TestHost {
        sql_rows: vec![
            vec![RuntimeValue::scalar(Value::Integer(1))],
            vec![RuntimeValue::scalar(Value::Integer(2))],
        ],
        ..TestHost::default()
    };
    let error = Interpreter
        .execute(
            &verified,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(ResourcePolicy::default_call()).unwrap(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::CardinalityTooManyRows);
    assert_eq!(error.primary_span(), Some(&source_span));
    assert_eq!(error.frames().len(), 1);
    assert_eq!(error.frames()[0].name, "strict_lookup");
}

#[test]
fn nested_calls_share_frame_budget_and_build_a_call_stack() {
    let integer = scalar(DataType::Integer, false);
    let child_id = object(16);
    let child = verify(program(
        16,
        "child",
        vec![slot("result", integer.clone())],
        Some(integer.clone()),
        vec![block(
            vec![Instruction::LoadConstant {
                destination: SlotId(0),
                value: RuntimeValue::scalar(Value::Integer(42)),
            }],
            Terminator::Return(Some(SlotId(0))),
        )],
    ))
    .unwrap();
    let parent = verify(program(
        15,
        "parent",
        vec![slot("result", integer.clone())],
        Some(integer),
        vec![block(
            vec![Instruction::Call {
                routine: child_id,
                arguments: Vec::new(),
                results: vec![SlotId(0)],
            }],
            Terminator::Return(Some(SlotId(0))),
        )],
    ))
    .unwrap();
    let mut host = TestHost::default();
    host.routines.insert(child_id, child);
    let mut policy = ResourcePolicy::default_call();
    policy.frames = 1;
    let error = Interpreter
        .execute(
            &parent,
            Vec::new(),
            &mut host,
            &BudgetOwner::new(policy).unwrap(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::ResourceFrames);
    assert_eq!(
        error
            .frames()
            .iter()
            .map(|frame| frame.name.as_str())
            .collect::<Vec<_>>(),
        vec!["parent", "child"]
    );
}

#[test]
fn cancellation_is_observed_on_a_loop_backedge() {
    let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
    let candidate = verify(program(
        17,
        "cancelled_loop",
        vec![
            slot("item", scalar(DataType::Integer, false)),
            slot("items", RuntimeType::collection(integer, 4).unwrap()),
        ],
        None,
        vec![
            block(
                vec![
                    Instruction::LoadConstant {
                        destination: SlotId(0),
                        value: RuntimeValue::scalar(Value::Integer(1)),
                    },
                    Instruction::CollectionAppend {
                        collection: SlotId(1),
                        value: SlotId(0),
                    },
                    Instruction::OpenCursor {
                        cursor: CursorId(0),
                        statement: statement("SELECT 1"),
                        parameters: Vec::new(),
                    },
                ],
                Terminator::Jump(BlockId(1)),
            ),
            block(Vec::new(), Terminator::Jump(BlockId(1))),
        ],
    ))
    .unwrap();
    let budget = BudgetOwner::new(ResourcePolicy::default_call()).unwrap();
    let cancellation = budget.cancellation_handle();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        cancellation.cancel();
    });
    let started = Instant::now();
    let mut host = TestHost::default();
    let error = Interpreter
        .execute(&candidate, Vec::new(), &mut host, &budget)
        .unwrap_err();
    let latency = started.elapsed();
    canceller.join().unwrap();
    assert_eq!(error.kind(), DiagnosticKind::ResourceCancelled);
    assert!(
        latency < Duration::from_millis(250),
        "loop cancellation latency exceeded bound: {latency:?}"
    );
    assert!(budget.snapshot().instructions > 0);
    assert_eq!(budget.snapshot().heap_bytes, 0);
    assert_eq!(budget.snapshot().live_frames, 0);
    assert_eq!(host.cursors_closed, 1);
}

#[test]
fn every_resource_dimension_fails_closed_with_a_stable_kind() {
    let mut policy = ResourcePolicy::default_call();
    policy.instructions = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    budget.charge_instructions(1).unwrap();
    assert_eq!(
        budget.charge_instructions(1).unwrap_err().kind(),
        DiagnosticKind::ResourceInstructions
    );

    let mut policy = ResourcePolicy::default_call();
    policy.heap_bytes = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    assert_eq!(
        budget.charge_heap(2).unwrap_err().kind(),
        DiagnosticKind::ResourceHeap
    );

    let mut policy = ResourcePolicy::default_call();
    policy.sql_statements = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    budget.charge_sql_statement().unwrap();
    assert_eq!(
        budget.charge_sql_statement().unwrap_err().kind(),
        DiagnosticKind::ResourceSqlStatements
    );

    let mut policy = ResourcePolicy::default_call();
    policy.rows = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    assert_eq!(
        budget.charge_rows(2).unwrap_err().kind(),
        DiagnosticKind::ResourceRows
    );

    let mut policy = ResourcePolicy::default_call();
    policy.result_bytes = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    assert_eq!(
        budget.charge_result_bytes(2).unwrap_err().kind(),
        DiagnosticKind::ResourceBytes
    );

    let mut policy = ResourcePolicy::default_call();
    policy.deadline_ms = 1;
    let budget = BudgetOwner::new(policy).unwrap();
    thread::sleep(Duration::from_millis(5));
    assert_eq!(
        budget.check_boundary().unwrap_err().kind(),
        DiagnosticKind::ResourceDeadline
    );
}
