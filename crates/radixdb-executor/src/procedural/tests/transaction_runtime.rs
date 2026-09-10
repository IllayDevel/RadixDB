use super::*;

#[test]
fn published_procedure_streams_bounded_table_results_through_call_stage() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE published_rows(IN input_value INTEGER NOT NULL) \
             RETURNS TABLE (value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 RETURN NEXT (input_value); \
                 RETURN NEXT (input_value + 1); \
             END;",
        )
        .unwrap();
    let procedure = routine_named(&executor, ObjectKind::Procedure, "published_rows").unwrap();
    let mut stage = TestStage::default();
    let outcome = executor
        .execute_procedure(
            procedure.id(),
            vec![RuntimeValue::scalar(Value::Integer(8))],
            &ExecutionContext::new(),
            principals(),
            &mut stage,
        )
        .unwrap();
    assert_eq!(outcome.execution().result_rows, 2);
    assert_eq!(
        stage.published,
        vec![
            vec![RuntimeValue::scalar(Value::Integer(8))],
            vec![RuntimeValue::scalar(Value::Integer(9))],
        ]
    );
}

#[test]
fn auto_transaction_commits_all_embedded_sql() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    let candidate = program(
        40,
        Vec::new(),
        None,
        Vec::new(),
        vec![block(
            vec![
                Instruction::ExecuteSql {
                    statement: statement("INSERT INTO bridge_rows VALUES (1, 10)"),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
                Instruction::ExecuteSql {
                    statement: statement("UPDATE bridge_rows SET value = 11 WHERE id = 1"),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
            ],
            Terminator::Return(None),
        )],
    );
    let mut stage = TestStage::default();
    execute(&executor, &candidate, &mut stage).unwrap();

    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT value FROM bridge_rows WHERE id = 1")
                .unwrap()
        ),
        11
    );
    assert_eq!(stage.publish_count, 1);
    assert!(!executor.has_active_transaction());
}

#[test]
fn failed_call_rolls_back_every_statement() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    let candidate = program(
        41,
        Vec::new(),
        None,
        Vec::new(),
        vec![block(
            vec![Instruction::ExecuteSql {
                statement: statement("INSERT INTO bridge_rows VALUES (1)"),
                parameters: Vec::new(),
                into: Vec::new(),
                strict: false,
            }],
            Terminator::Raise(DiagnosticKind::RuntimeInvalidArgument),
        )],
    );
    let mut stage = TestStage::default();
    assert_eq!(
        execute(&executor, &candidate, &mut stage)
            .unwrap_err()
            .kind(),
        DiagnosticKind::RuntimeInvalidArgument
    );
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        0
    );
    assert_eq!(stage.discard_count, 1);
    assert!(!executor.has_active_transaction());
}

#[test]
fn caller_transaction_survives_failed_call_at_its_savepoint() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute("BEGIN").unwrap();
    executor
        .execute("INSERT INTO bridge_rows VALUES (1)")
        .unwrap();
    let candidate = program(
        42,
        Vec::new(),
        None,
        Vec::new(),
        vec![block(
            vec![Instruction::ExecuteSql {
                statement: statement("INSERT INTO bridge_rows VALUES (2)"),
                parameters: Vec::new(),
                into: Vec::new(),
                strict: false,
            }],
            Terminator::Raise(DiagnosticKind::RuntimeInvalidArgument),
        )],
    );
    let mut stage = TestStage::default();
    execute(&executor, &candidate, &mut stage).unwrap_err();

    assert!(executor.has_active_transaction());
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        1
    );
    executor.execute("ROLLBACK").unwrap();
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        0
    );
}

#[test]
fn exception_savepoint_rolls_back_only_its_region() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute("INSERT INTO bridge_rows VALUES (1)")
        .unwrap();
    let candidate = program(
        43,
        Vec::new(),
        None,
        Vec::new(),
        vec![
            block(
                vec![
                    Instruction::EnterExceptionRegion {
                        routes: vec![ExceptionRoute {
                            kinds: vec![DiagnosticKind::RuntimeUniqueViolation],
                            handler: BlockId(1),
                            error_slot: None,
                        }],
                    },
                    Instruction::ExecuteSql {
                        statement: statement("INSERT INTO bridge_rows VALUES (2)"),
                        parameters: Vec::new(),
                        into: Vec::new(),
                        strict: false,
                    },
                    Instruction::ExecuteSql {
                        statement: statement("INSERT INTO bridge_rows VALUES (1)"),
                        parameters: Vec::new(),
                        into: Vec::new(),
                        strict: false,
                    },
                    Instruction::LeaveExceptionRegion,
                ],
                Terminator::Return(None),
            ),
            block(
                vec![
                    Instruction::LeaveExceptionRegion,
                    Instruction::ExecuteSql {
                        statement: statement("INSERT INTO bridge_rows VALUES (3)"),
                        parameters: Vec::new(),
                        into: Vec::new(),
                        strict: false,
                    },
                ],
                Terminator::Return(None),
            ),
        ],
    );
    execute(&executor, &candidate, &mut TestStage::default()).unwrap();
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        2
    );
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows WHERE id = 2")
                .unwrap()
        ),
        0
    );
}

#[test]
fn cursor_streams_from_the_same_transaction() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    let candidate = program(
        44,
        vec![
            SlotDefinition::new("value", integer(false)),
            SlotDefinition::new(
                "found",
                RuntimeType::scalar(CatalogDataType::scalar(DataType::Boolean).unwrap(), false),
            ),
        ],
        Some(integer(false)),
        Vec::new(),
        vec![block(
            vec![
                Instruction::ExecuteSql {
                    statement: statement("INSERT INTO bridge_rows VALUES (7)"),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
                Instruction::OpenCursor {
                    cursor: CursorId(0),
                    statement: statement("SELECT id FROM bridge_rows WHERE id = 7"),
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
    let outcome = execute(&executor, &candidate, &mut TestStage::default()).unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::Integer(7)))
    );
}

#[test]
fn result_rows_publish_only_after_a_successful_call_boundary() {
    let executor = executor();
    let row_type = integer(false);
    let emit = Instruction::EmitResultRow {
        values: vec![SlotId(0)],
    };
    let make = |marker, terminator| {
        program(
            marker,
            vec![SlotDefinition::new("value", row_type.clone())],
            None,
            vec![row_type.clone()],
            vec![block(
                vec![
                    Instruction::LoadConstant {
                        destination: SlotId(0),
                        value: RuntimeValue::scalar(Value::Integer(9)),
                    },
                    emit.clone(),
                ],
                terminator,
            )],
        )
    };

    let mut failed = TestStage::default();
    execute(
        &executor,
        &make(
            45,
            Terminator::Raise(DiagnosticKind::RuntimeInvalidArgument),
        ),
        &mut failed,
    )
    .unwrap_err();
    assert!(failed.published.is_empty());
    assert!(failed.staged.is_empty());
    assert_eq!(failed.discard_count, 1);

    let mut passed = TestStage::default();
    execute(&executor, &make(46, Terminator::Return(None)), &mut passed).unwrap();
    assert_eq!(
        passed.published,
        vec![vec![RuntimeValue::scalar(Value::Integer(9))]]
    );
    assert_eq!(passed.publish_count, 1);
}

#[test]
fn cancelled_caller_is_rejected_before_opening_a_transaction() {
    let executor = executor();
    let candidate = program(
        47,
        Vec::new(),
        None,
        Vec::new(),
        vec![block(Vec::new(), Terminator::Return(None))],
    );
    let context = ExecutionContext::new();
    context.cancel();
    let mut stage = TestStage::default();
    let error = executor
        .execute_procedural_program(
            &candidate,
            Vec::new(),
            &context,
            principals(),
            ResourcePolicy::default_call(),
            &mut stage,
        )
        .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::ResourceCancelled);
    assert_eq!(stage.begin_count, 0);
    assert!(!executor.has_active_transaction());
}

#[test]
fn dynamic_sql_uses_shared_parser_and_fail_closed_admission() {
    let executor = executor();
    executor
        .execute("CREATE TABLE bridge_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    let dynamic = |marker, source: &str| {
        program(
            marker,
            vec![SlotDefinition::new("source", text(false))],
            None,
            Vec::new(),
            vec![block(
                vec![
                    Instruction::LoadConstant {
                        destination: SlotId(0),
                        value: RuntimeValue::scalar(Value::text(source)),
                    },
                    Instruction::ExecuteDynamicSql {
                        source: SlotId(0),
                        parameters: Vec::new(),
                        into: Vec::new(),
                        strict: false,
                    },
                ],
                Terminator::Return(None),
            )],
        )
    };

    let mut stage = TestStage::default();
    let multi = dynamic(
        48,
        "INSERT INTO bridge_rows VALUES (1); INSERT INTO bridge_rows VALUES (2)",
    );
    assert_eq!(
        execute(&executor, &multi, &mut stage).unwrap_err().kind(),
        DiagnosticKind::ParseUnsupportedSyntax
    );
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        0
    );

    let ddl = dynamic(49, "DROP TABLE bridge_rows");
    assert_eq!(
        execute(&executor, &ddl, &mut TestStage::default())
            .unwrap_err()
            .kind(),
        DiagnosticKind::VerifyDynamicDdlNotSupported
    );
    let transaction = dynamic(50, "COMMIT");
    assert_eq!(
        execute(&executor, &transaction, &mut TestStage::default())
            .unwrap_err()
            .kind(),
        DiagnosticKind::VerifyTransactionControlForbidden
    );

    let using = program(
        52,
        vec![
            SlotDefinition::new("source", text(false)),
            SlotDefinition::new("id", integer(false)),
        ],
        None,
        Vec::new(),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::text("INSERT INTO bridge_rows VALUES ($1)")),
                },
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::Integer(7)),
                },
                Instruction::ExecuteDynamicSql {
                    source: SlotId(0),
                    parameters: vec![SlotId(1)],
                    into: Vec::new(),
                    strict: false,
                },
            ],
            Terminator::Return(None),
        )],
    );
    execute(&executor, &using, &mut TestStage::default()).unwrap();
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM bridge_rows")
                .unwrap()
        ),
        1
    );
}

#[test]
fn dynamic_sql_using_into_injection_and_statement_budget_share_one_boundary() {
    let executor = executor();
    executor
        .execute("CREATE TABLE dynamic_rows (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let attack = "literal'); DELETE FROM dynamic_rows; --";
    let query = program(
        53,
        vec![
            SlotDefinition::new("source", text(false)),
            SlotDefinition::new("input", text(false)),
            SlotDefinition::new("output", text(false)),
        ],
        Some(text(false)),
        Vec::new(),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::text("SELECT $1")),
                },
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::text(attack)),
                },
                Instruction::ExecuteDynamicSql {
                    source: SlotId(0),
                    parameters: vec![SlotId(1)],
                    into: vec![SlotId(2)],
                    strict: true,
                },
            ],
            Terminator::Return(Some(SlotId(2))),
        )],
    );
    let outcome = execute(&executor, &query, &mut TestStage::default()).unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::text(attack)))
    );

    let bounded = program(
        54,
        vec![SlotDefinition::new("source", text(false))],
        None,
        Vec::new(),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::text(
                        "INSERT INTO dynamic_rows VALUES (1, 'first')",
                    )),
                },
                Instruction::ExecuteDynamicSql {
                    source: SlotId(0),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::text(
                        "INSERT INTO dynamic_rows VALUES (2, 'second')",
                    )),
                },
                Instruction::ExecuteDynamicSql {
                    source: SlotId(0),
                    parameters: Vec::new(),
                    into: Vec::new(),
                    strict: false,
                },
            ],
            Terminator::Return(None),
        )],
    );
    let mut policy = ResourcePolicy::default_call();
    policy.sql_statements = 1;
    let error = executor
        .execute_procedural_program(
            &bounded,
            Vec::new(),
            &ExecutionContext::new(),
            principals(),
            policy,
            &mut TestStage::default(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::ResourceSqlStatements);
    assert_eq!(
        scalar_integer(
            executor
                .execute("SELECT COUNT(*) FROM dynamic_rows")
                .unwrap()
        ),
        0,
        "budget failure must roll back the first dynamic statement",
    );
}

#[test]
fn dynamic_sql_rechecks_object_privileges_for_the_effective_principal() {
    let executor = executor();
    executor
        .execute("CREATE TABLE private_dynamic_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute("INSERT INTO private_dynamic_rows VALUES (7)")
        .unwrap();
    executor.execute("CREATE PRINCIPAL dynamic_alice").unwrap();
    executor
        .execute("GRANT CONNECT ON DATABASE test TO dynamic_alice")
        .unwrap();
    executor
        .execute("GRANT USAGE ON SCHEMA public TO dynamic_alice")
        .unwrap();
    let alice = named_object(&executor, ObjectKind::Principal, "dynamic_alice")
        .unwrap()
        .id();
    let candidate = program(
        55,
        vec![
            SlotDefinition::new("source", text(false)),
            SlotDefinition::new("output", integer(false)),
        ],
        Some(integer(false)),
        Vec::new(),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::text("SELECT id FROM private_dynamic_rows")),
                },
                Instruction::ExecuteDynamicSql {
                    source: SlotId(0),
                    parameters: Vec::new(),
                    into: vec![SlotId(1)],
                    strict: true,
                },
            ],
            Terminator::Return(Some(SlotId(1))),
        )],
    );
    let caller = PrincipalContext {
        session_principal: alice,
        invoker_principal: alice,
        effective_principal: alice,
    };
    let error = executor
        .execute_procedural_program(
            &candidate,
            Vec::new(),
            &ExecutionContext::new().with_principal_id(alice),
            caller,
            ResourcePolicy::default_call(),
            &mut TestStage::default(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), DiagnosticKind::SecurityObjectDenied);
}

#[test]
fn sql_expression_vm_owns_procedural_binary_semantics() {
    let executor = executor();
    let candidate = program(
        51,
        vec![
            SlotDefinition::new("left", integer(false)),
            SlotDefinition::new("right", integer(false)),
            SlotDefinition::new("result", integer(false)),
        ],
        Some(integer(false)),
        Vec::new(),
        vec![block(
            vec![
                Instruction::LoadConstant {
                    destination: SlotId(0),
                    value: RuntimeValue::scalar(Value::Integer(20)),
                },
                Instruction::LoadConstant {
                    destination: SlotId(1),
                    value: RuntimeValue::scalar(Value::Integer(22)),
                },
                Instruction::EvaluateSqlBinary {
                    destination: SlotId(2),
                    left: SlotId(0),
                    right: SlotId(1),
                    operator: InfixOperator::Add,
                },
            ],
            Terminator::Return(Some(SlotId(2))),
        )],
    );
    let outcome = execute(&executor, &candidate, &mut TestStage::default()).unwrap();
    assert_eq!(
        outcome.return_value,
        Some(RuntimeValue::scalar(Value::Integer(42)))
    );
}

#[test]
fn caller_cancellation_interrupts_a_running_procedural_loop() {
    let executor = executor();
    let candidate = program(
        53,
        Vec::new(),
        None,
        Vec::new(),
        vec![block(Vec::new(), Terminator::Jump(BlockId(0)))],
    );
    let context = ExecutionContext::new();
    let cancellation = context.cancellation_handle();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        cancellation.cancel();
    });
    let started = Instant::now();
    let error = executor
        .execute_procedural_program(
            &candidate,
            Vec::new(),
            &context,
            principals(),
            ResourcePolicy::default_call(),
            &mut TestStage::default(),
        )
        .unwrap_err();
    canceller.join().unwrap();
    assert_eq!(error.kind(), DiagnosticKind::ResourceCancelled);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!executor.has_active_transaction());
}
