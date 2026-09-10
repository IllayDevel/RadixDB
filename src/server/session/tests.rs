use super::*;

#[test]
fn compaction_backpressure_keeps_its_retryable_protocol_identity() {
    let failure = database_failure(&DatabaseError::CompactionBackpressure {
        table: "items".to_string(),
        segments: 32,
        physical_bytes: 1024,
        hard_segments: 32,
        hard_bytes: 2048,
    });
    assert_eq!(failure.code, ProtocolErrorCode::CompactionBackpressure);
    assert!(failure.code.is_retryable());
    assert!(failure.message.contains("COMPACTION_BACKPRESSURE"));
}

#[test]
fn authorization_denial_keeps_its_protocol_identity() {
    let failure = database_failure(&DatabaseError::AuthorizationDenied(
        "PL_SECURITY_EXECUTE_DENIED: EXECUTE denied".to_string(),
    ));
    assert_eq!(failure.code, ProtocolErrorCode::AuthorizationDenied);
    assert!(!failure.code.is_retryable());
    assert!(failure.message.contains("PL_SECURITY_EXECUTE_DENIED"));
}

#[test]
fn r8_l01_batch_a_session_hooks_reset_during_unwind() {
    let unwind = std::panic::catch_unwind(|| {
        let _execute_hook = ExecuteSqlTestHookGuard::install(std::sync::Arc::new(|| {}));
        let _artifact_hook = ArtifactScanTestHookGuard::install(std::sync::Arc::new(|_| {}));
        panic!("exercise panic-safe hook reset");
    });
    assert!(unwind.is_err());

    // A parallel test may install its own hook as soon as the unwinding guard
    // releases ownership. Reacquire the same owner before inspecting global
    // state so this assertion observes this guard's completed cleanup rather
    // than another test's legitimate hook lifetime.
    let execute_owner = EXECUTE_SQL_TEST_HOOK_OWNER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(EXECUTE_SQL_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    drop(execute_owner);

    let artifact_owner = ARTIFACT_SCAN_TEST_HOOK_OWNER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(ARTIFACT_SCAN_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    drop(artifact_owner);
}

#[test]
fn r9_execution_registry_cancels_exact_active_request_and_releases_identity() {
    let runtime = RuntimeState::new();
    let session_handle = ServerCancellation::new();
    let mut context = ServerExecutionContext::positional(Vec::new().into());
    context.bind_parent_cancellation(&session_handle);
    let handle = context.cancellation();
    let registration = runtime.register_execution(77, handle.clone()).unwrap();
    assert!(runtime.register_execution(77, handle.clone()).is_err());
    assert!(runtime.cancel_execution(77).unwrap());
    assert!(handle.is_cancelled());
    assert!(!session_handle.is_cancelled());
    let mut next_context = ServerExecutionContext::positional(Vec::new().into());
    next_context.bind_parent_cancellation(&session_handle);
    assert!(!next_context.cancellation().is_cancelled());
    drop(registration);
    assert!(!runtime.cancel_execution(77).unwrap());
}

#[test]
fn r9_global_frame_budget_backpressures_until_capacity_is_released() {
    let runtime = std::sync::Arc::new(RuntimeState::new());
    let first = runtime.acquire_frame(48, 64).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let worker_runtime = runtime.clone();
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        let _second = worker_runtime.acquire_frame(32, 64).unwrap();
        acquired_tx.send(()).unwrap();
    });

    barrier.wait();
    assert!(acquired_rx
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_err());
    drop(first);
    acquired_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("releasing capacity must wake a waiting frame");
    worker.join().unwrap();
    assert_eq!(runtime.inflight_frame_bytes.load(Ordering::Acquire), 0);
}

#[test]
fn r9_failed_database_open_retries_after_repair_and_namespace_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    config.max_databases = 1;
    let databases = Mutex::new(BTreeMap::new());
    let broken = config.data_dir.join("databases").join("retry");
    std::fs::create_dir_all(broken.parent().unwrap()).unwrap();
    std::fs::write(&broken, b"temporary obstacle").unwrap();
    assert!(open_database(&config, &databases, "retry").is_err());
    std::fs::remove_file(&broken).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(110));
    let database = open_database(&config, &databases, "retry").expect("repaired open retries");
    assert!(matches!(
        open_database(&config, &databases, "second"),
        Err(OpenDatabaseError::Server(message)) if message.contains("database limit")
    ));
    database.close().unwrap();
}

#[test]
fn cursor_open_publishes_bound_type_and_nullability() {
    let database = Database::open_in_memory().expect("open database");
    database
        .execute(
            "CREATE TABLE metadata_owner (id INTEGER PRIMARY KEY, note TEXT)",
            (),
        )
        .unwrap();
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: Some("metadata_owner".to_string()),
        selected_database: Some(database),
        cursor: None,
        transaction: None,
        next_cursor_id: 1,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let config = test_config(tempfile::tempdir().unwrap().path().join("server-data"));
    let cancellation = ServerCancellation::new();
    let runtime = RuntimeState::new();
    let databases = Mutex::new(BTreeMap::new());
    let session_runtime = SessionRuntimeContext {
        databases: &databases,
        plugin_registry: &EMPTY_PLUGIN_REGISTRY,
        cancellation: &cancellation,
        runtime: &runtime,
    };
    let response = execute_sql(
        &mut session,
        1,
        "SELECT id, note, 1 AS literal FROM metadata_owner",
        WireBindings {
            positional: Vec::new(),
            named: BTreeMap::new(),
        },
        &config,
        &session_runtime,
    );
    let ServerMessage::CursorOpened { columns, .. } = response else {
        panic!("SELECT must open cursor");
    };
    assert_eq!(
        columns,
        vec![
            Column {
                name: "id".to_string(),
                type_name: "INTEGER".to_string(),
                nullable: false,
                external_type: None,
            },
            Column {
                name: "note".to_string(),
                type_name: "TEXT".to_string(),
                nullable: true,
                external_type: None,
            },
            Column {
                name: "literal".to_string(),
                type_name: "INTEGER".to_string(),
                nullable: false,
                external_type: None,
            },
        ]
    );
}

#[test]
fn r3_l02_batch_b_protocol_begin_rejects_an_existing_sql_transaction_owner() {
    let database = Database::open_in_memory().expect("open database");
    database
        .execute("BEGIN", ())
        .expect("begin SQL transaction");
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: Some("batch-b-owner".to_string()),
        selected_database: Some(database),
        cursor: None,
        transaction: None,
        next_cursor_id: 1,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };

    assert!(matches!(
        begin_transaction(
            &mut session,
            radixdb_client::TransactionIsolation::ReadCommitted,
        ),
        ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::TransactionState,
            ..
        })
    ));
    assert!(session.transaction.is_none());

    let config = test_config(tempfile::tempdir().unwrap().path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let cancellation = ServerCancellation::new();
    let runtime = RuntimeState::new();
    let session_runtime = SessionRuntimeContext {
        databases: &databases,
        plugin_registry: &EMPTY_PLUGIN_REGISTRY,
        cancellation: &cancellation,
        runtime: &runtime,
    };
    assert!(matches!(
        handle_ready_message(
            &mut session,
            ClientMessage::SelectDatabase {
                database: "another".to_string(),
            },
            &config,
            &session_runtime,
            config.max_frame_bytes,
        ),
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::TransactionState,
            ..
        }))
    ));
    session
        .selected_database
        .as_ref()
        .expect("selected database")
        .execute("ROLLBACK", ())
        .expect("rollback SQL transaction");
}

#[test]
fn r3_l02_batch_b_sql_begin_rejects_an_existing_protocol_transaction_owner() {
    let database = Database::open_in_memory().expect("open database");
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: Some("batch-b-owner-reverse".to_string()),
        selected_database: Some(database),
        cursor: None,
        transaction: None,
        next_cursor_id: 1,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };
    assert!(matches!(
        begin_transaction(
            &mut session,
            radixdb_client::TransactionIsolation::ReadCommitted,
        ),
        ServerMessage::TransactionBegan
    ));

    let config = test_config(tempfile::tempdir().unwrap().path().join("server-data"));
    let cancellation = ServerCancellation::new();
    let runtime = RuntimeState::new();
    let databases = Mutex::new(BTreeMap::new());
    let session_runtime = SessionRuntimeContext {
        databases: &databases,
        plugin_registry: &EMPTY_PLUGIN_REGISTRY,
        cancellation: &cancellation,
        runtime: &runtime,
    };
    assert!(matches!(
        execute_sql(
            &mut session,
            1,
            "BEGIN",
            WireBindings {
                positional: Vec::new(),
                named: BTreeMap::new(),
            },
            &config,
            &session_runtime,
        ),
        ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::TransactionState,
            ..
        })
    ));
    assert!(session.transaction.is_some());
    assert!(matches!(
        rollback_transaction(&mut session),
        ServerMessage::TransactionRolledBack
    ));
}

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: "127.0.0.1".parse().unwrap(),
        port: 15441,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 16,
        max_inflight_frame_bytes: crate::server::config::default_max_inflight_frame_bytes(),
        max_databases: crate::server::config::default_max_databases(),
        max_database_name_bytes: crate::server::config::default_max_database_name_bytes(),
        connect_timeout_secs: 1,
        connection_idle_timeout_secs: 1,
        net_read_timeout_secs: 1,
        net_write_timeout_secs: 1,
        cursor_batch_max_rows: 128,
        cursor_batch_max_bytes: 1024 * 1024,
        max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        copy_max_transaction_bytes: crate::server::config::default_copy_max_transaction_bytes(),
        max_compaction_jobs: crate::server::config::default_max_compaction_jobs(),
        storage_cpu_workers: crate::server::config::default_storage_cpu_workers(),
        page_cache_level: crate::server::config::default_page_cache_level(),
        page_cache_max_bytes: crate::server::config::default_page_cache_max_bytes(),
        page_cache_memory_reserve: crate::server::config::default_page_cache_memory_reserve(),
        target_volume_rows: crate::server::config::default_target_volume_rows(),
        seal_hot_bytes_threshold: crate::server::config::default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold:
            crate::server::config::default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

struct TimeoutAfterInputStream {
    input: std::io::Cursor<Vec<u8>>,
    output: Vec<u8>,
    timed_out: bool,
}

impl TimeoutAfterInputStream {
    fn new(input: Vec<u8>) -> Self {
        Self {
            input: std::io::Cursor::new(input),
            output: Vec::new(),
            timed_out: false,
        }
    }

    fn output(&self) -> &[u8] {
        &self.output
    }
}

impl Read for TimeoutAfterInputStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.input.position() as usize >= self.input.get_ref().len() {
            self.timed_out = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "test idle timeout",
            ));
        }
        self.input.read(buffer)
    }
}

impl Write for TimeoutAfterInputStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.output.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn push_client_message(input: &mut Vec<u8>, message: ClientMessage) {
    crate::protocol::write_frame(input, &message, DEFAULT_MAX_FRAME_BYTES)
        .expect("encode client message");
}

fn push_execute(input: &mut Vec<u8>, sql: impl Into<String>) {
    push_client_message(
        input,
        ClientMessage::Execute {
            request_id: 1,
            sql: sql.into(),
            positional: Vec::new(),
            named: BTreeMap::new(),
        },
    );
}

#[test]
fn r6_l01_b_authentication_remains_in_connect_timeout_phase() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let mut input = Vec::new();
    push_client_message(
        &mut input,
        ClientMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: Vec::new(),
        },
    );

    let mut stream = TimeoutAfterInputStream::new(input);
    let mut phases = Vec::new();
    serve_connection_with_databases_and_limits(&mut stream, &config, &databases, |_, phase| {
        phases.push(phase);
        Ok(())
    })
    .expect("authentication timeout closes the incomplete session cleanly");

    assert!(stream.timed_out);
    assert!(
        phases.is_empty(),
        "AwaitAuthentication must retain the initially installed connect timeout"
    );
}

fn decode_server_messages(output: &[u8]) -> Vec<ServerMessage> {
    let mut cursor = std::io::Cursor::new(output);
    let mut messages = Vec::new();
    while cursor.position() as usize != output.len() {
        messages.push(
            crate::protocol::read_frame(&mut cursor, DEFAULT_MAX_FRAME_BYTES)
                .expect("decode server message"),
        );
    }
    messages
}

#[test]
fn server_database_registry_keeps_engine_open_after_session_drop() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());

    {
        let db = open_database(&config, &databases, "extension_smoke").unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS smoke_item (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO smoke_item VALUES (1, 'first')", ())
            .unwrap();
    }

    let db = open_database(&config, &databases, "extension_smoke").unwrap();
    let mut rows = db
        .query("SELECT name FROM smoke_item WHERE id = 1", ())
        .unwrap();
    let row = rows.next().unwrap().unwrap();
    let name: String = row.get(0).unwrap();
    assert_eq!(name, "first");
}

#[test]
fn prepared_row_batch_reuses_one_exact_payload() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let rows = (0..251)
        .map(|index| Row {
            values: vec![
                WireValue::Int(index),
                WireValue::String(format!("row-{index}")),
            ],
        })
        .collect::<Vec<_>>();
    let expected = encode_payload(&ServerMessage::RowBatch {
        cursor_id: 42,
        rows: rows.clone(),
        eof: true,
    })
    .unwrap();

    crate::storage::instrumentation::begin_protocol_component_probe();
    let prepared = prepare_row_batch(42, rows, true, &config).unwrap();
    let probe = crate::storage::instrumentation::end_protocol_component_probe();

    assert_eq!(prepared.payload, expected);
    assert_eq!(prepared.emitted_rows, 251);
    assert!(prepared.eof);
    assert!(prepared.pending.is_none());
    assert_eq!(probe.encode_calls, 1);
    assert_eq!(probe.encode_bytes, expected.len() as u64);
}

#[test]
fn oversized_prepared_row_batch_retains_ordered_bounded_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    let make_row = |id| Row {
        values: vec![WireValue::Int(id), WireValue::String("x".repeat(128))],
    };
    let one_row_limit = encode_payload(&ServerMessage::RowBatch {
        cursor_id: 7,
        rows: vec![make_row(1)],
        eof: false,
    })
    .unwrap()
    .len();
    config.cursor_batch_max_bytes = one_row_limit;

    let mut rows = vec![make_row(1), make_row(2), make_row(3)];
    let mut source_eof = true;
    let mut emitted_ids = Vec::new();
    loop {
        let prepared = prepare_row_batch(7, rows, source_eof, &config).unwrap();
        assert!(prepared.payload.len() <= config.cursor_batch_max_bytes);
        let (message, consumed): (ServerMessage, usize) =
            bincode::serde::decode_from_slice(&prepared.payload, bincode::config::standard())
                .unwrap();
        assert_eq!(consumed, prepared.payload.len());
        let ServerMessage::RowBatch {
            rows: decoded, eof, ..
        } = message
        else {
            panic!("prepared payload must remain a RowBatch")
        };
        emitted_ids.extend(decoded.into_iter().map(|row| match row.values[0] {
            WireValue::Int(id) => id,
            ref other => panic!("unexpected first value {other:?}"),
        }));

        match prepared.pending {
            Some(pending) => {
                assert!(!eof);
                rows = pending.rows;
                source_eof = pending.eof;
            }
            None => {
                assert!(eof);
                break;
            }
        }
    }
    assert_eq!(emitted_ids, vec![1, 2, 3]);
}

#[test]
fn oversized_single_prepared_row_fails_without_partial_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    let row = Row {
        values: vec![WireValue::String("x".repeat(256))],
    };
    let exact = encode_payload(&ServerMessage::RowBatch {
        cursor_id: 8,
        rows: vec![row.clone()],
        eof: true,
    })
    .unwrap()
    .len();
    config.cursor_batch_max_bytes = exact - 1;

    let error = match prepare_row_batch(8, vec![row], true, &config) {
        Ok(_) => panic!("oversized single row must not produce a partial payload"),
        Err(error) => error,
    };
    assert!(error.contains("single row exceeds"));
}

#[test]
fn typed_columns_preserve_wire_value_semantics_without_row_adapter() {
    assert_eq!(
        column_data_to_wire_column(ServerColumnData::Int64 {
            values: vec![7, 9],
            nulls: vec![false, true],
        })
        .unwrap(),
        WireColumn::Int64 {
            values: vec![7, 9],
            nulls: vec![false, true],
        }
    );
    assert_eq!(
        column_data_to_wire_column(ServerColumnData::TimestampNanos {
            values: vec![-1, 1_999_999],
            nulls: vec![false, false],
        })
        .unwrap(),
        WireColumn::TimestampNanos {
            values: vec![-1, 1_999_999],
            nulls: vec![false, false],
        },
        "typed timestamp conversion must preserve nanoseconds"
    );
    assert_eq!(
        column_data_to_wire_column(ServerColumnData::DictionaryText {
            ids: vec![1, 0],
            dictionary: vec!["apple".into(), "pear".into()],
            nulls: vec![false, false],
        })
        .unwrap(),
        WireColumn::DictionaryText {
            ids: vec![1, 0],
            dictionary: vec!["apple".into(), "pear".into()],
            nulls: vec![false, false],
        }
    );
}

#[test]
fn typed_columns_preserve_legacy_row_wire_values() {
    let columns = [
        WireColumn::Int64 {
            values: vec![7, 0],
            nulls: vec![false, true],
        },
        WireColumn::Float64 {
            values: vec![1.5, 0.0],
            nulls: vec![false, true],
        },
        WireColumn::Boolean {
            values: vec![true, false],
            nulls: vec![false, true],
        },
        WireColumn::TimestampNanos {
            values: vec![1234, 0],
            nulls: vec![false, true],
        },
        WireColumn::DictionaryText {
            ids: vec![1, 0],
            dictionary: vec!["north".into(), "south".into()],
            nulls: vec![false, true],
        },
        WireColumn::Bytes {
            data: vec![1, 2, 3],
            offsets: vec![(0, 3), (0, 0)],
            nulls: vec![false, true],
        },
        WireColumn::JsonText {
            data: br#"{"ok":true}"#.to_vec(),
            offsets: vec![(0, 11), (0, 0)],
            nulls: vec![false, true],
        },
    ];

    assert_eq!(
        columns
            .iter()
            .map(|column| wire_value_from_column(column, 0).unwrap())
            .collect::<Vec<_>>(),
        vec![
            WireValue::Int(7),
            WireValue::Float64(1.5),
            WireValue::Bool(true),
            WireValue::TimestampNanos {
                nanos_since_unix_epoch_utc: 1234,
            },
            WireValue::String("south".into()),
            WireValue::Bytes(vec![1, 2, 3]),
            WireValue::Json(r#"{"ok":true}"#.into()),
        ]
    );
    assert!(columns
        .iter()
        .all(|column| wire_value_from_column(column, 1).unwrap() == WireValue::Null));
}

#[test]
fn column_batch_capability_is_negotiated_only_when_requested() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let mut state = SessionState::AwaitHandshake;
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: vec![ProtocolCapability::ColumnBatchV1],
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );
    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::HandshakeAccepted { capabilities, .. })
            if capabilities.as_slice() == [ProtocolCapability::ColumnBatchV1]
    ));

    let response = handle_message(
        &mut state,
        ClientMessage::Authenticate {
            login: "root".into(),
            password: None,
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );
    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::AuthenticationAccepted)
    ));
    let SessionState::Ready(session) = &state else {
        panic!("authentication must create a ready session");
    };
    assert!(session.column_batch_v1);
}

#[test]
fn r6_l02_wire_state_rejected_handshake_does_not_mutate_session_limits() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());

    for rejected_limit in [0, 1] {
        let mut state = SessionState::AwaitHandshake;
        let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;
        let response = handle_message(
            &mut state,
            ClientMessage::Handshake {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: rejected_limit,
                capabilities: vec![ProtocolCapability::ColumnBatchV1],
            },
            &config,
            &databases,
            &mut max_frame_bytes,
        );

        assert!(matches!(
            response,
            ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
                code: ProtocolErrorCode::ProtocolViolation,
                ..
            }))
        ));
        assert!(matches!(state, SessionState::AwaitHandshake));
        assert_eq!(max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    }
}

#[test]
fn r6_l02_release_config_uses_negotiated_client_frame_budget() {
    let mut config = test_config(tempfile::tempdir().unwrap().path().join("server-data"));
    config.cursor_batch_max_bytes = 8 * 1024 * 1024;
    config.max_frame_bytes = 64 * 1024 * 1024;
    let negotiated = effective_session_config(&config, 4096);
    assert_eq!(negotiated.max_frame_bytes, 4096);
    assert_eq!(negotiated.cursor_batch_max_bytes, 4096);
}

#[test]
fn passwordless_root_authentication_requires_loopback_bind_ip() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    config.bind_ip = "0.0.0.0".parse().unwrap();
    let databases = Mutex::new(BTreeMap::new());
    let mut state = SessionState::AwaitAuthentication {
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::Authenticate {
            login: "root".into(),
            password: None,
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );

    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::AuthenticationFailed,
            ref message,
        })) if message.contains("loopback")
    ));
    assert!(matches!(state, SessionState::AwaitAuthentication { .. }));
}

#[test]
fn password_authentication_is_explicitly_rejected_until_configured() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let mut state = SessionState::AwaitAuthentication {
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::Authenticate {
            login: "root".into(),
            password: Some("secret".into()),
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );

    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::AuthenticationFailed,
            ref message,
        })) if message.contains("password authentication is not configured")
    ));
    assert!(matches!(state, SessionState::AwaitAuthentication { .. }));
}

#[test]
fn configured_root_password_is_required_and_works_on_remote_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    config.bind_ip = "0.0.0.0".parse().unwrap();
    let encoded = radixdb_executor::credentials::hash_password_verifier("root-secret").unwrap();
    config.authentication.root_password_verifier =
        Some(crate::server::config::RootPasswordVerifier::parse(encoded).unwrap());

    assert_eq!(
        authenticate_client(&config, "root", Some("root-secret")),
        Ok(())
    );
    assert_eq!(
        authenticate_client(&config, "root", None),
        Err("root authentication failed".to_string())
    );
    assert_eq!(
        authenticate_client(&config, "root", Some("wrong")),
        Err("root authentication failed".to_string())
    );
}

#[test]
fn non_root_login_is_explicitly_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let mut state = SessionState::AwaitAuthentication {
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::Authenticate {
            login: "alice".into(),
            password: None,
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );

    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::AuthenticationFailed,
            ref message,
        })) if message.contains("unsupported login `alice`")
    ));
    assert!(matches!(state, SessionState::AwaitAuthentication { .. }));
}

#[test]
fn selecting_database_that_is_opening_returns_retryable_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::from([(
        "recovering".to_string(),
        DatabaseRegistryEntry::Opening {
            artifacts: unavailable_artifact_summary(),
        },
    )]));
    let mut state = SessionState::Ready(ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: None,
        selected_database: None,
        cursor: None,
        transaction: None,
        next_cursor_id: 1,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: false,
        build_identity_v1: false,
        external_value_v1: false,
    });
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::SelectDatabase {
            database: "recovering".to_string(),
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );

    assert!(matches!(
        response,
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::ServerError,
            message,
        })) if message.contains("opening/recovering")
    ));
}

#[test]
fn server_status_reports_opening_database_without_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::from([(
        "recovering".to_string(),
        DatabaseRegistryEntry::Opening {
            artifacts: unavailable_artifact_summary(),
        },
    )]));
    let mut state = SessionState::Ready(ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: None,
        selected_database: None,
        cursor: None,
        transaction: None,
        next_cursor_id: 1,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: false,
        build_identity_v1: true,
        external_value_v1: false,
    });
    let mut max_frame_bytes = DEFAULT_MAX_FRAME_BYTES;

    let response = handle_message(
        &mut state,
        ClientMessage::ServerStatus {
            database: Some("recovering".to_string()),
        },
        &config,
        &databases,
        &mut max_frame_bytes,
    );

    let ServerResponse::Message(ServerMessage::ServerStatus(status)) = response else {
        panic!("expected server status response");
    };
    let identity = status
        .build
        .as_ref()
        .expect("negotiated status must include build identity");
    assert_eq!(identity.semantic_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(identity.protocol_version, PROTOCOL_VERSION);
    assert_eq!(status.lifecycle, ServerLifecycleState::Opening);
    assert!(!status.ready);
    assert_eq!(status.databases.len(), 1);
    assert_eq!(status.databases[0].name, "recovering");
    assert_eq!(status.databases[0].lifecycle, ServerLifecycleState::Opening);
    assert!(!status.databases[0].ready);
    assert!(status.databases[0].message.contains("opening/recovering"));
}

#[test]
fn restricted_plugin_database_does_not_block_a_normal_database() {
    use radixdb_plugin_host::{PluginRegistry, RegisteredPackage};

    const PACKAGE_ID: [u8; 16] = [0x72; 16];
    const FINGERPRINT: [u8; 32] = [0xa6; 32];

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let databases = Mutex::new(BTreeMap::new());
    let installed = Arc::new(PluginRegistry::from_test_packages([
        RegisteredPackage::for_test(PACKAGE_ID, "status_sample", "1.2.3", FINGERPRINT),
    ]));

    let seeded = open_database_with_plugin_registry(
        &config,
        &databases,
        "restricted",
        Arc::clone(&installed),
    )
    .unwrap();
    seeded
        .execute("CREATE EXTENSION status_sample VERSION '1.2.3'", ())
        .unwrap();
    drop(seeded);
    let seeded = match databases.lock().unwrap().remove("restricted").unwrap() {
        DatabaseRegistryEntry::Ready { database, .. } => database,
        _ => panic!("seed database must be ready"),
    };
    seeded.close().unwrap();

    let empty = Arc::new(PluginRegistry::empty());
    let restricted =
        open_database_with_plugin_registry(&config, &databases, "restricted", Arc::clone(&empty))
            .unwrap();
    let normal = open_database_with_plugin_registry(&config, &databases, "normal", empty).unwrap();

    assert!(restricted.execute("SELECT 1", ()).is_err());
    normal.execute("SELECT 1", ()).unwrap();

    let ServerMessage::ServerStatus(status) =
        server_status(&config, &databases, None, false, &RuntimeState::new())
    else {
        panic!("expected server status");
    };
    assert_eq!(status.lifecycle, ServerLifecycleState::Ready);
    assert!(status.ready);
    assert!(status.message.contains("restricted plugin diagnostic mode"));
    assert_eq!(status.runtime.open_databases, 1);
    assert_eq!(status.runtime.retained_databases, 2);
    let restricted_status = status
        .databases
        .iter()
        .find(|database| database.name == "restricted")
        .unwrap();
    assert_eq!(restricted_status.lifecycle, ServerLifecycleState::Ready);
    assert!(!restricted_status.ready);
    assert!(restricted_status.message.contains("missing package"));
    let normal_status = status
        .databases
        .iter()
        .find(|database| database.name == "normal")
        .unwrap();
    assert!(normal_status.ready);
}

#[test]
fn status_uses_bounded_cached_inventory_for_opening_and_ready_state() {
    use std::sync::{mpsc, Arc, Condvar};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    std::fs::create_dir_all(config.data_dir.join("databases")).unwrap();
    let databases = Mutex::new(BTreeMap::new());
    let database_dir = config.data_dir.join("databases").join("recovering");
    let scan_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let scan_count_for_hook = Arc::clone(&scan_count);
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let release_for_hook = Arc::clone(&release);
    let database_dir_for_hook = database_dir.clone();
    let _artifact_hook = ArtifactScanTestHookGuard::install(Arc::new(move |path| {
        if path != database_dir_for_hook {
            return;
        }
        let ordinal = scan_count_for_hook.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if ordinal != 0 {
            return;
        }
        entered_tx.send(()).expect("report opening inventory");
        let (lock, changed) = &*release_for_hook;
        let mut released = lock.lock().expect("artifact release lock");
        while !*released {
            released = changed.wait(released).expect("artifact release wait");
        }
    }));

    let (
        opening_status,
        ready_status,
        ready_database,
        opening_status_was_bounded,
        ready_status_was_bounded,
    ) = std::thread::scope(|scope| {
        let opener = scope.spawn(|| open_database(&config, &databases, "recovering"));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("real open published its inventory phase");

        let scans_before_opening_status = scan_count.load(std::sync::atomic::Ordering::Acquire);
        let opening = server_status(
            &config,
            &databases,
            Some("recovering"),
            false,
            &RuntimeState::new(),
        );
        let opening_status_was_bounded =
            scan_count.load(std::sync::atomic::Ordering::Acquire) == scans_before_opening_status;
        {
            let (lock, changed) = &*release;
            *lock.lock().expect("artifact release lock") = true;
            changed.notify_all();
        }
        let database = opener
            .join()
            .expect("database opener thread")
            .expect("database reaches ready");
        let scans_before_ready_status = scan_count.load(std::sync::atomic::Ordering::Acquire);
        let ready = server_status(
            &config,
            &databases,
            Some("recovering"),
            false,
            &RuntimeState::new(),
        );
        let ready_status_was_bounded =
            scan_count.load(std::sync::atomic::Ordering::Acquire) == scans_before_ready_status;
        (
            opening,
            ready,
            database,
            opening_status_was_bounded,
            ready_status_was_bounded,
        )
    });
    let ServerMessage::ServerStatus(opening_status) = opening_status else {
        panic!("expected opening status");
    };
    let ServerMessage::ServerStatus(ready_status) = ready_status else {
        panic!("expected ready status");
    };
    assert_eq!(opening_status.lifecycle, ServerLifecycleState::Opening);
    assert!(!opening_status.ready);
    assert_eq!(ready_status.lifecycle, ServerLifecycleState::Ready);
    assert!(ready_status.ready);

    ready_database.close().expect("close ready database");
    let failed_path = config.data_dir.join("databases").join("failed");
    std::fs::write(&failed_path, b"not a database directory").unwrap();
    assert!(open_database(&config, &databases, "failed").is_err());
    let ServerMessage::ServerStatus(failed_status) = server_status(
        &config,
        &databases,
        Some("failed"),
        false,
        &RuntimeState::new(),
    ) else {
        panic!("expected retained failed status");
    };
    assert_eq!(failed_status.lifecycle, ServerLifecycleState::Emergency);
    assert!(!failed_status.ready);
    assert!(
        opening_status_was_bounded,
        "opening status must use its bounded lifecycle snapshot"
    );
    assert!(
        ready_status_was_bounded,
        "ready status must use the sampled inventory instead of rescanning storage"
    );
}

#[test]
fn r2_l01_batch_b_artifact_summary_is_semantic_and_fail_visible() {
    let dir = tempfile::tempdir().unwrap();
    let missing = collect_database_artifacts(&dir.path().join("missing"));
    assert!(!missing.complete);
    assert_eq!(missing.scan_errors, 1);
    assert_eq!(missing.table_dirs, 0);

    let database = dir.path().join("database");
    std::fs::create_dir_all(database.join("wal")).unwrap();
    std::fs::create_dir_all(database.join("manifests/tables/orders-id")).unwrap();
    std::fs::create_dir_all(database.join("manifests/tables/customers-id")).unwrap();
    std::fs::create_dir_all(database.join("artifacts/data/aa")).unwrap();
    std::fs::create_dir_all(database.join("artifacts/index/bb")).unwrap();
    std::fs::create_dir_all(database.join("catalog")).unwrap();
    std::fs::create_dir_all(database.join("snapshots/snapshot-id/members/data/aa")).unwrap();
    std::fs::write(database.join("wal").join("wal-1.log"), b"wal").unwrap();
    std::fs::write(database.join("artifacts/data/aa/artifact.data"), b"data").unwrap();
    std::fs::write(database.join("artifacts/index/bb/artifact.idx"), b"index").unwrap();
    std::fs::write(database.join("manifests/database-1.mft"), b"manifest").unwrap();
    std::fs::write(database.join("catalog/catalog-1.cat"), b"catalog").unwrap();
    std::fs::write(database.join("CONTROL.0"), b"control").unwrap();
    std::fs::write(
        database.join("snapshots/snapshot-id/SNAPSHOT.mft"),
        b"snapshot",
    )
    .unwrap();

    let summary = collect_database_artifacts(&database);
    assert!(!summary.complete);
    assert!(summary.snapshots_omitted);
    assert!(!summary.truncated);
    assert!(summary.sequence > 0);
    assert!(summary.sampled_unix_millis > 0);
    assert_eq!(summary.scan_errors, 0);
    assert_eq!(summary.table_dirs, 2, "table owners must be deduplicated");
    assert_eq!(summary.wal_files, 1);
    assert_eq!(summary.artifact_files, 2);
    assert_eq!(summary.snapshot_files, 0);
    assert_eq!(summary.checkpoint_files, 1);
    assert_eq!(summary.manifest_files, 2);
}

#[test]
fn artifact_summary_reports_entry_and_depth_budget_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("database");
    std::fs::create_dir_all(database.join("a/b/c")).unwrap();
    for ordinal in 0..16 {
        std::fs::write(database.join(format!("entry-{ordinal}.data")), b"x").unwrap();
    }

    let entry_limited = status::collect_database_artifacts_with_limits(&database, 4, 8);
    assert!(!entry_limited.complete);
    assert!(entry_limited.truncated);
    assert_eq!(entry_limited.entries_visited, 4);

    let depth_limited = status::collect_database_artifacts_with_limits(&database, 100, 1);
    assert!(!depth_limited.complete);
    assert!(depth_limited.truncated);
    assert!(depth_limited.entries_visited <= 20);
}

#[test]
fn column_batch_reuses_the_validated_payload_for_frame_write() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let message = ServerMessage::ColumnBatch {
        cursor_id: 7,
        columns: vec![WireColumn::Int64 {
            values: vec![10, 20],
            nulls: vec![false, false],
        }],
        row_count: 2,
        eof: false,
    };
    let expected_payload = encode_payload(&message).unwrap();
    let prepared = encode_column_batch_response(message.clone(), &config);
    let ServerResponse::Encoded { payload } = prepared else {
        panic!("bounded column batch must retain its prepared payload");
    };
    assert_eq!(payload, expected_payload);

    let mut frame = Vec::new();
    ServerResponse::Encoded {
        payload: payload.clone(),
    }
    .write_to(
        &mut frame,
        config.max_frame_bytes,
        &RuntimeState::new(),
        config.max_inflight_frame_bytes,
    )
    .unwrap();
    assert_eq!(&frame[..4], &(payload.len() as u32).to_be_bytes());
    assert_eq!(&frame[4..], payload.as_slice());
}

#[test]
fn protocol_component_probe_separates_encode_and_socket_write() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));

    let command = ServerMessage::CommandComplete {
        affected_rows: 3,
        last_insert_id: 7,
    };
    let command_payload = encode_payload(&command).unwrap();
    crate::storage::instrumentation::begin_protocol_component_probe();
    let mut command_frame = Vec::new();
    ServerResponse::Message(command)
        .write_to(
            &mut command_frame,
            config.max_frame_bytes,
            &RuntimeState::new(),
            config.max_inflight_frame_bytes,
        )
        .unwrap();
    let command_probe = crate::storage::instrumentation::end_protocol_component_probe();
    assert_eq!(command_probe.encode_calls, 1);
    assert_eq!(command_probe.encode_bytes, command_payload.len() as u64);
    assert_eq!(command_probe.socket_write_calls, 1);
    assert_eq!(
        command_probe.socket_write_bytes,
        command_payload.len() as u64 + 4
    );
    assert_eq!(
        &command_frame[..4],
        &(command_payload.len() as u32).to_be_bytes()
    );
    assert_eq!(&command_frame[4..], command_payload.as_slice());

    let column_batch = ServerMessage::ColumnBatch {
        cursor_id: 9,
        columns: vec![WireColumn::Int64 {
            values: vec![10, 20],
            nulls: vec![false, false],
        }],
        row_count: 2,
        eof: false,
    };
    let column_payload = encode_payload(&column_batch).unwrap();
    crate::storage::instrumentation::begin_protocol_component_probe();
    let prepared = encode_column_batch_response(column_batch, &config);
    let prepare_probe = crate::storage::instrumentation::end_protocol_component_probe();
    assert_eq!(prepare_probe.encode_calls, 1);
    assert_eq!(prepare_probe.encode_bytes, column_payload.len() as u64);
    assert_eq!(prepare_probe.socket_write_calls, 0);
    assert_eq!(prepare_probe.socket_write_bytes, 0);

    let ServerResponse::Encoded { payload } = prepared else {
        panic!("bounded column batch must keep its prepared payload");
    };
    assert_eq!(payload, column_payload);

    crate::storage::instrumentation::begin_protocol_component_probe();
    let mut column_frame = Vec::new();
    ServerResponse::Encoded {
        payload: payload.clone(),
    }
    .write_to(
        &mut column_frame,
        config.max_frame_bytes,
        &RuntimeState::new(),
        config.max_inflight_frame_bytes,
    )
    .unwrap();
    let write_probe = crate::storage::instrumentation::end_protocol_component_probe();
    assert_eq!(write_probe.encode_calls, 0);
    assert_eq!(write_probe.encode_bytes, 0);
    assert_eq!(write_probe.socket_write_calls, 1);
    assert_eq!(write_probe.socket_write_bytes, payload.len() as u64 + 4);
    assert_eq!(&column_frame[..4], &(payload.len() as u32).to_be_bytes());
    assert_eq!(&column_frame[4..], payload.as_slice());
}

#[test]
fn prepared_column_batch_frame_handles_short_socket_writes() {
    struct ShortWriter {
        bytes: Vec<u8>,
        max_chunk: usize,
    }

    impl std::io::Write for ShortWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            let written = self.max_chunk.min(buffer.len());
            self.bytes.extend_from_slice(&buffer[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("server-data"));
    let message = ServerMessage::ColumnBatch {
        cursor_id: 17,
        columns: vec![WireColumn::Int64 {
            values: vec![10, 20, 30],
            nulls: vec![false, false, false],
        }],
        row_count: 3,
        eof: false,
    };
    let payload = encode_payload(&message).unwrap();
    let mut writer = ShortWriter {
        bytes: Vec::new(),
        max_chunk: 2,
    };
    ServerResponse::Encoded {
        payload: payload.clone(),
    }
    .write_to(
        &mut writer,
        config.max_frame_bytes,
        &RuntimeState::new(),
        config.max_inflight_frame_bytes,
    )
    .unwrap();

    assert_eq!(&writer.bytes[..4], &(payload.len() as u32).to_be_bytes());
    assert_eq!(&writer.bytes[4..], payload.as_slice());
}

#[test]
fn column_batch_protocol_boundary_does_not_materialize_rows_for_supported_typed_batch() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/protocol_typed_boundary", dir.path().display());
    let database = Database::open(&dsn).unwrap();
    database
        .execute(
            "CREATE TABLE protocol_typed_boundary (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )",
            (),
        )
        .unwrap();
    for id in 1..=32 {
        let sql = format!(
            "INSERT INTO protocol_typed_boundary (id, name)
                 VALUES ({id}, '{}')",
            if id % 2 == 0 { "north" } else { "south" }
        );
        database.execute(&sql, ()).unwrap();
    }
    database.execute("PRAGMA CHECKPOINT", ()).unwrap();

    let rows = database
        .query("SELECT id, name FROM protocol_typed_boundary", ())
        .unwrap();
    assert!(
        rows.supports_server_column_batches(),
        "cold identity projection must advertise typed batches"
    );
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: Some("protocol_typed_boundary".into()),
        selected_database: Some(database),
        cursor: Some(SessionCursor {
            id: 71,
            rows,
            pending_row_batch: None,
            pending_row_columns: None,
            pending_column_batch: None,
            typed_batches_started: false,
            row_typed_batches_started: false,
        }),
        transaction: None,
        next_cursor_id: 72,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: true,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let config = test_config(dir.path().join("server-data"));

    crate::storage::instrumentation::begin_row_materialization_probe();
    let first = fetch_column_batch(&mut session, 71, &config);
    let terminal = fetch_column_batch(&mut session, 71, &config);
    let materialization = crate::storage::instrumentation::end_row_materialization_probe();

    let ServerResponse::Encoded { payload } = first else {
        panic!("supported typed batch should be emitted as prepared ColumnBatch");
    };
    let (message, consumed): (ServerMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard()).unwrap();
    assert_eq!(consumed, payload.len());
    let ServerMessage::ColumnBatch {
        columns,
        row_count,
        eof,
        ..
    } = message
    else {
        panic!("supported typed path must use ColumnBatch");
    };
    assert_eq!(row_count, 32);
    assert!(!eof);
    assert_eq!(columns.len(), 2);

    let ServerResponse::Encoded { payload } = terminal else {
        panic!("terminal typed batch should be emitted as prepared ColumnBatch");
    };
    let (message, consumed): (ServerMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard()).unwrap();
    assert_eq!(consumed, payload.len());
    assert!(matches!(
        message,
        ServerMessage::ColumnBatch {
            row_count: 0,
            eof: true,
            ..
        }
    ));
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn legacy_row_batch_uses_typed_storage_without_value_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}/protocol_row_typed_boundary",
        dir.path().display()
    );
    let database = Database::open(&dsn).unwrap();
    database
        .execute(
            "CREATE TABLE protocol_row_typed_boundary (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )",
            (),
        )
        .unwrap();
    for id in 1..=32 {
        let sql = format!(
            "INSERT INTO protocol_row_typed_boundary (id, name)
                 VALUES ({id}, '{}')",
            if id % 2 == 0 { "north" } else { "south" }
        );
        database.execute(&sql, ()).unwrap();
    }
    database.execute("PRAGMA CHECKPOINT", ()).unwrap();

    let rows = database
        .query("SELECT id, name FROM protocol_row_typed_boundary", ())
        .unwrap();
    assert!(rows.supports_server_column_batches());
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: Some("protocol_row_typed_boundary".into()),
        selected_database: Some(database),
        cursor: Some(SessionCursor {
            id: 81,
            rows,
            pending_row_batch: None,
            pending_row_columns: None,
            pending_column_batch: None,
            typed_batches_started: false,
            row_typed_batches_started: false,
        }),
        transaction: None,
        next_cursor_id: 82,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: true,
        build_identity_v1: false,
        external_value_v1: false,
    };
    let mut config = test_config(dir.path().join("server-data"));
    config.cursor_batch_max_rows = 16;

    crate::storage::instrumentation::begin_row_materialization_probe();
    let first = fetch_cursor(&mut session, 81, &config);
    assert!(
        session
            .cursor
            .as_ref()
            .expect("terminal batch has not been requested")
            .row_typed_batches_started
    );
    assert!(matches!(
        fetch_column_batch(&mut session, 81, &config),
        ServerResponse::Message(ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::CommandsOutOfSync,
            ..
        }))
    ));
    let terminal = fetch_cursor(&mut session, 81, &config);
    let materialization = crate::storage::instrumentation::end_row_materialization_probe();

    let ServerResponse::Encoded { payload } = first else {
        panic!("legacy Fetch should retain a prepared RowBatch payload")
    };
    let (message, consumed): (ServerMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard()).unwrap();
    assert_eq!(consumed, payload.len());
    let ServerMessage::RowBatch { rows, eof, .. } = message else {
        panic!("legacy Fetch must keep the RowBatch wire contract")
    };
    assert_eq!(rows.len(), 16);
    assert!(!eof);
    assert_eq!(
        rows[0].values,
        vec![WireValue::Int(1), WireValue::String("south".into())]
    );
    assert_eq!(
        rows[15].values,
        vec![WireValue::Int(16), WireValue::String("north".into())]
    );

    let ServerResponse::Encoded { payload } = terminal else {
        panic!("legacy Fetch terminal batch should also retain its payload")
    };
    let (message, consumed): (ServerMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard()).unwrap();
    assert_eq!(consumed, payload.len());
    let ServerMessage::RowBatch { rows, eof, .. } = message else {
        panic!("legacy Fetch must keep the RowBatch wire contract")
    };
    assert_eq!(rows.len(), 16);
    assert!(eof);
    assert_eq!(
        rows[15].values,
        vec![WireValue::Int(32), WireValue::String("north".into())]
    );
    assert!(session.cursor.is_none());
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn close_cursor_drops_a_pending_column_batch() {
    crate::storage::instrumentation::begin_protocol_pending_column_batch_probe();
    let database = Database::open("memory://").unwrap();
    let rows = database.query("SELECT 1", ()).unwrap();
    let mut session = ReadySession {
        principal_id: ObjectId::BOOTSTRAP_OWNER,
        authenticated_database: None,
        selected_database_name: None,
        selected_database: None,
        cursor: Some(SessionCursor {
            id: 44,
            rows,
            pending_row_batch: None,
            pending_row_columns: None,
            pending_column_batch: Some(PendingColumnBatch::new(
                vec![WireColumn::Int64 {
                    values: vec![1, 2],
                    nulls: vec![false, false],
                }],
                2,
            )),
            typed_batches_started: true,
            row_typed_batches_started: false,
        }),
        transaction: None,
        next_cursor_id: 45,
        prepared: BTreeMap::new(),
        next_statement_id: 1,
        column_batch_v1: true,
        build_identity_v1: false,
        external_value_v1: false,
    };

    assert!(matches!(
        close_cursor(&mut session, 44),
        ServerMessage::CursorClosed { cursor_id: 44 }
    ));
    assert!(session.cursor.is_none());
    let probe = crate::storage::instrumentation::end_protocol_pending_column_batch_probe();
    assert_eq!(probe.opened, 1);
    assert_eq!(probe.completed, 0);
    assert_eq!(probe.dropped, 1);
    assert_eq!(probe.current, 0);
    assert_eq!(probe.rows_current, 0);
    assert_eq!(probe.bytes_current, 0);
    assert_eq!(probe.max, 1);
    assert_eq!(probe.rows_max, 2);
    assert!(probe.bytes_max > 0);
}

#[test]
fn typed_column_cursor_idle_timeout_closes_session_cleanly_with_pending_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    config.cursor_batch_max_bytes = 256;
    config.max_frame_bytes = 512;
    let databases = Mutex::new(BTreeMap::new());

    let mut input = Vec::new();
    push_client_message(
        &mut input,
        ClientMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: vec![ProtocolCapability::ColumnBatchV1],
        },
    );
    push_client_message(
        &mut input,
        ClientMessage::Authenticate {
            login: "root".into(),
            password: None,
        },
    );
    push_client_message(
        &mut input,
        ClientMessage::SelectDatabase {
            database: "typed_timeout".into(),
        },
    );
    push_execute(
        &mut input,
        "CREATE TABLE timeout_batch (id INTEGER PRIMARY KEY, name TEXT)",
    );
    for id in 1..=64 {
        push_execute(
            &mut input,
            format!(
                "INSERT INTO timeout_batch (id, name) VALUES ({id}, '{}')",
                if id % 2 == 0 { "north" } else { "south" }
            ),
        );
    }
    push_execute(&mut input, "PRAGMA CHECKPOINT");
    push_client_message(&mut input, ClientMessage::Fetch { cursor_id: 1 });
    push_execute(&mut input, "SELECT id, name FROM timeout_batch");
    push_client_message(&mut input, ClientMessage::FetchColumnBatch { cursor_id: 2 });

    crate::storage::instrumentation::begin_protocol_pending_column_batch_probe();
    let mut stream = TimeoutAfterInputStream::new(input);
    let mut phases = Vec::new();
    serve_connection_with_databases_and_limits(&mut stream, &config, &databases, |_, phase| {
        phases.push(phase);
        Ok(())
    })
    .expect("idle timeout should close the typed cursor session cleanly");
    let probe = crate::storage::instrumentation::end_protocol_pending_column_batch_probe();

    assert!(stream.timed_out, "fixture must end through a read timeout");
    assert!(phases.contains(&SessionReadPhase::Idle));
    assert!(phases.contains(&SessionReadPhase::FramePayload));
    assert_eq!(probe.opened, 1);
    assert_eq!(probe.completed, 0);
    assert_eq!(probe.dropped, 1);
    assert_eq!(probe.current, 0);
    assert_eq!(probe.rows_current, 0);
    assert_eq!(probe.bytes_current, 0);
    assert_eq!(probe.max, 1);
    assert_eq!(probe.rows_max, 64);
    assert!(probe.bytes_max > 0);

    let responses = decode_server_messages(stream.output());
    assert!(
        !responses
            .iter()
            .any(|message| matches!(message, ServerMessage::Error(_))),
        "timeout fixture should not receive server errors: {responses:#?}"
    );
    let column_batches = responses
        .iter()
        .filter_map(|message| match message {
            ServerMessage::ColumnBatch {
                columns,
                row_count,
                eof,
                ..
            } => Some((columns, *row_count, *eof)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(column_batches.len(), 1);
    let (columns, row_count, eof) = column_batches[0];
    assert!(
        !eof,
        "client timed out before requesting the terminal batch"
    );
    assert!(
        row_count > 0 && row_count < 64,
        "configured byte limit must leave a pending typed remainder, got {row_count} rows"
    );
    assert_eq!(columns.len(), 2);
}

#[test]
fn oversized_column_batch_is_split_without_losing_order_or_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));

    let row_count = 32_usize;
    let expected_values = (0..row_count).map(|value| value as i64).collect::<Vec<_>>();
    let expected_ids = (0..row_count)
        .map(|value| (value % 2) as u32)
        .collect::<Vec<_>>();
    let expected_nulls = (0..row_count)
        .map(|value| value % 7 == 0)
        .collect::<Vec<_>>();
    crate::storage::instrumentation::begin_protocol_pending_column_batch_probe();
    let mut pending = PendingColumnBatch::new(
        vec![
            WireColumn::Int64 {
                values: expected_values.clone(),
                nulls: expected_nulls.clone(),
            },
            WireColumn::DictionaryText {
                ids: expected_ids.clone(),
                dictionary: vec!["north".into(), "south".into()],
                nulls: expected_nulls.clone(),
            },
        ],
        row_count,
    );
    let full_len = encode_payload(&ServerMessage::ColumnBatch {
        cursor_id: 91,
        columns: pending.columns.clone(),
        row_count: row_count as u32,
        eof: false,
    })
    .unwrap()
    .len();
    let one_row_len = encode_payload(&ServerMessage::ColumnBatch {
        cursor_id: 91,
        columns: slice_wire_columns(&pending.columns, 0, 1),
        row_count: 1,
        eof: false,
    })
    .unwrap()
    .len();
    assert!(one_row_len < full_len);
    let frame_limit = (one_row_len + full_len) / 2;
    config.cursor_batch_max_bytes = frame_limit;
    config.max_frame_bytes = frame_limit as u32;

    let mut actual_values = Vec::new();
    let mut actual_ids = Vec::new();
    let mut actual_nulls = Vec::new();
    let mut batches = 0_usize;
    while !pending.is_complete() {
        let remaining_before = pending.remaining_rows();
        let (response, emitted_rows) =
            encode_next_pending_column_batch(&mut pending, 91, &config).unwrap();
        assert!(emitted_rows > 0);
        assert!(emitted_rows <= remaining_before);
        let ServerResponse::Encoded { payload } = response else {
            panic!("a bounded pending range must be emitted as a prepared frame");
        };
        assert!(payload.len() <= config.cursor_batch_max_bytes);
        assert!(payload.len() <= config.max_frame_bytes as usize);
        let (message, consumed): (ServerMessage, usize) =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard()).unwrap();
        assert_eq!(consumed, payload.len());
        let ServerMessage::ColumnBatch {
            cursor_id,
            columns,
            row_count,
            eof,
        } = message
        else {
            panic!("pending range must use ColumnBatch");
        };
        assert_eq!(cursor_id, 91);
        assert_eq!(row_count as usize, emitted_rows);
        assert!(!eof);
        let WireColumn::Int64 { values, nulls } = &columns[0] else {
            panic!("first column must retain its integer layout");
        };
        let WireColumn::DictionaryText {
            ids,
            nulls: text_nulls,
            ..
        } = &columns[1]
        else {
            panic!("second column must retain dictionary layout");
        };
        assert_eq!(nulls, text_nulls);
        actual_values.extend_from_slice(values);
        actual_ids.extend_from_slice(ids);
        actual_nulls.extend_from_slice(nulls);
        batches += 1;
    }

    assert!(batches > 1, "the fixture must exercise splitting");
    assert_eq!(actual_values, expected_values);
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(actual_nulls, expected_nulls);
    drop(pending);
    let probe = crate::storage::instrumentation::end_protocol_pending_column_batch_probe();
    assert_eq!(probe.opened, 1);
    assert_eq!(probe.completed, 1);
    assert_eq!(probe.dropped, 0);
    assert_eq!(probe.current, 0);
    assert_eq!(probe.rows_current, 0);
    assert_eq!(probe.bytes_current, 0);
    assert_eq!(probe.max, 1);
    assert_eq!(probe.rows_max, row_count as u64);
    assert!(probe.bytes_max > 0);
}

#[test]
fn typed_batch_sizing_pass_matches_the_exact_wire_payload() {
    let row_count = 140_usize;
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for row in 0..row_count {
        let length = row % 19;
        let offset = bytes.len() as u64;
        bytes.extend(std::iter::repeat_n((row % 251) as u8, length));
        offsets.push((offset, length as u64));
    }
    let nulls = (0..row_count).map(|row| row % 11 == 0).collect::<Vec<_>>();
    let columns = vec![
        WireColumn::Int64 {
            values: (0..row_count).map(|row| row as i64 - 70).collect(),
            nulls: nulls.clone(),
        },
        WireColumn::Float64 {
            values: (0..row_count).map(|row| row as f64 / 3.0).collect(),
            nulls: nulls.clone(),
        },
        WireColumn::Boolean {
            values: (0..row_count).map(|row| row % 2 == 0).collect(),
            nulls: nulls.clone(),
        },
        WireColumn::TimestampNanos {
            values: (0..row_count).map(|row| row as i64 * 1_000).collect(),
            nulls: nulls.clone(),
        },
        WireColumn::DictionaryText {
            ids: (0..row_count).map(|row| (row % 3) as u32).collect(),
            dictionary: vec!["north".into(), "south".into(), "west".into()],
            nulls: nulls.clone(),
        },
        WireColumn::Bytes {
            data: bytes,
            offsets,
            nulls,
        },
    ];

    for start in [0_usize, 63] {
        let empty = ServerMessage::ColumnBatch {
            cursor_id: 99,
            columns: slice_wire_columns(&columns, start, start),
            row_count: 0,
            eof: false,
        };
        let mut estimated = encoded_payload_len(&empty).unwrap();
        let mut byte_payload_lengths = vec![0_usize; columns.len()];
        for take in 0..(row_count - start) {
            let row = start + take;
            estimated += wire_encoded_len(&((take + 1) as u32)).unwrap()
                - wire_encoded_len(&(take as u32)).unwrap();
            for (index, column) in columns.iter().enumerate() {
                estimated += wire_column_row_encoded_delta(
                    column,
                    row,
                    take,
                    take + 1,
                    byte_payload_lengths[index],
                )
                .unwrap();
                if let Some(length) = wire_byte_column_row_length(column, row).unwrap() {
                    byte_payload_lengths[index] += length;
                }
            }
            let actual = encoded_payload_len(&ServerMessage::ColumnBatch {
                cursor_id: 99,
                columns: slice_wire_columns(&columns, start, row + 1),
                row_count: (take + 1) as u32,
                eof: false,
            })
            .unwrap();
            assert_eq!(estimated, actual, "start={start} take={}", take + 1);
        }
    }
}

#[test]
fn oversized_single_typed_row_fails_without_emitting_a_partial_frame() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("server-data"));
    config.cursor_batch_max_bytes = 96;
    config.max_frame_bytes = 96;
    let mut pending = PendingColumnBatch::new(
        vec![WireColumn::DictionaryText {
            ids: vec![0],
            dictionary: vec!["x".repeat(256)],
            nulls: vec![false],
        }],
        1,
    );

    let error = match encode_next_pending_column_batch(&mut pending, 1, &config) {
        Ok(_) => panic!("an oversized single row must not produce a partial frame"),
        Err(error) => error,
    };
    assert!(error.contains("single typed row exceeds"));
    assert_eq!(pending.remaining_rows(), 1);
}
