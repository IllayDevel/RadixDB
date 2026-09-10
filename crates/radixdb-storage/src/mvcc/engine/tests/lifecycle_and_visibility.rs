#[test]
fn test_engine_creation() {
    let engine = MVCCEngine::in_memory();
    assert!(!engine.is_open());
    assert_eq!(engine.get_path(), "memory://");
}

#[test]
fn test_engine_open_close() {
    let engine = MVCCEngine::in_memory();

    engine.open_engine().unwrap();
    assert!(engine.is_open());

    engine.close_engine().unwrap();
    assert!(!engine.is_open());
}

#[test]
fn test_close_engine_waits_for_active_transaction_before_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("close_waits_for_active_tx");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.checkpoint_interval = 0;

    let engine = std::sync::Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let mut tx = engine.begin_transaction().unwrap();
    {
        let mut table = tx.get_table("items").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(1),
                Value::text("kept"),
            ]))
            .unwrap();
    }

    let close_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let close_done_worker = std::sync::Arc::clone(&close_done);
    let engine_worker = std::sync::Arc::clone(&engine);
    let closer = std::thread::spawn(move || {
        engine_worker.close_engine().unwrap();
        close_done_worker.store(true, std::sync::atomic::Ordering::Release);
    });

    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        !close_done.load(std::sync::atomic::Ordering::Acquire),
        "close_engine must wait for the active transaction before final checkpoint"
    );

    tx.commit().unwrap();
    closer.join().unwrap();
    assert!(close_done.load(std::sync::atomic::Ordering::Acquire));
    assert!(!engine.is_open());

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let rows = collect_rows(&reopened, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::text("kept")));
    reopened.close_engine().unwrap();
}

#[test]
fn test_r2_l02_batch_b_uncertain_commit_is_terminal_and_visible() {
    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();
    crate::test_failpoints::reset_all();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("uncertain_commit");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.sync_mode = crate::SyncMode::Normal;
    config.persistence.sync_interval_ms = 60_000;
    config.persistence.checkpoint_interval = 0;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let mut tx = engine.begin_transaction().unwrap();
    tx.get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("visible"),
        ]))
        .unwrap();

    crate::test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);
    let outcome = tx.commit();
    crate::test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);
    assert!(matches!(outcome, Err(Error::WalDurabilityUncertain { .. })));
    assert!(matches!(tx.rollback(), Err(Error::TransactionClosed)));

    let reader = engine.begin_transaction().unwrap();
    let table = reader.get_table("items").unwrap();
    let mut scanner = table.scan(&[0, 1], None).unwrap();
    assert!(
        scanner.next(),
        "uncertain marker must not cause a live rollback"
    );
    assert_eq!(scanner.take_row().get(0), Some(&Value::Integer(1)));
    assert!(!scanner.next());
    scanner.close().unwrap();
    drop(table);
    drop(reader);

    engine.close_engine().unwrap();
    crate::test_failpoints::reset_all();
}

#[test]
fn rollback_reports_abort_marker_write_failure() {
    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();
    crate::test_failpoints::reset_all();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rollback-marker-failure");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.sync_mode = crate::SyncMode::Normal;
    config.persistence.checkpoint_interval = 0;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let mut tx = engine.begin_transaction().unwrap();
    tx.get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("aborted"),
        ]))
        .unwrap();

    crate::test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
    let rollback = tx.rollback();
    crate::test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);
    assert!(
        rollback
            .as_ref()
            .is_err_and(|error| error.to_string().contains("WAL rollback record")),
        "rollback must expose abort-marker failure: {rollback:?}"
    );
    assert!(matches!(tx.rollback(), Err(Error::TransactionClosed)));
    assert!(collect_rows(&engine, "items").is_empty());

    engine.close_engine().unwrap();
    crate::test_failpoints::reset_all();
}

#[test]
fn test_r2_l02_batch_b_close_failure_is_retryable() {
    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();
    crate::test_failpoints::reset_all();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("retryable_close");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    crate::test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);
    let first = engine.close_engine();
    crate::test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);
    assert!(first.is_err());
    assert!(matches!(
        &*engine.lifecycle.read().unwrap(),
        EngineLifecycleState::CloseFailed(error)
            if error.to_string() == first.as_ref().unwrap_err().to_string()
    ));

    engine.close_engine().unwrap();
    assert!(matches!(
        &*engine.lifecycle.read().unwrap(),
        EngineLifecycleState::Closed
    ));
    crate::test_failpoints::reset_all();
}

#[test]
fn test_engine_create_table() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("users")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();

    let created = create_catalog_test_table(&engine, schema).unwrap();
    assert_eq!(created.table_name, "users");
    assert_ne!(created.catalog_id(), [0; 16]);
    assert_eq!(created.constraints()[0].id, 1);
    assert_eq!(created.constraints()[0].name, "pk_users");

    // Table should exist
    assert!(engine.table_exists("users").unwrap());
    assert!(engine.table_exists("USERS").unwrap()); // Case insensitive

    engine.close_engine().unwrap();
}

#[test]
fn test_engine_drop_table() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("temp")
        .column("id", DataType::Integer, false, true)
        .build();

    create_catalog_test_table(&engine, schema).unwrap();
    assert!(engine.table_exists("temp").unwrap());

    drop_catalog_test_table(&engine, "temp").unwrap();
    assert!(!engine.table_exists("temp").unwrap());

    engine.close_engine().unwrap();
}

#[test]
fn test_engine_duplicate_table_error() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("dup")
        .column("id", DataType::Integer, false, true)
        .build();

    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let result = create_catalog_test_table(&engine, schema);
    assert!(result.is_err());

    engine.close_engine().unwrap();
}

#[test]
fn test_engine_begin_transaction() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let txn = engine.begin_transaction();
    assert!(txn.is_ok());

    let mut txn = txn.unwrap();
    assert!(txn.id() > 0);

    txn.rollback().unwrap();
    engine.close().unwrap();
}

#[test]
fn test_engine_transaction_create_table() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let mut txn = engine.begin_transaction().unwrap();

    // Create table through transaction
    let schema = SchemaBuilder::new("txn_table")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Text, true, false)
        .build();

    let table = txn.create_table("txn_table", schema.clone()).unwrap();
    assert_eq!(table.name(), "txn_table");
    assert!(matches!(txn.commit(), Err(Error::NotSupported(_))));
    assert!(!engine.table_exists("txn_table").unwrap());

    create_catalog_test_table(&engine, schema).unwrap();
    assert!(engine.table_exists("txn_table").unwrap());
    engine.close().unwrap();
}

#[test]
fn transactional_create_table_validates_schema_and_name_before_reservation() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let mut txn = engine.begin_transaction().unwrap();
    let mismatched = SchemaBuilder::new("schema_name")
        .add_primary_key("id", DataType::Integer)
        .build();
    assert!(txn.create_table("requested_name", mismatched).is_err());

    let invalid = SchemaBuilder::new("invalid")
        .add_primary_key("id", DataType::Integer)
        .add_primary_key("other_id", DataType::Integer)
        .build();
    assert!(txn.create_table("invalid", invalid).is_err());
    assert!(engine.get_table_schema("requested_name").is_err());
    assert!(engine.get_table_schema("invalid").is_err());

    txn.rollback().unwrap();
    engine.close().unwrap();
}

#[test]
fn test_engine_transaction_insert_and_select() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    // Create table
    let schema = SchemaBuilder::new("data")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    // Insert data in transaction
    let mut txn = engine.begin_transaction().unwrap();
    let mut table = txn.get_table("data").unwrap();

    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("Alice"),
        ]))
        .unwrap();

    table
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("Bob"),
        ]))
        .unwrap();

    // Scan to verify
    let mut scanner = table.scan(&[0, 1], None).unwrap();
    let mut count = 0;
    while scanner.next() {
        count += 1;
    }
    assert_eq!(count, 2);

    txn.commit().unwrap();
    engine.close().unwrap();
}



#[test]
fn test_engine_isolation_level() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    // Default should be ReadCommitted
    assert_eq!(engine.get_isolation_level(), IsolationLevel::ReadCommitted);

    // Set to Snapshot
    engine
        .set_isolation_level(IsolationLevel::SnapshotIsolation)
        .unwrap();
    assert_eq!(
        engine.get_isolation_level(),
        IsolationLevel::SnapshotIsolation
    );

    engine.close().unwrap();
}

#[test]
fn test_engine_get_version_store() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("versioned")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let store = engine.get_version_store("versioned");
    assert!(store.is_ok());

    let store = engine.get_version_store("nonexistent");
    assert!(store.is_err());

    engine.close_engine().unwrap();
}

#[test]
fn test_engine_get_table_schema() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let schema = SchemaBuilder::new("test_schema")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let retrieved = engine.get_table_schema("test_schema").unwrap();
    assert_eq!(retrieved.columns.len(), 2);
    assert_eq!(retrieved.columns[0].name, "id");

    // Non-existent table
    assert!(engine.get_table_schema("nonexistent").is_err());

    engine.close().unwrap();
}

#[test]
fn test_engine_transaction_with_isolation_level() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let txn = engine.begin_transaction_with_level(IsolationLevel::SnapshotIsolation);
    assert!(txn.is_ok());

    let mut txn = txn.unwrap();
    txn.rollback().unwrap();

    engine.close().unwrap();
}

#[test]
fn test_engine_path() {
    let engine = MVCCEngine::in_memory();
    assert!(engine.path().is_none());

    let config = Config::with_path("/tmp/test.db");
    let engine = MVCCEngine::new(config);
    assert_eq!(engine.path(), Some("/tmp/test.db"));
}

#[test]
fn test_engine_create_snapshot() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    // A memory engine has no durable snapshot destination; the public
    // operation must fail explicitly instead of reporting a false success.
    assert!(matches!(
        engine.create_snapshot(),
        Err(Error::InvalidArgument(message))
            if message == "SNAPSHOT requires a persistent database"
    ));

    engine.close().unwrap();
}

#[test]
fn test_restart_merges_snapshot_and_newer_standalone_volume_without_duplicates() {
    fn count_rows(engine: &MVCCEngine, table_name: &str) -> i64 {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table(table_name).unwrap();
        let mut scanner = table.scan(&[0], None).unwrap();
        let mut count = 0i64;
        while scanner.next() {
            count += 1;
        }
        count
    }

    fn pseudo_random_payload(seed: i64, chunks: usize) -> String {
        let mut state = seed as u64 ^ 0x9E37_79B9_7F4A_7C15;
        let mut payload = String::with_capacity(chunks * 16);
        for _ in 0..chunks {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            payload.push_str(&format!("{:016x}", state));
        }
        payload
    }

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("volume_restart_dedupe");
    let db_path_str = db_path.to_string_lossy().to_string();

    let config = Config::with_path(&db_path_str);
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("note", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        for i in 0..100_000i64 {
            table
                .insert(Row::from_values(vec![
                    Value::Integer(i),
                    Value::text(pseudo_random_payload(i, 16)),
                ]))
                .unwrap();
        }
        tx.commit().unwrap();
    }

    // Checkpoint cycle: seals hot rows into frozen volumes and persists manifests.
    engine.checkpoint_cycle().unwrap();
    assert!(
        db_path.join("CONTROL.0").exists() || db_path.join("CONTROL.1").exists(),
        "checkpoint must publish one V6 CONTROL slot"
    );
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let manifest = mgr.manifest();
        assert!(
            !manifest.segments.is_empty(),
            "expected manifest segments after checkpoint publish"
        );
        assert!(
            manifest
                .segments
                .iter()
                .all(|segment| !segment.file_path.as_os_str().is_empty()),
            "checkpoint publish must persist durable volume paths in manifest"
        );
    }

    assert_table_segments_artifact_backed(&engine, "items");
    assert_eq!(count_rows(&engine, "items"), 100_000);
    let runtime = engine.runtime_stats_snapshot();
    assert!(runtime.maintenance.seal.completed >= 1);
    assert_eq!(runtime.maintenance.seal.failed, 0);
    assert!(runtime.maintenance.seal.last_input_rows >= 100_000);
    assert!(runtime.maintenance.seal.last_output_rows >= 100_000);
    assert!(runtime.maintenance.seal.last_output_bytes > 0);
    assert!(runtime.maintenance.checkpoint.completed >= 1);
    assert_eq!(runtime.maintenance.checkpoint.failed, 0);
    assert!(runtime.maintenance.checkpoint.last_result_marker > 0);
    assert!(runtime.maintenance.checkpoint.last_result_marker <= runtime.wal_current_lsn);
    assert_eq!(
        runtime.maintenance.compaction.completed, 0,
        "checkpoint durability must not wait for compaction"
    );
    assert!(engine.compaction_requested.load(Ordering::Acquire));
    assert!(runtime.wal_running);
    assert!(runtime.wal_max_file_bytes > 0);

    engine.close_engine().unwrap();

    let reopen_config = Config::with_path(&db_path_str);
    let reopened = MVCCEngine::new(reopen_config);
    reopened.open_engine().unwrap();
    {
        let mgrs = reopened.segment_managers.read().unwrap();
        let mgr = mgrs
            .get("items")
            .expect("items segment manager after reopen");
        let segments = mgr.get_segments_ordered_meta();
        assert!(
            !segments.is_empty(),
            "expected registered cold segments after reopen"
        );
        let manifest = mgr.manifest();
        assert!(
            manifest
                .segments
                .iter()
                .all(|segment| !segment.file_path.as_os_str().is_empty()),
            "reopened manifest must retain durable volume paths"
        );
        assert!(
            segments.iter().all(|segment| segment.is_cold()),
            "startup must register metadata-only artifact volumes"
        );
        assert!(
            segments
                .iter()
                .all(|segment| segment.artifact_source().is_some()),
            "startup must attach DATA sources without materializing payload bodies"
        );
    }
    assert_eq!(count_rows(&reopened, "items"), 100_000);
    reopened.close_engine().unwrap();
}

#[test]
fn r4_l02_batch_a_cold_backfill_flat_rename_and_type_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("r4_l02_batch_a");
    let mut config = Config::with_path(db_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = 9999;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("active", DataType::Boolean, false, false)
        .column("score", DataType::Integer, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();
    // This index exists before sealing, so artifact-backed owns its complete cold
    // postings. Recovery must use that durable coverage instead of
    // rebuilding the same rows into the hot in-memory index.
    commit_test_index(
        &engine,
        "items",
        "idx_score",
        &["score"],
        false,
        Some(IndexType::BTree),
    )
    .unwrap();
    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(1),
                Value::text("alice"),
                Value::Boolean(true),
                Value::Integer(30),
            ]))
            .unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(2),
                Value::text("bob"),
                Value::Boolean(false),
                Value::Integer(45),
            ]))
            .unwrap();
        tx.commit().unwrap();
    }
    engine.checkpoint_cycle_inner(true).unwrap();
    assert_table_segments_artifact_backed(&engine, "items");

    let definitions = [
        ("idx_name", vec!["name"], IndexType::Hash),
        ("idx_active", vec!["active"], IndexType::Bitmap),
        (
            "idx_name_score",
            vec!["name", "score"],
            IndexType::MultiColumn,
        ),
    ];
    for (name, columns, index_type) in &definitions {
        commit_test_index(
            &engine,
            "items",
            name,
            columns.as_slice(),
            false,
            Some(*index_type),
        )
        .unwrap();
    }
    {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();

        assert!(
            table
                .get_index("idx_name")
                .unwrap()
                .get_row_ids_equal(&[Value::text("alice")])
                .unwrap()
                .is_empty(),
            "CREATE INDEX after seal must release its temporary cold runtime backfill"
        );
        assert_eq!(
            table
                .collect_row_ids_by_index_values("name", &[Value::text("alice")])
                .expect("published INDEX should own the cold exact path")
                .unwrap(),
            vec![1],
            "CREATE INDEX after seal must publish complete cold artifact coverage"
        );

        drop(tx);
        rename_catalog_test_index(&engine, "items", "idx_name", "idx_name_v2").unwrap();
        rename_catalog_test_index(&engine, "items", "idx_name_v2", "idx_name_v3").unwrap();
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();
        let renamed = table.get_index("idx_name_v3").unwrap();
        let inner = renamed
            .metadata_inner()
            .expect("one metadata wrapper should own the renamed index");
        assert!(
            inner.metadata_inner().is_none(),
            "repeated rename must replace rather than nest wrappers"
        );
    }

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(3),
                Value::text("charlie"),
                Value::Boolean(true),
                Value::Integer(60),
            ]))
            .unwrap();
        tx.commit().unwrap();
    }
    engine.checkpoint_cycle_inner(true).unwrap();
    {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();
        assert!(
            table
                .get_index("idx_name_v3")
                .unwrap()
                .get_row_ids_equal(&[Value::text("charlie")])
                .unwrap()
                .is_empty(),
            "a later seal must release hot entries once its INDEX artifact is published"
        );
        assert_eq!(
            table
                .collect_row_ids_by_index_values("name", &[Value::text("charlie")])
                .expect("later seal should preserve the cold exact path")
                .unwrap(),
            vec![3],
            "a later seal must publish the row into immutable INDEX coverage"
        );
    }

    let types = engine.list_table_indexes("items").unwrap();
    assert_eq!(types.get("idx_name_v3").map(String::as_str), Some("Hash"));
    assert_eq!(types.get("idx_active").map(String::as_str), Some("Bitmap"));
    assert_eq!(types.get("idx_score").map(String::as_str), Some("BTree"));
    assert_eq!(
        types.get("idx_name_score").map(String::as_str),
        Some("MultiColumn")
    );
    engine.close_engine().unwrap();

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    assert_table_segments_artifact_backed(&engine, "items");
    let types = engine.list_table_indexes("items").unwrap();
    assert_eq!(types.get("idx_name_v3").map(String::as_str), Some("Hash"));
    assert_eq!(types.get("idx_active").map(String::as_str), Some("Bitmap"));
    assert_eq!(types.get("idx_score").map(String::as_str), Some("BTree"));
    assert!(
        engine
            .get_index("items", "idx_score")
            .unwrap()
            .get_row_ids_equal(&[Value::Integer(30)])
            .unwrap()
            .is_empty(),
        "persisted-covered ordinary indexes must not be rebuilt into hot memory at startup"
    );
    {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();
        assert_eq!(
            table
                .collect_row_ids_by_index_values("score", &[Value::Integer(30)])
                .expect("persisted coverage should admit the exact candidate path")
                .unwrap(),
            vec![1],
            "table-level lookup must union hot state with persisted artifact-backed postings"
        );
    }
    assert_eq!(
        engine
            .get_index("items", "idx_name_v3")
            .unwrap()
            .get_row_ids_equal(&[Value::text("alice")])
            .unwrap()
            .len(),
        0,
        "recovery must not rebuild post-cold ordinary indexes on every startup"
    );
    {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();
        assert_eq!(
            table
                .collect_row_ids_by_index_values("name", &[Value::text("alice")])
                .expect("DDL backfill must survive reopen as an INDEX artifact")
                .unwrap(),
            vec![1],
            "post-cold CREATE INDEX must not fall back to a DATA scan after reopen"
        );
        let rows = table.collect_rows_by_ids(&[1]).unwrap();
        assert_eq!(rows.len(), 1, "fallback must preserve the cold row");
        assert_eq!(rows[0].1.get(1), Some(&Value::text("alice")));
    }
    engine.close_engine().unwrap();
}

#[test]
fn test_unique_index_rejects_duplicate_cold_data() {
    // CREATE UNIQUE INDEX must validate cold volume data for duplicates.
    // If cold data already has duplicate values, the unique index must be rejected.
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("dup", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::text("dup")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(2), Value::text("dup")]),
    );
    engine
        .register_volume("items", Arc::new(builder.finish()))
        .unwrap();

    let result = commit_test_index(&engine, "items", "idx_dup_unique", &["dup"], true, None);
    assert!(
        result.is_err(),
        "unique index creation should fail when cold data has duplicates"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("unique constraint"));
    engine.close_engine().unwrap();
}

#[test]
fn test_unique_index_succeeds_on_distinct_cold_data() {
    // CREATE UNIQUE INDEX should succeed when cold data has no duplicates.
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::text("alice")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(2), Value::text("bob")]),
    );
    engine
        .register_volume("items", Arc::new(builder.finish()))
        .unwrap();

    let result = commit_test_index(&engine, "items", "idx_name_unique", &["name"], true, None);
    assert!(
        result.is_ok(),
        "unique index creation should succeed when cold data is distinct"
    );
    engine.close_engine().unwrap();
}

#[test]
fn test_hnsw_index_on_volume_backed_table_populates_cold() {
    // HNSW indexes must include cold data because vector similarity search
    // cannot fall back to zone maps like B-tree/Hash indexes can.
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("embedding", DataType::Vector, false, false)
        .set_last_vector_dimensions(2)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::vector(vec![1.0, 0.0])]),
    );
    engine
        .register_volume("items", Arc::new(builder.finish()))
        .unwrap();

    commit_test_index(
        &engine,
        "items",
        "idx_embedding",
        &["embedding"],
        false,
        Some(IndexType::Hnsw),
    )
    .unwrap();
    let tx = engine.begin_transaction().unwrap();
    let table = tx.get_table("items").unwrap();

    // HNSW index should include cold data after creation
    let index = table
        .get_index("idx_embedding")
        .expect("hnsw index should exist");
    let results = index
        .search_nearest(&Value::vector(vec![1.0, 0.0]), 1, 32)
        .unwrap_or_default();
    assert_eq!(
        results.len(),
        1,
        "HNSW index should include cold volume rows"
    );

    // Full table scan also returns volume data
    let rows = table.collect_all_rows(None).unwrap();
    assert_eq!(rows.len(), 1, "scan should find volume rows");

    drop(tx);
    engine.close_engine().unwrap();
}

#[test]
fn r8_l01_batch_c_hnsw_complete_graph_skips_cold_vector_blocks() {
    // Same contract as the eager-volume test above, but the cold segment
    // is artifact-backed metadata-only. Index backfill must use the artifact-backed scanner/block
    // source and must not access vol.columns directly.
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("embedding", DataType::Vector, false, false)
        .set_last_vector_dimensions(2)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let _dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(1), Value::vector(vec![1.0, 0.0])]),
        )],
    );

    commit_test_index(
        &engine,
        "items",
        "idx_embedding",
        &["embedding"],
        false,
        Some(IndexType::Hnsw),
    )
    .unwrap();
    let tx = engine.begin_transaction().unwrap();
    let table = tx.get_table("items").unwrap();

    let index = table
        .get_index("idx_embedding")
        .expect("hnsw index should exist");
    let results = index
        .search_nearest(&Value::vector(vec![1.0, 0.0]), 1, 32)
        .unwrap_or_default();
    assert_eq!(
        results.len(),
        1,
        "HNSW index should include artifact-backed metadata-only cold rows"
    );

    drop(tx);
    crate::instrumentation::begin_volume_read_probe();
    engine.populate_hnsw_from_segments().unwrap();
    let reads = crate::instrumentation::end_volume_read_probe();
    assert_eq!(
        reads.calls, 0,
        "complete graph coverage must skip vector blocks"
    );
    engine.close_engine().unwrap();
}

#[test]
fn test_update_and_delete_by_id_on_artifact_metadata_only_segment() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[
            (
                1,
                Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
            ),
        ],
    );

    let mut tx = engine.begin_transaction().unwrap();
    let mut table = tx.get_table("items").unwrap();
    let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(1));
    let updated = table
        .update(Some(&filter), &mut |mut row| {
            row.set(1, Value::Float(99.0))?;
            Ok((row, true))
        })
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(table.delete_by_row_ids(&[2]).unwrap(), 1);
    tx.commit().unwrap();

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::Float(99.0)));

    engine.close_engine().unwrap();
}

#[test]
fn test_cold_delete_records_exactly_one_wal_operation() {
    let db_dir = tempfile::tempdir().unwrap();
    let persistence = crate::config::PersistenceConfig {
        sync_mode: crate::config::SyncMode::Full,
        checkpoint_interval: u32::MAX,
        checkpoint_on_close: false,
        ..Default::default()
    };
    let engine = MVCCEngine::new(
        Config::with_path(db_dir.path().to_string_lossy().into_owned())
            .with_persistence(persistence),
    );
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            7,
            Row::from_values(vec![Value::Integer(7), Value::Float(70.0)]),
        )],
    );

    let mut tx = engine.begin_transaction().unwrap();
    let mut table = tx.get_table("items").unwrap();
    assert_eq!(table.delete_by_row_ids(&[7]).unwrap(), 1);
    drop(table);
    tx.commit().unwrap();

    let persistence = engine
        .persistence()
        .expect("persistent engine must expose its WAL manager");
    let table_id = engine
        .pin_catalog()
        .unwrap()
        .find_relation(radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE, "items")
        .unwrap()
        .unwrap()
        .id();
    let mut matching_deletes = Vec::new();
    persistence
        .replay_two_phase(0, |entry| {
            if entry.operation == WALOperationType::Delete
                && entry.table_id == Some(table_id)
                && entry.row_id == 7
            {
                matching_deletes.push(entry);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        matching_deletes.len(),
        1,
        "one logical cold DELETE must own exactly one durable WAL operation"
    );

    engine.close_engine().unwrap();
}

#[test]
fn test_commit_holds_membership_publication_fence_through_cold_update() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        )],
    );

    let mut writer = engine.begin_transaction().unwrap();
    let mut table = writer.get_table("items").unwrap();
    assert_eq!(
        table
            .update_by_row_ids(&[1], &mut |mut row| {
                row.set(1, Value::Float(99.0))?;
                Ok((row, true))
            })
            .unwrap(),
        1
    );
    drop(table);

    let publication_fence = engine
        .version_stores
        .read()
        .unwrap()
        .get("items")
        .expect("items VersionStore")
        .membership_fence();
    let reader_guard = publication_fence.read();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = writer.commit();
        finished_tx.send(result).unwrap();
    });

    started_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(
        finished_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "commit must wait while a membership reader owns the table fence"
    );
    drop(reader_guard);
    finished_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("commit should continue after membership probe leaves")
        .unwrap();
    handle.join().unwrap();

    let mut tx = engine.begin_transaction().unwrap();
    let table = tx.get_table("items").unwrap();
    let mut matches = [false];
    assert_eq!(table.probe_visible_row_ids(&[1], &mut matches).unwrap(), 1);
    assert_eq!(matches, [true]);
    drop(table);
    tx.rollback().unwrap();

    engine.close_engine().unwrap();
}

#[test]
fn table_handle_created_before_first_checkpoint_tracks_hot_to_cold_publication() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("first-checkpoint-handle");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = 9999;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("value", DataType::Text, false, false)
            .build(),
    )
    .unwrap();
    let mut writer = engine.begin_transaction().unwrap();
    writer
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(7),
            Value::text("before-checkpoint"),
        ]))
        .unwrap();
    writer.commit().unwrap();

    // Capture the same long-lived inner-table handle used by the count-only
    // indexed anti-join before this table owns its first cold segment.
    let mut reader = engine.begin_transaction().unwrap();
    let table = reader.get_table("items").unwrap();
    assert!(
        !engine.get_or_create_segment_manager("items").has_segments(),
        "fixture must start before the first cold publication"
    );

    engine.checkpoint_cycle_inner(true).unwrap();
    assert!(
        engine.get_or_create_segment_manager("items").has_segments(),
        "forced checkpoint must move the fixture into cold storage"
    );

    let mut matches = [false];
    assert_eq!(
        table.probe_visible_row_ids(&[7], &mut matches).unwrap(),
        1,
        "a handle created before first seal must follow the row into cold storage"
    );
    assert_eq!(matches, [true]);

    drop(table);
    reader.rollback().unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn hot_only_delete_discovery_waits_for_first_seal_publication() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("value", DataType::Text, false, false)
            .build(),
    )
    .unwrap();

    let mut writer = engine.begin_transaction().unwrap();
    writer
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(7),
            Value::text("before-first-seal"),
        ]))
        .unwrap();
    writer.commit().unwrap();

    let manager = engine.get_or_create_segment_manager("items");
    assert!(!manager.has_segments(), "fixture must be hot-only");
    let seal_guard = manager.acquire_seal_write();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let worker_engine = Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        let mut tx = worker_engine.begin_transaction().unwrap();
        let table = tx.get_table("items").unwrap();
        started_tx.send(()).unwrap();
        let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(7));
        let candidates = table
            .collect_delete_candidate_row_ids(Some(&filter))
            .unwrap();
        drop(table);
        finished_tx.send(candidates).unwrap();
        tx.rollback().unwrap();
    });

    started_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(
        finished_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "hot-only DELETE discovery must wait for the first seal publication"
    );
    drop(seal_guard);

    let candidates = finished_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("DELETE must continue after the seal fence is released");
    assert_eq!(candidates, vec![7]);
    handle.join().unwrap();
    assert_eq!(collect_rows(&engine, "items").len(), 1);
    engine.close_engine().unwrap();
}

#[test]
fn commit_waiting_for_checkpoint_seal_does_not_block_read_visibility() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .build(),
    )
    .unwrap();

    let mut writer = engine.begin_transaction().unwrap();
    let mut table = writer.get_table("items").unwrap();
    table
        .insert(Row::from_values(vec![Value::Integer(1)]))
        .unwrap();
    drop(table);

    let checkpoint_seal = engine.seal_fence.write();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        finished_tx.send(writer.commit()).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(
        finished_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "commit must wait for the checkpoint seal"
    );
    let reader_visibility = engine
        .visibility_fence
        .try_read_for(std::time::Duration::from_millis(250))
        .expect("commit waiting for seal must not exclude SELECT");
    drop(reader_visibility);
    drop(checkpoint_seal);

    finished_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("commit should publish after checkpoint seal leaves")
        .unwrap();
    handle.join().unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn contended_point_update_does_not_hold_segment_maintenance_fence() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("value", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();

    let mut seed = engine.begin_transaction().unwrap();
    seed.get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(0),
        ]))
        .unwrap();
    seed.commit().unwrap();

    let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(1));
    let mut owner = engine.begin_transaction().unwrap();
    assert_eq!(
        owner
            .get_table("items")
            .unwrap()
            .update(Some(&filter), &mut |mut row| {
                row.set(1, Value::Integer(1))?;
                Ok((row, true))
            })
            .unwrap(),
        1
    );

    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let waiter_engine = Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(1));
        let mut transaction = waiter_engine.begin_transaction().unwrap();
        let mut table = transaction.get_table("items").unwrap();
        started_tx.send(()).unwrap();
        let result = table.update(Some(&filter), &mut |mut row| {
            row.set(1, Value::Integer(2))?;
            Ok((row, true))
        });
        drop(table);
        if result.is_ok() {
            transaction.commit().unwrap();
        } else {
            transaction.rollback().unwrap();
        }
        finished_tx.send(result).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let manager = engine.get_or_create_segment_manager("items");
    let maintenance_guard = manager
        .try_acquire_seal_write_for(Duration::from_millis(40))
        .expect("row-claim waiting must happen before the segment maintenance fence");
    drop(maintenance_guard);
    assert!(
        finished_rx.try_recv().is_err(),
        "the second UPDATE must still be waiting for row ownership"
    );

    owner.rollback().unwrap();
    assert_eq!(
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("contended UPDATE should continue after the owner leaves")
            .unwrap(),
        1
    );
    waiter.join().unwrap();

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::Integer(2)));
    engine.close_engine().unwrap();
}

#[test]
fn test_update_by_id_and_delete_where_on_artifact_metadata_only_segment() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[
            (
                1,
                Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
            ),
            (
                3,
                Row::from_values(vec![Value::Integer(3), Value::Float(30.0)]),
            ),
        ],
    );

    let mut tx = engine.begin_transaction().unwrap();
    let mut table = tx.get_table("items").unwrap();
    let updated = table
        .update_by_row_ids(&[1], &mut |mut row| {
            row.set(1, Value::Float(77.0))?;
            Ok((row, true))
        })
        .unwrap();
    assert_eq!(updated, 1);
    let filter = crate::expression::ComparisonExpr::gt("id", Value::Integer(2));
    assert_eq!(table.delete(Some(&filter)).unwrap(), 1);
    tx.commit().unwrap();

    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::Float(77.0)));
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1.get(1), Some(&Value::Float(20.0)));

    engine.close_engine().unwrap();
}

#[test]
fn uuid_primary_key_cold_updates_do_not_resurrect_predecessors() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("uuid-cold-update");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 16;
    config.persistence.compact_threshold = 2;
    config.cleanup.enabled = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Uuid, false, true)
            .column("value", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();

    let key = Value::uuid([7; 16]);
    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![key.clone(), Value::Integer(0)]))
        .unwrap();
    insert.commit().unwrap();
    crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();

    for expected in 1..=8_i64 {
        let rows = collect_rows(&engine, "items");
        assert_eq!(rows.len(), 1, "duplicate before update {expected}: {rows:?}");
        let row_id = rows[0].0;
        let mut update = engine.begin_transaction().unwrap();
        let mut table = update.get_table("items").unwrap();
        assert_eq!(
            table
                .update_by_row_ids(&[row_id], &mut |mut row| {
                    row.set(1, Value::Integer(expected))?;
                    Ok((row, true))
                })
                .unwrap(),
            1
        );
        drop(table);
        update.commit().unwrap();
        crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();
        engine.compact_after_checkpoint_forced().unwrap();
    }

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1, "only the newest UUID-key row may remain");
    assert_eq!(rows[0].1.get(0), Some(&key));
    assert_eq!(rows[0].1.get(1), Some(&Value::Integer(8)));
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let rows = collect_rows(&reopened, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(0), Some(&key));
    assert_eq!(rows[0].1.get(1), Some(&Value::Integer(8)));
    reopened.close_engine().unwrap();
}

#[test]
fn stale_uuid_primary_key_writer_cannot_create_a_second_owner_after_seal() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("uuid-cold-update-stale-writer");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 16;
    config.persistence.compact_threshold = 2;
    config.cleanup.enabled = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Uuid, false, true)
            .column("value", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();

    let key = Value::uuid([8; 16]);
    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![key.clone(), Value::Integer(0)]))
        .unwrap();
    insert.commit().unwrap();
    crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();

    // Hold an old snapshot while a competing writer replaces the cold UUID row
    // and the replacement itself moves cold. The stale writer must not acquire a
    // second physical row identity for the same logical primary key.
    let mut stale = engine.begin_transaction().unwrap();
    let filter = crate::expression::ComparisonExpr::eq("id", key.clone());
    let mut winner = engine.begin_transaction().unwrap();
    assert_eq!(
        winner
            .get_table("items")
            .unwrap()
            .update(Some(&filter), &mut |mut row| {
                row.set(1, Value::Integer(1))?;
                Ok((row, true))
            })
            .unwrap(),
        1
    );
    winner.commit().unwrap();
    crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();

    let stale_updated = stale
        .get_table("items")
        .unwrap()
        .update(Some(&filter), &mut |mut row| {
            row.set(1, Value::Integer(2))?;
            Ok((row, true))
        })
        .unwrap();
    if stale_updated == 0 {
        stale.rollback().unwrap();
    } else {
        assert_eq!(stale_updated, 1);
        stale.commit().unwrap();
    }
    crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1, "UUID key must retain one owner: {rows:?}");
    engine.close_engine().unwrap();
}

#[test]
fn concurrent_uuid_primary_key_cold_updates_keep_one_logical_row() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("uuid-cold-update-concurrent");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 16;
    config.persistence.compact_threshold = 2;
    config.cleanup.enabled = false;

    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Uuid, false, true)
            .column("value", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();

    let key = Value::uuid([9; 16]);
    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![key.clone(), Value::Integer(0)]))
        .unwrap();
    insert.commit().unwrap();
    crate::traits::Engine::force_checkpoint_cycle(engine.as_ref()).unwrap();

    let workers = 8;
    let start = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let engine = Arc::clone(&engine);
        let start = Arc::clone(&start);
        let key = key.clone();
        handles.push(std::thread::spawn(move || {
            let mut transaction = engine.begin_transaction().unwrap();
            start.wait();
            let filter = crate::expression::ComparisonExpr::eq("id", key);
            let updated = transaction
                .get_table("items")
                .unwrap()
                .update(Some(&filter), &mut |mut row| {
                    let value = row.get(1).and_then(Value::as_int64).unwrap();
                    row.set(1, Value::Integer(value + 1))?;
                    Ok((row, true))
                })?;
            if updated == 0 {
                transaction.rollback()?;
                return Ok(false);
            }
            if updated != 1 {
                transaction.rollback()?;
                return Err(Error::internal(format!("contended UUID update changed {updated} rows")));
            }
            transaction.commit()?;
            Ok(true)
        }));
    }
    start.wait();

    let mut committed = 0_i64;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(true) => committed += 1,
            Ok(false) => {}
            Err(Error::TransactionSerializationConflict { .. })
            | Err(Error::RowLockTimeout { .. }) => {}
            Err(error) => panic!("unexpected concurrent UUID update error: {error}"),
        }
    }
    assert!(committed > 0);

    crate::traits::Engine::force_checkpoint_cycle(engine.as_ref()).unwrap();
    let rows = collect_rows(engine.as_ref(), "items");
    assert_eq!(rows.len(), 1, "UUID key must retain one logical owner: {rows:?}");
    assert_eq!(rows[0].1.get(0), Some(&key));
    assert_eq!(rows[0].1.get(1), Some(&Value::Integer(committed)));
    engine.compact_after_checkpoint_forced().unwrap();
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let rows = collect_rows(&reopened, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(0), Some(&key));
    assert_eq!(rows[0].1.get(1), Some(&Value::Integer(committed)));
    reopened.close_engine().unwrap();
}

#[test]
fn test_unique_constraints_probe_artifact_metadata_only_segment_without_full_load() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[
            (
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("alice")]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("bob")]),
            ),
        ],
    );

    commit_test_index(&engine, "items", "idx_name_unique", &["name"], true, None).unwrap();
    assert_table_segments_artifact_backed(&engine, "items");

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        let err = table
            .insert(Row::from_values(vec![
                Value::Integer(3),
                Value::text("alice"),
            ]))
            .expect_err("duplicate insert must probe artifact-backed cold unique data");
        assert!(err.to_string().contains("unique constraint"));
        tx.rollback().unwrap();
    }
    assert_table_segments_artifact_backed(&engine, "items");

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(1));
        let err = table
            .update(Some(&filter), &mut |mut row| {
                row.set(1, Value::text("bob"))?;
                Ok((row, true))
            })
            .expect_err("duplicate update must probe artifact-backed cold unique data");
        assert!(err.to_string().contains("unique constraint"));
        tx.rollback().unwrap();
    }
    assert_table_segments_artifact_backed(&engine, "items");

    engine.close_engine().unwrap();
}

#[test]
fn test_unique_seal_prebuilds_artifact_cold_index_without_full_column_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("unique_seal_artifact_prebuilt_index");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = crate::volume::column::ROW_GROUP_SIZE * 2;
    config.persistence.compact_threshold = 9999;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    commit_test_index(&engine, "items", "idx_name_unique", &["name"], true, None).unwrap();

    let row_count = crate::volume::column::ROW_GROUP_SIZE + 2;
    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        for i in 0..row_count as i64 {
            table
                .insert(Row::from_values(vec![
                    Value::Integer(i),
                    Value::text(format!("name-{i}")),
                ]))
                .unwrap();
        }
        tx.commit().unwrap();
    }

    engine.checkpoint_cycle_inner(true).unwrap();
    assert_table_segments_artifact_backed(&engine, "items");
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let segments = mgr.get_segments_ordered_meta();
        assert_eq!(segments.len(), 1, "test expects one sealed cold volume");
        assert!(
            segments[0].has_exact_postings(&[1usize]),
            "production seal must persist UNIQUE postings without eager FrozenVolume"
        );
    }

    engine.close_engine().unwrap();

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    assert_table_segments_artifact_backed(&engine, "items");
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let segments = mgr.get_segments_ordered_meta();
        assert_eq!(
            segments.len(),
            1,
            "restart should recover one sealed cold volume"
        );
        assert!(
            segments[0].has_exact_postings(&[1usize]),
            "restart must restore descriptor-backed artifact-backed UNIQUE postings"
        );
    }

    crate::instrumentation::begin_volume_read_probe();
    let err = {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        let err = table
            .insert(Row::from_values(vec![
                Value::Integer(row_count as i64 + 1),
                Value::text("name-1"),
            ]))
            .expect_err("duplicate insert must probe prebuilt cold artifact-backed unique index");
        tx.rollback().unwrap();
        err
    };
    let counters = crate::instrumentation::end_volume_read_probe();
    assert!(err.to_string().contains("unique constraint"));
    assert_eq!(
        counters.calls, 1,
        "prebuilt artifact-backed UNIQUE index should read only the candidate row block; \
             fallback participating-column scan would read all row groups"
    );
    assert_table_segments_artifact_backed(&engine, "items");

    engine.close_engine().unwrap();
}

#[test]
fn test_engine_list_table_indexes() {
    let mut engine = MVCCEngine::in_memory();
    engine.open().unwrap();

    let schema = SchemaBuilder::new("indexed")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    // Should return empty map (no indexes yet)
    let indexes = engine.list_table_indexes("indexed").unwrap();
    assert!(indexes.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_cross_transaction_visibility() {
    // This test simulates the executor pattern: INSERT in one transaction, SELECT in another
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    // Create table
    let schema = SchemaBuilder::new("test_xact")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    // Transaction 1: INSERT
    {
        let mut tx1 = engine.begin_transaction().unwrap();

        let mut table = tx1.get_table("test_xact").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(1),
                Value::text("Alice"),
            ]))
            .unwrap();

        // Commit publishes all touched tables through the shared marker protocol.
        tx1.commit().unwrap();
    }

    // Transaction 2: SELECT (different transaction)
    {
        let tx2 = engine.begin_transaction().unwrap();
        let table = tx2.get_table("test_xact").unwrap();
        let mut scanner = table.scan(&[0, 1], None).unwrap();

        let mut count = 0;
        while scanner.next() {
            count += 1;
        }
        // Should see the committed row from tx1
        assert_eq!(
            count, 1,
            "Transaction 2 should see 1 row committed by Transaction 1"
        );
    }

    engine.close_engine().unwrap();
}

// =========================================================================
// Cleanup Mechanism Tests
// =========================================================================

use crate::CleanupConfig;
use std::time::Duration;

#[test]
fn test_cleanup_config_disabled() {
    let config = Config::in_memory().with_cleanup(CleanupConfig::disabled());
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let engine = Arc::new(engine);

    // start_cleanup should be a no-op when disabled
    engine.start_cleanup();

    // Verify no cleanup handle was set
    let handle = engine.cleanup_handle.lock().unwrap();
    assert!(
        handle.is_none(),
        "Cleanup handle should be None when disabled"
    );
    drop(handle);

    engine.close_engine().unwrap();
}

#[test]
fn pressure_seal_bounds_hot_memory_without_advancing_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("pressure-seal");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;
    config.persistence.seal_hot_bytes_threshold = 4 * 1024;
    config.persistence.seal_incremental_hot_bytes_threshold = 2 * 1024;
    config.cleanup.interval_secs = 3600;

    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();
    engine.start_cleanup();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("payload", DataType::Text, false, false)
            .build(),
    )
    .unwrap();

    let initial_replay_floor = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap()
        .snapshot()
        .control()
        .wal_replay_floor();
    let payload = "x".repeat(256);
    for generation in 0..6_i64 {
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table("items").unwrap();
        for offset in 0..32_i64 {
            let row_id = generation * 32 + offset;
            table
                .insert(Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(&payload),
                ]))
                .unwrap();
        }
        drop(table);
        transaction.commit().unwrap();

        let started = std::time::Instant::now();
        while engine.hot_seal_pressure_exceeded()
            || engine.pressure_seal.requested.load(Ordering::Acquire)
        {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "pressure seal did not drain the committed hot generation"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    let store = engine
        .version_stores
        .read()
        .unwrap()
        .get("items")
        .cloned()
        .unwrap();
    assert_eq!(store.committed_hot_bytes(), 0);
    let manager = engine.get_or_create_segment_manager("items");
    assert!(manager.has_segments());
    assert_eq!(
        engine
            .physical_generation
            .load_full()
            .unwrap()
            .pin()
            .unwrap()
            .snapshot()
            .control()
            .wal_replay_floor(),
        initial_replay_floor,
        "a pressure seal must not impersonate a durability checkpoint"
    );
    assert_eq!(collect_rows(&engine, "items").len(), 192);

    engine.close_engine().unwrap();
    drop(engine);

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert_eq!(collect_rows(&reopened, "items").len(), 192);
    assert_eq!(
        reopened
            .physical_generation
            .load_full()
            .unwrap()
            .pin()
            .unwrap()
            .snapshot()
            .control()
            .wal_replay_floor(),
        initial_replay_floor
    );
    reopened.close_engine().unwrap();
}

#[test]
fn pressure_seal_bounds_aggregate_hot_memory_across_small_tables() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("aggregate-pressure-seal");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;
    config.persistence.seal_hot_bytes_threshold = 8 * 1024;
    config.persistence.seal_incremental_hot_bytes_threshold = 4 * 1024;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let payload = "x".repeat(256);
    let mut table_names = Vec::new();
    for table_index in 0..20 {
        let table_name = format!("items_{table_index}");
        create_catalog_test_table(
            &engine,
            SchemaBuilder::new(&table_name)
                .column("id", DataType::Integer, false, true)
                .column("payload", DataType::Text, false, false)
                .build(),
        )
        .unwrap();
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table(&table_name).unwrap();
        for row_id in 0..16_i64 {
            table
                .insert(Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(&payload),
                ]))
                .unwrap();
        }
        drop(table);
        transaction.commit().unwrap();
        table_names.push(table_name);
    }

    let stores = engine.version_stores.read().unwrap();
    assert!(stores
        .values()
        .all(|store| store.committed_hot_bytes() < 8 * 1024));
    let total_hot_bytes = stores.values().fold(0usize, |total, store| {
        total.saturating_add(store.committed_hot_bytes())
    });
    assert!(total_hot_bytes >= total_hot_soft_threshold(8 * 1024));
    assert!(total_hot_bytes >= total_hot_hard_threshold(8 * 1024));
    drop(stores);
    assert!(engine.hot_seal_pressure_exceeded());

    let mut cycles = 0;
    while engine.hot_seal_pressure_exceeded() {
        let outcome = engine.pressure_seal_cycle().unwrap();
        let pressure_remains = engine.hot_seal_pressure_exceeded();
        engine
            .pressure_seal
            .finish_cycle(outcome.keeps_request_armed(pressure_remains));
        cycles += 1;
        assert!(cycles <= table_names.len());
    }

    assert!(cycles > 0);
    assert!(engine
        .segment_managers
        .read()
        .unwrap()
        .values()
        .any(|manager| manager.has_segments()));
    for table_name in table_names {
        assert_eq!(collect_rows(&engine, &table_name).len(), 16);
    }

    engine.close_engine().unwrap();
}

#[test]
fn superseded_pressure_seal_cycle_does_not_remain_armed() {
    assert!(PressureSealCycleOutcome::Completed.keeps_request_armed(true));
    assert!(!PressureSealCycleOutcome::Completed.keeps_request_armed(false));
    assert!(!PressureSealCycleOutcome::Idle.keeps_request_armed(true));
    assert!(!PressureSealCycleOutcome::Superseded.keeps_request_armed(true));
}

#[test]
fn pressure_seal_wait_is_bounded_by_one_completed_cycle() {
    let control = Arc::new(PressureSealControl::new());
    control.worker_started();
    control.request();

    let waiting = Arc::clone(&control);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        ready_tx.send(()).unwrap();
        waiting.wait_before_commit();
        done_tx.send(()).unwrap();
    });

    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
    control.finish_cycle(true);
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("a completed cycle releases existing commit waiters");
    assert!(control.requested.load(Ordering::Acquire));

    control.worker_stopped();
    waiter.join().unwrap();
}

#[test]
fn pressure_seal_cycle_drains_only_one_hot_table() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("pressure-seal-one-table");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;
    config.persistence.seal_hot_bytes_threshold = 4 * 1024;
    config.persistence.seal_incremental_hot_bytes_threshold = 2 * 1024;
    config.cleanup.enabled = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    for table_name in ["alpha", "beta"] {
        create_catalog_test_table(
            &engine,
            SchemaBuilder::new(table_name)
                .column("id", DataType::Integer, false, true)
                .column("payload", DataType::Text, false, false)
                .build(),
        )
        .unwrap();
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table(table_name).unwrap();
        for row_id in 0..32_i64 {
            table
                .insert(Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text("x".repeat(256)),
                ]))
                .unwrap();
        }
        drop(table);
        transaction.commit().unwrap();
    }

    assert_eq!(
        engine.pressure_seal_cycle().unwrap(),
        PressureSealCycleOutcome::Completed
    );
    let sealed_after_first = engine
        .segment_managers
        .read()
        .unwrap()
        .values()
        .filter(|manager| manager.has_segments())
        .count();
    assert_eq!(sealed_after_first, 1);
    assert!(engine.hot_seal_pressure_exceeded());

    assert_eq!(
        engine.pressure_seal_cycle().unwrap(),
        PressureSealCycleOutcome::Completed
    );
    let sealed_after_second = engine
        .segment_managers
        .read()
        .unwrap()
        .values()
        .filter(|manager| manager.has_segments())
        .count();
    assert_eq!(sealed_after_second, 2);
    assert!(!engine.hot_seal_pressure_exceeded());

    engine.close_engine().unwrap();
}

#[test]
fn checkpoint_publishes_l0_and_truncates_wal_without_waiting_for_compaction() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("checkpoint-l0-no-compaction");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 1_000_000;
    config.persistence.compact_threshold = 2;
    config.cleanup.enabled = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("payload", DataType::Text, false, false)
            .build(),
    )
    .unwrap();

    for generation in 0..2_i64 {
        let mut transaction = engine.begin_transaction().unwrap();
        let mut table = transaction.get_table("items").unwrap();
        for offset in 0..2_i64 {
            let row_id = generation * 2 + offset + 1;
            table
                .insert(Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(format!("row-{row_id}")),
                ]))
                .unwrap();
        }
        drop(table);
        transaction.commit().unwrap();

        crate::traits::Engine::force_checkpoint_cycle(&engine).unwrap();
        assert!(engine.compaction_requested.load(Ordering::Acquire));
        assert!(!engine.compaction_running.load(Ordering::Acquire));
        let catalog = engine.pin_catalog().unwrap();
        let table_id = catalog
            .find_relation(radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE, "items")
            .unwrap()
            .unwrap()
            .id();
        let publisher = engine.physical_generation.load_full().unwrap();
        let lease = publisher.pin().unwrap();
        let physical = lease.snapshot();
        let manifest = physical.table_manifest(table_id).unwrap();
        assert_eq!(manifest.segments().len(), generation as usize + 1);
        assert!(manifest
            .segments()
            .iter()
            .all(|segment| segment.tier() == crate::v6::SegmentTier::L0));
        assert!(physical.control().wal_replay_floor().lsn() > 0);
    }

    let persistence = engine.persistence().unwrap();
    let mut replayed_dml = 0_u64;
    persistence
        .replay_two_phase(0, |entry| {
            if matches!(
                entry.operation,
                WALOperationType::Insert | WALOperationType::Update | WALOperationType::Delete
            ) {
                replayed_dml = replayed_dml.saturating_add(1);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        replayed_dml, 0,
        "manifest-published L0 must own data after WAL truncation"
    );
    let runtime = engine.runtime_stats_snapshot();
    assert_eq!(runtime.cold_l0_segments, 2);
    assert_eq!(runtime.cold_l1_segments, 0);
    assert!(runtime.cold_l0_debt_physical_bytes > 0);
    assert_eq!(
        runtime.max_compaction_input_segments,
        config.persistence.max_compaction_input_segments as u64
    );
    assert_eq!(
        runtime.max_compaction_input_bytes,
        config.persistence.max_compaction_input_bytes
    );
    assert_eq!(
        runtime.max_compaction_output_bytes,
        config.persistence.max_compaction_output_bytes
    );
    assert_eq!(
        runtime.compaction_job_time_budget_ms,
        config.persistence.compaction_job_time_budget_ms
    );
    assert_eq!(collect_rows(&engine, "items").len(), 4);
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert_eq!(collect_rows(&reopened, "items").len(), 4);
    let manager = reopened.get_or_create_segment_manager("items");
    let manifest = manager.manifest();
    assert_eq!(manifest.segments.len(), 2);
    assert!(manifest
        .segments
        .iter()
        .all(|segment| segment.level == SegmentLevel::L0));
    drop(manifest);
    reopened.close_engine().unwrap();
}

#[test]
fn test_cleanup_config_custom_settings() {
    let config = Config::in_memory().with_cleanup(
        CleanupConfig::default()
            .with_interval_secs(30)
            .with_deleted_row_retention_secs(120)
            .with_transaction_retention_secs(600),
    );

    assert_eq!(config.cleanup.interval_secs, 30);
    assert_eq!(config.cleanup.deleted_row_retention_secs, 120);
    assert_eq!(config.cleanup.transaction_retention_secs, 600);
    assert!(config.cleanup.enabled);
}

#[test]
fn test_cleanup_old_transactions_read_committed() {
    // READ COMMITTED writers still retain exact commit sequences so a
    // future snapshot can linearize against them. With no active viewer,
    // GC may remove those sequence records immediately.
    let config = Config::in_memory().with_cleanup(CleanupConfig::disabled());
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    // Create and commit multiple transactions
    for _ in 0..10 {
        let mut txn = engine.begin_transaction().unwrap();
        txn.commit().unwrap();
    }

    assert_eq!(engine.registry.committed_count(), 10);
    let cleaned = engine.cleanup_old_transactions(Duration::from_secs(0));
    assert_eq!(
        cleaned, 10,
        "GC should remove the retained commit sequences"
    );
    assert_eq!(engine.registry.committed_count(), 0);

    engine.close_engine().unwrap();
}

#[test]
fn test_cleanup_old_transactions_snapshot_isolation() {
    use radixdb_core::IsolationLevel;

    // In SNAPSHOT ISOLATION mode, old transactions can be cleaned
    let config = Config::in_memory().with_cleanup(CleanupConfig::disabled());
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    // Set isolation level to Snapshot Isolation
    engine
        .registry
        .set_global_isolation_level(IsolationLevel::SnapshotIsolation);

    // Create and commit multiple transactions
    for _ in 0..10 {
        let mut txn = engine.begin_transaction().unwrap();
        txn.commit().unwrap();
    }

    // Wait a tiny bit for transactions to be "old"
    std::thread::sleep(Duration::from_millis(10));

    // Cleanup with 0 retention should clean all committed transactions
    let cleaned = engine.cleanup_old_transactions(Duration::from_secs(0));
    assert!(
        cleaned > 0,
        "SNAPSHOT mode should clean up old committed transactions"
    );

    engine.close_engine().unwrap();
}

#[test]
fn test_start_and_stop_cleanup() {
    let config = Config::in_memory().with_cleanup(
        CleanupConfig::default()
            .with_interval_secs(60) // Long interval so it doesn't run during test
            .with_deleted_row_retention_secs(0),
    );
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let engine = Arc::new(engine);

    // Start cleanup
    engine.start_cleanup();

    // Verify cleanup handle was set
    {
        let handle = engine.cleanup_handle.lock().unwrap();
        assert!(
            handle.is_some(),
            "Cleanup handle should be set after start_cleanup"
        );
    }

    // Close engine should stop cleanup
    engine.close_engine().unwrap();

    // Verify cleanup handle was cleared
    {
        let handle = engine.cleanup_handle.lock().unwrap();
        assert!(
            handle.is_none(),
            "Cleanup handle should be cleared after close"
        );
    }
}

#[test]
fn test_scheduled_cleanup_runs() {
    let config = Config::in_memory().with_cleanup(
        CleanupConfig::default()
            .with_interval_secs(1) // 1 second interval
            .with_deleted_row_retention_secs(0), // Immediate cleanup
    );
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    // Create table
    let schema = SchemaBuilder::new("cleanup_test")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    // Insert rows
    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("cleanup_test").unwrap();
        for i in 1..=20 {
            table
                .insert(Row::from_values(vec![Value::Integer(i)]))
                .unwrap();
        }
        tx.commit().unwrap();
    }

    // Delete all rows using DELETE with no WHERE clause
    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("cleanup_test").unwrap();
        table.delete(None).unwrap();
        tx.commit().unwrap();
    }

    // Wrap in Arc for start_cleanup
    let engine = Arc::new(engine);

    // Start cleanup
    engine.start_cleanup();

    // Wait for scheduled cleanup to run
    std::thread::sleep(Duration::from_millis(1500));

    // Verify rows are cleaned
    {
        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("cleanup_test").unwrap();
        let mut scanner = table.scan(&[0], None).unwrap();
        let mut count = 0;
        while scanner.next() {
            count += 1;
        }
        assert_eq!(
            count, 0,
            "All deleted rows should be cleaned by scheduled cleanup"
        );
    }

    engine.close_engine().unwrap();
}

#[test]
fn r2_l01_batch_a_persistent_startup_is_owned_fail_closed_and_retryable() {
    let locked_dir = tempfile::tempdir().unwrap();
    let locked_path = locked_dir.path().join("owned-startup");
    let owner = FileLock::acquire(&locked_path).unwrap();
    let engine = MVCCEngine::new(Config::with_path(
        locked_path.to_string_lossy().into_owned(),
    ));

    assert!(
        !locked_path.join("wal").exists(),
        "constructing an engine must not mutate persistent artifacts before lock ownership"
    );
    let lock_error = engine.open_engine().unwrap_err();
    assert!(matches!(lock_error, Error::DatabaseLocked));
    assert!(
        !engine.is_open(),
        "failed open must not publish ready state"
    );

    drop(owner);
    engine
        .open_engine()
        .expect("a pre-start lock failure must remain retryable on the same engine");
    assert!(engine.is_open());
    assert!(matches!(
        FileLock::acquire(&locked_path).unwrap_err(),
        Error::DatabaseLocked
    ));
    let _retained_publisher = engine.physical_generation.load_full().unwrap();
    engine.close_engine().unwrap();
    let _released_after_publisher_drop = FileLock::acquire(&locked_path)
        .expect("close must revoke both engine and retained-publisher lock owners");

    let broken_dir = tempfile::tempdir().unwrap();
    let broken_path = broken_dir.path().join("broken-persistence");
    std::fs::create_dir_all(&broken_path).unwrap();
    std::fs::write(broken_path.join("wal"), b"not a directory").unwrap();

    let broken = Arc::new(MVCCEngine::new(Config::with_path(
        broken_path.to_string_lossy().into_owned(),
    )));
    assert!(!broken.is_open());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let attempts: Vec<_> = (0..2)
        .map(|_| {
            let broken = Arc::clone(&broken);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                broken.open_engine()
            })
        })
        .collect();
    barrier.wait();
    for attempt in attempts {
        let persistence_error = attempt.join().unwrap().unwrap_err();
        assert!(
            persistence_error.to_string().contains("wal")
                || persistence_error.to_string().contains("directory"),
            "persistent initialization cause must be returned, got: {persistence_error}"
        );
    }
    assert!(
        !broken.is_open(),
        "persistent initialization failure must not downgrade to a ready memory engine"
    );
    let _released = FileLock::acquire(&broken_path)
        .expect("failed persistent startup must release the database lock");
}

#[test]
fn r2_l01_batch_b_engine_lifecycle_preserves_typed_startup_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lifecycle");
    let owner = FileLock::acquire(&path).unwrap();
    let engine = MVCCEngine::new(Config::with_path(path.to_string_lossy().into_owned()));
    assert!(matches!(
        engine.lifecycle_state(),
        EngineLifecycleState::Closed
    ));

    let error = engine.open_engine().unwrap_err();
    assert!(matches!(error, Error::DatabaseLocked));
    assert!(matches!(
        engine.lifecycle_state(),
        EngineLifecycleState::Failed(Error::DatabaseLocked)
    ));

    drop(owner);
    engine.open_engine().unwrap();
    assert!(matches!(
        engine.lifecycle_state(),
        EngineLifecycleState::Ready
    ));
    engine.close_engine().unwrap();
    assert!(matches!(
        engine.lifecycle_state(),
        EngineLifecycleState::Closed
    ));

    let recovery_dir = tempfile::tempdir().unwrap();
    let recovery_path = recovery_dir.path().join("indexed-recovery");
    let recovery_config = Config::with_path(recovery_path.to_string_lossy().into_owned());
    let writer = MVCCEngine::new(Config::with_path(
        recovery_path.to_string_lossy().into_owned(),
    ));
    writer.open_engine().unwrap();
    create_catalog_test_table(
        &writer,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build(),
    )
    .unwrap();
    commit_test_index(&writer, "items", "idx_name", &["name"], false, None).unwrap();
    writer.close_engine().unwrap();

    let recovered = MVCCEngine::new(recovery_config);
    assert!(matches!(
        recovered.get_version_store("items"),
        Err(Error::EngineNotOpen)
    ));
    recovered
        .open_engine()
        .expect("internal WAL index reconstruction must not require public Ready state");
    assert!(recovered
        .get_version_store("items")
        .unwrap()
        .index_exists("idx_name"));
    assert!(matches!(
        recovered.lifecycle_state(),
        EngineLifecycleState::Ready
    ));
    recovered.close_engine().unwrap();
}

#[test]
fn r3_l03_batch_b_checkpoint_owns_catalog_generation_fence() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("checkpoint-ddl-fence");
    let engine = Arc::new(MVCCEngine::new(Config::with_path(
        database_path.to_string_lossy(),
    )));
    engine.open_engine().unwrap();

    let ddl = engine.acquire_ddl_statement_fence(true);
    let (done_tx, done_rx) = mpsc::channel();
    let checkpoint_engine = Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        let _ = done_tx.send(checkpoint_engine.checkpoint_cycle_inner(true));
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "checkpoint publication must wait for the active DDL generation"
    );
    drop(ddl);
    done_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("checkpoint must continue after DDL publication")
        .unwrap();
    worker.join().unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn r3_l03_batch_c_failed_hybrid_truncate_preserves_cold_topology() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("hybrid-truncate");
    let config = r2_l03_batch_a_config(&database_path);
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    r2_l03_batch_a_create_table(&engine, "items");
    r2_l03_batch_a_insert(&engine, "items", 7);
    engine.checkpoint_cycle_inner(true).unwrap();

    let mut writer = engine.begin_transaction().unwrap();
    writer
        .get_table("items")
        .unwrap()
        .delete_by_row_ids(&[7])
        .unwrap();

    let mut truncator = engine.begin_transaction().unwrap();
    let error = truncator
        .get_table("items")
        .unwrap()
        .truncate()
        .unwrap_err();
    assert!(matches!(error, Error::TableHasActiveTransactions));
    writer.rollback().unwrap();
    truncator.rollback().unwrap();

    let manager = engine.get_or_create_segment_manager("items");
    assert!(manager.has_segments());
    assert_eq!(manager.total_row_count(), 1);
    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(0), Some(&Value::Integer(7)));
    engine.close_engine().unwrap();
}

#[test]
fn r3_l03_batch_c_cold_delete_marker_failure_has_one_aborted_outcome() {
    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("cold-marker-failure");
    let mut config = r2_l03_batch_a_config(&database_path);
    config.persistence.sync_mode = crate::SyncMode::Full;
    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    r2_l03_batch_a_create_table(&engine, "items");
    r2_l03_batch_a_insert(&engine, "items", 7);
    engine.checkpoint_cycle_inner(true).unwrap();

    let mut transaction = engine.begin_transaction().unwrap();
    let target_txn = transaction.id();
    transaction
        .get_table("items")
        .unwrap()
        .delete_by_row_ids(&[7])
        .unwrap();
    let marker_seen = Arc::new(AtomicBool::new(false));
    let marker_seen_by_hook = Arc::clone(&marker_seen);
    let persistence = engine.persistence().unwrap();
    let append_hook = persistence
        .install_wal_append_test_hook(Arc::new(move |entry| {
            if entry.txn_id == target_txn && entry.is_commit_marker() {
                marker_seen_by_hook.store(true, Ordering::Release);
                crate::test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
            }
        }))
        .unwrap();
    let commit = transaction.commit();
    drop(append_hook);
    crate::test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

    assert!(commit.is_err());
    assert!(
        marker_seen.load(Ordering::Acquire),
        "oracle must fail the terminal marker after recording cold DML"
    );
    let manager = engine.get_or_create_segment_manager("items");
    assert!(!manager.is_tombstoned(7));
    assert_eq!(manager.pending_tombstone_count(target_txn), 0);
    assert_eq!(collect_rows(&engine, "items").len(), 1);
    engine.close_engine().unwrap();

    let recovered = MVCCEngine::new(config);
    recovered.open_engine().unwrap();
    assert_eq!(collect_rows(&recovered, "items").len(), 1);
    assert!(!recovered
        .get_or_create_segment_manager("items")
        .is_tombstoned(7));
    recovered.close_engine().unwrap();
}
