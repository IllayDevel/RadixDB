#[test]
fn test_r2_l04_batch_c_commit_recovery_is_atomic_bounded_and_minimal() {
    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();

    let multi_table_failure_is_one_outcome = {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("multi_table_failure");
        let mut config = r2_l03_batch_a_config(&database_path);
        config.persistence.sync_mode = crate::SyncMode::Full;
        let engine = MVCCEngine::new(config.clone());
        engine.open_engine().unwrap();
        r2_l03_batch_a_create_table(&engine, "alpha");
        r2_l03_batch_a_create_table(&engine, "beta");

        let mut transaction = engine.begin_transaction().unwrap();
        let target_txn_id = transaction.id();
        let dml_appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hook_appends = Arc::clone(&dml_appends);
        let persistence = engine.persistence().unwrap();
        let append_hook = persistence
            .install_wal_append_test_hook(Arc::new(move |entry| {
                if entry.txn_id != target_txn_id {
                    return;
                }
                if matches!(
                    entry.operation,
                    WALOperationType::Insert | WALOperationType::Update | WALOperationType::Delete
                ) {
                    if hook_appends.fetch_add(1, Ordering::AcqRel) == 1 {
                        crate::test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
                    }
                } else if entry.is_commit_marker() {
                    crate::test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);
                }
            }))
            .unwrap();

        for table_name in ["alpha", "beta"] {
            let mut table = transaction.get_table(table_name).unwrap();
            table
                .insert(Row::from_values(vec![
                    Value::Integer(1),
                    Value::text(table_name),
                ]))
                .unwrap();
        }
        let result = transaction.commit();
        drop(append_hook);
        crate::test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);

        let runtime_empty =
            collect_rows(&engine, "alpha").is_empty() && collect_rows(&engine, "beta").is_empty();
        engine.close_engine().unwrap();

        let reopened = MVCCEngine::new(config);
        reopened.open_engine().unwrap();
        let restart_empty = collect_rows(&reopened, "alpha").is_empty()
            && collect_rows(&reopened, "beta").is_empty();
        reopened.close_engine().unwrap();

        let returned_error = result.is_err();
        let append_count = dml_appends.load(Ordering::Acquire);
        returned_error && append_count == 2 && runtime_empty && restart_empty
    };

    let recovery_outcomes_spilled = {
        use crate::mvcc::wal_manager::{
            recovery_outcome_spill_count, reset_recovery_outcome_spill_count, WALEntry, WALManager,
            TEST_RECOVERY_OUTCOME_MEMORY_LIMIT,
        };

        let directory = tempfile::tempdir().unwrap();
        let wal = WALManager::new(directory.path(), crate::SyncMode::Full).unwrap();
        for txn_id in 1..=(TEST_RECOVERY_OUTCOME_MEMORY_LIMIT as i64 + 1) {
            wal.append_entry(WALEntry::new(
                txn_id,
                "spill".to_string(),
                txn_id,
                WALOperationType::Insert,
                vec![txn_id as u8],
            ))
            .unwrap();
            wal.write_commit_marker(txn_id).unwrap();
        }
        reset_recovery_outcome_spill_count();
        let mut applied = 0usize;
        let info = wal
            .replay_two_phase(0, |entry| {
                if entry.operation == WALOperationType::Insert {
                    applied += 1;
                }
                Ok(())
            })
            .unwrap();
        let spilled = recovery_outcome_spill_count() > 0
            && info.committed_transactions == TEST_RECOVERY_OUTCOME_MEMORY_LIMIT + 1
            && applied == TEST_RECOVERY_OUTCOME_MEMORY_LIMIT + 1;
        wal.close().unwrap();
        spilled
    };

    let hot_delete_payload_is_minimal = {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("minimal_delete");
        let engine = MVCCEngine::new(r2_l03_batch_a_config(&database_path));
        engine.open_engine().unwrap();
        r2_l03_batch_a_create_table(&engine, "wide_rows");

        let mut insert = engine.begin_transaction().unwrap();
        let mut table = insert.get_table("wide_rows").unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(7),
                Value::text("x".repeat(64 * 1024)),
            ]))
            .unwrap();
        insert.commit().unwrap();

        let mut delete = engine.begin_transaction().unwrap();
        let mut table = delete.get_table("wide_rows").unwrap();
        assert_eq!(table.delete(None).unwrap(), 1);
        delete.commit().unwrap();

        let mut delete_payloads = Vec::new();
        engine
            .persistence()
            .unwrap()
            .replay_two_phase(0, |entry| {
                if entry.operation == WALOperationType::Delete {
                    delete_payloads.push(entry.data.len());
                }
                Ok(())
            })
            .unwrap();
        engine.close_engine().unwrap();
        !delete_payloads.is_empty() && delete_payloads.iter().all(|size| *size == 0)
    };

    assert!(
            multi_table_failure_is_one_outcome
                && recovery_outcomes_spilled
                && hot_delete_payload_is_minimal,
            "batch C outcomes: multi_table_failure_is_one_outcome={multi_table_failure_is_one_outcome}, recovery_outcomes_spilled={recovery_outcomes_spilled}, hot_delete_payload_is_minimal={hot_delete_payload_is_minimal}"
        );
}

#[test]
fn test_register_artifact_backed_volume_records_manifest_file_path() {
    let engine = MVCCEngine::new(Config::default());
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::text("alice")]),
    )];

    let _volume_dir = register_artifact_volume(&engine, "items", &schema, &rows);
    let mgrs = engine.segment_managers.read().unwrap();
    let mgr = mgrs.get("items").expect("items segment manager");
    let manifest = mgr.manifest();
    assert_eq!(manifest.segments.len(), 1);
    let file_path = &manifest.segments[0].file_path;
    assert!(
        !file_path.as_os_str().is_empty(),
        "artifact-backed publish must record durable DATA path in manifest"
    );
    assert!(
        file_path
            .extension()
            .is_some_and(|extension| extension == "data"),
        "unexpected manifest segment file_path: {:?}",
        file_path
    );
}

#[test]
fn test_compaction_keeps_artifact_segments_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_artifact_metadata_only");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        for i in 0..100i64 {
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

    engine.compact_after_checkpoint_forced().unwrap();
    assert_table_segments_artifact_backed(&engine, "items");

    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0].1.get(1), Some(&Value::text("name-0")));
    assert_eq!(rows[99].1.get(1), Some(&Value::text("name-99")));

    engine.close_engine().unwrap();
}

#[test]
fn compaction_publishes_the_post_merge_tombstone_set_before_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compaction_tombstone_replacement");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 8;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();
    let mut insert = engine.begin_transaction().unwrap();
    let mut table = insert.get_table("items").unwrap();
    for row_id in 1..=3 {
        table
            .insert(Row::from_values(vec![Value::Integer(row_id)]))
            .unwrap();
    }
    insert.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();

    let mut delete = engine.begin_transaction().unwrap();
    let mut table = delete.get_table("items").unwrap();
    let filter = crate::expression::ComparisonExpr::gt("id", Value::Integer(2));
    assert_eq!(table.delete(Some(&filter)).unwrap(), 1);
    delete.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();
    assert_eq!(collect_rows(&engine, "items").len(), 2);

    engine.compact_after_checkpoint_forced().unwrap();
    assert_eq!(collect_rows(&engine, "items").len(), 2);
    assert_eq!(
        engine
            .segment_managers
            .read()
            .unwrap()
            .get("items")
            .unwrap()
            .tombstone_count(),
        0
    );
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert_eq!(collect_rows(&reopened, "items").len(), 2);
    let managers = reopened.segment_managers.read().unwrap();
    let manager = managers.get("items").unwrap();
    assert_eq!(manager.tombstone_count(), 0);
    assert_eq!(manager.total_row_count(), 2);
    drop(managers);
    reopened.close_engine().unwrap();
}

#[test]
fn wal_replay_promotes_cold_tombstone_with_durable_visibility_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cold_tombstone_replay_sequence");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![Value::Integer(7)]))
        .unwrap();
    insert.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();

    let mut delete = engine.begin_transaction().unwrap();
    let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(7));
    assert_eq!(
        delete
            .get_table("items")
            .unwrap()
            .delete(Some(&filter))
            .unwrap(),
        1
    );
    delete.commit().unwrap();
    assert!(collect_rows(&engine, "items").is_empty());
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert!(collect_rows(&reopened, "items").is_empty());
    let manager = reopened.get_or_create_segment_manager("items");
    let recovered_tombstones = manager.tombstone_set_arc();
    assert_eq!(recovered_tombstones.len(), 1);
    assert!(
        recovered_tombstones.values().all(|sequence| *sequence > 0),
        "replayed cold tombstones must use the WAL commit marker sequence"
    );
    drop(recovered_tombstones);

    // The immutable tombstone artifact rejects sequence zero. This checkpoint
    // therefore proves that replayed state is both logically correct and legal
    // for the next physical generation.
    reopened.checkpoint_cycle_inner(true).unwrap();
    assert!(collect_rows(&reopened, "items").is_empty());
    reopened.close_engine().unwrap();
}

#[test]
fn bounded_compaction_keeps_tombstone_until_every_physical_copy_is_retired() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bounded_compaction_duplicate_row_id_tombstone");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 8;
    config.persistence.max_compaction_input_segments = 1;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let _older = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(7), Value::text("older")]),
        )],
    );
    let _newer = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(7), Value::text("newer")]),
        )],
    );

    let manager = engine.get_or_create_segment_manager("items");
    manager.add_tombstones(&[1], 1);
    engine.checkpoint_cycle_inner(true).unwrap();
    assert!(collect_rows(&engine, "items").is_empty());

    engine.compact_after_checkpoint_forced().unwrap();
    assert!(
        collect_rows(&engine, "items").is_empty(),
        "retiring one copy must not expose the same row_id from an unselected segment"
    );
    assert_eq!(
        manager.tombstone_set_arc().get(&1),
        Some(&1),
        "the tombstone remains authoritative while an unselected physical copy exists"
    );
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert!(collect_rows(&reopened, "items").is_empty());
    let manager = reopened.get_or_create_segment_manager("items");
    assert_eq!(manager.tombstone_set_arc().get(&1), Some(&1));

    reopened.compact_after_checkpoint_forced().unwrap();
    assert!(collect_rows(&reopened, "items").is_empty());
    assert_eq!(manager.tombstone_count(), 0);
    reopened.close_engine().unwrap();
}

#[test]
fn compaction_defers_unique_key_reuse_while_old_snapshot_needs_both_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compaction_snapshot_unique_key_reuse");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.max_compaction_jobs = 1;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _old_volume = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(7), Value::text("old")]),
        )],
    );
    let _new_volume = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            2,
            Row::from_values(vec![Value::Integer(7), Value::text("new")]),
        )],
    );

    let snapshot = engine
        .begin_transaction_with_level(IsolationLevel::SnapshotIsolation)
        .unwrap();
    let snapshot_boundary = engine
        .registry
        .get_min_snapshot_begin_seq()
        .expect("snapshot transaction must pin compaction");
    let manager = engine.get_or_create_segment_manager("items");
    manager.add_tombstones(&[1], snapshot_boundary as u64);

    engine
        .compact_after_checkpoint_forced()
        .expect("unsafe tombstone must defer its segment instead of invalidating compaction");
    let current_rows = collect_rows(&engine, "items");
    assert_eq!(current_rows.len(), 1);
    assert_eq!(current_rows[0].1.get(1), Some(&Value::text("new")));

    drop(snapshot);
    engine.compact_after_checkpoint_forced().unwrap();
    assert_eq!(manager.tombstone_count(), 0);
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let rows = collect_rows(&reopened, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::text("new")));
    reopened.close_engine().unwrap();
}

#[test]
fn seal_preserves_tombstone_committed_after_extraction() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("seal-post-extraction-tombstone");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 16;
    config.persistence.compact_threshold = u32::MAX;
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

    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::uuid([11; 16]),
            Value::Integer(1),
        ]))
        .unwrap();
    insert.commit().unwrap();
    let row_id = collect_rows(&engine, "items")[0].0;

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::SealBeforePublish],
    );
    let controller = schedule.controller();
    let sealing_engine = Arc::clone(&engine);
    let seal = std::thread::spawn(move || {
        crate::traits::Engine::force_checkpoint_cycle(sealing_engine.as_ref())
    });
    let extraction_complete = controller
        .wait_for(
            InterleavePoint::SealBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .expect("seal must pause after extraction and before DATA publication");

    // Model a committed cold-row replacement that publishes its tombstone
    // after seal captured the source row. The older DATA artifact must not
    // clear storage state that did not belong to its extraction snapshot.
    let post_extraction_commit_seq = engine.registry.reserve_visibility_sequence().unwrap() as u64;
    let manager = engine.get_or_create_segment_manager("items");
    manager.add_tombstones(&[row_id], post_extraction_commit_seq);
    controller.release(extraction_complete);
    seal.join().unwrap().unwrap();

    assert_eq!(
        manager.tombstone_set_arc().get(&row_id),
        Some(&post_extraction_commit_seq),
        "seal must preserve a tombstone committed after its extraction snapshot"
    );
    drop(schedule);
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert!(
        collect_rows(&reopened, "items").is_empty(),
        "the post-extraction tombstone must remain authoritative after reopen"
    );
    reopened.close_engine().unwrap();
}

#[test]
fn committed_tombstone_publication_waits_for_compaction_fence() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("commit_tombstone_compaction_fence");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.max_compaction_jobs = 1;
    config.persistence.checkpoint_on_close = false;

    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema).unwrap();

    for row_id in 1..=2 {
        let mut insert = engine.begin_transaction().unwrap();
        insert
            .get_table("items")
            .unwrap()
            .insert(Row::from_values(vec![Value::Integer(row_id)]))
            .unwrap();
        insert.commit().unwrap();
        engine.checkpoint_cycle_inner(true).unwrap();
    }

    let mut delete = engine.begin_transaction().unwrap();
    let filter = crate::expression::ComparisonExpr::eq("id", Value::Integer(1));
    assert_eq!(
        delete
            .get_table("items")
            .unwrap()
            .delete(Some(&filter))
            .unwrap(),
        1
    );

    let manager = engine.get_or_create_segment_manager("items");
    let schedule = InterleaveGuard::install_scoped(engine.schema_scope_id, [
        InterleavePoint::CommittedStorageBeforePublish,
        InterleavePoint::CompactionTombstonesPrepared,
    ]);
    let controller = schedule.controller();
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let commit = std::thread::spawn(move || {
        let result = delete.commit();
        done_tx.send(result).unwrap();
    });
    let commit_ready = controller
        .wait_for(
            InterleavePoint::CommittedStorageBeforePublish,
            None,
            std::time::Duration::from_secs(2),
        )
        .expect("commit must reach the storage-publication boundary");

    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let compaction_ready = controller
        .wait_for(
            InterleavePoint::CompactionTombstonesPrepared,
            None,
            std::time::Duration::from_secs(2),
        )
        .expect("compaction must own the table fence after deriving tombstones");

    controller.release(commit_ready);
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "cold tombstone publication must wait while compaction owns the table fence"
    );

    controller.release(compaction_ready);
    compaction.join().unwrap().unwrap();
    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("commit must resume after the compaction fence is released")
        .unwrap();
    commit.join().unwrap();
    drop(schedule);

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(0), Some(&Value::Integer(2)));
    assert_eq!(manager.tombstone_count(), 1);
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let rows = collect_rows(&reopened, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(0), Some(&Value::Integer(2)));
    reopened.close_engine().unwrap();
}

#[test]
fn size_tiered_compaction_bounds_stable_segment_fanout_without_rebuilding_terminal_output() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_size_tier");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.max_compaction_jobs = 1;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("sequence", DataType::Integer, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    commit_test_index(&engine, "items", "items_name_uq", &["name"], true, None).unwrap();
    commit_test_index(
        &engine,
        "items",
        "items_sequence_idx",
        &["sequence"],
        false,
        None,
    )
    .unwrap();
    let mut volume_dirs = Vec::new();
    let mut next_row_id = 1_i64;

    for row_count in [50_usize, 50, 50] {
        let rows = (0..row_count)
            .map(|_| {
                let row_id = next_row_id;
                next_row_id += 1;
                (
                    row_id,
                    Row::from_values(vec![
                        Value::Integer(row_id),
                        Value::text(format!("name-{row_id}")),
                        Value::Integer(row_id),
                    ]),
                )
            })
            .collect::<Vec<_>>();
        volume_dirs.push(register_artifact_volume(&engine, "items", &schema, &rows));
    }

    let first_before = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    engine.compact_after_checkpoint_forced().unwrap();
    let first_after = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    assert_eq!(
        first_after - first_before,
        1,
        "one compacted V6 DATA output owns one INDEX artifact build"
    );

    for row_count in [25_usize, 25] {
        let rows = (0..row_count)
            .map(|_| {
                let row_id = next_row_id;
                next_row_id += 1;
                (
                    row_id,
                    Row::from_values(vec![
                        Value::Integer(row_id),
                        Value::text(format!("name-{row_id}")),
                        Value::Integer(row_id),
                    ]),
                )
            })
            .collect::<Vec<_>>();
        volume_dirs.push(register_artifact_volume(&engine, "items", &schema, &rows));
    }

    let second_before = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    engine.compact_after_checkpoint_forced().unwrap();
    let second_after = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    assert_eq!(second_after - second_before, 1);
    let terminal = {
        let manager = engine.get_or_create_segment_manager("items");
        let manifest = manager.manifest();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].row_count, 200);
        assert_eq!(manifest.segments[0].level, SegmentLevel::L1);
        (
            manifest.segments[0].segment_id,
            manifest.segments[0].file_path.clone(),
        )
    };

    for row_count in [25_usize, 25] {
        let rows = (0..row_count)
            .map(|_| {
                let row_id = next_row_id;
                next_row_id += 1;
                (
                    row_id,
                    Row::from_values(vec![
                        Value::Integer(row_id),
                        Value::text(format!("name-{row_id}")),
                        Value::Integer(row_id),
                    ]),
                )
            })
            .collect::<Vec<_>>();
        volume_dirs.push(register_artifact_volume(&engine, "items", &schema, &rows));
    }

    let third_before = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    engine.compact_after_checkpoint_forced().unwrap();
    let third_after = engine
        .runtime_stats_snapshot()
        .maintenance
        .compaction_cost
        .posting_outputs_generated;
    assert_eq!(
        third_after - third_before,
        1,
        "only the fresh L0 run receives a new V6 INDEX artifact"
    );
    let manager = engine.get_or_create_segment_manager("items");
    let manifest = manager.manifest();
    assert_eq!(manifest.segments.len(), 2);
    let preserved = manifest
        .segments
        .iter()
        .find(|segment| segment.segment_id == terminal.0)
        .expect("terminal size-tier output must remain published");
    assert_eq!(preserved.file_path, terminal.1);
    drop(manifest);

    let exact_value = Value::text("name-225");
    crate::instrumentation::begin_posting_lookup_probe();
    let exact = manager
        .find_row_ids_by_exact_index(&[1], &[&exact_value], None, None)
        .unwrap()
        .expect("every live segment must expose the primary-key posting");
    let ordered = manager
        .find_row_ids_by_ordered_index(
            &[2],
            &[],
            Some((225, true)),
            Some((225, true)),
            true,
            10,
            None,
            &FxHashSet::default(),
        )
        .unwrap()
        .expect("every live segment must expose the ordered primary-key posting");
    let fanout = crate::instrumentation::end_posting_lookup_probe();
    assert_eq!(exact, vec![225]);
    assert_eq!(ordered, vec![225]);
    assert_eq!(fanout.exact_calls, 1);
    assert_eq!(fanout.exact_segments, 2);
    assert_eq!(fanout.ordered_calls, 1);
    assert_eq!(fanout.ordered_segments, 2);

    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 250);
    assert_eq!(rows.iter().map(|(row_id, _)| *row_id).min(), Some(1));
    assert_eq!(rows.iter().map(|(row_id, _)| *row_id).max(), Some(250));

    drop(volume_dirs);
    engine.close_engine().unwrap();
}

#[test]
fn compaction_merge_admission_is_row_target_bounded() {
    assert!(compaction_segment_is_mergeable(500_000, 1_048_576));
    assert!(!compaction_segment_is_mergeable(1_048_576, 1_048_576));
    assert!(!compaction_segment_is_mergeable(2_000_000, 1_048_576));
}

#[test]
fn test_crash_before_manifest_publish_ignores_unreachable_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_crash_orphan_before_publish");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 8;
    config.persistence.checkpoint_on_close = false;

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        for i in 0..4i64 {
            table
                .insert(Row::from_values(vec![
                    Value::Integer(i),
                    Value::text(format!("kept-{i}")),
                ]))
                .unwrap();
        }
        tx.commit().unwrap();
    }

    engine.checkpoint_cycle_inner(true).unwrap();
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        assert_eq!(
            mgr.get_segments_ordered_meta().len(),
            1,
            "test setup expects one manifest-published segment"
        );
    }
    engine.close_engine().unwrap();

    // Simulate a crash after compaction wrote a durable output artifact but
    // before the CONTROL-selected manifest generation published it.
    let orphan_segment_id = 0x00ab_cdef_u64;
    let orphan_rows = vec![
        (
            1000,
            Row::from_values(vec![Value::Integer(1000), Value::text("orphan-a")]),
        ),
        (
            1001,
            Row::from_values(vec![Value::Integer(1001), Value::text("orphan-b")]),
        ),
    ];
    let orphan = crate::volume::test_artifact::build_artifact_volume(
        &db_path,
        &schema,
        orphan_segment_id,
        &orphan_rows,
    );
    assert!(
        orphan.absolute_path.exists(),
        "test setup must leave orphan compacted output on disk"
    );

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert!(
        orphan.absolute_path.exists(),
        "startup must not bypass the V6 two-cycle quarantine policy"
    );

    let mut rows = collect_rows(&reopened, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        rows.len(),
        4,
        "orphan compaction output must not be loaded as table data"
    );
    for (idx, (row_id, row)) in rows.iter().enumerate() {
        assert_eq!(*row_id, idx as i64);
        assert_eq!(
            row.get(1),
            Some(&Value::text(format!("kept-{idx}"))),
            "restart must preserve only manifest-published rows"
        );
    }

    reopened.close_engine().unwrap();
}

#[test]
fn compaction_rebases_over_l0_append_without_rebuilding_output() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_compatible_l0_append");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
    ];

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforePublish],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let arrival = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .unwrap();

    let _appended_dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            3,
            Row::from_values(vec![Value::Integer(3), Value::text("three")]),
        )],
    );
    controller.release(arrival);
    compaction.join().unwrap().unwrap();

    let manager = engine.get_or_create_segment_manager("items");
    let manifest = manager.manifest();
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(
        manifest
            .segments
            .iter()
            .filter(|segment| segment.level == SegmentLevel::L0)
            .count(),
        1,
        "independent append must survive the rebased compaction"
    );
    assert_eq!(
        manifest
            .segments
            .iter()
            .filter(|segment| segment.level == SegmentLevel::L1)
            .count(),
        1,
        "selected inputs must be replaced by one compacted output"
    );
    drop(manifest);

    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(cost.jobs_planned, 1);
    assert_eq!(cost.jobs_published, 1);
    assert_eq!(cost.jobs_invalidated, 0);

    drop(manager);
    engine.close_engine().unwrap();
    drop(engine);

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let mut reopened_rows = collect_rows(&reopened, "items");
    reopened_rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        reopened_rows
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "CONTROL reopen must preserve both compacted inputs and the independent append"
    );
    reopened.close_engine().unwrap();
}

#[test]
fn rebased_compaction_fault_matrix_reopens_one_complete_generation() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};
    use crate::v6::{
        GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode,
        PhysicalGenerationExpectation,
    };

    let points = [
        GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::DataFinalDirDurable,
        GenerationCrashPoint::IndexAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::IndexFinalDirDurable,
        GenerationCrashPoint::TableManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::TableManifestDirDurable,
        GenerationCrashPoint::DatabaseManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::DatabaseManifestDirDurable,
        GenerationCrashPoint::ControlAfterPartialWrite,
        GenerationCrashPoint::ControlAfterWriteBeforeFdatasync,
        GenerationCrashPoint::ControlDurable,
        GenerationCrashPoint::ControlAfterRootDirSync,
        GenerationCrashPoint::RuntimeGenerationBeforePublish,
        GenerationCrashPoint::RuntimeGenerationPublished,
    ];

    fn immutable_artifact_paths(root: &Path) -> FxHashSet<PathBuf> {
        fn visit(root: &Path, directory: &Path, paths: &mut FxHashSet<PathBuf>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    visit(root, &path, paths);
                } else if matches!(
                    path.extension().and_then(std::ffi::OsStr::to_str),
                    Some("data" | "idx")
                ) {
                    paths.insert(path.strip_prefix(root).unwrap().to_path_buf());
                }
            }
        }

        let mut paths = FxHashSet::default();
        visit(root, root, &mut paths);
        paths
    }

    for point in points {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("rebased-fault-{}", point.name()));
        let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
        config.persistence.checkpoint_interval = 0;
        config.persistence.target_volume_rows = 100;
        config.persistence.compact_threshold = 2;
        config.persistence.checkpoint_on_close = false;
        config.persistence.l0_soft_limit_segments = usize::MAX - 1;
        config.persistence.l0_hard_limit_segments = usize::MAX;
        config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
        config.persistence.l0_hard_limit_bytes = u64::MAX;

        let engine = Arc::new(MVCCEngine::new(config.clone()));
        engine.open_engine().unwrap();
        let schema = SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();
        create_catalog_test_table(&engine, schema.clone()).unwrap();
        let _volume_dirs = [
            register_artifact_volume(
                &engine,
                "items",
                &schema,
                &[(
                    1,
                    Row::from_values(vec![Value::Integer(1), Value::text("one")]),
                )],
            ),
            register_artifact_volume(
                &engine,
                "items",
                &schema,
                &[(
                    2,
                    Row::from_values(vec![Value::Integer(2), Value::text("two")]),
                )],
            ),
        ];

        let schedule = InterleaveGuard::install_scoped(
            engine.schema_scope_id,
            [InterleavePoint::CompactionBeforePublish],
        );
        let controller = schedule.controller();
        let compacting_engine = Arc::clone(&engine);
        let compaction = std::thread::spawn(move || {
            let fault = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
            let result = compacting_engine.compact_after_checkpoint_forced();
            (result, fault.hit_count())
        });
        let arrival = controller
            .wait_for(
                InterleavePoint::CompactionBeforePublish,
                None,
                Duration::from_secs(2),
            )
            .unwrap_or_else(|_| panic!("{} did not build compaction output", point.name()));
        let _appended_dir = register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                3,
                Row::from_values(vec![Value::Integer(3), Value::text("three")]),
            )],
        );
        let source_control = engine
            .physical_generation
            .load_full()
            .unwrap()
            .pin()
            .unwrap()
            .snapshot()
            .control();
        let target_generation = source_control.database_generation().checked_next().unwrap();
        controller.release(arrival);
        let (result, hit_count) = compaction.join().unwrap();
        assert!(result.is_err(), "{} must interrupt publication", point.name());
        assert_eq!(hit_count, 1, "{} was not traversed", point.name());
        drop(schedule);
        drop(engine);

        let reopened = MVCCEngine::new(config);
        reopened
            .open_engine()
            .unwrap_or_else(|error| panic!("{} reopen failed: {error}", point.name()));
        let lease = reopened
            .physical_generation
            .load_full()
            .unwrap()
            .pin()
            .unwrap();
        let snapshot = lease.snapshot();
        let selected_control = snapshot.control();
        assert_eq!(
            selected_control.wal_replay_floor(),
            source_control.wal_replay_floor(),
            "{} rolled back the newer WAL floor",
            point.name()
        );
        match point.physical_expectation() {
            PhysicalGenerationExpectation::Old => {
                assert_eq!(selected_control, source_control, "{}", point.name())
            }
            PhysicalGenerationExpectation::OldOrNew => assert!(
                selected_control == source_control
                    || selected_control.database_generation() == target_generation,
                "{} selected neither complete generation",
                point.name()
            ),
            PhysicalGenerationExpectation::New => assert_eq!(
                selected_control.database_generation(),
                target_generation,
                "{}",
                point.name()
            ),
        }

        let compacted = selected_control.database_generation() == target_generation;
        let manager = reopened.get_or_create_segment_manager("items");
        let manifest = manager.manifest();
        assert_eq!(manifest.segments.len(), if compacted { 2 } else { 3 });
        assert_eq!(
            manifest
                .segments
                .iter()
                .filter(|segment| segment.level == SegmentLevel::L1)
                .count(),
            usize::from(compacted)
        );
        drop(manifest);
        drop(manager);
        let mut rows = collect_rows(&reopened, "items");
        rows.sort_by_key(|(row_id, _)| *row_id);
        assert_eq!(
            rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "{} lost the independent append or selected input",
            point.name()
        );

        if !compacted {
            let reachable = snapshot
                .artifact_references()
                .into_iter()
                .map(|reference| reference.relative_path())
                .collect::<FxHashSet<_>>();
            assert!(
                immutable_artifact_paths(&db_path)
                    .iter()
                    .any(|path| !reachable.contains(path)),
                "{} must leave its prebuilt output unreachable for generation-aware GC",
                point.name()
            );
        }
        drop(lease);
        reopened.close_engine().unwrap();
    }
}

#[test]
#[ignore = "isolated child entrypoint; executed by rebased_compaction_process_abort_matrix"]
fn rebased_compaction_process_abort_child() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};
    use crate::v6::GenerationCrashPoint;
    use std::io::Write;

    let db_path = PathBuf::from(std::env::var_os("RADIXDB_REBASED_COMPACTION_TEST_ROOT").unwrap());
    let requested = std::env::var("RADIXDB_REBASED_COMPACTION_TEST_POINT").unwrap();
    let point = [
        GenerationCrashPoint::IndexFinalDirDurable,
        GenerationCrashPoint::DatabaseManifestDirDurable,
        GenerationCrashPoint::ControlAfterWriteBeforeFdatasync,
        GenerationCrashPoint::ControlDurable,
        GenerationCrashPoint::RuntimeGenerationPublished,
    ]
    .into_iter()
    .find(|point| point.name() == requested)
    .unwrap_or_else(|| panic!("unknown rebased compaction process point {requested}"));

    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
    ];
    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforePublish],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let arrival = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    let _appended_dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            3,
            Row::from_values(vec![Value::Integer(3), Value::text("three")]),
        )],
    );
    let source_control = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap()
        .snapshot()
        .control();
    let mut expected = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(db_path.join("rebased-compaction-expected"))
        .unwrap();
    writeln!(
        expected,
        "{} {} {}",
        source_control.database_generation().get(),
        source_control.wal_replay_floor().generation().get(),
        source_control.wal_replay_floor().lsn()
    )
    .unwrap();
    expected.sync_all().unwrap();

    std::env::set_var("RADIXDB_GENERATION_FAULT_POINT", point.name());
    std::env::set_var(
        "RADIXDB_GENERATION_FAULT_READY",
        db_path.join("rebased-compaction-boundary-hit"),
    );
    controller.release(arrival);
    let result = compaction.join();
    panic!("child reached the end instead of stopping: {result:?}");
}

#[test]
fn rebased_compaction_process_abort_matrix_preserves_append_and_wal_floor() {
    use crate::v6::{GenerationCrashPoint, PhysicalGenerationExpectation};

    let points = [
        GenerationCrashPoint::IndexFinalDirDurable,
        GenerationCrashPoint::DatabaseManifestDirDurable,
        GenerationCrashPoint::ControlAfterWriteBeforeFdatasync,
        GenerationCrashPoint::ControlDurable,
        GenerationCrashPoint::RuntimeGenerationPublished,
    ];
    for point in points {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("database");
        let evidence = db_path.join("rebased-compaction-boundary-hit");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("mvcc::engine::tests::rebased_compaction_process_abort_child")
            .arg("--ignored")
            .env("RADIXDB_REBASED_COMPACTION_TEST_ROOT", &db_path)
            .env("RADIXDB_REBASED_COMPACTION_TEST_POINT", point.name())
            .status()
            .unwrap();
        assert!(!status.success(), "{} did not stop the child", point.name());
        assert_eq!(
            std::fs::read_to_string(&evidence).unwrap().trim(),
            point.name()
        );
        let expected = std::fs::read_to_string(db_path.join("rebased-compaction-expected"))
            .unwrap();
        let mut expected = expected.split_whitespace().map(|value| value.parse::<u64>().unwrap());
        let source_generation = expected.next().unwrap();
        let expected_wal_generation = expected.next().unwrap();
        let expected_wal_lsn = expected.next().unwrap();
        assert!(expected.next().is_none());

        let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
        config.persistence.checkpoint_interval = 0;
        config.persistence.target_volume_rows = 100;
        config.persistence.compact_threshold = 2;
        config.persistence.checkpoint_on_close = false;
        config.persistence.l0_soft_limit_segments = usize::MAX - 1;
        config.persistence.l0_hard_limit_segments = usize::MAX;
        config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
        config.persistence.l0_hard_limit_bytes = u64::MAX;
        let reopened = MVCCEngine::new(config.clone());
        reopened
            .open_engine()
            .unwrap_or_else(|error| panic!("{} first reopen failed: {error}", point.name()));
        let lease = reopened
            .physical_generation
            .load_full()
            .unwrap()
            .pin()
            .unwrap();
        let control = lease.snapshot().control();
        let selected_generation = control.database_generation().get();
        assert_eq!(
            control.wal_replay_floor().generation().get(),
            expected_wal_generation
        );
        assert_eq!(control.wal_replay_floor().lsn(), expected_wal_lsn);
        match point.physical_expectation() {
            PhysicalGenerationExpectation::Old => {
                assert_eq!(selected_generation, source_generation)
            }
            PhysicalGenerationExpectation::OldOrNew => assert!(
                selected_generation == source_generation
                    || selected_generation == source_generation + 1
            ),
            PhysicalGenerationExpectation::New => {
                assert_eq!(selected_generation, source_generation + 1)
            }
        }
        let compacted = selected_generation == source_generation + 1;
        drop(lease);
        let manager = reopened.get_or_create_segment_manager("items");
        let manifest = manager.manifest();
        assert_eq!(manifest.segments.len(), if compacted { 2 } else { 3 });
        assert_eq!(
            manifest
                .segments
                .iter()
                .filter(|segment| segment.level == SegmentLevel::L1)
                .count(),
            usize::from(compacted)
        );
        drop(manifest);
        drop(manager);
        let mut rows = collect_rows(&reopened, "items");
        rows.sort_by_key(|(row_id, _)| *row_id);
        assert_eq!(
            rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "{} first reopen lost rows",
            point.name()
        );
        reopened.close_engine().unwrap();

        let second = MVCCEngine::new(config);
        second
            .open_engine()
            .unwrap_or_else(|error| panic!("{} second reopen failed: {error}", point.name()));
        assert_eq!(collect_rows(&second, "items").len(), 3);
        second.close_engine().unwrap();
    }
}

#[test]
fn slow_compaction_publishes_once_during_sustained_compatible_appends() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_sustained_compatible_appends");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let mut volume_dirs = Vec::new();
    for row_id in 1..=2 {
        volume_dirs.push(register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                row_id,
                Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(format!("row-{row_id}")),
                ]),
            )],
        ));
    }

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforePublish],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let arrival = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .expect("compaction must finish its one private output build");

    for row_id in 3..=18 {
        volume_dirs.push(register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                row_id,
                Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(format!("row-{row_id}")),
                ]),
            )],
        ));
    }
    controller.release(arrival);
    compaction.join().unwrap().unwrap();

    let manager = engine.get_or_create_segment_manager("items");
    let manifest = manager.manifest();
    assert_eq!(manifest.segments.len(), 17);
    assert_eq!(
        manifest
            .segments
            .iter()
            .filter(|segment| segment.level == SegmentLevel::L0)
            .count(),
        16
    );
    assert_eq!(
        manifest
            .segments
            .iter()
            .filter(|segment| segment.level == SegmentLevel::L1)
            .count(),
        1
    );
    drop(manifest);
    assert_eq!(collect_rows(&engine, "items").len(), 18);

    let cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(cost.jobs_planned, 1);
    assert_eq!(cost.jobs_published, 1);
    assert_eq!(cost.jobs_invalidated, 0);

    drop(volume_dirs);
    engine.close_engine().unwrap();
}

#[test]
fn forced_checkpoint_wal_floor_survives_prebuilt_compaction_rebase() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("forced_seal_prebuilt_compaction");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
    ];
    let mut transaction = engine.begin_transaction().unwrap();
    transaction
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(3),
            Value::text("three"),
        ]))
        .unwrap();
    transaction.commit().unwrap();

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [
            InterleavePoint::CompactionBeforePublish,
            InterleavePoint::SealBeforePublish,
        ],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let (compaction_tx, compaction_rx) = std::sync::mpsc::channel();
    let compaction = std::thread::spawn(move || {
        compaction_tx
            .send(compacting_engine.compact_after_checkpoint_forced())
            .unwrap();
    });
    let first_compaction = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .expect("compaction must finish its private build");

    let checkpoint_engine = Arc::clone(&engine);
    let checkpoint =
        std::thread::spawn(move || checkpoint_engine.checkpoint_cycle_inner(true));
    let seal = controller
        .wait_for(
            InterleavePoint::SealBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .expect("forced checkpoint must reach DATA publication");

    controller.release(first_compaction);
    assert!(
        matches!(
            compaction_rx.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "compaction commit must wait for the checkpoint publication coordinator"
    );

    controller.release(seal);
    checkpoint.join().unwrap().unwrap();
    let checkpoint_floor = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap()
        .snapshot()
        .control()
        .wal_replay_floor();
    compaction_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("compaction result")
        .unwrap();
    compaction.join().unwrap();
    drop(schedule);

    assert_eq!(
        engine
            .version_stores
            .read()
            .unwrap()
            .get("items")
            .unwrap()
            .committed_row_count(),
        0,
        "forced checkpoint must drain every committed hot row"
    );
    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let after_compaction = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap();
    assert_eq!(
        after_compaction.snapshot().control().wal_replay_floor(),
        checkpoint_floor,
        "rebased compaction must preserve the newer checkpoint WAL floor"
    );
    let cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(cost.jobs_planned, 1);
    assert_eq!(cost.jobs_published, 1);
    assert_eq!(cost.jobs_invalidated, 0);
    drop(after_compaction);
    engine.close_engine().unwrap();

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let mut rows = collect_rows(&reopened, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    reopened.close_engine().unwrap();
}

#[test]
fn compaction_cleans_staged_output_when_selected_input_is_replaced() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_input_replacement");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
    ];
    let manager = engine.get_or_create_segment_manager("items");
    let original_ids = manager
        .manifest()
        .segments
        .iter()
        .map(|meta| meta.segment_id)
        .collect::<Vec<_>>();
    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforePublish],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let arrival = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    let staged_paths = artifact_paths_with_extension(&db_path.join("staging"), "data");
    assert!(
        !staged_paths.is_empty(),
        "compaction must pause with its output in the private V6 staging tree"
    );

    let _replacement_dir = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            3,
            Row::from_values(vec![Value::Integer(3), Value::text("replacement")]),
        )],
    );
    let replacement_id = manager
        .manifest()
        .segments
        .iter()
        .map(|meta| meta.segment_id)
        .find(|segment_id| !original_ids.contains(segment_id))
        .expect("replacement segment must be manifest-published");
    let replacement_meta = manager
        .manifest()
        .segments
        .iter()
        .find(|meta| meta.segment_id == replacement_id)
        .cloned()
        .unwrap();
    let replacement_volume = Arc::clone(
        &manager
            .segments_raw()
            .get(&replacement_id)
            .expect("replacement volume must be live")
            .volume,
    );
    manager
        .replace_segments_atomic(
            replacement_id,
            replacement_volume,
            replacement_meta,
            &[original_ids[0], replacement_id],
        )
        .unwrap();

    controller.release(arrival);
    let error = compaction.join().unwrap().unwrap_err();
    assert!(error.to_string().contains("input_snapshot_changed"));
    assert!(error.to_string().contains("no longer live"));
    assert!(
        staged_paths.iter().all(|path| !path.exists()),
        "invalidated compaction must remove every unpublished V6 DATA member"
    );

    let manifest_ids = manager
        .manifest()
        .segments
        .iter()
        .map(|meta| meta.segment_id)
        .collect::<Vec<_>>();
    assert_eq!(manifest_ids, vec![replacement_id, original_ids[1]]);
    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        vec![2, 3]
    );
    let cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(cost.jobs_planned, 1);
    assert_eq!(cost.jobs_published, 0);
    assert_eq!(cost.jobs_invalidated, 1);
    assert_eq!(cost.jobs_cancelled_input_snapshot, 1);
    assert_eq!(cost.last_cancellation_reason, "input_snapshot_changed");

    engine.close_engine().unwrap();
}

#[test]
fn compaction_yields_ddl_and_cancels_stale_output_job() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_cooperative_ddl_cancel");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config));
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                3,
                Row::from_values(vec![Value::Integer(3), Value::text("three")]),
            )],
        ),
    ];

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforePublish],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());
    let arrival = controller
        .wait_for(
            InterleavePoint::CompactionBeforePublish,
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    let staged_paths = artifact_paths_with_extension(&db_path.join("staging"), "data");
    assert!(
        !staged_paths.is_empty(),
        "test must pause after compaction has written an unpublished V6 DATA member"
    );

    let schema_epoch = engine.schema_epoch.load(Ordering::Acquire);
    let ddl_guard = engine
        .ddl_fence
        .try_write_for(Duration::from_millis(250))
        .expect("compaction output construction must not hold the DDL fence");
    engine.schema_epoch.fetch_add(1, Ordering::AcqRel);
    drop(ddl_guard);
    controller.release(arrival);

    let error = compaction.join().unwrap().unwrap_err();
    assert!(error.to_string().contains("schema_epoch_changed"));
    {
        let manager = engine.get_or_create_segment_manager("items");
        let manifest = manager.manifest();
        assert_eq!(manifest.segments.len(), 3);
        assert!(manifest
            .segments
            .iter()
            .all(|segment| segment.level == SegmentLevel::L0));
    }
    let cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(cost.jobs_planned, 1);
    assert_eq!(cost.jobs_published, 0);
    assert_eq!(cost.jobs_invalidated, 1);
    assert_eq!(cost.jobs_cancelled_schema_epoch, 1);
    assert_eq!(cost.last_cancellation_reason, "schema_epoch_changed");
    assert_eq!(cost.last_invalidation_reason, "schema_epoch_changed");
    assert_eq!(cost.last_outcome, "schema_epoch_changed");
    assert!(
        staged_paths.iter().all(|path| !path.exists()),
        "cancelled compaction must remove every staged V6 DATA member"
    );

    let ddl_guard = engine.ddl_fence.write();
    engine.schema_epoch.store(schema_epoch, Ordering::Release);
    drop(ddl_guard);
    engine.close_engine().unwrap();
}

#[test]
fn compaction_runs_distinct_tables_concurrently_with_bounded_ownership() {
    use crate::test_failpoints::{InterleaveGuard, InterleavePoint};

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_distinct_tables_concurrently");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.max_compaction_jobs = 2;
    config.persistence.max_compaction_output_bytes = 16 * 1024 * 1024;
    config.persistence.compaction_disk_reserve_bytes = 0;
    config.persistence.checkpoint_on_close = false;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = Arc::new(MVCCEngine::new(config));
    engine.open_engine().unwrap();
    let mut volume_dirs = Vec::new();
    for table_name in ["left_items", "right_items"] {
        let schema = SchemaBuilder::new(table_name)
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();
        create_catalog_test_table(&engine, schema.clone()).unwrap();
        for row_id in 1..=3 {
            let rows = [(
                row_id,
                Row::from_values(vec![
                    Value::Integer(row_id),
                    Value::text(format!("{table_name}-{row_id}")),
                ]),
            )];
            volume_dirs.push(register_artifact_volume(
                &engine, table_name, &schema, &rows,
            ));
        }
    }

    let schedule = InterleaveGuard::install_scoped(
        engine.schema_scope_id,
        [InterleavePoint::CompactionBeforeOutput],
    );
    let controller = schedule.controller();
    let compacting_engine = Arc::clone(&engine);
    let compaction =
        std::thread::spawn(move || compacting_engine.compact_after_checkpoint_forced());

    let first = controller
        .wait_for(
            InterleavePoint::CompactionBeforeOutput,
            None,
            Duration::from_secs(5),
        )
        .unwrap();
    let second = controller
        .wait_for(
            InterleavePoint::CompactionBeforeOutput,
            None,
            Duration::from_secs(5),
        )
        .expect("a second distinct table must enter while the first is paused");
    controller.release(first);
    controller.release(second);
    compaction.join().unwrap().unwrap();

    for table_name in ["left_items", "right_items"] {
        let manager = engine.get_or_create_segment_manager(table_name);
        let manifest = manager.manifest();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].level, SegmentLevel::L1);
    }
    let snapshot = engine.runtime_stats_snapshot();
    assert_eq!(snapshot.max_compaction_jobs, 2);
    assert_eq!(snapshot.compaction_active_jobs, 0);
    assert_eq!(snapshot.compaction_peak_active_jobs, 2);
    assert_eq!(
        snapshot
            .maintenance
            .compaction_cost
            .jobs_selected_sub_target_merge,
        2
    );
    assert_eq!(snapshot.maintenance.compaction_cost.jobs_planned, 2);
    assert_eq!(snapshot.maintenance.compaction_cost.jobs_published, 2);
    assert_eq!(snapshot.maintenance.compaction_cost.jobs_invalidated, 0);

    drop(volume_dirs);
    engine.close_engine().unwrap();
}

#[test]
fn identical_failed_compaction_is_deferred_without_rebuilding_output() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_retry_cooldown");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.checkpoint_interval = 0;
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compaction_disk_reserve_bytes = u64::MAX;
    config.persistence.compaction_retry_cooldown_ms = 60_000;
    config.persistence.l0_soft_limit_segments = usize::MAX - 1;
    config.persistence.l0_hard_limit_segments = usize::MAX;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("two")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                3,
                Row::from_values(vec![Value::Integer(3), Value::text("three")]),
            )],
        ),
    ];

    let error = engine.compact_after_checkpoint_forced().unwrap_err();
    assert_eq!(
        compaction_abort_reason(&error),
        Some("disk_reserve_exhausted")
    );
    engine.compact_after_checkpoint_forced().unwrap();

    let manager = engine.get_or_create_segment_manager("items");
    let manifest = manager.manifest();
    assert_eq!(manifest.segments.len(), 3);
    assert!(manifest
        .segments
        .iter()
        .all(|segment| segment.level == SegmentLevel::L0));
    let snapshot = engine.runtime_stats_snapshot();
    assert_eq!(
        snapshot
            .maintenance
            .compaction_cost
            .jobs_selected_sub_target_merge,
        2
    );
    assert_eq!(snapshot.maintenance.compaction_cost.jobs_planned, 1);
    assert_eq!(snapshot.maintenance.compaction_cost.jobs_deferred, 1);
    assert_eq!(
        snapshot
            .maintenance
            .compaction_cost
            .jobs_waited_retry_cooldown,
        1
    );
    assert_eq!(
        snapshot.maintenance.compaction_cost.jobs_cancelled_budget,
        1
    );
    assert_eq!(
        snapshot.maintenance.compaction_cost.last_wait_reason,
        "retry_cooldown"
    );
    assert_eq!(snapshot.compaction_retry_suppressed, 1);
    assert!(snapshot.compaction_retry_cooldown_active);
    assert_eq!(
        snapshot.compaction_retry_last_reason,
        "disk_reserve_exhausted"
    );

    engine.close_engine().unwrap();
}

#[test]
fn test_compaction_unique_output_uses_metadata_only_seal_index() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_unique_metadata_only");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = 100;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let batch_rows = 10usize;
    let mut next_id = 0i64;
    let mut _volume_dirs = Vec::new();
    for _ in 0..3 {
        let mut rows = Vec::with_capacity(batch_rows);
        for _ in 0..batch_rows {
            rows.push((
                next_id,
                Row::from_values(vec![
                    Value::Integer(next_id),
                    Value::text(format!("name-{next_id}")),
                ]),
            ));
            next_id += 1;
        }
        _volume_dirs.push(register_artifact_volume(&engine, "items", &schema, &rows));
    }

    commit_test_index(&engine, "items", "idx_name_unique", &["name"], true, None).unwrap();
    let table_id = radixdb_catalog::ObjectId::from_user_bytes(
        engine
            .schemas
            .read()
            .unwrap()
            .get("items")
            .expect("items runtime schema")
            .catalog_id,
    )
    .unwrap();
    // DDL advances the logical catalog independently. Publish its complete
    // replacement generation before maintenance so compaction reads exactly
    // the catalog selected by CONTROL.
    engine.checkpoint_cycle_inner(true).unwrap();

    assert_table_segments_artifact_backed(&engine, "items");
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        assert_eq!(
            mgr.get_segments_ordered_meta().len(),
            3,
            "test setup expects three sub-target sealed volumes before compaction"
        );
    }
    let before_compaction = engine
        .physical_generation
        .load_full()
        .expect("persistent engine owns a physical generation")
        .pin()
        .unwrap();
    let live_index_count = before_compaction
        .snapshot()
        .table_manifests()
        .iter()
        .find(|manifest| manifest.table_id() == table_id)
        .expect("items table manifest must be CONTROL-reachable")
        .segments()
        .iter()
        .filter(|segment| segment.index_artifact().is_some())
        .count();
    assert_eq!(live_index_count, 3);
    assert!(
        count_index_artifacts(&db_path) >= live_index_count,
        "retained predecessor generations may keep unreachable files until bounded GC"
    );
    drop(before_compaction);

    engine.compact_after_checkpoint_forced().unwrap();
    assert_table_segments_artifact_backed(&engine, "items");
    let compaction_cost = engine.runtime_stats_snapshot().maintenance.compaction_cost;
    assert_eq!(compaction_cost.jobs_planned, 1);
    assert_eq!(compaction_cost.jobs_published, 1);
    assert_eq!(compaction_cost.jobs_invalidated, 0);
    assert_eq!(compaction_cost.jobs_failed, 0);
    assert_eq!(compaction_cost.total_published_input_rows, 30);
    assert_eq!(compaction_cost.total_published_output_rows, 30);
    assert!(compaction_cost.total_published_output_bytes > 0);
    assert_eq!(compaction_cost.posting_outputs_generated, 1);
    assert_eq!(compaction_cost.posting_outputs_published, 1);
    assert_eq!(compaction_cost.manifest_publications, 1);
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let segments = mgr.get_segments_ordered_meta();
        assert_eq!(
            segments.len(),
            1,
            "compaction should merge the three sub-target volumes"
        );
        assert_eq!(segments[0].meta.row_count, batch_rows * 3);
        assert!(
            segments[0].has_exact_postings(&[1usize]),
            "compaction output must carry descriptor-backed UNIQUE postings"
        );
    }
    let physical = engine
        .physical_generation
        .load_full()
        .expect("persistent engine owns a physical generation")
        .pin()
        .unwrap();
    let live_index_count = physical
        .snapshot()
        .table_manifests()
        .iter()
        .find(|manifest| manifest.table_id() == table_id)
        .expect("items table manifest must be CONTROL-reachable")
        .segments()
        .iter()
        .filter(|segment| segment.index_artifact().is_some())
        .count();
    assert_eq!(
        live_index_count, 1,
        "current CONTROL generation must retire predecessor INDEX members"
    );
    assert!(
        count_index_artifacts(&db_path) >= live_index_count,
        "retained predecessor generations may keep unreachable files until bounded GC"
    );

    let err = {
        let mut tx = engine.begin_transaction().unwrap();
        let mut table = tx.get_table("items").unwrap();
        let err = table
            .insert(Row::from_values(vec![
                Value::Integer(next_id + 1),
                Value::text("name-1"),
            ]))
            .expect_err("duplicate insert must probe compacted V6 UNIQUE index");
        tx.rollback().unwrap();
        err
    };
    assert!(err.to_string().contains("unique constraint"));

    engine.close_engine().unwrap();
}

#[test]
fn test_compaction_kway_keeps_newest_overlapping_row_id() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_artifact_kway_newest");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[
                (
                    1,
                    Row::from_values(vec![Value::Integer(1), Value::text("old")]),
                ),
                (
                    3,
                    Row::from_values(vec![Value::Integer(3), Value::text("gamma")]),
                ),
            ],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[
                (
                    1,
                    Row::from_values(vec![Value::Integer(1), Value::text("middle")]),
                ),
                (
                    2,
                    Row::from_values(vec![Value::Integer(2), Value::text("beta")]),
                ),
            ],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[
                (
                    1,
                    Row::from_values(vec![Value::Integer(1), Value::text("newest")]),
                ),
                (
                    4,
                    Row::from_values(vec![Value::Integer(4), Value::text("delta")]),
                ),
            ],
        ),
    ];

    engine.compact_after_checkpoint_forced().unwrap();
    assert_table_segments_artifact_backed(&engine, "items");

    let mut rows = collect_rows(&engine, "items");
    rows.sort_by_key(|(row_id, _)| *row_id);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, 1);
    assert_eq!(
        rows[0].1.get(1),
        Some(&Value::text("newest")),
        "k-way compaction must keep the newest segment for overlapping row_id"
    );
    assert_eq!(rows[1].1.get(1), Some(&Value::text("beta")));
    assert_eq!(rows[2].1.get(1), Some(&Value::text("gamma")));
    assert_eq!(rows[3].1.get(1), Some(&Value::text("delta")));

    engine.close_engine().unwrap();
}

#[test]
fn r3_l03_batch_a_compaction_uses_manifest_recency_not_numeric_segment_id() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_manifest_recency");
    let mut config = Config::with_path(db_path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let _volume_dirs = [
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("id-1")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("id-2")]),
            )],
        ),
        register_artifact_volume(
            &engine,
            "items",
            &schema,
            &[(
                1,
                Row::from_values(vec![Value::Integer(1), Value::text("id-3")]),
            )],
        ),
    ];

    // Canonical recency is manifest position. Make the lowest numeric ID
    // newest to prove compaction does not infer chronology from the ID.
    let mgr = engine.get_or_create_segment_manager("items");
    mgr.manifest_mut().segments.rotate_left(1);

    engine.compact_after_checkpoint_forced().unwrap();
    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get(1), Some(&Value::text("id-1")));
    engine.close_engine().unwrap();
}

#[test]
fn r8_l01_batch_e_compaction_tombstones_stream_against_selected_row_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compact_artifact_kway_all_tombstoned");
    let db_path_str = db_path.to_string_lossy().to_string();
    let mut config = Config::with_path(&db_path_str);
    config.persistence.target_volume_rows = 10;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let mut _volume_dirs = Vec::new();
    for row_id in 1..=3i64 {
        let rows = [(
            row_id,
            Row::from_values(vec![
                Value::Integer(row_id),
                Value::text(format!("name-{row_id}")),
            ]),
        )];
        _volume_dirs.push(register_artifact_volume(&engine, "items", &schema, &rows));
    }

    let mgr = engine.get_or_create_segment_manager("items");
    mgr.add_tombstones(&[1, 2, 3, 999], 1);

    engine.compact_after_checkpoint_forced().unwrap();
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        assert!(
            mgr.get_segments_ordered_meta().is_empty(),
            "all-tombstoned compaction should remove old segments without writing an empty volume"
        );
        assert_eq!(mgr.tombstone_count(), 1);
        assert_eq!(mgr.tombstone_set_arc().get(&999), Some(&1));
    }
    assert!(collect_rows(&engine, "items").is_empty());

    engine.close_engine().unwrap();
}

#[test]
fn r8_l01_batch_e_compaction_uses_one_operation_wide_decode_cache() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _dir1 = register_artifact_volume(
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
    let _dir2 = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[
            (
                3,
                Row::from_values(vec![Value::Integer(3), Value::text("carol")]),
            ),
            (
                4,
                Row::from_values(vec![Value::Integer(4), Value::text("dave")]),
            ),
        ],
    );

    let volumes = {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        mgr.get_segments_ordered_meta()
    };
    assert_eq!(volumes.len(), 2);
    let mapping = crate::volume::writer::ColumnMapping::identity(&volumes[0]);
    let mut cache = crate::volume::writer::CompactionBlockCache::default();

    crate::instrumentation::begin_volume_read_probe();
    let row_0 = volumes[0]
        .get_row_for_compaction_cached(0, &mapping, &mut cache)
        .unwrap();
    let row_1 = volumes[0]
        .get_row_for_compaction_cached(1, &mapping, &mut cache)
        .unwrap();
    let row_2 = volumes[1]
        .get_row_for_compaction_cached(0, &mapping, &mut cache)
        .unwrap();
    let row_3 = volumes[0]
        .get_row_for_compaction_cached(1, &mapping, &mut cache)
        .unwrap();
    let counters = crate::instrumentation::end_volume_read_probe();

    assert_eq!(row_0.get(1), Some(&Value::text("alice")));
    assert_eq!(row_1.get(1), Some(&Value::text("bob")));
    assert_eq!(row_2.get(1), Some(&Value::text("carol")));
    assert_eq!(row_3.get(1), Some(&Value::text("bob")));
    assert_eq!(cache.resident_segment_count(), 2);
    assert_eq!(cache.resident_block_count(), 4);
    assert_eq!(
        counters.calls, 4,
        "interleaved inputs must each load their row-group blocks only once"
    );

    engine.close_engine().unwrap();
}

#[test]
fn test_artifact_metadata_tombstones_visibility_do_not_read_blocks() {
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

    crate::instrumentation::begin_volume_read_probe();
    {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");

        mgr.add_tombstones(&[2], 1);
        mgr.recompute_visibility();

        let raw = mgr.segments_raw();
        assert_eq!(raw.len(), 1);
        let cold = raw.values().next().unwrap();
        assert!(
            cold.volume.is_cold(),
            "test must keep the artifact-backed segment metadata-only"
        );
        assert_eq!(cold.volume.meta.row_ids, vec![1, 2]);
        let _visibility_bits = cold.visible.as_ref().map(|bits| bits.len()).unwrap_or(0);

        let metadata_only = mgr.get_segments_ordered_meta();
        assert_eq!(metadata_only.len(), 1);
        assert!(metadata_only[0].is_cold());
        assert_eq!(metadata_only[0].meta.row_count, 2);

        let tombstones = mgr.tombstone_set_arc();
        assert_eq!(tombstones.get(&2), Some(&1));
    }
    let counters = crate::instrumentation::end_volume_read_probe();
    assert_eq!(
        counters.calls, 0,
        "metadata, tombstones and visibility recomputation must not read artifact-backed payload blocks"
    );
    assert_eq!(counters.bytes, 0);

    engine.close_engine().unwrap();
}

#[test]
fn test_compaction_row_ref_is_compact_and_checked() {
    assert_eq!(
        std::mem::size_of::<CompactionRowRef>(),
        16,
        "compaction row refs are kept as i64 + u32 + u32, not pointer-sized indices"
    );

    let row_ref = CompactionRowRef::new(42, 7, 9).unwrap();
    assert_eq!(row_ref.row_id, 42);
    assert_eq!(row_ref.vol_idx(), 7);
    assert_eq!(row_ref.row_idx(), 9);

    assert!(CompactionRowRef::new(1, u32::MAX as usize + 1, 0).is_err());
    assert!(CompactionRowRef::new(1, 0, u32::MAX as usize + 1).is_err());
}

#[test]
fn test_compaction_row_ref_packed_roundtrip() {
    let row_ref = CompactionRowRef::new(-7, 123, 456).unwrap();
    let encoded = row_ref.encode();
    assert_eq!(encoded.len(), COMPACTION_ROW_REF_ENCODED_LEN);
    assert_eq!(CompactionRowRef::decode(&encoded), row_ref);
}

#[test]
fn r8_l01_batch_e_compaction_spool_buffers_sequential_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = CompactionRowRefSpool::create(dir.path(), "items").unwrap();
    let spool_path = spool.path.clone();

    for i in 0..(ROW_GROUP_SIZE + 2) {
        spool
            .append(CompactionRowRef::new(i as i64, i % 3, i).unwrap())
            .unwrap();
    }

    assert!(spool_path.exists());
    assert_eq!(spool.len(), ROW_GROUP_SIZE + 2);
    assert_eq!(spool.ref_at(0).unwrap().row_id, 0);
    assert_eq!(
        spool.ref_at(ROW_GROUP_SIZE).unwrap().row_id,
        ROW_GROUP_SIZE as i64
    );
    assert_eq!(
        spool
            .cached_range
            .as_ref()
            .map(|range| (range.start, range.end)),
        Some((ROW_GROUP_SIZE, ROW_GROUP_SIZE + 2))
    );
    assert_eq!(
        spool.ref_at(ROW_GROUP_SIZE + 1).unwrap().row_id,
        (ROW_GROUP_SIZE + 1) as i64
    );
    let maximum_flushes =
        (spool.len() * COMPACTION_ROW_REF_ENCODED_LEN).div_ceil(COMPACTION_SPOOL_APPEND_BYTES);
    assert_eq!(spool.append_flushes, maximum_flushes);
    assert!(spool.append_flushes * 1000 < spool.len());

    drop(spool);
    assert!(
        !spool_path.exists(),
        "compaction row ref spool temp file must be removed on drop"
    );
}

#[test]
fn compaction_spool_range_rebases_indices_without_copying_refs() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = CompactionRowRefSpool::create(dir.path(), "items").unwrap();
    for row_id in 1..=4 {
        spool
            .append(CompactionRowRef::new(row_id, 0, (row_id - 1) as usize).unwrap())
            .unwrap();
    }

    let mut range = CompactionRowRefs::Spool {
        spool: &mut spool,
        range: 1..3,
    };
    assert_eq!(range.len(), 2);
    assert_eq!(range.ref_at(0).unwrap().row_id, 2);
    assert_eq!(range.ref_at(1).unwrap().row_id, 3);
    assert!(range.ref_at(2).is_err());
}

#[test]
fn test_compaction_row_source_reads_values_from_artifact_blocks() {
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

    let volume = {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let segments = mgr.get_segments_ordered_meta();
        Arc::clone(&segments[0])
    };
    let mapping = crate::volume::writer::ColumnMapping::identity(&volume);
    let refs = vec![
        CompactionRowRef::new(1, 0, 0).unwrap(),
        CompactionRowRef::new(2, 0, 1).unwrap(),
    ];
    let volumes = vec![(1, volume)];
    let mappings = vec![mapping];
    let mut cache = crate::volume::writer::CompactionBlockCache::default();
    let mut source = CompactionSealRowSource::new(&refs, &volumes, &mappings, &mut cache);

    source
        .with_value(0, 1, |value| {
            assert_eq!(value, &Value::text("alice"));
            Ok(())
        })
        .unwrap();
    source
        .with_value(1, 1, |value| {
            assert_eq!(value, &Value::text("bob"));
            Ok(())
        })
        .unwrap();

    engine.close_engine().unwrap();
}

#[test]
fn test_compaction_row_source_reads_spooled_refs() {
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

    let volume = {
        let mgrs = engine.segment_managers.read().unwrap();
        let mgr = mgrs.get("items").expect("items segment manager");
        let segments = mgr.get_segments_ordered_meta();
        Arc::clone(&segments[0])
    };
    let mapping = crate::volume::writer::ColumnMapping::identity(&volume);
    let temp = tempfile::tempdir().unwrap();
    let mut refs = CompactionRowRefSpool::create(temp.path(), "items").unwrap();
    refs.append(CompactionRowRef::new(1, 0, 0).unwrap())
        .unwrap();
    refs.append(CompactionRowRef::new(2, 0, 1).unwrap())
        .unwrap();

    let volumes = vec![(1, volume)];
    let mappings = vec![mapping];
    let mut cache = crate::volume::writer::CompactionBlockCache::default();
    let mut source =
        CompactionSealRowSource::new_spooled(&mut refs, &volumes, &mappings, &mut cache);

    assert_eq!(source.row_id(0).unwrap(), 1);
    source
        .with_value(1, 1, |value| {
            assert_eq!(value, &Value::text("bob"));
            Ok(())
        })
        .unwrap();

    engine.close_engine().unwrap();
}

#[test]
fn test_full_scan_on_artifact_segment_stays_lazy() {
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
                Row::from_values(vec![Value::Integer(1), Value::text("alpha")]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(2), Value::text("beta")]),
            ),
        ],
    );

    assert_table_segments_artifact_backed(&engine, "items");

    let tx = engine.begin_transaction().unwrap();
    let table = tx.get_table("items").unwrap();
    let mut scanner = table.scan(&[0, 1], None).unwrap();
    let mut rows = Vec::new();
    while scanner.next() {
        rows.push(scanner.take_row());
    }
    scanner.close().unwrap();
    drop(table);
    drop(tx);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(1), Some(&Value::text("alpha")));
    assert_eq!(rows[1].get(1), Some(&Value::text("beta")));
    assert_table_segments_artifact_backed(&engine, "items");

    engine.close_engine().unwrap();
}
