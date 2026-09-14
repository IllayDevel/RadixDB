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
