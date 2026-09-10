#[test]
fn create_index_rewrites_only_its_target_table_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("index-table-local-publication");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    for table_name in ["indexed_items", "unrelated_items"] {
        create_catalog_test_table(
            &engine,
            SchemaBuilder::new(table_name)
                .column("id", DataType::Integer, false, true)
                .column("value", DataType::Integer, false, false)
                .build(),
        )
        .unwrap();
        let mut transaction = engine.begin_transaction().unwrap();
        transaction
            .get_table(table_name)
            .unwrap()
            .insert(Row::from_values(vec![Value::Integer(1), Value::Integer(10)]))
            .unwrap();
        transaction.commit().unwrap();
    }
    engine.checkpoint_cycle_inner(true).unwrap();

    let catalog = engine.pin_catalog().unwrap();
    let indexed_id = catalog
        .find_relation(
            radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE,
            "indexed_items",
        )
        .unwrap()
        .unwrap()
        .id();
    let unrelated_id = catalog
        .find_relation(
            radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE,
            "unrelated_items",
        )
        .unwrap()
        .unwrap()
        .id();
    drop(catalog);

    let (indexed_before, unrelated_before, catalog_before) = {
        let publisher = engine.physical_generation.load_full().unwrap();
        let lease = publisher.pin().unwrap();
        let snapshot = lease.snapshot();
        let reference = |table_id| {
            snapshot
                .database_manifest()
                .tables()
                .iter()
                .copied()
                .find(|reference| reference.table_id() == table_id)
                .unwrap()
        };
        (
            reference(indexed_id),
            reference(unrelated_id),
            snapshot.database_manifest().catalog().generation(),
        )
    };

    commit_test_index(
        &engine,
        "indexed_items",
        "idx_indexed_value",
        &["value"],
        false,
        Some(IndexType::BTree),
    )
    .unwrap();

    {
        let publisher = engine.physical_generation.load_full().unwrap();
        let lease = publisher.pin().unwrap();
        let snapshot = lease.snapshot();
        let reference = |table_id| {
            snapshot
                .database_manifest()
                .tables()
                .iter()
                .copied()
                .find(|reference| reference.table_id() == table_id)
                .unwrap()
        };
        let indexed_after = reference(indexed_id);
        let unrelated_after = reference(unrelated_id);
        let catalog_after = snapshot.database_manifest().catalog().generation();
        assert_ne!(indexed_after, indexed_before);
        assert_eq!(unrelated_after, unrelated_before);
        assert!(catalog_after.get() > catalog_before.get());
        assert_eq!(
            snapshot
                .table_manifest(unrelated_id)
                .unwrap()
                .catalog_generation(),
            catalog_before,
            "unrelated table must retain its table-local physical catalog binding"
        );
    }

    engine.close_engine().unwrap();
    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert_eq!(collect_rows(&reopened, "indexed_items").len(), 1);
    assert_eq!(collect_rows(&reopened, "unrelated_items").len(), 1);
    let transaction = reopened.begin_transaction().unwrap();
    assert_eq!(
        transaction
            .get_table("indexed_items")
            .unwrap()
            .collect_row_ids_by_index_values("value", &[Value::Integer(10)])
            .unwrap()
            .unwrap(),
        vec![1]
    );
    drop(transaction);
    reopened.close_engine().unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn sequential_create_index_reuses_unchanged_physical_accelerators() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("sequential-index-page-reuse");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("left_key", DataType::Integer, false, false)
            .column("right_key", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();
    let mut transaction = engine.begin_transaction().unwrap();
    let mut table = transaction.get_table("items").unwrap();
    for value in 0..32_i64 {
        table
            .insert(Row::from_values(vec![
                Value::Integer(value),
                Value::Integer(value % 5),
                Value::Integer(value % 7),
            ]))
            .unwrap();
    }
    drop(table);
    transaction.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();

    commit_test_index(
        &engine,
        "items",
        "idx_left_key",
        &["left_key"],
        false,
        Some(IndexType::BTree),
    )
    .unwrap();
    crate::v6::reset_publication_diagnostics();
    commit_test_index(
        &engine,
        "items",
        "idx_right_key",
        &["right_key"],
        false,
        Some(IndexType::Hash),
    )
    .unwrap();

    let diagnostics = crate::v6::publication_diagnostics();
    assert_eq!(diagnostics.rebuild_invocations, 1);
    assert_eq!(diagnostics.source_stream_passes, 1);
    assert_eq!(diagnostics.source_rows, 32);
    assert_eq!(
        diagnostics.index_planning_passes, 1,
        "only the newly requested accelerator may be planned from DATA"
    );
    assert_eq!(
        diagnostics.index_encoding_passes, 1,
        "unchanged accelerator pages must be copied from the selected pack"
    );

    engine.close_engine().unwrap();
    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let transaction = reopened.begin_transaction().unwrap();
    let table = transaction.get_table("items").unwrap();
    assert_eq!(
        table
            .collect_row_ids_by_index_values("left_key", &[Value::Integer(3)])
            .unwrap()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        table
            .collect_row_ids_by_index_values("right_key", &[Value::Integer(3)])
            .unwrap()
            .unwrap()
            .len(),
        5
    );
    drop(transaction);
    reopened.close_engine().unwrap();
}

#[test]
fn repeated_catalog_publication_schedules_bounded_metadata_retirement() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("bounded-catalog-retirement");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;

    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let mut builder = SchemaBuilder::new("catalog_retirement")
        .column("id", DataType::Integer, false, true);
    for ordinal in 0..40 {
        builder = builder.column(
            format!("value_{ordinal:02}"),
            DataType::Integer,
            false,
            false,
        );
    }
    create_catalog_test_table(&engine, builder.build()).unwrap();
    let mut transaction = engine.begin_transaction().unwrap();
    let mut table = transaction.get_table("catalog_retirement").unwrap();
    table
        .insert(Row::from_values(
            (0..=40).map(Value::Integer).collect(),
        ))
        .unwrap();
    drop(table);
    transaction.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();
    for ordinal in 0..40 {
        let index_name = format!("idx_catalog_retirement_{ordinal:02}");
        let column_name = format!("value_{ordinal:02}");
        commit_test_index(
            &engine,
            "catalog_retirement",
            &index_name,
            &[&column_name],
            false,
            Some(IndexType::BTree),
        )
        .unwrap();
    }

    let retirement = engine
        .physical_generation
        .load_full()
        .expect("persistent engine owns a physical generation")
        .run_scheduled_retirement()
        .unwrap();
    assert!(retirement.is_some(), "retirement was not scheduled");
    let final_catalogs = std::fs::read_dir(database_path.join("catalog"))
        .unwrap()
        .count();
    assert!(
        final_catalogs <= 16,
        "scheduled two-cycle retirement left {final_catalogs} final catalog packs"
    );
    assert!(
        std::fs::read_dir(database_path.join("staging"))
            .unwrap()
            .next()
            .is_none(),
        "successful publications must not retain staging writer directories"
    );
    engine.close_engine().unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn sequential_create_index_rebuilds_a_damaged_reuse_candidate_from_data() {
    use std::io::{Read as _, Seek as _, Write as _};

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("damaged-index-page-reuse");
    let mut config = Config::with_path(database_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_interval = u32::MAX;
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = u32::MAX;

    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .column("id", DataType::Integer, false, true)
            .column("left_key", DataType::Integer, false, false)
            .column("right_key", DataType::Integer, false, false)
            .build(),
    )
    .unwrap();
    let mut transaction = engine.begin_transaction().unwrap();
    let mut table = transaction.get_table("items").unwrap();
    for value in 0..32_i64 {
        table
            .insert(Row::from_values(vec![
                Value::Integer(value),
                Value::Integer(value % 5),
                Value::Integer(value % 7),
            ]))
            .unwrap();
    }
    drop(table);
    transaction.commit().unwrap();
    engine.checkpoint_cycle_inner(true).unwrap();
    commit_test_index(
        &engine,
        "items",
        "idx_left_key",
        &["left_key"],
        false,
        Some(IndexType::BTree),
    )
    .unwrap();

    let catalog = engine.pin_catalog().unwrap();
    let table_id = catalog
        .find_relation(radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE, "items")
        .unwrap()
        .unwrap()
        .id();
    drop(catalog);
    let descriptor = {
        let publisher = engine.physical_generation.load_full().unwrap();
        let lease = publisher.pin().unwrap();
        lease
            .snapshot()
            .table_manifest(table_id)
            .unwrap()
            .segments()[0]
    };
    let data = crate::v6::ArtifactDataSource::open(
        database_path.join(descriptor.data_artifact().relative_path()),
        descriptor.data_artifact(),
    )
    .unwrap();
    let index_reference = descriptor.index_artifact().unwrap();
    let index_path = database_path.join(index_reference.relative_path());
    let index = crate::v6::ArtifactIndexSource::open(&index_path, index_reference, Arc::new(data))
        .unwrap();
    let page = index.layout().pages()[0];
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_path)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(page.offset())).unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(std::io::SeekFrom::Start(page.offset())).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    drop(file);
    drop(index);

    crate::v6::reset_publication_diagnostics();
    commit_test_index(
        &engine,
        "items",
        "idx_right_key",
        &["right_key"],
        false,
        Some(IndexType::Hash),
    )
    .unwrap();
    let diagnostics = crate::v6::publication_diagnostics();
    assert_eq!(diagnostics.rebuild_invocations, 1);
    assert_eq!(diagnostics.source_stream_passes, 1);
    assert_eq!(diagnostics.source_rows, 32);
    assert_eq!(
        diagnostics.index_planning_passes, 2,
        "the new accelerator and damaged reuse candidate must rebuild from DATA"
    );

    engine.close_engine().unwrap();
    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let transaction = reopened.begin_transaction().unwrap();
    let table = transaction.get_table("items").unwrap();
    assert_eq!(
        table
            .collect_row_ids_by_index_values("left_key", &[Value::Integer(3)])
            .unwrap()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        table
            .collect_row_ids_by_index_values("right_key", &[Value::Integer(3)])
            .unwrap()
            .unwrap()
            .len(),
        5
    );
    drop(transaction);
    reopened.close_engine().unwrap();
}

#[test]
fn create_index_physical_failure_completes_runtime_cold_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("create_index_runtime_fallback");
    let mut config = Config::with_path(db_path.to_string_lossy().into_owned());
    config.persistence.checkpoint_on_close = false;
    config.persistence.compact_threshold = 9999;

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
        table
            .insert(Row::from_values(vec![
                Value::Integer(1),
                Value::text("alice"),
            ]))
            .unwrap();
        table
            .insert(Row::from_values(vec![
                Value::Integer(2),
                Value::text("bob"),
            ]))
            .unwrap();
        tx.commit().unwrap();
    }
    engine.checkpoint_cycle_inner(true).unwrap();
    assert_table_segments_artifact_backed(&engine, "items");

    // The typed catalog mutation remains durable even if the rebuildable
    // accelerator generation cannot create staging. The commit must complete
    // the deferred cold population in runtime before readers are released.
    let staging = db_path.join("staging");
    if staging.is_dir() {
        std::fs::remove_dir_all(&staging).unwrap();
    }
    std::fs::write(&staging, b"force physical publication failure").unwrap();
    commit_test_index(
        &engine,
        "items",
        "idx_name",
        &["name"],
        false,
        Some(IndexType::Hash),
    )
    .unwrap();

    let index = engine.get_index("items", "idx_name").unwrap();
    assert_eq!(
        index
            .get_row_ids_equal(&[Value::text("alice")])
            .unwrap()
            .into_vec(),
        vec![1],
        "runtime fallback must contain cold rows when immutable publication fails"
    );
    assert!(
        engine
            .segment_managers
            .read()
            .unwrap()
            .get("items")
            .unwrap()
            .is_cold_populated_index("idx_name"),
        "fallback index must remain the declared cold runtime owner"
    );

    std::fs::remove_file(&staging).unwrap();
    std::fs::create_dir(&staging).unwrap();
    engine.close_engine().unwrap();
}
