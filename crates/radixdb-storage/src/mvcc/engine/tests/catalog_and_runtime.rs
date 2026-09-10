use super::*;
use crate::index::{
    BTreeIndex, BitmapIndex, HashIndex, HnswDistanceMetric, HnswIndex, MultiColumnIndex, PkIndex,
};
use radixdb_catalog::{
    AclEntryPayload, CatalogEdge, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject,
    CatalogPayload, EdgeKind, ObjectId, ProcedurePayload, ProceduralSource, ResourcePolicy,
    RoutineDefinition, RoutineResult, SecurityMode, Volatility, PRIVILEGE_EXECUTE,
    PROCEDURAL_CATALOG_MINOR,
};
use radixdb_core::{DataType, ForeignKeyAction, IndexType, Row, SchemaBuilder, Value};
use std::sync::mpsc;

fn bind_catalog_test_view_dependencies(query: &str) -> Result<Vec<String>> {
    match query {
        "SELECT * FROM source_rows" => Ok(vec!["source_rows".to_string()]),
        "SELECT * FROM inner_view" => Ok(vec!["inner_view".to_string()]),
        "SELECT * FROM viewed" => Ok(vec!["viewed".to_string()]),
        "SELECT 'lexical_name' AS note FROM source_rows" => Ok(vec!["source_rows".to_string()]),
        _ => Err(Error::invalid_argument(
            "unexpected catalog test view query",
        )),
    }
}
fn create_bound_catalog_test_view(
    engine: &MVCCEngine,
    name: &str,
    query: &str,
    dependencies: &[&str],
) -> Result<()> {
    create_catalog_test_view(engine, name, query, dependencies)
}

#[test]
fn bounded_compaction_selection_preserves_manifest_ranges_and_work_limits() {
    let candidate = |manifest_index, physical_bytes, must_rewrite| CompactionCandidate {
        manifest_index,
        physical_bytes,
        must_rewrite,
    };
    let candidates = [
        candidate(0, 10, false),
        candidate(1, 10, false),
        candidate(2, 10, false),
        candidate(4, 5, true),
    ];

    assert_eq!(
        select_bounded_compaction_run(&candidates, 2, u64::MAX),
        vec![0, 1]
    );
    assert_eq!(
        select_bounded_compaction_run(&candidates, 8, 25),
        vec![0, 1]
    );
    assert_eq!(select_bounded_compaction_run(&candidates, 8, 9), vec![4]);
    assert!(select_bounded_compaction_run(&[candidate(7, 1, false)], 8, 8).is_empty());
}

#[test]
fn compaction_scheduler_prioritizes_writer_pressure_deterministically() {
    let limits = L0PressureLimits {
        soft_segments: 16,
        hard_segments: 32,
        soft_bytes: 1_024,
        hard_bytes: 2_048,
        soft_wait: Duration::from_millis(100),
    };
    let mut candidates = vec![
        (
            "normal".to_string(),
            crate::volume::manifest::L0DebtSnapshot {
                segments: 8,
                physical_bytes: 512,
            },
        ),
        (
            "soft_bytes".to_string(),
            crate::volume::manifest::L0DebtSnapshot {
                segments: 3,
                physical_bytes: 1_024,
            },
        ),
        (
            "hard".to_string(),
            crate::volume::manifest::L0DebtSnapshot {
                segments: 32,
                physical_bytes: 128,
            },
        ),
        (
            "soft_segments".to_string(),
            crate::volume::manifest::L0DebtSnapshot {
                segments: 20,
                physical_bytes: 128,
            },
        ),
        (
            "alpha".to_string(),
            crate::volume::manifest::L0DebtSnapshot {
                segments: 4,
                physical_bytes: 64,
            },
        ),
    ];

    prioritize_compaction_tables(&mut candidates, limits);

    assert_eq!(
        candidates
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["hard", "soft_segments", "soft_bytes", "normal", "alpha"]
    );
}

#[test]
fn background_compaction_wave_replans_between_bounded_batches() {
    let mut candidates = vec!["urgent".to_string(), "cold-a".to_string(), "cold-b".to_string()];

    assert!(limit_compaction_wave(&mut candidates, Some(1)));
    assert_eq!(candidates, ["urgent"]);

    let mut final_wave = vec!["cold-a".to_string()];
    assert!(!limit_compaction_wave(&mut final_wave, Some(1)));
    assert_eq!(final_wave, ["cold-a"]);

    let mut forced = vec!["urgent".to_string(), "cold-a".to_string()];
    assert!(!limit_compaction_wave(&mut forced, None));
    assert_eq!(forced, ["urgent", "cold-a"]);
}

#[test]
fn compaction_execution_budget_bounds_admission_and_generated_output() {
    assert_eq!(effective_compaction_input_budget(500, 300, 0, 0), 300);
    assert_eq!(
        effective_compaction_input_budget(5_000, 4_000, 2_000, 1_000),
        1_000
    );
    assert_eq!(compaction_io_rate_per_job(0, 4), 0);
    assert_eq!(compaction_io_rate_per_job(1_000, 1), 1_000);
    assert_eq!(compaction_io_rate_per_job(1_000, 4), 250);
    assert_eq!(compaction_io_rate_per_job(2, 8), 1);

    let mut budget = CompactionExecutionBudget::new(0, 0, 10, 0);
    let live = || Ok(());
    budget.account_output_bytes(6, &live).unwrap();
    let error = budget.account_output_bytes(5, &live).unwrap_err();
    assert_eq!(
        compaction_abort_reason(&error),
        Some("output_budget_exceeded")
    );
}

#[test]
fn bounded_compaction_scheduler_rejects_duplicate_table_ownership() {
    let tables = vec!["items".to_string(), "items".to_string()];
    let error = run_bounded_compaction_jobs(&tables, 2, &|_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("duplicate table owner"));
}

#[test]
fn bounded_compaction_scheduler_replans_a_stale_physical_publisher() {
    let attempts = AtomicU64::new(0);
    run_bounded_compaction_jobs(&["items".to_string()], 1, &|_| {
        if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
            Err(compaction_abort(
                COMPACTION_STALE_PUBLICATION_REASON,
                "test publication race",
            ))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[test]
fn bounded_compaction_scheduler_reports_a_continuously_stale_publisher() {
    let attempts = AtomicU64::new(0);
    let error = run_bounded_compaction_jobs(&["items".to_string()], 1, &|_| {
        attempts.fetch_add(1, Ordering::Relaxed);
        Err(compaction_abort(
            COMPACTION_STALE_PUBLICATION_REASON,
            "test publication race",
        ))
    })
    .unwrap_err();
    assert_eq!(
        attempts.load(Ordering::Relaxed),
        (COMPACTION_STALE_REPLAN_LIMIT + 1) as u64
    );
    assert_eq!(
        compaction_abort_reason(&error),
        Some(COMPACTION_STALE_PUBLICATION_REASON)
    );
}

#[test]
fn compaction_job_concurrency_guard_tracks_active_and_peak() {
    let state = CompactionJobConcurrencyState::default();
    let first = state.start_job();
    assert_eq!(state.active(), 1);
    let second = state.start_job();
    assert_eq!(state.active(), 2);
    assert_eq!(state.peak(), 2);
    drop(first);
    assert_eq!(state.active(), 1);
    drop(second);
    assert_eq!(state.active(), 0);
    assert_eq!(state.peak(), 2);
}

#[test]
fn compaction_retry_cooldown_only_defers_the_exact_failed_job() {
    let cooldown = CompactionRetryCooldown::default();
    let signature = compaction_job_signature("items", 7, &[11, 12, 13]);
    let changed_inputs = compaction_job_signature("items", 7, &[11, 12, 14]);

    cooldown.record(signature, 60_000, "disk_reserve_exhausted");
    assert!(!cooldown.should_defer(changed_inputs));
    assert!(cooldown.should_defer(signature));
    assert!(cooldown.should_defer(signature));
    cooldown.record(changed_inputs, 60_000, "time_budget_exceeded");
    assert!(cooldown.should_defer(signature));
    assert!(cooldown.should_defer(changed_inputs));
    let (active, until, suppressed, reason) = cooldown.snapshot();
    assert!(active);
    assert!(until >= runtime_unix_millis());
    assert_eq!(suppressed, 4);
    assert_eq!(reason, "time_budget_exceeded");

    cooldown.clear(signature);
    assert!(!cooldown.should_defer(signature));
    assert!(cooldown.should_defer(changed_inputs));

    cooldown.record(signature, 0, "disabled");
    assert!(!cooldown.should_defer(signature));
    cooldown.clear(changed_inputs);
    assert!(!cooldown.snapshot().0);
}

#[test]
fn l0_pressure_classification_has_ordered_soft_and_hard_boundaries() {
    use crate::volume::manifest::L0DebtSnapshot;

    let limits = L0PressureLimits {
        soft_segments: 4,
        hard_segments: 8,
        soft_bytes: 100,
        hard_bytes: 200,
        soft_wait: Duration::from_millis(10),
    };
    assert_eq!(
        classify_l0_pressure(
            L0DebtSnapshot {
                segments: 3,
                physical_bytes: 99,
            },
            limits,
        ),
        L0PressureLevel::Normal
    );
    assert_eq!(
        classify_l0_pressure(
            L0DebtSnapshot {
                segments: 4,
                physical_bytes: 1,
            },
            limits,
        ),
        L0PressureLevel::Soft
    );
    assert_eq!(
        classify_l0_pressure(
            L0DebtSnapshot {
                segments: 1,
                physical_bytes: 200,
            },
            limits,
        ),
        L0PressureLevel::Hard
    );
}

#[test]
fn hard_l0_backpressure_rejects_before_commit_and_leaves_transaction_active() {
    let mut config = Config::in_memory();
    config.persistence.l0_soft_limit_segments = 1;
    config.persistence.l0_hard_limit_segments = 2;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;
    config.persistence.l0_soft_backpressure_wait_ms = 0;
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    for row_id in [1_i64, 2] {
        let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
        builder.add_row(row_id, &Row::from_values(vec![Value::Integer(row_id)]));
        engine
            .register_volume("items", Arc::new(builder.finish()))
            .unwrap();
    }

    let mut transaction = engine.begin_transaction().unwrap();
    transaction
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![Value::Integer(3)]))
        .unwrap();
    let error = transaction.commit().unwrap_err();
    assert!(matches!(error, Error::CompactionBackpressure { .. }));
    assert!(error.is_retryable());
    assert!(transaction.is_active());
    let stats = engine.runtime_stats_snapshot();
    assert!(stats.compaction_requested);
    assert_eq!(stats.compaction_hard_backpressure_rejections, 1);
    assert_eq!(stats.hot_rows, 0);
    assert_eq!(stats.staging_rows, 1);
    transaction.rollback().unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn soft_l0_backpressure_requests_compaction_without_rejecting_commit() {
    let mut config = Config::in_memory();
    config.persistence.l0_soft_limit_segments = 1;
    config.persistence.l0_hard_limit_segments = 2;
    config.persistence.l0_soft_limit_bytes = u64::MAX - 1;
    config.persistence.l0_hard_limit_bytes = u64::MAX;
    config.persistence.l0_soft_backpressure_wait_ms = 0;
    let engine = MVCCEngine::new(config);
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();

    let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
    builder.add_row(1, &Row::from_values(vec![Value::Integer(1)]));
    engine
        .register_volume("items", Arc::new(builder.finish()))
        .unwrap();

    let mut transaction = engine.begin_transaction().unwrap();
    transaction
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![Value::Integer(2)]))
        .unwrap();
    transaction.commit().unwrap();

    let stats = engine.runtime_stats_snapshot();
    assert!(stats.compaction_requested);
    assert_eq!(stats.compaction_soft_backpressure_waits, 1);
    assert_eq!(stats.compaction_hard_backpressure_rejections, 0);
    assert_eq!(stats.hot_rows, 1);
    engine.close_engine().unwrap();
}

#[test]
fn runtime_snapshot_never_queues_behind_contended_engine_owners() {
    let engine = MVCCEngine::new(Config::in_memory());
    let _lifecycle = engine.lifecycle.write().unwrap();
    let _hot = engine.version_stores.write().unwrap();
    let _staging = engine.txn_version_stores.write().unwrap();
    let _cold = engine.segment_managers.write().unwrap();
    let _config = engine.config.write().unwrap();
    let _checkpoint = engine.checkpoint_mutex.lock().unwrap();
    instrumentation::begin_artifact_io_probe();
    instrumentation::begin_ram_accelerator_probe();
    let started = Instant::now();

    let snapshot = engine.runtime_stats_snapshot();
    let io = instrumentation::end_artifact_io_probe();
    let accelerator = instrumentation::end_ram_accelerator_probe();

    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(!snapshot.complete);
    for expected in [
        "lifecycle_busy",
        "hot_owner_map_busy",
        "staging_owner_map_busy",
        "cold_owner_map_busy",
        "config_busy",
    ] {
        assert!(snapshot
            .missing_evidence
            .iter()
            .any(|item| item == expected));
    }
    assert_eq!(io.file_open_calls, 0);
    assert_eq!(io.pread_calls, 0);
    assert_eq!(accelerator.builds, 0);
}

#[test]
fn runtime_stats_publish_the_database_local_storage_cpu_budget() {
    let mut config = Config::in_memory();
    config.persistence.storage_cpu_workers = 1;
    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();

    let snapshot = engine.runtime_stats_snapshot();
    assert_eq!(snapshot.storage_cpu_workers_configured, 1);
    assert_eq!(snapshot.storage_cpu_workers_effective, 1);
    assert_eq!(snapshot.storage_cpu_workers_in_use, 0);
    assert_eq!(snapshot.storage_cpu_workers_reserved, 0);
    assert_eq!(snapshot.storage_cpu_peak_workers_reserved, 0);

    config.persistence.storage_cpu_workers = 2;
    let error = engine.update_engine_config(config).unwrap_err();
    assert!(error
        .to_string()
        .contains("storage_cpu_workers is fixed when the database opens"));
    engine.close_engine().unwrap();
}

#[test]
fn runtime_operation_snapshot_tracks_bounded_owner_and_outcome() {
    let state = RuntimeOperationState::new();
    let operation = state.start();
    let segment_ids = (0..(RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS as u64 + 7))
        .rev()
        .collect::<Vec<_>>();
    operation.set_detail_with_reason("private_table_name", &segment_ids, "test");
    operation.add_input(100, 1_000);
    operation.add_output(80, 700);
    operation.add_reclaimed(20, 300);
    operation.set_result_marker(42);

    let active = state.snapshot(runtime_unix_millis());
    assert!(active.active);
    assert_eq!(active.calls, 1);
    assert_eq!(active.current_input_rows, 100);
    assert_eq!(
        active.detail.table_id,
        Some(runtime_owner_id("private_table_name"))
    );
    assert_eq!(
        active.detail.segment_ids.len(),
        RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS
    );
    assert!(active.detail.segment_ids_truncated);
    assert!(!serde_json::to_string(&active)
        .unwrap()
        .contains("private_table_name"));

    operation.success();
    let completed = state.snapshot(runtime_unix_millis());
    assert!(!completed.active);
    assert_eq!(completed.completed, 1);
    assert_eq!(completed.failed, 0);
    assert_eq!(completed.last_input_rows, 100);
    assert_eq!(completed.last_output_rows, 80);
    assert_eq!(completed.last_reclaimed_rows, 20);
    assert_eq!(completed.total_input_bytes, 1_000);
    assert_eq!(completed.total_output_bytes, 700);
    assert_eq!(completed.total_reclaimed_bytes, 300);
    assert_eq!(completed.last_result_marker, 42);

    let failed = state.start();
    failed.add_input(1, 2);
    drop(failed);
    let completed = state.snapshot(runtime_unix_millis());
    assert_eq!(completed.calls, 2);
    assert_eq!(completed.completed, 1);
    assert_eq!(completed.failed, 1);
}

#[test]
fn compaction_cost_snapshot_separates_published_and_invalidated_work() {
    let state = EngineCompactionCostState::new();

    state.selected("sub_target_merge");
    state.deferred("retry_cooldown");

    let mut published = state.start_job(100, 1_000);
    published.set_generated_output(90, 700, 1);
    published.published(Duration::from_millis(3));

    let mut invalidated = state.start_job(200, 2_000);
    invalidated.set_generated_output(180, 1_400, 2);
    invalidated.invalidated("topology_publication_rejected");

    let snapshot = state.snapshot();
    assert_eq!(snapshot.jobs_selected_sub_target_merge, 1);
    assert_eq!(snapshot.jobs_planned, 2);
    assert_eq!(snapshot.jobs_deferred, 1);
    assert_eq!(snapshot.jobs_waited_retry_cooldown, 1);
    assert_eq!(snapshot.jobs_published, 1);
    assert_eq!(snapshot.jobs_invalidated, 1);
    assert_eq!(snapshot.jobs_invalidated_topology, 1);
    assert_eq!(snapshot.jobs_failed, 0);
    assert_eq!(snapshot.total_logical_input_rows, 300);
    assert_eq!(snapshot.total_generated_output_bytes, 2_100);
    assert_eq!(snapshot.total_published_input_bytes, 1_000);
    assert_eq!(snapshot.total_published_output_bytes, 700);
    assert_eq!(snapshot.total_wasted_input_bytes, 2_000);
    assert_eq!(snapshot.total_wasted_output_bytes, 1_400);
    assert_eq!(snapshot.posting_outputs_generated, 3);
    assert_eq!(snapshot.posting_outputs_published, 1);
    assert_eq!(snapshot.posting_outputs_wasted, 2);
    assert_eq!(snapshot.manifest_publications, 1);
    assert_eq!(snapshot.last_manifest_publication_nanos, 3_000_000);
    assert_eq!(snapshot.last_selection_reason, "sub_target_merge");
    assert_eq!(snapshot.last_wait_reason, "retry_cooldown");
    assert_eq!(
        snapshot.last_invalidation_reason,
        "topology_publication_rejected"
    );
    assert_eq!(snapshot.last_outcome, "topology_publication_rejected");
}

#[test]
fn compaction_cost_snapshot_keeps_bounded_reason_taxonomy() {
    let state = EngineCompactionCostState::new();
    state.selected("tombstone_cleanup");
    state.selected("oversized_segment_split");

    state.start_job(1, 1).invalidated("schema_epoch_changed");
    state.start_job(1, 1).invalidated("input_snapshot_changed");
    state.start_job(1, 1).invalidated("time_budget_exceeded");
    state
        .start_job(1, 1)
        .invalidated("prepublication_cancelled");

    let snapshot = state.snapshot();
    assert_eq!(snapshot.jobs_selected_tombstone_cleanup, 1);
    assert_eq!(snapshot.jobs_selected_oversized_segment_split, 1);
    assert_eq!(snapshot.jobs_invalidated_other, 4);
    assert_eq!(snapshot.jobs_cancelled_schema_epoch, 1);
    assert_eq!(snapshot.jobs_cancelled_input_snapshot, 1);
    assert_eq!(snapshot.jobs_cancelled_budget, 1);
    assert_eq!(snapshot.jobs_cancelled_other, 1);
    assert_eq!(snapshot.last_selection_reason, "oversized_segment_split");
    assert_eq!(
        snapshot.last_cancellation_reason,
        "prepublication_cancelled"
    );
    assert_eq!(
        snapshot.last_invalidation_reason,
        "prepublication_cancelled"
    );
}

#[test]
fn view_definition_codec_is_versioned_bounded_and_exact() {
    fn test_dependency_binder(query: &str) -> Result<Vec<String>> {
        match query {
            "SELECT * FROM users" => Ok(vec!["users".to_string()]),
            "SELECT * FROM legacy_source" => Ok(vec!["legacy_source".to_string()]),
            "SELECT 1" => Ok(Vec::new()),
            _ => Err(Error::invalid_argument("unexpected view fixture query")),
        }
    }

    let view = ViewDefinition::from_bound_query(
        "ActiveUsers",
        "SELECT * FROM users".to_string(),
        vec!["users".to_string()],
    )
    .unwrap();
    let encoded = view.serialize().unwrap();
    let decoded = ViewDefinition::deserialize(&encoded, test_dependency_binder).unwrap();
    assert_eq!(decoded.name, "activeusers");
    assert_eq!(decoded.original_name, "ActiveUsers");
    assert_eq!(decoded.query, "SELECT * FROM users");
    assert_eq!(decoded.dependencies, vec!["users"]);

    let unversioned = &encoded[VIEW_DEFINITION_MARKER_V2.len()..];
    assert!(
        ViewDefinition::deserialize(unversioned, test_dependency_binder)
            .unwrap_err()
            .to_string()
            .contains("expected RVW1/RVW2 marker")
    );

    let mut stale = view.clone();
    stale.dependencies.clear();
    assert!(stale
        .serialize()
        .unwrap_err()
        .to_string()
        .contains("stale dependency graph"));
    let mut forged = encoded.clone();
    let dependency = forged.len() - "users".len();
    forged[dependency..].copy_from_slice(b"other");
    assert!(ViewDefinition::deserialize(&forged, test_dependency_binder)
        .unwrap_err()
        .to_string()
        .contains("dependency graph does not match"));

    let mut trailing = encoded;
    trailing.push(0);
    assert!(
        ViewDefinition::deserialize(&trailing, test_dependency_binder)
            .unwrap_err()
            .to_string()
            .contains("trailing bytes")
    );

    let oversized_name = "v".repeat(usize::from(u16::MAX) + 1);
    assert!(
        ViewDefinition::from_bound_query(&oversized_name, "SELECT 1".to_string(), vec![])
            .unwrap()
            .serialize()
            .unwrap_err()
            .to_string()
            .contains("view name")
    );

    let legacy_name = "LegacyView";
    let legacy_query = "SELECT * FROM legacy_source";
    let mut legacy = VIEW_DEFINITION_MARKER_V1.to_vec();
    legacy.extend_from_slice(&(legacy_name.len() as u16).to_le_bytes());
    legacy.extend_from_slice(legacy_name.as_bytes());
    legacy.extend_from_slice(&(legacy_query.len() as u32).to_le_bytes());
    legacy.extend_from_slice(legacy_query.as_bytes());
    let decoded = ViewDefinition::deserialize(&legacy, test_dependency_binder).unwrap();
    assert_eq!(decoded.name, "legacyview");
    assert_eq!(decoded.dependencies, vec!["legacy_source"]);
}

#[test]
fn r6_view_dependency_graph_blocks_destructive_ddl_without_lexical_false_positives() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("source_rows")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("lexical_name")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_bound_catalog_test_view(
        &engine,
        "inner_view",
        "SELECT * FROM source_rows",
        &["source_rows"],
    )
    .unwrap();
    create_bound_catalog_test_view(
        &engine,
        "outer_view",
        "SELECT * FROM inner_view",
        &["inner_view"],
    )
    .unwrap();
    create_bound_catalog_test_view(
        &engine,
        "literal_view",
        "SELECT 'lexical_name' AS note FROM source_rows",
        &["source_rows"],
    )
    .unwrap();

    assert!(drop_catalog_test_table(&engine, "source_rows").is_err());
    assert!(drop_catalog_test_view(&engine, "inner_view").is_err());
    rename_catalog_test_table(&engine, "lexical_name", "renamed_lexical")
        .expect("a string literal is not a catalog dependency");

    drop_catalog_test_view(&engine, "outer_view").unwrap();
    drop_catalog_test_view(&engine, "inner_view").unwrap();
    assert!(drop_catalog_test_table(&engine, "source_rows").is_err());
    drop_catalog_test_view(&engine, "literal_view").unwrap();
    drop_catalog_test_table(&engine, "source_rows").unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn r6_transactional_catalog_validates_private_foreign_keys_at_commit() {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();

    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("parents")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("children")
            .add_primary_key("id", DataType::Integer)
            .add("parent_id", DataType::Integer)
            .add_foreign_key(ForeignKeyConstraint {
                column_index: 1,
                column_name: "parent_id".to_string(),
                referenced_table: "parents".to_string(),
                referenced_column: "id".to_string(),
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
            })
            .build(),
    )
    .unwrap();
    let error = drop_catalog_test_table(&engine, "parents")
        .expect_err("a catalog FK target cannot disappear at publication");
    assert!(error.to_string().contains("parents"), "{error}");
    assert!(engine.table_exists("parents").unwrap());
    assert!(engine.table_exists("children").unwrap());
    engine.close_engine().unwrap();
}

#[test]
fn r2_l06_batch_c_closed_vacuum_and_cleanup_panic_are_observable() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    engine.close_engine().unwrap();
    assert!(matches!(
        engine.vacuum(None, std::time::Duration::ZERO),
        Err(Error::EngineNotOpen)
    ));

    let worker = std::thread::spawn(|| panic!("injected cleanup worker panic"));
    let handle = CleanupHandle {
        stop_flag: Arc::new(AtomicBool::new(false)),
        thread: Some(worker),
    };
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(handle))).is_err());
}

#[test]
fn r4_l01_batch_a_index_identity_and_certification_contract() {
    // A concrete unique-index replacement must be failure-atomic: the old
    // forward/reverse membership survives a rejected conflicting update.
    let old = [Value::text("old")];
    let taken = [Value::text("taken")];
    let indexes: Vec<Box<dyn Index>> = vec![
        Box::new(BTreeIndex::new(
            "bt".into(),
            "items".into(),
            0,
            "key".into(),
            DataType::Text,
            true,
            0,
        )),
        Box::new(HashIndex::new(
            "hash".into(),
            "items".into(),
            vec!["key".into()],
            vec![0],
            vec![DataType::Text],
            true,
            0,
        )),
        Box::new(MultiColumnIndex::new(
            "multi".into(),
            "items".into(),
            vec!["key".into()],
            vec![0],
            vec![DataType::Text],
            true,
            0,
        )),
    ];
    for index in indexes {
        index.add(&old, 1, 1).unwrap();
        index.add(&taken, 2, 2).unwrap();
        assert!(index.add(&taken, 1, 1).is_err(), "{}", index.name());
        assert_eq!(
            index.get_row_ids_equal(&old).unwrap().into_vec(),
            vec![1],
            "{}",
            index.name()
        );
        assert_eq!(
            index.get_row_ids_equal(&taken).unwrap().into_vec(),
            vec![2],
            "{}",
            index.name()
        );
    }

    // Bitmap uses a u64 container internally, but the public row identity
    // is the full signed i64 domain in both single and batch paths.
    let bitmap = BitmapIndex::new(
        "bitmap".into(),
        "items".into(),
        vec!["flag".into()],
        vec![0],
        vec![DataType::Boolean],
        false,
        0,
    );
    let flag = [Value::Boolean(true)];
    bitmap
        .add_batch_slice(&[(-7, &flag), (i64::MIN, &flag)])
        .unwrap();
    let mut negative_ids = bitmap.get_row_ids_equal(&flag).unwrap().into_vec();
    negative_ids.sort_unstable();
    assert_eq!(negative_ids, vec![i64::MIN, -7]);
    bitmap
        .remove_batch_slice(&[(-7, &flag), (i64::MIN, &flag)])
        .unwrap();
    assert!(bitmap.get_row_ids_equal(&flag).unwrap().is_empty());

    // Every public index implementation accepts the full signed row-ID
    // domain in both single and legacy I64Map batch entry points.
    let full_domain_indexes: Vec<(Box<dyn Index>, Vec<Value>)> = vec![
        (
            Box::new(BTreeIndex::new(
                "bt_domain".into(),
                "items".into(),
                0,
                "key".into(),
                DataType::Text,
                false,
                0,
            )),
            vec![Value::text("minimum")],
        ),
        (
            Box::new(HashIndex::new(
                "hash_domain".into(),
                "items".into(),
                vec!["key".into()],
                vec![0],
                vec![DataType::Text],
                false,
                0,
            )),
            vec![Value::text("minimum")],
        ),
        (
            Box::new(BitmapIndex::new(
                "bitmap_domain".into(),
                "items".into(),
                vec!["key".into()],
                vec![0],
                vec![DataType::Text],
                false,
                0,
            )),
            vec![Value::text("minimum")],
        ),
        (
            Box::new(MultiColumnIndex::new(
                "multi_domain".into(),
                "items".into(),
                vec!["key".into()],
                vec![0],
                vec![DataType::Text],
                false,
                0,
            )),
            vec![Value::text("minimum")],
        ),
    ];
    for (index, values) in full_domain_indexes {
        index.add(&values, i64::MIN, i64::MIN).unwrap();
        assert_eq!(
            index.get_row_ids_equal(&values).unwrap().into_vec(),
            vec![i64::MIN],
            "{} single add",
            index.name()
        );
        index.remove(&values, i64::MIN, i64::MIN).unwrap();
        assert!(
            index.get_row_ids_equal(&values).unwrap().is_empty(),
            "{} single remove",
            index.name()
        );

        let mut entries = radixdb_core::I64Map::new();
        entries.insert(i64::MIN, values.clone());
        entries.insert(-7, values.clone());
        index.add_batch(&entries).unwrap();
        let mut row_ids = index.get_row_ids_equal(&values).unwrap().into_vec();
        row_ids.sort_unstable();
        assert_eq!(row_ids, vec![i64::MIN, -7], "{} batch add", index.name());
        index.remove_batch(&entries).unwrap();
        assert!(
            index.get_row_ids_equal(&values).unwrap().is_empty(),
            "{} batch remove",
            index.name()
        );
    }

    // A primary-key index derives its logical key from row_id, so each
    // batch row deliberately carries the corresponding INTEGER value.
    let pk = PkIndex::new("pk_domain".into(), "items".into(), 0, "id".into());
    let min_pk = [Value::Integer(i64::MIN)];
    let negative_pk = [Value::Integer(-7)];
    pk.add(&min_pk, i64::MIN, i64::MIN).unwrap();
    assert_eq!(
        pk.get_row_ids_equal(&min_pk).unwrap().into_vec(),
        vec![i64::MIN]
    );
    pk.remove(&min_pk, i64::MIN, i64::MIN).unwrap();
    assert!(pk.get_row_ids_equal(&min_pk).unwrap().is_empty());
    let mut pk_entries = radixdb_core::I64Map::new();
    pk_entries.insert(i64::MIN, min_pk.to_vec());
    pk_entries.insert(-7, negative_pk.to_vec());
    pk.add_batch(&pk_entries).unwrap();
    assert_eq!(
        pk.get_row_ids_equal(&min_pk).unwrap().into_vec(),
        vec![i64::MIN]
    );
    assert_eq!(
        pk.get_row_ids_equal(&negative_pk).unwrap().into_vec(),
        vec![-7]
    );
    pk.remove_batch(&pk_entries).unwrap();
    assert!(pk.get_row_ids_equal(&min_pk).unwrap().is_empty());
    assert!(pk.get_row_ids_equal(&negative_pk).unwrap().is_empty());

    let hnsw = HnswIndex::new(
        "hnsw_domain".into(),
        "items".into(),
        "embedding".into(),
        0,
        3,
        8,
        32,
        32,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    let vector = Value::vector(vec![1.0, 2.0, 3.0]);
    let vector_bytes = match &vector {
        Value::Extension(data) => &data[1..],
        _ => unreachable!("vector constructor must return an extension"),
    };
    hnsw.add(std::slice::from_ref(&vector), i64::MIN, i64::MIN)
        .unwrap();
    assert_eq!(hnsw.search_nearest(vector_bytes, 1, 32)[0].0, i64::MIN);
    hnsw.remove(std::slice::from_ref(&vector), i64::MIN, i64::MIN)
        .unwrap();
    assert!(hnsw.search_nearest(vector_bytes, 1, 32).is_empty());
    let mut vector_entries = radixdb_core::I64Map::new();
    vector_entries.insert(i64::MIN, vec![vector.clone()]);
    hnsw.add_batch(&vector_entries).unwrap();
    assert_eq!(hnsw.search_nearest(vector_bytes, 1, 32)[0].0, i64::MIN);
    hnsw.remove_batch(&vector_entries).unwrap();
    assert!(hnsw.search_nearest(vector_bytes, 1, 32).is_empty());

    // Direct Table writes and cached UPDATE must enforce VECTOR(N), not
    // merely the outer VECTOR type accepted by HNSW.
    let vector_engine = MVCCEngine::in_memory();
    vector_engine.open_engine().unwrap();
    let vector_schema = SchemaBuilder::new("vectors")
        .column("id", DataType::Integer, false, true)
        .column("embedding", DataType::Vector, false, false)
        .set_last_vector_dimensions(3)
        .build();
    create_catalog_test_table(&vector_engine, vector_schema).unwrap();
    let mut vector_tx = vector_engine.begin_transaction().unwrap();
    let mut vectors = vector_tx.get_table("vectors").unwrap();
    assert!(vectors
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::vector(vec![1.0, 2.0]),
        ]))
        .is_err());
    assert!(vectors
        .insert_batch(vec![Row::from_values(vec![
            Value::Integer(2),
            Value::vector(vec![1.0, 2.0]),
        ])])
        .is_err());
    let mut malformed_vector = vec![DataType::Vector as u8];
    malformed_vector.extend_from_slice(&[0; 13]);
    assert!(vectors
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::Extension(radixdb_core::CompactArc::from(malformed_vector)),
        ]))
        .is_err());
    vectors
        .insert(Row::from_values(vec![
            Value::Integer(3),
            Value::vector(vec![1.0, 2.0, 3.0]),
        ]))
        .unwrap();
    let id_three = crate::expression::ComparisonExpr::eq("id", Value::Integer(3));
    assert!(vectors
        .update(Some(&id_three), &mut |mut row| {
            row.set(1, Value::vector(vec![9.0, 8.0]))?;
            Ok((row, true))
        })
        .is_err());
    drop(vectors);
    vector_tx.rollback().unwrap();
    vector_engine.close_engine().unwrap();

    // UNIQUE certification is one visible hot+cold domain, not two
    // independent passes that both accept the same key.
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    let schema = SchemaBuilder::new("items")
        .column("id", DataType::Integer, false, true)
        .column("key", DataType::Text, false, false)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let mut builder = crate::volume::writer::VolumeBuilder::new(&schema);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::text("duplicate")]),
    );
    engine
        .register_volume("items", Arc::new(builder.finish()))
        .unwrap();
    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("duplicate"),
        ]))
        .unwrap();
    insert.commit().unwrap();
    let error =
        commit_test_index(&engine, "items", "items_key_uq", &["key"], true, None).unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    engine.close_engine().unwrap();
}

#[test]
fn r4_l01_batch_b_index_ddl_and_truncate_lifecycle_contract() {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("items")
            .add_primary_key("id", DataType::Integer)
            .add("code", DataType::Text)
            .add("flag", DataType::Boolean)
            .build(),
    )
    .unwrap();
    let mut seed = engine.begin_transaction().unwrap();
    seed.get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("one"),
            Value::Boolean(true),
        ]))
        .unwrap();
    seed.commit().unwrap();

    // DROP TABLE remains a private intent until commit; rollback preserves
    // both catalog identity and data.
    let mut drop_rollback = engine.begin_transaction().unwrap();
    drop_rollback.drop_table("items").unwrap();
    assert!(drop_rollback.get_table("items").is_err());
    drop_rollback.rollback().unwrap();
    assert_eq!(collect_rows(&engine, "items").len(), 1);

    commit_test_index(&engine, "items", "items_key_idx", &["code"], false, None).unwrap();
    let mut unsupported_index_ddl = engine.begin_transaction().unwrap();
    assert!(unsupported_index_ddl
        .drop_table_index("items", "items_key_idx")
        .is_err());
    unsupported_index_ddl.rollback().unwrap();
    assert!(engine.index_exists("items_key_idx", "items").unwrap());

    // A pure INSERT has no existing-row claim. The table membership fence
    // is therefore the required boundary between TRUNCATE and commit.
    let mut writer = engine.begin_transaction().unwrap();
    writer
        .get_table("items")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("two"),
            Value::Boolean(false),
        ]))
        .unwrap();
    let store = engine.get_version_store("items").unwrap();
    let membership_fence = store.membership_fence();
    let truncate_side = membership_fence.write();
    store.truncate_all().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let commit = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        finished_tx.send(writer.commit()).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(finished_rx
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_err());
    drop(truncate_side);
    finished_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    commit.join().unwrap();
    let rows = collect_rows(&engine, "items");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);
    let key_index = engine
        .get_version_store("items")
        .unwrap()
        .get_index("items_key_idx")
        .unwrap();
    assert_eq!(
        key_index
            .get_row_ids_equal(&[Value::text("two")])
            .unwrap()
            .into_vec(),
        vec![2]
    );
    engine.close_engine().unwrap();
}

#[test]
fn r4_l02_batch_b_transactional_catalog_namespace_and_dependents() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("catalog-contract");
    let config = r2_l03_batch_a_config(&database_path);
    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.install_view_dependency_binder(bind_catalog_test_view_dependencies);
    engine.open_engine().unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("source_rows")
            .add_primary_key("id", DataType::Integer)
            .add("value", DataType::Text)
            .build(),
    )
    .unwrap();
    let mut seed = engine.begin_transaction().unwrap();
    seed.get_table("source_rows")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("one"),
        ]))
        .unwrap();
    seed.commit().unwrap();
    create_bound_catalog_test_view(
        &engine,
        "stable_view",
        "SELECT * FROM source_rows",
        &["source_rows"],
    )
    .unwrap();

    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("mutable")
            .add_primary_key("id", DataType::Integer)
            .add("value", DataType::Text)
            .build(),
    )
    .unwrap();
    commit_test_index(
        &engine,
        "mutable",
        "mutable_value_idx",
        &["value"],
        false,
        None,
    )
    .unwrap();
    let original_schema = engine.get_table_schema("mutable").unwrap();
    let mut direct = engine.begin_transaction().unwrap();
    direct.rename_table("mutable", "mutated").unwrap();
    assert!(matches!(direct.commit(), Err(Error::NotSupported(_))));
    let mut direct = engine.begin_transaction().unwrap();
    assert!(matches!(
        direct.drop_table_index("mutable", "mutable_value_idx"),
        Err(Error::NotSupported(_))
    ));
    assert!(matches!(
        direct.create_table_btree_index("mutable", "value", false, None),
        Err(Error::NotSupported(_))
    ));
    assert!(matches!(
        direct.drop_table_btree_index("mutable", "value"),
        Err(Error::NotSupported(_))
    ));
    assert!(matches!(
        direct.drop_table_column("mutable", "value"),
        Err(Error::NotSupported(_))
    ));
    assert!(matches!(
        direct.rename_table_column("mutable", "value", "renamed_value"),
        Err(Error::NotSupported(_))
    ));
    let mut modified = original_schema.columns[1].clone();
    modified.nullable = true;
    assert!(matches!(
        direct.modify_table_column("mutable", modified),
        Err(Error::NotSupported(_))
    ));
    direct.rollback().unwrap();
    assert!(engine.get_table_schema("mutated").is_err());
    assert_eq!(
        engine.get_table_schema("mutable").unwrap().as_ref(),
        original_schema.as_ref()
    );
    let read = engine.begin_transaction().unwrap();
    assert!(read
        .get_table("mutable")
        .unwrap()
        .get_index("mutable_value_idx")
        .is_some());
    drop(read);

    // FK dependencies survive table rename by stable catalog identity;
    // only the metadata projection is rebound and no row data is copied.
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("parents")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("children")
            .add_primary_key("id", DataType::Integer)
            .column("parent_id", DataType::Integer, false, false)
            .add_foreign_key(ForeignKeyConstraint {
                column_index: 1,
                column_name: "parent_id".to_string(),
                referenced_table: "parents".to_string(),
                referenced_column: "id".to_string(),
                on_delete: radixdb_core::ForeignKeyAction::NoAction,
                on_update: radixdb_core::ForeignKeyAction::NoAction,
            })
            .build(),
    )
    .unwrap();
    rename_catalog_test_table(&engine, "parents", "renamed_parents").unwrap();
    assert_eq!(
        engine.get_table_schema("children").unwrap().foreign_keys[0].referenced_table,
        "renamed_parents"
    );

    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("viewed")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_bound_catalog_test_view(&engine, "viewed_rows", "SELECT * FROM viewed", &["viewed"])
        .unwrap();
    assert!(rename_catalog_test_table(&engine, "viewed", "renamed_viewed").is_err());

    // Table, view and pending-table names form one collision-free namespace.
    create_catalog_test_table(
        &engine,
        SchemaBuilder::new("free_table")
            .add_primary_key("id", DataType::Integer)
            .build(),
    )
    .unwrap();
    create_bound_catalog_test_view(
        &engine,
        "occupied_name",
        "SELECT * FROM source_rows",
        &["source_rows"],
    )
    .unwrap();
    assert!(rename_catalog_test_table(&engine, "free_table", "occupied_name").is_err());

    let mut pending = engine.begin_transaction().unwrap();
    let pending_schema = SchemaBuilder::new("pending_name")
        .column("id", DataType::Integer, false, true)
        .build();
    pending
        .create_table("pending_name", pending_schema)
        .unwrap();
    assert!(create_bound_catalog_test_view(
        &engine,
        "pending_name",
        "SELECT * FROM source_rows",
        &["source_rows"],
    )
    .is_err());
    pending.rollback().unwrap();

    let mut view_collision = engine.begin_transaction().unwrap();
    let collision_schema = SchemaBuilder::new("occupied_name")
        .column("id", DataType::Integer, false, true)
        .build();
    assert!(matches!(
        view_collision.create_table("occupied_name", collision_schema),
        Err(Error::ViewAlreadyExists(_))
    ));
    view_collision.rollback().unwrap();

    rename_catalog_test_table(&engine, "free_table", "free_renamed").unwrap();
    engine.close_engine().unwrap();
    drop(engine);

    let reopened = MVCCEngine::new(config);
    reopened.install_view_dependency_binder(bind_catalog_test_view_dependencies);
    reopened.open_engine().unwrap();
    assert!(reopened.table_exists("free_renamed").unwrap());
    assert!(!reopened.table_exists("free_table").unwrap());
    assert!(reopened.table_exists("renamed_parents").unwrap());
    assert_eq!(
        reopened.get_table_schema("children").unwrap().foreign_keys[0].referenced_table,
        "renamed_parents"
    );
    assert!(reopened.view_exists("stable_view").unwrap());
    assert!(reopened.view_exists("viewed_rows").unwrap());
    reopened.close_engine().unwrap();
}

#[test]
fn procedural_catalog_checkpoint_reuses_data_and_index_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("procedural-catalog-artifact-reuse");
    let config = r2_l03_batch_a_config(&database_path);
    let engine = MVCCEngine::new(config.clone());
    engine.open_engine().unwrap();

    let schema = SchemaBuilder::new("items")
        .add_primary_key("id", DataType::Integer)
        .add("value", DataType::Text)
        .build();
    create_catalog_test_table(&engine, schema.clone()).unwrap();
    let _volume = register_artifact_volume(
        &engine,
        "items",
        &schema,
        &[(
            1,
            Row::from_values(vec![Value::Integer(1), Value::text("one")]),
        )],
    );

    let before = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap();
    let before_artifacts = before
        .snapshot()
        .artifact_references()
        .into_iter()
        .collect::<FxHashSet<_>>();
    assert!(
        before_artifacts
            .iter()
            .any(|reference| reference.kind() == crate::v6::ArtifactKind::Data)
    );
    assert!(
        before_artifacts
            .iter()
            .any(|reference| reference.kind() == crate::v6::ArtifactKind::Index)
    );
    let before_data_paths = artifact_paths_with_extension(&database_path, "data");
    let before_index_paths = artifact_paths_with_extension(&database_path, "idx");
    drop(before);

    let generation = engine.pin_catalog().unwrap();
    let procedure_id = ObjectId::new();
    let acl_id = ObjectId::new();
    let definition = RoutineDefinition::new(
        ProceduralSource::new("BEGIN NULL; END").unwrap(),
        vec![],
        RoutineResult::Void,
        Volatility::Volatile,
        SecurityMode::Invoker,
        vec![ObjectId::BOOTSTRAP_NAMESPACE],
        vec![],
        1,
        1,
        1,
        ResourcePolicy::default_call(),
    )
    .unwrap();
    let procedure = CatalogObject::new(
        procedure_id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("catalog_reuse_probe").unwrap(),
        1,
        CatalogPayload::Procedure(ProcedurePayload::new(definition).unwrap()),
    )
    .unwrap();
    let acl = CatalogObject::new(
        acl_id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(format!("acl_{acl_id}")).unwrap(),
        1,
        CatalogPayload::AclEntry(
            AclEntryPayload::object_privileges(
                ObjectId::BOOTSTRAP_OWNER,
                PRIVILEGE_EXECUTE,
                0,
                vec![],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let mutation = CatalogMutationSet::for_generation(
        generation.as_ref(),
        vec![
            CatalogMutation::create(procedure),
            CatalogMutation::create(acl),
        ],
        vec![],
        vec![
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                procedure_id,
                EdgeKind::Contains,
                next_namespace_ordinal(generation.as_ref()).unwrap(),
            ),
            CatalogEdge::new(
                procedure_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
            CatalogEdge::new(acl_id, ObjectId::BOOTSTRAP_OWNER, EdgeKind::GrantedTo, 0),
            CatalogEdge::new(acl_id, procedure_id, EdgeKind::GrantsOn, 0),
        ],
    )
    .unwrap();
    drop(generation);
    let mut transaction = engine.begin_transaction().unwrap();
    transaction.stage_catalog_mutation(mutation).unwrap();
    transaction.commit().unwrap();

    let logical = engine.pin_catalog().unwrap();
    assert_eq!(logical.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert!(logical.object(procedure_id).is_some());
    assert!(logical.object(acl_id).is_some());
    let logical_generation = logical.meta().catalog_generation();
    drop(logical);

    engine.checkpoint_cycle_inner(true).unwrap();
    let after = engine
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap();
    assert_eq!(
        after.snapshot().database_manifest().catalog().generation().get(),
        logical_generation
    );
    assert_eq!(
        after
            .snapshot()
            .artifact_references()
            .into_iter()
            .collect::<FxHashSet<_>>(),
        before_artifacts
    );
    assert_eq!(
        artifact_paths_with_extension(&database_path, "data"),
        before_data_paths
    );
    assert_eq!(
        artifact_paths_with_extension(&database_path, "idx"),
        before_index_paths
    );
    drop(after);

    engine.close_engine().unwrap();
    drop(engine);

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    let catalog = reopened.pin_catalog().unwrap();
    assert_eq!(catalog.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert!(catalog.object(procedure_id).is_some());
    assert!(catalog.object(acl_id).is_some());
    assert_eq!(collect_rows(&reopened, "items").len(), 1);
    let physical = reopened
        .physical_generation
        .load_full()
        .unwrap()
        .pin()
        .unwrap();
    assert_eq!(
        physical
            .snapshot()
            .artifact_references()
            .into_iter()
            .collect::<FxHashSet<_>>(),
        before_artifacts
    );
    drop(physical);
    drop(catalog);
    reopened.close_engine().unwrap();
}

#[test]
fn r4_l02_batch_c_ctas_fk_and_public_primary_key_contract() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("typed-schema-contract");
    let config = r2_l03_batch_a_config(&database_path);
    let engine = Arc::new(MVCCEngine::new(config.clone()));
    engine.open_engine().unwrap();

    let text_schema = SchemaBuilder::new("text_keys")
        .add_primary_key("key", DataType::Text)
        .add("payload", DataType::Text)
        .build();
    create_catalog_test_table(&engine, text_schema).unwrap();
    let uuid_schema = SchemaBuilder::new("uuid_keys")
        .add_primary_key("key", DataType::Uuid)
        .add("payload", DataType::Text)
        .build();
    create_catalog_test_table(&engine, uuid_schema).unwrap();
    let invalid_composite = SchemaBuilder::new("invalid_composite")
        .column("tenant", DataType::Text, false, true)
        .column("code", DataType::Integer, false, true)
        .build();
    assert!(matches!(
        create_catalog_test_table(&engine, invalid_composite),
        Err(Error::NotSupported(_))
    ));

    let mut insert = engine.begin_transaction().unwrap();
    insert
        .get_table("text_keys")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::text("alpha"),
            Value::text("one"),
        ]))
        .unwrap();
    insert
        .get_table("uuid_keys")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::uuid([7; 16]),
            Value::text("seven"),
        ]))
        .unwrap();
    insert.commit().unwrap();

    for (table_name, row) in [
        (
            "text_keys",
            Row::from_values(vec![Value::text("alpha"), Value::text("duplicate")]),
        ),
        (
            "uuid_keys",
            Row::from_values(vec![Value::uuid([7; 16]), Value::text("duplicate")]),
        ),
    ] {
        let mut duplicate = engine.begin_transaction().unwrap();
        assert!(duplicate
            .get_table(table_name)
            .unwrap()
            .insert(row)
            .is_err());
        duplicate.rollback().unwrap();
    }

    assert!(engine.list_table_indexes("text_keys").unwrap().is_empty());
    assert!(engine.list_table_indexes("uuid_keys").unwrap().is_empty());
    engine.checkpoint_cycle_inner(true).unwrap();
    engine.close_engine().unwrap();
    drop(engine);

    let reopened = MVCCEngine::new(config);
    reopened.open_engine().unwrap();
    assert!(reopened.list_table_indexes("text_keys").unwrap().is_empty());
    assert!(reopened.list_table_indexes("uuid_keys").unwrap().is_empty());
    assert_eq!(collect_rows(&reopened, "text_keys").len(), 1);
    assert_eq!(collect_rows(&reopened, "uuid_keys").len(), 1);
    for (table_name, row) in [
        (
            "text_keys",
            Row::from_values(vec![Value::text("alpha"), Value::text("again")]),
        ),
        (
            "uuid_keys",
            Row::from_values(vec![Value::uuid([7; 16]), Value::text("again")]),
        ),
    ] {
        let mut duplicate = reopened.begin_transaction().unwrap();
        assert!(duplicate
            .get_table(table_name)
            .unwrap()
            .insert(row)
            .is_err());
        duplicate.rollback().unwrap();
    }
    reopened.close_engine().unwrap();
}

fn register_artifact_volume(
    engine: &MVCCEngine,
    table_name: &str,
    schema: &radixdb_core::Schema,
    rows: &[(i64, Row)],
) -> tempfile::TempDir {
    assert!(!rows.is_empty(), "test DATA artifact must contain rows");
    let dir = tempfile::tempdir().unwrap();
    let segment_id = engine
        .get_or_create_segment_manager(table_name)
        .manifest_mut()
        .allocate_segment_id();

    // Persistent tests must exercise the production V6 publisher. Registering
    // a privately written DATA file only in the runtime SegmentManager creates
    // a second, non-CONTROL authority and makes every subsequent compaction
    // test an assertion about the removed pre-cutover path.
    if engine.path != "memory://" {
        let published = engine
            .publish_data_segment(
                table_name,
                rows.iter().cloned().collect::<radixdb_core::RowVec>(),
                true,
            )
            .unwrap();
        engine
            .register_volume_with_id(table_name, published.volume, segment_id)
            .unwrap();
        return dir;
    }

    let columns = schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let marker = u8::try_from(index + 0x31).unwrap();
            crate::v6::DataColumnSpec::new(
                radixdb_catalog::ObjectId::from_user_bytes([marker; 16]).unwrap(),
                catalog_data_type(column).unwrap(),
                column.nullable,
            )
        })
        .collect::<Vec<_>>();
    let row_ids = rows
        .iter()
        .map(|(row_id, _)| crate::v6::encode_runtime_row_id(*row_id))
        .collect::<Vec<_>>();
    let mut blocks = Vec::with_capacity(columns.len() + 1);
    blocks.push(
        crate::v6::DataBlockSpec::row_ids(0, &row_ids, crate::v6::DataPhysicalCodec::None).unwrap(),
    );
    for (column_index, column) in columns.iter().copied().enumerate() {
        let values = rows
            .iter()
            .map(|(_, row)| row.get(column_index).unwrap().clone())
            .collect::<Vec<_>>();
        blocks.push(
            crate::v6::DataBlockSpec::column(
                0,
                u32::try_from(column_index).unwrap(),
                column,
                &values,
                crate::v6::DataValueEncoding::Plain,
                crate::v6::DataPhysicalCodec::None,
            )
            .unwrap(),
        );
    }
    let header = crate::v6::DataArtifactHeader::new(
        crate::v6::ArtifactId::new(),
        crate::v6::DatabaseId::new(),
        radixdb_catalog::ObjectId::from_user_bytes([0x61; 16]).unwrap(),
        crate::v6::SegmentId::new(),
        crate::v6::DatabaseGeneration::new(1).unwrap(),
        crate::v6::CatalogGeneration::new(1).unwrap(),
        1,
        1,
        u64::try_from(rows.len()).unwrap(),
        u32::try_from(columns.len()).unwrap(),
        1,
        crate::v6::SegmentKind::Rows,
        1,
    )
    .unwrap();
    let input = crate::v6::DataArtifactInput::new(header, columns, Vec::new(), blocks).unwrap();
    let (bytes, reference) = crate::v6::encode_data_artifact(&input).unwrap();
    let path = dir.path().join(format!("segment-{segment_id}.data"));
    std::fs::write(&path, bytes).unwrap();
    let source = Arc::new(crate::v6::ArtifactDataSource::open(&path, reference).unwrap());
    let volume = crate::volume::writer::FrozenVolume::from_artifact_source(schema, source).unwrap();
    engine
        .register_volume_with_id(table_name, Arc::new(volume), segment_id)
        .unwrap();
    dir
}

fn count_index_artifacts(root: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .map(|path| {
            if path.is_dir() {
                count_index_artifacts(&path)
            } else {
                usize::from(path.extension().and_then(|value| value.to_str()) == Some("idx"))
            }
        })
        .sum()
}

fn artifact_paths_with_extension(root: &Path, extension: &str) -> FxHashSet<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return FxHashSet::default();
    };
    let mut paths = FxHashSet::default();
    for path in entries.flatten().map(|entry| entry.path()) {
        if path.is_dir() {
            paths.extend(artifact_paths_with_extension(&path, extension));
        } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
            paths.insert(path);
        }
    }
    paths
}

fn commit_test_index(
    engine: &MVCCEngine,
    table_name: &str,
    index_name: &str,
    columns: &[&str],
    is_unique: bool,
    index_type: Option<IndexType>,
) -> Result<()> {
    let definition = PendingIndexDefinition {
        table_name: table_name.to_string(),
        index_name: index_name.to_string(),
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
        is_unique,
        index_type,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_distance_metric: None,
        partial_predicate: None,
        key_encoder: None,
    };
    let catalog_mutation = create_catalog_test_index_mutation(engine, &definition)?;
    let mut tx = engine.begin_transaction()?;
    tx.stage_create_index(definition)?;
    tx.stage_catalog_mutation(catalog_mutation)?;
    tx.commit()
}

fn collect_rows(engine: &MVCCEngine, table_name: &str) -> Vec<(i64, Row)> {
    let tx = engine.begin_transaction().unwrap();
    let table = tx.get_table(table_name).unwrap();
    table.collect_all_rows(None).unwrap().into_iter().collect()
}

fn r2_l03_batch_a_config(path: &Path) -> Config {
    let mut config = Config::with_path(path.to_string_lossy().as_ref());
    config.persistence.target_volume_rows = 2;
    config.persistence.compact_threshold = 2;
    config.persistence.checkpoint_on_close = false;
    config
}

fn r2_l03_batch_a_create_table(engine: &MVCCEngine, table_name: &str) {
    create_catalog_test_table(
        engine,
        SchemaBuilder::new(table_name)
            .column("id", DataType::Integer, false, true)
            .column("value", DataType::Text, false, false)
            .build(),
    )
    .unwrap();
}

fn r2_l03_batch_a_insert(engine: &MVCCEngine, table_name: &str, id: i64) {
    let mut transaction = engine.begin_transaction().unwrap();
    let mut table = transaction.get_table(table_name).unwrap();
    table
        .insert(Row::from_values(vec![
            Value::Integer(id),
            Value::text(format!("row-{id}")),
        ]))
        .unwrap();
    transaction.commit().unwrap();
}

fn assert_table_segments_artifact_backed(engine: &MVCCEngine, table_name: &str) {
    let mgrs = engine.segment_managers.read().unwrap();
    let mgr = mgrs
        .get(table_name)
        .unwrap_or_else(|| panic!("{table_name} segment manager"));
    let segments = mgr.get_segments_ordered_meta();
    assert!(
        !segments.is_empty(),
        "expected at least one immutable segment"
    );
    assert!(
        segments
            .iter()
            .all(|segment| segment.artifact_source().is_some()),
        "immutable segments must retain canonical DATA artifact sources"
    );
}
