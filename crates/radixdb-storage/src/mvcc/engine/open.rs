use super::physical_snapshot::reconcile_physical_restore;
use super::*;

impl MVCCEngine {
    pub(super) fn lock_checkpoint_mutex_profiled(&self) -> std::sync::MutexGuard<'_, ()> {
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        let guard = self.checkpoint_mutex.lock().unwrap();
        #[cfg(feature = "bench-harness")]
        instrumentation::record_runtime_wait(
            instrumentation::RuntimeWaitKind::CheckpointMutex,
            started.elapsed(),
        );
        guard
    }

    pub(super) fn lock_checkpoint_mutex_cancellable(
        &self,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<std::sync::MutexGuard<'_, ()>> {
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        let guard = loop {
            if is_cancelled() {
                return Err(Error::QueryCancelled);
            }
            match self.checkpoint_mutex.try_lock() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return Err(Error::internal(format!(
                        "checkpoint mutex is poisoned: {error}"
                    )));
                }
            }
        };
        #[cfg(feature = "bench-harness")]
        instrumentation::record_runtime_wait(
            instrumentation::RuntimeWaitKind::CheckpointMutex,
            started.elapsed(),
        );
        Ok(guard)
    }

    /// Guard statement planning/execution against an in-flight transactional
    /// catalog publication. Auto-commit DDL requests exclusive ownership;
    /// ordinary statements and DDL staged in an explicit transaction use the
    /// shared side.
    #[doc(hidden)]
    pub fn acquire_ddl_statement_fence(&self, exclusive: bool) -> DdlFenceGuard {
        if exclusive {
            DdlFenceGuard::exclusive(Arc::clone(&self.ddl_fence))
        } else {
            DdlFenceGuard::shared(Arc::clone(&self.ddl_fence))
        }
    }

    /// Admit one auto-commit catalog writer for the complete pin-to-commit
    /// interval without recursively owning the physical DDL fence.
    #[doc(hidden)]
    pub fn acquire_catalog_write_fence(&self) -> CatalogWriteFenceGuard {
        CatalogWriteFenceGuard::exclusive(Arc::clone(&self.catalog_write_fence))
    }

    /// Keep one general SELECT on a single committed cross-table epoch.
    #[doc(hidden)]
    pub fn acquire_statement_visibility_fence(&self) -> Option<VisibilityFenceGuard> {
        #[cfg(feature = "test-mutations")]
        if crate::test_mutations::statement_visibility_fence_disabled() {
            return None;
        }
        Some(VisibilityFenceGuard::shared(Arc::clone(
            &self.visibility_fence,
        )))
    }

    /// Creates a new MVCC engine with the given configuration
    pub fn new(config: Config) -> Self {
        Self::new_with_partial_index_predicate_binder(
            config,
            missing_partial_index_predicate_binder,
        )
    }

    /// Creates an engine with the composition-layer binder required to reopen
    /// durable partial indexes without making storage depend on SQL parsing.
    pub fn new_with_partial_index_predicate_binder(
        config: Config,
        partial_index_predicate_binder: crate::index::PartialIndexPredicateBinder,
    ) -> Self {
        Self::new_with_optional_composition_binders(
            config,
            partial_index_predicate_binder,
            None,
            None,
            None,
        )
    }

    /// Creates an engine with all upper-layer binders used by durable storage
    /// lifecycle operations.
    pub fn new_with_composition_binders(
        config: Config,
        partial_index_predicate_binder: crate::index::PartialIndexPredicateBinder,
        row_validator_binder: crate::validation::RowValidatorBinder,
        view_dependency_binder: ViewDependencyBinder,
        catalog_runtime_binder: impl super::IntoCatalogRuntimeBinder,
    ) -> Self {
        Self::new_with_optional_composition_binders(
            config,
            partial_index_predicate_binder,
            Some(row_validator_binder),
            Some(view_dependency_binder),
            Some(catalog_runtime_binder.into_catalog_runtime_binder()),
        )
    }

    pub(super) fn new_with_optional_composition_binders(
        config: Config,
        partial_index_predicate_binder: crate::index::PartialIndexPredicateBinder,
        row_validator_binder: Option<crate::validation::RowValidatorBinder>,
        view_dependency_binder: Option<ViewDependencyBinder>,
        catalog_runtime_binder: Option<CatalogRuntimeBinder>,
    ) -> Self {
        let path = config.path.clone().unwrap_or_default();
        let storage_cpu_runtime =
            crate::cpu_runtime::StorageCpuRuntime::new(config.persistence.storage_cpu_workers);
        let page_cache_warmup = crate::page_cache::PageCacheWarmupController::new(
            Path::new(&path),
            config.persistence.page_cache_level,
            config.persistence.page_cache_max_bytes,
            config.persistence.page_cache_memory_reserve,
        );

        let registry = Arc::new(TransactionRegistry::new());
        // A constructed engine is not ready. Admission is published only after
        // lock acquisition, persistence construction and recovery all succeed.
        registry.stop_accepting_transactions();

        let row_validator_binder_cell = OnceLock::new();
        if let Some(row_validator_binder) = row_validator_binder {
            let _ = row_validator_binder_cell.set(row_validator_binder);
        }
        let view_dependency_binder_cell = OnceLock::new();
        if let Some(view_dependency_binder) = view_dependency_binder {
            let _ = view_dependency_binder_cell.set(view_dependency_binder);
        }
        let catalog_runtime_binder_cell = OnceLock::new();
        if let Some(catalog_runtime_binder) = catalog_runtime_binder {
            let _ = catalog_runtime_binder_cell.set(catalog_runtime_binder);
        } else {
            #[cfg(test)]
            let _ = catalog_runtime_binder_cell.set(CatalogRuntimeBinder::new(
                catalog_runtime::bind_test_catalog_runtime,
            ));
        }
        let catalog_publisher = Arc::new(ArcSwap::from_pointee(CatalogPublisher::new(Arc::new(
            Self::bootstrap_catalog_generation(),
        ))));

        Self {
            path: if path.is_empty() {
                "memory://".to_string()
            } else {
                path
            },
            config: RwLock::new(config),
            partial_index_predicate_binder,
            row_validator_binder: row_validator_binder_cell,
            view_dependency_binder: view_dependency_binder_cell,
            catalog_runtime_binder: catalog_runtime_binder_cell,
            schemas: Arc::new(RwLock::new(FxHashMap::default())),
            version_stores: Arc::new(RwLock::new(FxHashMap::default())),
            pending_tables: Arc::new(RwLock::new(FxHashMap::default())),
            registry,
            open: AtomicBool::new(false),
            opened_once: AtomicBool::new(false),
            lifecycle: RwLock::new(EngineLifecycleState::Closed),
            runtime_snapshot_sequence: AtomicU64::new(0),
            runtime_maintenance: Arc::new(EngineMaintenanceState::new()),
            shutdown_checkpoint_complete: AtomicBool::new(false),
            txn_version_stores: Arc::new(RwLock::new(I64Map::new())),
            views: Arc::new(RwLock::new(FxHashMap::default())),
            catalog_publisher,
            physical_generation: Arc::new(ArcSwapOption::empty()),
            persistence: Arc::new(ArcSwapOption::empty()),
            loading_from_disk: Arc::new(AtomicBool::new(false)),
            file_lock: Mutex::new(None),
            startup_mutex: Mutex::new(()),
            schema_epoch: Arc::new(AtomicU64::new(0)),
            schema_scope_id: NEXT_SCHEMA_SCOPE_ID.fetch_add(1, Ordering::Relaxed),
            cleanup_handle: Mutex::new(None),
            page_cache_warmup,
            page_cache_warmup_handle: Mutex::new(None),
            pressure_seal: Arc::new(PressureSealControl::new()),
            fk_reverse_cache: RwLock::new((u64::MAX, StringMap::default())),
            segment_managers: Arc::new(RwLock::new(FxHashMap::default())),
            storage_cpu_runtime,
            force_seal_all: AtomicBool::new(false),
            checkpoint_mutex: Mutex::new(()),
            seal_fence: Arc::new(parking_lot::RwLock::new(())),
            ddl_fence: Arc::new(parking_lot::RwLock::new(())),
            catalog_write_fence: Arc::new(parking_lot::Mutex::new(())),
            visibility_fence: Arc::new(parking_lot::RwLock::new(())),
            snapshot_maintenance_fence: Arc::new(parking_lot::RwLock::new(())),
            compaction_running: Arc::new(AtomicBool::new(false)),
            compaction_requested: Arc::new(AtomicBool::new(false)),
            compaction_retry_cooldown: Arc::new(CompactionRetryCooldown::default()),
            compaction_job_concurrency: Arc::new(CompactionJobConcurrencyState::default()),
            compaction_soft_backpressure_waits: Arc::new(AtomicU64::new(0)),
            compaction_soft_backpressure_wait_millis: Arc::new(AtomicU64::new(0)),
            compaction_hard_backpressure_rejections: Arc::new(AtomicU64::new(0)),
            eviction_epoch: AtomicU64::new(0),
        }
    }

    /// Creates a new in-memory MVCC engine
    pub fn in_memory() -> Self {
        Self::new(Config::default())
    }

    /// Install the upper-layer row validator once. Engines used directly keep
    /// the schema-only fallback; constructing an executor supplies CHECK
    /// compilation before subsequent transactions snapshot their callbacks.
    pub fn install_row_validator_binder(&self, binder: crate::validation::RowValidatorBinder) {
        let _ = self.row_validator_binder.set(binder);
    }

    /// Install the SQL-layer view dependency binder once. Direct storage
    /// engines without this composition port reject raw or persisted view SQL.
    pub fn install_view_dependency_binder(&self, binder: ViewDependencyBinder) {
        let _ = self.view_dependency_binder.set(binder);
    }

    /// Install the upper-layer catalog-to-runtime binder once. Persistent
    /// facades provide it before recovery; an empty direct engine may receive
    /// it later when its executor owner is constructed.
    #[doc(hidden)]
    pub fn install_catalog_runtime_binder(&self, binder: impl super::IntoCatalogRuntimeBinder) {
        let _ = self
            .catalog_runtime_binder
            .set(binder.into_catalog_runtime_binder());
    }

    pub(super) fn catalog_runtime_binder(&self) -> CatalogRuntimeBinder {
        self.catalog_runtime_binder
            .get()
            .cloned()
            .unwrap_or_else(|| {
                CatalogRuntimeBinder::new(catalog_runtime::missing_catalog_runtime_binder)
            })
    }

    #[inline]
    pub(super) fn persistence(&self) -> Option<Arc<PersistenceManager>> {
        self.persistence.load_full()
    }

    /// Restore the pre-open in-memory state after recovery failed.
    ///
    /// Admission is stopped for the whole transition, so none of these maps
    /// can contain caller-owned work. Dropping managers (instead of calling
    /// their destructive clear paths) preserves every persistent artifact for
    /// a later retry or diagnostic open.
    pub(super) fn rollback_failed_startup(&self) {
        self.registry.stop_accepting_transactions();
        self.loading_from_disk.store(false, Ordering::Release);
        self.force_seal_all.store(false, Ordering::Release);
        self.compaction_requested.store(false, Ordering::Release);
        self.pressure_seal.finish_cycle(false);

        if let Some(pm) = self.persistence() {
            let _ = pm.stop();
        }
        self.persistence.store(None);
        self.physical_generation.store(None);

        {
            let mut stores = self.version_stores.write().unwrap();
            for store in stores.values() {
                store.close();
            }
            stores.clear();
        }
        self.schemas.write().unwrap().clear();
        self.pending_tables.write().unwrap().clear();
        self.txn_version_stores.write().unwrap().clear();
        self.views.write().unwrap().clear();
        self.segment_managers.write().unwrap().clear();
        self.schema_epoch.store(0, Ordering::Release);
        self.registry.reset_after_failed_startup();
        self.open.store(false, Ordering::Release);
        self.shutdown_checkpoint_complete
            .store(false, Ordering::Release);
    }

    pub(super) fn fail_startup(&self, error: Error) -> Error {
        self.rollback_failed_startup();
        *self.lifecycle.write().unwrap() = EngineLifecycleState::Failed(error.clone());
        error
    }

    /// Return the authoritative engine lifecycle outcome.
    pub fn lifecycle_state(&self) -> EngineLifecycleState {
        self.lifecycle.read().unwrap().clone()
    }

    /// Capture the engine's bounded observability contract without joining a
    /// workload lock queue.
    pub fn runtime_stats_snapshot(&self) -> EngineRuntimeStatsV2 {
        let started = std::time::Instant::now();
        let captured_unix_millis = runtime_unix_millis();
        let sequence = self
            .runtime_snapshot_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let mut missing_evidence = Vec::new();
        let mut truncated_owners = Vec::new();

        let lifecycle = match self.lifecycle.try_read() {
            Ok(state) => match &*state {
                EngineLifecycleState::Closed => "closed",
                EngineLifecycleState::Opening => "opening",
                EngineLifecycleState::Ready => "ready",
                EngineLifecycleState::Closing => "closing",
                EngineLifecycleState::CloseFailed(_) => "close_failed",
                EngineLifecycleState::Failed(_) => "failed",
            }
            .to_string(),
            Err(_) => {
                missing_evidence.push("lifecycle_busy".to_string());
                "unknown".to_string()
            }
        };

        let transaction = self
            .registry
            .runtime_snapshot(RUNTIME_STATS_MAX_TRANSACTIONS);
        if transaction.registry_busy {
            missing_evidence.push("transaction_registry_busy".to_string());
        }
        if transaction.wait_edges.is_none() {
            missing_evidence.push("transaction_wait_graph_busy".to_string());
        }
        if transaction.truncated {
            truncated_owners.push("transactions".to_string());
        }

        let (mut hot_tables, mut hot_rows, mut hot_bytes) = (0_u64, 0_u64, 0_u64);
        match self.version_stores.try_read() {
            Ok(stores) => {
                hot_tables = stores.len() as u64;
                if stores.len() > RUNTIME_STATS_MAX_TABLES {
                    truncated_owners.push("hot_tables".to_string());
                }
                for store in stores.values().take(RUNTIME_STATS_MAX_TABLES) {
                    hot_rows = hot_rows.saturating_add(store.committed_row_count() as u64);
                    hot_bytes = hot_bytes.saturating_add(store.committed_hot_bytes() as u64);
                }
            }
            Err(_) => missing_evidence.push("hot_owner_map_busy".to_string()),
        }

        let (mut staging_transactions, mut staging_tables, mut staging_rows) =
            (0_u64, 0_u64, 0_u64);
        match self.txn_version_stores.try_read() {
            Ok(stores) => {
                staging_transactions = stores.len() as u64;
                if stores.len() > RUNTIME_STATS_MAX_TRANSACTIONS {
                    truncated_owners.push("staging_transactions".to_string());
                }
                'transactions: for (_, tables) in stores.iter().take(RUNTIME_STATS_MAX_TRANSACTIONS)
                {
                    for (_, store) in tables {
                        if staging_tables as usize >= RUNTIME_STATS_MAX_STAGING_TABLES {
                            truncated_owners.push("staging_tables".to_string());
                            break 'transactions;
                        }
                        staging_tables = staging_tables.saturating_add(1);
                        match store.try_read() {
                            Ok(store) => {
                                staging_rows =
                                    staging_rows.saturating_add(store.local_count() as u64);
                            }
                            Err(_) => {
                                missing_evidence.push("staging_store_busy".to_string());
                            }
                        }
                    }
                }
            }
            Err(_) => missing_evidence.push("staging_owner_map_busy".to_string()),
        }

        let (
            mut cold_tables,
            mut cold_segments,
            mut cold_unleveled_segments,
            mut cold_l0_segments,
            mut cold_l1_segments,
            mut cold_l0_debt_physical_bytes,
            mut cold_rows,
            mut cold_resident_bytes,
            mut cold_metadata_bytes,
            mut cold_row_id_bytes,
            mut cold_exact_index_bytes,
            mut cold_ordered_index_bytes,
            mut cold_descriptor_bytes,
            mut cold_column_payload_bytes,
            mut cold_tombstones,
        ) = (
            0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64,
            0_u64, 0_u64, 0_u64,
        );
        match self.segment_managers.try_read() {
            Ok(managers) => {
                cold_tables = managers.len() as u64;
                if managers.len() > RUNTIME_STATS_MAX_TABLES {
                    truncated_owners.push("cold_tables".to_string());
                }
                let mut remaining_segments = RUNTIME_STATS_MAX_SEGMENTS;
                for manager in managers.values().take(RUNTIME_STATS_MAX_TABLES) {
                    let owner = manager.runtime_owner_snapshot(remaining_segments);
                    cold_segments = cold_segments.saturating_add(owner.segments);
                    cold_unleveled_segments =
                        cold_unleveled_segments.saturating_add(owner.unleveled_segments);
                    cold_l0_segments = cold_l0_segments.saturating_add(owner.l0_segments);
                    cold_l1_segments = cold_l1_segments.saturating_add(owner.l1_segments);
                    cold_l0_debt_physical_bytes =
                        cold_l0_debt_physical_bytes.saturating_add(owner.l0_debt_physical_bytes);
                    cold_rows = cold_rows.saturating_add(owner.rows);
                    cold_resident_bytes = cold_resident_bytes.saturating_add(owner.resident_bytes);
                    cold_metadata_bytes = cold_metadata_bytes.saturating_add(owner.metadata_bytes);
                    cold_row_id_bytes = cold_row_id_bytes.saturating_add(owner.row_id_bytes);
                    cold_exact_index_bytes =
                        cold_exact_index_bytes.saturating_add(owner.exact_index_bytes);
                    cold_ordered_index_bytes =
                        cold_ordered_index_bytes.saturating_add(owner.ordered_index_bytes);
                    cold_descriptor_bytes =
                        cold_descriptor_bytes.saturating_add(owner.descriptor_bytes);
                    cold_column_payload_bytes =
                        cold_column_payload_bytes.saturating_add(owner.column_payload_bytes);
                    cold_tombstones = cold_tombstones.saturating_add(owner.tombstones);
                    if owner.level_metadata_busy {
                        missing_evidence.push("cold_manifest_busy".to_string());
                    }
                    let visited = usize::try_from(owner.segments)
                        .unwrap_or(usize::MAX)
                        .min(remaining_segments);
                    remaining_segments = remaining_segments.saturating_sub(visited);
                    if owner.truncated || remaining_segments == 0 {
                        truncated_owners.push("cold_segments".to_string());
                        break;
                    }
                }
            }
            Err(_) => missing_evidence.push("cold_owner_map_busy".to_string()),
        }

        let (
            read_queue_depth,
            max_compaction_jobs,
            max_compaction_input_segments,
            max_compaction_input_bytes,
            max_compaction_output_bytes,
            compaction_job_time_budget_ms,
            compaction_io_bytes_per_sec,
            compaction_disk_reserve_bytes,
            compaction_retry_cooldown_ms,
            l0_soft_limit_segments,
            l0_hard_limit_segments,
            l0_soft_limit_bytes,
            l0_hard_limit_bytes,
        ) = match self.config.try_read() {
            Ok(config) => {
                let soft_segments = config.persistence.l0_soft_limit_segments.max(1) as u64;
                let soft_bytes = config.persistence.l0_soft_limit_bytes.max(1);
                (
                    config.persistence.read_queue_depth as u64,
                    config.persistence.max_compaction_jobs as u64,
                    config.persistence.max_compaction_input_segments as u64,
                    config.persistence.max_compaction_input_bytes,
                    config.persistence.max_compaction_output_bytes,
                    config.persistence.compaction_job_time_budget_ms,
                    config.persistence.compaction_io_bytes_per_sec,
                    config.persistence.compaction_disk_reserve_bytes,
                    config.persistence.compaction_retry_cooldown_ms,
                    soft_segments,
                    (config.persistence.l0_hard_limit_segments as u64)
                        .max(soft_segments.saturating_add(1)),
                    soft_bytes,
                    config
                        .persistence
                        .l0_hard_limit_bytes
                        .max(soft_bytes.saturating_add(1)),
                )
            }
            Err(_) => {
                missing_evidence.push("config_busy".to_string());
                (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            }
        };
        let (
            compaction_retry_cooldown_active,
            compaction_retry_cooldown_until_unix_millis,
            compaction_retry_suppressed,
            compaction_retry_last_reason,
        ) = self.compaction_retry_cooldown.snapshot();
        let page_cache_warmup = self.page_cache_warmup.snapshot();
        if page_cache_warmup.is_none() {
            missing_evidence.push("page_cache_warmup_busy".to_string());
        }
        let (
            wal_current_lsn,
            wal_current_file_bytes,
            wal_max_file_bytes,
            wal_running,
            wal_pending_durability_bytes,
            last_checkpoint_unix_nanos,
        ) = self
            .persistence()
            .map_or((0, 0, 0, false, Some(0), 0), |persistence| {
                let (current_bytes, max_bytes, running, pending_bytes) =
                    persistence.wal_runtime_state();
                (
                    persistence.current_lsn(),
                    current_bytes,
                    max_bytes,
                    running,
                    pending_bytes,
                    persistence.last_checkpoint_time(),
                )
            });
        let wal_pending_durability_bytes = match wal_pending_durability_bytes {
            Some(bytes) => bytes,
            None => {
                missing_evidence.push("wal_buffer_busy".to_string());
                0
            }
        };
        let checkpoint_mutex_busy = match self.checkpoint_mutex.try_lock() {
            Ok(guard) => {
                drop(guard);
                false
            }
            Err(_) => true,
        };
        let maintenance = self.runtime_maintenance.snapshot(captured_unix_millis);
        let storage_cpu = self.storage_cpu_runtime.snapshot();

        missing_evidence.sort();
        missing_evidence.dedup();
        truncated_owners.sort();
        truncated_owners.dedup();

        let mut snapshot = EngineRuntimeStatsV2 {
            format: 2,
            sequence,
            captured_unix_millis,
            snapshot_nanos: 0,
            complete: missing_evidence.is_empty() && truncated_owners.is_empty(),
            missing_evidence,
            truncated_owners,
            lifecycle,
            schema_epoch: self.schema_epoch.load(Ordering::Acquire),
            active_transactions: transaction.active,
            accepting_transactions: transaction.accepting,
            oldest_transaction_begin_sequence: transaction.oldest_begin_sequence,
            oldest_transaction_age_millis: transaction.oldest_begin_timestamp_nanos.map(
                |started| captured_unix_millis.saturating_sub((started.max(0) as u64) / 1_000_000),
            ),
            transaction_wait_edges: transaction.wait_edges,
            hot_tables,
            hot_rows,
            hot_bytes,
            staging_transactions,
            staging_tables,
            staging_rows,
            cold_tables,
            cold_segments,
            cold_unleveled_segments,
            cold_l0_segments,
            cold_l1_segments,
            cold_l0_debt_physical_bytes,
            cold_rows,
            cold_resident_bytes,
            cold_metadata_bytes,
            cold_row_id_bytes,
            cold_exact_index_bytes,
            cold_ordered_index_bytes,
            cold_descriptor_bytes,
            cold_column_payload_bytes,
            cold_tombstones,
            pressure_seal_requested: self.pressure_seal.requested.load(Ordering::Acquire),
            compaction_requested: self.compaction_requested.load(Ordering::Acquire),
            max_compaction_jobs,
            compaction_active_jobs: self.compaction_job_concurrency.active(),
            compaction_peak_active_jobs: self.compaction_job_concurrency.peak(),
            max_compaction_input_segments,
            max_compaction_input_bytes,
            max_compaction_output_bytes,
            compaction_job_time_budget_ms,
            compaction_io_bytes_per_sec,
            compaction_disk_reserve_bytes,
            compaction_retry_cooldown_ms,
            compaction_retry_cooldown_active,
            compaction_retry_cooldown_until_unix_millis,
            compaction_retry_suppressed,
            compaction_retry_last_reason,
            l0_soft_limit_segments,
            l0_hard_limit_segments,
            l0_soft_limit_bytes,
            l0_hard_limit_bytes,
            compaction_soft_backpressure_waits: self
                .compaction_soft_backpressure_waits
                .load(Ordering::Relaxed),
            compaction_soft_backpressure_wait_millis: self
                .compaction_soft_backpressure_wait_millis
                .load(Ordering::Relaxed),
            compaction_hard_backpressure_rejections: self
                .compaction_hard_backpressure_rejections
                .load(Ordering::Relaxed),
            seal_running: maintenance.seal.active,
            compaction_running: self.compaction_running.load(Ordering::Acquire),
            checkpoint_running: maintenance.checkpoint.active,
            checkpoint_mutex_busy,
            wal_current_lsn,
            wal_current_file_bytes,
            wal_max_file_bytes,
            wal_pending_durability_bytes,
            wal_running,
            last_checkpoint_unix_nanos,
            page_cache_warmup,
            read_queue_depth,
            storage_cpu_workers_configured: storage_cpu.configured_workers as u64,
            storage_cpu_workers_effective: storage_cpu.effective_workers as u64,
            storage_cpu_workers_in_use: storage_cpu.workers_in_use as u64,
            storage_cpu_peak_workers_in_use: storage_cpu.peak_workers_in_use as u64,
            storage_cpu_workers_reserved: storage_cpu.workers_reserved as u64,
            storage_cpu_peak_workers_reserved: storage_cpu.peak_workers_reserved as u64,
            storage_cpu_leases: storage_cpu.leases,
            storage_cpu_parallel_leases: storage_cpu.parallel_leases,
            maintenance,
            runtime_owners: instrumentation::runtime_owner_snapshot(),
            owner_visit_limits: EngineRuntimeVisitLimits {
                max_tables: RUNTIME_STATS_MAX_TABLES as u64,
                max_segments: RUNTIME_STATS_MAX_SEGMENTS as u64,
                max_transactions: RUNTIME_STATS_MAX_TRANSACTIONS as u64,
                max_staging_tables: RUNTIME_STATS_MAX_STAGING_TABLES as u64,
                max_active_segment_ids: RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS as u64,
            },
            counters: instrumentation::snapshot(),
        };
        snapshot.snapshot_nanos = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        snapshot
    }

    pub(super) fn record_close_failure(&self, error: Error) -> Error {
        *self.lifecycle.write().unwrap() = EngineLifecycleState::CloseFailed(error.clone());
        error
    }

    /// Opens the engine (inherent method)
    pub fn open_engine(&self) -> Result<()> {
        let _startup_guard = self
            .startup_mutex
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("engine startup".to_string()))?;

        if self.open.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.opened_once.load(Ordering::Acquire) {
            return Err(Error::EngineNotOpen);
        }

        *self.lifecycle.write().unwrap() = EngineLifecycleState::Opening;
        self.registry.stop_accepting_transactions();
        self.loading_from_disk.store(false, Ordering::Release);

        // Keep the lock local throughout startup. Publishing it into engine
        // state happens only after recovery succeeds; every error path drops it.
        let acquired_lock = match (self.path != "memory://")
            .then(|| FileLock::acquire(&self.path))
            .transpose()
        {
            Ok(lock) => lock,
            Err(error) => return Err(self.fail_startup(error)),
        };
        let persistent_requested = {
            let config = self.config.read().unwrap();
            config.is_persistent()
        };
        let startup_result = if persistent_requested {
            let persistence_config = self.config.read().unwrap().persistence.clone();
            reconcile_physical_restore(Path::new(&self.path)).and_then(|()| {
                self.recover_persistent_runtime(
                    &persistence_config,
                    acquired_lock
                        .as_ref()
                        .expect("persistent startup owns a database writer lock"),
                )
                .map(|publisher| self.physical_generation.store(Some(publisher)))
            })
        } else {
            Ok(())
        };

        if let Err(error) = startup_result {
            drop(acquired_lock);
            return Err(self.fail_startup(error));
        }

        *self.file_lock.lock().unwrap() = acquired_lock;
        self.registry.start_accepting_transactions();
        self.opened_once.store(true, Ordering::Release);
        self.open.store(true, Ordering::Release);
        *self.lifecycle.write().unwrap() = EngineLifecycleState::Ready;

        // Note: Cleanup is started separately via start_cleanup() after Arc wrapping
        // because start_periodic_cleanup requires Arc<Self>

        Ok(())
    }

    /// Starts the background cleanup thread
    ///
    /// This should be called after wrapping the engine in Arc.
    /// The cleanup thread periodically removes:
    /// - Deleted rows older than the retention period
    /// - Old previous versions no longer needed
    /// - Old transaction metadata (in Snapshot Isolation mode only)
    pub fn start_cleanup(self: &Arc<Self>) {
        let config = self.config.read().unwrap();
        let cleanup_enabled = config.cleanup.enabled;
        let persistence_enabled = config.persistence.enabled && self.path != "memory://";
        let page_cache_enabled = self.page_cache_warmup.enabled();
        if !cleanup_enabled && !persistence_enabled && !page_cache_enabled {
            return;
        }

        let interval = std::time::Duration::from_secs(config.cleanup.interval_secs);
        let deleted_row_retention =
            std::time::Duration::from_secs(config.cleanup.deleted_row_retention_secs);
        let txn_retention =
            std::time::Duration::from_secs(config.cleanup.transaction_retention_secs);
        drop(config);

        {
            let mut cleanup_handle = self.cleanup_handle.lock().unwrap();
            if cleanup_handle.is_none() && (cleanup_enabled || persistence_enabled) {
                *cleanup_handle = Some(self.start_periodic_cleanup_internal(
                    cleanup_enabled,
                    interval,
                    deleted_row_retention,
                    txn_retention,
                ));
            }
        }
        if page_cache_enabled {
            self.refresh_page_cache_volume_priorities();
            {
                let mut handle = self.page_cache_warmup_handle.lock().unwrap();
                if handle.is_none() {
                    *handle = self.page_cache_warmup.start();
                }
            }
            self.request_page_cache_warmup();
        }
    }

    /// Request reconciliation with the newest durably published generation.
    pub fn request_page_cache_warmup(&self) {
        self.refresh_page_cache_volume_priorities();
        let Some(publisher) = self.physical_generation.load_full() else {
            self.page_cache_warmup
                .reject_request("CONTROL-selected physical generation is unavailable");
            return;
        };
        match publisher.pin() {
            Ok(generation) => self.page_cache_warmup.request(generation),
            Err(error) => self.page_cache_warmup.reject_request(format!(
                "cannot pin CONTROL-selected physical generation: {error}"
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn pin_physical_generation_for_test(&self) -> crate::v6::PhysicalGenerationLease {
        self.physical_generation
            .load_full()
            .expect("persistent test engine must own a physical generation")
            .pin()
            .expect("test must pin the physical generation")
    }

    pub(super) fn refresh_page_cache_volume_priorities(&self) {
        if !self.page_cache_warmup.enabled() {
            return;
        }
        let priorities = self
            .segment_managers
            .read()
            .unwrap()
            .values()
            .flat_map(|manager| manager.page_cache_volume_priorities())
            .collect();
        self.page_cache_warmup.set_volume_priorities(priorities);
    }

    /// Wait for the current request without making warmup part of database
    /// startup correctness.
    pub fn wait_for_page_cache_warmup(&self, timeout: Duration) -> bool {
        self.page_cache_warmup.wait_until_idle(timeout)
    }

    pub fn page_cache_warmup_snapshot(&self) -> Option<crate::PageCacheWarmupSnapshot> {
        self.page_cache_warmup.snapshot()
    }

    /// Whether committed hot state crossed a per-table or process-wide byte
    /// budget. Row thresholds remain a layout/periodic-seal policy; only the
    /// memory budget is allowed to backpressure ordinary commits.
    pub(super) fn hot_seal_pressure_exceeded(&self) -> bool {
        let (first_bytes, incremental_bytes) = self
            .config
            .read()
            .map(|config| {
                (
                    config.persistence.seal_hot_bytes_threshold,
                    config.persistence.seal_incremental_hot_bytes_threshold,
                )
            })
            .unwrap_or((64 * 1024 * 1024, 16 * 1024 * 1024));
        let stores: Vec<(String, Arc<VersionStore>)> = self
            .version_stores
            .read()
            .unwrap()
            .iter()
            .map(|(name, store)| (name.clone(), Arc::clone(store)))
            .collect();
        let total_hot_bytes = stores.iter().fold(0usize, |total, (_, store)| {
            total.saturating_add(store.committed_hot_bytes())
        });
        if total_hot_bytes >= total_hot_soft_threshold(first_bytes) {
            return true;
        }

        let managers = self.segment_managers.read().unwrap();
        stores.into_iter().any(|(name, store)| {
            let incremental = managers
                .get(&name)
                .is_some_and(|manager| manager.has_segments());
            let byte_threshold = if incremental {
                incremental_bytes
            } else {
                first_bytes
            };
            store.committed_hot_bytes() >= byte_threshold
        })
    }

    pub(super) fn l0_pressure_limits(&self) -> L0PressureLimits {
        let config = self.config.read().unwrap();
        let soft_segments = config.persistence.l0_soft_limit_segments.max(1) as u64;
        let hard_segments =
            (config.persistence.l0_hard_limit_segments as u64).max(soft_segments.saturating_add(1));
        let soft_bytes = config.persistence.l0_soft_limit_bytes.max(1);
        let hard_bytes = config
            .persistence
            .l0_hard_limit_bytes
            .max(soft_bytes.saturating_add(1));
        L0PressureLimits {
            soft_segments,
            hard_segments,
            soft_bytes,
            hard_bytes,
            soft_wait: Duration::from_millis(config.persistence.l0_soft_backpressure_wait_ms),
        }
    }

    pub(super) fn l0_soft_pressure_exceeded(&self) -> bool {
        let limits = self.l0_pressure_limits();
        self.segment_managers
            .read()
            .unwrap()
            .values()
            .any(|manager| {
                classify_l0_pressure(manager.l0_debt_snapshot(), limits) != L0PressureLevel::Normal
            })
    }

    /// Drain threshold-crossing hot generations without advancing checkpoint
    /// LSN, truncating WAL, force-sealing stragglers, or scheduling compaction.
    /// The newly published segment manifests are nevertheless made durable so
    /// a crash can never depend on process-local catalog state.
    pub(super) fn pressure_seal_cycle(&self) -> Result<PressureSealCycleOutcome> {
        let Some(pm) = self.persistence() else {
            return Ok(PressureSealCycleOutcome::Idle);
        };
        if !pm.is_enabled() {
            return Ok(PressureSealCycleOutcome::Idle);
        }

        let _ddl_generation_guard = DdlFenceGuard::shared(Arc::clone(&self.ddl_fence));
        let _checkpoint_guard = self.lock_checkpoint_mutex_profiled();
        if !self.hot_seal_pressure_exceeded() {
            return Ok(PressureSealCycleOutcome::Idle);
        }

        let outcome = self.seal_next_hot_buffer_under_catalog_generation()?;
        self.request_page_cache_warmup();
        if self.l0_soft_pressure_exceeded() {
            self.compaction_requested.store(true, Ordering::Release);
        }
        // Pressure seal is advisory. A concurrent physical publisher may move
        // CONTROL after this cycle built its artifact, in which case the hot
        // suffix deliberately remains available for a later retry. Do not
        // keep foreground commits parked behind immediate retries of that
        // superseded source: releasing one commit lets the normal threshold
        // path re-arm maintenance without turning the advisory worker into an
        // unbounded commit outage.
        Ok(if outcome.stale_publication {
            PressureSealCycleOutcome::Superseded
        } else {
            PressureSealCycleOutcome::Completed
        })
    }

    /// Internal method to start periodic cleanup with configurable parameters
    pub(super) fn start_periodic_cleanup_internal(
        self: &Arc<Self>,
        cleanup_enabled: bool,
        interval: std::time::Duration,
        deleted_row_retention: std::time::Duration,
        txn_retention: std::time::Duration,
    ) -> CleanupHandle {
        use std::sync::atomic::AtomicBool;
        use std::thread;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = Arc::clone(&stop_flag);
        let engine = Arc::clone(self);
        engine.pressure_seal.worker_started();
        engine
            .runtime_maintenance
            .background_worker_alive
            .store(true, Ordering::Release);

        let handle = thread::spawn(move || {
            let _pressure_worker_guard = PressureSealWorkerGuard(Arc::clone(&engine.pressure_seal));
            let _runtime_worker_guard =
                BackgroundWorkerRuntimeGuard(Arc::clone(&engine.runtime_maintenance));
            let mut time_since_cleanup = std::time::Duration::ZERO;
            let mut time_since_artifact_retirement = std::time::Duration::from_secs(1);
            let check_interval = std::time::Duration::from_millis(100);
            let artifact_retirement_retry_interval = std::time::Duration::from_secs(1);

            while !stop_flag_clone.load(Ordering::Acquire) {
                thread::sleep(check_interval);
                if stop_flag_clone.load(Ordering::Acquire) {
                    break;
                }
                engine
                    .runtime_maintenance
                    .background_loop_epoch
                    .fetch_add(1, Ordering::Relaxed);

                if engine.pressure_seal.requested.load(Ordering::Acquire) {
                    match engine.pressure_seal_cycle() {
                        Ok(outcome) => {
                            let still_requested =
                                outcome.keeps_request_armed(engine.hot_seal_pressure_exceeded());
                            engine.pressure_seal.finish_cycle(still_requested);
                            engine.runtime_maintenance.record_background_success();
                        }
                        Err(error) => {
                            eprintln!("Warning: pressure seal failed: {}", error);
                            // Never turn a maintenance error into an unbounded
                            // commit outage. A later commit re-arms the request.
                            engine.pressure_seal.finish_cycle(false);
                        }
                    }
                }

                if engine.compaction_requested.swap(false, Ordering::AcqRel) {
                    if engine.compaction_running.load(Ordering::Acquire) {
                        engine.compaction_requested.store(true, Ordering::Release);
                    } else {
                        engine.spawn_compaction();
                    }
                }

                time_since_artifact_retirement += check_interval;
                if time_since_artifact_retirement >= artifact_retirement_retry_interval {
                    let retirement = engine
                        .physical_generation
                        .load_full()
                        .map(|publisher| publisher.run_scheduled_retirement());
                    match retirement {
                        Some(Ok(Some(_))) => {
                            time_since_artifact_retirement = std::time::Duration::ZERO;
                            engine.runtime_maintenance.record_background_success();
                        }
                        Some(Err(error)) => {
                            time_since_artifact_retirement = std::time::Duration::ZERO;
                            eprintln!(
                                "Warning: immutable-member retirement requires retry: {error}"
                            );
                        }
                        Some(Ok(None)) | None => {}
                    }
                }

                time_since_cleanup += check_interval;
                if cleanup_enabled && time_since_cleanup >= interval {
                    time_since_cleanup = std::time::Duration::ZERO;
                    let _txn_count = engine.cleanup_old_transactions(txn_retention);
                    let _row_count = engine.cleanup_deleted_rows(deleted_row_retention);
                    let _prev_version_count = engine.cleanup_old_previous_versions();
                    engine.runtime_maintenance.record_background_success();
                }

                let current_checkpoint_interval = {
                    let cfg = engine.config.read().unwrap();
                    if cfg.persistence.checkpoint_interval > 0 {
                        std::time::Duration::from_secs(cfg.persistence.checkpoint_interval as u64)
                    } else {
                        std::time::Duration::ZERO
                    }
                };

                // Auto-checkpoint remains an independent durability cadence.
                if !current_checkpoint_interval.is_zero() {
                    if let Some(pm) = engine.persistence() {
                        if pm.try_begin_scheduled_checkpoint(current_checkpoint_interval) {
                            // Call checkpoint_cycle_inner directly (not
                            // checkpoint_cycle) so compaction is spawned on
                            // a separate thread instead of running synchronously.
                            match engine.checkpoint_cycle_inner(false) {
                                Ok(()) => {
                                    // Eviction runs inside spawn_compaction, AFTER
                                    // compaction finishes. Running them in the same
                                    // cycle but concurrently causes thrashing:
                                    // eviction frees volumes that compaction
                                    // immediately reloads via segments_snapshot.
                                    engine.spawn_compaction();
                                    engine.runtime_maintenance.record_background_success();
                                }
                                Err(e) => {
                                    eprintln!("Warning: checkpoint cycle failed: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        CleanupHandle {
            stop_flag,
            thread: Some(handle),
        }
    }

    /// Populate all indexes across all version stores in a single pass per table
    pub(super) fn populate_all_indexes(&self) -> Result<()> {
        let stores = self.version_stores.read().unwrap();
        for store in stores.values() {
            store.populate_all_indexes()?;
        }
        Ok(())
    }

    /// Populate HNSW indexes from cold segment data.
    ///
    /// After WAL replay + populate_all_indexes(), HNSW indexes only contain hot rows.
    /// Cold segment rows must also be added because vector similarity search cannot
    /// fall back to zone maps like B-tree/Hash indexes can.
    ///
    /// Uses newest-first volume ordering with row_id dedup so that when the same
    /// row_id exists in multiple overlapping volumes, only the newest version is
    /// added to the HNSW graph. Also skips row_ids that are in the hot buffer
    /// (already indexed by populate_all_indexes).
    pub(super) fn populate_hnsw_from_segments(&self) -> Result<()> {
        let stores = self.version_stores.read().unwrap();
        let mgrs = self.segment_managers.read().unwrap();

        for (table_name, store) in stores.iter() {
            if let Some(mgr) = mgrs.get(table_name) {
                if !mgr.has_segments() {
                    continue;
                }

                // Collect complete, non-partial index definitions before
                // iterating volumes. Partial predicates require full-row
                // evaluation and retain their ordinary scan fallback.
                let indexes = store.get_all_indexes();
                let index_infos: Vec<(Vec<usize>, std::sync::Arc<dyn Index>)> = indexes
                    .iter()
                    .filter(|idx| {
                        idx.index_type() != radixdb_core::IndexType::PrimaryKey
                            && idx.partial_predicate().is_none()
                    })
                    .filter_map(|idx| {
                        let col_ids = idx.column_ids();
                        if col_ids.is_empty() {
                            return None;
                        }
                        let col_indices: Vec<usize> =
                            col_ids.iter().map(|&id| id as usize).collect();
                        if idx.index_type() != radixdb_core::IndexType::Hnsw {
                            return None;
                        }
                        Some((col_indices, std::sync::Arc::clone(idx)))
                    })
                    .collect();

                if index_infos.is_empty() {
                    continue;
                }

                let tombstones = mgr.tombstone_set_arc();
                // Use newest-first ordering so overlapping row_ids resolve to newest version.
                // Keep artifact-backed descriptor-backed volumes metadata-only: VolumeScanner reads
                // only the projected vector column blocks needed for HNSW backfill.
                let volumes = mgr.get_volumes_newest_first_lazy();

                // Seed per-index coverage from the graph itself. A graph loaded
                // from the matching snapshot already owns cold rows; hot WAL
                // replay may add more. Only IDs present in every HNSW index can
                // be skipped by the shared projected scanner.
                let hot_skip: rustc_hash::FxHashSet<i64> = store
                    .get_all_visible_row_ids(INVALID_TRANSACTION_ID + 1)
                    .into_iter()
                    .collect();
                let mut seen_by_index: Vec<rustc_hash::FxHashSet<i64>> = index_infos
                    .iter()
                    .map(|(_, index)| {
                        let mut covered: rustc_hash::FxHashSet<i64> = index
                            .hnsw_indexed_row_ids()
                            .unwrap_or_default()
                            .into_iter()
                            .collect();
                        covered.extend(hot_skip.iter().copied());
                        covered
                    })
                    .collect();
                let common_coverage = seen_by_index
                    .first()
                    .map(|first| {
                        first
                            .iter()
                            .copied()
                            .filter(|row_id| {
                                seen_by_index[1..]
                                    .iter()
                                    .all(|covered| covered.contains(row_id))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let hot_skip_arc = Arc::new(common_coverage);

                let mut projected_cols: Vec<usize> = index_infos
                    .iter()
                    .flat_map(|(cols, _)| cols.iter().copied())
                    .collect();
                projected_cols.sort_unstable();
                projected_cols.dedup();
                let projected_pos: FxHashMap<usize, usize> = projected_cols
                    .iter()
                    .enumerate()
                    .map(|(pos, &col_idx)| (col_idx, pos))
                    .collect();
                let schema = store.schema();

                // Stream directly from volumes into per-index batches.
                // Pre-allocate a reusable buffer to avoid per-row Vec allocations.
                let max_cols = index_infos
                    .iter()
                    .map(|(cols, _)| cols.len())
                    .max()
                    .unwrap_or(0);
                let mut batches: Vec<Vec<(i64, Vec<radixdb_core::Value>)>> =
                    (0..index_infos.len()).map(|_| Vec::new()).collect();
                let mut values_buf: Vec<radixdb_core::Value> = Vec::with_capacity(max_cols);

                const HNSW_FLUSH_THRESHOLD: usize = 8192;

                for (seg_id, cs) in volumes.iter() {
                    let volume = Arc::clone(&cs.volume);
                    let mapping = mgr.get_cold_segment_mapping(cs, &schema);
                    let mut scanner = crate::volume::scanner::VolumeScanner::new(
                        volume,
                        projected_cols.clone(),
                        None,
                    );
                    scanner.set_column_mapping(mapping)?;
                    scanner.set_visibility_bitmap(cs.visible.clone());
                    scanner.set_skip_sets(Arc::clone(&tombstones), Arc::clone(&hot_skip_arc));

                    while scanner.next() {
                        let row_id = scanner.current_row_id()?;
                        let row = scanner.row();
                        for (batch_idx, (col_indices, _)) in index_infos.iter().enumerate() {
                            if !seen_by_index[batch_idx].insert(row_id) {
                                continue;
                            }
                            values_buf.clear();
                            let mut has_null = false;
                            for &ci in col_indices {
                                let v = projected_pos
                                    .get(&ci)
                                    .and_then(|&pos| row.get(pos))
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        radixdb_core::Value::Null(radixdb_core::DataType::Null)
                                    });
                                if v.is_null() {
                                    has_null = true;
                                    break;
                                }
                                values_buf.push(v);
                            }
                            if !has_null {
                                // Move ownership instead of cloning to avoid per-row allocation
                                let owned = std::mem::replace(
                                    &mut values_buf,
                                    Vec::with_capacity(max_cols),
                                );
                                batches[batch_idx].push((row_id, owned));
                            }
                        }
                    }
                    if let Some(err) = scanner.err() {
                        return Err(Error::internal(format!(
                            "cold index segment scan failed for {} seg={}: {}",
                            table_name, seg_id, err
                        )));
                    }
                    if let Err(err) = scanner.close() {
                        return Err(Error::internal(format!(
                            "cold index segment scanner close failed for {} seg={}: {}",
                            table_name, seg_id, err
                        )));
                    }

                    // Flush large batches to limit peak memory
                    for (idx, (_, index)) in index_infos.iter().enumerate() {
                        if batches[idx].len() >= HNSW_FLUSH_THRESHOLD {
                            let entry_refs: Vec<(i64, &[radixdb_core::Value])> = batches[idx]
                                .iter()
                                .map(|(row_id, values)| (*row_id, values.as_slice()))
                                .collect();
                            if let Err(e) = index.add_batch_slice(&entry_refs) {
                                return Err(Error::internal(format!(
                                    "cold index population failed for {}: {}",
                                    table_name, e
                                )));
                            }
                            batches[idx].clear();
                        }
                    }
                }

                // Flush remaining entries
                for (idx, (_, index)) in index_infos.iter().enumerate() {
                    if !batches[idx].is_empty() {
                        let entry_refs: Vec<(i64, &[radixdb_core::Value])> = batches[idx]
                            .iter()
                            .map(|(row_id, values)| (*row_id, values.as_slice()))
                            .collect();
                        if let Err(e) = index.add_batch_slice(&entry_refs) {
                            return Err(Error::internal(format!(
                                "cold index population failed for {}: {}",
                                table_name, e
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Populate pre-computed `default_value` from `default_expr` on all schema columns.
    ///
    /// Neither snapshot nor WAL serialization persists `default_value` (only the expression
    /// string `default_expr` is stored). After recovery, `normalize_row_to_schema` uses
    /// `default_value` for schema-evolved rows (ALTER TABLE ADD COLUMN). Without this,
    /// old rows would get NULL instead of the configured DEFAULT.
    pub(super) fn populate_schema_defaults(&self) {
        let stores = self.version_stores.read().unwrap();
        for store in stores.values() {
            let mut schema_guard = store.schema_mut();
            let needs_update = schema_guard
                .columns
                .iter()
                .any(|col| col.default_value.is_none() && col.default_expr.is_some());
            if !needs_update {
                continue;
            }
            let schema = CompactArc::make_mut(&mut *schema_guard);
            for col in &mut schema.columns {
                if col.default_value.is_none() {
                    if let Some(ref expr) = col.default_expr {
                        // Backward-compat fallback for WAL/snapshots written before
                        // default_value persistence was added. Only handles simple
                        // literals (integers, floats, booleans, strings, NULL).
                        // Non-deterministic expressions (NOW(), CURRENT_TIMESTAMP)
                        // cannot be recovered here — they require the new format.
                        col.default_value = try_parse_default_literal(expr, col.data_type);
                    }
                }
            }
        }
        drop(stores);

        // Sync engine schema cache from version stores
        let stores = self.version_stores.read().unwrap();
        let mut schemas = self.schemas.write().unwrap();
        for (name, store) in stores.iter() {
            schemas.insert(name.clone(), store.schema().clone());
        }
    }

    pub(super) fn apply_wal_entry_resolved(
        &self,
        entry: crate::mvcc::wal_manager::WALEntry,
        table_name: Option<String>,
        pending_tombstone_tables: &mut FxHashMap<i64, SmallVec<[SmartString; 4]>>,
    ) -> Result<()> {
        use crate::mvcc::persistence::deserialize_row_version;
        use crate::mvcc::wal_manager::WALOperationType;

        let require_table_name = || {
            table_name.clone().ok_or_else(|| {
                Error::internal(format!(
                    "WAL {:?} at LSN {} has no live catalog table identity",
                    entry.operation, entry.lsn
                ))
            })
        };

        match entry.operation {
            WALOperationType::Insert | WALOperationType::Update => {
                // Deserialize row version and apply to version store.
                let row_version = deserialize_row_version(&entry.data).map_err(|error| {
                    Error::internal(format!(
                        "failed to decode {:?} WAL entry at LSN {}: {error}",
                        entry.operation, entry.lsn
                    ))
                })?;
                if row_version.txn_id != entry.txn_id {
                    return Err(Error::internal(format!(
                        "WAL transaction identity mismatch at LSN {}: envelope {} != payload {}",
                        entry.lsn, entry.txn_id, row_version.txn_id
                    )));
                }
                let table_name = require_table_name()?;
                let mgr = self.get_or_create_segment_manager(&table_name);
                let in_volume = mgr.is_row_id_in_volume(entry.row_id);

                if in_volume {
                    // Row exists in a cold volume. Two cases:
                    // 1. Sealed INSERT: original data, already in volume → skip
                    // 2. Post-seal UPDATE: new data supersedes cold → apply + tombstone
                    //
                    // Distinguish by checking if the row_id is already tombstoned.
                    // If tombstoned, a previous WAL entry already marked it as
                    // superseded (UPDATE or DELETE), so this entry is a later
                    // version that should be applied. If not tombstoned, this is
                    // the first (original sealed) INSERT → skip.
                    //
                    // ORDERING INVARIANT: record_transaction_dml writes tombstone
                    // DELETE entries BEFORE the corresponding INSERT entries.
                    // This guarantees `already_tombstoned` is true for post-seal
                    // UPDATEs (which are recorded as Insert in the WAL).
                    // The `WALOperationType::Update` arm is a safety net for
                    // any future code path that records with the Update op type.
                    let already_tombstoned = mgr.is_tombstoned(entry.row_id)
                        || mgr.is_pending_tombstone(entry.txn_id, entry.row_id);

                    if already_tombstoned || entry.operation == WALOperationType::Update {
                        // Post-seal change: apply to hot. The hot version
                        // shadows the cold version via skip set (hot_row_ids
                        // in the cumulative skip set at scan time). No tombstone
                        // needed here — the hot version IS the dedup mechanism.
                        let store = self.get_version_store_for_recovery(&table_name)?;
                        store.apply_recovered_version(entry.row_id, row_version)?;
                    }
                    // else: sealed INSERT, volume has authoritative data → skip
                } else {
                    // Row not in any volume: standard hot insert
                    let store = self.get_version_store_for_recovery(&table_name)?;
                    store.apply_recovered_version(entry.row_id, row_version)?;
                }
            }
            WALOperationType::Delete => {
                // For deletes, mark the row as deleted in the hot store.
                let table_name = require_table_name()?;
                let store = self.get_version_store_for_recovery(&table_name)?;
                store.mark_deleted_at(entry.row_id, entry.txn_id, entry.timestamp)?;
                // If the deleted row_id lives in a cold segment, add a tombstone
                // so it is excluded from scans and point lookups.
                let mgr = self.get_or_create_segment_manager(&table_name);
                if mgr.is_row_id_in_volume(entry.row_id) {
                    // Data records precede the transaction's commit marker in
                    // WAL. Keep the tombstone private until that marker supplies
                    // the exact durable visibility sequence. Sequence zero was
                    // historically treated as "always visible", but it is not a
                    // legal persisted MVCC sequence and breaks later checkpoint.
                    mgr.add_pending_tombstone(entry.txn_id, entry.row_id);
                    let table_name = SmartString::from(table_name);
                    let tables = pending_tombstone_tables.entry(entry.txn_id).or_default();
                    if !tables.contains(&table_name) {
                        tables.push(table_name);
                    }
                }
            }
            WALOperationType::Commit => {
                // Mark transaction as committed in registry for visibility
                // Use the LSN as the commit sequence number
                let commit_seq = i64::try_from(entry.lsn).map_err(|_| {
                    Error::internal(format!(
                        "WAL commit LSN {} exceeds the MVCC sequence domain",
                        entry.lsn
                    ))
                })?;
                self.registry
                    .recover_committed_transaction(entry.txn_id, commit_seq)?;
                if let Some(tables) = pending_tombstone_tables.remove(&entry.txn_id) {
                    let managers = self.segment_managers.read().unwrap();
                    for table_name in tables {
                        let manager = managers.get(table_name.as_str()).ok_or_else(|| {
                            Error::internal(format!(
                                "WAL commit for transaction {} lost tombstone table '{}'",
                                entry.txn_id, table_name
                            ))
                        })?;
                        manager.commit_pending_tombstones(entry.txn_id, entry.lsn);
                    }
                }
            }
            WALOperationType::Rollback => {
                self.registry.recover_aborted_transaction(entry.txn_id)?;
                if let Some(tables) = pending_tombstone_tables.remove(&entry.txn_id) {
                    let managers = self.segment_managers.read().unwrap();
                    for table_name in tables {
                        if let Some(manager) = managers.get(table_name.as_str()) {
                            manager.rollback_pending_tombstones(entry.txn_id);
                        }
                    }
                }
            }
            WALOperationType::TruncateTable => {
                // Clear all data from the table
                // During WAL recovery there are no concurrent transactions,
                // so truncate_all() will always succeed.
                let table_name = require_table_name()?;
                if let Ok(store) = self.get_version_store_for_recovery(&table_name) {
                    store.truncate_all().map_err(|error| {
                        Error::internal(format!(
                            "failed to replay TRUNCATE TABLE '{}' at LSN {}: {error}",
                            table_name, entry.lsn
                        ))
                    })?;
                }
                // Publish an empty manifest before deleting segment files.
                // The table remains part of the committed generation, so
                // TRUNCATE must retain the table identity published by the
                // catalog generation, unlike DROP.
                {
                    let mgrs = self.segment_managers.read().unwrap();
                    if let Some(mgr) = mgrs.get(&table_name) {
                        mgr.truncate_persisted_segments()?;
                    }
                }
            }
            WALOperationType::CatalogMutation => {
                // Catalog records are consumed as an ordered committed
                // substream by the catalog recovery owner before DML replay.
                // They never mutate row state through this callback.
            }
        }

        Ok(())
    }
}
