impl Executor {
    /// Execute SET statement
    pub(crate) fn execute_set(
        &self,
        stmt: &SetStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let name = stmt.name.value.to_uppercase();

        match name.as_str() {
            "ISOLATION_LEVEL" | "ISOLATIONLEVEL" | "TRANSACTION_ISOLATION" => {
                // Extract the value as a string
                let level_str = match &stmt.value {
                    Expression::StringLiteral(lit) => lit.value.to_uppercase(),
                    Expression::Identifier(id) => id.value.to_uppercase(),
                    _ => {
                        return Err(Error::internal(
                            "SET isolation_level requires a string value (e.g., 'READ COMMITTED', 'SNAPSHOT')",
                        ));
                    }
                };

                let isolation = crate::dispatch::transaction::parse_isolation_level(
                    &level_str,
                )?;

                if self.active_transaction.lock().unwrap().is_some() {
                    return Err(Error::invalid_argument(
                        "transaction isolation is fixed at BEGIN; SET ISOLATION_LEVEL is only allowed outside a transaction",
                    ));
                }
                self.set_default_isolation_level(isolation);

                Ok(Box::new(ExecResult::empty()))
            }
            _ => Err(Error::invalid_argument(format!(
                "unknown SET variable '{}'; supported variables: ISOLATION_LEVEL, ISOLATIONLEVEL, TRANSACTION_ISOLATION",
                stmt.name.value
            ))),
        }
    }

    /// Execute PRAGMA statement
    pub(crate) fn execute_pragma(
        &self,
        stmt: &PragmaStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let pragma_name = stmt.name.value.to_uppercase();

        match pragma_name.as_str() {
            "ISOLATION_LEVEL" => {
                if stmt.value.is_some() {
                    return Err(Error::invalid_argument(
                        "PRAGMA ISOLATION_LEVEL does not accept values",
                    ));
                }
                let level = match self.default_isolation_level() {
                    radixdb_core::IsolationLevel::ReadCommitted => "READ COMMITTED",
                    radixdb_core::IsolationLevel::SnapshotIsolation => "SNAPSHOT",
                };
                let columns = vec!["isolation_level".to_string()];
                let mut rows = RowVec::with_capacity(1);
                rows.push((0, Row::from_values(vec![Value::text(level)])));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "SNAPSHOT" => {
                // PRAGMA SNAPSHOT: Create a full backup snapshot of all tables.
                // Writes .bin files to snapshots/ directory. keep_snapshots limits retention.
                if stmt.value.is_some() {
                    return Err(Error::internal("PRAGMA SNAPSHOT does not accept values"));
                }

                {
                    let active_tx = self.active_transaction.lock().unwrap();
                    if active_tx.is_some() {
                        return Err(Error::internal(
                            "PRAGMA SNAPSHOT cannot run inside a transaction. \
                             Commit or rollback first.",
                        ));
                    }
                }

                let cancellation = ctx.cancellation_handle();
                let snapshot = self
                    .engine
                    .create_snapshot_cancellable(&|| cancellation.is_cancelled())?;

                let columns = vec![
                    "snapshot_id".to_string(),
                    "database_id".to_string(),
                    "physical_format".to_string(),
                ];
                let mut rows = RowVec::with_capacity(1);
                rows.push((0, Row::from_values(vec![
                    Value::text(snapshot.snapshot_id),
                    Value::text(snapshot.database_id),
                    Value::text(format!(
                        "{}.{}",
                        snapshot.physical_format_major, snapshot.physical_format_minor
                    )),
                ])));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "CHECKPOINT" => {
                // PRAGMA CHECKPOINT: seal hot rows, publish manifests and the
                // WAL retention boundary. Compaction is requested as separate
                // bounded maintenance and is not part of this command's success.
                if stmt.value.is_some() {
                    return Err(Error::internal("PRAGMA CHECKPOINT does not accept values"));
                }

                {
                    let active_tx = self.active_transaction.lock().unwrap();
                    if active_tx.is_some() {
                        return Err(Error::internal(
                            "PRAGMA CHECKPOINT cannot run inside a transaction. \
                             Commit or rollback first.",
                        ));
                    }
                }

                self.engine.force_checkpoint_cycle()?;

                let columns = vec!["result".to_string()];
                let mut rows = RowVec::with_capacity(1);
                rows.push((
                    0,
                    Row::from_values(vec![Value::text("Checkpoint completed successfully")]),
                ));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "RESTORE" => {
                // PRAGMA RESTORE: Restore database from latest backup snapshot.
                // PRAGMA RESTORE = '<snapshot-id>': Restore one exact snapshot.
                {
                    let active_tx = self.active_transaction.lock().unwrap();
                    if active_tx.is_some() {
                        return Err(Error::internal(
                            "PRAGMA RESTORE cannot run inside a transaction. \
                             Commit or rollback first.",
                        ));
                    }
                }

                let snapshot_id = if let Some(ref value) = stmt.value {
                    Some(self.extract_pragma_string_value(value)?)
                } else {
                    None
                };

                let result_msg = self.engine.restore_snapshot(snapshot_id.as_deref())?;

                // Clear all query caches since all data has changed
                self.clear_semantic_cache();
                self.query_cache.clear();
                crate::context::clear_scalar_subquery_cache();
                crate::context::clear_in_subquery_cache();
                crate::context::clear_semi_join_cache();

                let columns = vec!["result".to_string()];
                let mut rows = RowVec::with_capacity(1);
                rows.push((0, Row::from_values(vec![Value::text(&result_msg)])));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "DEDUP_SEGMENTS" => {
                let columns = vec!["message".to_string()];
                let mut rows = RowVec::with_capacity(1);
                rows.push((
                    0,
                    Row::from_values(vec![Value::text(
                        "Dedup segments is no longer needed (handled automatically)",
                    )]),
                ));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "SNAPSHOT_INTERVAL" | "CHECKPOINT_INTERVAL" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    // Set mode: PRAGMA checkpoint_interval = 60
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value < 0 {
                        return Err(Error::internal("checkpoint_interval must be non-negative"));
                    }
                    let installed = u32::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "checkpoint_interval is out of range for u32: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.checkpoint_interval = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::from(installed))]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    // Read mode: PRAGMA checkpoint_interval
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.checkpoint_interval as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "COMPACT_THRESHOLD" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value < 0 {
                        return Err(Error::internal("compact_threshold must be non-negative"));
                    }
                    let installed = u32::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "compact_threshold is out of range for u32: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.compact_threshold = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::from(installed))]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.compact_threshold as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "TARGET_VOLUME_ROWS" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value < 65536 {
                        return Err(Error::internal("target_volume_rows must be at least 65536"));
                    }
                    let installed = usize::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "target_volume_rows is out of range for usize: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.target_volume_rows = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::try_from(installed).map_err(
                            |_| {
                                Error::invalid_argument(
                                    "installed target_volume_rows cannot be represented as i64",
                                )
                            },
                        )?)]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.target_volume_rows as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "VOLUME_CACHE_BYTES" | "VOLUME_CACHE" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value < 0 {
                        return Err(Error::internal("volume_cache_bytes must be non-negative"));
                    }
                    let installed = usize::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "volume_cache_bytes is out of range for usize: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.volume_cache_bytes = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::try_from(installed).map_err(
                            |_| {
                                Error::invalid_argument(
                                    "installed volume_cache_bytes cannot be represented as i64",
                                )
                            },
                        )?)]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.volume_cache_bytes as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "READ_QUEUE_DEPTH" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value <= 0 {
                        return Err(Error::internal("read_queue_depth must be positive"));
                    }
                    let installed = usize::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "read_queue_depth is out of range for usize: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.read_queue_depth = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::try_from(installed).map_err(
                            |_| {
                                Error::invalid_argument(
                                    "installed read_queue_depth cannot be represented as i64",
                                )
                            },
                        )?)]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.read_queue_depth as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "SYNC_MODE" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if stmt.value.is_some() {
                    return Err(Error::NotSupported(
                        "sync_mode cannot be changed at runtime. Set it in the connection string: file:///path?sync_mode=none|normal|full".to_string(),
                    ));
                }
                let mut rows = RowVec::with_capacity(1);
                rows.push((
                    0,
                    Row::from_values(vec![Value::Integer(config.persistence.sync_mode as i64)]),
                ));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "WAL_FLUSH_TRIGGER" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if stmt.value.is_some() {
                    return Err(Error::NotSupported(
                        "wal_flush_trigger cannot be changed at runtime. Set it in the connection string: file:///path?wal_flush_trigger=N".to_string(),
                    ));
                }
                let mut rows = RowVec::with_capacity(1);
                rows.push((
                    0,
                    Row::from_values(vec![Value::Integer(
                        config.persistence.wal_flush_trigger as i64,
                    )]),
                ));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "KEEP_SNAPSHOTS" => {
                let config = self.engine.config();
                let columns: Vec<String> = vec![pragma_name.to_lowercase().into()];

                if let Some(ref value) = stmt.value {
                    let new_value = self.extract_pragma_int_value(value)?;
                    if new_value < 0 {
                        return Err(Error::internal("keep_snapshots must be non-negative"));
                    }
                    let installed = u32::try_from(new_value).map_err(|_| {
                        Error::invalid_argument(format!(
                            "keep_snapshots is out of range for u32: {new_value}"
                        ))
                    })?;
                    let mut new_config = config.clone();
                    new_config.persistence.keep_snapshots = installed;
                    self.engine.update_engine_config(new_config)?;
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(i64::from(installed))]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                } else {
                    let mut rows = RowVec::with_capacity(1);
                    rows.push((
                        0,
                        Row::from_values(vec![Value::Integer(
                            config.persistence.keep_snapshots as i64,
                        )]),
                    ));
                    Ok(Box::new(ExecutorResult::new(columns, rows)))
                }
            }
            "PAGE_CACHE_STATUS" => {
                if stmt.value.is_some() {
                    return Err(Error::internal(
                        "PRAGMA PAGE_CACHE_STATUS does not accept values",
                    ));
                }
                self.page_cache_warmup_result()
            }
            "PAGE_CACHE_WARMUP" => {
                if stmt.value.is_some() {
                    return Err(Error::internal(
                        "PRAGMA PAGE_CACHE_WARMUP does not accept values",
                    ));
                }
                if self.active_transaction.lock().unwrap().is_some() {
                    return Err(Error::internal(
                        "PRAGMA PAGE_CACHE_WARMUP cannot run inside a transaction",
                    ));
                }
                self.engine.request_page_cache_warmup();
                self.page_cache_warmup_result()
            }
            "PAGE_CACHE_WARMUP_WAIT" => {
                if self.active_transaction.lock().unwrap().is_some() {
                    return Err(Error::internal(
                        "PRAGMA PAGE_CACHE_WARMUP_WAIT cannot run inside a transaction",
                    ));
                }
                let timeout_millis = match stmt.value.as_ref() {
                    Some(value) => self.extract_pragma_int_value(value)?,
                    None => 300_000,
                };
                if !(0..=86_400_000).contains(&timeout_millis) {
                    return Err(Error::invalid_argument(
                        "page-cache warmup timeout must be in 0..=86400000 milliseconds",
                    ));
                }
                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(timeout_millis as u64);
                loop {
                    let snapshot = self.engine.page_cache_warmup_snapshot().ok_or_else(|| {
                        Error::internal("page-cache warmup status is busy; retry")
                    })?;
                    match snapshot.state.as_str() {
                        "complete" | "disabled" => break,
                        "failed" | "stopped" => {
                            return Err(Error::internal(format!(
                                "page-cache warmup {}: {}",
                                snapshot.state, snapshot.last_error
                            )))
                        }
                        _ => {}
                    }
                    if ctx.cancellation_handle().is_cancelled() {
                        return Err(Error::internal("page-cache warmup wait cancelled"));
                    }
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(Error::internal(format!(
                            "page-cache warmup timed out after {timeout_millis} ms"
                        )));
                    }
                    self.engine.wait_for_page_cache_warmup(
                        (deadline - now).min(std::time::Duration::from_millis(100)),
                    );
                }
                self.page_cache_warmup_result()
            }
            "VOLUME_STATS" => {
                if stmt.value.is_some() {
                    return Err(Error::internal(
                        "PRAGMA VOLUME_STATS does not accept values",
                    ));
                }

                let columns = vec![
                    "table_name".to_string(),
                    "segment_id".to_string(),
                    "tier".to_string(),
                    "row_count".to_string(),
                    "memory_bytes".to_string(),
                    "metadata_bytes".to_string(),
                    "row_id_bytes".to_string(),
                    "exact_index_bytes".to_string(),
                    "ordered_index_bytes".to_string(),
                    "descriptor_bytes".to_string(),
                    "column_payload_bytes".to_string(),
                    "idle_cycles".to_string(),
                    "tombstones".to_string(),
                ];

                let stats = self.engine.volume_stats();
                let mut rows = RowVec::with_capacity(stats.len());
                for (
                    i,
                    (
                        table,
                        seg_id,
                        tier,
                        row_count,
                        mem,
                        metadata,
                        row_ids,
                        exact_indices,
                        ordered_indices,
                        descriptor,
                        column_payload,
                        idle,
                        ts,
                    ),
                ) in stats.into_iter().enumerate()
                {
                    rows.push((
                        i as i64,
                        Row::from_values(vec![
                            Value::text(&table),
                            Value::Integer(seg_id as i64),
                            Value::text(tier),
                            Value::Integer(row_count as i64),
                            Value::Integer(mem as i64),
                            Value::Integer(metadata as i64),
                            Value::Integer(row_ids as i64),
                            Value::Integer(exact_indices as i64),
                            Value::Integer(ordered_indices as i64),
                            Value::Integer(descriptor as i64),
                            Value::Integer(column_payload as i64),
                            Value::Integer(idle as i64),
                            Value::Integer(ts as i64),
                        ]),
                    ));
                }
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "RUNTIME_STATS" => {
                if stmt.value.is_some() {
                    return Err(Error::internal(
                        "PRAGMA RUNTIME_STATS does not accept values",
                    ));
                }

                // One bounded JSON row is the versioned public contract. The
                // engine snapshot uses atomics, ArcSwap and try-lock reads only;
                // this path never opens a volume payload or builds an index.
                let snapshot = self.engine.runtime_stats_snapshot();
                let payload = serde_json::to_string(&snapshot).map_err(|error| {
                    Error::internal(format!("cannot encode runtime stats: {error}"))
                })?;
                let columns = vec!["runtime_stats_v1".to_string()];
                let mut rows = RowVec::with_capacity(1);
                rows.push((0, Row::from_values(vec![Value::text(&payload)])));
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "ACL_PROVENANCE" => {
                if stmt.value.is_some() {
                    return Err(Error::invalid_argument(
                        "PRAGMA ACL_PROVENANCE does not accept values",
                    ));
                }
                if ctx.principal_id() != radixdb_catalog::ObjectId::BOOTSTRAP_OWNER
                    || ctx.effective_principal_id()
                        != radixdb_catalog::ObjectId::BOOTSTRAP_OWNER
                {
                    return Err(Error::authorization_denied(
                        "ACL provenance inspection requires bootstrap authority",
                    ));
                }

                let (catalog, _) = crate::procedural::transaction_visible_catalog(self)?;
                let columns = vec![
                    "acl_id".to_string(),
                    "grantor_id".to_string(),
                    "grantor_name".to_string(),
                    "grantee_id".to_string(),
                    "grantee_name".to_string(),
                    "target_id".to_string(),
                    "target_name".to_string(),
                    "target_kind".to_string(),
                    "grant_kind".to_string(),
                    "privileges".to_string(),
                    "grant_options".to_string(),
                    "column_privileges".to_string(),
                    "column_grant_options".to_string(),
                    "admin_option".to_string(),
                ];
                let mut rows = RowVec::new();
                for acl in catalog.objects_of_kind(radixdb_catalog::ObjectKind::AclEntry) {
                    let payload = match acl.payload() {
                        radixdb_catalog::CatalogPayload::AclEntry(payload) => payload,
                        _ => unreachable!("catalog kind/payload invariant"),
                    };
                    let grantor_id = payload.grantor_principal_id();
                    let grantor = catalog.object(grantor_id).ok_or_else(|| {
                        Error::internal(format!(
                            "ACL {} references missing grantor {}",
                            acl.id(), grantor_id
                        ))
                    })?;
                    let grantee = acl_provenance_endpoint(
                        catalog.as_ref(),
                        acl.id(),
                        radixdb_catalog::EdgeKind::GrantedTo,
                    )?;
                    let target = acl_provenance_endpoint(
                        catalog.as_ref(),
                        acl.id(),
                        radixdb_catalog::EdgeKind::GrantsOn,
                    )?;
                    let (
                        grant_kind,
                        privileges,
                        grant_options,
                        column_privileges,
                        column_grant_options,
                        admin_option,
                    ) = match payload {
                        radixdb_catalog::AclEntryPayload::ObjectPrivileges {
                            privileges,
                            grant_option,
                            columns,
                            column_grant_options,
                            ..
                        } => (
                            "object_privileges",
                            acl_privilege_names(*privileges),
                            acl_privilege_names(*grant_option),
                            acl_column_privileges(columns),
                            acl_column_privileges(column_grant_options),
                            false,
                        ),
                        radixdb_catalog::AclEntryPayload::RoleMembership {
                            admin_option, ..
                        } => (
                            "role_membership",
                            String::new(),
                            String::new(),
                            String::new(),
                            String::new(),
                            *admin_option,
                        ),
                    };
                    rows.push((
                        rows.len() as i64,
                        Row::from_values(vec![
                            Value::text(acl.id().to_string()),
                            Value::text(grantor_id.to_string()),
                            Value::text(grantor.name().display().as_str()),
                            Value::text(grantee.id().to_string()),
                            Value::text(grantee.name().display().as_str()),
                            Value::text(target.id().to_string()),
                            Value::text(target.name().display().as_str()),
                            Value::text(target.kind().name()),
                            Value::text(grant_kind),
                            Value::text(&privileges),
                            Value::text(&grant_options),
                            Value::text(&column_privileges),
                            Value::text(&column_grant_options),
                            Value::Boolean(admin_option),
                        ]),
                    ));
                }
                Ok(Box::new(ExecutorResult::new(columns, rows)))
            }
            "VACUUM" => {
                if stmt.value.is_some() {
                    return Err(Error::internal("PRAGMA VACUUM does not accept values"));
                }
                // Delegate to the unified VACUUM implementation (no table filter)
                let vacuum_stmt = radixdb_sql::ast::VacuumStatement {
                    token: stmt.token.clone(),
                    table_name: None,
                };
                self.execute_vacuum(&vacuum_stmt, ctx)
            }
            _ => Err(Error::NotSupported(format!(
                "Unknown PRAGMA `{}`. Supported PRAGMA commands: \
                     SNAPSHOT, CHECKPOINT, RESTORE, DEDUP_SEGMENTS, \
                     SNAPSHOT_INTERVAL, CHECKPOINT_INTERVAL, COMPACT_THRESHOLD, \
                     TARGET_VOLUME_ROWS, VOLUME_CACHE_BYTES, VOLUME_CACHE, \
                     READ_QUEUE_DEPTH, SYNC_MODE, PAGE_CACHE_STATUS, \
                     PAGE_CACHE_WARMUP, PAGE_CACHE_WARMUP_WAIT, \
                     WAL_FLUSH_TRIGGER, KEEP_SNAPSHOTS, VOLUME_STATS, \
                     RUNTIME_STATS, ACL_PROVENANCE, VACUUM",
                stmt.name.value
            ))),
        }
    }

    fn page_cache_warmup_result(&self) -> Result<Box<dyn QueryResult>> {
        let snapshot = self
            .engine
            .page_cache_warmup_snapshot()
            .ok_or_else(|| Error::internal("page-cache warmup status is busy; retry"))?;
        let payload = serde_json::to_string(&snapshot).map_err(|error| {
            Error::internal(format!("cannot encode page-cache status: {error}"))
        })?;
        let columns = vec!["page_cache_warmup_v1".to_string()];
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, Row::from_values(vec![Value::text(&payload)])));
        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute VACUUM statement — manual cleanup of deleted rows and index compaction
    ///
    /// VACUUM aggressively reclaims storage by removing deleted rows, pruning old
    /// version chains, and cleaning up stale transaction metadata. Unlike background
    /// cleanup (which uses a 5-minute retention), VACUUM uses zero retention so all
    /// historical versions not needed by active transactions are removed.
    ///
    /// Safety: `cleanup_deleted_rows` and `cleanup_old_previous_versions_with_retention`
    /// check active transaction visibility before removing any version, so concurrent
    /// readers are never disrupted. However, AS OF TIMESTAMP queries that reference
    /// timestamps before the VACUUM will no longer work — this is intentional.
    pub(crate) fn execute_vacuum(
        &self,
        stmt: &VacuumStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // VACUUM must not run inside an explicit transaction
        let active_tx = self.active_transaction.lock().unwrap();
        if active_tx.is_some() {
            return Err(Error::internal(
                "VACUUM cannot be executed within an active transaction",
            ));
        }
        drop(active_tx);

        let retention = std::time::Duration::ZERO;
        let table_name = stmt.table_name.as_ref().map(|id| id.value_lower.as_str());

        let cleaned = self.engine.vacuum(table_name, retention)?;

        let columns = vec![
            "deleted_rows_cleaned".to_string(),
            "old_versions_cleaned".to_string(),
            "transactions_cleaned".to_string(),
        ];
        let mut rows = RowVec::with_capacity(1);
        rows.push((
            0,
            Row::from_values(vec![
                Value::Integer(cleaned.0 as i64),
                Value::Integer(cleaned.1 as i64),
                Value::Integer(cleaned.2 as i64),
            ]),
        ));
        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Extract integer value from PRAGMA value expression
    fn extract_pragma_int_value(&self, value: &radixdb_sql::Expression) -> Result<i64> {
        match value {
            radixdb_sql::Expression::IntegerLiteral(lit) => Ok(lit.value),
            _ => Err(Error::invalid_argument(
                "PRAGMA value must be an integer literal",
            )),
        }
    }

    fn extract_pragma_string_value(&self, value: &radixdb_sql::Expression) -> Result<String> {
        match value {
            radixdb_sql::Expression::StringLiteral(lit) => Ok(lit.value.to_string()),
            _ => Err(Error::internal(
                "PRAGMA value must be a quoted string, e.g. 'YYYYMMDD-HHMMSS.fff'",
            )),
        }
    }

    /// Execute an expression statement (SELECT 1+1)
    pub(crate) fn execute_expression_stmt(
        &self,
        stmt: &ExpressionStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let value = ExpressionEval::compile(&stmt.expression, &[])?
            .with_context(ctx)
            .eval_slice(&Row::new())?;
        let columns = vec!["result".to_string()];
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, Row::from_values(vec![value])));

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute a temporal query (AS OF TRANSACTION or AS OF TIMESTAMP)
    ///
    /// This enables time-travel queries to see historical data.
    fn execute_temporal_query(
        &self,
        table_name: &str,
        as_of: &AsOfClause,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        tx: &dyn radixdb_storage::traits::Transaction,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // classification is passed from caller to avoid redundant cache lookups

        // Get table schema
        let table = tx.get_table(table_name)?;
        let schema = table.schema().clone();
        let all_columns: Vec<String> = schema.column_names_owned().to_vec();

        // Parse temporal value
        let temporal_value = self.parse_temporal_value(as_of, ctx)?;

        // Temporal rows must retain the source schema until WHERE, window,
        // aggregation, and final SELECT projection have all run. Fetching the
        // SELECT subset here would make the later projection apply source
        // column indices to an already-projected row.
        let columns_to_fetch = all_columns.clone();

        // Build storage expression if WHERE clause exists
        // Try to push down to storage, fall back to memory filter for complex expressions
        let (storage_expr, needs_memory_filter) =
            access_predicate::prepare_bound_predicate(stmt.where_clause.as_deref(), &schema, ctx);

        // Execute temporal query
        let result = tx.select_as_of(
            table_name,
            &columns_to_fetch,
            storage_expr.as_deref(),
            &as_of.as_of_type,
            temporal_value,
            Some(&all_columns),
        )?;

        // Get output columns from result
        let _output_columns = result.columns().to_vec();

        // Collect rows for further processing (projection, aggregation, etc.)
        // Use synthetic row IDs since temporal queries don't preserve original IDs
        let mut rows = RowVec::with_capacity(64);
        let mut result_iter = result;
        let mut row_id = 0i64;
        while result_iter.next() {
            rows.push((row_id, result_iter.take_row()));
            row_id += 1;
        }

        // Apply in-memory WHERE filter if storage expression couldn't handle it fully
        if needs_memory_filter {
            if let Some(where_expr) = &stmt.where_clause {
                let where_filter = RowFilter::new(where_expr, &all_columns)?.with_context(ctx);

                where_filter.retain_checked(&mut rows)?;
            }
        }

        // Check for window functions
        if classification.has_window_functions {
            let result =
                self.execute_select_with_window_functions(stmt, ctx, &rows, &all_columns)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Check for aggregation
        if classification.has_aggregation {
            let result = self.execute_select_with_aggregation(stmt, ctx, rows, &all_columns)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Project rows
        let (projected_rows, final_columns) =
            self.project_rows_for_select(stmt, rows, &all_columns, ctx)?;

        let final_columns_arc = CompactArc::new(final_columns);
        Ok((
            Box::new(ExecutorResult::with_arc_columns(
                CompactArc::clone(&final_columns_arc),
                projected_rows,
            )),
            final_columns_arc,
            false,
            None,
        ))
    }

    /// Parse temporal value from AS OF clause
    fn parse_temporal_value(&self, as_of: &AsOfClause, ctx: &ExecutionContext) -> Result<i64> {
        let value = ExpressionEval::compile(&as_of.value, &[])?
            .with_context(ctx)
            .eval_slice(&Row::new())?;

        match as_of.as_of_type.to_uppercase().as_str() {
            "TRANSACTION" => {
                // Expect integer transaction ID
                match value {
                    Value::Integer(txn_id) => Ok(txn_id),
                    _ => Err(Error::invalid_argument(
                        "AS OF TRANSACTION requires integer value",
                    )),
                }
            }
            "TIMESTAMP" => {
                // Expect timestamp string or timestamp value
                match value {
                    Value::Timestamp(ts) => {
                        // Convert to nanoseconds since epoch
                        ts.timestamp_nanos_opt().ok_or_else(|| {
                            Error::invalid_argument(
                                "AS OF TIMESTAMP is outside the supported nanosecond range",
                            )
                        })
                    }
                    Value::Text(s) => {
                        // Parse timestamp string
                        use chrono::{DateTime, NaiveDateTime, Utc};
                        let ts = if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                            dt.with_timezone(&Utc)
                        } else if let Ok(ndt) =
                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        {
                            DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc)
                        } else if let Ok(ndt) =
                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                        {
                            DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc)
                        } else {
                            return Err(Error::invalid_argument(format!(
                                "Invalid timestamp format: {}",
                                s
                            )));
                        };
                        ts.timestamp_nanos_opt().ok_or_else(|| {
                            Error::invalid_argument(
                                "AS OF TIMESTAMP is outside the supported nanosecond range",
                            )
                        })
                    }
                    Value::Integer(i) => {
                        // Interpret as nanoseconds since epoch
                        Ok(i)
                    }
                    _ => Err(Error::invalid_argument(
                        "AS OF TIMESTAMP requires timestamp or string value",
                    )),
                }
            }
            _ => Err(Error::invalid_argument(format!(
                "Unsupported AS OF type: {}",
                as_of.as_of_type
            ))),
        }
    }

    /// Project rows for SELECT (helper for temporal queries)
    fn project_rows_for_select(
        &self,
        stmt: &SelectStatement,
        rows: RowVec,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<(RowVec, Vec<String>)> {
        // Check for SELECT * or t.*
        if stmt.columns.len() == 1
            && matches!(
                &stmt.columns[0],
                Expression::Star(_) | Expression::QualifiedStar(_)
            )
        {
            return Ok((rows, all_columns.to_vec()));
        }

        // Use existing project_rows method
        let projected = self.project_rows(&stmt.columns, rows, all_columns, ctx)?;
        // Note: This helper doesn't have table_alias available,
        // so we pass None. The prefix-based matching will still work for JOINs.
        let output_columns = self.get_output_column_names(&stmt.columns, all_columns, None);

        Ok((projected, output_columns))
    }

    /// Build a map of column aliases to their underlying expressions from SELECT columns
    /// Build alias map, optionally excluding aliases that shadow real table columns.
    ///
    /// In SQL, WHERE is evaluated before SELECT, so WHERE references table columns,
    /// not SELECT aliases. When an alias name matches a real column name, the column
    /// must take priority in WHERE context. Pass `base_columns` to exclude such aliases.
    fn build_alias_map_excluding<'a>(
        columns: &'a [Expression],
        base_columns: Option<&[String]>,
    ) -> FxHashMap<String, &'a Expression> {
        access_predicate::build_alias_map_excluding(columns, base_columns)
    }

    fn substitute_aliases(
        expr: &Expression,
        alias_map: &FxHashMap<String, &Expression>,
    ) -> Expression {
        access_predicate::substitute_aliases(expr, alias_map)
    }

    fn get_expression_column_name(expr: &Expression) -> String {
        match expr {
            Expression::Identifier(id) => id.value.to_string(),
            Expression::QualifiedIdentifier(qid) => qid.name.value.to_string(),
            Expression::Aliased(aliased) => aliased.alias.value.to_string(),
            Expression::FunctionCall(func) => {
                // Use function name as column name
                func.function.to_string()
            }
            Expression::Infix(infix) => {
                // For arithmetic, use the operator as hint
                format!("expr_{}", infix.operator)
            }
            Expression::Prefix(prefix) => {
                format!("expr_{}", prefix.operator)
            }
            Expression::Case(_) => "case".to_string(),
            Expression::Cast(cast) => {
                format!("cast_{}", cast.type_name)
            }
            Expression::Star(_) => "*".to_string(),
            Expression::QualifiedStar(qs) => format!("{}.*", qs.qualifier),
            Expression::IntegerLiteral(lit) => format!("{}", lit.value),
            Expression::FloatLiteral(lit) => format!("{}", lit.value),
            Expression::StringLiteral(lit) => lit.value.to_string(),
            Expression::BooleanLiteral(lit) => format!("{}", lit.value),
            Expression::NullLiteral(_) => "NULL".to_string(),
            _ => "expr".to_string(),
        }
    }

    /// Check if semi-join reduction optimization can be applied and return parameters
    /// Returns Some((limit_value, left_key_col, right_key_col)) if applicable, None otherwise
    ///
    /// Conditions for semi-join reduction:
    /// 1. INNER JOIN or LEFT JOIN (not RIGHT or FULL)
    /// 2. GROUP BY references only left table columns
    /// 3. LIMIT is present with no ORDER BY (for correctness)
    /// 4. Single equality join condition (a.col = b.col)
    ///
    /// For INNER JOIN, we over-fetch left rows (2x limit) since some may not have matches.
    /// For LEFT JOIN, exact limit is used since all left rows produce output.
    fn get_semijoin_reduction_limit(
        &self,
        join_type: &str,
        stmt: &SelectStatement,
        left_alias: Option<&str>,
        join_condition: &Option<Box<Expression>>,
    ) -> Option<(usize, String, String)> {
        // Applies to INNER JOIN and LEFT JOIN (not RIGHT or FULL)
        let is_inner = join_type == "INNER";
        let is_left = join_type.contains("LEFT") && !join_type.contains("FULL");
        if !is_inner && !is_left {
            return None;
        }

        // Must have GROUP BY
        if stmt.group_by.columns.is_empty() {
            return None;
        }

        // Must have LIMIT with no ORDER BY (for correctness)
        // Order of grouped results without ORDER BY is undefined, so early termination is safe
        if stmt.limit.is_none() || !stmt.order_by.is_empty() {
            return None;
        }

        // Check that GROUP BY references only left table columns
        let left_alias = left_alias?;
        for col in &stmt.group_by.columns {
            if !self.column_references_table(col, left_alias) {
                return None;
            }
        }

        // Extract join key columns from simple equality condition: left.col = right.col
        let condition = join_condition.as_ref()?;
        let (left_key_col, right_key_col) = self.extract_simple_join_key(condition)?;

        // Verify left key references the left table
        let left_key_lower = left_key_col.to_lowercase();
        let left_alias_lower = left_alias.to_lowercase();
        let left_alias_len = left_alias_lower.len();
        // Inline prefix check: "alias." without format! allocation
        let left_matches = left_key_lower.len() > left_alias_len
            && left_key_lower.starts_with(&left_alias_lower)
            && left_key_lower.as_bytes()[left_alias_len] == b'.';
        if !left_matches {
            // Maybe it's right.col = left.col (swapped)
            let right_key_lower = right_key_col.to_lowercase();
            let right_matches = right_key_lower.len() > left_alias_len
                && right_key_lower.starts_with(&left_alias_lower)
                && right_key_lower.as_bytes()[left_alias_len] == b'.';
            if !right_matches {
                return None;
            }
            // Swap the keys
            let limit_value = stmt.limit.as_ref().and_then(|e| {
                ExpressionEval::compile(e, &[])
                    .ok()
                    .and_then(|mut eval| eval.eval_slice(&Row::new()).ok())
                    .and_then(|v| match v {
                        Value::Integer(n) if n > 0 => Some(n as usize),
                        _ => None,
                    })
            })?;
            // For INNER JOIN, over-fetch to account for rows without matches
            // Use 2x multiplier as a balance between accuracy and performance
            let adjusted_limit = if is_inner {
                limit_value * 2
            } else {
                limit_value
            };
            return Some((adjusted_limit, right_key_col, left_key_col));
        }

        // Get LIMIT value
        let limit_value = stmt.limit.as_ref().and_then(|e| {
            ExpressionEval::compile(e, &[])
                .ok()
                .and_then(|mut eval| eval.eval_slice(&Row::new()).ok())
                .and_then(|v| match v {
                    Value::Integer(n) if n > 0 => Some(n as usize),
                    _ => None,
                })
        })?;

        // For INNER JOIN, over-fetch to account for rows without matches
        // Use 2x multiplier as a balance between accuracy and performance
        let adjusted_limit = if is_inner {
            limit_value * 2
        } else {
            limit_value
        };

        Some((adjusted_limit, left_key_col, right_key_col))
    }

    /// Check if a column expression references a specific table
    fn column_references_table(&self, expr: &Expression, table_alias: &str) -> bool {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                qid.qualifier.value.eq_ignore_ascii_case(table_alias)
            }
            Expression::Identifier(_) => {
                // Unqualified column - could be from any table, assume it's from the expected table
                // This is a conservative assumption
                true
            }
            _ => false,
        }
    }

    /// Extract join key column names from a simple equality condition
    /// Returns Some((left_col_qualified, right_col_qualified)) for patterns like a.id = b.user_id
    fn extract_simple_join_key(&self, condition: &Expression) -> Option<(String, String)> {
        match condition {
            Expression::Infix(infix) if infix.op_type == InfixOperator::Equal => {
                let left_col = self.extract_qualified_column_name(&infix.left)?;
                let right_col = self.extract_qualified_column_name(&infix.right)?;
                Some((left_col, right_col))
            }
            Expression::Infix(infix) if infix.op_type == InfixOperator::And => {
                // For AND conditions, try to extract the first simple equality
                if let Some(result) = self.extract_simple_join_key(&infix.left) {
                    return Some(result);
                }
                self.extract_simple_join_key(&infix.right)
            }
            _ => None,
        }
    }

    /// Extract qualified column name from an identifier expression
    fn extract_qualified_column_name(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                Some(format!("{}.{}", qid.qualifier.value, qid.name.value))
            }
            Expression::Identifier(id) => Some(id.value.to_string()),
            _ => None,
        }
    }

    /// Check if Index Nested Loop Join can be used.
    ///
    /// Returns the physical point-lookup edge plus whether the complete equality
    /// ON set proves cardinality 0..1 through PK/full UNIQUE metadata.
    /// - Right side is a simple TableSource OR a simple passthrough SubquerySource
    /// - There's an equality join condition on a column that has an index on the right table
    /// - Join type is INNER or LEFT (not RIGHT or FULL)
    ///
    /// The outer_key_idx will be determined after materializing the outer side.
    #[allow(clippy::type_complexity)]
    pub(crate) fn check_index_nested_loop_opportunity(
        &self,
        right_expr: &Expression,
        join_condition: Option<&Expression>,
        join_type: &str,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
    ) -> Option<access_index::IndexNestedLoopOpportunity> {
        access_index::index_nested_loop_opportunity(
            self.engine.as_ref(),
            right_expr,
            join_condition,
            join_type,
            left_alias,
            right_alias,
        )
    }

    /// Build an IN filter expression: column_name IN (v1, v2, ..., vN)
    pub(crate) fn build_in_filter_expression(
        &self,
        column_name: &str,
        values: &[Value],
    ) -> Option<Expression> {
        if values.is_empty() {
            return None;
        }

        // Parse the column name to handle qualified names like "o.user_id"
        let col_expr = if let Some(dot_pos) = column_name.find('.') {
            let qualifier = &column_name[..dot_pos];
            let name = &column_name[dot_pos + 1..];
            Expression::QualifiedIdentifier(QualifiedIdentifier {
                token: Token::new(TokenType::Identifier, qualifier, Position::default()),
                qualifier: Box::new(Identifier::new(
                    Token::new(TokenType::Identifier, qualifier, Position::default()),
                    qualifier.to_string(),
                )),
                intermediate: None,
                name: Box::new(Identifier::new(
                    Token::new(TokenType::Identifier, name, Position::default()),
                    name.to_string(),
                )),
            })
        } else {
            Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, column_name, Position::default()),
                column_name.to_string(),
            ))
        };

        // Build list of value literals.
        // IntegerLiteral uses "0" token (Display uses self.value, not token.literal).
        // FloatLiteral must use f.to_string() (Display uses token.literal).
        let value_exprs: Vec<Expression> = values
            .iter()
            .map(|v| match v {
                Value::Integer(i) => Expression::IntegerLiteral(IntegerLiteral {
                    token: Token::new(TokenType::Integer, "0", Position::default()),
                    value: *i,
                }),
                Value::Float(f) => Expression::FloatLiteral(FloatLiteral {
                    token: Token::new(TokenType::Float, f.to_string(), Position::default()),
                    value: *f,
                }),
                Value::Text(s) => Expression::StringLiteral(StringLiteral {
                    token: Token::new(TokenType::String, s.as_str(), Position::default()),
                    value: s.as_str().into(),
                    type_hint: None,
                }),
                Value::Boolean(b) => Expression::BooleanLiteral(BooleanLiteral {
                    token: Token::new(
                        TokenType::Keyword,
                        if *b { "true" } else { "false" },
                        Position::default(),
                    ),
                    value: *b,
                }),
                _ => Expression::NullLiteral(NullLiteral {
                    token: Token::new(TokenType::Keyword, "NULL", Position::default()),
                }),
            })
            .collect();

        // Create IN expression
        Some(Expression::In(InExpression {
            token: Token::new(TokenType::Keyword, "IN", Position::default()),
            left: Box::new(col_expr),
            right: Box::new(Expression::ExpressionList(Box::new(ExpressionList {
                token: Token::new(TokenType::Punctuator, "(", Position::default()),
                expressions: value_exprs,
            }))),
            not: false,
        }))
    }

    /// Find column index by name (case-insensitive), supporting qualified names like "t.col"
    fn find_column_index_by_name(col_name: &str, columns: &[String]) -> Option<usize> {
        let col_lower = col_name.to_lowercase();
        columns.iter().position(|c| {
            let c_lower = c.to_lowercase();
            // Exact match
            if c_lower == col_lower {
                return true;
            }
            // Match qualified to qualified: "t.col" matches "t.col"
            // Match qualified to unqualified: "t.col" -> check if "col" matches
            if let Some(dot_pos) = col_lower.rfind('.') {
                let unqualified = &col_lower[dot_pos + 1..];
                if c_lower.contains('.') {
                    // Both qualified
                    c_lower == col_lower
                } else {
                    // col_name is qualified, c is unqualified
                    c_lower == unqualified
                }
            } else if let Some(c_dot) = c_lower.rfind('.') {
                // col_name is unqualified, c is qualified
                c_lower[c_dot + 1..] == *col_lower
            } else {
                false
            }
        })
    }

    /// Extract simple aggregations from SELECT columns
    /// Returns Vec of (function_name, column_name, optional_alias)
    fn extract_aggregations_simple(
        &self,
        stmt: &SelectStatement,
    ) -> Vec<(String, String, Option<String>)> {
        let mut result = Vec::new();

        for col in &stmt.columns {
            match col {
                Expression::FunctionCall(fc) => {
                    let func_upper = fc.function.to_uppercase();
                    if matches!(func_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        let col_name = if fc.arguments.is_empty() {
                            "*".to_string()
                        } else if let Some(Expression::Star(_)) = fc.arguments.first() {
                            "*".to_string()
                        } else if let Some(Expression::Identifier(id)) = fc.arguments.first() {
                            id.value_lower.to_string()
                        } else {
                            continue; // Skip complex expressions
                        };
                        result.push((func_upper.into(), col_name, None));
                    }
                }
                Expression::Aliased(aliased) => {
                    if let Expression::FunctionCall(fc) = aliased.expression.as_ref() {
                        let func_upper = fc.function.to_uppercase();
                        if matches!(func_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                            let col_name = if fc.arguments.is_empty() {
                                "*".to_string()
                            } else if let Some(Expression::Star(_)) = fc.arguments.first() {
                                "*".to_string()
                            } else if let Some(Expression::Identifier(id)) = fc.arguments.first() {
                                id.value_lower.to_string()
                            } else {
                                continue; // Skip complex expressions
                            };
                            result.push((
                                func_upper.into(),
                                col_name,
                                Some(aliased.alias.value.to_string()),
                            ));
                        }
                    }
                }
                _ => {}
            }
        }

        result
    }

    /// Streaming GROUP BY optimization using B-tree index
    ///
    /// For queries like `SELECT user_id, SUM(amount) FROM orders GROUP BY user_id`
    /// where user_id has a B-tree index, we can iterate through the index in sorted
    /// order and aggregate each group without using a hash map. This is similar to
    /// SQLite's sorted GROUP BY approach.
    ///
    /// Benefits:
    /// - O(1) memory per group instead of O(groups) hash map
    /// - Better cache locality (sequential access)
    /// - Avoids hash computation overhead
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn try_streaming_group_by(
        &self,
        stmt: &SelectStatement,
        table: &dyn radixdb_storage::traits::Table,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<Option<(Box<dyn QueryResult>, CompactArc<Vec<String>>)>> {
        use radixdb_core::IndexType;

        // Only single-column GROUP BY is supported for now
        if stmt.group_by.columns.len() != 1 {
            return Ok(None);
        }

        // Check if GROUP BY is a simple column reference
        let group_col_name: String = match &stmt.group_by.columns[0] {
            Expression::Identifier(id) => id.value_lower.to_string(),
            _ => return Ok(None),
        };

        // Check for B-tree or primary key index on GROUP BY column
        let btree_index = match table.get_index_on_column(&group_col_name) {
            Some(idx)
                if idx.index_type() == IndexType::BTree
                    || idx.index_type() == IndexType::PrimaryKey =>
            {
                idx
            }
            _ => return Ok(None),
        };

        // Extract aggregations from SELECT columns
        let aggregations = self.extract_aggregations_simple(stmt);
        if aggregations.is_empty() {
            return Ok(None);
        }

        // Check for simple aggregates (SUM, COUNT, AVG, MIN, MAX)
        #[derive(Clone, Copy)]
        enum StreamingAgg {
            Count,
            Sum(usize), // column index
            Avg(usize), // column index (computed as sum/count)
            Min(usize), // column index
            Max(usize), // column index
        }

        let mut simple_aggs: Vec<StreamingAgg> = Vec::with_capacity(aggregations.len());
        for (func_name, col_name, _alias) in &aggregations {
            match func_name.as_str() {
                "COUNT" => simple_aggs.push(StreamingAgg::Count),
                "SUM" | "AVG" | "MIN" | "MAX" => {
                    // Find the column index for aggregate argument
                    let col_idx = all_columns
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(col_name));
                    match col_idx {
                        Some(idx) => match func_name.as_str() {
                            "SUM" => simple_aggs.push(StreamingAgg::Sum(idx)),
                            "AVG" => simple_aggs.push(StreamingAgg::Avg(idx)),
                            "MIN" => simple_aggs.push(StreamingAgg::Min(idx)),
                            "MAX" => simple_aggs.push(StreamingAgg::Max(idx)),
                            _ => unreachable!(),
                        },
                        None => return Ok(None),
                    }
                }
                _ => return Ok(None), // Unsupported aggregate (STDDEV, VARIANCE, etc.)
            }
        }

        // OPTIMIZATION: Don't use streaming GROUP BY when row fetch is needed without LIMIT.
        // Streaming benefits from early termination (LIMIT), but without it, bulk fetch
        // of all rows is more efficient than per-group fetching, even with buffer reuse.
        let needs_row_fetch = simple_aggs
            .iter()
            .any(|a| !matches!(a, StreamingAgg::Count));

        // Only use streaming for COUNT-only queries or when LIMIT allows early termination
        if needs_row_fetch && stmt.limit.is_none() {
            return Ok(None); // Fall back to non-streaming (faster for SUM/AVG/MIN/MAX)
        }

        // Check for simple HAVING filter
        let having_filter: Option<(usize, f64, bool)> = if let Some(ref having) = stmt.having {
            // Only support simple comparisons: agg_expr > constant
            match &**having {
                Expression::Infix(infix) => {
                    if infix.operator != ">" && infix.operator != ">=" {
                        return Ok(None);
                    }
                    // Left side should be an aggregate function
                    let agg_idx = match infix.left.as_ref() {
                        Expression::FunctionCall(fc) => {
                            let func_upper = fc.function.to_uppercase();
                            aggregations
                                .iter()
                                .position(|(name, _, _)| name == func_upper.as_str())
                        }
                        _ => None,
                    };
                    // Right side should be a constant
                    let threshold = match infix.right.as_ref() {
                        Expression::IntegerLiteral(n) => Some(n.value as f64),
                        Expression::FloatLiteral(f) => Some(f.value),
                        _ => None,
                    };
                    match (agg_idx, threshold) {
                        (Some(idx), Some(thresh)) => Some((idx, thresh, infix.operator == ">=")),
                        _ => return Ok(None), // Unsupported HAVING
                    }
                }
                _ => return Ok(None),
            }
        } else {
            None
        };

        // Build result columns
        let mut result_columns = Vec::with_capacity(1 + aggregations.len());
        result_columns.push(group_col_name.clone());
        for (func_name, col_name, alias) in &aggregations {
            let col_name = if let Some(ref a) = alias {
                a.clone()
            } else if col_name == "*" {
                format!("{}(*)", func_name)
            } else {
                format!("{}({})", func_name, col_name)
            };
            result_columns.push(col_name);
        }

        // Streaming aggregation: iterate through groups in sorted order
        let mut result_rows = RowVec::new();
        let mut result_row_id = 0i64;
        let num_aggs = simple_aggs.len();

        // Parse LIMIT for early termination
        // With streaming aggregation, we can stop once we have LIMIT groups that pass HAVING
        let limit_for_early_exit = stmt.limit.as_ref().and_then(|limit_expr| {
            ExpressionEval::compile(limit_expr, &[])
                .ok()
                .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                .and_then(|v| match v {
                    Value::Integer(n) if n > 0 => Some(n as usize),
                    _ => None,
                })
        });

        // Use streaming callback to avoid upfront allocation of all groups.
        // This is more efficient because:
        // 1. No Value cloning until we need to keep the result
        // 2. No SmallVec->Vec conversion for row IDs
        // 3. Early termination stops iteration immediately
        // 4. Reusable row buffer avoids per-group allocations

        // Pre-allocate reusable buffer for row fetching (avoids alloc/dealloc per group)
        let mut row_buffer = radixdb_core::RowVec::with_capacity(256);
        // Create true expression once outside loop
        use radixdb_storage::expression::logical::ConstBoolExpr;
        let true_expr = ConstBoolExpr::true_expr();

        let iteration_result =
            btree_index.for_each_group(&mut |group_value: &Value, row_ids: &[i64]| {
                // Aggregate state: sums for SUM/AVG, min/max values, counts
                let mut agg_sums = vec![0.0f64; num_aggs];
                let mut agg_mins = vec![f64::MAX; num_aggs];
                let mut agg_maxs = vec![f64::MIN; num_aggs];
                let mut agg_has_value = vec![false; num_aggs];
                let mut counts = vec![0i64; num_aggs];

                // Optimization: For COUNT-only aggregates, use row_ids.len() directly
                let row_count = row_ids.len() as i64;

                if needs_row_fetch {
                    // Use the reusable buffer for row fetching
                    row_buffer.clear();
                    table.fetch_rows_by_ids_into(row_ids, &true_expr, &mut row_buffer)?;

                    for (_row_id, row) in &row_buffer {
                        for (i, agg) in simple_aggs.iter().enumerate() {
                            match agg {
                                StreamingAgg::Count => {
                                    counts[i] += 1;
                                }
                                StreamingAgg::Sum(col_idx) | StreamingAgg::Avg(col_idx) => {
                                    if let Some(value) = row.get(*col_idx) {
                                        match value {
                                            Value::Integer(v) => {
                                                agg_sums[i] += *v as f64;
                                                counts[i] += 1;
                                                agg_has_value[i] = true;
                                            }
                                            Value::Float(v) => {
                                                agg_sums[i] += v;
                                                counts[i] += 1;
                                                agg_has_value[i] = true;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                StreamingAgg::Min(col_idx) => {
                                    if let Some(value) = row.get(*col_idx) {
                                        let v = match value {
                                            Value::Integer(v) => Some(*v as f64),
                                            Value::Float(v) => Some(*v),
                                            _ => None,
                                        };
                                        if let Some(v) = v {
                                            if v < agg_mins[i] {
                                                agg_mins[i] = v;
                                            }
                                            agg_has_value[i] = true;
                                        }
                                    }
                                }
                                StreamingAgg::Max(col_idx) => {
                                    if let Some(value) = row.get(*col_idx) {
                                        let v = match value {
                                            Value::Integer(v) => Some(*v as f64),
                                            Value::Float(v) => Some(*v),
                                            _ => None,
                                        };
                                        if let Some(v) = v {
                                            if v > agg_maxs[i] {
                                                agg_maxs[i] = v;
                                            }
                                            agg_has_value[i] = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Fast path: All aggregates are COUNT, no row fetch needed
                    for (i, agg) in simple_aggs.iter().enumerate() {
                        if matches!(agg, StreamingAgg::Count) {
                            counts[i] = row_count;
                        }
                    }
                }

                // Apply HAVING filter
                if let Some((agg_idx, threshold, inclusive)) = having_filter {
                    let agg_val = match simple_aggs[agg_idx] {
                        StreamingAgg::Count => counts[agg_idx] as f64,
                        StreamingAgg::Sum(_) | StreamingAgg::Avg(_) => {
                            if agg_has_value[agg_idx] {
                                match simple_aggs[agg_idx] {
                                    StreamingAgg::Avg(_) if counts[agg_idx] > 0 => {
                                        agg_sums[agg_idx] / counts[agg_idx] as f64
                                    }
                                    _ => agg_sums[agg_idx],
                                }
                            } else {
                                return Ok(true); // NULL doesn't pass HAVING, continue to next group
                            }
                        }
                        StreamingAgg::Min(_) => {
                            if agg_has_value[agg_idx] {
                                agg_mins[agg_idx]
                            } else {
                                return Ok(true); // Continue to next group
                            }
                        }
                        StreamingAgg::Max(_) => {
                            if agg_has_value[agg_idx] {
                                agg_maxs[agg_idx]
                            } else {
                                return Ok(true); // Continue to next group
                            }
                        }
                    };
                    let passes = if inclusive {
                        agg_val >= threshold
                    } else {
                        agg_val > threshold
                    };
                    if !passes {
                        return Ok(true); // Continue to next group
                    }
                }

                // Build result row - only clone group_value when we need to keep it
                let mut values = Vec::with_capacity(1 + num_aggs);
                values.push(group_value.clone());
                for (i, agg) in simple_aggs.iter().enumerate() {
                    let value = match agg {
                        StreamingAgg::Count => Value::Integer(counts[i]),
                        StreamingAgg::Sum(_) => {
                            if agg_has_value[i] {
                                Value::Float(agg_sums[i])
                            } else {
                                Value::null_unknown()
                            }
                        }
                        StreamingAgg::Avg(_) => {
                            if agg_has_value[i] && counts[i] > 0 {
                                Value::Float(agg_sums[i] / counts[i] as f64)
                            } else {
                                Value::null_unknown()
                            }
                        }
                        StreamingAgg::Min(_) => {
                            if agg_has_value[i] {
                                Value::Float(agg_mins[i])
                            } else {
                                Value::null_unknown()
                            }
                        }
                        StreamingAgg::Max(_) => {
                            if agg_has_value[i] {
                                Value::Float(agg_maxs[i])
                            } else {
                                Value::null_unknown()
                            }
                        }
                    };
                    values.push(value);
                }
                result_rows.push((result_row_id, Row::from_values(values)));
                result_row_id += 1;

                // Early termination: stop once we have LIMIT groups that passed HAVING
                if let Some(limit) = limit_for_early_exit {
                    if result_rows.len() >= limit {
                        return Ok(false); // Stop iteration
                    }
                }

                Ok(true) // Continue to next group
            });

        // Check if iteration was supported and succeeded
        match iteration_result {
            Some(Ok(())) => {}
            Some(Err(e)) => return Err(e),
            None => return Ok(None), // Fall back to regular GROUP BY
        }

        // Apply LIMIT if present
        if let Some(ref limit_expr) = stmt.limit {
            if let Ok(Value::Integer(n)) = ExpressionEval::compile(limit_expr, &[])
                .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()))
            {
                if n >= 0 {
                    result_rows.truncate(n as usize);
                }
            }
        }

        let result_columns = CompactArc::new(result_columns);
        let result =
            ExecutorResult::with_arc_columns(CompactArc::clone(&result_columns), result_rows);
        Ok(Some((Box::new(result), result_columns)))
    }
}

fn acl_provenance_endpoint(
    catalog: &radixdb_catalog::CatalogGeneration,
    acl_id: radixdb_catalog::ObjectId,
    edge_kind: radixdb_catalog::EdgeKind,
) -> Result<&radixdb_catalog::CatalogObject> {
    let mut endpoints = catalog
        .graph()
        .outgoing_edges(acl_id)
        .filter(|edge| edge.kind() == edge_kind)
        .filter_map(|edge| catalog.object(edge.target_object_id()));
    let endpoint = endpoints.next().ok_or_else(|| {
        Error::internal(format!(
            "ACL {acl_id} has no {} endpoint",
            edge_kind.name()
        ))
    })?;
    if endpoints.next().is_some() {
        return Err(Error::internal(format!(
            "ACL {acl_id} has multiple {} endpoints",
            edge_kind.name()
        )));
    }
    Ok(endpoint)
}

fn acl_privilege_names(bits: u64) -> String {
    [
        (radixdb_catalog::PRIVILEGE_CONNECT, "CONNECT"),
        (radixdb_catalog::PRIVILEGE_USAGE, "USAGE"),
        (radixdb_catalog::PRIVILEGE_CREATE, "CREATE"),
        (radixdb_catalog::PRIVILEGE_SELECT, "SELECT"),
        (radixdb_catalog::PRIVILEGE_INSERT, "INSERT"),
        (radixdb_catalog::PRIVILEGE_UPDATE, "UPDATE"),
        (radixdb_catalog::PRIVILEGE_DELETE, "DELETE"),
        (radixdb_catalog::PRIVILEGE_EXECUTE, "EXECUTE"),
    ]
    .into_iter()
    .filter_map(|(bit, name)| (bits & bit != 0).then_some(name))
    .collect::<Vec<_>>()
    .join(",")
}

fn acl_column_privileges(groups: &[radixdb_catalog::ColumnPrivilegeSet]) -> String {
    groups
        .iter()
        .map(|group| {
            format!(
                "{}:{}",
                acl_privilege_names(group.privilege()),
                group
                    .column_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}
