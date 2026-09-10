fn run_server(
    config: &BenchConfig,
    layout: &BenchLayout,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    println!("running RadixDB server participant");
    let database_dir = layout.server_data.join("databases").join(BENCH_DATABASE);
    reset_database_dir(&database_dir, &layout.server_data)?;
    fs::create_dir_all(&database_dir)?;
    instrumentation::reset();
    record_engine_snapshot(report, Participant::Server, "server.after_reset");
    let storage_sampler = StorageSampler::start(
        Participant::Server,
        "participant.run",
        layout.server_data.clone(),
    );
    let resource_sampler = ResourceSampler::start(
        Participant::Server,
        "participant.run",
        ProcessScope::Single(std::process::id() as i32),
        &layout.server_data,
    );

    let server_config = server_config(config, layout);
    let server = Server::bind(&server_config)?;
    let address = server.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_worker = Arc::clone(&shutdown);
    let server_thread = thread::spawn(move || server.run_until(&shutdown_worker));

    let mut client = connect_server(address)?;
    record_server_readiness(
        report,
        &mut client,
        "server.before_select_database",
        Some(BENCH_DATABASE),
    )?;
    client.select_database(BENCH_DATABASE)?;
    record_server_readiness(
        report,
        &mut client,
        "server.after_select_database",
        Some(BENCH_DATABASE),
    )?;

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        expect_command(client.execute(create_table_sql(table_index, false))?)?;
    }
    report.metrics.push(Metric::measured(
        Participant::Server,
        "schema.create_tables",
        start.elapsed(),
        TABLE_COUNT as u64,
        None,
    ));
    record_engine_snapshot(report, Participant::Server, "schema.create_tables.after");

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        let chunk_started = Instant::now();
        expect_command(client.execute(copy_sql(table_index, Path::new(&seed.artifact_dir)))?)?;
        let chunk_elapsed = chunk_started.elapsed();
        let chunk_rows = rows_for_table(seed.total_rows, table_index);
        let chunk_rows_per_second = chunk_rows as f64 / chunk_elapsed.as_secs_f64();
        println!(
            "RadixDB seed chunk {table_index:03}/{TABLE_COUNT}: rows={chunk_rows} elapsed_ms={:.3} rows_per_sec={chunk_rows_per_second:.3}",
            chunk_elapsed.as_secs_f64() * 1000.0
        );
        report.metrics.push(Metric::measured(
            Participant::Server,
            format!("seed.copy_csv.chunk.{table_index:03}"),
            chunk_elapsed,
            chunk_rows,
            None,
        ));
    }
    report.metrics.push(Metric::measured(
        Participant::Server,
        "seed.copy_csv",
        start.elapsed(),
        seed.total_rows,
        None,
    ));
    record_engine_snapshot(report, Participant::Server, "seed.copy_csv.after");

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        for sql in index_sql(table_index) {
            expect_command(client.execute(sql)?)?;
        }
    }
    report.metrics.push(Metric::measured(
        Participant::Server,
        "schema.create_indexes",
        start.elapsed(),
        benchmark_index_count(),
        None,
    ));
    record_engine_snapshot(report, Participant::Server, "schema.create_indexes.after");
    if config.page_cache_level > 0 {
        let checkpoint_started = Instant::now();
        let checkpoint = server_query_text(&mut client, "PRAGMA CHECKPOINT")?;
        report.metrics.push(Metric::measured(
            Participant::Server,
            "page_cache.publish_generation",
            checkpoint_started.elapsed(),
            1,
            Some(checkpoint),
        ));
    }
    record_page_cache_warmup(config, &mut client, report, "fresh.after_import")?;

    // Open the direct storage handle only after the TCP server has created and
    // populated the benchmark database.  Opening it earlier starts the shared
    // engine lifecycle while COPY is still in progress and makes the fresh
    // membership gate unreliable.
    let membership_database = matches!(config.only_case, Some(QueryCase::PkMembership))
        .then(|| Database::open(&server_database_dsn(&server_config, BENCH_DATABASE)))
        .transpose()?;

    let query_resource_sampler = ResourceSampler::start(
        Participant::Server,
        "query.phase",
        ProcessScope::Single(std::process::id() as i32),
        &layout.server_data,
    );
    let query_result = run_server_cases(
        config,
        &mut client,
        seed,
        report,
        membership_database.as_ref(),
    );
    let query_resource_result = query_resource_sampler.stop();
    drop(client);
    shutdown.store(true, Ordering::Release);
    let server_result: BenchResult<()> = match server_thread.join() {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err("RadixDB server benchmark thread panicked".into()),
    };
    report.engine.push(EngineMetric {
        participant: Participant::Server.as_str().to_string(),
        phase: "participant.run".to_string(),
        counters: instrumentation::snapshot(),
    });
    record_engine_snapshot(report, Participant::Server, "participant.run.final");
    let storage_result = storage_sampler.stop();
    let resource_result = resource_sampler.stop();

    query_result?;
    report.resources.push(query_resource_result?);
    server_result?;
    report.storage.push(storage_result?);
    report.resources.push(resource_result?);
    Ok(())
}

fn run_server_existing(
    config: &BenchConfig,
    layout: &BenchLayout,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    println!("running RadixDB server existing-data verification");
    let database_dir = layout.server_data.join("databases").join(BENCH_DATABASE);
    if !database_dir.exists() {
        return Err(format!(
            "existing benchmark database is missing: {}",
            database_dir.display()
        )
        .into());
    }
    let storage_sampler = StorageSampler::start(
        Participant::Server,
        "existing.verify",
        layout.server_data.clone(),
    );
    instrumentation::reset();
    record_engine_snapshot(report, Participant::Server, "existing.verify.after_reset");
    let resource_sampler = ResourceSampler::start(
        Participant::Server,
        "existing.verify",
        ProcessScope::Single(std::process::id() as i32),
        &layout.server_data,
    );

    if config.evict_database_page_cache {
        let eviction_started = Instant::now();
        let (files, bytes) = evict_database_page_cache(&database_dir)?;
        report.metrics.push(Metric::measured(
            Participant::Server,
            "page_cache.evict_dontneed",
            eviction_started.elapsed(),
            files,
            Some(bytes.to_string()),
        ));
    }
    let start = Instant::now();
    let server_config = server_config(config, layout);
    let membership_database = matches!(config.only_case, Some(QueryCase::PkMembership))
        .then(|| Database::open(&server_database_dsn(&server_config, BENCH_DATABASE)))
        .transpose()?;
    let server = Server::bind(&server_config)?;
    let address = server.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_worker = Arc::clone(&shutdown);
    let server_thread = thread::spawn(move || server.run_until(&shutdown_worker));

    let mut client = connect_server(address)?;
    record_server_readiness(
        report,
        &mut client,
        "server.existing.before_select_database",
        Some(BENCH_DATABASE),
    )?;
    client.select_database(BENCH_DATABASE)?;
    record_server_readiness(
        report,
        &mut client,
        "server.existing.after_select_database",
        Some(BENCH_DATABASE),
    )?;
    report.metrics.push(Metric::measured(
        Participant::Server,
        "server.cold_start_select_database",
        start.elapsed(),
        1,
        None,
    ));
    record_engine_snapshot(
        report,
        Participant::Server,
        "server.cold_start_select_database.after",
    );
    record_page_cache_warmup(config, &mut client, report, "existing.before_queries")?;

    let query_result = run_server_cases(
        config,
        &mut client,
        seed,
        report,
        membership_database.as_ref(),
    );
    drop(client);
    shutdown.store(true, Ordering::Release);
    let server_result: BenchResult<()> = match server_thread.join() {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err("RadixDB server benchmark thread panicked".into()),
    };
    report.engine.push(EngineMetric {
        participant: Participant::Server.as_str().to_string(),
        phase: "existing.verify".to_string(),
        counters: instrumentation::snapshot(),
    });
    record_engine_snapshot(report, Participant::Server, "existing.verify.final");
    let storage_result = storage_sampler.stop();
    let resource_result = resource_sampler.stop();

    query_result?;
    server_result?;
    report.storage.push(storage_result?);
    report.resources.push(resource_result?);
    Ok(())
}

fn server_config(config: &BenchConfig, layout: &BenchLayout) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: config.server_port,
        data_dir: layout.server_data.clone(),
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 151,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: radixdb::server::default_max_databases(),
        max_database_name_bytes: radixdb::server::default_max_database_name_bytes(),
        connect_timeout_secs: 10,
        connection_idle_timeout_secs: 28_800,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 60,
        cursor_batch_max_rows: 1024,
        cursor_batch_max_bytes: 8 * 1024 * 1024,
        max_frame_bytes: 64 * 1024 * 1024,
        copy_max_transaction_bytes: BENCH_COPY_TRANSACTION_BYTES,
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: config.storage_cpu_workers,
        page_cache_level: config.page_cache_level,
        page_cache_max_bytes: config.page_cache_max_bytes,
        page_cache_memory_reserve: config.page_cache_memory_reserve,
        target_volume_rows: config.target_volume_rows,
        seal_hot_bytes_threshold: config.seal_hot_bytes_threshold,
        seal_incremental_hot_bytes_threshold: config.seal_incremental_hot_bytes_threshold,
        read_queue_depth: config.read_queue_depth,
    }
}

fn server_database_dsn(config: &ServerConfig, database: &str) -> String {
    let database_dir = config.data_dir.join("databases").join(database);
    format!(
        "file://{}?copy_max_transaction_bytes={}&max_compaction_jobs={}&storage_cpu_workers={}&page_cache_level={}&page_cache_max_bytes={}&page_cache_memory_reserve={}&target_volume_rows={}&seal_hot_bytes_threshold={}&seal_incremental_hot_bytes_threshold={}&read_queue_depth={}",
        database_dir.display(),
        config.copy_max_transaction_bytes,
        config.max_compaction_jobs,
        config.storage_cpu_workers,
        config.page_cache_level,
        config.page_cache_max_bytes,
        config.page_cache_memory_reserve,
        config.target_volume_rows,
        config.seal_hot_bytes_threshold,
        config.seal_incremental_hot_bytes_threshold,
        config.read_queue_depth,
    )
}

fn connect_server(address: SocketAddr) -> BenchResult<Connection> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Connection::connect(address) {
            Ok(mut connection) => {
                connection.authenticate("root", None)?;
                return Ok(connection);
            }
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn record_server_readiness(
    report: &mut BenchReport,
    client: &mut Connection,
    phase: &str,
    database: Option<&str>,
) -> BenchResult<()> {
    let start = Instant::now();
    let status = match database {
        Some(database) => client.database_status(database)?,
        None => client.server_status()?,
    };
    report.readiness.push(ReadinessMetric {
        participant: Participant::Server.as_str().to_string(),
        phase: phase.to_string(),
        elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
        status,
    });
    Ok(())
}

fn run_server_cases(
    config: &BenchConfig,
    client: &mut Connection,
    seed: &SeedManifest,
    report: &mut BenchReport,
    membership_database: Option<&Database>,
) -> BenchResult<()> {
    if let Some(case) = config.only_case {
        return run_server_isolated_case(config, client, seed, report, case, membership_database);
    }

    measure_server_case(report, "correctness.seed_checksum", || {
        let mut total_rows = 0_i64;
        let mut amount_sum = 0_i128;
        for table_index in 1..=TABLE_COUNT {
            let table = table_name(table_index);
            total_rows += server_query_i64(client, &format!("SELECT COUNT(*) FROM {table}"))?;
            amount_sum +=
                server_query_i64(client, &format!("SELECT SUM(amount) FROM {table}"))? as i128;
        }
        if total_rows as u64 != seed.total_rows || amount_sum != seed.expected_amount_sum {
            return Err(format!(
                "server checksum mismatch: rows={total_rows}, amount_sum={amount_sum}"
            )
            .into());
        }
        Ok((
            (),
            seed.total_rows,
            Some(format!("{total_rows}:{amount_sum}")),
        ))
    })?;

    let table = table_name(60);
    let selected_id = rows_for_table(seed.total_rows, 60) / 2;
    let range_end = selected_id.saturating_add(999);
    measure_server_query_count(
        client,
        report,
        "select.pk",
        &format!("SELECT COUNT(*) FROM {table} WHERE id = {selected_id}"),
    )?;
    measure_server_query_count(
        client,
        report,
        "select.range",
        &format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN {selected_id} AND {range_end}"),
    )?;
    measure_server_query_rows(
        client,
        report,
        "scan.full",
        &format!("SELECT * FROM {table}"),
        config.fetch_mode,
    )?;
    measure_server_query_rows(
        client,
        report,
        "scan.projected",
        &format!("SELECT id, amount FROM {table}"),
        config.fetch_mode,
    )?;
    measure_server_query_count(
        client,
        report,
        "aggregate.group_having",
        &format!(
            "SELECT COUNT(*) FROM (SELECT bucket, COUNT(*) c, SUM(amount) s FROM {table} GROUP BY bucket HAVING COUNT(*) > 0) q"
        ),
    )?;
    measure_server_query_count(client, report, "join.parent", &join_parent_sql())?;
    let direct_count = expected_join_parent_count(seed.total_rows);
    run_server_repeated_reference_case(
        config,
        client,
        report,
        QueryCase::ReferenceNavigation,
        &reference_navigation_sql(),
        direct_count,
    )?;
    run_server_repeated_reference_case(
        config,
        client,
        report,
        QueryCase::ReferenceDirectExplicit,
        &reference_direct_join_sql(),
        direct_count,
    )?;
    let fact_count = expected_reference_fact_group_count(seed.total_rows);
    run_server_repeated_reference_case(
        config,
        client,
        report,
        QueryCase::ReferenceFactDictionary,
        &reference_fact_dictionary_sql(),
        fact_count,
    )?;
    run_server_repeated_reference_case(
        config,
        client,
        report,
        QueryCase::ReferenceFactFirst,
        &reference_fact_first_join_sql(),
        fact_count,
    )?;
    run_server_repeated_reference_case(
        config,
        client,
        report,
        QueryCase::ReferenceTargetFirst,
        &reference_target_first_join_sql(),
        fact_count,
    )?;
    measure_server_case(report, "update.rollback", || {
        client.begin()?;
        let updated = server_execute_rows(
            client,
            &format!(
                "UPDATE {} SET amount = amount + 1 WHERE bucket = 17",
                table_name(42)
            ),
        )?;
        client.rollback()?;
        Ok(((), updated, None))
    })?;

    measure_server_case(report, "delete.rollback", || {
        client.begin()?;
        let deleted = server_execute_rows(
            client,
            &format!("DELETE FROM {} WHERE bucket = 18", table_name(43)),
        )?;
        client.rollback()?;
        Ok(((), deleted, None))
    })?;
    Ok(())
}

fn run_server_isolated_case(
    config: &BenchConfig,
    client: &mut Connection,
    seed: &SeedManifest,
    report: &mut BenchReport,
    case: QueryCase,
    membership_database: Option<&Database>,
) -> BenchResult<()> {
    match case {
        QueryCase::JoinParent => {
            let sql = join_parent_sql();
            let expected_count = expected_join_parent_count(seed.total_rows);
            let mut execute = || server_query_i64(client, &sql);
            let warmup_ms = measure_server_count_iterations(
                report,
                case.as_str(),
                "warmup",
                config.warmup_runs,
                expected_count,
                &mut execute,
            )?;

            let measured_ms = measure_server_count_iterations(
                report,
                case.as_str(),
                "measured",
                config.repeat_runs,
                expected_count,
                &mut execute,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Server,
                case.as_str(),
                expected_count,
                Some(expected_count.to_string()),
                summary,
            ));
            record_server_access_path(report, client, case.as_str(), &sql)?;
            Ok(())
        }
        QueryCase::JoinParentSweep => {
            run_server_join_parent_selectivity_sweep(config, client, seed, report)
        }
        QueryCase::JoinParentDistribution => {
            run_server_join_parent_key_distribution_sweep(config, client, seed, report)
        }
        QueryCase::PkMembership => run_server_pk_membership_case(
            config,
            client,
            seed,
            report,
            membership_database.ok_or("pk.membership requires direct server database access")?,
        ),
        QueryCase::DeleteRollback => {
            let expected_rows = rows_for_table(seed.total_rows, 43);
            let expected_deleted = expected_table_bucket_count(seed.total_rows, 43, 18);
            let expected_amount_sum = expected_table_amount_sum(seed.total_rows, 43);
            let warmup_ms = measure_server_delete_rollback_iterations(
                client,
                report,
                "warmup",
                config.warmup_runs,
                expected_deleted,
                expected_rows,
                expected_amount_sum,
            )?;
            let measured_ms = measure_server_delete_rollback_iterations(
                client,
                report,
                "measured",
                config.repeat_runs,
                expected_deleted,
                expected_rows,
                expected_amount_sum,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Server,
                case.as_str(),
                expected_deleted,
                Some(format!("{expected_rows}:{expected_amount_sum}")),
                summary,
            ));
            record_server_access_path(
                report,
                client,
                case.as_str(),
                &format!("DELETE FROM {} WHERE bucket = 18", table_name(43)),
            )?;
            Ok(())
        }
        QueryCase::UpdateRollback => {
            let expected_rows = rows_for_table(seed.total_rows, 42);
            let expected_updated = expected_table_bucket_count(seed.total_rows, 42, 17);
            let expected_amount_sum = expected_table_amount_sum(seed.total_rows, 42);
            let warmup_ms = measure_server_update_rollback_iterations(
                client,
                report,
                "warmup",
                config.warmup_runs,
                expected_updated,
                expected_rows,
                expected_amount_sum,
            )?;
            let measured_ms = measure_server_update_rollback_iterations(
                client,
                report,
                "measured",
                config.repeat_runs,
                expected_updated,
                expected_rows,
                expected_amount_sum,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Server,
                case.as_str(),
                expected_updated,
                Some(format!("{expected_rows}:{expected_amount_sum}")),
                summary,
            ));
            record_server_access_path(
                report,
                client,
                case.as_str(),
                &format!(
                    "UPDATE {} SET amount = amount + 1 WHERE bucket = 17",
                    table_name(42)
                ),
            )?;
            Ok(())
        }
        QueryCase::ReferenceNavigation => run_server_repeated_reference_case(
            config,
            client,
            report,
            case,
            &reference_navigation_sql(),
            expected_join_parent_count(seed.total_rows),
        ),
        QueryCase::ReferenceDirectExplicit => run_server_repeated_reference_case(
            config,
            client,
            report,
            case,
            &reference_direct_join_sql(),
            expected_join_parent_count(seed.total_rows),
        ),
        QueryCase::ReferenceFactDictionary => run_server_repeated_reference_case(
            config,
            client,
            report,
            case,
            &reference_fact_dictionary_sql(),
            expected_reference_fact_group_count(seed.total_rows),
        ),
        QueryCase::ReferenceFactFirst => run_server_repeated_reference_case(
            config,
            client,
            report,
            case,
            &reference_fact_first_join_sql(),
            expected_reference_fact_group_count(seed.total_rows),
        ),
        QueryCase::ReferenceTargetFirst => run_server_repeated_reference_case(
            config,
            client,
            report,
            case,
            &reference_target_first_join_sql(),
            expected_reference_fact_group_count(seed.total_rows),
        ),
        QueryCase::ReferenceProjectionNavigation => run_server_repeated_projection_case(
            config,
            client,
            report,
            case,
            &reference_projection_navigation_sql(),
            expected_reference_projection(seed.total_rows).0,
            expected_reference_projection(seed.total_rows).1,
        ),
        QueryCase::ReferenceProjectionExplicit => run_server_repeated_projection_case(
            config,
            client,
            report,
            case,
            &reference_projection_explicit_sql(),
            expected_reference_projection(seed.total_rows).0,
            expected_reference_projection(seed.total_rows).1,
        ),
        QueryCase::ReferenceProjectionTransitive => run_server_repeated_projection_case(
            config,
            client,
            report,
            case,
            &reference_projection_transitive_sql(),
            expected_reference_projection(seed.total_rows).0,
            expected_reference_projection(seed.total_rows).1,
        ),
        QueryCase::ReferenceProjectionTransitiveExplicit => run_server_repeated_projection_case(
            config,
            client,
            report,
            case,
            &reference_projection_transitive_explicit_sql(),
            expected_reference_projection(seed.total_rows).0,
            expected_reference_projection(seed.total_rows).1,
        ),
    }
}

fn run_server_repeated_reference_case(
    config: &BenchConfig,
    client: &mut Connection,
    report: &mut BenchReport,
    case: QueryCase,
    sql: &str,
    expected_count: u64,
) -> BenchResult<()> {
    let statement = client.prepare(sql)?;
    let measurement = (|| {
        let mut execute = || server_prepared_query_i64(client, &statement, sql);
        let warmup_ms = measure_server_count_iterations(
            report,
            case.as_str(),
            "warmup",
            config.warmup_runs,
            expected_count,
            &mut execute,
        )?;
        let measured_ms = measure_server_count_iterations(
            report,
            case.as_str(),
            "measured",
            config.repeat_runs,
            expected_count,
            &mut execute,
        )?;
        let summary = RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
        report.metrics.push(Metric::repeated(
            Participant::Server,
            case.as_str(),
            expected_count,
            Some(expected_count.to_string()),
            summary,
        ));
        Ok(())
    })();
    let cleanup = client.close_prepared(statement).map_err(Into::into);
    match (measurement, cleanup) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) => return Err(error),
        (Ok(()), Err(error)) => return Err(error),
        (Err(primary), Err(cleanup)) => {
            return Err(format!(
                "{primary}; closing prepared benchmark case {} also failed: {cleanup}",
                case.as_str()
            )
            .into())
        }
    }
    record_server_access_path(report, client, case.as_str(), sql)?;
    Ok(())
}

fn run_server_repeated_projection_case(
    config: &BenchConfig,
    client: &mut Connection,
    report: &mut BenchReport,
    case: QueryCase,
    sql: &str,
    expected_rows: u64,
    expected_checksum: i128,
) -> BenchResult<()> {
    let statement = client.prepare(sql)?;
    let measurement = (|| {
        let mut execute = || server_prepared_query_rows_checksum(client, &statement, sql);
        let warmup_ms = measure_server_projection_iterations(
            report,
            case.as_str(),
            "warmup",
            config.warmup_runs,
            expected_rows,
            expected_checksum,
            &mut execute,
        )?;
        let measured_ms = measure_server_projection_iterations(
            report,
            case.as_str(),
            "measured",
            config.repeat_runs,
            expected_rows,
            expected_checksum,
            &mut execute,
        )?;
        let summary = RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
        report.metrics.push(Metric::repeated(
            Participant::Server,
            case.as_str(),
            expected_rows,
            Some(format!("{expected_rows}:{expected_checksum}")),
            summary,
        ));
        Ok(())
    })();
    let cleanup = client.close_prepared(statement).map_err(Into::into);
    match (measurement, cleanup) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) => return Err(error),
        (Ok(()), Err(error)) => return Err(error),
        (Err(primary), Err(cleanup)) => {
            return Err(format!(
                "{primary}; closing prepared benchmark case {} also failed: {cleanup}",
                case.as_str()
            )
            .into())
        }
    }
    record_server_access_path(report, client, case.as_str(), sql)?;
    Ok(())
}

/// Run the same physical `COUNT(*)` PK semi-join across a deterministic
/// child-side selectivity sweep. This stays on the normal TCP protocol and on
/// the benchmark seed; it does not encode any private storage assumptions.
fn run_server_join_parent_selectivity_sweep(
    config: &BenchConfig,
    client: &mut Connection,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    let cases = [
        ("zero", Some((1_000_u64, 1_000_u64))),
        ("one", Some((10, 10))),
        ("one_percent", Some((10, 19))),
        ("ten_percent", Some((10, 109))),
        ("fifty_percent", Some((10, 508))),
        ("hundred_percent", None),
    ];
    let child = table_name(60);
    let parent = table_name(59);

    for (label, bucket_range) in cases {
        let (sql, expected_count) = if let Some((first, last)) = bucket_range {
            (
                format!(
                    "SELECT COUNT(*) FROM {child} c JOIN {parent} p ON c.parent_id = p.id \
                     WHERE c.parent_table = 59 AND c.bucket BETWEEN {first} AND {last}"
                ),
                expected_child_bucket_range_count(seed.total_rows, first, last),
            )
        } else {
            (
                format!(
                    "SELECT COUNT(*) FROM {child} c JOIN {parent} p ON c.parent_id = p.id \
                     WHERE c.parent_table = 59"
                ),
                rows_for_table(seed.total_rows, 60),
            )
        };
        let case = format!("join.parent.selectivity.{label}");
        let mut execute = || server_query_i64(client, &sql);
        let warmup_ms = measure_server_count_iterations(
            report,
            &case,
            "warmup",
            config.warmup_runs,
            expected_count,
            &mut execute,
        )?;
        let measured_ms = measure_server_count_iterations(
            report,
            &case,
            "measured",
            config.repeat_runs,
            expected_count,
            &mut execute,
        )?;
        report.metrics.push(Metric::repeated(
            Participant::Server,
            &case,
            expected_count,
            Some(expected_count.to_string()),
            RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?,
        ));
        record_server_access_path(report, client, &case, &sql)?;
    }
    Ok(())
}

/// Exercise the count PK semi-join against key distributions which the normal
/// generated benchmark intentionally does not have.  The tables are created
/// through the TCP protocol, use ordinary SQL/COPY, and are removed before the
/// case returns, so this is a server-contract performance gate rather than a
/// private storage microbenchmark.
fn run_server_join_parent_key_distribution_sweep(
    config: &BenchConfig,
    client: &mut Connection,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    let rows = rows_for_table(seed.total_rows, 60).min(100_000);
    let seed_dir = Path::new(&report.root)
        .join("results")
        .join(&report.run_id)
        .join("seed");
    fs::create_dir_all(&seed_dir)?;

    let parent = "bench_join_dist_parent";
    let distributions = [
        ("unique", "bench_join_dist_unique", rows),
        ("repeated", "bench_join_dist_repeated", rows),
        (
            "mixed_orphan",
            "bench_join_dist_mixed_orphan",
            rows - rows / 3,
        ),
    ];
    let parent_csv = seed_dir.join("join_parent_distribution_parent.csv");
    write_join_parent_distribution_csv(&parent_csv, rows, DistributionKind::Parent)?;
    let child_csv = distributions
        .iter()
        .map(|(label, _, _)| {
            let path = seed_dir.join(format!("join_parent_distribution_{label}.csv"));
            let kind = match *label {
                "unique" => DistributionKind::Unique,
                "repeated" => DistributionKind::Repeated,
                "mixed_orphan" => DistributionKind::MixedOrphan,
                _ => unreachable!("static distribution label"),
            };
            write_join_parent_distribution_csv(&path, rows, kind)?;
            Ok((label, path))
        })
        .collect::<BenchResult<Vec<_>>>()?;

    let create_result = (|| -> BenchResult<()> {
        server_execute_rows(
            client,
            &format!("CREATE TABLE {parent} (id INTEGER PRIMARY KEY)"),
        )?;
        server_execute_rows(client, &copy_file_sql(parent, &parent_csv))?;
        for ((_, table, _), (_, path)) in distributions.iter().zip(&child_csv) {
            server_execute_rows(
                client,
                &format!(
                    "CREATE TABLE {table} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL)"
                ),
            )?;
            server_execute_rows(client, &copy_file_sql(table, path))?;
        }

        for ((label, table, expected_count), _) in distributions.iter().zip(&child_csv) {
            let case = format!("join.parent.distribution.{label}");
            let sql =
                format!("SELECT COUNT(*) FROM {table} c JOIN {parent} p ON c.parent_id = p.id");
            let mut execute = || server_query_i64(client, &sql);
            let warmup_ms = measure_server_count_iterations(
                report,
                &case,
                "warmup",
                config.warmup_runs,
                *expected_count,
                &mut execute,
            )?;
            let measured_ms = measure_server_count_iterations(
                report,
                &case,
                "measured",
                config.repeat_runs,
                *expected_count,
                &mut execute,
            )?;
            report.metrics.push(Metric::repeated(
                Participant::Server,
                &case,
                *expected_count,
                Some(expected_count.to_string()),
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?,
            ));
            record_server_access_path(report, client, &case, &sql)?;
        }
        Ok(())
    })();

    let mut cleanup_error = None;
    for (_, table, _) in distributions.iter().rev() {
        if let Err(error) = server_execute_rows(client, &format!("DROP TABLE {table}")) {
            cleanup_error.get_or_insert_with(|| error.to_string());
        }
    }
    if let Err(error) = server_execute_rows(client, &format!("DROP TABLE {parent}")) {
        cleanup_error.get_or_insert_with(|| error.to_string());
    }
    create_result?;
    if let Some(error) = cleanup_error {
        return Err(format!("join.parent.distribution cleanup failed: {error}").into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum DistributionKind {
    Parent,
    Unique,
    Repeated,
    MixedOrphan,
}

fn write_join_parent_distribution_csv(
    path: &Path,
    rows: u64,
    kind: DistributionKind,
) -> BenchResult<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    match kind {
        DistributionKind::Parent => {
            writeln!(writer, "id")?;
            for id in 1..=rows {
                writeln!(writer, "{id}")?;
            }
        }
        DistributionKind::Unique | DistributionKind::Repeated | DistributionKind::MixedOrphan => {
            writeln!(writer, "id,parent_id")?;
            for id in 1..=rows {
                let parent_id = match kind {
                    DistributionKind::Unique => id,
                    DistributionKind::Repeated => 1,
                    DistributionKind::MixedOrphan if id % 3 == 0 => rows + id,
                    DistributionKind::MixedOrphan => id,
                    DistributionKind::Parent => unreachable!("parent CSV has no child keys"),
                };
                writeln!(writer, "{id},{parent_id}")?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}

const MEMBERSHIP_MIN_SAMPLE: Duration = Duration::from_millis(10);
const MEMBERSHIP_MAX_INNER_ITERATIONS: usize = 16_384;

fn run_server_pk_membership_case(
    config: &BenchConfig,
    client: &mut Connection,
    seed: &SeedManifest,
    report: &mut BenchReport,
    database: &Database,
) -> BenchResult<()> {
    // Keep the client/server boundary in the benchmark: the key stream is
    // selected through the normal protocol and only the membership primitive is
    // measured directly. Stage 3 will replace this split with one physical JOIN
    // operator.
    let child_sql = format!(
        "SELECT parent_id FROM {} WHERE parent_table = 59 AND bucket BETWEEN 10 AND 20 ORDER BY id",
        table_name(60)
    );
    let parent_ids = server_query_i64_rows(client, &child_sql)?;
    let expected_hits = usize::try_from(expected_join_parent_count(seed.total_rows))?;
    if parent_ids.len() != expected_hits {
        return Err(format!(
            "pk.membership input yielded {} keys, expected {expected_hits}",
            parent_ids.len()
        )
        .into());
    }

    let mut transaction = database.engine().begin_transaction()?;
    let parent = transaction.get_table(&table_name(59))?;
    let sequential_bitmap: Vec<bool> = parent_ids
        .iter()
        .map(|&row_id| parent.has_row_id(row_id))
        .collect();
    let sequential_hits = sequential_bitmap.iter().filter(|&&matched| matched).count();
    let mut batch_bitmap = vec![false; parent_ids.len()];
    let batch_hits = parent.probe_visible_row_ids(&parent_ids, &mut batch_bitmap)?;
    if batch_hits != sequential_hits || batch_bitmap != sequential_bitmap {
        return Err(format!(
            "pk.membership bitmap mismatch: sequential_hits={sequential_hits}, batch_hits={batch_hits}"
        )
        .into());
    }
    if batch_hits != expected_hits {
        return Err(
            format!("pk.membership returned {batch_hits} hits, expected {expected_hits}").into(),
        );
    }

    let inner_iterations = calibrate_membership_iterations(&*parent, &parent_ids, expected_hits)?;
    let mut warmup = Vec::with_capacity(config.warmup_runs);
    for iteration in 0..config.warmup_runs {
        warmup.push(measure_membership_pair(
            &*parent,
            &parent_ids,
            expected_hits,
            inner_iterations,
            iteration,
        )?);
    }
    let mut measured = Vec::with_capacity(config.repeat_runs);
    for iteration in 0..config.repeat_runs {
        measured.push(measure_membership_pair(
            &*parent,
            &parent_ids,
            expected_hits,
            inner_iterations,
            config.warmup_runs + iteration,
        )?);
    }

    let sequential_samples: Vec<f64> = measured.iter().map(|sample| sample.sequential_ms).collect();
    let batch_samples: Vec<f64> = measured.iter().map(|sample| sample.batch_ms).collect();
    let sequential_median_ms = median_ms(&sequential_samples)?;
    let batch_median_ms = median_ms(&batch_samples)?;
    let median_batch_over_sequential = batch_median_ms / sequential_median_ms;
    if median_batch_over_sequential > 1.05 {
        return Err(format!(
            "pk.membership regressed: batch/sequential={median_batch_over_sequential:.3} exceeds 1.050"
        )
        .into());
    }
    for (iteration, sample) in measured.iter().enumerate() {
        if sample.batch_counters.volume_read_calls != 0
            || sample.batch_counters.decompression_calls != 0
            || sample.batch_counters.row_materialization_rows != 0
        {
            return Err(format!(
                "pk.membership measured batch {iteration} read payload: reads={}, decompressions={}, materialized_rows={}",
                sample.batch_counters.volume_read_calls,
                sample.batch_counters.decompression_calls,
                sample.batch_counters.row_materialization_rows,
            )
            .into());
        }
    }

    let profile = CaseProfile::for_config(config);
    report.metrics.push(Metric::repeated(
        Participant::Server,
        "pk.membership.sequential",
        expected_hits as u64,
        Some(expected_hits.to_string()),
        RepeatSummary::new(
            profile,
            warmup.iter().map(|sample| sample.sequential_ms).collect(),
            sequential_samples,
        )?,
    ));
    report.metrics.push(Metric::repeated(
        Participant::Server,
        "pk.membership.batch",
        expected_hits as u64,
        Some(expected_hits.to_string()),
        RepeatSummary::new(
            profile,
            warmup.iter().map(|sample| sample.batch_ms).collect(),
            batch_samples,
        )?,
    ));
    report.membership.push(MembershipMetric {
        profile,
        input_keys: parent_ids.len(),
        expected_hits,
        inner_iterations,
        warmup,
        measured,
        sequential_median_ms,
        batch_median_ms,
        median_batch_over_sequential,
    });
    drop(parent);
    transaction.rollback()?;
    Ok(())
}

fn calibrate_membership_iterations(
    table: &dyn Table,
    row_ids: &[i64],
    expected_hits: usize,
) -> BenchResult<usize> {
    let mut inner_iterations = 1usize;
    loop {
        let sequential =
            measure_membership_sequential(table, row_ids, expected_hits, inner_iterations)?;
        let batch = measure_membership_batch(table, row_ids, expected_hits, inner_iterations)?.0;
        if sequential >= MEMBERSHIP_MIN_SAMPLE && batch >= MEMBERSHIP_MIN_SAMPLE
            || inner_iterations >= MEMBERSHIP_MAX_INNER_ITERATIONS
        {
            return Ok(inner_iterations);
        }
        inner_iterations = inner_iterations.saturating_mul(2);
    }
}

fn measure_membership_pair(
    table: &dyn Table,
    row_ids: &[i64],
    expected_hits: usize,
    inner_iterations: usize,
    iteration: usize,
) -> BenchResult<MembershipSample> {
    // Alternate order so an incidental cache/turbo effect is not permanently
    // attributed to one side of the comparison.
    let (sequential, batch, batch_counters) = if iteration.is_multiple_of(2) {
        let sequential =
            measure_membership_sequential(table, row_ids, expected_hits, inner_iterations)?;
        instrumentation::reset();
        let (batch, counters) =
            measure_membership_batch(table, row_ids, expected_hits, inner_iterations)?;
        (sequential, batch, counters)
    } else {
        instrumentation::reset();
        let (batch, counters) =
            measure_membership_batch(table, row_ids, expected_hits, inner_iterations)?;
        let sequential =
            measure_membership_sequential(table, row_ids, expected_hits, inner_iterations)?;
        (sequential, batch, counters)
    };
    Ok(MembershipSample {
        sequential_ms: sequential.as_secs_f64() * 1000.0,
        batch_ms: batch.as_secs_f64() * 1000.0,
        batch_over_sequential: batch.as_secs_f64() / sequential.as_secs_f64(),
        batch_counters,
    })
}

fn measure_membership_sequential(
    table: &dyn Table,
    row_ids: &[i64],
    expected_hits: usize,
    inner_iterations: usize,
) -> BenchResult<Duration> {
    let started = Instant::now();
    for _ in 0..inner_iterations {
        let hits = row_ids
            .iter()
            .filter(|&&row_id| table.has_row_id(row_id))
            .count();
        if hits != expected_hits {
            return Err(format!(
                "pk.membership sequential returned {hits} hits, expected {expected_hits}"
            )
            .into());
        }
        std::hint::black_box(hits);
    }
    Ok(started.elapsed())
}

fn measure_membership_batch(
    table: &dyn Table,
    row_ids: &[i64],
    expected_hits: usize,
    inner_iterations: usize,
) -> BenchResult<(Duration, EngineCountersSnapshot)> {
    let mut matches = vec![false; row_ids.len()];
    let before = instrumentation::snapshot();
    let started = Instant::now();
    for _ in 0..inner_iterations {
        let hits = table.probe_visible_row_ids(row_ids, &mut matches)?;
        if hits != expected_hits
            || matches.iter().filter(|&&matched| matched).count() != expected_hits
        {
            return Err(format!(
                "pk.membership batch returned {hits} hits, expected {expected_hits}"
            )
            .into());
        }
        std::hint::black_box(hits);
    }
    let elapsed = started.elapsed();
    let after = instrumentation::snapshot();
    Ok((elapsed, engine_counter_delta(&after, &before)))
}

fn median_ms(samples: &[f64]) -> BenchResult<f64> {
    if samples.is_empty() {
        return Err("cannot calculate a median of zero membership samples".into());
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Ok(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn measure_server_query_count(
    client: &mut Connection,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
) -> BenchResult<i64> {
    let count = measure_server_case(report, case, || {
        let count = server_query_i64(client, sql)?;
        Ok((count, count.max(0) as u64, Some(count.to_string())))
    })?;
    record_server_access_path(report, client, case, sql)?;
    Ok(count)
}

fn measure_server_query_rows(
    client: &mut Connection,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
    fetch_mode: BenchFetchMode,
) -> BenchResult<()> {
    measure_server_case(report, case, || {
        let cursor = match client.execute(sql)? {
            ExecuteResult::Cursor(cursor) => cursor,
            ExecuteResult::CommandComplete { .. } => {
                return Err(format!("expected cursor result for `{sql}`").into())
            }
        };
        let mut row_count = 0_u64;
        let mut checksum = 0_i128;
        loop {
            match client.fetch_batch(&cursor, fetch_mode.client_mode())? {
                ColumnCursorBatch::Columnar {
                    columns,
                    row_count: batch_rows,
                    eof,
                } => {
                    checksum += first_wire_column_i128_sum(&columns, batch_rows, sql)?;
                    row_count += u64::from(batch_rows);
                    if eof {
                        break;
                    }
                }
                ColumnCursorBatch::Rows(batch) => {
                    for row in &batch.rows {
                        let value = row
                            .values
                            .first()
                            .ok_or_else(|| format!("empty row returned by `{sql}`"))?;
                        checksum += wire_value_to_i128(value)
                            .ok_or_else(|| format!("first column of `{sql}` is not an integer"))?;
                    }
                    row_count += batch.rows.len() as u64;
                    if batch.eof {
                        break;
                    }
                }
            }
        }
        Ok(((), row_count, Some(format!("{row_count}:{checksum}"))))
    })?;
    record_server_access_path(report, client, case, sql)?;
    Ok(())
}

fn record_server_access_path(
    report: &mut BenchReport,
    client: &mut Connection,
    case: &str,
    sql: &str,
) -> BenchResult<()> {
    match server_explain_lines(client, sql) {
        Ok(explain_lines) => {
            let access_paths = extract_access_paths(&explain_lines);
            let join_path_valid = case != QueryCase::JoinParent.as_str()
                || (access_paths
                    .iter()
                    .any(|line| line.starts_with("Join Access Path:"))
                    && access_paths
                        .iter()
                        .any(|line| line.starts_with("Access Path:")));
            report.access_paths.push(AccessPathMetric {
                participant: Participant::Server.as_str().to_string(),
                case: case.to_string(),
                sql: sql.to_string(),
                access_paths,
                explain_lines,
                error: None,
            });
            if !join_path_valid {
                return Err(
                    "join.parent EXPLAIN must expose both Join Access Path and Access Path".into(),
                );
            }
        }
        Err(error) => {
            let message = error.to_string();
            report.access_paths.push(AccessPathMetric {
                participant: Participant::Server.as_str().to_string(),
                case: case.to_string(),
                sql: sql.to_string(),
                access_paths: Vec::new(),
                explain_lines: Vec::new(),
                error: Some(message.clone()),
            });
            if case == QueryCase::JoinParent.as_str() {
                return Err(format!("join.parent EXPLAIN failed: {message}").into());
            }
        }
    }
    Ok(())
}

fn server_explain_lines(client: &mut Connection, sql: &str) -> BenchResult<Vec<String>> {
    let explain_sql = format!("EXPLAIN {sql}");
    let cursor = match client.execute(&explain_sql)? {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err(format!("expected cursor result for `{explain_sql}`").into())
        }
    };
    let mut lines = Vec::new();
    loop {
        let batch = client.fetch(&cursor)?;
        for row in &batch.rows {
            let value = row
                .values
                .first()
                .ok_or_else(|| format!("empty EXPLAIN row returned by `{explain_sql}`"))?;
            lines.push(wire_value_to_string(value));
        }
        if batch.eof {
            break;
        }
    }
    Ok(lines)
}

fn extract_access_paths(explain_lines: &[String]) -> Vec<String> {
    let mut paths = Vec::new();
    for line in explain_lines {
        let trimmed = line.trim();
        if trimmed.starts_with("Access Path:")
            || trimmed.starts_with("Access Source:")
            || trimmed.starts_with("Access Key:")
            || trimmed.starts_with("Access Filter:")
            || trimmed.starts_with("Join Access Path:")
            || trimmed.starts_with("Join Projection Boundary:")
            || trimmed.starts_with("Projection Boundary:")
            || trimmed.starts_with("Aggregation Path:")
            || trimmed.starts_with("Aggregation Fallback:")
            || trimmed.starts_with("Hot Access Path:")
            || trimmed.starts_with("Cold Metadata Path:")
            || trimmed.starts_with("RAM Accelerator:")
        {
            paths.push(trimmed.to_string());
        }
    }
    paths
}

fn wire_value_to_string(value: &WireValue) -> String {
    match value {
        WireValue::Null => "NULL".to_string(),
        WireValue::Bool(value) => value.to_string(),
        WireValue::Int(value) => value.to_string(),
        WireValue::Int8(value) => value.to_string(),
        WireValue::Int16(value) => value.to_string(),
        WireValue::Int32(value) => value.to_string(),
        WireValue::UInt(value) => value.to_string(),
        WireValue::UInt8(value) => value.to_string(),
        WireValue::UInt16(value) => value.to_string(),
        WireValue::UInt32(value) => value.to_string(),
        WireValue::Float64(value) => value.to_string(),
        WireValue::Decimal {
            unscaled,
            precision,
            scale,
        } => format!("{unscaled}p{precision}s{scale}"),
        WireValue::String(value) => value.clone(),
        other => format!("{other:?}"),
    }
}

fn wire_value_to_i128(value: &WireValue) -> Option<i128> {
    match value {
        WireValue::Int(value) => Some(i128::from(*value)),
        WireValue::Int8(value) => Some(i128::from(*value)),
        WireValue::Int16(value) => Some(i128::from(*value)),
        WireValue::Int32(value) => Some(i128::from(*value)),
        WireValue::UInt(value) => Some(i128::from(*value)),
        WireValue::UInt8(value) => Some(i128::from(*value)),
        WireValue::UInt16(value) => Some(i128::from(*value)),
        WireValue::UInt32(value) => Some(i128::from(*value)),
        _ => None,
    }
}

/// Benchmark checksum for the first output column of a direct columnar batch.
/// The full/projected scan contracts use a non-null INTEGER `id` first; retain
/// that assertion so a transport optimisation cannot mask a result-shape bug.
fn first_wire_column_i128_sum(
    columns: &[WireColumn],
    row_count: u32,
    sql: &str,
) -> BenchResult<i128> {
    let expected_rows = row_count as usize;
    let Some(first_column) = columns.first() else {
        return if expected_rows == 0 {
            Ok(0)
        } else {
            Err(
                format!("column batch returned {expected_rows} rows without columns for `{sql}`")
                    .into(),
            )
        };
    };
    match first_column {
        WireColumn::Int64 { values, nulls } => {
            if values.len() != expected_rows || nulls.len() != expected_rows {
                return Err(format!(
                    "invalid Int64 column batch lengths for `{sql}`: values={}, nulls={}, rows={expected_rows}",
                    values.len(),
                    nulls.len()
                )
                .into());
            }
            if nulls.iter().any(|is_null| *is_null) {
                return Err(format!("first column of `{sql}` unexpectedly contains NULL").into());
            }
            Ok(values.iter().map(|value| i128::from(*value)).sum())
        }
        other => {
            Err(format!("first column of `{sql}` is not an integer column batch: {other:?}").into())
        }
    }
}

fn server_execute_rows(client: &mut Connection, sql: &str) -> BenchResult<u64> {
    match client.execute(sql)? {
        ExecuteResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
        ExecuteResult::Cursor(cursor) => {
            client.close_cursor(cursor)?;
            Err(format!("expected command result for `{sql}`").into())
        }
    }
}

fn record_page_cache_warmup(
    config: &BenchConfig,
    client: &mut Connection,
    report: &mut BenchReport,
    phase: &str,
) -> BenchResult<()> {
    let sql = if config.page_cache_level == 0 {
        "PRAGMA PAGE_CACHE_STATUS".to_string()
    } else {
        format!(
            "PRAGMA PAGE_CACHE_WARMUP_WAIT = {}",
            config.page_cache_wait_millis
        )
    };
    let started = Instant::now();
    let payload = server_query_text(client, &sql)?;
    let elapsed = started.elapsed();
    let status: PageCacheStatusPayload = serde_json::from_str(&payload)?;
    if status.requested_level != config.page_cache_level {
        return Err(format!(
            "page-cache status reported level {}, expected {}",
            status.requested_level, config.page_cache_level
        )
        .into());
    }
    if config.page_cache_level > 0 && report.total_rows > 0 && status.total_generation_bytes == 0 {
        return Err("page-cache warmup found no durable current-generation bytes".into());
    }
    if config.page_cache_level == 0 {
        if status.state != "disabled" || status.warmed_bytes != 0 {
            return Err(format!(
                "page-cache level 0 changed I/O contract: state={} warmed_bytes={}",
                status.state, status.warmed_bytes
            )
            .into());
        }
    } else if status.state != "complete" || status.warmed_bytes != status.target_bytes {
        return Err(format!(
            "page-cache warmup did not reach target: state={} warmed={} target={}",
            status.state, status.warmed_bytes, status.target_bytes
        )
        .into());
    }
    report.page_cache.push(PageCacheMetric {
        phase: phase.to_string(),
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        wait_limit_millis: if config.page_cache_level == 0 {
            0
        } else {
            config.page_cache_wait_millis
        },
        requested_level: status.requested_level,
        state: status.state,
        generation_fingerprint: status.generation_fingerprint,
        total_generation_bytes: status.total_generation_bytes,
        available_memory_bytes: status.available_memory_bytes,
        memory_reserve_bytes: status.memory_reserve_bytes,
        safe_budget_bytes: status.safe_budget_bytes,
        target_bytes: status.target_bytes,
        warmed_bytes: status.warmed_bytes,
        resident_estimate_bytes: status.resident_estimate_bytes,
        worker_duration_millis: status.duration_millis,
        read_bytes_per_second: status.read_bytes_per_second,
        limited_by: status.limited_by,
        last_error: status.last_error,
    });
    Ok(())
}

fn server_query_text(client: &mut Connection, sql: &str) -> BenchResult<String> {
    let cursor = match client.execute(sql)? {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err(format!("expected cursor result for `{sql}`").into())
        }
    };
    let batch = client.fetch(&cursor)?;
    let value = batch
        .rows
        .first()
        .and_then(|row| row.values.first())
        .ok_or_else(|| format!("query returned no rows: {sql}"))?;
    let value = match value {
        WireValue::String(value) | WireValue::Json(value) => value.clone(),
        other => return Err(format!("expected text wire value, got {other:?}").into()),
    };
    if !batch.eof {
        let _ = client.close_cursor(cursor);
    }
    Ok(value)
}

fn server_query_i64(client: &mut Connection, sql: &str) -> BenchResult<i64> {
    let result = client.execute(sql)?;
    server_execute_result_i64(client, result, sql)
}

fn server_prepared_query_i64(
    client: &mut Connection,
    statement: &radixdb_client::PreparedStatement,
    sql: &str,
) -> BenchResult<i64> {
    let result = client.execute_prepared(statement, Vec::new())?;
    server_execute_result_i64(client, result, sql)
}

fn server_execute_result_i64(
    client: &mut Connection,
    result: ExecuteResult,
    sql: &str,
) -> BenchResult<i64> {
    let cursor = match result {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err(format!("expected cursor result for `{sql}`").into())
        }
    };
    let batch = client.fetch(&cursor)?;
    let value = batch
        .rows
        .first()
        .and_then(|row| row.values.first())
        .ok_or_else(|| format!("query returned no rows: {sql}"))?;
    let result = wire_value_as_i64(value)?;
    if !batch.eof {
        let _ = client.close_cursor(cursor);
    }
    Ok(result)
}

fn server_prepared_query_rows_checksum(
    client: &mut Connection,
    statement: &radixdb_client::PreparedStatement,
    sql: &str,
) -> BenchResult<(u64, i128)> {
    let cursor = match client.execute_prepared(statement, Vec::new())? {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err(format!("expected cursor result for `{sql}`").into())
        }
    };
    let mut rows = 0_u64;
    let mut checksum = 0_i128;
    loop {
        let batch = client.fetch(&cursor)?;
        for row in &batch.rows {
            let value = row
                .values
                .first()
                .ok_or_else(|| format!("empty row returned by `{sql}`"))?;
            checksum += wire_value_to_i128(value)
                .ok_or_else(|| format!("first column of `{sql}` is not an integer"))?;
            rows += 1;
        }
        if batch.eof {
            break;
        }
    }
    Ok((rows, checksum))
}

fn server_query_i64_rows(client: &mut Connection, sql: &str) -> BenchResult<Vec<i64>> {
    let cursor = match client.execute(sql)? {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err(format!("expected cursor result for `{sql}`").into())
        }
    };
    let mut values = Vec::new();
    loop {
        let batch = client.fetch(&cursor)?;
        for row in &batch.rows {
            let value = row
                .values
                .first()
                .ok_or_else(|| format!("empty row returned by `{sql}`"))?;
            values.push(wire_value_as_i64(value)?);
        }
        if batch.eof {
            break;
        }
    }
    Ok(values)
}

fn wire_value_as_i64(value: &WireValue) -> BenchResult<i64> {
    match value {
        WireValue::Int(value) => Ok(*value),
        WireValue::Int8(value) => Ok(*value as i64),
        WireValue::Int16(value) => Ok(*value as i64),
        WireValue::Int32(value) => Ok(*value as i64),
        WireValue::UInt(value) => Ok(i64::try_from(*value)?),
        WireValue::UInt8(value) => Ok(*value as i64),
        WireValue::UInt16(value) => Ok(*value as i64),
        WireValue::UInt32(value) => Ok(*value as i64),
        other => Err(format!("expected integer wire value, got {other:?}").into()),
    }
}

fn expect_command(result: ExecuteResult) -> BenchResult<u64> {
    match result {
        ExecuteResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
        ExecuteResult::Cursor(_) => Err("expected command completion, got cursor".into()),
    }
}

fn reset_database_dir(path: &Path, allowed_root: &Path) -> BenchResult<()> {
    let allowed_root = allowed_root
        .canonicalize()
        .unwrap_or_else(|_| allowed_root.to_path_buf());
    if path.exists() {
        let canonical = path.canonicalize()?;
        if !canonical.starts_with(&allowed_root) {
            return Err(format!(
                "refusing to remove database dir outside benchmark root: {}",
                canonical.display()
            )
            .into());
        }
        fs::remove_dir_all(&canonical)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn evict_database_page_cache(root: &Path) -> BenchResult<(u64, u64)> {
    use std::os::fd::AsRawFd;

    let canonical_root = root.canonicalize()?;
    let mut pending = vec![canonical_root.clone()];
    let mut files = 0_u64;
    let mut bytes = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "refusing page-cache eviction through benchmark symlink: {}",
                    path.display()
                )
                .into());
            }
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let canonical = path.canonicalize()?;
            if !canonical.starts_with(&canonical_root) {
                return Err(format!(
                    "benchmark page-cache member escaped database root: {}",
                    canonical.display()
                )
                .into());
            }
            let file = File::open(&canonical)?;
            // SAFETY: the descriptor is open for the duration of the call.
            // This is an advisory, data-preserving benchmark-only operation.
            let result =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            if result != 0 {
                return Err(std::io::Error::from_raw_os_error(result).into());
            }
            files = files.saturating_add(1);
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((files, bytes))
}

#[cfg(not(target_os = "linux"))]
fn evict_database_page_cache(_root: &Path) -> BenchResult<(u64, u64)> {
    Err("--evict-database-page-cache is supported only on Linux".into())
}
