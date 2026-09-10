fn run_postgres(
    config: &BenchConfig,
    layout: &BenchLayout,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    println!("running PostgreSQL participant");
    let started_by_harness = ensure_postgres(config, layout)?;
    let result = run_postgres_owned(config, layout, seed, report);
    if !started_by_harness {
        return result;
    }
    finish_harness_owned_postgres(result, stop_postgres(layout))
}

fn finish_harness_owned_postgres(
    result: BenchResult<()>,
    cleanup: BenchResult<()>,
) -> BenchResult<()> {
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(format!(
            "PostgreSQL participant completed but harness cleanup failed: {cleanup}"
        )
        .into()),
        (Err(primary), Err(cleanup)) => Err(format!(
            "PostgreSQL participant failed: {primary}; harness cleanup also failed: {cleanup}"
        )
        .into()),
    }
}

fn run_postgres_owned(
    config: &BenchConfig,
    layout: &BenchLayout,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    let connection = pg_connection_string(config, "postgres");
    let mut admin = PgClient::connect(&connection, NoTls)?;
    verify_postgres_ownership(config, layout, &mut admin)?;
    let pg_pid = read_postgres_postmaster_pid(layout)?;
    let storage_sampler = StorageSampler::start(
        Participant::Postgres,
        "participant.run",
        layout.pg_root.join("data"),
    );
    let resource_sampler = ResourceSampler::start(
        Participant::Postgres,
        "participant.run",
        ProcessScope::Tree(pg_pid),
        &layout.pg_root,
    );
    admin.batch_execute(&format!("DROP DATABASE IF EXISTS {BENCH_DATABASE}"))?;
    admin.batch_execute(&format!("CREATE DATABASE {BENCH_DATABASE}"))?;
    drop(admin);

    let mut client = PgClient::connect(&pg_connection_string(config, BENCH_DATABASE), NoTls)?;
    report.resolved.postgres.runtime_settings =
        apply_postgres_profile(&mut client, config.pg_profile)?;
    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        client.batch_execute(&create_table_sql(table_index, true))?;
    }
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "schema.create_tables",
        start.elapsed(),
        TABLE_COUNT as u64,
        None,
    ));

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        client.batch_execute(&copy_sql(table_index, Path::new(&seed.artifact_dir)))?;
    }
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "seed.copy_csv",
        start.elapsed(),
        seed.total_rows,
        None,
    ));

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        for sql in index_sql(table_index) {
            client.batch_execute(&sql)?;
        }
    }
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "schema.create_indexes",
        start.elapsed(),
        benchmark_index_count(),
        None,
    ));

    let query_resource_sampler = ResourceSampler::start(
        Participant::Postgres,
        "query.phase",
        ProcessScope::Tree(pg_pid),
        &layout.pg_root,
    );
    let query_result = run_postgres_cases(config, &mut client, seed, report);
    report.resources.push(query_resource_sampler.stop()?);
    query_result?;
    drop(client);

    report.resources.push(resource_sampler.stop()?);
    report.storage.push(storage_sampler.stop()?);
    Ok(())
}

/// SQLite is intentionally an embedded baseline.  It uses the exact same
/// 120-table schema, generated CSV seed and SQL cases as PostgreSQL/RadixDB,
/// but it does not include a server protocol in query timings.
fn run_sqlite(
    _config: &BenchConfig,
    layout: &BenchLayout,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    println!("running SQLite participant (embedded baseline)");
    let database_path = layout.sqlite_root.join("radixdb_bench.sqlite3");
    reset_sqlite_database(&database_path, &layout.sqlite_root)?;
    let storage_sampler = StorageSampler::start(
        Participant::Sqlite,
        "participant.run",
        layout.sqlite_root.clone(),
    );

    let mut connection = SqliteConnection::open(&database_path)?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
    )?;

    let start = Instant::now();
    let schema_sql = (1..=TABLE_COUNT)
        .map(|table_index| create_table_sql(table_index, false))
        .collect::<Vec<_>>()
        .join(";");
    connection.execute_batch(&schema_sql)?;
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "schema.create_tables",
        start.elapsed(),
        TABLE_COUNT as u64,
        None,
    ));

    let start = Instant::now();
    for table_index in 1..=TABLE_COUNT {
        let table = table_name(table_index);
        sqlite_import_csv(
            &database_path,
            &seed_csv_path(Path::new(&seed.artifact_dir), table_index),
            &table,
        )?;
        normalize_sqlite_imported_booleans(&connection, &table)?;
    }
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "seed.import_csv",
        start.elapsed(),
        seed.total_rows,
        None,
    ));

    let start = Instant::now();
    let index_sql = (1..=TABLE_COUNT)
        .flat_map(index_sql)
        .collect::<Vec<_>>()
        .join(";");
    connection.execute_batch(&index_sql)?;
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "schema.create_indexes",
        start.elapsed(),
        benchmark_index_count(),
        None,
    ));

    run_sqlite_cases(&mut connection, seed, report)?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(connection);
    report.storage.push(storage_sampler.stop()?);
    Ok(())
}

fn normalize_sqlite_imported_booleans(
    connection: &SqliteConnection,
    table: &str,
) -> BenchResult<()> {
    connection.execute(
        &format!(
            "UPDATE {table} SET active = CASE active \
             WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE active END"
        ),
        [],
    )?;
    Ok(())
}

fn validate_reference_differential(config: &BenchConfig, report: &BenchReport) -> BenchResult<()> {
    if config.participants != Participant::all() {
        return Ok(());
    }

    for case in [
        "reference.navigation",
        "reference.direct.explicit",
        "reference.fact_dictionary",
        "reference.fact_first",
        "reference.target_first",
    ] {
        let mut expected: Option<(&str, u64, Option<&str>)> = None;
        for participant in Participant::all() {
            let metric = report
                .metrics
                .iter()
                .find(|metric| metric.participant == participant.as_str() && metric.case == case)
                .ok_or_else(|| {
                    format!(
                        "reference differential is missing {case} for {}",
                        participant.as_str()
                    )
                })?;
            let actual = (metric.rows, metric.checksum.as_deref());
            if let Some((expected_participant, expected_rows, expected_checksum)) = expected {
                if actual != (expected_rows, expected_checksum) {
                    return Err(format!(
                        "reference differential mismatch for {case}: \
                         {expected_participant}={expected_rows}:{expected_checksum:?}, \
                         {}={}:{:?}",
                        participant.as_str(),
                        metric.rows,
                        metric.checksum
                    )
                    .into());
                }
            } else {
                expected = Some((
                    participant.as_str(),
                    metric.rows,
                    metric.checksum.as_deref(),
                ));
            }
        }
    }
    Ok(())
}

fn reset_sqlite_database(path: &Path, allowed_root: &Path) -> BenchResult<()> {
    let allowed_root = allowed_root
        .canonicalize()
        .unwrap_or_else(|_| allowed_root.to_path_buf());
    for suffix in ["", "-wal", "-shm"] {
        let candidate = PathBuf::from(format!("{}{}", path.display(), suffix));
        if !candidate.exists() {
            continue;
        }
        let canonical = candidate.canonicalize()?;
        if !canonical.starts_with(&allowed_root) {
            return Err(format!(
                "refusing to remove SQLite database outside benchmark root: {}",
                canonical.display()
            )
            .into());
        }
        fs::remove_file(canonical)?;
    }
    Ok(())
}

fn sqlite_import_csv(database_path: &Path, csv_path: &Path, table: &str) -> BenchResult<()> {
    let import = format!(".import --csv --skip 1 {} {table}", csv_path.display());
    let status = Command::new("sqlite3")
        .arg(database_path)
        .arg("-cmd")
        .arg(".output /dev/null")
        .arg("-cmd")
        .arg(import)
        .arg("SELECT 1;")
        .status()?;
    if !status.success() {
        return Err(format!("sqlite3 CSV import failed for {table}").into());
    }
    Ok(())
}

fn run_sqlite_cases(
    connection: &mut SqliteConnection,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    let start = Instant::now();
    let mut total_rows = 0_i64;
    let mut amount_sum = 0_i128;
    for table_index in 1..=TABLE_COUNT {
        let table = table_name(table_index);
        total_rows += sqlite_query_i64(connection, &format!("SELECT COUNT(*) FROM {table}"))?;
        amount_sum +=
            sqlite_query_i64(connection, &format!("SELECT SUM(amount) FROM {table}"))? as i128;
    }
    if total_rows as u64 != seed.total_rows || amount_sum != seed.expected_amount_sum {
        return Err(format!(
            "SQLite checksum mismatch: rows={total_rows}, amount_sum={amount_sum}"
        )
        .into());
    }
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "correctness.seed_checksum",
        start.elapsed(),
        seed.total_rows,
        Some(format!("{total_rows}:{amount_sum}")),
    ));

    let table = table_name(60);
    let selected_id = rows_for_table(seed.total_rows, 60) / 2;
    let range_end = selected_id.saturating_add(999);
    measure_sqlite_query_count(
        connection,
        report,
        "select.pk",
        &format!("SELECT COUNT(*) FROM {table} WHERE id = {selected_id}"),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "select.range",
        &format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN {selected_id} AND {range_end}"),
    )?;
    measure_sqlite_query_rows(
        connection,
        report,
        "scan.full",
        &format!("SELECT * FROM {table}"),
    )?;
    measure_sqlite_query_rows(
        connection,
        report,
        "scan.projected",
        &format!("SELECT id, amount FROM {table}"),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "aggregate.group_having",
        &format!(
            "SELECT COUNT(*) FROM (SELECT bucket, COUNT(*) c, SUM(amount) s FROM {table} GROUP BY bucket HAVING COUNT(*) > 0) q"
        ),
    )?;
    measure_sqlite_query_count(connection, report, "join.parent", &join_parent_sql())?;
    measure_sqlite_query_count(
        connection,
        report,
        "reference.navigation",
        &reference_direct_join_sql(),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "reference.direct.explicit",
        &reference_direct_join_sql(),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "reference.fact_dictionary",
        &reference_fact_first_join_sql(),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "reference.fact_first",
        &reference_fact_first_join_sql(),
    )?;
    measure_sqlite_query_count(
        connection,
        report,
        "reference.target_first",
        &reference_target_first_join_sql(),
    )?;

    let start = Instant::now();
    connection.execute_batch("BEGIN")?;
    let updated = connection.execute(
        &format!(
            "UPDATE {} SET amount = amount + 1 WHERE bucket = 17",
            table_name(42)
        ),
        [],
    )?;
    connection.execute_batch("ROLLBACK")?;
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "update.rollback",
        start.elapsed(),
        updated as u64,
        None,
    ));

    let start = Instant::now();
    connection.execute_batch("BEGIN")?;
    let deleted = connection.execute(
        &format!("DELETE FROM {} WHERE bucket = 18", table_name(43)),
        [],
    )?;
    connection.execute_batch("ROLLBACK")?;
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        "delete.rollback",
        start.elapsed(),
        deleted as u64,
        None,
    ));
    Ok(())
}

fn measure_sqlite_query_count(
    connection: &SqliteConnection,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
) -> BenchResult<()> {
    let start = Instant::now();
    let count = sqlite_query_i64(connection, sql)?;
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        case,
        start.elapsed(),
        count.max(0) as u64,
        Some(count.to_string()),
    ));
    Ok(())
}

fn measure_sqlite_query_rows(
    connection: &SqliteConnection,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
) -> BenchResult<()> {
    let start = Instant::now();
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    let mut row_count = 0_u64;
    let mut checksum = 0_i128;
    while let Some(row) = rows.next()? {
        checksum += i128::from(row.get::<_, i64>(0)?);
        row_count += 1;
    }
    report.metrics.push(Metric::measured(
        Participant::Sqlite,
        case,
        start.elapsed(),
        row_count,
        Some(format!("{row_count}:{checksum}")),
    ));
    Ok(())
}

fn sqlite_query_i64(connection: &SqliteConnection, sql: &str) -> BenchResult<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn pg_connection_string(config: &BenchConfig, database: &str) -> String {
    let mut connection = format!(
        "host=127.0.0.1 port={} user=postgres dbname={database}",
        config.pg_port
    );
    if let Some(password) = config.pg_password.as_deref() {
        connection.push_str(" password=");
        connection.push_str(password);
    }
    connection
}

fn ensure_postgres(config: &BenchConfig, layout: &BenchLayout) -> BenchResult<bool> {
    ensure_postgres_with_timeout(config, layout, Duration::from_secs(20))
}

fn ensure_postgres_with_timeout(
    config: &BenchConfig,
    layout: &BenchLayout,
    ready_timeout: Duration,
) -> BenchResult<bool> {
    load_postgres_ownership_marker(config, layout)?;
    if PgClient::connect(&pg_connection_string(config, "postgres"), NoTls).is_ok() {
        return Ok(false);
    }

    let status = Command::new(layout.pg_root.join("start.sh")).status()?;
    if !status.success() {
        let cleanup = stop_postgres(layout);
        return match cleanup {
            Ok(()) => Err("PG/start.sh failed; cleanup completed".into()),
            Err(cleanup) => {
                Err(format!("PG/start.sh failed; cleanup also failed: {cleanup}").into())
            }
        };
    }
    let deadline = Instant::now() + ready_timeout;
    while Instant::now() < deadline {
        if PgClient::connect(&pg_connection_string(config, "postgres"), NoTls).is_ok() {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(200));
    }
    let cleanup = stop_postgres(layout);
    match cleanup {
        Ok(()) => {
            Err("PostgreSQL did not become ready on benchmark port; cleanup completed".into())
        }
        Err(cleanup) => Err(format!(
            "PostgreSQL did not become ready on benchmark port; cleanup also failed: {cleanup}"
        )
        .into()),
    }
}

fn load_postgres_ownership_marker(
    config: &BenchConfig,
    layout: &BenchLayout,
) -> BenchResult<PostgresOwnershipMarker> {
    let marker_path = layout.pg_root.join(POSTGRES_OWNER_MARKER);
    let marker: PostgresOwnershipMarker =
        serde_json::from_str(&fs::read_to_string(&marker_path).map_err(|error| {
            format!(
                "PostgreSQL benchmark ownership marker is required at {}: {error}",
                marker_path.display()
            )
        })?)?;
    if marker.format != POSTGRES_OWNER_FORMAT {
        return Err(format!(
            "unsupported PostgreSQL ownership marker format `{}`",
            marker.format
        )
        .into());
    }
    let expected_data_dir = layout.pg_root.join("data").canonicalize()?;
    let marker_data_dir = marker.data_dir.canonicalize()?;
    if marker_data_dir != expected_data_dir || marker.port != config.pg_port {
        return Err(format!(
            "PostgreSQL ownership marker does not match benchmark root/port: marker={} port={}, expected={} port={}",
            marker_data_dir.display(),
            marker.port,
            expected_data_dir.display(),
            config.pg_port
        )
        .into());
    }
    if marker.system_identifier.is_empty()
        || !marker
            .system_identifier
            .bytes()
            .all(|byte| byte.is_ascii_digit())
    {
        return Err("PostgreSQL ownership marker has an invalid system identifier".into());
    }
    Ok(marker)
}

fn verify_postgres_ownership(
    config: &BenchConfig,
    layout: &BenchLayout,
    client: &mut PgClient,
) -> BenchResult<()> {
    let marker = load_postgres_ownership_marker(config, layout)?;
    let row = client.query_one(
        "SELECT current_setting('data_directory'), current_setting('port'), system_identifier::text FROM pg_control_system()",
        &[],
    )?;
    let actual_data_dir = PathBuf::from(row.get::<_, String>(0)).canonicalize()?;
    let actual_port = row.get::<_, String>(1).parse::<u16>()?;
    let actual_system_identifier = row.get::<_, String>(2);
    let expected_data_dir = layout.pg_root.join("data").canonicalize()?;
    if actual_data_dir != expected_data_dir
        || actual_port != config.pg_port
        || actual_system_identifier != marker.system_identifier
    {
        return Err(format!(
            "refusing destructive PostgreSQL benchmark setup: endpoint identity data_dir={} port={} system_identifier={} does not match owned cluster data_dir={} port={} system_identifier={}",
            actual_data_dir.display(),
            actual_port,
            actual_system_identifier,
            expected_data_dir.display(),
            marker.port,
            marker.system_identifier
        )
        .into());
    }
    Ok(())
}

fn stop_postgres(layout: &BenchLayout) -> BenchResult<()> {
    let status = Command::new(layout.pg_root.join("stop.sh")).status()?;
    if !status.success() {
        return Err("PG/stop.sh failed".into());
    }
    Ok(())
}

fn read_postgres_postmaster_pid(layout: &BenchLayout) -> BenchResult<i32> {
    let path = layout.pg_root.join("data/postmaster.pid");
    let content = fs::read_to_string(&path)?;
    let pid = content
        .lines()
        .next()
        .ok_or_else(|| format!("empty PostgreSQL postmaster pid file: {}", path.display()))?
        .trim()
        .parse()?;
    Ok(pid)
}

fn run_postgres_cases(
    config: &BenchConfig,
    client: &mut PgClient,
    seed: &SeedManifest,
    report: &mut BenchReport,
) -> BenchResult<()> {
    if let Some(case) = config.only_case {
        return run_postgres_isolated_case(config, client, seed, report, case);
    }

    let start = Instant::now();
    let mut total_rows = 0_i64;
    let mut amount_sum = 0_i128;
    for table_index in 1..=TABLE_COUNT {
        let table = table_name(table_index);
        total_rows += client
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])?
            .get::<_, i64>(0);
        amount_sum += client
            .query_one(&format!("SELECT SUM(amount) FROM {table}"), &[])?
            .get::<_, i64>(0) as i128;
    }
    if total_rows as u64 != seed.total_rows || amount_sum != seed.expected_amount_sum {
        return Err(format!(
            "postgres checksum mismatch: rows={total_rows}, amount_sum={amount_sum}"
        )
        .into());
    }
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "correctness.seed_checksum",
        start.elapsed(),
        seed.total_rows,
        Some(format!("{total_rows}:{amount_sum}")),
    ));

    let table = table_name(60);
    let selected_id = rows_for_table(seed.total_rows, 60) / 2;
    let range_end = selected_id.saturating_add(999);
    measure_pg_query_count(
        client,
        report,
        "select.pk",
        &format!("SELECT COUNT(*) FROM {table} WHERE id = {selected_id}"),
    )?;
    measure_pg_query_count(
        client,
        report,
        "select.range",
        &format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN {selected_id} AND {range_end}"),
    )?;
    measure_pg_query_rows(
        client,
        report,
        "scan.full",
        &format!("SELECT * FROM {table}"),
    )?;
    measure_pg_query_rows(
        client,
        report,
        "scan.projected",
        &format!("SELECT id, amount FROM {table}"),
    )?;
    measure_pg_query_count(
        client,
        report,
        "aggregate.group_having",
        &format!(
            "SELECT COUNT(*) FROM (SELECT bucket, COUNT(*) c, SUM(amount) s FROM {table} GROUP BY bucket HAVING COUNT(*) > 0) q"
        ),
    )?;
    measure_pg_query_count(client, report, "join.parent", &join_parent_sql())?;
    measure_pg_query_count(
        client,
        report,
        "reference.navigation",
        &reference_direct_join_sql(),
    )?;
    measure_pg_query_count(
        client,
        report,
        "reference.direct.explicit",
        &reference_direct_join_sql(),
    )?;
    measure_pg_query_count(
        client,
        report,
        "reference.fact_dictionary",
        &reference_fact_first_join_sql(),
    )?;
    measure_pg_query_count(
        client,
        report,
        "reference.fact_first",
        &reference_fact_first_join_sql(),
    )?;
    measure_pg_query_count(
        client,
        report,
        "reference.target_first",
        &reference_target_first_join_sql(),
    )?;

    let start = Instant::now();
    client.batch_execute("BEGIN")?;
    let updated = client.execute(
        &format!(
            "UPDATE {} SET amount = amount + 1 WHERE bucket = 17",
            table_name(42)
        ),
        &[],
    )?;
    client.batch_execute("ROLLBACK")?;
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "update.rollback",
        start.elapsed(),
        updated,
        None,
    ));

    let start = Instant::now();
    client.batch_execute("BEGIN")?;
    let deleted = client.execute(
        &format!("DELETE FROM {} WHERE bucket = 18", table_name(43)),
        &[],
    )?;
    client.batch_execute("ROLLBACK")?;
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        "delete.rollback",
        start.elapsed(),
        deleted,
        None,
    ));
    Ok(())
}

fn run_postgres_isolated_case(
    config: &BenchConfig,
    client: &mut PgClient,
    seed: &SeedManifest,
    report: &mut BenchReport,
    case: QueryCase,
) -> BenchResult<()> {
    match case {
        QueryCase::JoinParent => {
            let sql = join_parent_sql();
            let expected_count = expected_join_parent_count(seed.total_rows);
            let mut execute = || Ok(client.query_one(&sql, &[])?.get::<_, i64>(0));
            let warmup_ms = measure_count_iterations(
                case.as_str(),
                "warm-up",
                config.warmup_runs,
                expected_count,
                &mut execute,
            )?;
            let measured_ms = measure_count_iterations(
                case.as_str(),
                "measured",
                config.repeat_runs,
                expected_count,
                &mut execute,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Postgres,
                case.as_str(),
                expected_count,
                Some(expected_count.to_string()),
                summary,
            ));
            Ok(())
        }
        QueryCase::PkMembership => {
            Err("pk.membership is a RadixDB storage gate and has no PostgreSQL participant".into())
        }
        QueryCase::JoinParentSweep => Err(
            "join.parent.sweep is a RadixDB server-only gate and has no PostgreSQL participant"
                .into(),
        ),
        QueryCase::JoinParentDistribution => Err(
            "join.parent.distribution is a RadixDB server-only gate and has no PostgreSQL participant"
                .into(),
        ),
        QueryCase::DeleteRollback => {
            let expected_rows = rows_for_table(seed.total_rows, 43);
            let expected_deleted = expected_table_bucket_count(seed.total_rows, 43, 18);
            let expected_amount_sum = expected_table_amount_sum(seed.total_rows, 43);
            let warmup_ms = measure_pg_delete_rollback_iterations(
                client,
                "warmup",
                config.warmup_runs,
                expected_deleted,
                expected_rows,
                expected_amount_sum,
            )?;
            let measured_ms = measure_pg_delete_rollback_iterations(
                client,
                "measured",
                config.repeat_runs,
                expected_deleted,
                expected_rows,
                expected_amount_sum,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Postgres,
                case.as_str(),
                expected_deleted,
                Some(format!("{expected_rows}:{expected_amount_sum}")),
                summary,
            ));
            Ok(())
        }
        QueryCase::UpdateRollback => {
            Err("update.rollback isolated repeat is a RadixDB server-only gate".into())
        }
        QueryCase::ReferenceNavigation => {
            let sql = reference_direct_join_sql();
            let expected_count = expected_join_parent_count(seed.total_rows);
            let mut execute = || Ok(client.query_one(&sql, &[])?.get::<_, i64>(0));
            let warmup_ms = measure_count_iterations(
                case.as_str(),
                "warm-up",
                config.warmup_runs,
                expected_count,
                &mut execute,
            )?;
            let measured_ms = measure_count_iterations(
                case.as_str(),
                "measured",
                config.repeat_runs,
                expected_count,
                &mut execute,
            )?;
            let summary =
                RepeatSummary::new(CaseProfile::for_config(config), warmup_ms, measured_ms)?;
            report.metrics.push(Metric::repeated(
                Participant::Postgres,
                case.as_str(),
                expected_count,
                Some(expected_count.to_string()),
                summary,
            ));
            Ok(())
        }
        QueryCase::ReferenceDirectExplicit
        | QueryCase::ReferenceFactDictionary
        | QueryCase::ReferenceFactFirst
        | QueryCase::ReferenceTargetFirst
        | QueryCase::ReferenceProjectionNavigation
        | QueryCase::ReferenceProjectionExplicit
        | QueryCase::ReferenceProjectionTransitive
        | QueryCase::ReferenceProjectionTransitiveExplicit => Err(format!(
            "{} is a RadixDB server-only isolated comparison case",
            case.as_str()
        )
        .into()),
    }
}

fn measure_pg_query_count(
    client: &mut PgClient,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
) -> BenchResult<()> {
    let start = Instant::now();
    let count: i64 = client.query_one(sql, &[])?.get(0);
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        case,
        start.elapsed(),
        count.max(0) as u64,
        Some(count.to_string()),
    ));
    Ok(())
}

fn measure_pg_query_rows(
    client: &mut PgClient,
    report: &mut BenchReport,
    case: &str,
    sql: &str,
) -> BenchResult<()> {
    let start = Instant::now();
    let rows = client.query(sql, &[])?;
    let mut checksum = 0_i128;
    for row in &rows {
        checksum += i128::from(row.get::<_, i32>(0));
    }
    report.metrics.push(Metric::measured(
        Participant::Postgres,
        case,
        start.elapsed(),
        rows.len() as u64,
        Some(format!("{}:{checksum}", rows.len())),
    ));
    Ok(())
}

