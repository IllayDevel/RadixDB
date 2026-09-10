fn measure_seed_generation(config: &BenchConfig, seed_dir: &Path) -> BenchResult<SeedManifest> {
    if let Some(seed) = try_reuse_seed(config, seed_dir)? {
        println!("reusing seed artifact: {}", seed_dir.display());
        return Ok(seed);
    }

    if seed_dir.exists() {
        fs::remove_dir_all(seed_dir)?;
    }
    fs::create_dir_all(seed_dir)?;

    let start = Instant::now();
    let mut generated_csv_bytes = 0_u64;
    let mut expected_amount_sum = 0_i128;
    let mut files = Vec::with_capacity(TABLE_COUNT);

    for table_index in 1..=TABLE_COUNT {
        let rows = rows_for_table(config.scale.total_rows(), table_index);
        let previous_rows = if table_index > 1 {
            rows_for_table(config.scale.total_rows(), table_index - 1)
        } else {
            0
        };
        let path = seed_csv_path(seed_dir, table_index);
        let file = File::create(&path)?;
        let writer = BufWriter::with_capacity(1024 * 1024, file);
        let mut writer = ChecksumWriter::new(writer);
        writeln!(
            writer,
            "id,tenant_id,parent_table,parent_id,kind,bucket,amount,score,active,created_at,payload"
        )?;
        for id in 1..=rows {
            let row = GeneratedRow::new(table_index, id, previous_rows);
            expected_amount_sum += row.amount as i128;
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{:.3},{},{},{}",
                row.id,
                row.tenant_id,
                row.parent_table,
                row.parent_id,
                row.kind,
                row.bucket,
                row.amount,
                row.score,
                row.active,
                row.created_at,
                row.payload
            )?;
        }
        writer.flush()?;
        generated_csv_bytes += writer.bytes_written();
        files.push(SeedFileManifest {
            file_name: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
            rows,
            bytes: writer.bytes_written(),
            checksum_fnv1a64: format!("{:016x}", writer.checksum()),
        });
    }

    let manifest = SeedManifest {
        scale: config.scale.as_str().to_string(),
        total_rows: config.scale.total_rows(),
        table_count: TABLE_COUNT,
        expected_amount_sum,
        generated_csv_bytes,
        elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
        generated: true,
        artifact_dir: seed_dir.display().to_string(),
        files,
    };
    write_seed_manifest(seed_dir, &manifest)?;
    Ok(manifest)
}

fn seed_from_expected_checksum(config: &BenchConfig) -> BenchResult<SeedManifest> {
    let expected = config
        .expected_checksum
        .ok_or("--verify-existing requires --expected-checksum rows:sum")?;
    if expected.rows != config.scale.total_rows() {
        return Err(format!(
            "expected checksum rows {} does not match scale {} rows {}",
            expected.rows,
            config.scale.as_str(),
            config.scale.total_rows()
        )
        .into());
    }
    Ok(SeedManifest {
        scale: config.scale.as_str().to_string(),
        total_rows: expected.rows,
        table_count: TABLE_COUNT,
        expected_amount_sum: expected.amount_sum,
        generated_csv_bytes: 0,
        elapsed_ms: 0.0,
        generated: false,
        artifact_dir: "verify-existing:no-seed".to_string(),
        files: Vec::new(),
    })
}

fn try_reuse_seed(config: &BenchConfig, seed_dir: &Path) -> BenchResult<Option<SeedManifest>> {
    let manifest_path = seed_dir.join(SEED_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Ok(None);
    }

    let mut manifest: SeedManifest = serde_json::from_str(&fs::read_to_string(&manifest_path)?)?;
    let mut reason = None;

    if manifest.scale != config.scale.as_str() {
        reason = Some(format!(
            "manifest scale {} does not match requested {}",
            manifest.scale,
            config.scale.as_str()
        ));
    } else if manifest.total_rows != config.scale.total_rows() {
        reason = Some(format!(
            "manifest total_rows {} does not match requested {}",
            manifest.total_rows,
            config.scale.total_rows()
        ));
    } else if manifest.table_count != TABLE_COUNT {
        reason = Some(format!(
            "manifest table_count {} does not match required {}",
            manifest.table_count, TABLE_COUNT
        ));
    } else if manifest.files.len() != TABLE_COUNT {
        reason = Some(format!(
            "manifest file count {} does not match required {}",
            manifest.files.len(),
            TABLE_COUNT
        ));
    } else {
        for file in &manifest.files {
            let path = seed_dir.join(&file.file_name);
            match checksum_file(&path) {
                Ok((bytes, checksum)) => {
                    let checksum_hex = format!("{checksum:016x}");
                    if bytes != file.bytes {
                        reason = Some(format!(
                            "{} size mismatch: manifest {}, actual {}",
                            file.file_name, file.bytes, bytes
                        ));
                        break;
                    }
                    if checksum_hex != file.checksum_fnv1a64 {
                        reason = Some(format!(
                            "{} checksum mismatch: manifest {}, actual {}",
                            file.file_name, file.checksum_fnv1a64, checksum_hex
                        ));
                        break;
                    }
                }
                Err(error) => {
                    reason = Some(format!("{} cannot be verified: {error}", path.display()));
                    break;
                }
            }
        }
    }

    if let Some(reason) = reason {
        println!(
            "seed artifact mismatch: {reason}; regenerating {}",
            seed_dir.display()
        );
        return Ok(None);
    }

    manifest.generated = false;
    manifest.elapsed_ms = 0.0;
    manifest.artifact_dir = seed_dir.display().to_string();
    Ok(Some(manifest))
}

fn write_seed_manifest(seed_dir: &Path, manifest: &SeedManifest) -> BenchResult<()> {
    let manifest_path = seed_dir.join(SEED_MANIFEST_FILE);
    fs::write(
        manifest_path,
        serde_json::to_vec_pretty(manifest)
            .map_err(|error| format!("failed to encode seed manifest: {error}"))?,
    )?;
    Ok(())
}

fn checksum_file(path: &Path) -> BenchResult<(u64, u64)> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut checksum = FNV1A64_OFFSET;
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        update_fnv1a64(&mut checksum, &buffer[..read]);
        bytes += read as u64;
    }
    Ok((bytes, checksum))
}

struct ChecksumWriter<W> {
    inner: W,
    checksum: u64,
    bytes: u64,
}

impl<W> ChecksumWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            checksum: FNV1A64_OFFSET,
            bytes: 0,
        }
    }

    fn checksum(&self) -> u64 {
        self.checksum
    }

    fn bytes_written(&self) -> u64 {
        self.bytes
    }
}

impl<W: Write> Write for ChecksumWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        update_fnv1a64(&mut self.checksum, &buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn update_fnv1a64(checksum: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *checksum ^= u64::from(*byte);
        *checksum = checksum.wrapping_mul(FNV1A64_PRIME);
    }
}

#[derive(Debug)]
struct GeneratedRow {
    id: u64,
    tenant_id: u32,
    parent_table: usize,
    parent_id: u64,
    kind: u16,
    bucket: u16,
    amount: i64,
    score: f64,
    active: bool,
    created_at: &'static str,
    payload: String,
}

impl GeneratedRow {
    fn new(table_index: usize, id: u64, previous_rows: u64) -> Self {
        let parent_table = table_index.saturating_sub(1);
        let parent_id = if previous_rows == 0 {
            0
        } else {
            ((id - 1) % previous_rows) + 1
        };
        let amount = ((id as i64 * 31 + table_index as i64 * 17) % 1_000_000) + 1;
        Self {
            id,
            tenant_id: (id % 1024) as u32,
            parent_table,
            parent_id,
            kind: (table_index % 32) as u16,
            bucket: (id % 997) as u16,
            amount,
            score: amount as f64 / 100.0,
            active: !(id + table_index as u64).is_multiple_of(5),
            created_at: "2026-01-01T00:00:00Z",
            payload: format!("p{table_index:03}_{}", id % 10_000),
        }
    }
}

fn rows_for_table(total_rows: u64, table_index: usize) -> u64 {
    let base = total_rows / TABLE_COUNT as u64;
    let remainder = total_rows % TABLE_COUNT as u64;
    base + u64::from((table_index as u64) <= remainder)
}

fn table_name(table_index: usize) -> String {
    format!("bench_object_{table_index:03}")
}

fn join_parent_sql() -> String {
    format!(
        "SELECT COUNT(*) FROM {} c JOIN {} p ON c.parent_id = p.id WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20",
        table_name(60),
        table_name(59)
    )
}

fn reference_navigation_sql() -> String {
    format!(
        "SELECT COUNT(c.parent_id.payload) FROM {} c \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20",
        table_name(60)
    )
}

fn reference_direct_join_sql() -> String {
    format!(
        "SELECT COUNT(p.payload) FROM {} c \
         LEFT JOIN {} p ON c.parent_id = p.id \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20",
        table_name(60),
        table_name(59)
    )
}

fn reference_fact_dictionary_sql() -> String {
    format!(
        "SELECT COUNT(*) FROM (\
             SELECT c.parent_id.parent_id.payload AS department, SUM(c.amount) AS total_amount \
             FROM {} c \
             WHERE c.parent_table = 59 AND c.parent_id.active = TRUE \
             GROUP BY c.parent_id.parent_id.payload \
             HAVING SUM(c.amount) > 0\
         ) grouped_payments",
        table_name(60)
    )
}

fn reference_fact_first_join_sql() -> String {
    format!(
        "SELECT COUNT(*) FROM (\
             SELECT d.payload AS department, SUM(c.amount) AS total_amount \
             FROM {} c \
             LEFT JOIN {} e ON c.parent_id = e.id \
             LEFT JOIN {} d ON e.parent_id = d.id \
             WHERE c.parent_table = 59 AND e.active = TRUE \
             GROUP BY d.payload \
             HAVING SUM(c.amount) > 0\
         ) grouped_payments",
        table_name(60),
        table_name(59),
        table_name(58)
    )
}

fn reference_target_first_join_sql() -> String {
    format!(
        "SELECT COUNT(*) FROM (\
             SELECT d.payload AS department, SUM(c.amount) AS total_amount \
             FROM {} e \
             JOIN {} c ON c.parent_id = e.id \
             LEFT JOIN {} d ON e.parent_id = d.id \
             WHERE c.parent_table = 59 AND e.active = TRUE \
             GROUP BY d.payload \
             HAVING SUM(c.amount) > 0\
         ) grouped_payments",
        table_name(59),
        table_name(60),
        table_name(58)
    )
}

fn reference_projection_navigation_sql() -> String {
    format!(
        "SELECT c.id, c.parent_id.payload FROM {} c \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20 ORDER BY c.id",
        table_name(60)
    )
}

fn reference_projection_explicit_sql() -> String {
    format!(
        "SELECT c.id, p.payload FROM {} c \
         LEFT JOIN {} p ON c.parent_id = p.id \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20 ORDER BY c.id",
        table_name(60),
        table_name(59)
    )
}

fn reference_projection_transitive_sql() -> String {
    format!(
        "SELECT c.id, c.parent_id.payload, c.parent_id.parent_id.payload FROM {} c \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20 ORDER BY c.id",
        table_name(60)
    )
}

fn reference_projection_transitive_explicit_sql() -> String {
    format!(
        "SELECT c.id, e.payload, d.payload FROM {} c \
         LEFT JOIN {} e ON c.parent_id = e.id \
         LEFT JOIN {} d ON e.parent_id = d.id \
         WHERE c.parent_table = 59 AND c.bucket BETWEEN 10 AND 20 ORDER BY c.id",
        table_name(60),
        table_name(59),
        table_name(58)
    )
}

fn expected_join_parent_count(total_rows: u64) -> u64 {
    const BUCKET_MODULUS: u64 = 997;
    const FIRST_BUCKET: u64 = 10;
    const LAST_BUCKET: u64 = 20;

    let child_rows = rows_for_table(total_rows, 60);
    let buckets_per_cycle = LAST_BUCKET - FIRST_BUCKET + 1;
    let full_cycles = child_rows / BUCKET_MODULUS;
    let remainder = child_rows % BUCKET_MODULUS;
    let remainder_matches = if remainder < FIRST_BUCKET {
        0
    } else {
        remainder.min(LAST_BUCKET) - FIRST_BUCKET + 1
    };
    full_cycles * buckets_per_cycle + remainder_matches
}

fn expected_reference_fact_group_count(total_rows: u64) -> u64 {
    let department_rows = rows_for_table(total_rows, 58);
    let employee_rows = rows_for_table(total_rows, 59);
    let payment_rows = rows_for_table(total_rows, 60);
    if department_rows == 0 || employee_rows == 0 {
        return 0;
    }

    let mut payloads = HashSet::new();
    for payment_id in 1..=payment_rows {
        let employee_id = ((payment_id - 1) % employee_rows) + 1;
        if (employee_id + 59).is_multiple_of(5) {
            continue;
        }
        let department_id = ((employee_id - 1) % department_rows) + 1;
        payloads.insert(department_id % 10_000);
    }
    payloads.len() as u64
}

fn expected_reference_projection(total_rows: u64) -> (u64, i128) {
    let mut rows = 0_u64;
    let mut id_sum = 0_i128;
    for id in 1..=rows_for_table(total_rows, 60) {
        let bucket = id % 997;
        if (10..=20).contains(&bucket) {
            rows += 1;
            id_sum += i128::from(id);
        }
    }
    (rows, id_sum)
}

fn expected_child_bucket_range_count(total_rows: u64, first_bucket: u64, last_bucket: u64) -> u64 {
    if first_bucket > last_bucket || first_bucket >= 997 {
        return 0;
    }
    let last_bucket = last_bucket.min(996);
    let child_rows = rows_for_table(total_rows, 60);
    let full_cycles = child_rows / 997;
    let remainder = child_rows % 997;
    let per_cycle = last_bucket - first_bucket + 1;
    let remainder_matches = (1..=remainder)
        .filter(|id| {
            let bucket = id % 997;
            bucket >= first_bucket && bucket <= last_bucket
        })
        .count() as u64;
    full_cycles * per_cycle + remainder_matches
}

fn expected_table_bucket_count(total_rows: u64, table_index: usize, bucket: u64) -> u64 {
    const BUCKET_MODULUS: u64 = 997;
    if bucket >= BUCKET_MODULUS {
        return 0;
    }
    let rows = rows_for_table(total_rows, table_index);
    let full_cycles = rows / BUCKET_MODULUS;
    let remainder = rows % BUCKET_MODULUS;
    full_cycles
        + u64::from(if bucket == 0 {
            false
        } else {
            remainder >= bucket
        })
}

fn expected_table_amount_sum(total_rows: u64, table_index: usize) -> i128 {
    let rows = rows_for_table(total_rows, table_index);
    (1..=rows)
        .map(|id| i128::from(((id as i64 * 31 + table_index as i64 * 17) % 1_000_000) + 1))
        .sum()
}

fn measure_count_iterations(
    case: &str,
    phase: &str,
    runs: usize,
    expected_count: u64,
    execute: &mut impl FnMut() -> BenchResult<i64>,
) -> BenchResult<Vec<f64>> {
    let expected_count = i64::try_from(expected_count)?;
    let mut elapsed_ms = Vec::with_capacity(runs);
    for iteration in 1..=runs {
        let start = Instant::now();
        let actual_count = execute()?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if actual_count != expected_count {
            return Err(format!(
                "{case} {phase} iteration {iteration} returned {actual_count}, expected {expected_count}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn measure_server_count_iterations(
    report: &mut BenchReport,
    case: &str,
    phase: &str,
    runs: usize,
    expected_count: u64,
    execute: &mut impl FnMut() -> BenchResult<i64>,
) -> BenchResult<Vec<f64>> {
    let expected_count = i64::try_from(expected_count)?;
    let mut elapsed_ms = Vec::with_capacity(runs);
    for iteration in 1..=runs {
        let engine_before = instrumentation::snapshot();
        let start = Instant::now();
        let measured = execute();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let engine_after = instrumentation::snapshot();
        report.engine.push(EngineMetric {
            participant: Participant::Server.as_str().to_string(),
            phase: format!("case.{case}.{phase}.{iteration}"),
            counters: engine_counter_delta(&engine_after, &engine_before),
        });
        record_engine_snapshot(
            report,
            Participant::Server,
            format!("case.{case}.{phase}.{iteration}.after"),
        );

        let actual_count = measured?;
        if actual_count != expected_count {
            return Err(format!(
                "{case} {phase} iteration {iteration} returned {actual_count}, expected {expected_count}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn measure_server_projection_iterations(
    report: &mut BenchReport,
    case: &str,
    phase: &str,
    runs: usize,
    expected_rows: u64,
    expected_checksum: i128,
    execute: &mut impl FnMut() -> BenchResult<(u64, i128)>,
) -> BenchResult<Vec<f64>> {
    let mut elapsed_ms = Vec::with_capacity(runs);
    for iteration in 1..=runs {
        let engine_before = instrumentation::snapshot();
        let start = Instant::now();
        let measured = execute();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let engine_after = instrumentation::snapshot();
        report.engine.push(EngineMetric {
            participant: Participant::Server.as_str().to_string(),
            phase: format!("case.{case}.{phase}.{iteration}"),
            counters: engine_counter_delta(&engine_after, &engine_before),
        });
        record_engine_snapshot(
            report,
            Participant::Server,
            format!("case.{case}.{phase}.{iteration}.after"),
        );

        let (actual_rows, actual_checksum) = measured?;
        if actual_rows != expected_rows || actual_checksum != expected_checksum {
            return Err(format!(
                "{case} {phase} iteration {iteration} returned \
                 {actual_rows}:{actual_checksum}, expected {expected_rows}:{expected_checksum}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn measure_pg_delete_rollback_iterations(
    client: &mut PgClient,
    phase: &str,
    runs: usize,
    expected_deleted: u64,
    expected_rows: u64,
    expected_amount_sum: i128,
) -> BenchResult<Vec<f64>> {
    let table = table_name(43);
    let delete_sql = format!("DELETE FROM {table} WHERE bucket = 18");
    let verify_sql = format!("SELECT COUNT(*), SUM(amount) FROM {table}");
    let mut elapsed_ms = Vec::with_capacity(runs);

    for iteration in 1..=runs {
        let start = Instant::now();
        client.batch_execute("BEGIN")?;
        let deleted = client.execute(&delete_sql, &[])?;
        client.batch_execute("ROLLBACK")?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if deleted != expected_deleted {
            return Err(format!(
                "delete.rollback {phase} iteration {iteration} affected {deleted} rows, expected {expected_deleted}"
            )
            .into());
        }

        // Correctness verification is deliberately outside the timed window.
        let row = client.query_one(&verify_sql, &[])?;
        let rows = row.get::<_, i64>(0) as u64;
        let amount_sum = i128::from(row.get::<_, i64>(1));
        if rows != expected_rows || amount_sum != expected_amount_sum {
            return Err(format!(
                "delete.rollback {phase} iteration {iteration} changed data after rollback: rows={rows}, amount_sum={amount_sum}; expected {expected_rows}:{expected_amount_sum}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn measure_server_delete_rollback_iterations(
    client: &mut Connection,
    report: &mut BenchReport,
    phase: &str,
    runs: usize,
    expected_deleted: u64,
    expected_rows: u64,
    expected_amount_sum: i128,
) -> BenchResult<Vec<f64>> {
    let case = QueryCase::DeleteRollback.as_str();
    let table = table_name(43);
    let delete_sql = format!("DELETE FROM {table} WHERE bucket = 18");
    let mut elapsed_ms = Vec::with_capacity(runs);

    for iteration in 1..=runs {
        let engine_before = instrumentation::snapshot();
        let start = Instant::now();
        client.begin()?;
        let deleted = server_execute_rows(client, &delete_sql)?;
        client.rollback()?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let engine_after = instrumentation::snapshot();
        report.engine.push(EngineMetric {
            participant: Participant::Server.as_str().to_string(),
            phase: format!("case.{case}.{phase}.{iteration}"),
            counters: engine_counter_delta(&engine_after, &engine_before),
        });
        record_engine_snapshot(
            report,
            Participant::Server,
            format!("case.{case}.{phase}.{iteration}.after"),
        );

        if deleted != expected_deleted {
            return Err(format!(
                "delete.rollback {phase} iteration {iteration} affected {deleted} rows, expected {expected_deleted}"
            )
            .into());
        }
        // Correctness verification is deliberately outside the timed/counter
        // window so it cannot improve or penalize the DML result.
        let rows = server_query_i64(client, &format!("SELECT COUNT(*) FROM {table}"))? as u64;
        let amount_sum = i128::from(server_query_i64(
            client,
            &format!("SELECT SUM(amount) FROM {table}"),
        )?);
        if rows != expected_rows || amount_sum != expected_amount_sum {
            return Err(format!(
                "delete.rollback {phase} iteration {iteration} changed data after rollback: rows={rows}, amount_sum={amount_sum}; expected {expected_rows}:{expected_amount_sum}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn measure_server_update_rollback_iterations(
    client: &mut Connection,
    report: &mut BenchReport,
    phase: &str,
    runs: usize,
    expected_updated: u64,
    expected_rows: u64,
    expected_amount_sum: i128,
) -> BenchResult<Vec<f64>> {
    let case = QueryCase::UpdateRollback.as_str();
    let table = table_name(42);
    let update_sql = format!("UPDATE {table} SET amount = amount + 1 WHERE bucket = 17");
    let mut elapsed_ms = Vec::with_capacity(runs);

    for iteration in 1..=runs {
        let engine_before = instrumentation::snapshot();
        let start = Instant::now();
        client.begin()?;
        let updated = server_execute_rows(client, &update_sql)?;
        client.rollback()?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let engine_after = instrumentation::snapshot();
        report.engine.push(EngineMetric {
            participant: Participant::Server.as_str().to_string(),
            phase: format!("case.{case}.{phase}.{iteration}"),
            counters: engine_counter_delta(&engine_after, &engine_before),
        });
        record_engine_snapshot(
            report,
            Participant::Server,
            format!("case.{case}.{phase}.{iteration}.after"),
        );

        if updated != expected_updated {
            return Err(format!(
                "update.rollback {phase} iteration {iteration} affected {updated} rows, expected {expected_updated}"
            )
            .into());
        }
        // Correctness verification is deliberately outside the timed/counter
        // window so it cannot improve or penalize the DML result.
        let rows = server_query_i64(client, &format!("SELECT COUNT(*) FROM {table}"))? as u64;
        let amount_sum = i128::from(server_query_i64(
            client,
            &format!("SELECT SUM(amount) FROM {table}"),
        )?);
        if rows != expected_rows || amount_sum != expected_amount_sum {
            return Err(format!(
                "update.rollback {phase} iteration {iteration} changed data after rollback: rows={rows}, amount_sum={amount_sum}; expected {expected_rows}:{expected_amount_sum}"
            )
            .into());
        }
        elapsed_ms.push(elapsed);
    }
    Ok(elapsed_ms)
}

fn seed_csv_path(seed_dir: &Path, table_index: usize) -> PathBuf {
    seed_dir.join(format!("{}.csv", table_name(table_index)))
}

fn create_table_sql(table_index: usize, postgres: bool) -> String {
    let table = table_name(table_index);
    let float_type = if postgres {
        "DOUBLE PRECISION"
    } else {
        "FLOAT"
    };
    let parent_reference = match table_index {
        59 | 60 => format!(" REFERENCES {}(id)", table_name(table_index - 1)),
        _ => String::new(),
    };
    format!(
        "CREATE TABLE {table} (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            parent_table INTEGER NOT NULL,
            parent_id INTEGER NOT NULL{parent_reference},
            kind INTEGER NOT NULL,
            bucket INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            score {float_type} NOT NULL,
            active BOOLEAN NOT NULL,
            created_at TEXT NOT NULL,
            payload TEXT NOT NULL
        )"
    )
}

fn index_sql(table_index: usize) -> Vec<String> {
    let table = table_name(table_index);
    let mut indexes = vec![format!(
        "CREATE INDEX idx_{table}_tenant ON {table}(tenant_id)"
    )];
    if !matches!(table_index, 59 | 60) {
        indexes.push(format!(
            "CREATE INDEX idx_{table}_parent ON {table}(parent_id)"
        ));
    }
    indexes.push(format!(
        "CREATE INDEX idx_{table}_bucket ON {table}(bucket)"
    ));
    indexes
}

fn benchmark_index_count() -> u64 {
    (1..=TABLE_COUNT)
        .map(|table_index| index_sql(table_index).len() as u64)
        .sum()
}

fn copy_sql(table_index: usize, seed_dir: &Path) -> String {
    copy_file_sql(
        &table_name(table_index),
        &seed_csv_path(seed_dir, table_index),
    )
}

fn copy_file_sql(table: &str, path: &Path) -> String {
    format!(
        "COPY {table} FROM '{}' WITH (FORMAT CSV, HEADER true)",
        quote_sql_literal(&path.display().to_string())
    )
}

fn quote_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn apply_postgres_profile(
    client: &mut PgClient,
    profile: PgBenchProfile,
) -> BenchResult<Vec<PgRuntimeSetting>> {
    for setting in profile.settings() {
        if setting.apply == "session" {
            let value = quote_sql_literal(setting.value);
            client.batch_execute(&format!("SET {} = '{}'", setting.name, value))?;
        }
    }
    capture_postgres_profile_settings(client, profile)
}

fn capture_postgres_profile_settings(
    client: &mut PgClient,
    profile: PgBenchProfile,
) -> BenchResult<Vec<PgRuntimeSetting>> {
    let mut snapshots = Vec::with_capacity(profile.settings().len());
    for setting in profile.settings() {
        if let Some(row) = client.query_opt(
            "SELECT setting, unit, context, source FROM pg_settings WHERE name = $1",
            &[&setting.name],
        )? {
            let unit: Option<String> = row.get(1);
            snapshots.push(PgRuntimeSetting {
                name: setting.name.to_string(),
                requested: (setting.value != "<server default>").then(|| setting.value.to_string()),
                actual: row.get(0),
                unit,
                context: row.get(2),
                source: row.get(3),
            });
        } else {
            snapshots.push(PgRuntimeSetting {
                name: setting.name.to_string(),
                requested: (setting.value != "<server default>").then(|| setting.value.to_string()),
                actual: "<missing>".to_string(),
                unit: None,
                context: "<missing>".to_string(),
                source: "<missing>".to_string(),
            });
        }
    }
    Ok(snapshots)
}

