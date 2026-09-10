fn main() {
    if let Err(error) = run() {
        eprintln!("radixdb-bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() == 2 && argv[1] == "--version" {
        println!("{}", radixdb::server::version_line("radixdb-bench"));
        return Ok(());
    }
    let config = BenchConfig::parse(argv.iter().skip(1).cloned())?;
    let layout = BenchLayout::new(&config)?;
    layout.ensure(&config.participants)?;
    if config.participants.contains(&Participant::Server) {
        write_server_config(&config, &layout, config.server_port)?;
    }

    if config.scale.requires_large_opt_in() && !config.allow_large {
        return Err(format!(
            "scale {} requires --allow-large; use --dry-run to inspect the plan",
            config.scale.as_str()
        )
        .into());
    }

    let run_id = config.run_id.clone().unwrap_or_else(make_run_id);
    validate_run_id(&run_id)?;
    let results_dir = layout.results_dir.canonicalize()?;
    let run_dir = results_dir.join(&run_id);
    fs::create_dir(&run_dir).map_err(|error| {
        format!(
            "cannot create exclusive benchmark run directory {}: {error}",
            run_dir.display()
        )
    })?;
    let canonical_run_dir = run_dir.canonicalize()?;
    if canonical_run_dir.parent() != Some(results_dir.as_path()) {
        return Err(format!(
            "benchmark run directory escaped results root: {}",
            canonical_run_dir.display()
        )
        .into());
    }

    let mut report = BenchReport::new(&config, &layout, &run_id);
    write_environment(&run_dir, &config, &layout, &argv)?;

    println!("RadixDB benchmark harness");
    println!("run_id      : {run_id}");
    println!("scale       : {}", config.scale.as_str());
    println!("total rows  : {}", config.scale.total_rows());
    println!("participants: {:?}", config.participants);
    println!(
        "mode        : {}",
        if config.run { "run" } else { "dry-run" }
    );
    println!("verify only : {}", config.verify_existing);
    println!(
        "only case   : {}",
        config.only_case.map(QueryCase::as_str).unwrap_or("all")
    );
    println!("pg profile  : {}", config.pg_profile.as_str());
    if config.only_case.is_some() {
        println!(
            "case runs   : {} warm-up + {} measured",
            config.warmup_runs, config.repeat_runs
        );
    }
    println!("results     : {}", run_dir.display());

    if !config.run {
        write_report_files(&run_dir, &report)?;
        println!("dry-run complete; pass --run to execute");
        return Ok(());
    }

    let seed_dir = layout.seed_dir(config.scale);
    let seed = if config.verify_existing {
        seed_from_expected_checksum(&config)?
    } else {
        measure_seed_generation(&config, &seed_dir)?
    };
    report.seed = Some(seed.clone());
    write_report_files(&run_dir, &report)?;

    for participant in &config.participants {
        let participant_result = match participant {
            Participant::Postgres => run_postgres(&config, &layout, &seed, &mut report),
            Participant::Sqlite => run_sqlite(&config, &layout, &seed, &mut report),
            Participant::Server if config.verify_existing => {
                run_server_existing(&config, &layout, &seed, &mut report)
            }
            Participant::Server => run_server(&config, &layout, &seed, &mut report),
        };
        write_report_files(&run_dir, &report)?;
        participant_result?;
    }

    validate_reference_differential(&config, &report)?;

    write_report_files(&run_dir, &report)?;
    println!("benchmark finished: {}", run_dir.display());
    Ok(())
}

#[derive(Debug, Clone)]
struct BenchConfig {
    root: PathBuf,
    scale: Scale,
    participants: Vec<Participant>,
    run: bool,
    allow_large: bool,
    keep_seed: bool,
    verify_existing: bool,
    expected_checksum: Option<ExpectedChecksum>,
    only_case: Option<QueryCase>,
    warmup_runs: usize,
    repeat_runs: usize,
    run_id: Option<String>,
    pg_port: u16,
    pg_password: Option<String>,
    pg_profile: PgBenchProfile,
    server_port: u16,
    storage_cpu_workers: usize,
    page_cache_level: u8,
    page_cache_max_bytes: u64,
    page_cache_memory_reserve: u64,
    page_cache_wait_millis: u64,
    evict_database_page_cache: bool,
    target_volume_rows: usize,
    seal_hot_bytes_threshold: usize,
    seal_incremental_hot_bytes_threshold: usize,
    read_queue_depth: usize,
    fetch_mode: BenchFetchMode,
}

impl BenchConfig {
    fn parse(mut args: impl Iterator<Item = String>) -> BenchResult<Self> {
        let mut config = Self {
            root: PathBuf::from(DEFAULT_TEST_ROOT),
            scale: Scale::Dev,
            participants: Participant::all(),
            run: false,
            allow_large: false,
            keep_seed: false,
            verify_existing: false,
            expected_checksum: None,
            only_case: None,
            warmup_runs: DEFAULT_ISOLATED_WARMUP_RUNS,
            repeat_runs: DEFAULT_ISOLATED_MEASURED_RUNS,
            run_id: None,
            pg_port: DEFAULT_PG_PORT,
            pg_password: std::env::var("RADIXDB_BENCH_PG_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
            pg_profile: PgBenchProfile::Default,
            server_port: DEFAULT_RADIXDB_SERVER_PORT,
            storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
            page_cache_level: radixdb::server::default_page_cache_level(),
            page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
            page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
            page_cache_wait_millis: 300_000,
            evict_database_page_cache: false,
            target_volume_rows: radixdb::server::default_target_volume_rows(),
            seal_hot_bytes_threshold: radixdb::server::default_seal_hot_bytes_threshold(),
            seal_incremental_hot_bytes_threshold:
                radixdb::server::default_seal_incremental_hot_bytes_threshold(),
            read_queue_depth: 1,
            fetch_mode: BenchFetchMode::Auto,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--run" => config.run = true,
                "--dry-run" => config.run = false,
                "--allow-large" => config.allow_large = true,
                "--keep-seed" => config.keep_seed = true,
                "--verify-existing" => config.verify_existing = true,
                "--expected-checksum" => {
                    let value = next_value(&mut args, "--expected-checksum")?;
                    config.expected_checksum = Some(ExpectedChecksum::parse(&value)?);
                }
                "--only-case" => {
                    let value = next_value(&mut args, "--only-case")?;
                    config.only_case = Some(QueryCase::parse(&value)?);
                }
                "--warmup" | "--warmups" => {
                    config.warmup_runs = next_value(&mut args, "--warmup")?.parse()?;
                }
                "--repeats" => {
                    config.repeat_runs = next_value(&mut args, "--repeats")?.parse()?;
                }
                "--scale" => {
                    let value = next_value(&mut args, "--scale")?;
                    config.scale = Scale::parse(&value)?;
                }
                "--participant" | "--participants" => {
                    let value = next_value(&mut args, "--participant")?;
                    config.participants = Participant::parse_list(&value)?;
                }
                "--root" => config.root = PathBuf::from(next_value(&mut args, "--root")?),
                "--run-id" => config.run_id = Some(next_value(&mut args, "--run-id")?),
                "--pg-port" => {
                    config.pg_port = next_value(&mut args, "--pg-port")?.parse()?;
                }
                "--pg-password" => {
                    let password = next_value(&mut args, "--pg-password")?;
                    config.pg_password = (!password.is_empty()).then_some(password);
                }
                "--pg-profile" | "--postgres-profile" => {
                    let value = next_value(&mut args, "--pg-profile")?;
                    config.pg_profile = PgBenchProfile::parse(&value)?;
                }
                "--server-port" => {
                    config.server_port = next_value(&mut args, "--server-port")?.parse()?;
                }
                "--storage-cpu-workers" | "--storage_cpu_workers" => {
                    config.storage_cpu_workers =
                        next_value(&mut args, "--storage-cpu-workers")?.parse()?;
                }
                "--page-cache-level" | "--page_cache_level" => {
                    let value = next_value(&mut args, "--page-cache-level")?;
                    config.page_cache_level = value.parse()?;
                    if config.page_cache_level > 10 {
                        return Err(format!(
                            "invalid --page-cache-level `{value}`; expected 0..=10"
                        )
                        .into());
                    }
                }
                "--page-cache-max-bytes" | "--page_cache_max_bytes" => {
                    config.page_cache_max_bytes =
                        next_value(&mut args, "--page-cache-max-bytes")?.parse()?;
                }
                "--page-cache-memory-reserve" | "--page_cache_memory_reserve" => {
                    config.page_cache_memory_reserve =
                        next_value(&mut args, "--page-cache-memory-reserve")?.parse()?;
                }
                "--page-cache-wait-ms" | "--page_cache_wait_ms" => {
                    let value = next_value(&mut args, "--page-cache-wait-ms")?;
                    config.page_cache_wait_millis = value.parse()?;
                    if config.page_cache_wait_millis > 86_400_000 {
                        return Err(format!(
                            "invalid --page-cache-wait-ms `{value}`; expected 0..=86400000"
                        )
                        .into());
                    }
                }
                "--evict-database-page-cache" => config.evict_database_page_cache = true,
                "--target-volume-rows" | "--target_volume_rows" => {
                    config.target_volume_rows = next_value(&mut args, "--target-volume-rows")?
                        .parse::<usize>()?
                        .max(65_536);
                }
                "--seal-hot-bytes-threshold"
                | "--seal-hot-bytes"
                | "--seal_hot_bytes_threshold"
                | "--seal_hot_bytes" => {
                    config.seal_hot_bytes_threshold =
                        next_value(&mut args, "--seal-hot-bytes-threshold")?
                            .parse::<usize>()?
                            .max(1);
                }
                "--seal-incremental-hot-bytes-threshold"
                | "--seal-incremental-hot-bytes"
                | "--seal_incremental_hot_bytes_threshold"
                | "--seal_incremental_hot_bytes" => {
                    config.seal_incremental_hot_bytes_threshold =
                        next_value(&mut args, "--seal-incremental-hot-bytes-threshold")?
                            .parse::<usize>()?
                            .max(1);
                }
                "--read-queue-depth" => {
                    config.read_queue_depth = next_value(&mut args, "--read-queue-depth")?
                        .parse::<usize>()?
                        .max(1);
                }
                "--fetch-mode" => {
                    config.fetch_mode =
                        BenchFetchMode::parse(&next_value(&mut args, "--fetch-mode")?)?;
                }
                "--column-batches" => config.fetch_mode = BenchFetchMode::Columnar,
                "--row-batches" => config.fetch_mode = BenchFetchMode::Rows,
                _ => return Err(format!("unknown argument `{arg}`; use --help").into()),
            }
        }

        if config.participants.is_empty() {
            return Err("at least one participant is required".into());
        }
        if config.only_case.is_some() {
            if config.warmup_runs == 0 {
                return Err("--only-case requires at least one warm-up run".into());
            }
            if config.repeat_runs < DEFAULT_ISOLATED_MEASURED_RUNS {
                return Err(format!(
                    "--only-case requires at least {DEFAULT_ISOLATED_MEASURED_RUNS} measured repeats"
                )
                .into());
            }
        }
        if matches!(
            config.only_case,
            Some(
                QueryCase::PkMembership
                    | QueryCase::JoinParentSweep
                    | QueryCase::JoinParentDistribution
                    | QueryCase::ReferenceDirectExplicit
                    | QueryCase::ReferenceFactDictionary
                    | QueryCase::ReferenceFactFirst
                    | QueryCase::ReferenceTargetFirst
                    | QueryCase::ReferenceProjectionNavigation
                    | QueryCase::ReferenceProjectionExplicit
                    | QueryCase::ReferenceProjectionTransitive
                    | QueryCase::ReferenceProjectionTransitiveExplicit
                    | QueryCase::UpdateRollback,
            )
        ) && config.participants != vec![Participant::Server]
        {
            return Err(
                "this --only-case selection supports only --participant server: pk.membership, join.parent.sweep, join.parent.distribution, update.rollback, or an isolated explicit/transitive reference case"
                    .into(),
            );
        }
        if config.only_case.is_some() && config.participants.contains(&Participant::Sqlite) {
            if config.participants == Participant::all() {
                // Preserve the historical default for isolated cases. SQLite
                // deliberately has only the complete-slice adapter for now.
                config.participants = vec![Participant::Postgres, Participant::Server];
            } else {
                return Err(
                    "SQLite currently participates only in the complete query slice; --only-case is not supported"
                        .into(),
                );
            }
        }
        if config.verify_existing {
            if config.participants != vec![Participant::Server] {
                return Err(
                    "--verify-existing currently supports only --participant server".into(),
                );
            }
            if config.expected_checksum.is_none() {
                return Err("--verify-existing requires --expected-checksum rows:sum".into());
            }
        } else if config.evict_database_page_cache {
            return Err("--evict-database-page-cache requires --verify-existing".into());
        }
        if let Some(run_id) = config.run_id.as_deref() {
            validate_run_id(run_id)?;
        }
        Ok(config)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum QueryCase {
    #[serde(rename = "join.parent")]
    JoinParent,
    #[serde(rename = "join.parent.sweep")]
    JoinParentSweep,
    #[serde(rename = "join.parent.distribution")]
    JoinParentDistribution,
    #[serde(rename = "pk.membership")]
    PkMembership,
    #[serde(rename = "delete.rollback")]
    DeleteRollback,
    #[serde(rename = "update.rollback")]
    UpdateRollback,
    #[serde(rename = "reference.navigation")]
    ReferenceNavigation,
    #[serde(rename = "reference.direct.explicit")]
    ReferenceDirectExplicit,
    #[serde(rename = "reference.fact_dictionary")]
    ReferenceFactDictionary,
    #[serde(rename = "reference.fact_first")]
    ReferenceFactFirst,
    #[serde(rename = "reference.target_first")]
    ReferenceTargetFirst,
    #[serde(rename = "reference.projection.navigation")]
    ReferenceProjectionNavigation,
    #[serde(rename = "reference.projection.explicit")]
    ReferenceProjectionExplicit,
    #[serde(rename = "reference.projection.transitive")]
    ReferenceProjectionTransitive,
    #[serde(rename = "reference.projection.transitive.explicit")]
    ReferenceProjectionTransitiveExplicit,
}

impl QueryCase {
    fn parse(value: &str) -> BenchResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "join.parent" => Ok(Self::JoinParent),
            "join.parent.sweep" => Ok(Self::JoinParentSweep),
            "join.parent.distribution" => Ok(Self::JoinParentDistribution),
            "pk.membership" => Ok(Self::PkMembership),
            "delete.rollback" => Ok(Self::DeleteRollback),
            "update.rollback" => Ok(Self::UpdateRollback),
            "reference.navigation" => Ok(Self::ReferenceNavigation),
            "reference.direct.explicit" => Ok(Self::ReferenceDirectExplicit),
            "reference.fact_dictionary" => Ok(Self::ReferenceFactDictionary),
            "reference.fact_first" => Ok(Self::ReferenceFactFirst),
            "reference.target_first" => Ok(Self::ReferenceTargetFirst),
            "reference.projection.navigation" => Ok(Self::ReferenceProjectionNavigation),
            "reference.projection.explicit" => Ok(Self::ReferenceProjectionExplicit),
            "reference.projection.transitive" => Ok(Self::ReferenceProjectionTransitive),
            "reference.projection.transitive.explicit" => {
                Ok(Self::ReferenceProjectionTransitiveExplicit)
            }
            _ => Err(format!(
                "unsupported isolated case `{value}`; currently supported: join.parent, join.parent.sweep, join.parent.distribution, pk.membership, update.rollback, delete.rollback, reference.navigation, reference.direct.explicit, reference.fact_dictionary, reference.fact_first, reference.target_first, reference.projection.navigation, reference.projection.explicit, reference.projection.transitive, reference.projection.transitive.explicit"
            )
            .into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::JoinParent => "join.parent",
            Self::JoinParentSweep => "join.parent.sweep",
            Self::JoinParentDistribution => "join.parent.distribution",
            Self::PkMembership => "pk.membership",
            Self::DeleteRollback => "delete.rollback",
            Self::UpdateRollback => "update.rollback",
            Self::ReferenceNavigation => "reference.navigation",
            Self::ReferenceDirectExplicit => "reference.direct.explicit",
            Self::ReferenceFactDictionary => "reference.fact_dictionary",
            Self::ReferenceFactFirst => "reference.fact_first",
            Self::ReferenceTargetFirst => "reference.target_first",
            Self::ReferenceProjectionNavigation => "reference.projection.navigation",
            Self::ReferenceProjectionExplicit => "reference.projection.explicit",
            Self::ReferenceProjectionTransitive => "reference.projection.transitive",
            Self::ReferenceProjectionTransitiveExplicit => {
                "reference.projection.transitive.explicit"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CaseProfile {
    FreshHot,
    RestartHot,
}

impl CaseProfile {
    fn for_config(config: &BenchConfig) -> Self {
        if config.verify_existing {
            Self::RestartHot
        } else {
            Self::FreshHot
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::FreshHot => "fresh-hot",
            Self::RestartHot => "restart-hot",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct ExpectedChecksum {
    rows: u64,
    amount_sum: i128,
}

impl ExpectedChecksum {
    fn parse(value: &str) -> BenchResult<Self> {
        let Some((rows, amount_sum)) = value.split_once(':') else {
            return Err("expected checksum format is rows:sum".into());
        };
        Ok(Self {
            rows: rows.parse()?,
            amount_sum: amount_sum.parse()?,
        })
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> BenchResult<String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!(
        r#"Usage:
  radixdb-bench [--dry-run]
  radixdb-bench --run --scale dev --participant server
  radixdb-bench --run --scale 1m --participants postgres,server --keep-seed
  radixdb-bench --run --verify-existing --scale 100m --participant server \
    --expected-checksum 100000000:49734600639880
  radixdb-bench --run --verify-existing --scale 100m --participant server \
    --expected-checksum 100000000:49734600639880 \
    --only-case join.parent --warmup 1 --repeats 3
  radixdb-bench --run --scale 500m --allow-large

Options:
  --version
      Print the frozen build identity used by prerelease candidate preflight.

  --scale dev|20k|1m|10m|100m|250m|500m|1b
      dev is a tiny local development scale: 12_000 rows total.
      20k is the reproducible Linux micro-device minimum-footprint profile.
      Official benchmark scales are 1m, 10m, 100m and 250m.
      500m and 1b remain supported only as manual experimental stress scales.

  --participant postgres|sqlite|server|all
      The fixed full benchmark order is postgres -> sqlite -> server.
      SQLite is an embedded baseline; PostgreSQL and RadixDB are server paths.

  --verify-existing
      Do not reset/schema/seed. Start RadixDB server on existing RD_test/server-data
      and run correctness/query/update-rollback checks. Requires --participant server
      and --expected-checksum rows:sum.

  --only-case join.parent|join.parent.sweep|join.parent.distribution|pk.membership|update.rollback|delete.rollback|reference.navigation
      Run only the selected query case. Fresh runs keep the normal full schema/seed/
      index setup. With --verify-existing the case is dispatched before the global
      checksum so an existing/cold dataset is not warmed by unrelated queries.

  --warmup N
      Warm-up executions before an isolated measured series. Default: 1; minimum: 1.

  --repeats N
      Measured executions for an isolated case. Default and minimum: 5.
      REPORT/results retain raw samples and publish median/min.

  --run / --dry-run
      Default is --dry-run. Big scales never start by accident.

  --allow-large
      Required for 100m, 250m, 500m and 1b.

  --root PATH
      Default: ./RadixTest
      Reusable input CSV seeds are stored under ROOT/seeds/<scale> with
      seed_manifest.json. Valid manifests are reused across benchmark runs.

  --run-id NAME
      One portable filename component below ROOT/results. Existing run
      directories are rejected and never overwritten.

  PostgreSQL safety
      A PostgreSQL participant requires ROOT/PG/BENCHMARK_OWNER.json bound to
      the exact data directory, port and cluster system identifier. Register an
      intentionally dedicated cluster with scripts/register-benchmark-postgres.sh.

  --keep-seed
      Compatibility flag. Seed artifacts are reusable by default and are no
      longer copied into or deleted with each results/<run_id>.

  --pg-password PASSWORD
      Optional PostgreSQL password for benchmark connections. Prefer
      RADIXDB_BENCH_PG_PASSWORD in automation to avoid shell history leaks.
      If omitted, RadixDB bench connects without a password.

  --pg-profile default|nvme-local
      Named PostgreSQL benchmark profile. default only records tracked PG knobs.
      nvme-local applies safe session-level settings and expects the benchmark
      cluster's postgresql.conf to carry restart/reload-level knobs.

  --read-queue-depth N
      RadixDB server artifact-backed physical/read-decode queue depth for benchmark runs.

  --storage-cpu-workers N
      Shared seal/compaction CPU budget. 0 selects all host-visible CPUs;
      positive values are hard limits (use 1 for bounded-cost diagnostics).

  --page-cache-level 0..10
      Proactive current-generation page-cache warmup level. 0 disables warmup;
      10 requests the full generation subject to the safe memory budget.

  --page-cache-max-bytes BYTES
      Hard warmup byte cap. 0 selects the safe automatic cap.

  --page-cache-memory-reserve BYTES
      Memory headroom excluded from warmup. 0 selects the safe automatic reserve.

  --page-cache-wait-ms MILLISECONDS
      Maximum explicit warmup wait before measured queries (default 300000,
      maximum 86400000). Used only when page-cache-level is non-zero.

  --evict-database-page-cache
      Before existing-data verification, issue per-file DONTNEED hints only for
      the dedicated benchmark database. This is an explicit Linux cold-cache gate.

  --fetch-mode auto|rows|columnar
      Cursor transport for full/projected scans. auto is the production default:
      use negotiated ColumnBatchV1 and accept an explicit row fallback. rows is
      the compatibility protocol; columnar requires ColumnBatchV1.

  --column-batches / --row-batches
      Compatibility aliases for --fetch-mode columnar / rows.

  --target-volume-rows ROWS
      RadixDB server target rows per cold volume. Values below 65536 are clamped.

  --seal-hot-bytes-threshold BYTES
      RadixDB server first hot-buffer byte threshold that triggers seal.

  --seal-incremental-hot-bytes-threshold BYTES
      RadixDB server subsequent hot-buffer byte threshold that triggers seal.
"#
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Scale {
    Dev,
    TwentyThousand,
    OneMillion,
    TenMillion,
    HundredMillion,
    TwoHundredFiftyMillion,
    FiveHundredMillion,
    OneBillion,
}

impl Scale {
    fn parse(value: &str) -> BenchResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "dev" | "tiny" => Ok(Self::Dev),
            "20k" | "20000" | "20_000" => Ok(Self::TwentyThousand),
            "1m" | "1000000" | "1_000_000" => Ok(Self::OneMillion),
            "10m" | "10000000" | "10_000_000" => Ok(Self::TenMillion),
            "100m" | "100000000" | "100_000_000" => Ok(Self::HundredMillion),
            "250m" | "250000000" | "250_000_000" => Ok(Self::TwoHundredFiftyMillion),
            "500m" | "500000000" | "500_000_000" => Ok(Self::FiveHundredMillion),
            "1b" | "1000000000" | "1_000_000_000" => Ok(Self::OneBillion),
            _ => Err(format!("unsupported scale `{value}`").into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::TwentyThousand => "20k",
            Self::OneMillion => "1m",
            Self::TenMillion => "10m",
            Self::HundredMillion => "100m",
            Self::TwoHundredFiftyMillion => "250m",
            Self::FiveHundredMillion => "500m",
            Self::OneBillion => "1b",
        }
    }

    fn total_rows(self) -> u64 {
        match self {
            Self::Dev => 12_000,
            Self::TwentyThousand => 20_000,
            Self::OneMillion => 1_000_000,
            Self::TenMillion => 10_000_000,
            Self::HundredMillion => 100_000_000,
            Self::TwoHundredFiftyMillion => 250_000_000,
            Self::FiveHundredMillion => 500_000_000,
            Self::OneBillion => 1_000_000_000,
        }
    }

    fn requires_large_opt_in(self) -> bool {
        matches!(
            self,
            Self::HundredMillion
                | Self::TwoHundredFiftyMillion
                | Self::FiveHundredMillion
                | Self::OneBillion
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Participant {
    Postgres,
    Sqlite,
    Server,
}

impl Participant {
    fn all() -> Vec<Self> {
        vec![Self::Postgres, Self::Sqlite, Self::Server]
    }

    fn parse_list(value: &str) -> BenchResult<Vec<Self>> {
        if value.eq_ignore_ascii_case("all") {
            return Ok(Self::all());
        }

        let mut parsed = Vec::new();
        for part in value.split(',') {
            let participant = match part.trim().to_ascii_lowercase().as_str() {
                "postgres" | "pg" => Self::Postgres,
                "sqlite" => Self::Sqlite,
                "server" | "radixdb-server" => Self::Server,
                other => return Err(format!("unsupported participant `{other}`").into()),
            };
            if !parsed.contains(&participant) {
                parsed.push(participant);
            }
        }

        let order = Self::all();
        parsed.sort_by_key(|participant| {
            order
                .iter()
                .position(|ordered| ordered == participant)
                .unwrap_or(usize::MAX)
        });
        Ok(parsed)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
            Self::Server => "radixdb-server",
        }
    }
}

#[derive(Debug, Clone)]
struct BenchLayout {
    root: PathBuf,
    pg_root: PathBuf,
    sqlite_root: PathBuf,
    rd_root: PathBuf,
    server_data: PathBuf,
    results_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresOwnershipMarker {
    format: String,
    data_dir: PathBuf,
    port: u16,
    system_identifier: String,
}

impl BenchLayout {
    fn new(config: &BenchConfig) -> BenchResult<Self> {
        let root = config
            .root
            .canonicalize()
            .unwrap_or_else(|_| config.root.clone());
        Ok(Self {
            pg_root: root.join("PG"),
            sqlite_root: root.join("SQLite"),
            rd_root: root.join("RD_test"),
            server_data: root.join("RD_test/server-data"),
            results_dir: root.join("results"),
            root,
        })
    }

    fn ensure(&self, participants: &[Participant]) -> BenchResult<()> {
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.root.join("seeds"))?;
        fs::create_dir_all(&self.results_dir)?;
        if participants.contains(&Participant::Server) {
            fs::create_dir_all(&self.rd_root)?;
            fs::create_dir_all(&self.server_data)?;
        }
        if participants.contains(&Participant::Sqlite) {
            fs::create_dir_all(&self.sqlite_root)?;
        }
        if participants.contains(&Participant::Postgres) && !self.pg_root.is_dir() {
            return Err(format!(
                "PostgreSQL test root is missing: {}",
                self.pg_root.display()
            )
            .into());
        }
        Ok(())
    }

    fn seed_dir(&self, scale: Scale) -> PathBuf {
        self.root.join("seeds").join(scale.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeedManifest {
    scale: String,
    total_rows: u64,
    table_count: usize,
    expected_amount_sum: i128,
    generated_csv_bytes: u64,
    elapsed_ms: f64,
    generated: bool,
    artifact_dir: String,
    files: Vec<SeedFileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeedFileManifest {
    file_name: String,
    rows: u64,
    bytes: u64,
    checksum_fnv1a64: String,
}

#[derive(Debug, Serialize)]
struct BenchReport {
    run_id: String,
    scale: Scale,
    total_rows: u64,
    table_count: usize,
    root: String,
    participants: Vec<Participant>,
    dry_run: bool,
    resolved: ResolvedBenchmarkConfig,
    seed: Option<SeedManifest>,
    metrics: Vec<Metric>,
    storage: Vec<StorageMetric>,
    resources: Vec<ResourceMetric>,
    engine: Vec<EngineMetric>,
    client_protocol: Vec<ClientProtocolMetric>,
    engine_snapshots: Vec<EngineSnapshotMetric>,
    access_paths: Vec<AccessPathMetric>,
    membership: Vec<MembershipMetric>,
    readiness: Vec<ReadinessMetric>,
    page_cache: Vec<PageCacheMetric>,
}

impl BenchReport {
    fn new(config: &BenchConfig, layout: &BenchLayout, run_id: &str) -> Self {
        Self {
            run_id: run_id.to_string(),
            scale: config.scale,
            total_rows: config.scale.total_rows(),
            table_count: TABLE_COUNT,
            root: layout.root.display().to_string(),
            participants: config.participants.clone(),
            dry_run: !config.run,
            resolved: ResolvedBenchmarkConfig::new(config, layout),
            seed: None,
            metrics: Vec::new(),
            storage: Vec::new(),
            resources: Vec::new(),
            engine: Vec::new(),
            client_protocol: Vec::new(),
            engine_snapshots: Vec::new(),
            access_paths: Vec::new(),
            membership: Vec::new(),
            readiness: Vec::new(),
            page_cache: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ReadinessMetric {
    participant: String,
    phase: String,
    elapsed_ms: f64,
    status: ServerStatus,
}

#[derive(Debug, Clone, Serialize)]
struct PageCacheMetric {
    phase: String,
    elapsed_ms: f64,
    wait_limit_millis: u64,
    requested_level: u8,
    state: String,
    generation_fingerprint: u64,
    total_generation_bytes: u64,
    available_memory_bytes: u64,
    memory_reserve_bytes: u64,
    safe_budget_bytes: u64,
    target_bytes: u64,
    warmed_bytes: u64,
    resident_estimate_bytes: u64,
    worker_duration_millis: u64,
    read_bytes_per_second: u64,
    limited_by: String,
    last_error: String,
}

#[derive(Debug, Deserialize)]
struct PageCacheStatusPayload {
    state: String,
    requested_level: u8,
    generation_fingerprint: u64,
    total_generation_bytes: u64,
    available_memory_bytes: u64,
    memory_reserve_bytes: u64,
    safe_budget_bytes: u64,
    target_bytes: u64,
    warmed_bytes: u64,
    resident_estimate_bytes: u64,
    duration_millis: u64,
    read_bytes_per_second: u64,
    limited_by: String,
    last_error: String,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedBenchmarkConfig {
    benchmark: ResolvedBenchmarkOptions,
    postgres: ResolvedPostgresConfig,
    sqlite: ResolvedSqliteConfig,
    radixdb_server: ServerConfig,
}

impl ResolvedBenchmarkConfig {
    fn new(config: &BenchConfig, layout: &BenchLayout) -> Self {
        Self {
            benchmark: ResolvedBenchmarkOptions {
                root: layout.root.display().to_string(),
                pg_root: layout.pg_root.display().to_string(),
                rd_root: layout.rd_root.display().to_string(),
                seed_root: layout.root.join("seeds").display().to_string(),
                results_dir: layout.results_dir.display().to_string(),
                scale: config.scale,
                total_rows: config.scale.total_rows(),
                table_count: TABLE_COUNT,
                keep_seed: config.keep_seed,
                verify_existing: config.verify_existing,
                allow_large: config.allow_large,
                only_case: config.only_case,
                warmup_runs: config.warmup_runs,
                repeat_runs: config.repeat_runs,
                fetch_mode: config.fetch_mode,
                page_cache_wait_millis: config.page_cache_wait_millis,
                evict_database_page_cache: config.evict_database_page_cache,
            },
            postgres: ResolvedPostgresConfig {
                port: config.pg_port,
                data_dir: layout.pg_root.join("data").display().to_string(),
                database: BENCH_DATABASE.to_string(),
                profile: config.pg_profile.as_str().to_string(),
                profile_settings: config.pg_profile.settings().to_vec(),
                runtime_settings: Vec::new(),
            },
            sqlite: ResolvedSqliteConfig {
                database_path: layout
                    .sqlite_root
                    .join("radixdb_bench.sqlite3")
                    .display()
                    .to_string(),
                adapter: "rusqlite 0.32 + bundled SQLite; embedded baseline".to_string(),
            },
            radixdb_server: server_config(config, layout),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedBenchmarkOptions {
    root: String,
    pg_root: String,
    rd_root: String,
    seed_root: String,
    results_dir: String,
    scale: Scale,
    total_rows: u64,
    table_count: usize,
    keep_seed: bool,
    verify_existing: bool,
    allow_large: bool,
    only_case: Option<QueryCase>,
    warmup_runs: usize,
    repeat_runs: usize,
    fetch_mode: BenchFetchMode,
    page_cache_wait_millis: u64,
    evict_database_page_cache: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedPostgresConfig {
    port: u16,
    data_dir: String,
    database: String,
    profile: String,
    profile_settings: Vec<PgProfileSetting>,
    runtime_settings: Vec<PgRuntimeSetting>,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedSqliteConfig {
    database_path: String,
    adapter: String,
}

#[derive(Debug, Clone, Serialize)]
struct Metric {
    participant: String,
    case: String,
    elapsed_ms: f64,
    rows: u64,
    rows_per_sec: Option<f64>,
    checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repeat_summary: Option<RepeatSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct EngineMetric {
    participant: String,
    phase: String,
    counters: EngineCountersSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct ClientProtocolMetric {
    participant: String,
    phase: String,
    counters: ClientProtocolCountersSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct EngineSnapshotMetric {
    participant: String,
    phase: String,
    counters: EngineCountersSnapshot,
    diagnostics: Vec<EngineCounterDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
struct EngineCounterDiagnostic {
    level: String,
    code: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct AccessPathMetric {
    participant: String,
    case: String,
    sql: String,
    access_paths: Vec<String>,
    explain_lines: Vec<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RepeatSummary {
    profile: CaseProfile,
    warmup_ms: Vec<f64>,
    measured_ms: Vec<f64>,
    restart_cold_first_ms: Option<f64>,
    median_ms: f64,
    min_ms: f64,
}

/// Direct-storage comparison used to gate the parent PK membership primitive
/// before a higher-level JOIN operator consumes it. The input IDs are obtained
/// through the benchmark's TCP client; only the two storage membership paths
/// are timed here.
#[derive(Debug, Clone, Serialize)]
struct MembershipMetric {
    profile: CaseProfile,
    input_keys: usize,
    expected_hits: usize,
    inner_iterations: usize,
    warmup: Vec<MembershipSample>,
    measured: Vec<MembershipSample>,
    sequential_median_ms: f64,
    batch_median_ms: f64,
    median_batch_over_sequential: f64,
}

#[derive(Debug, Clone, Serialize)]
struct MembershipSample {
    sequential_ms: f64,
    batch_ms: f64,
    batch_over_sequential: f64,
    batch_counters: EngineCountersSnapshot,
}

impl RepeatSummary {
    fn new(profile: CaseProfile, warmup_ms: Vec<f64>, measured_ms: Vec<f64>) -> BenchResult<Self> {
        if warmup_ms.is_empty() {
            return Err("isolated benchmark requires at least one warm-up sample".into());
        }
        if measured_ms.len() < DEFAULT_ISOLATED_MEASURED_RUNS {
            return Err(format!(
                "isolated benchmark requires at least {DEFAULT_ISOLATED_MEASURED_RUNS} measured samples"
            )
            .into());
        }

        let mut sorted = measured_ms.clone();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median_ms = if sorted.len().is_multiple_of(2) {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        };
        let min_ms = sorted[0];
        let restart_cold_first_ms =
            matches!(profile, CaseProfile::RestartHot).then_some(warmup_ms[0]);
        Ok(Self {
            profile,
            warmup_ms,
            measured_ms,
            restart_cold_first_ms,
            median_ms,
            min_ms,
        })
    }
}

impl Metric {
    fn measured(
        participant: Participant,
        case: impl Into<String>,
        elapsed: Duration,
        rows: u64,
        checksum: Option<String>,
    ) -> Self {
        let elapsed_secs = elapsed.as_secs_f64();
        Self {
            participant: participant.as_str().to_string(),
            case: case.into(),
            elapsed_ms: elapsed_secs * 1000.0,
            rows,
            rows_per_sec: (elapsed_secs > 0.0).then_some(rows as f64 / elapsed_secs),
            checksum,
            repeat_summary: None,
        }
    }

    fn repeated(
        participant: Participant,
        case: impl Into<String>,
        rows: u64,
        checksum: Option<String>,
        repeat_summary: RepeatSummary,
    ) -> Self {
        let elapsed_secs = repeat_summary.median_ms / 1000.0;
        Self {
            participant: participant.as_str().to_string(),
            case: case.into(),
            elapsed_ms: repeat_summary.median_ms,
            rows,
            rows_per_sec: (elapsed_secs > 0.0).then_some(rows as f64 / elapsed_secs),
            checksum,
            repeat_summary: Some(repeat_summary),
        }
    }
}
