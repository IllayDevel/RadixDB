// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::{self, File};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::{Parser, ValueEnum};
use radixdb::storage::instrumentation;
use radixdb::Database;
use radixdb_join_workload::{
    apply_schema, execute_case_with_timeout, explain_analyze_case_with_timeout, seed_database,
    verify_fixture_manifest, CaseId, CaseResult, WorkloadScale, SOURCE_COMMIT, SOURCE_PATH,
    SOURCE_SHA256,
};
use serde::Serialize;
use sha2::Digest;

type AnyError = Box<dyn std::error::Error + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

#[derive(Debug, Parser)]
#[command(about = "Frozen Q1-Q6/C1 current-HEAD baseline runner")]
struct Args {
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = default_run_id())]
    run_id: String,
    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    profile: Profile,
    #[arg(long, default_value_t = 2)]
    warmup_runs: usize,
    #[arg(long, default_value_t = 10)]
    measured_runs: usize,
    #[arg(long, default_value_t = 4)]
    clients: usize,
    #[arg(long, default_value_t = 120)]
    case_timeout_secs: u64,
    #[arg(long, default_value_t = 0)]
    outbox_profile_polls: usize,
    #[arg(long, default_value_t = 250)]
    outbox_poll_interval_ms: u64,
    #[arg(long, default_value_t = 4)]
    outbox_max_jobs: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Smoke,
    Consumer,
}

impl Profile {
    const fn scale(self) -> WorkloadScale {
        match self {
            Self::Smoke => WorkloadScale::smoke(),
            Self::Consumer => WorkloadScale::consumer_profile(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Report<'a> {
    format_version: u32,
    run_id: &'a str,
    engine_revision: &'static str,
    build_profile: &'static str,
    consumer_source_commit: &'static str,
    consumer_source_path: &'static str,
    consumer_source_sha256: &'static str,
    profile: Profile,
    warmup_runs: usize,
    measured_runs: usize,
    clients: usize,
    case_timeout_secs: u64,
    seed_wall_ms: f64,
    database_bytes: u64,
    cases: &'a [CaseReport],
    outbox_profile: Option<&'a OutboxProfileReport>,
}

#[derive(Debug, Serialize)]
struct CaseReport {
    case: CaseId,
    plan: Vec<String>,
    cold: Measurement,
    warm_sequential: Distribution,
    warm_concurrent: Distribution,
}

#[derive(Serialize)]
struct ProgressReport<'a> {
    format_version: u32,
    status: &'static str,
    engine_revision: &'static str,
    profile: Profile,
    case_timeout_secs: u64,
    seed_wall_ms: f64,
    database_bytes: u64,
    active_case: Option<CaseId>,
    completed_cases: &'a [CaseReport],
}

#[derive(Debug, Serialize)]
struct Measurement {
    wall_ms: f64,
    cpu_ms: f64,
    rows: usize,
    canonical_result_bytes: u64,
    checksum_sha256: String,
    engine_counters: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct Distribution {
    samples: usize,
    wall_total_ms: f64,
    cpu_total_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
    rows_per_query: usize,
    canonical_result_bytes_per_query: u64,
    checksum_sha256: String,
    engine_counters: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OutboxProfileReport {
    poll_interval_ms: u64,
    configured_max_parallel_jobs: usize,
    actual_peak_parallel_jobs: usize,
    polls: usize,
    rows_per_poll: usize,
    job_invocations: usize,
    maximum_poll_start_lag_ms: f64,
    cases: Vec<OutboxCaseReport>,
    engine_counters: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OutboxCaseReport {
    case: CaseId,
    samples: usize,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
    rows_per_query: usize,
    canonical_result_bytes_per_query: u64,
    checksum_sha256: String,
}

fn main() -> AnyResult<()> {
    let args = Args::parse();
    if args.clients == 0 || args.measured_runs == 0 {
        return Err("--clients and --measured-runs must be greater than zero".into());
    }
    if args.outbox_profile_polls > 0
        && (args.outbox_poll_interval_ms == 0 || !(3..=4).contains(&args.outbox_max_jobs))
    {
        return Err(
            "outbox profile requires a non-zero interval and max jobs in the range 3..=4".into(),
        );
    }
    verify_fixture_manifest()?;

    let run_dir = args.output_dir.join(&args.run_id);
    if run_dir.exists() {
        return Err(format!("refusing to overwrite existing run {}", run_dir.display()).into());
    }
    fs::create_dir_all(&run_dir)?;
    let database_dir = run_dir.join("database");
    let dsn = format!("file://{}", database_dir.display());
    let scale = args.profile.scale();

    let seed_started = Instant::now();
    let db = Database::open(&dsn)?;
    apply_schema(&db)?;
    seed_database(&db, scale)?;
    db.close()?;
    let seed_wall_ms = duration_ms(seed_started.elapsed());
    let database_bytes = directory_bytes(&database_dir)?;

    let mut cases = Vec::with_capacity(CaseId::ALL.len());
    write_progress(
        &run_dir,
        &args,
        seed_wall_ms,
        database_bytes,
        None,
        &cases,
        "running",
    )?;
    let case_timeout = Duration::from_secs(args.case_timeout_secs);
    for case in CaseId::ALL {
        write_progress(
            &run_dir,
            &args,
            seed_wall_ms,
            database_bytes,
            Some(case),
            &cases,
            "running",
        )?;
        let (evicted_files, evicted_bytes) = evict_database_page_cache(&database_dir)?;
        if evicted_files == 0 || evicted_bytes == 0 {
            return Err(format!(
                "cold admission for {} evicted no database files",
                case.name()
            )
            .into());
        }
        let db = Database::open(&dsn)?;

        instrumentation::reset();
        let cold_cpu = process_cpu_time()?;
        let cold_result = execute_case_with_timeout(&db, case, scale, case_timeout)?;
        let cold = Measurement::from_result(
            cold_result,
            process_cpu_time()?.saturating_sub(cold_cpu),
            serde_json::to_value(instrumentation::snapshot())?,
        );

        for _ in 0..args.warmup_runs {
            assert_same_result(
                &cold,
                &execute_case_with_timeout(&db, case, scale, case_timeout)?,
            )?;
        }
        let warm_sequential =
            measure_sequential(&db, case, scale, args.measured_runs, case_timeout, &cold)?;
        let plan = explain_analyze_case_with_timeout(&db, case, scale, case_timeout)?;
        let warm_concurrent = measure_concurrent(
            &db,
            case,
            scale,
            args.measured_runs,
            args.clients,
            case_timeout,
            &cold,
        )?;
        db.close()?;

        eprintln!(
            "{} cold={:.3}ms warm-p95={:.3}ms clients-{}-p95={:.3}ms",
            case.name(),
            cold.wall_ms,
            warm_sequential.latency_p95_ms,
            args.clients,
            warm_concurrent.latency_p95_ms
        );
        cases.push(CaseReport {
            case,
            plan,
            cold,
            warm_sequential,
            warm_concurrent,
        });
        write_progress(
            &run_dir,
            &args,
            seed_wall_ms,
            database_bytes,
            None,
            &cases,
            "running",
        )?;
    }

    let db = Database::open(&dsn)?;
    let outbox_profile = (args.outbox_profile_polls > 0)
        .then(|| run_outbox_profile(&db, scale, &args, case_timeout))
        .transpose()?;
    db.close()?;

    let report = Report {
        format_version: 3,
        run_id: &args.run_id,
        engine_revision: radixdb::common::GIT_COMMIT,
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        consumer_source_commit: SOURCE_COMMIT,
        consumer_source_path: SOURCE_PATH,
        consumer_source_sha256: SOURCE_SHA256,
        profile: args.profile,
        warmup_runs: args.warmup_runs,
        measured_runs: args.measured_runs,
        clients: args.clients,
        case_timeout_secs: args.case_timeout_secs,
        seed_wall_ms,
        database_bytes,
        cases: &cases,
        outbox_profile: outbox_profile.as_ref(),
    };
    let json = serde_json::to_vec_pretty(&report)?;
    fs::write(run_dir.join("results.json"), &json)?;
    fs::write(run_dir.join("report.md"), render_markdown(&report))?;
    let mut done = File::create(run_dir.join("COMPLETE"))?;
    writeln!(done, "sha256={:x}", sha2::Sha256::digest(&json))?;
    write_progress(
        &run_dir,
        &args,
        seed_wall_ms,
        database_bytes,
        None,
        &cases,
        "complete",
    )?;
    println!("{}", run_dir.display());
    Ok(())
}

impl Measurement {
    fn from_result(result: CaseResult, cpu: Duration, engine_counters: serde_json::Value) -> Self {
        Self {
            wall_ms: duration_ms(result.elapsed),
            cpu_ms: duration_ms(cpu),
            rows: result.rows,
            canonical_result_bytes: result.canonical_result_bytes,
            checksum_sha256: result.checksum_sha256,
            engine_counters,
        }
    }
}

fn measure_sequential(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    samples: usize,
    timeout: Duration,
    oracle: &Measurement,
) -> AnyResult<Distribution> {
    instrumentation::reset();
    let wall = Instant::now();
    let cpu = process_cpu_time()?;
    let mut results = Vec::with_capacity(samples);
    for _ in 0..samples {
        let result = execute_case_with_timeout(db, case, scale, timeout)?;
        assert_same_result(oracle, &result)?;
        results.push(result);
    }
    distribution(
        results,
        wall.elapsed(),
        process_cpu_time()?.saturating_sub(cpu),
        serde_json::to_value(instrumentation::snapshot())?,
    )
}

fn measure_concurrent(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    samples_per_client: usize,
    clients: usize,
    timeout: Duration,
    oracle: &Measurement,
) -> AnyResult<Distribution> {
    instrumentation::reset();
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut workers = Vec::with_capacity(clients);
    for _ in 0..clients {
        let db = db.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(
            move || -> std::result::Result<Vec<CaseResult>, String> {
                barrier.wait();
                let mut results = Vec::with_capacity(samples_per_client);
                for _ in 0..samples_per_client {
                    results.push(
                        execute_case_with_timeout(&db, case, scale, timeout)
                            .map_err(|error| error.to_string())?,
                    );
                }
                Ok(results)
            },
        ));
    }
    let wall = Instant::now();
    let cpu = process_cpu_time()?;
    barrier.wait();
    let mut results = Vec::with_capacity(clients * samples_per_client);
    for worker in workers {
        let worker_results = worker.join().map_err(|_| "benchmark worker panicked")??;
        for result in worker_results {
            assert_same_result(oracle, &result)?;
            results.push(result);
        }
    }
    distribution(
        results,
        wall.elapsed(),
        process_cpu_time()?.saturating_sub(cpu),
        serde_json::to_value(instrumentation::snapshot())?,
    )
}

fn write_progress(
    run_dir: &Path,
    args: &Args,
    seed_wall_ms: f64,
    database_bytes: u64,
    active_case: Option<CaseId>,
    completed_cases: &[CaseReport],
    status: &'static str,
) -> AnyResult<()> {
    let progress = ProgressReport {
        format_version: 1,
        status,
        engine_revision: radixdb::common::GIT_COMMIT,
        profile: args.profile,
        case_timeout_secs: args.case_timeout_secs,
        seed_wall_ms,
        database_bytes,
        active_case,
        completed_cases,
    };
    let payload = serde_json::to_vec_pretty(&progress)?;
    let temporary = run_dir.join("progress.json.tmp");
    fs::write(&temporary, payload)?;
    fs::rename(temporary, run_dir.join("progress.json"))?;
    Ok(())
}

fn distribution(
    results: Vec<CaseResult>,
    wall: Duration,
    cpu: Duration,
    engine_counters: serde_json::Value,
) -> AnyResult<Distribution> {
    let first = results.first().ok_or("cannot summarize zero samples")?;
    let mut latencies: Vec<f64> = results
        .iter()
        .map(|result| duration_ms(result.elapsed))
        .collect();
    latencies.sort_by(f64::total_cmp);
    Ok(Distribution {
        samples: results.len(),
        wall_total_ms: duration_ms(wall),
        cpu_total_ms: duration_ms(cpu),
        latency_p50_ms: percentile(&latencies, 50),
        latency_p95_ms: percentile(&latencies, 95),
        latency_p99_ms: percentile(&latencies, 99),
        latency_max_ms: *latencies.last().expect("non-empty latency set"),
        rows_per_query: first.rows,
        canonical_result_bytes_per_query: first.canonical_result_bytes,
        checksum_sha256: first.checksum_sha256.clone(),
        engine_counters,
    })
}

fn assert_same_result(oracle: &Measurement, result: &CaseResult) -> AnyResult<()> {
    if oracle.rows != result.rows
        || oracle.canonical_result_bytes != result.canonical_result_bytes
        || oracle.checksum_sha256 != result.checksum_sha256
    {
        return Err(format!(
            "{} result drift: expected rows={} bytes={} checksum={}, got rows={} bytes={} checksum={}",
            result.case.name(),
            oracle.rows,
            oracle.canonical_result_bytes,
            oracle.checksum_sha256,
            result.rows,
            result.canonical_result_bytes,
            result.checksum_sha256
        )
        .into());
    }
    Ok(())
}

fn run_outbox_profile(
    db: &Database,
    scale: WorkloadScale,
    args: &Args,
    timeout: Duration,
) -> AnyResult<OutboxProfileReport> {
    const P0_CASES: [CaseId; 3] = [
        CaseId::Q1MessagePush,
        CaseId::Q2CallPush,
        CaseId::Q3StreamPush,
    ];

    instrumentation::reset();
    let interval = Duration::from_millis(args.outbox_poll_interval_ms);
    let mut scheduled_at = Instant::now();
    let mut maximum_poll_start_lag = Duration::ZERO;
    let mut rows_per_poll = None;
    let mut case_results: [Vec<CaseResult>; 3] = std::array::from_fn(|_| Vec::new());

    for poll_index in 0..args.outbox_profile_polls {
        if poll_index > 0 {
            scheduled_at = scheduled_at
                .checked_add(interval)
                .ok_or("outbox poll schedule overflow")?;
            if let Some(remaining) = scheduled_at.checked_duration_since(Instant::now()) {
                thread::sleep(remaining);
            }
        }
        maximum_poll_start_lag = maximum_poll_start_lag.max(
            Instant::now()
                .checked_duration_since(scheduled_at)
                .unwrap_or(Duration::ZERO),
        );

        let poll = execute_case_with_timeout(db, CaseId::C1OutboxPoll, scale, timeout)?;
        if poll.rows != scale.expected_rows(CaseId::C1OutboxPoll) {
            return Err(format!(
                "outbox poll returned {} rows instead of {}",
                poll.rows,
                scale.expected_rows(CaseId::C1OutboxPoll)
            )
            .into());
        }
        if let Some(expected) = rows_per_poll {
            if poll.rows != expected {
                return Err("outbox poll cardinality changed during the profile".into());
            }
        } else {
            rows_per_poll = Some(poll.rows);
        }

        let mut workers = Vec::with_capacity(P0_CASES.len());
        for case in P0_CASES {
            let db = db.clone();
            workers.push((
                case,
                thread::spawn(move || {
                    execute_case_with_timeout(&db, case, scale, timeout)
                        .map_err(|error| error.to_string())
                }),
            ));
        }
        for (case, worker) in workers {
            let result = worker
                .join()
                .map_err(|_| format!("{} outbox worker panicked", case.name()))??;
            let slot = P0_CASES
                .iter()
                .position(|candidate| *candidate == case)
                .expect("P0 worker case belongs to the fixed profile");
            if let Some(oracle) = case_results[slot].first() {
                assert_case_result_same(oracle, &result)?;
            }
            case_results[slot].push(result);
        }
    }

    let cases = P0_CASES
        .into_iter()
        .zip(case_results)
        .map(|(case, results)| summarize_outbox_case(case, results))
        .collect::<AnyResult<Vec<_>>>()?;
    Ok(OutboxProfileReport {
        poll_interval_ms: args.outbox_poll_interval_ms,
        configured_max_parallel_jobs: args.outbox_max_jobs,
        actual_peak_parallel_jobs: P0_CASES.len(),
        polls: args.outbox_profile_polls,
        rows_per_poll: rows_per_poll.unwrap_or(0),
        job_invocations: args.outbox_profile_polls.saturating_mul(P0_CASES.len()),
        maximum_poll_start_lag_ms: duration_ms(maximum_poll_start_lag),
        cases,
        engine_counters: serde_json::to_value(instrumentation::snapshot())?,
    })
}

fn assert_case_result_same(oracle: &CaseResult, result: &CaseResult) -> AnyResult<()> {
    if oracle.case != result.case
        || oracle.rows != result.rows
        || oracle.canonical_result_bytes != result.canonical_result_bytes
        || oracle.checksum_sha256 != result.checksum_sha256
    {
        return Err(format!(
            "{} outbox result changed: expected rows={} bytes={} checksum={}, got rows={} bytes={} checksum={}",
            result.case.name(),
            oracle.rows,
            oracle.canonical_result_bytes,
            oracle.checksum_sha256,
            result.rows,
            result.canonical_result_bytes,
            result.checksum_sha256
        )
        .into());
    }
    Ok(())
}

fn summarize_outbox_case(case: CaseId, results: Vec<CaseResult>) -> AnyResult<OutboxCaseReport> {
    let first = results
        .first()
        .ok_or("cannot summarize empty outbox case")?;
    let mut latencies = results
        .iter()
        .map(|result| duration_ms(result.elapsed))
        .collect::<Vec<_>>();
    latencies.sort_by(f64::total_cmp);
    Ok(OutboxCaseReport {
        case,
        samples: latencies.len(),
        latency_p50_ms: percentile(&latencies, 50),
        latency_p95_ms: percentile(&latencies, 95),
        latency_p99_ms: percentile(&latencies, 99),
        latency_max_ms: *latencies.last().expect("non-empty outbox latency set"),
        rows_per_query: first.rows,
        canonical_result_bytes_per_query: first.canonical_result_bytes,
        checksum_sha256: first.checksum_sha256.clone(),
    })
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn process_cpu_time() -> std::io::Result<Duration> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` points to valid writable storage for the duration of the call.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Duration::new(time.tv_sec as u64, time.tv_nsec as u32))
}

fn evict_database_page_cache(root: &Path) -> AnyResult<(u64, u64)> {
    let canonical_root = root.canonicalize()?;
    let mut pending = vec![canonical_root.clone()];
    let mut files = 0u64;
    let mut bytes = 0u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(
                    format!("refusing cache eviction through symlink {}", path.display()).into(),
                );
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
                return Err(
                    format!("database member escaped root: {}", canonical.display()).into(),
                );
            }
            let file = File::open(&canonical)?;
            // SAFETY: the descriptor stays open for the complete advisory call.
            let result =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            if result != 0 {
                return Err(std::io::Error::from_raw_os_error(result).into());
            }
            files += 1;
            bytes += metadata.len();
        }
    }
    Ok((files, bytes))
}

fn directory_bytes(root: &Path) -> std::io::Result<u64> {
    let mut pending = vec![root.to_path_buf()];
    let mut bytes = 0u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                bytes = bytes.saturating_add(metadata.len());
            }
        }
    }
    Ok(bytes)
}

fn render_markdown(report: &Report) -> String {
    let mut output = format!(
        "# JOIN/reference baseline: {}\n\n- Engine: `{}`\n- Profile: `{:?}`\n- Seed: `{:.3} ms`\n- Database: `{}` bytes\n- Runs: warmup `{}`, measured `{}`, clients `{}`\n\n",
        report.run_id,
        report.engine_revision,
        report.profile,
        report.seed_wall_ms,
        report.database_bytes,
        report.warmup_runs,
        report.measured_runs,
        report.clients
    );
    output.push_str("| Case | Cold ms | Warm p50 | Warm p95 | Warm p99 | Concurrent p50 | Concurrent p95 | Concurrent p99 | Rows |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for case in report.cases {
        output.push_str(&format!(
            "| {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {} |\n",
            case.case.name(),
            case.cold.wall_ms,
            case.warm_sequential.latency_p50_ms,
            case.warm_sequential.latency_p95_ms,
            case.warm_sequential.latency_p99_ms,
            case.warm_concurrent.latency_p50_ms,
            case.warm_concurrent.latency_p95_ms,
            case.warm_concurrent.latency_p99_ms,
            case.cold.rows
        ));
    }
    if let Some(profile) = report.outbox_profile {
        output.push_str(&format!(
            "\n## Outbox profile\n\n- Poll interval: `{}` ms\n- Polls: `{}`\n- Configured max jobs: `{}`\n- Actual peak jobs: `{}`\n- Maximum poll start lag: `{:.3}` ms\n\n",
            profile.poll_interval_ms,
            profile.polls,
            profile.configured_max_parallel_jobs,
            profile.actual_peak_parallel_jobs,
            profile.maximum_poll_start_lag_ms
        ));
        output.push_str("| Case | Samples | p50 ms | p95 ms | p99 ms | max ms | Rows |\n");
        output.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
        for case in &profile.cases {
            output.push_str(&format!(
                "| {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {} |\n",
                case.case.name(),
                case.samples,
                case.latency_p50_ms,
                case.latency_p95_ms,
                case.latency_p99_ms,
                case.latency_max_ms,
                case.rows_per_query
            ));
        }
    }
    output.push_str("\nFull plans and engine counter snapshots are retained in `results.json`.\n");
    output
}

fn default_run_id() -> String {
    format!("join-current-{}", Utc::now().format("%Y%m%d-%H%M%S"))
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_keep_tail_samples() {
        let samples = [1.0, 2.0, 3.0, 4.0, 100.0];
        assert_eq!(percentile(&samples, 50), 3.0);
        assert_eq!(percentile(&samples, 95), 100.0);
        assert_eq!(percentile(&samples, 99), 100.0);
    }
}
