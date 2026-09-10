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

//! PostgreSQL 18 performance reference for the frozen Q1-Q6/C1 corpus.
//!
//! The DSN is accepted only through the environment and is never retained in
//! artifacts. Every run owns one isolated schema and drops it before exit.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::{Parser, ValueEnum};
use postgres::{Client, NoTls};
use radixdb_join_workload::postgres_oracle;
use radixdb_join_workload::{
    verify_fixture_manifest, CaseId, CaseResult, WorkloadScale, SOURCE_COMMIT, SOURCE_PATH,
    SOURCE_SHA256,
};
use serde::Serialize;
use sha2::Digest;

type AnyError = Box<dyn std::error::Error + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

#[derive(Debug, Parser)]
#[command(about = "PostgreSQL 18 reference for the frozen JOIN corpus")]
struct Args {
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = default_run_id())]
    run_id: String,
    #[arg(long, value_enum, default_value_t = Profile::Consumer)]
    profile: Profile,
    #[arg(long, default_value_t = 2)]
    warmup_runs: usize,
    #[arg(long, default_value_t = 10)]
    measured_runs: usize,
    #[arg(long, default_value_t = 120)]
    case_timeout_secs: u64,
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
    engine: &'static str,
    server_version: &'a str,
    consumer_source_commit: &'static str,
    consumer_source_path: &'static str,
    consumer_source_sha256: &'static str,
    profile: Profile,
    warmup_runs: usize,
    measured_runs: usize,
    case_timeout_secs: u64,
    seed_wall_ms: f64,
    analyze_wall_ms: f64,
    database_bytes: u64,
    cases: &'a [CaseReport],
}

#[derive(Debug, Serialize)]
struct CaseReport {
    case: CaseId,
    samples: usize,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
    rows: usize,
    canonical_result_bytes: u64,
    checksum_sha256: String,
}

fn main() -> AnyResult<()> {
    let args = Args::parse();
    if args.measured_runs == 0 || args.case_timeout_secs == 0 {
        return Err("--measured-runs and --case-timeout-secs must be greater than zero".into());
    }
    verify_fixture_manifest()?;
    let dsn = std::env::var("RADIXDB_JOIN_PG_DSN")
        .map_err(|_| "RADIXDB_JOIN_PG_DSN must identify the registered PostgreSQL 18 cluster")?;
    let run_dir = args.output_dir.join(&args.run_id);
    if run_dir.exists() {
        return Err(format!("refusing to overwrite existing run {}", run_dir.display()).into());
    }
    fs::create_dir_all(&run_dir)?;

    let mut client = Client::connect(&dsn, NoTls)?;
    let server_version_num = client
        .query_one("SHOW server_version_num", &[])?
        .get::<_, String>(0);
    if !server_version_num.starts_with("18") {
        return Err(format!("PostgreSQL 18 is required, got {server_version_num}").into());
    }
    let server_version = client
        .query_one("SHOW server_version", &[])?
        .get::<_, String>(0);
    let schema = isolated_schema_name(&args.run_id);
    client.batch_execute(&format!("CREATE SCHEMA \"{schema}\""))?;

    let run_result = run_benchmark(&mut client, &schema, &args, &run_dir, &server_version);
    let cleanup_result = client.batch_execute(&format!(
        "SET search_path TO public; DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"
    ));
    match (run_result, cleanup_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => {
            println!("{}", run_dir.display());
            Ok(())
        }
    }
}

fn run_benchmark(
    client: &mut Client,
    schema: &str,
    args: &Args,
    run_dir: &Path,
    server_version: &str,
) -> AnyResult<()> {
    let scale = args.profile.scale();
    let seed_started = Instant::now();
    let mut transaction = client.transaction()?;
    transaction.batch_execute(&format!(
        "SET LOCAL search_path TO \"{schema}\", public; \
         SET LOCAL statement_timeout TO '{}ms'",
        args.case_timeout_secs.saturating_mul(1_000)
    ))?;
    postgres_oracle::apply_schema(&mut transaction)?;
    postgres_oracle::seed_database(&mut transaction, scale)?;
    transaction.commit()?;
    let seed_wall_ms = duration_ms(seed_started.elapsed());

    client.batch_execute(&format!(
        "SET search_path TO \"{schema}\", public; \
         SET statement_timeout TO '{}ms'",
        args.case_timeout_secs.saturating_mul(1_000)
    ))?;
    let analyze_started = Instant::now();
    client.batch_execute("ANALYZE")?;
    let analyze_wall_ms = duration_ms(analyze_started.elapsed());
    let database_bytes = schema_bytes(client, schema)?;

    let mut cases = Vec::with_capacity(CaseId::ALL.len());
    for case in CaseId::ALL {
        let mut oracle: Option<CaseResult> = None;
        for _ in 0..args.warmup_runs {
            let result = postgres_oracle::execute_case(client, case, scale)?;
            assert_same_result(oracle.as_ref(), &result)?;
            oracle.get_or_insert(result);
        }
        let mut results = Vec::with_capacity(args.measured_runs);
        for _ in 0..args.measured_runs {
            let result = postgres_oracle::execute_case(client, case, scale)?;
            assert_same_result(oracle.as_ref(), &result)?;
            if oracle.is_none() {
                oracle = Some(result.clone());
            }
            results.push(result);
        }
        let report = summarize(case, &results)?;
        eprintln!(
            "{} warm-p95={:.3}ms rows={}",
            case.name(),
            report.latency_p95_ms,
            report.rows
        );
        cases.push(report);
    }

    let report = Report {
        format_version: 1,
        run_id: &args.run_id,
        engine: "PostgreSQL",
        server_version,
        consumer_source_commit: SOURCE_COMMIT,
        consumer_source_path: SOURCE_PATH,
        consumer_source_sha256: SOURCE_SHA256,
        profile: args.profile,
        warmup_runs: args.warmup_runs,
        measured_runs: args.measured_runs,
        case_timeout_secs: args.case_timeout_secs,
        seed_wall_ms,
        analyze_wall_ms,
        database_bytes,
        cases: &cases,
    };
    let json = serde_json::to_vec_pretty(&report)?;
    fs::write(run_dir.join("results.json"), &json)?;
    fs::write(run_dir.join("report.md"), render_markdown(&report))?;
    let mut complete = File::create(run_dir.join("COMPLETE"))?;
    writeln!(complete, "sha256={:x}", sha2::Sha256::digest(&json))?;
    Ok(())
}

fn schema_bytes(client: &mut Client, schema: &str) -> AnyResult<u64> {
    let bytes = client
        .query_one(
            "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint \
             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname=$1 AND c.relkind IN ('r','m')",
            &[&schema],
        )?
        .get::<_, i64>(0);
    Ok(u64::try_from(bytes)?)
}

fn summarize(case: CaseId, results: &[CaseResult]) -> AnyResult<CaseReport> {
    let first = results.first().ok_or("cannot summarize zero samples")?;
    let mut latencies: Vec<f64> = results
        .iter()
        .map(|result| duration_ms(result.elapsed))
        .collect();
    latencies.sort_by(f64::total_cmp);
    Ok(CaseReport {
        case,
        samples: results.len(),
        latency_p50_ms: percentile(&latencies, 50),
        latency_p95_ms: percentile(&latencies, 95),
        latency_p99_ms: percentile(&latencies, 99),
        latency_max_ms: *latencies.last().expect("non-empty samples"),
        rows: first.rows,
        canonical_result_bytes: first.canonical_result_bytes,
        checksum_sha256: first.checksum_sha256.clone(),
    })
}

fn assert_same_result(oracle: Option<&CaseResult>, result: &CaseResult) -> AnyResult<()> {
    let Some(oracle) = oracle else {
        return Ok(());
    };
    if oracle.rows != result.rows
        || oracle.canonical_result_bytes != result.canonical_result_bytes
        || oracle.checksum_sha256 != result.checksum_sha256
    {
        return Err(format!("{} result changed between samples", result.case.name()).into());
    }
    Ok(())
}

fn render_markdown(report: &Report<'_>) -> String {
    let mut output = format!(
        "# PostgreSQL JOIN reference: {}\n\n- Engine: PostgreSQL {}\n- Profile: {:?}\n- Seed: {:.3} ms\n- ANALYZE: {:.3} ms\n- Relations: {} bytes\n- Runs: warmup {}, measured {}\n\n",
        report.run_id,
        report.server_version,
        report.profile,
        report.seed_wall_ms,
        report.analyze_wall_ms,
        report.database_bytes,
        report.warmup_runs,
        report.measured_runs
    );
    output.push_str("| Case | p50 ms | p95 ms | p99 ms | max ms | Rows |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|\n");
    for case in report.cases {
        output.push_str(&format!(
            "| {} | {:.3} | {:.3} | {:.3} | {:.3} | {} |\n",
            case.case.name(),
            case.latency_p50_ms,
            case.latency_p95_ms,
            case.latency_p99_ms,
            case.latency_max_ms,
            case.rows
        ));
    }
    output
}

fn isolated_schema_name(run_id: &str) -> String {
    let suffix: String = run_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(40)
        .collect();
    format!("jr14_{suffix}_{}", std::process::id())
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn default_run_id() -> String {
    format!("postgres-join-{}", Utc::now().format("%Y%m%d-%H%M%S"))
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_is_safe_and_bounded() {
        let name = isolated_schema_name("../../Q1 PUSH with spaces and a very long suffix");
        assert!(name.starts_with("jr14_"));
        assert!(name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        }));
        assert!(name.len() <= 64);
    }

    #[test]
    fn nearest_rank_percentiles_keep_tail_samples() {
        let samples = [1.0, 2.0, 3.0, 4.0, 100.0];
        assert_eq!(percentile(&samples, 50), 3.0);
        assert_eq!(percentile(&samples, 95), 100.0);
    }
}
