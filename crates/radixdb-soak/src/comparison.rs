use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    config::DatabaseEngine,
    diagnostics::{DiagnosticMetricFrameV2, SemanticProgressSnapshot},
    status::{
        Counters, InvariantState, InvariantStatus, LatencySnapshot, ResourceSnapshot, RunEvent,
        RunState, ServerRuntimeSnapshot, StatusSnapshot,
    },
};

const SUMMARY_FORMAT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricStats {
    pub samples: u64,
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub time_weighted_mean: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseSummary {
    pub name: String,
    pub sample_count: u64,
    pub first_unix_millis: u64,
    pub last_unix_millis: u64,
    pub duration_millis: u64,
    pub tps: MetricStats,
    pub latency_p50_micros: MetricStats,
    pub latency_p95_micros: MetricStats,
    pub latency_p99_micros: MetricStats,
    pub rss_bytes: MetricStats,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvariantSummary {
    pub name: String,
    pub state: InvariantState,
    pub checks: u64,
    pub failures: u64,
    pub first_checked_unix_millis: Option<u64>,
    pub last_checked_unix_millis: Option<u64>,
    pub mean_interval_millis: Option<f64>,
    pub max_interval_millis: Option<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySummary {
    pub kind: String,
    pub unix_millis: u64,
    pub elapsed_millis: u64,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSummary {
    pub metrics: BTreeMap<String, MetricStats>,
    pub diagnostic_gauges: BTreeMap<String, MetricStats>,
    pub diagnostic_counter_deltas: BTreeMap<String, i64>,
    pub cpu_average_cores: f64,
    pub process_read_bytes: u64,
    pub process_write_bytes: u64,
    pub final_database_bytes: u64,
    pub peak_database_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSummary {
    pub format: u32,
    pub run_id: String,
    pub database_engine: DatabaseEngine,
    pub profile: String,
    pub state: RunState,
    pub started_unix_millis: u64,
    pub seed: u64,
    pub target_rows: u64,
    pub client_steps: Vec<usize>,
    pub configured_duration_millis: u64,
    pub elapsed_millis: u64,
    pub seed_elapsed_millis: Option<u64>,
    pub sample_count: u64,
    pub tps: MetricStats,
    pub final_counters: Counters,
    pub final_latency: LatencySnapshot,
    pub longest_semantic_progress_gap_millis: u64,
    pub phases: Vec<PhaseSummary>,
    pub invariants: Vec<InvariantSummary>,
    pub recoveries: Vec<RecoverySummary>,
    pub resources: ResourceSummary,
    pub logical_digest: Option<String>,
    pub failure: Option<String>,
    pub server_identity: String,
    pub evidence_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricComparison {
    pub radixdb: f64,
    pub postgresql: f64,
    pub radixdb_over_postgresql: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonReport {
    pub format: u32,
    pub pair_id: String,
    pub radixdb: RunSummary,
    pub postgresql: RunSummary,
    pub metrics: BTreeMap<String, MetricComparison>,
}

#[derive(Debug, Deserialize)]
struct ManifestInput {
    run_id: String,
    profile: String,
    duration_millis: u64,
    seed: u64,
    database_engine: DatabaseEngine,
    client_steps: Vec<usize>,
    active_rows: u64,
    server_identity: String,
    started_unix_millis: u64,
}

#[derive(Debug, Deserialize)]
struct SampleInput {
    unix_millis: u64,
    active_clients: u64,
    current_tps: f64,
    latency: LatencySnapshot,
    resources: ResourceSnapshot,
    server_runtime: ServerRuntimeSnapshot,
}

pub fn summarize_run(run_dir: &Path) -> Result<RunSummary, String> {
    let manifest: ManifestInput = read_json(&run_dir.join("manifest.json"))?;
    let status: StatusSnapshot = read_json(&run_dir.join("status.json"))?;
    if manifest.run_id != status.run_id || manifest.profile != status.profile {
        return Err("manifest/status identity mismatch".into());
    }
    let samples: Vec<SampleInput> = read_json_lines(&run_dir.join("samples.jsonl"))?;
    if samples.is_empty() {
        return Err("run contains no workload samples".into());
    }
    let events: Vec<RunEvent> = read_json_lines(&run_dir.join("events.jsonl"))?;
    let semantic: Vec<SemanticProgressSnapshot> =
        read_json_lines(&run_dir.join("semantic-progress.jsonl"))?;
    let frames: Vec<DiagnosticMetricFrameV2> =
        read_optional_json_lines(&run_dir.join("diagnostic-frames.jsonl"))?;

    let tps = stats(
        samples
            .iter()
            .map(|sample| (sample.unix_millis, sample.current_tps)),
    );
    let phases = summarize_phases(&samples);
    let invariants = summarize_invariants(&status.invariants, &events);
    let recoveries = summarize_recoveries(&events);
    let resources = summarize_resources(&samples, &frames);
    let seed_started = events
        .iter()
        .find(|event| event.kind == "seed_started")
        .map(|event| event.unix_millis);
    let workload_started = events
        .iter()
        .find(|event| event.kind == "phase" && event.detail.starts_with("clients-"))
        .map(|event| event.unix_millis);
    let seed_elapsed_millis = seed_started
        .zip(workload_started)
        .map(|(started, finished)| finished.saturating_sub(started));
    let evidence_sha256 = fs::read(run_dir.join("SHA256SUMS"))
        .ok()
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)));

    Ok(RunSummary {
        format: SUMMARY_FORMAT,
        run_id: manifest.run_id,
        database_engine: manifest.database_engine,
        profile: manifest.profile,
        state: status.state,
        started_unix_millis: manifest.started_unix_millis,
        seed: manifest.seed,
        target_rows: manifest.active_rows,
        client_steps: manifest.client_steps,
        configured_duration_millis: manifest.duration_millis,
        elapsed_millis: status.elapsed_millis,
        seed_elapsed_millis,
        sample_count: samples.len() as u64,
        tps,
        final_counters: status.counters,
        final_latency: status.latency,
        longest_semantic_progress_gap_millis: longest_semantic_gap(&semantic),
        phases,
        invariants,
        recoveries,
        resources,
        logical_digest: status.logical_digest,
        failure: status.failure,
        server_identity: manifest.server_identity,
        evidence_sha256,
    })
}

pub fn compare_runs(
    pair_id: &str,
    radixdb_run: &Path,
    postgresql_run: &Path,
) -> Result<ComparisonReport, String> {
    validate_pair_id(pair_id)?;
    let radixdb = summarize_run(radixdb_run)?;
    let postgresql = summarize_run(postgresql_run)?;
    validate_pair_identity_and_order(pair_id, &radixdb, &postgresql)?;
    for (name, equal) in [
        ("profile", radixdb.profile == postgresql.profile),
        ("seed", radixdb.seed == postgresql.seed),
        ("target_rows", radixdb.target_rows == postgresql.target_rows),
        (
            "configured_duration",
            radixdb.configured_duration_millis == postgresql.configured_duration_millis,
        ),
        (
            "client_steps",
            radixdb.client_steps == postgresql.client_steps,
        ),
    ] {
        if !equal {
            return Err(format!("A/B contract mismatch: {name}"));
        }
    }

    let mut metrics = BTreeMap::new();
    insert_comparison(
        &mut metrics,
        "workload.tps.time_weighted_mean",
        radixdb.tps.time_weighted_mean,
        postgresql.tps.time_weighted_mean,
    );
    insert_comparison(
        &mut metrics,
        "workload.latency.p50_micros",
        radixdb.final_latency.p50_micros as f64,
        postgresql.final_latency.p50_micros as f64,
    );
    insert_comparison(
        &mut metrics,
        "workload.latency.p95_micros",
        radixdb.final_latency.p95_micros as f64,
        postgresql.final_latency.p95_micros as f64,
    );
    insert_comparison(
        &mut metrics,
        "workload.latency.p99_micros",
        radixdb.final_latency.p99_micros as f64,
        postgresql.final_latency.p99_micros as f64,
    );
    insert_comparison(
        &mut metrics,
        "workload.committed",
        radixdb.final_counters.transactions_committed as f64,
        postgresql.final_counters.transactions_committed as f64,
    );
    insert_comparison(
        &mut metrics,
        "workload.conflicts",
        radixdb.final_counters.conflicts as f64,
        postgresql.final_counters.conflicts as f64,
    );
    insert_comparison(
        &mut metrics,
        "progress.longest_gap_millis",
        radixdb.longest_semantic_progress_gap_millis as f64,
        postgresql.longest_semantic_progress_gap_millis as f64,
    );
    insert_comparison(
        &mut metrics,
        "resource.cpu_average_cores",
        radixdb.resources.cpu_average_cores,
        postgresql.resources.cpu_average_cores,
    );
    insert_comparison(
        &mut metrics,
        "resource.process_read_bytes",
        radixdb.resources.process_read_bytes as f64,
        postgresql.resources.process_read_bytes as f64,
    );
    insert_comparison(
        &mut metrics,
        "resource.process_write_bytes",
        radixdb.resources.process_write_bytes as f64,
        postgresql.resources.process_write_bytes as f64,
    );
    insert_comparison(
        &mut metrics,
        "resource.final_database_bytes",
        radixdb.resources.final_database_bytes as f64,
        postgresql.resources.final_database_bytes as f64,
    );
    for name in radixdb
        .resources
        .metrics
        .keys()
        .filter(|name| postgresql.resources.metrics.contains_key(*name))
    {
        let left = &radixdb.resources.metrics[name];
        let right = &postgresql.resources.metrics[name];
        insert_comparison(
            &mut metrics,
            &format!("resource.{name}.time_weighted_mean"),
            left.time_weighted_mean,
            right.time_weighted_mean,
        );
        insert_comparison(
            &mut metrics,
            &format!("resource.{name}.max"),
            left.max,
            right.max,
        );
    }
    for phase in &radixdb.phases {
        if let Some(other) = postgresql
            .phases
            .iter()
            .find(|candidate| candidate.name == phase.name)
        {
            insert_comparison(
                &mut metrics,
                &format!("phase.{}.tps.time_weighted_mean", phase.name),
                phase.tps.time_weighted_mean,
                other.tps.time_weighted_mean,
            );
            insert_comparison(
                &mut metrics,
                &format!("phase.{}.latency_p95_micros.mean", phase.name),
                phase.latency_p95_micros.mean,
                other.latency_p95_micros.mean,
            );
        }
    }
    for name in radixdb
        .resources
        .diagnostic_gauges
        .keys()
        .filter(|name| postgresql.resources.diagnostic_gauges.contains_key(*name))
    {
        insert_comparison(
            &mut metrics,
            &format!("diagnostic.{name}.time_weighted_mean"),
            radixdb.resources.diagnostic_gauges[name].time_weighted_mean,
            postgresql.resources.diagnostic_gauges[name].time_weighted_mean,
        );
    }

    Ok(ComparisonReport {
        format: SUMMARY_FORMAT,
        pair_id: pair_id.into(),
        radixdb,
        postgresql,
        metrics,
    })
}

pub fn write_run_summary(
    run_dir: &Path,
    json: &Path,
    markdown: &Path,
) -> Result<RunSummary, String> {
    let summary = summarize_run(run_dir)?;
    write_new_json(json, &summary)?;
    write_new(markdown, run_markdown(&summary).as_bytes())?;
    Ok(summary)
}

pub fn write_comparison(
    pair_id: &str,
    radixdb_run: &Path,
    postgresql_run: &Path,
    json: &Path,
    markdown: &Path,
) -> Result<ComparisonReport, String> {
    let report = compare_runs(pair_id, radixdb_run, postgresql_run)?;
    write_new_json(json, &report)?;
    write_new(markdown, comparison_markdown(&report).as_bytes())?;
    Ok(report)
}

fn summarize_phases(samples: &[SampleInput]) -> Vec<PhaseSummary> {
    let mut grouped = BTreeMap::<String, Vec<&SampleInput>>::new();
    for sample in samples {
        let name = if sample.active_clients == 0 {
            "seed".to_string()
        } else {
            format!("clients-{}", sample.active_clients)
        };
        grouped.entry(name).or_default().push(sample);
    }
    grouped
        .into_iter()
        .map(|(name, samples)| {
            let first = samples.first().map_or(0, |sample| sample.unix_millis);
            let last = samples.last().map_or(first, |sample| sample.unix_millis);
            PhaseSummary {
                name,
                sample_count: samples.len() as u64,
                first_unix_millis: first,
                last_unix_millis: last,
                duration_millis: last.saturating_sub(first),
                tps: stats(
                    samples
                        .iter()
                        .map(|sample| (sample.unix_millis, sample.current_tps)),
                ),
                latency_p50_micros: stats(
                    samples
                        .iter()
                        .map(|sample| (sample.unix_millis, sample.latency.p50_micros as f64)),
                ),
                latency_p95_micros: stats(
                    samples
                        .iter()
                        .map(|sample| (sample.unix_millis, sample.latency.p95_micros as f64)),
                ),
                latency_p99_micros: stats(
                    samples
                        .iter()
                        .map(|sample| (sample.unix_millis, sample.latency.p99_micros as f64)),
                ),
                rss_bytes: stats(
                    samples
                        .iter()
                        .map(|sample| (sample.unix_millis, sample.resources.rss_bytes as f64)),
                ),
            }
        })
        .collect()
}

fn summarize_invariants(
    final_invariants: &BTreeMap<String, InvariantStatus>,
    events: &[RunEvent],
) -> Vec<InvariantSummary> {
    final_invariants
        .values()
        .map(|invariant| {
            let mut checks = events
                .iter()
                .filter(|event| {
                    matches!(event.kind.as_str(), "invariant_passed" | "invariant_failed")
                        && event
                            .detail
                            .strip_prefix(&invariant.name)
                            .is_some_and(|rest| rest.starts_with(':'))
                })
                .map(|event| event.unix_millis)
                .collect::<Vec<_>>();
            checks.sort_unstable();
            let intervals = checks
                .windows(2)
                .map(|pair| pair[1].saturating_sub(pair[0]))
                .collect::<Vec<_>>();
            let mean_interval_millis = (!intervals.is_empty()).then(|| {
                intervals.iter().map(|value| *value as f64).sum::<f64>() / intervals.len() as f64
            });
            InvariantSummary {
                name: invariant.name.clone(),
                state: invariant.state.clone(),
                checks: invariant.checks,
                failures: invariant.failures,
                first_checked_unix_millis: checks.first().copied(),
                last_checked_unix_millis: checks.last().copied(),
                mean_interval_millis,
                max_interval_millis: intervals.into_iter().max(),
                detail: invariant.detail.clone(),
            }
        })
        .collect()
}

fn summarize_recoveries(events: &[RunEvent]) -> Vec<RecoverySummary> {
    events
        .iter()
        .filter(|event| event.kind == "reopened")
        .filter_map(|event| {
            let kind = field(&event.detail, "kind")?.to_string();
            let elapsed_millis = field(&event.detail, "elapsed_millis")?.parse().ok()?;
            Some(RecoverySummary {
                kind,
                unix_millis: event.unix_millis,
                elapsed_millis,
                detail: event.detail.clone(),
            })
        })
        .collect()
}

fn summarize_resources(
    samples: &[SampleInput],
    frames: &[DiagnosticMetricFrameV2],
) -> ResourceSummary {
    let mut metrics = BTreeMap::<String, Vec<(u64, f64)>>::new();
    for sample in samples {
        let timestamp = sample.unix_millis;
        for (name, value) in resource_values(&sample.resources, &sample.server_runtime) {
            metrics.entry(name).or_default().push((timestamp, value));
        }
    }
    let metrics = metrics
        .into_iter()
        .map(|(name, values)| (name, stats(values)))
        .collect();

    let mut diagnostic_series = BTreeMap::<String, Vec<(u64, f64)>>::new();
    let mut first_counters = BTreeMap::<String, i64>::new();
    let mut last_counters = BTreeMap::<String, i64>::new();
    for frame in frames {
        for (name, value) in &frame.gauges {
            diagnostic_series
                .entry(name.clone())
                .or_default()
                .push((frame.unix_millis, *value));
        }
        for (name, value) in &frame.counters {
            first_counters.entry(name.clone()).or_insert(*value);
            last_counters.insert(name.clone(), *value);
        }
    }
    let diagnostic_gauges = diagnostic_series
        .into_iter()
        .map(|(name, values)| (name, stats(values)))
        .collect();
    let diagnostic_counter_deltas = last_counters
        .into_iter()
        .map(|(name, last)| {
            let first = first_counters.get(&name).copied().unwrap_or(last);
            (name, last.saturating_sub(first))
        })
        .collect();

    let elapsed = samples
        .last()
        .zip(samples.first())
        .map_or(0, |(last, first)| {
            last.unix_millis.saturating_sub(first.unix_millis)
        });
    let cpu_millis = cumulative_delta(samples, |sample| {
        sample
            .resources
            .cpu_user_millis
            .saturating_add(sample.resources.cpu_system_millis)
    });
    let process_read_bytes =
        cumulative_delta(samples, |sample| sample.resources.process_read_bytes);
    let process_write_bytes =
        cumulative_delta(samples, |sample| sample.resources.process_write_bytes);
    ResourceSummary {
        metrics,
        diagnostic_gauges,
        diagnostic_counter_deltas,
        cpu_average_cores: if elapsed == 0 {
            0.0
        } else {
            cpu_millis as f64 / elapsed as f64
        },
        process_read_bytes,
        process_write_bytes,
        final_database_bytes: samples
            .last()
            .map_or(0, |sample| sample.resources.database_bytes),
        peak_database_bytes: samples
            .iter()
            .map(|sample| sample.resources.database_bytes)
            .max()
            .unwrap_or(0),
    }
}

fn resource_values(
    resource: &ResourceSnapshot,
    runtime: &ServerRuntimeSnapshot,
) -> Vec<(String, f64)> {
    [
        ("rss_bytes", resource.rss_bytes),
        ("virtual_bytes", resource.virtual_bytes),
        ("open_fds", resource.open_fds),
        ("socket_fds", resource.socket_fds),
        ("threads", resource.threads),
        ("cpu_user_millis", resource.cpu_user_millis),
        ("cpu_system_millis", resource.cpu_system_millis),
        ("process_read_bytes", resource.process_read_bytes),
        ("process_write_bytes", resource.process_write_bytes),
        ("database_bytes", resource.database_bytes),
        ("database_files", resource.database_files),
        ("database_data_bytes", resource.database_data_bytes),
        ("database_index_bytes", resource.database_index_bytes),
        ("database_metadata_bytes", resource.database_metadata_bytes),
        ("database_wal_bytes", resource.database_wal_bytes),
        ("database_other_bytes", resource.database_other_bytes),
        (
            "voluntary_context_switches",
            resource.voluntary_context_switches,
        ),
        (
            "involuntary_context_switches",
            resource.involuntary_context_switches,
        ),
        ("server.open_databases", runtime.open_databases),
        ("server.retained_databases", runtime.retained_databases),
        ("server.max_databases", runtime.max_databases),
        ("server.active_connections", runtime.active_connections),
        ("server.max_connections", runtime.max_connections),
        ("server.inflight_frame_bytes", runtime.inflight_frame_bytes),
        (
            "server.max_inflight_frame_bytes",
            runtime.max_inflight_frame_bytes,
        ),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value as f64))
    .collect()
}

fn cumulative_delta(samples: &[SampleInput], value: impl Fn(&SampleInput) -> u64) -> u64 {
    samples.windows(2).fold(0_u64, |total, pair| {
        let previous = value(&pair[0]);
        let current = value(&pair[1]);
        total.saturating_add(if current >= previous {
            current - previous
        } else {
            current
        })
    })
}

fn longest_semantic_gap(samples: &[SemanticProgressSnapshot]) -> u64 {
    let mut timestamps = samples
        .iter()
        .map(|sample| sample.last_workload_progress_unix_millis)
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    timestamps.dedup();
    timestamps
        .windows(2)
        .map(|pair| pair[1].saturating_sub(pair[0]))
        .max()
        .unwrap_or(0)
}

fn stats(values: impl IntoIterator<Item = (u64, f64)>) -> MetricStats {
    let mut values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        return MetricStats {
            samples: 0,
            min: 0.0,
            mean: 0.0,
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            max: 0.0,
            time_weighted_mean: 0.0,
        };
    }
    values.sort_by_key(|(timestamp, _)| *timestamp);
    let mut sorted = values.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    sorted.sort_by(f64::total_cmp);
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let weighted = values.windows(2).fold((0.0, 0_u64), |(sum, time), pair| {
        let duration = pair[1].0.saturating_sub(pair[0].0);
        (sum + pair[0].1 * duration as f64, time + duration)
    });
    MetricStats {
        samples: sorted.len() as u64,
        min: sorted[0],
        mean,
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        max: sorted[sorted.len() - 1],
        time_weighted_mean: if weighted.1 == 0 {
            mean
        } else {
            weighted.0 / weighted.1 as f64
        },
    }
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn insert_comparison(
    output: &mut BTreeMap<String, MetricComparison>,
    name: &str,
    radixdb: f64,
    postgresql: f64,
) {
    output.insert(
        name.into(),
        MetricComparison {
            radixdb,
            postgresql,
            radixdb_over_postgresql: (postgresql != 0.0).then_some(radixdb / postgresql),
        },
    );
}

fn field<'a>(detail: &'a str, name: &str) -> Option<&'a str> {
    detail.split_ascii_whitespace().find_map(|field| {
        field
            .strip_prefix(name)
            .and_then(|field| field.strip_prefix('='))
    })
}

fn validate_pair_id(pair_id: &str) -> Result<(), String> {
    if pair_id.is_empty()
        || pair_id.len() > 117
        || !pair_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err("pair_id must contain 1..=117 safe label bytes".into());
    }
    Ok(())
}

fn validate_pair_identity_and_order(
    pair_id: &str,
    radixdb: &RunSummary,
    postgresql: &RunSummary,
) -> Result<(), String> {
    if radixdb.database_engine != DatabaseEngine::Radixdb
        || postgresql.database_engine != DatabaseEngine::Postgresql
    {
        return Err("comparison requires a RadixDB run and a PostgreSQL run".into());
    }
    if radixdb.state != RunState::Passed || postgresql.state != RunState::Passed {
        return Err("A/B comparison requires two fully passed runs".into());
    }
    if postgresql.started_unix_millis >= radixdb.started_unix_millis {
        return Err(
            "A/B order violation: PostgreSQL must start before the paired RadixDB run".into(),
        );
    }
    let expected_postgresql_run_id = format!("{pair_id}-postgresql");
    let expected_radixdb_run_id = format!("{pair_id}-radixdb");
    if postgresql.run_id != expected_postgresql_run_id || radixdb.run_id != expected_radixdb_run_id
    {
        return Err(format!(
            "A/B identity mismatch: expected runs {expected_postgresql_run_id} and {expected_radixdb_run_id}"
        ));
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|error| format!("parse {}: {error}", path.display()))
}

fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, String> {
    let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some((index, other)),
        })
        .map(|(index, line)| {
            let line = line.map_err(|error| format!("read {}: {error}", path.display()))?;
            serde_json::from_str(&line)
                .map_err(|error| format!("parse {} line {}: {error}", path.display(), index + 1))
        })
        .collect()
}

fn read_optional_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, String> {
    if path.exists() {
        read_json_lines(path)
    } else {
        Ok(Vec::new())
    }
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    write_new(path, &bytes)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("write {}: {error}", path.display()))
}

fn run_markdown(summary: &RunSummary) -> String {
    let mut output = format!(
        "# Soak summary: {}\n\n- Engine: `{}`\n- State: `{:?}`\n- Samples: `{}`\n- Elapsed: `{:.3}` s\n- Seed: `{}` ms\n- Evidence SHA256: `{}`\n\n## Workload phases\n\n| Phase | Samples | Duration, s | TPS avg | TPS p95 | Latency p95, us | RSS peak, MiB |\n|---|---:|---:|---:|---:|---:|---:|\n",
        summary.run_id,
        summary.database_engine.as_str(),
        summary.state,
        summary.sample_count,
        summary.elapsed_millis as f64 / 1_000.0,
        summary.seed_elapsed_millis.map_or_else(|| "n/a".into(), |value| value.to_string()),
        summary.evidence_sha256.as_deref().unwrap_or("not-finalized"),
    );
    for phase in &summary.phases {
        output.push_str(&format!(
            "| {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.2} |\n",
            phase.name,
            phase.sample_count,
            phase.duration_millis as f64 / 1_000.0,
            phase.tps.time_weighted_mean,
            phase.tps.p95,
            phase.latency_p95_micros.time_weighted_mean,
            phase.rss_bytes.max / 1_048_576.0,
        ));
    }
    output.push_str("\n## Invariants\n\n| Invariant | State | Checks | Failures | Mean interval, ms | Max interval, ms | Detail |\n|---|---|---:|---:|---:|---:|---|\n");
    for invariant in &summary.invariants {
        output.push_str(&format!(
            "| {} | {:?} | {} | {} | {} | {} | {} |\n",
            invariant.name,
            invariant.state,
            invariant.checks,
            invariant.failures,
            invariant
                .mean_interval_millis
                .map_or_else(|| "n/a".into(), |value| format!("{value:.3}")),
            invariant
                .max_interval_millis
                .map_or_else(|| "n/a".into(), |value| value.to_string()),
            invariant.detail.replace('|', "\\|"),
        ));
    }
    output.push_str("\n## Resource metrics\n\n| Metric | Mean | Time-weighted mean | p95 | p99 | Max |\n|---|---:|---:|---:|---:|---:|\n");
    for (name, metric) in &summary.resources.metrics {
        output.push_str(&format!(
            "| {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            name, metric.mean, metric.time_weighted_mean, metric.p95, metric.p99, metric.max
        ));
    }
    output
}

fn comparison_markdown(report: &ComparisonReport) -> String {
    let mut output = format!(
        "# RadixDB / PostgreSQL 6h soak comparison\n\n- Pair: `{}`\n- Order: `PostgreSQL -> RadixDB`\n- RadixDB run: `{}`\n- PostgreSQL run: `{}`\n- Seed: `{}`\n- Rows: `{}`\n- Ladder: `{:?}`\n\n| Metric | RadixDB | PostgreSQL | RadixDB / PostgreSQL |\n|---|---:|---:|---:|\n",
        report.pair_id,
        report.radixdb.run_id,
        report.postgresql.run_id,
        report.radixdb.seed,
        report.radixdb.target_rows,
        report.radixdb.client_steps,
    );
    for (name, metric) in &report.metrics {
        output.push_str(&format!(
            "| {} | {:.6} | {:.6} | {} |\n",
            name,
            metric.radixdb,
            metric.postgresql,
            metric
                .radixdb_over_postgresql
                .map_or_else(|| "n/a".into(), |value| format!("{value:.6}")),
        ));
    }
    output.push_str("\n## Invariants\n\n| Invariant | RadixDB checks/failures | PostgreSQL checks/failures |\n|---|---:|---:|\n");
    for left in &report.radixdb.invariants {
        let right = report
            .postgresql
            .invariants
            .iter()
            .find(|candidate| candidate.name == left.name);
        output.push_str(&format!(
            "| {} | {}/{} | {} |\n",
            left.name,
            left.checks,
            left.failures,
            right.map_or_else(
                || "missing".into(),
                |right| format!("{}/{}", right.checks, right.failures)
            ),
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_include_real_time_weighting_and_tail_percentiles() {
        let metric = stats([(0, 10.0), (1_000, 20.0), (4_000, 100.0)]);
        assert_eq!(metric.samples, 3);
        assert_eq!(metric.p50, 20.0);
        assert_eq!(metric.p95, 100.0);
        assert_eq!(metric.time_weighted_mean, 17.5);
    }

    #[test]
    fn pair_id_is_safe_for_engine_qualified_run_ids() {
        assert!(validate_pair_id("pair-01").is_ok());
        assert!(validate_pair_id("").is_err());
        assert!(validate_pair_id("pair/01").is_err());
        assert!(validate_pair_id(&"x".repeat(118)).is_err());
    }

    #[test]
    fn cumulative_delta_survives_server_pid_reset() {
        fn sample(at: u64, read: u64) -> SampleInput {
            SampleInput {
                unix_millis: at,
                active_clients: 1,
                current_tps: 1.0,
                latency: LatencySnapshot::default(),
                resources: ResourceSnapshot {
                    process_read_bytes: read,
                    ..ResourceSnapshot::default()
                },
                server_runtime: ServerRuntimeSnapshot::default(),
            }
        }
        let samples = [sample(0, 100), sample(1, 150), sample(2, 20), sample(3, 30)];
        assert_eq!(
            cumulative_delta(&samples, |sample| sample.resources.process_read_bytes),
            80
        );
    }

    #[test]
    fn run_summary_reads_immutable_artifacts_and_invariant_intervals() {
        let temporary = tempfile::tempdir().unwrap();
        let run_dir = temporary.path();
        fs::write(
            run_dir.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "run_id": "summary-test",
                "profile": "6h",
                "duration_millis": 21_600_000,
                "seed": 7,
                "database_engine": "radixdb",
                "client_steps": [16, 32, 64, 128, 256],
                "active_rows": 100_000_000,
                "server_identity": "server-test",
                "started_unix_millis": 1_000
            }))
            .unwrap(),
        )
        .unwrap();
        let mut invariants = BTreeMap::new();
        invariants.insert(
            "cardinality".into(),
            InvariantStatus {
                name: "cardinality".into(),
                state: InvariantState::Passed,
                checks: 2,
                failures: 0,
                last_checked_unix_millis: Some(2_000),
                detail: "rows=100000000".into(),
            },
        );
        let status = StatusSnapshot {
            format: 1,
            run_id: "summary-test".into(),
            profile: "6h".into(),
            seed: 7,
            state: RunState::Passed,
            phase: "passed".into(),
            started_unix_millis: 1_000,
            updated_unix_millis: 3_000,
            elapsed_millis: 2_000,
            remaining_millis: 0,
            last_progress_unix_millis: 2_900,
            watchdog_timeout_millis: 1_000,
            watchdog_silence_millis: 0,
            watchdog_state: crate::status::WatchdogState::Terminal,
            active_clients: 0,
            target_clients: 0,
            current_tps: 0.0,
            identity: crate::status::BuildIdentity {
                soak: "test".into(),
                server: Some("server-test".into()),
                cargo_lock_sha256: "test".into(),
            },
            counters: Counters::default(),
            latency: LatencySnapshot::default(),
            resources: ResourceSnapshot::default(),
            resource_slopes: crate::status::ResourceSlopes::default(),
            server_runtime: ServerRuntimeSnapshot::default(),
            invariants,
            logical_digest: Some("digest".into()),
            failure: None,
        };
        fs::write(
            run_dir.join("status.json"),
            serde_json::to_vec(&status).unwrap(),
        )
        .unwrap();
        let sample = |sequence, unix_millis, clients, tps, rss| {
            serde_json::json!({
                "sequence": sequence,
                "unix_millis": unix_millis,
                "elapsed_millis": unix_millis - 1_000,
                "active_clients": clients,
                "current_tps": tps,
                "counters": Counters::default(),
                "latency": LatencySnapshot::default(),
                "resources": ResourceSnapshot { rss_bytes: rss, ..ResourceSnapshot::default() },
                "resource_slopes": crate::status::ResourceSlopes::default(),
                "server_runtime": ServerRuntimeSnapshot::default()
            })
        };
        fs::write(
            run_dir.join("samples.jsonl"),
            format!(
                "{}\n{}\n",
                sample(1, 1_000, 0, 0.0, 100),
                sample(2, 2_000, 16, 10.0, 200)
            ),
        )
        .unwrap();
        let events = [
            RunEvent {
                sequence: 1,
                unix_millis: 1_000,
                kind: "seed_started".into(),
                detail: "target_rows=100000000".into(),
            },
            RunEvent {
                sequence: 2,
                unix_millis: 1_500,
                kind: "invariant_passed".into(),
                detail: "cardinality: rows=100000000".into(),
            },
            RunEvent {
                sequence: 3,
                unix_millis: 2_000,
                kind: "phase".into(),
                detail: "clients-16".into(),
            },
            RunEvent {
                sequence: 4,
                unix_millis: 2_500,
                kind: "invariant_passed".into(),
                detail: "cardinality: rows=100000000".into(),
            },
        ];
        fs::write(
            run_dir.join("events.jsonl"),
            events
                .iter()
                .map(|event| serde_json::to_string(event).unwrap())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let progress = SemanticProgressSnapshot {
            format: crate::diagnostics::DIAGNOSTIC_FORMAT_V2,
            sequence: 1,
            phase_epoch: 1,
            phase: "clients-16".into(),
            phase_started_unix_millis: 1_000,
            workload_epoch: 1,
            workload_units: 1,
            last_workload_progress_unix_millis: 1_000,
            operation_epoch: 0,
            active_operation: None,
            last_operation_progress_unix_millis: 1_000,
            planned_silence: None,
        };
        fs::write(
            run_dir.join("semantic-progress.jsonl"),
            serde_json::to_string(&progress).unwrap(),
        )
        .unwrap();

        let summary = summarize_run(run_dir).unwrap();
        assert_eq!(summary.sample_count, 2);
        assert_eq!(summary.seed_elapsed_millis, Some(1_000));
        assert_eq!(summary.resources.metrics["rss_bytes"].max, 200.0);
        assert_eq!(summary.invariants[0].mean_interval_millis, Some(1_000.0));

        let mut radixdb = summary.clone();
        radixdb.run_id = "pair-01-radixdb".into();
        radixdb.started_unix_millis = 2_000;
        let mut postgresql = summary;
        postgresql.run_id = "pair-01-postgresql".into();
        postgresql.database_engine = DatabaseEngine::Postgresql;
        assert!(validate_pair_identity_and_order("pair-01", &radixdb, &postgresql).is_ok());
        postgresql.started_unix_millis = 3_000;
        assert!(validate_pair_identity_and_order("pair-01", &radixdb, &postgresql).is_err());
    }
}
