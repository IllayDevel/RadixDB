use super::model::{
    DiagnosticAlertV2, DiagnosticConfidence, DiagnosticMetricFrameV2, DiagnosticSeverity,
    DiagnosticState, EvidenceSignal, DIAGNOSTIC_FORMAT_V2,
};

use std::collections::BTreeMap;
use std::io::BufRead;

#[derive(Clone, Copy, Debug)]
pub struct DetectorConfig {
    pub workload_stall_millis: u64,
    pub agent_heartbeat_timeout_millis: u64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            workload_stall_millis: 60_000,
            agent_heartbeat_timeout_millis: 30_000,
        }
    }
}

pub fn replay(
    frames: &[DiagnosticMetricFrameV2],
    config: DetectorConfig,
) -> Result<Vec<DiagnosticAlertV2>, String> {
    let mut detector = DiagnosticDetector::new(config)?;
    let mut alerts = Vec::new();
    for frame in frames {
        for transition in detector.observe(frame)? {
            if transition.state == DiagnosticState::Recovered {
                alerts.push(transition);
            }
        }
    }
    alerts.extend(detector.active_alerts());
    Ok(alerts)
}

pub struct DiagnosticDetector {
    config: DetectorConfig,
    previous_sequence: u64,
    active: BTreeMap<String, DiagnosticAlertV2>,
    baseline_gauges: BTreeMap<String, f64>,
    disabled_rule: Option<String>,
}

impl DiagnosticDetector {
    pub fn new(config: DetectorConfig) -> Result<Self, String> {
        if config.workload_stall_millis == 0 || config.agent_heartbeat_timeout_millis == 0 {
            return Err("detector timeouts must be non-zero".into());
        }
        Ok(Self {
            config,
            previous_sequence: 0,
            active: BTreeMap::new(),
            baseline_gauges: BTreeMap::new(),
            disabled_rule: None,
        })
    }

    #[cfg(test)]
    fn with_disabled_rule(mut self, kind: &str) -> Self {
        self.disabled_rule = Some(kind.into());
        self
    }

    pub fn observe(
        &mut self,
        frame: &DiagnosticMetricFrameV2,
    ) -> Result<Vec<DiagnosticAlertV2>, String> {
        frame.validate()?;
        if frame.sequence <= self.previous_sequence {
            return Err("diagnostic replay frames must have increasing sequence".into());
        }
        self.previous_sequence = frame.sequence;
        let mut transitions = Vec::new();
        let agent_silence = frame
            .heartbeats
            .agent_unix_millis
            .map(|heartbeat| frame.unix_millis.saturating_sub(heartbeat));
        let agent_alive = agent_silence
            .is_some_and(|silence| silence <= self.config.agent_heartbeat_timeout_millis);
        let terminal = matches!(
            frame.progress.phase.as_str(),
            "passed" | "failed" | "interrupted"
        );
        let phase_expects_workload = frame.progress.phase.starts_with("clients-")
            || frame.progress.phase.starts_with("seeding");
        let planned_silence_active = frame
            .progress
            .planned_silence
            .as_ref()
            .is_some_and(|silence| frame.unix_millis <= silence.deadline_unix_millis);
        let workload_silence = frame
            .unix_millis
            .saturating_sub(frame.progress.last_workload_progress_unix_millis);
        let raw_workload_stall = agent_alive
            && phase_expects_workload
            && workload_silence > self.config.workload_stall_millis;
        let engine_maintenance_active = gauge(frame, "engine.seal_running") >= 1.0
            || gauge(frame, "engine.compaction_running") >= 1.0
            || gauge(frame, "engine.checkpoint_running") >= 1.0;
        let maintenance_age_millis = [
            "engine.maintenance.seal.elapsed_millis",
            "engine.maintenance.compaction.elapsed_millis",
            "engine.maintenance.checkpoint.elapsed_millis",
        ]
        .into_iter()
        .map(|name| gauge(frame, name))
        .fold(0.0_f64, f64::max);
        let maintenance_progress_rate = [
            "engine.counter.seal_rows",
            "engine.counter.seal_output_bytes",
            "engine.counter.volume_read_bytes",
            "engine.counter.wal_write_bytes",
            "engine.maintenance.seal.total_output_bytes",
            "engine.maintenance.compaction.total_output_bytes",
            "engine.maintenance.checkpoint.completed",
        ]
        .into_iter()
        .map(|name| rate(frame, name))
        .sum::<f64>();
        let maintenance_grace_millis = self.config.workload_stall_millis.saturating_mul(2) as f64;
        let maintenance_within_corridor = planned_silence_active
            || (engine_maintenance_active
                && (maintenance_progress_rate > 0.0
                    || maintenance_age_millis <= maintenance_grace_millis));
        let causal_stall = raw_workload_stall && !planned_silence_active;
        let workload_stalled = causal_stall && !maintenance_within_corridor;
        self.apply_rule(
            frame,
            "workload_stall",
            workload_stalled,
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "semantic_progress_silence_millis".into(),
                observed: workload_silence as f64,
                expected: Some(self.config.workload_stall_millis as f64),
                unit: "milliseconds".into(),
                detail: "agent heartbeat is live while workload epoch is unchanged".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "expected_maintenance",
            raw_workload_stall && maintenance_within_corridor,
            DiagnosticSeverity::Info,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "engine_maintenance_active".into(),
                observed: 1.0,
                expected: Some(1.0),
                unit: "boolean".into(),
                detail: "semantic progress is quiet while declared maintenance is active or engine maintenance remains inside its bounded progress corridor".into(),
            },
            &mut transitions,
        )?;

        let server_cpu_ticks =
            rate(frame, "process.server.user_ticks") + rate(frame, "process.server.system_ticks");
        let disk_busy_millis = rate(frame, "disk.io_millis");
        let io_psi = gauge(frame, "host.psi.io.some_avg10");
        self.apply_rule(
            frame,
            "cpu_bound_engine",
            causal_stall && server_cpu_ticks >= 50.0 && disk_busy_millis < 200.0 && io_psi < 5.0,
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "server_cpu_ticks_per_second".into(),
                observed: server_cpu_ticks,
                expected: Some(50.0),
                unit: "ticks_per_second".into(),
                detail: "server consumes CPU while storage busy time and I/O pressure stay low"
                    .into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "storage_bound_engine",
            causal_stall && (disk_busy_millis >= 500.0 || io_psi >= 10.0),
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "storage_pressure".into(),
                observed: disk_busy_millis.max(io_psi),
                expected: Some(if io_psi >= 10.0 { 10.0 } else { 500.0 }),
                unit: "mixed_pressure_score".into(),
                detail: "semantic progress stopped while disk busy time or I/O PSI is elevated"
                    .into(),
            },
            &mut transitions,
        )?;
        let reclaim_rate =
            rate(frame, "host.vmstat.pgscan_kswapd") + rate(frame, "host.vmstat.pgscan_direct");
        let swap_rate = rate(frame, "host.vmstat.pswpin") + rate(frame, "host.vmstat.pswpout");
        let major_fault_rate = rate(frame, "process.server.major_faults");
        self.apply_rule(
            frame,
            "memory_reclaim_or_swap",
            causal_stall
                && gauge(frame, "host.psi.memory.some_avg10") >= 5.0
                && (reclaim_rate > 0.0 || swap_rate > 0.0 || major_fault_rate >= 1.0),
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "memory_pressure_activity".into(),
                observed: reclaim_rate + swap_rate + major_fault_rate,
                expected: Some(0.0),
                unit: "events_per_second".into(),
                detail: "memory PSI coincides with reclaim, swap or major faults".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "lock_or_backpressure_stall",
            causal_stall
                && gauge(frame, "engine.transaction_wait_edges") >= 1.0
                && gauge(frame, "engine.oldest_transaction_age_millis")
                    > self.config.workload_stall_millis as f64
                && rate(frame, "engine.counter.runtime_profile.wait_nanos") > 0.0
                && server_cpu_ticks < 10.0
                && disk_busy_millis < 100.0,
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "engine_transaction_wait_edges".into(),
                observed: gauge(frame, "engine.transaction_wait_edges"),
                expected: Some(0.0),
                unit: "wait_edges".into(),
                detail: "old transaction and wait graph persist while cumulative lock wait grows and CPU/storage stay idle".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "seal_backlog",
            gauge(frame, "engine.hot_bytes") >= 64.0 * 1024.0 * 1024.0
                && gauge(frame, "engine.pressure_seal_requested") >= 1.0
                && (gauge(frame, "engine.seal_running") < 1.0
                    || (gauge(frame, "engine.maintenance.seal.elapsed_millis")
                        > maintenance_grace_millis
                        && rate(frame, "engine.counter.seal_output_bytes") == 0.0)),
            DiagnosticSeverity::Warning,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "engine_hot_bytes".into(),
                observed: gauge(frame, "engine.hot_bytes"),
                expected: Some(64.0 * 1024.0 * 1024.0),
                unit: "bytes".into(),
                detail: "hot owner crossed the diagnostic floor while seal is absent or no longer produces output inside its corridor".into(),
            },
            &mut transitions,
        )?;
        let compaction_rate = rate(frame, "engine.counter.compaction_calls");
        let compaction_cpu = rate(frame, "engine.counter.compaction_nanos");
        let generated_output_bytes = rate(
            frame,
            "engine.maintenance.compaction_cost.total_generated_output_bytes",
        );
        let wasted_output_bytes = rate(
            frame,
            "engine.maintenance.compaction_cost.total_wasted_output_bytes",
        );
        let discarded_jobs = rate(frame, "engine.maintenance.compaction_cost.jobs_invalidated")
            + rate(frame, "engine.maintenance.compaction_cost.jobs_failed")
            + rate(
                frame,
                "engine.maintenance.compaction_cost.jobs_cancelled_schema_epoch",
            )
            + rate(
                frame,
                "engine.maintenance.compaction_cost.jobs_cancelled_input_snapshot",
            )
            + rate(
                frame,
                "engine.maintenance.compaction_cost.jobs_cancelled_budget",
            )
            + rate(
                frame,
                "engine.maintenance.compaction_cost.jobs_cancelled_other",
            );
        let compaction_waste_ratio = wasted_output_bytes / generated_output_bytes.max(1.0);
        self.apply_rule(
            frame,
            "compaction_churn",
            compaction_rate > 0.0
                && compaction_cpu >= 800_000_000.0
                && generated_output_bytes > 0.0
                && wasted_output_bytes > 0.0
                && discarded_jobs > 0.0
                && compaction_waste_ratio > 0.05,
            DiagnosticSeverity::Warning,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "compaction_wasted_output_ratio".into(),
                observed: compaction_waste_ratio,
                expected: Some(0.05),
                unit: "wasted_output_bytes_per_generated_output_byte".into(),
                detail: "expensive compaction discarded more than five percent of generated output and reported an invalidated, cancelled or failed job".into(),
            },
            &mut transitions,
        )?;
        let cache_misses = rate(frame, "engine.counter.ram_accelerator_misses");
        let cache_hits = rate(frame, "engine.counter.ram_accelerator_hits");
        let cache_evictions = rate(frame, "engine.counter.ram_accelerator_evictions");
        self.apply_rule(
            frame,
            "cache_thrash",
            cache_misses >= 100.0 && cache_misses > cache_hits * 4.0 && cache_evictions > 0.0,
            DiagnosticSeverity::Warning,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "cache_miss_hit_ratio".into(),
                observed: cache_misses / cache_hits.max(1.0),
                expected: Some(4.0),
                unit: "ratio".into(),
                detail: "cache misses dominate hits while eviction remains active".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "wal_checkpoint_stall",
            causal_stall
                && gauge(frame, "engine.checkpoint_running") >= 1.0
                && gauge(frame, "engine.maintenance.checkpoint.elapsed_millis")
                    > maintenance_grace_millis
                && gauge(frame, "engine.wal_pending_durability_bytes") > 0.0
                && rate(frame, "engine.maintenance.checkpoint.last_result_marker") == 0.0,
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "wal_pending_durability_bytes".into(),
                observed: gauge(frame, "engine.wal_pending_durability_bytes"),
                expected: Some(0.0),
                unit: "bytes".into(),
                detail: "checkpoint exceeded its corridor while durability backlog remains and no checkpoint LSN is published".into(),
            },
            &mut transitions,
        )?;

        for name in [
            "process.server.open_fds",
            "process.server.threads",
            "process.server.socket_fds",
            "process.server.rss_bytes",
            "engine.owner.authenticated_sessions",
            "engine.owner.active_cursors",
            "engine.owner.prepared_statements",
            "engine.owner.active_executions",
            "engine.cold_exact_index_bytes",
            "engine.cold_ordered_index_bytes",
        ] {
            self.baseline_gauges
                .entry(name.into())
                .or_insert_with(|| gauge(frame, name));
        }
        let posting_growth = gauge(frame, "engine.cold_exact_index_bytes")
            + gauge(frame, "engine.cold_ordered_index_bytes")
            - self
                .baseline_gauges
                .get("engine.cold_exact_index_bytes")
                .copied()
                .unwrap_or(0.0)
            - self
                .baseline_gauges
                .get("engine.cold_ordered_index_bytes")
                .copied()
                .unwrap_or(0.0);
        let rss_growth = gauge(frame, "process.server.rss_bytes")
            - self
                .baseline_gauges
                .get("process.server.rss_bytes")
                .copied()
                .unwrap_or(0.0);
        self.apply_rule(
            frame,
            "resident_posting_growth",
            posting_growth >= 64.0 * 1024.0 * 1024.0
                && rss_growth >= posting_growth * 0.5,
            DiagnosticSeverity::Warning,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "resident_posting_growth_bytes".into(),
                observed: posting_growth,
                expected: Some(64.0 * 1024.0 * 1024.0),
                unit: "bytes_above_baseline".into(),
                detail: "exact/ordered posting ownership and server RSS grow together instead of remaining descriptor-backed".into(),
            },
            &mut transitions,
        )?;
        let fd_growth = gauge(frame, "process.server.open_fds")
            - self
                .baseline_gauges
                .get("process.server.open_fds")
                .copied()
                .unwrap_or(0.0);
        let thread_growth = gauge(frame, "process.server.threads")
            - self
                .baseline_gauges
                .get("process.server.threads")
                .copied()
                .unwrap_or(0.0);
        let socket_growth = gauge(frame, "process.server.socket_fds")
            - self
                .baseline_gauges
                .get("process.server.socket_fds")
                .copied()
                .unwrap_or(0.0);
        let owner_growth = [
            "engine.owner.authenticated_sessions",
            "engine.owner.active_cursors",
            "engine.owner.prepared_statements",
            "engine.owner.active_executions",
        ]
        .into_iter()
        .map(|name| gauge(frame, name) - self.baseline_gauges.get(name).copied().unwrap_or(0.0))
        .fold(0.0_f64, f64::max);
        self.apply_rule(
            frame,
            "process_resource_leak",
            terminal
                && (fd_growth >= 64.0
                    || thread_growth >= 8.0
                    || socket_growth >= 32.0
                    || owner_growth >= 8.0),
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Probable,
            EvidenceSignal {
                name: "process_resource_growth".into(),
                observed: fd_growth
                    .max(thread_growth)
                    .max(socket_growth)
                    .max(owner_growth),
                expected: Some(0.0),
                unit: "owners_above_baseline".into(),
                detail: "server resources did not return near the run-start baseline at terminal quiescence".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "tcp_or_listener_failure",
            !terminal
                && !planned_silence_active
                && frame.heartbeats.server_unix_millis.is_some()
                && gauge(frame, "engine.sample_error") >= 1.0,
            DiagnosticSeverity::Incident,
            DiagnosticConfidence::Possible,
            EvidenceSignal {
                name: "engine_diagnostic_connection_failed".into(),
                observed: gauge(frame, "engine.sample_error"),
                expected: Some(0.0),
                unit: "boolean".into(),
                detail: "server process exists but the independent diagnostic connection failed"
                    .into(),
            },
            &mut transitions,
        )?;
        let oom_events = rate(frame, "cgroup.server.memory_events.oom")
            + rate(frame, "cgroup.server.memory_events.oom_kill")
            + rate(frame, "host.vmstat.oom_kill");
        self.apply_rule(
            frame,
            "oom_or_cgroup_kill",
            oom_events > 0.0,
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "oom_events_per_second".into(),
                observed: oom_events,
                expected: Some(0.0),
                unit: "events_per_second".into(),
                detail: "kernel or cgroup OOM counter advanced".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "observer_failure",
            gauge(frame, "observer.resumed_after_interruption") >= 1.0
                || gauge(frame, "observer.sampling_gap_exceeded") >= 1.0,
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "observer_lifecycle_discontinuity".into(),
                observed: gauge(frame, "observer.resumed_after_interruption")
                    .max(gauge(frame, "observer.sampling_gap_exceeded")),
                expected: Some(0.0),
                unit: "boolean".into(),
                detail: "observer restarted or its independent sampling deadline was missed".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "host_power_loss",
            gauge(frame, "observer.boot_identity_changed") >= 1.0,
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "observer_boot_identity_changed".into(),
                observed: gauge(frame, "observer.boot_identity_changed"),
                expected: Some(0.0),
                unit: "boolean".into(),
                detail: "observer resumed under a different kernel boot identity".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "disk_degradation",
            gauge(frame, "host.disk_degradation") >= 1.0,
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "host_disk_degradation".into(),
                observed: gauge(frame, "host.disk_degradation"),
                expected: Some(0.0),
                unit: "boolean".into(),
                detail: "SMART, kernel I/O or filesystem evidence reports physical degradation"
                    .into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "agent_failure",
            !terminal
                && ((gauge(frame, "observer.agent_identity_checked") >= 1.0
                    && gauge(frame, "process.agent.present") < 1.0)
                    || gauge(frame, "process.agent.state_stopped") >= 1.0
                    || agent_silence.is_some_and(|silence| {
                        silence > self.config.agent_heartbeat_timeout_millis
                    })),
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "agent_heartbeat_silence_millis".into(),
                observed: (agent_silence.unwrap_or(0) as f64)
                    .max(
                        gauge(frame, "process.agent.state_stopped")
                            * self.config.agent_heartbeat_timeout_millis as f64,
                    )
                    .max(
                        (1.0 - gauge(frame, "process.agent.present"))
                            * self.config.agent_heartbeat_timeout_millis as f64,
                    ),
                expected: Some(self.config.agent_heartbeat_timeout_millis as f64),
                unit: "milliseconds".into(),
                detail: "observer is live but agent heartbeat deadline expired".into(),
            },
            &mut transitions,
        )?;
        self.apply_rule(
            frame,
            "server_failure",
            !terminal
                && !planned_silence_active
                && frame.gauges.get("observer.server_identity_checked") == Some(&1.0)
                && (frame.heartbeats.server_unix_millis.is_none()
                    || gauge(frame, "process.server.state_stopped") >= 1.0),
            DiagnosticSeverity::Fatal,
            DiagnosticConfidence::Confirmed,
            EvidenceSignal {
                name: "server_process_present".into(),
                observed: f64::from(
                    frame.heartbeats.server_unix_millis.is_some()
                        && gauge(frame, "process.server.state_stopped") < 1.0,
                ),
                expected: Some(1.0),
                unit: "boolean".into(),
                detail: "exact expected server executable is absent".into(),
            },
            &mut transitions,
        )?;
        Ok(transitions)
    }

    pub fn active_alerts(&self) -> Vec<DiagnosticAlertV2> {
        self.active.values().cloned().collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_rule(
        &mut self,
        frame: &DiagnosticMetricFrameV2,
        kind: &str,
        triggered: bool,
        severity: DiagnosticSeverity,
        confidence: DiagnosticConfidence,
        evidence: EvidenceSignal,
        transitions: &mut Vec<DiagnosticAlertV2>,
    ) -> Result<(), String> {
        if self.disabled_rule.as_deref() == Some(kind) {
            return Ok(());
        }
        match (triggered, self.active.get_mut(kind)) {
            (true, None) => {
                let alert = DiagnosticAlertV2 {
                    format: DIAGNOSTIC_FORMAT_V2,
                    id: format!("{}-{}", kind.replace('_', "-"), frame.sequence),
                    kind: kind.into(),
                    severity,
                    confidence,
                    state: DiagnosticState::Active,
                    first_unix_millis: frame.unix_millis,
                    last_unix_millis: frame.unix_millis,
                    first_frame_sequence: frame.sequence,
                    last_frame_sequence: frame.sequence,
                    evidence: vec![evidence],
                    counter_evidence: counter_evidence(frame, kind),
                };
                alert.validate()?;
                transitions.push(alert.clone());
                self.active.insert(kind.into(), alert);
            }
            (true, Some(alert)) => {
                alert.last_unix_millis = frame.unix_millis;
                alert.last_frame_sequence = frame.sequence;
                alert.evidence[0] = evidence;
                alert.counter_evidence = counter_evidence(frame, kind);
            }
            (false, Some(_)) => {
                let mut alert = self.active.remove(kind).unwrap();
                alert.state = DiagnosticState::Recovered;
                alert.last_unix_millis = frame.unix_millis;
                alert.last_frame_sequence = frame.sequence;
                alert.validate()?;
                transitions.push(alert);
            }
            (false, None) => {}
        }
        Ok(())
    }
}

fn gauge(frame: &DiagnosticMetricFrameV2, name: &str) -> f64 {
    frame.gauges.get(name).copied().unwrap_or(0.0)
}

fn rate(frame: &DiagnosticMetricFrameV2, counter: &str) -> f64 {
    gauge(frame, &format!("rate.{counter}_per_second"))
}

fn counter_evidence(frame: &DiagnosticMetricFrameV2, kind: &str) -> Vec<EvidenceSignal> {
    let signal = match kind {
        "workload_stall" => EvidenceSignal {
            name: "counter_maintenance_progress".into(),
            observed: rate(frame, "engine.counter.seal_output_bytes")
                + rate(frame, "engine.maintenance.compaction.total_output_bytes")
                + rate(frame, "engine.maintenance.checkpoint.completed"),
            expected: Some(1.0),
            unit: "progress_per_second".into(),
            detail: "counter-signal checked: advancing maintenance could explain semantic silence"
                .into(),
        },
        "cpu_bound_engine" => EvidenceSignal {
            name: "counter_storage_pressure".into(),
            observed: rate(frame, "disk.io_millis").max(gauge(frame, "host.psi.io.some_avg10")),
            expected: Some(500.0),
            unit: "pressure_score".into(),
            detail:
                "counter-signal checked: high storage pressure would weaken a CPU-bound diagnosis"
                    .into(),
        },
        "storage_bound_engine" => EvidenceSignal {
            name: "counter_server_cpu_ticks_per_second".into(),
            observed: rate(frame, "process.server.user_ticks")
                + rate(frame, "process.server.system_ticks"),
            expected: Some(50.0),
            unit: "ticks_per_second".into(),
            detail:
                "counter-signal checked: dominant server CPU would weaken a storage-bound diagnosis"
                    .into(),
        },
        "memory_reclaim_or_swap" => EvidenceSignal {
            name: "counter_memory_pressure_below_threshold".into(),
            observed: gauge(frame, "host.psi.memory.some_avg10"),
            expected: Some(5.0),
            unit: "psi_avg10".into(),
            detail: "counter-signal checked: low memory PSI would weaken reclaim attribution"
                .into(),
        },
        "lock_or_backpressure_stall" => EvidenceSignal {
            name: "counter_active_cpu_or_storage".into(),
            observed: (rate(frame, "process.server.user_ticks")
                + rate(frame, "process.server.system_ticks"))
            .max(rate(frame, "disk.io_millis")),
            expected: Some(100.0),
            unit: "activity_score".into(),
            detail:
                "counter-signal checked: active compute or I/O would weaken an idle-wait diagnosis"
                    .into(),
        },
        "agent_failure" => EvidenceSignal {
            name: "counter_terminal_phase".into(),
            observed: f64::from(matches!(
                frame.progress.phase.as_str(),
                "passed" | "failed" | "interrupted"
            )),
            expected: Some(1.0),
            unit: "boolean".into(),
            detail: "counter-signal checked: a terminal run legitimately stops agent heartbeat"
                .into(),
        },
        "server_failure" | "tcp_or_listener_failure" => EvidenceSignal {
            name: "counter_planned_silence".into(),
            observed: f64::from(frame.progress.planned_silence.is_some()),
            expected: Some(1.0),
            unit: "boolean".into(),
            detail: "counter-signal checked: declared recovery could explain server unavailability"
                .into(),
        },
        _ => return Vec::new(),
    };
    vec![signal]
}

pub fn read_frames(reader: impl BufRead) -> Result<Vec<DiagnosticMetricFrameV2>, String> {
    let mut frames = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line_number = index.saturating_add(1);
        let line =
            line.map_err(|error| format!("read diagnostic trace line {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let frame: DiagnosticMetricFrameV2 = serde_json::from_str(&line)
            .map_err(|error| format!("parse diagnostic trace line {line_number}: {error}"))?;
        frame
            .validate()
            .map_err(|error| format!("validate diagnostic trace line {line_number}: {error}"))?;
        frames.push(frame);
    }
    if frames.is_empty() {
        return Err("diagnostic trace contains no frames".into());
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::diagnostics::{HeartbeatSnapshot, SemanticProgressSnapshot};

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct KnownTrace {
        name: String,
        expected: Vec<String>,
        forbidden: Vec<String>,
        frames: Vec<DiagnosticMetricFrameV2>,
    }

    struct DeterministicClock {
        millis: u64,
    }

    impl DeterministicClock {
        fn advance(&mut self, millis: u64) {
            self.millis = self.millis.saturating_add(millis);
        }
    }

    struct FakeMetricSource {
        clock: DeterministicClock,
        sequence: u64,
        workload_at: u64,
    }

    impl FakeMetricSource {
        fn frozen_workload(start_millis: u64) -> Self {
            Self {
                clock: DeterministicClock {
                    millis: start_millis,
                },
                sequence: 0,
                workload_at: start_millis,
            }
        }

        fn advance(&mut self, millis: u64) {
            self.clock.advance(millis);
        }

        fn sample(&mut self) -> DiagnosticMetricFrameV2 {
            self.sequence += 1;
            frame(
                self.sequence,
                self.clock.millis,
                self.workload_at,
                self.clock.millis,
            )
        }
    }

    fn frame(
        sequence: u64,
        now: u64,
        workload_at: u64,
        heartbeat_at: u64,
    ) -> DiagnosticMetricFrameV2 {
        DiagnosticMetricFrameV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            sequence,
            monotonic_millis: now,
            unix_millis: now,
            boot_id: "boot-1".into(),
            run_id: "run-1".into(),
            heartbeats: HeartbeatSnapshot {
                agent_unix_millis: Some(heartbeat_at),
                ..HeartbeatSnapshot::default()
            },
            progress: SemanticProgressSnapshot {
                format: DIAGNOSTIC_FORMAT_V2,
                sequence: 1,
                phase_epoch: 1,
                phase: "clients-16".into(),
                phase_started_unix_millis: 1_000,
                workload_epoch: 1,
                workload_units: 1,
                last_workload_progress_unix_millis: workload_at,
                operation_epoch: 0,
                active_operation: None,
                last_operation_progress_unix_millis: 1_000,
                planned_silence: None,
            },
            counters: BTreeMap::new(),
            gauges: BTreeMap::new(),
        }
    }

    fn kinds(frames: &[DiagnosticMetricFrameV2]) -> Vec<String> {
        let mut kinds = replay(
            frames,
            DetectorConfig {
                workload_stall_millis: 100,
                agent_heartbeat_timeout_millis: 20,
            },
        )
        .unwrap()
        .into_iter()
        .map(|alert| alert.kind)
        .collect::<Vec<_>>();
        kinds.sort();
        kinds
    }

    fn assert_kind(frames: &[DiagnosticMetricFrameV2], expected: &str) {
        let actual = kinds(frames);
        assert!(
            actual.iter().any(|kind| kind == expected),
            "missing `{expected}` in {actual:?}"
        );
    }

    fn oracle_cases() -> Vec<(&'static str, Vec<DiagnosticMetricFrameV2>)> {
        let baseline = frame(1, 1_000, 1_000, 1_000);
        let stalled = frame(2, 1_201, 1_000, 1_201);
        let live = frame(2, 1_050, 1_000, 1_050);
        let mut cases = vec![("workload_stall", vec![baseline.clone(), stalled.clone()])];

        let mut maintenance = stalled.clone();
        maintenance.gauges.insert("engine.seal_running".into(), 1.0);
        cases.push(("expected_maintenance", vec![baseline.clone(), maintenance]));

        let mut cpu = stalled.clone();
        cpu.gauges.insert("engine.seal_running".into(), 1.0);
        cpu.gauges
            .insert("engine.maintenance.seal.elapsed_millis".into(), 250.0);
        cpu.gauges
            .insert("rate.process.server.user_ticks_per_second".into(), 60.0);
        cases.push(("cpu_bound_engine", vec![baseline.clone(), cpu]));

        let mut storage = stalled.clone();
        storage
            .gauges
            .insert("engine.compaction_running".into(), 1.0);
        storage
            .gauges
            .insert("engine.maintenance.compaction.elapsed_millis".into(), 250.0);
        storage
            .gauges
            .insert("rate.disk.io_millis_per_second".into(), 600.0);
        cases.push(("storage_bound_engine", vec![baseline.clone(), storage]));

        let mut memory = stalled.clone();
        memory
            .gauges
            .insert("host.psi.memory.some_avg10".into(), 6.0);
        memory
            .gauges
            .insert("rate.host.vmstat.pgscan_kswapd_per_second".into(), 10.0);
        cases.push(("memory_reclaim_or_swap", vec![baseline.clone(), memory]));

        let mut lock = stalled.clone();
        lock.gauges
            .insert("engine.transaction_wait_edges".into(), 1.0);
        lock.gauges
            .insert("engine.oldest_transaction_age_millis".into(), 201.0);
        lock.gauges.insert(
            "rate.engine.counter.runtime_profile.wait_nanos_per_second".into(),
            1.0,
        );
        cases.push(("lock_or_backpressure_stall", vec![baseline.clone(), lock]));

        let mut seal = live.clone();
        seal.gauges
            .insert("engine.hot_bytes".into(), 64.0 * 1024.0 * 1024.0);
        seal.gauges
            .insert("engine.pressure_seal_requested".into(), 1.0);
        cases.push(("seal_backlog", vec![baseline.clone(), seal]));

        let mut compaction = live.clone();
        compaction.gauges.insert(
            "rate.engine.counter.compaction_calls_per_second".into(),
            1.0,
        );
        compaction.gauges.insert(
            "rate.engine.counter.compaction_nanos_per_second".into(),
            900_000_000.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.total_generated_output_bytes_per_second"
                .into(),
            1_000.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.total_wasted_output_bytes_per_second".into(),
            100.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.jobs_invalidated_per_second".into(),
            1.0,
        );
        cases.push(("compaction_churn", vec![baseline.clone(), compaction]));

        let mut cache = live.clone();
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_misses_per_second".into(),
            500.0,
        );
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_hits_per_second".into(),
            10.0,
        );
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_evictions_per_second".into(),
            1.0,
        );
        cases.push(("cache_thrash", vec![baseline.clone(), cache]));

        let mut postings = live.clone();
        postings.gauges.insert(
            "engine.cold_exact_index_bytes".into(),
            96.0 * 1024.0 * 1024.0,
        );
        postings
            .gauges
            .insert("process.server.rss_bytes".into(), 80.0 * 1024.0 * 1024.0);
        cases.push(("resident_posting_growth", vec![baseline.clone(), postings]));

        let mut wal = stalled.clone();
        wal.gauges.insert("engine.checkpoint_running".into(), 1.0);
        wal.gauges
            .insert("engine.maintenance.checkpoint.elapsed_millis".into(), 250.0);
        wal.gauges
            .insert("engine.wal_pending_durability_bytes".into(), 1_024.0);
        cases.push(("wal_checkpoint_stall", vec![baseline.clone(), wal]));

        let mut leak = live.clone();
        leak.progress.phase = "passed".into();
        leak.gauges
            .insert("engine.owner.authenticated_sessions".into(), 8.0);
        cases.push(("process_resource_leak", vec![baseline.clone(), leak]));

        let mut tcp = live.clone();
        tcp.heartbeats.server_unix_millis = Some(1_050);
        tcp.gauges.insert("engine.sample_error".into(), 1.0);
        cases.push(("tcp_or_listener_failure", vec![baseline.clone(), tcp]));

        let mut oom = live.clone();
        oom.gauges.insert(
            "rate.cgroup.server.memory_events.oom_kill_per_second".into(),
            1.0,
        );
        cases.push(("oom_or_cgroup_kill", vec![baseline.clone(), oom]));

        let mut power = live.clone();
        power
            .gauges
            .insert("observer.boot_identity_changed".into(), 1.0);
        cases.push(("host_power_loss", vec![baseline.clone(), power]));

        let mut observer = live.clone();
        observer
            .gauges
            .insert("observer.resumed_after_interruption".into(), 1.0);
        cases.push(("observer_failure", vec![baseline.clone(), observer]));

        let mut disk = live.clone();
        disk.gauges.insert("host.disk_degradation".into(), 1.0);
        cases.push(("disk_degradation", vec![baseline.clone(), disk]));

        let mut agent = frame(1, 1_201, 1_200, 1_000);
        agent
            .gauges
            .insert("observer.server_identity_checked".into(), 1.0);
        agent.heartbeats.server_unix_millis = Some(1_201);
        cases.push(("agent_failure", vec![agent]));

        let mut server = frame(1, 1_050, 1_000, 1_050);
        server
            .gauges
            .insert("observer.server_identity_checked".into(), 1.0);
        server.heartbeats.server_unix_millis = None;
        cases.push(("server_failure", vec![server]));
        cases
    }

    fn verify_oracle(disabled_rule: Option<&str>) -> Result<(), String> {
        for (expected, frames) in oracle_cases() {
            let config = DetectorConfig {
                workload_stall_millis: 100,
                agent_heartbeat_timeout_millis: 20,
            };
            let mut detector = DiagnosticDetector::new(config)?;
            if let Some(disabled_rule) = disabled_rule {
                detector = detector.with_disabled_rule(disabled_rule);
            }
            let mut alerts = Vec::new();
            for frame in &frames {
                alerts.extend(detector.observe(frame)?);
            }
            alerts.extend(detector.active_alerts());
            if !alerts.iter().any(|alert| alert.kind == expected) {
                return Err(format!(
                    "oracle case `{expected}` lost its required classification"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn replay_is_deterministic_and_heartbeat_does_not_hide_stall() {
        let mut source = FakeMetricSource::frozen_workload(1_000);
        let mut frames = vec![source.sample()];
        source.advance(50);
        frames.push(source.sample());
        source.advance(51);
        frames.push(source.sample());
        let config = DetectorConfig {
            workload_stall_millis: 100,
            agent_heartbeat_timeout_millis: 20,
        };

        let first = replay(&frames, config).unwrap();
        let second = replay(&frames, config).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, "workload_stall");
        assert_eq!(first[0].state, DiagnosticState::Active);
        assert_eq!(first[0].first_frame_sequence, 3);
    }

    #[test]
    fn known_semantic_stall_fixture_has_stable_classification() {
        let frames = read_frames(std::io::Cursor::new(include_bytes!(
            "../../tests/fixtures/diagnostics/semantic-stall-v2.jsonl"
        )))
        .unwrap();
        let config = DetectorConfig {
            workload_stall_millis: 100,
            agent_heartbeat_timeout_millis: 20,
        };

        let first = replay(&frames, config).unwrap();
        let encoded_first = serde_json::to_vec(&first).unwrap();
        let encoded_second = serde_json::to_vec(&replay(&frames, config).unwrap()).unwrap();

        assert_eq!(encoded_first, encoded_second);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, "workload_stall");
        assert_eq!(first[0].first_frame_sequence, 3);
    }

    #[test]
    fn anonymized_known_incident_traces_have_stable_oracles() {
        let reader = std::io::BufReader::new(std::io::Cursor::new(include_bytes!(
            "../../tests/fixtures/diagnostics/known-incidents-v2.jsonl"
        )));
        let mut cases = 0_usize;
        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();
            let trace: KnownTrace = serde_json::from_str(&line)
                .unwrap_or_else(|error| panic!("trace line {}: {error}", index + 1));
            let alerts = replay(
                &trace.frames,
                DetectorConfig {
                    workload_stall_millis: 100,
                    agent_heartbeat_timeout_millis: 20,
                },
            )
            .unwrap_or_else(|error| panic!("trace `{}`: {error}", trace.name));
            for expected in &trace.expected {
                assert!(
                    alerts.iter().any(|alert| &alert.kind == expected),
                    "trace `{}` missed `{expected}`: {alerts:?}",
                    trace.name
                );
            }
            for forbidden in &trace.forbidden {
                assert!(
                    alerts.iter().all(|alert| &alert.kind != forbidden),
                    "trace `{}` unexpectedly produced `{forbidden}`: {alerts:?}",
                    trace.name
                );
            }
            cases += 1;
        }
        assert_eq!(cases, 7);
    }

    #[test]
    fn independent_observer_distinguishes_agent_and_server_death() {
        let config = DetectorConfig {
            workload_stall_millis: 10_000,
            agent_heartbeat_timeout_millis: 100,
        };
        let mut agent_dead = frame(1, 1_201, 1_200, 1_000);
        agent_dead
            .gauges
            .insert("observer.server_identity_checked".into(), 1.0);
        agent_dead.heartbeats.server_unix_millis = Some(1_201);
        let agent_alerts = replay(&[agent_dead], config).unwrap();
        assert_eq!(agent_alerts.len(), 1);
        assert_eq!(agent_alerts[0].kind, "agent_failure");

        let mut server_dead = frame(1, 1_050, 1_000, 1_050);
        server_dead
            .gauges
            .insert("observer.server_identity_checked".into(), 1.0);
        server_dead.heartbeats.server_unix_millis = None;
        let server_alerts = replay(&[server_dead], config).unwrap();
        assert_eq!(server_alerts.len(), 1);
        assert_eq!(server_alerts[0].kind, "server_failure");
    }

    #[test]
    fn planned_recovery_suppresses_expected_server_absence() {
        let mut recovery = frame(1, 1_050, 1_000, 1_050);
        recovery
            .gauges
            .insert("observer.server_identity_checked".into(), 1.0);
        recovery.heartbeats.server_unix_millis = None;
        recovery.progress.planned_silence = Some(crate::diagnostics::PlannedSilence {
            reason: "kill_reopen".into(),
            started_unix_millis: 1_000,
            deadline_unix_millis: 2_000,
            phase_epoch: recovery.progress.phase_epoch,
        });

        assert!(replay(&[recovery], DetectorConfig::default())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn causal_rule_corpus_classifies_engine_host_and_resource_failures() {
        let baseline = frame(1, 1_000, 1_000, 1_000);
        let stalled = frame(2, 1_201, 1_000, 1_201);

        let mut cpu = stalled.clone();
        cpu.gauges
            .insert("rate.process.server.user_ticks_per_second".into(), 60.0);
        assert_kind(&[baseline.clone(), cpu], "cpu_bound_engine");

        let mut storage = stalled.clone();
        storage
            .gauges
            .insert("rate.disk.io_millis_per_second".into(), 600.0);
        assert_kind(&[baseline.clone(), storage], "storage_bound_engine");

        let mut memory = stalled.clone();
        memory
            .gauges
            .insert("host.psi.memory.some_avg10".into(), 6.0);
        memory
            .gauges
            .insert("rate.host.vmstat.pgscan_kswapd_per_second".into(), 10.0);
        assert_kind(&[baseline.clone(), memory], "memory_reclaim_or_swap");

        let mut lock = stalled.clone();
        lock.gauges
            .insert("engine.transaction_wait_edges".into(), 1.0);
        lock.gauges
            .insert("engine.oldest_transaction_age_millis".into(), 201.0);
        lock.gauges.insert(
            "rate.engine.counter.runtime_profile.wait_nanos_per_second".into(),
            1.0,
        );
        assert_kind(&[baseline.clone(), lock], "lock_or_backpressure_stall");

        let mut maintenance = stalled.clone();
        maintenance.gauges.insert("engine.seal_running".into(), 1.0);
        let maintenance_kinds = kinds(&[baseline.clone(), maintenance]);
        assert!(maintenance_kinds.contains(&"expected_maintenance".into()));
        assert!(!maintenance_kinds.contains(&"workload_stall".into()));

        let mut seal = frame(2, 1_050, 1_000, 1_050);
        seal.gauges
            .insert("engine.hot_bytes".into(), 64.0 * 1024.0 * 1024.0);
        seal.gauges
            .insert("engine.pressure_seal_requested".into(), 1.0);
        assert_kind(&[baseline.clone(), seal], "seal_backlog");

        let mut compaction = frame(2, 1_050, 1_000, 1_050);
        compaction
            .gauges
            .insert("engine.compaction_running".into(), 1.0);
        compaction.gauges.insert(
            "rate.engine.counter.compaction_calls_per_second".into(),
            1.0,
        );
        compaction.gauges.insert(
            "rate.engine.counter.compaction_nanos_per_second".into(),
            900_000_000.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.total_generated_output_bytes_per_second"
                .into(),
            1_000.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.total_wasted_output_bytes_per_second".into(),
            100.0,
        );
        compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.jobs_invalidated_per_second".into(),
            1.0,
        );
        assert_kind(&[baseline.clone(), compaction], "compaction_churn");

        let mut productive_compaction = frame(2, 1_050, 1_000, 1_050);
        productive_compaction.gauges.insert(
            "rate.engine.counter.compaction_calls_per_second".into(),
            1.0,
        );
        productive_compaction.gauges.insert(
            "rate.engine.counter.compaction_nanos_per_second".into(),
            900_000_000.0,
        );
        productive_compaction.gauges.insert(
            "rate.engine.maintenance.compaction.total_input_rows_per_second".into(),
            100.0,
        );
        productive_compaction.gauges.insert(
            "rate.engine.maintenance.compaction.total_output_rows_per_second".into(),
            100.0,
        );
        productive_compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.total_generated_output_bytes_per_second"
                .into(),
            1_000.0,
        );
        productive_compaction.gauges.insert(
            "rate.engine.maintenance.compaction_cost.jobs_published_per_second".into(),
            1.0,
        );
        let productive_kinds = kinds(&[baseline.clone(), productive_compaction]);
        assert!(
            !productive_kinds.contains(&"compaction_churn".into()),
            "published zero-waste compaction is useful work: {productive_kinds:?}"
        );

        let mut cache = frame(2, 1_050, 1_000, 1_050);
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_misses_per_second".into(),
            500.0,
        );
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_hits_per_second".into(),
            10.0,
        );
        cache.gauges.insert(
            "rate.engine.counter.ram_accelerator_evictions_per_second".into(),
            1.0,
        );
        assert_kind(&[baseline.clone(), cache], "cache_thrash");

        let mut wal = stalled.clone();
        wal.gauges.insert("engine.checkpoint_running".into(), 1.0);
        wal.gauges
            .insert("engine.maintenance.checkpoint.elapsed_millis".into(), 250.0);
        wal.gauges
            .insert("engine.wal_pending_durability_bytes".into(), 1_024.0);
        wal.gauges.insert(
            "rate.engine.maintenance.checkpoint.last_result_marker_per_second".into(),
            0.0,
        );
        assert_kind(&[baseline.clone(), wal], "wal_checkpoint_stall");

        let mut leak = frame(2, 1_050, 1_000, 1_050);
        leak.progress.phase = "passed".into();
        leak.gauges
            .insert("engine.owner.authenticated_sessions".into(), 8.0);
        assert_kind(&[baseline.clone(), leak], "process_resource_leak");

        let mut tcp = frame(2, 1_050, 1_000, 1_050);
        tcp.heartbeats.server_unix_millis = Some(1_050);
        tcp.gauges.insert("engine.sample_error".into(), 1.0);
        assert_kind(&[baseline.clone(), tcp], "tcp_or_listener_failure");

        let mut oom = frame(2, 1_050, 1_000, 1_050);
        oom.gauges.insert(
            "rate.cgroup.server.memory_events.oom_kill_per_second".into(),
            1.0,
        );
        assert_kind(&[baseline.clone(), oom], "oom_or_cgroup_kill");

        let mut power = frame(2, 1_050, 1_000, 1_050);
        power
            .gauges
            .insert("observer.boot_identity_changed".into(), 1.0);
        assert_kind(&[baseline.clone(), power], "host_power_loss");

        let mut disk = frame(2, 1_050, 1_000, 1_050);
        disk.gauges.insert("host.disk_degradation".into(), 1.0);
        assert_kind(&[baseline, disk], "disk_degradation");
    }

    #[test]
    fn oracle_corpus_detects_every_removed_rule() {
        verify_oracle(None).unwrap();
        let mut rules = oracle_cases()
            .into_iter()
            .map(|(kind, _)| kind)
            .collect::<Vec<_>>();
        rules.sort_unstable();
        rules.dedup();
        assert_eq!(rules.len(), 19, "every causal protection needs a mutation");
        for rule in rules {
            let error = verify_oracle(Some(rule))
                .expect_err("removing a required detector rule must make the oracle red");
            assert!(
                error.contains(rule),
                "mutation `{rule}` failed for an unrelated reason: {error}"
            );
        }
    }
}
