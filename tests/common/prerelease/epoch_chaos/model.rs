use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::config::DATABASE;
use super::telemetry::LayerTelemetryReport;
use super::{config::ChaosProfile, journal::JournalSummary};
use crate::common::prerelease::{tcp_command, tcp_connect_with_read_timeout};

#[derive(Clone)]
pub struct LayerContext {
    pub address: SocketAddr,
    pub seed: u64,
    pub profile: ChaosProfile,
    pub journal_root: PathBuf,
    pub progress: Arc<AtomicU64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LayerSummary {
    pub name: String,
    pub wall_millis: u64,
    pub semantic_operations: u64,
    pub transactions: u64,
    pub committed_transactions: u64,
    pub rolled_back_transactions: u64,
    pub disconnected_transactions: u64,
    pub conflicted_transactions: u64,
    pub row_lock_timeout_conflicts: u64,
    pub compaction_backpressure_conflicts: u64,
    pub rejected_statements: u64,
    pub reader_checks: u64,
    pub maintenance_attempts: u64,
    pub maintenance_busy: u64,
    pub vacuum_attempts: u64,
    pub vacuum_busy: u64,
    pub rebuild_attempts: u64,
    pub rebuild_busy: u64,
    pub reconnects: u64,
    pub active_clients_high_watermark: u64,
    pub throughput_operations_per_second: f64,
    pub transaction_latency: LatencyHistogram,
    pub telemetry: LayerTelemetryReport,
    pub journal: JournalSummary,
    pub details: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LatencyHistogram {
    pub count: u64,
    pub total_nanos: u128,
    pub max_nanos: u64,
    pub le_100_micros: u64,
    pub le_1_millis: u64,
    pub le_10_millis: u64,
    pub le_100_millis: u64,
    pub le_1_second: u64,
    pub gt_1_second: u64,
}

impl LatencyHistogram {
    pub fn record(&mut self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.count += 1;
        self.total_nanos += nanos as u128;
        self.max_nanos = self.max_nanos.max(nanos);
        match nanos {
            0..=100_000 => self.le_100_micros += 1,
            100_001..=1_000_000 => self.le_1_millis += 1,
            1_000_001..=10_000_000 => self.le_10_millis += 1,
            10_000_001..=100_000_000 => self.le_100_millis += 1,
            100_000_001..=1_000_000_000 => self.le_1_second += 1,
            _ => self.gt_1_second += 1,
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.total_nanos += other.total_nanos;
        self.max_nanos = self.max_nanos.max(other.max_nanos);
        self.le_100_micros += other.le_100_micros;
        self.le_1_millis += other.le_1_millis;
        self.le_10_millis += other.le_10_millis;
        self.le_100_millis += other.le_100_millis;
        self.le_1_second += other.le_1_second;
        self.gt_1_second += other.gt_1_second;
    }

    fn validate(&self) -> Result<(), String> {
        let buckets = self
            .le_100_micros
            .saturating_add(self.le_1_millis)
            .saturating_add(self.le_10_millis)
            .saturating_add(self.le_100_millis)
            .saturating_add(self.le_1_second)
            .saturating_add(self.gt_1_second);
        if buckets != self.count {
            return Err(format!(
                "latency histogram bucket count {buckets} differs from samples {}",
                self.count
            ));
        }
        Ok(())
    }
}

impl LayerSummary {
    pub fn validate(&self) -> Result<(), String> {
        self.journal.validate()?;
        self.transaction_latency.validate()?;
        if self.name.is_empty() || self.semantic_operations == 0 || self.transactions == 0 {
            return Err(format!("incomplete layer summary: {self:?}"));
        }
        let terminal_transactions = self
            .committed_transactions
            .saturating_add(self.rolled_back_transactions)
            .saturating_add(self.disconnected_transactions)
            .saturating_add(self.conflicted_transactions);
        if terminal_transactions != self.transactions {
            return Err(format!(
                "layer {} transaction outcomes {} differ from attempts {}",
                self.name, terminal_transactions, self.transactions
            ));
        }
        if self.row_lock_timeout_conflicts > self.conflicted_transactions {
            return Err(format!(
                "layer {} row-lock timeout conflicts {} exceed all conflicts {}",
                self.name, self.row_lock_timeout_conflicts, self.conflicted_transactions
            ));
        }
        if self.compaction_backpressure_conflicts > self.conflicted_transactions {
            return Err(format!(
                "layer {} compaction-backpressure conflicts {} exceed all conflicts {}",
                self.name, self.compaction_backpressure_conflicts, self.conflicted_transactions
            ));
        }
        if self.transaction_latency.count != self.transactions {
            return Err(format!(
                "layer {} latency samples {} differ from transactions {}",
                self.name, self.transaction_latency.count, self.transactions
            ));
        }
        if self.semantic_operations != self.journal.terminal.count {
            return Err(format!(
                "layer {} semantic operations {} differ from journal terminals {}",
                self.name, self.semantic_operations, self.journal.terminal.count
            ));
        }
        if self.wall_millis == 0
            || !self.throughput_operations_per_second.is_finite()
            || self.throughput_operations_per_second <= 0.0
            || self.active_clients_high_watermark == 0
        {
            return Err(format!("incomplete layer telemetry: {self:?}"));
        }
        if self.details.contains_key("runtime_stats_after")
            && (self.telemetry.samples_taken < 2
                || self.telemetry.first.is_none()
                || self.telemetry.last.is_none())
        {
            return Err(format!(
                "layer {} has no bounded resource interval: {:?}",
                self.name, self.telemetry
            ));
        }
        Ok(())
    }

    pub fn finalize_timing(&mut self, elapsed: Duration) {
        self.wall_millis = elapsed.as_millis().max(1) as u64;
        self.throughput_operations_per_second =
            self.semantic_operations as f64 / elapsed.as_secs_f64().max(0.000_001);
    }

    pub fn merge_maintenance(&mut self, maintenance: MaintenanceSummary) {
        self.maintenance_attempts += maintenance.attempts;
        self.maintenance_busy += maintenance.busy;
        self.vacuum_attempts += maintenance.vacuum_attempts;
        self.vacuum_busy += maintenance.vacuum_busy;
        self.rebuild_attempts += maintenance.rebuild_attempts;
        self.rebuild_busy += maintenance.rebuild_busy;
    }
}

pub fn merge_journals(
    summaries: impl IntoIterator<Item = JournalSummary>,
) -> Result<JournalSummary, String> {
    let mut merged = JournalSummary::default();
    for summary in summaries {
        summary.validate()?;
        merged.merge(&summary);
    }
    merged.validate()?;
    Ok(merged)
}

pub fn expected_busy(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("committed hot rows unsealed")
        || normalized.contains("checkpoint timed out acquiring the commit fence")
        || normalized.contains("busy")
        || normalized.contains("conflict")
        || normalized.contains("serialization")
        || is_row_lock_timeout(&normalized)
        || is_compaction_backpressure(&normalized)
}

pub fn is_row_lock_timeout(error: &str) -> bool {
    error
        .to_ascii_lowercase()
        .contains("transaction timed out while waiting to update row")
}

pub fn is_compaction_backpressure(error: &str) -> bool {
    error
        .to_ascii_lowercase()
        .contains("compaction_backpressure:")
}

pub fn operation_id(layer: u8, shard: usize, ordinal: u64) -> u64 {
    ((layer as u64) << 56) | ((shard as u64 & 0xffff) << 40) | (ordinal & 0xffff_ffffff)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MaintenanceSummary {
    pub attempts: u64,
    pub busy: u64,
    pub vacuum_attempts: u64,
    pub vacuum_busy: u64,
    pub rebuild_attempts: u64,
    pub rebuild_busy: u64,
}

pub struct MaintenanceActor {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<MaintenanceSummary, String>>>,
}

impl MaintenanceActor {
    pub fn start(context: &LayerContext, progress_interval: u64) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let progress = Arc::clone(&context.progress);
        let address = context.address;
        let timeout = context.profile.stage_timeout;
        let initial_progress = progress.load(Ordering::Acquire);
        let rebuild_index = format!("epoch_chaos_rebuild_{initial_progress}");
        let worker = thread::Builder::new()
            .name("epoch-chaos-maintenance".to_string())
            .spawn(move || -> Result<MaintenanceSummary, String> {
                let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
                let mut summary = MaintenanceSummary::default();
                let mut rebuild_present = false;
                let mut next = progress
                    .load(Ordering::Acquire)
                    .saturating_add(progress_interval.max(1));
                while !worker_stop.load(Ordering::Acquire) {
                    let current = progress.load(Ordering::Acquire);
                    if current < next {
                        thread::sleep(Duration::from_millis(25));
                        continue;
                    }
                    summary.attempts += 1;
                    if let Err(error) = tcp_command(&mut connection, "PRAGMA CHECKPOINT") {
                        if expected_busy(&error) {
                            summary.busy += 1;
                        } else {
                            return Err(format!("maintenance checkpoint: {error}"));
                        }
                    }
                    if summary.attempts.is_multiple_of(4) {
                        summary.vacuum_attempts += 1;
                        if let Err(error) = tcp_command(&mut connection, "PRAGMA VACUUM") {
                            if expected_busy(&error) {
                                summary.vacuum_busy += 1;
                            } else {
                                return Err(format!("maintenance vacuum: {error}"));
                            }
                        }
                    }
                    if summary.attempts.is_multiple_of(8) {
                        summary.rebuild_attempts += 1;
                        let sql = if rebuild_present {
                            format!("DROP INDEX {rebuild_index} ON chaos_cells")
                        } else {
                            format!("CREATE INDEX {rebuild_index} ON chaos_cells (live, id)")
                        };
                        match tcp_command(&mut connection, sql) {
                            Ok(()) => rebuild_present = !rebuild_present,
                            Err(error) if expected_busy(&error) => summary.rebuild_busy += 1,
                            Err(error) => {
                                return Err(format!("maintenance index rebuild: {error}"))
                            }
                        }
                    }
                    next = current.saturating_add(progress_interval.max(1));
                }
                if rebuild_present {
                    summary.rebuild_attempts += 1;
                    match tcp_command(
                        &mut connection,
                        format!("DROP INDEX {rebuild_index} ON chaos_cells"),
                    ) {
                        Ok(()) => {}
                        Err(error) if expected_busy(&error) => summary.rebuild_busy += 1,
                        Err(error) => return Err(format!("maintenance index cleanup: {error}")),
                    }
                }
                Ok(summary)
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }

    pub fn stop(mut self) -> Result<MaintenanceSummary, String> {
        self.stop.store(true, Ordering::Release);
        self.worker
            .take()
            .ok_or_else(|| "maintenance actor was already joined".to_string())?
            .join()
            .map_err(|_| "maintenance actor panicked".to_string())?
    }
}

impl Drop for MaintenanceActor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use radixdb_core::Error;

    use super::{expected_busy, is_compaction_backpressure, is_row_lock_timeout};

    #[test]
    fn row_claim_timeout_is_a_visible_retryable_conflict() {
        let error =
            "server SqlError: transaction timed out while waiting to update row 42 after 10000 ms";
        assert!(is_row_lock_timeout(error));
        assert!(expected_busy(error));
        assert!(!is_row_lock_timeout("query timed out after 10000 ms"));
        assert!(!expected_busy("query timed out after 10000 ms"));
    }

    #[test]
    fn every_public_retryable_transaction_error_is_classified() {
        let errors = [
            Error::TransactionSerializationConflict { row_id: 7 },
            Error::RowLockTimeout {
                row_id: 7,
                timeout_ms: 10_000,
            },
            Error::CompactionBackpressure {
                table: "items".to_string(),
                segments: 32,
                physical_bytes: 4096,
                hard_segments: 32,
                hard_bytes: 8192,
            },
        ];
        for error in &errors {
            assert!(error.is_retryable());
            assert!(expected_busy(&error.to_string()), "{error}");
        }
        assert!(is_compaction_backpressure(&errors[2].to_string()));
        assert!(!is_compaction_backpressure("checkpoint backpressure"));
    }
}
