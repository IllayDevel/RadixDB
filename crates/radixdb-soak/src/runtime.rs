use std::sync::atomic::{AtomicU64, Ordering};

use crate::status::{Counters, LatencySnapshot};

#[derive(Default)]
pub struct RuntimeMetrics {
    transactions_planned: AtomicU64,
    transactions_committed: AtomicU64,
    transactions_rolled_back: AtomicU64,
    conflicts: AtomicU64,
    disconnects: AtomicU64,
    ambiguous_resolved: AtomicU64,
    operations: AtomicU64,
    invariant_passes: AtomicU64,
    invariant_failures: AtomicU64,
    checkpoints: AtomicU64,
    checkpoint_deferred: AtomicU64,
    backups: AtomicU64,
    restores: AtomicU64,
    reopens: AtomicU64,
    latency: LatencyHistogram,
}

macro_rules! counter_method {
    ($name:ident, $field:ident) => {
        pub fn $name(&self) {
            self.$field.fetch_add(1, Ordering::Relaxed);
        }
    };
}

impl RuntimeMetrics {
    counter_method!(planned, transactions_planned);
    counter_method!(committed, transactions_committed);
    counter_method!(rolled_back, transactions_rolled_back);
    counter_method!(conflict, conflicts);
    counter_method!(disconnected, disconnects);
    counter_method!(ambiguous_resolved, ambiguous_resolved);
    counter_method!(operation, operations);
    counter_method!(invariant_passed, invariant_passes);
    counter_method!(invariant_failed, invariant_failures);
    counter_method!(checkpoint, checkpoints);
    counter_method!(checkpoint_deferred, checkpoint_deferred);
    counter_method!(backup, backups);
    counter_method!(restore, restores);
    counter_method!(reopen, reopens);

    pub fn add_operations(&self, count: u64) {
        self.operations.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_latency_micros(&self, micros: u64) {
        self.latency.record(micros);
    }

    pub fn counters(&self, auth_failures: u64) -> Counters {
        Counters {
            transactions_planned: self.transactions_planned.load(Ordering::Relaxed),
            transactions_committed: self.transactions_committed.load(Ordering::Relaxed),
            transactions_rolled_back: self.transactions_rolled_back.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            ambiguous_resolved: self.ambiguous_resolved.load(Ordering::Relaxed),
            operations: self.operations.load(Ordering::Relaxed),
            invariant_passes: self.invariant_passes.load(Ordering::Relaxed),
            invariant_failures: self.invariant_failures.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            checkpoint_deferred: self.checkpoint_deferred.load(Ordering::Relaxed),
            backups: self.backups.load(Ordering::Relaxed),
            restores: self.restores.load(Ordering::Relaxed),
            reopens: self.reopens.load(Ordering::Relaxed),
            auth_failures,
        }
    }

    pub fn latency(&self) -> LatencySnapshot {
        self.latency.snapshot()
    }
}

pub struct LatencyHistogram {
    buckets: [AtomicU64; 64],
    samples: AtomicU64,
    minimum: AtomicU64,
    maximum: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            samples: AtomicU64::new(0),
            minimum: AtomicU64::new(u64::MAX),
            maximum: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub fn record(&self, micros: u64) {
        let bucket = if micros == 0 {
            0
        } else {
            (u64::BITS - micros.leading_zeros()) as usize
        }
        .min(self.buckets.len() - 1);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.minimum.fetch_min(micros, Ordering::Relaxed);
        self.maximum.fetch_max(micros, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let samples = self.samples.load(Ordering::Relaxed);
        if samples == 0 {
            return LatencySnapshot::default();
        }
        LatencySnapshot {
            samples,
            min_micros: self.minimum.load(Ordering::Relaxed),
            p50_micros: self.percentile(samples, 50),
            p95_micros: self.percentile(samples, 95),
            p99_micros: self.percentile(samples, 99),
            max_micros: self.maximum.load(Ordering::Relaxed),
        }
    }

    fn percentile(&self, samples: u64, percentile: u64) -> u64 {
        let target = samples.saturating_mul(percentile).saturating_add(99) / 100;
        let mut cumulative = 0u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
            if cumulative >= target {
                return if index == 0 { 0 } else { 1u64 << (index - 1) };
            }
        }
        self.maximum.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_histogram_is_bounded_and_monotonic() {
        let histogram = LatencyHistogram::default();
        for value in [1, 2, 3, 4, 100, 1_000, 10_000] {
            histogram.record(value);
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.samples, 7);
        assert_eq!(snapshot.min_micros, 1);
        assert_eq!(snapshot.max_micros, 10_000);
        assert!(snapshot.p50_micros <= snapshot.p95_micros);
        assert!(snapshot.p95_micros <= snapshot.p99_micros);
    }
}
