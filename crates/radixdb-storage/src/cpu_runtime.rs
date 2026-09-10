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

//! Per-engine CPU admission for seal and compaction work.
//!
//! Every physical writer receives a lease from one database-local owner before
//! it builds metadata, postings or encoded blocks. Concurrent compactions can
//! therefore share a configured CPU envelope without creating an independent
//! thread pool for every output segment.

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

/// `0` is the stable public spelling for automatic host/cgroup parallelism.
pub const AUTO_STORAGE_CPU_WORKERS: usize = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageCpuSnapshot {
    pub configured_workers: usize,
    pub effective_workers: usize,
    pub workers_in_use: usize,
    pub peak_workers_in_use: usize,
    pub workers_reserved: usize,
    pub peak_workers_reserved: usize,
    pub leases: u64,
    pub parallel_leases: u64,
}

#[derive(Debug)]
pub struct StorageCpuRuntime {
    configured_workers: usize,
    effective_workers: usize,
    token_tx: crossbeam::channel::Sender<()>,
    token_rx: crossbeam::channel::Receiver<()>,
    workers_reserved: AtomicUsize,
    peak_workers_reserved: AtomicUsize,
    workers_active: AtomicUsize,
    peak_workers_active: AtomicUsize,
    leases: AtomicU64,
    parallel_leases: AtomicU64,
}

impl StorageCpuRuntime {
    pub fn new(configured_workers: usize) -> Arc<Self> {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .max(1);
        let effective_workers = if configured_workers == AUTO_STORAGE_CPU_WORKERS {
            available
        } else {
            configured_workers.min(available).max(1)
        };
        let (token_tx, token_rx) = crossbeam::channel::bounded(effective_workers);
        for _ in 0..effective_workers {
            token_tx
                .send(())
                .expect("fresh storage CPU budget must accept every token");
        }
        Arc::new(Self {
            configured_workers,
            effective_workers,
            token_tx,
            token_rx,
            workers_reserved: AtomicUsize::new(0),
            peak_workers_reserved: AtomicUsize::new(0),
            workers_active: AtomicUsize::new(0),
            peak_workers_active: AtomicUsize::new(0),
            leases: AtomicU64::new(0),
            parallel_leases: AtomicU64::new(0),
        })
    }

    /// Acquire at least one CPU participant and opportunistically reserve the
    /// remaining shared capacity. Waiting is intentional: with `workers=1`,
    /// concurrent seal/compaction owners must not silently exceed one CPU-heavy
    /// participant.
    pub fn acquire(self: &Arc<Self>, requested_workers: usize) -> StorageCpuLease {
        self.token_rx
            .recv()
            .expect("storage CPU runtime owns a live token sender");
        let requested = requested_workers.max(1).min(self.effective_workers);
        let mut workers = 1;
        while workers < requested && self.token_rx.try_recv().is_ok() {
            workers += 1;
        }

        let reserved = self.workers_reserved.fetch_add(workers, Ordering::AcqRel) + workers;
        self.peak_workers_reserved
            .fetch_max(reserved, Ordering::AcqRel);
        self.leases.fetch_add(1, Ordering::Relaxed);
        if workers > 1 {
            self.parallel_leases.fetch_add(1, Ordering::Relaxed);
        }
        StorageCpuLease {
            runtime: Arc::clone(self),
            workers,
        }
    }

    pub fn snapshot(&self) -> StorageCpuSnapshot {
        StorageCpuSnapshot {
            configured_workers: self.configured_workers,
            effective_workers: self.effective_workers,
            workers_in_use: self.workers_active.load(Ordering::Acquire),
            peak_workers_in_use: self.peak_workers_active.load(Ordering::Acquire),
            workers_reserved: self.workers_reserved.load(Ordering::Acquire),
            peak_workers_reserved: self.peak_workers_reserved.load(Ordering::Acquire),
            leases: self.leases.load(Ordering::Relaxed),
            parallel_leases: self.parallel_leases.load(Ordering::Relaxed),
        }
    }
}

pub struct StorageCpuLease {
    runtime: Arc<StorageCpuRuntime>,
    workers: usize,
}

impl StorageCpuLease {
    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn activate(&self) -> StorageCpuActivity {
        let active = self.runtime.workers_active.fetch_add(1, Ordering::AcqRel) + 1;
        self.runtime
            .peak_workers_active
            .fetch_max(active, Ordering::AcqRel);
        StorageCpuActivity {
            runtime: Arc::clone(&self.runtime),
        }
    }
}

pub struct StorageCpuActivity {
    runtime: Arc<StorageCpuRuntime>,
}

impl Drop for StorageCpuActivity {
    fn drop(&mut self) {
        self.runtime.workers_active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for StorageCpuLease {
    fn drop(&mut self) {
        self.runtime
            .workers_reserved
            .fetch_sub(self.workers, Ordering::AcqRel);
        for _ in 0..self.workers {
            self.runtime
                .token_tx
                .send(())
                .expect("storage CPU runtime outlives every lease");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_single_worker_is_strict_and_released() {
        let runtime = StorageCpuRuntime::new(1);
        let first = runtime.acquire(usize::MAX);
        assert_eq!(first.workers(), 1);
        assert_eq!(runtime.snapshot().workers_reserved, 1);
        assert_eq!(runtime.snapshot().workers_in_use, 0);
        let active = first.activate();
        assert_eq!(runtime.snapshot().workers_in_use, 1);
        drop(active);
        drop(first);
        assert_eq!(runtime.snapshot().workers_in_use, 0);
        assert_eq!(runtime.snapshot().peak_workers_in_use, 1);
        assert_eq!(runtime.snapshot().workers_reserved, 0);
        assert_eq!(runtime.snapshot().peak_workers_reserved, 1);
    }

    #[test]
    fn auto_and_explicit_limits_never_exceed_available_parallelism() {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .max(1);
        let automatic = StorageCpuRuntime::new(0);
        assert_eq!(automatic.snapshot().effective_workers, available);
        let explicit = StorageCpuRuntime::new(available.saturating_add(17));
        assert_eq!(explicit.snapshot().effective_workers, available);
    }
}
