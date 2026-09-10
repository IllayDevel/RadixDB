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

//! Storage engine configuration.
//!

/// WAL sync mode for controlling durability vs performance tradeoff
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Fastest but least durable - doesn't force syncs
    None = 0,
    /// Fsync at most once per sync_interval_ms (default 1s) and on DDL operations
    #[default]
    Normal = 1,
    /// Forces syncs on every WAL write - slowest but most durable
    Full = 2,
}

impl From<i32> for SyncMode {
    fn from(value: i32) -> Self {
        match value {
            0 => SyncMode::None,
            2 => SyncMode::Full,
            _ => SyncMode::Normal,
        }
    }
}

impl From<SyncMode> for i32 {
    fn from(mode: SyncMode) -> Self {
        mode as i32
    }
}

/// Conservative memory envelope for one atomic COPY transaction. The COPY
/// executor accounts row payload plus MVCC/WAL amplification against it.
pub const DEFAULT_COPY_MAX_TRANSACTION_BYTES: usize = 512 * 1024 * 1024;
/// Maximum number of immutable cold segments owned by one compaction job.
/// This is a work bound, not a trigger threshold.
pub const DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS: usize = 8;
/// Concurrent table-local compaction jobs. One remains the conservative
/// hardware-neutral default; higher values are an explicit operator choice.
pub const DEFAULT_MAX_COMPACTION_JOBS: usize = 1;
/// Defensive process-wide cap for explicitly configured compaction workers.
/// Every worker owns an independent output-memory and disk reservation, so the
/// public bound stays deliberately small even on large hosts.
pub const MAX_COMPACTION_JOBS: usize = 8;
/// Maximum physical bytes owned by one compaction job across its DATA and
/// INDEX inputs.
pub const DEFAULT_MAX_COMPACTION_INPUT_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum physical output bytes produced by one compaction job.
pub const DEFAULT_MAX_COMPACTION_OUTPUT_BYTES: u64 = 1024 * 1024 * 1024;
/// Wall-clock budget for one compaction job. Zero disables the deadline.
pub const DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS: u64 = 20 * 60 * 1000;
/// Optional aggregate average compaction I/O rate across concurrent jobs. Zero
/// leaves throughput unthrottled.
pub const DEFAULT_COMPACTION_IO_BYTES_PER_SEC: u64 = 0;
/// Free filesystem space which compaction must leave untouched.
pub const DEFAULT_COMPACTION_DISK_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;
/// Cooldown before retrying the same failed compaction input snapshot.
pub const DEFAULT_COMPACTION_RETRY_COOLDOWN_MS: u64 = 30_000;
pub const DEFAULT_L0_SOFT_LIMIT_SEGMENTS: usize = 16;
pub const DEFAULT_L0_HARD_LIMIT_SEGMENTS: usize = 32;
pub const DEFAULT_L0_SOFT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_L0_HARD_LIMIT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_L0_SOFT_BACKPRESSURE_WAIT_MS: u64 = 100;
/// Automatic storage CPU parallelism follows the host/cgroup-visible CPU set.
pub const DEFAULT_STORAGE_CPU_WORKERS: usize = 0;
/// Proactive operating-system page-cache warmup is disabled by default. The
/// policy is deliberately opt-in so opening a database keeps its historical
/// I/O and memory behaviour.
pub const DEFAULT_PAGE_CACHE_LEVEL: u8 = 0;
pub const DEFAULT_PAGE_CACHE_MAX_BYTES: u64 = 0;
pub const DEFAULT_PAGE_CACHE_MEMORY_RESERVE: u64 = 0;
pub const MAX_PAGE_CACHE_LEVEL: u8 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceConfig {
    /// Whether persistence is enabled
    /// Default: true if Path is not empty
    pub enabled: bool,

    /// WAL sync strategy
    /// Default: Normal
    pub sync_mode: SyncMode,

    /// Time between checkpoint cycles in seconds
    /// Default: 60 (1 minute)
    pub checkpoint_interval: u32,

    /// Number of sub-target volumes per table before compaction merges them
    /// Default: 4
    pub compact_threshold: u32,

    /// Maximum concurrent compaction jobs. Jobs in one scheduler wave always
    /// own distinct tables. Default: 1.
    pub max_compaction_jobs: usize,

    /// Shared CPU-heavy worker budget for seal metadata, posting construction,
    /// block encoding and compaction output. `0` selects all host/cgroup-visible
    /// CPUs; a positive value is a hard upper bound. Default: auto.
    pub storage_cpu_workers: usize,

    /// Proactive page-cache warmup policy in `0..=10`. Zero disables warmup;
    /// ten requests the complete current database generation, subject to the
    /// safe memory budget.
    pub page_cache_level: u8,

    /// Optional hard cap for proactive warmup bytes. Zero selects the safe
    /// automatic budget derived from memory availability and reserve.
    pub page_cache_max_bytes: u64,

    /// Memory kept available for the engine, clients and operating system.
    /// Zero selects a conservative automatic reserve.
    pub page_cache_memory_reserve: u64,

    /// Maximum number of contiguous manifest inputs selected for one
    /// compaction job. Default: 8.
    pub max_compaction_input_segments: usize,

    /// Maximum physical `.data` + `.idx` bytes selected for one compaction job.
    /// Default: 512 MiB.
    pub max_compaction_input_bytes: u64,

    /// Maximum physical `.data` + `.idx` bytes produced by one compaction job.
    /// Default: 1 GiB.
    pub max_compaction_output_bytes: u64,

    /// Maximum wall time for one compaction job. Zero disables the deadline.
    /// Default: 20 minutes.
    pub compaction_job_time_budget_ms: u64,

    /// Optional average read + write rate for compaction. Zero disables
    /// throttling. Default: unthrottled so hardware-specific tuning is explicit.
    pub compaction_io_bytes_per_sec: u64,

    /// Free filesystem space reserved from compaction outputs. Zero disables
    /// this additional reserve. Default: 1 GiB.
    pub compaction_disk_reserve_bytes: u64,

    /// Delay before retrying the exact same failed compaction job. Zero disables
    /// suppression. Default: 30 seconds.
    pub compaction_retry_cooldown_ms: u64,

    /// L0/legacy debt at which committing writers briefly cooperate with
    /// background compaction. Default: 16 segments.
    pub l0_soft_limit_segments: usize,

    /// L0/legacy debt at which a write commit fails with an explicit retryable
    /// error before publication. Default: 32 segments.
    pub l0_hard_limit_segments: usize,

    /// Physical L0/legacy bytes that activate soft backpressure. Default: 1 GiB.
    pub l0_soft_limit_bytes: u64,

    /// Physical L0/legacy bytes that reject a write commit. Default: 2 GiB.
    pub l0_hard_limit_bytes: u64,

    /// Maximum cooperative wait performed by one commit above the soft limit.
    /// Default: 100 ms.
    pub l0_soft_backpressure_wait_ms: u64,

    /// Size in bytes that triggers a WAL flush
    /// Default: 32768 (32KB)
    pub wal_flush_trigger: usize,

    /// Initial WAL buffer size in bytes
    /// Default: 65536 (64KB)
    pub wal_buffer_size: usize,

    /// Maximum size of a WAL file before rotation in bytes
    /// Default: 67108864 (64MB)
    pub wal_max_size: usize,

    /// Minimum time between syncs in milliseconds in SyncNormal mode
    /// Default: 1000
    pub sync_interval_ms: u32,

    /// Enable LZ4 compression for WAL entries
    /// Default: true
    pub wal_compression: bool,

    /// Enable LZ4 compression for cold volume files
    /// Default: true
    pub volume_compression: bool,

    /// Number of backup snapshots to keep per table
    /// Default: 3
    pub keep_snapshots: u32,

    /// Whether to run a final checkpoint (seal all hot rows to volumes) on close.
    /// Default: true. Set to false when simulating crashes in tests.
    pub checkpoint_on_close: bool,

    /// Maximum estimated resident memory owned by one atomic COPY statement.
    /// Exceeding the budget aborts and rolls back the complete statement.
    /// Default: 512 MiB.
    pub copy_max_transaction_bytes: usize,

    /// Target number of rows per cold volume. Seal and compaction split their
    /// output into volumes of approximately this size. Smaller values reduce
    /// compaction write amplification; larger values improve compression and
    /// reduce per-volume metadata overhead.
    /// Default: 1,048,576 (1M rows = ~16 row groups of 64K each)
    pub target_volume_rows: usize,

    /// Approximate hot MVCC byte budget that triggers the first seal for a table.
    /// The engine still keeps the row threshold; seal runs when either threshold
    /// is reached.
    /// Default: 64 MiB
    pub seal_hot_bytes_threshold: usize,

    /// Approximate hot MVCC byte budget that triggers subsequent seals for a
    /// table that already has cold segments.
    /// Default: 16 MiB
    pub seal_incremental_hot_bytes_threshold: usize,

    /// Global resident cold-volume payload cache budget in bytes.
    ///
    /// This budget covers evictable column payloads kept in segment memory:
    /// materialized eager columns. Immutable segment
    /// metadata (row ids, zone maps, descriptors) is not counted because cold
    /// query routing depends on it staying resident.
    /// Default: 1 GiB
    pub volume_cache_bytes: usize,

    /// Maximum number of artifact-backed cold-volume physical read groups allowed in flight.
    ///
    /// `1` preserves the historical sequential path. Larger values are the
    /// stage-4 NVMe-first knob used by the bounded parallel `pread` executor.
    /// Default: 1
    pub read_queue_depth: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sync_mode: SyncMode::Normal,
            checkpoint_interval: 60, // 1 minute
            compact_threshold: 4,    // Compact after 4 segments
            max_compaction_jobs: DEFAULT_MAX_COMPACTION_JOBS,
            storage_cpu_workers: DEFAULT_STORAGE_CPU_WORKERS,
            page_cache_level: DEFAULT_PAGE_CACHE_LEVEL,
            page_cache_max_bytes: DEFAULT_PAGE_CACHE_MAX_BYTES,
            page_cache_memory_reserve: DEFAULT_PAGE_CACHE_MEMORY_RESERVE,
            max_compaction_input_segments: DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS,
            max_compaction_input_bytes: DEFAULT_MAX_COMPACTION_INPUT_BYTES,
            max_compaction_output_bytes: DEFAULT_MAX_COMPACTION_OUTPUT_BYTES,
            compaction_job_time_budget_ms: DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS,
            compaction_io_bytes_per_sec: DEFAULT_COMPACTION_IO_BYTES_PER_SEC,
            compaction_disk_reserve_bytes: DEFAULT_COMPACTION_DISK_RESERVE_BYTES,
            compaction_retry_cooldown_ms: DEFAULT_COMPACTION_RETRY_COOLDOWN_MS,
            l0_soft_limit_segments: DEFAULT_L0_SOFT_LIMIT_SEGMENTS,
            l0_hard_limit_segments: DEFAULT_L0_HARD_LIMIT_SEGMENTS,
            l0_soft_limit_bytes: DEFAULT_L0_SOFT_LIMIT_BYTES,
            l0_hard_limit_bytes: DEFAULT_L0_HARD_LIMIT_BYTES,
            l0_soft_backpressure_wait_ms: DEFAULT_L0_SOFT_BACKPRESSURE_WAIT_MS,
            wal_flush_trigger: 32 * 1024,   // 32KB
            wal_buffer_size: 64 * 1024,     // 64KB
            wal_max_size: 64 * 1024 * 1024, // 64MB
            sync_interval_ms: 1000,         // 1 second between syncs
            wal_compression: true,          // Enable WAL compression
            volume_compression: true,       // Enable volume LZ4 compression
            keep_snapshots: 3,              // Keep 3 backup snapshots per table
            checkpoint_on_close: true,      // Seal all data on clean shutdown
            copy_max_transaction_bytes: DEFAULT_COPY_MAX_TRANSACTION_BYTES,
            target_volume_rows: 1_048_576, // 1M rows per volume (~16 row groups)
            seal_hot_bytes_threshold: 64 * 1024 * 1024,
            seal_incremental_hot_bytes_threshold: 16 * 1024 * 1024,
            volume_cache_bytes: 1024 * 1024 * 1024,
            read_queue_depth: 1,
        }
    }
}

impl PersistenceConfig {
    /// Creates a new PersistenceConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a PersistenceConfig optimized for maximum durability
    pub fn durable() -> Self {
        Self {
            enabled: true,
            sync_mode: SyncMode::Full,
            checkpoint_interval: 30, // 30 seconds
            compact_threshold: 4,
            max_compaction_jobs: DEFAULT_MAX_COMPACTION_JOBS,
            storage_cpu_workers: DEFAULT_STORAGE_CPU_WORKERS,
            page_cache_level: DEFAULT_PAGE_CACHE_LEVEL,
            page_cache_max_bytes: DEFAULT_PAGE_CACHE_MAX_BYTES,
            page_cache_memory_reserve: DEFAULT_PAGE_CACHE_MEMORY_RESERVE,
            max_compaction_input_segments: DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS,
            max_compaction_input_bytes: DEFAULT_MAX_COMPACTION_INPUT_BYTES,
            max_compaction_output_bytes: DEFAULT_MAX_COMPACTION_OUTPUT_BYTES,
            compaction_job_time_budget_ms: DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS,
            compaction_io_bytes_per_sec: DEFAULT_COMPACTION_IO_BYTES_PER_SEC,
            compaction_disk_reserve_bytes: DEFAULT_COMPACTION_DISK_RESERVE_BYTES,
            compaction_retry_cooldown_ms: DEFAULT_COMPACTION_RETRY_COOLDOWN_MS,
            l0_soft_limit_segments: DEFAULT_L0_SOFT_LIMIT_SEGMENTS,
            l0_hard_limit_segments: DEFAULT_L0_HARD_LIMIT_SEGMENTS,
            l0_soft_limit_bytes: DEFAULT_L0_SOFT_LIMIT_BYTES,
            l0_hard_limit_bytes: DEFAULT_L0_HARD_LIMIT_BYTES,
            l0_soft_backpressure_wait_ms: DEFAULT_L0_SOFT_BACKPRESSURE_WAIT_MS,
            wal_flush_trigger: 8 * 1024,    // 8KB - flush more often
            wal_buffer_size: 32 * 1024,     // 32KB
            wal_max_size: 32 * 1024 * 1024, // 32MB - smaller files
            sync_interval_ms: 0,            // Immediate sync
            wal_compression: true,
            volume_compression: true,
            keep_snapshots: 3,
            checkpoint_on_close: true,
            copy_max_transaction_bytes: DEFAULT_COPY_MAX_TRANSACTION_BYTES,
            target_volume_rows: 1_048_576,
            seal_hot_bytes_threshold: 64 * 1024 * 1024,
            seal_incremental_hot_bytes_threshold: 16 * 1024 * 1024,
            volume_cache_bytes: 512 * 1024 * 1024,
            read_queue_depth: 1,
        }
    }

    /// Creates a PersistenceConfig optimized for maximum performance
    pub fn fast() -> Self {
        Self {
            enabled: true,
            sync_mode: SyncMode::None,
            checkpoint_interval: 120, // 2 minutes
            compact_threshold: 8,
            max_compaction_jobs: DEFAULT_MAX_COMPACTION_JOBS,
            storage_cpu_workers: DEFAULT_STORAGE_CPU_WORKERS,
            page_cache_level: DEFAULT_PAGE_CACHE_LEVEL,
            page_cache_max_bytes: DEFAULT_PAGE_CACHE_MAX_BYTES,
            page_cache_memory_reserve: DEFAULT_PAGE_CACHE_MEMORY_RESERVE,
            max_compaction_input_segments: DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS,
            max_compaction_input_bytes: DEFAULT_MAX_COMPACTION_INPUT_BYTES,
            max_compaction_output_bytes: DEFAULT_MAX_COMPACTION_OUTPUT_BYTES,
            compaction_job_time_budget_ms: DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS,
            compaction_io_bytes_per_sec: DEFAULT_COMPACTION_IO_BYTES_PER_SEC,
            compaction_disk_reserve_bytes: DEFAULT_COMPACTION_DISK_RESERVE_BYTES,
            compaction_retry_cooldown_ms: DEFAULT_COMPACTION_RETRY_COOLDOWN_MS,
            l0_soft_limit_segments: DEFAULT_L0_SOFT_LIMIT_SEGMENTS,
            l0_hard_limit_segments: DEFAULT_L0_HARD_LIMIT_SEGMENTS,
            l0_soft_limit_bytes: DEFAULT_L0_SOFT_LIMIT_BYTES,
            l0_hard_limit_bytes: DEFAULT_L0_HARD_LIMIT_BYTES,
            l0_soft_backpressure_wait_ms: DEFAULT_L0_SOFT_BACKPRESSURE_WAIT_MS,
            wal_flush_trigger: 64 * 1024,    // 64KB
            wal_buffer_size: 128 * 1024,     // 128KB
            wal_max_size: 128 * 1024 * 1024, // 128MB
            sync_interval_ms: 100,           // Less frequent sync
            wal_compression: true,
            volume_compression: true,
            keep_snapshots: 3,
            checkpoint_on_close: true,
            copy_max_transaction_bytes: DEFAULT_COPY_MAX_TRANSACTION_BYTES,
            target_volume_rows: 2_097_152, // 2M rows for fast mode
            seal_hot_bytes_threshold: 128 * 1024 * 1024,
            seal_incremental_hot_bytes_threshold: 32 * 1024 * 1024,
            volume_cache_bytes: 2 * 1024 * 1024 * 1024,
            read_queue_depth: 1,
        }
    }

    /// Builder method to set sync mode
    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Builder method to set checkpoint interval.
    /// A value of 0 disables periodic checkpoints (data stays in hot buffer).
    /// Non-zero values are clamped to a minimum of 5 seconds.
    pub fn with_checkpoint_interval(mut self, seconds: u32) -> Self {
        self.checkpoint_interval = if seconds == 0 { 0 } else { seconds.max(5) };
        self
    }

    /// Set the fail-closed resident-memory envelope for one atomic COPY.
    pub fn with_copy_max_transaction_bytes(mut self, bytes: usize) -> Self {
        self.copy_max_transaction_bytes = bytes.max(1);
        self
    }

    /// Builder method to set compaction threshold (number of segments)
    pub fn with_compact_threshold(mut self, count: u32) -> Self {
        self.compact_threshold = count;
        self
    }

    /// Configure bounded parallelism across distinct tables.
    pub fn with_max_compaction_jobs(mut self, count: usize) -> Self {
        self.max_compaction_jobs = count.clamp(1, MAX_COMPACTION_JOBS);
        self
    }

    /// Configure the shared storage CPU budget. Zero keeps automatic
    /// host/cgroup-visible parallelism; positive values are strict caps.
    pub fn with_storage_cpu_workers(mut self, workers: usize) -> Self {
        self.storage_cpu_workers = workers;
        self
    }

    /// Configure proactive operating-system page-cache warmup. Values above
    /// the public contract are clamped for builder callers; textual configs
    /// reject them explicitly.
    pub fn with_page_cache_level(mut self, level: u8) -> Self {
        self.page_cache_level = level.min(MAX_PAGE_CACHE_LEVEL);
        self
    }

    /// Apply an optional hard cap to proactive warmup bytes. Zero keeps the
    /// automatic safe budget.
    pub fn with_page_cache_max_bytes(mut self, bytes: u64) -> Self {
        self.page_cache_max_bytes = bytes;
        self
    }

    /// Reserve memory from proactive warmup. Zero selects the automatic safe
    /// reserve.
    pub fn with_page_cache_memory_reserve(mut self, bytes: u64) -> Self {
        self.page_cache_memory_reserve = bytes;
        self
    }

    /// Bound one compaction job by immutable input count.
    pub fn with_max_compaction_input_segments(mut self, count: usize) -> Self {
        self.max_compaction_input_segments = count.max(1);
        self
    }

    /// Bound one compaction job by physical payload and posting bytes.
    pub fn with_max_compaction_input_bytes(mut self, bytes: u64) -> Self {
        self.max_compaction_input_bytes = bytes.max(1);
        self
    }

    /// Bound one compaction job by physical payload and posting output bytes.
    pub fn with_max_compaction_output_bytes(mut self, bytes: u64) -> Self {
        self.max_compaction_output_bytes = bytes.max(1);
        self
    }

    /// Bound one compaction job by wall time. Zero disables the deadline.
    pub fn with_compaction_job_time_budget_ms(mut self, millis: u64) -> Self {
        self.compaction_job_time_budget_ms = millis;
        self
    }

    /// Set the optional average compaction read + write rate.
    pub fn with_compaction_io_bytes_per_sec(mut self, bytes: u64) -> Self {
        self.compaction_io_bytes_per_sec = bytes;
        self
    }

    /// Reserve filesystem space from compaction output publication.
    pub fn with_compaction_disk_reserve_bytes(mut self, bytes: u64) -> Self {
        self.compaction_disk_reserve_bytes = bytes;
        self
    }

    /// Delay retrying one identical failed compaction input snapshot.
    pub fn with_compaction_retry_cooldown_ms(mut self, millis: u64) -> Self {
        self.compaction_retry_cooldown_ms = millis;
        self
    }

    /// Configure ordered L0 segment debt thresholds.
    pub fn with_l0_segment_limits(mut self, soft: usize, hard: usize) -> Self {
        self.l0_soft_limit_segments = soft.max(1);
        self.l0_hard_limit_segments = hard.max(self.l0_soft_limit_segments.saturating_add(1));
        self
    }

    /// Configure ordered L0 physical byte debt thresholds.
    pub fn with_l0_byte_limits(mut self, soft: u64, hard: u64) -> Self {
        self.l0_soft_limit_bytes = soft.max(1);
        self.l0_hard_limit_bytes = hard.max(self.l0_soft_limit_bytes.saturating_add(1));
        self
    }

    /// Configure the bounded cooperative commit wait above the soft limit.
    pub fn with_l0_soft_backpressure_wait_ms(mut self, millis: u64) -> Self {
        self.l0_soft_backpressure_wait_ms = millis;
        self
    }

    /// Builder method to enable/disable WAL compression
    pub fn with_wal_compression(mut self, enabled: bool) -> Self {
        self.wal_compression = enabled;
        self
    }

    /// Builder method to enable/disable volume LZ4 compression
    pub fn with_volume_compression(mut self, enabled: bool) -> Self {
        self.volume_compression = enabled;
        self
    }

    /// Builder method to enable/disable all compression (WAL + volume)
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.wal_compression = enabled;
        self.volume_compression = enabled;
        self
    }

    /// Builder method to set keep count for backup snapshots
    pub fn with_keep_snapshots(mut self, count: u32) -> Self {
        self.keep_snapshots = count;
        self
    }

    /// Builder method to set target rows per volume (minimum: 65,536 = one row group)
    pub fn with_target_volume_rows(mut self, rows: usize) -> Self {
        self.target_volume_rows = rows.max(65_536);
        self
    }

    /// Builder method to set the first-seal hot byte threshold.
    pub fn with_seal_hot_bytes_threshold(mut self, bytes: usize) -> Self {
        self.seal_hot_bytes_threshold = bytes.max(1);
        self
    }

    /// Builder method to set the incremental hot byte threshold.
    pub fn with_seal_incremental_hot_bytes_threshold(mut self, bytes: usize) -> Self {
        self.seal_incremental_hot_bytes_threshold = bytes.max(1);
        self
    }

    /// Builder method to set the global resident cold-volume payload cache budget.
    pub fn with_volume_cache_bytes(mut self, bytes: usize) -> Self {
        self.volume_cache_bytes = bytes;
        self
    }

    /// Builder method to set the artifact-backed cold-volume read queue depth.
    pub fn with_read_queue_depth(mut self, depth: usize) -> Self {
        self.read_queue_depth = depth.max(1);
        self
    }
}

/// Configuration for background cleanup operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupConfig {
    /// Whether automatic cleanup is enabled
    /// Default: true
    pub enabled: bool,

    /// Interval between cleanup runs in seconds
    /// Default: 60 (1 minute)
    pub interval_secs: u64,

    /// Retention period for deleted rows in seconds
    /// Rows deleted longer than this will be permanently removed
    /// Default: 300 (5 minutes)
    pub deleted_row_retention_secs: u64,

    /// Retention period for old transaction metadata in seconds
    /// Only applies in Snapshot Isolation mode (READ COMMITTED requires keeping all)
    /// Default: 3600 (1 hour)
    pub transaction_retention_secs: u64,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            deleted_row_retention_secs: 300,
            transaction_retention_secs: 3600,
        }
    }
}

impl CleanupConfig {
    /// Creates a cleanup config with cleanup disabled
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Builder method to set cleanup interval
    pub fn with_interval_secs(mut self, secs: u64) -> Self {
        self.interval_secs = secs;
        self
    }

    /// Builder method to set deleted row retention
    pub fn with_deleted_row_retention_secs(mut self, secs: u64) -> Self {
        self.deleted_row_retention_secs = secs;
        self
    }

    /// Builder method to set transaction retention
    pub fn with_transaction_retention_secs(mut self, secs: u64) -> Self {
        self.transaction_retention_secs = secs;
        self
    }
}

/// Configuration for the storage engine
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    /// Path to the database directory
    /// If empty, database operates in memory-only mode
    pub path: Option<String>,

    /// Configuration options for disk persistence
    /// Only used if path is Some
    pub persistence: PersistenceConfig,

    /// Configuration for background cleanup operations
    pub cleanup: CleanupConfig,
}

impl Config {
    /// Creates a new in-memory configuration (no persistence)
    pub fn in_memory() -> Self {
        Self {
            path: None,
            persistence: PersistenceConfig {
                enabled: false,
                ..Default::default()
            },
            cleanup: CleanupConfig::default(),
        }
    }

    /// Creates a new configuration with persistence at the given path
    pub fn with_path<P: Into<String>>(path: P) -> Self {
        Self {
            path: Some(path.into()),
            persistence: PersistenceConfig::default(),
            cleanup: CleanupConfig::default(),
        }
    }

    /// Returns true if persistence is enabled
    pub fn is_persistent(&self) -> bool {
        self.path.is_some() && self.persistence.enabled
    }

    /// Builder method to set persistence config
    pub fn with_persistence(mut self, config: PersistenceConfig) -> Self {
        self.persistence = config;
        self
    }

    /// Builder method to set cleanup config
    pub fn with_cleanup(mut self, config: CleanupConfig) -> Self {
        self.cleanup = config;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_mode_default() {
        assert_eq!(SyncMode::default(), SyncMode::Normal);
    }

    #[test]
    fn test_sync_mode_from_i32() {
        assert_eq!(SyncMode::from(0), SyncMode::None);
        assert_eq!(SyncMode::from(1), SyncMode::Normal);
        assert_eq!(SyncMode::from(2), SyncMode::Full);
        assert_eq!(SyncMode::from(99), SyncMode::Normal); // Invalid defaults to Normal
    }

    #[test]
    fn test_persistence_config_default() {
        let config = PersistenceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.sync_mode, SyncMode::Normal);
        assert_eq!(config.checkpoint_interval, 60);
        assert_eq!(config.compact_threshold, 4);
        assert_eq!(config.max_compaction_jobs, DEFAULT_MAX_COMPACTION_JOBS);
        assert_eq!(
            config.max_compaction_input_segments,
            DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS
        );
        assert_eq!(
            config.max_compaction_input_bytes,
            DEFAULT_MAX_COMPACTION_INPUT_BYTES
        );
        assert_eq!(
            config.max_compaction_output_bytes,
            DEFAULT_MAX_COMPACTION_OUTPUT_BYTES
        );
        assert_eq!(
            config.compaction_job_time_budget_ms,
            DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS
        );
        assert_eq!(
            config.compaction_io_bytes_per_sec,
            DEFAULT_COMPACTION_IO_BYTES_PER_SEC
        );
        assert_eq!(
            config.compaction_disk_reserve_bytes,
            DEFAULT_COMPACTION_DISK_RESERVE_BYTES
        );
        assert_eq!(
            config.compaction_retry_cooldown_ms,
            DEFAULT_COMPACTION_RETRY_COOLDOWN_MS
        );
        assert_eq!(
            config.l0_soft_limit_segments,
            DEFAULT_L0_SOFT_LIMIT_SEGMENTS
        );
        assert_eq!(
            config.l0_hard_limit_segments,
            DEFAULT_L0_HARD_LIMIT_SEGMENTS
        );
        assert_eq!(config.l0_soft_limit_bytes, DEFAULT_L0_SOFT_LIMIT_BYTES);
        assert_eq!(config.l0_hard_limit_bytes, DEFAULT_L0_HARD_LIMIT_BYTES);
        assert_eq!(
            config.l0_soft_backpressure_wait_ms,
            DEFAULT_L0_SOFT_BACKPRESSURE_WAIT_MS
        );
        assert_eq!(config.wal_flush_trigger, 32 * 1024);
        assert_eq!(config.wal_buffer_size, 64 * 1024);
        assert_eq!(config.wal_max_size, 64 * 1024 * 1024);
        assert_eq!(config.sync_interval_ms, 1000);
        assert!(config.wal_compression);
        assert_eq!(config.keep_snapshots, 3);
        assert_eq!(config.seal_hot_bytes_threshold, 64 * 1024 * 1024);
        assert_eq!(
            config.seal_incremental_hot_bytes_threshold,
            16 * 1024 * 1024
        );
        assert_eq!(config.volume_cache_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.read_queue_depth, 1);
    }

    #[test]
    fn test_persistence_config_durable() {
        let config = PersistenceConfig::durable();
        assert_eq!(config.sync_mode, SyncMode::Full);
        assert_eq!(config.sync_interval_ms, 0);
    }

    #[test]
    fn test_persistence_config_fast() {
        let config = PersistenceConfig::fast();
        assert_eq!(config.sync_mode, SyncMode::None);
    }

    #[test]
    fn test_persistence_config_builder() {
        let config = PersistenceConfig::new()
            .with_sync_mode(SyncMode::Full)
            .with_checkpoint_interval(120)
            .with_compact_threshold(8)
            .with_max_compaction_jobs(4)
            .with_max_compaction_input_segments(6)
            .with_max_compaction_input_bytes(32 * 1024 * 1024)
            .with_max_compaction_output_bytes(48 * 1024 * 1024)
            .with_compaction_job_time_budget_ms(30_000)
            .with_compaction_io_bytes_per_sec(8 * 1024 * 1024)
            .with_compaction_disk_reserve_bytes(16 * 1024 * 1024)
            .with_compaction_retry_cooldown_ms(5_000)
            .with_l0_segment_limits(10, 20)
            .with_l0_byte_limits(64 * 1024 * 1024, 128 * 1024 * 1024)
            .with_l0_soft_backpressure_wait_ms(25)
            .with_seal_hot_bytes_threshold(1024)
            .with_seal_incremental_hot_bytes_threshold(512)
            .with_volume_cache_bytes(256)
            .with_read_queue_depth(8);

        assert_eq!(config.sync_mode, SyncMode::Full);
        assert_eq!(config.checkpoint_interval, 120);
        assert_eq!(config.compact_threshold, 8);
        assert_eq!(config.max_compaction_jobs, 4);
        assert_eq!(config.max_compaction_input_segments, 6);
        assert_eq!(config.max_compaction_input_bytes, 32 * 1024 * 1024);
        assert_eq!(config.max_compaction_output_bytes, 48 * 1024 * 1024);
        assert_eq!(config.compaction_job_time_budget_ms, 30_000);
        assert_eq!(config.compaction_io_bytes_per_sec, 8 * 1024 * 1024);
        assert_eq!(config.compaction_disk_reserve_bytes, 16 * 1024 * 1024);
        assert_eq!(config.compaction_retry_cooldown_ms, 5_000);
        assert_eq!(config.l0_soft_limit_segments, 10);
        assert_eq!(config.l0_hard_limit_segments, 20);
        assert_eq!(config.l0_soft_limit_bytes, 64 * 1024 * 1024);
        assert_eq!(config.l0_hard_limit_bytes, 128 * 1024 * 1024);
        assert_eq!(config.l0_soft_backpressure_wait_ms, 25);
        assert_eq!(config.seal_hot_bytes_threshold, 1024);
        assert_eq!(config.seal_incremental_hot_bytes_threshold, 512);
        assert_eq!(config.volume_cache_bytes, 256);
        assert_eq!(config.read_queue_depth, 8);

        let clamped = PersistenceConfig::new().with_read_queue_depth(0);
        assert_eq!(clamped.read_queue_depth, 1);
        let clamped = PersistenceConfig::new().with_max_compaction_input_segments(0);
        assert_eq!(clamped.max_compaction_input_segments, 1);
        let clamped = PersistenceConfig::new().with_max_compaction_jobs(usize::MAX);
        assert_eq!(clamped.max_compaction_jobs, MAX_COMPACTION_JOBS);
        let clamped = PersistenceConfig::new().with_max_compaction_input_bytes(0);
        assert_eq!(clamped.max_compaction_input_bytes, 1);
        let clamped = PersistenceConfig::new().with_max_compaction_output_bytes(0);
        assert_eq!(clamped.max_compaction_output_bytes, 1);
        let clamped = PersistenceConfig::new().with_l0_segment_limits(0, 0);
        assert_eq!(clamped.l0_soft_limit_segments, 1);
        assert_eq!(clamped.l0_hard_limit_segments, 2);
        let clamped = PersistenceConfig::new().with_l0_byte_limits(0, 0);
        assert_eq!(clamped.l0_soft_limit_bytes, 1);
        assert_eq!(clamped.l0_hard_limit_bytes, 2);
    }

    #[test]
    fn test_persistence_config_compression() {
        // Test disabling all compression
        let config = PersistenceConfig::new().with_compression(false);
        assert!(!config.wal_compression);
        assert!(!config.volume_compression);

        // Test individual compression settings
        let config = PersistenceConfig::new().with_wal_compression(false);
        assert!(!config.wal_compression);
        assert!(config.volume_compression); // volume unaffected

        let config = PersistenceConfig::new().with_volume_compression(false);
        assert!(config.wal_compression); // WAL unaffected
        assert!(!config.volume_compression);
    }

    #[test]
    fn test_config_in_memory() {
        let config = Config::in_memory();
        assert!(config.path.is_none());
        assert!(!config.persistence.enabled);
        assert!(!config.is_persistent());
    }

    #[test]
    fn test_config_with_path() {
        let config = Config::with_path("/tmp/test.db");
        assert_eq!(config.path, Some("/tmp/test.db".to_string()));
        assert!(config.persistence.enabled);
        assert!(config.is_persistent());
    }

    #[test]
    fn test_config_builder() {
        let config =
            Config::with_path("/tmp/test.db").with_persistence(PersistenceConfig::durable());

        assert!(config.is_persistent());
        assert_eq!(config.persistence.sync_mode, SyncMode::Full);
    }
}
