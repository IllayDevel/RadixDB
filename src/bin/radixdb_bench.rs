use postgres::{Client as PgClient, NoTls};
use rusqlite::Connection as SqliteConnection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use radixdb::storage::instrumentation::RuntimeProfileSnapshot;
use radixdb::{
    api::Database,
    client::{
        protocol::{client_protocol_counters_snapshot, ClientProtocolCountersSnapshot},
        ColumnCursorBatch, Connection, CursorFetchMode, ExecuteResult, ServerStatus, WireColumn,
        WireValue,
    },
    server::{Server, ServerConfig},
    storage::instrumentation::{self, EngineCountersSnapshot},
    Engine, Table,
};

const TABLE_COUNT: usize = 120;
const DEFAULT_TEST_ROOT: &str = "RadixTest";
const DEFAULT_PG_PORT: u16 = 55_432;
const DEFAULT_RADIXDB_SERVER_PORT: u16 = 5_440;
const BENCH_DATABASE: &str = "radixdb_bench";
// The benchmark imports one generated table per atomic COPY.  At 100M the
// largest table needs about 1.4 GiB of transaction budget after typed-value
// accounting.  Keep the engine/server default conservative and raise the
// budget only for this isolated, reproducible benchmark fixture.
const BENCH_COPY_TRANSACTION_BYTES: usize = 2 * 1024 * 1024 * 1024;
const DEFAULT_ISOLATED_WARMUP_RUNS: usize = 1;
const DEFAULT_ISOLATED_MEASURED_RUNS: usize = 5;
const SEED_MANIFEST_FILE: &str = "seed_manifest.json";
const POSTGRES_OWNER_MARKER: &str = "BENCHMARK_OWNER.json";
const POSTGRES_OWNER_FORMAT: &str = "radixdb-benchmark-postgres-owner-v1";
const FNV1A64_OFFSET: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x100000001b3;

type DynError = Box<dyn std::error::Error>;
type BenchResult<T> = Result<T, DynError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum PgBenchProfile {
    Default,
    NvmeLocal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum BenchFetchMode {
    Auto,
    Rows,
    Columnar,
}

impl BenchFetchMode {
    fn parse(value: &str) -> BenchResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "rows" | "row" | "row-batches" => Ok(Self::Rows),
            "columnar" | "columns" | "column-batches" => Ok(Self::Columnar),
            _ => Err(
                format!("unsupported fetch mode `{value}`; supported: auto, rows, columnar").into(),
            ),
        }
    }

    fn client_mode(self) -> CursorFetchMode {
        match self {
            Self::Auto => CursorFetchMode::Auto,
            Self::Rows => CursorFetchMode::Rows,
            Self::Columnar => CursorFetchMode::Columnar,
        }
    }
}

impl PgBenchProfile {
    fn parse(value: &str) -> BenchResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "default" => Ok(Self::Default),
            "nvme-local" | "nvme" | "local-nvme" => Ok(Self::NvmeLocal),
            _ => Err(format!(
                "unsupported PostgreSQL profile `{value}`; supported: default, nvme-local"
            )
            .into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::NvmeLocal => "nvme-local",
        }
    }

    fn settings(self) -> &'static [PgProfileSetting] {
        match self {
            Self::Default => PG_DEFAULT_TRACKED_SETTINGS,
            Self::NvmeLocal => PG_NVME_LOCAL_SETTINGS,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct PgProfileSetting {
    name: &'static str,
    value: &'static str,
    apply: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct PgRuntimeSetting {
    name: String,
    requested: Option<String>,
    actual: String,
    unit: Option<String>,
    context: String,
    source: String,
}

const PG_DEFAULT_TRACKED_SETTINGS: &[PgProfileSetting] = &[
    PgProfileSetting {
        name: "shared_buffers",
        value: "<server default>",
        apply: "tracked",
        reason: "major buffer-pool knob; captured for benchmark comparability",
    },
    PgProfileSetting {
        name: "effective_cache_size",
        value: "<server default>",
        apply: "tracked",
        reason: "planner cache-size estimate; captured for benchmark comparability",
    },
    PgProfileSetting {
        name: "maintenance_work_mem",
        value: "<server default>",
        apply: "tracked",
        reason: "index-build memory budget; important for create_indexes phase",
    },
    PgProfileSetting {
        name: "work_mem",
        value: "<server default>",
        apply: "tracked",
        reason: "sort/hash memory budget; important for joins and aggregates",
    },
    PgProfileSetting {
        name: "checkpoint_timeout",
        value: "<server default>",
        apply: "tracked",
        reason: "checkpoint cadence affects bulk-load write pressure",
    },
    PgProfileSetting {
        name: "max_wal_size",
        value: "<server default>",
        apply: "tracked",
        reason: "WAL budget affects checkpoint pressure during large loads",
    },
    PgProfileSetting {
        name: "wal_compression",
        value: "<server default>",
        apply: "tracked",
        reason: "WAL compression changes write volume and CPU trade-off",
    },
    PgProfileSetting {
        name: "synchronous_commit",
        value: "<server default>",
        apply: "tracked",
        reason: "commit durability latency knob; must be explicit in reports",
    },
    PgProfileSetting {
        name: "autovacuum",
        value: "<server default>",
        apply: "tracked",
        reason: "background maintenance can add benchmark noise",
    },
    PgProfileSetting {
        name: "max_parallel_workers_per_gather",
        value: "<server default>",
        apply: "tracked",
        reason: "query parallelism affects scans, joins and aggregates",
    },
    PgProfileSetting {
        name: "effective_io_concurrency",
        value: "<server default>",
        apply: "tracked",
        reason: "NVMe read scheduling hint; important for scan comparisons",
    },
];

const PG_NVME_LOCAL_SETTINGS: &[PgProfileSetting] = &[
    PgProfileSetting {
        name: "shared_buffers",
        value: "4GB",
        apply: "postgresql.conf/restart",
        reason: "local benchmark cluster buffer pool; restart-level setting",
    },
    PgProfileSetting {
        name: "effective_cache_size",
        value: "24GB",
        apply: "session",
        reason: "planner estimate for a RAM-rich single-NVMe workstation",
    },
    PgProfileSetting {
        name: "maintenance_work_mem",
        value: "2GB",
        apply: "session",
        reason: "larger CREATE INDEX memory budget for synthetic benchmark data",
    },
    PgProfileSetting {
        name: "work_mem",
        value: "256MB",
        apply: "session",
        reason: "larger per-query sort/hash budget for joins and aggregates",
    },
    PgProfileSetting {
        name: "checkpoint_timeout",
        value: "30min",
        apply: "postgresql.conf/reload",
        reason: "avoid checkpoint churn during large sequential loads",
    },
    PgProfileSetting {
        name: "max_wal_size",
        value: "64GB",
        apply: "postgresql.conf/reload",
        reason: "avoid WAL-driven checkpoints during large sequential loads",
    },
    PgProfileSetting {
        name: "wal_compression",
        value: "on",
        apply: "postgresql.conf/reload",
        reason: "reduce WAL write volume on large synthetic CSV load",
    },
    PgProfileSetting {
        name: "synchronous_commit",
        value: "off",
        apply: "session",
        reason: "bulk benchmark throughput mode; explicit durability trade-off",
    },
    PgProfileSetting {
        name: "autovacuum",
        value: "off",
        apply: "postgresql.conf/reload",
        reason: "remove background vacuum noise during isolated synthetic runs",
    },
    PgProfileSetting {
        name: "max_parallel_workers_per_gather",
        value: "4",
        apply: "session",
        reason: "allow PG planner to use CPU parallelism on scans/aggregates",
    },
    PgProfileSetting {
        name: "effective_io_concurrency",
        value: "256",
        apply: "postgresql.conf/reload",
        reason: "NVMe-oriented IO concurrency hint",
    },
    PgProfileSetting {
        name: "maintenance_io_concurrency",
        value: "256",
        apply: "postgresql.conf/reload",
        reason: "NVMe-oriented maintenance IO concurrency hint",
    },
    PgProfileSetting {
        name: "random_page_cost",
        value: "1.1",
        apply: "session",
        reason: "SSD/NVMe planner cost model",
    },
    PgProfileSetting {
        name: "seq_page_cost",
        value: "1.0",
        apply: "session",
        reason: "baseline sequential page cost for local NVMe",
    },
    PgProfileSetting {
        name: "parallel_setup_cost",
        value: "0",
        apply: "session",
        reason: "make PG parallel plans visible in synthetic query phases",
    },
    PgProfileSetting {
        name: "parallel_tuple_cost",
        value: "0.01",
        apply: "session",
        reason: "lower tuple transfer penalty for local CPU benchmark",
    },
    PgProfileSetting {
        name: "jit",
        value: "off",
        apply: "session",
        reason: "avoid compilation noise in repeated small query samples",
    },
    PgProfileSetting {
        name: "temp_buffers",
        value: "256MB",
        apply: "session",
        reason: "larger temporary table buffer budget when PG uses temp paths",
    },
];

include!("radixdb_bench/command.rs");
include!("radixdb_bench/metrics.rs");
include!("radixdb_bench/fixture.rs");
include!("radixdb_bench/relational.rs");
include!("radixdb_bench/server.rs");
include!("radixdb_bench/tests.rs");
