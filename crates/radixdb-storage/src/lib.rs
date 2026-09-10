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

//! Canonical storage contracts and physical persistence engine for RadixDB.
//!
//! This crate is the single owner of storage expressions, indexes, immutable
//! volumes, MVCC state, WAL/recovery, page-cache policy and storage statistics.
//! It is a private implementation crate, not an application dependency, and
//! never depends on SQL, functions, executor, API, server or the root facade.

pub mod buffer_pool;
#[doc(hidden)]
pub mod byte_limiter;
pub mod config;
#[doc(hidden)]
pub mod cpu_runtime;
pub mod expression;
pub mod index;
pub mod instrumentation;
pub mod mvcc;
pub mod page_cache;
pub mod statistics;
#[doc(hidden)]
pub mod test_failpoints;
#[cfg(feature = "test-mutations")]
#[doc(hidden)]
pub mod test_mutations;
#[doc(hidden)]
pub mod timestamp;
pub mod traits;
#[doc(hidden)]
pub mod v6;
pub mod validation;
pub mod volume;

pub use buffer_pool::BufferPool;
pub use config::{CleanupConfig, Config, PersistenceConfig, SyncMode};
pub use expression::{
    AndExpr, BetweenExpr, CastExpr, ComparisonExpr, CompoundExpr, Expression, InListExpr, NotExpr,
    NullCheckExpr, OrExpr, RangeExpr,
};
pub use index::{
    intersect_multiple_sorted_ids, intersect_sorted_ids, union_multiple_sorted_ids,
    union_sorted_ids, BTreeIndex, BitmapIndex, CompositeKey, HashIndex, HnswDistanceMetric,
    HnswIndex, MultiColumnIndex, PartialIndex, PartialIndexPredicate, PartialIndexPredicateBinder,
    PartialIndexPredicateMetadata, PkIndex, PreparedIndexKeyEncoder, RenamedIndex,
};
pub use page_cache::PageCacheWarmupSnapshot;
pub use statistics::{
    is_stats_table, ColumnStats, Histogram, HistogramOp, SelectivityEstimator, TableStats,
    CREATE_COLUMN_STATS_SQL, CREATE_TABLE_STATS_SQL, DEFAULT_HISTOGRAM_BUCKETS,
    DEFAULT_SAMPLE_SIZE, SYS_COLUMN_STATS, SYS_TABLE_STATS,
};
pub use traits::{
    AggregateOp, AliasedResult, DeferredColumnSource, DeferredRow, DeferredSum, EmptyResult,
    EmptyScanner, Engine, GroupedAggregateResult, Index, MemoryResult, PendingIndexDefinition,
    PendingIndexDrop, PendingSchemaChange, PendingTableRename, PhysicalSnapshotIdentity,
    QueryResult, ScanPlan, Scanner, ScannerResult, SchemaPhysicalTransition, Table, TemporalType,
    Transaction, VecScanner,
};
pub use validation::{PreparedRowValidator, RowValidatorBinder};
pub use volume::zonemap::{PruneStats, TableZoneMap, DEFAULT_SEGMENT_SIZE};
