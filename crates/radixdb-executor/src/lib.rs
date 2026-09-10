//! SQL binding, planning, and execution engine for RadixDB.
//!
//! This private implementation crate is the canonical owner of SQL binding,
//! planning, relational operators, DDL/DML dispatch and result execution. The
//! root `radixdb::executor` path is a compatibility facade, not a second owner.
//! Allowed internal dependencies are limited to `radixdb-core`,
//! `radixdb-sql`, `radixdb-functions`, `radixdb-catalog`,
//! `radixdb-procedural`, and `radixdb-storage`.

#![forbid(unsafe_code)]

pub mod access;
pub mod aggregation;
pub mod application;
mod authorization;
pub mod binding;
pub mod compiled_plan;
pub mod context;
pub mod credentials;
pub mod cte;
pub mod dispatch;
mod executor;
mod executor_host;
mod explain;
pub mod expr_converter;
pub mod expression;
pub mod hash_table;
pub mod index_optimizer;
mod index_optimizer_host;
pub mod join_executor;
pub mod join_graph;
pub mod lookup_key;
#[doc(hidden)]
pub mod memory;
pub mod mutation;
mod mutation_host;
pub mod navigation;
pub mod operator;
pub mod operators;
pub mod optimizer;
pub mod parallel;
pub mod pipeline;
pub mod planner;
mod prepared;
pub mod procedural;
pub mod public_read;
pub mod pushdown;
mod query;
pub mod query_cache;
pub mod query_classification;
pub mod result;
pub mod semantic_cache;
mod show;
pub mod statistics;
pub mod subquery;
#[cfg(feature = "test-mutations")]
#[doc(hidden)]
pub mod test_mutations;
pub mod utils;
pub mod window;

mod catalog;

#[doc(hidden)]
pub use catalog::{bind_runtime_catalog, plugin_catalog_runtime_binder};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use executor::test_executor_construction_count;
pub use executor::{count_parameters, Executor};
#[doc(hidden)]
pub use prepared::{program_contains_transaction_control, PreparedProgram};
pub use public_read::{
    BoundPublicReadPolicy, PublicReadColumnBinding, PublicReadLimits, PublicReadMaterialized,
    PublicReadRelationBinding, PublicReadRelationSpec,
};

// Historical `radixdb::executor::*` names remain source-compatible through
// direct exports from their canonical owner. The root crate aliases this crate
// and no longer carries a mirror module tree.
pub use binding::output::QueryOutputColumn;
pub use context::{clear_all_thread_local_caches, ExecutionContext, TimeoutGuard};
pub use expression::{
    CompileContext, CompileError, CompiledEvaluator, ExecuteContext, ExprCompiler, ExprVM,
    Program as ExprProgram,
};
pub use hash_table::{hash_row_keys, verify_key_equality, JoinHashTable};
pub use join_executor::{JoinAnalysis, JoinExecutor, JoinInputOrderings, JoinRequest, JoinResult};
pub use operator::{
    ColumnInfo, CompositeRow, EmptyOperator, MaterializedOperator, Operator, OrderingProperty,
    RowRef,
};
pub use operators::{
    BatchIndexNestedLoopJoinOperator, HashJoinOperator, IndexLookupStrategy,
    IndexNestedLoopJoinOperator, JoinProjection, JoinSide, JoinType, MergeJoinOperator,
    NestedLoopJoinOperator,
};
pub use parallel::{
    JoinType as ParallelJoinType, ParallelConfig, DEFAULT_PARALLEL_CHUNK_SIZE,
    DEFAULT_PARALLEL_FILTER_THRESHOLD, DEFAULT_PARALLEL_JOIN_THRESHOLD,
};
pub use planner::{
    ColumnStatsCache, QueryPlanner, RuntimeJoinAlgorithm, RuntimeJoinDecision, StatsHealth,
};
pub use query_cache::{CacheStats, CachedPlanRef, CachedQueryPlan, QueryCache, DEFAULT_CACHE_SIZE};
pub use query_classification::clear_classification_cache;
pub use radixdb_plugin_host::{DatabasePluginAdmission, PluginRegistry};
pub use result::{ColumnarResult, ExecResult, ExecutionResult, ExecutorResult};
pub use semantic_cache::{
    CacheLookupResult, CachedResult, QueryFingerprint, SemanticCache, SemanticCacheStats,
    SemanticCacheStatsSnapshot, SubsumptionResult, DEFAULT_CACHE_TTL_SECS, DEFAULT_MAX_CACHED_ROWS,
    DEFAULT_MAX_GLOBAL_CACHED_BYTES, DEFAULT_SEMANTIC_CACHE_SIZE,
};
pub use utils::{compute_join_projection, extract_join_keys_and_residual, JoinProjectionIndices};
