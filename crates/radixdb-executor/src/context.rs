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

//! Execution Context
//!
//! This module provides the execution context for SQL queries, including
//! parameter handling, transaction state, and query options.

use chrono::{DateTime, Utc};
use lru::LruCache;
use radixdb_catalog::ObjectId;
use radixdb_core::time_compat::{system_time_now, Instant};
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Duration;

// Cache size limits for subquery caches to prevent unbounded memory growth.
// These are per-thread limits since the caches are thread-local.
const SCALAR_SUBQUERY_CACHE_SIZE: usize = 128;
const IN_SUBQUERY_CACHE_SIZE: usize = 128;
const SEMI_JOIN_CACHE_SIZE: usize = 256;

use crate::hash_table::{JoinHashState, JoinHashTable, JoinMemoryOwner, JoinMemoryReservation};
use radixdb_core::ParamVec;
use radixdb_core::{CompactArc, StringMap};
use radixdb_core::{Result, Row, Value, ValueMap, ValueSet};
use radixdb_procedural::BudgetOwner;

/// Request-local bridge used by expression bytecode for durable SQL
/// functions.  The executor owns resolution, MVCC, principals and procedural
/// budgets; the expression VM only forwards evaluated scalar arguments.
pub(crate) trait StoredFunctionInvoker: std::fmt::Debug + Send + Sync {
    fn invoke(self: Arc<Self>, name: &str, arguments: &[Value]) -> Result<Value>;

    fn external_equal(&self, left: &Value, right: &Value) -> Result<bool> {
        let _ = (left, right);
        Err(radixdb_core::Error::NotSupported(
            "external equality is unavailable in this execution context".to_owned(),
        ))
    }

    fn external_compare(&self, left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
        let _ = (left, right);
        Err(radixdb_core::Error::NotSupported(
            "external ordering is unavailable in this execution context".to_owned(),
        ))
    }

    fn external_input(&self, type_name: &str, input: &Value) -> Result<Value> {
        let _ = (type_name, input);
        Err(radixdb_core::Error::NotSupported(
            "external type input is unavailable in this execution context".to_owned(),
        ))
    }

    fn external_output(&self, value: &Value, target_type: radixdb_core::DataType) -> Result<Value> {
        let _ = (value, target_type);
        Err(radixdb_core::Error::NotSupported(
            "external type output is unavailable in this execution context".to_owned(),
        ))
    }
}

pub(crate) const STORED_OPERATOR_CALL_PREFIX: &str = "\u{1f}operator:";

// Static defaults for ExecutionContext to avoid allocations for empty values.
// These are shared across all contexts and only require Arc refcount bump on clone.
// Note: cancelled is NOT shared - each context needs its own cancellation flag.
static EMPTY_PARAMS: LazyLock<CompactArc<ParamVec>> =
    LazyLock::new(|| CompactArc::new(ParamVec::new()));
static EMPTY_DATABASE: LazyLock<Arc<Option<String>>> = LazyLock::new(|| Arc::new(None));
static EMPTY_SESSION_VARS: LazyLock<Arc<AHashMap<String, Value>>> =
    LazyLock::new(|| Arc::new(AHashMap::new()));

pub(crate) fn is_system_context_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CURRENT_PRINCIPAL"
            | "CURRENT_EFFECTIVE_PRINCIPAL"
            | "CURRENT_TRANSACTION_ID"
            | "CURRENT_STATEMENT_TIMESTAMP"
            | "CURRENT_REQUEST_ID"
            | "CURRENT_IDEMPOTENCY_KEY"
            | "CURRENT_JOB_ID"
            | "CURRENT_JOB_ATTEMPT"
            | "CURRENT_JOB_SCHEDULED_AT"
    )
}

// Cache for scalar subquery results to avoid re-execution.
// Thread-local to avoid synchronization overhead.
// Uses SQL string as key (not hash) to avoid collision risk.
// Stores (tables_referenced, result) for table-based invalidation.
// LRU-bounded to prevent unbounded memory growth.
use crate::expression::RowFilter;
use smallvec::SmallVec;

/// Cached scalar subquery entry: (tables_referenced for invalidation, result value)
type ScalarSubqueryCacheEntry = (SmallVec<[CompactArc<str>; 2]>, Value);

thread_local! {
    static SCALAR_SUBQUERY_CACHE: RefCell<LruCache<String, ScalarSubqueryCacheEntry>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(SCALAR_SUBQUERY_CACHE_SIZE).unwrap()));
    static EXISTS_PREDICATE_CACHE: RefCell<FxHashMap<String, RowFilter>> =
        RefCell::new(FxHashMap::default());
    static ACTIVE_QUERY_CANCELLATION: RefCell<Vec<CancellationHandle>> = const {
        RefCell::new(Vec::new())
    };
}

/// Clear the expression-VM-owned EXISTS predicate cache.
pub fn clear_exists_predicate_cache() {
    EXISTS_PREDICATE_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Return a compiled EXISTS predicate from the expression-VM-owned cache.
pub fn get_cached_exists_predicate(key: &str) -> Option<RowFilter> {
    EXISTS_PREDICATE_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Store a compiled EXISTS predicate in the expression-VM-owned cache.
pub fn cache_exists_predicate(key: String, filter: RowFilter) {
    EXISTS_PREDICATE_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, filter);
    });
}

#[doc(hidden)]
pub struct StatementCancellationScope;

impl Drop for StatementCancellationScope {
    fn drop(&mut self) {
        ACTIVE_QUERY_CANCELLATION.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

#[doc(hidden)]
pub fn current_query_is_cancelled() -> bool {
    ACTIVE_QUERY_CANCELLATION.with(|stack| {
        stack
            .borrow()
            .last()
            .is_some_and(CancellationHandle::is_cancelled)
    })
}

/// Pass the active query signal to a lower-level consumer without exposing
/// `ExecutionContext` as part of that consumer's contract.
#[inline]
#[doc(hidden)]
pub fn with_current_query_cancellation<T>(
    callback: impl FnOnce(Option<&dyn radixdb_functions::FunctionCancellation>) -> T,
) -> T {
    ACTIVE_QUERY_CANCELLATION.with(|stack| {
        let stack = stack.borrow();
        callback(
            stack
                .last()
                .map(|handle| handle as &dyn radixdb_functions::FunctionCancellation),
        )
    })
}

#[inline]
#[doc(hidden)]
pub fn check_current_query_cancelled() -> Result<()> {
    if current_query_is_cancelled() {
        Err(radixdb_core::Error::QueryCancelled)
    } else {
        Ok(())
    }
}

/// Clear the scalar subquery cache completely.
/// NOTE: For normal operation, use `invalidate_scalar_subquery_cache_for_table` instead.
/// This is only used for explicit cache clearing (e.g., after DDL operations).
pub fn clear_scalar_subquery_cache() {
    SCALAR_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Invalidate scalar subquery cache entries for a specific table.
/// Should be called after INSERT, UPDATE, DELETE, or TRUNCATE on a table.
#[inline]
pub fn invalidate_scalar_subquery_cache_for_table(table_name: &str) {
    SCALAR_SUBQUERY_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if c.is_empty() {
            return;
        }
        // Collect keys to remove (LruCache doesn't have retain)
        let keys_to_remove: Vec<String> = c
            .iter()
            .filter(|(_, (tables, _))| tables.iter().any(|t| t.eq_ignore_ascii_case(table_name)))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            c.pop(&key);
        }
    });
}

/// Get a cached scalar subquery result by SQL string key.
pub fn get_cached_scalar_subquery(key: &str) -> Option<Value> {
    SCALAR_SUBQUERY_CACHE.with(|cache| cache.borrow_mut().get(key).map(|(_, v)| v.clone()))
}

/// Cache a scalar subquery result with the tables it references.
pub fn cache_scalar_subquery(key: String, tables: SmallVec<[CompactArc<str>; 2]>, value: Value) {
    SCALAR_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().put(key, (tables, value));
    });
}

// Cache for IN subquery results to avoid re-execution.
// Thread-local to avoid synchronization overhead.
// Uses SQL string as key (not hash) to avoid collision risk.
// Stores (tables_referenced, result) for table-based invalidation.
// LRU-bounded to prevent unbounded memory growth.

/// Cached IN subquery entry: (tables_referenced for invalidation, result values)
type InSubqueryCacheEntry = (SmallVec<[CompactArc<str>; 2]>, Vec<Value>);

thread_local! {
    static IN_SUBQUERY_CACHE: RefCell<LruCache<String, InSubqueryCacheEntry>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(IN_SUBQUERY_CACHE_SIZE).unwrap()));
}

/// Clear the IN subquery cache completely.
/// NOTE: For normal operation, use `invalidate_in_subquery_cache_for_table` instead.
/// This is only used for explicit cache clearing (e.g., after DDL operations).
pub fn clear_in_subquery_cache() {
    IN_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Invalidate IN subquery cache entries for a specific table.
/// Should be called after INSERT, UPDATE, DELETE, or TRUNCATE on a table.
#[inline]
pub fn invalidate_in_subquery_cache_for_table(table_name: &str) {
    IN_SUBQUERY_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if c.is_empty() {
            return;
        }
        // Collect keys to remove (LruCache doesn't have retain)
        let keys_to_remove: Vec<String> = c
            .iter()
            .filter(|(_, (tables, _))| tables.iter().any(|t| t.eq_ignore_ascii_case(table_name)))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            c.pop(&key);
        }
    });
}

/// Get a cached IN subquery result by SQL string key.
pub fn get_cached_in_subquery(key: &str) -> Option<Vec<Value>> {
    IN_SUBQUERY_CACHE.with(|cache| cache.borrow_mut().get(key).map(|(_, v)| v.clone()))
}

/// Cache an IN subquery result with the tables it references.
pub fn cache_in_subquery(key: String, tables: SmallVec<[CompactArc<str>; 2]>, values: Vec<Value>) {
    IN_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().put(key, (tables, values));
    });
}

use radixdb_sql::ast::{Expression, SelectStatement};

/// Extract actual table names from a SelectStatement for cache invalidation.
/// This returns the real table names (not aliases) because DML operations
/// reference tables by their actual names, not aliases.
pub fn extract_table_names_for_cache(stmt: &SelectStatement) -> SmallVec<[CompactArc<str>; 2]> {
    let mut tables = SmallVec::new();
    if let Some(ref table_expr) = stmt.table_expr {
        collect_real_table_names(table_expr, &mut tables);
    }
    tables
}

/// Recursively collect actual table names (not aliases) from a table source expression.
fn collect_real_table_names(source: &Expression, tables: &mut SmallVec<[CompactArc<str>; 2]>) {
    match source {
        Expression::TableSource(ts) => {
            // Always use the actual table name for cache invalidation
            tables.push(CompactArc::from(ts.name.value_lower.as_str()));
        }
        Expression::JoinSource(js) => {
            collect_real_table_names(&js.left, tables);
            collect_real_table_names(&js.right, tables);
        }
        Expression::SubquerySource(ss) => {
            // Recursively extract tables from nested subquery
            if let Some(ref table_expr) = ss.subquery.table_expr {
                collect_real_table_names(table_expr, tables);
            }
        }
        _ => {}
    }
}

// Cache for semi-join (EXISTS) hash sets to avoid re-execution.
// Thread-local to avoid synchronization overhead.
// Uses u64 hash key to avoid string allocation entirely.
// LRU-bounded to prevent unbounded memory growth.
use ahash::AHashMap;
use std::hash::{Hash, Hasher};

/// Cached semi-join entry: (table_name for invalidation, hash_set values)
type SemiJoinCacheEntry = (CompactArc<str>, CompactArc<ValueSet>);

/// Compute a cache key hash from table, column, and predicate hash without allocation.
#[inline]
pub fn compute_semi_join_cache_key(table: &str, column: &str, pred_hash: u64) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    table.hash(&mut hasher);
    column.hash(&mut hasher);
    pred_hash.hash(&mut hasher);
    hasher.finish()
}

thread_local! {
    static SEMI_JOIN_CACHE: RefCell<LruCache<u64, SemiJoinCacheEntry>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(SEMI_JOIN_CACHE_SIZE).unwrap()));
}

/// Clear the semi-join cache completely.
/// NOTE: This is now only used for explicit cache clearing (e.g., after DDL operations).
/// For DML operations, use `invalidate_semi_join_cache_for_table` instead.
pub fn clear_semi_join_cache() {
    SEMI_JOIN_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Invalidate semi-join cache entries for a specific table.
/// Should be called after INSERT, UPDATE, DELETE, or TRUNCATE on a table.
#[inline]
pub fn invalidate_semi_join_cache_for_table(table_name: &str) {
    SEMI_JOIN_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if c.is_empty() {
            return;
        }
        // Collect keys to remove (LruCache doesn't have retain)
        let keys_to_remove: Vec<u64> = c
            .iter()
            .filter(|(_, (key_table, _))| key_table.eq_ignore_ascii_case(table_name))
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            c.pop(&key);
        }
    });
}

/// Get a cached semi-join hash set by key hash.
#[inline]
pub fn get_cached_semi_join(key_hash: u64) -> Option<CompactArc<ValueSet>> {
    SEMI_JOIN_CACHE.with(|cache| {
        cache
            .borrow_mut()
            .get(&key_hash)
            .map(|(_, v)| CompactArc::clone(v))
    })
}

/// Cache a semi-join hash set result (CompactArc version for zero-copy).
#[inline]
pub fn cache_semi_join_arc(key_hash: u64, table: &str, values: CompactArc<ValueSet>) {
    SEMI_JOIN_CACHE.with(|cache| {
        cache
            .borrow_mut()
            .put(key_hash, (CompactArc::from(table), values));
    });
}

// Cache for EXISTS index lookups to avoid re-fetching per row.
// The key is "table_name:column_name", the value is the index reference.
use radixdb_storage::traits::Index;
thread_local! {
    static EXISTS_INDEX_CACHE: RefCell<FxHashMap<String, std::sync::Arc<dyn Index>>> = RefCell::new(FxHashMap::default());
}

/// Clear the EXISTS index cache. Should be called at the start of each top-level query.
pub fn clear_exists_index_cache() {
    EXISTS_INDEX_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Get a cached EXISTS index by key.
pub fn get_cached_exists_index(key: &str) -> Option<std::sync::Arc<dyn Index>> {
    EXISTS_INDEX_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Cache an EXISTS index.
pub fn cache_exists_index(key: String, index: std::sync::Arc<dyn Index>) {
    EXISTS_INDEX_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, index);
    });
}

/// Type alias for row fetcher function used in EXISTS/COUNT optimization.
pub type RowFetcher =
    Box<dyn Fn(&[i64]) -> radixdb_core::Result<radixdb_core::RowVec> + Send + Sync>;

/// Type alias for row counter function used in COUNT(*) optimization.
/// This only counts visible rows without cloning their data.
pub type RowCounter = Box<dyn Fn(&[i64]) -> usize + Send + Sync>;

// Cache for EXISTS row fetchers to avoid repeated version store lookups.
// The key is the table name, the value is the row fetcher function.
thread_local! {
    static EXISTS_FETCHER_CACHE: RefCell<FxHashMap<String, std::sync::Arc<RowFetcher>>> = RefCell::new(FxHashMap::default());
}

// Cache for COUNT row counters to avoid repeated version store lookups.
// The key is the table name, the value is the row counter function.
thread_local! {
    static COUNT_COUNTER_CACHE: RefCell<FxHashMap<String, std::sync::Arc<RowCounter>>> = RefCell::new(FxHashMap::default());
}

/// Clear the EXISTS row fetcher cache. Should be called at the start of each top-level query.
pub fn clear_exists_fetcher_cache() {
    EXISTS_FETCHER_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Clear the COUNT row counter cache. Should be called at the start of each top-level query.
pub fn clear_count_counter_cache() {
    COUNT_COUNTER_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Get a cached EXISTS row fetcher by table name.
pub fn get_cached_exists_fetcher(key: &str) -> Option<std::sync::Arc<RowFetcher>> {
    EXISTS_FETCHER_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Get a cached COUNT row counter by table name.
pub fn get_cached_count_counter(key: &str) -> Option<std::sync::Arc<RowCounter>> {
    COUNT_COUNTER_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Cache an EXISTS row fetcher.
pub fn cache_exists_fetcher(key: String, fetcher: RowFetcher) {
    EXISTS_FETCHER_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, std::sync::Arc::new(fetcher));
    });
}

/// Cache a COUNT row counter.
pub fn cache_count_counter(key: String, counter: RowCounter) {
    COUNT_COUNTER_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, std::sync::Arc::new(counter));
    });
}

// Cache for table schema column names to avoid repeated get_table_schema() calls.
// The key is the table name, the value is the list of column names.
thread_local! {
    static EXISTS_SCHEMA_CACHE: RefCell<FxHashMap<String, CompactArc<Vec<String>>>> = RefCell::new(FxHashMap::default());
}

/// Clear the EXISTS schema cache. Should be called at the start of each top-level query.
pub fn clear_exists_schema_cache() {
    EXISTS_SCHEMA_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Get cached table column names by table name.
pub fn get_cached_exists_schema(key: &str) -> Option<CompactArc<Vec<String>>> {
    EXISTS_SCHEMA_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Cache table column names (takes Arc for zero-copy sharing).
pub fn cache_exists_schema(key: String, columns: CompactArc<Vec<String>>) {
    EXISTS_SCHEMA_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, columns);
    });
}

// Cache for pre-computed EXISTS predicate cache keys to avoid expensive format!("{:?}") on every probe.
// The key is the subquery pointer address (usize), the value is the predicate cache key.
thread_local! {
    static EXISTS_PRED_KEY_CACHE: RefCell<FxHashMap<usize, String>> = RefCell::new(FxHashMap::default());
}

/// Clear the EXISTS predicate key cache.
pub fn clear_exists_pred_key_cache() {
    EXISTS_PRED_KEY_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Get cached predicate cache key by subquery pointer address.
#[inline]
pub fn get_cached_exists_pred_key(subquery_ptr: usize) -> Option<String> {
    EXISTS_PRED_KEY_CACHE.with(|cache| cache.borrow().get(&subquery_ptr).cloned())
}

/// Cache a predicate cache key.
#[inline]
pub fn cache_exists_pred_key(subquery_ptr: usize, pred_key: String) {
    EXISTS_PRED_KEY_CACHE.with(|cache| {
        cache.borrow_mut().insert(subquery_ptr, pred_key);
    });
}

// Cache for batch aggregate subquery results (e.g., COUNT(*) GROUP BY user_id).
// Thread-local to avoid synchronization overhead.
// The key is a stable identifier for the subquery, the value is a map from group key to aggregate value.
thread_local! {
    static BATCH_AGGREGATE_CACHE: RefCell<FxHashMap<String, CompactArc<ValueMap<Value>>>> = RefCell::new(FxHashMap::default());
}

/// Clear the batch aggregate cache. Should be called at the start of each top-level query.
pub fn clear_batch_aggregate_cache() {
    BATCH_AGGREGATE_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
}

/// Get a cached batch aggregate result map by subquery identifier.
pub fn get_cached_batch_aggregate(key: &str) -> Option<CompactArc<ValueMap<Value>>> {
    BATCH_AGGREGATE_CACHE.with(|cache| cache.borrow().get(key).cloned())
}

/// Cache a batch aggregate result map.
pub fn cache_batch_aggregate(key: String, values: ValueMap<Value>) {
    BATCH_AGGREGATE_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, CompactArc::new(values));
    });
}

/// Pre-computed info for batch aggregate lookups to avoid per-row allocations.
#[derive(Clone)]
pub struct BatchAggregateLookupInfo {
    /// The cache key for the batch aggregate results
    pub cache_key: String,
    /// The outer column name (lowercase) to look up in outer_row
    pub outer_column_lower: String,
    /// Optional qualified outer column name (e.g., "u.id")
    pub outer_qualified_lower: Option<String>,
    /// Whether this is a COUNT expression (returns 0 for missing keys)
    pub is_count: bool,
}

// Cache for batch aggregate lookup info to avoid recomputing per row.
// The key is the subquery pointer address (usize), avoiding expensive to_string() per row.
// Value is Arc-wrapped to avoid cloning strings on every lookup.
thread_local! {
    static BATCH_AGGREGATE_INFO_CACHE: RefCell<FxHashMap<usize, Option<Arc<BatchAggregateLookupInfo>>>> = RefCell::new(FxHashMap::default());
}

/// Clear the batch aggregate info cache.
pub fn clear_batch_aggregate_info_cache() {
    BATCH_AGGREGATE_INFO_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
}

/// Get cached batch aggregate lookup info by subquery pointer address.
/// Returns Arc to avoid cloning strings on every lookup.
#[inline]
pub fn get_cached_batch_aggregate_info(
    subquery_ptr: usize,
) -> Option<Option<Arc<BatchAggregateLookupInfo>>> {
    BATCH_AGGREGATE_INFO_CACHE.with(|cache| cache.borrow().get(&subquery_ptr).cloned())
}

/// Cache batch aggregate lookup info and return the Arc-wrapped version.
/// Returns None if info was None (not batchable).
#[inline]
pub fn cache_batch_aggregate_info(
    subquery_ptr: usize,
    info: Option<BatchAggregateLookupInfo>,
) -> Option<Arc<BatchAggregateLookupInfo>> {
    let arc_info = info.map(Arc::new);
    let result = arc_info.clone();
    BATCH_AGGREGATE_INFO_CACHE.with(|cache| {
        cache.borrow_mut().insert(subquery_ptr, arc_info);
    });
    result
}

/// Pre-computed info for index nested loop EXISTS lookups to avoid per-row string operations.
/// This caches the pre-computed lowercase column names for O(1) outer row lookups.
#[derive(Clone)]
pub struct ExistsCorrelationInfo {
    /// The outer column name in original case
    pub outer_column: String,
    /// The outer table name (optional)
    pub outer_table: Option<String>,
    /// The inner column name
    pub inner_column: String,
    /// The inner table name
    pub inner_table: String,
    /// Pre-computed lowercase outer column name for fast HashMap lookup
    pub outer_column_lower: String,
    /// Pre-computed qualified outer column name (e.g., "u.id") in lowercase
    pub outer_qualified_lower: Option<String>,
    /// The additional predicate beyond the correlation (if any)
    pub additional_predicate: Option<Expression>,
    /// Pre-computed index cache key ("table:column") to avoid per-probe format! allocation
    pub index_cache_key: String,
}

// Cache for EXISTS correlation info to avoid per-row extraction.
// The key is the subquery pointer address (usize), avoiding format! allocation.
// Value is Arc-wrapped to avoid cloning strings on every lookup.
thread_local! {
    static EXISTS_CORRELATION_CACHE: RefCell<FxHashMap<usize, Option<Arc<ExistsCorrelationInfo>>>> = RefCell::new(FxHashMap::default());
}

/// Clear the EXISTS correlation cache.
pub fn clear_exists_correlation_cache() {
    EXISTS_CORRELATION_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Clear ALL thread-local caches to release memory.
/// Call this when a database is dropped to prevent memory leaks.
/// This also shrinks all cache capacities to zero where applicable.
pub fn clear_executor_thread_local_caches() {
    // Clear LRU-bounded caches (no shrink_to_fit needed - fixed capacity)
    SCALAR_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    IN_SUBQUERY_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    SEMI_JOIN_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    // Clear and shrink unbounded caches
    EXISTS_INDEX_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    EXISTS_FETCHER_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    COUNT_COUNTER_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    EXISTS_SCHEMA_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    EXISTS_PRED_KEY_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    BATCH_AGGREGATE_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    BATCH_AGGREGATE_INFO_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });
    EXISTS_CORRELATION_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.clear();
        c.shrink_to_fit();
    });

    // Clear storage expression caches (regex patterns)
    radixdb_storage::expression::clear_regex_cache();
    radixdb_storage::expression::clear_like_regex_cache();

    // Clear RowVec and RowIdVec thread-local pools
    radixdb_core::row_vec::clear_row_vec_pool();
    radixdb_core::row_vec::clear_row_id_vec_pool();

    // Clear global transaction version map pools
    radixdb_storage::mvcc::clear_version_map_pools();
}

/// Clear every executor, expression and storage thread-local cache.
pub fn clear_all_thread_local_caches() {
    clear_executor_thread_local_caches();
    clear_exists_predicate_cache();
    crate::expression::clear_program_cache();
    crate::query::clear_join_dependency_projection_cache();
    crate::utils::clear_join_projection_lookup_cache();
    crate::query_classification::clear_classification_cache();
}

/// Get cached EXISTS correlation info by subquery pointer address.
/// Returns Arc to avoid cloning strings on every lookup.
#[inline]
pub fn get_cached_exists_correlation(
    subquery_ptr: usize,
) -> Option<Option<Arc<ExistsCorrelationInfo>>> {
    EXISTS_CORRELATION_CACHE.with(|cache| cache.borrow().get(&subquery_ptr).cloned())
}

/// Cache EXISTS correlation info and return the Arc-wrapped version.
/// Returns None if info was None (correlation not extractable).
#[inline]
pub fn cache_exists_correlation(
    subquery_ptr: usize,
    info: Option<ExistsCorrelationInfo>,
) -> Option<Arc<ExistsCorrelationInfo>> {
    let arc_info = info.map(Arc::new);
    let result = arc_info.clone();
    EXISTS_CORRELATION_CACHE.with(|cache| {
        cache.borrow_mut().insert(subquery_ptr, arc_info);
    });
    result
}

/// Execution context for SQL queries
///
/// The execution context carries state and configuration for query execution,
/// including parameters, transaction state, and cancellation support.
///
/// Note: This struct uses Arc for immutable shared data to make cloning cheap
/// during correlated subquery processing where context is cloned per row.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    session: SessionState,
    query: QueryState,
}

/// State inherited from the connection/session boundary. Query derivation
/// clones this owner without mixing it with request-local cancellation,
/// correlated rows, CTE materialization or memory accounting.
#[derive(Debug, Clone)]
struct SessionState {
    /// Whether to use auto-commit for DML statements.
    auto_commit: bool,
    /// Current database/schema name.
    current_database: Arc<Option<String>>,
    /// Variables established with `SET` for this session.
    session_vars: Arc<AHashMap<String, Value>>,
    /// Stable Principal identity established by the authentication boundary.
    /// Transport credentials never enter executor state.
    principal_id: ObjectId,
}

/// State owned by one top-level statement and its derived nested queries.
/// Clones deliberately share cancellation, timeout, JOIN accounting and CTE
/// materialization while carrying their own depth and correlated-row cursor.
#[derive(Debug, Clone)]
struct QueryState {
    /// Principal whose privileges apply at the current nested execution
    /// boundary. `None` means the authenticated session principal. This is
    /// request-local so SECURITY DEFINER never mutates connection identity.
    effective_principal_id: Option<ObjectId>,
    /// Query parameters ($1, $2, etc.) - wrapped in Arc for cheap cloning
    params: CompactArc<ParamVec>,
    /// Named parameters (:name) - wrapped in Arc for cheap cloning
    named_params: Arc<FxHashMap<String, Value>>,
    /// Cancellation flag
    cancelled: Arc<AtomicBool>,
    /// Set only by the timeout manager before it cancels this request.
    timed_out: Arc<AtomicBool>,
    /// Request-local lifecycle gauge for ReferenceExpand owners. Context
    /// clones used by subqueries share the same counter.
    active_reference_expands: Arc<AtomicUsize>,
    /// Optional session/server lifetime whose cancellation also terminates this
    /// query without making request-local cancellation poison later queries.
    parent_cancelled: Option<Arc<AtomicBool>>,
    /// Query timeout in milliseconds (0 = no timeout)
    timeout_ms: u64,
    /// Maximum additional blocking memory retained by all physical JOIN owners
    /// of this request. Oversized builds use bounded fallbacks.
    join_hash_state_max_bytes: usize,
    /// Request-local immutable hash states. Context clones used by nested JOIN
    /// expressions share this bounded owner; unrelated top-level requests do not.
    join_hash_states: Arc<Mutex<JoinHashStateCache>>,
    /// Shared accounting owner for cache entries and currently executing
    /// hash/parallel/merge states. Context clones cannot each spend the limit.
    join_memory_owner: Arc<JoinMemoryOwner>,
    /// Current view nesting depth (for detecting infinite recursion)
    view_depth: usize,
    /// Query execution depth (0 = top-level query, >0 = subquery/nested)
    /// Used to ensure TimeoutGuard is only created once at the top level
    query_depth: usize,
    /// Outer row context for correlated subqueries
    /// Maps column name (lowercase) to value from the outer query
    /// Uses FxHashMap<CompactArc<str>, Value> for zero-cost key cloning in hot loops
    /// Ownership can be recovered through `take_outer_row` in optimized loops.
    outer_row: Option<FxHashMap<CompactArc<str>, Value>>,
    /// Outer row column names (for qualified identifier resolution) - wrapped in Arc
    outer_columns: Option<CompactArc<Vec<String>>>,
    /// CTE data for subqueries to reference CTEs from outer query
    /// Maps CTE name (lowercase) to (columns, rows)
    cte_data: Option<Arc<CteDataMap>>,
    /// Current transaction ID for CURRENT_TRANSACTION_ID() function
    transaction_id: Option<u64>,
    /// Executor-owned durable function dispatcher for this request.
    stored_function_invoker: Option<Arc<dyn StoredFunctionInvoker>>,
    /// Shared owner for a complete procedural call and every SQL/function leaf
    /// entered from it. Context clones must never mint a fresh nested budget.
    procedural_budget: Option<BudgetOwner>,
    /// Optional request-wide row-source admission owner installed only by the
    /// public ORM read boundary. Every scanner opened by this context shares
    /// the same counter, including JOIN inputs and nested execution helpers.
    public_scan_budget: Option<Arc<PublicScanBudget>>,
}

#[derive(Debug)]
struct PublicScanBudget {
    remaining: AtomicUsize,
}

impl PublicScanBudget {
    fn new(max_rows: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(max_rows),
        }
    }

    fn claim(&self, rows: usize) -> Result<()> {
        self.remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(rows)
            })
            .map(|_| ())
            .map_err(|_| {
                radixdb_core::Error::invalid_argument("public read scanned-row budget exceeded")
            })
    }
}

/// Type alias for CTE data: (columns, rows) with Arc for zero-copy sharing
/// Uses Vec<(i64, Row)> for rows - same structure as RowVec but Arc-shareable
#[doc(hidden)]
pub type CteMaterializedRows = Arc<std::sync::OnceLock<CompactArc<Vec<Row>>>>;
#[doc(hidden)]
pub type CteData = (
    CompactArc<Vec<String>>,
    CompactArc<Vec<(i64, Row)>>,
    CteMaterializedRows,
);

/// Type alias for CTE data map to reduce type complexity
/// Uses CompactArc<Vec<String>> for columns and CompactArc<Vec<(i64, Row)>> for rows
/// to enable zero-copy sharing of CTE results with joins
#[doc(hidden)]
pub type CteDataMap = StringMap<CteData>;

#[derive(Default)]
struct JoinHashStateCache {
    states: Vec<JoinHashState>,
}

impl std::fmt::Debug for JoinHashStateCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinHashStateCache")
            .field("states", &self.states.len())
            .finish()
    }
}

impl JoinHashStateCache {
    fn get(
        &mut self,
        build_rows: &CompactArc<Vec<Row>>,
        key_indices: &[usize],
    ) -> Option<JoinHashState> {
        let index = self
            .states
            .iter()
            .position(|state| state.matches(build_rows, key_indices))?;
        let state = self.states.remove(index);
        let result = state.clone();
        self.states.push(state);
        Some(result)
    }

    fn insert(&mut self, state: JoinHashState) {
        self.states.push(state);
    }

    fn pop_lru(&mut self) -> Option<JoinHashState> {
        (!self.states.is_empty()).then(|| self.states.remove(0))
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContext {
    /// Create a new empty execution context
    /// Uses static defaults for empty collections to avoid allocations
    pub fn new() -> Self {
        let mut system_values = FxHashMap::default();
        system_values.insert(
            "CURRENT_PRINCIPAL".to_owned(),
            Value::uuid(ObjectId::BOOTSTRAP_OWNER.into_bytes()),
        );
        system_values.insert(
            "CURRENT_EFFECTIVE_PRINCIPAL".to_owned(),
            Value::uuid(ObjectId::BOOTSTRAP_OWNER.into_bytes()),
        );
        system_values.insert(
            "CURRENT_STATEMENT_TIMESTAMP".to_owned(),
            Value::timestamp(system_time_now().into()),
        );
        Self {
            session: SessionState {
                auto_commit: true,
                current_database: EMPTY_DATABASE.clone(),
                session_vars: EMPTY_SESSION_VARS.clone(),
                principal_id: ObjectId::BOOTSTRAP_OWNER,
            },
            query: QueryState {
                effective_principal_id: None,
                params: EMPTY_PARAMS.clone(),
                named_params: Arc::new(system_values),
                cancelled: Arc::new(AtomicBool::new(false)),
                timed_out: Arc::new(AtomicBool::new(false)),
                active_reference_expands: Arc::new(AtomicUsize::new(0)),
                parent_cancelled: None,
                timeout_ms: 0,
                join_hash_state_max_bytes: crate::hash_table::DEFAULT_JOIN_HASH_STATE_MAX_BYTES,
                join_hash_states: Arc::new(Mutex::new(JoinHashStateCache::default())),
                join_memory_owner: Arc::new(JoinMemoryOwner::default()),
                view_depth: 0,
                query_depth: 0,
                outer_row: None,
                outer_columns: None,
                cte_data: None,
                transaction_id: None,
                stored_function_invoker: None,
                procedural_budget: None,
                public_scan_budget: None,
            },
        }
    }

    #[doc(hidden)]
    pub fn enter_statement_scope(&self) -> StatementCancellationScope {
        ACTIVE_QUERY_CANCELLATION.with(|stack| {
            stack.borrow_mut().push(self.cancellation_handle());
        });
        StatementCancellationScope
    }

    #[doc(hidden)]
    pub fn enter_reference_expand(&self) -> ReferenceExpandExecutionGuard {
        self.query
            .active_reference_expands
            .fetch_add(1, Ordering::AcqRel);
        ReferenceExpandExecutionGuard {
            active: Arc::clone(&self.query.active_reference_expands),
        }
    }

    #[doc(hidden)]
    pub fn active_reference_expands(&self) -> usize {
        self.query.active_reference_expands.load(Ordering::Acquire)
    }

    /// Create an execution context with positional parameters
    pub fn with_params(params: ParamVec) -> Self {
        let mut context = Self::new();
        context.query.params = CompactArc::new(params);
        context
    }

    /// Create an execution context with named parameters
    pub fn with_named_params(named_params: FxHashMap<String, Value>) -> Self {
        let mut context = Self::new();
        let system_values = context.query.named_params.clone();
        let mut admitted = named_params;
        admitted.retain(|name, _| !is_system_context_name(name));
        admitted.extend(
            system_values
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        context.query.named_params = Arc::new(admitted);
        context
    }

    /// Get a positional parameter by index (1-based)
    pub fn get_param(&self, index: usize) -> Option<&Value> {
        if index == 0 || index > self.query.params.len() {
            None
        } else {
            self.query.params.get(index - 1)
        }
    }

    /// Get a named parameter by name
    pub fn get_named_param(&self, name: &str) -> Option<&Value> {
        self.query.named_params.get(name)
    }

    #[inline]
    #[doc(hidden)]
    pub fn join_hash_state_max_bytes(&self) -> usize {
        self.query.join_hash_state_max_bytes
    }

    /// Reserve one blocking JOIN owner against the common request budget.
    /// Cached states are evicted LRU before admission fails. Eviction cannot
    /// free a state still used by another physical edge; its shared reservation
    /// remains charged until the final clone is dropped.
    #[doc(hidden)]
    pub fn reserve_join_memory(&self, bytes: usize) -> Option<JoinMemoryReservation> {
        loop {
            if let Some(reservation) = self
                .query
                .join_memory_owner
                .try_reserve(bytes, self.query.join_hash_state_max_bytes)
            {
                return Some(reservation);
            }

            let evicted = self
                .query
                .join_hash_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_lru();
            let evicted = evicted?;
            // Drop outside the cache lock: the last state clone releases its
            // reservation through the same shared memory owner.
            drop(evicted);
        }
    }

    #[doc(hidden)]
    pub fn retained_join_memory_bytes(&self) -> usize {
        self.query.join_memory_owner.retained_bytes()
    }

    #[doc(hidden)]
    pub fn peak_join_memory_bytes(&self) -> usize {
        self.query.join_memory_owner.peak_bytes()
    }

    /// Return one exact request-local hash state, building it once when the
    /// immutable row batch has another owner (CTE/semantic relation cache).
    /// Sole-owned transient batches are deliberately not retained here.
    #[doc(hidden)]
    pub fn join_hash_state_for(
        &self,
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        retain_for_reuse: bool,
    ) -> Option<JoinHashState> {
        if key_indices.is_empty() {
            return None;
        }
        if retain_for_reuse {
            let mut cache = self
                .query
                .join_hash_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(state) = cache.get(&build_rows, key_indices) {
                radixdb_storage::instrumentation::record_join_hash_state_reuse();
                return Some(state);
            }
        }

        let state_bytes = JoinHashTable::estimated_retained_bytes(build_rows.len())?;
        let reservation = self.reserve_join_memory(state_bytes)?;
        let state = JoinHashState::build_reserved(build_rows, key_indices, reservation);
        radixdb_storage::instrumentation::record_join_hash_state_build();
        if retain_for_reuse {
            let mut cache = self
                .query
                .join_hash_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = cache.get(state.build_rows(), key_indices) {
                radixdb_storage::instrumentation::record_join_hash_state_reuse();
                return Some(existing);
            }
            cache.insert(state.clone());
        }
        Some(state)
    }

    /// Single-pass hash+bloom construction under the same common request
    /// reservation used by ordinary hash states.
    #[doc(hidden)]
    pub fn join_hash_state_with_bloom_for(
        &self,
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        bloom_builder: &mut impl crate::hash_table::JoinHashObserver,
    ) -> Option<JoinHashState> {
        if key_indices.is_empty() {
            return None;
        }
        let state_bytes = JoinHashTable::estimated_retained_bytes(build_rows.len())?
            .checked_add(bloom_builder.retained_bytes())?;
        let reservation = self.reserve_join_memory(state_bytes)?;
        let state = JoinHashState::build_with_bloom_reserved(
            build_rows,
            key_indices,
            bloom_builder,
            reservation,
        );
        radixdb_storage::instrumentation::record_join_hash_state_build();
        Some(state)
    }

    /// Get all positional parameters
    pub fn params(&self) -> &[Value] {
        &self.query.params
    }

    /// Get the params Arc for zero-copy sharing.
    /// Used by evaluator bridge to avoid cloning params.
    pub fn params_arc(&self) -> &CompactArc<ParamVec> {
        &self.query.params
    }

    /// Get all named parameters
    pub fn named_params(&self) -> &FxHashMap<String, Value> {
        &self.query.named_params
    }

    /// Get the named_params Arc for zero-copy sharing.
    /// Used by evaluator bridge to avoid cloning params.
    pub fn named_params_arc(&self) -> &Arc<FxHashMap<String, Value>> {
        &self.query.named_params
    }

    #[doc(hidden)]
    pub(crate) fn stored_function_invoker(&self) -> Option<&Arc<dyn StoredFunctionInvoker>> {
        self.query.stored_function_invoker.as_ref()
    }

    #[doc(hidden)]
    pub(crate) fn with_stored_function_invoker(
        mut self,
        invoker: Arc<dyn StoredFunctionInvoker>,
    ) -> Self {
        self.query.stored_function_invoker = Some(invoker);
        self
    }

    #[doc(hidden)]
    pub(crate) fn procedural_budget(&self) -> Option<&BudgetOwner> {
        self.query.procedural_budget.as_ref()
    }

    #[doc(hidden)]
    pub(crate) fn with_procedural_budget(mut self, budget: BudgetOwner) -> Self {
        self.query.procedural_budget = Some(budget);
        self
    }

    /// Get the number of positional parameters
    pub fn param_count(&self) -> usize {
        self.query.params.len()
    }

    /// Set positional parameters
    pub fn set_params(&mut self, params: ParamVec) {
        self.query.params = CompactArc::new(params);
    }

    /// Add a positional parameter
    pub fn add_param(&mut self, value: Value) {
        CompactArc::make_mut(&mut self.query.params).push(value);
    }

    /// Set a named parameter
    pub fn set_named_param(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        if !is_system_context_name(&name) {
            Arc::make_mut(&mut self.query.named_params).insert(name, value);
        }
    }

    /// Check if auto-commit is enabled
    pub fn auto_commit(&self) -> bool {
        self.session.auto_commit
    }

    /// Set auto-commit mode
    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.session.auto_commit = auto_commit;
    }

    /// Check if the query has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.query.cancelled.load(Ordering::Relaxed)
            || self
                .query
                .parent_cancelled
                .as_ref()
                .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
    }

    #[doc(hidden)]
    pub fn did_time_out(&self) -> bool {
        self.query.timed_out.load(Ordering::Acquire)
    }

    /// Cancel the query
    pub fn cancel(&self) {
        self.query.cancelled.store(true, Ordering::Relaxed);
    }

    /// Get a cancellation handle that can be used from another thread
    pub fn cancellation_handle(&self) -> CancellationHandle {
        CancellationHandle {
            cancelled: self.query.cancelled.clone(),
            parent_cancelled: self.query.parent_cancelled.clone(),
        }
    }

    /// Keep request-local cancellation independent while also inheriting a
    /// longer-lived session/server shutdown signal.
    #[doc(hidden)]
    pub fn bind_parent_cancellation(&mut self, handle: &CancellationHandle) {
        self.query.parent_cancelled = Some(Arc::clone(&handle.cancelled));
    }

    /// Get the current database/schema name
    pub fn current_database(&self) -> Option<&str> {
        self.session.current_database.as_ref().as_deref()
    }

    /// Set the current database/schema name
    pub fn set_current_database(&mut self, database: impl Into<String>) {
        self.session.current_database = Arc::new(Some(database.into()));
    }

    /// Return the stable catalog Principal selected by authentication.
    pub const fn principal_id(&self) -> ObjectId {
        self.session.principal_id
    }

    /// Derive a context for a stable Principal without carrying credentials.
    pub fn with_principal_id(&self, principal_id: ObjectId) -> Self {
        let mut nested = self.clone();
        nested.session.principal_id = principal_id;
        nested.query.effective_principal_id = None;
        nested
            .set_system_context_value("CURRENT_PRINCIPAL", Value::uuid(principal_id.into_bytes()));
        nested.set_system_context_value(
            "CURRENT_EFFECTIVE_PRINCIPAL",
            Value::uuid(principal_id.into_bytes()),
        );
        nested
    }

    /// Return the Principal whose privileges apply to the current execution
    /// frame. It differs from `principal_id` only inside SECURITY DEFINER.
    pub const fn effective_principal_id(&self) -> ObjectId {
        match self.query.effective_principal_id {
            Some(principal) => principal,
            None => self.session.principal_id,
        }
    }

    /// Derive a nested execution frame with an explicit effective Principal.
    /// Authentication identity remains unchanged.
    pub fn with_effective_principal_id(&self, principal_id: ObjectId) -> Self {
        let mut nested = self.clone();
        nested.query.effective_principal_id = Some(principal_id);
        nested.set_system_context_value(
            "CURRENT_EFFECTIVE_PRINCIPAL",
            Value::uuid(principal_id.into_bytes()),
        );
        nested
    }

    /// Install one request-wide row-source budget. This is intentionally a
    /// hidden executor contract: trusted embedded SQL keeps its historical
    /// behavior, while public ORM reads fail closed before unbounded materialization.
    #[doc(hidden)]
    pub fn with_public_scan_limit(&self, max_rows: usize) -> Self {
        let mut nested = self.clone();
        nested.query.public_scan_budget = Some(Arc::new(PublicScanBudget::new(max_rows)));
        nested
    }

    #[doc(hidden)]
    pub fn claim_public_scan_rows(&self, rows: usize) -> Result<()> {
        self.query
            .public_scan_budget
            .as_ref()
            .map_or(Ok(()), |budget| budget.claim(rows))
    }

    #[doc(hidden)]
    pub fn has_public_scan_budget(&self) -> bool {
        self.query.public_scan_budget.is_some()
    }

    /// Get a session variable
    pub fn get_session_var(&self, name: &str) -> Option<&Value> {
        self.session.session_vars.get(name)
    }

    /// Set a session variable
    pub fn set_session_var(&mut self, name: impl Into<String>, value: Value) {
        Arc::make_mut(&mut self.session.session_vars).insert(name.into(), value);
    }

    /// Get the query timeout in milliseconds
    pub fn timeout_ms(&self) -> u64 {
        self.query.timeout_ms
    }

    /// Set the query timeout in milliseconds
    pub fn set_timeout_ms(&mut self, timeout_ms: u64) {
        self.query.timeout_ms = timeout_ms;
    }

    /// Check if a timeout has been set
    pub fn has_timeout(&self) -> bool {
        self.query.timeout_ms > 0
    }

    /// Get the current view nesting depth
    pub fn view_depth(&self) -> usize {
        self.query.view_depth
    }

    /// Create a new context with incremented view depth.
    /// Used when executing nested views to track recursion depth.
    /// Also increments query_depth since views are nested queries.
    pub fn with_incremented_view_depth(&self) -> Self {
        let mut nested = self.clone();
        nested.query.view_depth += 1;
        nested.query.query_depth += 1;
        nested
    }

    /// Create a new context with incremented query depth.
    /// Used when executing subqueries to ensure TimeoutGuard is only created at the top level.
    pub fn with_incremented_query_depth(&self) -> Self {
        let mut nested = self.clone();
        nested.query.query_depth += 1;
        nested
    }

    /// Get the outer row context for correlated subqueries
    pub fn outer_row(&self) -> Option<&FxHashMap<CompactArc<str>, Value>> {
        self.query.outer_row.as_ref()
    }

    /// Recover the query-local correlated row for allocation reuse.
    #[doc(hidden)]
    pub fn take_outer_row(&mut self) -> Option<FxHashMap<CompactArc<str>, Value>> {
        self.query.outer_row.take()
    }

    /// Current nested-query depth. Zero denotes the statement boundary.
    #[doc(hidden)]
    pub fn query_depth(&self) -> usize {
        self.query.query_depth
    }

    /// Get the outer row columns for correlated subqueries
    pub fn outer_columns(&self) -> Option<&[String]> {
        self.query.outer_columns.as_ref().map(|v| v.as_slice())
    }

    /// Create a new context with outer row context for correlated subqueries.
    /// The outer row maps lowercase column names (as `CompactArc<str>`) to their values.
    /// NOTE: This is now cheap to clone due to Arc wrapping of immutable fields.
    pub fn with_outer_row(
        &self,
        outer_row: FxHashMap<CompactArc<str>, Value>,
        outer_columns: CompactArc<Vec<String>>,
    ) -> Self {
        let mut nested = self.with_incremented_query_depth();
        nested.query.outer_row = Some(outer_row);
        nested.query.outer_columns = Some(outer_columns);
        nested
    }

    /// Get CTE data by name (case-insensitive)
    /// Returns Arc references to enable zero-copy sharing with joins
    pub fn get_cte(&self, name: &str) -> Option<&CteData> {
        self.query
            .cte_data
            .as_ref()
            .and_then(|data| data.get(&name.to_lowercase()))
    }

    /// Get CTE data by name that is already lowercase.
    /// Use this when the name is known to be lowercase (e.g., from value_lower fields)
    /// to avoid redundant to_lowercase() allocation.
    #[inline]
    pub fn get_cte_by_lower(&self, name_lower: &str) -> Option<&CteData> {
        self.query
            .cte_data
            .as_ref()
            .and_then(|data| data.get(name_lower))
    }

    /// Return the immutable row-only CTE relation shared by every unfiltered
    /// JOIN reference. Conversion from `(row_id, Row)` happens once per CTE.
    #[doc(hidden)]
    pub fn get_cte_materialized_rows_by_lower(
        &self,
        name_lower: &str,
    ) -> Option<CompactArc<Vec<Row>>> {
        let (_, rows_with_ids, materialized) = self.get_cte_by_lower(name_lower)?;
        Some(
            materialized
                .get_or_init(|| {
                    CompactArc::new(rows_with_ids.iter().map(|(_, row)| row.clone()).collect())
                })
                .clone(),
        )
    }

    /// Check if context has CTE data
    pub fn has_cte(&self, name: &str) -> bool {
        self.query
            .cte_data
            .as_ref()
            .is_some_and(|data| data.contains_key(&name.to_lowercase()))
    }

    /// Check if context has CTE data by name that is already lowercase.
    /// Use this when the name is known to be lowercase to avoid allocation.
    #[inline]
    pub fn has_cte_by_lower(&self, name_lower: &str) -> bool {
        self.query
            .cte_data
            .as_ref()
            .is_some_and(|data| data.contains_key(name_lower))
    }

    /// Create a new context with CTE data for subqueries to reference
    /// Takes an Arc to avoid cloning large CTE datasets
    pub fn with_cte_data(&self, cte_data: Arc<CteDataMap>) -> Self {
        let mut nested = self.clone();
        nested.query.cte_data = Some(cte_data);
        nested
    }

    /// Get the current transaction ID
    pub fn transaction_id(&self) -> Option<u64> {
        self.query.transaction_id
    }

    /// Set the transaction ID
    pub fn set_transaction_id(&mut self, txn_id: u64) {
        self.query.transaction_id = Some(txn_id);
        if let Ok(txn_id) = i64::try_from(txn_id) {
            self.set_system_context_value("CURRENT_TRANSACTION_ID", Value::Integer(txn_id));
        }
    }

    /// Create a new context with a transaction ID
    pub fn with_transaction_id(&self, txn_id: u64) -> Self {
        let mut nested = self.clone();
        nested.set_transaction_id(txn_id);
        nested
    }

    /// Bind immutable metadata supplied by the network request boundary.
    #[doc(hidden)]
    pub fn set_request_id(&mut self, request_id: u64) -> Result<()> {
        let request_id = i64::try_from(request_id).map_err(|_| {
            radixdb_core::Error::invalid_argument("request ID exceeds the SQL INTEGER domain")
        })?;
        self.set_system_context_value("CURRENT_REQUEST_ID", Value::Integer(request_id));
        Ok(())
    }

    /// Bind immutable metadata supplied by the durable Job attempt boundary.
    #[doc(hidden)]
    pub fn set_job_context(
        &mut self,
        idempotency_key: &str,
        job_id: ObjectId,
        attempt: u32,
        scheduled_at: DateTime<Utc>,
    ) {
        self.set_system_context_value("CURRENT_IDEMPOTENCY_KEY", Value::text(idempotency_key));
        self.set_system_context_value("CURRENT_JOB_ID", Value::uuid(job_id.into_bytes()));
        self.set_system_context_value("CURRENT_JOB_ATTEMPT", Value::Integer(i64::from(attempt)));
        self.set_system_context_value("CURRENT_JOB_SCHEDULED_AT", Value::Timestamp(scheduled_at));
    }

    fn set_system_context_value(&mut self, name: &str, value: Value) {
        Arc::make_mut(&mut self.query.named_params).insert(name.to_owned(), value);
    }

    /// Check for cancellation and return an error if cancelled
    pub fn check_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(radixdb_core::Error::QueryCancelled)
        } else {
            Ok(())
        }
    }
}

#[doc(hidden)]
pub struct ReferenceExpandExecutionGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ReferenceExpandExecutionGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Handle for cancelling a query from another thread
#[derive(Debug, Clone)]
pub struct CancellationHandle {
    cancelled: Arc<AtomicBool>,
    parent_cancelled: Option<Arc<AtomicBool>>,
}

impl CancellationHandle {
    /// Cancel the query
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if the query has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
            || self
                .parent_cancelled
                .as_ref()
                .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
    }
}

impl radixdb_functions::FunctionCancellation for CancellationHandle {
    #[inline]
    fn is_cancelled(&self) -> bool {
        Self::is_cancelled(self)
    }
}

// ============================================================================
// Global Timeout Manager
// ============================================================================
//
// Uses a single background thread to manage all query timeouts efficiently.
// This avoids spawning a new thread for each query with a timeout.

/// Entry in the timeout priority queue
struct TimeoutEntry {
    /// When the timeout expires
    deadline: Instant,
    /// Unique ID for this timeout (for cancellation)
    id: u64,
    /// Handle to cancel the query
    cancel_handle: CancellationHandle,
    /// Whether this timeout has been cancelled (query completed)
    cancelled: Arc<AtomicBool>,
    /// Request-local marker used to distinguish timeout from manual cancel.
    timed_out: Arc<AtomicBool>,
}

impl PartialEq for TimeoutEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

impl Eq for TimeoutEntry {}

impl PartialOrd for TimeoutEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeoutEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap becomes a min-heap (earliest deadline first)
        other.deadline.cmp(&self.deadline)
    }
}

/// Global timeout manager state
struct TimeoutManagerState {
    /// Priority queue of pending timeouts (min-heap by deadline)
    timeouts: BinaryHeap<TimeoutEntry>,
}

/// Global timeout manager that handles all query timeouts in a single thread
struct TimeoutManager {
    /// Shared state protected by mutex
    state: Mutex<TimeoutManagerState>,
    /// Condition variable to wake the timer thread
    condvar: Condvar,
    /// Counter for generating unique timeout IDs
    next_id: AtomicU64,
}

impl TimeoutManager {
    /// Create a new timeout manager and spawn its background thread
    fn new() -> Arc<Self> {
        let manager = Arc::new(Self {
            state: Mutex::new(TimeoutManagerState {
                timeouts: BinaryHeap::new(),
            }),
            condvar: Condvar::new(),
            next_id: AtomicU64::new(1),
        });

        // Spawn the background timer thread
        let manager_clone = Arc::clone(&manager);
        std::thread::Builder::new()
            .name("radixdb-timeout-manager".to_string())
            .spawn(move || {
                manager_clone.run();
            })
            .expect("Failed to spawn timeout manager thread");

        manager
    }

    /// Background thread loop
    fn run(&self) {
        loop {
            let mut state = self.state.lock().unwrap();

            // Process expired timeouts
            let now = Instant::now();
            while let Some(entry) = state.timeouts.peek() {
                if entry.deadline <= now {
                    let entry = state.timeouts.pop().unwrap();
                    // Only cancel if the timeout wasn't already cancelled
                    if !entry.cancelled.load(Ordering::Relaxed) {
                        entry.timed_out.store(true, Ordering::Release);
                        entry.cancel_handle.cancel();
                    }
                } else {
                    break;
                }
            }

            // Calculate wait time until next timeout
            let wait_duration = if let Some(entry) = state.timeouts.peek() {
                entry.deadline.saturating_duration_since(now)
            } else {
                // No timeouts pending, wait indefinitely for new work
                Duration::from_secs(3600) // 1 hour max wait
            };

            // Wait for new work or timeout
            if wait_duration.is_zero() {
                continue; // Immediately process
            }
            let (_state, _timeout_result) =
                self.condvar.wait_timeout(state, wait_duration).unwrap();
        }
    }

    /// Register a new timeout, returns the timeout ID
    fn register(
        &self,
        timeout_ms: u64,
        cancel_handle: CancellationHandle,
        cancelled: Arc<AtomicBool>,
        timed_out: Arc<AtomicBool>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);

        let entry = TimeoutEntry {
            deadline,
            id,
            cancel_handle,
            cancelled,
            timed_out,
        };

        let mut state = self.state.lock().unwrap();
        let was_empty = state.timeouts.is_empty();
        let is_earliest = state.timeouts.peek().is_none_or(|e| deadline < e.deadline);

        state.timeouts.push(entry);

        // Wake the timer thread if this is the new earliest deadline
        if was_empty || is_earliest {
            self.condvar.notify_one();
        }

        id
    }

    fn unregister(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        let removed_earliest = state.timeouts.peek().is_some_and(|entry| entry.id == id);
        state.timeouts.retain(|entry| entry.id != id);
        drop(state);
        if removed_earliest {
            self.condvar.notify_one();
        }
    }
}

/// Get or create the global timeout manager
fn global_timeout_manager() -> &'static Arc<TimeoutManager> {
    use std::sync::OnceLock;
    static MANAGER: OnceLock<Arc<TimeoutManager>> = OnceLock::new();
    MANAGER.get_or_init(TimeoutManager::new)
}

#[doc(hidden)]
pub fn pending_timeout_count_for(ctx: &ExecutionContext) -> usize {
    global_timeout_manager()
        .state
        .lock()
        .unwrap()
        .timeouts
        .iter()
        .filter(|entry| Arc::ptr_eq(&entry.timed_out, &ctx.query.timed_out))
        .count()
}

/// Guard that automatically cancels a query after a timeout.
/// Uses a global timeout manager for efficient handling of many concurrent timeouts.
pub struct TimeoutGuard {
    registration_id: u64,
    /// Flag to signal that the query completed (timeout should be ignored)
    cancelled: Arc<AtomicBool>,
}

impl TimeoutGuard {
    /// Create a new timeout guard that will cancel the query after timeout_ms.
    /// Returns None if timeout_ms is 0 (no timeout).
    pub fn new(ctx: &ExecutionContext) -> Option<Self> {
        let timeout_ms = ctx.timeout_ms();
        if timeout_ms == 0 {
            return None;
        }

        let cancel_handle = ctx.cancellation_handle();
        let cancelled = Arc::new(AtomicBool::new(false));

        // Register with the global timeout manager
        let registration_id = global_timeout_manager().register(
            timeout_ms,
            cancel_handle,
            Arc::clone(&cancelled),
            Arc::clone(&ctx.query.timed_out),
        );

        Some(Self {
            registration_id,
            cancelled,
        })
    }
}

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        // Mark this timeout as cancelled so the manager ignores it
        self.cancelled.store(true, Ordering::Relaxed);
        global_timeout_manager().unregister(self.registration_id);
    }
}

/// Builder for ExecutionContext
pub struct ExecutionContextBuilder {
    ctx: ExecutionContext,
}

impl ExecutionContextBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            ctx: ExecutionContext::new(),
        }
    }

    /// Add positional parameters
    pub fn params(mut self, params: ParamVec) -> Self {
        self.ctx.query.params = CompactArc::new(params);
        self
    }

    /// Add a positional parameter
    pub fn param(mut self, value: Value) -> Self {
        let mut v = (*self.ctx.query.params).clone();
        v.push(value);
        self.ctx.query.params = CompactArc::new(v);
        self
    }

    /// Add a named parameter
    pub fn named_param(mut self, name: impl Into<String>, value: Value) -> Self {
        self.ctx.set_named_param(name, value);
        self
    }

    /// Set auto-commit mode
    pub fn auto_commit(mut self, auto_commit: bool) -> Self {
        self.ctx.session.auto_commit = auto_commit;
        self
    }

    /// Set the current database
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.ctx.session.current_database = Arc::new(Some(database.into()));
        self
    }

    /// Set the stable Principal selected by the authentication boundary.
    pub fn principal_id(mut self, principal_id: ObjectId) -> Self {
        self.ctx = self.ctx.with_principal_id(principal_id);
        self
    }

    /// Set a session variable
    pub fn session_var(mut self, name: impl Into<String>, value: Value) -> Self {
        let mut variables = (*self.ctx.session.session_vars).clone();
        variables.insert(name.into(), value);
        self.ctx.session.session_vars = Arc::new(variables);
        self
    }

    /// Set the query timeout
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.ctx.query.timeout_ms = timeout_ms;
        self
    }

    /// Override the per-operator hash-state admission boundary for deterministic
    /// gates. JR-09 will wire the common query budget to public configuration.
    #[doc(hidden)]
    pub fn join_hash_state_max_bytes(mut self, max_bytes: usize) -> Self {
        self.ctx.query.join_hash_state_max_bytes = max_bytes;
        self
    }

    /// Build the execution context
    pub fn build(self) -> ExecutionContext {
        self.ctx
    }
}

impl Default for ExecutionContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustc_hash::FxHashMap;

    struct TestHashObserver {
        retained_bytes: usize,
        observed: usize,
    }

    impl TestHashObserver {
        fn new(retained_bytes: usize) -> Self {
            Self {
                retained_bytes,
                observed: 0,
            }
        }
    }

    impl crate::hash_table::JoinHashObserver for TestHashObserver {
        fn insert_raw_hash(&mut self, _hash: u64) {
            self.observed += 1;
        }

        fn retained_bytes(&self) -> usize {
            self.retained_bytes
        }
    }

    #[test]
    fn test_context_new() {
        let ctx = ExecutionContext::new();
        assert_eq!(ctx.param_count(), 0);
        assert!(ctx.auto_commit());
        assert!(!ctx.is_cancelled());
    }

    #[test]
    fn test_context_with_params() {
        let ctx = ExecutionContext::with_params(smallvec::smallvec![
            Value::Integer(1),
            Value::text("hello")
        ]);
        assert_eq!(ctx.param_count(), 2);
        assert_eq!(ctx.get_param(1), Some(&Value::Integer(1)));
        assert_eq!(ctx.get_param(2), Some(&Value::text("hello")));
        assert_eq!(ctx.get_param(0), None); // 0 is invalid
        assert_eq!(ctx.get_param(3), None); // Out of bounds
    }

    #[test]
    fn test_context_named_params() {
        let mut params = FxHashMap::default();
        params.insert("name".to_string(), Value::text("Alice"));
        params.insert("age".to_string(), Value::Integer(30));

        let ctx = ExecutionContext::with_named_params(params);
        assert_eq!(ctx.get_named_param("name"), Some(&Value::text("Alice")));
        assert_eq!(ctx.get_named_param("age"), Some(&Value::Integer(30)));
        assert_eq!(ctx.get_named_param("unknown"), None);
    }

    #[test]
    fn test_context_cancellation() {
        let ctx = ExecutionContext::new();
        assert!(!ctx.is_cancelled());

        let handle = ctx.cancellation_handle();
        assert!(!handle.is_cancelled());

        handle.cancel();
        assert!(ctx.is_cancelled());
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_context_check_cancelled() {
        let ctx = ExecutionContext::new();
        assert!(ctx.check_cancelled().is_ok());

        ctx.cancel();
        assert!(ctx.check_cancelled().is_err());
    }

    #[test]
    fn test_context_session_vars() {
        let mut ctx = ExecutionContext::new();
        ctx.set_session_var("timezone", Value::text("UTC"));

        assert_eq!(ctx.get_session_var("timezone"), Some(&Value::text("UTC")));
        assert_eq!(ctx.get_session_var("unknown"), None);
    }

    #[test]
    fn test_context_builder() {
        let ctx = ExecutionContextBuilder::new()
            .params(smallvec::smallvec![Value::Integer(1)])
            .param(Value::Integer(2))
            .named_param("name", Value::text("test"))
            .auto_commit(false)
            .database("mydb")
            .timeout_ms(5000)
            .build();

        assert_eq!(ctx.param_count(), 2);
        assert_eq!(ctx.get_param(1), Some(&Value::Integer(1)));
        assert_eq!(ctx.get_param(2), Some(&Value::Integer(2)));
        assert_eq!(ctx.get_named_param("name"), Some(&Value::text("test")));
        assert!(!ctx.auto_commit());
        assert_eq!(ctx.current_database(), Some("mydb"));
        assert_eq!(ctx.timeout_ms(), 5000);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn request_local_join_hash_cache_evicts_to_its_byte_budget() {
        let retained = crate::hash_table::JoinHashTable::estimated_retained_bytes(1)
            .expect("one-row hash state size");
        let ctx = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(retained)
            .build();
        let first = CompactArc::new(vec![Row::from_values(vec![Value::Integer(1)])]);
        let second = CompactArc::new(vec![Row::from_values(vec![Value::Integer(2)])]);

        radixdb_storage::instrumentation::begin_join_execution_probe();
        drop(
            ctx.join_hash_state_for(CompactArc::clone(&first), &[0], true)
                .expect("first state"),
        );
        drop(
            ctx.join_hash_state_for(CompactArc::clone(&second), &[0], true)
                .expect("second state"),
        );
        drop(
            ctx.join_hash_state_for(CompactArc::clone(&second), &[0], true)
                .expect("second state reuse"),
        );
        drop(
            ctx.join_hash_state_for(CompactArc::clone(&first), &[0], true)
                .expect("first state rebuild after eviction"),
        );
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();

        assert_eq!(probe.hash_state_builds, 3);
        assert_eq!(probe.hash_state_reuses, 1);
    }

    #[test]
    fn active_and_cached_hash_states_share_one_request_budget() {
        let retained = JoinHashTable::estimated_retained_bytes(1).unwrap();
        let ctx = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(retained)
            .build();
        let cached_rows = CompactArc::new(vec![Row::from_values(vec![Value::Integer(1)])]);

        let active_cached = ctx
            .join_hash_state_for(CompactArc::clone(&cached_rows), &[0], true)
            .expect("first state fits common budget");
        assert_eq!(ctx.retained_join_memory_bytes(), retained);

        // Cache eviction cannot pretend that memory became free while another
        // physical consumer still owns a clone of the same state.
        let rejected = ctx.join_hash_state_for(
            CompactArc::new(vec![Row::from_values(vec![Value::Integer(2)])]),
            &[0],
            false,
        );
        assert!(rejected.is_none());
        assert_eq!(ctx.retained_join_memory_bytes(), retained);

        drop(active_cached);
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
        let admitted = ctx
            .join_hash_state_for(
                CompactArc::new(vec![Row::from_values(vec![Value::Integer(2)])]),
                &[0],
                false,
            )
            .expect("released request budget admits next transient state");
        assert_eq!(ctx.retained_join_memory_bytes(), retained);
        drop(admitted);
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn context_clones_cannot_each_spend_the_join_budget() {
        let retained = JoinHashTable::estimated_retained_bytes(1).unwrap();
        let ctx = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(retained)
            .build();
        let clone = ctx.clone();
        let first = ctx
            .join_hash_state_for(
                CompactArc::new(vec![Row::from_values(vec![Value::Integer(1)])]),
                &[0],
                false,
            )
            .unwrap();
        assert!(clone
            .join_hash_state_for(
                CompactArc::new(vec![Row::from_values(vec![Value::Integer(2)])]),
                &[0],
                false,
            )
            .is_none());
        drop(first);
        assert_eq!(clone.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn bloom_and_hash_share_the_same_join_memory_reservation() {
        let rows: CompactArc<Vec<Row>> = CompactArc::new(
            (0..100)
                .map(|value| Row::from_values(vec![Value::Integer(value)]))
                .collect(),
        );
        let mut builder = TestHashObserver::new(256);
        let hash_bytes = JoinHashTable::estimated_retained_bytes(rows.len()).unwrap();
        let bloom_bytes = crate::hash_table::JoinHashObserver::retained_bytes(&builder);
        assert!(bloom_bytes > 0);

        let too_small = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(hash_bytes)
            .build();
        assert!(too_small
            .join_hash_state_with_bloom_for(CompactArc::clone(&rows), &[0], &mut builder)
            .is_none());
        assert_eq!(too_small.retained_join_memory_bytes(), 0);

        let exact = ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(hash_bytes + bloom_bytes)
            .build();
        let state = exact
            .join_hash_state_with_bloom_for(rows, &[0], &mut builder)
            .expect("combined hash and bloom bytes fit exactly");
        assert_eq!(builder.observed, 100);
        assert_eq!(exact.retained_join_memory_bytes(), hash_bytes + bloom_bytes);
        drop(state);
        assert_eq!(exact.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn r6_l01_b_completed_timeout_is_unregistered_immediately() {
        let ctx = ExecutionContextBuilder::new().timeout_ms(60_000).build();
        let guard = TimeoutGuard::new(&ctx).expect("timeout guard");
        assert_eq!(pending_timeout_count_for(&ctx), 1);
        drop(guard);
        assert_eq!(pending_timeout_count_for(&ctx), 0);
        assert!(!ctx.is_cancelled());
    }
}
