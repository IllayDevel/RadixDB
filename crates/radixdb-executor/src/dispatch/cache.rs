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

//! Query Cache for parsed SQL statements
//!
//! This module provides a cache for previously parsed SQL queries,
//! storing the parse tree to avoid the overhead of parsing the same
//! query multiple times.
//!
//! # Example
//!
//! ```ignore
//! let cache = QueryCache::<()>::new(1000);
//!
//! // First query - will be parsed and cached
//! if let Some(plan) = cache.get("SELECT * FROM users") {
//!     // Use cached plan
//! } else {
//!     // Parse and cache
//!     let stmt = parse(sql);
//!     cache.put(sql, stmt, false, 0);
//! }
//!
//! // Second identical query - retrieved from cache
//! let plan = cache.get("SELECT * FROM users").unwrap();
//! ```

use radixdb_core::time_compat::Instant;
use std::borrow::Cow;
use std::sync::{Arc, RwLock};

use radixdb_core::SmartString;
use rustc_hash::FxHashMap;

use crate::context::ExecutionContext;
use radixdb_core::{Error, Result};
use radixdb_sql::ast::Statement;

pub use crate::compiled_plan::{
    CompiledCountDistinct, CompiledCountStar, CompiledExecution, CompiledInsert, CompiledPkDelete,
    CompiledPkLookup, CompiledPkUpdate, CompiledUpdateColumn, PkValueSource, UpdateValueSource,
};

/// Convert to lowercase without allocation if already lowercase.
#[inline]
fn to_lowercase_cow(s: &str) -> Cow<'_, str> {
    if s.bytes().all(|b| !b.is_ascii_uppercase()) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(s.to_lowercase())
    }
}

/// Exact parameter shape owned by one parsed statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParameterContract {
    positional_count: usize,
    named_params: Arc<Vec<SmartString>>,
}

impl ParameterContract {
    #[doc(hidden)]
    pub fn from_statement(statement: &Statement) -> Self {
        let mut positional_count = 0;
        let mut named_params = Vec::new();
        radixdb_sql::ast::walk_statement_tree(statement, &mut |expression| {
            if let radixdb_sql::ast::Expression::Parameter(parameter) = expression {
                if let Some(name) = parameter.name.strip_prefix(':') {
                    named_params.push(SmartString::new(name));
                } else {
                    positional_count = positional_count.max(parameter.index);
                }
            }
        });
        named_params.sort_unstable();
        named_params.dedup();
        Self {
            positional_count,
            named_params: Arc::new(named_params),
        }
    }

    #[doc(hidden)]
    pub fn from_statements(statements: &[Statement]) -> Self {
        let mut positional_count = 0;
        let mut named_params = Vec::new();
        for statement in statements {
            let contract = Self::from_statement(statement);
            positional_count = positional_count.max(contract.positional_count);
            named_params.extend(contract.named_params.iter().cloned());
        }
        named_params.sort_unstable();
        named_params.dedup();
        Self {
            positional_count,
            named_params: Arc::new(named_params),
        }
    }

    /// Number of positional values required by the statement.
    pub fn positional_count(&self) -> usize {
        self.positional_count
    }

    /// Exact set of named bindings required by the statement.
    pub fn named_params(&self) -> &[SmartString] {
        &self.named_params
    }

    pub fn has_params(&self) -> bool {
        self.positional_count != 0 || !self.named_params.is_empty()
    }

    #[doc(hidden)]
    pub fn validate(&self, context: &ExecutionContext) -> Result<()> {
        let provided_positional = context.params().len();
        if provided_positional != self.positional_count {
            return Err(Error::invalid_argument(format!(
                "statement requires exactly {} positional parameters, got {}",
                self.positional_count, provided_positional
            )));
        }

        let provided_named = context.named_params();
        let required_user_count = self
            .named_params
            .iter()
            .filter(|name| !crate::context::is_system_context_name(name.as_str()))
            .count();
        let provided_user_count = provided_named
            .keys()
            .filter(|name| !crate::context::is_system_context_name(name))
            .count();
        if provided_user_count != required_user_count
            || self
                .named_params
                .iter()
                .any(|name| !provided_named.contains_key(name.as_str()))
        {
            let required = self
                .named_params
                .iter()
                .filter(|name| !crate::context::is_system_context_name(name.as_str()))
                .map(SmartString::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::invalid_argument(format!(
                "statement requires exactly the named parameters [{required}]"
            )));
        }
        Ok(())
    }
}

/// Default cache size (number of cached plans)
pub const DEFAULT_CACHE_SIZE: usize = 1000;

/// Lightweight reference to a cached plan for query execution.
/// Contains only what's needed to execute: the immutable statement and param info.
#[derive(Debug, Clone)]
pub struct CachedPlanRef<B = ()> {
    /// The parsed AST (cheap Arc clone)
    #[doc(hidden)]
    pub statement: Arc<Statement>,
    /// Whether this query has parameter placeholders
    pub(crate) has_params: bool,
    /// Number of parameters required
    pub(crate) param_count: usize,
    /// Exact positional/named binding contract.
    #[doc(hidden)]
    pub parameter_contract: ParameterContract,
    /// Shared reference to compiled execution state (lazily populated)
    #[doc(hidden)]
    pub compiled: Arc<RwLock<CompiledExecution>>,
    /// Schema-bound logical reference graph, populated before physical paths.
    #[doc(hidden)]
    pub reference_expand: Arc<RwLock<B>>,
    owner_token: Arc<()>,
}

impl<B> CachedPlanRef<B> {
    /// Return the immutable parsed statement owned by this plan.
    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    /// Whether the statement requires positional or named bindings.
    pub fn has_params(&self) -> bool {
        self.has_params
    }

    /// Exact number of positional bindings required by the statement.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Read-only binding contract associated atomically with the plan.
    pub fn parameter_contract(&self) -> &ParameterContract {
        &self.parameter_contract
    }

    /// Shared compiled fast-path state owned by this parsed plan.
    #[doc(hidden)]
    pub fn compiled_state(&self) -> &Arc<RwLock<CompiledExecution>> {
        &self.compiled
    }

    /// Higher-layer schema binding cache associated with this parsed plan.
    #[doc(hidden)]
    pub fn binding_cache(&self) -> &Arc<RwLock<B>> {
        &self.reference_expand
    }
}

/// Represents a parsed and prepared statement stored in the cache
#[derive(Debug, Clone)]
pub struct CachedQueryPlan<B = ()> {
    /// The parsed AST (wrapped in Arc for cheap cloning - statements are immutable)
    pub statement: Arc<Statement>,
    /// Original query text
    pub query_text: SmartString,
    /// Last time this plan was used (monotonic)
    pub last_used: Instant,
    /// Number of times this plan has been used
    pub usage_count: u64,
    /// Whether this query has parameter placeholders
    pub has_params: bool,
    /// Number of parameters required
    pub param_count: usize,
    /// Exact positional/named binding contract.
    pub parameter_contract: ParameterContract,
    /// Normalized query text (cache key)
    pub normalized_query: SmartString,
    /// Compiled execution state (lazily populated on first execution)
    pub compiled: Arc<RwLock<CompiledExecution>>,
    /// Schema-bound logical reference graph (lazily populated).
    #[doc(hidden)]
    pub reference_expand: Arc<RwLock<B>>,
}

impl<B: Default> CachedQueryPlan<B> {
    /// Create a new cached query plan
    pub fn new(
        statement: Arc<Statement>,
        query_text: SmartString,
        _has_params: bool,
        _param_count: usize,
        normalized_query: SmartString,
    ) -> Self {
        let parameter_contract = ParameterContract::from_statement(&statement);
        let has_params = parameter_contract.has_params();
        let param_count = parameter_contract.positional_count();
        Self {
            statement,
            query_text,
            last_used: Instant::now(),
            usage_count: 1,
            has_params,
            param_count,
            parameter_contract,
            normalized_query,
            compiled: Arc::new(RwLock::new(CompiledExecution::Unknown)),
            reference_expand: Arc::new(RwLock::new(B::default())),
        }
    }
}

/// Query cache for parsed SQL statements
///
/// Provides thread-safe caching of parsed SQL queries to avoid
/// the overhead of parsing the same query multiple times.
pub struct QueryCache<B = ()> {
    /// Cached plans indexed by normalized query text (FxHash for fast string hashing)
    plans: RwLock<FxHashMap<SmartString, CachedQueryPlan<B>>>,
    /// Maximum number of cached plans
    max_size: usize,
    /// Factor to determine how many plans to prune when cache is full (0.0-1.0)
    prune_factor: f64,
    owner_token: Arc<()>,
}

impl<B: Default> QueryCache<B> {
    /// Create a new query cache with the given maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            plans: RwLock::new(FxHashMap::default()),
            max_size,
            prune_factor: 0.2, // Prune 20% of entries when cache is full
            owner_token: Arc::new(()),
        }
    }

    /// Create a new query cache with default size
    pub fn default_sized() -> Self {
        Self::new(DEFAULT_CACHE_SIZE)
    }

    /// Get a cached plan for a query if available
    ///
    /// Returns a cheap Arc clone of the cached statement and metadata.
    /// The Statement is immutable and shared via Arc.
    ///
    /// A hit updates recency and usage so eviction reflects actual access.
    pub fn get(&self, query: &str) -> Option<CachedPlanRef<B>> {
        let normalized = normalize_query(query);

        let mut plans = self.plans.write().ok()?;
        let plan = plans.get_mut(normalized.as_ref())?;
        plan.last_used = Instant::now();
        plan.usage_count = plan.usage_count.saturating_add(1);

        // Only clone the Arc (cheap) and copy the small fields
        Some(CachedPlanRef {
            statement: plan.statement.clone(),
            has_params: plan.has_params,
            param_count: plan.param_count,
            parameter_contract: plan.parameter_contract.clone(),
            compiled: plan.compiled.clone(), // Share compiled state
            reference_expand: plan.reference_expand.clone(),
            owner_token: Arc::clone(&self.owner_token),
        })
    }

    /// Add a plan to the cache
    ///
    /// Returns a lightweight reference to the cached plan (CachedPlanRef).
    /// This avoids cloning SmartStrings since callers only need the statement
    /// and compiled execution state.
    pub fn put(
        &self,
        query: &str,
        statement: Arc<Statement>,
        _has_params: bool,
        _param_count: usize,
    ) -> CachedPlanRef<B> {
        let parameter_contract = ParameterContract::from_statement(&statement);
        let has_params = parameter_contract.has_params();
        let param_count = parameter_contract.positional_count();
        let normalized = normalize_query(query);
        // Convert Cow to SmartString for storage
        let normalized_key: SmartString = match normalized {
            Cow::Borrowed(s) => SmartString::new(s),
            Cow::Owned(s) => SmartString::new(&s),
        };

        // Create the compiled state upfront - shared between stored plan and returned ref
        let compiled = Arc::new(RwLock::new(CompiledExecution::Unknown));
        let reference_expand = Arc::new(RwLock::new(B::default()));

        if self.max_size > 0 {
            if let Ok(mut plans) = self.plans.write() {
                // Check if we need to prune the cache
                if plans.len() >= self.max_size {
                    self.prune_cache(&mut plans);
                }

                // Insert plan into map - use normalized_key for both key and field
                // Only clone normalized_key for the map key; move it into the plan struct
                let key_for_insert = normalized_key.clone();
                plans.insert(
                    key_for_insert,
                    CachedQueryPlan {
                        statement: statement.clone(),
                        query_text: SmartString::new(query),
                        last_used: Instant::now(),
                        usage_count: 1,
                        has_params,
                        param_count,
                        parameter_contract: parameter_contract.clone(),
                        normalized_query: normalized_key, // moved, not cloned
                        compiled: compiled.clone(),       // Arc clone - cheap
                        reference_expand: reference_expand.clone(),
                    },
                );
            }
        }

        // Return lightweight reference - only Arc clones, no SmartString clones
        CachedPlanRef {
            statement,
            has_params,
            param_count,
            parameter_contract,
            compiled,
            reference_expand,
            owner_token: Arc::clone(&self.owner_token),
        }
    }

    #[doc(hidden)]
    pub fn owns(&self, plan: &CachedPlanRef<B>) -> bool {
        Arc::ptr_eq(&self.owner_token, &plan.owner_token)
    }

    /// Clear the cache
    pub fn clear(&self) {
        if let Ok(mut plans) = self.plans.write() {
            plans.clear();
        }
    }

    /// Invalidate all cached plans that reference a specific table
    /// Called after DDL operations (ALTER TABLE, DROP TABLE, etc.)
    pub fn invalidate_table(&self, table_name: &str) {
        let table_lower = to_lowercase_cow(table_name);
        if let Ok(mut plans) = self.plans.write() {
            // Remove plans that reference this table
            // Check both the compiled lookup table name and query text
            plans.retain(|_key, plan| {
                // Check if compiled lookup references this table
                if let Ok(compiled) = plan.compiled.read() {
                    match &*compiled {
                        CompiledExecution::PkLookup(lookup)
                            if lookup.table_name == *table_lower =>
                        {
                            return false; // Remove this plan
                        }
                        CompiledExecution::CountDistinct(cd) if cd.table_name == *table_lower => {
                            return false; // Remove this plan
                        }
                        CompiledExecution::CountStar(cs) if cs.table_name == *table_lower => {
                            return false; // Remove this plan
                        }
                        _ => {}
                    }
                }
                // Also check query text for table reference (simple heuristic)
                let query_lower = to_lowercase_cow(&plan.query_text);
                !query_lower.contains(&format!(" {} ", &*table_lower))
                    && !query_lower.contains(&format!(" {}\n", &*table_lower))
                    && !query_lower.contains(&format!(" {};", &*table_lower))
                    && !query_lower.contains(&format!("from {}", &*table_lower))
                    && !query_lower.contains(&format!("join {}", &*table_lower))
                    && !query_lower.contains(&format!("into {}", &*table_lower))
                    && !query_lower.contains(&format!("update {}", &*table_lower))
            });
        }
    }

    /// Get the number of plans in the cache
    pub fn size(&self) -> usize {
        self.plans.read().map(|p| p.len()).unwrap_or(0)
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let plans = match self.plans.read() {
            Ok(p) => p,
            Err(_) => {
                return CacheStats {
                    size: 0,
                    max_size: self.max_size,
                    total_usage: 0,
                    avg_usage: 0.0,
                }
            }
        };

        let size = plans.len();
        let total_usage: u64 = plans.values().map(|p| p.usage_count).sum();
        let avg_usage = if size > 0 {
            total_usage as f64 / size as f64
        } else {
            0.0
        };

        CacheStats {
            size,
            max_size: self.max_size,
            total_usage,
            avg_usage,
        }
    }

    /// Prune the least recently used entries when the cache is full
    fn prune_cache(&self, plans: &mut FxHashMap<SmartString, CachedQueryPlan<B>>) {
        // Calculate how many entries to remove
        let num_to_remove = ((self.max_size as f64) * self.prune_factor).ceil() as usize;
        let num_to_remove = num_to_remove.max(1);

        if plans.is_empty() {
            return;
        }

        // Build a list of references sorted by last used time and usage count
        // Use references to avoid cloning all keys
        let mut entries: Vec<(&SmartString, Instant, u64)> = plans
            .iter()
            .map(|(k, p)| (k, p.last_used, p.usage_count))
            .collect();

        // Sort by last used (oldest first), then by usage count (least used first)
        entries.sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)));

        // Collect only the keys to remove (clone only what we need)
        let keys_to_remove: Vec<SmartString> = entries
            .into_iter()
            .take(num_to_remove.min(plans.len()))
            .map(|(k, _, _)| k.clone())
            .collect();

        // Remove the oldest/least used entries
        for key in keys_to_remove {
            plans.remove(&key);
        }
    }
}

impl<B: Default> Default for QueryCache<B> {
    fn default() -> Self {
        Self::default_sized()
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of cached plans
    pub size: usize,
    /// Maximum cache size
    pub max_size: usize,
    /// Total usage count across all cached plans
    pub total_usage: u64,
    /// Average usage per cached plan
    pub avg_usage: f64,
}

/// Return the exact SQL source used as the cache identity.
///
/// SQL whitespace is not globally insignificant: it can occur inside string
/// literals, quoted identifiers and comments. A lexer-unaware normalizer can
/// therefore map different programs to the same cached AST. Exact source bytes
/// are the conservative, allocation-free identity.
#[inline]
fn normalize_query(query: &str) -> std::borrow::Cow<'_, str> {
    std::borrow::Cow::Borrowed(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::ast::{Expression, GroupByClause, SelectStatement, StarExpression};
    use radixdb_sql::token::{Position, Token, TokenType};

    fn dummy_token() -> Token {
        Token::new(TokenType::Keyword, "SELECT", Position::new(0, 1, 1))
    }

    fn star_token() -> Token {
        Token::new(TokenType::Operator, "*", Position::new(0, 1, 1))
    }

    fn create_test_statement() -> Arc<Statement> {
        Arc::new(Statement::Select(SelectStatement {
            token: dummy_token(),
            with: None,
            distinct: false,
            distinct_on: vec![],
            columns: vec![Expression::Star(StarExpression {
                token: star_token(),
            })],
            table_expr: None,
            where_clause: None,
            group_by: GroupByClause::default(),
            having: None,
            window_defs: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            set_operations: vec![],
        }))
    }

    #[test]
    fn test_cache_put_get() {
        let cache = QueryCache::<()>::new(100);
        let stmt = create_test_statement();

        // Put in cache
        cache.put("SELECT * FROM users", stmt.clone(), false, 0);
        assert_eq!(cache.size(), 1);

        // Get from cache
        let plan = cache.get("SELECT * FROM users");
        assert!(plan.is_some());

        let plan = plan.unwrap();
        assert!(!plan.has_params);
        assert_eq!(plan.param_count, 0);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::<()>::new(100);

        let plan = cache.get("SELECT * FROM users");
        assert!(plan.is_none());
    }

    #[test]
    fn test_cache_usage_count() {
        let cache = QueryCache::<()>::new(100);
        let stmt = create_test_statement();

        cache.put("SELECT * FROM users", stmt, false, 0);

        // Get multiple times and verify operational usage accounting.
        for _ in 0..5 {
            cache.get("SELECT * FROM users");
        }

        let stats = cache.stats();
        assert_eq!(stats.total_usage, 6);
    }

    #[test]
    fn r5_l03_cache_budgets_and_lru_follow_runtime_usage_query_plan() {
        let cache = QueryCache::<()>::new(2);
        let stmt = create_test_statement();
        cache.put("SELECT 'a'", stmt.clone(), false, 0);
        std::thread::sleep(std::time::Duration::from_millis(1));
        cache.put("SELECT 'b'", stmt.clone(), false, 0);
        assert!(cache.get("SELECT 'a'").is_some());
        std::thread::sleep(std::time::Duration::from_millis(1));
        cache.put("SELECT 'c'", stmt, false, 0);

        assert!(cache.get("SELECT 'a'").is_some(), "hot plan was evicted");
        assert!(cache.get("SELECT 'b'").is_none(), "cold plan survived");
        assert!(cache.get("SELECT 'c'").is_some());
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryCache::<()>::new(100);
        let stmt = create_test_statement();

        cache.put("SELECT * FROM users", stmt, false, 0);
        assert_eq!(cache.size(), 1);

        cache.clear();
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_cache_pruning() {
        let cache = QueryCache::<()>::new(5);
        let stmt = create_test_statement();

        // Fill the cache
        for i in 0..10 {
            let query = format!("SELECT * FROM table{}", i);
            cache.put(&query, stmt.clone(), false, 0);
        }

        // Cache should have pruned some entries
        assert!(cache.size() <= 5);
    }

    #[test]
    fn test_normalize_query() {
        assert_eq!(
            normalize_query("  SELECT  *  FROM  users  "),
            "  SELECT  *  FROM  users  "
        );
        assert_eq!(
            normalize_query("SELECT\n*\nFROM\nusers"),
            "SELECT\n*\nFROM\nusers"
        );
        assert_ne!(
            normalize_query("SELECT 'a  b'"),
            normalize_query("SELECT 'a b'")
        );
    }

    #[test]
    fn test_normalize_query_utf8() {
        // UTF-8 characters should be preserved in fast path (no normalization needed)
        assert_eq!(
            normalize_query("SELECT * FROM t WHERE name = '日本語'"),
            "SELECT * FROM t WHERE name = '日本語'"
        );

        // UTF-8 and its surrounding source bytes are preserved exactly.
        assert_eq!(
            normalize_query("SELECT  *  FROM t WHERE name = '日本語'"),
            "SELECT  *  FROM t WHERE name = '日本語'"
        );

        // Mixed ASCII and UTF-8 with tabs/newlines
        assert_eq!(
            normalize_query("SELECT\t*\tFROM t WHERE city = '東京' AND country = '中国'"),
            "SELECT\t*\tFROM t WHERE city = '東京' AND country = '中国'"
        );

        // Emoji should also be preserved
        assert_eq!(
            normalize_query("SELECT  *  FROM t WHERE emoji = '🎉'"),
            "SELECT  *  FROM t WHERE emoji = '🎉'"
        );
    }

    #[test]
    fn test_distinct_source_has_distinct_cache_key() {
        let cache = QueryCache::<()>::new(100);
        let stmt = create_test_statement();

        // Put with one formatting
        cache.put("SELECT * FROM users", stmt, false, 0);

        // Lexer-unaware whitespace folding is unsafe, so different source misses.
        let plan = cache.get("  SELECT  *  FROM  users  ");
        assert!(plan.is_none());
    }

    #[test]
    fn test_parameterized_query() {
        let cache = QueryCache::<()>::new(100);
        let stmt = Arc::new(
            radixdb_sql::parse_sql("SELECT * FROM users WHERE id = $1")
                .expect("parse parameterized statement")
                .into_iter()
                .next()
                .expect("one statement"),
        );

        cache.put("SELECT * FROM users WHERE id = $1", stmt, false, 0);

        let plan = cache.get("SELECT * FROM users WHERE id = $1").unwrap();
        assert!(plan.has_params);
        assert_eq!(plan.param_count, 1);
    }

    #[test]
    fn test_cache_stats() {
        let cache = QueryCache::<()>::new(100);
        let stmt = create_test_statement();

        cache.put("SELECT 1", stmt.clone(), false, 0);
        cache.put("SELECT 2", stmt.clone(), false, 0);

        // Access first query more.
        for _ in 0..5 {
            cache.get("SELECT 1");
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.max_size, 100);
        assert_eq!(stats.total_usage, 7);
    }

    #[test]
    fn v2_r5_zero_and_one_capacity_are_hard_bounds() {
        let stmt = create_test_statement();
        let disabled = QueryCache::<()>::new(0);
        disabled.put("SELECT 1", stmt.clone(), false, 0);
        assert_eq!(disabled.size(), 0);
        assert!(disabled.get("SELECT 1").is_none());

        let one = QueryCache::<()>::new(1);
        one.put("SELECT 1", stmt.clone(), false, 0);
        one.put("SELECT 2", stmt, false, 0);
        assert_eq!(one.size(), 1);
        assert!(one.get("SELECT 2").is_some());
    }

    #[test]
    fn test_cache_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(QueryCache::<()>::new(1000));
        let stmt = create_test_statement();

        // Pre-populate
        cache.put("SELECT * FROM users", stmt.clone(), false, 0);

        let mut handles = vec![];

        // Spawn multiple reader threads
        for _ in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    cache.get("SELECT * FROM users");
                }
            }));
        }

        // Spawn writer threads
        for i in 0..5 {
            let cache = Arc::clone(&cache);
            let stmt = stmt.clone();
            handles.push(thread::spawn(move || {
                for j in 0..20 {
                    let query = format!("SELECT * FROM table{}_{}", i, j);
                    cache.put(&query, stmt.clone(), false, 0);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should still be functional
        assert!(cache.get("SELECT * FROM users").is_some());
    }
}
