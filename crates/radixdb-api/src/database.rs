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

//! Database struct and operations
//!
//! Provides a modern, ergonomic Rust API for database operations.
//!
//! # Examples
//!
//! ```no_run
//! use radixdb_api::{Database, params};
//! # fn main() -> radixdb_core::Result<()> {
//!
//! let db = Database::open("memory://")?;
//!
//! // DDL - no params needed
//! db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)", ())?;
//!
//! // Insert with params - using tuple syntax
//! db.execute("INSERT INTO users VALUES ($1, $2, $3)", (1, "Alice", 30))?;
//!
//! // Insert with params! macro
//! db.execute("INSERT INTO users VALUES ($1, $2, $3)", params![2, "Bob", 25])?;
//!
//! // Query with iteration
//! for row in db.query("SELECT * FROM users WHERE age > $1", (20,))? {
//!     let row = row?;
//!     let name: String = row.get(1)?;
//!     println!("{}", name);
//! }
//!
//! // Query single value
//! let count: i64 = db.query_one("SELECT COUNT(*) FROM users", ())?;
//! # assert_eq!(count, 2);
//! # Ok(())
//! # }
//! ```

use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex, RwLock};

#[cfg(test)]
type DatabaseOpenTestHook = Arc<dyn Fn(&str) + Send + Sync>;

#[cfg(test)]
static DATABASE_OPEN_TEST_HOOK: std::sync::LazyLock<Mutex<Option<DatabaseOpenTestHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));
#[cfg(test)]
static DATABASE_OPEN_TEST_HOOK_OWNER: Mutex<()> = Mutex::new(());

#[cfg(test)]
struct DatabaseOpenTestHookGuard {
    _owner: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl DatabaseOpenTestHookGuard {
    fn install(hook: DatabaseOpenTestHook) -> Self {
        let owner = DATABASE_OPEN_TEST_HOOK_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *DATABASE_OPEN_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        Self { _owner: owner }
    }
}

#[cfg(test)]
impl Drop for DatabaseOpenTestHookGuard {
    fn drop(&mut self) {
        *DATABASE_OPEN_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
fn run_database_open_test_hook(dsn: &str) {
    let hook = DATABASE_OPEN_TEST_HOOK
        .lock()
        .expect("database open test hook lock")
        .clone();
    if let Some(hook) = hook {
        hook(dsn);
    }
}

use radixdb_core::{Error, IsolationLevel, Result};
use radixdb_executor::context::{
    clear_all_thread_local_caches, ExecutionContext, ExecutionContextBuilder,
};
use radixdb_executor::optimizer::FeedbackCache;
use radixdb_executor::query_cache::CachedPlanRef;
use radixdb_executor::semantic_cache::{SemanticCache, SemanticCacheStatsSnapshot};
use radixdb_executor::Executor;
use radixdb_executor::{DatabasePluginAdmission, PluginRegistry};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::Engine;
use radixdb_storage::{Config, SyncMode};

use super::params::{NamedParams, Params};
use super::rows::{FromRow, Rows};
use super::statement::Statement;
use super::transaction::Transaction;

pub use super::value::FromValue;

/// Storage scheme constants
pub const MEMORY_SCHEME: &str = "memory";
pub const FILE_SCHEME: &str = "file";

fn composed_mvcc_engine(config: Config, plugin_registry: Arc<PluginRegistry>) -> MVCCEngine {
    MVCCEngine::new_with_composition_binders(
        config,
        radixdb_executor::mutation::partial_index::bind_from_sql,
        radixdb_executor::mutation::row_validation::bind,
        radixdb_executor::mutation::view_binding::bind_from_sql,
        radixdb_executor::plugin_catalog_runtime_binder(plugin_registry),
    )
}

#[cfg(not(feature = "test-filedb"))]
fn composed_in_memory_engine() -> MVCCEngine {
    composed_mvcc_engine(Config::default(), Arc::clone(&DEFAULT_PLUGIN_REGISTRY))
}

#[derive(Clone)]
enum DatabaseRegistryEntry {
    Opening(Arc<DatabaseOpenSlot>),
    Ready(Arc<DatabaseOwner>),
}

struct DatabaseOpenSlot {
    outcome: Mutex<Option<Result<()>>>,
    changed: Condvar,
}

impl DatabaseOpenSlot {
    fn new() -> Self {
        Self {
            outcome: Mutex::new(None),
            changed: Condvar::new(),
        }
    }

    fn finish(&self, outcome: Result<()>) {
        let mut state = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *state = Some(outcome);
        self.changed.notify_all();
    }

    fn wait(&self) -> Result<()> {
        let mut state = self
            .outcome
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("database open slot".to_string()))?;
        while state.is_none() {
            state = self
                .changed
                .wait(state)
                .map_err(|_| Error::LockAcquisitionFailed("database open slot".to_string()))?;
        }
        state
            .as_ref()
            .expect("database open slot outcome was checked")
            .clone()
    }
}

/// Global database registry to ensure single instance per DSN while allowing
/// independent DSNs to recover concurrently.
static DATABASE_REGISTRY: std::sync::LazyLock<RwLock<FxHashMap<String, DatabaseRegistryEntry>>> =
    std::sync::LazyLock::new(|| RwLock::new(FxHashMap::default()));
static DEFAULT_PLUGIN_REGISTRY: std::sync::LazyLock<Arc<PluginRegistry>> =
    std::sync::LazyLock::new(|| Arc::new(PluginRegistry::empty()));

/// Registry-owned durable engine/file-lock lifetime. It intentionally has no
/// SQL executor or connection-local transaction state.
struct DatabaseOwner {
    engine: Arc<MVCCEngine>,
    semantic_cache: Arc<SemanticCache>,
    feedback_cache: Arc<FeedbackCache>,
    plugin_registry: Arc<PluginRegistry>,
    registry_key: String,
    dsn: String,
    requested_config: Config,
    /// Temp directory for test-filedb feature. Deleted on drop.
    #[cfg(feature = "test-filedb")]
    _temp_dir: Option<tempfile::TempDir>,
}

/// Connection-local database state. Multiple connections may share the owner
/// and engine, but never this Executor or its hidden SQL transaction.
pub(crate) struct DatabaseInner {
    engine: Arc<MVCCEngine>,
    executor: Mutex<Executor>,
    owner: Arc<DatabaseOwner>,
}

/// Type alias for Statement to use (avoids exposing DatabaseInner directly)
pub(crate) type DatabaseInnerHandle = DatabaseInner;

impl DatabaseInner {
    /// Build a transaction-local executor while retaining the engine-owner
    /// caches shared by every connection. Parsed plans and transaction state
    /// stay local, but committed DML must invalidate the same semantic and
    /// feedback caches that later readers consult.
    pub(crate) fn transaction_executor(&self) -> Executor {
        Executor::with_shared_runtime_caches_and_plugin_registry(
            Arc::clone(&self.engine),
            Arc::clone(&self.owner.semantic_cache),
            Arc::clone(&self.owner.feedback_cache),
            Arc::clone(&self.owner.plugin_registry),
        )
    }
}

impl Drop for DatabaseOwner {
    fn drop(&mut self) {
        if let Err(error) = self.engine.close_engine() {
            eprintln!("automatic database close failed: {error}");
        }
    }
}

impl Drop for DatabaseInner {
    fn drop(&mut self) {
        clear_all_thread_local_caches();
        // Statements and FFI objects retain this whole connection inner. Only
        // its final drop releases the owner's last connection reference.
        Database::try_unregister_owner(&self.owner);
    }
}

/// Database represents a RadixDB database connection.
///
/// This is the main entry point for using RadixDB. It wraps the storage engine
/// and executor, providing a simple API for executing SQL queries.
///
/// # Thread Safety
///
/// Database is thread-safe and can be shared across threads via cloning.
/// Each clone shares the same underlying storage engine.
///
/// # Examples
///
/// ```ignore
/// use radixdb::{Database, params};
///
/// // Open in-memory database
/// let db = Database::open("memory://")?;
///
/// // Create table
/// db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())?;
///
/// // Insert with parameters
/// db.execute("INSERT INTO users VALUES ($1, $2)", (1, "Alice"))?;
///
/// // Query
/// for row in db.query("SELECT * FROM users", ())? {
///     let row = row?;
///     println!("{}: {}", row.get::<i64>("id")?, row.get::<String>("name")?);
/// }
/// ```
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    /// Authenticate one durable catalog Principal and return its stable ID.
    #[doc(hidden)]
    pub fn authenticate_principal(&self, login: &str, password: &str) -> Result<crate::ObjectId> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.authenticate_principal(login, password)
    }

    /// Return the bounded package-binding admission result without decoding
    /// any external value or exposing storage internals.
    #[doc(hidden)]
    pub fn plugin_admission(&self) -> Result<DatabasePluginAdmission> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.plugin_admission()
    }

    #[doc(hidden)]
    pub fn plugin_admission_diagnostic(&self) -> Result<Option<String>> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.plugin_admission_diagnostic()
    }

    pub(crate) fn with_connection_executor<T>(
        &self,
        operation: impl FnOnce(&Executor) -> Result<T>,
    ) -> Result<T> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        operation(&executor)
    }

    fn canonical_registry_key(dsn: &str) -> Result<String> {
        let (scheme, path) = Self::parse_dsn(dsn)?;
        if scheme == MEMORY_SCHEME {
            return Ok(format!("{MEMORY_SCHEME}://{path}"));
        }

        let (clean_path, _) = Self::parse_file_config(&path)?;
        let absolute = if std::path::Path::new(&clean_path).is_absolute() {
            std::path::PathBuf::from(clean_path)
        } else {
            std::env::current_dir()
                .map_err(|error| Error::internal(format!("cannot resolve database path: {error}")))?
                .join(clean_path)
        };
        let mut normalized = std::path::PathBuf::new();
        for component in absolute.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                other => normalized.push(other.as_os_str()),
            }
        }
        Ok(format!("{FILE_SCHEME}://{}", normalized.display()))
    }

    fn requested_config(dsn: &str, registry_key: &str) -> Result<Config> {
        let (scheme, path) = Self::parse_dsn(dsn)?;
        if scheme == MEMORY_SCHEME {
            return Ok(Config::in_memory());
        }
        let (_, mut config) = Self::parse_file_config(&path)?;
        config.path = Some(
            registry_key
                .strip_prefix("file://")
                .expect("file registry key must have file scheme")
                .to_string(),
        );
        Ok(config)
    }

    fn ensure_matching_config(owner: &DatabaseOwner, requested: &Config, dsn: &str) -> Result<()> {
        if &owner.requested_config == requested {
            Ok(())
        } else {
            Err(Error::invalid_argument(format!(
                "database `{dsn}` is already open with a different effective configuration"
            )))
        }
    }

    fn ensure_matching_plugin_registry(
        owner: &DatabaseOwner,
        requested: &Arc<PluginRegistry>,
        dsn: &str,
    ) -> Result<()> {
        if Arc::ptr_eq(&owner.plugin_registry, requested) {
            Ok(())
        } else {
            Err(Error::invalid_argument(format!(
                "database `{dsn}` is already open with a different plugin registry generation"
            )))
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        match self.inner.engine.lifecycle_state() {
            radixdb_storage::mvcc::engine::EngineLifecycleState::Ready => Ok(()),
            radixdb_storage::mvcc::engine::EngineLifecycleState::CloseFailed(error)
            | radixdb_storage::mvcc::engine::EngineLifecycleState::Failed(error) => Err(error),
            _ => Err(Error::EngineNotOpen),
        }
    }

    fn owner_arc(&self) -> &Arc<DatabaseOwner> {
        &self.inner.owner
    }

    /// Build a connection-local facade around the registry-owned engine.
    fn connection_from_owner(owner: Arc<DatabaseOwner>) -> Self {
        let engine = Arc::clone(&owner.engine);
        let executor = Executor::with_shared_runtime_caches_and_plugin_registry(
            Arc::clone(&engine),
            Arc::clone(&owner.semantic_cache),
            Arc::clone(&owner.feedback_cache),
            Arc::clone(&owner.plugin_registry),
        );
        Self {
            inner: Arc::new(DatabaseInner {
                engine,
                executor: Mutex::new(executor),
                owner,
            }),
        }
    }

    /// Remove an engine owner only after its final connection is released.
    fn try_unregister_owner(owner: &Arc<DatabaseOwner>) {
        if let Ok(mut registry) = DATABASE_REGISTRY.write() {
            if let Some(DatabaseRegistryEntry::Ready(entry)) = registry.get(&owner.registry_key) {
                if Arc::ptr_eq(entry, owner) && Arc::strong_count(owner) == 2 {
                    registry.remove(&owner.registry_key);
                }
            }
        }
    }
}

impl Clone for Database {
    /// Clone with independent transaction state over the shared engine.
    fn clone(&self) -> Self {
        Self::connection_from_owner(Arc::clone(self.owner_arc()))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // DatabaseInner may be retained by a Statement/FFI object. Registry
        // cleanup therefore belongs to its final Drop, not to this facade.
    }
}

impl Database {
    /// Open a memory or file DSN, reusing the engine owner for an identical DSN.
    pub fn open(dsn: &str) -> Result<Self> {
        Self::open_with_plugin_registry(dsn, Arc::clone(&DEFAULT_PLUGIN_REGISTRY))
    }

    /// Open against the immutable registry admitted before server bind.
    #[doc(hidden)]
    pub fn open_with_plugin_registry(
        dsn: &str,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Result<Self> {
        let registry_key = Self::canonical_registry_key(dsn)?;
        let requested_config = Self::requested_config(dsn, &registry_key)?;
        let (opening_slot, owns_open) = {
            let mut registry = DATABASE_REGISTRY
                .write()
                .map_err(|_| Error::LockAcquisitionFailed("registry write".to_string()))?;
            match registry.get(&registry_key) {
                Some(DatabaseRegistryEntry::Ready(inner)) => match inner.engine.lifecycle_state() {
                    radixdb_storage::mvcc::engine::EngineLifecycleState::Ready => {
                        Self::ensure_matching_config(inner, &requested_config, dsn)?;
                        Self::ensure_matching_plugin_registry(inner, &plugin_registry, dsn)?;
                        return Ok(Self::connection_from_owner(Arc::clone(inner)));
                    }
                    radixdb_storage::mvcc::engine::EngineLifecycleState::Closed => {
                        let slot = Arc::new(DatabaseOpenSlot::new());
                        registry.insert(
                            registry_key.clone(),
                            DatabaseRegistryEntry::Opening(Arc::clone(&slot)),
                        );
                        (slot, true)
                    }
                    radixdb_storage::mvcc::engine::EngineLifecycleState::CloseFailed(error)
                    | radixdb_storage::mvcc::engine::EngineLifecycleState::Failed(error) => {
                        return Err(error);
                    }
                    state => {
                        return Err(Error::internal(format!(
                            "database `{dsn}` is not ready for open: {state:?}"
                        )));
                    }
                },
                Some(DatabaseRegistryEntry::Opening(slot)) => (Arc::clone(slot), false),
                None => {
                    let slot = Arc::new(DatabaseOpenSlot::new());
                    registry.insert(
                        registry_key.clone(),
                        DatabaseRegistryEntry::Opening(Arc::clone(&slot)),
                    );
                    (slot, true)
                }
            }
        };

        if !owns_open {
            opening_slot.wait()?;
            let registry = DATABASE_REGISTRY
                .read()
                .map_err(|_| Error::LockAcquisitionFailed("registry read".to_string()))?;
            let Some(DatabaseRegistryEntry::Ready(inner)) = registry.get(&registry_key) else {
                return Err(Error::internal(format!(
                    "database `{dsn}` open completed without publishing its owner"
                )));
            };
            Self::ensure_matching_config(inner, &requested_config, dsn)?;
            Self::ensure_matching_plugin_registry(inner, &plugin_registry, dsn)?;
            return Ok(Self::connection_from_owner(Arc::clone(inner)));
        }

        #[cfg(test)]
        run_database_open_test_hook(dsn);

        let open_result = (|| -> Result<Arc<DatabaseOwner>> {
            let (scheme, _path) = Self::parse_dsn(dsn)?;

            #[cfg(feature = "test-filedb")]
            let mut _temp_dir_holder: Option<tempfile::TempDir> = None;

            let engine = match scheme.as_str() {
                MEMORY_SCHEME => {
                    #[cfg(feature = "test-filedb")]
                    {
                        let tmp = tempfile::tempdir().map_err(|error| {
                            Error::internal(format!("failed to create temp dir: {error}"))
                        })?;
                        let file_dsn = format!("file://{}", tmp.path().display());
                        let (_clean_path, config) = Self::parse_file_config(&file_dsn[7..])?;
                        let engine = composed_mvcc_engine(config, Arc::clone(&plugin_registry));
                        engine.open_engine()?;
                        let engine = Arc::new(engine);
                        engine.start_cleanup();
                        _temp_dir_holder = Some(tmp);
                        engine
                    }
                    #[cfg(not(feature = "test-filedb"))]
                    {
                        let engine =
                            composed_mvcc_engine(Config::default(), Arc::clone(&plugin_registry));
                        engine.open_engine()?;
                        let engine = Arc::new(engine);
                        engine.start_cleanup();
                        engine
                    }
                }
                FILE_SCHEME => {
                    let engine = composed_mvcc_engine(
                        requested_config.clone(),
                        Arc::clone(&plugin_registry),
                    );
                    engine.open_engine()?;
                    let engine = Arc::new(engine);
                    engine.start_cleanup();
                    engine
                }
                _ => {
                    return Err(Error::parse(format!(
                        "Unsupported scheme '{scheme}'. Use 'memory://' or 'file://path'"
                    )));
                }
            };

            Ok(Arc::new(DatabaseOwner {
                engine,
                semantic_cache: Arc::new(SemanticCache::new()),
                feedback_cache: Arc::new(FeedbackCache::new()),
                plugin_registry: Arc::clone(&plugin_registry),
                registry_key: registry_key.clone(),
                dsn: dsn.to_string(),
                requested_config: requested_config.clone(),
                #[cfg(feature = "test-filedb")]
                _temp_dir: _temp_dir_holder,
            }))
        })();

        match open_result {
            Ok(inner) => {
                let mut registry = match DATABASE_REGISTRY.write() {
                    Ok(registry) => registry,
                    Err(_) => {
                        let error = Error::LockAcquisitionFailed("registry write".to_string());
                        opening_slot.finish(Err(error.clone()));
                        return Err(error);
                    }
                };
                let owns_slot = matches!(
                    registry.get(&registry_key),
                    Some(DatabaseRegistryEntry::Opening(slot))
                        if Arc::ptr_eq(slot, &opening_slot)
                );
                if !owns_slot {
                    let error = Error::internal(format!(
                        "database `{dsn}` opening owner changed before publication"
                    ));
                    opening_slot.finish(Err(error.clone()));
                    return Err(error);
                }
                registry.insert(
                    registry_key.clone(),
                    DatabaseRegistryEntry::Ready(Arc::clone(&inner)),
                );
                drop(registry);
                opening_slot.finish(Ok(()));
                Ok(Self::connection_from_owner(inner))
            }
            Err(error) => {
                if let Ok(mut registry) = DATABASE_REGISTRY.write() {
                    let owns_slot = matches!(
                        registry.get(&registry_key),
                        Some(DatabaseRegistryEntry::Opening(slot))
                            if Arc::ptr_eq(slot, &opening_slot)
                    );
                    if owns_slot {
                        registry.remove(&registry_key);
                    }
                }
                opening_slot.finish(Err(error.clone()));
                Err(error)
            }
        }
    }

    /// Open an in-memory database
    ///
    /// This is a convenience method that creates a new in-memory database.
    /// Each call creates a unique instance (unlike `open("memory://")` which
    /// would share the same instance).
    pub fn open_in_memory() -> Result<Self> {
        Self::create_in_memory_engine()
    }

    #[cfg(feature = "test-filedb")]
    fn create_in_memory_engine() -> Result<Self> {
        let tmp = tempfile::tempdir()
            .map_err(|e| Error::internal(format!("failed to create temp dir: {}", e)))?;
        let file_dsn = format!("file://{}", tmp.path().display());
        let (_clean_path, config) = Self::parse_file_config(&file_dsn[7..])?;
        let engine = composed_mvcc_engine(config, Arc::clone(&DEFAULT_PLUGIN_REGISTRY));
        engine.open_engine()?;
        let engine = Arc::new(engine);
        engine.start_cleanup();
        let owner = Arc::new(DatabaseOwner {
            engine,
            semantic_cache: Arc::new(SemanticCache::new()),
            feedback_cache: Arc::new(FeedbackCache::new()),
            plugin_registry: Arc::clone(&DEFAULT_PLUGIN_REGISTRY),
            registry_key: "memory://".to_string(),
            dsn: "memory://".to_string(),
            requested_config: Config::in_memory(),
            _temp_dir: Some(tmp),
        });
        Ok(Self::connection_from_owner(owner))
    }

    #[cfg(not(feature = "test-filedb"))]
    fn create_in_memory_engine() -> Result<Self> {
        let engine = composed_in_memory_engine();
        engine.open_engine()?;
        let engine = Arc::new(engine);
        engine.start_cleanup();
        let owner = Arc::new(DatabaseOwner {
            engine,
            semantic_cache: Arc::new(SemanticCache::new()),
            feedback_cache: Arc::new(FeedbackCache::new()),
            plugin_registry: Arc::clone(&DEFAULT_PLUGIN_REGISTRY),
            registry_key: "memory://".to_string(),
            dsn: "memory://".to_string(),
            requested_config: Config::in_memory(),
        });
        Ok(Self::connection_from_owner(owner))
    }

    /// Parse a DSN into scheme and path
    fn parse_dsn(dsn: &str) -> Result<(String, String)> {
        let idx = dsn
            .find("://")
            .ok_or_else(|| Error::parse("Invalid DSN format: expected scheme://path"))?;

        let scheme = dsn[..idx].to_lowercase();
        let path = dsn[idx + 3..].to_string();

        // Validate scheme
        match scheme.as_str() {
            MEMORY_SCHEME | FILE_SCHEME => {}
            _ => {
                return Err(Error::parse(format!(
                    "Unsupported scheme '{}'. Use 'memory://' or 'file://path'",
                    scheme
                )));
            }
        }

        // Validate file path
        if scheme == FILE_SCHEME {
            let clean_path = if path.contains('?') {
                &path[..path.find('?').unwrap()]
            } else {
                &path
            };

            if clean_path.is_empty() {
                return Err(Error::parse("file:// scheme requires a non-empty path"));
            }
        }

        Ok((scheme, path))
    }

    /// Parse file:// config from query parameters
    fn parse_file_config(path: &str) -> Result<(String, Config)> {
        fn parse_bool_option(key: &str, value: &str) -> Result<bool> {
            match value.to_ascii_lowercase().as_str() {
                "on" | "true" | "1" | "yes" => Ok(true),
                "off" | "false" | "0" | "no" => Ok(false),
                _ => Err(Error::invalid_argument(format!(
                    "invalid {key}: '{value}' (expected on/off)"
                ))),
            }
        }

        let (clean_path, query) = if let Some(idx) = path.find('?') {
            (path[..idx].to_string(), Some(&path[idx + 1..]))
        } else {
            (path.to_string(), None)
        };

        let mut config = Config::with_path(&clean_path);

        // Parse query parameters
        if let Some(query) = query {
            let mut seen_options = HashSet::new();
            for param in query.split('&') {
                let mut parts = param.splitn(2, '=');
                let key = parts.next().unwrap_or("");
                let value = parts.next().unwrap_or("");
                if !seen_options.insert(key) {
                    return Err(Error::invalid_argument(format!(
                        "duplicate file database option: '{key}'"
                    )));
                }

                match key {
                    // Sync mode: sync_mode=none|normal|full
                    "sync_mode" => {
                        config.persistence.sync_mode = match value.to_lowercase().as_str() {
                            "none" | "off" | "0" => SyncMode::None,
                            "normal" | "1" => SyncMode::Normal,
                            "full" | "2" => SyncMode::Full,
                            _ => {
                                return Err(Error::invalid_argument(format!(
                                    "invalid sync mode: '{value}' (expected none/normal/full)"
                                )))
                            }
                        };
                    }
                    // Checkpoint interval in seconds: checkpoint_interval=60
                    "checkpoint_interval" => {
                        config.persistence.checkpoint_interval =
                            value.parse::<u32>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid checkpoint_interval: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Compaction threshold: compact_threshold=4
                    "compact_threshold" => {
                        config.persistence.compact_threshold =
                            value.parse::<u32>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid compact_threshold: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "max_compaction_jobs" => {
                        let count = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid max_compaction_jobs: '{}'",
                                value
                            ))
                        })?;
                        if !(1..=radixdb_storage::config::MAX_COMPACTION_JOBS).contains(&count) {
                            return Err(Error::invalid_argument(format!(
                                "max_compaction_jobs must be in 1..={}",
                                radixdb_storage::config::MAX_COMPACTION_JOBS
                            )));
                        }
                        config.persistence.max_compaction_jobs = count;
                    }
                    // Shared CPU-heavy storage worker budget. Zero selects
                    // host/cgroup-visible automatic parallelism.
                    "storage_cpu_workers" => {
                        config.persistence.storage_cpu_workers =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid storage_cpu_workers: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "page_cache_level" => {
                        let level = value.parse::<u8>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid page_cache_level: '{}'",
                                value
                            ))
                        })?;
                        if level > radixdb_storage::config::MAX_PAGE_CACHE_LEVEL {
                            return Err(Error::invalid_argument(format!(
                                "page_cache_level must be in 0..={}",
                                radixdb_storage::config::MAX_PAGE_CACHE_LEVEL
                            )));
                        }
                        config.persistence.page_cache_level = level;
                    }
                    "page_cache_max_bytes" => {
                        config.persistence.page_cache_max_bytes =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid page_cache_max_bytes: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "page_cache_memory_reserve" => {
                        config.persistence.page_cache_memory_reserve =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid page_cache_memory_reserve: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Maximum immutable inputs owned by one compaction job.
                    "max_compaction_input_segments" => {
                        let count = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid max_compaction_input_segments: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.max_compaction_input_segments = count.max(1);
                    }
                    // Maximum physical payload + posting bytes owned by one
                    // compaction job.
                    "max_compaction_input_bytes" => {
                        let bytes = value.parse::<u64>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid max_compaction_input_bytes: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.max_compaction_input_bytes = bytes.max(1);
                    }
                    "max_compaction_output_bytes" => {
                        let bytes = value.parse::<u64>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid max_compaction_output_bytes: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.max_compaction_output_bytes = bytes.max(1);
                    }
                    "compaction_job_time_budget_ms" => {
                        config.persistence.compaction_job_time_budget_ms =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid compaction_job_time_budget_ms: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "compaction_io_bytes_per_sec" => {
                        config.persistence.compaction_io_bytes_per_sec =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid compaction_io_bytes_per_sec: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "compaction_disk_reserve_bytes" => {
                        config.persistence.compaction_disk_reserve_bytes =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid compaction_disk_reserve_bytes: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "compaction_retry_cooldown_ms" => {
                        config.persistence.compaction_retry_cooldown_ms =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid compaction_retry_cooldown_ms: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "l0_soft_limit_segments" => {
                        config.persistence.l0_soft_limit_segments =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid l0_soft_limit_segments: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "l0_hard_limit_segments" => {
                        config.persistence.l0_hard_limit_segments =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid l0_hard_limit_segments: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "l0_soft_limit_bytes" => {
                        config.persistence.l0_soft_limit_bytes =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid l0_soft_limit_bytes: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "l0_hard_limit_bytes" => {
                        config.persistence.l0_hard_limit_bytes =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid l0_hard_limit_bytes: '{}'",
                                    value
                                ))
                            })?;
                    }
                    "l0_soft_backpressure_wait_ms" => {
                        config.persistence.l0_soft_backpressure_wait_ms =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid l0_soft_backpressure_wait_ms: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Number of backup snapshots to keep: keep_snapshots=3
                    "keep_snapshots" => {
                        config.persistence.keep_snapshots = value.parse::<u32>().map_err(|_| {
                            Error::invalid_argument(format!("invalid keep_snapshots: '{}'", value))
                        })?;
                    }
                    // WAL flush trigger in bytes: wal_flush_trigger=32768
                    "wal_flush_trigger" => {
                        config.persistence.wal_flush_trigger =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid wal_flush_trigger: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // WAL buffer size in bytes: wal_buffer_size=65536
                    "wal_buffer_size" => {
                        config.persistence.wal_buffer_size =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid wal_buffer_size: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // WAL max size in bytes: wal_max_size=67108864
                    "wal_max_size" => {
                        config.persistence.wal_max_size = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!("invalid wal_max_size: '{}'", value))
                        })?;
                    }
                    // Fail-closed memory envelope for one atomic COPY.
                    "copy_max_transaction_bytes" => {
                        let bytes = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid copy_max_transaction_bytes: '{}'",
                                value
                            ))
                        })?;
                        if bytes == 0 {
                            return Err(Error::invalid_argument(
                                "copy_max_transaction_bytes must not be zero",
                            ));
                        }
                        config.persistence.copy_max_transaction_bytes = bytes;
                    }
                    // Removed before v1: Normal mode durably syncs every
                    // terminal commit, so accepting this knob would promise a
                    // batching policy that does not exist.
                    "commit_batch_size" => {
                        return Err(Error::invalid_argument(format!(
                            "commit_batch_size is unsupported; choose sync_mode instead (got '{}')",
                            value
                        )));
                    }
                    // Sync interval in ms: sync_interval_ms=10
                    "sync_interval_ms" => {
                        config.persistence.sync_interval_ms =
                            value.parse::<u32>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid sync_interval_ms: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // WAL compression: wal_compression=on|off
                    "wal_compression" => {
                        config.persistence.wal_compression =
                            parse_bool_option("wal_compression", value)?;
                    }
                    // Volume LZ4 compression: volume_compression=on|off
                    "volume_compression" => {
                        config.persistence.volume_compression =
                            parse_bool_option("volume_compression", value)?;
                    }
                    // All compressions (WAL + volume): compression=on|off
                    "compression" => {
                        let enabled = parse_bool_option("compression", value)?;
                        config.persistence.wal_compression = enabled;
                        config.persistence.volume_compression = enabled;
                    }
                    // Target rows per volume: target_volume_rows=1048576
                    "target_volume_rows" => {
                        let rows = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid target_volume_rows: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.target_volume_rows = rows.max(65_536);
                    }
                    // First seal hot byte threshold: seal_hot_bytes_threshold=67108864
                    "seal_hot_bytes_threshold" => {
                        let bytes = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid seal_hot_bytes_threshold: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.seal_hot_bytes_threshold = bytes.max(1);
                    }
                    // Incremental seal hot byte threshold:
                    // seal_incremental_hot_bytes_threshold=16777216
                    "seal_incremental_hot_bytes_threshold" => {
                        let bytes = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid seal_incremental_hot_bytes_threshold: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.seal_incremental_hot_bytes_threshold = bytes.max(1);
                    }
                    // Global resident cold-volume payload cache budget:
                    // volume_cache_bytes=1073741824
                    "volume_cache_bytes" => {
                        config.persistence.volume_cache_bytes =
                            value.parse::<usize>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid volume_cache_bytes: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Cold-volume read queue depth:
                    // read_queue_depth=4
                    "read_queue_depth" => {
                        let depth = value.parse::<usize>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid read_queue_depth: '{}'",
                                value
                            ))
                        })?;
                        config.persistence.read_queue_depth = depth.max(1);
                    }
                    // Checkpoint on close: checkpoint_on_close=off
                    // Set to off to simulate crashes in tests (WAL not truncated)
                    "checkpoint_on_close" => {
                        config.persistence.checkpoint_on_close =
                            parse_bool_option("checkpoint_on_close", value)?;
                    }
                    // Cleanup interval in seconds: cleanup_interval=60
                    "cleanup_interval" => {
                        config.cleanup.interval_secs = value.parse::<u64>().map_err(|_| {
                            Error::invalid_argument(format!(
                                "invalid cleanup_interval: '{}'",
                                value
                            ))
                        })?;
                    }
                    // Deleted row retention in seconds: deleted_row_retention=300
                    "deleted_row_retention" => {
                        config.cleanup.deleted_row_retention_secs =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid deleted_row_retention: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Transaction retention in seconds: transaction_retention=3600
                    "transaction_retention" => {
                        config.cleanup.transaction_retention_secs =
                            value.parse::<u64>().map_err(|_| {
                                Error::invalid_argument(format!(
                                    "invalid transaction_retention: '{}'",
                                    value
                                ))
                            })?;
                    }
                    // Disable cleanup: cleanup=off
                    "cleanup" => {
                        config.cleanup.enabled = parse_bool_option("cleanup", value)?;
                    }
                    _ => {
                        return Err(Error::invalid_argument(format!(
                            "unknown file database option: '{key}'"
                        )))
                    }
                }
            }
        }

        if config.persistence.l0_soft_limit_segments == 0
            || config.persistence.l0_soft_limit_segments
                >= config.persistence.l0_hard_limit_segments
        {
            return Err(Error::invalid_argument(
                "l0 segment limits require 0 < soft < hard",
            ));
        }
        if config.persistence.l0_soft_limit_bytes == 0
            || config.persistence.l0_soft_limit_bytes >= config.persistence.l0_hard_limit_bytes
        {
            return Err(Error::invalid_argument(
                "l0 byte limits require 0 < soft < hard",
            ));
        }

        Ok((clean_path, config))
    }

    /// Execute a SQL statement
    ///
    /// Use this for DDL (CREATE, DROP, ALTER) and DML (INSERT, UPDATE, DELETE) statements.
    ///
    /// # Parameters
    ///
    /// Parameters can be passed using:
    /// - Empty tuple `()` for no parameters
    /// - Tuple syntax `(1, "Alice", 30)` for multiple parameters
    /// - `params!` macro `params![1, "Alice", 30]`
    ///
    /// # Returns
    ///
    /// Returns the number of rows affected for DML statements, or 0 for DDL.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // DDL - no parameters
    /// db.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())?;
    ///
    /// // DML with tuple parameters
    /// db.execute("INSERT INTO users VALUES ($1, $2)", (1, "Alice"))?;
    ///
    /// // DML with params! macro
    /// db.execute("INSERT INTO users VALUES ($1, $2)", params![2, "Bob"])?;
    ///
    /// // Update with mixed types
    /// let affected = db.execute(
    ///     "UPDATE users SET name = $1 WHERE id = $2",
    ///     ("Charlie", 1)
    /// )?;
    /// ```
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<i64> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let result = executor.execute_with_params(sql, params.into_params())?;
        Ok(result.rows_affected())
    }

    /// Execute a query that returns rows
    ///
    /// # Parameters
    ///
    /// Parameters can be passed using:
    /// - Empty tuple `()` for no parameters
    /// - Tuple syntax `(value,)` for single parameter (note trailing comma)
    /// - Tuple syntax `(1, "Alice")` for multiple parameters
    /// - `params!` macro `params![1, "Alice"]`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Query all rows
    /// for row in db.query("SELECT * FROM users", ())? {
    ///     let row = row?;
    ///     let id: i64 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    ///
    /// // Query with parameters
    /// for row in db.query("SELECT * FROM users WHERE age > $1", (18,))? {
    ///     // ...
    /// }
    ///
    /// // Collect into Vec
    /// let users: Vec<_> = db.query("SELECT * FROM users", ())?
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// ```
    pub fn query<P: Params>(&self, sql: &str, params: P) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let result = executor.execute_with_params(sql, params.into_params())?;
        Ok(Rows::new(result))
    }

    /// Execute a query and return a single value
    ///
    /// This is a convenience method for queries that return a single row with a single column.
    /// Returns an error if the query returns no rows.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let count: i64 = db.query_one("SELECT COUNT(*) FROM users", ())?;
    /// let name: String = db.query_one("SELECT name FROM users WHERE id = $1", (1,))?;
    /// ```
    pub fn query_one<T: FromValue, P: Params>(&self, sql: &str, params: P) -> Result<T> {
        let row = self
            .query(sql, params)?
            .next()
            .ok_or(Error::NoRowsReturned)??;
        row.get(0)
    }

    /// Execute a query and return an optional single value
    ///
    /// Like `query_one`, but returns `None` if no rows are returned.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let name: Option<String> = db.query_opt("SELECT name FROM users WHERE id = $1", (999,))?;
    /// assert!(name.is_none());
    /// ```
    pub fn query_opt<T: FromValue, P: Params>(&self, sql: &str, params: P) -> Result<Option<T>> {
        match self.query(sql, params)?.next() {
            Some(row) => Ok(Some(row?.get(0)?)),
            None => Ok(None),
        }
    }

    /// Execute a write statement with a timeout
    ///
    /// Like `execute`, but cancels the query if it exceeds the timeout.
    /// Timeout is specified in milliseconds. Use 0 for no timeout.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Execute with 5 second timeout
    /// db.execute_with_timeout("DELETE FROM large_table WHERE old = true", (), 5000)?;
    /// ```
    pub fn execute_with_timeout<P: Params>(
        &self,
        sql: &str,
        params: P,
        timeout_ms: u64,
    ) -> Result<i64> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let param_values = params.into_params();
        let ctx = ExecutionContextBuilder::new()
            .params(param_values)
            .timeout_ms(timeout_ms)
            .build();

        let result = executor.execute_with_context(sql, &ctx)?;
        Ok(result.rows_affected())
    }

    /// Execute a query with a timeout
    ///
    /// Like `query`, but cancels the query if it exceeds the timeout.
    /// Timeout is specified in milliseconds. Use 0 for no timeout.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Query with 10 second timeout
    /// for row in db.query_with_timeout("SELECT * FROM large_table", (), 10000)? {
    ///     // process row
    /// }
    /// ```
    pub fn query_with_timeout<P: Params>(
        &self,
        sql: &str,
        params: P,
        timeout_ms: u64,
    ) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let param_values = params.into_params();
        let ctx = ExecutionContextBuilder::new()
            .params(param_values)
            .timeout_ms(timeout_ms)
            .build();

        let result = executor.execute_with_context(sql, &ctx)?;
        Ok(Rows::new(result))
    }

    /// Prepare a SQL statement for repeated execution
    ///
    /// Prepared statements are more efficient when executing the same query
    /// multiple times with different parameters.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let stmt = db.prepare("SELECT * FROM users WHERE id = $1")?;
    ///
    /// // Execute multiple times with different parameters
    /// for id in 1..=10 {
    ///     for row in stmt.query((id,))? {
    ///         // ...
    ///     }
    /// }
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        self.ensure_open()?;
        Statement::new(Arc::downgrade(&self.inner), sql.to_string(), self)
    }

    /// Create a Database from an existing Arc<DatabaseInner>.
    /// Used by Statement to upgrade weak references.
    pub(crate) fn from_inner(inner: Arc<DatabaseInner>) -> Self {
        Database { inner }
    }

    /// Execute a statement with named parameters
    ///
    /// Named parameters use the `:name` syntax in SQL queries.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::{Database, named_params};
    ///
    /// let db = Database::open("memory://")?;
    /// db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)", ())?;
    ///
    /// // Insert with named params
    /// db.execute_named(
    ///     "INSERT INTO users VALUES (:id, :name, :age)",
    ///     named_params!{ id: 1, name: "Alice", age: 30 }
    /// )?;
    ///
    /// // Update with named params
    /// db.execute_named(
    ///     "UPDATE users SET name = :name WHERE id = :id",
    ///     named_params!{ id: 1, name: "Alicia" }
    /// )?;
    /// ```
    pub fn execute_named(&self, sql: &str, params: NamedParams) -> Result<i64> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let result = executor.execute_with_named_params(sql, params.into_inner())?;
        Ok(result.rows_affected())
    }

    /// Execute a query with named parameters
    ///
    /// Named parameters use the `:name` syntax in SQL queries.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::{Database, named_params};
    ///
    /// let db = Database::open("memory://")?;
    /// db.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())?;
    /// db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')", ())?;
    ///
    /// // Query with named params
    /// for row in db.query_named(
    ///     "SELECT * FROM users WHERE name = :name",
    ///     named_params!{ name: "Alice" }
    /// )? {
    ///     let row = row?;
    ///     println!("Found user: id={}", row.get::<i64>(0)?);
    /// }
    /// ```
    pub fn query_named(&self, sql: &str, params: NamedParams) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let result = executor.execute_with_named_params(sql, params.into_inner())?;
        Ok(Rows::new(result))
    }

    /// Execute a query with named parameters and a cancellation deadline.
    ///
    /// This is the named-parameter counterpart of [`Database::query_with_timeout`].
    /// A timeout of zero keeps the existing unlimited behavior.
    pub fn query_named_with_timeout(
        &self,
        sql: &str,
        params: NamedParams,
        timeout_ms: u64,
    ) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let mut ctx = ExecutionContext::with_named_params(params.into_inner());
        ctx.set_timeout_ms(timeout_ms);
        let result = executor.execute_with_context(sql, &ctx)?;
        Ok(Rows::new(result))
    }

    /// Execute one server request through the embedded facade contract.
    #[doc(hidden)]
    pub fn query_for_server(
        &self,
        sql: &str,
        context: &super::ServerExecutionContext,
    ) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let result = executor.execute_with_context(sql, context.inner())?;
        Ok(Rows::new(result))
    }

    /// Execute a query with named parameters and return a single value
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::{Database, named_params};
    ///
    /// let count: i64 = db.query_one_named(
    ///     "SELECT COUNT(*) FROM users WHERE age > :min_age",
    ///     named_params!{ min_age: 18 }
    /// )?;
    /// ```
    pub fn query_one_named<T: FromValue>(&self, sql: &str, params: NamedParams) -> Result<T> {
        let mut rows = self.query_named(sql, params)?;
        match rows.next() {
            Some(Ok(row)) => row.get(0),
            Some(Err(e)) => Err(e),
            None => Err(Error::NoRowsReturned),
        }
    }

    /// Execute a query and map results to structs
    ///
    /// This method executes a query and converts each row to a struct
    /// that implements the `FromRow` trait.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::{Database, FromRow, ResultRow, Result};
    ///
    /// struct User {
    ///     id: i64,
    ///     name: String,
    /// }
    ///
    /// impl FromRow for User {
    ///     fn from_row(row: &ResultRow) -> Result<Self> {
    ///         Ok(User {
    ///             id: row.get(0)?,
    ///             name: row.get(1)?,
    ///         })
    ///     }
    /// }
    ///
    /// let db = Database::open("memory://")?;
    /// db.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())?;
    /// db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')", ())?;
    ///
    /// // Query and map to structs
    /// let users: Vec<User> = db.query_as("SELECT id, name FROM users", ())?;
    /// assert_eq!(users.len(), 2);
    /// assert_eq!(users[0].name, "Alice");
    /// ```
    pub fn query_as<T: FromRow, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        let rows = self.query(sql, params)?;
        rows.map(|r| r.and_then(|row| T::from_row(&row))).collect()
    }

    /// Execute a query with named parameters and map results to structs
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::{Database, FromRow, ResultRow, Result, named_params};
    ///
    /// struct Product {
    ///     id: i64,
    ///     name: String,
    ///     price: f64,
    /// }
    ///
    /// impl FromRow for Product {
    ///     fn from_row(row: &ResultRow) -> Result<Self> {
    ///         Ok(Product {
    ///             id: row.get(0)?,
    ///             name: row.get(1)?,
    ///             price: row.get(2)?,
    ///         })
    ///     }
    /// }
    ///
    /// let products: Vec<Product> = db.query_as_named(
    ///     "SELECT id, name, price FROM products WHERE price > :min_price",
    ///     named_params!{ min_price: 10.0 }
    /// )?;
    /// ```
    pub fn query_as_named<T: FromRow>(&self, sql: &str, params: NamedParams) -> Result<Vec<T>> {
        let rows = self.query_named(sql, params)?;
        rows.map(|r| r.and_then(|row| T::from_row(&row))).collect()
    }

    /// Begin a new transaction with default isolation level
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let tx = db.begin()?;
    /// tx.execute("INSERT INTO users VALUES ($1, $2)", (1, "Alice"))?;
    /// tx.commit()?;
    /// ```
    pub fn begin(&self) -> Result<Transaction> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let tx = executor.begin_transaction()?;
        Ok(Transaction::new(tx, Arc::clone(&self.inner)))
    }

    /// Begin the read-only transaction used by the logical SQL exporter.
    ///
    /// Lock ordering intentionally matches normal connection execution:
    /// connection executor first, then the shared DDL fence. The returned
    /// transaction retains that fence and streams rows from one MVCC snapshot.
    #[doc(hidden)]
    pub fn begin_logical_export(&self) -> Result<Transaction> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let engine = executor.engine().clone();
        let plugin_registry = executor.plugin_registry();
        let fence = engine.acquire_ddl_statement_fence(false);
        let tx = executor.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation)?;
        Ok(Transaction::new_logical_export(
            tx,
            engine,
            plugin_registry,
            Arc::clone(&self.inner),
            fence,
        ))
    }

    /// Begin a new transaction with a specific isolation level
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::IsolationLevel;
    ///
    /// let tx = db.begin_with_isolation(IsolationLevel::SnapshotIsolation)?;
    /// // All reads in this transaction see a consistent snapshot
    /// tx.execute("UPDATE users SET balance = balance - 100 WHERE id = $1", (1,))?;
    /// tx.commit()?;
    /// ```
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Result<Transaction> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;

        let tx = executor.begin_transaction_with_isolation(isolation)?;
        Ok(Transaction::new(tx, Arc::clone(&self.inner)))
    }

    /// Get the underlying storage engine
    ///
    /// This is primarily for advanced use cases and testing.
    pub fn engine(&self) -> &Arc<MVCCEngine> {
        &self.inner.engine
    }

    /// Close the database connection
    ///
    /// This removes the database from the global registry and closes the engine,
    /// releasing the file lock immediately so another process can open the database.
    ///
    /// Note: The engine is also closed automatically when all Database instances
    /// are dropped.
    pub fn close(&self) -> Result<()> {
        if Arc::strong_count(&self.inner) != 1 {
            return Err(Error::invalid_argument(
                "cannot close database while a Transaction or retained connection owner is active",
            ));
        }
        // Explicit close is terminal for the shared engine, but registry
        // ownership remains intact until every mandatory close stage succeeds.
        self.inner.engine.close_engine()?;
        let owner = self.owner_arc();
        let mut registry = DATABASE_REGISTRY
            .write()
            .map_err(|_| Error::LockAcquisitionFailed("registry write".to_string()))?;
        if matches!(
            registry.get(&owner.registry_key),
            Some(DatabaseRegistryEntry::Ready(entry)) if Arc::ptr_eq(entry, owner)
        ) {
            registry.remove(&owner.registry_key);
        }

        Ok(())
    }

    /// Get a cached plan for a SQL statement (parse once, execute many times).
    ///
    /// Returns a `CachedPlanRef` that can be stored and passed to
    /// `execute_plan()` / `query_plan()` for zero-lookup execution.
    pub fn cached_plan(&self, sql: &str) -> Result<CachedPlanRef> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.get_or_create_plan(sql)
    }

    /// Execute a pre-cached plan with positional parameters (no parsing, no cache lookup).
    pub fn execute_plan<P: Params>(&self, plan: &CachedPlanRef, params: P) -> Result<i64> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let param_values = params.into_params();
        let ctx = if param_values.is_empty() {
            ExecutionContext::new()
        } else {
            ExecutionContext::with_params(param_values)
        };
        let result = executor.execute_with_cached_plan(plan, &ctx)?;
        Ok(result.rows_affected())
    }

    /// Query using a pre-cached plan with positional parameters (no parsing, no cache lookup).
    pub fn query_plan<P: Params>(&self, plan: &CachedPlanRef, params: P) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let param_values = params.into_params();
        let ctx = if param_values.is_empty() {
            ExecutionContext::new()
        } else {
            ExecutionContext::with_params(param_values)
        };
        let result = executor.execute_with_cached_plan(plan, &ctx)?;
        Ok(Rows::new(result))
    }

    /// Execute a pre-cached plan with named parameters (no parsing, no cache lookup).
    pub fn execute_named_plan(&self, plan: &CachedPlanRef, params: NamedParams) -> Result<i64> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = executor.execute_with_cached_plan(plan, &ctx)?;
        Ok(result.rows_affected())
    }

    /// Query using a pre-cached plan with named parameters (no parsing, no cache lookup).
    pub fn query_named_plan(&self, plan: &CachedPlanRef, params: NamedParams) -> Result<Rows> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = executor.execute_with_cached_plan(plan, &ctx)?;
        Ok(Rows::new(result))
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> Result<bool> {
        self.ensure_open()?;
        let engine = &self.inner.engine;
        let tx = engine.begin_transaction()?;
        match tx.get_table(name) {
            Ok(_) => Ok(true),
            Err(Error::TableNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Get the DSN this database was opened with
    pub fn dsn(&self) -> &str {
        &self.inner.owner.dsn
    }

    /// Set the default isolation level for new transactions
    pub fn set_default_isolation_level(&self, level: IsolationLevel) -> Result<()> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.set_default_isolation_level(level);
        Ok(())
    }

    /// Return this connection's default isolation for future transactions.
    pub fn default_isolation_level(&self) -> Result<IsolationLevel> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        Ok(executor.default_isolation_level())
    }

    /// Create a backup snapshot of the database
    ///
    /// This pins one complete catalog/data/index generation and copies every
    /// reachable immutable member into a committed physical snapshot.
    /// Normal persistence advances the same generation graph plus WAL.
    ///
    /// In-memory databases reject this operation because no durable snapshot
    /// artifact can be created.
    pub fn create_snapshot(&self) -> Result<radixdb_storage::PhysicalSnapshotIdentity> {
        use radixdb_storage::Engine;
        self.ensure_open()?;
        self.inner.engine.create_snapshot()
    }

    /// Restore the database from a backup snapshot.
    ///
    /// If no identity is provided, restores from the latest snapshot.
    /// Otherwise restores the snapshot with the specified stable identity.
    ///
    /// This is a destructive operation that atomically replaces the current
    /// catalog/data/index generation with the selected complete snapshot.
    pub fn restore_snapshot(&self, snapshot_id: Option<&str>) -> Result<String> {
        use radixdb_storage::Engine;
        self.ensure_open()?;
        // Cache admission is fallible, so acquire it before the destructive
        // engine operation. A poisoned lock must never turn a committed
        // restore into an error outcome.
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        let result = self.inner.engine.restore_snapshot(snapshot_id)?;
        // Clear all query caches since all data has changed.
        executor.clear_semantic_cache();
        radixdb_executor::context::clear_scalar_subquery_cache();
        radixdb_executor::context::clear_in_subquery_cache();
        radixdb_executor::context::clear_semi_join_cache();
        Ok(result)
    }

    /// Get the internal executor (for Statement use)
    pub(crate) fn executor(&self) -> &Mutex<Executor> {
        &self.inner.executor
    }

    #[doc(hidden)]
    pub fn describe_query_output(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<radixdb_executor::QueryOutputColumn>>> {
        self.ensure_open()?;
        self.inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?
            .describe_query_output(sql)
    }

    /// Whether this connection-local SQL executor owns a BEGIN transaction.
    #[doc(hidden)]
    pub fn has_active_sql_transaction(&self) -> Result<bool> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        Ok(executor.has_active_transaction())
    }

    /// Get semantic-cache statistics.
    pub fn semantic_cache_stats(&self) -> Result<SemanticCacheStatsSnapshot> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        Ok(executor.semantic_cache_stats())
    }

    /// Clear all semantic-cache entries.
    pub fn clear_semantic_cache(&self) -> Result<()> {
        self.ensure_open()?;
        let executor = self
            .inner
            .executor
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("executor".to_string()))?;
        executor.clear_semantic_cache();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::named_params;
    use radixdb_core::Value;

    #[test]
    fn checkpoint_reopens_from_canonical_artifact_generation() {
        let directory = tempfile::tempdir().expect("create database directory");
        let database_path = directory.path().join("canonical-checkpoint");
        let dsn = format!(
            "file://{}?checkpoint_on_close=false",
            database_path.display()
        );
        let database = Database::open(&dsn).expect("open persistent database");
        database
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .expect("create table");
        let catalog = database.engine().pin_catalog().expect("pin catalog");
        assert!(
            catalog
                .graph()
                .objects()
                .any(|object| object.name().display().as_str() == "items"),
            "CREATE TABLE must publish the logical catalog before returning"
        );
        database
            .execute("INSERT INTO items VALUES (1, 'one'), (2, 'two')", ())
            .expect("insert rows");
        radixdb_storage::traits::Engine::force_checkpoint_cycle(database.engine().as_ref())
            .expect("publish physical checkpoint");
        database.close().expect("close database");

        let mut pending = vec![database_path.clone()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("read artifact tree") {
                let entry = entry.expect("read artifact member");
                let path = entry.path();
                if entry.file_type().expect("read artifact type").is_dir() {
                    pending.push(path);
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("artifact name is UTF-8");
                assert!(!name.starts_with("ddl-"), "legacy DDL owner: {name}");
                assert!(!name.ends_with(".vol"), "legacy volume: {name}");
                assert!(!name.ends_with(".rpi"), "legacy postings: {name}");
            }
        }
        assert!(!database_path.join("volumes").exists());

        let reopened = Database::open(&dsn).expect("reopen persistent database");
        let rows = reopened
            .query("SELECT id, payload FROM items ORDER BY id", ())
            .expect("query recovered rows")
            .collect_vec()
            .expect("collect recovered rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<i64>(0).expect("first id"), 1);
        assert_eq!(rows[0].get::<String>(1).expect("first payload"), "one");
        assert_eq!(rows[1].get::<i64>(0).expect("second id"), 2);
        assert_eq!(rows[1].get::<String>(1).expect("second payload"), "two");
        reopened.close().expect("close reopened database");
    }

    #[test]
    fn transactional_index_rename_and_drop_survive_reopen() {
        let directory = tempfile::tempdir().expect("create database directory");
        let database_path = directory.path().join("transactional-index-ddl");
        let dsn = format!(
            "file://{}?checkpoint_on_close=false",
            database_path.display()
        );
        let database = Database::open(&dsn).expect("open persistent database");
        database
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .expect("create table");
        database
            .execute("CREATE INDEX idx_payload ON items(payload)", ())
            .expect("create index");
        database
            .execute("ALTER INDEX idx_payload RENAME TO idx_payload_live", ())
            .expect("rename index");
        database.close().expect("close renamed-index database");

        let reopened = Database::open(&dsn).expect("reopen renamed-index database");
        let indexes = reopened
            .schema()
            .table("items")
            .indexes()
            .fetch()
            .expect("fetch indexes after rename reopen");
        assert!(indexes.iter().any(|index| index.name == "idx_payload_live"));
        assert!(!indexes.iter().any(|index| index.name == "idx_payload"));
        reopened
            .execute("DROP INDEX idx_payload_live ON items", ())
            .expect("drop renamed index");
        reopened.close().expect("close dropped-index database");

        let reopened = Database::open(&dsn).expect("reopen dropped-index database");
        let indexes = reopened
            .schema()
            .table("items")
            .indexes()
            .fetch()
            .expect("fetch indexes after drop reopen");
        assert!(!indexes.iter().any(|index| index.name == "idx_payload_live"));
        reopened.close().expect("close final database");
    }

    #[test]
    fn ctas_and_view_catalog_survive_reopen_without_legacy_ddl_owner() {
        let directory = tempfile::tempdir().expect("create database directory");
        let database_path = directory.path().join("ctas-view-catalog");
        let dsn = format!(
            "file://{}?checkpoint_on_close=false&checkpoint_interval=0",
            database_path.display()
        );
        let database = Database::open(&dsn).expect("open persistent database");
        database
            .execute(
                "CREATE TABLE source_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .expect("create source table");
        database
            .execute("INSERT INTO source_rows VALUES (1, 'one'), (2, 'two')", ())
            .expect("insert source rows");
        database
            .execute(
                "CREATE TABLE copied_rows AS SELECT id, payload FROM source_rows",
                (),
            )
            .expect("create table as select");
        database
            .execute(
                "CREATE VIEW copied_view AS SELECT id, payload FROM copied_rows",
                (),
            )
            .expect("create view");
        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM copied_view", ())
                .expect("query live view"),
            2
        );
        database.close().expect("close catalog database");

        let reopened = Database::open(&dsn).expect("reopen catalog database");
        assert_eq!(
            reopened
                .query_one::<i64, _>("SELECT COUNT(*) FROM copied_rows", ())
                .expect("query reopened CTAS table"),
            2
        );
        assert_eq!(
            reopened
                .query_one::<i64, _>("SELECT COUNT(*) FROM copied_view", ())
                .expect("query reopened view"),
            2
        );
        reopened
            .execute("DROP VIEW copied_view", ())
            .expect("drop view");
        assert!(reopened.query("SELECT * FROM copied_view", ()).is_err());
        reopened.close().expect("close dropped-view database");

        let reopened = Database::open(&dsn).expect("reopen dropped-view database");
        assert!(reopened.query("SELECT * FROM copied_view", ()).is_err());
        reopened.close().expect("close final database");
    }

    #[test]
    fn checkpoint_uses_the_actual_wal_generation_after_size_rotations() {
        fn selected_wal_floor(root: &std::path::Path) -> u64 {
            [
                ("CONTROL.0", radixdb_storage::v6::ControlSlotIndex::Zero),
                ("CONTROL.1", radixdb_storage::v6::ControlSlotIndex::One),
            ]
            .into_iter()
            .filter_map(|(name, slot)| {
                let bytes = std::fs::read(root.join(name)).ok()?;
                radixdb_storage::v6::decode_control_slot(&bytes, slot).ok()
            })
            .max_by_key(|control| control.database_generation())
            .expect("at least one valid CONTROL")
            .wal_replay_floor()
            .generation()
            .get()
        }

        fn active_wal_generations(root: &std::path::Path) -> Vec<u64> {
            let mut generations = std::fs::read_dir(root.join("wal"))
                .expect("read WAL directory")
                .filter_map(std::result::Result::ok)
                .filter_map(|entry| {
                    let name = entry.file_name();
                    let name = name.to_str()?;
                    let encoded = name.strip_prefix("wal-")?.strip_suffix(".log")?;
                    (encoded.len() == 16)
                        .then(|| u64::from_str_radix(encoded, 16).ok())
                        .flatten()
                })
                .collect::<Vec<_>>();
            generations.sort_unstable();
            generations
        }

        let directory = tempfile::tempdir().expect("create database directory");
        let database_path = directory.path().join("rotated-wal-checkpoint");
        let dsn = format!(
            "file://{}?checkpoint_on_close=false&checkpoint_interval=0&wal_max_size=512",
            database_path.display()
        );
        let database = Database::open(&dsn).expect("open persistent database");
        database
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .expect("create table");
        for id in 1..=12_i64 {
            database
                .execute(
                    "INSERT INTO items VALUES (?, ?)",
                    (id, format!("{id:04}-{}", "x".repeat(512))),
                )
                .expect("insert rotation row");
        }

        let wal_files_before_checkpoint = std::fs::read_dir(database_path.join("wal"))
            .expect("read WAL directory")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("wal-") && name.ends_with(".log"))
            })
            .count();
        assert!(
            wal_files_before_checkpoint >= 3,
            "test did not create multiple WAL rotations"
        );

        radixdb_storage::traits::Engine::force_checkpoint_cycle(database.engine().as_ref())
            .expect("publish checkpoint at actual WAL generation");
        let first_checkpoint_floor = selected_wal_floor(&database_path);
        assert!(
            first_checkpoint_floor > 1,
            "checkpoint did not retain the actual rotated generation"
        );

        for id in 13..=24_i64 {
            database
                .execute(
                    "INSERT INTO items VALUES (?, ?)",
                    (id, format!("{id:04}-{}", "y".repeat(512))),
                )
                .expect("insert second rotation row");
        }
        radixdb_storage::traits::Engine::force_checkpoint_cycle(database.engine().as_ref())
            .expect("publish second checkpoint and retire old rotation prefix");
        let active_generations = active_wal_generations(&database_path);
        assert!(
            active_generations
                .iter()
                .all(|generation| *generation >= first_checkpoint_floor),
            "WAL generations below retained floor {first_checkpoint_floor} leaked: {active_generations:?}"
        );
        database.close().expect("close database");

        let reopened = Database::open(&dsn).expect("reopen rotated WAL database");
        let rows = reopened
            .query("SELECT COUNT(*) FROM items", ())
            .expect("count recovered rows")
            .collect_vec()
            .expect("collect count row");
        let count = rows
            .first()
            .expect("count row")
            .get::<i64>(0)
            .expect("count value");
        assert_eq!(count, 24);
        reopened.close().expect("close reopened database");
    }

    #[test]
    fn compacted_tier_and_row_precedence_survive_reopen() {
        fn data_artifact_count(root: &std::path::Path) -> usize {
            let mut count = 0;
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                for entry in std::fs::read_dir(directory).expect("read artifact directory") {
                    let entry = entry.expect("read artifact member");
                    let path = entry.path();
                    if entry.file_type().expect("read artifact type").is_dir() {
                        pending.push(path);
                    } else if path.extension().and_then(|value| value.to_str()) == Some("data") {
                        count += 1;
                    }
                }
            }
            count
        }

        let directory = tempfile::tempdir().expect("create database directory");
        let database_path = directory.path().join("canonical-compaction");
        let dsn = format!(
            "file://{}?checkpoint_on_close=false&checkpoint_interval=0&compact_threshold=2&target_volume_rows=65536",
            database_path.display()
        );
        let database = Database::open(&dsn).expect("open persistent database");
        database
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .expect("create table");
        database
            .execute("INSERT INTO items VALUES (1, 'old'), (2, 'two')", ())
            .expect("insert first generation");
        radixdb_storage::traits::Engine::force_checkpoint_cycle(database.engine().as_ref())
            .expect("publish first L0 generation");
        database
            .execute("UPDATE items SET payload = 'new' WHERE id = 1", ())
            .expect("update overlapping row");
        database
            .execute("INSERT INTO items VALUES (3, 'three')", ())
            .expect("insert second generation");
        radixdb_storage::traits::Engine::force_checkpoint_cycle(database.engine().as_ref())
            .expect("publish second L0 generation");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let stats = database.engine().runtime_stats_snapshot();
            if stats.cold_l1_segments == 1
                && stats.cold_l0_segments == 0
                && stats.cold_unleveled_segments == 0
                && !stats.compaction_running
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "canonical compaction did not settle: {stats:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let rows = database
            .query("SELECT id, payload FROM items ORDER BY id", ())
            .expect("query compacted rows")
            .collect_vec()
            .expect("collect compacted rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<i64>(0).expect("first id"), 1);
        assert_eq!(rows[0].get::<String>(1).expect("updated payload"), "new");
        database.close().expect("close compacted database");

        let artifact_count_before_reopen = data_artifact_count(&database_path);
        let reopened = Database::open(&dsn).expect("reopen compacted database");
        let recovered_stats = reopened.engine().runtime_stats_snapshot();
        assert_eq!(recovered_stats.cold_unleveled_segments, 0);
        assert_eq!(recovered_stats.cold_l0_segments, 0);
        assert_eq!(recovered_stats.cold_l1_segments, 1);
        let rows = reopened
            .query("SELECT id, payload FROM items ORDER BY id", ())
            .expect("query recovered compacted rows")
            .collect_vec()
            .expect("collect recovered compacted rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get::<i64>(0).expect("first id"), 1);
        assert_eq!(rows[0].get::<String>(1).expect("recovered payload"), "new");

        radixdb_storage::traits::Engine::force_checkpoint_cycle(reopened.engine().as_ref())
            .expect("request post-recovery maintenance");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let stats = reopened.engine().runtime_stats_snapshot();
            if stats.maintenance.compaction.completed >= 1 && !stats.compaction_running {
                assert_eq!(stats.cold_unleveled_segments, 0);
                assert_eq!(stats.cold_l0_segments, 0);
                assert_eq!(stats.cold_l1_segments, 1);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "post-recovery compaction check did not settle: {stats:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        reopened.close().expect("close reopened database");
        assert!(
            data_artifact_count(&database_path) <= artifact_count_before_reopen,
            "stable L1 recovery must not publish another DATA artifact"
        );
    }

    #[test]
    fn named_query_timeout_preserves_named_parameter_binding() {
        let database = Database::open_in_memory().expect("open database");
        database
            .execute("CREATE TABLE named_timeout (id INTEGER PRIMARY KEY)", ())
            .expect("create table");
        database
            .execute("INSERT INTO named_timeout VALUES (7)", ())
            .expect("insert row");

        let mut rows = database
            .query_named_with_timeout(
                "SELECT id FROM named_timeout WHERE id = :id",
                named_params! { id: 7 },
                1_000,
            )
            .expect("query with named deadline");
        assert!(rows.advance());
        assert_eq!(rows.current_row().unwrap().get(0), Some(&Value::Integer(7)));
        assert!(!rows.advance());
        assert!(rows.error().is_none());
    }

    #[test]
    fn r3_l02_batch_b_same_dsn_opens_have_independent_sql_transactions() {
        let dsn = "memory://r3-l02-b-independent-opens";
        let first = Database::open(dsn).expect("open first connection");
        first
            .execute(
                "CREATE TABLE batch_b_connection (id INTEGER PRIMARY KEY, value INTEGER)",
                (),
            )
            .expect("create table");
        let second = Database::open(dsn).expect("open second connection");

        first.execute("BEGIN", ()).expect("begin first connection");
        first
            .execute("INSERT INTO batch_b_connection VALUES (1, 10)", ())
            .expect("insert in first connection");
        let visible_to_second: i64 = second
            .query_one("SELECT COUNT(*) FROM batch_b_connection", ())
            .expect("query second connection");
        assert_eq!(
            visible_to_second, 0,
            "a separately opened connection must not inherit the first connection's SQL transaction"
        );
        first.execute("ROLLBACK", ()).expect("rollback first");
    }

    #[test]
    fn r3_l02_batch_b_engine_get_index_returns_the_existing_public_index() {
        use radixdb_storage::Engine;

        let database = Database::open_in_memory().expect("open database");
        database
            .execute(
                "CREATE TABLE batch_b_index (id INTEGER PRIMARY KEY, value INTEGER)",
                (),
            )
            .expect("create table");
        database
            .execute(
                "CREATE INDEX idx_batch_b_value ON batch_b_index (value)",
                (),
            )
            .expect("create index");

        let index = Engine::get_index(
            database.engine().as_ref(),
            "batch_b_index",
            "idx_batch_b_value",
        )
        .expect("public Engine::get_index must return an existing index");
        assert_eq!(index.name(), "idx_batch_b_value");
    }

    #[test]
    fn r2_l05_c_independent_database_open_does_not_wait_for_another_dsn() {
        use std::sync::mpsc;
        use std::time::Duration;

        let temp = tempfile::tempdir().expect("temp dir");
        let slow_dsn = format!("file://{}", temp.path().join("slow").display());
        let fast_dsn = format!("file://{}", temp.path().join("fast").display());
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release_for_hook = Arc::clone(&release);
        let slow_for_hook = slow_dsn.clone();
        let open_hook = DatabaseOpenTestHookGuard::install(Arc::new(move |dsn| {
            if dsn != slow_for_hook {
                return;
            }
            entered_tx.send(()).expect("report slow open admission");
            let (lock, changed) = &*release_for_hook;
            let mut released = lock.lock().expect("release lock");
            while !*released {
                released = changed.wait(released).expect("release wait");
            }
        }));

        let slow_waiter_dsn = slow_dsn.clone();
        let slow = std::thread::spawn(move || Database::open(&slow_dsn));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("slow open reached deterministic recovery hook");

        let same_dsn_waiter = std::thread::spawn(move || Database::open(&slow_waiter_dsn));

        let (fast_tx, fast_rx) = mpsc::channel();
        let fast = std::thread::spawn(move || {
            fast_tx
                .send(Database::open(&fast_dsn))
                .expect("report independent open result");
        });
        let independent_completed = fast_rx.recv_timeout(Duration::from_millis(250));
        let completed_before_release = independent_completed.is_ok();

        {
            let (lock, changed) = &*release;
            *lock.lock().expect("release lock") = true;
            changed.notify_all();
        }
        let slow_database = slow
            .join()
            .expect("slow open thread")
            .expect("slow database opens");
        let same_dsn_database = same_dsn_waiter
            .join()
            .expect("same DSN waiter thread")
            .expect("same DSN waiter receives open outcome");
        let fast_database = match independent_completed {
            Ok(result) => result.expect("independent database opens"),
            Err(_) => fast_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("independent open eventually reports")
                .expect("independent database opens after release"),
        };
        fast.join().expect("independent open thread");
        drop(open_hook);

        assert!(Arc::ptr_eq(
            slow_database.owner_arc(),
            same_dsn_database.owner_arc()
        ));
        drop(same_dsn_database);
        slow_database.close().expect("close slow database");
        fast_database.close().expect("close fast database");
        assert!(
            completed_before_release,
            "opening one DSN held the process-wide registry lock across recovery"
        );

        let blocked_parent = temp.path().join("not-a-directory");
        std::fs::write(&blocked_parent, b"registry failure oracle")
            .expect("create non-directory parent");
        let failed_dsn = format!("file://{}", blocked_parent.join("database").display());
        let failed_retry_dsn = failed_dsn.clone();
        let failed_registry_key =
            Database::canonical_registry_key(&failed_dsn).expect("canonical failed-open key");
        let failed_waiter_dsn = failed_dsn.clone();
        let (failed_entered_tx, failed_entered_rx) = mpsc::channel();
        let failed_release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let failed_release_for_hook = Arc::clone(&failed_release);
        let failed_for_hook = failed_dsn.clone();
        let failure_hook = DatabaseOpenTestHookGuard::install(Arc::new(move |dsn| {
            if dsn != failed_for_hook {
                return;
            }
            failed_entered_tx
                .send(())
                .expect("report failed open admission");
            let (lock, changed) = &*failed_release_for_hook;
            let mut released = lock.lock().expect("failure release lock");
            while !*released {
                released = changed.wait(released).expect("failure release wait");
            }
        }));
        let failed_owner = std::thread::spawn(move || Database::open(&failed_dsn));
        failed_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("failed open reached hook");
        let failed_waiter = std::thread::spawn(move || Database::open(&failed_waiter_dsn));
        let waiter_deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let waiter_joined = DATABASE_REGISTRY
                .read()
                .expect("registry read")
                .get(&failed_registry_key)
                .is_some_and(|entry| {
                    matches!(
                        entry,
                        DatabaseRegistryEntry::Opening(slot) if Arc::strong_count(slot) >= 3
                    )
                });
            if waiter_joined {
                break;
            }
            assert!(
                std::time::Instant::now() < waiter_deadline,
                "same DSN waiter did not join opening slot"
            );
            std::thread::yield_now();
        }
        {
            let (lock, changed) = &*failed_release;
            *lock.lock().expect("failure release lock") = true;
            changed.notify_all();
        }
        let owner_error = match failed_owner.join().expect("failed owner thread") {
            Ok(_) => panic!("owner open through a non-directory parent must fail"),
            Err(error) => error,
        };
        let waiter_error = match failed_waiter.join().expect("failed waiter thread") {
            Ok(_) => panic!("waiter open through a non-directory parent must fail"),
            Err(error) => error,
        };
        drop(failure_hook);
        assert_eq!(owner_error.to_string(), waiter_error.to_string());
        assert!(Database::open(&failed_retry_dsn).is_err());
    }

    #[test]
    fn r2_l05_a_database_default_is_connection_local_and_explicit_isolation_wins() {
        let db = Database::open_in_memory().expect("open database");
        db.set_default_isolation_level(IsolationLevel::SnapshotIsolation)
            .expect("set default isolation");

        let default_tx = db.begin().expect("begin with database default");
        assert_eq!(
            db.engine().registry().get_isolation_level(default_tx.id()),
            IsolationLevel::SnapshotIsolation
        );

        let explicit_tx = db
            .begin_with_isolation(IsolationLevel::ReadCommitted)
            .expect("begin explicit read committed");
        assert_eq!(
            db.engine().registry().get_isolation_level(explicit_tx.id()),
            IsolationLevel::ReadCommitted
        );

        let clone = db.clone();
        let clone_default = clone.begin().expect("clone uses its local default");
        assert_eq!(
            db.engine()
                .registry()
                .get_isolation_level(clone_default.id()),
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn r2_l05_b_failed_close_is_retained_until_retry_and_fresh_open() {
        let _failpoint_guard = radixdb_storage::test_failpoints::FailpointGuard::new();
        radixdb_storage::test_failpoints::reset_all();
        let temp = tempfile::tempdir().expect("temp dir");
        let dsn = format!("file://{}", temp.path().join("close-retry").display());
        let db = Database::open(&dsn).expect("open database");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1)", ())
            .expect("insert durable row");

        radixdb_storage::test_failpoints::WAL_SYNC_FAIL
            .store(true, std::sync::atomic::Ordering::Release);
        let close_error = db.close().expect_err("WAL failure must fail close");
        let reopen_error = match Database::open(&dsn) {
            Ok(_) => panic!("close-failed registry owner must not reopen as ready"),
            Err(error) => error,
        };
        assert_eq!(close_error.to_string(), reopen_error.to_string());

        radixdb_storage::test_failpoints::WAL_SYNC_FAIL
            .store(false, std::sync::atomic::Ordering::Release);
        db.close().expect("retry close succeeds");
        let reopened = Database::open(&dsn).expect("fresh engine opens after terminal close");
        assert!(!Arc::ptr_eq(db.owner_arc(), reopened.owner_arc()));
        let rows = reopened
            .query("SELECT id FROM t", ())
            .expect("query recovered row")
            .collect_vec()
            .expect("collect recovered row");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<i64>(0).expect("recovered id"), 1);
        reopened.close().expect("close fresh engine");
        radixdb_storage::test_failpoints::reset_all();
    }

    #[test]
    fn test_open_memory() {
        let db = Database::open("memory://").unwrap();
        assert_eq!(db.dsn(), "memory://");
    }

    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute("INSERT INTO test VALUES ($1)", (1,)).unwrap();

        for row in db.query("SELECT * FROM test", ()).unwrap() {
            let row = row.unwrap();
            let id: i64 = row.get(0).unwrap();
            assert_eq!(id, 1);
        }
    }

    #[test]
    fn test_execute_and_query_new_api() {
        let db = Database::open_in_memory().unwrap();

        // Create table - no params
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            (),
        )
        .unwrap();

        // Insert with tuple params
        let affected = db
            .execute(
                "INSERT INTO users VALUES ($1, $2, $3), ($4, $5, $6)",
                (1, "Alice", 30, 2, "Bob", 25),
            )
            .unwrap();
        assert_eq!(affected, 2);

        // Query with tuple params
        let rows: Vec<_> = db
            .query("SELECT * FROM users ORDER BY id", ())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<i64>(0).unwrap(), 1);
        assert_eq!(rows[0].get::<String>(1).unwrap(), "Alice");
        assert_eq!(rows[0].get::<i64>(2).unwrap(), 30);
    }

    #[test]
    fn test_query_one() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute("INSERT INTO test VALUES ($1), ($2), ($3)", (1, 2, 3))
            .unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_query_opt() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute("INSERT INTO test VALUES ($1)", (1,)).unwrap();

        // Found
        let result: Option<i64> = db
            .query_opt("SELECT id FROM test WHERE id = $1", (1,))
            .unwrap();
        assert_eq!(result, Some(1));

        // Not found
        let result: Option<i64> = db
            .query_opt("SELECT id FROM test WHERE id = $1", (999,))
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_params_macro() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();

        // Use params! macro
        db.execute(
            "INSERT INTO users VALUES ($1, $2)",
            crate::params![1, "Alice"],
        )
        .unwrap();

        let names: Vec<String> = db
            .query("SELECT name FROM users WHERE id = $1", crate::params![1])
            .unwrap()
            .map(|r| r.and_then(|row| row.get(0)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(names, vec!["Alice"]);
    }

    #[test]
    fn test_parse_dsn() {
        // Memory
        let (scheme, path) = Database::parse_dsn("memory://").unwrap();
        assert_eq!(scheme, "memory");
        assert_eq!(path, "");

        // File
        let (scheme, path) = Database::parse_dsn("file:///tmp/test.db").unwrap();
        assert_eq!(scheme, "file");
        assert_eq!(path, "/tmp/test.db");

        // File with params
        let (scheme, path) = Database::parse_dsn("file:///tmp/test.db?sync=full").unwrap();
        assert_eq!(scheme, "file");
        assert_eq!(path, "/tmp/test.db?sync=full");

        // Invalid
        assert!(Database::parse_dsn("invalid").is_err());
        assert!(Database::parse_dsn("unknown://test").is_err());
    }

    #[test]
    fn test_parse_file_config_seal_hot_bytes() {
        let (_path, config) = Database::parse_file_config(
            "/tmp/test.db?copy_max_transaction_bytes=16384&max_compaction_jobs=4&storage_cpu_workers=3&page_cache_level=5&page_cache_max_bytes=1073741824&page_cache_memory_reserve=536870912&max_compaction_input_segments=6&max_compaction_input_bytes=33554432&max_compaction_output_bytes=50331648&compaction_job_time_budget_ms=30000&compaction_io_bytes_per_sec=8388608&compaction_disk_reserve_bytes=16777216&compaction_retry_cooldown_ms=5000&l0_soft_limit_segments=10&l0_hard_limit_segments=20&l0_soft_limit_bytes=67108864&l0_hard_limit_bytes=134217728&l0_soft_backpressure_wait_ms=25&seal_hot_bytes_threshold=1024&seal_incremental_hot_bytes_threshold=512&volume_cache_bytes=2048&read_queue_depth=8",
        )
        .unwrap();

        assert_eq!(config.persistence.copy_max_transaction_bytes, 16_384);
        assert_eq!(config.persistence.max_compaction_jobs, 4);
        assert_eq!(config.persistence.storage_cpu_workers, 3);
        assert_eq!(config.persistence.page_cache_level, 5);
        assert_eq!(config.persistence.page_cache_max_bytes, 1_073_741_824);
        assert_eq!(config.persistence.page_cache_memory_reserve, 536_870_912);
        assert_eq!(config.persistence.max_compaction_input_segments, 6);
        assert_eq!(config.persistence.max_compaction_input_bytes, 33_554_432);
        assert_eq!(config.persistence.max_compaction_output_bytes, 50_331_648);
        assert_eq!(config.persistence.compaction_job_time_budget_ms, 30_000);
        assert_eq!(config.persistence.compaction_io_bytes_per_sec, 8_388_608);
        assert_eq!(config.persistence.compaction_disk_reserve_bytes, 16_777_216);
        assert_eq!(config.persistence.compaction_retry_cooldown_ms, 5_000);
        assert_eq!(config.persistence.l0_soft_limit_segments, 10);
        assert_eq!(config.persistence.l0_hard_limit_segments, 20);
        assert_eq!(config.persistence.l0_soft_limit_bytes, 67_108_864);
        assert_eq!(config.persistence.l0_hard_limit_bytes, 134_217_728);
        assert_eq!(config.persistence.l0_soft_backpressure_wait_ms, 25);
        assert_eq!(config.persistence.seal_hot_bytes_threshold, 1024);
        assert_eq!(config.persistence.seal_incremental_hot_bytes_threshold, 512);
        assert_eq!(config.persistence.volume_cache_bytes, 2048);
        assert_eq!(config.persistence.read_queue_depth, 8);

        let error = Database::parse_file_config(
            "/tmp/test.db?l0_soft_limit_segments=8&l0_hard_limit_segments=8",
        )
        .expect_err("unordered L0 segment limits must fail closed");
        assert!(error.to_string().contains("0 < soft < hard"));

        let error = Database::parse_file_config("/tmp/test.db?max_compaction_jobs=0")
            .expect_err("zero compaction workers must fail closed");
        assert!(error.to_string().contains("max_compaction_jobs must be in"));

        let error = Database::parse_file_config("/tmp/test.db?page_cache_level=11")
            .expect_err("page cache levels above ten must fail closed");
        assert!(error.to_string().contains("page_cache_level must be in"));
    }

    #[test]
    fn file_config_rejects_retired_aliases_and_duplicate_canonical_options() {
        for alias in [
            "sync=full",
            "snapshot_interval=60",
            "sync_interval=10",
            "snapshot_compression=on",
            "seal_hot_bytes=1024",
            "seal_incremental_hot_bytes=512",
            "volume_cache=2048",
            "compression_threshold=64",
            "scan_prefetch_cache_bytes=4096",
            "block_cache_bytes=8192",
        ] {
            let error = Database::parse_file_config(&format!("/tmp/radix?{alias}"))
                .expect_err("retired DSN alias must fail closed");
            assert!(error.to_string().contains("unknown file database option"));
        }

        let error = Database::parse_file_config("/tmp/radix?sync_mode=full&sync_mode=none")
            .expect_err("duplicate canonical option must not be order-dependent");
        assert!(error.to_string().contains("duplicate file database option"));
    }

    #[test]
    fn r2_l06_batch_a_rejects_obsolete_commit_batch_size() {
        let error = Database::parse_file_config("/tmp/radix?commit_batch_size=2")
            .expect_err("r2_l06_batch_a: removed durability knob must not be accepted");
        assert!(error.to_string().contains("commit_batch_size"));
    }

    #[test]
    fn r2_l06_batch_b_restore_never_commits_before_fallible_cache_admission() {
        let dir = tempfile::tempdir().unwrap();
        let dsn = format!("file://{}", dir.path().join("restore_cache").display());
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.create_snapshot().unwrap();
        db.execute("INSERT INTO t VALUES (1)", ()).unwrap();

        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _executor = db.inner.executor.lock().unwrap();
            panic!("poison executor before restore");
        }));
        assert!(poisoned.is_err());
        assert!(db.restore_snapshot(None).is_err());
        db.close().unwrap();
        drop(db);

        let reopened = Database::open(&dsn).unwrap();
        let count: i64 = reopened.query_one("SELECT COUNT(*) FROM t", ()).unwrap();
        assert_eq!(
            count, 1,
            "restore must not replace durable state before cache admission can succeed"
        );
        reopened.close().unwrap();
    }

    #[test]
    fn test_from_value_types() {
        assert_eq!(i64::from_value(&Value::Integer(42)).unwrap(), 42);
        assert_eq!(f64::from_value(&Value::Float(3.5)).unwrap(), 3.5);
        assert_eq!(
            f64::from_value(&Value::decimal(12_345, 5, 2)).unwrap(),
            123.45
        );
        assert_eq!(
            String::from_value(&Value::Text("hello".into())).unwrap(),
            "hello"
        );
        assert!(bool::from_value(&Value::Boolean(true)).unwrap());

        // Optional
        assert_eq!(
            Option::<i64>::from_value(&Value::Integer(42)).unwrap(),
            Some(42)
        );
        assert_eq!(
            Option::<i64>::from_value(&Value::null_unknown()).unwrap(),
            None
        );
    }

    #[test]
    fn test_cached_plan_insert_and_query() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)",
            (),
        )
        .unwrap();

        let insert_plan = db
            .cached_plan("INSERT INTO test VALUES ($1, $2, $3)")
            .unwrap();

        // Batch insert using cached plan
        db.execute_plan(&insert_plan, (1, "Alice", 95.5)).unwrap();
        db.execute_plan(&insert_plan, (2, "Bob", 82.0)).unwrap();
        db.execute_plan(&insert_plan, (3, "Charlie", 91.0)).unwrap();

        // Query using cached plan
        let query_plan = db
            .cached_plan("SELECT name FROM test WHERE id = $1")
            .unwrap();
        let mut rows = db.query_plan(&query_plan, (2,)).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "Bob");
    }

    #[test]
    fn test_cached_plan_reuse() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();

        // Get the same plan twice — second call should hit the cache
        let plan1 = db.cached_plan("INSERT INTO test VALUES ($1, $2)").unwrap();
        let plan2 = db.cached_plan("INSERT INTO test VALUES ($1, $2)").unwrap();

        // Both should work independently
        db.execute_plan(&plan1, (1, 100)).unwrap();
        db.execute_plan(&plan2, (2, 200)).unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_cached_plan_update_delete() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO test VALUES (2, 200)", ()).unwrap();

        // Update via cached plan
        let update_plan = db
            .cached_plan("UPDATE test SET value = $1 WHERE id = $2")
            .unwrap();
        let affected = db.execute_plan(&update_plan, (999, 1)).unwrap();
        assert_eq!(affected, 1);

        let val: i64 = db
            .query_one("SELECT value FROM test WHERE id = 1", ())
            .unwrap();
        assert_eq!(val, 999);

        // Delete via cached plan
        let delete_plan = db.cached_plan("DELETE FROM test WHERE id = $1").unwrap();
        let affected = db.execute_plan(&delete_plan, (2,)).unwrap();
        assert_eq!(affected, 1);

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_cached_plan_no_params() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES (1, 10)", ()).unwrap();
        db.execute("INSERT INTO test VALUES (2, 20)", ()).unwrap();

        let plan = db.cached_plan("SELECT COUNT(*) FROM test").unwrap();
        let mut rows = db.query_plan(&plan, ()).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 2);
    }

    #[test]
    fn test_cached_plan_named_params() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();

        let plan = db
            .cached_plan("INSERT INTO test VALUES (:id, :name)")
            .unwrap();
        db.execute_named_plan(&plan, named_params! { id: 1, name: "Alice" })
            .unwrap();
        db.execute_named_plan(&plan, named_params! { id: 2, name: "Bob" })
            .unwrap();

        let query_plan = db
            .cached_plan("SELECT name FROM test WHERE id = :id")
            .unwrap();
        let mut rows = db
            .query_named_plan(&query_plan, named_params! { id: 1 })
            .unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "Alice");
    }

    #[test]
    fn test_cached_plan_multi_statement_error() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        // Multiple statements should fail
        let result = db.cached_plan("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2)");
        assert!(result.is_err());
    }
}
