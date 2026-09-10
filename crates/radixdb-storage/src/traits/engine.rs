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

//! Engine trait for the storage engine
//!

use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::config::Config;
use crate::traits::{Index, Transaction};
use radixdb_core::{
    CompactArc, Error, IsolationLevel, NavigationErrorCode, ReferenceDescriptor,
    ReferenceTargetKey, Result, RowVec, Schema, SchemaColumnId, SchemaTableId,
};

/// Stable, machine-readable identity of a committed physical snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalSnapshotIdentity {
    pub snapshot_id: String,
    pub database_id: String,
    pub physical_format_major: u16,
    pub physical_format_minor: u16,
}

/// Engine represents the storage engine
///
/// This is the main entry point for interacting with the database.
/// It manages transactions, tables, indexes, and persistence.
///
/// # Example
///
/// ```ignore
/// let config = Config::with_path("/tmp/mydb");
/// let mut engine = MvccEngine::new(config);
/// engine.open()?;
///
/// let tx = engine.begin_transaction()?;
/// // ... perform operations ...
/// tx.commit()?;
///
/// engine.close()?;
/// ```
pub trait Engine: Send + Sync {
    /// Opens the storage engine
    ///
    /// This initializes the engine, opens the database path (if any),
    /// recovers from WAL, and loads existing data.
    fn open(&mut self) -> Result<()>;

    /// Closes the storage engine
    ///
    /// This flushes pending writes, seals all hot rows into cold volumes
    /// (when checkpoint_on_close is enabled), and releases all resources.
    fn close(&mut self) -> Result<()>;

    /// Begins a new transaction
    ///
    /// The transaction will use the engine's default isolation level.
    fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;

    /// Begins a new transaction with a specific isolation level
    ///
    /// # Arguments
    /// * `level` - The isolation level for the transaction
    fn begin_transaction_with_level(&self, level: IsolationLevel) -> Result<Box<dyn Transaction>>;

    /// Returns the path to the database directory
    ///
    /// Returns `None` if operating in memory-only mode.
    fn path(&self) -> Option<&str>;

    /// Checks if a table exists
    fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// Checks if an index exists
    fn index_exists(&self, index_name: &str, table_name: &str) -> Result<bool>;

    /// Gets an index by name
    fn get_index(&self, table_name: &str, index_name: &str) -> Result<Arc<dyn Index>>;

    /// Gets the schema for a table
    ///
    /// Returns an Arc to avoid cloning the schema on every access.
    /// This is a critical optimization for hot paths like PK lookups.
    fn get_table_schema(&self, table_name: &str) -> Result<CompactArc<Schema>>;

    /// Gets the current schema epoch
    ///
    /// This is a monotonically increasing counter that increments on any
    /// CREATE TABLE, ALTER TABLE, or DROP TABLE operation. Used for fast
    /// cache invalidation without HashMap lookup (~1ns vs ~7ns).
    fn schema_epoch(&self) -> u64;

    /// Process-local identity of this engine's schema registry. Bound IDs from
    /// another database must never be accepted as local catalog identities.
    fn schema_scope_id(&self) -> u64;

    /// Bind an authoritative table name to a generation-scoped runtime ID.
    fn bind_schema_table_id(&self, table_name: &str) -> Result<SchemaTableId> {
        let generation = self.schema_epoch();
        let schema = self.get_table_schema(table_name)?;
        let id = SchemaTableId::new(
            self.schema_scope_id(),
            generation,
            schema.table_name().to_lowercase(),
        );
        if self.schema_epoch() != generation {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!("schema generation changed while binding table '{table_name}'"),
            ));
        }
        Ok(id)
    }

    /// Bind a column name under an already-bound table identity.
    fn bind_schema_column_id(
        &self,
        table: &SchemaTableId,
        column_name: &str,
    ) -> Result<SchemaColumnId> {
        self.validate_schema_table_id(table)?;
        let schema = self.get_table_schema(table.table_name())?;
        let (ordinal, column) = schema.find_column(column_name).ok_or_else(|| {
            Error::ColumnNotFound(format!("{}.{}", table.table_name(), column_name))
        })?;
        if column.id != ordinal {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!(
                    "column '{}.{}' has stale identity",
                    table.table_name(),
                    column_name
                ),
            ));
        }
        if self.schema_epoch() != table.schema_generation() {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!(
                    "schema generation changed while binding '{}.{}'",
                    table.table_name(),
                    column_name
                ),
            ));
        }
        Ok(SchemaColumnId::new(table.clone(), ordinal))
    }

    /// Resolve one FK column to its eligible navigation target.
    ///
    /// `Ok(None)` means the source column is not an FK. Unsupported or corrupt
    /// metadata is an error and must never be guessed into a path.
    fn get_reference_descriptor(
        &self,
        source: &SchemaColumnId,
    ) -> Result<Option<ReferenceDescriptor>> {
        self.validate_schema_table_id(source.table())?;
        let generation = source.table().schema_generation();
        let source_schema = self.get_table_schema(source.table().table_name())?;
        let source_column = source_schema.get_column(source.ordinal()).ok_or_else(|| {
            Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!(
                    "source column ordinal {} no longer exists in '{}'",
                    source.ordinal(),
                    source.table().table_name()
                ),
            )
        })?;
        if source_column.id != source.ordinal() {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                "source column identity is stale",
            ));
        }

        let mut constraints = source_schema
            .foreign_keys()
            .iter()
            .filter(|foreign_key| foreign_key.column_index == source.ordinal());
        let Some(foreign_key) = constraints.next() else {
            return Ok(None);
        };
        if constraints.next().is_some() {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!(
                    "'{}.{}' has more than one reference target",
                    source.table().table_name(),
                    source_column.name
                ),
            ));
        }

        let target_schema = self
            .get_table_schema(&foreign_key.referenced_table)
            .map_err(|error| {
                Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    format!(
                        "target table '{}' is unavailable: {error}",
                        foreign_key.referenced_table
                    ),
                )
            })?;
        let (target_ordinal, target_column) = target_schema
            .find_column(&foreign_key.referenced_column)
            .ok_or_else(|| {
                Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    format!(
                        "target column '{}.{}' is unavailable",
                        foreign_key.referenced_table, foreign_key.referenced_column
                    ),
                )
            })?;
        if target_column.id != target_ordinal || source_column.data_type != target_column.data_type
        {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                "reference column identity or type is inconsistent",
            ));
        }

        let target_key = if target_column.primary_key
            && target_schema.primary_key_indices().len() == 1
        {
            ReferenceTargetKey::PrimaryKey
        } else {
            let unique_not_null = !target_column.nullable
                && self
                    .get_all_indexes(target_schema.table_name())?
                    .iter()
                    .any(|index| {
                        index.is_unique()
                            && index.partial_predicate().is_none()
                            && index.column_ids().len() == 1
                            && usize::try_from(index.column_ids()[0]).ok() == Some(target_ordinal)
                    });
            if !unique_not_null {
                return Err(Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    format!(
                        "target '{}.{}' is not PRIMARY KEY or UNIQUE NOT NULL",
                        target_schema.table_name(),
                        target_column.name
                    ),
                ));
            }
            ReferenceTargetKey::UniqueNotNull
        };

        if self.schema_epoch() != generation {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!(
                    "schema generation changed while resolving '{}.{}'",
                    source.table().table_name(),
                    source_column.name
                ),
            ));
        }
        let target_table = SchemaTableId::new(
            self.schema_scope_id(),
            generation,
            target_schema.table_name().to_lowercase(),
        );
        Ok(Some(ReferenceDescriptor::new(
            source.clone(),
            SchemaColumnId::new(target_table, target_ordinal),
            source_column.nullable,
            source_column.data_type,
            target_key,
        )))
    }

    /// Validate that a generation-scoped table ID still belongs to this exact
    /// engine/catalog snapshot.
    fn validate_schema_table_id(&self, table: &SchemaTableId) -> Result<()> {
        if table.scope_id() != self.schema_scope_id() {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                "cross-database reference identity",
            ));
        }
        let actual = self.schema_epoch();
        if table.schema_generation() != actual {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!(
                    "expected generation {}, found {actual}",
                    table.schema_generation()
                ),
            ));
        }
        Ok(())
    }

    /// Lists all indexes for a table
    ///
    /// Returns a map from index name to index type string.
    fn list_table_indexes(&self, table_name: &str) -> Result<FxHashMap<String, String>>;

    /// Gets all index objects for a table
    fn get_all_indexes(&self, table_name: &str) -> Result<Vec<std::sync::Arc<dyn Index>>>;

    /// Gets the current default isolation level
    fn get_isolation_level(&self) -> IsolationLevel;

    /// Sets the default isolation level for new transactions
    fn set_isolation_level(&mut self, level: IsolationLevel) -> Result<()>;

    /// Gets the current engine configuration
    ///
    /// Returns a clone of the configuration to avoid lifetime issues with internal locks.
    fn get_config(&self) -> Config;

    /// Updates the engine configuration
    ///
    /// Note: Some configuration changes may require a restart to take effect.
    fn update_config(&mut self, config: Config) -> Result<()>;

    /// Creates a physical backup of one complete catalog/data/index generation
    /// and its immutable WAL suffix under the `snapshots/` directory.
    /// `keep_snapshots` limits the number of complete database snapshots.
    fn create_snapshot(&self) -> Result<PhysicalSnapshotIdentity>;

    /// Creates a snapshot while observing a caller-owned cancellation signal.
    /// Engines without an incremental implementation retain the legacy atomic
    /// behavior; MVCC checks this signal between bounded write batches.
    fn create_snapshot_cancellable(
        &self,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<PhysicalSnapshotIdentity> {
        if is_cancelled() {
            return Err(radixdb_core::Error::QueryCancelled);
        }
        self.create_snapshot()
    }

    /// Restores the database state from one complete physical snapshot.
    /// If `snapshot_id` is `None`, restores the latest committed snapshot.
    /// Otherwise restores the snapshot with that exact stable identity.
    /// This is a destructive operation that replaces all current data.
    fn restore_snapshot(&self, _snapshot_id: Option<&str>) -> Result<String> {
        Err(radixdb_core::Error::internal(
            "restore_snapshot not supported by this engine",
        ))
    }

    /// Runs the checkpoint cycle: seal hot buffers to volumes, persist manifests,
    /// compact volumes, and truncate WAL. Used by the background checkpoint thread.
    /// Respects seal thresholds (only seals when hot buffer exceeds size limits).
    fn checkpoint_cycle(&self) -> Result<()>;

    /// Runs the checkpoint cycle with force seal: seals ALL hot rows into volumes
    /// regardless of threshold, then persists manifests, compacts, and truncates WAL.
    /// Used by PRAGMA CHECKPOINT and close_engine where all data must be durable.
    fn force_checkpoint_cycle(&self) -> Result<()>;

    /// Record TRUNCATE TABLE operation to WAL for persistence
    fn record_truncate_table(&self, table_name: &str) -> Result<()> {
        // Default implementation does nothing (for in-memory engines)
        let _ = table_name;
        Ok(())
    }

    /// Fetch rows by IDs directly from storage without creating a full transaction.
    ///
    /// This is an optimization for EXISTS subquery evaluation where we only need
    /// to check if rows exist and evaluate predicates. It avoids the ~2-5μs overhead
    /// of creating a new transaction per EXISTS probe.
    ///
    /// The returned rows represent the latest committed state visible to any reader.
    fn fetch_rows_by_ids(&self, table_name: &str, row_ids: &[i64]) -> Result<RowVec> {
        // Default implementation: fall back to creating a transaction
        // Concrete implementations can override for better performance
        let _ = (table_name, row_ids);
        Err(radixdb_core::Error::internal(
            "fetch_rows_by_ids not supported by this engine",
        ))
    }

    /// Get a cached row fetcher for a table.
    ///
    /// This returns a function that can be called repeatedly to fetch rows without
    /// the overhead of looking up the table each time. This is useful for EXISTS
    /// subquery evaluation where we probe the same table many times.
    #[allow(clippy::type_complexity)]
    fn get_row_fetcher(
        &self,
        table_name: &str,
    ) -> Result<Box<dyn Fn(&[i64]) -> Result<RowVec> + Send + Sync>> {
        // Default implementation: fall back to fetch_rows_by_ids
        let _ = table_name;
        Err(radixdb_core::Error::internal(
            "get_row_fetcher not supported by this engine",
        ))
    }

    /// Get a count-only function for counting visible rows by their IDs.
    /// This is optimized for COUNT(*) subqueries where we don't need the actual row data.
    #[allow(clippy::type_complexity)]
    fn get_row_counter(
        &self,
        table_name: &str,
    ) -> Result<Box<dyn Fn(&[i64]) -> usize + Send + Sync>> {
        let _ = table_name;
        Err(radixdb_core::Error::internal(
            "get_row_counter not supported by this engine",
        ))
    }
}

#[cfg(test)]
mod tests {
    // Engine tests will be implemented when we have concrete implementations
    // For now, just verify the trait compiles correctly

    use super::*;

    // Verify trait is object-safe
    fn _assert_object_safe(_: &dyn Engine) {}
}
