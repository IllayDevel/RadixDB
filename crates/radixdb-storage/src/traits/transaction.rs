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

//! Transaction trait for database transactions
//!

use rustc_hash::FxHashMap;

use radixdb_catalog::CatalogMutationSet;
use radixdb_core::{Error, IndexType, IsolationLevel, Result, Schema, SchemaColumn};

use crate::expression::Expression;
use crate::index::PartialIndexPredicate;
use crate::traits::{QueryResult, Table};

/// Catalog definition staged by CREATE INDEX inside an explicit transaction.
/// The physical index is built during commit while the DDL publication fence is
/// exclusive, then its WAL entry shares the user transaction's commit marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIndexDefinition {
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub index_type: Option<IndexType>,
    pub hnsw_m: Option<u16>,
    pub hnsw_ef_construction: Option<u16>,
    pub hnsw_ef_search: Option<u16>,
    pub hnsw_distance_metric: Option<u8>,
    /// Executor-bound predicate ready for storage index construction.
    /// Durable metadata is derived only when the committed definition is
    /// written to WAL/snapshot authority.
    pub partial_predicate: Option<PartialIndexPredicate>,
    /// Prepared external-key mapping. Storage invokes it only with values and
    /// keeps ownership of every index/MVCC/WAL operation.
    pub key_encoder: Option<crate::index::PreparedIndexKeyEncoder>,
}

/// Existing physical index removed atomically with a staged catalog change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIndexDrop {
    pub table_name: String,
    pub index_name: String,
    /// True when the schema replacement itself removes the derived PK index.
    /// Such entries are transaction-local visibility intents, not separate WAL
    /// DROP INDEX records.
    pub schema_owned: bool,
}

/// Existing physical index renamed atomically with its catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIndexRename {
    pub table_name: String,
    pub old_index_name: String,
    pub new_index_name: String,
}

/// Runtime table-name transition published atomically with its catalog rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTableRename {
    pub old_name: String,
    pub new_name: String,
}

/// Complete schema transition staged by ALTER TABLE inside a transaction.
///
/// The owning transaction plans and executes subsequent statements against
/// `schema`, while every other transaction continues to observe the catalog
/// snapshot in `expected_catalog_schema`.  Commit compares the expected
/// snapshot, records the complete replacement in WAL, and publishes the last
/// transition for each table only after the transaction marker is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSchemaChange {
    pub table_name: String,
    pub schema: Schema,
    /// Shared catalog schema observed before the first staged ALTER on this
    /// table. Commit rejects concurrent DDL instead of publishing a candidate
    /// derived from a stale catalog generation.
    pub expected_catalog_schema: Schema,
    /// Older physical rows require logical width normalization after ADD
    /// COLUMN. Constraint-only replacements leave the row layout untouched.
    pub requires_row_normalization: bool,
    /// Physical transform which cannot be represented by replacing the schema
    /// pointer alone. It is applied only after the shared commit marker.
    pub physical_transition: Option<SchemaPhysicalTransition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaPhysicalTransition {
    DropColumn {
        column_name: String,
        column_index: usize,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
}

/// Transaction represents a database transaction
///
/// This trait defines the interface for transaction operations including
/// DDL (table/index management), DML (select/insert/update/delete),
/// and transaction control (begin/commit/rollback).
///
/// # Example
///
/// ```ignore
/// let tx = engine.begin_transaction()?;
///
/// // Create a table
/// let schema = Schema::new("users").with_column("id", DataType::Integer, false);
/// tx.create_table("users", schema)?;
///
/// // Insert data
/// let table = tx.get_table("users")?;
/// table.insert(Row::from_values(vec![Value::Integer(1)]))?;
///
/// // Query data
/// let result = tx.select("users", &["id"], None, None)?;
///
/// tx.commit()?;
/// ```
pub trait Transaction: Send {
    /// Whether the transaction can still accept statements or explicit rollback.
    fn is_active(&self) -> bool {
        true
    }

    /// Begins the transaction
    fn begin(&mut self) -> Result<()>;

    /// Commits the transaction
    fn commit(&mut self) -> Result<()>;

    /// Rolls back the transaction
    fn rollback(&mut self) -> Result<()>;

    /// Creates a savepoint with the given name
    ///
    /// Records the current state so it can be rolled back to later.
    /// If a savepoint with this name already exists, it is overwritten.
    fn create_savepoint(&mut self, name: &str) -> Result<()>;

    /// Releases (removes) a savepoint without rolling back
    ///
    /// The changes made after the savepoint remain intact.
    fn release_savepoint(&mut self, name: &str) -> Result<()>;

    /// Rolls back to a savepoint, discarding all changes made after it
    ///
    /// The target savepoint remains valid after rollback; savepoints created
    /// after it are removed.
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<()>;

    /// Attach the one coalesced catalog mutation published by this transaction.
    ///
    /// The mutation shares the ordinary transaction commit marker. Storage
    /// rejects a second attachment instead of creating an implicit second
    /// catalog authority.
    fn stage_catalog_mutation(&mut self, _mutation: CatalogMutationSet) -> Result<()> {
        Err(Error::NotSupported(
            "transactional catalog mutation is not supported by this storage engine".to_string(),
        ))
    }

    /// Gets the timestamp associated with a savepoint
    ///
    /// Returns None if the savepoint doesn't exist.
    fn get_savepoint_timestamp(&self, name: &str) -> Option<i64>;

    /// Returns the transaction ID
    fn id(&self) -> i64;

    /// Sets the isolation level for this transaction
    fn set_isolation_level(&mut self, level: IsolationLevel) -> Result<()>;

    // ---- Table Operations ----

    /// Creates a new table with the given schema
    fn create_table(&mut self, name: &str, schema: Schema) -> Result<Box<dyn Table>>;

    /// Drops a table
    fn drop_table(&mut self, name: &str) -> Result<()>;

    /// Gets a reference to a table by name
    fn get_table(&self, name: &str) -> Result<Box<dyn Table>>;

    /// Lists all table names
    fn list_tables(&self) -> Result<Vec<String>>;

    /// Renames a table
    fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()>;

    // ---- Index Operations ----

    /// Creates an index on a table
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `index_name` - Name for the new index
    /// * `columns` - Column names to include in the index
    /// * `is_unique` - Whether this is a unique index
    fn create_table_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: &[String],
        is_unique: bool,
    ) -> Result<()>;

    /// Stage a full CREATE INDEX definition for atomic commit publication.
    fn stage_create_index(&mut self, _definition: PendingIndexDefinition) -> Result<()> {
        Err(Error::NotSupported(
            "transactional CREATE INDEX is not supported by this storage engine".to_string(),
        ))
    }

    /// Index definitions staged by this transaction and visible only to it.
    fn staged_index_definitions(&self, _table_name: &str) -> Vec<PendingIndexDefinition> {
        Vec::new()
    }

    /// Stage DROP INDEX for the transaction commit boundary.
    fn stage_drop_index(&mut self, _drop: PendingIndexDrop) -> Result<()> {
        Err(Error::NotSupported(
            "transactional DROP INDEX is not supported by this storage engine".to_string(),
        ))
    }

    /// Stage ALTER INDEX RENAME for the transaction commit boundary.
    fn stage_rename_index(&mut self, _rename: PendingIndexRename) -> Result<()> {
        Err(Error::NotSupported(
            "transactional ALTER INDEX is not supported by this storage engine".to_string(),
        ))
    }

    /// Drops an index from a table
    fn drop_table_index(&mut self, table_name: &str, index_name: &str) -> Result<()>;

    /// Creates a btree index on a table column
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `column_name` - Name of the column to index
    /// * `is_unique` - Whether this is a unique index
    /// * `custom_name` - Optional custom name for the index
    fn create_table_btree_index(
        &mut self,
        table_name: &str,
        column_name: &str,
        is_unique: bool,
        custom_name: Option<&str>,
    ) -> Result<()>;

    /// Drops a btree index from a table
    fn drop_table_btree_index(&mut self, table_name: &str, column_name: &str) -> Result<()>;

    // ---- Column Operations (ALTER TABLE) ----

    /// Adds a column to a table
    fn add_table_column(&mut self, table_name: &str, column: SchemaColumn) -> Result<()>;

    /// Stages a complete schema replacement for ALTER TABLE.
    ///
    /// Engines that support transactional DDL keep this schema private until
    /// commit. `requires_row_normalization` is true when existing physical rows
    /// must be widened after publication (for example ADD COLUMN).
    fn stage_table_schema_change(
        &mut self,
        _table_name: &str,
        _schema: Schema,
        _requires_row_normalization: bool,
    ) -> Result<()> {
        Err(Error::NotSupported(
            "transactional ALTER TABLE schema replacement is not supported by this storage engine"
                .to_string(),
        ))
    }

    /// Drops a column from a table
    fn drop_table_column(&mut self, table_name: &str, column_name: &str) -> Result<()>;

    /// Renames a column in a table
    fn rename_table_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()>;

    /// Stage a complete schema together with the physical transform required
    /// to publish it after commit.
    fn stage_table_schema_transition(
        &mut self,
        table_name: &str,
        schema: Schema,
        requires_row_normalization: bool,
        transition: SchemaPhysicalTransition,
    ) -> Result<()>;

    /// Modifies a column in a table
    fn modify_table_column(&mut self, table_name: &str, column: SchemaColumn) -> Result<()>;

    // ---- Query Operations ----

    /// Executes a SELECT query
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to query
    /// * `columns_to_fetch` - Column names to include in the result
    /// * `expr` - Optional filter expression
    /// * `original_columns` - Optional original column names (for aliasing)
    fn select(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>>;

    /// Executes a SELECT query with column aliases
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to query
    /// * `columns_to_fetch` - Column names to include in the result
    /// * `expr` - Optional filter expression
    /// * `aliases` - Map from alias names to original column names
    /// * `original_columns` - Optional original column names
    fn select_with_aliases(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        aliases: &FxHashMap<String, String>,
        original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>>;

    /// Executes a temporal SELECT query as of a specific transaction or timestamp
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to query
    /// * `columns_to_fetch` - Column names to include in the result
    /// * `expr` - Optional filter expression
    /// * `temporal_type` - Either "TRANSACTION" or "TIMESTAMP"
    /// * `temporal_value` - Transaction ID or timestamp in nanoseconds
    /// * `original_columns` - Optional original column names
    fn select_as_of(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        temporal_type: &str,
        temporal_value: i64,
        original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>>;
}

/// Temporal query type for time-travel queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalType {
    /// Query as of a specific transaction ID
    Transaction,
    /// Query as of a specific timestamp
    Timestamp,
}

impl std::str::FromStr for TemporalType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TRANSACTION" => Ok(Self::Transaction),
            "TIMESTAMP" => Ok(Self::Timestamp),
            _ => Err(()),
        }
    }
}

impl TemporalType {
    /// Parses a temporal type from a string
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temporal_type_from_str() {
        assert_eq!(
            TemporalType::parse("TRANSACTION"),
            Some(TemporalType::Transaction)
        );
        assert_eq!(
            TemporalType::parse("transaction"),
            Some(TemporalType::Transaction)
        );
        assert_eq!(
            TemporalType::parse("TIMESTAMP"),
            Some(TemporalType::Timestamp)
        );
        assert_eq!(
            TemporalType::parse("timestamp"),
            Some(TemporalType::Timestamp)
        );
        assert_eq!(TemporalType::parse("INVALID"), None);
    }

    // Verify trait is object-safe
    fn _assert_object_safe(_: &dyn Transaction) {}
}
