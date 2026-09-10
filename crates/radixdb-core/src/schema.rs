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

//! Schema types for RadixDB - table and column definitions
//!
//! This module defines SchemaColumn and Schema types for table structure.

use std::fmt;
use std::sync::{Arc, OnceLock};

use ahash::{AHashMap, AHashSet};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::{
    CompactArc, DataType, Error, ExternalTypeRef, ForeignKeyAction, LogicalTypeRef, Result,
    SchemaColumnId, Value,
};

type StringMap<V> = AHashMap<String, V>;
type StringSet = AHashSet<String>;

impl crate::RowSchema for Schema {
    fn row_column_count(&self) -> usize {
        self.columns.len()
    }

    fn row_column(&self, index: usize) -> Option<crate::RowColumnRef<'_>> {
        self.columns.get(index).map(|column| {
            crate::RowColumnRef::new(&column.name, column.data_type, column.nullable)
                .with_logical_type(column.logical_type())
        })
    }
}

/// A column definition in a table schema
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    /// Unique identifier for the column (0-based index)
    pub id: usize,

    /// Column name
    pub name: String,

    /// Pre-computed lowercase column name for case-insensitive lookups
    #[doc(hidden)]
    pub name_lower: String,

    /// Data type of the column
    pub data_type: DataType,

    /// Stable external type identity. Built-in columns keep this empty.
    pub external_type: Option<ExternalTypeRef>,

    /// Schema-qualified SQL type name used by introspection and dump output.
    pub external_type_name: Option<String>,

    /// Whether the column can contain NULL values
    pub nullable: bool,

    /// Whether this column is part of the primary key
    pub primary_key: bool,

    /// Whether this column auto-increments (generates sequential IDs for NULL values)
    pub auto_increment: bool,

    /// Default value expression as a string (to be parsed and evaluated during INSERT)
    pub default_expr: Option<String>,

    /// Pre-computed default value for schema evolution (used when adding column to existing rows)
    pub default_value: Option<Value>,

    /// CHECK constraint expression as a string (to be parsed and evaluated during INSERT)
    pub check_expr: Option<String>,

    /// Number of dimensions for VECTOR columns (0 = not a vector column)
    pub vector_dimensions: u16,

    /// Declared precision for DECIMAL columns (0 = unconstrained DECIMAL).
    pub decimal_precision: u8,

    /// Declared scale for DECIMAL columns. This is zero when precision is zero.
    pub decimal_scale: u8,
}

impl SchemaColumn {
    /// Create a new column definition
    pub fn new(
        id: usize,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
    ) -> Self {
        let name_str = name.into();
        let name_lower = name_str.to_lowercase();
        Self {
            id,
            name: name_str,
            name_lower,
            data_type,
            external_type: None,
            external_type_name: None,
            nullable,
            primary_key,
            auto_increment: false,
            default_expr: None,
            default_value: None,
            check_expr: None,
            vector_dimensions: 0,
            decimal_precision: 0,
            decimal_scale: 0,
        }
    }

    /// Set vector dimensions (for VECTOR columns)
    pub fn with_vector_dimensions(mut self, dims: u16) -> Self {
        self.vector_dimensions = dims;
        self
    }

    /// Set exact DECIMAL(p,s) column parameters. A precision of zero denotes
    /// the unparameterized DECIMAL domain and requires scale zero.
    pub fn with_decimal_parameters(mut self, precision: u8, scale: u8) -> Self {
        self.decimal_precision = precision;
        self.decimal_scale = scale;
        self
    }

    pub fn with_external_type(
        mut self,
        type_ref: ExternalTypeRef,
        sql_name: impl Into<String>,
    ) -> Self {
        self.data_type = DataType::Null;
        self.external_type = Some(type_ref);
        self.external_type_name = Some(sql_name.into());
        self
    }

    pub const fn logical_type(&self) -> LogicalTypeRef {
        match self.external_type {
            Some(type_ref) => LogicalTypeRef::External(type_ref),
            None => LogicalTypeRef::Builtin(self.data_type),
        }
    }

    /// Render the complete declared SQL type, including durable modifiers.
    pub fn formatted_data_type(&self) -> String {
        if let Some(name) = &self.external_type_name {
            name.clone()
        } else if self.data_type == DataType::Vector && self.vector_dimensions > 0 {
            format!("VECTOR({})", self.vector_dimensions)
        } else if self.data_type == DataType::Decimal && self.decimal_precision > 0 {
            format!("DECIMAL({},{})", self.decimal_precision, self.decimal_scale)
        } else {
            self.data_type.to_string()
        }
    }

    /// Exact schema-level type identity used by references and descriptors.
    pub fn has_same_declared_type(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.external_type == other.external_type
            && self.vector_dimensions == other.vector_dimensions
            && self.decimal_precision == other.decimal_precision
            && self.decimal_scale == other.decimal_scale
    }

    /// Validate value-level modifiers that are not represented by [`DataType`].
    /// The value is not rewritten: exact decimal payload bytes remain intact.
    pub fn validate_declared_value(&self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(type_ref) = self.external_type {
            if value.logical_type() != LogicalTypeRef::External(type_ref) {
                return Err(Error::Type(format!(
                    "value for column '{}' has the wrong external type identity",
                    self.name
                )));
            }
            return Ok(());
        }
        if self.data_type != DataType::Decimal || self.decimal_precision == 0 {
            return Ok(());
        }
        let (unscaled, _, mut scale) = value.as_decimal_parts().ok_or_else(|| {
            Error::Type(format!(
                "value for column '{}' is not a valid DECIMAL payload",
                self.name
            ))
        })?;
        let mut magnitude = unscaled.unsigned_abs();
        while scale > self.decimal_scale && magnitude % 10 == 0 {
            magnitude /= 10;
            scale -= 1;
        }
        if scale > self.decimal_scale {
            return Err(Error::Type(format!(
                "DECIMAL value for column '{}' has scale {}, exceeding declared scale {}",
                self.name, scale, self.decimal_scale
            )));
        }
        let coefficient_digits = if magnitude == 0 {
            1
        } else {
            magnitude.ilog10() as usize + 1
        };
        let integer_digits = coefficient_digits.saturating_sub(usize::from(scale));
        let allowed_integer_digits = usize::from(self.decimal_precision - self.decimal_scale);
        if integer_digits > allowed_integer_digits {
            return Err(Error::Type(format!(
                "DECIMAL value for column '{}' exceeds declared precision {} and scale {}",
                self.name, self.decimal_precision, self.decimal_scale
            )));
        }
        Ok(())
    }

    /// Create a new column definition with all options
    #[allow(clippy::too_many_arguments)]
    pub fn with_constraints(
        id: usize,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
        auto_increment: bool,
        default_expr: Option<String>,
        check_expr: Option<String>,
    ) -> Self {
        let name_str = name.into();
        let name_lower = name_str.to_lowercase();
        Self {
            id,
            name: name_str,
            name_lower,
            data_type,
            external_type: None,
            external_type_name: None,
            nullable,
            primary_key,
            auto_increment,
            default_expr,
            default_value: None,
            check_expr,
            vector_dimensions: 0,
            decimal_precision: 0,
            decimal_scale: 0,
        }
    }

    /// Create a new column definition with pre-computed default value
    #[allow(clippy::too_many_arguments)]
    pub fn with_default_value(
        id: usize,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
        auto_increment: bool,
        default_expr: Option<String>,
        default_value: Option<Value>,
        check_expr: Option<String>,
    ) -> Self {
        let name_str = name.into();
        let name_lower = name_str.to_lowercase();
        Self {
            id,
            name: name_str,
            name_lower,
            data_type,
            external_type: None,
            external_type_name: None,
            nullable,
            primary_key,
            auto_increment,
            default_expr,
            default_value,
            check_expr,
            vector_dimensions: 0,
            decimal_precision: 0,
            decimal_scale: 0,
        }
    }

    /// Create a simple non-nullable, non-primary-key column
    pub fn simple(id: usize, name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(id, name, data_type, false, false)
    }

    /// Create a nullable column
    pub fn nullable(id: usize, name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(id, name, data_type, true, false)
    }

    /// Create a primary key column
    pub fn primary_key(id: usize, name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(id, name, data_type, false, true)
    }
}

impl fmt::Display for SchemaColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.formatted_data_type())?;
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if !self.nullable && !self.primary_key {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

/// Foreign key constraint metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraint {
    /// Index of the FK column in this table's schema
    pub column_index: usize,
    /// FK column name (for error messages)
    pub column_name: String,
    /// Referenced parent table name (lowercase)
    pub referenced_table: String,
    /// Referenced parent column name (lowercase)
    pub referenced_column: String,
    /// Action when parent row is deleted
    pub on_delete: ForeignKeyAction,
    /// Action when parent PK is updated
    pub on_update: ForeignKeyAction,
}

/// Durable catalog identity for a SQL table constraint.
///
/// `id` and `name` are assigned once when the constraint is created and are
/// persisted with the schema.  Renaming a table or column updates the bound
/// columns in `kind`, but deliberately does not rename the public constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaConstraint {
    pub id: u64,
    pub name: String,
    pub kind: SchemaConstraintKind,
}

/// Complete, catalog-visible constraint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaConstraintKind {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
        /// Physical index owned by this UNIQUE constraint.
        index_name: String,
    },
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
    Check {
        column_name: Option<String>,
        expression: String,
        /// Monotonic table-local CHECK ordinal used by the public name.
        ordinal: u32,
    },
}

impl SchemaConstraintKind {
    pub fn columns(&self) -> &[String] {
        match self {
            Self::PrimaryKey { columns }
            | Self::Unique { columns, .. }
            | Self::ForeignKey { columns, .. } => columns,
            Self::Check {
                column_name: Some(column_name),
                ..
            } => std::slice::from_ref(column_name),
            Self::Check {
                column_name: None, ..
            } => &[],
        }
    }
}

/// Public constraint identifiers use the same bounded representation in every
/// parser/catalog/descriptor path.
pub const MAX_CONSTRAINT_NAME_BYTES: usize = 128;
const CONSTRAINT_HASH_SUFFIX_BYTES: usize = 10; // "__" + eight hex digits
const CONSTRAINT_NAME_DESCRIPTOR_PREFIX: &str = "radixdb.constraint-name.v1";

/// Proof used by navigation planning for one eligible reference target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceTargetKey {
    PrimaryKey,
    UniqueNotNull,
}

/// Read-only schema capability for one source FK column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceDescriptor {
    source: SchemaColumnId,
    target: SchemaColumnId,
    source_nullable: bool,
    data_type: DataType,
    target_key: ReferenceTargetKey,
}

impl ReferenceDescriptor {
    #[doc(hidden)]
    pub fn new(
        source: SchemaColumnId,
        target: SchemaColumnId,
        source_nullable: bool,
        data_type: DataType,
        target_key: ReferenceTargetKey,
    ) -> Self {
        Self {
            source,
            target,
            source_nullable,
            data_type,
            target_key,
        }
    }

    pub fn source(&self) -> &SchemaColumnId {
        &self.source
    }

    pub fn target(&self) -> &SchemaColumnId {
        &self.target
    }

    pub fn source_nullable(&self) -> bool {
        self.source_nullable
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn target_key(&self) -> ReferenceTargetKey {
        self.target_key
    }

    pub fn schema_generation(&self) -> u64 {
        self.source.table().schema_generation()
    }
}

/// Table schema definition
///

#[derive(Debug)]
pub struct Schema {
    /// Durable table identity assigned once at engine admission. The all-zero
    /// value is reserved for detached schemas that have not entered a catalog.
    #[doc(hidden)]
    pub catalog_id: [u8; 16],

    /// Name of the table
    #[doc(hidden)]
    pub table_name: String,

    /// Pre-computed lowercase table name for case-insensitive lookups
    #[doc(hidden)]
    pub table_name_lower: String,

    /// Column definitions
    #[doc(hidden)]
    pub columns: Vec<SchemaColumn>,

    /// Foreign key constraints
    #[doc(hidden)]
    pub foreign_keys: Vec<ForeignKeyConstraint>,

    /// Table-level CHECK expressions evaluated against the complete row.
    ///
    /// These are distinct from `SchemaColumn::check_expr`: a table CHECK may
    /// reference any number of columns and therefore cannot be evaluated with
    /// a single-column row context.
    #[doc(hidden)]
    pub table_checks: Vec<String>,

    /// Durable named constraint catalog. Enforcement remains in the existing
    /// column/FK/CHECK/index owners; this list gives every owner a stable public
    /// identity and records physical UNIQUE ownership.
    #[doc(hidden)]
    pub constraints: Vec<SchemaConstraint>,

    /// Next stable constraint identity. IDs are never reused after DROP.
    #[doc(hidden)]
    pub next_constraint_id: u64,

    /// Next table-local CHECK ordinal. Ordinals are never reused after DROP.
    #[doc(hidden)]
    pub next_check_ordinal: u32,

    /// Creation timestamp
    #[doc(hidden)]
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    #[doc(hidden)]
    pub updated_at: DateTime<Utc>,

    /// Cached column names (computed lazily on first access, Arc for zero-copy sharing)
    column_names_cache: OnceLock<CompactArc<Vec<String>>>,

    /// Cached primary key column index (computed lazily on first access)
    /// None means not computed yet, Some(None) means no PK, Some(Some(idx)) means PK at idx
    pk_column_index_cache: OnceLock<Option<usize>>,

    /// Cached column index map (lowercase name -> index) for O(1) column lookup
    column_index_map_cache: OnceLock<StringMap<usize>>,

    /// Cached primary key indices (computed lazily on first access)
    pk_indices_cache: OnceLock<Arc<Vec<usize>>>,

    /// Cached lowercase column names (computed lazily on first access)
    column_names_lower_cache: OnceLock<CompactArc<Vec<String>>>,
}

impl Clone for Schema {
    fn clone(&self) -> Self {
        // Clone caches if already computed to avoid recomputation
        let column_names_cache = OnceLock::new();
        if let Some(names) = self.column_names_cache.get() {
            // CompactArc clone is O(1) - just increments ref count
            let _ = column_names_cache.set(CompactArc::clone(names));
        }

        let pk_column_index_cache = OnceLock::new();
        if let Some(pk_idx) = self.pk_column_index_cache.get() {
            let _ = pk_column_index_cache.set(*pk_idx);
        }

        let column_index_map_cache = OnceLock::new();
        if let Some(map) = self.column_index_map_cache.get() {
            let _ = column_index_map_cache.set(map.clone());
        }

        let pk_indices_cache = OnceLock::new();
        if let Some(indices) = self.pk_indices_cache.get() {
            let _ = pk_indices_cache.set(Arc::clone(indices));
        }

        let column_names_lower_cache = OnceLock::new();
        if let Some(names) = self.column_names_lower_cache.get() {
            let _ = column_names_lower_cache.set(CompactArc::clone(names));
        }

        Self {
            catalog_id: self.catalog_id,
            table_name: self.table_name.clone(),
            table_name_lower: self.table_name_lower.clone(),
            columns: self.columns.clone(),
            foreign_keys: self.foreign_keys.clone(),
            table_checks: self.table_checks.clone(),
            constraints: self.constraints.clone(),
            next_constraint_id: self.next_constraint_id,
            next_check_ordinal: self.next_check_ordinal,
            created_at: self.created_at,
            updated_at: self.updated_at,
            column_names_cache,
            pk_column_index_cache,
            column_index_map_cache,
            pk_indices_cache,
            column_names_lower_cache,
        }
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.catalog_id == other.catalog_id
            && self.table_name == other.table_name
            && self.columns == other.columns
            && self.foreign_keys == other.foreign_keys
            && self.table_checks == other.table_checks
            && self.constraints == other.constraints
            && self.next_constraint_id == other.next_constraint_id
            && self.next_check_ordinal == other.next_check_ordinal
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
    }
}

impl Eq for Schema {}

impl Schema {
    /// Create a new schema with the given table name and columns
    pub fn new(table_name: impl Into<String>, columns: Vec<SchemaColumn>) -> Self {
        Self::with_foreign_keys(table_name, columns, Vec::new())
    }

    /// Create a new schema with columns and foreign key constraints
    pub fn with_foreign_keys(
        table_name: impl Into<String>,
        columns: Vec<SchemaColumn>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        Self::with_constraints(table_name, columns, foreign_keys, Vec::new())
    }

    /// Create a new schema with all table-level constraints.
    pub fn with_constraints(
        table_name: impl Into<String>,
        mut columns: Vec<SchemaColumn>,
        foreign_keys: Vec<ForeignKeyConstraint>,
        table_checks: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        let name = table_name.into();
        let name_lower = name.to_lowercase();
        normalize_columns(&mut columns);

        // Eagerly compute caches to avoid recomputation on clone
        let column_names_cache = OnceLock::new();
        let _ = column_names_cache.set(CompactArc::new(
            columns.iter().map(|c| c.name.clone()).collect(),
        ));

        let pk_column_index_cache = OnceLock::new();
        let pk_idx = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.primary_key && col.data_type == DataType::Integer)
            .map(|(i, _)| i);
        let _ = pk_column_index_cache.set(pk_idx);

        let column_index_map_cache = OnceLock::new();
        let _ = column_index_map_cache.set(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name_lower.clone(), i))
                .collect(),
        );

        let pk_indices_cache = OnceLock::new();
        let _ = pk_indices_cache.set(Arc::new(
            columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.primary_key)
                .map(|(i, _)| i)
                .collect(),
        ));

        let column_names_lower_cache = OnceLock::new();
        let _ = column_names_lower_cache.set(CompactArc::new(
            columns.iter().map(|c| c.name_lower.clone()).collect(),
        ));

        Self {
            catalog_id: [0; 16],
            table_name: name,
            table_name_lower: name_lower,
            columns,
            foreign_keys,
            table_checks,
            constraints: Vec::new(),
            next_constraint_id: 1,
            next_check_ordinal: 1,
            created_at: now,
            updated_at: now,
            column_names_cache,
            pk_column_index_cache,
            column_index_map_cache,
            pk_indices_cache,
            column_names_lower_cache,
        }
    }

    /// Create a new schema with explicit timestamps
    pub fn with_timestamps(
        table_name: impl Into<String>,
        columns: Vec<SchemaColumn>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self::with_timestamps_and_foreign_keys(
            table_name,
            columns,
            Vec::new(),
            created_at,
            updated_at,
        )
    }

    /// Create a new schema with explicit timestamps and foreign key constraints
    pub fn with_timestamps_and_foreign_keys(
        table_name: impl Into<String>,
        columns: Vec<SchemaColumn>,
        foreign_keys: Vec<ForeignKeyConstraint>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self::with_timestamps_and_constraints(
            table_name,
            columns,
            foreign_keys,
            Vec::new(),
            created_at,
            updated_at,
        )
    }

    /// Create a schema with explicit timestamps and all table constraints.
    pub fn with_timestamps_and_constraints(
        table_name: impl Into<String>,
        mut columns: Vec<SchemaColumn>,
        foreign_keys: Vec<ForeignKeyConstraint>,
        table_checks: Vec<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        let name = table_name.into();
        let name_lower = name.to_lowercase();
        normalize_columns(&mut columns);

        // Eagerly compute caches to avoid recomputation on clone
        let column_names_cache = OnceLock::new();
        let _ = column_names_cache.set(CompactArc::new(
            columns.iter().map(|c| c.name.clone()).collect(),
        ));

        let pk_column_index_cache = OnceLock::new();
        let pk_idx = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.primary_key && col.data_type == DataType::Integer)
            .map(|(i, _)| i);
        let _ = pk_column_index_cache.set(pk_idx);

        let column_index_map_cache = OnceLock::new();
        let _ = column_index_map_cache.set(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name_lower.clone(), i))
                .collect(),
        );

        let pk_indices_cache = OnceLock::new();
        let _ = pk_indices_cache.set(Arc::new(
            columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.primary_key)
                .map(|(i, _)| i)
                .collect(),
        ));

        let column_names_lower_cache = OnceLock::new();
        let _ = column_names_lower_cache.set(CompactArc::new(
            columns.iter().map(|c| c.name_lower.clone()).collect(),
        ));

        Self {
            catalog_id: [0; 16],
            table_name: name,
            table_name_lower: name_lower,
            columns,
            foreign_keys,
            table_checks,
            constraints: Vec::new(),
            next_constraint_id: 1,
            next_check_ordinal: 1,
            created_at,
            updated_at,
            column_names_cache,
            pk_column_index_cache,
            column_index_map_cache,
            pk_indices_cache,
            column_names_lower_cache,
        }
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the schema has any columns
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Return the authoritative table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Return the durable 128-bit catalog identity. Detached schemas expose
    /// the reserved all-zero value until an engine admits them.
    pub fn catalog_id(&self) -> [u8; 16] {
        self.catalog_id
    }

    #[doc(hidden)]
    pub fn ensure_catalog_identity(&mut self) {
        if self.catalog_id == [0; 16] {
            self.catalog_id = *uuid::Uuid::now_v7().as_bytes();
        }
    }

    #[doc(hidden)]
    pub fn install_catalog_identity(&mut self, catalog_id: [u8; 16]) -> Result<()> {
        if catalog_id == [0; 16] {
            return Err(Error::InvalidArgument(
                "persisted schema has the reserved zero catalog identity".to_string(),
            ));
        }
        self.catalog_id = catalog_id;
        Ok(())
    }

    #[doc(hidden)]
    pub fn canonicalize_for_persistence(&self) -> Result<Self> {
        let mut schema = self.clone();
        schema.ensure_catalog_identity();
        schema.ensure_constraint_catalog()?;
        Ok(schema)
    }

    /// Return the authoritative column definitions.
    pub fn columns(&self) -> &[SchemaColumn] {
        &self.columns
    }

    /// Return the table foreign-key constraints.
    pub fn foreign_keys(&self) -> &[ForeignKeyConstraint] {
        &self.foreign_keys
    }

    /// Return the table-level CHECK expressions.
    pub fn table_checks(&self) -> &[String] {
        &self.table_checks
    }

    /// Return durable named SQL constraints in creation order.
    pub fn constraints(&self) -> &[SchemaConstraint] {
        &self.constraints
    }

    /// Return a named constraint using catalog case-insensitive identity.
    pub fn find_constraint(&self, name: &str) -> Option<&SchemaConstraint> {
        let name_lower = name.to_lowercase();
        self.constraints
            .iter()
            .find(|constraint| constraint.name.to_lowercase() == name_lower)
    }

    /// Complete automatic names for a schema constructed through the public
    /// Rust API. SQL DDL normally arrives with the same entries already
    /// installed; engine admission and persistence call this method so a
    /// current-format schema never publishes unnamed enforcement owners.
    #[doc(hidden)]
    pub fn ensure_constraint_catalog(&mut self) -> Result<()> {
        if !self.constraints.is_empty() {
            return self.validate_constraint_catalog_complete();
        }

        if let Some(primary_key) = self.primary_key_columns().first() {
            self.register_primary_key_constraint(vec![primary_key.name.clone()])?;
        }
        for column in self.columns.clone() {
            if let Some(expression) = column.check_expr {
                self.register_check_constraint(Some(column.name), expression)?;
            }
        }
        for foreign_key in self.foreign_keys.clone() {
            self.register_foreign_key_constraint(&foreign_key)?;
        }
        for expression in self.table_checks.clone() {
            self.register_check_constraint(None, expression)?;
        }
        self.validate_constraint_catalog_complete()
    }

    /// Install a constraint catalog decoded from the current persistence
    /// format. This is intentionally unavailable as an implicit migration path:
    /// older schema markers fail before reaching this method.
    #[doc(hidden)]
    pub fn install_constraint_catalog(
        &mut self,
        constraints: Vec<SchemaConstraint>,
        next_constraint_id: u64,
        next_check_ordinal: u32,
    ) -> Result<()> {
        let mut names = StringSet::default();
        let mut ids = std::collections::HashSet::new();
        let mut max_id = 0u64;
        let mut max_check_ordinal = 0u32;
        for constraint in &constraints {
            if constraint.id == 0 || !ids.insert(constraint.id) {
                return Err(Error::InvalidArgument(
                    "constraint catalog contains a zero or duplicate stable identity".to_string(),
                ));
            }
            if constraint.name.is_empty()
                || constraint.name.len() > MAX_CONSTRAINT_NAME_BYTES
                || !names.insert(constraint.name.to_lowercase())
            {
                return Err(Error::InvalidArgument(
                    "constraint catalog contains an empty, oversized, or duplicate name"
                        .to_string(),
                ));
            }
            max_id = max_id.max(constraint.id);
            if let SchemaConstraintKind::Check { ordinal, .. } = constraint.kind {
                if ordinal == 0 {
                    return Err(Error::InvalidArgument(
                        "CHECK constraint ordinal must be non-zero".to_string(),
                    ));
                }
                max_check_ordinal = max_check_ordinal.max(ordinal);
            }
        }
        if next_constraint_id <= max_id || next_check_ordinal <= max_check_ordinal {
            return Err(Error::InvalidArgument(
                "constraint catalog next identity/ordinal would reuse an existing value"
                    .to_string(),
            ));
        }
        self.constraints = constraints;
        self.next_constraint_id = next_constraint_id;
        self.next_check_ordinal = next_check_ordinal;
        self.validate_constraint_catalog_complete()?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn register_primary_key_constraint(&mut self, columns: Vec<String>) -> Result<String> {
        let canonical_columns = self.resolve_constraint_columns(columns)?;
        let base = format!("pk_{}", self.table_name_lower);
        self.register_constraint(
            base,
            "primary_key",
            &canonical_columns,
            &[],
            SchemaConstraintKind::PrimaryKey {
                columns: canonical_columns.clone(),
            },
        )
    }

    #[doc(hidden)]
    pub fn register_unique_constraint(&mut self, columns: Vec<String>) -> Result<String> {
        let canonical_columns = self.resolve_constraint_columns(columns)?;
        let base = format!(
            "uq_{}_{}",
            self.table_name_lower,
            canonical_columns
                .iter()
                .map(|column| column.to_lowercase())
                .collect::<Vec<_>>()
                .join("_")
        );
        let placeholder = SchemaConstraintKind::Unique {
            columns: canonical_columns.clone(),
            index_name: String::new(),
        };
        let name =
            self.register_constraint(base, "unique", &canonical_columns, &[], placeholder)?;
        let constraint = self
            .constraints
            .last_mut()
            .expect("registered UNIQUE constraint exists");
        let SchemaConstraintKind::Unique { index_name, .. } = &mut constraint.kind else {
            unreachable!("registered UNIQUE kind changed")
        };
        *index_name = name.clone();
        Ok(name)
    }

    #[doc(hidden)]
    pub fn register_foreign_key_constraint(
        &mut self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        let columns = self.resolve_constraint_columns(vec![foreign_key.column_name.clone()])?;
        let referenced_table = foreign_key.referenced_table.to_lowercase();
        let referenced_columns = vec![foreign_key.referenced_column.to_lowercase()];
        let base = format!(
            "fk_{}_{}___{}",
            self.table_name_lower,
            columns[0].to_lowercase(),
            referenced_table
        );
        let mut extra = vec![referenced_table.clone()];
        extra.extend(referenced_columns.iter().cloned());
        self.register_constraint(
            base,
            "foreign_key",
            &columns,
            &extra,
            SchemaConstraintKind::ForeignKey {
                columns: columns.clone(),
                referenced_table,
                referenced_columns,
                on_delete: foreign_key.on_delete,
                on_update: foreign_key.on_update,
            },
        )
    }

    #[doc(hidden)]
    pub fn register_check_constraint(
        &mut self,
        column_name: Option<String>,
        expression: String,
    ) -> Result<String> {
        let column_name = match column_name {
            Some(column_name) => Some(
                self.resolve_constraint_columns(vec![column_name])?
                    .remove(0),
            ),
            None => None,
        };
        let ordinal = self.next_check_ordinal;
        self.next_check_ordinal = self
            .next_check_ordinal
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("CHECK ordinal space exhausted".to_string()))?;
        let base = format!("chk_{}_{}", self.table_name_lower, ordinal);
        let result = self.register_constraint(
            base,
            "check",
            &[],
            &[ordinal.to_string()],
            SchemaConstraintKind::Check {
                column_name,
                expression,
                ordinal,
            },
        );
        if result.is_err() {
            self.next_check_ordinal = ordinal;
        }
        result
    }

    /// Remove only the durable catalog entry. The DDL owner first validates
    /// dependencies and updates the matching enforcement owner atomically.
    #[doc(hidden)]
    pub fn take_constraint(&mut self, name: &str) -> Option<SchemaConstraint> {
        let index = self
            .constraints
            .iter()
            .position(|constraint| constraint.name.eq_ignore_ascii_case(name))?;
        Some(self.constraints.remove(index))
    }

    fn resolve_constraint_columns(&self, columns: Vec<String>) -> Result<Vec<String>> {
        if columns.is_empty() {
            return Err(Error::InvalidArgument(
                "constraint must reference at least one column".to_string(),
            ));
        }
        columns
            .into_iter()
            .map(|column| {
                self.get_column_by_name(&column)
                    .map(|resolved| resolved.name.clone())
                    .ok_or(Error::ColumnNotFound(column))
            })
            .collect()
    }

    fn register_constraint(
        &mut self,
        base: String,
        kind_name: &str,
        columns: &[String],
        extra_fields: &[String],
        kind: SchemaConstraintKind,
    ) -> Result<String> {
        let base_conflicts = self
            .constraints
            .iter()
            .any(|constraint| constraint.name.eq_ignore_ascii_case(&base));
        let name = generated_constraint_name(
            &base,
            kind_name,
            &self.table_name_lower,
            columns,
            extra_fields,
            base_conflicts,
        );
        if self
            .constraints
            .iter()
            .any(|constraint| constraint.name.eq_ignore_ascii_case(&name))
        {
            return Err(Error::InvalidArgument(format!(
                "generated constraint name '{}' collides with an existing constraint",
                name
            )));
        }
        let id = self.next_constraint_id;
        self.next_constraint_id = self.next_constraint_id.checked_add(1).ok_or_else(|| {
            Error::InvalidArgument("constraint identity space exhausted".to_string())
        })?;
        self.constraints.push(SchemaConstraint {
            id,
            name: name.clone(),
            kind,
        });
        Ok(name)
    }

    /// Return the schema creation timestamp.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Return the schema update timestamp.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Install timestamps selected by the authoritative durable catalog.
    #[doc(hidden)]
    pub fn install_catalog_timestamps(
        &mut self,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        if updated_at < created_at {
            return Err(Error::invalid_argument(
                "schema update timestamp precedes its creation timestamp",
            ));
        }
        self.created_at = created_at;
        self.updated_at = updated_at;
        Ok(())
    }

    /// Find a column by name (case-insensitive)
    /// Returns the column index and reference
    /// OPTIMIZATION: Uses cached column_index_map for O(1) lookup
    pub fn find_column(&self, name: &str) -> Option<(usize, &SchemaColumn)> {
        let name_lower = name.to_lowercase();
        self.column_index_map()
            .get(&name_lower)
            .map(|&idx| (idx, &self.columns[idx]))
    }

    /// Get a column by index
    pub fn get_column(&self, index: usize) -> Option<&SchemaColumn> {
        self.columns.get(index)
    }

    /// Get a column by name (case-insensitive)
    pub fn get_column_by_name(&self, name: &str) -> Option<&SchemaColumn> {
        self.find_column(name).map(|(_, col)| col)
    }

    /// Get the column index by name (case-insensitive)
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.find_column(name).map(|(idx, _)| idx)
    }

    /// Get the data type of a column by name
    pub fn get_column_type(&self, name: &str) -> Option<DataType> {
        self.get_column_by_name(name).map(|col| col.data_type)
    }

    /// Check if a column exists by name
    pub fn has_column(&self, name: &str) -> bool {
        self.find_column(name).is_some()
    }

    /// Get all column names as borrowed strings (allocates Vec but not strings)
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get all column names as owned strings (cached - only clones once)
    ///
    /// This is more efficient than calling `.columns.iter().map(|c| c.name.clone()).collect()`
    /// repeatedly, as the result is computed once and cached.
    #[inline]
    pub fn column_names_owned(&self) -> &[String] {
        self.column_names_cache
            .get_or_init(|| CompactArc::new(self.columns.iter().map(|c| c.name.clone()).collect()))
    }

    /// Get column names as Arc for zero-copy sharing with results
    ///
    /// This is the most efficient way to pass column names to `ExecutorResult::with_arc_columns`
    /// as it avoids any string cloning after the first call.
    #[inline]
    pub fn column_names_arc(&self) -> CompactArc<Vec<String>> {
        CompactArc::clone(
            self.column_names_cache.get_or_init(|| {
                CompactArc::new(self.columns.iter().map(|c| c.name.clone()).collect())
            }),
        )
    }

    /// Get lowercase column names as Arc for zero-copy sharing
    ///
    /// Uses the pre-computed `name_lower` from each SchemaColumn, avoiding
    /// per-query `to_lowercase()` calls.
    #[inline]
    pub fn column_names_lower_arc(&self) -> CompactArc<Vec<String>> {
        CompactArc::clone(self.column_names_lower_cache.get_or_init(|| {
            CompactArc::new(self.columns.iter().map(|c| c.name_lower.clone()).collect())
        }))
    }

    /// Get a cached map of lowercase column names to their indices
    /// OPTIMIZATION: Cached to avoid creating this map on every query
    #[inline]
    pub fn column_index_map(&self) -> &StringMap<usize> {
        self.column_index_map_cache.get_or_init(|| {
            self.columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name_lower.clone(), i))
                .collect()
        })
    }

    /// Get the primary key columns
    pub fn primary_key_columns(&self) -> Vec<&SchemaColumn> {
        self.columns.iter().filter(|c| c.primary_key).collect()
    }

    /// Check if the schema has a primary key
    pub fn has_primary_key(&self) -> bool {
        self.columns.iter().any(|c| c.primary_key)
    }

    /// Get the primary key column indices (cached for performance)
    /// OPTIMIZATION: Cached to avoid allocating Vec on every call
    #[inline]
    pub fn primary_key_indices(&self) -> &[usize] {
        self.pk_indices_cache.get_or_init(|| {
            Arc::new(
                self.columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.primary_key)
                    .map(|(i, _)| i)
                    .collect(),
            )
        })
    }

    /// Get the single primary key column index (cached for performance)
    /// Returns None if there's no PK or if PK is not an integer type
    /// OPTIMIZATION: Cached to avoid iteration on every INSERT
    #[inline]
    pub fn pk_column_index(&self) -> Option<usize> {
        *self.pk_column_index_cache.get_or_init(|| {
            for (i, col) in self.columns.iter().enumerate() {
                if col.primary_key && col.data_type == DataType::Integer {
                    return Some(i);
                }
            }
            None
        })
    }

    /// Validate column count matches expected value
    pub fn validate_column_count(&self, expected: usize) -> Result<()> {
        if self.columns.len() != expected {
            return Err(Error::table_columns_not_match(expected, self.columns.len()));
        }
        Ok(())
    }

    /// Validate that every foreign-key ordinal still names the same local column.
    ///
    /// Foreign keys retain both an ordinal (used by row consumers) and a name
    /// (used by diagnostics and persistence).  Schema mutation and artifact
    /// decode must never publish a schema in which those two identities drift.
    #[doc(hidden)]
    pub fn validate_foreign_key_invariants(&self) -> Result<()> {
        for fk in &self.foreign_keys {
            let column = self.columns.get(fk.column_index).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "foreign key column index {} is out of bounds for table '{}' with {} columns",
                    fk.column_index,
                    self.table_name,
                    self.columns.len()
                ))
            })?;

            if !column.name.eq_ignore_ascii_case(&fk.column_name) {
                return Err(Error::InvalidArgument(format!(
                    "foreign key column identity mismatch in table '{}': index {} names '{}', metadata names '{}'",
                    self.table_name, fk.column_index, column.name, fk.column_name
                )));
            }
            if fk.referenced_table.is_empty() || fk.referenced_column.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "foreign key on '{}.{}' has an empty referenced table or column",
                    self.table_name, column.name
                )));
            }
            if (matches!(fk.on_delete, ForeignKeyAction::SetNull)
                || matches!(fk.on_update, ForeignKeyAction::SetNull))
                && !column.nullable
            {
                return Err(Error::InvalidArgument(format!(
                    "foreign key on non-nullable column '{}.{}' uses SET NULL",
                    self.table_name, column.name
                )));
            }
        }

        Ok(())
    }

    #[doc(hidden)]
    pub fn validate_constraint_catalog(&self) -> Result<()> {
        let mut names = StringSet::default();
        let mut ids = std::collections::HashSet::new();
        for constraint in &self.constraints {
            if constraint.id == 0 || !ids.insert(constraint.id) {
                return Err(Error::InvalidArgument(format!(
                    "constraint '{}' has a zero or duplicate stable identity",
                    constraint.name
                )));
            }
            if constraint.name.is_empty()
                || constraint.name.len() > MAX_CONSTRAINT_NAME_BYTES
                || !names.insert(constraint.name.to_lowercase())
            {
                return Err(Error::InvalidArgument(format!(
                    "constraint '{}' has an invalid or duplicate public name",
                    constraint.name
                )));
            }
            for column in constraint.kind.columns() {
                if self.find_column(column).is_none() {
                    return Err(Error::InvalidArgument(format!(
                        "constraint '{}' references missing column '{}'",
                        constraint.name, column
                    )));
                }
            }
            match &constraint.kind {
                SchemaConstraintKind::PrimaryKey { columns } => {
                    if columns.len() != 1
                        || self
                            .get_column_by_name(&columns[0])
                            .is_none_or(|column| !column.primary_key)
                    {
                        return Err(Error::InvalidArgument(format!(
                            "constraint '{}' does not match the PRIMARY KEY owner",
                            constraint.name
                        )));
                    }
                }
                SchemaConstraintKind::Unique { index_name, .. } => {
                    if index_name.is_empty() {
                        return Err(Error::InvalidArgument(format!(
                            "UNIQUE constraint '{}' has no owned physical index",
                            constraint.name
                        )));
                    }
                }
                SchemaConstraintKind::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    on_delete,
                    on_update,
                } => {
                    if columns.len() != 1
                        || referenced_columns.len() != 1
                        || !self.foreign_keys.iter().any(|foreign_key| {
                            foreign_key.column_name.eq_ignore_ascii_case(&columns[0])
                                && foreign_key
                                    .referenced_table
                                    .eq_ignore_ascii_case(referenced_table)
                                && foreign_key
                                    .referenced_column
                                    .eq_ignore_ascii_case(&referenced_columns[0])
                                && foreign_key.on_delete == *on_delete
                                && foreign_key.on_update == *on_update
                        })
                    {
                        return Err(Error::InvalidArgument(format!(
                            "constraint '{}' does not match a FOREIGN KEY owner",
                            constraint.name
                        )));
                    }
                }
                SchemaConstraintKind::Check {
                    column_name,
                    expression,
                    ordinal,
                } => {
                    if *ordinal == 0 {
                        return Err(Error::InvalidArgument(format!(
                            "CHECK constraint '{}' has ordinal zero",
                            constraint.name
                        )));
                    }
                    let matches_owner = if let Some(column_name) = column_name {
                        self.get_column_by_name(column_name)
                            .and_then(|column| column.check_expr.as_ref())
                            == Some(expression)
                    } else {
                        self.table_checks.iter().any(|check| check == expression)
                    };
                    if !matches_owner {
                        return Err(Error::InvalidArgument(format!(
                            "constraint '{}' does not match a CHECK owner",
                            constraint.name
                        )));
                    }
                }
            }
        }
        let max_id = self
            .constraints
            .iter()
            .map(|constraint| constraint.id)
            .max();
        if max_id.is_some_and(|max_id| self.next_constraint_id <= max_id) {
            return Err(Error::InvalidArgument(
                "next constraint identity would reuse an existing identity".to_string(),
            ));
        }
        let max_ordinal = self
            .constraints
            .iter()
            .filter_map(|constraint| match constraint.kind {
                SchemaConstraintKind::Check { ordinal, .. } => Some(ordinal),
                _ => None,
            })
            .max();
        if max_ordinal.is_some_and(|max_ordinal| self.next_check_ordinal <= max_ordinal) {
            return Err(Error::InvalidArgument(
                "next CHECK ordinal would reuse an existing ordinal".to_string(),
            ));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn validate_constraint_catalog_complete(&self) -> Result<()> {
        self.validate_constraint_catalog()?;
        let primary_key_owner_count = usize::from(self.has_primary_key());
        let primary_key_catalog_count = self
            .constraints
            .iter()
            .filter(|constraint| matches!(constraint.kind, SchemaConstraintKind::PrimaryKey { .. }))
            .count();
        if primary_key_owner_count != primary_key_catalog_count {
            return Err(Error::InvalidArgument(
                "PRIMARY KEY owner and named constraint catalog are incomplete".to_string(),
            ));
        }

        for foreign_key in &self.foreign_keys {
            let count = self
                .constraints
                .iter()
                .filter(|constraint| match &constraint.kind {
                    SchemaConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } => {
                        columns.len() == 1
                            && referenced_columns.len() == 1
                            && columns[0].eq_ignore_ascii_case(&foreign_key.column_name)
                            && referenced_table.eq_ignore_ascii_case(&foreign_key.referenced_table)
                            && referenced_columns[0]
                                .eq_ignore_ascii_case(&foreign_key.referenced_column)
                            && *on_delete == foreign_key.on_delete
                            && *on_update == foreign_key.on_update
                    }
                    _ => false,
                })
                .count();
            if count != 1 {
                return Err(Error::InvalidArgument(format!(
                    "FOREIGN KEY owner on column '{}' has {count} named catalog entries",
                    foreign_key.column_name
                )));
            }
        }

        for column in &self.columns {
            if let Some(expression) = &column.check_expr {
                let count = self
                    .constraints
                    .iter()
                    .filter(|constraint| {
                        matches!(
                            &constraint.kind,
                            SchemaConstraintKind::Check {
                                column_name: Some(column_name),
                                expression: candidate,
                                ..
                            } if column_name.eq_ignore_ascii_case(&column.name)
                                && candidate == expression
                        )
                    })
                    .count();
                if count != 1 {
                    return Err(Error::InvalidArgument(format!(
                        "column CHECK owner on '{}' has {count} named catalog entries",
                        column.name
                    )));
                }
            }
        }

        let mut table_check_owners = std::collections::HashMap::<&str, usize>::new();
        for expression in &self.table_checks {
            *table_check_owners.entry(expression.as_str()).or_default() += 1;
        }
        let mut table_check_catalog = std::collections::HashMap::<&str, usize>::new();
        for constraint in &self.constraints {
            if let SchemaConstraintKind::Check {
                column_name: None,
                expression,
                ..
            } = &constraint.kind
            {
                *table_check_catalog.entry(expression.as_str()).or_default() += 1;
            }
        }
        if table_check_owners != table_check_catalog {
            return Err(Error::InvalidArgument(
                "table CHECK owners and named constraint catalog are incomplete".to_string(),
            ));
        }
        Ok(())
    }

    /// Validate the single authoritative schema snapshot used by all caches.
    #[doc(hidden)]
    pub fn validate_structural_invariants(&self) -> Result<()> {
        if self.table_name.is_empty() || self.table_name_lower != self.table_name.to_lowercase() {
            return Err(Error::InvalidArgument(
                "schema table identity is empty or not normalized".to_string(),
            ));
        }
        let mut seen = StringSet::default();
        let mut primary_keys = 0usize;
        for (index, column) in self.columns.iter().enumerate() {
            if column.id != index
                || column.name.is_empty()
                || column.name_lower != column.name.to_lowercase()
            {
                return Err(Error::InvalidArgument(format!(
                    "column {} has a stale ordinal or normalized identity",
                    column.name
                )));
            }
            if !seen.insert(column.name_lower.clone()) {
                return Err(Error::DuplicateColumn);
            }
            if column.primary_key && column.nullable {
                return Err(Error::InvalidArgument(format!(
                    "PRIMARY KEY column '{}' cannot be nullable",
                    column.name
                )));
            }
            if column.auto_increment
                && !matches!(column.data_type, DataType::Integer | DataType::Uuid)
            {
                return Err(Error::InvalidArgument(format!(
                    "AUTO_INCREMENT column '{}' must be INTEGER or UUID",
                    column.name
                )));
            }
            if column.external_type.is_some() {
                if column.data_type != DataType::Null
                    || column
                        .external_type_name
                        .as_deref()
                        .is_none_or(str::is_empty)
                    || column.vector_dimensions != 0
                    || column.decimal_precision != 0
                    || column.decimal_scale != 0
                {
                    return Err(Error::InvalidArgument(format!(
                        "external column '{}' carries inconsistent built-in metadata",
                        column.name
                    )));
                }
            } else if column.data_type == DataType::Null {
                return Err(Error::InvalidArgument(format!(
                    "stored column '{}' cannot use NULL as a built-in type",
                    column.name
                )));
            }
            if column.data_type != DataType::Vector && column.vector_dimensions != 0 {
                return Err(Error::InvalidArgument(format!(
                    "non-VECTOR column '{}' carries VECTOR dimensions",
                    column.name
                )));
            }
            if column.data_type == DataType::Decimal {
                if column.decimal_precision > 38
                    || (column.decimal_precision == 0 && column.decimal_scale != 0)
                    || column.decimal_scale > column.decimal_precision
                {
                    return Err(Error::InvalidArgument(format!(
                        "column '{}' has invalid DECIMAL({},{}) parameters",
                        column.name, column.decimal_precision, column.decimal_scale
                    )));
                }
            } else if column.decimal_precision != 0 || column.decimal_scale != 0 {
                return Err(Error::InvalidArgument(format!(
                    "non-DECIMAL column '{}' carries DECIMAL parameters",
                    column.name
                )));
            }
            if let Some(default_value) = &column.default_value {
                column.validate_declared_value(default_value)?;
            }
            primary_keys += usize::from(column.primary_key);
        }
        if primary_keys > 1 {
            return Err(Error::NotSupported(
                "schemas support exactly one PRIMARY KEY column".to_string(),
            ));
        }
        self.validate_foreign_key_invariants()?;
        self.validate_constraint_catalog()
    }

    /// Mark the schema as updated (sets updated_at to now)
    pub fn mark_updated(&mut self) {
        self.updated_at = Utc::now();
    }

    /// Complete an in-place schema mutation without reconstructing the schema.
    /// This preserves stable constraint IDs and public names.
    #[doc(hidden)]
    pub fn finish_catalog_mutation(&mut self) -> Result<()> {
        self.mark_updated();
        self.rebuild_caches();
        self.validate_structural_invariants()
    }

    /// Rename the table while keeping its normalized identity synchronized.
    pub fn rename_table(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.table_name_lower = name.to_lowercase();
        self.table_name = name;
    }

    /// Rebuild all caches after schema mutation
    /// This replaces the OnceLock fields with fresh ones containing updated values
    fn rebuild_caches(&mut self) {
        // Rebuild column names cache
        self.column_names_cache = OnceLock::new();
        let _ = self.column_names_cache.set(CompactArc::new(
            self.columns.iter().map(|c| c.name.clone()).collect(),
        ));

        // Rebuild PK cache
        self.pk_column_index_cache = OnceLock::new();
        let pk_idx = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.primary_key && col.data_type == DataType::Integer)
            .map(|(i, _)| i);
        let _ = self.pk_column_index_cache.set(pk_idx);

        // Rebuild column index map cache
        self.column_index_map_cache = OnceLock::new();
        let _ = self.column_index_map_cache.set(
            self.columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name_lower.clone(), i))
                .collect(),
        );

        // Rebuild primary key indices cache
        self.pk_indices_cache = OnceLock::new();
        let _ = self.pk_indices_cache.set(Arc::new(
            self.columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.primary_key)
                .map(|(i, _)| i)
                .collect(),
        ));

        // Rebuild lowercase column names cache
        self.column_names_lower_cache = OnceLock::new();
        let _ = self.column_names_lower_cache.set(CompactArc::new(
            self.columns.iter().map(|c| c.name_lower.clone()).collect(),
        ));
    }

    /// Add a column to the schema
    pub fn add_column(&mut self, column: SchemaColumn) -> Result<()> {
        // Check for duplicate column name
        if self.has_column(&column.name) {
            return Err(Error::DuplicateColumn);
        }
        self.columns.push(column);
        self.mark_updated();
        self.rebuild_caches();
        Ok(())
    }

    /// Remove a column by name
    pub fn remove_column(&mut self, name: &str) -> Result<SchemaColumn> {
        let idx = self
            .get_column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;

        if let Some(constraint) = self.constraints.iter().find(|constraint| {
            constraint
                .kind
                .columns()
                .iter()
                .any(|column| column.eq_ignore_ascii_case(name))
        }) {
            return Err(Error::InvalidArgument(format!(
                "cannot drop column '{}' because constraint '{}' depends on it",
                self.columns[idx].name, constraint.name
            )));
        }

        if self.foreign_keys.iter().any(|fk| fk.column_index == idx) {
            return Err(Error::InvalidArgument(format!(
                "cannot drop column '{}' because it owns a foreign key constraint",
                self.columns[idx].name
            )));
        }

        let column = self.columns.remove(idx);

        // Re-index remaining columns
        for (i, col) in self.columns.iter_mut().enumerate() {
            col.id = i;
        }

        // Keep every surviving FK ordinal aligned with its local column.
        for fk in &mut self.foreign_keys {
            if fk.column_index > idx {
                fk.column_index -= 1;
            }
            fk.column_name = self.columns[fk.column_index].name.clone();
        }

        self.mark_updated();
        self.rebuild_caches();
        self.validate_foreign_key_invariants()?;
        Ok(column)
    }

    /// Rename a column
    pub fn rename_column(&mut self, old_name: &str, new_name: impl Into<String>) -> Result<()> {
        let new_name = new_name.into();

        // Check new name doesn't exist
        if self.has_column(&new_name) {
            return Err(Error::DuplicateColumn);
        }

        let idx = self
            .get_column_index(old_name)
            .ok_or_else(|| Error::ColumnNotFound(old_name.to_string()))?;

        self.columns[idx].name_lower = new_name.to_lowercase();
        self.columns[idx].name = new_name;
        for fk in &mut self.foreign_keys {
            if fk.column_index == idx {
                fk.column_name = self.columns[idx].name.clone();
            }
        }
        for constraint in &mut self.constraints {
            match &mut constraint.kind {
                SchemaConstraintKind::PrimaryKey { columns }
                | SchemaConstraintKind::Unique { columns, .. }
                | SchemaConstraintKind::ForeignKey { columns, .. } => {
                    for column in columns {
                        if column.eq_ignore_ascii_case(old_name) {
                            *column = self.columns[idx].name.clone();
                        }
                    }
                }
                SchemaConstraintKind::Check {
                    column_name: Some(column_name),
                    ..
                } if column_name.eq_ignore_ascii_case(old_name) => {
                    *column_name = self.columns[idx].name.clone();
                }
                SchemaConstraintKind::Check { .. } => {}
            }
        }
        self.mark_updated();
        self.rebuild_caches();
        self.validate_foreign_key_invariants()?;
        Ok(())
    }

    /// Modify a column's properties (except name)
    pub fn modify_column(
        &mut self,
        name: &str,
        data_type: Option<DataType>,
        nullable: Option<bool>,
    ) -> Result<()> {
        let idx = self
            .get_column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;

        if let Some(dt) = data_type {
            self.columns[idx].data_type = dt;
        }
        if let Some(n) = nullable {
            self.columns[idx].nullable = n;
        }

        self.mark_updated();
        self.rebuild_caches();
        Ok(())
    }

    /// Set or replace a column default expression and its pre-computed value.
    pub fn set_column_default(
        &mut self,
        name: &str,
        default_expr: Option<String>,
        default_value: Option<Value>,
    ) -> Result<()> {
        let idx = self
            .get_column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;

        self.columns[idx].default_expr = default_expr;
        self.columns[idx].default_value = default_value;

        self.mark_updated();
        self.rebuild_caches();
        Ok(())
    }

    /// Set or replace a column CHECK expression.
    pub fn set_column_check(&mut self, name: &str, check_expr: Option<String>) -> Result<()> {
        let idx = self
            .get_column_index(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;

        self.columns[idx].check_expr = check_expr;
        self.mark_updated();
        self.rebuild_caches();
        Ok(())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new("", Vec::new())
    }
}

fn normalize_columns(columns: &mut [SchemaColumn]) {
    for (index, column) in columns.iter_mut().enumerate() {
        column.id = index;
        column.name_lower = column.name.to_lowercase();
    }
}

/// Produce the one canonical automatic constraint name used by both the
/// runtime schema and the durable catalog binder.
///
/// Keeping this algorithm in the core contract prevents the two owners from
/// assigning different public names when a readable base collides or exceeds
/// the identifier ceiling.
#[doc(hidden)]
pub fn generated_constraint_name(
    base: &str,
    kind: &str,
    table: &str,
    columns: &[String],
    extra_fields: &[String],
    base_conflicts: bool,
) -> String {
    if base.len() <= MAX_CONSTRAINT_NAME_BYTES && !base_conflicts {
        return base.to_owned();
    }

    fn append_field(output: &mut Vec<u8>, value: &str) {
        let bytes = value.as_bytes();
        let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(bytes);
    }

    let mut descriptor = CONSTRAINT_NAME_DESCRIPTOR_PREFIX.as_bytes().to_vec();
    append_field(&mut descriptor, kind);
    append_field(&mut descriptor, &table.to_lowercase());
    for column in columns {
        append_field(&mut descriptor, &column.to_lowercase());
    }
    for field in extra_fields {
        append_field(&mut descriptor, &field.to_lowercase());
    }
    let digest = Sha256::digest(&descriptor);
    let suffix = format!(
        "__{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    );
    let max_base = MAX_CONSTRAINT_NAME_BYTES - CONSTRAINT_HASH_SUFFIX_BYTES;
    let mut end = base.len().min(max_base);
    while !base.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &base[..end], suffix)
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table_name)?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }
        for (i, check) in self.table_checks.iter().enumerate() {
            if !self.columns.is_empty() || i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "CHECK ({})", check)?;
        }
        write!(f, ")")
    }
}

/// Builder for creating schemas more ergonomically
pub struct SchemaBuilder {
    table_name: String,
    columns: Vec<SchemaColumn>,
    foreign_keys: Vec<ForeignKeyConstraint>,
    table_checks: Vec<String>,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            foreign_keys: Vec::new(),
            table_checks: Vec::new(),
        }
    }

    /// Continue binding constraints against an existing schema.
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            table_name: schema.table_name.clone(),
            columns: schema.columns.clone(),
            foreign_keys: schema.foreign_keys.clone(),
            table_checks: schema.table_checks.clone(),
        }
    }

    /// Add a column
    pub fn column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
    ) -> Self {
        let id = self.columns.len();
        self.columns.push(SchemaColumn::new(
            id,
            name,
            data_type,
            nullable,
            primary_key,
        ));
        self
    }

    /// Add a simple non-nullable column
    pub fn add(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.column(name, data_type, false, false)
    }

    /// Add a nullable column
    pub fn add_nullable(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.column(name, data_type, true, false)
    }

    /// Add a primary key column
    pub fn add_primary_key(self, name: impl Into<String>, data_type: DataType) -> Self {
        self.column(name, data_type, false, true)
    }

    /// Add a column with full constraints (default, check)
    #[allow(clippy::too_many_arguments)]
    pub fn add_with_constraints(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
        auto_increment: bool,
        default_expr: Option<String>,
        check_expr: Option<String>,
    ) -> Self {
        let id = self.columns.len();
        self.columns.push(SchemaColumn::with_constraints(
            id,
            name,
            data_type,
            nullable,
            primary_key,
            auto_increment,
            default_expr,
            check_expr,
        ));
        self
    }

    /// Set vector dimensions on the last added column
    pub fn set_last_vector_dimensions(mut self, dims: u16) -> Self {
        if let Some(col) = self.columns.last_mut() {
            col.vector_dimensions = dims;
        }
        self
    }

    /// Set DECIMAL precision/scale on the last added column.
    pub fn set_last_decimal_parameters(mut self, precision: u8, scale: u8) -> Self {
        if let Some(col) = self.columns.last_mut() {
            col.decimal_precision = precision;
            col.decimal_scale = scale;
        }
        self
    }

    pub fn set_last_external_type(
        mut self,
        type_ref: ExternalTypeRef,
        sql_name: impl Into<String>,
    ) -> Self {
        if let Some(column) = self.columns.last_mut() {
            column.data_type = DataType::Null;
            column.external_type = Some(type_ref);
            column.external_type_name = Some(sql_name.into());
        }
        self
    }

    /// Set pre-computed default value on the last added column.
    pub fn set_last_default_value(mut self, default_value: Option<Value>) -> Self {
        if let Some(col) = self.columns.last_mut() {
            col.default_value = default_value;
        }
        self
    }

    /// Find column index by name (case-insensitive)
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let lower = name.to_lowercase();
        self.columns
            .iter()
            .position(|c| c.name.to_lowercase() == lower)
    }

    /// Check if a column is nullable by index
    pub fn is_column_nullable(&self, idx: usize) -> bool {
        self.columns.get(idx).is_some_and(|c| c.nullable)
    }

    /// Return the declared type of a column already admitted by the builder.
    pub fn column_data_type(&self, idx: usize) -> Option<DataType> {
        self.columns.get(idx).map(|column| column.data_type)
    }

    pub fn column_logical_type(&self, idx: usize) -> Option<LogicalTypeRef> {
        self.columns.get(idx).map(SchemaColumn::logical_type)
    }

    /// Inspect a column already admitted by the builder.
    pub fn column_definition(&self, idx: usize) -> Option<&SchemaColumn> {
        self.columns.get(idx)
    }

    /// Return the single declared primary-key column, if present.
    pub fn primary_key_column(&self) -> Option<(usize, &SchemaColumn)> {
        let mut primary_keys = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key);
        let primary_key = primary_keys.next()?;
        if primary_keys.next().is_some() {
            None
        } else {
            Some(primary_key)
        }
    }

    /// Add a foreign key constraint
    pub fn add_foreign_key(mut self, fk: ForeignKeyConstraint) -> Self {
        self.foreign_keys.push(fk);
        self
    }

    /// Add a CHECK constraint evaluated against the complete row.
    pub fn add_table_check(mut self, expression: impl Into<String>) -> Self {
        self.table_checks.push(expression.into());
        self
    }

    /// Build the schema
    pub fn build(self) -> Schema {
        Schema::with_constraints(
            self.table_name,
            self.columns,
            self.foreign_keys,
            self.table_checks,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        SchemaBuilder::new("users")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .add_nullable("email", DataType::Text)
            .add("active", DataType::Boolean)
            .build()
    }

    #[test]
    fn test_schema_column_creation() {
        let col = SchemaColumn::new(0, "id", DataType::Integer, false, true);
        assert_eq!(col.id, 0);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Integer);
        assert!(!col.nullable);
        assert!(col.primary_key);
    }

    #[test]
    fn test_schema_column_helpers() {
        let simple = SchemaColumn::simple(0, "name", DataType::Text);
        assert!(!simple.nullable);
        assert!(!simple.primary_key);

        let nullable = SchemaColumn::nullable(1, "email", DataType::Text);
        assert!(nullable.nullable);
        assert!(!nullable.primary_key);

        let pk = SchemaColumn::primary_key(2, "id", DataType::Integer);
        assert!(!pk.nullable);
        assert!(pk.primary_key);
    }

    #[test]
    fn test_schema_creation() {
        let schema = create_test_schema();
        assert_eq!(schema.table_name, "users");
        assert_eq!(schema.column_count(), 4);
        assert!(!schema.is_empty());
    }

    #[test]
    fn test_schema_find_column() {
        let schema = create_test_schema();

        // Find by exact name
        let (idx, col) = schema.find_column("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.name, "name");

        // Case-insensitive
        let (idx, _) = schema.find_column("NAME").unwrap();
        assert_eq!(idx, 1);

        // Not found
        assert!(schema.find_column("nonexistent").is_none());
    }

    #[test]
    fn test_schema_get_column() {
        let schema = create_test_schema();

        let col = schema.get_column(0).unwrap();
        assert_eq!(col.name, "id");

        let col = schema.get_column_by_name("email").unwrap();
        assert_eq!(col.data_type, DataType::Text);
        assert!(col.nullable);

        assert!(schema.get_column(100).is_none());
    }

    #[test]
    fn test_schema_column_names() {
        let schema = create_test_schema();
        let names = schema.column_names();
        assert_eq!(names, vec!["id", "name", "email", "active"]);
    }

    #[test]
    fn test_schema_primary_key() {
        let schema = create_test_schema();

        assert!(schema.has_primary_key());

        let pk_cols = schema.primary_key_columns();
        assert_eq!(pk_cols.len(), 1);
        assert_eq!(pk_cols[0].name, "id");

        let pk_indices = schema.primary_key_indices();
        assert_eq!(pk_indices, vec![0]);
    }

    #[test]
    fn orm_01_constraint_names_ids_collisions_and_ordinals_are_stable() {
        let mut schema = SchemaBuilder::new("constraint_collision")
            .add_primary_key("id", DataType::Integer)
            .add("a_b", DataType::Text)
            .add("c", DataType::Text)
            .add("a", DataType::Text)
            .add("b_c", DataType::Text)
            .build();

        assert_eq!(
            schema
                .register_primary_key_constraint(vec!["id".to_string()])
                .unwrap(),
            "pk_constraint_collision"
        );
        let first = schema
            .register_unique_constraint(vec!["a_b".to_string(), "c".to_string()])
            .unwrap();
        let second = schema
            .register_unique_constraint(vec!["a".to_string(), "b_c".to_string()])
            .unwrap();
        assert_eq!(first, "uq_constraint_collision_a_b_c");
        assert!(second.starts_with("uq_constraint_collision_a_b_c__"));
        assert_eq!(second.len(), first.len() + 10);

        let first_check = schema
            .register_check_constraint(None, "id >= 0".to_string())
            .unwrap();
        schema.table_checks.push("id >= 0".to_string());
        // Registration precedes enforcement only in this narrow unit setup;
        // install the owner before validating/removing the entry.
        assert_eq!(first_check, "chk_constraint_collision_1");
        let first_check_id = schema.find_constraint(&first_check).unwrap().id;
        schema.take_constraint(&first_check);
        schema.table_checks.clear();
        schema.table_checks.push("id <= 100".to_string());
        let second_check = schema
            .register_check_constraint(None, "id <= 100".to_string())
            .unwrap();
        assert_eq!(second_check, "chk_constraint_collision_2");
        assert!(schema.find_constraint(&second_check).unwrap().id > first_check_id);

        let stable = schema
            .constraints()
            .iter()
            .map(|constraint| (constraint.id, constraint.name.clone()))
            .collect::<Vec<_>>();
        schema.rename_column("a_b", "renamed").unwrap();
        schema.rename_table("renamed_table");
        assert_eq!(
            schema
                .constraints()
                .iter()
                .map(|constraint| (constraint.id, constraint.name.clone()))
                .collect::<Vec<_>>(),
            stable
        );
        schema.validate_constraint_catalog().unwrap();
    }

    #[test]
    fn orm_01_long_constraint_name_is_utf8_bounded_and_deterministic() {
        let table = "таблица_".repeat(20);
        let column = "колонка_".repeat(20);
        let mut first = SchemaBuilder::new(&table)
            .add(&column, DataType::Text)
            .build();
        let mut second = first.clone();
        let first_name = first
            .register_unique_constraint(vec![column.clone()])
            .unwrap();
        let second_name = second.register_unique_constraint(vec![column]).unwrap();
        assert_eq!(first_name, second_name);
        assert!(first_name.len() <= MAX_CONSTRAINT_NAME_BYTES);
        assert!(first_name.is_char_boundary(first_name.len()));
        assert_eq!(first_name.rsplit_once("__").unwrap().1.len(), 8);
    }

    #[test]
    fn test_schema_validate_column_count() {
        let schema = create_test_schema();

        assert!(schema.validate_column_count(4).is_ok());

        let err = schema.validate_column_count(3).unwrap_err();
        assert!(matches!(
            err,
            Error::TableColumnsNotMatch {
                expected: 3,
                got: 4
            }
        ));
    }

    #[test]
    fn test_schema_add_column() {
        let mut schema = create_test_schema();
        let original_count = schema.column_count();

        schema
            .add_column(SchemaColumn::simple(
                original_count,
                "age",
                DataType::Integer,
            ))
            .unwrap();

        assert_eq!(schema.column_count(), original_count + 1);
        assert!(schema.has_column("age"));

        // Duplicate column should fail
        let err = schema
            .add_column(SchemaColumn::simple(0, "age", DataType::Integer))
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateColumn));
    }

    #[test]
    fn test_schema_remove_column() {
        let mut schema = create_test_schema();

        let removed = schema.remove_column("email").unwrap();
        assert_eq!(removed.name, "email");
        assert_eq!(schema.column_count(), 3);
        assert!(!schema.has_column("email"));

        // Column IDs should be re-indexed
        assert_eq!(schema.columns[2].id, 2);

        // Removing non-existent column should fail
        assert!(schema.remove_column("nonexistent").is_err());
    }

    #[test]
    fn test_schema_rename_column() {
        let mut schema = create_test_schema();

        schema.rename_column("name", "full_name").unwrap();
        assert!(schema.has_column("full_name"));
        assert!(!schema.has_column("name"));

        // Renaming to existing name should fail
        let err = schema.rename_column("full_name", "id").unwrap_err();
        assert!(matches!(err, Error::DuplicateColumn));

        // Renaming non-existent column should fail
        assert!(schema.rename_column("nonexistent", "new_name").is_err());
    }

    fn schema_with_foreign_key() -> Schema {
        SchemaBuilder::new("children")
            .add_primary_key("id", DataType::Integer)
            .add("prefix", DataType::Text)
            .add("parent_id", DataType::Integer)
            .add_foreign_key(ForeignKeyConstraint {
                column_index: 2,
                column_name: "parent_id".to_string(),
                referenced_table: "parents".to_string(),
                referenced_column: "id".to_string(),
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
            })
            .build()
    }

    #[test]
    fn test_schema_mutation_preserves_foreign_key_identity() {
        let mut schema = schema_with_foreign_key();

        schema.remove_column("prefix").unwrap();
        assert_eq!(schema.foreign_keys[0].column_index, 1);
        assert_eq!(schema.foreign_keys[0].column_name, "parent_id");
        schema.validate_foreign_key_invariants().unwrap();

        schema.rename_column("parent_id", "owner_id").unwrap();
        assert_eq!(schema.foreign_keys[0].column_index, 1);
        assert_eq!(schema.foreign_keys[0].column_name, "owner_id");
        schema.validate_foreign_key_invariants().unwrap();
    }

    #[test]
    fn test_schema_rejects_dropping_or_decoding_invalid_foreign_key_identity() {
        let mut schema = schema_with_foreign_key();
        let error = schema.remove_column("parent_id").unwrap_err();
        assert!(error.to_string().contains("owns a foreign key constraint"));

        schema.foreign_keys[0].column_index = 99;
        let error = schema.validate_foreign_key_invariants().unwrap_err();
        assert!(error.to_string().contains("out of bounds"));

        schema.foreign_keys[0].column_index = 2;
        schema.foreign_keys[0].column_name = "wrong_column".to_string();
        let error = schema.validate_foreign_key_invariants().unwrap_err();
        assert!(error.to_string().contains("identity mismatch"));
    }

    #[test]
    fn test_schema_modify_column() {
        let mut schema = create_test_schema();

        schema
            .modify_column("name", Some(DataType::Json), Some(true))
            .unwrap();

        let col = schema.get_column_by_name("name").unwrap();
        assert_eq!(col.data_type, DataType::Json);
        assert!(col.nullable);

        // Modifying non-existent column should fail
        assert!(schema
            .modify_column("nonexistent", None, Some(true))
            .is_err());
    }

    #[test]
    fn test_schema_column_display() {
        let col = SchemaColumn::new(0, "id", DataType::Integer, false, true);
        assert_eq!(col.to_string(), "id INTEGER PRIMARY KEY");

        let col = SchemaColumn::new(1, "name", DataType::Text, false, false);
        assert_eq!(col.to_string(), "name TEXT NOT NULL");

        let col = SchemaColumn::new(2, "email", DataType::Text, true, false);
        assert_eq!(col.to_string(), "email TEXT");
    }

    #[test]
    fn test_schema_display() {
        let schema = SchemaBuilder::new("users")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .build();

        let expected = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        assert_eq!(schema.to_string(), expected);
    }

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::new("products")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .add_nullable("description", DataType::Text)
            .add("price", DataType::Float)
            .build();

        assert_eq!(schema.table_name, "products");
        assert_eq!(schema.column_count(), 4);
        assert!(schema.get_column_by_name("id").unwrap().primary_key);
        assert!(schema.get_column_by_name("description").unwrap().nullable);
    }

    #[test]
    fn test_schema_timestamps() {
        let schema1 = Schema::new("test", vec![]);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let schema2 = Schema::new("test", vec![]);

        // Different creation times
        assert!(schema2.created_at >= schema1.created_at);
    }

    #[test]
    fn test_schema_get_column_type() {
        let schema = create_test_schema();

        assert_eq!(schema.get_column_type("id"), Some(DataType::Integer));
        assert_eq!(schema.get_column_type("name"), Some(DataType::Text));
        assert_eq!(schema.get_column_type("active"), Some(DataType::Boolean));
        assert_eq!(schema.get_column_type("nonexistent"), None);
    }

    #[test]
    fn schema_constructor_rebuilds_authoritative_column_identity() {
        let mut column = SchemaColumn::new(99, "old", DataType::Integer, false, true);
        column.name = "Renamed".to_string();
        let mut schema = Schema::new("MixedCase", vec![column]);

        assert_eq!(schema.table_name(), "MixedCase");
        assert_eq!(schema.columns()[0].id, 0);
        assert_eq!(schema.get_column_index("renamed"), Some(0));
        assert_eq!(schema.column_names_owned(), &["Renamed".to_string()]);
        assert!(schema.validate_structural_invariants().is_ok());

        schema.rename_table("OtherName");
        assert_eq!(schema.table_name(), "OtherName");
        assert_eq!(schema.table_name_lower, "othername");
    }

    #[test]
    fn structural_validation_rejects_impossible_key_and_vector_metadata() {
        let mut nullable_pk = SchemaBuilder::new("nullable_pk")
            .add_primary_key("id", DataType::Integer)
            .build();
        nullable_pk.columns[0].nullable = true;
        assert!(nullable_pk
            .validate_structural_invariants()
            .unwrap_err()
            .to_string()
            .contains("cannot be nullable"));

        let mut wrong_auto = SchemaBuilder::new("wrong_auto")
            .add("id", DataType::Text)
            .build();
        wrong_auto.columns[0].auto_increment = true;
        assert!(wrong_auto
            .validate_structural_invariants()
            .unwrap_err()
            .to_string()
            .contains("AUTO_INCREMENT"));

        let mut stale_vector = SchemaBuilder::new("stale_vector")
            .add("value", DataType::Text)
            .build();
        stale_vector.columns[0].vector_dimensions = 3;
        assert!(stale_vector
            .validate_structural_invariants()
            .unwrap_err()
            .to_string()
            .contains("VECTOR dimensions"));
    }
}
