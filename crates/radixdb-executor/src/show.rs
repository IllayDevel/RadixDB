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

//! SHOW and DESCRIBE statement execution
//!
//! This module handles metadata query commands:
//! - SHOW TABLES
//! - SHOW VIEWS
//! - SHOW CREATE TABLE
//! - SHOW CREATE VIEW
//! - SHOW INDEXES
//! - DESCRIBE

use rustc_hash::FxHashSet;
use std::collections::BTreeMap;

use radixdb_catalog::{CatalogPayload, ObjectKind};
use radixdb_core::row_vec::RowVec;
use radixdb_core::SmartString;
use radixdb_core::{
    ColumnDescriptor, ConstraintDefinition, ConstraintDescriptor, DataTypeDescriptor,
    DatabaseDescriptor, DescriptorEnvelope, DescriptorKind, ForeignKeyActionDescriptor,
    IndexDescriptor, ResultColumnDescriptor, TableDescriptor, ViewDescriptor,
};
use radixdb_core::{DataType, Error, ForeignKeyAction, Result, Row, SchemaConstraintKind, Value};
use radixdb_sql::ast::*;
use radixdb_storage::traits::{Engine, QueryResult};

use super::context::ExecutionContext;
use super::result::ExecutorResult;
use super::Executor;

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn descriptor_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}

fn plugin_requirement_metadata(executor: &Executor) -> Result<Option<serde_json::Value>> {
    let (catalog, _) = crate::procedural::transaction_visible_catalog(executor)?;
    let mut extension_names = BTreeMap::new();
    let mut extensions = Vec::new();
    for object in catalog.objects_of_kind(ObjectKind::Extension) {
        let CatalogPayload::Extension(payload) = object.payload() else {
            return Err(Error::internal(
                "catalog admitted Extension with a different payload",
            ));
        };
        let name = object.name().display().as_str().to_owned();
        extension_names.insert(object.id(), name.clone());
        extensions.push(serde_json::json!({
            "sql_name": name,
            "package_id": uuid::Uuid::from_bytes(payload.package_id().into_bytes()).to_string(),
            "version": payload.version(),
            "abi_major": payload.abi_major(),
            "abi_min_minor": payload.abi_min_minor(),
            "abi_max_minor": payload.abi_max_minor(),
            "descriptor_fingerprint": descriptor_hex(payload.descriptor_fingerprint()),
        }));
    }
    extensions.sort_by(|left, right| left["sql_name"].as_str().cmp(&right["sql_name"].as_str()));

    let mut types = Vec::new();
    for object in catalog.objects_of_kind(ObjectKind::ExternalType) {
        let CatalogPayload::ExternalType(payload) = object.payload() else {
            return Err(Error::internal(
                "catalog admitted ExternalType with a different payload",
            ));
        };
        let extension_name = extension_names
            .get(&payload.extension_binding_id())
            .ok_or_else(|| Error::internal("external type extension binding disappeared"))?;
        types.push(serde_json::json!({
            "sql_name": crate::catalog::qualified_catalog_name(catalog.as_ref(), object)?,
            "extension": extension_name,
            "local_id": payload.local_id(),
            "type_object_id": uuid::Uuid::from_bytes(object.id().into_bytes()).to_string(),
            "codec_version": payload.write_codec_version(),
            "semantic_revision": payload.semantic_revision(),
            "storage_kind": payload.storage_kind().tag(),
            "fixed_bytes": payload.fixed_bytes(),
            "max_payload_bytes": payload.max_canonical_payload_bytes(),
            "codec_fingerprint": descriptor_hex(payload.codec_fingerprint()),
            "capabilities": payload.capabilities(),
        }));
    }
    types.sort_by(|left, right| left["sql_name"].as_str().cmp(&right["sql_name"].as_str()));

    if extensions.is_empty() && types.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::json!({
            "extensions": extensions,
            "types": types,
        })))
    }
}

fn descriptor_data_type(
    data_type: DataType,
    vector_dimensions: u16,
    decimal_precision: u8,
    decimal_scale: u8,
) -> DataTypeDescriptor {
    match data_type {
        DataType::Null => DataTypeDescriptor::Null,
        DataType::Integer => DataTypeDescriptor::Integer,
        DataType::Float => DataTypeDescriptor::Float,
        DataType::Text => DataTypeDescriptor::Text,
        DataType::Boolean => DataTypeDescriptor::Boolean,
        DataType::Timestamp => DataTypeDescriptor::Timestamp,
        DataType::Date => DataTypeDescriptor::Date,
        DataType::Json => DataTypeDescriptor::Json,
        DataType::Uuid => DataTypeDescriptor::Uuid,
        DataType::Bytes => DataTypeDescriptor::Bytes,
        DataType::Decimal => DataTypeDescriptor::Decimal {
            precision: (decimal_precision > 0).then_some(decimal_precision),
            scale: (decimal_precision > 0).then_some(decimal_scale),
        },
        DataType::Vector => DataTypeDescriptor::Vector {
            dimensions: vector_dimensions,
        },
    }
}

fn descriptor_fk_action(action: ForeignKeyAction) -> ForeignKeyActionDescriptor {
    match action {
        ForeignKeyAction::Restrict => ForeignKeyActionDescriptor::Restrict,
        ForeignKeyAction::Cascade => ForeignKeyActionDescriptor::Cascade,
        ForeignKeyAction::SetNull => ForeignKeyActionDescriptor::SetNull,
        ForeignKeyAction::NoAction => ForeignKeyActionDescriptor::NoAction,
    }
}

fn descriptor_error(error: impl std::fmt::Display) -> Error {
    Error::internal(format!("failed to build schema descriptor: {error}"))
}

/// Derive a durable opaque generation token from canonical descriptor content.
///
/// The engine's `schema_epoch` is deliberately process-local and resets on
/// reopen, so it must never leak into the public descriptor or its fingerprint.
/// A content generation remains byte-stable across embedded/TCP/reopen while
/// changing whenever any catalog-visible schema field changes.
fn descriptor_content_generation(seed_fingerprint: &str) -> Result<u64> {
    let prefix = seed_fingerprint.get(..16).ok_or_else(|| {
        descriptor_error("canonical fingerprint is shorter than 64 hexadecimal digits")
    })?;
    u64::from_str_radix(prefix, 16)
        .map_err(|error| descriptor_error(format!("invalid fingerprint prefix: {error}")))
}

impl Executor {
    fn catalog_list_tables(&self) -> Result<Vec<String>> {
        let active = self.active_transaction.lock().unwrap();
        if let Some(state) = active.as_ref() {
            return state.transaction.list_tables();
        }
        self.engine.begin_transaction()?.list_tables()
    }

    fn catalog_get_table(&self, name: &str) -> Result<Box<dyn radixdb_storage::traits::Table>> {
        let active = self.active_transaction.lock().unwrap();
        if let Some(state) = active.as_ref() {
            return state.transaction.get_table(name);
        }
        self.engine.begin_transaction()?.get_table(name)
    }

    /// Execute SHOW TABLES statement
    pub(crate) fn execute_show_tables(
        &self,
        _stmt: &ShowTablesStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let tables = self.catalog_list_tables()?;

        let columns = vec!["table_name".to_string()];
        let mut rows = RowVec::with_capacity(tables.len());
        for (i, name) in tables.into_iter().enumerate() {
            rows.push((
                i as i64,
                Row::from_values(vec![Value::Text(SmartString::from_string(name))]),
            ));
        }

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute SHOW VIEWS statement
    pub(crate) fn execute_show_views(
        &self,
        _stmt: &ShowViewsStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let views = self.visible_view_names()?;

        let columns = vec!["view_name".to_string()];
        let mut rows = RowVec::with_capacity(views.len());
        for (i, name) in views.into_iter().enumerate() {
            rows.push((
                i as i64,
                Row::from_values(vec![Value::Text(SmartString::from_string(name))]),
            ));
        }

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute SHOW CREATE TABLE statement
    pub(crate) fn execute_show_create_table(
        &self,
        stmt: &ShowCreateTableStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;
        let table = self.catalog_get_table(table_name)?;
        let schema = table.schema();

        // Build CREATE TABLE statement
        let mut create_sql = format!("CREATE TABLE {} (", quote_identifier(table_name));
        let col_defs: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let data_type = col.formatted_data_type();
                let mut def = format!("{} {}", quote_identifier(&col.name), data_type);
                if col.primary_key {
                    def.push_str(" PRIMARY KEY");
                } else {
                    if !col.nullable {
                        def.push_str(" NOT NULL");
                    }
                }
                if col.auto_increment {
                    def.push_str(" AUTO_INCREMENT");
                }
                // Add DEFAULT if present
                if let Some(default_expr) = &col.default_expr {
                    def.push_str(&format!(" DEFAULT {}", default_expr));
                }
                // Add CHECK constraint if present
                if let Some(check) = &col.check_expr {
                    def.push_str(&format!(" CHECK ({})", check));
                }
                def
            })
            .collect();
        create_sql.push_str(&col_defs.join(", "));

        for check in &schema.table_checks {
            create_sql.push_str(&format!(", CHECK ({check})"));
        }

        for columns in schema
            .constraints()
            .iter()
            .filter_map(|constraint| match &constraint.kind {
                SchemaConstraintKind::Unique { columns, .. } => Some(columns),
                _ => None,
            })
        {
            let columns = columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            create_sql.push_str(&format!(", UNIQUE ({columns})"));
        }

        // Add foreign key constraints
        for fk in &schema.foreign_keys {
            use std::fmt::Write;
            write!(
                create_sql,
                ", FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE {} ON UPDATE {}",
                quote_identifier(&fk.column_name),
                quote_identifier(&fk.referenced_table),
                quote_identifier(&fk.referenced_column),
                fk.on_delete,
                fk.on_update
            )
            .unwrap();
        }

        create_sql.push(')');

        let columns = vec!["Table".to_string(), "Create Table".to_string()];
        let mut rows = RowVec::with_capacity(1);
        rows.push((
            0,
            Row::from_values(vec![
                Value::Text(SmartString::new(table_name)),
                Value::Text(SmartString::from_string(create_sql)),
            ]),
        ));

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute SHOW CREATE VIEW statement
    pub(crate) fn execute_show_create_view(
        &self,
        stmt: &ShowCreateViewStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let view_name = &stmt.view_name.value;

        // Get the view definition
        let view_def = self
            .visible_view(view_name)?
            .ok_or_else(|| Error::ViewNotFound(view_name.to_string()))?;

        // Build CREATE VIEW statement
        let create_sql = format!(
            "CREATE VIEW {} AS {}",
            view_def.original_name, view_def.query
        );

        let columns = vec!["View".to_string(), "Create View".to_string()];
        let mut rows = RowVec::with_capacity(1);
        rows.push((
            0,
            Row::from_values(vec![
                Value::Text(SmartString::new(&view_def.original_name)),
                Value::Text(SmartString::from_string(create_sql)),
            ]),
        ));

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Execute SHOW INDEXES statement
    pub(crate) fn execute_show_indexes(
        &self,
        stmt: &ShowIndexesStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let table_name = &stmt.table_name.value;

        let table = self.catalog_get_table(table_name)?;
        let indexes = table.get_indexes();

        // Build rows with: table_name, index_name, column_name, index_type, is_unique, options
        let columns = vec![
            "table_name".to_string(),
            "index_name".to_string(),
            "column_name".to_string(),
            "index_type".to_string(),
            "is_unique".to_string(),
            "options".to_string(),
        ];

        let mut rows = RowVec::with_capacity(indexes.len());
        for (row_id, index) in indexes.into_iter().enumerate() {
            let index_name = index.name().to_string();
            let column_names = index.column_names();
            // Show all columns for multi-column indexes
            let column_name = if column_names.len() > 1 {
                format!("({})", column_names.join(", "))
            } else {
                column_names
                    .first()
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            };
            let is_unique = index.is_unique();
            let index_type = index.index_type().as_str().to_uppercase();

            let mut options = Vec::new();

            // Build options string for HNSW indexes
            if index_type == "HNSW" {
                let metric = match index.hnsw_distance_metric() {
                    Some(0) => "l2",
                    Some(1) => "cosine",
                    Some(2) => "ip",
                    _ => "cosine",
                };
                let m = index.hnsw_m().unwrap_or(16);
                let ef = index.hnsw_ef_construction().unwrap_or(200);
                let ef_search = index.default_ef_search().unwrap_or(50);
                options.push(format!(
                    "metric={}, m={}, ef_construction={}, ef_search={}",
                    metric, m, ef, ef_search
                ));
            }

            if let Some(predicate) = index.partial_predicate() {
                options.push(format!("where={}", predicate.canonical_sql()));
            }

            let options = options.join(", ");

            rows.push((
                row_id as i64,
                Row::from_values(vec![
                    Value::Text(SmartString::new(table_name)),
                    Value::Text(SmartString::new(&index_name)),
                    Value::Text(SmartString::from_string(column_name)),
                    Value::Text(SmartString::from_string(index_type)),
                    Value::Boolean(is_unique),
                    Value::Text(SmartString::from_string(options)),
                ]),
            ));
        }

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    fn table_descriptor(&self, table_name: &str) -> Result<TableDescriptor> {
        let table = self.catalog_get_table(table_name)?;
        let schema = table.schema();

        let columns = schema
            .columns()
            .iter()
            .enumerate()
            .map(|(ordinal, column)| {
                let mut extensions = BTreeMap::new();
                if let (Some(type_ref), Some(sql_name)) =
                    (column.external_type, column.external_type_name.as_deref())
                {
                    extensions.insert(
                        "radixdb.external_type.v1".to_owned(),
                        serde_json::json!({
                            "sql_name": sql_name,
                            "type_object_id": uuid::Uuid::from_bytes(type_ref.type_object_id()).to_string(),
                            "codec_version": type_ref.codec_version(),
                        }),
                    );
                }
                ColumnDescriptor {
                    ordinal: ordinal as u32,
                    name: column.name.clone(),
                    data_type: descriptor_data_type(
                        column.data_type,
                        column.vector_dimensions,
                        column.decimal_precision,
                        column.decimal_scale,
                    ),
                    nullable: column.nullable,
                    auto_increment: column.auto_increment,
                    default_expression: column.default_expr.clone(),
                    extensions,
                }
            })
            .collect();

        let constraints = schema
            .constraints()
            .iter()
            .map(|constraint| ConstraintDescriptor {
                id: constraint.id,
                name: constraint.name.clone(),
                definition: match &constraint.kind {
                    SchemaConstraintKind::PrimaryKey { columns } => {
                        ConstraintDefinition::PrimaryKey {
                            columns: columns.clone(),
                        }
                    }
                    SchemaConstraintKind::Unique {
                        columns,
                        index_name,
                    } => ConstraintDefinition::Unique {
                        columns: columns.clone(),
                        owned_index: index_name.clone(),
                    },
                    SchemaConstraintKind::ForeignKey {
                        columns,
                        referenced_table,
                        referenced_columns,
                        on_delete,
                        on_update,
                    } => ConstraintDefinition::ForeignKey {
                        columns: columns.clone(),
                        referenced_table: referenced_table.clone(),
                        referenced_columns: referenced_columns.clone(),
                        on_delete: descriptor_fk_action(*on_delete),
                        on_update: descriptor_fk_action(*on_update),
                    },
                    SchemaConstraintKind::Check {
                        column_name,
                        expression,
                        ordinal,
                    } => ConstraintDefinition::Check {
                        column: column_name.clone(),
                        expression: expression.clone(),
                        ordinal: *ordinal,
                    },
                },
            })
            .collect();

        let mut indexes = table
            .get_indexes()
            .into_iter()
            .map(|index| {
                let mut options = BTreeMap::new();
                if index.index_type() == radixdb_core::IndexType::Hnsw {
                    options.insert(
                        "distance_metric".to_string(),
                        serde_json::Value::String(
                            match index.hnsw_distance_metric() {
                                Some(0) => "l2",
                                Some(1) => "cosine",
                                Some(2) => "ip",
                                _ => "cosine",
                            }
                            .to_string(),
                        ),
                    );
                    options.insert(
                        "m".to_string(),
                        serde_json::Value::from(index.hnsw_m().unwrap_or(16)),
                    );
                    options.insert(
                        "ef_construction".to_string(),
                        serde_json::Value::from(index.hnsw_ef_construction().unwrap_or(200)),
                    );
                    options.insert(
                        "ef_search".to_string(),
                        serde_json::Value::from(index.default_ef_search().unwrap_or(50)),
                    );
                }
                IndexDescriptor {
                    name: index.name().to_string(),
                    method: index.index_type().as_str().to_string(),
                    columns: index.column_names().to_vec(),
                    unique: index.is_unique(),
                    predicate: index
                        .partial_predicate()
                        .map(|predicate| predicate.canonical_sql().to_string()),
                    options,
                }
            })
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));

        let mut descriptor = TableDescriptor {
            catalog_id: uuid::Uuid::from_bytes(schema.catalog_id()).to_string(),
            name: schema.table_name().to_string(),
            schema_generation: 0,
            fingerprint: String::new(),
            created_at: schema
                .created_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            updated_at: schema
                .updated_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            columns,
            constraints,
            indexes,
            extensions: BTreeMap::new(),
        };
        descriptor.schema_generation = descriptor_content_generation(
            &descriptor
                .computed_fingerprint()
                .map_err(descriptor_error)?,
        )?;
        descriptor.refresh_fingerprint().map_err(descriptor_error)?;
        Ok(descriptor)
    }

    fn describe_json(&self, target: &DescribeTarget) -> Result<String> {
        match target {
            DescribeTarget::Table(table) => {
                let descriptor = self.table_descriptor(&table.value)?;
                DescriptorEnvelope::new(DescriptorKind::Table, descriptor)
                    .to_json()
                    .map_err(descriptor_error)
            }
            DescribeTarget::Database => {
                let mut table_names = self.catalog_list_tables()?;
                table_names.sort_by_key(|name| name.to_lowercase());
                let mut tables = Vec::with_capacity(table_names.len());
                for table_name in table_names {
                    tables.push(self.table_descriptor(&table_name)?);
                }

                let mut view_names = self.visible_view_names()?;
                view_names.sort_by_key(|name| name.to_lowercase());
                let mut views = Vec::with_capacity(view_names.len());
                for view_name in view_names {
                    let Some(view) = self.visible_view(&view_name)? else {
                        return Err(Error::internal(format!(
                            "view '{view_name}' disappeared during DESCRIBE DATABASE"
                        )));
                    };
                    let result_columns = self
                        .describe_query_output(&view.query)?
                        .ok_or_else(|| {
                            Error::internal(format!(
                                "persisted view '{}' is not a SELECT",
                                view.original_name
                            ))
                        })?
                        .into_iter()
                        .map(|column| ResultColumnDescriptor {
                            name: column.name,
                            data_type: descriptor_data_type(column.data_type, 0, 0, 0),
                            nullable: column.nullable,
                        })
                        .collect();
                    views.push(ViewDescriptor {
                        name: view.original_name.clone(),
                        query: view.query.clone(),
                        dependencies: view.dependencies.clone(),
                        result_columns,
                    });
                }

                let mut extensions = BTreeMap::new();
                if let Some(requirements) = plugin_requirement_metadata(self)? {
                    extensions.insert("radixdb.plugin_requirements.v1".to_owned(), requirements);
                }
                let mut descriptor = DatabaseDescriptor {
                    schema_generation: 0,
                    fingerprint: String::new(),
                    tables,
                    views,
                    extensions,
                };
                descriptor.schema_generation = descriptor_content_generation(
                    &descriptor
                        .computed_fingerprint()
                        .map_err(descriptor_error)?,
                )?;
                descriptor.refresh_fingerprint().map_err(descriptor_error)?;
                DescriptorEnvelope::new(DescriptorKind::Database, descriptor)
                    .to_json()
                    .map_err(descriptor_error)
            }
        }
    }

    /// Execute DESCRIBE statement - shows table structure
    pub(crate) fn execute_describe(
        &self,
        stmt: &DescribeStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if stmt.format == DescribeFormat::Json {
            let json = self.describe_json(&stmt.target)?;
            let mut rows = RowVec::with_capacity(1);
            rows.push((
                0,
                Row::from_values(vec![Value::Text(SmartString::from_string(json))]),
            ));
            return Ok(Box::new(ExecutorResult::new(
                vec!["descriptor_json".to_string()],
                rows,
            )));
        }

        let DescribeTarget::Table(table_name) = &stmt.target else {
            return Err(Error::invalid_argument(
                "DESCRIBE DATABASE requires FORMAT JSON",
            ));
        };
        let table_name = &table_name.value;
        let table = self.catalog_get_table(table_name)?;
        let schema = table.schema();

        // Column headers: Field, Type, Null, Key, Default, Extra
        let columns = vec![
            "Field".to_string(),
            "Type".to_string(),
            "Null".to_string(),
            "Key".to_string(),
            "Default".to_string(),
            "Extra".to_string(),
        ];

        // Build unique/multi-column key sets from indexes (matching MySQL conventions)
        // Single-column UNIQUE -> UNI, composite UNIQUE first col -> MUL
        let mut unique_columns: FxHashSet<String> = FxHashSet::default();
        let mut mul_columns: FxHashSet<String> = FxHashSet::default();
        for index in table.get_indexes() {
            let col_names = index.column_names();
            if index.is_unique() && col_names.len() == 1 {
                unique_columns.insert(col_names[0].to_lowercase());
            } else if col_names.len() > 1 {
                // MySQL shows MUL for columns that are part of a composite index
                for name in col_names {
                    mul_columns.insert(name.to_lowercase());
                }
            }
        }
        {
            let active = self.active_transaction.lock().unwrap();
            if let Some(state) = active.as_ref() {
                for definition in state.transaction.staged_index_definitions(table_name) {
                    if definition.is_unique && definition.columns.len() == 1 {
                        unique_columns.insert(definition.columns[0].to_lowercase());
                    } else if definition.columns.len() > 1 {
                        for name in &definition.columns {
                            mul_columns.insert(name.to_lowercase());
                        }
                    }
                }
            }
        }

        let mut rows = RowVec::with_capacity(schema.columns.len());
        for (i, col) in schema.columns.iter().enumerate() {
            // Determine type string
            let type_str = col.formatted_data_type();

            // Determine nullability
            let null_str = if col.nullable { "YES" } else { "NO" };

            // Determine key type (MySQL convention: PRI > UNI > MUL)
            let key_str = if col.primary_key {
                "PRI"
            } else if unique_columns.contains(&col.name_lower) {
                "UNI"
            } else if schema.foreign_keys.iter().any(|fk| fk.column_index == i)
                || mul_columns.contains(&col.name_lower)
            {
                "MUL"
            } else {
                ""
            };

            // Get default value if any
            let default_str = col
                .default_expr
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();

            // Extra info (e.g., auto_increment equivalent)
            let extra_str = if col.auto_increment {
                "auto_increment"
            } else {
                ""
            };

            rows.push((
                i as i64,
                Row::from_values(vec![
                    Value::Text(SmartString::new(&col.name)),
                    Value::Text(SmartString::from_string(type_str)),
                    Value::Text(SmartString::new(null_str)),
                    Value::Text(SmartString::new(key_str)),
                    Value::Text(SmartString::from_string(default_str)),
                    Value::Text(SmartString::new(extra_str)),
                ]),
            ));
        }

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }
}
