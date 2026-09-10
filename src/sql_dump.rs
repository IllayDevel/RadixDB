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

//! Stable logical SQL dump boundary.
//!
//! Physical database generations deliberately have no compatibility reader.
//! An engine that can open a generation exports this versioned SQL stream; a
//! newer engine imports it into an isolated empty staging database.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use radixdb_orm::{
    ConstraintDefinition, DataTypeDescriptor, DatabaseDescriptor, ForeignKeyActionDescriptor,
    IndexDescriptor, TableDescriptor, ViewDescriptor,
};
use radixdb_plugin_host::PluginRegistry;
use sha2::{Digest, Sha256};

use crate::api::Database;
use crate::parser::{parse_sql, Lexer, Statement, TokenType};
use crate::{DataType, Error, Result, Row, Value};

pub const SQL_DUMP_FORMAT: u32 = 1;
pub const MAX_DUMP_STATEMENT_BYTES: usize = 256 * 1024 * 1024;
const TARGET_INSERT_BYTES: usize = 4 * 1024 * 1024;
const TARGET_INSERT_ROWS: usize = 256;
const COUNTS_PREFIX: &str = "-- radixdb-sql-dump-counts: ";
const FOOTER_PREFIX: &str = "-- radixdb-sql-dump-sha256: ";
const PLUGIN_REQUIREMENTS_KEY: &str = "radixdb.plugin_requirements.v1";
const EXTERNAL_TYPE_KEY: &str = "radixdb.external_type.v1";

#[derive(Clone, Debug, serde::Deserialize)]
struct DumpPluginRequirements {
    extensions: Vec<DumpExtensionRequirement>,
    types: Vec<DumpExternalTypeRequirement>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct DumpExtensionRequirement {
    sql_name: String,
    package_id: String,
    version: String,
    abi_major: u16,
    abi_min_minor: u16,
    abi_max_minor: u16,
    descriptor_fingerprint: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct DumpExternalTypeRequirement {
    sql_name: String,
    extension: String,
    local_id: String,
    type_object_id: String,
    codec_version: u32,
    semantic_revision: u32,
    storage_kind: u16,
    fixed_bytes: Option<u32>,
    max_payload_bytes: u32,
    codec_fingerprint: String,
    capabilities: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct DumpColumnExternalType {
    sql_name: String,
    type_object_id: String,
    codec_version: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlDumpSummary {
    pub tables: u64,
    pub rows: u64,
    pub statements: u64,
    pub sha256: String,
}

fn dump_error(context: impl std::fmt::Display) -> Error {
    Error::internal(format!("logical SQL dump: {context}"))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_qualified_identifier(identifier: &str) -> Result<String> {
    let components = identifier.split('.').collect::<Vec<_>>();
    if components.len() < 2 || components.iter().any(|component| component.is_empty()) {
        return Err(dump_error(format!(
            "external type name '{identifier}' is not schema-qualified"
        )));
    }
    Ok(components
        .into_iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join("."))
}

fn quote_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("String writes cannot fail");
    }
    encoded
}

fn plugin_requirements(descriptor: &DatabaseDescriptor) -> Result<Option<DumpPluginRequirements>> {
    descriptor
        .extensions
        .get(PLUGIN_REQUIREMENTS_KEY)
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| dump_error(format!("invalid plugin requirements: {error}")))
        })
        .transpose()
}

fn column_external_type(
    column: &radixdb_orm::ColumnDescriptor,
) -> Result<Option<DumpColumnExternalType>> {
    column
        .extensions
        .get(EXTERNAL_TYPE_KEY)
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|error| {
                dump_error(format!(
                    "column '{}': invalid external type descriptor: {error}",
                    column.name
                ))
            })
        })
        .transpose()
}

fn validate_hex(value: &str, bytes: usize, field: &str) -> Result<()> {
    if value.len() != bytes * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(dump_error(format!(
            "{field} must contain exactly {} hexadecimal digits",
            bytes * 2
        )));
    }
    Ok(())
}

fn validate_uuid(value: &str, field: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(value).map_err(|error| dump_error(format!("invalid {field}: {error}")))
}

fn render_plugin_ddl(descriptor: &DatabaseDescriptor) -> Result<Vec<String>> {
    let Some(mut requirements) = plugin_requirements(descriptor)? else {
        return Ok(Vec::new());
    };
    requirements
        .extensions
        .sort_by_key(|requirement| requirement.sql_name.to_ascii_lowercase());
    requirements
        .types
        .sort_by_key(|requirement| requirement.sql_name.to_ascii_lowercase());

    let mut extension_names = BTreeSet::new();
    let mut sql = Vec::with_capacity(requirements.extensions.len() + requirements.types.len());
    for extension in requirements.extensions {
        validate_uuid(&extension.package_id, "plugin package id")?;
        validate_hex(
            &extension.descriptor_fingerprint,
            32,
            "plugin descriptor fingerprint",
        )?;
        if extension.abi_major == 0 || extension.abi_min_minor > extension.abi_max_minor {
            return Err(dump_error("invalid plugin ABI requirement"));
        }
        if !extension_names.insert(extension.sql_name.to_ascii_lowercase()) {
            return Err(dump_error("duplicate extension requirement"));
        }
        sql.push(format!(
            "CREATE EXTENSION {} VERSION {};",
            quote_identifier(&extension.sql_name),
            quote_text(&extension.version)
        ));
    }
    for external_type in requirements.types {
        validate_uuid(&external_type.type_object_id, "external type object id")?;
        validate_hex(
            &external_type.codec_fingerprint,
            32,
            "external codec fingerprint",
        )?;
        if external_type.codec_version == 0
            || external_type.semantic_revision == 0
            || external_type.max_payload_bytes == 0
            || !matches!(external_type.storage_kind, 1 | 2)
            || (external_type.storage_kind == 1 && external_type.fixed_bytes.is_none())
            || (external_type.storage_kind == 2 && external_type.fixed_bytes.is_some())
        {
            return Err(dump_error("invalid external type requirement"));
        }
        let _capabilities = external_type.capabilities;
        if !extension_names.contains(&external_type.extension.to_ascii_lowercase()) {
            return Err(dump_error(format!(
                "external type '{}' references missing extension '{}'",
                external_type.sql_name, external_type.extension
            )));
        }
        sql.push(format!(
            "CREATE TYPE {} FROM EXTENSION {} AS {};",
            quote_qualified_identifier(&external_type.sql_name)?,
            quote_identifier(&external_type.extension),
            quote_text(&external_type.local_id)
        ));
    }
    Ok(sql)
}

fn render_data_type(data_type: &DataTypeDescriptor) -> Result<String> {
    Ok(match data_type {
        DataTypeDescriptor::Null => {
            return Err(dump_error("NULL is not a valid durable column type"));
        }
        DataTypeDescriptor::Integer => "INTEGER".to_string(),
        DataTypeDescriptor::Float => "FLOAT".to_string(),
        DataTypeDescriptor::Text => "TEXT".to_string(),
        DataTypeDescriptor::Boolean => "BOOLEAN".to_string(),
        DataTypeDescriptor::Timestamp => "TIMESTAMP".to_string(),
        DataTypeDescriptor::Date => "DATE".to_string(),
        DataTypeDescriptor::Json => "JSON".to_string(),
        DataTypeDescriptor::Uuid => "UUID".to_string(),
        DataTypeDescriptor::Bytes => "BYTES".to_string(),
        DataTypeDescriptor::Decimal { precision, scale } => match (precision, scale) {
            (Some(precision), Some(scale)) => format!("DECIMAL({precision},{scale})"),
            (None, None) => "DECIMAL".to_string(),
            _ => return Err(dump_error("incomplete DECIMAL descriptor")),
        },
        DataTypeDescriptor::Vector { dimensions } => format!("VECTOR({dimensions})"),
    })
}

fn fk_action(action: ForeignKeyActionDescriptor) -> &'static str {
    match action {
        ForeignKeyActionDescriptor::Restrict => "RESTRICT",
        ForeignKeyActionDescriptor::Cascade => "CASCADE",
        ForeignKeyActionDescriptor::SetNull => "SET NULL",
        ForeignKeyActionDescriptor::NoAction => "NO ACTION",
    }
}

fn render_create_table(table: &TableDescriptor) -> Result<String> {
    let mut definitions = Vec::with_capacity(table.columns.len() + table.constraints.len());
    let mut columns = table.columns.clone();
    columns.sort_by_key(|column| column.ordinal);
    for column in columns {
        let rendered_type = match column_external_type(&column)? {
            Some(external) => quote_qualified_identifier(&external.sql_name)?,
            None => render_data_type(&column.data_type)?,
        };
        let mut definition = format!("{} {}", quote_identifier(&column.name), rendered_type);
        if !column.nullable {
            definition.push_str(" NOT NULL");
        }
        if column.auto_increment {
            definition.push_str(" AUTO_INCREMENT");
        }
        if let Some(default) = column.default_expression {
            write!(&mut definition, " DEFAULT {default}").unwrap();
        }
        definitions.push(definition);
    }

    let mut constraints = table.constraints.clone();
    constraints.sort_by_key(|constraint| constraint.id);
    for constraint in constraints {
        match constraint.definition {
            ConstraintDefinition::PrimaryKey { columns } => definitions.push(format!(
                "PRIMARY KEY ({})",
                columns
                    .iter()
                    .map(|column| quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            ConstraintDefinition::Unique { columns, .. } => definitions.push(format!(
                "UNIQUE ({})",
                columns
                    .iter()
                    .map(|column| quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            ConstraintDefinition::Check {
                column, expression, ..
            } => {
                if let Some(column) = column {
                    let position = table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(&column))
                        .ok_or_else(|| {
                            dump_error(format!(
                                "CHECK '{}' references missing column '{}'",
                                constraint.name, column
                            ))
                        })?;
                    definitions[position].push_str(&format!(" CHECK ({expression})"));
                } else {
                    definitions.push(format!("CHECK ({expression})"));
                }
            }
            ConstraintDefinition::ForeignKey { .. } => {}
        }
    }

    if definitions.is_empty() {
        return Err(dump_error(format!(
            "table '{}' has no column definitions",
            table.name
        )));
    }
    Ok(format!(
        "CREATE TABLE {} ({});",
        quote_identifier(&table.name),
        definitions.join(", ")
    ))
}

fn render_foreign_keys(table: &TableDescriptor) -> Result<Vec<String>> {
    let mut constraints = table.constraints.clone();
    constraints.sort_by_key(|constraint| constraint.id);
    let mut sql = Vec::new();
    for constraint in constraints {
        let ConstraintDefinition::ForeignKey {
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } = constraint.definition
        else {
            continue;
        };
        if columns.len() != 1 || referenced_columns.len() != 1 {
            return Err(dump_error(format!(
                "constraint '{}' uses an unsupported multi-column foreign key",
                constraint.name
            )));
        }
        sql.push(format!(
            "ALTER TABLE {} ADD CONSTRAINT FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {};",
            quote_identifier(&table.name),
            quote_identifier(&columns[0]),
            quote_identifier(&referenced_table),
            quote_identifier(&referenced_columns[0]),
            fk_action(on_delete),
            fk_action(on_update)
        ));
    }
    Ok(sql)
}

fn render_option_value(value: &serde_json::Value) -> Result<String> {
    Ok(match value {
        serde_json::Value::String(value) => quote_text(value),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        _ => return Err(dump_error("index option is not a scalar SQL value")),
    })
}

fn render_indexes(table: &TableDescriptor) -> Result<Vec<String>> {
    let owned: BTreeSet<String> = table
        .constraints
        .iter()
        .filter_map(|constraint| match &constraint.definition {
            ConstraintDefinition::Unique { owned_index, .. } => {
                Some(owned_index.to_ascii_lowercase())
            }
            _ => None,
        })
        .collect();
    let primary_keys = table
        .constraints
        .iter()
        .filter_map(|constraint| match &constraint.definition {
            ConstraintDefinition::PrimaryKey { columns } => Some(columns),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut indexes = table.indexes.clone();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    let mut sql = Vec::new();
    for index in indexes {
        if owned.contains(&index.name.to_ascii_lowercase())
            || index.method.eq_ignore_ascii_case("primary_key")
            || index.name.starts_with("__pk_")
            || (index.unique
                && primary_keys.iter().any(|columns| {
                    columns.len() == index.columns.len()
                        && columns
                            .iter()
                            .zip(&index.columns)
                            .all(|(left, right)| left.eq_ignore_ascii_case(right))
                }))
        {
            continue;
        }
        sql.push(render_index(&table.name, &index)?);
    }
    Ok(sql)
}

fn render_index(table: &str, index: &IndexDescriptor) -> Result<String> {
    if index.columns.is_empty() {
        return Err(dump_error(format!("index '{}' has no columns", index.name)));
    }
    let mut sql = String::from("CREATE ");
    if index.unique {
        sql.push_str("UNIQUE ");
    }
    write!(
        &mut sql,
        "INDEX {} ON {} ({})",
        quote_identifier(&index.name),
        quote_identifier(table),
        index
            .columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    )
    .unwrap();
    let method = index.method.to_ascii_uppercase();
    // MultiColumn is the engine's physical descriptor for an ordinary SQL
    // composite index, not a parser-visible USING method. Column cardinality
    // selects it again during import.
    if !matches!(method.as_str(), "" | "AUTO" | "MULTICOLUMN") {
        write!(&mut sql, " USING {method}").unwrap();
    }
    if !index.options.is_empty() {
        let mut options = Vec::with_capacity(index.options.len());
        for (key, value) in &index.options {
            let key = if key == "distance_metric" {
                "metric"
            } else {
                key
            };
            options.push(format!("{key} = {}", render_option_value(value)?));
        }
        write!(&mut sql, " WITH ({})", options.join(", ")).unwrap();
    }
    if let Some(predicate) = &index.predicate {
        write!(&mut sql, " WHERE {predicate}").unwrap();
    }
    sql.push(';');
    Ok(sql)
}

fn ordered_views(views: &[ViewDescriptor]) -> Result<Vec<&ViewDescriptor>> {
    let by_name: BTreeMap<String, &ViewDescriptor> = views
        .iter()
        .map(|view| (view.name.to_ascii_lowercase(), view))
        .collect();
    if by_name.len() != views.len() {
        return Err(dump_error("duplicate case-insensitive view name"));
    }
    let mut remaining: BTreeMap<String, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, view) in &by_name {
        let dependencies: BTreeSet<String> = view
            .dependencies
            .iter()
            .map(|dependency| dependency.to_ascii_lowercase())
            .filter(|dependency| by_name.contains_key(dependency))
            .collect();
        remaining.insert(name.clone(), dependencies.len());
        for dependency in dependencies {
            dependents.entry(dependency).or_default().push(name.clone());
        }
    }
    let mut ready: BTreeSet<String> = remaining
        .iter()
        .filter_map(|(name, count)| (*count == 0).then_some(name.clone()))
        .collect();
    let mut ordered = Vec::with_capacity(views.len());
    while let Some(name) = ready.pop_first() {
        ordered.push(by_name[&name]);
        if let Some(children) = dependents.get(&name) {
            for child in children {
                let count = remaining
                    .get_mut(child)
                    .expect("view dependency target is registered");
                *count -= 1;
                if *count == 0 {
                    ready.insert(child.clone());
                }
            }
        }
    }
    if ordered.len() != views.len() {
        return Err(dump_error("view dependency graph contains a cycle"));
    }
    Ok(ordered)
}

/// Hash only the canonical SQL schema contract. Catalog UUIDs, generation
/// counters and timestamps are intentionally excluded: recreating the same
/// logical schema in a new physical generation must produce the same value.
fn logical_schema_fingerprint(descriptor: &DatabaseDescriptor) -> Result<String> {
    let mut digest = Sha256::new();
    if let Some(requirements) = descriptor.extensions.get(PLUGIN_REQUIREMENTS_KEY) {
        digest.update(
            serde_json::to_vec(requirements).map_err(|error| {
                dump_error(format!("cannot encode plugin requirements: {error}"))
            })?,
        );
        digest.update(b"\n");
    }
    for statement in render_plugin_ddl(descriptor)? {
        digest.update(statement.as_bytes());
        digest.update(b"\n");
    }
    for table in &descriptor.tables {
        digest.update(render_create_table(table)?.as_bytes());
        digest.update(b"\n");
    }
    for table in &descriptor.tables {
        for foreign_key in render_foreign_keys(table)? {
            digest.update(foreign_key.as_bytes());
            digest.update(b"\n");
        }
    }
    for table in &descriptor.tables {
        for index in render_indexes(table)? {
            digest.update(index.as_bytes());
            digest.update(b"\n");
        }
    }
    for view in ordered_views(&descriptor.views)? {
        digest.update(
            format!(
                "CREATE VIEW {} AS {};\n",
                quote_identifier(&view.name),
                view.query.trim().trim_end_matches(';')
            )
            .as_bytes(),
        );
    }
    Ok(hex(digest.finalize().as_slice()))
}

struct DigestWriter<W> {
    inner: W,
    digest: Sha256,
}

impl<W: Write> DigestWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
        }
    }

    fn finish(self) -> (W, String) {
        let digest = self.digest.finalize();
        (self.inner, hex(digest.as_slice()))
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_hashed(writer: &mut impl Write, text: &str) -> Result<()> {
    writer
        .write_all(text.as_bytes())
        .map_err(|error| dump_error(format!("write failed: {error}")))
}

fn render_value(value: &Value, column: &radixdb_orm::ColumnDescriptor) -> Result<String> {
    if let Some(external) = value.as_external() {
        let descriptor = column_external_type(column)?.ok_or_else(|| {
            dump_error(format!(
                "column '{}' contains an external value without external type metadata",
                column.name
            ))
        })?;
        let expected = validate_uuid(&descriptor.type_object_id, "external column type object id")?;
        if external.type_ref().type_object_id() != *expected.as_bytes()
            || external.type_ref().codec_version() != descriptor.codec_version
        {
            return Err(dump_error(format!(
                "column '{}' external value identity differs from its descriptor",
                column.name
            )));
        }
        return Ok(format!(
            "CAST(FROM_HEX({}) AS {})",
            quote_text(&hex(external.payload())),
            quote_qualified_identifier(&descriptor.sql_name)?
        ));
    }
    Ok(match value {
        Value::Null(_) => "NULL".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) if value.is_nan() => "CAST('NaN' AS FLOAT)".to_string(),
        Value::Float(value) if *value == f64::INFINITY => "CAST('inf' AS FLOAT)".to_string(),
        Value::Float(value) if *value == f64::NEG_INFINITY => "CAST('-inf' AS FLOAT)".to_string(),
        Value::Float(value) if value.to_bits() == (-0.0_f64).to_bits() => {
            "CAST('-0.0' AS FLOAT)".to_string()
        }
        Value::Float(value) => value.to_string(),
        Value::Text(value) => quote_text(value),
        Value::Boolean(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        Value::Timestamp(value) => {
            format!("CAST({} AS TIMESTAMP)", quote_text(&value.to_rfc3339()))
        }
        Value::Extension(_) => match value.data_type() {
            DataType::Json => format!(
                "CAST({} AS JSON)",
                quote_text(
                    value
                        .as_json()
                        .ok_or_else(|| dump_error("invalid JSON value"))?
                )
            ),
            DataType::Vector => format!(
                "CAST({} AS VECTOR({}))",
                quote_text(&value.to_string()),
                value
                    .as_vector_f32()
                    .ok_or_else(|| dump_error("invalid VECTOR value"))?
                    .len()
            ),
            DataType::Uuid => format!("CAST({} AS UUID)", quote_text(&value.to_string())),
            DataType::Decimal => format!("CAST({} AS DECIMAL)", quote_text(&value.to_string())),
            DataType::Date => format!("CAST({} AS DATE)", quote_text(&value.to_string())),
            DataType::Bytes => format!(
                "FROM_HEX({})",
                quote_text(&hex(value
                    .as_bytes_value()
                    .ok_or_else(|| dump_error("invalid BYTES value"))?))
            ),
            other => {
                return Err(dump_error(format!(
                    "unsupported extension value type {other}"
                )));
            }
        },
    })
}

fn append_row(
    row_sql: &mut String,
    row: &Row,
    columns: &[radixdb_orm::ColumnDescriptor],
) -> Result<()> {
    if row.len() != columns.len() {
        return Err(dump_error(format!(
            "row width {} differs from descriptor width {}",
            row.len(),
            columns.len()
        )));
    }
    row_sql.push('(');
    for (index, (value, column)) in row.iter().zip(columns).enumerate() {
        if index > 0 {
            row_sql.push_str(", ");
        }
        row_sql.push_str(&render_value(value, column)?);
        if row_sql.len() > MAX_DUMP_STATEMENT_BYTES {
            return Err(dump_error(format!(
                "one exported row exceeds the {MAX_DUMP_STATEMENT_BYTES}-byte statement budget"
            )));
        }
    }
    row_sql.push(')');
    Ok(())
}

fn write_table_rows(
    transaction: &mut crate::api::Transaction,
    writer: &mut impl Write,
    table: &TableDescriptor,
    summary: &mut SqlDumpSummary,
) -> Result<()> {
    let mut columns = table.columns.clone();
    columns.sort_by_key(|column| column.ordinal);
    let prefix = format!(
        "INSERT INTO {} ({}) VALUES ",
        quote_identifier(&table.name),
        columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut statement = String::with_capacity(TARGET_INSERT_BYTES.min(64 * 1024));
    let mut batch_rows = 0_usize;
    let mut flush = |statement: &mut String, batch_rows: &mut usize| -> Result<()> {
        if *batch_rows == 0 {
            return Ok(());
        }
        statement.push_str(";\n");
        write_hashed(writer, statement)?;
        summary.statements += 1;
        statement.clear();
        *batch_rows = 0;
        Ok(())
    };

    transaction.visit_logical_export_rows(&table.name, &mut |_row_id, row| {
        let mut row_sql = String::new();
        append_row(&mut row_sql, &row, &columns)?;
        let separator = usize::from(batch_rows > 0) * 2;
        if batch_rows > 0
            && (batch_rows >= TARGET_INSERT_ROWS
                || statement.len() + separator + row_sql.len() > TARGET_INSERT_BYTES)
        {
            flush(&mut statement, &mut batch_rows)?;
        }
        if batch_rows == 0 {
            statement.push_str(&prefix);
        } else {
            statement.push_str(", ");
        }
        statement.push_str(&row_sql);
        if statement.len() > MAX_DUMP_STATEMENT_BYTES {
            return Err(dump_error(format!(
                "INSERT for table '{}' exceeds the {MAX_DUMP_STATEMENT_BYTES}-byte statement budget",
                table.name
            )));
        }
        batch_rows += 1;
        summary.rows += 1;
        Ok(())
    })?;
    flush(&mut statement, &mut batch_rows)
}

/// Export one deterministic logical snapshot to a versioned SQL stream.
pub fn export_sql_dump<W: Write>(database: &Database, writer: W) -> Result<SqlDumpSummary> {
    let mut transaction = database.begin_logical_export()?;
    let descriptor_json: String = transaction.query_one("DESCRIBE DATABASE FORMAT JSON", ())?;
    let mut descriptor = DatabaseDescriptor::from_json(&descriptor_json)
        .map_err(|error| dump_error(format!("invalid catalog descriptor: {error}")))?;
    descriptor
        .tables
        .sort_by_key(|table| table.name.to_ascii_lowercase());
    descriptor
        .views
        .sort_by_key(|view| view.name.to_ascii_lowercase());
    let schema_fingerprint = logical_schema_fingerprint(&descriptor)?;

    let mut writer = DigestWriter::new(writer);
    write_hashed(
        &mut writer,
        &format!("-- radixdb-sql-dump: {SQL_DUMP_FORMAT}\n"),
    )?;
    write_hashed(
        &mut writer,
        &format!("-- source-engine-version: {}\n", env!("CARGO_PKG_VERSION")),
    )?;
    write_hashed(
        &mut writer,
        &format!("-- schema-fingerprint: {schema_fingerprint}\n"),
    )?;

    let mut summary = SqlDumpSummary {
        tables: descriptor.tables.len() as u64,
        ..SqlDumpSummary::default()
    };
    for statement in render_plugin_ddl(&descriptor)? {
        write_hashed(&mut writer, &statement)?;
        write_hashed(&mut writer, "\n")?;
        summary.statements += 1;
    }
    for table in &descriptor.tables {
        write_hashed(&mut writer, &render_create_table(table)?)?;
        write_hashed(&mut writer, "\n")?;
        summary.statements += 1;
    }
    for table in &descriptor.tables {
        write_table_rows(&mut transaction, &mut writer, table, &mut summary)?;
    }
    for table in &descriptor.tables {
        for foreign_key in render_foreign_keys(table)? {
            write_hashed(&mut writer, &foreign_key)?;
            write_hashed(&mut writer, "\n")?;
            summary.statements += 1;
        }
    }
    for table in &descriptor.tables {
        for index in render_indexes(table)? {
            write_hashed(&mut writer, &index)?;
            write_hashed(&mut writer, "\n")?;
            summary.statements += 1;
        }
    }
    for view in ordered_views(&descriptor.views)? {
        write_hashed(
            &mut writer,
            &format!(
                "CREATE VIEW {} AS {};\n",
                quote_identifier(&view.name),
                view.query.trim().trim_end_matches(';')
            ),
        )?;
        summary.statements += 1;
    }

    transaction.rollback()?;
    drop(transaction);
    write_hashed(
        &mut writer,
        &format!(
            "{COUNTS_PREFIX}tables={} rows={} statements={}\n",
            summary.tables, summary.rows, summary.statements
        ),
    )?;
    let (mut writer, digest) = writer.finish();
    writer
        .write_all(format!("{FOOTER_PREFIX}{digest}\n").as_bytes())
        .and_then(|_| writer.flush())
        .map_err(|error| dump_error(format!("final write failed: {error}")))?;
    summary.sha256 = digest;
    Ok(summary)
}

fn read_line(reader: &mut impl BufRead, buffer: &mut Vec<u8>) -> Result<usize> {
    buffer.clear();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| dump_error(format!("read failed: {error}")))?;
        if available.is_empty() {
            return Ok(buffer.len());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| position + 1)
            .unwrap_or(available.len());
        if buffer
            .len()
            .checked_add(take)
            .is_none_or(|length| length > MAX_DUMP_STATEMENT_BYTES)
        {
            return Err(dump_error(format!(
                "physical line exceeds the {MAX_DUMP_STATEMENT_BYTES}-byte import budget"
            )));
        }
        buffer.extend_from_slice(&available[..take]);
        reader.consume(take);
        if buffer.last() == Some(&b'\n') {
            return Ok(buffer.len());
        }
    }
}

fn read_header_line(
    reader: &mut impl BufRead,
    raw: &mut Vec<u8>,
    digest: &mut Sha256,
) -> Result<String> {
    if read_line(reader, raw)? == 0 {
        return Err(dump_error("truncated dump header"));
    }
    digest.update(raw.as_slice());
    std::str::from_utf8(raw)
        .map(str::to_string)
        .map_err(|error| dump_error(format!("dump is not UTF-8: {error}")))
}

fn statement_complete(sql: &str) -> Result<bool> {
    let mut lexer = Lexer::new(sql);
    let mut terminal = false;
    loop {
        let token = lexer.next_token();
        if token.is_error() {
            // A physical line may end inside a quoted SQL value. The same
            // lexer reports that prefix as an unterminated token until the
            // next line is appended; final validation still uses parse_sql.
            return Ok(false);
        }
        if token.is_eof() {
            return Ok(terminal);
        }
        if token.token_type != TokenType::Comment {
            terminal = token.is_punctuator(";");
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ImportedStatementStats {
    tables: u64,
    rows: u64,
}

fn validate_import_statement(sql: &str) -> Result<ImportedStatementStats> {
    let statements = parse_sql(sql).map_err(|error| dump_error(error.to_string()))?;
    let [statement] = statements.as_slice() else {
        return Err(dump_error(
            "each dump unit must contain exactly one SQL statement",
        ));
    };
    let stats = match statement {
        Statement::CreateTable(_) => ImportedStatementStats { tables: 1, rows: 0 },
        Statement::Insert(insert)
            if insert.select.is_none()
                && !insert.values.is_empty()
                && !insert.on_duplicate
                && !insert.do_nothing
                && insert.returning.is_empty() =>
        {
            ImportedStatementStats {
                tables: 0,
                rows: insert.values.len() as u64,
            }
        }
        Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::CreateExtension(_)
        | Statement::CreateExternalType(_) => ImportedStatementStats::default(),
        Statement::AlterTable(statement) => matches!(
            statement.operation,
            crate::parser::ast::AlterTableOperation::AddConstraint
        )
        .then(ImportedStatementStats::default)
        .ok_or_else(|| {
            dump_error(format!(
                "statement is outside the logical import contract: {statement}"
            ))
        })?,
        _ => {
            return Err(dump_error(format!(
                "statement is outside the logical import contract: {statement}"
            )));
        }
    };
    Ok(stats)
}

fn parse_expected_counts(line: &str) -> Result<(u64, u64, u64)> {
    let payload = line
        .strip_prefix(COUNTS_PREFIX)
        .and_then(|value| value.strip_suffix('\n'))
        .ok_or_else(|| dump_error("malformed logical counts trailer"))?;
    let mut values = BTreeMap::new();
    for field in payload.split_ascii_whitespace() {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| dump_error("malformed logical counts trailer"))?;
        if values
            .insert(
                name,
                value
                    .parse::<u64>()
                    .map_err(|_| dump_error(format!("invalid logical count '{name}={value}'")))?,
            )
            .is_some()
        {
            return Err(dump_error(format!(
                "duplicate logical count field '{name}'"
            )));
        }
    }
    if values.len() != 3 {
        return Err(dump_error("incomplete logical counts trailer"));
    }
    Ok((
        *values
            .get("tables")
            .ok_or_else(|| dump_error("missing tables count"))?,
        *values
            .get("rows")
            .ok_or_else(|| dump_error("missing rows count"))?,
        *values
            .get("statements")
            .ok_or_else(|| dump_error("missing statements count"))?,
    ))
}

fn database_is_empty(database: &Database) -> Result<bool> {
    let mut table_count = 0_usize;
    for row in database.query("SHOW TABLES", ())? {
        row?;
        table_count += 1;
    }
    let mut view_count = 0_usize;
    for row in database.query("SHOW VIEWS", ())? {
        row?;
        view_count += 1;
    }
    Ok(table_count == 0 && view_count == 0)
}

/// Import a verified SQL stream into an already isolated empty database.
///
/// This lower-level stream function intentionally mutates only the isolated
/// `Database` passed to it. [`import_sql_dump_to_new_database`] owns durable
/// staging and atomic target publication for application callers.
pub fn import_sql_dump<R: BufRead>(database: &Database, mut reader: R) -> Result<SqlDumpSummary> {
    if !database_is_empty(database)? {
        return Err(dump_error("import target database is not empty"));
    }

    let mut digest = Sha256::new();
    let mut raw = Vec::new();
    let first = read_header_line(&mut reader, &mut raw, &mut digest)?;
    if first != format!("-- radixdb-sql-dump: {SQL_DUMP_FORMAT}\n") {
        return Err(dump_error("unsupported or malformed dump format header"));
    }
    let source = read_header_line(&mut reader, &mut raw, &mut digest)?;
    if !source.starts_with("-- source-engine-version: ") || !source.ends_with('\n') {
        return Err(dump_error("malformed source engine header"));
    }
    let fingerprint = read_header_line(&mut reader, &mut raw, &mut digest)?;
    let expected_schema_fingerprint = fingerprint
        .strip_prefix("-- schema-fingerprint: ")
        .and_then(|value| value.strip_suffix('\n'))
        .ok_or_else(|| dump_error("malformed schema fingerprint header"))?
        .to_ascii_lowercase();
    validate_hex(&expected_schema_fingerprint, 32, "schema fingerprint")?;

    let mut statement = String::new();
    let mut summary = SqlDumpSummary::default();
    let mut expected_counts = None;
    let expected_digest = loop {
        let length = read_line(&mut reader, &mut raw)?;
        if length == 0 {
            return Err(dump_error("dump checksum footer is missing"));
        }
        let line = std::str::from_utf8(&raw)
            .map_err(|error| dump_error(format!("dump is not UTF-8: {error}")))?;
        if statement.trim().is_empty() && line.starts_with(COUNTS_PREFIX) {
            if expected_counts.is_some() {
                return Err(dump_error("duplicate logical counts trailer"));
            }
            expected_counts = Some(parse_expected_counts(line)?);
            digest.update(raw.as_slice());
            continue;
        }
        if statement.trim().is_empty() && line.starts_with(FOOTER_PREFIX) {
            let encoded = line
                .strip_prefix(FOOTER_PREFIX)
                .unwrap()
                .strip_suffix('\n')
                .ok_or_else(|| dump_error("malformed checksum footer"))?;
            if encoded.len() != 64 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(dump_error("malformed SHA-256 checksum"));
            }
            break encoded.to_ascii_lowercase();
        }

        digest.update(raw.as_slice());
        statement.push_str(line);
        if statement.len() > MAX_DUMP_STATEMENT_BYTES {
            return Err(dump_error(format!(
                "statement exceeds the {MAX_DUMP_STATEMENT_BYTES}-byte import budget"
            )));
        }
        if statement_complete(&statement)? {
            if expected_counts.is_some() {
                return Err(dump_error(
                    "SQL statement appears after logical counts trailer",
                ));
            }
            let stats = validate_import_statement(&statement)?;
            database.execute(&statement, ())?;
            summary.statements += 1;
            summary.tables += stats.tables;
            summary.rows += stats.rows;
            statement.clear();
        }
    };
    if !statement.trim().is_empty() {
        return Err(dump_error("unterminated SQL before checksum footer"));
    }
    if read_line(&mut reader, &mut raw)? != 0 {
        return Err(dump_error("trailing bytes after checksum footer"));
    }
    let actual_digest = hex(digest.finalize().as_slice());
    if actual_digest != expected_digest {
        return Err(dump_error(format!(
            "checksum mismatch: expected {expected_digest}, computed {actual_digest}"
        )));
    }
    let expected_counts =
        expected_counts.ok_or_else(|| dump_error("logical counts trailer is missing"))?;
    let actual_counts = (summary.tables, summary.rows, summary.statements);
    if actual_counts != expected_counts {
        return Err(dump_error(format!(
            "logical counts mismatch: expected tables={} rows={} statements={}, imported tables={} rows={} statements={}",
            expected_counts.0,
            expected_counts.1,
            expected_counts.2,
            actual_counts.0,
            actual_counts.1,
            actual_counts.2
        )));
    }

    let descriptor_json: String = database.query_one("DESCRIBE DATABASE FORMAT JSON", ())?;
    let mut descriptor = DatabaseDescriptor::from_json(&descriptor_json)
        .map_err(|error| dump_error(format!("invalid imported catalog descriptor: {error}")))?;
    descriptor
        .tables
        .sort_by_key(|table| table.name.to_ascii_lowercase());
    descriptor
        .views
        .sort_by_key(|view| view.name.to_ascii_lowercase());
    let actual_schema_fingerprint = logical_schema_fingerprint(&descriptor)?;
    if actual_schema_fingerprint != expected_schema_fingerprint {
        return Err(dump_error(format!(
            "schema fingerprint mismatch: expected {expected_schema_fingerprint}, imported {actual_schema_fingerprint}"
        )));
    }

    if database.dsn().starts_with("file://") {
        let checkpoint = database.query("PRAGMA CHECKPOINT", ())?;
        for row in checkpoint {
            row?;
        }
    }
    summary.sha256 = actual_digest;
    Ok(summary)
}

fn sync_dump_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| dump_error(format!("cannot sync '{}': {error}", path.display())))
}

fn unique_dump_sibling(target: &Path, role: &str) -> Result<PathBuf> {
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| dump_error(format!("{role} target must have a UTF-8 file name")))?;
    for _ in 0..32 {
        let candidate = parent.join(format!(".{name}.{role}.{}", uuid::Uuid::now_v7().simple()));
        if std::fs::symlink_metadata(&candidate).is_err() {
            return Ok(candidate);
        }
    }
    Err(dump_error(format!(
        "cannot allocate unique {role} staging path"
    )))
}

fn atomic_publish_directory_no_replace(staging: &Path, target: &Path) -> Result<()> {
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| dump_error("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| dump_error("target path contains NUL"))?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(dump_error(io::Error::last_os_error()))
    }
}

fn import_staging_dsn(source_dsn: &str, staging: &Path) -> Result<String> {
    let source = source_dsn
        .strip_prefix("file://")
        .ok_or_else(|| dump_error("logical import requires a file:// database"))?;
    let mut options: Vec<&str> = source
        .split_once('?')
        .map(|(_, query)| query.split('&').collect())
        .unwrap_or_default();
    options.retain(|option| {
        !matches!(
            option
                .split_once('=')
                .map(|(key, _)| key)
                .unwrap_or(*option),
            "sync_mode" | "checkpoint_on_close"
        )
    });
    // Publication is a migration durability boundary, not a benchmark knob.
    options.push("sync_mode=full");
    options.push("checkpoint_on_close=on");
    Ok(format!(
        "file://{}?{}",
        staging.display(),
        options.join("&")
    ))
}

fn finish_database<T, E: std::fmt::Display>(
    database: &Database,
    outcome: std::result::Result<T, E>,
) -> Result<T> {
    let close = database.close();
    match (outcome, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(close_error)) => Err(dump_error(format!(
            "operation completed but terminal database close failed; durability outcome is unknown: {close_error}"
        ))),
        (Err(operation_error), Ok(())) => Err(dump_error(operation_error)),
        (Err(operation_error), Err(close_error)) => Err(dump_error(format!(
            "operation failed: {operation_error}; terminal database close also failed: {close_error}"
        ))),
    }
}

/// Export one logical snapshot to an atomically published file.
///
/// The caller selects the transport. File durability and publication belong to
/// this dump owner, so no CLI or application needs to reproduce the protocol.
pub fn export_sql_dump_to_file(database: &Database, destination: &Path) -> Result<SqlDumpSummary> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = unique_dump_sibling(destination, "export")?;
    let result = (|| -> Result<SqlDumpSummary> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                dump_error(format!("cannot create '{}': {error}", temporary.display()))
            })?;
        let mut writer = BufWriter::new(&file);
        let summary = export_sql_dump(database, &mut writer)?;
        writer
            .flush()
            .map_err(|error| dump_error(format!("dump flush failed: {error}")))?;
        drop(writer);
        file.sync_all()
            .map_err(|error| dump_error(format!("dump sync failed: {error}")))?;
        std::fs::rename(&temporary, destination).map_err(|error| {
            dump_error(format!(
                "cannot publish SQL dump '{}' -> '{}': {error}",
                temporary.display(),
                destination.display()
            ))
        })?;
        sync_dump_directory(parent)?;
        Ok(summary)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Import a verified stream into a new durable database and publish it once.
///
/// The requested target must not exist. The database is built in a sibling
/// full-sync staging root, closed, synced and then published with
/// `RENAME_NOREPLACE`; failed imports never expose a partial target.
pub fn import_sql_dump_to_new_database<R: BufRead>(
    target_dsn: &str,
    target: &Path,
    reader: R,
) -> Result<SqlDumpSummary> {
    import_sql_dump_to_new_database_with_plugin_registry(
        target_dsn,
        target,
        reader,
        std::sync::Arc::new(PluginRegistry::empty()),
    )
}

/// Import into a new durable database using the already admitted immutable
/// plugin registry. Extension packages are never discovered or loaded by the
/// dump reader itself.
pub fn import_sql_dump_to_new_database_with_plugin_registry<R: BufRead>(
    target_dsn: &str,
    target: &Path,
    reader: R,
    plugin_registry: std::sync::Arc<PluginRegistry>,
) -> Result<SqlDumpSummary> {
    match std::fs::symlink_metadata(target) {
        Ok(_) => {
            return Err(dump_error(format!(
                "logical import target '{}' already exists; use a new database root",
                target.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(dump_error(format!("cannot inspect import target: {error}"))),
    }
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = unique_dump_sibling(target, "import")?;
    let staging_dsn = import_staging_dsn(target_dsn, &staging)?;
    let database = match Database::open_with_plugin_registry(&staging_dsn, plugin_registry) {
        Ok(database) => database,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
    };
    let outcome = finish_database(&database, import_sql_dump(&database, reader));
    drop(database);
    let summary = match outcome {
        Ok(summary) => summary,
        Err(error) => {
            let cleanup = std::fs::remove_dir_all(&staging);
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => dump_error(format!(
                    "{error}; failed staging database retained at '{}': {cleanup}",
                    staging.display()
                )),
            });
        }
    };
    sync_dump_directory(&staging)?;
    atomic_publish_directory_no_replace(&staging, target).map_err(|error| {
        dump_error(format!(
            "cannot atomically publish import; staging retained at '{}': {error}",
            staging.display()
        ))
    })?;
    sync_dump_directory(parent)?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use radixdb_plugin_abi as abi;
    use radixdb_plugin_host::{
        derive_object_id, PluginRegistry, RegisteredExternalType, RegisteredPackage,
    };

    use super::*;

    const TEST_PACKAGE_ID: [u8; 16] = [0x73; 16];
    const TEST_PACKAGE_FINGERPRINT: [u8; 32] = [0xa7; 32];

    unsafe extern "C" fn echo_parse(
        _context: *const abi::RadixAbiCallContextV1,
        input: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let (Some(write), Some(finish)) = (output.write, output.finish) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let status = unsafe { write(output.handle, 0, 0, input) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { finish(output.handle) }
    }

    unsafe extern "C" fn echo_encode(
        context: *const abi::RadixAbiCallContextV1,
        input: *const abi::RadixAbiValueV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(input) = (unsafe { input.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        unsafe { echo_parse(context, input.borrowed_bytes, output) }
    }

    unsafe extern "C" fn byte_equal(
        _context: *const abi::RadixAbiCallContextV1,
        left: *const abi::RadixAbiValueV1,
        right: *const abi::RadixAbiValueV1,
        output: *mut u8,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(left), Some(right), Some(output)) = (
            unsafe { left.as_ref() },
            unsafe { right.as_ref() },
            unsafe { output.as_mut() },
        ) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let left = unsafe {
            std::slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
        };
        let right = unsafe {
            std::slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
        };
        *output = u8::from(left == right);
        abi::RADIX_STATUS_OK
    }

    fn external_type_registry() -> Arc<PluginRegistry> {
        let object_id = derive_object_id(TEST_PACKAGE_ID, "point").unwrap();
        let package = RegisteredPackage::for_test(
            TEST_PACKAGE_ID,
            "dump_sample",
            "1.2.3",
            TEST_PACKAGE_FINGERPRINT,
        );
        let external_type = RegisteredExternalType {
            package_id: TEST_PACKAGE_ID,
            object_id,
            local_id: "point".to_owned(),
            display_name: "Point".to_owned(),
            codec_version: 1,
            semantic_revision: 1,
            storage_kind: abi::RADIX_EXTERNAL_STORAGE_VARIABLE,
            fixed_bytes: 0,
            max_bytes: 1024,
            capabilities: abi::RADIX_TYPE_CAP_EQUALITY
                | abi::RADIX_TYPE_CAP_TEXT_INPUT
                | abi::RADIX_TYPE_CAP_TEXT_OUTPUT
                | abi::RADIX_TYPE_CAP_BINARY_INPUT
                | abi::RADIX_TYPE_CAP_BINARY_OUTPUT,
            codec_fingerprint: [0x5b; 32],
            encode: echo_encode,
            decode: echo_parse,
            equality: Some(byte_equal),
            hash: None,
            ordering: None,
            text_input: Some(echo_parse),
            text_output: Some(echo_encode),
            binary_input: Some(echo_parse),
            binary_output: Some(echo_encode),
        };
        Arc::new(PluginRegistry::from_test_objects(
            [package],
            [external_type],
            [],
            [],
            [],
            [],
        ))
    }

    #[test]
    fn external_type_dump_preserves_exact_requirements_and_canonical_payload() {
        let registry = external_type_registry();
        let source = Database::open_with_plugin_registry(
            "memory://sql_dump_external_source",
            Arc::clone(&registry),
        )
        .unwrap();
        source
            .execute("CREATE EXTENSION dump_sample VERSION '1.2.3'", ())
            .unwrap();
        source
            .execute(
                "CREATE TYPE public.point FROM EXTENSION dump_sample AS 'point'",
                (),
            )
            .unwrap();
        source
            .execute("CREATE TABLE samples (id INTEGER, value public.point)", ())
            .unwrap();
        source
            .execute(
                "INSERT INTO samples VALUES (1, CAST('alpha' AS public.point))",
                (),
            )
            .unwrap();

        let mut dump = Vec::new();
        let exported = export_sql_dump(&source, &mut dump).unwrap();
        let rendered = std::str::from_utf8(&dump).unwrap();
        assert!(rendered.contains("CREATE EXTENSION \"dump_sample\" VERSION '1.2.3';"));
        assert!(rendered.contains(
            "CREATE TYPE \"public\".\"point\" FROM EXTENSION \"dump_sample\" AS 'point';"
        ));
        assert!(rendered.contains("CAST(FROM_HEX('616c706861') AS \"public\".\"point\")"));

        let target = Database::open_with_plugin_registry(
            "memory://sql_dump_external_target",
            Arc::clone(&registry),
        )
        .unwrap();
        let imported = import_sql_dump(&target, Cursor::new(dump.clone())).unwrap();
        assert_eq!(imported, exported);
        assert_eq!(
            target
                .query_one::<String, _>("SELECT CAST(value AS TEXT) FROM samples", ())
                .unwrap(),
            "alpha"
        );
        assert_eq!(
            target
                .query_one::<i64, _>(
                    "SELECT COUNT(*) FROM samples WHERE value = CAST('alpha' AS public.point)",
                    (),
                )
                .unwrap(),
            1
        );

        let mut reexported = Vec::new();
        let reexported_summary = export_sql_dump(&target, &mut reexported).unwrap();
        assert_eq!(reexported_summary, exported);
        assert_eq!(reexported, dump);

        let missing = Database::open("memory://sql_dump_external_missing").unwrap();
        let error = import_sql_dump(&missing, Cursor::new(reexported)).unwrap_err();
        assert!(error.to_string().contains("is not active"), "{error}");
    }

    #[test]
    fn sql_dump_round_trip_is_deterministic_and_preserves_complex_catalog() {
        let source = Database::open("memory://sql_dump_source").unwrap();
        source
            .execute(
                "CREATE TABLE parent (id UUID PRIMARY KEY, label TEXT UNIQUE, payload BYTES, amount DECIMAL(12,3), happened TIMESTAMP, day DATE, metadata JSON, embedding VECTOR(2))",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id UUID, active BOOLEAN, score FLOAT, CHECK (score >= 0))",
                (),
            )
            .unwrap();
        source
            .execute(
                "ALTER TABLE child ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE INDEX child_active_idx ON child(active) WHERE active = TRUE",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE INDEX child_parent_active_idx ON child(parent_id, active)",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE INDEX parent_embedding_hnsw ON parent(embedding) USING HNSW WITH (m = 16, ef_construction = 100, metric = 'cosine')",
                (),
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO parent VALUES (CAST('018c0e27-aa31-7000-8000-112233445566' AS UUID), 'O''Reilly\nline', FROM_HEX('00ff7f'), CAST('12.340' AS DECIMAL), CAST('2026-08-27T12:34:56.123456789Z' AS TIMESTAMP), CAST('2026-08-27' AS DATE), CAST('{\"k\":1}' AS JSON), CAST('[1.25,-2.5]' AS VECTOR(2)))",
                (),
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO child VALUES (7, CAST('018c0e27-aa31-7000-8000-112233445566' AS UUID), TRUE, CAST('-0.0' AS FLOAT))",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE TABLE composite_row (id INTEGER PRIMARY KEY, tenant_id INTEGER, code TEXT, optional TEXT DEFAULT 'fallback', UNIQUE (tenant_id, code))",
                (),
            )
            .unwrap();
        source
            .execute("INSERT INTO composite_row VALUES (1, 9, 'A''B', NULL)", ())
            .unwrap();
        source
            .execute(
                "CREATE TABLE bulk_rows (id INTEGER PRIMARY KEY, payload TEXT)",
                (),
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO bulk_rows SELECT value, 'streamed' FROM generate_series(1, 4097)",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE TABLE cycle_a (id INTEGER PRIMARY KEY, b_id INTEGER)",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE TABLE cycle_b (id INTEGER PRIMARY KEY, a_id INTEGER)",
                (),
            )
            .unwrap();
        source
            .execute(
                "ALTER TABLE cycle_a ADD CONSTRAINT FOREIGN KEY (b_id) REFERENCES cycle_b(id)",
                (),
            )
            .unwrap();
        source
            .execute(
                "ALTER TABLE cycle_b ADD CONSTRAINT FOREIGN KEY (a_id) REFERENCES cycle_a(id)",
                (),
            )
            .unwrap();
        source
            .execute("INSERT INTO cycle_a VALUES (1, NULL)", ())
            .unwrap();
        source
            .execute("INSERT INTO cycle_b VALUES (1, 1)", ())
            .unwrap();
        source
            .execute("UPDATE cycle_a SET b_id = 1 WHERE id = 1", ())
            .unwrap();
        source
            .execute(
                "CREATE VIEW child_parent AS SELECT c.id, p.label FROM child c LEFT JOIN parent p ON c.parent_id = p.id",
                (),
            )
            .unwrap();
        source
            .execute(
                "CREATE VIEW active_child_parent AS SELECT * FROM child_parent WHERE id > 0",
                (),
            )
            .unwrap();

        let mut first = Vec::new();
        let first_summary = export_sql_dump(&source, &mut first).unwrap();
        let mut second = Vec::new();
        let second_summary = export_sql_dump(&source, &mut second).unwrap();
        assert_eq!(first, second);
        let rendered = std::str::from_utf8(&first).unwrap();
        assert!(rendered.contains(
            "CREATE INDEX \"child_parent_active_idx\" ON \"child\" (\"parent_id\", \"active\");"
        ));
        assert!(!rendered.contains("USING MULTICOLUMN"));
        assert_eq!(first_summary.sha256, second_summary.sha256);
        assert_eq!(first_summary.tables, 6);
        assert_eq!(first_summary.rows, 4102);

        let directory = tempfile::tempdir().unwrap();
        let dsn = format!("file://{}", directory.path().join("target").display());
        let target = Database::open(&dsn).unwrap();
        let imported = import_sql_dump(&target, Cursor::new(first)).unwrap();
        assert_eq!(imported.tables, first_summary.tables);
        assert_eq!(imported.rows, first_summary.rows);
        assert_eq!(imported.statements, first_summary.statements);
        target.close().unwrap();
        drop(target);

        let target = Database::open(&dsn).unwrap();
        assert_eq!(
            target
                .query_one::<i64, _>("SELECT COUNT(*) FROM active_child_parent", ())
                .unwrap(),
            1
        );
        let bytes: Value = target
            .query("SELECT payload FROM parent", ())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .clone();
        assert_eq!(bytes.as_bytes_value(), Some(&[0x00, 0xff, 0x7f][..]));
        let score: f64 = target.query_one("SELECT score FROM child", ()).unwrap();
        assert_eq!(score.to_bits(), (-0.0_f64).to_bits());
        let navigation_label: String = target
            .query_one("SELECT c.parent_id.label FROM child c WHERE c.id = 7", ())
            .unwrap();
        assert_eq!(navigation_label, "O'Reilly\nline");
        assert_eq!(
            target
                .query_one::<i64, _>("SELECT COUNT(*) FROM bulk_rows", ())
                .unwrap(),
            4097
        );

        let mut reexported = Vec::new();
        let reexported_summary = export_sql_dump(&target, &mut reexported).unwrap();
        assert_eq!(reexported_summary, first_summary);
        assert_eq!(reexported, second);
        target.close().unwrap();
    }

    #[test]
    fn logical_export_uses_one_mvcc_snapshot_while_commits_continue() {
        let database = Database::open("memory://sql_dump_snapshot").unwrap();
        database
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)", ())
            .unwrap();
        database
            .execute("INSERT INTO t VALUES (1, 'before')", ())
            .unwrap();

        let mut export = database.begin_logical_export().unwrap();
        let _: String = export
            .query_one("DESCRIBE DATABASE FORMAT JSON", ())
            .unwrap();
        database
            .execute("INSERT INTO t VALUES (2, 'after')", ())
            .unwrap();

        let mut seen = Vec::new();
        export
            .visit_logical_export_rows("t", &mut |row_id, row| {
                seen.push((row_id, row));
                Ok(())
            })
            .unwrap();
        export.rollback().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM t", ())
                .unwrap(),
            2
        );
    }

    #[test]
    fn sql_dump_rejects_corruption_and_nonempty_target() {
        let source = Database::open("memory://sql_dump_corrupt_source").unwrap();
        source
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        source.execute("INSERT INTO t VALUES (1)", ()).unwrap();
        let mut dump = Vec::new();
        export_sql_dump(&source, &mut dump).unwrap();
        let position = dump.iter().position(|byte| *byte == b'1').unwrap();
        dump[position] = b'2';

        let target = Database::open("memory://sql_dump_corrupt_target").unwrap();
        assert!(import_sql_dump(&target, Cursor::new(dump)).is_err());

        let nonempty = Database::open("memory://sql_dump_nonempty_target").unwrap();
        nonempty
            .execute("CREATE TABLE existing (id INTEGER)", ())
            .unwrap();
        let mut valid = Vec::new();
        export_sql_dump(&source, &mut valid).unwrap();
        assert!(import_sql_dump(&nonempty, Cursor::new(valid)).is_err());
    }

    #[test]
    fn durable_import_forces_full_sync_and_publishes_only_a_complete_database() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("published");
        let target_dsn = format!(
            "file://{}?sync_mode=none&checkpoint_on_close=off&compression=off",
            target.display()
        );
        let staging_dsn = import_staging_dsn(&target_dsn, &directory.path().join("probe")).unwrap();
        assert!(staging_dsn.contains("compression=off"));
        assert!(staging_dsn.contains("sync_mode=full"));
        assert!(staging_dsn.contains("checkpoint_on_close=on"));
        assert!(!staging_dsn.contains("sync_mode=none"));
        assert!(!staging_dsn.contains("checkpoint_on_close=off"));

        let source = Database::open_in_memory().unwrap();
        source
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
                (),
            )
            .unwrap();
        source
            .execute("INSERT INTO items VALUES (1, 'one'), (2, 'two')", ())
            .unwrap();
        let mut dump = Vec::new();
        let exported = export_sql_dump(&source, &mut dump).unwrap();
        let imported =
            import_sql_dump_to_new_database(&target_dsn, &target, Cursor::new(dump)).unwrap();
        assert_eq!(imported, exported);
        assert!(target.is_dir());

        let reopened = Database::open(&target_dsn).unwrap();
        assert_eq!(
            reopened
                .query_one::<i64, _>("SELECT COUNT(*) FROM items", ())
                .unwrap(),
            2
        );
        reopened.close().unwrap();
        assert!(!std::fs::read_dir(directory.path())
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().contains(".import.")));
    }
}
