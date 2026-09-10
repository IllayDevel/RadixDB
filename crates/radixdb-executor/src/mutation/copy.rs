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

//! COPY FROM Statement Execution
//!
//! Bulk imports data from CSV or JSON files, bypassing per-row SQL parsing
//! for significantly faster loading compared to individual INSERT statements.

use radixdb_core::{time_compat::Instant, CompactArc, SmartString};
use radixdb_core::{DataType, Error, Result, Row, Schema, Value};
use radixdb_sql::ast::{CopyFormat, CopyStatement};
use radixdb_storage::traits::{Engine, QueryResult, Table};
use rustc_hash::{FxHashMap, FxHashSet};

use super::dml_support::evaluate_default_expr;
use crate::context::{
    invalidate_in_subquery_cache_for_table, invalidate_scalar_subquery_cache_for_table,
    invalidate_semi_join_cache_for_table, ExecutionContext,
};
use crate::mutation::host::MutationHost;
use crate::mutation::validation::{
    compile_table_check_constraints, prepare_insert_row_constraints,
};
use crate::result::ExecResult;

// One uncommitted COPY row is represented simultaneously by row values,
// transaction-local MVCC/version maps, constraint/index state and commit/WAL
// staging. The budget is deliberately conservative; it is a safety envelope,
// not a malloc profiler.
const COPY_TRANSACTION_MEMORY_AMPLIFICATION: usize = 8;
// Keep COPY parsing streaming while amortizing the cold-segment snapshot and
// seal-fence work owned by SegmentedTable::insert_batch. The rows are moved
// into transaction-local MVCC storage at every flush, so this is only a small
// bounded staging window, not a second COPY-sized owner.
const COPY_INSERT_BATCH_ROWS: usize = 4096;

#[inline]
fn flush_copy_insert_batch(table: &mut Box<dyn Table>, rows: &mut Vec<Row>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let next = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);
    table.insert_batch(std::mem::replace(rows, next))
}

fn account_copy_transaction_row(
    used_bytes: &mut usize,
    limit_bytes: usize,
    row: &Row,
    row_number: i64,
) -> Result<()> {
    let row_bytes = radixdb_storage::mvcc::version_store::estimate_row_hot_bytes(row)
        .saturating_mul(COPY_TRANSACTION_MEMORY_AMPLIFICATION);
    let attempted_bytes = used_bytes.saturating_add(row_bytes);
    if attempted_bytes > limit_bytes {
        return Err(Error::CopyTransactionMemoryLimit {
            row: row_number,
            limit_bytes,
            attempted_bytes,
        });
    }
    *used_bytes = attempted_bytes;
    Ok(())
}

/// Parse a CSV field directly into a Value for the target type.
/// Avoids the intermediate Value::text() + coerce_to_type() allocation path.
#[inline]
fn parse_field(field: &str, target_type: DataType, col_name: &str) -> Result<Value> {
    match target_type {
        DataType::Integer => field.parse::<i64>().map(Value::Integer).map_err(|_| {
            Error::Type(format!(
                "cannot convert value '{}' to INTEGER for column '{}'",
                field, col_name
            ))
        }),
        DataType::Float => field.parse::<f64>().map(Value::Float).map_err(|_| {
            Error::Type(format!(
                "cannot convert value '{}' to FLOAT for column '{}'",
                field, col_name
            ))
        }),
        DataType::Boolean => {
            if field.eq_ignore_ascii_case("true")
                || field.eq_ignore_ascii_case("t")
                || field.eq_ignore_ascii_case("yes")
                || field.eq_ignore_ascii_case("y")
                || field == "1"
            {
                Ok(Value::Boolean(true))
            } else if field.eq_ignore_ascii_case("false")
                || field.eq_ignore_ascii_case("f")
                || field.eq_ignore_ascii_case("no")
                || field.eq_ignore_ascii_case("n")
                || field == "0"
            {
                Ok(Value::Boolean(false))
            } else {
                Err(Error::Type(format!(
                    "cannot convert value '{}' to BOOLEAN for column '{}'",
                    field, col_name
                )))
            }
        }
        DataType::Timestamp => radixdb_core::parse_timestamp(field)
            .map(Value::Timestamp)
            .map_err(|_| {
                Error::Type(format!(
                    "cannot convert value '{}' to TIMESTAMP for column '{}'",
                    field, col_name
                ))
            }),
        DataType::Decimal => radixdb_core::value::parse_decimal_str(field)
            .and_then(|(unscaled, precision, scale)| {
                Value::try_decimal(unscaled, precision, scale).ok()
            })
            .ok_or_else(|| {
                Error::Type(format!(
                    "cannot convert value '{}' to DECIMAL for column '{}'",
                    field, col_name
                ))
            }),
        DataType::Date => radixdb_core::value::parse_date_days_since_unix_epoch(field)
            .map(Value::date)
            .ok_or_else(|| {
                Error::Type(format!(
                    "cannot convert value '{}' to DATE for column '{}'",
                    field, col_name
                ))
            }),
        DataType::Bytes => Ok(Value::bytes(field.as_bytes().to_vec())),
        DataType::Text => {
            // SmartString::new takes &str: inlines <=15 bytes (0 allocs),
            // heap-allocates only for longer strings (1 alloc for Arc)
            Ok(Value::Text(SmartString::new(field)))
        }
        DataType::Json => Value::try_json(field).map_err(|_| {
            Error::Type(format!(
                "cannot convert value '{}' to JSON for column '{}'",
                field, col_name
            ))
        }),
        DataType::Uuid => radixdb_core::value::parse_uuid_str(field)
            .map(Value::uuid)
            .ok_or_else(|| {
                Error::Type(format!(
                    "cannot convert value '{}' to UUID for column '{}'",
                    field, col_name
                ))
            }),
        _ => {
            // Fallback: go through Value::text + coerce for uncommon types (Vector, etc.)
            let text_val = Value::text(field);
            let coerced = text_val.coerce_to_type(target_type);
            if !text_val.is_null() && coerced.is_null() {
                return Err(Error::Type(format!(
                    "cannot convert value '{}' to {:?} for column '{}'",
                    field, target_type, col_name
                )));
            }
            Ok(coerced)
        }
    }
}

#[doc(hidden)]
pub trait CopyExecutorExt: MutationHost {
    /// Execute a COPY FROM statement
    fn execute_copy(
        &self,
        stmt: &CopyStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let copy_started = Instant::now();
        let table_name = &stmt.table_name.value_lower;

        // COPY is not allowed inside explicit transactions (like PRAGMA CHECKPOINT)
        {
            let active_tx = self.mutation_active_transaction().lock().unwrap();
            if active_tx.is_some() {
                return Err(Error::InvalidArgument(
                    "COPY FROM cannot be used inside an explicit transaction".to_string(),
                ));
            }
        }

        // One storage transaction owns the target schema snapshot, every row,
        // and the publication point. A terminal parse/constraint error leaves
        // no durable prefix for the caller to discover or deduplicate.
        let mut transaction = self.mutation_engine().begin_transaction()?;
        let mut table = transaction.get_table(table_name)?;
        let schema = table.schema().clone();
        let schema_column_count = schema.columns.len();
        let copy_max_transaction_bytes = self
            .mutation_engine()
            .config()
            .persistence
            .copy_max_transaction_bytes;
        let mut copy_transaction_bytes = 0usize;

        let all_column_types: Vec<DataType> = schema.columns.iter().map(|c| c.data_type).collect();
        let all_vector_dims: Vec<u16> =
            schema.columns.iter().map(|c| c.vector_dimensions).collect();
        let default_exprs: Vec<Option<String>> = schema
            .columns
            .iter()
            .map(|c| c.default_expr.clone())
            .collect();
        let column_indices: Vec<usize> = if stmt.columns.is_empty() {
            (0..schema_column_count).collect()
        } else {
            let mut seen = FxHashSet::default();
            for identifier in &stmt.columns {
                if !seen.insert(identifier.value_lower.clone()) {
                    return Err(Error::InvalidArgument(format!(
                        "duplicate COPY target column '{}'",
                        identifier.value
                    )));
                }
            }
            let col_map = schema.column_index_map();
            stmt.columns
                .iter()
                .map(|id| {
                    col_map
                        .get(id.value_lower.as_str())
                        .copied()
                        .ok_or_else(|| Error::ColumnNotFound(id.value.to_string()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        // Pre-compute FK info
        let fk_schema: Option<CompactArc<Schema>> = if !schema.foreign_keys.is_empty() {
            Some(CompactArc::new(schema.clone()))
        } else {
            None
        };

        let parse_started = Instant::now();
        let outcome = match stmt.format {
            CopyFormat::Csv => self.copy_from_csv(
                stmt,
                &mut table,
                &schema,
                &column_indices,
                &all_column_types,
                &all_vector_dims,
                &default_exprs,
                &fk_schema,
                schema_column_count,
                &mut copy_transaction_bytes,
                copy_max_transaction_bytes,
            ),
            CopyFormat::Json => self.copy_from_json(
                stmt,
                &mut table,
                &schema,
                &column_indices,
                &all_column_types,
                &all_vector_dims,
                &default_exprs,
                &fk_schema,
                schema_column_count,
                &mut copy_transaction_bytes,
                copy_max_transaction_bytes,
            ),
        };
        let parse_elapsed = parse_started.elapsed();

        drop(table);
        let rows_affected = match outcome {
            Ok(rows_affected) => {
                let commit_started = Instant::now();
                let commit_result = transaction.commit();
                let commit_elapsed = commit_started.elapsed();
                if let Err(error) = commit_result {
                    radixdb_storage::instrumentation::record_copy(
                        rows_affected.max(0) as u64,
                        parse_elapsed,
                        commit_elapsed,
                        copy_started.elapsed(),
                    );
                    return Err(error);
                }
                if rows_affected > 0 {
                    self.invalidate_copy_caches(table_name);
                }
                radixdb_storage::instrumentation::record_copy(
                    rows_affected.max(0) as u64,
                    parse_elapsed,
                    commit_elapsed,
                    copy_started.elapsed(),
                );
                rows_affected
            }
            Err(error) => {
                transaction.rollback()?;
                radixdb_storage::instrumentation::record_copy(
                    0,
                    parse_elapsed,
                    std::time::Duration::ZERO,
                    copy_started.elapsed(),
                );
                return Err(error);
            }
        };

        Ok(Box::new(ExecResult::with_rows_affected(rows_affected)))
    }

    /// Clear table-scoped caches at the one successful COPY publication point.
    fn invalidate_copy_caches(&self, table_name: &str) {
        self.mutation_invalidate_semantic_cache(table_name);
        invalidate_semi_join_cache_for_table(table_name);
        invalidate_scalar_subquery_cache_for_table(table_name);
        invalidate_in_subquery_cache_for_table(table_name);
    }

    /// Import rows from a CSV file
    #[allow(clippy::too_many_arguments)]
    fn copy_from_csv(
        &self,
        stmt: &CopyStatement,
        table: &mut Box<dyn Table>,
        schema: &Schema,
        column_indices: &[usize],
        all_column_types: &[DataType],
        all_vector_dims: &[u16],
        default_exprs: &[Option<String>],
        fk_schema: &Option<CompactArc<Schema>>,
        schema_column_count: usize,
        copy_transaction_bytes: &mut usize,
        copy_max_transaction_bytes: usize,
    ) -> Result<i64> {
        let file = std::fs::File::open(&stmt.file_path).map_err(|e| {
            Error::InvalidArgument(format!("cannot open file '{}': {}", stmt.file_path, e))
        })?;

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(stmt.header)
            .delimiter(stmt.delimiter)
            .from_reader(std::io::BufReader::new(file));

        // If header is present and columns are not specified, try to map header names to columns
        let field_to_col: Option<Vec<usize>> = if stmt.header && stmt.columns.is_empty() {
            let headers = reader
                .headers()
                .map_err(|e| Error::InvalidArgument(format!("cannot read CSV headers: {}", e)))?;
            let col_map = {
                let m = schema.column_index_map();
                m.clone()
            };
            let mut mapping = Vec::with_capacity(headers.len());
            let mut seen_headers = FxHashSet::default();
            for h in headers.iter() {
                let lower = h.to_lowercase();
                if !seen_headers.insert(lower.clone()) {
                    return Err(Error::InvalidArgument(format!(
                        "duplicate CSV header '{}'",
                        h
                    )));
                }
                if let Some(&idx) = col_map.get(lower.as_str()) {
                    mapping.push(idx);
                } else {
                    return Err(Error::ColumnNotFound(h.to_string()));
                }
            }
            Some(mapping)
        } else {
            None
        };

        let null_str = stmt.null_string.as_deref().unwrap_or("");
        let mut rows_affected = 0i64;

        let compiled_table_checks = compile_table_check_constraints(schema)?;
        let mut table_check_vm = crate::expression::ExprVM::new();
        let mut insert_batch = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);

        for result in reader.records() {
            let record = result.map_err(|e| {
                Error::InvalidArgument(format!(
                    "CSV parse error at row {}: {}",
                    rows_affected + 1,
                    e
                ))
            })?;

            let effective_indices = field_to_col.as_deref().unwrap_or(column_indices);

            if record.len() != effective_indices.len() {
                return Err(Error::InvalidArgument(format!(
                    "CSV row {} has {} fields but expected {}",
                    rows_affected + 1,
                    record.len(),
                    effective_indices.len()
                )));
            }

            // Defaults are expressions and may be volatile; evaluate them for
            // every resulting row, never once per COPY statement.
            let mut row_values =
                build_default_row(default_exprs, all_column_types, schema_column_count)?;

            // Parse CSV fields directly into target types (no intermediate Value::text allocation)
            for (i, field) in record.iter().enumerate() {
                let col_idx = effective_indices[i];

                if field == null_str {
                    row_values[col_idx] = Value::null_unknown();
                    continue;
                }

                let target_type = all_column_types[col_idx];
                let col_name = schema.columns[col_idx].name.as_str();
                let value = parse_field(field, target_type, col_name)?;
                validate_vector_dims(&value, target_type, all_vector_dims[col_idx])?;
                row_values[col_idx] = value;
            }

            let mut row = Row::from_values(row_values);
            prepare_insert_row_constraints(
                table.as_mut(),
                schema,
                &compiled_table_checks,
                &mut row,
                &mut table_check_vm,
            )?;

            // FK parent validation
            if let Some(ref fks) = fk_schema {
                crate::mutation::foreign_key::check_parent_exists(
                    self.mutation_engine(),
                    table.txn_id(),
                    fks,
                    &row,
                )?;
            }

            account_copy_transaction_row(
                copy_transaction_bytes,
                copy_max_transaction_bytes,
                &row,
                rows_affected + 1,
            )?;
            insert_batch.push(row);
            // A later row in a self-referential COPY may depend on a parent
            // inserted earlier by the same statement. Publish FK-bearing rows
            // into the transaction-local table immediately; the whole COPY is
            // still committed or rolled back as one storage transaction.
            if fk_schema.is_some() || insert_batch.len() == COPY_INSERT_BATCH_ROWS {
                flush_copy_insert_batch(table, &mut insert_batch)?;
            }
            rows_affected += 1;
        }

        flush_copy_insert_batch(table, &mut insert_batch)?;

        Ok(rows_affected)
    }

    /// Import rows from a JSON file (JSON Lines or JSON array)
    #[allow(clippy::too_many_arguments)]
    fn copy_from_json(
        &self,
        stmt: &CopyStatement,
        table: &mut Box<dyn Table>,
        schema: &Schema,
        column_indices: &[usize],
        all_column_types: &[DataType],
        all_vector_dims: &[u16],
        default_exprs: &[Option<String>],
        fk_schema: &Option<CompactArc<Schema>>,
        schema_column_count: usize,
        copy_transaction_bytes: &mut usize,
        copy_max_transaction_bytes: usize,
    ) -> Result<i64> {
        let null_str = stmt.null_string.as_deref();
        let use_columns = !stmt.columns.is_empty();

        let compiled_table_checks = compile_table_check_constraints(schema)?;
        let mut table_check_vm = crate::expression::ExprVM::new();
        let mut insert_batch = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);

        // Pre-build lowercase column name map for case-insensitive JSON key matching
        let col_name_lower_map: FxHashMap<String, usize> = if use_columns {
            stmt.columns
                .iter()
                .enumerate()
                .map(|(i, column)| (column.value_lower.to_string(), column_indices[i]))
                .collect()
        } else {
            schema
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| (c.name.to_lowercase(), idx))
                .collect()
        };

        // Stream JSON objects one at a time with O(object) memory.
        // For JSON arrays, we strip `[`, `]`, and `,` between objects so
        // StreamDeserializer sees a sequence of top-level values.
        // For JSON Lines, objects are already top-level.
        let file = std::fs::File::open(&stmt.file_path).map_err(|e| {
            Error::InvalidArgument(format!("cannot open file '{}': {}", stmt.file_path, e))
        })?;
        let reader = JsonArrayStripper::new(std::io::BufReader::new(file));
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<serde_json::Value>();

        let mut rows_affected = 0i64;
        for (idx, result) in stream.enumerate() {
            let item = result.map_err(|e| {
                Error::InvalidArgument(format!("JSON parse error at object {}: {}", idx + 1, e))
            })?;

            let obj = item.as_object().ok_or_else(|| {
                Error::InvalidArgument(format!("JSON item {} is not an object", idx + 1))
            })?;
            let row = self.prepare_json_row(
                obj,
                table,
                schema,
                default_exprs,
                schema_column_count,
                &col_name_lower_map,
                use_columns,
                all_column_types,
                all_vector_dims,
                null_str,
                &compiled_table_checks,
                &mut table_check_vm,
                fk_schema,
                copy_transaction_bytes,
                copy_max_transaction_bytes,
                rows_affected + 1,
            )?;
            insert_batch.push(row);
            if fk_schema.is_some() || insert_batch.len() == COPY_INSERT_BATCH_ROWS {
                flush_copy_insert_batch(table, &mut insert_batch)?;
            }
            rows_affected += 1;
        }

        flush_copy_insert_batch(table, &mut insert_batch)?;

        Ok(rows_affected)
    }

    /// Prepare one JSON object for the next bounded insert batch.
    #[allow(clippy::too_many_arguments)]
    fn prepare_json_row(
        &self,
        obj: &serde_json::Map<String, serde_json::Value>,
        table: &mut Box<dyn Table>,
        schema: &Schema,
        default_exprs: &[Option<String>],
        schema_column_count: usize,
        col_name_lower_map: &FxHashMap<String, usize>,
        use_columns: bool,
        all_column_types: &[DataType],
        all_vector_dims: &[u16],
        null_str: Option<&str>,
        compiled_table_checks: &[(String, crate::expression::SharedProgram)],
        table_check_vm: &mut crate::expression::ExprVM,
        fk_schema: &Option<CompactArc<Schema>>,
        copy_transaction_bytes: &mut usize,
        copy_max_transaction_bytes: usize,
        row_number: i64,
    ) -> Result<Row> {
        let mut normalized_obj = FxHashMap::default();
        for (key, value) in obj {
            let normalized = key.to_lowercase();
            if normalized_obj.insert(normalized, value).is_some() {
                return Err(Error::InvalidArgument(format!(
                    "duplicate JSON key '{}' after case normalization",
                    key
                )));
            }
        }

        // Validate the complete supplied key domain before defaults,
        // constraints, auto-increment, or row mutation can run.
        for lower_key in normalized_obj.keys() {
            if !col_name_lower_map.contains_key(lower_key) {
                return Err(Error::ColumnNotFound(lower_key.clone()));
            }
        }

        let mut row_values =
            build_default_row(default_exprs, all_column_types, schema_column_count)?;

        if use_columns {
            // Only import specified columns
            for (lower_name, col_idx) in col_name_lower_map {
                let target_type = all_column_types[*col_idx];
                if let Some(v) = normalized_obj.get(lower_name) {
                    let value = json_value_to_radixdb(v, target_type, lower_name, null_str)?;
                    validate_vector_dims(&value, target_type, all_vector_dims[*col_idx])?;
                    row_values[*col_idx] = value;
                }
                // Missing key: keep default/null
            }
        } else {
            // Import all columns by matching JSON keys case-insensitively
            for (lower_key, json_val) in normalized_obj {
                let col_idx = col_name_lower_map[&lower_key];
                let target_type = all_column_types[col_idx];
                let value = json_value_to_radixdb(json_val, target_type, &lower_key, null_str)?;
                validate_vector_dims(&value, target_type, all_vector_dims[col_idx])?;
                row_values[col_idx] = value;
            }
        }

        let mut row = Row::from_values(row_values);
        prepare_insert_row_constraints(
            &mut **table,
            schema,
            compiled_table_checks,
            &mut row,
            table_check_vm,
        )?;

        if let Some(ref fks) = fk_schema {
            crate::mutation::foreign_key::check_parent_exists(
                self.mutation_engine(),
                table.txn_id(),
                fks,
                &row,
            )?;
        }

        account_copy_transaction_row(
            copy_transaction_bytes,
            copy_max_transaction_bytes,
            &row,
            row_number,
        )?;
        Ok(row)
    }
}

impl<T: MutationHost + ?Sized> CopyExecutorExt for T {}

/// Build the default row template from schema default expressions.
fn build_default_row(
    default_exprs: &[Option<String>],
    all_column_types: &[DataType],
    schema_column_count: usize,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(schema_column_count);
    for i in 0..schema_column_count {
        if let Some(ref default_expr) = default_exprs[i] {
            let default_type = all_column_types[i];
            row.push(evaluate_default_expr(default_expr, default_type)?);
        } else {
            row.push(Value::null_unknown());
        }
    }
    Ok(row)
}

/// Validate vector dimensions if the column is a VECTOR type.
#[inline]
fn validate_vector_dims(value: &Value, target_type: DataType, expected_dims: u16) -> Result<()> {
    if target_type == DataType::Vector && expected_dims > 0 {
        if let Value::Extension(data) = value {
            if data.first() == Some(&(DataType::Vector as u8)) {
                let got_dim = u16::try_from((data.len() - 1) / 4).unwrap_or(u16::MAX);
                if got_dim != expected_dims {
                    return Err(Error::VectorDimensionMismatch {
                        expected: expected_dims,
                        got: got_dim,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Convert a serde_json::Value to a radixdb Value with type coercion.
/// Returns an error if a non-null value silently becomes null during coercion.
fn json_value_to_radixdb(
    v: &serde_json::Value,
    target_type: DataType,
    col_name: &str,
    null_str: Option<&str>,
) -> Result<Value> {
    let val = match v {
        serde_json::Value::Null => return Ok(Value::null_unknown()),
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            // Keep the canonical decimal spelling until the destination type
            // performs its checked conversion. Routing large integers through
            // f64 silently rounds values above i64::MAX and 2^53.
            Value::text(n.to_string())
        }
        serde_json::Value::String(s) => {
            if let Some(ns) = null_str {
                if s == ns {
                    return Ok(Value::null_unknown());
                }
            }
            Value::text(s)
        }
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(v.to_string()),
    };

    val.try_coerce_to_type(target_type).map_err(|error| {
        Error::Type(format!(
            "cannot convert value '{}' to {:?} for column '{}': {}",
            val, target_type, col_name, error
        ))
    })
}

/// A Read adapter that transforms a JSON array `[{...},{...}]` into a stream
/// of top-level objects `{...} {...}` by replacing `[`, `]`, and inter-element
/// commas with whitespace. For JSON Lines input (no leading `[`), bytes pass
/// through unchanged. This lets `serde_json::StreamDeserializer` yield one
/// object at a time with O(object) memory for both formats.
struct JsonArrayStripper<R> {
    inner: R,
    is_array: bool,
    /// Nesting depth inside JSON values. 0 = between top-level values.
    depth: u32,
    /// True while inside a JSON string literal (skip structural chars).
    in_string: bool,
    /// Previous byte was `\` inside a string (skip escaped quotes).
    escape: bool,
    /// Saved first non-whitespace byte for non-array input (needs replay).
    pending: Option<u8>,
    /// An outer array must alternate object, comma, object and end with `]`.
    expect_value: bool,
    seen_value: bool,
    array_closed: bool,
    terminal_checked: bool,
}

impl<R: std::io::Read> JsonArrayStripper<R> {
    fn new(mut inner: R) -> Self {
        // Peek at first non-whitespace byte to detect array format
        let mut first = [0u8; 1];
        let (is_array, pending) = loop {
            match inner.read(&mut first) {
                Ok(1) if first[0].is_ascii_whitespace() => continue,
                Ok(1) if first[0] == b'[' => break (true, None), // `[` consumed, don't replay
                Ok(1) => break (false, Some(first[0])),          // save for replay
                _ => break (false, None),                        // empty file
            }
        };

        JsonArrayStripper {
            inner,
            is_array,
            depth: 0,
            in_string: false,
            escape: false,
            pending,
            expect_value: true,
            seen_value: false,
            array_closed: false,
            terminal_checked: false,
        }
    }
}

impl<R: std::io::Read> std::io::Read for JsonArrayStripper<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Replay the saved first byte if present
        if let Some(b) = self.pending.take() {
            buf[0] = b;
            if buf.len() == 1 {
                return Ok(1);
            }
            let n = self.inner.read(&mut buf[1..])?;
            return Ok(1 + n);
        }

        let n = self.inner.read(buf)?;
        if self.is_array {
            if n == 0 {
                if !self.terminal_checked {
                    self.terminal_checked = true;
                    if !self.array_closed || self.in_string || self.depth != 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unterminated JSON array",
                        ));
                    }
                }
            } else {
                self.strip_array_syntax(buf, n)?;
            }
        }
        Ok(n)
    }
}

impl<R> JsonArrayStripper<R> {
    /// Replace outer-array `[`, `]`, and inter-element `,` with spaces.
    /// Tracks nesting depth and string literals to avoid touching structural
    /// characters inside JSON values.
    fn strip_array_syntax(&mut self, buf: &mut [u8], len: usize) -> std::io::Result<()> {
        for b in &mut buf[..len] {
            if self.array_closed {
                if !b.is_ascii_whitespace() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "trailing data after JSON array",
                    ));
                }
                continue;
            }

            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if *b == b'\\' {
                    self.escape = true;
                } else if *b == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match *b {
                b'"' => {
                    if self.depth == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "JSON array COPY items must be objects",
                        ));
                    }
                    self.in_string = true;
                }
                b'{' if self.depth == 0 => {
                    if !self.expect_value {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "missing comma between JSON array items",
                        ));
                    }
                    self.expect_value = false;
                    self.seen_value = true;
                    self.depth = 1;
                }
                b'{' | b'[' => {
                    self.depth += 1;
                }
                b'}' | b']' => {
                    if self.depth > 0 {
                        self.depth -= 1;
                    } else if *b == b']' && (!self.expect_value || !self.seen_value) {
                        // Closing `]` of the outer array
                        *b = b' ';
                        self.array_closed = true;
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid JSON array closing delimiter",
                        ));
                    }
                }
                b',' if self.depth == 0 => {
                    if self.expect_value || !self.seen_value {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unexpected comma in JSON array",
                        ));
                    }
                    // Comma between top-level array elements
                    *b = b' ';
                    self.expect_value = true;
                }
                _ if self.depth == 0 && !b.is_ascii_whitespace() => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "JSON array COPY items must be objects",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}
