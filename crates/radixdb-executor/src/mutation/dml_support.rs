//! Admission and row-result helpers for DML phases.

use radixdb_core::{DataType, Error, Result, Row, Schema, Value};
use radixdb_sql::ast::{Expression, Identifier};
use radixdb_storage::expression::{ComparisonExpr, Expression as StorageExpr};
use radixdb_storage::traits::Table;

/// Check whether a constraint violation matches the ON CONFLICT target columns.
/// Returns true if the conflict should be handled (DO UPDATE / DO NOTHING).
/// Returns false if the violation is on a different constraint, meaning the
/// error should be re-raised.
///
/// When `conflict_target` is empty (MySQL ON DUPLICATE KEY UPDATE style),
/// all conflicts match. When specified (PostgreSQL ON CONFLICT (cols) style),
/// only violations on matching columns are handled.
pub(super) fn conflict_matches_target(
    conflict_target: &[Identifier],
    schema: &Schema,
    error: &Error,
) -> bool {
    // Empty target = match all conflicts (MySQL semantics)
    if conflict_target.is_empty() {
        return true;
    }

    // Build normalized set of target column names
    let target_set: rustc_hash::FxHashSet<&str> = conflict_target
        .iter()
        .map(|id| id.value_lower.as_str())
        .collect();

    match error {
        Error::PrimaryKeyConstraint { .. } => {
            // Collect PK column names from schema
            let pk_cols: rustc_hash::FxHashSet<&str> = schema
                .columns
                .iter()
                .filter(|c| c.primary_key)
                .map(|c| c.name_lower.as_str())
                .collect();
            target_set == pk_cols
        }
        Error::UniqueConstraint { column, .. } => {
            // `column` is comma-separated (e.g. "a" or "a, b")
            let violated_cols: rustc_hash::FxHashSet<&str> =
                column.split(", ").map(|s| s.trim()).collect();
            // Compare lowercase: conflict_target is already lowered by Identifier::new;
            // UniqueConstraint.column comes from index column_names which are lowercase
            target_set == violated_cols
        }
        _ => false,
    }
}

pub(super) fn validate_conflict_target(
    conflict_target: &[Identifier],
    schema: &Schema,
    table: &dyn Table,
) -> Result<()> {
    if conflict_target.is_empty() {
        return Ok(());
    }

    let mut target = rustc_hash::FxHashSet::default();
    for column in conflict_target {
        if schema
            .get_column_index(column.value_lower.as_str())
            .is_none()
        {
            return Err(Error::ColumnNotFound(column.value.to_string()));
        }
        if !target.insert(column.value_lower.as_str()) {
            return Err(Error::InvalidArgument(format!(
                "ON CONFLICT target column '{}' is duplicated",
                column.value
            )));
        }
    }

    let primary_key: rustc_hash::FxHashSet<&str> = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name_lower.as_str())
        .collect();
    if !primary_key.is_empty() && target == primary_key {
        return Ok(());
    }

    let matches_unique = table.get_unique_non_pk_indexes().iter().any(|index| {
        index.partial_predicate().is_none()
            && index.column_names().len() == target.len()
            && index
                .column_names()
                .iter()
                .all(|column| target.contains(column.to_lowercase().as_str()))
    });
    if matches_unique {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "ON CONFLICT target does not name a complete PRIMARY KEY or UNIQUE constraint"
                .to_string(),
        ))
    }
}

/// Validate type coercion didn't silently fail.
/// Returns an error if a non-null value became null during coercion.
pub(super) fn validate_coercion(
    original: &Value,
    coerced: &Value,
    column_name: &str,
    target_type: DataType,
    vector_dimensions: u16,
) -> Result<()> {
    // If original was non-null but coerced is null, the conversion failed
    if !original.is_null() && coerced.is_null() {
        return Err(Error::Type(format!(
            "cannot convert value '{}' to {:?} for column '{}'",
            original, target_type, column_name
        )));
    }
    // Validate vector dimension matches column definition
    if target_type == DataType::Vector {
        if let Value::Extension(data) = coerced {
            if data.first() == Some(&(DataType::Vector as u8)) {
                // Payload is packed LE f32 bytes after the tag byte
                let got_dim = u16::try_from((data.len() - 1) / 4).unwrap_or(u16::MAX);
                // vector_dimensions == 0 means unspecified, skip check
                if vector_dimensions > 0 && got_dim != vector_dimensions {
                    return Err(Error::VectorDimensionMismatch {
                        expected: vector_dimensions,
                        got: got_dim,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Try to extract a literal value directly from an expression without VM compilation.
/// Returns Some(value) for simple literals, None for complex expressions that need VM.
#[inline]
pub(super) fn try_extract_literal(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::IntegerLiteral(lit) => Some(Value::Integer(lit.value)),
        Expression::FloatLiteral(lit) => Some(Value::Float(lit.value)),
        Expression::StringLiteral(lit) => Some(Value::text(lit.value.as_str())),
        Expression::BooleanLiteral(lit) => Some(Value::Boolean(lit.value)),
        Expression::NullLiteral(_) => Some(Value::null_unknown()),
        // Negative numbers: -5, -3.14
        Expression::Prefix(prefix) if prefix.operator == "-" => match prefix.right.as_ref() {
            Expression::IntegerLiteral(lit) => Some(Value::Integer(-lit.value)),
            Expression::FloatLiteral(lit) => Some(Value::Float(-lit.value)),
            _ => None,
        },
        _ => None, // Complex expression - needs VM
    }
}

/// Pre-compiled upsert expressions built once per INSERT statement and reused for every
/// conflicting row. Without this, `apply_on_duplicate_update` recompiles expressions and
/// re-parses CHECK constraint SQL on every conflict, causing O(n) allocation churn.
#[doc(hidden)]
pub struct CompiledUpsert {
    /// (column_index, column_type, vector_dimensions, compiled_program)
    pub(crate) compiled_updates: Vec<(usize, DataType, u16, crate::expression::SharedProgram)>,
    /// Full-row table CHECK programs.
    pub(crate) compiled_table_checks: Vec<(String, crate::expression::SharedProgram)>,
}
#[inline]
pub(super) fn auto_increment_pk_index(schema: &Schema) -> Option<usize> {
    schema
        .pk_column_index()
        .filter(|&idx| schema.columns[idx].auto_increment)
}

#[inline]
pub(super) fn capture_last_insert_id(
    row: &Row,
    auto_increment_pk_idx: Option<usize>,
    dest: &mut i64,
) {
    if let Some(idx) = auto_increment_pk_idx {
        if let Some(id) = row.get(idx).and_then(Value::as_int64) {
            *dest = id;
        }
    }
}

#[inline]
pub(super) fn insert_row_for_command_result(
    table: &mut dyn Table,
    row: Row,
    has_returning: bool,
    auto_increment_pk_idx: Option<usize>,
    last_insert_id: &mut i64,
) -> Result<Option<Row>> {
    if has_returning || auto_increment_pk_idx.is_some() {
        let inserted_row = table.insert(row)?;
        capture_last_insert_id(&inserted_row, auto_increment_pk_idx, last_insert_id);
        Ok(has_returning.then_some(inserted_row))
    } else {
        table.insert_discard(row)?;
        Ok(None)
    }
}

pub(crate) fn evaluate_default_expr(default_expr: &str, target_type: DataType) -> Result<Value> {
    let sql = format!("SELECT {default_expr}");
    let statements = radixdb_sql::parse_sql(&sql)
        .map_err(|error| Error::Parse(format!("invalid default expression: {error}")))?;
    if statements.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "default expression '{default_expr}' produced no statement"
        )));
    }

    if let radixdb_sql::ast::Statement::Select(select) = &statements[0] {
        if let Some(expression) = select.columns.first() {
            let value = crate::expression::ExpressionEval::compile(expression, &[])?
                .eval_slice(&Row::new())?;
            return value.try_coerce_to_type(target_type);
        }
    }

    Err(Error::InvalidArgument(format!(
        "default expression '{default_expr}' is not a SELECT expression"
    )))
}

/// Find a row by unique index value (supports single and composite unique constraints).
/// Uses direct index lookup (O(1) hash) instead of full table scan.
pub(super) fn find_row_by_unique_index(
    table: &dyn Table,
    schema: &radixdb_core::Schema,
    index_name: &str,
    column_name: &str,
    row_values: &[Value],
) -> Result<Option<i64>> {
    // Try direct index lookup first (O(1) for hash/multi-column indexes)
    if let Some(index) = table.get_index(index_name) {
        // Build the value array in the index's column order
        let col_ids = index.column_ids();
        let mut lookup_values: Vec<Value> = Vec::with_capacity(col_ids.len());
        for &col_id in col_ids {
            let value = row_values
                .get(col_id as usize)
                .cloned()
                .unwrap_or(Value::null_unknown());
            lookup_values.push(value);
        }

        let row_ids = index.get_row_ids_equal(&lookup_values)?;
        if let Some(&row_id) = row_ids.first() {
            return Ok(Some(row_id));
        }
        // Hot index didn't find it — fall through to scan-based lookup
        // which searches both hot buffer AND cold segments.
    }

    if let Some(row_id) = table.find_unique_conflict_row_id(index_name, column_name, row_values)? {
        return Ok(Some(row_id));
    }

    // Fallback: scan with filter expression (searches hot + cold via SegmentedTable)
    let col_names: Vec<&str> = column_name.split(", ").collect();

    let mut comparisons: Vec<Box<dyn StorageExpr>> = Vec::with_capacity(col_names.len());
    for col_name in &col_names {
        let col_lower = col_name.to_lowercase();
        let col_idx = match schema.column_index_map().get(&col_lower) {
            Some(&idx) => idx,
            None => return Ok(None),
        };
        let value = row_values
            .get(col_idx)
            .cloned()
            .unwrap_or(Value::null_unknown());

        let mut expr = ComparisonExpr::new(col_name.to_string(), radixdb_core::Operator::Eq, value);
        expr.prepare_for_schema(schema);
        comparisons.push(Box::new(expr));
    }

    let scan_expr: Box<dyn StorageExpr> = if comparisons.len() == 1 {
        comparisons.pop().unwrap()
    } else {
        use radixdb_storage::expression::AndExpr;
        let mut and_expr = AndExpr::new(comparisons);
        and_expr.prepare_for_schema(schema);
        Box::new(and_expr)
    };

    // Only project the PK column (if any) — we only need the row_id,
    // not the full row. This avoids materializing all columns.
    let pk_idx = schema.pk_column_index();
    let column_indices: Vec<usize> = if let Some(pk) = pk_idx {
        vec![pk]
    } else {
        vec![0]
    };
    let mut scanner = table.scan(&column_indices, Some(&*scan_expr))?;

    let result = if scanner.next() {
        let row_id = scanner.current_row_id()?;
        if row_id >= 0 {
            Some(row_id)
        } else if pk_idx.is_some() {
            // PK column is at index 0 in our minimal projection
            let row = scanner.row();
            if let Some(Value::Integer(id)) = row.get(0) {
                Some(*id)
            } else {
                Some(row_id)
            }
        } else {
            Some(row_id)
        }
    } else {
        None
    };

    scanner.close()?;
    Ok(result)
}
