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

//! Foreign Key Constraint Enforcement
//!
//! This module provides helpers for checking referential integrity:
//! - On INSERT/UPDATE: verify parent rows exist (index-based O(log n) / O(1))
//! - On DELETE/UPDATE of parent: enforce RESTRICT/CASCADE/SET NULL
//!
//! All operations participate in the caller's transaction (via `txn_id`), ensuring:
//! - CASCADE effects are atomic with the parent operation
//! - FK checks see uncommitted rows from the current transaction
//! - No independent transactions are created (no resource leaks)
//!
//! Performance guarantees:
//! - Zero cost for non-FK tables (all checks short-circuit on empty foreign_keys)
//! - Cached reverse FK mapping (rebuilt only on schema_epoch change)
//! - Index-based parent lookups (no table scans when index exists)

use std::sync::Arc;

use radixdb_core::{
    DataType, Error, ForeignKeyAction, ForeignKeyConstraint, Result, Row, Schema, Value,
};
use radixdb_storage::expression::Expression as StorageExpression;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::Engine;

fn validate_cascade_row(
    schema: &Schema,
    row: &Row,
    compiled_checks: &[(String, crate::expression::SharedProgram)],
    check_vm: &mut crate::expression::ExprVM,
) -> Result<()> {
    crate::mutation::validation::validate_resulting_row_constraints(
        schema,
        compiled_checks,
        row,
        check_vm,
    )
}

/// Check that all FK values in a row reference existing parent rows.
/// Called on INSERT and UPDATE (when FK columns change).
///
/// Uses `txn_id` to check within the caller's transaction, so uncommitted
/// parent rows (inserted in the same transaction) are visible.
///
/// Short-circuits immediately if schema has no FKs (zero cost for non-FK tables).
pub fn check_parent_exists(
    engine: &MVCCEngine,
    txn_id: i64,
    schema: &Schema,
    row: &radixdb_core::Row,
) -> Result<()> {
    for fk in &schema.foreign_keys {
        let fk_value = match row.get(fk.column_index) {
            Some(v) if !v.is_null() => v,
            _ => continue, // NULL FK is allowed (no reference)
        };

        if !parent_row_exists(
            engine,
            txn_id,
            &fk.referenced_table,
            &fk.referenced_column,
            fk_value,
        )? {
            return Err(Error::foreign_key_violation(
                &schema.table_name,
                &fk.column_name,
                &fk.referenced_table,
                &fk.referenced_column,
                format!(
                    "referenced row with {} = {} does not exist",
                    fk.referenced_column, fk_value
                ),
            ));
        }
    }
    Ok(())
}

/// Pre-validate a single FK value against its parent table.
/// Used for early validation of constant SET values in UPDATE statements
/// to prevent dirty state in explicit transactions.
///
/// NULL values are allowed (no reference) and should be skipped by the caller.
pub fn validate_fk_value(
    engine: &MVCCEngine,
    txn_id: i64,
    fk: &ForeignKeyConstraint,
    value: &Value,
    child_table: &str,
) -> Result<()> {
    if !parent_row_exists(
        engine,
        txn_id,
        &fk.referenced_table,
        &fk.referenced_column,
        value,
    )? {
        return Err(Error::foreign_key_violation(
            child_table,
            &fk.column_name,
            &fk.referenced_table,
            &fk.referenced_column,
            format!(
                "referenced row with {} = {} does not exist",
                fk.referenced_column, value
            ),
        ));
    }
    Ok(())
}

/// Check if a value exists in the parent table's referenced column.
///
/// Integer primary keys are identical to physical row IDs, so their common FK
/// path uses the transaction-aware membership probe without materializing row
/// payloads. Other referenced domains use the filtered lookup fallback.
fn parent_row_exists(
    engine: &MVCCEngine,
    txn_id: i64,
    parent_table: &str,
    parent_column: &str,
    value: &Value,
) -> Result<bool> {
    let parent_schema = engine
        .get_table_schema_for_txn(txn_id, parent_table)
        .map_err(|_| {
            Error::internal(format!(
                "foreign key references non-existent table '{}'",
                parent_table
            ))
        })?;

    let (_, ref_col) = parent_schema.find_column(parent_column).ok_or_else(|| {
        Error::internal(format!(
            "foreign key references non-existent column '{}' in table '{}'",
            parent_column, parent_table
        ))
    })?;

    let parent = engine.get_table_for_txn(txn_id, parent_table)?;
    if ref_col.primary_key && ref_col.data_type == DataType::Integer {
        if let Value::Integer(row_id) = value {
            let mut matches = [false];
            let hits = parent.probe_visible_row_ids(&[*row_id], &mut matches)?;
            return Ok(hits == 1 && matches[0]);
        }
    }

    // Generic fallback for UUID/TEXT/other unique referenced domains.
    let mut expr = radixdb_storage::expression::ComparisonExpr::new(
        ref_col.name.as_str(),
        radixdb_core::Operator::Eq,
        value.clone(),
    );
    expr.prepare_for_schema(&parent_schema);

    let rows = parent.collect_rows_with_limit_unordered(Some(&expr), 1, 0)?;
    Ok(!rows.is_empty())
}

/// Find all foreign key constraints in other tables that reference the given parent table.
/// Delegates to the engine's cached reverse mapping (rebuilt only on schema_epoch change).
/// Returns Arc-wrapped Vec (ref-count bump only, no cloning).
pub fn find_referencing_fks(
    engine: &MVCCEngine,
    parent_table: &str,
) -> Arc<Vec<(String, ForeignKeyConstraint)>> {
    engine.find_referencing_fks(parent_table)
}

pub fn find_referencing_fks_for_txn(
    engine: &MVCCEngine,
    txn_id: i64,
    parent_table: &str,
) -> Arc<Vec<(String, ForeignKeyConstraint)>> {
    engine.find_referencing_fks_for_txn(txn_id, parent_table)
}

/// Enforce referential actions for DELETE from a parent table.
/// Accepts an iterator of PK values to avoid allocating a separate Vec.
///
/// All CASCADE/SET NULL operations use `txn_id` to participate in the caller's
/// transaction, ensuring atomicity (rollback undoes cascade effects).
///
/// For each deleted PK value, checks all child tables:
/// - RESTRICT/NO ACTION: error if child rows exist
/// - CASCADE: delete matching child rows (batched per child table)
/// - SET NULL: set FK column to NULL in matching child rows (batched per child table)
///
/// Returns the total count of cascaded/affected child rows.
pub fn enforce_delete_actions_iter<'a>(
    engine: &MVCCEngine,
    txn_id: i64,
    parent_table: &str,
    parent_schema: &radixdb_core::Schema,
    deleted_rows: impl Iterator<Item = &'a Row>,
    referencing_fks: &[(String, ForeignKeyConstraint)],
) -> Result<i32> {
    if referencing_fks.is_empty() {
        return Ok(0);
    }

    let mut total_affected = 0i32;

    for row in deleted_rows {
        for (child_table_name, fk) in referencing_fks {
            let (referenced_index, _) = parent_schema
                .find_column(&fk.referenced_column)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "foreign key references missing column '{}.{}'",
                        parent_table, fk.referenced_column
                    ))
                })?;
            let referenced_value = row.get(referenced_index).ok_or_else(|| {
                Error::internal(format!(
                    "deleted row is missing referenced column '{}.{}'",
                    parent_table, fk.referenced_column
                ))
            })?;
            let action = fk.on_delete;

            match action {
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                    // Check if any child rows reference this PK value
                    if child_rows_exist(engine, txn_id, child_table_name, fk, referenced_value)? {
                        return Err(Error::foreign_key_violation(
                            child_table_name,
                            &fk.column_name,
                            parent_table,
                            &fk.referenced_column,
                            format!(
                                "cannot delete row with {} = {} — still referenced by table '{}'",
                                fk.referenced_column, referenced_value, child_table_name
                            ),
                        ));
                    }
                }
                ForeignKeyAction::Cascade => {
                    // Delete matching child rows within the caller's transaction
                    let affected =
                        cascade_delete(engine, txn_id, child_table_name, fk, referenced_value)?;
                    total_affected = total_affected.saturating_add(affected);
                }
                ForeignKeyAction::SetNull => {
                    // Set FK column to NULL in matching child rows
                    let affected =
                        set_null_on_delete(engine, txn_id, child_table_name, fk, referenced_value)?;
                    total_affected = total_affected.saturating_add(affected);
                }
            }
        }
    }

    Ok(total_affected)
}

/// Pre-check RESTRICT constraints and CASCADE depth before writing parent rows.
/// Walks the full FK tree (including recursive grandchild RESTRICT behind CASCADE/SET NULL)
/// to detect violations before any rows are modified, preserving statement atomicity.
/// Returns true if the tree has constraints that need row-level pre-checking.
pub fn pre_check_restrict_for_update(
    engine: &MVCCEngine,
    txn_id: i64,
    parent_table: &str,
    old_value: &Value,
    referencing_fks: &[(String, ForeignKeyConstraint)],
) -> Result<()> {
    pre_check_restrict_recursive(engine, txn_id, parent_table, old_value, referencing_fks, 0)
}

/// Check if the FK tree rooted at these referencing FKs needs row-level pre-checking.
/// Returns true if any path contains RESTRICT/NoAction or exceeds CASCADE depth.
/// This is a metadata-only walk (no row scans) used to skip the expensive pre-scan
/// when the tree is pure CASCADE/SET NULL within depth limits.
pub fn fk_tree_needs_precheck(
    engine: &MVCCEngine,
    txn_id: i64,
    referencing_fks: &[(String, ForeignKeyConstraint)],
) -> bool {
    fk_tree_needs_precheck_recursive(engine, txn_id, referencing_fks, 0)
}

fn fk_tree_needs_precheck_recursive(
    engine: &MVCCEngine,
    txn_id: i64,
    referencing_fks: &[(String, ForeignKeyConstraint)],
    depth: usize,
) -> bool {
    if depth >= MAX_CASCADE_DEPTH {
        return true; // depth limit will be hit → needs pre-check
    }
    for (child_table_name, fk) in referencing_fks {
        match fk.on_update {
            ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                return true;
            }
            ForeignKeyAction::Cascade | ForeignKeyAction::SetNull => {
                let grandchild_fks = find_referencing_fks_for_txn(engine, txn_id, child_table_name);
                let child_fk_col = &fk.column_name;
                let relevant: Vec<_> = grandchild_fks
                    .iter()
                    .filter(|(_, gfk)| gfk.referenced_column == *child_fk_col)
                    .cloned()
                    .collect();
                if !relevant.is_empty()
                    && fk_tree_needs_precheck_recursive(engine, txn_id, &relevant, depth + 1)
                {
                    return true;
                }
            }
        }
    }
    false
}

fn pre_check_restrict_recursive(
    engine: &MVCCEngine,
    txn_id: i64,
    parent_table: &str,
    old_value: &Value,
    referencing_fks: &[(String, ForeignKeyConstraint)],
    depth: usize,
) -> Result<()> {
    for (child_table_name, fk) in referencing_fks {
        // Check if child rows actually reference old_value before doing anything.
        // If no matching rows exist, neither RESTRICT nor CASCADE applies.
        if !child_rows_exist(engine, txn_id, child_table_name, fk, old_value)? {
            continue;
        }
        match fk.on_update {
            ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                return Err(Error::foreign_key_violation(
                    child_table_name,
                    &fk.column_name,
                    parent_table,
                    &fk.referenced_column,
                    format!(
                        "cannot update row with {} = {} — still referenced by table '{}'",
                        fk.referenced_column, old_value, child_table_name
                    ),
                ));
            }
            ForeignKeyAction::Cascade | ForeignKeyAction::SetNull => {
                // Child rows exist and will be cascaded. Check depth limit
                // now — if exceeded, the actual cascade would fail too.
                if depth >= MAX_CASCADE_DEPTH {
                    return Err(Error::internal(format!(
                        "foreign key CASCADE depth limit ({}) exceeded — possible circular reference",
                        MAX_CASCADE_DEPTH
                    )));
                }
                let grandchild_fks = find_referencing_fks_for_txn(engine, txn_id, child_table_name);
                let child_fk_col = &fk.column_name;
                let relevant: Vec<_> = grandchild_fks
                    .iter()
                    .filter(|(_, gfk)| gfk.referenced_column == *child_fk_col)
                    .cloned()
                    .collect();
                if !relevant.is_empty() {
                    pre_check_restrict_recursive(
                        engine,
                        txn_id,
                        child_table_name,
                        old_value,
                        &relevant,
                        depth + 1,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Enforce referential actions for UPDATE of a referenced column.
/// RESTRICT is already handled by pre_check_restrict_for_update before
/// the parent row is written. This function only dispatches CASCADE/SET NULL.
pub fn enforce_update_actions(
    engine: &MVCCEngine,
    txn_id: i64,
    old_pk_value: &Value,
    new_pk_value: &Value,
    referencing_fks: &[(String, ForeignKeyConstraint)],
) -> Result<i32> {
    if referencing_fks.is_empty() {
        return Ok(0);
    }

    let mut total_affected = 0i32;

    for (child_table_name, fk) in referencing_fks {
        let action = fk.on_update;

        match action {
            ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                // RESTRICT is already enforced by pre_check_restrict_for_update
                // before the parent row is written. Nothing to do here.
            }
            ForeignKeyAction::Cascade => {
                let affected = cascade_update(
                    engine,
                    txn_id,
                    child_table_name,
                    fk,
                    old_pk_value,
                    new_pk_value,
                )?;
                total_affected = total_affected.saturating_add(affected);
            }
            ForeignKeyAction::SetNull => {
                let affected =
                    set_null_on_delete(engine, txn_id, child_table_name, fk, old_pk_value)?;
                total_affected = total_affected.saturating_add(affected);
            }
        }
    }

    Ok(total_affected)
}

/// Check if any child rows in the child table reference the given parent PK value.
///
/// Uses `collect_rows_with_limit_unordered(limit=1)` with a ComparisonExpr filter
/// for both correctness and performance:
/// - O(log N) via secondary index when an index exists on the FK column
/// - Falls back to filtered scan with early termination otherwise
/// - Always txn-aware: sees uncommitted INSERTs, respects uncommitted DELETEs
fn child_rows_exist(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    parent_pk_value: &Value,
) -> Result<bool> {
    let child = engine.get_table_for_txn(txn_id, child_table)?;
    let child_schema = child.schema();

    // Build a ComparisonExpr for `fk_column = parent_pk_value`
    let col_name = &child_schema.columns[fk.column_index].name;
    let mut expr = radixdb_storage::expression::ComparisonExpr::new(
        col_name.as_str(),
        radixdb_core::Operator::Eq,
        parent_pk_value.clone(),
    );
    expr.prepare_for_schema(child_schema);

    let rows = child.collect_rows_with_limit_unordered(Some(&expr), 1, 0)?;
    Ok(!rows.is_empty())
}

/// Maximum CASCADE recursion depth to prevent infinite loops from circular FK references.
const MAX_CASCADE_DEPTH: usize = 16;

/// CASCADE DELETE: delete all child rows referencing the given parent PK value.
/// Operates within the caller's transaction (no independent commit).
/// Recursively cascades to grandchild tables (up to MAX_CASCADE_DEPTH).
fn cascade_delete(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    parent_pk_value: &Value,
) -> Result<i32> {
    cascade_delete_recursive(engine, txn_id, child_table, fk, parent_pk_value, 0)
}

fn cascade_delete_recursive(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    parent_pk_value: &Value,
    depth: usize,
) -> Result<i32> {
    if depth >= MAX_CASCADE_DEPTH {
        return Err(Error::internal(format!(
            "foreign key CASCADE depth limit ({}) exceeded — possible circular reference",
            MAX_CASCADE_DEPTH
        )));
    }

    // Before deleting child rows, collect their PK values for recursive CASCADE.
    // This is needed because the child table may itself be a parent with CASCADE children.
    let grandchild_fks = find_referencing_fks_for_txn(engine, txn_id, child_table);
    let mut deleted_child_rows: Vec<Row> = Vec::new();
    let child_schema = engine.get_table_schema(child_table)?;

    if !grandchild_fks.is_empty() {
        // Preserve complete logical rows so every grandchild FK can extract
        // its own referenced UUID/UNIQUE column. No INTEGER-only PK helper is
        // involved in recursive identity propagation.
        let child_handle = engine.get_table_for_txn(txn_id, child_table)?;
        let col_name = &child_schema.columns[fk.column_index].name;
        let mut filter = radixdb_storage::expression::ComparisonExpr::new(
            col_name.as_str(),
            radixdb_core::Operator::Eq,
            parent_pk_value.clone(),
        );
        filter.prepare_for_schema(&child_schema);
        deleted_child_rows = child_handle
            .collect_all_rows(Some(&filter))?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
    }

    // Pre-check: verify grandchild RESTRICT constraints BEFORE deleting child rows.
    // If we deleted children first and a grandchild RESTRICT check fails, the child
    // deletions would remain in the transaction state (orphaning data in explicit txns).
    if !grandchild_fks.is_empty() && !deleted_child_rows.is_empty() {
        for child_row in &deleted_child_rows {
            for (grandchild_table, grandchild_fk) in grandchild_fks.iter() {
                let (referenced_index, _) = child_schema
                    .find_column(&grandchild_fk.referenced_column)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "foreign key references missing column '{}.{}'",
                            child_table, grandchild_fk.referenced_column
                        ))
                    })?;
                let child_key = child_row.get(referenced_index).ok_or_else(|| {
                    Error::internal(format!(
                        "cascaded row is missing referenced column '{}.{}'",
                        child_table, grandchild_fk.referenced_column
                    ))
                })?;
                if matches!(
                    grandchild_fk.on_delete,
                    ForeignKeyAction::Restrict | ForeignKeyAction::NoAction
                ) && child_rows_exist(
                    engine,
                    txn_id,
                    grandchild_table,
                    grandchild_fk,
                    child_key,
                )? {
                    return Err(Error::foreign_key_violation(
                        grandchild_table,
                        &grandchild_fk.column_name,
                        child_table,
                        &grandchild_fk.referenced_column,
                        format!(
                            "cannot cascade-delete row with {} = {} — still referenced by table '{}'",
                            grandchild_fk.referenced_column, child_key, grandchild_table
                        ),
                    ));
                }
            }
        }
    }

    // Now delete the matching child rows (safe — RESTRICT checks passed above)
    let mut child = engine.get_table_for_txn(txn_id, child_table)?;
    let col_name = &child_schema.columns[fk.column_index].name;
    let mut expr = radixdb_storage::expression::ComparisonExpr::new(
        col_name.as_str(),
        radixdb_core::Operator::Eq,
        parent_pk_value.clone(),
    );
    expr.prepare_for_schema(&child_schema);

    let count = child.delete(Some(&expr))?;
    // Do NOT commit here — changes stay in TransactionVersionStore and are committed
    // atomically when the parent transaction's all-table publisher runs.

    let mut total = count;

    // Recursively enforce CASCADE/SET NULL on grandchild tables (RESTRICT already checked above)
    if !grandchild_fks.is_empty() && !deleted_child_rows.is_empty() {
        for child_row in &deleted_child_rows {
            for (grandchild_table, grandchild_fk) in grandchild_fks.iter() {
                let (referenced_index, _) = child_schema
                    .find_column(&grandchild_fk.referenced_column)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "foreign key references missing column '{}.{}'",
                            child_table, grandchild_fk.referenced_column
                        ))
                    })?;
                let child_key = child_row.get(referenced_index).ok_or_else(|| {
                    Error::internal(format!(
                        "cascaded row is missing referenced column '{}.{}'",
                        child_table, grandchild_fk.referenced_column
                    ))
                })?;
                match grandchild_fk.on_delete {
                    ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                        // Already checked above — skip
                    }
                    ForeignKeyAction::Cascade => {
                        let affected = cascade_delete_recursive(
                            engine,
                            txn_id,
                            grandchild_table,
                            grandchild_fk,
                            child_key,
                            depth + 1,
                        )?;
                        total = total.saturating_add(affected);
                    }
                    ForeignKeyAction::SetNull => {
                        let affected = set_null_on_delete(
                            engine,
                            txn_id,
                            grandchild_table,
                            grandchild_fk,
                            child_key,
                        )?;
                        total = total.saturating_add(affected);
                    }
                }
            }
        }
    }

    Ok(total)
}

/// CASCADE UPDATE: update FK column in all child rows from old to new value.
/// Recursively cascades to grandchild tables (up to MAX_CASCADE_DEPTH).
/// Operates within the caller's transaction (no independent commit).
fn cascade_update(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    old_value: &Value,
    new_value: &Value,
) -> Result<i32> {
    cascade_update_recursive(engine, txn_id, child_table, fk, old_value, new_value, 0)
}

fn cascade_update_recursive(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    old_value: &Value,
    new_value: &Value,
    depth: usize,
) -> Result<i32> {
    // Early exit: if no child rows reference old_value, cascade stops here.
    // No depth check, RESTRICT check, or writes needed.
    if !child_rows_exist(engine, txn_id, child_table, fk, old_value)? {
        return Ok(0);
    }

    // Child rows exist. Check depth limit before doing any work.
    if depth >= MAX_CASCADE_DEPTH {
        return Err(Error::internal(format!(
            "foreign key CASCADE depth limit ({}) exceeded — possible circular reference",
            MAX_CASCADE_DEPTH
        )));
    }

    // Find grandchild FKs that reference the column being updated
    let grandchild_fks = find_referencing_fks_for_txn(engine, txn_id, child_table);
    let child_fk_col = &fk.column_name;

    let relevant_grandchild_fks: Vec<_> = grandchild_fks
        .iter()
        .filter(|(_, gfk)| gfk.referenced_column == *child_fk_col)
        .collect();

    // Pre-check: verify grandchild RESTRICT constraints BEFORE updating child rows
    if !relevant_grandchild_fks.is_empty() {
        for (grandchild_table, grandchild_fk) in &relevant_grandchild_fks {
            if matches!(
                grandchild_fk.on_update,
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction
            ) && child_rows_exist(engine, txn_id, grandchild_table, grandchild_fk, old_value)?
            {
                return Err(Error::foreign_key_violation(
                    grandchild_table,
                    &grandchild_fk.column_name,
                    child_table,
                    &grandchild_fk.referenced_column,
                    format!(
                        "cannot cascade-update row with {} = {} — still referenced by table '{}'",
                        grandchild_fk.referenced_column, old_value, grandchild_table
                    ),
                ));
            }
        }
    }

    // Now update the matching child rows (safe — RESTRICT checks passed above)
    let mut child = engine.get_table_for_txn(txn_id, child_table)?;
    let col_idx = fk.column_index;
    let new_val = new_value.clone();

    let child_schema = child.schema().clone();
    let compiled_checks =
        crate::mutation::validation::compile_table_check_constraints(&child_schema)?;
    let mut check_vm = crate::expression::ExprVM::new();
    let col_name = &child_schema.columns[col_idx].name;
    let mut expr = radixdb_storage::expression::ComparisonExpr::new(
        col_name.as_str(),
        radixdb_core::Operator::Eq,
        old_value.clone(),
    );
    expr.prepare_for_schema(&child_schema);

    let count = child.update(Some(&expr), &mut |mut row| {
        let _ = row.set(col_idx, new_val.clone());
        validate_cascade_row(&child_schema, &row, &compiled_checks, &mut check_vm)?;
        Ok((row, true))
    })?;

    let mut total = count;

    if !relevant_grandchild_fks.is_empty() && count > 0 {
        for (grandchild_table, grandchild_fk) in &relevant_grandchild_fks {
            match grandchild_fk.on_update {
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                    // Already checked above
                }
                ForeignKeyAction::Cascade => {
                    let affected = cascade_update_recursive(
                        engine,
                        txn_id,
                        grandchild_table,
                        grandchild_fk,
                        old_value,
                        new_value,
                        depth + 1,
                    )?;
                    total = total.saturating_add(affected);
                }
                ForeignKeyAction::SetNull => {
                    let affected = set_null_recursive(
                        engine,
                        txn_id,
                        grandchild_table,
                        grandchild_fk,
                        old_value,
                        depth + 1,
                    )?;
                    total = total.saturating_add(affected);
                }
            }
        }
    }

    Ok(total)
}

/// SET NULL: set FK column to NULL in all child rows referencing the given parent PK value.
/// Operates within the caller's transaction (no independent commit).
fn set_null_on_delete(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    parent_pk_value: &Value,
) -> Result<i32> {
    set_null_recursive(engine, txn_id, child_table, fk, parent_pk_value, 0)
}

fn set_null_recursive(
    engine: &MVCCEngine,
    txn_id: i64,
    child_table: &str,
    fk: &ForeignKeyConstraint,
    parent_pk_value: &Value,
    depth: usize,
) -> Result<i32> {
    if !child_rows_exist(engine, txn_id, child_table, fk, parent_pk_value)? {
        return Ok(0);
    }
    if depth >= MAX_CASCADE_DEPTH {
        return Err(Error::internal(format!(
            "foreign key CASCADE depth limit ({}) exceeded — possible circular reference",
            MAX_CASCADE_DEPTH
        )));
    }

    let mut child = engine.get_table_for_txn(txn_id, child_table)?;

    let col_idx = fk.column_index;

    // Check that the FK column is nullable
    let child_schema = child.schema().clone();
    if !child_schema.columns[col_idx].nullable {
        return Err(Error::foreign_key_violation(
            child_table,
            &fk.column_name,
            &fk.referenced_table,
            &fk.referenced_column,
            format!(
                "cannot SET NULL on non-nullable column '{}'",
                fk.column_name
            ),
        ));
    }

    let null_val = Value::null(child_schema.columns[col_idx].data_type);
    let grandchild_fks = find_referencing_fks_for_txn(engine, txn_id, child_table);
    let relevant_grandchild_fks: Vec<_> = grandchild_fks
        .iter()
        .filter(|(_, grandchild_fk)| grandchild_fk.referenced_column == fk.column_name)
        .cloned()
        .collect();
    if !relevant_grandchild_fks.is_empty() {
        pre_check_restrict_recursive(
            engine,
            txn_id,
            child_table,
            parent_pk_value,
            &relevant_grandchild_fks,
            depth + 1,
        )?;
    }

    let compiled_checks =
        crate::mutation::validation::compile_table_check_constraints(&child_schema)?;
    let mut check_vm = crate::expression::ExprVM::new();
    let col_name = &child_schema.columns[col_idx].name;
    let mut expr = radixdb_storage::expression::ComparisonExpr::new(
        col_name.as_str(),
        radixdb_core::Operator::Eq,
        parent_pk_value.clone(),
    );
    expr.prepare_for_schema(&child_schema);

    let count = child.update(Some(&expr), &mut |mut row| {
        let _ = row.set(col_idx, null_val.clone());
        validate_cascade_row(&child_schema, &row, &compiled_checks, &mut check_vm)?;
        Ok((row, true))
    })?;
    // Do NOT commit here — changes committed atomically with parent transaction.

    let mut total = count;
    if count > 0 {
        for (grandchild_table, grandchild_fk) in &relevant_grandchild_fks {
            let affected = match grandchild_fk.on_update {
                ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => 0,
                ForeignKeyAction::Cascade => cascade_update_recursive(
                    engine,
                    txn_id,
                    grandchild_table,
                    grandchild_fk,
                    parent_pk_value,
                    &null_val,
                    depth + 1,
                )?,
                ForeignKeyAction::SetNull => set_null_recursive(
                    engine,
                    txn_id,
                    grandchild_table,
                    grandchild_fk,
                    parent_pk_value,
                    depth + 1,
                )?,
            };
            total = total.saturating_add(affected);
        }
    }

    Ok(total)
}

/// Check if any child tables have rows that actually reference the given parent table.
/// Used by DROP TABLE and TRUNCATE to ensure no referencing rows exist.
/// Only counts rows where the FK column is non-NULL (NULL means "no reference").
///
/// Blocks for ALL FK action types (RESTRICT, CASCADE, SET NULL, NO ACTION) because
/// DROP TABLE/TRUNCATE are DDL operations that don't cascade to child rows — they
/// would leave orphaned references. The user must delete child rows first.
///
/// When `txn_id` is provided, uses the caller's transaction for visibility (sees
/// uncommitted deletes within an explicit transaction). Otherwise creates a fresh
/// read-only transaction.
pub fn check_no_referencing_rows(
    engine: &MVCCEngine,
    parent_table: &str,
    txn_id: Option<i64>,
) -> Result<()> {
    let referencing = if let Some(txn_id) = txn_id {
        find_referencing_fks_for_txn(engine, txn_id, parent_table)
    } else {
        find_referencing_fks(engine, parent_table)
    };
    if referencing.is_empty() {
        return Ok(());
    }

    for (child_table, fk) in referencing.iter() {
        // Build IS NOT NULL filter on the FK column — pushed down to storage layer
        // so indexes can be used and we stop after the first match (limit=1)
        let child_schema = engine.get_table_schema(child_table)?;
        let col_name = &child_schema.columns[fk.column_index].name;
        let mut not_null_expr =
            radixdb_storage::expression::NullCheckExpr::is_not_null(col_name.as_str());
        not_null_expr.prepare_for_schema(&child_schema);

        let has_ref = if let Some(tid) = txn_id {
            let child = engine.get_table_for_txn(tid, child_table)?;
            !child
                .collect_rows_with_limit_unordered(Some(&not_null_expr), 1, 0)?
                .is_empty()
        } else {
            let tx = engine.begin_transaction()?;
            let child = tx.get_table(child_table)?;
            !child
                .collect_rows_with_limit_unordered(Some(&not_null_expr), 1, 0)?
                .is_empty()
        };

        if has_ref {
            return Err(Error::foreign_key_violation(
                child_table,
                &fk.column_name,
                parent_table,
                &fk.referenced_column,
                format!(
                    "cannot drop/truncate table '{}' — rows in '{}' still reference it",
                    parent_table, child_table
                ),
            ));
        }
    }

    Ok(())
}
