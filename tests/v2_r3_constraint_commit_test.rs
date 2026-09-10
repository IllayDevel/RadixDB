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

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use radixdb::{Engine, Error, Expression, MVCCEngine, Row, Schema, Value};
use rustc_hash::FxHashMap;

#[derive(Debug)]
struct FailAfterFirstMatch {
    calls: AtomicUsize,
}

impl FailAfterFirstMatch {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl Expression for FailAfterFirstMatch {
    fn evaluate(&self, _row: &Row) -> radixdb::Result<bool> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            Ok(true)
        } else {
            Err(Error::internal("intentional predicate failure"))
        }
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        self.evaluate(row).unwrap_or(false)
    }

    fn with_aliases(&self, _aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        Box::new(Self::new())
    }

    fn prepare_for_schema(&mut self, _schema: &Schema) {}

    fn is_prepared(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(Self::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn open_engine() -> Arc<MVCCEngine> {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().unwrap();
    engine
}

fn row_count(engine: &MVCCEngine, table_name: &str) -> usize {
    let transaction = engine.begin_transaction().unwrap();
    transaction
        .get_table(table_name)
        .unwrap()
        .collect_rows_with_limit_unordered(None, 100, 0)
        .unwrap()
        .len()
}

#[test]
fn engine_managed_table_handle_cannot_publish_outside_transaction_commit() {
    let engine = open_engine();
    let executor = radixdb::executor::Executor::new(Arc::clone(&engine));
    executor
        .execute("CREATE TABLE guarded (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();

    let mut transaction = engine.begin_transaction().unwrap();
    let mut table = transaction.get_table("guarded").unwrap();
    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(10),
        ]))
        .unwrap();
    assert!(matches!(table.commit(), Err(Error::NotSupported(_))));
    drop(table);
    transaction.rollback().unwrap();

    assert_eq!(row_count(&engine, "guarded"), 0);
    engine.close_engine().unwrap();
}

#[test]
fn transaction_commit_revalidates_direct_table_check_and_foreign_key_writes() {
    let engine = open_engine();
    let executor = radixdb::executor::Executor::new(Arc::clone(&engine));
    executor
        .execute(
            "CREATE TABLE checked_rows (
                id INTEGER PRIMARY KEY,
                value INTEGER,
                CHECK (value > 0)
            )",
        )
        .unwrap();
    executor
        .execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id)
            )",
        )
        .unwrap();

    let mut check_tx = engine.begin_transaction().unwrap();
    check_tx
        .get_table("checked_rows")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(-1),
        ]))
        .unwrap();
    assert!(matches!(
        check_tx.commit(),
        Err(Error::CheckConstraintViolation { .. })
    ));
    assert!(check_tx.is_active());
    check_tx.rollback().unwrap();
    assert_eq!(row_count(&engine, "checked_rows"), 0);

    let mut fk_tx = engine.begin_transaction().unwrap();
    fk_tx
        .get_table("children")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(404),
        ]))
        .unwrap();
    assert!(matches!(
        fk_tx.commit(),
        Err(Error::ForeignKeyViolation { .. })
    ));
    assert!(fk_tx.is_active());
    fk_tx.rollback().unwrap();
    assert_eq!(row_count(&engine, "children"), 0);

    engine.close_engine().unwrap();
}

#[test]
fn foreign_key_commit_race_cannot_publish_child_without_parent() {
    let engine = open_engine();
    let executor = radixdb::executor::Executor::new(Arc::clone(&engine));
    executor
        .execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id)
            )",
        )
        .unwrap();
    executor.execute("INSERT INTO parents VALUES (1)").unwrap();

    let mut child_first_started = engine.begin_transaction().unwrap();
    child_first_started
        .get_table("children")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(10),
            Value::Integer(1),
        ]))
        .unwrap();
    let mut parent_deleted_first = engine.begin_transaction().unwrap();
    assert_eq!(
        parent_deleted_first
            .get_table("parents")
            .unwrap()
            .delete_by_row_ids(&[1])
            .unwrap(),
        1
    );
    parent_deleted_first.commit().unwrap();
    assert!(matches!(
        child_first_started.commit(),
        Err(Error::ForeignKeyViolation { .. })
    ));
    child_first_started.rollback().unwrap();
    assert_eq!(row_count(&engine, "parents"), 0);
    assert_eq!(row_count(&engine, "children"), 0);

    executor.execute("INSERT INTO parents VALUES (2)").unwrap();
    let mut child_committed_first = engine.begin_transaction().unwrap();
    child_committed_first
        .get_table("children")
        .unwrap()
        .insert(Row::from_values(vec![
            Value::Integer(20),
            Value::Integer(2),
        ]))
        .unwrap();
    let mut parent_delete_loses = engine.begin_transaction().unwrap();
    assert_eq!(
        parent_delete_loses
            .get_table("parents")
            .unwrap()
            .delete_by_row_ids(&[2])
            .unwrap(),
        1
    );
    child_committed_first.commit().unwrap();
    assert!(matches!(
        parent_delete_loses.commit(),
        Err(Error::ForeignKeyViolation { .. })
    ));
    parent_delete_loses.rollback().unwrap();
    assert_eq!(row_count(&engine, "parents"), 1);
    assert_eq!(row_count(&engine, "children"), 1);

    engine.close_engine().unwrap();
}

#[test]
fn public_batch_and_scan_dml_are_failure_atomic() {
    let engine = open_engine();
    let executor = radixdb::executor::Executor::new(Arc::clone(&engine));
    executor
        .execute("CREATE TABLE batch_rows (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();

    let mut insert_tx = engine.begin_transaction().unwrap();
    let mut batch_rows = insert_tx.get_table("batch_rows").unwrap();
    assert!(batch_rows
        .insert_batch(vec![
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
            Row::from_values(vec![Value::Integer(1), Value::Integer(20)]),
        ])
        .is_err());
    assert_eq!(
        batch_rows
            .collect_rows_with_limit_unordered(None, 100, 0)
            .unwrap()
            .len(),
        0
    );
    drop(batch_rows);
    insert_tx.commit().unwrap();
    assert_eq!(row_count(&engine, "batch_rows"), 0);

    executor
        .execute("INSERT INTO batch_rows VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let mut update_tx = engine.begin_transaction().unwrap();
    let mut update_rows = update_tx.get_table("batch_rows").unwrap();
    let mut setter_calls = 0usize;
    assert!(update_rows
        .update(None, &mut |mut row| {
            setter_calls += 1;
            if setter_calls == 2 {
                return Err(Error::internal("intentional setter failure"));
            }
            row.set(1, Value::Integer(999))?;
            Ok((row, true))
        })
        .is_err());
    let mut values: Vec<i64> = update_rows
        .collect_rows_with_limit_unordered(None, 100, 0)
        .unwrap()
        .into_iter()
        .filter_map(|(_, row)| match row.get(1) {
            Some(Value::Integer(value)) => Some(*value),
            _ => None,
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![10, 20, 30]);
    drop(update_rows);
    update_tx.commit().unwrap();

    let mut delete_tx = engine.begin_transaction().unwrap();
    let mut delete_rows = delete_tx.get_table("batch_rows").unwrap();
    assert!(delete_rows
        .delete(Some(&FailAfterFirstMatch::new()))
        .is_err());
    assert_eq!(
        delete_rows
            .collect_rows_with_limit_unordered(None, 100, 0)
            .unwrap()
            .len(),
        3
    );
    drop(delete_rows);
    delete_tx.commit().unwrap();
    assert_eq!(row_count(&engine, "batch_rows"), 3);

    engine.close_engine().unwrap();
}
