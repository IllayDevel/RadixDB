//! Count-only unique PK semi-join.
//!
//! This physical operator is intentionally narrower than the general join
//! operators: it consumes a filtered child key stream, checks the referenced
//! integer primary keys in bounded batches, and produces one scalar count. It
//! implements `INNER JOIN + COUNT(*)` and `INNER/LEFT JOIN +
//! COUNT(right.not_null_column)`. It never constructs a joined row or fetches a
//! parent payload row.

use crate::context::ExecutionContext;
use crate::operator::{ColumnInfo, Operator, RowRef};
use radixdb_core::{Result, Row, Value};
use radixdb_storage::traits::Table;

/// Default bound for parent membership probes. It is deliberately independent
/// of table size: memory is O(batch size), even when a child scan returns many
/// millions of rows.
pub const DEFAULT_PK_SEMIJOIN_BATCH_SIZE: usize = 1_024;

pub struct CountPkSemiJoinOperator {
    child: Box<dyn Operator>,
    parent: Box<dyn Table>,
    child_key_index: usize,
    batch_size: usize,
    schema: Vec<ColumnInfo>,
    result: Option<Row>,
    context: Option<ExecutionContext>,
    opened: bool,
}

impl CountPkSemiJoinOperator {
    pub fn new(child: Box<dyn Operator>, parent: Box<dyn Table>, child_key_index: usize) -> Self {
        Self::with_batch_size(
            child,
            parent,
            child_key_index,
            DEFAULT_PK_SEMIJOIN_BATCH_SIZE,
        )
    }

    pub fn with_batch_size(
        child: Box<dyn Operator>,
        parent: Box<dyn Table>,
        child_key_index: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            child,
            parent,
            child_key_index,
            batch_size: batch_size.max(1),
            schema: vec![ColumnInfo::new("count")],
            result: None,
            context: None,
            opened: false,
        }
    }

    /// Preserve cancellation/timeout checks while a large child stream is
    /// consumed eagerly to produce the scalar aggregate.
    pub fn with_context(mut self, context: &ExecutionContext) -> Self {
        self.context = Some(context.clone());
        self
    }

    fn flush_batch(&self, keys: &mut Vec<i64>, matches: &mut Vec<bool>) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        matches.clear();
        matches.resize(keys.len(), false);
        let hits = self.parent.probe_visible_row_ids(keys, matches)?;
        radixdb_storage::instrumentation::record_join_pk_probe(keys.len() as u64, hits as u64, 0);
        keys.clear();
        Ok(hits as u64)
    }
}

impl Operator for CountPkSemiJoinOperator {
    fn open(&mut self) -> Result<()> {
        if let Err(error) = self.child.open() {
            let _ = self.child.close();
            return Err(error);
        }
        let mut keys = Vec::with_capacity(self.batch_size);
        let mut matches = Vec::with_capacity(self.batch_size);
        let mut child_rows = 0_u64;
        let mut child_key_rows = 0_u64;
        let computation = (|| -> Result<u64> {
            let mut count = 0_u64;
            if let Some(context) = &self.context {
                context.check_cancelled()?;
            }

            while let Some(row) = self.child.next()? {
                child_rows = child_rows.saturating_add(1);
                if child_rows.is_multiple_of(256) {
                    if let Some(context) = &self.context {
                        context.check_cancelled()?;
                    }
                }
                if let Some(Value::Integer(key)) = row.get(self.child_key_index) {
                    child_key_rows = child_key_rows.saturating_add(1);
                    keys.push(*key);
                    if keys.len() == self.batch_size {
                        count = count.saturating_add(self.flush_batch(&mut keys, &mut matches)?);
                    }
                }
            }
            Ok(count.saturating_add(self.flush_batch(&mut keys, &mut matches)?))
        })();
        let count = match computation {
            Ok(count) => count,
            Err(error) => {
                // This operator consumes its input eagerly. Make the resource
                // lifecycle explicit even if a scan, cancellation or parent
                // membership probe fails halfway through the stream.
                let _ = self.child.close();
                return Err(error);
            }
        };
        radixdb_storage::instrumentation::record_join_outer_rows(child_rows, child_key_rows);

        self.result = Some(Row::from_values(vec![Value::Integer(
            i64::try_from(count).unwrap_or(i64::MAX),
        )]));
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        if !self.opened {
            return Err(radixdb_core::Error::internal(
                "CountPkSemiJoinOperator::next called before open",
            ));
        }
        Ok(self.result.take().map(RowRef::Owned))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }

    fn name(&self) -> &str {
        "CountPkSemiJoin"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MaterializedOperator;
    use std::sync::Arc;

    use radixdb_storage::mvcc::engine::MVCCEngine;
    use radixdb_storage::traits::Engine;

    #[test]
    fn counts_matching_keys_preserves_child_duplicates_and_skips_nulls() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let executor = crate::Executor::new(Arc::clone(&engine));
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        drop(executor);

        let mut write = engine.begin_transaction().unwrap();
        let mut parent = write.get_table("parent").unwrap();
        parent
            .insert(Row::from_values(vec![
                Value::Integer(10),
                Value::from("must not be read"),
            ]))
            .unwrap();
        parent
            .insert(Row::from_values(vec![
                Value::Integer(20),
                Value::from("must not be read"),
            ]))
            .unwrap();
        drop(parent);
        write.commit().unwrap();

        let mut read = engine.begin_transaction().unwrap();
        let parent = read.get_table("parent").unwrap();
        let child = MaterializedOperator::new(
            vec![
                Row::from_values(vec![Value::Integer(10)]),
                Row::from_values(vec![Value::Integer(10)]),
                Row::from_values(vec![Value::Integer(99)]),
                Row::from_values(vec![Value::null_unknown()]),
                Row::from_values(vec![Value::Integer(20)]),
            ],
            vec![ColumnInfo::new("parent_id")],
        );
        let mut op = CountPkSemiJoinOperator::with_batch_size(Box::new(child), parent, 0, 2);
        op.open().unwrap();
        let row = op.next().unwrap().unwrap().into_owned();
        assert_eq!(row.get(0), Some(&Value::Integer(3)));
        assert!(op.next().unwrap().is_none());
        op.close().unwrap();
        read.rollback().unwrap();
        engine.close_engine().unwrap();
    }

    #[test]
    fn checks_cancellation_before_consuming_child_stream() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let executor = crate::Executor::new(Arc::clone(&engine));
        executor
            .execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(executor);
        let mut tx = engine.begin_transaction().unwrap();
        let parent = tx.get_table("parent").unwrap();
        let child = MaterializedOperator::new(
            vec![Row::from_values(vec![Value::Integer(1)])],
            vec![ColumnInfo::new("parent_id")],
        );
        let context = ExecutionContext::new();
        context.cancel();
        let mut op =
            CountPkSemiJoinOperator::new(Box::new(child), parent, 0).with_context(&context);
        assert!(op.open().is_err());
        tx.rollback().unwrap();
        engine.close_engine().unwrap();
    }
}
