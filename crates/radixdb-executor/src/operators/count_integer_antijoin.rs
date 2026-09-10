//! Count-only integer anti-join.
//!
//! This operator implements the common integrity-check shape
//! `COUNT(*) FROM outer LEFT JOIN inner ... WHERE inner.not_null IS NULL`
//! without constructing joined rows. The outer side remains a narrow stream.
//! Integer primary keys use bounded visibility probes; other integer keys use
//! a compact key set built from an exact one-column scan of the inner side.

use rustc_hash::FxHashSet;

use crate::context::ExecutionContext;
use crate::operator::{ColumnInfo, Operator, RowRef};
use radixdb_core::{Error, Result, Row, Value};
use radixdb_storage::traits::Table;

/// A full anti-count has no early-return latency requirement. Use a larger
/// metadata batch than the interactive semi-join so the segmented table can
/// reuse one publication guard and cold snapshot across many probes.
const DEFAULT_INTEGER_ANTIJOIN_PK_BATCH_SIZE: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerAntiJoinLookup {
    PrimaryKey,
    Column(usize),
}

pub struct CountIntegerAntiJoinOperator {
    outer: Box<dyn Operator>,
    inner: Box<dyn Table>,
    outer_key_index: usize,
    lookup: IntegerAntiJoinLookup,
    batch_size: usize,
    schema: Vec<ColumnInfo>,
    result: Option<Row>,
    context: Option<ExecutionContext>,
    opened: bool,
}

impl CountIntegerAntiJoinOperator {
    pub fn new(
        outer: Box<dyn Operator>,
        inner: Box<dyn Table>,
        outer_key_index: usize,
        lookup: IntegerAntiJoinLookup,
    ) -> Self {
        Self {
            outer,
            inner,
            outer_key_index,
            lookup,
            batch_size: DEFAULT_INTEGER_ANTIJOIN_PK_BATCH_SIZE,
            schema: vec![ColumnInfo::new("count")],
            result: None,
            context: None,
            opened: false,
        }
    }

    pub fn with_context(mut self, context: &ExecutionContext) -> Self {
        self.context = Some(context.clone());
        self
    }

    fn check_cancelled(&self) -> Result<()> {
        if let Some(context) = &self.context {
            context.check_cancelled()?;
        }
        Ok(())
    }

    fn count_with_primary_key_probe(&mut self) -> Result<u64> {
        let mut keys = Vec::with_capacity(self.batch_size);
        let mut matches = Vec::with_capacity(self.batch_size);
        let mut unmatched = 0_u64;
        let mut outer_rows = 0_u64;
        let mut outer_key_rows = 0_u64;

        while let Some(row) = self.outer.next()? {
            outer_rows = outer_rows.saturating_add(1);
            if outer_rows.is_multiple_of(256) {
                self.check_cancelled()?;
            }
            match row.get(self.outer_key_index) {
                Some(Value::Integer(key)) => {
                    outer_key_rows = outer_key_rows.saturating_add(1);
                    keys.push(*key);
                    if keys.len() == self.batch_size {
                        unmatched =
                            unmatched.saturating_add(self.flush_pk_batch(&mut keys, &mut matches)?);
                    }
                }
                Some(value) if value.is_null() => {
                    // SQL equality never matches NULL, so the preserved outer
                    // row belongs to the anti result.
                    unmatched = unmatched.saturating_add(1);
                }
                Some(_) => {
                    return Err(Error::internal(
                        "count integer anti-join received a non-integer outer key",
                    ));
                }
                None => {
                    return Err(Error::internal(
                        "count integer anti-join outer row omitted its key",
                    ));
                }
            }
        }
        unmatched = unmatched.saturating_add(self.flush_pk_batch(&mut keys, &mut matches)?);
        radixdb_storage::instrumentation::record_join_outer_rows(outer_rows, outer_key_rows);
        Ok(unmatched)
    }

    fn flush_pk_batch(&self, keys: &mut Vec<i64>, matches: &mut Vec<bool>) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        matches.clear();
        matches.resize(keys.len(), false);
        let hits = self.inner.probe_visible_row_ids(keys, matches)?;
        radixdb_storage::instrumentation::record_join_pk_probe(keys.len() as u64, hits as u64, 0);
        let unmatched = keys.len().saturating_sub(hits) as u64;
        keys.clear();
        Ok(unmatched)
    }

    fn count_with_integer_key_set(&mut self, inner_key_index: usize) -> Result<u64> {
        let mut scanner = self.inner.scan_exact_projection(&[inner_key_index], None)?;
        let mut inner_keys = FxHashSet::default();
        let mut inner_rows = 0_u64;
        while scanner.next() {
            inner_rows = inner_rows.saturating_add(1);
            if inner_rows.is_multiple_of(256) {
                self.check_cancelled()?;
            }
            match scanner.row().get(0) {
                Some(Value::Integer(key)) => {
                    inner_keys.insert(*key);
                }
                Some(value) if value.is_null() => {}
                Some(_) => {
                    let _ = scanner.close();
                    return Err(Error::internal(
                        "count integer anti-join received a non-integer inner key",
                    ));
                }
                None => {
                    let _ = scanner.close();
                    return Err(Error::internal(
                        "count integer anti-join inner row omitted its key",
                    ));
                }
            }
        }
        let scan_error = scanner.err().cloned();
        let close_result = scanner.close();
        if let Some(error) = scan_error {
            return Err(error);
        }
        close_result?;

        let mut unmatched = 0_u64;
        let mut outer_rows = 0_u64;
        let mut outer_key_rows = 0_u64;
        while let Some(row) = self.outer.next()? {
            outer_rows = outer_rows.saturating_add(1);
            if outer_rows.is_multiple_of(256) {
                self.check_cancelled()?;
            }
            match row.get(self.outer_key_index) {
                Some(Value::Integer(key)) => {
                    outer_key_rows = outer_key_rows.saturating_add(1);
                    if !inner_keys.contains(key) {
                        unmatched = unmatched.saturating_add(1);
                    }
                }
                Some(value) if value.is_null() => {
                    unmatched = unmatched.saturating_add(1);
                }
                Some(_) => {
                    return Err(Error::internal(
                        "count integer anti-join received a non-integer outer key",
                    ));
                }
                None => {
                    return Err(Error::internal(
                        "count integer anti-join outer row omitted its key",
                    ));
                }
            }
        }
        radixdb_storage::instrumentation::record_join_outer_rows(outer_rows, outer_key_rows);
        Ok(unmatched)
    }
}

impl Operator for CountIntegerAntiJoinOperator {
    fn open(&mut self) -> Result<()> {
        if let Err(error) = self.outer.open() {
            let _ = self.outer.close();
            return Err(error);
        }
        self.check_cancelled()?;
        let computation = match self.lookup {
            IntegerAntiJoinLookup::PrimaryKey => self.count_with_primary_key_probe(),
            IntegerAntiJoinLookup::Column(index) => self.count_with_integer_key_set(index),
        };
        let count = match computation {
            Ok(count) => count,
            Err(error) => {
                let _ = self.outer.close();
                return Err(error);
            }
        };
        self.result = Some(Row::from_values(vec![Value::Integer(
            i64::try_from(count).unwrap_or(i64::MAX),
        )]));
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        if !self.opened {
            return Err(Error::internal(
                "CountIntegerAntiJoinOperator::next called before open",
            ));
        }
        Ok(self.result.take().map(RowRef::Owned))
    }

    fn close(&mut self) -> Result<()> {
        self.outer.close()
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }

    fn name(&self) -> &str {
        "CountIntegerAntiJoin"
    }
}
