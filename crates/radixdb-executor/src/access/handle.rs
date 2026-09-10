//! Query-local transaction and table handles.

use std::sync::{Arc, Mutex};

use radixdb_core::{Error, Result};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{Engine, Table, Transaction};

use crate::mutation::host::ActiveTransaction;

/// Owns the physical table handle and, for an implicit statement, the
/// transaction whose snapshot keeps that handle valid.
pub struct QueryTableHandle {
    pub table: Box<dyn Table>,
    pub in_explicit_transaction: bool,
    _statement_transaction: Option<Box<dyn Transaction>>,
}

pub struct QueryTablePairHandle {
    pub left: Box<dyn Table>,
    pub right: Box<dyn Table>,
    pub in_explicit_transaction: bool,
    pub statement_transaction: Option<Box<dyn Transaction>>,
}

impl QueryTableHandle {
    pub fn into_parts(self) -> (Box<dyn Table>, Option<Box<dyn Transaction>>, bool) {
        (
            self.table,
            self._statement_transaction,
            self.in_explicit_transaction,
        )
    }
}

pub enum QuerySourceHandle {
    Current(QueryTableHandle),
    Temporal(Box<dyn Transaction>),
}

/// Open a table against the explicit transaction snapshot when present, or
/// create one statement-local snapshot otherwise.
pub fn open_query_table(
    engine: &Arc<MVCCEngine>,
    active_transaction: &Mutex<Option<ActiveTransaction>>,
    table_name: &str,
) -> Result<QueryTableHandle> {
    let active = active_transaction.lock().unwrap();
    if let Some(state) = active.as_ref() {
        let table = map_table_not_found(state.transaction.get_table(table_name), table_name)?;
        drop(active);
        return Ok(QueryTableHandle {
            table,
            in_explicit_transaction: true,
            _statement_transaction: None,
        });
    }
    drop(active);

    let transaction = engine.begin_transaction()?;
    let table = map_table_not_found(transaction.get_table(table_name), table_name)?;
    Ok(QueryTableHandle {
        table,
        in_explicit_transaction: false,
        _statement_transaction: Some(transaction),
    })
}

/// Open a table without translating the storage error. Internal JOIN paths
/// retain their established error contract while sharing snapshot ownership.
pub fn open_query_table_raw(
    engine: &Arc<MVCCEngine>,
    active_transaction: &Mutex<Option<ActiveTransaction>>,
    table_name: &str,
) -> Result<QueryTableHandle> {
    let active = active_transaction.lock().unwrap();
    if let Some(state) = active.as_ref() {
        let table = state.transaction.get_table(table_name)?;
        drop(active);
        return Ok(QueryTableHandle {
            table,
            in_explicit_transaction: true,
            _statement_transaction: None,
        });
    }
    drop(active);
    let transaction = engine.begin_transaction()?;
    let table = transaction.get_table(table_name)?;
    Ok(QueryTableHandle {
        table,
        in_explicit_transaction: false,
        _statement_transaction: Some(transaction),
    })
}

/// Open two tables from exactly one query snapshot.
pub fn open_query_table_pair(
    engine: &Arc<MVCCEngine>,
    active_transaction: &Mutex<Option<ActiveTransaction>>,
    left_name: &str,
    right_name: &str,
) -> Result<QueryTablePairHandle> {
    let active = active_transaction.lock().unwrap();
    if let Some(state) = active.as_ref() {
        let left = state.transaction.get_table(left_name)?;
        let right = state.transaction.get_table(right_name)?;
        drop(active);
        return Ok(QueryTablePairHandle {
            left,
            right,
            in_explicit_transaction: true,
            statement_transaction: None,
        });
    }
    drop(active);
    let transaction = engine.begin_transaction()?;
    let left = transaction.get_table(left_name)?;
    let right = transaction.get_table(right_name)?;
    Ok(QueryTablePairHandle {
        left,
        right,
        in_explicit_transaction: false,
        statement_transaction: Some(transaction),
    })
}

/// Metadata-only table probe with query-snapshot semantics.
pub fn query_table_has_cold_segments(
    engine: &Arc<MVCCEngine>,
    active_transaction: &Mutex<Option<ActiveTransaction>>,
    table_name: &str,
) -> bool {
    open_query_table_raw(engine, active_transaction, table_name)
        .is_ok_and(|handle| handle.table.has_cold_segments())
}

/// Open the physical source once while preserving the legacy rule that an
/// explicit transaction owns the snapshot and therefore suppresses AS OF.
pub fn open_query_source(
    engine: &Arc<MVCCEngine>,
    active_transaction: &Mutex<Option<ActiveTransaction>>,
    table_name: &str,
    temporal_requested: bool,
) -> Result<QuerySourceHandle> {
    let active = active_transaction.lock().unwrap();
    if let Some(state) = active.as_ref() {
        let table = map_table_not_found(state.transaction.get_table(table_name), table_name)?;
        drop(active);
        return Ok(QuerySourceHandle::Current(QueryTableHandle {
            table,
            in_explicit_transaction: true,
            _statement_transaction: None,
        }));
    }
    drop(active);

    let transaction = engine.begin_transaction()?;
    if temporal_requested {
        return Ok(QuerySourceHandle::Temporal(transaction));
    }
    let table = map_table_not_found(transaction.get_table(table_name), table_name)?;
    Ok(QuerySourceHandle::Current(QueryTableHandle {
        table,
        in_explicit_transaction: false,
        _statement_transaction: Some(transaction),
    }))
}

/// Begin the statement-local snapshot used by temporal reads.
pub fn begin_query_transaction(engine: &Arc<MVCCEngine>) -> Result<Box<dyn Transaction>> {
    engine.begin_transaction()
}

fn map_table_not_found<T>(result: Result<T>, table_name: &str) -> Result<T> {
    result.map_err(|error| {
        if matches!(error, Error::TableNotFound(_)) {
            Error::TableOrViewNotFound(table_name.to_string())
        } else {
            error
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_storage::Config;

    #[test]
    fn missing_table_uses_public_table_or_view_error() {
        let engine = Arc::new(MVCCEngine::new(Config::in_memory()));
        engine.open_engine().unwrap();
        let active = Mutex::new(None);
        let error = match open_query_table(&engine, &active, "missing") {
            Ok(_) => panic!("missing table unexpectedly opened"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::TableOrViewNotFound(ref name) if name == "missing"),
            "unexpected error: {error:?}"
        );
    }
}
