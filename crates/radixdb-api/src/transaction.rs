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

//! Transaction API
//!
//! Provides ACID transaction support with the same ergonomic API as Database.
//!
//! # Examples
//!
//! ```no_run
//! use radixdb_api::Database;
//! # fn main() -> radixdb_core::Result<()> {
//!
//! let db = Database::open("memory://")?;
//! db.execute("CREATE TABLE accounts (id INTEGER, balance INTEGER)", ())?;
//! db.execute("INSERT INTO accounts VALUES ($1, $2), ($3, $4)", (1, 1000, 2, 500))?;
//!
//! // Transfer money atomically
//! let mut tx = db.begin()?;
//! tx.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", (100, 1))?;
//! tx.execute("UPDATE accounts SET balance = balance + $1 WHERE id = $2", (100, 2))?;
//! tx.commit()?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use crate::params::{NamedParams, ParamVec};
use radixdb_core::{Error, Result};
use radixdb_executor::context::{ExecutionContext, ExecutionContextBuilder};
use radixdb_executor::result::ExecutionResult;
use radixdb_executor::Executor;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::mvcc::transaction::DdlFenceGuard;
use radixdb_storage::traits::Transaction as StorageTransaction;

use super::database::{DatabaseInnerHandle, FromValue};
use super::params::Params;
use super::rows::Rows;
use super::statement::Statement;

/// Transaction represents a database transaction
///
/// Provides ACID guarantees for a series of database operations.
/// Must be explicitly committed or rolled back.
pub struct Transaction {
    executor: Executor,
    database_inner: Option<Arc<DatabaseInnerHandle>>,
    /// Keeps one catalog generation stable for a logical export. Ordinary
    /// transactions leave this empty and acquire statement fences normally.
    _logical_export_fence: Option<DdlFenceGuard>,
    id: i64,
    committed: bool,
    rolled_back: bool,
}

impl Transaction {
    #[doc(hidden)]
    pub fn describe_query_output(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<radixdb_executor::QueryOutputColumn>>> {
        self.check_active()?;
        self.executor.describe_query_output(sql)
    }

    /// Create a new transaction wrapper
    pub(crate) fn new(
        tx: Box<dyn StorageTransaction>,
        database_inner: Arc<DatabaseInnerHandle>,
    ) -> Self {
        let executor = database_inner.transaction_executor();
        executor.install_transaction(tx);
        let id = executor
            .active_transaction_id()
            .expect("installed transaction must expose its identity");
        Self {
            executor,
            database_inner: Some(database_inner),
            _logical_export_fence: None,
            id,
            committed: false,
            rolled_back: false,
        }
    }

    pub(crate) fn new_logical_export(
        tx: Box<dyn StorageTransaction>,
        engine: Arc<MVCCEngine>,
        plugin_registry: Arc<radixdb_executor::PluginRegistry>,
        database_inner: Arc<DatabaseInnerHandle>,
        fence: DdlFenceGuard,
    ) -> Self {
        let executor = Executor::new_with_owned_ddl_fence(engine, plugin_registry);
        executor.install_transaction(tx);
        let id = executor
            .active_transaction_id()
            .expect("installed logical export transaction must expose its identity");
        Self {
            executor,
            database_inner: Some(database_inner),
            _logical_export_fence: Some(fence),
            id,
            committed: false,
            rolled_back: false,
        }
    }

    #[doc(hidden)]
    pub fn visit_logical_export_rows(
        &mut self,
        table_name: &str,
        visitor: &mut dyn FnMut(i64, radixdb_core::Row) -> Result<()>,
    ) -> Result<()> {
        self.check_active()?;
        self.executor.visit_logical_export_rows(table_name, visitor)
    }

    /// Check if the transaction is still active
    pub(crate) fn check_active(&self) -> Result<()> {
        if self.committed {
            return Err(Error::TransactionEnded);
        }
        if self.rolled_back {
            return Err(Error::TransactionEnded);
        }
        if !self.executor.has_active_transaction() {
            return Err(Error::TransactionNotStarted);
        }
        Ok(())
    }

    pub(crate) fn executor(&self) -> &Executor {
        &self.executor
    }

    /// Get the transaction ID
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Execute a SQL statement within the transaction
    ///
    /// # Parameters
    ///
    /// Parameters can be passed using:
    /// - Empty tuple `()` for no parameters
    /// - Tuple syntax `(1, "Alice", 30)` for multiple parameters
    /// - `params!` macro `params![1, "Alice", 30]`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut tx = db.begin()?;
    /// tx.execute("INSERT INTO users VALUES ($1, $2)", (1, "Alice"))?;
    /// tx.execute("UPDATE accounts SET balance = balance - $1 WHERE user_id = $2", (100, 1))?;
    /// tx.commit()?;
    /// ```
    pub fn execute<P: Params>(&mut self, sql: &str, params: P) -> Result<i64> {
        self.check_active()?;

        let param_values = params.into_params();
        let result = self.execute_sql(sql, param_values)?;
        Ok(result.rows_affected())
    }

    /// Execute a statement in this transaction with a cancellation deadline.
    pub fn execute_with_timeout<P: Params>(
        &mut self,
        sql: &str,
        params: P,
        timeout_ms: u64,
    ) -> Result<i64> {
        self.check_active()?;
        let ctx = ExecutionContextBuilder::new()
            .params(params.into_params())
            .timeout_ms(timeout_ms)
            .build();
        let result = self.execute_sql_with_ctx(sql, ctx)?;
        Ok(result.rows_affected())
    }

    /// Execute a high-level prepared statement with parameters.
    ///
    /// Avoids re-parsing SQL on every call — ideal for batch operations
    /// where the same statement is executed many times with different params.
    ///
    pub fn execute_prepared<P: Params>(&mut self, statement: &Statement, params: P) -> Result<i64> {
        self.check_active()?;
        let ctx = ExecutionContext::with_params(params.into_params());
        let result = self.execute_prepared_statement(statement, ctx)?;
        Ok(result.rows_affected())
    }

    /// Query using a pre-parsed statement with parameters.
    ///
    /// Avoids re-parsing SQL on every call — ideal for batch read operations
    /// where the same query is executed many times with different params.
    pub fn query_prepared<P: Params>(&mut self, statement: &Statement, params: P) -> Result<Rows> {
        self.check_active()?;
        let ctx = ExecutionContext::with_params(params.into_params());
        let result = self.execute_prepared_statement(statement, ctx)?;
        Ok(Rows::new(result))
    }

    /// Execute a prepared statement for a network session.
    #[doc(hidden)]
    pub fn query_prepared_for_server(
        &mut self,
        statement: &Statement,
        context: super::ServerExecutionContext,
    ) -> Result<Rows> {
        self.check_active()?;
        let result = self.execute_prepared_statement(statement, context.into_inner())?;
        Ok(Rows::new(result))
    }

    /// Execute a query within the transaction
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut tx = db.begin()?;
    /// for row in tx.query("SELECT * FROM users WHERE age > $1", (18,))? {
    ///     let row = row?;
    ///     println!("{}", row.get::<String>("name")?);
    /// }
    /// tx.commit()?;
    /// ```
    pub fn query<P: Params>(&mut self, sql: &str, params: P) -> Result<Rows> {
        self.check_active()?;

        let param_values = params.into_params();
        let result = self.execute_sql(sql, param_values)?;
        Ok(Rows::new(result))
    }

    /// Query in this transaction with a cancellation deadline.
    pub fn query_with_timeout<P: Params>(
        &mut self,
        sql: &str,
        params: P,
        timeout_ms: u64,
    ) -> Result<Rows> {
        self.check_active()?;
        let ctx = ExecutionContextBuilder::new()
            .params(params.into_params())
            .timeout_ms(timeout_ms)
            .build();
        let result = self.execute_sql_with_ctx(sql, ctx)?;
        Ok(Rows::new(result))
    }

    /// Execute a query and return a single value
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut tx = db.begin()?;
    /// let count: i64 = tx.query_one("SELECT COUNT(*) FROM users", ())?;
    /// tx.commit()?;
    /// ```
    pub fn query_one<T: FromValue, P: Params>(&mut self, sql: &str, params: P) -> Result<T> {
        let row = self
            .query(sql, params)?
            .next()
            .ok_or(Error::NoRowsReturned)??;
        row.get(0)
    }

    /// Execute a query and return an optional single value
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut tx = db.begin()?;
    /// let name: Option<String> = tx.query_opt("SELECT name FROM users WHERE id = $1", (999,))?;
    /// tx.commit()?;
    /// ```
    pub fn query_opt<T: FromValue, P: Params>(
        &mut self,
        sql: &str,
        params: P,
    ) -> Result<Option<T>> {
        match self.query(sql, params)?.next() {
            Some(row) => Ok(Some(row?.get(0)?)),
            None => Ok(None),
        }
    }

    /// Execute a SQL statement with named parameters within the transaction
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use radixdb::named_params;
    ///
    /// let mut tx = db.begin()?;
    /// tx.execute_named(
    ///     "INSERT INTO users VALUES (:id, :name)",
    ///     named_params!{ id: 1, name: "Alice" }
    /// )?;
    /// tx.commit()?;
    /// ```
    pub fn execute_named(&mut self, sql: &str, params: NamedParams) -> Result<i64> {
        self.check_active()?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = self.execute_sql_with_ctx(sql, ctx)?;
        Ok(result.rows_affected())
    }

    /// Execute a query with named parameters within the transaction
    pub fn query_named(&mut self, sql: &str, params: NamedParams) -> Result<Rows> {
        self.check_active()?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = self.execute_sql_with_ctx(sql, ctx)?;
        Ok(Rows::new(result))
    }

    /// Execute SQL for a network session.
    #[doc(hidden)]
    pub fn query_for_server(
        &mut self,
        sql: &str,
        context: super::ServerExecutionContext,
    ) -> Result<Rows> {
        self.check_active()?;
        let result = self.execute_sql_with_ctx(sql, context.into_inner())?;
        Ok(Rows::new(result))
    }

    /// Execute a pre-parsed statement with named parameters.
    ///
    /// Combines `execute_prepared` (skip parsing) with `execute_named` (named params).
    pub fn execute_prepared_named(
        &mut self,
        statement: &Statement,
        params: NamedParams,
    ) -> Result<i64> {
        self.check_active()?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = self.execute_prepared_statement(statement, ctx)?;
        Ok(result.rows_affected())
    }

    /// Query using a pre-parsed statement with named parameters.
    ///
    /// Combines `query_prepared` (skip parsing) with `query_named` (named params).
    pub fn query_prepared_named(
        &mut self,
        statement: &Statement,
        params: NamedParams,
    ) -> Result<Rows> {
        self.check_active()?;
        let ctx = ExecutionContext::with_named_params(params.into_inner());
        let result = self.execute_prepared_statement(statement, ctx)?;
        Ok(Rows::new(result))
    }

    /// Internal SQL execution
    fn execute_sql(&mut self, sql: &str, params: ParamVec) -> Result<ExecutionResult> {
        let ctx = if params.is_empty() {
            ExecutionContext::new()
        } else {
            ExecutionContext::with_params(params)
        };
        self.execute_sql_with_ctx(sql, ctx)
    }

    /// Internal SQL execution with a pre-built execution context
    fn execute_sql_with_ctx(
        &mut self,
        sql: &str,
        ctx: ExecutionContext,
    ) -> Result<ExecutionResult> {
        self.executor.execute_installed_transaction_sql(sql, &ctx)
    }

    fn execute_prepared_statement(
        &mut self,
        statement: &Statement,
        ctx: ExecutionContext,
    ) -> Result<ExecutionResult> {
        statement.validate_owner(
            self.database_inner
                .as_ref()
                .ok_or(Error::TransactionEnded)?,
        )?;
        self.executor.execute_installed_transaction_prepared(
            statement.prepared_program(),
            &ctx,
            statement.sql(),
        )
    }

    /// Commit the transaction
    ///
    /// All changes made within the transaction become permanent.
    pub fn commit(&mut self) -> Result<()> {
        self.check_active()?;

        match self.executor.commit_installed_transaction() {
            Ok(()) => {
                self.committed = true;
                self.database_inner.take();
            }
            Err(error) => {
                if !self.executor.has_active_transaction() {
                    self.rolled_back = true;
                    self.database_inner.take();
                }
                return Err(error);
            }
        }

        Ok(())
    }

    /// Roll back the transaction
    ///
    /// All changes made within the transaction are discarded.
    pub fn rollback(&mut self) -> Result<()> {
        if self.committed {
            return Err(Error::TransactionCommitted);
        }

        if self.rolled_back {
            return Ok(()); // Already rolled back
        }

        let result = self.executor.rollback_installed_transaction();
        // The executor takes the storage handle before cleanup. Success or
        // failure is therefore terminal for this public handle.
        self.rolled_back = true;
        self.database_inner.take();
        result
    }

    /// True when the public handle still represents an active storage
    /// transaction and can be explicitly committed or rolled back.
    pub fn is_active(&self) -> bool {
        !self.committed && !self.rolled_back && self.executor.has_active_transaction()
    }

    /// Create or replace a transaction savepoint.
    ///
    /// Names passed through the Rust API are exact strings. SQL identifier
    /// folding is applied by the SQL executor before it reaches this facade.
    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;
        self.executor.create_active_savepoint(name)
    }

    /// Roll back all changes made after `name` while retaining the target
    /// savepoint, so it can be used again or explicitly released.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;
        self.executor.rollback_active_to_savepoint(name)
    }

    /// Release a savepoint without rolling back its changes.
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;
        self.executor.release_active_savepoint(name)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Auto-rollback if not committed
        if !self.committed && !self.rolled_back {
            let _ = self.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn r3_l02_batch_b_explicit_transaction_retains_one_executor_across_statements() {
        let db = Database::open_in_memory().expect("open database");
        db.execute(
            "CREATE TABLE batch_b_executor (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .expect("create table");
        let before = radixdb_executor::test_executor_construction_count();

        let mut transaction = db.begin().expect("begin transaction");
        transaction
            .execute("INSERT INTO batch_b_executor VALUES (1, 10)", ())
            .expect("first statement");
        transaction
            .execute("INSERT INTO batch_b_executor VALUES (2, 20)", ())
            .expect("second statement");

        assert_eq!(
            radixdb_executor::test_executor_construction_count() - before,
            1,
            "an explicit transaction must construct one retained Executor, not one per statement"
        );
        transaction.rollback().expect("rollback transaction");
    }

    #[test]
    fn test_transaction_commit() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES ($1, $2)", (1, 100))
            .unwrap();

        // Verify data exists
        let value: i64 = db
            .query_one("SELECT value FROM test WHERE id = $1", (1,))
            .unwrap();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_transaction_rollback() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES ($1, $2)", (1, 100))
            .unwrap();

        let mut tx = db.begin().unwrap();
        tx.execute("UPDATE test SET value = $1 WHERE id = $2", (200, 1))
            .unwrap();
        tx.rollback().unwrap();

        let value: i64 = db
            .query_one("SELECT value FROM test WHERE id = $1", (1,))
            .unwrap();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_transaction_delete_uses_shared_executor_and_respects_outcome() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)", ())
            .unwrap();

        let mut rollback_tx = db.begin().unwrap();
        assert_eq!(
            rollback_tx
                .execute("DELETE FROM test WHERE id = $1", (2,))
                .unwrap(),
            1
        );
        let visible_inside: i64 = rollback_tx
            .query_one("SELECT COUNT(*) FROM test", ())
            .unwrap();
        assert_eq!(visible_inside, 2);
        rollback_tx.rollback().unwrap();

        let visible_after_rollback: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(visible_after_rollback, 3);

        let mut commit_tx = db.begin().unwrap();
        assert_eq!(
            commit_tx
                .execute("DELETE FROM test WHERE id = $1", (2,))
                .unwrap(),
            1
        );
        commit_tx.commit().unwrap();

        let visible_after_commit: i64 = db
            .query_one("SELECT COUNT(*) FROM test WHERE id = 2", ())
            .unwrap();
        assert_eq!(visible_after_commit, 0);
    }

    #[test]
    fn test_transaction_auto_rollback() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES ($1, $2)", (1, 100))
            .unwrap();

        {
            let mut tx = db.begin().unwrap();
            tx.execute("UPDATE test SET value = $1 WHERE id = $2", (200, 1))
                .unwrap();
            // tx dropped without commit - should auto-rollback
        }

        let value: i64 = db
            .query_one("SELECT value FROM test WHERE id = $1", (1,))
            .unwrap();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_transaction_query() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES ($1, $2)", (1, 100))
            .unwrap();

        let mut tx = db.begin().unwrap();

        // New API: query with params
        for row in tx.query("SELECT * FROM test", ()).unwrap() {
            let row = row.unwrap();
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<i64>(1).unwrap(), 100);
        }

        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_query_one() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES ($1, $2)", (1, 100))
            .unwrap();

        let mut tx = db.begin().unwrap();
        let value: i64 = tx
            .query_one("SELECT value FROM test WHERE id = $1", (1,))
            .unwrap();
        assert_eq!(value, 100);
        tx.commit().unwrap();
    }

    #[test]
    fn test_committed_transaction_error() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        let mut tx = db.begin().unwrap();
        tx.commit().unwrap();

        // Should error on further operations
        assert!(tx.execute("INSERT INTO test VALUES ($1)", (1,)).is_err());
        assert!(tx.commit().is_err());
    }

    #[test]
    fn test_transaction_id() {
        let db = Database::open_in_memory().unwrap();
        let tx = db.begin().unwrap();
        assert!(tx.id() > 0);
    }

    #[test]
    fn test_execute_prepared_insert() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value FLOAT)",
            (),
        )
        .unwrap();

        let stmt = db.prepare("INSERT INTO test VALUES ($1, $2, $3)").unwrap();

        // Execute multiple times with different params
        let mut tx = db.begin().unwrap();
        tx.execute_prepared(&stmt, (1, "Alice", 10.5)).unwrap();
        tx.execute_prepared(&stmt, (2, "Bob", 20.0)).unwrap();
        tx.execute_prepared(&stmt, (3, "Charlie", 30.0)).unwrap();
        tx.commit().unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count, 3);

        let name: String = db
            .query_one("SELECT name FROM test WHERE id = $1", (2,))
            .unwrap();
        assert_eq!(name, "Bob");
    }

    #[test]
    fn test_execute_prepared_no_params() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 0)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test VALUES (1, 100)", ()).unwrap();

        let stmt = db.prepare("UPDATE test SET value = 999").unwrap();

        let mut tx = db.begin().unwrap();
        let affected = tx.execute_prepared(&stmt, ()).unwrap();
        assert_eq!(affected, 1);
        tx.commit().unwrap();

        let value: i64 = db
            .query_one("SELECT value FROM test WHERE id = 1", ())
            .unwrap();
        assert_eq!(value, 999);
    }

    #[test]
    fn test_execute_prepared_on_committed_tx_errors() {
        let db = Database::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        let stmt = db.prepare("INSERT INTO test VALUES ($1)").unwrap();

        let mut tx = db.begin().unwrap();
        tx.commit().unwrap();
        assert!(tx.execute_prepared(&stmt, (1,)).is_err());
    }

    #[test]
    fn test_transaction_aggregate_count() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price FLOAT)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'A', 10.0)", ())
            .unwrap();
        db.execute("INSERT INTO items VALUES (2, 'B', 20.0)", ())
            .unwrap();
        db.execute("INSERT INTO items VALUES (3, 'A', 30.0)", ())
            .unwrap();

        let mut tx = db.begin().unwrap();
        let count: i64 = tx.query_one("SELECT COUNT(*) FROM items", ()).unwrap();
        assert_eq!(count, 3);

        let sum: f64 = tx.query_one("SELECT SUM(price) FROM items", ()).unwrap();
        assert!((sum - 60.0).abs() < f64::EPSILON);

        let avg: f64 = tx.query_one("SELECT AVG(price) FROM items", ()).unwrap();
        assert!((avg - 20.0).abs() < f64::EPSILON);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_group_by() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO sales VALUES (1, 'A', 10)", ())
            .unwrap();
        db.execute("INSERT INTO sales VALUES (2, 'B', 20)", ())
            .unwrap();
        db.execute("INSERT INTO sales VALUES (3, 'A', 30)", ())
            .unwrap();

        let mut tx = db.begin().unwrap();
        let rows: Vec<_> = tx
            .query(
                "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY category",
                (),
            )
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<String>(0).unwrap(), "A");
        assert_eq!(rows[0].get::<i64>(1).unwrap(), 40);
        assert_eq!(rows[1].get::<String>(0).unwrap(), "B");
        assert_eq!(rows[1].get::<i64>(1).unwrap(), 20);
        tx.commit().unwrap();
    }

    #[test]
    fn test_transaction_select_after_insert() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();

        let mut tx = db.begin().unwrap();
        tx.execute("INSERT INTO test VALUES (1, 100)", ()).unwrap();
        tx.execute("INSERT INTO test VALUES (2, 200)", ()).unwrap();

        // Should see uncommitted inserts within the same transaction
        let count: i64 = tx.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count, 2);

        let sum: i64 = tx.query_one("SELECT SUM(value) FROM test", ()).unwrap();
        assert_eq!(sum, 300);

        // Can still do more DML after SELECT delegation
        tx.execute("INSERT INTO test VALUES (3, 300)", ()).unwrap();
        let count2: i64 = tx.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(count2, 3);

        tx.commit().unwrap();

        // Verify committed data
        let final_count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        assert_eq!(final_count, 3);
    }
}
