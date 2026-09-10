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

//! Internal composition seam for the executor-owned index optimizer.

use crate::index_optimizer::IndexOptimizerHost;
use crate::subquery::SubqueryExecutorExt;
use radixdb_core::{Result, RowVec};
use radixdb_sql::ast::{Expression, SelectStatement};
use radixdb_storage::traits::QueryResult;

use super::Executor;
use crate::context::ExecutionContext;
use crate::planner::QueryPlanner;

impl IndexOptimizerHost for Executor {
    fn index_project_rows(
        &self,
        select_exprs: &[Expression],
        rows: RowVec,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        self.project_rows(select_exprs, rows, all_columns, ctx)
    }

    fn index_project_rows_with_alias(
        &self,
        select_exprs: &[Expression],
        rows: RowVec,
        all_columns: &[String],
        all_columns_lower: Option<&[String]>,
        ctx: &ExecutionContext,
        table_alias: Option<&str>,
    ) -> Result<RowVec> {
        self.project_rows_with_alias(
            select_exprs,
            rows,
            all_columns,
            all_columns_lower,
            ctx,
            table_alias,
        )
    }

    fn index_output_column_names(
        &self,
        select_exprs: &[Expression],
        all_columns: &[String],
        table_alias: Option<&str>,
    ) -> Vec<String> {
        self.get_output_column_names(select_exprs, all_columns, table_alias)
    }

    fn index_query_planner(&self) -> &QueryPlanner {
        self.get_query_planner()
    }

    fn index_execute_select(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select(stmt, ctx)
    }

    fn index_process_where_subqueries(
        &self,
        expr: &Expression,
        ctx: &ExecutionContext,
    ) -> Result<Expression> {
        self.process_where_subqueries(expr, ctx)
    }

    fn index_has_subqueries(expr: &Expression) -> bool {
        Executor::has_subqueries(expr)
    }

    fn index_is_subquery_correlated(subquery: &SelectStatement) -> bool {
        Executor::is_subquery_correlated(subquery)
    }
}
