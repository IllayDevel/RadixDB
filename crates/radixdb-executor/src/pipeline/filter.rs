//! WHERE/HAVING filter admission for the relational pipeline.

use radixdb_core::Result;
use radixdb_sql::ast::Expression;
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;
use crate::expression::RowFilter;
use crate::result::FilteredResult;

/// Compile a predicate once and attach it to a streaming result.
pub fn apply(
    input: Box<dyn QueryResult>,
    predicate: &Expression,
    ctx: &ExecutionContext,
) -> Result<Box<dyn QueryResult>> {
    let filter = RowFilter::new(predicate, input.columns())?.with_context(ctx);
    Ok(Box::new(FilteredResult::from_filter(input, filter)))
}
