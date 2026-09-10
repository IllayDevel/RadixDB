//! Functional slices of the `Table` implementation for segmented storage.
//!
//! Each macro expands only trait methods. The split keeps one Rust trait impl
//! while assigning schema, mutation, read, aggregate, index, lifecycle and
//! query behavior to bounded source owners.

use super::*;

mod aggregate;
mod indexes;
mod lifecycle;
mod mutation;
mod ordered;
mod pushdown;
mod query;
mod read;
mod schema;

use aggregate::segmented_table_aggregate_methods;
use indexes::segmented_table_index_methods;
use lifecycle::segmented_table_lifecycle_methods;
use mutation::segmented_table_mutation_methods;
use ordered::segmented_table_ordered_methods;
use pushdown::segmented_table_pushdown_methods;
use query::segmented_table_query_methods;
use read::segmented_table_read_methods;
use schema::segmented_table_schema_methods;

impl Table for SegmentedTable {
    segmented_table_schema_methods!();
    segmented_table_mutation_methods!();
    segmented_table_read_methods!();
    segmented_table_aggregate_methods!();
    segmented_table_ordered_methods!();
    segmented_table_lifecycle_methods!();
    segmented_table_index_methods!();
    segmented_table_query_methods!();
    segmented_table_pushdown_methods!();
}
