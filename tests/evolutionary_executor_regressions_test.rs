//! Regression tests retained while deleting the former root executor facades.

mod executor {
    pub use radixdb_executor::*;
}

#[path = "evolutionary_executor_regressions/aggregation.rs"]
mod aggregation;
#[path = "evolutionary_executor_regressions/cte.rs"]
mod cte;
#[path = "evolutionary_executor_regressions/dml.rs"]
mod dml;
#[path = "evolutionary_executor_regressions/navigation.rs"]
mod navigation;
#[path = "evolutionary_executor_regressions/window.rs"]
mod window;
