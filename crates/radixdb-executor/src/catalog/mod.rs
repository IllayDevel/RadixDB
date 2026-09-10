//! Catalog DDL binding and transaction-local catalog visibility.
//!
//! The module converts SQL DDL into typed catalog mutations. Durable
//! publication remains owned by the storage transaction boundary.

mod constraints;
mod extension;
mod external_type;
mod index;
mod lookup;
mod native_function;
mod operator;
mod procedural;
mod reconcile;
mod runtime;
pub(crate) mod security;
mod table;
mod transaction;
mod view;

pub use lookup::TableCatalog;
pub(crate) use procedural::{
    resolve_namespace_path, resolve_optional_job, resolve_optional_routine_signature,
    resolve_optional_trigger, resolve_trigger_function, validate_routine_source_contract,
    BoundJobDefinition, PROCEDURAL_COMPILER_ABI, PROCEDURAL_RUNTIME_ABI,
};
pub(crate) use runtime::{
    bind_pending_index_semantics, bind_runtime_view, list_runtime_views, qualified_catalog_name,
};
pub use runtime::{bind_runtime_catalog, plugin_catalog_runtime_binder};
pub(crate) use table::{bind_catalog_type, bind_catalog_type_in_generation};
pub use transaction::DdlTransaction;
pub use view::ViewCatalog;

#[cfg(test)]
mod checkpoint;
#[cfg(test)]
mod snapshot;
#[cfg(test)]
pub use checkpoint::{CatalogCheckpointHarness, CatalogHarnessError, CatalogRecovery};
#[cfg(test)]
pub use snapshot::CatalogSnapshot;
#[cfg(test)]
pub use transaction::DdlTransactionState;

#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod vertical_tests;
