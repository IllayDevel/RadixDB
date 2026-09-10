//! Read-only host and public metadata contracts for output-shape binding.

use std::sync::Arc;

use radixdb_core::{CompactArc, DataType, LogicalTypeRef, Result, Schema};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::SelectStatement;
use radixdb_storage::mvcc::{engine::MVCCEngine, ViewDefinition};
use radixdb_storage::traits::Engine;

/// Metadata supplied by the navigation binder without exposing its physical
/// execution plan to output-shape binding.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NavigationOutputBinding {
    pub display_path: String,
    pub terminal_type: DataType,
    pub nullable: bool,
}

/// One column in a bound SELECT result.
#[doc(hidden)]
#[derive(Clone)]
pub struct BoundOutputColumn {
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: DataType,
    pub logical_type: LogicalTypeRef,
    pub type_name: String,
    pub nullable: bool,
}

/// Stable public result metadata used by transports. External type identity is
/// kept separate from the closed built-in [`DataType`] enum.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutputColumn {
    pub name: String,
    pub type_name: String,
    pub data_type: DataType,
    pub logical_type: LogicalTypeRef,
    pub nullable: bool,
}

/// Narrow composition port for schema-only navigation binding.
#[doc(hidden)]
pub trait OutputBindingHost {
    fn output_binding_engine(&self) -> &MVCCEngine;

    fn output_binding_functions(&self) -> &FunctionRegistry;

    fn output_binding_type_name(&self, name: &str) -> Result<(DataType, LogicalTypeRef, String)>;

    fn output_binding_stored_function(
        &self,
        _name: &str,
        _argument_types: &[Option<LogicalTypeRef>],
    ) -> Result<Option<(DataType, LogicalTypeRef, String, bool)>> {
        Ok(None)
    }

    fn output_binding_table_schema(&self, name_lower: &str) -> Result<CompactArc<Schema>> {
        self.output_binding_engine().get_table_schema(name_lower)
    }

    fn output_binding_view(&self, name_lower: &str) -> Result<Option<Arc<ViewDefinition>>>;

    fn output_binding_navigation(
        &self,
        select: &SelectStatement,
    ) -> Result<Vec<NavigationOutputBinding>>;
}
