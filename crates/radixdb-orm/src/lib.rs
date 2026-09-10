//! Language-neutral contracts used by the RadixDB Rust ORM facade.
//!
//! This crate deliberately has no dependency on the server, storage engine,
//! TCP transport, or generated Rust models. Other SDKs can implement the same
//! versioned JSON and SQL fixtures without reproducing engine internals.

mod artifacts;
mod codegen;
mod dynamic;
mod gui;
mod ir;
mod record;
mod session;
mod sql;

pub use artifacts::*;
pub use codegen::*;
pub use dynamic::*;
pub use gui::*;
pub use ir::*;
pub use radixdb_core::{
    canonical_fingerprint, ColumnDescriptor, ConstraintDefinition, ConstraintDescriptor,
    DataTypeDescriptor, DatabaseDescriptor, DescriptorEnvelope, DescriptorError, DescriptorKind,
    EditorKind, ForeignKeyActionDescriptor, FormFieldDescriptor, IndexDescriptor,
    ReferenceSelector, ResultColumnDescriptor, TableDescriptor, TableFormDescriptor,
    ViewDescriptor, SCHEMA_DESCRIPTOR_VERSION,
};
pub use record::*;
pub use session::*;
pub use sql::*;
