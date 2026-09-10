//! DDL, DML, COPY, referential-integrity, and compiled fast-path owners.

pub mod copy;
pub mod ddl;
pub mod dml;
pub mod dml_fast_path;
mod dml_support;
pub(crate) use dml_support::evaluate_default_expr;
pub mod extension;
pub mod external_type;
pub mod foreign_key;
pub mod host;
pub mod operator;
pub mod partial_index;
mod persistent_value;
pub mod pk_fast_path;
mod returning;
pub mod row_validation;
mod type_binding;
mod upsert;
pub mod validation;
pub mod view_binding;
