//! Canonical value, identity, and shared contracts used by RadixDB crates.
//!
//! This is a private implementation crate, not an application dependency.
//! External embedded applications use the public types re-exported by
//! `radixdb`; lower crates use this owner directly to keep type identity.

pub mod checksum;
pub mod compact_arc;
pub mod compact_vec;
pub mod cow_btree;
pub mod error;
pub mod form_descriptor;
pub mod i64_map;
pub mod identity;
pub mod params;
pub mod row;
pub mod row_vec;
pub mod schema;
pub mod schema_descriptor;
pub mod smart_string;
pub mod string_map;
pub mod time_compat;
pub mod types;
pub mod unicode;
pub mod value;
pub mod vector;

#[doc(hidden)]
pub use checksum::{crc32_ieee, derive_plugin_object_identity_bytes, sha256_digest};
pub use compact_arc::{CompactArc, CompactArcDrop};
pub use compact_vec::CompactVec;
pub use cow_btree::CowBTree;
pub use error::{Error, ErrorCategory, ErrorCode, ErrorContext, NavigationErrorCode, Result};
pub use form_descriptor::{
    EditorKind, FormFieldDescriptor, ReferenceSelector, TableFormDescriptor,
};
pub use i64_map::{I64Map, I64Set};
#[doc(hidden)]
pub use identity::new_durable_identity_bytes;
pub use identity::{SchemaColumnId, SchemaTableId};
pub use params::ParamVec;
pub use row::{Row, RowColumnRef, RowIter, RowIterMut, RowSchema};
pub use row_vec::{RowIdVec, RowVec};
#[doc(hidden)]
pub use schema::generated_constraint_name;
pub use schema::{
    ForeignKeyConstraint, ReferenceDescriptor, ReferenceTargetKey, Schema, SchemaBuilder,
    SchemaColumn, SchemaConstraint, SchemaConstraintKind, MAX_CONSTRAINT_NAME_BYTES,
};
pub use schema_descriptor::{
    canonical_fingerprint, ColumnDescriptor, ConstraintDefinition, ConstraintDescriptor,
    DataTypeDescriptor, DatabaseDescriptor, DescriptorEnvelope, DescriptorError, DescriptorKind,
    ForeignKeyActionDescriptor, IndexDescriptor, ResultColumnDescriptor, TableDescriptor,
    ViewDescriptor, SCHEMA_DESCRIPTOR_VERSION,
};
pub use smart_string::SmartString;
pub use string_map::{StringMap, StringSet};
pub use types::{
    DataType, ExternalTypeRef, ForeignKeyAction, IndexEntry, IndexType, IsolationLevel,
    LogicalTypeRef, Operator,
};
pub use unicode::{canonical_unicode_lowercase_nfc, canonical_unicode_nfc};
pub use value::{parse_timestamp, ExternalValueRef, Value, NULL_VALUE};

/// Hash map for canonical values with randomized hashing.
pub type ValueMap<V> = ahash::AHashMap<Value, V>;

/// Hash set for canonical values with randomized hashing.
pub type ValueSet = ahash::AHashSet<Value>;
