//! Physical immutable-volume primitives and I/O.

#[doc(hidden)]
pub mod column;
#[doc(hidden)]
pub mod index_hash;
#[doc(hidden)]
pub mod index_metadata;
#[doc(hidden)]
pub mod manifest;
pub mod scanner;
#[doc(hidden)]
pub mod stats;
pub mod table;
#[cfg(test)]
pub(crate) mod test_artifact;
pub mod writer;
pub mod zonemap;
