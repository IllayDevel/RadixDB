pub(crate) mod codec;
mod database;
mod model;
mod table;

pub(crate) use database::encoded_database_manifest_length;
pub use database::{decode_database_manifest, encode_database_manifest};
pub use model::{
    DatabaseManifest, SegmentDescriptor, SegmentKind, SegmentTier, TableManifest, TableManifestRef,
    MAX_ROWS_PER_DATA_ARTIFACT, MAX_SEGMENTS_PER_TABLE_MANIFEST, MAX_TABLES_PER_DATABASE,
};
pub use table::{decode_table_manifest, encode_table_manifest};
