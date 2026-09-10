mod decode;
mod encode;
mod payload;
mod primitives;
mod types;

pub use decode::decode_catalog_pack;
#[doc(hidden)]
pub use decode::decode_catalog_pack_for_max_minor;
pub use encode::encode_catalog_pack;
#[doc(hidden)]
pub use encode::{encode_catalog_pack_body, finish_catalog_pack, CatalogPackBody};
pub use primitives::{
    MAX_CATALOG_EDGES, MAX_CATALOG_FILE_BYTES, MAX_CATALOG_OBJECTS, MAX_FIELDS_PER_OBJECT,
    MAX_PAYLOAD_AREA_BYTES, MAX_PAYLOAD_BYTES_PER_OBJECT, MAX_STRING_AREA_BYTES,
};
pub use types::{CatalogPack, CatalogPackMeta};

pub(crate) use encode::encode_edge;
pub(crate) use payload::{decode_payload, encode_payload};
pub(crate) use primitives::{
    checked_product, enforce_limit, optional_id_bytes, put_bytes, put_u16, put_u32, put_u64,
    read_array, read_u16, read_u32, read_u64, EDGE_ENTRY_BYTES,
};
