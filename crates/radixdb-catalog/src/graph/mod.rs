mod edge;
mod object;
mod validate;

pub use edge::{CatalogEdge, DecodedEdgeKind, EdgeKind, ReservedEdgeKind, EDGE_VERSION};
pub use object::CatalogObject;
pub use validate::{CatalogGraph, MAX_DEPENDENCY_DEPTH};
