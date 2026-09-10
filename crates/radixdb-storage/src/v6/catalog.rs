//! Catalog artifact encoding at the storage lifecycle boundary.

use std::sync::Arc;

use radixdb_catalog::{
    CatalogGeneration as RuntimeCatalogGeneration, CatalogGraph, CatalogPackMeta, CatalogPublisher,
    PreparedCatalogMutation,
};

use super::{fault::reach_generation_boundary, FormatError, FormatResult, GenerationCrashPoint};

/// Encode one catalog artifact while exposing the exact body/footer lifecycle
/// boundary to the test-only fault owner.
pub fn encode_catalog_artifact(
    meta: CatalogPackMeta,
    graph: &CatalogGraph,
) -> FormatResult<Vec<u8>> {
    let body = radixdb_catalog::encode_catalog_pack_body(meta, graph)?;
    reach_generation_boundary(GenerationCrashPoint::CatalogPackAfterBodyBeforeFooter).map_err(
        |error| FormatError::PublicationIo {
            operation: "inject after catalog body",
            kind: error.kind(),
        },
    )?;
    Ok(radixdb_catalog::finish_catalog_pack(body)?)
}

/// Publish a fully prepared immutable catalog generation through the single
/// runtime owner while exposing the before/after visibility boundaries.
pub fn publish_catalog_mutation(
    publisher: &CatalogPublisher,
    prepared: PreparedCatalogMutation,
) -> FormatResult<Arc<RuntimeCatalogGeneration>> {
    #[cfg(any(test, feature = "test-failpoints"))]
    crate::test_failpoints::interleave(
        crate::test_failpoints::InterleavePoint::CatalogBeforePublish,
        0,
    );
    reach_generation_boundary(GenerationCrashPoint::CatalogRuntimeBeforePublish).map_err(
        |error| FormatError::PublicationIo {
            operation: "inject before catalog runtime publication",
            kind: error.kind(),
        },
    )?;
    let previous = publisher.publish_prepared(prepared)?;
    #[cfg(any(test, feature = "test-failpoints"))]
    crate::test_failpoints::interleave(
        crate::test_failpoints::InterleavePoint::CatalogPublished,
        0,
    );
    reach_generation_boundary(GenerationCrashPoint::CatalogRuntimePublished).map_err(|error| {
        FormatError::PublicationRecoveryRequired {
            operation: "inject after catalog runtime publication",
            kind: error.kind(),
        }
    })?;
    Ok(previous)
}
