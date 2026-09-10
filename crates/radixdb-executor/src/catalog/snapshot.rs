use radixdb_catalog::decode_catalog_pack;
use radixdb_storage::v6::{
    encode_catalog_artifact, CatalogGeneration as DurableCatalogGeneration, CatalogId, CatalogRef,
    CatalogWalReplayLimits,
};

use super::{CatalogCheckpointHarness, CatalogHarnessError, CatalogRecovery};

#[derive(Debug, Clone)]
struct CatalogSnapshotMember {
    reference: CatalogRef,
    bytes: Vec<u8>,
}

/// Closed snapshot oracle with one complete catalog-generation member.
///
/// Logical DDL is represented only by the referenced `catalog.pack`. There is
/// no side list or object-kind-specific container to order during restore.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    catalog: CatalogSnapshotMember,
}

impl CatalogSnapshot {
    pub fn capture(
        source: &CatalogCheckpointHarness,
        limits: CatalogWalReplayLimits,
    ) -> Result<Self, CatalogHarnessError> {
        let recovery = source.reopen(limits)?;
        let bytes =
            encode_catalog_artifact(recovery.generation().meta(), recovery.generation().graph())?;
        let pack = decode_catalog_pack(&bytes)?;
        let meta = pack.meta();
        let byte_length = u64::try_from(bytes.len()).map_err(|_| {
            CatalogHarnessError::SnapshotReferenceMismatch {
                field: "byte length overflows u64",
            }
        })?;
        let reference = CatalogRef::new(
            CatalogId::from_bytes(meta.catalog_id())?,
            DurableCatalogGeneration::new(meta.catalog_generation())?,
            byte_length,
            *pack.body_sha256(),
        )?;
        Self::from_persisted_member(reference, bytes, limits)
    }

    pub fn from_persisted_member(
        reference: CatalogRef,
        bytes: Vec<u8>,
        limits: CatalogWalReplayLimits,
    ) -> Result<Self, CatalogHarnessError> {
        admit_member(reference, &bytes)?;
        CatalogCheckpointHarness::from_persisted(bytes.clone(), Vec::new(), limits)?;
        Ok(Self {
            catalog: CatalogSnapshotMember { reference, bytes },
        })
    }

    pub const fn catalog_ref(&self) -> CatalogRef {
        self.catalog.reference
    }

    pub fn catalog_bytes(&self) -> &[u8] {
        &self.catalog.bytes
    }

    pub const fn member_count(&self) -> usize {
        1
    }

    pub fn restore(
        &self,
        limits: CatalogWalReplayLimits,
    ) -> Result<CatalogCheckpointHarness, CatalogHarnessError> {
        admit_member(self.catalog.reference, &self.catalog.bytes)?;
        CatalogCheckpointHarness::from_persisted(self.catalog.bytes.clone(), Vec::new(), limits)
    }

    pub fn reopen(
        &self,
        limits: CatalogWalReplayLimits,
    ) -> Result<CatalogRecovery, CatalogHarnessError> {
        self.restore(limits)?.reopen(limits)
    }
}

fn admit_member(reference: CatalogRef, bytes: &[u8]) -> Result<(), CatalogHarnessError> {
    let actual_length =
        u64::try_from(bytes.len()).map_err(|_| CatalogHarnessError::SnapshotReferenceMismatch {
            field: "byte length overflows u64",
        })?;
    if actual_length != reference.byte_length() {
        return Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "byte length",
        });
    }

    let pack = decode_catalog_pack(bytes)?;
    let meta = pack.meta();
    if meta.catalog_id() != *reference.id().as_bytes() {
        return Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog identity",
        });
    }
    if meta.catalog_generation() != reference.generation().get() {
        return Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog generation",
        });
    }
    if pack.body_sha256() != reference.body_sha256() {
        return Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog body digest",
        });
    }
    Ok(())
}

#[cfg(test)]
mod layout_tests {
    use super::CatalogSnapshot;

    #[allow(dead_code)]
    fn snapshot_has_exactly_one_catalog_member(snapshot: CatalogSnapshot) {
        let CatalogSnapshot { catalog } = snapshot;
        let super::CatalogSnapshotMember {
            reference: _,
            bytes: _,
        } = catalog;
    }
}
