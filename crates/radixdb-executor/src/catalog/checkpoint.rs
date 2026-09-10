use std::fmt;

use radixdb_catalog::{decode_catalog_pack, CatalogError, CatalogGeneration};
use radixdb_core::Error;
use radixdb_storage::v6::{
    decode_catalog_wal, encode_catalog_artifact, encode_catalog_wal_transaction,
    replay_catalog_wal, CatalogWalReplayLimits, CatalogWalTransaction, FormatError,
};

use super::view::validate_all_views;

/// Failure while admitting or advancing the closed catalog persistence harness.
#[derive(Debug)]
pub enum CatalogHarnessError {
    Catalog(CatalogError),
    Storage(FormatError),
    Semantic(Error),
    IncompleteWalTail { bytes: usize },
    SnapshotReferenceMismatch { field: &'static str },
}

impl fmt::Display for CatalogHarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "catalog pack rejected: {error}"),
            Self::Storage(error) => write!(formatter, "catalog WAL rejected: {error}"),
            Self::Semantic(error) => write!(formatter, "catalog semantics rejected: {error}"),
            Self::IncompleteWalTail { bytes } => write!(
                formatter,
                "catalog WAL has an incomplete {bytes}-byte tail; recover it before append"
            ),
            Self::SnapshotReferenceMismatch { field } => {
                write!(formatter, "catalog snapshot reference mismatch: {field}")
            }
        }
    }
}

impl std::error::Error for CatalogHarnessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Semantic(error) => Some(error),
            Self::IncompleteWalTail { .. } | Self::SnapshotReferenceMismatch { .. } => None,
        }
    }
}

impl From<CatalogError> for CatalogHarnessError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<FormatError> for CatalogHarnessError {
    fn from(error: FormatError) -> Self {
        Self::Storage(error)
    }
}

/// One admitted reopen result; the generation is rebuilt from pack plus WAL.
#[derive(Debug)]
pub struct CatalogRecovery {
    generation: CatalogGeneration,
    committed_transactions: usize,
    committed_wal_bytes: usize,
    incomplete_tail_bytes: usize,
}

impl CatalogRecovery {
    pub const fn generation(&self) -> &CatalogGeneration {
        &self.generation
    }

    pub const fn committed_transactions(&self) -> usize {
        self.committed_transactions
    }

    pub const fn committed_wal_bytes(&self) -> usize {
        self.committed_wal_bytes
    }

    pub const fn incomplete_tail_bytes(&self) -> usize {
        self.incomplete_tail_bytes
    }
}

/// Observable result of folding committed WAL into a new full catalog pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogCheckpointOutcome {
    pub folded_transactions: usize,
    pub discarded_incomplete_tail_bytes: usize,
    pub catalog_pack_bytes: usize,
}

/// In-memory durable-byte oracle for catalog checkpoint and recovery.
///
/// It is compiled only inside the closed executor harness. The production
/// engine, its current replay path and its filesystem authority are untouched.
#[derive(Debug)]
pub struct CatalogCheckpointHarness {
    catalog_pack: Vec<u8>,
    catalog_wal: Vec<u8>,
}

impl CatalogCheckpointHarness {
    pub fn from_generation(generation: &CatalogGeneration) -> Result<Self, CatalogHarnessError> {
        let catalog_pack = encode_catalog_artifact(generation.meta(), generation.graph())?;
        Self::from_persisted(catalog_pack, Vec::new(), CatalogWalReplayLimits::hard())
    }

    pub fn from_persisted(
        catalog_pack: Vec<u8>,
        catalog_wal: Vec<u8>,
        limits: CatalogWalReplayLimits,
    ) -> Result<Self, CatalogHarnessError> {
        let harness = Self {
            catalog_pack,
            catalog_wal,
        };
        harness.reopen(limits)?;
        Ok(harness)
    }

    pub fn catalog_pack_bytes(&self) -> &[u8] {
        &self.catalog_pack
    }

    pub fn catalog_wal_bytes(&self) -> &[u8] {
        &self.catalog_wal
    }

    pub fn reopen(
        &self,
        limits: CatalogWalReplayLimits,
    ) -> Result<CatalogRecovery, CatalogHarnessError> {
        recover(&self.catalog_pack, &self.catalog_wal, limits)
    }

    pub fn append_committed(
        &mut self,
        transaction: &CatalogWalTransaction,
        limits: CatalogWalReplayLimits,
    ) -> Result<CatalogRecovery, CatalogHarnessError> {
        let current = self.reopen(limits)?;
        if current.incomplete_tail_bytes() != 0 {
            return Err(CatalogHarnessError::IncompleteWalTail {
                bytes: current.incomplete_tail_bytes(),
            });
        }

        let encoded = encode_catalog_wal_transaction(transaction)?;
        let mut candidate = self.catalog_wal.clone();
        candidate.extend_from_slice(&encoded);
        let recovered = recover(&self.catalog_pack, &candidate, limits)?;
        debug_assert_eq!(recovered.incomplete_tail_bytes(), 0);
        self.catalog_wal = candidate;
        Ok(recovered)
    }

    pub fn discard_incomplete_tail(
        &mut self,
        limits: CatalogWalReplayLimits,
    ) -> Result<usize, CatalogHarnessError> {
        let recovery = self.reopen(limits)?;
        let discarded = recovery.incomplete_tail_bytes();
        self.catalog_wal.truncate(recovery.committed_wal_bytes());
        Ok(discarded)
    }

    pub fn checkpoint(
        &mut self,
        limits: CatalogWalReplayLimits,
    ) -> Result<CatalogCheckpointOutcome, CatalogHarnessError> {
        let recovery = self.reopen(limits)?;
        let catalog_pack =
            encode_catalog_artifact(recovery.generation().meta(), recovery.generation().graph())?;

        // Re-admit the exact new durable image before replacing either owner.
        recover(&catalog_pack, &[], limits)?;
        let outcome = CatalogCheckpointOutcome {
            folded_transactions: recovery.committed_transactions(),
            discarded_incomplete_tail_bytes: recovery.incomplete_tail_bytes(),
            catalog_pack_bytes: catalog_pack.len(),
        };
        self.catalog_pack = catalog_pack;
        self.catalog_wal.clear();
        Ok(outcome)
    }
}

fn recover(
    catalog_pack: &[u8],
    catalog_wal: &[u8],
    limits: CatalogWalReplayLimits,
) -> Result<CatalogRecovery, CatalogHarnessError> {
    let base = CatalogGeneration::from_pack(decode_catalog_pack(catalog_pack)?);
    let replay = decode_catalog_wal(catalog_wal, limits)?;
    let committed_transactions = replay.transactions().len();
    let committed_wal_bytes = replay.committed_bytes();
    let incomplete_tail_bytes = replay.incomplete_tail_bytes();
    let generation = replay_catalog_wal(&base, &replay)?;
    validate_all_views(&generation).map_err(CatalogHarnessError::Semantic)?;
    Ok(CatalogRecovery {
        generation,
        committed_transactions,
        committed_wal_bytes,
        incomplete_tail_bytes,
    })
}
