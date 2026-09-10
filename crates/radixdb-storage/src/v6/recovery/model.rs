use std::path::{Path, PathBuf};
use std::sync::Arc;

use radixdb_catalog::{CatalogGeneration as RuntimeCatalogGeneration, ObjectId};

use crate::v6::{
    CatalogGeneration, CatalogWalReplayLimits, ControlRecord, FormatResult,
    PhysicalGenerationSnapshot, ReachabilityAllowance, ReachabilityLimits, UnavailableIndex,
    WalReplayFloor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryLimits {
    reachability: ReachabilityLimits,
    catalog_wal: CatalogWalReplayLimits,
}

impl RecoveryLimits {
    pub const fn new(
        reachability: ReachabilityLimits,
        catalog_wal: CatalogWalReplayLimits,
    ) -> Self {
        Self {
            reachability,
            catalog_wal,
        }
    }

    pub const fn reachability(self) -> ReachabilityLimits {
        self.reachability
    }

    pub const fn catalog_wal(self) -> CatalogWalReplayLimits {
        self.catalog_wal
    }
}

/// Immutable inputs handed to the authoritative DML/MVCC WAL replay owner
/// after CONTROL, the physical graph and catalog WAL are fully validated.
#[derive(Debug, Clone, Copy)]
pub struct DataWalRecoveryContext<'a> {
    root: &'a Path,
    floor: WalReplayFloor,
    physical: &'a PhysicalGenerationSnapshot,
    catalog: &'a RuntimeCatalogGeneration,
    metadata_allowance: ReachabilityAllowance,
}

impl<'a> DataWalRecoveryContext<'a> {
    pub(crate) const fn new(
        root: &'a Path,
        floor: WalReplayFloor,
        physical: &'a PhysicalGenerationSnapshot,
        catalog: &'a RuntimeCatalogGeneration,
        metadata_allowance: ReachabilityAllowance,
    ) -> Self {
        Self {
            root,
            floor,
            physical,
            catalog,
            metadata_allowance,
        }
    }

    pub const fn root(self) -> &'a Path {
        self.root
    }

    pub const fn floor(self) -> WalReplayFloor {
        self.floor
    }

    pub const fn physical(self) -> &'a PhysicalGenerationSnapshot {
        self.physical
    }

    pub const fn catalog(self) -> &'a RuntimeCatalogGeneration {
        self.catalog
    }

    pub const fn metadata_allowance(self) -> ReachabilityAllowance {
        self.metadata_allowance
    }
}

/// The existing MVCC WAL owner implements this boundary during production
/// cutover. Recovery invokes it exactly once, after physical root selection;
/// a failure is fail-closed and never retries another CONTROL against a
/// partially mutated runtime state.
pub trait WalRecovery {
    type State;

    /// Return only catalog-mutation records from the exact physical WAL
    /// generation named by `floor`. This is a read-only selection probe: it
    /// may be called for a newer candidate that is subsequently rejected.
    /// The implementation must extract the substream from the same WAL
    /// authority used by `replay_data`; a second catalog WAL is forbidden.
    fn read_catalog_transactions(
        &mut self,
        floor: WalReplayFloor,
        byte_budget: u64,
    ) -> FormatResult<Vec<u8>>;

    /// Replay DML/MVCC state exactly once after the immutable root and final
    /// catalog have been selected. Failure is terminal for this open attempt.
    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>>;
}

#[derive(Debug)]
pub struct DataWalRecoveryOutcome<State> {
    state: State,
    last_lsn: u64,
    applied_transactions: u64,
    applied_entries: u64,
    skipped_entries: u64,
    runtime_table_ids: Vec<ObjectId>,
}

impl<State> DataWalRecoveryOutcome<State> {
    pub fn new(
        state: State,
        last_lsn: u64,
        applied_transactions: u64,
        applied_entries: u64,
        skipped_entries: u64,
        runtime_table_ids: Vec<ObjectId>,
    ) -> Self {
        Self {
            state,
            last_lsn,
            applied_transactions,
            applied_entries,
            skipped_entries,
            runtime_table_ids,
        }
    }

    pub(crate) fn into_parts(self) -> (State, DataWalRecoveryReport, Vec<ObjectId>) {
        (
            self.state,
            DataWalRecoveryReport {
                last_lsn: self.last_lsn,
                applied_transactions: self.applied_transactions,
                applied_entries: self.applied_entries,
                skipped_entries: self.skipped_entries,
            },
            self.runtime_table_ids,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogRecoveryReport {
    replay_floor: WalReplayFloor,
    decoded_transactions: u64,
    replayed_transactions: u64,
    committed_bytes: u64,
    incomplete_tail_bytes: u64,
    base_generation: CatalogGeneration,
    final_generation: CatalogGeneration,
}

impl CatalogRecoveryReport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        replay_floor: WalReplayFloor,
        decoded_transactions: u64,
        replayed_transactions: u64,
        committed_bytes: u64,
        incomplete_tail_bytes: u64,
        base_generation: CatalogGeneration,
        final_generation: CatalogGeneration,
    ) -> Self {
        Self {
            replay_floor,
            decoded_transactions,
            replayed_transactions,
            committed_bytes,
            incomplete_tail_bytes,
            base_generation,
            final_generation,
        }
    }

    pub const fn replay_floor(self) -> WalReplayFloor {
        self.replay_floor
    }

    pub const fn decoded_transactions(self) -> u64 {
        self.decoded_transactions
    }

    pub const fn replayed_transactions(self) -> u64 {
        self.replayed_transactions
    }

    pub const fn committed_bytes(self) -> u64 {
        self.committed_bytes
    }

    pub const fn incomplete_tail_bytes(self) -> u64 {
        self.incomplete_tail_bytes
    }

    pub const fn base_generation(self) -> CatalogGeneration {
        self.base_generation
    }

    pub const fn final_generation(self) -> CatalogGeneration {
        self.final_generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataWalRecoveryReport {
    last_lsn: u64,
    applied_transactions: u64,
    applied_entries: u64,
    skipped_entries: u64,
}

impl DataWalRecoveryReport {
    pub const fn last_lsn(self) -> u64 {
        self.last_lsn
    }

    pub const fn applied_transactions(self) -> u64 {
        self.applied_transactions
    }

    pub const fn applied_entries(self) -> u64 {
        self.applied_entries
    }

    pub const fn skipped_entries(self) -> u64 {
        self.skipped_entries
    }
}

pub struct RecoveredDatabase<State> {
    root: PathBuf,
    control: ControlRecord,
    physical: PhysicalGenerationSnapshot,
    catalog: Arc<RuntimeCatalogGeneration>,
    unavailable_indexes: Vec<UnavailableIndex>,
    catalog_report: CatalogRecoveryReport,
    data_report: DataWalRecoveryReport,
    runtime_state: State,
}

impl<State> RecoveredDatabase<State> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        root: PathBuf,
        control: ControlRecord,
        physical: PhysicalGenerationSnapshot,
        catalog: RuntimeCatalogGeneration,
        unavailable_indexes: Vec<UnavailableIndex>,
        catalog_report: CatalogRecoveryReport,
        data_report: DataWalRecoveryReport,
        runtime_state: State,
    ) -> Self {
        Self {
            root,
            control,
            physical,
            catalog: Arc::new(catalog),
            unavailable_indexes,
            catalog_report,
            data_report,
            runtime_state,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn control(&self) -> ControlRecord {
        self.control
    }

    pub const fn physical(&self) -> &PhysicalGenerationSnapshot {
        &self.physical
    }

    pub const fn catalog(&self) -> &Arc<RuntimeCatalogGeneration> {
        &self.catalog
    }

    pub fn unavailable_indexes(&self) -> &[UnavailableIndex] {
        &self.unavailable_indexes
    }

    pub const fn catalog_report(&self) -> CatalogRecoveryReport {
        self.catalog_report
    }

    pub const fn data_report(&self) -> DataWalRecoveryReport {
        self.data_report
    }

    pub fn into_runtime_state(self) -> State {
        self.runtime_state
    }
}
