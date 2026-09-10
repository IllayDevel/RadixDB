//! Canonical multi-version state owners.

pub mod arena;
pub mod engine;
pub mod file_lock;
pub mod persistence;
pub mod registry;
pub mod table;
pub mod transaction;
pub mod version_store;
pub mod wal_manager;

/// Source-compatible path for the timestamp owner after facade removal.
#[doc(hidden)]
pub mod timestamp {
    pub use crate::timestamp::*;
}

/// Source-compatible path for volume zone maps after facade removal.
#[doc(hidden)]
pub mod zonemap {
    pub use crate::volume::zonemap::*;
}

/// Durable transaction identity used for schema changes.
pub const DDL_TXN_ID: i64 = -2;

pub use crate::index::PkIndex;
pub use crate::timestamp::get_fast_timestamp;
pub use crate::traits::AggregateOp;
pub use arena::{ArenaReadGuard, ArenaRowMeta, RowArena};
pub use engine::{
    CatalogRuntime, CatalogRuntimeBinder, CatalogRuntimeTable, EngineCompactionCostSnapshot,
    EngineLifecycleState, EngineMaintenanceSnapshot, EngineRuntimeOperationDetail,
    EngineRuntimeOperationSnapshot, EngineRuntimeStatsV2, EngineRuntimeVisitLimits, MVCCEngine,
    ViewDefinition, ViewDependencyBinder,
};
pub use file_lock::FileLock;
pub use persistence::{
    deserialize_row_version, deserialize_value, serialize_row_version, serialize_value,
    serialize_value_into, IndexMetadata, PersistenceManager, PersistenceMeta,
    DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_KEEP_SNAPSHOTS,
};
pub use registry::{TransactionRegistry, INVALID_TRANSACTION_ID, RECOVERY_TRANSACTION_ID};
pub(crate) use table::MVCCTable;
pub use transaction::{
    CatalogWriteFenceGuard, DdlFenceGuard, MvccTransaction, SealFenceGuard,
    TransactionEngineOperations, TransactionState, TransactionalDdlPreparation,
    TransactionalDdlPublication, VisibilityFenceGuard,
};
pub use version_store::{
    clear_version_map_pools, AggregateResult, IndexDefinition, RowIndex, RowVersion,
    SealedIndexCleanup, TransactionVersionStore, VersionStore, VisibilityChecker, WriteSetEntry,
};
pub use wal_manager::{
    WALEntry, WALManager, WALOperationType, DEFAULT_WAL_BUFFER_SIZE, DEFAULT_WAL_FLUSH_TRIGGER,
    DEFAULT_WAL_MAX_SIZE,
};
