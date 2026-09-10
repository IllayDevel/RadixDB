// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Persistence Manager for MVCC Engine
//!
//! Coordinates all disk operations including:
//! - WAL (Write-Ahead Log) management
//! - Snapshot creation and loading
//! - Recovery from disk
//!

use std::fs;
use std::path::{Path, PathBuf};

use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use radixdb_core::time_compat::{system_time_now, Instant, UNIX_EPOCH};

use crate::cpu_runtime::StorageCpuRuntime;
use crate::index::PartialIndexPredicateMetadata;
use crate::mvcc::version_store::RowVersion;
use crate::mvcc::wal_manager::{WALEntry, WALManager, WALOperationType};
#[cfg(any(test, feature = "test-hooks"))]
use crate::mvcc::wal_manager::{WalAppendTestHook, WalAppendTestHookGuard};
use crate::PersistenceConfig;
use radixdb_catalog::ObjectId;
use radixdb_core::{CompactArc, SmartString};
use radixdb_core::{
    DataType, Error, IndexType, Result, Row, Schema, SchemaConstraint, SchemaConstraintKind, Value,
};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// Default checkpoint interval (1 minute)
pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);

/// Default number of snapshots to keep
pub const DEFAULT_KEEP_SNAPSHOTS: usize = 3;

/// Legacy transaction ID used by auto-commit DDL before every DDL unit received
/// its own recovery identity. Kept reserved so newly allocated internal IDs can
/// never alias WAL produced by an older release.
pub use crate::mvcc::DDL_TXN_ID;
#[doc(hidden)]
pub struct PendingDmlWalOperation {
    pub table_id: ObjectId,
    pub row_id: i64,
    pub operation: WALOperationType,
    pub version: RowVersion,
}

impl PendingDmlWalOperation {
    fn to_wal_entry(&self, txn_id: i64) -> Result<WALEntry> {
        let data = if self.operation == WALOperationType::Delete {
            Vec::new()
        } else {
            serialize_row_version(&self.version)?
        };
        let mut entry = WALEntry::new(
            txn_id,
            Some(self.table_id),
            self.row_id,
            self.operation,
            data,
        );
        entry.timestamp = self.version.create_time;
        Ok(entry)
    }
}

mod codec;
mod manager;
mod row_codec;

pub use codec::*;
pub use manager::*;
pub use row_codec::*;

#[cfg(test)]
mod tests;
