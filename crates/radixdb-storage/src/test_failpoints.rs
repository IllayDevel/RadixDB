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

//! Test-only failpoint flags for injecting I/O errors.
//!
//! Each flag is owned by the test thread that armed it. This keeps parallel
//! tests from observing another test's injected failure while preserving the
//! small `store`/`load` API used at call sites. Source code checks these flags
//! only in test/failpoint builds, so production has zero cost.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

#[cfg(any(test, feature = "test-failpoints"))]
use std::{
    collections::{BTreeSet, VecDeque},
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::{Arc, Condvar},
    time::{Duration, Instant},
};

static NEXT_THREAD_TOKEN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_TOKEN: Cell<u64> =
        Cell::new(NEXT_THREAD_TOKEN.fetch_add(1, Ordering::Relaxed));
}

fn current_thread_token() -> u64 {
    THREAD_TOKEN.with(Cell::get)
}

/// Test-only failpoint whose armed state is visible only to its owner thread.
pub struct Failpoint {
    active: AtomicBool,
    owner: AtomicU64,
    hits: AtomicU64,
}

impl Failpoint {
    pub const fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            owner: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    pub fn store(&self, enabled: bool, ordering: Ordering) {
        if enabled {
            self.hits.store(0, Ordering::Relaxed);
            self.owner.store(current_thread_token(), Ordering::Relaxed);
            self.active.store(true, ordering);
        } else {
            self.active.store(false, ordering);
            self.owner.store(0, Ordering::Relaxed);
        }
    }

    pub fn load(&self, ordering: Ordering) -> bool {
        let hit = self.active.load(ordering)
            && self.owner.load(Ordering::Relaxed) == current_thread_token();
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Number of times the owner thread reached this injection point since it
    /// was last armed. Tests must assert this instead of accepting a green
    /// operation that never traversed the intended failure boundary.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

impl Default for Failpoint {
    fn default() -> Self {
        Self::new()
    }
}

/// Fail WAL `write_to_file()` with an I/O error
pub static WAL_WRITE_FAIL: Failpoint = Failpoint::new();

/// Fail WAL `sync_locked()` (fsync) with an I/O error
pub static WAL_SYNC_FAIL: Failpoint = Failpoint::new();

/// Report a test-only filesystem-capacity exhaustion at the durable WAL
/// boundary. This is the safe ENOSPC profile: it never fills a host filesystem.
pub static FILESYSTEM_FULL_FAIL: Failpoint = Failpoint::new();

/// Test-only physical execution selection. The default build cannot change
/// paths through this surface; prerelease parity tests enable it explicitly.
#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum ExecutionPathMode {
    #[default]
    Automatic = 0,
    DeclineIndexes = 1,
    ForceSerial = 2,
    ForceParallel = 3,
}

#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionPathCounters {
    pub hot_primary_key_scans: u64,
    pub hot_index_scans: u64,
    pub hot_full_scans: u64,
    pub hot_table_scans: u64,
    pub cold_table_scans: u64,
    pub mixed_table_scans: u64,
    pub serial_filters: u64,
    pub parallel_filters: u64,
    pub serial_joins: u64,
    pub parallel_joins: u64,
}

#[cfg(any(test, feature = "test-failpoints"))]
static EXECUTION_PATH_CONTROL_LOCK: Mutex<()> = Mutex::new(());

#[cfg(any(test, feature = "test-failpoints"))]
static EXECUTION_PATH_COUNTS: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];

#[cfg(any(test, feature = "test-failpoints"))]
thread_local! {
    // Path selection is made synchronously before work is dispatched to
    // Rayon. Keep the override local to the test thread so a forced path
    // cannot alter an unrelated test running in the same process. Counters
    // remain process-wide because the selected work itself may leave this
    // thread.
    static EXECUTION_PATH_MODE: Cell<ExecutionPathMode> =
        const { Cell::new(ExecutionPathMode::Automatic) };

    // Process-wide counters are required by execution tests whose work can
    // leave the calling thread.  A thread-local mirror gives synchronous unit
    // tests an uncontaminated observation when the Rust test harness executes
    // unrelated scans in parallel.
    static EXECUTION_PATH_THREAD_COUNTS: std::cell::RefCell<[u64; 10]> =
        const { std::cell::RefCell::new([0; 10]) };
}

/// Serializes one path-control and process-wide counter observation interval.
///
/// The mode override belongs only to the installing thread. The lock prevents
/// two tests that read the process-wide counters from resetting or sampling
/// the same interval concurrently.
#[cfg(any(test, feature = "test-failpoints"))]
pub struct ExecutionPathControlGuard {
    _lock: MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-failpoints"))]
impl ExecutionPathControlGuard {
    pub fn install(mode: ExecutionPathMode) -> Self {
        let lock = EXECUTION_PATH_CONTROL_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_execution_path_counters();
        EXECUTION_PATH_MODE.with(|current| current.set(mode));
        Self { _lock: lock }
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
impl Drop for ExecutionPathControlGuard {
    fn drop(&mut self) {
        EXECUTION_PATH_MODE.with(|current| current.set(ExecutionPathMode::Automatic));
        reset_execution_path_counters();
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
fn execution_path_mode() -> ExecutionPathMode {
    EXECUTION_PATH_MODE.with(Cell::get)
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn decline_indexes() -> bool {
    execution_path_mode() == ExecutionPathMode::DeclineIndexes
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn force_serial_execution() -> bool {
    execution_path_mode() == ExecutionPathMode::ForceSerial
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn force_parallel_execution() -> bool {
    execution_path_mode() == ExecutionPathMode::ForceParallel
}

#[cfg(test)]
mod execution_path_scope_tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn forced_mode_is_thread_local_while_control_intervals_remain_serial() {
        let forced = ExecutionPathControlGuard::install(ExecutionPathMode::ForceParallel);
        assert!(force_parallel_execution());
        assert!(!force_serial_execution());
        assert!(!decline_indexes());

        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let (installed_tx, installed_rx) = mpsc::sync_channel(1);
        let unrelated = std::thread::spawn(move || {
            observed_tx
                .send((
                    force_parallel_execution(),
                    force_serial_execution(),
                    decline_indexes(),
                ))
                .unwrap();
            let _serial = ExecutionPathControlGuard::install(ExecutionPathMode::ForceSerial);
            assert!(force_serial_execution());
            installed_tx.send(()).unwrap();
        });

        assert_eq!(
            observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            (false, false, false),
            "a forced path must not leak into an unrelated test thread"
        );
        assert!(
            installed_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "a second counter-control interval must wait for the first"
        );

        drop(forced);
        installed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the next control interval must proceed after release");
        unrelated.join().unwrap();
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn record_execution_path(index: usize) {
    EXECUTION_PATH_COUNTS[index].fetch_add(1, Ordering::Relaxed);
    EXECUTION_PATH_THREAD_COUNTS.with(|counts| {
        counts.borrow_mut()[index] += 1;
    });
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn reset_execution_path_counters() {
    for counter in &EXECUTION_PATH_COUNTS {
        counter.store(0, Ordering::Relaxed);
    }
    EXECUTION_PATH_THREAD_COUNTS.with(|counts| counts.replace([0; 10]));
}

#[cfg(any(test, feature = "test-failpoints"))]
pub fn execution_path_counters() -> ExecutionPathCounters {
    let read = |index: usize| EXECUTION_PATH_COUNTS[index].load(Ordering::Relaxed);
    execution_path_counters_from(read)
}

/// Returns only paths selected synchronously on the calling thread.
///
/// This is intentionally separate from [`execution_path_counters`]: the
/// process-wide view remains authoritative for tests that dispatch through
/// Rayon, while local table-selection tests are isolated from unrelated test
/// threads.
#[cfg(any(test, feature = "test-failpoints"))]
pub fn current_thread_execution_path_counters() -> ExecutionPathCounters {
    EXECUTION_PATH_THREAD_COUNTS.with(|counts| {
        let counts = counts.borrow();
        execution_path_counters_from(|index| counts[index])
    })
}

#[cfg(any(test, feature = "test-failpoints"))]
fn execution_path_counters_from(read: impl Fn(usize) -> u64) -> ExecutionPathCounters {
    ExecutionPathCounters {
        hot_primary_key_scans: read(0),
        hot_index_scans: read(1),
        hot_full_scans: read(2),
        hot_table_scans: read(3),
        cold_table_scans: read(4),
        mixed_table_scans: read(5),
        serial_filters: read(6),
        parallel_filters: read(7),
        serial_joins: read(8),
        parallel_joins: read(9),
    }
}

/// Fail checkpoint metadata write
pub static CHECKPOINT_WRITE_FAIL: Failpoint = Failpoint::new();

/// Fail table manifest publication before the atomic manifest write.
pub static MANIFEST_WRITE_FAIL: Failpoint = Failpoint::new();

/// Stop checkpoint after the database generation is durable but before WAL
/// retention completion. Recovery must accept the published generation and
/// replay the still-present WAL suffix.
pub static CHECKPOINT_AFTER_GENERATION_FAIL: Failpoint = Failpoint::new();

/// Fail the parent-directory durability barrier after artifact-backed volume rename.
pub static VOLUME_PUBLISH_SYNC_FAIL: Failpoint = Failpoint::new();

/// Fail table storage rename after the directory move, before publication.
pub static TABLE_RENAME_PREPARE_FAIL: Failpoint = Failpoint::new();

/// Fail the compensating table-directory rename used by rename rollback.
pub static TABLE_RENAME_ROLLBACK_FAIL: Failpoint = Failpoint::new();

/// Fail restore after the old durable directories have moved to backup.
pub static RESTORE_FAIL_AFTER_OLD_MOVE: Failpoint = Failpoint::new();

/// Fail restore after staged durable directories have moved into live paths.
pub static RESTORE_FAIL_AFTER_NEW_MOVE: Failpoint = Failpoint::new();

/// Serializes tests that intentionally mutate the shared failpoint registry.
/// Thread ownership additionally protects unrelated parallel tests.
static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

/// Reset all failpoints to disabled state
pub fn reset_all() {
    use std::sync::atomic::Ordering::Release;
    WAL_WRITE_FAIL.store(false, Release);
    WAL_SYNC_FAIL.store(false, Release);
    FILESYSTEM_FULL_FAIL.store(false, Release);
    CHECKPOINT_WRITE_FAIL.store(false, Release);
    MANIFEST_WRITE_FAIL.store(false, Release);
    CHECKPOINT_AFTER_GENERATION_FAIL.store(false, Release);
    VOLUME_PUBLISH_SYNC_FAIL.store(false, Release);
    TABLE_RENAME_PREPARE_FAIL.store(false, Release);
    TABLE_RENAME_ROLLBACK_FAIL.store(false, Release);
    RESTORE_FAIL_AFTER_OLD_MOVE.store(false, Release);
    RESTORE_FAIL_AFTER_NEW_MOVE.store(false, Release);
}

/// RAII guard that serializes failpoint tests and resets all failpoints on drop.
/// Acquires FAILPOINT_LOCK so only one test runs at a time, and ensures
/// cleanup even if a test panics after arming a failpoint.
pub struct FailpointGuard {
    _lock: MutexGuard<'static, ()>,
}

impl FailpointGuard {
    pub fn new() -> Self {
        // If a previous test panicked while holding the lock, the Mutex is
        // poisoned. Recover by accepting the poisoned guard; reset_all()
        // below will clean up the stale flags.
        let lock = FAILPOINT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_all();
        FailpointGuard { _lock: lock }
    }
}

impl Default for FailpointGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        reset_all();
    }
}

/// Named production boundaries exposed only to deterministic race tests.
///
/// Unlike error failpoints, an interleave point does not change an outcome. It
/// pauses the thread after recording the exact boundary until the test
/// scheduler releases that one arrival. Production call sites are compiled
/// out unless `test-failpoints` is enabled.
#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InterleavePoint {
    WalBeforeCommitMarker,
    WalCommitMarkerDurable,
    ConstraintsValidated,
    IndexPrepared,
    IndexPublished,
    VisibilityBeforePublish,
    VisibilityDmlPublished,
    VisibilityPublished,
    CommittedStorageBeforePublish,
    SealBeforePublish,
    CompactionBeforeOutput,
    CompactionBeforePublish,
    CompactionTombstonesPrepared,
    GenerationPinLoaded,
    CheckpointBeforePublish,
    ManifestBeforePublish,
    CatalogBeforePublish,
    CatalogPublished,
    CommitAckReady,
    ActiveTransactionBegan,
    CursorPublished,
}

#[cfg(any(test, feature = "test-failpoints"))]
impl InterleavePoint {
    /// Stable spelling used by cross-process crash barriers. Keep these names
    /// independent of Rust's debug formatting so recorded prerelease traces
    /// remain replayable after harmless enum refactors.
    pub const fn crash_name(self) -> &'static str {
        match self {
            Self::WalBeforeCommitMarker => "wal_before_commit_marker",
            Self::WalCommitMarkerDurable => "wal_commit_marker_durable",
            Self::ConstraintsValidated => "constraints_validated",
            Self::IndexPrepared => "index_prepared",
            Self::IndexPublished => "index_published",
            Self::VisibilityBeforePublish => "visibility_before_publish",
            Self::VisibilityDmlPublished => "visibility_dml_published",
            Self::VisibilityPublished => "visibility_published",
            Self::CommittedStorageBeforePublish => "committed_storage_before_publish",
            Self::SealBeforePublish => "seal_before_publish",
            Self::CompactionBeforeOutput => "compaction_before_output",
            Self::CompactionBeforePublish => "compaction_before_publish",
            Self::CompactionTombstonesPrepared => "compaction_tombstones_prepared",
            Self::GenerationPinLoaded => "generation_pin_loaded",
            Self::CheckpointBeforePublish => "checkpoint_before_publish",
            Self::ManifestBeforePublish => "manifest_before_publish",
            Self::CatalogBeforePublish => "catalog_before_publish",
            Self::CatalogPublished => "catalog_published",
            Self::CommitAckReady => "commit_ack_ready",
            Self::ActiveTransactionBegan => "active_transaction_began",
            Self::CursorPublished => "cursor_published",
        }
    }
}

/// One paused boundary. `token` identifies one arrival even when two
/// transactions reach the same point concurrently; `subject` is normally the
/// transaction id and is zero for global storage lifecycle operations.
#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterleaveArrival {
    /// Process-local runtime owner. Scoped controllers ignore boundaries from
    /// unrelated engines running in parallel in the same test binary.
    pub scope: Option<u64>,
    pub point: InterleavePoint,
    pub subject: i64,
    token: u64,
}

#[cfg(any(test, feature = "test-failpoints"))]
#[derive(Default)]
struct InterleaveState {
    enabled: BTreeSet<InterleavePoint>,
    arrivals: VecDeque<InterleaveArrival>,
    released: BTreeSet<u64>,
    next_token: u64,
    closed: bool,
}

/// Deterministic scheduler endpoint shared by production threads and a test.
#[cfg(any(test, feature = "test-failpoints"))]
pub struct InterleaveController {
    scope: Option<u64>,
    state: Mutex<InterleaveState>,
    arrived: Condvar,
    released: Condvar,
}

#[cfg(any(test, feature = "test-failpoints"))]
impl InterleaveController {
    fn new(scope: Option<u64>, points: impl IntoIterator<Item = InterleavePoint>) -> Self {
        Self {
            scope,
            state: Mutex::new(InterleaveState {
                enabled: points.into_iter().collect(),
                next_token: 1,
                ..InterleaveState::default()
            }),
            arrived: Condvar::new(),
            released: Condvar::new(),
        }
    }

    fn hit(&self, scope: Option<u64>, point: InterleavePoint, subject: i64) {
        if self.scope.is_some() && self.scope != scope {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || !state.enabled.contains(&point) {
            return;
        }
        let token = state.next_token;
        state.next_token = state.next_token.saturating_add(1);
        state.arrivals.push_back(InterleaveArrival {
            scope,
            point,
            subject,
            token,
        });
        self.arrived.notify_all();
        while !state.closed && !state.released.remove(&token) {
            state = self
                .released
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Wait for the next matching boundary. Other arrivals remain queued so a
    /// schedule may deliberately release transactions in a different order.
    pub fn wait_for(
        &self,
        point: InterleavePoint,
        subject: Option<i64>,
        timeout: Duration,
    ) -> Result<InterleaveArrival, String> {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(position) = state.arrivals.iter().position(|arrival| {
                arrival.point == point && subject.is_none_or(|value| arrival.subject == value)
            }) {
                return Ok(state
                    .arrivals
                    .remove(position)
                    .expect("interleave arrival position remains valid"));
            }
            if state.closed {
                return Err(format!(
                    "interleave controller closed before {point:?} subject {subject:?} arrived"
                ));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "timed out waiting for {point:?} subject {subject:?}; queued={:?}",
                    state.arrivals
                ));
            }
            let (next, wait) = self
                .arrived
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if wait.timed_out() && Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {point:?} subject {subject:?}; queued={:?}",
                    state.arrivals
                ));
            }
        }
    }

    /// Release exactly one arrival previously returned by `wait_for`.
    pub fn release(&self, arrival: InterleaveArrival) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.closed {
            state.released.insert(arrival.token);
            self.released.notify_all();
        }
    }

    /// Stop pausing future arrivals at one boundary. Arrivals already returned
    /// by `wait_for` remain paused until explicitly released, which lets a test
    /// isolate one exact window while the same owner continues other work.
    pub fn disable(&self, point: InterleavePoint) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.enabled.remove(&point);
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        self.arrived.notify_all();
        self.released.notify_all();
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
static INTERLEAVE_OWNER: Mutex<()> = Mutex::new(());
#[cfg(any(test, feature = "test-failpoints"))]
static INTERLEAVE_CONTROLLER: Mutex<Option<Arc<InterleaveController>>> = Mutex::new(None);

#[cfg(any(test, feature = "test-failpoints"))]
static CRASH_BARRIER_HIT: AtomicBool = AtomicBool::new(false);

#[cfg(any(test, feature = "test-failpoints"))]
fn crash_barrier(point: InterleavePoint, subject: i64) {
    const POINT_ENV: &str = "RADIXDB_PRERELEASE_CRASH_POINT";
    const READY_ENV: &str = "RADIXDB_PRERELEASE_CRASH_READY";

    let Ok(requested) = std::env::var(POINT_ENV) else {
        return;
    };
    if requested != point.crash_name()
        || CRASH_BARRIER_HIT
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return;
    }

    let ready = std::env::var_os(READY_ENV)
        .unwrap_or_else(|| panic!("{READY_ENV} is required when {POINT_ENV} is set"));
    let ready = Path::new(&ready);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(ready)
        .unwrap_or_else(|error| {
            panic!(
                "failed to publish crash barrier '{}': {error}",
                ready.display()
            )
        });
    writeln!(file, "{} {subject}", point.crash_name())
        .expect("write prerelease crash-barrier evidence");
    file.sync_all()
        .expect("sync prerelease crash-barrier evidence");
    while CRASH_BARRIER_HIT.load(Ordering::Acquire) {
        std::thread::park_timeout(Duration::from_secs(60));
    }
}

/// RAII installation guard. Only one deterministic schedule may own the
/// process-wide boundary registry; dropping the guard releases every paused
/// production thread, including during panic unwinding.
#[cfg(any(test, feature = "test-failpoints"))]
pub struct InterleaveGuard {
    controller: Arc<InterleaveController>,
    _owner: MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-failpoints"))]
impl InterleaveGuard {
    pub fn install(points: impl IntoIterator<Item = InterleavePoint>) -> Self {
        Self::install_internal(None, points)
    }

    /// Install a controller for one process-local engine/runtime owner. Calls
    /// from every other owner pass through without pausing, so deterministic
    /// race tests remain safe under the default parallel Rust test harness.
    pub fn install_scoped(scope: u64, points: impl IntoIterator<Item = InterleavePoint>) -> Self {
        assert_ne!(
            scope, 0,
            "interleave scope zero is reserved for unscoped callers"
        );
        Self::install_internal(Some(scope), points)
    }

    fn install_internal(
        scope: Option<u64>,
        points: impl IntoIterator<Item = InterleavePoint>,
    ) -> Self {
        let owner = INTERLEAVE_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let controller = Arc::new(InterleaveController::new(scope, points));
        *INTERLEAVE_CONTROLLER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&controller));
        Self {
            controller,
            _owner: owner,
        }
    }

    pub fn controller(&self) -> Arc<InterleaveController> {
        Arc::clone(&self.controller)
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
impl Drop for InterleaveGuard {
    fn drop(&mut self) {
        *INTERLEAVE_CONTROLLER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.controller.close();
    }
}

/// Pause at a named deterministic-race boundary when a test installed it.
#[cfg(any(test, feature = "test-failpoints"))]
pub fn interleave(point: InterleavePoint, subject: i64) {
    interleave_in_scope(None, point, subject);
}

/// Pause at a deterministic boundary only when the installed controller owns
/// this exact process-local runtime scope.
#[cfg(any(test, feature = "test-failpoints"))]
pub fn interleave_scoped(scope: u64, point: InterleavePoint, subject: i64) {
    interleave_in_scope(Some(scope), point, subject);
}

#[cfg(any(test, feature = "test-failpoints"))]
fn interleave_in_scope(scope: Option<u64>, point: InterleavePoint, subject: i64) {
    crash_barrier(point, subject);
    let controller = INTERLEAVE_CONTROLLER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(controller) = controller {
        controller.hit(scope, point, subject);
    }
}

#[cfg(test)]
mod interleave_scope_tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn scoped_controller_pauses_only_its_runtime_owner() {
        const OWNER_SCOPE: u64 = 41;
        const UNRELATED_SCOPE: u64 = 42;
        let guard =
            InterleaveGuard::install_scoped(OWNER_SCOPE, [InterleavePoint::CompactionBeforeOutput]);
        let controller = guard.controller();

        let (unrelated_done_tx, unrelated_done_rx) = mpsc::sync_channel(1);
        let unrelated = std::thread::spawn(move || {
            interleave_scoped(UNRELATED_SCOPE, InterleavePoint::CompactionBeforeOutput, 7);
            unrelated_done_tx.send(()).unwrap();
        });
        unrelated_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("unrelated runtime must not pause on another owner's controller");
        unrelated.join().unwrap();

        let (owned_done_tx, owned_done_rx) = mpsc::sync_channel(1);
        let owned = std::thread::spawn(move || {
            interleave_scoped(OWNER_SCOPE, InterleavePoint::CompactionBeforeOutput, 9);
            owned_done_tx.send(()).unwrap();
        });
        let arrival = controller
            .wait_for(
                InterleavePoint::CompactionBeforeOutput,
                Some(9),
                Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(arrival.scope, Some(OWNER_SCOPE));
        assert!(owned_done_rx.try_recv().is_err());
        controller.release(arrival);
        owned_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("owned runtime must resume after exact arrival release");
        owned.join().unwrap();
    }
}
