//! Bounded, generation-aware operating-system page-cache warmup.
//!
//! This is deliberately not an engine-owned database cache. The worker reads
//! immutable files through one small reusable buffer and lets the operating
//! system own residency and eviction.

use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fs::{File, Metadata};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::Serialize;

use super::config::MAX_PAGE_CACHE_LEVEL;
use super::v6::{
    catalog_path, database_manifest_path, table_manifest_path, ArtifactKind,
    PhysicalGenerationLease, PhysicalGenerationSnapshot,
};

const READ_BUFFER_BYTES: usize = 1024 * 1024;
const PROGRESS_UPDATE_BYTES: u64 = 16 * 1024 * 1024;
const AUTO_RESERVE_MIN_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PageCacheWarmupSnapshot {
    pub state: String,
    pub requested_level: u8,
    pub generation_fingerprint: u64,
    pub total_generation_bytes: u64,
    pub available_memory_bytes: u64,
    pub memory_reserve_bytes: u64,
    pub safe_budget_bytes: u64,
    pub target_bytes: u64,
    pub warmed_bytes: u64,
    pub resident_estimate_bytes: u64,
    pub duration_millis: u64,
    pub read_bytes_per_second: u64,
    pub limited_by: String,
    pub last_error: String,
}

impl PageCacheWarmupSnapshot {
    fn new(level: u8) -> Self {
        Self {
            state: if level == 0 { "disabled" } else { "idle" }.to_string(),
            requested_level: level,
            generation_fingerprint: 0,
            total_generation_bytes: 0,
            available_memory_bytes: 0,
            memory_reserve_bytes: 0,
            safe_budget_bytes: 0,
            target_bytes: 0,
            warmed_bytes: 0,
            resident_estimate_bytes: 0,
            duration_millis: 0,
            read_bytes_per_second: 0,
            limited_by: if level == 0 {
                "disabled".to_string()
            } else {
                "none".to_string()
            },
            last_error: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WarmupPolicy {
    level: u8,
    max_bytes: u64,
    memory_reserve: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum WarmupFileKind {
    Metadata,
    Index,
    Data,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified_nanos: u128,
}

#[derive(Clone, Debug)]
struct WarmupFile {
    path: PathBuf,
    kind: WarmupFileKind,
    stamp: FileStamp,
    access_priority: u64,
}

#[derive(Clone, Copy, Debug)]
struct WarmedFile {
    stamp: FileStamp,
    bytes: u64,
}

#[derive(Debug)]
struct WarmupInventory {
    files: Vec<WarmupFile>,
    fingerprint: u64,
    total_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct TargetBudget {
    available: u64,
    reserve: u64,
    safe: u64,
    target: u64,
    limited_by: &'static str,
}

#[derive(Debug)]
struct WarmupRuntime {
    snapshot: PageCacheWarmupSnapshot,
    warmed: HashMap<PathBuf, WarmedFile>,
    requested_generation: Option<PhysicalGenerationLease>,
    handled_epoch: u64,
}

#[derive(Debug)]
struct WarmupShared {
    stop: AtomicBool,
    request_epoch: AtomicU64,
    runtime: Mutex<WarmupRuntime>,
    wake: Condvar,
}

/// Database-local owner of one bounded page-cache worker.
#[derive(Debug)]
pub(crate) struct PageCacheWarmupController {
    database_root: PathBuf,
    policy: WarmupPolicy,
    volume_priorities: Mutex<HashMap<PathBuf, u64>>,
    shared: Arc<WarmupShared>,
}

impl PageCacheWarmupController {
    pub(crate) fn new(
        database_path: &Path,
        level: u8,
        max_bytes: u64,
        memory_reserve: u64,
    ) -> Arc<Self> {
        let level = level.min(MAX_PAGE_CACHE_LEVEL);
        Arc::new(Self {
            database_root: database_path.to_path_buf(),
            policy: WarmupPolicy {
                level,
                max_bytes,
                memory_reserve,
            },
            volume_priorities: Mutex::new(HashMap::new()),
            shared: Arc::new(WarmupShared {
                stop: AtomicBool::new(false),
                request_epoch: AtomicU64::new(0),
                runtime: Mutex::new(WarmupRuntime {
                    snapshot: PageCacheWarmupSnapshot::new(level),
                    warmed: HashMap::new(),
                    requested_generation: None,
                    handled_epoch: 0,
                }),
                wake: Condvar::new(),
            }),
        })
    }

    pub(crate) fn enabled(&self) -> bool {
        self.policy.level > 0
    }

    pub(crate) fn start(self: &Arc<Self>) -> Option<PageCacheWarmupHandle> {
        if !self.enabled() {
            return None;
        }
        let controller = Arc::clone(self);
        let thread = match std::thread::Builder::new()
            .name("radixdb-page-cache".to_string())
            .spawn(move || controller.worker_loop())
        {
            Ok(thread) => thread,
            Err(error) => {
                let mut runtime = self.shared.runtime.lock().unwrap();
                runtime.snapshot.state = "failed".to_string();
                runtime.snapshot.last_error = format!("cannot start page-cache worker: {error}");
                runtime.handled_epoch = self.shared.request_epoch.load(Ordering::Acquire);
                self.shared.wake.notify_all();
                return None;
            }
        };
        Some(PageCacheWarmupHandle {
            controller: Arc::clone(self),
            thread: Some(thread),
        })
    }

    /// Reconcile warmup against the newest durably published generation.
    pub(crate) fn request(&self, generation: PhysicalGenerationLease) {
        if !self.enabled() {
            return;
        }
        self.shared.runtime.lock().unwrap().requested_generation = Some(generation);
        self.shared.request_epoch.fetch_add(1, Ordering::AcqRel);
        self.shared.wake.notify_all();
    }

    pub(crate) fn reject_request(&self, error: impl Into<String>) {
        if !self.enabled() {
            return;
        }
        let epoch = self.shared.request_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut runtime = self.shared.runtime.lock().unwrap();
        runtime.snapshot.state = "failed".to_string();
        runtime.snapshot.last_error = error.into();
        runtime.handled_epoch = epoch;
        self.shared.wake.notify_all();
    }

    pub(crate) fn set_volume_priorities(&self, priorities: Vec<(PathBuf, u64)>) {
        if !self.enabled() {
            return;
        }
        let mut resolved: HashMap<PathBuf, u64> = HashMap::with_capacity(priorities.len());
        for (relative, priority) in priorities {
            if let Ok(path) = resolve_generation_path(&self.database_root, &relative) {
                resolved
                    .entry(path)
                    .and_modify(|current| *current = (*current).max(priority))
                    .or_insert(priority);
            }
        }
        *self.volume_priorities.lock().unwrap() = resolved;
    }

    pub(crate) fn snapshot(&self) -> Option<PageCacheWarmupSnapshot> {
        self.shared
            .runtime
            .try_lock()
            .ok()
            .map(|runtime| runtime.snapshot.clone())
    }

    pub(crate) fn wait_until_idle(&self, timeout: Duration) -> bool {
        if !self.enabled() {
            return true;
        }
        let deadline = Instant::now() + timeout;
        let mut runtime = self.shared.runtime.lock().unwrap();
        loop {
            let requested = self.shared.request_epoch.load(Ordering::Acquire);
            if runtime.handled_epoch >= requested && runtime.snapshot.state != "running" {
                return runtime.snapshot.state == "complete";
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, result) = self
                .shared
                .wake
                .wait_timeout(runtime, deadline - now)
                .unwrap();
            runtime = next;
            if result.timed_out() {
                return false;
            }
        }
    }

    fn worker_loop(&self) {
        loop {
            let (epoch, generation) = {
                let mut runtime = self.shared.runtime.lock().unwrap();
                while !self.shared.stop.load(Ordering::Acquire)
                    && runtime.handled_epoch >= self.shared.request_epoch.load(Ordering::Acquire)
                {
                    runtime = self.shared.wake.wait(runtime).unwrap();
                }
                if self.shared.stop.load(Ordering::Acquire) {
                    runtime.snapshot.state = "stopped".to_string();
                    self.shared.wake.notify_all();
                    return;
                }
                let epoch = self.shared.request_epoch.load(Ordering::Acquire);
                let Some(generation) = runtime.requested_generation.clone() else {
                    runtime.snapshot.state = "failed".to_string();
                    runtime.snapshot.last_error =
                        "page-cache request has no pinned physical generation".to_string();
                    runtime.handled_epoch = epoch;
                    self.shared.wake.notify_all();
                    continue;
                };
                runtime.snapshot.state = "running".to_string();
                runtime.snapshot.last_error.clear();
                (epoch, generation)
            };

            let started = Instant::now();
            let outcome = self.run_once(generation.snapshot(), started);
            let mut runtime = self.shared.runtime.lock().unwrap();
            runtime.handled_epoch = epoch;
            runtime.snapshot.duration_millis = duration_millis(started.elapsed());
            match outcome {
                Ok(()) => runtime.snapshot.state = "complete".to_string(),
                Err(error) if self.shared.stop.load(Ordering::Acquire) => {
                    runtime.snapshot.state = "stopped".to_string();
                    runtime.snapshot.last_error = error;
                }
                Err(error) => {
                    runtime.snapshot.state = "failed".to_string();
                    runtime.snapshot.last_error = error;
                }
            }
            self.shared.wake.notify_all();
        }
    }

    fn run_once(
        &self,
        generation: &PhysicalGenerationSnapshot,
        started: Instant,
    ) -> Result<(), String> {
        let mut inventory = collect_current_generation_files(&self.database_root, generation)?;
        let priorities = self.volume_priorities.lock().unwrap().clone();
        for file in &mut inventory.files {
            if file.kind == WarmupFileKind::Data {
                file.access_priority = priorities.get(&file.path).copied().unwrap_or(0);
            }
        }
        sort_warmup_files(&mut inventory.files);
        let available = effective_available_memory_bytes();
        let budget = calculate_target(inventory.total_bytes, self.policy, available);

        {
            let mut runtime = self.shared.runtime.lock().unwrap();
            runtime.snapshot.generation_fingerprint = inventory.fingerprint;
            runtime.snapshot.total_generation_bytes = inventory.total_bytes;
            runtime.snapshot.available_memory_bytes = budget.available;
            runtime.snapshot.memory_reserve_bytes = budget.reserve;
            runtime.snapshot.safe_budget_bytes = budget.safe;
            runtime.snapshot.target_bytes = budget.target;
            runtime.snapshot.limited_by = budget.limited_by.to_string();
            runtime.snapshot.warmed_bytes = 0;
            runtime.snapshot.resident_estimate_bytes = 0;
            runtime.snapshot.read_bytes_per_second = 0;
        }

        let desired = desired_ranges(&inventory.files, budget.target);
        let mut warmed = {
            let mut runtime = self.shared.runtime.lock().unwrap();
            std::mem::take(&mut runtime.warmed)
        };
        warmed.retain(|path, prior| {
            desired
                .iter()
                .any(|(file, bytes)| file.path == *path && file.stamp == prior.stamp && *bytes > 0)
        });

        let mut warmed_total = 0_u64;
        let mut newly_read = 0_u64;
        for (file, desired_bytes) in &desired {
            if self.shared.stop.load(Ordering::Acquire) {
                return Err("page-cache warmup cancelled".to_string());
            }
            let prior = warmed
                .get(&file.path)
                .filter(|prior| prior.stamp == file.stamp)
                .map_or(0, |prior| prior.bytes.min(*desired_bytes));
            let read = warm_file_range(
                &file.path,
                prior,
                desired_bytes.saturating_sub(prior),
                &self.shared.stop,
                |delta| {
                    newly_read = newly_read.saturating_add(delta);
                    if newly_read % PROGRESS_UPDATE_BYTES < delta {
                        self.update_progress(
                            warmed_total
                                .saturating_add(prior)
                                .saturating_add(newly_read),
                            newly_read,
                            started.elapsed(),
                        );
                    }
                },
            )
            .map_err(|error| format!("warmup read '{}': {error}", file.path.display()))?;
            let bytes = prior.saturating_add(read).min(*desired_bytes);
            warmed.insert(
                file.path.clone(),
                WarmedFile {
                    stamp: file.stamp,
                    bytes,
                },
            );
            warmed_total = warmed_total.saturating_add(bytes);
        }

        let resident_estimate = desired.iter().fold(0_u64, |total, (file, bytes)| {
            total.saturating_add(estimate_resident_bytes(&file.path, *bytes).unwrap_or(0))
        });
        let elapsed = started.elapsed();
        let mut runtime = self.shared.runtime.lock().unwrap();
        runtime.warmed = warmed;
        runtime.snapshot.warmed_bytes = warmed_total;
        runtime.snapshot.resident_estimate_bytes = resident_estimate;
        runtime.snapshot.read_bytes_per_second = bytes_per_second(newly_read, elapsed);
        Ok(())
    }

    fn update_progress(&self, warmed: u64, newly_read: u64, elapsed: Duration) {
        let mut runtime = self.shared.runtime.lock().unwrap();
        runtime.snapshot.warmed_bytes = warmed.min(runtime.snapshot.target_bytes);
        runtime.snapshot.duration_millis = duration_millis(elapsed);
        runtime.snapshot.read_bytes_per_second = bytes_per_second(newly_read, elapsed);
    }
}

pub(crate) struct PageCacheWarmupHandle {
    controller: Arc<PageCacheWarmupController>,
    thread: Option<JoinHandle<()>>,
}

impl PageCacheWarmupHandle {
    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.controller.shared.stop.store(true, Ordering::Release);
        self.controller.shared.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|payload| {
                payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("unknown panic payload")
                    .to_string()
            })?;
        }
        Ok(())
    }
}

impl Drop for PageCacheWarmupHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn collect_current_generation_files(
    database_root: &Path,
    generation: &PhysicalGenerationSnapshot,
) -> Result<WarmupInventory, String> {
    let control_name = match generation.control().slot() {
        super::v6::ControlSlotIndex::Zero => "CONTROL.0",
        super::v6::ControlSlotIndex::One => "CONTROL.1",
    };
    let mut files = vec![warmup_file(
        database_root.join(control_name),
        WarmupFileKind::Metadata,
    )?];
    files.push(warmup_file(
        resolve_generation_path(
            database_root,
            &database_manifest_path(generation.control().database_manifest()),
        )?,
        WarmupFileKind::Metadata,
    )?);
    files.push(warmup_file(
        resolve_generation_path(
            database_root,
            &catalog_path(generation.database_manifest().catalog()),
        )?,
        WarmupFileKind::Metadata,
    )?);
    for reference in generation.database_manifest().tables() {
        files.push(warmup_file(
            resolve_generation_path(database_root, &table_manifest_path(*reference))?,
            WarmupFileKind::Metadata,
        )?);
    }
    for reference in generation.artifact_references() {
        let kind = match reference.kind() {
            ArtifactKind::Data => WarmupFileKind::Data,
            ArtifactKind::Index => WarmupFileKind::Index,
        };
        files.push(warmup_file(
            resolve_generation_path(database_root, &reference.relative_path())?,
            kind,
        )?);
    }

    sort_warmup_files(&mut files);
    files.dedup_by(|left, right| left.path == right.path);
    let total_bytes = files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.stamp.len));
    let mut hasher = DefaultHasher::new();
    for file in &files {
        file.path.hash(&mut hasher);
        file.stamp.len.hash(&mut hasher);
        file.stamp.modified_nanos.hash(&mut hasher);
    }
    Ok(WarmupInventory {
        files,
        fingerprint: hasher.finish(),
        total_bytes,
    })
}

fn resolve_generation_path(root: &Path, relative: &Path) -> Result<PathBuf, String> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "page-cache generation contains unsafe path '{}'",
            relative.display()
        ));
    }
    Ok(root.join(relative))
}

fn warmup_file(path: PathBuf, kind: WarmupFileKind) -> Result<WarmupFile, String> {
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|error| format!("inspect '{}': {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("warmup member '{}' is not a file", path.display()));
    }
    Ok(WarmupFile {
        path,
        kind,
        stamp: file_stamp(&metadata),
        access_priority: 0,
    })
}

fn sort_warmup_files(files: &mut [WarmupFile]) {
    files.sort_by(|left, right| {
        left.kind.cmp(&right.kind).then_with(|| {
            if left.kind == WarmupFileKind::Data {
                right
                    .access_priority
                    .cmp(&left.access_priority)
                    .then_with(|| right.stamp.modified_nanos.cmp(&left.stamp.modified_nanos))
                    .then_with(|| right.path.cmp(&left.path))
            } else {
                left.path.cmp(&right.path)
            }
        })
    });
}

fn file_stamp(metadata: &Metadata) -> FileStamp {
    FileStamp {
        len: metadata.len(),
        modified_nanos: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_nanos()),
    }
}

fn desired_ranges(files: &[WarmupFile], target: u64) -> Vec<(WarmupFile, u64)> {
    let mut remaining = target;
    files
        .iter()
        .filter_map(|file| {
            if remaining == 0 {
                return None;
            }
            let bytes = file.stamp.len.min(remaining);
            remaining -= bytes;
            Some((file.clone(), bytes))
        })
        .collect()
}

fn calculate_target(total: u64, policy: WarmupPolicy, available: Option<u64>) -> TargetBudget {
    let available = available.unwrap_or(0);
    let reserve = if policy.memory_reserve > 0 {
        policy.memory_reserve
    } else {
        AUTO_RESERVE_MIN_BYTES.max(available / 10)
    };
    let memory_safe = available.saturating_sub(reserve);
    let safe = if policy.max_bytes > 0 {
        memory_safe.min(policy.max_bytes)
    } else {
        memory_safe
    };
    let requested = total
        .saturating_mul(u64::from(policy.level))
        .saturating_add(u64::from(MAX_PAGE_CACHE_LEVEL - 1))
        / u64::from(MAX_PAGE_CACHE_LEVEL);
    let target = requested.min(safe);
    let limited_by = if policy.level == 0 {
        "disabled"
    } else if available == 0 {
        "memory_evidence_unavailable"
    } else if target < requested && policy.max_bytes > 0 && policy.max_bytes <= memory_safe {
        "page_cache_max_bytes"
    } else if target < requested {
        "available_memory"
    } else if policy.level < MAX_PAGE_CACHE_LEVEL {
        "page_cache_level"
    } else {
        "none"
    };
    TargetBudget {
        available,
        reserve,
        safe,
        target,
        limited_by,
    }
}

fn effective_available_memory_bytes() -> Option<u64> {
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let value = line.strip_prefix("MemAvailable:")?;
                value
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|kilobytes| kilobytes.saturating_mul(1024))
            })
        });
    let cgroup = cgroup_v2_available_memory_bytes();
    match (host, cgroup) {
        (Some(host), Some(cgroup)) => Some(host.min(cgroup)),
        (Some(host), None) => Some(host),
        (None, Some(cgroup)) => Some(cgroup),
        (None, None) => None,
    }
}

fn cgroup_v2_available_memory_bytes() -> Option<u64> {
    cgroup_v2_available_memory_bytes_from(
        Path::new("/proc/self/cgroup"),
        Path::new("/sys/fs/cgroup"),
    )
}

fn cgroup_v2_available_memory_bytes_from(proc_cgroup: &Path, mount: &Path) -> Option<u64> {
    let membership = std::fs::read_to_string(proc_cgroup).ok()?;
    let relative = membership.lines().find_map(|line| {
        let (hierarchy, path) = line.split_once("::")?;
        (hierarchy == "0").then_some(path.trim_start_matches('/'))
    })?;
    let relative = Path::new(relative);
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
        && !relative.as_os_str().is_empty()
    {
        return None;
    }
    let directory = mount.join(relative);
    let current = read_cgroup_u64(&directory.join("memory.current"))?;
    let limit = ["memory.max", "memory.high"]
        .into_iter()
        .filter_map(|name| read_cgroup_u64(&directory.join(name)))
        .min()?;
    Some(limit.saturating_sub(current))
}

fn read_cgroup_u64(path: &Path) -> Option<u64> {
    let value = std::fs::read_to_string(path).ok()?;
    let value = value.trim();
    (value != "max")
        .then(|| value.parse::<u64>().ok())
        .flatten()
}

fn warm_file_range<F>(
    path: &Path,
    offset: u64,
    len: u64,
    stop: &AtomicBool,
    mut progress: F,
) -> io::Result<u64>
where
    F: FnMut(u64),
{
    if len == 0 {
        return Ok(0);
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    // The hint is an optimization only. Filesystems which reject it still
    // receive the same bounded sequential reads below.
    let _ = advise_will_need(&file, offset, len);
    let buffer_len = usize::try_from(len.min(READ_BUFFER_BYTES as u64)).unwrap();
    let mut buffer = vec![0_u8; buffer_len];
    let mut read_total = 0_u64;
    while read_total < len {
        if stop.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "warmup stopped"));
        }
        let wanted = usize::try_from((len - read_total).min(buffer.len() as u64)).unwrap();
        let read = file.read(&mut buffer[..wanted])?;
        if read == 0 {
            break;
        }
        read_total = read_total.saturating_add(read as u64);
        progress(read as u64);
    }
    Ok(read_total)
}

fn advise_will_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "warmup offset too large"))?;
    let len = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "warmup length too large"))?;
    // SAFETY: the descriptor remains open for this call and both offsets were
    // checked for the platform `off_t` representation.
    let result =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(target_os = "linux")]
fn estimate_resident_bytes(path: &Path, len: u64) -> io::Result<u64> {
    use std::os::fd::AsRawFd;
    const WINDOW_BYTES: u64 = 256 * 1024 * 1024;
    if len == 0 {
        return Ok(0);
    }
    let file = File::open(path)?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::other("cannot determine page size"));
    }
    let page_size = page_size as u64;
    let mut offset = 0_u64;
    let mut resident = 0_u64;
    while offset < len {
        let window = (len - offset).min(WINDOW_BYTES);
        let mapping_len = usize::try_from(window)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "warmup window too large"))?;
        // SAFETY: the file is open, offset is page-aligned because every prior
        // window is 256 MiB, and the mapping is released before the next loop.
        let mapping = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapping_len,
                libc::PROT_NONE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                libc::off_t::try_from(offset).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "resident offset too large")
                })?,
            )
        };
        if mapping == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let pages = window.div_ceil(page_size) as usize;
        let mut vector = vec![0_u8; pages];
        // SAFETY: `mapping` names `mapping_len` mapped bytes and `vector`
        // contains one byte for every covered page as required by mincore.
        let result = unsafe { libc::mincore(mapping, mapping_len, vector.as_mut_ptr()) };
        // SAFETY: this exactly unmaps the successful mapping above.
        let unmap_result = unsafe { libc::munmap(mapping, mapping_len) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        if unmap_result != 0 {
            return Err(io::Error::last_os_error());
        }
        resident = resident.saturating_add(
            vector.iter().filter(|entry| **entry & 1 == 1).count() as u64 * page_size,
        );
        offset = offset.saturating_add(window);
    }
    Ok(resident.min(len))
}

#[cfg(not(target_os = "linux"))]
fn estimate_resident_bytes(_path: &Path, _len: u64) -> io::Result<u64> {
    Ok(0)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn bytes_per_second(bytes: u64, duration: Duration) -> u64 {
    if duration.is_zero() {
        return 0;
    }
    (bytes as u128)
        .saturating_mul(1_000_000_000)
        .checked_div(duration.as_nanos())
        .unwrap_or(0)
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests;
