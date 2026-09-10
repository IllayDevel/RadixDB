use std::{
    collections::VecDeque,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crate::config::DatabaseEngine;
use crate::status::{ResourceSlopes, ResourceSnapshot};

const SLOPE_WINDOW: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Debug)]
struct TimedResources {
    at: Instant,
    resources: ResourceSnapshot,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DatabaseFootprint {
    total_bytes: u64,
    files: u64,
    data_bytes: u64,
    index_bytes: u64,
    metadata_bytes: u64,
    wal_bytes: u64,
    other_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseFileKind {
    Data,
    Index,
    Metadata,
    Wal,
    Other,
}

pub struct ResourceSampler {
    server_executable: PathBuf,
    data_dir: PathBuf,
    engine: DatabaseEngine,
    pid: Option<u32>,
    history: VecDeque<TimedResources>,
}

impl ResourceSampler {
    pub fn new(server_executable: PathBuf, data_dir: PathBuf, engine: DatabaseEngine) -> Self {
        Self {
            server_executable,
            data_dir,
            engine,
            pid: None,
            history: VecDeque::new(),
        }
    }

    pub fn sample(&mut self) -> io::Result<(ResourceSnapshot, ResourceSlopes)> {
        let pid = self.resolve_server_pid()?;
        let mut resources = sample_process_tree(pid)?;
        let footprint = directory_footprint(&self.data_dir)?;
        resources.database_bytes = footprint.total_bytes;
        resources.database_files = footprint.files;
        resources.database_data_bytes = footprint.data_bytes;
        resources.database_index_bytes = footprint.index_bytes;
        resources.database_metadata_bytes = footprint.metadata_bytes;
        resources.database_wal_bytes = footprint.wal_bytes;
        resources.database_other_bytes = footprint.other_bytes;
        let now = Instant::now();
        self.history.push_back(TimedResources {
            at: now,
            resources: resources.clone(),
        });
        while self.history.len() > 2 && now.duration_since(self.history[0].at) > SLOPE_WINDOW {
            self.history.pop_front();
        }
        let slopes = self
            .history
            .front()
            .map(|start| slopes(start, now, &resources))
            .unwrap_or_default();
        Ok((resources, slopes))
    }

    pub fn server_pid(&mut self) -> io::Result<u32> {
        self.resolve_server_pid()
    }

    pub fn forget_process(&mut self) {
        self.pid = None;
    }

    fn resolve_server_pid(&mut self) -> io::Result<u32> {
        let expected = fs::canonicalize(&self.server_executable)?;
        if let Some(pid) = self.pid {
            if process_executable(pid).is_ok_and(|path| path == expected) {
                return Ok(pid);
            }
            self.pid = None;
        }
        let mut matches = Vec::new();
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            if process_executable(pid).is_ok_and(|path| path == expected) {
                matches.push(pid);
            }
        }
        match matches.as_slice() {
            [pid] => {
                self.pid = Some(*pid);
                Ok(*pid)
            }
            [] => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no process owns expected server executable {}",
                    expected.display()
                ),
            )),
            _ if self.engine == DatabaseEngine::Postgresql => {
                let roots = postgres_cluster_roots(&matches, &self.data_dir)?;
                match roots.as_slice() {
                    [pid] => {
                        self.pid = Some(*pid);
                        Ok(*pid)
                    }
                    _ => Err(io::Error::other(format!(
                        "cannot identify one PostgreSQL postmaster for {}: candidates={matches:?} roots={roots:?}",
                        self.data_dir.display()
                    ))),
                }
            }
            _ => Err(io::Error::other(format!(
                "multiple processes own expected server executable {}: {matches:?}",
                expected.display()
            ))),
        }
    }
}

fn postgres_cluster_roots(matches: &[u32], data_dir: &Path) -> io::Result<Vec<u32>> {
    let canonical_data = fs::canonicalize(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
    let data_text = canonical_data.to_string_lossy();
    let mut explicit = Vec::new();
    for pid in matches {
        let cmdline = fs::read(format!("/proc/{pid}/cmdline"))?;
        let fields = cmdline
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty())
            .map(|field| String::from_utf8_lossy(field))
            .collect::<Vec<_>>();
        if fields.windows(2).any(|pair| {
            pair[0] == "-D"
                && fs::canonicalize(pair[1].as_ref())
                    .unwrap_or_else(|_| PathBuf::from(pair[1].as_ref()))
                    == canonical_data
        }) || fields.iter().any(|field| field.as_ref() == data_text)
        {
            explicit.push(*pid);
        }
    }
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    let match_set = matches
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    Ok(matches
        .iter()
        .copied()
        .filter(|pid| process_parent(*pid).is_ok_and(|parent| !match_set.contains(&parent)))
        .collect())
}

fn sample_process_tree(root: u32) -> io::Result<ResourceSnapshot> {
    let mut descendants = vec![root];
    let mut cursor = 0;
    while cursor < descendants.len() {
        let parent = descendants[cursor];
        cursor += 1;
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            if !descendants.contains(&pid) && process_parent(pid).ok() == Some(parent) {
                descendants.push(pid);
            }
        }
    }
    let mut total = ResourceSnapshot::default();
    let mut sampled_root = false;
    for pid in descendants {
        match sample_process(pid) {
            Ok(sample) => {
                sampled_root |= pid == root;
                add_process_resources(&mut total, &sample);
            }
            Err(error) if pid != root && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    if !sampled_root {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "server process disappeared while sampling",
        ));
    }
    Ok(total)
}

fn add_process_resources(total: &mut ResourceSnapshot, value: &ResourceSnapshot) {
    total.rss_bytes = total.rss_bytes.saturating_add(value.rss_bytes);
    total.virtual_bytes = total.virtual_bytes.saturating_add(value.virtual_bytes);
    total.open_fds = total.open_fds.saturating_add(value.open_fds);
    total.socket_fds = total.socket_fds.saturating_add(value.socket_fds);
    total.threads = total.threads.saturating_add(value.threads);
    total.cpu_user_millis = total.cpu_user_millis.saturating_add(value.cpu_user_millis);
    total.cpu_system_millis = total
        .cpu_system_millis
        .saturating_add(value.cpu_system_millis);
    total.process_read_bytes = total
        .process_read_bytes
        .saturating_add(value.process_read_bytes);
    total.process_write_bytes = total
        .process_write_bytes
        .saturating_add(value.process_write_bytes);
    total.voluntary_context_switches = total
        .voluntary_context_switches
        .saturating_add(value.voluntary_context_switches);
    total.involuntary_context_switches = total
        .involuntary_context_switches
        .saturating_add(value.involuntary_context_switches);
}

fn process_parent(pid: u32) -> io::Result<u32> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:\t"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing PPid"))?
        .trim()
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid PPid"))
}

fn process_executable(pid: u32) -> io::Result<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe")).and_then(fs::canonicalize)
}

fn sample_process(pid: u32) -> io::Result<ResourceSnapshot> {
    let proc = PathBuf::from(format!("/proc/{pid}"));
    let stat = fs::read_to_string(proc.join("stat"))?;
    let (_, fields) = stat
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid /proc stat"))?;
    let fields = fields.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() < 22 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short /proc stat",
        ));
    }
    let ticks = clock_ticks()?;
    let page_size = page_size()?;
    let user_ticks = number(fields[11], "utime")?;
    let system_ticks = number(fields[12], "stime")?;
    let virtual_bytes = number(fields[20], "vsize")?;
    let rss_pages = number(fields[21], "rss")?;
    let threads = number(fields[17], "threads")?;

    let mut read_bytes = 0;
    let mut write_bytes = 0;
    for line in fs::read_to_string(proc.join("io"))?.lines() {
        if let Some(value) = line.strip_prefix("read_bytes: ") {
            read_bytes = number(value, "read_bytes")?;
        } else if let Some(value) = line.strip_prefix("write_bytes: ") {
            write_bytes = number(value, "write_bytes")?;
        }
    }
    let mut voluntary = 0;
    let mut involuntary = 0;
    for line in fs::read_to_string(proc.join("status"))?.lines() {
        if let Some(value) = line.strip_prefix("voluntary_ctxt_switches:\t") {
            voluntary = number(value, "voluntary context switches")?;
        } else if let Some(value) = line.strip_prefix("nonvoluntary_ctxt_switches:\t") {
            involuntary = number(value, "involuntary context switches")?;
        }
    }
    let mut open_fds = 0u64;
    let mut socket_fds = 0u64;
    for entry in fs::read_dir(proc.join("fd"))? {
        let entry = entry?;
        open_fds = open_fds.saturating_add(1);
        if fs::read_link(entry.path())
            .is_ok_and(|target| target.to_string_lossy().starts_with("socket:["))
        {
            socket_fds = socket_fds.saturating_add(1);
        }
    }
    Ok(ResourceSnapshot {
        rss_bytes: rss_pages.saturating_mul(page_size),
        virtual_bytes,
        open_fds,
        socket_fds,
        threads,
        cpu_user_millis: user_ticks.saturating_mul(1_000) / ticks,
        cpu_system_millis: system_ticks.saturating_mul(1_000) / ticks,
        process_read_bytes: read_bytes,
        process_write_bytes: write_bytes,
        database_bytes: 0,
        database_files: 0,
        database_data_bytes: 0,
        database_index_bytes: 0,
        database_metadata_bytes: 0,
        database_wal_bytes: 0,
        database_other_bytes: 0,
        voluntary_context_switches: voluntary,
        involuntary_context_switches: involuntary,
    })
}

fn clock_ticks() -> io::Result<u64> {
    // SAFETY: sysconf has no pointer preconditions and `_SC_CLK_TCK` is valid.
    let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::other("sysconf(_SC_CLK_TCK) failed"))
}

fn page_size() -> io::Result<u64> {
    // SAFETY: sysconf has no pointer preconditions and `_SC_PAGESIZE` is valid.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| io::Error::other("sysconf(_SC_PAGESIZE) failed"))
}

fn number(value: &str, name: &str) -> io::Result<u64> {
    value.trim().parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {name} in /proc"),
        )
    })
}

fn slopes(start: &TimedResources, end: Instant, current: &ResourceSnapshot) -> ResourceSlopes {
    let elapsed = end.duration_since(start.at);
    if elapsed.is_zero() {
        return ResourceSlopes::default();
    }
    let hours = elapsed.as_secs_f64() / 3600.0;
    ResourceSlopes {
        window_millis: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        rss_bytes_per_hour: signed_delta(current.rss_bytes, start.resources.rss_bytes) / hours,
        database_bytes_per_hour: signed_delta(
            current.database_bytes,
            start.resources.database_bytes,
        ) / hours,
    }
}

fn signed_delta(current: u64, start: u64) -> f64 {
    if current >= start {
        (current - start) as f64
    } else {
        -((start - current) as f64)
    }
}

fn directory_footprint(root: &Path) -> io::Result<DatabaseFootprint> {
    let mut footprint = DatabaseFootprint::default();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound && path != root => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let Some(metadata) = entry_metadata_if_present(&entry)? else {
                continue;
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                let path = entry.path();
                let bytes = metadata.len();
                footprint.total_bytes = footprint.total_bytes.saturating_add(bytes);
                footprint.files = footprint.files.saturating_add(1);
                match classify_database_file(root, &path) {
                    DatabaseFileKind::Data => {
                        footprint.data_bytes = footprint.data_bytes.saturating_add(bytes);
                    }
                    DatabaseFileKind::Index => {
                        footprint.index_bytes = footprint.index_bytes.saturating_add(bytes);
                    }
                    DatabaseFileKind::Metadata => {
                        footprint.metadata_bytes = footprint.metadata_bytes.saturating_add(bytes);
                    }
                    DatabaseFileKind::Wal => {
                        footprint.wal_bytes = footprint.wal_bytes.saturating_add(bytes);
                    }
                    DatabaseFileKind::Other => {
                        footprint.other_bytes = footprint.other_bytes.saturating_add(bytes);
                    }
                }
            }
        }
    }
    Ok(footprint)
}

fn classify_database_file(root: &Path, path: &Path) -> DatabaseFileKind {
    let relative = path.strip_prefix(root).unwrap_or(path);
    if relative
        .components()
        .any(|component| component.as_os_str() == OsStr::new("wal"))
    {
        return DatabaseFileKind::Wal;
    }
    let name = relative
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default();
    match Path::new(name).extension().and_then(OsStr::to_str) {
        Some("data") => DatabaseFileKind::Data,
        Some("idx") => DatabaseFileKind::Index,
        Some("mft" | "cat") => DatabaseFileKind::Metadata,
        _ if matches!(name, "CONTROL.0" | "CONTROL.1") => DatabaseFileKind::Metadata,
        _ => DatabaseFileKind::Other,
    }
}

fn entry_metadata_if_present(entry: &fs::DirEntry) -> io::Result<Option<fs::Metadata>> {
    match entry.metadata() {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disappearing_directory_entry_is_not_a_sampling_failure() {
        let directory = tempfile::tempdir().unwrap();
        let transient = directory.path().join("compaction-output.data");
        fs::write(&transient, b"temporary").unwrap();
        let entry = fs::read_dir(directory.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        fs::remove_file(transient).unwrap();

        assert!(entry_metadata_if_present(&entry).unwrap().is_none());
        assert_eq!(
            directory_footprint(directory.path()).unwrap().total_bytes,
            0
        );
    }

    #[test]
    fn missing_database_root_remains_a_sampling_failure() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing-root");
        let error = directory_footprint(&missing).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn database_footprint_separates_data_index_metadata_wal_and_other_files() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("artifacts/data/00");
        let index = directory.path().join("artifacts/index/00");
        let manifests = directory.path().join("manifests/tables");
        let wal = directory.path().join("wal");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&index).unwrap();
        fs::create_dir_all(&manifests).unwrap();
        fs::create_dir_all(&wal).unwrap();
        fs::write(data.join("artifact.data"), vec![0_u8; 10]).unwrap();
        fs::write(index.join("artifact.idx"), vec![0_u8; 7]).unwrap();
        fs::write(manifests.join("table.mft"), vec![0_u8; 5]).unwrap();
        fs::write(directory.path().join("CONTROL.0"), vec![0_u8; 2]).unwrap();
        fs::write(wal.join("wal-0001.log"), vec![0_u8; 11]).unwrap();
        fs::write(directory.path().join("unknown.bin"), vec![0_u8; 3]).unwrap();

        let footprint = directory_footprint(directory.path()).unwrap();
        assert_eq!(footprint.total_bytes, 38);
        assert_eq!(footprint.files, 6);
        assert_eq!(footprint.data_bytes, 10);
        assert_eq!(footprint.index_bytes, 7);
        assert_eq!(footprint.metadata_bytes, 7);
        assert_eq!(footprint.wal_bytes, 11);
        assert_eq!(footprint.other_bytes, 3);
    }

    #[test]
    fn slope_handles_growth_and_shrink() {
        let start = TimedResources {
            at: Instant::now(),
            resources: ResourceSnapshot {
                rss_bytes: 10,
                database_bytes: 100,
                ..ResourceSnapshot::default()
            },
        };
        let end = start.at + Duration::from_secs(3600);
        let current = ResourceSnapshot {
            rss_bytes: 30,
            database_bytes: 90,
            ..ResourceSnapshot::default()
        };
        let value = slopes(&start, end, &current);
        assert_eq!(value.rss_bytes_per_hour, 20.0);
        assert_eq!(value.database_bytes_per_hour, -10.0);
    }
}
