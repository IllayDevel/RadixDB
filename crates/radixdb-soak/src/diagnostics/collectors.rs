use std::{
    collections::BTreeMap,
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};

use super::DIAGNOSTIC_FORMAT_V2;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSampleV2 {
    pub format: u32,
    pub sequence: u64,
    pub monotonic_millis: u64,
    pub unix_millis: u64,
    pub boot_id: String,
    pub load_1: f64,
    pub load_5: f64,
    pub load_15: f64,
    pub runnable_tasks: u64,
    pub total_tasks: u64,
    pub meminfo_kib: BTreeMap<String, u64>,
    pub vmstat: BTreeMap<String, u64>,
    pub psi: BTreeMap<String, PsiSnapshot>,
    pub network: BTreeMap<String, u64>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PsiSnapshot {
    pub some_avg10: Option<f64>,
    pub some_total_micros: Option<u64>,
    pub full_avg10: Option<f64>,
    pub full_total_micros: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSampleV2 {
    pub format: u32,
    pub sequence: u64,
    pub monotonic_millis: u64,
    pub unix_millis: u64,
    pub roles: BTreeMap<String, ProcessSnapshot>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub start_ticks: u64,
    pub executable: String,
    pub state: String,
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    pub pss_kib: Option<u64>,
    pub anonymous_kib: Option<u64>,
    pub swap_kib: Option<u64>,
    pub minor_faults: u64,
    pub major_faults: u64,
    pub user_ticks: u64,
    pub system_ticks: u64,
    pub threads: u64,
    pub open_fds: u64,
    pub socket_fds: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub cancelled_write_bytes: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
    pub cgroup_path: Option<String>,
    pub cgroup: Option<CgroupSnapshot>,
    pub wchan: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupSnapshot {
    pub memory_current: Option<u64>,
    pub memory_peak: Option<u64>,
    pub memory_events: BTreeMap<String, u64>,
    pub memory_stat: BTreeMap<String, u64>,
    pub cpu_stat: BTreeMap<String, u64>,
    pub io_stat: BTreeMap<String, u64>,
    pub pids_current: Option<u64>,
    pub pids_max: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskSampleV2 {
    pub format: u32,
    pub sequence: u64,
    pub monotonic_millis: u64,
    pub unix_millis: u64,
    pub major: u64,
    pub minor: u64,
    pub device: String,
    pub reads_completed: u64,
    pub reads_merged: u64,
    pub sectors_read: u64,
    pub read_millis: u64,
    pub writes_completed: u64,
    pub writes_merged: u64,
    pub sectors_written: u64,
    pub write_millis: u64,
    pub io_in_progress: u64,
    pub io_millis: u64,
    pub weighted_io_millis: u64,
    pub filesystem_total_bytes: u64,
    pub filesystem_free_bytes: u64,
    pub filesystem_available_bytes: u64,
    pub filesystem_total_inodes: u64,
    pub filesystem_free_inodes: u64,
    #[serde(default)]
    pub filesystem_read_only: bool,
    #[serde(default)]
    pub filesystem_mount_options: Vec<String>,
    #[serde(default)]
    pub filesystem_mount_options_error: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SmartSnapshotV2 {
    pub format: u32,
    pub unix_millis: u64,
    pub device: Option<String>,
    pub smartctl_status: Option<i32>,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessCollectionDepth {
    Light,
    Full,
}

pub fn boot_id() -> io::Result<String> {
    let value = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid boot id",
        ));
    }
    Ok(value.into())
}

pub fn collect_host(
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
    boot_id: &str,
) -> HostSampleV2 {
    let mut errors = Vec::new();
    let (load_1, load_5, load_15, runnable_tasks, total_tasks) =
        match parse_loadavg(&read("/proc/loadavg")) {
            Ok(value) => value,
            Err(error) => {
                errors.push(error);
                (0.0, 0.0, 0.0, 0, 0)
            }
        };
    let meminfo_kib = parse_named_u64(
        &read("/proc/meminfo"),
        &[
            "MemTotal",
            "MemAvailable",
            "MemFree",
            "Buffers",
            "Cached",
            "SwapTotal",
            "SwapFree",
            "Dirty",
            "Writeback",
            "AnonPages",
            "Mapped",
            "Slab",
        ],
        &mut errors,
    );
    let vmstat = parse_named_u64(
        &read("/proc/vmstat"),
        &[
            "pgfault",
            "pgmajfault",
            "pgscan_kswapd",
            "pgscan_direct",
            "pgsteal_kswapd",
            "pgsteal_direct",
            "pswpin",
            "pswpout",
            "oom_kill",
        ],
        &mut errors,
    );
    let mut psi = BTreeMap::new();
    for resource in ["cpu", "memory", "io"] {
        match parse_psi(&read(&format!("/proc/pressure/{resource}"))) {
            Ok(value) => {
                psi.insert(resource.into(), value);
            }
            Err(error) => errors.push(format!("psi {resource}: {error}")),
        }
    }
    let mut network = BTreeMap::new();
    for path in ["/proc/net/snmp", "/proc/net/netstat"] {
        match fs::read_to_string(path) {
            Ok(value) => network.extend(parse_network_tables(&value)),
            Err(error) => errors.push(format!("{path}: {error}")),
        }
    }
    if let Ok(value) = fs::read_to_string("/proc/net/sockstat") {
        network.extend(parse_sockstat(&value));
    }
    HostSampleV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        sequence,
        monotonic_millis,
        unix_millis,
        boot_id: boot_id.into(),
        load_1,
        load_5,
        load_15,
        runnable_tasks,
        total_tasks,
        meminfo_kib,
        vmstat,
        psi,
        network,
        errors,
    }
}

pub fn collect_processes(
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
    expected: &[(&str, &Path)],
) -> ProcessSampleV2 {
    collect_processes_with_depth(
        sequence,
        monotonic_millis,
        unix_millis,
        expected,
        ProcessCollectionDepth::Full,
    )
}

pub fn collect_processes_with_depth(
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
    expected: &[(&str, &Path)],
    depth: ProcessCollectionDepth,
) -> ProcessSampleV2 {
    let mut roles = BTreeMap::new();
    let mut errors = Vec::new();
    for (role, executable) in expected {
        match find_exact_process(executable, depth) {
            Ok(Some(process)) => {
                roles.insert((*role).into(), process);
            }
            Ok(None) => errors.push(format!("{role}: process not found")),
            Err(error) => errors.push(format!("{role}: {error}")),
        }
    }
    ProcessSampleV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        sequence,
        monotonic_millis,
        unix_millis,
        roles,
        errors,
    }
}

pub fn collect_disk(
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
    data_dir: &Path,
) -> DiskSampleV2 {
    let mut sample = DiskSampleV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        sequence,
        monotonic_millis,
        unix_millis,
        major: 0,
        minor: 0,
        device: String::new(),
        reads_completed: 0,
        reads_merged: 0,
        sectors_read: 0,
        read_millis: 0,
        writes_completed: 0,
        writes_merged: 0,
        sectors_written: 0,
        write_millis: 0,
        io_in_progress: 0,
        io_millis: 0,
        weighted_io_millis: 0,
        filesystem_total_bytes: 0,
        filesystem_free_bytes: 0,
        filesystem_available_bytes: 0,
        filesystem_total_inodes: 0,
        filesystem_free_inodes: 0,
        filesystem_read_only: false,
        filesystem_mount_options: Vec::new(),
        filesystem_mount_options_error: None,
        error: None,
    };
    if let Err(error) = fill_disk(&mut sample, data_dir) {
        sample.error = Some(error);
    }
    sample
}

pub fn collect_smart(unix_millis: u64, data_dir: &Path) -> SmartSnapshotV2 {
    let mut snapshot = SmartSnapshotV2 {
        format: DIAGNOSTIC_FORMAT_V2,
        unix_millis,
        device: None,
        smartctl_status: None,
        data: None,
        error: None,
    };
    let result = (|| -> Result<(), String> {
        let device = physical_block_device(data_dir)?;
        snapshot.device = Some(device.display().to_string());
        let executable = ["/usr/sbin/smartctl", "/usr/bin/smartctl"]
            .iter()
            .map(Path::new)
            .find(|path| path.is_file())
            .ok_or_else(|| "smartctl is not installed".to_string())?;
        let output = Command::new(executable)
            .args(["--json", "--all"])
            .arg(&device)
            .output()
            .map_err(|error| error.to_string())?;
        snapshot.smartctl_status = output.status.code();
        if output.stdout.len() > 1024 * 1024 || output.stderr.len() > 64 * 1024 {
            return Err("smartctl output exceeds diagnostic bound".into());
        }
        snapshot.data = Some(
            serde_json::from_slice(&output.stdout)
                .map_err(|error| format!("parse smartctl JSON: {error}"))?,
        );
        if !output.stderr.is_empty() {
            return Err(format!(
                "smartctl stderr: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(())
    })();
    if let Err(error) = result {
        snapshot.error = Some(error);
    }
    snapshot
}

/// Return fail-visible SMART deltas that represent physical degradation.
/// Missing tooling or a replaced device remains explicit in the samples but
/// does not fabricate a disk failure.
pub fn smart_degradation(baseline: &SmartSnapshotV2, current: &SmartSnapshotV2) -> Vec<String> {
    if baseline.device.is_none()
        || current.device.is_none()
        || baseline.device != current.device
        || baseline.data.is_none()
        || current.data.is_none()
    {
        return Vec::new();
    }
    let baseline_data = baseline.data.as_ref().unwrap();
    let current_data = current.data.as_ref().unwrap();
    let mut evidence = Vec::new();
    if baseline_data
        .pointer("/smart_status/passed")
        .and_then(serde_json::Value::as_bool)
        != Some(false)
        && current_data
            .pointer("/smart_status/passed")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
    {
        evidence.push("SMART overall health changed to failed".into());
    }
    for name in [
        "Reallocated_Sector_Ct",
        "Current_Pending_Sector",
        "Offline_Uncorrectable",
        "UDMA_CRC_Error_Count",
    ] {
        let before = ata_raw_value(baseline_data, name);
        let after = ata_raw_value(current_data, name);
        if after > before {
            evidence.push(format!("{name} increased from {before} to {after}"));
        }
    }
    for (pointer, name) in [
        (
            "/nvme_smart_health_information_log/media_errors",
            "NVMe media_errors",
        ),
        (
            "/nvme_smart_health_information_log/num_err_log_entries",
            "NVMe error log entries",
        ),
    ] {
        let before = baseline_data
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let after = current_data
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if after > before {
            evidence.push(format!("{name} increased from {before} to {after}"));
        }
    }
    let before_warning = baseline_data
        .pointer("/nvme_smart_health_information_log/critical_warning")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let after_warning = current_data
        .pointer("/nvme_smart_health_information_log/critical_warning")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if after_warning & !before_warning != 0 {
        evidence.push(format!(
            "NVMe critical warning changed from {before_warning} to {after_warning}"
        ));
    }
    evidence
}

fn ata_raw_value(data: &serde_json::Value, name: &str) -> u64 {
    data.pointer("/ata_smart_attributes/table")
        .and_then(serde_json::Value::as_array)
        .and_then(|attributes| {
            attributes.iter().find(|attribute| {
                attribute.get("name").and_then(serde_json::Value::as_str) == Some(name)
            })
        })
        .and_then(|attribute| attribute.pointer("/raw/value"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn fill_disk(sample: &mut DiskSampleV2, data_dir: &Path) -> Result<(), String> {
    let metadata = fs::metadata(data_dir).map_err(|error| error.to_string())?;
    let device_id = metadata.dev();
    let (major, minor) = linux_device_major_minor(device_id);
    sample.major = major;
    sample.minor = minor;
    let diskstats = fs::read_to_string("/proc/diskstats").map_err(|error| error.to_string())?;
    let line = diskstats
        .lines()
        .find(|line| {
            let mut fields = line.split_ascii_whitespace();
            fields.next().and_then(|value| value.parse::<u64>().ok()) == Some(major)
                && fields.next().and_then(|value| value.parse::<u64>().ok()) == Some(minor)
        })
        .ok_or_else(|| format!("device {major}:{minor} absent from /proc/diskstats"))?;
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() < 14 {
        return Err("short /proc/diskstats line".into());
    }
    sample.device = fields[2].into();
    sample.reads_completed = parse_u64(fields[3], "reads_completed")?;
    sample.reads_merged = parse_u64(fields[4], "reads_merged")?;
    sample.sectors_read = parse_u64(fields[5], "sectors_read")?;
    sample.read_millis = parse_u64(fields[6], "read_millis")?;
    sample.writes_completed = parse_u64(fields[7], "writes_completed")?;
    sample.writes_merged = parse_u64(fields[8], "writes_merged")?;
    sample.sectors_written = parse_u64(fields[9], "sectors_written")?;
    sample.write_millis = parse_u64(fields[10], "write_millis")?;
    sample.io_in_progress = parse_u64(fields[11], "io_in_progress")?;
    sample.io_millis = parse_u64(fields[12], "io_millis")?;
    sample.weighted_io_millis = parse_u64(fields[13], "weighted_io_millis")?;

    let path = std::ffi::CString::new(data_dir.as_os_str().as_encoded_bytes())
        .map_err(|_| "data directory contains NUL".to_string())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is a valid NUL-terminated byte string and stat points to
    // writable storage for one libc::statvfs result.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    // SAFETY: successful statvfs initialized the result.
    let stat = unsafe { stat.assume_init() };
    let block_size = stat.f_frsize;
    sample.filesystem_total_bytes = stat.f_blocks.saturating_mul(block_size);
    sample.filesystem_free_bytes = stat.f_bfree.saturating_mul(block_size);
    sample.filesystem_available_bytes = stat.f_bavail.saturating_mul(block_size);
    sample.filesystem_total_inodes = stat.f_files;
    sample.filesystem_free_inodes = stat.f_ffree;
    sample.filesystem_read_only = stat.f_flag & libc::ST_RDONLY != 0;
    match filesystem_mount_options(data_dir, major, minor) {
        Ok(options) => sample.filesystem_mount_options = options,
        Err(error) => sample.filesystem_mount_options_error = Some(error),
    }
    Ok(())
}

fn filesystem_mount_options(
    data_dir: &Path,
    major: u64,
    minor: u64,
) -> Result<Vec<String>, String> {
    const MAX_OPTIONS: usize = 32;
    const MAX_OPTION_BYTES: usize = 128;

    let data_dir = fs::canonicalize(data_dir).map_err(|error| error.to_string())?;
    let mountinfo =
        fs::read_to_string("/proc/self/mountinfo").map_err(|error| error.to_string())?;
    let device = format!("{major}:{minor}");
    let mut best: Option<(usize, Vec<String>)> = None;
    for line in mountinfo.lines() {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        let Some(separator) = fields.iter().position(|field| *field == "-") else {
            continue;
        };
        if separator < 6 || fields.get(2) != Some(&device.as_str()) || fields.len() <= separator + 3
        {
            continue;
        }
        let mount_point = PathBuf::from(decode_mountinfo_field(fields[4])?);
        if !data_dir.starts_with(&mount_point) {
            continue;
        }
        let mut options = fields[5]
            .split(',')
            .chain(fields[separator + 3].split(','))
            .filter(|option| !option.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        options.sort();
        options.dedup();
        if options.len() > MAX_OPTIONS
            || options.iter().any(|option| option.len() > MAX_OPTION_BYTES)
        {
            return Err("filesystem mount options exceed diagnostic bound".into());
        }
        let depth = mount_point.as_os_str().len();
        if best
            .as_ref()
            .is_none_or(|(best_depth, _)| depth > *best_depth)
        {
            best = Some((depth, options));
        }
    }
    best.map(|(_, options)| options)
        .ok_or_else(|| format!("mount {device} absent from /proc/self/mountinfo"))
}

fn decode_mountinfo_field(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let digits = bytes
                .get(index + 1..index + 4)
                .ok_or_else(|| "truncated mountinfo escape".to_string())?;
            if !digits.iter().all(|digit| matches!(digit, b'0'..=b'7')) {
                return Err("invalid mountinfo escape".into());
            }
            let value = (digits[0] - b'0') * 64 + (digits[1] - b'0') * 8 + (digits[2] - b'0');
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "mountinfo path is not UTF-8".into())
}

fn physical_block_device(data_dir: &Path) -> Result<PathBuf, String> {
    let device_id = fs::metadata(data_dir)
        .map_err(|error| error.to_string())?
        .dev();
    let (major, minor) = linux_device_major_minor(device_id);
    let sys_path = fs::canonicalize(format!("/sys/dev/block/{major}:{minor}"))
        .map_err(|error| error.to_string())?;
    let components = sys_path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>();
    let block_index = components
        .iter()
        .position(|component| *component == "block")
        .ok_or_else(|| "sysfs device path has no block owner".to_string())?;
    let name = components
        .get(block_index + 1)
        .ok_or_else(|| "sysfs block owner is missing".to_string())?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("unsafe physical block device name".into());
    }
    Ok(Path::new("/dev").join(name))
}

fn find_exact_process(
    expected_executable: &Path,
    depth: ProcessCollectionDepth,
) -> io::Result<Option<ProcessSnapshot>> {
    let expected = fs::canonicalize(expected_executable)?;
    let mut matches = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let executable = match fs::read_link(entry.path().join("exe")) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if executable == expected {
            matches.push((pid, executable));
        }
    }
    if matches.len() > 1 {
        let pids = matches.iter().map(|(pid, _)| *pid).collect::<Vec<_>>();
        let roots = matches
            .iter()
            .filter(|(pid, _)| process_parent(*pid).is_ok_and(|parent| !pids.contains(&parent)))
            .cloned()
            .collect::<Vec<_>>();
        if let [(pid, executable)] = roots.as_slice() {
            return read_process(*pid, executable, depth).map(Some);
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "multiple exact process roots for {}: {pids:?}",
                expected.display()
            ),
        ));
    }
    let Some((pid, executable)) = matches.pop() else {
        return Ok(None);
    };
    read_process(pid, &executable, depth).map(Some)
}

fn process_parent(pid: u32) -> io::Result<u32> {
    fs::read_to_string(format!("/proc/{pid}/status"))?
        .lines()
        .find_map(|line| line.strip_prefix("PPid:\t"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing PPid"))?
        .trim()
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid PPid"))
}

fn read_process(
    pid: u32,
    executable: &Path,
    depth: ProcessCollectionDepth,
) -> io::Result<ProcessSnapshot> {
    let root = PathBuf::from(format!("/proc/{pid}"));
    let stat = fs::read_to_string(root.join("stat"))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed proc stat"))?;
    let fields = stat[close + 1..]
        .split_ascii_whitespace()
        .collect::<Vec<_>>();
    if fields.len() < 22 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short proc stat",
        ));
    }
    let status = fs::read_to_string(root.join("status"))?;
    let status_values = parse_status_values(&status);
    let io_values = parse_key_values(&fs::read_to_string(root.join("io")).unwrap_or_default());
    let smaps = matches!(depth, ProcessCollectionDepth::Full)
        .then(|| {
            parse_key_values(&fs::read_to_string(root.join("smaps_rollup")).unwrap_or_default())
        })
        .unwrap_or_default();
    let mut open_fds = 0u64;
    let mut socket_fds = 0u64;
    if matches!(depth, ProcessCollectionDepth::Full) {
        if let Ok(entries) = fs::read_dir(root.join("fd")) {
            for entry in entries.flatten() {
                open_fds = open_fds.saturating_add(1);
                if fs::read_link(entry.path())
                    .ok()
                    .is_some_and(|path| path.to_string_lossy().starts_with("socket:["))
                {
                    socket_fds = socket_fds.saturating_add(1);
                }
            }
        }
    }
    let cgroup_path = matches!(depth, ProcessCollectionDepth::Full)
        .then(|| fs::read_to_string(root.join("cgroup")).ok())
        .flatten()
        .and_then(|value| {
            value
                .lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_string))
        });
    let cgroup = cgroup_path
        .as_deref()
        .and_then(|path| read_cgroup(path).ok());
    Ok(ProcessSnapshot {
        pid,
        start_ticks: field_u64(&fields, 19, "starttime")?,
        executable: executable.display().to_string(),
        state: fields[0].into(),
        rss_bytes: status_values.get("VmRSS").copied().unwrap_or(0) * 1024,
        virtual_bytes: status_values.get("VmSize").copied().unwrap_or(0) * 1024,
        pss_kib: smaps.get("Pss").copied(),
        anonymous_kib: smaps.get("Anonymous").copied(),
        swap_kib: smaps.get("Swap").copied(),
        minor_faults: field_u64(&fields, 7, "minflt")?,
        major_faults: field_u64(&fields, 9, "majflt")?,
        user_ticks: field_u64(&fields, 11, "utime")?,
        system_ticks: field_u64(&fields, 12, "stime")?,
        threads: field_u64(&fields, 17, "num_threads")?,
        open_fds,
        socket_fds,
        read_bytes: io_values.get("read_bytes").copied().unwrap_or(0),
        write_bytes: io_values.get("write_bytes").copied().unwrap_or(0),
        cancelled_write_bytes: io_values.get("cancelled_write_bytes").copied().unwrap_or(0),
        voluntary_context_switches: status_values
            .get("voluntary_ctxt_switches")
            .copied()
            .unwrap_or(0),
        involuntary_context_switches: status_values
            .get("nonvoluntary_ctxt_switches")
            .copied()
            .unwrap_or(0),
        cgroup_path,
        cgroup,
        wchan: fs::read_to_string(root.join("wchan"))
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    })
}

fn read_cgroup(path: &str) -> io::Result<CgroupSnapshot> {
    let relative = Path::new(path)
        .strip_prefix("/")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid cgroup path"))?;
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsafe cgroup path",
        ));
    }
    let root = Path::new("/sys/fs/cgroup").join(relative);
    let read_value = |name: &str| -> Option<u64> {
        fs::read_to_string(root.join(name))
            .ok()?
            .trim()
            .parse()
            .ok()
    };
    let parse_file = |name: &str| -> BTreeMap<String, u64> {
        fs::read_to_string(root.join(name))
            .ok()
            .map(|value| parse_space_values(&value))
            .unwrap_or_default()
    };
    let io_stat = fs::read_to_string(root.join("io.stat"))
        .ok()
        .map(|value| aggregate_io_stat(&value))
        .unwrap_or_default();
    Ok(CgroupSnapshot {
        memory_current: read_value("memory.current"),
        memory_peak: read_value("memory.peak"),
        memory_events: parse_file("memory.events"),
        memory_stat: parse_file("memory.stat"),
        cpu_stat: parse_file("cpu.stat"),
        io_stat,
        pids_current: read_value("pids.current"),
        pids_max: read_value("pids.max"),
    })
}

fn parse_space_values(value: &str) -> BTreeMap<String, u64> {
    value
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_ascii_whitespace();
            let name = fields.next()?;
            let value = fields.next()?.parse().ok()?;
            Some((name.into(), value))
        })
        .collect()
}

fn aggregate_io_stat(value: &str) -> BTreeMap<String, u64> {
    let mut result = BTreeMap::<String, u64>::new();
    for line in value.lines() {
        for (name, value) in line
            .split_ascii_whitespace()
            .skip(1)
            .filter_map(|field| field.split_once('='))
        {
            if let Ok(value) = value.parse::<u64>() {
                let entry = result.entry(name.into()).or_default();
                *entry = entry.saturating_add(value);
            }
        }
    }
    result
}

fn read(path: &str) -> Result<String, String> {
    fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))
}

fn parse_loadavg(value: &Result<String, String>) -> Result<(f64, f64, f64, u64, u64), String> {
    let value = value.as_ref().map_err(Clone::clone)?;
    let fields = value.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() < 4 {
        return Err("short /proc/loadavg".into());
    }
    let (runnable, total) = fields[3]
        .split_once('/')
        .ok_or_else(|| "malformed /proc/loadavg task count".to_string())?;
    Ok((
        fields[0].parse().map_err(|_| "invalid load1")?,
        fields[1].parse().map_err(|_| "invalid load5")?,
        fields[2].parse().map_err(|_| "invalid load15")?,
        runnable.parse().map_err(|_| "invalid runnable tasks")?,
        total.parse().map_err(|_| "invalid total tasks")?,
    ))
}

fn parse_named_u64(
    value: &Result<String, String>,
    names: &[&str],
    errors: &mut Vec<String>,
) -> BTreeMap<String, u64> {
    let Ok(value) = value else {
        errors.push(value.as_ref().unwrap_err().clone());
        return BTreeMap::new();
    };
    let all = parse_key_values(value);
    names
        .iter()
        .filter_map(|name| all.get(*name).copied().map(|value| ((*name).into(), value)))
        .collect()
}

fn parse_key_values(value: &str) -> BTreeMap<String, u64> {
    value
        .lines()
        .filter_map(|line| {
            let (name, rest) = line.split_once(':')?;
            let value = rest.split_ascii_whitespace().next()?.parse().ok()?;
            Some((name.into(), value))
        })
        .collect()
}

fn parse_status_values(value: &str) -> BTreeMap<String, u64> {
    parse_key_values(value)
}

fn parse_psi(value: &Result<String, String>) -> Result<PsiSnapshot, String> {
    let value = value.as_ref().map_err(Clone::clone)?;
    let mut result = PsiSnapshot::default();
    for line in value.lines() {
        let mut fields = line.split_ascii_whitespace();
        let kind = fields.next().unwrap_or_default();
        let values = fields
            .filter_map(|field| field.split_once('='))
            .collect::<BTreeMap<_, _>>();
        let avg10 = values.get("avg10").and_then(|value| value.parse().ok());
        let total = values.get("total").and_then(|value| value.parse().ok());
        match kind {
            "some" => {
                result.some_avg10 = avg10;
                result.some_total_micros = total;
            }
            "full" => {
                result.full_avg10 = avg10;
                result.full_total_micros = total;
            }
            _ => {}
        }
    }
    Ok(result)
}

fn parse_network_tables(value: &str) -> BTreeMap<String, u64> {
    let selected = [
        "Tcp.CurrEstab",
        "Tcp.ActiveOpens",
        "Tcp.PassiveOpens",
        "Tcp.AttemptFails",
        "Tcp.EstabResets",
        "Tcp.InSegs",
        "Tcp.OutSegs",
        "Tcp.RetransSegs",
        "Tcp.InErrs",
        "Tcp.OutRsts",
        "TcpExt.ListenOverflows",
        "TcpExt.ListenDrops",
        "TcpExt.TCPTimeouts",
        "TcpExt.TCPAbortOnMemory",
    ];
    let mut result = BTreeMap::new();
    let lines = value.lines().collect::<Vec<_>>();
    for pair in lines.chunks_exact(2) {
        let mut header = pair[0].split_ascii_whitespace();
        let mut values = pair[1].split_ascii_whitespace();
        let Some(prefix) = header.next().map(|value| value.trim_end_matches(':')) else {
            continue;
        };
        if values.next().map(|value| value.trim_end_matches(':')) != Some(prefix) {
            continue;
        }
        for (name, value) in header.zip(values) {
            let key = format!("{prefix}.{name}");
            if selected.contains(&key.as_str()) {
                if let Ok(value) = value.parse() {
                    result.insert(key, value);
                }
            }
        }
    }
    result
}

fn parse_sockstat(value: &str) -> BTreeMap<String, u64> {
    let mut result = BTreeMap::new();
    for line in value.lines() {
        let mut fields = line.split_ascii_whitespace();
        let Some(prefix) = fields.next().map(|value| value.trim_end_matches(':')) else {
            continue;
        };
        let pairs = fields.collect::<Vec<_>>();
        for pair in pairs.chunks_exact(2) {
            if let Ok(value) = pair[1].parse() {
                result.insert(format!("Sockstat.{prefix}.{}", pair[0]), value);
            }
        }
    }
    result
}

fn field_u64(fields: &[&str], index: usize, name: &str) -> io::Result<u64> {
    fields
        .get(index)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {name}")))?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("invalid {name}")))
}

fn parse_u64(value: &str, name: &str) -> Result<u64, String> {
    value.parse().map_err(|_| format!("invalid {name}"))
}

fn linux_device_major_minor(device: u64) -> (u64, u64) {
    let major = (device >> 8) & 0xfff;
    let minor = (device & 0xff) | ((device >> 12) & 0xfff00);
    (major, minor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collectors_are_bounded_and_parse_current_linux_procfs() {
        let boot = boot_id().unwrap();
        let host = collect_host(1, 1, 1, &boot);
        assert_eq!(host.format, DIAGNOSTIC_FORMAT_V2);
        assert!(host.meminfo_kib.len() <= 13);
        assert!(host.vmstat.len() <= 9);
        assert!(host.psi.len() <= 3);
        assert!(host.network.len() <= 64);

        let disk = collect_disk(1, 1, 1, Path::new("/tmp"));
        assert_eq!(disk.format, DIAGNOSTIC_FORMAT_V2);
        let smart = collect_smart(1, Path::new("/tmp"));
        assert_eq!(smart.format, DIAGNOSTIC_FORMAT_V2);
    }

    #[test]
    fn linux_device_number_round_trips_common_layout() {
        assert_eq!(linux_device_major_minor((8 << 8) | 1), (8, 1));
    }

    #[test]
    fn mountinfo_path_decoder_accepts_only_bounded_octal_escapes() {
        assert_eq!(
            decode_mountinfo_field("/storage/radix\\040db").unwrap(),
            "/storage/radix db"
        );
        assert!(decode_mountinfo_field("/storage/invalid\\999").is_err());
        assert!(decode_mountinfo_field("/storage/truncated\\04").is_err());
    }

    #[test]
    fn smart_delta_detects_media_growth_but_not_device_replacement() {
        let snapshot = |device: &str, pending: u64| SmartSnapshotV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            unix_millis: 1,
            device: Some(device.into()),
            smartctl_status: Some(0),
            data: Some(serde_json::json!({
                "smart_status": {"passed": true},
                "ata_smart_attributes": {"table": [{
                    "name": "Current_Pending_Sector",
                    "raw": {"value": pending}
                }]}
            })),
            error: None,
        };
        let baseline = snapshot("/dev/sda", 0);
        let degraded = snapshot("/dev/sda", 2);
        assert_eq!(smart_degradation(&baseline, &degraded).len(), 1);
        assert!(smart_degradation(&baseline, &snapshot("/dev/sdb", 2)).is_empty());
    }
}
