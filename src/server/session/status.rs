use super::*;
use std::sync::atomic::AtomicU64;

const ARTIFACT_STATUS_MAX_ENTRIES: u64 = 4_096;
const ARTIFACT_STATUS_MAX_DEPTH: usize = 8;
static ARTIFACT_STATUS_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn server_status(
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    requested_database: Option<&str>,
    include_build_identity: bool,
    runtime: &RuntimeState,
) -> ServerMessage {
    if let Some(name) = requested_database {
        if let Err(message) = validate_database_name(name, config.max_database_name_bytes) {
            return ServerMessage::Error(ProtocolFailure {
                code: ProtocolErrorCode::ProtocolViolation,
                message,
            });
        }
    }

    let databases_root = config.data_dir.join("databases");
    let mut statuses = Vec::new();
    let registry_snapshot = match databases.lock() {
        Ok(guard) => guard
            .iter()
            .map(|(name, state)| {
                let (lifecycle, ready, message) = registry_entry_status(state);
                (
                    name.clone(),
                    (
                        lifecycle,
                        ready,
                        message,
                        registry_entry_artifacts(state).clone(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>(),
        Err(_) => {
            return ServerMessage::Error(ProtocolFailure {
                code: ProtocolErrorCode::ServerError,
                message: "database registry is poisoned".to_string(),
            })
        }
    };

    if let Some(name) = requested_database {
        if let Some((lifecycle, ready, message, artifacts)) = registry_snapshot.get(name).cloned() {
            statuses.push(database_status_for(
                name, lifecycle, ready, message, artifacts,
            ));
        } else {
            let database_dir = databases_root.join(name);
            if database_dir.exists() {
                statuses.push(DatabaseStatus {
                    name: name.to_string(),
                    lifecycle: ServerLifecycleState::Starting,
                    ready: false,
                    message: "database exists on disk but is not opened in this server process; select it to start open/recovery".to_string(),
                    artifacts: unavailable_artifact_summary(),
                });
            } else {
                return ServerMessage::Error(ProtocolFailure {
                    code: ProtocolErrorCode::DatabaseNotFound,
                    message: format!("database `{name}` does not exist"),
                });
            }
        }
    } else {
        for (name, (lifecycle, ready, message, artifacts)) in &registry_snapshot {
            statuses.push(database_status_for(
                name,
                *lifecycle,
                *ready,
                message.clone(),
                artifacts.clone(),
            ));
        }
        for name in list_disk_database_names(&databases_root) {
            if registry_snapshot.contains_key(&name) {
                continue;
            }
            statuses.push(DatabaseStatus {
                name,
                lifecycle: ServerLifecycleState::Starting,
                ready: false,
                message: "database exists on disk but has not been opened in this server process"
                    .to_string(),
                artifacts: unavailable_artifact_summary(),
            });
        }
        statuses.sort_by(|left, right| left.name.cmp(&right.name));
    }

    let lifecycle = if requested_database.is_some() {
        statuses
            .first()
            .map(|status| status.lifecycle)
            .unwrap_or(ServerLifecycleState::Ready)
    } else if statuses
        .iter()
        .any(|status| status.lifecycle == ServerLifecycleState::Emergency)
    {
        ServerLifecycleState::Emergency
    } else if statuses
        .iter()
        .any(|status| status.lifecycle == ServerLifecycleState::Opening)
    {
        ServerLifecycleState::Recovering
    } else {
        ServerLifecycleState::Ready
    };
    let ready = if requested_database.is_some() {
        statuses.iter().all(|status| status.ready)
    } else {
        lifecycle == ServerLifecycleState::Ready
    };
    let message = if ready {
        if requested_database.is_some() {
            "requested database is ready".to_string()
        } else if statuses
            .iter()
            .any(|status| status.lifecycle == ServerLifecycleState::Ready && !status.ready)
        {
            "server is accepting sessions; one or more databases are in restricted plugin diagnostic mode"
                .to_string()
        } else {
            "server is accepting sessions".to_string()
        }
    } else if statuses
        .iter()
        .any(|status| status.lifecycle == ServerLifecycleState::Emergency)
    {
        "one or more databases failed to open or recover".to_string()
    } else if statuses
        .iter()
        .any(|status| status.lifecycle == ServerLifecycleState::Opening)
    {
        "one or more databases are opening/recovering; retry later".to_string()
    } else if requested_database.is_some() {
        "requested database is not ready".to_string()
    } else {
        "server is accepting sessions; some disk databases have not been opened".to_string()
    };

    ServerMessage::ServerStatus(Box::new(ServerStatus {
        build: include_build_identity.then(build_identity),
        lifecycle,
        ready,
        message,
        databases: statuses,
        runtime: crate::protocol::ServerRuntimeStatus {
            open_databases: registry_snapshot
                .values()
                .filter(|(lifecycle, ready, _, _)| {
                    *lifecycle == ServerLifecycleState::Ready && *ready
                })
                .count() as u64,
            retained_databases: registry_snapshot.len() as u64,
            max_databases: config.max_databases as u64,
            active_connections: runtime.active_connections.load(Ordering::Acquire) as u64,
            max_connections: config.max_connections as u64,
            inflight_frame_bytes: runtime.inflight_frame_bytes.load(Ordering::Acquire) as u64,
            max_inflight_frame_bytes: config.max_inflight_frame_bytes as u64,
            job_scheduler_cycles: runtime.job_scheduler.cycles.load(Ordering::Acquire),
            job_attempts_started: runtime
                .job_scheduler
                .attempts_started
                .load(Ordering::Acquire),
            job_attempts_succeeded: runtime
                .job_scheduler
                .attempts_succeeded
                .load(Ordering::Acquire),
            job_attempts_failed: runtime
                .job_scheduler
                .attempts_failed
                .load(Ordering::Acquire),
            job_attempts_active: runtime
                .job_scheduler
                .active_attempts
                .load(Ordering::Acquire),
            job_scheduler_last_error: runtime
                .job_scheduler
                .last_error
                .lock()
                .ok()
                .and_then(|error| error.clone()),
        },
    }))
}

fn registry_entry_status(entry: &DatabaseRegistryEntry) -> (ServerLifecycleState, bool, String) {
    match entry {
        DatabaseRegistryEntry::Opening { .. } => (
            ServerLifecycleState::Opening,
            false,
            "database is opening/recovering; retry later".to_string(),
        ),
        DatabaseRegistryEntry::Failed { error, .. } => (
            ServerLifecycleState::Emergency,
            false,
            format!("database open/recovery failed: {error}"),
        ),
        DatabaseRegistryEntry::Ready { database, .. } => match database.runtime_state() {
            DatabaseRuntimeState::Closed => (
                ServerLifecycleState::Starting,
                false,
                "database engine is closed".to_string(),
            ),
            DatabaseRuntimeState::Opening => (
                ServerLifecycleState::Opening,
                false,
                "database is opening/recovering; retry later".to_string(),
            ),
            DatabaseRuntimeState::Ready => match database.plugin_admission_diagnostic() {
                Ok(None) => (
                    ServerLifecycleState::Ready,
                    true,
                    "database is ready".to_string(),
                ),
                Ok(Some(diagnostic)) => (ServerLifecycleState::Ready, false, diagnostic),
                Err(error) => (
                    ServerLifecycleState::Emergency,
                    false,
                    format!("database plugin admission failed: {error}"),
                ),
            },
            DatabaseRuntimeState::Closing => (
                ServerLifecycleState::Opening,
                false,
                "database is closing".to_string(),
            ),
            DatabaseRuntimeState::CloseFailed(error) => (
                ServerLifecycleState::Emergency,
                false,
                format!("database close failed: {error}"),
            ),
            DatabaseRuntimeState::Failed(error) => (
                ServerLifecycleState::Emergency,
                false,
                format!("database open/recovery failed: {error}"),
            ),
        },
    }
}

fn registry_entry_artifacts(entry: &DatabaseRegistryEntry) -> &DatabaseArtifactSummary {
    match entry {
        DatabaseRegistryEntry::Opening { artifacts }
        | DatabaseRegistryEntry::Ready { artifacts, .. }
        | DatabaseRegistryEntry::Failed { artifacts, .. } => artifacts,
    }
}

fn database_status_for(
    name: &str,
    lifecycle: ServerLifecycleState,
    ready: bool,
    message: String,
    artifacts: DatabaseArtifactSummary,
) -> DatabaseStatus {
    DatabaseStatus {
        name: name.to_string(),
        lifecycle,
        ready,
        message,
        artifacts,
    }
}

pub(super) fn unavailable_artifact_summary() -> DatabaseArtifactSummary {
    DatabaseArtifactSummary {
        complete: false,
        ..DatabaseArtifactSummary::default()
    }
}

fn list_disk_database_names(databases_root: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(databases_root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            entry.file_name().to_str().map(str::to_string)
        })
        .collect()
}

pub(super) fn collect_database_artifacts(
    database_dir: &std::path::Path,
) -> DatabaseArtifactSummary {
    collect_database_artifacts_with_limits(
        database_dir,
        ARTIFACT_STATUS_MAX_ENTRIES,
        ARTIFACT_STATUS_MAX_DEPTH,
    )
}

pub(super) fn collect_database_artifacts_with_limits(
    database_dir: &std::path::Path,
    max_entries: u64,
    max_depth: usize,
) -> DatabaseArtifactSummary {
    #[cfg(test)]
    {
        let hook = ARTIFACT_SCAN_TEST_HOOK
            .lock()
            .expect("artifact scan test hook lock")
            .clone();
        if let Some(hook) = hook {
            hook(database_dir);
        }
    }
    let mut summary = DatabaseArtifactSummary {
        complete: true,
        sequence: ARTIFACT_STATUS_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        sampled_unix_millis: chrono::Utc::now().timestamp_millis().max(0) as u64,
        ..DatabaseArtifactSummary::default()
    };
    let mut table_names = BTreeSet::new();
    {
        let mut traversal = ArtifactTraversal {
            max_entries,
            max_depth,
            table_names: &mut table_names,
            summary: &mut summary,
        };
        collect_database_artifacts_recursive(database_dir, true, false, 0, &mut traversal);
    }
    summary.table_dirs = u64::try_from(table_names.len()).unwrap_or(u64::MAX);
    summary
}

struct ArtifactTraversal<'a> {
    max_entries: u64,
    max_depth: usize,
    table_names: &'a mut BTreeSet<String>,
    summary: &'a mut DatabaseArtifactSummary,
}

fn collect_database_artifacts_recursive(
    path: &std::path::Path,
    is_root: bool,
    in_snapshot: bool,
    depth: usize,
    traversal: &mut ArtifactTraversal<'_>,
) {
    if depth > traversal.max_depth {
        traversal.summary.complete = false;
        traversal.summary.truncated = true;
        return;
    }
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => {
            record_artifact_scan_error(traversal.summary);
            return;
        }
    };
    for entry in entries {
        if traversal.summary.entries_visited >= traversal.max_entries {
            traversal.summary.complete = false;
            traversal.summary.truncated = true;
            return;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                record_artifact_scan_error(traversal.summary);
                continue;
            }
        };
        traversal.summary.entries_visited = traversal.summary.entries_visited.saturating_add(1);
        let entry_path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => {
                record_artifact_scan_error(traversal.summary);
                continue;
            }
        };
        if file_type.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            let is_table_manifest_root = path
                .file_name()
                .and_then(|component| component.to_str())
                .is_some_and(|component| component.eq_ignore_ascii_case("tables"))
                && path
                    .parent()
                    .and_then(std::path::Path::file_name)
                    .and_then(|component| component.to_str())
                    .is_some_and(|component| component.eq_ignore_ascii_case("manifests"));
            if is_table_manifest_root {
                traversal.table_names.insert(name.clone());
            }
            let child_in_snapshot =
                in_snapshot || (is_root && name.eq_ignore_ascii_case("snapshots"));
            if child_in_snapshot {
                traversal.summary.complete = false;
                traversal.summary.snapshots_omitted = true;
                continue;
            }
            collect_database_artifacts_recursive(
                &entry_path,
                false,
                child_in_snapshot,
                depth.saturating_add(1),
                traversal,
            );
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if in_snapshot {
            traversal.summary.snapshot_files = traversal.summary.snapshot_files.saturating_add(1);
        } else if name.starts_with("wal-") && name.ends_with(".log") {
            traversal.summary.wal_files = traversal.summary.wal_files.saturating_add(1);
        } else if name.ends_with(".data") || name.ends_with(".idx") {
            traversal.summary.artifact_files = traversal.summary.artifact_files.saturating_add(1);
        } else if matches!(name.as_str(), "control.0" | "control.1") {
            traversal.summary.checkpoint_files =
                traversal.summary.checkpoint_files.saturating_add(1);
        } else if name.ends_with(".mft") || name.ends_with(".cat") {
            traversal.summary.manifest_files = traversal.summary.manifest_files.saturating_add(1);
        } else {
            traversal.summary.other_files = traversal.summary.other_files.saturating_add(1);
        }
    }
}

fn record_artifact_scan_error(summary: &mut DatabaseArtifactSummary) {
    summary.complete = false;
    summary.scan_errors = summary.scan_errors.saturating_add(1);
}
