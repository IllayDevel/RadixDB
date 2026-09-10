use super::*;

impl WALManager {
    /// Create a new WAL manager with default config
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn new(path: impl AsRef<Path>, sync_mode: SyncMode) -> Result<Self> {
        Self::with_config(path, sync_mode, None)
    }

    pub(super) fn canonical_filename(generation: u64) -> String {
        format!("wal-{generation:016x}.log")
    }

    pub(super) fn parse_canonical_generation(filename: &str) -> Option<u64> {
        let encoded = filename.strip_prefix("wal-")?.strip_suffix(".log")?;
        (encoded.len() == 16 && encoded.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| u64::from_str_radix(encoded, 16).ok())
            .flatten()
            .filter(|generation| *generation != 0)
    }

    pub(super) fn validate_retired_directory(path: &Path, floor_generation: u64) -> Result<()> {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            Error::internal(format!(
                "failed to inspect retired WAL directory {}: {}",
                path.display(),
                error
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::internal(format!(
                "retired WAL path {} is not a regular directory",
                path.display()
            )));
        }
        for entry in fs::read_dir(path).map_err(|error| {
            Error::internal(format!(
                "failed to enumerate retired WAL directory {}: {}",
                path.display(),
                error
            ))
        })? {
            let entry = entry.map_err(|error| {
                Error::internal(format!("failed to read retired WAL entry: {}", error))
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            let generation = Self::parse_canonical_generation(&name).ok_or_else(|| {
                Error::internal(format!(
                    "non-canonical file '{}' exists in retired WAL directory",
                    name
                ))
            })?;
            if generation >= floor_generation {
                return Err(Error::internal(format!(
                    "retired WAL generation {} is not below selected floor {}",
                    generation, floor_generation
                )));
            }
            let entry_type = entry.file_type().map_err(|error| {
                Error::internal(format!(
                    "failed to inspect retired WAL entry '{}': {}",
                    name, error
                ))
            })?;
            if entry_type.is_symlink() || !entry_type.is_file() {
                return Err(Error::internal(format!(
                    "retired WAL entry '{}' is not a regular file",
                    name
                )));
            }
        }
        Ok(())
    }

    pub(super) fn sync_directory(path: &Path) -> Result<()> {
        #[cfg(not(windows))]
        {
            File::open(path)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    Error::internal(format!(
                        "failed to sync WAL directory {}: {}",
                        path.display(),
                        error
                    ))
                })?;
        }
        Ok(())
    }

    pub(super) fn remove_generation_durably(path: &Path) -> Result<()> {
        if path.exists() {
            fs::remove_file(path).map_err(|error| {
                Error::internal(format!(
                    "failed to remove WAL generation {}: {}",
                    path.display(),
                    error
                ))
            })?;
            let directory = path.parent().ok_or_else(|| {
                Error::internal(format!("WAL generation {} has no parent", path.display()))
            })?;
            Self::sync_directory(directory)?;
        }
        Ok(())
    }

    pub(super) fn validate_generation(
        path: PathBuf,
        sequence: u64,
        start_lsn: u64,
    ) -> Result<ValidatedWalGeneration> {
        let started = Instant::now();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::internal("WAL generation has a non-UTF-8 filename"))?
            .to_string();
        if Self::parse_canonical_generation(&name) != Some(sequence) {
            return Err(Error::internal(format!(
                "WAL generation {} is not the canonical file for generation {}",
                name, sequence
            )));
        }
        let identity_before = WalFileIdentity::read(&path)?;

        let mut reader = ValidatedWalReader::open(&path)?;
        let mut end_lsn = start_lsn;
        let mut first_record = true;
        while let Some(entry) = reader.next_entry()? {
            if entry.lsn <= start_lsn {
                return Err(Error::internal(format!(
                    "WAL generation {} contains LSN {} at or below its start boundary {}",
                    name, entry.lsn, start_lsn
                )));
            }
            if first_record {
                if entry.previous_lsn != start_lsn {
                    return Err(Error::internal(format!(
                        "WAL generation {} first record LSN {} chains to {}, expected start boundary {}",
                        name, entry.lsn, entry.previous_lsn, start_lsn
                    )));
                }
                first_record = false;
            }
            end_lsn = entry.lsn;
        }

        let identity = WalFileIdentity::read(&path)?;
        if identity != identity_before {
            return Err(Error::internal(format!(
                "WAL generation {} changed while it was being validated",
                path.display()
            )));
        }
        instrumentation::record_wal_generation_validation(identity.len, started.elapsed());

        Ok(ValidatedWalGeneration {
            path,
            name,
            start_lsn,
            end_lsn,
            sequence,
            identity,
        })
    }

    pub(super) fn collect_validated_generations(
        wal_dir: &Path,
        floor_generation: u64,
        floor_lsn: u64,
    ) -> Result<Vec<ValidatedWalGeneration>> {
        let entries = fs::read_dir(wal_dir).map_err(|error| {
            Error::internal(format!(
                "failed to enumerate WAL directory {}: {}",
                wal_dir.display(),
                error
            ))
        })?;
        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                Error::internal(format!("failed to read WAL directory entry: {}", error))
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "retired" {
                Self::validate_retired_directory(&entry.path(), floor_generation)?;
                continue;
            }
            if let Some(generation) = Self::parse_canonical_generation(&name) {
                if generation >= floor_generation {
                    paths.push((generation, entry.path()));
                }
            } else {
                return Err(Error::internal(format!(
                    "non-canonical file '{}' exists in WAL directory",
                    name
                )));
            }
        }
        paths.sort_unstable_by_key(|(generation, _)| *generation);
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        if paths.first().map(|(generation, _)| *generation) != Some(floor_generation) {
            return Err(Error::internal(format!(
                "WAL generation {} selected by CONTROL is missing",
                floor_generation
            )));
        }

        let mut generations = Vec::with_capacity(paths.len());
        let mut expected_generation = floor_generation;
        let mut start_lsn = floor_lsn;
        for (generation, path) in paths {
            if generation != expected_generation {
                return Err(Error::internal(format!(
                    "missing WAL generation between {} and {}",
                    expected_generation.saturating_sub(1),
                    generation
                )));
            }
            let validated = Self::validate_generation(path, generation, start_lsn)?;
            start_lsn = validated.end_lsn;
            expected_generation = expected_generation
                .checked_add(1)
                .ok_or_else(|| Error::internal("WAL generation domain exhausted"))?;
            generations.push(validated);
        }

        Ok(generations)
    }

    pub(super) fn validate_required_generation_suffix(
        generations: &[ValidatedWalGeneration],
        replay_floor: u64,
    ) -> Result<()> {
        let Some(first_required) = generations
            .iter()
            .position(|generation| generation.end_lsn >= replay_floor)
        else {
            return if generations.is_empty() && replay_floor == 0 {
                Ok(())
            } else {
                Err(Error::internal(format!(
                    "WAL has no generation covering required replay floor {replay_floor}"
                )))
            };
        };

        let first = &generations[first_required];
        if first.start_lsn > replay_floor {
            return Err(Error::internal(format!(
                "WAL generation {} starts at {}, after required replay floor {}",
                first.name, first.start_lsn, replay_floor
            )));
        }

        for pair in generations[first_required..].windows(2) {
            let previous = &pair[0];
            let next = &pair[1];
            if previous.end_lsn != next.start_lsn {
                return Err(Error::internal(format!(
                    "missing WAL generation between {} ending at {} and {} starting at {}",
                    previous.name, previous.end_lsn, next.name, next.start_lsn
                )));
            }
        }
        Ok(())
    }

    /// Create a new WAL manager with custom config
    ///
    /// This allows configuring:
    /// - `sync_interval_ms`: Minimum time between syncs in milliseconds (SyncNormal mode)
    /// - `wal_flush_trigger`: Buffer size that triggers a flush
    /// - `wal_buffer_size`: Initial buffer size
    /// - `wal_max_size`: Maximum WAL file size before rotation
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn with_config(
        path: impl AsRef<Path>,
        sync_mode: SyncMode,
        config: Option<&PersistenceConfig>,
    ) -> Result<Self> {
        let floor = crate::v6::WalReplayFloor::new(
            crate::v6::WalGeneration::new(1).map_err(|error| Error::internal(error.to_string()))?,
            0,
        );
        Self::with_replay_floor(path, sync_mode, config, floor)
    }

    pub(crate) fn with_replay_floor(
        path: impl AsRef<Path>,
        sync_mode: SyncMode,
        config: Option<&PersistenceConfig>,
        floor: crate::v6::WalReplayFloor,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create WAL directory if it doesn't exist
        fs::create_dir_all(&path)
            .map_err(|e| Error::internal(format!("failed to create WAL directory: {}", e)))?;

        let mut wal_file: Option<File> = None;
        let mut initial_lsn = floor.lsn();
        let initial_checkpoint_lsn = floor.lsn();
        let initial_transaction_high_water = 0i64;
        let mut wal_filename = String::new();
        let generations =
            Self::collect_validated_generations(&path, floor.generation().get(), floor.lsn())?;
        Self::validate_required_generation_suffix(&generations, floor.lsn())?;
        let initial_sequence = generations
            .iter()
            .map(|generation| generation.sequence)
            .max()
            .unwrap_or(floor.generation().get());
        if let Some(newest) = generations.last() {
            wal_filename = newest.name.clone();
            initial_lsn = newest.end_lsn;
            wal_file = Some(
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&newest.path)
                    .map_err(|error| {
                        Error::internal(format!(
                            "failed to open validated WAL generation {}: {}",
                            newest.path.display(),
                            error
                        ))
                    })?,
            );
        }

        // Create new WAL file if none exists
        if wal_file.is_none() {
            wal_filename = Self::canonical_filename(floor.generation().get());
            let wal_path = path.join(&wal_filename);

            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&wal_path)
                .map_err(|e| Error::internal(format!("failed to create WAL file: {}", e)))?;

            wal_file = Some(file);
            Self::sync_directory(&path)?;
        }

        if initial_checkpoint_lsn > initial_lsn {
            return Err(Error::internal(format!(
                "checkpoint LSN {} is ahead of validated WAL LSN {}",
                initial_checkpoint_lsn, initial_lsn
            )));
        }

        // Extract config values with defaults
        let (sync_interval_nanos, flush_trigger, buffer_size, max_wal_size) =
            if let Some(cfg) = config {
                (
                    (cfg.sync_interval_ms as u64).saturating_mul(1_000_000),
                    cfg.wal_flush_trigger as u64,
                    cfg.wal_buffer_size,
                    cfg.wal_max_size as u64,
                )
            } else {
                (
                    10_000_000, // 10ms in nanoseconds
                    DEFAULT_WAL_FLUSH_TRIGGER,
                    DEFAULT_WAL_BUFFER_SIZE,
                    DEFAULT_WAL_MAX_SIZE,
                )
            };
        let sync_clock_origin = Instant::now();

        // Get initial file position if we have an existing WAL file
        let initial_file_position = if let Some(ref file) = wal_file {
            file.metadata().map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };
        let validated_closed_generations = generations
            .iter()
            .take(generations.len().saturating_sub(1))
            .cloned()
            .collect();

        Ok(Self {
            path,
            wal_file: Mutex::new(wal_file),
            current_wal_file: Mutex::new(wal_filename),
            current_lsn: AtomicU64::new(initial_lsn),
            previous_lsn: AtomicU64::new(initial_lsn),
            buffer: Mutex::new(Vec::with_capacity(buffer_size)),
            flush_trigger,
            max_wal_size,
            last_checkpoint: AtomicU64::new(initial_checkpoint_lsn),
            transaction_high_water: AtomicI64::new(initial_transaction_high_water),
            sync_mode,
            running: AtomicBool::new(true),
            transition: Mutex::new(WalTransitionState {
                lifecycle: WalLifecycle::Running,
            }),
            sync_clock_origin,
            last_sync_elapsed_nanos: AtomicU64::new(0),
            sync_interval_nanos,
            current_file_position: AtomicU64::new(initial_file_position),
            last_synced_file_position: AtomicU64::new(initial_file_position),
            wal_sequence: AtomicU64::new(initial_sequence),
            replay_floor: Mutex::new(floor),
            validated_closed_generations: Mutex::new(validated_closed_generations),
            #[cfg(any(test, feature = "test-hooks"))]
            runtime_generation_validation_bytes: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            append_test_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            append_test_hook_owner: Mutex::new(()),
            #[cfg(test)]
            close_test_hook: Mutex::new(None),
            #[cfg(test)]
            close_test_hook_owner: Mutex::new(()),
        })
    }
}
