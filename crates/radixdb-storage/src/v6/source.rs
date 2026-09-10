use std::collections::{HashMap, VecDeque};
use std::fs::{File, Metadata};
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use parking_lot::{Condvar, Mutex};

use super::{FormatError, FormatResult};

/// Runtime artifact readers share a small process-wide descriptor corridor.
///
/// DATA and INDEX artifacts are immutable, and one database can legitimately
/// expose hundreds of them. Keeping one descriptor in every runtime source
/// makes descriptor use grow with schema size. Thirty-two cached descriptors
/// preserve locality for active tables while leaving the rest of the process
/// descriptor table available to WAL, sockets, publication and diagnostics.
const MAX_CACHED_ARTIFACT_DESCRIPTORS: usize = 32;

pub trait ArtifactSource {
    fn byte_length(&self) -> FormatResult<u64>;

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()>;
}

impl ArtifactSource for [u8] {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        let start = usize::try_from(offset).map_err(|_| FormatError::InvalidReference {
            owner: "artifact source",
            detail: "source offset does not fit this platform",
        })?;
        let end = start
            .checked_add(destination.len())
            .ok_or(FormatError::InvalidReference {
                owner: "artifact source",
                detail: "source range overflows",
            })?;
        let source = self.get(start..end).ok_or(FormatError::InvalidReference {
            owner: "artifact source",
            detail: "source range is outside artifact",
        })?;
        destination.copy_from_slice(source);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArtifactFileIdentity {
    byte_length: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    modified_nanos: u128,
}

impl ArtifactFileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Self {
                byte_length: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(not(unix))]
        {
            use std::time::UNIX_EPOCH;

            let modified_nanos = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            Self {
                byte_length: metadata.len(),
                modified_nanos,
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArtifactFileKey {
    path: PathBuf,
    identity: ArtifactFileIdentity,
}

struct CachedArtifactFile {
    file: Mutex<File>,
}

struct DescriptorEntry {
    file: Arc<CachedArtifactFile>,
    active_leases: usize,
}

#[derive(Default)]
struct DescriptorPoolState {
    owners: HashMap<ArtifactFileKey, usize>,
    entries: HashMap<ArtifactFileKey, DescriptorEntry>,
    /// Least recently used key is at the front.
    recency: VecDeque<ArtifactFileKey>,
}

struct ArtifactDescriptorPool {
    state: Mutex<DescriptorPoolState>,
    descriptor_available: Condvar,
}

static ARTIFACT_DESCRIPTOR_POOL: LazyLock<ArtifactDescriptorPool> =
    LazyLock::new(ArtifactDescriptorPool::new);

impl ArtifactDescriptorPool {
    fn new() -> Self {
        Self {
            state: Mutex::new(DescriptorPoolState::default()),
            descriptor_available: Condvar::new(),
        }
    }

    fn register(&self, key: ArtifactFileKey, file: File) {
        let mut state = self.state.lock();
        *state.owners.entry(key.clone()).or_default() += 1;
        if state.entries.contains_key(&key) {
            touch(&mut state.recency, &key);
            return;
        }
        self.wait_for_slot(&mut state);
        state.entries.insert(
            key.clone(),
            DescriptorEntry {
                file: Arc::new(CachedArtifactFile {
                    file: Mutex::new(file),
                }),
                active_leases: 0,
            },
        );
        touch(&mut state.recency, &key);
    }

    fn unregister(&self, key: &ArtifactFileKey) {
        let mut state = self.state.lock();
        let remove_owner = match state.owners.get_mut(key) {
            Some(owners) if *owners > 1 => {
                *owners -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };
        if !remove_owner {
            return;
        }
        state.owners.remove(key);
        let can_close = state
            .entries
            .get(key)
            .map(|entry| entry.active_leases == 0)
            .unwrap_or(false);
        if can_close {
            state.entries.remove(key);
            remove_recency(&mut state.recency, key);
            self.descriptor_available.notify_all();
        }
    }

    fn acquire(&'static self, key: &ArtifactFileKey) -> FormatResult<ArtifactDescriptorLease> {
        let mut state = self.state.lock();
        if !state.owners.contains_key(key) {
            return Err(FormatError::InvalidReference {
                owner: "artifact source",
                detail: "artifact descriptor has no live runtime owner",
            });
        }

        if let Some(entry) = state.entries.get_mut(key) {
            entry.active_leases += 1;
            let file = Arc::clone(&entry.file);
            touch(&mut state.recency, key);
            return Ok(ArtifactDescriptorLease {
                pool: self,
                key: key.clone(),
                file,
            });
        }

        self.wait_for_slot(&mut state);
        let file = File::open(&key.path).map_err(|error| io_error("open", error))?;
        crate::instrumentation::record_artifact_file_open();
        let metadata = file
            .metadata()
            .map_err(|error| io_error("metadata", error))?;
        crate::instrumentation::record_artifact_file_stat();
        let identity = ArtifactFileIdentity::from_metadata(&metadata);
        if identity != key.identity {
            return Err(FormatError::InvalidReference {
                owner: "artifact source",
                detail: "artifact file identity changed after metadata open",
            });
        }
        crate::instrumentation::record_artifact_file_identity_check();

        let file = Arc::new(CachedArtifactFile {
            file: Mutex::new(file),
        });
        state.entries.insert(
            key.clone(),
            DescriptorEntry {
                file: Arc::clone(&file),
                active_leases: 1,
            },
        );
        touch(&mut state.recency, key);
        Ok(ArtifactDescriptorLease {
            pool: self,
            key: key.clone(),
            file,
        })
    }

    fn release(&self, key: &ArtifactFileKey) {
        let mut state = self.state.lock();
        let mut remove = false;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.active_leases = entry.active_leases.saturating_sub(1);
            remove = entry.active_leases == 0 && !state.owners.contains_key(key);
        }
        if remove {
            state.entries.remove(key);
            remove_recency(&mut state.recency, key);
        }
        self.descriptor_available.notify_all();
    }

    fn wait_for_slot(&self, state: &mut parking_lot::MutexGuard<'_, DescriptorPoolState>) {
        while state.entries.len() >= MAX_CACHED_ARTIFACT_DESCRIPTORS {
            if evict_one_inactive(state) {
                continue;
            }
            self.descriptor_available.wait(state);
        }
    }
}

struct ArtifactDescriptorLease {
    pool: &'static ArtifactDescriptorPool,
    key: ArtifactFileKey,
    file: Arc<CachedArtifactFile>,
}

impl Drop for ArtifactDescriptorLease {
    fn drop(&mut self) {
        self.pool.release(&self.key);
    }
}

fn evict_one_inactive(state: &mut DescriptorPoolState) -> bool {
    let Some(position) = state.recency.iter().position(|key| {
        state
            .entries
            .get(key)
            .map(|entry| entry.active_leases == 0)
            .unwrap_or(false)
    }) else {
        return false;
    };
    let key = state
        .recency
        .remove(position)
        .expect("descriptor recency position disappeared");
    state.entries.remove(&key);
    true
}

fn touch(recency: &mut VecDeque<ArtifactFileKey>, key: &ArtifactFileKey) {
    remove_recency(recency, key);
    recency.push_back(key.clone());
}

fn remove_recency(recency: &mut VecDeque<ArtifactFileKey>, key: &ArtifactFileKey) {
    if let Some(position) = recency.iter().position(|candidate| candidate == key) {
        recency.remove(position);
    }
}

enum ArtifactFileBacking {
    Pooled(ArtifactFileKey),
    Owned(Mutex<File>),
}

pub struct ArtifactFile {
    backing: ArtifactFileBacking,
    byte_length: u64,
}

impl ArtifactFile {
    pub fn open(path: impl AsRef<Path>) -> FormatResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path).map_err(|error| io_error("open", error))?;
        crate::instrumentation::record_artifact_descriptor_open();
        let metadata = file
            .metadata()
            .map_err(|error| io_error("metadata", error))?;
        crate::instrumentation::record_artifact_file_stat();
        let identity = ArtifactFileIdentity::from_metadata(&metadata);
        crate::instrumentation::record_artifact_file_identity_check();
        let byte_length = identity.byte_length;
        let key = ArtifactFileKey { path, identity };
        ARTIFACT_DESCRIPTOR_POOL.register(key.clone(), file);
        Ok(Self {
            backing: ArtifactFileBacking::Pooled(key),
            byte_length,
        })
    }

    pub fn from_file(file: File) -> FormatResult<Self> {
        let byte_length = file
            .metadata()
            .map_err(|error| io_error("metadata", error))?
            .len();
        Ok(Self {
            backing: ArtifactFileBacking::Owned(Mutex::new(file)),
            byte_length,
        })
    }
}

impl Drop for ArtifactFile {
    fn drop(&mut self) {
        if let ArtifactFileBacking::Pooled(key) = &self.backing {
            ARTIFACT_DESCRIPTOR_POOL.unregister(key);
        }
    }
}

impl ArtifactSource for ArtifactFile {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.byte_length)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        let started = std::time::Instant::now();
        match &self.backing {
            ArtifactFileBacking::Pooled(key) => {
                let lease = ARTIFACT_DESCRIPTOR_POOL.acquire(key)?;
                read_exact(&lease.file.file, offset, destination)?;
            }
            ArtifactFileBacking::Owned(file) => read_exact(file, offset, destination)?,
        }
        crate::instrumentation::record_volume_read(destination.len() as u64, started.elapsed());
        Ok(())
    }
}

fn read_exact(file: &Mutex<File>, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
    let mut file = file.lock();
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| io_error("seek", error))?;
    file.read_exact(destination)
        .map_err(|error| io_error("read", error))
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::ArtifactIo {
        operation,
        kind: error.kind(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_pool_is_bounded_and_evicted_sources_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let mut sources = Vec::new();
        for ordinal in 0..MAX_CACHED_ARTIFACT_DESCRIPTORS + 8 {
            let path = directory.path().join(format!("artifact-{ordinal}.data"));
            std::fs::write(&path, [ordinal as u8; 16]).unwrap();
            let source = ArtifactFile::open(path).unwrap();
            let mut first = [0_u8; 1];
            source.read_exact_at(0, &mut first).unwrap();
            assert_eq!(first[0], ordinal as u8);
            sources.push(source);
        }

        assert!(
            ARTIFACT_DESCRIPTOR_POOL.state.lock().entries.len() <= MAX_CACHED_ARTIFACT_DESCRIPTORS
        );
        let mut first = [0_u8; 1];
        sources[0].read_exact_at(0, &mut first).unwrap();
        assert_eq!(first[0], 0);
    }

    #[test]
    fn descriptor_reopen_rejects_replaced_file_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("replace.data");
        std::fs::write(&path, b"old artifact").unwrap();
        let source = ArtifactFile::open(&path).unwrap();

        {
            let mut state = ARTIFACT_DESCRIPTOR_POOL.state.lock();
            if let ArtifactFileBacking::Pooled(key) = &source.backing {
                state.entries.remove(key);
                remove_recency(&mut state.recency, key);
            }
        }
        let replacement = directory.path().join("replacement.data");
        std::fs::write(&replacement, b"new artifact").unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::rename(replacement, &path).unwrap();

        let mut byte = [0_u8; 1];
        assert!(matches!(
            source.read_exact_at(0, &mut byte),
            Err(FormatError::InvalidReference {
                owner: "artifact source",
                detail: "artifact file identity changed after metadata open"
            })
        ));
    }
}
