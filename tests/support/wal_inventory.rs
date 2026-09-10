use std::path::Path;

/// Print only stable filesystem facts about a WAL directory.
///
/// Integration tests must not duplicate the private WAL decoder. Canonical
/// validation belongs to the storage recovery owner; these persistence tests
/// prove it by reopening the database and checking semantic state.
pub fn print(directory: &Path) {
    if !directory.exists() {
        eprintln!("WAL directory is absent: {}", directory.display());
        return;
    }

    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let metadata = entry.metadata().unwrap();
        eprintln!(
            "WAL member: {:?} ({} bytes, file={})",
            entry.file_name(),
            metadata.len(),
            metadata.is_file()
        );
    }
}
