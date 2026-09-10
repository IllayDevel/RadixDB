use super::*;

impl MVCCEngine {
    /// Cleanup old transactions that have been idle for too long
    pub fn cleanup_old_transactions(&self, max_age: std::time::Duration) -> i32 {
        if !self.is_open() {
            return 0;
        }
        self.registry.cleanup_old_transactions(max_age)
    }

    /// Cleanup deleted rows older than retention period from all tables
    pub fn cleanup_deleted_rows(&self, max_age: std::time::Duration) -> i32 {
        if !self.is_open() {
            return 0;
        }

        let stores = self.version_stores.read().unwrap();
        let mut total_removed = 0;

        for store in stores.values() {
            total_removed += store.cleanup_deleted_rows(max_age);
        }

        total_removed
    }

    /// Cleanup old previous versions that are no longer needed from all tables
    pub fn cleanup_old_previous_versions(&self) -> i32 {
        if !self.is_open() {
            return 0;
        }

        let stores = self.version_stores.read().unwrap();
        let mut total_cleaned = 0;

        for store in stores.values() {
            total_cleaned += store.cleanup_old_previous_versions();
        }

        total_cleaned
    }

    /// Manual VACUUM: cleanup deleted rows, old versions, and stale transactions.
    ///
    /// When `table_name` is `Some`, only that table is vacuumed.
    /// Returns `(deleted_rows_cleaned, old_versions_cleaned, transactions_cleaned)`.
    pub fn vacuum(
        &self,
        table_name: Option<&str>,
        retention: std::time::Duration,
    ) -> radixdb_core::Result<(i32, i32, i32)> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let txn_cleaned = self.cleanup_old_transactions(retention);

        let stores = self.version_stores.read().unwrap();

        let mut rows_cleaned = 0;
        let mut versions_cleaned = 0;

        if let Some(name) = table_name {
            if let Some(store) = stores.get(name) {
                rows_cleaned += store.cleanup_deleted_rows(retention);
                versions_cleaned += store.cleanup_old_previous_versions_with_retention(retention);
            } else {
                return Err(radixdb_core::Error::TableNotFound(name.to_string()));
            }
        } else {
            for store in stores.values() {
                rows_cleaned += store.cleanup_deleted_rows(retention);
                versions_cleaned += store.cleanup_old_previous_versions_with_retention(retention);
            }
        }

        drop(stores);

        Ok((rows_cleaned, versions_cleaned, txn_cleaned))
    }

    /// Start periodic cleanup of old transactions and deleted rows
    ///
    /// Returns a handle that can be used to stop the cleanup thread.
    pub fn start_periodic_cleanup(
        self: &Arc<Self>,
        interval: std::time::Duration,
        max_age: std::time::Duration,
    ) -> CleanupHandle {
        use std::sync::atomic::AtomicBool;
        use std::thread;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = Arc::clone(&stop_flag);
        let engine = Arc::clone(self);

        let handle = thread::spawn(move || {
            while !stop_flag_clone.load(Ordering::Acquire) {
                // Sleep for the interval (check stop flag periodically)
                let check_interval = std::time::Duration::from_millis(100);
                let mut elapsed = std::time::Duration::ZERO;
                while elapsed < interval && !stop_flag_clone.load(Ordering::Acquire) {
                    thread::sleep(check_interval);
                    elapsed += check_interval;
                }

                if stop_flag_clone.load(Ordering::Acquire) {
                    break;
                }

                // Perform cleanup
                let _txn_count = engine.cleanup_old_transactions(max_age);
                let _row_count = engine.cleanup_deleted_rows(max_age);
                let _prev_version_count = engine.cleanup_old_previous_versions();
            }
        });

        CleanupHandle {
            stop_flag,
            thread: Some(handle),
        }
    }
}

/// Handle for stopping the cleanup thread.
pub struct CleanupHandle {
    pub(super) stop_flag: Arc<AtomicBool>,
    pub(super) thread: Option<std::thread::JoinHandle<()>>,
}

impl CleanupHandle {
    /// Stop the cleanup thread
    pub fn stop(&mut self) -> Result<()> {
        self.stop_flag.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            handle.join().map_err(|payload| {
                let detail = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("unknown panic payload");
                Error::internal(format!("cleanup worker panicked: {detail}"))
            })?;
        }
        Ok(())
    }
}

impl Drop for CleanupHandle {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            if std::thread::panicking() {
                eprintln!("cleanup worker failed during panic: {error}");
            } else {
                panic!("cleanup worker failed: {error}");
            }
        }
    }
}
