macro_rules! segmented_table_lifecycle_methods {
    () => {
        // =========================================================================
        // Transaction operations — delegate to hot buffer
        // =========================================================================

        fn close(&mut self) -> Result<()> {
            self.hot.close()
        }

        fn commit(&mut self) -> Result<()> {
            let txn_id = self.txn_id();
            for (index, values, row_id) in self.segment_mgr.pending_cold_index_removals(txn_id) {
                self.hot
                    .stage_external_index_removal(index, values, row_id)?;
            }
            self.hot.commit()?;
            // Apply pending tombstones to the shared tombstone set.
            // commit_seq=0 means "always visible to all snapshots". This is safe because
            // the main commit path goes through the engine transaction publisher, which passes
            // the real commit_seq. This fallback is for direct Table::commit() calls.
            self.segment_mgr.commit_pending_tombstones(txn_id, 0);
            self.segment_mgr.commit_cold_index_removals(txn_id);
            self.segment_mgr.clear_txn_seal_generation(txn_id);
            Ok(())
        }

        fn rollback(&mut self) {
            self.hot.rollback();
            let txn_id = self.txn_id();
            self.segment_mgr.rollback_cold_index_removals(txn_id);
            self.segment_mgr.rollback_pending_tombstones(txn_id);
            self.segment_mgr.clear_txn_seal_generation(txn_id);
        }

        fn rollback_to_timestamp(&self, timestamp: i64) {
            self.hot.rollback_to_timestamp(timestamp);
            self.segment_mgr
                .rollback_cold_index_removals_to_timestamp(self.txn_id(), timestamp);
            self.segment_mgr
                .rollback_pending_tombstones_to_timestamp(self.txn_id(), timestamp);
        }

        fn has_local_changes(&self) -> bool {
            self.hot.has_local_changes()
                || self.segment_mgr.has_pending_tombstones(self.txn_id())
                || self
                    .segment_mgr
                    .has_pending_cold_index_removals(self.txn_id())
        }

        fn get_pending_versions(&self) -> Vec<(i64, Row, bool, i64, i64)> {
            self.hot.get_pending_versions()
        }
    };
}

pub(super) use segmented_table_lifecycle_methods;
