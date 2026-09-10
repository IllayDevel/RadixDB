use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct UniqueClaimKey {
    index_name: SmartString,
    values: Vec<Value>,
}

impl UniqueClaimKey {
    fn new(index: &dyn Index, values: Vec<Value>) -> Self {
        Self {
            index_name: index.name().to_lowercase().into(),
            values,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct UniqueClaimOwner {
    pub(super) txn_id: i64,
    pub(super) row_id: i64,
}

pub(super) type SharedUniqueClaimMap = AHashMap<UniqueClaimKey, UniqueClaimOwner>;
type IndexedUniqueClaim = (Arc<dyn Index>, UniqueClaimKey);

impl TransactionVersionStore {
    fn active_unique_indexes(&self) -> Vec<Arc<dyn Index>> {
        let mut indexes: Vec<_> = self
            .parent_store
            .get_all_indexes()
            .into_iter()
            .filter(|index| {
                index.is_unique()
                    && !self.is_prepared_final_view_index(index)
                    && !self.is_parent_index_disabled(index)
            })
            .collect();
        indexes.sort_by(|left, right| left.name().cmp(right.name()));
        indexes
    }

    fn unique_keys_for_row(
        indexes: &[Arc<dyn Index>],
        row: &Row,
    ) -> Result<Vec<IndexedUniqueClaim>, Error> {
        let mut keys = Vec::with_capacity(indexes.len());
        for index in indexes {
            let Some(values) = index_values_for_row(index.as_ref(), row)? else {
                continue;
            };
            if values.iter().any(Value::is_null) {
                continue;
            }
            keys.push((
                Arc::clone(index),
                UniqueClaimKey::new(index.as_ref(), values),
            ));
        }
        Ok(keys)
    }

    fn unique_error(index: &dyn Index, key: &UniqueClaimKey, row_id: i64) -> Error {
        Error::UniqueConstraint {
            index: index.name().to_string(),
            column: index.column_names().join(", "),
            value: format!("{:?}", key.values),
            row_id,
        }
    }

    fn committed_unique_conflict(
        index: &dyn Index,
        key: &UniqueClaimKey,
        row_id: i64,
    ) -> Result<Option<i64>, Error> {
        if index.index_type() == IndexType::Hnsw {
            let hnsw = index
                .as_any()
                .downcast_ref::<crate::index::HnswIndex>()
                .ok_or_else(|| {
                    Error::internal(format!(
                        "index '{}' advertised HNSW type but cannot be downcast",
                        index.name()
                    ))
                })?;
            return Ok(key
                .values
                .first()
                .and_then(|value| hnsw.find_exact_duplicate(value, row_id, None)));
        }

        Ok(index
            .get_row_ids_equal(&key.values)?
            .iter()
            .copied()
            .find(|conflict| *conflict != row_id))
    }

    /// Atomically reserve the UNIQUE keys of a proposed statement-local row
    /// transition. The committed index remains the durable authority; this
    /// table-local claim map only closes the interval before commit publishes
    /// those index entries.
    pub(super) fn reserve_unique_keys_for_rows(
        &mut self,
        rows: &[(i64, Option<Row>)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let indexes = self.active_unique_indexes();
        if indexes.is_empty() {
            return Ok(());
        }

        // Only keys affected by this statement are copied. Bulk INSERT must
        // remain O(rows * indexes), not O(all transaction keys per row).
        let mut changed: AHashMap<UniqueClaimKey, Option<i64>> = AHashMap::new();
        let mut index_by_name: FxHashMap<SmartString, Arc<dyn Index>> = FxHashMap::default();
        for index in &indexes {
            index_by_name.insert(index.name().to_lowercase().into(), Arc::clone(index));
        }

        // All removals are applied before additions so a multi-row statement
        // may exchange UNIQUE values using its complete final row view.
        for (row_id, _) in rows {
            let Some(version) = self.get_local_version(*row_id) else {
                continue;
            };
            if version.is_deleted() {
                continue;
            }
            for (_, key) in Self::unique_keys_for_row(&indexes, &version.data)? {
                let owner = self.current_unique_keys.get(&key).copied();
                let entry = changed.entry(key).or_insert(owner);
                if *entry == Some(*row_id) {
                    *entry = None;
                }
            }
        }

        for (row_id, row) in rows {
            let Some(row) = row else {
                continue;
            };
            for (_, key) in Self::unique_keys_for_row(&indexes, row)? {
                let owner = self.current_unique_keys.get(&key).copied();
                let entry = changed.entry(key.clone()).or_insert(owner);
                if let Some(conflict) = *entry {
                    if conflict != *row_id {
                        let index = index_by_name
                            .get(&key.index_name)
                            .expect("active UNIQUE index identity must be present");
                        return Err(Self::unique_error(index.as_ref(), &key, conflict));
                    }
                }
                *entry = Some(*row_id);
            }
        }

        let parent = Arc::clone(&self.parent_store);
        let mut wait_budget = ClaimOwnerWaitBudget::new();
        let mut waited_for_owner = false;
        let mut wait_guard = parent.claim_wait_mutex.lock();
        let mut shared = loop {
            let shared = parent.unique_key_claims.lock();
            let conflict = changed
                .iter()
                .filter_map(|(key, owner)| owner.map(|_| key))
                .find_map(|key| {
                    shared
                        .get(key)
                        .copied()
                        .filter(|owner| owner.txn_id != self.txn_id)
                });
            let Some(owner) = conflict else {
                if waited_for_owner {
                    // The previous owner may have committed before releasing
                    // its claim. Recheck the now-published index while the
                    // claim handoff fence is still held; otherwise this
                    // statement could report success and defer a durable
                    // conflict until COMMIT.
                    for (key, row_id) in changed
                        .iter()
                        .filter_map(|(key, owner)| owner.map(|row_id| (key, row_id)))
                    {
                        let index = index_by_name
                            .get(&key.index_name)
                            .expect("active UNIQUE index identity must be present");
                        if let Some(conflict) =
                            Self::committed_unique_conflict(index.as_ref(), key, row_id)?
                        {
                            return Err(Self::unique_error(index.as_ref(), key, conflict));
                        }
                    }
                }
                break shared;
            };
            drop(shared);
            waited_for_owner = true;

            let wait_registered = parent
                .visibility_checker
                .as_ref()
                .is_none_or(|checker| checker.register_row_wait(self.txn_id, owner.txn_id));
            if !wait_registered {
                return Err(Error::TransactionSerializationConflict {
                    row_id: owner.row_id,
                });
            }

            let Some(remaining) = wait_budget.remaining(owner) else {
                if let Some(checker) = &parent.visibility_checker {
                    checker.clear_row_wait(self.txn_id);
                }
                return Err(Error::RowLockTimeout {
                    row_id: owner.row_id,
                    timeout_ms: ROW_CLAIM_WAIT_TIMEOUT.as_millis() as u64,
                });
            };
            parent.claim_changed.wait_for(&mut wait_guard, remaining);
            if let Some(checker) = &parent.visibility_checker {
                checker.clear_row_wait(self.txn_id);
            }
        };

        let claimed_at = get_fast_timestamp();
        for (key, owner) in changed {
            match owner {
                Some(row_id) => {
                    shared.insert(
                        key.clone(),
                        UniqueClaimOwner {
                            txn_id: self.txn_id,
                            row_id,
                        },
                    );
                    self.current_unique_keys.insert(key.clone(), row_id);
                    self.retained_unique_keys.entry(key).or_insert(claimed_at);
                }
                None => {
                    self.current_unique_keys.remove(&key);
                    // The shared claim is deliberately retained until the
                    // transaction is terminal. A later statement failure or
                    // savepoint rollback can then restore this key race-free.
                }
            }
        }
        Ok(())
    }

    /// Rebuild only the transaction-local final-view map after savepoint or
    /// statement rollback. Shared claims are a conservative superset and stay
    /// owned until the transaction terminates.
    pub(super) fn rebuild_current_unique_keys(&mut self) -> Result<(), Error> {
        let indexes = self.active_unique_indexes();
        let mut current = AHashMap::new();
        for (row_id, version) in self.iter_local() {
            if version.is_deleted() {
                continue;
            }
            for (index, key) in Self::unique_keys_for_row(&indexes, &version.data)? {
                if let Some(conflict) = current.insert(key.clone(), row_id) {
                    if conflict != row_id {
                        return Err(Self::unique_error(index.as_ref(), &key, conflict));
                    }
                }
            }
        }
        self.current_unique_keys = current;
        Ok(())
    }

    /// Restore the current final-view map and release only claims first
    /// acquired after the rollback boundary. Older superseded claims stay
    /// retained because the restored row history may need them again.
    pub(super) fn rollback_unique_keys_to(&mut self, timestamp: i64) -> Result<(), Error> {
        self.rebuild_current_unique_keys()?;

        let parent = Arc::clone(&self.parent_store);
        let _wait_guard = parent.claim_wait_mutex.lock();
        let mut shared = parent.unique_key_claims.lock();
        let mut released = Vec::new();
        for (key, claimed_at) in &self.retained_unique_keys {
            if let Some(row_id) = self.current_unique_keys.get(key) {
                if shared
                    .get(key)
                    .is_some_and(|owner| owner.txn_id == self.txn_id)
                {
                    shared.insert(
                        key.clone(),
                        UniqueClaimOwner {
                            txn_id: self.txn_id,
                            row_id: *row_id,
                        },
                    );
                }
            } else if *claimed_at > timestamp
                && shared
                    .get(key)
                    .is_some_and(|owner| owner.txn_id == self.txn_id)
            {
                shared.remove(key);
                released.push(key.clone());
            }
        }
        drop(shared);
        let did_release = !released.is_empty();
        for key in released {
            self.retained_unique_keys.remove(&key);
        }
        if did_release {
            parent.claim_changed.notify_all();
        }
        Ok(())
    }

    pub(super) fn release_retained_unique_keys(&mut self) {
        if self.retained_unique_keys.is_empty() {
            self.current_unique_keys.clear();
            return;
        }
        let parent = Arc::clone(&self.parent_store);
        let _wait_guard = parent.claim_wait_mutex.lock();
        let mut shared = parent.unique_key_claims.lock();
        let mut released = false;
        for key in self.retained_unique_keys.keys() {
            if shared
                .get(key)
                .is_some_and(|owner| owner.txn_id == self.txn_id)
            {
                shared.remove(key);
                released = true;
            }
        }
        drop(shared);
        self.current_unique_keys.clear();
        self.retained_unique_keys.clear();
        if released {
            parent.claim_changed.notify_all();
        }
    }
}
