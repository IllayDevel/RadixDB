macro_rules! segmented_table_index_methods {
    () => {
        // =========================================================================
        // Index operations
        // =========================================================================

        fn create_index(&self, name: &str, columns: &[&str], is_unique: bool) -> Result<()> {
            self.create_index_with_type(name, columns, is_unique, None)
        }

        fn create_index_with_type(
            &self,
            name: &str,
            columns: &[&str],
            is_unique: bool,
            index_type: Option<IndexType>,
        ) -> Result<()> {
            // For unique indexes (non-HNSW), validate cold data has no duplicates first.
            // HNSW unique validation happens during populate_index_from_cold via index.add().
            if is_unique && index_type != Some(IndexType::Hnsw) && self.segment_mgr.has_segments() {
                self.validate_cold_unique(name, columns)?;
            }

            let index = self
                .hot
                .build_index_with_type_detached(name, columns, is_unique, index_type)?;
            let has_cold_entries =
                self.segment_mgr.has_segments() && index.index_type() != IndexType::PrimaryKey;
            if has_cold_entries {
                self.populate_detached_index_from_cold(&index, columns)?;
            }
            let index_name = index.name().to_string();
            self.hot.publish_detached_index(index)?;
            if has_cold_entries {
                self.segment_mgr.mark_cold_populated_index(&index_name);
            }
            Ok(())
        }

        fn create_index_with_key_encoder(
            &self,
            name: &str,
            columns: &[&str],
            is_unique: bool,
            index_type: IndexType,
            encoder: crate::index::PreparedIndexKeyEncoder,
        ) -> Result<()> {
            self.hot
                .create_index_with_key_encoder(name, columns, is_unique, index_type, encoder)?;

            if self.segment_mgr.has_segments() {
                let index = self
                    .hot
                    .get_index(name)
                    .ok_or_else(|| radixdb_core::Error::IndexNotFound(name.to_string()))?;
                if let Err(error) = self.populate_detached_index_from_cold(&index, columns) {
                    let _ = self.hot.drop_index(name);
                    return Err(error);
                }
                self.segment_mgr.mark_cold_populated_index(name);
            }
            Ok(())
        }

        fn create_index_with_deferred_cold_backfill(
            &self,
            name: &str,
            columns: &[&str],
            is_unique: bool,
            index_type: Option<IndexType>,
        ) -> Result<()> {
            if is_unique {
                return Err(radixdb_core::Error::invalid_argument(
                    "UNIQUE indexes cannot defer cold validation",
                ));
            }
            let index = self
                .hot
                .build_index_with_type_detached(name, columns, false, index_type)?;
            self.hot.publish_detached_index(index)
        }

        fn complete_deferred_cold_index(&self, name: &str, columns: &[&str]) -> Result<()> {
            let index = self
                .hot
                .get_index(name)
                .ok_or_else(|| radixdb_core::Error::IndexNotFound(name.to_string()))?;
            if self.segment_mgr.has_segments() {
                self.populate_detached_index_from_cold(&index, columns)?;
                self.segment_mgr.mark_cold_populated_index(name);
            }
            Ok(())
        }

        fn create_partial_index_with_type(
            &self,
            name: &str,
            columns: &[&str],
            is_unique: bool,
            index_type: Option<IndexType>,
            predicate: crate::index::PartialIndexPredicate,
        ) -> Result<()> {
            self.hot
                .create_partial_index_with_type(name, columns, is_unique, index_type, predicate)?;

            if self.segment_mgr.has_segments() {
                let index = self
                    .hot
                    .get_index(name)
                    .ok_or_else(|| radixdb_core::Error::IndexNotFound(name.to_string()))?;
                if let Err(e) = self.populate_detached_index_from_cold(&index, columns) {
                    let _ = self.hot.drop_index(name);
                    return Err(e);
                }
                self.segment_mgr.mark_cold_populated_index(name);
            }
            Ok(())
        }

        fn create_hnsw_index(
            &self,
            name: &str,
            column: &str,
            is_unique: bool,
            m: usize,
            ef_construction: usize,
            ef_search: usize,
            metric: crate::index::HnswDistanceMetric,
        ) -> Result<()> {
            // Delegate to hot store which creates the HNSW with custom params
            self.hot.create_hnsw_index(
                name,
                column,
                is_unique,
                m,
                ef_construction,
                ef_search,
                metric,
            )?;

            // Populate from cold segments (HNSW must include all data)
            if self.segment_mgr.has_segments() {
                let index = self
                    .hot
                    .get_index(name)
                    .ok_or_else(|| radixdb_core::Error::IndexNotFound(name.to_string()))?;
                if let Err(e) = self.populate_detached_index_from_cold(&index, &[column]) {
                    let _ = self.hot.drop_index(name);
                    return Err(e);
                }
                self.segment_mgr.mark_cold_populated_index(name);
            }
            Ok(())
        }

        fn drop_index(&self, name: &str) -> Result<()> {
            self.hot.drop_index(name)?;
            self.segment_mgr.unmark_cold_populated_index(name);
            Ok(())
        }

        fn rename_index(&self, old_name: &str, new_name: &str) -> Result<()> {
            self.hot.rename_index(old_name, new_name)?;
            self.segment_mgr
                .rename_cold_populated_index(old_name, new_name);
            Ok(())
        }

        fn create_btree_index(
            &self,
            column_name: &str,
            is_unique: bool,
            custom_name: Option<&str>,
        ) -> Result<()> {
            if is_unique && self.segment_mgr.has_segments() {
                self.validate_cold_unique(custom_name.unwrap_or(column_name), &[column_name])?;
            }
            self.hot
                .create_btree_index(column_name, is_unique, custom_name)
        }

        fn drop_btree_index(&self, column_name: &str) -> Result<()> {
            self.hot.drop_btree_index(column_name)
        }

        fn create_multi_column_index(
            &self,
            name: &str,
            columns: &[&str],
            is_unique: bool,
        ) -> Result<()> {
            if is_unique && self.segment_mgr.has_segments() {
                self.validate_cold_unique(name, columns)?;
            }
            self.hot.create_multi_column_index(name, columns, is_unique)
        }

        fn has_index_on_column(&self, column_name: &str) -> bool {
            self.get_index_on_column(column_name).is_some()
        }

        fn get_index_on_column(&self, column_name: &str) -> Option<Arc<dyn Index>> {
            let index = self.hot.get_index_on_column(column_name)?;
            let persisted_complete =
                index.partial_predicate().is_none()
                    && self.hot.schema().find_column(column_name).is_some_and(
                        |(column_index, _)| {
                            self.segment_mgr.has_exact_index_columns(&[column_index])
                                || self.segment_mgr.has_ordered_index_columns(&[column_index])
                        },
                    );
            if !self.segment_mgr.has_segments()
                || matches!(index.index_type(), IndexType::PrimaryKey | IndexType::Hnsw)
                || self.segment_mgr.is_cold_populated_index(index.name())
                || persisted_complete
            {
                Some(index)
            } else {
                None
            }
        }

        fn collect_row_ids_by_index_values(
            &self,
            column_name: &str,
            values: &[Value],
        ) -> Option<Result<Vec<i64>>> {
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .collect_row_ids_by_index_values(column_name, values);
            }
            let candidates = match self.exact_index_candidates(column_name, values)? {
                Ok(candidates) => candidates,
                Err(error) => return Some(Err(error)),
            };
            if !candidates.requires_recheck || candidates.row_ids.is_empty() {
                return Some(Ok(candidates.row_ids));
            }
            let rows = match self
                .collect_rows_by_ids_projected(&candidates.row_ids, &[candidates.column_index])
            {
                Ok(rows) => rows,
                Err(error) => return Some(Err(error)),
            };
            let mut row_ids: Vec<i64> = rows
                .into_iter()
                .filter_map(|(row_id, row)| {
                    row.get(0)
                        .is_some_and(|actual| candidates.requested_values.contains(actual))
                        .then_some(row_id)
                })
                .collect();
            row_ids.sort_unstable();
            Some(Ok(row_ids))
        }

        fn collect_rows_by_index_values_projected(
            &self,
            column_name: &str,
            values: &[Value],
            projection: Option<&[usize]>,
        ) -> Option<Result<RowVec>> {
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_rows_by_index_values_projected(
                    column_name,
                    values,
                    projection,
                );
            }
            let candidates = match self.exact_index_candidates(column_name, values)? {
                Ok(candidates) => candidates,
                Err(error) => return Some(Err(error)),
            };

            if !candidates.requires_recheck {
                return Some(match projection {
                    Some(columns) => {
                        self.collect_rows_by_ids_projected(&candidates.row_ids, columns)
                    }
                    None => self.collect_rows_by_ids(&candidates.row_ids),
                });
            }

            let mut appended_key = false;
            let (physical_projection, key_position) = match projection {
                Some(columns) => {
                    let mut physical = columns.to_vec();
                    let key_position = physical
                        .iter()
                        .position(|column| *column == candidates.column_index)
                        .unwrap_or_else(|| {
                            appended_key = true;
                            physical.push(candidates.column_index);
                            physical.len() - 1
                        });
                    (Some(physical), key_position)
                }
                None => (None, candidates.column_index),
            };
            let rows = match physical_projection.as_deref() {
                Some(columns) => self.collect_rows_by_ids_projected(&candidates.row_ids, columns),
                None => self.collect_rows_by_ids(&candidates.row_ids),
            };
            let rows = match rows {
                Ok(rows) => rows,
                Err(error) => return Some(Err(error)),
            };
            let mut result = RowVec::with_capacity(rows.len());
            for (row_id, mut row) in rows {
                if !row
                    .get(key_position)
                    .is_some_and(|actual| candidates.requested_values.contains(actual))
                {
                    continue;
                }
                if appended_key {
                    let _ = row.pop();
                }
                result.push((row_id, row));
            }
            Some(Ok(result))
        }

        fn collect_row_ids_by_index_ranges(
            &self,
            index_name: &str,
            ranges: &[crate::traits::IndexKeyRange],
        ) -> Option<Result<Vec<i64>>> {
            if self.segment_mgr.has_segments()
                && !self.segment_mgr.is_cold_populated_index(index_name)
            {
                return None;
            }
            self.hot.collect_row_ids_by_index_ranges(index_name, ranges)
        }

        fn has_cold_segments(&self) -> bool {
            self.segment_mgr.has_segments()
        }

        fn get_index(&self, name: &str) -> Option<Arc<dyn Index>> {
            self.hot.get_index(name)
        }

        fn get_unique_indexes(&self) -> Vec<(String, Vec<String>)> {
            self.hot.get_unique_indexes()
        }

        fn get_indexes(&self) -> Vec<Arc<dyn Index>> {
            self.hot.get_indexes()
        }

        fn get_unique_non_pk_indexes(&self) -> Vec<Arc<dyn Index>> {
            self.hot.get_unique_non_pk_indexes()
        }

        fn for_each_unique_non_pk_index(
            &self,
            f: &mut dyn FnMut(&str, &[String]) -> Result<()>,
        ) -> Result<()> {
            self.hot.for_each_unique_non_pk_index(f)
        }

        fn find_unique_conflict_row_id(
            &self,
            _index_name: &str,
            column_name: &str,
            row_values: &[Value],
        ) -> Result<Option<i64>> {
            if !self.segment_mgr.has_segments() {
                return Ok(None);
            }

            let schema = self.hot.schema();
            let mut col_indices = Vec::new();
            let mut values = Vec::new();
            for col_name in column_name.split(", ") {
                let Some(&col_idx) = schema
                    .column_index_map()
                    .get(col_name.to_lowercase().as_str())
                else {
                    return Ok(None);
                };
                let Some(value) = row_values.get(col_idx) else {
                    return Ok(None);
                };
                if value.is_null() {
                    return Ok(None);
                }
                col_indices.push(col_idx);
                values.push(value);
            }

            self.find_segment_row_id_by_values(&col_indices, &values)
        }

        fn has_unique_non_pk_indexes(&self) -> bool {
            self.hot.has_unique_non_pk_indexes()
        }

        fn get_multi_column_index(
            &self,
            predicate_columns: &[&str],
        ) -> Option<(Arc<dyn Index>, usize)> {
            self.hot.get_multi_column_index(predicate_columns)
        }

        fn get_index_min_value(&self, column_name: &str) -> Option<Value> {
            if !self.segment_mgr.has_segments() {
                return self.hot.get_index_min_value(column_name);
            }
            let hot_min = self.hot.get_index_min_value(column_name);
            let segments = self.segment_mgr.get_segments_ordered_meta();
            let mut vol_min: Option<Value> = None;
            for vol in &segments {
                // Resolve physical column index per volume (schema evolution safe)
                let Some(pi) = vol.column_index(column_name) else {
                    continue;
                };
                if pi >= vol.meta.zone_maps.len() {
                    continue;
                }
                let zm_min = &vol.meta.zone_maps[pi].min;
                if !zm_min.is_null() {
                    match &vol_min {
                        None => vol_min = Some(zm_min.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Less) = zm_min.compare(current) {
                                vol_min = Some(zm_min.clone());
                            }
                        }
                    }
                }
            }
            match (hot_min, vol_min) {
                (Some(h), Some(v)) => {
                    if let Ok(std::cmp::Ordering::Less) = v.compare(&h) {
                        Some(v)
                    } else {
                        Some(h)
                    }
                }
                (Some(h), None) => Some(h),
                (None, Some(v)) => Some(v),
                (None, None) => None,
            }
        }

        fn get_index_max_value(&self, column_name: &str) -> Option<Value> {
            if !self.segment_mgr.has_segments() {
                return self.hot.get_index_max_value(column_name);
            }
            let hot_max = self.hot.get_index_max_value(column_name);
            let segments = self.segment_mgr.get_segments_ordered_meta();
            let mut vol_max: Option<Value> = None;
            for vol in &segments {
                // Resolve physical column index per volume (schema evolution safe)
                let Some(pi) = vol.column_index(column_name) else {
                    continue;
                };
                if pi >= vol.meta.zone_maps.len() {
                    continue;
                }
                let zm_max = &vol.meta.zone_maps[pi].max;
                if !zm_max.is_null() {
                    match &vol_max {
                        None => vol_max = Some(zm_max.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Greater) = zm_max.compare(current) {
                                vol_max = Some(zm_max.clone());
                            }
                        }
                    }
                }
            }
            match (hot_max, vol_max) {
                (Some(h), Some(v)) => {
                    if let Ok(std::cmp::Ordering::Greater) = v.compare(&h) {
                        Some(v)
                    } else {
                        Some(h)
                    }
                }
                (Some(h), None) => Some(h),
                (None, Some(v)) => Some(v),
                (None, None) => None,
            }
        }
    };
}

pub(super) use segmented_table_index_methods;
