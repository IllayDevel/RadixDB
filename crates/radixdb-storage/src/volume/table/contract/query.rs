macro_rules! segmented_table_query_methods {
    () => {
        // =========================================================================
        // Column operations — delegate to hot buffer
        // =========================================================================

        fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
            self.hot.rename_column(old_name, new_name)
        }

        fn modify_column(
            &mut self,
            name: &str,
            column_type: DataType,
            nullable: bool,
        ) -> Result<()> {
            self.hot.modify_column(name, column_type, nullable)
        }

        fn modify_column_with_default(
            &mut self,
            name: &str,
            column_type: DataType,
            nullable: bool,
            default_expr: Option<String>,
            default_value: Option<Value>,
        ) -> Result<()> {
            self.hot.modify_column_with_default(
                name,
                column_type,
                nullable,
                default_expr,
                default_value,
            )
        }

        // =========================================================================
        // Query operations
        // =========================================================================

        fn select(
            &self,
            columns: &[&str],
            expr: Option<&dyn Expression>,
        ) -> Result<Box<dyn QueryResult>> {
            if !self.segment_mgr.has_segments() {
                return self.hot.select(columns, expr);
            }

            let schema = self.hot.schema();
            let selects_all_columns =
                columns.is_empty() || (columns.len() == 1 && columns[0] == "*");
            let (column_indices, col_names): (Vec<usize>, Vec<String>) = if selects_all_columns {
                (
                    (0..schema.columns.len()).collect(),
                    schema.columns.iter().map(|col| col.name.clone()).collect(),
                )
            } else {
                let mut indices = Vec::with_capacity(columns.len());
                let mut names = Vec::with_capacity(columns.len());
                for name in columns {
                    if let Some((idx, _)) = schema.find_column(name) {
                        indices.push(idx);
                        names.push((*name).to_string());
                    }
                }
                (indices, names)
            };

            let scanner = self.scan(&column_indices, expr)?;
            Ok(Box::new(crate::traits::ScannerResult::new(
                scanner, col_names,
            )))
        }

        fn select_with_aliases(
            &self,
            columns: &[&str],
            expr: Option<&dyn Expression>,
            aliases: &FxHashMap<String, String>,
        ) -> Result<Box<dyn QueryResult>> {
            if !self.segment_mgr.has_segments() {
                return self.hot.select_with_aliases(columns, expr, aliases);
            }
            self.select(columns, expr)
        }

        fn select_as_of(
            &self,
            columns: &[&str],
            expr: Option<&dyn Expression>,
            temporal_type: &str,
            temporal_value: i64,
        ) -> Result<Box<dyn QueryResult>> {
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .select_as_of(columns, expr, temporal_type, temporal_value);
            }

            // Historical point-in-time queries on segment-backed tables are not
            // supported because cold rows lack version chains and create_time.
            let is_current_query = temporal_type.eq_ignore_ascii_case("CURRENT");
            if is_current_query {
                // CURRENT on a segment-backed table is the normal current read. Reuse
                // the projected streaming SELECT path instead of maintaining a
                // duplicate full-width cold scan here.
                return self.select(columns, expr);
            }

            Err(radixdb_core::Error::internal(
                "AS OF temporal queries are not supported on tables with sealed segments. \
             Sealed rows lack version history for point-in-time reconstruction.",
            ))
        }

        fn explain_scan(&self, where_expr: Option<&dyn Expression>) -> ScanPlan {
            if !self.segment_mgr.has_segments() {
                return self.hot.explain_scan(where_expr);
            }

            if let Some(plans) = where_expr.and_then(|expr| self.plan_cold_multi_index_or(expr)) {
                return ScanPlan::SegmentedMultiIndexScan {
                    table: self.hot.schema().table_name.clone(),
                    indexes: plans
                        .into_iter()
                        .map(|plan| {
                            (
                                plan.index_name,
                                format!("({})", plan.columns.join(", ")),
                                plan.conditions.join(" AND "),
                            )
                        })
                        .collect(),
                    operation: "OR".to_string(),
                    filter: where_expr.map(|expr| format!("{:?}", expr)),
                    cold_segments: self.segment_mgr.segment_count(),
                    cold_rows_hint: self.segment_row_count_hint(),
                    hot_rows_hint: self.hot.row_count_hint(),
                };
            }

            if let Some(plan) = where_expr.and_then(|expr| self.plan_cold_exact_set(expr)) {
                return ScanPlan::SegmentedCompositeIndexScan {
                    table: self.hot.schema().table_name.clone(),
                    index_name: plan.index_name,
                    columns: vec![plan.column],
                    conditions: vec![format!(
                        "IN ({})",
                        plan.values
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )],
                    filter: where_expr.map(|expr| format!("{:?}", expr)),
                    cold_segments: self.segment_mgr.segment_count(),
                    cold_rows_hint: self.segment_row_count_hint(),
                    hot_rows_hint: self.hot.row_count_hint(),
                    ordered: false,
                    composite: plan.declared_column_count > 1,
                };
            }

            if let Some(plan) = where_expr.and_then(|expr| self.plan_cold_composite_ordered(expr)) {
                return ScanPlan::SegmentedCompositeIndexScan {
                    table: self.hot.schema().table_name.clone(),
                    index_name: plan.index_name,
                    columns: plan.columns,
                    conditions: plan.conditions,
                    filter: where_expr.map(|expr| format!("{:?}", expr)),
                    cold_segments: self.segment_mgr.segment_count(),
                    cold_rows_hint: self.segment_row_count_hint(),
                    hot_rows_hint: self.hot.row_count_hint(),
                    ordered: true,
                    composite: plan.declared_column_count > 1,
                };
            }

            if let Some(plan) = where_expr.and_then(|expr| self.plan_cold_composite_exact(expr)) {
                return ScanPlan::SegmentedCompositeIndexScan {
                    table: self.hot.schema().table_name.clone(),
                    index_name: plan.index_name,
                    columns: plan.columns,
                    conditions: plan.conditions,
                    // Execution always rechecks the complete expression after hash
                    // candidate selection, including collision verification and any
                    // predicates not covered by the index key.
                    filter: where_expr.map(|expr| format!("{:?}", expr)),
                    cold_segments: self.segment_mgr.segment_count(),
                    cold_rows_hint: self.segment_row_count_hint(),
                    hot_rows_hint: self.hot.row_count_hint(),
                    ordered: false,
                    composite: plan.declared_column_count > 1,
                };
            }

            let segments = self.segment_mgr.segments_raw();
            let cold_segments = segments.len();
            let mut cold_rows_hint = 0usize;
            let mut cold_row_groups_hint = 0usize;
            let mut cold_selected_segments = 0usize;
            let mut cold_selected_rows_hint = 0usize;
            let mut cold_selected_row_groups_hint = 0usize;
            let mut cold_metadata_pruned_segments = 0usize;
            let mut cold_metadata_pruned_rows_hint = 0usize;
            let comparisons = where_expr
                .map(|expr| expr.collect_comparisons())
                .unwrap_or_default();
            let bloom_hashes = Self::precompute_bloom_hashes(&comparisons);

            for cold in segments.values() {
                let rows = cold.volume.meta.row_count;
                let row_groups = cold.volume.meta.row_groups.len();
                cold_rows_hint += rows;
                cold_row_groups_hint += row_groups;

                let (should_skip, start, end) = if comparisons.is_empty() {
                    (false, 0, rows)
                } else {
                    Self::prune_volume(&cold.volume, &comparisons, &bloom_hashes)
                };

                if should_skip {
                    cold_metadata_pruned_segments += 1;
                    cold_metadata_pruned_rows_hint += rows;
                } else {
                    cold_selected_segments += 1;
                    cold_selected_rows_hint += end.saturating_sub(start);
                    cold_selected_row_groups_hint += row_groups;
                }
            }
            ScanPlan::SegmentedScan {
                table: self.hot.schema().table_name.clone(),
                filter: where_expr.map(|expr| format!("{:?}", expr)),
                cold_segments,
                cold_rows_hint,
                cold_row_groups_hint,
                cold_selected_segments,
                cold_selected_rows_hint,
                cold_selected_row_groups_hint,
                cold_metadata_pruned_segments,
                cold_metadata_pruned_rows_hint,
                hot_rows_hint: self.hot.row_count_hint(),
            }
        }

        // =========================================================================
        // Zone maps — delegate to hot buffer
        // =========================================================================

        fn set_zone_maps(&self, zone_maps: crate::volume::zonemap::TableZoneMap) {
            self.hot.set_zone_maps(zone_maps)
        }

        fn zone_map_generation(&self) -> u64 {
            self.hot.zone_map_generation()
        }

        fn get_zone_maps(&self) -> Option<Arc<crate::volume::zonemap::TableZoneMap>> {
            self.hot.get_zone_maps()
        }
    };
}

pub(super) use segmented_table_query_methods;
