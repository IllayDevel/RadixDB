//! Bounded cold and mixed-source aggregate execution.

use super::*;

impl SegmentedTable {
    pub(super) fn add_cold_projection_column(
        columns: &mut Vec<usize>,
        positions: &mut [Option<usize>],
        col_idx: usize,
    ) {
        if col_idx < positions.len() && positions[col_idx].is_none() {
            positions[col_idx] = Some(columns.len());
            columns.push(col_idx);
        }
    }

    pub(super) fn build_cold_aggregate_projection(
        schema_len: usize,
        aggregates: &[(AggregateOp, usize)],
        extra_columns: &[usize],
    ) -> Option<ColdAggregateProjection> {
        let mut columns = Vec::new();
        let mut positions = vec![None; schema_len];

        for &col_idx in extra_columns {
            if col_idx >= schema_len {
                return None;
            }
            Self::add_cold_projection_column(&mut columns, &mut positions, col_idx);
        }

        for &(op, col_idx) in aggregates {
            if op == AggregateOp::CountStar {
                continue;
            }
            if col_idx >= schema_len {
                return None;
            }
            Self::add_cold_projection_column(&mut columns, &mut positions, col_idx);
        }

        let mut mapped_aggregates = Vec::with_capacity(aggregates.len());
        for &(op, col_idx) in aggregates {
            let mapped_col = if op == AggregateOp::CountStar {
                0
            } else {
                positions.get(col_idx).and_then(|pos| *pos)?
            };
            mapped_aggregates.push((op, mapped_col));
        }

        Some(ColdAggregateProjection {
            columns,
            positions,
            aggregates: mapped_aggregates,
        })
    }

    /// Build the physical contract for the DATA artifact grouped-aggregate path.
    ///
    /// Mapping identity is a correctness boundary, not an optimisation hint:
    /// defaults, renamed columns, and ALTER TABLE mappings remain on the
    /// generic scanner until the columnar operator supports them explicitly.
    pub(super) fn build_artifact_columnar_group_plan(
        schema: &Schema,
        mapping: &crate::volume::writer::ColumnMapping,
        group_by_indices: &[usize],
        aggregates: &[(AggregateOp, usize)],
    ) -> std::result::Result<
        ArtifactColumnarGroupPlan,
        crate::instrumentation::ArtifactColumnarGroupFallback,
    > {
        use crate::instrumentation::ArtifactColumnarGroupFallback;

        if group_by_indices.len() != 1 {
            return Err(ArtifactColumnarGroupFallback::GroupKey);
        }
        if mapping.sources.len() != schema.columns.len() {
            return Err(ArtifactColumnarGroupFallback::Schema);
        }
        if !mapping.is_identity {
            return Err(ArtifactColumnarGroupFallback::Schema);
        }

        // `is_identity` is cached metadata. Validate its observable contract
        // before using logical indices as physical block IDs.
        for (logical_idx, source) in mapping.sources.iter().enumerate() {
            if !matches!(source, ColSource::Volume(physical_idx) if *physical_idx == logical_idx) {
                return Err(ArtifactColumnarGroupFallback::Schema);
            }
        }

        let group_idx = group_by_indices[0];
        let group_data_type = schema
            .columns
            .get(group_idx)
            .ok_or(ArtifactColumnarGroupFallback::GroupKey)?
            .data_type;
        if !matches!(group_data_type, DataType::Integer | DataType::Timestamp) {
            return Err(ArtifactColumnarGroupFallback::GroupKey);
        }

        let mut physical_projection = vec![group_idx];
        let mut resolved_aggregates = Vec::with_capacity(aggregates.len());
        for &(operation, logical_col) in aggregates {
            let data_type = if operation == AggregateOp::CountStar {
                DataType::Null
            } else {
                schema
                    .columns
                    .get(logical_col)
                    .ok_or(ArtifactColumnarGroupFallback::Aggregate)?
                    .data_type
            };
            let projection_pos = if operation == AggregateOp::CountStar {
                None
            } else {
                let physical_idx = match mapping
                    .sources
                    .get(logical_col)
                    .ok_or(ArtifactColumnarGroupFallback::Aggregate)?
                {
                    ColSource::Volume(physical_idx) => *physical_idx,
                    ColSource::Default(_) => return Err(ArtifactColumnarGroupFallback::Schema),
                };
                let pos = physical_projection
                    .iter()
                    .position(|&idx| idx == physical_idx)
                    .unwrap_or_else(|| {
                        physical_projection.push(physical_idx);
                        physical_projection.len() - 1
                    });
                Some(pos)
            };

            // COUNT only inspects the null bitmap.  Numeric aggregate work is
            // intentionally restricted to native DATA representations; string
            // and extension ordering stay with the semantic scanner.
            if matches!(
                operation,
                AggregateOp::Sum | AggregateOp::Avg | AggregateOp::Min | AggregateOp::Max
            ) && !matches!(
                data_type,
                DataType::Integer | DataType::Timestamp | DataType::Float
            ) {
                return Err(ArtifactColumnarGroupFallback::Aggregate);
            }

            resolved_aggregates.push(ArtifactColumnarGroupAggregate {
                operation,
                data_type,
                projection_pos,
            });
        }

        Ok(ArtifactColumnarGroupPlan {
            group_data_type,
            group_projection_pos: 0,
            physical_projection,
            aggregates: resolved_aggregates,
        })
    }

    /// Apply one DATA artifact batch to a grouped accumulator map.
    ///
    /// A false result means the on-disk column representation disagreed with
    /// the declared plan. The caller discards partial state and uses the
    /// scanner, preserving correctness instead of guessing.
    pub(super) fn accumulate_artifact_columnar_group_batch(
        plan: &ArtifactColumnarGroupPlan,
        batch: &ArtifactColumnBatch,
        groups: &mut ArtifactColumnarGroupState,
    ) -> bool {
        let row_count = batch.row_range().len();
        let mut columns = Vec::with_capacity(plan.physical_projection.len());
        for physical_idx in &plan.physical_projection {
            let Some((_, column)) = batch
                .columns()
                .iter()
                .find(|(batch_idx, _)| batch_idx == physical_idx)
            else {
                return false;
            };
            if column.len() != row_count {
                return false;
            }
            columns.push(column);
        }

        let Some(group_column) = columns.get(plan.group_projection_pos).copied() else {
            return false;
        };
        let (group_values, group_nulls) = match (plan.group_data_type, group_column) {
            (DataType::Integer, ColumnData::Int64 { values, nulls })
            | (DataType::Timestamp, ColumnData::TimestampNanos { values, nulls }) => {
                (values.as_slice(), nulls.as_slice())
            }
            _ => return false,
        };

        for row_idx in 0..row_count {
            let group_key = (!group_nulls[row_idx]).then_some(group_values[row_idx]);
            let Some(accums) = groups.accums_for_key(group_key, plan.aggregates.len()) else {
                return false;
            };

            for (accum, aggregate) in accums.iter_mut().zip(&plan.aggregates) {
                let column = aggregate
                    .projection_pos
                    .and_then(|projection_pos| columns.get(projection_pos).copied());
                match aggregate.operation {
                    AggregateOp::CountStar => accum.count += 1,
                    AggregateOp::Count => {
                        let Some(column) = column else {
                            return false;
                        };
                        if column.data_type() != aggregate.data_type {
                            return false;
                        }
                        if !column.is_null(row_idx) {
                            accum.count += 1;
                        }
                    }
                    AggregateOp::Sum | AggregateOp::Avg | AggregateOp::Min | AggregateOp::Max => {
                        let Some(column) = column else {
                            return false;
                        };
                        match (aggregate.data_type, column) {
                            (DataType::Integer, ColumnData::Int64 { values, nulls })
                            | (DataType::Timestamp, ColumnData::TimestampNanos { values, nulls }) =>
                            {
                                if nulls[row_idx] {
                                    continue;
                                }
                                let value = values[row_idx];
                                match aggregate.operation {
                                    AggregateOp::Sum | AggregateOp::Avg => {
                                        accum.int_sum += value as i128;
                                        accum.count += 1;
                                    }
                                    AggregateOp::Min => {
                                        accum.min_i64 = Some(
                                            accum
                                                .min_i64
                                                .map_or(value, |current| current.min(value)),
                                        );
                                    }
                                    AggregateOp::Max => {
                                        accum.max_i64 = Some(
                                            accum
                                                .max_i64
                                                .map_or(value, |current| current.max(value)),
                                        );
                                    }
                                    _ => unreachable!("numeric aggregate operation was filtered"),
                                }
                            }
                            (DataType::Float, ColumnData::Float64 { values, nulls }) => {
                                if nulls[row_idx] || values[row_idx].is_nan() {
                                    continue;
                                }
                                let value = values[row_idx];
                                match aggregate.operation {
                                    AggregateOp::Sum | AggregateOp::Avg => {
                                        accum.float_sum += value;
                                        accum.count += 1;
                                    }
                                    AggregateOp::Min => {
                                        accum.min_f64 = Some(
                                            accum
                                                .min_f64
                                                .map_or(value, |current| current.min(value)),
                                        );
                                    }
                                    AggregateOp::Max => {
                                        accum.max_f64 = Some(
                                            accum
                                                .max_f64
                                                .map_or(value, |current| current.max(value)),
                                        );
                                    }
                                    _ => unreachable!("numeric aggregate operation was filtered"),
                                }
                            }
                            _ => return false,
                        }
                    }
                }
            }
        }

        true
    }

    pub(super) fn finalize_artifact_columnar_group_accums(
        plan: &ArtifactColumnarGroupPlan,
        accums: &[ArtifactColumnarAccum],
    ) -> Option<Vec<Value>> {
        plan.aggregates
            .iter()
            .zip(accums)
            .map(|(aggregate, accum)| {
                Some(match aggregate.operation {
                    AggregateOp::Count | AggregateOp::CountStar => Value::Integer(accum.count),
                    AggregateOp::Sum => {
                        if accum.count == 0 {
                            Value::Null(DataType::Float)
                        } else if aggregate.data_type == DataType::Integer {
                            exact_integer_sum_value(accum.int_sum)?
                        } else {
                            Value::Float(accum.int_sum as f64 + accum.float_sum)
                        }
                    }
                    AggregateOp::Avg => {
                        if accum.count == 0 {
                            Value::Null(DataType::Float)
                        } else {
                            Value::Float(
                                (accum.int_sum as f64 + accum.float_sum) / accum.count as f64,
                            )
                        }
                    }
                    AggregateOp::Min => match (accum.min_i64, accum.min_f64, aggregate.data_type) {
                        (Some(value), None, DataType::Timestamp) => {
                            chrono::DateTime::from_timestamp(
                                value.div_euclid(1_000_000_000),
                                value.rem_euclid(1_000_000_000) as u32,
                            )
                            .map(Value::Timestamp)
                            .unwrap_or(Value::Null(DataType::Timestamp))
                        }
                        (Some(value), None, _) => Value::Integer(value),
                        (None, Some(value), _) => Value::Float(value),
                        (None, None, data_type) => Value::Null(data_type),
                        // A physical column has one type, so this is unreachable
                        // for a valid artifact-backed plan. Retain the scanner's deterministic
                        // comparison if a future mixed representation reaches it.
                        (Some(integer), Some(float), _) if (integer as f64) <= float => {
                            Value::Integer(integer)
                        }
                        (Some(_), Some(float), _) => Value::Float(float),
                    },
                    AggregateOp::Max => match (accum.max_i64, accum.max_f64, aggregate.data_type) {
                        (Some(value), None, DataType::Timestamp) => {
                            chrono::DateTime::from_timestamp(
                                value.div_euclid(1_000_000_000),
                                value.rem_euclid(1_000_000_000) as u32,
                            )
                            .map(Value::Timestamp)
                            .unwrap_or(Value::Null(DataType::Timestamp))
                        }
                        (Some(value), None, _) => Value::Integer(value),
                        (None, Some(value), _) => Value::Float(value),
                        (None, None, data_type) => Value::Null(data_type),
                        (Some(integer), Some(float), _) if (integer as f64) >= float => {
                            Value::Integer(integer)
                        }
                        (Some(_), Some(float), _) => Value::Float(float),
                    },
                })
            })
            .collect()
    }

    pub(super) fn artifact_columnar_group_zone_map(
        segment: &ColdSegment,
        physical_col_idx: usize,
        row_group_idx: usize,
    ) -> Option<&ZoneMap> {
        if segment.volume.meta.row_groups.is_empty() {
            return (row_group_idx == 0)
                .then(|| segment.volume.meta.zone_maps.get(physical_col_idx))
                .flatten();
        }
        segment
            .volume
            .meta
            .row_groups
            .get(row_group_idx)?
            .zone_maps
            .get(physical_col_idx)
    }

    pub(super) fn artifact_columnar_group_bound_value(
        data_type: DataType,
        value: &Value,
    ) -> Option<i64> {
        match (data_type, value) {
            (DataType::Integer, Value::Integer(value)) => Some(*value),
            (DataType::Timestamp, Value::Timestamp(value)) => value.timestamp_nanos_opt(),
            // Some internal paths normalize timestamp predicates to i64 nanos.
            // Accept that representation if a future metadata writer exposes it
            // directly, but keep all other shapes on the hash-map path.
            (DataType::Timestamp, Value::Integer(value)) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn artifact_columnar_group_direct_bounds(
        plan: &ArtifactColumnarGroupPlan,
        volumes: &[(u64, ColdSegment)],
    ) -> Option<(i64, i64)> {
        let group_physical_idx = *plan.physical_projection.get(plan.group_projection_pos)?;
        let mut min_key: Option<i64> = None;
        let mut max_key: Option<i64> = None;

        for (_, segment) in volumes {
            let source = segment.volume.artifact_source()?;
            for row_group_idx in 0..source.row_group_count() {
                let zone = Self::artifact_columnar_group_zone_map(
                    segment,
                    group_physical_idx,
                    row_group_idx,
                )?;
                if zone.null_count == zone.row_count {
                    continue;
                }
                let group_min =
                    Self::artifact_columnar_group_bound_value(plan.group_data_type, &zone.min)?;
                let group_max =
                    Self::artifact_columnar_group_bound_value(plan.group_data_type, &zone.max)?;
                min_key = Some(min_key.map_or(group_min, |current| current.min(group_min)));
                max_key = Some(max_key.map_or(group_max, |current| current.max(group_max)));
            }
        }

        let min_key = min_key?;
        let max_key = max_key?;
        if max_key < min_key {
            return None;
        }
        Some((min_key, max_key))
    }

    pub(super) fn accumulate_artifact_columnar_group_segment(
        plan: &ArtifactColumnarGroupPlan,
        segment: &ColdSegment,
        direct_bounds: Option<(i64, i64)>,
    ) -> std::result::Result<
        ArtifactColumnarGroupSegmentResult,
        crate::instrumentation::ArtifactColumnarGroupFallback,
    > {
        use crate::instrumentation::ArtifactColumnarGroupFallback;

        let Some(source) = segment.volume.artifact_source() else {
            return Err(ArtifactColumnarGroupFallback::Storage);
        };
        let mut groups = ArtifactColumnarGroupState::new(direct_bounds);
        let mut observed_row_groups = 0u64;
        let mut observed_selected_blocks = 0u64;
        let mut observed_input_rows = 0u64;

        for group_idx in 0..source.row_group_count() {
            let batch = source
                .read_columns(group_idx, &plan.physical_projection)
                .map_err(|_| ArtifactColumnarGroupFallback::Storage)?;
            debug_assert_eq!(batch.row_group_index(), group_idx);
            if !Self::accumulate_artifact_columnar_group_batch(plan, &batch, &mut groups) {
                return Err(ArtifactColumnarGroupFallback::ColumnShape);
            }
            observed_row_groups = observed_row_groups.saturating_add(1);
            observed_selected_blocks =
                observed_selected_blocks.saturating_add(plan.physical_projection.len() as u64);
            observed_input_rows =
                observed_input_rows.saturating_add(batch.row_range().len() as u64);
        }

        Ok(ArtifactColumnarGroupSegmentResult {
            groups,
            row_groups: observed_row_groups,
            selected_blocks: observed_selected_blocks,
            input_rows: observed_input_rows,
        })
    }

    pub(super) fn finalize_artifact_columnar_group_state(
        plan: &ArtifactColumnarGroupPlan,
        groups: ArtifactColumnarGroupState,
    ) -> Option<Vec<GroupedAggregateResult>> {
        groups
            .into_key_accums()
            .into_iter()
            .map(|(group_key, accums)| {
                let group_value = match (group_key, plan.group_data_type) {
                    (Some(value), DataType::Timestamp) => chrono::DateTime::from_timestamp(
                        value.div_euclid(1_000_000_000),
                        value.rem_euclid(1_000_000_000) as u32,
                    )
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null(DataType::Timestamp)),
                    (Some(value), _) => Value::Integer(value),
                    (None, data_type) => Value::Null(data_type),
                };
                Some(GroupedAggregateResult {
                    group_values: vec![group_value],
                    aggregate_values: Self::finalize_artifact_columnar_group_accums(plan, &accums)?,
                })
            })
            .collect()
    }

    /// Aggregate a stable DATA-artifact cold set without creating a
    /// row adapter. This is intentionally narrower than the scanner path:
    /// in-flight DML, tombstones, schema mapping and multi-key GROUP BY stay
    /// on the established generic implementation.
    pub(super) fn compute_grouped_aggregates_artifact_columnar(
        &self,
        group_by_indices: &[usize],
        aggregates: &[(AggregateOp, usize)],
    ) -> Option<Vec<GroupedAggregateResult>> {
        use crate::instrumentation::{
            record_artifact_columnar_group_fallback, ArtifactColumnarGroupFallback,
            ArtifactColumnarGroupRecord,
        };

        if self.hot.row_count() != 0
            || self.segment_mgr.has_pending_tombstones(self.txn_id())
            || !self.segment_mgr.is_tombstone_set_empty()
        {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::RowState);
            return None;
        }

        let schema = self.hot.schema();
        let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
        let Some((_, first_segment)) = volumes.first() else {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::NoColdArtifact);
            return None;
        };
        let plan = match Self::build_artifact_columnar_group_plan(
            schema,
            &first_segment.mapping,
            group_by_indices,
            aggregates,
        ) {
            Ok(plan) => plan,
            Err(reason) => {
                record_artifact_columnar_group_fallback(reason);
                return None;
            }
        };

        // Any visibility overlay means a row-level decision is necessary.
        // The scanner already owns that contract, so never approximate it
        // here just to keep the fast path alive.
        for (_, segment) in volumes.iter() {
            if segment.visible.is_some() {
                record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Visibility);
                return None;
            }
            if !segment.mapping.is_identity {
                record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Schema);
                return None;
            }
            if segment.volume.artifact_source().is_none() {
                record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Storage);
                return None;
            }
        }

        let direct_bounds = Self::artifact_columnar_group_direct_bounds(&plan, volumes.as_slice());
        let mut groups = ArtifactColumnarGroupState::new(direct_bounds);
        let uses_direct_accumulator = groups.is_direct_array();
        let mut observed_row_groups = 0u64;
        let mut observed_selected_blocks = 0u64;
        let mut observed_input_rows = 0u64;
        let mut observed_local_merges = 0u64;
        #[cfg(feature = "parallel")]
        let (scheduled_results, observed_scheduler_runs, observed_scheduled_segments) = {
            if volumes.len() > 1 {
                use rayon::prelude::*;
                (
                    Some(
                        volumes
                            .par_iter()
                            .map(|(_, segment)| {
                                Self::accumulate_artifact_columnar_group_segment(
                                    &plan,
                                    segment,
                                    direct_bounds,
                                )
                            })
                            .collect::<Vec<_>>(),
                    ),
                    1,
                    volumes.len() as u64,
                )
            } else {
                (None, 0, 0)
            }
        };
        #[cfg(not(feature = "parallel"))]
        let scheduled_results: Option<
            Vec<
                std::result::Result<
                    ArtifactColumnarGroupSegmentResult,
                    crate::instrumentation::ArtifactColumnarGroupFallback,
                >,
            >,
        > = None;
        #[cfg(not(feature = "parallel"))]
        let (observed_scheduler_runs, observed_scheduled_segments) = (0u64, 0u64);

        if let Some(results) = scheduled_results {
            for result in results {
                let segment = match result {
                    Ok(segment) => segment,
                    Err(reason) => {
                        record_artifact_columnar_group_fallback(reason);
                        return None;
                    }
                };
                observed_row_groups = observed_row_groups.saturating_add(segment.row_groups);
                observed_selected_blocks =
                    observed_selected_blocks.saturating_add(segment.selected_blocks);
                observed_input_rows = observed_input_rows.saturating_add(segment.input_rows);
                if !groups.merge_from(segment.groups, plan.aggregates.len()) {
                    record_artifact_columnar_group_fallback(
                        ArtifactColumnarGroupFallback::Accumulator,
                    );
                    return None;
                }
                observed_local_merges = observed_local_merges.saturating_add(1);
            }
        } else {
            for (_, segment) in volumes.iter() {
                let segment = match Self::accumulate_artifact_columnar_group_segment(
                    &plan,
                    segment,
                    direct_bounds,
                ) {
                    Ok(segment) => segment,
                    Err(reason) => {
                        record_artifact_columnar_group_fallback(reason);
                        return None;
                    }
                };
                observed_row_groups = observed_row_groups.saturating_add(segment.row_groups);
                observed_selected_blocks =
                    observed_selected_blocks.saturating_add(segment.selected_blocks);
                observed_input_rows = observed_input_rows.saturating_add(segment.input_rows);
                if !groups.merge_from(segment.groups, plan.aggregates.len()) {
                    record_artifact_columnar_group_fallback(
                        ArtifactColumnarGroupFallback::Accumulator,
                    );
                    return None;
                }
                observed_local_merges = observed_local_merges.saturating_add(1);
            }
        }

        if observed_local_merges != volumes.len() as u64 {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Accumulator);
            return None;
        }

        let observed_output_groups = groups.group_count() as u64;
        if observed_input_rows == 0 && observed_output_groups != 0 {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Accumulator);
            return None;
        }

        if observed_scheduler_runs > 0 && observed_scheduled_segments != volumes.len() as u64 {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Accumulator);
            return None;
        }

        if observed_scheduler_runs == 0 && observed_scheduled_segments != 0 {
            record_artifact_columnar_group_fallback(ArtifactColumnarGroupFallback::Accumulator);
            return None;
        }
        crate::instrumentation::record_artifact_columnar_group_aggregate(
            ArtifactColumnarGroupRecord {
                row_groups: observed_row_groups,
                selected_blocks: observed_selected_blocks,
                input_rows: observed_input_rows,
                output_groups: observed_output_groups,
                direct_accumulators: u64::from(uses_direct_accumulator),
                hash_accumulators: u64::from(!uses_direct_accumulator),
                local_merges: observed_local_merges,
                merged_groups: observed_output_groups,
                scheduler_runs: observed_scheduler_runs,
                scheduled_segments: observed_scheduled_segments,
            },
        );

        Self::finalize_artifact_columnar_group_state(&plan, groups)
    }

    pub(super) fn compute_filtered_aggregates_scanner(
        &self,
        aggregates: &[(AggregateOp, usize)],
        where_expr: &dyn Expression,
    ) -> Option<Vec<Value>> {
        #[derive(Clone)]
        struct Accum {
            count: i64,
            int_sum: i128,
            float_sum: f64,
            is_int_agg: bool,
            min: Option<Value>,
            max: Option<Value>,
        }

        impl Default for Accum {
            fn default() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: 0.0,
                    is_int_agg: true,
                    min: None,
                    max: None,
                }
            }
        }

        fn update_accums(accums: &mut [Accum], aggregates: &[(AggregateOp, usize)], row: &Row) {
            for (agg_idx, (op, col_idx)) in aggregates.iter().enumerate() {
                let acc = &mut accums[agg_idx];
                match op {
                    AggregateOp::CountStar => acc.count += 1,
                    AggregateOp::Count => {
                        if row.get(*col_idx).is_some_and(|value| !value.is_null()) {
                            acc.count += 1;
                        }
                    }
                    AggregateOp::Sum | AggregateOp::Avg => match row.get(*col_idx) {
                        Some(Value::Integer(value)) => {
                            acc.int_sum += *value as i128;
                            acc.count += 1;
                        }
                        Some(Value::Float(value)) if !value.is_nan() => {
                            acc.float_sum += *value;
                            acc.count += 1;
                            acc.is_int_agg = false;
                        }
                        _ => {}
                    },
                    AggregateOp::Min => {
                        let Some(value) = row.get(*col_idx) else {
                            continue;
                        };
                        if value.is_null() {
                            continue;
                        }
                        match &acc.min {
                            None => acc.min = Some(value.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Less) = value.compare(current) {
                                    acc.min = Some(value.clone());
                                }
                            }
                        }
                    }
                    AggregateOp::Max => {
                        let Some(value) = row.get(*col_idx) else {
                            continue;
                        };
                        if value.is_null() {
                            continue;
                        }
                        match &acc.max {
                            None => acc.max = Some(value.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Greater) = value.compare(current) {
                                    acc.max = Some(value.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        let schema = self.hot.schema().clone();
        let mut accums = vec![Accum::default(); aggregates.len()];

        let cold_projection =
            Self::build_cold_aggregate_projection(schema.columns.len(), aggregates, &[])?;

        let mut hot_scanner = self
            .hot
            .scan(&cold_projection.columns, Some(where_expr))
            .ok()?;
        let mut hot_skip: FxHashSet<i64> = FxHashSet::with_capacity_and_hasher(
            hot_scanner.estimated_count().unwrap_or(1024).max(1024),
            Default::default(),
        );
        while hot_scanner.next() {
            hot_skip.insert(hot_scanner.current_row_id().ok()?);
            update_accums(&mut accums, &cold_projection.aggregates, hot_scanner.row());
        }
        if hot_scanner.err().is_some() {
            return None;
        }
        if hot_scanner.close().is_err() {
            return None;
        }
        self.hot.collect_hot_row_ids_into(&mut hot_skip);
        self.segment_mgr
            .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

        let mut scanners = self.create_segment_scanners_filtered_exact_projection(
            &cold_projection.columns,
            Some(where_expr),
            hot_skip,
        );
        for scanner in scanners.iter_mut() {
            while scanner.next() {
                update_accums(&mut accums, &cold_projection.aggregates, scanner.row());
            }
            if scanner.err().is_some() {
                return None;
            }
            if scanner.close().is_err() {
                return None;
            }
        }

        Some(
            aggregates
                .iter()
                .zip(accums.iter())
                .map(|((op, col_idx), acc)| match op {
                    AggregateOp::Count | AggregateOp::CountStar => Value::Integer(acc.count),
                    AggregateOp::Sum => {
                        if acc.count == 0 {
                            Value::Null(DataType::Float)
                        } else if acc.is_int_agg && acc.float_sum == 0.0 {
                            if acc.int_sum >= i64::MIN as i128 && acc.int_sum <= i64::MAX as i128 {
                                Value::Integer(acc.int_sum as i64)
                            } else {
                                Value::Float(acc.int_sum as f64)
                            }
                        } else {
                            Value::Float(acc.int_sum as f64 + acc.float_sum)
                        }
                    }
                    AggregateOp::Avg => {
                        if acc.count == 0 {
                            Value::Null(DataType::Float)
                        } else {
                            Value::Float((acc.int_sum as f64 + acc.float_sum) / acc.count as f64)
                        }
                    }
                    AggregateOp::Min => acc.min.clone().unwrap_or_else(|| {
                        let data_type = schema
                            .columns
                            .get(*col_idx)
                            .map(|col| col.data_type)
                            .unwrap_or(DataType::Null);
                        Value::Null(data_type)
                    }),
                    AggregateOp::Max => acc.max.clone().unwrap_or_else(|| {
                        let data_type = schema
                            .columns
                            .get(*col_idx)
                            .map(|col| col.data_type)
                            .unwrap_or(DataType::Null);
                        Value::Null(data_type)
                    }),
                })
                .collect(),
        )
    }

    pub(super) fn compute_grouped_aggregates_scanner(
        &self,
        group_by_indices: &[usize],
        aggregates: &[(AggregateOp, usize)],
        where_expr: Option<&dyn Expression>,
    ) -> Option<Vec<GroupedAggregateResult>> {
        if group_by_indices.is_empty() {
            return None;
        }

        #[derive(Clone)]
        struct Accum {
            count: i64,
            int_sum: i128,
            float_sum: f64,
            has_float: bool,
            overflowed: bool,
            min: Option<Value>,
            max: Option<Value>,
        }

        impl Default for Accum {
            fn default() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: 0.0,
                    has_float: false,
                    overflowed: false,
                    min: None,
                    max: None,
                }
            }
        }

        fn update_accums(accums: &mut [Accum], aggregates: &[(AggregateOp, usize)], row: &Row) {
            for (agg_idx, (op, col_idx)) in aggregates.iter().enumerate() {
                let acc = &mut accums[agg_idx];
                match op {
                    AggregateOp::CountStar => acc.count += 1,
                    AggregateOp::Count => {
                        if row.get(*col_idx).is_some_and(|value| !value.is_null()) {
                            acc.count += 1;
                        }
                    }
                    AggregateOp::Sum | AggregateOp::Avg => match row.get(*col_idx) {
                        Some(Value::Integer(value)) => {
                            match acc.int_sum.checked_add(*value as i128) {
                                Some(sum) => acc.int_sum = sum,
                                None => acc.overflowed = true,
                            }
                            acc.count += 1;
                        }
                        Some(Value::Float(value)) if !value.is_nan() => {
                            acc.float_sum += *value;
                            acc.has_float = true;
                            acc.count += 1;
                        }
                        _ => {}
                    },
                    AggregateOp::Min => {
                        let Some(value) = row.get(*col_idx) else {
                            continue;
                        };
                        if value.is_null() {
                            continue;
                        }
                        match &acc.min {
                            None => acc.min = Some(value.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Less) = value.compare(current) {
                                    acc.min = Some(value.clone());
                                }
                            }
                        }
                    }
                    AggregateOp::Max => {
                        let Some(value) = row.get(*col_idx) else {
                            continue;
                        };
                        if value.is_null() {
                            continue;
                        }
                        match &acc.max {
                            None => acc.max = Some(value.clone()),
                            Some(current) => {
                                if let Ok(std::cmp::Ordering::Greater) = value.compare(current) {
                                    acc.max = Some(value.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        fn finalize_accums(
            aggregates: &[(AggregateOp, usize)],
            accums: &[Accum],
            schema: &Schema,
        ) -> Option<Vec<Value>> {
            aggregates
                .iter()
                .zip(accums.iter())
                .map(|((op, col_idx), acc)| {
                    Some(match op {
                        AggregateOp::Count | AggregateOp::CountStar => Value::Integer(acc.count),
                        AggregateOp::Sum => {
                            if acc.overflowed {
                                return None;
                            } else if acc.count == 0 {
                                Value::Null(DataType::Float)
                            } else if acc.has_float {
                                Value::Float(acc.int_sum as f64 + acc.float_sum)
                            } else {
                                exact_integer_sum_value(acc.int_sum)?
                            }
                        }
                        AggregateOp::Avg => {
                            if acc.count == 0 {
                                Value::Null(DataType::Float)
                            } else {
                                Value::Float(
                                    (acc.int_sum as f64 + acc.float_sum) / acc.count as f64,
                                )
                            }
                        }
                        AggregateOp::Min => acc.min.clone().unwrap_or_else(|| {
                            let data_type = schema
                                .columns
                                .get(*col_idx)
                                .map(|col| col.data_type)
                                .unwrap_or(DataType::Null);
                            Value::Null(data_type)
                        }),
                        AggregateOp::Max => acc.max.clone().unwrap_or_else(|| {
                            let data_type = schema
                                .columns
                                .get(*col_idx)
                                .map(|col| col.data_type)
                                .unwrap_or(DataType::Null);
                            Value::Null(data_type)
                        }),
                    })
                })
                .collect()
        }

        let schema = self.hot.schema().clone();
        let mut group_types = Vec::with_capacity(group_by_indices.len());
        for &gb_idx in group_by_indices {
            let column = schema.columns.get(gb_idx)?;
            group_types.push(column.data_type);
        }

        fn group_entry_key(
            row: &Row,
            row_group_indices: &[usize],
            group_types: &[DataType],
        ) -> (GroupKey, Vec<Value>) {
            let values: Vec<Value> = row_group_indices
                .iter()
                .enumerate()
                .map(|(pos, &idx)| {
                    row.get(idx)
                        .cloned()
                        .unwrap_or_else(|| Value::Null(group_types[pos]))
                })
                .collect();
            let key = if values.len() == 1 {
                GroupKey::Single(CompactArc::new(values[0].clone()))
            } else {
                GroupKey::Multi(
                    values
                        .iter()
                        .cloned()
                        .map(CompactArc::new)
                        .collect::<Vec<_>>(),
                )
            };
            (key, values)
        }

        let mut groups: GroupKeyMap<(Vec<Value>, Vec<Accum>)> = GroupKeyMap::default();

        let cold_projection = Self::build_cold_aggregate_projection(
            schema.columns.len(),
            aggregates,
            group_by_indices,
        )?;
        let cold_group_indices: Vec<usize> = group_by_indices
            .iter()
            .map(|&idx| cold_projection.positions.get(idx).and_then(|pos| *pos))
            .collect::<Option<Vec<_>>>()?;

        let mut hot_scanner = self.hot.scan(&cold_projection.columns, where_expr).ok()?;
        let mut hot_skip: FxHashSet<i64> = FxHashSet::with_capacity_and_hasher(
            hot_scanner.estimated_count().unwrap_or(1024).max(1024),
            Default::default(),
        );
        while hot_scanner.next() {
            hot_skip.insert(hot_scanner.current_row_id().ok()?);
            let row = hot_scanner.row();
            let (key, group_values) = group_entry_key(row, &cold_group_indices, &group_types);
            let entry = groups
                .entry(key)
                .or_insert_with(|| (group_values, vec![Accum::default(); aggregates.len()]));
            update_accums(&mut entry.1, &cold_projection.aggregates, row);
        }
        if hot_scanner.err().is_some() {
            return None;
        }
        if hot_scanner.close().is_err() {
            return None;
        }
        self.hot.collect_hot_row_ids_into(&mut hot_skip);
        self.segment_mgr
            .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

        let mut scanners = self.create_segment_scanners_filtered_exact_projection(
            &cold_projection.columns,
            where_expr,
            hot_skip,
        );
        for scanner in scanners.iter_mut() {
            while scanner.next() {
                let row = scanner.row();
                let (key, group_values) = group_entry_key(row, &cold_group_indices, &group_types);
                let entry = groups
                    .entry(key)
                    .or_insert_with(|| (group_values, vec![Accum::default(); aggregates.len()]));
                update_accums(&mut entry.1, &cold_projection.aggregates, row);
            }
            if scanner.err().is_some() {
                return None;
            }
            if scanner.close().is_err() {
                return None;
            }
        }

        let mut results = Vec::with_capacity(groups.len());
        for (group_values, accums) in groups.into_values() {
            results.push(GroupedAggregateResult {
                group_values,
                aggregate_values: finalize_accums(aggregates, &accums, &schema)?,
            });
        }
        Some(results)
    }
}
