macro_rules! segmented_table_pushdown_methods {
    () => {
        // =========================================================================
        // Deferred aggregation
        // =========================================================================

        fn compute_filtered_aggregates(
            &self,
            aggregates: &[(AggregateOp, usize)],
            where_expr: &dyn Expression,
        ) -> Option<Vec<Value>> {
            if self.snapshot_seq.is_some()
                || self.segment_mgr.seal_overlap() > 0
                || !self.segment_mgr.has_segments()
            {
                return None;
            }
            self.compute_filtered_aggregates_scanner(aggregates, where_expr)
        }

        fn compute_grouped_aggregates(
            &self,
            group_by_indices: &[usize],
            aggregates: &[(AggregateOp, usize)],
        ) -> Option<Vec<GroupedAggregateResult>> {
            if self.snapshot_seq.is_some() || group_by_indices.is_empty() {
                return None;
            }
            if !self.segment_mgr.has_segments() {
                return self
                    .hot
                    .compute_grouped_aggregates(group_by_indices, aggregates);
            }
            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }
            self.compute_grouped_aggregates_artifact_columnar(group_by_indices, aggregates)
                .or_else(|| {
                    self.compute_grouped_aggregates_scanner(group_by_indices, aggregates, None)
                })
        }

        fn compute_filtered_grouped_aggregates(
            &self,
            group_by_indices: &[usize],
            aggregates: &[(AggregateOp, usize)],
            where_expr: &dyn Expression,
        ) -> Option<Vec<GroupedAggregateResult>> {
            if self.snapshot_seq.is_some() {
                return None;
            }
            if group_by_indices.is_empty() {
                return None;
            }
            // Hot-only tables should use the regular executor path for now: the
            // hot arena grouped aggregation contract has no WHERE filter variant.
            if !self.segment_mgr.has_segments() {
                return None;
            }
            // During seal, hot+cold overlap makes aggregation unreliable.
            if self.segment_mgr.seal_overlap() > 0 {
                return None;
            }
            self.compute_grouped_aggregates_scanner(group_by_indices, aggregates, Some(where_expr))
        }
    };
}

pub(super) use segmented_table_pushdown_methods;
