// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ANALYZE command executor for statistics collection
//!
//! This module implements the ANALYZE command which collects statistics
//! about tables and stores them in system tables for query optimization.

use radixdb_core::time_compat::{system_time_now, UNIX_EPOCH};
use std::hash::{Hash, Hasher};

use rustc_hash::FxHasher;

use radixdb_catalog::ObjectId;
use radixdb_core::{DataType, Error, Result, Row, RowVec, Schema, SchemaBuilder, Value, ValueSet};
use radixdb_sql::ast::AnalyzeStatement;
use radixdb_storage::statistics::{
    encode_statistics_value, is_stats_table, Histogram, CREATE_COLUMN_STATS_SQL,
    CREATE_TABLE_STATS_SQL, DEFAULT_HISTOGRAM_BUCKETS, DEFAULT_SAMPLE_SIZE, SYS_COLUMN_STATS,
    SYS_TABLE_STATS,
};
use radixdb_storage::traits::{Engine, QueryResult, Table, Transaction};
use radixdb_storage::volume::zonemap::{ZoneMapBuilder, DEFAULT_SEGMENT_SIZE};

use super::context::ExecutionContext;
use super::result::ExecutorResult;
use super::Executor;

type CollectedColumnStats = (
    i64,
    i64,
    Option<Value>,
    Option<Value>,
    i64,
    Option<Histogram>,
);

struct PendingStatisticsPublication {
    table: Box<dyn Table>,
    zone_maps: radixdb_storage::volume::zonemap::TableZoneMap,
}

const EXACT_DISTINCT_HASH_LIMIT: usize = DEFAULT_SAMPLE_SIZE;
const HLL_PRECISION: usize = 10;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;

enum BoundedDistinct {
    Exact(ValueSet),
    Approx(Box<[u8; HLL_REGISTERS]>),
}

impl Default for BoundedDistinct {
    fn default() -> Self {
        Self::Exact(ValueSet::default())
    }
}

impl BoundedDistinct {
    fn insert(&mut self, value: &Value) {
        let mut hasher = FxHasher::default();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        match self {
            Self::Exact(values) => {
                if values.contains(value) {
                    return;
                }
                if values.len() < EXACT_DISTINCT_HASH_LIMIT {
                    values.insert(value.clone());
                    return;
                }
                let mut registers = Box::new([0u8; HLL_REGISTERS]);
                for existing in values.drain() {
                    let mut hasher = FxHasher::default();
                    existing.hash(&mut hasher);
                    Self::hll_insert(&mut registers, hasher.finish());
                }
                Self::hll_insert(&mut registers, hash);
                *self = Self::Approx(registers);
            }
            Self::Approx(registers) => Self::hll_insert(registers, hash),
        }
    }

    fn hll_insert(registers: &mut [u8; HLL_REGISTERS], hash: u64) {
        let index = (hash >> (64 - HLL_PRECISION)) as usize;
        let remaining = hash << HLL_PRECISION;
        let rank = remaining.leading_zeros().saturating_add(1) as u8;
        registers[index] = registers[index].max(rank);
    }

    fn estimate(&self) -> u64 {
        match self {
            Self::Exact(values) => values.len() as u64,
            Self::Approx(registers) => {
                let m = HLL_REGISTERS as f64;
                let harmonic: f64 = registers
                    .iter()
                    .map(|rank| 2f64.powi(-(*rank as i32)))
                    .sum();
                let alpha = 0.7213 / (1.0 + 1.079 / m);
                let raw = alpha * m * m / harmonic.max(f64::MIN_POSITIVE);
                let zeroes = registers.iter().filter(|&&rank| rank == 0).count();
                let estimate = if zeroes > 0 {
                    m * (m / zeroes as f64).ln()
                } else {
                    raw
                };
                estimate.round().max(1.0) as u64
            }
        }
    }
}

fn canonical_table_stats_schema() -> Schema {
    let mut schema = SchemaBuilder::new(SYS_TABLE_STATS)
        .add_with_constraints("id", DataType::Integer, false, true, true, None, None)
        .add("table_name", DataType::Text)
        .add_with_constraints(
            "row_count",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_with_constraints(
            "page_count",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_with_constraints(
            "avg_row_size",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_nullable("last_analyzed", DataType::Timestamp)
        .build();
    schema
        .register_primary_key_constraint(vec!["id".to_owned()])
        .expect("canonical statistics primary key must be valid");
    schema
        .register_unique_constraint(vec!["table_name".to_owned()])
        .expect("canonical statistics unique key must be valid");
    schema
}

fn canonical_column_stats_schema() -> Schema {
    let mut schema = SchemaBuilder::new(SYS_COLUMN_STATS)
        .add_with_constraints("id", DataType::Integer, false, true, true, None, None)
        .add("table_name", DataType::Text)
        .add("column_name", DataType::Text)
        .add_with_constraints(
            "null_count",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_with_constraints(
            "distinct_count",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_nullable("min_value", DataType::Text)
        .add_nullable("max_value", DataType::Text)
        .add_with_constraints(
            "avg_width",
            DataType::Integer,
            false,
            false,
            false,
            Some("0".to_string()),
            None,
        )
        .set_last_default_value(Some(Value::Integer(0)))
        .add_nullable("histogram", DataType::Text)
        .build();
    schema
        .register_primary_key_constraint(vec!["id".to_owned()])
        .expect("canonical column-statistics primary key must be valid");
    schema
}

fn validate_statistics_schema(actual: &Schema, expected: &Schema) -> Result<()> {
    if actual.columns != expected.columns
        || actual.foreign_keys != expected.foreign_keys
        || actual.table_checks != expected.table_checks
    {
        return Err(Error::invalid_argument(format!(
            "system statistics table '{}' has an incompatible schema",
            actual.table_name
        )));
    }
    Ok(())
}

fn stage_statistics_table_catalog(
    catalog: &mut crate::catalog::DdlTransaction,
    schema: &mut Schema,
    create_sql: &str,
) -> Result<()> {
    schema.ensure_catalog_identity();
    let table_id = ObjectId::from_user_bytes(schema.catalog_id())
        .map_err(|error| Error::internal(format!("statistics catalog ID rejected: {error}")))?;
    let mut statements =
        radixdb_sql::parse_sql(create_sql).map_err(|error| Error::Parse(error.to_string()))?;
    if statements.len() != 1 {
        return Err(Error::internal(
            "statistics bootstrap must contain one CREATE TABLE statement",
        ));
    }
    catalog.stage_statement_with_object_ids(
        statements
            .pop()
            .expect("single statistics CREATE TABLE statement exists"),
        [table_id],
    )
}

#[derive(Default)]
struct ColumnStatsAccumulator {
    null_count: i64,
    distinct: BoundedDistinct,
    min_value: Option<Value>,
    max_value: Option<Value>,
    total_width: usize,
    numeric_seen: u64,
    histogram_sample: Vec<Value>,
}

impl ColumnStatsAccumulator {
    fn update(&mut self, value: Option<&Value>, width: usize, column_index: usize) {
        self.total_width = self.total_width.saturating_add(width);
        let Some(value) = value else {
            self.null_count += 1;
            return;
        };
        if value.is_null() {
            self.null_count += 1;
            return;
        }

        self.distinct.insert(value);
        if self
            .min_value
            .as_ref()
            .is_none_or(|minimum| value < minimum)
        {
            self.min_value = Some(value.clone());
        }
        if self
            .max_value
            .as_ref()
            .is_none_or(|maximum| value > maximum)
        {
            self.max_value = Some(value.clone());
        }

        if matches!(value, Value::Integer(_) | Value::Float(_)) {
            self.numeric_seen += 1;
            if self.histogram_sample.len() < DEFAULT_SAMPLE_SIZE {
                self.histogram_sample.push(value.clone());
            } else {
                let slot = deterministic_reservoir_slot(self.numeric_seen, column_index);
                if slot < DEFAULT_SAMPLE_SIZE as u64 {
                    self.histogram_sample[slot as usize] = value.clone();
                }
            }
        }
    }

    fn finish(mut self, row_count: usize) -> CollectedColumnStats {
        self.histogram_sample.sort();
        let histogram = (self.histogram_sample.len() >= DEFAULT_HISTOGRAM_BUCKETS * 2)
            .then(|| {
                Histogram::from_sorted_sample(
                    &self.histogram_sample,
                    DEFAULT_HISTOGRAM_BUCKETS,
                    self.numeric_seen,
                )
            })
            .flatten();
        (
            self.null_count,
            self.distinct.estimate().min(row_count as u64) as i64,
            self.min_value,
            self.max_value,
            self.total_width.checked_div(row_count).unwrap_or(0) as i64,
            histogram,
        )
    }
}

fn deterministic_reservoir_slot(seen: u64, column_index: usize) -> u64 {
    let mut value = seen ^ (column_index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) % seen.max(1)
}

impl Executor {
    /// Execute ANALYZE statement
    ///
    /// Collects statistics for the specified table (or all tables if none specified)
    /// and stores them in the system statistics tables.
    pub(crate) fn execute_analyze(
        &self,
        stmt: &AnalyzeStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        if self.has_active_transaction() {
            return Err(Error::NotSupported(
                "ANALYZE owns an atomic statistics transaction and cannot run inside an explicit transaction"
                    .to_string(),
            ));
        }

        // Ensure system tables exist
        self.ensure_stats_tables_exist()?;

        // Get list of tables to analyze
        let tables_to_analyze: Vec<String> = if let Some(ref table_name) = stmt.table_name {
            // Analyze specific table
            vec![table_name.to_string()]
        } else {
            // Analyze all tables - need a transaction to list tables
            let tx = self.engine.begin_transaction()?;
            let all_tables = tx.list_tables()?;
            all_tables
                .into_iter()
                .filter(|name| !is_stats_table(name))
                .collect()
        };

        let targeted = stmt.table_name.is_some();
        let mut analyzed_count = 0;
        let mut failures = Vec::new();

        for table_name in &tables_to_analyze {
            // Skip system tables
            if is_stats_table(table_name) {
                continue;
            }

            // Begin a transaction for this table's analysis
            let mut tx = self.engine.begin_transaction()?;

            let success = match self.analyze_table(&mut *tx, table_name) {
                Ok(publication) => {
                    tx.commit()?;
                    publication.table.set_zone_maps(publication.zone_maps);
                    analyzed_count += 1;
                    true
                }
                Err(e) => {
                    let _ = tx.rollback();
                    if targeted {
                        return Err(e);
                    }
                    failures.push(format!("{table_name}: {e}"));
                    false
                }
            };

            // Invalidate cached statistics AFTER transaction is dropped
            // This avoids potential lock ordering issues between transaction locks
            // and stats_cache write lock
            if success {
                self.get_query_planner().invalidate_stats_cache(table_name);
            }
        }

        if !failures.is_empty() {
            return Err(Error::internal(format!(
                "ANALYZE completed with {} failed table(s): {}",
                failures.len(),
                failures.join("; ")
            )));
        }

        // Return result showing how many tables were analyzed
        let columns = vec!["tables_analyzed".to_string()];
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, Row::from_values(vec![Value::Integer(analyzed_count)])));

        Ok(Box::new(ExecutorResult::new(columns, rows)))
    }

    /// Ensure the system statistics tables exist
    fn ensure_stats_tables_exist(&self) -> Result<()> {
        let mut tx = self.engine.begin_transaction()?;
        let tables = tx.list_tables()?;
        let has_table_stats = tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(SYS_TABLE_STATS));
        let has_column_stats = tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(SYS_COLUMN_STATS));
        let mut expected_table = canonical_table_stats_schema();
        let mut expected_column = canonical_column_stats_schema();
        if has_table_stats {
            let table = tx.get_table(SYS_TABLE_STATS)?;
            validate_statistics_schema(table.schema(), &expected_table)?;
            let index = table
                .get_indexes()
                .into_iter()
                .find(|index| {
                    index.is_unique()
                        && index.column_names().len() == 1
                        && index.column_names()[0].eq_ignore_ascii_case("table_name")
                })
                .ok_or_else(|| {
                    Error::invalid_argument(
                        "system table '_sys_table_stats' is missing its table_name UNIQUE index",
                    )
                })?;
            debug_assert!(index.is_unique());
        }
        if has_column_stats {
            let table = tx.get_table(SYS_COLUMN_STATS)?;
            validate_statistics_schema(table.schema(), &expected_column)?;
        }
        if has_table_stats && has_column_stats {
            return tx.commit();
        }

        let generation = self.engine.pin_catalog()?;
        let mut catalog = crate::catalog::DdlTransaction::begin_shared(generation);
        if !has_table_stats {
            stage_statistics_table_catalog(
                &mut catalog,
                &mut expected_table,
                CREATE_TABLE_STATS_SQL,
            )?;
            tx.create_table(SYS_TABLE_STATS, expected_table)?;
            tx.create_table_index(
                SYS_TABLE_STATS,
                "uq__sys_table_stats_table_name",
                &["table_name".to_string()],
                true,
            )?;
        }
        if !has_column_stats {
            stage_statistics_table_catalog(
                &mut catalog,
                &mut expected_column,
                CREATE_COLUMN_STATS_SQL,
            )?;
            tx.create_table(SYS_COLUMN_STATS, expected_column)?;
        }
        if let Some(mutation) = catalog.pending_mutation()? {
            tx.stage_catalog_mutation(mutation)?;
        }
        tx.commit()
    }

    /// Analyze a single table and update statistics
    fn analyze_table(
        &self,
        tx: &mut dyn Transaction,
        table_name: &str,
    ) -> Result<PendingStatisticsPublication> {
        let table = tx.get_table(table_name)?;
        let schema = table.schema().clone();
        let zone_map_generation = table.zone_map_generation();

        let mut zone_map_builder =
            ZoneMapBuilder::new_for_generation(DEFAULT_SEGMENT_SIZE, zone_map_generation);
        let mut column_stats: Vec<ColumnStatsAccumulator> = (0..schema.columns.len())
            .map(|_| ColumnStatsAccumulator::default())
            .collect();
        let mut row_count = 0usize;
        let mut total_size = 0usize;

        // One snapshot walk owns every persisted denominator. Native tables
        // stream this callback directly from MVCC/artifact-backed owners; only bounded
        // sketches/reservoirs and the zone-map generation remain resident.
        table.visit_visible_rows(&mut |row_id, row| {
            row_count = row_count.saturating_add(1);
            let row_size = self.estimate_row_size(&row);
            total_size = total_size.saturating_add(row_size);
            zone_map_builder.add_row_from_schema_with_id(row_id, &schema, &row);
            for (column_index, accumulator) in column_stats.iter_mut().enumerate() {
                let value = row.get(column_index);
                let width = value.map_or(1, |value| self.estimate_value_size(value));
                accumulator.update(value, width, column_index);
            }
            Ok(())
        })?;

        let zone_maps = zone_map_builder.build();
        let avg_row_size = total_size.checked_div(row_count).unwrap_or(0);

        // Estimate page count (assuming 8KB pages)
        let page_count = total_size.div_ceil(8192).max(1);
        // Collect column statistics before dropping table
        let column_stats_list: Vec<_> = schema
            .columns
            .iter()
            .zip(column_stats)
            .map(|(column, stats)| (column.name.clone(), stats.finish(row_count)))
            .collect();

        let row_count = i64::try_from(row_count)
            .map_err(|_| Error::invalid_argument("ANALYZE row_count exceeds INTEGER domain"))?;
        let page_count = i64::try_from(page_count)
            .map_err(|_| Error::invalid_argument("ANALYZE page_count exceeds INTEGER domain"))?;
        let avg_row_size = i64::try_from(avg_row_size)
            .map_err(|_| Error::invalid_argument("ANALYZE avg_row_size exceeds INTEGER domain"))?;

        self.replace_statistics(
            tx,
            table_name,
            row_count,
            page_count,
            avg_row_size,
            &column_stats_list,
        )?;

        Ok(PendingStatisticsPublication { table, zone_maps })
    }

    /// Estimate size of a row in bytes
    fn estimate_row_size(&self, row: &Row) -> usize {
        row.iter().map(|v| self.estimate_value_size(v)).sum()
    }

    /// Estimate size of a value in bytes
    fn estimate_value_size(&self, value: &Value) -> usize {
        match value {
            Value::Null(_) => 1,
            Value::Boolean(_) => 1,
            Value::Integer(_) => 8,
            Value::Float(_) => 8,
            Value::Text(s) => s.len() + 4, // string + length prefix
            Value::Timestamp(_) => 8,
            Value::Extension(data) => data.len() + 4,
        }
    }

    fn statistics_row_ids(
        table: &dyn Table,
        table_name_column: usize,
        table_name: &str,
    ) -> Result<Vec<i64>> {
        let rows = table.collect_all_rows(None)?;
        Ok(rows
            .iter()
            .filter_map(|(row_id, row)| match row.get(table_name_column) {
                Some(Value::Text(name)) if name.eq_ignore_ascii_case(table_name) => Some(*row_id),
                _ => None,
            })
            .collect())
    }

    fn delete_statistics_rows(
        table: &mut dyn Table,
        table_name_column: usize,
        table_name: &str,
    ) -> Result<()> {
        let row_ids = Self::statistics_row_ids(table, table_name_column, table_name)?;
        if !row_ids.is_empty() {
            table.delete_by_row_ids(&row_ids)?;
        }
        Ok(())
    }

    /// Replace one table's complete statistics generation inside the owning
    /// storage transaction. Any late table/column write failure therefore
    /// rolls back the entire durable generation.
    fn replace_statistics(
        &self,
        tx: &mut dyn Transaction,
        table_name: &str,
        row_count: i64,
        page_count: i64,
        avg_row_size: i64,
        columns: &[(String, CollectedColumnStats)],
    ) -> Result<()> {
        let now = system_time_now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let analyzed_at = chrono::DateTime::from_timestamp(now, 0)
            .map(Value::timestamp)
            .ok_or_else(|| Error::internal("ANALYZE timestamp is outside chrono range"))?;

        let mut table_stats = tx.get_table(SYS_TABLE_STATS)?;
        let replacement = Row::from_values(vec![
            Value::Null(DataType::Integer),
            Value::text(table_name),
            Value::Integer(row_count),
            Value::Integer(page_count),
            Value::Integer(avg_row_size),
            analyzed_at,
        ]);
        let existing = Self::statistics_row_ids(table_stats.as_ref(), 1, table_name)?;
        if let Some(&row_id) = existing.first() {
            let mut replacement = Some(replacement);
            table_stats.update_by_row_ids(&[row_id], &mut |row| {
                let mut values = replacement
                    .take()
                    .ok_or_else(|| Error::internal("statistics replacement row was reused"))?
                    .into_values();
                values[0] = row
                    .get(0)
                    .cloned()
                    .ok_or_else(|| Error::internal("statistics row is missing its primary key"))?;
                Ok((Row::from_values(values), true))
            })?;
            if existing.len() > 1 {
                table_stats.delete_by_row_ids(&existing[1..])?;
            }
        } else {
            table_stats.insert_discard(replacement)?;
        }
        drop(table_stats);

        let mut column_stats = tx.get_table(SYS_COLUMN_STATS)?;
        Self::delete_statistics_rows(column_stats.as_mut(), 1, table_name)?;
        for (column_name, stats) in columns {
            let min_value = stats
                .2
                .as_ref()
                .map(|value| Value::text(encode_statistics_value(value)))
                .unwrap_or(Value::Null(DataType::Text));
            let max_value = stats
                .3
                .as_ref()
                .map(|value| Value::text(encode_statistics_value(value)))
                .unwrap_or(Value::Null(DataType::Text));
            let histogram = stats
                .5
                .as_ref()
                .map(|histogram| Value::text(histogram.to_json()))
                .unwrap_or(Value::Null(DataType::Text));
            column_stats.insert_discard(Row::from_values(vec![
                Value::Null(DataType::Integer),
                Value::text(table_name),
                Value::text(column_name),
                Value::Integer(stats.0),
                Value::Integer(stats.1),
                min_value,
                max_value,
                Value::Integer(stats.4),
                histogram,
            ]))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use radixdb_core::DataType;
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use radixdb_storage::statistics::{CREATE_TABLE_STATS_SQL, DEFAULT_SAMPLE_SIZE};

    fn executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn scalar_i64(executor: &Executor, sql: &str) -> i64 {
        let mut result = executor.execute(sql).unwrap();
        assert!(result.next(), "query returned no row: {sql}");
        result
            .row()
            .get(0)
            .and_then(Value::as_int64)
            .expect("query did not return an INTEGER scalar")
    }

    #[test]
    fn r3_l04_batch_c_sampled_statistics_use_full_table_domain() {
        let executor = executor();
        executor
            .execute("CREATE TABLE sampled_stats (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();

        let mut tx = executor.begin_transaction().unwrap();
        let mut table = tx.get_table("sampled_stats").unwrap();
        let rows = (0..=DEFAULT_SAMPLE_SIZE)
            .map(|id| {
                Row::from_values(vec![Value::Integer(id as i64), Value::Null(DataType::Text)])
            })
            .collect();
        table.insert_batch(rows).unwrap();
        drop(table);
        tx.commit().unwrap();

        let mut analyzed = executor.execute("ANALYZE sampled_stats").unwrap();
        assert!(analyzed.next());
        assert_eq!(analyzed.row().get(0).and_then(Value::as_int64), Some(1));

        assert_eq!(
            scalar_i64(
                &executor,
                "SELECT null_count FROM _sys_column_stats \
                 WHERE table_name = 'sampled_stats' AND column_name = 'payload'",
            ),
            (DEFAULT_SAMPLE_SIZE + 1) as i64,
        );

        let expected_pages = ((DEFAULT_SAMPLE_SIZE + 1) * 9).div_ceil(8192).max(1) as i64;
        assert_eq!(
            scalar_i64(
                &executor,
                "SELECT page_count FROM _sys_table_stats WHERE table_name = 'sampled_stats'",
            ),
            expected_pages,
        );
    }

    #[test]
    fn r8_l01_batch_g_analyze_accumulators_stay_bounded_above_sample_limit() {
        let row_count = DEFAULT_SAMPLE_SIZE * 3;
        let mut stats = ColumnStatsAccumulator::default();
        for value in 0..row_count {
            let value = Value::Integer(value as i64);
            stats.update(Some(&value), 8, 0);
        }

        assert_eq!(stats.histogram_sample.len(), DEFAULT_SAMPLE_SIZE);
        assert!(matches!(stats.distinct, BoundedDistinct::Approx(_)));
        let collected = stats.finish(row_count);
        assert_eq!(collected.0, 0);
        assert_eq!(collected.4, 8);
        assert!(collected.1 > (row_count as i64 * 9 / 10));
        assert!(collected.1 < (row_count as i64 * 11 / 10));
        assert_eq!(collected.5.unwrap().total_rows(), row_count as u64);
    }

    #[test]
    fn r6_exact_distinct_retains_values_until_a_new_identity_crosses_the_limit() {
        let mut distinct = BoundedDistinct::default();
        for value in 0..EXACT_DISTINCT_HASH_LIMIT {
            distinct.insert(&Value::Integer(value as i64));
        }
        distinct.insert(&Value::Float(42.0));
        assert!(matches!(distinct, BoundedDistinct::Exact(_)));
        assert_eq!(distinct.estimate(), EXACT_DISTINCT_HASH_LIMIT as u64);

        distinct.insert(&Value::Integer(EXACT_DISTINCT_HASH_LIMIT as i64));
        assert!(matches!(distinct, BoundedDistinct::Approx(_)));
    }

    #[test]
    fn r6_statistics_bootstrap_is_atomic_and_targeted_failure_is_visible() {
        let subject = executor();
        subject
            .execute("CREATE TABLE _sys_column_stats (id INTEGER PRIMARY KEY, broken TEXT)")
            .unwrap();

        let error = match subject.execute("ANALYZE missing_target") {
            Err(error) => error,
            Ok(_) => panic!("targeted ANALYZE must surface catalog/bootstrap failure"),
        };
        assert!(error.to_string().contains("incompatible schema"), "{error}");
        assert!(
            subject.execute("SELECT * FROM _sys_table_stats").is_err(),
            "the sibling statistics table was published despite rollback"
        );
        assert!(subject
            .execute("SELECT COUNT(*) FROM _sys_column_stats")
            .is_ok());

        let clean = executor();
        let error = match clean.execute("ANALYZE definitely_missing") {
            Err(error) => error,
            Ok(_) => panic!("a missing explicit target must never look successful"),
        };
        assert!(error.to_string().contains("definitely_missing"), "{error}");
    }

    #[test]
    fn r3_l04_batch_c_failed_analyze_publishes_nothing() {
        let executor = executor();
        executor
            .execute("CREATE TABLE analyze_target (id INTEGER PRIMARY KEY, payload INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO analyze_target VALUES (1, 10), (2, 20)")
            .unwrap();
        executor.execute(CREATE_TABLE_STATS_SQL).unwrap();
        executor
            .execute(
                "CREATE TABLE _sys_column_stats (\
                    id INTEGER PRIMARY KEY AUTO_INCREMENT, broken TEXT)",
            )
            .unwrap();

        {
            let tx = executor.begin_transaction().unwrap();
            let table = tx.get_table("analyze_target").unwrap();
            assert!(table.get_zone_maps().is_none());
        }

        let error = match executor.execute("ANALYZE analyze_target") {
            Err(error) => error,
            Ok(_) => panic!("incompatible statistics catalog must fail closed"),
        };
        assert!(error.to_string().contains("incompatible schema"), "{error}");

        assert_eq!(
            scalar_i64(
                &executor,
                "SELECT COUNT(*) FROM _sys_table_stats WHERE table_name = 'analyze_target'",
            ),
            0,
        );
        let tx = executor.begin_transaction().unwrap();
        let table = tx.get_table("analyze_target").unwrap();
        assert!(
            table.get_zone_maps().is_none(),
            "failed ANALYZE published a new zone-map generation"
        );
    }
}
