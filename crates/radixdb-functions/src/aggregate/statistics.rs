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

//! Statistical aggregate functions: STDDEV, VARIANCE, MEDIAN

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use radixdb_core::{Error, Result, Value};

use crate::{AggregateFunction, FunctionDataType, FunctionInfo, FunctionSignature, FunctionType};

use super::DistinctTracker;

/// Helper to extract f64 from Value
fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

/// Constant-memory, numerically stable variance state (Welford/Chan).
#[derive(Default)]
struct RunningVariance {
    count: u64,
    mean: f64,
    m2: f64,
}

impl RunningVariance {
    #[inline]
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    #[inline]
    fn population_variance(&self) -> Option<f64> {
        (self.count > 0).then(|| self.m2 / self.count as f64)
    }

    #[inline]
    fn sample_variance(&self) -> Option<f64> {
        (self.count > 1).then(|| self.m2 / (self.count - 1) as f64)
    }

    #[inline]
    fn reset(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// STDDEV_POP / STDDEV - Population Standard Deviation
// ============================================================================

/// STDDEV_POP aggregate function (Population Standard Deviation)
///
/// Computes the population standard deviation of all non-NULL values.
/// Formula: sqrt(sum((x - mean)^2) / N)
#[derive(Default)]
pub struct StddevPopFunction {
    state: RunningVariance,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for StddevPopFunction {
    fn name(&self) -> &str {
        "STDDEV_POP"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "STDDEV_POP",
            FunctionType::Aggregate,
            "Returns the population standard deviation of non-NULL values",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        if value.is_null() {
            return;
        }

        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return;
            }
        }

        if let Some(f) = value_to_f64(value) {
            self.state.add(f);
        }
    }

    fn result(&self) -> Value {
        self.state
            .population_variance()
            .map(|variance| Value::Float(variance.sqrt()))
            .unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.state.reset();
        self.distinct_tracker = None;
    }
}

/// STDDEV aggregate function (alias for STDDEV_POP)
#[derive(Default)]
pub struct StddevFunction {
    inner: StddevPopFunction,
}

impl AggregateFunction for StddevFunction {
    fn name(&self) -> &str {
        "STDDEV"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "STDDEV",
            FunctionType::Aggregate,
            "Returns the population standard deviation (alias for STDDEV_POP)",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        self.inner.accumulate(value, distinct);
    }

    fn result(&self) -> Value {
        self.inner.result()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

// ============================================================================
// STDDEV_SAMP - Sample Standard Deviation
// ============================================================================

/// STDDEV_SAMP aggregate function (Sample Standard Deviation)
///
/// Computes the sample standard deviation of all non-NULL values.
/// Formula: sqrt(sum((x - mean)^2) / (N - 1))
/// Uses Bessel's correction (N-1 instead of N).
#[derive(Default)]
pub struct StddevSampFunction {
    state: RunningVariance,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for StddevSampFunction {
    fn name(&self) -> &str {
        "STDDEV_SAMP"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "STDDEV_SAMP",
            FunctionType::Aggregate,
            "Returns the sample standard deviation of non-NULL values",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        if value.is_null() {
            return;
        }

        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return;
            }
        }

        if let Some(f) = value_to_f64(value) {
            self.state.add(f);
        }
    }

    fn result(&self) -> Value {
        self.state
            .sample_variance()
            .map(|variance| Value::Float(variance.sqrt()))
            .unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.state.reset();
        self.distinct_tracker = None;
    }
}

// ============================================================================
// VAR_POP / VARIANCE - Population Variance
// ============================================================================

/// VAR_POP aggregate function (Population Variance)
///
/// Computes the population variance of all non-NULL values.
/// Formula: sum((x - mean)^2) / N
#[derive(Default)]
pub struct VarPopFunction {
    state: RunningVariance,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for VarPopFunction {
    fn name(&self) -> &str {
        "VAR_POP"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VAR_POP",
            FunctionType::Aggregate,
            "Returns the population variance of non-NULL values",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        if value.is_null() {
            return;
        }

        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return;
            }
        }

        if let Some(f) = value_to_f64(value) {
            self.state.add(f);
        }
    }

    fn result(&self) -> Value {
        self.state
            .population_variance()
            .map(Value::Float)
            .unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.state.reset();
        self.distinct_tracker = None;
    }
}

/// VARIANCE aggregate function (alias for VAR_POP)
#[derive(Default)]
pub struct VarianceFunction {
    inner: VarPopFunction,
}

impl AggregateFunction for VarianceFunction {
    fn name(&self) -> &str {
        "VARIANCE"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VARIANCE",
            FunctionType::Aggregate,
            "Returns the population variance (alias for VAR_POP)",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        self.inner.accumulate(value, distinct);
    }

    fn result(&self) -> Value {
        self.inner.result()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

// ============================================================================
// VAR_SAMP - Sample Variance
// ============================================================================

/// VAR_SAMP aggregate function (Sample Variance)
///
/// Computes the sample variance of all non-NULL values.
/// Formula: sum((x - mean)^2) / (N - 1)
/// Uses Bessel's correction (N-1 instead of N).
#[derive(Default)]
pub struct VarSampFunction {
    state: RunningVariance,
    distinct_tracker: Option<DistinctTracker>,
}

impl AggregateFunction for VarSampFunction {
    fn name(&self) -> &str {
        "VAR_SAMP"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VAR_SAMP",
            FunctionType::Aggregate,
            "Returns the sample variance of non-NULL values",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        if value.is_null() {
            return;
        }

        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return;
            }
        }

        if let Some(f) = value_to_f64(value) {
            self.state.add(f);
        }
    }

    fn result(&self) -> Value {
        self.state
            .sample_variance()
            .map(Value::Float)
            .unwrap_or_else(Value::null_unknown)
    }

    fn reset(&mut self) {
        self.state.reset();
        self.distinct_tracker = None;
    }
}

// ============================================================================
// MEDIAN
// ============================================================================

const MEDIAN_MEMORY_VALUES: usize = 8_192;
static MEDIAN_SPOOL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct MedianSpill {
    path: PathBuf,
    value_count: u64,
}

impl Drop for MedianSpill {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// MEDIAN aggregate function
///
/// Returns the median (middle value) of all non-NULL values.
/// For even number of values, returns the average of the two middle values.
pub struct MedianFunction {
    values: Vec<f64>,
    spill: Option<MedianSpill>,
    spill_error: Option<String>,
    distinct_tracker: Option<DistinctTracker>,
}

impl Default for MedianFunction {
    fn default() -> Self {
        Self {
            values: Vec::with_capacity(MEDIAN_MEMORY_VALUES),
            spill: None,
            spill_error: None,
            distinct_tracker: None,
        }
    }
}

impl MedianFunction {
    fn spill_buffer(&mut self) {
        if self.values.is_empty() || self.spill_error.is_some() {
            return;
        }
        let result = (|| -> std::io::Result<()> {
            if self.spill.is_none() {
                let sequence = MEDIAN_SPOOL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "radixdb-median-{}-{sequence}.bin",
                    std::process::id()
                ));
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)?;
                self.spill = Some(MedianSpill {
                    path,
                    value_count: 0,
                });
            }
            let spill = self.spill.as_mut().unwrap();
            let mut file = OpenOptions::new().append(true).open(&spill.path)?;
            for value in &self.values {
                file.write_all(&value.to_bits().to_le_bytes())?;
            }
            spill.value_count = spill.value_count.saturating_add(self.values.len() as u64);
            Ok(())
        })();
        if let Err(error) = result {
            self.spill_error = Some(error.to_string());
        }
        self.values.clear();
    }

    fn order_key(value: f64) -> u64 {
        if value.is_nan() {
            return u64::MAX;
        }
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        if bits >> 63 == 0 {
            bits ^ (1u64 << 63)
        } else {
            !bits
        }
    }

    fn value_from_order_key(key: u64) -> f64 {
        if key == u64::MAX {
            return f64::NAN;
        }
        let bits = if key >> 63 == 1 {
            key ^ (1u64 << 63)
        } else {
            !key
        };
        f64::from_bits(bits)
    }

    fn for_each_order_key(&self, mut visitor: impl FnMut(u64)) -> Result<()> {
        if let Some(spill) = &self.spill {
            let file = File::open(&spill.path)
                .map_err(|error| Error::io(format!("MEDIAN spill open failed: {error}")))?;
            let mut reader = BufReader::new(file);
            let mut bytes = [0u8; 8];
            for _ in 0..spill.value_count {
                reader
                    .read_exact(&mut bytes)
                    .map_err(|error| Error::io(format!("MEDIAN spill read failed: {error}")))?;
                visitor(Self::order_key(f64::from_bits(u64::from_le_bytes(bytes))));
            }
        }
        for &value in &self.values {
            visitor(Self::order_key(value));
        }
        Ok(())
    }

    fn select_order_key(&self, mut rank: u64) -> Result<u64> {
        let mut prefix = 0u64;
        let mut mask = 0u64;
        for shift in (0..=56).rev().step_by(8) {
            let mut counts = [0u64; 256];
            self.for_each_order_key(|key| {
                if key & mask == prefix {
                    counts[((key >> shift) & 0xff) as usize] += 1;
                }
            })?;
            let mut selected = None;
            for (byte, count) in counts.into_iter().enumerate() {
                if rank < count {
                    selected = Some(byte as u64);
                    break;
                }
                rank -= count;
            }
            let byte = selected.ok_or_else(|| Error::internal("MEDIAN spill rank is invalid"))?;
            prefix |= byte << shift;
            mask |= 0xffu64 << shift;
        }
        Ok(prefix)
    }

    fn try_median(&self) -> Result<Value> {
        if let Some(error) = &self.spill_error {
            return Err(Error::io(format!("MEDIAN spill write failed: {error}")));
        }
        let count = self
            .spill
            .as_ref()
            .map_or(0, |spill| spill.value_count)
            .saturating_add(self.values.len() as u64);
        if count == 0 {
            return Ok(Value::null_unknown());
        }
        let upper = Self::value_from_order_key(self.select_order_key(count / 2)?);
        if count % 2 == 1 {
            Ok(Value::Float(upper))
        } else {
            let lower = Self::value_from_order_key(self.select_order_key(count / 2 - 1)?);
            Ok(Value::Float(lower / 2.0 + upper / 2.0))
        }
    }
}

impl AggregateFunction for MedianFunction {
    fn name(&self) -> &str {
        "MEDIAN"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "MEDIAN",
            FunctionType::Aggregate,
            "Returns the median (middle value) of non-NULL values",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn accumulate(&mut self, value: &Value, distinct: bool) {
        if value.is_null() {
            return;
        }

        if distinct {
            if self.distinct_tracker.is_none() {
                self.distinct_tracker = Some(DistinctTracker::default());
            }
            if !self.distinct_tracker.as_mut().unwrap().check_and_add(value) {
                return;
            }
        }

        if let Some(f) = value_to_f64(value) {
            self.values.push(f);
            if self.values.len() >= MEDIAN_MEMORY_VALUES {
                self.spill_buffer();
            }
        }
    }

    fn result(&self) -> Value {
        self.try_median().unwrap_or_else(|_| Value::null_unknown())
    }

    fn try_result(&self) -> Result<Value> {
        self.try_median()
    }

    fn reset(&mut self) {
        self.values.clear();
        self.spill = None;
        self.spill_error = None;
        self.distinct_tracker = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // STDDEV_POP tests
    #[test]
    fn test_stddev_pop_basic() {
        let mut stddev = StddevPopFunction::default();
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Variance = 4, StdDev = 2
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            stddev.accumulate(&Value::Float(v), false);
        }
        let result = stddev.result();
        if let Value::Float(f) = result {
            assert!((f - 2.0).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_stddev_pop_single_value() {
        let mut stddev = StddevPopFunction::default();
        stddev.accumulate(&Value::Float(5.0), false);
        // Single value has stddev = 0
        assert_eq!(stddev.result(), Value::Float(0.0));
    }

    #[test]
    fn test_stddev_pop_empty() {
        let stddev = StddevPopFunction::default();
        assert!(stddev.result().is_null());
    }

    #[test]
    fn test_stddev_alias() {
        let mut stddev = StddevFunction::default();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            stddev.accumulate(&Value::Float(v), false);
        }
        let result = stddev.result();
        if let Value::Float(f) = result {
            assert!((f - 2.0).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    // STDDEV_SAMP tests
    #[test]
    fn test_stddev_samp_basic() {
        let mut stddev = StddevSampFunction::default();
        // Values: 2, 4, 4, 4, 5, 5, 7, 9 (n=8)
        // Mean = 5, Sample Variance = 32/7 ≈ 4.571, Sample StdDev ≈ 2.138
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            stddev.accumulate(&Value::Float(v), false);
        }
        let result = stddev.result();
        if let Value::Float(f) = result {
            assert!((f - 2.138).abs() < 0.01);
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_stddev_samp_single_value() {
        let mut stddev = StddevSampFunction::default();
        stddev.accumulate(&Value::Float(5.0), false);
        // Sample stddev requires at least 2 values
        assert!(stddev.result().is_null());
    }

    // VAR_POP tests
    #[test]
    fn test_var_pop_basic() {
        let mut var = VarPopFunction::default();
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Variance = 4
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            var.accumulate(&Value::Float(v), false);
        }
        let result = var.result();
        if let Value::Float(f) = result {
            assert!((f - 4.0).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_variance_alias() {
        let mut var = VarianceFunction::default();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            var.accumulate(&Value::Float(v), false);
        }
        let result = var.result();
        if let Value::Float(f) = result {
            assert!((f - 4.0).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    // VAR_SAMP tests
    #[test]
    fn test_var_samp_basic() {
        let mut var = VarSampFunction::default();
        // Values: 2, 4, 4, 4, 5, 5, 7, 9 (n=8)
        // Mean = 5, Sample Variance = 32/7 ≈ 4.571
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            var.accumulate(&Value::Float(v), false);
        }
        let result = var.result();
        if let Value::Float(f) = result {
            assert!((f - 4.571).abs() < 0.01);
        } else {
            panic!("Expected float result");
        }
    }

    #[test]
    fn test_var_samp_single_value() {
        let mut var = VarSampFunction::default();
        var.accumulate(&Value::Float(5.0), false);
        // Sample variance requires at least 2 values
        assert!(var.result().is_null());
    }

    // MEDIAN tests
    #[test]
    fn test_median_odd_count() {
        let mut median = MedianFunction::default();
        // Values: 1, 3, 5, 7, 9 -> median = 5
        for v in [1.0, 3.0, 5.0, 7.0, 9.0] {
            median.accumulate(&Value::Float(v), false);
        }
        assert_eq!(median.result(), Value::Float(5.0));
    }

    #[test]
    fn test_median_even_count() {
        let mut median = MedianFunction::default();
        // Values: 1, 3, 5, 7 -> median = (3+5)/2 = 4
        for v in [1.0, 3.0, 5.0, 7.0] {
            median.accumulate(&Value::Float(v), false);
        }
        assert_eq!(median.result(), Value::Float(4.0));
    }

    #[test]
    fn test_median_unsorted_input() {
        let mut median = MedianFunction::default();
        // Values: 9, 1, 7, 3, 5 -> sorted: 1, 3, 5, 7, 9 -> median = 5
        for v in [9.0, 1.0, 7.0, 3.0, 5.0] {
            median.accumulate(&Value::Float(v), false);
        }
        assert_eq!(median.result(), Value::Float(5.0));
    }

    #[test]
    fn test_median_single_value() {
        let mut median = MedianFunction::default();
        median.accumulate(&Value::Float(42.0), false);
        assert_eq!(median.result(), Value::Float(42.0));
    }

    #[test]
    fn test_median_empty() {
        let median = MedianFunction::default();
        assert!(median.result().is_null());
    }

    #[test]
    fn test_median_with_integers() {
        let mut median = MedianFunction::default();
        for v in [1, 2, 3, 4, 5] {
            median.accumulate(&Value::Integer(v), false);
        }
        assert_eq!(median.result(), Value::Float(3.0));
    }

    #[test]
    fn test_median_ignores_null() {
        let mut median = MedianFunction::default();
        median.accumulate(&Value::Float(1.0), false);
        median.accumulate(&Value::null_unknown(), false);
        median.accumulate(&Value::Float(3.0), false);
        median.accumulate(&Value::null_unknown(), false);
        median.accumulate(&Value::Float(5.0), false);
        assert_eq!(median.result(), Value::Float(3.0));
    }

    #[test]
    fn test_median_distinct() {
        let mut median = MedianFunction::default();
        median.accumulate(&Value::Float(1.0), true);
        median.accumulate(&Value::Float(3.0), true);
        median.accumulate(&Value::Float(3.0), true); // duplicate
        median.accumulate(&Value::Float(5.0), true);
        // Distinct values: 1, 3, 5 -> median = 3
        assert_eq!(median.result(), Value::Float(3.0));
    }

    // Test with NULL handling
    #[test]
    fn test_statistics_ignore_nulls() {
        let mut stddev = StddevPopFunction::default();
        stddev.accumulate(&Value::Float(2.0), false);
        stddev.accumulate(&Value::null_unknown(), false);
        stddev.accumulate(&Value::Float(4.0), false);
        stddev.accumulate(&Value::null_unknown(), false);
        stddev.accumulate(&Value::Float(6.0), false);
        // Mean = 4, Variance = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 3 = 8/3
        let result = stddev.result();
        if let Value::Float(f) = result {
            let expected = (8.0_f64 / 3.0).sqrt();
            assert!((f - expected).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    // Test distinct
    #[test]
    fn test_stddev_distinct() {
        let mut stddev = StddevPopFunction::default();
        stddev.accumulate(&Value::Float(2.0), true);
        stddev.accumulate(&Value::Float(4.0), true);
        stddev.accumulate(&Value::Float(2.0), true); // duplicate
        stddev.accumulate(&Value::Float(6.0), true);
        // Distinct values: 2, 4, 6 -> Mean = 4
        let result = stddev.result();
        if let Value::Float(f) = result {
            let expected = (8.0_f64 / 3.0).sqrt();
            assert!((f - expected).abs() < 0.0001);
        } else {
            panic!("Expected float result");
        }
    }

    // Test reset
    #[test]
    fn test_stddev_reset() {
        let mut stddev = StddevPopFunction::default();
        stddev.accumulate(&Value::Float(2.0), false);
        stddev.accumulate(&Value::Float(4.0), false);
        stddev.reset();
        assert!(stddev.result().is_null());
    }

    #[test]
    fn test_median_reset() {
        let mut median = MedianFunction::default();
        median.accumulate(&Value::Float(1.0), false);
        median.accumulate(&Value::Float(2.0), false);
        median.reset();
        assert!(median.result().is_null());
    }

    #[test]
    fn r8_l01_batch_g_exact_median_spills_with_bounded_memory() {
        let mut median = MedianFunction::default();
        let count = MEDIAN_MEMORY_VALUES * 2 + 1;
        for value in (0..count).rev() {
            median.accumulate(&Value::Integer(value as i64), false);
        }

        assert!(median.values.len() < MEDIAN_MEMORY_VALUES);
        let spill_path = median.spill.as_ref().unwrap().path.clone();
        assert!(spill_path.exists());
        assert_eq!(
            median.try_result().unwrap(),
            Value::Float((count / 2) as f64)
        );

        median.reset();
        assert!(!spill_path.exists());
        assert!(median.try_result().unwrap().is_null());
    }

    #[test]
    fn median_spill_failure_is_sticky_and_fallible() {
        let mut median = MedianFunction::default();
        median.values.push(1.0);
        median.spill_error = Some("injected write failure".to_string());
        assert!(median.try_result().is_err());
        assert!(median.try_result().is_err());
    }
}
