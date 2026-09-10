use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::artifacts::ResourceSnapshot;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CounterSet(BTreeMap<String, u64>);

impl CounterSet {
    pub fn increment(&mut self, name: impl Into<String>) -> Result<(), String> {
        self.add(name, 1)
    }

    pub fn add(&mut self, name: impl Into<String>, amount: u64) -> Result<(), String> {
        let name = name.into();
        if name.is_empty() {
            return Err("counter name must not be empty".to_string());
        }
        let value = self.0.entry(name.clone()).or_default();
        *value = value
            .checked_add(amount)
            .ok_or_else(|| format!("counter `{name}` overflow"))?;
        Ok(())
    }

    pub fn get(&self, name: &str) -> u64 {
        self.0.get(name).copied().unwrap_or(0)
    }

    pub fn values(&self) -> &BTreeMap<String, u64> {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub samples: usize,
    pub min_micros: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

impl LatencySummary {
    pub fn from_samples(samples: &[u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Some(Self {
            samples: sorted.len(),
            min_micros: sorted[0],
            p50_micros: percentile(&sorted, 50),
            p95_micros: percentile(&sorted, 95),
            p99_micros: percentile(&sorted, 99),
            max_micros: sorted[sorted.len() - 1],
        })
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    debug_assert!(!sorted.is_empty());
    debug_assert!((1..=100).contains(&percentile));
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimedResourceSample {
    pub elapsed_millis: u64,
    pub resources: ResourceSnapshot,
}

impl TimedResourceSample {
    pub fn capture(
        elapsed_millis: u64,
        active_sessions: u64,
        active_cursors: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            elapsed_millis,
            resources: ResourceSnapshot::capture_linux(active_sessions, active_cursors)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSlope {
    pub samples: usize,
    pub rss_bytes_per_second: f64,
    pub file_descriptors_per_second: f64,
    pub workers_per_second: f64,
    pub sessions_per_second: f64,
    pub cursors_per_second: f64,
}

impl ResourceSlope {
    /// Calculate least-squares slopes after the caller's warm-up boundary.
    /// A minimum of three samples avoids treating a single allocator jump as
    /// sustained growth.
    pub fn from_samples(samples: &[TimedResourceSample]) -> Result<Self, String> {
        if samples.len() < 3 {
            return Err("resource slope requires at least three samples".to_string());
        }
        if samples
            .windows(2)
            .any(|pair| pair[0].elapsed_millis >= pair[1].elapsed_millis)
        {
            return Err("resource samples must have strictly increasing timestamps".to_string());
        }
        let x = samples
            .iter()
            .map(|sample| sample.elapsed_millis as f64 / 1_000.0)
            .collect::<Vec<_>>();
        Ok(Self {
            samples: samples.len(),
            rss_bytes_per_second: least_squares(&x, samples.iter().map(|s| s.resources.rss_bytes)),
            file_descriptors_per_second: least_squares(
                &x,
                samples.iter().map(|s| s.resources.open_file_descriptors),
            ),
            workers_per_second: least_squares(
                &x,
                samples.iter().map(|s| s.resources.active_workers),
            ),
            sessions_per_second: least_squares(
                &x,
                samples.iter().map(|s| s.resources.active_sessions),
            ),
            cursors_per_second: least_squares(
                &x,
                samples.iter().map(|s| s.resources.active_cursors),
            ),
        })
    }

    pub fn exceeds(
        &self,
        rss_bytes_per_second: f64,
        file_descriptors_per_second: f64,
        workers_per_second: f64,
    ) -> bool {
        self.rss_bytes_per_second > rss_bytes_per_second
            || self.file_descriptors_per_second > file_descriptors_per_second
            || self.workers_per_second > workers_per_second
            || self.sessions_per_second > 0.0
            || self.cursors_per_second > 0.0
    }
}

fn least_squares(values_x: &[f64], values_y: impl Iterator<Item = u64>) -> f64 {
    let values_y = values_y.map(|value| value as f64).collect::<Vec<_>>();
    let count = values_x.len() as f64;
    let mean_x = values_x.iter().sum::<f64>() / count;
    let mean_y = values_y.iter().sum::<f64>() / count;
    let numerator = values_x
        .iter()
        .zip(&values_y)
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let denominator = values_x.iter().map(|x| (x - mean_x).powi(2)).sum::<f64>();
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}
