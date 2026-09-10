use super::super::FormatResult;
use super::model::{invalid, limit, DataArtifactLayout};

pub const MAX_DATA_OPEN_METADATA_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataOpenLimits {
    max_accounted_bytes: u64,
}

impl DataOpenLimits {
    pub fn new(max_accounted_bytes: u64) -> FormatResult<Self> {
        if max_accounted_bytes == 0 || max_accounted_bytes > MAX_DATA_OPEN_METADATA_BYTES {
            return Err(limit(
                "metadata-open accounted bytes",
                max_accounted_bytes,
                MAX_DATA_OPEN_METADATA_BYTES,
            ));
        }
        Ok(Self {
            max_accounted_bytes,
        })
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }
}

impl Default for DataOpenLimits {
    fn default() -> Self {
        Self {
            max_accounted_bytes: MAX_DATA_OPEN_METADATA_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DataOpenMetrics {
    read_calls: u64,
    read_bytes: u64,
    accounted_allocation_bytes: u64,
}

impl DataOpenMetrics {
    pub const fn read_calls(self) -> u64 {
        self.read_calls
    }

    pub const fn read_bytes(self) -> u64 {
        self.read_bytes
    }

    pub const fn accounted_allocation_bytes(self) -> u64 {
        self.accounted_allocation_bytes
    }

    pub(crate) fn record_read(&mut self, bytes: u64) -> FormatResult<()> {
        self.read_calls = self
            .read_calls
            .checked_add(1)
            .ok_or_else(|| invalid("metadata read-call counter overflows"))?;
        self.read_bytes = self
            .read_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("metadata read-byte counter overflows"))?;
        Ok(())
    }

    pub(crate) fn account_allocation(
        &mut self,
        bytes: u64,
        limits: DataOpenLimits,
    ) -> FormatResult<()> {
        let total = self
            .accounted_allocation_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("metadata allocation accounting overflows"))?;
        if total > limits.max_accounted_bytes() {
            return Err(limit(
                "metadata-open accounted bytes",
                total,
                limits.max_accounted_bytes(),
            ));
        }
        self.accounted_allocation_bytes = total;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenedDataArtifact {
    layout: DataArtifactLayout,
    metrics: DataOpenMetrics,
}

impl OpenedDataArtifact {
    pub(crate) const fn new(layout: DataArtifactLayout, metrics: DataOpenMetrics) -> Self {
        Self { layout, metrics }
    }

    pub const fn layout(&self) -> &DataArtifactLayout {
        &self.layout
    }

    pub const fn metrics(&self) -> DataOpenMetrics {
        self.metrics
    }

    pub fn into_layout(self) -> DataArtifactLayout {
        self.layout
    }
}
