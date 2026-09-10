//! Cross-source scanner composition.

use super::*;

/// Scanner that merges results from multiple sources (hot buffer + volumes).
///
/// This is the key integration point: a query over a table with frozen volumes
/// first scans the volumes (column-major, possibly zone-map-pruned), then
/// scans the hot buffer (current live rows). The executor sees a single
/// unified Scanner.
pub struct MergingScanner {
    /// Scanners to merge (processed in order: volumes first, hot buffer last)
    pub(super) sources: Vec<Box<dyn Scanner>>,
    /// Index of the current active source
    current_source: usize,
    closed: bool,
    terminal_error: Option<Error>,
}

/// Small row-source adapter used for the hot tail of an otherwise typed artifact-backed
/// scan. It preserves the row API, but can batch the already-visible hot rows
/// into the same storage-level column representation as the cold prefix.
pub(crate) struct RowTypedScanner {
    inner: Box<dyn Scanner>,
    schema: Schema,
    row_iteration_started: bool,
    closed: bool,
}

impl RowTypedScanner {
    pub(crate) fn new(inner: Box<dyn Scanner>, schema: Schema) -> Self {
        Self {
            inner,
            schema,
            row_iteration_started: false,
            closed: false,
        }
    }

    fn schema_is_supported(&self) -> bool {
        !self.schema.columns.is_empty()
            && self.schema.columns.iter().all(|column| {
                matches!(
                    column.data_type,
                    DataType::Integer
                        | DataType::Float
                        | DataType::Text
                        | DataType::Boolean
                        | DataType::Timestamp
                        | DataType::Bytes
                        | DataType::Json
                )
            })
    }
}

impl Scanner for RowTypedScanner {
    fn next(&mut self) -> bool {
        self.row_iteration_started = true;
        self.inner.next()
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn err(&self) -> Option<&Error> {
        self.inner.err()
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.inner.close()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        self.inner.take_row_with_id()
    }

    fn current_row_id(&self) -> Result<i64> {
        self.inner.current_row_id()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn supports_typed_batches(&self) -> bool {
        !self.closed && !self.row_iteration_started && self.schema_is_supported()
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        if self.closed {
            Some(TypedBatchFallbackReason::Closed)
        } else if self.row_iteration_started {
            Some(TypedBatchFallbackReason::RowIterationStarted)
        } else if !self.schema_is_supported() {
            Some(TypedBatchFallbackReason::UnsupportedStorageType)
        } else {
            None
        }
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if !self.supports_typed_batches() {
            return Err(Error::internal(
                "typed batch requested from unsupported hot-row adapter",
            ));
        }
        let capacity = crate::volume::column::ROW_GROUP_SIZE;
        let mut rows = Vec::with_capacity(capacity);
        while rows.len() < capacity && self.inner.next() {
            rows.push(self.inner.take_row());
        }
        if let Some(error) = self.inner.err().cloned() {
            return Err(error);
        }
        if rows.is_empty() {
            return Ok(None);
        }

        let mut builder =
            crate::volume::writer::VolumeBuilder::with_capacity(&self.schema, rows.len());
        for (index, row) in rows.iter().enumerate() {
            builder.try_add_row(index as i64 + 1, row)?;
        }
        let columns = builder.finish().columns.take_columns();
        Ok(Some(TypedColumnBatch::new(rows.len(), columns)))
    }
}

impl MergingScanner {
    /// Create a merging scanner from multiple sources.
    ///
    /// Sources are scanned in order. Typically:
    /// `[volume_0_scanner, volume_1_scanner, ..., hot_buffer_scanner]`
    pub fn new(sources: Vec<Box<dyn Scanner>>) -> Self {
        let mut scanner = Self {
            sources,
            current_source: 0,
            closed: false,
            terminal_error: None,
        };
        scanner.warm_active_sources();
        scanner
    }

    fn warm_active_sources(&mut self) {
        if self.closed {
            return;
        }
        let end = (self.current_source + 2).min(self.sources.len());
        for source in &mut self.sources[self.current_source..end] {
            source.warmup();
        }
    }

    fn release_current_source(&mut self) -> Result<()> {
        if self.current_source >= self.sources.len() {
            return Ok(());
        }
        let mut completed = std::mem::replace(
            &mut self.sources[self.current_source],
            Box::new(EmptyScanner::new()),
        );
        completed.close()?;
        drop(completed);
        self.current_source += 1;
        self.warm_active_sources();
        Ok(())
    }
}

impl Scanner for MergingScanner {
    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }
        while self.current_source < self.sources.len() {
            if self.sources[self.current_source].next() {
                return true;
            }
            // Check for errors before moving to next source
            if self.sources[self.current_source].err().is_some() {
                return false;
            }
            if let Err(error) = self.release_current_source() {
                self.terminal_error = Some(error);
                return false;
            }
        }
        false
    }

    fn row(&self) -> &Row {
        debug_assert!(
            self.current_source < self.sources.len(),
            "row() called after iteration completed"
        );
        self.sources[self.current_source].row()
    }

    fn current_row_id(&self) -> Result<i64> {
        if self.current_source < self.sources.len() {
            self.sources[self.current_source].current_row_id()
        } else {
            Err(Error::internal(
                "row identity requested after merged scanner completion",
            ))
        }
    }

    fn err(&self) -> Option<&Error> {
        if self.closed {
            return self.terminal_error.as_ref();
        }
        if self.current_source < self.sources.len() {
            self.sources[self.current_source].err()
        } else {
            None
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return self.terminal_error.clone().map_or(Ok(()), Err);
        }
        let mut first_error = None;
        for source in &mut self.sources {
            if let Err(error) = source.close() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        self.sources.clear();
        self.current_source = 0;
        self.closed = true;
        self.terminal_error = first_error.clone();
        first_error.map_or(Ok(()), Err)
    }

    fn take_row(&mut self) -> Row {
        debug_assert!(
            self.current_source < self.sources.len(),
            "take_row() called after iteration completed"
        );
        self.sources[self.current_source].take_row()
    }

    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        if self.current_source >= self.sources.len() {
            return Err(Error::internal(
                "row identity requested after merged scanner completion",
            ));
        }
        self.sources[self.current_source].take_row_with_id()
    }

    fn estimated_count(&self) -> Option<usize> {
        if self.closed {
            return Some(0);
        }
        let mut total = 0usize;
        for source in &self.sources {
            total += source.estimated_count()?;
        }
        Some(total)
    }

    fn supports_typed_batches(&self) -> bool {
        if self.closed {
            return false;
        }
        self.sources[self.current_source..]
            .iter()
            .all(|source| source.supports_typed_batches())
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        if self.supports_typed_batches() {
            return None;
        }
        let remaining = &self.sources[self.current_source..];
        let has_typed_source = remaining
            .iter()
            .any(|source| source.supports_typed_batches());
        let has_row_source = remaining
            .iter()
            .any(|source| !source.supports_typed_batches());
        if has_typed_source && has_row_source {
            return Some(TypedBatchFallbackReason::MixedTypedAndRowSources);
        }
        remaining
            .iter()
            .find_map(|source| source.typed_batch_fallback_reason())
            .or(Some(TypedBatchFallbackReason::MergedSource))
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.closed {
            return Ok(None);
        }
        if !self.supports_typed_batches() {
            return Err(Error::internal(
                "typed batch requested for a merged scanner with row-level sources",
            ));
        }
        while self.current_source < self.sources.len() {
            match self.sources[self.current_source].next_typed_batch()? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    if let Some(error) = self.sources[self.current_source].err().cloned() {
                        return Err(error);
                    }
                    self.release_current_source()?;
                }
            }
        }
        Ok(None)
    }
}
