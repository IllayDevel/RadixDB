//! Core-owned index wrapper for composition-layer key encoders.
//!
//! The callback receives and returns ordinary bounded [`Value`] objects. It
//! has no storage, transaction, page, WAL or catalog authority. All physical
//! index operations remain delegated to an existing core access method.

use std::fmt;
use std::sync::Arc;

use crate::expression::{Expression, InListExpr};
use crate::traits::Index;
use radixdb_core::{
    DataType, Error, I64Map, IndexEntry, IndexType, Operator, Result, RowIdVec, Value,
};

type EncodeKey = dyn Fn(&Value) -> Result<Value> + Send + Sync;

#[derive(Clone)]
pub struct PreparedIndexKeyEncoder {
    operator_class_id: [u8; 16],
    semantic_revision: u32,
    key_codec_revision: u32,
    fingerprint: [u8; 32],
    physical_data_type: DataType,
    encode: Arc<EncodeKey>,
}

impl PreparedIndexKeyEncoder {
    pub fn new(
        operator_class_id: [u8; 16],
        semantic_revision: u32,
        key_codec_revision: u32,
        fingerprint: [u8; 32],
        physical_data_type: DataType,
        encode: impl Fn(&Value) -> Result<Value> + Send + Sync + 'static,
    ) -> Result<Self> {
        if operator_class_id == [0; 16]
            || semantic_revision == 0
            || key_codec_revision == 0
            || physical_data_type == DataType::Null
        {
            return Err(Error::invalid_argument(
                "prepared index key encoder has an invalid identity or physical type",
            ));
        }
        Ok(Self {
            operator_class_id,
            semantic_revision,
            key_codec_revision,
            fingerprint,
            physical_data_type,
            encode: Arc::new(encode),
        })
    }

    pub const fn operator_class_id(&self) -> [u8; 16] {
        self.operator_class_id
    }

    pub const fn semantic_revision(&self) -> u32 {
        self.semantic_revision
    }

    pub const fn key_codec_revision(&self) -> u32 {
        self.key_codec_revision
    }

    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }

    pub const fn physical_data_type(&self) -> DataType {
        self.physical_data_type
    }

    pub fn encode(&self, value: &Value) -> Result<Value> {
        let encoded = (self.encode)(value)?;
        if !encoded.is_null() && encoded.data_type() != self.physical_data_type {
            return Err(Error::internal(
                "operator-class key encoder returned a different physical type",
            ));
        }
        Ok(encoded)
    }
}

impl fmt::Debug for PreparedIndexKeyEncoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedIndexKeyEncoder")
            .field("operator_class_id", &self.operator_class_id)
            .field("semantic_revision", &self.semantic_revision)
            .field("key_codec_revision", &self.key_codec_revision)
            .field("fingerprint", &self.fingerprint)
            .field("physical_data_type", &self.physical_data_type)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PreparedIndexKeyEncoder {
    fn eq(&self, other: &Self) -> bool {
        self.operator_class_id == other.operator_class_id
            && self.semantic_revision == other.semantic_revision
            && self.key_codec_revision == other.key_codec_revision
            && self.fingerprint == other.fingerprint
            && self.physical_data_type == other.physical_data_type
    }
}

impl Eq for PreparedIndexKeyEncoder {}

/// Translates logical external values before delegating to a core-owned index.
pub struct EncodedIndex {
    inner: Arc<dyn Index>,
    logical_data_types: Vec<DataType>,
    encoder: PreparedIndexKeyEncoder,
}

impl EncodedIndex {
    pub fn new(
        inner: Arc<dyn Index>,
        logical_data_types: Vec<DataType>,
        encoder: PreparedIndexKeyEncoder,
    ) -> Result<Self> {
        if inner.column_ids().len() != 1
            || logical_data_types.len() != 1
            || inner.data_types() != [encoder.physical_data_type()]
        {
            return Err(Error::invalid_argument(
                "encoded index requires one logical key and one matching physical key",
            ));
        }
        Ok(Self {
            inner,
            logical_data_types,
            encoder,
        })
    }

    fn encode_values(&self, values: &[Value]) -> Result<Vec<Value>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        if values.len() != 1 {
            return Err(Error::invalid_argument(
                "encoded index lookup requires exactly one key",
            ));
        }
        self.encoder.encode(&values[0]).map(|value| vec![value])
    }

    fn encode_entries(&self, entries: &I64Map<Vec<Value>>) -> Result<I64Map<Vec<Value>>> {
        let mut encoded = I64Map::with_capacity(entries.len());
        for (row_id, values) in entries.iter() {
            encoded.insert(row_id, self.encode_values(values)?);
        }
        Ok(encoded)
    }

    fn all_row_ids(&self) -> Result<RowIdVec> {
        let entries = self.inner.all_entries()?;
        let mut rows = RowIdVec::new();
        rows.reserve(entries.len());
        rows.extend(entries.into_iter().map(|entry| entry.row_id));
        Ok(rows)
    }
}

impl fmt::Debug for EncodedIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedIndex")
            .field("name", &self.inner.name())
            .field("encoder", &self.encoder)
            .finish_non_exhaustive()
    }
}

impl Index for EncodedIndex {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn table_name(&self) -> &str {
        self.inner.table_name()
    }

    fn build(&mut self) -> Result<()> {
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| {
                Error::NotSupported("cannot exclusively build a shared encoded index".to_owned())
            })?
            .build()
    }

    fn add(&self, values: &[Value], row_id: i64, ref_id: i64) -> Result<()> {
        self.inner.add(&self.encode_values(values)?, row_id, ref_id)
    }

    fn add_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        self.inner.add_batch(&self.encode_entries(entries)?)
    }

    fn remove(&self, values: &[Value], row_id: i64, ref_id: i64) -> Result<()> {
        self.inner
            .remove(&self.encode_values(values)?, row_id, ref_id)
    }

    fn remove_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        self.inner.remove_batch(&self.encode_entries(entries)?)
    }

    fn add_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        let encoded = entries
            .iter()
            .map(|(row_id, values)| self.encode_values(values).map(|values| (*row_id, values)))
            .collect::<Result<Vec<_>>>()?;
        let borrowed = encoded
            .iter()
            .map(|(row_id, values)| (*row_id, values.as_slice()))
            .collect::<Vec<_>>();
        self.inner.add_batch_slice(&borrowed)
    }

    fn remove_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        let encoded = entries
            .iter()
            .map(|(row_id, values)| self.encode_values(values).map(|values| (*row_id, values)))
            .collect::<Result<Vec<_>>>()?;
        let borrowed = encoded
            .iter()
            .map(|(row_id, values)| (*row_id, values.as_slice()))
            .collect::<Vec<_>>();
        self.inner.remove_batch_slice(&borrowed)
    }

    fn column_ids(&self) -> &[i32] {
        self.inner.column_ids()
    }
    fn column_names(&self) -> &[String] {
        self.inner.column_names()
    }
    fn data_types(&self) -> &[DataType] {
        &self.logical_data_types
    }
    fn index_type(&self) -> IndexType {
        self.inner.index_type()
    }
    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn prepared_key_encoder(&self) -> Option<PreparedIndexKeyEncoder> {
        Some(self.encoder.clone())
    }

    fn find(&self, values: &[Value]) -> Result<Vec<IndexEntry>> {
        self.inner.find(&self.encode_values(values)?)
    }

    fn find_range(
        &self,
        min: &[Value],
        max: &[Value],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        self.inner.find_range(
            &self.encode_values(min)?,
            &self.encode_values(max)?,
            min_inclusive,
            max_inclusive,
        )
    }

    fn find_physical_range(
        &self,
        min: &[Value],
        max: &[Value],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        self.inner
            .find_range(min, max, min_inclusive, max_inclusive)
    }

    fn find_range_ordered_limited(
        &self,
        min: &[Value],
        max: &[Value],
        min_inclusive: bool,
        max_inclusive: bool,
        ascending: bool,
        limit: usize,
    ) -> Result<Vec<IndexEntry>> {
        self.inner.find_range_ordered_limited(
            &self.encode_values(min)?,
            &self.encode_values(max)?,
            min_inclusive,
            max_inclusive,
            ascending,
            limit,
        )
    }

    fn find_with_operator(&self, op: Operator, values: &[Value]) -> Result<Vec<IndexEntry>> {
        self.inner
            .find_with_operator(op, &self.encode_values(values)?)
    }

    fn get_row_ids_equal_into(&self, values: &[Value], buffer: &mut Vec<i64>) -> Result<()> {
        self.inner
            .get_row_ids_equal_into(&self.encode_values(values)?, buffer)
    }

    fn get_row_ids_in_range_into(
        &self,
        min: &[Value],
        max: &[Value],
        include_min: bool,
        include_max: bool,
        buffer: &mut Vec<i64>,
    ) -> Result<()> {
        self.inner.get_row_ids_in_range_into(
            &self.encode_values(min)?,
            &self.encode_values(max)?,
            include_min,
            include_max,
            buffer,
        )
    }

    fn get_row_ids_in_into(&self, values: &[Value], buffer: &mut Vec<i64>) -> Result<()> {
        for value in values {
            self.get_row_ids_equal_into(std::slice::from_ref(value), buffer)?;
        }
        Ok(())
    }

    fn get_filtered_row_ids(&self, expression: &dyn Expression) -> Result<RowIdVec> {
        if let Some((column, operator, value)) = expression.get_comparison_info() {
            if self.column_names()[0].eq_ignore_ascii_case(column) {
                let entries = self.find_with_operator(operator, std::slice::from_ref(value))?;
                return Ok(entries.into_iter().map(|entry| entry.row_id).collect());
            }
        }
        if let Some(list) = expression.as_any().downcast_ref::<InListExpr>() {
            if list
                .get_column_name()
                .is_some_and(|column| self.column_names()[0].eq_ignore_ascii_case(column))
            {
                return self.get_row_ids_in(list.get_values());
            }
        }
        // Unknown predicate shapes are returned as candidates. The executor's
        // residual expression remains the correctness owner.
        self.all_row_ids()
    }

    fn get_distinct_count_excluding_null(&self) -> Option<usize> {
        self.inner.get_distinct_count_excluding_null()
    }

    fn get_row_ids_ordered(
        &self,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<Vec<i64>> {
        self.inner.get_row_ids_ordered(ascending, limit, offset)
    }

    fn clear(&self) {
        self.inner.clear()
    }
    fn cleanup(&self) -> Result<()> {
        self.inner.cleanup()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn close(&mut self) -> Result<()> {
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| {
                Error::NotSupported("cannot exclusively close a shared encoded index".to_owned())
            })?
            .close()
    }
}
