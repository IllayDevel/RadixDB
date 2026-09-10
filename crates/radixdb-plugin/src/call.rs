use std::{marker::PhantomData, panic::AssertUnwindSafe};

use radixdb_plugin_abi::{
    RadixAbiBatchViewV1, RadixAbiCallContextV1, RadixAbiColumnViewV1, RadixAbiDiagnosticV1,
    RadixAbiHashSinkV1, RadixAbiHeaderV1, RadixAbiResultBuilderV1, RadixAbiSliceV1,
    RadixAbiStatusV1, RadixAbiValueV1, RADIX_COLUMN_LAYOUT_FIXED, RADIX_DIAGNOSTIC_CANCELLED,
    RADIX_DIAGNOSTIC_DOMAIN, RADIX_DIAGNOSTIC_INTERNAL, RADIX_DIAGNOSTIC_INVALID_INPUT,
    RADIX_DIAGNOSTIC_LIMIT, RADIX_DIAGNOSTIC_PLUGIN_PANIC, RADIX_HASH_COMPONENT_BYTES,
    RADIX_HASH_COMPONENT_F64_BITS, RADIX_HASH_COMPONENT_I64, RADIX_HASH_COMPONENT_U64,
    RADIX_RESULT_ITEM_FLAG_NULL, RADIX_STATUS_CANCELLED, RADIX_STATUS_CONTRACT_VIOLATION,
    RADIX_STATUS_DOMAIN_ERROR, RADIX_STATUS_INTERNAL_ERROR, RADIX_STATUS_INVALID_ARGUMENT,
    RADIX_STATUS_LIMIT_EXCEEDED, RADIX_STATUS_OK, RADIX_STATUS_PANIC, RADIX_TYPE_REF_EXTERNAL,
};

use crate::{value::AbiValue, PluginError, PluginErrorKind, PluginResult, RadixType, ValueType};

pub struct CallContext<'a> {
    raw: &'a RadixAbiCallContextV1,
}

impl CallContext<'_> {
    pub fn check_cancelled(&self) -> PluginResult<()> {
        let callback = self
            .raw
            .check_cancelled
            .ok_or_else(|| PluginError::internal("missing cancellation callback"))?;
        // SAFETY: callback and handle are supplied by the host for this call.
        match unsafe { callback(self.raw.handle) } {
            RADIX_STATUS_OK => Ok(()),
            RADIX_STATUS_CANCELLED => Err(PluginError::cancelled()),
            _ => Err(PluginError::internal(
                "host cancellation callback violated its contract",
            )),
        }
    }

    pub fn charge_work(&self, units: u32) -> PluginResult<()> {
        let callback = self
            .raw
            .charge_work
            .ok_or_else(|| PluginError::internal("missing work-accounting callback"))?;
        // SAFETY: callback and handle are supplied by the host for this call.
        match unsafe { callback(self.raw.handle, units) } {
            RADIX_STATUS_OK => Ok(()),
            RADIX_STATUS_LIMIT_EXCEEDED => {
                Err(PluginError::limit_exceeded("plugin work budget exhausted"))
            }
            RADIX_STATUS_CANCELLED => Err(PluginError::cancelled()),
            _ => Err(PluginError::internal(
                "host work callback violated its contract",
            )),
        }
    }

    pub fn deadline_unix_ns(&self) -> u64 {
        self.raw.deadline_unix_ns
    }

    pub fn max_output_bytes(&self) -> u32 {
        self.raw.max_output_bytes
    }
}

pub struct HashSink<'a> {
    backend: HashSinkBackend<'a>,
}

enum HashSinkBackend<'a> {
    Abi(&'a RadixAbiHashSinkV1),
    Test(&'a mut Vec<(u16, Vec<u8>)>),
}

impl HashSink<'_> {
    pub(crate) fn for_testing(components: &mut Vec<(u16, Vec<u8>)>) -> HashSink<'_> {
        HashSink {
            backend: HashSinkBackend::Test(components),
        }
    }

    pub fn bytes(&mut self, value: &[u8]) -> PluginResult<()> {
        self.append(RADIX_HASH_COMPONENT_BYTES, value)
    }

    pub fn i64(&mut self, value: i64) -> PluginResult<()> {
        self.append(RADIX_HASH_COMPONENT_I64, &value.to_le_bytes())
    }

    pub fn u64(&mut self, value: u64) -> PluginResult<()> {
        self.append(RADIX_HASH_COMPONENT_U64, &value.to_le_bytes())
    }

    pub fn f64_bits(&mut self, value: f64) -> PluginResult<()> {
        self.append(
            RADIX_HASH_COMPONENT_F64_BITS,
            &value.to_bits().to_le_bytes(),
        )
    }

    fn append(&mut self, kind: u16, bytes: &[u8]) -> PluginResult<()> {
        if bytes.len() > u32::MAX as usize {
            return Err(PluginError::limit_exceeded("hash component is too large"));
        }
        match &mut self.backend {
            HashSinkBackend::Abi(raw) => {
                let callback = raw
                    .append
                    .ok_or_else(|| PluginError::internal("missing hash sink callback"))?;
                // SAFETY: the slice remains live for the synchronous host callback.
                let status = unsafe {
                    callback(
                        raw.handle,
                        kind,
                        0,
                        RadixAbiSliceV1 {
                            ptr: bytes.as_ptr(),
                            len: bytes.len() as u32,
                            reserved: 0,
                        },
                    )
                };
                status_result(status, "hash sink rejected component")
            }
            HashSinkBackend::Test(components) => {
                if components.len() >= radixdb_plugin_abi::RADIX_MAX_HASH_COMPONENTS as usize
                    || components
                        .iter()
                        .map(|(_, value)| value.len())
                        .sum::<usize>()
                        + bytes.len()
                        > radixdb_plugin_abi::RADIX_MAX_HASH_BYTES as usize
                {
                    return Err(PluginError::limit_exceeded(
                        "semantic hash components exceed SDK test bounds",
                    ));
                }
                components.push((kind, bytes.to_vec()));
                Ok(())
            }
        }
    }
}

pub struct ResultBuilder<'a> {
    raw: &'a RadixAbiResultBuilderV1,
    items: u32,
    bytes: u32,
    finished: bool,
}

impl ResultBuilder<'_> {
    pub fn push<T: ValueType>(&mut self, value: T) -> PluginResult<()> {
        if value.is_null() {
            return self.push_null();
        }
        let bytes = value.encode_abi()?;
        self.write(0, &bytes)
    }

    pub fn push_null(&mut self) -> PluginResult<()> {
        self.write(RADIX_RESULT_ITEM_FLAG_NULL, &[])
    }

    fn write(&mut self, flags: u32, bytes: &[u8]) -> PluginResult<()> {
        if self.finished {
            return Err(PluginError::internal("write after result finish"));
        }
        let next_items = self
            .items
            .checked_add(1)
            .ok_or_else(|| PluginError::limit_exceeded("result item count overflow"))?;
        let next_bytes = self
            .bytes
            .checked_add(bytes.len() as u32)
            .ok_or_else(|| PluginError::limit_exceeded("result byte count overflow"))?;
        if next_items > self.raw.max_items || next_bytes > self.raw.max_bytes {
            return Err(PluginError::limit_exceeded(
                "result exceeds host-owned builder bounds",
            ));
        }
        let callback = self
            .raw
            .write
            .ok_or_else(|| PluginError::internal("missing result writer"))?;
        // SAFETY: bytes live through the synchronous host-owned copy.
        let status = unsafe {
            callback(
                self.raw.handle,
                flags,
                0,
                RadixAbiSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len() as u32,
                    reserved: 0,
                },
            )
        };
        status_result(status, "result builder rejected output")?;
        self.items = next_items;
        self.bytes = next_bytes;
        Ok(())
    }

    fn finish(&mut self) -> PluginResult<()> {
        if self.finished {
            return Err(PluginError::internal("result builder finished twice"));
        }
        let callback = self
            .raw
            .finish
            .ok_or_else(|| PluginError::internal("missing result finisher"))?;
        // SAFETY: callback and handle are host-owned for this call.
        let status = unsafe { callback(self.raw.handle) };
        status_result(status, "result builder finish failed")?;
        self.finished = true;
        Ok(())
    }
}

pub struct ColumnBuilder<'borrow, 'host, T> {
    output: &'borrow mut ResultBuilder<'host>,
    marker: PhantomData<T>,
}

impl<T: ValueType> ColumnBuilder<'_, '_, T> {
    pub fn push(&mut self, value: T) -> PluginResult<()> {
        self.output.push(value)
    }

    pub fn push_null(&mut self) -> PluginResult<()> {
        self.output.push_null()
    }
}

#[derive(Clone, Copy)]
pub struct ColumnView<'a, T> {
    raw: &'a RadixAbiColumnViewV1,
    row: u32,
    marker: PhantomData<T>,
}

impl<'a, T: ValueType> Iterator for ColumnView<'a, T> {
    type Item = PluginResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.raw.row_count {
            return None;
        }
        let row = self.row;
        self.row += 1;
        Some(decode_column_value::<T>(self.raw, row))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PredicateView<'a> {
    bytes: &'a [u8],
}

impl<'a> PredicateView<'a> {
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn target_function_id(&self) -> PluginResult<[u8; 16]> {
        self.validate_header()?;
        Ok(self.bytes[4..20].try_into().expect("validated fixed width"))
    }

    pub fn operator_class_id(&self) -> PluginResult<[u8; 16]> {
        self.validate_header()?;
        Ok(self.bytes[20..36]
            .try_into()
            .expect("validated fixed width"))
    }

    pub fn indexed_argument(&self) -> PluginResult<u16> {
        self.validate_header()?;
        Ok(u16::from_le_bytes(
            self.bytes[36..38]
                .try_into()
                .expect("validated fixed width"),
        ))
    }

    pub fn argument_count(&self) -> PluginResult<u16> {
        self.validate_header()?;
        Ok(u16::from_le_bytes(
            self.bytes[38..40]
                .try_into()
                .expect("validated fixed width"),
        ))
    }

    /// Decode one normalized constant. `None` is SQL NULL; the indexed-column
    /// marker is not a constant and is rejected.
    pub fn constant<T: ValueType>(&self, index: u16) -> PluginResult<Option<T>> {
        let argument = self.argument(index)?;
        if argument.kind != 2 {
            return Err(PluginError::invalid_input(
                "requested normalized argument is the indexed column",
            ));
        }
        if argument.is_null {
            return Ok(None);
        }
        let mut inline_bytes = [0; 16];
        let variable = argument.type_ref.kind == RADIX_TYPE_REF_EXTERNAL
            || matches!(
                argument.type_ref.builtin_tag,
                radixdb_plugin_abi::RADIX_BUILTIN_TEXT
                    | radixdb_plugin_abi::RADIX_BUILTIN_JSON
                    | radixdb_plugin_abi::RADIX_BUILTIN_VECTOR
                    | radixdb_plugin_abi::RADIX_BUILTIN_DECIMAL
                    | radixdb_plugin_abi::RADIX_BUILTIN_BYTES
            );
        let borrowed_bytes = if variable {
            RadixAbiSliceV1 {
                ptr: argument.bytes.as_ptr(),
                len: argument.bytes.len() as u32,
                reserved: 0,
            }
        } else {
            if argument.bytes.len() > inline_bytes.len() {
                return Err(PluginError::invalid_input(
                    "normalized fixed value exceeds ABI width",
                ));
            }
            inline_bytes[..argument.bytes.len()].copy_from_slice(argument.bytes);
            RadixAbiSliceV1::EMPTY
        };
        let raw = RadixAbiValueV1 {
            type_ref: argument.type_ref,
            flags: 0,
            reserved: 0,
            inline_bytes,
            borrowed_bytes,
        };
        // SAFETY: raw only borrows this immutable predicate for the decode.
        let value = unsafe { AbiValue::new(&raw)? };
        T::decode_abi(&value).map(Some)
    }

    fn validate_header(&self) -> PluginResult<()> {
        if self.bytes.len() < 40 || self.bytes[..4] != *b"RPN1" {
            return Err(PluginError::invalid_input(
                "invalid normalized predicate header",
            ));
        }
        Ok(())
    }

    fn argument(&self, requested: u16) -> PluginResult<NormalizedArgument<'a>> {
        self.validate_header()?;
        let count = self.argument_count()?;
        if requested >= count {
            return Err(PluginError::invalid_input(
                "normalized predicate argument is out of bounds",
            ));
        }
        let mut offset = 40usize;
        for index in 0..count {
            let header_end = offset
                .checked_add(32)
                .ok_or_else(|| PluginError::invalid_input("predicate length overflow"))?;
            if header_end > self.bytes.len() {
                return Err(PluginError::invalid_input(
                    "truncated normalized predicate argument",
                ));
            }
            let kind = self.bytes[offset];
            let flags = self.bytes[offset + 1];
            if !matches!(kind, 1 | 2)
                || flags & !1 != 0
                || self.bytes[offset + 2..offset + 4] != [0, 0]
            {
                return Err(PluginError::invalid_input(
                    "invalid normalized predicate argument header",
                ));
            }
            let type_ref = radixdb_plugin_abi::RadixAbiTypeRefV1 {
                kind: u16::from_le_bytes(
                    self.bytes[offset + 4..offset + 6]
                        .try_into()
                        .expect("fixed width"),
                ),
                builtin_tag: u16::from_le_bytes(
                    self.bytes[offset + 6..offset + 8]
                        .try_into()
                        .expect("fixed width"),
                ),
                object_id: self.bytes[offset + 8..offset + 24]
                    .try_into()
                    .expect("fixed width"),
                codec_version: u32::from_le_bytes(
                    self.bytes[offset + 24..offset + 28]
                        .try_into()
                        .expect("fixed width"),
                ),
            };
            abi_contract(radixdb_plugin_abi::validate_type_ref(&type_ref))?;
            let len = u32::from_le_bytes(
                self.bytes[offset + 28..header_end]
                    .try_into()
                    .expect("fixed width"),
            ) as usize;
            let end = header_end
                .checked_add(len)
                .ok_or_else(|| PluginError::invalid_input("predicate length overflow"))?;
            if end > self.bytes.len()
                || (kind == 1 && (len != 0 || flags != 0))
                || (flags & 1 != 0 && len != 0)
            {
                return Err(PluginError::invalid_input(
                    "invalid normalized predicate argument payload",
                ));
            }
            if index == requested {
                return Ok(NormalizedArgument {
                    kind,
                    is_null: flags & 1 != 0,
                    type_ref,
                    bytes: &self.bytes[header_end..end],
                });
            }
            offset = end;
        }
        Err(PluginError::invalid_input(
            "normalized predicate argument is missing",
        ))
    }
}

struct NormalizedArgument<'a> {
    kind: u8,
    is_null: bool,
    type_ref: radixdb_plugin_abi::RadixAbiTypeRefV1,
    bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateSpan {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

pub struct CandidatePlanBuilder<'borrow, 'host> {
    output: &'borrow mut ResultBuilder<'host>,
    context: &'borrow CallContext<'host>,
}

impl CandidatePlanBuilder<'_, '_> {
    pub fn set_estimate(&mut self, estimated_rows: u64, cost_hint: u32) -> PluginResult<()> {
        self.context.check_cancelled()?;
        self.context.charge_work(1)?;
        let mut bytes = Vec::with_capacity(20);
        bytes.extend_from_slice(&[2, 0, 0, 0]);
        bytes.extend_from_slice(&estimated_rows.to_le_bytes());
        bytes.extend_from_slice(&cost_hint.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        self.output.write(0, &bytes)
    }

    pub fn push_span(&mut self, span: CandidateSpan) -> PluginResult<()> {
        self.context.check_cancelled()?;
        self.context.charge_work(1)?;
        let start_len = u32::try_from(span.start.len())
            .map_err(|_| PluginError::limit_exceeded("candidate start key is too large"))?;
        let end_len = u32::try_from(span.end.len())
            .map_err(|_| PluginError::limit_exceeded("candidate end key is too large"))?;
        let mut bytes = Vec::with_capacity(12 + span.start.len() + span.end.len());
        bytes.extend_from_slice(&[1, 0, 0, 0]);
        bytes.extend_from_slice(&start_len.to_le_bytes());
        bytes.extend_from_slice(&end_len.to_le_bytes());
        bytes.extend_from_slice(&span.start);
        bytes.extend_from_slice(&span.end);
        self.output.write(0, &bytes)
    }
}

fn decode_column_value<T: ValueType>(column: &RadixAbiColumnViewV1, row: u32) -> PluginResult<T> {
    let is_null = if column.null_bitmap.len == 0 {
        false
    } else {
        // SAFETY: validated host batch memory remains borrowed for this callback.
        let bitmap = unsafe {
            std::slice::from_raw_parts(column.null_bitmap.ptr, column.null_bitmap.len as usize)
        };
        bitmap[row as usize / 8] & (1 << (row % 8)) != 0
    };
    let (inline_bytes, borrowed_bytes) = if column.layout == RADIX_COLUMN_LAYOUT_FIXED {
        let start = row as usize * column.stride as usize;
        let width = column.element_width as usize;
        // SAFETY: batch validation proves row_count * stride and readable data.
        let data = unsafe { std::slice::from_raw_parts(column.data.ptr, column.data.len as usize) };
        if column.type_ref.kind == RADIX_TYPE_REF_EXTERNAL {
            ([0; 16], &data[start..start + width])
        } else {
            let mut inline = [0; 16];
            if width > inline.len() {
                return Err(PluginError::invalid_input(
                    "fixed built-in column element exceeds inline ABI width",
                ));
            }
            inline[..width].copy_from_slice(&data[start..start + width]);
            (inline, &[][..])
        }
    } else {
        // SAFETY: batch validation proves row_count + 1 aligned offsets.
        let offsets =
            unsafe { std::slice::from_raw_parts(column.offsets.ptr, column.offsets.len as usize) };
        // SAFETY: batch validation proves terminal offset is within data.
        let data = unsafe { std::slice::from_raw_parts(column.data.ptr, column.data.len as usize) };
        let start = offsets[row as usize] as usize;
        let end = offsets[row as usize + 1] as usize;
        ([0; 16], &data[start..end])
    };
    let raw = RadixAbiValueV1 {
        type_ref: column.type_ref,
        flags: if is_null { 1 } else { 0 },
        reserved: 0,
        inline_bytes,
        borrowed_bytes: RadixAbiSliceV1 {
            ptr: borrowed_bytes.as_ptr(),
            len: borrowed_bytes.len() as u32,
            reserved: 0,
        },
    };
    // SAFETY: raw references local inline data or the validated column range.
    let value = unsafe { AbiValue::new(&raw)? };
    T::decode_abi(&value)
}

pub(crate) fn status_result(status: u32, detail: &'static str) -> PluginResult<()> {
    match status {
        RADIX_STATUS_OK => Ok(()),
        RADIX_STATUS_INVALID_ARGUMENT | RADIX_STATUS_CONTRACT_VIOLATION => {
            Err(PluginError::invalid_input(detail))
        }
        RADIX_STATUS_DOMAIN_ERROR => Err(PluginError::domain(detail)),
        RADIX_STATUS_LIMIT_EXCEEDED => Err(PluginError::limit_exceeded(detail)),
        RADIX_STATUS_CANCELLED => Err(PluginError::cancelled()),
        _ => Err(PluginError::internal(detail)),
    }
}

fn abi_contract<T>(
    result: Result<T, radixdb_plugin_abi::RadixAbiValidationError>,
) -> PluginResult<T> {
    result.map_err(|error| PluginError::invalid_input(format!("ABI contract violation: {error:?}")))
}

fn error_status(error: &PluginError) -> RadixAbiStatusV1 {
    match error.kind() {
        PluginErrorKind::InvalidInput => RADIX_STATUS_INVALID_ARGUMENT,
        PluginErrorKind::Domain => RADIX_STATUS_DOMAIN_ERROR,
        PluginErrorKind::LimitExceeded => RADIX_STATUS_LIMIT_EXCEEDED,
        PluginErrorKind::Cancelled => RADIX_STATUS_CANCELLED,
        PluginErrorKind::Internal => RADIX_STATUS_INTERNAL_ERROR,
    }
}

fn report_error(context: Option<&RadixAbiCallContextV1>, error: &PluginError, panic: bool) {
    let Some(context) = context else {
        return;
    };
    // SAFETY: a non-null diagnostic sink is part of the validated call context.
    let Some(sink) = (unsafe { context.diagnostics.as_ref() }) else {
        return;
    };
    let Some(write) = sink.write else {
        return;
    };
    let detail = error.detail().as_bytes();
    let field = error.field().unwrap_or_default().as_bytes();
    let category = if panic {
        RADIX_DIAGNOSTIC_PLUGIN_PANIC
    } else {
        match error.kind() {
            PluginErrorKind::InvalidInput => RADIX_DIAGNOSTIC_INVALID_INPUT,
            PluginErrorKind::Domain => RADIX_DIAGNOSTIC_DOMAIN,
            PluginErrorKind::LimitExceeded => RADIX_DIAGNOSTIC_LIMIT,
            PluginErrorKind::Cancelled => RADIX_DIAGNOSTIC_CANCELLED,
            PluginErrorKind::Internal => RADIX_DIAGNOSTIC_INTERNAL,
        }
    };
    let diagnostic = RadixAbiDiagnosticV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiDiagnosticV1>(0),
        category,
        status: if panic {
            RADIX_STATUS_PANIC
        } else {
            error_status(error)
        },
        detail: RadixAbiSliceV1 {
            ptr: detail.as_ptr(),
            len: detail.len() as u32,
            reserved: 0,
        },
        field: RadixAbiSliceV1 {
            ptr: field.as_ptr(),
            len: field.len() as u32,
            reserved: 0,
        },
    };
    // SAFETY: diagnostic and its slices remain live for the synchronous write.
    let _ = unsafe { write(sink.handle, &diagnostic) };
}

#[doc(hidden)]
pub unsafe fn run_scalar<F>(
    context: *const RadixAbiCallContextV1,
    arguments: *const RadixAbiValueV1,
    argument_count: u32,
    output: *const RadixAbiResultBuilderV1,
    expected_arguments: u32,
    strict: bool,
    callback: F,
) -> RadixAbiStatusV1
where
    F: FnOnce(&CallContext<'_>, &[AbiValue<'_>], &mut ResultBuilder<'_>) -> PluginResult<()>,
{
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = CallContext {
            raw: context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?,
        };
        abi_contract(radixdb_plugin_abi::validate_call_context(context.raw))?;
        let raw_output = unsafe { output.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null result builder"))?;
        abi_contract(radixdb_plugin_abi::validate_result_builder(raw_output))?;
        if raw_output.max_bytes > context.max_output_bytes() {
            return Err(PluginError::invalid_input(
                "result builder exceeds call output budget",
            ));
        }
        if argument_count != expected_arguments || (argument_count != 0 && arguments.is_null()) {
            return Err(PluginError::invalid_input("scalar argument count mismatch"));
        }
        let raw_arguments = if argument_count == 0 {
            &[]
        } else {
            // SAFETY: host validates the call-scoped argument table.
            unsafe { std::slice::from_raw_parts(arguments, argument_count as usize) }
        };
        let values = raw_arguments
            .iter()
            .map(|raw| {
                abi_contract(unsafe { radixdb_plugin_abi::validate_value_contents(raw) })?;
                unsafe { AbiValue::new(raw) }
            })
            .collect::<PluginResult<Vec<_>>>()?;
        let mut builder = ResultBuilder {
            raw: raw_output,
            items: 0,
            bytes: 0,
            finished: false,
        };
        if strict && values.iter().any(AbiValue::is_null) {
            builder.push_null()?;
        } else {
            callback(&context, &values, &mut builder)?;
        }
        if builder.items != 1 {
            return Err(PluginError::internal(
                "scalar callback must emit exactly one item",
            ));
        }
        builder.finish()
    }));
    finish_outcome(context_ref, outcome)
}

#[doc(hidden)]
pub unsafe fn run_batch<F>(
    context: *const RadixAbiCallContextV1,
    input: *const RadixAbiBatchViewV1,
    output: *const RadixAbiResultBuilderV1,
    expected_columns: u32,
    callback: F,
) -> RadixAbiStatusV1
where
    F: FnOnce(&CallContext<'_>, &BatchInput<'_>, &mut ResultBuilder<'_>) -> PluginResult<()>,
{
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = CallContext {
            raw: context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?,
        };
        abi_contract(radixdb_plugin_abi::validate_call_context(context.raw))?;
        let batch = unsafe { input.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null batch input"))?;
        abi_contract(unsafe { radixdb_plugin_abi::validate_batch_columns(batch) })?;
        if batch.column_count != expected_columns
            || (batch.column_count != 0 && batch.columns.is_null())
        {
            return Err(PluginError::invalid_input("batch column count mismatch"));
        }
        let columns = if batch.column_count == 0 {
            &[]
        } else {
            // SAFETY: the host validates and owns the call-scoped table.
            unsafe { std::slice::from_raw_parts(batch.columns, batch.column_count as usize) }
        };
        if columns
            .iter()
            .any(|column| column.row_count != batch.row_count)
        {
            return Err(PluginError::invalid_input("batch row count mismatch"));
        }
        let raw_output = unsafe { output.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null result builder"))?;
        abi_contract(radixdb_plugin_abi::validate_result_builder(raw_output))?;
        if raw_output.max_bytes > context.max_output_bytes() {
            return Err(PluginError::invalid_input(
                "result builder exceeds call output budget",
            ));
        }
        let input = BatchInput {
            rows: batch.row_count,
            columns,
        };
        let mut builder = ResultBuilder {
            raw: raw_output,
            items: 0,
            bytes: 0,
            finished: false,
        };
        callback(&context, &input, &mut builder)?;
        if builder.items != batch.row_count {
            return Err(PluginError::internal(
                "batch callback output count differs from input rows",
            ));
        }
        builder.finish()
    }));
    finish_outcome(context_ref, outcome)
}

pub struct BatchInput<'a> {
    rows: u32,
    columns: &'a [RadixAbiColumnViewV1],
}

impl<'a> BatchInput<'a> {
    pub fn row_count(&self) -> u32 {
        self.rows
    }

    pub fn column<T: ValueType>(&self, index: usize) -> PluginResult<ColumnView<'a, T>> {
        let raw = self
            .columns
            .get(index)
            .ok_or_else(|| PluginError::invalid_input("batch column is missing"))?;
        Ok(ColumnView {
            raw,
            row: 0,
            marker: PhantomData,
        })
    }
}

#[doc(hidden)]
pub unsafe fn run_codec_encode<T: RadixType>(
    context: *const RadixAbiCallContextV1,
    input: *const RadixAbiValueV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1 {
    unsafe {
        run_scalar(
            context,
            input,
            1,
            output,
            1,
            true,
            |_context, arguments, output| {
                let value = T::decode_abi(&arguments[0])?;
                output.push(value)
            },
        )
    }
}

#[doc(hidden)]
pub unsafe fn run_codec_decode<T: RadixType>(
    context: *const RadixAbiCallContextV1,
    input: RadixAbiSliceV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1 {
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = CallContext {
            raw: context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?,
        };
        abi_contract(radixdb_plugin_abi::validate_call_context(context.raw))?;
        if input.reserved != 0 || (input.len != 0 && input.ptr.is_null()) {
            return Err(PluginError::invalid_input("invalid codec input slice"));
        }
        let bytes = if input.len == 0 {
            &[]
        } else {
            // SAFETY: the host owns this call-scoped input range.
            unsafe { std::slice::from_raw_parts(input.ptr, input.len as usize) }
        };
        let mut reader = crate::CodecReader::new(bytes);
        let value = T::decode(&mut reader)?;
        reader.finish()?;
        let raw_output = unsafe { output.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null result builder"))?;
        abi_contract(radixdb_plugin_abi::validate_result_builder(raw_output))?;
        if raw_output.max_bytes > context.max_output_bytes() {
            return Err(PluginError::invalid_input(
                "result builder exceeds call output budget",
            ));
        }
        let mut builder = ResultBuilder {
            raw: raw_output,
            items: 0,
            bytes: 0,
            finished: false,
        };
        builder.push(value)?;
        builder.finish()
    }));
    finish_outcome(context_ref, outcome)
}

#[doc(hidden)]
pub unsafe fn run_equal<T: RadixType>(
    context: *const RadixAbiCallContextV1,
    left: *const RadixAbiValueV1,
    right: *const RadixAbiValueV1,
    output: *mut u8,
) -> RadixAbiStatusV1 {
    let callback = |left: T, right: T| {
        let result = T::semantic_equal(&left, &right)
            .ok_or_else(|| PluginError::internal("equality capability is not implemented"))?;
        if output.is_null() {
            return Err(PluginError::invalid_input("null equality output"));
        }
        // SAFETY: host provides a writable one-byte output for this callback.
        unsafe { output.write(u8::from(result)) };
        Ok(())
    };
    unsafe { run_binary_value(context, left, right, callback) }
}

#[doc(hidden)]
pub unsafe fn run_compare<T: RadixType>(
    context: *const RadixAbiCallContextV1,
    left: *const RadixAbiValueV1,
    right: *const RadixAbiValueV1,
    output: *mut i8,
) -> RadixAbiStatusV1 {
    let callback = |left: T, right: T| {
        let result = T::semantic_compare(&left, &right)
            .ok_or_else(|| PluginError::internal("ordering capability is not implemented"))?;
        if output.is_null() {
            return Err(PluginError::invalid_input("null ordering output"));
        }
        let value = match result {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        };
        // SAFETY: host provides a writable one-byte output for this callback.
        unsafe { output.write(value) };
        Ok(())
    };
    unsafe { run_binary_value(context, left, right, callback) }
}

unsafe fn run_binary_value<T: RadixType, F>(
    context: *const RadixAbiCallContextV1,
    left: *const RadixAbiValueV1,
    right: *const RadixAbiValueV1,
    callback: F,
) -> RadixAbiStatusV1
where
    F: FnOnce(T, T) -> PluginResult<()>,
{
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = CallContext {
            raw: context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?,
        };
        abi_contract(radixdb_plugin_abi::validate_call_context(context.raw))?;
        let left = unsafe { left.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null left value"))?;
        let right = unsafe { right.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null right value"))?;
        abi_contract(unsafe { radixdb_plugin_abi::validate_value_contents(left) })?;
        abi_contract(unsafe { radixdb_plugin_abi::validate_value_contents(right) })?;
        let left = T::decode_abi(&unsafe { AbiValue::new(left)? })?;
        let right = T::decode_abi(&unsafe { AbiValue::new(right)? })?;
        callback(left, right)
    }));
    finish_outcome(context_ref, outcome)
}

#[doc(hidden)]
pub unsafe fn run_hash<T: RadixType>(
    context: *const RadixAbiCallContextV1,
    value: *const RadixAbiValueV1,
    sink: *const RadixAbiHashSinkV1,
) -> RadixAbiStatusV1 {
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?;
        abi_contract(radixdb_plugin_abi::validate_call_context(context))?;
        let value = unsafe { value.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null hash value"))?;
        abi_contract(unsafe { radixdb_plugin_abi::validate_value_contents(value) })?;
        let value = T::decode_abi(&unsafe { AbiValue::new(value)? })?;
        let raw_sink =
            unsafe { sink.as_ref() }.ok_or_else(|| PluginError::invalid_input("null hash sink"))?;
        abi_contract(radixdb_plugin_abi::validate_hash_sink(raw_sink))?;
        let mut sink = HashSink {
            backend: HashSinkBackend::Abi(raw_sink),
        };
        T::semantic_hash(&value, &mut sink)
            .ok_or_else(|| PluginError::internal("hash capability is not implemented"))?
    }));
    finish_outcome(context_ref, outcome)
}

#[doc(hidden)]
pub unsafe fn run_key_encoder<T: ValueType, K: ValueType>(
    context: *const RadixAbiCallContextV1,
    value: *const RadixAbiValueV1,
    output: *const RadixAbiResultBuilderV1,
    callback: fn(T) -> PluginResult<K>,
) -> RadixAbiStatusV1 {
    unsafe {
        run_scalar(
            context,
            value,
            1,
            output,
            1,
            true,
            |_context, arguments, output| output.push(callback(T::decode_abi(&arguments[0])?)?),
        )
    }
}

#[doc(hidden)]
pub unsafe fn run_planner<F>(
    context: *const RadixAbiCallContextV1,
    predicate: RadixAbiSliceV1,
    output: *const RadixAbiResultBuilderV1,
    callback: F,
) -> RadixAbiStatusV1
where
    F: FnOnce(PredicateView<'_>, &mut CandidatePlanBuilder<'_, '_>) -> PluginResult<()>,
{
    let context_ref = unsafe { context.as_ref() };
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| -> PluginResult<()> {
        let context = CallContext {
            raw: context_ref.ok_or_else(|| PluginError::invalid_input("null call context"))?,
        };
        abi_contract(radixdb_plugin_abi::validate_call_context(context.raw))?;
        if predicate.reserved != 0 || (predicate.len != 0 && predicate.ptr.is_null()) {
            return Err(PluginError::invalid_input("invalid predicate input"));
        }
        let bytes = if predicate.len == 0 {
            &[]
        } else {
            // SAFETY: the host owns this call-scoped predicate range.
            unsafe { std::slice::from_raw_parts(predicate.ptr, predicate.len as usize) }
        };
        let raw_output = unsafe { output.as_ref() }
            .ok_or_else(|| PluginError::invalid_input("null planner output"))?;
        abi_contract(radixdb_plugin_abi::validate_result_builder(raw_output))?;
        if raw_output.max_bytes > context.max_output_bytes() {
            return Err(PluginError::invalid_input(
                "result builder exceeds call output budget",
            ));
        }
        let mut result = ResultBuilder {
            raw: raw_output,
            items: 0,
            bytes: 0,
            finished: false,
        };
        {
            let mut builder = CandidatePlanBuilder {
                output: &mut result,
                context: &context,
            };
            callback(PredicateView { bytes }, &mut builder)?;
        }
        result.finish()
    }));
    finish_outcome(context_ref, outcome)
}

fn finish_outcome(
    context: Option<&RadixAbiCallContextV1>,
    outcome: Result<PluginResult<()>, Box<dyn std::any::Any + Send>>,
) -> RadixAbiStatusV1 {
    match outcome {
        Ok(Ok(())) => RADIX_STATUS_OK,
        Ok(Err(error)) => {
            let status = error_status(&error);
            report_error(context, &error, false);
            status
        }
        Err(_) => {
            let error = PluginError::internal("plugin callback panicked");
            report_error(context, &error, true);
            RADIX_STATUS_PANIC
        }
    }
}

#[doc(hidden)]
pub fn column_builder<'borrow, 'host, T>(
    output: &'borrow mut ResultBuilder<'host>,
) -> ColumnBuilder<'borrow, 'host, T> {
    ColumnBuilder {
        output,
        marker: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_argument(
        frame: &mut Vec<u8>,
        kind: u8,
        flags: u8,
        type_ref: radixdb_plugin_abi::RadixAbiTypeRefV1,
        value: &[u8],
    ) {
        frame.extend_from_slice(&[kind, flags, 0, 0]);
        frame.extend_from_slice(&type_ref.kind.to_le_bytes());
        frame.extend_from_slice(&type_ref.builtin_tag.to_le_bytes());
        frame.extend_from_slice(&type_ref.object_id);
        frame.extend_from_slice(&type_ref.codec_version.to_le_bytes());
        frame.extend_from_slice(&(value.len() as u32).to_le_bytes());
        frame.extend_from_slice(value);
    }

    #[test]
    fn normalized_predicate_view_decodes_typed_constants() {
        let integer = radixdb_plugin_abi::RadixAbiTypeRefV1::builtin(
            radixdb_plugin_abi::RADIX_BUILTIN_INTEGER,
        );
        let mut frame = Vec::new();
        frame.extend_from_slice(b"RPN1");
        frame.extend_from_slice(&[1; 16]);
        frame.extend_from_slice(&[2; 16]);
        frame.extend_from_slice(&0_u16.to_le_bytes());
        frame.extend_from_slice(&3_u16.to_le_bytes());
        push_argument(&mut frame, 1, 0, integer, &[]);
        push_argument(&mut frame, 2, 0, integer, &42_i64.to_le_bytes());
        push_argument(&mut frame, 2, 1, integer, &[]);

        let predicate = PredicateView { bytes: &frame };
        assert_eq!(predicate.target_function_id().unwrap(), [1; 16]);
        assert_eq!(predicate.operator_class_id().unwrap(), [2; 16]);
        assert_eq!(predicate.indexed_argument().unwrap(), 0);
        assert_eq!(predicate.argument_count().unwrap(), 3);
        assert!(predicate.constant::<i64>(0).is_err());
        assert_eq!(predicate.constant::<i64>(1).unwrap(), Some(42));
        assert_eq!(predicate.constant::<i64>(2).unwrap(), None);
        assert!(predicate.constant::<i64>(3).is_err());
    }

    #[test]
    fn normalized_predicate_view_rejects_malformed_frames() {
        let predicate = PredicateView { bytes: b"bad" };
        assert!(predicate.argument_count().is_err());

        let mut truncated = Vec::new();
        truncated.extend_from_slice(b"RPN1");
        truncated.extend_from_slice(&[1; 32]);
        truncated.extend_from_slice(&0_u16.to_le_bytes());
        truncated.extend_from_slice(&1_u16.to_le_bytes());
        let predicate = PredicateView { bytes: &truncated };
        assert!(predicate.constant::<i64>(0).is_err());
    }
}
