use std::{
    cmp::Ordering,
    slice,
    time::{SystemTime, UNIX_EPOCH},
};

use radixdb_core::{DataType, ExternalTypeRef, Value};
use radixdb_plugin_abi as abi;
use thiserror::Error;

use crate::{
    ObjectId, PluginRegistry, RegisteredExternalType, RegisteredFunction, RegisteredTypeRef,
};

const DEFAULT_WORK_UNITS: u32 = 1_000_000;
const MAX_NORMALIZED_PREDICATE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PluginInvocationError {
    #[error("external type {0:02x?} is not present in the immutable plugin registry")]
    UnknownType([u8; 16]),
    #[error("native function {0:02x?} is not present in the immutable plugin registry")]
    UnknownFunction([u8; 16]),
    #[error("operator class {0:02x?} is not present in the immutable plugin registry")]
    UnknownOperatorClass([u8; 16]),
    #[error("planner support {0:02x?} is not present in the immutable plugin registry")]
    UnknownPlannerSupport([u8; 16]),
    #[error("external value codec version {actual} does not match admitted version {expected}")]
    CodecVersion { expected: u32, actual: u32 },
    #[error("external type does not provide the required {0} capability")]
    MissingCapability(&'static str),
    #[error("external value exceeds admitted payload limit")]
    PayloadLimit,
    #[error("plugin callback failed with status {status}: {diagnostic}")]
    Callback { status: u32, diagnostic: String },
    #[error("plugin callback violated the ABI result contract: {0}")]
    Contract(&'static str),
}

impl PluginInvocationError {
    pub const fn status_code(&self) -> abi::RadixAbiStatusV1 {
        match self {
            Self::UnknownType(_)
            | Self::UnknownFunction(_)
            | Self::UnknownOperatorClass(_)
            | Self::UnknownPlannerSupport(_)
            | Self::MissingCapability(_) => abi::RADIX_STATUS_INVALID_ARGUMENT,
            Self::CodecVersion { .. } | Self::Contract(_) => abi::RADIX_STATUS_CONTRACT_VIOLATION,
            Self::PayloadLimit => abi::RADIX_STATUS_LIMIT_EXCEEDED,
            Self::Callback { status, .. } => *status,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NormalizedPredicateArgument<'a> {
    IndexedColumn,
    Constant(&'a Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateKeySpan {
    pub start: Value,
    pub end: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlannerPlanIdentity {
    pub registry_generation: u64,
    pub support_id: ObjectId,
    pub support_semantic_revision: u32,
    pub support_fingerprint: [u8; 32],
    pub target_function_id: ObjectId,
    pub function_semantic_revision: u32,
    pub operator_class_id: ObjectId,
    pub operator_class_semantic_revision: u32,
    pub key_codec_revision: u32,
    pub operator_class_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidatePlan {
    pub spans: Vec<CandidateKeySpan>,
    pub requires_recheck: bool,
    pub estimated_rows: u64,
    pub cost_hint: u32,
    pub identity: PlannerPlanIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerSupportOutcome {
    Plan(CandidatePlan),
    Fallback { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashComponent {
    pub kind: u16,
    pub bytes: Vec<u8>,
}

#[derive(Default)]
struct CallState {
    staged: Vec<Vec<u8>>,
    staged_nulls: Vec<bool>,
    finished: bool,
    output_bytes: usize,
    max_output_bytes: u32,
    max_items: u32,
    work_units: u32,
    diagnostic: String,
    hash_components: Vec<HashComponent>,
    hash_bytes: usize,
    cancel_check: Option<fn() -> bool>,
    deadline_unix_ns: u64,
}

impl CallState {
    fn new(max_output_bytes: u32, max_items: u32) -> Self {
        Self {
            max_output_bytes,
            max_items,
            deadline_unix_ns: u64::MAX,
            ..Self::default()
        }
    }

    fn with_limits(mut self, limits: InvocationLimits) -> Self {
        self.cancel_check = limits.cancel_check;
        self.deadline_unix_ns = limits.deadline_unix_ns;
        self
    }

    fn handle(&mut self) -> u64 {
        self as *mut Self as usize as u64
    }

    fn diagnostic_sink(&mut self) -> abi::RadixAbiDiagnosticSinkV1 {
        abi::RadixAbiDiagnosticSinkV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiDiagnosticSinkV1>(0),
            handle: self.handle(),
            max_detail_bytes: abi::RADIX_MAX_DIAGNOSTIC_BYTES,
            reserved: 0,
            write: Some(write_diagnostic),
        }
    }

    fn context(
        &mut self,
        diagnostics: &abi::RadixAbiDiagnosticSinkV1,
    ) -> abi::RadixAbiCallContextV1 {
        abi::RadixAbiCallContextV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiCallContextV1>(0),
            handle: self.handle(),
            deadline_unix_ns: self.deadline_unix_ns,
            max_output_bytes: self.max_output_bytes.max(1),
            max_work_units: DEFAULT_WORK_UNITS,
            check_cancelled: Some(check_cancelled),
            charge_work: Some(charge_work),
            diagnostics,
        }
    }

    fn result_builder(&mut self) -> abi::RadixAbiResultBuilderV1 {
        abi::RadixAbiResultBuilderV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiResultBuilderV1>(0),
            handle: self.handle(),
            max_bytes: self.max_output_bytes.max(1),
            max_items: self.max_items.max(1),
            write: Some(write_result),
            finish: Some(finish_result),
        }
    }

    fn hash_sink(&mut self) -> abi::RadixAbiHashSinkV1 {
        abi::RadixAbiHashSinkV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiHashSinkV1>(0),
            handle: self.handle(),
            max_components: abi::RADIX_MAX_HASH_COMPONENTS,
            max_bytes: abi::RADIX_MAX_HASH_BYTES,
            append: Some(append_hash),
        }
    }

    fn callback_error(&self, status: abi::RadixAbiStatusV1) -> PluginInvocationError {
        PluginInvocationError::Callback {
            status,
            diagnostic: if self.diagnostic.is_empty() {
                "plugin returned no diagnostic".to_owned()
            } else {
                self.diagnostic.clone()
            },
        }
    }
}

unsafe fn state(handle: u64) -> &'static mut CallState {
    // SAFETY: every host callback is synchronous and the handle is made from
    // the live stack-owned CallState for that invocation.
    unsafe { &mut *(handle as usize as *mut CallState) }
}

unsafe extern "C" fn check_cancelled(_handle: u64) -> abi::RadixAbiStatusV1 {
    let state = unsafe { state(_handle) };
    let cancelled = state.cancel_check.is_some_and(|check| check());
    let expired = state.deadline_unix_ns != u64::MAX
        && SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(true, |now| {
                now.as_nanos() >= u128::from(state.deadline_unix_ns)
            });
    if cancelled || expired {
        abi::RADIX_STATUS_CANCELLED
    } else {
        abi::RADIX_STATUS_OK
    }
}

unsafe extern "C" fn charge_work(handle: u64, units: u32) -> abi::RadixAbiStatusV1 {
    let state = unsafe { state(handle) };
    let Some(total) = state.work_units.checked_add(units) else {
        return abi::RADIX_STATUS_LIMIT_EXCEEDED;
    };
    if total > DEFAULT_WORK_UNITS {
        abi::RADIX_STATUS_LIMIT_EXCEEDED
    } else {
        state.work_units = total;
        abi::RADIX_STATUS_OK
    }
}

unsafe extern "C" fn write_diagnostic(
    handle: u64,
    diagnostic: *const abi::RadixAbiDiagnosticV1,
) -> abi::RadixAbiStatusV1 {
    let Some(diagnostic) = (unsafe { diagnostic.as_ref() }) else {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    };
    if abi::validate_diagnostic(diagnostic).is_err() {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    let bytes = if diagnostic.detail.len == 0 {
        &[][..]
    } else {
        // SAFETY: the plugin owns this validated range for the synchronous call.
        unsafe { slice::from_raw_parts(diagnostic.detail.ptr, diagnostic.detail.len as usize) }
    };
    let Ok(detail) = std::str::from_utf8(bytes) else {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    };
    unsafe { state(handle) }.diagnostic = detail.to_owned();
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn write_result(
    handle: u64,
    flags: u32,
    reserved: u32,
    bytes: abi::RadixAbiSliceV1,
) -> abi::RadixAbiStatusV1 {
    let state = unsafe { state(handle) };
    if state.finished
        || state.staged.len() >= state.max_items as usize
        || abi::validate_result_item(flags, reserved, bytes, state.max_output_bytes).is_err()
    {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    let next = match state.output_bytes.checked_add(bytes.len as usize) {
        Some(next) if next <= state.max_output_bytes as usize => next,
        _ => return abi::RADIX_STATUS_LIMIT_EXCEEDED,
    };
    let copied = if bytes.len == 0 {
        Vec::new()
    } else {
        // SAFETY: validate_result_item admitted this call-scoped byte range.
        unsafe { slice::from_raw_parts(bytes.ptr, bytes.len as usize) }.to_vec()
    };
    state.output_bytes = next;
    state.staged.push(copied);
    state
        .staged_nulls
        .push(flags & abi::RADIX_RESULT_ITEM_FLAG_NULL != 0);
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn finish_result(handle: u64) -> abi::RadixAbiStatusV1 {
    let state = unsafe { state(handle) };
    if state.finished {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    state.finished = true;
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn append_hash(
    handle: u64,
    kind: u16,
    reserved: u16,
    bytes: abi::RadixAbiSliceV1,
) -> abi::RadixAbiStatusV1 {
    let state = unsafe { state(handle) };
    let remaining = abi::RADIX_MAX_HASH_BYTES.saturating_sub(state.hash_bytes as u32);
    if state.hash_components.len() >= abi::RADIX_MAX_HASH_COMPONENTS as usize
        || abi::validate_hash_component(kind, reserved, bytes, remaining).is_err()
    {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    let copied = if bytes.len == 0 {
        Vec::new()
    } else {
        // SAFETY: validate_hash_component admitted this call-scoped byte range.
        unsafe { slice::from_raw_parts(bytes.ptr, bytes.len as usize) }.to_vec()
    };
    state.hash_bytes += copied.len();
    state.hash_components.push(HashComponent {
        kind,
        bytes: copied,
    });
    abi::RADIX_STATUS_OK
}

fn external<'r, 'v>(
    registry: &'r PluginRegistry,
    value: &'v Value,
) -> Result<
    (
        &'r RegisteredExternalType,
        radixdb_core::ExternalValueRef<'v>,
    ),
    PluginInvocationError,
> {
    let view = value
        .as_external()
        .ok_or(PluginInvocationError::Contract("value is not external"))?;
    let registered = registry
        .external_type(&view.type_ref().type_object_id())
        .ok_or(PluginInvocationError::UnknownType(
            view.type_ref().type_object_id(),
        ))?;
    if view.type_ref().codec_version() != registered.codec_version {
        return Err(PluginInvocationError::CodecVersion {
            expected: registered.codec_version,
            actual: view.type_ref().codec_version(),
        });
    }
    if view.payload().len() > registered.max_bytes as usize {
        return Err(PluginInvocationError::PayloadLimit);
    }
    Ok((registered, view))
}

fn raw_value(view: radixdb_core::ExternalValueRef<'_>) -> abi::RadixAbiValueV1 {
    abi::RadixAbiValueV1 {
        type_ref: abi::RadixAbiTypeRefV1::external(
            view.type_ref().type_object_id(),
            view.type_ref().codec_version(),
        ),
        flags: 0,
        reserved: 0,
        inline_bytes: [0; 16],
        borrowed_bytes: abi::RadixAbiSliceV1 {
            ptr: view.payload().as_ptr(),
            len: view.payload().len() as u32,
            reserved: 0,
        },
    }
}

fn run_parse(
    callback: abi::RadixAbiParseFnV1,
    input: &[u8],
    maximum: u32,
) -> Result<Vec<u8>, PluginInvocationError> {
    if input.len() > u32::MAX as usize {
        return Err(PluginInvocationError::PayloadLimit);
    }
    let mut state = CallState::new(maximum, 1);
    let diagnostics = state.diagnostic_sink();
    let context = state.context(&diagnostics);
    let output = state.result_builder();
    let input = abi::RadixAbiSliceV1 {
        ptr: input.as_ptr(),
        len: input.len() as u32,
        reserved: 0,
    };
    // SAFETY: all pointers refer to live call-scoped host objects and input.
    let status = unsafe { callback(&context, input, &output) };
    finish_single_output(state, status)
}

fn run_codec(
    callback: abi::RadixAbiCodecFnV1,
    input: &abi::RadixAbiValueV1,
    maximum: u32,
) -> Result<Vec<u8>, PluginInvocationError> {
    let mut state = CallState::new(maximum, 1);
    let diagnostics = state.diagnostic_sink();
    let context = state.context(&diagnostics);
    let output = state.result_builder();
    // SAFETY: all pointers refer to live call-scoped host objects and value bytes.
    let status = unsafe { callback(&context, input, &output) };
    finish_single_output(state, status)
}

fn finish_single_output(
    mut state: CallState,
    status: abi::RadixAbiStatusV1,
) -> Result<Vec<u8>, PluginInvocationError> {
    if status != abi::RADIX_STATUS_OK {
        return Err(state.callback_error(status));
    }
    if !state.finished || state.staged.len() != 1 {
        return Err(PluginInvocationError::Contract(
            "callback did not finish exactly one result",
        ));
    }
    if state.staged_nulls != [false] {
        return Err(PluginInvocationError::Contract(
            "codec callback returned NULL",
        ));
    }
    Ok(state.staged.pop().expect("one checked result"))
}

#[derive(Debug, Clone, Copy)]
pub struct InvocationLimits {
    pub cancel_check: Option<fn() -> bool>,
    pub deadline_unix_ns: u64,
}

impl Default for InvocationLimits {
    fn default() -> Self {
        Self {
            cancel_check: None,
            deadline_unix_ns: u64::MAX,
        }
    }
}

impl InvocationLimits {
    fn cancelled_or_expired(self) -> bool {
        self.cancel_check.is_some_and(|check| check())
            || (self.deadline_unix_ns != u64::MAX
                && SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(true, |now| {
                        now.as_nanos() >= u128::from(self.deadline_unix_ns)
                    }))
    }
}

fn matching_pair<'a>(
    registry: &'a PluginRegistry,
    left: &Value,
    right: &Value,
) -> Result<
    (
        &'a RegisteredExternalType,
        abi::RadixAbiValueV1,
        abi::RadixAbiValueV1,
    ),
    PluginInvocationError,
> {
    let (left_type, left_view) = external(registry, left)?;
    let (_, right_view) = external(registry, right)?;
    if left_view.type_ref() != right_view.type_ref() {
        return Err(PluginInvocationError::Contract(
            "external comparison requires identical type identity and codec version",
        ));
    }
    Ok((left_type, raw_value(left_view), raw_value(right_view)))
}

fn abi_type_ref(value: RegisteredTypeRef) -> abi::RadixAbiTypeRefV1 {
    match value {
        RegisteredTypeRef::Builtin(tag) => abi::RadixAbiTypeRefV1::builtin(tag),
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => abi::RadixAbiTypeRefV1::external(object_id, codec_version),
    }
}

fn raw_argument(
    value: &Value,
    expected: RegisteredTypeRef,
    backing: &mut Vec<Vec<u8>>,
) -> Result<abi::RadixAbiValueV1, PluginInvocationError> {
    let type_ref = abi_type_ref(expected);
    if value.is_null() {
        return Ok(abi::RadixAbiValueV1 {
            type_ref,
            flags: abi::RADIX_VALUE_FLAG_NULL,
            reserved: 0,
            inline_bytes: [0; 16],
            borrowed_bytes: abi::RadixAbiSliceV1::EMPTY,
        });
    }
    let mut inline_bytes = [0; 16];
    let variable = match expected {
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => {
            let external = value.as_external().ok_or(PluginInvocationError::Contract(
                "native function argument type differs from descriptor",
            ))?;
            if external.type_ref().type_object_id() != object_id
                || external.type_ref().codec_version() != codec_version
            {
                return Err(PluginInvocationError::Contract(
                    "native function external argument identity differs from descriptor",
                ));
            }
            Some(external.payload().to_vec())
        }
        RegisteredTypeRef::Builtin(tag) => {
            let actual = u16::from(value.data_type().as_u8());
            if actual != tag {
                return Err(PluginInvocationError::Contract(
                    "native function built-in argument type differs from descriptor",
                ));
            }
            match tag {
                abi::RADIX_BUILTIN_INTEGER => {
                    inline_bytes[..8].copy_from_slice(
                        &value
                            .as_int64()
                            .expect("exact INTEGER type was checked")
                            .to_le_bytes(),
                    );
                    None
                }
                abi::RADIX_BUILTIN_FLOAT => {
                    inline_bytes[..8].copy_from_slice(
                        &value
                            .as_float64()
                            .expect("exact FLOAT type was checked")
                            .to_bits()
                            .to_le_bytes(),
                    );
                    None
                }
                abi::RADIX_BUILTIN_BOOLEAN => {
                    inline_bytes[0] =
                        u8::from(value.as_boolean().expect("exact BOOLEAN type was checked"));
                    None
                }
                abi::RADIX_BUILTIN_TIMESTAMP => {
                    inline_bytes[..8].copy_from_slice(
                        &value
                            .artifact_timestamp_nanos()
                            .ok_or(PluginInvocationError::Contract(
                                "timestamp is outside the ABI nanosecond range",
                            ))?
                            .to_le_bytes(),
                    );
                    None
                }
                abi::RADIX_BUILTIN_UUID => {
                    inline_bytes.copy_from_slice(
                        &value.as_uuid_bytes().expect("exact UUID type was checked"),
                    );
                    None
                }
                abi::RADIX_BUILTIN_DATE => {
                    inline_bytes[..4].copy_from_slice(
                        &value
                            .as_date_days()
                            .expect("exact DATE type was checked")
                            .to_le_bytes(),
                    );
                    None
                }
                abi::RADIX_BUILTIN_TEXT => Some(
                    value
                        .as_str()
                        .expect("exact TEXT type was checked")
                        .as_bytes()
                        .to_vec(),
                ),
                abi::RADIX_BUILTIN_JSON => Some(
                    value
                        .as_json()
                        .expect("exact JSON type was checked")
                        .as_bytes()
                        .to_vec(),
                ),
                abi::RADIX_BUILTIN_VECTOR => Some(
                    value
                        .as_vector_f32()
                        .expect("exact VECTOR type was checked")
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                ),
                abi::RADIX_BUILTIN_DECIMAL => {
                    let (unscaled, precision, scale) = value
                        .as_decimal_parts()
                        .expect("exact DECIMAL type was checked");
                    let mut bytes = unscaled.to_le_bytes().to_vec();
                    bytes.extend([precision, scale]);
                    Some(bytes)
                }
                abi::RADIX_BUILTIN_BYTES => Some(
                    value
                        .as_bytes_value()
                        .expect("exact BYTES type was checked")
                        .to_vec(),
                ),
                _ => {
                    return Err(PluginInvocationError::Contract(
                        "native function descriptor contains an unknown built-in type",
                    ))
                }
            }
        }
    };
    let borrowed_bytes = if let Some(bytes) = variable {
        backing.push(bytes);
        let bytes = backing.last().expect("just pushed ABI backing bytes");
        abi::RadixAbiSliceV1 {
            ptr: bytes.as_ptr(),
            len: bytes.len() as u32,
            reserved: 0,
        }
    } else {
        abi::RadixAbiSliceV1::EMPTY
    };
    let raw = abi::RadixAbiValueV1 {
        type_ref,
        flags: 0,
        reserved: 0,
        inline_bytes,
        borrowed_bytes,
    };
    // SAFETY: variable storage remains owned by `backing` for the callback.
    unsafe { abi::validate_value_contents(&raw) }
        .map_err(|_| PluginInvocationError::Contract("host encoded an invalid ABI argument"))?;
    Ok(raw)
}

fn encode_normalized_predicate(
    target_function_id: ObjectId,
    operator_class_id: ObjectId,
    indexed_argument: usize,
    arguments: &[NormalizedPredicateArgument<'_>],
    expected: &[RegisteredTypeRef],
) -> Result<Vec<u8>, PluginInvocationError> {
    let indexed_argument = u16::try_from(indexed_argument).map_err(|_| {
        PluginInvocationError::Contract("normalized predicate argument index exceeds u16")
    })?;
    let argument_count = u16::try_from(arguments.len()).map_err(|_| {
        PluginInvocationError::Contract("normalized predicate argument count exceeds u16")
    })?;
    let mut output = Vec::new();
    output.extend_from_slice(b"RPN1");
    output.extend_from_slice(&target_function_id);
    output.extend_from_slice(&operator_class_id);
    output.extend_from_slice(&indexed_argument.to_le_bytes());
    output.extend_from_slice(&argument_count.to_le_bytes());
    let mut backing = Vec::with_capacity(arguments.len());
    for (argument, expected) in arguments.iter().zip(expected.iter().copied()) {
        let (kind, flags, bytes) = match argument {
            NormalizedPredicateArgument::IndexedColumn => (1_u8, 0_u8, Vec::new()),
            NormalizedPredicateArgument::Constant(value) => {
                let raw = raw_argument(value, expected, &mut backing)?;
                let bytes = if raw.borrowed_bytes.len != 0 {
                    // SAFETY: raw points into one of the call-owned backing buffers.
                    unsafe {
                        slice::from_raw_parts(
                            raw.borrowed_bytes.ptr,
                            raw.borrowed_bytes.len as usize,
                        )
                    }
                    .to_vec()
                } else if raw.flags & abi::RADIX_VALUE_FLAG_NULL != 0 {
                    Vec::new()
                } else {
                    let width = normalized_fixed_width(expected)?;
                    raw.inline_bytes[..width].to_vec()
                };
                (
                    2_u8,
                    u8::from(raw.flags & abi::RADIX_VALUE_FLAG_NULL != 0),
                    bytes,
                )
            }
        };
        let type_ref = abi_type_ref(expected);
        let len = u32::try_from(bytes.len()).map_err(|_| PluginInvocationError::PayloadLimit)?;
        output.push(kind);
        output.push(flags);
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&type_ref.kind.to_le_bytes());
        output.extend_from_slice(&type_ref.builtin_tag.to_le_bytes());
        output.extend_from_slice(&type_ref.object_id);
        output.extend_from_slice(&type_ref.codec_version.to_le_bytes());
        output.extend_from_slice(&len.to_le_bytes());
        output.extend_from_slice(&bytes);
    }
    if output.len() > MAX_NORMALIZED_PREDICATE_BYTES {
        return Err(PluginInvocationError::PayloadLimit);
    }
    Ok(output)
}

fn normalized_fixed_width(expected: RegisteredTypeRef) -> Result<usize, PluginInvocationError> {
    match expected {
        RegisteredTypeRef::External { .. } => Ok(0),
        RegisteredTypeRef::Builtin(tag) => match tag {
            abi::RADIX_BUILTIN_INTEGER
            | abi::RADIX_BUILTIN_FLOAT
            | abi::RADIX_BUILTIN_TIMESTAMP => Ok(8),
            abi::RADIX_BUILTIN_BOOLEAN => Ok(1),
            abi::RADIX_BUILTIN_UUID => Ok(16),
            abi::RADIX_BUILTIN_DATE => Ok(4),
            abi::RADIX_BUILTIN_TEXT
            | abi::RADIX_BUILTIN_JSON
            | abi::RADIX_BUILTIN_VECTOR
            | abi::RADIX_BUILTIN_DECIMAL
            | abi::RADIX_BUILTIN_BYTES => Ok(0),
            _ => Err(PluginInvocationError::Contract(
                "normalized predicate contains an unknown built-in type",
            )),
        },
    }
}

fn decode_function_result(
    registry: &PluginRegistry,
    expected: RegisteredTypeRef,
    is_null: bool,
    bytes: Vec<u8>,
) -> Result<Value, PluginInvocationError> {
    if is_null {
        return Ok(Value::Null(match expected {
            RegisteredTypeRef::Builtin(tag) => u8::try_from(tag)
                .ok()
                .and_then(DataType::from_u8)
                .unwrap_or(DataType::Null),
            RegisteredTypeRef::External { .. } => DataType::Null,
        }));
    }
    let value = match expected {
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => Value::try_external(
            ExternalTypeRef::new(object_id, codec_version)
                .map_err(|_| PluginInvocationError::Contract("invalid result type identity"))?,
            bytes,
        )
        .map_err(|_| PluginInvocationError::PayloadLimit)?,
        RegisteredTypeRef::Builtin(tag) => match tag {
            abi::RADIX_BUILTIN_INTEGER if bytes.len() == 8 => Value::Integer(i64::from_le_bytes(
                bytes.as_slice().try_into().expect("length checked"),
            )),
            abi::RADIX_BUILTIN_FLOAT if bytes.len() == 8 => Value::Float(f64::from_bits(
                u64::from_le_bytes(bytes.as_slice().try_into().expect("length checked")),
            )),
            abi::RADIX_BUILTIN_BOOLEAN if bytes.as_slice() == [0] => Value::Boolean(false),
            abi::RADIX_BUILTIN_BOOLEAN if bytes.as_slice() == [1] => Value::Boolean(true),
            abi::RADIX_BUILTIN_TIMESTAMP if bytes.len() == 8 => Value::Integer(i64::from_le_bytes(
                bytes.as_slice().try_into().expect("length checked"),
            ))
            .try_coerce_to_type(DataType::Timestamp)
            .map_err(|_| PluginInvocationError::Contract("invalid TIMESTAMP result"))?,
            abi::RADIX_BUILTIN_TEXT => Value::text(
                String::from_utf8(bytes)
                    .map_err(|_| PluginInvocationError::Contract("TEXT result is not UTF-8"))?,
            ),
            abi::RADIX_BUILTIN_JSON => Value::try_json(
                String::from_utf8(bytes)
                    .map_err(|_| PluginInvocationError::Contract("JSON result is not UTF-8"))?,
            )
            .map_err(|_| PluginInvocationError::Contract("JSON result is invalid"))?,
            abi::RADIX_BUILTIN_VECTOR => Value::try_vector_from_bytes(bytes.into())
                .map_err(|_| PluginInvocationError::Contract("VECTOR result is invalid"))?,
            abi::RADIX_BUILTIN_UUID if bytes.len() == 16 => {
                Value::uuid(bytes.as_slice().try_into().expect("length checked"))
            }
            abi::RADIX_BUILTIN_DECIMAL if bytes.len() == 18 => Value::try_decimal(
                i128::from_le_bytes(bytes[..16].try_into().expect("length checked")),
                bytes[16],
                bytes[17],
            )
            .map_err(|_| PluginInvocationError::Contract("DECIMAL result is invalid"))?,
            abi::RADIX_BUILTIN_DATE if bytes.len() == 4 => Value::date(i32::from_le_bytes(
                bytes.as_slice().try_into().expect("length checked"),
            )),
            abi::RADIX_BUILTIN_BYTES => Value::bytes(bytes),
            _ => {
                return Err(PluginInvocationError::Contract(
                    "native function returned bytes incompatible with its result type",
                ))
            }
        },
    };
    if matches!(expected, RegisteredTypeRef::External { .. }) {
        registry.validate_external_value(&value)?;
    } else {
        value
            .validate_shape()
            .map_err(|_| PluginInvocationError::Contract("native function result is malformed"))?;
    }
    Ok(value)
}

fn finish_function_output(
    registry: &PluginRegistry,
    function: &RegisteredFunction,
    mut state: CallState,
    status: abi::RadixAbiStatusV1,
) -> Result<Value, PluginInvocationError> {
    if status != abi::RADIX_STATUS_OK {
        return Err(state.callback_error(status));
    }
    if !state.finished || state.staged.len() != 1 || state.staged_nulls.len() != 1 {
        return Err(PluginInvocationError::Contract(
            "native function did not finish exactly one result",
        ));
    }
    let bytes = state.staged.pop().expect("one checked result");
    let is_null = state.staged_nulls.pop().expect("one checked null flag");
    decode_function_result(registry, function.result, is_null, bytes)
}

enum BatchColumnStorage {
    Aligned(Vec<u64>),
    Bytes(Vec<u8>),
}

impl BatchColumnStorage {
    fn bytes(&self) -> (*const u8, usize) {
        match self {
            Self::Aligned(words) => (words.as_ptr().cast(), words.len() * 8),
            Self::Bytes(bytes) => (bytes.as_ptr(), bytes.len()),
        }
    }
}

struct OwnedBatchColumn {
    type_ref: abi::RadixAbiTypeRefV1,
    layout: u16,
    element_width: u16,
    alignment: u16,
    stride: u32,
    null_bitmap: Vec<u8>,
    storage: BatchColumnStorage,
    offsets: Vec<u32>,
}

impl OwnedBatchColumn {
    fn new(
        registry: &PluginRegistry,
        expected: RegisteredTypeRef,
        values: &[&Value],
    ) -> Result<Self, PluginInvocationError> {
        let type_ref = abi_type_ref(expected);
        let mut backing = Vec::with_capacity(values.len());
        let raw = values
            .iter()
            .map(|value| raw_argument(value, expected, &mut backing))
            .collect::<Result<Vec<_>, _>>()?;
        let mut null_bitmap = vec![0_u8; values.len().div_ceil(8)];
        for (index, value) in values.iter().enumerate() {
            if value.is_null() {
                null_bitmap[index / 8] |= 1 << (index % 8);
            }
        }
        if null_bitmap.iter().all(|byte| *byte == 0) {
            null_bitmap.clear();
        }

        if let Some(width) = batch_fixed_width(registry, expected)? {
            let stride = width.div_ceil(8) * 8;
            let total = stride
                .checked_mul(values.len())
                .ok_or(PluginInvocationError::PayloadLimit)?;
            let mut words = vec![0_u64; total.div_ceil(8)];
            // SAFETY: the u64 allocation owns `words.len() * 8` initialized
            // bytes and provides the alignment declared by the ABI view.
            let bytes = unsafe {
                slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), words.len() * 8)
            };
            for (row, value) in raw.iter().enumerate() {
                if value.flags & abi::RADIX_VALUE_FLAG_NULL != 0 {
                    continue;
                }
                let source = if value.borrowed_bytes.len == 0 {
                    &value.inline_bytes[..width]
                } else {
                    // SAFETY: `backing` owns every borrowed argument until
                    // this constructor has copied the bytes into the column.
                    unsafe {
                        slice::from_raw_parts(
                            value.borrowed_bytes.ptr,
                            value.borrowed_bytes.len as usize,
                        )
                    }
                };
                if source.len() != width {
                    return Err(PluginInvocationError::Contract(
                        "native batch fixed-width argument has the wrong width",
                    ));
                }
                let start = row * stride;
                bytes[start..start + width].copy_from_slice(source);
            }
            return Ok(Self {
                type_ref,
                layout: abi::RADIX_COLUMN_LAYOUT_FIXED,
                element_width: u16::try_from(width)
                    .map_err(|_| PluginInvocationError::PayloadLimit)?,
                alignment: 8,
                stride: u32::try_from(stride).map_err(|_| PluginInvocationError::PayloadLimit)?,
                null_bitmap,
                storage: BatchColumnStorage::Aligned(words),
                offsets: Vec::new(),
            });
        }

        let mut bytes = Vec::new();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0);
        for value in &raw {
            if value.flags & abi::RADIX_VALUE_FLAG_NULL == 0 {
                // SAFETY: variable arguments borrow from `backing`, which is
                // live until the complete column has been copied.
                bytes.extend_from_slice(unsafe {
                    slice::from_raw_parts(
                        value.borrowed_bytes.ptr,
                        value.borrowed_bytes.len as usize,
                    )
                });
            }
            offsets
                .push(u32::try_from(bytes.len()).map_err(|_| PluginInvocationError::PayloadLimit)?);
        }
        Ok(Self {
            type_ref,
            layout: abi::RADIX_COLUMN_LAYOUT_VARIABLE,
            element_width: 0,
            alignment: 1,
            stride: 0,
            null_bitmap,
            storage: BatchColumnStorage::Bytes(bytes),
            offsets,
        })
    }

    fn as_abi(&self, row_count: u32) -> abi::RadixAbiColumnViewV1 {
        let (data, data_len) = self.storage.bytes();
        abi::RadixAbiColumnViewV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiColumnViewV1>(0),
            type_ref: self.type_ref,
            row_count,
            layout: self.layout,
            element_width: self.element_width,
            alignment: self.alignment,
            reserved_u16: 0,
            stride: self.stride,
            null_bitmap: abi::RadixAbiSliceV1 {
                ptr: if self.null_bitmap.is_empty() {
                    std::ptr::null()
                } else {
                    self.null_bitmap.as_ptr()
                },
                len: self.null_bitmap.len() as u32,
                reserved: 0,
            },
            data: abi::RadixAbiSliceV1 {
                ptr: if data_len == 0 {
                    std::ptr::null()
                } else {
                    data
                },
                len: data_len as u32,
                reserved: 0,
            },
            offsets: abi::RadixAbiU32SliceV1 {
                ptr: if self.offsets.is_empty() {
                    std::ptr::null()
                } else {
                    self.offsets.as_ptr()
                },
                len: self.offsets.len() as u32,
                reserved: 0,
            },
        }
    }
}

fn batch_fixed_width(
    registry: &PluginRegistry,
    expected: RegisteredTypeRef,
) -> Result<Option<usize>, PluginInvocationError> {
    match expected {
        RegisteredTypeRef::Builtin(tag) => Ok(match tag {
            abi::RADIX_BUILTIN_INTEGER
            | abi::RADIX_BUILTIN_FLOAT
            | abi::RADIX_BUILTIN_TIMESTAMP => Some(8),
            abi::RADIX_BUILTIN_BOOLEAN => Some(1),
            abi::RADIX_BUILTIN_UUID => Some(16),
            abi::RADIX_BUILTIN_DECIMAL => Some(18),
            abi::RADIX_BUILTIN_DATE => Some(4),
            abi::RADIX_BUILTIN_TEXT
            | abi::RADIX_BUILTIN_JSON
            | abi::RADIX_BUILTIN_VECTOR
            | abi::RADIX_BUILTIN_BYTES => None,
            _ => {
                return Err(PluginInvocationError::Contract(
                    "native function descriptor contains an unknown built-in type",
                ))
            }
        }),
        RegisteredTypeRef::External { object_id, .. } => {
            let registered = registry
                .external_type(&object_id)
                .ok_or(PluginInvocationError::UnknownType(object_id))?;
            Ok(
                (registered.storage_kind == abi::RADIX_EXTERNAL_STORAGE_FIXED)
                    .then_some(registered.fixed_bytes as usize),
            )
        }
    }
}

fn finish_batch_output(
    registry: &PluginRegistry,
    function: &RegisteredFunction,
    state: CallState,
    status: abi::RadixAbiStatusV1,
    expected_rows: usize,
) -> Result<Vec<Value>, PluginInvocationError> {
    if status != abi::RADIX_STATUS_OK {
        return Err(state.callback_error(status));
    }
    if !state.finished
        || state.staged.len() != expected_rows
        || state.staged_nulls.len() != expected_rows
    {
        return Err(PluginInvocationError::Contract(
            "native batch did not finish exactly one result per row",
        ));
    }
    state
        .staged
        .into_iter()
        .zip(state.staged_nulls)
        .map(|(bytes, is_null)| decode_function_result(registry, function.result, is_null, bytes))
        .collect()
}

impl PluginRegistry {
    /// Invoke a database-bound planner support callback with a closed,
    /// versioned predicate frame. The plugin can only return declarative key
    /// spans; every span is decoded and canonicalized by the host.
    pub fn invoke_planner_support(
        &self,
        support_id: ObjectId,
        arguments: &[NormalizedPredicateArgument<'_>],
        limits: InvocationLimits,
    ) -> Result<PlannerSupportOutcome, PluginInvocationError> {
        let support = self
            .planner_support(&support_id)
            .ok_or(PluginInvocationError::UnknownPlannerSupport(support_id))?;
        let target_function_id =
            support
                .target_function_id
                .ok_or(PluginInvocationError::Contract(
                    "planner support has no target function",
                ))?;
        let operator_class_id =
            support
                .target_operator_class_id
                .ok_or(PluginInvocationError::Contract(
                    "planner support has no target operator class",
                ))?;
        let function = self
            .function(&target_function_id)
            .ok_or(PluginInvocationError::UnknownFunction(target_function_id))?;
        let operator_class = self.operator_class(&operator_class_id).ok_or(
            PluginInvocationError::UnknownOperatorClass(operator_class_id),
        )?;
        if arguments.len() != function.arguments.len() {
            return Err(PluginInvocationError::Contract(
                "normalized predicate arity differs from target function",
            ));
        }
        let indexed = arguments
            .iter()
            .position(|argument| matches!(argument, NormalizedPredicateArgument::IndexedColumn))
            .ok_or(PluginInvocationError::Contract(
                "normalized predicate has no indexed argument",
            ))?;
        if arguments
            .iter()
            .skip(indexed + 1)
            .any(|argument| matches!(argument, NormalizedPredicateArgument::IndexedColumn))
            || function.arguments[indexed] != operator_class.input_type
        {
            return Err(PluginInvocationError::Contract(
                "normalized predicate has an invalid indexed argument",
            ));
        }
        if limits.cancelled_or_expired() {
            return Ok(PlannerSupportOutcome::Fallback {
                reason: "planner support was cancelled before dispatch".to_owned(),
            });
        }

        let predicate = encode_normalized_predicate(
            target_function_id,
            operator_class_id,
            indexed,
            arguments,
            &function.arguments,
        )?;
        let mut state = CallState::new(
            support.max_output_bytes,
            support.max_spans.saturating_add(1),
        )
        .with_limits(limits);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let output = state.result_builder();
        let input = abi::RadixAbiSliceV1 {
            ptr: predicate.as_ptr(),
            len: predicate.len() as u32,
            reserved: 0,
        };
        // SAFETY: admission validated the callback and every host-owned buffer
        // remains live for the complete synchronous call.
        let status = unsafe { (support.callback)(&context, input, &output) };
        if status != abi::RADIX_STATUS_OK {
            if matches!(
                status,
                abi::RADIX_STATUS_INVALID_ARGUMENT
                    | abi::RADIX_STATUS_DOMAIN_ERROR
                    | abi::RADIX_STATUS_LIMIT_EXCEEDED
                    | abi::RADIX_STATUS_CANCELLED
            ) {
                return Ok(PlannerSupportOutcome::Fallback {
                    reason: state.callback_error(status).to_string(),
                });
            }
            return Err(state.callback_error(status));
        }
        if !state.finished || state.staged.len() != state.staged_nulls.len() {
            return Err(PluginInvocationError::Contract(
                "planner support did not finish its candidate plan",
            ));
        }
        if state.staged_nulls.iter().any(|value| *value) {
            return Err(PluginInvocationError::Contract(
                "planner support emitted a NULL plan item",
            ));
        }

        let mut estimate = None;
        let mut spans = Vec::new();
        for item in state.staged {
            match item.first().copied() {
                Some(1) => {
                    if item.len() < 12 || item[..4] != [1, 0, 0, 0] {
                        return Err(PluginInvocationError::Contract(
                            "planner support emitted a malformed span header",
                        ));
                    }
                    let start_len =
                        u32::from_le_bytes(item[4..8].try_into().expect("fixed width")) as usize;
                    let end_len =
                        u32::from_le_bytes(item[8..12].try_into().expect("fixed width")) as usize;
                    let split =
                        12usize
                            .checked_add(start_len)
                            .ok_or(PluginInvocationError::Contract(
                                "planner support span length overflow",
                            ))?;
                    let end = split
                        .checked_add(end_len)
                        .ok_or(PluginInvocationError::Contract(
                            "planner support span length overflow",
                        ))?;
                    if end != item.len() {
                        return Err(PluginInvocationError::Contract(
                            "planner support span lengths are invalid",
                        ));
                    }
                    let start = decode_function_result(
                        self,
                        operator_class.key_type,
                        false,
                        item[12..split].to_vec(),
                    )?;
                    let end_value = decode_function_result(
                        self,
                        operator_class.key_type,
                        false,
                        item[split..end].to_vec(),
                    )?;
                    if start > end_value {
                        return Err(PluginInvocationError::Contract(
                            "planner support span start exceeds its end",
                        ));
                    }
                    spans.push(CandidateKeySpan {
                        start,
                        end: end_value,
                    });
                }
                Some(2) => {
                    if item.len() != 20 || item[..4] != [2, 0, 0, 0] || estimate.is_some() {
                        return Err(PluginInvocationError::Contract(
                            "planner support emitted malformed or duplicate estimate metadata",
                        ));
                    }
                    let rows = u64::from_le_bytes(item[4..12].try_into().expect("fixed width"));
                    let cost = u32::from_le_bytes(item[12..16].try_into().expect("fixed width"));
                    if item[16..20] != [0, 0, 0, 0] || cost > 1_000_000 {
                        return Err(PluginInvocationError::Contract(
                            "planner support estimate metadata is outside its bounds",
                        ));
                    }
                    estimate = Some((rows, cost));
                }
                _ => {
                    return Err(PluginInvocationError::Contract(
                        "planner support emitted an unknown plan item",
                    ))
                }
            }
        }
        let Some((estimated_rows, cost_hint)) = estimate else {
            return Ok(PlannerSupportOutcome::Fallback {
                reason: "planner support returned an unknown estimate".to_owned(),
            });
        };
        if spans.len() > support.max_spans as usize {
            return Err(PluginInvocationError::Contract(
                "planner support exceeded its admitted span bound",
            ));
        }
        spans.sort_by(|left, right| {
            left.start
                .cmp(&right.start)
                .then_with(|| left.end.cmp(&right.end))
        });
        let mut canonical: Vec<CandidateKeySpan> = Vec::with_capacity(spans.len());
        for span in spans {
            if let Some(previous) = canonical.last_mut() {
                if span.start <= previous.end {
                    if span.end > previous.end {
                        previous.end = span.end;
                    }
                    continue;
                }
            }
            canonical.push(span);
        }
        Ok(PlannerSupportOutcome::Plan(CandidatePlan {
            spans: canonical,
            requires_recheck: support.recheck_policy == abi::RADIX_RECHECK_ALWAYS,
            estimated_rows,
            cost_hint,
            identity: PlannerPlanIdentity {
                registry_generation: self.generation(),
                support_id,
                support_semantic_revision: support.semantic_revision,
                support_fingerprint: support.fingerprint,
                target_function_id,
                function_semantic_revision: function.semantic_revision,
                operator_class_id,
                operator_class_semantic_revision: operator_class.semantic_revision,
                key_codec_revision: operator_class.key_codec_revision,
                operator_class_fingerprint: operator_class.fingerprint,
            },
        }))
    }

    /// Produce one core-owned physical key for an admitted operator class.
    ///
    /// B-tree/bitmap classes use their bounded encoder callback. Hash classes
    /// only emit semantic components into the host sink; the core HashIndex
    /// remains the sole owner of hashing algorithm and seed.
    pub fn encode_operator_class_key(
        &self,
        object_id: ObjectId,
        value: &Value,
        limits: InvocationLimits,
    ) -> Result<Value, PluginInvocationError> {
        let operator_class = self
            .operator_class(&object_id)
            .ok_or(PluginInvocationError::UnknownOperatorClass(object_id))?;
        let mut backing = Vec::with_capacity(1);
        let raw = raw_argument(value, operator_class.input_type, &mut backing)?;
        if operator_class.access_method == abi::RADIX_ACCESS_METHOD_HASH {
            if operator_class.key_type != RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BYTES) {
                return Err(PluginInvocationError::Contract(
                    "hash operator class physical key type is not BYTES",
                ));
            }
            let components = self.external_hash_components(value)?;
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&(components.len() as u32).to_le_bytes());
            for component in components {
                bytes.extend_from_slice(&component.kind.to_le_bytes());
                bytes.extend_from_slice(&(component.bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&component.bytes);
            }
            return Ok(Value::bytes(bytes));
        }
        if limits.cancelled_or_expired() {
            return Err(PluginInvocationError::Callback {
                status: abi::RADIX_STATUS_CANCELLED,
                diagnostic: "operator-class key encoding was cancelled before dispatch".to_owned(),
            });
        }
        let mut state = CallState::new(abi::RADIX_MAX_EXTERNAL_VALUE_BYTES, 1).with_limits(limits);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let output = state.result_builder();
        // SAFETY: admission validated the callback and all pointers remain
        // live for this synchronous invocation.
        let status = unsafe { (operator_class.encode_key)(&context, &raw, &output) };
        let bytes = finish_single_output(state, status)?;
        decode_function_result(self, operator_class.key_type, false, bytes)
    }

    pub fn invoke_scalar_function(
        &self,
        object_id: ObjectId,
        arguments: &[Value],
        limits: InvocationLimits,
    ) -> Result<Value, PluginInvocationError> {
        let function = self
            .function(&object_id)
            .ok_or(PluginInvocationError::UnknownFunction(object_id))?;
        if arguments.len() != function.arguments.len() {
            return Err(PluginInvocationError::Contract(
                "native function argument count differs from descriptor",
            ));
        }
        if function.strict && arguments.iter().any(Value::is_null) {
            return decode_function_result(self, function.result, true, Vec::new());
        }
        if limits.cancelled_or_expired() {
            return Err(PluginInvocationError::Callback {
                status: abi::RADIX_STATUS_CANCELLED,
                diagnostic: "native function call was cancelled before dispatch".to_owned(),
            });
        }
        let mut backing = Vec::with_capacity(arguments.len());
        let raw = arguments
            .iter()
            .zip(&function.arguments)
            .map(|(value, expected)| raw_argument(value, *expected, &mut backing))
            .collect::<Result<Vec<_>, _>>()?;
        let mut state = CallState::new(function.max_output_bytes, 1).with_limits(limits);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let output = state.result_builder();
        // SAFETY: descriptor admission validated the callback and every pointer
        // remains alive for this synchronous invocation.
        let status =
            unsafe { (function.scalar)(&context, raw.as_ptr(), raw.len() as u32, &output) };
        finish_function_output(self, function, state, status)
    }

    pub fn invoke_function_batch(
        &self,
        object_id: ObjectId,
        rows: &[Vec<Value>],
        limits: InvocationLimits,
    ) -> Result<Vec<Value>, PluginInvocationError> {
        let function = self
            .function(&object_id)
            .ok_or(PluginInvocationError::UnknownFunction(object_id))?;
        if rows.len() > u32::MAX as usize
            || rows.iter().any(|row| row.len() != function.arguments.len())
        {
            return Err(PluginInvocationError::Contract(
                "native function batch shape differs from descriptor",
            ));
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        // A strict batch containing NULL rows needs row selection/scatter.
        // Scalar execution is the correctness fallback until that optional
        // optimization is added; it preserves strict NULL short-circuiting.
        let Some(batch_callback) = function
            .batch
            .filter(|_| !function.strict || !rows.iter().flatten().any(Value::is_null))
        else {
            return rows
                .iter()
                .map(|row| self.invoke_scalar_function(object_id, row, limits))
                .collect();
        };
        if limits.cancelled_or_expired() {
            return Err(PluginInvocationError::Callback {
                status: abi::RADIX_STATUS_CANCELLED,
                diagnostic: "native function batch was cancelled before dispatch".to_owned(),
            });
        }
        let columns = function
            .arguments
            .iter()
            .enumerate()
            .map(|(index, expected)| {
                let values = rows.iter().map(|row| &row[index]).collect::<Vec<_>>();
                OwnedBatchColumn::new(self, *expected, &values)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_count = rows.len() as u32;
        let raw_columns = columns
            .iter()
            .map(|column| column.as_abi(row_count))
            .collect::<Vec<_>>();
        let batch = abi::RadixAbiBatchViewV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiBatchViewV1>(0),
            row_count,
            column_count: raw_columns.len() as u32,
            columns: if raw_columns.is_empty() {
                std::ptr::null()
            } else {
                raw_columns.as_ptr()
            },
        };
        // SAFETY: all descriptors and buffers are host-owned and stay alive
        // for the complete synchronous callback.
        unsafe { abi::validate_batch_columns(&batch) }.map_err(|_| {
            PluginInvocationError::Contract("host constructed an invalid native batch")
        })?;
        let maximum = function.max_output_bytes.saturating_mul(row_count);
        let mut state = CallState::new(maximum, row_count).with_limits(limits);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let output = state.result_builder();
        // SAFETY: descriptor admission validated the callback and all input,
        // context and result-builder pointers remain live for this call.
        let status = unsafe { batch_callback(&context, &batch, &output) };
        finish_batch_output(self, function, state, status, rows.len())
    }

    pub fn validate_external_value(&self, value: &Value) -> Result<(), PluginInvocationError> {
        let (registered, view) = external(self, value)?;
        let output = run_parse(registered.decode, view.payload(), registered.max_bytes)?;
        if output != view.payload() {
            return Err(PluginInvocationError::Contract(
                "canonical codec decode/encode did not preserve payload bytes",
            ));
        }
        Ok(())
    }

    pub fn parse_external_text(
        &self,
        type_ref: ExternalTypeRef,
        text: &str,
    ) -> Result<Value, PluginInvocationError> {
        let registered = self.external_type(&type_ref.type_object_id()).ok_or(
            PluginInvocationError::UnknownType(type_ref.type_object_id()),
        )?;
        if registered.codec_version != type_ref.codec_version() {
            return Err(PluginInvocationError::CodecVersion {
                expected: registered.codec_version,
                actual: type_ref.codec_version(),
            });
        }
        let callback = registered
            .text_input
            .ok_or(PluginInvocationError::MissingCapability("text input"))?;
        let payload = run_parse(callback, text.as_bytes(), registered.max_bytes)?;
        let value = Value::try_external(type_ref, payload)
            .map_err(|_| PluginInvocationError::PayloadLimit)?;
        self.validate_external_value(&value)?;
        Ok(value)
    }

    pub fn format_external_text(&self, value: &Value) -> Result<String, PluginInvocationError> {
        let (registered, view) = external(self, value)?;
        let callback = registered
            .text_output
            .ok_or(PluginInvocationError::MissingCapability("text output"))?;
        let bytes = run_codec(
            callback,
            &raw_value(view),
            abi::RADIX_MAX_EXTERNAL_VALUE_BYTES,
        )?;
        String::from_utf8(bytes)
            .map_err(|_| PluginInvocationError::Contract("text output is not UTF-8"))
    }

    pub fn parse_external_binary(
        &self,
        type_ref: ExternalTypeRef,
        bytes: &[u8],
    ) -> Result<Value, PluginInvocationError> {
        let registered = self.external_type(&type_ref.type_object_id()).ok_or(
            PluginInvocationError::UnknownType(type_ref.type_object_id()),
        )?;
        if registered.codec_version != type_ref.codec_version() {
            return Err(PluginInvocationError::CodecVersion {
                expected: registered.codec_version,
                actual: type_ref.codec_version(),
            });
        }
        let callback = registered.binary_input.unwrap_or(registered.decode);
        let payload = run_parse(callback, bytes, registered.max_bytes)?;
        let value = Value::try_external(type_ref, payload)
            .map_err(|_| PluginInvocationError::PayloadLimit)?;
        self.validate_external_value(&value)?;
        Ok(value)
    }

    pub fn format_external_binary(&self, value: &Value) -> Result<Vec<u8>, PluginInvocationError> {
        let (registered, view) = external(self, value)?;
        let callback = registered.binary_output.unwrap_or(registered.encode);
        run_codec(callback, &raw_value(view), registered.max_bytes)
    }

    pub fn external_equal(
        &self,
        left: &Value,
        right: &Value,
    ) -> Result<bool, PluginInvocationError> {
        let (registered, left, right) = matching_pair(self, left, right)?;
        let callback = registered
            .equality
            .ok_or(PluginInvocationError::MissingCapability("equality"))?;
        let mut state = CallState::new(1, 1);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let mut output = u8::MAX;
        // SAFETY: pointers are live for the synchronous callback.
        let status = unsafe { callback(&context, &left, &right, &mut output) };
        if status != abi::RADIX_STATUS_OK {
            return Err(state.callback_error(status));
        }
        match output {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PluginInvocationError::Contract(
                "equality callback returned a non-boolean byte",
            )),
        }
    }

    pub fn external_compare(
        &self,
        left: &Value,
        right: &Value,
    ) -> Result<Ordering, PluginInvocationError> {
        let (registered, left, right) = matching_pair(self, left, right)?;
        let callback = registered
            .ordering
            .ok_or(PluginInvocationError::MissingCapability("ordering"))?;
        let mut state = CallState::new(1, 1);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let mut output = i8::MIN;
        // SAFETY: pointers are live for the synchronous callback.
        let status = unsafe { callback(&context, &left, &right, &mut output) };
        if status != abi::RADIX_STATUS_OK {
            return Err(state.callback_error(status));
        }
        match output {
            -1 => Ok(Ordering::Less),
            0 => Ok(Ordering::Equal),
            1 => Ok(Ordering::Greater),
            _ => Err(PluginInvocationError::Contract(
                "ordering callback returned a value outside -1..=1",
            )),
        }
    }

    pub fn external_hash_components(
        &self,
        value: &Value,
    ) -> Result<Vec<HashComponent>, PluginInvocationError> {
        let (registered, view) = external(self, value)?;
        let callback = registered
            .hash
            .ok_or(PluginInvocationError::MissingCapability("hash"))?;
        let raw = raw_value(view);
        let mut state = CallState::new(1, 1);
        let diagnostics = state.diagnostic_sink();
        let context = state.context(&diagnostics);
        let sink = state.hash_sink();
        // SAFETY: pointers are live for the synchronous callback.
        let status = unsafe { callback(&context, &raw, &sink) };
        if status != abi::RADIX_STATUS_OK {
            return Err(state.callback_error(status));
        }
        if state.hash_components.is_empty() {
            return Err(PluginInvocationError::Contract(
                "hash callback emitted no semantic components",
            ));
        }
        Ok(state.hash_components)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RegisteredExternalType, RegisteredFunction, RegisteredOperatorClass, RegisteredPackage,
        RegisteredPlannerSupport,
    };
    use std::ptr;

    unsafe extern "C" fn parse_echo(
        _context: *const abi::RadixAbiCallContextV1,
        input: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let Some(write) = output.write else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let Some(finish) = output.finish else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let status = unsafe { write(output.handle, 0, 0, input) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { finish(output.handle) }
    }

    unsafe extern "C" fn codec_echo(
        _context: *const abi::RadixAbiCallContextV1,
        input: *const abi::RadixAbiValueV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(input) = (unsafe { input.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        unsafe { parse_echo(ptr::null(), input.borrowed_bytes, output) }
    }

    unsafe extern "C" fn equal(
        _context: *const abi::RadixAbiCallContextV1,
        left: *const abi::RadixAbiValueV1,
        right: *const abi::RadixAbiValueV1,
        output: *mut u8,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(left), Some(right), Some(output)) = (
            unsafe { left.as_ref() },
            unsafe { right.as_ref() },
            unsafe { output.as_mut() },
        ) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let left = unsafe {
            slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
        };
        let right = unsafe {
            slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
        };
        *output = u8::from(left == right);
        abi::RADIX_STATUS_OK
    }

    unsafe extern "C" fn compare(
        _context: *const abi::RadixAbiCallContextV1,
        left: *const abi::RadixAbiValueV1,
        right: *const abi::RadixAbiValueV1,
        output: *mut i8,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(left), Some(right), Some(output)) = (
            unsafe { left.as_ref() },
            unsafe { right.as_ref() },
            unsafe { output.as_mut() },
        ) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let left = unsafe {
            slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
        };
        let right = unsafe {
            slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
        };
        *output = match left.cmp(right) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        };
        abi::RADIX_STATUS_OK
    }

    unsafe extern "C" fn hash(
        _context: *const abi::RadixAbiCallContextV1,
        value: *const abi::RadixAbiValueV1,
        sink: *const abi::RadixAbiHashSinkV1,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(value), Some(sink)) = (unsafe { value.as_ref() }, unsafe { sink.as_ref() })
        else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let Some(append) = sink.append else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        unsafe {
            append(
                sink.handle,
                abi::RADIX_HASH_COMPONENT_BYTES,
                0,
                value.borrowed_bytes,
            )
        }
    }

    unsafe extern "C" fn increment_scalar(
        context: *const abi::RadixAbiCallContextV1,
        arguments: *const abi::RadixAbiValueV1,
        argument_count: u32,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(context), Some(output)) =
            (unsafe { context.as_ref() }, unsafe { output.as_ref() })
        else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        if argument_count != 1 || arguments.is_null() {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        }
        if let Some(check) = context.check_cancelled {
            let status = unsafe { check(context.handle) };
            if status != abi::RADIX_STATUS_OK {
                return status;
            }
        }
        let argument = unsafe { &*arguments };
        let value = i64::from_le_bytes(argument.inline_bytes[..8].try_into().unwrap()) + 1;
        let bytes = value.to_le_bytes();
        let Some(write) = output.write else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let status = unsafe {
            write(
                output.handle,
                0,
                0,
                abi::RadixAbiSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len() as u32,
                    reserved: 0,
                },
            )
        };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn unused_key_encoder(
        _context: *const abi::RadixAbiCallContextV1,
        _value: *const abi::RadixAbiValueV1,
        _output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        abi::RADIX_STATUS_INTERNAL_ERROR
    }

    unsafe fn write_plan_item(
        output: &abi::RadixAbiResultBuilderV1,
        bytes: &[u8],
    ) -> abi::RadixAbiStatusV1 {
        unsafe {
            output.write.unwrap()(
                output.handle,
                0,
                0,
                abi::RadixAbiSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len() as u32,
                    reserved: 0,
                },
            )
        }
    }

    unsafe fn write_integer_span(
        output: &abi::RadixAbiResultBuilderV1,
        start: i64,
        end: i64,
    ) -> abi::RadixAbiStatusV1 {
        let mut span = Vec::with_capacity(28);
        span.extend_from_slice(&[1, 0, 0, 0]);
        span.extend_from_slice(&8_u32.to_le_bytes());
        span.extend_from_slice(&8_u32.to_le_bytes());
        span.extend_from_slice(&start.to_le_bytes());
        span.extend_from_slice(&end.to_le_bytes());
        unsafe { write_plan_item(output, &span) }
    }

    unsafe fn write_estimate(
        output: &abi::RadixAbiResultBuilderV1,
        rows: u64,
    ) -> abi::RadixAbiStatusV1 {
        let mut estimate = Vec::with_capacity(20);
        estimate.extend_from_slice(&[2, 0, 0, 0]);
        estimate.extend_from_slice(&rows.to_le_bytes());
        estimate.extend_from_slice(&7_u32.to_le_bytes());
        estimate.extend_from_slice(&0_u32.to_le_bytes());
        unsafe { write_plan_item(output, &estimate) }
    }

    unsafe extern "C" fn overlapping_plan(
        _context: *const abi::RadixAbiCallContextV1,
        _predicate: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        for (start, end) in [(3, 7), (1, 5), (8, 9)] {
            let status = unsafe { write_integer_span(output, start, end) };
            if status != abi::RADIX_STATUS_OK {
                return status;
            }
        }
        let status = unsafe { write_estimate(output, 4) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn unknown_estimate_plan(
        _context: *const abi::RadixAbiCallContextV1,
        _predicate: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let status = unsafe { write_integer_span(output, 1, 2) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn malformed_plan(
        _context: *const abi::RadixAbiCallContextV1,
        _predicate: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let status = unsafe { write_plan_item(output, &[99, 0, 0, 0]) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn exploding_plan(
        _context: *const abi::RadixAbiCallContextV1,
        _predicate: abi::RadixAbiSliceV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        for value in 0..=2 {
            let status = unsafe { write_integer_span(output, value, value) };
            if status != abi::RADIX_STATUS_OK {
                return status;
            }
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn unsupported_plan(
        _context: *const abi::RadixAbiCallContextV1,
        _predicate: abi::RadixAbiSliceV1,
        _output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        abi::RADIX_STATUS_DOMAIN_ERROR
    }

    fn planner_registry(
        callback: abi::RadixAbiPlannerSupportFnV1,
        max_spans: u32,
    ) -> (PluginRegistry, ObjectId) {
        let package_id = [0x71; 16];
        let function_id = [0x72; 16];
        let class_id = [0x73; 16];
        let support_id = [0x74; 16];
        let integer = RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_INTEGER);
        let function = RegisteredFunction {
            package_id,
            object_id: function_id,
            local_id: "predicate".to_owned(),
            display_name: "predicate".to_owned(),
            semantic_revision: 2,
            arguments: vec![integer, integer],
            result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BOOLEAN),
            volatility: abi::RADIX_VOLATILITY_IMMUTABLE,
            cancellation: abi::RADIX_CANCELLATION_BOUNDED,
            strict: true,
            parallel_safe: true,
            cost: 1,
            max_output_bytes: 1,
            scalar: increment_scalar,
            batch: None,
        };
        let operator_class = RegisteredOperatorClass {
            package_id,
            object_id: class_id,
            local_id: "integer_btree".to_owned(),
            semantic_revision: 3,
            access_method: abi::RADIX_ACCESS_METHOD_BTREE,
            input_type: integer,
            key_type: integer,
            key_codec_revision: 4,
            strategies: vec![],
            supports: vec![],
            fingerprint: [0x75; 32],
            encode_key: unused_key_encoder,
        };
        let support = RegisteredPlannerSupport {
            package_id,
            object_id: support_id,
            local_id: "predicate_support".to_owned(),
            semantic_revision: 5,
            max_spans,
            max_output_bytes: 4096,
            recheck_policy: abi::RADIX_RECHECK_ALWAYS,
            target_function_id: Some(function_id),
            target_operator_class_id: Some(class_id),
            fingerprint: [0x76; 32],
            callback,
        };
        let package = RegisteredPackage::for_test(package_id, "planner", "1.0.0", [0x77; 32]);
        (
            PluginRegistry::from_test_objects(
                [package],
                [],
                [function],
                [],
                [operator_class],
                [support],
            ),
            support_id,
        )
    }

    unsafe extern "C" fn increment_batch(
        _context: *const abi::RadixAbiCallContextV1,
        input: *const abi::RadixAbiBatchViewV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let (Some(input), Some(output)) = (unsafe { input.as_ref() }, unsafe { output.as_ref() })
        else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        if input.column_count != 1 || input.columns.is_null() {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        }
        let column = unsafe { &*input.columns };
        let data = unsafe { slice::from_raw_parts(column.data.ptr, column.data.len as usize) };
        for row in 0..input.row_count as usize {
            let start = row * column.stride as usize;
            let value = i64::from_le_bytes(data[start..start + 8].try_into().unwrap()) + 1;
            let bytes = value.to_le_bytes();
            let status = unsafe {
                output.write.unwrap()(
                    output.handle,
                    0,
                    0,
                    abi::RadixAbiSliceV1 {
                        ptr: bytes.as_ptr(),
                        len: bytes.len() as u32,
                        reserved: 0,
                    },
                )
            };
            if status != abi::RADIX_STATUS_OK {
                return status;
            }
        }
        unsafe { output.finish.unwrap()(output.handle) }
    }

    unsafe extern "C" fn partial_batch_error(
        _context: *const abi::RadixAbiCallContextV1,
        _input: *const abi::RadixAbiBatchViewV1,
        output: *const abi::RadixAbiResultBuilderV1,
    ) -> abi::RadixAbiStatusV1 {
        let Some(output) = (unsafe { output.as_ref() }) else {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        };
        let bytes = 42_i64.to_le_bytes();
        let _ = unsafe {
            output.write.unwrap()(
                output.handle,
                0,
                0,
                abi::RadixAbiSliceV1 {
                    ptr: bytes.as_ptr(),
                    len: bytes.len() as u32,
                    reserved: 0,
                },
            )
        };
        abi::RADIX_STATUS_INTERNAL_ERROR
    }

    fn registry() -> (PluginRegistry, ExternalTypeRef) {
        let package_id = [0x51; 16];
        let object_id = [0x52; 16];
        let external = RegisteredExternalType {
            package_id,
            object_id,
            local_id: "sample".to_owned(),
            display_name: "sample".to_owned(),
            codec_version: 3,
            semantic_revision: 1,
            storage_kind: abi::RADIX_EXTERNAL_STORAGE_VARIABLE,
            fixed_bytes: 0,
            max_bytes: 1024,
            capabilities: abi::RADIX_TYPE_CAP_EQUALITY
                | abi::RADIX_TYPE_CAP_HASH
                | abi::RADIX_TYPE_CAP_ORDERING
                | abi::RADIX_TYPE_CAP_TEXT_INPUT
                | abi::RADIX_TYPE_CAP_TEXT_OUTPUT
                | abi::RADIX_TYPE_CAP_BINARY_INPUT
                | abi::RADIX_TYPE_CAP_BINARY_OUTPUT,
            codec_fingerprint: [7; 32],
            encode: codec_echo,
            decode: parse_echo,
            equality: Some(equal),
            hash: Some(hash),
            ordering: Some(compare),
            text_input: Some(parse_echo),
            text_output: Some(codec_echo),
            binary_input: Some(parse_echo),
            binary_output: Some(codec_echo),
        };
        let package = RegisteredPackage::for_test(package_id, "fixture", "1.0.0", [8; 32]);
        (
            PluginRegistry::from_test_objects([package], [external], [], [], [], []),
            ExternalTypeRef::new(object_id, 3).unwrap(),
        )
    }

    fn function_registry(
        strict: bool,
        batch: Option<abi::RadixAbiBatchFnV1>,
    ) -> (PluginRegistry, ObjectId) {
        let package_id = [0x61; 16];
        let object_id = [0x62; 16];
        let function = RegisteredFunction {
            package_id,
            object_id,
            local_id: "increment".to_owned(),
            display_name: "increment".to_owned(),
            semantic_revision: 1,
            arguments: vec![RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_INTEGER)],
            result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_INTEGER),
            volatility: abi::RADIX_VOLATILITY_IMMUTABLE,
            cancellation: abi::RADIX_CANCELLATION_BOUNDED,
            strict,
            parallel_safe: true,
            cost: 1,
            max_output_bytes: 8,
            scalar: increment_scalar,
            batch,
        };
        let package = RegisteredPackage::for_test(package_id, "functions", "1.0.0", [9; 32]);
        (
            PluginRegistry::from_test_objects([package], [], [function], [], [], []),
            object_id,
        )
    }

    #[test]
    fn external_callbacks_own_semantics_and_io() {
        let (registry, type_ref) = registry();
        let left = registry.parse_external_text(type_ref, "alpha").unwrap();
        let same = registry.parse_external_binary(type_ref, b"alpha").unwrap();
        let greater = Value::try_external(type_ref, b"beta").unwrap();

        registry.validate_external_value(&left).unwrap();
        assert_eq!(registry.format_external_text(&left).unwrap(), "alpha");
        assert_eq!(registry.format_external_binary(&left).unwrap(), b"alpha");
        assert!(registry.external_equal(&left, &same).unwrap());
        assert_eq!(
            registry.external_compare(&left, &greater).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            registry.external_hash_components(&left).unwrap(),
            vec![HashComponent {
                kind: abi::RADIX_HASH_COMPONENT_BYTES,
                bytes: b"alpha".to_vec(),
            }]
        );
    }

    #[test]
    fn external_callbacks_fail_closed_on_identity_and_codec_mismatch() {
        let (registry, type_ref) = registry();
        let valid = Value::try_external(type_ref, b"alpha").unwrap();
        let other =
            Value::try_external(ExternalTypeRef::new([0x53; 16], 3).unwrap(), b"alpha").unwrap();
        let stale =
            Value::try_external(ExternalTypeRef::new([0x52; 16], 2).unwrap(), b"alpha").unwrap();

        assert!(matches!(
            registry.external_equal(&valid, &other),
            Err(PluginInvocationError::UnknownType(_))
        ));
        assert!(matches!(
            registry.validate_external_value(&stale),
            Err(PluginInvocationError::CodecVersion { .. })
        ));
    }

    #[test]
    fn scalar_and_batch_function_paths_have_identical_results() {
        let (registry, object_id) = function_registry(false, Some(increment_batch));
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(41)],
            vec![Value::Integer(-2)],
        ];
        let scalar = rows
            .iter()
            .map(|row| registry.invoke_scalar_function(object_id, row, InvocationLimits::default()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let batch = registry
            .invoke_function_batch(object_id, &rows, InvocationLimits::default())
            .unwrap();
        assert_eq!(batch, scalar);
        assert_eq!(
            batch,
            vec![Value::Integer(2), Value::Integer(42), Value::Integer(-1)]
        );
    }

    #[test]
    fn strict_null_uses_scalar_fallback_and_short_circuits_callback() {
        let (registry, object_id) = function_registry(true, Some(increment_batch));
        let rows = vec![
            vec![Value::Integer(1)],
            vec![Value::Null(DataType::Integer)],
        ];
        assert_eq!(
            registry
                .invoke_function_batch(object_id, &rows, InvocationLimits::default())
                .unwrap(),
            vec![Value::Integer(2), Value::Null(DataType::Integer)]
        );
    }

    #[test]
    fn cancellation_and_batch_failure_publish_no_partial_results() {
        fn cancelled() -> bool {
            true
        }
        let (registry, object_id) = function_registry(false, Some(increment_batch));
        assert!(matches!(
            registry.invoke_function_batch(
                object_id,
                &[vec![Value::Integer(1)]],
                InvocationLimits {
                    cancel_check: Some(cancelled),
                    deadline_unix_ns: u64::MAX,
                },
            ),
            Err(PluginInvocationError::Callback {
                status: abi::RADIX_STATUS_CANCELLED,
                ..
            })
        ));

        let (registry, object_id) = function_registry(false, Some(partial_batch_error));
        assert!(matches!(
            registry.invoke_function_batch(
                object_id,
                &[vec![Value::Integer(1)], vec![Value::Integer(2)]],
                InvocationLimits::default(),
            ),
            Err(PluginInvocationError::Callback {
                status: abi::RADIX_STATUS_INTERNAL_ERROR,
                ..
            })
        ));
    }

    #[test]
    fn planner_support_canonicalizes_overlaps_and_carries_dependency_identity() {
        let (registry, support_id) = planner_registry(overlapping_plan, 8);
        let outcome = registry
            .invoke_planner_support(
                support_id,
                &[
                    NormalizedPredicateArgument::IndexedColumn,
                    NormalizedPredicateArgument::Constant(&Value::Integer(5)),
                ],
                InvocationLimits::default(),
            )
            .unwrap();
        let PlannerSupportOutcome::Plan(plan) = outcome else {
            panic!("valid candidate plan fell back")
        };
        assert_eq!(
            plan.spans,
            vec![
                CandidateKeySpan {
                    start: Value::Integer(1),
                    end: Value::Integer(7),
                },
                CandidateKeySpan {
                    start: Value::Integer(8),
                    end: Value::Integer(9),
                },
            ]
        );
        assert!(plan.requires_recheck);
        assert_eq!(plan.estimated_rows, 4);
        assert_eq!(plan.identity.registry_generation, registry.generation());
        assert_eq!(plan.identity.support_semantic_revision, 5);
        assert_eq!(plan.identity.function_semantic_revision, 2);
        assert_eq!(plan.identity.operator_class_semantic_revision, 3);
        assert_eq!(plan.identity.key_codec_revision, 4);
    }

    #[test]
    fn planner_support_falls_back_for_unknown_estimate_and_range_explosion() {
        for (callback, max_spans) in [
            (unknown_estimate_plan as abi::RadixAbiPlannerSupportFnV1, 2),
            (exploding_plan as abi::RadixAbiPlannerSupportFnV1, 2),
            (unsupported_plan as abi::RadixAbiPlannerSupportFnV1, 2),
        ] {
            let (registry, support_id) = planner_registry(callback, max_spans);
            assert!(matches!(
                registry
                    .invoke_planner_support(
                        support_id,
                        &[
                            NormalizedPredicateArgument::IndexedColumn,
                            NormalizedPredicateArgument::Constant(&Value::Integer(5)),
                        ],
                        InvocationLimits::default(),
                    )
                    .unwrap(),
                PlannerSupportOutcome::Fallback { .. }
            ));
        }
    }

    #[test]
    fn malformed_planner_output_fails_closed() {
        let (registry, support_id) = planner_registry(malformed_plan, 2);
        assert!(matches!(
            registry.invoke_planner_support(
                support_id,
                &[
                    NormalizedPredicateArgument::IndexedColumn,
                    NormalizedPredicateArgument::Constant(&Value::Integer(5)),
                ],
                InvocationLimits::default(),
            ),
            Err(PluginInvocationError::Contract(_))
        ));
    }
}
