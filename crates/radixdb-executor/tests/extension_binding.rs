use std::{
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

use radixdb_catalog::{
    CatalogDataType, CatalogEdge, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject,
    CatalogPayload, EdgeKind, FunctionPayload, ObjectId, ObjectKind, ProceduralSource,
    ResourcePolicy, RoutineDefinition, RoutineResult, SecurityMode, Volatility,
};
use radixdb_core::{DataType, ExternalTypeRef, Operator, Value};
use radixdb_executor::{ExecutionContext, Executor};
use radixdb_plugin_abi as abi;
use radixdb_plugin_host::{
    derive_object_id, DatabasePluginAdmission, PluginRegistry, RegisteredBinding,
    RegisteredExternalType, RegisteredFunction, RegisteredOperator, RegisteredOperatorClass,
    RegisteredPackage, RegisteredPlannerSupport, RegisteredTypeRef, RequirementIssue,
};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::Engine;
use radixdb_storage::Config;

const PACKAGE_ID: [u8; 16] = [0x71; 16];
const FINGERPRINT: [u8; 32] = [0xa5; 32];

fn registry() -> Arc<PluginRegistry> {
    Arc::new(PluginRegistry::from_test_packages([
        RegisteredPackage::for_test(PACKAGE_ID, "sample", "1.2.3", FINGERPRINT),
    ]))
}

unsafe extern "C" fn codec_parse_echo(
    _context: *const abi::RadixAbiCallContextV1,
    input: abi::RadixAbiSliceV1,
    output: *const abi::RadixAbiResultBuilderV1,
) -> abi::RadixAbiStatusV1 {
    let Some(output) = (unsafe { output.as_ref() }) else {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    };
    let (Some(write), Some(finish)) = (output.write, output.finish) else {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    };
    let status = unsafe { write(output.handle, 0, 0, input) };
    if status != abi::RADIX_STATUS_OK {
        return status;
    }
    unsafe { finish(output.handle) }
}

unsafe extern "C" fn codec_encode_echo(
    context: *const abi::RadixAbiCallContextV1,
    input: *const abi::RadixAbiValueV1,
    output: *const abi::RadixAbiResultBuilderV1,
) -> abi::RadixAbiStatusV1 {
    let Some(input) = (unsafe { input.as_ref() }) else {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    };
    unsafe { codec_parse_echo(context, input.borrowed_bytes, output) }
}

unsafe extern "C" fn semantic_equal(
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
        std::slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
    };
    let right = unsafe {
        std::slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
    };
    *output = u8::from(left == right);
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn semantic_compare(
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
        std::slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
    };
    let right = unsafe {
        std::slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
    };
    *output = match left.cmp(right) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn semantic_hash(
    _context: *const abi::RadixAbiCallContextV1,
    value: *const abi::RadixAbiValueV1,
    sink: *const abi::RadixAbiHashSinkV1,
) -> abi::RadixAbiStatusV1 {
    let (Some(value), Some(sink)) = (unsafe { value.as_ref() }, unsafe { sink.as_ref() }) else {
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

unsafe fn comparison_scalar(
    arguments: *const abi::RadixAbiValueV1,
    argument_count: u32,
    output: *const abi::RadixAbiResultBuilderV1,
    predicate: fn(std::cmp::Ordering) -> bool,
) -> abi::RadixAbiStatusV1 {
    let Some(output) = (unsafe { output.as_ref() }) else {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    };
    if argument_count != 2 || arguments.is_null() {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    }
    let left = unsafe { &*arguments };
    let right = unsafe { &*arguments.add(1) };
    let left = unsafe {
        std::slice::from_raw_parts(left.borrowed_bytes.ptr, left.borrowed_bytes.len as usize)
    };
    let right = unsafe {
        std::slice::from_raw_parts(right.borrowed_bytes.ptr, right.borrowed_bytes.len as usize)
    };
    let bytes = [u8::from(predicate(left.cmp(right)))];
    let status = unsafe {
        output.write.unwrap()(
            output.handle,
            0,
            0,
            abi::RadixAbiSliceV1 {
                ptr: bytes.as_ptr(),
                len: 1,
                reserved: 0,
            },
        )
    };
    if status != abi::RADIX_STATUS_OK {
        return status;
    }
    unsafe { output.finish.unwrap()(output.handle) }
}

macro_rules! comparison_callback {
    ($name:ident, $predicate:expr) => {
        unsafe extern "C" fn $name(
            _context: *const abi::RadixAbiCallContextV1,
            arguments: *const abi::RadixAbiValueV1,
            argument_count: u32,
            output: *const abi::RadixAbiResultBuilderV1,
        ) -> abi::RadixAbiStatusV1 {
            unsafe { comparison_scalar(arguments, argument_count, output, $predicate) }
        }
    };
}

comparison_callback!(point_lt_scalar, |order| order == std::cmp::Ordering::Less);
comparison_callback!(point_le_scalar, |order| order
    != std::cmp::Ordering::Greater);
comparison_callback!(point_eq_scalar, |order| order == std::cmp::Ordering::Equal);
comparison_callback!(point_ge_scalar, |order| order != std::cmp::Ordering::Less);
comparison_callback!(point_gt_scalar, |order| order
    == std::cmp::Ordering::Greater);

unsafe extern "C" fn increment_scalar(
    context: *const abi::RadixAbiCallContextV1,
    arguments: *const abi::RadixAbiValueV1,
    argument_count: u32,
    output: *const abi::RadixAbiResultBuilderV1,
) -> abi::RadixAbiStatusV1 {
    let (Some(context), Some(output)) = (unsafe { context.as_ref() }, unsafe { output.as_ref() })
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
    unsafe { output.finish.unwrap()(output.handle) }
}

unsafe fn write_planner_item(
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

unsafe extern "C" fn point_eq_planner_support(
    _context: *const abi::RadixAbiCallContextV1,
    predicate: abi::RadixAbiSliceV1,
    output: *const abi::RadixAbiResultBuilderV1,
) -> abi::RadixAbiStatusV1 {
    let Some(output) = (unsafe { output.as_ref() }) else {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    };
    if predicate.ptr.is_null() {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    }
    let predicate = unsafe { std::slice::from_raw_parts(predicate.ptr, predicate.len as usize) };
    if predicate.len() < 40 || &predicate[..4] != b"RPN1" {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    }
    let argument_count = u16::from_le_bytes(predicate[38..40].try_into().unwrap()) as usize;
    if argument_count != 2 {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    }
    let mut cursor = 40usize;
    let mut constant = None;
    for argument in 0..argument_count {
        if cursor + 32 > predicate.len() {
            return abi::RADIX_STATUS_INVALID_ARGUMENT;
        }
        let kind = predicate[cursor];
        let len =
            u32::from_le_bytes(predicate[cursor + 28..cursor + 32].try_into().unwrap()) as usize;
        let end = match cursor
            .checked_add(32)
            .and_then(|value| value.checked_add(len))
        {
            Some(value) if value <= predicate.len() => value,
            _ => return abi::RADIX_STATUS_INVALID_ARGUMENT,
        };
        if argument == 1 && kind == 2 {
            constant = Some(&predicate[cursor + 32..end]);
        }
        cursor = end;
    }
    if cursor != predicate.len() || constant.is_none() {
        return abi::RADIX_STATUS_INVALID_ARGUMENT;
    }

    // Deliberately overlapping, approximate ranges. The host must merge them
    // and the executor must evaluate point_eq for every candidate.
    for (start, end) in [(b"10".as_slice(), b"25".as_slice()), (b"20", b"30")] {
        let mut span = Vec::with_capacity(12 + start.len() + end.len());
        span.extend_from_slice(&[1, 0, 0, 0]);
        span.extend_from_slice(&(start.len() as u32).to_le_bytes());
        span.extend_from_slice(&(end.len() as u32).to_le_bytes());
        span.extend_from_slice(start);
        span.extend_from_slice(end);
        let status = unsafe { write_planner_item(output, &span) };
        if status != abi::RADIX_STATUS_OK {
            return status;
        }
    }
    let mut estimate = Vec::with_capacity(20);
    estimate.extend_from_slice(&[2, 0, 0, 0]);
    estimate.extend_from_slice(&2_u64.to_le_bytes());
    estimate.extend_from_slice(&1_u32.to_le_bytes());
    estimate.extend_from_slice(&0_u32.to_le_bytes());
    let status = unsafe { write_planner_item(output, &estimate) };
    if status != abi::RADIX_STATUS_OK {
        return status;
    }
    unsafe { output.finish.unwrap()(output.handle) }
}

fn native_registry() -> (Arc<PluginRegistry>, ObjectId) {
    let object_id = derive_object_id(PACKAGE_ID, "increment").unwrap();
    let function = RegisteredFunction {
        package_id: PACKAGE_ID,
        object_id,
        local_id: "increment".to_owned(),
        display_name: "increment".to_owned(),
        semantic_revision: 3,
        arguments: vec![RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_INTEGER)],
        result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_INTEGER),
        volatility: abi::RADIX_VOLATILITY_IMMUTABLE,
        cancellation: abi::RADIX_CANCELLATION_BOUNDED,
        strict: true,
        parallel_safe: true,
        cost: 4,
        max_output_bytes: 8,
        scalar: increment_scalar,
        batch: None,
    };
    let package = RegisteredPackage::for_test(PACKAGE_ID, "sample", "1.2.3", FINGERPRINT);
    (
        Arc::new(PluginRegistry::from_test_objects(
            [package],
            [],
            [function],
            [],
            [],
            [],
        )),
        ObjectId::from_user_bytes(object_id).unwrap(),
    )
}

fn external_registry() -> (Arc<PluginRegistry>, ExternalTypeRef) {
    external_registry_with_version("1.2.3")
}

fn external_registry_with_version(version: &str) -> (Arc<PluginRegistry>, ExternalTypeRef) {
    let object_id = derive_object_id(PACKAGE_ID, "point").unwrap();
    let external_type = RegisteredExternalType {
        package_id: PACKAGE_ID,
        object_id,
        local_id: "point".to_owned(),
        display_name: "Point".to_owned(),
        codec_version: 1,
        semantic_revision: 1,
        storage_kind: abi::RADIX_EXTERNAL_STORAGE_VARIABLE,
        fixed_bytes: 0,
        max_bytes: 1024,
        capabilities: abi::RADIX_TYPE_CAP_EQUALITY
            | abi::RADIX_TYPE_CAP_ORDERING
            | abi::RADIX_TYPE_CAP_TEXT_INPUT
            | abi::RADIX_TYPE_CAP_TEXT_OUTPUT
            | abi::RADIX_TYPE_CAP_BINARY_INPUT
            | abi::RADIX_TYPE_CAP_BINARY_OUTPUT,
        codec_fingerprint: [0x5a; 32],
        encode: codec_encode_echo,
        decode: codec_parse_echo,
        equality: Some(semantic_equal),
        hash: None,
        ordering: Some(semantic_compare),
        text_input: Some(codec_parse_echo),
        text_output: Some(codec_encode_echo),
        binary_input: Some(codec_parse_echo),
        binary_output: Some(codec_encode_echo),
    };
    let package = RegisteredPackage::for_test(PACKAGE_ID, "sample", version, FINGERPRINT);
    (
        Arc::new(PluginRegistry::from_test_objects(
            [package],
            [external_type],
            [],
            [],
            [],
            [],
        )),
        ExternalTypeRef::new(object_id, 1).unwrap(),
    )
}

fn indexed_external_registry() -> (Arc<PluginRegistry>, ExternalTypeRef) {
    indexed_external_registry_with_class_revision(1)
}

fn indexed_external_registry_with_class_revision(
    class_revision: u32,
) -> (Arc<PluginRegistry>, ExternalTypeRef) {
    let type_id = derive_object_id(PACKAGE_ID, "point").unwrap();
    let point = RegisteredTypeRef::External {
        object_id: type_id,
        codec_version: 1,
    };
    let external_type = RegisteredExternalType {
        package_id: PACKAGE_ID,
        object_id: type_id,
        local_id: "point".to_owned(),
        display_name: "Point".to_owned(),
        codec_version: 1,
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
        codec_fingerprint: [0x5a; 32],
        encode: codec_encode_echo,
        decode: codec_parse_echo,
        equality: Some(semantic_equal),
        hash: Some(semantic_hash),
        ordering: Some(semantic_compare),
        text_input: Some(codec_parse_echo),
        text_output: Some(codec_encode_echo),
        binary_input: Some(codec_parse_echo),
        binary_output: Some(codec_encode_echo),
    };
    let specs: [(&str, &str, &str, abi::RadixAbiScalarFnV1); 5] = [
        ("point_lt", "point_lt_operator", "<", point_lt_scalar),
        ("point_le", "point_le_operator", "<=", point_le_scalar),
        ("point_eq", "point_eq_operator", "=", point_eq_scalar),
        ("point_ge", "point_ge_operator", ">=", point_ge_scalar),
        ("point_gt", "point_gt_operator", ">", point_gt_scalar),
    ];
    let functions = specs
        .iter()
        .map(|(function, _, _, callback)| RegisteredFunction {
            package_id: PACKAGE_ID,
            object_id: derive_object_id(PACKAGE_ID, function).unwrap(),
            local_id: (*function).to_owned(),
            display_name: (*function).to_owned(),
            semantic_revision: 1,
            arguments: vec![point, point],
            result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BOOLEAN),
            volatility: abi::RADIX_VOLATILITY_IMMUTABLE,
            cancellation: abi::RADIX_CANCELLATION_BOUNDED,
            strict: true,
            parallel_safe: true,
            cost: 1,
            max_output_bytes: 1,
            scalar: *callback,
            batch: None,
        })
        .collect::<Vec<_>>();
    let mut operators = specs
        .iter()
        .map(|(function, operator, symbol, _)| RegisteredOperator {
            package_id: PACKAGE_ID,
            object_id: derive_object_id(PACKAGE_ID, operator).unwrap(),
            local_id: (*operator).to_owned(),
            symbol: (*symbol).to_owned(),
            semantic_revision: 1,
            left: Some(point),
            right: point,
            result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BOOLEAN),
            function_id: derive_object_id(PACKAGE_ID, function).unwrap(),
        })
        .collect::<Vec<_>>();
    operators.push(RegisteredOperator {
        package_id: PACKAGE_ID,
        object_id: derive_object_id(PACKAGE_ID, "point_overlap_operator").unwrap(),
        local_id: "point_overlap_operator".to_owned(),
        symbol: "&&".to_owned(),
        semantic_revision: 1,
        left: Some(point),
        right: point,
        result: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BOOLEAN),
        function_id: derive_object_id(PACKAGE_ID, "point_eq").unwrap(),
    });
    let strategy_ids = specs
        .iter()
        .enumerate()
        .map(|(index, (_, operator, _, _))| RegisteredBinding {
            slot: (index + 1) as u16,
            object_id: derive_object_id(PACKAGE_ID, operator).unwrap(),
        })
        .collect::<Vec<_>>();
    let planner_support_id = derive_object_id(PACKAGE_ID, "point_eq_support").unwrap();
    let classes = [
        RegisteredOperatorClass {
            package_id: PACKAGE_ID,
            object_id: derive_object_id(PACKAGE_ID, "point_btree").unwrap(),
            local_id: "point_btree".to_owned(),
            semantic_revision: class_revision,
            access_method: abi::RADIX_ACCESS_METHOD_BTREE,
            input_type: point,
            key_type: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BYTES),
            key_codec_revision: 1,
            strategies: strategy_ids,
            supports: vec![RegisteredBinding {
                slot: 1,
                object_id: planner_support_id,
            }],
            fingerprint: [0xb1; 32],
            encode_key: codec_encode_echo,
        },
        RegisteredOperatorClass {
            package_id: PACKAGE_ID,
            object_id: derive_object_id(PACKAGE_ID, "point_hash").unwrap(),
            local_id: "point_hash".to_owned(),
            semantic_revision: class_revision,
            access_method: abi::RADIX_ACCESS_METHOD_HASH,
            input_type: point,
            key_type: RegisteredTypeRef::Builtin(abi::RADIX_BUILTIN_BYTES),
            key_codec_revision: 1,
            strategies: vec![RegisteredBinding {
                slot: 1,
                object_id: derive_object_id(PACKAGE_ID, "point_eq_operator").unwrap(),
            }],
            supports: vec![],
            fingerprint: [0xb2; 32],
            encode_key: codec_encode_echo,
        },
    ];
    let planner_support = RegisteredPlannerSupport {
        package_id: PACKAGE_ID,
        object_id: planner_support_id,
        local_id: "point_eq_support".to_owned(),
        semantic_revision: 1,
        max_spans: 4,
        max_output_bytes: 1024,
        recheck_policy: abi::RADIX_RECHECK_ALWAYS,
        target_function_id: Some(derive_object_id(PACKAGE_ID, "point_eq").unwrap()),
        target_operator_class_id: Some(derive_object_id(PACKAGE_ID, "point_btree").unwrap()),
        fingerprint: [0xc1; 32],
        callback: point_eq_planner_support,
    };
    let package = RegisteredPackage::for_test(PACKAGE_ID, "sample", "1.2.3", FINGERPRINT);
    (
        Arc::new(PluginRegistry::from_test_objects(
            [package],
            [external_type],
            functions,
            operators,
            classes,
            [planner_support],
        )),
        ExternalTypeRef::new(type_id, 1).unwrap(),
    )
}

fn opened_engine(config: Config) -> Arc<MVCCEngine> {
    let engine = Arc::new(MVCCEngine::new(config));
    engine.install_catalog_runtime_binder(
        radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(
            radixdb_executor::bind_runtime_catalog,
        ),
    );
    engine.open_engine().unwrap();
    engine
}

fn opened_engine_with_registry(config: Config, registry: Arc<PluginRegistry>) -> Arc<MVCCEngine> {
    let engine = Arc::new(MVCCEngine::new(config));
    engine
        .install_catalog_runtime_binder(radixdb_executor::plugin_catalog_runtime_binder(registry));
    engine.open_engine().unwrap();
    engine
}

fn scalar_integer(executor: &Executor, sql: &str) -> i64 {
    let mut result = executor.execute(sql).unwrap();
    assert!(result.next());
    let value = result.row().get(0).and_then(Value::as_int64).unwrap();
    assert!(!result.next());
    value
}

fn assert_external_runtime_index_metadata(engine: &MVCCEngine) {
    let btree = engine.get_index("btree_values", "btree_values_p").unwrap();
    let btree_encoder = btree
        .prepared_key_encoder()
        .expect("external B-tree index must retain its prepared encoder");
    assert_eq!(
        btree_encoder.operator_class_id(),
        derive_object_id(PACKAGE_ID, "point_btree").unwrap()
    );
    assert_eq!(btree_encoder.semantic_revision(), 1);
    assert_eq!(btree_encoder.key_codec_revision(), 1);

    let hash = engine.get_index("hash_values", "hash_values_p").unwrap();
    let hash_encoder = hash
        .prepared_key_encoder()
        .expect("external hash index must retain its prepared encoder");
    assert_eq!(
        hash_encoder.operator_class_id(),
        derive_object_id(PACKAGE_ID, "point_hash").unwrap()
    );
    assert_eq!(hash_encoder.semantic_revision(), 1);
    assert_eq!(hash_encoder.key_codec_revision(), 1);
}

fn bind_indexed_external_catalog(executor: &Executor) {
    executor
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();
    executor
        .execute("CREATE TYPE public.point FROM EXTENSION sample AS 'point'")
        .unwrap();
    for name in ["point_lt", "point_le", "point_eq", "point_ge", "point_gt"] {
        executor
            .execute(&format!(
                "CREATE FUNCTION {name}(lhs public.point NOT NULL, rhs public.point NOT NULL) \
                 RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE FROM EXTENSION sample AS '{name}'"
            ))
            .unwrap();
    }
    let incomplete = executor
        .execute(
            "CREATE OPERATOR CLASS public.point_btree FOR TYPE public.point USING BTREE \
             FROM EXTENSION sample AS 'point_btree'",
        )
        .err()
        .expect("operator class without bound strategy operators must fail");
    assert!(incomplete.to_string().contains("not bound"));
    for (function, operator, symbol) in [
        ("point_lt", "point_lt_operator", "<"),
        ("point_le", "point_le_operator", "<="),
        ("point_eq", "point_eq_operator", "="),
        ("point_ge", "point_ge_operator", ">="),
        ("point_gt", "point_gt_operator", ">"),
    ] {
        executor
            .execute(&format!(
                "CREATE OPERATOR public.{symbol} (LEFTARG = public.point, RIGHTARG = public.point, \
                 FUNCTION = public.{function}(public.point, public.point)) \
                 FROM EXTENSION sample AS '{operator}'"
            ))
            .unwrap();
    }
    executor
        .execute(
            "CREATE OPERATOR public.&& (LEFTARG = public.point, RIGHTARG = public.point, \
             FUNCTION = public.point_eq(public.point, public.point)) \
             FROM EXTENSION sample AS 'point_overlap_operator'",
        )
        .unwrap();
    executor
        .execute(
            "CREATE OPERATOR CLASS public.point_btree FOR TYPE public.point USING BTREE \
             FROM EXTENSION sample AS 'point_btree'",
        )
        .unwrap();
    executor
        .execute(
            "CREATE OPERATOR CLASS public.point_hash FOR TYPE public.point USING HASH \
             FROM EXTENSION sample AS 'point_hash'",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PLANNER SUPPORT public.point_eq_support \
             FOR FUNCTION public.point_eq(public.point, public.point) \
             FROM EXTENSION sample AS 'point_eq_support'",
        )
        .unwrap();
}

fn integer_rows(executor: &Executor, sql: &str, parameter: Value) -> Vec<i64> {
    let mut result = executor
        .execute_with_params(sql, smallvec::smallvec![parameter])
        .unwrap();
    let mut rows = Vec::new();
    while result.next() {
        rows.push(result.row().get(0).and_then(Value::as_int64).unwrap());
    }
    rows
}

#[test]
fn create_and_drop_extension_use_the_catalog_transaction_owner() {
    let engine = opened_engine(Config::in_memory());
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry());

    executor
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();
    executor
        .execute("CREATE EXTENSION IF NOT EXISTS sample VERSION '1.2.3'")
        .unwrap();

    let catalog = engine.pin_catalog().unwrap();
    assert_eq!(catalog.format_minor(), 2);
    let extension = catalog.find_extension("sample").unwrap().unwrap();
    assert_eq!(
        extension.id(),
        ObjectId::from_user_bytes(PACKAGE_ID).unwrap()
    );
    assert_eq!(extension.kind(), ObjectKind::Extension);
    let CatalogPayload::Extension(payload) = extension.payload() else {
        panic!("extension payload expected");
    };
    assert_eq!(payload.version(), "1.2.3");
    assert_eq!(payload.descriptor_fingerprint(), &FINGERPRINT);
    assert!(catalog.graph().outgoing_edges(extension.id()).any(|edge| {
        edge.kind() == EdgeKind::OwnedBy && edge.target_object_id() == ObjectId::BOOTSTRAP_OWNER
    }));
    drop(catalog);

    executor.execute("DROP EXTENSION sample RESTRICT").unwrap();
    assert!(engine
        .pin_catalog()
        .unwrap()
        .find_extension("sample")
        .unwrap()
        .is_none());
}

#[test]
fn reopen_without_package_is_restricted_but_root_can_drop_the_binding() {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config {
        path: Some(temporary.path().to_string_lossy().into_owned()),
        ..Config::default()
    };

    {
        let engine = opened_engine(config.clone());
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry());
        executor
            .execute("CREATE EXTENSION sample VERSION '1.2.3'")
            .unwrap();
        drop(executor);
        engine.close_engine().unwrap();
    }

    let engine = opened_engine(config);
    let executor =
        Executor::with_plugin_registry(Arc::clone(&engine), Arc::new(PluginRegistry::empty()));
    assert!(matches!(
        executor.plugin_admission().unwrap(),
        DatabasePluginAdmission::Restricted { ref issues }
            if matches!(issues.as_slice(), [RequirementIssue::MissingPackage { package_id }] if *package_id == PACKAGE_ID)
    ));
    let error = match executor.execute("SELECT 1") {
        Ok(_) => panic!("restricted database accepted ordinary SQL"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("restricted plugin diagnostic mode"));
    assert!(executor.authenticate_principal("radix_system", "").is_err());

    executor.execute("DROP EXTENSION sample RESTRICT").unwrap();
    assert_eq!(
        executor.plugin_admission().unwrap(),
        DatabasePluginAdmission::Normal
    );
    executor.execute("SELECT 1").unwrap();
}

#[test]
fn drop_extension_restrict_reports_the_first_dependent_object() {
    let engine = opened_engine(Config::in_memory());
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry());
    executor
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();
    let catalog = engine.pin_catalog().unwrap();
    let extension = catalog.find_extension("sample").unwrap().unwrap();
    let function_id = ObjectId::new();
    let data_type = CatalogDataType::scalar(DataType::Integer).unwrap();
    let definition = RoutineDefinition::new(
        ProceduralSource::new("BEGIN RETURN 1; END").unwrap(),
        vec![],
        RoutineResult::Scalar {
            data_type,
            nullable: false,
        },
        Volatility::Immutable,
        SecurityMode::Invoker,
        vec![ObjectId::BOOTSTRAP_NAMESPACE],
        vec![extension.id()],
        1,
        1,
        1,
        ResourcePolicy::default_call(),
    )
    .unwrap();
    let function = CatalogObject::new(
        function_id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("dependent_function").unwrap(),
        1,
        CatalogPayload::Function(FunctionPayload::new(definition).unwrap()),
    )
    .unwrap();
    let dependency = CatalogMutationSet::for_generation(
        catalog.as_ref(),
        vec![CatalogMutation::Create { object: function }],
        Vec::new(),
        vec![
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                function_id,
                EdgeKind::Contains,
                0,
            ),
            CatalogEdge::new(function_id, ObjectId::BOOTSTRAP_OWNER, EdgeKind::OwnedBy, 0),
            CatalogEdge::new(
                function_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
            CatalogEdge::new(function_id, extension.id(), EdgeKind::DependsOn, 0),
        ],
    )
    .unwrap();
    drop(catalog);
    let mut transaction = engine.begin_transaction().unwrap();
    transaction.stage_catalog_mutation(dependency).unwrap();
    transaction.commit().unwrap();

    let error = match executor.execute("DROP EXTENSION sample RESTRICT") {
        Ok(_) => panic!("DROP RESTRICT removed an extension with a dependent"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("dependent_function"));
    assert!(error.to_string().contains("depends on it"));
}

#[test]
fn extension_binding_ddl_requires_the_database_owner() {
    let engine = opened_engine(Config::in_memory());
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry());
    executor.execute("CREATE PRINCIPAL alice").unwrap();
    executor
        .execute("GRANT CONNECT ON DATABASE test TO alice")
        .unwrap();
    let alice = engine
        .pin_catalog()
        .unwrap()
        .objects_of_kind(ObjectKind::Principal)
        .find(|object| object.name().normalized().as_str() == "alice")
        .unwrap()
        .id();
    let alice = ExecutionContext::new().with_principal_id(alice);

    let create_error =
        match executor.execute_with_context("CREATE EXTENSION sample VERSION '1.2.3'", &alice) {
            Ok(_) => panic!("non-owner created an extension binding"),
            Err(error) => error,
        };
    assert!(
        create_error
            .to_string()
            .contains("only the bootstrap owner"),
        "unexpected CREATE denial: {create_error}"
    );

    executor
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();
    let drop_error = match executor.execute_with_context("DROP EXTENSION sample RESTRICT", &alice) {
        Ok(_) => panic!("non-owner dropped an extension binding"),
        Err(error) => error,
    };
    assert!(
        drop_error.to_string().contains("only the bootstrap owner"),
        "unexpected DROP denial: {drop_error}"
    );
}

#[test]
fn external_type_survives_sql_mvcc_checkpoint_and_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config {
        path: Some(temporary.path().to_string_lossy().into_owned()),
        ..Config::default()
    };
    let (registry, type_ref) = external_registry();
    let first = Value::try_external(type_ref, b"10.0,20.0").unwrap();
    let second = Value::try_external(type_ref, b"30.0,40.0").unwrap();

    {
        let engine = opened_engine(config.clone());
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&registry));
        executor
            .execute("CREATE EXTENSION sample VERSION '1.2.3'")
            .unwrap();
        executor
            .execute("CREATE TYPE public.point FROM EXTENSION sample AS 'point'")
            .unwrap();
        executor
            .execute("CREATE TABLE samples (id INTEGER PRIMARY KEY, p public.point)")
            .unwrap();
        executor
            .execute_with_params(
                "INSERT INTO samples (id, p) VALUES ($1, $2)",
                smallvec::smallvec![Value::Integer(1), first.clone()],
            )
            .unwrap();

        let mut selected = executor
            .execute_with_params(
                "SELECT p FROM samples WHERE p = $1",
                smallvec::smallvec![first.clone()],
            )
            .unwrap();
        assert!(selected.next());
        assert_eq!(selected.row().get(0), Some(&first));
        assert!(!selected.next());

        executor
            .execute_with_params(
                "UPDATE samples SET p = $1 WHERE id = 1",
                smallvec::smallvec![second.clone()],
            )
            .unwrap();
        executor
            .execute("INSERT INTO samples (id, p) VALUES (2, CAST('50.0,60.0' AS public.point))")
            .unwrap();
        let mut text = executor
            .execute("SELECT CAST(p AS TEXT) FROM samples WHERE id = 2")
            .unwrap();
        assert!(text.next());
        assert_eq!(text.row().get(0).and_then(Value::as_str), Some("50.0,60.0"));
        let mut binary = executor
            .execute("SELECT CAST(p AS BYTES) FROM samples WHERE id = 2")
            .unwrap();
        assert!(binary.next());
        assert_eq!(
            binary.row().get(0).and_then(Value::as_bytes_value),
            Some(b"50.0,60.0".as_slice())
        );
        let index_error = executor
            .execute("CREATE INDEX samples_p ON samples(p)")
            .err()
            .expect("external index without operator class must fail");
        assert!(index_error.to_string().contains("operator class"));
        executor.execute("PRAGMA CHECKPOINT").unwrap();
        drop(executor);
        engine.close_engine().unwrap();
    }

    let engine = opened_engine(config);
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
    assert_eq!(
        executor.plugin_admission().unwrap(),
        DatabasePluginAdmission::Normal
    );
    let mut selected = executor
        .execute("SELECT p FROM samples WHERE id = 1")
        .unwrap();
    assert!(selected.next());
    assert_eq!(selected.row().get(0), Some(&second));
    assert!(!selected.next());
    assert_eq!(
        executor
            .execute("DELETE FROM samples WHERE id = 1")
            .unwrap()
            .rows_affected(),
        1
    );

    let stale = Value::try_external(
        ExternalTypeRef::new(type_ref.type_object_id(), 2).unwrap(),
        b"x",
    )
    .unwrap();
    let error = executor
        .execute_with_params(
            "INSERT INTO samples (id, p) VALUES ($1, $2)",
            smallvec::smallvec![Value::Integer(3), stale],
        )
        .err()
        .expect("stale codec must fail before mutation");
    assert!(
        error.to_string().contains("codec version"),
        "unexpected stale-codec error: {error}"
    );
    let mut absent = executor
        .execute("SELECT id FROM samples WHERE id = 3")
        .unwrap();
    assert!(!absent.next());

    let missing_object_registry = Arc::new(PluginRegistry::from_test_objects(
        [RegisteredPackage::for_test(
            PACKAGE_ID,
            "sample",
            "1.2.3",
            FINGERPRINT,
        )],
        [],
        [],
        [],
        [],
        [],
    ));
    let restricted = Executor::with_plugin_registry(engine, missing_object_registry);
    let DatabasePluginAdmission::Restricted { issues } = restricted.plugin_admission().unwrap()
    else {
        panic!("missing external type descriptor admitted the database normally");
    };
    assert!(issues.iter().any(|issue| matches!(
        issue,
        RequirementIssue::MissingOrStaleObject { object_id, .. }
            if *object_id == type_ref.type_object_id()
    )));
}

#[test]
fn native_function_direct_pl_acl_dependency_and_reopen_contract() {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config {
        path: Some(temporary.path().to_string_lossy().into_owned()),
        ..Config::default()
    };
    let (registry, native_id) = native_registry();

    {
        let engine = opened_engine(config.clone());
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&registry));
        executor
            .execute("CREATE EXTENSION sample VERSION '1.2.3'")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION native_increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
                 LANGUAGE NATIVE FROM EXTENSION sample AS 'increment';",
            )
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION wrapped_increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
                 LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN native_increment(value); END;",
            )
            .unwrap();

        assert_eq!(scalar_integer(&executor, "SELECT native_increment(41)"), 42);
        assert_eq!(
            scalar_integer(&executor, "SELECT wrapped_increment(41)"),
            42
        );
        let catalog = engine.pin_catalog().unwrap();
        let native = catalog.object(native_id).unwrap();
        let CatalogPayload::Function(FunctionPayload::Native(definition)) = native.payload() else {
            panic!("native function payload expected");
        };
        assert_eq!(definition.local_id(), "increment");
        assert_eq!(definition.semantic_revision(), 3);
        assert_eq!(definition.cost(), 4);
        let wrapper = catalog
            .objects_of_kind(ObjectKind::Function)
            .find(|object| object.name().normalized().as_str() == "wrapped_increment")
            .unwrap();
        let CatalogPayload::Function(FunctionPayload::Procedural(definition)) = wrapper.payload()
        else {
            panic!("procedural wrapper payload expected");
        };
        assert!(definition.dependency_ids().contains(&native_id));
        drop(catalog);

        executor.execute("CREATE PRINCIPAL native_caller").unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO native_caller")
            .unwrap();
        executor
            .execute("GRANT USAGE ON SCHEMA public TO native_caller")
            .unwrap();
        executor
            .execute("GRANT EXECUTE ON FUNCTION native_increment(INTEGER) TO native_caller")
            .unwrap();
        executor
            .execute("GRANT EXECUTE ON FUNCTION wrapped_increment(INTEGER) TO native_caller")
            .unwrap();
        let caller = engine
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Principal)
            .find(|object| object.name().normalized().as_str() == "native_caller")
            .unwrap()
            .id();
        let caller = ExecutionContext::new().with_principal_id(caller);
        let mut result = executor
            .execute_with_context("SELECT wrapped_increment(9)", &caller)
            .unwrap();
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(10)));
        drop(result);
        executor
            .execute("REVOKE EXECUTE ON FUNCTION native_increment(INTEGER) FROM native_caller")
            .unwrap();
        assert!(executor
            .execute_with_context("SELECT wrapped_increment(9)", &caller)
            .is_err());
        executor
            .execute("GRANT EXECUTE ON FUNCTION native_increment(INTEGER) TO native_caller")
            .unwrap();

        assert!(executor
            .execute("DROP FUNCTION native_increment(INTEGER) CASCADE")
            .err()
            .expect("native CASCADE must fail")
            .to_string()
            .contains("RESTRICT"));
        assert!(executor
            .execute("DROP FUNCTION native_increment(INTEGER) RESTRICT")
            .err()
            .expect("dependent native RESTRICT must fail")
            .to_string()
            .contains("depends on it"));
        executor.execute("PRAGMA CHECKPOINT").unwrap();
        drop(executor);
        engine.close_engine().unwrap();
    }

    let engine = opened_engine(config);
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
    assert_eq!(
        scalar_integer(&executor, "SELECT wrapped_increment(73)"),
        74
    );
    executor
        .execute("DROP FUNCTION wrapped_increment(INTEGER) RESTRICT")
        .unwrap();
    executor
        .execute("DROP FUNCTION native_increment(INTEGER) RESTRICT")
        .unwrap();
    engine.close_engine().unwrap();
}

#[test]
fn native_function_binding_is_exact_and_transactional() {
    let engine = opened_engine(Config::in_memory());
    let (registry, native_id) = native_registry();
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
    executor
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();

    for invalid in [
        "CREATE FUNCTION wrong_type(value TEXT NOT NULL) RETURNS INTEGER NOT NULL \
         LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        "CREATE FUNCTION nullable_strict(value INTEGER) RETURNS INTEGER NOT NULL \
         LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        "CREATE FUNCTION wrong_result(value INTEGER NOT NULL) RETURNS TEXT NOT NULL \
         LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
    ] {
        assert!(executor.execute(invalid).is_err(), "accepted {invalid}");
        assert!(engine.pin_catalog().unwrap().object(native_id).is_none());
    }

    executor.execute("BEGIN").unwrap();
    executor
        .execute(
            "CREATE FUNCTION native_increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
             LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        )
        .unwrap();
    assert_eq!(scalar_integer(&executor, "SELECT native_increment(1)"), 2);
    executor.execute("ROLLBACK").unwrap();
    assert!(engine.pin_catalog().unwrap().object(native_id).is_none());
    assert!(executor.execute("SELECT native_increment(1)").is_err());

    executor
        .execute(
            "CREATE FUNCTION native_increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
             LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        )
        .unwrap();
    assert!(executor
        .execute(
            "CREATE FUNCTION native_alias(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
             LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        )
        .is_err());
    assert_eq!(scalar_integer(&executor, "SELECT native_increment(8)"), 9);
}

#[test]
fn external_btree_hash_operator_dml_and_reopen_contract() {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config {
        path: Some(temporary.path().to_string_lossy().into_owned()),
        ..Config::default()
    };
    let (registry, type_ref) = indexed_external_registry();
    let point = |value: &'static [u8]| Value::try_external(type_ref, value).unwrap();

    {
        let engine = opened_engine_with_registry(config.clone(), Arc::clone(&registry));
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&registry));
        bind_indexed_external_catalog(&executor);
        executor
            .execute(
                "CREATE TABLE btree_values (id INTEGER PRIMARY KEY, p public.point NOT NULL); \
                 CREATE TABLE hash_values (id INTEGER PRIMARY KEY, p public.point NOT NULL)",
            )
            .unwrap();
        for id in 1..=3_i64 {
            let value = point(match id {
                1 => b"10",
                2 => b"20",
                _ => b"30",
            });
            for table in ["btree_values", "hash_values"] {
                executor
                    .execute_with_params(
                        &format!("INSERT INTO {table} (id, p) VALUES ($1, $2)"),
                        smallvec::smallvec![Value::Integer(id), value.clone()],
                    )
                    .unwrap();
            }
        }
        executor
            .execute(
                "CREATE INDEX disposable_point_index ON btree_values(p public.point_hash) USING HASH",
            )
            .unwrap();
        executor
            .execute("DROP INDEX disposable_point_index ON btree_values")
            .unwrap();
        assert!(engine
            .pin_catalog()
            .unwrap()
            .find_index(ObjectId::BOOTSTRAP_NAMESPACE, "disposable_point_index")
            .unwrap()
            .is_none());
        executor
            .execute(
                "CREATE INDEX btree_values_p ON btree_values(p public.point_btree) USING BTREE; \
                 CREATE INDEX hash_values_p ON hash_values(p public.point_hash) USING HASH",
            )
            .unwrap();

        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE point_eq(p, $1) ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        let mut explain = executor
            .execute_with_params(
                "EXPLAIN ANALYZE SELECT id FROM btree_values WHERE point_eq(p, $1)",
                smallvec::smallvec![point(b"20")],
            )
            .unwrap();
        let mut explain_lines = Vec::new();
        while explain.next() {
            explain_lines.push(
                explain
                    .row()
                    .get(0)
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_owned(),
            );
        }
        assert!(explain_lines.iter().any(|line| {
            line.contains("Plugin Candidate Scan")
                && line.contains("spans=1")
                && line.contains("candidates=3")
                && line.contains("rechecks=3")
        }));
        executor
            .execute(
                "CREATE TABLE selectivity_value (id INTEGER PRIMARY KEY, p public.point NOT NULL)",
            )
            .unwrap();
        executor
            .execute_with_params(
                "INSERT INTO selectivity_value (id, p) VALUES (1, $1)",
                smallvec::smallvec![point(b"20")],
            )
            .unwrap();
        executor
            .execute(
                "CREATE INDEX selectivity_value_p ON selectivity_value(p public.point_btree) USING BTREE",
            )
            .unwrap();
        let mut fallback = executor
            .execute_with_params(
                "EXPLAIN ANALYZE SELECT id FROM selectivity_value WHERE point_eq(p, $1)",
                smallvec::smallvec![point(b"20")],
            )
            .unwrap();
        let mut saw_selectivity_fallback = false;
        while fallback.next() {
            saw_selectivity_fallback |=
                fallback
                    .row()
                    .get(0)
                    .and_then(Value::as_str)
                    .is_some_and(|line| {
                        line.contains("Plugin Candidate Scan") && line.contains("fallbacks=1")
                    });
        }
        assert!(saw_selectivity_fallback);

        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE p >= $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2, 3]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM hash_values WHERE p = $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE p && $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );

        executor
            .execute_with_params(
                "UPDATE btree_values SET p = $1 WHERE id = 3",
                smallvec::smallvec![point(b"05")],
            )
            .unwrap();
        executor
            .execute_with_params(
                "UPDATE hash_values SET p = $1 WHERE id = 3",
                smallvec::smallvec![point(b"25")],
            )
            .unwrap();
        executor
            .execute("DELETE FROM btree_values WHERE id = 1")
            .unwrap();
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE p >= $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM hash_values WHERE p = $1 ORDER BY id",
                point(b"25"),
            ),
            vec![3]
        );
        assert_external_runtime_index_metadata(&engine);
        let btree = engine.get_index("btree_values", "btree_values_p").unwrap();
        assert_eq!(btree.get_row_ids_equal(&[point(b"20")]).unwrap().len(), 1);
        assert!(btree.get_row_ids_equal(&[point(b"10")]).unwrap().is_empty());
        assert_eq!(
            btree
                .find_with_operator(Operator::Gte, &[point(b"20")])
                .unwrap()
                .len(),
            1
        );
        let hash = engine.get_index("hash_values", "hash_values_p").unwrap();
        assert_eq!(hash.get_row_ids_equal(&[point(b"25")]).unwrap().len(), 1);
        assert!(hash.get_row_ids_equal(&[point(b"30")]).unwrap().is_empty());

        let catalog = engine.pin_catalog().unwrap();
        for (index_name, class_name) in [
            ("btree_values_p", "point_btree"),
            ("hash_values_p", "point_hash"),
        ] {
            let index = catalog
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, index_name)
                .unwrap()
                .unwrap();
            let class = catalog
                .find_operator_class(ObjectId::BOOTSTRAP_NAMESPACE, class_name)
                .unwrap()
                .unwrap();
            let CatalogPayload::Index(payload) = index.payload() else {
                panic!("index payload expected");
            };
            assert_eq!(payload.operator_class_id(), Some(class.id()));
            assert!(catalog.graph().outgoing_edges(index.id()).any(|edge| {
                edge.kind() == EdgeKind::DependsOn && edge.target_object_id() == class.id()
            }));
        }
        let btree_class = catalog
            .find_operator_class(ObjectId::BOOTSTRAP_NAMESPACE, "point_btree")
            .unwrap()
            .unwrap();
        let CatalogPayload::OperatorClass(payload) = btree_class.payload() else {
            panic!("operator class payload expected");
        };
        assert_eq!(payload.strategies().len(), 5);
        assert_eq!(
            payload.strategies()[2].object_id(),
            ObjectId::from_user_bytes(derive_object_id(PACKAGE_ID, "point_eq_operator").unwrap())
                .unwrap()
        );
        let support = catalog
            .find_planner_support(ObjectId::BOOTSTRAP_NAMESPACE, "point_eq_support")
            .unwrap()
            .unwrap();
        let CatalogPayload::PlannerSupport(payload) = support.payload() else {
            panic!("planner support payload expected");
        };
        assert_eq!(
            payload.target_function_id(),
            Some(
                ObjectId::from_user_bytes(derive_object_id(PACKAGE_ID, "point_eq").unwrap())
                    .unwrap()
            )
        );
        assert_eq!(payload.target_operator_class_id(), Some(btree_class.id()));
        assert!(catalog.graph().outgoing_edges(support.id()).any(|edge| {
            edge.kind().is_dependency() && edge.target_object_id() == btree_class.id()
        }));
        drop(catalog);

        assert!(executor
            .execute("DROP OPERATOR CLASS public.point_btree USING BTREE RESTRICT")
            .err()
            .expect("dependent operator class must not drop")
            .to_string()
            .contains("depends on it"));
        assert!(executor
            .execute("DROP OPERATOR public.= (public.point, public.point) RESTRICT")
            .err()
            .expect("strategy operator must not drop")
            .to_string()
            .contains("depends on it"));
        executor.execute("PRAGMA CHECKPOINT").unwrap();
        drop(executor);
        engine.close_engine().unwrap();
    }

    {
        let engine = opened_engine_with_registry(config.clone(), Arc::clone(&registry));
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
        assert_eq!(
            executor.plugin_admission().unwrap(),
            DatabasePluginAdmission::Normal
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE p >= $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM hash_values WHERE p = $1 ORDER BY id",
                point(b"25"),
            ),
            vec![3]
        );
        assert_external_runtime_index_metadata(&engine);
        for table in ["btree_values", "hash_values"] {
            executor
                .execute_with_params(
                    &format!("INSERT INTO {table} (id, p) VALUES (4, $1)"),
                    smallvec::smallvec![point(b"40")],
                )
                .unwrap();
            assert_eq!(
                engine
                    .get_index(table, &format!("{table}_p"))
                    .unwrap()
                    .get_row_ids_equal(&[point(b"40")])
                    .unwrap()
                    .len(),
                1
            );
        }
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE p >= $1 ORDER BY id",
                point(b"20"),
            ),
            vec![2, 4]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE point_eq(p, $1) ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM hash_values WHERE p = $1 ORDER BY id",
                point(b"40"),
            ),
            vec![4]
        );
        executor
            .execute("DROP PLANNER SUPPORT public.point_eq_support RESTRICT")
            .unwrap();
        assert_eq!(
            integer_rows(
                &executor,
                "SELECT id FROM btree_values WHERE point_eq(p, $1) ORDER BY id",
                point(b"20"),
            ),
            vec![2]
        );
        drop(executor);
        engine.close_engine().unwrap();
    }

    let (stale_registry, _) = indexed_external_registry_with_class_revision(2);
    let engine = opened_engine_with_registry(config, Arc::clone(&stale_registry));
    let executor = Executor::with_plugin_registry(engine, stale_registry);
    let DatabasePluginAdmission::Restricted { issues } = executor.plugin_admission().unwrap()
    else {
        panic!("stale operator-class semantics were admitted");
    };
    assert!(issues.iter().any(|issue| matches!(
        issue,
        RequirementIssue::MissingOrStaleObject {
            kind: radixdb_plugin_host::ObjectKind::OperatorClass,
            ..
        }
    )));
    assert!(executor.execute("SELECT 1").is_err());
}

#[test]
fn incompatible_package_upgrade_fails_closed_and_rollback_reads_old_codec_data() {
    let temporary = tempfile::tempdir().unwrap();
    let config = Config {
        path: Some(temporary.path().to_string_lossy().into_owned()),
        ..Config::default()
    };
    let (old_registry, type_ref) = external_registry_with_version("1.2.3");
    let old_value = Value::try_external(type_ref, b"reachable-old-codec").unwrap();

    {
        let engine = opened_engine_with_registry(config.clone(), Arc::clone(&old_registry));
        let executor =
            Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&old_registry));
        executor
            .execute("CREATE EXTENSION sample VERSION '1.2.3'")
            .unwrap();
        executor
            .execute("CREATE TYPE public.point FROM EXTENSION sample AS 'point'")
            .unwrap();
        executor
            .execute("CREATE TABLE upgrade_values (id INTEGER PRIMARY KEY, p public.point)")
            .unwrap();
        executor
            .execute_with_params(
                "INSERT INTO upgrade_values (id, p) VALUES (1, $1)",
                smallvec::smallvec![old_value.clone()],
            )
            .unwrap();
        executor.execute("PRAGMA CHECKPOINT").unwrap();
        drop(executor);
        engine.close_engine().unwrap();
    }

    let (new_registry, _) = external_registry_with_version("1.2.4");
    {
        let engine = opened_engine_with_registry(config.clone(), Arc::clone(&new_registry));
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), new_registry);
        let DatabasePluginAdmission::Restricted { issues } = executor.plugin_admission().unwrap()
        else {
            panic!("unrequested exact-package upgrade was admitted");
        };
        assert!(issues
            .iter()
            .any(|issue| matches!(issue, RequirementIssue::PackageVersion { .. })));
        assert!(executor.execute("SELECT p FROM upgrade_values").is_err());
        drop(executor);
        engine.close_engine().unwrap();
    }

    let engine = opened_engine_with_registry(config, Arc::clone(&old_registry));
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), old_registry);
    assert_eq!(
        executor.plugin_admission().unwrap(),
        DatabasePluginAdmission::Normal
    );
    let mut result = executor
        .execute("SELECT p FROM upgrade_values WHERE id = 1")
        .unwrap();
    assert!(result.next());
    assert_eq!(result.row().get(0), Some(&old_value));
    assert!(!result.next());
    drop(result);
    drop(executor);
    engine.close_engine().unwrap();
}

#[test]
fn concurrent_native_calls_observe_atomic_revoke_and_catalog_ddl() {
    let engine = opened_engine(Config::in_memory());
    let (registry, _) = native_registry();
    let administrator = Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&registry));
    administrator
        .execute("CREATE EXTENSION sample VERSION '1.2.3'")
        .unwrap();
    administrator
        .execute(
            "CREATE FUNCTION native_increment(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
             LANGUAGE NATIVE FROM EXTENSION sample AS 'increment'",
        )
        .unwrap();
    administrator.execute("CREATE PRINCIPAL caller").unwrap();
    administrator
        .execute("GRANT CONNECT ON DATABASE test TO caller")
        .unwrap();
    administrator
        .execute("GRANT USAGE ON SCHEMA public TO caller")
        .unwrap();
    administrator
        .execute("GRANT EXECUTE ON FUNCTION native_increment(INTEGER) TO caller")
        .unwrap();
    let caller = engine
        .pin_catalog()
        .unwrap()
        .objects_of_kind(ObjectKind::Principal)
        .find(|object| object.name().normalized().as_str() == "caller")
        .unwrap()
        .id();

    const WORKERS: usize = 8;
    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let phase = Arc::new(AtomicU8::new(0));
    let allowed_before = Arc::new(AtomicUsize::new(0));
    let denied = Arc::new(AtomicUsize::new(0));
    let allowed_after = Arc::new(AtomicUsize::new(0));
    let unexpected = Arc::new(AtomicUsize::new(0));
    let workers = (0..WORKERS)
        .map(|_| {
            let engine = Arc::clone(&engine);
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            let phase = Arc::clone(&phase);
            let allowed_before = Arc::clone(&allowed_before);
            let denied = Arc::clone(&denied);
            let allowed_after = Arc::clone(&allowed_after);
            let unexpected = Arc::clone(&unexpected);
            thread::spawn(move || {
                let executor = Executor::with_plugin_registry(engine, registry);
                let context = ExecutionContext::new().with_principal_id(caller);
                barrier.wait();
                loop {
                    let observed_phase = phase.load(Ordering::Acquire);
                    if observed_phase == 3 {
                        break;
                    }
                    match executor.execute_with_context("SELECT native_increment(41)", &context) {
                        Ok(mut result) => {
                            if !result.next() || result.row().get(0) != Some(&Value::Integer(42)) {
                                unexpected.fetch_add(1, Ordering::Relaxed);
                            } else if observed_phase == 0 {
                                allowed_before.fetch_add(1, Ordering::Relaxed);
                            } else if observed_phase == 2 {
                                allowed_after.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(error) => {
                            let diagnostic = error.to_string();
                            if observed_phase == 1
                                && (diagnostic.contains("EXECUTE")
                                    || diagnostic.contains("privilege"))
                            {
                                denied.fetch_add(1, Ordering::Relaxed);
                            } else if observed_phase != 1 {
                                // A call that began in the preceding generation may
                                // finish after the phase marker changed. Both complete
                                // old/new outcomes are valid; malformed results are not.
                            } else {
                                unexpected.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    thread::yield_now();
                }
            })
        })
        .collect::<Vec<_>>();

    fn wait_for(counter: &AtomicUsize) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while counter.load(Ordering::Acquire) < 4 {
            assert!(
                Instant::now() < deadline,
                "concurrent gate made no progress"
            );
            thread::yield_now();
        }
    }

    barrier.wait();
    wait_for(&allowed_before);
    administrator
        .execute("REVOKE EXECUTE ON FUNCTION native_increment(INTEGER) FROM caller")
        .unwrap();
    phase.store(1, Ordering::Release);
    wait_for(&denied);

    for revision in 0..8 {
        let name = format!("temporary_wrapper_{revision}");
        administrator
            .execute(&format!(
                "CREATE FUNCTION {name}(value INTEGER NOT NULL) RETURNS INTEGER NOT NULL \
                 LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN native_increment(value); END;"
            ))
            .unwrap();
        administrator
            .execute(&format!("DROP FUNCTION {name}(INTEGER) RESTRICT"))
            .unwrap();
    }

    administrator
        .execute("GRANT EXECUTE ON FUNCTION native_increment(INTEGER) TO caller")
        .unwrap();
    phase.store(2, Ordering::Release);
    wait_for(&allowed_after);
    phase.store(3, Ordering::Release);
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(unexpected.load(Ordering::Relaxed), 0);
    assert!(allowed_before.load(Ordering::Relaxed) >= 4);
    assert!(denied.load(Ordering::Relaxed) >= 4);
    assert!(allowed_after.load(Ordering::Relaxed) >= 4);
}
