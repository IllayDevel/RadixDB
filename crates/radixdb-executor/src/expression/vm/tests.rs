use super::*;
use radixdb_core::CompactArc;
use radixdb_core::Row;
use radixdb_core::ValueSet;
use radixdb_functions::{
    FunctionDataType, FunctionInfo, FunctionSignature, FunctionType, ScalarFunction,
};
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

static COW_SCALAR_CALLS: AtomicUsize = AtomicUsize::new(0);

struct CountingScalar;

struct FailingScalar;

impl ScalarFunction for CountingScalar {
    fn name(&self) -> &str {
        "COUNTING_SCALAR"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            self.name(),
            FunctionType::Scalar,
            "Cow fallback test",
            FunctionSignature::new(
                FunctionDataType::Integer,
                vec![FunctionDataType::Integer],
                1,
                1,
            ),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        COW_SCALAR_CALLS.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(args[0].clone())
    }
}

impl ScalarFunction for FailingScalar {
    fn name(&self) -> &str {
        "FAILING_SCALAR"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            self.name(),
            FunctionType::Scalar,
            "Boolean error propagation test",
            FunctionSignature::new(FunctionDataType::Boolean, vec![], 0, 0),
        )
    }

    fn evaluate(&self, _args: &[Value]) -> Result<Value> {
        Err(radixdb_core::Error::Type(
            "deliberate scalar failure".to_string(),
        ))
    }
}

#[test]
fn test_simple_comparison() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(Value::Integer(5)),
        Op::Gt,
        Op::Return,
    ]);

    // Test with row where column 0 > 5
    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Test with row where column 0 <= 5
    let row = Row::from_values(vec![Value::Integer(3)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_and_short_circuit() {
    let mut vm = ExprVM::new();
    // WHERE col0 > 5 AND col1 < 10
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(Value::Integer(5)),
        Op::Gt,
        Op::And(8), // Jump to return false if first condition is false
        Op::LoadColumn(1),
        Op::LoadConst(Value::Integer(10)),
        Op::Lt,
        Op::AndFinalize,
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(10), Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(3), Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_in_set() {
    let mut vm = ExprVM::new();
    let set: ValueSet = [Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        .into_iter()
        .collect();

    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::InSet(CompactArc::new(set), false),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(2)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// ExecuteContext tests
// =========================================================================

#[test]
fn test_context_new() {
    let row = Row::from_values(vec![Value::Integer(1), Value::Text("test".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(ctx.row.len(), 2);
    assert!(ctx.row2.is_none());
    assert!(ctx.outer_row.is_none());
    assert!(ctx.params.is_empty());
}

#[test]
fn test_context_for_join() {
    let row1 = Row::from_values(vec![Value::Integer(1)]);
    let row2 = Row::from_values(vec![Value::Integer(2)]);
    let ctx = ExecuteContext::for_join(&row1, &row2);
    assert_eq!(ctx.row.len(), 1);
    assert!(ctx.row2.is_some());
    assert_eq!(*ctx.row2.unwrap().get(0).unwrap(), Value::Integer(2));
}

#[test]
fn test_context_with_params() {
    let row = Row::from_values(vec![Value::Integer(1)]);
    let params = vec![Value::Text("param1".into())];
    let ctx = ExecuteContext::new(&row).with_params(&params);
    assert_eq!(ctx.params.len(), 1);
}

#[test]
fn test_context_with_named_params() {
    let row = Row::from_values(vec![Value::Integer(1)]);
    let mut named = FxHashMap::default();
    named.insert("name".to_string(), Value::Text("value".into()));
    let ctx = ExecuteContext::new(&row).with_named_params(&named);
    assert!(ctx.named_params.is_some());
}

#[test]
fn test_context_with_transaction_id() {
    let row = Row::from_values(vec![Value::Integer(1)]);
    let ctx = ExecuteContext::new(&row).with_transaction_id(Some(12345));
    assert_eq!(ctx.transaction_id, Some(12345));
}

#[test]
fn test_context_with_outer_row() {
    let row = Row::from_values(vec![Value::Integer(1)]);
    let mut outer: FxHashMap<CompactArc<str>, Value> = FxHashMap::default();
    outer.insert(CompactArc::from("outer_col"), Value::Integer(42));
    let ctx = ExecuteContext::new(&row).with_outer_row(&outer);
    assert!(ctx.outer_row.is_some());
}

// =========================================================================
// ExprVM creation tests
// =========================================================================

#[test]
fn test_vm_new() {
    let vm = ExprVM::new();
    assert_eq!(vm.stack.len(), 0);
}

#[test]
fn test_vm_with_capacity() {
    let vm = ExprVM::with_capacity(16);
    assert!(vm.stack.capacity() >= 16);
}

#[test]
fn test_vm_default() {
    let vm = ExprVM::default();
    assert_eq!(vm.stack.len(), 0);
}

// =========================================================================
// Load operations tests
// =========================================================================

#[test]
fn test_load_column() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(1), Op::Return]);
    let row = Row::from_values(vec![
        Value::Integer(10),
        Value::Integer(20),
        Value::Integer(30),
    ]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(20));
}

#[test]
fn test_load_column_out_of_bounds() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(10), Op::Return]);
    let row = Row::from_values(vec![Value::Integer(1)]);
    let ctx = ExecuteContext::new(&row);
    // Out of bounds returns null
    assert!(vm.execute(&program, &ctx).unwrap().is_null());
}

#[test]
fn test_load_column2() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn2(0), Op::Return]);
    let row1 = Row::from_values(vec![Value::Integer(1)]);
    let row2 = Row::from_values(vec![Value::Integer(2)]);
    let ctx = ExecuteContext::for_join(&row1, &row2);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(2));
}

#[test]
fn test_load_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadConst(Value::Float(1.23)), Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Float(1.23));
}

#[test]
fn test_load_param() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadParam(0), Op::Return]);
    let row = Row::new();
    let params = vec![Value::Text("hello".into())];
    let ctx = ExecuteContext::new(&row).with_params(&params);
    assert_eq!(
        vm.execute(&program, &ctx).unwrap(),
        Value::Text("hello".into())
    );
}

#[test]
fn test_load_named_param() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadNamedParam(CompactArc::from("myvar")),
        Op::Return,
    ]);
    let row = Row::new();
    let mut named = FxHashMap::default();
    named.insert("myvar".to_string(), Value::Integer(999));
    let ctx = ExecuteContext::new(&row).with_named_params(&named);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(999));
}

#[test]
fn test_load_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadNull(DataType::Integer), Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute(&program, &ctx).unwrap().is_null());
}

#[test]
fn test_load_outer_column() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadOuterColumn(CompactArc::from("outer_val")),
        Op::Return,
    ]);
    let row = Row::new();
    let mut outer: FxHashMap<CompactArc<str>, Value> = FxHashMap::default();
    outer.insert(CompactArc::from("outer_val"), Value::Integer(100));
    let ctx = ExecuteContext::new(&row).with_outer_row(&outer);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(100));
}

// =========================================================================
// Comparison operations tests
// =========================================================================

#[test]
fn test_eq() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(5)),
        Op::Eq,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_ne() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(10)),
        Op::Ne,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_lt() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(3)),
        Op::LoadConst(Value::Integer(5)),
        Op::Lt,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_le() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(5)),
        Op::Le,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_ge() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(5)),
        Op::Ge,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_exact_mixed_numeric_comparison_paths() {
    let two53 = 1_i64 << 53;
    let empty = Row::new();
    let empty_ctx = ExecuteContext::new(&empty);
    let mut vm = ExprVM::new();

    let stack_eq = Program::new(vec![
        Op::LoadConst(Value::Integer(two53 + 1)),
        Op::LoadConst(Value::Float(two53 as f64)),
        Op::Eq,
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&stack_eq, &empty_ctx).unwrap(),
        Value::Boolean(false)
    );
    assert_eq!(
        vm.execute_cow(&stack_eq, &empty_ctx).unwrap(),
        Value::Boolean(false)
    );

    let stack_gt = Program::new(vec![
        Op::LoadConst(Value::Integer(two53 + 1)),
        Op::LoadConst(Value::Float(two53 as f64)),
        Op::Gt,
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&stack_gt, &empty_ctx).unwrap(),
        Value::Boolean(true)
    );
    assert_eq!(
        vm.execute_cow(&stack_gt, &empty_ctx).unwrap(),
        Value::Boolean(true)
    );

    let max_vs_two63 = Program::new(vec![
        Op::LoadConst(Value::Integer(i64::MAX)),
        Op::LoadConst(Value::Float(2_f64.powi(63))),
        Op::Lt,
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&max_vs_two63, &empty_ctx).unwrap(),
        Value::Boolean(true)
    );

    let signed_zero = Program::new(vec![
        Op::LoadConst(Value::Integer(0)),
        Op::LoadConst(Value::Float(-0.0)),
        Op::Eq,
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&signed_zero, &empty_ctx).unwrap(),
        Value::Boolean(true)
    );

    let canonical_nan = Program::new(vec![
        Op::LoadConst(Value::Float(f64::NAN)),
        Op::LoadConst(Value::Float(f64::NAN)),
        Op::Eq,
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&canonical_nan, &empty_ctx).unwrap(),
        Value::Boolean(true)
    );

    let row = Row::from_values(vec![Value::Integer(two53 + 1)]);
    let ctx = ExecuteContext::new(&row);
    let fused_eq = Program::new(vec![
        Op::EqColumnConst(0, Value::Float(two53 as f64)),
        Op::Return,
    ]);
    assert_eq!(vm.execute(&fused_eq, &ctx).unwrap(), Value::Boolean(false));
    assert_eq!(
        vm.execute_cow(&fused_eq, &ctx).unwrap(),
        Value::Boolean(false)
    );
    assert!(!vm.execute_bool_checked(&fused_eq, &ctx).unwrap());
    assert_eq!(
        ExprVM::eval_single_op_tribool(&Op::GtColumnConst(0, Value::Float(two53 as f64)), &ctx,)
            .unwrap(),
        Some(true)
    );

    let max_row = Row::from_values(vec![Value::Integer(i64::MAX)]);
    let max_ctx = ExecuteContext::new(&max_row);
    let fused_between = Program::new(vec![
        Op::BetweenColumnConst(0, Value::Float(two53 as f64), Value::Float(2_f64.powi(63))),
        Op::Return,
    ]);
    assert_eq!(
        vm.execute(&fused_between, &max_ctx).unwrap(),
        Value::Boolean(true)
    );
    assert!(vm.execute_bool_checked(&fused_between, &max_ctx).unwrap());
    assert_eq!(
        ExprVM::eval_single_op_tribool(&fused_between.ops()[0], &max_ctx).unwrap(),
        Some(true)
    );

    let set: ValueSet = [Value::Float(two53 as f64)].into_iter().collect();
    let fused_in = Program::new(vec![
        Op::InSetColumn(0, CompactArc::new(set), false),
        Op::Return,
    ]);
    assert_eq!(vm.execute(&fused_in, &ctx).unwrap(), Value::Boolean(false));
    assert!(!vm.execute_bool_checked(&fused_in, &ctx).unwrap());
}

#[test]
fn sql_literal_binding_does_not_change_structural_value_identity() {
    let uuid_text = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7106";
    let uuid = Value::uuid(radixdb_core::value::parse_uuid_str(uuid_text).unwrap());
    let timestamp =
        Value::Timestamp(radixdb_core::value::parse_timestamp("2024-01-08T00:00:00Z").unwrap());
    let timestamp_text = Value::text("2024-01-07");
    let row = Row::new();
    let context = ExecuteContext::new(&row);

    assert!(uuid.compare(&Value::text(uuid_text)).is_err());
    assert!(timestamp.compare(&timestamp_text).is_err());

    assert!(ExprVM::sql_values_equal(&context, &uuid, &Value::text(uuid_text)).unwrap());
    assert_eq!(
        ExprVM::sql_ordering(&context, &timestamp, &timestamp_text).unwrap(),
        Some(std::cmp::Ordering::Greater),
    );
    assert_eq!(
        ExprVM::sql_ordering(&context, &Value::Boolean(true), &Value::text("true")).unwrap(),
        None,
    );
}

#[derive(Debug)]
struct ExternalSemanticFixture;

impl crate::context::StoredFunctionInvoker for ExternalSemanticFixture {
    fn invoke(
        self: std::sync::Arc<Self>,
        _name: &str,
        _arguments: &[Value],
    ) -> radixdb_core::Result<Value> {
        Err(radixdb_core::Error::NotSupported("fixture".to_owned()))
    }

    fn external_equal(&self, _left: &Value, _right: &Value) -> radixdb_core::Result<bool> {
        Ok(true)
    }

    fn external_compare(
        &self,
        _left: &Value,
        _right: &Value,
    ) -> radixdb_core::Result<std::cmp::Ordering> {
        Ok(std::cmp::Ordering::Greater)
    }
}

#[test]
fn external_comparisons_require_and_use_runtime_callbacks() {
    let type_ref = radixdb_core::ExternalTypeRef::new([0x61; 16], 1).unwrap();
    let left = Value::try_external(type_ref, b"raw-left").unwrap();
    let right = Value::try_external(type_ref, b"raw-right").unwrap();
    assert_ne!(left, right);

    let row = Row::from_values(vec![left.clone()]);
    let no_callbacks = ExecuteContext::new(&row);
    let equality = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(right.clone()),
        Op::Eq,
        Op::Return,
    ]);
    assert!(ExprVM::new().execute(&equality, &no_callbacks).is_err());

    let invoker: std::sync::Arc<dyn crate::context::StoredFunctionInvoker> =
        std::sync::Arc::new(ExternalSemanticFixture);
    let callbacks = ExecuteContext::new(&row).with_stored_function_invoker(Some(&invoker));
    assert_eq!(
        ExprVM::new().execute(&equality, &callbacks).unwrap(),
        Value::Boolean(true)
    );

    let ordering = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(right),
        Op::Gt,
        Op::Return,
    ]);
    assert_eq!(
        ExprVM::new().execute(&ordering, &callbacks).unwrap(),
        Value::Boolean(true)
    );
}

// =========================================================================
// Null checks tests
// =========================================================================

#[test]
fn test_is_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsNull, Op::Return]);

    // Test with null
    let row = Row::from_values(vec![Value::Null(DataType::Integer)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Test with non-null
    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_is_not_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsNotNull, Op::Return]);

    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Null(DataType::Integer)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_is_distinct_from() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadColumn(1),
        Op::IsDistinctFrom,
        Op::Return,
    ]);

    // Two nulls are NOT distinct
    let row = Row::from_values(vec![
        Value::Null(DataType::Integer),
        Value::Null(DataType::Integer),
    ]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    // Null and non-null ARE distinct
    let row = Row::from_values(vec![Value::Null(DataType::Integer), Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Same values are NOT distinct
    let row = Row::from_values(vec![Value::Integer(5), Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    // Different values ARE distinct
    let row = Row::from_values(vec![Value::Integer(5), Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_is_not_distinct_from() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadColumn(1),
        Op::IsNotDistinctFrom,
        Op::Return,
    ]);

    // Two nulls ARE "not distinct"
    let row = Row::from_values(vec![
        Value::Null(DataType::Integer),
        Value::Null(DataType::Integer),
    ]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Same values ARE "not distinct"
    let row = Row::from_values(vec![Value::Integer(5), Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

// =========================================================================
// Fused comparison operations tests
// =========================================================================

#[test]
fn test_eq_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::EqColumnConst(0, Value::Integer(42)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(42)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(100)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_ne_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::NeColumnConst(0, Value::Integer(42)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(100)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_lt_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LtColumnConst(0, Value::Integer(10)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(15)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_le_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LeColumnConst(0, Value::Integer(10)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_gt_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::GtColumnConst(0, Value::Integer(10)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(15)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_ge_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::GeColumnConst(0, Value::Integer(10)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_is_null_column() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::IsNullColumn(0), Op::Return]);

    let row = Row::from_values(vec![Value::Null(DataType::Integer)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(1)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_is_not_null_column() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::IsNotNullColumn(0), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(1)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_between_column_const() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::BetweenColumnConst(0, Value::Integer(5), Value::Integer(15)),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(20)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    // Edge case: value equals lower bound
    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Edge case: value equals upper bound
    let row = Row::from_values(vec![Value::Integer(15)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

// =========================================================================
// Logical operations tests
// =========================================================================

#[test]
fn test_or_short_circuit() {
    let mut vm = ExprVM::new();
    // WHERE col0 = 1 OR col1 = 2
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(Value::Integer(1)),
        Op::Eq,
        Op::Or(8), // Jump if first condition is true
        Op::LoadColumn(1),
        Op::LoadConst(Value::Integer(2)),
        Op::Eq,
        Op::OrFinalize,
        Op::Return,
    ]);

    // First condition true
    let row = Row::from_values(vec![Value::Integer(1), Value::Integer(0)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Second condition true
    let row = Row::from_values(vec![Value::Integer(0), Value::Integer(2)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Both conditions false
    let row = Row::from_values(vec![Value::Integer(0), Value::Integer(0)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_not() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(true)),
        Op::Not,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_xor() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(true)),
        Op::LoadConst(Value::Boolean(false)),
        Op::Xor,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    // Both true = false
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(true)),
        Op::LoadConst(Value::Boolean(true)),
        Op::Xor,
        Op::Return,
    ]);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// Arithmetic operations tests
// =========================================================================

#[test]
fn test_add() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(10)),
        Op::LoadConst(Value::Integer(5)),
        Op::Add,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(15));
}

#[test]
fn test_add_floats() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Float(2.5)),
        Op::LoadConst(Value::Float(3.5)),
        Op::Add,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Float(6.0));
}

#[test]
fn test_sub() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(10)),
        Op::LoadConst(Value::Integer(3)),
        Op::Sub,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(7));
}

#[test]
fn test_mul() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(6)),
        Op::LoadConst(Value::Integer(7)),
        Op::Mul,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(42));
}

#[test]
fn test_div() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(20)),
        Op::LoadConst(Value::Integer(4)),
        Op::Div,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(5));
}

#[test]
fn test_mod() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(17)),
        Op::LoadConst(Value::Integer(5)),
        Op::Mod,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(2));
}

#[test]
fn test_neg() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadConst(Value::Integer(5)), Op::Neg, Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(-5));
}

// =========================================================================
// Bitwise operations tests
// =========================================================================

#[test]
fn test_bit_and() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(0b1100)),
        Op::LoadConst(Value::Integer(0b1010)),
        Op::BitAnd,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(0b1000));
}

#[test]
fn test_bit_or() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(0b1100)),
        Op::LoadConst(Value::Integer(0b1010)),
        Op::BitOr,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(0b1110));
}

#[test]
fn test_bit_xor() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(0b1100)),
        Op::LoadConst(Value::Integer(0b1010)),
        Op::BitXor,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(0b0110));
}

#[test]
fn test_bit_not() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(0b0000_0000_0000_0101)),
        Op::BitNot,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    // Bitwise NOT of 5 is -6 in two's complement
    assert_eq!(result, Value::Integer(!5));
}

#[test]
fn test_shl() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(1)),
        Op::LoadConst(Value::Integer(4)),
        Op::Shl,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(16));
}

#[test]
fn test_shr() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(16)),
        Op::LoadConst(Value::Integer(2)),
        Op::Shr,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(4));
}

// =========================================================================
// String operations tests
// =========================================================================

#[test]
fn test_concat() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Text("Hello".into())),
        Op::LoadConst(Value::Text(" World".into())),
        Op::Concat,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(
        vm.execute(&program, &ctx).unwrap(),
        Value::Text("Hello World".into())
    );
}

#[test]
fn test_like_pattern() {
    use super::super::ops::CompiledPattern;

    let mut vm = ExprVM::new();
    let pattern = CompiledPattern::compile("%world%", false).unwrap();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::Like(Arc::new(pattern), false),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Text("hello world!".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Text("hello there!".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_glob_pattern() {
    use super::super::ops::CompiledPattern;

    let mut vm = ExprVM::new();
    let pattern = CompiledPattern::compile_glob("*.txt").unwrap();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::Glob(Arc::new(pattern)),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Text("document.txt".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Text("document.pdf".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// Between tests
// =========================================================================

#[test]
fn test_between() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(15)),
        Op::Between,
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(3)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_not_between() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(15)),
        Op::NotBetween,
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(3)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// Not In Set tests
// =========================================================================

#[test]
fn test_not_in_set() {
    let mut vm = ExprVM::new();
    let set: ValueSet = [Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        .into_iter()
        .collect();

    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::NotInSet(CompactArc::new(set), false),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(2)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_in_set_with_null() {
    let mut vm = ExprVM::new();
    let set: ValueSet = [Value::Integer(1), Value::Integer(2)].into_iter().collect();

    // has_null = true means the set conceptually contains NULL
    let program = Program::new(vec![
        Op::LoadColumn(0),
        Op::InSet(CompactArc::new(set), true),
        Op::Return,
    ]);

    // Value not in set, but set has null -> returns NULL
    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    assert!(result.is_null());
}

// =========================================================================
// In Set Column (fused) tests
// =========================================================================

#[test]
fn test_in_set_column() {
    let mut vm = ExprVM::new();
    let set: ValueSet = [Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        .into_iter()
        .collect();

    let program = Program::new(vec![
        Op::InSetColumn(0, CompactArc::new(set), false),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(2)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Integer(5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// Boolean checks (IS TRUE, IS FALSE, etc.)
// =========================================================================

#[test]
fn test_is_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsTrue, Op::Return]);

    let row = Row::from_values(vec![Value::Boolean(true)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Boolean(false)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    let row = Row::from_values(vec![Value::Null(DataType::Boolean)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_is_not_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsNotTrue, Op::Return]);

    let row = Row::from_values(vec![Value::Boolean(true)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    let row = Row::from_values(vec![Value::Boolean(false)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Null(DataType::Boolean)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_is_false() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsFalse, Op::Return]);

    let row = Row::from_values(vec![Value::Boolean(false)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Boolean(true)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_is_not_false() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadColumn(0), Op::IsNotFalse, Op::Return]);

    let row = Row::from_values(vec![Value::Boolean(false)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));

    let row = Row::from_values(vec![Value::Boolean(true)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Null(DataType::Boolean)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

// =========================================================================
// Coalesce, NullIf, Greatest, Least tests
// =========================================================================

#[test]
fn test_coalesce() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadNull(DataType::Integer),
        Op::LoadNull(DataType::Integer),
        Op::LoadConst(Value::Integer(42)),
        Op::Coalesce(3),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(42));
}

#[test]
fn test_nullif() {
    let mut vm = ExprVM::new();
    // NULLIF(5, 5) -> NULL
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(5)),
        Op::NullIf,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute(&program, &ctx).unwrap().is_null());

    // NULLIF(5, 10) -> 5
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(5)),
        Op::LoadConst(Value::Integer(10)),
        Op::NullIf,
        Op::Return,
    ]);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(5));
}

#[test]
fn test_greatest() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(3)),
        Op::LoadConst(Value::Integer(7)),
        Op::LoadConst(Value::Integer(2)),
        Op::Greatest(3),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(7));
}

#[test]
fn test_least() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(3)),
        Op::LoadConst(Value::Integer(7)),
        Op::LoadConst(Value::Integer(2)),
        Op::Least(3),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(2));
}

// =========================================================================
// Stack operations tests
// =========================================================================

#[test]
fn test_dup() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(42)),
        Op::Dup,
        Op::Add, // 42 + 42
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(84));
}

#[test]
fn test_swap() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(10)),
        Op::LoadConst(Value::Integer(3)),
        Op::Swap,
        Op::Sub, // 3 - 10
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(-7));
}

#[test]
fn test_pop() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(100)),
        Op::LoadConst(Value::Integer(42)),
        Op::Pop, // Discard 42
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(100));
}

// =========================================================================
// Jump operations tests
// =========================================================================

#[test]
fn test_jump() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(1)),
        Op::Jump(3),
        Op::LoadConst(Value::Integer(2)), // Skipped
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

#[test]
fn test_jump_if_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(true)),
        Op::JumpIfTrue(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

#[test]
fn test_jump_if_false() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(false)),
        Op::JumpIfFalse(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

#[test]
fn test_jump_if_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadNull(DataType::Integer),
        Op::JumpIfNull(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

#[test]
fn test_jump_if_not_null() {
    let mut vm = ExprVM::new();
    // Test 1: Non-null value should jump
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(42)),
        Op::JumpIfNotNull(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));

    // Test 2: Null value should NOT jump
    let program2 = Program::new(vec![
        Op::LoadNull(DataType::Integer),
        Op::JumpIfNotNull(4),
        Op::LoadConst(Value::Integer(99)), // Executed (no jump)
        Op::Return,
        Op::LoadConst(Value::Integer(0)), // Not reached
        Op::Return,
    ]);
    assert_eq!(vm.execute(&program2, &ctx).unwrap(), Value::Integer(99));
}

#[test]
fn test_coalesce_short_circuit() {
    // Test short-circuit COALESCE compilation pattern:
    // COALESCE(NULL, NULL, 42) should return 42 without evaluating further
    let mut vm = ExprVM::new();

    // Simulates: COALESCE(NULL, NULL, 42)
    // Bytecode: LoadNull, JumpIfNotNull(end), Pop, LoadNull, JumpIfNotNull(end), Pop, LoadConst(42)
    let program = Program::new(vec![
        Op::LoadNull(DataType::Integer),   // arg1: NULL
        Op::JumpIfNotNull(8),              // if not null, jump to end (position 8)
        Op::Pop,                           // pop the null
        Op::LoadNull(DataType::Integer),   // arg2: NULL
        Op::JumpIfNotNull(8),              // if not null, jump to end
        Op::Pop,                           // pop the null
        Op::LoadConst(Value::Integer(42)), // arg3: 42
        Op::Return,                        // end
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(42));

    // Test COALESCE(100, NULL, 42) - first non-null wins
    let program2 = Program::new(vec![
        Op::LoadConst(Value::Integer(100)), // arg1: 100 (not null)
        Op::JumpIfNotNull(8),               // jump to end
        Op::Pop,
        Op::LoadNull(DataType::Integer), // arg2: NULL (skipped)
        Op::JumpIfNotNull(8),
        Op::Pop,
        Op::LoadConst(Value::Integer(42)), // arg3: 42 (skipped)
        Op::Return,
    ]);
    assert_eq!(vm.execute(&program2, &ctx).unwrap(), Value::Integer(100));

    // Test COALESCE(NULL, 50, 42) - second wins
    let program3 = Program::new(vec![
        Op::LoadNull(DataType::Integer),   // arg1: NULL
        Op::JumpIfNotNull(8),              // no jump (is null)
        Op::Pop,                           // pop the null
        Op::LoadConst(Value::Integer(50)), // arg2: 50 (not null)
        Op::JumpIfNotNull(8),              // jump to end
        Op::Pop,
        Op::LoadConst(Value::Integer(42)), // arg3: 42 (skipped)
        Op::Return,
    ]);
    assert_eq!(vm.execute(&program3, &ctx).unwrap(), Value::Integer(50));
}

// =========================================================================
// Return variants tests
// =========================================================================

#[test]
fn test_return_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::ReturnTrue]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));
}

#[test]
fn test_return_false() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::ReturnFalse]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

#[test]
fn test_return_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::ReturnNull(DataType::Text)]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    assert!(result.is_null());
}

// =========================================================================
// Nop tests
// =========================================================================

#[test]
fn test_nop() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(42)),
        Op::Nop,
        Op::Nop,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(42));
}

// =========================================================================
// Empty program tests
// =========================================================================

#[test]
fn test_empty_program() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    assert!(result.is_null());
}

// =========================================================================
// execute_bool tests
// =========================================================================

#[test]
fn test_execute_bool_simple() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::GtColumnConst(0, Value::Integer(5)), Op::Return]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute_bool(&program, &ctx).unwrap());

    let row = Row::from_values(vec![Value::Integer(3)]);
    let ctx = ExecuteContext::new(&row);
    assert!(!vm.execute_bool(&program, &ctx).unwrap());
}

#[test]
fn test_execute_bool_with_null() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::GtColumnConst(0, Value::Integer(5)), Op::Return]);

    // Null comparison returns false (not true)
    let row = Row::from_values(vec![Value::Null(DataType::Integer)]);
    let ctx = ExecuteContext::new(&row);
    assert!(!vm.execute_bool(&program, &ctx).unwrap());
}

#[test]
fn test_execute_bool_between() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::BetweenColumnConst(0, Value::Integer(5), Value::Integer(15)),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Integer(10)]);
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute_bool(&program, &ctx).unwrap());

    let row = Row::from_values(vec![Value::Integer(20)]);
    let ctx = ExecuteContext::new(&row);
    assert!(!vm.execute_bool(&program, &ctx).unwrap());
}

#[test]
fn test_execute_bool_is_null_column() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::IsNullColumn(0), Op::Return]);

    let row = Row::from_values(vec![Value::Null(DataType::Integer)]);
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute_bool(&program, &ctx).unwrap());

    let row = Row::from_values(vec![Value::Integer(1)]);
    let ctx = ExecuteContext::new(&row);
    assert!(!vm.execute_bool(&program, &ctx).unwrap());
}

#[test]
fn test_execute_bool_load_const_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadConst(Value::Boolean(true)), Op::Return]);

    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert!(vm.execute_bool(&program, &ctx).unwrap());
}

// =========================================================================
// Cast tests
// =========================================================================

#[test]
fn test_cast_int_to_float() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(42)),
        Op::Cast(DataType::Float),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Float(42.0));
}

#[test]
fn test_cast_float_to_int() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Float(42.7)),
        Op::Cast(DataType::Integer),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(42));
}

#[test]
fn test_cast_text_to_int() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Text("123".into())),
        Op::Cast(DataType::Integer),
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(123));
}

// =========================================================================
// LikeColumn (fused) tests
// =========================================================================

#[test]
fn test_like_column() {
    use super::super::ops::CompiledPattern;

    let mut vm = ExprVM::new();
    let pattern = CompiledPattern::compile("test%", false).unwrap();
    let program = Program::new(vec![
        Op::LikeColumn(0, Arc::new(pattern), false),
        Op::Return,
    ]);

    let row = Row::from_values(vec![Value::Text("testing".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Text("other".into())]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// TruncateToDate tests
// =========================================================================

#[test]
fn test_truncate_to_date() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Text("2024-01-15 14:30:00".into())),
        Op::TruncateToDate,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    // TruncateToDate returns a Timestamp with time set to 00:00:00
    if let Value::Timestamp(t) = result {
        use chrono::Datelike;
        assert_eq!(t.year(), 2024);
        assert_eq!(t.month(), 1);
        assert_eq!(t.day(), 15);
    } else {
        panic!("Expected Timestamp result, got {:?}", result);
    }
}

// =========================================================================
// Pop/Jump combinations
// =========================================================================

#[test]
fn test_pop_jump_if_true() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(true)),
        Op::PopJumpIfTrue(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

#[test]
fn test_pop_jump_if_false() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Boolean(false)),
        Op::PopJumpIfFalse(4),
        Op::LoadConst(Value::Integer(0)), // Skipped
        Op::Return,
        Op::LoadConst(Value::Integer(1)), // Executed
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(1));
}

// =========================================================================
// Mixed type arithmetic
// =========================================================================

#[test]
fn test_add_int_and_float() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(10)),
        Op::LoadConst(Value::Float(2.5)),
        Op::Add,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Float(12.5));
}

// =========================================================================
// String concatenation with non-strings
// =========================================================================

#[test]
fn test_concat_int_to_string() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![
        Op::LoadConst(Value::Text("Value: ".into())),
        Op::LoadConst(Value::Integer(42)),
        Op::Concat,
        Op::Return,
    ]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    assert_eq!(
        vm.execute(&program, &ctx).unwrap(),
        Value::Text("Value: 42".into())
    );
}

// =========================================================================
// Float comparisons
// =========================================================================

#[test]
fn test_gt_column_const_float() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::GtColumnConst(0, Value::Float(2.5)), Op::Return]);

    let row = Row::from_values(vec![Value::Float(3.5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(true));

    let row = Row::from_values(vec![Value::Float(1.5)]);
    let ctx = ExecuteContext::new(&row);
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Boolean(false));
}

// =========================================================================
// Test LoadTransactionId
// =========================================================================

#[test]
fn test_load_transaction_id() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadTransactionId, Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row).with_transaction_id(Some(12345));
    assert_eq!(vm.execute(&program, &ctx).unwrap(), Value::Integer(12345));
}

#[test]
fn test_load_transaction_id_none() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadTransactionId, Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row);
    let result = vm.execute(&program, &ctx).unwrap();
    assert!(result.is_null());
}

#[test]
fn transaction_id_outside_sql_integer_domain_is_rejected() {
    let mut vm = ExprVM::new();
    let program = Program::new(vec![Op::LoadTransactionId, Op::Return]);
    let row = Row::new();
    let ctx = ExecuteContext::new(&row).with_transaction_id(Some(i64::MAX as u64 + 1));
    assert!(vm.execute(&program, &ctx).is_err());
}

#[test]
fn cow_fallback_selects_interpreter_before_observable_prefix() {
    COW_SCALAR_CALLS.store(0, AtomicOrdering::SeqCst);
    let program = Program::new(vec![
        Op::LoadConst(Value::Integer(40)),
        Op::CallScalar {
            func: Arc::new(CountingScalar),
            arg_count: 1,
        },
        Op::LoadConst(Value::Integer(2)),
        Op::Add,
        Op::Return,
    ]);
    let row = Row::new();
    let context = ExecuteContext::new(&row);
    let mut vm = ExprVM::new();
    assert_eq!(
        vm.execute_cow(&program, &context).unwrap(),
        Value::Integer(42)
    );
    assert_eq!(COW_SCALAR_CALLS.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn boolean_execution_propagates_runtime_errors() {
    let program = Program::new(vec![
        Op::CallScalar {
            func: Arc::new(FailingScalar),
            arg_count: 0,
        },
        Op::Return,
    ]);
    let row = Row::new();
    let context = ExecuteContext::new(&row);
    assert!(ExprVM::new().execute_bool(&program, &context).is_err());

    let wrong_type = Program::new(vec![
        Op::LoadConst(Value::Text("not a predicate".into())),
        Op::Return,
    ]);
    assert!(ExprVM::new().execute_bool(&wrong_type, &context).is_err());
}
