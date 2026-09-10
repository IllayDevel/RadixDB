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

// Expression Virtual Machine
//
// The VM executes compiled Programs against row data.
// Design goals:
// - Zero allocation in hot path
// - Linear instruction dispatch
// - Reusable across rows (clear() between uses)
// - No recursion

use std::borrow::Cow;
use std::sync::Arc;

use smallvec::SmallVec;

use super::execution_context::ExecuteContext;
use super::ops::{CompiledPattern, Op};
use super::program::Program;
use radixdb_core::SmartString;
use radixdb_core::{DataType, Error, Result, Value, NULL_VALUE};

/// Stack value that can be borrowed (from row/constants) or owned (from operations)
type StackValue<'a> = Cow<'a, Value>;

/// Stack capacity for inline storage (avoids heap allocation for simple expressions)
/// Most expressions need 4-8 stack slots, so 8 covers the common case.
const STACK_INLINE_CAPACITY: usize = 16;

/// Arithmetic operation type (used for safe wrapping operations)
#[derive(Clone, Copy)]
#[allow(dead_code)]
enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Expression Virtual Machine
///
/// Executes compiled Programs against row data.
/// The VM is reusable - call execute() with different contexts.
/// Capacity for reusable args buffer (most functions have <= 4 args)
const ARGS_BUFFER_CAPACITY: usize = 8;

/// Parsed interval value: either a fixed-length duration or a calendar-relative month count.
/// Months/years require calendar-aware arithmetic (leap years, variable month lengths).
enum IntervalValue {
    Duration(chrono::Duration),
    Months(i64),
}

pub struct ExprVM {
    /// Evaluation stack (reused between executions)
    /// Uses SmallVec to avoid heap allocation for simple expressions (stack depth <= 16)
    stack: SmallVec<[Value; STACK_INLINE_CAPACITY]>,

    /// Reusable buffer for function arguments (avoids allocation per call)
    /// Uses SmallVec to avoid heap allocation for functions with <= 8 args
    args_buffer: SmallVec<[Value; ARGS_BUFFER_CAPACITY]>,

    /// Cache for dynamic LIKE patterns (avoids recompilation per row)
    /// Stores (pattern_string, case_insensitive, escape_char, compiled_pattern)
    cached_like: Option<(SmartString, bool, Option<char>, CompiledPattern)>,

    /// Cache for dynamic GLOB patterns (separate from LIKE to avoid cross-contamination)
    /// Stores (pattern_string, compiled_pattern)
    cached_glob: Option<(SmartString, CompiledPattern)>,

    /// Cache for dynamic REGEXP patterns (avoids recompilation per row)
    /// Stores (pattern_string, compiled_regex)
    cached_regexp: Option<(SmartString, regex::Regex)>,
}

impl ExprVM {
    /// Create a new VM with default stack capacity
    /// Uses inline storage for up to 16 stack values and 8 args (no heap allocation)
    pub fn new() -> Self {
        Self {
            stack: SmallVec::new(),
            args_buffer: SmallVec::new(),
            cached_like: None,
            cached_glob: None,
            cached_regexp: None,
        }
    }

    /// Create a VM with specific stack capacity
    /// If capacity > 16, will spill to heap when needed
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            stack: SmallVec::with_capacity(capacity),
            args_buffer: SmallVec::new(),
            cached_like: None,
            cached_glob: None,
            cached_regexp: None,
        }
    }

    /// Execute a program and return the result
    #[inline]
    pub fn execute(&mut self, program: &Program, ctx: &ExecuteContext) -> Result<Value> {
        // Ensure stack has enough capacity
        if self.stack.capacity() < program.max_stack_depth() {
            self.stack
                .reserve(program.max_stack_depth() - self.stack.capacity());
        }
        self.stack.clear();

        let ops = program.ops();
        if ops.is_empty() {
            return Ok(Value::null_unknown());
        }

        let mut pc: usize = 0;

        // Main execution loop
        loop {
            if pc >= ops.len() {
                break;
            }

            match &ops[pc] {
                // =============================================================
                // LOAD OPERATIONS
                // =============================================================
                Op::LoadColumn(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .row
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadColumn2(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .row2
                        .and_then(|r| r.get(idx).cloned())
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadOuterColumn(name) => {
                    let value = ctx
                        .outer_row
                        .and_then(|r| r.get(name.as_ref()).cloned())
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadConst(value) => {
                    self.stack.push(value.clone());
                    pc += 1;
                }

                Op::LoadParam(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .params
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadNamedParam(name) => {
                    let value = ctx
                        .named_params
                        .and_then(|p| p.get(name.as_ref()).cloned())
                        .or_else(|| {
                            // DEFAULT evaluation and catalog recovery compile
                            // scalar expressions without a full statement
                            // context. CURRENT_TIMESTAMP must remain a valid
                            // system value there instead of degrading to NULL;
                            // ordinary statement execution supplies one stable
                            // value through named_params.
                            (name.as_ref() == "CURRENT_STATEMENT_TIMESTAMP").then(|| {
                                Value::timestamp(
                                    radixdb_core::time_compat::system_time_now().into(),
                                )
                            })
                        })
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadNull(dt) => {
                    self.stack.push(Value::Null(*dt));
                    pc += 1;
                }

                Op::LoadAggregateResult(idx) => {
                    // Aggregate results are stored in the row at specific indices
                    let idx = *idx as usize;
                    let value = ctx
                        .row
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(Value::null_unknown);
                    self.stack.push(value);
                    pc += 1;
                }

                Op::LoadTransactionId => {
                    // Load current transaction ID, or NULL if not in a transaction
                    let value = match ctx.transaction_id {
                        Some(txn_id) => Value::Integer(i64::try_from(txn_id).map_err(|_| {
                            radixdb_core::Error::invalid_argument(
                                "transaction ID exceeds the SQL INTEGER domain",
                            )
                        })?),
                        None => Value::null_unknown(),
                    };
                    self.stack.push(value);
                    pc += 1;
                }

                // =============================================================
                // COMPARISON OPERATIONS
                // =============================================================
                Op::Eq => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_equality_result(ctx, &a, &b, false)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Ne => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_equality_result(ctx, &a, &b, true)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Lt => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_order_result(ctx, &a, &b, |ordering| {
                        ordering == std::cmp::Ordering::Less
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Le => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_order_result(ctx, &a, &b, |ordering| {
                        ordering != std::cmp::Ordering::Greater
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Gt => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_order_result(ctx, &a, &b, |ordering| {
                        ordering == std::cmp::Ordering::Greater
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Ge => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_order_result(ctx, &a, &b, |ordering| {
                        ordering != std::cmp::Ordering::Less
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::IsNull => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    self.stack.push(Value::Boolean(v.is_null()));
                    pc += 1;
                }

                Op::IsNotNull => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    self.stack.push(Value::Boolean(!v.is_null()));
                    pc += 1;
                }

                Op::IsDistinctFrom => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    // NULL IS DISTINCT FROM NULL = FALSE
                    // NULL IS DISTINCT FROM non-NULL = TRUE
                    // non-NULL IS DISTINCT FROM NULL = TRUE
                    // Otherwise use regular comparison
                    let result = match (a.is_null(), b.is_null()) {
                        (true, true) => false,
                        (true, false) | (false, true) => true,
                        (false, false) => !Self::sql_values_equal(ctx, &a, &b)?,
                    };
                    self.stack.push(Value::Boolean(result));
                    pc += 1;
                }

                Op::IsNotDistinctFrom => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (a.is_null(), b.is_null()) {
                        (true, true) => true,
                        (true, false) | (false, true) => false,
                        (false, false) => Self::sql_values_equal(ctx, &a, &b)?,
                    };
                    self.stack.push(Value::Boolean(result));
                    pc += 1;
                }

                // =============================================================
                // FUSED COMPARISON OPERATIONS
                // Single instruction for column vs constant (avoids push/pop)
                // =============================================================
                Op::EqColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_equality_result(ctx, col_val, val, false)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::NeColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_equality_result(ctx, col_val, val, true)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::LtColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering == std::cmp::Ordering::Less
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::LeColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering != std::cmp::Ordering::Greater
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::GtColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering == std::cmp::Ordering::Greater
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::GeColumnConst(idx, val) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering != std::cmp::Ordering::Less
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::IsNullColumn(idx) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    self.stack.push(Value::Boolean(col_val.is_null()));
                    pc += 1;
                }

                Op::IsNotNullColumn(idx) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    self.stack.push(Value::Boolean(!col_val.is_null()));
                    pc += 1;
                }

                Op::LikeColumn(idx, pattern, case_insensitive) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = match col_val {
                        Value::Text(s) => Value::Boolean(pattern.matches(s, *case_insensitive)),
                        Value::Null(_) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::InSetColumn(idx, set, has_null) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = if col_val.is_null() {
                        Value::Null(DataType::Boolean)
                    } else if Self::sql_set_contains(ctx, set, col_val)? {
                        Value::Boolean(true)
                    } else if *has_null {
                        Value::Null(DataType::Boolean)
                    } else {
                        Value::Boolean(false)
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::BetweenColumnConst(idx, low, high) => {
                    let col_val = ctx
                        .row
                        .get(*idx as usize)
                        .unwrap_or(&Value::Null(DataType::Null));
                    let result = Self::sql_between_result(ctx, col_val, low, high, false)?;
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // LOGICAL OPERATIONS
                // =============================================================
                Op::And(jump_target) => {
                    // Short-circuit AND: if top is false, jump
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    match top {
                        Value::Boolean(false) => {
                            // Result is false, jump to target
                            pc = *jump_target as usize;
                        }
                        Value::Null(_) => {
                            // Need to evaluate right side to check for false
                            pc += 1;
                        }
                        _ => {
                            // True or truthy, continue to evaluate right side
                            pc += 1;
                        }
                    }
                }

                Op::Or(jump_target) => {
                    // Short-circuit OR: if top is true, jump
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    match top {
                        Value::Boolean(true) => {
                            // Result is true, jump to target
                            pc = *jump_target as usize;
                        }
                        Value::Null(_) => {
                            // Need to evaluate right side to check for true
                            pc += 1;
                        }
                        _ => {
                            // False or falsy, continue to evaluate right side
                            pc += 1;
                        }
                    }
                }

                Op::AndFinalize => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (Self::to_tribool(&a), Self::to_tribool(&b)) {
                        (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
                        (Some(true), Some(true)) => Value::Boolean(true),
                        _ => Value::Null(DataType::Boolean),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::OrFinalize => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (Self::to_tribool(&a), Self::to_tribool(&b)) {
                        (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
                        (Some(false), Some(false)) => Value::Boolean(false),
                        _ => Value::Null(DataType::Boolean),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Not => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match Self::to_tribool(&v) {
                        Some(b) => Value::Boolean(!b),
                        None => Value::Null(DataType::Boolean),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Xor => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (Self::to_tribool(&a), Self::to_tribool(&b)) {
                        (Some(a), Some(b)) => Value::Boolean(a ^ b),
                        _ => Value::Null(DataType::Boolean),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // ARITHMETIC OPERATIONS
                // =============================================================
                Op::Add => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    // Handle timestamp + interval or timestamp + integer (days)
                    let result = match (&a, &b) {
                        (Value::Timestamp(t), Value::Integer(days)) => {
                            Self::timestamp_add_days(*t, *days)?
                        }
                        (Value::Integer(days), Value::Timestamp(t)) => {
                            Self::timestamp_add_days(*t, *days)?
                        }
                        (Value::Extension(_), Value::Integer(days))
                            if a.as_date_days().is_some() =>
                        {
                            Self::date_add_days(a.as_date_days().expect("date was checked"), *days)?
                        }
                        (Value::Integer(days), Value::Extension(_))
                            if b.as_date_days().is_some() =>
                        {
                            Self::date_add_days(b.as_date_days().expect("date was checked"), *days)?
                        }
                        (Value::Timestamp(_), Value::Text(_)) => {
                            // Parse interval string - pass references directly to avoid clone
                            self.timestamp_add_interval(&a, &b, true)?
                        }
                        _ => Self::arithmetic_op(&a, &b, ArithmeticOp::Add, |x, y| x + y)?,
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Sub => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    // Handle timestamp - interval, timestamp - integer (days), or timestamp - timestamp
                    let result = match (&a, &b) {
                        (Value::Timestamp(t1), Value::Timestamp(t2)) => {
                            // Return interval text
                            let duration = t1.signed_duration_since(*t2);
                            Value::Text(SmartString::from_string(
                                self.format_duration_as_interval(duration),
                            ))
                        }
                        (Value::Timestamp(t), Value::Integer(days)) => Self::timestamp_add_days(
                            *t,
                            days.checked_neg().ok_or_else(|| {
                                Error::Type("timestamp interval overflow".to_string())
                            })?,
                        )?,
                        (Value::Extension(_), Value::Integer(days))
                            if a.as_date_days().is_some() =>
                        {
                            Self::date_add_days(
                                a.as_date_days().expect("date was checked"),
                                days.checked_neg().ok_or_else(|| {
                                    Error::Type("DATE arithmetic overflow".to_string())
                                })?,
                            )?
                        }
                        (Value::Extension(_), Value::Extension(_))
                            if a.as_date_days().is_some() && b.as_date_days().is_some() =>
                        {
                            Value::Integer(
                                i64::from(a.as_date_days().expect("date was checked"))
                                    - i64::from(b.as_date_days().expect("date was checked")),
                            )
                        }
                        (Value::Timestamp(_), Value::Text(_)) => {
                            // Parse interval string - pass references directly to avoid clone
                            self.timestamp_add_interval(&a, &b, false)?
                        }
                        _ => Self::arithmetic_op(&a, &b, ArithmeticOp::Sub, |x, y| x - y)?,
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Mul => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::arithmetic_op(&a, &b, ArithmeticOp::Mul, |x, y| x * y)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Div => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::div_op(&a, &b)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Mod => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::mod_op(&a, &b)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Neg => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Integer(i) => match i.checked_neg() {
                            Some(neg) => Value::Integer(neg),
                            None => Value::Null(DataType::Integer), // i64::MIN overflow
                        },
                        Value::Float(f) => Value::Float(-f),
                        Value::Null(dt) => Value::Null(dt),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // BITWISE OPERATIONS (inlined for performance)
                // =============================================================
                Op::BitAnd => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&a, &b) {
                        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x & y),
                        _ if a.is_null() || b.is_null() => Value::Null(DataType::Integer),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::BitOr => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&a, &b) {
                        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x | y),
                        _ if a.is_null() || b.is_null() => Value::Null(DataType::Integer),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::BitXor => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&a, &b) {
                        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x ^ y),
                        _ if a.is_null() || b.is_null() => Value::Null(DataType::Integer),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::BitNot => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Integer(i) => Value::Integer(!i),
                        Value::Null(dt) => Value::Null(dt),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Shl => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&a, &b) {
                        (Value::Integer(x), Value::Integer(y)) => {
                            Value::Integer(x.wrapping_shl(*y as u32))
                        }
                        _ if a.is_null() || b.is_null() => Value::Null(DataType::Integer),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Shr => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&a, &b) {
                        (Value::Integer(x), Value::Integer(y)) => {
                            Value::Integer(x.wrapping_shr(*y as u32))
                        }
                        _ if a.is_null() || b.is_null() => Value::Null(DataType::Integer),
                        _ => Value::Null(DataType::Null),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // STRING OPERATIONS
                // =============================================================
                Op::Concat => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if a.is_null() || b.is_null() {
                        Value::Null(DataType::Text)
                    } else {
                        // Fast path: both are Text
                        match (&a, &b) {
                            (Value::Text(a_str), Value::Text(b_str)) => {
                                // Use optimized concat - handles inline and heap efficiently
                                Value::Text(SmartString::concat(a_str, b_str))
                            }
                            (Value::Text(a_str), _) => {
                                // a is Text, b needs conversion - use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(a_str.len() + 32);
                                s.push_str(a_str);
                                let _ = write!(s, "{}", b);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                            (_, Value::Text(b_str)) => {
                                // a needs conversion, b is Text - use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(32 + b_str.len());
                                let _ = write!(s, "{}", a);
                                s.push_str(b_str);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                            _ => {
                                // Both need conversion - use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(64);
                                let _ = write!(s, "{}{}", a, b);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                        }
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::ConcatN(n) => {
                    let n = *n as usize;
                    let start = self.stack.len().saturating_sub(n);

                    // Single pass: check for NULL and calculate total length
                    let mut total_len = 0usize;
                    let mut has_null = false;
                    let mut all_text = true;

                    for v in &self.stack[start..] {
                        match v {
                            Value::Null(_) => {
                                has_null = true;
                                break;
                            }
                            Value::Text(s) => total_len += s.len(),
                            _ => {
                                all_text = false;
                                total_len += 32;
                            }
                        }
                    }

                    if has_null {
                        self.stack.truncate(start);
                        self.stack.push(Value::Null(DataType::Text));
                        pc += 1;
                        continue;
                    }

                    // Build result - optimize for inline vs heap
                    let result = if all_text && total_len <= 15 {
                        // Fast path: build directly into inline SmartString (no heap allocation)
                        let mut data = [0u8; 15];
                        let mut pos = 0;
                        for v in self.stack.drain(start..) {
                            if let Value::Text(text) = v {
                                let bytes = text.as_bytes();
                                data[pos..pos + bytes.len()].copy_from_slice(bytes);
                                pos += bytes.len();
                            }
                        }
                        let text = std::str::from_utf8(&data[..total_len])
                            .expect("concatenated Text values remain valid UTF-8");
                        SmartString::new(text)
                    } else if all_text {
                        // Heap path: exact capacity, into_boxed_str is O(1) when len == capacity
                        let mut s = String::with_capacity(total_len);
                        for v in self.stack.drain(start..) {
                            if let Value::Text(text) = v {
                                s.push_str(&text);
                            }
                        }
                        // len == capacity, so into_boxed_str is O(1)
                        SmartString::from_string(s)
                    } else {
                        // Mixed types: capacity is estimate, use Arc to avoid shrink_to_fit
                        let mut s = String::with_capacity(total_len);
                        for v in self.stack.drain(start..) {
                            match v {
                                Value::Text(text) => s.push_str(&text),
                                _ => {
                                    use std::fmt::Write;
                                    let _ = write!(s, "{}", v);
                                }
                            }
                        }
                        SmartString::from_string_shared(s)
                    };
                    self.stack.push(Value::Text(result));
                    pc += 1;
                }

                Op::Like(pattern, case_insensitive) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match &v {
                        Value::Text(s) => Value::Boolean(pattern.matches(s, *case_insensitive)),
                        Value::Null(_) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Glob(pattern) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match &v {
                        Value::Text(s) => Value::Boolean(pattern.matches(s, false)),
                        Value::Null(_) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Regexp(regex) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match &v {
                        Value::Text(s) => Value::Boolean(regex.is_match(s)),
                        Value::Null(_) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::LikeEscape(pattern, case_insensitive, _escape) => {
                    // ESCAPE is already incorporated into the compiled pattern
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match &v {
                        Value::Text(s) => Value::Boolean(pattern.matches(s, *case_insensitive)),
                        Value::Null(_) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::LikeDynamic(case_insensitive) => {
                    let pattern_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let text_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ci = *case_insensitive;
                    let result = match (&text_val, &pattern_val) {
                        (Value::Text(text), Value::Text(pat)) => {
                            // Cache compiled pattern: parameters are constant per query,
                            // so recompiling every row is wasteful
                            let need_compile = match &self.cached_like {
                                Some((cached_pat, cached_ci, cached_esc, _)) => {
                                    cached_pat.as_str() != pat.as_str()
                                        || *cached_ci != ci
                                        || cached_esc.is_some()
                                }
                                None => true,
                            };
                            if need_compile {
                                let compiled = CompiledPattern::compile(pat, ci)?;
                                self.cached_like = Some((pat.clone(), ci, None, compiled));
                            }
                            let (_, _, _, ref compiled) = self.cached_like.as_ref().unwrap();
                            Value::Boolean(compiled.matches(text, ci))
                        }
                        (Value::Null(_), _) | (_, Value::Null(_)) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::LikeDynamicEscape(case_insensitive, escape_char) => {
                    let pattern_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let text_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&text_val, &pattern_val) {
                        (Value::Text(text), Value::Text(pat)) => {
                            let ci = *case_insensitive;
                            let esc = *escape_char;
                            let need_compile = match &self.cached_like {
                                Some((cached_pat, cached_ci, cached_esc, _)) => {
                                    cached_pat.as_str() != pat.as_str()
                                        || *cached_ci != ci
                                        || *cached_esc != Some(esc)
                                }
                                None => true,
                            };
                            if need_compile {
                                // Pre-process the escape character in the pattern at runtime,
                                // converting e.g. !% -> \% so CompiledPattern treats it as literal
                                let processed = process_like_escape_runtime(pat, esc);
                                let compiled = CompiledPattern::compile(&processed, ci)?;
                                self.cached_like = Some((pat.clone(), ci, Some(esc), compiled));
                            }
                            let (_, _, _, ref compiled) = self.cached_like.as_ref().unwrap();
                            Value::Boolean(compiled.matches(text, ci))
                        }
                        (Value::Null(_), _) | (_, Value::Null(_)) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::GlobDynamic => {
                    let pattern_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let text_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&text_val, &pattern_val) {
                        (Value::Text(text), Value::Text(pat)) => {
                            let need_compile = match &self.cached_glob {
                                Some((cached_pat, _)) => cached_pat.as_str() != pat.as_str(),
                                None => true,
                            };
                            if need_compile {
                                let compiled = CompiledPattern::compile_glob(pat)?;
                                self.cached_glob = Some((pat.clone(), compiled));
                            }
                            let (_, ref compiled) = self.cached_glob.as_ref().unwrap();
                            Value::Boolean(compiled.matches(text, false))
                        }
                        (Value::Null(_), _) | (_, Value::Null(_)) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::RegexpDynamic => {
                    let pattern_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let text_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&text_val, &pattern_val) {
                        (Value::Text(text), Value::Text(pat)) => {
                            let need_compile = match &self.cached_regexp {
                                Some((cached_pat, _)) => cached_pat.as_str() != pat.as_str(),
                                None => true,
                            };
                            if need_compile {
                                match regex::Regex::new(pat) {
                                    Ok(re) => {
                                        self.cached_regexp = Some((pat.clone(), re));
                                    }
                                    Err(e) => {
                                        self.cached_regexp = None;
                                        // Return error for invalid regex, matching literal
                                        // REGEXP behavior where bad patterns fail at compile time
                                        return Err(radixdb_core::Error::invalid_argument(
                                            format!("Invalid regular expression '{}': {}", pat, e),
                                        ));
                                    }
                                }
                            }
                            let (_, ref re) = self.cached_regexp.as_ref().unwrap();
                            Value::Boolean(re.is_match(text))
                        }
                        (Value::Null(_), _) | (_, Value::Null(_)) => Value::Null(DataType::Boolean),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // JSON OPERATIONS
                // =============================================================
                Op::JsonAccess => {
                    let key = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let json_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = self.json_access(&json_val, &key, false);
                    self.stack.push(result);
                    pc += 1;
                }

                Op::JsonAccessText => {
                    let key = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let json_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = self.json_access(&json_val, &key, true);
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // TIMESTAMP OPERATIONS
                // =============================================================
                Op::TimestampAddInterval => {
                    let interval = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ts = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = self.timestamp_add_interval(&ts, &interval, true)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::TimestampSubInterval => {
                    let interval = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ts = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = self.timestamp_add_interval(&ts, &interval, false)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::TimestampDiff => {
                    let ts2 = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ts1 = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&ts1, &ts2) {
                        (Value::Timestamp(t1), Value::Timestamp(t2)) => {
                            let duration = t1.signed_duration_since(*t2);
                            Value::Text(SmartString::from_string(
                                self.format_duration_as_interval(duration),
                            ))
                        }
                        _ if ts1.is_null() || ts2.is_null() => Value::Null(DataType::Text),
                        _ => Value::Null(DataType::Text),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::TimestampAddDays => {
                    let days = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ts = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&ts, &days) {
                        (Value::Timestamp(t), Value::Integer(d)) => {
                            Value::Timestamp(*t + chrono::Duration::days(*d))
                        }
                        (Value::Extension(_), Value::Integer(d)) if ts.as_date_days().is_some() => {
                            Self::date_add_days(ts.as_date_days().expect("date was checked"), *d)?
                        }
                        _ if ts.is_null() || days.is_null() => Value::Null(DataType::Timestamp),
                        _ => Value::Null(DataType::Timestamp),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::TimestampSubDays => {
                    let days = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let ts = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match (&ts, &days) {
                        (Value::Timestamp(t), Value::Integer(d)) => {
                            Value::Timestamp(*t - chrono::Duration::days(*d))
                        }
                        (Value::Extension(_), Value::Integer(d)) if ts.as_date_days().is_some() => {
                            Self::date_add_days(
                                ts.as_date_days().expect("date was checked"),
                                d.checked_neg().ok_or_else(|| {
                                    Error::Type("DATE arithmetic overflow".to_string())
                                })?,
                            )?
                        }
                        _ if ts.is_null() || days.is_null() => Value::Null(DataType::Timestamp),
                        _ => Value::Null(DataType::Timestamp),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // SET OPERATIONS
                // =============================================================
                Op::InSet(set, has_null) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if v.is_null() {
                        Value::Null(DataType::Boolean)
                    } else if Self::sql_set_contains(ctx, set, &v)? {
                        Value::Boolean(true)
                    } else if *has_null {
                        Value::Null(DataType::Boolean)
                    } else {
                        Value::Boolean(false)
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::NotInSet(set, has_null) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if v.is_null() {
                        Value::Null(DataType::Boolean)
                    } else if Self::sql_set_contains(ctx, set, &v)? {
                        Value::Boolean(false)
                    } else if *has_null {
                        Value::Null(DataType::Boolean)
                    } else {
                        Value::Boolean(true)
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Between => {
                    let high = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let low = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_between_result(ctx, &val, &low, &high, false)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::NotBetween => {
                    let high = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let low = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = Self::sql_between_result(ctx, &val, &low, &high, true)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::InTupleSet {
                    tuple_size,
                    values,
                    negated,
                } => {
                    let tuple_size = *tuple_size as usize;
                    let start = self.stack.len().saturating_sub(tuple_size);

                    // Reuse args_buffer to avoid allocation
                    self.args_buffer.clear();
                    self.args_buffer.extend(self.stack.drain(start..));

                    // Check if any values are NULL
                    let has_null_in_tuple = self.args_buffer.iter().any(|v| v.is_null());

                    if has_null_in_tuple {
                        // NULL in tuple -> result is NULL
                        self.stack.push(Value::Null(DataType::Boolean));
                    } else {
                        // Check membership
                        let mut found = false;
                        for tuple in values.iter() {
                            if tuple.len() != self.args_buffer.len() {
                                continue;
                            }
                            let mut equal = true;
                            for (left, right) in tuple.iter().zip(self.args_buffer.iter()) {
                                if !Self::sql_values_equal(ctx, left, right)? {
                                    equal = false;
                                    break;
                                }
                            }
                            if equal {
                                found = true;
                                break;
                            }
                        }

                        let result = if *negated { !found } else { found };
                        self.stack.push(Value::Boolean(result));
                    }
                    pc += 1;
                }

                // =============================================================
                // BOOLEAN CHECKS
                // =============================================================
                Op::IsTrue => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Boolean(b) => Value::Boolean(b),
                        Value::Null(_) => Value::Boolean(false),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::IsNotTrue => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Boolean(b) => Value::Boolean(!b),
                        Value::Null(_) => Value::Boolean(true),
                        _ => Value::Boolean(true),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::IsFalse => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Boolean(b) => Value::Boolean(!b),
                        Value::Null(_) => Value::Boolean(false),
                        _ => Value::Boolean(false),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::IsNotFalse => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match v {
                        Value::Boolean(b) => Value::Boolean(b),
                        Value::Null(_) => Value::Boolean(true),
                        _ => Value::Boolean(true),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // FUNCTION CALLS
                // =============================================================
                Op::CallScalar { func, arg_count } => {
                    let arg_count = *arg_count as usize;
                    let start = self.stack.len().saturating_sub(arg_count);

                    // Reuse args_buffer to avoid allocation
                    self.args_buffer.clear();
                    self.args_buffer.extend(self.stack.drain(start..));

                    func.info().signature.validate_values(&self.args_buffer)?;
                    let result = crate::context::with_current_query_cancellation(|cancellation| {
                        func.evaluate_with_cancellation(&self.args_buffer, cancellation)
                    })?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::CallStored { name, arg_count } => {
                    let arg_count = *arg_count as usize;
                    let start = self.stack.len().saturating_sub(arg_count);
                    self.args_buffer.clear();
                    self.args_buffer.extend(self.stack.drain(start..));
                    let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "stored function {name} is unavailable in this execution context"
                        ))
                    })?;
                    let result = Arc::clone(invoker).invoke(name, &self.args_buffer)?;
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Coalesce(n) => {
                    let n = *n as usize;
                    let start = self.stack.len().saturating_sub(n);

                    // Find first non-null using slice iteration (no intermediate buffer)
                    let result_idx = self.stack[start..]
                        .iter()
                        .position(|v| !v.is_null())
                        .map(|i| start + i);

                    let result = if let Some(idx) = result_idx {
                        // Swap the result to end, pop it, then truncate the rest
                        let last = self.stack.len() - 1;
                        self.stack.swap(idx, last);
                        let result = self.stack.pop().unwrap_or_else(Value::null_unknown);
                        self.stack.truncate(start);
                        result
                    } else {
                        self.stack.truncate(start);
                        Value::null_unknown()
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::NullIf => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if Self::sql_values_equal(ctx, &a, &b)? {
                        Value::null_unknown()
                    } else {
                        a
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Greatest(n) => {
                    let n = *n as usize;
                    let start = self.stack.len().saturating_sub(n);

                    // Find max using slice iteration (no intermediate buffer)
                    let mut max_idx: Option<usize> = None;
                    for (i, v) in self.stack[start..].iter().enumerate() {
                        if !v.is_null() {
                            match max_idx {
                                None => max_idx = Some(start + i),
                                Some(mi) => {
                                    if matches!(
                                        Self::sql_ordering(ctx, v, &self.stack[mi])?,
                                        Some(std::cmp::Ordering::Greater)
                                    ) {
                                        max_idx = Some(start + i);
                                    }
                                }
                            }
                        }
                    }

                    let result = if let Some(idx) = max_idx {
                        // Swap the result to end, pop it, then truncate the rest
                        let last = self.stack.len() - 1;
                        self.stack.swap(idx, last);
                        let result = self.stack.pop().unwrap_or_else(Value::null_unknown);
                        self.stack.truncate(start);
                        result
                    } else {
                        self.stack.truncate(start);
                        Value::null_unknown()
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::Least(n) => {
                    let n = *n as usize;
                    let start = self.stack.len().saturating_sub(n);

                    // Find min using slice iteration (no intermediate buffer)
                    let mut min_idx: Option<usize> = None;
                    for (i, v) in self.stack[start..].iter().enumerate() {
                        if !v.is_null() {
                            match min_idx {
                                None => min_idx = Some(start + i),
                                Some(mi) => {
                                    if matches!(
                                        Self::sql_ordering(ctx, v, &self.stack[mi])?,
                                        Some(std::cmp::Ordering::Less)
                                    ) {
                                        min_idx = Some(start + i);
                                    }
                                }
                            }
                        }
                    }

                    let result = if let Some(idx) = min_idx {
                        // Swap the result to end, pop it, then truncate the rest
                        let last = self.stack.len() - 1;
                        self.stack.swap(idx, last);
                        let result = self.stack.pop().unwrap_or_else(Value::null_unknown);
                        self.stack.truncate(start);
                        result
                    } else {
                        self.stack.truncate(start);
                        Value::null_unknown()
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // NATIVE SCALAR FUNCTIONS (direct function pointer call)
                // In-place mutation - no pop/push overhead
                // =============================================================
                Op::NativeFn1(func) => {
                    if let Some(v) = self.stack.last_mut() {
                        func(v);
                    }
                    pc += 1;
                }

                // =============================================================
                // TYPE OPERATIONS
                // =============================================================
                Op::Cast(target_type) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if v.as_external().is_some() {
                        let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                            Error::invalid_argument(
                                "external type output is unavailable in this execution context",
                            )
                        })?;
                        invoker.external_output(&v, *target_type)?
                    } else {
                        v.try_coerce_to_type(*target_type)?
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                Op::CastExternal(type_name) => {
                    let value = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                        Error::invalid_argument(
                            "external type input is unavailable in this execution context",
                        )
                    })?;
                    self.stack.push(invoker.external_input(type_name, &value)?);
                    pc += 1;
                }

                Op::TruncateToDate => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = match &v {
                        Value::Timestamp(t) => {
                            use chrono::{Datelike, TimeZone, Utc};
                            let truncated = Utc
                                .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                                .single()
                                .unwrap_or(*t);
                            Value::Timestamp(truncated)
                        }
                        Value::Text(s) => match radixdb_core::parse_timestamp(s) {
                            Ok(t) => {
                                use chrono::{Datelike, TimeZone, Utc};
                                let truncated = Utc
                                    .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                                    .single()
                                    .unwrap_or(t);
                                Value::Timestamp(truncated)
                            }
                            Err(_) => Value::Null(DataType::Timestamp),
                        },
                        Value::Integer(i) => {
                            use chrono::{Datelike, TimeZone, Utc};
                            match Utc.timestamp_opt(*i, 0) {
                                chrono::LocalResult::Single(t) => {
                                    let truncated = Utc
                                        .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                                        .single()
                                        .unwrap_or(t);
                                    Value::Timestamp(truncated)
                                }
                                _ => Value::Null(DataType::Timestamp),
                            }
                        }
                        Value::Null(_) => Value::Null(DataType::Timestamp),
                        _ => Value::Null(DataType::Timestamp),
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // CASE EXPRESSION
                // =============================================================
                Op::CaseStart => {
                    // Marker only, no operation
                    pc += 1;
                }

                Op::CaseWhen(next_branch) => {
                    let cond = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    if !Self::to_bool(&cond) {
                        pc = *next_branch as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::CaseThen(end_pos) => {
                    // Result is on stack, jump to end
                    pc = *end_pos as usize;
                }

                Op::CaseElse => {
                    // Marker only
                    pc += 1;
                }

                Op::CaseEnd => {
                    // Marker only
                    pc += 1;
                }

                Op::CaseCompare => {
                    let when_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let case_val = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let result = if !case_val.is_null() && !when_val.is_null() {
                        Value::Boolean(case_val == when_val)
                    } else {
                        Value::Boolean(false)
                    };
                    self.stack.push(result);
                    pc += 1;
                }

                // =============================================================
                // CONTROL FLOW
                // =============================================================
                Op::Jump(target) => {
                    pc = *target as usize;
                }

                Op::JumpIfTrue(target) => {
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    if Self::to_bool(top) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::JumpIfFalse(target) => {
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    if !Self::to_bool(top) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::JumpIfNull(target) => {
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    if top.is_null() {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::JumpIfNotNull(target) => {
                    let top = self.stack.last().unwrap_or(&Value::Null(DataType::Boolean));
                    if !top.is_null() {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::PopJumpIfTrue(target) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    if Self::to_bool(&v) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::PopJumpIfFalse(target) => {
                    let v = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    if !Self::to_bool(&v) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::Dup => {
                    if let Some(v) = self.stack.last().cloned() {
                        self.stack.push(v);
                    }
                    pc += 1;
                }

                Op::Pop => {
                    // Use truncate instead of pop to drop in-place without copying value out
                    let new_len = self.stack.len().saturating_sub(1);
                    self.stack.truncate(new_len);
                    pc += 1;
                }

                Op::Swap => {
                    let len = self.stack.len();
                    if len >= 2 {
                        self.stack.swap(len - 1, len - 2);
                    }
                    pc += 1;
                }

                // =============================================================
                // SPECIAL
                // =============================================================
                Op::Nop => {
                    pc += 1;
                }

                Op::Return => {
                    break;
                }

                Op::ReturnTrue => {
                    self.stack.clear();
                    self.stack.push(Value::Boolean(true));
                    break;
                }

                Op::ReturnFalse => {
                    self.stack.clear();
                    self.stack.push(Value::Boolean(false));
                    break;
                }

                Op::ReturnNull(dt) => {
                    self.stack.clear();
                    self.stack.push(Value::Null(*dt));
                    break;
                }

                // =============================================================
                // VECTOR DISTANCE OPERATIONS
                // =============================================================
                Op::VectorDistanceL2 => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let mut buf_a = Vec::new();
                    let mut buf_b = Vec::new();
                    if a.is_null() || b.is_null() {
                        self.stack.push(Value::null_unknown());
                    } else {
                        match (
                            extract_vector_bytes(&a, &mut buf_a),
                            extract_vector_bytes(&b, &mut buf_b),
                        ) {
                            (Some(ba), Some(bb)) => {
                                self.stack.push(Value::Float(
                                    radixdb_functions::scalar::vector::l2_distance_bytes(ba, bb)?,
                                ));
                            }
                            _ => {
                                return Err(Error::Type(
                                    "vector distance requires two valid VECTOR values".to_string(),
                                ));
                            }
                        }
                    }
                    pc += 1;
                }

                Op::VectorDistanceCosine => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let mut buf_a = Vec::new();
                    let mut buf_b = Vec::new();
                    if a.is_null() || b.is_null() {
                        self.stack.push(Value::null_unknown());
                    } else {
                        match (
                            extract_vector_bytes(&a, &mut buf_a),
                            extract_vector_bytes(&b, &mut buf_b),
                        ) {
                            (Some(ba), Some(bb)) => {
                                self.stack.push(Value::Float(
                                    radixdb_functions::scalar::vector::cosine_distance_bytes(
                                        ba, bb,
                                    )?,
                                ));
                            }
                            _ => {
                                return Err(Error::Type(
                                    "vector distance requires two valid VECTOR values".to_string(),
                                ));
                            }
                        }
                    }
                    pc += 1;
                }

                Op::VectorDistanceIP => {
                    let b = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let a = self.stack.pop().unwrap_or_else(Value::null_unknown);
                    let mut buf_a = Vec::new();
                    let mut buf_b = Vec::new();
                    if a.is_null() || b.is_null() {
                        self.stack.push(Value::null_unknown());
                    } else {
                        match (
                            extract_vector_bytes(&a, &mut buf_a),
                            extract_vector_bytes(&b, &mut buf_b),
                        ) {
                            (Some(ba), Some(bb)) => {
                                self.stack.push(Value::Float(
                                    radixdb_functions::scalar::vector::ip_distance_bytes(ba, bb)?,
                                ));
                            }
                            _ => {
                                return Err(Error::Type(
                                    "vector distance requires two valid VECTOR values".to_string(),
                                ));
                            }
                        }
                    }
                    pc += 1;
                }
            }
        }

        // Return top of stack or NULL
        Ok(self.stack.pop().unwrap_or_else(Value::null_unknown))
    }

    /// Execute a program using borrowed values where possible (Cow-based stack)
    ///
    /// This version avoids cloning values from the row when possible.
    /// Values are only cloned when they need to be modified or passed to functions.
    #[inline]
    pub fn execute_cow<'a>(
        &mut self,
        program: &'a Program,
        ctx: &'a ExecuteContext<'a>,
    ) -> Result<Value> {
        // Choose the interpreter before executing any observable instruction.
        // Restarting after a partial Cow prefix would duplicate scalar calls.
        if !program.ops().iter().all(Self::cow_supports_op) {
            return self.execute(program, ctx);
        }

        // Local stack with borrowed values - lifetime tied to this execution
        let mut stack: SmallVec<[StackValue<'a>; STACK_INLINE_CAPACITY]> = SmallVec::new();

        let ops = program.ops();
        if ops.is_empty() {
            return Ok(NULL_VALUE.clone());
        }

        let mut pc: usize = 0;

        loop {
            if pc >= ops.len() {
                break;
            }

            match &ops[pc] {
                // LOAD OPERATIONS - borrow instead of clone
                Op::LoadColumn(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .row
                        .get(idx)
                        .map(Cow::Borrowed)
                        .unwrap_or_else(|| Cow::Borrowed(&NULL_VALUE));
                    stack.push(value);
                    pc += 1;
                }

                Op::LoadColumn2(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .row2
                        .and_then(|r| r.get(idx))
                        .map(Cow::Borrowed)
                        .unwrap_or_else(|| Cow::Borrowed(&NULL_VALUE));
                    stack.push(value);
                    pc += 1;
                }

                Op::LoadConst(value) => {
                    stack.push(Cow::Borrowed(value));
                    pc += 1;
                }

                Op::LoadParam(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .params
                        .get(idx)
                        .map(Cow::Borrowed)
                        .unwrap_or_else(|| Cow::Borrowed(&NULL_VALUE));
                    stack.push(value);
                    pc += 1;
                }

                Op::LoadNull(dt) => {
                    stack.push(Cow::Owned(Value::Null(*dt)));
                    pc += 1;
                }

                Op::LoadAggregateResult(idx) => {
                    let idx = *idx as usize;
                    let value = ctx
                        .row
                        .get(idx)
                        .map(Cow::Borrowed)
                        .unwrap_or_else(|| Cow::Borrowed(&NULL_VALUE));
                    stack.push(value);
                    pc += 1;
                }

                // COMPARISON OPERATIONS - work with references
                Op::Eq => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_equality_result(ctx, a.as_ref(), b.as_ref(), false)?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Ne => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_equality_result(ctx, a.as_ref(), b.as_ref(), true)?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Lt => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_order_result(ctx, a.as_ref(), b.as_ref(), |ordering| {
                        ordering == std::cmp::Ordering::Less
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Le => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_order_result(ctx, a.as_ref(), b.as_ref(), |ordering| {
                        ordering != std::cmp::Ordering::Greater
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Gt => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_order_result(ctx, a.as_ref(), b.as_ref(), |ordering| {
                        ordering == std::cmp::Ordering::Greater
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Ge => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = Self::sql_order_result(ctx, a.as_ref(), b.as_ref(), |ordering| {
                        ordering != std::cmp::Ordering::Less
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::IsNull => {
                    let v = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    stack.push(Cow::Owned(Value::Boolean(v.is_null())));
                    pc += 1;
                }

                Op::IsNotNull => {
                    let v = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    stack.push(Cow::Owned(Value::Boolean(!v.is_null())));
                    pc += 1;
                }

                // LOGICAL OPERATIONS
                Op::And(jump_target) => {
                    let top = stack.last().map(|v| &**v).unwrap_or(&NULL_VALUE);
                    match top {
                        Value::Boolean(false) => pc = *jump_target as usize,
                        Value::Null(_) => pc += 1,
                        _ => pc += 1,
                    }
                }

                Op::Or(jump_target) => {
                    let top = stack.last().map(|v| &**v).unwrap_or(&NULL_VALUE);
                    match top {
                        Value::Boolean(true) => pc = *jump_target as usize,
                        Value::Null(_) => pc += 1,
                        _ => pc += 1,
                    }
                }

                Op::AndFinalize => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = match (Self::to_tribool(&a), Self::to_tribool(&b)) {
                        (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
                        (Some(true), Some(true)) => Value::Boolean(true),
                        _ => Value::Null(DataType::Boolean),
                    };
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::OrFinalize => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = match (Self::to_tribool(&a), Self::to_tribool(&b)) {
                        (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
                        (Some(false), Some(false)) => Value::Boolean(false),
                        _ => Value::Null(DataType::Boolean),
                    };
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Not => {
                    let v = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = match Self::to_tribool(&v) {
                        Some(b) => Value::Boolean(!b),
                        None => Value::Null(DataType::Boolean),
                    };
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                // FUNCTION CALLS - need to convert to owned
                Op::CallScalar { func, arg_count } => {
                    let arg_count = *arg_count as usize;
                    let start = stack.len().saturating_sub(arg_count);

                    // Convert Cow values to owned for function call
                    self.args_buffer.clear();
                    for cow_val in stack.drain(start..) {
                        self.args_buffer.push(cow_val.into_owned());
                    }

                    func.info().signature.validate_values(&self.args_buffer)?;
                    let result = crate::context::with_current_query_cancellation(|cancellation| {
                        func.evaluate_with_cancellation(&self.args_buffer, cancellation)
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::CallStored { name, arg_count } => {
                    let arg_count = *arg_count as usize;
                    let start = stack.len().saturating_sub(arg_count);
                    self.args_buffer.clear();
                    for value in stack.drain(start..) {
                        self.args_buffer.push(value.into_owned());
                    }
                    let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "stored function {name} is unavailable in this execution context"
                        ))
                    })?;
                    let result = Arc::clone(invoker).invoke(name, &self.args_buffer)?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                // FUSED OPERATIONS (already optimized, no stack involvement)
                Op::GtColumnConst(idx, val) => {
                    let col_val = ctx.row.get(*idx as usize).unwrap_or(&NULL_VALUE);
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering == std::cmp::Ordering::Greater
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::LtColumnConst(idx, val) => {
                    let col_val = ctx.row.get(*idx as usize).unwrap_or(&NULL_VALUE);
                    let result = Self::sql_order_result(ctx, col_val, val, |ordering| {
                        ordering == std::cmp::Ordering::Less
                    })?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::EqColumnConst(idx, val) => {
                    let col_val = ctx.row.get(*idx as usize).unwrap_or(&NULL_VALUE);
                    let result = Self::sql_equality_result(ctx, col_val, val, false)?;
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                // COALESCE - return first non-null value
                Op::Coalesce(n) => {
                    let n = *n as usize;
                    let start = stack.len().saturating_sub(n);

                    // Find first non-null value index
                    let result_idx = stack[start..]
                        .iter()
                        .position(|v| !v.is_null())
                        .map(|i| start + i);

                    let result = if let Some(idx) = result_idx {
                        // Move the result out, swap to end, pop, then truncate
                        let last = stack.len() - 1;
                        stack.swap(idx, last);
                        // pop() should always succeed since we found an index
                        stack.pop().expect("stack underflow in COALESCE")
                    } else {
                        Cow::Borrowed(&NULL_VALUE)
                    };
                    stack.truncate(start);
                    stack.push(result);
                    pc += 1;
                }

                // NULLIF - return NULL if both args are equal
                Op::NullIf => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = if Self::sql_values_equal(ctx, a.as_ref(), b.as_ref())? {
                        Cow::Borrowed(&NULL_VALUE)
                    } else {
                        a
                    };
                    stack.push(result);
                    pc += 1;
                }

                // GREATEST - return maximum non-null value
                Op::Greatest(n) => {
                    let n = *n as usize;
                    let start = stack.len().saturating_sub(n);

                    // Find max value index
                    let mut max_idx: Option<usize> = None;
                    for (i, v) in stack[start..].iter().enumerate() {
                        if !v.is_null() {
                            match max_idx {
                                None => max_idx = Some(start + i),
                                Some(mi) => {
                                    if matches!(
                                        Self::sql_ordering(ctx, v.as_ref(), stack[mi].as_ref())?,
                                        Some(std::cmp::Ordering::Greater)
                                    ) {
                                        max_idx = Some(start + i);
                                    }
                                }
                            }
                        }
                    }

                    let result = if let Some(idx) = max_idx {
                        let last = stack.len() - 1;
                        stack.swap(idx, last);
                        stack.pop().expect("stack underflow in GREATEST")
                    } else {
                        Cow::Borrowed(&NULL_VALUE)
                    };
                    stack.truncate(start);
                    stack.push(result);
                    pc += 1;
                }

                // LEAST - return minimum non-null value
                Op::Least(n) => {
                    let n = *n as usize;
                    let start = stack.len().saturating_sub(n);

                    // Find min value index
                    let mut min_idx: Option<usize> = None;
                    for (i, v) in stack[start..].iter().enumerate() {
                        if !v.is_null() {
                            match min_idx {
                                None => min_idx = Some(start + i),
                                Some(mi) => {
                                    if matches!(
                                        Self::sql_ordering(ctx, v.as_ref(), stack[mi].as_ref())?,
                                        Some(std::cmp::Ordering::Less)
                                    ) {
                                        min_idx = Some(start + i);
                                    }
                                }
                            }
                        }
                    }

                    let result = if let Some(idx) = min_idx {
                        let last = stack.len() - 1;
                        stack.swap(idx, last);
                        stack.pop().expect("stack underflow in LEAST")
                    } else {
                        Cow::Borrowed(&NULL_VALUE)
                    };
                    stack.truncate(start);
                    stack.push(result);
                    pc += 1;
                }

                // JUMP/CONTROL FLOW for short-circuit evaluation (used by COALESCE)
                Op::JumpIfNotNull(target) => {
                    if let Some(top) = stack.last() {
                        if !top.is_null() {
                            pc = *target as usize;
                            continue;
                        }
                    }
                    pc += 1;
                }

                Op::Pop => {
                    // Use truncate instead of pop to drop in-place without copying value out
                    let new_len = stack.len().saturating_sub(1);
                    stack.truncate(new_len);
                    pc += 1;
                }

                Op::Jump(target) => {
                    pc = *target as usize;
                }

                Op::JumpIfTrue(target) => {
                    if let Some(top) = stack.last() {
                        if Self::to_bool(top) {
                            pc = *target as usize;
                            continue;
                        }
                    }
                    pc += 1;
                }

                Op::JumpIfFalse(target) => {
                    if let Some(top) = stack.last() {
                        if !Self::to_bool(top) {
                            pc = *target as usize;
                            continue;
                        }
                    } else {
                        // Empty stack → treat as NULL → falsy → jump
                        pc = *target as usize;
                        continue;
                    }
                    pc += 1;
                }

                Op::JumpIfNull(target) => {
                    if let Some(top) = stack.last() {
                        if top.is_null() {
                            pc = *target as usize;
                            continue;
                        }
                    }
                    pc += 1;
                }

                Op::PopJumpIfFalse(target) => {
                    let v = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    if !Self::to_bool(&v) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::PopJumpIfTrue(target) => {
                    let v = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    if Self::to_bool(&v) {
                        pc = *target as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::Nop => {
                    pc += 1;
                }

                // STRING CONCATENATION
                Op::Concat => {
                    let b = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let a = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = if a.is_null() || b.is_null() {
                        Value::Null(DataType::Text)
                    } else {
                        // Fast path: both are Text
                        match (&*a, &*b) {
                            (Value::Text(a_str), Value::Text(b_str)) => {
                                // Use optimized concat - handles inline and heap efficiently
                                Value::Text(SmartString::concat(a_str, b_str))
                            }
                            (Value::Text(a_str), _) => {
                                // Use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(a_str.len() + 32);
                                s.push_str(a_str);
                                let _ = write!(s, "{}", *b);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                            (_, Value::Text(b_str)) => {
                                // Use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(32 + b_str.len());
                                let _ = write!(s, "{}", *a);
                                s.push_str(b_str);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                            _ => {
                                // Use Arc to avoid shrink_to_fit
                                use std::fmt::Write;
                                let mut s = String::with_capacity(64);
                                let _ = write!(s, "{}{}", *a, *b);
                                Value::Text(SmartString::from_string_shared(s))
                            }
                        }
                    };
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                // Multi-value string concatenation (optimized for chained ||)
                Op::ConcatN(n) => {
                    let n = *n as usize;
                    let start = stack.len().saturating_sub(n);

                    // Single pass: check for NULL and calculate total length
                    let mut total_len = 0usize;
                    let mut has_null = false;
                    let mut all_text = true;

                    for v in &stack[start..] {
                        match &**v {
                            Value::Null(_) => {
                                has_null = true;
                                break;
                            }
                            Value::Text(s) => total_len += s.len(),
                            _ => {
                                all_text = false;
                                total_len += 32;
                            }
                        }
                    }

                    if has_null {
                        stack.truncate(start);
                        stack.push(Cow::Owned(Value::Null(DataType::Text)));
                        pc += 1;
                        continue;
                    }

                    // Build result - optimize for inline vs heap
                    let result = if all_text && total_len <= 15 {
                        // Fast path: build directly into inline SmartString (no heap allocation)
                        let mut data = [0u8; 15];
                        let mut pos = 0;
                        for v in stack.drain(start..) {
                            if let Value::Text(text) = &*v {
                                let bytes = text.as_bytes();
                                data[pos..pos + bytes.len()].copy_from_slice(bytes);
                                pos += bytes.len();
                            }
                        }
                        let text = std::str::from_utf8(&data[..total_len])
                            .expect("concatenated Text values remain valid UTF-8");
                        SmartString::new(text)
                    } else if all_text {
                        // Heap path: exact capacity, into_boxed_str is O(1) when len == capacity
                        let mut s = String::with_capacity(total_len);
                        for v in stack.drain(start..) {
                            if let Value::Text(text) = &*v {
                                s.push_str(text);
                            }
                        }
                        // len == capacity, so into_boxed_str is O(1)
                        SmartString::from_string(s)
                    } else {
                        // Mixed types: capacity is estimate, use Arc to avoid shrink_to_fit
                        let mut s = String::with_capacity(total_len);
                        for v in stack.drain(start..) {
                            match &*v {
                                Value::Text(text) => s.push_str(text),
                                other => {
                                    use std::fmt::Write;
                                    let _ = write!(s, "{}", other);
                                }
                            }
                        }
                        SmartString::from_string_shared(s)
                    };
                    stack.push(Cow::Owned(Value::Text(result)));
                    pc += 1;
                }

                // CASE expression operations
                Op::CaseStart => {
                    // Marker only, no operation
                    pc += 1;
                }

                Op::CaseWhen(next_branch) => {
                    let cond = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    if !Self::to_bool(&cond) {
                        pc = *next_branch as usize;
                    } else {
                        pc += 1;
                    }
                }

                Op::CaseThen(end_pos) => {
                    // Result is on stack, jump to end
                    pc = *end_pos as usize;
                }

                Op::CaseElse => {
                    // Marker only
                    pc += 1;
                }

                Op::CaseEnd => {
                    // Marker only
                    pc += 1;
                }

                Op::CaseCompare => {
                    let when_val = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let case_val = stack.pop().unwrap_or(Cow::Borrowed(&NULL_VALUE));
                    let result = if !case_val.is_null() && !when_val.is_null() {
                        Value::Boolean(*case_val == *when_val)
                    } else {
                        Value::Boolean(false)
                    };
                    stack.push(Cow::Owned(result));
                    pc += 1;
                }

                Op::Return => break,
                Op::ReturnTrue => {
                    return Ok(Value::Boolean(true));
                }
                Op::ReturnFalse => {
                    return Ok(Value::Boolean(false));
                }
                Op::ReturnNull(dt) => {
                    return Ok(Value::Null(*dt));
                }

                // For any unhandled operation, fall back to the regular execute
                _ => {
                    return Err(radixdb_core::Error::internal(
                        "Cow VM support classification drifted from dispatch",
                    ));
                }
            }
        }

        // Return top of stack or NULL
        Ok(stack
            .pop()
            .map(Cow::into_owned)
            .unwrap_or_else(Value::null_unknown))
    }

    #[inline]
    fn cow_supports_op(op: &Op) -> bool {
        matches!(
            op,
            Op::LoadColumn(_)
                | Op::LoadColumn2(_)
                | Op::LoadConst(_)
                | Op::LoadParam(_)
                | Op::LoadNull(_)
                | Op::LoadAggregateResult(_)
                | Op::Eq
                | Op::Ne
                | Op::Lt
                | Op::Le
                | Op::Gt
                | Op::Ge
                | Op::IsNull
                | Op::IsNotNull
                | Op::And(_)
                | Op::Or(_)
                | Op::AndFinalize
                | Op::OrFinalize
                | Op::Not
                | Op::CallScalar { .. }
                | Op::CallStored { .. }
                | Op::GtColumnConst(_, _)
                | Op::LtColumnConst(_, _)
                | Op::EqColumnConst(_, _)
                | Op::Coalesce(_)
                | Op::NullIf
                | Op::Greatest(_)
                | Op::Least(_)
                | Op::JumpIfNotNull(_)
                | Op::Pop
                | Op::Jump(_)
                | Op::JumpIfTrue(_)
                | Op::JumpIfFalse(_)
                | Op::JumpIfNull(_)
                | Op::PopJumpIfFalse(_)
                | Op::PopJumpIfTrue(_)
                | Op::Nop
                | Op::Concat
                | Op::ConcatN(_)
                | Op::CaseStart
                | Op::CaseWhen(_)
                | Op::CaseThen(_)
                | Op::CaseElse
                | Op::CaseEnd
                | Op::CaseCompare
                | Op::Return
                | Op::ReturnTrue
                | Op::ReturnFalse
                | Op::ReturnNull(_)
        )
    }

    /// Execute and return boolean result (for WHERE clauses)
    ///
    /// This method is optimized for common filter patterns, avoiding
    /// the full VM loop overhead for simple comparisons.
    #[inline]
    pub fn execute_bool(&mut self, program: &Program, ctx: &ExecuteContext) -> Result<bool> {
        self.execute_bool_checked(program, ctx)
    }

    /// Like execute_bool but returns errors instead of swallowing them.
    ///
    /// Used by RowFilter::matches_checked to propagate VM errors (e.g. invalid
    /// REGEXP patterns) through the query result iterator.
    #[inline]
    pub fn execute_bool_checked(
        &mut self,
        program: &Program,
        ctx: &ExecuteContext,
    ) -> radixdb_core::Result<bool> {
        let ops = program.ops();

        // Fast path: Single comparison + Return (most common filter)
        if ops.len() == 2 && matches!(&ops[1], Op::Return) && Self::is_fast_bool_op(&ops[0]) {
            return Self::eval_single_op_bool(&ops[0], ctx);
        }

        // Fast path: Two comparisons with AND/OR
        if ops.len() == 5 {
            if Self::is_fast_bool_op(&ops[0])
                && Self::is_fast_bool_op(&ops[2])
                && matches!(
                    (&ops[1], &ops[3], &ops[4]),
                    (Op::And(_), Op::AndFinalize, Op::Return)
                )
            {
                let a = Self::eval_single_op_tribool(&ops[0], ctx)?;
                if a == Some(false) {
                    return Ok(false);
                }
                let b = Self::eval_single_op_tribool(&ops[2], ctx)?;
                return Ok(a == Some(true) && b == Some(true));
            }
            if Self::is_fast_bool_op(&ops[0])
                && Self::is_fast_bool_op(&ops[2])
                && matches!(
                    (&ops[1], &ops[3], &ops[4]),
                    (Op::Or(_), Op::OrFinalize, Op::Return)
                )
            {
                let a = Self::eval_single_op_tribool(&ops[0], ctx)?;
                if a == Some(true) {
                    return Ok(true);
                }
                let b = Self::eval_single_op_tribool(&ops[2], ctx)?;
                return Ok(a == Some(true) || b == Some(true));
            }
        }

        // General path: full VM execution
        match self.execute_cow(program, ctx) {
            Ok(Value::Boolean(b)) => Ok(b),
            Ok(Value::Integer(i)) => Ok(i != 0),
            Ok(Value::Null(_)) => Ok(false),
            Ok(value) => Err(Error::Type(format!(
                "predicate expression produced {}, expected BOOLEAN, INTEGER, or NULL",
                value.data_type()
            ))),
            Err(e) => Err(e),
        }
    }

    #[inline]
    fn is_fast_bool_op(op: &Op) -> bool {
        matches!(
            op,
            Op::GtColumnConst(_, _)
                | Op::LtColumnConst(_, _)
                | Op::GeColumnConst(_, _)
                | Op::LeColumnConst(_, _)
                | Op::EqColumnConst(_, _)
                | Op::NeColumnConst(_, _)
                | Op::IsNullColumn(_)
                | Op::IsNotNullColumn(_)
                | Op::BetweenColumnConst(_, _, _)
                | Op::InSetColumn(_, _, _)
                | Op::LoadConst(Value::Boolean(_) | Value::Integer(_) | Value::Null(_))
                | Op::LikeColumn(_, _, _)
        )
    }

    /// Evaluate a single comparison op and return bool (for fast path)
    #[inline]
    fn eval_single_op_bool(op: &Op, ctx: &ExecuteContext) -> Result<bool> {
        Ok(match op {
            Op::GtColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering == std::cmp::Ordering::Greater
                })?
                .unwrap_or(false),
                None => false,
            },
            Op::LtColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering == std::cmp::Ordering::Less
                })?
                .unwrap_or(false),
                None => false,
            },
            Op::GeColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering != std::cmp::Ordering::Less
                })?
                .unwrap_or(false),
                None => false,
            },
            Op::LeColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering != std::cmp::Ordering::Greater
                })?
                .unwrap_or(false),
                None => false,
            },
            Op::EqColumnConst(idx, value) => match ctx.row.get(*idx as usize) {
                Some(column) => {
                    Self::sql_equality_tribool(ctx, column, value, false)?.unwrap_or(false)
                }
                None => false,
            },
            Op::NeColumnConst(idx, value) => match ctx.row.get(*idx as usize) {
                Some(column) => {
                    Self::sql_equality_tribool(ctx, column, value, true)?.unwrap_or(false)
                }
                None => false,
            },
            Op::IsNullColumn(idx) => ctx.row.get(*idx as usize).is_some_and(|v| v.is_null()),
            Op::IsNotNullColumn(idx) => ctx.row.get(*idx as usize).is_some_and(|v| !v.is_null()),
            Op::BetweenColumnConst(idx, low, high) => match ctx.row.get(*idx as usize) {
                Some(col_val) => {
                    Self::sql_between_tribool(ctx, col_val, low, high)?.unwrap_or(false)
                }
                _ => false,
            },
            Op::InSetColumn(idx, set, has_null) => {
                match ctx.row.get(*idx as usize) {
                    Some(v) if v.is_null() => false, // NULL IN set -> NULL -> false in bool context
                    Some(v) => {
                        let mut found = false;
                        for candidate in set.iter() {
                            if Self::sql_values_equal(ctx, v, candidate)? {
                                found = true;
                                break;
                            }
                        }
                        let _ = has_null;
                        found
                    }
                    None => false,
                }
            }
            // Handle LIKE pattern matching (e.g., fruit LIKE 'a%')
            Op::LikeColumn(idx, pattern, case_insensitive) => {
                match ctx.row.get(*idx as usize) {
                    Some(Value::Text(s)) => pattern.matches(s, *case_insensitive),
                    _ => false, // NULL or non-text -> false
                }
            }
            // For other ops, fall back to tribool and convert
            _ => Self::eval_single_op_tribool(op, ctx)? == Some(true),
        })
    }

    /// Evaluate a single comparison op and return Option<bool> (tribool)
    /// None = NULL, Some(true) = true, Some(false) = false
    #[inline]
    fn eval_single_op_tribool(op: &Op, ctx: &ExecuteContext) -> Result<Option<bool>> {
        Ok(match op {
            Op::GtColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering == std::cmp::Ordering::Greater
                })?,
                None => None,
            },
            Op::LtColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering == std::cmp::Ordering::Less
                })?,
                None => None,
            },
            Op::GeColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering != std::cmp::Ordering::Less
                })?,
                None => None,
            },
            Op::LeColumnConst(idx, threshold) => match ctx.row.get(*idx as usize) {
                Some(value) => Self::sql_order_tribool(ctx, value, threshold, |ordering| {
                    ordering != std::cmp::Ordering::Greater
                })?,
                None => None,
            },
            Op::EqColumnConst(idx, value) => match ctx.row.get(*idx as usize) {
                Some(column) => Self::sql_equality_tribool(ctx, column, value, false)?,
                None => None,
            },
            Op::NeColumnConst(idx, value) => match ctx.row.get(*idx as usize) {
                Some(column) => Self::sql_equality_tribool(ctx, column, value, true)?,
                None => None,
            },
            Op::IsNullColumn(idx) => Some(ctx.row.get(*idx as usize).is_some_and(|v| v.is_null())),
            Op::IsNotNullColumn(idx) => {
                Some(ctx.row.get(*idx as usize).is_some_and(|v| !v.is_null()))
            }
            Op::BetweenColumnConst(idx, low, high) => match ctx.row.get(*idx as usize) {
                Some(col_val) => Self::sql_between_tribool(ctx, col_val, low, high)?,
                None => None,
            },
            Op::InSetColumn(idx, set, has_null) => {
                match ctx.row.get(*idx as usize) {
                    Some(v) if v.is_null() => None, // NULL IN set -> NULL
                    Some(v) => {
                        let mut found = false;
                        for candidate in set.iter() {
                            if Self::sql_values_equal(ctx, v, candidate)? {
                                found = true;
                                break;
                            }
                        }
                        if found {
                            Some(true)
                        } else if *has_null {
                            None
                        } else {
                            Some(false)
                        }
                    }
                    None => None,
                }
            }
            // Handle boolean constants (from processed subqueries like NOT EXISTS)
            Op::LoadConst(Value::Boolean(b)) => Some(*b),
            Op::LoadConst(Value::Integer(i)) => Some(*i != 0),
            Op::LoadConst(Value::Null(_)) => None,
            // Handle LIKE pattern matching (e.g., fruit LIKE 'a%')
            Op::LikeColumn(idx, pattern, case_insensitive) => match ctx.row.get(*idx as usize) {
                Some(Value::Text(s)) => Some(pattern.matches(s, *case_insensitive)),
                Some(Value::Null(_)) | None => None,
                _ => Some(false),
            },
            // For other comparisons, return None to fall back to full VM
            _ => None,
        })
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    #[inline]
    fn to_bool(v: &Value) -> bool {
        match v {
            Value::Boolean(b) => *b,
            Value::Integer(i) => *i != 0,
            Value::Null(_) => false,
            _ => true, // Non-null values are truthy
        }
    }

    #[inline]
    fn to_tribool(v: &Value) -> Option<bool> {
        match v {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            Value::Null(_) => None,
            _ => Some(true),
        }
    }

    /// SQL equality uses plugin-owned semantics for external types. Structural
    /// `Value::Eq` is never a fallback for an external payload.
    #[inline]
    fn sql_values_equal(ctx: &ExecuteContext<'_>, a: &Value, b: &Value) -> Result<bool> {
        if a.is_external() || b.is_external() {
            let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                Error::NotSupported(
                    "external equality is unavailable in this execution context".to_owned(),
                )
            })?;
            return invoker.external_equal(a, b);
        }
        match a
            .compare(b)
            .ok()
            .or_else(|| Self::sql_literal_ordering(a, b))
        {
            Some(std::cmp::Ordering::Equal) => Ok(true),
            Some(_) => Ok(false),
            None => Ok(a == b),
        }
    }

    #[inline]
    fn sql_set_contains(
        ctx: &ExecuteContext<'_>,
        set: &radixdb_core::ValueSet,
        value: &Value,
    ) -> Result<bool> {
        if !value.is_external() {
            return Ok(set.contains(value));
        }
        for candidate in set.iter() {
            if Self::sql_values_equal(ctx, value, candidate)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Canonical SQL comparison result shared by the stack, fused and
    /// bool/tribool execution paths. NULL remains UNKNOWN; non-NULL numeric
    /// values use `Value::compare`, which does not round an i64 through f64.
    #[inline]
    fn sql_ordering(
        ctx: &ExecuteContext<'_>,
        a: &Value,
        b: &Value,
    ) -> Result<Option<std::cmp::Ordering>> {
        if a.is_null() || b.is_null() {
            return Ok(None);
        }
        if a.is_external() || b.is_external() {
            let invoker = ctx.stored_function_invoker.ok_or_else(|| {
                Error::NotSupported(
                    "external ordering is unavailable in this execution context".to_owned(),
                )
            })?;
            return invoker.external_compare(a, b).map(Some);
        }
        Ok(a.compare(b)
            .ok()
            .or_else(|| Self::sql_literal_ordering(a, b)))
    }

    /// SQL literals may be compared with typed UUID/TIMESTAMP values before a
    /// schema-aware pushdown can bind them (TVFs and post-join residuals are
    /// the two important cases). Keep those explicit admissions here instead
    /// of making structural `Value` identity perform general string coercion.
    #[inline]
    fn sql_literal_ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (Value::Extension(_), Value::Text(text)) => a
                .as_uuid_bytes()
                .zip(radixdb_core::value::parse_uuid_str(text))
                .map(|(left, right)| left.cmp(&right)),
            (Value::Text(text), Value::Extension(_)) => radixdb_core::value::parse_uuid_str(text)
                .zip(b.as_uuid_bytes())
                .map(|(left, right)| left.cmp(&right)),
            (Value::Timestamp(left), Value::Text(text)) => {
                radixdb_core::value::parse_timestamp(text)
                    .ok()
                    .map(|right| left.cmp(&right))
            }
            (Value::Text(text), Value::Timestamp(right)) => {
                radixdb_core::value::parse_timestamp(text)
                    .ok()
                    .map(|left| left.cmp(right))
            }
            _ => None,
        }
    }

    #[inline]
    fn sql_equality_result(
        ctx: &ExecuteContext<'_>,
        a: &Value,
        b: &Value,
        negated: bool,
    ) -> Result<Value> {
        if a.is_null() || b.is_null() {
            Ok(Value::Null(DataType::Boolean))
        } else {
            Ok(Value::Boolean(
                Self::sql_values_equal(ctx, a, b)? != negated,
            ))
        }
    }

    #[inline]
    fn sql_order_result(
        ctx: &ExecuteContext<'_>,
        a: &Value,
        b: &Value,
        predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    ) -> Result<Value> {
        match Self::sql_ordering(ctx, a, b)? {
            Some(ordering) => Ok(Value::Boolean(predicate(ordering))),
            None => Ok(Value::Null(DataType::Boolean)),
        }
    }

    #[inline]
    fn sql_order_tribool(
        ctx: &ExecuteContext<'_>,
        a: &Value,
        b: &Value,
        predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    ) -> Result<Option<bool>> {
        Ok(Self::sql_ordering(ctx, a, b)?.map(predicate))
    }

    #[inline]
    fn sql_equality_tribool(
        ctx: &ExecuteContext<'_>,
        a: &Value,
        b: &Value,
        negated: bool,
    ) -> Result<Option<bool>> {
        if a.is_null() || b.is_null() {
            Ok(None)
        } else {
            Ok(Some(Self::sql_values_equal(ctx, a, b)? != negated))
        }
    }

    #[inline]
    fn sql_between_result(
        ctx: &ExecuteContext<'_>,
        value: &Value,
        low: &Value,
        high: &Value,
        negated: bool,
    ) -> Result<Value> {
        let Some(ge_low) = Self::sql_order_tribool(ctx, value, low, |ordering| {
            ordering != std::cmp::Ordering::Less
        })?
        else {
            return Ok(Value::Null(DataType::Boolean));
        };
        let Some(le_high) = Self::sql_order_tribool(ctx, value, high, |ordering| {
            ordering != std::cmp::Ordering::Greater
        })?
        else {
            return Ok(Value::Null(DataType::Boolean));
        };
        Ok(Value::Boolean((ge_low && le_high) != negated))
    }

    #[inline]
    fn sql_between_tribool(
        ctx: &ExecuteContext<'_>,
        value: &Value,
        low: &Value,
        high: &Value,
    ) -> Result<Option<bool>> {
        let Some(ge_low) = Self::sql_order_tribool(ctx, value, low, |ordering| {
            ordering != std::cmp::Ordering::Less
        })?
        else {
            return Ok(None);
        };
        let Some(le_high) = Self::sql_order_tribool(ctx, value, high, |ordering| {
            ordering != std::cmp::Ordering::Greater
        })?
        else {
            return Ok(None);
        };
        Ok(Some(ge_low && le_high))
    }

    #[inline]
    fn date_add_days(days_since_epoch: i32, delta_days: i64) -> Result<Value> {
        let result = i64::from(days_since_epoch)
            .checked_add(delta_days)
            .and_then(|days| i32::try_from(days).ok())
            .ok_or_else(|| Error::Type("DATE arithmetic overflow".to_string()))?;
        Ok(Value::date(result))
    }

    #[inline]
    fn arithmetic_op<FF>(a: &Value, b: &Value, int_op: ArithmeticOp, float_op: FF) -> Result<Value>
    where
        FF: Fn(f64, f64) -> f64,
    {
        match (a, b) {
            (Value::Integer(x), Value::Integer(y)) => {
                // Use checked operations to detect overflow and return an error
                let result = match int_op {
                    ArithmeticOp::Add => x.checked_add(*y),
                    ArithmeticOp::Sub => x.checked_sub(*y),
                    ArithmeticOp::Mul => x.checked_mul(*y),
                    ArithmeticOp::Div => {
                        if *y == 0 {
                            return Ok(Value::Null(DataType::Integer));
                        }
                        x.checked_div(*y)
                    }
                    ArithmeticOp::Mod => {
                        if *y == 0 {
                            return Ok(Value::Null(DataType::Integer));
                        }
                        x.checked_rem(*y)
                    }
                };
                match result {
                    Some(r) => Ok(Value::Integer(r)),
                    None => Err(radixdb_core::Error::Type(format!(
                        "Integer overflow in arithmetic operation: {} and {}",
                        x, y
                    ))),
                }
            }
            (Value::Float(x), Value::Float(y)) => Ok(Value::Float(float_op(*x, *y))),
            (Value::Integer(x), Value::Float(y)) => Ok(Value::Float(float_op(*x as f64, *y))),
            (Value::Float(x), Value::Integer(y)) => Ok(Value::Float(float_op(*x, *y as f64))),
            _ if a.is_null() || b.is_null() => Ok(Value::Null(DataType::Float)),
            _ => Ok(Value::Null(DataType::Null)),
        }
    }

    #[inline]
    fn div_op(a: &Value, b: &Value) -> radixdb_core::Result<Value> {
        match (a, b) {
            (Value::Integer(x), Value::Integer(y)) if *y != 0 => x
                .checked_div(*y)
                .map(Value::Integer)
                .ok_or_else(|| radixdb_core::Error::Type("integer division overflow".to_string())),
            (Value::Float(x), Value::Float(y)) if *y != 0.0 => Ok(Value::Float(x / y)),
            (Value::Integer(x), Value::Float(y)) if *y != 0.0 => Ok(Value::Float(*x as f64 / y)),
            (Value::Float(x), Value::Integer(y)) if *y != 0 => Ok(Value::Float(x / *y as f64)),
            _ if a.is_null() || b.is_null() => Ok(Value::Null(DataType::Float)),
            _ => Ok(Value::Null(DataType::Null)),
        }
    }

    #[inline]
    fn mod_op(a: &Value, b: &Value) -> radixdb_core::Result<Value> {
        match (a, b) {
            (Value::Integer(x), Value::Integer(y)) if *y != 0 => x
                .checked_rem(*y)
                .map(Value::Integer)
                .ok_or_else(|| radixdb_core::Error::Type("integer remainder overflow".to_string())),
            (Value::Float(x), Value::Float(y)) if *y != 0.0 => Ok(Value::Float(x % y)),
            (Value::Integer(x), Value::Float(y)) if *y != 0.0 => Ok(Value::Float(*x as f64 % y)),
            (Value::Float(x), Value::Integer(y)) if *y != 0 => Ok(Value::Float(x % *y as f64)),
            _ if a.is_null() || b.is_null() => Ok(Value::Null(DataType::Float)),
            _ => Ok(Value::Null(DataType::Null)),
        }
    }

    /// JSON access helper
    /// If as_text is true, returns TEXT; otherwise returns JSON
    fn json_access(&self, json_val: &Value, key: &Value, as_text: bool) -> Value {
        use serde_json;

        // Get the JSON string
        let json_str = match json_val {
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                std::str::from_utf8(&data[1..]).unwrap_or("")
            }
            Value::Text(s) => s.as_ref(),
            Value::Null(_) => {
                return Value::Null(if as_text {
                    DataType::Text
                } else {
                    DataType::Json
                })
            }
            _ => {
                return Value::Null(if as_text {
                    DataType::Text
                } else {
                    DataType::Json
                })
            }
        };

        // Parse the JSON
        let parsed: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => {
                return Value::Null(if as_text {
                    DataType::Text
                } else {
                    DataType::Json
                })
            }
        };

        // Access by key or index
        let result = match key {
            Value::Text(k) => parsed.get(k.as_str()),
            Value::Integer(i) => {
                if *i >= 0 {
                    parsed.get(*i as usize)
                } else {
                    None
                }
            }
            _ => None,
        };

        match result {
            Some(v) => {
                if as_text {
                    // ->> returns text
                    match v {
                        serde_json::Value::String(s) => Value::Text(SmartString::new(s)),
                        serde_json::Value::Null => Value::Null(DataType::Text),
                        other => Value::Text(SmartString::from_string(other.to_string())),
                    }
                } else {
                    // -> returns JSON
                    Value::json(v.to_string())
                }
            }
            None => Value::Null(if as_text {
                DataType::Text
            } else {
                DataType::Json
            }),
        }
    }

    /// Add or subtract interval from timestamp
    fn timestamp_add_days(timestamp: chrono::DateTime<chrono::Utc>, days: i64) -> Result<Value> {
        let duration = chrono::Duration::try_days(days)
            .ok_or_else(|| Error::Type("timestamp day interval overflow".to_string()))?;
        timestamp
            .checked_add_signed(duration)
            .map(Value::Timestamp)
            .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))
    }

    fn timestamp_add_interval(&self, ts: &Value, interval: &Value, add: bool) -> Result<Value> {
        let timestamp = match ts {
            Value::Timestamp(t) => *t,
            Value::Null(_) => return Ok(Value::Null(DataType::Timestamp)),
            _ => return Ok(Value::Null(DataType::Timestamp)),
        };

        let interval_str = match interval {
            Value::Text(s) => s.as_ref(),
            Value::Null(_) => return Ok(Value::Null(DataType::Timestamp)),
            _ => return Ok(Value::Null(DataType::Timestamp)),
        };

        // Parse interval string
        // Formats: "1 day", "2 hours", "30 minutes", "1 year", "1 month", etc.
        match self.parse_interval(interval_str)? {
            IntervalValue::Duration(duration) => {
                let duration = if add {
                    duration
                } else {
                    duration
                        .checked_mul(-1)
                        .ok_or_else(|| Error::Type("fixed interval overflow".to_string()))?
                };
                timestamp
                    .checked_add_signed(duration)
                    .map(Value::Timestamp)
                    .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))
            }
            IntervalValue::Months(months) => {
                let months = if add {
                    months
                } else {
                    months
                        .checked_neg()
                        .ok_or_else(|| Error::Type("calendar interval overflow".to_string()))?
                };
                Self::calendar_add_months(timestamp, months)
                    .map(Value::Timestamp)
                    .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))
            }
        }
    }

    /// Calendar-aware month addition preserving time-of-day and nanoseconds.
    fn calendar_add_months(
        ts: chrono::DateTime<chrono::Utc>,
        months: i64,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        use chrono::{Datelike, NaiveDate, Timelike};

        let total_months = (ts.year() as i64)
            .checked_mul(12)?
            .checked_add(i64::from(ts.month()) - 1)?
            .checked_add(months)?;
        let new_year_i64 = total_months.div_euclid(12);
        let new_month = (total_months.rem_euclid(12) + 1) as u32;

        let new_year = i32::try_from(new_year_i64).ok()?;
        if !(1..=9999).contains(&new_year) {
            return None;
        }

        // Clamp day to valid range for the new month
        let max_day = match new_month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (new_year % 4 == 0 && new_year % 100 != 0) || (new_year % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            _ => 30,
        };
        let day = ts.day().min(max_day);

        // Rebuild date preserving original time including nanoseconds
        let date = NaiveDate::from_ymd_opt(new_year, new_month, day)?;
        let time = ts.time();
        let naive = date.and_hms_nano_opt(
            time.hour(),
            time.minute(),
            time.second(),
            ts.timestamp_subsec_nanos(),
        )?;
        Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            naive,
            chrono::Utc,
        ))
    }

    /// Parsed interval: either a fixed duration or a calendar-relative month count.
    fn parse_interval(&self, s: &str) -> Result<IntervalValue> {
        let s = s.trim();
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.len() < 2 {
            // Try parsing as just a number (days)
            if let Ok(n) = s.parse::<i64>() {
                return chrono::Duration::try_days(n)
                    .map(IntervalValue::Duration)
                    .ok_or_else(|| Error::Type("interval is out of range".to_string()));
            }
            return Err(Error::Type(format!("invalid interval: {s}")));
        }

        let value: i64 = parts[0]
            .parse()
            .map_err(|_| Error::Type(format!("invalid interval: {s}")))?;
        let unit = parts[1];

        // Case-insensitive unit matching without allocation
        // Handle both singular and plural forms
        if unit.eq_ignore_ascii_case("year") || unit.eq_ignore_ascii_case("years") {
            value
                .checked_mul(12)
                .map(IntervalValue::Months)
                .ok_or_else(|| Error::Type("calendar interval overflow".to_string()))
        } else if unit.eq_ignore_ascii_case("month") || unit.eq_ignore_ascii_case("months") {
            Ok(IntervalValue::Months(value))
        } else if unit.eq_ignore_ascii_case("week") || unit.eq_ignore_ascii_case("weeks") {
            chrono::Duration::try_weeks(value)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()))
        } else if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") {
            chrono::Duration::try_days(value)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()))
        } else if unit.eq_ignore_ascii_case("hour") || unit.eq_ignore_ascii_case("hours") {
            chrono::Duration::try_hours(value)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()))
        } else if unit.eq_ignore_ascii_case("minute")
            || unit.eq_ignore_ascii_case("minutes")
            || unit.eq_ignore_ascii_case("min")
        {
            chrono::Duration::try_minutes(value)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()))
        } else if unit.eq_ignore_ascii_case("second")
            || unit.eq_ignore_ascii_case("seconds")
            || unit.eq_ignore_ascii_case("sec")
        {
            chrono::Duration::try_seconds(value)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()))
        } else if unit.eq_ignore_ascii_case("millisecond")
            || unit.eq_ignore_ascii_case("milliseconds")
            || unit.eq_ignore_ascii_case("ms")
        {
            Ok(IntervalValue::Duration(chrono::Duration::milliseconds(
                value,
            )))
        } else if unit.eq_ignore_ascii_case("microsecond")
            || unit.eq_ignore_ascii_case("microseconds")
            || unit.eq_ignore_ascii_case("us")
        {
            Ok(IntervalValue::Duration(chrono::Duration::microseconds(
                value,
            )))
        } else {
            Err(Error::Type(format!("invalid interval unit: {unit}")))
        }
    }

    /// Format chrono Duration as interval string
    fn format_duration_as_interval(&self, duration: chrono::TimeDelta) -> String {
        let total_seconds = duration.num_seconds();
        let abs_seconds = total_seconds.abs();

        let days = abs_seconds / 86400;
        let hours = (abs_seconds % 86400) / 3600;
        let minutes = (abs_seconds % 3600) / 60;
        let seconds = abs_seconds % 60;

        let sign = if total_seconds < 0 { "-" } else { "" };

        if days > 0 {
            format!(
                "{}{} days {:02}:{:02}:{:02}",
                sign, days, hours, minutes, seconds
            )
        } else {
            format!("{}{:02}:{:02}:{:02}", sign, hours, minutes, seconds)
        }
    }
}

impl Default for ExprVM {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract raw LE f32 bytes from a vector Value, zero-copy for Extension.
/// Process a LIKE pattern with a custom escape character at runtime.
///
/// Converts escaped wildcards (e.g. `!%` with escape `!`) into the
/// default `\%` escape form that `CompiledPattern::compile` understands.
fn process_like_escape_runtime(pattern: &str, escape: char) -> String {
    let mut result = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        if c == escape {
            if let Some(&next) = chars.peek() {
                if next == '%' || next == '_' || next == escape {
                    // Convert to default escape form: \% or \_ or \\
                    result.push('\\');
                    result.push(chars.next().unwrap());
                } else {
                    result.push(c);
                }
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// For Text values, parses and writes into `buf` as fallback.
#[inline]
fn extract_vector_bytes<'a>(v: &'a Value, buf: &'a mut Vec<u8>) -> Option<&'a [u8]> {
    match v {
        Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
            Some(&data[1..])
        }
        Value::Text(s) => {
            let floats = radixdb_core::value::parse_vector_str(s.as_ref())?;
            buf.clear();
            buf.reserve(floats.len() * 4);
            for f in &floats {
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Some(buf.as_slice())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
