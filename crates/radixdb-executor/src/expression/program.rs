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

// Compiled Expression Program
//
// A Program is the compiled form of an AST Expression.
// It contains:
// - A sequence of operations (the "bytecode")
// - Metadata for efficient execution

use rustc_hash::FxHashSet;

use super::ops::Op;
use radixdb_core::CompactArc;
use radixdb_core::{Error, Result, Value};

/// Constant value stored in the program
#[derive(Debug, Clone)]
pub enum Constant {
    /// A literal value
    Value(Value),
    /// A string (for patterns, column names, etc.)
    String(CompactArc<str>),
}

/// Compiled expression program
///
/// This is the executable form of an expression. It's designed to be:
/// - Cheap to clone (uses Arc internally for large data)
/// - Fast to execute (linear operation sequence)
/// - Self-contained (no external lookups needed)
#[derive(Clone)]
pub struct Program {
    /// The operation sequence
    ops: Vec<Op>,

    /// Maximum stack depth needed (for pre-allocation)
    max_stack_depth: usize,

    /// Whether this program needs outer row context (correlated subquery)
    needs_outer_context: bool,

    /// Whether this program needs second row (join evaluation)
    needs_second_row: bool,

    /// Source expression string (for debugging)
    #[cfg(debug_assertions)]
    source: Option<String>,
}

impl Program {
    /// Validate and create a program from a public bytecode sequence.
    pub fn try_new(ops: Vec<Op>) -> Result<Self> {
        Self::validate(&ops)?;
        let program = Self::new(ops);
        Self::validate(&program.ops)?;
        Ok(program)
    }

    /// Validate and create an unoptimized program.
    pub fn try_new_unoptimized(ops: Vec<Op>) -> Result<Self> {
        Self::validate(&ops)?;
        Ok(Self::new_unoptimized(ops))
    }

    /// Create a new program from operations.
    /// Automatically applies peephole optimizations (instruction fusion).
    pub(crate) fn new(ops: Vec<Op>) -> Self {
        // Apply peephole optimizations
        let ops = Self::peephole_optimize(ops);

        let max_stack_depth = Self::compute_stack_depth(&ops);
        let needs_outer_context = ops.iter().any(|op| matches!(op, Op::LoadOuterColumn(_)));
        let needs_second_row = ops.iter().any(|op| matches!(op, Op::LoadColumn2(_)));
        Self {
            ops,
            max_stack_depth,
            needs_outer_context,
            needs_second_row,
            #[cfg(debug_assertions)]
            source: None,
        }
    }

    /// Create a new program without peephole optimization.
    /// Use this for testing or when optimization is not desired.
    pub(crate) fn new_unoptimized(ops: Vec<Op>) -> Self {
        let max_stack_depth = Self::compute_stack_depth(&ops);
        let needs_outer_context = ops.iter().any(|op| matches!(op, Op::LoadOuterColumn(_)));
        let needs_second_row = ops.iter().any(|op| matches!(op, Op::LoadColumn2(_)));
        Self {
            ops,
            max_stack_depth,
            needs_outer_context,
            needs_second_row,
            #[cfg(debug_assertions)]
            source: None,
        }
    }

    /// Create an empty program that returns NULL
    pub fn null() -> Self {
        Self::new(vec![Op::LoadNull(radixdb_core::DataType::Null), Op::Return])
    }

    /// Create a program that returns a constant value
    pub fn constant(value: Value) -> Self {
        Self::new(vec![Op::LoadConst(value), Op::Return])
    }

    /// Create a program that returns true
    pub fn always_true() -> Self {
        Self::new(vec![Op::ReturnTrue])
    }

    /// Create a program that returns false
    pub fn always_false() -> Self {
        Self::new(vec![Op::ReturnFalse])
    }

    /// Get the operations
    #[inline]
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// Get the maximum stack depth needed
    #[inline]
    pub fn max_stack_depth(&self) -> usize {
        self.max_stack_depth
    }

    /// Check if this program needs outer row context
    #[inline]
    pub fn needs_outer_context(&self) -> bool {
        self.needs_outer_context
    }

    /// Check if this program needs a second row (for joins)
    #[inline]
    pub fn needs_second_row(&self) -> bool {
        self.needs_second_row
    }

    /// Get the number of operations
    #[inline]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Check if program is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    fn validate(ops: &[Op]) -> Result<()> {
        use std::collections::VecDeque;

        if ops.is_empty() {
            return Err(Error::invalid_argument("expression program is empty"));
        }
        if ops.len() > u16::MAX as usize {
            return Err(Error::invalid_argument(
                "expression program exceeds the u16 instruction limit",
            ));
        }

        let mut depths = vec![None; ops.len()];
        let mut queue = VecDeque::from([(0usize, 0usize)]);
        while let Some((pc, depth)) = queue.pop_front() {
            if pc >= ops.len() {
                return Err(Error::invalid_argument(
                    "expression program falls through without a return",
                ));
            }
            if let Some(existing) = depths[pc] {
                if existing != depth {
                    return Err(Error::invalid_argument(format!(
                        "expression program reaches instruction {pc} with incompatible stack depths {existing} and {depth}"
                    )));
                }
                continue;
            }
            depths[pc] = Some(depth);

            let (required, pushed) = Self::stack_contract(&ops[pc]);
            if depth < required {
                return Err(Error::invalid_argument(format!(
                    "expression program stack underflow at instruction {pc}"
                )));
            }
            let next_depth = depth - required + pushed;

            let mut enqueue_target = |target: u16| -> Result<()> {
                let target = target as usize;
                if target <= pc || target >= ops.len() {
                    return Err(Error::invalid_argument(format!(
                        "expression program has invalid or backward jump from {pc} to {target}"
                    )));
                }
                queue.push_back((target, next_depth));
                Ok(())
            };

            match &ops[pc] {
                Op::Return | Op::ReturnTrue | Op::ReturnFalse | Op::ReturnNull(_) => {}
                Op::Jump(target) | Op::CaseThen(target) => enqueue_target(*target)?,
                Op::And(target)
                | Op::Or(target)
                | Op::JumpIfTrue(target)
                | Op::JumpIfFalse(target)
                | Op::JumpIfNull(target)
                | Op::JumpIfNotNull(target)
                | Op::PopJumpIfTrue(target)
                | Op::PopJumpIfFalse(target)
                | Op::CaseWhen(target) => {
                    enqueue_target(*target)?;
                    queue.push_back((pc + 1, next_depth));
                }
                _ => queue.push_back((pc + 1, next_depth)),
            }
        }
        Ok(())
    }

    /// Number of values consumed and produced by one instruction.
    fn stack_contract(op: &Op) -> (usize, usize) {
        match op {
            Op::LoadColumn(_)
            | Op::LoadColumn2(_)
            | Op::LoadOuterColumn(_)
            | Op::LoadConst(_)
            | Op::LoadParam(_)
            | Op::LoadNamedParam(_)
            | Op::LoadNull(_)
            | Op::LoadAggregateResult(_)
            | Op::LoadTransactionId
            | Op::EqColumnConst(_, _)
            | Op::NeColumnConst(_, _)
            | Op::LtColumnConst(_, _)
            | Op::LeColumnConst(_, _)
            | Op::GtColumnConst(_, _)
            | Op::GeColumnConst(_, _)
            | Op::IsNullColumn(_)
            | Op::IsNotNullColumn(_)
            | Op::LikeColumn(_, _, _)
            | Op::InSetColumn(_, _, _)
            | Op::BetweenColumnConst(_, _, _) => (0, 1),
            Op::Dup => (1, 2),
            Op::Eq
            | Op::Ne
            | Op::Lt
            | Op::Le
            | Op::Gt
            | Op::Ge
            | Op::IsDistinctFrom
            | Op::IsNotDistinctFrom
            | Op::AndFinalize
            | Op::OrFinalize
            | Op::Add
            | Op::Sub
            | Op::Mul
            | Op::Div
            | Op::Mod
            | Op::BitAnd
            | Op::BitOr
            | Op::BitXor
            | Op::Shl
            | Op::Shr
            | Op::Concat
            | Op::Xor
            | Op::NullIf
            | Op::LikeDynamic(_)
            | Op::LikeDynamicEscape(_, _)
            | Op::GlobDynamic
            | Op::RegexpDynamic
            | Op::JsonAccess
            | Op::JsonAccessText
            | Op::TimestampAddInterval
            | Op::TimestampSubInterval
            | Op::TimestampDiff
            | Op::TimestampAddDays
            | Op::TimestampSubDays
            | Op::VectorDistanceL2
            | Op::VectorDistanceCosine
            | Op::VectorDistanceIP => (2, 1),
            Op::Between | Op::NotBetween => (3, 1),
            Op::IsNull
            | Op::IsNotNull
            | Op::IsTrue
            | Op::IsNotTrue
            | Op::IsFalse
            | Op::IsNotFalse
            | Op::Not
            | Op::Neg
            | Op::BitNot
            | Op::Like(_, _)
            | Op::LikeEscape(_, _, _)
            | Op::Glob(_)
            | Op::Regexp(_)
            | Op::InSet(_, _)
            | Op::NotInSet(_, _)
            | Op::Cast(_)
            | Op::CastExternal(_)
            | Op::TruncateToDate
            | Op::NativeFn1(_) => (1, 1),
            Op::InTupleSet { tuple_size, .. } => (*tuple_size as usize, 1),
            Op::CallScalar { arg_count, .. } | Op::CallStored { arg_count, .. } => {
                (*arg_count as usize, 1)
            }
            Op::Coalesce(n) | Op::Greatest(n) | Op::Least(n) | Op::ConcatN(n) => (*n as usize, 1),
            Op::Pop | Op::PopJumpIfTrue(_) | Op::PopJumpIfFalse(_) | Op::CaseWhen(_) => (1, 0),
            Op::Swap => (2, 2),
            Op::And(_)
            | Op::Or(_)
            | Op::JumpIfTrue(_)
            | Op::JumpIfFalse(_)
            | Op::JumpIfNull(_)
            | Op::JumpIfNotNull(_)
            | Op::CaseThen(_) => (1, 1),
            Op::CaseCompare => (2, 1),
            Op::Return => (1, 0),
            Op::Jump(_)
            | Op::Nop
            | Op::CaseStart
            | Op::CaseElse
            | Op::CaseEnd
            | Op::ReturnTrue
            | Op::ReturnFalse
            | Op::ReturnNull(_) => (0, 0),
        }
    }

    /// Set source string for debugging
    #[cfg(debug_assertions)]
    pub fn with_source(mut self, source: String) -> Self {
        self.source = Some(source);
        self
    }

    /// Get source string
    #[cfg(debug_assertions)]
    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    /// Compute the maximum stack depth needed for a sequence of operations
    fn compute_stack_depth(ops: &[Op]) -> usize {
        let mut depth: i32 = 0;
        let mut max_depth: i32 = 0;

        for op in ops {
            // Calculate stack effect of each operation
            let effect = match op {
                // Push operations (+1)
                Op::LoadColumn(_)
                | Op::LoadColumn2(_)
                | Op::LoadOuterColumn(_)
                | Op::LoadConst(_)
                | Op::LoadParam(_)
                | Op::LoadNamedParam(_)
                | Op::LoadNull(_)
                | Op::LoadAggregateResult(_)
                | Op::LoadTransactionId
                | Op::Dup
                // Fused compare ops: push 1 (load + compare in single op)
                | Op::EqColumnConst(_, _)
                | Op::NeColumnConst(_, _)
                | Op::LtColumnConst(_, _)
                | Op::LeColumnConst(_, _)
                | Op::GtColumnConst(_, _)
                | Op::GeColumnConst(_, _)
                // More fused ops: push 1
                | Op::IsNullColumn(_)
                | Op::IsNotNullColumn(_)
                | Op::LikeColumn(_, _, _)
                | Op::InSetColumn(_, _, _)
                | Op::BetweenColumnConst(_, _, _) => 1,

                // Pop 2, push 1 (-1)
                Op::Eq
                | Op::Ne
                | Op::Lt
                | Op::Le
                | Op::Gt
                | Op::Ge
                | Op::IsDistinctFrom
                | Op::IsNotDistinctFrom
                | Op::AndFinalize
                | Op::OrFinalize
                | Op::Add
                | Op::Sub
                | Op::Mul
                | Op::Div
                | Op::Mod
                | Op::BitAnd
                | Op::BitOr
                | Op::BitXor
                | Op::Shl
                | Op::Shr
                | Op::Concat
                | Op::Xor
                | Op::NullIf
                | Op::CaseCompare => -1,

                // Pop 3, push 1 (-2)
                Op::Between | Op::NotBetween => -2,

                // Dynamic pattern ops: Pop 2, push 1 (-1)
                Op::LikeDynamic(_)
                | Op::LikeDynamicEscape(_, _)
                | Op::GlobDynamic
                | Op::RegexpDynamic
                // JSON/Timestamp binary ops: Pop 2, push 1 (-1)
                | Op::JsonAccess
                | Op::JsonAccessText
                | Op::TimestampAddInterval
                | Op::TimestampSubInterval
                | Op::TimestampDiff
                | Op::TimestampAddDays
                | Op::TimestampSubDays
                | Op::VectorDistanceL2
                | Op::VectorDistanceCosine
                | Op::VectorDistanceIP => -1,

                // Transform (0) - pop 1, push 1
                Op::IsNull
                | Op::IsNotNull
                | Op::IsTrue
                | Op::IsNotTrue
                | Op::IsFalse
                | Op::IsNotFalse
                | Op::Not
                | Op::Neg
                | Op::BitNot
                | Op::Like(_, _)
                | Op::LikeEscape(_, _, _)
                | Op::Glob(_)
                | Op::Regexp(_)
                | Op::InSet(_, _)
                | Op::NotInSet(_, _)
                | Op::Cast(_)
                | Op::CastExternal(_)
                | Op::TruncateToDate
                | Op::NativeFn1(_) => 0,

                // Multi-column IN: pop N, push 1
                Op::InTupleSet { tuple_size, .. } => 1 - (*tuple_size as i32),

                // Pop only (-1)
                Op::Pop => -1,

                // Conditionals (no change to depth calculation)
                Op::And(_) | Op::Or(_) => 0,

                // Function calls: pop N, push 1
                Op::CallScalar { arg_count, .. } | Op::CallStored { arg_count, .. } => {
                    1 - (*arg_count as i32)
                }
                Op::Coalesce(n) | Op::Greatest(n) | Op::Least(n) | Op::ConcatN(n) => {
                    1 - (*n as i32)
                }

                // Control flow (no stack effect for depth calculation)
                Op::Jump(_)
                | Op::JumpIfTrue(_)
                | Op::JumpIfFalse(_)
                | Op::JumpIfNull(_)
                | Op::JumpIfNotNull(_)
                | Op::PopJumpIfTrue(_)
                | Op::PopJumpIfFalse(_)
                | Op::Swap
                | Op::Nop
                | Op::Return
                | Op::ReturnTrue
                | Op::ReturnFalse
                | Op::ReturnNull(_)
                | Op::CaseStart
                | Op::CaseWhen(_)
                | Op::CaseThen(_)
                | Op::CaseElse
                | Op::CaseEnd => 0,
            };

            depth += effect;
            max_depth = max_depth.max(depth);
        }

        // Ensure at least 1 for safety
        (max_depth as usize).max(1)
    }

    /// Disassemble the program for debugging
    pub fn disassemble(&self) -> String {
        let mut result = String::new();
        for (i, op) in self.ops.iter().enumerate() {
            result.push_str(&format!("{:04}: {:?}\n", i, op));
        }
        result
    }

    /// Apply peephole optimizations to fuse common instruction patterns.
    /// This is called automatically when creating a program via `new()`.
    pub fn optimize(mut self) -> Self {
        self.ops = Self::peephole_optimize(self.ops);
        // Recalculate metadata after optimization
        self.max_stack_depth = Self::compute_stack_depth(&self.ops);
        self
    }

    /// Peephole optimizer: fuse common instruction patterns into single ops
    fn peephole_optimize(mut ops: Vec<Op>) -> Vec<Op> {
        if ops.len() < 2 {
            return ops;
        }

        // Build a set of positions that are jump targets - we can't fuse instructions
        // that are jump targets because that would make the jump land in the middle
        // of what becomes a single instruction.
        let mut jump_targets = FxHashSet::default();
        for op in &ops {
            match op {
                Op::And(t)
                | Op::Or(t)
                | Op::Jump(t)
                | Op::JumpIfTrue(t)
                | Op::JumpIfFalse(t)
                | Op::JumpIfNull(t)
                | Op::JumpIfNotNull(t)
                | Op::PopJumpIfTrue(t)
                | Op::PopJumpIfFalse(t)
                | Op::CaseWhen(t)
                | Op::CaseThen(t) => {
                    jump_targets.insert(*t as usize);
                }
                _ => {}
            }
        }

        let mut result = Vec::with_capacity(ops.len());
        // Map from old instruction position to new instruction position
        let mut position_map: Vec<usize> = Vec::with_capacity(ops.len());
        let mut i = 0;

        while i < ops.len() {
            // Record the new position for this old position
            let new_pos = result.len();

            // Pattern 1: LoadColumn + LoadConst + LoadConst + Between → BetweenColumnConst (4 ops → 1)
            if i + 3 < ops.len() {
                let is_safe = !jump_targets.contains(&i)
                    && !jump_targets.contains(&(i + 1))
                    && !jump_targets.contains(&(i + 2))
                    && !jump_targets.contains(&(i + 3));

                if is_safe {
                    if let (
                        Op::LoadColumn(col_idx),
                        Op::LoadConst(low_val),
                        Op::LoadConst(high_val),
                        Op::Between,
                    ) = (&ops[i], &ops[i + 1], &ops[i + 2], &ops[i + 3])
                    {
                        result.push(Op::BetweenColumnConst(
                            *col_idx,
                            low_val.clone(),
                            high_val.clone(),
                        ));
                        // All 4 positions map to this single new position
                        position_map.push(new_pos);
                        position_map.push(new_pos);
                        position_map.push(new_pos);
                        position_map.push(new_pos);
                        i += 4;
                        continue;
                    }
                }
            }

            // Pattern 2: LoadColumn + LoadConst + Compare → XxColumnConst (3 ops → 1)
            if i + 2 < ops.len() {
                let is_safe = !jump_targets.contains(&i)
                    && !jump_targets.contains(&(i + 1))
                    && !jump_targets.contains(&(i + 2));

                if is_safe {
                    if let (Op::LoadColumn(col_idx), Op::LoadConst(const_val)) =
                        (&ops[i], &ops[i + 1])
                    {
                        let fused = match &ops[i + 2] {
                            Op::Eq => Some(Op::EqColumnConst(*col_idx, const_val.clone())),
                            Op::Ne => Some(Op::NeColumnConst(*col_idx, const_val.clone())),
                            Op::Lt => Some(Op::LtColumnConst(*col_idx, const_val.clone())),
                            Op::Le => Some(Op::LeColumnConst(*col_idx, const_val.clone())),
                            Op::Gt => Some(Op::GtColumnConst(*col_idx, const_val.clone())),
                            Op::Ge => Some(Op::GeColumnConst(*col_idx, const_val.clone())),
                            _ => None,
                        };

                        if let Some(fused_op) = fused {
                            result.push(fused_op);
                            // All 3 positions map to this single new position
                            position_map.push(new_pos);
                            position_map.push(new_pos);
                            position_map.push(new_pos);
                            i += 3;
                            continue;
                        }
                    }
                }
            }

            // Pattern 3: LoadColumn + IsNull/IsNotNull/Like/InSet (2 ops → 1)
            if i + 1 < ops.len() {
                let is_safe = !jump_targets.contains(&i) && !jump_targets.contains(&(i + 1));

                if is_safe {
                    if let Op::LoadColumn(col_idx) = &ops[i] {
                        let fused = match &ops[i + 1] {
                            Op::IsNull => Some(Op::IsNullColumn(*col_idx)),
                            Op::IsNotNull => Some(Op::IsNotNullColumn(*col_idx)),
                            Op::Like(pattern, case_insensitive) => {
                                Some(Op::LikeColumn(*col_idx, pattern.clone(), *case_insensitive))
                            }
                            Op::InSet(set, has_null) => {
                                Some(Op::InSetColumn(*col_idx, set.clone(), *has_null))
                            }
                            _ => None,
                        };

                        if let Some(fused_op) = fused {
                            result.push(fused_op);
                            // Both positions map to this single new position
                            position_map.push(new_pos);
                            position_map.push(new_pos);
                            i += 2;
                            continue;
                        }
                    }
                }
            }

            // No fusion, copy the op
            result.push(std::mem::replace(&mut ops[i], Op::Nop));
            position_map.push(new_pos);
            i += 1;
        }

        // If we fused anything, we need to adjust jump targets
        if result.len() != ops.len() {
            Self::adjust_jump_targets(&mut result, &position_map);
        }

        result
    }

    /// Adjust jump targets after peephole optimization using the position map.
    fn adjust_jump_targets(ops: &mut [Op], position_map: &[usize]) {
        for op in ops.iter_mut() {
            match op {
                Op::And(t)
                | Op::Or(t)
                | Op::Jump(t)
                | Op::JumpIfTrue(t)
                | Op::JumpIfFalse(t)
                | Op::JumpIfNull(t)
                | Op::JumpIfNotNull(t)
                | Op::PopJumpIfTrue(t)
                | Op::PopJumpIfFalse(t)
                | Op::CaseWhen(t)
                | Op::CaseThen(t) => {
                    let old_target = *t as usize;
                    if old_target < position_map.len() {
                        *t = u16::try_from(position_map[old_target]).unwrap_or(u16::MAX);
                    }
                }
                _ => {}
            }
        }
    }
}

impl std::fmt::Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Program")
            .field("ops_count", &self.ops.len())
            .field("max_stack_depth", &self.max_stack_depth)
            .field("needs_outer_context", &self.needs_outer_context)
            .field("needs_second_row", &self.needs_second_row)
            .finish()
    }
}

/// Builder for constructing programs
pub struct ProgramBuilder {
    ops: Vec<Op>,
    overflowed: bool,
    invalid_patch: Option<String>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self {
            ops: Vec::with_capacity(32),
            overflowed: false,
            invalid_patch: None,
        }
    }

    /// Emit an operation
    #[inline]
    pub fn emit(&mut self, op: Op) {
        if self.ops.len() >= u16::MAX as usize {
            self.overflowed = true;
        }
        self.ops.push(op);
    }

    /// Get current position (for jump targets)
    #[inline]
    pub fn position(&self) -> u16 {
        u16::try_from(self.ops.len()).unwrap_or(u16::MAX)
    }

    /// Whether an instruction or jump target exceeded the bytecode ABI.
    pub fn is_overflowed(&self) -> bool {
        self.overflowed
    }

    /// Patch a jump target at a specific position
    pub fn patch_jump(&mut self, pos: usize, target: u16) {
        let Some(op) = self.ops.get_mut(pos) else {
            self.invalid_patch = Some(format!(
                "cannot patch expression jump at instruction {pos}: program has {} instructions",
                self.ops.len()
            ));
            return;
        };

        match op {
            Op::And(t)
            | Op::Or(t)
            | Op::Jump(t)
            | Op::JumpIfTrue(t)
            | Op::JumpIfFalse(t)
            | Op::JumpIfNull(t)
            | Op::JumpIfNotNull(t)
            | Op::PopJumpIfTrue(t)
            | Op::PopJumpIfFalse(t)
            | Op::CaseWhen(t)
            | Op::CaseThen(t) => *t = target,
            other => {
                self.invalid_patch = Some(format!(
                    "cannot patch non-jump expression instruction at {pos}: {other:?}"
                ));
            }
        }
    }

    /// Validate and build the final program.
    pub fn build(self) -> Result<Program> {
        if let Some(message) = self.invalid_patch {
            return Err(Error::invalid_argument(message));
        }
        if self.overflowed {
            return Err(Error::invalid_argument(
                "expression bytecode exceeds the u16 instruction limit",
            ));
        }
        Program::try_new(self.ops)
    }

    /// Validate and build without peephole optimization (for tiny fold programs).
    pub fn build_unoptimized(self) -> Result<Program> {
        if let Some(message) = self.invalid_patch {
            return Err(Error::invalid_argument(message));
        }
        if self.overflowed {
            return Err(Error::invalid_argument(
                "expression bytecode exceeds the u16 instruction limit",
            ));
        }
        Program::try_new_unoptimized(self.ops)
    }
}

impl Default for ProgramBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Program factory methods
    // =========================================================================

    #[test]
    fn test_program_null() {
        let prog = Program::null();
        assert!(!prog.is_empty());
        assert!(!prog.needs_outer_context());
        assert!(!prog.needs_second_row());
    }

    #[test]
    fn test_program_constant() {
        let prog = Program::constant(Value::Integer(42));
        assert!(!prog.is_empty());
        assert!(!prog.needs_outer_context());
        assert!(!prog.needs_second_row());
    }

    #[test]
    fn test_program_always_true() {
        let prog = Program::always_true();
        assert_eq!(prog.len(), 1);
        assert!(matches!(prog.ops()[0], Op::ReturnTrue));
    }

    #[test]
    fn test_program_always_false() {
        let prog = Program::always_false();
        assert_eq!(prog.len(), 1);
        assert!(matches!(prog.ops()[0], Op::ReturnFalse));
    }

    // =========================================================================
    // Stack depth calculation
    // =========================================================================

    #[test]
    fn test_stack_depth_simple() {
        // LoadConst pushes 1, Return pops and returns
        let prog = Program::new_unoptimized(vec![Op::LoadConst(Value::Integer(1)), Op::Return]);
        assert_eq!(prog.max_stack_depth(), 1);
    }

    #[test]
    fn test_stack_depth_binary_op() {
        // LoadConst (+1), LoadConst (+1), Add (-1) = max 2
        let prog = Program::new_unoptimized(vec![
            Op::LoadConst(Value::Integer(1)),
            Op::LoadConst(Value::Integer(2)),
            Op::Add,
            Op::Return,
        ]);
        assert_eq!(prog.max_stack_depth(), 2);
    }

    #[test]
    fn test_stack_depth_nested() {
        // (1 + 2) * (3 + 4)
        // Load 1, Load 2, Add, Load 3, Load 4, Add, Mul
        let prog = Program::new_unoptimized(vec![
            Op::LoadConst(Value::Integer(1)),
            Op::LoadConst(Value::Integer(2)),
            Op::Add,
            Op::LoadConst(Value::Integer(3)),
            Op::LoadConst(Value::Integer(4)),
            Op::Add,
            Op::Mul,
            Op::Return,
        ]);
        // Stack trace: 1, 2, 1, 2, 3, 2, 1
        // Max is 3 (after loading 3rd value before second add)
        assert!(prog.max_stack_depth() >= 2);
    }

    #[test]
    fn test_stack_depth_fused_ops() {
        // Fused ops push 1
        let prog =
            Program::new_unoptimized(vec![Op::EqColumnConst(0, Value::Integer(5)), Op::Return]);
        assert_eq!(prog.max_stack_depth(), 1);
    }

    #[test]
    fn test_stack_depth_between() {
        // Between pops 3, pushes 1
        let prog = Program::new_unoptimized(vec![
            Op::LoadColumn(0),
            Op::LoadConst(Value::Integer(1)),
            Op::LoadConst(Value::Integer(10)),
            Op::Between,
            Op::Return,
        ]);
        assert_eq!(prog.max_stack_depth(), 3);
    }

    // =========================================================================
    // Metadata detection
    // =========================================================================

    #[test]
    fn test_needs_outer_context() {
        let prog = Program::new_unoptimized(vec![Op::LoadOuterColumn("col".into()), Op::Return]);
        assert!(prog.needs_outer_context());

        let prog2 = Program::new_unoptimized(vec![Op::LoadColumn(0), Op::Return]);
        assert!(!prog2.needs_outer_context());
    }

    #[test]
    fn test_needs_second_row() {
        let prog = Program::new_unoptimized(vec![
            Op::LoadColumn(0),
            Op::LoadColumn2(1),
            Op::Eq,
            Op::Return,
        ]);
        assert!(prog.needs_second_row());

        let prog2 = Program::new_unoptimized(vec![Op::LoadColumn(0), Op::Return]);
        assert!(!prog2.needs_second_row());
    }

    // =========================================================================
    // Peephole optimization
    // =========================================================================

    #[test]
    fn test_peephole_eq_column_const() {
        // LoadColumn + LoadConst + Eq should fuse to EqColumnConst
        let ops = vec![
            Op::LoadColumn(0),
            Op::LoadConst(Value::Integer(5)),
            Op::Eq,
            Op::Return,
        ];
        let prog = Program::new(ops);
        // Should be fused to 2 ops: EqColumnConst + Return
        assert_eq!(prog.len(), 2);
        assert!(matches!(prog.ops()[0], Op::EqColumnConst(0, _)));
    }

    #[test]
    fn test_peephole_lt_column_const() {
        let ops = vec![
            Op::LoadColumn(1),
            Op::LoadConst(Value::Integer(10)),
            Op::Lt,
            Op::Return,
        ];
        let prog = Program::new(ops);
        assert_eq!(prog.len(), 2);
        assert!(matches!(prog.ops()[0], Op::LtColumnConst(1, _)));
    }

    #[test]
    fn test_peephole_is_null_column() {
        let ops = vec![Op::LoadColumn(2), Op::IsNull, Op::Return];
        let prog = Program::new(ops);
        assert_eq!(prog.len(), 2);
        assert!(matches!(prog.ops()[0], Op::IsNullColumn(2)));
    }

    #[test]
    fn test_peephole_between_column_const() {
        let ops = vec![
            Op::LoadColumn(0),
            Op::LoadConst(Value::Integer(1)),
            Op::LoadConst(Value::Integer(100)),
            Op::Between,
            Op::Return,
        ];
        let prog = Program::new(ops);
        // Should fuse 4 ops to 1 BetweenColumnConst
        assert_eq!(prog.len(), 2);
        assert!(matches!(prog.ops()[0], Op::BetweenColumnConst(0, _, _)));
    }

    #[test]
    fn test_peephole_no_fusion_when_not_applicable() {
        // Can't fuse when pattern doesn't match
        let ops = vec![
            Op::LoadConst(Value::Integer(5)),
            Op::LoadColumn(0),
            Op::Eq,
            Op::Return,
        ];
        let prog = Program::new(ops);
        // LoadConst + LoadColumn + Eq doesn't match (order is wrong)
        assert!(prog.len() >= 3);
    }

    #[test]
    fn test_peephole_preserves_jumps() {
        // Ensure jump targets aren't fused over
        let ops = vec![
            Op::LoadColumn(0),
            Op::JumpIfFalse(3), // Jump to position 3
            Op::LoadConst(Value::Integer(5)),
            Op::Eq, // This is position 3 - a jump target
            Op::Return,
        ];
        let prog = Program::new(ops);
        // The fusion should not break jump semantics
        assert!(prog.len() >= 3);
    }

    // =========================================================================
    // ProgramBuilder
    // =========================================================================

    #[test]
    fn test_builder_basic() {
        let mut builder = ProgramBuilder::new();
        builder.emit(Op::LoadConst(Value::Integer(42)));
        builder.emit(Op::Return);

        let prog = builder.build().unwrap();
        assert!(!prog.is_empty());
    }

    #[test]
    fn test_builder_position() {
        let mut builder = ProgramBuilder::new();
        assert_eq!(builder.position(), 0);

        builder.emit(Op::LoadColumn(0));
        assert_eq!(builder.position(), 1);

        builder.emit(Op::LoadColumn(1));
        assert_eq!(builder.position(), 2);
    }

    #[test]
    fn test_builder_patch_jump() {
        let mut builder = ProgramBuilder::new();
        builder.emit(Op::LoadColumn(0));
        builder.emit(Op::JumpIfFalse(0)); // Placeholder target
        let jump_pos = 1;
        builder.emit(Op::Pop);
        builder.emit(Op::LoadConst(Value::Integer(1)));
        let return_pos = builder.position();
        builder.emit(Op::Return);

        builder.patch_jump(jump_pos, return_pos);

        let prog = builder.build().unwrap();
        // After optimization, check that jump exists
        let has_jump = prog.ops().iter().any(|op| matches!(op, Op::JumpIfFalse(_)));
        assert!(has_jump || prog.len() < 4); // Either has jump or was optimized away
    }

    #[test]
    fn test_builder_rejects_invalid_jump_patch() {
        let mut missing = ProgramBuilder::new();
        missing.emit(Op::LoadConst(Value::Integer(1)));
        missing.emit(Op::Return);
        missing.patch_jump(7, 1);
        assert!(missing.build().is_err());

        let mut not_a_jump = ProgramBuilder::new();
        not_a_jump.emit(Op::LoadConst(Value::Integer(1)));
        not_a_jump.emit(Op::Return);
        not_a_jump.patch_jump(0, 1);
        assert!(not_a_jump.build_unoptimized().is_err());
    }

    #[test]
    fn test_builder_default() {
        let builder: ProgramBuilder = Default::default();
        assert!(builder.build().is_err());
    }

    // =========================================================================
    // Disassemble
    // =========================================================================

    #[test]
    fn test_disassemble() {
        let prog = Program::new_unoptimized(vec![
            Op::LoadColumn(0),
            Op::LoadConst(Value::Integer(5)),
            Op::Eq,
            Op::Return,
        ]);
        let disasm = prog.disassemble();
        assert!(disasm.contains("LoadColumn"));
        assert!(disasm.contains("LoadConst"));
        assert!(disasm.contains("Eq"));
        assert!(disasm.contains("Return"));
        // Check format has line numbers
        assert!(disasm.contains("0000:"));
        assert!(disasm.contains("0001:"));
    }

    // =========================================================================
    // Debug formatting
    // =========================================================================

    #[test]
    fn test_program_debug() {
        let prog = Program::constant(Value::Integer(42));
        let debug_str = format!("{:?}", prog);
        assert!(debug_str.contains("Program"));
        assert!(debug_str.contains("ops_count"));
        assert!(debug_str.contains("max_stack_depth"));
    }

    // =========================================================================
    // Constant enum
    // =========================================================================

    #[test]
    fn test_constant_value() {
        let c = Constant::Value(Value::Integer(42));
        assert!(matches!(c, Constant::Value(Value::Integer(42))));
    }

    #[test]
    fn test_constant_string() {
        let c = Constant::String("test".into());
        assert!(matches!(c, Constant::String(_)));
    }

    #[test]
    fn test_constant_clone() {
        let c1 = Constant::Value(Value::Text("hello".into()));
        let c2 = c1.clone();
        assert!(matches!(c2, Constant::Value(Value::Text(_))));
    }

    #[test]
    fn test_public_constructors_reject_malformed_programs() {
        assert!(Program::try_new(vec![]).is_err());
        assert!(Program::try_new_unoptimized(vec![Op::Add, Op::Return]).is_err());
        assert!(
            Program::try_new_unoptimized(vec![Op::LoadConst(Value::Integer(1)), Op::Jump(0),])
                .is_err()
        );
        assert!(Program::try_new_unoptimized(vec![Op::LoadConst(Value::Integer(1))]).is_err());
    }
}
