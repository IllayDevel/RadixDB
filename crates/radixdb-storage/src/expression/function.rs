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

//! Function expression for evaluating scalar functions in WHERE clauses
//!
//! This module provides `FunctionExpr` which wraps a scalar function call
//! and evaluates it as a boolean expression. This enables queries like:
//!
//! ```sql
//! SELECT * FROM users WHERE UPPER(name) = 'ALICE'
//! SELECT * FROM products WHERE LENGTH(description) > 100
//! ```

use std::any::Any;
use std::fmt::{self, Debug};

use rustc_hash::FxHashMap;
use std::sync::Arc;

use super::{find_column_index, resolve_alias, Expression};
use radixdb_core::{Operator, Result, Row, Schema, Value};

/// Lower-layer callable contract used by storage predicates.
///
/// Registry metadata, volatility policy and SQL name resolution remain above
/// storage; this port only evaluates already-bound scalar arguments.
pub trait ScalarEvaluator: Send + Sync {
    fn name(&self) -> &str;
    fn evaluate(&self, args: &[Value]) -> Result<Value>;
}

/// Expression that evaluates a scalar function and compares the result
///
/// This expression wraps a scalar function call with its arguments,
/// evaluates the function for each row, and compares the result to
/// a target value using a comparison operator.
///
/// # Example
///
/// For `UPPER(name) = 'ALICE'`:
/// - function: UPPER
/// - arguments: [ColumnArg("name")]
/// - operator: Eq
/// - compare_value: Text("ALICE")
pub struct FunctionExpr {
    /// The scalar function to call
    function: Arc<dyn ScalarEvaluator>,
    /// Arguments to pass to the function
    arguments: Vec<FunctionArg>,
    /// Comparison operator
    operator: Operator,
    /// Value to compare the function result against
    compare_value: Value,
    /// Pre-computed column indices for arguments
    arg_bindings: Vec<FunctionArgBinding>,
    /// Whether this expression has been prepared
    prepared: bool,
}

#[derive(Clone, Debug)]
enum FunctionArgBinding {
    Column(Option<usize>),
    Literal,
    Function(Vec<FunctionArgBinding>),
}

impl FunctionArgBinding {
    fn unbound(arg: &FunctionArg) -> Self {
        match arg {
            FunctionArg::Column(_) => Self::Column(None),
            FunctionArg::Literal(_) => Self::Literal,
            FunctionArg::Function { arguments, .. } => {
                Self::Function(arguments.iter().map(Self::unbound).collect())
            }
        }
    }

    fn bind(arg: &FunctionArg, schema: &Schema) -> Self {
        match arg {
            FunctionArg::Column(column) => Self::Column(find_column_index(schema, column)),
            FunctionArg::Literal(_) => Self::Literal,
            FunctionArg::Function { arguments, .. } => Self::Function(
                arguments
                    .iter()
                    .map(|arg| Self::bind(arg, schema))
                    .collect(),
            ),
        }
    }
}

// Thread-local buffer for function arguments (avoids allocation per row)
thread_local! {
    static ARG_BUFFER: std::cell::RefCell<Vec<Value>> = std::cell::RefCell::new(Vec::with_capacity(4));
}

/// Argument to a function in a FunctionExpr
#[derive(Clone)]
pub enum FunctionArg {
    /// A column reference
    Column(String),
    /// A literal value
    Literal(Value),
    /// A nested function call (for function composition)
    Function {
        function: Arc<dyn ScalarEvaluator>,
        arguments: Vec<FunctionArg>,
    },
}

impl Debug for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArg::Column(col) => f.debug_tuple("Column").field(col).finish(),
            FunctionArg::Literal(val) => f.debug_tuple("Literal").field(val).finish(),
            FunctionArg::Function {
                function,
                arguments,
            } => f
                .debug_struct("Function")
                .field("name", &function.name())
                .field("arguments", arguments)
                .finish(),
        }
    }
}

impl FunctionArg {
    fn with_aliases(&self, aliases: &FxHashMap<String, String>) -> Self {
        match self {
            Self::Column(column) => Self::Column(resolve_alias(column, aliases).to_string()),
            Self::Literal(value) => Self::Literal(value.clone()),
            Self::Function {
                function,
                arguments,
            } => Self::Function {
                function: Arc::clone(function),
                arguments: arguments
                    .iter()
                    .map(|argument| argument.with_aliases(aliases))
                    .collect(),
            },
        }
    }
}

impl FunctionExpr {
    /// Create a new function expression
    ///
    /// # Arguments
    /// * `function` - The scalar function to call
    /// * `arguments` - Arguments to pass to the function
    /// * `operator` - Comparison operator
    /// * `compare_value` - Value to compare the function result against
    pub fn new(
        function: Arc<dyn ScalarEvaluator>,
        arguments: Vec<FunctionArg>,
        operator: Operator,
        compare_value: Value,
    ) -> Self {
        let arg_bindings = arguments.iter().map(FunctionArgBinding::unbound).collect();
        Self {
            function,
            arguments,
            operator,
            compare_value,
            arg_bindings,
            prepared: false,
        }
    }

    /// Create a function expression for equality comparison
    pub fn eq(
        function: Arc<dyn ScalarEvaluator>,
        arguments: Vec<FunctionArg>,
        compare_value: Value,
    ) -> Self {
        Self::new(function, arguments, Operator::Eq, compare_value)
    }

    /// Create a function expression that evaluates to boolean (no comparison)
    ///
    /// This is for functions that return boolean directly, like custom predicates
    pub fn boolean(function: Arc<dyn ScalarEvaluator>, arguments: Vec<FunctionArg>) -> Self {
        Self::new(function, arguments, Operator::Eq, Value::Boolean(true))
    }

    /// Get the function name
    pub fn function_name(&self) -> &str {
        self.function.name()
    }

    /// Get the arguments
    pub fn get_arguments(&self) -> &[FunctionArg] {
        &self.arguments
    }

    /// Get the operator
    pub fn get_operator(&self) -> Operator {
        self.operator
    }

    /// Get the compare value
    pub fn get_compare_value(&self) -> &Value {
        &self.compare_value
    }

    /// Evaluate a function argument to get its value
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_arg(
        &self,
        arg: &FunctionArg,
        binding: &FunctionArgBinding,
        row: &Row,
    ) -> Result<Value> {
        match (arg, binding) {
            (FunctionArg::Column(col_name), FunctionArgBinding::Column(arg_index)) => {
                if let Some(idx) = *arg_index {
                    Ok(row.get(idx).cloned().unwrap_or_else(Value::null_unknown))
                } else {
                    // Fallback: try to find column by name (shouldn't happen if prepared)
                    Err(radixdb_core::Error::ColumnNotFound(col_name.to_string()))
                }
            }
            (FunctionArg::Literal(value), FunctionArgBinding::Literal) => Ok(value.clone()),
            (
                FunctionArg::Function {
                    function,
                    arguments,
                },
                FunctionArgBinding::Function(bindings),
            ) => {
                // Recursively evaluate nested function
                let args: Result<Vec<Value>> = arguments
                    .iter()
                    .zip(bindings)
                    .map(|(arg, binding)| self.evaluate_arg(arg, binding, row))
                    .collect();
                function.evaluate(&args?)
            }
            _ => Err(radixdb_core::Error::InvalidArgument(
                "function argument binding shape mismatch".to_string(),
            )),
        }
    }

    fn evaluate_function(&self, row: &Row) -> Result<Value> {
        if self.arguments.len() == 1 {
            if let FunctionArg::Column(_) = &self.arguments[0] {
                if let Some(FunctionArgBinding::Column(Some(idx))) = self.arg_bindings.first() {
                    let value = row.get(*idx).cloned().unwrap_or_else(Value::null_unknown);
                    return self.function.evaluate(std::slice::from_ref(&value));
                }
            }
        }

        ARG_BUFFER.with(|buf_cell| {
            let mut arg_values = buf_cell.borrow_mut();
            arg_values.clear();
            for (index, arg) in self.arguments.iter().enumerate() {
                let binding = self.arg_bindings.get(index).ok_or_else(|| {
                    radixdb_core::Error::InvalidArgument(
                        "missing function argument binding".to_string(),
                    )
                })?;
                arg_values.push(self.evaluate_arg(arg, binding, row)?);
            }
            self.function.evaluate(&arg_values)
        })
    }

    /// Compare using SQL NULL and canonical scalar semantics.
    fn compare(&self, result: &Value, target: &Value) -> Result<Option<bool>> {
        if result.is_null() || target.is_null() {
            return Ok(None);
        }
        let ordering = result.compare(target)?;
        Ok(Some(match self.operator {
            Operator::Eq => ordering == std::cmp::Ordering::Equal,
            Operator::Ne => ordering != std::cmp::Ordering::Equal,
            Operator::Lt => ordering == std::cmp::Ordering::Less,
            Operator::Lte => ordering != std::cmp::Ordering::Greater,
            Operator::Gt => ordering == std::cmp::Ordering::Greater,
            Operator::Gte => ordering != std::cmp::Ordering::Less,
            _ => false,
        }))
    }
}

impl Debug for FunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionExpr")
            .field("function", &self.function.name())
            .field("arguments", &self.arguments)
            .field("operator", &self.operator)
            .field("compare_value", &self.compare_value)
            .field("prepared", &self.prepared)
            .finish()
    }
}

impl Expression for FunctionExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        let result = self.evaluate_function(row)?;
        Ok(self.compare(&result, &self.compare_value)?.unwrap_or(false))
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        self.evaluate(row).unwrap_or(false)
    }

    fn with_aliases(&self, aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        let new_arguments: Vec<FunctionArg> = self
            .arguments
            .iter()
            .map(|arg| arg.with_aliases(aliases))
            .collect();
        let arg_bindings = new_arguments
            .iter()
            .map(FunctionArgBinding::unbound)
            .collect();

        Box::new(FunctionExpr {
            function: Arc::clone(&self.function),
            arguments: new_arguments,
            operator: self.operator,
            compare_value: self.compare_value.clone(),
            arg_bindings,
            prepared: false,
        })
    }

    fn prepare_for_schema(&mut self, schema: &Schema) {
        self.arg_bindings = self
            .arguments
            .iter()
            .map(|arg| FunctionArgBinding::bind(arg, schema))
            .collect();
        self.prepared = true;
    }

    fn is_prepared(&self) -> bool {
        self.prepared
    }

    fn can_use_index(&self) -> bool {
        // Function expressions generally can't use indexes
        // unless we have function-based indexes (future optimization)
        false
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(FunctionExpr {
            function: Arc::clone(&self.function),
            arguments: self.arguments.clone(),
            operator: self.operator,
            compare_value: self.compare_value.clone(),
            arg_bindings: self.arg_bindings.clone(),
            prepared: self.prepared,
        })
    }

    fn is_unknown_due_to_null(&self, row: &Row) -> bool {
        self.compare_value.is_null()
            || self
                .evaluate_function(row)
                .ok()
                .is_some_and(|value| value.is_null())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Expression that wraps a closure for dynamic evaluation
///
/// This is useful for testing or when you need a custom predicate
/// that doesn't fit the standard expression types.
pub struct EvalExpr {
    /// The evaluation function
    eval_fn: Arc<dyn Fn(&Row) -> bool + Send + Sync>,
}

impl EvalExpr {
    /// Create a new eval expression from a closure
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&Row) -> bool + Send + Sync + 'static,
    {
        Self {
            eval_fn: Arc::new(f),
        }
    }
}

impl Debug for EvalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvalExpr").finish()
    }
}

impl Expression for EvalExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        Ok((self.eval_fn)(row))
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        (self.eval_fn)(row)
    }

    fn with_aliases(&self, _aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        self.clone_box()
    }

    fn prepare_for_schema(&mut self, _schema: &Schema) {
        // Nothing to prepare for closures
    }

    fn is_prepared(&self) -> bool {
        true // Always "prepared"
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(Self {
            eval_fn: Arc::clone(&self.eval_fn),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{DataType, SchemaBuilder};

    fn test_schema() -> Schema {
        SchemaBuilder::new("test")
            .add_primary_key("id", DataType::Integer)
            .add("name", DataType::Text)
            .add("age", DataType::Integer)
            .build()
    }

    // Helper function for testing - LENGTH function
    struct TestLengthFn;

    impl ScalarEvaluator for TestLengthFn {
        fn name(&self) -> &str {
            "LENGTH"
        }

        fn evaluate(&self, args: &[Value]) -> Result<Value> {
            match args.first() {
                Some(Value::Text(s)) => Ok(Value::Integer(s.len() as i64)),
                Some(Value::Null(_)) => Ok(Value::null_unknown()),
                _ => Ok(Value::Integer(0)),
            }
        }
    }

    struct TestIdentityFn;

    impl ScalarEvaluator for TestIdentityFn {
        fn name(&self) -> &str {
            "TEST_IDENTITY"
        }

        fn evaluate(&self, args: &[Value]) -> Result<Value> {
            Ok(args.first().cloned().unwrap_or_else(Value::null_unknown))
        }
    }

    struct TestUpperFn;

    impl ScalarEvaluator for TestUpperFn {
        fn name(&self) -> &str {
            "UPPER"
        }

        fn evaluate(&self, args: &[Value]) -> Result<Value> {
            match args.first() {
                Some(Value::Text(value)) => Ok(Value::text(value.to_uppercase())),
                Some(Value::Null(_)) => Ok(Value::null_unknown()),
                _ => Err(radixdb_core::Error::Type(
                    "UPPER test evaluator expects text".to_string(),
                )),
            }
        }
    }

    #[test]
    fn test_function_expr_upper() {
        let schema = test_schema();
        let upper_fn = Arc::new(TestUpperFn);

        let mut expr = FunctionExpr::eq(
            upper_fn,
            vec![FunctionArg::Column("name".to_string())],
            Value::text("ALICE"),
        );
        expr.prepare_for_schema(&schema);

        // Row with name = "alice" should match UPPER(name) = 'ALICE'
        let row1 = Row::from_values(vec![
            Value::Integer(1),
            Value::text("alice"),
            Value::Integer(30),
        ]);
        assert!(expr.evaluate(&row1).unwrap());

        // Row with name = "Alice" should also match
        let row2 = Row::from_values(vec![
            Value::Integer(2),
            Value::text("Alice"),
            Value::Integer(25),
        ]);
        assert!(expr.evaluate(&row2).unwrap());

        // Row with name = "bob" should not match
        let row3 = Row::from_values(vec![
            Value::Integer(3),
            Value::text("bob"),
            Value::Integer(35),
        ]);
        assert!(!expr.evaluate(&row3).unwrap());
    }

    #[test]
    fn test_function_expr_with_literal() {
        let schema = test_schema();
        let upper_fn = Arc::new(TestUpperFn);

        let mut expr = FunctionExpr::eq(
            upper_fn,
            vec![FunctionArg::Literal(Value::text("hello"))],
            Value::text("HELLO"),
        );
        expr.prepare_for_schema(&schema);

        // Should always match since it's comparing literals
        let row = Row::from_values(vec![
            Value::Integer(1),
            Value::text("anything"),
            Value::Integer(30),
        ]);
        assert!(expr.evaluate(&row).unwrap());
    }

    #[test]
    fn test_function_expr_operators() {
        let schema = test_schema();

        let length_fn = Arc::new(TestLengthFn);

        // Test LENGTH(name) > 3
        let mut expr = FunctionExpr::new(
            length_fn,
            vec![FunctionArg::Column("name".to_string())],
            Operator::Gt,
            Value::Integer(3),
        );
        expr.prepare_for_schema(&schema);

        // "alice" has length 5 > 3
        let row1 = Row::from_values(vec![
            Value::Integer(1),
            Value::text("alice"),
            Value::Integer(30),
        ]);
        assert!(expr.evaluate(&row1).unwrap());

        // "bob" has length 3, not > 3
        let row2 = Row::from_values(vec![
            Value::Integer(2),
            Value::text("bob"),
            Value::Integer(25),
        ]);
        assert!(!expr.evaluate(&row2).unwrap());
    }

    #[test]
    fn test_function_expr_mixed_numeric_identity_at_f64_boundary() {
        let schema = test_schema();
        let identity_fn = Arc::new(TestIdentityFn);
        let boundary = 1_i64 << 53;
        let row = Row::new();

        let mut exact_eq = FunctionExpr::eq(
            identity_fn.clone(),
            vec![FunctionArg::Literal(Value::Integer(boundary))],
            Value::Float(boundary as f64),
        );
        exact_eq.prepare_for_schema(&schema);
        assert!(exact_eq.evaluate(&row).unwrap());

        let mut rounded_eq = FunctionExpr::new(
            identity_fn.clone(),
            vec![FunctionArg::Literal(Value::Integer(boundary + 1))],
            Operator::Eq,
            Value::Float(boundary as f64),
        );
        rounded_eq.prepare_for_schema(&schema);
        assert!(!rounded_eq.evaluate(&row).unwrap());

        let mut rounded_gt = FunctionExpr::new(
            identity_fn,
            vec![FunctionArg::Literal(Value::Integer(boundary + 1))],
            Operator::Gt,
            Value::Float(boundary as f64),
        );
        rounded_gt.prepare_for_schema(&schema);
        assert!(rounded_gt.evaluate(&row).unwrap());
    }

    #[test]
    fn test_eval_expr() {
        let expr = EvalExpr::new(|row| {
            // Return true if first column is Integer > 5
            match row.get(0) {
                Some(Value::Integer(n)) => *n > 5,
                _ => false,
            }
        });

        let row1 = Row::from_values(vec![Value::Integer(10)]);
        assert!(expr.evaluate(&row1).unwrap());

        let row2 = Row::from_values(vec![Value::Integer(3)]);
        assert!(!expr.evaluate(&row2).unwrap());
    }

    #[test]
    fn test_function_expr_clone() {
        let upper_fn = Arc::new(TestUpperFn);

        let expr = FunctionExpr::eq(
            upper_fn,
            vec![FunctionArg::Column("name".to_string())],
            Value::text("ALICE"),
        );

        let cloned = expr.clone_box();
        assert!(format!("{:?}", cloned).contains("FunctionExpr"));
    }

    #[test]
    fn function_comparison_preserves_sql_null_unknown() {
        let schema = test_schema();
        let mut expr = FunctionExpr::eq(
            Arc::new(TestIdentityFn),
            vec![FunctionArg::Column("name".to_string())],
            Value::text("ALICE"),
        );
        expr.prepare_for_schema(&schema);
        let row = Row::from_values(vec![
            Value::integer(1),
            Value::null(DataType::Text),
            Value::integer(30),
        ]);
        assert!(!expr.evaluate(&row).unwrap());
        assert!(!expr.evaluate_fast(&row));
        assert!(expr.is_unknown_due_to_null(&row));

        let mut null_target = FunctionExpr::eq(
            Arc::new(TestIdentityFn),
            vec![FunctionArg::Literal(Value::integer(1))],
            Value::null(DataType::Integer),
        );
        null_target.prepare_for_schema(&schema);
        assert!(!null_target.evaluate(&Row::new()).unwrap());
        assert!(null_target.is_unknown_due_to_null(&Row::new()));
    }

    #[test]
    fn eval_expr_lifecycle_is_total() {
        let expr: Box<dyn Expression> = Box::new(EvalExpr::new(|row| !row.is_empty()));
        let cloned = expr.clone();
        let aliased = cloned.with_aliases(&FxHashMap::default());
        assert!(aliased.as_any().is::<EvalExpr>());
        assert!(aliased
            .evaluate(&Row::from_values(vec![Value::integer(1)]))
            .unwrap());
    }
}
