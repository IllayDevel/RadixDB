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

//! ROW_NUMBER window function

use crate::{FunctionDataType, FunctionInfo, FunctionSignature, FunctionType, WindowFunction};
use radixdb_core::{Result, Value};

/// ROW_NUMBER window function
///
/// Returns the sequential row number within the current partition, starting at 1.
/// Unlike RANK(), ROW_NUMBER() assigns consecutive numbers even for equal values.
#[derive(Default)]
pub struct RowNumberFunction;

impl WindowFunction for RowNumberFunction {
    fn name(&self) -> &str {
        "ROW_NUMBER"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "ROW_NUMBER",
            FunctionType::Window,
            "Returns the sequential row number within the current partition",
            FunctionSignature::new(FunctionDataType::Integer, vec![], 0, 0),
        )
    }

    fn process(
        &self,
        partition: &[Value],
        _order_by: &[Value],
        current_row: usize,
    ) -> Result<Value> {
        super::validate_window_position(self.name(), partition.len(), current_row)?;
        let row_number = current_row
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| radixdb_core::Error::invalid_argument("ROW_NUMBER overflow"))?;
        Ok(Value::Integer(row_number))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_number_basic() {
        let f = RowNumberFunction;

        // Invalid positions are rejected by the public API.
        let partition = vec![];
        let order_by = vec![];
        assert!(f.process(&partition, &order_by, 0).is_err());
    }

    #[test]
    fn test_row_number_with_partition() {
        let f = RowNumberFunction;

        // ROW_NUMBER doesn't actually use partition values,
        // it just uses the current_row position
        let partition = vec![
            Value::Integer(100),
            Value::Integer(200),
            Value::Integer(300),
        ];
        let order_by = vec![];

        assert_eq!(
            f.process(&partition, &order_by, 0).unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            f.process(&partition, &order_by, 1).unwrap(),
            Value::Integer(2)
        );
        assert_eq!(
            f.process(&partition, &order_by, 2).unwrap(),
            Value::Integer(3)
        );
    }

    #[test]
    fn test_row_number_info() {
        let f = RowNumberFunction;
        let info = f.info();
        assert_eq!(info.name(), "ROW_NUMBER");
        assert_eq!(info.function_type(), FunctionType::Window);
    }
}
