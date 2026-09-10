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

//! Canonical admission of scalar values to physical integer lookup keys.

use radixdb_core::Value;

/// Result of admitting a scalar equality operand to an INTEGER lookup.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerPkAdmission {
    /// The scalar has canonical INTEGER identity and may be probed directly.
    Exact(i64),
    /// A numeric scalar can never equal an INTEGER primary key.
    NoMatch,
}

/// Admit a script value to the physical INTEGER primary-key domain.
#[doc(hidden)]
#[inline]
pub fn integer_pk_admission(value: &Value) -> Option<IntegerPkAdmission> {
    match value {
        Value::Integer(_) | Value::Float(_) => Some(
            value
                .exact_integer_identity()
                .map_or(IntegerPkAdmission::NoMatch, IntegerPkAdmission::Exact),
        ),
        _ => None,
    }
}

/// Return the exact integer lookup key when the scalar has canonical identity.
#[doc(hidden)]
#[inline]
pub fn exact_integer_pk_value(value: &Value) -> Option<i64> {
    match integer_pk_admission(value)? {
        IntegerPkAdmission::Exact(integer) => Some(integer),
        IntegerPkAdmission::NoMatch => None,
    }
}
