// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Canonical SQL function contracts and built-in implementations for RadixDB.
//!
//! This private implementation crate owns the coherent function registry. It
//! never owns SELECT execution policy; external applications use the stable
//! re-exports from `radixdb`.

pub mod aggregate;
mod metadata;
pub mod registry;
pub mod scalar;
mod traits;
pub mod tvf;
mod version;
pub mod window;

pub use metadata::{
    AggregateOrderBySpec, FunctionDataType, FunctionInfo, FunctionReturnRule, FunctionSignature,
    FunctionType, FunctionVolatility,
};
pub use registry::{global_registry, FunctionRegistry};
pub use traits::{
    AggregateFunction, FunctionCancellation, NativeFn1, ScalarFunction, WindowFunction,
};
