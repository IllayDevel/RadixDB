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

//! Function Registry
//!
//! This module provides the function registry for looking up and managing
//! SQL functions (aggregate, scalar, and window functions).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use ahash::AHashMap;

type StringMap<V> = AHashMap<String, V>;

/// Global function registry instance
static GLOBAL_REGISTRY: OnceLock<FunctionRegistry> = OnceLock::new();

/// Get the global function registry
#[inline]
pub fn global_registry() -> &'static FunctionRegistry {
    GLOBAL_REGISTRY.get_or_init(FunctionRegistry::new)
}

use super::aggregate::{
    ArrayAggFunction, AvgFunction, CountFunction, FirstFunction, GroupConcatFunction, LastFunction,
    MaxFunction, MedianFunction, MinFunction, StddevFunction, StddevPopFunction,
    StddevSampFunction, StringAggFunction, SumFunction, VarPopFunction, VarSampFunction,
    VarianceFunction,
};
#[cfg(feature = "semantic")]
use super::scalar::semantic::EmbedFunction;
use super::scalar::vector::{
    VecDimsFunction, VecDistanceCosineFunction, VecDistanceIpFunction, VecDistanceL2Function,
    VecNormFunction, VecToTextFunction,
};
use super::scalar::{
    AbsFunction, CastFunction, CeilFunction, CeilingFunction, CharFunction, CharLengthFunction,
    CoalesceFunction, CollateFunction, ConcatFunction, ConcatWsFunction, ContainsFunction,
    CosFunction, Crc32Function, CurrentDateFunction, CurrentTimeFunction, CurrentTimestampFunction,
    DateAddFunction, DateDiffAliasFunction, DateDiffFunction, DateSubFunction, DateTruncFunction,
    DayFunction, EndsWithFunction, ExpFunction, ExtractFunction, FloorFunction, FromHexFunction,
    GreatestFunction, HourFunction, IfNullFunction, IifFunction, InstrFunction, JsonArrayFunction,
    JsonArrayLengthFunction, JsonExtractFunction, JsonKeysFunction, JsonObjectFunction,
    JsonTypeFunction, JsonTypeOfFunction, JsonValidFunction, LeastFunction, LeftFunction,
    LengthFunction, LnFunction, LocateFunction, Log10Function, Log2Function, LogFunction,
    LowerFunction, LpadFunction, LtrimFunction, Md5Function, MinuteFunction, ModFunction,
    MonthFunction, NowFunction, NullIfFunction, PiFunction, PositionFunction, PowFunction,
    PowerFunction, RandomFunction, RepeatFunction, ReplaceFunction, ReverseFunction, RightFunction,
    RoundFunction, RpadFunction, RtrimFunction, SecondFunction, Sha1Function, Sha256Function,
    Sha384Function, Sha512Function, SignFunction, SinFunction, SleepFunction, SplitPartFunction,
    SqrtFunction, StartsWithFunction, StrposFunction, SubstrFunction, SubstringFunction,
    TanFunction, TimeTruncFunction, ToCharFunction, TrimFunction, TruncFunction, TruncateFunction,
    TypeOfFunction, UpperFunction, VersionFunction, YearFunction,
};
use super::tvf::{GenerateSeriesFunction, GenerateSeriesScalarFunction, TableValuedFunction};
use super::window::{
    CumeDistFunction, DenseRankFunction, FirstValueFunction, LagFunction, LastValueFunction,
    LeadFunction, NthValueFunction, NtileFunction, PercentRankFunction, RankFunction,
    RowNumberFunction,
};
use super::{AggregateFunction, FunctionInfo, ScalarFunction, WindowFunction};

/// Type alias for aggregate function factory
type AggregateFnFactory = Arc<dyn Fn() -> Box<dyn AggregateFunction> + Send + Sync>;
/// Type alias for scalar function factory
type ScalarFnFactory = Arc<dyn Fn() -> Box<dyn ScalarFunction> + Send + Sync>;
/// Type alias for window function factory
type WindowFnFactory = Arc<dyn Fn() -> Box<dyn WindowFunction> + Send + Sync>;
/// Type alias for table-valued function factory
type TvfFactory = Arc<dyn Fn() -> Box<dyn TableValuedFunction> + Send + Sync>;

#[derive(Default)]
struct RegistryState {
    aggregate_functions: StringMap<AggregateFnFactory>,
    scalar_functions: StringMap<ScalarFnFactory>,
    window_functions: StringMap<WindowFnFactory>,
    tvf_functions: StringMap<TvfFactory>,
    tvf_function_info: StringMap<FunctionInfo>,
    function_info: StringMap<FunctionInfo>,
}

/// Function registry for SQL functions
pub struct FunctionRegistry {
    /// One lock publishes implementation and metadata as one coherent identity.
    state: RwLock<RegistryState>,
    /// Changes whenever compilation-visible function identity changes.
    generation: AtomicU64,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    /// Create a new function registry with all built-in functions registered
    pub fn new() -> Self {
        let registry = Self {
            state: RwLock::new(RegistryState::default()),
            generation: AtomicU64::new(0),
        };

        // Register built-in aggregate functions
        registry.register_aggregate::<CountFunction>();
        registry.register_aggregate::<SumFunction>();
        registry.register_aggregate::<AvgFunction>();
        registry.register_aggregate::<MinFunction>();
        registry.register_aggregate::<MaxFunction>();
        registry.register_aggregate::<FirstFunction>();
        registry.register_aggregate::<LastFunction>();
        registry.register_aggregate::<StringAggFunction>();
        registry.register_aggregate::<GroupConcatFunction>();
        registry.register_aggregate::<ArrayAggFunction>();
        registry.register_aggregate::<StddevPopFunction>();
        registry.register_aggregate::<StddevFunction>();
        registry.register_aggregate::<StddevSampFunction>();
        registry.register_aggregate::<VarPopFunction>();
        registry.register_aggregate::<VarianceFunction>();
        registry.register_aggregate::<VarSampFunction>();
        registry.register_aggregate::<MedianFunction>();

        // Register built-in scalar functions
        // String functions
        registry.register_scalar::<UpperFunction>();
        registry.register_scalar::<LowerFunction>();
        registry.register_scalar::<LengthFunction>();
        registry.register_scalar::<CharLengthFunction>();
        registry.register_scalar::<CharFunction>();
        registry.register_scalar::<FromHexFunction>();
        registry.register_scalar::<ConcatFunction>();
        registry.register_scalar::<ConcatWsFunction>();
        registry.register_scalar::<SubstringFunction>();
        registry.register_scalar::<SubstrFunction>();
        registry.register_scalar::<TrimFunction>();
        registry.register_scalar::<LtrimFunction>();
        registry.register_scalar::<RtrimFunction>();
        registry.register_scalar::<ReplaceFunction>();
        registry.register_scalar::<ReverseFunction>();
        registry.register_scalar::<LeftFunction>();
        registry.register_scalar::<RightFunction>();
        registry.register_scalar::<RepeatFunction>();
        registry.register_scalar::<SplitPartFunction>();
        registry.register_scalar::<StartsWithFunction>();
        registry.register_scalar::<EndsWithFunction>();
        registry.register_scalar::<ContainsFunction>();
        registry.register_scalar::<PositionFunction>();
        registry.register_scalar::<StrposFunction>();
        registry.register_scalar::<InstrFunction>();
        registry.register_scalar::<LocateFunction>();
        registry.register_scalar::<LpadFunction>();
        registry.register_scalar::<RpadFunction>();

        // Math functions
        registry.register_scalar::<AbsFunction>();
        registry.register_scalar::<RoundFunction>();
        registry.register_scalar::<FloorFunction>();
        registry.register_scalar::<CeilingFunction>();
        registry.register_scalar::<CeilFunction>();
        registry.register_scalar::<ModFunction>();
        registry.register_scalar::<PowerFunction>();
        registry.register_scalar::<PowFunction>();
        registry.register_scalar::<SqrtFunction>();
        registry.register_scalar::<LogFunction>();
        registry.register_scalar::<Log10Function>();
        registry.register_scalar::<Log2Function>();
        registry.register_scalar::<LnFunction>();
        registry.register_scalar::<ExpFunction>();
        registry.register_scalar::<SignFunction>();
        registry.register_scalar::<TruncateFunction>();
        registry.register_scalar::<TruncFunction>();
        registry.register_scalar::<PiFunction>();
        registry.register_scalar::<RandomFunction>();
        registry.register_scalar::<SinFunction>();
        registry.register_scalar::<CosFunction>();
        registry.register_scalar::<TanFunction>();

        // Date/Time functions
        registry.register_scalar::<NowFunction>();
        registry.register_scalar::<CurrentDateFunction>();
        registry.register_scalar::<CurrentTimeFunction>();
        registry.register_scalar::<CurrentTimestampFunction>();
        registry.register_scalar::<DateTruncFunction>();
        registry.register_scalar::<TimeTruncFunction>();
        registry.register_scalar::<ExtractFunction>();
        registry.register_scalar::<YearFunction>();
        registry.register_scalar::<MonthFunction>();
        registry.register_scalar::<DayFunction>();
        registry.register_scalar::<HourFunction>();
        registry.register_scalar::<MinuteFunction>();
        registry.register_scalar::<SecondFunction>();
        registry.register_scalar::<DateAddFunction>();
        registry.register_scalar::<DateSubFunction>();
        registry.register_scalar::<DateDiffFunction>();
        registry.register_scalar::<DateDiffAliasFunction>(); // DATE_DIFF alias for DATEDIFF
        registry.register_scalar::<ToCharFunction>();

        // Utility/System functions
        registry.register_scalar::<VersionFunction>();
        registry.register_scalar::<CoalesceFunction>();
        registry.register_scalar::<NullIfFunction>();
        registry.register_scalar::<IfNullFunction>();
        registry.register_scalar::<CastFunction>();
        registry.register_scalar::<CollateFunction>();
        registry.register_scalar::<GreatestFunction>();
        registry.register_scalar::<LeastFunction>();
        registry.register_scalar::<IifFunction>();
        registry.register_scalar::<JsonExtractFunction>();
        registry.register_scalar::<JsonArrayLengthFunction>();
        registry.register_scalar::<JsonArrayFunction>();
        registry.register_scalar::<JsonObjectFunction>();
        registry.register_scalar::<JsonTypeFunction>();
        registry.register_scalar::<JsonTypeOfFunction>();
        registry.register_scalar::<JsonValidFunction>();
        registry.register_scalar::<JsonKeysFunction>();
        registry.register_scalar::<TypeOfFunction>();
        registry.register_scalar::<SleepFunction>();

        // Hash functions
        registry.register_scalar::<Md5Function>();
        registry.register_scalar::<Sha1Function>();
        registry.register_scalar::<Sha256Function>();
        registry.register_scalar::<Sha384Function>();
        registry.register_scalar::<Sha512Function>();
        registry.register_scalar::<Crc32Function>();

        // Register vector functions
        registry.register_scalar::<VecDistanceL2Function>();
        registry.register_scalar::<VecDistanceCosineFunction>();
        registry.register_scalar::<VecDistanceIpFunction>();
        registry.register_scalar::<VecDimsFunction>();
        registry.register_scalar::<VecNormFunction>();
        registry.register_scalar::<VecToTextFunction>();

        // Register semantic embedding function (requires --features semantic)
        #[cfg(feature = "semantic")]
        registry.register_scalar::<EmbedFunction>();

        // Register generate_series as scalar (returns JSON array for SELECT usage)
        registry.register_scalar::<GenerateSeriesScalarFunction>();

        // Register built-in window functions
        registry.register_window::<RowNumberFunction>();
        registry.register_window::<RankFunction>();
        registry.register_window::<DenseRankFunction>();
        registry.register_window::<NtileFunction>();
        registry.register_window::<LeadFunction>();
        registry.register_window::<LagFunction>();
        registry.register_window::<FirstValueFunction>();
        registry.register_window::<LastValueFunction>();
        registry.register_window::<NthValueFunction>();
        registry.register_window::<PercentRankFunction>();
        registry.register_window::<CumeDistFunction>();

        // Register built-in table-valued functions
        registry.register_tvf(
            "GENERATE_SERIES",
            Arc::new(|| Box::new(GenerateSeriesFunction)),
        );

        registry
    }

    /// Register an aggregate function
    #[inline]
    pub fn register_aggregate<F: AggregateFunction + Default + 'static>(&self) {
        let instance = F::default();
        self.register_aggregate_inner(
            instance.name().to_uppercase(),
            instance.info(),
            Arc::new(|| Box::new(F::default())),
        );
    }

    fn register_aggregate_inner(
        &self,
        name: String,
        info: FunctionInfo,
        factory: AggregateFnFactory,
    ) {
        let mut state = self.state.write().unwrap();
        state.scalar_functions.remove(&name);
        state.window_functions.remove(&name);
        state.aggregate_functions.insert(name.clone(), factory);
        state.function_info.insert(name, info);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Register a scalar function
    #[inline]
    pub fn register_scalar<F: ScalarFunction + Default + 'static>(&self) {
        let instance = F::default();
        self.register_scalar_inner(
            instance.name().to_uppercase(),
            instance.info(),
            Arc::new(|| Box::new(F::default())),
        );
    }

    /// Register a scalar factory whose implementation is constructed from
    /// captured immutable state rather than a concrete `Default` Rust type.
    /// Implementation and metadata are published under the same registry
    /// generation, so compiled consumers cannot observe a partial update.
    pub fn register_scalar_factory(
        &self,
        name: impl Into<String>,
        info: FunctionInfo,
        factory: impl Fn() -> Box<dyn ScalarFunction> + Send + Sync + 'static,
    ) {
        self.register_scalar_inner(name.into().to_uppercase(), info, Arc::new(factory));
    }

    fn register_scalar_inner(&self, name: String, info: FunctionInfo, factory: ScalarFnFactory) {
        let mut state = self.state.write().unwrap();
        state.aggregate_functions.remove(&name);
        state.window_functions.remove(&name);
        state.scalar_functions.insert(name.clone(), factory);
        state.function_info.insert(name, info);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Register a window function
    #[inline]
    pub fn register_window<F: WindowFunction + Default + 'static>(&self) {
        let instance = F::default();
        self.register_window_inner(
            instance.name().to_uppercase(),
            instance.info(),
            Arc::new(|| Box::new(F::default())),
        );
    }

    fn register_window_inner(&self, name: String, info: FunctionInfo, factory: WindowFnFactory) {
        let mut state = self.state.write().unwrap();
        state.aggregate_functions.remove(&name);
        state.scalar_functions.remove(&name);
        state.window_functions.insert(name.clone(), factory);
        state.function_info.insert(name, info);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Compilation epoch used to invalidate programs that embed function implementations.
    #[inline]
    #[doc(hidden)]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Get a new instance of an aggregate function by name
    pub fn get_aggregate(&self, name: &str) -> Option<Box<dyn AggregateFunction>> {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if let Some(f) = state.aggregate_functions.get(name) {
            return Some(f());
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.aggregate_functions.get(&upper).map(|f| f())
    }

    /// Get a new instance of a scalar function by name
    pub fn get_scalar(&self, name: &str) -> Option<Box<dyn ScalarFunction>> {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if let Some(f) = state.scalar_functions.get(name) {
            return Some(f());
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.scalar_functions.get(&upper).map(|f| f())
    }

    /// Get a new instance of a window function by name
    pub fn get_window(&self, name: &str) -> Option<Box<dyn WindowFunction>> {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if let Some(f) = state.window_functions.get(name) {
            return Some(f());
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.window_functions.get(&upper).map(|f| f())
    }

    /// Check if a function name is an aggregate function
    pub fn is_aggregate(&self, name: &str) -> bool {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if state.aggregate_functions.contains_key(name) {
            return true;
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.aggregate_functions.contains_key(&upper)
    }

    /// Check if a function name is a scalar function
    pub fn is_scalar(&self, name: &str) -> bool {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if state.scalar_functions.contains_key(name) {
            return true;
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.scalar_functions.contains_key(&upper)
    }

    /// Check if a function name is a window function
    pub fn is_window(&self, name: &str) -> bool {
        // OPTIMIZATION: Fast path - if name is already uppercase, avoid allocation
        let state = self.state.read().unwrap();
        if state.window_functions.contains_key(name) {
            return true;
        }
        // Slow path - try uppercase
        let upper = name.to_uppercase();
        state.window_functions.contains_key(&upper)
    }

    /// Register a table-valued function
    pub fn register_tvf(&self, name: &str, factory: TvfFactory) {
        let info = factory().info();
        let canonical_name = name.to_uppercase();
        let mut state = self.state.write().unwrap();
        state.tvf_functions.insert(canonical_name.clone(), factory);
        state.tvf_function_info.insert(canonical_name, info);
    }

    /// Get a new instance of a table-valued function by name
    pub fn get_tvf(&self, name: &str) -> Option<Box<dyn TableValuedFunction>> {
        let state = self.state.read().unwrap();
        if let Some(f) = state.tvf_functions.get(name) {
            return Some(f());
        }
        let upper = name.to_uppercase();
        state.tvf_functions.get(&upper).map(|f| f())
    }

    /// Check if a function name is a table-valued function
    pub fn is_tvf(&self, name: &str) -> bool {
        let state = self.state.read().unwrap();
        if state.tvf_functions.contains_key(name) {
            return true;
        }
        let upper = name.to_uppercase();
        state.tvf_functions.contains_key(&upper)
    }

    /// Check if a function exists
    pub fn exists(&self, name: &str) -> bool {
        let state = self.state.read().unwrap();
        let upper;
        let name = if state.aggregate_functions.contains_key(name)
            || state.scalar_functions.contains_key(name)
            || state.window_functions.contains_key(name)
            || state.tvf_functions.contains_key(name)
        {
            name
        } else {
            upper = name.to_uppercase();
            upper.as_str()
        };
        state.aggregate_functions.contains_key(name)
            || state.scalar_functions.contains_key(name)
            || state.window_functions.contains_key(name)
            || state.tvf_functions.contains_key(name)
    }

    /// Get function info by name
    pub fn get_info(&self, name: &str) -> Option<FunctionInfo> {
        let name = name.to_uppercase();
        let state = self.state.read().unwrap();
        state.function_info.get(&name).cloned()
    }

    /// Return every overload advertised under a SQL function name.
    pub fn get_infos(&self, name: &str) -> Vec<FunctionInfo> {
        let name = name.to_uppercase();
        let state = self.state.read().unwrap();
        let mut infos = Vec::with_capacity(2);
        if let Some(info) = state.function_info.get(&name) {
            infos.push(info.clone());
        }
        if let Some(info) = state.tvf_function_info.get(&name) {
            infos.push(info.clone());
        }
        infos
    }

    /// Check if a function is deterministic (safe to constant-fold).
    /// Returns false for non-deterministic functions (NOW, RANDOM, SLEEP, etc.)
    /// and for unknown functions (conservative default).
    pub fn is_deterministic(&self, name: &str) -> bool {
        let state = self.state.read().unwrap();
        if let Some(info) = state.function_info.get(name) {
            return info.deterministic;
        }
        let upper = name.to_uppercase();
        state
            .function_info
            .get(&upper)
            .is_some_and(|info| info.deterministic)
    }

    /// List all aggregate function names
    pub fn list_aggregates(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .aggregate_functions
            .keys()
            .cloned()
            .collect()
    }

    /// List all scalar function names
    pub fn list_scalars(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .scalar_functions
            .keys()
            .cloned()
            .collect()
    }

    /// List all table-valued function names.
    pub fn list_tvfs(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .tvf_functions
            .keys()
            .cloned()
            .collect()
    }

    /// List all window function names
    pub fn list_windows(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .window_functions
            .keys()
            .cloned()
            .collect()
    }

    /// List all function names
    pub fn list_all(&self) -> Vec<String> {
        let mut names = Vec::new();
        names.extend(self.list_aggregates());
        names.extend(self.list_scalars());
        names.extend(self.list_windows());
        names.extend(self.list_tvfs());
        names.sort();
        names.dedup();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{Result, Value};

    #[derive(Default)]
    struct CountScalar;

    struct CapturedScalar {
        value: i64,
    }

    impl ScalarFunction for CountScalar {
        fn name(&self) -> &str {
            "COUNT"
        }

        fn info(&self) -> FunctionInfo {
            FunctionInfo::new(
                "COUNT",
                super::super::FunctionType::Scalar,
                "test replacement",
                super::super::FunctionSignature::new(
                    super::super::FunctionDataType::Integer,
                    vec![],
                    0,
                    0,
                ),
            )
        }

        fn evaluate(&self, _args: &[Value]) -> Result<Value> {
            Ok(Value::Integer(7))
        }
    }

    impl ScalarFunction for CapturedScalar {
        fn name(&self) -> &str {
            "CAPTURED"
        }

        fn info(&self) -> FunctionInfo {
            FunctionInfo::new(
                "CAPTURED",
                super::super::FunctionType::Scalar,
                "captured immutable state",
                super::super::FunctionSignature::new(
                    super::super::FunctionDataType::Integer,
                    vec![],
                    0,
                    0,
                ),
            )
        }

        fn evaluate(&self, _args: &[Value]) -> Result<Value> {
            Ok(Value::Integer(self.value))
        }
    }

    #[test]
    fn test_registry_new() {
        let registry = FunctionRegistry::new();
        assert!(registry.is_aggregate("COUNT"));
        assert!(registry.is_aggregate("SUM"));
        assert!(registry.is_aggregate("AVG"));
        assert!(registry.is_aggregate("MIN"));
        assert!(registry.is_aggregate("MAX"));
    }

    #[test]
    fn test_registry_case_insensitive() {
        let registry = FunctionRegistry::new();
        assert!(registry.is_aggregate("count"));
        assert!(registry.is_aggregate("COUNT"));
        assert!(registry.is_aggregate("Count"));
    }

    #[test]
    fn test_get_aggregate() {
        let registry = FunctionRegistry::new();
        let count = registry.get_aggregate("COUNT");
        assert!(count.is_some());
        assert_eq!(count.unwrap().name(), "COUNT");
    }

    #[test]
    fn test_get_scalar() {
        let registry = FunctionRegistry::new();
        let upper = registry.get_scalar("UPPER");
        assert!(upper.is_some());
        assert_eq!(upper.unwrap().name(), "UPPER");
    }

    #[test]
    fn test_get_window() {
        let registry = FunctionRegistry::new();
        let row_number = registry.get_window("ROW_NUMBER");
        assert!(row_number.is_some());
        assert_eq!(row_number.unwrap().name(), "ROW_NUMBER");
    }

    #[test]
    fn test_function_info() {
        let registry = FunctionRegistry::new();
        let info = registry.get_info("COUNT");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "COUNT");
    }

    #[test]
    fn test_list_functions() {
        let registry = FunctionRegistry::new();
        let aggregates = registry.list_aggregates();
        assert!(aggregates.contains(&"COUNT".to_string()));
        assert!(aggregates.contains(&"SUM".to_string()));

        let scalars = registry.list_scalars();
        assert!(scalars.contains(&"UPPER".to_string()));
        assert!(scalars.contains(&"LOWER".to_string()));

        let windows = registry.list_windows();
        assert!(windows.contains(&"ROW_NUMBER".to_string()));
    }

    #[test]
    fn test_global_registry() {
        let registry = global_registry();
        assert!(registry.is_aggregate("COUNT"));
        assert!(registry.is_scalar("UPPER"));
        assert!(registry.is_window("ROW_NUMBER"));
    }

    #[test]
    fn test_exists() {
        let registry = FunctionRegistry::new();
        assert!(registry.exists("COUNT"));
        assert!(registry.exists("UPPER"));
        assert!(registry.exists("ROW_NUMBER"));
        assert!(!registry.exists("NONEXISTENT"));
    }

    #[test]
    fn registry_replacement_is_unique_and_advances_generation() {
        let registry = FunctionRegistry::new();
        let before = registry.generation();
        registry.register_scalar::<CountScalar>();
        assert!(registry.generation() > before);
        assert!(registry.get_aggregate("COUNT").is_none());
        assert!(registry.get_scalar("COUNT").is_some());
        assert_eq!(
            registry.get_info("COUNT").unwrap().function_type,
            super::super::FunctionType::Scalar
        );
    }

    #[test]
    fn scalar_factory_supports_non_default_captured_state_atomically() {
        let registry = FunctionRegistry::new();
        let before = registry.generation();
        let info = CapturedScalar { value: 0 }.info();
        let captured = 73;
        registry.register_scalar_factory("captured", info, move || {
            Box::new(CapturedScalar { value: captured })
        });

        assert!(registry.generation() > before);
        assert_eq!(
            registry
                .get_scalar("CAPTURED")
                .unwrap()
                .evaluate(&[])
                .unwrap(),
            Value::Integer(73)
        );
        assert_eq!(registry.get_info("captured").unwrap().name, "CAPTURED");
    }
}
