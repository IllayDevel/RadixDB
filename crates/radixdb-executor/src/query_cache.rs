//! Concrete parsed-plan cache aliases for the executor-owned navigation state.

pub use crate::dispatch::cache::{
    CacheStats, CompiledCountDistinct, CompiledCountStar, CompiledExecution, CompiledInsert,
    CompiledPkDelete, CompiledPkLookup, CompiledPkUpdate, CompiledUpdateColumn, ParameterContract,
    PkValueSource, UpdateValueSource, DEFAULT_CACHE_SIZE,
};

pub type QueryCache = crate::dispatch::cache::QueryCache<crate::navigation::CachedReferenceExpand>;
pub type CachedPlanRef =
    crate::dispatch::cache::CachedPlanRef<crate::navigation::CachedReferenceExpand>;
pub type CachedQueryPlan =
    crate::dispatch::cache::CachedQueryPlan<crate::navigation::CachedReferenceExpand>;
