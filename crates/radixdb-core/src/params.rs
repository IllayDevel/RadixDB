//! Neutral execution parameter containers.

use smallvec::SmallVec;

use crate::Value;

/// Positional SQL parameters shared by API adapters and the executor.
///
/// The inline capacity is part of the existing allocation contract: ordinary
/// queries with at most eight parameters do not require a heap allocation.
pub type ParamVec = SmallVec<[Value; 8]>;
