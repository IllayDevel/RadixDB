//! Physical SELECT access-path ownership.
//!
//! This boundary owns query-local storage handles, predicate preparation,
//! index eligibility and construction of storage scan inputs. Relational row
//! processing remains in the next migration stages.

pub mod handle;
pub mod index;
pub mod predicate;
pub mod projection;
pub mod scan;
