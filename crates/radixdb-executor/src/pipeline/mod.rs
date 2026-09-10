//! Relational SELECT pipeline ownership.
//!
//! Physical result operators live in [`crate::result`]. This module owns the
//! SQL ordering between those operators, their row-shape contract, paging and
//! set algebra. Higher layers provide only recursive SELECT/subquery callbacks.

pub mod distinct;
pub mod filter;
pub mod ordering;
pub mod paging;
pub mod projection;
pub mod set;
pub mod shape;
