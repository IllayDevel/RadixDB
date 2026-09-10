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

//! Abstract syntax tree contracts for RadixDB SQL.

use crate::token::{Position, Token, TokenType};
use radixdb_core::{CompactArc, ForeignKeyAction, SmartString, Value, ValueSet};
use rustc_hash::{FxHashMap, FxHashSet};
use std::fmt;

mod control;
mod ddl;
mod dml;
mod expression;
mod procedural;
mod query;
mod security;
mod source;
mod statement;
mod visitor;

pub use control::*;
pub use ddl::*;
pub use dml::*;
pub use expression::*;
pub use procedural::*;
pub use query::*;
pub use security::*;
pub use source::*;
pub use statement::*;
pub use visitor::{
    walk_expression_tree, walk_expression_tree_mut, walk_physical_table_sources, walk_select_tree,
    walk_select_tree_mut, walk_statement_physical_table_sources, walk_statement_tree,
    walk_statement_tree_mut,
};

#[cfg(test)]
mod tests;

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
