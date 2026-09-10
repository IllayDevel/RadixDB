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

//! Statement grammar grouped by functional owner.

use crate::ast::*;
use crate::parser::Parser;
use crate::precedence::Precedence;
use crate::token::{Token, TokenType};
use radixdb_core::{ForeignKeyAction, NavigationErrorCode, SmartString};
use rustc_hash::FxHashMap;

mod control;
mod ddl;
mod dispatch;
mod dml;
mod operator;
mod procedural;
mod query;
mod security;

#[cfg(test)]
mod tests;
