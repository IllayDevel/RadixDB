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

//! Canonical SQL syntax contracts and parser for RadixDB.
//!
//! This private implementation crate owns tokens, lexer, AST, diagnostics and
//! parsing. It is not an application dependency and has no storage or query
//! execution policy.

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod precedence;
pub mod token;

mod expressions;
mod parse;
mod statements;

pub use ast::*;
pub use error::{ParseError, ParseErrors};
pub use lexer::Lexer;
pub use parse::parse_sql;
pub use parser::Parser;
pub use precedence::Precedence;
pub use token::{
    is_keyword, is_operator, is_operator_char, is_punctuator, punctuator_str, Position, Token,
    TokenType, KEYWORDS, OPERATORS, PUNCTUATORS,
};
