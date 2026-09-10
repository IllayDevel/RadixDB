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

//! SQL Parser - Main Parser struct and core parsing logic

use std::sync::LazyLock;

use rustc_hash::FxHashSet;

use super::ast::*;
use super::error::{ParseError, ParseErrors};
use super::lexer::Lexer;
use super::precedence::Precedence;
use super::token::{Position, Token, TokenType};

/// Reserved SQL keywords that cannot be used as identifiers (O(1) lookup)
static RESERVED_KEYWORDS: LazyLock<FxHashSet<&'static str>> = LazyLock::new(|| {
    [
        // Core SQL keywords that should never be identifiers
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "DROP",
        "TABLE",
        "INDEX",
        "VIEW",
        "EXTENSION",
        "PLANNER",
        "SUPPORT",
        "ALTER",
        "ADD",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "NULL",
        "TRUE",
        "FALSE",
        "AS",
        "ON",
        "JOIN",
        // LEFT and RIGHT are handled specially - they can be function names
        // or column names when not followed by JOIN
        "INNER",
        "OUTER",
        "FULL",
        "CROSS",
        "GROUP",
        "BY",
        "ORDER",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "DISTINCT",
        "ALL",
        "EXISTS",
        "IN",
        "BETWEEN",
        "LIKE",
        "GLOB",
        "REGEXP",
        "RLIKE",
        "IS",
        "ASC",
        "DESC",
        "NULLS",
        // FIRST and LAST are handled specially - they can be function names
        // or column names, or ORDER BY modifiers (NULLS FIRST/LAST)
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "IF",
        "WITH",
        "RECURSIVE",
    ]
    .into_iter()
    .collect()
});

const MAX_EXPRESSION_NESTING: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PositionalParameterStyle {
    Anonymous,
    Explicit,
}

/// SQL Parser using Pratt parsing algorithm
pub struct Parser {
    /// Original SQL source retained for diagnostics from the public Parser API.
    pub(crate) source: Box<str>,
    /// The lexer providing tokens
    lexer: Lexer,
    /// Current token being examined
    pub(crate) cur_token: Token,
    /// Next token (peek)
    pub(crate) peek_token: Token,
    /// Collected errors
    errors: Vec<ParseError>,
    /// Current clause context (for error messages and parameter tracking)
    pub(crate) current_clause: String,
    /// Parameter counter within current statement
    parameter_counter: usize,
    /// Positional placeholder syntax used by the current statement.
    pub(crate) positional_parameter_style: Option<PositionalParameterStyle>,
    /// Current recursive expression depth, bounded before entering parser frames.
    pub(crate) expression_depth: usize,
    /// Non-zero while parsing a stored procedural definition.
    pub(crate) procedural_definition_depth: usize,
    /// Number of non-comment tokens consumed from this source.
    pub(crate) token_count: usize,
}

impl Parser {
    fn next_parser_token(lexer: &mut Lexer) -> Token {
        loop {
            let token = lexer.next_token();
            if token.token_type != TokenType::Comment {
                return token;
            }
        }
    }

    /// Create a new parser for the given input
    pub fn new(input: &str) -> Self {
        let normalized = if input.contains('\r') {
            input.replace("\r\n", "\n").replace('\r', "\n")
        } else {
            input.to_owned()
        };
        let mut lexer = Lexer::new(&normalized);
        let cur_token = Self::next_parser_token(&mut lexer);
        let peek_token = Self::next_parser_token(&mut lexer);

        Parser {
            source: normalized.into(),
            lexer,
            cur_token,
            peek_token,
            errors: Vec::new(),
            current_clause: String::new(),
            parameter_counter: 1,
            positional_parameter_style: None,
            expression_depth: 0,
            procedural_definition_depth: 0,
            token_count: 2,
        }
    }

    /// Parse the input and return a Program
    pub fn parse_program(&mut self) -> Result<Program, ParseErrors> {
        // Pre-allocate for common case (most queries have 1 statement)
        let mut statements = Vec::with_capacity(1);

        while !self.cur_token_is(TokenType::Eof) {
            // Skip comments
            if self.cur_token_is(TokenType::Comment) {
                self.next_token();
                continue;
            }

            if let Some(stmt) = self.parse_statement() {
                statements.push(stmt);
            }

            if self.peek_token_is_punctuator(";") {
                // One explicit delimiter is required between adjacent statements.
                // Additional/trailing delimiters remain harmless.
                while self.peek_token_is_punctuator(";") {
                    self.next_token();
                }
                self.next_token();
            } else if self.peek_token_is(TokenType::Eof) {
                self.next_token();
            } else {
                self.add_error(format!(
                    "expected ';' between statements before {}",
                    Self::format_token_for_error(&self.peek_token)
                ));
                break;
            }
            self.parameter_counter = 1;
            self.positional_parameter_style = None;
        }

        if !self.errors.is_empty() {
            return Err(ParseErrors::from_errors_with_sql(
                self.errors.clone(),
                self.source.as_ref(),
            ));
        }

        Ok(Program { statements })
    }

    /// Advance to the next token
    pub(crate) fn next_token(&mut self) {
        let next = Self::next_parser_token(&mut self.lexer);
        self.cur_token = std::mem::replace(&mut self.peek_token, next);
        self.token_count = self.token_count.saturating_add(1);
        if self.procedural_definition_depth > 0
            && self.cur_token.token_type == TokenType::Parameter
            && !self.cur_token.literal.starts_with(':')
        {
            self.add_error_at(
                "stored procedural source cannot contain external '$n' or '?' parameters"
                    .to_string(),
                self.cur_token.position,
            );
        }
    }

    /// Check if the current token is of the given type
    pub(crate) fn cur_token_is(&self, t: TokenType) -> bool {
        self.cur_token.token_type == t
    }

    /// Check if the peek token is of the given type
    pub(crate) fn peek_token_is(&self, t: TokenType) -> bool {
        self.peek_token.token_type == t
    }

    /// Check if the current token can be used as an identifier
    /// This allows keywords like TIMESTAMP, DATE, etc. to be used as column/table names
    pub(crate) fn cur_token_is_identifier_like(&self) -> bool {
        match self.cur_token.token_type {
            TokenType::Identifier => true,
            TokenType::Keyword => {
                // Allow non-reserved keywords as identifiers
                // Reserved keywords that cannot be used as identifiers
                !Self::is_reserved_keyword(&self.cur_token.literal)
            }
            _ => false,
        }
    }

    /// Create an Identifier from the current token.
    /// Identifier::new automatically lowercases keyword tokens.
    pub(crate) fn cur_token_as_column_identifier(&self) -> Identifier {
        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone())
    }

    /// Parse the relation spelling at the current token into the legacy
    /// physical-name carrier used by SQL DDL/DML AST nodes.
    ///
    /// Catalog-backed routines already retain [`ObjectName`] components. The
    /// table executor still addresses ordinary storage tables by one string,
    /// so relation paths are preserved losslessly as `namespace.name` here
    /// instead of being misparsed as a column qualification. This keeps one
    /// lexer/parser and makes system relations such as `audit.event`
    /// reachable through ordinary SQL until table AST nodes themselves carry
    /// stable catalog identities.
    pub(crate) fn parse_relation_identifier_current(&mut self) -> Option<Identifier> {
        if !matches!(
            self.cur_token.token_type,
            TokenType::Identifier | TokenType::Keyword
        ) {
            self.add_error(format!(
                "expected relation name, got {}",
                Self::format_token_for_error(&self.cur_token)
            ));
            return None;
        }

        let token = self.cur_token.clone();
        let mut value = self.cur_token.literal.clone();
        while self.peek_token_is_punctuator(".") {
            self.next_token();
            if !self.expect_peek_identifier_like() {
                return None;
            }
            value.push('.');
            value.push_str(&self.cur_token.literal);
        }
        Some(Identifier::new(token, value))
    }

    /// Check if a keyword is truly reserved and cannot be used as an identifier
    /// Note: Some keywords like LEFT, RIGHT, FIRST, LAST are handled specially in
    /// parse_keyword_prefix() where they can be functions or identifiers.
    /// Uses O(1) HashSet lookup instead of O(n) match chain.
    pub(crate) fn is_reserved_keyword(keyword: &str) -> bool {
        // Use uppercase for case-insensitive comparison
        // Note: Keywords are typically already uppercase from the lexer
        RESERVED_KEYWORDS.contains(keyword.to_uppercase().as_str())
    }

    /// Check if the current token is a specific keyword
    pub(crate) fn cur_token_is_keyword(&self, keyword: &str) -> bool {
        self.cur_token.token_type == TokenType::Keyword
            && self.cur_token.literal.eq_ignore_ascii_case(keyword)
    }

    /// Check if the peek token is a specific keyword
    pub(crate) fn peek_token_is_keyword(&self, keyword: &str) -> bool {
        self.peek_token.token_type == TokenType::Keyword
            && self.peek_token.literal.eq_ignore_ascii_case(keyword)
    }

    /// Check if the current token is a specific punctuator
    pub(crate) fn cur_token_is_punctuator(&self, punc: &str) -> bool {
        self.cur_token.token_type == TokenType::Punctuator && self.cur_token.literal == punc
    }

    /// Check if the peek token is a specific punctuator
    pub(crate) fn peek_token_is_punctuator(&self, punc: &str) -> bool {
        self.peek_token.token_type == TokenType::Punctuator && self.peek_token.literal == punc
    }

    /// Check if the peek token is a specific operator
    pub(crate) fn peek_token_is_operator(&self, op: &str) -> bool {
        self.peek_token.token_type == TokenType::Operator && self.peek_token.literal == op
    }

    /// Check if the peek token can be used as an identifier (true identifier or non-reserved keyword)
    pub(crate) fn peek_token_is_identifier_like(&self) -> bool {
        match self.peek_token.token_type {
            TokenType::Identifier => true,
            TokenType::Keyword => !Self::is_reserved_keyword(&self.peek_token.literal),
            _ => false,
        }
    }

    /// Expect the peek token to be an identifier (or non-reserved keyword) and advance
    pub(crate) fn expect_peek_identifier_like(&mut self) -> bool {
        if self.peek_token_is_identifier_like() {
            self.next_token();
            true
        } else {
            self.peek_error(TokenType::Identifier);
            false
        }
    }

    /// Expect the peek token to be of a specific type and advance
    pub(crate) fn expect_peek(&mut self, t: TokenType) -> bool {
        if self.peek_token_is(t) {
            self.next_token();
            true
        } else {
            self.peek_error(t);
            false
        }
    }

    /// Expect the peek token to be a specific keyword and advance
    pub(crate) fn expect_keyword(&mut self, keyword: &str) -> bool {
        if self.peek_token_is_keyword(keyword) {
            self.next_token();
            true
        } else {
            self.add_error(format!(
                "expected {} after {}, got {}",
                keyword,
                self.cur_token.literal,
                Self::format_token_for_error(&self.peek_token)
            ));
            false
        }
    }

    /// Get the precedence of the peek token
    pub(crate) fn peek_precedence(&self) -> Precedence {
        match self.peek_token.token_type {
            TokenType::Operator => Precedence::for_operator(&self.peek_token.literal),
            TokenType::Keyword => Precedence::for_operator(&self.peek_token.literal),
            TokenType::Punctuator => {
                if self.peek_token.literal == "." {
                    Precedence::Dot
                } else if self.peek_token.literal == "(" {
                    Precedence::Call
                } else if self.peek_token.literal == "[" {
                    Precedence::Index
                } else {
                    Precedence::Lowest
                }
            }
            _ => Precedence::Lowest,
        }
    }

    /// Get the precedence of the current token
    pub(crate) fn cur_precedence(&self) -> Precedence {
        match self.cur_token.token_type {
            TokenType::Operator => Precedence::for_operator(&self.cur_token.literal),
            TokenType::Keyword => Precedence::for_operator(&self.cur_token.literal),
            TokenType::Punctuator => {
                if self.cur_token.literal == "." {
                    Precedence::Dot
                } else if self.cur_token.literal == "(" {
                    Precedence::Call
                } else if self.cur_token.literal == "[" {
                    Precedence::Index
                } else {
                    Precedence::Lowest
                }
            }
            _ => Precedence::Lowest,
        }
    }

    /// Add an error for unexpected peek token type
    pub(crate) fn peek_error(&mut self, expected: TokenType) {
        let position = self.peek_token.position;
        let expected_desc = match expected {
            TokenType::Identifier => "identifier (name)",
            TokenType::Keyword => "keyword",
            TokenType::Punctuator => "'(' or ')'",
            TokenType::String => "string literal",
            TokenType::Integer => "integer",
            TokenType::Float => "number",
            _ => "token",
        };

        if self.peek_token.token_type == TokenType::Eof {
            if !self.current_clause.is_empty() {
                self.add_error_at(
                    format!("expected {} after {}", expected_desc, self.current_clause),
                    position,
                );
            } else {
                self.add_error_at(
                    format!("unexpected end of input, expected {}", expected_desc),
                    position,
                );
            }
        } else if expected == TokenType::Identifier
            && self.peek_token.token_type == TokenType::Keyword
            && Self::is_reserved_keyword(&self.peek_token.literal)
        {
            self.add_error_at(
                format!(
                    "'{}' is a reserved keyword and cannot be used as an identifier. \
                 Use double quotes to escape it: \"{}\"",
                    self.peek_token.literal.to_uppercase(),
                    self.peek_token.literal
                ),
                position,
            );
        } else {
            self.add_error_at(
                format!(
                    "expected {}, got {}",
                    expected_desc,
                    Self::format_token_for_error(&self.peek_token)
                ),
                position,
            );
        }
    }

    /// Format a token for display in error messages (shows "end of input" for EOF)
    pub(crate) fn format_token_for_error(token: &Token) -> String {
        if token.token_type == TokenType::Eof {
            "end of input".to_string()
        } else {
            format!("'{}'", token.literal)
        }
    }

    /// Add an error message
    pub(crate) fn add_error(&mut self, msg: String) {
        self.add_error_at(msg, self.cur_token.position);
    }

    pub(crate) fn add_error_at(&mut self, msg: String, position: super::token::Position) {
        self.errors.push(ParseError::new(msg, position));
    }

    pub(crate) fn source_range_from(&self, start: Position) -> SourceRange {
        SourceRange::new(start, self.peek_token.position)
    }

    pub(crate) fn source_range_through_peek_from(&self, start: Position) -> SourceRange {
        let mut end = self.peek_token.position;
        end.offset = end.offset.saturating_add(self.peek_token.literal.len());
        end.column = end
            .column
            .saturating_add(self.peek_token.literal.chars().count());
        SourceRange::new(start, end)
    }

    pub(crate) fn normalized_source_for(&self, range: &SourceRange) -> String {
        let start = range.start.offset.min(self.source.len());
        let end = range.end.offset.min(self.source.len());
        self.source[start..end].to_owned()
    }

    pub(crate) fn enter_expression(&mut self) -> bool {
        if self.expression_depth >= MAX_EXPRESSION_NESTING {
            self.add_error(format!(
                "expression nesting depth exceeds limit of {MAX_EXPRESSION_NESTING}"
            ));
            return false;
        }
        self.expression_depth += 1;
        true
    }

    /// Get collected errors
    pub fn errors(&self) -> &[ParseError] {
        &self.errors
    }

    /// Get the next parameter index
    pub(crate) fn next_parameter_index(&mut self) -> usize {
        let idx = self.parameter_counter;
        self.parameter_counter += 1;
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_creation() {
        let parser = Parser::new("SELECT * FROM users");
        assert!(parser.cur_token_is_keyword("SELECT"));
    }

    #[test]
    fn test_next_token() {
        let mut parser = Parser::new("SELECT * FROM users");
        assert!(parser.cur_token_is_keyword("SELECT"));
        parser.next_token();
        assert!(parser.cur_token_is(TokenType::Operator));
        assert_eq!(parser.cur_token.literal, "*");
    }

    #[test]
    fn test_peek_token() {
        let parser = Parser::new("SELECT * FROM users");
        assert!(parser.cur_token_is_keyword("SELECT"));
        assert!(parser.peek_token_is_operator("*"));
    }
}
