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

//! SQL Lexer (Tokenizer)
//!
//! This module provides the lexer for tokenizing SQL input strings.

mod cursor;
mod identifier;
mod literal;
mod trivia;

use super::token::{
    is_keyword, is_operator, is_operator_char, is_punctuator, punctuator_str, Position, Token,
    TokenType,
};
use radixdb_core::SmartString;

/// SQL Lexer for tokenizing input
///
/// Uses byte-based indexing for efficiency. SQL is predominantly ASCII,
/// so we can work with bytes directly and only decode UTF-8 when needed.
pub struct Lexer {
    /// Input string as bytes (avoids `Vec<char>` allocation)
    input: Box<[u8]>,
    /// Current byte position in input
    position: usize,
    /// Next byte position in input (after current char)
    read_position: usize,
    /// Current character under examination
    ch: char,
    /// Whether `ch` is the synthetic end-of-input marker rather than U+0000
    eof: bool,
    /// Current position tracking
    pos: Position,
    /// Last error encountered
    last_error: Option<String>,
}

impl Lexer {
    /// Create a new lexer for the given input
    pub fn new(input: &str) -> Self {
        // Store as bytes - much more memory efficient than Vec<char>
        let bytes: Box<[u8]> = input.as_bytes().into();
        let mut lexer = Self {
            input: bytes,
            position: 0,
            read_position: 0,
            ch: '\0',
            eof: true,
            pos: Position::new(0, 1, 1),
            last_error: None,
        };
        lexer.read_char();
        lexer
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let pos = self.pos;

        match self.ch {
            '\0' if self.eof => Token::eof(pos),

            '\0' => {
                self.read_char();
                Token::error("NULL byte (0x00) is not allowed in SQL input", "", pos)
            }

            // String literal (single quotes)
            '\'' => {
                let literal = self.read_string_literal();
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new(TokenType::String, literal, pos)
            }

            // Double-quoted identifier (identifier with string fallback)
            '"' => {
                let literal = self.read_quoted_identifier('"');
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new_quoted(TokenType::Identifier, literal, pos)
            }

            // Backtick-quoted identifier (MySQL style)
            '`' => {
                let literal = self.read_quoted_identifier('`');
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new_quoted(TokenType::Identifier, literal, pos)
            }

            // Negative numbers: parser handles unary minus, not the lexer.

            // Number literal
            c if c.is_ascii_digit() => {
                let literal = self.read_number();
                if literal.contains('.') || literal.contains('e') || literal.contains('E') {
                    Token::new(TokenType::Float, literal, pos)
                } else {
                    Token::new(TokenType::Integer, literal, pos)
                }
            }

            // Single line comment (#)
            '#' => {
                let literal = self.read_line_comment();
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new(TokenType::Comment, literal, pos)
            }

            // Single line comment (--) per SQL standard (SQL:2023 section 5.2)
            // Double negation should be written as `- -val` or `- (-val)`
            '-' if self.peek_char() == '-' => {
                let literal = self.read_line_comment();
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new(TokenType::Comment, literal, pos)
            }

            // Multi-line comment
            '/' if self.peek_char() == '*' => {
                let literal = self.read_block_comment();
                if let Some(err) = self.last_error.take() {
                    return Token::error(err, "", pos);
                }
                Token::new(TokenType::Comment, literal, pos)
            }

            // Parameter ($1, $2, etc.)
            '$' if self.peek_char().is_ascii_digit() => {
                let literal = self.read_parameter();
                Token::new(TokenType::Parameter, literal, pos)
            }

            // Parameter (?)
            '?' => {
                self.read_char();
                Token::new(TokenType::Parameter, "?", pos)
            }

            // Named parameter (:name)
            ':' if self.peek_char().is_alphabetic() || self.peek_char() == '_' => {
                let literal = self.read_named_parameter();
                Token::new(TokenType::Parameter, literal, pos)
            }

            // Procedural assignment. This must precede punctuator handling so
            // longest-match keeps `:=` atomic while bare `:` remains intact.
            ':' if self.peek_char() == '=' => {
                let literal = self.read_operator();
                Token::new(TokenType::Operator, literal, pos)
            }

            // Star is always an operator (SELECT * handled by parser)
            '*' => {
                self.read_char();
                Token::new(TokenType::Operator, "*", pos)
            }

            // Regular punctuator - use static string to avoid allocation
            c if is_punctuator(c) => {
                self.read_char();
                // SAFETY: We already checked is_punctuator(c), so punctuator_str always returns Some
                Token::new(TokenType::Punctuator, punctuator_str(c).unwrap(), pos)
            }

            // Operator
            c if is_operator_char(c) => {
                let literal = self.read_operator();
                Token::new(TokenType::Operator, literal, pos)
            }

            // Identifier or keyword
            c if c.is_alphabetic() || c == '_' => {
                let literal = self.read_identifier();
                if is_keyword(&literal) {
                    Token::new(TokenType::Keyword, literal.to_uppercase(), pos)
                } else {
                    Token::new(TokenType::Identifier, literal, pos)
                }
            }

            // Unrecognized character
            c => {
                self.read_char();
                Token::error(
                    format!("unrecognized character: {:?}", c),
                    c.to_string(),
                    pos,
                )
            }
        }
    }

    /// Read an operator
    fn read_operator(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        let first_char = self.ch;
        result.push(first_char);
        self.read_char();

        // Check for multi-character operators
        if !self.eof {
            let two_chars: SmartString =
                SmartString::from_iter([first_char, self.ch].iter().copied());
            if is_operator(&two_chars) {
                result.push(self.ch);
                self.read_char();

                // Check for three-character operators
                if !self.eof {
                    let mut three_chars = two_chars.clone();
                    three_chars.push(self.ch);
                    if is_operator(&three_chars) {
                        result.push(self.ch);
                        self.read_char();
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        assert_eq!(token.literal, "SELECT");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Operator);
        assert_eq!(token.literal, "*");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        assert_eq!(token.literal, "FROM");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Identifier);
        assert_eq!(token.literal, "users");

        let token = lexer.next_token();
        assert!(token.is_eof());
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("123 45.67 -89 3.14e10 1.5E-3");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Integer);
        assert_eq!(token.literal, "123");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Float);
        assert_eq!(token.literal, "45.67");

        // Negative numbers are tokenized as operator + number (parser handles unary minus)
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Operator);
        assert_eq!(token.literal, "-");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Integer);
        assert_eq!(token.literal, "89");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Float);
        assert_eq!(token.literal, "3.14e10");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Float);
        assert_eq!(token.literal, "1.5E-3");
    }

    #[test]
    fn test_string_literals() {
        let mut lexer = Lexer::new("'hello' 'world''s' 'escaped\\ntext'");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::String);
        assert_eq!(token.literal, "'hello'");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::String);
        assert_eq!(token.literal, "'world's'");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::String);
        assert_eq!(token.literal, "'escaped\\ntext'");
    }

    #[test]
    fn test_quoted_identifiers() {
        let mut lexer = Lexer::new("\"table name\" `column`");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Identifier);
        assert_eq!(token.literal, "table name");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Identifier);
        assert_eq!(token.literal, "column");
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("= <> >= <= != + - * / || -> ->>");

        let expected = vec![
            "=", "<>", ">=", "<=", "!=", "+", "-", "*", "/", "||", "->", "->>",
        ];

        for exp in expected {
            let token = lexer.next_token();
            assert_eq!(token.token_type, TokenType::Operator);
            assert_eq!(token.literal, exp);
        }
    }

    #[test]
    fn test_punctuators() {
        let mut lexer = Lexer::new("( ) , ; . [ ]");

        let expected = vec!["(", ")", ",", ";", ".", "[", "]"];

        for exp in expected {
            let token = lexer.next_token();
            assert_eq!(token.token_type, TokenType::Punctuator);
            assert_eq!(token.literal, exp);
        }
    }

    #[test]
    fn test_comments() {
        let mut lexer = Lexer::new("-- line comment\nSELECT /* block */ 1");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);
        assert!(token.literal.contains("line comment"));

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        assert_eq!(token.literal, "SELECT");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);
        assert!(token.literal.contains("block"));

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Integer);
        assert_eq!(token.literal, "1");
    }

    #[test]
    fn test_double_dash_is_always_comment() {
        // Per SQL standard (SQL:2023 5.2), -- always starts a line comment
        // regardless of what follows the dashes.

        // --5 is a comment, not double negation
        let mut lexer = Lexer::new("SELECT --5");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        assert_eq!(token.literal, "SELECT");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);

        // --val is a comment, not double negation
        let mut lexer = Lexer::new("SELECT --val");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);

        // --comment at start of input is a comment
        let mut lexer = Lexer::new("--comment\nSELECT 1");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        assert_eq!(token.literal, "SELECT");

        // -- with space is still a comment
        let mut lexer = Lexer::new("SELECT -- comment");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Comment);

        // Double negation must use `- -` with a space
        let mut lexer = Lexer::new("SELECT - -5");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Operator);
        assert_eq!(token.literal, "-");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Operator);
        assert_eq!(token.literal, "-");
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Integer);
        assert_eq!(token.literal, "5");
    }

    #[test]
    fn test_parameters() {
        let mut lexer = Lexer::new("$1 $23 ? :name :user_id :_private");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, "$1");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, "$23");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, "?");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, ":name");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, ":user_id");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Parameter);
        assert_eq!(token.literal, ":_private");
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let mut lexer = Lexer::new("select SELECT Select");

        for _ in 0..3 {
            let token = lexer.next_token();
            assert_eq!(token.token_type, TokenType::Keyword);
            assert_eq!(token.literal, "SELECT");
        }
    }

    #[test]
    fn test_position_tracking() {
        let mut lexer = Lexer::new("SELECT\nFROM");

        let token = lexer.next_token();
        assert_eq!(token.position.line, 1);
        assert_eq!(token.position.column, 1);

        let token = lexer.next_token();
        assert_eq!(token.position.line, 2);
        assert_eq!(token.position.column, 1);
    }

    #[test]
    fn test_complex_query() {
        let query = r#"
            SELECT u.id, u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = TRUE AND o.amount >= 100.50
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY order_count DESC
            LIMIT 10
        "#;

        let mut lexer = Lexer::new(query);
        let mut tokens = Vec::new();

        loop {
            let token = lexer.next_token();
            if token.is_eof() {
                break;
            }
            tokens.push(token);
        }

        // Verify we got reasonable tokens
        assert!(tokens.len() > 30);
        assert!(tokens.iter().any(|t| t.is_keyword("SELECT")));
        assert!(tokens.iter().any(|t| t.is_keyword("FROM")));
        assert!(tokens.iter().any(|t| t.is_keyword("JOIN")));
        assert!(tokens.iter().any(|t| t.is_keyword("WHERE")));
        assert!(tokens.iter().any(|t| t.is_keyword("GROUP")));
        assert!(tokens.iter().any(|t| t.is_keyword("HAVING")));
        assert!(tokens.iter().any(|t| t.is_keyword("ORDER")));
        assert!(tokens.iter().any(|t| t.is_keyword("LIMIT")));
    }

    #[test]
    fn test_error_token() {
        let mut lexer = Lexer::new("SELECT © FROM");

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Keyword);

        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::Error);
        // Error message is stored in literal field
        assert!(!token.literal.is_empty());
    }
}
