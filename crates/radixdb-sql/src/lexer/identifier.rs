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

use super::Lexer;
use radixdb_core::SmartString;

impl Lexer {
    /// Read an identifier
    pub(super) fn read_identifier(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        result.push(self.ch);
        self.read_char();

        while self.ch.is_alphanumeric() || self.ch == '_' || self.ch == '$' {
            result.push(self.ch);
            self.read_char();
        }

        result
    }

    /// Read a quoted identifier (double quotes or backticks)
    pub(super) fn read_quoted_identifier(&mut self, quote: char) -> SmartString {
        let mut result = SmartString::new("");
        self.read_char(); // consume opening quote

        while !self.eof && self.ch != '\0' {
            // Handle doubled quotes as escape (e.g., "abc""def" -> abc"def)
            if self.ch == quote && self.peek_char() == quote {
                result.push(self.ch);
                self.read_char(); // consume first quote
                self.read_char(); // consume second quote
            } else if self.ch == quote {
                // Found closing quote
                break;
            } else {
                result.push(self.ch);
                self.read_char();
            }
        }

        // Consume closing quote if not EOF
        if self.ch == quote {
            self.read_char();
        } else if self.eof {
            self.last_error = Some(format!(
                "unterminated quoted identifier starting with {}",
                quote
            ));
        } else {
            // NULL byte encountered
            self.last_error =
                Some("NULL byte (0x00) is not allowed in quoted identifiers".to_string());
        }

        result
    }

    /// Read a parameter ($1, $2, etc.)
    pub(super) fn read_parameter(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        result.push(self.ch); // $
        self.read_char();

        // Read all digits
        while self.ch.is_ascii_digit() {
            result.push(self.ch);
            self.read_char();
        }

        // Validate parameter has digits
        if result.len() == 1 {
            self.last_error = Some("parameter number expected after $".to_string());
        }

        result
    }

    /// Read a named parameter (:name)
    pub(super) fn read_named_parameter(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        result.push(self.ch); // :
        self.read_char();

        // Read identifier part (alphanumeric + underscore)
        while self.ch.is_alphanumeric() || self.ch == '_' {
            result.push(self.ch);
            self.read_char();
        }

        result
    }
}
