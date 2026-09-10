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
    /// Read a single-line comment (-- or #)
    pub(super) fn read_line_comment(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        result.push(self.ch);

        // Skip the start of comment (-- or #)
        if self.ch == '-' && self.peek_char() == '-' {
            self.read_char(); // first -
            result.push(self.ch); // second -
            self.read_char(); // move past second -
        } else if self.ch == '#' {
            self.read_char(); // move past #
        }

        // Read until end of line or EOF
        while self.ch != '\n' && !self.eof {
            if self.ch == '\0' {
                self.last_error = Some("NULL byte (0x00) is not allowed in SQL input".to_string());
                break;
            }
            result.push(self.ch);
            self.read_char();
        }

        result
    }

    /// Read a block comment (/* ... */)
    pub(super) fn read_block_comment(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        let mut depth = 1usize;

        // Start with the opening /* sequence
        result.push(self.ch); // /
        self.read_char();
        result.push(self.ch); // *
        self.read_char();

        while depth > 0 && !self.eof {
            if self.ch == '\0' {
                self.last_error = Some("NULL byte (0x00) is not allowed in SQL input".to_string());
                break;
            }
            if self.ch == '/' && self.peek_char() == '*' {
                depth += 1;
                result.push(self.ch);
                self.read_char();
                result.push(self.ch);
                self.read_char();
                continue;
            }
            if self.ch == '*' && self.peek_char() == '/' {
                depth -= 1;
                result.push(self.ch);
                self.read_char();
                result.push(self.ch);
                self.read_char();
                continue;
            }
            result.push(self.ch);
            self.read_char();
        }

        if depth != 0 && self.last_error.is_none() {
            self.last_error = Some("unterminated block comment".to_string());
        }

        result
    }
}
