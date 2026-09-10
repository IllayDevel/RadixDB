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

impl Lexer {
    /// Decode UTF-8 character at given byte position
    /// Returns (char, byte_length)
    #[inline]
    pub(super) fn decode_char_at(&self, pos: usize) -> (char, usize) {
        if pos >= self.input.len() {
            return ('\0', 0);
        }

        let b = self.input[pos];

        // Fast path: ASCII (most common in SQL)
        if b < 0x80 {
            return (b as char, 1);
        }

        // Decode only the current scalar. Validating the entire remaining suffix
        // for every non-ASCII character turns Unicode input into O(n^2).
        let width = match b {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => return ('\u{FFFD}', 1),
        };
        let Some(end) = pos
            .checked_add(width)
            .filter(|&end| end <= self.input.len())
        else {
            return ('\u{FFFD}', 1);
        };
        match std::str::from_utf8(&self.input[pos..end]) {
            Ok(s) => (s.chars().next().unwrap_or('\u{FFFD}'), width),
            Err(_) => ('\u{FFFD}', 1),
        }
    }

    /// Read the next character
    pub(super) fn read_char(&mut self) {
        // Update position before changing character
        if !self.eof && self.ch == '\n' {
            self.pos.line += 1;
            self.pos.column = 1;
        } else if !self.eof {
            self.pos.column += 1;
        }

        if self.read_position >= self.input.len() {
            self.ch = '\0'; // EOF
            self.position = self.read_position;
            self.eof = true;
        } else {
            let (ch, len) = self.decode_char_at(self.read_position);
            self.ch = ch;
            self.position = self.read_position;
            self.read_position += len;
            self.eof = false;
        }

        self.pos.offset = self.position;
    }

    /// Peek at the next character without advancing
    #[inline]
    pub(super) fn peek_char(&self) -> char {
        self.decode_char_at(self.read_position).0
    }

    /// Skip whitespace characters
    pub(super) fn skip_whitespace(&mut self) {
        while self.ch.is_whitespace() {
            // Note: read_char() handles line/column tracking when it encounters '\n'
            // So we don't need to update pos.line here
            if self.ch == '\r' && self.peek_char() == '\n' {
                // Skip \r in \r\n sequences (Windows line endings)
                self.read_char(); // consume '\r'
            }
            self.read_char();
        }
    }
}
