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

const MAX_LITERAL_BYTES: usize = 8 * 1024 * 1024;

impl Lexer {
    /// Read a number (integer or float)
    pub(super) fn read_number(&mut self) -> SmartString {
        let mut result = SmartString::new("");
        result.push(self.ch);
        self.read_char();

        // Read all digits before decimal point
        while self.ch.is_ascii_digit() {
            result.push(self.ch);
            self.read_char();
        }

        // Check for decimal point
        if self.ch == '.' && self.peek_char().is_ascii_digit() {
            result.push(self.ch);
            self.read_char();

            // Read all digits after decimal point
            while self.ch.is_ascii_digit() {
                result.push(self.ch);
                self.read_char();
            }
        }

        // Check for exponent (E or e)
        if self.ch == 'e' || self.ch == 'E' {
            result.push(self.ch);
            self.read_char();

            // Check for sign after exponent
            if self.ch == '+' || self.ch == '-' {
                result.push(self.ch);
                self.read_char();
            }

            // Must have at least one digit after exponent
            if !self.ch.is_ascii_digit() {
                self.last_error = Some("invalid number format: exponent has no digits".to_string());
                return result;
            }

            // Read all digits in exponent
            while self.ch.is_ascii_digit() {
                result.push(self.ch);
                self.read_char();
            }
        }

        result
    }

    /// Read a string literal (single-quoted)
    pub(super) fn read_string_literal(&mut self) -> SmartString {
        let quote = self.ch;
        let quote_byte = quote as u8;
        let start_pos = self.position; // byte position of opening quote

        // Fast path: scan raw bytes for closing quote.
        // If no escape characters found, slice the input directly (zero allocation).
        // This is safe for UTF-8: quote (0x27/0x22), backslash (0x5C), and NUL (0x00)
        // are all single-byte ASCII and cannot appear as continuation bytes in multi-byte sequences.
        let mut scan_pos = self.read_position; // byte after opening quote
        let input_len = self.input.len();
        let mut found_escape = false;

        while scan_pos < input_len {
            let b = self.input[scan_pos];
            if b == quote_byte {
                // Check for doubled quote escape
                if scan_pos + 1 < input_len && self.input[scan_pos + 1] == quote_byte {
                    found_escape = true;
                    break;
                }
                // Found unescaped closing quote — fast path succeeds
                let end_pos = scan_pos + 1; // one past closing quote

                // Account for the opening quote and update positions by Unicode
                // scalars, not UTF-8 bytes.
                self.pos.column += 1;
                let contents = unsafe {
                    std::str::from_utf8_unchecked(&self.input[self.read_position..scan_pos])
                };
                for ch in contents.chars() {
                    if ch == '\n' {
                        self.pos.line += 1;
                        self.pos.column = 1;
                    } else {
                        self.pos.column += 1;
                    }
                }
                // Account for closing quote
                self.pos.column += 1;

                // Advance lexer past closing quote
                self.position = scan_pos;
                self.read_position = end_pos;
                self.pos.offset = self.position;

                // Read next char after closing quote
                if self.read_position >= input_len {
                    self.ch = '\0';
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

                if end_pos.saturating_sub(start_pos) > MAX_LITERAL_BYTES {
                    self.last_error = Some(format!(
                        "string literal exceeds limit of {MAX_LITERAL_BYTES} bytes"
                    ));
                    return SmartString::new("");
                }

                // SAFETY: input was constructed from valid UTF-8 (String/&str).
                // We only scanned for ASCII bytes, so all byte boundaries are valid.
                let slice =
                    unsafe { std::str::from_utf8_unchecked(&self.input[start_pos..end_pos]) };
                return SmartString::new(slice);
            } else if b == b'\\' || b == 0 {
                found_escape = true;
                break;
            }
            scan_pos += 1;
        }

        if !found_escape && scan_pos >= input_len {
            // EOF without closing quote — fall through to slow path for error handling
        }

        // Slow path: escapes or edge cases. Build a String with pre-allocated capacity.
        let estimated_len = if scan_pos > self.read_position {
            scan_pos - start_pos + 2
        } else {
            32
        };
        let mut result = String::with_capacity(estimated_len);
        let mut oversized = false;
        result.push(quote);
        self.read_char(); // consume opening quote

        loop {
            if self.ch == '\0' {
                if self.eof {
                    self.last_error = Some("unterminated string literal".to_string());
                } else {
                    self.last_error =
                        Some("NULL byte (0x00) is not allowed in string literals".to_string());
                }
                if !oversized {
                    result.push(quote);
                }
                break;
            } else if self.ch == quote {
                if self.peek_char() == quote {
                    if !oversized {
                        result.push(self.ch);
                    }
                    self.read_char();
                    self.read_char();
                } else {
                    if !oversized {
                        result.push(quote);
                    }
                    self.read_char();
                    break;
                }
            } else if self.ch == '\\' {
                if !oversized {
                    result.push(self.ch);
                }
                self.read_char();
                if self.ch != '\0' {
                    if !oversized {
                        result.push(self.ch);
                    }
                    self.read_char();
                }
            } else {
                if !oversized {
                    result.push(self.ch);
                }
                self.read_char();
            }
            if !oversized && result.len() > MAX_LITERAL_BYTES {
                oversized = true;
                result.clear();
            }
        }

        if oversized {
            self.last_error = Some(format!(
                "string literal exceeds limit of {MAX_LITERAL_BYTES} bytes"
            ));
        }

        SmartString::from_string(result)
    }
}
