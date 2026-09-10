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

use super::*;

impl Parser {
    /// Parse a statement
    pub fn parse_statement(&mut self) -> Option<Statement> {
        // Skip comments
        while self.cur_token_is(TokenType::Comment) {
            self.next_token();
        }

        if self.cur_token_is(TokenType::Eof) {
            return None;
        }

        if self.cur_token_is(TokenType::Keyword) {
            let keyword = self.cur_token.literal.to_uppercase();
            match keyword.as_str() {
                "SELECT" => self.parse_select_statement().map(Statement::Select),
                "WITH" => self.parse_with_statement(),
                "INSERT" => self.parse_insert_statement().map(Statement::Insert),
                "UPDATE" => self.parse_update_statement().map(Statement::Update),
                "DELETE" => self.parse_delete_statement().map(Statement::Delete),
                "TRUNCATE" => self.parse_truncate_statement().map(Statement::Truncate),
                "CREATE" => self.parse_create_statement(),
                "DROP" => self.parse_drop_statement(),
                "ALTER" => self.parse_alter_statement(),
                "BEGIN" => self.parse_begin_statement().map(Statement::Begin),
                "COMMIT" => self.parse_commit_statement().map(Statement::Commit),
                "ROLLBACK" => self.parse_rollback_statement().map(Statement::Rollback),
                "SAVEPOINT" => self.parse_savepoint_statement().map(Statement::Savepoint),
                "RELEASE" => self
                    .parse_release_savepoint_statement()
                    .map(Statement::ReleaseSavepoint),
                "SET" => self
                    .parse_set_statement()
                    .map(|s| Statement::Set(Box::new(s))),
                "PRAGMA" => self.parse_pragma_statement().map(Statement::Pragma),
                "VACUUM" => self.parse_vacuum_statement().map(Statement::Vacuum),
                "SHOW" => self.parse_show_statement(),
                "DESCRIBE" | "DESC" => self.parse_describe_statement().map(Statement::Describe),
                "EXPLAIN" => self.parse_explain_statement().map(Statement::Explain),
                "ANALYZE" => self.parse_analyze_statement().map(Statement::Analyze),
                "COPY" => self.parse_copy_statement().map(Statement::Copy),
                "CALL" => self
                    .parse_outer_call_statement()
                    .map(|statement| Statement::Call(Box::new(statement))),
                "GRANT" => self
                    .parse_grant_statement()
                    .map(|statement| Statement::Grant(Box::new(statement))),
                "REVOKE" => self
                    .parse_revoke_statement()
                    .map(|statement| Statement::Revoke(Box::new(statement))),
                _ => {
                    // Try to parse as expression statement
                    self.parse_expression_statement().map(Statement::Expression)
                }
            }
        } else {
            // Try to parse as expression statement
            self.parse_expression_statement().map(Statement::Expression)
        }
    }
}
