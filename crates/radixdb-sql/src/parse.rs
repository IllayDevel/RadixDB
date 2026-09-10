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

use crate::{ParseError, ParseErrors, Parser, Position, Statement};

/// Parse SQL and return statements
///
/// This is the main entry point for parsing SQL strings.
///
/// # Arguments
///
/// * `sql` - The SQL string to parse
///
/// # Returns
///
/// * `Ok(Vec<Statement>)` - Successfully parsed statements
/// * `Err(ParseErrors)` - Parse errors encountered
///
/// # Example
///
/// ```
/// use radixdb_sql::parse_sql;
///
/// let statements = parse_sql("SELECT 1").unwrap();
/// assert_eq!(statements.len(), 1);
/// ```
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParseErrors> {
    if sql.starts_with('\u{feff}') {
        return Err(ParseErrors::from_errors_with_sql(
            vec![ParseError::new(
                "UTF-8 BOM is not allowed in SQL source".to_string(),
                Position::new(0, 1, 1),
            )],
            sql,
        ));
    }
    if sql.trim().is_empty() {
        return Err(ParseErrors::from_errors_with_sql(
            vec![ParseError::new(
                "No statements found in query".to_string(),
                Position::new(0, 1, 1),
            )],
            sql,
        ));
    }

    let mut parser = Parser::new(sql);
    let program = parser.parse_program()?;

    if program.statements.is_empty() {
        return Err(ParseErrors::from_errors_with_sql(
            vec![ParseError::new(
                "No statements found in query".to_string(),
                Position::new(0, 1, 1),
            )],
            sql,
        ));
    }

    Ok(program.statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let result = parse_sql("SELECT 1");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_parse_select_from() {
        let result = parse_sql("SELECT * FROM users");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Select(s) => {
                assert!(!s.distinct);
                assert_eq!(s.columns.len(), 1);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_empty_string() {
        let result = parse_sql("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_only() {
        let result = parse_sql("   \n\t  ");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_select_with_where() {
        let result = parse_sql("SELECT id, name FROM users WHERE id = 1");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let result = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Insert(s) => {
                assert_eq!(s.table_name.value, "users");
                assert_eq!(s.columns.len(), 2);
                assert_eq!(s.values.len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let result = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Update(s) => {
                assert_eq!(s.table_name.value, "users");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let result = parse_sql("DELETE FROM users WHERE id = 1");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Delete(s) => {
                assert_eq!(s.table_name.value, "users");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let result = parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::CreateTable(s) => {
                assert_eq!(s.table_name.value, "users");
                assert_eq!(s.columns.len(), 2);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_complex_query() {
        let result = parse_sql(
            r#"
            SELECT u.id, u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = TRUE
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY order_count DESC
            LIMIT 10
        "#,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_cte() {
        let result = parse_sql("WITH temp AS (SELECT * FROM users) SELECT * FROM temp");
        assert!(result.is_ok());
        let statements = result.unwrap();
        match &statements[0] {
            Statement::Select(s) => {
                assert!(s.with.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_transaction() {
        let begin = parse_sql("BEGIN TRANSACTION").unwrap();
        assert!(matches!(begin[0], Statement::Begin(_)));

        let commit = parse_sql("COMMIT").unwrap();
        assert!(matches!(commit[0], Statement::Commit(_)));

        let rollback = parse_sql("ROLLBACK").unwrap();
        assert!(matches!(rollback[0], Statement::Rollback(_)));
    }
}
