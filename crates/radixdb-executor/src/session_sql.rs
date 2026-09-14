use radixdb_core::{Error as DatabaseError, SessionTimeZone};
use radixdb_sql::{Expression as SqlExpression, Parser as SqlParser, Statement as SqlStatement};

/// Validate an isolated session time-zone command without changing session state.
pub fn session_time_zone_change(sql: &str) -> Result<Option<SessionTimeZone>, DatabaseError> {
    if !contains_ascii_case_insensitive(sql.as_bytes(), b"SET")
        || !contains_ascii_case_insensitive(sql.as_bytes(), b"TIME")
    {
        return Ok(None);
    }
    let program = SqlParser::new(sql)
        .parse_program()
        .map_err(|error| DatabaseError::parse(error.to_string()))?;
    let mut time_zone = None;

    for statement in &program.statements {
        let SqlStatement::Set(statement) = statement else {
            continue;
        };
        if !matches!(
            statement.name.value.to_uppercase().as_str(),
            "TIME ZONE" | "TIMEZONE" | "TIME_ZONE"
        ) {
            continue;
        }
        if program.statements.len() != 1 {
            return Err(DatabaseError::invalid_argument(
                "SET TIME ZONE must be the only statement in a request",
            ));
        }
        let value = match &statement.value {
            SqlExpression::StringLiteral(value) => value.value.as_str(),
            SqlExpression::Identifier(value) => value.value.as_str(),
            _ => {
                return Err(DatabaseError::invalid_argument(
                    "SET TIME ZONE requires a string value",
                ));
            }
        };
        time_zone = Some(SessionTimeZone::parse(value)?);
    }

    Ok(time_zone)
}

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| {
        window
            .iter()
            .zip(needle)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
    })
}
