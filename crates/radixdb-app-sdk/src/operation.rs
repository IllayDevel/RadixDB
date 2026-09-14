use radixdb_client::{Column, Row, WireValue};
use radixdb_orm::IrDocument;
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultLimits {
    max_rows: usize,
    max_bytes: usize,
}

impl ResultLimits {
    pub const HARD_MAX_ROWS: usize = 100_000;
    pub const HARD_MAX_BYTES: usize = 64 * 1024 * 1024;

    pub fn new(max_rows: usize, max_bytes: usize) -> Result<Self, OperationContractError> {
        if max_rows == 0 || max_rows > Self::HARD_MAX_ROWS {
            return Err(OperationContractError::InvalidLimit {
                name: "max rows",
                value: max_rows,
                maximum: Self::HARD_MAX_ROWS,
            });
        }
        if max_bytes == 0 || max_bytes > Self::HARD_MAX_BYTES {
            return Err(OperationContractError::InvalidLimit {
                name: "max bytes",
                value: max_bytes,
                maximum: Self::HARD_MAX_BYTES,
            });
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    pub fn max_rows(self) -> usize {
        self.max_rows
    }

    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub encoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureResult {
    CommandComplete {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows(QueryResult),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperationContractError {
    #[error("operation name must not be empty")]
    EmptyName,
    #[error("operation name exceeds 256 bytes or contains a control character")]
    InvalidName,
    #[error("procedure {part} must not be empty")]
    EmptyProcedureIdentifier { part: &'static str },
    #[error("procedure {part} exceeds 256 bytes or contains a control character")]
    InvalidProcedureIdentifier { part: &'static str },
    #[error("invalid {name} limit {value}; expected 1..={maximum}")]
    InvalidLimit {
        name: &'static str,
        value: usize,
        maximum: usize,
    },
    #[error("result exceeded {kind} limit {limit}")]
    ResultLimitExceeded { kind: &'static str, limit: usize },
    #[error("database returned an unexpected result shape: {0}")]
    UnexpectedResult(&'static str),
    #[error("typed result decoding failed: {0}")]
    Decode(String),
    #[error("typed operation encoding failed: {0}")]
    Encode(String),
}

pub trait TypedQuery {
    type Output;

    const NAME: &'static str;

    fn document(&self) -> Result<IrDocument, OperationContractError>;

    fn limits(&self) -> ResultLimits {
        ResultLimits::default()
    }

    fn decode(self, result: QueryResult) -> Result<Self::Output, OperationContractError>;
}

pub trait TypedProcedure {
    type Output;

    const NAME: &'static str;
    const SCHEMA: &'static str;
    const PROCEDURE: &'static str;

    fn positional_parameters(&self) -> Result<Vec<WireValue>, OperationContractError> {
        Ok(Vec::new())
    }

    fn limits(&self) -> ResultLimits {
        ResultLimits::default()
    }

    fn decode(self, result: ProcedureResult) -> Result<Self::Output, OperationContractError>;
}

pub trait ApplicationEvent: Serialize + DeserializeOwned {
    const TOPIC: &'static str;
    const VERSION: u32;
}

pub(crate) fn validate_operation_name(name: &str) -> Result<(), OperationContractError> {
    if name.is_empty() {
        return Err(OperationContractError::EmptyName);
    }
    if name.len() > 256 || name.chars().any(char::is_control) {
        return Err(OperationContractError::InvalidName);
    }
    Ok(())
}

pub(crate) fn render_procedure_statement(
    schema: &str,
    procedure: &str,
    parameter_count: usize,
) -> Result<String, OperationContractError> {
    let schema = quote_identifier("schema", schema)?;
    let procedure = quote_identifier("name", procedure)?;
    let parameters = std::iter::repeat_n("?", parameter_count)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("CALL {schema}.{procedure}({parameters})"))
}

fn quote_identifier(
    part: &'static str,
    identifier: &str,
) -> Result<String, OperationContractError> {
    if identifier.is_empty() {
        return Err(OperationContractError::EmptyProcedureIdentifier { part });
    }
    if identifier.len() > 256 || identifier.chars().any(char::is_control) {
        return Err(OperationContractError::InvalidProcedureIdentifier { part });
    }
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_limits_are_bounded_on_both_axes() {
        assert!(ResultLimits::new(1, 1).is_ok());
        assert!(ResultLimits::new(0, 1).is_err());
        assert!(ResultLimits::new(1, 0).is_err());
        assert!(ResultLimits::new(ResultLimits::HARD_MAX_ROWS + 1, 1).is_err());
        assert!(ResultLimits::new(1, ResultLimits::HARD_MAX_BYTES + 1).is_err());
    }

    #[test]
    fn procedure_call_is_rendered_from_identifiers_and_arity() {
        assert_eq!(
            render_procedure_statement("app", "work", 2).unwrap(),
            "CALL \"app\".\"work\"(?, ?)"
        );
        assert_eq!(
            render_procedure_statement("quoted", "a\"b", 0).unwrap(),
            "CALL \"quoted\".\"a\"\"b\"()"
        );
        assert!(render_procedure_statement("", "work", 0).is_err());
        assert!(render_procedure_statement("app", "line\nbreak", 0).is_err());
    }
}
