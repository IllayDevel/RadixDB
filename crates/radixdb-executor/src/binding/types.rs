//! SQL type-name binding shared by schema and output binders.

use radixdb_core::{DataType, Error, Result};

/// Bind a SQL type spelling to the executor's scalar type identity.
pub fn parse_data_type(type_name: &str) -> Result<DataType> {
    let upper = type_name.trim().to_uppercase();
    let base_type = upper.split('(').next().unwrap_or(&upper);
    if upper.contains('(') && !matches!(base_type, "DECIMAL" | "NUMERIC" | "VECTOR") {
        return Err(Error::NotSupported(format!(
            "type modifiers are not supported for {base_type}; declare the base type without parameters"
        )));
    }

    match base_type {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" => Ok(DataType::Integer),
        "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
        "DECIMAL" | "NUMERIC" => Ok(DataType::Decimal),
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "CLOB" => Ok(DataType::Text),
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TIMESTAMP" | "DATETIME" | "TIME" => Ok(DataType::Timestamp),
        "DATE" => Ok(DataType::Date),
        "JSON" | "JSONB" => Ok(DataType::Json),
        "UUID" => Ok(DataType::Uuid),
        "BYTES" | "BLOB" | "BINARY" | "VARBINARY" => Ok(DataType::Bytes),
        "VECTOR" => Ok(DataType::Vector),
        _ => Err(Error::Type(format!("Unknown data type: {type_name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_supported_aliases_and_rejects_unsupported_modifiers() {
        assert_eq!(parse_data_type("INT").unwrap(), DataType::Integer);
        assert_eq!(parse_data_type("double").unwrap(), DataType::Float);
        assert_eq!(parse_data_type("JSONB").unwrap(), DataType::Json);
        assert_eq!(parse_data_type("BLOB").unwrap(), DataType::Bytes);
        assert_eq!(parse_data_type("DECIMAL(10,2)").unwrap(), DataType::Decimal);
        assert_eq!(parse_data_type("NUMERIC(12)").unwrap(), DataType::Decimal);
        assert_eq!(parse_data_type("VECTOR(32)").unwrap(), DataType::Vector);
        assert!(parse_data_type("VARCHAR(255)").is_err());
        assert!(parse_data_type("unknown").is_err());
    }
}
