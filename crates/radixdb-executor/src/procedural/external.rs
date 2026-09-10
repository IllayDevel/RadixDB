use radixdb_core::{DataType, Error, Result, Value};

use crate::Executor;

use super::transaction_visible_catalog;

pub(super) fn equal(executor: &Executor, left: &Value, right: &Value) -> Result<bool> {
    executor
        .plugin_registry
        .external_equal(left, right)
        .map_err(|error| Error::invalid_argument(error.to_string()))
}

pub(super) fn compare(
    executor: &Executor,
    left: &Value,
    right: &Value,
) -> Result<std::cmp::Ordering> {
    executor
        .plugin_registry
        .external_compare(left, right)
        .map_err(|error| Error::invalid_argument(error.to_string()))
}

pub(super) fn input(executor: &Executor, type_name: &str, input: &Value) -> Result<Value> {
    if input.is_null() {
        return Ok(Value::null_unknown());
    }
    let (catalog, _) = transaction_visible_catalog(executor)?;
    let catalog_type =
        crate::catalog::bind_catalog_type_in_generation(type_name, catalog.as_ref())?;
    let type_ref = catalog_type.external_type_ref().ok_or_else(|| {
        Error::invalid_argument(format!("type '{type_name}' is not an external type"))
    })?;
    if let Some(external) = input.as_external() {
        if external.type_ref() != type_ref {
            return Err(Error::Type(format!(
                "cannot cast external value to unrelated type '{type_name}'"
            )));
        }
        executor
            .plugin_registry
            .validate_external_value(input)
            .map_err(|error| Error::invalid_argument(error.to_string()))?;
        return Ok(input.clone());
    }
    if let Some(text) = input.as_str() {
        return executor
            .plugin_registry
            .parse_external_text(type_ref, text)
            .map_err(|error| Error::invalid_argument(error.to_string()));
    }
    if let Some(bytes) = input.as_bytes_value() {
        return executor
            .plugin_registry
            .parse_external_binary(type_ref, bytes)
            .map_err(|error| Error::invalid_argument(error.to_string()));
    }
    Err(Error::Type(format!(
        "external type '{type_name}' accepts TEXT or BYTES input"
    )))
}

pub(super) fn output(executor: &Executor, value: &Value, target_type: DataType) -> Result<Value> {
    match target_type {
        DataType::Text => executor
            .plugin_registry
            .format_external_text(value)
            .map(|value| Value::Text(value.into()))
            .map_err(|error| Error::invalid_argument(error.to_string())),
        DataType::Bytes => executor
            .plugin_registry
            .format_external_binary(value)
            .map(Value::bytes)
            .map_err(|error| Error::invalid_argument(error.to_string())),
        _ => Err(Error::Type(format!(
            "external values can only be cast to TEXT or BYTES, not {target_type}"
        ))),
    }
}
