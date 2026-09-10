use radixdb_catalog::{CatalogDataType, CatalogGeneration};
use radixdb_core::DataType;
use radixdb_sql::{CastExpression, Expression, Parameter, Token};

pub(super) fn typed_parameter(
    index: usize,
    data_type: CatalogDataType,
    token: Token,
) -> Expression {
    let parameter_name = format!("${index}");
    Expression::Cast(CastExpression {
        token: token.clone(),
        expr: Box::new(Expression::Parameter(Parameter {
            token,
            name: parameter_name.into(),
            index,
            field: None,
        })),
        type_name: catalog_type_spelling(data_type).into(),
    })
}

pub(super) fn typed_parameter_in_catalog(
    index: usize,
    data_type: CatalogDataType,
    token: Token,
    catalog: &CatalogGeneration,
) -> Expression {
    let parameter_name = format!("${index}");
    Expression::Cast(CastExpression {
        token: token.clone(),
        expr: Box::new(Expression::Parameter(Parameter {
            token,
            name: parameter_name.into(),
            index,
            field: None,
        })),
        type_name: super::native_function::catalog_type_name(catalog, data_type).into(),
    })
}

pub(super) fn typed_context_parameter(
    name: &str,
    data_type: CatalogDataType,
    token: Token,
) -> Expression {
    Expression::Cast(CastExpression {
        token: token.clone(),
        expr: Box::new(Expression::Parameter(Parameter {
            token,
            name: format!(":{}", name.to_ascii_uppercase()).into(),
            index: 0,
            field: None,
        })),
        type_name: catalog_type_spelling(data_type).into(),
    })
}

pub(super) fn context_value_type(name: &str) -> Option<(CatalogDataType, bool)> {
    let (data_type, nullable) = match name.to_ascii_uppercase().as_str() {
        "CURRENT_PRINCIPAL" | "CURRENT_EFFECTIVE_PRINCIPAL" | "CURRENT_JOB_ID" => {
            (DataType::Uuid, name.eq_ignore_ascii_case("CURRENT_JOB_ID"))
        }
        "CURRENT_TRANSACTION_ID" | "CURRENT_REQUEST_ID" | "CURRENT_JOB_ATTEMPT" => (
            DataType::Integer,
            !name.eq_ignore_ascii_case("CURRENT_TRANSACTION_ID"),
        ),
        "CURRENT_STATEMENT_TIMESTAMP" | "CURRENT_JOB_SCHEDULED_AT" => (
            DataType::Timestamp,
            name.eq_ignore_ascii_case("CURRENT_JOB_SCHEDULED_AT"),
        ),
        "CURRENT_IDEMPOTENCY_KEY" => (DataType::Text, true),
        _ => return None,
    };
    CatalogDataType::scalar(data_type)
        .ok()
        .map(|data_type| (data_type, nullable))
}

pub(super) fn catalog_type_spelling(data_type: CatalogDataType) -> String {
    match data_type.logical_type() {
        DataType::Decimal if data_type.parameter_1() > 0 => format!(
            "DECIMAL({},{})",
            data_type.parameter_1(),
            data_type.parameter_2()
        ),
        DataType::Vector => format!("VECTOR({})", data_type.parameter_1()),
        logical => logical.to_string(),
    }
}
