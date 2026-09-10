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

//! Stable serialization of bound values into catalog-owned SQL expressions.

use radixdb_core::{Error, Result, Value};
use radixdb_sql::ast::*;
use radixdb_sql::token::Token;

/// Convert a runtime value into SQL AST which can be persisted in catalog
/// expressions and parsed again after restart. DDL parameters are bound once,
/// at statement execution time; `$1`/`:name` must never leak into a stored
/// DEFAULT, CHECK, or partial-index predicate.
pub(super) fn persistent_value_expression(value: &Value, token: &Token) -> Result<Expression> {
    value.validate_shape()?;
    let string_literal = |value: String| {
        Expression::StringLiteral(StringLiteral {
            token: token.clone(),
            value: value.into(),
            type_hint: None,
        })
    };
    let cast_text = |value: String, type_name: String| {
        Expression::Cast(CastExpression {
            token: token.clone(),
            expr: Box::new(string_literal(value)),
            type_name: type_name.into(),
        })
    };

    Ok(match value {
        Value::Null(_) => Expression::NullLiteral(NullLiteral {
            token: token.clone(),
        }),
        Value::Integer(value) => Expression::IntegerLiteral(IntegerLiteral {
            token: token.clone(),
            value: *value,
        }),
        Value::Float(value) if value.is_finite() => Expression::FloatLiteral(FloatLiteral {
            token: token.clone(),
            value: *value,
        }),
        Value::Float(value) => cast_text(value.to_string(), "FLOAT".to_string()),
        Value::Text(value) => string_literal(value.to_string()),
        Value::Boolean(value) => Expression::BooleanLiteral(BooleanLiteral {
            token: token.clone(),
            value: *value,
        }),
        Value::Timestamp(value) => Expression::StringLiteral(StringLiteral {
            token: token.clone(),
            value: value.to_rfc3339().into(),
            type_hint: Some("TIMESTAMP".into()),
        }),
        Value::Extension(_) if value.as_json().is_some() => cast_text(
            value
                .as_json()
                .expect("JSON shape was validated")
                .to_string(),
            "JSON".to_string(),
        ),
        Value::Extension(_) if value.as_uuid_bytes().is_some() => cast_text(
            value
                .as_string()
                .ok_or_else(|| Error::type_conversion("UUID", "persistent SQL literal"))?,
            "UUID".to_string(),
        ),
        Value::Extension(_) if value.as_vector_f32().is_some() => cast_text(
            value
                .as_string()
                .ok_or_else(|| Error::type_conversion("VECTOR", "persistent SQL literal"))?,
            "VECTOR".to_string(),
        ),
        Value::Extension(_) if value.as_decimal_parts().is_some() => {
            let (_, precision, scale) = value
                .as_decimal_parts()
                .expect("DECIMAL shape was validated");
            cast_text(
                value
                    .as_string()
                    .ok_or_else(|| Error::type_conversion("DECIMAL", "persistent SQL literal"))?,
                format!("DECIMAL({precision},{scale})"),
            )
        }
        Value::Extension(_) if value.as_date_days().is_some() => {
            Expression::StringLiteral(StringLiteral {
                token: token.clone(),
                value: value
                    .as_string()
                    .ok_or_else(|| Error::type_conversion("DATE", "persistent SQL literal"))?
                    .into(),
                type_hint: Some("DATE".into()),
            })
        }
        Value::Extension(_) if value.as_bytes_value().is_some() => {
            return Err(Error::NotSupported(
                "BYTES parameters are not supported in persistent schema expressions; use an explicit SQL expression"
                    .to_string(),
            ));
        }
        Value::Extension(_) => {
            return Err(Error::NotSupported(format!(
                "{} parameters are not supported in persistent schema expressions",
                value.data_type()
            )));
        }
    })
}
