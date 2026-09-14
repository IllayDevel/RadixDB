use std::collections::BTreeMap;
use std::fmt::Write;

use radixdb_orm::{
    generate_rust_database, CodegenError as OrmCodegenError, DataTypeDescriptor,
    DatabaseDescriptor, DescriptorEnvelope, DescriptorError, DescriptorKind, ProcedureDescriptor,
    RoutineArgumentDescriptor, RoutineArgumentModeDescriptor, RoutineResultColumnDescriptor,
    RoutineResultDescriptor, SCHEMA_DESCRIPTOR_VERSION,
};

#[derive(Debug, thiserror::Error)]
pub enum ApplicationCodegenError {
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),
    #[error(transparent)]
    Orm(#[from] OrmCodegenError),
    #[error("procedure descriptor fingerprint mismatch for '{name}': expected {expected}, computed {actual}")]
    ProcedureFingerprint {
        name: String,
        expected: String,
        actual: String,
    },
    #[error("procedure '{0}' must have exactly one schema qualifier")]
    ProcedureName(String),
    #[error("identifier '{0}' cannot be represented safely in generated Rust")]
    Identifier(String),
    #[error("procedure '{procedure}' uses unsupported extension type '{sql_type}'")]
    ExternalType { procedure: String, sql_type: String },
    #[error("{object} fingerprint mismatch: expected {expected}, computed {actual}")]
    FingerprintMismatch {
        object: String,
        expected: String,
        actual: String,
    },
    #[error("procedure '{0}' mixes OUT/INOUT arguments with an explicit RETURNS contract")]
    MixedProcedureResult(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedApplicationRust {
    pub source: String,
    pub descriptor_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationDescriptorFingerprints {
    pub catalog: String,
    pub schema: String,
}

/// Validate a catalog-bound descriptor and derive the portable application
/// schema fingerprint without generating source code.
pub fn application_descriptor_fingerprints(
    descriptor: &DescriptorEnvelope<DatabaseDescriptor>,
) -> Result<ApplicationDescriptorFingerprints, ApplicationCodegenError> {
    if descriptor.descriptor != SCHEMA_DESCRIPTOR_VERSION
        || descriptor.kind != DescriptorKind::Database
    {
        return Err(DescriptorError::KindMismatch {
            expected: DescriptorKind::Database,
            actual: descriptor.kind,
        }
        .into());
    }
    for table in &descriptor.payload.tables {
        validate_fingerprint(
            format!("table '{}'", table.name),
            &table.fingerprint,
            table.computed_fingerprint()?,
        )?;
    }
    for procedure in &descriptor.payload.procedures {
        validate_fingerprint(
            format!("procedure '{}'", procedure.name),
            &procedure.fingerprint,
            procedure.computed_fingerprint()?,
        )?;
    }
    validate_fingerprint(
        "database".to_owned(),
        &descriptor.payload.fingerprint,
        descriptor.payload.computed_fingerprint()?,
    )?;
    let portable = portable_application_descriptor(descriptor)?;
    Ok(ApplicationDescriptorFingerprints {
        catalog: descriptor.payload.fingerprint.clone(),
        schema: portable.payload.fingerprint,
    })
}

fn validate_fingerprint(
    object: String,
    expected: &str,
    actual: String,
) -> Result<(), ApplicationCodegenError> {
    if expected != actual {
        return Err(ApplicationCodegenError::FingerprintMismatch {
            object,
            expected: expected.to_owned(),
            actual,
        });
    }
    Ok(())
}

/// Generate one deterministic Rust application contract from a canonical
/// database descriptor. Table records/navigation come from `radixdb-orm`;
/// typed procedure calls are appended from the same fingerprinted descriptor.
pub fn generate_rust_application(
    descriptor: &DescriptorEnvelope<DatabaseDescriptor>,
) -> Result<GeneratedApplicationRust, ApplicationCodegenError> {
    if descriptor.descriptor != SCHEMA_DESCRIPTOR_VERSION
        || descriptor.kind != DescriptorKind::Database
    {
        return Err(DescriptorError::KindMismatch {
            expected: DescriptorKind::Database,
            actual: descriptor.kind,
        }
        .into());
    }
    // Validate the complete catalog-bound descriptor before removing physical
    // identity. This prevents normalization from laundering a stale or forged
    // source artifact.
    let _validated = generate_rust_database(descriptor)?;
    for procedure in &descriptor.payload.procedures {
        validate_procedure_fingerprint(procedure)?;
    }
    let portable = portable_application_descriptor(descriptor)?;
    let generated = generate_rust_database(&portable)?;
    let mut source = generated.source;
    source.push_str("\n// Typed application procedures generated from the same descriptor.\n");
    source.push_str("#[allow(unused_imports)]\nuse radixdb_app_sdk::{OperationContractError, ProcedureResult, QueryResult, TypedProcedure};\n");
    source.push_str("#[allow(unused_imports)]\nuse radixdb_client::{typed_value_to_wire, wire_value_to_typed, WireValue};\n\n");

    let duplicate_names = duplicate_procedure_type_names(&portable.payload.procedures)?;
    for procedure in &portable.payload.procedures {
        render_procedure(&mut source, procedure, &duplicate_names)?;
    }

    Ok(GeneratedApplicationRust {
        source,
        descriptor_fingerprint: generated.descriptor_fingerprint,
    })
}

/// Convert a valid database-instance descriptor into the portable application
/// contract used by generated SDKs and runtime schema negotiation.
///
/// Catalog object IDs, catalog generations, timestamps, and routine revision
/// counters identify one physical database history. They must not force a
/// broker rebuild when the same migrations are applied to another database.
/// Structural table/view/routine definitions remain fingerprinted.
pub fn portable_application_descriptor(
    descriptor: &DescriptorEnvelope<DatabaseDescriptor>,
) -> Result<DescriptorEnvelope<DatabaseDescriptor>, ApplicationCodegenError> {
    if descriptor.descriptor != SCHEMA_DESCRIPTOR_VERSION
        || descriptor.kind != DescriptorKind::Database
    {
        return Err(DescriptorError::KindMismatch {
            expected: DescriptorKind::Database,
            actual: descriptor.kind,
        }
        .into());
    }
    let mut portable = descriptor.clone();
    portable.payload.schema_generation = 0;
    for table in &mut portable.payload.tables {
        table.catalog_id.clear();
        table.schema_generation = 0;
        table.created_at.clear();
        table.updated_at.clear();
        table.refresh_fingerprint()?;
    }
    for procedure in &mut portable.payload.procedures {
        procedure.catalog_id.clear();
        procedure.definition_revision = 0;
        procedure.refresh_fingerprint()?;
    }
    portable.payload.refresh_fingerprint()?;
    Ok(portable)
}

fn validate_procedure_fingerprint(
    procedure: &ProcedureDescriptor,
) -> Result<(), ApplicationCodegenError> {
    let actual = procedure.computed_fingerprint()?;
    if actual != procedure.fingerprint {
        return Err(ApplicationCodegenError::ProcedureFingerprint {
            name: procedure.name.clone(),
            expected: procedure.fingerprint.clone(),
            actual,
        });
    }
    Ok(())
}

fn duplicate_procedure_type_names(
    procedures: &[ProcedureDescriptor],
) -> Result<BTreeMap<String, usize>, ApplicationCodegenError> {
    let mut names = BTreeMap::new();
    for procedure in procedures {
        *names.entry(rust_type_name(&procedure.name)?).or_default() += 1;
    }
    Ok(names)
}

fn render_procedure(
    output: &mut String,
    procedure: &ProcedureDescriptor,
    duplicate_names: &BTreeMap<String, usize>,
) -> Result<(), ApplicationCodegenError> {
    let (schema, name) = split_procedure_name(&procedure.name)?;
    let mut type_name = rust_type_name(&procedure.name)?;
    if duplicate_names.get(&type_name).copied().unwrap_or_default() > 1 {
        type_name.push_str(&procedure.fingerprint[..8].to_ascii_uppercase());
    }
    let call = format!("{type_name}Call");
    let output_name = format!("{type_name}Output");
    let inputs = procedure
        .arguments
        .iter()
        .filter(|argument| argument.mode != RoutineArgumentModeDescriptor::Out)
        .collect::<Vec<_>>();
    let outputs = procedure
        .arguments
        .iter()
        .filter(|argument| argument.mode != RoutineArgumentModeDescriptor::In)
        .collect::<Vec<_>>();
    if !outputs.is_empty() && !matches!(procedure.result, RoutineResultDescriptor::Void) {
        return Err(ApplicationCodegenError::MixedProcedureResult(
            procedure.name.clone(),
        ));
    }

    writeln!(
        output,
        "pub const {}_FINGERPRINT: &str = {:?};",
        rust_const_name(&procedure.name)?,
        procedure.fingerprint
    )
    .unwrap();
    writeln!(
        output,
        "#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]\n#[serde(deny_unknown_fields)]\npub struct {call} {{"
    )
    .unwrap();
    for argument in &inputs {
        writeln!(
            output,
            "    pub {}: {},",
            rust_field_name(&argument.name)?,
            rust_field_type(argument, procedure)?
        )
        .unwrap();
    }
    output.push_str("}\n\n");

    let result_columns = result_columns(procedure, &outputs)?;
    let table_result =
        outputs.is_empty() && matches!(procedure.result, RoutineResultDescriptor::Table { .. });
    if !result_columns.is_empty() {
        writeln!(
            output,
            "#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]\n#[serde(deny_unknown_fields)]\npub struct {output_name} {{"
        )
        .unwrap();
        for column in &result_columns {
            writeln!(
                output,
                "    pub {}: {},",
                rust_field_name(column.name)?,
                rust_result_type(
                    column.data_type,
                    column.nullable,
                    procedure,
                    column.sql_type
                )?
            )
            .unwrap();
        }
        output.push_str("}\n\n");
    }

    writeln!(output, "impl TypedProcedure for {call} {{").unwrap();
    let output_type = if result_columns.is_empty() {
        "()".to_owned()
    } else if table_result {
        format!("Vec<{output_name}>")
    } else {
        output_name.clone()
    };
    writeln!(output, "    type Output = {output_type};").unwrap();
    writeln!(
        output,
        "    const NAME: &'static str = {:?};",
        procedure.name
    )
    .unwrap();
    writeln!(output, "    const SCHEMA: &'static str = {schema:?};").unwrap();
    writeln!(output, "    const PROCEDURE: &'static str = {name:?};").unwrap();
    output.push_str(
        "    fn positional_parameters(&self) -> Result<Vec<WireValue>, OperationContractError> {\n        Ok(vec![\n",
    );
    for argument in &inputs {
        let expression = encode_argument(argument, procedure)?;
        writeln!(output, "            {expression},").unwrap();
    }
    output.push_str("        ])\n    }\n");
    output.push_str(
        "    fn decode(self, result: ProcedureResult) -> Result<Self::Output, OperationContractError> {\n",
    );
    if result_columns.is_empty() {
        output.push_str(
            "        match result {\n            ProcedureResult::CommandComplete { .. } => Ok(()),\n            ProcedureResult::Rows(_) => Err(OperationContractError::UnexpectedResult(\"procedure returned rows but no output contract was declared\")),\n        }\n",
        );
    } else if table_result {
        output.push_str(
            "        let ProcedureResult::Rows(QueryResult { columns, rows, .. }) = result else {\n            return Err(OperationContractError::UnexpectedResult(\"procedure row set expected\"));\n        };\n",
        );
        writeln!(
            output,
            "        if columns.len() != {} || rows.iter().any(|row| row.values.len() != {}) {{",
            result_columns.len(),
            result_columns.len()
        )
        .unwrap();
        output.push_str(
            "            return Err(OperationContractError::UnexpectedResult(\"procedure output shape differs from descriptor\"));\n        }\n",
        );
        for (index, column) in result_columns.iter().enumerate() {
            writeln!(
                output,
                "        if columns[{index}].name != {:?} {{ return Err(OperationContractError::UnexpectedResult(\"procedure output column differs from descriptor\")); }}",
                column.name
            )
            .unwrap();
        }
        output.push_str(
            "        rows.into_iter().map(|row| {\n            let values = row.values;\n",
        );
        writeln!(output, "            Ok({output_name} {{").unwrap();
        for (index, column) in result_columns.iter().enumerate() {
            writeln!(
                output,
                "                {}: {},",
                rust_field_name(column.name)?,
                decode_column(index, column, procedure)?
            )
            .unwrap();
        }
        output.push_str("            })\n        }).collect()\n");
    } else {
        output.push_str(
            "        let ProcedureResult::Rows(QueryResult { columns, rows, .. }) = result else {\n            return Err(OperationContractError::UnexpectedResult(\"procedure output row expected\"));\n        };\n",
        );
        writeln!(
            output,
            "        if columns.len() != {} || rows.len() != 1 || rows[0].values.len() != {} {{",
            result_columns.len(),
            result_columns.len()
        )
        .unwrap();
        output.push_str(
            "            return Err(OperationContractError::UnexpectedResult(\"procedure output shape differs from descriptor\"));\n        }\n",
        );
        output.push_str("        let values = &rows[0].values;\n");
        for (index, column) in result_columns.iter().enumerate() {
            writeln!(
                output,
                "        if columns[{index}].name != {:?} {{ return Err(OperationContractError::UnexpectedResult(\"procedure output column differs from descriptor\")); }}",
                column.name
            )
            .unwrap();
        }
        writeln!(output, "        Ok({output_name} {{").unwrap();
        for (index, column) in result_columns.iter().enumerate() {
            writeln!(
                output,
                "            {}: {},",
                rust_field_name(column.name)?,
                decode_column(index, column, procedure)?
            )
            .unwrap();
        }
        output.push_str("        })\n");
    }
    output.push_str("    }\n}\n\n");
    Ok(())
}

struct ResultColumnRef<'a> {
    name: &'a str,
    sql_type: &'a str,
    data_type: Option<&'a DataTypeDescriptor>,
    nullable: bool,
}

fn result_columns<'a>(
    procedure: &'a ProcedureDescriptor,
    output_arguments: &[&'a RoutineArgumentDescriptor],
) -> Result<Vec<ResultColumnRef<'a>>, ApplicationCodegenError> {
    if !output_arguments.is_empty() {
        return Ok(output_arguments
            .iter()
            .map(|argument| ResultColumnRef {
                name: &argument.name,
                sql_type: &argument.sql_type,
                data_type: argument.data_type.as_ref(),
                nullable: argument.nullable,
            })
            .collect());
    }
    Ok(match &procedure.result {
        RoutineResultDescriptor::Void => Vec::new(),
        RoutineResultDescriptor::Scalar {
            sql_type,
            data_type,
            nullable,
        } => vec![ResultColumnRef {
            name: "value",
            sql_type,
            data_type: data_type.as_ref(),
            nullable: *nullable,
        }],
        RoutineResultDescriptor::Table { columns } => {
            columns.iter().map(result_column_ref).collect::<Vec<_>>()
        }
        RoutineResultDescriptor::Trigger => {
            return Err(ApplicationCodegenError::MixedProcedureResult(
                procedure.name.clone(),
            ));
        }
    })
}

fn result_column_ref(column: &RoutineResultColumnDescriptor) -> ResultColumnRef<'_> {
    ResultColumnRef {
        name: &column.name,
        sql_type: &column.sql_type,
        data_type: column.data_type.as_ref(),
        nullable: column.nullable,
    }
}

fn rust_field_type(
    argument: &RoutineArgumentDescriptor,
    procedure: &ProcedureDescriptor,
) -> Result<String, ApplicationCodegenError> {
    rust_result_type(
        argument.data_type.as_ref(),
        argument.nullable,
        procedure,
        &argument.sql_type,
    )
}

fn rust_result_type(
    data_type: Option<&DataTypeDescriptor>,
    nullable: bool,
    procedure: &ProcedureDescriptor,
    sql_type: &str,
) -> Result<String, ApplicationCodegenError> {
    let Some(data_type) = data_type else {
        return Err(ApplicationCodegenError::ExternalType {
            procedure: procedure.name.clone(),
            sql_type: sql_type.to_owned(),
        });
    };
    let value = match data_type {
        DataTypeDescriptor::Integer => "i64",
        DataTypeDescriptor::Float | DataTypeDescriptor::DoublePrecision => "f64",
        DataTypeDescriptor::Text { .. }
        | DataTypeDescriptor::Timestamp
        | DataTypeDescriptor::CivilTimestamp
        | DataTypeDescriptor::Time
        | DataTypeDescriptor::Date
        | DataTypeDescriptor::Uuid
        | DataTypeDescriptor::Bytes
        | DataTypeDescriptor::Decimal { .. } => "String",
        DataTypeDescriptor::Boolean => "bool",
        DataTypeDescriptor::Json => "serde_json::Value",
        DataTypeDescriptor::Vector { .. } => "Vec<f32>",
        DataTypeDescriptor::Null => {
            return Err(ApplicationCodegenError::ExternalType {
                procedure: procedure.name.clone(),
                sql_type: sql_type.to_owned(),
            });
        }
    };
    Ok(if nullable {
        format!("Option<{value}>")
    } else {
        value.to_owned()
    })
}

fn encode_argument(
    argument: &RoutineArgumentDescriptor,
    procedure: &ProcedureDescriptor,
) -> Result<String, ApplicationCodegenError> {
    let Some(data_type) = argument.data_type.as_ref() else {
        return Err(ApplicationCodegenError::ExternalType {
            procedure: procedure.name.clone(),
            sql_type: argument.sql_type.clone(),
        });
    };
    let field = format!("self.{}", rust_field_name(&argument.name)?);
    let typed = if argument.nullable {
        format!(
            "match &{field} {{ Some(value) => {}, None => radixdb_orm::TypedValue::Null({}) }}",
            typed_value_expression("value", data_type, true),
            rust_data_type(data_type)
        )
    } else {
        typed_value_expression(&field, data_type, false)
    };
    Ok(format!(
        "typed_value_to_wire(&({typed})).map_err(|error| OperationContractError::Encode(error.to_string()))?"
    ))
}

fn decode_column(
    index: usize,
    column: &ResultColumnRef<'_>,
    procedure: &ProcedureDescriptor,
) -> Result<String, ApplicationCodegenError> {
    let Some(data_type) = column.data_type else {
        return Err(ApplicationCodegenError::ExternalType {
            procedure: procedure.name.clone(),
            sql_type: column.sql_type.to_owned(),
        });
    };
    let decode = format!(
        "wire_value_to_typed(&values[{index}], &{}).map_err(|error| OperationContractError::Decode(error.to_string()))?",
        rust_data_type(data_type)
    );
    let expected = typed_value_pattern(data_type, "value");
    let value = typed_value_output(data_type, "value");
    if column.nullable {
        Ok(format!(
            "match {decode} {{ radixdb_orm::TypedValue::Null(_) => None, {expected} => Some({value}), _ => return Err(OperationContractError::Decode(\"procedure output type differs from descriptor\".to_owned())) }}"
        ))
    } else {
        Ok(format!(
            "match {decode} {{ {expected} => {value}, _ => return Err(OperationContractError::Decode(\"procedure output type differs from descriptor\".to_owned())) }}"
        ))
    }
}

fn typed_value_expression(value: &str, data_type: &DataTypeDescriptor, borrowed: bool) -> String {
    match data_type {
        DataTypeDescriptor::Integer if borrowed => {
            format!("radixdb_orm::TypedValue::Integer(*{value})")
        }
        DataTypeDescriptor::Integer => format!("radixdb_orm::TypedValue::Integer({value})"),
        DataTypeDescriptor::Float | DataTypeDescriptor::DoublePrecision if borrowed => {
            format!("radixdb_orm::TypedValue::Float((*{value}).into())")
        }
        DataTypeDescriptor::Float | DataTypeDescriptor::DoublePrecision => {
            format!("radixdb_orm::TypedValue::Float({value}.into())")
        }
        DataTypeDescriptor::Text { .. } => {
            format!("radixdb_orm::TypedValue::Text({value}.clone())")
        }
        DataTypeDescriptor::Boolean if borrowed => {
            format!("radixdb_orm::TypedValue::Boolean(*{value})")
        }
        DataTypeDescriptor::Boolean => format!("radixdb_orm::TypedValue::Boolean({value})"),
        DataTypeDescriptor::Timestamp => {
            format!("radixdb_orm::TypedValue::Timestamp({value}.clone())")
        }
        DataTypeDescriptor::CivilTimestamp => {
            format!("radixdb_orm::TypedValue::CivilTimestamp({value}.clone())")
        }
        DataTypeDescriptor::Time => {
            format!("radixdb_orm::TypedValue::Time({value}.clone())")
        }
        DataTypeDescriptor::Date => format!("radixdb_orm::TypedValue::Date({value}.clone())"),
        DataTypeDescriptor::Json => format!("radixdb_orm::TypedValue::Json({value}.clone())"),
        DataTypeDescriptor::Uuid => format!("radixdb_orm::TypedValue::Uuid({value}.clone())"),
        DataTypeDescriptor::Bytes => format!("radixdb_orm::TypedValue::Bytes({value}.clone())"),
        DataTypeDescriptor::Decimal { .. } => {
            format!("radixdb_orm::TypedValue::Decimal({value}.clone())")
        }
        DataTypeDescriptor::Vector { .. } => {
            format!("radixdb_orm::TypedValue::Vector({value}.clone())")
        }
        DataTypeDescriptor::Null => {
            "radixdb_orm::TypedValue::Null(radixdb_orm::DataTypeDescriptor::Null)".to_owned()
        }
    }
}

fn typed_value_pattern(data_type: &DataTypeDescriptor, binding: &str) -> String {
    let variant = match data_type {
        DataTypeDescriptor::Integer => "Integer",
        DataTypeDescriptor::Float | DataTypeDescriptor::DoublePrecision => "Float",
        DataTypeDescriptor::Text { .. } => "Text",
        DataTypeDescriptor::Boolean => "Boolean",
        DataTypeDescriptor::Timestamp => "Timestamp",
        DataTypeDescriptor::CivilTimestamp => "CivilTimestamp",
        DataTypeDescriptor::Time => "Time",
        DataTypeDescriptor::Date => "Date",
        DataTypeDescriptor::Json => "Json",
        DataTypeDescriptor::Uuid => "Uuid",
        DataTypeDescriptor::Bytes => "Bytes",
        DataTypeDescriptor::Decimal { .. } => "Decimal",
        DataTypeDescriptor::Vector { .. } => "Vector",
        DataTypeDescriptor::Null => "Null",
    };
    format!("radixdb_orm::TypedValue::{variant}({binding})")
}

fn typed_value_output(data_type: &DataTypeDescriptor, binding: &str) -> String {
    if matches!(
        data_type,
        DataTypeDescriptor::Float | DataTypeDescriptor::DoublePrecision
    ) {
        format!("{binding}.as_f64()")
    } else {
        binding.to_owned()
    }
}

fn rust_data_type(data_type: &DataTypeDescriptor) -> String {
    match data_type {
        DataTypeDescriptor::Null => "radixdb_orm::DataTypeDescriptor::Null".to_owned(),
        DataTypeDescriptor::Integer => "radixdb_orm::DataTypeDescriptor::Integer".to_owned(),
        DataTypeDescriptor::Float => "radixdb_orm::DataTypeDescriptor::Float".to_owned(),
        DataTypeDescriptor::DoublePrecision => {
            "radixdb_orm::DataTypeDescriptor::DoublePrecision".to_owned()
        }
        DataTypeDescriptor::Text { max_chars } => format!(
            "radixdb_orm::DataTypeDescriptor::Text {{ max_chars: {max_chars:?} }}"
        ),
        DataTypeDescriptor::Boolean => "radixdb_orm::DataTypeDescriptor::Boolean".to_owned(),
        DataTypeDescriptor::Timestamp => "radixdb_orm::DataTypeDescriptor::Timestamp".to_owned(),
        DataTypeDescriptor::CivilTimestamp => {
            "radixdb_orm::DataTypeDescriptor::CivilTimestamp".to_owned()
        }
        DataTypeDescriptor::Time => "radixdb_orm::DataTypeDescriptor::Time".to_owned(),
        DataTypeDescriptor::Date => "radixdb_orm::DataTypeDescriptor::Date".to_owned(),
        DataTypeDescriptor::Json => "radixdb_orm::DataTypeDescriptor::Json".to_owned(),
        DataTypeDescriptor::Uuid => "radixdb_orm::DataTypeDescriptor::Uuid".to_owned(),
        DataTypeDescriptor::Bytes => "radixdb_orm::DataTypeDescriptor::Bytes".to_owned(),
        DataTypeDescriptor::Decimal { precision, scale } => format!(
            "radixdb_orm::DataTypeDescriptor::Decimal {{ precision: {precision:?}, scale: {scale:?} }}"
        ),
        DataTypeDescriptor::Vector { dimensions } => format!(
            "radixdb_orm::DataTypeDescriptor::Vector {{ dimensions: {dimensions} }}"
        ),
    }
}

fn split_procedure_name(name: &str) -> Result<(&str, &str), ApplicationCodegenError> {
    let parts = name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [schema, procedure] if !schema.is_empty() && !procedure.is_empty() => {
            Ok((schema, procedure))
        }
        _ => Err(ApplicationCodegenError::ProcedureName(name.to_owned())),
    }
}

fn rust_type_name(identifier: &str) -> Result<String, ApplicationCodegenError> {
    let mut output = String::new();
    for component in identifier.split('.') {
        for part in component.split('_').filter(|part| !part.is_empty()) {
            let mut characters = part.chars();
            let Some(first) = characters.next() else {
                continue;
            };
            if !(first == '_' || first.is_alphabetic())
                || characters
                    .clone()
                    .any(|character| !(character == '_' || character.is_alphanumeric()))
            {
                return Err(ApplicationCodegenError::Identifier(identifier.to_owned()));
            }
            output.extend(first.to_uppercase());
            output.extend(characters);
        }
    }
    if output.is_empty() {
        return Err(ApplicationCodegenError::Identifier(identifier.to_owned()));
    }
    Ok(output)
}

fn rust_field_name(identifier: &str) -> Result<String, ApplicationCodegenError> {
    if identifier.is_empty()
        || identifier.chars().enumerate().any(|(index, character)| {
            if index == 0 {
                !(character == '_' || character.is_alphabetic())
            } else {
                !(character == '_' || character.is_alphanumeric())
            }
        })
    {
        return Err(ApplicationCodegenError::Identifier(identifier.to_owned()));
    }
    let normalized = identifier.to_lowercase();
    if is_rust_keyword(&normalized) {
        Ok(format!("r#{normalized}"))
    } else {
        Ok(normalized)
    }
}

fn rust_const_name(identifier: &str) -> Result<String, ApplicationCodegenError> {
    let mut output = String::new();
    for character in identifier.chars() {
        if character == '.' || character == '_' {
            if !output.ends_with('_') {
                output.push('_');
            }
        } else if character.is_alphanumeric() {
            output.extend(character.to_uppercase());
        } else {
            return Err(ApplicationCodegenError::Identifier(identifier.to_owned()));
        }
    }
    if output.is_empty()
        || output
            .chars()
            .next()
            .is_some_and(|value| value.is_numeric())
    {
        return Err(ApplicationCodegenError::Identifier(identifier.to_owned()));
    }
    Ok(output)
}

fn is_rust_keyword(value: &str) -> bool {
    matches!(
        value,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use radixdb_orm::{
        DataTypeDescriptor, DatabaseDescriptor, DescriptorEnvelope, DescriptorKind,
        ProcedureDescriptor, RoutineArgumentDescriptor, RoutineArgumentModeDescriptor,
        RoutineResourcePolicyDescriptor, RoutineResultColumnDescriptor, RoutineResultDescriptor,
        RoutineSecurityDescriptor, RoutineVolatilityDescriptor,
    };

    use super::*;

    fn descriptor() -> DescriptorEnvelope<DatabaseDescriptor> {
        let mut procedure = ProcedureDescriptor {
            catalog_id: "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e01".to_owned(),
            name: "app.update_document".to_owned(),
            definition_revision: 1,
            fingerprint: String::new(),
            source_sha256: "a".repeat(64),
            language: "radix_pl".to_owned(),
            language_version: 1,
            compiler_abi: 1,
            runtime_abi: 1,
            security: RoutineSecurityDescriptor::Invoker,
            volatility: RoutineVolatilityDescriptor::Volatile,
            arguments: vec![
                RoutineArgumentDescriptor {
                    ordinal: 0,
                    name: "document_id".to_owned(),
                    mode: RoutineArgumentModeDescriptor::In,
                    sql_type: "UUID".to_owned(),
                    data_type: Some(DataTypeDescriptor::Uuid),
                    nullable: false,
                    default_expression: None,
                },
                RoutineArgumentDescriptor {
                    ordinal: 1,
                    name: "version".to_owned(),
                    mode: RoutineArgumentModeDescriptor::InOut,
                    sql_type: "INTEGER".to_owned(),
                    data_type: Some(DataTypeDescriptor::Integer),
                    nullable: false,
                    default_expression: None,
                },
            ],
            result: RoutineResultDescriptor::Void,
            resource_policy: RoutineResourcePolicyDescriptor {
                instructions: 100,
                heap_bytes: 1024,
                frames: 8,
                sql_statements: 10,
                rows: 100,
                result_bytes: 4096,
                deadline_ms: 1000,
            },
        };
        procedure.refresh_fingerprint().unwrap();
        let mut database = DatabaseDescriptor {
            schema_generation: 1,
            fingerprint: String::new(),
            tables: Vec::new(),
            views: Vec::new(),
            procedures: vec![procedure],
            extensions: BTreeMap::new(),
        };
        database.refresh_fingerprint().unwrap();
        DescriptorEnvelope::new(DescriptorKind::Database, database)
    }

    #[test]
    fn typed_call_is_generated_from_the_fingerprinted_procedure_contract() {
        let generated = generate_rust_application(&descriptor()).unwrap();
        assert!(generated
            .source
            .contains("pub struct AppUpdateDocumentCall"));
        assert!(generated
            .source
            .contains("#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]"));
        assert!(generated.source.contains("#[serde(deny_unknown_fields)]"));
        assert!(generated.source.contains("pub document_id: String"));
        assert!(generated.source.contains("pub version: i64"));
        assert!(generated
            .source
            .contains("pub struct AppUpdateDocumentOutput"));
        assert!(generated
            .source
            .contains("const PROCEDURE: &'static str = \"update_document\""));
        assert!(generated.source.contains("columns[0].name != \"version\""));
        assert!(generated
            .source
            .contains("radixdb_orm::TypedValue::Uuid(self.document_id.clone())"));
        assert!(generated
            .source
            .contains("radixdb_orm::TypedValue::Integer(self.version)"));
        assert!(!generated.source.contains("*(&self."));
        assert!(!generated.source.contains("(&self."));
    }

    #[test]
    fn double_precision_generates_f64_with_distinct_schema_identity() {
        let mut descriptor = descriptor();
        let argument = &mut descriptor.payload.procedures[0].arguments[1];
        argument.sql_type = "DOUBLE PRECISION".to_owned();
        argument.data_type = Some(DataTypeDescriptor::DoublePrecision);
        descriptor.payload.procedures[0]
            .refresh_fingerprint()
            .unwrap();
        descriptor.payload.refresh_fingerprint().unwrap();

        let generated = generate_rust_application(&descriptor).unwrap();
        assert!(generated.source.contains("pub version: f64"));
        assert!(generated
            .source
            .contains("radixdb_orm::DataTypeDescriptor::DoublePrecision"));
        assert!(generated
            .source
            .contains("radixdb_orm::TypedValue::Float(self.version.into())"));
    }

    #[test]
    fn a_modified_procedure_descriptor_is_rejected_before_generation() {
        let mut descriptor = descriptor();
        descriptor.payload.procedures[0].runtime_abi = 2;
        assert!(matches!(
            generate_rust_application(&descriptor),
            Err(ApplicationCodegenError::Orm(
                OrmCodegenError::Fingerprint { .. }
            )) | Err(ApplicationCodegenError::ProcedureFingerprint { .. })
        ));
    }

    #[test]
    fn table_result_is_generated_as_a_bounded_row_vector() {
        let mut descriptor = descriptor();
        let procedure = &mut descriptor.payload.procedures[0];
        procedure
            .arguments
            .retain(|argument| argument.mode == RoutineArgumentModeDescriptor::In);
        procedure.result = RoutineResultDescriptor::Table {
            columns: vec![RoutineResultColumnDescriptor {
                ordinal: 0,
                name: "document_id".to_owned(),
                sql_type: "UUID".to_owned(),
                data_type: Some(DataTypeDescriptor::Uuid),
                nullable: false,
            }],
        };
        procedure.refresh_fingerprint().unwrap();
        descriptor.payload.refresh_fingerprint().unwrap();

        let generated = generate_rust_application(&descriptor).unwrap();
        assert!(generated
            .source
            .contains("type Output = Vec<AppUpdateDocumentOutput>;"));
        assert!(generated
            .source
            .contains("rows.iter().any(|row| row.values.len() != 1)"));
        assert!(generated.source.contains("rows.into_iter().map(|row|"));
        assert!(!generated.source.contains("rows.len() != 1"));
    }

    #[test]
    fn generated_contract_ignores_physical_catalog_identity() {
        let first = descriptor();
        let mut second = first.clone();
        second.payload.schema_generation = 99;
        second.payload.procedures[0].catalog_id = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e99".to_owned();
        second.payload.procedures[0].definition_revision = 17;
        second.payload.procedures[0].refresh_fingerprint().unwrap();
        second.payload.refresh_fingerprint().unwrap();

        let first = generate_rust_application(&first).unwrap();
        let second = generate_rust_application(&second).unwrap();
        assert_eq!(first.descriptor_fingerprint, second.descriptor_fingerprint);
        assert_eq!(first.source, second.source);
    }

    #[test]
    fn startup_fingerprints_validate_catalog_and_normalize_physical_identity() {
        let first = descriptor();
        let mut second = first.clone();
        second.payload.schema_generation = 99;
        second.payload.procedures[0].catalog_id = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e99".to_owned();
        second.payload.procedures[0].definition_revision = 17;
        second.payload.procedures[0].refresh_fingerprint().unwrap();
        second.payload.refresh_fingerprint().unwrap();

        let first_fingerprints = application_descriptor_fingerprints(&first).unwrap();
        let second_fingerprints = application_descriptor_fingerprints(&second).unwrap();
        assert_ne!(first_fingerprints.catalog, second_fingerprints.catalog);
        assert_eq!(first_fingerprints.schema, second_fingerprints.schema);

        second.payload.procedures[0].runtime_abi = 2;
        assert!(matches!(
            application_descriptor_fingerprints(&second),
            Err(ApplicationCodegenError::FingerprintMismatch { .. })
        ));
    }
}
