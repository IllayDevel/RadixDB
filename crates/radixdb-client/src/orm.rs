//! Client-side ORM execution extensions. These methods borrow the existing
//! connection; they do not create a wrapper session or independent transport.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use base64::Engine as _;
use chrono::{DateTime, NaiveDate};
use radixdb_orm::{
    CatalogOperation, ColumnDescriptor, ConstraintDescriptor, DatabaseDescriptor,
    DescriptorEnvelope, DescriptorError, DescriptorKind, IndexDescriptor, IrDocument, RenderError,
    TableDescriptor, TypedValue,
};

use crate::{ClientError, Connection, Cursor, ExecuteResult, Row, WireValue};

#[derive(Debug, thiserror::Error)]
pub enum OrmClientError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Render(#[from] RenderError),
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),
    #[error("invalid typed ORM value: {0}")]
    InvalidValue(String),
    #[error("unexpected ORM result: {0}")]
    UnexpectedResult(&'static str),
    #[error("ORM record was not found")]
    NotFound,
    #[error(transparent)]
    Record(#[from] radixdb_orm::RecordError),
    #[error(transparent)]
    Build(#[from] radixdb_orm::BuilderError),
    #[error(transparent)]
    Generated(#[from] radixdb_orm::GeneratedRecordError),
}

impl<S: Read + Write> Connection<S> {
    /// Compile and execute ORM IR over this exact connection/session.
    pub fn execute_orm(&mut self, document: &IrDocument) -> Result<ExecuteResult, OrmClientError> {
        let compiled = document.to_sql()?;
        let parameters = compiled
            .parameters
            .iter()
            .map(typed_value_to_wire)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self.execute_with_positional_parameters(compiled.sql, parameters)?)
    }

    /// Borrow the same connection for schema/catalog operations.
    pub fn schema(&mut self) -> SchemaClient<'_, S> {
        SchemaClient { connection: self }
    }

    pub fn entity(
        &mut self,
        table: impl Into<String>,
    ) -> Result<radixdb_orm::DynamicEntity, OrmClientError> {
        Ok(radixdb_orm::DynamicEntity::new(
            self.schema().table(table).describe().fetch()?,
        ))
    }
}

impl<S: Read + Write> radixdb_orm::OrmSession for &mut Connection<S> {
    type CommandOutput = ExecuteResult;
    type QueryOutput = ExecuteResult;
    type Error = OrmClientError;

    fn execute_document(self, document: &IrDocument) -> Result<Self::CommandOutput, Self::Error> {
        self.execute_orm(document)
    }

    fn query_document(self, document: &IrDocument) -> Result<Self::QueryOutput, Self::Error> {
        self.execute_orm(document)
    }
}

impl<S: Read + Write> radixdb_orm::OrmRecordSession for &mut Connection<S> {
    type Error = OrmClientError;

    fn mutate_record(
        self,
        record: &mut radixdb_orm::DynamicRecord,
        mutation: radixdb_orm::RecordMutation,
    ) -> Result<(), Self::Error> {
        let document = match mutation {
            radixdb_orm::RecordMutation::Insert => record.insert_document()?,
            radixdb_orm::RecordMutation::Save => record.save_document()?,
            radixdb_orm::RecordMutation::Update => record.update_document()?,
            radixdb_orm::RecordMutation::Delete => record.delete_document()?,
        };
        let result = self.execute_orm(&document)?;
        if mutation == radixdb_orm::RecordMutation::Delete {
            let ExecuteResult::CommandComplete { affected_rows, .. } = result else {
                return Err(OrmClientError::UnexpectedResult(
                    "DELETE expected command completion",
                ));
            };
            if affected_rows == 0 {
                return Err(OrmClientError::NotFound);
            }
            return Ok(());
        }
        let values = fetch_one_typed_record(self, result, record.descriptor())?;
        record.apply_returning(values)?;
        Ok(())
    }
}

impl<S: Read + Write> radixdb_orm::OrmGeneratedRecordSession for &mut Connection<S> {
    type Error = OrmClientError;

    fn mutate_generated_record<R: radixdb_orm::GeneratedRecord>(
        self,
        record: &mut R,
        mutation: radixdb_orm::RecordMutation,
    ) -> Result<(), Self::Error> {
        let descriptor = self
            .schema()
            .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
            .describe()
            .fetch()?;
        let mut dynamic = record.to_dynamic(&descriptor)?;
        radixdb_orm::OrmRecordSession::mutate_record(&mut *self, &mut dynamic, mutation)?;
        record.apply_dynamic(&dynamic)?;
        Ok(())
    }
}

impl<S: Read + Write> radixdb_orm::OrmGeneratedQuerySession for &mut Connection<S> {
    type Error = OrmClientError;

    fn query_generated_records<R: radixdb_orm::GeneratedRecord + Default>(
        self,
        document: &IrDocument,
    ) -> Result<Vec<R>, Self::Error> {
        let descriptor = self
            .schema()
            .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
            .describe()
            .fetch()?;
        radixdb_orm::ensure_schema_fingerprint(
            <R::Entity as radixdb_orm::GeneratedEntity>::SCHEMA_FINGERPRINT,
            &descriptor.fingerprint,
        )
        .map_err(radixdb_orm::GeneratedRecordError::from)?;
        let ExecuteResult::Cursor(cursor) = self.execute_orm(document)? else {
            return Err(OrmClientError::UnexpectedResult(
                "generated SELECT expected a cursor",
            ));
        };
        if cursor.columns().len() != descriptor.columns.len() {
            return Err(OrmClientError::UnexpectedResult(
                "generated SELECT columns differ from descriptor",
            ));
        }
        let mut generated = Vec::new();
        loop {
            let batch = self.fetch(&cursor)?;
            for row in batch.rows {
                if row.values.len() != descriptor.columns.len() {
                    return Err(OrmClientError::UnexpectedResult(
                        "generated SELECT row width differs from descriptor",
                    ));
                }
                let values = descriptor
                    .columns
                    .iter()
                    .zip(&row.values)
                    .map(|(column, value)| {
                        Ok((
                            column.name.clone(),
                            wire_value_to_typed(value, &column.data_type)?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, OrmClientError>>()?;
                let mut dynamic = radixdb_orm::DynamicRecord::new(descriptor.clone());
                dynamic.apply_returning(values)?;
                let mut record = R::default();
                record.apply_dynamic(&dynamic)?;
                generated.push(record);
            }
            if batch.eof {
                return Ok(generated);
            }
        }
    }
}

fn fetch_one_typed_record<S: Read + Write>(
    connection: &mut Connection<S>,
    result: ExecuteResult,
    descriptor: &TableDescriptor,
) -> Result<BTreeMap<String, TypedValue>, OrmClientError> {
    let ExecuteResult::Cursor(cursor) = result else {
        return Err(OrmClientError::UnexpectedResult(
            "RETURNING expected a cursor",
        ));
    };
    let columns = cursor.columns().to_vec();
    let mut rows = Vec::new();
    loop {
        let batch = connection.fetch(&cursor)?;
        rows.extend(batch.rows);
        if batch.eof {
            break;
        }
    }
    let [row] = rows.as_slice() else {
        if rows.is_empty() {
            return Err(OrmClientError::NotFound);
        }
        return Err(OrmClientError::UnexpectedResult(
            "RETURNING produced multiple rows",
        ));
    };
    if row.values.len() != descriptor.columns.len() || columns.len() != descriptor.columns.len() {
        return Err(OrmClientError::UnexpectedResult(
            "RETURNING row width differs from descriptor",
        ));
    }
    descriptor
        .columns
        .iter()
        .zip(&row.values)
        .map(|(column, value)| {
            Ok((
                column.name.clone(),
                wire_value_to_typed(value, &column.data_type)?,
            ))
        })
        .collect()
}

fn wire_value_to_typed(
    value: &WireValue,
    declared: &radixdb_orm::DataTypeDescriptor,
) -> Result<TypedValue, OrmClientError> {
    use radixdb_orm::FloatValue;
    Ok(match value {
        WireValue::Null => TypedValue::Null(declared.clone()),
        WireValue::Bool(value) => TypedValue::Boolean(*value),
        WireValue::Int(value) => TypedValue::Integer(*value),
        WireValue::Int8(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::Int16(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::Int32(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::UInt(value) => TypedValue::Integer(i64::try_from(*value).map_err(|_| {
            OrmClientError::InvalidValue("unsigned result exceeds i64".to_string())
        })?),
        WireValue::UInt8(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::UInt16(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::UInt32(value) => TypedValue::Integer(i64::from(*value)),
        WireValue::Float64(value) => TypedValue::Float(FloatValue::from(*value)),
        WireValue::Decimal {
            unscaled, scale, ..
        } => TypedValue::Decimal(format_decimal(*unscaled, *scale)),
        WireValue::String(value) => TypedValue::Text(value.clone()),
        WireValue::Bytes(value) => {
            TypedValue::Bytes(base64::engine::general_purpose::STANDARD.encode(value))
        }
        WireValue::Date {
            days_since_unix_epoch,
        } => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            TypedValue::Date(
                (epoch + chrono::Duration::days(i64::from(*days_since_unix_epoch)))
                    .format("%Y-%m-%d")
                    .to_string(),
            )
        }
        WireValue::DateTime {
            millis_since_unix_epoch_utc,
        } => {
            let timestamp = DateTime::from_timestamp_millis(*millis_since_unix_epoch_utc)
                .ok_or_else(|| {
                    OrmClientError::InvalidValue("invalid DATETIME result".to_string())
                })?;
            TypedValue::Timestamp(timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        WireValue::TimestampNanos {
            nanos_since_unix_epoch_utc,
        } => {
            let seconds = nanos_since_unix_epoch_utc.div_euclid(1_000_000_000);
            let nanos = nanos_since_unix_epoch_utc.rem_euclid(1_000_000_000) as u32;
            let timestamp = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
                OrmClientError::InvalidValue("invalid TIMESTAMP result".to_string())
            })?;
            TypedValue::Timestamp(timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        WireValue::Json(value) => TypedValue::Json(
            serde_json::from_str(value)
                .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?,
        ),
        WireValue::Vector(bytes) => {
            if bytes.len() % 4 != 0 {
                return Err(OrmClientError::InvalidValue(
                    "invalid VECTOR result".to_string(),
                ));
            }
            TypedValue::Vector(
                bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("exact chunk")))
                    .collect(),
            )
        }
        WireValue::Uuid(value) => TypedValue::Uuid(uuid::Uuid::from_bytes(*value).to_string()),
        WireValue::External { .. } => {
            return Err(OrmClientError::InvalidValue(
                "external values require a generated plugin-aware ORM adapter".to_string(),
            ));
        }
    })
}

fn format_decimal(unscaled: i128, scale: u8) -> String {
    let negative = unscaled < 0;
    let mut digits = unscaled.unsigned_abs().to_string();
    if scale > 0 {
        let scale = usize::from(scale);
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    digits
}

pub struct SchemaClient<'a, S> {
    connection: &'a mut Connection<S>,
}

impl<'a, S: Read + Write> SchemaClient<'a, S> {
    pub fn tables(self) -> ListTablesRequest<'a, S> {
        ListTablesRequest {
            connection: self.connection,
        }
    }

    pub fn table(self, name: impl Into<String>) -> TableSchemaClient<'a, S> {
        TableSchemaClient {
            connection: self.connection,
            table: name.into(),
        }
    }

    pub fn describe_database(self) -> DescribeDatabaseRequest<'a, S> {
        DescribeDatabaseRequest {
            connection: self.connection,
        }
    }

    pub fn create_table(self, table: impl Into<String>) -> CreateTableRequest<'a, S> {
        let table = table.into();
        CreateTableRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::create_table(table.clone()),
            table,
        }
    }

    pub fn alter_table(self, table: impl Into<String>) -> AlterTableRequest<'a, S> {
        let table = table.into();
        AlterTableRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::alter_table(table.clone()),
            result_table: table,
        }
    }

    pub fn create_table_as(
        self,
        table: impl Into<String>,
        query: radixdb_orm::QueryBuilder,
    ) -> CreateTableAsRequest<'a, S> {
        let table = table.into();
        CreateTableAsRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::create_table_as(table.clone(), query),
            table,
        }
    }

    pub fn drop_table(self, table: impl Into<String>) -> DdlRequest<'a, S> {
        DdlRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::drop_table(table),
        }
    }

    pub fn truncate_table(self, table: impl Into<String>) -> DdlRequest<'a, S> {
        DdlRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::truncate_table(table),
        }
    }

    pub fn create_index(self, index: radixdb_orm::IndexDefinition) -> DdlRequest<'a, S> {
        DdlRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::create_index(index),
        }
    }

    pub fn drop_index(
        self,
        table: impl Into<String>,
        index: impl Into<String>,
        if_exists: bool,
    ) -> DdlRequest<'a, S> {
        DdlRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::drop_index(table, index, if_exists),
        }
    }

    pub fn alter_index(
        self,
        index: impl Into<String>,
        new_name: impl Into<String>,
    ) -> DdlRequest<'a, S> {
        DdlRequest {
            connection: self.connection,
            builder: radixdb_orm::DdlBuilder::alter_index(index, new_name),
        }
    }
}

pub struct CreateTableAsRequest<'a, S> {
    connection: &'a mut Connection<S>,
    builder: radixdb_orm::CreateTableAsBuilder,
    table: String,
}

impl<S: Read + Write> CreateTableAsRequest<'_, S> {
    pub fn if_not_exists(mut self, value: bool) -> Self {
        self.builder = self.builder.if_not_exists(value);
        self
    }
    pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
        radixdb_orm::OrmBuilder::to_json(&self.builder)
    }
    pub fn to_sql(&self) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
        radixdb_orm::OrmBuilder::to_sql(&self.builder)
    }
    pub fn execute(self) -> Result<TableDescriptor, OrmClientError> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)?;
        expect_command(self.connection.execute_orm(&document)?)?;
        describe_table(self.connection, self.table)
    }
}

pub struct CreateTableRequest<'a, S> {
    connection: &'a mut Connection<S>,
    builder: radixdb_orm::CreateTableBuilder,
    table: String,
}

impl<S: Read + Write> CreateTableRequest<'_, S> {
    pub fn if_not_exists(mut self, value: bool) -> Self {
        self.builder = self.builder.if_not_exists(value);
        self
    }
    pub fn column(mut self, column: radixdb_orm::Column) -> Self {
        self.builder = self.builder.column(column);
        self
    }
    pub fn constraint(mut self, constraint: radixdb_orm::ConstraintDefinitionIr) -> Self {
        self.builder = self.builder.constraint(constraint);
        self
    }
    pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
        radixdb_orm::OrmBuilder::to_json(&self.builder)
    }
    pub fn to_sql(&self) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
        radixdb_orm::OrmBuilder::to_sql(&self.builder)
    }
    pub fn execute(self) -> Result<TableDescriptor, OrmClientError> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
        expect_command(self.connection.execute_orm(&document)?)?;
        describe_table(self.connection, self.table)
    }
}

pub struct AlterTableRequest<'a, S> {
    connection: &'a mut Connection<S>,
    builder: radixdb_orm::AlterTableBuilder,
    result_table: String,
}

impl<S: Read + Write> AlterTableRequest<'_, S> {
    pub fn add_column(mut self, column: radixdb_orm::Column) -> Self {
        self.builder = self.builder.add_column(column);
        self
    }
    pub fn modify_column(mut self, column: radixdb_orm::Column) -> Self {
        self.builder = self.builder.modify_column(column);
        self
    }
    pub fn drop_column(mut self, column: impl Into<String>) -> Self {
        self.builder = self.builder.drop_column(column);
        self
    }
    pub fn rename_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.builder = self.builder.rename_column(from, to);
        self
    }
    pub fn rename_table(mut self, to: impl Into<String>) -> Self {
        let to = to.into();
        self.result_table = to.clone();
        self.builder = self.builder.rename_table(to);
        self
    }
    pub fn add_constraint(mut self, constraint: radixdb_orm::ConstraintDefinitionIr) -> Self {
        self.builder = self.builder.add_constraint(constraint);
        self
    }
    pub fn drop_constraint(mut self, name: impl Into<String>, if_exists: bool) -> Self {
        self.builder = self.builder.drop_constraint(name, if_exists);
        self
    }
    pub fn execute(self) -> Result<TableDescriptor, OrmClientError> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
        expect_command(self.connection.execute_orm(&document)?)?;
        describe_table(self.connection, self.result_table)
    }
}

pub struct DdlRequest<'a, S> {
    connection: &'a mut Connection<S>,
    builder: radixdb_orm::DdlBuilder,
}

impl<S: Read + Write> DdlRequest<'_, S> {
    pub fn if_exists(mut self, value: bool) -> Self {
        self.builder = self.builder.if_exists(value);
        self
    }
    pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
        radixdb_orm::OrmBuilder::to_json(&self.builder)
    }
    pub fn to_sql(&self) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
        radixdb_orm::OrmBuilder::to_sql(&self.builder)
    }
    pub fn execute(self) -> Result<u64, OrmClientError> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
        expect_command(self.connection.execute_orm(&document)?)
    }
}

fn expect_command(result: ExecuteResult) -> Result<u64, OrmClientError> {
    match result {
        ExecuteResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
        ExecuteResult::Cursor(_) => Err(OrmClientError::UnexpectedResult(
            "DDL expected command completion",
        )),
    }
}

fn describe_table<S: Read + Write>(
    connection: &mut Connection<S>,
    table: String,
) -> Result<TableDescriptor, OrmClientError> {
    let document = IrDocument::new(radixdb_orm::Operation::Catalog {
        operation: CatalogOperation::DescribeTable { table },
    });
    let result = connection.execute_orm(&document)?;
    let json = fetch_one_text(connection, result)?;
    Ok(DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?.payload)
}

pub struct ListTablesRequest<'a, S> {
    connection: &'a mut Connection<S>,
}

impl<S: Read + Write> ListTablesRequest<'_, S> {
    pub fn fetch(self) -> Result<Vec<String>, OrmClientError> {
        let document = IrDocument::new(radixdb_orm::Operation::Catalog {
            operation: CatalogOperation::ListTables,
        });
        let result = self.connection.execute_orm(&document)?;
        fetch_single_text_column(self.connection, result)
    }
}

pub struct TableSchemaClient<'a, S> {
    connection: &'a mut Connection<S>,
    table: String,
}

impl<'a, S: Read + Write> TableSchemaClient<'a, S> {
    pub fn describe(self) -> DescribeTableRequest<'a, S> {
        DescribeTableRequest {
            connection: self.connection,
            table: self.table,
        }
    }

    pub fn columns(self) -> TableColumnsRequest<'a, S> {
        TableColumnsRequest {
            request: self.describe(),
        }
    }

    pub fn indexes(self) -> TableIndexesRequest<'a, S> {
        TableIndexesRequest {
            request: self.describe(),
        }
    }

    pub fn constraints(self) -> TableConstraintsRequest<'a, S> {
        TableConstraintsRequest {
            request: self.describe(),
        }
    }
}

pub struct DescribeTableRequest<'a, S> {
    connection: &'a mut Connection<S>,
    table: String,
}

impl<S: Read + Write> DescribeTableRequest<'_, S> {
    pub fn fetch(self) -> Result<TableDescriptor, OrmClientError> {
        let document = IrDocument::new(radixdb_orm::Operation::Catalog {
            operation: CatalogOperation::DescribeTable { table: self.table },
        });
        let result = self.connection.execute_orm(&document)?;
        let json = fetch_one_text(self.connection, result)?;
        let envelope =
            DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?;
        Ok(envelope.payload)
    }
}

pub struct TableColumnsRequest<'a, S> {
    request: DescribeTableRequest<'a, S>,
}

impl<S: Read + Write> TableColumnsRequest<'_, S> {
    pub fn fetch(self) -> Result<Vec<ColumnDescriptor>, OrmClientError> {
        Ok(self.request.fetch()?.columns)
    }
}

pub struct TableIndexesRequest<'a, S> {
    request: DescribeTableRequest<'a, S>,
}

impl<S: Read + Write> TableIndexesRequest<'_, S> {
    pub fn fetch(self) -> Result<Vec<IndexDescriptor>, OrmClientError> {
        Ok(self.request.fetch()?.indexes)
    }
}

pub struct TableConstraintsRequest<'a, S> {
    request: DescribeTableRequest<'a, S>,
}

impl<S: Read + Write> TableConstraintsRequest<'_, S> {
    pub fn fetch(self) -> Result<Vec<ConstraintDescriptor>, OrmClientError> {
        Ok(self.request.fetch()?.constraints)
    }
}

pub struct DescribeDatabaseRequest<'a, S> {
    connection: &'a mut Connection<S>,
}

impl<S: Read + Write> DescribeDatabaseRequest<'_, S> {
    pub fn fetch(self) -> Result<DatabaseDescriptor, OrmClientError> {
        let document = IrDocument::new(radixdb_orm::Operation::Catalog {
            operation: CatalogOperation::DescribeDatabase,
        });
        let result = self.connection.execute_orm(&document)?;
        let json = fetch_one_text(self.connection, result)?;
        let envelope =
            DescriptorEnvelope::<DatabaseDescriptor>::from_json(&json, DescriptorKind::Database)?;
        Ok(envelope.payload)
    }
}

fn fetch_one_text<S: Read + Write>(
    connection: &mut Connection<S>,
    result: ExecuteResult,
) -> Result<String, OrmClientError> {
    let values = fetch_single_text_column(connection, result)?;
    match values.as_slice() {
        [value] => Ok(value.clone()),
        _ => Err(OrmClientError::UnexpectedResult(
            "expected exactly one text row",
        )),
    }
}

fn fetch_single_text_column<S: Read + Write>(
    connection: &mut Connection<S>,
    result: ExecuteResult,
) -> Result<Vec<String>, OrmClientError> {
    let ExecuteResult::Cursor(cursor) = result else {
        return Err(OrmClientError::UnexpectedResult("expected a cursor"));
    };
    fetch_cursor_text(connection, &cursor)
}

fn fetch_cursor_text<S: Read + Write>(
    connection: &mut Connection<S>,
    cursor: &Cursor,
) -> Result<Vec<String>, OrmClientError> {
    let mut values = Vec::new();
    loop {
        let batch = connection.fetch(cursor)?;
        for Row { values: row } in batch.rows {
            let [WireValue::String(value)] = row.as_slice() else {
                return Err(OrmClientError::UnexpectedResult("expected one text column"));
            };
            values.push(value.clone());
        }
        if batch.eof {
            return Ok(values);
        }
    }
}

pub(crate) fn typed_value_to_wire(value: &TypedValue) -> Result<WireValue, OrmClientError> {
    Ok(match value {
        TypedValue::Null(_) => WireValue::Null,
        TypedValue::Integer(value) => WireValue::Int(*value),
        TypedValue::Float(value) => WireValue::Float64(value.as_f64()),
        TypedValue::Text(value) => WireValue::String(value.clone()),
        TypedValue::Boolean(value) => WireValue::Bool(*value),
        TypedValue::Timestamp(value) => {
            let timestamp = DateTime::parse_from_rfc3339(value).map_err(|error| {
                OrmClientError::InvalidValue(format!("invalid RFC3339 timestamp: {error}"))
            })?;
            let nanos = timestamp.timestamp_nanos_opt().ok_or_else(|| {
                OrmClientError::InvalidValue("timestamp is outside i64 nanoseconds".to_string())
            })?;
            WireValue::TimestampNanos {
                nanos_since_unix_epoch_utc: nanos,
            }
        }
        TypedValue::Date(value) => {
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
                OrmClientError::InvalidValue(format!("invalid ISO date: {error}"))
            })?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days =
                i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| {
                    OrmClientError::InvalidValue("date is outside i32 day domain".to_string())
                })?;
            WireValue::Date {
                days_since_unix_epoch: days,
            }
        }
        TypedValue::Json(value) => {
            WireValue::Json(serde_json::to_string(value).map_err(|error| {
                OrmClientError::InvalidValue(format!("invalid JSON value: {error}"))
            })?)
        }
        TypedValue::Uuid(value) => WireValue::Uuid(
            *uuid::Uuid::parse_str(value)
                .map_err(|error| OrmClientError::InvalidValue(format!("invalid UUID: {error}")))?
                .as_bytes(),
        ),
        TypedValue::Bytes(value) => WireValue::Bytes(
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|error| {
                    OrmClientError::InvalidValue(format!("invalid base64 BYTES: {error}"))
                })?,
        ),
        TypedValue::Decimal(value) => {
            let (unscaled, precision, scale) = radixdb_orm::parse_decimal_literal(value)
                .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
            WireValue::Decimal {
                unscaled,
                precision,
                scale,
            }
        }
        TypedValue::Vector(values) => WireValue::Vector(
            values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect(),
        ),
    })
}

#[cfg(feature = "tokio")]
mod asynchronous {
    use std::collections::BTreeMap;

    use radixdb_orm::{
        CatalogOperation, ColumnDescriptor, ConstraintDescriptor, DatabaseDescriptor,
        DescriptorEnvelope, DescriptorKind, IndexDescriptor, IrDocument, TableDescriptor,
        TypedValue,
    };
    use tokio::io::{AsyncRead, AsyncWrite};

    use crate::{AsyncConnection, ExecuteResult, Row, WireValue};

    use super::{typed_value_to_wire, wire_value_to_typed, OrmClientError};

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncConnection<S> {
        pub async fn execute_orm(
            &mut self,
            document: &IrDocument,
        ) -> Result<ExecuteResult, OrmClientError> {
            let compiled = document.to_sql()?;
            let parameters = compiled
                .parameters
                .iter()
                .map(typed_value_to_wire)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(self
                .execute_with_positional_parameters(compiled.sql, parameters)
                .await?)
        }

        pub fn schema(&mut self) -> AsyncSchemaClient<'_, S> {
            AsyncSchemaClient { connection: self }
        }

        pub async fn entity(
            &mut self,
            table: impl Into<String>,
        ) -> Result<radixdb_orm::DynamicEntity, OrmClientError> {
            Ok(radixdb_orm::DynamicEntity::new(
                self.schema().table(table).describe().fetch().await?,
            ))
        }

        pub async fn mutate_record_orm(
            &mut self,
            record: &mut radixdb_orm::DynamicRecord,
            mutation: radixdb_orm::RecordMutation,
        ) -> Result<(), OrmClientError> {
            let document = match mutation {
                radixdb_orm::RecordMutation::Insert => record.insert_document()?,
                radixdb_orm::RecordMutation::Save => record.save_document()?,
                radixdb_orm::RecordMutation::Update => record.update_document()?,
                radixdb_orm::RecordMutation::Delete => record.delete_document()?,
            };
            let result = self.execute_orm(&document).await?;
            if mutation == radixdb_orm::RecordMutation::Delete {
                let ExecuteResult::CommandComplete { affected_rows, .. } = result else {
                    return Err(OrmClientError::UnexpectedResult(
                        "DELETE expected command completion",
                    ));
                };
                if affected_rows == 0 {
                    return Err(OrmClientError::NotFound);
                }
                return Ok(());
            }
            let values = fetch_one_typed_record(self, result, record.descriptor()).await?;
            record.apply_returning(values)?;
            Ok(())
        }

        pub async fn mutate_generated_record_orm<R: radixdb_orm::GeneratedRecord>(
            &mut self,
            record: &mut R,
            mutation: radixdb_orm::RecordMutation,
        ) -> Result<(), OrmClientError> {
            let descriptor = self
                .schema()
                .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
                .describe()
                .fetch()
                .await?;
            let mut dynamic = record.to_dynamic(&descriptor)?;
            self.mutate_record_orm(&mut dynamic, mutation).await?;
            record.apply_dynamic(&dynamic)?;
            Ok(())
        }

        pub async fn query_generated_records_orm<R: radixdb_orm::GeneratedRecord + Default>(
            &mut self,
            document: &IrDocument,
        ) -> Result<Vec<R>, OrmClientError> {
            let descriptor = self
                .schema()
                .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
                .describe()
                .fetch()
                .await?;
            radixdb_orm::ensure_schema_fingerprint(
                <R::Entity as radixdb_orm::GeneratedEntity>::SCHEMA_FINGERPRINT,
                &descriptor.fingerprint,
            )
            .map_err(radixdb_orm::GeneratedRecordError::from)?;
            let ExecuteResult::Cursor(cursor) = self.execute_orm(document).await? else {
                return Err(OrmClientError::UnexpectedResult(
                    "generated SELECT expected a cursor",
                ));
            };
            let mut generated = Vec::new();
            loop {
                let batch = self.fetch(&cursor).await?;
                for row in batch.rows {
                    if row.values.len() != descriptor.columns.len() {
                        return Err(OrmClientError::UnexpectedResult(
                            "generated SELECT row width differs from descriptor",
                        ));
                    }
                    let values = descriptor
                        .columns
                        .iter()
                        .zip(&row.values)
                        .map(|(column, value)| {
                            Ok((
                                column.name.clone(),
                                wire_value_to_typed(value, &column.data_type)?,
                            ))
                        })
                        .collect::<Result<BTreeMap<_, _>, OrmClientError>>()?;
                    let mut dynamic = radixdb_orm::DynamicRecord::new(descriptor.clone());
                    dynamic.apply_returning(values)?;
                    let mut record = R::default();
                    record.apply_dynamic(&dynamic)?;
                    generated.push(record);
                }
                if batch.eof {
                    return Ok(generated);
                }
            }
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> radixdb_orm::AsyncOrmSession for AsyncConnection<S> {
        type CommandOutput = ExecuteResult;
        type QueryOutput = ExecuteResult;
        type Error = OrmClientError;

        fn execute_document_async<'a>(
            &'a mut self,
            document: &'a IrDocument,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::CommandOutput, Self::Error>> + 'a>,
        > {
            Box::pin(self.execute_orm(document))
        }

        fn query_document_async<'a>(
            &'a mut self,
            document: &'a IrDocument,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::QueryOutput, Self::Error>> + 'a>,
        > {
            Box::pin(self.execute_orm(document))
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> radixdb_orm::AsyncOrmRecordSession
        for AsyncConnection<S>
    {
        type Error = OrmClientError;

        fn mutate_record_async<'a>(
            &'a mut self,
            record: &'a mut radixdb_orm::DynamicRecord,
            mutation: radixdb_orm::RecordMutation,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>>
        {
            Box::pin(self.mutate_record_orm(record, mutation))
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> radixdb_orm::AsyncOrmGeneratedRecordSession
        for AsyncConnection<S>
    {
        type Error = OrmClientError;

        fn mutate_generated_record_async<'a, R: radixdb_orm::GeneratedRecord + 'a>(
            &'a mut self,
            record: &'a mut R,
            mutation: radixdb_orm::RecordMutation,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + 'a>>
        {
            Box::pin(self.mutate_generated_record_orm(record, mutation))
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> radixdb_orm::AsyncOrmGeneratedQuerySession
        for AsyncConnection<S>
    {
        type Error = OrmClientError;

        fn query_generated_records_async<'a, R: radixdb_orm::GeneratedRecord + Default + 'a>(
            &'a mut self,
            document: &'a IrDocument,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<R>, Self::Error>> + 'a>>
        {
            Box::pin(self.query_generated_records_orm(document))
        }
    }

    pub struct AsyncSchemaClient<'a, S> {
        connection: &'a mut AsyncConnection<S>,
    }

    impl<'a, S: AsyncRead + AsyncWrite + Unpin + Send> AsyncSchemaClient<'a, S> {
        pub fn tables(self) -> AsyncListTablesRequest<'a, S> {
            AsyncListTablesRequest {
                connection: self.connection,
            }
        }

        pub fn table(self, name: impl Into<String>) -> AsyncTableSchemaClient<'a, S> {
            AsyncTableSchemaClient {
                connection: self.connection,
                table: name.into(),
            }
        }

        pub fn describe_database(self) -> AsyncDescribeDatabaseRequest<'a, S> {
            AsyncDescribeDatabaseRequest {
                connection: self.connection,
            }
        }

        pub fn create_table(self, table: impl Into<String>) -> AsyncCreateTableRequest<'a, S> {
            let table = table.into();
            AsyncCreateTableRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::create_table(table.clone()),
                table,
            }
        }

        pub fn alter_table(self, table: impl Into<String>) -> AsyncAlterTableRequest<'a, S> {
            let table = table.into();
            AsyncAlterTableRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::alter_table(table.clone()),
                result_table: table,
            }
        }

        pub fn create_table_as(
            self,
            table: impl Into<String>,
            query: radixdb_orm::QueryBuilder,
        ) -> AsyncCreateTableAsRequest<'a, S> {
            let table = table.into();
            AsyncCreateTableAsRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::create_table_as(table.clone(), query),
                table,
            }
        }

        pub fn drop_table(self, table: impl Into<String>) -> AsyncDdlRequest<'a, S> {
            AsyncDdlRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::drop_table(table),
            }
        }

        pub fn truncate_table(self, table: impl Into<String>) -> AsyncDdlRequest<'a, S> {
            AsyncDdlRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::truncate_table(table),
            }
        }

        pub fn create_index(self, index: radixdb_orm::IndexDefinition) -> AsyncDdlRequest<'a, S> {
            AsyncDdlRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::create_index(index),
            }
        }

        pub fn drop_index(
            self,
            table: impl Into<String>,
            index: impl Into<String>,
            if_exists: bool,
        ) -> AsyncDdlRequest<'a, S> {
            AsyncDdlRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::drop_index(table, index, if_exists),
            }
        }

        pub fn alter_index(
            self,
            index: impl Into<String>,
            new_name: impl Into<String>,
        ) -> AsyncDdlRequest<'a, S> {
            AsyncDdlRequest {
                connection: self.connection,
                builder: radixdb_orm::DdlBuilder::alter_index(index, new_name),
            }
        }
    }

    pub struct AsyncCreateTableAsRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        builder: radixdb_orm::CreateTableAsBuilder,
        table: String,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncCreateTableAsRequest<'_, S> {
        pub fn if_not_exists(mut self, value: bool) -> Self {
            self.builder = self.builder.if_not_exists(value);
            self
        }
        pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
            radixdb_orm::OrmBuilder::to_json(&self.builder)
        }
        pub fn to_sql(
            &self,
        ) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
            radixdb_orm::OrmBuilder::to_sql(&self.builder)
        }
        pub async fn execute(self) -> Result<TableDescriptor, OrmClientError> {
            let document = radixdb_orm::OrmBuilder::document(&self.builder)?;
            expect_command(self.connection.execute_orm(&document).await?)?;
            describe_table(self.connection, self.table).await
        }
    }

    pub struct AsyncCreateTableRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        builder: radixdb_orm::CreateTableBuilder,
        table: String,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncCreateTableRequest<'_, S> {
        pub fn if_not_exists(mut self, value: bool) -> Self {
            self.builder = self.builder.if_not_exists(value);
            self
        }
        pub fn column(mut self, column: radixdb_orm::Column) -> Self {
            self.builder = self.builder.column(column);
            self
        }
        pub fn constraint(mut self, constraint: radixdb_orm::ConstraintDefinitionIr) -> Self {
            self.builder = self.builder.constraint(constraint);
            self
        }
        pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
            radixdb_orm::OrmBuilder::to_json(&self.builder)
        }
        pub fn to_sql(
            &self,
        ) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
            radixdb_orm::OrmBuilder::to_sql(&self.builder)
        }
        pub async fn execute(self) -> Result<TableDescriptor, OrmClientError> {
            let document = radixdb_orm::OrmBuilder::document(&self.builder)
                .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
            expect_command(self.connection.execute_orm(&document).await?)?;
            describe_table(self.connection, self.table).await
        }
    }

    pub struct AsyncAlterTableRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        builder: radixdb_orm::AlterTableBuilder,
        result_table: String,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncAlterTableRequest<'_, S> {
        pub fn add_column(mut self, column: radixdb_orm::Column) -> Self {
            self.builder = self.builder.add_column(column);
            self
        }
        pub fn modify_column(mut self, column: radixdb_orm::Column) -> Self {
            self.builder = self.builder.modify_column(column);
            self
        }
        pub fn drop_column(mut self, column: impl Into<String>) -> Self {
            self.builder = self.builder.drop_column(column);
            self
        }
        pub fn rename_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
            self.builder = self.builder.rename_column(from, to);
            self
        }
        pub fn rename_table(mut self, to: impl Into<String>) -> Self {
            let to = to.into();
            self.result_table = to.clone();
            self.builder = self.builder.rename_table(to);
            self
        }
        pub fn add_constraint(mut self, constraint: radixdb_orm::ConstraintDefinitionIr) -> Self {
            self.builder = self.builder.add_constraint(constraint);
            self
        }
        pub fn drop_constraint(mut self, name: impl Into<String>, if_exists: bool) -> Self {
            self.builder = self.builder.drop_constraint(name, if_exists);
            self
        }
        pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
            radixdb_orm::OrmBuilder::to_json(&self.builder)
        }
        pub fn to_sql(
            &self,
        ) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
            radixdb_orm::OrmBuilder::to_sql(&self.builder)
        }
        pub async fn execute(self) -> Result<TableDescriptor, OrmClientError> {
            let document = radixdb_orm::OrmBuilder::document(&self.builder)
                .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
            expect_command(self.connection.execute_orm(&document).await?)?;
            describe_table(self.connection, self.result_table).await
        }
    }

    pub struct AsyncDdlRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        builder: radixdb_orm::DdlBuilder,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncDdlRequest<'_, S> {
        pub fn if_exists(mut self, value: bool) -> Self {
            self.builder = self.builder.if_exists(value);
            self
        }
        pub fn to_json(&self) -> Result<String, radixdb_orm::BuilderJsonError> {
            radixdb_orm::OrmBuilder::to_json(&self.builder)
        }
        pub fn to_sql(
            &self,
        ) -> Result<radixdb_orm::CompiledStatement, radixdb_orm::BuilderSqlError> {
            radixdb_orm::OrmBuilder::to_sql(&self.builder)
        }
        pub async fn execute(self) -> Result<u64, OrmClientError> {
            let document = radixdb_orm::OrmBuilder::document(&self.builder)
                .map_err(|error| OrmClientError::InvalidValue(error.to_string()))?;
            expect_command(self.connection.execute_orm(&document).await?)
        }
    }

    fn expect_command(result: ExecuteResult) -> Result<u64, OrmClientError> {
        match result {
            ExecuteResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
            ExecuteResult::Cursor(_) => Err(OrmClientError::UnexpectedResult(
                "DDL expected command completion",
            )),
        }
    }

    async fn describe_table<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &mut AsyncConnection<S>,
        table: String,
    ) -> Result<TableDescriptor, OrmClientError> {
        let document = IrDocument::new(radixdb_orm::Operation::Catalog {
            operation: CatalogOperation::DescribeTable { table },
        });
        let result = connection.execute_orm(&document).await?;
        let json = fetch_one_text(connection, result).await?;
        Ok(DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?.payload)
    }

    async fn fetch_one_typed_record<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &mut AsyncConnection<S>,
        result: ExecuteResult,
        descriptor: &TableDescriptor,
    ) -> Result<BTreeMap<String, TypedValue>, OrmClientError> {
        let ExecuteResult::Cursor(cursor) = result else {
            return Err(OrmClientError::UnexpectedResult(
                "RETURNING expected a cursor",
            ));
        };
        let mut rows = Vec::new();
        loop {
            let batch = connection.fetch(&cursor).await?;
            rows.extend(batch.rows);
            if batch.eof {
                break;
            }
        }
        let [row] = rows.as_slice() else {
            if rows.is_empty() {
                return Err(OrmClientError::NotFound);
            }
            return Err(OrmClientError::UnexpectedResult(
                "RETURNING produced multiple rows",
            ));
        };
        if row.values.len() != descriptor.columns.len() {
            return Err(OrmClientError::UnexpectedResult(
                "RETURNING row width differs from descriptor",
            ));
        }
        descriptor
            .columns
            .iter()
            .zip(&row.values)
            .map(|(column, value)| {
                Ok((
                    column.name.clone(),
                    wire_value_to_typed(value, &column.data_type)?,
                ))
            })
            .collect()
    }

    pub struct AsyncListTablesRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncListTablesRequest<'_, S> {
        pub async fn fetch(self) -> Result<Vec<String>, OrmClientError> {
            let document = IrDocument::new(radixdb_orm::Operation::Catalog {
                operation: CatalogOperation::ListTables,
            });
            let result = self.connection.execute_orm(&document).await?;
            fetch_text_column(self.connection, result).await
        }
    }

    pub struct AsyncTableSchemaClient<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        table: String,
    }

    impl<'a, S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTableSchemaClient<'a, S> {
        pub fn describe(self) -> AsyncDescribeTableRequest<'a, S> {
            AsyncDescribeTableRequest {
                connection: self.connection,
                table: self.table,
            }
        }

        pub fn columns(self) -> AsyncTableColumnsRequest<'a, S> {
            AsyncTableColumnsRequest {
                request: self.describe(),
            }
        }

        pub fn indexes(self) -> AsyncTableIndexesRequest<'a, S> {
            AsyncTableIndexesRequest {
                request: self.describe(),
            }
        }

        pub fn constraints(self) -> AsyncTableConstraintsRequest<'a, S> {
            AsyncTableConstraintsRequest {
                request: self.describe(),
            }
        }
    }

    pub struct AsyncDescribeTableRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
        table: String,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncDescribeTableRequest<'_, S> {
        pub async fn fetch(self) -> Result<TableDescriptor, OrmClientError> {
            let document = IrDocument::new(radixdb_orm::Operation::Catalog {
                operation: CatalogOperation::DescribeTable { table: self.table },
            });
            let result = self.connection.execute_orm(&document).await?;
            let json = fetch_one_text(self.connection, result).await?;
            Ok(
                DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?
                    .payload,
            )
        }
    }

    pub struct AsyncTableColumnsRequest<'a, S> {
        request: AsyncDescribeTableRequest<'a, S>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTableColumnsRequest<'_, S> {
        pub async fn fetch(self) -> Result<Vec<ColumnDescriptor>, OrmClientError> {
            Ok(self.request.fetch().await?.columns)
        }
    }

    pub struct AsyncTableIndexesRequest<'a, S> {
        request: AsyncDescribeTableRequest<'a, S>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTableIndexesRequest<'_, S> {
        pub async fn fetch(self) -> Result<Vec<IndexDescriptor>, OrmClientError> {
            Ok(self.request.fetch().await?.indexes)
        }
    }

    pub struct AsyncTableConstraintsRequest<'a, S> {
        request: AsyncDescribeTableRequest<'a, S>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTableConstraintsRequest<'_, S> {
        pub async fn fetch(self) -> Result<Vec<ConstraintDescriptor>, OrmClientError> {
            Ok(self.request.fetch().await?.constraints)
        }
    }

    pub struct AsyncDescribeDatabaseRequest<'a, S> {
        connection: &'a mut AsyncConnection<S>,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncDescribeDatabaseRequest<'_, S> {
        pub async fn fetch(self) -> Result<DatabaseDescriptor, OrmClientError> {
            let document = IrDocument::new(radixdb_orm::Operation::Catalog {
                operation: CatalogOperation::DescribeDatabase,
            });
            let result = self.connection.execute_orm(&document).await?;
            let json = fetch_one_text(self.connection, result).await?;
            Ok(DescriptorEnvelope::<DatabaseDescriptor>::from_json(
                &json,
                DescriptorKind::Database,
            )?
            .payload)
        }
    }

    async fn fetch_one_text<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &mut AsyncConnection<S>,
        result: ExecuteResult,
    ) -> Result<String, OrmClientError> {
        let values = fetch_text_column(connection, result).await?;
        match values.as_slice() {
            [value] => Ok(value.clone()),
            _ => Err(OrmClientError::UnexpectedResult(
                "expected exactly one text row",
            )),
        }
    }

    async fn fetch_text_column<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &mut AsyncConnection<S>,
        result: ExecuteResult,
    ) -> Result<Vec<String>, OrmClientError> {
        let ExecuteResult::Cursor(cursor) = result else {
            return Err(OrmClientError::UnexpectedResult("expected a cursor"));
        };
        let mut values = Vec::new();
        loop {
            let batch = connection.fetch(&cursor).await?;
            for Row { values: row } in batch.rows {
                let [WireValue::String(value)] = row.as_slice() else {
                    return Err(OrmClientError::UnexpectedResult("expected one text column"));
                };
                values.push(value.clone());
            }
            if batch.eof {
                return Ok(values);
            }
        }
    }
}

#[cfg(feature = "tokio")]
pub use asynchronous::*;

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_orm::{FloatSpecial, FloatValue};

    #[test]
    fn typed_values_convert_without_loss_or_sql_text() {
        assert!(matches!(
            typed_value_to_wire(&TypedValue::Float(FloatValue::Special(FloatSpecial::Nan)))
                .unwrap(),
            WireValue::Float64(value) if value.is_nan()
        ));
        assert_eq!(
            typed_value_to_wire(&TypedValue::Decimal("-123.40".to_string())).unwrap(),
            WireValue::Decimal {
                unscaled: -12340,
                precision: 5,
                scale: 2,
            }
        );
        assert!(typed_value_to_wire(&TypedValue::Decimal("1.2.3".to_string())).is_err());
        assert!(typed_value_to_wire(&TypedValue::Uuid("not-a-uuid".to_string())).is_err());
        let timestamp = typed_value_to_wire(&TypedValue::Timestamp(
            "2026-08-21T12:34:56.123456789Z".to_string(),
        ))
        .unwrap();
        assert!(matches!(timestamp, WireValue::TimestampNanos { .. }));
    }
}
