//! Embedded ORM execution extensions.
//!
//! The facade borrows the existing [`Database`] or [`Transaction`]. It never
//! creates a second executor, transaction, connection, or catalog snapshot.

use std::collections::BTreeMap;

use base64::Engine as _;
use chrono::{DateTime, NaiveDate, Utc};
use radixdb_orm::{
    CatalogOperation, ColumnDescriptor, ConstraintDescriptor, DataTypeDescriptor,
    DatabaseDescriptor, DescriptorEnvelope, DescriptorKind, IndexDescriptor, IrDocument,
    TableDescriptor, TypedValue,
};

use crate::{Database, FromValue, Rows, Transaction};
use radixdb_core::{DataType, Error, Value};

#[derive(Debug, thiserror::Error)]
pub enum OrmError {
    #[error(transparent)]
    Database(#[from] Error),
    #[error(transparent)]
    Render(#[from] radixdb_orm::RenderError),
    #[error(transparent)]
    Descriptor(#[from] radixdb_orm::DescriptorError),
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

pub type OrmResult<T> = std::result::Result<T, OrmError>;

impl Database {
    /// Compile and execute command-like ORM IR on this exact embedded session.
    pub fn execute_orm(&self, document: &IrDocument) -> OrmResult<i64> {
        let compiled = document.to_sql()?;
        Ok(self.execute(&compiled.sql, typed_values_to_core(&compiled.parameters)?)?)
    }

    /// Compile and execute row-producing ORM IR on this exact embedded session.
    pub fn query_orm(&self, document: &IrDocument) -> OrmResult<Rows> {
        let compiled = document.to_sql()?;
        Ok(self.query(&compiled.sql, typed_values_to_core(&compiled.parameters)?)?)
    }

    /// Borrow this same embedded session for catalog operations.
    pub fn schema(&self) -> EmbeddedSchemaClient<'_> {
        EmbeddedSchemaClient {
            session: EmbeddedSession::Database(self),
        }
    }

    pub fn entity(&self, table: impl Into<String>) -> OrmResult<radixdb_orm::DynamicEntity> {
        Ok(radixdb_orm::DynamicEntity::new(
            self.schema().table(table).describe().fetch()?,
        ))
    }
}

impl Transaction {
    /// Compile and execute command-like ORM IR inside this transaction.
    pub fn execute_orm(&mut self, document: &IrDocument) -> OrmResult<i64> {
        let compiled = document.to_sql()?;
        Ok(self.execute(&compiled.sql, typed_values_to_core(&compiled.parameters)?)?)
    }

    /// Compile and execute row-producing ORM IR inside this transaction.
    pub fn query_orm(&mut self, document: &IrDocument) -> OrmResult<Rows> {
        let compiled = document.to_sql()?;
        Ok(self.query(&compiled.sql, typed_values_to_core(&compiled.parameters)?)?)
    }

    /// Borrow this transaction for catalog operations without leaving it.
    pub fn schema(&mut self) -> EmbeddedSchemaClient<'_> {
        EmbeddedSchemaClient {
            session: EmbeddedSession::Transaction(self),
        }
    }

    pub fn entity(&mut self, table: impl Into<String>) -> OrmResult<radixdb_orm::DynamicEntity> {
        Ok(radixdb_orm::DynamicEntity::new(
            self.schema().table(table).describe().fetch()?,
        ))
    }
}

enum EmbeddedSession<'a> {
    Database(&'a Database),
    Transaction(&'a mut Transaction),
}

impl EmbeddedSession<'_> {
    fn execute_orm(&mut self, document: &IrDocument) -> OrmResult<i64> {
        match self {
            Self::Database(database) => database.execute_orm(document),
            Self::Transaction(transaction) => transaction.execute_orm(document),
        }
    }

    fn query_orm(&mut self, document: &IrDocument) -> OrmResult<Rows> {
        match self {
            Self::Database(database) => database.query_orm(document),
            Self::Transaction(transaction) => transaction.query_orm(document),
        }
    }
}

pub struct EmbeddedSchemaClient<'a> {
    session: EmbeddedSession<'a>,
}

impl<'a> EmbeddedSchemaClient<'a> {
    pub fn tables(self) -> EmbeddedListTablesRequest<'a> {
        EmbeddedListTablesRequest {
            session: self.session,
        }
    }

    pub fn table(self, name: impl Into<String>) -> EmbeddedTableSchemaClient<'a> {
        EmbeddedTableSchemaClient {
            session: self.session,
            table: name.into(),
        }
    }

    pub fn describe_database(self) -> EmbeddedDescribeDatabaseRequest<'a> {
        EmbeddedDescribeDatabaseRequest {
            session: self.session,
        }
    }

    pub fn create_table(self, table: impl Into<String>) -> EmbeddedCreateTableRequest<'a> {
        let table = table.into();
        EmbeddedCreateTableRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::create_table(table.clone()),
            table,
        }
    }

    pub fn alter_table(self, table: impl Into<String>) -> EmbeddedAlterTableRequest<'a> {
        let table = table.into();
        EmbeddedAlterTableRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::alter_table(table.clone()),
            result_table: table,
        }
    }

    pub fn create_table_as(
        self,
        table: impl Into<String>,
        query: radixdb_orm::QueryBuilder,
    ) -> EmbeddedCreateTableAsRequest<'a> {
        let table = table.into();
        EmbeddedCreateTableAsRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::create_table_as(table.clone(), query),
            table,
        }
    }

    pub fn drop_table(self, table: impl Into<String>) -> EmbeddedDdlRequest<'a> {
        EmbeddedDdlRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::drop_table(table),
        }
    }

    pub fn truncate_table(self, table: impl Into<String>) -> EmbeddedDdlRequest<'a> {
        EmbeddedDdlRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::truncate_table(table),
        }
    }

    pub fn create_index(self, index: radixdb_orm::IndexDefinition) -> EmbeddedDdlRequest<'a> {
        EmbeddedDdlRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::create_index(index),
        }
    }

    pub fn drop_index(
        self,
        table: impl Into<String>,
        index: impl Into<String>,
        if_exists: bool,
    ) -> EmbeddedDdlRequest<'a> {
        EmbeddedDdlRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::drop_index(table, index, if_exists),
        }
    }

    pub fn alter_index(
        self,
        index: impl Into<String>,
        new_name: impl Into<String>,
    ) -> EmbeddedDdlRequest<'a> {
        EmbeddedDdlRequest {
            session: self.session,
            builder: radixdb_orm::DdlBuilder::alter_index(index, new_name),
        }
    }
}

pub struct EmbeddedCreateTableAsRequest<'a> {
    session: EmbeddedSession<'a>,
    builder: radixdb_orm::CreateTableAsBuilder,
    table: String,
}

impl EmbeddedCreateTableAsRequest<'_> {
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
    pub fn execute(mut self) -> OrmResult<TableDescriptor> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmError::InvalidValue(error.to_string()))?;
        self.session.execute_orm(&document)?;
        describe_table_on_session(&mut self.session, self.table)
    }
}

pub struct EmbeddedCreateTableRequest<'a> {
    session: EmbeddedSession<'a>,
    builder: radixdb_orm::CreateTableBuilder,
    table: String,
}

impl EmbeddedCreateTableRequest<'_> {
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

    pub fn execute(mut self) -> OrmResult<TableDescriptor> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmError::InvalidValue(error.to_string()))?;
        self.session.execute_orm(&document)?;
        describe_table_on_session(&mut self.session, self.table)
    }
}

pub struct EmbeddedAlterTableRequest<'a> {
    session: EmbeddedSession<'a>,
    builder: radixdb_orm::AlterTableBuilder,
    result_table: String,
}

impl EmbeddedAlterTableRequest<'_> {
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
    pub fn execute(mut self) -> OrmResult<TableDescriptor> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmError::InvalidValue(error.to_string()))?;
        self.session.execute_orm(&document)?;
        describe_table_on_session(&mut self.session, self.result_table)
    }
}

pub struct EmbeddedDdlRequest<'a> {
    session: EmbeddedSession<'a>,
    builder: radixdb_orm::DdlBuilder,
}

impl EmbeddedDdlRequest<'_> {
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
    pub fn execute(mut self) -> OrmResult<i64> {
        let document = radixdb_orm::OrmBuilder::document(&self.builder)
            .map_err(|error| OrmError::InvalidValue(error.to_string()))?;
        self.session.execute_orm(&document)
    }
}

fn describe_table_on_session(
    session: &mut EmbeddedSession<'_>,
    table: String,
) -> OrmResult<TableDescriptor> {
    let document = catalog_document(CatalogOperation::DescribeTable { table });
    let json = fetch_one_text(session.query_orm(&document)?)?;
    Ok(DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?.payload)
}

pub struct EmbeddedListTablesRequest<'a> {
    session: EmbeddedSession<'a>,
}

impl EmbeddedListTablesRequest<'_> {
    pub fn fetch(mut self) -> OrmResult<Vec<String>> {
        let document = catalog_document(CatalogOperation::ListTables);
        fetch_single_text_column(self.session.query_orm(&document)?)
    }
}

pub struct EmbeddedTableSchemaClient<'a> {
    session: EmbeddedSession<'a>,
    table: String,
}

impl<'a> EmbeddedTableSchemaClient<'a> {
    pub fn describe(self) -> EmbeddedDescribeTableRequest<'a> {
        EmbeddedDescribeTableRequest {
            session: self.session,
            table: self.table,
        }
    }

    pub fn columns(self) -> EmbeddedTableColumnsRequest<'a> {
        EmbeddedTableColumnsRequest {
            request: self.describe(),
        }
    }

    pub fn indexes(self) -> EmbeddedTableIndexesRequest<'a> {
        EmbeddedTableIndexesRequest {
            request: self.describe(),
        }
    }

    pub fn constraints(self) -> EmbeddedTableConstraintsRequest<'a> {
        EmbeddedTableConstraintsRequest {
            request: self.describe(),
        }
    }
}

pub struct EmbeddedDescribeTableRequest<'a> {
    session: EmbeddedSession<'a>,
    table: String,
}

impl EmbeddedDescribeTableRequest<'_> {
    pub fn fetch(mut self) -> OrmResult<TableDescriptor> {
        let document = catalog_document(CatalogOperation::DescribeTable { table: self.table });
        let json = fetch_one_text(self.session.query_orm(&document)?)?;
        Ok(DescriptorEnvelope::<TableDescriptor>::from_json(&json, DescriptorKind::Table)?.payload)
    }
}

pub struct EmbeddedTableColumnsRequest<'a> {
    request: EmbeddedDescribeTableRequest<'a>,
}

impl EmbeddedTableColumnsRequest<'_> {
    pub fn fetch(self) -> OrmResult<Vec<ColumnDescriptor>> {
        Ok(self.request.fetch()?.columns)
    }
}

pub struct EmbeddedTableIndexesRequest<'a> {
    request: EmbeddedDescribeTableRequest<'a>,
}

impl EmbeddedTableIndexesRequest<'_> {
    pub fn fetch(self) -> OrmResult<Vec<IndexDescriptor>> {
        Ok(self.request.fetch()?.indexes)
    }
}

pub struct EmbeddedTableConstraintsRequest<'a> {
    request: EmbeddedDescribeTableRequest<'a>,
}

impl EmbeddedTableConstraintsRequest<'_> {
    pub fn fetch(self) -> OrmResult<Vec<ConstraintDescriptor>> {
        Ok(self.request.fetch()?.constraints)
    }
}

pub struct EmbeddedDescribeDatabaseRequest<'a> {
    session: EmbeddedSession<'a>,
}

impl EmbeddedDescribeDatabaseRequest<'_> {
    pub fn fetch(mut self) -> OrmResult<DatabaseDescriptor> {
        let document = catalog_document(CatalogOperation::DescribeDatabase);
        let json = fetch_one_text(self.session.query_orm(&document)?)?;
        Ok(
            DescriptorEnvelope::<DatabaseDescriptor>::from_json(&json, DescriptorKind::Database)?
                .payload,
        )
    }
}

fn catalog_document(operation: CatalogOperation) -> IrDocument {
    IrDocument::new(radixdb_orm::Operation::Catalog { operation })
}

fn fetch_one_text(rows: Rows) -> OrmResult<String> {
    let values = fetch_single_text_column(rows)?;
    match values.as_slice() {
        [value] => Ok(value.clone()),
        _ => Err(OrmError::UnexpectedResult("expected exactly one text row")),
    }
}

fn fetch_single_text_column(rows: Rows) -> OrmResult<Vec<String>> {
    rows.map(|row| {
        let row = row?;
        String::from_value(
            row.get_value(0)
                .ok_or_else(|| Error::invalid_argument("expected one text result column"))?,
        )
        .map_err(OrmError::from)
    })
    .collect()
}

pub(crate) fn typed_values_to_core(values: &[TypedValue]) -> OrmResult<Vec<Value>> {
    values.iter().map(typed_value_to_core).collect()
}

fn typed_value_to_core(value: &TypedValue) -> OrmResult<Value> {
    Ok(match value {
        TypedValue::Null(data_type) => Value::null(descriptor_data_type(data_type)),
        TypedValue::Integer(value) => Value::integer(*value),
        TypedValue::Float(value) => Value::float(value.as_f64()),
        TypedValue::Text(value) => Value::text(value.clone()),
        TypedValue::Boolean(value) => Value::boolean(*value),
        TypedValue::Timestamp(value) => {
            let timestamp = DateTime::parse_from_rfc3339(value).map_err(|error| {
                OrmError::InvalidValue(format!("invalid RFC3339 timestamp: {error}"))
            })?;
            Value::timestamp(timestamp.with_timezone(&Utc))
        }
        TypedValue::Date(value) => {
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|error| OrmError::InvalidValue(format!("invalid ISO date: {error}")))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid Unix epoch");
            let days =
                i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| {
                    OrmError::InvalidValue("date is outside i32 day domain".to_string())
                })?;
            Value::date(days)
        }
        TypedValue::Json(value) => Value::try_json(
            serde_json::to_string(value)
                .map_err(|error| OrmError::InvalidValue(format!("invalid JSON value: {error}")))?,
        )?,
        TypedValue::Uuid(value) => Value::uuid(
            *uuid::Uuid::parse_str(value)
                .map_err(|error| OrmError::InvalidValue(format!("invalid UUID: {error}")))?
                .as_bytes(),
        ),
        TypedValue::Bytes(value) => Value::bytes(
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|error| {
                    OrmError::InvalidValue(format!("invalid base64 BYTES: {error}"))
                })?,
        ),
        TypedValue::Decimal(value) => {
            let (unscaled, precision, scale) = radixdb_orm::parse_decimal_literal(value)
                .map_err(|error| OrmError::InvalidValue(error.to_string()))?;
            Value::try_decimal(unscaled, precision, scale)?
        }
        TypedValue::Vector(values) => Value::vector(values.clone()),
    })
}

fn descriptor_data_type(data_type: &DataTypeDescriptor) -> DataType {
    match data_type {
        DataTypeDescriptor::Null => DataType::Null,
        DataTypeDescriptor::Integer => DataType::Integer,
        DataTypeDescriptor::Float => DataType::Float,
        DataTypeDescriptor::Text => DataType::Text,
        DataTypeDescriptor::Boolean => DataType::Boolean,
        DataTypeDescriptor::Timestamp => DataType::Timestamp,
        DataTypeDescriptor::Date => DataType::Date,
        DataTypeDescriptor::Json => DataType::Json,
        DataTypeDescriptor::Uuid => DataType::Uuid,
        DataTypeDescriptor::Bytes => DataType::Bytes,
        DataTypeDescriptor::Decimal { .. } => DataType::Decimal,
        DataTypeDescriptor::Vector { .. } => DataType::Vector,
    }
}

impl radixdb_orm::OrmSession for &Database {
    type CommandOutput = i64;
    type QueryOutput = Rows;
    type Error = OrmError;

    fn execute_document(self, document: &IrDocument) -> OrmResult<Self::CommandOutput> {
        self.execute_orm(document)
    }

    fn query_document(self, document: &IrDocument) -> OrmResult<Self::QueryOutput> {
        self.query_orm(document)
    }
}

impl radixdb_orm::OrmSession for &mut Transaction {
    type CommandOutput = i64;
    type QueryOutput = Rows;
    type Error = OrmError;

    fn execute_document(self, document: &IrDocument) -> OrmResult<Self::CommandOutput> {
        self.execute_orm(document)
    }

    fn query_document(self, document: &IrDocument) -> OrmResult<Self::QueryOutput> {
        self.query_orm(document)
    }
}

impl radixdb_orm::OrmRecordSession for &Database {
    type Error = OrmError;

    fn mutate_record(
        self,
        record: &mut radixdb_orm::DynamicRecord,
        mutation: radixdb_orm::RecordMutation,
    ) -> OrmResult<()> {
        if mutation == radixdb_orm::RecordMutation::Delete {
            let document = record.delete_document()?;
            if self.execute_orm(&document)? == 0 {
                return Err(OrmError::NotFound);
            }
            return Ok(());
        }
        let document = record_mutation_document(record, mutation)?;
        let values = one_embedded_record(self.query_orm(&document)?, record.descriptor())?;
        record.apply_returning(values)?;
        Ok(())
    }
}

impl radixdb_orm::OrmRecordSession for &mut Transaction {
    type Error = OrmError;

    fn mutate_record(
        self,
        record: &mut radixdb_orm::DynamicRecord,
        mutation: radixdb_orm::RecordMutation,
    ) -> OrmResult<()> {
        if mutation == radixdb_orm::RecordMutation::Delete {
            let document = record.delete_document()?;
            if self.execute_orm(&document)? == 0 {
                return Err(OrmError::NotFound);
            }
            return Ok(());
        }
        let document = record_mutation_document(record, mutation)?;
        let values = one_embedded_record(self.query_orm(&document)?, record.descriptor())?;
        record.apply_returning(values)?;
        Ok(())
    }
}

impl radixdb_orm::OrmGeneratedRecordSession for &Database {
    type Error = OrmError;

    fn mutate_generated_record<R: radixdb_orm::GeneratedRecord>(
        self,
        record: &mut R,
        mutation: radixdb_orm::RecordMutation,
    ) -> OrmResult<()> {
        let descriptor = self
            .schema()
            .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
            .describe()
            .fetch()?;
        let mut dynamic = record.to_dynamic(&descriptor)?;
        radixdb_orm::OrmRecordSession::mutate_record(self, &mut dynamic, mutation)?;
        record.apply_dynamic(&dynamic)?;
        Ok(())
    }
}

impl radixdb_orm::OrmGeneratedRecordSession for &mut Transaction {
    type Error = OrmError;

    fn mutate_generated_record<R: radixdb_orm::GeneratedRecord>(
        self,
        record: &mut R,
        mutation: radixdb_orm::RecordMutation,
    ) -> OrmResult<()> {
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

impl radixdb_orm::OrmGeneratedQuerySession for &Database {
    type Error = OrmError;

    fn query_generated_records<R: radixdb_orm::GeneratedRecord + Default>(
        self,
        document: &IrDocument,
    ) -> OrmResult<Vec<R>> {
        let descriptor = self
            .schema()
            .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
            .describe()
            .fetch()?;
        generated_records_from_embedded_rows::<R>(self.query_orm(document)?, descriptor)
    }
}

impl radixdb_orm::OrmGeneratedQuerySession for &mut Transaction {
    type Error = OrmError;

    fn query_generated_records<R: radixdb_orm::GeneratedRecord + Default>(
        self,
        document: &IrDocument,
    ) -> OrmResult<Vec<R>> {
        let descriptor = self
            .schema()
            .table(<R::Entity as radixdb_orm::GeneratedEntity>::TABLE)
            .describe()
            .fetch()?;
        generated_records_from_embedded_rows::<R>(self.query_orm(document)?, descriptor)
    }
}

fn generated_records_from_embedded_rows<R: radixdb_orm::GeneratedRecord + Default>(
    rows: Rows,
    descriptor: TableDescriptor,
) -> OrmResult<Vec<R>> {
    radixdb_orm::ensure_schema_fingerprint(
        <R::Entity as radixdb_orm::GeneratedEntity>::SCHEMA_FINGERPRINT,
        &descriptor.fingerprint,
    )
    .map_err(radixdb_orm::GeneratedRecordError::from)?;
    rows.map(|row| {
        let row = row?;
        if row.len() != descriptor.columns.len() {
            return Err(OrmError::UnexpectedResult(
                "generated SELECT row width differs from descriptor",
            ));
        }
        let values = descriptor
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let value = row.get_value(index).ok_or(OrmError::UnexpectedResult(
                    "generated SELECT row width differs from descriptor",
                ))?;
                Ok((
                    column.name.clone(),
                    core_value_to_typed(value, &column.data_type)?,
                ))
            })
            .collect::<OrmResult<BTreeMap<_, _>>>()?;
        let mut dynamic = radixdb_orm::DynamicRecord::new(descriptor.clone());
        dynamic.apply_returning(values)?;
        let mut generated = R::default();
        generated.apply_dynamic(&dynamic)?;
        Ok(generated)
    })
    .collect()
}

fn record_mutation_document(
    record: &radixdb_orm::DynamicRecord,
    mutation: radixdb_orm::RecordMutation,
) -> OrmResult<IrDocument> {
    Ok(match mutation {
        radixdb_orm::RecordMutation::Insert => record.insert_document()?,
        radixdb_orm::RecordMutation::Save => record.save_document()?,
        radixdb_orm::RecordMutation::Update => record.update_document()?,
        radixdb_orm::RecordMutation::Delete => record.delete_document()?,
    })
}

fn one_embedded_record(
    mut rows: Rows,
    descriptor: &TableDescriptor,
) -> OrmResult<BTreeMap<String, TypedValue>> {
    let row = rows.next().ok_or(OrmError::NotFound)??;
    if rows.next().is_some() {
        return Err(OrmError::UnexpectedResult(
            "RETURNING produced multiple rows",
        ));
    }
    descriptor
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = row.get_value(index).ok_or(OrmError::UnexpectedResult(
                "RETURNING row width differs from descriptor",
            ))?;
            Ok((
                column.name.clone(),
                core_value_to_typed(value, &column.data_type)?,
            ))
        })
        .collect()
}

fn core_value_to_typed(value: &Value, declared: &DataTypeDescriptor) -> OrmResult<TypedValue> {
    if value.is_null() {
        return Ok(TypedValue::Null(declared.clone()));
    }
    Ok(match value {
        Value::Integer(value) => TypedValue::Integer(*value),
        Value::Float(value) => TypedValue::Float((*value).into()),
        Value::Text(value) => TypedValue::Text(value.to_string()),
        Value::Boolean(value) => TypedValue::Boolean(*value),
        Value::Timestamp(value) => {
            TypedValue::Timestamp(value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Value::Extension(_) => match declared {
            DataTypeDescriptor::Json => TypedValue::Json(
                serde_json::from_str(value.as_json().ok_or_else(|| {
                    OrmError::InvalidValue("invalid JSON result payload".to_string())
                })?)
                .map_err(|error| OrmError::InvalidValue(error.to_string()))?,
            ),
            DataTypeDescriptor::Uuid => TypedValue::Uuid(
                uuid::Uuid::from_bytes(value.as_uuid_bytes().ok_or_else(|| {
                    OrmError::InvalidValue("invalid UUID result payload".to_string())
                })?)
                .to_string(),
            ),
            DataTypeDescriptor::Decimal { .. } => {
                let (unscaled, _, scale) = value.as_decimal_parts().ok_or_else(|| {
                    OrmError::InvalidValue("invalid DECIMAL result payload".to_string())
                })?;
                TypedValue::Decimal(radixdb_core::value::format_decimal_parts(unscaled, scale))
            }
            DataTypeDescriptor::Date => {
                let days = value.as_date_days().ok_or_else(|| {
                    OrmError::InvalidValue("invalid DATE result payload".to_string())
                })?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
                TypedValue::Date(
                    (epoch + chrono::Duration::days(i64::from(days)))
                        .format("%Y-%m-%d")
                        .to_string(),
                )
            }
            DataTypeDescriptor::Bytes => {
                TypedValue::Bytes(base64::engine::general_purpose::STANDARD.encode(
                    value.as_bytes_value().ok_or_else(|| {
                        OrmError::InvalidValue("invalid BYTES result payload".to_string())
                    })?,
                ))
            }
            DataTypeDescriptor::Vector { .. } => {
                TypedValue::Vector(value.as_vector_f32().ok_or_else(|| {
                    OrmError::InvalidValue("invalid VECTOR result payload".to_string())
                })?)
            }
            other => {
                return Err(OrmError::InvalidValue(format!(
                    "extension result does not match {other:?}"
                )))
            }
        },
        Value::Null(_) => unreachable!("handled above"),
    })
}

#[cfg(test)]
mod tests {
    use radixdb_orm::FieldValue;
    use radixdb_orm::{
        BinaryOperator, Expression, Insert, Operation, Projection, Relation, Select, TypedValue,
    };

    use super::*;

    fn users_select() -> IrDocument {
        IrDocument::new(Operation::Select {
            query: Select {
                projection: vec![Projection {
                    expression: Expression::column("name"),
                    alias: None,
                }],
                from: Some(Relation::Table {
                    name: "orm_users".to_string(),
                    alias: None,
                }),
                filter: Some(Expression::Binary {
                    left: Box::new(Expression::column("id")),
                    operator: BinaryOperator::Eq,
                    right: Box::new(Expression::literal(TypedValue::Integer(1))),
                }),
                ..Select::default()
            },
        })
    }

    #[test]
    fn raw_orm_raw_share_one_embedded_transaction() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE orm_users (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .unwrap();

        let mut transaction = db.begin().unwrap();
        transaction
            .execute("INSERT INTO orm_users VALUES (1, 'raw-before')", ())
            .unwrap();
        let selected: String = transaction
            .query_orm(&users_select())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(selected, "raw-before");

        let insert = IrDocument::new(Operation::Insert {
            statement: Insert {
                table: "orm_users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![vec![
                    Expression::literal(TypedValue::Integer(2)),
                    Expression::literal(TypedValue::Text("orm-middle".to_string())),
                ]],
                source: None,
                returning: Vec::new(),
            },
        });
        assert_eq!(transaction.execute_orm(&insert).unwrap(), 1);
        assert_eq!(
            transaction
                .query_one::<i64, _>("SELECT COUNT(*) FROM orm_users", ())
                .unwrap(),
            2
        );
        assert_eq!(
            transaction.schema().tables().fetch().unwrap(),
            vec!["orm_users"]
        );
        transaction.rollback().unwrap();

        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM orm_users", ())
                .unwrap(),
            0
        );
    }

    #[test]
    fn dynamic_record_crud_hydrates_only_after_server_success() {
        let db = Database::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE orm_records (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'server')",
            (),
        )
        .unwrap();
        let descriptor = db.schema().table("orm_records").describe().fetch().unwrap();
        let mut record = radixdb_orm::DynamicRecord::new(descriptor);
        record.set("id", TypedValue::Integer(7)).unwrap();
        record.insert(&db).unwrap();
        assert!(!record.is_dirty());
        assert!(matches!(
            record.field("name").unwrap().value(),
            FieldValue::Value { value: TypedValue::Text(value) } if value == "server"
        ));

        record
            .set("name", TypedValue::Text("updated".to_string()))
            .unwrap();
        record.update(&db).unwrap();
        assert!(!record.is_dirty());
        assert_eq!(
            db.query_one::<String, _>("SELECT name FROM orm_records WHERE id = 7", ())
                .unwrap(),
            "updated"
        );

        record
            .set("name", TypedValue::Text("saved".to_string()))
            .unwrap();
        record.save(&db).unwrap();
        assert!(!record.is_dirty());
        record.delete(&db).unwrap();
        assert!(matches!(record.delete(&db), Err(OrmError::NotFound)));

        record
            .set("name", TypedValue::Text("missing-row".to_string()))
            .unwrap();
        assert!(matches!(record.update(&db), Err(OrmError::NotFound)));
        assert!(record.is_dirty(), "zero-row UPDATE must retain dirty state");

        let descriptor = db.schema().table("orm_records").describe().fetch().unwrap();
        let mut duplicate = radixdb_orm::DynamicRecord::new(descriptor);
        duplicate.set("id", TypedValue::Integer(8)).unwrap();
        duplicate
            .set("name", TypedValue::Text("first".to_string()))
            .unwrap();
        duplicate.insert(&db).unwrap();
        let mut rejected = duplicate.clone();
        rejected
            .set("name", TypedValue::Text("duplicate".to_string()))
            .unwrap();
        assert!(rejected.insert(&db).is_err());
        assert!(rejected.is_dirty(), "failed INSERT must retain dirty state");
    }

    #[test]
    fn bound_schema_ddl_builders_execute_and_return_current_descriptor() {
        let db = Database::open_in_memory().unwrap();
        let created = db
            .schema()
            .create_table("orm_ddl")
            .column(
                radixdb_orm::Column::integer("id")
                    .primary_key(true)
                    .check(radixdb_orm::Expr::column("id").gt(0_i64)),
            )
            .column(radixdb_orm::Column::text("name"))
            .column(
                radixdb_orm::Column::new("active", radixdb_orm::DataTypeDescriptor::Boolean)
                    .not_null(true)
                    .default(true),
            )
            .column(radixdb_orm::Column::vector("embedding", 3))
            .execute()
            .unwrap();
        assert_eq!(created.name, "orm_ddl");
        assert_eq!(created.columns.len(), 4);
        db.execute(
            "INSERT INTO orm_ddl(id, name, embedding) VALUES (1, 'bound', $1)",
            vec![Value::vector(vec![1.0, 2.0, 3.0])],
        )
        .unwrap();
        assert!(db
            .query_one::<bool, _>("SELECT active FROM orm_ddl WHERE id = 1", ())
            .unwrap());

        let altered = db
            .schema()
            .alter_table("orm_ddl")
            .add_column(radixdb_orm::Column::text("note"))
            .execute()
            .unwrap();
        assert!(altered.columns.iter().any(|column| column.name == "note"));
        db.schema()
            .create_index(
                radixdb_orm::IndexDefinition::new(
                    "idx_orm_ddl_embedding",
                    "orm_ddl",
                    ["embedding"],
                )
                .method(radixdb_orm::IndexMethod::Hnsw)
                .option("m", TypedValue::Integer(8))
                .option("metric", TypedValue::Text("cosine".to_string())),
            )
            .execute()
            .unwrap();
        db.schema()
            .create_index(
                radixdb_orm::IndexDefinition::new("idx_orm_ddl_active", "orm_ddl", ["active"])
                    .if_not_exists(true)
                    .where_(radixdb_orm::Expr::column("active").eq(true)),
            )
            .execute()
            .unwrap();
        let indexes = db.schema().table("orm_ddl").indexes().fetch().unwrap();
        assert!(indexes.iter().any(|index| {
            index.name == "idx_orm_ddl_embedding"
                && index.options.get("m") == Some(&serde_json::Value::from(8))
                && index.options.get("distance_metric") == Some(&serde_json::Value::from("cosine"))
        }));
        assert!(
            indexes.iter().any(|index| {
                index.name == "idx_orm_ddl_active"
                    && index.predicate.as_deref() == Some("(\"active\" = TRUE)")
            }),
            "unexpected index descriptors: {indexes:?}"
        );
        db.schema()
            .alter_index("idx_orm_ddl_active", "idx_orm_ddl_active_renamed")
            .execute()
            .unwrap();
        assert!(db
            .schema()
            .table("orm_ddl")
            .indexes()
            .fetch()
            .unwrap()
            .iter()
            .any(|index| index.name == "idx_orm_ddl_active_renamed"));
        db.schema()
            .drop_index("orm_ddl", "idx_orm_ddl_active_renamed", false)
            .execute()
            .unwrap();
        assert!(!db
            .schema()
            .table("orm_ddl")
            .indexes()
            .fetch()
            .unwrap()
            .iter()
            .any(|index| index.name == "idx_orm_ddl_active_renamed"));

        let copied = db
            .schema()
            .create_table_as(
                "orm_ddl_copy",
                radixdb_orm::QueryBuilder::from_relation(radixdb_orm::table("orm_ddl")).select([
                    radixdb_orm::Expr::column("id"),
                    radixdb_orm::Expr::column("name"),
                ]),
            )
            .execute()
            .unwrap();
        assert_eq!(copied.columns.len(), 2);
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM orm_ddl_copy", ())
                .unwrap(),
            1
        );
        db.schema()
            .drop_table("orm_ddl_copy")
            .if_exists(true)
            .execute()
            .unwrap();
        db.schema()
            .drop_table("orm_ddl_copy")
            .if_exists(true)
            .execute()
            .unwrap();
        db.schema().truncate_table("orm_ddl").execute().unwrap();
        db.schema().drop_table("orm_ddl").execute().unwrap();
        assert!(db.schema().table("orm_ddl").describe().fetch().is_err());
    }
}
