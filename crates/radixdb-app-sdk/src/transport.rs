use std::{future::Future, pin::Pin};

use radixdb_client::{AsyncConnection, ExecuteResult, Row, WireValue};
use radixdb_orm::{
    CatalogOperation, DatabaseDescriptor, DescriptorEnvelope, DescriptorKind, IrDocument, Operation,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    ApplicationError, ApplicationErrorCode, OperationContractError, OperationOutcome,
    ProcedureResult, QueryResult, ResultLimits, RetryClass,
};

pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DatabaseResponse, ApplicationError>> + Send + 'a>>;

#[derive(Debug, Clone)]
pub enum DatabaseRequest {
    Query {
        document: Box<IrDocument>,
        limits: ResultLimits,
    },
    Procedure {
        statement: String,
        positional: Vec<WireValue>,
        limits: ResultLimits,
    },
    DescribeDatabase {
        limits: ResultLimits,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseResponse {
    Query(QueryResult),
    Procedure(ProcedureResult),
    DatabaseDescriptor(DescriptorEnvelope<DatabaseDescriptor>),
}

pub trait ApplicationTransport: Send {
    fn execute(&mut self, request: DatabaseRequest) -> TransportFuture<'_>;

    fn is_reusable(&self) -> bool;
}

pub struct AsyncConnectionTransport<S> {
    connection: AsyncConnection<S>,
}

impl<S> AsyncConnectionTransport<S> {
    pub fn new(connection: AsyncConnection<S>) -> Self {
        Self { connection }
    }

    pub fn connection(&self) -> &AsyncConnection<S> {
        &self.connection
    }

    pub fn connection_mut(&mut self) -> &mut AsyncConnection<S> {
        &mut self.connection
    }

    pub fn into_inner(self) -> AsyncConnection<S> {
        self.connection
    }
}

impl<S> ApplicationTransport for AsyncConnectionTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn execute(&mut self, request: DatabaseRequest) -> TransportFuture<'_> {
        Box::pin(async move {
            match request {
                DatabaseRequest::Query { document, limits } => {
                    let result = self
                        .connection
                        .execute_orm(&document)
                        .await
                        .map_err(ApplicationError::from)?;
                    let query = collect_rows(&mut self.connection, result, limits).await?;
                    Ok(DatabaseResponse::Query(query))
                }
                DatabaseRequest::Procedure {
                    statement,
                    positional,
                    limits,
                } => {
                    let result = self
                        .connection
                        .execute_with_positional_parameters(statement, positional)
                        .await
                        .map_err(ApplicationError::from)?;
                    let procedure = match result {
                        ExecuteResult::CommandComplete {
                            affected_rows,
                            last_insert_id,
                        } => ProcedureResult::CommandComplete {
                            affected_rows,
                            last_insert_id,
                        },
                        cursor @ ExecuteResult::Cursor(_) => ProcedureResult::Rows(
                            collect_rows(&mut self.connection, cursor, limits).await?,
                        ),
                    };
                    Ok(DatabaseResponse::Procedure(procedure))
                }
                DatabaseRequest::DescribeDatabase { limits } => {
                    let document = IrDocument::new(Operation::Catalog {
                        operation: CatalogOperation::DescribeDatabase,
                    });
                    let result = self
                        .connection
                        .execute_orm(&document)
                        .await
                        .map_err(ApplicationError::from)?;
                    let query = collect_rows(&mut self.connection, result, limits).await?;
                    Ok(DatabaseResponse::DatabaseDescriptor(
                        decode_database_descriptor(query)?,
                    ))
                }
            }
        })
    }

    fn is_reusable(&self) -> bool {
        self.connection.is_reusable()
    }
}

fn decode_database_descriptor(
    result: QueryResult,
) -> Result<DescriptorEnvelope<DatabaseDescriptor>, ApplicationError> {
    let [row] = result.rows.as_slice() else {
        return Err(OperationContractError::UnexpectedResult(
            "database descriptor expected exactly one row",
        )
        .into());
    };
    let [WireValue::String(json)] = row.values.as_slice() else {
        return Err(OperationContractError::UnexpectedResult(
            "database descriptor expected exactly one text column",
        )
        .into());
    };
    DescriptorEnvelope::from_json(json, DescriptorKind::Database).map_err(|error| {
        ApplicationError::new(
            ApplicationErrorCode::ProtocolViolation,
            error.to_string(),
            RetryClass::Never,
            OperationOutcome::Unknown,
        )
    })
}

async fn collect_rows<S>(
    connection: &mut AsyncConnection<S>,
    result: ExecuteResult,
    limits: ResultLimits,
) -> Result<QueryResult, ApplicationError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let ExecuteResult::Cursor(cursor) = result else {
        return Err(
            OperationContractError::UnexpectedResult("typed query expected a cursor").into(),
        );
    };
    let columns = cursor.columns().to_vec();
    let mut rows = Vec::new();
    let mut encoded_bytes = 0_usize;
    loop {
        let batch = connection
            .fetch(&cursor)
            .await
            .map_err(ApplicationError::from)?;
        for row in batch.rows {
            if rows.len() == limits.max_rows() {
                close_after_limit(connection, cursor).await;
                return Err(limit_error("row", limits.max_rows()));
            }
            let row_bytes = estimate_row_bytes(&row);
            if encoded_bytes.saturating_add(row_bytes) > limits.max_bytes() {
                close_after_limit(connection, cursor).await;
                return Err(limit_error("byte", limits.max_bytes()));
            }
            encoded_bytes += row_bytes;
            rows.push(row);
        }
        if batch.eof {
            return Ok(QueryResult {
                columns,
                rows,
                encoded_bytes,
            });
        }
    }
}

async fn close_after_limit<S>(connection: &mut AsyncConnection<S>, cursor: radixdb_client::Cursor)
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let _ = connection.close_cursor(cursor).await;
}

fn limit_error(kind: &'static str, limit: usize) -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::ResultLimitExceeded,
        OperationContractError::ResultLimitExceeded { kind, limit }.to_string(),
        RetryClass::Never,
        OperationOutcome::RejectedBeforeCommit,
    )
}

fn estimate_row_bytes(row: &Row) -> usize {
    row.values
        .iter()
        .map(|value| 16_usize.saturating_add(estimate_value_bytes(value)))
        .sum()
}

fn estimate_value_bytes(value: &WireValue) -> usize {
    match value {
        WireValue::Null => 0,
        WireValue::Bool(_) | WireValue::Int8(_) | WireValue::UInt8(_) => 1,
        WireValue::Int16(_) | WireValue::UInt16(_) => 2,
        WireValue::Int32(_) | WireValue::UInt32(_) | WireValue::Date { .. } => 4,
        WireValue::Int(_)
        | WireValue::UInt(_)
        | WireValue::Float64(_)
        | WireValue::DateTime { .. }
        | WireValue::TimestampNanos { .. }
        | WireValue::CivilTimestampNanos { .. }
        | WireValue::TimeNanos { .. } => 8,
        WireValue::Decimal { .. } => 18,
        WireValue::String(value) | WireValue::Json(value) => value.len(),
        WireValue::Bytes(value) | WireValue::Vector(value) => value.len(),
        WireValue::Uuid(_) => 16,
        WireValue::External { payload, .. } => 20 + payload.len(),
    }
}
