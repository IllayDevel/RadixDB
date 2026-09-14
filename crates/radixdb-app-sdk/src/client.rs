use crate::{
    operation::{render_procedure_statement, validate_operation_name},
    ApplicationError, ApplicationTransport, DatabaseRequest, DatabaseResponse, RequestContext,
    TypedProcedure, TypedQuery,
};
use radixdb_orm::{DatabaseDescriptor, DescriptorEnvelope};

pub struct ApplicationClient<T> {
    transport: T,
}

impl<T> ApplicationClient<T>
where
    T: ApplicationTransport,
{
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_inner(self) -> T {
        self.transport
    }

    pub fn is_reusable(&self) -> bool {
        self.transport.is_reusable()
    }

    pub async fn query<Q>(
        &mut self,
        context: &RequestContext,
        query: Q,
    ) -> Result<Q::Output, ApplicationError>
    where
        Q: TypedQuery,
    {
        context.check_active()?;
        validate_operation_name(Q::NAME)?;
        let request = DatabaseRequest::Query {
            document: Box::new(query.document()?),
            limits: query.limits(),
        };
        let response = self.execute(context, request).await?;
        let DatabaseResponse::Query(result) = response else {
            return Err(crate::OperationContractError::UnexpectedResult(
                "query transport returned a procedure response",
            )
            .into());
        };
        query.decode(result).map_err(ApplicationError::from)
    }

    pub async fn call<P>(
        &mut self,
        context: &RequestContext,
        procedure: P,
    ) -> Result<P::Output, ApplicationError>
    where
        P: TypedProcedure,
    {
        context.check_active()?;
        validate_operation_name(P::NAME)?;
        let positional = procedure.positional_parameters()?;
        let request = DatabaseRequest::Procedure {
            statement: render_procedure_statement(P::SCHEMA, P::PROCEDURE, positional.len())?,
            positional,
            limits: procedure.limits(),
        };
        let response = self.execute(context, request).await?;
        let DatabaseResponse::Procedure(result) = response else {
            return Err(crate::OperationContractError::UnexpectedResult(
                "procedure transport returned a query response",
            )
            .into());
        };
        procedure.decode(result).map_err(ApplicationError::from)
    }

    /// Fetch the canonical database descriptor through the same authenticated,
    /// cancellable transport used for application calls.
    ///
    /// This is a broker startup/diagnostic operation. Application feature code
    /// should continue using generated queries and procedures.
    pub async fn describe_database(
        &mut self,
        context: &RequestContext,
        max_bytes: usize,
    ) -> Result<DescriptorEnvelope<DatabaseDescriptor>, ApplicationError> {
        context.check_active()?;
        let limits = crate::ResultLimits::new(1, max_bytes)?;
        let response = self
            .execute(context, DatabaseRequest::DescribeDatabase { limits })
            .await?;
        let DatabaseResponse::DatabaseDescriptor(descriptor) = response else {
            return Err(crate::OperationContractError::UnexpectedResult(
                "descriptor transport returned an operation response",
            )
            .into());
        };
        Ok(descriptor)
    }

    async fn execute(
        &mut self,
        context: &RequestContext,
        request: DatabaseRequest,
    ) -> Result<DatabaseResponse, ApplicationError> {
        let cancellation = context.cancellation().clone();
        let operation = self.transport.execute(request);
        tokio::pin!(operation);

        if let Some(deadline) = context.deadline() {
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
            tokio::pin!(sleep);
            tokio::select! {
                biased;
                response = &mut operation => response,
                () = cancellation.cancelled() => Err(ApplicationError::cancelled_after_start()),
                () = &mut sleep => Err(ApplicationError::deadline_after_start()),
            }
        } else {
            tokio::select! {
                biased;
                response = &mut operation => response,
                () = cancellation.cancelled() => Err(ApplicationError::cancelled_after_start()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use radixdb_orm::{IrDocument, Operation, Select};

    use super::*;
    use crate::{
        ApplicationSession, CancellationSignal, ContextError, OperationContractError,
        OperationOutcome, ProcedureResult, QueryResult, RequestId, RetryClass, TransportFuture,
    };

    struct StubTransport {
        requests: Arc<Mutex<Vec<DatabaseRequest>>>,
        response: Option<Result<DatabaseResponse, ApplicationError>>,
        reusable: bool,
    }

    impl ApplicationTransport for StubTransport {
        fn execute(&mut self, request: DatabaseRequest) -> TransportFuture<'_> {
            self.requests.lock().unwrap().push(request);
            let response = self.response.take().expect("one response");
            Box::pin(async move { response })
        }

        fn is_reusable(&self) -> bool {
            self.reusable
        }
    }

    struct PendingTransport {
        entered: Arc<tokio::sync::Notify>,
    }

    impl ApplicationTransport for PendingTransport {
        fn execute(&mut self, _request: DatabaseRequest) -> TransportFuture<'_> {
            let entered = self.entered.clone();
            Box::pin(async move {
                entered.notify_one();
                std::future::pending().await
            })
        }

        fn is_reusable(&self) -> bool {
            false
        }
    }

    struct CountQuery;

    impl TypedQuery for CountQuery {
        type Output = usize;
        const NAME: &'static str = "sample.count";

        fn document(&self) -> Result<IrDocument, OperationContractError> {
            Ok(IrDocument::new(Operation::Select {
                query: Select::default(),
            }))
        }

        fn decode(self, result: QueryResult) -> Result<Self::Output, OperationContractError> {
            Ok(result.rows.len())
        }
    }

    struct TouchProcedure;

    impl TypedProcedure for TouchProcedure {
        type Output = u64;
        const NAME: &'static str = "sample.touch";
        const SCHEMA: &'static str = "sample";
        const PROCEDURE: &'static str = "touch";

        fn decode(self, result: ProcedureResult) -> Result<Self::Output, OperationContractError> {
            match result {
                ProcedureResult::CommandComplete { affected_rows, .. } => Ok(affected_rows),
                ProcedureResult::Rows(_) => Err(OperationContractError::UnexpectedResult(
                    "command completion expected",
                )),
            }
        }
    }

    fn context(cancellation: CancellationSignal) -> RequestContext {
        let session =
            Arc::new(ApplicationSession::new("subject", "session", 1, Vec::new()).unwrap());
        RequestContext::new(RequestId::new("request").unwrap(), session)
            .with_cancellation(cancellation)
    }

    #[tokio::test]
    async fn typed_query_is_the_only_input_to_the_facade() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = StubTransport {
            requests: requests.clone(),
            response: Some(Ok(DatabaseResponse::Query(QueryResult {
                columns: Vec::new(),
                rows: vec![radixdb_client::Row { values: Vec::new() }],
                encoded_bytes: 0,
            }))),
            reusable: true,
        };
        let mut client = ApplicationClient::new(transport);
        let result = client
            .query(&context(CancellationSignal::new()), CountQuery)
            .await
            .unwrap();
        assert_eq!(result, 1);
        assert!(client.is_reusable());
        assert!(matches!(
            requests.lock().unwrap().as_slice(),
            [DatabaseRequest::Query { .. }]
        ));
    }

    #[tokio::test]
    async fn typed_procedure_decodes_command_completion() {
        let transport = StubTransport {
            requests: Arc::new(Mutex::new(Vec::new())),
            response: Some(Ok(DatabaseResponse::Procedure(
                ProcedureResult::CommandComplete {
                    affected_rows: 2,
                    last_insert_id: 0,
                },
            ))),
            reusable: true,
        };
        let mut client = ApplicationClient::new(transport);
        assert_eq!(
            client
                .call(&context(CancellationSignal::new()), TouchProcedure)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn descriptor_negotiation_uses_bounded_authenticated_transport() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let descriptor = DescriptorEnvelope::new(
            radixdb_orm::DescriptorKind::Database,
            DatabaseDescriptor {
                schema_generation: 0,
                fingerprint: "a".repeat(64),
                tables: Vec::new(),
                views: Vec::new(),
                procedures: Vec::new(),
                extensions: std::collections::BTreeMap::new(),
            },
        );
        let transport = StubTransport {
            requests: requests.clone(),
            response: Some(Ok(DatabaseResponse::DatabaseDescriptor(descriptor.clone()))),
            reusable: true,
        };
        let mut client = ApplicationClient::new(transport);
        assert_eq!(
            client
                .describe_database(&context(CancellationSignal::new()), 1024 * 1024)
                .await
                .unwrap(),
            descriptor
        );
        let requests = requests.lock().unwrap();
        let [DatabaseRequest::DescribeDatabase { limits }] = requests.as_slice() else {
            panic!("expected one descriptor request");
        };
        assert_eq!(limits.max_rows(), 1);
        assert_eq!(limits.max_bytes(), 1024 * 1024);
    }

    #[tokio::test]
    async fn pre_cancelled_request_never_reaches_transport() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = StubTransport {
            requests: requests.clone(),
            response: None,
            reusable: true,
        };
        let cancellation = CancellationSignal::new();
        cancellation.cancel();
        let mut client = ApplicationClient::new(transport);
        let error = client
            .query(&context(cancellation), CountQuery)
            .await
            .unwrap_err();
        assert_eq!(error.outcome(), OperationOutcome::NotStarted);
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancellation_during_execution_never_claims_safe_retry() {
        let cancellation = CancellationSignal::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let mut client = ApplicationClient::new(PendingTransport {
            entered: entered.clone(),
        });
        let request_context = context(cancellation.clone());
        let cancel = async move {
            entered.notified().await;
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(client.query(&request_context, CountQuery), cancel);
        let error = result.unwrap_err();
        assert_eq!(error.retry_class(), RetryClass::RequiresOutcomeResolution);
        assert_eq!(error.outcome(), OperationOutcome::Unknown);
    }

    #[tokio::test]
    async fn elapsed_deadline_is_rejected_before_transport_start() {
        let transport = StubTransport {
            requests: Arc::new(Mutex::new(Vec::new())),
            response: None,
            reusable: true,
        };
        let mut request_context = context(CancellationSignal::new());
        request_context = request_context.with_deadline(Instant::now() - Duration::from_millis(1));
        let mut client = ApplicationClient::new(transport);
        let error = client
            .query(&request_context, CountQuery)
            .await
            .unwrap_err();
        assert_eq!(error.outcome(), OperationOutcome::NotStarted);
    }

    #[allow(dead_code)]
    fn assert_transport_future_is_send(
        future: Pin<Box<dyn Future<Output = Result<DatabaseResponse, ApplicationError>> + Send>>,
    ) {
        drop(future);
    }

    #[allow(dead_code)]
    fn context_error_remains_public(error: ContextError) -> String {
        error.to_string()
    }
}
