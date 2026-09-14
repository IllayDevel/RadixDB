//! Domain-neutral contracts for applications backed by a separate RadixDB
//! server.
//!
//! Generated application SDKs provide concrete query, procedure and event
//! types. This crate owns only context propagation, bounded execution and
//! conservative failure classification. It deliberately cannot reach engine,
//! executor, catalog, WAL, MVCC, page or storage internals.

mod client;
mod codegen;
mod context;
mod error;
mod operation;
mod transport;

pub use client::ApplicationClient;
pub use codegen::{
    application_descriptor_fingerprints, generate_rust_application,
    portable_application_descriptor, ApplicationCodegenError, ApplicationDescriptorFingerprints,
    GeneratedApplicationRust,
};
pub use context::{
    ApplicationSession, CancellationSignal, ContextError, IdempotencyKey, RequestContext, RequestId,
};
pub use error::{ApplicationError, ApplicationErrorCode, OperationOutcome, RetryClass};
pub use operation::{
    ApplicationEvent, OperationContractError, ProcedureResult, QueryResult, ResultLimits,
    TypedProcedure, TypedQuery,
};
pub use transport::{
    ApplicationTransport, AsyncConnectionTransport, DatabaseRequest, DatabaseResponse,
    TransportFuture,
};

pub use radixdb_client::{Column, Row, WireValue};
pub use radixdb_orm::{IrDocument, TypedValue};
