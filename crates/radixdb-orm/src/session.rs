use crate::{DynamicRecord, GeneratedRecord, IrDocument};
use std::future::Future;
use std::pin::Pin;

/// Runtime-neutral owned future used by async ORM session traits.
pub type OrmFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;

/// Synchronous execution boundary shared by embedded and TCP sessions.
///
/// Implementations are provided for borrowed session objects, so executing an
/// ORM builder never creates or owns a connection and cannot escape the
/// caller's transaction/cursor state.
pub trait OrmSession {
    type CommandOutput;
    type QueryOutput;
    type Error;

    fn execute_document(self, document: &IrDocument) -> Result<Self::CommandOutput, Self::Error>;
    fn query_document(self, document: &IrDocument) -> Result<Self::QueryOutput, Self::Error>;
}

/// Async execution boundary over the caller-owned connection/session.
///
/// The boxed future keeps this crate independent from a particular async
/// runtime while preserving exactly the same IR and result ownership as the
/// synchronous [`OrmSession`].
pub trait AsyncOrmSession {
    type CommandOutput;
    type QueryOutput;
    type Error;

    fn execute_document_async<'a>(
        &'a mut self,
        document: &'a IrDocument,
    ) -> OrmFuture<'a, Self::CommandOutput, Self::Error>;

    fn query_document_async<'a>(
        &'a mut self,
        document: &'a IrDocument,
    ) -> OrmFuture<'a, Self::QueryOutput, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMutation {
    Insert,
    Save,
    Update,
    Delete,
}

/// Transport-specific `RETURNING *` hydration while preserving one session.
pub trait OrmRecordSession {
    type Error;

    fn mutate_record(
        self,
        record: &mut DynamicRecord,
        mutation: RecordMutation,
    ) -> Result<(), Self::Error>;
}

pub trait AsyncOrmRecordSession {
    type Error;

    fn mutate_record_async<'a>(
        &'a mut self,
        record: &'a mut DynamicRecord,
        mutation: RecordMutation,
    ) -> OrmFuture<'a, (), Self::Error>;
}

/// Transport-owned execution for deterministic generated records.
pub trait OrmGeneratedRecordSession {
    type Error;

    fn mutate_generated_record<R: GeneratedRecord>(
        self,
        record: &mut R,
        mutation: RecordMutation,
    ) -> Result<(), Self::Error>;
}

pub trait OrmGeneratedQuerySession {
    type Error;

    fn query_generated_records<R: GeneratedRecord + Default>(
        self,
        document: &IrDocument,
    ) -> Result<Vec<R>, Self::Error>;
}

/// Async counterpart used by transport clients without introducing a runtime
/// dependency into the language-neutral ORM crate.
pub trait AsyncOrmGeneratedRecordSession {
    type Error;

    fn mutate_generated_record_async<'a, R: GeneratedRecord + 'a>(
        &'a mut self,
        record: &'a mut R,
        mutation: RecordMutation,
    ) -> OrmFuture<'a, (), Self::Error>;
}

pub trait AsyncOrmGeneratedQuerySession {
    type Error;

    fn query_generated_records_async<'a, R: GeneratedRecord + Default + 'a>(
        &'a mut self,
        document: &'a IrDocument,
    ) -> OrmFuture<'a, Vec<R>, Self::Error>;
}
