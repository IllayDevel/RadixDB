use chrono::{DateTime, Utc};
use radixdb_catalog::{
    CatalogName, CatalogPayload, JobArgument, JobSchedule, ObjectId, ObjectKind, ResourcePolicy,
};
use radixdb_core::{Error, Result, Value};
use radixdb_procedural::{
    Diagnostic, DiagnosticCategory, DiagnosticKind, PrincipalContext, RuntimeValue,
};
use radixdb_sql::{CreateJobStatement, Expression, JobScheduleSyntax};
use radixdb_storage::mvcc::persistence::{deserialize_value, serialize_value};

use crate::catalog::BoundJobDefinition;
use crate::context::ExecutionContext;
use crate::Executor;

use super::call::{bind_job_call, CallStatementStage};
use super::error::map_executor_error;
use super::{transaction_visible_catalog, ProceduralCallOutcome, ProceduralResultStage};

const MAX_JOB_ATTEMPT: u32 = i32::MAX as u32;

fn scheduler_may_retry(kind: DiagnosticKind) -> bool {
    matches!(
        kind,
        DiagnosticKind::RuntimeConflict
            | DiagnosticKind::ResourceDeadline
            | DiagnosticKind::ResourceCancelled
    )
}

fn job_attempt_failure(
    job_id: ObjectId,
    metadata: &JobAttemptMetadata,
    cause: Diagnostic,
) -> Diagnostic {
    if cause.kind() == DiagnosticKind::JobAttemptFailed {
        return cause;
    }
    let cause_kind = cause.kind();
    let cause_category = cause.category();
    let mut failure = Diagnostic::new(
        DiagnosticKind::JobAttemptFailed,
        "scheduled job attempt failed",
    )
    .with_cause(cause_kind)
    .with_detail("job_id", job_id.to_string())
    .with_detail(
        "scheduled_at_unix_ns",
        metadata.scheduled_at_unix_ns.to_string(),
    )
    .with_detail("attempt", metadata.attempt.to_string())
    .with_detail("cause_kind", cause_kind.as_str())
    .with_detail("cause_category", cause_category.as_str())
    .with_detail(
        "scheduler_retryable",
        scheduler_may_retry(cause_kind).to_string(),
    )
    .with_primary_span(cause.primary_span().cloned());
    for secondary in cause.secondary_spans() {
        failure = failure.with_secondary_span(secondary.label.clone(), secondary.span.clone());
    }
    for frame in cause.frames() {
        failure = failure.with_frame(frame.clone());
    }
    // Security errors deliberately keep only the stable class. Their message
    // may contain an object name that the attempt principal must not disclose
    // through scheduler history or a remote status surface.
    if cause_category != DiagnosticCategory::Security {
        failure = failure.with_detail("cause_message", cause.message());
        for detail in cause.details() {
            failure = failure.with_detail(format!("cause.{}", detail.key), detail.value.clone());
        }
    }
    failure
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobAttemptMetadata {
    pub scheduled_at_unix_ns: i64,
    pub attempt: u32,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobAttemptOutcome {
    pub job_id: ObjectId,
    pub scheduled_at_unix_ns: i64,
    pub attempt: u32,
    pub idempotency_key: String,
    pub call: ProceduralCallOutcome,
}

/// Immutable scheduler input captured from one catalog generation.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledJobDefinition {
    pub job_id: ObjectId,
    pub definition_version: u32,
    pub schedule: ScheduledJobSchedule,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledJobSchedule {
    EveryNs(u64),
    AtUnixNs(i64),
}

pub(crate) fn bind_job_definition(
    executor: &Executor,
    statement: &CreateJobStatement,
    catalog: &radixdb_catalog::CatalogGeneration,
    context: &ExecutionContext,
) -> Result<BoundJobDefinition> {
    let principal_name = statement
        .principal
        .components
        .last()
        .ok_or_else(|| Error::invalid_argument("job principal name is empty"))?;
    if statement.principal.components.len() != 1 {
        return Err(Error::invalid_argument(
            "principals are global and cannot be namespace-qualified",
        ));
    }
    let principal_name = CatalogName::new(principal_name.value.as_str())
        .map_err(|error| Error::invalid_argument(error.to_string()))?;
    let principal = catalog
        .objects_of_kind(ObjectKind::Principal)
        .find(|object| object.name().normalized() == principal_name.normalized())
        .ok_or_else(|| {
            Error::invalid_argument(format!(
                "job principal '{}' does not exist",
                statement.principal
            ))
        })?;
    let schedule = bind_schedule(&statement.schedule)?;
    let (procedure_id, arguments) = bind_job_call(
        executor,
        catalog,
        &statement.procedure,
        &statement.arguments,
        context,
    )?;
    let arguments = arguments
        .into_iter()
        .map(|(data_type, value)| {
            let encoded = if value.is_null() {
                None
            } else {
                Some(serialize_value(&value)?)
            };
            JobArgument::new(None, data_type, encoded)
                .map_err(|error| Error::invalid_argument(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundJobDefinition {
        procedure_id,
        principal_id: principal.id(),
        schedule,
        arguments,
        resource_policy: ResourcePolicy::default_job(),
    })
}

fn bind_schedule(schedule: &JobScheduleSyntax) -> Result<JobSchedule> {
    match schedule {
        JobScheduleSyntax::Every(Expression::IntervalLiteral(interval)) => {
            if interval.quantity <= 0 {
                return Err(Error::invalid_argument(
                    "job EVERY interval must be greater than zero",
                ));
            }
            let nanos_per_unit = match interval.unit.as_str() {
                "second" => 1_000_000_000_u64,
                "minute" => 60 * 1_000_000_000,
                "hour" => 60 * 60 * 1_000_000_000,
                "day" => 24 * 60 * 60 * 1_000_000_000,
                "week" => 7 * 24 * 60 * 60 * 1_000_000_000,
                "month" | "year" => {
                    return Err(Error::invalid_argument(
                        "job EVERY does not accept calendar MONTH/YEAR intervals",
                    ))
                }
                _ => return Err(Error::invalid_argument("unknown job interval unit")),
            };
            let quantity = u64::try_from(interval.quantity)
                .map_err(|_| Error::invalid_argument("job interval is outside u64"))?;
            let value = quantity
                .checked_mul(nanos_per_unit)
                .ok_or_else(|| Error::invalid_argument("job interval nanoseconds overflow"))?;
            Ok(JobSchedule::EveryNs(value))
        }
        JobScheduleSyntax::Every(_) => Err(Error::invalid_argument(
            "job EVERY schedule must be one INTERVAL literal",
        )),
        JobScheduleSyntax::At(expression) => {
            let Expression::StringLiteral(literal) = expression else {
                return Err(Error::invalid_argument(
                    "job AT schedule must be one TIMESTAMP literal",
                ));
            };
            if !literal.type_hint.as_deref().is_some_and(|hint| {
                matches!(
                    hint.to_ascii_uppercase().as_str(),
                    "TIMESTAMP" | "TIMESTAMPTZ"
                )
            }) {
                return Err(Error::invalid_argument(
                    "job AT schedule requires TIMESTAMP or TIMESTAMPTZ",
                ));
            }
            let timestamp = radixdb_core::value::parse_timestamp(literal.value.as_str())?;
            let nanos = timestamp.timestamp_nanos_opt().ok_or_else(|| {
                Error::invalid_argument("job timestamp is outside nanosecond range")
            })?;
            Ok(JobSchedule::AtUnixNs(nanos))
        }
    }
}

fn decode_arguments(payload: &radixdb_catalog::JobPayload) -> Result<Vec<RuntimeValue>> {
    payload
        .arguments()
        .iter()
        .map(|argument| {
            let value = match argument.value() {
                Some(encoded) => deserialize_value(encoded)?,
                None => Value::null(argument.data_type().logical_type()),
            };
            if value.data_type() != argument.data_type().logical_type() {
                return Err(Error::invalid_argument(
                    "job argument payload type differs from its catalog descriptor",
                ));
            }
            Ok(RuntimeValue::scalar(value))
        })
        .collect()
}

impl Executor {
    #[doc(hidden)]
    pub fn scheduled_jobs_snapshot(&self) -> Result<Vec<ScheduledJobDefinition>> {
        let catalog = self.engine.pin_catalog()?;
        let mut jobs = catalog
            .objects_of_kind(ObjectKind::Job)
            .filter_map(|object| {
                let CatalogPayload::Job(payload) = object.payload() else {
                    return None;
                };
                payload.enabled().then_some(ScheduledJobDefinition {
                    job_id: object.id(),
                    definition_version: payload.definition_version(),
                    schedule: match payload.schedule() {
                        JobSchedule::EveryNs(interval) => ScheduledJobSchedule::EveryNs(interval),
                        JobSchedule::AtUnixNs(timestamp) => {
                            ScheduledJobSchedule::AtUnixNs(timestamp)
                        }
                    },
                })
            })
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    pub fn execute_job_attempt(
        &self,
        job_id: ObjectId,
        metadata: JobAttemptMetadata,
        context: &ExecutionContext,
    ) -> std::result::Result<JobAttemptOutcome, Diagnostic> {
        self.execute_job_attempt_inner(job_id, metadata.clone(), context)
            .map_err(|cause| job_attempt_failure(job_id, &metadata, cause))
    }

    fn execute_job_attempt_inner(
        &self,
        job_id: ObjectId,
        metadata: JobAttemptMetadata,
        context: &ExecutionContext,
    ) -> std::result::Result<JobAttemptOutcome, Diagnostic> {
        if !self.ddl_fence_already_held {
            let _fence = self.engine.acquire_ddl_statement_fence(false);
            return self
                .fork_with_owned_ddl_fence()
                .execute_job_attempt_inner(job_id, metadata, context);
        }
        if metadata.attempt == 0 || metadata.attempt > MAX_JOB_ATTEMPT {
            return Err(map_executor_error(Error::invalid_argument(
                "job attempt must be inside 1..=2147483647",
            )));
        }
        if metadata.idempotency_key.is_empty() || metadata.idempotency_key.len() > 1024 {
            return Err(map_executor_error(Error::invalid_argument(
                "job idempotency key must contain 1..=1024 bytes",
            )));
        }
        if self.has_active_transaction() {
            return Err(map_executor_error(Error::invalid_argument(
                "job attempts require a fresh executor transaction",
            )));
        }
        let mut stage = CallStatementStage::default();
        stage.begin()?;
        let boundary = match self.begin_procedural_boundary() {
            Ok(boundary) => boundary,
            Err(error) => {
                stage.discard();
                return Err(map_executor_error(error));
            }
        };
        let (catalog, _) = match transaction_visible_catalog(self) {
            Ok(value) => value,
            Err(error) => {
                stage.discard();
                return Err(self.abort_after_setup_error(&boundary, map_executor_error(error)));
            }
        };
        let job = catalog
            .object(job_id)
            .filter(|object| object.kind() == ObjectKind::Job)
            .ok_or_else(|| Error::invalid_argument(format!("job object {job_id} does not exist")));
        let job = match job {
            Ok(job) => job,
            Err(error) => {
                stage.discard();
                return Err(self.abort_after_setup_error(&boundary, map_executor_error(error)));
            }
        };
        let CatalogPayload::Job(payload) = job.payload() else {
            unreachable!("catalog kind/payload invariant")
        };
        if !payload.enabled() {
            stage.discard();
            return Err(self.abort_after_setup_error(
                &boundary,
                map_executor_error(Error::invalid_argument(format!("job {job_id} is disabled"))),
            ));
        }
        let principal = catalog
            .object(payload.principal_id())
            .filter(|object| object.kind() == ObjectKind::Principal)
            .ok_or_else(|| Error::invalid_argument("job principal disappeared from the catalog"));
        match principal {
            Ok(_) => {}
            Err(error) => {
                stage.discard();
                return Err(self.abort_after_setup_error(&boundary, map_executor_error(error)));
            }
        }
        let procedure_id = payload.procedure_id();
        let arguments = match decode_arguments(payload) {
            Ok(arguments) => arguments,
            Err(error) => {
                stage.discard();
                return Err(self.abort_after_setup_error(&boundary, map_executor_error(error)));
            }
        };
        let policy = payload.resource_policy();
        let principal_id = payload.principal_id();
        drop(catalog);

        let scheduled_at = DateTime::<Utc>::from_timestamp_nanos(metadata.scheduled_at_unix_ns);
        let mut job_context = context.clone();
        job_context = job_context.with_principal_id(principal_id);
        job_context.set_timeout_ms(match context.timeout_ms() {
            0 => policy.deadline_ms,
            caller => caller.min(policy.deadline_ms),
        });
        job_context.set_job_context(
            &metadata.idempotency_key,
            job_id,
            metadata.attempt,
            scheduled_at,
        );

        let call = self.execute_procedure_inside_boundary(
            procedure_id,
            arguments,
            &job_context,
            PrincipalContext {
                session_principal: principal_id,
                invoker_principal: principal_id,
                effective_principal: principal_id,
            },
            &mut stage,
            &boundary,
        )?;
        Ok(JobAttemptOutcome {
            job_id,
            scheduled_at_unix_ns: metadata.scheduled_at_unix_ns,
            attempt: metadata.attempt,
            idempotency_key: metadata.idempotency_key,
            call,
        })
    }
}
