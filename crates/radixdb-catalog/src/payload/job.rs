use std::collections::BTreeSet;

use crate::payload::common::{validate_flags, validate_version};
use crate::{CatalogDataType, CatalogError, CatalogName, CatalogResult, ObjectId, ResourcePolicy};

pub const MAX_JOB_ARGUMENTS: usize = 1024;
pub const MAX_LITERAL_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobArgument {
    name: Option<CatalogName>,
    data_type: CatalogDataType,
    value: Option<Vec<u8>>,
}

impl JobArgument {
    pub fn new(
        name: Option<CatalogName>,
        data_type: CatalogDataType,
        value: Option<Vec<u8>>,
    ) -> CatalogResult<Self> {
        if value
            .as_ref()
            .is_some_and(|value| value.len() > MAX_LITERAL_BYTES)
        {
            return Err(CatalogError::CatalogLimitExceeded {
                field: "job argument literal bytes",
                actual: value.as_ref().map_or(0, Vec::len) as u64,
                limit: MAX_LITERAL_BYTES as u64,
            });
        }
        Ok(Self {
            name,
            data_type,
            value,
        })
    }

    pub fn name(&self) -> Option<&CatalogName> {
        self.name.as_ref()
    }
    pub const fn data_type(&self) -> CatalogDataType {
        self.data_type
    }
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSchedule {
    AtUnixNs(i64),
    EveryNs(u64),
}

impl JobSchedule {
    pub fn validate(self) -> CatalogResult<Self> {
        if matches!(self, Self::EveryNs(0)) {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "job interval must be non-zero",
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobPayload {
    procedure_id: ObjectId,
    principal_id: ObjectId,
    schedule: JobSchedule,
    arguments: Vec<JobArgument>,
    enabled: bool,
    definition_version: u32,
    resource_policy: ResourcePolicy,
}

impl JobPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        procedure_id: ObjectId,
        principal_id: ObjectId,
        schedule: JobSchedule,
        arguments: Vec<JobArgument>,
        enabled: bool,
        definition_version: u32,
        resource_policy: ResourcePolicy,
    ) -> CatalogResult<Self> {
        if arguments.len() > MAX_JOB_ARGUMENTS || definition_version == 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "job argument count or definition version is invalid",
            });
        }
        let mut named_started = false;
        let mut names = BTreeSet::new();
        for argument in &arguments {
            if let Some(name) = argument.name() {
                named_started = true;
                if !names.insert(name.normalized().as_str().to_owned()) {
                    return Err(CatalogError::InvalidCatalogFormat {
                        detail: "duplicate named job argument",
                    });
                }
            } else if named_started {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "positional job argument follows named argument",
                });
            }
        }
        Ok(Self {
            procedure_id,
            principal_id,
            schedule: schedule.validate()?,
            arguments,
            enabled,
            definition_version,
            resource_policy: resource_policy.validate()?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        procedure_id: ObjectId,
        principal_id: ObjectId,
        schedule: JobSchedule,
        arguments: Vec<JobArgument>,
        enabled: bool,
        definition_version: u32,
        resource_policy: ResourcePolicy,
    ) -> CatalogResult<Self> {
        validate_version("job", version)?;
        validate_flags("job", flags)?;
        Self::new(
            procedure_id,
            principal_id,
            schedule,
            arguments,
            enabled,
            definition_version,
            resource_policy,
        )
    }

    pub const fn procedure_id(&self) -> ObjectId {
        self.procedure_id
    }
    pub const fn principal_id(&self) -> ObjectId {
        self.principal_id
    }
    pub const fn schedule(&self) -> JobSchedule {
        self.schedule
    }
    pub fn arguments(&self) -> &[JobArgument] {
        &self.arguments
    }
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
    pub const fn definition_version(&self) -> u32 {
        self.definition_version
    }
    pub const fn resource_policy(&self) -> ResourcePolicy {
        self.resource_policy
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}
