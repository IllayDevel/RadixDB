use crate::payload::common::{ordered_unique_ids, validate_flags, validate_version, CanonicalSql};
use crate::{CatalogDataType, CatalogError, CatalogName, CatalogResult, ObjectId};

pub const PROCEDURAL_LANGUAGE_VERSION: u16 = 1;
pub const NATIVE_LANGUAGE_VERSION: u16 = 1;
pub const MAX_ROUTINE_ARGUMENTS: usize = 1024;
pub const MAX_RESULT_COLUMNS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum LanguageKind {
    RadixPl = 1,
    Native = 2,
}

impl TryFrom<u16> for LanguageKind {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::RadixPl),
            2 => Ok(Self::Native),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "routine language",
                tag,
            }),
        }
    }
}

impl LanguageKind {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProceduralSource {
    normalized: String,
    digest: [u8; 32],
}

impl ProceduralSource {
    pub fn new(source: impl Into<String>) -> CatalogResult<Self> {
        let source = source.into();
        if source.starts_with('\u{feff}') || source.contains('\0') {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "procedural source contains BOM or NUL",
            });
        }
        let normalized = source.replace("\r\n", "\n").replace('\r', "\n");
        if normalized.is_empty() || normalized.len() > super::MAX_CANONICAL_SQL_BYTES {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "procedural source is empty or exceeds 16 MiB",
            });
        }
        let digest = radixdb_core::sha256_digest(normalized.as_bytes());
        Ok(Self { normalized, digest })
    }

    pub fn from_fields(source: impl Into<String>, digest: [u8; 32]) -> CatalogResult<Self> {
        let value = Self::new(source)?;
        if value.digest != digest {
            return Err(CatalogError::CatalogChecksumMismatch {
                scope: "procedural source digest",
            });
        }
        Ok(value)
    }
    pub fn as_str(&self) -> &str {
        &self.normalized
    }
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ArgumentMode {
    In = 1,
    Out = 2,
    InOut = 3,
}

impl TryFrom<u16> for ArgumentMode {
    type Error = CatalogError;
    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::In),
            2 => Ok(Self::Out),
            3 => Ok(Self::InOut),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "argument mode",
                tag,
            }),
        }
    }
}
impl ArgumentMode {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineArgument {
    name: CatalogName,
    mode: ArgumentMode,
    data_type: CatalogDataType,
    nullable: bool,
    default_sql: Option<CanonicalSql>,
}

impl RoutineArgument {
    pub fn new(
        name: CatalogName,
        mode: ArgumentMode,
        data_type: CatalogDataType,
        nullable: bool,
        default_sql: Option<String>,
    ) -> CatalogResult<Self> {
        if mode == ArgumentMode::Out && default_sql.is_some() {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "OUT argument cannot have a default",
            });
        }
        Ok(Self {
            name,
            mode,
            data_type,
            nullable,
            default_sql: default_sql
                .map(|sql| CanonicalSql::new("routine.argument.default", sql))
                .transpose()?,
        })
    }
    pub fn name(&self) -> &CatalogName {
        &self.name
    }
    pub const fn mode(&self) -> ArgumentMode {
        self.mode
    }
    pub const fn data_type(&self) -> CatalogDataType {
        self.data_type
    }
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn default_sql(&self) -> Option<&CanonicalSql> {
        self.default_sql.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    name: CatalogName,
    data_type: CatalogDataType,
    nullable: bool,
}
impl ResultColumn {
    pub const fn new(name: CatalogName, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
    pub fn name(&self) -> &CatalogName {
        &self.name
    }
    pub const fn data_type(&self) -> CatalogDataType {
        self.data_type
    }
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutineResult {
    Void,
    Scalar {
        data_type: CatalogDataType,
        nullable: bool,
    },
    Table(Vec<ResultColumn>),
    Trigger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Volatility {
    Immutable = 1,
    Stable = 2,
    Volatile = 3,
}
impl TryFrom<u16> for Volatility {
    type Error = CatalogError;
    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Immutable),
            2 => Ok(Self::Stable),
            3 => Ok(Self::Volatile),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "volatility",
                tag,
            }),
        }
    }
}
impl Volatility {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SecurityMode {
    Invoker = 1,
    Definer = 2,
}
impl TryFrom<u16> for SecurityMode {
    type Error = CatalogError;
    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Invoker),
            2 => Ok(Self::Definer),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "security mode",
                tag,
            }),
        }
    }
}
impl SecurityMode {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePolicy {
    pub instructions: u64,
    pub heap_bytes: u64,
    pub frames: u32,
    pub sql_statements: u64,
    pub rows: u64,
    pub result_bytes: u64,
    pub deadline_ms: u64,
}

impl ResourcePolicy {
    pub const fn default_call() -> Self {
        Self {
            instructions: 10_000_000,
            heap_bytes: 64 * 1024 * 1024,
            frames: 64,
            sql_statements: 100_000,
            rows: 1_000_000,
            result_bytes: 256 * 1024 * 1024,
            deadline_ms: 60_000,
        }
    }

    pub const fn default_job() -> Self {
        Self {
            deadline_ms: 15 * 60 * 1000,
            ..Self::default_call()
        }
    }
    pub fn validate(self) -> CatalogResult<Self> {
        if self.instructions == 0
            || self.instructions > 1_000_000_000
            || self.heap_bytes == 0
            || self.heap_bytes > 256 * 1024 * 1024
            || self.frames == 0
            || self.frames > 256
            || self.sql_statements == 0
            || self.sql_statements > 10_000_000
            || self.rows == 0
            || self.rows > 10_000_000
            || self.result_bytes == 0
            || self.result_bytes > 1024 * 1024 * 1024
            || self.deadline_ms == 0
            || self.deadline_ms > 86_400_000
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "resource policy is zero or exceeds a hard ceiling",
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineDefinition {
    source: ProceduralSource,
    arguments: Vec<RoutineArgument>,
    result: RoutineResult,
    volatility: Volatility,
    security: SecurityMode,
    search_path: Vec<ObjectId>,
    dependency_ids: Vec<ObjectId>,
    definition_version: u32,
    compiler_abi: u32,
    runtime_abi: u32,
    resource_policy: ResourcePolicy,
}

impl RoutineDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: ProceduralSource,
        arguments: Vec<RoutineArgument>,
        result: RoutineResult,
        volatility: Volatility,
        security: SecurityMode,
        search_path: Vec<ObjectId>,
        dependency_ids: Vec<ObjectId>,
        definition_version: u32,
        compiler_abi: u32,
        runtime_abi: u32,
        resource_policy: ResourcePolicy,
    ) -> CatalogResult<Self> {
        if arguments.len() > MAX_ROUTINE_ARGUMENTS
            || definition_version == 0
            || compiler_abi == 0
            || runtime_abi == 0
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "routine argument count or version/ABI is invalid",
            });
        }
        if matches!(&result, RoutineResult::Table(columns) if columns.is_empty() || columns.len() > MAX_RESULT_COLUMNS)
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "table result column count is invalid",
            });
        }
        let mut argument_names = std::collections::BTreeSet::new();
        let mut default_seen = false;
        for argument in &arguments {
            if !argument_names.insert(argument.name().normalized().as_str().to_owned()) {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "duplicate routine argument name",
                });
            }
            if argument.mode() != ArgumentMode::Out {
                if argument.default_sql().is_some() {
                    default_seen = true;
                } else if default_seen {
                    return Err(CatalogError::InvalidCatalogFormat {
                        detail: "required routine argument follows defaulted argument",
                    });
                }
            }
        }
        if let RoutineResult::Table(columns) = &result {
            let mut names = std::collections::BTreeSet::new();
            if columns
                .iter()
                .any(|column| !names.insert(column.name().normalized().as_str().to_owned()))
            {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "duplicate routine result column name",
                });
            }
        }
        let search_path = ordered_unique_ids("routine.search_path", search_path, true)?;
        if search_path.len() > 64 {
            return Err(CatalogError::CatalogLimitExceeded {
                field: "routine search path",
                actual: search_path.len() as u64,
                limit: 64,
            });
        }
        let dependency_ids = crate::payload::common::sorted_unique_ids(
            "routine.dependency_ids",
            dependency_ids,
            true,
        )?;
        if dependency_ids.iter().any(|id| search_path.contains(id)) {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "routine dependency duplicates a search-path namespace",
            });
        }
        Ok(Self {
            source,
            arguments,
            result,
            volatility,
            security,
            search_path,
            dependency_ids,
            definition_version,
            compiler_abi,
            runtime_abi,
            resource_policy: resource_policy.validate()?,
        })
    }
    pub fn source(&self) -> &ProceduralSource {
        &self.source
    }
    pub fn arguments(&self) -> &[RoutineArgument] {
        &self.arguments
    }
    pub fn result(&self) -> &RoutineResult {
        &self.result
    }
    pub const fn volatility(&self) -> Volatility {
        self.volatility
    }
    pub const fn security(&self) -> SecurityMode {
        self.security
    }
    pub fn search_path(&self) -> &[ObjectId] {
        &self.search_path
    }
    pub fn dependency_ids(&self) -> &[ObjectId] {
        &self.dependency_ids
    }
    pub const fn language_kind(&self) -> LanguageKind {
        LanguageKind::RadixPl
    }
    pub const fn language_version(&self) -> u16 {
        PROCEDURAL_LANGUAGE_VERSION
    }
    pub const fn definition_version(&self) -> u32 {
        self.definition_version
    }
    pub const fn compiler_abi(&self) -> u32 {
        self.compiler_abi
    }
    pub const fn runtime_abi(&self) -> u32 {
        self.runtime_abi
    }
    pub const fn resource_policy(&self) -> ResourcePolicy {
        self.resource_policy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFunctionDefinition {
    arguments: Vec<RoutineArgument>,
    result: RoutineResult,
    volatility: Volatility,
    semantic_revision: u32,
    dependency_ids: Vec<ObjectId>,
    extension_binding_id: ObjectId,
    local_id: String,
    strict: bool,
    parallel_safe: bool,
    cost: u32,
    cancellation_kind: u16,
    batch: bool,
    max_output_bytes: u32,
}

impl NativeFunctionDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        arguments: Vec<RoutineArgument>,
        result: RoutineResult,
        volatility: Volatility,
        semantic_revision: u32,
        dependency_ids: Vec<ObjectId>,
        extension_binding_id: ObjectId,
        local_id: impl Into<String>,
        strict: bool,
        parallel_safe: bool,
        cost: u32,
        cancellation_kind: u16,
        batch: bool,
        max_output_bytes: u32,
    ) -> CatalogResult<Self> {
        let local_id = local_id.into();
        if arguments.len() > MAX_ROUTINE_ARGUMENTS
            || arguments.iter().any(|argument| {
                argument.mode() != ArgumentMode::In || argument.default_sql().is_some()
            })
            || !matches!(result, RoutineResult::Scalar { .. })
            || semantic_revision == 0
            || local_id.is_empty()
            || local_id.len() > 255
            || local_id.contains('\0')
            || cost == 0
            || cost > 1_000_000
            || cancellation_kind != 1
            || max_output_bytes == 0
            || max_output_bytes > 16 * 1024 * 1024
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "native function descriptor is invalid or exceeds a hard limit",
            });
        }
        if strict && arguments.iter().any(RoutineArgument::nullable) {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "native function argument nullability disagrees with strictness",
            });
        }
        let dependency_ids = crate::payload::common::sorted_unique_ids(
            "native_function.dependency_ids",
            dependency_ids,
            true,
        )?;
        if !dependency_ids.contains(&extension_binding_id) {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "native function dependencies omit extension binding",
            });
        }
        Ok(Self {
            arguments,
            result,
            volatility,
            semantic_revision,
            dependency_ids,
            extension_binding_id,
            local_id,
            strict,
            parallel_safe,
            cost,
            cancellation_kind,
            batch,
            max_output_bytes,
        })
    }

    pub fn arguments(&self) -> &[RoutineArgument] {
        &self.arguments
    }
    pub const fn result(&self) -> &RoutineResult {
        &self.result
    }
    pub const fn volatility(&self) -> Volatility {
        self.volatility
    }
    pub const fn semantic_revision(&self) -> u32 {
        self.semantic_revision
    }
    pub fn dependency_ids(&self) -> &[ObjectId] {
        &self.dependency_ids
    }
    pub const fn extension_binding_id(&self) -> ObjectId {
        self.extension_binding_id
    }
    pub fn local_id(&self) -> &str {
        &self.local_id
    }
    pub const fn strict(&self) -> bool {
        self.strict
    }
    pub const fn parallel_safe(&self) -> bool {
        self.parallel_safe
    }
    pub const fn cost(&self) -> u32 {
        self.cost
    }
    pub const fn cancellation_kind(&self) -> u16 {
        self.cancellation_kind
    }
    pub const fn batch(&self) -> bool {
        self.batch
    }
    pub const fn max_output_bytes(&self) -> u32 {
        self.max_output_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionPayload {
    Procedural(RoutineDefinition),
    Native(NativeFunctionDefinition),
}
impl FunctionPayload {
    pub fn new(definition: RoutineDefinition) -> CatalogResult<Self> {
        if definition
            .arguments
            .iter()
            .any(|argument| argument.mode != ArgumentMode::In)
            || matches!(definition.result, RoutineResult::Void)
            || (matches!(definition.result, RoutineResult::Trigger)
                && definition.volatility != Volatility::Volatile)
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "function requires IN-only arguments and a non-VOID result",
            });
        }
        Ok(Self::Procedural(definition))
    }
    pub fn from_fields(
        version: u16,
        flags: u64,
        definition: RoutineDefinition,
    ) -> CatalogResult<Self> {
        validate_version("function", version)?;
        validate_flags("function", flags)?;
        Self::new(definition)
    }
    pub fn new_native(definition: NativeFunctionDefinition) -> Self {
        Self::Native(definition)
    }
    pub const fn procedural_definition(&self) -> Option<&RoutineDefinition> {
        match self {
            Self::Procedural(definition) => Some(definition),
            Self::Native(_) => None,
        }
    }
    pub const fn native_definition(&self) -> Option<&NativeFunctionDefinition> {
        match self {
            Self::Procedural(_) => None,
            Self::Native(definition) => Some(definition),
        }
    }
    pub fn arguments(&self) -> &[RoutineArgument] {
        match self {
            Self::Procedural(definition) => definition.arguments(),
            Self::Native(definition) => definition.arguments(),
        }
    }
    pub fn result(&self) -> &RoutineResult {
        match self {
            Self::Procedural(definition) => definition.result(),
            Self::Native(definition) => definition.result(),
        }
    }
    pub const fn volatility(&self) -> Volatility {
        match self {
            Self::Procedural(definition) => definition.volatility(),
            Self::Native(definition) => definition.volatility(),
        }
    }
    pub fn dependency_ids(&self) -> &[ObjectId] {
        match self {
            Self::Procedural(definition) => definition.dependency_ids(),
            Self::Native(definition) => definition.dependency_ids(),
        }
    }
    pub const fn language_kind(&self) -> LanguageKind {
        match self {
            Self::Procedural(_) => LanguageKind::RadixPl,
            Self::Native(_) => LanguageKind::Native,
        }
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcedurePayload(RoutineDefinition);
impl ProcedurePayload {
    pub fn new(definition: RoutineDefinition) -> CatalogResult<Self> {
        let has_output_arguments = definition
            .arguments
            .iter()
            .any(|argument| argument.mode != ArgumentMode::In);
        if matches!(definition.result, RoutineResult::Trigger)
            || (has_output_arguments && !matches!(definition.result, RoutineResult::Void))
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "procedure result cannot be TRIGGER or mix OUT/INOUT with RETURNS",
            });
        }
        Ok(Self(definition))
    }
    pub fn from_fields(
        version: u16,
        flags: u64,
        definition: RoutineDefinition,
    ) -> CatalogResult<Self> {
        validate_version("procedure", version)?;
        validate_flags("procedure", flags)?;
        Self::new(definition)
    }
    pub const fn definition(&self) -> &RoutineDefinition {
        &self.0
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}
