use std::fmt;

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    InvalidObjectIdHex,
    ZeroObjectId,
    ReservedObjectId {
        hex: String,
    },
    UnknownObjectKind {
        tag: u16,
    },
    ReservedObjectKind {
        tag: u16,
        name: &'static str,
    },
    UnsupportedCatalogMinor {
        major: u16,
        minor: u16,
    },
    ObjectKindRequiresCatalogMinor {
        tag: u16,
        name: &'static str,
        required_minor: u16,
        actual_minor: u16,
    },
    EmptyName,
    EmbeddedNameNul,
    DisplayNameTooLong {
        actual: usize,
        limit: usize,
    },
    NormalizedNameTooLong {
        actual: usize,
        limit: usize,
    },
    NonCanonicalNormalizedName,
    UnsupportedPayloadVersion {
        kind: &'static str,
        version: u16,
    },
    UnknownPayloadFlags {
        kind: &'static str,
        flags: u64,
    },
    EmptyCanonicalSql {
        field: &'static str,
    },
    EmbeddedSqlNul {
        field: &'static str,
    },
    CanonicalSqlTooLong {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    EmptyObjectIdList {
        field: &'static str,
    },
    TooManyObjectIds {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    DuplicateObjectId {
        field: &'static str,
        id: String,
    },
    InvalidDataTypeDescriptor {
        detail: &'static str,
    },
    InvalidColumnPayload {
        detail: &'static str,
    },
    InvalidTablePayload {
        detail: &'static str,
    },
    InvalidConstraintPayload {
        detail: &'static str,
    },
    InvalidIndexPayload {
        detail: &'static str,
    },
    InvalidExtensionPayload {
        detail: &'static str,
    },
    InvalidExternalTypePayload {
        detail: &'static str,
    },
    InvalidOperatorPayload {
        detail: &'static str,
    },
    InvalidOperatorClassPayload {
        detail: &'static str,
    },
    InvalidPlannerSupportPayload {
        detail: &'static str,
    },
    UnknownPayloadEnumTag {
        owner: &'static str,
        tag: u16,
    },
    UnknownEdgeKind {
        tag: u16,
    },
    ReservedEdgeKind {
        tag: u16,
        name: &'static str,
    },
    UnsupportedEdgeVersion {
        version: u16,
    },
    UnknownEdgeFlags {
        flags: u32,
    },
    DuplicateCatalogObject {
        id: String,
    },
    DuplicateCatalogName {
        name: String,
    },
    InvalidCatalogObject {
        id: String,
        detail: &'static str,
    },
    PayloadKindMismatch {
        id: String,
        header: &'static str,
        payload: &'static str,
    },
    MissingCatalogObject {
        role: &'static str,
        id: String,
    },
    DuplicateCatalogEdge {
        source: String,
        target: String,
        kind: &'static str,
        ordinal: u32,
    },
    IllegalCatalogEdge {
        source: String,
        target: String,
        kind: &'static str,
        detail: &'static str,
    },
    CatalogDependencyCycle {
        id: String,
    },
    CatalogDependencyDepthExceeded {
        id: String,
        depth: usize,
        limit: usize,
    },
    InvalidCatalogFormat {
        detail: &'static str,
    },
    CatalogLimitExceeded {
        field: &'static str,
        actual: u64,
        limit: u64,
    },
    CatalogChecksumMismatch {
        scope: &'static str,
    },
    InvalidCatalogUtf8 {
        field: &'static str,
    },
    NonCanonicalCatalogEncoding {
        detail: &'static str,
    },
    InvalidCatalogPublication {
        detail: &'static str,
    },
    CatalogPublicationUnavailable,
    InvalidCatalogMutation {
        detail: &'static str,
    },
    CatalogMutationLimitExceeded {
        field: &'static str,
        actual: usize,
        limit: usize,
    },
    StaleCatalogMutation {
        field: &'static str,
        expected: String,
        actual: String,
    },
    CatalogObjectPreconditionFailed {
        id: String,
        detail: &'static str,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObjectIdHex => {
                formatter.write_str("object ID must contain exactly 32 hexadecimal digits")
            }
            Self::ZeroObjectId => formatter.write_str("all-zero object ID is not an identity"),
            Self::ReservedObjectId { hex } => {
                write!(
                    formatter,
                    "object ID {hex} belongs to the reserved V6 range"
                )
            }
            Self::UnknownObjectKind { tag } => write!(formatter, "unknown object kind tag {tag}"),
            Self::ReservedObjectKind { tag, name } => write!(
                formatter,
                "object kind {name} ({tag}) is reserved and not admitted"
            ),
            Self::UnsupportedCatalogMinor { major, minor } => {
                write!(formatter, "unsupported catalog format {major}.{minor}")
            }
            Self::ObjectKindRequiresCatalogMinor {
                tag,
                name,
                required_minor,
                actual_minor,
            } => write!(
                formatter,
                "object kind {name} ({tag}) requires catalog 6.{required_minor}, got 6.{actual_minor}"
            ),
            Self::EmptyName => formatter.write_str("catalog name cannot be empty"),
            Self::EmbeddedNameNul => formatter.write_str("catalog name contains an embedded NUL"),
            Self::DisplayNameTooLong { actual, limit } => write!(
                formatter,
                "display name has {actual} UTF-8 bytes; V6 limit is {limit}"
            ),
            Self::NormalizedNameTooLong { actual, limit } => write!(
                formatter,
                "normalized name has {actual} UTF-8 bytes; V6 limit is {limit}"
            ),
            Self::NonCanonicalNormalizedName => {
                formatter.write_str("stored normalized name is not canonical for its display name")
            }
            Self::UnsupportedPayloadVersion { kind, version } => {
                write!(formatter, "unsupported {kind} payload version {version}")
            }
            Self::UnknownPayloadFlags { kind, flags } => {
                write!(formatter, "unknown {kind} payload flags 0x{flags:016x}")
            }
            Self::EmptyCanonicalSql { field } => {
                write!(formatter, "canonical SQL field {field} cannot be empty")
            }
            Self::EmbeddedSqlNul { field } => {
                write!(
                    formatter,
                    "canonical SQL field {field} contains an embedded NUL"
                )
            }
            Self::CanonicalSqlTooLong {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "canonical SQL field {field} has {actual} UTF-8 bytes; V6 limit is {limit}"
            ),
            Self::EmptyObjectIdList { field } => {
                write!(formatter, "object ID list {field} cannot be empty")
            }
            Self::TooManyObjectIds {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "object ID list {field} has {actual} entries; V6 limit is {limit}"
            ),
            Self::DuplicateObjectId { field, id } => {
                write!(formatter, "object ID list {field} contains duplicate {id}")
            }
            Self::InvalidDataTypeDescriptor { detail } => {
                write!(formatter, "invalid data type descriptor: {detail}")
            }
            Self::InvalidColumnPayload { detail } => {
                write!(formatter, "invalid column payload: {detail}")
            }
            Self::InvalidTablePayload { detail } => {
                write!(formatter, "invalid table payload: {detail}")
            }
            Self::InvalidConstraintPayload { detail } => {
                write!(formatter, "invalid constraint payload: {detail}")
            }
            Self::InvalidIndexPayload { detail } => {
                write!(formatter, "invalid index payload: {detail}")
            }
            Self::InvalidExtensionPayload { detail } => {
                write!(formatter, "invalid extension payload: {detail}")
            }
            Self::InvalidExternalTypePayload { detail } => {
                write!(formatter, "invalid external type payload: {detail}")
            }
            Self::InvalidOperatorPayload { detail } => {
                write!(formatter, "invalid operator payload: {detail}")
            }
            Self::InvalidOperatorClassPayload { detail } => {
                write!(formatter, "invalid operator class payload: {detail}")
            }
            Self::InvalidPlannerSupportPayload { detail } => {
                write!(formatter, "invalid planner support payload: {detail}")
            }
            Self::UnknownPayloadEnumTag { owner, tag } => {
                write!(formatter, "unknown {owner} tag {tag}")
            }
            Self::UnknownEdgeKind { tag } => write!(formatter, "unknown catalog edge kind {tag}"),
            Self::ReservedEdgeKind { tag, name } => write!(
                formatter,
                "catalog edge kind {name} ({tag}) is reserved and not admitted in V6.0"
            ),
            Self::UnsupportedEdgeVersion { version } => {
                write!(formatter, "unsupported catalog edge version {version}")
            }
            Self::UnknownEdgeFlags { flags } => {
                write!(formatter, "unknown catalog edge flags 0x{flags:08x}")
            }
            Self::DuplicateCatalogObject { id } => {
                write!(formatter, "duplicate catalog object ID {id}")
            }
            Self::DuplicateCatalogName { name } => {
                write!(formatter, "duplicate catalog name key {name}")
            }
            Self::InvalidCatalogObject { id, detail } => {
                write!(formatter, "invalid catalog object {id}: {detail}")
            }
            Self::PayloadKindMismatch {
                id,
                header,
                payload,
            } => write!(
                formatter,
                "catalog object {id} header kind {header} does not match payload kind {payload}"
            ),
            Self::MissingCatalogObject { role, id } => {
                write!(
                    formatter,
                    "missing catalog object {id} referenced as {role}"
                )
            }
            Self::DuplicateCatalogEdge {
                source,
                target,
                kind,
                ordinal,
            } => write!(
                formatter,
                "duplicate catalog edge {source} -{kind}[{ordinal}]-> {target}"
            ),
            Self::IllegalCatalogEdge {
                source,
                target,
                kind,
                detail,
            } => write!(
                formatter,
                "illegal catalog edge {source} -{kind}-> {target}: {detail}"
            ),
            Self::CatalogDependencyCycle { id } => {
                write!(formatter, "catalog dependency cycle reaches object {id}")
            }
            Self::CatalogDependencyDepthExceeded { id, depth, limit } => write!(
                formatter,
                "catalog dependency depth at {id} is {depth}; V6 limit is {limit}"
            ),
            Self::InvalidCatalogFormat { detail } => {
                write!(formatter, "invalid V6 catalog format: {detail}")
            }
            Self::CatalogLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "V6 catalog field {field} is {actual}; hard limit is {limit}"
            ),
            Self::CatalogChecksumMismatch { scope } => {
                write!(formatter, "V6 catalog {scope} checksum mismatch")
            }
            Self::InvalidCatalogUtf8 { field } => {
                write!(formatter, "V6 catalog field {field} is not valid UTF-8")
            }
            Self::NonCanonicalCatalogEncoding { detail } => {
                write!(formatter, "non-canonical V6 catalog encoding: {detail}")
            }
            Self::InvalidCatalogPublication { detail } => {
                write!(formatter, "invalid catalog publication: {detail}")
            }
            Self::CatalogPublicationUnavailable => {
                formatter.write_str("catalog publication lock is unavailable")
            }
            Self::InvalidCatalogMutation { detail } => {
                write!(formatter, "invalid catalog mutation: {detail}")
            }
            Self::CatalogMutationLimitExceeded {
                field,
                actual,
                limit,
            } => write!(
                formatter,
                "catalog mutation {field} is {actual}; V6 limit is {limit}"
            ),
            Self::StaleCatalogMutation {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "stale catalog mutation: expected {field} {expected}, found {actual}"
            ),
            Self::CatalogObjectPreconditionFailed { id, detail } => {
                write!(
                    formatter,
                    "catalog object {id} precondition failed: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for CatalogError {}
