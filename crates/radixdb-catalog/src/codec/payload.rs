use std::collections::BTreeMap;

use radixdb_core::DataType;

use super::primitives::{
    enforce_limit, put_bytes, put_u16, put_u32, read_array, read_u16, read_u32, read_u64,
    FIELD_ENTRY_BYTES, MAX_FIELDS_PER_OBJECT, MAX_PAYLOAD_BYTES_PER_OBJECT, PAYLOAD_HEADER_BYTES,
    PAYLOAD_MAGIC,
};
use crate::{
    AccessMethod, AclEntryPayload, ArgumentMode, CatalogDataType, CatalogError, CatalogName,
    CatalogPayload, CatalogResult, ColumnPayload, ColumnPrivilegeSet, ConstraintKind,
    ConstraintPayload, CredentialVerifier, ExtensionPayload, ExternalStorageKind,
    ExternalTypePayload, ForeignKeyAction, ForeignKeyMatch, FunctionPayload, HnswDistanceMetric,
    HnswParameters, IndexPayload, JobArgument, JobPayload, JobSchedule, LanguageKind,
    NamespacePayload, NativeFunctionDefinition, ObjectId, ObjectKind, OperatorBinding,
    OperatorClassPayload, OperatorPayload, PlannerRecheckPolicy, PlannerSupportPayload,
    PrincipalPayload, ProceduralSource, ProcedurePayload, ResourcePolicy, ResultColumn,
    RolePayload, RoutineArgument, RoutineDefinition, RoutineResult, SecurityMode, TablePayload,
    TablePayloadFields, TriggerLevel, TriggerPayload, TriggerTiming, ViewPayload, Volatility,
    NATIVE_LANGUAGE_VERSION, PAYLOAD_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum FieldType {
    U8 = 1,
    U16 = 2,
    U32 = 3,
    U64 = 4,
    Bool = 5,
    ObjectId = 6,
    ObjectIdArray = 7,
    Utf8 = 8,
    Bytes = 9,
    DataTypeDescriptor = 10,
}

impl TryFrom<u16> for FieldType {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::U8),
            2 => Ok(Self::U16),
            3 => Ok(Self::U32),
            4 => Ok(Self::U64),
            5 => Ok(Self::Bool),
            6 => Ok(Self::ObjectId),
            7 => Ok(Self::ObjectIdArray),
            8 => Ok(Self::Utf8),
            9 => Ok(Self::Bytes),
            10 => Ok(Self::DataTypeDescriptor),
            _ => Err(CatalogError::InvalidCatalogFormat {
                detail: "unknown catalog payload field type",
            }),
        }
    }
}

struct EncodedField {
    tag: u16,
    kind: FieldType,
    optional: bool,
    count: u32,
    bytes: Vec<u8>,
}

impl EncodedField {
    fn scalar(tag: u16, kind: FieldType, optional: bool, bytes: Vec<u8>) -> Self {
        Self {
            tag,
            kind,
            optional,
            count: 1,
            bytes,
        }
    }

    fn object_ids(tag: u16, optional: bool, ids: &[ObjectId]) -> CatalogResult<Self> {
        let capacity = ids
            .len()
            .checked_mul(16)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "object ID array byte length overflow",
            })?;
        let mut bytes = Vec::with_capacity(capacity);
        for id in ids {
            bytes.extend_from_slice(id.as_bytes());
        }
        Ok(Self {
            tag,
            kind: FieldType::ObjectIdArray,
            optional,
            count: u32::try_from(ids.len()).map_err(|_| CatalogError::CatalogLimitExceeded {
                field: "payload object ID array count",
                actual: ids.len() as u64,
                limit: u32::MAX as u64,
            })?,
            bytes,
        })
    }

    fn variable(tag: u16, kind: FieldType, optional: bool, bytes: Vec<u8>) -> CatalogResult<Self> {
        let count = u32::try_from(bytes.len()).map_err(|_| CatalogError::CatalogLimitExceeded {
            field: "payload variable field bytes",
            actual: bytes.len() as u64,
            limit: u32::MAX as u64,
        })?;
        Ok(Self {
            tag,
            kind,
            optional,
            count,
            bytes,
        })
    }
}

pub fn encode_payload(payload: &CatalogPayload) -> CatalogResult<Vec<u8>> {
    encode_payload_fields(payload.kind(), payload.version(), payload_fields(payload)?)
}

fn encode_payload_fields(
    kind: ObjectKind,
    version: u16,
    mut fields: Vec<EncodedField>,
) -> CatalogResult<Vec<u8>> {
    fields.sort_unstable_by_key(|field| field.tag);
    let directory_length =
        fields
            .len()
            .checked_mul(FIELD_ENTRY_BYTES)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "payload field directory overflow",
            })?;
    let values_length = fields.iter().try_fold(0_usize, |total, field| {
        total
            .checked_add(field.bytes.len())
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "payload values length overflow",
            })
    })?;
    let body_length =
        directory_length
            .checked_add(values_length)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "payload body length overflow",
            })?;
    let total_length = PAYLOAD_HEADER_BYTES.checked_add(body_length).ok_or(
        CatalogError::InvalidCatalogFormat {
            detail: "payload total length overflow",
        },
    )?;
    enforce_limit(
        "catalog payload bytes per object",
        total_length as u64,
        MAX_PAYLOAD_BYTES_PER_OBJECT,
    )?;

    let mut output = vec![0_u8; total_length];
    put_bytes(&mut output, 0, PAYLOAD_MAGIC)?;
    put_u16(&mut output, 4, kind.tag())?;
    put_u16(&mut output, 6, version)?;
    put_u32(&mut output, 8, PAYLOAD_HEADER_BYTES as u32)?;
    put_u32(&mut output, 12, body_length as u32)?;
    put_u32(&mut output, 16, fields.len() as u32)?;
    put_u32(&mut output, 20, directory_length as u32)?;

    let mut value_offset = directory_length;
    for (index, field) in fields.iter().enumerate() {
        let entry = PAYLOAD_HEADER_BYTES + index * FIELD_ENTRY_BYTES;
        put_u16(&mut output, entry, field.tag)?;
        put_u16(&mut output, entry + 2, field.kind as u16)?;
        put_u32(&mut output, entry + 4, u32::from(field.optional))?;
        super::primitives::put_u64(&mut output, entry + 8, value_offset as u64)?;
        put_u32(&mut output, entry + 16, field.bytes.len() as u32)?;
        put_u32(&mut output, entry + 20, field.count)?;
        let start = PAYLOAD_HEADER_BYTES + value_offset;
        put_bytes(&mut output, start, &field.bytes)?;
        value_offset += field.bytes.len();
    }
    let body_crc = radixdb_core::crc32_ieee(&output[PAYLOAD_HEADER_BYTES..]);
    put_u32(&mut output, 28, body_crc)?;
    Ok(output)
}

fn payload_fields(payload: &CatalogPayload) -> CatalogResult<Vec<EncodedField>> {
    let fields = match payload {
        CatalogPayload::Namespace(payload) => vec![field_u64(1, false, payload.flags())],
        CatalogPayload::Table(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                EncodedField::object_ids(2, false, payload.column_ids())?,
                EncodedField::object_ids(3, false, payload.constraint_ids())?,
                EncodedField::object_ids(4, false, payload.index_ids())?,
                field_u64(6, false, payload.created_unix_ns()),
                field_u64(7, false, payload.updated_unix_ns()),
            ];
            if let Some(id) = payload.primary_key_constraint_id() {
                fields.push(field_object_id(5, true, id));
            }
            fields
        }
        CatalogPayload::Column(payload) => {
            let mut fields = vec![
                field_u32(1, false, payload.ordinal()),
                field_data_type(2, payload.data_type()),
                field_bool(3, false, payload.nullable()),
                field_u64(6, false, payload.flags()),
            ];
            if let Some(sql) = payload.default_sql() {
                fields.push(EncodedField::variable(
                    4,
                    FieldType::Utf8,
                    true,
                    sql.as_str().as_bytes().to_vec(),
                )?);
            }
            if let Some(sql) = payload.generated_sql() {
                fields.push(EncodedField::variable(
                    5,
                    FieldType::Utf8,
                    true,
                    sql.as_str().as_bytes().to_vec(),
                )?);
            }
            fields
        }
        CatalogPayload::Constraint(payload) => constraint_fields(payload)?,
        CatalogPayload::Index(payload) => {
            let mut fields = vec![
                field_u16(1, false, payload.access_method().tag()),
                field_bool(2, false, payload.unique()),
                EncodedField::object_ids(4, false, payload.include_column_ids())?,
                field_u64(7, false, payload.flags()),
            ];
            if !payload.key_column_ids().is_empty() {
                fields.push(EncodedField::object_ids(3, true, payload.key_column_ids())?);
            }
            if let Some(sql) = payload.expression_sql() {
                fields.push(EncodedField::variable(
                    5,
                    FieldType::Utf8,
                    true,
                    sql.as_str().as_bytes().to_vec(),
                )?);
            }
            if let Some(sql) = payload.predicate_sql() {
                fields.push(EncodedField::variable(
                    6,
                    FieldType::Utf8,
                    true,
                    sql.as_str().as_bytes().to_vec(),
                )?);
            }
            if let Some(parameters) = payload.hnsw_parameters() {
                fields.push(field_u16(8, true, parameters.m()));
                fields.push(field_u16(9, true, parameters.ef_construction()));
                fields.push(field_u16(10, true, parameters.ef_search()));
                fields.push(field_u16(11, true, parameters.distance_metric().tag()));
            }
            if let Some(operator_class_id) = payload.operator_class_id() {
                fields.push(field_object_id(12, true, operator_class_id));
            }
            fields
        }
        CatalogPayload::View(payload) => vec![
            EncodedField::variable(
                1,
                FieldType::Utf8,
                false,
                payload.canonical_sql().as_str().as_bytes().to_vec(),
            )?,
            EncodedField::object_ids(2, false, payload.dependency_ids())?,
            EncodedField::variable(
                3,
                FieldType::Bytes,
                false,
                payload.output_signature().to_vec(),
            )?,
            field_u64(4, false, payload.flags()),
        ],
        CatalogPayload::Principal(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_bool(2, false, payload.login_enabled()),
                field_bool(3, false, payload.system()),
            ];
            if let Some(credential) = payload.credential() {
                fields.push(field_u16(4, true, credential.scheme()));
                fields.push(EncodedField::variable(
                    5,
                    FieldType::Bytes,
                    true,
                    credential.encoded().to_vec(),
                )?);
            }
            fields
        }
        CatalogPayload::Role(payload) => vec![
            field_u64(1, false, payload.flags()),
            field_bool(2, false, payload.inheritable()),
            field_bool(3, true, payload.enabled()),
        ],
        CatalogPayload::AclEntry(payload) => acl_fields(payload)?,
        CatalogPayload::Function(FunctionPayload::Procedural(definition)) => {
            routine_fields(definition, 0)?
        }
        CatalogPayload::Function(FunctionPayload::Native(definition)) => {
            native_function_fields(definition)?
        }
        CatalogPayload::Procedure(payload) => {
            routine_fields(payload.definition(), payload.flags())?
        }
        CatalogPayload::Trigger(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_object_id(2, false, payload.table_id()),
                field_object_id(3, false, payload.function_id()),
                field_u16(4, false, payload.timing().tag()),
                field_u16(5, false, payload.events()),
                field_u16(6, false, payload.level().tag()),
                EncodedField::object_ids(7, false, payload.update_column_ids())?,
                field_u32(8, false, payload.priority() as u32),
            ];
            if let Some(sql) = payload.when_sql() {
                fields.push(EncodedField::variable(
                    9,
                    FieldType::Utf8,
                    true,
                    sql.as_str().as_bytes().to_vec(),
                )?);
            }
            fields
        }
        CatalogPayload::Job(payload) => {
            let (schedule_kind, schedule_value) = match payload.schedule() {
                JobSchedule::AtUnixNs(value) => (1, value as u64),
                JobSchedule::EveryNs(value) => (2, value),
            };
            vec![
                field_u64(1, false, payload.flags()),
                field_object_id(2, false, payload.procedure_id()),
                field_object_id(3, false, payload.principal_id()),
                field_u16(4, false, schedule_kind),
                field_u64(5, false, schedule_value),
                EncodedField::variable(
                    6,
                    FieldType::Bytes,
                    false,
                    encode_job_arguments(payload.arguments())?,
                )?,
                field_bool(7, false, payload.enabled()),
                field_u32(8, false, payload.definition_version()),
                EncodedField::variable(
                    9,
                    FieldType::Bytes,
                    false,
                    encode_resource_policy(payload.resource_policy()),
                )?,
            ]
        }
        CatalogPayload::Extension(payload) => vec![
            field_u64(1, false, 0),
            field_object_id(2, false, payload.package_id()),
            EncodedField::variable(
                3,
                FieldType::Utf8,
                false,
                payload.version().as_bytes().to_vec(),
            )?,
            field_u16(4, false, payload.abi_major()),
            field_u16(5, false, payload.abi_min_minor()),
            field_u16(6, false, payload.abi_max_minor()),
            EncodedField::variable(
                7,
                FieldType::Bytes,
                false,
                payload.descriptor_fingerprint().to_vec(),
            )?,
        ],
        CatalogPayload::ExternalType(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_object_id(2, false, payload.extension_binding_id()),
                EncodedField::variable(
                    3,
                    FieldType::Utf8,
                    false,
                    payload.local_id().as_bytes().to_vec(),
                )?,
                field_u32(4, false, payload.write_codec_version()),
                field_u32(5, false, payload.semantic_revision()),
                field_u16(6, false, payload.storage_kind().tag()),
                field_u32(8, false, payload.max_canonical_payload_bytes()),
                EncodedField::variable(
                    9,
                    FieldType::Bytes,
                    false,
                    payload.codec_fingerprint().to_vec(),
                )?,
                field_u64(10, false, payload.capabilities()),
            ];
            if let Some(fixed_bytes) = payload.fixed_bytes() {
                fields.push(field_u32(7, true, fixed_bytes));
            }
            fields
        }
        CatalogPayload::Operator(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_object_id(2, false, payload.extension_binding_id()),
                EncodedField::variable(
                    3,
                    FieldType::Utf8,
                    false,
                    payload.local_id().as_bytes().to_vec(),
                )?,
                field_u32(4, false, payload.semantic_revision()),
                EncodedField::variable(
                    5,
                    FieldType::Utf8,
                    false,
                    payload.symbol().as_bytes().to_vec(),
                )?,
                field_data_type(8, payload.result_type()),
                field_object_id(9, false, payload.backing_function_id()),
            ];
            if let Some(data_type) = payload.left_argument() {
                fields.push(field_data_type_optional(6, data_type));
            }
            if let Some(data_type) = payload.right_argument() {
                fields.push(field_data_type_optional(7, data_type));
            }
            fields
        }
        CatalogPayload::OperatorClass(payload) => vec![
            field_u64(1, false, payload.flags()),
            field_object_id(2, false, payload.extension_binding_id()),
            EncodedField::variable(
                3,
                FieldType::Utf8,
                false,
                payload.local_id().as_bytes().to_vec(),
            )?,
            field_u32(4, false, payload.semantic_revision()),
            field_u16(5, false, payload.access_method().tag()),
            field_data_type(6, payload.input_type()),
            field_data_type(7, payload.key_type()),
            EncodedField::variable(
                8,
                FieldType::Bytes,
                false,
                encode_operator_bindings(payload.strategies())?,
            )?,
            EncodedField::variable(
                9,
                FieldType::Bytes,
                false,
                encode_operator_bindings(payload.supports())?,
            )?,
            field_u32(10, false, payload.key_codec_revision()),
            EncodedField::variable(11, FieldType::Bytes, false, payload.fingerprint().to_vec())?,
        ],
        CatalogPayload::PlannerSupport(payload) => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_object_id(2, false, payload.extension_binding_id()),
                EncodedField::variable(
                    3,
                    FieldType::Utf8,
                    false,
                    payload.local_id().as_bytes().to_vec(),
                )?,
                field_u32(4, false, payload.semantic_revision()),
                field_u32(7, false, payload.max_spans()),
                field_u32(8, false, payload.max_output_bytes()),
                field_u16(9, false, payload.recheck_policy().tag()),
                EncodedField::variable(
                    10,
                    FieldType::Bytes,
                    false,
                    payload.fingerprint().to_vec(),
                )?,
            ];
            if let Some(id) = payload.target_function_id() {
                fields.push(field_object_id(5, true, id));
            }
            if let Some(id) = payload.target_operator_class_id() {
                fields.push(field_object_id(6, true, id));
            }
            fields
        }
    };
    Ok(fields)
}

fn acl_fields(payload: &AclEntryPayload) -> CatalogResult<Vec<EncodedField>> {
    Ok(match payload {
        AclEntryPayload::ObjectPrivileges {
            grantor_principal_id,
            privileges,
            grant_option,
            columns,
            column_grant_options,
        } => {
            let mut fields = vec![
                field_u64(1, false, payload.flags()),
                field_u16(2, false, 1),
                field_object_id(3, false, *grantor_principal_id),
                field_u64(4, false, *privileges),
                field_u64(5, false, *grant_option),
                EncodedField::variable(
                    6,
                    FieldType::Bytes,
                    false,
                    encode_column_privileges(columns)?,
                )?,
            ];
            fields.push(EncodedField::variable(
                8,
                FieldType::Bytes,
                true,
                encode_column_privileges(column_grant_options)?,
            )?);
            fields
        }
        AclEntryPayload::RoleMembership {
            grantor_principal_id,
            admin_option,
        } => vec![
            field_u64(1, false, payload.flags()),
            field_u16(2, false, 2),
            field_object_id(3, false, *grantor_principal_id),
            field_bool(7, false, *admin_option),
        ],
    })
}

fn routine_fields(definition: &RoutineDefinition, flags: u64) -> CatalogResult<Vec<EncodedField>> {
    Ok(vec![
        field_u64(1, false, flags),
        EncodedField::variable(
            2,
            FieldType::Utf8,
            false,
            definition.source().as_str().as_bytes().to_vec(),
        )?,
        EncodedField::variable(
            3,
            FieldType::Bytes,
            false,
            definition.source().digest().to_vec(),
        )?,
        EncodedField::variable(
            4,
            FieldType::Bytes,
            false,
            encode_arguments(definition.arguments())?,
        )?,
        EncodedField::variable(
            5,
            FieldType::Bytes,
            false,
            encode_result(definition.result())?,
        )?,
        field_u16(6, false, definition.volatility().tag()),
        field_u16(7, false, definition.security().tag()),
        EncodedField::object_ids(8, false, definition.search_path())?,
        field_u32(9, false, definition.definition_version()),
        field_u32(10, false, definition.compiler_abi()),
        field_u32(11, false, definition.runtime_abi()),
        EncodedField::variable(
            12,
            FieldType::Bytes,
            false,
            encode_resource_policy(definition.resource_policy()),
        )?,
        EncodedField::object_ids(13, false, definition.dependency_ids())?,
        field_u16(14, false, definition.language_kind().tag()),
        field_u16(15, false, definition.language_version()),
    ])
}

fn native_function_fields(
    definition: &NativeFunctionDefinition,
) -> CatalogResult<Vec<EncodedField>> {
    Ok(vec![
        field_u64(1, false, 0),
        EncodedField::variable(
            4,
            FieldType::Bytes,
            false,
            encode_arguments(definition.arguments())?,
        )?,
        EncodedField::variable(
            5,
            FieldType::Bytes,
            false,
            encode_result(definition.result())?,
        )?,
        field_u16(6, false, definition.volatility().tag()),
        field_u16(7, false, SecurityMode::Invoker.tag()),
        field_u32(9, false, definition.semantic_revision()),
        EncodedField::object_ids(13, false, definition.dependency_ids())?,
        field_u16(14, false, LanguageKind::Native.tag()),
        field_u16(15, false, NATIVE_LANGUAGE_VERSION),
        field_object_id(16, false, definition.extension_binding_id()),
        EncodedField::variable(
            17,
            FieldType::Utf8,
            false,
            definition.local_id().as_bytes().to_vec(),
        )?,
        field_bool(18, false, definition.strict()),
        field_bool(19, false, definition.parallel_safe()),
        field_u32(20, false, definition.cost()),
        field_u16(21, false, definition.cancellation_kind()),
        field_bool(22, false, definition.batch()),
        field_u32(23, false, definition.max_output_bytes()),
    ])
}

fn constraint_fields(payload: &ConstraintPayload) -> CatalogResult<Vec<EncodedField>> {
    let mut fields = vec![
        field_u16(1, false, payload.kind().tag()),
        field_u64(9, false, payload.flags()),
    ];
    match payload {
        ConstraintPayload::PrimaryKey { local_column_ids }
        | ConstraintPayload::Unique { local_column_ids } => {
            fields.push(EncodedField::object_ids(2, false, local_column_ids)?);
        }
        ConstraintPayload::ForeignKey {
            local_column_ids,
            referenced_table_id,
            referenced_column_ids,
            match_action,
            on_update_action,
            on_delete_action,
        } => {
            fields.push(EncodedField::object_ids(2, false, local_column_ids)?);
            fields.push(field_object_id(3, false, *referenced_table_id));
            fields.push(EncodedField::object_ids(4, false, referenced_column_ids)?);
            fields.push(field_u16(6, false, match_action.tag()));
            fields.push(field_u16(7, false, on_update_action.tag()));
            fields.push(field_u16(8, false, on_delete_action.tag()));
        }
        ConstraintPayload::Check {
            local_column_id,
            check_sql,
        } => {
            if let Some(local_column_id) = local_column_id {
                fields.push(EncodedField::object_ids(2, true, &[*local_column_id])?);
            }
            fields.push(EncodedField::variable(
                5,
                FieldType::Utf8,
                false,
                check_sql.as_str().as_bytes().to_vec(),
            )?);
        }
        ConstraintPayload::NotNull { local_column_id } => {
            fields.push(EncodedField::object_ids(2, false, &[*local_column_id])?);
        }
    }
    Ok(fields)
}

fn field_u16(tag: u16, optional: bool, value: u16) -> EncodedField {
    EncodedField::scalar(tag, FieldType::U16, optional, value.to_le_bytes().to_vec())
}

fn field_u32(tag: u16, optional: bool, value: u32) -> EncodedField {
    EncodedField::scalar(tag, FieldType::U32, optional, value.to_le_bytes().to_vec())
}

fn field_u64(tag: u16, optional: bool, value: u64) -> EncodedField {
    EncodedField::scalar(tag, FieldType::U64, optional, value.to_le_bytes().to_vec())
}

fn field_bool(tag: u16, optional: bool, value: bool) -> EncodedField {
    EncodedField::scalar(tag, FieldType::Bool, optional, vec![u8::from(value)])
}

fn field_object_id(tag: u16, optional: bool, value: ObjectId) -> EncodedField {
    EncodedField::scalar(
        tag,
        FieldType::ObjectId,
        optional,
        value.as_bytes().to_vec(),
    )
}

fn field_data_type(tag: u16, value: CatalogDataType) -> EncodedField {
    EncodedField::scalar(
        tag,
        FieldType::DataTypeDescriptor,
        false,
        encode_data_type(value).to_vec(),
    )
}

fn field_data_type_optional(tag: u16, value: CatalogDataType) -> EncodedField {
    EncodedField::scalar(
        tag,
        FieldType::DataTypeDescriptor,
        true,
        encode_data_type(value).to_vec(),
    )
}

fn encode_operator_bindings(bindings: &[OperatorBinding]) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::with_capacity(4 + bindings.len() * 20);
    push_len(&mut output, bindings.len(), "operator-class binding count")?;
    for binding in bindings {
        output.extend_from_slice(&binding.slot().to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(binding.object_id().as_bytes());
    }
    Ok(output)
}

fn encode_data_type(value: CatalogDataType) -> [u8; 32] {
    let mut bytes = [0_u8; 32];
    bytes[0..2].copy_from_slice(&value.descriptor_marker().to_le_bytes());
    bytes[2..4].copy_from_slice(&value.descriptor_version().to_le_bytes());
    bytes[4..8].copy_from_slice(&value.flags().to_le_bytes());
    bytes[8..12].copy_from_slice(&value.parameter_1().to_le_bytes());
    bytes[12..16].copy_from_slice(&value.parameter_2().to_le_bytes());
    bytes[16..32].copy_from_slice(&value.collation_id());
    bytes
}

fn push_len(output: &mut Vec<u8>, value: usize, field: &'static str) -> CatalogResult<()> {
    let value = u32::try_from(value).map_err(|_| CatalogError::CatalogLimitExceeded {
        field,
        actual: value as u64,
        limit: u32::MAX as u64,
    })?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn encode_arguments(arguments: &[RoutineArgument]) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::new();
    push_len(&mut output, arguments.len(), "routine argument count")?;
    for argument in arguments {
        let name = argument.name().display().as_str().as_bytes();
        let default = argument.default_sql().map(|sql| sql.as_str().as_bytes());
        push_len(&mut output, name.len(), "routine argument name bytes")?;
        output.extend_from_slice(&argument.mode().tag().to_le_bytes());
        output.push(u8::from(argument.nullable()));
        output.push(0);
        output.extend_from_slice(&encode_data_type(argument.data_type()));
        match default {
            Some(default) => {
                push_len(&mut output, default.len(), "routine argument default bytes")?
            }
            None => output.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
        output.extend_from_slice(name);
        if let Some(default) = default {
            output.extend_from_slice(default);
        }
    }
    Ok(output)
}

fn encode_result(result: &RoutineResult) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::new();
    match result {
        RoutineResult::Void => output.extend_from_slice(&1_u16.to_le_bytes()),
        RoutineResult::Scalar {
            data_type,
            nullable,
        } => {
            output.extend_from_slice(&2_u16.to_le_bytes());
            output.extend_from_slice(&encode_data_type(*data_type));
            output.push(u8::from(*nullable));
        }
        RoutineResult::Table(columns) => {
            output.extend_from_slice(&3_u16.to_le_bytes());
            push_len(&mut output, columns.len(), "routine result column count")?;
            for column in columns {
                let name = column.name().display().as_str().as_bytes();
                push_len(&mut output, name.len(), "routine result column name bytes")?;
                output.extend_from_slice(&encode_data_type(column.data_type()));
                output.push(u8::from(column.nullable()));
                output.extend_from_slice(name);
            }
        }
        RoutineResult::Trigger => output.extend_from_slice(&4_u16.to_le_bytes()),
    }
    Ok(output)
}

fn encode_resource_policy(policy: ResourcePolicy) -> Vec<u8> {
    let mut output = Vec::with_capacity(52);
    output.extend_from_slice(&policy.instructions.to_le_bytes());
    output.extend_from_slice(&policy.heap_bytes.to_le_bytes());
    output.extend_from_slice(&policy.frames.to_le_bytes());
    output.extend_from_slice(&policy.sql_statements.to_le_bytes());
    output.extend_from_slice(&policy.rows.to_le_bytes());
    output.extend_from_slice(&policy.result_bytes.to_le_bytes());
    output.extend_from_slice(&policy.deadline_ms.to_le_bytes());
    output
}

fn encode_column_privileges(columns: &[ColumnPrivilegeSet]) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::new();
    push_len(
        &mut output,
        columns.len(),
        "ACL column privilege group count",
    )?;
    for column in columns {
        output.extend_from_slice(&column.privilege().to_le_bytes());
        push_len(
            &mut output,
            column.column_ids().len(),
            "ACL column ID count",
        )?;
        for id in column.column_ids() {
            output.extend_from_slice(id.as_bytes());
        }
    }
    Ok(output)
}

fn encode_job_arguments(arguments: &[JobArgument]) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::new();
    push_len(&mut output, arguments.len(), "job argument count")?;
    for argument in arguments {
        match argument.name() {
            Some(name) => push_len(
                &mut output,
                name.display().as_str().len(),
                "job argument name bytes",
            )?,
            None => output.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
        output.extend_from_slice(&encode_data_type(argument.data_type()));
        match argument.value() {
            Some(value) => push_len(&mut output, value.len(), "job argument literal bytes")?,
            None => output.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
        if let Some(name) = argument.name() {
            output.extend_from_slice(name.display().as_str().as_bytes());
        }
        if let Some(value) = argument.value() {
            output.extend_from_slice(value);
        }
    }
    Ok(output)
}

#[derive(Clone, Copy)]
struct DecodedField<'a> {
    kind: FieldType,
    optional: bool,
    count: u32,
    bytes: &'a [u8],
}

struct FieldSet<'a> {
    values: BTreeMap<u16, DecodedField<'a>>,
}

impl<'a> FieldSet<'a> {
    fn take(
        &mut self,
        tag: u16,
        kind: FieldType,
        optional: bool,
    ) -> CatalogResult<DecodedField<'a>> {
        let field = self
            .values
            .remove(&tag)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "required catalog payload field is missing",
            })?;
        validate_field_shape(field, kind, optional)?;
        Ok(field)
    }

    fn take_optional(
        &mut self,
        tag: u16,
        kind: FieldType,
    ) -> CatalogResult<Option<DecodedField<'a>>> {
        let Some(field) = self.values.remove(&tag) else {
            return Ok(None);
        };
        validate_field_shape(field, kind, true)?;
        Ok(Some(field))
    }

    fn finish(self) -> CatalogResult<()> {
        if !self.values.is_empty() {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "catalog payload contains an extra field tag",
            });
        }
        Ok(())
    }
}

pub fn decode_payload(
    input: &[u8],
    expected_kind: ObjectKind,
    expected_version: u16,
) -> CatalogResult<CatalogPayload> {
    enforce_limit(
        "catalog payload bytes per object",
        input.len() as u64,
        MAX_PAYLOAD_BYTES_PER_OBJECT,
    )?;
    if input.len() < PAYLOAD_HEADER_BYTES || &input[..4] != PAYLOAD_MAGIC {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload header/magic is invalid",
        });
    }
    let kind = ObjectKind::try_from(read_u16(input, 4)?)?;
    let version = read_u16(input, 6)?;
    let supported_version = if matches!(kind, ObjectKind::Function | ObjectKind::Index) {
        matches!(version, PAYLOAD_VERSION | 2)
    } else if matches!(
        kind,
        ObjectKind::Principal | ObjectKind::Role | ObjectKind::AclEntry
    ) {
        matches!(
            version,
            PAYLOAD_VERSION | crate::payload::SECURITY_PAYLOAD_VERSION
        )
    } else {
        version == PAYLOAD_VERSION
    };
    if kind != expected_kind || version != expected_version || !supported_version {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload kind/version disagrees with object entry",
        });
    }
    if read_u32(input, 8)? != PAYLOAD_HEADER_BYTES as u32 || read_u32(input, 24)? != 0 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload header length/flags are invalid",
        });
    }
    let body_length = read_u32(input, 12)? as usize;
    if PAYLOAD_HEADER_BYTES.checked_add(body_length) != Some(input.len()) {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload body length is invalid",
        });
    }
    if radixdb_core::crc32_ieee(&input[PAYLOAD_HEADER_BYTES..]) != read_u32(input, 28)? {
        return Err(CatalogError::CatalogChecksumMismatch {
            scope: "payload body",
        });
    }
    let field_count = read_u32(input, 16)? as u64;
    enforce_limit(
        "catalog fields per object",
        field_count,
        MAX_FIELDS_PER_OBJECT,
    )?;
    let directory_length = super::primitives::checked_product(
        field_count,
        FIELD_ENTRY_BYTES,
        "catalog payload field directory",
    )?;
    if read_u32(input, 20)? as usize != directory_length || directory_length > body_length {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload field directory length is invalid",
        });
    }

    let mut values = BTreeMap::new();
    let mut expected_value_offset = directory_length;
    let mut previous_tag = None;
    for index in 0..field_count as usize {
        let entry = PAYLOAD_HEADER_BYTES + index * FIELD_ENTRY_BYTES;
        let tag = read_u16(input, entry)?;
        if previous_tag.is_some_and(|previous| previous >= tag) {
            return Err(CatalogError::NonCanonicalCatalogEncoding {
                detail: "payload field tags are not strictly increasing",
            });
        }
        previous_tag = Some(tag);
        let kind = FieldType::try_from(read_u16(input, entry + 2)?)?;
        let flags = read_u32(input, entry + 4)?;
        if flags & !1 != 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "catalog payload field has unknown flags",
            });
        }
        let offset = read_u64(input, entry + 8)?;
        let length = read_u32(input, entry + 16)?;
        let count = read_u32(input, entry + 20)?;
        if offset != expected_value_offset as u64 {
            return Err(CatalogError::NonCanonicalCatalogEncoding {
                detail: "payload field values are not tightly packed in tag order",
            });
        }
        validate_length_count(kind, length, count)?;
        let start = PAYLOAD_HEADER_BYTES.checked_add(offset as usize).ok_or(
            CatalogError::InvalidCatalogFormat {
                detail: "payload field offset overflow",
            },
        )?;
        let end = start
            .checked_add(length as usize)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "payload field length overflow",
            })?;
        let bytes = input
            .get(start..end)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "payload field range is out of bounds",
            })?;
        expected_value_offset = expected_value_offset.checked_add(length as usize).ok_or(
            CatalogError::InvalidCatalogFormat {
                detail: "payload value offset overflow",
            },
        )?;
        if values
            .insert(
                tag,
                DecodedField {
                    kind,
                    optional: flags == 1,
                    count,
                    bytes,
                },
            )
            .is_some()
        {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "duplicate catalog payload field tag",
            });
        }
    }
    if expected_value_offset != body_length {
        return Err(CatalogError::NonCanonicalCatalogEncoding {
            detail: "catalog payload has unowned trailing bytes",
        });
    }
    decode_fields(kind, version, FieldSet { values })
}

fn decode_fields(
    kind: ObjectKind,
    version: u16,
    mut fields: FieldSet<'_>,
) -> CatalogResult<CatalogPayload> {
    let payload = match kind {
        ObjectKind::Namespace => CatalogPayload::Namespace(NamespacePayload::from_fields(
            version,
            decode_u64(fields.take(1, FieldType::U64, false)?)?,
        )?),
        ObjectKind::Table => {
            let flags = decode_u64(fields.take(1, FieldType::U64, false)?)?;
            let columns = decode_object_ids(fields.take(2, FieldType::ObjectIdArray, false)?)?;
            let constraints =
                decode_object_ids(fields.take(3, FieldType::ObjectIdArray, false)?)?;
            let indexes = decode_object_ids(fields.take(4, FieldType::ObjectIdArray, false)?)?;
            let primary = fields
                .take_optional(5, FieldType::ObjectId)?
                .map(decode_object_id)
                .transpose()?;
            let created_unix_ns = decode_u64(fields.take(6, FieldType::U64, false)?)?;
            let updated_unix_ns = decode_u64(fields.take(7, FieldType::U64, false)?)?;
            let payload = TablePayload::from_fields(TablePayloadFields {
                version,
                flags,
                column_ids: columns.clone(),
                constraint_ids: constraints.clone(),
                index_ids: indexes.clone(),
                primary_key_constraint_id: primary,
                created_unix_ns,
                updated_unix_ns,
            })?;
            if payload.column_ids() != columns
                || payload.constraint_ids() != constraints
                || payload.index_ids() != indexes
            {
                return Err(CatalogError::NonCanonicalCatalogEncoding {
                    detail: "table ID arrays are not canonical",
                });
            }
            CatalogPayload::Table(payload)
        }
        ObjectKind::Column => CatalogPayload::Column(ColumnPayload::from_fields(
            version,
            decode_u64(fields.take(6, FieldType::U64, false)?)?,
            decode_u32(fields.take(1, FieldType::U32, false)?)?,
            decode_data_type(fields.take(2, FieldType::DataTypeDescriptor, false)?)?,
            decode_bool(fields.take(3, FieldType::Bool, false)?)?,
            fields
                .take_optional(4, FieldType::Utf8)?
                .map(|field| decode_utf8(field, "column.default_sql"))
                .transpose()?,
            fields
                .take_optional(5, FieldType::Utf8)?
                .map(|field| decode_utf8(field, "column.generated_sql"))
                .transpose()?,
        )?),
        ObjectKind::Constraint => {
            let kind =
                ConstraintKind::try_from(decode_u16(fields.take(1, FieldType::U16, false)?)?)?;
            let payload = decode_constraint(kind, &mut fields)?;
            let flags = decode_u64(fields.take(9, FieldType::U64, false)?)?;
            if flags != 0 {
                return Err(CatalogError::UnknownPayloadFlags {
                    kind: "constraint",
                    flags,
                });
            }
            CatalogPayload::Constraint(payload)
        }
        ObjectKind::Index => {
            let method =
                AccessMethod::try_from(decode_u16(fields.take(1, FieldType::U16, false)?)?)?;
            let unique = decode_bool(fields.take(2, FieldType::Bool, false)?)?;
            let keys = fields
                .take_optional(3, FieldType::ObjectIdArray)?
                .map(decode_object_ids)
                .transpose()?
                .unwrap_or_default();
            let includes = decode_object_ids(fields.take(4, FieldType::ObjectIdArray, false)?)?;
            let expression = fields
                .take_optional(5, FieldType::Utf8)?
                .map(|field| decode_utf8(field, "index.expression_sql"))
                .transpose()?;
            let predicate = fields
                .take_optional(6, FieldType::Utf8)?
                .map(|field| decode_utf8(field, "index.predicate_sql"))
                .transpose()?;
            let flags = decode_u64(fields.take(7, FieldType::U64, false)?)?;
            let hnsw_m = fields
                .take_optional(8, FieldType::U16)?
                .map(decode_u16)
                .transpose()?;
            let hnsw_ef_construction = fields
                .take_optional(9, FieldType::U16)?
                .map(decode_u16)
                .transpose()?;
            let hnsw_ef_search = fields
                .take_optional(10, FieldType::U16)?
                .map(decode_u16)
                .transpose()?;
            let hnsw_metric = fields
                .take_optional(11, FieldType::U16)?
                .map(decode_u16)
                .transpose()?;
            let hnsw_parameters =
                decode_hnsw_parameters(hnsw_m, hnsw_ef_construction, hnsw_ef_search, hnsw_metric)?;
            let operator_class_id = fields
                .take_optional(12, FieldType::ObjectId)?
                .map(decode_object_id)
                .transpose()?;
            let payload = IndexPayload::from_fields(
                version,
                flags,
                method,
                unique,
                keys.clone(),
                includes.clone(),
                expression,
                predicate,
                hnsw_parameters,
                operator_class_id,
            )?;
            if payload.key_column_ids() != keys || payload.include_column_ids() != includes {
                return Err(CatalogError::NonCanonicalCatalogEncoding {
                    detail: "index ID arrays are not canonical",
                });
            }
            CatalogPayload::Index(payload)
        }
        ObjectKind::View => {
            let sql = decode_utf8(
                fields.take(1, FieldType::Utf8, false)?,
                "view.canonical_sql",
            )?;
            let dependencies =
                decode_object_ids(fields.take(2, FieldType::ObjectIdArray, false)?)?;
            let signature_field = fields.take(3, FieldType::Bytes, false)?;
            let signature: [u8; 32] = signature_field.bytes.try_into().map_err(|_| {
                CatalogError::InvalidCatalogFormat {
                    detail: "view output signature is not 32 bytes",
                }
            })?;
            let flags = decode_u64(fields.take(4, FieldType::U64, false)?)?;
            let payload =
                ViewPayload::from_fields(version, flags, sql, dependencies.clone(), signature)?;
            if payload.dependency_ids() != dependencies {
                return Err(CatalogError::NonCanonicalCatalogEncoding {
                    detail: "view dependency IDs are not canonical",
                });
            }
            CatalogPayload::View(payload)
        }
        ObjectKind::Principal => {
            let scheme = if version >= crate::payload::SECURITY_PAYLOAD_VERSION {
                fields
                    .take_optional(4, FieldType::U16)?
                    .map(decode_u16)
                    .transpose()?
            } else {
                None
            };
            let encoded = if version >= crate::payload::SECURITY_PAYLOAD_VERSION {
                fields
                    .take_optional(5, FieldType::Bytes)?
                    .map(|field| field.bytes)
            } else {
                None
            };
            let credential = match (scheme, encoded) {
                (None, None) => None,
                (Some(scheme), Some(encoded)) => {
                    Some(CredentialVerifier::new(scheme, encoded.to_vec())?)
                }
                _ => {
                    return Err(CatalogError::InvalidCatalogFormat {
                        detail: "principal credential scheme and payload must appear together",
                    })
                }
            };
            CatalogPayload::Principal(PrincipalPayload::from_fields_with_credential(
                version,
                decode_u64(fields.take(1, FieldType::U64, false)?)?,
                decode_bool(fields.take(2, FieldType::Bool, false)?)?,
                decode_bool(fields.take(3, FieldType::Bool, false)?)?,
                credential,
            )?)
        }
        ObjectKind::Role => CatalogPayload::Role(RolePayload::from_fields(
            version,
            decode_u64(fields.take(1, FieldType::U64, false)?)?,
            decode_bool(fields.take(2, FieldType::Bool, false)?)?,
            if version >= crate::payload::SECURITY_PAYLOAD_VERSION {
                fields
                    .take_optional(3, FieldType::Bool)?
                    .map(decode_bool)
                    .transpose()?
                    .unwrap_or(true)
            } else {
                true
            },
        )?),
        ObjectKind::AclEntry => {
            let flags = decode_u64(fields.take(1, FieldType::U64, false)?)?;
            let discriminator = decode_u16(fields.take(2, FieldType::U16, false)?)?;
            let grantor = decode_object_id(fields.take(3, FieldType::ObjectId, false)?)?;
            let value = match discriminator {
                1 => AclEntryPayload::object_privileges_with_column_options(
                    grantor,
                    decode_u64(fields.take(4, FieldType::U64, false)?)?,
                    decode_u64(fields.take(5, FieldType::U64, false)?)?,
                    decode_column_privileges(fields.take(6, FieldType::Bytes, false)?.bytes)?,
                    if version >= crate::payload::SECURITY_PAYLOAD_VERSION {
                        fields
                            .take_optional(8, FieldType::Bytes)?
                            .map(|field| decode_column_privileges(field.bytes))
                            .transpose()?
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    },
                )?,
                2 => AclEntryPayload::role_membership(
                    grantor,
                    decode_bool(fields.take(7, FieldType::Bool, false)?)?,
                ),
                tag => {
                    return Err(CatalogError::UnknownPayloadEnumTag {
                        owner: "ACL entry discriminator",
                        tag,
                    })
                }
            };
            CatalogPayload::AclEntry(AclEntryPayload::from_fields(version, flags, value)?)
        }
        ObjectKind::Function if version == 2 => {
            let flags = decode_u64(fields.take(1, FieldType::U64, false)?)?;
            if flags != 0 {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "native function flags are non-zero",
                });
            }
            let arguments = decode_arguments(fields.take(4, FieldType::Bytes, false)?.bytes)?;
            let result = decode_result(fields.take(5, FieldType::Bytes, false)?.bytes)?;
            let volatility =
                Volatility::try_from(decode_u16(fields.take(6, FieldType::U16, false)?)?)?;
            if SecurityMode::try_from(decode_u16(fields.take(7, FieldType::U16, false)?)?)?
                != SecurityMode::Invoker
            {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "native function must use security invoker",
                });
            }
            let semantic_revision = decode_u32(fields.take(9, FieldType::U32, false)?)?;
            let dependency_ids =
                decode_object_ids(fields.take(13, FieldType::ObjectIdArray, false)?)?;
            if LanguageKind::try_from(decode_u16(fields.take(14, FieldType::U16, false)?)?)?
                != LanguageKind::Native
                || decode_u16(fields.take(15, FieldType::U16, false)?)? != NATIVE_LANGUAGE_VERSION
            {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "native function language kind/version is unsupported",
                });
            }
            let definition = NativeFunctionDefinition::new(
                arguments,
                result,
                volatility,
                semantic_revision,
                dependency_ids,
                decode_object_id(fields.take(16, FieldType::ObjectId, false)?)?,
                decode_utf8(
                    fields.take(17, FieldType::Utf8, false)?,
                    "native_function.local_id",
                )?,
                decode_bool(fields.take(18, FieldType::Bool, false)?)?,
                decode_bool(fields.take(19, FieldType::Bool, false)?)?,
                decode_u32(fields.take(20, FieldType::U32, false)?)?,
                decode_u16(fields.take(21, FieldType::U16, false)?)?,
                decode_bool(fields.take(22, FieldType::Bool, false)?)?,
                decode_u32(fields.take(23, FieldType::U32, false)?)?,
            )?;
            CatalogPayload::Function(FunctionPayload::new_native(definition))
        }
        ObjectKind::Function | ObjectKind::Procedure => {
            let flags = decode_u64(fields.take(1, FieldType::U64, false)?)?;
            let source = decode_utf8(fields.take(2, FieldType::Utf8, false)?, "routine.source")?;
            let digest: [u8; 32] = fields
                .take(3, FieldType::Bytes, false)?
                .bytes
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalogFormat {
                    detail: "routine source digest is not 32 bytes",
                })?;
            let source = ProceduralSource::from_fields(source, digest)?;
            let arguments = decode_arguments(fields.take(4, FieldType::Bytes, false)?.bytes)?;
            let result = decode_result(fields.take(5, FieldType::Bytes, false)?.bytes)?;
            let volatility =
                Volatility::try_from(decode_u16(fields.take(6, FieldType::U16, false)?)?)?;
            let security =
                SecurityMode::try_from(decode_u16(fields.take(7, FieldType::U16, false)?)?)?;
            let search_path =
                decode_object_ids(fields.take(8, FieldType::ObjectIdArray, false)?)?;
            let definition_version = decode_u32(fields.take(9, FieldType::U32, false)?)?;
            let compiler_abi = decode_u32(fields.take(10, FieldType::U32, false)?)?;
            let runtime_abi = decode_u32(fields.take(11, FieldType::U32, false)?)?;
            let resource_policy =
                decode_resource_policy(fields.take(12, FieldType::Bytes, false)?.bytes)?;
            let dependency_ids =
                decode_object_ids(fields.take(13, FieldType::ObjectIdArray, false)?)?;
            let language_kind =
                LanguageKind::try_from(decode_u16(fields.take(14, FieldType::U16, false)?)?)?;
            let language_version = decode_u16(fields.take(15, FieldType::U16, false)?)?;
            if language_kind != LanguageKind::RadixPl
                || language_version != crate::PROCEDURAL_LANGUAGE_VERSION
            {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "routine language kind/version is unsupported",
                });
            }
            let definition = RoutineDefinition::new(
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
                resource_policy,
            )?;
            match kind {
                ObjectKind::Function => CatalogPayload::Function(FunctionPayload::from_fields(
                    version, flags, definition,
                )?),
                ObjectKind::Procedure => CatalogPayload::Procedure(ProcedurePayload::from_fields(
                    version, flags, definition,
                )?),
                _ => unreachable!(),
            }
        }
        ObjectKind::Trigger => CatalogPayload::Trigger(TriggerPayload::from_fields(
            version,
            decode_u64(fields.take(1, FieldType::U64, false)?)?,
            decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
            decode_object_id(fields.take(3, FieldType::ObjectId, false)?)?,
            TriggerTiming::try_from(decode_u16(fields.take(4, FieldType::U16, false)?)?)?,
            decode_u16(fields.take(5, FieldType::U16, false)?)?,
            TriggerLevel::try_from(decode_u16(fields.take(6, FieldType::U16, false)?)?)?,
            decode_object_ids(fields.take(7, FieldType::ObjectIdArray, false)?)?,
            decode_u32(fields.take(8, FieldType::U32, false)?)? as i32,
            fields
                .take_optional(9, FieldType::Utf8)?
                .map(|field| decode_utf8(field, "trigger.when_sql"))
                .transpose()?,
        )?),
        ObjectKind::Job => {
            let flags = decode_u64(fields.take(1, FieldType::U64, false)?)?;
            let procedure_id = decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?;
            let principal_id = decode_object_id(fields.take(3, FieldType::ObjectId, false)?)?;
            let schedule_value = decode_u64(fields.take(5, FieldType::U64, false)?)?;
            let schedule = match decode_u16(fields.take(4, FieldType::U16, false)?)? {
                1 => JobSchedule::AtUnixNs(schedule_value as i64),
                2 => JobSchedule::EveryNs(schedule_value),
                tag => {
                    return Err(CatalogError::UnknownPayloadEnumTag {
                        owner: "job schedule",
                        tag,
                    })
                }
            };
            CatalogPayload::Job(JobPayload::from_fields(
                version,
                flags,
                procedure_id,
                principal_id,
                schedule,
                decode_job_arguments(fields.take(6, FieldType::Bytes, false)?.bytes)?,
                decode_bool(fields.take(7, FieldType::Bool, false)?)?,
                decode_u32(fields.take(8, FieldType::U32, false)?)?,
                decode_resource_policy(fields.take(9, FieldType::Bytes, false)?.bytes)?,
            )?)
        }
        ObjectKind::Extension => {
            let fingerprint: [u8; 32] = fields
                .take(7, FieldType::Bytes, false)?
                .bytes
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalogFormat {
                detail: "extension descriptor fingerprint is not 32 bytes",
            })?;
            CatalogPayload::Extension(ExtensionPayload::from_fields(
                version,
                decode_u64(fields.take(1, FieldType::U64, false)?)?,
                decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
                decode_utf8(fields.take(3, FieldType::Utf8, false)?, "extension.version")?,
                decode_u16(fields.take(4, FieldType::U16, false)?)?,
                decode_u16(fields.take(5, FieldType::U16, false)?)?,
                decode_u16(fields.take(6, FieldType::U16, false)?)?,
                fingerprint,
            )?)
        }
        ObjectKind::ExternalType => {
            let fingerprint: [u8; 32] = fields
                .take(9, FieldType::Bytes, false)?
                .bytes
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalogFormat {
                detail: "external type codec fingerprint is not 32 bytes",
            })?;
            CatalogPayload::ExternalType(ExternalTypePayload::from_fields(
                version,
                decode_u64(fields.take(1, FieldType::U64, false)?)?,
                decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
                decode_utf8(
                    fields.take(3, FieldType::Utf8, false)?,
                    "external_type.local_id",
                )?,
                decode_u32(fields.take(4, FieldType::U32, false)?)?,
                decode_u32(fields.take(5, FieldType::U32, false)?)?,
                ExternalStorageKind::try_from(decode_u16(fields.take(
                    6,
                    FieldType::U16,
                    false,
                )?)?)?,
                fields
                    .take_optional(7, FieldType::U32)?
                    .map(decode_u32)
                    .transpose()?,
                decode_u32(fields.take(8, FieldType::U32, false)?)?,
                fingerprint,
                decode_u64(fields.take(10, FieldType::U64, false)?)?,
            )?)
        }
        ObjectKind::Operator => CatalogPayload::Operator(OperatorPayload::from_fields(
            version,
            decode_u64(fields.take(1, FieldType::U64, false)?)?,
            decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
            decode_utf8(fields.take(3, FieldType::Utf8, false)?, "operator.local_id")?,
            decode_u32(fields.take(4, FieldType::U32, false)?)?,
            decode_utf8(fields.take(5, FieldType::Utf8, false)?, "operator.symbol")?,
            fields
                .take_optional(6, FieldType::DataTypeDescriptor)?
                .map(decode_data_type)
                .transpose()?,
            fields
                .take_optional(7, FieldType::DataTypeDescriptor)?
                .map(decode_data_type)
                .transpose()?,
            decode_data_type(fields.take(8, FieldType::DataTypeDescriptor, false)?)?,
            decode_object_id(fields.take(9, FieldType::ObjectId, false)?)?,
        )?),
        ObjectKind::OperatorClass => {
            let fingerprint: [u8; 32] = fields
                .take(11, FieldType::Bytes, false)?
                .bytes
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalogFormat {
                    detail: "operator class fingerprint is not 32 bytes",
                })?;
            CatalogPayload::OperatorClass(OperatorClassPayload::from_fields(
                version,
                decode_u64(fields.take(1, FieldType::U64, false)?)?,
                decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
                decode_utf8(
                    fields.take(3, FieldType::Utf8, false)?,
                    "operator_class.local_id",
                )?,
                decode_u32(fields.take(4, FieldType::U32, false)?)?,
                AccessMethod::try_from(decode_u16(fields.take(5, FieldType::U16, false)?)?)?,
                decode_data_type(fields.take(6, FieldType::DataTypeDescriptor, false)?)?,
                decode_data_type(fields.take(7, FieldType::DataTypeDescriptor, false)?)?,
                decode_operator_bindings(fields.take(8, FieldType::Bytes, false)?)?,
                decode_operator_bindings(fields.take(9, FieldType::Bytes, false)?)?,
                decode_u32(fields.take(10, FieldType::U32, false)?)?,
                fingerprint,
            )?)
        }
        ObjectKind::PlannerSupport => {
            let fingerprint: [u8; 32] = fields
                .take(10, FieldType::Bytes, false)?
                .bytes
                .try_into()
                .map_err(|_| CatalogError::InvalidCatalogFormat {
                    detail: "planner support fingerprint is not 32 bytes",
                })?;
            CatalogPayload::PlannerSupport(PlannerSupportPayload::from_fields(
                version,
                decode_u64(fields.take(1, FieldType::U64, false)?)?,
                decode_object_id(fields.take(2, FieldType::ObjectId, false)?)?,
                decode_utf8(
                    fields.take(3, FieldType::Utf8, false)?,
                    "planner_support.local_id",
                )?,
                decode_u32(fields.take(4, FieldType::U32, false)?)?,
                fields
                    .take_optional(5, FieldType::ObjectId)?
                    .map(decode_object_id)
                    .transpose()?,
                fields
                    .take_optional(6, FieldType::ObjectId)?
                    .map(decode_object_id)
                    .transpose()?,
                decode_u32(fields.take(7, FieldType::U32, false)?)?,
                decode_u32(fields.take(8, FieldType::U32, false)?)?,
                PlannerRecheckPolicy::try_from(decode_u16(fields.take(
                    9,
                    FieldType::U16,
                    false,
                )?)?)?,
                fingerprint,
            )?)
        }
    };
    fields.finish()?;
    Ok(payload)
}

fn decode_hnsw_parameters(
    m: Option<u16>,
    ef_construction: Option<u16>,
    ef_search: Option<u16>,
    metric: Option<u16>,
) -> CatalogResult<Option<HnswParameters>> {
    match (m, ef_construction, ef_search, metric) {
        (None, None, None, None) => Ok(None),
        (Some(m), Some(ef_construction), Some(ef_search), Some(metric)) => HnswParameters::new(
            m,
            ef_construction,
            ef_search,
            HnswDistanceMetric::try_from(metric)?,
        )
        .map(Some),
        _ => Err(CatalogError::InvalidCatalogFormat {
            detail: "partial HNSW catalog parameters",
        }),
    }
}

fn decode_constraint(
    kind: ConstraintKind,
    fields: &mut FieldSet<'_>,
) -> CatalogResult<ConstraintPayload> {
    match kind {
        ConstraintKind::PrimaryKey => ConstraintPayload::primary_key(decode_object_ids(
            fields.take(2, FieldType::ObjectIdArray, false)?,
        )?),
        ConstraintKind::Unique => ConstraintPayload::unique(decode_object_ids(fields.take(
            2,
            FieldType::ObjectIdArray,
            false,
        )?)?),
        ConstraintKind::ForeignKey => ConstraintPayload::foreign_key(
            decode_object_ids(fields.take(2, FieldType::ObjectIdArray, false)?)?,
            decode_object_id(fields.take(3, FieldType::ObjectId, false)?)?,
            decode_object_ids(fields.take(4, FieldType::ObjectIdArray, false)?)?,
            ForeignKeyMatch::try_from(decode_u16(fields.take(6, FieldType::U16, false)?)?)?,
            ForeignKeyAction::try_from(decode_u16(fields.take(7, FieldType::U16, false)?)?)?,
            ForeignKeyAction::try_from(decode_u16(fields.take(8, FieldType::U16, false)?)?)?,
        ),
        ConstraintKind::Check => {
            let local_column_id = fields
                .take_optional(2, FieldType::ObjectIdArray)?
                .map(decode_object_ids)
                .transpose()?
                .map(|ids| {
                    if ids.len() != 1 {
                        return Err(CatalogError::InvalidConstraintPayload {
                            detail: "column CHECK requires exactly one local column",
                        });
                    }
                    Ok(ids[0])
                })
                .transpose()?;
            let check_sql = decode_utf8(
                fields.take(5, FieldType::Utf8, false)?,
                "constraint.check_sql",
            )?;
            match local_column_id {
                Some(local_column_id) => {
                    ConstraintPayload::column_check(local_column_id, check_sql)
                }
                None => ConstraintPayload::check(check_sql),
            }
        }
        ConstraintKind::NotNull => {
            let ids = decode_object_ids(fields.take(2, FieldType::ObjectIdArray, false)?)?;
            if ids.len() != 1 {
                return Err(CatalogError::InvalidConstraintPayload {
                    detail: "NOT NULL requires exactly one local column",
                });
            }
            Ok(ConstraintPayload::not_null(ids[0]))
        }
    }
}

fn validate_field_shape(
    field: DecodedField<'_>,
    kind: FieldType,
    optional: bool,
) -> CatalogResult<()> {
    if field.kind != kind || field.optional != optional {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload field type/optional flag is invalid",
        });
    }
    Ok(())
}

fn validate_length_count(kind: FieldType, length: u32, count: u32) -> CatalogResult<()> {
    let valid = match kind {
        FieldType::U8 => length == 1 && count == 1,
        FieldType::U16 => length == 2 && count == 1,
        FieldType::U32 => length == 4 && count == 1,
        FieldType::U64 => length == 8 && count == 1,
        FieldType::Bool => length == 1 && count == 1,
        FieldType::ObjectId => length == 16 && count == 1,
        FieldType::ObjectIdArray => {
            count <= crate::MAX_OBJECT_IDS_PER_FIELD as u32 && count.checked_mul(16) == Some(length)
        }
        FieldType::Utf8 | FieldType::Bytes => length == count,
        FieldType::DataTypeDescriptor => length == 32 && count == 1,
    };
    if !valid {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload field length/count is invalid",
        });
    }
    Ok(())
}

fn decode_u16(field: DecodedField<'_>) -> CatalogResult<u16> {
    read_u16(field.bytes, 0)
}

fn decode_u32(field: DecodedField<'_>) -> CatalogResult<u32> {
    read_u32(field.bytes, 0)
}

fn decode_u64(field: DecodedField<'_>) -> CatalogResult<u64> {
    read_u64(field.bytes, 0)
}

fn decode_bool(field: DecodedField<'_>) -> CatalogResult<bool> {
    match field.bytes[0] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog payload boolean is not 0 or 1",
        }),
    }
}

fn decode_object_id(field: DecodedField<'_>) -> CatalogResult<ObjectId> {
    ObjectId::from_bytes(field.bytes.try_into().expect("validated 16-byte ObjectId"))
}

fn decode_object_ids(field: DecodedField<'_>) -> CatalogResult<Vec<ObjectId>> {
    let mut ids = Vec::with_capacity(field.count as usize);
    for bytes in field.bytes.chunks_exact(16) {
        ids.push(ObjectId::from_bytes(
            bytes.try_into().expect("validated ObjectId chunk"),
        )?);
    }
    Ok(ids)
}

fn decode_utf8(field: DecodedField<'_>, name: &'static str) -> CatalogResult<String> {
    std::str::from_utf8(field.bytes)
        .map(str::to_owned)
        .map_err(|_| CatalogError::InvalidCatalogUtf8 { field: name })
}

fn decode_data_type(field: DecodedField<'_>) -> CatalogResult<CatalogDataType> {
    decode_data_type_bytes(field.bytes)
}

fn decode_operator_bindings(field: DecodedField<'_>) -> CatalogResult<Vec<OperatorBinding>> {
    if field.bytes.len() < 4 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "operator-class binding table is truncated",
        });
    }
    let count = read_u32(field.bytes, 0)? as usize;
    if field.bytes.len() != 4_usize.saturating_add(count.saturating_mul(20)) {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "operator-class binding table length is invalid",
        });
    }
    let mut output = Vec::with_capacity(count);
    for entry in field.bytes[4..].chunks_exact(20) {
        if read_u16(entry, 2)? != 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "operator-class binding flags are non-zero",
            });
        }
        output.push(OperatorBinding::new(
            read_u16(entry, 0)?,
            ObjectId::from_bytes(read_array(entry, 4)?)?,
        ));
    }
    Ok(output)
}

fn decode_data_type_bytes(bytes: &[u8]) -> CatalogResult<CatalogDataType> {
    if bytes.len() != 32 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "nested data type descriptor is not 32 bytes",
        });
    }
    let type_tag = read_u16(bytes, 0)?;
    if type_tag == 0xffff {
        if read_u32(bytes, 4)? != 0 || read_u32(bytes, 12)? != 0 {
            return Err(CatalogError::InvalidDataTypeDescriptor {
                detail: "external descriptor flags/reserved fields must be zero",
            });
        }
        return CatalogDataType::external(
            ObjectId::from_bytes(read_array(bytes, 16)?)?,
            read_u32(bytes, 8)?,
        )
        .and_then(|descriptor| {
            if read_u16(bytes, 2)? == descriptor.descriptor_version() {
                Ok(descriptor)
            } else {
                Err(CatalogError::InvalidDataTypeDescriptor {
                    detail: "unsupported external descriptor version",
                })
            }
        });
    }
    let type_tag = u8::try_from(type_tag).map_err(|_| CatalogError::InvalidDataTypeDescriptor {
        detail: "logical type tag exceeds u8",
    })?;
    let logical_type =
        DataType::from_u8(type_tag).ok_or(CatalogError::InvalidDataTypeDescriptor {
            detail: "unknown logical type tag",
        })?;
    CatalogDataType::from_fields(
        logical_type,
        read_u16(bytes, 2)?,
        read_u32(bytes, 4)?,
        read_u32(bytes, 8)?,
        read_u32(bytes, 12)?,
        read_array(bytes, 16)?,
    )
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> CatalogResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "nested payload offset overflow",
            })?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "nested payload is truncated",
            })?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> CatalogResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> CatalogResult<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> CatalogResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> CatalogResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn finish(self) -> CatalogResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CatalogError::NonCanonicalCatalogEncoding {
                detail: "nested payload has trailing bytes",
            })
        }
    }
}

fn decode_arguments(bytes: &[u8]) -> CatalogResult<Vec<RoutineArgument>> {
    let mut cursor = ByteCursor::new(bytes);
    let count = cursor.u32()? as usize;
    if count > crate::MAX_ROUTINE_ARGUMENTS {
        return Err(CatalogError::CatalogLimitExceeded {
            field: "routine argument count",
            actual: count as u64,
            limit: crate::MAX_ROUTINE_ARGUMENTS as u64,
        });
    }
    let mut output = Vec::with_capacity(count);
    let mut default_seen = false;
    for _ in 0..count {
        let name_length = cursor.u32()? as usize;
        let mode = ArgumentMode::try_from(cursor.u16()?)?;
        let nullable = decode_nested_bool(cursor.u8()?)?;
        if cursor.u8()? != 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "routine argument reserved byte is non-zero",
            });
        }
        let data_type = decode_data_type_bytes(cursor.take(32)?)?;
        let default_length = cursor.u32()?;
        let name = std::str::from_utf8(cursor.take(name_length)?).map_err(|_| {
            CatalogError::InvalidCatalogUtf8 {
                field: "routine argument name",
            }
        })?;
        let default_sql = if default_length == u32::MAX {
            None
        } else {
            default_seen = true;
            Some(
                std::str::from_utf8(cursor.take(default_length as usize)?)
                    .map_err(|_| CatalogError::InvalidCatalogUtf8 {
                        field: "routine argument default",
                    })?
                    .to_owned(),
            )
        };
        if default_seen && default_sql.is_none() && mode != ArgumentMode::Out {
            return Err(CatalogError::NonCanonicalCatalogEncoding {
                detail: "required routine argument follows defaulted argument",
            });
        }
        output.push(RoutineArgument::new(
            CatalogName::new(name)?,
            mode,
            data_type,
            nullable,
            default_sql,
        )?);
    }
    cursor.finish()?;
    Ok(output)
}

fn decode_result(bytes: &[u8]) -> CatalogResult<RoutineResult> {
    let mut cursor = ByteCursor::new(bytes);
    let value = match cursor.u16()? {
        1 => RoutineResult::Void,
        2 => RoutineResult::Scalar {
            data_type: decode_data_type_bytes(cursor.take(32)?)?,
            nullable: decode_nested_bool(cursor.u8()?)?,
        },
        3 => {
            let count = cursor.u32()? as usize;
            if count == 0 || count > crate::MAX_RESULT_COLUMNS {
                return Err(CatalogError::InvalidCatalogFormat {
                    detail: "routine result column count is invalid",
                });
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let name_length = cursor.u32()? as usize;
                let data_type = decode_data_type_bytes(cursor.take(32)?)?;
                let nullable = decode_nested_bool(cursor.u8()?)?;
                let name = std::str::from_utf8(cursor.take(name_length)?).map_err(|_| {
                    CatalogError::InvalidCatalogUtf8 {
                        field: "routine result column name",
                    }
                })?;
                columns.push(ResultColumn::new(
                    CatalogName::new(name)?,
                    data_type,
                    nullable,
                ));
            }
            RoutineResult::Table(columns)
        }
        4 => RoutineResult::Trigger,
        tag => {
            return Err(CatalogError::UnknownPayloadEnumTag {
                owner: "routine result",
                tag,
            })
        }
    };
    cursor.finish()?;
    Ok(value)
}

fn decode_resource_policy(bytes: &[u8]) -> CatalogResult<ResourcePolicy> {
    let mut cursor = ByteCursor::new(bytes);
    let policy = ResourcePolicy {
        instructions: cursor.u64()?,
        heap_bytes: cursor.u64()?,
        frames: cursor.u32()?,
        sql_statements: cursor.u64()?,
        rows: cursor.u64()?,
        result_bytes: cursor.u64()?,
        deadline_ms: cursor.u64()?,
    };
    cursor.finish()?;
    policy.validate()
}

fn decode_column_privileges(bytes: &[u8]) -> CatalogResult<Vec<ColumnPrivilegeSet>> {
    let mut cursor = ByteCursor::new(bytes);
    let count = cursor.u32()? as usize;
    if count > 3 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "too many ACL column privilege groups",
        });
    }
    let mut output = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let privilege = cursor.u64()?;
        if previous.is_some_and(|value| value >= privilege) {
            return Err(CatalogError::NonCanonicalCatalogEncoding {
                detail: "ACL column privilege groups are not strictly ordered",
            });
        }
        previous = Some(privilege);
        let id_count = cursor.u32()? as usize;
        if id_count == 0 || id_count > crate::MAX_OBJECT_IDS_PER_FIELD {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "ACL column ID count is invalid",
            });
        }
        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            ids.push(ObjectId::from_bytes(cursor.take(16)?.try_into().unwrap())?);
        }
        output.push(ColumnPrivilegeSet::new(privilege, ids)?);
    }
    cursor.finish()?;
    Ok(output)
}

fn decode_job_arguments(bytes: &[u8]) -> CatalogResult<Vec<JobArgument>> {
    let mut cursor = ByteCursor::new(bytes);
    let count = cursor.u32()? as usize;
    if count > crate::MAX_JOB_ARGUMENTS {
        return Err(CatalogError::CatalogLimitExceeded {
            field: "job argument count",
            actual: count as u64,
            limit: crate::MAX_JOB_ARGUMENTS as u64,
        });
    }
    let mut output = Vec::with_capacity(count);
    for _ in 0..count {
        let name_length = cursor.u32()?;
        let data_type = decode_data_type_bytes(cursor.take(32)?)?;
        let value_length = cursor.u32()?;
        let name = if name_length == u32::MAX {
            None
        } else {
            let name = std::str::from_utf8(cursor.take(name_length as usize)?).map_err(|_| {
                CatalogError::InvalidCatalogUtf8 {
                    field: "job argument name",
                }
            })?;
            Some(CatalogName::new(name)?)
        };
        let value = if value_length == u32::MAX {
            None
        } else {
            Some(cursor.take(value_length as usize)?.to_vec())
        };
        output.push(JobArgument::new(name, data_type, value)?);
    }
    cursor.finish()?;
    Ok(output)
}

fn decode_nested_bool(value: u8) -> CatalogResult<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CatalogError::InvalidCatalogFormat {
            detail: "nested payload boolean is not 0 or 1",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> ObjectId {
        let mut bytes = [byte; 16];
        bytes[0] = 1;
        ObjectId::from_user_bytes(bytes).unwrap()
    }

    #[test]
    fn all_payload_variants_have_byte_stable_roundtrip() {
        let scalar = CatalogDataType::scalar(DataType::Integer).unwrap();
        let external = CatalogDataType::external(id(40), 1).unwrap();
        let function_definition = RoutineDefinition::new(
            ProceduralSource::new("BEGIN RETURN value; END").unwrap(),
            vec![RoutineArgument::new(
                CatalogName::new("value").unwrap(),
                ArgumentMode::In,
                scalar,
                false,
                Some("1".into()),
            )
            .unwrap()],
            RoutineResult::Scalar {
                data_type: scalar,
                nullable: false,
            },
            Volatility::Immutable,
            SecurityMode::Invoker,
            vec![id(10)],
            vec![],
            1,
            1,
            1,
            ResourcePolicy::default_call(),
        )
        .unwrap();
        let procedure_definition = RoutineDefinition::new(
            ProceduralSource::new("BEGIN value := value + 1; END").unwrap(),
            vec![RoutineArgument::new(
                CatalogName::new("value").unwrap(),
                ArgumentMode::InOut,
                scalar,
                false,
                None,
            )
            .unwrap()],
            RoutineResult::Void,
            Volatility::Volatile,
            SecurityMode::Definer,
            vec![id(10)],
            vec![],
            1,
            1,
            1,
            ResourcePolicy::default_call(),
        )
        .unwrap();
        let payloads = vec![
            CatalogPayload::Namespace(NamespacePayload::new()),
            CatalogPayload::Table(
                TablePayload::new(vec![id(3)], vec![id(4)], vec![id(5)], Some(id(4))).unwrap(),
            ),
            CatalogPayload::Column(
                ColumnPayload::new_with_auto_increment(
                    0,
                    CatalogDataType::scalar(DataType::Integer).unwrap(),
                    false,
                    true,
                    None,
                    None,
                )
                .unwrap(),
            ),
            CatalogPayload::Constraint(
                ConstraintPayload::foreign_key(
                    vec![id(3)],
                    id(6),
                    vec![id(7)],
                    ForeignKeyMatch::Full,
                    ForeignKeyAction::Cascade,
                    ForeignKeyAction::SetNull,
                )
                .unwrap(),
            ),
            CatalogPayload::Index(
                IndexPayload::new(
                    AccessMethod::Btree,
                    false,
                    vec![],
                    vec![id(8)],
                    Some("lower(name)".into()),
                    Some("active".into()),
                )
                .unwrap(),
            ),
            CatalogPayload::Index(
                IndexPayload::new_external(AccessMethod::Btree, false, id(3), None, id(43))
                    .unwrap(),
            ),
            CatalogPayload::View(
                ViewPayload::new("SELECT id FROM messages", vec![id(9)], [3; 32]).unwrap(),
            ),
            CatalogPayload::Principal(PrincipalPayload::new(true, false)),
            CatalogPayload::Role(RolePayload::new(true)),
            CatalogPayload::AclEntry(
                AclEntryPayload::object_privileges(
                    id(10),
                    crate::PRIVILEGE_SELECT | crate::PRIVILEGE_UPDATE,
                    crate::PRIVILEGE_SELECT,
                    vec![ColumnPrivilegeSet::new(crate::PRIVILEGE_UPDATE, vec![id(11)]).unwrap()],
                )
                .unwrap(),
            ),
            CatalogPayload::Function(FunctionPayload::new(function_definition).unwrap()),
            CatalogPayload::Procedure(ProcedurePayload::new(procedure_definition).unwrap()),
            CatalogPayload::Trigger(
                TriggerPayload::new(
                    id(12),
                    id(13),
                    TriggerTiming::Before,
                    crate::TRIGGER_EVENT_INSERT | crate::TRIGGER_EVENT_UPDATE,
                    TriggerLevel::Row,
                    vec![id(11)],
                    1000,
                    Some("NEW.value IS NOT NULL".into()),
                )
                .unwrap(),
            ),
            CatalogPayload::Job(
                JobPayload::new(
                    id(14),
                    id(10),
                    JobSchedule::EveryNs(1_000_000_000),
                    vec![JobArgument::new(
                        Some(CatalogName::new("value").unwrap()),
                        scalar,
                        Some(7_i64.to_le_bytes().to_vec()),
                    )
                    .unwrap()],
                    true,
                    1,
                    ResourcePolicy::default_call(),
                )
                .unwrap(),
            ),
            CatalogPayload::Operator(
                OperatorPayload::new(
                    id(39),
                    "point_equal_operator",
                    1,
                    "=",
                    Some(external),
                    Some(external),
                    CatalogDataType::scalar(DataType::Boolean).unwrap(),
                    id(38),
                )
                .unwrap(),
            ),
            CatalogPayload::OperatorClass(
                OperatorClassPayload::new(
                    id(39),
                    "point_btree",
                    1,
                    AccessMethod::Btree,
                    external,
                    CatalogDataType::scalar(DataType::Bytes).unwrap(),
                    vec![
                        OperatorBinding::new(1, id(31)),
                        OperatorBinding::new(2, id(32)),
                        OperatorBinding::new(3, id(33)),
                        OperatorBinding::new(4, id(34)),
                        OperatorBinding::new(5, id(35)),
                    ],
                    vec![],
                    1,
                    [17; 32],
                )
                .unwrap(),
            ),
        ];
        for payload in payloads {
            let bytes = encode_payload(&payload).unwrap();
            let decoded = decode_payload(&bytes, payload.kind(), payload.version()).unwrap();
            assert_eq!(decoded, payload);
            assert_eq!(encode_payload(&decoded).unwrap(), bytes);
        }
    }

    #[test]
    fn security_v1_payloads_upgrade_with_fail_closed_defaults() {
        let role_bytes = encode_payload_fields(
            ObjectKind::Role,
            PAYLOAD_VERSION,
            vec![field_u64(1, false, 0), field_bool(2, false, true)],
        )
        .unwrap();
        let role = decode_payload(&role_bytes, ObjectKind::Role, PAYLOAD_VERSION).unwrap();
        assert!(matches!(
            role,
            CatalogPayload::Role(ref payload) if payload.inheritable() && payload.enabled()
        ));

        let legacy_acl = AclEntryPayload::object_privileges(
            id(10),
            crate::PRIVILEGE_SELECT,
            crate::PRIVILEGE_SELECT,
            vec![ColumnPrivilegeSet::new(crate::PRIVILEGE_UPDATE, vec![id(11)]).unwrap()],
        )
        .unwrap();
        let mut legacy_acl_fields = acl_fields(&legacy_acl).unwrap();
        legacy_acl_fields.retain(|field| field.tag != 8);
        let acl_bytes =
            encode_payload_fields(ObjectKind::AclEntry, PAYLOAD_VERSION, legacy_acl_fields)
                .unwrap();
        let acl = decode_payload(&acl_bytes, ObjectKind::AclEntry, PAYLOAD_VERSION).unwrap();
        assert!(matches!(
            acl,
            CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
                privileges,
                grant_option,
                ref columns,
                ref column_grant_options,
                ..
            }) if privileges == crate::PRIVILEGE_SELECT
                && grant_option == crate::PRIVILEGE_SELECT
                && columns.len() == 1
                && column_grant_options.is_empty()
        ));
    }

    #[test]
    fn principal_credential_verifier_has_byte_stable_all_or_nothing_roundtrip() {
        let credential = CredentialVerifier::new(
            crate::CREDENTIAL_SCHEME_ARGON2ID_PHC_V1,
            b"$argon2id$v=19$m=19456,t=2,p=1$fixture$verifier".to_vec(),
        )
        .unwrap();
        let payload = CatalogPayload::Principal(
            PrincipalPayload::new(true, false).with_credential(Some(credential.clone())),
        );
        let encoded = encode_payload(&payload).unwrap();
        let decoded = decode_payload(
            &encoded,
            ObjectKind::Principal,
            crate::payload::SECURITY_PAYLOAD_VERSION,
        )
        .unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(encode_payload(&decoded).unwrap(), encoded);

        let mut fields = payload_fields(&payload).unwrap();
        fields.retain(|field| field.tag != 4);
        let missing_scheme = encode_payload_fields(
            ObjectKind::Principal,
            crate::payload::SECURITY_PAYLOAD_VERSION,
            fields,
        )
        .unwrap();
        assert!(decode_payload(
            &missing_scheme,
            ObjectKind::Principal,
            crate::payload::SECURITY_PAYLOAD_VERSION
        )
        .is_err());
    }

    #[test]
    fn hnsw_parameters_have_byte_stable_roundtrip_and_are_all_or_nothing() {
        let payload = CatalogPayload::Index(
            IndexPayload::new_hnsw(
                id(7),
                vec![],
                HnswParameters::new(16, 200, 64, HnswDistanceMetric::Cosine).unwrap(),
            )
            .unwrap(),
        );
        let bytes = encode_payload(&payload).unwrap();
        let decoded = decode_payload(&bytes, ObjectKind::Index, PAYLOAD_VERSION).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(encode_payload(&decoded).unwrap(), bytes);

        assert!(matches!(
            decode_hnsw_parameters(Some(16), Some(200), None, Some(1)),
            Err(CatalogError::InvalidCatalogFormat {
                detail: "partial HNSW catalog parameters"
            })
        ));
    }

    #[test]
    fn body_crc_fails_closed() {
        let payload = CatalogPayload::Namespace(NamespacePayload::new());
        let mut bytes = encode_payload(&payload).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode_payload(&bytes, ObjectKind::Namespace, 1),
            Err(CatalogError::CatalogChecksumMismatch { .. })
        ));
    }
}
