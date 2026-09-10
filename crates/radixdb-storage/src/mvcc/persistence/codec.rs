use super::*;

const PARTIAL_INDEX_PREDICATE_MARKER: &[u8; 4] = b"PIDX";
pub(super) const INDEX_METADATA_MARKER_V1: &[u8; 4] = b"RIX1";

/// Versioned suffix for table-level CHECK expressions in serialized schemas.
/// The base schema layout predates table checks, so the suffix is appended
/// after default values and is absent from older WAL/snapshot payloads.
const TABLE_CHECKS_MARKER_V1: &[u8; 4] = b"RCK1";
const CONSTRAINT_CATALOG_MARKER_V1: &[u8; 4] = b"RCN1";
const MAX_TABLE_CHECK_COUNT: usize = 65_535;
const MAX_TABLE_CHECK_EXPRESSION_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONSTRAINT_COUNT: usize = 65_535;

#[doc(hidden)]
pub fn checked_u16_len(label: &str, len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| {
        Error::invalid_argument(format!(
            "{label} length {len} exceeds persistent format limit {}",
            u16::MAX
        ))
    })
}

#[doc(hidden)]
pub fn checked_u32_len(label: &str, len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| {
        Error::invalid_argument(format!(
            "{label} length {len} exceeds persistent format limit {}",
            u32::MAX
        ))
    })
}

/// Validate every schema field whose persistent representation has a bounded
/// length.  This is shared by WAL admission and snapshot writing so a schema
/// accepted by the engine cannot later become an unframed artifact.
#[doc(hidden)]
pub fn validate_schema_persistence(schema: &Schema) -> Result<()> {
    if schema.table_name.is_empty() {
        return Err(Error::invalid_argument("schema table name cannot be empty"));
    }
    schema.validate_foreign_key_invariants()?;
    checked_u16_len("table name", schema.table_name.len())?;
    checked_u16_len("schema column count", schema.columns.len())?;
    checked_u16_len("foreign key count", schema.foreign_keys.len())?;

    let mut column_names = std::collections::HashSet::with_capacity(schema.columns.len());
    for (index, column) in schema.columns.iter().enumerate() {
        if column.id != index {
            return Err(Error::invalid_argument(format!(
                "column '{}' has ordinal {}, expected {index}",
                column.name, column.id
            )));
        }
        if column.name.is_empty() {
            return Err(Error::invalid_argument("column name cannot be empty"));
        }
        if column.name_lower != column.name.to_lowercase() {
            return Err(Error::invalid_argument(format!(
                "column '{}' has stale lowercase identity '{}'",
                column.name, column.name_lower
            )));
        }
        if !column_names.insert(column.name_lower.clone()) {
            return Err(Error::DuplicateColumn);
        }
        if column.auto_increment && !matches!(column.data_type, DataType::Integer | DataType::Uuid)
        {
            return Err(Error::invalid_argument(format!(
                "auto-increment column '{}' must be INTEGER or UUID",
                column.name
            )));
        }
        if column.data_type == DataType::Decimal {
            if column.decimal_precision > 38
                || (column.decimal_precision == 0 && column.decimal_scale != 0)
                || column.decimal_scale > column.decimal_precision
            {
                return Err(Error::invalid_argument(format!(
                    "column '{}' has invalid DECIMAL({},{}) parameters",
                    column.name, column.decimal_precision, column.decimal_scale
                )));
            }
        } else if column.decimal_precision != 0 || column.decimal_scale != 0 {
            return Err(Error::invalid_argument(format!(
                "non-DECIMAL column '{}' carries DECIMAL parameters",
                column.name
            )));
        }
        checked_u16_len(&format!("column {index} name"), column.name.len())?;
        if let Some(expression) = &column.default_expr {
            checked_u16_len(
                &format!("column '{}' DEFAULT expression", column.name),
                expression.len(),
            )?;
        }
        if let Some(expression) = &column.check_expr {
            checked_u16_len(
                &format!("column '{}' CHECK expression", column.name),
                expression.len(),
            )?;
        }
        if let Some(value) = &column.default_value {
            let encoded = serialize_value(value)?;
            checked_u16_len(
                &format!("column '{}' default value", column.name),
                encoded.len(),
            )?;
        }
    }

    for (index, foreign_key) in schema.foreign_keys.iter().enumerate() {
        checked_u16_len(
            &format!("foreign key {index} column index"),
            foreign_key.column_index,
        )?;
        checked_u16_len(
            &format!("foreign key {index} column name"),
            foreign_key.column_name.len(),
        )?;
        checked_u16_len(
            &format!("foreign key {index} referenced table"),
            foreign_key.referenced_table.len(),
        )?;
        checked_u16_len(
            &format!("foreign key {index} referenced column"),
            foreign_key.referenced_column.len(),
        )?;
    }

    if schema.table_checks.len() > MAX_TABLE_CHECK_COUNT {
        return Err(Error::invalid_argument(format!(
            "table CHECK count {} exceeds persistent format limit {}",
            schema.table_checks.len(),
            MAX_TABLE_CHECK_COUNT
        )));
    }
    for expression in &schema.table_checks {
        if expression.len() > MAX_TABLE_CHECK_EXPRESSION_BYTES {
            return Err(Error::invalid_argument(format!(
                "table CHECK expression length {} exceeds persistent format limit {}",
                expression.len(),
                MAX_TABLE_CHECK_EXPRESSION_BYTES
            )));
        }
        checked_u32_len("table CHECK expression", expression.len())?;
    }
    schema.validate_constraint_catalog()?;
    if schema.constraints.len() > MAX_CONSTRAINT_COUNT {
        return Err(Error::invalid_argument(format!(
            "constraint count {} exceeds persistent format limit {}",
            schema.constraints.len(),
            MAX_CONSTRAINT_COUNT
        )));
    }
    for constraint in &schema.constraints {
        checked_u16_len("constraint name", constraint.name.len())?;
        for column in constraint.kind.columns() {
            checked_u16_len("constraint column name", column.len())?;
        }
    }

    Ok(())
}

#[doc(hidden)]
pub fn serialize_table_checks(buf: &mut Vec<u8>, checks: &[String]) -> Result<()> {
    if checks.len() > MAX_TABLE_CHECK_COUNT {
        return Err(Error::invalid_argument(format!(
            "table CHECK count {} exceeds persistent format limit {}",
            checks.len(),
            MAX_TABLE_CHECK_COUNT
        )));
    }
    buf.extend_from_slice(TABLE_CHECKS_MARKER_V1);
    buf.extend_from_slice(&checked_u32_len("table CHECK count", checks.len())?.to_le_bytes());
    for expression in checks {
        if expression.len() > MAX_TABLE_CHECK_EXPRESSION_BYTES {
            return Err(Error::invalid_argument(format!(
                "table CHECK expression length {} exceeds persistent format limit {}",
                expression.len(),
                MAX_TABLE_CHECK_EXPRESSION_BYTES
            )));
        }
        buf.extend_from_slice(
            &checked_u32_len("table CHECK expression", expression.len())?.to_le_bytes(),
        );
        buf.extend_from_slice(expression.as_bytes());
    }
    Ok(())
}

#[doc(hidden)]
pub fn deserialize_table_checks(data: &[u8], pos: &mut usize) -> Result<Vec<String>> {
    if *pos == data.len() {
        return Ok(Vec::new());
    }
    if data.len().saturating_sub(*pos) < 8 {
        return Err(Error::internal(
            "corrupted schema: truncated table CHECK extension",
        ));
    }
    if &data[*pos..*pos + 4] != TABLE_CHECKS_MARKER_V1 {
        return Err(Error::internal(
            "corrupted schema: unknown trailing schema extension",
        ));
    }
    *pos += 4;

    let count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if count > MAX_TABLE_CHECK_COUNT {
        return Err(Error::internal(format!(
            "corrupted schema: table CHECK count {} exceeds {}",
            count, MAX_TABLE_CHECK_COUNT
        )));
    }
    let payload_derived_max = data.len().saturating_sub(*pos) / 4;
    if count > payload_derived_max {
        return Err(Error::internal(format!(
            "corrupted schema: table CHECK count {} exceeds payload-derived maximum {}",
            count, payload_derived_max
        )));
    }

    let mut checks = Vec::with_capacity(count);
    for _ in 0..count {
        if data.len().saturating_sub(*pos) < 4 {
            return Err(Error::internal(
                "corrupted schema: truncated table CHECK length",
            ));
        }
        let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        if len > MAX_TABLE_CHECK_EXPRESSION_BYTES {
            return Err(Error::internal(format!(
                "corrupted schema: table CHECK expression length {} exceeds {}",
                len, MAX_TABLE_CHECK_EXPRESSION_BYTES
            )));
        }
        if data.len().saturating_sub(*pos) < len {
            return Err(Error::internal(
                "corrupted schema: truncated table CHECK expression",
            ));
        }
        let expression = std::str::from_utf8(&data[*pos..*pos + len])
            .map_err(|error| Error::internal(format!("invalid table CHECK UTF-8: {error}")))?
            .to_owned();
        *pos += len;
        checks.push(expression);
    }

    if *pos != data.len()
        && data.get(*pos..pos.saturating_add(4)) != Some(CONSTRAINT_CATALOG_MARKER_V1)
    {
        return Err(Error::internal(
            "corrupted schema: trailing bytes after table CHECK extension",
        ));
    }

    Ok(checks)
}

fn serialize_catalog_string(buf: &mut Vec<u8>, label: &str, value: &str) -> Result<()> {
    buf.extend_from_slice(&checked_u16_len(label, value.len())?.to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn serialize_catalog_columns(buf: &mut Vec<u8>, columns: &[String]) -> Result<()> {
    buf.extend_from_slice(
        &checked_u16_len("constraint column count", columns.len())?.to_le_bytes(),
    );
    for column in columns {
        serialize_catalog_string(buf, "constraint column name", column)?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn serialize_constraint_catalog(buf: &mut Vec<u8>, schema: &Schema) -> Result<()> {
    let mut normalized;
    let schema = if schema.constraints().is_empty() {
        normalized = schema.clone();
        normalized.ensure_constraint_catalog()?;
        &normalized
    } else {
        schema.validate_constraint_catalog_complete()?;
        schema
    };
    validate_schema_persistence(schema)?;
    buf.extend_from_slice(CONSTRAINT_CATALOG_MARKER_V1);
    buf.extend_from_slice(&schema.next_constraint_id.to_le_bytes());
    buf.extend_from_slice(&schema.next_check_ordinal.to_le_bytes());
    buf.extend_from_slice(
        &checked_u16_len("constraint count", schema.constraints.len())?.to_le_bytes(),
    );
    for constraint in &schema.constraints {
        buf.extend_from_slice(&constraint.id.to_le_bytes());
        serialize_catalog_string(buf, "constraint name", &constraint.name)?;
        match &constraint.kind {
            SchemaConstraintKind::PrimaryKey { columns } => {
                buf.push(1);
                serialize_catalog_columns(buf, columns)?;
            }
            SchemaConstraintKind::Unique {
                columns,
                index_name,
            } => {
                buf.push(2);
                serialize_catalog_columns(buf, columns)?;
                serialize_catalog_string(buf, "UNIQUE owned index name", index_name)?;
            }
            SchemaConstraintKind::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => {
                buf.push(3);
                serialize_catalog_columns(buf, columns)?;
                serialize_catalog_string(buf, "referenced table", referenced_table)?;
                serialize_catalog_columns(buf, referenced_columns)?;
                buf.push(on_delete.as_u8());
                buf.push(on_update.as_u8());
            }
            SchemaConstraintKind::Check {
                column_name,
                expression,
                ordinal,
            } => {
                buf.push(4);
                match column_name {
                    Some(column_name) => {
                        buf.push(1);
                        serialize_catalog_string(buf, "CHECK column name", column_name)?;
                    }
                    None => buf.push(0),
                }
                buf.extend_from_slice(&ordinal.to_le_bytes());
                buf.extend_from_slice(
                    &checked_u32_len("CHECK expression", expression.len())?.to_le_bytes(),
                );
                buf.extend_from_slice(expression.as_bytes());
            }
        }
    }
    Ok(())
}

fn read_catalog_bytes<'a>(
    data: &'a [u8],
    pos: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| Error::internal(format!("corrupted schema: {label} length overflow")))?;
    let bytes = data
        .get(*pos..end)
        .ok_or_else(|| Error::internal(format!("corrupted schema: truncated {label}")))?;
    *pos = end;
    Ok(bytes)
}

fn read_catalog_u16(data: &[u8], pos: &mut usize, label: &str) -> Result<u16> {
    let bytes = read_catalog_bytes(data, pos, 2, label)?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_catalog_u32(data: &[u8], pos: &mut usize, label: &str) -> Result<u32> {
    let bytes = read_catalog_bytes(data, pos, 4, label)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_catalog_u64(data: &[u8], pos: &mut usize, label: &str) -> Result<u64> {
    let bytes = read_catalog_bytes(data, pos, 8, label)?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_catalog_string(data: &[u8], pos: &mut usize, label: &str) -> Result<String> {
    let len = read_catalog_u16(data, pos, &format!("{label} length"))? as usize;
    let bytes = read_catalog_bytes(data, pos, len, label)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| {
            Error::internal(format!("corrupted schema: invalid {label} UTF-8: {error}"))
        })
}

fn read_catalog_columns(data: &[u8], pos: &mut usize) -> Result<Vec<String>> {
    let count = read_catalog_u16(data, pos, "constraint column count")? as usize;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(read_catalog_string(data, pos, "constraint column name")?);
    }
    Ok(columns)
}

#[doc(hidden)]
pub fn deserialize_constraint_catalog(
    data: &[u8],
    pos: &mut usize,
) -> Result<(Vec<SchemaConstraint>, u64, u32)> {
    if data.get(*pos..pos.saturating_add(4)) != Some(CONSTRAINT_CATALOG_MARKER_V1) {
        return Err(Error::internal(
            "pre-naming schema catalog is unsupported; logical export/import into a new database is required",
        ));
    }
    *pos += 4;
    let next_constraint_id = read_catalog_u64(data, pos, "next constraint identity")?;
    let next_check_ordinal = read_catalog_u32(data, pos, "next CHECK ordinal")?;
    let count = read_catalog_u16(data, pos, "constraint count")? as usize;
    if count > MAX_CONSTRAINT_COUNT {
        return Err(Error::internal(
            "corrupted schema: constraint count exceeds limit",
        ));
    }
    let mut constraints = Vec::with_capacity(count);
    for _ in 0..count {
        let id = read_catalog_u64(data, pos, "constraint identity")?;
        let name = read_catalog_string(data, pos, "constraint name")?;
        let tag = *read_catalog_bytes(data, pos, 1, "constraint kind")?
            .first()
            .unwrap();
        let kind = match tag {
            1 => SchemaConstraintKind::PrimaryKey {
                columns: read_catalog_columns(data, pos)?,
            },
            2 => SchemaConstraintKind::Unique {
                columns: read_catalog_columns(data, pos)?,
                index_name: read_catalog_string(data, pos, "UNIQUE owned index name")?,
            },
            3 => {
                let columns = read_catalog_columns(data, pos)?;
                let referenced_table = read_catalog_string(data, pos, "referenced table")?;
                let referenced_columns = read_catalog_columns(data, pos)?;
                let actions = read_catalog_bytes(data, pos, 2, "foreign key actions")?;
                let on_delete = radixdb_core::ForeignKeyAction::from_u8(actions[0])
                    .ok_or_else(|| Error::internal("corrupted schema: invalid ON DELETE action"))?;
                let on_update = radixdb_core::ForeignKeyAction::from_u8(actions[1])
                    .ok_or_else(|| Error::internal("corrupted schema: invalid ON UPDATE action"))?;
                SchemaConstraintKind::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    on_delete,
                    on_update,
                }
            }
            4 => {
                let has_column = *read_catalog_bytes(data, pos, 1, "CHECK column flag")?
                    .first()
                    .unwrap();
                let column_name = match has_column {
                    0 => None,
                    1 => Some(read_catalog_string(data, pos, "CHECK column name")?),
                    _ => {
                        return Err(Error::internal(
                            "corrupted schema: invalid CHECK column flag",
                        ))
                    }
                };
                let ordinal = read_catalog_u32(data, pos, "CHECK ordinal")?;
                let expression_len =
                    read_catalog_u32(data, pos, "CHECK expression length")? as usize;
                if expression_len > MAX_TABLE_CHECK_EXPRESSION_BYTES {
                    return Err(Error::internal(
                        "corrupted schema: CHECK expression exceeds limit",
                    ));
                }
                let expression = std::str::from_utf8(read_catalog_bytes(
                    data,
                    pos,
                    expression_len,
                    "CHECK expression",
                )?)
                .map(str::to_owned)
                .map_err(|error| Error::internal(format!("invalid CHECK UTF-8: {error}")))?;
                SchemaConstraintKind::Check {
                    column_name,
                    expression,
                    ordinal,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "corrupted schema: unknown constraint kind tag {tag}"
                )))
            }
        };
        constraints.push(SchemaConstraint { id, name, kind });
    }
    if *pos != data.len() {
        return Err(Error::internal(
            "corrupted schema: trailing bytes after constraint catalog",
        ));
    }
    Ok((constraints, next_constraint_id, next_check_ordinal))
}

/// Index metadata for persistence
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Index name
    pub name: String,
    /// Table the index belongs to
    pub table_name: String,
    /// Names of the columns this index is for
    pub column_names: Vec<String>,
    /// IDs of the columns in the table schema
    pub column_ids: Vec<i32>,
    /// Types of data in the index
    pub data_types: Vec<DataType>,
    /// Whether the index enforces uniqueness
    pub is_unique: bool,
    /// Type of index (BTree, Hash, Bitmap)
    pub index_type: IndexType,
    /// HNSW parameter: max connections per node per layer (default: 16)
    pub hnsw_m: Option<u16>,
    /// HNSW parameter: build beam width (default: 200)
    pub hnsw_ef_construction: Option<u16>,
    /// HNSW parameter: search beam width (default: 64)
    pub hnsw_ef_search: Option<u16>,
    /// HNSW parameter: distance metric (0=L2, 1=Cosine, 2=InnerProduct)
    pub hnsw_distance_metric: Option<u8>,
    /// Optional partial-index predicate metadata.
    pub partial_predicate: Option<PartialIndexPredicateMetadata>,
}

impl IndexMetadata {
    /// Serialize to binary format
    pub fn serialize(&self) -> Result<Vec<u8>> {
        if self.column_names.len() != self.column_ids.len()
            || self.column_names.len() != self.data_types.len()
        {
            return Err(Error::invalid_argument(format!(
                "index metadata arrays have different lengths: names={}, ids={}, types={}",
                self.column_names.len(),
                self.column_ids.len(),
                self.data_types.len()
            )));
        }

        let mut buf = Vec::new();
        buf.extend_from_slice(INDEX_METADATA_MARKER_V1);

        // Index name
        buf.extend_from_slice(&checked_u16_len("index name", self.name.len())?.to_le_bytes());
        buf.extend_from_slice(self.name.as_bytes());

        // Table name
        buf.extend_from_slice(
            &checked_u16_len("index table name", self.table_name.len())?.to_le_bytes(),
        );
        buf.extend_from_slice(self.table_name.as_bytes());

        // Column count
        buf.extend_from_slice(
            &checked_u16_len("index column count", self.column_names.len())?.to_le_bytes(),
        );

        // Column names
        for name in &self.column_names {
            buf.extend_from_slice(&checked_u16_len("index column name", name.len())?.to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
        }

        // Column IDs
        for id in &self.column_ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }

        // Data types
        buf.extend_from_slice(
            &checked_u16_len("index data type count", self.data_types.len())?.to_le_bytes(),
        );
        for dt in &self.data_types {
            buf.push(dt.as_u8());
        }

        // Unique flag
        buf.push(if self.is_unique { 1 } else { 0 });

        // Index type (1 byte: 0=BTree, 1=Hash, 2=Bitmap, 3=MultiColumn)
        let index_type_byte = match self.index_type {
            IndexType::BTree => 0,
            IndexType::Hash => 1,
            IndexType::Bitmap => 2,
            IndexType::MultiColumn => 3,
            IndexType::Hnsw => 5,
            IndexType::PrimaryKey => return Err(Error::invalid_argument(
                "primary-key indexes are schema-owned and cannot be serialized as IndexMetadata",
            )),
        };
        buf.push(index_type_byte);

        // HNSW-specific parameters (appended after index type for backward compat)
        if self.index_type == IndexType::Hnsw {
            buf.extend_from_slice(&self.hnsw_m.unwrap_or(16).to_le_bytes());
            buf.extend_from_slice(&self.hnsw_ef_construction.unwrap_or(200).to_le_bytes());
            buf.extend_from_slice(&self.hnsw_ef_search.unwrap_or(64).to_le_bytes());
            let metric = self.hnsw_distance_metric.unwrap_or(0);
            if metric > 2 {
                return Err(Error::invalid_argument(format!(
                    "invalid HNSW distance metric tag {metric}"
                )));
            }
            buf.push(metric); // 0 = L2
        }

        if let Some(predicate) = &self.partial_predicate {
            let canonical_sql = predicate.canonical_sql().as_bytes();
            buf.extend_from_slice(PARTIAL_INDEX_PREDICATE_MARKER);
            buf.extend_from_slice(
                &checked_u32_len("partial index predicate", canonical_sql.len())?.to_le_bytes(),
            );
            buf.extend_from_slice(canonical_sql);
        }

        Ok(buf)
    }

    /// Deserialize from binary format
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::internal("empty metadata"));
        }

        if !data.starts_with(INDEX_METADATA_MARKER_V1) {
            return Err(Error::internal(
                "unsupported unversioned index metadata; expected RIX1 marker",
            ));
        }
        let versioned = true;
        let mut pos = INDEX_METADATA_MARKER_V1.len();

        // Index name
        if pos + 2 > data.len() {
            return Err(Error::internal("invalid metadata: missing name length"));
        }
        let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + name_len > data.len() {
            return Err(Error::internal("invalid metadata: missing name"));
        }
        let name = String::from_utf8(data[pos..pos + name_len].to_vec())
            .map_err(|e| Error::internal(format!("invalid name: {}", e)))?;
        pos += name_len;

        // Table name
        if pos + 2 > data.len() {
            return Err(Error::internal(
                "invalid metadata: missing table name length",
            ));
        }
        let table_name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + table_name_len > data.len() {
            return Err(Error::internal("invalid metadata: missing table name"));
        }
        let table_name = String::from_utf8(data[pos..pos + table_name_len].to_vec())
            .map_err(|e| Error::internal(format!("invalid table name: {}", e)))?;
        pos += table_name_len;

        // Column count
        if pos + 2 > data.len() {
            return Err(Error::internal("invalid metadata: missing column count"));
        }
        let column_count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        // Column names
        let mut column_names = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if pos + 2 > data.len() {
                return Err(Error::internal(
                    "invalid metadata: missing column name length",
                ));
            }
            let col_name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            if pos + col_name_len > data.len() {
                return Err(Error::internal("invalid metadata: missing column name"));
            }
            let col_name = String::from_utf8(data[pos..pos + col_name_len].to_vec())
                .map_err(|e| Error::internal(format!("invalid column name: {}", e)))?;
            pos += col_name_len;
            column_names.push(col_name);
        }

        // Column IDs
        let mut column_ids = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if pos + 4 > data.len() {
                return Err(Error::internal("invalid metadata: missing column ID"));
            }
            column_ids.push(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
            pos += 4;
        }

        // Data types
        if pos + 2 > data.len() {
            return Err(Error::internal("invalid metadata: missing data type count"));
        }
        let data_type_count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut data_types = Vec::with_capacity(data_type_count);
        for _ in 0..data_type_count {
            if pos >= data.len() {
                return Err(Error::internal("invalid metadata: missing data type"));
            }
            let tag = data[pos];
            let dt = DataType::from_u8(tag).ok_or_else(|| {
                Error::internal(format!("invalid metadata: unknown data type tag {tag}"))
            })?;
            pos += 1;
            data_types.push(dt);
        }

        if data_type_count != column_count {
            return Err(Error::internal(format!(
                "invalid metadata: data type count {data_type_count} does not match column count {column_count}"
            )));
        }

        // Unique flag
        let is_unique = if pos < data.len() {
            let tag = data[pos];
            pos += 1;
            match tag {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::internal(format!(
                        "invalid metadata: unknown unique flag {tag}"
                    )))
                }
            }
        } else if versioned {
            return Err(Error::internal("invalid metadata: missing unique flag"));
        } else {
            false
        };

        // Index type (1 byte: 0=BTree, 1=Hash, 2=Bitmap, 3=MultiColumn)
        let index_type = if pos < data.len() {
            let t = match data[pos] {
                0 => IndexType::BTree,
                1 => IndexType::Hash,
                2 => IndexType::Bitmap,
                3 => IndexType::MultiColumn,
                5 => IndexType::Hnsw,
                tag => {
                    return Err(Error::internal(format!(
                        "invalid metadata: unknown index type tag {tag}"
                    )))
                }
            };
            pos += 1;
            t
        } else if versioned {
            return Err(Error::internal("invalid metadata: missing index type"));
        } else {
            IndexType::BTree
        };

        // HNSW-specific parameters (trailing bytes, backward compat: use defaults if missing)
        let (hnsw_m, hnsw_ef_construction, hnsw_ef_search, hnsw_distance_metric) =
            if index_type == IndexType::Hnsw && pos + 7 <= data.len() {
                let m = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
                pos += 2;
                let ef_c = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
                pos += 2;
                let ef_s = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
                pos += 2;
                let metric = data[pos];
                pos += 1;
                if metric > 2 {
                    return Err(Error::internal(format!(
                        "invalid metadata: unknown HNSW distance metric tag {metric}"
                    )));
                }
                (Some(m), Some(ef_c), Some(ef_s), Some(metric))
            } else if index_type == IndexType::Hnsw && versioned {
                return Err(Error::internal(
                    "invalid metadata: truncated HNSW parameter block",
                ));
            } else {
                (None, None, None, None)
            };

        let partial_predicate = if pos < data.len() {
            if pos + PARTIAL_INDEX_PREDICATE_MARKER.len() > data.len()
                || &data[pos..pos + PARTIAL_INDEX_PREDICATE_MARKER.len()]
                    != PARTIAL_INDEX_PREDICATE_MARKER
            {
                return Err(Error::internal(
                    "invalid metadata: unknown trailing index metadata marker",
                ));
            }
            pos += PARTIAL_INDEX_PREDICATE_MARKER.len();

            if pos + 4 > data.len() {
                return Err(Error::internal(
                    "invalid metadata: missing partial index predicate length",
                ));
            }
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + len > data.len() {
                return Err(Error::internal(
                    "invalid metadata: partial index predicate exceeds metadata length",
                ));
            }
            let canonical_sql = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|e| Error::internal(format!("invalid partial predicate: {}", e)))?;
            pos += len;

            if pos != data.len() {
                return Err(Error::internal(
                    "invalid metadata: unexpected bytes after partial index predicate",
                ));
            }

            Some(PartialIndexPredicateMetadata::new(canonical_sql))
        } else {
            None
        };

        Ok(Self {
            name,
            table_name,
            column_names,
            column_ids,
            data_types,
            is_unique,
            index_type,
            hnsw_m,
            hnsw_ef_construction,
            hnsw_ef_search,
            hnsw_distance_metric,
            partial_predicate,
        })
    }
}

impl From<&IndexMetadata> for crate::mvcc::IndexDefinition {
    fn from(metadata: &IndexMetadata) -> Self {
        Self {
            name: metadata.name.clone(),
            table_name: metadata.table_name.clone(),
            column_names: metadata.column_names.clone(),
            column_ids: metadata.column_ids.clone(),
            data_types: metadata.data_types.clone(),
            is_unique: metadata.is_unique,
            index_type: metadata.index_type,
            hnsw_m: metadata.hnsw_m,
            hnsw_ef_construction: metadata.hnsw_ef_construction,
            hnsw_ef_search: metadata.hnsw_ef_search,
            hnsw_distance_metric: metadata.hnsw_distance_metric,
            partial_predicate: metadata.partial_predicate.clone(),
            // Catalog recovery is the authority for external operator-class
            // callbacks. Legacy index metadata never reconstructs one.
            key_encoder: None,
        }
    }
}
