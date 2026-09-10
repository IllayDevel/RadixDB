//! Public, read-only ORM admission and deterministic keyset pagination.
//!
//! This API never accepts SQL text. The ORM IR is validated before rendering,
//! then the executor independently validates the rendered SELECT while holding
//! the catalog fence used for authorization and complete result consumption.

use std::fmt;

use base64::Engine as _;
use chrono::{NaiveDate, SecondsFormat};
use radixdb_core::{DataType, Error, Row, Value};
use radixdb_executor::{
    BoundPublicReadPolicy, PublicReadLimits, PublicReadRelationBinding, PublicReadRelationSpec,
};
use radixdb_orm::{
    BinaryOperator, ColumnRef, Expression, FloatValue, IrDocument, JoinKind, NullPlacement,
    Operation, OrderBy, Projection, Relation, Select, SortDirection, TypedValue,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::orm::typed_values_to_core;
use crate::{Database, ObjectId, ServerExecutionContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicReadErrorCode {
    InvalidIr,
    UnsupportedShape,
    InvalidCursor,
    Policy,
    Authorization,
    ResourceLimit,
    Execution,
}

impl PublicReadErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidIr => "public_read.invalid_ir",
            Self::UnsupportedShape => "public_read.unsupported_shape",
            Self::InvalidCursor => "public_read.invalid_cursor",
            Self::Policy => "public_read.policy",
            Self::Authorization => "public_read.authorization",
            Self::ResourceLimit => "public_read.resource_limit",
            Self::Execution => "public_read.execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicReadError {
    code: PublicReadErrorCode,
    fingerprint: Option<String>,
    detail: &'static str,
}

impl PublicReadError {
    fn new(code: PublicReadErrorCode, fingerprint: Option<&str>, detail: &'static str) -> Self {
        Self {
            code,
            fingerprint: fingerprint.map(str::to_owned),
            detail,
        }
    }

    pub const fn code(&self) -> PublicReadErrorCode {
        self.code
    }

    pub fn fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }

    pub const fn detail(&self) -> &'static str {
        self.detail
    }
}

impl fmt::Display for PublicReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.detail)?;
        if let Some(fingerprint) = &self.fingerprint {
            write!(formatter, " [fingerprint={fingerprint}]")?;
        }
        Ok(())
    }
}

impl std::error::Error for PublicReadError {}

pub type PublicReadResult<T> = std::result::Result<T, PublicReadError>;

#[derive(Debug, Clone, PartialEq)]
pub struct PublicReadRequest {
    pub document: IrDocument,
    pub page_size: usize,
    pub cursor: Option<PublicReadCursor>,
}

impl PublicReadRequest {
    pub fn new(document: IrDocument, page_size: usize) -> Self {
        Self {
            document,
            page_size,
            cursor: None,
        }
    }

    pub fn after(mut self, cursor: PublicReadCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicReadCursor {
    pub fingerprint: String,
    /// Binds the cursor to the original parameter values without exposing
    /// them in the public diagnostic fingerprint.
    pub scope_digest: String,
    pub keys: Vec<PublicReadCursorKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicReadCursorKey {
    pub relation_ordinal: u32,
    pub column_id: String,
    pub value: TypedValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicReadPage {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub next_cursor: Option<PublicReadCursor>,
    pub fingerprint: String,
    pub database_id: [u8; 16],
    pub catalog_id: [u8; 16],
    pub catalog_generation: u64,
}

#[derive(Debug, Clone)]
struct RelationOccurrence<'a> {
    ordinal: u32,
    qualifier: String,
    binding: &'a PublicReadRelationBinding,
}

#[derive(Debug, Clone)]
struct KeyProjection {
    relation_ordinal: u32,
    qualifier: String,
    column_id: ObjectId,
    column_name: String,
    output_index: usize,
    data_type: DataType,
}

impl Database {
    /// Bind deployment configuration names to durable catalog identities.
    /// Reusing the returned policy after DROP/recreate or incompatible ALTER
    /// fails closed; names are never silently rebound during a request.
    pub fn bind_public_read_policy(
        &self,
        relations: &[PublicReadRelationSpec],
        functions: &[String],
    ) -> PublicReadResult<BoundPublicReadPolicy> {
        self.with_connection_executor(|executor| {
            executor.bind_public_read_policy(relations, functions)
        })
        .map_err(|error| public_error(error, None))
    }

    /// Execute one bounded public ORM SELECT for an authenticated principal.
    /// There is intentionally no public-read method accepting raw SQL.
    pub fn public_read(
        &self,
        request: &PublicReadRequest,
        context: &ServerExecutionContext,
        policy: &BoundPublicReadPolicy,
        limits: PublicReadLimits,
    ) -> PublicReadResult<PublicReadPage> {
        let limits = limits
            .validate()
            .map_err(|error| public_error(error, None))?;
        if request.page_size == 0 || request.page_size > limits.max_page_size {
            return Err(PublicReadError::new(
                PublicReadErrorCode::ResourceLimit,
                None,
                "page size is outside the admitted limit",
            ));
        }
        request.document.validate().map_err(|_| {
            PublicReadError::new(PublicReadErrorCode::InvalidIr, None, "invalid ORM envelope")
        })?;
        let Operation::Select { query } = &request.document.payload else {
            return Err(PublicReadError::new(
                PublicReadErrorCode::UnsupportedShape,
                None,
                "only ORM SELECT is admitted",
            ));
        };
        validate_ir_subset(query, limits)?;
        let occurrences = relation_occurrences(query.from.as_ref(), policy)?;
        let keys = bind_key_projections(query, &occurrences)?;
        let base = request.document.to_sql().map_err(|_| {
            PublicReadError::new(PublicReadErrorCode::InvalidIr, None, "ORM rendering failed")
        })?;
        let fingerprint = public_fingerprint(&base.shape_fingerprint, &occurrences, &keys);
        let scope_digest = cursor_scope_digest(&fingerprint, &base.parameters);
        validate_cursor(request.cursor.as_ref(), &fingerprint, &scope_digest, &keys)?;

        let mut admitted = query.clone();
        install_keyset_boundary(&mut admitted, request.cursor.as_ref(), &keys);
        admitted.order_by = keys
            .iter()
            .map(|key| OrderBy {
                expression: Expression::Column {
                    column: ColumnRef::qualified(&key.qualifier, &key.column_name),
                },
                direction: SortDirection::Asc,
                nulls: Some(NullPlacement::Last),
            })
            .collect();
        admitted.limit = Some((request.page_size + 1) as u64);
        admitted.offset = None;
        let admitted = IrDocument::new(Operation::Select { query: admitted });
        let compiled = admitted.to_sql().map_err(|_| {
            PublicReadError::new(
                PublicReadErrorCode::InvalidIr,
                Some(&fingerprint),
                "ORM rendering failed",
            )
        })?;
        let params = typed_values_to_core(&compiled.parameters).map_err(|_| {
            PublicReadError::new(
                PublicReadErrorCode::InvalidIr,
                Some(&fingerprint),
                "typed ORM parameter is invalid",
            )
        })?;
        let materialized = self
            .with_connection_executor(|executor| {
                executor.execute_public_read_sql(
                    &compiled.sql,
                    params.into(),
                    context.inner(),
                    policy,
                    limits,
                )
            })
            .map_err(|error| public_error(error, Some(&fingerprint)))?;

        let mut rows = materialized.rows;
        let has_more = rows.len() > request.page_size;
        if has_more {
            rows.truncate(request.page_size);
        }
        let next_cursor = if has_more {
            rows.last()
                .map(|row| cursor_from_row(&fingerprint, &scope_digest, row, &keys))
                .transpose()?
        } else {
            None
        };
        Ok(PublicReadPage {
            columns: materialized.columns,
            rows,
            next_cursor,
            fingerprint,
            database_id: materialized.database_id,
            catalog_id: materialized.catalog_id,
            catalog_generation: materialized.catalog_generation,
        })
    }
}

fn validate_ir_subset(select: &Select, limits: PublicReadLimits) -> PublicReadResult<()> {
    if !select.ctes.is_empty()
        || select.recursive
        || select.distinct
        || !select.distinct_on.is_empty()
        || select.group_by.is_some()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.set_operations.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
    {
        return Err(unsupported("server owns ordering and pagination"));
    }
    if select.projection.is_empty() || select.projection.len() > limits.max_projection {
        return Err(resource("projection width is outside the admitted limit"));
    }
    let mut joins = 0usize;
    let mut filter_nodes = 0usize;
    validate_relation(
        select.from.as_ref(),
        &mut joins,
        limits.max_navigation_depth,
        &mut filter_nodes,
    )?;
    if joins > limits.max_joins {
        return Err(resource("JOIN count exceeds the admitted limit"));
    }
    if let Some(filter) = &select.filter {
        validate_expression(filter, limits.max_navigation_depth, &mut filter_nodes)?;
    }
    if filter_nodes > limits.max_filter_nodes {
        return Err(resource("filter complexity exceeds the admitted limit"));
    }
    for projection in &select.projection {
        let mut ignored = 0;
        validate_expression(
            &projection.expression,
            limits.max_navigation_depth,
            &mut ignored,
        )?;
    }
    Ok(())
}

fn validate_relation(
    relation: Option<&Relation>,
    joins: &mut usize,
    max_navigation_depth: usize,
    predicate_nodes: &mut usize,
) -> PublicReadResult<()> {
    let Some(relation) = relation else {
        return Err(unsupported("a physical relation source is required"));
    };
    match relation {
        Relation::Table { .. } => Ok(()),
        Relation::Join {
            left,
            right,
            kind,
            on,
        } => {
            if !matches!(kind, JoinKind::Inner | JoinKind::Cross) {
                return Err(unsupported("only INNER and CROSS JOIN are admitted"));
            }
            if matches!(kind, JoinKind::Inner) && on.is_none() {
                return Err(unsupported("INNER JOIN requires an ON predicate"));
            }
            *joins = joins.saturating_add(1);
            validate_relation(Some(left), joins, max_navigation_depth, predicate_nodes)?;
            validate_relation(Some(right), joins, max_navigation_depth, predicate_nodes)?;
            if let Some(on) = on {
                validate_expression(on, max_navigation_depth, predicate_nodes)?;
            }
            Ok(())
        }
        Relation::Cte { .. } | Relation::Derived { .. } | Relation::Values { .. } => Err(
            unsupported("CTE, derived and VALUES sources are not admitted"),
        ),
    }
}

fn validate_expression(
    expression: &Expression,
    max_navigation_depth: usize,
    nodes: &mut usize,
) -> PublicReadResult<()> {
    *nodes = nodes.saturating_add(1);
    match expression {
        Expression::Column { .. } | Expression::Literal { .. } => Ok(()),
        Expression::Unary { expression, .. }
        | Expression::Cast { expression, .. }
        | Expression::IsNull { expression, .. } => {
            validate_expression(expression, max_navigation_depth, nodes)
        }
        Expression::Binary { left, right, .. } => {
            validate_expression(left, max_navigation_depth, nodes)?;
            validate_expression(right, max_navigation_depth, nodes)
        }
        Expression::Function { arguments, .. } | Expression::Tuple { values: arguments } => {
            for argument in arguments {
                validate_expression(argument, max_navigation_depth, nodes)?;
            }
            Ok(())
        }
        Expression::Case {
            operand,
            branches,
            otherwise,
        } => {
            if let Some(operand) = operand {
                validate_expression(operand, max_navigation_depth, nodes)?;
            }
            for branch in branches {
                validate_expression(&branch.when, max_navigation_depth, nodes)?;
                validate_expression(&branch.then, max_navigation_depth, nodes)?;
            }
            if let Some(otherwise) = otherwise {
                validate_expression(otherwise, max_navigation_depth, nodes)?;
            }
            Ok(())
        }
        Expression::Between {
            expression,
            lower,
            upper,
            ..
        } => {
            validate_expression(expression, max_navigation_depth, nodes)?;
            validate_expression(lower, max_navigation_depth, nodes)?;
            validate_expression(upper, max_navigation_depth, nodes)
        }
        Expression::InList {
            expression, values, ..
        } => {
            validate_expression(expression, max_navigation_depth, nodes)?;
            for value in values {
                validate_expression(value, max_navigation_depth, nodes)?;
            }
            Ok(())
        }
        Expression::Navigation { path, .. } => {
            if path.is_empty() || path.len() > max_navigation_depth {
                Err(resource("navigation depth is outside the admitted limit"))
            } else {
                Ok(())
            }
        }
        Expression::Star { .. }
        | Expression::Aggregate { .. }
        | Expression::Window { .. }
        | Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::Grouping { .. } => Err(unsupported(
            "stars, aggregates, windows, grouping and subqueries are not admitted",
        )),
    }
}

fn relation_occurrences<'a>(
    relation: Option<&Relation>,
    policy: &'a BoundPublicReadPolicy,
) -> PublicReadResult<Vec<RelationOccurrence<'a>>> {
    fn collect<'a>(
        relation: &Relation,
        policy: &'a BoundPublicReadPolicy,
        output: &mut Vec<RelationOccurrence<'a>>,
    ) -> PublicReadResult<()> {
        match relation {
            Relation::Table { name, alias } => {
                let binding = policy.relation(name).ok_or_else(|| {
                    PublicReadError::new(
                        PublicReadErrorCode::Policy,
                        None,
                        "relation is not explicitly published",
                    )
                })?;
                let ordinal = u32::try_from(output.len()).map_err(|_| {
                    resource("relation occurrence count exceeds the supported domain")
                })?;
                output.push(RelationOccurrence {
                    ordinal,
                    qualifier: alias.as_deref().unwrap_or(name).to_lowercase(),
                    binding,
                });
                Ok(())
            }
            Relation::Join { left, right, .. } => {
                collect(left, policy, output)?;
                collect(right, policy, output)
            }
            _ => Err(unsupported("relation source is outside the public subset")),
        }
    }

    let mut output = Vec::new();
    collect(
        relation.ok_or_else(|| unsupported("a relation source is required"))?,
        policy,
        &mut output,
    )?;
    let mut aliases = std::collections::BTreeSet::new();
    if output
        .iter()
        .any(|occurrence| !aliases.insert(occurrence.qualifier.clone()))
    {
        return Err(unsupported("relation aliases must be unique"));
    }
    Ok(output)
}

fn bind_key_projections(
    select: &Select,
    occurrences: &[RelationOccurrence<'_>],
) -> PublicReadResult<Vec<KeyProjection>> {
    let mut output = Vec::new();
    for occurrence in occurrences {
        for key in &occurrence.binding.primary_key {
            let indices = select
                .projection
                .iter()
                .enumerate()
                .filter_map(|(index, projection)| {
                    projection_matches_key(projection, occurrence, &key.name).then_some(index)
                })
                .collect::<Vec<_>>();
            if indices.len() != 1 {
                return Err(unsupported(
                    "every pagination key must be projected directly exactly once",
                ));
            }
            output.push(KeyProjection {
                relation_ordinal: occurrence.ordinal,
                qualifier: occurrence.qualifier.clone(),
                column_id: key.object_id,
                column_name: key.name.clone(),
                output_index: indices[0],
                data_type: key.data_type,
            });
        }
    }
    Ok(output)
}

fn projection_matches_key(
    projection: &Projection,
    occurrence: &RelationOccurrence<'_>,
    key_name: &str,
) -> bool {
    let Expression::Column { column } = &projection.expression else {
        return false;
    };
    if !column.name.eq_ignore_ascii_case(key_name) {
        return false;
    }
    match &column.relation {
        Some(relation) => relation.eq_ignore_ascii_case(&occurrence.qualifier),
        None => occurrence.ordinal == 0,
    }
}

fn public_fingerprint(
    shape: &str,
    occurrences: &[RelationOccurrence<'_>],
    keys: &[KeyProjection],
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"radixdb.public-read\0");
    hash.update(shape.as_bytes());
    for occurrence in occurrences {
        hash.update(occurrence.ordinal.to_le_bytes());
        hash.update(occurrence.binding.object_id.as_bytes());
        hash.update(occurrence.binding.definition_revision.to_le_bytes());
        for column in &occurrence.binding.columns {
            hash.update(column.object_id.as_bytes());
        }
    }
    for key in keys {
        hash.update(key.relation_ordinal.to_le_bytes());
        hash.update(key.column_id.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn cursor_scope_digest(fingerprint: &str, parameters: &[TypedValue]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"radixdb.public-read.cursor-scope\0");
    hash.update(fingerprint.as_bytes());
    hash.update(serde_json::to_vec(parameters).expect("validated ORM parameters are serializable"));
    format!("{:x}", hash.finalize())
}

fn validate_cursor(
    cursor: Option<&PublicReadCursor>,
    fingerprint: &str,
    scope_digest: &str,
    keys: &[KeyProjection],
) -> PublicReadResult<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.fingerprint != fingerprint
        || cursor.scope_digest != scope_digest
        || cursor.keys.len() != keys.len()
    {
        return Err(invalid_cursor(fingerprint));
    }
    for (cursor, key) in cursor.keys.iter().zip(keys) {
        if cursor.relation_ordinal != key.relation_ordinal
            || cursor.column_id != key.column_id.to_string()
            || !typed_value_matches(&cursor.value, key.data_type)
            || matches!(cursor.value, TypedValue::Null(_))
        {
            return Err(invalid_cursor(fingerprint));
        }
    }
    Ok(())
}

fn typed_value_matches(value: &TypedValue, data_type: DataType) -> bool {
    matches!(
        (value, data_type),
        (TypedValue::Integer(_), DataType::Integer)
            | (TypedValue::Float(_), DataType::Float)
            | (TypedValue::Text(_), DataType::Text)
            | (TypedValue::Boolean(_), DataType::Boolean)
            | (TypedValue::Timestamp(_), DataType::Timestamp)
            | (TypedValue::Date(_), DataType::Date)
            | (TypedValue::Json(_), DataType::Json)
            | (TypedValue::Uuid(_), DataType::Uuid)
            | (TypedValue::Bytes(_), DataType::Bytes)
            | (TypedValue::Decimal(_), DataType::Decimal)
            | (TypedValue::Vector(_), DataType::Vector)
    )
}

fn install_keyset_boundary(
    select: &mut Select,
    cursor: Option<&PublicReadCursor>,
    keys: &[KeyProjection],
) {
    let Some(cursor) = cursor else {
        return;
    };
    let mut disjunction = None;
    for index in 0..keys.len() {
        let mut conjunction = None;
        for (equal_index, equal_key) in keys.iter().enumerate().take(index) {
            let equality = comparison(
                equal_key,
                BinaryOperator::Eq,
                cursor.keys[equal_index].value.clone(),
            );
            conjunction = Some(and(conjunction, equality));
        }
        let greater = comparison(
            &keys[index],
            BinaryOperator::Gt,
            cursor.keys[index].value.clone(),
        );
        let arm = and(conjunction, greater);
        disjunction = Some(or(disjunction, arm));
    }
    if let Some(boundary) = disjunction {
        select.filter = Some(match select.filter.take() {
            Some(existing) => Expression::Binary {
                left: Box::new(existing),
                operator: BinaryOperator::And,
                right: Box::new(boundary),
            },
            None => boundary,
        });
    }
}

fn comparison(key: &KeyProjection, operator: BinaryOperator, value: TypedValue) -> Expression {
    Expression::Binary {
        left: Box::new(Expression::Column {
            column: ColumnRef::qualified(&key.qualifier, &key.column_name),
        }),
        operator,
        right: Box::new(Expression::Literal { value }),
    }
}

fn and(left: Option<Expression>, right: Expression) -> Expression {
    left.map_or(right.clone(), |left| Expression::Binary {
        left: Box::new(left),
        operator: BinaryOperator::And,
        right: Box::new(right),
    })
}

fn or(left: Option<Expression>, right: Expression) -> Expression {
    left.map_or(right.clone(), |left| Expression::Binary {
        left: Box::new(left),
        operator: BinaryOperator::Or,
        right: Box::new(right),
    })
}

fn cursor_from_row(
    fingerprint: &str,
    scope_digest: &str,
    row: &Row,
    keys: &[KeyProjection],
) -> PublicReadResult<PublicReadCursor> {
    let mut cursor_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let value = row
            .get(key.output_index)
            .ok_or_else(|| invalid_cursor(fingerprint))?;
        cursor_keys.push(PublicReadCursorKey {
            relation_ordinal: key.relation_ordinal,
            column_id: key.column_id.to_string(),
            value: core_to_typed(value).ok_or_else(|| invalid_cursor(fingerprint))?,
        });
    }
    Ok(PublicReadCursor {
        fingerprint: fingerprint.to_owned(),
        scope_digest: scope_digest.to_owned(),
        keys: cursor_keys,
    })
}

fn core_to_typed(value: &Value) -> Option<TypedValue> {
    match value {
        Value::Null(_) => None,
        Value::Integer(value) => Some(TypedValue::Integer(*value)),
        Value::Float(value) => Some(TypedValue::Float(FloatValue::from(*value))),
        Value::Text(value) => Some(TypedValue::Text(value.as_str().to_owned())),
        Value::Boolean(value) => Some(TypedValue::Boolean(*value)),
        Value::Timestamp(value) => Some(TypedValue::Timestamp(
            value.to_rfc3339_opts(SecondsFormat::Nanos, true),
        )),
        Value::Extension(_) => match value.data_type() {
            DataType::Json => serde_json::from_str(value.as_json()?)
                .ok()
                .map(TypedValue::Json),
            DataType::Vector => value.as_vector_f32().map(TypedValue::Vector),
            DataType::Uuid => value.as_uuid_bytes().map(|bytes| {
                TypedValue::Uuid(uuid::Uuid::from_bytes(bytes).hyphenated().to_string())
            }),
            DataType::Decimal => {
                value
                    .as_decimal_parts()
                    .map(|(coefficient, _precision, scale)| {
                        TypedValue::Decimal(decimal_string(coefficient, scale))
                    })
            }
            DataType::Date => value.as_date_days().and_then(|days| {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                epoch
                    .checked_add_signed(chrono::Duration::days(i64::from(days)))
                    .map(|date| TypedValue::Date(date.format("%Y-%m-%d").to_string()))
            }),
            DataType::Bytes => value.as_bytes_value().map(|bytes| {
                TypedValue::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes))
            }),
            _ => None,
        },
    }
}

fn decimal_string(coefficient: i128, scale: u8) -> String {
    if scale == 0 {
        return coefficient.to_string();
    }
    let negative = coefficient < 0;
    let digits = coefficient.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        &padded[..split],
        &padded[split..]
    )
}

fn public_error(error: Error, fingerprint: Option<&str>) -> PublicReadError {
    let rendered = error.to_string().to_lowercase();
    let code = if rendered.contains("authorization") || rendered.contains("permission") {
        PublicReadErrorCode::Authorization
    } else if rendered.contains("budget") || rendered.contains("limit") {
        PublicReadErrorCode::ResourceLimit
    } else if rendered.contains("public read") || rendered.contains("catalog") {
        PublicReadErrorCode::Policy
    } else {
        PublicReadErrorCode::Execution
    };
    let detail = match code {
        PublicReadErrorCode::Authorization => "authorization denied",
        PublicReadErrorCode::ResourceLimit => "resource limit exceeded",
        PublicReadErrorCode::Policy => "public policy or catalog binding rejected the request",
        _ => "query execution failed",
    };
    PublicReadError::new(code, fingerprint, detail)
}

fn unsupported(detail: &'static str) -> PublicReadError {
    PublicReadError::new(PublicReadErrorCode::UnsupportedShape, None, detail)
}

fn resource(detail: &'static str) -> PublicReadError {
    PublicReadError::new(PublicReadErrorCode::ResourceLimit, None, detail)
}

fn invalid_cursor(fingerprint: &str) -> PublicReadError {
    PublicReadError::new(
        PublicReadErrorCode::InvalidCursor,
        Some(fingerprint),
        "cursor does not match the admitted query and catalog identities",
    )
}

impl ServerExecutionContext {
    /// Construct a request context for an authenticated stable Principal.
    pub fn for_principal(principal_id: ObjectId) -> Self {
        Self {
            inner: radixdb_executor::ExecutionContext::new().with_principal_id(principal_id),
        }
    }
}

#[cfg(test)]
#[path = "public_read_tests.rs"]
mod tests;
