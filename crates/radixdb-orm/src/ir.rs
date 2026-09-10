use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{DataTypeDescriptor, ResultColumnDescriptor};

pub const ORM_IR_VERSION: &str = "radixdb.orm.v1";

#[derive(Debug, thiserror::Error)]
pub enum IrError {
    #[error("unsupported ORM IR version '{0}'")]
    UnsupportedVersion(String),
    #[error("ORM IR kind mismatch: envelope is {envelope:?}, operation is {operation:?}")]
    KindMismatch { envelope: IrKind, operation: IrKind },
    #[error("ORM IR JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IrKind {
    Catalog,
    Ddl,
    Select,
    Insert,
    Upsert,
    Update,
    Delete,
    Explain,
    Transaction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IrDocument {
    pub ir: String,
    pub kind: IrKind,
    pub payload: Operation,
}

impl IrDocument {
    pub fn new(payload: Operation) -> Self {
        Self {
            ir: ORM_IR_VERSION.to_string(),
            kind: payload.kind(),
            payload,
        }
    }

    pub fn validate(&self) -> Result<(), IrError> {
        if self.ir != ORM_IR_VERSION {
            return Err(IrError::UnsupportedVersion(self.ir.clone()));
        }
        let operation = self.payload.kind();
        if self.kind != operation {
            return Err(IrError::KindMismatch {
                envelope: self.kind,
                operation,
            });
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String, IrError> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }

    pub fn to_pretty_json(&self) -> Result<String, IrError> {
        self.validate()?;
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, IrError> {
        let document: Self = serde_json::from_str(json)?;
        document.validate()?;
        Ok(document)
    }

    /// Serialize a log-safe form. Typed values keep their public type tag but
    /// their payload is replaced; executable JSON is never used as log JSON.
    pub fn to_redacted_json(&self) -> Result<String, IrError> {
        self.validate()?;
        let mut value = serde_json::to_value(self)?;
        redact_typed_values(&mut value);
        Ok(serde_json::to_string(&value)?)
    }
}

fn redact_typed_values(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                redact_typed_values(value);
            }
        }
        serde_json::Value::Object(object) => {
            let is_typed_value = object
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|tag| {
                    matches!(
                        tag,
                        "integer"
                            | "float"
                            | "text"
                            | "boolean"
                            | "timestamp"
                            | "date"
                            | "json"
                            | "uuid"
                            | "bytes"
                            | "decimal"
                            | "vector"
                    )
                });
            if is_typed_value && object.contains_key("value") {
                object.insert(
                    "value".to_string(),
                    serde_json::Value::String("<redacted>".to_string()),
                );
                return;
            }
            for value in object.values_mut() {
                redact_typed_values(value);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
// This is the stable, language-neutral JSON root. Boxing selected variants
// would leak an allocation-driven Rust detail into every constructor without
// improving the wire representation, so the size trade-off is intentional.
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    Catalog { operation: CatalogOperation },
    Ddl { operation: DdlOperation },
    Select { query: Select },
    Insert { statement: Insert },
    Upsert { statement: Upsert },
    Update { statement: Update },
    Delete { statement: Delete },
    Explain { statement: Explain },
    Transaction { statement: TransactionOperation },
}

impl Operation {
    pub fn kind(&self) -> IrKind {
        match self {
            Self::Catalog { .. } => IrKind::Catalog,
            Self::Ddl { .. } => IrKind::Ddl,
            Self::Select { .. } => IrKind::Select,
            Self::Insert { .. } => IrKind::Insert,
            Self::Upsert { .. } => IrKind::Upsert,
            Self::Update { .. } => IrKind::Update,
            Self::Delete { .. } => IrKind::Delete,
            Self::Explain { .. } => IrKind::Explain,
            Self::Transaction { .. } => IrKind::Transaction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum CatalogOperation {
    ListTables,
    DescribeTable { table: String },
    DescribeDatabase,
    ShowIndexes { table: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum TypedValue {
    Null(DataTypeDescriptor),
    Integer(i64),
    Float(FloatValue),
    Text(String),
    Boolean(bool),
    Timestamp(String),
    Date(String),
    Json(serde_json::Value),
    Uuid(String),
    Bytes(String),
    Decimal(String),
    Vector(Vec<f32>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FloatValue {
    Number(f64),
    Special(FloatSpecial),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FloatSpecial {
    Nan,
    PositiveInfinity,
    NegativeInfinity,
}

impl From<f64> for FloatValue {
    fn from(value: f64) -> Self {
        if value.is_nan() {
            Self::Special(FloatSpecial::Nan)
        } else if value == f64::INFINITY {
            Self::Special(FloatSpecial::PositiveInfinity)
        } else if value == f64::NEG_INFINITY {
            Self::Special(FloatSpecial::NegativeInfinity)
        } else {
            Self::Number(value)
        }
    }
}

impl FloatValue {
    pub fn as_f64(self) -> f64 {
        match self {
            Self::Number(value) => value,
            Self::Special(FloatSpecial::Nan) => f64::NAN,
            Self::Special(FloatSpecial::PositiveInfinity) => f64::INFINITY,
            Self::Special(FloatSpecial::NegativeInfinity) => f64::NEG_INFINITY,
        }
    }
}

impl TypedValue {
    pub fn data_type(&self) -> DataTypeDescriptor {
        match self {
            Self::Null(data_type) => data_type.clone(),
            Self::Integer(_) => DataTypeDescriptor::Integer,
            Self::Float(_) => DataTypeDescriptor::Float,
            Self::Text(_) => DataTypeDescriptor::Text,
            Self::Boolean(_) => DataTypeDescriptor::Boolean,
            Self::Timestamp(_) => DataTypeDescriptor::Timestamp,
            Self::Date(_) => DataTypeDescriptor::Date,
            Self::Json(_) => DataTypeDescriptor::Json,
            Self::Uuid(_) => DataTypeDescriptor::Uuid,
            Self::Bytes(_) => DataTypeDescriptor::Bytes,
            Self::Decimal(_) => DataTypeDescriptor::Decimal {
                precision: None,
                scale: None,
            },
            Self::Vector(values) => DataTypeDescriptor::Vector {
                dimensions: u16::try_from(values.len()).unwrap_or(u16::MAX),
            },
        }
    }

    /// Decode the language-neutral DECIMAL text form into RadixDB's exact
    /// coefficient/precision/scale tuple. SDK transports use this one parser
    /// so embedded and TCP execution cannot disagree about admission.
    pub fn decimal_parts(&self) -> Result<(i128, u8, u8), TypedValueError> {
        match self {
            Self::Decimal(value) => parse_decimal_literal(value),
            _ => Err(TypedValueError::ExpectedDecimal),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum TypedValueError {
    #[error("expected a DECIMAL typed value")]
    ExpectedDecimal,
    #[error("invalid DECIMAL literal '{0}'")]
    InvalidDecimal(String),
    #[error("DECIMAL precision must be in 1..=38")]
    DecimalPrecision,
    #[error("DECIMAL scale must not exceed precision")]
    DecimalScale,
    #[error("DECIMAL coefficient exceeds declared precision")]
    DecimalCoefficient,
}

pub fn parse_decimal_literal(value: &str) -> Result<(i128, u8, u8), TypedValueError> {
    let value = value.trim();
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fractional = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(TypedValueError::InvalidDecimal(value.to_string()));
    }

    let digits = format!("{integer}{fractional}");
    let precision = u8::try_from(digits.len()).map_err(|_| TypedValueError::DecimalPrecision)?;
    let scale = u8::try_from(fractional.len()).map_err(|_| TypedValueError::DecimalPrecision)?;
    if precision == 0 || precision > 38 {
        return Err(TypedValueError::DecimalPrecision);
    }
    if scale > precision {
        return Err(TypedValueError::DecimalScale);
    }

    let mut unscaled = digits
        .parse::<i128>()
        .map_err(|_| TypedValueError::DecimalCoefficient)?;
    if negative {
        unscaled = unscaled
            .checked_neg()
            .ok_or(TypedValueError::DecimalCoefficient)?;
    }
    let coefficient_digits = if unscaled == 0 {
        1
    } else {
        unscaled.unsigned_abs().ilog10() as usize + 1
    };
    if coefficient_digits > usize::from(precision) {
        return Err(TypedValueError::DecimalCoefficient);
    }
    Ok((unscaled, precision, scale))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub relation: Option<String>,
    pub name: String,
}

impl ColumnRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    pub fn qualified(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            relation: Some(relation.into()),
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOperator {
    Not,
    Negate,
    Positive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Like,
    NotLike,
    Glob,
    Regexp,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum Expression {
    Column {
        column: ColumnRef,
    },
    Literal {
        value: TypedValue,
    },
    Star {
        relation: Option<String>,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<Expression>,
    },
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Function {
        name: String,
        arguments: Vec<Expression>,
    },
    Aggregate {
        name: String,
        arguments: Vec<Expression>,
        distinct: bool,
        filter: Option<Box<Expression>>,
        order_by: Vec<OrderBy>,
    },
    Window {
        function: Box<Expression>,
        specification: WindowSpecification,
    },
    Cast {
        expression: Box<Expression>,
        data_type: DataTypeDescriptor,
    },
    Case {
        operand: Option<Box<Expression>>,
        branches: Vec<CaseBranch>,
        otherwise: Option<Box<Expression>>,
    },
    IsNull {
        expression: Box<Expression>,
        negated: bool,
    },
    Between {
        expression: Box<Expression>,
        lower: Box<Expression>,
        upper: Box<Expression>,
        negated: bool,
    },
    InList {
        expression: Box<Expression>,
        values: Vec<Expression>,
        negated: bool,
    },
    InSubquery {
        expression: Box<Expression>,
        query: Box<Select>,
        negated: bool,
    },
    Exists {
        query: Box<Select>,
        negated: bool,
    },
    ScalarSubquery {
        query: Box<Select>,
    },
    Tuple {
        values: Vec<Expression>,
    },
    /// Read-only RadixDB navigation path. `root` is a table alias and every
    /// path segment is an authoritative reference/column identifier.
    Navigation {
        root: String,
        path: Vec<String>,
    },
    Grouping {
        expressions: Vec<Expression>,
    },
}

impl Expression {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column {
            column: ColumnRef::new(name),
        }
    }

    pub fn literal(value: TypedValue) -> Self {
        Self::Literal { value }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseBranch {
    pub when: Expression,
    pub then: Expression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPlacement {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub expression: Expression,
    pub direction: SortDirection,
    pub nulls: Option<NullPlacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowFrameUnit {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "bound", content = "offset", rename_all = "snake_case")]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WindowSpecification {
    pub name: Option<String>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedWindow {
    pub name: String,
    pub specification: WindowSpecification,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum Relation {
    Table {
        name: String,
        alias: Option<String>,
    },
    Cte {
        name: String,
        alias: Option<String>,
    },
    Derived {
        query: Box<Select>,
        alias: String,
    },
    Values {
        rows: Vec<Vec<Expression>>,
        alias: String,
        columns: Vec<String>,
    },
    Join {
        left: Box<Relation>,
        right: Box<Relation>,
        kind: JoinKind,
        on: Option<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "grouping", rename_all = "snake_case")]
pub enum Grouping {
    Expressions { expressions: Vec<Expression> },
    Rollup { expressions: Vec<Expression> },
    Cube { expressions: Vec<Expression> },
    Sets { sets: Vec<Vec<Expression>> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommonTableExpression {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<Select>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetArm {
    pub operator: SetOperator,
    pub query: Box<Select>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Select {
    pub ctes: Vec<CommonTableExpression>,
    pub recursive: bool,
    pub distinct: bool,
    pub distinct_on: Vec<Expression>,
    pub projection: Vec<Projection>,
    pub from: Option<Relation>,
    pub filter: Option<Expression>,
    pub group_by: Option<Grouping>,
    pub having: Option<Expression>,
    pub windows: Vec<NamedWindow>,
    pub set_operations: Vec<SetArm>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub expected_result_shape: Vec<ResultColumnDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expression>>,
    pub source: Option<Box<Select>>,
    pub returning: Vec<Projection>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Upsert {
    pub insert: Insert,
    pub conflict_columns: Vec<String>,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub alias: Option<String>,
    pub assignments: Vec<Assignment>,
    pub from: Option<Relation>,
    pub filter: Option<Expression>,
    pub returning: Vec<Projection>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub alias: Option<String>,
    pub using: Option<Relation>,
    pub filter: Option<Expression>,
    pub all_rows: bool,
    pub returning: Vec<Projection>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Explain {
    pub analyze: bool,
    pub operation: Box<Operation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum TransactionOperation {
    Begin,
    Commit,
    Rollback,
    Savepoint { name: String },
    RollbackToSavepoint { name: String },
    ReleaseSavepoint { name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataTypeDescriptor,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub default: Option<Expression>,
    pub check: Option<Expression>,
    pub reference: Option<ReferenceDefinition>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceDefinition {
    pub table: String,
    pub column: String,
    pub on_delete: crate::ForeignKeyActionDescriptor,
    pub on_update: crate::ForeignKeyActionDescriptor,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "constraint", rename_all = "snake_case")]
pub enum ConstraintDefinitionIr {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: crate::ForeignKeyActionDescriptor,
        on_update: crate::ForeignKeyActionDescriptor,
    },
    Check {
        expression: Expression,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    #[serde(default)]
    pub if_not_exists: bool,
    pub method: Option<String>,
    pub predicate: Option<Expression>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, TypedValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AlterTableAction {
    AddColumn { column: ColumnDefinition },
    ModifyColumn { column: ColumnDefinition },
    DropColumn { column: String },
    RenameColumn { from: String, to: String },
    RenameTable { to: String },
    AddConstraint { constraint: ConstraintDefinitionIr },
    DropConstraint { name: String, if_exists: bool },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum DdlOperation {
    CreateTable {
        table: String,
        if_not_exists: bool,
        columns: Vec<ColumnDefinition>,
        constraints: Vec<ConstraintDefinitionIr>,
    },
    CreateTableAs {
        table: String,
        if_not_exists: bool,
        query: Box<Select>,
    },
    AlterTable {
        table: String,
        action: AlterTableAction,
    },
    DropTable {
        table: String,
        if_exists: bool,
    },
    TruncateTable {
        table: String,
    },
    CreateIndex {
        index: IndexDefinition,
    },
    DropIndex {
        table: String,
        index: String,
        if_exists: bool,
    },
    AlterIndex {
        index: String,
        new_name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_round_trip_is_versioned_kind_checked_and_redacted() {
        let document = IrDocument::new(Operation::Select {
            query: Select {
                projection: vec![Projection {
                    expression: Expression::literal(TypedValue::Text("secret".to_string())),
                    alias: Some("value".to_string()),
                }],
                ..Select::default()
            },
        });
        let json = document.to_json().unwrap();
        assert_eq!(IrDocument::from_json(&json).unwrap(), document);
        let redacted = document.to_redacted_json().unwrap();
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("<redacted>"));

        let mut wrong = document.clone();
        wrong.kind = IrKind::Delete;
        assert!(wrong.to_json().is_err());

        let unknown = json.replace(ORM_IR_VERSION, "radixdb.orm.v999");
        assert!(matches!(
            IrDocument::from_json(&unknown),
            Err(IrError::UnsupportedVersion(_))
        ));
    }
}
