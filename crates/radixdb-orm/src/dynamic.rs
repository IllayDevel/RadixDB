//! Connection-independent dynamic ORM builders.
//!
//! These builders only produce canonical IR. Execution is delegated to an
//! [`OrmSession`], keeping SQL, TCP and embedded transaction ownership outside
//! the builder graph.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::*;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BuilderError {
    #[error("unknown column '{column}' on entity '{table}'")]
    UnknownColumn { table: String, column: String },
    #[error("builder requires at least one projection")]
    EmptyProjection,
    #[error("builder requires at least one value")]
    EmptyValues,
    #[error("ALTER TABLE builder requires exactly one action")]
    AlterActionCardinality,
    #[error("schema identifier '{0}' is not present in the bound descriptor")]
    UnknownIdentifier(String),
    #[error("invalid builder option: {0}")]
    InvalidOption(String),
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderExecutionError<E> {
    #[error(transparent)]
    Build(#[from] BuilderError),
    #[error("session execution failed")]
    Session(E),
}

pub trait OrmBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError>;

    fn to_json(&self) -> Result<String, BuilderJsonError> {
        Ok(self.document()?.to_json()?)
    }

    fn to_sql(&self) -> Result<CompiledStatement, BuilderSqlError> {
        Ok(self.document()?.to_sql()?)
    }
}

async fn execute_builder_async<B, S>(
    builder: &B,
    session: &mut S,
) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>>
where
    B: OrmBuilder,
    S: AsyncOrmSession,
{
    let document = builder.document()?;
    session
        .execute_document_async(&document)
        .await
        .map_err(BuilderExecutionError::Session)
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderJsonError {
    #[error(transparent)]
    Build(#[from] BuilderError),
    #[error(transparent)]
    Ir(#[from] IrError),
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderSqlError {
    #[error(transparent)]
    Build(#[from] BuilderError),
    #[error(transparent)]
    Render(#[from] RenderError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Expr(pub Expression);

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self(Expression::column(name))
    }

    pub fn qualified(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Self(Expression::Column {
            column: ColumnRef::qualified(relation, name),
        })
    }

    pub fn value(value: impl Into<TypedValue>) -> Self {
        Self(Expression::literal(value.into()))
    }

    pub fn star() -> Self {
        Self(Expression::Star { relation: None })
    }

    pub fn navigation(
        root: impl Into<String>,
        path: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self(Expression::Navigation {
            root: root.into(),
            path: path.into_iter().map(Into::into).collect(),
        })
    }

    pub fn function(name: impl Into<String>, arguments: impl IntoIterator<Item = Expr>) -> Self {
        Self(Expression::Function {
            name: name.into(),
            arguments: arguments.into_iter().map(|value| value.0).collect(),
        })
    }

    pub fn aggregate(
        name: impl Into<String>,
        arguments: impl IntoIterator<Item = Expr>,
        distinct: bool,
        filter: Option<Expr>,
        order_by: Vec<Order>,
    ) -> Self {
        Self(Expression::Aggregate {
            name: name.into(),
            arguments: arguments.into_iter().map(|value| value.0).collect(),
            distinct,
            filter: filter.map(|value| Box::new(value.0)),
            order_by: order_by.into_iter().map(Order::into_ir).collect(),
        })
    }

    pub fn window(self, specification: WindowSpecification) -> Self {
        Self(Expression::Window {
            function: Box::new(self.0),
            specification,
        })
    }

    pub fn cast(self, data_type: DataTypeDescriptor) -> Self {
        Self(Expression::Cast {
            expression: Box::new(self.0),
            data_type,
        })
    }

    pub fn alias(self, alias: impl Into<String>) -> Projection {
        Projection {
            expression: self.0,
            alias: Some(alias.into()),
        }
    }

    pub fn projection(self) -> Projection {
        Projection {
            expression: self.0,
            alias: None,
        }
    }

    pub fn unary(self, operator: UnaryOperator) -> Self {
        Self(Expression::Unary {
            operator,
            expression: Box::new(self.0),
        })
    }

    pub fn binary(self, operator: BinaryOperator, right: impl Into<Expr>) -> Self {
        Self(Expression::Binary {
            left: Box::new(self.0),
            operator,
            right: Box::new(right.into().0),
        })
    }

    pub fn eq(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Eq, right)
    }
    pub fn ne(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Ne, right)
    }
    pub fn lt(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Lt, right)
    }
    pub fn lte(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Lte, right)
    }
    pub fn gt(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Gt, right)
    }
    pub fn gte(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Gte, right)
    }
    pub fn and(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::And, right)
    }
    pub fn or(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Or, right)
    }
    // These names are the deliberate fluent ORM vocabulary. Implementing the
    // operator traits would make generic expression operands less ergonomic
    // and would not replace the method-call API serialized in examples.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Add, right)
    }
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Subtract, right)
    }
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Multiply, right)
    }
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Divide, right)
    }
    pub fn modulo(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Modulo, right)
    }
    pub fn like(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Like, right)
    }
    pub fn not_like(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::NotLike, right)
    }
    pub fn regexp(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Regexp, right)
    }
    pub fn glob(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::Glob, right)
    }
    pub fn is_distinct_from(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::IsDistinctFrom, right)
    }
    pub fn is_not_distinct_from(self, right: impl Into<Expr>) -> Self {
        self.binary(BinaryOperator::IsNotDistinctFrom, right)
    }
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        self.unary(UnaryOperator::Not)
    }

    pub fn is_null(self) -> Self {
        Self(Expression::IsNull {
            expression: Box::new(self.0),
            negated: false,
        })
    }

    pub fn is_not_null(self) -> Self {
        Self(Expression::IsNull {
            expression: Box::new(self.0),
            negated: true,
        })
    }

    pub fn between(self, lower: impl Into<Expr>, upper: impl Into<Expr>) -> Self {
        Self(Expression::Between {
            expression: Box::new(self.0),
            lower: Box::new(lower.into().0),
            upper: Box::new(upper.into().0),
            negated: false,
        })
    }

    pub fn not_between(self, lower: impl Into<Expr>, upper: impl Into<Expr>) -> Self {
        Self(Expression::Between {
            expression: Box::new(self.0),
            lower: Box::new(lower.into().0),
            upper: Box::new(upper.into().0),
            negated: true,
        })
    }

    pub fn in_list(self, values: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        Self(Expression::InList {
            expression: Box::new(self.0),
            values: values.into_iter().map(|value| value.into().0).collect(),
            negated: false,
        })
    }

    pub fn not_in_list(self, values: impl IntoIterator<Item = impl Into<Expr>>) -> Self {
        Self(Expression::InList {
            expression: Box::new(self.0),
            values: values.into_iter().map(|value| value.into().0).collect(),
            negated: true,
        })
    }

    pub fn in_subquery(self, query: QueryBuilder) -> Self {
        Self(Expression::InSubquery {
            expression: Box::new(self.0),
            query: Box::new(query.into_select()),
            negated: false,
        })
    }

    pub fn not_in_subquery(self, query: QueryBuilder) -> Self {
        Self(Expression::InSubquery {
            expression: Box::new(self.0),
            query: Box::new(query.into_select()),
            negated: true,
        })
    }

    pub fn tuple(values: impl IntoIterator<Item = Expr>) -> Self {
        Self(Expression::Tuple {
            values: values.into_iter().map(|value| value.0).collect(),
        })
    }

    pub fn exists(query: QueryBuilder) -> Self {
        Self(Expression::Exists {
            query: Box::new(query.into_select()),
            negated: false,
        })
    }

    pub fn not_exists(query: QueryBuilder) -> Self {
        Self(Expression::Exists {
            query: Box::new(query.into_select()),
            negated: true,
        })
    }

    pub fn scalar_subquery(query: QueryBuilder) -> Self {
        Self(Expression::ScalarSubquery {
            query: Box::new(query.into_select()),
        })
    }

    pub fn case(
        operand: Option<Expr>,
        branches: impl IntoIterator<Item = (Expr, Expr)>,
        otherwise: Option<Expr>,
    ) -> Self {
        Self(Expression::Case {
            operand: operand.map(|value| Box::new(value.0)),
            branches: branches
                .into_iter()
                .map(|(when, then)| CaseBranch {
                    when: when.0,
                    then: then.0,
                })
                .collect(),
            otherwise: otherwise.map(|value| Box::new(value.0)),
        })
    }

    pub fn grouping(expressions: impl IntoIterator<Item = Expr>) -> Self {
        Self(Expression::Grouping {
            expressions: expressions.into_iter().map(|value| value.0).collect(),
        })
    }

    pub fn asc(self) -> Order {
        Order::new(self, SortDirection::Asc)
    }
    pub fn desc(self) -> Order {
        Order::new(self, SortDirection::Desc)
    }
}

macro_rules! typed_value_from {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for TypedValue {
            fn from(value: $ty) -> Self {
                Self::$variant(value.into())
            }
        }
        impl From<$ty> for Expr {
            fn from(value: $ty) -> Self {
                Self::value(value)
            }
        }
    };
}

typed_value_from!(i64, Integer);
typed_value_from!(bool, Boolean);
typed_value_from!(String, Text);
typed_value_from!(&str, Text);

impl From<f64> for TypedValue {
    fn from(value: f64) -> Self {
        Self::Float(value.into())
    }
}
impl From<f64> for Expr {
    fn from(value: f64) -> Self {
        Self::value(value)
    }
}
impl From<TypedValue> for Expr {
    fn from(value: TypedValue) -> Self {
        Self::value(value)
    }
}
impl From<Expression> for Expr {
    fn from(value: Expression) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order(pub OrderBy);

impl Order {
    pub fn new(expression: Expr, direction: SortDirection) -> Self {
        Self(OrderBy {
            expression: expression.0,
            direction,
            nulls: None,
        })
    }
    pub fn nulls_first(mut self) -> Self {
        self.0.nulls = Some(NullPlacement::First);
        self
    }
    pub fn nulls_last(mut self) -> Self {
        self.0.nulls = Some(NullPlacement::Last);
        self
    }
    fn into_ir(self) -> OrderBy {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicColumn {
    table: String,
    descriptor: ColumnDescriptor,
}

impl DynamicColumn {
    pub fn name(&self) -> &str {
        &self.descriptor.name
    }
    pub fn descriptor(&self) -> &ColumnDescriptor {
        &self.descriptor
    }
    pub fn expr(&self) -> Expr {
        Expr::qualified(self.table.clone(), self.descriptor.name.clone())
    }
    pub fn eq(&self, value: impl Into<Expr>) -> Expr {
        self.expr().eq(value)
    }
    pub fn asc(&self) -> Order {
        self.expr().asc()
    }
    pub fn desc(&self) -> Order {
        self.expr().desc()
    }
}

impl From<DynamicColumn> for Expr {
    fn from(value: DynamicColumn) -> Self {
        value.expr()
    }
}
impl From<&DynamicColumn> for Expr {
    fn from(value: &DynamicColumn) -> Self {
        value.expr()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicEntity {
    descriptor: TableDescriptor,
    alias: Option<String>,
}

impl DynamicEntity {
    pub fn new(descriptor: TableDescriptor) -> Self {
        Self {
            descriptor,
            alias: None,
        }
    }
    pub fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
    pub fn column(&self, name: &str) -> Result<DynamicColumn, BuilderError> {
        let descriptor = self
            .descriptor
            .columns
            .iter()
            .find(|column| column.name == name)
            .cloned()
            .ok_or_else(|| BuilderError::UnknownColumn {
                table: self.descriptor.name.clone(),
                column: name.to_string(),
            })?;
        Ok(DynamicColumn {
            table: self
                .alias
                .clone()
                .unwrap_or_else(|| self.descriptor.name.clone()),
            descriptor,
        })
    }
    /// Build a key-only reference after proving that `column` is a
    /// one-column PRIMARY KEY or UNIQUE NOT NULL key in this descriptor.
    pub fn reference(
        &self,
        column: &str,
        key: TypedValue,
    ) -> Result<DynamicReference, RecordError> {
        validate_reference_target(&self.descriptor, column)?;
        let descriptor = self
            .descriptor
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
            .ok_or_else(|| RecordError::UnknownField(column.to_string()))?;
        if matches!(key, TypedValue::Null(_)) {
            return Err(RecordError::NullReferenceKey);
        }
        if !typed_value_matches(&key, &descriptor.data_type) {
            return Err(RecordError::ValueTypeMismatch {
                field: format!("{}.{}", self.descriptor.name, column),
                expected: descriptor.data_type.clone(),
                actual: key.data_type(),
            });
        }
        Ok(DynamicReference::new(&self.descriptor.name, column, key))
    }
    pub fn relation(&self) -> Relation {
        Relation::Table {
            name: self.descriptor.name.clone(),
            alias: self.alias.clone(),
        }
    }
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::from_relation(self.relation())
    }
    pub fn insert(&self) -> InsertBuilder {
        InsertBuilder::new(self.descriptor.name.clone())
    }
    pub fn update(&self) -> UpdateBuilder {
        UpdateBuilder::new(self.descriptor.name.clone())
    }
    pub fn delete(&self) -> DeleteBuilder {
        DeleteBuilder::new(self.descriptor.name.clone())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryBuilder {
    select: Select,
}

impl QueryBuilder {
    pub fn from_relation(relation: Relation) -> Self {
        Self {
            select: Select {
                from: Some(relation),
                ..Select::default()
            },
        }
    }
    pub fn select(mut self, expressions: impl IntoIterator<Item = Expr>) -> Self {
        self.select.projection = expressions.into_iter().map(Expr::projection).collect();
        self
    }
    pub fn select_projections(mut self, projections: Vec<Projection>) -> Self {
        self.select.projection = projections;
        self
    }
    pub fn filter(mut self, filter: impl Into<Expr>) -> Self {
        self.select.filter = Some(filter.into().0);
        self
    }
    pub fn distinct(mut self) -> Self {
        self.select.distinct = true;
        self
    }
    pub fn distinct_on(mut self, expressions: impl IntoIterator<Item = Expr>) -> Self {
        self.select.distinct_on = expressions.into_iter().map(|v| v.0).collect();
        self
    }
    pub fn join(mut self, kind: JoinKind, right: Relation, on: Option<Expr>) -> Self {
        let left = self
            .select
            .from
            .take()
            .expect("query relation is always present");
        self.select.from = Some(Relation::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            on: on.map(|v| v.0),
        });
        self
    }
    pub fn inner_join(self, right: Relation, on: Expr) -> Self {
        self.join(JoinKind::Inner, right, Some(on))
    }
    pub fn left_join(self, right: Relation, on: Expr) -> Self {
        self.join(JoinKind::Left, right, Some(on))
    }
    pub fn right_join(self, right: Relation, on: Expr) -> Self {
        self.join(JoinKind::Right, right, Some(on))
    }
    pub fn full_join(self, right: Relation, on: Expr) -> Self {
        self.join(JoinKind::Full, right, Some(on))
    }
    pub fn cross_join(self, right: Relation) -> Self {
        self.join(JoinKind::Cross, right, None)
    }
    pub fn group_by(mut self, grouping: Grouping) -> Self {
        self.select.group_by = Some(grouping);
        self
    }
    pub fn having(mut self, expression: impl Into<Expr>) -> Self {
        self.select.having = Some(expression.into().0);
        self
    }
    pub fn window(mut self, window: NamedWindow) -> Self {
        self.select.windows.push(window);
        self
    }
    pub fn order_by(mut self, values: impl IntoIterator<Item = Order>) -> Self {
        self.select.order_by = values.into_iter().map(Order::into_ir).collect();
        self
    }
    pub fn limit(mut self, value: u64) -> Self {
        self.select.limit = Some(value);
        self
    }
    pub fn offset(mut self, value: u64) -> Self {
        self.select.offset = Some(value);
        self
    }
    pub fn with_cte(mut self, cte: CommonTableExpression) -> Self {
        self.select.ctes.push(cte);
        self
    }
    pub fn recursive(mut self, value: bool) -> Self {
        self.select.recursive = value;
        self
    }
    pub fn set_operation(mut self, operator: SetOperator, query: QueryBuilder) -> Self {
        self.select.set_operations.push(SetArm {
            operator,
            query: Box::new(query.select),
        });
        self
    }
    pub fn expected_shape(mut self, shape: Vec<ResultColumnDescriptor>) -> Self {
        self.select.expected_result_shape = shape;
        self
    }
    pub fn into_select(self) -> Select {
        self.select
    }
    pub fn derived(self, alias: impl Into<String>) -> Relation {
        Relation::Derived {
            query: Box::new(self.select),
            alias: alias.into(),
        }
    }
    pub fn explain(&self, analyze: bool) -> Result<IrDocument, BuilderError> {
        let operation = self.document()?.payload;
        Ok(IrDocument::new(Operation::Explain {
            statement: Explain {
                analyze,
                operation: Box::new(operation),
            },
        }))
    }
    pub fn fetch<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::QueryOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .query_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn fetch_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::QueryOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .query_document_async(&document)
            .await
            .map_err(BuilderExecutionError::Session)
    }
}

impl OrmBuilder for QueryBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        if self.select.projection.is_empty() {
            return Err(BuilderError::EmptyProjection);
        }
        Ok(IrDocument::new(Operation::Select {
            query: self.select.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertBuilder {
    insert: Insert,
    upsert: Option<(Vec<String>, Vec<Assignment>)>,
}

impl InsertBuilder {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            insert: Insert {
                table: table.into(),
                columns: Vec::new(),
                rows: vec![Vec::new()],
                source: None,
                returning: Vec::new(),
            },
            upsert: None,
        }
    }
    pub fn value(mut self, column: impl Into<String>, value: impl Into<Expr>) -> Self {
        self.insert.columns.push(column.into());
        self.insert.rows[0].push(value.into().0);
        self
    }
    pub fn rows(mut self, columns: Vec<String>, rows: Vec<Vec<Expr>>) -> Self {
        self.insert.columns = columns;
        self.insert.rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| v.0).collect())
            .collect();
        self
    }
    pub fn from_select(mut self, columns: Vec<String>, query: QueryBuilder) -> Self {
        self.insert.columns = columns;
        self.insert.rows.clear();
        self.insert.source = Some(Box::new(query.select));
        self
    }
    pub fn returning(mut self, values: impl IntoIterator<Item = Expr>) -> Self {
        self.insert.returning = values.into_iter().map(Expr::projection).collect();
        self
    }
    pub fn returning_all(mut self) -> Self {
        self.insert.returning = vec![Expr::star().projection()];
        self
    }
    pub fn on_conflict(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.upsert = Some((columns.into_iter().map(Into::into).collect(), Vec::new()));
        self
    }
    pub fn upsert_on(self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.on_conflict(columns)
    }
    pub fn do_update(mut self, column: impl Into<String>, value: impl Into<Expr>) -> Self {
        self.upsert
            .get_or_insert_with(|| (Vec::new(), Vec::new()))
            .1
            .push(Assignment {
                column: column.into(),
                value: value.into().0,
            });
        self
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for InsertBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        if self.insert.columns.is_empty() {
            return Err(BuilderError::EmptyValues);
        }
        let payload = match &self.upsert {
            Some((columns, assignments)) => Operation::Upsert {
                statement: Upsert {
                    insert: self.insert.clone(),
                    conflict_columns: columns.clone(),
                    assignments: assignments.clone(),
                },
            },
            None => Operation::Insert {
                statement: self.insert.clone(),
            },
        };
        Ok(IrDocument::new(payload))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateBuilder {
    update: Update,
}

impl UpdateBuilder {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            update: Update {
                table: table.into(),
                alias: None,
                assignments: Vec::new(),
                from: None,
                filter: None,
                returning: Vec::new(),
            },
        }
    }
    pub fn set(mut self, column: impl Into<String>, value: impl Into<Expr>) -> Self {
        self.update.assignments.push(Assignment {
            column: column.into(),
            value: value.into().0,
        });
        self
    }
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.update.alias = Some(alias.into());
        self
    }
    pub fn filter(mut self, value: impl Into<Expr>) -> Self {
        self.update.filter = Some(value.into().0);
        self
    }
    pub fn from(mut self, relation: Relation) -> Self {
        self.update.from = Some(relation);
        self
    }
    pub fn returning(mut self, values: impl IntoIterator<Item = Expr>) -> Self {
        self.update.returning = values.into_iter().map(Expr::projection).collect();
        self
    }
    pub fn returning_all(mut self) -> Self {
        self.update.returning = vec![Expr::star().projection()];
        self
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for UpdateBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        Ok(IrDocument::new(Operation::Update {
            statement: self.update.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteBuilder {
    delete: Delete,
}

impl DeleteBuilder {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            delete: Delete {
                table: table.into(),
                alias: None,
                using: None,
                filter: None,
                all_rows: false,
                returning: Vec::new(),
            },
        }
    }
    pub fn filter(mut self, value: impl Into<Expr>) -> Self {
        self.delete.filter = Some(value.into().0);
        self
    }
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.delete.alias = Some(alias.into());
        self
    }
    pub fn all_rows(mut self) -> Self {
        self.delete.all_rows = true;
        self
    }
    pub fn using(mut self, relation: Relation) -> Self {
        self.delete.using = Some(relation);
        self
    }
    pub fn returning(mut self, values: impl IntoIterator<Item = Expr>) -> Self {
        self.delete.returning = values.into_iter().map(Expr::projection).collect();
        self
    }
    pub fn returning_all(mut self) -> Self {
        self.delete.returning = vec![Expr::star().projection()];
        self
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for DeleteBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        Ok(IrDocument::new(Operation::Delete {
            statement: self.delete.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    definition: ColumnDefinition,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataTypeDescriptor) -> Self {
        Self {
            definition: ColumnDefinition {
                name: name.into(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                check: None,
                reference: None,
                extensions: BTreeMap::new(),
            },
        }
    }
    pub fn integer(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Integer)
    }
    pub fn float(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Float)
    }
    pub fn text(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Text)
    }
    pub fn boolean(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Boolean)
    }
    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Timestamp)
    }
    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Date)
    }
    pub fn json(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Json)
    }
    pub fn uuid(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Uuid)
    }
    pub fn bytes(name: impl Into<String>) -> Self {
        Self::new(name, DataTypeDescriptor::Bytes)
    }
    pub fn decimal(name: impl Into<String>, precision: u8, scale: u8) -> Self {
        Self::new(
            name,
            DataTypeDescriptor::Decimal {
                precision: Some(precision),
                scale: Some(scale),
            },
        )
    }
    pub fn vector(name: impl Into<String>, dimensions: u16) -> Self {
        Self::new(name, DataTypeDescriptor::Vector { dimensions })
    }
    pub fn not_null(mut self, value: bool) -> Self {
        self.definition.nullable = !value;
        self
    }
    pub fn primary_key(mut self, value: bool) -> Self {
        self.definition.primary_key = value;
        if value {
            self.definition.nullable = false;
        }
        self
    }
    pub fn unique(mut self, value: bool) -> Self {
        self.definition.unique = value;
        self
    }
    pub fn auto_increment(mut self, value: bool) -> Self {
        self.definition.auto_increment = value;
        self
    }
    pub fn default(mut self, value: impl Into<Expr>) -> Self {
        self.definition.default = Some(value.into().0);
        self
    }
    pub fn check(mut self, value: impl Into<Expr>) -> Self {
        self.definition.check = Some(value.into().0);
        self
    }
    pub fn reference(mut self, reference: ReferenceDefinition) -> Self {
        self.definition.reference = Some(reference);
        self
    }
    pub fn extension(mut self, name: impl Into<String>, value: serde_json::Value) -> Self {
        self.definition.extensions.insert(name.into(), value);
        self
    }
    pub fn into_ir(self) -> ColumnDefinition {
        self.definition
    }
}

pub fn reference(table: impl Into<String>, column: impl Into<String>) -> ReferenceDefinition {
    ReferenceDefinition {
        table: table.into(),
        column: column.into(),
        on_delete: ForeignKeyActionDescriptor::Restrict,
        on_update: ForeignKeyActionDescriptor::Restrict,
    }
}

impl ReferenceDefinition {
    pub fn on_delete(mut self, action: ForeignKeyActionDescriptor) -> Self {
        self.on_delete = action;
        self
    }

    pub fn on_update(mut self, action: ForeignKeyActionDescriptor) -> Self {
        self.on_update = action;
        self
    }
}

pub fn table(name: impl Into<String>) -> Relation {
    Relation::Table {
        name: name.into(),
        alias: None,
    }
}

pub fn table_as(name: impl Into<String>, alias: impl Into<String>) -> Relation {
    Relation::Table {
        name: name.into(),
        alias: Some(alias.into()),
    }
}

pub fn cte(name: impl Into<String>) -> Relation {
    Relation::Cte {
        name: name.into(),
        alias: None,
    }
}

pub fn values_relation(
    rows: Vec<Vec<Expr>>,
    alias: impl Into<String>,
    columns: impl IntoIterator<Item = impl Into<String>>,
) -> Relation {
    Relation::Values {
        rows: rows
            .into_iter()
            .map(|row| row.into_iter().map(|value| value.0).collect())
            .collect(),
        alias: alias.into(),
        columns: columns.into_iter().map(Into::into).collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    BTree,
    Hash,
    Bitmap,
    Hnsw,
}

impl IndexMethod {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::BTree => "BTREE",
            Self::Hash => "HASH",
            Self::Bitmap => "BITMAP",
            Self::Hnsw => "HNSW",
        }
    }
}

impl IndexDefinition {
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            unique: false,
            if_not_exists: false,
            method: None,
            predicate: None,
            options: BTreeMap::new(),
        }
    }

    pub fn unique(mut self, value: bool) -> Self {
        self.unique = value;
        self
    }

    pub fn if_not_exists(mut self, value: bool) -> Self {
        self.if_not_exists = value;
        self
    }

    pub fn method(mut self, method: IndexMethod) -> Self {
        self.method = Some(method.as_sql().to_string());
        self
    }

    pub fn where_(mut self, predicate: impl Into<Expr>) -> Self {
        self.predicate = Some(predicate.into().0);
        self
    }

    pub fn option(mut self, name: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DdlBuilder {
    operation: DdlOperation,
    error: Option<BuilderError>,
}

impl DdlBuilder {
    pub fn create_table(table: impl Into<String>) -> CreateTableBuilder {
        CreateTableBuilder {
            table: table.into(),
            if_not_exists: false,
            columns: Vec::new(),
            constraints: Vec::new(),
        }
    }
    pub fn alter_table(table: impl Into<String>) -> AlterTableBuilder {
        AlterTableBuilder {
            table: table.into(),
            action: None,
        }
    }
    pub fn create_table_as(table: impl Into<String>, query: QueryBuilder) -> CreateTableAsBuilder {
        CreateTableAsBuilder {
            table: table.into(),
            if_not_exists: false,
            query,
        }
    }
    pub fn drop_table(table: impl Into<String>) -> Self {
        Self {
            operation: DdlOperation::DropTable {
                table: table.into(),
                if_exists: false,
            },
            error: None,
        }
    }
    pub fn truncate_table(table: impl Into<String>) -> Self {
        Self {
            operation: DdlOperation::TruncateTable {
                table: table.into(),
            },
            error: None,
        }
    }
    pub fn create_index(index: IndexDefinition) -> Self {
        Self {
            operation: DdlOperation::CreateIndex { index },
            error: None,
        }
    }
    pub fn drop_index(table: impl Into<String>, index: impl Into<String>, if_exists: bool) -> Self {
        Self {
            operation: DdlOperation::DropIndex {
                table: table.into(),
                index: index.into(),
                if_exists,
            },
            error: None,
        }
    }
    pub fn alter_index(index: impl Into<String>, new_name: impl Into<String>) -> Self {
        Self {
            operation: DdlOperation::AlterIndex {
                index: index.into(),
                new_name: new_name.into(),
            },
            error: None,
        }
    }
    pub fn if_exists(mut self, value: bool) -> Self {
        match &mut self.operation {
            DdlOperation::DropTable { if_exists, .. }
            | DdlOperation::DropIndex { if_exists, .. } => {
                *if_exists = value;
            }
            _ => {
                self.error = Some(BuilderError::InvalidOption(
                    "if_exists is valid only for DROP TABLE or DROP INDEX".to_string(),
                ));
            }
        }
        self
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableAsBuilder {
    table: String,
    if_not_exists: bool,
    query: QueryBuilder,
}

impl CreateTableAsBuilder {
    pub fn if_not_exists(mut self, value: bool) -> Self {
        self.if_not_exists = value;
        self
    }

    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for CreateTableAsBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        let query = self.query.document()?;
        let Operation::Select { query } = query.payload else {
            unreachable!("QueryBuilder always produces SELECT")
        };
        Ok(IrDocument::new(Operation::Ddl {
            operation: DdlOperation::CreateTableAs {
                table: self.table.clone(),
                if_not_exists: self.if_not_exists,
                query: Box::new(query),
            },
        }))
    }
}

impl OrmBuilder for DdlBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(IrDocument::new(Operation::Ddl {
            operation: self.operation.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableBuilder {
    table: String,
    if_not_exists: bool,
    columns: Vec<ColumnDefinition>,
    constraints: Vec<ConstraintDefinitionIr>,
}

impl CreateTableBuilder {
    pub fn if_not_exists(mut self, value: bool) -> Self {
        self.if_not_exists = value;
        self
    }
    pub fn column(mut self, column: Column) -> Self {
        self.columns.push(column.into_ir());
        self
    }
    pub fn constraint(mut self, constraint: ConstraintDefinitionIr) -> Self {
        self.constraints.push(constraint);
        self
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for CreateTableBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        Ok(IrDocument::new(Operation::Ddl {
            operation: DdlOperation::CreateTable {
                table: self.table.clone(),
                if_not_exists: self.if_not_exists,
                columns: self.columns.clone(),
                constraints: self.constraints.clone(),
            },
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableBuilder {
    table: String,
    action: Option<AlterTableAction>,
}

impl AlterTableBuilder {
    fn action(mut self, action: AlterTableAction) -> Self {
        self.action = Some(action);
        self
    }
    pub fn add_column(self, column: Column) -> Self {
        self.action(AlterTableAction::AddColumn {
            column: column.into_ir(),
        })
    }
    pub fn modify_column(self, column: Column) -> Self {
        self.action(AlterTableAction::ModifyColumn {
            column: column.into_ir(),
        })
    }
    pub fn drop_column(self, column: impl Into<String>) -> Self {
        self.action(AlterTableAction::DropColumn {
            column: column.into(),
        })
    }
    pub fn rename_column(self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.action(AlterTableAction::RenameColumn {
            from: from.into(),
            to: to.into(),
        })
    }
    pub fn rename_table(self, to: impl Into<String>) -> Self {
        self.action(AlterTableAction::RenameTable { to: to.into() })
    }
    pub fn add_constraint(self, constraint: ConstraintDefinitionIr) -> Self {
        self.action(AlterTableAction::AddConstraint { constraint })
    }
    pub fn drop_constraint(self, name: impl Into<String>, if_exists: bool) -> Self {
        self.action(AlterTableAction::DropConstraint {
            name: name.into(),
            if_exists,
        })
    }
    pub fn execute<S: OrmSession>(
        self,
        session: S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        let document = self.document()?;
        session
            .execute_document(&document)
            .map_err(BuilderExecutionError::Session)
    }

    pub async fn execute_async<S: AsyncOrmSession>(
        self,
        session: &mut S,
    ) -> Result<S::CommandOutput, BuilderExecutionError<S::Error>> {
        execute_builder_async(&self, session).await
    }
}

impl OrmBuilder for AlterTableBuilder {
    fn document(&self) -> Result<IrDocument, BuilderError> {
        let action = self
            .action
            .clone()
            .ok_or(BuilderError::AlterActionCardinality)?;
        Ok(IrDocument::new(Operation::Ddl {
            operation: DdlOperation::AlterTable {
                table: self.table.clone(),
                action,
            },
        }))
    }
}

pub fn primary_key(columns: impl IntoIterator<Item = impl Into<String>>) -> ConstraintDefinitionIr {
    ConstraintDefinitionIr::PrimaryKey {
        columns: columns.into_iter().map(Into::into).collect(),
    }
}
pub fn unique(columns: impl IntoIterator<Item = impl Into<String>>) -> ConstraintDefinitionIr {
    ConstraintDefinitionIr::Unique {
        columns: columns.into_iter().map(Into::into).collect(),
    }
}
pub fn check(expression: impl Into<Expr>) -> ConstraintDefinitionIr {
    ConstraintDefinitionIr::Check {
        expression: expression.into().0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_and_dynamic_facades_produce_identical_ir() {
        let descriptor = TableDescriptor {
            catalog_id: "catalog".to_string(),
            name: "people".to_string(),
            schema_generation: 1,
            fingerprint: "fingerprint".to_string(),
            created_at: "2026-08-21T00:00:00Z".to_string(),
            updated_at: "2026-08-21T00:00:00Z".to_string(),
            columns: vec![
                ColumnDescriptor {
                    ordinal: 0,
                    name: "id".to_string(),
                    data_type: DataTypeDescriptor::Integer,
                    nullable: false,
                    auto_increment: false,
                    default_expression: None,
                    extensions: BTreeMap::new(),
                },
                ColumnDescriptor {
                    ordinal: 1,
                    name: "name".to_string(),
                    data_type: DataTypeDescriptor::Text,
                    nullable: false,
                    auto_increment: false,
                    default_expression: None,
                    extensions: BTreeMap::new(),
                },
            ],
            constraints: Vec::new(),
            indexes: Vec::new(),
            extensions: BTreeMap::new(),
        };
        let dynamic = DynamicEntity::new(descriptor);
        let dynamic_query = dynamic
            .query()
            .select([
                dynamic.column("id").unwrap().expr(),
                dynamic.column("name").unwrap().expr(),
            ])
            .filter(dynamic.column("id").unwrap().eq(7_i64));

        let id = TypedColumn::<i64>::new("people", "id", DataTypeDescriptor::Integer, false);
        let name = TypedColumn::<String>::new("people", "name", DataTypeDescriptor::Text, false);
        let generated_query = QueryBuilder::from_relation(table("people"))
            .select([id.clone().expr(), name.expr()])
            .filter(id.eq(7_i64));
        assert_eq!(
            dynamic_query.document().unwrap(),
            generated_query.document().unwrap()
        );
    }

    #[test]
    fn dynamic_builders_share_canonical_ir_and_reject_unsafe_delete() {
        let create = DdlBuilder::create_table("people")
            .column(Column::integer("id").primary_key(true))
            .column(Column::text("name").not_null(true));
        assert_eq!(
            create.to_sql().unwrap().sql,
            "CREATE TABLE \"people\" (\"id\" INTEGER PRIMARY KEY, \"name\" TEXT NOT NULL)"
        );
        assert_eq!(
            IrDocument::from_json(&create.to_json().unwrap()).unwrap(),
            create.document().unwrap()
        );

        let source = QueryBuilder::from_relation(table("people")).select([Expr::column("id")]);
        assert_eq!(
            DdlBuilder::create_table_as("people_copy", source)
                .if_not_exists(true)
                .to_sql()
                .unwrap()
                .sql,
            "CREATE TABLE IF NOT EXISTS \"people_copy\" AS SELECT \"id\" FROM \"people\""
        );
        assert_eq!(
            DdlBuilder::alter_index("idx_people", "idx_people_new")
                .to_sql()
                .unwrap()
                .sql,
            "ALTER INDEX \"idx_people\" RENAME TO \"idx_people_new\""
        );
        assert_eq!(
            DdlBuilder::drop_table("people")
                .if_exists(true)
                .to_sql()
                .unwrap()
                .sql,
            "DROP TABLE IF EXISTS \"people\""
        );
        assert!(matches!(
            DdlBuilder::truncate_table("people")
                .if_exists(true)
                .document(),
            Err(BuilderError::InvalidOption(_))
        ));
        assert_eq!(
            DdlBuilder::create_index(
                IndexDefinition::new("idx_people_name", "people", ["name"])
                    .if_not_exists(true)
                    .method(IndexMethod::Hash),
            )
            .to_sql()
            .unwrap()
            .sql,
            "CREATE INDEX IF NOT EXISTS \"idx_people_name\" ON \"people\" (\"name\") USING \"HASH\""
        );

        let descriptor = TableDescriptor {
            catalog_id: "00000000-0000-0000-0000-000000000001".to_string(),
            name: "people".to_string(),
            schema_generation: 1,
            fingerprint: "f".to_string(),
            created_at: "x".to_string(),
            updated_at: "x".to_string(),
            columns: vec![ColumnDescriptor {
                ordinal: 0,
                name: "id".to_string(),
                data_type: DataTypeDescriptor::Integer,
                nullable: false,
                auto_increment: false,
                default_expression: None,
                extensions: BTreeMap::new(),
            }],
            constraints: Vec::new(),
            indexes: Vec::new(),
            extensions: BTreeMap::new(),
        };
        let entity = DynamicEntity::new(descriptor);
        assert!(matches!(
            entity.column("missing"),
            Err(BuilderError::UnknownColumn { .. })
        ));
        let query = entity
            .query()
            .select([entity.column("id").unwrap().expr()])
            .filter(entity.column("id").unwrap().eq(7_i64));
        let compiled = query.to_sql().unwrap();
        assert_eq!(compiled.parameters, vec![TypedValue::Integer(7)]);
        assert!(DeleteBuilder::new("people").to_sql().is_err());
    }
}
