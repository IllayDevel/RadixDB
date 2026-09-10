//! Record state, reference keys, and mutation safety.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::*;

macro_rules! typed_expression_methods {
    () => {
        pub fn eq(self, value: impl Into<Expr>) -> Expr {
            self.expr().eq(value)
        }
        pub fn ne(self, value: impl Into<Expr>) -> Expr {
            self.expr().ne(value)
        }
        pub fn lt(self, value: impl Into<Expr>) -> Expr {
            self.expr().lt(value)
        }
        pub fn lte(self, value: impl Into<Expr>) -> Expr {
            self.expr().lte(value)
        }
        pub fn gt(self, value: impl Into<Expr>) -> Expr {
            self.expr().gt(value)
        }
        pub fn gte(self, value: impl Into<Expr>) -> Expr {
            self.expr().gte(value)
        }
        pub fn is_null(self) -> Expr {
            self.expr().is_null()
        }
        pub fn is_not_null(self) -> Expr {
            self.expr().is_not_null()
        }
        pub fn between(self, lower: impl Into<Expr>, upper: impl Into<Expr>) -> Expr {
            self.expr().between(lower, upper)
        }
        pub fn not_between(self, lower: impl Into<Expr>, upper: impl Into<Expr>) -> Expr {
            self.expr().not_between(lower, upper)
        }
        pub fn in_(self, values: impl IntoIterator<Item = impl Into<Expr>>) -> Expr {
            self.expr().in_list(values)
        }
        pub fn not_in(self, values: impl IntoIterator<Item = impl Into<Expr>>) -> Expr {
            self.expr().not_in_list(values)
        }
        pub fn in_subquery(self, query: QueryBuilder) -> Expr {
            self.expr().in_subquery(query)
        }
        pub fn not_in_subquery(self, query: QueryBuilder) -> Expr {
            self.expr().not_in_subquery(query)
        }
        pub fn like(self, value: impl Into<Expr>) -> Expr {
            self.expr().like(value)
        }
        pub fn not_like(self, value: impl Into<Expr>) -> Expr {
            self.expr().not_like(value)
        }
        pub fn regexp(self, value: impl Into<Expr>) -> Expr {
            self.expr().regexp(value)
        }
        pub fn glob(self, value: impl Into<Expr>) -> Expr {
            self.expr().glob(value)
        }
        pub fn is_distinct_from(self, value: impl Into<Expr>) -> Expr {
            self.expr().is_distinct_from(value)
        }
        pub fn is_not_distinct_from(self, value: impl Into<Expr>) -> Expr {
            self.expr().is_not_distinct_from(value)
        }
        #[allow(clippy::should_implement_trait)]
        pub fn add(self, value: impl Into<Expr>) -> Expr {
            self.expr().add(value)
        }
        #[allow(clippy::should_implement_trait)]
        pub fn sub(self, value: impl Into<Expr>) -> Expr {
            self.expr().sub(value)
        }
        #[allow(clippy::should_implement_trait)]
        pub fn mul(self, value: impl Into<Expr>) -> Expr {
            self.expr().mul(value)
        }
        #[allow(clippy::should_implement_trait)]
        pub fn div(self, value: impl Into<Expr>) -> Expr {
            self.expr().div(value)
        }
        pub fn modulo(self, value: impl Into<Expr>) -> Expr {
            self.expr().modulo(value)
        }
        pub fn cast(self, data_type: DataTypeDescriptor) -> Expr {
            self.expr().cast(data_type)
        }
        pub fn alias(self, alias: impl Into<String>) -> Projection {
            self.expr().alias(alias)
        }
        pub fn projection(self) -> Projection {
            self.expr().projection()
        }
        pub fn asc(self) -> Order {
            self.expr().asc()
        }
        pub fn desc(self) -> Order {
            self.expr().desc()
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedColumn<T, Owner = ()> {
    pub table: &'static str,
    pub name: &'static str,
    pub data_type: DataTypeDescriptor,
    pub nullable: bool,
    marker: PhantomData<fn() -> (T, Owner)>,
}

impl<T, Owner> TypedColumn<T, Owner> {
    pub const fn new(
        table: &'static str,
        name: &'static str,
        data_type: DataTypeDescriptor,
        nullable: bool,
    ) -> Self {
        Self {
            table,
            name,
            data_type,
            nullable,
            marker: PhantomData,
        }
    }
    pub fn expr(self) -> Expr {
        Expr::qualified(self.table, self.name)
    }

    typed_expression_methods!();
}

impl<T, Owner> From<TypedColumn<T, Owner>> for Expr {
    fn from(value: TypedColumn<T, Owner>) -> Self {
        value.expr()
    }
}

impl<T, Owner> From<&TypedColumn<T, Owner>> for Expr {
    fn from(value: &TypedColumn<T, Owner>) -> Self {
        Expr::qualified(value.table, value.name)
    }
}

impl<T, Owner> From<TypedColumn<T, Owner>> for String {
    fn from(value: TypedColumn<T, Owner>) -> Self {
        value.name.to_string()
    }
}

/// Statically rooted read-only navigation path. Each additional `field()`
/// appends one schema-validated reference edge; the server remains the final
/// authority for catalog compatibility and rejects stale generated paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedNavigation<T, Root = ()> {
    root: &'static str,
    path: Vec<&'static str>,
    marker: PhantomData<fn() -> (T, Root)>,
}

impl<Source, Target: GeneratedEntity> TypedColumn<Reference<Target>, Source> {
    pub fn field<U>(self, target: TypedColumn<U, Target>) -> TypedNavigation<U, Source> {
        TypedNavigation {
            root: self.table,
            path: vec![self.name, target.name],
            marker: PhantomData,
        }
    }
}

impl<Root, Target: GeneratedEntity> TypedNavigation<Reference<Target>, Root> {
    pub fn field<U>(mut self, target: TypedColumn<U, Target>) -> TypedNavigation<U, Root> {
        self.path.push(target.name);
        TypedNavigation {
            root: self.root,
            path: self.path,
            marker: PhantomData,
        }
    }
}

impl<T, Root> TypedNavigation<T, Root> {
    pub fn expr(self) -> Expr {
        Expr::navigation(self.root, self.path)
    }

    typed_expression_methods!();
}

impl<T, Root> From<TypedNavigation<T, Root>> for Expr {
    fn from(value: TypedNavigation<T, Root>) -> Self {
        value.expr()
    }
}

pub trait GeneratedEntity {
    type Record;
    const TABLE: &'static str;
    const CATALOG_ID: &'static str;
    const SCHEMA_FINGERPRINT: &'static str;
}

/// A generated one-column PRIMARY KEY or UNIQUE NOT NULL descriptor.
///
/// The owner type binds the key to one generated entity, while the encoder
/// preserves the exact RadixDB value tag. References can therefore be created
/// only through a key that codegen proved valid against the saved descriptor.
#[derive(Debug, Clone)]
pub struct TypedKey<Owner, T> {
    column: TypedColumn<T, Owner>,
    primary: bool,
    encoder: fn(T) -> TypedValue,
}

impl<Owner, T> TypedKey<Owner, T> {
    pub const fn new(
        column: TypedColumn<T, Owner>,
        primary: bool,
        encoder: fn(T) -> TypedValue,
    ) -> Self {
        Self {
            column,
            primary,
            encoder,
        }
    }

    pub fn column(&self) -> &TypedColumn<T, Owner> {
        &self.column
    }

    pub const fn is_primary(&self) -> bool {
        self.primary
    }

    pub fn reference(&self, key: T) -> Reference<Owner> {
        Reference::new(self.column.table, self.column.name, (self.encoder)(key))
    }
}

/// Runtime bridge implemented by deterministic generated record types.
/// Execution remains transport-owned; generated code only converts between
/// its typed fields and the canonical [`DynamicRecord`].
pub trait GeneratedRecord: Sized {
    type Entity: GeneratedEntity<Record = Self>;

    fn to_dynamic(
        &self,
        descriptor: &TableDescriptor,
    ) -> Result<DynamicRecord, GeneratedRecordError>;

    fn apply_dynamic(&mut self, record: &DynamicRecord) -> Result<(), GeneratedRecordError>;
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("generated schema changed: expected {expected}, actual {actual}")]
pub struct SchemaChanged {
    pub expected: String,
    pub actual: String,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum GeneratedRecordError {
    #[error(transparent)]
    Schema(#[from] SchemaChanged),
    #[error(transparent)]
    Record(#[from] RecordError),
}

pub fn ensure_schema_fingerprint(expected: &str, actual: &str) -> Result<(), SchemaChanged> {
    if expected == actual {
        Ok(())
    } else {
        Err(SchemaChanged {
            expected: expected.to_string(),
            actual: actual.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FieldValue<T> {
    Omitted,
    Value { value: T },
    Null { data_type: DataTypeDescriptor },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldState<T> {
    value: FieldValue<T>,
    dirty: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    declared_type: Option<DataTypeDescriptor>,
}

impl<T> Default for FieldState<T> {
    fn default() -> Self {
        Self::omitted()
    }
}

impl<T> FieldState<T> {
    pub const fn omitted() -> Self {
        Self {
            value: FieldValue::Omitted,
            dirty: false,
            declared_type: None,
        }
    }
    pub fn typed(data_type: DataTypeDescriptor) -> Self {
        Self {
            value: FieldValue::Omitted,
            dirty: false,
            declared_type: Some(data_type),
        }
    }
    pub fn value(&self) -> &FieldValue<T> {
        &self.value
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    pub fn is_omitted(&self) -> bool {
        matches!(self.value, FieldValue::Omitted)
    }
    pub fn set(&mut self, value: impl Into<T>) {
        self.value = FieldValue::Value {
            value: value.into(),
        };
        self.dirty = true;
    }
    pub fn set_null(&mut self) {
        self.value = FieldValue::Null {
            data_type: self
                .declared_type
                .clone()
                .unwrap_or(DataTypeDescriptor::Null),
        };
        self.dirty = true;
    }
    pub fn set_null_as(&mut self, data_type: DataTypeDescriptor) {
        self.declared_type = Some(data_type.clone());
        self.value = FieldValue::Null { data_type };
        self.dirty = true;
    }
    /// Remove the field from the next INSERT/UPDATE. This also clears a pending
    /// dirty transition; it never writes SQL NULL implicitly.
    pub fn unset(&mut self) {
        self.value = FieldValue::Omitted;
        self.dirty = false;
    }
    pub fn hydrate(&mut self, value: FieldValue<T>) {
        if let FieldValue::Null { data_type } = &value {
            self.declared_type = Some(data_type.clone());
        }
        self.value = value;
        self.dirty = false;
    }
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Reference<T> {
    target_table: String,
    target_column: String,
    key: TypedValue,
    #[serde(skip)]
    marker: PhantomData<T>,
}

/// Runtime counterpart of [`Reference<T>`] for descriptor-driven clients.
/// It carries only a validated target key; it never owns or loads a row.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DynamicReference {
    target_table: String,
    target_column: String,
    key: TypedValue,
}

impl DynamicReference {
    pub(crate) fn new(
        target_table: impl Into<String>,
        target_column: impl Into<String>,
        key: TypedValue,
    ) -> Self {
        Self {
            target_table: target_table.into(),
            target_column: target_column.into(),
            key,
        }
    }

    pub fn key(&self) -> &TypedValue {
        &self.key
    }

    pub fn target_table(&self) -> &str {
        &self.target_table
    }

    pub fn target_column(&self) -> &str {
        &self.target_column
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl<T> Reference<T> {
    pub(crate) fn new(
        target_table: impl Into<String>,
        target_column: impl Into<String>,
        key: TypedValue,
    ) -> Self {
        Self {
            target_table: target_table.into(),
            target_column: target_column.into(),
            key,
            marker: PhantomData,
        }
    }
    pub fn key(&self) -> &TypedValue {
        &self.key
    }

    pub fn target_table(&self) -> &str {
        &self.target_table
    }

    pub fn target_column(&self) -> &str {
        &self.target_column
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl<T> From<Reference<T>> for TypedValue {
    fn from(reference: Reference<T>) -> Self {
        reference.key
    }
}

impl<T> From<Reference<T>> for Expr {
    fn from(reference: Reference<T>) -> Self {
        Expr::value(reference.key)
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RecordError {
    #[error(transparent)]
    Build(#[from] BuilderError),
    #[error("unknown record field '{0}'")]
    UnknownField(String),
    #[error("table '{0}' has no primary key")]
    MissingPrimaryKey(String),
    #[error("record primary-key field '{0}' is omitted or NULL")]
    MissingPrimaryKeyValue(String),
    #[error("record UPDATE has no dirty non-primary-key fields")]
    EmptyUpdate,
    #[error("reference target must be a one-column PRIMARY KEY or UNIQUE NOT NULL key")]
    UnsupportedReferenceTarget,
    #[error("field '{field}' expects {expected:?}, got {actual:?}")]
    ValueTypeMismatch {
        field: String,
        expected: DataTypeDescriptor,
        actual: DataTypeDescriptor,
    },
    #[error("NULL reference keys are not valid; use an explicit typed NULL on the source field")]
    NullReferenceKey,
    #[error(
        "field '{table}.{column}' is not a one-column reference to '{target_table}.{target_column}'"
    )]
    ReferenceSourceMismatch {
        table: String,
        column: String,
        target_table: String,
        target_column: String,
    },
    #[error("hydrated row is missing expected field '{0}'")]
    IncompleteHydration(String),
    #[error("hydrated field '{field}' does not match generated type {expected:?}")]
    HydrationTypeMismatch {
        field: String,
        expected: DataTypeDescriptor,
    },
    #[error("generated query expected one row but returned none")]
    ExpectedOneRow,
    #[error("generated query expected at most one row but returned {0}")]
    ExpectedAtMostOneRow(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedQuery<R> {
    builder: QueryBuilder,
    marker: PhantomData<fn() -> R>,
}

impl<R> GeneratedQuery<R> {
    pub fn new(builder: QueryBuilder) -> Self {
        Self {
            builder,
            marker: PhantomData,
        }
    }

    pub fn builder(&self) -> &QueryBuilder {
        &self.builder
    }

    pub fn document(&self) -> Result<IrDocument, BuilderError> {
        self.builder.document()
    }

    pub fn to_json(&self) -> Result<String, BuilderJsonError> {
        self.builder.to_json()
    }

    pub fn to_sql(&self) -> Result<CompiledStatement, BuilderSqlError> {
        self.builder.to_sql()
    }
}

impl<R: GeneratedRecord + Default> GeneratedQuery<R> {
    pub fn all<S>(self, session: S) -> Result<Vec<R>, S::Error>
    where
        S: OrmGeneratedQuerySession,
        S::Error: From<BuilderError>,
    {
        let document = self.document().map_err(S::Error::from)?;
        session.query_generated_records(&document)
    }

    pub fn one<S>(self, session: S) -> Result<R, S::Error>
    where
        S: OrmGeneratedQuerySession,
        S::Error: From<RecordError> + From<BuilderError>,
    {
        let mut rows = self.all(session)?;
        match rows.len() {
            1 => Ok(rows.pop().expect("one row")),
            0 => Err(RecordError::ExpectedOneRow.into()),
            count => Err(RecordError::ExpectedAtMostOneRow(count).into()),
        }
    }

    pub fn optional<S>(self, session: S) -> Result<Option<R>, S::Error>
    where
        S: OrmGeneratedQuerySession,
        S::Error: From<RecordError> + From<BuilderError>,
    {
        let mut rows = self.all(session)?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            count => Err(RecordError::ExpectedAtMostOneRow(count).into()),
        }
    }

    pub async fn all_async<S>(self, session: &mut S) -> Result<Vec<R>, S::Error>
    where
        S: AsyncOrmGeneratedQuerySession,
        S::Error: From<BuilderError>,
    {
        let document = self.document().map_err(S::Error::from)?;
        session.query_generated_records_async(&document).await
    }

    pub async fn one_async<S>(self, session: &mut S) -> Result<R, S::Error>
    where
        S: AsyncOrmGeneratedQuerySession,
        S::Error: From<RecordError> + From<BuilderError>,
    {
        let mut rows = self.all_async(session).await?;
        match rows.len() {
            1 => Ok(rows.pop().expect("one row")),
            0 => Err(RecordError::ExpectedOneRow.into()),
            count => Err(RecordError::ExpectedAtMostOneRow(count).into()),
        }
    }

    pub async fn optional_async<S>(self, session: &mut S) -> Result<Option<R>, S::Error>
    where
        S: AsyncOrmGeneratedQuerySession,
        S::Error: From<RecordError> + From<BuilderError>,
    {
        let mut rows = self.all_async(session).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            count => Err(RecordError::ExpectedAtMostOneRow(count).into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamicRecord {
    descriptor: TableDescriptor,
    fields: BTreeMap<String, FieldState<TypedValue>>,
}

impl DynamicRecord {
    pub fn new(descriptor: TableDescriptor) -> Self {
        let fields = descriptor
            .columns
            .iter()
            .map(|column| (column.name.clone(), FieldState::omitted()))
            .collect();
        Self { descriptor, fields }
    }
    pub fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }
    pub fn field(&self, name: &str) -> Result<&FieldState<TypedValue>, RecordError> {
        self.fields
            .get(name)
            .ok_or_else(|| RecordError::UnknownField(name.to_string()))
    }
    pub fn field_mut(&mut self, name: &str) -> Result<&mut FieldState<TypedValue>, RecordError> {
        self.fields
            .get_mut(name)
            .ok_or_else(|| RecordError::UnknownField(name.to_string()))
    }
    pub fn set(&mut self, name: &str, value: TypedValue) -> Result<(), RecordError> {
        let column = self.column_descriptor(name)?;
        if !typed_value_matches(&value, &column.data_type) {
            return Err(RecordError::ValueTypeMismatch {
                field: name.to_string(),
                expected: column.data_type.clone(),
                actual: value.data_type(),
            });
        }
        self.field_mut(name)?.set(value);
        Ok(())
    }
    pub fn set_reference(
        &mut self,
        name: &str,
        reference: &DynamicReference,
    ) -> Result<(), RecordError> {
        let matches = self.descriptor.constraints.iter().any(|constraint| {
            let ConstraintDefinition::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
                ..
            } = &constraint.definition
            else {
                return false;
            };
            columns.as_slice() == [name]
                && referenced_table == &reference.target_table
                && referenced_columns.as_slice() == [reference.target_column.as_str()]
        });
        if !matches {
            return Err(RecordError::ReferenceSourceMismatch {
                table: self.descriptor.name.clone(),
                column: name.to_string(),
                target_table: reference.target_table.clone(),
                target_column: reference.target_column.clone(),
            });
        }
        self.set(name, reference.key.clone())
    }
    pub fn set_null(&mut self, name: &str) -> Result<(), RecordError> {
        let data_type = self
            .descriptor
            .columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.data_type.clone())
            .ok_or_else(|| RecordError::UnknownField(name.to_string()))?;
        self.field_mut(name)?.set_null_as(data_type);
        Ok(())
    }
    pub fn unset(&mut self, name: &str) -> Result<(), RecordError> {
        self.field_mut(name)?.unset();
        Ok(())
    }

    /// Install a value read from the server without marking it dirty.
    /// Generated facades use this to preserve PK and clean-field state while
    /// converting back into the shared dynamic mutation owner.
    pub fn hydrate(&mut self, name: &str, value: TypedValue) -> Result<(), RecordError> {
        let column = self.column_descriptor(name)?;
        let hydrated = match value {
            TypedValue::Null(data_type) if data_type == column.data_type => {
                FieldValue::Null { data_type }
            }
            TypedValue::Null(data_type) => {
                return Err(RecordError::ValueTypeMismatch {
                    field: name.to_string(),
                    expected: column.data_type.clone(),
                    actual: data_type,
                });
            }
            value if typed_value_matches(&value, &column.data_type) => FieldValue::Value { value },
            value => {
                return Err(RecordError::ValueTypeMismatch {
                    field: name.to_string(),
                    expected: column.data_type.clone(),
                    actual: value.data_type(),
                });
            }
        };
        self.field_mut(name)?.hydrate(hydrated);
        Ok(())
    }
    pub fn is_dirty(&self) -> bool {
        self.fields.values().any(FieldState::is_dirty)
    }

    pub fn insert<S: OrmRecordSession>(&mut self, session: S) -> Result<(), S::Error> {
        session.mutate_record(self, RecordMutation::Insert)
    }

    pub fn save<S: OrmRecordSession>(&mut self, session: S) -> Result<(), S::Error> {
        session.mutate_record(self, RecordMutation::Save)
    }

    pub fn update<S: OrmRecordSession>(&mut self, session: S) -> Result<(), S::Error> {
        session.mutate_record(self, RecordMutation::Update)
    }

    pub fn delete<S: OrmRecordSession>(&mut self, session: S) -> Result<(), S::Error> {
        session.mutate_record(self, RecordMutation::Delete)
    }

    pub async fn insert_async<S: AsyncOrmRecordSession>(
        &mut self,
        session: &mut S,
    ) -> Result<(), S::Error> {
        session
            .mutate_record_async(self, RecordMutation::Insert)
            .await
    }

    pub async fn save_async<S: AsyncOrmRecordSession>(
        &mut self,
        session: &mut S,
    ) -> Result<(), S::Error> {
        session
            .mutate_record_async(self, RecordMutation::Save)
            .await
    }

    pub async fn update_async<S: AsyncOrmRecordSession>(
        &mut self,
        session: &mut S,
    ) -> Result<(), S::Error> {
        session
            .mutate_record_async(self, RecordMutation::Update)
            .await
    }

    pub async fn delete_async<S: AsyncOrmRecordSession>(
        &mut self,
        session: &mut S,
    ) -> Result<(), S::Error> {
        session
            .mutate_record_async(self, RecordMutation::Delete)
            .await
    }

    pub fn insert_document(&self) -> Result<IrDocument, RecordError> {
        let (columns, values) = self.present_fields(false)?;
        Ok(IrDocument::new(Operation::Insert {
            statement: Insert {
                table: self.descriptor.name.clone(),
                columns,
                rows: vec![values],
                source: None,
                returning: return_all(),
            },
        }))
    }

    pub fn save_document(&self) -> Result<IrDocument, RecordError> {
        let primary_key = self.primary_key_columns()?;
        self.primary_key_predicate(&primary_key)?;
        let (columns, values) = self.present_fields(false)?;
        let primary: BTreeSet<_> = primary_key.iter().cloned().collect();
        let assignments = self
            .fields
            .iter()
            .filter(|(column, state)| state.is_dirty() && !primary.contains(*column))
            .map(|(column, _)| Assignment {
                column: column.clone(),
                value: Expression::Column {
                    column: ColumnRef::qualified("excluded", column.clone()),
                },
            })
            .collect();
        Ok(IrDocument::new(Operation::Upsert {
            statement: Upsert {
                insert: Insert {
                    table: self.descriptor.name.clone(),
                    columns,
                    rows: vec![values],
                    source: None,
                    returning: return_all(),
                },
                conflict_columns: primary_key,
                assignments,
            },
        }))
    }

    pub fn update_document(&self) -> Result<IrDocument, RecordError> {
        let primary_key = self.primary_key_columns()?;
        let filter = self.primary_key_predicate(&primary_key)?;
        let primary: BTreeSet<_> = primary_key.iter().cloned().collect();
        let assignments: Vec<_> = self
            .fields
            .iter()
            .filter(|(name, state)| state.is_dirty() && !primary.contains(*name))
            .filter_map(|(name, state)| {
                field_expression(state).map(|value| Assignment {
                    column: name.clone(),
                    value,
                })
            })
            .collect();
        if assignments.is_empty() {
            return Err(RecordError::EmptyUpdate);
        }
        Ok(IrDocument::new(Operation::Update {
            statement: Update {
                table: self.descriptor.name.clone(),
                alias: None,
                assignments,
                from: None,
                filter: Some(filter),
                returning: return_all(),
            },
        }))
    }

    pub fn delete_document(&self) -> Result<IrDocument, RecordError> {
        let primary_key = self.primary_key_columns()?;
        let filter = self.primary_key_predicate(&primary_key)?;
        Ok(IrDocument::new(Operation::Delete {
            statement: Delete {
                table: self.descriptor.name.clone(),
                alias: None,
                using: None,
                filter: Some(filter),
                all_rows: false,
                returning: Vec::new(),
            },
        }))
    }

    /// Replace the local record only after the server returned a complete
    /// `RETURNING *` row. Callers keep dirty state untouched on every error.
    pub fn apply_returning(
        &mut self,
        values: BTreeMap<String, TypedValue>,
    ) -> Result<(), RecordError> {
        for column in &self.descriptor.columns {
            if !values.contains_key(&column.name) {
                return Err(RecordError::IncompleteHydration(column.name.clone()));
            }
        }
        for column in &self.descriptor.columns {
            let value = values.get(&column.name).expect("checked complete").clone();
            let hydrated = match value {
                TypedValue::Null(data_type) if data_type == column.data_type => {
                    FieldValue::Null { data_type }
                }
                TypedValue::Null(data_type) => {
                    return Err(RecordError::ValueTypeMismatch {
                        field: column.name.clone(),
                        expected: column.data_type.clone(),
                        actual: data_type,
                    });
                }
                value if typed_value_matches(&value, &column.data_type) => {
                    FieldValue::Value { value }
                }
                value => {
                    return Err(RecordError::ValueTypeMismatch {
                        field: column.name.clone(),
                        expected: column.data_type.clone(),
                        actual: value.data_type(),
                    });
                }
            };
            self.fields
                .get_mut(&column.name)
                .expect("descriptor owns field")
                .hydrate(hydrated);
        }
        Ok(())
    }

    fn column_descriptor(&self, name: &str) -> Result<&ColumnDescriptor, RecordError> {
        self.descriptor
            .columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| RecordError::UnknownField(name.to_string()))
    }

    fn present_fields(
        &self,
        dirty_only: bool,
    ) -> Result<(Vec<String>, Vec<Expression>), RecordError> {
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for column in &self.descriptor.columns {
            let state = self
                .fields
                .get(&column.name)
                .expect("descriptor owns field");
            if dirty_only && !state.is_dirty() {
                continue;
            }
            if let Some(value) = field_expression(state) {
                columns.push(column.name.clone());
                values.push(value);
            }
        }
        if columns.is_empty() {
            return Err(RecordError::EmptyUpdate);
        }
        Ok((columns, values))
    }

    fn primary_key_columns(&self) -> Result<Vec<String>, RecordError> {
        self.descriptor
            .constraints
            .iter()
            .find_map(|constraint| match &constraint.definition {
                crate::ConstraintDefinition::PrimaryKey { columns } => Some(columns.clone()),
                _ => None,
            })
            .filter(|columns| !columns.is_empty())
            .ok_or_else(|| RecordError::MissingPrimaryKey(self.descriptor.name.clone()))
    }

    fn primary_key_predicate(&self, columns: &[String]) -> Result<Expression, RecordError> {
        let mut predicates = Vec::new();
        for column in columns {
            let state = self
                .fields
                .get(column)
                .ok_or_else(|| RecordError::UnknownField(column.clone()))?;
            let FieldValue::Value { value } = state.value() else {
                return Err(RecordError::MissingPrimaryKeyValue(column.clone()));
            };
            predicates.push(Expression::Binary {
                left: Box::new(Expression::column(column)),
                operator: BinaryOperator::Eq,
                right: Box::new(Expression::literal(value.clone())),
            });
        }
        Ok(predicates
            .into_iter()
            .reduce(|left, right| Expression::Binary {
                left: Box::new(left),
                operator: BinaryOperator::And,
                right: Box::new(right),
            })
            .expect("primary key is nonempty"))
    }
}

/// Decode one canonical dynamic field into a generated Rust field while
/// preserving omitted/NULL/clean state exactly.
pub fn decode_generated_field<T: GeneratedValue>(
    field: &str,
    state: &FieldState<TypedValue>,
    expected: &DataTypeDescriptor,
) -> Result<FieldValue<T>, RecordError> {
    match state.value() {
        FieldValue::Omitted => Ok(FieldValue::Omitted),
        FieldValue::Null { data_type } if data_type == expected => Ok(FieldValue::Null {
            data_type: data_type.clone(),
        }),
        FieldValue::Null { .. } => Err(RecordError::HydrationTypeMismatch {
            field: field.to_string(),
            expected: expected.clone(),
        }),
        FieldValue::Value { value } => T::decode(value, expected)
            .map(|value| FieldValue::Value { value })
            .map_err(|_| RecordError::HydrationTypeMismatch {
                field: field.to_string(),
                expected: expected.clone(),
            }),
    }
}

pub fn decode_generated_reference_field<T>(
    field: &str,
    state: &FieldState<TypedValue>,
    expected: &DataTypeDescriptor,
    target_table: &str,
    target_column: &str,
) -> Result<FieldValue<Reference<T>>, RecordError> {
    match state.value() {
        FieldValue::Omitted => Ok(FieldValue::Omitted),
        FieldValue::Null { data_type } if data_type == expected => Ok(FieldValue::Null {
            data_type: data_type.clone(),
        }),
        FieldValue::Null { .. } => Err(RecordError::HydrationTypeMismatch {
            field: field.to_string(),
            expected: expected.clone(),
        }),
        FieldValue::Value { value } if typed_value_matches(value, expected) => {
            Ok(FieldValue::Value {
                value: Reference::new(target_table, target_column, value.clone()),
            })
        }
        FieldValue::Value { .. } => Err(RecordError::HydrationTypeMismatch {
            field: field.to_string(),
            expected: expected.clone(),
        }),
    }
}

pub(crate) fn typed_value_matches(value: &TypedValue, expected: &DataTypeDescriptor) -> bool {
    matches!(
        (value, expected),
        (TypedValue::Integer(_), DataTypeDescriptor::Integer)
            | (TypedValue::Float(_), DataTypeDescriptor::Float)
            | (TypedValue::Text(_), DataTypeDescriptor::Text)
            | (TypedValue::Boolean(_), DataTypeDescriptor::Boolean)
            | (TypedValue::Timestamp(_), DataTypeDescriptor::Timestamp)
            | (TypedValue::Date(_), DataTypeDescriptor::Date)
            | (TypedValue::Json(_), DataTypeDescriptor::Json)
            | (TypedValue::Uuid(_), DataTypeDescriptor::Uuid)
            | (TypedValue::Bytes(_), DataTypeDescriptor::Bytes)
            | (TypedValue::Decimal(_), DataTypeDescriptor::Decimal { .. })
            | (TypedValue::Vector(_), DataTypeDescriptor::Vector { .. })
    )
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("typed value does not match the generated field type")]
pub struct GeneratedValueDecodeError;

pub trait GeneratedValue: Sized {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError>;
}

impl GeneratedValue for i64 {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Integer(value), DataTypeDescriptor::Integer) => Ok(*value),
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

impl GeneratedValue for f64 {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Float(value), DataTypeDescriptor::Float) => Ok(value.as_f64()),
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

impl GeneratedValue for bool {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Boolean(value), DataTypeDescriptor::Boolean) => Ok(*value),
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

impl GeneratedValue for serde_json::Value {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Json(value), DataTypeDescriptor::Json) => Ok(value.clone()),
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

impl GeneratedValue for Vec<f32> {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Vector(value), DataTypeDescriptor::Vector { dimensions })
                if value.len() == usize::from(*dimensions) =>
            {
                Ok(value.clone())
            }
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

impl GeneratedValue for String {
    fn decode(
        value: &TypedValue,
        expected: &DataTypeDescriptor,
    ) -> Result<Self, GeneratedValueDecodeError> {
        match (value, expected) {
            (TypedValue::Text(value), DataTypeDescriptor::Text)
            | (TypedValue::Timestamp(value), DataTypeDescriptor::Timestamp)
            | (TypedValue::Date(value), DataTypeDescriptor::Date)
            | (TypedValue::Uuid(value), DataTypeDescriptor::Uuid)
            | (TypedValue::Bytes(value), DataTypeDescriptor::Bytes)
            | (TypedValue::Decimal(value), DataTypeDescriptor::Decimal { .. }) => Ok(value.clone()),
            _ => Err(GeneratedValueDecodeError),
        }
    }
}

pub fn validate_reference_target(table: &TableDescriptor, column: &str) -> Result<(), RecordError> {
    let target = table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or(RecordError::UnsupportedReferenceTarget)?;
    if target.nullable {
        return Err(RecordError::UnsupportedReferenceTarget);
    }
    let valid = table
        .constraints
        .iter()
        .any(|constraint| match &constraint.definition {
            crate::ConstraintDefinition::PrimaryKey { columns }
            | crate::ConstraintDefinition::Unique { columns, .. } => columns.as_slice() == [column],
            _ => false,
        });
    if valid {
        Ok(())
    } else {
        Err(RecordError::UnsupportedReferenceTarget)
    }
}

fn field_expression(state: &FieldState<TypedValue>) -> Option<Expression> {
    match state.value() {
        FieldValue::Omitted => None,
        FieldValue::Value { value } => Some(Expression::literal(value.clone())),
        FieldValue::Null { data_type } => {
            Some(Expression::literal(TypedValue::Null(data_type.clone())))
        }
    }
}

fn return_all() -> Vec<Projection> {
    vec![Projection {
        expression: Expression::Star { relation: None },
        alias: None,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Clone, PartialEq)]
    struct TestGeneratedRecord;

    struct TestGeneratedEntity;

    struct EmployeeEntity;
    struct DepartmentEntity;

    impl GeneratedEntity for TestGeneratedEntity {
        type Record = TestGeneratedRecord;
        const TABLE: &'static str = "people";
        const CATALOG_ID: &'static str = "test";
        const SCHEMA_FINGERPRINT: &'static str = "test";
    }

    impl GeneratedEntity for EmployeeEntity {
        type Record = TestGeneratedRecord;
        const TABLE: &'static str = "employees";
        const CATALOG_ID: &'static str = "employees";
        const SCHEMA_FINGERPRINT: &'static str = "employees";
    }

    impl GeneratedEntity for DepartmentEntity {
        type Record = TestGeneratedRecord;
        const TABLE: &'static str = "departments";
        const CATALOG_ID: &'static str = "departments";
        const SCHEMA_FINGERPRINT: &'static str = "departments";
    }

    impl GeneratedRecord for TestGeneratedRecord {
        type Entity = TestGeneratedEntity;

        fn to_dynamic(
            &self,
            _descriptor: &TableDescriptor,
        ) -> Result<DynamicRecord, GeneratedRecordError> {
            unreachable!("query mock does not mutate records")
        }

        fn apply_dynamic(&mut self, _record: &DynamicRecord) -> Result<(), GeneratedRecordError> {
            unreachable!("query mock returns already decoded records")
        }
    }

    struct MockGeneratedQuerySession(Vec<TestGeneratedRecord>);

    impl OrmGeneratedQuerySession for MockGeneratedQuerySession {
        type Error = RecordError;

        fn query_generated_records<R: GeneratedRecord + Default>(
            self,
            _document: &IrDocument,
        ) -> Result<Vec<R>, Self::Error> {
            Ok(self.0.into_iter().map(|_| R::default()).collect())
        }
    }

    fn descriptor() -> TableDescriptor {
        TableDescriptor {
            catalog_id: "00000000-0000-0000-0000-000000000001".to_string(),
            name: "people".to_string(),
            schema_generation: 1,
            fingerprint: "f".to_string(),
            created_at: "x".to_string(),
            updated_at: "x".to_string(),
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
                    nullable: true,
                    auto_increment: false,
                    default_expression: None,
                    extensions: BTreeMap::new(),
                },
                ColumnDescriptor {
                    ordinal: 2,
                    name: "note".to_string(),
                    data_type: DataTypeDescriptor::Text,
                    nullable: true,
                    auto_increment: false,
                    default_expression: None,
                    extensions: BTreeMap::new(),
                },
            ],
            constraints: vec![ConstraintDescriptor {
                id: 1,
                name: "pk_people".to_string(),
                definition: crate::ConstraintDefinition::PrimaryKey {
                    columns: vec!["id".to_string()],
                },
            }],
            indexes: Vec::new(),
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn record_distinguishes_omitted_null_value_and_keeps_dirty_until_hydration() {
        let mut record = DynamicRecord::new(descriptor());
        record.set("id", TypedValue::Integer(7)).unwrap();
        record.set_null("name").unwrap();
        let insert = record.insert_document().unwrap().to_sql().unwrap();
        assert!(insert.sql.ends_with("RETURNING *"));
        assert_eq!(insert.parameters.len(), 2);
        assert!(record.is_dirty());

        record.unset("name").unwrap();
        assert!(record.update_document().is_err());
        record
            .set("name", TypedValue::Text("Alice".to_string()))
            .unwrap();
        let update = record.update_document().unwrap().to_sql().unwrap();
        assert_eq!(
            update.parameters,
            vec![
                TypedValue::Text("Alice".to_string()),
                TypedValue::Integer(7)
            ]
        );
        assert!(record.is_dirty());

        record
            .apply_returning(BTreeMap::from([
                ("id".to_string(), TypedValue::Integer(7)),
                ("name".to_string(), TypedValue::Text("Alice".to_string())),
                ("note".to_string(), TypedValue::Text("clean".to_string())),
            ]))
            .unwrap();
        assert!(!record.is_dirty());

        record
            .set("name", TypedValue::Text("Changed".to_string()))
            .unwrap();
        let save = record.save_document().unwrap().to_sql().unwrap();
        assert!(save
            .sql
            .contains("DO UPDATE SET \"name\" = \"excluded\".\"name\""));
        assert!(!save.sql.contains("SET \"note\""));
    }

    #[test]
    fn generated_query_cardinality_is_fail_closed() {
        let query = || {
            GeneratedQuery::<TestGeneratedRecord>::new(
                QueryBuilder::from_relation(crate::table("people")).select([Expr::star()]),
            )
        };
        assert_eq!(
            query()
                .one(MockGeneratedQuerySession(vec![TestGeneratedRecord]))
                .unwrap(),
            TestGeneratedRecord
        );
        assert!(matches!(
            query().one(MockGeneratedQuerySession(Vec::new())),
            Err(RecordError::ExpectedOneRow)
        ));
        assert!(matches!(
            query().optional(MockGeneratedQuerySession(vec![
                TestGeneratedRecord,
                TestGeneratedRecord,
            ])),
            Err(RecordError::ExpectedAtMostOneRow(2))
        ));
        let invalid = GeneratedQuery::<TestGeneratedRecord>::new(QueryBuilder::from_relation(
            crate::table("people"),
        ));
        assert!(matches!(
            invalid.all(MockGeneratedQuerySession(Vec::new())),
            Err(RecordError::Build(BuilderError::EmptyProjection))
        ));
    }

    #[test]
    fn reference_targets_require_one_nonnullable_primary_or_unique_column() {
        let mut table = descriptor();
        assert!(validate_reference_target(&table, "id").is_ok());
        assert!(matches!(
            validate_reference_target(&table, "name"),
            Err(RecordError::UnsupportedReferenceTarget)
        ));

        table.columns[1].nullable = false;
        table.constraints.push(ConstraintDescriptor {
            id: 2,
            name: "uq_people_name".to_string(),
            definition: crate::ConstraintDefinition::Unique {
                columns: vec!["name".to_string()],
                owned_index: "uq_people_name".to_string(),
            },
        });
        assert!(validate_reference_target(&table, "name").is_ok());
        table.constraints[1].definition = crate::ConstraintDefinition::Unique {
            columns: vec!["name".to_string(), "note".to_string()],
            owned_index: "uq_people_name_note".to_string(),
        };
        assert!(matches!(
            validate_reference_target(&table, "name"),
            Err(RecordError::UnsupportedReferenceTarget)
        ));
    }

    #[test]
    fn dynamic_references_are_key_only_typed_and_source_checked() {
        let target = descriptor();
        let target_entity = DynamicEntity::new(target.clone());
        let reference = target_entity
            .reference("id", TypedValue::Integer(7))
            .unwrap();
        assert_eq!(reference.key(), &TypedValue::Integer(7));
        assert_eq!(reference.target_table(), "people");
        assert_eq!(reference.target_column(), "id");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reference.to_json().unwrap()).unwrap(),
            serde_json::json!({
                "target_table": "people",
                "target_column": "id",
                "key": { "type": "integer", "value": 7 }
            })
        );
        assert!(matches!(
            target_entity.reference("name", TypedValue::Text("x".to_string())),
            Err(RecordError::UnsupportedReferenceTarget)
        ));
        assert!(matches!(
            target_entity.reference("id", TypedValue::Float(7.0.into())),
            Err(RecordError::ValueTypeMismatch { .. })
        ));

        let mut source = descriptor();
        source.name = "documents".to_string();
        source.constraints.push(ConstraintDescriptor {
            id: 2,
            name: "fk_documents_id___people".to_string(),
            definition: ConstraintDefinition::ForeignKey {
                columns: vec!["id".to_string()],
                referenced_table: "people".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyActionDescriptor::Restrict,
                on_update: ForeignKeyActionDescriptor::Restrict,
            },
        });
        let mut record = DynamicRecord::new(source);
        record.set_reference("id", &reference).unwrap();
        assert_eq!(
            record.field("id").unwrap().value(),
            &FieldValue::Value {
                value: TypedValue::Integer(7)
            }
        );

        let other = DynamicReference::new("other", "id", TypedValue::Integer(7));
        assert!(matches!(
            record.set_reference("id", &other),
            Err(RecordError::ReferenceSourceMismatch { .. })
        ));
    }

    #[test]
    fn typed_columns_cover_predicates_and_transitive_navigation() {
        let employee = TypedColumn::<Reference<EmployeeEntity>, TestGeneratedEntity>::new(
            "payroll_documents",
            "employee_id",
            DataTypeDescriptor::Integer,
            false,
        );
        let department = TypedColumn::<Reference<DepartmentEntity>, EmployeeEntity>::new(
            "employees",
            "department_id",
            DataTypeDescriptor::Integer,
            false,
        );
        let department_name = TypedColumn::<String, DepartmentEntity>::new(
            "departments",
            "name",
            DataTypeDescriptor::Text,
            false,
        );

        let path = employee.field(department).field(department_name);
        assert_eq!(
            path.expr().0,
            Expression::Navigation {
                root: "payroll_documents".to_string(),
                path: vec![
                    "employee_id".to_string(),
                    "department_id".to_string(),
                    "name".to_string(),
                ],
            }
        );

        let score = TypedColumn::<i64>::new("people", "score", DataTypeDescriptor::Integer, true);
        let query = QueryBuilder::from_relation(crate::table("people"))
            .select([score.clone().expr()])
            .filter(score.clone().between(10_i64, 20_i64).or(score.is_null()));
        let compiled = query.to_sql().unwrap();
        assert_eq!(compiled.parameters.len(), 2);
        assert!(compiled.sql.contains("BETWEEN $1 AND $2"));
    }
}
