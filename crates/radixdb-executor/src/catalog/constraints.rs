use std::collections::BTreeSet;

use radixdb_catalog::{
    CatalogDataType, CatalogEdge, CatalogGeneration, CatalogName, CatalogObject, CatalogPayload,
    ConstraintPayload, EdgeKind, ForeignKeyAction as CatalogForeignKeyAction, ForeignKeyMatch,
    ObjectId, ObjectKind,
};
use radixdb_core::{
    generated_constraint_name, DataType, Error, ForeignKeyAction, Result, SchemaBuilder,
};
use radixdb_sql::ast::{ColumnConstraint, ColumnDefinition, CreateTableStatement, TableConstraint};

use crate::expression::ExpressionEval;
use crate::mutation::validation::compile_table_check_constraints;

use super::transaction::{catalog_argument, ObjectIdSource};

#[derive(Debug, Clone)]
pub(super) struct ForeignKeyInput {
    referenced_table: String,
    referenced_column: Option<String>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
}

#[derive(Debug, Clone)]
pub(super) struct BoundColumn {
    pub id: ObjectId,
    pub name: CatalogName,
    pub data_type: CatalogDataType,
    pub explicit_not_null: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub default_sql: Option<String>,
    pub unique: bool,
    pub checks: Vec<String>,
    pub foreign_key: Option<ForeignKeyInput>,
}

impl BoundColumn {
    pub fn bind(
        definition: &ColumnDefinition,
        id: ObjectId,
        data_type: CatalogDataType,
    ) -> Result<Self> {
        let mut explicit_not_null = false;
        let mut primary_key = false;
        let mut auto_increment = false;
        let mut unique = false;
        let mut default_sql = None;
        let mut checks = Vec::new();
        let mut foreign_key = None;

        for constraint in &definition.constraints {
            match constraint {
                ColumnConstraint::NotNull => {
                    set_once(&mut explicit_not_null, &definition.name.value, "NOT NULL")?
                }
                ColumnConstraint::PrimaryKey => {
                    set_once(&mut primary_key, &definition.name.value, "PRIMARY KEY")?
                }
                ColumnConstraint::Unique => {
                    set_once(&mut unique, &definition.name.value, "UNIQUE")?
                }
                ColumnConstraint::AutoIncrement => set_once(
                    &mut auto_increment,
                    &definition.name.value,
                    "AUTO_INCREMENT",
                )?,
                ColumnConstraint::Default(expression) => {
                    ExpressionEval::compile(expression, &[]).map_err(|error| {
                        Error::InvalidArgument(format!(
                            "invalid DEFAULT for column '{}': {error}",
                            definition.name.value
                        ))
                    })?;
                    if default_sql.replace(expression.to_string()).is_some() {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' has more than one DEFAULT constraint",
                            definition.name.value
                        )));
                    }
                }
                ColumnConstraint::Check(expression) => checks.push(expression.to_string()),
                ColumnConstraint::References {
                    table,
                    column,
                    on_delete,
                    on_update,
                } => {
                    if foreign_key
                        .replace(ForeignKeyInput {
                            referenced_table: table.value.to_string(),
                            referenced_column: column
                                .as_ref()
                                .map(|column| column.value.to_string()),
                            on_delete: *on_delete,
                            on_update: *on_update,
                        })
                        .is_some()
                    {
                        return Err(Error::InvalidArgument(format!(
                            "column '{}' has more than one REFERENCES constraint",
                            definition.name.value
                        )));
                    }
                }
            }
        }
        if auto_increment && !matches!(data_type.logical_type(), DataType::Integer | DataType::Uuid)
        {
            return Err(Error::InvalidArgument(format!(
                "AUTO_INCREMENT column '{}' must be INTEGER or UUID",
                definition.name.value
            )));
        }
        if data_type.is_external() && default_sql.is_some() {
            return Err(Error::NotSupported(format!(
                "DEFAULT for external column '{}' requires an explicit native/text constructor",
                definition.name.value
            )));
        }

        Ok(Self {
            id,
            name: CatalogName::new(definition.name.value.as_str()).map_err(catalog_argument)?,
            data_type,
            explicit_not_null,
            primary_key,
            auto_increment,
            default_sql,
            unique,
            checks,
            foreign_key,
        })
    }

    pub fn is_nullable(&self, primary_key_columns: &[ObjectId]) -> bool {
        !self.explicit_not_null && !primary_key_columns.contains(&self.id)
    }
}

#[derive(Debug, Default)]
pub(super) struct BoundConstraints {
    pub objects: Vec<CatalogObject>,
    pub edges: Vec<CatalogEdge>,
    pub ids: Vec<ObjectId>,
    pub primary_key_id: Option<ObjectId>,
    pub primary_key_columns: Vec<ObjectId>,
}

pub(super) fn bind_constraints(
    statement: &CreateTableStatement,
    table_id: ObjectId,
    columns: &[BoundColumn],
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<BoundConstraints> {
    let mut output = BoundConstraints::default();
    let mut names = BTreeSet::new();

    let primary_key_columns = bind_primary_key(statement, columns)?;
    if !primary_key_columns.is_empty() {
        let primary_key_names = primary_key_columns
            .iter()
            .map(|id| {
                columns
                    .iter()
                    .find(|column| column.id == *id)
                    .expect("bound primary-key column exists")
                    .name
                    .normalized()
                    .as_str()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let id = push_constraint(
            &mut output,
            &mut names,
            ids,
            generation,
            table_id,
            format!("pk_{}", statement.table_name.value_lower),
            "primary_key",
            statement.table_name.value_lower.as_str(),
            &primary_key_names,
            &[],
            ConstraintPayload::primary_key(primary_key_columns.clone())
                .map_err(catalog_argument)?,
            &[],
        )?;
        output.primary_key_id = Some(id);
        output.primary_key_columns = primary_key_columns;
    }

    for column in columns {
        if column.explicit_not_null {
            let column_names = vec![column.name.normalized().as_str().to_owned()];
            push_constraint(
                &mut output,
                &mut names,
                ids,
                generation,
                table_id,
                format!(
                    "nn_{}_{}",
                    statement.table_name.value_lower,
                    column.name.normalized().as_str()
                ),
                "not_null",
                statement.table_name.value_lower.as_str(),
                &column_names,
                &[],
                ConstraintPayload::not_null(column.id),
                &[],
            )?;
        }
        if column.unique {
            push_unique(
                &mut output,
                &mut names,
                ids,
                generation,
                table_id,
                statement.table_name.value_lower.as_str(),
                &[column.id],
                &[column.name.normalized().as_str()],
            )?;
        }
        for check_sql in &column.checks {
            push_check(
                &mut output,
                &mut names,
                ids,
                generation,
                table_id,
                statement.table_name.value_lower.as_str(),
                Some(column.id),
                check_sql,
            )?;
        }
    }

    for constraint in &statement.table_constraints {
        match constraint {
            TableConstraint::PrimaryKey(_) => {}
            TableConstraint::Unique(names_to_resolve) => {
                let resolved = resolve_local_columns(columns, names_to_resolve)?;
                let display = names_to_resolve
                    .iter()
                    .map(|name| name.value_lower.as_str())
                    .collect::<Vec<_>>();
                push_unique(
                    &mut output,
                    &mut names,
                    ids,
                    generation,
                    table_id,
                    statement.table_name.value_lower.as_str(),
                    &resolved,
                    &display,
                )?;
            }
            TableConstraint::Check(expression) => {
                push_check(
                    &mut output,
                    &mut names,
                    ids,
                    generation,
                    table_id,
                    statement.table_name.value_lower.as_str(),
                    None,
                    &expression.to_string(),
                )?;
            }
            TableConstraint::ForeignKey(_) => {}
        }
    }

    // Resolve FKs only after all local PK/UNIQUE declarations are present;
    // SQL declaration order cannot change whether a target is considered unique.
    for constraint in &statement.table_constraints {
        let TableConstraint::ForeignKey(foreign_key) = constraint else {
            continue;
        };
        let local = require_local_column(columns, foreign_key.column.value.as_str())?;
        let input = ForeignKeyInput {
            referenced_table: foreign_key.ref_table.value.to_string(),
            referenced_column: foreign_key
                .ref_column
                .as_ref()
                .map(|column| column.value.to_string()),
            on_delete: foreign_key.on_delete,
            on_update: foreign_key.on_update,
        };
        push_foreign_key(
            &mut output,
            &mut names,
            ids,
            generation,
            table_id,
            statement.table_name.value_lower.as_str(),
            columns,
            local,
            &input,
        )?;
    }

    for column in columns {
        if let Some(foreign_key) = &column.foreign_key {
            push_foreign_key(
                &mut output,
                &mut names,
                ids,
                generation,
                table_id,
                statement.table_name.value_lower.as_str(),
                columns,
                column,
                foreign_key,
            )?;
        }
    }

    validate_check_expressions(statement, columns, &output.primary_key_columns)?;
    Ok(output)
}

fn bind_primary_key(
    statement: &CreateTableStatement,
    columns: &[BoundColumn],
) -> Result<Vec<ObjectId>> {
    let column_keys = columns
        .iter()
        .filter(|column| column.primary_key)
        .collect::<Vec<_>>();
    let table_keys = statement
        .table_constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::PrimaryKey(columns) => Some(columns),
            _ => None,
        })
        .collect::<Vec<_>>();
    if column_keys.len() + table_keys.len() > 1 {
        return Err(Error::InvalidArgument(
            "table declares more than one PRIMARY KEY".to_owned(),
        ));
    }
    let resolved = if let Some(column) = column_keys.first() {
        vec![column.id]
    } else if let Some(names) = table_keys.first() {
        resolve_local_columns(columns, names)?
    } else {
        return Ok(Vec::new());
    };
    if resolved.len() != 1 {
        return Err(Error::NotSupported(
            "composite PRIMARY KEY is not supported by the current RadixDB SQL contract".to_owned(),
        ));
    }
    let column = columns
        .iter()
        .find(|column| column.id == resolved[0])
        .expect("resolved primary-key column exists");
    if !matches!(
        column.data_type.logical_type(),
        DataType::Integer | DataType::Uuid
    ) {
        return Err(Error::InvalidArgument(format!(
            "PRIMARY KEY column '{}' must be INTEGER or UUID",
            column.name.display().as_str()
        )));
    }
    Ok(resolved)
}

#[allow(clippy::too_many_arguments)]
fn push_unique(
    output: &mut BoundConstraints,
    names: &mut BTreeSet<String>,
    ids: &mut ObjectIdSource,
    generation: &CatalogGeneration,
    table_id: ObjectId,
    table_name: &str,
    column_ids: &[ObjectId],
    column_names: &[&str],
) -> Result<ObjectId> {
    let canonical_columns = column_names
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    push_constraint(
        output,
        names,
        ids,
        generation,
        table_id,
        format!("uq_{table_name}_{}", column_names.join("_")),
        "unique",
        table_name,
        &canonical_columns,
        &[],
        ConstraintPayload::unique(column_ids.to_vec()).map_err(catalog_argument)?,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
fn push_check(
    output: &mut BoundConstraints,
    names: &mut BTreeSet<String>,
    ids: &mut ObjectIdSource,
    generation: &CatalogGeneration,
    table_id: ObjectId,
    table_name: &str,
    local_column_id: Option<ObjectId>,
    check_sql: &str,
) -> Result<ObjectId> {
    let ordinal = output
        .objects
        .iter()
        .filter(|object| {
            matches!(
                object.payload(),
                CatalogPayload::Constraint(ConstraintPayload::Check { .. })
            )
        })
        .count()
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("CHECK ordinal overflow".to_owned()))?;
    let name_extra_fields = vec![ordinal.to_string()];
    push_constraint(
        output,
        names,
        ids,
        generation,
        table_id,
        format!("chk_{table_name}_{ordinal}"),
        "check",
        table_name,
        &[],
        &name_extra_fields,
        match local_column_id {
            Some(local_column_id) => ConstraintPayload::column_check(local_column_id, check_sql),
            None => ConstraintPayload::check(check_sql),
        }
        .map_err(catalog_argument)?,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
fn push_foreign_key(
    output: &mut BoundConstraints,
    names: &mut BTreeSet<String>,
    ids: &mut ObjectIdSource,
    generation: &CatalogGeneration,
    table_id: ObjectId,
    table_name: &str,
    local_columns: &[BoundColumn],
    local: &BoundColumn,
    input: &ForeignKeyInput,
) -> Result<ObjectId> {
    let target = resolve_foreign_key_target(
        generation,
        table_id,
        table_name,
        local_columns,
        output,
        input,
    )?;
    if local.data_type != target.column_type {
        return Err(Error::InvalidArgument(format!(
            "foreign key column '{}' has a different type from '{}.{}'",
            local.name.display().as_str(),
            input.referenced_table,
            target.column_name
        )));
    }
    if (matches!(input.on_delete, ForeignKeyAction::SetNull)
        || matches!(input.on_update, ForeignKeyAction::SetNull))
        && !local.is_nullable(&output.primary_key_columns)
    {
        return Err(Error::InvalidArgument(format!(
            "foreign key column '{}' has SET NULL action but is NOT NULL",
            local.name.display().as_str()
        )));
    }
    let payload = ConstraintPayload::foreign_key(
        vec![local.id],
        target.table_id,
        vec![target.column_id],
        ForeignKeyMatch::Simple,
        map_action(input.on_update),
        map_action(input.on_delete),
    )
    .map_err(catalog_argument)?;
    let name_columns = vec![local.name.normalized().as_str().to_owned()];
    let name_extra_fields = vec![
        input.referenced_table.to_lowercase(),
        target.column_name.to_lowercase(),
    ];
    push_constraint(
        output,
        names,
        ids,
        generation,
        table_id,
        format!(
            "fk_{table_name}_{}___{}",
            local.name.normalized().as_str(),
            input.referenced_table.to_lowercase()
        ),
        "foreign_key",
        table_name,
        &name_columns,
        &name_extra_fields,
        payload,
        &[target.table_id, target.column_id],
    )
}

#[derive(Debug)]
struct ForeignKeyTarget {
    table_id: ObjectId,
    column_id: ObjectId,
    column_name: String,
    column_type: CatalogDataType,
}

fn resolve_foreign_key_target(
    generation: &CatalogGeneration,
    table_id: ObjectId,
    table_name: &str,
    local_columns: &[BoundColumn],
    local_constraints: &BoundConstraints,
    input: &ForeignKeyInput,
) -> Result<ForeignKeyTarget> {
    let referenced_table_name =
        CatalogName::new(input.referenced_table.as_str()).map_err(catalog_argument)?;
    let current_table_name = CatalogName::new(table_name).map_err(catalog_argument)?;
    if referenced_table_name.normalized() == current_table_name.normalized() {
        let column = if let Some(name) = &input.referenced_column {
            require_local_column(local_columns, name)?
        } else {
            let [primary] = local_constraints.primary_key_columns.as_slice() else {
                return Err(Error::InvalidArgument(format!(
                    "table '{}' has no single primary key for FK reference default",
                    input.referenced_table
                )));
            };
            local_columns
                .iter()
                .find(|column| column.id == *primary)
                .expect("local primary-key column exists")
        };
        if !local_target_is_unique(local_constraints, column.id)
            && !catalog_index_is_full_unique(generation, table_id, column.id)
        {
            return Err(Error::InvalidArgument(format!(
                "foreign key target '{}.{}' is neither PRIMARY KEY nor UNIQUE",
                input.referenced_table,
                column.name.display().as_str()
            )));
        }
        return Ok(ForeignKeyTarget {
            table_id,
            column_id: column.id,
            column_name: column.name.display().as_str().to_owned(),
            column_type: column.data_type,
        });
    }

    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, &input.referenced_table)
        .map_err(catalog_argument)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(input.referenced_table.clone()))?;
    let CatalogPayload::Table(table_payload) = table.payload() else {
        return Err(Error::internal("referenced table payload changed kind"));
    };
    let column = if let Some(name) = &input.referenced_column {
        generation
            .find_column(table.id(), name)
            .map_err(catalog_argument)?
            .ok_or_else(|| Error::ColumnNotFound(name.clone()))?
    } else {
        let primary_id = table_payload.primary_key_constraint_id().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "table '{}' has no primary key for FK reference default",
                input.referenced_table
            ))
        })?;
        let primary = generation
            .object(primary_id)
            .ok_or_else(|| Error::internal("primary-key catalog object is missing"))?;
        let CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) =
            primary.payload()
        else {
            return Err(Error::internal("primary-key payload changed kind"));
        };
        let [column_id] = local_column_ids.as_slice() else {
            return Err(Error::InvalidArgument(format!(
                "table '{}' has no single primary key for FK reference default",
                input.referenced_table
            )));
        };
        generation
            .object(*column_id)
            .expect("validated primary-key column exists")
    };
    if !external_target_is_unique(generation, table_payload, column.id()) {
        return Err(Error::InvalidArgument(format!(
            "foreign key target '{}.{}' is neither PRIMARY KEY nor UNIQUE",
            input.referenced_table,
            column.name().display().as_str()
        )));
    }
    let CatalogPayload::Column(column_payload) = column.payload() else {
        return Err(Error::internal("referenced column payload changed kind"));
    };
    Ok(ForeignKeyTarget {
        table_id: table.id(),
        column_id: column.id(),
        column_name: column.name().display().as_str().to_owned(),
        column_type: column_payload.data_type(),
    })
}

fn local_target_is_unique(constraints: &BoundConstraints, column_id: ObjectId) -> bool {
    constraints
        .objects
        .iter()
        .any(|object| match object.payload() {
            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids })
            | CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => {
                local_column_ids.as_slice() == [column_id]
            }
            _ => false,
        })
}

fn external_target_is_unique(
    generation: &CatalogGeneration,
    table: &radixdb_catalog::TablePayload,
    column_id: ObjectId,
) -> bool {
    table.constraint_ids().iter().any(|id| {
        generation
            .object(*id)
            .is_some_and(|object| match object.payload() {
                CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids })
                | CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => {
                    local_column_ids.as_slice() == [column_id]
                }
                _ => false,
            })
    }) || table
        .index_ids()
        .iter()
        .any(|id| catalog_index_payload_is_full_unique(generation, *id, column_id))
}

fn catalog_index_is_full_unique(
    generation: &CatalogGeneration,
    table_id: ObjectId,
    column_id: ObjectId,
) -> bool {
    generation
        .object(table_id)
        .and_then(|object| match object.payload() {
            CatalogPayload::Table(table) => Some(table),
            _ => None,
        })
        .is_some_and(|table| {
            table
                .index_ids()
                .iter()
                .any(|id| catalog_index_payload_is_full_unique(generation, *id, column_id))
        })
}

fn catalog_index_payload_is_full_unique(
    generation: &CatalogGeneration,
    index_id: ObjectId,
    column_id: ObjectId,
) -> bool {
    generation
        .object(index_id)
        .is_some_and(|object| match object.payload() {
            CatalogPayload::Index(index) => {
                index.unique()
                    && index.predicate_sql().is_none()
                    && index.expression_sql().is_none()
                    && index.key_column_ids() == [column_id]
            }
            _ => false,
        })
}

#[allow(clippy::too_many_arguments)]
fn push_constraint(
    output: &mut BoundConstraints,
    names: &mut BTreeSet<String>,
    ids: &mut ObjectIdSource,
    generation: &CatalogGeneration,
    table_id: ObjectId,
    base_name: String,
    name_kind: &str,
    name_table: &str,
    name_columns: &[String],
    name_extra_fields: &[String],
    payload: ConstraintPayload,
    references: &[ObjectId],
) -> Result<ObjectId> {
    let id = ids.next(generation)?;
    let name = allocate_constraint_name(
        base_name,
        name_kind,
        name_table,
        name_columns,
        name_extra_fields,
        names,
    )?;
    let ordinal = u32::try_from(output.objects.len())
        .map_err(|_| Error::InvalidArgument("too many constraints on table".to_owned()))?;
    output.objects.push(
        CatalogObject::new(
            id,
            Some(ObjectId::BOOTSTRAP_NAMESPACE),
            Some(table_id),
            ObjectId::BOOTSTRAP_OWNER,
            name,
            1,
            CatalogPayload::Constraint(payload),
        )
        .map_err(catalog_argument)?,
    );
    output.ids.push(id);
    output
        .edges
        .push(CatalogEdge::new(table_id, id, EdgeKind::Contains, ordinal));
    for (ordinal, target) in references.iter().copied().enumerate() {
        output.edges.push(CatalogEdge::new(
            id,
            target,
            EdgeKind::References,
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many FK targets".to_owned()))?,
        ));
    }
    Ok(id)
}

fn allocate_constraint_name(
    base_name: String,
    kind: &str,
    table: &str,
    columns: &[String],
    extra_fields: &[String],
    names: &mut BTreeSet<String>,
) -> Result<CatalogName> {
    let base_conflicts = CatalogName::new(base_name.as_str())
        .ok()
        .is_some_and(|candidate| names.contains(candidate.normalized().as_str()));
    let allocated = generated_constraint_name(
        &base_name,
        kind,
        table,
        columns,
        extra_fields,
        base_conflicts,
    );
    let allocated = CatalogName::new(allocated).map_err(catalog_argument)?;
    if !names.insert(allocated.normalized().as_str().to_owned()) {
        return Err(Error::internal(
            "canonical generated constraint name collides with an existing constraint",
        ));
    }
    Ok(allocated)
}

fn resolve_local_columns(
    columns: &[BoundColumn],
    names: &[radixdb_sql::ast::Identifier],
) -> Result<Vec<ObjectId>> {
    if names.is_empty() {
        return Err(Error::InvalidArgument(
            "constraint must reference at least one column".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    names
        .iter()
        .map(|name| {
            let column = require_local_column(columns, name.value.as_str())?;
            if !seen.insert(column.id) {
                return Err(Error::InvalidArgument(format!(
                    "constraint references column '{}' more than once",
                    name.value
                )));
            }
            Ok(column.id)
        })
        .collect()
}

fn require_local_column<'a>(columns: &'a [BoundColumn], name: &str) -> Result<&'a BoundColumn> {
    let normalized = CatalogName::new(name).map_err(catalog_argument)?;
    columns
        .iter()
        .find(|column| column.name.normalized() == normalized.normalized())
        .ok_or_else(|| Error::ColumnNotFound(name.to_owned()))
}

fn validate_check_expressions(
    statement: &CreateTableStatement,
    columns: &[BoundColumn],
    primary_key_columns: &[ObjectId],
) -> Result<()> {
    let mut builder = SchemaBuilder::new(statement.table_name.value.as_str());
    for column in columns {
        builder = builder.add_with_constraints(
            column.name.display().as_str(),
            column.data_type.logical_type(),
            column.is_nullable(primary_key_columns),
            primary_key_columns.contains(&column.id),
            column.auto_increment,
            column.default_sql.clone(),
            None,
        );
        if column.data_type.logical_type() == DataType::Vector {
            builder = builder.set_last_vector_dimensions(column.data_type.parameter_1() as u16);
        }
        if column.data_type.logical_type() == DataType::Decimal {
            builder = builder.set_last_decimal_parameters(
                column.data_type.parameter_1() as u8,
                column.data_type.parameter_2() as u8,
            );
        }
    }
    for check in columns.iter().flat_map(|column| &column.checks) {
        builder = builder.add_table_check(check.clone());
    }
    for constraint in &statement.table_constraints {
        if let TableConstraint::Check(expression) = constraint {
            builder = builder.add_table_check(expression.to_string());
        }
    }
    compile_table_check_constraints(&builder.build())?;
    Ok(())
}

fn map_action(action: ForeignKeyAction) -> CatalogForeignKeyAction {
    match action {
        ForeignKeyAction::Restrict => CatalogForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade => CatalogForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull => CatalogForeignKeyAction::SetNull,
        ForeignKeyAction::NoAction => CatalogForeignKeyAction::NoAction,
    }
}

fn set_once(slot: &mut bool, column_name: &str, constraint: &str) -> Result<()> {
    if *slot {
        return Err(Error::InvalidArgument(format!(
            "column '{column_name}' declares {constraint} more than once"
        )));
    }
    *slot = true;
    Ok(())
}
