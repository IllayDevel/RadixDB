use std::fmt::Write;

use sha2::{Digest, Sha256};

use crate::*;

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("invalid SQL identifier: {0}")]
    InvalidIdentifier(String),
    #[error("invalid ORM operation: {0}")]
    InvalidOperation(String),
    #[error("unsupported ORM operation: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledStatement {
    pub sql: String,
    pub parameters: Vec<TypedValue>,
    pub expected_result_shape: Vec<ResultColumnDescriptor>,
    pub shape_fingerprint: String,
}

impl IrDocument {
    pub fn to_sql(&self) -> Result<CompiledStatement, RenderError> {
        self.validate()
            .map_err(|error| RenderError::InvalidOperation(error.to_string()))?;
        SqlRenderer::compile(&self.payload)
    }
}

pub struct SqlRenderer {
    sql: String,
    parameters: Vec<TypedValue>,
}

impl SqlRenderer {
    pub fn compile(operation: &Operation) -> Result<CompiledStatement, RenderError> {
        let mut renderer = Self {
            sql: String::new(),
            parameters: Vec::new(),
        };
        renderer.render_operation(operation)?;
        let expected_result_shape = match operation {
            Operation::Select { query } => query.expected_result_shape.clone(),
            _ => Vec::new(),
        };
        let shape_fingerprint =
            shape_fingerprint(&renderer.sql, &renderer.parameters, &expected_result_shape);
        Ok(CompiledStatement {
            sql: renderer.sql,
            parameters: renderer.parameters,
            expected_result_shape,
            shape_fingerprint,
        })
    }

    fn render_operation(&mut self, operation: &Operation) -> Result<(), RenderError> {
        match operation {
            Operation::Catalog { operation } => self.render_catalog(operation),
            Operation::Ddl { operation } => self.render_ddl(operation),
            Operation::Select { query } => self.render_select(query),
            Operation::Insert { statement } => self.render_insert(statement),
            Operation::Upsert { statement } => self.render_upsert(statement),
            Operation::Update { statement } => self.render_update(statement),
            Operation::Delete { statement } => self.render_delete(statement),
            Operation::Explain { statement } => {
                self.sql.push_str("EXPLAIN ");
                if statement.analyze {
                    self.sql.push_str("ANALYZE ");
                }
                match statement.operation.as_ref() {
                    Operation::Select { .. }
                    | Operation::Insert { .. }
                    | Operation::Upsert { .. }
                    | Operation::Update { .. }
                    | Operation::Delete { .. } => self.render_operation(&statement.operation),
                    _ => Err(RenderError::InvalidOperation(
                        "EXPLAIN accepts query or DML operations only".to_string(),
                    )),
                }
            }
            Operation::Transaction { statement } => self.render_transaction(statement),
        }
    }

    fn render_catalog(&mut self, operation: &CatalogOperation) -> Result<(), RenderError> {
        match operation {
            CatalogOperation::ListTables => self.sql.push_str("SHOW TABLES"),
            CatalogOperation::DescribeTable { table } => {
                self.sql.push_str("DESCRIBE TABLE ");
                self.identifier(table)?;
                self.sql.push_str(" FORMAT JSON");
            }
            CatalogOperation::DescribeDatabase => {
                self.sql.push_str("DESCRIBE DATABASE FORMAT JSON")
            }
            CatalogOperation::ShowIndexes { table } => {
                self.sql.push_str("SHOW INDEXES FROM ");
                self.identifier(table)?;
            }
        }
        Ok(())
    }

    fn render_select(&mut self, select: &Select) -> Result<(), RenderError> {
        if !select.ctes.is_empty() {
            self.sql.push_str("WITH ");
            if select.recursive {
                self.sql.push_str("RECURSIVE ");
            }
            for (index, cte) in select.ctes.iter().enumerate() {
                if index > 0 {
                    self.sql.push_str(", ");
                }
                self.identifier(&cte.name)?;
                if !cte.columns.is_empty() {
                    self.sql.push_str(" (");
                    self.identifier_list(&cte.columns)?;
                    self.sql.push(')');
                }
                self.sql.push_str(" AS (");
                self.render_select(&cte.query)?;
                self.sql.push(')');
            }
            self.sql.push(' ');
        }

        self.sql.push_str("SELECT ");
        if !select.distinct_on.is_empty() {
            self.sql.push_str("DISTINCT ON (");
            self.expression_list(&select.distinct_on)?;
            self.sql.push_str(") ");
        } else if select.distinct {
            self.sql.push_str("DISTINCT ");
        }
        if select.projection.is_empty() {
            return Err(RenderError::InvalidOperation(
                "SELECT requires at least one projection".to_string(),
            ));
        }
        self.projection_list(&select.projection)?;
        if let Some(from) = &select.from {
            self.sql.push_str(" FROM ");
            self.render_relation(from)?;
        }
        if let Some(filter) = &select.filter {
            self.sql.push_str(" WHERE ");
            self.render_expression(filter)?;
        }
        if let Some(grouping) = &select.group_by {
            self.sql.push_str(" GROUP BY ");
            self.render_grouping(grouping)?;
        }
        if let Some(having) = &select.having {
            self.sql.push_str(" HAVING ");
            self.render_expression(having)?;
        }
        if !select.windows.is_empty() {
            self.sql.push_str(" WINDOW ");
            for (index, window) in select.windows.iter().enumerate() {
                if index > 0 {
                    self.sql.push_str(", ");
                }
                self.identifier(&window.name)?;
                self.sql.push_str(" AS (");
                self.render_window_specification(&window.specification)?;
                self.sql.push(')');
            }
        }
        for arm in &select.set_operations {
            self.sql.push(' ');
            self.sql.push_str(match arm.operator {
                SetOperator::Union => "UNION",
                SetOperator::UnionAll => "UNION ALL",
                SetOperator::Intersect => "INTERSECT",
                SetOperator::Except => "EXCEPT",
            });
            self.sql.push(' ');
            self.render_select(&arm.query)?;
        }
        if !select.order_by.is_empty() {
            self.sql.push_str(" ORDER BY ");
            self.order_by_list(&select.order_by)?;
        }
        if let Some(limit) = select.limit {
            write!(&mut self.sql, " LIMIT {limit}").unwrap();
        }
        if let Some(offset) = select.offset {
            write!(&mut self.sql, " OFFSET {offset}").unwrap();
        }
        Ok(())
    }

    fn projection_list(&mut self, projections: &[Projection]) -> Result<(), RenderError> {
        for (index, projection) in projections.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.render_expression(&projection.expression)?;
            if let Some(alias) = &projection.alias {
                self.sql.push_str(" AS ");
                self.identifier(alias)?;
            }
        }
        Ok(())
    }

    fn render_relation(&mut self, relation: &Relation) -> Result<(), RenderError> {
        match relation {
            Relation::Table { name, alias } | Relation::Cte { name, alias } => {
                self.qualified_identifier(name)?;
                if let Some(alias) = alias {
                    self.sql.push_str(" AS ");
                    self.identifier(alias)?;
                }
            }
            Relation::Derived { query, alias } => {
                self.sql.push('(');
                self.render_select(query)?;
                self.sql.push_str(") AS ");
                self.identifier(alias)?;
            }
            Relation::Values {
                rows,
                alias,
                columns,
            } => {
                if rows.is_empty() {
                    return Err(RenderError::InvalidOperation(
                        "VALUES relation requires at least one row".to_string(),
                    ));
                }
                self.sql.push_str("(VALUES ");
                self.expression_rows(rows)?;
                self.sql.push_str(") AS ");
                self.identifier(alias)?;
                if !columns.is_empty() {
                    self.sql.push_str(" (");
                    self.identifier_list(columns)?;
                    self.sql.push(')');
                }
            }
            Relation::Join {
                left,
                right,
                kind,
                on,
            } => {
                self.render_relation(left)?;
                self.sql.push(' ');
                self.sql.push_str(match kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Left => "LEFT JOIN",
                    JoinKind::Right => "RIGHT JOIN",
                    JoinKind::Full => "FULL JOIN",
                    JoinKind::Cross => "CROSS JOIN",
                });
                self.sql.push(' ');
                self.render_relation(right)?;
                if *kind == JoinKind::Cross {
                    if on.is_some() {
                        return Err(RenderError::InvalidOperation(
                            "CROSS JOIN cannot have an ON predicate".to_string(),
                        ));
                    }
                } else {
                    let on = on.as_ref().ok_or_else(|| {
                        RenderError::InvalidOperation(
                            "non-CROSS JOIN requires an ON predicate".to_string(),
                        )
                    })?;
                    self.sql.push_str(" ON ");
                    self.render_expression(on)?;
                }
            }
        }
        Ok(())
    }

    fn render_expression(&mut self, expression: &Expression) -> Result<(), RenderError> {
        match expression {
            Expression::Column { column } => {
                if let Some(relation) = &column.relation {
                    self.identifier(relation)?;
                    self.sql.push('.');
                }
                self.identifier(&column.name)?;
            }
            Expression::Literal { value } => self.parameter(value.clone()),
            Expression::Star { relation } => {
                if let Some(relation) = relation {
                    self.identifier(relation)?;
                    self.sql.push('.');
                }
                self.sql.push('*');
            }
            Expression::Unary {
                operator,
                expression,
            } => {
                self.sql.push_str(match operator {
                    UnaryOperator::Not => "NOT ",
                    UnaryOperator::Negate => "-",
                    UnaryOperator::Positive => "+",
                });
                self.sql.push('(');
                self.render_expression(expression)?;
                self.sql.push(')');
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                self.sql.push('(');
                self.render_expression(left)?;
                self.sql.push(' ');
                self.sql.push_str(match operator {
                    BinaryOperator::Eq => "=",
                    BinaryOperator::Ne => "<>",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Lte => "<=",
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Gte => ">=",
                    BinaryOperator::And => "AND",
                    BinaryOperator::Or => "OR",
                    BinaryOperator::Xor => "XOR",
                    BinaryOperator::Add => "+",
                    BinaryOperator::Subtract => "-",
                    BinaryOperator::Multiply => "*",
                    BinaryOperator::Divide => "/",
                    BinaryOperator::Modulo => "%",
                    BinaryOperator::Like => "LIKE",
                    BinaryOperator::NotLike => "NOT LIKE",
                    BinaryOperator::Glob => "GLOB",
                    BinaryOperator::Regexp => "REGEXP",
                    BinaryOperator::IsDistinctFrom => "IS DISTINCT FROM",
                    BinaryOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                });
                self.sql.push(' ');
                self.render_expression(right)?;
                self.sql.push(')');
            }
            Expression::Function { name, arguments } => {
                self.function_name(name)?;
                self.sql.push('(');
                self.expression_list(arguments)?;
                self.sql.push(')');
            }
            Expression::Aggregate {
                name,
                arguments,
                distinct,
                filter,
                order_by,
            } => {
                self.function_name(name)?;
                self.sql.push('(');
                if *distinct {
                    self.sql.push_str("DISTINCT ");
                }
                self.expression_list(arguments)?;
                if !order_by.is_empty() {
                    self.sql.push_str(" ORDER BY ");
                    self.order_by_list(order_by)?;
                }
                self.sql.push(')');
                if let Some(filter) = filter {
                    self.sql.push_str(" FILTER (WHERE ");
                    self.render_expression(filter)?;
                    self.sql.push(')');
                }
            }
            Expression::Window {
                function,
                specification,
            } => {
                self.render_expression(function)?;
                self.sql.push_str(" OVER ");
                if let Some(name) = &specification.name {
                    if specification.partition_by.is_empty()
                        && specification.order_by.is_empty()
                        && specification.frame.is_none()
                    {
                        self.identifier(name)?;
                        return Ok(());
                    }
                }
                self.sql.push('(');
                self.render_window_specification(specification)?;
                self.sql.push(')');
            }
            Expression::Cast {
                expression,
                data_type,
            } => {
                self.sql.push_str("CAST(");
                self.render_expression(expression)?;
                self.sql.push_str(" AS ");
                self.render_data_type(data_type)?;
                self.sql.push(')');
            }
            Expression::Case {
                operand,
                branches,
                otherwise,
            } => {
                self.sql.push_str("CASE");
                if let Some(operand) = operand {
                    self.sql.push(' ');
                    self.render_expression(operand)?;
                }
                for branch in branches {
                    self.sql.push_str(" WHEN ");
                    self.render_expression(&branch.when)?;
                    self.sql.push_str(" THEN ");
                    self.render_expression(&branch.then)?;
                }
                if let Some(otherwise) = otherwise {
                    self.sql.push_str(" ELSE ");
                    self.render_expression(otherwise)?;
                }
                self.sql.push_str(" END");
            }
            Expression::IsNull {
                expression,
                negated,
            } => {
                self.sql.push('(');
                self.render_expression(expression)?;
                self.sql.push_str(if *negated {
                    " IS NOT NULL)"
                } else {
                    " IS NULL)"
                });
            }
            Expression::Between {
                expression,
                lower,
                upper,
                negated,
            } => {
                self.sql.push('(');
                self.render_expression(expression)?;
                self.sql.push_str(if *negated {
                    " NOT BETWEEN "
                } else {
                    " BETWEEN "
                });
                self.render_expression(lower)?;
                self.sql.push_str(" AND ");
                self.render_expression(upper)?;
                self.sql.push(')');
            }
            Expression::InList {
                expression,
                values,
                negated,
            } => {
                if values.is_empty() {
                    return Err(RenderError::InvalidOperation(
                        "IN list cannot be empty".to_string(),
                    ));
                }
                self.sql.push('(');
                self.render_expression(expression)?;
                self.sql
                    .push_str(if *negated { " NOT IN (" } else { " IN (" });
                self.expression_list(values)?;
                self.sql.push_str("))");
            }
            Expression::InSubquery {
                expression,
                query,
                negated,
            } => {
                self.sql.push('(');
                self.render_expression(expression)?;
                self.sql
                    .push_str(if *negated { " NOT IN (" } else { " IN (" });
                self.render_select(query)?;
                self.sql.push_str("))");
            }
            Expression::Exists { query, negated } => {
                if *negated {
                    self.sql.push_str("NOT ");
                }
                self.sql.push_str("EXISTS (");
                self.render_select(query)?;
                self.sql.push(')');
            }
            Expression::ScalarSubquery { query } => {
                self.sql.push('(');
                self.render_select(query)?;
                self.sql.push(')');
            }
            Expression::Tuple { values } => {
                self.sql.push('(');
                self.expression_list(values)?;
                self.sql.push(')');
            }
            Expression::Navigation { root, path } => {
                if path.is_empty() {
                    return Err(RenderError::InvalidOperation(
                        "navigation path requires at least one segment".to_string(),
                    ));
                }
                self.identifier(root)?;
                for segment in path {
                    self.sql.push('.');
                    self.identifier(segment)?;
                }
            }
            Expression::Grouping { expressions } => {
                self.sql.push_str("GROUPING(");
                self.expression_list(expressions)?;
                self.sql.push(')');
            }
        }
        Ok(())
    }

    fn render_grouping(&mut self, grouping: &Grouping) -> Result<(), RenderError> {
        match grouping {
            Grouping::Expressions { expressions } => self.expression_list(expressions),
            Grouping::Rollup { expressions } => {
                self.sql.push_str("ROLLUP (");
                self.expression_list(expressions)?;
                self.sql.push(')');
                Ok(())
            }
            Grouping::Cube { expressions } => {
                self.sql.push_str("CUBE (");
                self.expression_list(expressions)?;
                self.sql.push(')');
                Ok(())
            }
            Grouping::Sets { sets } => {
                self.sql.push_str("GROUPING SETS (");
                for (index, set) in sets.iter().enumerate() {
                    if index > 0 {
                        self.sql.push_str(", ");
                    }
                    self.sql.push('(');
                    self.expression_list(set)?;
                    self.sql.push(')');
                }
                self.sql.push(')');
                Ok(())
            }
        }
    }

    fn render_window_specification(
        &mut self,
        specification: &WindowSpecification,
    ) -> Result<(), RenderError> {
        let mut wrote = false;
        if let Some(name) = &specification.name {
            self.identifier(name)?;
            wrote = true;
        }
        if !specification.partition_by.is_empty() {
            if wrote {
                self.sql.push(' ');
            }
            self.sql.push_str("PARTITION BY ");
            self.expression_list(&specification.partition_by)?;
            wrote = true;
        }
        if !specification.order_by.is_empty() {
            if wrote {
                self.sql.push(' ');
            }
            self.sql.push_str("ORDER BY ");
            self.order_by_list(&specification.order_by)?;
            wrote = true;
        }
        if let Some(frame) = &specification.frame {
            if wrote {
                self.sql.push(' ');
            }
            self.sql.push_str(match frame.unit {
                WindowFrameUnit::Rows => "ROWS ",
                WindowFrameUnit::Range => "RANGE ",
            });
            if let Some(end) = &frame.end {
                self.sql.push_str("BETWEEN ");
                self.render_frame_bound(&frame.start);
                self.sql.push_str(" AND ");
                self.render_frame_bound(end);
            } else {
                self.render_frame_bound(&frame.start);
            }
        }
        Ok(())
    }

    fn render_frame_bound(&mut self, bound: &WindowFrameBound) {
        match bound {
            WindowFrameBound::UnboundedPreceding => self.sql.push_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Preceding(offset) => {
                write!(&mut self.sql, "{offset} PRECEDING").unwrap()
            }
            WindowFrameBound::CurrentRow => self.sql.push_str("CURRENT ROW"),
            WindowFrameBound::Following(offset) => {
                write!(&mut self.sql, "{offset} FOLLOWING").unwrap()
            }
            WindowFrameBound::UnboundedFollowing => self.sql.push_str("UNBOUNDED FOLLOWING"),
        }
    }

    fn order_by_list(&mut self, order: &[OrderBy]) -> Result<(), RenderError> {
        for (index, item) in order.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.render_expression(&item.expression)?;
            self.sql.push_str(match item.direction {
                SortDirection::Asc => " ASC",
                SortDirection::Desc => " DESC",
            });
            if let Some(nulls) = item.nulls {
                self.sql.push_str(match nulls {
                    NullPlacement::First => " NULLS FIRST",
                    NullPlacement::Last => " NULLS LAST",
                });
            }
        }
        Ok(())
    }

    fn render_insert(&mut self, insert: &Insert) -> Result<(), RenderError> {
        if insert.columns.is_empty() {
            return Err(RenderError::InvalidOperation(
                "INSERT requires explicit columns".to_string(),
            ));
        }
        if insert.rows.is_empty() == insert.source.is_none() {
            return Err(RenderError::InvalidOperation(
                "INSERT requires exactly one of VALUES rows or SELECT source".to_string(),
            ));
        }
        self.sql.push_str("INSERT INTO ");
        self.qualified_identifier(&insert.table)?;
        self.sql.push_str(" (");
        self.identifier_list(&insert.columns)?;
        self.sql.push_str(") ");
        if !insert.rows.is_empty() {
            if insert
                .rows
                .iter()
                .any(|row| row.len() != insert.columns.len())
            {
                return Err(RenderError::InvalidOperation(
                    "INSERT row width differs from column count".to_string(),
                ));
            }
            self.sql.push_str("VALUES ");
            self.expression_rows(&insert.rows)?;
        } else if let Some(source) = &insert.source {
            self.render_select(source)?;
        }
        self.render_returning(&insert.returning)
    }

    fn render_upsert(&mut self, upsert: &Upsert) -> Result<(), RenderError> {
        self.render_insert(&Insert {
            returning: Vec::new(),
            ..upsert.insert.clone()
        })?;
        if upsert.conflict_columns.is_empty() {
            return Err(RenderError::InvalidOperation(
                "UPSERT requires explicit conflict columns".to_string(),
            ));
        }
        self.sql.push_str(" ON CONFLICT (");
        self.identifier_list(&upsert.conflict_columns)?;
        if upsert.assignments.is_empty() {
            self.sql.push_str(") DO NOTHING");
        } else {
            self.sql.push_str(") DO UPDATE SET ");
            self.assignment_list(&upsert.assignments)?;
        }
        self.render_returning(&upsert.insert.returning)
    }

    fn render_update(&mut self, update: &Update) -> Result<(), RenderError> {
        if update.assignments.is_empty() {
            return Err(RenderError::InvalidOperation(
                "UPDATE requires at least one assignment".to_string(),
            ));
        }
        if update.alias.is_some() || update.from.is_some() {
            return Err(RenderError::Unsupported(
                "UPDATE aliases and UPDATE ... FROM are not part of the current RadixDB SQL grammar"
                    .to_string(),
            ));
        }
        self.sql.push_str("UPDATE ");
        self.qualified_identifier(&update.table)?;
        self.sql.push_str(" SET ");
        self.assignment_list(&update.assignments)?;
        if let Some(filter) = &update.filter {
            self.sql.push_str(" WHERE ");
            self.render_expression(filter)?;
        }
        self.render_returning(&update.returning)
    }

    fn render_delete(&mut self, delete: &Delete) -> Result<(), RenderError> {
        if delete.filter.is_none() && !delete.all_rows {
            return Err(RenderError::InvalidOperation(
                "unguarded DELETE requires explicit all_rows".to_string(),
            ));
        }
        if delete.using.is_some() {
            return Err(RenderError::Unsupported(
                "DELETE ... USING is not part of the current RadixDB SQL grammar".to_string(),
            ));
        }
        self.sql.push_str("DELETE FROM ");
        self.qualified_identifier(&delete.table)?;
        if let Some(alias) = &delete.alias {
            self.sql.push_str(" AS ");
            self.identifier(alias)?;
        }
        if let Some(filter) = &delete.filter {
            self.sql.push_str(" WHERE ");
            self.render_expression(filter)?;
        }
        self.render_returning(&delete.returning)
    }

    fn assignment_list(&mut self, assignments: &[Assignment]) -> Result<(), RenderError> {
        for (index, assignment) in assignments.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.identifier(&assignment.column)?;
            self.sql.push_str(" = ");
            self.render_expression(&assignment.value)?;
        }
        Ok(())
    }

    fn render_returning(&mut self, returning: &[Projection]) -> Result<(), RenderError> {
        if !returning.is_empty() {
            self.sql.push_str(" RETURNING ");
            self.projection_list(returning)?;
        }
        Ok(())
    }

    fn render_ddl(&mut self, operation: &DdlOperation) -> Result<(), RenderError> {
        match operation {
            DdlOperation::CreateTable {
                table,
                if_not_exists,
                columns,
                constraints,
            } => {
                if columns.is_empty() {
                    return Err(RenderError::InvalidOperation(
                        "CREATE TABLE requires columns".to_string(),
                    ));
                }
                self.sql.push_str("CREATE TABLE ");
                if *if_not_exists {
                    self.sql.push_str("IF NOT EXISTS ");
                }
                self.qualified_identifier(table)?;
                self.sql.push_str(" (");
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        self.sql.push_str(", ");
                    }
                    self.render_column_definition(column)?;
                }
                for constraint in constraints {
                    self.sql.push_str(", ");
                    self.render_constraint(constraint)?;
                }
                self.sql.push(')');
            }
            DdlOperation::CreateTableAs {
                table,
                if_not_exists,
                query,
            } => {
                self.sql.push_str("CREATE TABLE ");
                if *if_not_exists {
                    self.sql.push_str("IF NOT EXISTS ");
                }
                self.qualified_identifier(table)?;
                self.sql.push_str(" AS ");
                self.render_select(query)?;
            }
            DdlOperation::AlterTable { table, action } => {
                self.sql.push_str("ALTER TABLE ");
                self.qualified_identifier(table)?;
                self.sql.push(' ');
                self.render_alter_action(action)?;
            }
            DdlOperation::DropTable { table, if_exists } => {
                self.sql.push_str("DROP TABLE ");
                if *if_exists {
                    self.sql.push_str("IF EXISTS ");
                }
                self.qualified_identifier(table)?;
            }
            DdlOperation::TruncateTable { table } => {
                self.sql.push_str("TRUNCATE TABLE ");
                self.qualified_identifier(table)?;
            }
            DdlOperation::CreateIndex { index } => self.render_create_index(index)?,
            DdlOperation::DropIndex {
                table,
                index,
                if_exists,
            } => {
                self.sql.push_str("DROP INDEX ");
                if *if_exists {
                    self.sql.push_str("IF EXISTS ");
                }
                self.identifier(index)?;
                self.sql.push_str(" ON ");
                self.qualified_identifier(table)?;
            }
            DdlOperation::AlterIndex { index, new_name } => {
                self.sql.push_str("ALTER INDEX ");
                self.identifier(index)?;
                self.sql.push_str(" RENAME TO ");
                self.identifier(new_name)?;
            }
        }
        Ok(())
    }

    fn render_column_definition(&mut self, column: &ColumnDefinition) -> Result<(), RenderError> {
        self.identifier(&column.name)?;
        self.sql.push(' ');
        self.render_data_type(&column.data_type)?;
        if column.primary_key {
            self.sql.push_str(" PRIMARY KEY");
        }
        if !column.nullable && !column.primary_key {
            self.sql.push_str(" NOT NULL");
        }
        if column.unique && !column.primary_key {
            self.sql.push_str(" UNIQUE");
        }
        if column.auto_increment {
            self.sql.push_str(" AUTO_INCREMENT");
        }
        if let Some(default) = &column.default {
            self.sql.push_str(" DEFAULT ");
            self.render_expression(default)?;
        }
        if let Some(check) = &column.check {
            self.sql.push_str(" CHECK (");
            self.render_expression(check)?;
            self.sql.push(')');
        }
        if let Some(reference) = &column.reference {
            self.sql.push_str(" REFERENCES ");
            self.qualified_identifier(&reference.table)?;
            self.sql.push_str(" (");
            self.identifier(&reference.column)?;
            self.sql.push(')');
            self.render_fk_actions(reference.on_delete, reference.on_update);
        }
        Ok(())
    }

    fn render_constraint(
        &mut self,
        constraint: &ConstraintDefinitionIr,
    ) -> Result<(), RenderError> {
        match constraint {
            ConstraintDefinitionIr::PrimaryKey { columns } => {
                self.sql.push_str("PRIMARY KEY (");
                self.identifier_list(columns)?;
                self.sql.push(')');
            }
            ConstraintDefinitionIr::Unique { columns } => {
                self.sql.push_str("UNIQUE (");
                self.identifier_list(columns)?;
                self.sql.push(')');
            }
            ConstraintDefinitionIr::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => {
                self.sql.push_str("FOREIGN KEY (");
                self.identifier_list(columns)?;
                self.sql.push_str(") REFERENCES ");
                self.qualified_identifier(referenced_table)?;
                self.sql.push_str(" (");
                self.identifier_list(referenced_columns)?;
                self.sql.push(')');
                self.render_fk_actions(*on_delete, *on_update);
            }
            ConstraintDefinitionIr::Check { expression } => {
                self.sql.push_str("CHECK (");
                self.render_expression(expression)?;
                self.sql.push(')');
            }
        }
        Ok(())
    }

    fn render_fk_actions(
        &mut self,
        on_delete: ForeignKeyActionDescriptor,
        on_update: ForeignKeyActionDescriptor,
    ) {
        self.sql.push_str(" ON DELETE ");
        self.sql.push_str(fk_action(on_delete));
        self.sql.push_str(" ON UPDATE ");
        self.sql.push_str(fk_action(on_update));
    }

    fn render_alter_action(&mut self, action: &AlterTableAction) -> Result<(), RenderError> {
        match action {
            AlterTableAction::AddColumn { column } => {
                self.sql.push_str("ADD COLUMN ");
                self.render_column_definition(column)?;
            }
            AlterTableAction::ModifyColumn { column } => {
                self.sql.push_str("MODIFY COLUMN ");
                self.render_column_definition(column)?;
            }
            AlterTableAction::DropColumn { column } => {
                self.sql.push_str("DROP COLUMN ");
                self.identifier(column)?;
            }
            AlterTableAction::RenameColumn { from, to } => {
                self.sql.push_str("RENAME COLUMN ");
                self.identifier(from)?;
                self.sql.push_str(" TO ");
                self.identifier(to)?;
            }
            AlterTableAction::RenameTable { to } => {
                self.sql.push_str("RENAME TO ");
                self.qualified_identifier(to)?;
            }
            AlterTableAction::AddConstraint { constraint } => {
                self.sql.push_str("ADD CONSTRAINT ");
                self.render_constraint(constraint)?;
            }
            AlterTableAction::DropConstraint { name, if_exists } => {
                self.sql.push_str("DROP CONSTRAINT ");
                if *if_exists {
                    self.sql.push_str("IF EXISTS ");
                }
                self.identifier(name)?;
            }
        }
        Ok(())
    }

    fn render_create_index(&mut self, index: &IndexDefinition) -> Result<(), RenderError> {
        self.sql.push_str("CREATE ");
        if index.unique {
            self.sql.push_str("UNIQUE ");
        }
        self.sql.push_str("INDEX ");
        if index.if_not_exists {
            self.sql.push_str("IF NOT EXISTS ");
        }
        self.identifier(&index.name)?;
        self.sql.push_str(" ON ");
        self.qualified_identifier(&index.table)?;
        self.sql.push_str(" (");
        self.identifier_list(&index.columns)?;
        self.sql.push(')');
        if let Some(method) = &index.method {
            self.sql.push_str(" USING ");
            self.identifier(method)?;
        }
        if !index.options.is_empty() {
            self.sql.push_str(" WITH (");
            for (position, (name, value)) in index.options.iter().enumerate() {
                if position > 0 {
                    self.sql.push_str(", ");
                }
                self.identifier(name)?;
                self.sql.push_str(" = ");
                self.parameter(value.clone());
            }
            self.sql.push(')');
        }
        if let Some(predicate) = &index.predicate {
            self.sql.push_str(" WHERE ");
            self.render_expression(predicate)?;
        }
        Ok(())
    }

    fn render_transaction(&mut self, operation: &TransactionOperation) -> Result<(), RenderError> {
        match operation {
            TransactionOperation::Begin => self.sql.push_str("BEGIN"),
            TransactionOperation::Commit => self.sql.push_str("COMMIT"),
            TransactionOperation::Rollback => self.sql.push_str("ROLLBACK"),
            TransactionOperation::Savepoint { name } => {
                self.sql.push_str("SAVEPOINT ");
                self.identifier(name)?;
            }
            TransactionOperation::RollbackToSavepoint { name } => {
                self.sql.push_str("ROLLBACK TO SAVEPOINT ");
                self.identifier(name)?;
            }
            TransactionOperation::ReleaseSavepoint { name } => {
                self.sql.push_str("RELEASE SAVEPOINT ");
                self.identifier(name)?;
            }
        }
        Ok(())
    }

    fn render_data_type(&mut self, data_type: &DataTypeDescriptor) -> Result<(), RenderError> {
        match data_type {
            DataTypeDescriptor::Null => {
                return Err(RenderError::InvalidOperation(
                    "NULL is not a schema column type".to_string(),
                ))
            }
            DataTypeDescriptor::Integer => self.sql.push_str("INTEGER"),
            DataTypeDescriptor::Float => self.sql.push_str("FLOAT"),
            DataTypeDescriptor::Text => self.sql.push_str("TEXT"),
            DataTypeDescriptor::Boolean => self.sql.push_str("BOOLEAN"),
            DataTypeDescriptor::Timestamp => self.sql.push_str("TIMESTAMP"),
            DataTypeDescriptor::Date => self.sql.push_str("DATE"),
            DataTypeDescriptor::Json => self.sql.push_str("JSON"),
            DataTypeDescriptor::Uuid => self.sql.push_str("UUID"),
            DataTypeDescriptor::Bytes => self.sql.push_str("BYTES"),
            DataTypeDescriptor::Decimal { precision, scale } => match (precision, scale) {
                (None, None) => self.sql.push_str("DECIMAL"),
                (Some(precision), scale) => {
                    let scale = scale.unwrap_or(0);
                    if *precision == 0 || *precision > 38 || scale > *precision {
                        return Err(RenderError::InvalidOperation(format!(
                            "invalid DECIMAL({precision},{scale})"
                        )));
                    }
                    write!(&mut self.sql, "DECIMAL({precision},{scale})").unwrap();
                }
                (None, Some(_)) => {
                    return Err(RenderError::InvalidOperation(
                        "DECIMAL scale requires precision".to_string(),
                    ))
                }
            },
            DataTypeDescriptor::Vector { dimensions } => {
                if *dimensions == 0 {
                    return Err(RenderError::InvalidOperation(
                        "VECTOR dimensions must be positive".to_string(),
                    ));
                }
                write!(&mut self.sql, "VECTOR({dimensions})").unwrap();
            }
        }
        Ok(())
    }

    fn expression_list(&mut self, expressions: &[Expression]) -> Result<(), RenderError> {
        for (index, expression) in expressions.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.render_expression(expression)?;
        }
        Ok(())
    }

    fn expression_rows(&mut self, rows: &[Vec<Expression>]) -> Result<(), RenderError> {
        for (index, row) in rows.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.sql.push('(');
            self.expression_list(row)?;
            self.sql.push(')');
        }
        Ok(())
    }

    fn identifier_list(&mut self, identifiers: &[String]) -> Result<(), RenderError> {
        if identifiers.is_empty() {
            return Err(RenderError::InvalidOperation(
                "identifier list cannot be empty".to_string(),
            ));
        }
        for (index, identifier) in identifiers.iter().enumerate() {
            if index > 0 {
                self.sql.push_str(", ");
            }
            self.identifier(identifier)?;
        }
        Ok(())
    }

    fn identifier(&mut self, identifier: &str) -> Result<(), RenderError> {
        if identifier.is_empty() || identifier.contains(['\0', '.']) {
            return Err(RenderError::InvalidIdentifier(identifier.to_string()));
        }
        self.sql.push('"');
        self.sql.push_str(&identifier.replace('"', "\"\""));
        self.sql.push('"');
        Ok(())
    }

    fn qualified_identifier(&mut self, identifier: &str) -> Result<(), RenderError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
            return Err(RenderError::InvalidIdentifier(identifier.to_string()));
        }
        for (index, part) in parts.iter().enumerate() {
            if index > 0 {
                self.sql.push('.');
            }
            self.identifier(part)?;
        }
        Ok(())
    }

    fn function_name(&mut self, name: &str) -> Result<(), RenderError> {
        self.qualified_identifier(name)
    }

    fn parameter(&mut self, value: TypedValue) {
        self.parameters.push(value);
        write!(&mut self.sql, "${}", self.parameters.len()).unwrap();
    }
}

fn fk_action(action: ForeignKeyActionDescriptor) -> &'static str {
    match action {
        ForeignKeyActionDescriptor::Restrict => "RESTRICT",
        ForeignKeyActionDescriptor::Cascade => "CASCADE",
        ForeignKeyActionDescriptor::SetNull => "SET NULL",
        ForeignKeyActionDescriptor::NoAction => "NO ACTION",
    }
}

fn shape_fingerprint(
    sql: &str,
    parameters: &[TypedValue],
    result: &[ResultColumnDescriptor],
) -> String {
    let parameter_types = parameters
        .iter()
        .map(TypedValue::data_type)
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&(sql, parameter_types, result))
        .expect("shape fingerprint inputs are serializable");
    let digest = Sha256::digest(bytes);
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut fingerprint, "{byte:02x}").unwrap();
    }
    fingerprint
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(relation: &str, name: &str) -> Expression {
        Expression::Column {
            column: ColumnRef::qualified(relation, name),
        }
    }

    #[test]
    fn complex_select_renders_navigation_grouping_window_and_ordered_parameters() {
        let query = Select {
            projection: vec![
                Projection {
                    expression: Expression::Navigation {
                        root: "p".to_string(),
                        path: vec!["fio".to_string(), "name".to_string()],
                    },
                    alias: Some("fio_name".to_string()),
                },
                Projection {
                    expression: Expression::Window {
                        function: Box::new(Expression::Function {
                            name: "row_number".to_string(),
                            arguments: Vec::new(),
                        }),
                        specification: WindowSpecification {
                            partition_by: vec![col("p", "department_id")],
                            order_by: vec![OrderBy {
                                expression: col("p", "name"),
                                direction: SortDirection::Asc,
                                nulls: Some(NullPlacement::Last),
                            }],
                            ..WindowSpecification::default()
                        },
                    },
                    alias: Some("rn".to_string()),
                },
            ],
            from: Some(Relation::Table {
                name: "people".to_string(),
                alias: Some("p".to_string()),
            }),
            filter: Some(Expression::Binary {
                left: Box::new(col("p", "name")),
                operator: BinaryOperator::Like,
                right: Box::new(Expression::literal(TypedValue::Text("Ivan%".to_string()))),
            }),
            group_by: Some(Grouping::Cube {
                expressions: vec![col("p", "department_id")],
            }),
            order_by: vec![OrderBy {
                expression: col("p", "name"),
                direction: SortDirection::Desc,
                nulls: Some(NullPlacement::First),
            }],
            limit: Some(10),
            ..Select::default()
        };
        let compiled = IrDocument::new(Operation::Select { query })
            .to_sql()
            .unwrap();
        assert_eq!(compiled.parameters, vec![TypedValue::Text("Ivan%".into())]);
        assert!(compiled.sql.contains("\"p\".\"fio\".\"name\""));
        assert!(compiled.sql.contains("GROUP BY CUBE"));
        assert!(compiled.sql.contains("$1"));
        assert_eq!(compiled.shape_fingerprint.len(), 64);
    }

    #[test]
    fn renderer_never_interpolates_values_and_rejects_unsafe_mutations() {
        let insert = Insert {
            table: "people".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec![Expression::literal(TypedValue::Text(
                "x'); DROP TABLE people; --".to_string(),
            ))]],
            source: None,
            returning: vec![Projection {
                expression: Expression::Star { relation: None },
                alias: None,
            }],
        };
        let compiled = SqlRenderer::compile(&Operation::Insert { statement: insert }).unwrap();
        assert!(!compiled.sql.contains("DROP TABLE"));
        assert!(compiled.sql.contains("$1"));

        let delete = Delete {
            table: "people".to_string(),
            alias: None,
            using: None,
            filter: None,
            all_rows: false,
            returning: Vec::new(),
        };
        assert!(SqlRenderer::compile(&Operation::Delete { statement: delete }).is_err());
    }
}
