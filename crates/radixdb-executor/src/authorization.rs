//! Fail-closed executor authorization over the transaction-visible catalog.
//!
//! Transport authentication selects only a stable Principal ID. Every SQL
//! entrypoint reaches this module again under the statement catalog fence, so
//! cached/prepared/procedural callers cannot assert privileges themselves.

use std::collections::BTreeSet;

use radixdb_catalog::{
    CatalogGeneration, CatalogObject, CatalogPayload, ObjectId, ObjectKind, PRIVILEGE_CONNECT,
    PRIVILEGE_CREATE, PRIVILEGE_DELETE, PRIVILEGE_EXECUTE, PRIVILEGE_INSERT, PRIVILEGE_SELECT,
    PRIVILEGE_UPDATE,
};
use radixdb_core::{Error, Result};
use radixdb_sql::{
    walk_statement_physical_table_sources, walk_statement_tree, DescribeTarget, Expression,
    Identifier, SelectStatement, SimpleTableSource, Statement,
};

use crate::application::{AUDIT_RELATION_NAME, OUTBOX_RELATION_NAME};
use crate::catalog::security::{
    has_object_privilege, require_column_privilege, require_namespace_usage,
    require_object_privilege, require_owner, require_principal, resolve_relation,
    resolve_unqualified_relation, resolve_unqualified_table,
};
use crate::context::ExecutionContext;
use crate::procedural::transaction_visible_catalog;
use crate::Executor;

pub(crate) fn authorize_routine_invocation(
    executor: &Executor,
    session_principal: ObjectId,
    effective_principal: ObjectId,
    routine: ObjectId,
) -> Result<()> {
    if session_principal == ObjectId::BOOTSTRAP_OWNER
        && effective_principal == ObjectId::BOOTSTRAP_OWNER
    {
        return Ok(());
    }
    let (catalog, _) = transaction_visible_catalog(executor)?;
    require_principal(catalog.as_ref(), session_principal)?;
    require_principal(catalog.as_ref(), effective_principal)?;
    require_object_privilege(
        catalog.as_ref(),
        session_principal,
        ObjectId::BOOTSTRAP_NAMESPACE,
        PRIVILEGE_CONNECT,
        "CONNECT",
    )?;
    let routine = catalog.object(routine).ok_or_else(|| {
        Error::invalid_argument(format!("routine catalog object {routine} does not exist"))
    })?;
    if !matches!(routine.kind(), ObjectKind::Function | ObjectKind::Procedure) {
        return Err(Error::invalid_argument(format!(
            "catalog object {} is not an executable routine",
            routine.id()
        )));
    }
    require_namespace_usage(catalog.as_ref(), effective_principal, routine)?;
    require_object_privilege(
        catalog.as_ref(),
        effective_principal,
        routine.id(),
        radixdb_catalog::PRIVILEGE_EXECUTE,
        "EXECUTE",
    )
}

/// Authorize the exact stable table/column identities admitted by the public
/// ORM binder. This closes the navigation-target gap in ordinary syntactic
/// source discovery without creating a second ACL authority.
pub(crate) fn authorize_public_read_accesses(
    catalog: &CatalogGeneration,
    context: &ExecutionContext,
    accesses: &std::collections::BTreeMap<ObjectId, BTreeSet<ObjectId>>,
) -> Result<()> {
    if context.principal_id() == ObjectId::BOOTSTRAP_OWNER
        && context.effective_principal_id() == ObjectId::BOOTSTRAP_OWNER
    {
        return Ok(());
    }
    let session = context.principal_id();
    let effective = context.effective_principal_id();
    require_principal(catalog, session)?;
    require_principal(catalog, effective)?;
    require_object_privilege(
        catalog,
        session,
        ObjectId::BOOTSTRAP_NAMESPACE,
        PRIVILEGE_CONNECT,
        "CONNECT",
    )?;
    for (relation_id, columns) in accesses {
        let relation = catalog.object(*relation_id).ok_or_else(|| {
            Error::invalid_argument(format!(
                "public read relation catalog object {relation_id} is missing"
            ))
        })?;
        if relation.kind() != ObjectKind::Table {
            return Err(Error::invalid_argument(
                "public read access target is not a table",
            ));
        }
        require_namespace_usage(catalog, effective, relation)?;
        if has_object_privilege(catalog, effective, *relation_id, PRIVILEGE_SELECT)? {
            continue;
        }
        if columns.is_empty() {
            return Err(Error::authorization_denied(
                "public read without a bound column requires table-level SELECT",
            ));
        }
        for column in columns {
            require_column_privilege(
                catalog,
                effective,
                *relation_id,
                *column,
                PRIVILEGE_SELECT,
                "SELECT",
            )?;
        }
    }
    Ok(())
}

pub(crate) fn authorize_statement(
    executor: &Executor,
    statement: &Statement,
    context: &ExecutionContext,
) -> Result<()> {
    authorize_application_relation_contract(statement)?;
    if context.principal_id() == ObjectId::BOOTSTRAP_OWNER
        && context.effective_principal_id() == ObjectId::BOOTSTRAP_OWNER
    {
        return Ok(());
    }

    let (catalog, _) = transaction_visible_catalog(executor)?;
    let catalog = catalog.as_ref();
    let session = context.principal_id();
    let effective = context.effective_principal_id();
    require_principal(catalog, session)?;
    require_principal(catalog, effective)?;
    // CONNECT belongs to the authenticated session and is deliberately not
    // elevated by SECURITY DEFINER.
    require_object_privilege(
        catalog,
        session,
        ObjectId::BOOTSTRAP_NAMESPACE,
        PRIVILEGE_CONNECT,
        "CONNECT",
    )?;

    match statement {
        Statement::Select(select) => authorize_select(catalog, effective, select),
        Statement::Insert(insert) => {
            let table = resolve_unqualified_table(catalog, insert.table_name.value())?;
            require_namespace_usage(catalog, effective, table)?;
            require_write_columns(
                catalog,
                effective,
                table,
                PRIVILEGE_INSERT,
                "INSERT",
                &insert.columns,
            )?;
            authorize_read_sources(catalog, effective, statement)?;
            authorize_target_read_columns(catalog, effective, table, statement)
        }
        Statement::Update(update) => {
            let table = resolve_unqualified_table(catalog, update.table_name.value())?;
            require_namespace_usage(catalog, effective, table)?;
            if !has_object_privilege(catalog, effective, table.id(), PRIVILEGE_UPDATE)? {
                for column in update.updates.keys() {
                    require_named_column(
                        catalog,
                        effective,
                        table,
                        column,
                        PRIVILEGE_UPDATE,
                        "UPDATE",
                    )?;
                }
            }
            authorize_read_sources(catalog, effective, statement)?;
            authorize_target_read_columns(catalog, effective, table, statement)
        }
        Statement::Delete(delete) => {
            let table = resolve_unqualified_table(catalog, delete.table_name.value())?;
            require_namespace_usage(catalog, effective, table)?;
            require_object_privilege(catalog, effective, table.id(), PRIVILEGE_DELETE, "DELETE")?;
            authorize_read_sources(catalog, effective, statement)?;
            authorize_target_read_columns(catalog, effective, table, statement)
        }
        Statement::Truncate(truncate) => {
            let table = resolve_unqualified_table(catalog, truncate.table_name.value())?;
            require_owner(catalog, effective, table)
        }
        Statement::Copy(copy) => {
            let table = resolve_unqualified_table(catalog, copy.table_name.value())?;
            require_namespace_usage(catalog, effective, table)?;
            require_write_columns(
                catalog,
                effective,
                table,
                PRIVILEGE_INSERT,
                "INSERT",
                &copy.columns,
            )
        }
        Statement::CreateTable(create) => {
            require_root_create(catalog, effective)?;
            if let Some(select) = &create.as_select {
                authorize_select(catalog, effective, select)?;
            }
            Ok(())
        }
        Statement::CreateView(create) => {
            require_root_create(catalog, effective)?;
            authorize_select(catalog, effective, &create.query)
        }
        Statement::CreateRoutine(_) => require_root_create(catalog, effective),
        Statement::CreateExtension(_) | Statement::DropExtension(_) => {
            require_bootstrap(effective, statement)
        }
        Statement::CreateExternalType(create) => {
            crate::catalog::security::require_external_type_create_authority(
                catalog, effective, create,
            )
        }
        Statement::DropExternalType(drop) => {
            crate::catalog::security::require_external_type_drop_authority(catalog, effective, drop)
        }
        Statement::CreateOperator(_)
        | Statement::DropOperator(_)
        | Statement::CreateOperatorClass(_)
        | Statement::DropOperatorClass(_)
        | Statement::CreatePlannerSupport(_)
        | Statement::DropPlannerSupport(_) => require_root_create(catalog, effective),
        Statement::CreateIndex(create) => {
            let table = resolve_unqualified_table(catalog, create.table_name.value())?;
            require_owner(catalog, effective, table)
        }
        Statement::CreateTrigger(create) => {
            let table = resolve_relation(catalog, &create.table)?;
            require_owner(catalog, effective, table)?;
            let function = crate::catalog::resolve_trigger_function(catalog, create)?;
            require_namespace_usage(catalog, effective, function)?;
            require_object_privilege(
                catalog,
                effective,
                function.id(),
                PRIVILEGE_EXECUTE,
                "EXECUTE",
            )
        }
        Statement::DropRoutine(drop) => match crate::catalog::resolve_optional_routine_signature(
            catalog,
            drop.kind,
            &drop.signature,
        )? {
            Some(object) => require_owner(catalog, effective, object),
            None if drop.if_exists => Ok(()),
            None => Err(Error::InvalidArgument(format!(
                "routine '{}' does not exist",
                drop.signature
            ))),
        },
        Statement::DropTrigger(drop) => {
            match crate::catalog::resolve_optional_trigger(catalog, &drop.name, &drop.table)? {
                Some(object) => require_owner(catalog, effective, object),
                None if drop.if_exists => Ok(()),
                None => Err(Error::InvalidArgument(format!(
                    "trigger '{}' does not exist",
                    drop.name
                ))),
            }
        }
        Statement::DropJob(drop) => {
            match crate::catalog::resolve_optional_job(catalog, &drop.name)? {
                Some(object) => require_owner(catalog, effective, object),
                None if drop.if_exists => Ok(()),
                None => Err(Error::InvalidArgument(format!(
                    "job '{}' does not exist",
                    drop.name
                ))),
            }
        }
        Statement::AlterJob(alter) => {
            let job =
                crate::catalog::resolve_optional_job(catalog, &alter.name)?.ok_or_else(|| {
                    Error::InvalidArgument(format!("job '{}' does not exist", alter.name))
                })?;
            require_owner(catalog, effective, job)
        }
        Statement::CreateSchema(_)
        | Statement::CreatePrincipal(_)
        | Statement::CreateRole(_)
        | Statement::AlterSecuritySubject(_)
        | Statement::DropSecuritySubject(_)
        | Statement::CreateJob(_) => require_bootstrap(effective, statement),
        Statement::DropTable(drop) => authorize_optional_owner(
            catalog,
            effective,
            resolve_unqualified_relation(catalog, drop.table_name.value()),
            drop.if_exists,
        ),
        Statement::DropView(drop) => authorize_optional_owner(
            catalog,
            effective,
            resolve_unqualified_relation(catalog, drop.view_name.value()),
            drop.if_exists,
        ),
        Statement::AlterTable(alter) => {
            let table = resolve_unqualified_table(catalog, alter.table_name.value())?;
            require_owner(catalog, effective, table)
        }
        Statement::DropIndex(drop) => {
            let index = catalog
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, drop.index_name.value())
                .map_err(catalog_error)?;
            authorize_optional_owner(
                catalog,
                effective,
                index.ok_or_else(|| Error::IndexNotFound(drop.index_name.value().to_owned())),
                drop.if_exists,
            )
        }
        Statement::AlterIndex(alter) => {
            let index = catalog
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, alter.index_name.value())
                .map_err(catalog_error)?
                .ok_or_else(|| Error::IndexNotFound(alter.index_name.value().to_owned()))?;
            require_owner(catalog, effective, index)
        }
        Statement::AlterOwner(_) | Statement::Grant(_) | Statement::Revoke(_) => Ok(()),
        Statement::ShowCreateTable(show) => {
            authorize_relation_metadata(catalog, effective, show.table_name.value())
        }
        Statement::ShowCreateView(show) => {
            authorize_relation_metadata(catalog, effective, show.view_name.value())
        }
        Statement::ShowIndexes(show) => {
            authorize_relation_metadata(catalog, effective, show.table_name.value())
        }
        Statement::Describe(describe) => match &describe.target {
            DescribeTarget::Table(table) => {
                authorize_relation_metadata(catalog, effective, table.value())
            }
            DescribeTarget::Database => require_bootstrap(effective, statement),
        },
        Statement::ShowTables(_)
        | Statement::ShowViews(_)
        | Statement::Pragma(_)
        | Statement::Analyze(_)
        | Statement::Vacuum(_) => require_bootstrap(effective, statement),
        Statement::Explain(explain) => authorize_statement(executor, &explain.statement, context),
        Statement::Call(_)
        | Statement::Expression(_)
        | Statement::Begin(_)
        | Statement::Commit(_)
        | Statement::Rollback(_)
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::Set(_) => Ok(()),
    }
}

fn authorize_application_relation_contract(statement: &Statement) -> Result<()> {
    let deny = |operation: &str, relation: &str| {
        Err(Error::authorization_denied(format!(
            "{operation} is forbidden for system-owned relation '{relation}'"
        )))
    };
    match statement {
        Statement::Insert(statement)
            if is_application_relation(statement.table_name.value_lower()) =>
        {
            deny("INSERT", statement.table_name.value_lower())
        }
        Statement::Update(statement)
            if statement.table_name.value_lower() == AUDIT_RELATION_NAME =>
        {
            deny("UPDATE", AUDIT_RELATION_NAME)
        }
        Statement::Delete(statement)
            if statement.table_name.value_lower() == AUDIT_RELATION_NAME =>
        {
            deny("DELETE", AUDIT_RELATION_NAME)
        }
        Statement::Truncate(statement)
            if is_application_relation(statement.table_name.value_lower()) =>
        {
            deny("TRUNCATE", statement.table_name.value_lower())
        }
        Statement::DropTable(statement)
            if is_application_relation(statement.table_name.value_lower()) =>
        {
            deny("DROP TABLE", statement.table_name.value_lower())
        }
        Statement::AlterTable(statement)
            if is_application_relation(statement.table_name.value_lower()) =>
        {
            deny("ALTER TABLE", statement.table_name.value_lower())
        }
        Statement::AlterOwner(statement) => match &statement.target {
            radixdb_sql::OwnershipTargetSyntax::Table(name) => {
                let relation = name
                    .components
                    .iter()
                    .map(Identifier::value_lower)
                    .collect::<Vec<_>>()
                    .join(".");
                if is_application_relation(&relation) {
                    deny("ALTER TABLE OWNER", &relation)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

fn is_application_relation(name: &str) -> bool {
    matches!(name, AUDIT_RELATION_NAME | OUTBOX_RELATION_NAME)
}

fn require_bootstrap(principal: ObjectId, statement: &Statement) -> Result<()> {
    if principal == ObjectId::BOOTSTRAP_OWNER {
        Ok(())
    } else {
        Err(Error::authorization_denied(format!(
            "only the bootstrap owner may execute {statement}"
        )))
    }
}

fn require_root_create(catalog: &CatalogGeneration, principal: ObjectId) -> Result<()> {
    require_object_privilege(
        catalog,
        principal,
        ObjectId::BOOTSTRAP_NAMESPACE,
        PRIVILEGE_CREATE,
        "CREATE",
    )
}

fn authorize_optional_owner(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    object: Result<&CatalogObject>,
    if_exists: bool,
) -> Result<()> {
    match object {
        Ok(object) => require_owner(catalog, principal, object),
        Err(error) if if_exists && error.is_not_found() => Ok(()),
        Err(error) => Err(error),
    }
}

fn authorize_relation_metadata(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    name: &str,
) -> Result<()> {
    let relation = resolve_unqualified_relation(catalog, name)?;
    require_namespace_usage(catalog, principal, relation)?;
    require_object_privilege(
        catalog,
        principal,
        relation.id(),
        PRIVILEGE_SELECT,
        "SELECT",
    )
}

fn authorize_select(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    select: &SelectStatement,
) -> Result<()> {
    authorize_read_sources(catalog, principal, &Statement::Select(select.clone()))
}

fn authorize_read_sources(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    statement: &Statement,
) -> Result<()> {
    let mut sources = Vec::<SimpleTableSource>::new();
    walk_statement_physical_table_sources(statement, &mut |source| sources.push(source.clone()));
    if sources.is_empty() {
        return Ok(());
    }

    let mut relations = Vec::with_capacity(sources.len());
    for source in &sources {
        let relation = resolve_unqualified_relation(catalog, source.name.value())?;
        require_namespace_usage(catalog, principal, relation)?;
        relations.push(relation);
    }
    let mut all_have_object_select = true;
    for relation in &relations {
        all_have_object_select &=
            has_object_privilege(catalog, principal, relation.id(), PRIVILEGE_SELECT)?;
    }
    if all_have_object_select {
        return Ok(());
    }
    // Column-only grants are admitted for the unambiguous one-relation shape.
    // Joins/subqueries must use object SELECT until the public read binder in
    // DB-80 has stable per-column source IDs.
    if relations.len() != 1 || relations[0].kind() != ObjectKind::Table {
        return Err(Error::authorization_denied(
            "column-only SELECT grants require an unambiguous single-table query",
        ));
    }
    let required = referenced_columns(catalog, relations[0], statement, Some(&sources[0]))?;
    if required.is_empty() {
        return Err(Error::authorization_denied(
            "SELECT without a referenced column requires table-level SELECT",
        ));
    }
    for column in required {
        require_column_privilege(
            catalog,
            principal,
            relations[0].id(),
            column,
            PRIVILEGE_SELECT,
            "SELECT",
        )?;
    }
    Ok(())
}

fn authorize_target_read_columns(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    table: &CatalogObject,
    statement: &Statement,
) -> Result<()> {
    let required = referenced_columns(catalog, table, statement, None)?;
    for column in required {
        require_column_privilege(
            catalog,
            principal,
            table.id(),
            column,
            PRIVILEGE_SELECT,
            "SELECT",
        )?;
    }
    Ok(())
}

fn referenced_columns(
    catalog: &CatalogGeneration,
    table: &CatalogObject,
    statement: &Statement,
    source: Option<&SimpleTableSource>,
) -> Result<BTreeSet<ObjectId>> {
    let mut names = BTreeSet::<String>::new();
    let mut all = false;
    let source_names = source.map(|source| {
        let mut names = BTreeSet::from([source.name.value_lower().to_owned()]);
        if let Some(alias) = &source.alias {
            names.insert(alias.value_lower().to_owned());
        }
        names
    });
    walk_statement_tree(statement, &mut |expression| match expression {
        Expression::Identifier(identifier) => {
            names.insert(identifier.value_lower().to_owned());
        }
        Expression::QualifiedIdentifier(identifier) => {
            if source_names
                .as_ref()
                .is_none_or(|allowed| allowed.contains(identifier.qualifier.value_lower()))
            {
                names.insert(identifier.name.value_lower().to_owned());
            }
        }
        Expression::Star(_) => all = true,
        Expression::QualifiedStar(star)
            if source_names
                .as_ref()
                .is_none_or(|allowed| allowed.contains(star.qualifier.to_lowercase().as_str())) =>
        {
            all = true;
        }
        _ => {}
    });
    let CatalogPayload::Table(payload) = table.payload() else {
        return Err(Error::internal(
            "column authorization target is not a table",
        ));
    };
    if all {
        return Ok(payload.column_ids().iter().copied().collect());
    }
    let mut ids = BTreeSet::new();
    for name in names {
        if let Some(column) = catalog
            .find_column(table.id(), &name)
            .map_err(catalog_error)?
        {
            ids.insert(column.id());
        }
    }
    Ok(ids)
}

fn require_write_columns(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    table: &CatalogObject,
    privilege: u64,
    label: &str,
    names: &[Identifier],
) -> Result<()> {
    if has_object_privilege(catalog, principal, table.id(), privilege)? {
        return Ok(());
    }
    let CatalogPayload::Table(payload) = table.payload() else {
        return Err(Error::internal("write authorization target is not a table"));
    };
    if names.is_empty() {
        for column in payload.column_ids() {
            require_column_privilege(catalog, principal, table.id(), *column, privilege, label)?;
        }
    } else {
        for column in names {
            require_named_column(catalog, principal, table, column.value(), privilege, label)?;
        }
    }
    Ok(())
}

fn require_named_column(
    catalog: &CatalogGeneration,
    principal: ObjectId,
    table: &CatalogObject,
    name: &str,
    privilege: u64,
    label: &str,
) -> Result<()> {
    let column = catalog
        .find_column(table.id(), name)
        .map_err(catalog_error)?
        .ok_or_else(|| Error::ColumnNotFound(name.to_owned()))?;
    require_column_privilege(
        catalog,
        principal,
        table.id(),
        column.id(),
        privilege,
        label,
    )
}

fn catalog_error(error: radixdb_catalog::CatalogError) -> Error {
    Error::invalid_argument(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use radixdb_catalog::{CatalogPayload, ObjectId, ObjectKind};
    use radixdb_core::{Error, Value};
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use radixdb_storage::traits::Engine;
    use radixdb_storage::Config;

    use crate::{ExecutionContext, Executor};

    fn executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn principal(executor: &Executor, name: &str) -> ObjectId {
        executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Principal)
            .find(|object| object.name().normalized().as_str() == name)
            .unwrap()
            .id()
    }

    fn context(principal: ObjectId) -> ExecutionContext {
        ExecutionContext::new().with_principal_id(principal)
    }

    fn scalar_integer(mut rows: Box<dyn radixdb_storage::traits::QueryResult>) -> i64 {
        assert!(rows.next());
        let value = match rows.take_row().get(0) {
            Some(Value::Integer(value)) => *value,
            value => panic!("expected INTEGER scalar, got {value:?}"),
        };
        assert!(!rows.next());
        assert!(rows.last_error().is_none());
        rows.close().unwrap();
        value
    }

    #[test]
    fn system_relation_insert_is_denied_before_bootstrap_bypass() {
        for sql in [
            "INSERT INTO audit.event VALUES (1)",
            "INSERT INTO outbox.message VALUES (1)",
        ] {
            let statements = radixdb_sql::parse_sql(sql).unwrap();
            let [radixdb_sql::Statement::Insert(insert)] = statements.as_slice() else {
                panic!("expected INSERT AST for {sql}")
            };
            assert!(matches!(
                insert.table_name.value_lower(),
                "audit.event" | "outbox.message"
            ));
            assert!(matches!(
                super::authorize_application_relation_contract(&statements[0]),
                Err(Error::AuthorizationDenied(_))
            ));
        }
    }

    #[test]
    fn principal_credentials_are_verified_and_never_stored_as_plaintext() {
        let executor = executor();
        assert!(executor
            .execute("CREATE PRINCIPAL empty PASSWORD ''")
            .is_err());
        executor
            .execute("CREATE PRINCIPAL alice PASSWORD 'catalog-secret'")
            .unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        let alice = principal(&executor, "alice");
        assert_eq!(
            executor
                .authenticate_principal("alice", "catalog-secret")
                .unwrap(),
            alice
        );
        assert!(executor
            .authenticate_principal("alice", "wrong-secret")
            .is_err());

        let catalog = executor.engine().pin_catalog().unwrap();
        let payload = match catalog.object(alice).unwrap().payload() {
            CatalogPayload::Principal(payload) => payload,
            payload => panic!("expected Principal payload, got {payload:?}"),
        };
        let verifier = payload.credential().expect("stored credential verifier");
        assert_eq!(
            verifier.scheme(),
            radixdb_catalog::CREDENTIAL_SCHEME_ARGON2ID_PHC_V1
        );
        assert!(verifier.encoded().starts_with(b"$argon2id$"));
        assert!(!verifier
            .encoded()
            .windows(b"catalog-secret".len())
            .any(|window| window == b"catalog-secret"));
    }

    #[test]
    fn column_grants_roles_and_revoke_are_enforced_after_cache_fill() {
        let executor = executor();
        executor
            .execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO documents (id, body) VALUES (1, 'secret')")
            .unwrap();
        executor.execute("CREATE PRINCIPAL alice").unwrap();
        executor.execute("CREATE ROLE reader").unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        executor
            .execute("GRANT USAGE ON SCHEMA public TO alice")
            .unwrap();
        executor
            .execute("GRANT SELECT (id) ON TABLE documents TO alice")
            .unwrap();
        executor
            .execute("GRANT SELECT (body) ON TABLE documents TO reader")
            .unwrap();
        let alice = context(principal(&executor, "alice"));

        let mut id = executor
            .execute_with_context("SELECT id FROM documents", &alice)
            .unwrap();
        assert!(id.next());
        assert_eq!(id.row().get(0), Some(&Value::Integer(1)));
        assert!(matches!(
            executor.execute_with_context("SELECT body FROM documents", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        assert!(matches!(
            executor.execute_with_context("SELECT * FROM documents", &alice),
            Err(Error::AuthorizationDenied(_))
        ));

        executor.execute("GRANT reader TO alice").unwrap();
        let mut body = executor
            .execute_with_context("SELECT body FROM documents", &alice)
            .unwrap();
        assert!(body.next());
        assert_eq!(body.row().get(0), Some(&Value::text("secret")));
        drop(body);

        // The same SQL text is cached, but authorization is re-evaluated.
        executor.execute("REVOKE reader FROM alice").unwrap();
        assert!(matches!(
            executor.execute_with_context("SELECT body FROM documents", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
    }

    #[test]
    fn role_cycles_and_non_owner_ddl_fail_closed() {
        let executor = executor();
        executor.execute("CREATE PRINCIPAL alice").unwrap();
        executor.execute("CREATE PRINCIPAL bob").unwrap();
        executor.execute("CREATE ROLE first_role").unwrap();
        executor.execute("CREATE ROLE second_role").unwrap();
        executor.execute("GRANT first_role TO second_role").unwrap();
        assert!(executor.execute("GRANT second_role TO first_role").is_err());

        for name in ["alice", "bob"] {
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT USAGE ON SCHEMA public TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CREATE ON SCHEMA public TO {name}"))
                .unwrap();
        }
        let alice_id = principal(&executor, "alice");
        let bob_id = principal(&executor, "bob");
        executor
            .execute_with_context(
                "CREATE TABLE alice_table (id INTEGER PRIMARY KEY, value TEXT)",
                &context(alice_id),
            )
            .unwrap();
        let catalog = executor.engine().pin_catalog().unwrap();
        let table = catalog
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "alice_table")
            .unwrap()
            .unwrap();
        assert_eq!(table.owner_principal_id(), alice_id);
        drop(catalog);

        assert!(matches!(
            executor.execute_with_context(
                "ALTER TABLE alice_table ADD COLUMN denied INTEGER",
                &context(bob_id),
            ),
            Err(Error::AuthorizationDenied(_))
        ));
        executor
            .execute("ALTER TABLE alice_table OWNER TO bob")
            .unwrap();
        executor
            .execute_with_context(
                "ALTER TABLE alice_table ADD COLUMN accepted INTEGER",
                &context(bob_id),
            )
            .unwrap();
    }

    #[test]
    fn invoker_definer_and_execute_revoke_keep_the_effective_principal_bounded() {
        let executor = executor();
        executor
            .execute("CREATE TABLE protected_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("INSERT INTO protected_rows VALUES (7)")
            .unwrap();
        executor
            .execute(
                "CREATE PROCEDURE invoker_read(OUT output_value INTEGER NOT NULL) \
                 LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 SELECT id INTO STRICT output_value FROM protected_rows WHERE id = 7; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE PROCEDURE definer_read(OUT output_value INTEGER NOT NULL) \
                 LANGUAGE RADIX SECURITY DEFINER SEARCH PATH (public) AS BEGIN \
                 SELECT id INTO STRICT output_value FROM protected_rows WHERE id = 7; END;",
            )
            .unwrap();
        executor.execute("CREATE PRINCIPAL alice").unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        executor
            .execute("GRANT USAGE ON SCHEMA public TO alice")
            .unwrap();
        executor
            .execute("GRANT EXECUTE ON PROCEDURE invoker_read() TO alice")
            .unwrap();
        executor
            .execute("GRANT EXECUTE ON PROCEDURE definer_read() TO alice")
            .unwrap();
        let alice = context(principal(&executor, "alice"));

        assert!(matches!(
            executor.execute_with_context("CALL invoker_read()", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        let mut definer = executor
            .execute_with_context("CALL definer_read()", &alice)
            .unwrap();
        assert!(definer.next());
        assert_eq!(definer.row().get(0), Some(&Value::Integer(7)));
        assert!(!definer.next());
        assert!(definer.last_error().is_none());
        definer.close().unwrap();

        // SECURITY DEFINER affects only its dynamic call frame. The caller is
        // not left with the routine owner's SELECT privilege afterwards.
        assert!(matches!(
            executor.execute_with_context("SELECT id FROM protected_rows", &alice),
            Err(Error::AuthorizationDenied(_))
        ));

        // Reusing the exact CALL text must not retain an earlier EXECUTE
        // admission through either the query or procedural cache.
        executor
            .execute("REVOKE EXECUTE ON PROCEDURE definer_read() FROM alice")
            .unwrap();
        assert!(matches!(
            executor.execute_with_context("CALL definer_read()", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
    }

    #[test]
    fn trigger_attachment_and_every_firing_recheck_exact_execute_privilege() {
        let executor = executor();
        executor
            .execute("CREATE TABLE protected_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("CREATE TABLE trigger_target (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION privileged_trigger() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
                 SECURITY DEFINER SEARCH PATH (public) AS \
                 DECLARE effect_id INTEGER; BEGIN \
                     effect_id := NEW.id; \
                     INSERT INTO protected_effects VALUES (:effect_id); RETURN NEW; \
                 END;",
            )
            .unwrap();
        executor.execute("CREATE PRINCIPAL alice").unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        executor
            .execute("GRANT USAGE ON SCHEMA public TO alice")
            .unwrap();
        executor
            .execute("ALTER TABLE trigger_target OWNER TO alice")
            .unwrap();
        let alice_id = principal(&executor, "alice");
        let alice = context(alice_id);
        let create_trigger =
            "CREATE TRIGGER privileged BEFORE INSERT ON trigger_target FOR EACH ROW \
             EXECUTE FUNCTION privileged_trigger();";

        assert!(matches!(
            executor.execute_with_context(create_trigger, &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        assert_eq!(
            executor
                .engine()
                .pin_catalog()
                .unwrap()
                .objects_of_kind(ObjectKind::Trigger)
                .count(),
            0,
            "denied attachment must not publish a trigger"
        );

        executor
            .execute("GRANT EXECUTE ON FUNCTION privileged_trigger() TO alice")
            .unwrap();
        executor
            .execute_with_context(create_trigger, &alice)
            .unwrap();
        let trigger_owner = executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Trigger)
            .find(|object| object.name().normalized().as_str() == "privileged")
            .unwrap()
            .owner_principal_id();
        assert_eq!(trigger_owner, alice_id);

        executor
            .execute_with_context("INSERT INTO trigger_target VALUES (1)", &alice)
            .unwrap();
        assert_eq!(
            scalar_integer(
                executor
                    .execute("SELECT COUNT(*) FROM protected_effects")
                    .unwrap()
            ),
            1
        );

        executor
            .execute("REVOKE EXECUTE ON FUNCTION privileged_trigger() FROM alice")
            .unwrap();
        assert!(matches!(
            executor.execute_with_context("INSERT INTO trigger_target VALUES (2)", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        assert_eq!(
            scalar_integer(
                executor
                    .execute("SELECT COUNT(*) FROM trigger_target")
                    .unwrap()
            ),
            1,
            "firing denial must roll back the outer DML"
        );
        assert_eq!(
            scalar_integer(
                executor
                    .execute("SELECT COUNT(*) FROM protected_effects")
                    .unwrap()
            ),
            1,
            "cached SECURITY DEFINER body must not run after revoke"
        );

        // A bootstrap replacement cannot silently transfer attachment ownership and thereby
        // erase the firing-time check against the original table owner.
        executor
            .execute(
                "CREATE OR REPLACE TRIGGER privileged BEFORE INSERT ON trigger_target \
                 FOR EACH ROW EXECUTE FUNCTION privileged_trigger();",
            )
            .unwrap();
        let replaced_owner = executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Trigger)
            .find(|object| object.name().normalized().as_str() == "privileged")
            .unwrap()
            .owner_principal_id();
        assert_eq!(replaced_owner, alice_id);
        assert!(matches!(
            executor.execute_with_context("INSERT INTO trigger_target VALUES (2)", &alice),
            Err(Error::AuthorizationDenied(_))
        ));

        executor
            .execute("GRANT EXECUTE ON FUNCTION privileged_trigger() TO alice")
            .unwrap();
        executor
            .execute_with_context("INSERT INTO trigger_target VALUES (2)", &alice)
            .unwrap();
        assert_eq!(
            scalar_integer(
                executor
                    .execute("SELECT COUNT(*) FROM protected_effects")
                    .unwrap()
            ),
            2
        );
    }

    #[test]
    fn grant_and_revoke_publish_atomically_across_sessions() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let admin = Executor::new(engine.clone());
        let peer = Executor::new(engine);
        admin
            .execute("CREATE TABLE shared_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        admin.execute("INSERT INTO shared_rows VALUES (1)").unwrap();
        admin.execute("CREATE PRINCIPAL alice").unwrap();
        admin
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        admin
            .execute("GRANT USAGE ON SCHEMA public TO alice")
            .unwrap();
        let alice = context(principal(&admin, "alice"));

        admin.execute("BEGIN").unwrap();
        admin
            .execute("GRANT SELECT ON TABLE shared_rows TO alice")
            .unwrap();
        let mut private = admin
            .execute_with_context("SELECT id FROM shared_rows", &alice)
            .unwrap();
        assert!(private.next());
        private.close().unwrap();
        assert!(matches!(
            peer.execute_with_context("SELECT id FROM shared_rows", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        admin.execute("COMMIT").unwrap();

        let mut published = peer
            .execute_with_context("SELECT id FROM shared_rows", &alice)
            .unwrap();
        assert!(published.next());
        published.close().unwrap();

        admin.execute("BEGIN").unwrap();
        admin
            .execute("REVOKE SELECT ON TABLE shared_rows FROM alice")
            .unwrap();
        assert!(matches!(
            admin.execute_with_context("SELECT id FROM shared_rows", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        let mut still_published = peer
            .execute_with_context("SELECT id FROM shared_rows", &alice)
            .unwrap();
        assert!(still_published.next());
        still_published.close().unwrap();
        admin.execute("ROLLBACK").unwrap();

        let mut restored = peer
            .execute_with_context("SELECT id FROM shared_rows", &alice)
            .unwrap();
        assert!(restored.next());
        restored.close().unwrap();
        admin.execute("BEGIN").unwrap();
        admin
            .execute("REVOKE SELECT ON TABLE shared_rows FROM alice")
            .unwrap();
        admin.execute("COMMIT").unwrap();
        assert!(matches!(
            peer.execute_with_context("SELECT id FROM shared_rows", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
    }

    #[test]
    fn grants_cannot_be_redelegated_without_owner_or_admin_authority() {
        let executor = executor();
        for name in ["alice", "bob", "carol"] {
            executor
                .execute(&format!("CREATE PRINCIPAL {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT USAGE ON SCHEMA public TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CREATE ON SCHEMA public TO {name}"))
                .unwrap();
        }
        executor.execute("CREATE ROLE readers").unwrap();
        let alice = context(principal(&executor, "alice"));
        let bob = context(principal(&executor, "bob"));
        executor
            .execute_with_context("CREATE TABLE owned_rows (id INTEGER PRIMARY KEY)", &alice)
            .unwrap();
        executor
            .execute_with_context("GRANT SELECT ON TABLE owned_rows TO bob", &alice)
            .unwrap();
        assert!(matches!(
            executor.execute_with_context("GRANT SELECT ON TABLE owned_rows TO carol", &bob,),
            Err(Error::AuthorizationDenied(_))
        ));
        assert!(matches!(
            executor.execute_with_context("GRANT readers TO carol", &alice),
            Err(Error::AuthorizationDenied(_))
        ));

        executor
            .execute("GRANT readers TO alice WITH ADMIN OPTION")
            .unwrap();
        executor
            .execute_with_context("GRANT readers TO carol", &alice)
            .unwrap();
        executor
            .execute_with_context("REVOKE readers FROM carol", &alice)
            .unwrap();
    }

    #[test]
    fn r12_batch_c_schema_create_is_independent_from_usage() {
        let executor = executor();
        executor
            .execute("CREATE TABLE existing_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        for name in ["usage_only", "create_only"] {
            executor
                .execute(&format!("CREATE PRINCIPAL {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
        }
        executor
            .execute("GRANT USAGE ON SCHEMA public TO usage_only")
            .unwrap();
        executor
            .execute("GRANT CREATE ON SCHEMA public TO create_only")
            .unwrap();
        executor
            .execute("GRANT SELECT ON TABLE existing_rows TO create_only")
            .unwrap();

        assert!(matches!(
            executor.execute_with_context(
                "CREATE TABLE denied_rows (id INTEGER)",
                &context(principal(&executor, "usage_only")),
            ),
            Err(Error::AuthorizationDenied(_))
        ));
        executor
            .execute_with_context(
                "CREATE TABLE accepted_rows (id INTEGER)",
                &context(principal(&executor, "create_only")),
            )
            .unwrap();
        assert!(matches!(
            executor.execute_with_context(
                "SELECT id FROM existing_rows",
                &context(principal(&executor, "create_only")),
            ),
            Err(Error::AuthorizationDenied(_))
        ));
    }

    #[test]
    fn r12_batch_c_object_grant_option_restrict_cascade_and_multiple_paths() {
        let executor = executor();
        executor
            .execute("CREATE TABLE reports (id INTEGER PRIMARY KEY, secret TEXT)")
            .unwrap();
        executor
            .execute("INSERT INTO reports VALUES (1, 'hidden')")
            .unwrap();
        for name in ["alice", "bob", "carol"] {
            executor
                .execute(&format!("CREATE PRINCIPAL {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT USAGE ON SCHEMA public TO {name}"))
                .unwrap();
        }
        executor
            .execute("GRANT SELECT (id) ON TABLE reports TO alice WITH GRANT OPTION")
            .unwrap();
        let alice = context(principal(&executor, "alice"));
        executor
            .execute_with_context("GRANT SELECT (id) ON TABLE reports TO carol", &alice)
            .unwrap();
        assert!(matches!(
            executor
                .execute_with_context("GRANT SELECT (secret) ON TABLE reports TO carol", &alice,),
            Err(Error::AuthorizationDenied(_))
        ));
        assert!(executor
            .execute("REVOKE GRANT OPTION FOR SELECT (id) ON TABLE reports FROM alice RESTRICT")
            .is_err());

        executor
            .execute("GRANT SELECT (id) ON TABLE reports TO bob WITH GRANT OPTION")
            .unwrap();
        executor
            .execute_with_context(
                "GRANT SELECT (id) ON TABLE reports TO carol",
                &context(principal(&executor, "bob")),
            )
            .unwrap();
        executor
            .execute("REVOKE GRANT OPTION FOR SELECT (id) ON TABLE reports FROM alice CASCADE")
            .unwrap();
        let mut carol_rows = executor
            .execute_with_context(
                "SELECT id FROM reports",
                &context(principal(&executor, "carol")),
            )
            .unwrap();
        assert!(
            carol_rows.next(),
            "Bob's independent grant path must remain"
        );
        carol_rows.close().unwrap();

        executor
            .execute("REVOKE GRANT OPTION FOR SELECT (id) ON TABLE reports FROM bob CASCADE")
            .unwrap();
        assert!(matches!(
            executor.execute_with_context(
                "SELECT id FROM reports",
                &context(principal(&executor, "carol")),
            ),
            Err(Error::AuthorizationDenied(_))
        ));
        let mut alice_rows = executor
            .execute_with_context("SELECT id FROM reports", &alice)
            .unwrap();
        assert!(alice_rows.next(), "option-only revoke must preserve SELECT");
        alice_rows.close().unwrap();
    }

    #[test]
    fn r12_batch_c_role_delegation_and_lifecycle_are_atomic() {
        let executor = executor();
        executor
            .execute("CREATE TABLE role_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("INSERT INTO role_rows VALUES (1)")
            .unwrap();
        for name in ["alice", "bob", "carol"] {
            executor
                .execute(&format!("CREATE PRINCIPAL {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT CONNECT ON DATABASE test TO {name}"))
                .unwrap();
            executor
                .execute(&format!("GRANT USAGE ON SCHEMA public TO {name}"))
                .unwrap();
        }
        executor.execute("CREATE ROLE readers").unwrap();
        executor
            .execute("GRANT SELECT ON TABLE role_rows TO readers")
            .unwrap();
        executor
            .execute("GRANT readers TO alice WITH ADMIN OPTION")
            .unwrap();
        executor
            .execute("GRANT readers TO bob WITH ADMIN OPTION")
            .unwrap();
        let alice = context(principal(&executor, "alice"));
        let bob = context(principal(&executor, "bob"));
        executor
            .execute_with_context("GRANT readers TO carol", &alice)
            .unwrap();
        executor
            .execute_with_context("GRANT readers TO carol", &bob)
            .unwrap();

        let mut provenance = executor.execute("PRAGMA ACL_PROVENANCE").unwrap();
        assert_eq!(provenance.columns()[8], "grant_kind");
        let mut delegated_to_carol = 0;
        while provenance.next() {
            let row = provenance.row();
            if row.get(4) == Some(&Value::text("carol"))
                && row.get(6) == Some(&Value::text("readers"))
                && row.get(8) == Some(&Value::text("role_membership"))
            {
                delegated_to_carol += 1;
            }
        }
        assert_eq!(delegated_to_carol, 2);
        assert!(matches!(
            executor.execute_with_context("PRAGMA ACL_PROVENANCE", &alice),
            Err(Error::AuthorizationDenied(_))
        ));

        assert!(executor
            .execute("REVOKE ADMIN OPTION FOR readers FROM alice")
            .is_err());
        executor
            .execute("REVOKE ADMIN OPTION FOR readers FROM alice CASCADE")
            .unwrap();
        let mut carol_rows = executor
            .execute_with_context(
                "SELECT id FROM role_rows",
                &context(principal(&executor, "carol")),
            )
            .unwrap();
        assert!(carol_rows.next(), "Bob's independent role path must remain");
        carol_rows.close().unwrap();
        executor
            .execute("REVOKE ADMIN OPTION FOR readers FROM bob CASCADE")
            .unwrap();
        assert!(matches!(
            executor.execute_with_context(
                "SELECT id FROM role_rows",
                &context(principal(&executor, "carol")),
            ),
            Err(Error::AuthorizationDenied(_))
        ));
        let mut alice_rows = executor
            .execute_with_context("SELECT id FROM role_rows", &alice)
            .unwrap();
        assert!(alice_rows.next());
        alice_rows.close().unwrap();

        let role_id = executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Role)
            .find(|role| role.name().normalized().as_str() == "readers")
            .unwrap()
            .id();
        executor.execute("BEGIN").unwrap();
        executor
            .execute("ALTER ROLE readers RENAME TO viewers")
            .unwrap();
        executor.execute("ROLLBACK").unwrap();
        assert!(executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Role)
            .any(|role| role.id() == role_id && role.name().normalized().as_str() == "readers"));
        executor.execute("ALTER ROLE readers DISABLE").unwrap();
        assert!(matches!(
            executor.execute_with_context("SELECT id FROM role_rows", &alice),
            Err(Error::AuthorizationDenied(_))
        ));
        executor.execute("ALTER ROLE readers ENABLE").unwrap();
        executor
            .execute("ALTER ROLE readers RENAME TO viewers")
            .unwrap();
        assert!(executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Role)
            .any(|role| role.id() == role_id && role.name().normalized().as_str() == "viewers"));
        assert!(executor.execute("DROP ROLE viewers").is_err());
        executor.execute("DROP ROLE viewers CASCADE").unwrap();
        assert!(executor
            .engine()
            .pin_catalog()
            .unwrap()
            .objects_of_kind(ObjectKind::Role)
            .all(|role| role.id() != role_id));
    }

    #[test]
    fn r12_batch_c_principal_lifecycle_survives_checkpoint_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::with_path(directory.path().to_string_lossy().to_string());
        config.persistence.checkpoint_on_close = false;
        let engine = Arc::new(MVCCEngine::new(config.clone()));
        engine.install_catalog_runtime_binder(
            radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(crate::bind_runtime_catalog),
        );
        engine.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&engine));
        executor.execute("CREATE PRINCIPAL alice").unwrap();
        let alice_id = principal(&executor, "alice");
        executor.execute("ALTER PRINCIPAL alice ENABLE").unwrap();
        executor
            .execute("GRANT CONNECT ON DATABASE test TO alice")
            .unwrap();
        executor
            .execute("GRANT USAGE ON SCHEMA public TO alice")
            .unwrap();
        executor
            .execute("GRANT CREATE ON SCHEMA public TO alice")
            .unwrap();
        executor
            .execute_with_context(
                "CREATE TABLE owned_by_alice (id INTEGER PRIMARY KEY)",
                &context(alice_id),
            )
            .unwrap();
        executor
            .execute_with_context("INSERT INTO owned_by_alice VALUES (1)", &context(alice_id))
            .unwrap();
        executor
            .execute(
                "CREATE PROCEDURE alice_job_proc() LANGUAGE RADIX SECURITY INVOKER AS \
                 BEGIN INSERT INTO owned_by_alice VALUES (2); END;",
            )
            .unwrap();
        executor
            .execute("GRANT EXECUTE ON PROCEDURE alice_job_proc() TO alice")
            .unwrap();
        executor
            .execute(
                "CREATE JOB alice_job SCHEDULE EVERY INTERVAL '1 hour' \
                 RUN AS alice CALL alice_job_proc() DISABLE;",
            )
            .unwrap();
        executor.execute("BEGIN").unwrap();
        executor
            .execute("ALTER PRINCIPAL alice RENAME TO discarded_name")
            .unwrap();
        executor.execute("ROLLBACK").unwrap();
        assert_eq!(principal(&executor, "alice"), alice_id);
        executor
            .execute("ALTER PRINCIPAL alice RENAME TO alice_live")
            .unwrap();
        executor
            .execute("ALTER PRINCIPAL alice_live DISABLE")
            .unwrap();
        let before = executor.engine().pin_catalog().unwrap();
        let alice = before.object(alice_id).unwrap();
        assert_eq!(alice.name().normalized().as_str(), "alice_live");
        assert!(matches!(
            alice.payload(),
            CatalogPayload::Principal(payload) if !payload.login_enabled()
        ));
        drop(before);
        engine.force_checkpoint_cycle().unwrap();
        drop(executor);
        engine.close_engine().unwrap();
        drop(engine);

        let reopened = Arc::new(MVCCEngine::new(config));
        reopened.install_catalog_runtime_binder(
            radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(crate::bind_runtime_catalog),
        );
        reopened.open_engine().unwrap();
        let executor = Executor::new(Arc::clone(&reopened));
        let catalog = executor.engine().pin_catalog().unwrap();
        let alice = catalog.object(alice_id).unwrap();
        assert_eq!(alice.name().normalized().as_str(), "alice_live");
        assert!(matches!(
            alice.payload(),
            CatalogPayload::Principal(payload) if !payload.login_enabled()
        ));
        drop(catalog);
        assert!(executor.execute("DROP PRINCIPAL alice_live").is_err());
        executor
            .execute("DROP PRINCIPAL alice_live CASCADE")
            .unwrap();
        assert!(executor
            .engine()
            .pin_catalog()
            .unwrap()
            .object(alice_id)
            .is_none());
        let catalog = executor.engine().pin_catalog().unwrap();
        assert!(catalog.objects_of_kind(ObjectKind::Job).all(|job| job
            .name()
            .normalized()
            .as_str()
            != "alice_job"));
        let table = catalog
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "owned_by_alice")
            .unwrap()
            .unwrap();
        assert_eq!(table.owner_principal_id(), ObjectId::BOOTSTRAP_OWNER);
        drop(catalog);
        let mut rows = executor.execute("SELECT id FROM owned_by_alice").unwrap();
        assert!(rows.next(), "DROP CASCADE must not drop owned data objects");
        rows.close().unwrap();
        drop(executor);
        reopened.close_engine().unwrap();
    }

    #[test]
    fn concurrent_call_dml_ddl_and_revoke_observe_only_complete_generations() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let admin = Executor::new(Arc::clone(&engine));
        admin
            .execute("CREATE TABLE concurrent_acl_rows (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        admin
            .execute("INSERT INTO concurrent_acl_rows VALUES (1, 0)")
            .unwrap();
        admin
            .execute(
                "CREATE PROCEDURE concurrent_acl_touch() LANGUAGE RADIX \
                 SECURITY DEFINER SEARCH PATH (public) AS BEGIN \
                     UPDATE concurrent_acl_rows SET value = value + 1 WHERE id = 1; \
                 END;",
            )
            .unwrap();
        admin.execute("CREATE PRINCIPAL concurrent_alice").unwrap();
        admin
            .execute("GRANT CONNECT ON DATABASE test TO concurrent_alice")
            .unwrap();
        admin
            .execute("GRANT USAGE ON SCHEMA public TO concurrent_alice")
            .unwrap();
        admin
            .execute("GRANT EXECUTE ON PROCEDURE concurrent_acl_touch() TO concurrent_alice")
            .unwrap();
        let alice_id = principal(&admin, "concurrent_alice");

        let mut warm = admin
            .execute_with_context("CALL concurrent_acl_touch()", &context(alice_id))
            .unwrap();
        while warm.next() {}
        assert!(warm.last_error().is_none());
        warm.close().unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let successes = Arc::new(AtomicUsize::new(0));
        let denials = Arc::new(AtomicUsize::new(0));
        let caller_engine = Arc::clone(&engine);
        let caller_barrier = Arc::clone(&barrier);
        let caller_successes = Arc::clone(&successes);
        let caller_denials = Arc::clone(&denials);
        let caller = std::thread::spawn(move || {
            let executor = Executor::new(caller_engine);
            let alice = context(alice_id);
            caller_barrier.wait();
            for _ in 0..128 {
                match executor.execute_with_context("CALL concurrent_acl_touch()", &alice) {
                    Ok(mut result) => {
                        while result.next() {}
                        assert!(result.last_error().is_none());
                        result.close().unwrap();
                        caller_successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(Error::AuthorizationDenied(_)) => {
                        caller_denials.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => panic!("unexpected concurrent CALL error: {error}"),
                }
            }
        });

        barrier.wait();
        admin
            .execute("CREATE TABLE concurrent_ddl_marker (id INTEGER PRIMARY KEY)")
            .unwrap();
        admin
            .execute("REVOKE EXECUTE ON PROCEDURE concurrent_acl_touch() FROM concurrent_alice")
            .unwrap();
        caller.join().unwrap();

        assert!(matches!(
            admin.execute_with_context("CALL concurrent_acl_touch()", &context(alice_id)),
            Err(Error::AuthorizationDenied(_))
        ));
        let mut count = admin
            .execute("SELECT value FROM concurrent_acl_rows WHERE id = 1")
            .unwrap();
        assert!(count.next());
        assert_eq!(
            count.row().get(0),
            Some(&Value::Integer(
                1 + successes.load(Ordering::Relaxed) as i64
            ))
        );
        assert!(successes.load(Ordering::Relaxed) + denials.load(Ordering::Relaxed) == 128);
    }
}
